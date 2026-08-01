# Pre-trade cost telemetry

`arbitrage_evaluation` contains a diagnostic `baseline.pretrade_cost` object for
both arbitrage directions on a bounded one-Hz sample whenever the exact baseline
quote is available. It samples evaluations below and above the production 20
bps gate, so it can measure opportunities that a future net-edge policy might
capture. Unsampled evaluations retain `pretrade_cost: null` and identify the
sampling contract in top-level fields.

The model is `diagnostic_net_edge_v2`. It subtracts these costs from raw venue
profit:

- the conservative side-specific Binance taker fee, rounded up in token-A base
  units; the BNB discount is intentionally not assumed;
- the current execution-owner gas fee cap multiplied by the last successful
  same-protocol gas usage, falling back to the reviewed 250,000 gas bound;
- the last successful same-protocol World Chain `l1Fee`, when World Chain
  requires a separate L1 fee estimate.

The exact CLMM quote already includes the pool fee and curve impact. Calldata
slippage is an execution bound, not an expected cost, and is not subtracted a
second time. Recovery, inventory, and future market movement remain excluded
and are explicitly listed in the payload.

Every source carries availability and age fields. The background model keeps
the current and previous gas, native-conversion, and receipt samples and picks
the newest one captured no later than the evaluated Binance quote. This avoids
look-ahead without losing the valid predecessor when a refresh races queued
serialization. Gas must satisfy the reviewed two-second execution-cache TTL;
native conversion has a diagnostic-only 30-second TTL. The hypothetical 5 bps
result is `null` unless all model inputs are available and fresh. None of these
values is read by readiness, sizing, admission, preflight, or execution.

On startup, each execution owner independently and best-effort fetches the
newest successful same-protocol receipt named in its durable EVM journal. A
five-second timeout or any RPC failure only leaves the diagnostic model
incomplete; it cannot delay readiness or execution. Payloads distinguish
`journal_bootstrap_receipt` from `live_execution_receipt`. This gives World
Chain an L1-fee model before the first post-release fill when historical
receipt evidence is available.
For a network that requires a separate L1 fee, a zero or omitted receipt
`l1Fee` remains unavailable rather than silently becoming a zero-cost input.

The calculation and JSON serialization run in the existing background hot
telemetry task and are capped at one snapshot per pair per second. No additional
telemetry record is created per evaluation, so the change does not increase
record pressure on the bounded ClickHouse channel. The producer path remains
the existing `try_send`; drops are still reported by
`hot_telemetry_dropped_records`.

The unauthenticated `collect-prices` sidecar disables the cost model entirely:
it has no symbol commission or execution-owner gas source and must not create a
plausible but invalid cohort. Live engines label commission provenance as
`authenticated_account_symbol_commission`.

Every accepted admission also enqueues one bounded background
`pretrade_cost_candidate` record. It contains the exact selected size,
`plan_id`, pair, direction, Binance `update_id`, receive time, and the same v2
cost payload. New trade journal intents persist `opportunity_update_id`, and
`arbitrage_result` repeats it. The report can therefore join a prediction to
the exact admitted and realized trade without running cost math on the trading
owner. A full candidate channel is counted as a hot-telemetry drop and never
blocks admission.

After a 24-hour collection window, summarize the cohort with:

```bash
scripts/report-pretrade-cost-model START_UTC END_UTC
```

The report prints both the sampled market cohort and admitted-to-realized join
coverage. Before proposing a gate change, require a representative
complete-input cohort, inspect net-edge quantiles and hypothetical new captures
separately by pair and direction, verify telemetry queue delay/drop health, and
compare the estimator against realized receipt gas and primary execution drag.
Changing the live 20 bps gate remains a separate reviewed production change.

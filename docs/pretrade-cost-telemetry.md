# Pre-trade cost telemetry

`arbitrage_evaluation` contains a diagnostic `baseline.pretrade_cost` object for
both arbitrage directions on a bounded one-Hz sample whenever the exact baseline
quote is available. It samples evaluations below and above the production 20
bps gate, so it can measure opportunities that a future net-edge policy might
capture. Unsampled evaluations retain `pretrade_cost: null` and identify the
sampling contract in top-level fields.

The model is `diagnostic_net_edge_v1`. It subtracts these costs from raw venue
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

Every source carries availability and age fields. A gas or conversion sample
captured after the evaluated Binance quote is rejected to prevent look-ahead.
The hypothetical 5 bps result is `null` unless all model inputs are available.
None of these values is read by readiness, sizing, admission, preflight, or
execution.

The calculation and JSON serialization run in the existing background hot
telemetry task and are capped at one snapshot per pair per second. No additional
telemetry record is created per evaluation, so the change does not increase
record pressure on the bounded ClickHouse channel. The producer path remains
the existing `try_send`; drops are still reported by
`hot_telemetry_dropped_records`.

After a 24-hour collection window, summarize the cohort with:

```bash
scripts/report-pretrade-cost-model START_UTC END_UTC
```

Before proposing a gate change, require a representative complete-input cohort,
inspect net-edge quantiles and hypothetical new captures separately by pair and
direction, verify telemetry queue delay/drop health, and compare the estimator
against realized Binance commissions and receipt gas costs. Changing the live
20 bps gate remains a separate reviewed production change.

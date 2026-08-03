# Comparable arbitrage results

Status: live parent accounting active in GKE production
Last reviewed: 2026-08-04

The Rust equivalent of Rails `arbitrage_results` is the ClickHouse
`arbitrage_results` table. It is populated asynchronously from terminal parent
trade events and is never read by admission, execution, recovery, or restart
logic.

Paper modes emit `paper_arbitrage_result`, including explicit
`comparable_to_live=false`, `includes_binance_fee=true`, and
`includes_gas=false`. The materialized view accepts only `arbitrage_result`, so
paper fills can validate orchestration and restart behavior but can never be
mistaken for the 100 executed opportunities in the goal criterion.

One row means one parent intent reached `balanced_profit` or `balanced_loss`.
An opportunity evaluation is not a result, and an unknown or halted exposure
must not be counted as a completed trade.

The live task emits the exact `arbitrage_result` kind consumed by the
materialized view. `arbitrage_admitted` and `arbitrage_inventory_state` retain
the same deterministic `plan_id`, so the result population can be audited back
to admitted opportunities and unresolved reservations without joining on
timestamps or prices.

`opportunity_received_unix_us` is the stable market-observation timestamp
persisted with new intents. `resumed_after_restart=true` identifies a terminal
result first produced while reconciling an older journal operation. Such a
result is economically real, but it must not be attributed to a new opportunity
in the restart hour.

## Terminal state projection for dashboards

Live completion also emits `arbitrage_terminal_state` with the deterministic
`plan_id`, `pair_id`, terminal stage, and `state=Balanced`. Unlike
`arbitrage_result`, this is an idempotent state projection rather than an
accounting row. Every process startup re-emits it for terminal operations found
in the durable coordinator journal. Consumers may therefore use it to close a
stale `BlockedUnknown` after a crash without double-counting P&L. P&L queries
and the `arbitrage_results` materialized view must continue to consume only
`arbitrage_result`.

The diagnostics dashboard should compare the latest blocked timestamp with
both result and terminal-state evidence. In particular, replace the historical
`result_at = 0` predicate with a latest-state predicate equivalent to:

```sql
SELECT plan_id, blocked_at, result_at, terminal_at, pair_id
FROM
(
    SELECT
        JSONExtractString(payload_json, 'plan_id') AS plan_id,
        maxIf(
            observed_at_ms,
            kind = 'arbitrage_inventory_state'
              AND JSONExtractString(payload_json, 'state') = 'BlockedUnknown'
        ) AS blocked_at,
        maxIf(observed_at_ms, kind = 'arbitrage_result') AS result_at,
        maxIf(
            observed_at_ms,
            kind = 'arbitrage_terminal_state'
              AND JSONExtractString(payload_json, 'state') = 'Balanced'
        ) AS terminal_at,
        argMaxIf(
            JSONExtractString(payload_json, 'pair_id'),
            observed_at_ms,
            kind IN (
                'arbitrage_admitted',
                'arbitrage_result',
                'arbitrage_terminal_state'
            ) AND JSONExtractString(payload_json, 'pair_id') != ''
        ) AS pair_id
    FROM runtime_telemetry
    WHERE kind IN (
        'arbitrage_inventory_state',
        'arbitrage_admitted',
        'arbitrage_result',
        'arbitrage_terminal_state'
    )
      AND observed_at_ms >= toUnixTimestamp64Milli(now64(3, 'UTC') - INTERVAL 30 DAY)
    GROUP BY plan_id
)
WHERE blocked_at > greatest(result_at, terminal_at)
  AND plan_id != '';
```

This keeps a genuinely newer block visible while suppressing a block that has
later durable terminal evidence. Do not union `arbitrage_terminal_state` into
the daily P&L query: it is deliberately replayed on startup.

## Accounting contract

All financial values are signed integer base units represented as decimal
strings. For the current pair token A is USDC with six decimals and token B is
WLD with eighteen decimals.

```text
realized_profit_token_a =
    signed DEX token-A balance delta
  + signed initial CEX token-A balance delta
  + signed recovery token-A balance deltas
  - gas converted to token A at the terminal accounting snapshot

final_token_b_inventory_delta =
    signed DEX token-B balance delta
  + signed initial CEX token-B balance delta
  + signed recovery token-B balance deltas

comparable_profit_token_a =
    realized_profit_token_a
  + conservative token-A mark of the final token-B inventory delta
```

The historical telemetry field remains named
`token_b_residual_base_units`, but it now means the final signed WLD inventory
delta after the primary order and its bounded recovery attempts. A terminal
row may contain a delta of any size. A positive delta is marked at the
persisted Binance bid and a negative delta at the persisted ask. The mark is
prorated by quantity, with assets rounded down and liabilities rounded up.
This value is accounting telemetry only: it never triggers a second automatic
balance order. Aggregate signed and absolute drift are monitored over time; a
separate inventory-balancing design is required if they become material.

Production pays Binance commissions from the account's BNB balance. Recovery
therefore never increases MARKET BUY quantity for a hypothetical WLD
commission; both sides submit the immutable target rounded down to the exchange
step. The actual discounted BNB fee follows the Rails accounting contract:

- the exact negative BNB balance delta is retained in
  `third_asset_deltas`;
- the current in-memory `BNBUSDT` bid is retained in
  `third_asset_prices_token_a`;
- USDT and USDC are treated at numeric parity for this accounting conversion,
  matching `CalculateArbitrageProfitJob`;
- the converted cost is included in the realized token-A PnL but never changes
  sizing, admission, hedge quantity, or recovery.

If the accounting-only BNB feed is unavailable, the executed order and its
residual still reconcile normally. The raw BNB delta remains durable and
`third_asset_valuation_complete=false` identifies the result that must be
revalued later; a missing auxiliary quote must not create Unknown exposure.

DEX gas is recorded separately and subtracted exactly once. Recovery loss is
broken out so a profitable primary spread cannot hide systematically expensive
compensation. `realized_profit_token_a_base_units` remains the settled cash
delta after realized gas and third-asset fee valuation;
`comparable_profit_token_a_base_units` is the criterion metric because it also
carries economically real dust.

The expected fields contain only the raw venue economics that cleared the
20 bps gate. There is deliberately no expected-after-commission,
expected-after-gas, bounded-profit, or forecast-recovery result model. Those
costs appear only when they are realized by execution.

This maps to Rails as follows:

| Rust | Rails `arbitrage_results` |
| --- | --- |
| `comparable_profit_token_a_base_units` | `estimated_profit`, converted to USDC base units |
| `realized_profit_token_a_base_units` | actual token-A deltas plus valued BNB/ETH costs before marking token-B dust |
| DEX/CEX signed deltas in `payload_json` | `token_a_balance_change`, `token_b_balance_change` |
| CEX `third_asset_deltas.BNB` and its price | `bnb_balance_change`, `bnb_price` |
| DEX gas converted to token A | `eth_balance_change * eth_price` contribution |
| execution direction | `scenario` |
| execution mode | no direct Rails field; filter Rust control to `dex_first` |

Rails calls its field `estimated_profit`, but it is computed from actual venue
balance changes after execution and marks residual token B at the latest
Binance bid. Rust uses the admission-time Binance side appropriate to the sign
of the residual. The comparison therefore uses Rust comparable profit, not the
opportunity's expected profit or its cash-only component.

## Equal-window comparison

Use one UTC half-open interval `[start, end)` and the same WLDUSDC pair. For the
Rust control population:

```bash
scripts/compare-arbitrage-results 2026-07-17T13:11:53Z 2026-07-17T14:18:10Z
```

The script validates the timestamp shape, runs the Rails query inside a
read-only transaction, queries ClickHouse telemetry, and prints both aggregates
plus admitted/balanced/blocked counts without exposing either credential. Rust
results are included only when the same `plan_id` has an
`arbitrage_admitted` event inside the requested window. This keeps restart
reconciliation of older journal work out of a new-opportunity comparison
without suppressing its terminal accounting record.

The underlying Rust query is:

```sql
SELECT
    count() AS completed_trades,
    countIf(outcome = 'balanced_profit') AS profitable_trades,
    sum(toInt128(realized_profit_token_a_base_units)) / 1000000 AS cash_realized_usdc,
    sum(toInt128(JSONExtractString(payload_json, 'residual_value_token_a_base_units'))) / 1000000 AS residual_mark_usdc,
    sum(toInt128(JSONExtractString(payload_json, 'comparable_profit_token_a_base_units'))) / 1000000 AS comparable_usdc,
    avg(toInt128(JSONExtractString(payload_json, 'comparable_profit_token_a_base_units'))) / 1000000 AS avg_comparable_usdc_per_trade,
    sum(toInt128(recovery_loss_token_a_base_units)) / 1000000 AS recovery_loss_usdc
FROM arb_bot_prod.arbitrage_results
WHERE pair_id = 'world-chain-usdc-wld'
  AND execution_mode = 'dex_first'
  AND observed_at_ms >= toUnixTimestamp64Milli(toDateTime64({start:String}, 3, 'UTC'))
  AND observed_at_ms <  toUnixTimestamp64Milli(toDateTime64({end:String}, 3, 'UTC'));
```

The operator-side Rails query must use `arbitrage_results.created_at` in the
same interval and pair ID `3`. Rails Postgres remains a local export source; it
is never a Rust runtime dependency or secret.

The goal criterion is evaluated only after at least 100 terminal Rust trades
(stored under the legacy `balanced_profit`/`balanced_loss` outcomes).
Report total and per-trade comparable USDC, with cash realized and the residual
mark shown separately. Also report unknown/halted parent intents separately:
excluding unresolved exposure from PnL does not make it economically harmless.
During the live run, the operator can watch that gate and trigger the same
equal-window comparison automatically:

```bash
scripts/watch-arbitrage-results 2026-07-17T16:05:26Z 100
```

The watcher counts only live `arbitrage_result` rows for `dex_first`; paper
results remain excluded.

Audit the one-to-one admission/result relationship over the same interval:

```sql
SELECT
    uniqExactIf(JSONExtractString(payload_json, 'plan_id'), kind = 'arbitrage_admitted') AS admitted,
    uniqExactIf(JSONExtractString(payload_json, 'plan_id'), kind = 'arbitrage_result') AS balanced,
    uniqExactIf(
        JSONExtractString(payload_json, 'plan_id'),
        kind = 'arbitrage_inventory_state'
        AND JSONExtractString(payload_json, 'state') = 'BlockedUnknown'
    ) AS blocked_unknown
FROM arb_bot_prod.runtime_telemetry
WHERE observed_at_ms >= toUnixTimestamp64Milli(toDateTime64({start:String}, 3, 'UTC'))
  AND observed_at_ms <  toUnixTimestamp64Milli(toDateTime64({end:String}, 3, 'UTC'));
```

An evaluation window is usable after `balanced >= 100` and every counted result
has a matching admission. Report `blocked_unknown` separately and verify that
each unknown retained only its own exact reservation and did not stop later
admissions. Unknown exposure remains economically material, but its mere
presence is not a global comparison or execution dead end.

The frozen 24-hour production example and the interpretation of its cohorts are
in [Rust/Rails comparison on 2026-07-23](rust-rails-comparison-2026-07-23.md).
Architecture changes inferred from comparisons belong in
[Rust production architecture](rust-production-architecture.md), not in this
accounting contract.

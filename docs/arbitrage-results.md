# Comparable arbitrage results

Status: parent paper accounting and runtime integration implemented; live rows pending
Last reviewed: 2026-07-17

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

token_b_residual =
    signed DEX token-B balance delta
  + signed initial CEX token-B balance delta
  + signed recovery token-B balance deltas

comparable_profit_token_a =
    realized_profit_token_a
  + conservative token-A mark of any non-actionable token-B residual
```

A balanced row requires no *actionable* residual: exact zero, or an absolute
residual smaller than the Binance token-B step. Dust remains visible in
`token_b_residual_base_units`. A positive residual is marked down at the
persisted full-depth sell quote net of commission; a negative residual is
marked up at the persisted full-depth buy quote including commission. The mark
is prorated by quantity, with assets rounded down and liabilities rounded up.

Binance commissions must already be reflected in the CEX balance deltas. DEX
gas is recorded separately and subtracted exactly once. Recovery loss is broken
out so a profitable primary spread cannot hide systematically expensive
compensation. `realized_profit_token_a_base_units` remains the settled cash
delta; `comparable_profit_token_a_base_units` is the criterion metric because it
also carries economically real dust.

This maps to Rails as follows:

| Rust | Rails `arbitrage_results` |
| --- | --- |
| `comparable_profit_token_a_base_units` | `estimated_profit`, converted to USDC base units |
| `realized_profit_token_a_base_units` | settled USDC cash component before marking token-B dust |
| DEX/CEX signed deltas in `payload_json` | `token_a_balance_change`, `token_b_balance_change` |
| DEX gas converted to token A | `eth_balance_change * eth_price` contribution |
| execution direction | `scenario` |
| execution mode | no direct Rails field; filter Rust control to `dex_first` |

Rails calls its field `estimated_profit`, but it is computed from actual venue
balance changes after execution and marks residual token B at the latest
Binance bid. Rust uses the admission-time depth quote and, for a token-B
liability, the buy side plus commission. This is at least as conservative. The
comparison therefore uses Rust comparable profit, not the opportunity's
expected profit or its cash-only component.

## Equal-window comparison

Use one UTC half-open interval `[start, end)` and the same WLDUSDC pair. For the
Rust control population:

```bash
scripts/compare-arbitrage-results 2026-07-17T13:11:53Z 2026-07-17T14:18:10Z
```

The script validates the timestamp shape, runs the Rails query inside a
read-only transaction, queries ClickHouse telemetry, and prints both aggregates
plus admitted/balanced/blocked counts without exposing either credential.

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

The goal criterion is evaluated only after at least 100 balanced Rust trades.
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

The 100-trade gate requires `balanced >= 100`, every counted result to have a
matching admission, and `blocked_unknown = 0` at the comparison cutoff.

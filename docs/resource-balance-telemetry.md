# Resource balance telemetry

The live runtime polls operational fee balances once per minute in a dedicated
background task:

- native ETH for every trading wallet location in the compiled network runtime
  (currently World Chain and Arbitrum);
- native ETH for the primary bridge wallet on Optimism;
- free plus locked BNB on the Binance trading subaccount.

Each successful observation is emitted as `resource_balance_snapshot` through
the bounded, non-blocking telemetry channel. ClickHouse materializes successful
records into `resource_balance_snapshots`; failed observations stay in
`runtime_telemetry` with `outcome = 'failed'`. Neither failures nor stale
resource balances affect readiness, admission, sizing, preflight, rebalancing,
or execution.

## Consumption fields

Consumption is the sum of positive balance decreases in the trailing 24-hour
window. Balance increases are treated as refills and do not subtract previously
observed consumption. The same row contains:

- `consumption_24h`: observed consumption inside the rolling window;
- `average_daily_consumption`: the observed consumption normalized to a
  24-hour rate while the initial window is filling;
- `consumption_window_ms` and `consumption_window_complete`: the amount and
  completeness of in-process history behind the calculation.

The tracker deliberately starts a new history window after process restart.
The completeness columns let dashboards distinguish a full 24-hour cohort from
a partial one, while the raw one-minute balance series remains available for a
cross-restart dashboard calculation.

Use `scripts/sql/resource_balances.sql` as the current-balance dashboard query.
Run `arb_bot migrate` before deploying the first revision that emits these
records so the ClickHouse table and materialized view exist.

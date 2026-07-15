# Singapore deployment and ClickHouse cutover

Status: region selected; application deployment pending  
Last reviewed: 2026-07-15

## Decision

- Run the Rust worker in GCP `asia-southeast1` (Singapore).
- Use ClickHouse Cloud on GCP `asia-southeast1`.
- Do not deploy the trading worker in a US region because Binance is not
  available from US infrastructure.
- Keep ClickHouse outside the critical trading path. The process writes to it
  only through the bounded background telemetry channel.

The selected ClickHouse endpoint and password live only in ignored local env
files and GCP Secret Manager. Credential-bearing URLs and passwords must never
be committed or logged.

## Cutover plan

The new Singapore ClickHouse service is independent of the existing pump bot
service in `us-east1`. Do not move or delete the pump bot database as part of
this clone.

1. Create `arb_bot_prod` tables in the Singapore service with `cargo run --
   migrate` using `.env.production`.
2. Write shadow telemetry only to Singapore and verify inserts, batching,
   overflow counters, and retention.
3. If old clone telemetry needs to be retained, copy it asynchronously after
   schema validation. Historical backfill is operational work, never a runtime
   dependency.
4. Update GCP Secret Manager secret versions for `CLICKHOUSE_URL` and
   `CLICKHOUSE_PASSWORD` with `scripts/sync-gcp-secrets`; configure the Worker
   Pool to reference those secrets.
5. Deploy one read-only instance in `asia-southeast1`, confirm Binance and
   ClickHouse health, then retire any obsolete clone-only US deployment.
6. Keep the old ClickHouse service until row counts and required historical
   data are reconciled. The pump bot service is out of scope and stays intact.

## Production checks

- Binance WebSocket connects from the actual Worker Pool egress and remains
  fresh across reconnects.
- Alchemy p50/p95/p99 latency is measured from the same Worker Pool before DEX
  quoting becomes a readiness dependency.
- ClickHouse slowdown or outage increments telemetry drop/failure metrics but
  does not increase market-event queue age or stop the engine.
- No trading secrets are attached while the service is read-only.

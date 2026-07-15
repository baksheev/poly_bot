# Singapore deployment and ClickHouse cutover

Status: read-only production worker deployed and verified
Last reviewed: 2026-07-16

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

## Worker sizing

The initial Worker Pool runs exactly one instance with 8 vCPU and 16 GiB RAM.
This is the largest Cloud Run CPU allocation and intentionally leaves headroom
for Tokio network tasks, TLS, telemetry compression, reconnect recovery, and
the latency-sensitive state owner. Revisit the allocation only after production
CPU and latency histograms are available; cost optimization is secondary to the
first performance baseline.

Deploy a committed revision with:

```bash
scripts/deploy-gcp-worker
```

The script enables the required APIs, creates the Artifact Registry repository
and dedicated runtime service account when absent, synchronizes non-trading
secrets, builds an image tagged with the git SHA, and deploys one read-only
Worker Pool instance. It refuses to deploy a dirty worktree.

## Current production baseline

The verified deployment is Worker Pool `arb-bot-rust-shadow`, revision
`arb-bot-rust-shadow-00004-tvb`, from source revision `c798349c8c8f` and image
digest `sha256:5023049d0c4340f7eff2c2b06bdb12c159fb5eeee63cd7a8405e510e4e242d67`.

- Cloud Run reports the revision `Ready` with one manually scaled instance,
  8 vCPU, 16 GiB RAM, and CPU idle disabled.
- The process hydrated five configured Uniswap pools at World Chain block
  `32407281`, completed its race-free backfill, and
  established filtered Alchemy WebSocket subscriptions.
- The process connected to the Binance Spot raw stream
  `wss://stream.binance.com:9443/ws/wldusdc@bookTicker`; both the market-data
  and execution products in the active domain snapshot are `spot`.
- Before the cache, the fixed production window from `2026-07-15 20:49:18 UTC`
  through `20:52:00 UTC` contained 288 evaluations. In-memory opportunity
  calculation latency was 453 us p50, 560 us p95, 911 us p99, and 1,715 us
  maximum.
- After the cache, the fixed window from `2026-07-15 21:16:50 UTC` through
  `21:21:30 UTC` contained 666 evaluations. Overall calculation latency was
  12 us p50, 25 us p95, 630 us p99, and 1,106 us maximum: 37.8x, 22.4x, and
  1.45x faster at p50, p95, and p99 respectively.
- Of those evaluations, 628 were fully warm: 11 us p50, 19 us p95, 51 us p99,
  and 94 us maximum. Compared with the pre-cache distribution, the warm path
  is 41.2x, 29.5x, and 17.9x faster at p50, p95, and p99.
- The remaining 38 evaluations followed an applied DEX event and recomputed at
  least one invalidated entry. They measured 25 us p50, 792 us p95, 1,106 us
  p99/max. Across all 6,550 cache lookups, 6,382 hit and 168 missed (97.4% hit
  rate). These visible, state-driven recomputations explain the overall p99;
  capacity-search quotes remain deliberately uncached.
- This calculation timer measures the complete two-direction, five-pool
  evaluation including conditional capacity search, but excludes network
  latency and telemetry insertion. Overall decision latency in the post-cache
  window was 100 us p50, 235 us p95, and 715 us p99.
- No Worker Pool warning or error logs appeared during the startup check.
- No wallet, signing, or Binance trading credentials are attached.

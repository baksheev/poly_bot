# poly_bot

Autonomous low-latency Rust arbitrage runtime built beside the Rails application
at `/Users/baksheev/code/arb_bot`.

The authoritative description of production behavior, safety invariants, and
change-review rules is [Rust production architecture](docs/rust-production-architecture.md).
Read it before changing market data, opportunity selection, execution,
settlement, reservations, rebalancing, or deployment.

## Production

- One application Pod runs on the fixed `c4-highcpu-8` node in the private
  zonal GKE Standard cluster `arb-bot` in `asia-southeast1-b`.
- The production runtime uses the reviewed v12 adaptive-live domain artifact
  with arbitrage and rebalancing in `full_live`.
- The stopped `arb-bot-rust-shadow-gce` VM is a rollback target only. It must
  never run while the GKE Deployment has a nonzero replica count.
- Rails runs independently with separate mutable state. Rust never reads Rails
  Postgres or Redis and never calls Rails services at runtime.
- ClickHouse is an asynchronous telemetry and journal sink, not part of the
  trading path.

The live path is:

```text
Binance book/depth + World Chain pool state
                  ↓
       in-memory opportunity engine
                  ↓
       latest-wins pending candidate
                  ↓
     admission, reservation, preflight
                  ↓
     DEX swap → Binance IOC → recovery
                  ↓
       accounting and reconciliation
```

Production stays DEX-first. Adaptive sizing may use up to 200 USDC from
sequence-matched or sufficiently recent full depth; top-of-book-only sizing is
capped at 40 USDC. The profitability gate is the configured 20 bps spread.
Inventory uses the exact execution envelope without the legacy Rails `3x`
multiplier. Binance price readiness uses a 30-second maximum transport silence;
an unchanged top remains current while the WebSocket heartbeat is fresh.

## Local commands

```bash
cp .env.example .env
cargo run -- check
cargo run -- hydrate
cargo run -- run
scripts/quality.sh
```

`hydrate` loads V3/V4 pool state at one canonical block and exits without
starting execution. Without `CLICKHOUSE_URL`, `run` uses log-only telemetry.
Create the current telemetry schema with:

```bash
cargo run -- migrate
```

Local paper execution remains available as a test harness:

```bash
ARBITRAGE_EXECUTION_MODE=paper_dex_first \
ARBITRAGE_TRADE_JOURNAL_PATH=./tmp/arbitrage-trades.jsonl \
cargo run -- run
```

Use `./scripts/gcloud-local` for every local Google Cloud command. Rails
Postgres is available only to ignored, operator-side comparison tooling:

```bash
scripts/read-rails-pair-config WLDUSDC
scripts/compare-arbitrage-results START_UTC END_UTC
```

## Delivery

Routine production revisions go directly through
`.github/workflows/deploy-gke.yml` on `main`. The workflow runs CI, builds and
pushes the image, resolves its immutable digest, deploys it to the existing
fixed node, and verifies the rollout. Do not build production images locally,
restart the GCE rollback VM, or mutate the GKE application deployment from a
workstation.

## Documentation map

- [Rust production architecture](docs/rust-production-architecture.md) —
  canonical production decisions and review checklist.
- [Trading runbook](docs/trading-runbook.md) — stop, recovery, rollout, and
  rollback operations.
- [GKE deployment](docs/gke-deployment.md) — production delivery topology.
- [Versioned domain configuration](docs/domain-configuration.md) — reviewed
  strategy artifact and startup validation.
- [Binance runtime](docs/binance-runtime.md) and
  [Uniswap execution](docs/uniswap-execution.md) — venue-specific behavior.
- [Adaptive arbitrage sizing](docs/adaptive-arbitrage-sizing.md) — sizing
  algorithm and rollout evidence.
- [Comparable arbitrage results](docs/arbitrage-results.md) — accounting and
  equal-window comparison contract.
- [24-hour Rust/Rails comparison](docs/rust-rails-comparison-2026-07-23.md) —
  frozen production evidence used for the current priorities.
- [Binance price telemetry baseline](docs/binance-price-telemetry-2026-07-23.md)
  — frozen evidence for the locked strategy-price path.
- [Concurrent execution](docs/concurrent-execution.md) — separately gated
  experiment; DEX-first remains the production control.
- [Release changelog](CHANGELOG.md) — implementation history.

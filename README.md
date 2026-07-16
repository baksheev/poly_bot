# arb_bot Rust migration

Low-latency Rust replacement for the existing Rails application at
`/Users/baksheev/code/arb_bot`.

The new service is built beside the existing bot, one component at a time.
Rails keeps running unchanged while the Rust service grows into a complete,
autonomous clone. The two runtimes do not share mutable state or delegate live
work to one another.

## Target architecture

```text
Binance WebSocket / balance REST ──┐
Alchemy pool-log / newHeads WSS ───┼─> one Rust process ─> in-memory state
Alchemy HTTP hydration / balance ──┘          │                 │
                                      │                 ├─> opportunity engine
                                      │                 ├─> risk / inventory
                                      │                 └─> execution (later)
                                      │
                                      └─> bounded async queue ─> ClickHouse
```

The trading path does not read from Postgres or ClickHouse and does not cross
a job queue. Network clients, parsed configuration, market state, balances,
reservations, nonces, and execution context are process-scoped and reused.
ClickHouse is an asynchronous telemetry and journal sink only.

## Clone strategy

Each component is cloned and verified independently: configuration, Binance
market data, DEX quoting, opportunity math, wallet state, DEX execution,
Binance execution, recovery, and finally rebalancing. The existing Rails code
is a behavioral specification and fixture source, not a runtime dependency.

The first useful chain of components reproduces the current `USDC-WLD` /
`WLDUSDC` loop in read-only mode:

1. Binance `bookTicker` WebSocket.
2. In-memory Uniswap V3/V4 pool mirrors and local exact-input quotes.
3. Both arbitrage calculations in memory using fixed-point values.
4. ClickHouse capture and deterministic replay.

After the full clone passes paper and recovery tests, it receives separate EVM
wallets, a separate Binance account/API key and separate inventory. It can then
be enabled with a small isolated capital budget while the Rails bot continues
to run.

See [the migration design](docs/arb-bot-rust-migration.md) for the current
Rails flow, ownership boundaries, stages, safety gates, and acceptance
criteria.

## Current status

The first clone components are implemented in read-only shadow mode: persistent
Binance Spot `WLDUSDC@bookTicker` WebSocket ingestion, exact decimal
parsing, reconnect generations, freshness/readiness state, a single in-memory
state owner, and non-blocking ClickHouse telemetry. Startup now loads a
fail-closed, versioned snapshot of the active production World Chain
`USDC-WLD` configuration and reports its SHA-256 fingerprint. It has
authenticated Binance account-read credentials, but no wallet signer or live
trading gate, and cannot place orders. The DEX slice includes pinned-block
V3/V4 hydration, an HTTP log backfill after WSS
subscription, ordered Alchemy `logs`/`newHeads` ingestion, and the shared
hookless concentrated-liquidity calculation core. Authenticated Binance free
and locked balances are refreshed once per second in a dedicated task. Wallet
native and ERC-20 balances are refreshed at every accepted World Chain
`newHeads` notification, with all token calls pinned to the same block hash.
Both snapshots live under the single state owner and are readiness inputs; the
network requests never run in the quote hot path. Only the public
`EVM_WALLET_ADDRESS` is required for this read-only observer. `Swap`, `Mint`,
`Burn`, and `ModifyLiquidity` update the same single-owner pool mirrors used by
local quotes; no RPC call is made on the event or quote hot path.

Every accepted Binance update now evaluates both arbitrage directions against
all hydrated pools. The read-only opportunity engine uses exact-output DEX
quotes when buying WLD, exact-input quotes when selling WLD, applies the
configured 20 bps threshold and 4 bps reserve, and estimates the largest
step-aligned WLD size supported by current DEX liquidity and the observed
Binance top of book. It writes every evaluation and threshold-crossing
opportunity asynchronously to ClickHouse. Market data and eventual execution
now both use Spot. The estimate is still not execution-ready because
bookTicker exposes only the best level; deeper Spot liquidity, fees, gas,
deeper Spot liquidity, reservations, and executable inventory limits are not
integrated yet.

Temporary infrastructure identifiers still use the original `poly_bot`
bootstrap names:

- GitHub remote: `baksheev/poly_bot`;
- GCP project: `poly-bot-502515`;
- runtime region: `asia-southeast1` (Singapore);
- ClickHouse region: GCP `asia-southeast1` (Singapore).

The GCP project and repository keep their bootstrap names for now. See
[Singapore deployment and ClickHouse cutover](docs/singapore-infrastructure.md).

## Local setup

```bash
cp .env.example .env
cargo run -- check
cargo run -- hydrate
cargo run -- run
```

`hydrate` requires the endpoint named by the domain snapshot
(`ALCHEMY_WORLDCHAIN_RPC_URL` for the current pair). It discovers and loads all
V3/V4 pool state at one canonical block, logs only public pool metadata, and
exits without starting market data or execution.

To use ignored production-local configuration explicitly:

```bash
ENV_FILE=.env.production cargo run -- hydrate
```

Without `CLICKHOUSE_URL`, `run` uses log-only telemetry. To create the current
telemetry table in a configured ClickHouse instance:

```bash
cargo run -- migrate
```

`run` requires `EVM_WALLET_ADDRESS`, Binance read credentials, and the World
Chain HTTP/WSS endpoints. Balance sync cadence and freshness are controlled by
`BALANCE_SYNC_INTERVAL_MS` (default `1000`) and `BALANCE_MAX_AGE_MS` (default
`5000`). See [balance synchronization](docs/balance-synchronization.md).

The committed domain snapshot is documented in
[versioned domain configuration](docs/domain-configuration.md). A local,
read-only comparison with the current Rails production pair is available when
`ARB_BOT_DATABASE_URL` is set in ignored `.env.production`:

```bash
scripts/read-rails-pair-config WLDUSDC
```

Quality gate:

```bash
scripts/quality.sh
```

Use `./scripts/gcloud-local` for all local Google Cloud commands. Its
repository-local configuration does not mutate the global gcloud account,
project, or ADC state. See [local GCP authentication](docs/gcp-local-auth.md).

Production is a single read-only Compute Engine VM in Singapore. The current
machine is `c4-highcpu-8` (8 dedicated vCPUs, 16 GiB RAM) in
`asia-southeast1-b`, running Container-Optimized OS and a digest-pinned image.
Provision a new VM from an already published image with
`scripts/create-gce-worker IMAGE SOURCE_REVISION`. ClickHouse and Alchemy
credentials remain in Secret Manager and are read at boot through the attached
service account.

Authenticated manual Binance diagnostics use the separate IAP-only VM with
static egress IP `34.143.148.4`; do not open SSH on or run ad hoc commands from
the production trading VM. The repository wrapper accepts only read-only
commands and never places an order or submits a withdrawal:

```bash
scripts/gce-binance-test binance-account
scripts/gce-binance-test binance-capital
scripts/gce-binance-test binance-recent-validation-orders --limit 20
```

See [Singapore infrastructure](docs/singapore-infrastructure.md) for image
updates, IAM boundaries, and the complete diagnostic runbook.

## Planned implementation slices

1. Prove local quote parity against V3/V4 Quoter calls sampled outside the hot
   path and add release-mode latency/allocation benchmarks.
2. Derive token-B sizing from the in-memory Binance ask and calculate both DEX
   directions locally on every accepted market update.
3. Port both `profit_token_a` opportunity calculations as pure fixed-point
   functions and run
   synchronized shadow comparisons.
4. Add in-process WSS reconnect/gap repair; the current fail-closed path exits
   on a DEX discontinuity so systemd restarts the container from a fresh
   snapshot.
5. Extend the isolated account/wallet snapshots with reservations, allowances,
   open orders, pending nonces, then add paper execution and forced recovery
   tests before live trading is enabled.

See [the local DEX quoting design](docs/low-latency-dex-quoting.md) for the
hot-path contract, hydration boundary, reorg handling, and latency budget.
See [the concurrent execution design](docs/concurrent-execution.md) for parallel
DEX/CEX dispatch, orphan-leg recovery, exposure accounting, and paper rollout.

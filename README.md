# arb_bot Rust migration

Low-latency Rust replacement for the existing Rails application at
`/Users/baksheev/code/arb_bot`.

The new service is built beside the existing bot, one component at a time.
Rails keeps running unchanged while the Rust service grows into a complete,
autonomous clone. The two runtimes do not share mutable state or delegate live
work to one another.

## Target architecture

```text
Binance WebSocket ─────────┐
Alchemy pool-log WSS ──────┼─> one Rust process ─> in-memory state
Alchemy HTTP recovery ─────┘          │                 │
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

## Archived experiment: USDT/AED

The initial OKX snapshot showed only about 6.6 bps gross edge before fees,
versus roughly 17.5 bps for two taker legs on a regular account. That direction
is not being implemented. The evidence is retained in
[USDT/AED arbitrage validation](docs/usdt-aed-arbitrage-validation.md).

## Current status

The first clone components are implemented in read-only shadow mode: persistent
Binance USD-M Futures `WLDUSDC@bookTicker` WebSocket ingestion, exact decimal
parsing, reconnect generations, freshness/readiness state, a single in-memory
state owner, and non-blocking ClickHouse telemetry. Startup now loads a
fail-closed, versioned snapshot of the active production World Chain
`USDC-WLD` configuration and reports its SHA-256 fingerprint. It has no private
Binance, wallet, signing, or trading credentials and cannot place orders. The
DEX slice includes pinned-block V3/V4 hydration, an HTTP log backfill after WSS
subscription, ordered Alchemy `logs`/`newHeads` ingestion, and the shared
hookless concentrated-liquidity calculation core. `Swap`, `Mint`, `Burn`, and
`ModifyLiquidity` update the same single-owner pool mirrors used by local
quotes; no RPC call is made on the event or quote hot path.

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

Production is a single read-only Cloud Run Worker Pool instance in Singapore
with 8 vCPU and 16 GiB RAM. After committing the exact revision, deploy it with
`scripts/deploy-gcp-worker`; the script builds a git-SHA-tagged image and keeps
all ClickHouse and Alchemy credentials in Secret Manager.

## Planned implementation slices

1. Prove local quote parity against V3/V4 Quoter calls sampled outside the hot
   path and add release-mode latency/allocation benchmarks.
2. Derive token-B sizing from the in-memory Binance ask and calculate both DEX
   directions locally on every accepted market update.
3. Port both `profit_token_a` opportunity calculations as pure fixed-point
   functions and run
   synchronized shadow comparisons.
4. Add in-process WSS reconnect/gap repair; the current fail-closed path exits
   on a DEX discontinuity so the Worker Pool restarts from a fresh snapshot.
5. Add isolated account/wallet hydration, then paper execution and forced
   recovery tests before any live credentials are provisioned.

See [the local DEX quoting design](docs/low-latency-dex-quoting.md) for the
hot-path contract, hydration boundary, reorg handling, and latency budget.

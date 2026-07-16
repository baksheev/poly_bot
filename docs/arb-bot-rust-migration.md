# Side-by-side Rust clone of arb_bot

Status: implementation started; Binance public market data first  
Last reviewed: 2026-07-15

## Decision

Build a complete Rust clone beside `/Users/baksheev/code/arb_bot`, one
component at a time. Do not progressively replace Rails jobs in production and
do not make the Rust service depend on Rails, its Postgres database, or its
cache.

The existing Rails bot continues operating independently. After the Rust clone
reaches functional parity and passes paper/recovery testing, give it separate
wallets, a separate Binance account, separate inventory, and a small capital
budget. Run it as an independent live canary. Stopping the clone must never
require changing the original bot.

## Why this topology

- component work cannot destabilize the existing live bot;
- ownership is unambiguous: a wallet, nonce, order, and balance belong to
  exactly one bot;
- the clone can be stopped instantly without a distributed rollback;
- Rust architecture is not constrained by Rails queues and persistence models;
- comparisons are possible without creating a hybrid execution system;
- live behavior can be tested with real but isolated capital before deciding
  whether to retire anything in Rails.

## Current Rails component map

Repository inspection on the review date found this shape:

```text
recurring scheduler (1 second)
  -> UpdateTradingPairsPricesJob
      -> Binance REST price snapshot
      -> Binance Futures bookTicker relay through Rails.cache
      -> Alchemy RPC Multicall3 quotes for Uniswap V3/V4
      -> optional 0x quotes
  -> JobBatchService
  -> DetectArbitrageJob
      -> Postgres snapshots and attempt
      -> opportunity math for both directions
      -> wallet selection and reservation
  -> PerformArbitrageJob
      -> DEX swap
      -> Binance limit hedge, then market fallback
  -> hedge recovery, result calculation, and rebalancing jobs
```

The Rust clone reproduces behavior, not the Rails topology. Scheduler, queue,
cache, and per-cycle Postgres reads/writes collapse into a single in-memory
runtime.

## Target runtime

```text
Binance public/private WS ──┐
Alchemy WSS/HTTP recovery ──┼─> adapters ─> single state owner
Binance REST control API ───┘                    │
                                                ├─> quotes / opportunity
                                                ├─> risk / reservations
                                                ├─> DEX then CEX execution
                                                └─> recovery / reconciliation
                                                          │
                                                          └─> async ClickHouse
```

Network readers can run in separate Tokio tasks. All latency-sensitive mutable
strategy state has one logical owner. ClickHouse receives telemetry and state
journals through bounded background channels and is never queried for a trade
decision.

## Component build order

Each component has a typed interface, protocol fixtures, golden behavior tests,
health/readiness state, and ClickHouse observability before the next component
depends on it.

### 1. Domain configuration

Clone the effective behavior of `Chain`, `Token`, `TradingPair`, provider
settings, Binance filters, quote sizes, thresholds, fee tiers, pool configs,
and strategy selection into a versioned non-secret configuration snapshot.

The Rust runtime owns its copy. It does not query Rails Postgres. A small export
tool may produce a snapshot from Rails for operator review, but the production
Rust process only consumes the validated artifact.

Definition of done:

- canonical token/pair identity and decimal rules are explicit;
- unknown fields/versions fail closed;
- config has a stable hash/version included in every decision;
- secrets and credential-bearing RPC URLs are not stored in the snapshot.

Implementation status: the v1 World Chain `USDC-WLD` snapshot, strict loader,
SHA-256 identity, production whitelist query, and Binance subscription wiring
are complete. Account/wallet/rebalance fields stay out until their owning
components are implemented.

### 2. Telemetry and deterministic replay

Define normalized envelopes for input, state transition, decision, command,
acknowledgement, error, and recovery events. Add an `engine_id` that uniquely
identifies Rails comparison data, Rust shadow runs, and each live clone.

Definition of done:

- a captured input sequence produces identical Rust decisions on replay;
- ClickHouse loss or latency cannot delay market-data processing;
- telemetry overflow is visible and makes a parity run invalid rather than
  silently looking complete.

### 3. Binance public market data

Replace `BinancePriceRelay`, `BinancePriceReader`, and per-cycle REST price
fetches with a persistent typed Spot `@bookTicker` WebSocket stream. This is an
explicit divergence from the current Futures relay: the signal and eventual
hedge must observe the same Spot market. Maintain exchange ID/time, monotonic
receive time, reconnect generation, and freshness.

Definition of done:

- connection rotation/reconnect has no unexplained silent gap;
- malformed, crossed, stale, and regressed updates fail closed;
- decimal strings are parsed directly into fixed-point values;
- captured output is comparable with Rails relay fixtures.

### 4. DEX quote adapters

Clone the candidate set and best-output behavior of
`Dex::UniswapPoolBatchQuoteService`, but remove RPC from the decision path.
Hydrate V3/V4 concentrated-liquidity state at one pinned block, maintain it
from ordered Alchemy WSS logs, and calculate every candidate locally in both
directions. Reusable HTTP RPC and Quoter calls are recovery and sampled parity
tools only.

Add 0x as a separate provider component after V3/V4 parity. Provider failures
remain isolated and explicit.

Definition of done:

- local integer output matches V3/V4 Quoter results at the same block and input;
- token base units, block identity, pool-state fingerprint, route metadata, and
  selected candidate are recorded;
- missing ticks, gaps, reorgs, and no-liquidity cases fail closed;
- no network call, lock, or allocation occurs in the steady-state quote loop;
- HTTP/WSS clients and ABI metadata are reused, not rebuilt per event.

### 5. Opportunity engine

Extract `DetectArbitrageJob` economics into pure Rust domain functions:

- quote freshness and availability;
- executable CEX bid/ask and DEX effective prices;
- both arbitrage directions;
- fees, gas, slippage reserve, exchange filters, and rounding;
- configured inventory strategy and reason/status codes.

Definition of done:

- no I/O, clocks, globals, or JSON inside the calculation core;
- all financial math uses checked fixed-point/integer representations;
- golden cases cover opportunity/no-opportunity and every rejection reason;
- mismatches against Rails are classified as input, config, rounding,
  selection, freshness, or formula differences.

At this point the composed clone can run read-only for `USDC-WLD` /
`WLDUSDC`, capture inputs, and compare decisions without any credentials.

### 6. Account and wallet state

Implemented foundation: Binance free/locked balances are refreshed once per
second in a background task, and World Chain native/ERC-20 balances are
refreshed on Alchemy `newHeads` with every token read pinned to one block hash.
The public wallet address and reusable RPC clients are separate from signing.
Both observations are held in memory and gate readiness by freshness.

Build independent read-only hydration for:

- allowances for each clone wallet;
- gas balances and pending nonces on every chain;
- Binance filters and open orders;
- in-memory wallet lanes and reservations.

The blockchain and Binance are the recovery sources of truth after restart.
ClickHouse is the audit journal, not a transactional lock service.

### 7. DEX transaction component

Clone V3, V4, and later 0x calldata/signing/submission behavior behind a paper
transport first. Model submitted, known-mined, known-reverted, and unknown
outcomes separately. Never reuse a nonce until reconciliation proves it safe.

### 8. Binance order component

Clone symbol filter validation, quantity/price rounding, limit hedge, immediate
market fallback, order query, fill reconciliation, and idempotent client order
IDs. Use a mock/paper transport before attaching the clone account.

### 9. Trade orchestrator and recovery

Compose the exact existing invariant first: DEX leg, then Binance hedge using
the actual DEX received amount when quantity semantics match. Preserve:

- one active swap lane per wallet;
- durable command IDs and unknown-outcome reconciliation;
- immediate market hedge fallback;
- retry/recovery for a completed DEX leg with a failed Binance hedge;
- restart hydration before new entries are enabled.

Changing leg ordering is a separate strategy design, not part of cloning.
After this control mode reaches paper parity, implement the concurrent mode
behind the same coordinator and compare them through the randomized protocol in
[the concurrent execution design](concurrent-execution.md). The external Rails
bot is an operational benchmark, not the statistical control.

### 10. Rebalancing and operations

Only after trading/recovery parity, clone the components required for autonomy:
balance snapshots, deposits, withdrawals, bridges, travel-rule flow, sweep,
investment, operator health, and emergency controls. These are not latency
critical, but they are required before the clone can manage capital without
manual intervention.

## Isolation for the live clone

The canary must use its own:

- EVM wallet on every supported chain and therefore its own nonce space;
- Binance account or sub-account, API key, open orders, and balances;
- capital allocation and per-order/daily/inventory risk limits;
- Alchemy application/key and quota budget where practical;
- GCP service account and Secret Manager secret versions;
- `engine_id`, execution namespace, client order ID prefix, and ClickHouse
  records;
- alerting, kill switch, recovery queue, and operator runbook.

No wallet, private key, Binance account, reservation, nonce, open order, deposit
address workflow, or rebalance operation is shared with the Rails bot.

Both bots still trade the same public markets, so separate accounts do not
eliminate strategy interaction. The canary starts with minimum size, a hard
capital cap, and either an assigned pair/time window or an explicitly accepted
risk that both bots may act on the same opportunity.

## Live enablement sequence

1. Run individual components against fixtures.
2. Compose read-only market/quote/opportunity clone.
3. Run at least 24 hours of synchronized capture and deterministic replay.
4. Run the complete clone with paper transports and forced failures.
5. Provision isolated wallets/account/secrets with small balances.
6. Hydrate and reconcile all state while entries remain disabled.
7. Enable one pair, minimum size, low daily limit, and automatic fail-closed
   gates.
8. Compare realized execution, hedge latency, PnL, failures, and recovery with
   the old bot without sharing state; separately run the predeclared randomized
   Rust `dex_first` versus `concurrent_hedged` experiment.
9. Increase scope only through explicit limit/config changes.

Stopping the canary means disabling its entry gate and reconciling its own
positions/orders. The Rails bot continues normally.

## Financial representation

- Token amounts are integers in base units (`U256` or checked equivalent).
- Binance decimal strings are parsed to scaled integers using instrument
  metadata.
- Profitability uses checked integer/rational or validated decimal arithmetic
  with explicit rounding direction.
- `f64` is forbidden in quote selection, risk, sizing, threshold, order, and
  PnL math.

## Region decision

The selected runtime region is GCP `asia-southeast1` (Singapore). US regions
are excluded because Binance is unavailable from US infrastructure. ClickHouse
is also provisioned in GCP `asia-southeast1`, but remains an asynchronous sink
and never participates in a trade decision.

Before live canary, benchmark the exact worker image against Binance market and
order endpoints, Alchemy RPC, block propagation, transaction submission, and
receipt observation for every target chain. Region selection is settled for
the first deployment; measurements still drive later network and instance
tuning.

## First implementation milestone

The first executable slice now provides:

- no remaining Polymarket runtime types or endpoints;
- fixed-point Binance top-of-book values with no `f64` strategy data;
- persistent read-only Binance Spot `bookTicker` for configured symbols;
- reconnect generations, stale/regressed update rejection, and readiness based
  on connection plus quote freshness;
- a single in-memory state owner and bounded asynchronous ClickHouse telemetry;
- protocol fixtures and state/config tests with no execution dependencies.

Versioned pair/chain configuration and deterministic replay remain part of the
next domain-configuration slice. Alchemy, strategy calculations, wallets, and
live keys are not included yet.

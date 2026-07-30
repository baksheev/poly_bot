# Multi-pair, multi-network trading runtime

Status: **proposed migration specification**
Last reviewed: 2026-07-29
Applies to: Binance account ownership, EVM network runtimes, pool state,
strategy scheduling, inventory, execution, recovery, and rebalancing

This proposal is subordinate to
[`rust-production-architecture.md`](rust-production-architecture.md). It
describes how to evolve the current single-pair bootstrap into one process that
can safely trade 10–20 pairs on several EVM networks from one Rust-owned Binance
subaccount and, initially, one EVM signer.

The migration must preserve the current WLD/USDC World Chain v12 production
behavior at every milestone. ESP/USDC Arbitrum remains read-only until the
milestone that explicitly authorizes a bounded live canary.

## Decision summary

The target runtime has five ownership layers:

1. one `HotPathDecisionOwner` directly polls Binance strategy-price streams,
   applies ordered DEX events, owns local pool mirrors and strategy state, and
   immediately evaluates only the affected strategies;
2. one `BinanceAccountRuntime` supervises the shared Rust subaccount
   infrastructure while separate internal owners isolate public market data,
   authenticated account state, order execution, rate limits, and capital
   operations;
3. one `NetworkRuntime` owns the reusable I/O and read coordination for each EVM
   network;
4. one `PortfolioOwner` atomically owns observed inventory, reservations, and
   account-wide capital/risk allocation;
5. one `EvmExecutionOwner` per `(chain_id, wallet_id)` owns every signed
   transaction and nonce on that lane, whether it belongs to a trade,
   allowance, transfer, bridge, or rebalance.

The first production topology uses:

- one shared Rust-owned Binance Spot subaccount for all configured pairs;
- one Binance market-data and authenticated-account infrastructure stack;
- one account-wide capital allocator and durable rebalance saga owner;
- one configured EVM signer, reused across networks but with independent nonce
  space on every chain;
- one single-owner EVM executor per `(chain_id, wallet_id)`;
- one local pool mirror per configured Uniswap pool;
- no Postgres, Rails, Redis, or ClickHouse dependency in the trading path.

The rule is **one owner per external mutation namespace**, not one subaccount or
wallet per strategy.

There is no separate production paper milestone for ESP/USDC. Implementation
still starts behind the repository's non-mutating execution mode and explicit
live-trading gate, but that mode is used only for deterministic tests,
read-only production validation, and preflight verification. Once its exit
criteria pass, the next rollout is the bounded live canary rather than an
extended paper observation period.

## Goals

- Add pairs without creating another Binance account runtime, User Data Stream,
  balance cache, rate limiter, journal owner, or rebalancer.
- Add EVM networks without duplicating strategy logic or allowing two tasks to
  allocate the same nonce.
- Subscribe to and hydrate all configured pools once per network.
- Use network-level Multicall3 batches for bootstrap, reconciliation, wallet
  balance reads, and sampled Quoter parity where appropriate.
- Keep executable DEX quotes local and synchronous after startup.
- Let strategies evaluate independently while all external mutations remain
  serialized by their real ownership boundary.
- Reserve shared Binance assets atomically across every pair.
- Route every on-chain mutation for one wallet location through the same signer,
  nonce allocator, gas policy, and transaction journal.
- Preserve deterministic recovery after process restart.
- Leave a clean extension point for multiple wallets without implementing that
  extension in the first migration.

## Non-goals

- Running one OS process or Pod per pair.
- Requiring one Tokio task or OS thread per strategy before profiling proves
  that sharding the decision owner is necessary.
- Assigning one Binance subaccount to every pair.
- Adding a second production replica or distributed coordinator.
- Using Multicall, Quoter, RPC, Postgres, or ClickHouse in opportunity
  evaluation or entry preflight.
- Enabling ESP/USDC trading as part of the structural refactor.
- Implementing multi-wallet allocation, wallet selection, or capital routing
  in the first version.
- Changing the reviewed WLD/USDC economics, adaptive sizing, DEX-first ordering,
  recovery behavior, or live thresholds.

## Target topology

```text
Binance strategy-price sockets ───────────────────────────────────────┐
Network WSS logs/heads ─> ordered canonical events ───────────────────┤
                                                                      v
                                                          HotPathDecisionOwner
                                                     pool mirrors + strategy state
                                                               │ candidates
                                                               v
                                                        CandidateScheduler
                                                               │
                                                               v
                                                          PortfolioOwner
                                                     admission + reservation
                                                               │
                                                               v
                                                            TradeSaga
                                              ┌────────────────┴───────────────┐
                                              v                                v
                                  BinanceOrderExecutionOwner       EvmExecutionOwner
                                                                   per chain/wallet

Binance REST/User Data ─> BinanceAccountStateOwner ─────────> PortfolioOwner
Network HTTP/Multicall ─> NetworkReadCoordinator ───────────> hydration/balances
CapitalAllocator ───────> RebalanceSaga ────────────────────> same venue owners
```

The diagram omits individual evaluator instances because strategies have no
mutation authority. The registries and identifiers must not encode a two-pair
limit.

## Identity and ownership model

Every stateful component uses explicit typed identities:

| Identity | Meaning | Initial cardinality |
| --- | --- | --- |
| `BinanceAccountId` | One authenticated Spot account and order namespace | 1 |
| `InstrumentId` | Binance account plus Spot symbol | 2 |
| `StrategyId` | Versioned pair strategy and its execution policy | 2 |
| `NetworkId` | EVM `chain_id` | 2 |
| `WalletId` | Stable configured signer identity, not an address string alias | 1 |
| `WalletLocation` | `(chain_id, wallet_id)` | 2 |
| `PoolId` | `(chain_id, protocol, address-or-v4-pool-id)` | all configured pools |
| `ExecutionLaneId` | `(chain_id, wallet_id)` | 2 |
| `VenueAssetId` | Exact Binance asset or chain/token-contract identity | all configured assets |
| `EconomicAssetId` | Capital-policy identity joining reviewed representations | configured currencies |

The same private key produces the same address on World Chain and Arbitrum, but
the two wallet locations have independent balances, gas policies, RPC health,
journals, and nonce sequences. They must never share a wallet inventory key or
nonce lane.

### Single-owner rules

- `HotPathDecisionOwner` is the only writer of executable Binance price state,
  local pool mirrors, prepared curves, and strategy calculation state.
- `BinanceAccountStateOwner` is the only writer of authenticated Binance account
  state.
- `BinanceOrderExecutionOwner` is the only component allowed to place or
  reconcile Spot orders for the configured account.
- One `NetworkRuntime` is the only owner of canonical network ingestion,
  hydration, gap repair, and block-pinned read scheduling for its chain. It
  delivers ordered events to the hot-path owner, which applies them to the local
  mirrors.
- One `EvmExecutionOwner` is the only component allowed to sign, broadcast, or
  reconcile any transaction for an `ExecutionLaneId`.
- `PortfolioOwner` is the only writer of observed inventory, reservations, and
  capital allocations.
- `CapitalAllocator` is the only component allowed to propose movement between
  the Binance account and wallet locations. A durable `RebalanceSaga` routes its
  child mutations through the normal Binance and EVM venue owners.
- Strategies are synchronous evaluators owned by the hot-path runtime. They
  never perform I/O, place orders, reserve funds, select nonces, rebalance, or
  mutate pool state.

## Binance account runtime

One account runtime serves all configured symbols and assets, but it is a
supervision and shared-infrastructure boundary rather than one large event loop.
It contains:

- `BinanceMarketDataIngress`, which owns the derived set of public Spot
  subscriptions and exposes their socket futures directly to the
  `HotPathDecisionOwner`;
- `BinanceAccountStateOwner`, which owns the authenticated WebSocket API
  session, User Data subscription, account snapshots, open-order observations,
  filters, commissions, and clock synchronization;
- `BinanceOrderExecutionOwner`, which owns deterministic client-order IDs,
  placement, order journals, fill reconciliation, and recovery-required orders;
- `BinanceRateLimitGovernor`, which accounts for request weight and order limits
  across every symbol and authenticated client;
- `BinanceCapitalSagaOwner`, which owns only the Binance transfer, deposit, and
  withdrawal children of account-wide rebalancing.

The children reuse process-scoped connection pools and account metadata. They
have separate bounded queues and failure handling so a slow withdrawal,
commission refresh, or REST reconciliation cannot delay strategy-price parsing
or opportunity evaluation.

The public connection set is one account-level facility, not necessarily one
physical socket. The domain compiler deterministically assigns symbols to a
small bounded set of combined streams based on measured message rate and
Binance connection limits. Connection generation and liveness remain
per-stream and per-symbol. Adding a noisy symbol must not create an unbounded
queue or force unrelated symbols onto a failed connection shard.

Public price state remains per symbol. Account balances, BNB commission
inventory, rate limits, open orders, and recovery state are shared.

An unavailable or stale symbol degrades only strategies that require that
symbol. An unavailable authenticated account snapshot prevents new execution
for every strategy because shared inventory is then unknown. Existing recovery
continues with priority.

### Binance order concurrency

The first migration preserves the current globally serialized trade
coordinator. This minimizes behavioral change while WLD/USDC remains live.

The interfaces must still permit a later reviewed increase in concurrency:

- reservations are operation-scoped rather than lane-global;
- client order IDs include the strategy and parent operation identity;
- the Binance owner can reconcile several known orders;
- global exchange rate limits remain enforced across all symbols.

Parallel order placement is not enabled merely because strategies run in
separate evaluators or are later assigned to separate decision shards.

The trading/order credential and the master treasury credential remain
capability-separated. The order owner cannot withdraw or perform master-account
capital operations. The capital saga owner cannot expose treasury credentials
to strategies or the order-placement path. Both remain under one Binance
account infrastructure and rate-limit policy without sharing mutation
authority.

## Network runtime

The process creates exactly one `NetworkRuntime` for every enabled `chain_id`.
It owns reusable network-scoped resources:

- Alchemy or equivalent HTTP and WebSocket clients;
- canonical head, ordered log ingestion, and reorg state;
- the registry of V3/V4 pools on that network;
- a `NetworkReadCoordinator` for startup hydration, wallet reads,
  reconciliation, Quoter parity, and gap repair;
- block-pinned wallet balances for configured tokens;
- the chain-specific gas and transaction fee policy;
- router, Quoter, Multicall3, factory, PoolManager, and StateView contracts;
- one `EvmExecutionOwner` for the initial wallet;
- one durable transaction/nonce journal namespace per wallet location.

The network runtime does not evaluate strategies. It delivers canonical,
ordered, generation-tagged DEX events and hydration results to the
`HotPathDecisionOwner`. The hot-path owner applies those events, owns the local
CLMM mirrors, and publishes immutable prepared curve generations without
cloning complete pools or acquiring a lock during evaluation.

Large Multicall responses, bitmap/tick decoding, gap repair, and provider retry
logic remain on network I/O workers. They may publish only bounded typed results
to the hot-path owner. A burst on one network cannot execute response decoding
or allocate large temporary collections on the decision thread.

Gas policy is part of the network configuration. World Chain fallback constants
must not be reused on Arbitrum. A missing reviewed fee policy makes execution on
that network unavailable while read-only pool observation may continue.

### Multicall and local quote boundary

“One request per network” means one logical, block-hash-pinned batch round for
all reads in the same traffic class on that network. It does not mean combining
unrelated readiness and diagnostic work into one giant Multicall.

`NetworkReadCoordinator` has these priority classes:

| Class | Priority and isolation |
| --- | --- |
| canonical gap repair and restart recovery | highest; new chain entries remain closed |
| wallet balance snapshot | high; independent freshness deadline |
| startup pool hydration | startup-critical for dependent strategies |
| periodic state reconciliation | bounded background work |
| sampled Quoter parity and diagnostics | lowest; freely shed before critical reads |

All classes reuse the same network client pool and provider capability profile,
but have independent deadlines, concurrency limits, chunk sizes, and
backpressure. The implementation may split a logical round when an RPC
provider's response-size, batch-count, or Multicall gas limit requires bounded
chunks. It must not create one client or sequential request loop per pair.

Use Multicall3 or a JSON-RPC batch for:

- initial pool head and static metadata reads;
- V3 bitmap/tick and V4 state hydration reads that are known for the selected
  canonical block;
- ERC-20 `balanceOf` reads for all configured wallet assets;
- periodic state reconciliation and gap repair;
- read-only collector quotes and sampled local-vs-Quoter parity.

Do not use Multicall or a Quoter for:

- opportunity evaluation after the mirror is ready;
- adaptive sizing;
- admission;
- entry preflight;
- transaction construction.

After hydration, canonical WebSocket logs update local CLMM mirrors. Binance
price events evaluate all relevant local prepared curves without network I/O.
Pool state is shared when several strategies reference the same pool.

Every batch result is tagged with:

- `chain_id`;
- canonical block number and hash;
- network connection generation;
- read class and provider capability profile;
- requested and returned pool/token identities;
- batch/chunk count and duration;
- partial or failed call identities.

A partial hydration does not produce a ready pool generation. A failed pool is
removed from the eligible candidate set; a strategy degrades only when it no
longer has the minimum healthy pool set required by its policy. Unknown
canonical head continuity still degrades every strategy on that network.

The provider profile records support for EIP-1898 block-hash `eth_call`,
maximum safe batch/chunk sizes, and the reviewed Multicall3 code identity.
Failure to prove the requested block hash invalidates the round rather than
silently falling back to an unpinned latest-state read.

## Hot-path decision and strategy runtime

The first multi-pair runtime has one `HotPathDecisionOwner` on a dedicated
single-thread Tokio runtime. It directly polls the Binance strategy-price socket
futures, prioritizes and drains already-queued canonical DEX events, applies
local mirror changes, and synchronously invokes affected strategy evaluators.
There is no channel or task wakeup between an accepted Binance price frame and
baseline opportunity evaluation.

Before evaluating a symbol, the owner drains already-queued DEX events only for
the networks and pools in that symbol's strategy dependencies. An unrelated
Arbitrum burst cannot postpone a World Chain WLD/USDC decision, and vice versa.
Canonical events are still never discarded; unrelated work remains queued for
its own dependency-scoped turn.

Each enabled strategy is a synchronous `StrategyEvaluator` owned by that
runtime. It contains only pair-specific calculation state:

- the selected Binance instrument;
- its candidate pool set;
- direction-specific thresholds and sizing rules;
- pair execution mode and risk caps;
- latest-only pending opportunity;
- pair telemetry and health projection.

The owner maintains two precompiled dependency indexes:

- calculation dependencies map Binance symbols, pools, and networks to the
  strategies that must be evaluated when they change;
- admission dependencies map account and wallet-location balances, execution
  capabilities, and risk policies to strategies whose candidates may become
  eligible or ineligible.

A market event invokes only strategies from the calculation index. A balance
event updates admission state without needlessly repeating DEX math. Strategy
output is an immutable candidate carrying the complete dependency vector used
to calculate and admit it:

```text
artifact fingerprint
Binance symbol + connection/update generation
network canonical head generation
selected pool generation(s)
Binance account balance generation
wallet-location balance generation
strategy policy generation
```

Prepared curves are immutable generations. Baseline evaluation borrows them
directly from the single owner without an atomic reference-count operation,
deep pool clone, shared lock, string lookup, or heap allocation. Only work that
actually crosses into an asynchronous sizing worker receives a
reference-counted immutable handle.

Baseline evaluation is bounded and synchronous. Exhaustive adaptive sizing and
other measured-heavy pure calculations use bounded workers against immutable
snapshots. Each worker has one latest-only slot per strategy. A completed result
is accepted only when its entire dependency vector is still current.

The runtime does not assume one task or thread per pair. If production profiling
shows that one decision owner cannot meet the target-node p99 budget at the
configured maximum pair count, the same synchronous evaluators may be assigned
to a small fixed set of decision shards. Every symbol, pool mirror, and strategy
then has exactly one shard owner; no hot-path mutex or arbitrary work-stealing
is introduced.

## Candidate scheduling, portfolio, and trade sagas

The mutation path is split into explicit owners rather than one process
coordinator:

- `CandidateScheduler` keeps at most one latest candidate per strategy, applies
  a versioned deterministic scheduling policy and the current globally
  serialized live policy, and never owns venue state;
- `PortfolioOwner` atomically validates account/pair risk and creates exact
  resource reservations;
- one durable `TradeSaga` owns the parent state machine, child identities,
  DEX-first ordering, recovery, settlement, and terminal PnL for each admitted
  operation;
- the Binance and EVM owners perform and reconcile only their typed child
  mutations.

An architectural owner is not automatically a Tokio task. In the first
implementation, `CandidateScheduler` and the admission-facing portion of
`PortfolioOwner` are co-located with `HotPathDecisionOwner` and invoked
synchronously. No actor/channel handoff is added between price receipt,
baseline evaluation, candidate selection, and exact reservation. The first
normal asynchronous handoff is the bounded accepted-work mailbox to the durable
trade path.

Admission is a two-phase protocol:

1. candidate scheduler submits the immutable candidate;
2. portfolio owner validates balance, capability, and risk generations and
   atomically reserves inventory/risk;
3. `HotPathDecisionOwner` validates current market generations, locally
   requotes when required, and returns an immutable `EntryPreflightProof`;
4. the trade saga validates that proof, selects the execution lane, and fsyncs
   its parent intent;
5. venue owners validate only their typed venue commands and durably accept
   their child intents before external mutation;
6. failure before any child submission releases the reservation;
7. an unknown child outcome retains only that operation's exact claims while
   reconciliation continues.

The initial live policy permits one newly dispatching parent at a time. The
interfaces and journals still support several known or recovering parent sagas,
so an old unknown operation does not globally erase unrelated available
inventory or order state.

The initial scheduling policy is starvation-bounded round robin across
strategies with eligible latest candidates. A later policy may rank comparable
economic value only through a separately reviewed artifact field; scheduler
implementation order, hash-map order, or task wakeup timing must never decide
which pair receives the shared lane.

The scheduler uses the compiled dependency index and a ready bitset/queue; it
does not scan every configured strategy on every price frame.

Lane availability is scheduling, not market-data readiness. A busy lane keeps
the latest candidate for each dependent strategy; a newer candidate supersedes
only the older candidate for that same strategy.

### Location-aware inventory

The current `(venue, asset)` wallet key is insufficient for multiple networks.
It would incorrectly merge World Chain USDC and Arbitrum USDC.

The target key is equivalent to:

```text
InventoryLocation =
  Binance { account_id }
  | EvmWallet { chain_id, wallet_id }

VenueAssetId =
  BinanceAsset { account_id, symbol }
  | Erc20Asset { chain_id, token_address }

InventoryKey = { location, venue_asset_id }
```

`EconomicAssetId` is a separate capital-policy identity such as `USDC`. A
reviewed configuration mapping may associate Binance `USDC`, World Chain USDC,
and Arbitrum USDC with that economic asset, but exact balance and reservation
keys always retain their venue/contract identity. An economic mapping never
makes the representations interchangeable for execution.

Consequences:

- Binance USDC is one shared account balance across WLD/USDC and ESP/USDC.
- World Chain USDC and Arbitrum USDC are independent balances.
- reservations on either pair atomically reduce the same Binance USDC
  availability;
- a World Chain wallet reservation cannot reduce Arbitrum wallet inventory;
- token identity always includes the chain-specific contract on EVM locations;
- balance generations and settlement barriers are location-scoped.

The first implementation keeps exact primary-debit reservations and the current
v12 rules. It must not restore the Rails `3x` multiplier or reserve hypothetical
recovery.

The portfolio owner also records bounded non-inventory resource claims when
needed, including execution-lane assignment, pair/account exposure limits, and
canary budgets. These claims never fabricate balances and do not replace the
venue owners' nonce and exchange-rate-limit checks.

Observed and reserved totals are indexed by `InventoryKey`. Admission reads the
pre-aggregated reserved total and updates only the candidate's small fixed claim
set; it must not fold over every active or unknown reservation on each
opportunity. Operation-level claims remain available for audit and settlement.

## Account-wide rebalancing

Rebalancing is shared infrastructure, not a strategy-owned job. It has three
separate responsibilities:

- `CapitalAllocator` calculates desired location balances;
- `RebalanceSaga` durably coordinates one transfer operation and its recovery;
- Binance and EVM venue owners perform the actual child mutations.

The planner observes:

- the single Binance account inventory;
- every enabled wallet location;
- active trade and rebalance reservations;
- pending deposits, withdrawals, bridges, and settlement barriers;
- per-asset, per-network route availability and configured capital targets;
- explicit `minimum`, `target`, `maximum`, and priority for every funded
  location.

The allocator evaluates each `EconomicAssetId` once across the entire account.
The shared Binance balance appears once in the conservation equation and is
never copied into a separate pair-level budget. Wallet targets remain
location-specific. The allocator must prove that proposed debits, credits, fees,
and in-flight transfers conserve the account-wide economic asset within exact
configured representation mappings.

Capital allocation is triggered by balance/settlement generations, never by a
Binance price frame. Calculation runs as latest-only cold work against an
immutable portfolio snapshot; `PortfolioOwner` revalidates its generations
before reserving a proposed transfer. A slow multi-location allocation cannot
extend strategy decision latency.

It emits at most one new external transfer operation at a time during the first
migration. This retains the current simple recovery boundary and avoids two
transfers competing for shared Binance inventory or the same wallet nonce.

Rebalance policy is location-aware:

- a deficit of USDC on Arbitrum is not repaired by measuring World Chain USDC
  as if it were local;
- a Binance-side asset target is account-wide;
- a wallet-side target belongs to a specific `(chain_id, wallet_id, asset)`;
- route selection is pinned after the first external side effect;
- every wallet transaction is routed exclusively through that network's
  `EvmExecutionOwner`; the rebalance saga never signs or allocates a nonce.

The rebalancer may move capital while strategy evaluation continues. Active
operations reduce available inventory through reservations but do not become a
global market-data readiness gate.

The EVM lane scheduler prefers recovery and admitted trades over starting a new
rebalance child. Once a rebalance transaction has been broadcast, its nonce and
recovery remain authoritative; a later trade may be queued or assigned a later
nonce according to the reviewed lane policy, but cannot bypass or replace the
rebalance transaction.

Before ESP/USDC live trading, its Arbitrum deposit/withdrawal/bridge routes and
recovery semantics require independent validation. World Chain route evidence
must not be reused as Arbitrum evidence.

## Journals and restart recovery

All mutation identities must be globally unique and recoverable:

```text
account/{account_id}/orders
network/{chain_id}/wallet/{wallet_id}/transactions
rebalance/{account_id}
trade/{strategy_id}/{parent_id}
```

These are logical namespaces; the implementation may use fewer physical files
if one file has a single lock-owning writer and typed records.

Every journal record contains a schema version, domain fingerprint, process
epoch, owner identity, operation identity, monotonic sequence, previous-record
checksum, and payload checksum. The GKE single-process deployment and file lock
remain the primary ownership fence; the process epoch prevents an older
in-memory owner from accepting work after a supervised restart has begun.

Parent/child mutation ordering is:

1. the trade or rebalance parent intent is written and fsynced;
2. the venue owner validates the typed child command, writes its child intent,
   and fsyncs before acknowledging acceptance;
3. only the venue owner performs the external mutation;
4. the venue outcome or explicit Unknown state is written and fsynced;
5. the parent consumes that durable child state and fsyncs its transition;
6. authoritative settlement observations advance the reservation barrier;
7. only then may the portfolio owner release the settled claims.

No cross-file atomic commit is assumed. Deterministic parent/child IDs and the
ordering above make every partially completed handoff recoverable and
idempotent.

Startup order is:

1. validate the entire domain artifact and unique identities;
2. acquire every required journal lock before accepting live work;
3. hydrate Binance account and order recovery state;
4. hydrate each configured network's wallet nonce and transaction recovery
   state;
5. recover non-terminal rebalance and trade operations;
6. hydrate market data, pool mirrors, filters, commissions, and balances;
7. mark each strategy independently ready only when its dependencies are ready;
8. open new execution only after all shared-account authorization invariants
   hold.

An unresolved operation blocks only inventory and the mutation lane it actually
owns, except when authenticated Binance account state is unknown or journal
ownership cannot be proven. Those account-level failures close all new entries.

Every journal schema change declares:

- the oldest readable version;
- whether the previous production binary can safely read records written by the
  new binary;
- an explicit migration and rollback procedure when it cannot;
- fixtures for restart at every fsync boundary.

A deployment must fail before acquiring live ownership when journal
compatibility cannot be proved. Rollback to an older binary is forbidden after
an incompatible record has been written unless the reviewed workflow migrates
or archives the journal after all operations are terminal.

## Configuration model

Operators maintain modular source documents for accounts, instruments,
networks, wallets, tokens, pools, strategies, and policies. A deterministic
domain compiler resolves and validates them into one canonical immutable
runtime bundle. Production loads only that bundle and records its fingerprint;
it never resolves includes, queries Rails, or discovers executable policy at
runtime.

The compiled bundle defines:

- Binance accounts;
- instruments and their account association;
- networks and their RPC/WSS environment-variable names;
- wallets and enabled wallet locations;
- contracts and chain-specific fee policies;
- tokens with chain-specific contract identities;
- pools;
- strategies and their pool/instrument references;
- account-wide risk, order, and rebalance policies;
- per-strategy execution modes and caps.

The runtime derives every Binance subscription and network pool filter from this
bundle. Environment variables provide secrets and endpoints only; they must not
become a second pair, symbol, pool, or network allowlist.

Validation rejects:

- duplicate typed identities;
- a strategy referencing different networks in one atomic DEX leg;
- a pool/token/network mismatch;
- a live strategy without a router and reviewed chain fee policy;
- two owners configured for the same Binance account or execution lane;
- overlapping journal paths with incompatible owners;
- a symbol whose assets do not match the strategy tokens;
- rebalancing without a location-specific route policy;
- live execution on a network that has only read-only validation evidence.

The current v12 WLD/USDC artifact and ESP/USDC v2 artifact remain immutable.
Migration creates a new combined artifact version; it does not edit historical
artifacts in place.

Pool configuration has an explicit lifecycle:

```text
discovered -> observed -> validated -> execution_eligible
```

Runtime factory checks may discover or revalidate pools, but a newly discovered
pool remains observation-only until a reviewed source artifact promotes it.
On-chain discovery can never silently add a pool to live routing.

The compiler also emits:

- the strategy dependency index;
- deterministic Binance stream-shard assignments;
- network read limits and provider capabilities;
- exact venue-to-economic-asset mappings;
- journal namespace and owner assignments;
- a capability matrix showing which pairs may observe, plan, rebalance, or
  execute.

## Readiness projection

Readiness is a dependency-scoped capability projection rather than one
undifferentiated boolean. Strategy evaluation, new admission, each execution
lane, and recovery have separate capability views.

| Failure | Effect on new work |
| --- | --- |
| ESPUSDC public stream stale | ESP strategies only |
| Arbitrum pool mirror incoherent | Remove that pool; degrade only strategies without another policy-eligible pool |
| Arbitrum canonical head unknown | All Arbitrum strategies |
| World Chain head unknown | All World Chain strategies |
| Arbitrum wallet balance stale | Arbitrum strategies requiring that inventory |
| Binance authenticated balance stale | Every strategy on the shared account |
| Binance order ownership unknown | Every new Binance mutation |
| One strategy sizing worker overloaded | That strategy's older sizing work is superseded |
| One EVM lane busy | No readiness change; candidates remain latest-only and lane scheduling applies |
| One asset fully reserved | Candidates that debit that location and asset |
| ClickHouse unavailable | No readiness effect |

Existing recovery always continues even when new entries are disabled.

## Event delivery and backpressure

Every boundary has an explicit loss policy:

| Event class | Delivery contract |
| --- | --- |
| Binance strategy-price frame | Parsed directly by the hot-path owner; no intermediate queue before baseline evaluation |
| canonical DEX log | Never deliberately dropped; overflow or ordering uncertainty degrades the network and requires gap repair/rehydration |
| canonical head | May be coalesced only after parent continuity and every intervening log range are proved |
| Binance User Data order/fill event | Never deliberately dropped; overflow closes new account mutations and triggers authoritative reconciliation |
| complete balance snapshot | Latest valid generation wins; regressed generations are rejected |
| strategy sizing request/result | Latest per strategy wins; stale dependency vectors are rejected |
| candidate | Latest per strategy wins before reservation |
| accepted execution/recovery command | Durable and never dropped |
| telemetry | May be dropped from a bounded channel with an explicit counter |

Queue capacity, enqueue latency, high-water mark, dropped/superseded count, and
recovery action are observable per boundary. An owner must never continue from a
plausible partial event stream after its loss contract has been violated.

## Supervision and shutdown

One `RootSupervisor` owns every task/thread handle and applies these policies:

- a validated strategy error or sizing-worker overload disables only that
  strategy and alerts;
- a public stream failure reconnects its bounded shard and degrades only
  dependent strategies;
- a network ingestion failure degrades that network until canonical recovery;
- a panic or death of the hot-path owner, portfolio owner, Binance order owner,
  capital saga owner, trade saga supervisor, or EVM execution owner closes new
  mutations and terminates the process after durable state is flushed;
- telemetry failure is observable but never changes trading readiness;
- panic or unexpected channel closure is never treated as a clean terminal
  child result.

Controlled shutdown first closes new admission, then stops producing new
rebalance work, preserves/reconciles already accepted mutations, flushes
durable journals, and finally relinquishes locks. It must not wait indefinitely
for market-data or telemetry drains.

## Performance preservation contract

The migration is not allowed to trade the current latency advantage for
architectural neatness. Ownership boundaries are logical boundaries; they do
not authorize extra queues, task wakeups, serialization, locks, allocations, or
network calls in the hot path.

### Primary regression risks

| Risk introduced by this design | Required control |
| --- | --- |
| actor handoff between price receipt, evaluator, scheduler, and portfolio | co-locate the initial owners and call them synchronously through reservation |
| draining every network before every pair evaluation | dependency-scoped DEX drain only |
| evaluating or scanning every strategy for one symbol | compiled adjacency index plus ready bitset |
| cloning prepared pools or `Arc` handles on every frame | borrowed single-owner baseline path; clone only for asynchronous sizing |
| rebuilding unrelated pool curves | rebuild only the event's affected pool and dependent execution envelope |
| decoding large Multicall responses on the decision runtime | bounded network worker decode and compact typed publication |
| summing every active reservation during admission | indexed observed and reserved totals per `InventoryKey` |
| running capital allocation on price events | latest-only cold calculation on balance/settlement generations |
| adding parent/child journal barriers before first network write | reuse existing necessary durable barriers, add no new sequential barrier, and instrument their combined span |
| formatting more per-frame JSON telemetry | fixed-size hot records and background serialization; bounded drops remain visible |
| public-stream, RPC, sizing, or telemetry tasks contending for the hot core | separate bounded runtimes/queues; measure target-node CPU, throttling, and decision tails |

No implementation may add a generic `mpsc`, `watch`, mutex, `RwLock`, dynamic
JSON construction, string-key lookup, RPC call, `fsync`, or heap allocation
between accepted Binance strategy-price frame parsing and completed baseline
evaluation.

### Frozen production reference

The reference below was queried from ClickHouse on 2026-07-29. It is a completed
stable GKE owner window, not a laptop benchmark:

| Property | Value |
| --- | --- |
| deployment source-revision annotation | `d93fb2955b47de64fc8118a36e339a7c8fa90207` |
| engine | `arb-bot-rust-shadow-gke-arb-bot-7964b95cf7-7hjkv` |
| artifact | `config/strategies/usdc-wld-world-chain.v12.json` |
| node | fixed `c4-highcpu-8`, `asia-southeast1-b` |
| telemetry interval | `[2026-07-28T10:00:05Z, 2026-07-29T02:57:52Z]` |
| WLDUSDC strategy frames | 157,247 |
| Binance-triggered WLD evaluations | 157,248 |
| threshold direction events | 1,815 |
| adaptive sizing tasks | 1,623 |
| admitted plans | 1,325 |
| superseded pending plans | 1,180 |
| plans reaching live task | 145 |
| entry-preflight rejections | 49 |
| terminal results | 96 |
| maximum reported hot-telemetry drops | 0 |

The immediately following Pod at the same source revision produced 11,429
WLDUSDC frames in its initial short sample with p99 `parse_time_us=3`,
`decision_complete_us=41`, `calculation_time_us=10`, and
`decision_latency_us=18`. This confirms that the completed window below is a
conservative current reference rather than a one-off fast sample.

### Market-data and local-decision reference

Durations are microseconds. `max` is diagnostic; release gates use p95/p99 and
sample sufficiency.

| Stage and telemetry field | n | p50 | p95 | p99 | max |
| --- | ---: | ---: | ---: | ---: | ---: |
| WLDUSDC JSON parse, `binance_book_ticker.parse_time_us` | 157,247 | 0 | 4 | 7 | 35 |
| socket receipt to completed decision, `decision_complete_us` | 157,247 | 13 | 36 | 46 | 162 |
| Binance-triggered baseline calculation, `arbitrage_evaluation.calculation_time_us` | 157,248 | 7 | 17 | 19 | 89 |
| Binance-triggered receipt-to-evaluation, `decision_latency_us` | 157,248 | 11 | 29 | 37 | 112 |
| WLD depth parse/apply, `binance_depth_applied.parse_apply_time_us` | 161,212 | 7 | 15 | 21 | 72 |
| DEX event receive-to-owner, `dex_pool_event.engine_queue_age_us` | 6,890 | 28 | 61 | 122 | 1,189 |
| head receive-to-owner, `world_chain_head.engine_queue_age_us` | 30,534 | 25 | 48 | 145 | 377,922 |
| V3 fee-500 prepared-curve total, `dex_pool_prepared.total_time_us` | 5,776 | 19 | 30 | 146 | 168 |
| V3 fee-3000 prepared-curve total | 1,147 | 16 | 26 | 32 | 55 |
| V4 fee-3000 prepared-curve total | 12 | 62 | 87 | 87 | 87 |

The V4 cohort is too small for a percentile gate and is reference-only. The
head maximum is one tail outlier; p99 plus canonical continuity, not maximum
head delay, is the release comparison.

### Adaptive sizing and admission reference

| Stage and telemetry field | n | p50 μs | p95 μs | p99 μs | max μs |
| --- | ---: | ---: | ---: | ---: | ---: |
| sizing snapshot | 1,623 | 5 | 15 | 19 | 37 |
| sizing worker queue | 1,623 | 10 | 67 | 115 | 208 |
| sizing worker calculation | 1,623 | 30 | 51 | 62 | 73 |
| sizing result handoff | 1,623 | 15 | 63 | 143 | 626 |
| optimizer calculation | 1,623 | 21 | 36 | 45 | 54 |
| trigger to admitted | 1,325 | 130 | 245 | 296 | 483 |
| admission total | 1,325 | 20 | 38 | 45 | 64 |
| exact inventory reservation | 1,325 | 1 | 3 | 4 | 10 |
| accepted mailbox submit | 1,325 | 6 | 10 | 12 | 21 |

`market_to_admitted_us` had p50 `134`, p95 `269`, p99 `609,200`, and maximum
`10,323,119`. It includes asynchronous sizing and the age of an unchanged
event-driven quote, so it is not a local compute-latency gate. It remains a
cohort/scheduling diagnostic.

### Durable and venue-execution reference

The execution cohorts are smaller (`n=72–266`), so these values are frozen
references rather than precise long-run tail claims.

| Stage | n | p50 μs | p95 μs | p99 μs | max μs |
| --- | ---: | ---: | ---: | ---: | ---: |
| entry validation preflight | 145 | 13 | 26 | 29 | 31 |
| coordinator admission journal | 96 | 3,353 | 4,299 | 5,720 | 5,720 |
| coordinator command journal | 266 | 2,856 | 3,929 | 4,995 | 8,938 |
| coordinator result journal | 170 | 2,849 | 3,640 | 4,078 | 4,319 |
| DEX worker queue | 96 | 17 | 30 | 32,193 | 32,193 |
| nonce reserve/sign/journal | 96 | 3,241 | 4,376 | 7,538 | 7,538 |
| DEX receipt journal | 96 | 1,794 | 2,285 | 2,831 | 2,831 |
| Binance worker queue | 72 | 13 | 33 | 109 | 109 |
| Binance intent journal | 72 | 2,555 | 3,096 | 3,371 | 3,371 |
| Binance placement WebSocket API | 72 | 72,695 | 77,071 | 79,145 | 79,145 |
| DEX broadcast RPC | 96 | 177,666 | 223,661 | 366,677 | 366,677 |
| DEX confirmation RPC | 96 | 414,318 | 548,515 | 721,110 | 721,110 |
| DEX worker total | 96 | 602,507 | 768,301 | 908,769 | 908,769 |
| live task total | 145 | 605,741 | 851,069 | 1,013,033 | 1,092,708 |
| latest-only mailbox wait | 145 | 14,439 | 1,079,037 | 6,371,251 | 10,323,135 |
| market observation to terminal | 145 | 701,354 | 1,686,850 | 7,146,342 | 10,870,321 |

RPC and Binance placement distributions include external venue/network latency.
They must be reported, but a code release is rejected primarily on new local
queue, journal, and handoff time unless an equal-window comparison also proves
an external regression.

The new saga architecture must add direct spans that the current telemetry does
not provide:

- `candidate_selected_to_reservation_complete_us`;
- `reservation_to_preflight_proof_us`;
- `preflight_proof_to_parent_fsync_us`;
- `parent_fsync_to_evm_first_write_us`;
- `dex_receipt_to_binance_first_write_us`;
- `child_terminal_to_reservation_settled_us`.

Without these spans, an extra sequential fsync or owner wakeup could hide inside
an end-to-end venue duration.

### Background isolation reference

Background calls were materially slower than the local decision path:

| Background request | n | p50 | p95 | p99 | max |
| --- | ---: | ---: | ---: | ---: | ---: |
| Binance account balance REST | 12,214 | 75.050 ms | 83.050 ms | 210.876 ms | 3.975 s |
| block-pinned wallet balance read | 30,523 | 8.397 ms | 484.338 ms | 686.113 ms | 11.547 s |

Despite those tails, strategy decision p99 stayed at `46 μs`. Every milestone
must preserve this isolation. Making balance, Multicall, reconciliation, or
capital-allocation latency faster is useful, but never at the cost of moving its
work onto the decision owner.

`runtime_starting` to first `ready` was `897 ms` for the frozen engine and
`679 ms` for its successor. This telemetry begins after some DEX bootstrap work,
so it is not a complete process-startup metric.

### Target-node resource reference

`scripts/report-gke-runtime-resources` reproduced this reference for the same
`[2026-07-28T10:00:05Z, 2026-07-29T02:57:52Z)` window:

| Resource | n | p50 | p95 | p99 | max |
| --- | ---: | ---: | ---: | ---: | ---: |
| container CPU cores, 60-second aligned rate | 1,018 | 0.00972 | 0.01912 | 0.02422 | 0.02878 |
| CPU limit utilization | 1,018 | 0.00162 | 0.00319 | 0.00416 | 0.00488 |
| memory used bytes | 2,036 | 22,577,152 | 74,985,472 | 75,251,712 | 76,595,200 |
| page faults per second | 2,036 | 0 | 112.61 | 118.36 | 218.27 |

A read-only cgroup/process snapshot of the successor Pod at
`2026-07-29T04:26:21Z`, still on source revision `d93fb29`, recorded:

- `nr_throttled=0` and `throttled_usec=0` across 32,339 CFS periods;
- cgroup `memory.peak=99,835,904` bytes with zero `high`, `max`, OOM, and
  OOM-kill events;
- process `VmHWM=79,988 KiB`, nine threads, 442,698 voluntary and 120
  involuntary context switches.

The very low CPU utilization is not a license to add hot-path handoffs. M1–M11
must continue to compare the decision percentiles and background-tail
non-interference, not only aggregate CPU headroom.

### Missing measurements required in M0

Before structural migration, telemetry must add:

- process start to domain validation complete;
- journal lock/recovery duration by owner;
- per-network canonical block selection, hydration, subscription acknowledgement,
  backfill, and first-ready duration;
- Multicall/JSON-RPC batch queue, provider, decode, publication, chunk count, and
  response bytes;
- per-frame dependency fanout count and dependency-scoped DEX drain duration;
- decision-owner loop lag and longest non-price handler duration;
- candidate scheduler and portfolio synchronous spans;
- executor queue depth and enqueue-to-first-write spans;
- capital allocation calculation/validation duration;
- per-thread/runtime CPU time, Pod CPU throttling, memory high-water mark, and
  allocator pressure from Cloud Monitoring or an equivalent non-blocking
  source.

Instrumentation itself must use fixed-size/bounded hot records and background
formatting. Adding the measurement may not invalidate the baseline it is meant
to protect.

### M0 implementation contract

M0 uses the following versioned operator reports:

- `scripts/report-m0-performance START_UTC END_UTC` executes the checked-in
  ClickHouse queries in `scripts/sql/m0_*.sql` and reproduces counts, hot-path,
  pool-specific DEX receive/build/publication, sizing/admission, execution,
  queue, startup, journal, owner-loop, balance batch, and allocator tables for
  one half-open UTC window;
- `scripts/report-gke-runtime-resources START_UTC END_UTC` reports aligned GKE
  container CPU, CPU-limit utilization when present, memory, and page-fault
  distributions, then captures read-only cgroup CPU-throttling, memory
  high-water, memory-event, thread, and context-switch counters from the sole
  production Pod.

The compatibility runtime emits stable identity fields before M1 introduces the
compiled typed registries. Their formats are deliberately deterministic:

| Typed identity | M0 compatibility encoding |
| --- | --- |
| `BinanceAccountId` | `binance-spot:primary` |
| `InstrumentId` | `binance-spot:primary:<SYMBOL>` |
| `StrategyId` | `strategy:<pair_id>` |
| `NetworkId` | `eip155:<chain_id>` |
| `WalletId` | `evm-wallet:primary` |
| `WalletLocation` / `ExecutionLaneId` | `eip155:<chain_id>:evm-wallet:primary` |
| `PoolId` | `eip155:<chain_id>:pool:<canonical debug identity>` |

These strings are telemetry compatibility projections, not the M1 registry
implementation. M1 must round-trip them or introduce an explicitly versioned
replacement; it may not silently merge histories.

M0 adds only non-blocking/background or already-existing event-boundary
measurements:

- process start through domain validation, canonical block selection, pool
  hydration, subscription acknowledgement, backfill, and first ready;
- journal lock/recovery by trade, Binance order, EVM, and rebalance owner;
- wallet JSON-RPC batch build, provider, decode, publication, chunks, and
  response bytes;
- per-frame dependency fanout, dependency-scoped DEX drain, decision-loop lag,
  and longest non-price handler;
- candidate-to-reservation, reservation-to-preflight,
  preflight-to-parent-fsync, worker queue depth, and enqueue-to-first durable
  child write;
- capital-allocation calculation plus validation.

No new queue, lock, RPC call, JSON construction, or typed-ID formatting occurs
between a parsed strategy-price frame and completed baseline evaluation. The
fixed-size hot records continue to be formatted only by the background
telemetry task.

The same boundary applies to canonical DEX ingestion. `dex_pool_event`,
`world_chain_head`, and `dex_pool_prepared` cross the decision-owner boundary
as bounded fixed-size records. Pool/pair lookup, compatibility-ID formatting,
base-unit string conversion, JSON construction, and serialization run only on
the background hot-telemetry task. This prevents telemetry for one pool log
from increasing the receive-to-owner age of later logs in the same burst.

The aggregate hot-path table remains stable for comparison with the frozen
single-engine baseline. A separate `m0_dex_pool_hot_path.sql` table groups the
DEX event, curve-build, and total publication distributions by engine, pair,
strategy, network, stable pool ID, and canonical pool identity. It also reports
complete stage-timing counts and maximum prepared segment counts, so a sparse
fee tier cannot be hidden inside a faster aggregate pool distribution.

Adaptive sizing task, optimizer, and admission rows carry the M0 compatibility
IDs for their pair, strategy, Binance account, instrument, and network.
`m0_admission_execution.sql` groups on those dimensions, so M1 cannot hide one
strategy's queue or worker tail inside another strategy on the same engine.

The owner applies an already-queued canonical DEX burst before doing any curve
work. Build requests are coalesced by pool generation and keep only the newest
mirror snapshot for each pool. Independent pools are then built by ascending
last-published segment count, with pool index as the deterministic tie-breaker,
so a small curve does not wait behind an unrelated sparse curve. A replacement
generation inherits the estimate captured by the first request for that pool.
All affected curves are still rebuilt inline before any strategy evaluation,
receipt settlement completes, or an adaptive-sizing result is accepted. This
removes an unobservable intermediate build from the receive path of the next
queued log without weakening generation validation or canonical event order.

Prepared CLMM segments store only their cumulative end points; each segment's
start is the preceding end point and is derived during lookup. The bounded
curve vector reserves 128 segments, covering the measured sparse V3 execution
envelope without allocator growth while adding only bounded sub-megabyte
working-set overhead to the current four-pool runtime. Each pool also shares an
immutable cache of the square-root prices at empty bitmap-word boundaries
across its cheap event-build clones. Initialized ticks still use the canonical
tick calculation, while sparse empty-word traversal avoids recalculating the
same deterministic boundary prices after every swap. Exact quote parity and
rounding remain regression-tested at every segment boundary.

Subsequent generations retain the three published curve-vector allocations
instead of freeing and reallocating them for every Swap. Coalescing transfers
those buffers from the superseded request to the newest request for the same
pool. The reverse-direction token-A exact-output envelope limit is calculated
with the same allocation-free bounded traversal used as the curve oracle rather
than materializing a fourth temporary curve whose segments were immediately
discarded. Reachable-capacity behavior remains unchanged if liquidity ends
inside the configured envelope. The three published curves and every boundary quote remain
byte-for-byte equivalent to the iterative CLMM math; this changes allocation
work only, not sizing, admission, slippage, or execution semantics.

`m0_cohort.sql` is the authoritative cohort gate in
`scripts/report-m0-performance`: WLD percentile claims remain `collecting`
until the selected half-open window contains at least 100,000 strategy frames,
1,000 adaptive-sizing evaluations, and zero hot-telemetry drops.

### Current single-pair assumption register

Every known production bootstrap restriction has an explicit migration owner:

| Current assumption or restriction | Current owner | Removal milestone |
| --- | --- | --- |
| `run` requires exactly one enabled Binance symbol | direct hot-path bootstrap in `main` | M2 |
| `collect-prices` requires one enabled symbol and selects one market-data pair | ESP compatibility collector | M1, then M4 |
| Binance account hydration returns one symbol rule/commission view | authenticated account bootstrap | M2 |
| one `BookTickerFeed` plus one depth book drives executable strategy state | direct hot-path bootstrap | M2 and M4 |
| `chain_endpoints` accepts one enabled chain and one RPC/WSS pair | DEX bootstrap | M3 |
| one `DexMirror` owns pools for the selected network and emits the legacy `world_chain_head` kind | DEX owner | M3 |
| balance sync receives one Binance symbol, one wallet location, and one pair's asset list | balance bootstrap | M2, M3, and M5 |
| inventory keys are only `(Binance|Wallet, symbol)` | inventory owner | M5 |
| admission selects the first matching domain pair and one global execution mailbox | strategy/coordinator compatibility path | M4 and M6 |
| one global trade coordinator serializes all live parents | trade coordinator | retained through M6; reviewed only after M9 |
| EVM journals and nonce ownership are World Chain/wallet compatibility paths | DEX and rebalance executors | M6 |
| gas policy contains reviewed World Chain fallback constants | DEX executor | generic boundary in M3; Arbitrum policy in M8 |
| rebalancing assumes Binance plus the World Chain wallet and Optimism fallback | rebalance tracker/executor | M5, M6, and M10 |
| the deployment workflow validates WLD v12 and ESP v2 as separate artifacts | production delivery | M1 |
| validation/canary commands intentionally restrict mutations to WLD/World Chain | operator-only validation commands | retained until M8/M9 |

The initial single signer is a target-topology decision, not an accidental
single-pair restriction. Multi-wallet selection remains deferred as specified
below.

### Release gates

For WLD/USDC, every milestone that changes runtime code must compare an optimized
build on the target C4 class against this reference:

1. at least 100,000 WLDUSDC strategy frames and 1,000 adaptive tasks for
   hot-path percentile claims;
2. for reference percentiles at or above `10 μs`, p95 may not exceed `1.15x`
   and p99 may not exceed `1.20x` without an explicit reviewed exception;
   smaller values use the independent absolute ceiling because integer
   microsecond quantization makes a relative comparison misleading;
3. independent hard p99 ceilings are `10 μs` parse, `60 μs`
   socket-to-decision, `25 μs` baseline calculation, `30 μs` depth apply,
   `175 μs` DEX event receive-to-owner, `200 μs` prepared-curve publication,
   `150 μs` sizing queue, `75 μs` sizing worker, `400 μs`
   trigger-to-admitted, `60 μs` admission total, `10 μs` reservation, and
   `20 μs` accepted mailbox submit;
4. hot telemetry drops, canonical DEX event drops, execution-command drops, and
   unknown queue overflows must remain zero;
5. no new network call, lock, allocation, serialization, task wakeup, or
   all-strategy scan may appear in the Binance frame-to-baseline path;
6. small execution cohorts must report p50/p95/p99 and exact `n`; no release may
   claim tail improvement from fewer than 100 observations;
7. background p95/p99 may change, but WLD decision tails during their slowest
   cohorts must still meet the same hot-path gates;
8. M11 maximum-pair replay must run on the target CPU class and include
   reconnect/rehydration bursts, not only steady-state average load.

The hard ceilings do not replace relative comparison. Passing `60 μs` after
moving from `46 μs` to `59 μs` is still a regression requiring review.

### Performance evidence by milestone

| Milestone | Required evidence before exit |
| --- | --- |
| M0 | freeze the queries above, add missing spans, and record target-node CPU/throttling baseline |
| M1 | compiled bundle load/validation time and memory; unchanged WLD hot tables |
| M2 | per-stream parse/decision percentiles, shard fairness, reconnect isolation, zero hot drops |
| M3 | batch queue/provider/decode/publication percentiles and unchanged WLD decision/Dex-event tails |
| M4 | dependency fanout, DEX drain, baseline evaluation, sizing, loop lag, and no frame-to-evaluator handoff |
| M5 | scheduler/portfolio/reservation and allocator spans; allocator slow cohorts must not move WLD tails |
| M6 | parent/child fsync and enqueue-to-first-write spans; no additional sequential durable barrier |
| M7 | full frozen-table comparison from the new WLD production path with equal or larger cohorts |
| M8 | Arbitrum pool-event, curve-build, read-batch, and ESP decision percentiles while WLD gates still pass |
| M9 | ESP live local/external execution table plus unchanged WLD execution and decision tails |
| M10 | rebalance child/saga/settlement spans and WLD/ESP non-interference during slow transfer cohorts |
| M11 | maximum 10–20 pair target-node replay, burst recovery, stream-shard fairness, CPU and memory headroom |

The matching row is a mandatory exit criterion for every milestone, alongside
its functional criteria below.

## Milestones

Each milestone is independently deployable and must leave production in a
supported state. No milestone combines a structural ownership change with
enabling ESP/USDC live trading.

### M0 — Baseline contracts and observability

Deliver:

- capture current WLD/USDC v12 startup, readiness, latency, opportunity,
  execution, recovery, nonce, balance, and rebalance behavior as regression
  tests and production baselines;
- add stable typed IDs to telemetry where they are currently implicit;
- document the current single-pair assumptions and map each one to a later
  milestone;
- define latency and dropped-work counters per account, network, pool, and
  strategy;
- capture current event-delivery, journal, and shutdown behavior at every
  mutation boundary;
- version the ClickHouse baseline queries and add the missing spans listed in
  the performance contract before changing the measured ownership path;
- capture target-node CPU, throttling, memory, and decision-loop interference
  under normal and background-RPC-tail cohorts.

Exit criteria:

- `scripts/quality.sh` passes;
- production WLD/USDC behavior is unchanged;
- baseline dashboards/queries can separate WLD/USDC and ESP/USDC;
- the frozen performance tables are reproducible from versioned queries and the
  new instrumentation itself passes the same hot-path gates;
- every known `exactly one pair/symbol` bootstrap restriction has an owner and
  target milestone.

Rollback:

- code-only rollback; no artifact or secret change.

#### M0 transition record

On 2026-07-29 the operator explicitly accepted the available post-fix M0
production cohort as sufficient to begin M1. The accepted half-open cohort had
10,437 WLDUSDC frames, 166 adaptive tasks, zero hot-telemetry drops, no CPU
throttling, and no memory high/max/OOM event. It is a reviewed exception to the
100,000-frame/1,000-task exit sample requirement, not a claim that the formal
M0 percentile gate reached `ready`. The observed WLD hot-path values remain the
comparison baseline for the M1 rollout; the formal sample thresholds continue
to apply to future percentile claims unless another exception is reviewed.

### M1 — Compiled multi-pair domain graph

Deliver:

- introduce typed registries for accounts, instruments, networks, wallets,
  venue assets, economic assets, pools, and strategies;
- add the deterministic domain compiler and load its canonical combined
  read-only bundle containing WLD/USDC World Chain and ESP/USDC Arbitrum;
- validate references, uniqueness, execution capabilities, and environment
  requirements before network connections start;
- emit the dependency index, stream shards, owner/journal assignments, asset
  mappings, and capability matrix;
- keep the existing WLD live runtime adapter and ESP collector behavior behind
  compatibility projections.

Exit criteria:

- both existing artifacts round-trip into the new internal graph in tests;
- the combined artifact derives exactly `WLDUSDC` and `ESPUSDC`;
- source ordering does not change the canonical bundle or fingerprint;
- no symbol, pool, or network list is duplicated in environment variables;
- selecting the combined artifact alone cannot enable a new live pair.

Rollback:

- select the unchanged WLD/USDC v12 artifact.

Implementation uses
`config/domain/multi-pair-production.v1.sources.json` as the versioned compiler
manifest and
`config/domain/compiled-multi-pair-production.v1.json` as the generated
canonical bundle. `run` selects `compat-live-runtime`; `collect-prices` selects
`compat-public-price-collector`. The compiler requires the reviewed live
strategy set to equal the execution-enabled strategies in the immutable source
artifacts, so merely selecting the bundle cannot grant ESP execution or
rebalance capability. Bundle validation emits load duration, bundle size, and
Linux RSS before/after values before any network connection.
`scripts/report-m1-domain START_UTC END_UTC` reproduces those startup rows for
both compatibility projections from Cloud Logging; the normal M0 hot-path and
GKE resource reports remain the latency and memory comparison sources.

#### M1 production record

Revision `9c5f929849ec6650338fdac129216ff6dc766a2e` was deployed on
2026-07-29 as immutable image
`sha256:4fb9b9207dde26ccaf9b85ef49404e11fd6bb9e2fb0986be6f7374e8786ac5be`.
Both containers validated the same 25,831-byte compiled bundle before network
startup. The live projection loaded in 363 microseconds and the collector
projection in 320 microseconds; observed RSS deltas were 1,613,824 and
1,634,304 bytes. The first WLD cohort had 499 frames, zero telemetry drops,
3-microsecond parse p99/max, 24-microsecond socket-to-decision p99, and
48-microsecond maximum. CPU, memory, throttling, restarts, and production ERROR
checks showed no M0 regression. This was a deployment regression check, not a
new formal percentile cohort.

### M2 — Shared Binance account runtime

Deliver:

- remove the live bootstrap restriction requiring exactly one symbol;
- introduce the supervised market-data, account-state, order, rate-limit, and
  capital-owner boundaries under one account runtime;
- multiplex and deterministically shard public Spot subscriptions for all
  configured instruments;
- hydrate filters and commissions per symbol;
- materialize one shared account balance snapshot for all configured assets and
  BNB;
- centralize User Data, rate limits, open-order tracking, order identity, and
  reconciliation;
- keep ESP order placement disabled.

Exit criteria:

- one process connection set serves both WLDUSDC and ESPUSDC;
- there is exactly one authenticated account snapshot generation;
- slow capital REST calls cannot delay parsed bookTicker evaluation;
- trading and treasury credentials remain capability-separated;
- symbol-local failures have the readiness scope defined above;
- shadow telemetry proves no WLD decision-latency or transport-liveness
  regression against M0;
- restart tests reconcile interleaved deterministic orders from two symbols
  without duplicate placement.

Rollback:

- retain the new code but select the single-symbol v12 artifact, or revert to
  the M1 compatibility adapter.

Implementation keeps the compatibility strategy projection for World Chain
execution, but derives the account-wide symbol, asset, stream-shard, and
execution-capability registries from the compiled graph. One directly-polled
Spot socket subscribes deterministically to `ESPUSDC@bookTicker`,
`WLDUSDC@bookTicker`, and WLD depth; ESP frames are parsed and emitted as
symbol-scoped observer telemetry in the same owner loop and can never enter
the WLD execution engine. The startup order is clock synchronization, User
Data subscription, then one shared `/api/v3/account` generation plus concurrent
per-symbol filters, commissions, and open-order reads. Periodic account REST
and all-symbol open-order reconciliation remain in the existing background
task and cannot head-of-line block socket parsing.

`SharedBinanceRuntime` makes market-data, account-state, User Data, order,
rate-limit, reconciliation, and capital-saga ownership explicit. Its
capability check permits WLD orders and rejects ESP orders, and the capital
owner retains the separate treasury credential scope. The durable order
journal remains account-wide; restart tests cover interleaved WLD and ESP
identities and reject duplicate placement after recovery.
`scripts/report-m2-binance-runtime START_UTC END_UTC` reports the single account
generation, both hydrated symbols, shard/capability/asset registries, direct
ESP parse latency, then runs the unchanged M0 report for WLD regression gates.

#### M2 production record

The final M2 revision
`edb650e0758eac9703997d1618743c1ca9728898` was deployed on 2026-07-29 as
immutable image
`sha256:3114902609de10be7cd60e14f771c5e6c7cb0a3694f1cbdec0a6d05a8aac9762`.
The live process hydrated one account generation with `ESPUSDC` and `WLDUSDC`,
four Spot assets, seven explicit owners, and WLD as the sole executable
instrument. During the initial half-open cohort, 653 ESP observer frames
crossed the fixed-size non-mutating record boundary with queue p95/p99/max
30/172/249 microseconds and zero drops. The contemporaneous WLD sample had
150 frames, parse p99 8 microseconds, and socket-to-decision p99 54
microseconds. Fee-500 prepared-curve p99/max was 176 microseconds. CPU p99 was
0.00918 core, cgroup peak memory was 44.4 MB, and throttling, memory pressure,
OOM, restarts, and production ERROR count were all zero. This is the reviewed
M2 deployment regression cohort, not a replacement for the formal large-sample
M0 gate.

### M3 — Network runtime registry and batched hydration

Deliver:

- create one World Chain and one Arbitrum `NetworkRuntime`;
- move RPC/WSS clients, head tracking, pool registries, wallet readers, and
  chain configuration behind the registry;
- implement priority-isolated, block-hash-pinned read classes with bounded
  batch chunking;
- share a pool mirror when multiple strategies reference the same pool;
- retain local CLMM quoting as the only executable quote path;
- define the generic `EvmExecutionOwner` command and ownership interface without
  enabling Arbitrum mutations;
- introduce chain-specific gas/fee policy and fail closed for unsupported live
  networks.

Exit criteria:

- no client is constructed per pair, tick, quote, or order;
- captured and fork/integration fixtures prove batch hydration is identical to
  individual reads at the same block;
- wallet reads, gap repair, and Quoter parity cannot head-of-line block one
  another;
- local quotes match sampled V3/V4 Quoter results within exact integer
  semantics;
- a partial batch cannot mark the affected strategy ready;
- World Chain live execution still uses the reviewed v12 gas semantics;
- Arbitrum remains read-only.

Rollback:

- use the World Chain compatibility runtime and the existing standalone ESP
  collector.

Implementation derives one typed runtime plan per compiled network. The live
projection starts World Chain and Arbitrum concurrently; the public collector
starts only Arbitrum. Each runtime owns one reusable HTTP client pool, its WSS
endpoint, initial canonical head, deduplicated pool and asset registries,
wallet location, execution lane, provider capability profile, and five
independently bounded read lanes. Startup pool and ERC-20 wallet reads use
EIP-1898 block-hash pinning and bounded JSON-RPC batches. A mismatched response
count cannot publish a wallet snapshot or ready pool generation.

The generic `EvmExecutionOwner` requires matching chain and execution-lane
identities. World Chain alone receives the reviewed v12 policy with the
100,000-wei fallback and L1 fee accounting; Arbitrum receives `ReadOnly`, and
any other executable network fails compilation. The compatibility hot owner
continues to use local V3/V4 CLMM curves only. The captured World Chain fixture
proves individual and batched reads are byte-identical at block hash
`0x8a5e…7a90`; the explicit archival-RPC integration gate proves both hookless
World Chain V4 fee tiers match the V4 Quoter with exact integer output at one
pinned head.

`scripts/report-m3-network-runtime START_UTC END_UTC` reports exact-engine,
network, generation, and read-class queue/provider/decode/publication
percentiles, completeness, EIP-1898 capability, chunks, and response bytes,
then runs the unchanged M0 WLD report.

Production record (2026-07-29): the accepted M3 revision is
`39827f6b01d14e4cd87c1de46ba684eb9df6c2ab` (CI `30468060778`, Deploy GKE
`30468466425`, immutable image
`sha256:a274156c1fc3c275de9a4fa26e11e4342baf0bc68102ee9b30244a019861fb23`).
The sole Pod was `arb-bot-5c78b9b9ff-fx6js`; both containers started at
`2026-07-29T16:05:09Z`, became Ready without a restart, and the GCE rollback
owner remained `TERMINATED`. The authoritative half-open cohort
`[16:05:09Z, 16:22:30Z)` contained two runtime identities, kept Arbitrum
execution disabled, and completed every pinned startup and wallet batch with
exact requested/returned counts. The World Chain startup lane hydrated
1,729 calls in 13 rounds; the Arbitrum collector hydrated 6,940 calls in four
rounds. Wallet queue p99 remained 2 microseconds while provider p99 reached
688,278 microseconds, demonstrating that the slow provider/indexing tail did
not head-of-line block the market-data owner.

The first M3 rollout exposed two real WLD DEX tails and was not accepted. The
owner was changed to drain newly arrived canonical events between individual
prepared-pool builds, and the fee-500 builder was then fused so its sparse
106-segment exact-input traversal reuses the exact-output capacity traversal
instead of walking the same words twice. In the accepted cohort, the fee-500
pool had 114 complete builds: build p95/p99/max was 139/155/159 microseconds
and total publication p95/p99/max was 148/160/168 microseconds. Its
receive-to-owner cohort had 124 observations at p95/p99/max 43/80/129
microseconds. WLD had 3,499 strategy frames with JSON parse p99 9
microseconds, socket-to-decision p99 45 microseconds, depth apply p99 21
microseconds, and zero hot-telemetry drops. CPU max was 0.01396 core; cgroup
memory peak was 48.0 MB; throttling, memory high/max/OOM, container restarts,
and production `ERROR` records were all zero. M3 therefore passes the frozen
WLD compatibility and the 175-microsecond DEX receive /
200-microsecond prepared-publication hard gates.

### M4 — Multi-pair hot-path decision owner

Deliver:

- move pair-specific evaluation behind a synchronous `StrategyEvaluator`
  interface owned by one hot-path runtime;
- add the compiled dependency index and immutable generation-tagged curve
  handles;
- directly poll all Binance strategy-price sockets and apply prioritized ordered
  DEX events without a Binance frame-to-evaluation handoff;
- move exhaustive sizing to bounded latest-only workers;
- add per-strategy calculation budget and overload telemetry;
- route candidates to a non-mutating coordinator sink for shadow comparison.

Exit criteria:

- deterministic replay produces the same WLD/USDC candidate and calldata bounds
  as the current engine;
- saturating ESP sizing does not affect WLD baseline ingestion or decisions;
- stale snapshot results are rejected deterministically;
- no unbounded queue exists between market data and strategies;
- an unrelated symbol or pool update does not evaluate WLD/USDC;
- production WLD execution still uses the existing coordinator path.

Rollback:

- switch WLD to the existing single-pair hot-path adapter.

The compiled graph now emits an immutable hot-path plan for every observed
strategy. `CompiledStrategyDependencyIndex` is the only `symbol -> strategy`
and `pool -> strategy` routing source; an unrelated event has no evaluator to
call. `HotPathDecisionOwner` is not a Tokio task and owns no input channel. The
task polling the combined authenticated Binance shard calls its synchronous
`StrategyEvaluator` implementations directly. A single-route event is moved
into its evaluator without even an `Arc` clone; cloning is reserved for the
future case where multiple strategies intentionally share one dependency.

The existing `TradingEngine` is the executable WLD compatibility evaluator and
retains the complete reviewed coordinator, reservations, journals, preflight,
and recovery path. The ESP evaluator is constructed without order, signer,
wallet mutation, reservation, nonce, or coordinator command handles. It owns
the Arbitrum mirror hydrated through the shared `NetworkRuntime`, publishes
baseline and exhaustive candidates only to
`TelemetryCoordinatorShadowSink`, and records
`external_mutation_authorized=false`.

Prepared curves are installed as immutable generation-tagged `Arc` handles.
The owner borrows them for baseline calculations; only a snapshot crossing to
a blocking sizing worker clones the handles. A rebuild reuses the old
allocation when uniquely owned and falls back to a deep copy only when an
in-flight worker still retains the old generation. WLD and ESP each have an
independent `LatestOnlySizingSlots`: one running snapshot and one replaceable
latest snapshot, with no unbounded queue. Results compare both Binance
connection/update identity and every pool generation before publication, so a
stale result is deterministically superseded.

Both DEX subscriptions can begin delivering ordered events while the remaining
Binance account and journal startup work is still running. Immediately before
publishing readiness, the owner therefore drains and applies both startup
backlogs, rebuilds every affected prepared generation, and records their count
and maximum queue age as `startup_dex_backlog_drain`. Those pre-owner events do
not enter `dex_event_receive_to_owner` or `head_receive_to_owner`; the latter
remain steady-state socket-to-owner latency measures. Readiness is published
only after both receivers have been observed empty.

Per-strategy hot telemetry includes the 200-microsecond baseline budget and an
explicit exceeded flag. Worker queue/runtime, superseded outcomes, latest
replacement overload, and non-mutating sink proofs are reported by
`scripts/report-m4-hot-path-runtime START_UTC END_UTC`, followed by the
unchanged WLD M0 regression report.

Production acceptance used revision
`03d474926f948bfe4f9207e313b092b8279228ac` (CI `30479065217`, Deploy GKE
`30479474198`) and image
`sha256:f0dad2465f8bb910dae10a22a9ceb7765ce6b0ce2b930ad2684d903e39844695`.
The sole Pod `arb-bot-77d458f8cc-9q2ck` started at
`2026-07-29T18:27:23Z`, became ready with both containers and zero restarts,
and proved an empty pre-ready backlog after applying four World Chain and
eight Arbitrum startup DEX events. GCE remained `TERMINATED`.

The final revision's `[18:27:23Z,18:36:22Z)` cohort contained 735 ESP
exhaustive sizing jobs (p99/max 34/42 microseconds; queue p99/max 81/282
microseconds), 764 direct ESP baselines (p99/max 15/27 microseconds), and
3,390 WLD baselines (p99/max 18/34 microseconds), with no calculation-budget
breach or hot telemetry drop. The immediately preceding production revision
had identical hot-path code and contributed 1,334 additional ESP sizing jobs;
the final change only reclassified fully known terminal DEX reverts from error
to warning. WLD JSON parse, depth, and socket-to-decision p99 were 6, 12, and
34 microseconds. World Chain fee-500 DEX receive p99/max was 43/43
microseconds and prepared publication p99/max was 140/140 microseconds, below
the frozen 175/200-microsecond hard limits. CPU max was 0.0242 core; cgroup
memory current/peak was 84.9/88.5 MB. CPU throttling, memory pressure/OOM,
container restarts, production errors, and hot-path drops were all zero.

### M5 — Portfolio owner and shared capital allocation

Deliver:

- replace `(venue, asset)` wallet accounting with
  `(inventory_location, venue_asset_id)`;
- add reviewed `VenueAssetId` to `EconomicAssetId` mappings;
- keep Binance assets account-scoped and wallet assets chain/wallet-scoped;
- centralize reservations across every strategy and rebalance operation;
- extend settlement barriers with explicit locations;
- implement an account-wide, conservation-checked `CapitalAllocator` in
  `disabled`/shadow mode for the combined graph;
- keep the live WLD rebalance behavior behind a parity adapter until replay
  proves equivalence.

Exit criteria:

- concurrent reservation tests prove two pairs cannot double-spend Binance
  USDC;
- tests prove World Chain USDC and Arbitrum USDC never collide;
- property tests prove Binance USDC is counted once across any number of wallet
  targets;
- allocator proposals conserve each economic asset across balances, fees, and
  in-flight transfers;
- trade and rebalance requests contend through the same atomic reservation
  owner;
- replay of WLD production snapshots produces the current v12 rebalance
  decision;
- Arbitrum routes remain incapable of external mutation.

Rollback:

- retain the combined market-data runtime but use the v12 inventory/rebalance
  adapter for the only live pair.

The compiled graph now emits one `CompiledPortfolioRuntimePlan`. Every venue
asset is assigned to exactly one account- or chain/wallet-scoped
`InventoryLocation` and must have a reviewed economic-asset mapping and exact
decimals. The atomic owner keys observations and pre-aggregated reservations by
`(inventory_location, venue_asset_id)`; it therefore cannot alias World Chain
USDC with Arbitrum USDC or count account-scoped Binance USDC once per strategy.
Trade and rebalance claims share this owner and settlement generations are
recorded for explicit locations rather than the ambiguous `Binance/Wallet`
pair.

The account-wide `CapitalAllocator` validates every observed and reserved
asset, in-flight transfer, proposal credit, and fee against the economic
mapping. `disabled` returns no proposal; production `shadow` may produce only
conserved proposals with `external_mutation_authorized=false`. World Chain
continues to execute the frozen tracker through
`V12RebalanceParityAdapter`; snapshot replay compares adapter and control
decisions exactly. Arbitrum startup wallet inventory enters the same portfolio
owner but its execution owner and allocator remain structurally non-mutating.
Scheduler, portfolio snapshot, reservation snapshot, allocator validation, and
unchanged WLD hot tails are reported by
`scripts/report-m5-portfolio-runtime START_UTC END_UTC`.

Production acceptance used revision
`1fe5d58399ac6f6e4dfce76969097731d6d78a5a` (CI `30483032907`, Deploy GKE
`30483422913`) and image
`sha256:31d7023f179ba510c2d0d8bcb88c2bd0187b2365bccb4764863d9743477706ab`.
The sole Pod `arb-bot-5775cd5b99-4rkqp` and both containers started at
`2026-07-29T19:20:24Z`, stayed ready with zero restarts, and reported three
inventory locations, ten venue assets, five economic assets, shadow allocator,
v12 parity adapter, and no Arbitrum or allocator mutation authority. GCE
remained `TERMINATED`.

In the authoritative half-open window
`[2026-07-29T19:20:24Z,2026-07-29T19:26:41Z)`, 313 portfolio audits all passed
conservation with zero failure or mutation record. Allocator p99/max was 7/16
microseconds, latest-only scheduler max was 81 microseconds, and portfolio
snapshot max was 4 microseconds. The shared owner completed 112 live trade
reservations at p99/max 4/5 microseconds; the v12 parity adapter p99/max was
4/12 microseconds. Under 2,021 ESP sizing jobs, WLD baseline p99/max improved
to 11/25 microseconds, JSON parse/depth/socket p99 were 5/14/35 microseconds,
and hot drops remained zero. World Chain fee-500 receive p99/max was 34/34
microseconds and prepared publication p99/max was 125/125 microseconds. CPU
max was 0.0207 core and cgroup memory current/peak was 78.8/81.3 MB; CPU
throttling, memory pressure/OOM, container restarts, and production errors were
zero. Three market-movement DEX receipts reverted with known terminal status
and remained warning-classified; they did not dispatch a Binance leg or create
an unknown outcome.

### M6 — Durable trade sagas and per-network EVM owners

Deliver:

- add the candidate scheduler, portfolio admission protocol, and one durable
  `TradeSaga` per accepted parent;
- use the single Binance order owner for the subaccount;
- create one generic `EvmExecutionOwner` per `(chain_id, wallet_id)` for swaps,
  approvals, transfers, bridges, and rebalance calls;
- make trade, DEX transaction, Binance order, and recovery journals explicitly
  account/network/strategy scoped;
- implement the parent/child fsync protocol, schema compatibility, and supervised
  shutdown;
- route every rebalancing wallet mutation through the same EVM owner;
- preserve global trade serialization for the first live revision.

Exit criteria:

- WLD/USDC replay and deterministic execution simulation preserve DEX-first
  ordering and recovery;
- two simulated strategies cannot allocate the same Binance inventory;
- trade and rebalance simulations cannot allocate the same network nonce;
- no rebalance component can access a signer or nonce allocator directly;
- World Chain and Arbitrum may both allocate nonce `N` because their chain IDs
  differ;
- unknown Binance placement and unknown EVM broadcast recover without a
  duplicate external mutation;
- only WLD/USDC is execution-enabled.

Rollback:

- deploy the last v12 single-pair revision and its compatible journals;
- do not downgrade after the new runtime has written incompatible live journal
  records unless the workflow includes a reviewed journal migration.

The M6 implementation keeps admission synchronous through the shared portfolio
owner and replaces the single pending trade slot with a bounded
latest-per-strategy scheduler. Eligible strategies are selected round-robin,
while the accepted-work lane still permits only one newly dispatching parent.
The existing production-shaped coordinator is the durable `TradeSaga`: it
fsyncs the parent before returning any DEX-first child command, and each
single-owner Binance or EVM worker fsyncs its child intent before external
mutation.

New parent and child records carry schema-v2 ownership scopes. Trade parents
record account, network, chain, wallet, strategy, and symbol; Binance orders
record account and strategy; EVM transactions record network, wallet, and
strategy; rebalance parents record account, origin network, and strategy.
Missing scopes remain readable for v1 recovery, and a scoped request may
reconcile an otherwise identical v1 Binance intent without authorizing a
duplicate order.

`RebalanceExecutor` no longer owns signer or nonce fields. Its typed
`RebalanceEvmExecutionOwner` owns both chain clients, signing material, the
durable transaction journal, and the World/Optimism nonce lanes. Trade and
rebalance lanes continue to meet at the process-scoped `(chain_id, wallet)`
nonce owner; different chains deliberately have independent nonce spaces.
The compiled startup gate fails closed unless exactly one executable strategy
exists and it is `WLDUSDC`; ESP remains observation/planning-only.

`scripts/report-m6-execution-ownership START_UTC END_UTC` reports the exact
account, owner counts, executable symbol, journal schema, scheduler policy,
parent/child fsync latency, recovery scopes, and then chains the unchanged M5
portfolio and M0 hot-path gates.

Production acceptance used revision
`012deb46722368998bd06f9e4bdfe06c713ad7cc` (CI `30486542791`, Deploy GKE
`30486903741`) and image
`sha256:e7865f41e78cf375cd6ce2e29c2b51d19acce5ce7080ce67714ac9beb4d4a7ec`.
The sole Pod `arb-bot-59cd8fd59-d6ch9` and both containers started at
`2026-07-29T20:09:17Z`, became ready with zero restarts, and GCE remained
`TERMINATED`.

In the authoritative early half-open window
`[2026-07-29T20:09:17Z,2026-07-29T20:13:20Z)`, the ownership record proved one
Binance owner, two EVM owners, one executable strategy (`WLDUSDC`), global
serialization, schema v2, and zero rebalance signer access. Trade, rebalance,
and EVM journal recovery all completed successfully. There was no live parent
dispatch in this market window; deterministic DEX-first, recovery,
parent/child fsync, v1 compatibility, shared-inventory, and same-chain/cross-
chain nonce tests therefore remain the mutation evidence.

Under 1,085 WLD strategy frames, baseline p99/max was 11/18 microseconds,
JSON parse/depth/socket p99 was 5/13/24 microseconds, World Chain fee-500
receive max was 48 microseconds, and prepared publication p99/max was 142/142
microseconds. Hot telemetry drops, production errors, container restarts, CPU
throttling, and memory pressure/OOM were zero. CPU max was 0.0163 core and
cgroup memory current/peak was 47.7/49.8 MB. These tails and resources stayed
inside the frozen M0/M5 gates, so M6 required no corrective release.

### M7 — Combined production shadow

Deliver:

- run WLD/USDC live through the new ownership graph;
- run ESP/USDC price, pool, balances, strategies, reservations, and rebalance
  planning in shadow;
- compare old-baseline and new-path WLD decisions asynchronously;
- add production alerts scoped by account, network, strategy, and execution
  lane.

Exit criteria:

- a representative production window shows no material WLD opportunity,
  sizing, execution, recovery, or realized-PnL regression;
- WLD decision latency remains within the M0 budget;
- ESP failures do not degrade WLD readiness;
- supervisor fault injection produces the documented dependency-scoped
  degradation or fail-fast behavior;
- GKE remains the only process owner and the GCE rollback target remains
  `TERMINATED`;
- no ESP signer/order mutation is possible from its strategy gates.

Rollback:

- deploy the last verified v12 single-pair digest through the GKE workflow.

M7 keeps the executable WLD evaluator unchanged inside
`HotPathDecisionOwner`; consequently there is no second implementation of
financial arithmetic that could silently drift from v12. Each immutable WLD
`PairEvaluation` already crossing the bounded hot-telemetry channel is
projected independently into the legacy-v12 and ownership-graph decision
shapes by the background writer. It publishes
`strategy_decision_compatibility` with the exact update identity, candidate
counts, queue latency, and equality result. This adds no queue, allocation, or
serialization to the accepted Binance frame-to-baseline interval.

An ESP shadow candidate now produces pure `shadow_reservation_plan` and
`shadow_rebalance_plan` records in addition to the coordinator observation.
The reservation proposal describes both exact primary debits but does not
create an `InventoryReservations` entry. The rebalance proposal deliberately
stops at the post-trade authoritative-balance trigger and has neither an
executor nor a signer/order command handle. Every record carries
`external_mutation_authorized=false`; Arbitrum execution remains disabled by
the compiled graph and network runtime.

`RootSupervisorPolicy` indexes the compiled account/network/strategy/execution-
lane scopes once at startup. Strategy and network faults degrade only that
scope; a shadow evaluator fault is retained once and subsequent ESP events are
ignored while the synchronous WLD route continues. A terminal Arbitrum shadow
connector is likewise converted into network-scoped degradation instead of
terminating the live WLD owner. Critical owner faults remain fail-fast and
telemetry faults remain observation-only. Deterministic injection covers ESP
strategy/network degradation, WLD mutation closure, critical fail-fast, and
telemetry isolation.

The `poly_bot_runtime_dependency_fault` GKE-only log metric extracts and groups
alerts by Binance account, network, strategy, execution lane, and selected
supervisor action. `scripts/report-m7-combined-shadow START_UTC END_UTC`
reports those faults, exact background WLD comparisons, ESP planning/mutation
proofs, and then chains the unchanged M6 through M0 gates.

The authoritative corrective M7 revision
`cfa316b26d5b996cf4411671908c0e8efe438c70` ran in Pod
`arb-bot-7cc7d7888f-g5dtg` from `2026-07-29T21:04:44Z`. In the first
8m16s, the exact main engine produced 1,692/1,692 matching WLD decision
projections, zero mismatches, zero dependency-scoped faults, and zero shadow
mutation-capability records. WLD socket-to-decision p99/max was 24/40
microseconds; the World Chain fee-500 prepared-curve total p99/max was
174/174 microseconds, within the frozen relative and hard M0 bounds. The
preceding runtime-identical revision produced 30 ESP candidate, reservation,
and rebalance proposals; the corrective revision changed only the report's
engine filter, while its shorter market window contained no qualifying ESP
candidate.

The same exact-Pod window had no production `ERROR`, no container restart,
0 cgroup throttles, 51.8 MB peak cgroup memory, no memory high/max/OOM event,
and 0.0148 CPU-core maximum. Both containers remained Ready and the GCE
rollback target remained `TERMINATED`. This closes M7 without changing the WLD
execution path.

### M8 — ESP/USDC Arbitrum live readiness

Deliver:

- add the reviewed Arbitrum V3 router, allowance policy, fee construction,
  receipt accounting, revert diagnostics, and transaction recovery;
- validate Arbitrum wallet funding and exact token contracts;
- validate Binance ESPUSDC filters, commissions, quantity/price rounding, IOC
  and MARKET recovery requests without enabling unrestricted live entries;
- validate Arbitrum-specific rebalance routes separately;
- validate end-to-end immutable plans through the
  candidate/portfolio/trade-saga path using deterministic fixtures and read-only
  live market data;
- prepare a new immutable artifact with an explicit ESP live gate and bounded
  canary limits, without enabling that artifact in production yet.

Exit criteria:

- local V3 quotes and calldata match block-pinned on-chain validation;
- Arbitrum gas policy has no World Chain fallback constants;
- allowance, transaction construction, and restart-recovery fixtures are
  deterministic;
- prepared primary and recovery orders pass Binance filters for both
  directions;
- opportunity telemetry reports gross and realized/counterfactual costs without
  changing the reviewed 20 bps admission model;
- deterministic failure injection covers DEX revert, unknown broadcast,
  Binance rejection, partial IOC, unknown placement, and bounded MARKET
  recovery;
- an explicit production approval is still required to select the live ESP
  artifact.

Rollback:

- disable the ESP pair execution gate; retain read-only collection.

M8 introduces `usdc-esp-arbitrum.v3.json` as a non-mutating readiness
artifact. It pins official Arbitrum One SwapRouter02
`0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45`, native USDC, ESP, the viable
V3 0.01% pool, a 10-USDC per-parent cap, 20-USDC cumulative cap, one concurrent
parent, two parents, a 15-minute window, and a 1-USDC realized-loss stop. Its
gate is `explicit_production_approval_required`; `execution_enabled`,
rebalance, and external mutation authorization remain false.

The process-scoped Arbitrum execution policy is prepared but cannot authorize
the current owner. It requires a fresh two-second `eth_gasPrice` sample, sets
the sequencer priority tip to zero, has no World Chain fallback, grants only
the exact bounded V3 allowance during a future approved startup phase, and
then locks allowance writes. Arbitrum receipt accounting uses
`gasUsed * effectiveGasPrice` without adding the World Chain-only `l1Fee`.
The existing nonce journal, known-revert diagnostics, unknown-broadcast
recovery, positional receipt settlement, and exact transfer accounting remain
shared typed components.

The live read-only M8 startup proof checks authenticated ESPUSDC filters and
commissions and constructs deterministic BUY/SELL LIMIT IOC plus BUY/SELL
MARKET recovery requests without submitting them. A block-pinned wallet proof
checks the exact token contracts, token/router bytecode, native gas floor, and
nonzero RPC fee. Binance capital metadata is independently projected into
direct Arbitrum rebalance routes for USDC and ESP without exposing an executor.
`scripts/report-m8-live-readiness START_UTC END_UTC` requires all three proofs,
four valid order shapes, two direct routes, the fail-closed gas policy, and
zero mutation capability before chaining M7 through M0.

The explicit archival-RPC parity test passed in both directions at Arbitrum
block `489077578`,
`0xe5ba358e8a603b04b7a5d07d7ad6106aa678b3709ad7ebbdf061bc500dce060a`:
the local V3 curve exactly matched QuoterV2 using EIP-1898 block pinning. The
checked calldata has the QuoterV2 tuple selector and a stable digest, while the
swap plan uses the matching SwapRouter02 `exactInput` ABI. Deterministic
fixtures also cover bounded allowances, transaction fees, restart journal
reconciliation, known DEX revert, unknown broadcast, Binance rejection,
partial IOC, unknown placement, and the three-attempt MARKET recovery bound.

The first M8 production rollout, revision
`748ac746d07a00bfc4f8fb69acde1d682f388ed1`, correctly failed its chain
readiness stage because the Arbitrum wallet held zero native ETH. The operator
funded the reviewed wallet, and a read-only Arbitrum RPC check at
`2026-07-30T02:47Z` observed `49,980,000,000,000,000 wei` (`0.04998 ETH`)
against the artifact's `1,000,000,000,000,000 wei` minimum.

M8 chain readiness is therefore refreshed outside the decision owner every
60 seconds using the isolated wallet-read class and one block-hash-pinned
wallet snapshot. Token and router code, native balance, and current RPC gas
price remain read-only inputs. Telemetry is emitted only when the complete
readiness state changes; every record keeps external mutation authorization
false. The production report selects the latest record for each readiness
stage with `argMax`, so a later degradation fails closed, while any mutation
capability record anywhere in the reporting window independently fails the
gate. This lets an operator repair native gas funding without restarting the
live WLD owner and without making an old successful readiness sample sticky.

The corrective production revision
`6c4e76b30456fac99463d06dc79417f59529ab3d` passed CI run `30509551085`
and Deploy GKE run `30509795472`. The workflow deployed immutable image
`sha256:47b4aa7037d450e45b29fd54dc671187c9b068ba2c9baaf422488649949cc0e9`
to sole Pod `arb-bot-6c4f97bfb9-9js67`; both containers started at
`2026-07-30T03:02:42Z`, became Ready at `03:02:51Z`, and had zero restarts.
The authoritative read-only window
`[2026-07-30T03:02:42Z, 2026-07-30T03:05:45Z)` reported all three M8
readiness stages ready, four valid Binance request shapes, exact token and
router code, funded native gas, a fresh fail-closed Arbitrum gas-price sample,
two direct rebalance routes, zero Arbitrum execution authority, and zero
external-mutation capability records. `m8_gate`, M7 combined shadow, and the
M5 allocator gates were ready; all 255 WLD comparison projections matched.

The same cohort had zero production `ERROR` records and zero hot-telemetry
drops. WLD Binance parse p99/max was `7/9 us`, socket-to-decision p99/max was
`44/83 us`, and the World Chain fee-500 prepared-curve total p99/max was
`33/33 us`. CPU max was `0.01395` core; cgroup memory current/peak was
`44.9/47.3 MB`; CPU throttling, memory high/max/OOM events, and container
restarts were all zero. After more than two 60-second refresh intervals, the
unchanged Arbitrum state still had exactly one startup readiness record, which
proves transition-only publication. The GCE rollback owner remained
`TERMINATED`. M8 is therefore closed without enabling an ESP trade or
rebalance mutation; M9 still requires the explicit production approval below.

### M9 — Bounded ESP/USDC live canary

Implementation status:

- the operator explicitly approved M9 at `2026-07-30T03:45:21Z`; immutable
  artifact `usdc-esp-arbitrum.v4.json` records that approval and is the only
  source allowed to project ESP execution capability;
- one process-wide trade coordinator and one dispatch mailbox serialize WLD
  and ESP parents, while a routed executor selects the already-owned World
  Chain or Arbitrum nonce lane and the one shared Binance execution service;
- both strategy engines use the same atomic inventory reservation owner.
  Binance balance generations are applied once, wallet balances remain
  network-scoped, and a competing candidate cannot reserve the same shared
  USDC twice;
- Arbitrum entry readiness is refreshed outside the decision owner. A stale
  or incomplete token, router, gas, wallet, pool, or stream state stops only
  new ESP entries; durable recovery and WLD execution remain available;
- the durable trade journal is the authority for the canary counters across
  restarts. Before every parent admission it enforces at most `10 USDC` per
  parent, `20 USDC` cumulative, `10 USDC` unhedged, `1 USDC` realized loss,
  two parents, one failed parent, one concurrent parent, and a 15-minute
  window beginning at the first admitted parent;
- Binance LIMIT IOC and bounded MARKET recovery share symbol-scoped journals
  for WLDUSDC and ESPUSDC. A request for an unreviewed symbol fails closed;
- Arbitrum allowances are bounded by the reviewed cumulative USDC cap and the
  funded ESP balance. Both token-funding minima are versioned readiness inputs;
  Arbitrum rebalance route mutation remains disabled;
- `scripts/report-m9-live-canary START_UTC END_UTC` proves the startup
  authority, latest complete readiness, exact canary caps, unique parent
  admissions, cumulative bounds, zero rebalance mutations, and the inherited
  M8 through M0 gates.

Before the production rollout, the shared Binance account already held
`10,000 ESP`, sufficient USDC and BNB, and the Arbitrum wallet held
`0.04998 ETH`. The operator subsequently approved funding the same reviewed
wallet through a one-shot direct Arbitrum rebalance. The immutable v4 artifact
therefore pins targets of at least `25 USDC` and `400 ESP`, at most two
Binance-to-wallet transfers, gross debits of at most `30 USDC` and `500 ESP`,
and withdrawal-fee caps of `5 USDC` and `100 ESP`. These are bootstrap and
fee-risk limits, not relaxed trade admission limits.

The bootstrap is not steady-state M10 rebalance authority. The GKE Deployment
uses `Recreate`, so Kubernetes removes the sole old Pod and ends its wallet,
Binance, journal, and PVC ownership before the new Pod's immutable-image
`prefund-arbitrum-m9` init container can start. That container opens the
existing durable rebalance and wallet journals, recovers the sole non-terminal
saga if one exists, reads the current Binance withdrawal fee and Arbitrum
balances, and transfers only the deficit plus that fee. A fee, minimum, integer
multiple, route, or debit outside the versioned caps fails closed. It creates no
order, DEX allowance, or wallet transaction and must finish before either M9
application container can start. Failure triggers the existing Deployment
rollback to the previous M8 owner.

On success the init container atomically fsyncs a version-, domain-, approval-,
wallet-, and target-bound completion marker to the shared PVC. A restart of the
same revision validates that marker and refuses to fund again, even if later
canary trades have reduced a token balance; normal M9 readiness then fails
closed instead of silently replenishing inventory. Before the marker exists,
partial or unknown outcomes resume through the deterministic journal identity.

The first production bootstrap attempt transferred `25 USDC` to Arbitrum; the
later approved Satoshi ownership test returned `0.9987 USDC` to Binance. The
exact `401.2 ESP` subaccount-to-master transfer also completed, but the
incorrect local-entity withdrawal endpoint rejected ESP with HTTP `400`, code
`-4024`, business detail `[031031] User does not own this currency.` No
transaction was broadcast by that request. A USDC-to-ESP swap is not an
acceptable rebalance substitute: inventory balancing must deliver ESP as ESP.

While that capability is diagnosed, production projects the v3 read-only ESP
artifact and keeps WLD v12 live unchanged. A Recreate init probe performs only
signed reads, records the account-specific questionnaire country, sanitized
master/subaccount balances, and every Binance ESP network capability, checks
both capital and Travel Rule v2 history for the rejected deterministic client
ID, and closes only that exact durable `-4024` incident. It submits no
withdrawal, order, allowance, bridge, or wallet transaction.

The first probe established questionnaire country `AE`, exact master Spot
balance `401.2 ESP`, and live direct ESP withdrawal capabilities on Arbitrum
and Ethereum. Binance's published UAE questionnaire defines
`isAddressOwner=1` as self-owned and `sendTo=1` as a private wallet, so the
submitted two-field questionnaire was already the complete UAE self-wallet
shape. The first probe appeared to expose a failed Travel Rule record, but the
authoritative post-verification v2 queries returned no matching row, including
when filtered only by ESP and Arbitrum. Binance's synchronous HTTP `400` /
`-4024` validation rejection did not produce a capital withdrawal. A later
exact v2 query exposed `trId=67181540`, `travelRuleStatus=4`, an omitted
`withdrawalStatus`, and no transaction hash. Because Binance's status `4` is
not an authorization boundary, recovery refetches that exact `trId`, requires
stable identity, proves capital history empty, proves the durable internal
transfer, and requires the master account to hold exactly `401.2 ESP` free and
zero locked. Any completed status, transaction hash, identity mismatch, or
balance mismatch fails closed.

Rails originally used `/sapi/v1/capital/withdraw/apply`; commit `6520658`
globally replaced it with `/sapi/v1/localentity/withdraw/apply`. That replacement
mixed two independent flows. Withdrawals always use the standard capital
endpoint, independent of asset, network, or amount. Travel Rule handling starts
only after a wallet-to-Binance transfer: the deposit-history row's
`requireQuestionnaire` and `travelRuleReqStatus` fields determine whether Rust
submits `deposit/provide-info`. No local amount threshold selects an endpoint.
The exact Arbitrum address is ownership-`VERIFIED` and present in the withdrawal
address list with `whiteStatus=true`.

The operator's ordinary capital withdrawal credited exactly `400 ESP` to that
wallet in transaction
`0xc65237273346c647f2e47e04ad67b81e7002eedf6da779d04a5b3c49e2fd129b`;
Binance charged `1.2 ESP`. Recovery accepts it only after matching the exact
capital-history transaction, successful receipt, token contract, recipient,
credit, fee, gross debit, zero starting wallet balance, and exhausted master
inventory. It then closes the already durable operation without another
external submission. The legacy Travel Rule row remains recovery evidence
only and cannot authorize a new withdrawal. No bridge or swap is authorized
while direct Arbitrum withdrawal remains available.

Deliver:

- enable ESP/USDC on the same Rust-owned Binance subaccount and the same
  configured signer, under the shared owners;
- make this canary the first ESP/USDC external trading mutation from the new
  runtime;
- prefund Arbitrum inventory through the reviewed one-shot bootstrap and keep
  steady-state Arbitrum rebalance mutation disabled;
- start with a versioned `CanaryPolicy` containing per-trade notional, active
  parent, cumulative notional/loss, failure-count, gas, and time-window limits;
- retain the one-newly-dispatching-parent execution policy;
- observe fills, gas, recovery, wallet settlement, BNB commissions, and
  contention with WLD/USDC.

Exit criteria:

- no duplicate orders, nonces, reservations, or transfers;
- both strategy inventories reconcile from authoritative venue observations;
- recovery exercises complete within reviewed bounds;
- shared Binance USDC contention rejects only the losing candidate;
- account-wide rate limits and BNB fee inventory remain healthy;
- canary stop conditions disable only new ESP entries while preserving
  reconciliation;
- a reviewed production cohort justifies enabling its rebalance route, raising
  limits, or allowing parallel network lanes.

Rollback:

- disable only ESP execution while WLD/USDC remains live; Arbitrum rebalance is
  not yet live in this milestone;
- reconcile any non-terminal ESP operation before changing owner revisions.

### M10 — Arbitrum rebalance live canary

Deliver:

- enable Arbitrum capital routes through the shared `CapitalAllocator` and
  durable `RebalanceSaga`;
- route Binance capital children through `BinanceCapitalSagaOwner` and every
  wallet child through the existing Arbitrum `EvmExecutionOwner`;
- begin with one external transfer at a time and independent transfer count,
  value, fee, route, and recovery limits;
- keep ESP trading enabled only if the funded balances and existing canary
  limits permit it.

Exit criteria:

- no trade and rebalance operation can double-spend Binance or Arbitrum
  inventory;
- trade, allowance, transfer, and bridge children cannot allocate the same
  Arbitrum nonce;
- Binance USDC is counted once while World Chain and Arbitrum targets are
  satisfied independently;
- route pinning and restart recovery succeed at every external-side-effect
  boundary;
- disabling Arbitrum rebalance leaves ESP trading possible while prefunded
  inventory remains sufficient.

Rollback:

- disable only Arbitrum rebalance route creation, reconcile the active saga, and
  continue WLD/USDC plus bounded ESP/USDC trading from observed inventory.

### M11 — Scale to 10–20 pairs

Deliver:

- add pairs only through modular reviewed sources compiled into a new immutable
  domain bundle;
- measure CPU, socket, RPC batch, pool-build, decision-owner, sizing-worker,
  Binance
  rate-limit, journal, and telemetry capacity;
- tune bounded network batch chunking and fair candidate scheduling;
- introduce decision sharding only if measured single-owner p99 exceeds its
  reviewed budget;
- optionally permit concurrent EVM lanes on different networks after explicit
  risk review, while retaining one nonce owner per lane;
- define per-pair and account-wide exposure caps.

Exit criteria:

- target-node p95/p99 latency and CPU headroom meet the production budget at
  maximum configured pair/pool count;
- reconnect and full rehydration complete within a measured bound;
- one noisy symbol or pool cannot starve other strategies;
- shared-account recovery and restart tests cover several simultaneous known
  and unknown operations;
- rate-limit usage remains below reviewed safety thresholds.

Rollback:

- remove strategies from a new immutable artifact without changing the shared
  ownership topology.

## Future multi-wallet extension

Multi-wallet support adds more `WalletId` values and execution lanes; it must
not change strategy code or Binance account ownership.

The future allocator will choose a wallet location before reservation using
explicit policy and observed inventory. Once admitted, the selected
`(chain_id, wallet_id)` is immutable for that parent operation and recovery.

Additional requirements will include:

- separate signer and journal ownership per wallet;
- explicit capital targets per wallet location;
- deterministic wallet selection and fairness;
- prevention of cross-wallet recovery;
- bounded total exposure across wallets;
- operator-visible wallet draining and disablement.

This extension is intentionally deferred until the one-wallet multi-pair
runtime has production evidence.

## Verification matrix

Every milestone runs `scripts/quality.sh` plus the relevant additions below:

| Layer | Required verification |
| --- | --- |
| Domain | canonical compilation, duplicate/reference/capability/property tests |
| Binance | multi-symbol replay, reconnect, rate-limit, order recovery |
| Network | block-hash-pinned hydration, read-class isolation, reorg/gap repair, partial batch failure |
| Pools | exact local/Quoter parity, shared-pool deduplication |
| Strategies | dependency-index replay, stale generation, overload isolation |
| Inventory | cross-pair contention, cross-chain separation, settlement |
| Nonces | trade/rebalance serialization, restart and unknown broadcast |
| Rebalance | location-aware route replay, idempotency, settlement barriers |
| Journals | parent/child fsync boundaries, schema migration, rollback compatibility |
| Supervision | dependency-scoped degradation, critical-owner fail-fast shutdown |
| Production | WLD parity, ESP isolation, GKE single ownership |

Production delivery continues exclusively through
`.github/workflows/deploy-gke.yml` from `main`. Structural deployment alone
must never implicitly change a pair from `disabled` or shadow to `full_live`.

## Completion definition

The migration is complete when:

- WLD/USDC and ESP/USDC run inside one process and one domain graph;
- both use one Binance account runtime, portfolio owner, capital allocator, and
  rebalance saga infrastructure;
- World Chain and Arbitrum each have one network runtime and one
  `EvmExecutionOwner` for the initial wallet;
- strategies perform no external mutations;
- local pool mirrors provide every executable quote;
- shared Binance assets and chain-specific wallet assets are reserved correctly;
- restart recovery proves ownership for every account and network lane;
- ESP/USDC has passed its separately approved canary;
- Arbitrum rebalancing has passed its separately limited live canary;
- the single hot-path owner meets its reviewed p99 budget at the configured pair
  count, or measured evidence has justified a fixed owner-sharding plan;
- adding another pair is primarily an artifact change, plus protocol/route code
  only when the new pair requires genuinely new capabilities.

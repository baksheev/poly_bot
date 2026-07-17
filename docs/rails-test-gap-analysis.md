# Full-launch test gate and Rails gap analysis

Last reviewed: 2026-07-17

## Current launch verdict

**Live arbitrage: NO-GO.** The production runtime is ready for shadow market
observation and autonomous rebalancing, but it cannot submit arbitrage orders.
This is an intentional code-level gate, not an operator setting:
`PairConfig::validate` rejects `execution_enabled=true`, and every committed
domain snapshot keeps it `false`.

**Autonomous rebalance: GO within the current caps.** All six supported direct
and Across routes have completed on the isolated production account. The
executor is journaled and recoverable, waits for both post-transfer balance
streams before releasing a route, does not stop opportunity calculation, and
has Cloud Monitoring heartbeat/fault email alerts. See
`docs/rebalancing.md` for operation IDs and balances.

**Standalone trade components: validated only behind manual 10 USDC gates.**
Uniswap V3/V4 buy and sell transactions and Binance LIMIT IOC/MARKET buy and
sell orders completed on the isolated production identities. Their dedicated
workers, journals, caps, and independent venue reconciliation are implemented.
This evidence validates the adapters; it does not validate a composed
arbitrage, shared reservations, recovery PnL, or restart of a two-leg intent.

"Full launch" in this document means that the Rust service may independently
detect an opportunity, reserve inventory, execute and reconcile both DEX and
Binance legs, recover an orphan leg or unknown outcome, and resume after a
process restart without relying on Rails, Postgres, or ClickHouse.

## Launch gate

| Gate | Status | Evidence now | Required evidence to close |
|---|---|---|---|
| Isolated production runtime and capital | Passed | Dedicated GCE runtime, wallet, Binance subaccount/keys, static egress, Secret Manager, per-token rebalance caps | Keep ownership isolated from Rails and verify the same identities at every startup |
| Market and pool state | Partial | Persistent Binance `bookTicker`; REST-bootstrapped `depth@100ms` with strict `U..u` continuity, freshness gating, exact top cross-check and full-depth bounded recovery quote; sequence-aware World Chain logs/heads; local V3/V4 state and quotes | Fault-inject Binance/Alchemy disconnect, gap, reorg, and stale-state recovery |
| Account and inventory truth | Partial | Exact Binance and wallet snapshots; one-second/block-driven refresh and open-order reconciliation; actual commission hydration; startup compiles exchange filters and rejects open orders, locked balances, and exhausted order-rate counters; one multiplexed signed WebSocket actor routes order responses and UDS events without loss; post-subscribe REST closes the startup race; deterministic order and nonce journals exist | Reconcile unresolved live child state and reservations across every forced restart boundary |
| Opportunity economics | Partial | Fixed-point direction-specific Rails 20 USDC baselines, adaptive 5–50 bps slippage plus DEX fee reserve, hydrated commission, full-depth bounded recovery, conservative maximum DEX gas, persisted primary/recovery limits, native-gas checks and atomic inventory reservations | Verify realized fee/gas/recovery accounting under composed fault injection and the capped live matrix |
| Rebalance execution | Passed | Direct and Across routes passed in both supported directions; durable journal, route locks, settlement barrier and monitoring are deployed | No launch blocker; retain regression tests and production alerts |
| Trade execution coordinator | Partial | The durable parent is connected to paper and explicitly gated composed live adapters; it persists exact DEX routes and two-sided bounded Binance recovery limits, supports `dex_first`/`concurrent_hedged`, hedges actual deltas, conservatively marks sub-step dust, and blocks unknown outcomes. Composed reject/revert/unknown and CEX/recovery restart tests pass | Run the production-shaped paper deployment and capped live canaries before selecting the v5/config live gates |
| Orphan-leg and unknown-outcome recovery | Partial | DEX nonce and Binance client-ID journals feed a composed parent; known unsubmitted/rejected legs are `Failed`, mined DEX reverts are zero-token `Failed` with actual gas, and ambiguous children are `Unknown`; residual recovery is bounded LIMIT IOC | Fault-inject Binance timeout/partial fill and EVM timeout/replacement; restart safely from every coordinator state and record realized recovery loss |
| Trading risk controls | Partial | Stale inputs fail closed; parent permits one active/unknown plan; the live gate requires exact confirmation, v5 selection, single-owner deployment, durable journals, and an entry-stop file that blocks only new entries, never restart recovery. Rust no longer adds per-plan cost, recovery-loss, cumulative-loss, total-entry, or entry-rate caps that Rails does not have. | Validate alert/runbook behavior in the GCE canary |
| Trading observability and operations | Partial | Hot-path telemetry is asynchronous; rebalance heartbeat/fault email alerts are live | Alert on stale execution inputs, unresolved intents/orders/nonces, orphan exposure, recovery loss and disabled/stopped executor; add an operator recovery/kill-switch runbook |
| Production trade validation | Partial | Dedicated-account V3/V4 swaps plus fully filled Binance LIMIT IOC and MARKET buy/sell canaries passed under capped commands; venue history and final balances agree | Complete partial-fill, User Data Stream, unknown-outcome, restart and composed arbitrage canaries below before enabling autonomous entry |

### P0 work remaining

The standalone DEX and Binance adapters are no longer P0 implementation gaps.
The following workstreams are the shortest safe path to `GO`; later rows cannot
compensate for an earlier missing ownership or recovery boundary.

| ID | Workstream | Status | Exit criterion |
|---|---|---|---|
| P0-1 | Parent trade coordinator | Partial | The durable parent owns both child legs, supports `dex_first` and `concurrent_hedged`, persists dispatch plus exact child plans before delivery, reconciles residual exposure, and blocks unknown outcomes across restart. Composed production adapters compile behind two closed live gates; fault/restart validation remains. |
| P0-2 | Persistent Binance execution state | Partial | Startup hydrates filters, order counters and open orders; Spot depth is sequence-consistent and freshness-gated; one signed multiplexing actor losslessly routes order responses and UDS events, while REST reconciliation remains independent. Forced disconnect/restart reconciliation still needs composed evidence. |
| P0-3 | Shared inventory and reservations | Partial | One in-memory owner hydrates free Binance and wallet base-unit balances. Paper trade admission atomically reserves both prefunded legs with the Rails `3x` safety multiplier; live rebalance reserves its source; unknown outcomes remain held; balanced work releases only after both venues advance. Live child orders, recovery deltas, nonce, and order-rate claims still need the same owner. |
| P0-4 | Admission economics and PnL | Partial | Parent accounting records planned/realized token-A PnL, gas, bounded recovery loss and visible sub-step dust without `f64`; ClickHouse has a Rails-comparison projection. Admission charges full recovery depth and maximum gas, and persists both price bounds. Composed/capped evidence remains. |
| P0-5 | Composed compensation and restart | Code complete; live evidence pending | DEX receipt logs and Binance fills/commissions map to exact deltas. Tests cover partial fill, deterministic CEX reject plus recovery, mined DEX revert with gas, ambiguous CEX blocking, restart before DEX/CEX/recovery, entry-stop restart recovery, and buy-vs-sell recovery after residual sign flips. Production timeout/replacement evidence remains. |
| P0-6 | Trading risk and operator controls | Partial | Per-plan cost, single active exposure, durable total entries, entry rate, cumulative comparable/recovery loss, gas reserve, stale-input limits and emergency entry stop are enforced; recovery remains allowed while entry is stopped. Production values and boundary canaries remain. |
| P0-7 | Trading alerts and runbook | Partial | The operator runbook documents single ownership, inspect/reconcile/stop/resume, one-entry canary, cumulative 100-entry cap, comparison and rollback without journal editing. Trading-specific alert policies still need production validation. |
| P0-8 | Replay, fault injection and capped canaries | Partial | Deterministic paper replay passes, every fault below ends balanced or explicitly blocked, then the remaining production matrix passes on isolated identities |

Recommended implementation order:

1. Add P0-6/P0-7 risk controls, emergency entry stop, alerts and runbook while
   recovery remains independently enabled.
2. Exhaust P0-5/P0-8 composed fault and restart tests with the code/config live
   gates still closed.
3. Run a capped composed paper/dry canary against production-shaped state and
   verify the Rails-comparison ledger.
4. Exhaust the remaining isolated production matrix rows, then run the tiny
   composed live canary on dedicated identities.
5. Keep the gate closed until 100 balanced live opportunities meet the stated
   Rails economics criterion and there are no unresolved child states.

### Required production canary matrix

Standalone component rows may be completed before the parent coordinator. A
composed or fault row starts only after its paper/fault gate exists; a live
success cannot substitute for the missing deterministic test. Transaction
hashes are abbreviated here for readability; full hashes and amounts are in
`docs/production-validation.md`.

| Canary | Status | Evidence now | Still required |
|---|---|---|---|
| Dedicated Binance MARKET buy and sell | Partial | Orders `455789048` and `455789056` filled under deterministic IDs; `allOrders`, `openOrders=0` and exact final WLD balance agree | Reconcile the same fills/commissions through User Data Stream and runtime reservations |
| Dedicated Binance LIMIT IOC full fill | Passed | Orders `455788994` and `455788998` filled and independently reconciled; final WLD balance exactly matched the start | Retain capped regression canary; this does not cover partial fill |
| Binance IOC partial fill/cancel | Pending | Exact remaining-quantity math and deterministic bounded recovery IOC planning are implemented | Force controlled partial execution; reconcile filled/cancelled quantities, commissions, balances and released reservation |
| DEX V3 USDC/WLD round trip | Passed | Buy `0xc560...985c3`, recovery sell `0xf196...27fc`; exact WLD delta unwound, nonce/journal terminal | Retain capped regression canary |
| DEX V4 USDC/WLD round trip | Passed | Buy `0xb9dc...2e0b`, sell `0x7721...5fe8`; exact WLD delta unwound, nonce/journal terminal | Retain capped regression canary |
| Binance unknown placement | Partial | Ambiguous codes/transport enter durable `outcome_unknown`; client-ID lookup blocks duplicate placement | Inject timeout after matching-engine acceptance, reconcile through WS event/query and restart to one terminal order |
| EVM unknown/replaced/reverted transaction | Partial | Mock-RPC tests prove reverted/unknown journaling and nonce hold | Exercise timeout/replacement/revert recovery with restart; prove no duplicate nonce or abandoned exposure |
| DEX-first arbitrage in both directions | Pending | Standalone child adapters passed | Hedge actual DEX delta; record planned/realized PnL and zero final exposure in both directions |
| DEX failure after CEX exposure | Pending | Admission-persisted LIMIT IOC recovery bounds and exact residual planning exist | Inject DEX failure, unwind CEX, record bounded realized loss and parent terminal state |
| CEX reject/partial fill after DEX exposure | Partial | Composed deterministic tests remove only the exact residual after reject and partial fill, then record terminal economics | Repeat through the capped production child adapters and reconcile venue history/balances |
| Restart at every coordinator state | Partial | Tests restart before DEX, CEX and recovery dispatch completion, preserve unknown blocking, select the correct directional limit, and never replay a proven earlier leg | Repeat forced process restarts in the capped production canary |
| Rebalance during trading | Pending | Rebalance stayed healthy during standalone canaries, but no shared trade reservation existed | Run a controlled overlap; prove shared reservations prevent overspend without globally pausing healthy market processing |

### Final `GO` rule

The launch verdict changes only when all rows above are `Passed`,
`scripts/quality.sh` succeeds, the production ledger contains venue-verifiable
evidence for the complete canary matrix, and there are no unresolved orders,
transactions, reservations, nonces, or rebalance operations. Passing rebalance
alone, matching the Rails test count, or observing profitable shadow signals
cannot change the verdict.

This audit compares the Rails `arb_bot` suite with the business logic that is
already present in the Rust service. It is not a target to reproduce the Rails
test count: Active Record, Active Job, factories, cache adapters, Sentry calls,
and deploy-compatibility tests do not map directly to the single-process Rust
runtime.

## Inventory

The Rails repository currently has 132 files under `spec/` and roughly 2,200
`describe`, `context`, and `it` declarations. Every spec file was inventoried.
The following areas with a current Rust equivalent were reviewed in detail:

- Binance service, price reader, order processor, capital routes, and
  withdrawal flow;
- periodic Binance account and EVM wallet balance synchronization, freshness,
  and readiness gating;
- Across bridge service, quote/status transport, supported ERC20 routes, and
  all four Rails Across fixtures;
- arbitrage detection, execution failure, and Binance hedge recovery;
- wallet transaction, signing, nonce ownership, receipt monitoring, and
  balance deltas;
- DEX quote selection and Uniswap V3 quote/swap behavior;
- rebalance planning, in-flight operations, transfer locks, deposits, and
  withdrawals.

## Coverage mapped to current Rust code

| Rails behavior | Rust location | Current result |
|---|---|---|
| Binance bookTicker parsing, precision, missing fields, invalid symbol/book | `market_data::binance`, `state` | Covered |
| Quote freshness, reconnect generations, duplicate/regressed updates | `state` | Covered, including exact age boundary |
| Binance signed account hydration, clock offset, commission, and request encoding | `binance::account`, diagnostic VM canary | Covered and live-verified from whitelisted static egress; WLD and USDC funding is present |
| Binance balance cache: one-second polling, exact decimals, expected zero balances, retry after clock resync, and snapshot publication | `balances`, `binance::account`, `state` | Implemented with a process-scoped client; value/state boundaries are unit-tested and the steady-state cadence is production-verified |
| EVM wallet balance cache: address validation, native balance, batched ERC20 `balanceOf`, and one canonical EIP-1898 block hash per snapshot | `balances`, `chain::rpc`, `state`, `market_data::alchemy` | Implemented from World Chain `newHeads`; regressed snapshots are rejected and steady-state behavior is production-verified |
| Balance readiness: both sources initialized, Binance account tradable, snapshots no older than five seconds, and last-good state retained through transient failures | `state`, `engine` | Covered at freshness and regression boundaries; end-to-end fault injection and recovery remain open |
| Binance capital routes, live network limits, Travel Rule response/history, and withdrawal identity validation | `binance::capital`, `rebalance::runtime` | Covered and live-verified through the recoverable executor |
| BUY/SELL balance deltas and commissions in base, quote, or third asset | `binance::ws_api::OrderResult` | Added in this audit |
| Binance LIMIT IOC/MARKET canary placement, deterministic IDs, terminal status and journal restart | `binance::ws_api`, `binance::execution`, `binance::order_journal`, `binance::validation` | Covered at component level and live-verified with four capped orders; autonomous execution uses bounded IOC only; forced partial/unknown outcomes remain open |
| Across HTTP status handling, bounded responses, and sanitized transport failures | `across::AcrossClient` | Covered for quote and deposit-status calls |
| Across USDC quote tokens, chains, amounts, approval, recipient, spender, calldata, and value | `across` | Covered against all Rails fixtures |
| Across ERC20 quote, calldata, approval, receipt, destination minimum, and both WLD directions | `across`, `rebalance::runtime` | Covered and live-verified through the recoverable executor |
| EVM key/raw-payload redaction, EIP-1559 signing, hydration, simulation, Rails-compatible additional gas, submission, and receipt decoding | `wallet`, `chain::rpc`, `wallet::journal`, `dex::execution`, `rebalance::executor` | Covered with a process-owned nonce lane and durable transaction recovery; V3/V4 live canaries passed |
| CLMM quoting, tick crossing, liquidity limits, both directions | `dex::clmm` | Already stronger than Rails unit coverage |
| Uniswap V3/V4 exact-input calldata, approvals/Permit2, route identity, balance delta and recovery sell | `dex::calldata`, `dex::execution`, `dex::validation` | Covered at component level and live-verified in both directions; composed hedge/recovery remains open |
| Opportunity threshold, conservative reserve, provider choice, sizing | `opportunity` | Covered, with boundary tests added |
| Rebalance targets, in-flight projection, direct/Across fallback, direction-specific availability, and live withdrawal limits | `rebalance::planner`, `binance::capital` | Covered, with validation/overflow tests and live route hydration |

The Rust suite targets business invariants and failure modes rather than Rails
framework behavior. Its count is intentionally not frozen in this document;
`scripts/quality.sh` is the authoritative verification command.

Read-only Binance diagnostics validate credential/IP restrictions, account
funding, commissions, and capital-route availability without exposing secrets.
Mutating rebalance validation now runs only through the production executor;
all six supported direct and Across operations are recorded in
`docs/rebalancing.md`.

Continuous Binance and wallet balance synchronization is deployed on GKE. The
completed rebalance matrix exercised balance refresh, reservations, nonce
ownership, allowances, and post-mutation reconciliation. Scheduler fault
injection and short-reorg recovery remain explicit test gaps below.

## Rails scenarios not ported yet

These Rails tests describe remaining features or failure-injection scenarios.
They become mandatory acceptance criteria when the corresponding component is
implemented or expanded.

### Balance synchronization and inventory readiness

The production loop now observes one Binance account and one World Chain wallet
without putting Postgres or ClickHouse on the trading path. The remaining tests
and state are required before those balances can safely authorize live orders:

- drive Binance 429, timeout, timestamp-skew, and malformed-account responses
  through the real scheduler and verify degradation, clock-resync retry, and
  recovery without overlapping polls;
- drive Alchemy WebSocket reconnects, slow HTTP batches, partial batch errors,
  missing canonical block hashes, and short reorgs through the wallet task;
- verify that `newHeads` coalescing under a slow RPC never publishes an older
  snapshot after a newer one;
- test `TradingEngine` degradation and recovery when either five-second
  freshness deadline is crossed, including background-channel shutdown;
- hydrate Binance open orders continuously in the production state owner and
  reconcile them with deterministic client IDs; the manual canary preflight
  checks `openOrders`; the authenticated User Data Stream is implemented, but
  forced disconnect/restart reconciliation still needs production evidence;
- subtract process-owned reservations and unresolved orders/transactions from
  available balances before sizing an opportunity;
- reconcile snapshots immediately after the service's own order or transaction
  instead of waiting only for the next periodic/block-driven refresh;
- add isolated lanes and acceptance tests before supporting more than the
  current single wallet, chain, and Binance account.

### Execution coordinator

Port from `perform_arbitrage_job_spec.rb` and `retry_binance_hedge_job_spec.rb`:

- submit DEX and CEX legs concurrently from one durable execution intent;
- use actual DEX receipt delta for the CEX amount;
- if DEX fails after CEX exposure, unwind only the residual with the persisted
  depth-bounded recovery IOC limit;
- if CEX IOC partially fills, recover only the exact residual within the same
  admitted price/loss bound and record realized loss;
- compose the implemented Binance client-ID reconciliation with the parent
  intent and fault-inject a timeout after matching-engine acceptance;
- prove across process restart that the implemented Binance and wallet unknown
  states prevent a second child order or nonce;
- restart from every intermediate state without duplicating a leg.

### Out-of-scope Rails components

Do not port unless they enter the Rust architecture: Active Record validation
and associations, Active Job queue configuration, Rails cache behavior, Sentry
call expectations, 0x provider code, CoinGecko, market-pair discovery reports,
rake tasks, and compatibility with partially deployed database schemas.

## Rule for future migration work

Before implementing a Rails component in Rust, read its service/job spec first,
copy the business scenarios into the implementation plan, and add the
fail-closed and recovery tests in the same change as the component. Test counts
are secondary; externally observable invariants and restart safety are the
acceptance criteria.

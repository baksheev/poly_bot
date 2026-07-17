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

"Full launch" in this document means that the Rust service may independently
detect an opportunity, reserve inventory, execute and reconcile both DEX and
Binance legs, recover an orphan leg or unknown outcome, and resume after a
process restart without relying on Rails, Postgres, or ClickHouse.

## Launch gate

| Gate | Status | Evidence now | Required evidence to close |
|---|---|---|---|
| Isolated production runtime and capital | Passed | Dedicated GKE runtime, wallet, Binance subaccount/keys, static egress, Secret Manager, per-token rebalance caps | Keep ownership isolated from Rails and verify the same identities at every startup |
| Market and pool state | Partial | Persistent Binance `bookTicker`; sequence-aware World Chain logs/heads; local V3/V4 state and quotes; freshness gating | Add sequence-consistent Binance Spot depth sufficient for the primary and recovery orders; fault-inject Binance/Alchemy disconnect, gap, reorg, and stale-state recovery |
| Account and inventory truth | Partial | Exact Binance and wallet snapshots; one-second/block-driven refresh; actual commission hydration | Hydrate exchange filters, order-rate limits, open orders and a fresh User Data Stream; reconcile deterministic client IDs; subtract reservations and unresolved venue state from available balances |
| Opportunity economics | Partial | Fixed-point two-direction quote math, threshold/reserve, local DEX liquidity sizing | Size against executable CEX depth and reserved inventory; include actual Binance fees, gas and worst-case recovery cost in admission |
| Rebalance execution | Passed | Direct and Across routes passed in both supported directions; durable journal, route locks, settlement barrier and monitoring are deployed | No launch blocker; retain regression tests and production alerts |
| Trade execution coordinator | Blocked | Binance WS API request/result primitives and wallet transaction primitives exist, but no trade coordinator is connected to `main` | Implement `dex_first` control and paper `concurrent_hedged` behind one durable intent/state machine; only then permit the config gate to be enabled |
| Orphan-leg and unknown-outcome recovery | Blocked | Rebalance and wallet recovery patterns exist; Rails behavior is documented | Reconcile Binance timeouts by client order ID; hold unknown wallet nonces; use actual DEX receipt delta; LIMIT then MARKET compensation; restart safely from every intermediate state |
| Trading risk controls | Blocked | Stale input readiness fails closed and production capital is isolated | Add per-plan/global exposure, order size, rate, loss and recovery-loss limits; one emergency entry stop that never prevents recovery; prevent a second entry while exposure is unknown |
| Trading observability and operations | Partial | Hot-path telemetry is asynchronous; rebalance heartbeat/fault email alerts are live | Alert on stale execution inputs, unresolved intents/orders/nonces, orphan exposure, recovery loss and disabled/stopped executor; add an operator recovery/kill-switch runbook |
| Production trade validation | Blocked | Read-only subaccount checks and historical pre-isolation MARKET orders passed | Complete the capped canary matrix below on the dedicated account after paper/fault gates pass |

### P0 work remaining

The following sequence is the shortest safe path to `GO`. Later items must not
be used to compensate for a missing earlier gate.

1. Build the process-owned execution state machine and durable intent journal.
   Implement `dex_first` first as the Rails-compatible control. Keep
   `execution_enabled=false` and use adapters that cannot submit in paper mode.
2. Add a persistent authenticated Binance execution session: local Spot depth,
   User Data Stream, exchange filters/rate limits, open-order hydration, and
   deterministic client-order-ID reconciliation.
3. Add atomic in-memory inventory, nonce, and order reservations shared by
   admission, execution, recovery, and rebalance. Available inventory must
   exclude every unresolved mutation.
4. Build exact DEX swap transactions from the accepted pool state, simulate
   them, journal before submission, and use receipt balance deltas rather than
   planned quantities for hedging and PnL.
5. Implement compensation and restart recovery: Binance LIMIT-to-MARKET
   fallback, unknown placement lookup, unknown nonce hold, partial-fill
   handling, and one balanced terminal state for every accepted intent.
6. Enforce explicit risk and operator controls before changing the domain gate:
   per-order/global exposure caps, maximum recovery loss, rate limits,
   freshness limits, gas reserve, emergency entry stop, and recovery priority.
7. Run deterministic replay, paper execution, and scheduler/venue fault
   injection. Every injected timeout, disconnect, partial fill, revert,
   replacement, stale snapshot, and restart must end balanced or in an explicit
   alerting fail-closed state.
8. Run capped production canaries on the isolated account, reconcile them from
   venue history and ClickHouse, then enable a tiny live budget through a
   separate explicit acknowledgement. Increasing size is a later decision.

### Required production canary matrix

This matrix begins only after P0 paper and fault-injection gates pass:

| Canary | Required result |
|---|---|
| Dedicated Binance MARKET buy and sell | Both fill under deterministic client IDs; account, User Data Stream, order query, fills, commissions and balance deltas agree |
| Binance IOC/partial fill | Filled and cancelled quantities reconcile exactly; no reservation leaks |
| DEX USDC to WLD and WLD to USDC | Simulation, submission, receipt, token deltas, nonce and pool-state reconciliation agree |
| DEX-first arbitrage in both directions | Hedge uses actual DEX delta; realized net PnL and final exposure are recorded |
| Binance unknown placement | Timeout cannot create a duplicate; client-ID lookup reaches one terminal state |
| EVM unknown/replaced/reverted transaction | Nonce lane stays held until the chain outcome is known; restart does not duplicate the leg |
| DEX failure after CEX exposure | LIMIT unwind is attempted, MARKET fallback removes residual exposure, realized loss is recorded |
| CEX reject/partial fill after DEX exposure | Recovery removes residual exposure within the configured maximum loss |
| Restart at every coordinator state | No duplicated order/transaction and no silently abandoned exposure |
| Rebalance during trading | It may run in parallel, but reservations prevent overspend; only insufficient available inventory blocks a new trade |

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
| Across HTTP status handling, bounded responses, and sanitized transport failures | `across::AcrossClient` | Covered for quote and deposit-status calls |
| Across USDC quote tokens, chains, amounts, approval, recipient, spender, calldata, and value | `across` | Covered against all Rails fixtures |
| Across ERC20 quote, calldata, approval, receipt, destination minimum, and both WLD directions | `across`, `rebalance::runtime` | Covered and live-verified through the recoverable executor |
| EVM key/raw-payload redaction, EIP-1559 signing, hydration, simulation, gas, submission, and receipt decoding | `wallet`, `chain::rpc`, `wallet::journal`, `rebalance::executor` | Covered with a process-owned nonce lane and durable transaction recovery |
| CLMM quoting, tick crossing, liquidity limits, both directions | `dex::clmm` | Already stronger than Rails unit coverage |
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
- hydrate Binance open orders and reconcile them with deterministic client
  order IDs; an authenticated user-data stream is not implemented;
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
- if DEX fails, unwind CEX by LIMIT at the original price and then MARKET;
- if CEX LIMIT fails, retry MARKET and record the realized loss;
- reconcile an unknown Binance placement by deterministic client order ID;
- never submit a second order while the first result is unknown;
- hold the wallet lane when an approval or swap nonce is unknown;
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

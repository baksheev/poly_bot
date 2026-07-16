# Rails test gap analysis

Last reviewed: 2026-07-17

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

## Deliberately not ported yet

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

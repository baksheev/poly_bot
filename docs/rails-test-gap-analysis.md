# Rails test gap analysis

This audit compares the Rails `arb_bot` suite with the business logic that is
already present in the Rust service. It is not a target to reproduce the Rails
test count: Active Record, Active Job, factories, cache adapters, Sentry calls,
and deploy-compatibility tests do not map directly to the single-process Rust
runtime.

## Inventory

The Rails repository currently has 132 files under `spec/` and roughly 2,200
`describe`, `context`, and `it` declarations. Every spec file was inventoried.
The following areas with a current Rust equivalent were reviewed in detail:

- Binance service, price reader, order processor, and withdrawal flow;
- Across bridge service and all four Across fixtures;
- arbitrage detection, execution failure, and Binance hedge recovery;
- wallet transaction, nonce ownership, receipt monitoring, and balance deltas;
- DEX quote selection and Uniswap V3 quote/swap behavior;
- rebalance planning, in-flight operations, transfer locks, deposits, and
  withdrawals.

## Coverage mapped to current Rust code

| Rails behavior | Rust location | Current result |
|---|---|---|
| Binance bookTicker parsing, precision, missing fields, invalid symbol/book | `market_data::binance`, `state` | Covered |
| Quote freshness, reconnect generations, duplicate/regressed updates | `state` | Covered, including exact age boundary |
| Binance signed request encoding and Travel Rule response/history | `binance::account`, `binance::capital` | Covered for implemented calls |
| BUY/SELL balance deltas and commissions in base, quote, or third asset | `binance::ws_api::OrderResult` | Added in this audit |
| Across quote tokens, chains, amounts, recipient, spender, calldata, value | `across` | Added in this audit |
| Across pending/filled status and minimum received amount | `across` | Added in this audit |
| CLMM quoting, tick crossing, liquidity limits, both directions | `dex::clmm` | Already stronger than Rails unit coverage |
| Opportunity threshold, conservative reserve, provider choice, sizing | `opportunity` | Covered, with boundary tests added |
| Rebalance targets, in-flight projection, route fallback, live limits | `rebalance::planner` | Covered, with validation/overflow tests added |

The Rust suite increased from 83 to 119 tests. The additional tests target
business invariants and failure modes rather than framework behavior.

## Deliberately not ported yet

These Rails tests describe features that do not yet exist in Rust. Adding mock
tests before the state and ownership models exist would create false confidence.
They become mandatory acceptance criteria when the corresponding component is
implemented.

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

### EVM transaction owner

Port from `eth_wallet_service_spec.rb`:

- one nonce owner per chain and wallet;
- nonce allocation and broadcast inside the same critical section;
- `already known` returns the locally derived transaction hash;
- pending, successful, reverted, missing, and timed-out receipts;
- bounded gas estimation margin and EIP-1559 fee caps;
- ERC20 approve/transfer calldata, native value, address, and uint256 bounds;
- token balance deltas from logs, including multiple transfers and unrelated
  logs.

### Rebalance executor and recovery journal

Port from the Binance deposit/withdrawal and rebalance job specs:

- persist intent before the first external mutation and fsync it;
- resume accepted Binance withdrawals by `withdrawOrderId`/`trId`;
- use the actual bridge destination amount for the following transfer;
- approval, bridge, Binance transfer, and credit monitoring are separately
  resumable and idempotent;
- Travel Rule deposit questionnaire is submitted only when required and not
  already passed;
- completed transfer directions remain locked until balances are reconciled;
- per-wallet native gas minimum participates in readiness and planning.

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

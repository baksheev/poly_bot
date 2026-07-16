# Binance runtime parity and low-latency design

Status: authenticated read-only slice deployed; execution and rebalance remain disabled
Last reviewed: 2026-07-16

## Decisions

- Rust trades Spot, never USD-M Futures.
- Rust uses a dedicated Binance subaccount and a dedicated EVM execution
  wallet. Rails keeps its current Binance account and wallets.
- Rust owns capital rebalancing for its subaccount and wallet. Rails must not
  transfer their funds or submit transactions from the Rust wallet.
- The initial credential check may use the existing Rails credentials, but
  live Rust execution receives a dedicated subaccount API key before entries
  are enabled.
- Market data, account state, orders, fills, reservations, and recovery state
  live in the Rust process. Postgres and ClickHouse are not coordination or
  readiness dependencies.

## Rails endpoint inventory

The inventory below comes from `BinanceService`, `BinanceSwapService`, the
price relay, balance snapshots, gas-price refresh, profit investment, and
arbitrage execution jobs in the Rails application.

| Rails endpoint or stream | Current purpose | Rust decision |
|---|---|---|
| `GET /api/v3/time` | Cache server clock offset for signed requests | Keep. Measure request midpoint, refresh periodically and immediately after `-1021`. |
| Spot/Futures `<symbol>@bookTicker` | Current bid, ask, and best-level quantities | Keep only the Spot stream. It remains the fastest opportunity trigger. |
| `GET /api/v3/ticker/bookTicker` | REST fallback, batch observations, gas prices | Do not use in the hot path. The Spot WebSocket owns live prices; REST is diagnostic recovery only. |
| `GET /api/v3/depth` | Not present in Rails | Add for local Spot-book bootstrap, followed by diff-depth updates and gap repair. |
| `<symbol>@depth@100ms` | Not present in Rails | Add for executable capacity and market-fallback risk. Never use Futures depth. |
| `GET /api/v3/exchangeInfo` | Discover pairs and obtain filters | Keep at startup and on explicit metadata refresh. Compile filters into per-symbol in-memory values. |
| `GET /api/v3/account` | Not present in Rails | Add as the trading-account hydration source for permissions and nonzero free/locked balances. |
| `GET /api/v3/account/commission` | Not present in Rails | Add at startup per traded symbol so opportunity math uses the real account fee. |
| `POST /sapi/v3/asset/getUserAsset` | Rails balance snapshots, investment, and rebalance | Do not use for the Rust trading path. `account` plus User Data Stream is lower-weight and directly matches Spot execution state. |
| `POST /api/v3/order`, `LIMIT IOC` | Primary Rails hedge at the opportunity price | Preserve semantics, but send with WebSocket API `order.place` on a persistent connection. |
| `POST /api/v3/order`, `MARKET quantity` | Hedge fallback and recovery | Preserve semantics for the unfilled residual only. Send with WebSocket API `order.place`. |
| `POST /api/v3/order`, `MARKET quoteOrderQty` | Periodic investment of excess token-A profit | Preserve only in the later non-critical parity slice; it is not part of arbitrage execution. |
| `GET /api/v3/order` | Find an order by order ID or deterministic client order ID after an ambiguous create | Preserve as `order.status` over WebSocket API, with REST as an independent recovery fallback. |
| User Data Stream | Missing in Rails | Add. `executionReport`, `outboundAccountPosition`, and `balanceUpdate` are the primary order/fill/balance state source. |
| `GET /api/v3/openOrders` | Missing in Rails | Add at startup/reconnect for reconciliation of the Rust client-order namespace. |
| `GET /api/v3/myTrades` | Missing in Rails | Add only for restart recovery when fills cannot be reconstructed from orders and the journal. |
| `GET /api/v3/rateLimit/order` | Missing in Rails | Add outside the hot path for readiness and rate-limit telemetry. |
| `GET /sapi/v1/capital/config/getall` | Rails chooses direct vs bridge and refreshes withdrawal limits | Keep in the cold path. Hydrate live network enablement, `busy`, fee, min/max, and integer multiple before every rebalance reservation. Prefer direct `WLD`, fall back to `OPTIMISM` plus Across independently for deposit and withdrawal. |

The following Rails endpoints stay out of the trading hot path but are needed
by the Rust rebalance state machine:

- `GET /sapi/v1/capital/deposit/address/list`;
- `GET /sapi/v2/localentity/deposit/history`;
- `PUT /sapi/v2/localentity/deposit/provide-info`;
- `POST /sapi/v1/capital/withdraw/apply` for the current non-local-entity
  subaccount, or explicit `POST /sapi/v1/localentity/withdraw/apply` mode when
  Binance reports a Travel Rule questionnaire requirement;
- `GET /sapi/v1/capital/withdraw/history`.

## Rails execution semantics to preserve

Rails currently:

1. Rounds Binance quantity to the configured step and the limit price in the
   aggressive direction: buy up, sell down.
2. Creates an IOC limit order with deterministic ID `arb<swap-id>L`.
3. If the create result is ambiguous, queries by client order ID before any
   retry.
4. If the IOC expires after a partial fill, submits a market order only for
   the remaining quantity using `arb<swap-id>M`.
5. If the initial Binance leg fails after DEX success, retries the market hedge
   asynchronously until exposure is closed.

Rust preserves those economic semantics, but not the Rails retry latency. A
single in-memory order state machine owns each attempt and consumes both the
request response and User Data Stream events. Deterministic rejections stop
immediately. Network timeout, HTTP 5xx, disconnect, or a missing response means
`UNKNOWN`, never `FAILED`: Rust waits briefly for `executionReport`, queries
the client order ID, and only resends after proving the order absent.

## Faster transport

The target topology uses three persistent Binance connections:

1. Spot market-data WebSocket for `bookTicker` and diff depth.
2. Spot WebSocket API for `order.place`, `order.status`, account queries, and
   the User Data Stream subscription.
3. A pooled REST client for depth bootstrap and independent recovery.

The current shared HMAC key can sign every WebSocket API request. A later
Ed25519 subaccount key allows `session.logon`, removes per-request API-key and
signature fields after authentication, and is Binance's preferred key type.
It must be benchmarked from the Singapore VM before changing the signer.

For REST recovery, benchmark `api.binance.com`, `api-gcp.binance.com`, and the
performance endpoints `api1` through `api4` from the production VM. Binance
documents `api1` through `api4` as potentially faster but less stable, so they
must never be the only recovery route.

## Authenticated diagnostic boundary

Run manual authenticated Binance reads through `arb-bot-binance-test`, not
from a developer workstation and not by opening SSH on the production VM. Its
static Singapore egress IP is `34.143.148.4` and must remain in the applicable
Binance API-key IP whitelist. Direct internet SSH is blocked; the repository
wrapper connects through IAP:

```bash
scripts/gce-binance-test binance-account
scripts/gce-binance-test binance-capital
scripts/gce-binance-test binance-recent-validation-orders --limit 20
```

The remote wrapper independently allowlists read-only subcommands. Do not add
order, withdrawal, wallet, or bridge commands to it. A mutating canary requires
its existing explicit cap, `--confirm-live`, deterministic recovery identity,
and a separately reviewed execution path.

The diagnostic VM uses a dedicated service account with access only to the
Binance API key/secret and Artifact Registry image. Its currently validated
image is source revision `1c6eb17a6954`, pinned to digest
`sha256:a2325f44b3907c782656dbc15198c3806a427197f5404a969ba4732e8d0fab22`.
Refresh the VM only with `scripts/update-gce-binance-test-image` and another
digest-pinned image; never point it at a mutable tag.

On 2026-07-16 the account check authenticated successfully as Spot with
`canTrade=true`, hydrated the WLDUSDC commission, and reported exactly two
nonzero balances with both WLD and USDC present. The capital check found WLD
direct and Optimism routes available in both directions; USDC had no direct
route and had Optimism available in both directions. These reads confirm
funding and current capabilities, but they do not satisfy order-placement or
rebalance recovery readiness gates. A read-only `allOrders` audit found no
recent `rustval...` orders in this dedicated subaccount. Earlier MARKET canary
orders were placed with pre-isolation credentials and must not be treated as
execution evidence for the funded subaccount.

## Subaccount and rebalance boundary

The production Rust subaccount should have:

- its own Spot balances and API key;
- Spot read and trading permissions;
- withdrawals disabled on the trading key;
- IP restriction to the VM static address `34.21.220.162`;
- a Rust-specific client order ID prefix, separate from Rails `arb...` IDs.

A second, IP-restricted master treasury API key is required for withdrawals
from the isolated subaccount. It is loaded by the cold-path rebalance owner
only and is never accepted by an order client. The master first performs a
deterministically identified Universal Transfer from subaccount Spot to master
Spot, then submits the external withdrawal. Withdrawal-address whitelisting is
required if the Binance account supports it. Runtime capability discovery must
fail closed if the master cannot resolve the configured subaccount, transfer
from it, or withdraw externally.

The same in-process owner serializes DEX and rebalance wallet nonces. The
initial safe implementation pauses new entries before reserving a rebalance
operation and resumes only after balances, nonce, and external transfer state
are reconciled. See `docs/rebalancing.md`.

## Readiness gates before order placement

Live Binance order placement remains disabled until all of these are true:

- clock synchronization is fresh;
- account type is `SPOT` and `canTrade=true`;
- WLD and USDC balances are hydrated;
- actual per-symbol commission is loaded;
- exchange filters are loaded and every planned price/quantity passes them;
- local depth is sequence-consistent;
- User Data Stream is subscribed and fresh;
- open orders and the Rust client-order namespace are reconciled;
- in-memory free/locked balances match the latest account generation;
- no order or fill remains in an unknown state.

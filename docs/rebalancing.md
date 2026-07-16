# Autonomous rebalance design

Status: planner implemented; all external mutations disabled
Last reviewed: 2026-07-16

## Ownership and safety boundary

Rust owns one Binance Spot subaccount and one EVM wallet. Rails owns neither
and must not rebalance their capital. This removes both Binance balance races
and EVM nonce races.

Use two Binance credentials:

1. A trading key with read and Spot trading permissions, withdrawals disabled,
   and an IP restriction to the production VM.
2. A treasury key with only the permissions required for deposit/withdrawal
   reconciliation and withdrawals, the same IP restriction, and withdrawal
   address whitelisting where available.

The credentials used for initial parity testing are not production-isolated.
Rotate them before live trading and replace them with keys belonging to the
Rust subaccount.

New entries remain disabled while a rebalance operation owns the wallet. The
single runtime owner reserves inventory and serializes every EVM nonce for DEX
execution, approvals, bridges, and transfers.

## Planner parity

The pure `rebalance::planner` module operates only on integer base units. It:

- projects active transfers before making another plan;
- fails closed if projected arithmetic is inconsistent;
- rejects inventory below `binance_min + wallet_min`;
- targets half of total inventory on Binance and half in the wallet;
- refills the deficient side using only surplus above the other side's minimum;
- applies Binance withdrawal minimum, maximum, and integer-multiple rules;
- selects routes from live Binance `depositEnable`, `withdrawEnable`, and
  `busy` state independently for each direction;
- prefers the direct World Chain route and falls back to Optimism plus Across;
- emits at most one action for one token per planning pass;
- fails closed when neither a direct route nor a verified Across direction is
  currently available.

The production Rails configuration captured on 2026-07-16 for World Chain
pair `WLDUSDC` (pair id 3) is:

| Token | Binance minimum | Wallet minimum | Withdrawal min / max / multiple | Route |
|---|---:|---:|---:|---|
| USDC | 2,000 | 2,000 | 5 / 9,999,999 / 0.000001 | Prefer direct only when live; otherwise Binance Optimism plus Across between chain 10 and World Chain 480 |
| WLD | 4,000 | 4,000 | 0.2 / 8,700,000 / 0.00000001 | Prefer Binance `WLD`; fall back to Binance `OPTIMISM` plus Across when the direct network disappears |

These values are evidence for parity tests, not immutable production limits.
At runtime the Binance capital configuration is authoritative for network
enablement, fees, and withdrawal constraints. A changed or unavailable route
blocks rebalance and therefore blocks new entries once inventory is unsafe.

Route choice follows the Rails behavior but is stricter:

1. For Binance to wallet, use the target network only while live
   `withdrawEnable=true` and `busy=false`.
2. Otherwise require the configured Optimism network to be withdrawable and a
   fresh Across Optimism-to-World-Chain quote.
3. For wallet to Binance, repeat the decision independently using
   `depositEnable`; withdrawal availability must not influence deposit routing.
4. Use withdrawal min/max/multiple and fee from the selected network, because
   the Optimism constraints can differ from the `WLD` constraints.
5. Recheck immediately before reserving the operation. Once the first external
   side effect is submitted, pin that route and recover it rather than silently
   switching routes midway.

## Execution state machine

Every transfer follows a recoverable sequence:

1. Hydrate Binance balances, wallet token balances, gas, network capabilities,
   deposit addresses, nonce, and all active external operations.
2. Plan and reserve the amount in memory; close the live-entry gate.
3. Write a durable operation intent before the first external side effect.
4. Execute one idempotent step at a time with deterministic identifiers.
5. Confirm both the source debit and destination credit from authoritative
   sources, not merely an HTTP success response.
6. Rehydrate all balances and nonce, close the operation, then reopen entries
   only if every normal readiness gate also passes.

Routes:

- WLD Binance to wallet: submit a withdrawal with deterministic
  `withdrawOrderId`, reconcile withdrawal history, transaction receipt, and
  final World Chain balance.
- WLD wallet to Binance: transfer to a freshly verified Binance deposit
  address, confirm the chain receipt, then reconcile Binance deposit history
  and credited balance.
- USDC Binance to wallet: withdraw native USDC to the Optimism wallet, measure
  the actual received amount, request a fresh Across quote, execute approvals
  and bridge transaction, then confirm the World Chain credit.
- USDC wallet to Binance: bridge World Chain USDC to Optimism first, measure the
  actual destination amount, transfer it to the verified Binance deposit
  address, then reconcile the Binance credit.

For Rails parity, Across uses the public unauthenticated
`https://app.across.to/api/swap/approval` endpoint. It sends no API key and no
Integrator ID. The endpoint produces optional approval transactions and the
on-chain swap transaction; Rust signs and submits those transactions itself.
The response is short-lived and must never be cached. Before signing, validate
the returned chain ID, token/spender, transaction target, recipient, input and
minimum output amounts, value, deadline, and calldata against the reserved
rebalance operation. Never trust response-provided gas blindly: estimate it on
the origin RPC and apply a bounded margin.

Completion uses the public `/deposit/status?depositTxnRef=...` endpoint for
Rails parity. It is a secondary tracker and can lag its indexer, so the origin
receipt, destination-chain receipt or balance delta, and expected recipient
remain authoritative reconciliation evidence. This path is API-assisted
calldata generation followed by an on-chain transaction; it does not require
Across credentials.

Official references:

- <https://docs.across.to/introduction/swap-api>
- <https://docs.across.to/introduction/tracking-deposits>
- <https://developers.binance.com/en/docs/wallet/capital/withdraw>
- <https://developers.binance.com/en/docs/wallet/capital/deposite-history>

## Recovery durability

ClickHouse telemetry remains asynchronous and cannot authorize a transfer.
Long-running rebalance operations need a small synchronous durable journal
outside the hot trading path. Before enabling mutation, provision a persistent
GCE disk and append/fsync the operation intent, deterministic Binance ID,
signed EVM transaction hash, nonce, step, and confirmation evidence. Startup
recovery reconciles that journal against Binance and both chains before
planning anything new.

This durability cost does not affect opportunity detection or order latency;
it is paid only by the cold rebalance path.

## Delivery phases

1. Pure planner and parity tests — implemented.
2. Read-only hydrator for wallet balances, gas, Binance network capabilities,
   deposit address, deposit history, and withdrawal history.
3. Persistent operation journal and startup recovery.
4. Paper executor that validates generated routes and calldata without signing.
5. Direct WLD transfers behind a separate explicit live gate.
6. Across USDC route behind its own explicit live gate.
7. Rebalance soak test, failure injection, and only then trading integration.

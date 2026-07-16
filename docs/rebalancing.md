# Autonomous rebalance design

Status: full executor and GKE template implemented; shared-key mode selected for the isolated subaccount; first live rollout pending
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

This is the preferred least-privilege topology, not a functional requirement.
For the isolated Rust subaccount, the operator selected explicit
`shared_trading` mode. It reuses one process-scoped client and one credential
pair for reads, trading, and treasury operations. This increases credential
blast radius and must never be enabled implicitly or used to share ownership
with the Rails bot.

The credentials used for initial parity testing are not production-isolated.
Rotate them before live trading and replace them with keys belonging to the
Rust subaccount.

New entries remain disabled while a rebalance operation owns the wallet. The
single runtime owner reserves inventory and serializes every EVM nonce for DEX
execution, approvals, bridges, and transfers.

## Planner parity

The runtime captures the first complete, fresh Binance plus World Chain wallet
snapshot after startup as the reference maximum inventory for each token. The
reference is process-scoped and never ratchets down during the process lifetime.
The versioned domain artifact supplies `rebalance.start_threshold_bps`; v3 uses
`2500`, so either location triggers at 25% of that startup total. The target
remains Rails-compatible: half of the latest projected total inventory.

The `rebalance` modules operate only on integer base units. The planner:

- projects active transfers before making another plan;
- fails closed if projected arithmetic is inconsistent;
- derives the Binance and wallet start limits from the startup reference;
- rejects inventory below twice the derived start balance;
- targets half of total inventory on Binance and half in the wallet;
- refills the deficient side using only surplus above the other side's minimum;
- applies Binance withdrawal minimum, maximum, and integer-multiple rules;
- selects routes from live Binance `depositEnable`, `withdrawEnable`, and
  `busy` state independently for each direction;
- prefers the direct World Chain route and falls back to Optimism plus Across;
- emits at most one action for one token per planning pass;
- fails closed when neither a direct route nor a verified Across direction is
  currently available.

The steady-state balance owner evaluates the planner whenever either balance
snapshot changes and emits `rebalance_plan_evaluated` telemetry. When an action
is required or planning fails, the normal readiness gate closes, so new trading
cannot start while inventory is unsafe. `disabled` remains the default and does
not submit transfers, approvals, bridges, withdrawals, signatures, or other
external mutations.

The first production execution slice is deliberately narrower than the full
planner. `direct_wld_canary` permits exactly one Binance-to-World-Chain WLD
withdrawal, capped at 1 WLD. It does not execute USDC, Across,
wallet-to-Binance, or a second WLD operation. Those plans remain fail-closed
and observable.

`full_live` connects the same planner to the recoverable executor. The GKE
Deployment template selects it, but no GKE workload has been rolled out yet.
Startup requires an explicitly selected Binance credential mode, a wallet
signer, both durable journals, positive per-token caps, and the exact
`REBALANCE_LIVE_CONFIRMATION=ENABLE_FULL_REBALANCE` acknowledgement.
Before opening either journal, startup verifies the selected Binance key has
reading, withdrawal, and IP-restriction permissions through
`account/apiRestrictions`; account-level `canWithdraw` alone is insufficient.
The current subaccount uses explicit `standard` withdrawal API mode; the
`localentity` Travel Rule endpoint is available only through explicit
`travel_rule` configuration and is never used as an implicit fallback.
The worker uses a bounded cold-path channel, so Binance, Across, and RPC waits
never run inside the market-data loop. Only one operation may be active; after
completion, both Binance and wallet snapshots must refresh before another plan
can be dispatched.

## Wallet primitive

The reusable wallet layer provides the cold-path subset used by the full
rebalance executor:

- canonical-block hydration of native balance, ERC-20 balances, and requested
  allowances through a process-scoped RPC client;
- both latest and pending nonces, with an explicit unresolved-pending signal;
- validated native transfer, exact ERC-20 `transfer`, exact ERC-20 `approve`,
  and protocol-validated contract-call construction;
- an unsigned RPC call representation for mandatory simulation and gas
  estimation before signing;
- checked maximum native-cost calculation and validated EIP-1559 chain, nonce,
  gas, and fee fields;
- redacted local signing, deterministic transaction hash calculation, and a
  broadcast helper that rejects an RPC-returned hash mismatch.

The existing explicitly gated Across native-ETH canary uses this shared API.
Disabled and direct-canary runtime modes still load only the public wallet
address. `full_live` additionally loads the signer and a second Optimism RPC.
The canary and full executor require durable journals and use the nonce lane
state machine. On startup the runtime automatically queries a known
unresolved hash: a matching receipt durably closes the operation, while a
transaction without a receipt is accepted only after chain, sender, nonce,
target, value, and calldata hash match the journaled intent. Missing, replaced,
or unsigned operations remain blocked for operator review. The first GKE
deployment uses the verified isolated subaccount, operator-selected caps, and
shared-key acknowledgement. The Rails failure-mode review is recorded in
[Rails wallet failure lessons](rails-wallet-parity.md).

Explicitly gated mutation commands require `EVM_WALLET_JOURNAL_PATH`. Its
parent directory must already exist; a new journal is created with mode `0600`
on Unix, existing group/world-readable files are rejected, and an exclusive OS
file lock prevents a second local process from owning the same journal.

## Binance capital recovery hydrator

The read-only Binance capital hydrator now retrieves and validates:

- the exact enabled default EVM deposit address for one coin and network;
- Travel Rule deposit history by EVM transaction hash;
- capital withdrawal history, matched locally by the deterministic
  `withdrawOrderId` rather than the unrelated Travel Rule submission ID;
- exact decimal amounts and fees plus typed deposit and withdrawal states.

It intentionally preserves two Rails recovery lessons: deposit transaction
hashes are compared case-insensitively, and deposit status `6` is credited even
though Binance still prevents withdrawal. Missing history remains "not indexed
yet" evidence and never proves failure. Duplicate records, a mismatched
coin/network, an ambiguous or tagged deposit address, malformed hashes, and
non-EVM addresses fail closed. See
[Rails Binance capital failure lessons](rails-binance-capital-parity.md).

The diagnostic command performs only signed GET requests:

```bash
cargo run -- binance-capital-recovery \
  --coin USDC \
  --network OPTIMISM \
  --deposit-transaction-hash 0x... \
  --withdraw-order-id rustwd123
```

The production Rails configuration captured on 2026-07-16 for World Chain
pair `WLDUSDC` (pair id 3) is:

| Token | Binance minimum | Wallet minimum | Withdrawal min / max / multiple | Route |
|---|---:|---:|---:|---|
| USDC | 2,000 | 2,000 | 5 / 9,999,999 / 0.000001 | Prefer direct only when live; otherwise Binance Optimism plus Across between chain 10 and World Chain 480 |
| WLD | 4,000 | 4,000 | 0.2 / 8,700,000 / 0.00000001 | Prefer Binance `WLD`; fall back to Binance `OPTIMISM` plus Across when the direct network disappears |

These values are historical Rails evidence for parity tests, not Rust runtime
limits. Rust derives its start limits from the isolated account's startup
inventory instead.
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

Routes for both WLD and USDC:

- Direct Binance to wallet: submit a withdrawal with deterministic
  `withdrawOrderId` through the explicitly selected standard or Travel Rule
  API, reconcile withdrawal history, and confirm the final World Chain balance.
- Direct wallet to Binance: transfer to a freshly verified Binance deposit
  address, confirm the chain receipt, then reconcile Travel Rule state,
  deposit history, and credited balance.
- Across Binance to wallet: withdraw to Optimism, measure the actual amount
  received after the Binance fee, execute any required approval plus the
  validated Across call, then confirm the World Chain credit.
- Across wallet to Binance: bridge World Chain to Optimism, measure the actual
  output, transfer that exact amount to the verified Binance deposit address,
  then reconcile the Binance credit.

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

The direct-WLD canary uses a checksummed append-only JSONL journal on a zonal
ReadWriteOnce persistent disk. It fsyncs the operation identity, amount,
destination, and deterministic `withdrawOrderId` before calling Binance. A
restart reconciles history first. If an intent exists but Binance has not yet
indexed a matching withdrawal, the runtime stops for operator review and never
blindly resubmits. Completion requires both Binance status `6` with a
transaction ID and the expected World Chain wallet balance increase.

The full executor adds a second checksummed, fsynced high-level journal. It
records the pinned route and each authoritative confirmation. Before an Across
submission it stores the validated target, exact calldata, calldata hash,
minimum output, and destination balance baseline. The existing transaction
journal then records nonce, signed hash, broadcast, unknown outcome, and
receipt. Recovery can therefore continue the exact call and never substitute a
new quote after an ambiguous outcome.

## Delivery phases

1. Pure planner and parity tests — implemented.
2. Continuous balance integration, startup-derived thresholds, live Binance
   network capability hydration, paper plans, and readiness gating — implemented.
3. Reusable wallet hydration, call construction, signing, and checked broadcast
   primitives — implemented; signing is loaded only in explicitly gated modes.
4. Read-only hydrator for deposit address, deposit history, and withdrawal
   history — implemented with a diagnostic recovery command.
5. Checksummed fsynced operation journal and per-chain nonce lane — implemented
   and used by the gated EVM canary, including conservative startup
   reconciliation of unresolved transaction hashes.
6. Recoverable direct and Across executor for WLD and USDC — implemented behind
   the explicit `full_live` gate; no production activation has occurred.
7. One-shot direct WLD Binance-to-wallet transfer behind a capped production
   gate — implemented and retained as the GKE control mode.
8. Explicit separate-treasury or shared-trading credential mode, per-token
   caps, exact live acknowledgement, route revalidation, and dual-chain nonce
   recovery — implemented.
9. Isolated-capital canaries, failure injection, soak testing, and only then a
   reviewed first GKE `full_live` rollout — pending.

# Autonomous rebalancing

Status: enabled in production on GKE; all Binance/World Chain directions that
are supported by the live accounts have completed successfully.

Last reviewed: 2026-07-17.

## Scope and ownership

The Rust process owns one Binance Spot subaccount and one World Chain wallet.
No Rails process may trade, rebalance, withdraw, deposit, or allocate EVM
nonces for these funds.

The runtime has two Binance identities with different jobs:

- `BINANCE_API_KEY` and `BINANCE_SECRET_KEY` belong to the trading subaccount.
  They hydrate balances and capital state and receive deposits.
- `BINANCE_TREASURY_API_KEY` and `BINANCE_TREASURY_SECRET_KEY` belong to the
  master account. They perform Universal Transfer from the subaccount and
  external withdrawals.
- `BINANCE_SUBACCOUNT_EMAIL` identifies the only subaccount that the treasury
  client may use.

A separate treasury identity is mandatory. Binance subaccounts cannot perform
the external withdrawal used by Binance-to-wallet rebalancing. There is no
shared-credential mode or runtime flag for it.

The wallet signer is loaded only when `REBALANCE_EXECUTION_MODE=full_live`.
The same process owns the World Chain and Optimism nonce lanes. A rebalance
operation closes the normal live-entry gate until the operation is terminal
and fresh balances from both venues have been observed.

## Sources of truth

Decisions use only in-memory state hydrated from authoritative external
sources:

- Binance Spot account state for free and locked subaccount balances;
- World Chain RPC for wallet balances and nonce state;
- Binance capital configuration for live network availability, fees, minimum,
  maximum, and integer-multiple withdrawal constraints;
- Binance transfer, withdrawal, and deposit history for reconciliation;
- World Chain and Optimism receipts and balances for EVM reconciliation;
- Across quotes and deposit status for validated bridge calldata and secondary
  fill evidence.

ClickHouse is telemetry, never an execution dependency. A slow or failed
ClickHouse write cannot delay planning, signing, transfer submission, or
recovery.

## Inventory reference, threshold, and target

For each token, the first complete fresh snapshot after process startup sets:

```text
reference_inventory = binance_balance + wallet_balance
start_balance       = reference_inventory * start_threshold_bps / 10_000
```

The production domain artifact uses `start_threshold_bps = 2500`, so a venue
becomes unsafe below 25% of the startup inventory. The reference is fixed for
the process lifetime and never ratchets down after fees or transfers.

For every later snapshot:

```text
current_total = projected_binance + projected_wallet
binance_target = floor(current_total / 2)
wallet_target  = current_total - binance_target
```

If Binance is below `start_balance`, the planner transfers wallet surplus
toward `binance_target`. If the wallet is below `start_balance`, it transfers
Binance surplus toward `wallet_target`. It does nothing while both venues are
at or above the threshold.

The planner fails closed when:

- total inventory is below twice the startup-derived minimum;
- an observed or projected debit is arithmetically impossible;
- no route supports the required direction;
- a Binance withdrawal cannot satisfy the selected network's current limits;
- token metadata, decimal precision, or route identity differs from the
  approved domain.

All financial arithmetic uses `U256` base units or validated `Decimal` values.
`f64` is not used. Binance balances with more fractional digits than an on-chain
token are floored only for observation; transaction amounts must convert
exactly.

The tracker evaluates both USDC and WLD on every balance update. The engine
dispatches at most one operation at a time. After the first token completes and
fresh snapshots arrive, a pending action for the second token remains eligible;
it is not lost when the first budget is reconciled.

## Live route matrix

Routes are derived on startup from one live Binance capital snapshot. The
planner orders the direct World Chain network before the Optimism fallback and
checks deposit and withdrawal availability independently.

| Token | Direction | Preferred route | Fallback | Production validation |
|---|---|---|---|---|
| USDC | Binance to wallet | unavailable on Binance World Chain | Optimism + Across | passed |
| USDC | wallet to Binance | unavailable on Binance World Chain | Across + Optimism | passed |
| WLD | Binance to wallet | Binance `WLD` direct | Binance `OPTIMISM` + Across | both passed |
| WLD | wallet to Binance | Binance `WLD` direct | Across + Binance `OPTIMISM` | both passed |

The live account currently reports:

- WLD `WLD`: deposit and withdrawal available;
- WLD `OPTIMISM`: deposit and withdrawal available;
- USDC `WLD`: absent;
- USDC `OPTIMISM`: deposit and withdrawal available.

Therefore a direct USDC test on World Chain is not missing; Binance does not
offer that route for this account. WLD direct is normally selected. WLD Across
is a real fallback and was verified in both directions with a bounded,
fail-closed production exercise; the temporary forced-route configuration used
for that exercise has been removed.

Route availability is checked again immediately before a withdrawal or
deposit. After the first external side effect, the route is pinned in the
durable intent. Recovery never switches an in-flight operation to another
network or replaces its validated Across calldata.

## Planner and executor boundary

The market-data and balance owner runs the planner synchronously in memory. It
does not wait for Binance REST, Across, an RPC receipt, or disk I/O.

When an action is eligible, the owner sends a bounded request to one cold-path
executor task. The executor:

1. validates token, wallet, direction, route, observed balances, and the
   configured per-token maximum;
2. writes and fsyncs a deterministic operation intent;
3. performs one external step at a time;
4. records authoritative evidence after every step;
5. returns a terminal result to the owner;
6. waits for newly observed Binance and wallet snapshots before another
   operation can be dispatched.

Only one non-terminal operation may exist. The executor journal file lock and
the GKE `Recreate` rollout with a ReadWriteOnce volume enforce a single process
owner.

## Route execution

### Direct Binance to wallet

1. Transfer the exact requested amount from subaccount Spot to master Spot by
   Universal Transfer with a deterministic `clientTranId`.
2. Confirm the internal transfer in Binance history.
3. Revalidate the pinned direct network and withdrawal constraints.
4. Submit the master-account withdrawal with a deterministic
   `withdrawOrderId`.
5. Reconcile Binance withdrawal history and the World Chain wallet credit.

### Direct wallet to Binance

1. Fetch the network-scoped EVM deposit address from
   `/sapi/v1/capital/deposit/address`.
2. Validate coin identity, nonzero EVM address, and absence of a tag.
3. Transfer the exact ERC-20 amount on World Chain.
4. Confirm the receipt, then reconcile Binance deposit history and credited
   balance.

### Across Binance to wallet

1. Transfer the requested amount from subaccount Spot to master Spot.
2. Withdraw through Binance `OPTIMISM` to the configured wallet.
3. Read the completed withdrawal and measure the actual Optimism receipt after
   the Binance fee.
4. Request Across calldata for that net amount.
5. Validate and execute any required ERC-20 approval.
6. Persist the exact bridge target, input, calldata, calldata hash, minimum
   output, and destination balance baseline before signing.
7. Execute the Optimism bridge transaction and confirm the World Chain fill.

The withdrawal history amount is treated as net received. The requested debit
is `amount + transactionFee`. The approval and bridge input use the net amount,
not the original Binance debit.

### Across wallet to Binance

1. Request and validate Across calldata from World Chain to Optimism.
2. Execute approval when required and then the pinned bridge call.
3. Measure the actual Optimism output.
4. Fetch and validate the Binance Optimism deposit address.
5. Transfer exactly the received amount to Binance.
6. Reconcile the EVM receipt, Travel Rule deposit state, optional questionnaire,
   and final credited balance.

Binance may expose more decimal digits in account observations than a token or
deposit credit supports. The credited amount is reconciled exactly as reported;
the planner tolerates normal fee and precision residue while both venues remain
above their startup threshold.

## Across validation

The runtime uses `https://app.across.to/api/swap/approval` without credentials.
The quote is short-lived and is never cached as executable state.

Before signing, Rust validates:

- origin and destination chain IDs (`10` and `480`);
- exact input and output token contracts;
- depositor and recipient;
- exact input and minimum output;
- approval token, spender, amount, and allowance;
- transaction target, native value, calldata selector, encoded fields, and
  expiry;
- RPC simulation, gas estimate, and bounded EIP-1559 fees.

Across `/deposit/status` is secondary evidence. The code accepts the current
filled response shape where legacy output amount/token fields may be absent,
but still requires the reserved origin hash, destination chain, and a valid
fill transaction reference. When amount fields are present, they must meet the
reserved minimum.

Both WLD directions are covered with 18-decimal validation tests. Approval
calldata also accepts a full `uint256` allowance; it is not narrowed to `u128`.

## Binance permissions and Travel Rule behavior

Startup validates both account views before opening the live executor:

- the trading key can read the isolated Spot account and reports the expected
  WLD/USDC balances;
- the master key has reading, withdrawals, Internal Transfer, Universal
  Transfer, and trusted-IP restrictions;
- the master view of the subaccount matches the trading-key view;
- both credentials are restricted to the production egress IP.

Production uses `REBALANCE_BINANCE_WITHDRAWAL_API_MODE=travel_rule` and the
Binance local-entity endpoint. A rejection from one withdrawal API is never
interpreted as permission to retry through another API.

Deposit reconciliation uses Travel Rule history. A deposit with a required
questionnaire is submitted once and then polled until credited. A missing
history row means "not indexed yet", not failure. Transaction hashes are
matched case-insensitively. Duplicate matches, wrong networks, malformed hashes,
or ambiguous addresses fail closed.

## Durability, idempotency, and recovery

Two synchronous JSONL journals live on `/var/lib/arb-bot`:

- `REBALANCE_EXECUTOR_JOURNAL_PATH` stores the high-level operation and pinned
  route;
- `EVM_WALLET_JOURNAL_PATH` stores per-chain nonce, call identity, signed hash,
  broadcast outcome, and receipt.

Both journals:

- are append-only, checksummed, and fsynced;
- are created with mode `0600` and reject group/world-readable files;
- reject symlinks, partial records, sequence gaps, invalid transitions, and
  multiple owners;
- never store a private key or raw signed transaction;
- retain backward checksum compatibility for journal records written before
  the approval input amount was introduced.

High-level progress includes intent, subaccount transfer, withdrawal, funds on
the bridge chain, approval, prepared bridge calldata, mined bridge, Across fill,
deposit transfer, Binance credit, and terminal completion or failure.

At startup, `recover_active` resumes the only non-terminal operation before a
new planner action is allowed. Known transaction hashes are checked against the
chain. A matching receipt closes the step. A transaction without a receipt is
accepted only after sender, chain, nonce, target, value, and calldata hash match
the journal. Missing, replaced, unsigned, or contradictory evidence remains
blocked for operator review; the runtime never blindly resubmits an unknown
outcome.

## Runtime configuration

Production mutation requires all of the following:

- `REBALANCE_EXECUTION_MODE=full_live`;
- `REBALANCE_LIVE_CONFIRMATION=ENABLE_FULL_REBALANCE`;
- positive `REBALANCE_MAX_WLD_AMOUNT` and `REBALANCE_MAX_USDC_AMOUNT`;
- `REBALANCE_EXECUTOR_TIMEOUT_SECONDS` between 60 and 86,400;
- `REBALANCE_BINANCE_WITHDRAWAL_API_MODE` set to `standard` or `travel_rule`;
- the trading, treasury, subaccount, wallet, RPC, and journal configuration
  described above.

`disabled` is the only other execution mode. Retired canary modes, route-force
flags, canary amount flags, and the separate canary journal no longer exist.
The production GKE manifest sets `full_live`; configuration validation still
defaults to `disabled` outside that manifest.

## Telemetry and operations

The engine emits bounded asynchronous ClickHouse records for:

- Binance and wallet balance snapshots;
- reference inventory, threshold, targets, selected direction, amount, and
  route for every changed plan;
- balance-source failures and readiness changes;
- the normal hot-path opportunity and runtime state.

The durable journal, Binance history, and chain receipts authorize recovery.
ClickHouse is used to audit what the in-memory owner observed and decided.

Useful read-only commands remain available:

```bash
cargo run -- binance-account
cargo run -- binance-capital
cargo run -- binance-capital-recovery \
  --coin WLD \
  --network OPTIMISM \
  --deposit-transaction-hash 0x... \
  --withdraw-order-id rb...
cargo run -- wallet-hydrate
cargo run -- across-usdc-quote --origin-chain-id 480 --amount 1000000
```

One-off CLI commands that placed MARKET orders, bought gas, withdrew canary
funds, or bridged native ETH were removed after production validation. New
financial mutations must go through the recoverable executor.

## Production validation record

The supported matrix was completed on the isolated production account:

| Operation | Route | Result |
|---|---|---|
| USDC Binance to wallet | Binance Optimism, Across to World Chain | completed |
| USDC wallet to Binance | Across to Optimism, Binance deposit | completed |
| WLD Binance to wallet | Binance `WLD` direct | completed |
| WLD wallet to Binance | Binance `WLD` direct deposit | completed |
| WLD Binance to wallet | Binance Optimism, Across to World Chain | completed |
| WLD wallet to Binance | Across to Optimism, Binance deposit | completed |

The final WLD fallback operations were:

- `rebalance-27-6022301e0756c887`, Binance to wallet, requested `875.5 WLD`,
  completed with `874.827444370905913695 WLD` delivered on World Chain;
- `rebalance-37-ba77134a174e0d45`, wallet to Binance, input
  `500.163722185452956847 WLD`, completed with `499.82385325 WLD` credited by
  Binance.

After the temporary reserve was returned, ClickHouse showed:

- Binance: `649.893890 USDC`, `1249.29385325 WLD`;
- World Chain wallet: `349.942972 USDC`,
  `1249.633722185452956848 WLD`;
- WLD reference after the clean restart:
  `2498.927575435452956848 WLD`;
- planner action: none.

The verification route override was removed and the clean GKE revision started
with normal automatic route preference and no active journal operation.

## Tests that protect live findings

Regression coverage includes:

- the actual 1,000 USDC / 2,500 WLD two-token budget sequence;
- retention of the second token action after the first token completes;
- both WLD Across directions with 18 decimals;
- the observed post-fee WLD balances remaining safely idle;
- Binance withdrawal requested versus net-received fee semantics for USDC and
  WLD;
- full-`uint256` approval allowances and approval input equal to the net bridge
  amount;
- current Across filled status without legacy amount fields;
- singular network-scoped Binance deposit-address endpoint;
- deposit hash matching, Travel Rule questionnaire state, and exact deposit
  credit;
- legacy journal checksum and approval recovery;
- one active executor, legal state transitions, nonce ownership, and unknown
  outcome recovery.

Run the complete repository gate before every release:

```bash
scripts/quality.sh
```

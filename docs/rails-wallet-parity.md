# Rails wallet failure lessons

Status: encoded in the Rust nonce lane and transaction journal
Reviewed: 2026-07-16

The Rails application remains a behavioral specification, not a runtime
dependency. This review covered `EthWalletService`, wallet reservations, DEX
swap error propagation, `PerformArbitrageJob`, their specs, and the wallet fix
history in `/Users/baksheev/code/arb_bot`.

## Invariants retained in Rust

| Rails evidence | Failure that it prevents | Rust behavior |
|---|---|---|
| `0fc89bc` serializes nonce selection and broadcast by chain plus wallet | Parallel workers selecting the same nonce | One process-owned `NonceLane` accepts only one active operation for one chain and wallet |
| `f071ffa` moves pending waiting inside the nonce lock | Waiting outside the lock followed by a stale nonce selection | Lane hydration evaluates latest nonce, pending nonce, and the durable journal together before accepting work |
| `send_raw` spec for `already known` | Treating an idempotent rebroadcast as a failed transaction | Broadcast returns the locally calculated signed transaction hash and does not allocate or submit another nonce |
| `nonce too low` handling | Blind retry that can duplicate an already executed effect | No automatic retry or fresh-nonce submission; the signed operation remains recoverable by its local hash |
| `16acfa1` recovery hold | Releasing inventory or the wallet lane after a receipt timeout | Any error after signing or broadcast becomes `OutcomeUnknown` and blocks new lane reservations |
| `f9bd0f2` receipt-only balance reconciliation | Requiring a transaction lookup after a receipt already proves mining | A matching canonical receipt finalizes success or revert and consumes the nonce |
| Wallet reservation idempotency keys | Duplicate work after job retry | Journal operation IDs are globally unique and a duplicate intent fails closed |

## Rust state model

The durable sequence is:

1. `intent_recorded` is appended and fsynced before the nonce is exposed.
2. `signed` records the locally calculated transaction hash, chain, wallet,
   and nonce. Raw signed bytes and calldata are never journaled.
3. `broadcast` records a matching RPC submission, including `already known`.
4. A transport/RPC/confirmation ambiguity records `outcome_unknown` and keeps
   the lane blocked across restart.
5. A matching receipt records `mined_success` or `mined_reverted`; both consume
   the nonce.
6. Only an intent that has never been signed may be cancelled and reuse its
   nonce.

At startup, a known unresolved hash is checked for a receipt first. A matching
receipt is sufficient evidence and is appended as `mined_success` or
`mined_reverted`. Without a receipt, `eth_getTransactionByHash` must match the
journaled chain, sender, nonce, target, native value, and calldata hash. A known
pending transaction, missing hash, nonce occupied by another transaction, or
unsigned intent keeps the lane blocked; recovery never silently allocates a
fresh nonce.

Every JSONL record has a monotonically increasing sequence, version, timestamp,
and SHA-256 checksum. Appends call `sync_data`. A partial record, checksum
mismatch, sequence gap, identity change, transaction-hash change, insecure file
permissions, second process lock, duplicate nonce owner, or illegal state
transition prevents the journal from opening or accepting work.

## Intentional differences from Rails

- Rust does not use an expiring Redis/cache lock. One owner holds the in-memory
  lane, while the journal survives restart. Deployment still needs exclusive
  process fencing before autonomous live execution.
- Rust does not continue when pending-nonce hydration fails and does not treat
  an RPC error as permission to continue.
- Rust does not automatically replace an underpriced transaction with a fresh
  nonce. Replacement requires a separately designed same-nonce fee-bump event
  and reconciliation of every known hash.
- The journal is synchronous only on the cold execution path. ClickHouse
  remains asynchronous telemetry and cannot authorize or recover a transfer.

## Remaining recovery work

- Apply the implemented EVM startup reconciler to future autonomous executors
  on every supported chain, not only the explicitly gated canary.
- Add an operator-reviewed path for a recovered unsigned intent and for a
  signed hash proven absent while latest and pending nonce remain unchanged.
- Add same-nonce fee replacement without losing the original transaction hash.
- Add inventory/gas reservations tied to the journal operation ID.
- Add exclusive runtime fencing so two Pods cannot open independent lanes for
  the same wallet.

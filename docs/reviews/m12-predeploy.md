# M12 pre-deploy review: full calculated ESP/USDC rebalance

Status: two endpoint probes failed closed; the operator completed the exact
withdrawal in Binance UI, root cause is reproduced from Binance history, and
one consolidated recovery/routing deployment is pending.

The operator explicitly approved immediate ESP/USDC rebalancing on
2026-07-31. The allocator's last production snapshot requests one direct
Binance-to-Arbitrum transfer of
`4,464.93818055 ESP` (`4464938180550000000000` base units), from
`9,464.8 ESP` on Binance and `534.923638887482447575 ESP` in the wallet toward
the existing 50/50 target. M12 removes the obsolete `401.2 ESP` canary
truncation. It does not make the executor unbounded: the immutable approval
session has a `10,000 ESP` catastrophic-error ceiling, enough to execute the
complete current plan but not an erroneous unit-scaled or duplicate debit.

## External mutation matrix

- [x] The only newly authorized external mutation is a direct ESP withdrawal
  from the configured Binance subaccount to the existing wallet on network
  `ARBITRUM` / chain `42161`.
- [x] Every withdrawal starts with
  `POST /sapi/v1/capital/withdraw/apply`; no asset, network, or locally guessed
  amount selects another endpoint. Only the exact synchronous Binance `-4104`
  response durably routes the same request to
  `POST /sapi/v1/localentity/withdraw/apply`.
- [x] The fallback reuses the exact ownership proof stored by Binance for this
  address (`VERIFIED`, `satoshiToken=USDC`, `verifyMethod=1`,
  `isAddressOwner=1`, `sendTo=1`, `vaspName=Unhosted Wallet`). This fixes the
  missing metadata that caused the API path to differ from the successful UI
  path.
- [x] The current plan is exactly `4,464.93818055 ESP`; the runtime does not
  split or truncate it to the historical `401.2 ESP` canary amount.
- [x] Bridge mutations remain disabled. Optimism cannot be selected by the M12
  approval, and wallet-to-Binance, USDC, trade, allowance, and rebalance
  authority are unchanged.
- [x] The live fee is re-read immediately before submission and must be no more
  than `2 ESP`; the durable amount plus fee authority is checked before the
  deterministic withdrawal request is sent.
- [x] The shared Binance capital owner and one global rebalance mutation lane
  remain mandatory; an active historical operation blocks M12 creation.

## Unknown-outcome and restart matrix

- [x] The R2 approval session ID is included in the request fingerprint and
  durable intent before any Binance transfer or withdrawal side effect.
- [x] A restart reconstructs R2 count, amount, fee, first-start timestamp, and
  active state from the journal and cannot start another transfer.
- [x] A withdrawal submission with an unknown result performs at most one
  deterministic reconciliation query and never authorizes resubmission.
- [x] A standard `-4104` history row and a later local-entity row may share the
  economic identity even when Binance omits `withdrawOrderId` on one row.
  Exact manual recovery selects the two versioned trIds from network history.
  Both reviewed unbroadcast rows carry the full gross debit in `amount` and an
  empty `transactionFee`; an empty fee is accepted only when `amount` already
  equals the durable gross debit. Net amounts still require an exact numeric
  fee before they can match.
  Restart recovery validates all matching rows, ignores only explicit
  unbroadcast failures, accepts at most one viable submission, and never
  replays an ambiguous POST.
- [x] The failed R2 ESP operation may reopen only when operation ID,
  fingerprint, `withdrawOrderId`, amount, wallet, network, successful master
  transfer `396036135710`, exact `-4024` rejection, and zero matching capital
  history all match the versioned correction. It reuses the existing master
  inventory and contains no second `universalTransfer`.
- [x] Completed R1 risk does not consume R2 authority, while an active R1
  operation still owns the global mutation lane and must recover first.
- [x] Historical checksummed R1 records written before the session field remain
  recoverable only as
  `esp-usdc-arbitrum-rebalance-20260730-r1`; no missing field can inherit R2.
- [x] A request with a missing, different, malformed, or unreviewed approval
  session fails before journal reservation.

## Versioned artifact semantic diff

- [x] V4 remains immutable and maps to the historical R1 approval. V5 records
  actor `operator`, timestamp `2026-07-31T08:09:37Z`, and session
  `esp-usdc-arbitrum-rebalance-20260731-r2`.
- [x] The only live rebalance-policy changes are the new approval session and
  debit ceilings: USDC `25 -> 2,600`, ESP `401.2 -> 10,000`. Fee, count,
  concurrency, failure, 15-minute, single-query, direct-route, and no-bridge
  controls are unchanged.
- [x] The corrective semantic diff starts new withdrawals at
  `capital/withdraw/apply`, routes only exact `-4104` to Travel Rule, and adds
  the exact R2 recovery evidence. Historical modes remain replayable but cannot
  select the endpoint for a new submission.
- [x] The manual receipt is pinned to Binance withdrawal
  `e02357b25de24e1ba9965bf524db37f7`, transaction
  `0x553d9635dab1477c6aab9a17fc4ab860040e44db8ca085cb894a6b3184bc27fd`,
  gross debit `4,464.93818055 ESP`, fee `1.1 ESP`, credit
  `4,463.83818055 ESP`, master transfer `396036135710`, and the two
  unbroadcast bot rows `67294348` / `67298920`.
- [x] The higher USDC ceiling is only a symmetric catastrophic-error bound for
  the same two-token allocator; the observed plan requests ESP and does not
  create a USDC action.
- [x] Configuration accepts only the exact reviewed R1 or R2 session/cap tuple.
  Changing the session, increasing a cap, partially enabling a route, or
  enabling bridge mutation fails validation.
- [x] The canonical compiled bundle was regenerated from the V5 source and the
  deployment workflow asserts the exact snapshot, session, timestamp, caps,
  route, and recovery controls before rollout.

## Latency and resource observation plan

- [x] M10/M12 risk, saga, and Binance child telemetry is tagged with the exact
  approval session; reporting filters R2 so R1 rows cannot be combined.
- [x] The report fails on more than two transfers, one concurrent or failed
  transfer, `2,600 USDC`, `10,000 ESP`, `5 USDC`, or `2 ESP` cumulative
  authority, or any allocator/saga/child/EVM failure.
- [x] Post-deploy verification will inspect the exact withdrawal operation and
  Binance/wallet balances, then compare WLD parse, socket-to-decision, DEX
  receive/build/total, telemetry drops, production errors, CPU, throttling,
  memory, OOM, and restarts over a half-open window.
- [x] Reporting and journal writes remain on existing bounded background or
  durable execution paths; no new allocation, lookup, or handoff was added to
  the WLD decision hot path.

## Final diff review

- [x] Targeted compile, exact 4,464.93818055 ESP authorization, 10,001 ESP
  rejection, R1/R2 isolation, restart replay, config, reporting, and deployment
  contract tests pass locally.
- [x] A read-only production query reconfirmed the exact planner inputs:
  reference `9,999.723638887482447575 ESP`, Binance `9,464.8 ESP`, wallet
  `534.923638887482447575 ESP`, direct Arbitrum amount
  `4,464.93818055 ESP`.
- [x] The current production owner has no active operation: the R2 ESP
  operation is terminal failed after a successful master transfer and
  synchronous unbroadcast `-4024` then `-4104` responses. The operator's exact
  UI withdrawal is complete; read-only Arbitrum state is
  `4,998.761819437482447575 ESP`, exactly the reviewed pre-balance plus the
  receipt credit. Recovery contains no transfer or withdrawal call.
- [x] The complete diff is reviewed as one change before push. The final
  `scripts/predeploy-review` and `scripts/quality.sh` gates must pass on the
  same revision; there will be one clean fast-forward push and one deployment.

# M12 pre-deploy review: full calculated ESP/USDC rebalance

Status: implementation review complete; production evidence pending the single
consolidated deployment.

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
- [x] Withdrawal submission remains the Rails-compatible local-entity flow;
  Travel Rule questionnaire logic remains deposit-only and no asset or amount
  selects another withdrawal endpoint.
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
- [x] The current production owner has no active rebalance operation and its
  historical R1 window is terminal; no local production mutation was used for
  this review.
- [x] The complete diff is reviewed as one change before push. The final
  `scripts/predeploy-review` and `scripts/quality.sh` gates must pass on the
  same revision; there will be one clean fast-forward push and one deployment.

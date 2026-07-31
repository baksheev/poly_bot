# M10 pre-deploy review

This artifact remains incomplete until every gate below is supported by a test
or an exact diff reference. `scripts/predeploy-review
docs/reviews/m10-predeploy.md` runs the automated contracts before reporting
unchecked manual gates, and `scripts/quality.sh` is required afterwards. No M10
revision may be pushed to `main` merely to discover a locally reviewable
contract error.

## External mutation matrix

- [x] Binance-to-wallet USDC and ESP use only Rails-compatible
  `POST /sapi/v1/localentity/withdraw/apply` with the inline self-wallet
  questionnaire; asset, network, and amount cannot select a different
  withdrawal endpoint. Evidence:
  `tests/binance_capital_contract.rs`.
- [x] Wallet-to-Binance first obtains the exact Binance deposit address, sends
  the ERC-20 transfer on the pinned route chain, then reads deposit history.
  Evidence: `RebalanceExecutor::direct_wallet_to_binance` and
  `direct_arbitrum_deposit_settlement_keeps_the_pinned_wallet_chain`.
- [x] Travel Rule questionnaire submission occurs only after a matching
  deposit record says it is required; it never changes withdrawal routing.
  Evidence: `deposit_questionnaire_matches_rails_order_and_is_durable_before_submission`.
- [x] Every Arbitrum wallet child goes through the same process-scoped
  `EvmExecutionOwner` and nonce journal as ESP trade and allowance children.
  Evidence:
  `capital_handle_uses_the_same_nonce_journal_and_cannot_keep_service_alive`.
- [x] Bridge and steady-state Optimism mutations remain impossible in M10.
  Evidence: `m10_authority_cannot_select_a_bridge_or_another_network`.
- [x] Live Binance withdrawal fee is re-read immediately before submission and
  cannot exceed the durable per-operation and cumulative canary authority.
  Evidence: `withdrawal_unknown_outcome_and_live_fee_recheck_are_fail_closed`
  and `live_canary_authorizes_only_one_bounded_direct_transfer`.

## Unknown-outcome and restart matrix

- [x] A recovered Binance internal-transfer intent queries exact deterministic
  history; an existing transfer resumes and an absent result fails closed
  without submitting a second transfer. Evidence:
  `RebalanceExecutor::begin_master_transfer`.
- [x] Restart after internal transfer submission reconciles its deterministic
  client ID and never creates a second transfer. Evidence:
  `persists_master_transfer_before_binance_withdrawal`.
- [x] Binance withdrawal intent is durable before the request; an unindexed
  unknown submission fails closed after the single allowed reconciliation
  query and cannot resubmit. Evidence:
  `restart_preserves_unknown_local_entity_withdrawal_without_resubmission_authority`.
- [x] The pre-existing World Chain USDC operation
  `rebalance-288-18c185631ae867dd` was previously closed read-only after its
  standard-endpoint attempt remained unindexed and the operator confirmed
  absence. That historical recovery is no longer selected by the active
  artifact.
- [x] The first M10 rollout closed that older operation read-only, then exposed
  a separate pre-transfer crash intent
  `rebalance-294-96fd53e70c1ab390` for 1,197.503244 USDC. The process crashed
  four milliseconds after the durable `intent_recorded` row, exact
  deterministic master-transfer history was empty after restart, and the
  operator separately verified that Binance contains no withdrawal for that
  amount. The versioned corrective recovery waits at least 300 seconds from
  the first empty observation, repeats only the exact master-transfer history
  query, requires the full immutable operation fingerprint/route/balances and
  closes the local journal without transfer or withdrawal calls. Evidence:
  `operator_absence_closes_only_the_exact_pretransfer_crash_intent` and
  `pretransfer_crash_recovery_is_read_only_and_cannot_create_capital_work`.
- [x] The corrective rollout reused that deterministic identity, completed the
  exact master transfer `395924104268`, and then received synchronous HTTP 400 /
  `-4104` from the incorrect standard endpoint before Binance created a
  withdrawal. The operator confirmed that no withdrawal exists. Active recovery
  binds operation `rebalance-296-96fd53e70c1ab390`, full fingerprint, client ID,
  amount, route, transfer ID, response code/message, and unchanged Optimism
  balance; it cannot query or submit another withdrawal. All subsequent
  withdrawals use the Rails-compatible local-entity endpoint. Evidence:
  `operator_absence_closes_only_the_exact_synchronously_rejected_standard_withdrawal`
  and `operator_absence_recovery_cannot_query_or_submit_a_second_withdrawal`.
- [x] Restart before, during, and after an Arbitrum wallet child uses the same
  operation ID and transaction journal; mined success is reused and unknown
  nonce state cannot submit another transaction. Evidence:
  `capital_handle_uses_the_same_nonce_journal_and_cannot_keep_service_alive`
  plus the existing transaction-journal recovery suite.
- [x] Route, asset, wallet, chain, amount, fee authority, and journal scope are
  immutable across recovery. Evidence: the M10 request fingerprint,
  `validate_operation`, and `m10_cumulative_risk_is_derived_from_the_durable_saga_after_restart`.
- [x] No terminal or unknown M10 child can release inventory without fresh
  Binance and Arbitrum snapshots satisfying settlement. Evidence:
  `RebalanceSettlementBarrier` and shared inventory pending-settlement state.

## Versioned artifact semantic diff

- [x] The operator's separate M10 approval is recorded in the checked-in V4
  artifact as actor `operator` at `2026-07-30T23:26:16Z`; live authority is
  absent from a fully disabled projection and every partial enable or revoke
  fails validation. Evidence: checked-in V4 artifact and
  `checked_in_m10_approval_compiles_live_authority_and_public_projection_scrubs_it`.
- [x] The source artifact and compiled production bundle encode identical M10
  route, count, concurrency, failure, duration, value, fee, bridge, and unknown
  reconciliation limits. Evidence:
  `checked_in_bundle_is_exact_compiler_output`.
- [x] Enabling M10 requires a separate audited actor/timestamp plus both the
  pair and canary rebalance flags; changing flags alone fails validation.
  Evidence:
  `domain::config::tests::committed_esp_canary_has_versioned_m10_approval_and_bounds`.
- [x] The exact compiled/public projections are regenerated and their semantic
  diff is reviewed, with secrets and approval data scrubbed where required.
  Evidence:
  `checked_in_m10_approval_compiles_live_authority_and_public_projection_scrubs_it`.
- [x] The completed M9 prefunder is removed from the approved M10 Deployment.
  Its command intentionally requires steady-state rebalance to be disabled and
  would otherwise reject every approved M10 startup. The shared PVC and
  durable saga/nonce journals remain mounted. Evidence:
  `gke_m10_removes_the_completed_m9_prefunder_and_keeps_durable_state`.
- [x] The production-derived startup readiness guard accepts only a fully
  disabled rebalance projection or the exact approved direct-Arbitrum M10
  projection. Missing either enable flag or enabling bridge mutation fails
  closed. Evidence:
  `approved_m10_rebalance_is_a_valid_readiness_projection_and_partial_gates_fail`.
- [x] Durable rebalance checksums are verified against the byte-exact stored
  payload before schema-default deserialization. The production M9
  `binance_withdrawal_submission_started` record without
  `reconciliation_queries` replays as zero while a payload mutation still
  fails closed. Evidence:
  `legacy_defaulted_progress_field_validates_the_stored_payload_bytes`.
- [x] The workflow inspects the live init-container list and removes it with a
  guarded JSON Patch only when its sole member is the completed
  `prefund-arbitrum-m9`. Kubernetes server-side dry-run proved that both an
  explicit empty list and `null` preserve this list under the prior field
  owner, so the release asserts the field is absent before applying M10.
  Durable state schema version 2 is recorded on the Deployment; a failed
  forward-schema rollout is never automatically reverted to an older reader
  or its runtime projection. Evidence:
  `gke_m10_removes_the_completed_m9_prefunder_and_keeps_durable_state`.

## Latency and resource observation plan

- [x] Report allocator queue/calculation, EVM capital queue/provider/receipt,
  Binance capital child, and settlement durations without adding telemetry
  writes to the trading hot path. Evidence:
  `scripts/sql/m10_rebalance_canary.sql`, validated read-only against the live
  ClickHouse schema with an empty future window.
- [x] Compare WLD and ESP socket-to-decision, parse, DEX receive/build/total,
  errors, drops, CPU, throttling, memory, OOM, and restarts over equal
  half-open production windows. Evidence: the M10 report plus the existing M9,
  WLD performance, error, and GKE resource reports.
- [x] Stop conditions and rollback disable only new Arbitrum rebalance
  creation while preserving active-saga recovery and funded ESP trading.
  Evidence: `remaining_m10_rebalance_authority`,
  `stop_pending_rebalance_creation`, and startup `recover_active`.

## Final diff review

- [x] Review every changed line from `origin/main`, not only the latest fix.
  Evidence: consolidated diff review covered source, compiled artifacts,
  deployment configuration, tests, reporting, and documentation after the
  final endpoint/authority/telemetry corrections.
- [x] Run the outbound endpoint/route scan and confirm no runtime-selectable
  withdrawal mode or local-entity withdrawal submission exists.
  Evidence: `rg` scan plus `tests/binance_capital_contract.rs`.
- [x] Run all targeted restart/limit/owner tests embedded in
  `scripts/predeploy-review`, including the production-derived WLD reservation
  contention regression and the true shortfall/invariant controls.
- [x] Audit current production `ERROR` rows before release. The observed WLD
  rejection had sufficient raw inventory but insufficient *available*
  inventory because active operations held the balance. The typed reserve
  failure now classifies that case as rate-limited `INFO` without weakening
  real capital-shortfall or reservation-invariant errors.
- [x] Treat the first `b2d77da` rollout as diagnostic, not authoritative:
  Pod `arb-bot-7d9c9db47f-7rlvp` restarted once and no M10 external mutation
  occurred. The consolidated corrective diff fixes every observed common
  cause before another deploy: reservation IDs include pair identity, the
  shared rebalance executor has one explicit busy lane with primary priority,
  expected inventory contention keeps pending work instead of terminating the
  process, process start removes a stale readiness marker before its first
  await, and a rejected canary result no longer immediately re-evaluates an
  unchanged DEX generation.
  Evidence: `rebalance_reservations_are_unique_across_pair_engines`,
  `process_start_removes_a_stale_runtime_readiness_marker`, plus the trade
  result dispatch and inventory source diff.
- [x] Run `scripts/quality.sh` once after the final corrective edit. The first
  security audit discovered `RUSTSEC-2026-0220` in transitive `ruint 1.19.0`;
  the lockfile now selects fixed `ruint 1.20.0`, and the complete gate passes
  with 436 library tests, 6 main tests, all deployment/reporting/monitoring
  contracts, and only the three existing allowed unmaintained-crate warnings.
- [x] Confirm a clean fast-forward to current `origin/main`; after the full
  corrective gate, the next external action is one consolidated push/deploy.
  Evidence: fetched `origin/main` at `b2d77da9ccf`; it exactly equals the
  current branch HEAD before this corrective commit.

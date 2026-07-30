# M10 pre-deploy review

This artifact remains incomplete until every gate below is supported by a test
or an exact diff reference. `scripts/predeploy-review
docs/reviews/m10-predeploy.md` runs the automated contracts before reporting
unchecked manual gates, and `scripts/quality.sh` is required afterwards. No M10
revision may be pushed to `main` merely to discover a locally reviewable
contract error.

## External mutation matrix

- [x] Binance-to-wallet USDC and ESP use only
  `POST /sapi/v1/capital/withdraw/apply`; asset, network, and amount cannot
  select a different withdrawal endpoint. Evidence:
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
  `restart_preserves_unknown_standard_withdrawal_without_resubmission_authority`.
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

- [x] M10 remains `explicit_production_approval_required` and externally
  disabled until the separate release approval is recorded; the approved
  projection is exercised locally without granting authority to the checked-in
  artifact. Evidence: checked-in V4 artifact and
  `explicit_m10_approval_is_required_to_compile_live_allocator_authority`.
- [x] The source artifact and compiled production bundle encode identical M10
  route, count, concurrency, failure, duration, value, fee, bridge, and unknown
  reconciliation limits. Evidence:
  `checked_in_bundle_is_exact_compiler_output`.
- [x] Enabling M10 requires a separate audited actor/timestamp plus both the
  pair and canary rebalance flags; changing flags alone fails validation.
  Evidence: `domain::config::tests::committed_esp_canary_requires_versioned_bidirectional_prefunding`.
- [x] The exact compiled/public projections are regenerated and their semantic
  diff is reviewed, with secrets and approval data scrubbed where required.
  Evidence:
  `explicit_m10_approval_is_required_to_compile_live_allocator_authority`.

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
  `scripts/predeploy-review`; the final artifact-complete invocation remains
  below.
- [x] Run `scripts/quality.sh` once after the final edit.
- [x] Confirm a clean fast-forward to current `origin/main`; after explicit
  approval, the next external action is one consolidated push/deploy of the
  reviewed M10 revision. Evidence: fetched `origin/main` at `94b61e81f5ff`;
  it is an ancestor of local `2f7d68c9eca3` plus the reviewed working-tree
  implementation.

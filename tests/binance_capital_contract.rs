const CAPITAL: &str = include_str!("../src/binance/capital.rs");
const REBALANCE_EXECUTOR: &str = include_str!("../src/rebalance/executor.rs");
const REBALANCE_RUNTIME: &str = include_str!("../src/rebalance/runtime.rs");
const APP_CONFIG: &str = include_str!("../src/config.rs");
const GKE_DEPLOYMENT: &str = include_str!("../infra/gcp/gke/deployment.yaml");
const GCE_STARTUP: &str = include_str!("../infra/gcp/gce-startup.sh");
const ESP_DOMAIN: &str = include_str!("../config/strategies/usdc-esp-arbitrum.v7.json");

#[test]
fn withdrawal_starts_standard_and_only_exact_4104_selects_travel_rule() {
    assert!(CAPITAL.contains(
        "const STANDARD_WITHDRAWAL_ENDPOINT: &str = \"/sapi/v1/capital/withdraw/apply\";"
    ));
    assert!(CAPITAL.contains(
        "const TRAVEL_RULE_WITHDRAWAL_ENDPOINT: &str = \"/sapi/v1/localentity/withdraw/apply\";"
    ));
    assert!(CAPITAL.contains(".signed_post(\n                STANDARD_WITHDRAWAL_ENDPOINT,"));
    assert!(CAPITAL.contains(".signed_post(\n                TRAVEL_RULE_WITHDRAWAL_ENDPOINT,"));
    assert!(CAPITAL.contains("\"isAddressOwner\": 1"));
    assert!(CAPITAL.contains("\"sendTo\": 1"));
    assert!(CAPITAL.contains("\"vaspName\": \"Unhosted Wallet\""));
    assert!(CAPITAL.contains("\"satoshiToken\": ownership_proof.satoshi_token.as_str()"));
    assert!(CAPITAL.contains("\"verifyMethod\": ownership_proof.verify_method"));
    assert!(CAPITAL.contains("\"vaspName\": \"Unhosted Wallet\""));
    assert!(REBALANCE_RUNTIME.contains(".withdraw_standard("));
    assert!(REBALANCE_RUNTIME.contains(".withdraw_travel_rule_ae_self_owned("));
    assert!(REBALANCE_RUNTIME.contains("is_travel_rule_required_rejection(&error)"));
    assert!(REBALANCE_RUNTIME.contains(
        "TRAVEL_RULE_REQUIRED_API_MODE: &str = \"travel_rule_required_after_standard_-4104\""
    ));
}

#[test]
fn withdrawal_endpoint_cannot_be_selected_by_asset_network_or_local_config() {
    for source in [APP_CONFIG, GKE_DEPLOYMENT, GCE_STARTUP] {
        assert!(!source.contains("REBALANCE_BINANCE_WITHDRAWAL_API_MODE"));
        assert!(!source.contains("rebalance_binance_withdrawal_api_mode"));
    }

    let domain: serde_json::Value = serde_json::from_str(ESP_DOMAIN).unwrap();
    assert!(
        domain["pairs"][0]["full_live_policy"]
            .get("withdrawal_api_mode")
            .is_none()
    );
}

#[test]
fn deposit_questionnaire_remains_conditional_and_separate_from_withdrawal() {
    assert!(CAPITAL.contains(
        "const DEPOSIT_TRAVEL_RULE_ENDPOINT: &str = \"/sapi/v2/localentity/deposit/provide-info\";"
    ));
    assert!(CAPITAL.contains("pub fn questionnaire_required(&self) -> bool"));
    assert!(
        CAPITAL.contains("self.require_questionnaire && self.travel_rule_req_status != Some(0)")
    );

    assert!(CAPITAL.contains("/sapi/v1/capital/withdraw/apply"));
    assert!(CAPITAL.contains("/sapi/v1/localentity/withdraw/apply"));
    assert!(CAPITAL.contains("/sapi/v2/localentity/withdraw/history"));
    assert!(!REBALANCE_RUNTIME.contains(".signed_post("));
}

#[test]
fn deposit_questionnaire_matches_rails_order_and_is_durable_before_submission() {
    let start = REBALANCE_RUNTIME
        .find("async fn wait_binance_deposit(")
        .unwrap();
    let end = REBALANCE_RUNTIME[start..]
        .find("async fn wait_token_credit(")
        .map(|offset| start + offset)
        .unwrap();
    let implementation = &REBALANCE_RUNTIME[start..end];

    let requirement = implementation
        .find("record.questionnaire_required()")
        .unwrap();
    let durable_state = implementation
        .find("RebalanceExecutionProgress::DepositQuestionnaireSubmissionStarted")
        .unwrap();
    let submission = implementation
        .find(".submit_deposit_questionnaire(")
        .unwrap();
    let credited = implementation.find("if record.is_credited()").unwrap();
    assert!(requirement < durable_state);
    assert!(durable_state < submission);
    assert!(submission < credited);
    assert!(implementation.contains("deposit_id == &record.deposit_id"));
}

#[test]
fn withdrawal_unknown_outcome_requires_composite_absence_proof_before_retry() {
    let direct_start = REBALANCE_RUNTIME
        .find("async fn direct_binance_to_wallet(")
        .unwrap();
    let direct_end = REBALANCE_RUNTIME[direct_start..]
        .find("async fn direct_wallet_to_binance(")
        .map(|offset| direct_start + offset)
        .unwrap();
    let direct = &REBALANCE_RUNTIME[direct_start..direct_end];
    let completed_transfer = direct
        .find("RebalanceExecutionProgress::BinanceTransferCompleted")
        .unwrap();
    let last_route_check = direct[completed_transfer..].find(".verify_route(").unwrap();
    let withdrawal = direct[completed_transfer..]
        .find(".begin_binance_withdrawal(")
        .unwrap();
    assert!(last_route_check < withdrawal);

    let begin_start = REBALANCE_RUNTIME
        .find("async fn begin_binance_withdrawal(")
        .unwrap();
    let begin_end = REBALANCE_RUNTIME[begin_start..]
        .find("async fn wait_master_transfer(")
        .map(|offset| begin_start + offset)
        .unwrap();
    let begin = &REBALANCE_RUNTIME[begin_start..begin_end];
    assert!(begin.contains("reconciliation_queries: 1"));
    assert!(begin.contains("confirm_unknown_withdrawal_absence"));
    assert!(begin.contains("BinanceWithdrawalRetryAuthorized"));
    assert!(begin.contains("current_required_withdrawal"));
    assert!(begin.contains("cancel_stale_withdrawal_retry"));
    assert!(begin.contains("BinanceMasterReturnSubmissionStarted"));
    assert!(begin.contains("universal_transfer_to_subaccount"));
    assert!(begin.contains("CancelledStale"));
    assert!(begin.contains("standard_history.is_empty()"));
    assert!(begin.contains("travel_rule_withdrawal_history_v2_for_network"));
    assert!(begin.contains("master_free_base_units >= operation.intent.amount"));
    assert!(begin.contains("master_locked_base_units.is_zero()"));
    assert!(begin.contains("same_withdrawal_retry_authority"));
    assert!(
        REBALANCE_RUNTIME
            .contains("std::cmp::min(evidence.master_free_base_units, staged_master_amount)")
    );
    assert!(!begin.contains("wallet_balance == bridge_balance_before"));
    assert!(begin.contains("UNKNOWN_WITHDRAWAL_ABSENCE_CONFIRMATION_DELAY"));
    assert!(begin.contains("is_terminal_binance_withdrawal_rejection(&error)"));
    assert!(begin.contains("is_travel_rule_required_rejection(&error)"));
    assert!(begin.contains("is_retryable_travel_rule_ownership_rejection(&error)"));
    assert!(begin.contains("submit_required_travel_rule_withdrawal"));
    assert!(begin.contains("RebalanceExecutionProgress::Failed { reason }"));
    assert!(
        REBALANCE_RUNTIME.contains("BinanceApiError::is_known_pre_submission_withdrawal_rejection")
    );
    assert!(!REBALANCE_RUNTIME.contains("BinanceWithdrawalRejected"));
}

#[test]
fn unindexed_master_transfer_reuses_its_id_only_after_two_phase_absence_proof() {
    let start = REBALANCE_RUNTIME
        .find("async fn begin_master_transfer(")
        .unwrap();
    let end = REBALANCE_RUNTIME[start..]
        .find("async fn finish_master_transfer(")
        .map(|offset| start + offset)
        .unwrap();
    let implementation = &REBALANCE_RUNTIME[start..end];

    let proof = implementation
        .find("confirm_unindexed_master_transfer_absent")
        .unwrap();
    let retry = implementation
        .find("universal_transfer_from_subaccount")
        .unwrap();
    assert!(proof < retry);
    assert!(implementation.contains("UNKNOWN_WITHDRAWAL_ABSENCE_CONFIRMATION_DELAY"));
    assert!(implementation.contains("history.is_empty()"));
    assert!(implementation.contains("validate_unindexed_master_transfer_absence"));
    assert!(REBALANCE_RUNTIME.contains("first.0 == second.0"));
    assert!(REBALANCE_RUNTIME.contains("first.1.is_zero()"));
    assert!(REBALANCE_RUNTIME.contains("second.2 >= target"));
    assert!(implementation.contains("client_transaction_id"));
    assert!(implementation.contains("validate_master_transfer_record"));
}

#[test]
fn exact_travel_rule_ownership_rejection_gets_three_proven_retries() {
    let start = REBALANCE_RUNTIME
        .find("async fn submit_required_travel_rule_withdrawal(")
        .unwrap();
    let end = REBALANCE_RUNTIME[start..]
        .find("async fn ensure_travel_rule_ae_self_owned(")
        .map(|offset| start + offset)
        .unwrap();
    let submission = &REBALANCE_RUNTIME[start..end];

    assert!(submission.contains("is_retryable_travel_rule_ownership_rejection(&error)"));
    assert!(submission.contains("reconcile_unknown_withdrawal_and_retry(operation, network)"));
    assert!(submission.contains("retry_limit = MAX_TRAVEL_RULE_OWNERSHIP_REJECTION_RETRIES"));
    assert!(
        submission
            .find("is_retryable_travel_rule_ownership_rejection")
            .unwrap()
            < submission
                .find("is_terminal_binance_withdrawal_rejection")
                .unwrap()
    );
    assert!(
        REBALANCE_EXECUTOR
            .contains("pub const MAX_TRAVEL_RULE_OWNERSHIP_REJECTION_RETRIES: u8 = 3;")
    );
    assert!(REBALANCE_RUNTIME.contains("process_with_travel_rule_ownership_retries"));
    assert!(REBALANCE_RUNTIME.contains("reopen_retryable_travel_rule_ownership_failure"));
}

#[test]
fn verified_address_metadata_is_forwarded_into_the_travel_rule_questionnaire() {
    let start = REBALANCE_RUNTIME
        .find("async fn ensure_travel_rule_ae_self_owned(")
        .unwrap();
    let end = REBALANCE_RUNTIME[start..]
        .find("async fn wait_master_transfer(")
        .map(|offset| start + offset)
        .unwrap();
    let proof = &REBALANCE_RUNTIME[start..end];

    assert!(proof.contains("questionnaire_country_code.as_deref() == Some(\"AE\")"));
    assert!(proof.contains("verified_self_owned_evm_address_record(record, &wallet)"));
    assert!(REBALANCE_RUNTIME.contains("record.status == \"VERIFIED\""));
    assert!(REBALANCE_RUNTIME.contains("record.address_questionnaire.is_address_owner == Some(1)"));
    assert!(REBALANCE_RUNTIME.contains("record.address_questionnaire.verify_method == Some(1)"));
    assert!(!proof.contains("record.token == operation.intent.token_symbol"));
    assert!(proof.contains("matching.first().context("));
    assert!(proof.contains("equivalent_verified_record_count = matching.len()"));
    assert!(proof.contains("satoshi_token: operation.intent.token_symbol.clone()"));
    assert!(CAPITAL.contains("\"satoshiToken\": ownership_proof.satoshi_token.as_str()"));
    assert!(CAPITAL.contains("\"verifyMethod\": ownership_proof.verify_method"));
}

#[test]
fn travel_rule_restart_ignores_failed_standard_routing_rows_but_never_replays() {
    let start = REBALANCE_RUNTIME
        .find("async fn begin_binance_withdrawal(")
        .unwrap();
    let end = REBALANCE_RUNTIME[start..]
        .find("async fn submit_required_travel_rule_withdrawal(")
        .map(|offset| start + offset)
        .unwrap();
    let recovery = &REBALANCE_RUNTIME[start..end];

    assert!(recovery.contains("!record.is_failed_without_broadcast()"));
    assert!(recovery.contains("!record.is_approved_without_withdrawal()"));
    assert!(recovery.contains("viable.len() <= 1"));
    assert!(recovery.contains("reconciliation_queries: 1"));
    assert!(!recovery.contains(".withdraw_travel_rule_ae_self_owned("));
}

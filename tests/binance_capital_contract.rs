const CAPITAL: &str = include_str!("../src/binance/capital.rs");
const REBALANCE_RUNTIME: &str = include_str!("../src/rebalance/runtime.rs");
const APP_CONFIG: &str = include_str!("../src/config.rs");
const GKE_DEPLOYMENT: &str = include_str!("../infra/gcp/gke/deployment.yaml");
const GCE_STARTUP: &str = include_str!("../infra/gcp/gce-startup.sh");
const ESP_DOMAIN: &str = include_str!("../config/strategies/usdc-esp-arbitrum.v4.json");

#[test]
fn every_new_withdrawal_is_compile_time_pinned_to_the_standard_capital_api() {
    assert!(CAPITAL.contains(
        "const STANDARD_WITHDRAWAL_ENDPOINT: &str = \"/sapi/v1/capital/withdraw/apply\";"
    ));
    assert!(CAPITAL.contains(".signed_post(\n                STANDARD_WITHDRAWAL_ENDPOINT,"));
    assert!(!CAPITAL.contains("/sapi/v1/localentity/withdraw/apply"));
    assert!(REBALANCE_RUNTIME.contains(".withdraw_standard("));
    assert!(!REBALANCE_RUNTIME.contains("withdraw_travel_rule("));
    assert!(!REBALANCE_RUNTIME.contains("api_mode == \"travel_rule\""));
}

#[test]
fn withdrawal_endpoint_cannot_be_selected_by_asset_network_or_amount_at_runtime() {
    for source in [APP_CONFIG, GKE_DEPLOYMENT, GCE_STARTUP] {
        assert!(!source.contains("REBALANCE_BINANCE_WITHDRAWAL_API_MODE"));
        assert!(!source.contains("rebalance_binance_withdrawal_api_mode"));
    }

    let domain: serde_json::Value = serde_json::from_str(ESP_DOMAIN).unwrap();
    let prefunding = &domain["pairs"][0]["live_canary"]["prefunding_rebalance"];
    assert_eq!(prefunding["withdrawal_api_mode"], "standard");
}

#[test]
fn travel_rule_submission_is_deposit_only_and_legacy_withdrawal_reads_are_recovery_only() {
    assert!(CAPITAL.contains(
        "const DEPOSIT_TRAVEL_RULE_ENDPOINT: &str = \"/sapi/v2/localentity/deposit/provide-info\";"
    ));
    assert!(CAPITAL.contains("pub fn questionnaire_required(&self) -> bool"));
    assert!(
        CAPITAL.contains("self.require_questionnaire && self.travel_rule_req_status != Some(0)")
    );

    assert!(!CAPITAL.contains("/sapi/v1/localentity/withdraw/apply"));
    assert!(CAPITAL.contains("/sapi/v2/localentity/withdraw/history"));
    assert!(!REBALANCE_RUNTIME.contains(".signed_post("));
}

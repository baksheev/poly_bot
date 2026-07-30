use std::fs;

#[test]
fn m8_report_proves_readiness_without_authorizing_esp_mutations() {
    let report =
        fs::read_to_string("scripts/report-m8-live-readiness").expect("M8 report must be readable");
    let query = fs::read_to_string("scripts/sql/m8_live_readiness.sql")
        .expect("M8 readiness query must be readable");

    assert!(report.contains("M8 Arbitrum live readiness configured"));
    assert!(report.contains("report-m7-combined-shadow"));
    assert!(query.contains("m8_live_readiness"));
    assert!(query.contains("binance_request_count = 4"));
    assert!(query.contains("readiness_stage_count = 3"));
    assert!(query.contains("readiness_latest"));
    assert!(query.contains("argMax(ready, observed_at_ms)"));
    assert!(query.contains("mutation_by_engine"));
    assert!(query.contains("direct_rebalance_routes = 2"));
    assert!(query.contains("arbitrum_one_fail_closed"));
    assert!(query.contains("mutation_capability_records = 0"));
    assert!(query.contains("NOT arbitrum_execution_enabled"));
    assert!(query.contains("'arb-bot-rust-shadow-gke-'"));
    assert!(query.contains("'ready'"));
}

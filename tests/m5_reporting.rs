use std::{fs, path::PathBuf};

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn m5_report_proves_exact_portfolio_ownership_and_conservation() {
    let root = repository_root();
    let report = fs::read_to_string(root.join("scripts/report-m5-portfolio-runtime"))
        .expect("M5 report script must be readable");
    let query = fs::read_to_string(root.join("scripts/sql/m5_portfolio_runtime.sql"))
        .expect("M5 portfolio query must be readable");

    assert!(report.contains("scripts/gcloud-local"));
    assert!(report.contains("portfolio_inventory_key"));
    assert!(report.contains("portfolio_location_count"));
    assert!(report.contains("portfolio_allocator_mode"));
    assert!(report.contains("portfolio_external_mutation_authorized"));
    assert!(report.contains("live_rebalance_adapter"));
    assert!(report.contains("arbitrum_execution_enabled"));
    assert!(report.contains("scripts/report-m4-hot-path-runtime"));
    assert!(!report.contains("kubectl logs"));

    for kind in [
        "portfolio_capital_allocator_evaluated",
        "capital_allocation_evaluated",
        "arbitrage_admitted",
    ] {
        assert!(query.contains(kind), "M5 query must include {kind}");
    }
    assert!(query.contains("conservation_checked"));
    assert!(query.contains("external_mutation_authorized"));
    assert!(query.contains("scheduler_queue_us"));
    assert!(query.contains("portfolio_snapshot_us"));
    assert!(query.contains("reservation_snapshot_us"));
    assert!(query.contains("quantileExact(0.99)"));
}

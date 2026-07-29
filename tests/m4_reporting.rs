use std::{fs, path::PathBuf};

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn m4_report_proves_direct_multi_strategy_owner_and_bounded_workers() {
    let root = repository_root();
    let report = fs::read_to_string(root.join("scripts/report-m4-hot-path-runtime"))
        .expect("M4 report script must be readable");
    let query = fs::read_to_string(root.join("scripts/sql/m4_hot_path_runtime.sql"))
        .expect("M4 hot-path query must be readable");

    assert!(report.contains("scripts/gcloud-local"));
    assert!(report.contains("hot_path_strategy_count"));
    assert!(report.contains("hot_path_direct_binance_poll"));
    assert!(report.contains("hot_path_dependency_index"));
    assert!(report.contains("hot_path_sizing_policy"));
    assert!(report.contains("hot_path_shadow_external_mutation_authorized"));
    assert!(report.contains("scripts/report-m0-performance"));
    assert!(!report.contains("kubectl logs"));

    for kind in [
        "arbitrage_evaluation",
        "strategy_sizing_task",
        "strategy_calculation_overload",
        "coordinator_shadow_candidate",
    ] {
        assert!(query.contains(kind), "M4 query must include {kind}");
    }
    assert!(query.contains("calculation_budget_exceeded"));
    assert!(query.contains("replaced_pending_snapshot"));
    assert!(query.contains("superseded"));
    assert!(query.contains("proven_non_mutating_candidates"));
    assert!(query.contains("quantileExact(0.99)"));
}

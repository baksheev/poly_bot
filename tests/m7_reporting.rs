use std::fs;

#[test]
fn m7_report_proves_background_compatibility_shadow_planning_and_scoped_faults() {
    let report = fs::read_to_string("scripts/report-m7-combined-shadow")
        .expect("M7 report must be readable");
    let query = fs::read_to_string("scripts/sql/m7_combined_shadow.sql")
        .expect("M7 combined shadow query must be readable");

    assert!(report.contains("M7 combined production shadow configured"));
    assert!(report.contains("runtime dependency fault"));
    assert!(report.contains("report-m6-execution-ownership"));
    for kind in [
        "strategy_decision_compatibility",
        "coordinator_shadow_candidate",
        "shadow_reservation_plan",
        "shadow_rebalance_plan",
    ] {
        assert!(query.contains(kind), "M7 query must include {kind}");
    }
    assert!(query.contains("wld_comparison_mismatches"));
    assert!(query.contains("shadow_mutation_capability_records"));
    assert!(query.contains("'ready'"));
}

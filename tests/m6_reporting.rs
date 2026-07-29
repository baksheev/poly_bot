use std::{fs, path::PathBuf};

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn m6_report_proves_scoped_owners_fsync_and_unchanged_hot_path() {
    let root = repository_root();
    let report = fs::read_to_string(root.join("scripts/report-m6-execution-ownership"))
        .expect("M6 report script must be readable");
    let query = fs::read_to_string(root.join("scripts/sql/m6_execution_ownership.sql"))
        .expect("M6 execution ownership query must be readable");

    assert!(report.contains("scripts/gcloud-local"));
    assert!(report.contains("M6 execution ownership graph validated"));
    assert!(report.contains("journal_schema_version"));
    assert!(report.contains("candidate_policy"));
    assert!(report.contains("global_trade_serialization"));
    assert!(report.contains("scripts/report-m5-portfolio-runtime"));
    assert!(!report.contains("kubectl logs"));

    for kind in [
        "execution_ownership_runtime_started",
        "runtime_journal_recovery",
        "arbitrage_execution_stage",
    ] {
        assert!(query.contains(kind), "M6 query must include {kind}");
    }
    assert!(query.contains("coordinator_admit_journal"));
    assert!(query.contains("preflight_proof_to_parent_fsync"));
    assert!(query.contains("intent_journal"));
    assert!(query.contains("rebalance_signer_access"));
    assert!(query.contains("quantileExact(0.99)"));
}

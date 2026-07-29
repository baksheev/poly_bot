use std::{fs, path::PathBuf};

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn m2_report_proves_shared_account_and_direct_stream_boundaries() {
    let report = fs::read_to_string(repository_root().join("scripts/report-m2-binance-runtime"))
        .expect("M2 report script must be readable");

    assert!(report.contains("scripts/gcloud-local"));
    assert!(report.contains("binance_account_snapshot_generation"));
    assert!(report.contains("binance_hydrated_symbols"));
    assert!(report.contains("binance_stream_shards"));
    assert!(report.contains("binance_executable_symbols"));
    assert!(report.contains("m2_binance_shared_stream"));
    assert!(report.contains("scripts/report-m0-performance"));
    assert!(!report.contains("kubectl logs"));

    let query =
        fs::read_to_string(repository_root().join("scripts/sql/m2_binance_shared_stream.sql"))
            .expect("M2 shared-stream query must be readable");
    assert!(query.contains("binance_shared_stream_event"));
    assert!(query.contains("direct_owner_poll"));
    assert!(query.contains("quantileExact(0.99)"));
    assert!(query.contains("parse_p99_us"));
    assert!(query.contains("GROUP BY"));
    assert!(query.contains("generation"));
}

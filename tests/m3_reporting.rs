use std::{fs, path::PathBuf};

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn m3_report_proves_network_registry_batches_and_wld_regression_gates() {
    let root = repository_root();
    let report = fs::read_to_string(root.join("scripts/report-m3-network-runtime"))
        .expect("M3 report script must be readable");
    let query = fs::read_to_string(root.join("scripts/sql/m3_network_runtime.sql"))
        .expect("M3 network-runtime query must be readable");

    assert!(report.contains("scripts/gcloud-local"));
    assert!(report.contains("network_runtime_count"));
    assert!(report.contains("network_runtime_ids"));
    assert!(report.contains("arbitrum_execution_enabled"));
    assert!(report.contains("m3_network_runtime"));
    assert!(report.contains("scripts/report-m0-performance"));
    assert!(!report.contains("kubectl logs"));

    assert!(query.contains("network_read_batch"));
    assert!(query.contains("'engine_id'"));
    assert!(query.contains("'network_id'"));
    assert!(query.contains("'read_class'"));
    assert!(query.contains("'supports_eip1898_block_hash'"));
    assert!(query.contains("'complete'"));
    for metric in [
        "queue_p99_us",
        "provider_p99_us",
        "decode_p99_us",
        "publication_p99_us",
    ] {
        assert!(query.contains(metric), "M3 query must report {metric}");
    }
    assert!(query.contains("quantileExact(0.95)"));
    assert!(query.contains("quantileExact(0.99)"));
    assert!(query.contains("GROUP BY"));
}

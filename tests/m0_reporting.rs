use std::{fs, path::PathBuf};

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn m0_report_includes_pool_scoped_dex_latency_table() {
    let root = repository_root();
    let report = fs::read_to_string(root.join("scripts/report-m0-performance"))
        .expect("M0 report script must be readable");
    let query = fs::read_to_string(root.join("scripts/sql/m0_dex_pool_hot_path.sql"))
        .expect("pool-scoped M0 query must be readable");

    assert!(
        report.contains("m0_dex_pool_hot_path"),
        "the authoritative M0 report must execute the pool-scoped query"
    );
    for identity in [
        "'pair_id'",
        "'strategy_id'",
        "'network_id'",
        "'pool_id'",
        "'identity'",
    ] {
        assert!(
            query.contains(identity),
            "pool-scoped query must extract {identity}"
        );
    }
    for stage in [
        "'dex_event_receive_to_owner'",
        "'prepared_curve_build'",
        "'prepared_curve_total'",
    ] {
        assert!(
            query.contains(stage),
            "pool-scoped query must report {stage}"
        );
    }
    assert!(query.contains("stage_timing_complete_records"));
    assert!(query.contains("max_exact_output_segments"));
    assert!(query.contains("max_exact_input_segments"));
    assert!(query.contains("max_token_a_exact_input_segments"));
    assert!(query.contains("quantileExact(0.95)"));
    assert!(query.contains("quantileExact(0.99)"));
}

#[test]
fn m0_admission_report_is_strategy_and_account_scoped() {
    let query =
        fs::read_to_string(repository_root().join("scripts/sql/m0_admission_execution.sql"))
            .expect("M0 admission query must be readable");

    for identity in [
        "'pair_id'",
        "'strategy_id'",
        "'binance_account_id'",
        "'instrument_id'",
        "'network_id'",
    ] {
        assert!(
            query.contains(identity),
            "admission query must extract {identity}"
        );
    }
    assert!(query.contains("GROUP BY"));
    assert!(query.contains("binance_account_id,"));
    assert!(query.contains("instrument_id,"));
    assert!(query.contains("network_id,"));
}

#[test]
fn admitted_telemetry_carries_the_pair_scope_used_by_the_m0_report() {
    let engine = fs::read_to_string(repository_root().join("src/engine.rs"))
        .expect("engine source must be readable");
    let admitted_payload = engine
        .split_once("let admitted_payload = json!({")
        .expect("admitted telemetry payload must exist")
        .1
        .split_once("self.telemetry.emit(\"arbitrage_admitted\"")
        .expect("admitted telemetry emission must exist")
        .0;

    assert!(
        admitted_payload.contains("\"pair_id\": pair_id,"),
        "arbitrage_admitted must carry pair_id for the per-pair M0 report"
    );
}

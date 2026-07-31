use std::fs;

const MAIN: &str = include_str!("../src/main.rs");

#[test]
fn m13_report_proves_per_operation_full_live_authority() {
    let report =
        fs::read_to_string("scripts/report-m13-full-live").expect("M13 report must be readable");
    let query =
        fs::read_to_string("scripts/sql/m13_full_live.sql").expect("M13 query must be readable");

    assert!(report.contains("ESP Arbitrum full-live execution configured"));
    assert!(report.contains("max_uint256_then_locked"));
    assert!(report.contains("maximum_unknown_reconciliation_queries"));
    assert!(report.contains("shared_arbitrum_evm_owner"));
    assert!(query.contains("esp-usdc-arbitrum-full-live"));
    assert!(query.contains("allocator_mode') = 'full_live'"));
    assert!(query.contains("active_transfer_count > 1"));
    assert!(query.contains("per_operation_limit_breaches"));
    assert!(query.contains("toUInt256('2600000000')"));
    assert!(query.contains("toUInt256('10000000000000000000000')"));
    assert!(query.contains("toUInt256('5000000')"));
    assert!(query.contains("toUInt256('2000000000000000000')"));
    assert!(query.contains("'limit_or_execution_breach'"));
    assert!(query.contains("'armed'"));
    assert!(query.contains("'active'"));
    assert!(query.contains("'full_live_observed'"));
}

#[test]
fn startup_projection_reports_full_live_and_shared_rebalance_ownership() {
    assert!(MAIN.contains("CompiledCapitalAllocatorMode::FullLive"));
    assert!(MAIN.contains("max_uint256_then_locked"));
    assert!(MAIN.contains("shared_arbitrum_rebalance_owner_attached"));
    assert!(MAIN.contains("RebalanceExecutionAuthority::ArbitrumFullLive"));
}

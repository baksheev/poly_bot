use std::fs;

const MAIN: &str = include_str!("../src/main.rs");

#[test]
fn m10_report_proves_bounded_shared_owner_rebalance_authority() {
    let report = fs::read_to_string("scripts/report-m10-rebalance-canary")
        .expect("M10 report must be readable");
    let query = fs::read_to_string("scripts/sql/m10_rebalance_canary.sql")
        .expect("M10 canary query must be readable");

    assert!(report.contains("M10 Arbitrum rebalance live canary configured"));
    assert!(report.contains("maximum_unknown_reconciliation_queries"));
    assert!(report.contains("shared_arbitrum_evm_owner"));
    assert!(report.contains("bridge_mutations_enabled"));
    assert!(query.contains("kind = 'm10_rebalance_risk_snapshot'"));
    assert!(query.contains("kind = 'portfolio_capital_allocator_planned'"));
    assert!(query.contains("kind = 'm10_rebalance_saga'"));
    assert!(query.contains("kind = 'm10_rebalance_child'"));
    assert!(query.contains("kind = 'arbitrage_execution_stage'"));
    assert!(query.contains("kind = 'rebalance_settlement_reconciled'"));
    assert!(query.contains("esp-usdc-arbitrum-rebalance-20260731-r2"));
    assert!(query.contains("allocator_queue_p99_us"));
    assert!(query.contains("allocator_calculation_p99_us"));
    assert!(query.contains("binance_capital_child_p99_us"));
    assert!(query.contains("evm_queue_p99_us"));
    assert!(query.contains("evm_provider_p99_us"));
    assert!(query.contains("evm_receipt_p99_us"));
    assert!(query.contains("settlement_p99_us"));
    assert!(query.contains("transfer_count > 2"));
    assert!(query.contains("active_transfer_count > 1"));
    assert!(query.contains("failed_transfer_count > 1"));
    assert!(query.contains("token_a_debit > toUInt256('2600000000')"));
    assert!(query.contains("token_b_debit > toUInt256('10000000000000000000000')"));
    assert!(query.contains("token_a_maximum_fee > toUInt256('5000000')"));
    assert!(query.contains("token_b_maximum_fee > toUInt256('2000000000000000000')"));
    assert!(query.contains("'limit_breach'"));
    assert!(query.contains("'armed'"));
    assert!(query.contains("'active'"));
    assert!(query.contains("'canary_observed'"));
}

#[test]
fn startup_projection_reports_the_compiled_m10_authority_instead_of_a_constant() {
    assert!(MAIN.contains("hot_path_canary_rebalance_mutation_authorized ="));
    assert!(MAIN.contains("CompiledCapitalAllocatorMode::LiveCanary"));
    assert!(MAIN.contains("CompiledCapitalAllocatorMode::FullLive"));
    assert!(!MAIN.contains("hot_path_canary_rebalance_mutation_authorized = false"));
    assert!(!MAIN.contains("hot_path_canary_rebalance_mutation_authorized = true"));
}

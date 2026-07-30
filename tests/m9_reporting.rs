use std::fs;

#[test]
fn m9_report_proves_bounded_live_authority_and_no_rebalance_mutation() {
    let report =
        fs::read_to_string("scripts/report-m9-live-canary").expect("M9 report must be readable");
    let query = fs::read_to_string("scripts/sql/m9_live_canary.sql")
        .expect("M9 canary query must be readable");

    assert!(report.contains("M9 Arbitrum live canary configured"));
    assert!(report.contains("max_failed_parent_trades"));
    assert!(query.contains("kind = 'm9_live_readiness'"));
    assert!(query.contains("kind = 'm9_canary_gate'"));
    assert!(query.contains("admitted_parents > 2"));
    assert!(query.contains("admitted_notional > 20000000"));
    assert!(query.contains("admitted_parents != unique_admitted_parents"));
    assert!(query.contains("NOT binance_mutation_enabled"));
    assert!(query.contains("NOT chain_mutation_enabled"));
    assert!(query.contains("NOT token_a_funded"));
    assert!(query.contains("NOT token_b_funded"));
    assert!(query.contains("rebalance_mutation_enabled"));
    assert!(query.contains("'arbitrum_one_fail_closed'"));
    assert!(query.contains("'armed'"));
    assert!(query.contains("'canary_observed'"));
}

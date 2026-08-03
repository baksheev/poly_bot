const MAIN: &str = include_str!("../src/main.rs");

#[test]
fn rebalance_dispatch_uses_claim_scoped_inventory_conflicts() {
    let start = MAIN
        .find("async fn dispatch_next_rebalance_execution(")
        .unwrap();
    let end = MAIN[start..]
        .find("fn mark_runtime_ready(")
        .map(|offset| start + offset)
        .unwrap();
    let dispatch = &MAIN[start..end];

    assert!(!dispatch.contains("active_inventory_operation_count"));
    assert!(!dispatch.contains("shared inventory operation must settle"));
    assert!(!dispatch.contains("active inventory operation must settle"));
    assert!(dispatch.contains("take_rebalance_execution"));
}

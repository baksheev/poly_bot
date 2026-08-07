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

#[test]
fn rebalance_dispatch_caps_runtime_amount_before_allocation_and_intent() {
    let start = MAIN.find("async fn dispatch_rebalance_execution(").unwrap();
    let end = MAIN[start..]
        .find("fn mark_runtime_ready(")
        .map(|offset| start + offset)
        .unwrap();
    let dispatch = &MAIN[start..end];

    let runtime_limit = dispatch.find("maximum_base_units_for").unwrap();
    let cap = dispatch
        .find("cap_pending_rebalance_amount(runtime_maximum)")
        .unwrap();
    let allocation = dispatch
        .find("authorize_pending_rebalance_allocation")
        .unwrap();
    let intent = dispatch.find("take_rebalance_execution").unwrap();

    assert!(runtime_limit < cap);
    assert!(cap < allocation);
    assert!(allocation < intent);
}

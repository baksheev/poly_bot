use serde_json::Value;

const REBALANCE_FAULT_METRIC: &str =
    include_str!("../infra/gcp/monitoring/rebalance-fault-log-metric.json");

#[test]
fn rebalance_fault_metric_matches_the_boolean_health_field() {
    let config: Value = serde_json::from_str(REBALANCE_FAULT_METRIC)
        .expect("rebalance fault log metric must be valid JSON");
    let filter = config["filter"]
        .as_str()
        .expect("rebalance fault log metric must have a string filter");

    assert!(filter.contains("rebalance executor failed closed"));
    assert!(filter.contains("rebalance planning failed closed"));
    assert!(filter.contains(
        r#"jsonPayload.fields.message="rebalance health heartbeat" AND jsonPayload.fields.healthy=false"#
    ));
    assert!(filter.contains(
        r#"jsonPayload.message=~"\"message\":\"rebalance health heartbeat\".*\"healthy\":false""#
    ));
    assert!(!filter.contains(r#""rebalance health heartbeat" AND "healthy" AND "false""#));
}

use serde_json::Value;

const REBALANCE_HEARTBEAT_METRIC: &str =
    include_str!("../infra/gcp/monitoring/rebalance-heartbeat-log-metric.json");
const REBALANCE_HEARTBEAT_MISSING_POLICY: &str =
    include_str!("../infra/gcp/monitoring/rebalance-heartbeat-missing-policy.json");
const REBALANCE_FAULT_METRIC: &str =
    include_str!("../infra/gcp/monitoring/rebalance-fault-log-metric.json");
const REBALANCE_FAULT_POLICY: &str =
    include_str!("../infra/gcp/monitoring/rebalance-fault-policy.json");
const TRADING_INVENTORY_BLOCKED_METRIC: &str =
    include_str!("../infra/gcp/monitoring/trading-inventory-blocked-log-metric.json");
const TRADING_INVENTORY_BLOCKED_POLICY: &str =
    include_str!("../infra/gcp/monitoring/trading-inventory-blocked-policy.json");
const BINANCE_DEPTH_UNHEALTHY_METRIC: &str =
    include_str!("../infra/gcp/monitoring/binance-depth-unhealthy-log-metric.json");

fn filter_from(config: &str, name: &str) -> String {
    let config: Value = serde_json::from_str(config).unwrap_or_else(|error| {
        panic!("{name} must be valid JSON: {error}");
    });
    config["filter"]
        .as_str()
        .unwrap_or_else(|| panic!("{name} must have a string filter"))
        .to_owned()
}

fn first_condition_filter_from(config: &str, name: &str) -> String {
    let config: Value = serde_json::from_str(config).unwrap_or_else(|error| {
        panic!("{name} must be valid JSON: {error}");
    });
    config["conditions"][0]["conditionAbsent"]["filter"]
        .as_str()
        .or_else(|| config["conditions"][0]["conditionThreshold"]["filter"].as_str())
        .unwrap_or_else(|| panic!("{name} must have a first condition filter"))
        .to_owned()
}

#[test]
fn rebalance_heartbeat_metric_only_matches_runtime_heartbeat_logs() {
    let filter = filter_from(REBALANCE_HEARTBEAT_METRIC, "rebalance heartbeat log metric");

    assert!(filter.contains(r#"resource.type="gce_instance""#));
    assert!(filter.contains(r#"resource.type="k8s_container""#));
    assert!(filter.contains(r#"resource.labels.cluster_name="arb-bot""#));
    assert!(filter.contains(r#"resource.labels.namespace_name="arb-bot""#));
    assert!(filter.contains(r#"resource.labels.container_name="arb-bot""#));
    assert!(filter.contains(r#"jsonPayload.fields.message="rebalance health heartbeat""#));
    assert!(
        filter.contains(r#"jsonPayload.message=~"\"message\":\"rebalance health heartbeat\"""#)
    );
    assert!(!filter.contains(r#" AND "rebalance health heartbeat""#));
}

#[test]
fn rebalance_heartbeat_missing_policy_targets_active_gke_owner() {
    let filter = first_condition_filter_from(
        REBALANCE_HEARTBEAT_MISSING_POLICY,
        "rebalance heartbeat missing policy",
    );

    assert!(filter.contains(r#"resource.type = "k8s_container""#));
    assert!(!filter.contains(r#"resource.type = "gce_instance""#));
    assert!(filter.contains(
        r#"metric.type = "logging.googleapis.com/user/poly_bot_rebalance_health_heartbeat""#
    ));
}

#[test]
fn rebalance_fault_metric_matches_the_boolean_health_field() {
    let filter = filter_from(REBALANCE_FAULT_METRIC, "rebalance fault log metric");

    assert!(filter.contains(r#"jsonPayload.fields.message="rebalance executor failed closed""#));
    assert!(filter.contains(r#"jsonPayload.fields.message="rebalance planning failed closed""#));
    assert!(filter.contains(
        r#"jsonPayload.fields.message="rebalance health heartbeat" AND jsonPayload.fields.healthy=false"#
    ));
    assert!(filter.contains(
        r#"jsonPayload.message=~"\"message\":\"rebalance health heartbeat\".*\"healthy\":false""#
    ));
    assert!(!filter.contains(r#""rebalance health heartbeat" AND "healthy" AND "false""#));
}

#[test]
fn rebalance_fault_policy_targets_active_gke_owner() {
    let filter = first_condition_filter_from(REBALANCE_FAULT_POLICY, "rebalance fault policy");

    assert!(filter.contains(r#"resource.type = "k8s_container""#));
    assert!(!filter.contains(r#"resource.type = "gce_instance""#));
    assert!(
        filter.contains(r#"metric.type = "logging.googleapis.com/user/poly_bot_rebalance_fault""#)
    );
}

#[test]
fn trading_inventory_blocked_monitoring_accepts_runtime_logs_and_targets_active_gke_owner() {
    let metric_filter = filter_from(
        TRADING_INVENTORY_BLOCKED_METRIC,
        "trading inventory blocked log metric",
    );
    let policy_filter = first_condition_filter_from(
        TRADING_INVENTORY_BLOCKED_POLICY,
        "trading inventory blocked policy",
    );

    assert!(metric_filter.contains(
        r#"jsonPayload.fields.message="arbitrage admission blocked by insufficient inventory""#
    ));
    assert!(metric_filter.contains(r#"jsonPayload.message=~"\"message\":\"arbitrage admission blocked by insufficient inventory\"""#));
    assert!(
        !metric_filter.contains(r#" AND "arbitrage admission blocked by insufficient inventory""#)
    );
    assert!(policy_filter.contains(r#"resource.type = "k8s_container""#));
    assert!(!policy_filter.contains(r#"resource.type = "gce_instance""#));
    assert!(policy_filter.contains(
        r#"metric.type = "logging.googleapis.com/user/poly_bot_trading_inventory_blocked""#
    ));
}

#[test]
fn binance_depth_health_is_a_separate_gke_metric() {
    let filter = filter_from(
        BINANCE_DEPTH_UNHEALTHY_METRIC,
        "Binance depth unhealthy log metric",
    );

    assert!(filter.contains(r#"resource.type="k8s_container""#));
    assert!(filter.contains(r#"resource.labels.cluster_name="arb-bot""#));
    assert!(filter.contains(r#"jsonPayload.fields.message="Binance depth health heartbeat""#));
    assert!(filter.contains(r#"jsonPayload.fields.healthy=false"#));
    assert!(!filter.contains("runtime phase changed"));
}

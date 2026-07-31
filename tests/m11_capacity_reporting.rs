use std::fs;

const DEPLOY_WORKFLOW: &str = include_str!("../.github/workflows/deploy-gke.yml");

#[test]
fn m11_report_enforces_the_full_read_only_capacity_gate() {
    let report = fs::read_to_string("scripts/report-m11-capacity-replay")
        .expect("M11 capacity report must be readable");
    let artifact = fs::read_to_string("config/capacity/m11-maximum-pair-replay.v1.json")
        .expect("M11 capacity artifact must be readable");

    assert!(report.contains(".pair_count == 20"));
    assert!(report.contains(".frames_per_pair >= 100000"));
    assert!(report.contains(".route_failures == 0"));
    assert!(report.contains(".dependency_faults == 0"));
    assert!(report.contains(".decision_owner_latency.p99_ns <= $p99_hard_ns"));
    assert!(report.contains(".fairness.maximum_observed_running"));
    assert!(report.contains(".fairness.unique_strategies_before_noisy_repeat == .pair_count"));
    assert!(report.contains(".rehydration.pool_publications >= 100"));
    assert!(report.contains(".rehydration.partial_batches_rejected == 1"));
    assert!(report.contains(".rehydration.pool_build_latency.p99_ns <= 200000"));
    assert!(report.contains(".network_io_performed == false"));
    assert!(report.contains(".external_mutations == 0"));
    assert!(report.contains("M11_REQUIRE_LINUX_RSS"));

    assert!(artifact.contains("\"mode\": \"capacity_replay_only\""));
    assert!(artifact.contains("\"network_io_enabled\": false"));
    assert!(artifact.contains("\"external_mutation_authorized\": false"));
    assert_eq!(artifact.matches("\"pair_id\":").count(), 20);
}

#[test]
fn deployment_gates_the_exact_image_on_the_fixed_c4_before_rollout() {
    let gate = DEPLOY_WORKFLOW
        .find("Gate exact image with M11 maximum-pair replay on target C4")
        .expect("M11 target gate must exist");
    let rollout = DEPLOY_WORKFLOW
        .find("Roll out on the fixed node")
        .expect("rollout step must exist");

    assert!(gate < rollout);
    assert!(DEPLOY_WORKFLOW.contains("--target-cpu-class c4-highcpu-8"));
    assert!(DEPLOY_WORKFLOW.contains("tee /dev/termination-log"));
    assert!(DEPLOY_WORKFLOW.contains("state.terminated.message"));
    assert!(DEPLOY_WORKFLOW.contains(".decision_owner_latency.p99_ns <= 25000"));
    assert!(DEPLOY_WORKFLOW.contains(".total_strategy_frames == 2000000"));
    assert!(DEPLOY_WORKFLOW.contains(".rehydration.pool_publications == 115"));
    assert!(DEPLOY_WORKFLOW.contains(".route_failures == 0"));
    assert!(DEPLOY_WORKFLOW.contains(".external_mutations == 0"));
    assert!(DEPLOY_WORKFLOW.contains("automountServiceAccountToken = false"));
    assert!(DEPLOY_WORKFLOW.contains("\"requests\": {\"cpu\": \"1\", \"memory\": \"128Mi\"}"));
    assert!(DEPLOY_WORKFLOW.contains(".gate == \"target_c4_replay_ready\""));
}

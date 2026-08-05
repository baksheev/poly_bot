use std::fs;

const DEPLOY_WORKFLOW: &str = include_str!("../.github/workflows/deploy-gke.yml");

#[test]
fn report_enforces_the_full_read_only_capacity_gate() {
    let report = fs::read_to_string("scripts/report-capacity-replay")
        .expect("capacity report must be readable");
    let artifact = fs::read_to_string("config/capacity/maximum-pair-replay.v1.json")
        .expect("capacity artifact must be readable");

    assert!(report.contains(".pair_count == 20"));
    assert!(report.contains(".provider_pool_counts.camelot_v3 == 1"));
    assert!(report.contains(".provider_parity.prepared_quote.p99_ratio_bps <= 10500"));
    assert!(report.contains(".provider_parity.prepared_curve_build.p99_ratio_bps <= 12000"));
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
    assert!(report.contains("REQUIRE_LINUX_RSS"));

    assert!(artifact.contains("\"mode\": \"capacity_replay_only\""));
    assert!(artifact.contains("\"network_io_enabled\": false"));
    assert!(artifact.contains("\"external_mutation_authorized\": false"));
    assert_eq!(artifact.matches("\"pair_id\":").count(), 20);
    assert!(artifact.contains("\"pair_id\": \"capacity-arbitrum-arb-usdc\""));
    assert!(artifact.contains("\"symbol\": \"ARBUSDC\""));
    assert!(artifact.contains("\"pancakeswap_v3\""));
    assert!(artifact.contains("\"camelot_v3\""));
}

#[test]
fn deployment_gates_the_exact_image_on_the_fixed_c4_before_rollout() {
    let gate = DEPLOY_WORKFLOW
        .find("Gate exact image with capacity maximum-pair replay on target C4")
        .expect("capacity target gate must exist");
    let rollout = DEPLOY_WORKFLOW
        .find("Roll out on the fixed node")
        .expect("rollout step must exist");
    let bootstrap = DEPLOY_WORKFLOW
        .find("Bootstrap reviewed ARB inventory once")
        .expect("ARB bootstrap step must exist");

    assert!(gate < rollout);
    assert!(DEPLOY_WORKFLOW.contains("--target-cpu-class c4-highcpu-8"));
    assert!(DEPLOY_WORKFLOW.contains("tee /dev/termination-log"));
    assert!(DEPLOY_WORKFLOW.contains("state.terminated.message"));
    assert!(DEPLOY_WORKFLOW.contains(".decision_owner_latency.p99_ns <= 25000"));
    assert!(DEPLOY_WORKFLOW.contains(".total_strategy_frames == 2000000"));
    assert!(DEPLOY_WORKFLOW.contains(".pool_count == 25"));
    assert!(DEPLOY_WORKFLOW.contains(".provider_pool_counts.camelot_v3 == 1"));
    assert!(DEPLOY_WORKFLOW.contains(".provider_parity.prepared_quote.p99_ratio_bps <= 10500"));
    assert!(
        DEPLOY_WORKFLOW.contains(".provider_parity.prepared_curve_build.p99_ratio_bps <= 12000")
    );
    assert!(DEPLOY_WORKFLOW.contains(".rehydration.pool_publications == 125"));
    assert!(DEPLOY_WORKFLOW.contains(".route_failures == 0"));
    assert!(DEPLOY_WORKFLOW.contains(".external_mutations == 0"));
    assert!(DEPLOY_WORKFLOW.contains("automountServiceAccountToken = false"));
    assert!(DEPLOY_WORKFLOW.contains(".spec.template.spec.initContainers"));
    assert!(DEPLOY_WORKFLOW.contains("\"requests\": {\"cpu\": \"250m\", \"memory\": \"128Mi\"}"));
    assert!(DEPLOY_WORKFLOW.contains(".gate == \"target_c4_replay_ready\""));
    assert!(DEPLOY_WORKFLOW.contains(
        "replay_deployment=\"arb-bot-capacity-replay-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}\""
    ));
    assert!(!DEPLOY_WORKFLOW[gate..bootstrap].contains("kubectl delete job"));
    assert!(DEPLOY_WORKFLOW.contains("kubectl delete deployment \"${replay_deployment}\""));
    assert!(!DEPLOY_WORKFLOW.contains("kubectl get node "));
    assert!(!DEPLOY_WORKFLOW.contains(".spec.template.spec.activeDeadlineSeconds"));
    assert!(DEPLOY_WORKFLOW.contains("replay_scheduling_reason"));
    assert!(DEPLOY_WORKFLOW.contains("= Unschedulable"));
}

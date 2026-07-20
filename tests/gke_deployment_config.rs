const RELEASE_PLATFORM: &str = include_str!("../infra/gcp/gke/release-platform.yaml");
const DEPLOY_WORKFLOW: &str = include_str!("../.github/workflows/deploy-gke.yml");

#[test]
fn gke_manifest_is_the_full_live_v7_adaptive_shadow_owner() {
    assert!(
        RELEASE_PLATFORM
            .contains("DOMAIN_CONFIG_PATH: config/strategies/usdc-wld-world-chain.v7.json")
    );
    assert!(RELEASE_PLATFORM.contains("ARBITRAGE_EXECUTION_MODE: full_live"));
    assert!(RELEASE_PLATFORM.contains("REBALANCE_EXECUTION_MODE: full_live"));
    assert!(
        RELEASE_PLATFORM
            .contains("ARBITRAGE_TRADE_JOURNAL_PATH: /var/lib/arb-bot/arbitrage-live-trades.jsonl")
    );
    assert!(
        RELEASE_PLATFORM
            .contains("ARBITRAGE_WALLET_JOURNAL_PATH: /var/lib/arb-bot/arbitrage-wallet.jsonl")
    );
    assert!(RELEASE_PLATFORM.contains(
        "ARBITRAGE_BINANCE_ORDER_JOURNAL_PATH: /var/lib/arb-bot/arbitrage-binance-orders.jsonl"
    ));
    assert!(
        RELEASE_PLATFORM
            .contains("EVM_WALLET_JOURNAL_PATH: /var/lib/arb-bot/rebalance-wallet.jsonl")
    );
    assert!(!RELEASE_PLATFORM.contains("usdc-wld-world-chain.v4.json"));
}

#[test]
fn gke_workflow_verifies_the_runtime_startup_mode() {
    assert!(DEPLOY_WORKFLOW.contains("Verify GCE live owner is stopped"));
    assert!(DEPLOY_WORKFLOW.contains(".data.ARBITRAGE_EXECUTION_MODE"));
    assert!(DEPLOY_WORKFLOW.contains(".data.REBALANCE_EXECUTION_MODE"));
    assert!(DEPLOY_WORKFLOW.contains("usdc-wld-world-chain.v7.json"));
    assert!(DEPLOY_WORKFLOW.contains("previous_runtime_config"));
    assert!(!DEPLOY_WORKFLOW.contains("kubectl logs"));
}

const RELEASE_PLATFORM: &str = include_str!("../infra/gcp/gke/release-platform.yaml");
const DEPLOYMENT: &str = include_str!("../infra/gcp/gke/deployment.yaml");
const DEPLOY_WORKFLOW: &str = include_str!("../.github/workflows/deploy-gke.yml");

#[test]
fn gke_manifest_is_the_full_live_v12_adaptive_owner() {
    assert!(
        RELEASE_PLATFORM
            .contains("DOMAIN_CONFIG_PATH: config/strategies/usdc-wld-world-chain.v12.json")
    );
    assert!(!RELEASE_PLATFORM.contains("GAS_PRICE_MAX_TRANSPORT_SILENCE_MS"));
    assert!(RELEASE_PLATFORM.contains("DEX_HEAD_MAX_AGE_MS: \"30000\""));
    assert!(RELEASE_PLATFORM.contains("BALANCE_SYNC_INTERVAL_MS: \"5000\""));
    assert!(RELEASE_PLATFORM.contains("BALANCE_MAX_AGE_MS: \"10000\""));
    assert!(!RELEASE_PLATFORM.contains("MARKET_DATA_MAX_AGE_MS"));
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
    assert!(DEPLOY_WORKFLOW.contains("usdc-wld-world-chain.v12.json"));
    assert!(DEPLOY_WORKFLOW.contains("opportunity_threshold_bps"));
    assert!(DEPLOY_WORKFLOW.contains("max_quote_age_ms"));
    assert!(DEPLOY_WORKFLOW.contains("max_transport_silence_ms"));
    assert!(!DEPLOY_WORKFLOW.contains("GAS_PRICE_MAX_TRANSPORT_SILENCE_MS"));
    assert!(DEPLOY_WORKFLOW.contains(".data.DEX_HEAD_MAX_AGE_MS"));
    assert!(DEPLOY_WORKFLOW.contains(".data.BALANCE_SYNC_INTERVAL_MS"));
    assert!(DEPLOY_WORKFLOW.contains(".data.BALANCE_MAX_AGE_MS"));
    assert!(!DEPLOY_WORKFLOW.contains("MARKET_DATA_MAX_AGE_MS"));
    assert!(DEPLOY_WORKFLOW.contains("min_expected_profit_token_a_base_units"));
    assert!(DEPLOY_WORKFLOW.contains(".adaptive_sizing.mode"));
    assert!(DEPLOY_WORKFLOW.contains("max_trade_notional_token_a_base_units"));
    assert!(DEPLOY_WORKFLOW.contains("recent_full_depth_max_age_ms"));
    assert!(DEPLOY_WORKFLOW.contains("recent_full_depth_max_update_delta"));
    assert!(DEPLOY_WORKFLOW.contains("top_of_book_max_trade_notional_token_a_base_units"));
    assert!(DEPLOY_WORKFLOW.contains("has(\"balance_safety_multiplier\")"));
    assert!(DEPLOY_WORKFLOW.contains("previous_runtime_config"));
    assert!(!DEPLOY_WORKFLOW.contains("kubectl logs"));
}

#[test]
fn gke_manifest_runs_esp_as_an_isolated_public_market_data_collector() {
    assert!(
        RELEASE_PLATFORM
            .contains("DOMAIN_CONFIG_PATH: config/strategies/usdc-esp-arbitrum.v2.json")
    );
    assert!(RELEASE_PLATFORM.contains("name: arb-bot-esp-market-data"));
    assert!(RELEASE_PLATFORM.contains("RUNTIME_READY_FILE: /tmp/arb-bot-esp-ready"));
    assert!(DEPLOYMENT.contains("name: esp-market-data"));
    assert!(DEPLOYMENT.contains("exec arb_bot collect-prices"));
    assert!(DEPLOYMENT.contains("secretProviderClass: arb-bot-esp-market-data"));
    assert!(!DEPLOYMENT.contains("/var/run/secrets/arb-bot-esp/BINANCE_API_KEY"));
    assert!(!DEPLOYMENT.contains("/var/run/secrets/arb-bot-esp/EVM_WALLET_PRIVATE_KEY"));
    assert!(DEPLOY_WORKFLOW.contains("usdc-esp-arbitrum.v2.json"));
    assert!(DEPLOY_WORKFLOW.contains(".pairs[0].execution_enabled"));
    assert!(DEPLOY_WORKFLOW.contains(".pairs[0].rebalance.enabled"));
}

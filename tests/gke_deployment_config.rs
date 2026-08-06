const RELEASE_PLATFORM: &str = include_str!("../infra/gcp/gke/release-platform.yaml");
const DEPLOYMENT: &str = include_str!("../infra/gcp/gke/deployment.yaml");
const DEPLOY_WORKFLOW: &str = include_str!("../.github/workflows/deploy-gke.yml");
const RECOVERY_WORKFLOW: &str = include_str!("../.github/workflows/operate-gke-recovery.yml");
const MAIN: &str = include_str!("../src/main.rs");
const COMPILED_DOMAIN: &str =
    include_str!("../config/domain/compiled-multi-pair-production.v1.json");

#[test]
fn gke_manifest_is_the_full_live_v14_adaptive_owner() {
    assert!(
        RELEASE_PLATFORM
            .contains("DOMAIN_CONFIG_PATH: config/domain/compiled-multi-pair-production.v1.json")
    );
    assert_eq!(
        RELEASE_PLATFORM
            .matches("DOMAIN_CONFIG_PATH: config/domain/compiled-multi-pair-production.v1.json")
            .count(),
        2
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
        "ARBITRAGE_ARBITRUM_WALLET_JOURNAL_PATH: /var/lib/arb-bot/arbitrage-arbitrum-wallet.jsonl"
    ));
    assert!(RELEASE_PLATFORM.contains(
        "ARBITRAGE_LINEA_WALLET_JOURNAL_PATH: /var/lib/arb-bot/arbitrage-linea-wallet.jsonl"
    ));
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
    let compiled: serde_json::Value = serde_json::from_str(COMPILED_DOMAIN).unwrap();
    let live_strategy = &compiled["sources"]
        .as_array()
        .unwrap()
        .iter()
        .find(|source| source["snapshot"]["live_trading_enabled"] == true)
        .unwrap()["snapshot"]["pairs"][0]["strategy"];
    assert!(live_strategy.get("balance_safety_multiplier").is_none());
    assert!(DEPLOY_WORKFLOW.contains("Verify GCE live owner is stopped"));
    assert!(DEPLOY_WORKFLOW.contains("Verify operator-maintained Linea gas invariant read-only"));
    assert!(DEPLOY_WORKFLOW.contains("https://rpc.linea.build"));
    assert!(DEPLOY_WORKFLOW.contains("2386f26fc10000"));
    assert!(DEPLOY_WORKFLOW.contains("less than the reviewed 0.01 ETH Linea operator gas reserve"));
    assert!(DEPLOY_WORKFLOW.contains(".data.ARBITRAGE_EXECUTION_MODE"));
    assert!(DEPLOY_WORKFLOW.contains(".data.REBALANCE_EXECUTION_MODE"));
    assert!(DEPLOY_WORKFLOW.contains("compiled-multi-pair-production.v1.json"));
    assert!(DEPLOY_WORKFLOW.contains(".bundle_kind"));
    assert!(DEPLOY_WORKFLOW.contains(".capabilities"));
    assert!(DEPLOY_WORKFLOW.contains(".stream_shards"));
    assert!(DEPLOY_WORKFLOW.contains("binance-spot:primary:ARBUSDC"));
    assert!(DEPLOY_WORKFLOW.contains("binance-spot:primary:ESPUSDC"));
    assert!(DEPLOY_WORKFLOW.contains("binance-spot:primary:WLDUSDC"));
    assert!(DEPLOY_WORKFLOW.contains("binance-spot:primary:USDCUSDT"));
    assert!(DEPLOY_WORKFLOW.contains("strategy:linea-usdt-usdc"));
    assert!(DEPLOY_WORKFLOW.contains("linea-usdt-usdc-lynex-algebra-v1-9-full-live-v1"));
    assert!(DEPLOY_WORKFLOW.contains("0x6e9ad0b8a41e2c148e7b0385d3ecbfdb8a216a9b"));
    assert!(DEPLOY_WORKFLOW.contains("lynex_algebra_v1_9"));
    let linea_pool = compiled["pools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|pool| pool["pair_id"] == "linea-usdt-usdc")
        .expect("compiled Linea pool exists");
    assert_eq!(linea_pool["protocol"], "lynex_algebra_v1_9");
    assert!(linea_pool["fee_pips"].is_null());
    assert_eq!(linea_pool["tick_spacing"], 1);
    assert!(DEPLOY_WORKFLOW.contains(
        "protocol == \"lynex_algebra_v1_9\" and .fee_pips == null and .tick_spacing == 1"
    ));
    assert!(DEPLOY_WORKFLOW.contains("live_runtime"));
    assert!(DEPLOY_WORKFLOW.contains("public_price_collector"));
    assert!(
        DEPLOY_WORKFLOW.contains("arb-bot-production-usdc-wld-world-chain-v14-v3-one-percent-pool")
    );
    assert!(DEPLOY_WORKFLOW.contains("[500,3000,10000]"));
    assert!(DEPLOY_WORKFLOW.contains("0x610e319b3a3ab56a0ed5562927d37c233774ba39"));
    assert!(DEPLOY_WORKFLOW.contains("arb-bot-production-usdc-arb-arbitrum-v5-camelot-v3-live"));
    assert!(DEPLOY_WORKFLOW.contains("[500,3000]"));
    assert!(DEPLOY_WORKFLOW.contains("0xb0f6ca40411360c03d41c5ffc5f179b8403cdcf8"));
    assert!(DEPLOY_WORKFLOW.contains("0x9ffca51d23ac7f7df82da414865ef1055e5afcc3"));
    assert!(DEPLOY_WORKFLOW.contains("0xfae2ae0a9f87fd35b5b0e24b47bac796a7eefea1"));
    assert!(DEPLOY_WORKFLOW.contains("0x1F721E2E82F6676FCE4eA07A5958cF098D339e18"));
    assert!(DEPLOY_WORKFLOW.contains("selection_enabled\":true"));
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
    assert!(DEPLOY_WORKFLOW.contains("balance_safety_multiplier"));
    assert!(DEPLOY_WORKFLOW.contains("previous_runtime_data"));
    assert!(DEPLOY_WORKFLOW.contains("runtime_rollback_patch"));
    assert!(DEPLOY_WORKFLOW.contains("kubectl patch configmap arb-bot-runtime"));
    assert!(DEPLOY_WORKFLOW.contains("--type=json"));
    assert!(DEPLOY_WORKFLOW.contains("Apply ClickHouse telemetry migrations"));
    assert!(DEPLOY_WORKFLOW.contains("exec arb_bot migrate"));
    assert!(DEPLOY_WORKFLOW.contains("secretProviderClass\": \"arb-bot-esp-market-data"));
    assert!(!DEPLOY_WORKFLOW.contains("gcloud secrets versions access"));
    assert!(MAIN.contains("network_runtime_count"));
    assert!(MAIN.contains("binance_strategy_max_transport_silence_ms"));
    assert!(MAIN.contains("hot_path_strategy_count"));
    assert!(MAIN.contains("hot_path_direct_binance_poll"));
    assert!(MAIN.contains("hot_path_dependency_index"));
    assert!(MAIN.contains("hot_path_sizing_policy"));
    assert!(MAIN.contains("secondary_hot_path_external_mutation_authorized"));
    assert!(MAIN.contains("portfolio_inventory_key"));
    assert!(MAIN.contains("portfolio_location_count"));
    assert!(MAIN.contains("portfolio_allocator_mode"));
    assert!(MAIN.contains("portfolio_external_mutation_authorized"));
    assert!(MAIN.contains("live_rebalance_adapter"));
    assert!(MAIN.contains("Arbitrum full-live production strategies configured"));
    assert!(MAIN.contains("shared_inventory_owner"));
    assert!(MAIN.contains("shared_binance_order_owner"));
    assert!(MAIN.contains("enable_camelot_submissions_after_allowance_lock"));
    let camelot_allowance = MAIN
        .find("protocol: DexProtocol::CamelotV3")
        .expect("Camelot allowance is prepared by the Arbitrum execution owner");
    let camelot_gate = MAIN
        .find("enable_camelot_submissions_after_allowance_lock")
        .expect("Camelot submission gate is wired");
    assert!(camelot_allowance < camelot_gate);
    assert!(MAIN.contains("secondary_hot_path_rebalance_mutation_authorized"));
    assert!(MAIN.contains("report_strategy_dependency_faults"));
    assert!(MAIN.contains("engine.take_adaptive_sizing_jobs()"));
    assert!(MAIN.contains("esp_engine.take_adaptive_sizing_jobs()"));
    assert!(MAIN.contains("arb_engine.take_adaptive_sizing_jobs()"));
    assert!(MAIN.contains("linea_engine.take_adaptive_sizing_jobs()"));
    assert!(MAIN.contains("engine.on_adaptive_sizing_result(result)"));
    assert!(MAIN.contains("esp_engine.on_adaptive_sizing_result(result)"));
    assert!(MAIN.contains("arb_engine.on_adaptive_sizing_result(result)"));
    assert!(MAIN.contains("linea_engine.on_adaptive_sizing_result(result)"));
    assert!(DEPLOY_WORKFLOW.contains("Bootstrap reviewed ARB inventory once"));
    assert!(DEPLOY_WORKFLOW.contains("bootstrap-arb-inventory --quote-usdc 500"));
    assert!(DEPLOY_WORKFLOW.contains("active_operation_count=0"));
    assert!(DEPLOY_WORKFLOW.contains("arb-inventory-bootstrap-v1=complete:"));
    assert!(DEPLOY_WORKFLOW.contains("arb-bot-arb-entry-stop"));
    assert!(DEPLOY_WORKFLOW.contains("arb-bot-arb-entry-clear"));
    let startup_drain = MAIN
        .find("drain_startup_dex_backlog(")
        .expect("startup DEX backlog drain is wired");
    let readiness = MAIN
        .find("let runtime_ready_file = mark_runtime_ready()")
        .expect("runtime readiness is wired");
    assert!(startup_drain < readiness);
    assert!(MAIN.contains("backlog_empty_before_ready"));
    assert!(MAIN.contains("on_startup_dex_event"));
    assert!(!DEPLOY_WORKFLOW.contains("kubectl exec"));
    assert!(!DEPLOY_WORKFLOW.contains("kubectl scale"));
    assert!(!DEPLOY_WORKFLOW.contains("kind: \"Job\""));
    assert!(DEPLOY_WORKFLOW.contains("\"path\":\"/spec/replicas\",\"value\":0"));
    assert!(DEPLOY_WORKFLOW.contains("\"path\":\"/spec/replicas\",\"value\":1"));
    assert!(!DEPLOY_WORKFLOW.contains("gcloud logging read"));
    assert!(DEPLOY_WORKFLOW.contains("wait_operation_owner \"${bootstrap_owner}\" bootstrap"));
    assert!(DEPLOY_WORKFLOW.contains("Verify Binance LINEA direct capital routes read-only"));
    assert!(DEPLOY_WORKFLOW.contains("binance-capital-recovery --coin USDC --network LINEA"));
    assert!(DEPLOY_WORKFLOW.contains("binance-capital-recovery --coin USDT --network LINEA"));
    assert!(DEPLOY_WORKFLOW.contains("secretProviderClass\": \"arb-bot-binance-capital-read"));
    let release_platform = DEPLOY_WORKFLOW
        .find("Apply release platform before production preflights")
        .expect("release platform is applied before production preflights");
    let linea_capital_preflight = DEPLOY_WORKFLOW
        .find("Verify Binance LINEA direct capital routes read-only")
        .expect("LINEA capital preflight is configured");
    let rollout = DEPLOY_WORKFLOW
        .find("Roll out on the fixed node")
        .expect("fixed-node rollout is configured");
    assert!(release_platform < linea_capital_preflight);
    assert!(linea_capital_preflight < rollout);
    assert_eq!(
        DEPLOY_WORKFLOW
            .match_indices("infra/gcp/gke/release-platform.yaml")
            .count(),
        1
    );
    assert!(!DEPLOY_WORKFLOW.contains("scripts/create-gke-node-pool"));
    assert!(!DEPLOY_WORKFLOW.contains("node-pools delete"));
    assert!(!DEPLOY_WORKFLOW.contains("delete persistentvolumeclaim arb-bot-state"));
    assert!(DEPLOY_WORKFLOW.contains(".config.machineType"));
    assert!(DEPLOY_WORKFLOW.contains(".autoscaling.enabled // false"));
}

#[test]
fn gke_manifest_runs_esp_as_an_isolated_public_market_data_collector() {
    assert!(
        RELEASE_PLATFORM
            .contains("DOMAIN_CONFIG_PATH: config/domain/compiled-multi-pair-production.v1.json")
    );
    assert!(RELEASE_PLATFORM.contains("name: arb-bot-esp-market-data"));
    assert!(RELEASE_PLATFORM.contains("RUNTIME_READY_FILE: /tmp/arb-bot-esp-ready"));
    assert!(DEPLOYMENT.contains("name: esp-market-data"));
    assert!(DEPLOYMENT.contains("exec arb_bot collect-prices"));
    assert!(DEPLOYMENT.contains("secretProviderClass: arb-bot-esp-market-data"));
    assert_eq!(
        DEPLOYMENT
            .matches("export ARBITRUM_RPC_URL=\"https://arb-mainnet.g.alchemy.com/v2/")
            .count(),
        2
    );
    assert_eq!(
        DEPLOYMENT
            .matches("export ARBITRUM_WS_URL=\"wss://arb-mainnet.g.alchemy.com/v2/")
            .count(),
        2
    );
    assert_eq!(
        DEPLOYMENT
            .matches("export LINEA_RPC_URL=\"https://linea-mainnet.g.alchemy.com/v2/")
            .count(),
        2
    );
    assert_eq!(
        DEPLOYMENT
            .matches("export LINEA_WS_URL=\"wss://linea-mainnet.g.alchemy.com/v2/")
            .count(),
        2
    );
    assert!(!DEPLOYMENT.contains("/var/run/secrets/arb-bot-esp/BINANCE_API_KEY"));
    assert!(!DEPLOYMENT.contains("/var/run/secrets/arb-bot-esp/EVM_WALLET_PRIVATE_KEY"));
    assert!(DEPLOY_WORKFLOW.contains("arb-bot-production-usdc-esp-arbitrum-v7-six-usdc-detector"));
    assert_eq!(
        DEPLOY_WORKFLOW
            .matches(".pairs[0].quote_sizing.token_a_base_units")
            .count(),
        4
    );
    assert_eq!(DEPLOY_WORKFLOW.matches("= 6000000").count(), 4);
    assert!(DEPLOY_WORKFLOW.contains(".pairs[0].full_live_policy.production_approval_actor"));
    assert!(DEPLOY_WORKFLOW.contains("arbitrum_max_fee_headroom_bps"));
    assert!(DEPLOY_WORKFLOW.contains("router_allowance_mode"));
    assert!(DEPLOY_WORKFLOW.contains("maximum_rebalance_token_a_debit_base_units"));
    assert!(DEPLOY_WORKFLOW.contains("maximum_rebalance_token_b_debit_base_units"));
    assert!(DEPLOY_WORKFLOW.contains(".pairs[0].execution_enabled"));
    assert!(DEPLOY_WORKFLOW.contains(".pairs[0].full_live"));
    assert!(DEPLOY_WORKFLOW.contains(".pairs[0].rebalance.enabled"));
}

#[test]
fn gke_full_live_runtime_keeps_durable_state_and_safe_rollback_guards() {
    assert!(DEPLOYMENT.contains("strategy:\n    type: Recreate"));
    assert!(DEPLOYMENT.contains("arb-bot/durable-state-schema-version: \"2\""));
    assert!(!DEPLOYMENT.contains("initContainers:"));
    assert!(DEPLOYMENT.contains("claimName: arb-bot-state"));
    assert!(!DEPLOYMENT.contains("kind: Job"));
    let rollout = DEPLOY_WORKFLOW
        .find("Roll out on the fixed node")
        .expect("rollout step exists");
    assert!(!DEPLOY_WORKFLOW[rollout..].contains("kubectl scale"));
    assert!(!DEPLOY_WORKFLOW.contains("jobs.batch"));
    assert!(DEPLOY_WORKFLOW.contains("maximum_rebalance_token_a_fee_base_units"));
    assert!(DEPLOY_WORKFLOW.contains("maximum_rebalance_token_b_fee_base_units"));
    assert!(DEPLOY_WORKFLOW.contains(".status.containerStatuses"));
    assert!(DEPLOY_WORKFLOW.contains(".restartCount >= 2"));
    assert!(DEPLOY_WORKFLOW.contains("repeatedly failed startup"));
    assert!(DEPLOY_WORKFLOW.contains("previous_durable_schema_version"));
    assert!(DEPLOY_WORKFLOW.contains("automatic_rollback_allowed=false"));
    assert!(DEPLOY_WORKFLOW.contains("previous_durable_schema_version >= durable_schema_version"));
    assert!(DEPLOY_WORKFLOW.contains("automatic rollback refused"));
    assert!(DEPLOY_WORKFLOW.contains("kubectl rollout undo deployment/arb-bot"));
    assert!(DEPLOY_WORKFLOW.contains("fixed_pool"));
    assert!(!DEPLOY_WORKFLOW.contains("GKE_RELEASE_ID"));
}

#[test]
fn gke_recovery_workflow_enforces_a_quiescent_single_owner_handoff() {
    assert!(RECOVERY_WORKFLOW.contains("workflow_dispatch:"));
    assert!(!RECOVERY_WORKFLOW.contains("workflow_run:"));
    assert!(!RECOVERY_WORKFLOW.contains("push:"));
    assert!(RECOVERY_WORKFLOW.contains("group: production-gke"));
    assert!(RECOVERY_WORKFLOW.contains("environment: production"));
    assert!(RECOVERY_WORKFLOW.contains("test \"${gce_status}\" = TERMINATED"));
    assert!(RECOVERY_WORKFLOW.contains("test \"${deployed_revision}\" = \"${SOURCE_SHA}\""));
    assert!(RECOVERY_WORKFLOW.contains("[[ \"${image}\" == *@sha256:* ]]"));
    assert!(RECOVERY_WORKFLOW.contains("active_operation_count=0"));
    assert!(RECOVERY_WORKFLOW.contains("arb-bot/recovery-handoff="));

    let proof = RECOVERY_WORKFLOW
        .find("test -s \"${ARBITRAGE_ENTRY_STOP_FILE}.recovery-safe\"")
        .expect("recovery workflow waits for the runtime quiescence proof");
    let stop = RECOVERY_WORKFLOW
        .find("kubectl scale deployment arb-bot")
        .expect("recovery workflow stops the application owner");
    let job = RECOVERY_WORKFLOW
        .find("Run the isolated recovery command")
        .expect("recovery workflow creates a one-shot owner");
    assert!(proof < stop && stop < job);

    assert!(RECOVERY_WORKFLOW.contains("backoffLimit: 0"));
    assert!(RECOVERY_WORKFLOW.contains("activeDeadlineSeconds: 1200"));
    assert!(RECOVERY_WORKFLOW.contains("select(.name == \"arb-bot\")"));
    assert!(RECOVERY_WORKFLOW.contains(".spec.containers = ["));
    assert!(RECOVERY_WORKFLOW.contains("arb_bot arbitrage-record-operator-recovery"));
    assert!(RECOVERY_WORKFLOW.contains("RECORD_LIVE_ARBITRAGE_OPERATOR_RECOVERY"));
    assert!(RECOVERY_WORKFLOW.contains("inputs.operation == 'recovery-execute'"));
    assert!(RECOVERY_WORKFLOW.contains("if: success() && inputs.operation == 'recovery-execute'"));
    assert!(!RECOVERY_WORKFLOW.contains("confirmation=RELEASE"));
    assert!(RECOVERY_WORKFLOW.contains("test \"${RECOVERY_CONFIRMATION}\" = RELEASE"));
}

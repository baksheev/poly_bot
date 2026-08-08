use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use alloy_primitives::{Address, B256, U256};
use anyhow::{Context, bail, ensure};
use arb_bot::{
    across::{
        AcrossClient, AcrossQuoteRequest, LINEA_USDC, LINEA_USDT, OPTIMISM_CHAIN_ID, OPTIMISM_USDC,
        OPTIMISM_USDT, WORLD_CHAIN_CHAIN_ID, WORLD_CHAIN_USDC, is_retryable_quote_error,
        validate_quote,
    },
    arbitrage::{
        EntryPreflightHandle, ExecutionMode, LegRole, LegStatus, MAX_RECOVERY_ATTEMPTS,
        OperatorRecoveryEvidence, PaperTradeCoordinator, TradeJournalScope, TradeStage,
        paper_trade_channel,
    },
    balances::{
        BalanceEvent, BalanceSource, BalanceSync, WalletBalanceSnapshot, WalletReadClient,
        binance_snapshot, fetch_wallet_snapshot, fetch_wallet_snapshot_coordinated,
        spawn_balance_sync, spawn_wallet_balance_sync,
    },
    binance::account::{
        AccountInformation, BinanceAccountClient, BinanceAccountState, BinanceClockSync,
    },
    binance::capital::{
        CapitalRecoverySnapshot, CapitalRouteState, TravelRuleWithdrawalRecord, WithdrawalRecord,
        select_capital_routes,
    },
    binance::{
        bootstrap::bootstrap_arb_inventory,
        execution::{BinanceExecutionService, BinanceOrderRequest, BinanceOrderRequestKind},
        order_journal::{
            BinanceOrderIntent, BinanceOrderJournal, BinanceOrderJournalScope, BinanceOrderProgress,
        },
        order_plan::{decimal_from_base_units, recovery_client_order_id},
        runtime::SharedBinanceRuntime,
        user_data::{UserDataEvent, UserDataStream},
        validation::{BinanceCanaryKind, execute_order_round_trip},
        ws_api::{BinanceWsApiClient, OrderResult, WsApiError},
    },
    chain::{
        logs::EthLogFilter,
        rpc::{CanonicalBlock, JsonRpcClient},
    },
    config::{self, Cli, Command},
    dex::{
        events::build_log_filters,
        execution::{AllowanceRequirement, DexExecutionService, DexExecutor, DexProtocol},
        hydration::{DexHydrator, PoolIdentity},
        mirror::{DexMirror, LogApplyResult},
        revert_diagnostics::dex_revert_diagnostic_channel,
        validation::{execute_recovery_sell, execute_round_trip},
    },
    domain::{
        compiled::{
            CompatibilityRole, CompiledBinanceRuntimePlan, CompiledCapitalAllocatorMode,
            CompiledCapitalNetworkPolicy, CompiledCapitalPolicy, CompiledCapitalTokenPolicy,
            CompiledGraphSummary, CompiledHotPathRuntimePlan, CompiledNetworkGasPolicy,
            CompiledNetworkRuntimePlan, CompiledPortfolioRuntimePlan, EconomicAssetId, NetworkId,
            compile_manifest_to_path, load_compatibility_domain,
        },
        config::{DexProvider, LoadedDomainConfig},
    },
    engine::{AdaptiveSizingJob, AdaptiveSizingTaskResult, BinanceFeeBps, TradingEngine},
    execution_accounting::{
        CommissionAssetValuation, binance_leg_result, dex_leg_result,
        native_gas_to_token_a_base_units,
    },
    hot_telemetry,
    inventory::SharedInventoryReservations,
    live_execution::{
        ComposedLiveLegExecutor, ComposedLiveLegExecutorConfig, LivePairPolicy, LiveRiskLimits,
        RoutedLiveLegExecutor, live_trade_channel,
    },
    live_readiness::{
        ARBITRUM_CHAIN_ID, CHAIN_READINESS_REFRESH_INTERVAL, ChainReadiness, ChainReadinessProbe,
        ChainReadinessStatus, inspect_chain_readiness, validate_binance_readiness,
        validate_detector_control_notional, validate_rebalance_readiness,
    },
    market_data::{
        MarketEvent,
        alchemy::{AlchemyDexStream, DexStreamEvent, connect_dex_stream},
        binance::BookTickerFeed,
    },
    network_runtime::NetworkRuntimeRegistry,
    opportunity::{
        ArbitrageDirection, OpportunityEngine, PreparedPoolBuildBatch, PreparedPoolBuildRequest,
    },
    portfolio::{
        PortfolioCatalog, capital_allocator_channel, remaining_rebalance_authority_on_chain,
    },
    pretrade_cost::PreTradeCostTelemetry,
    rebalance::{
        Direction, RebalanceAction, RebalanceExecutionAuthority, RebalanceExecutionOperation,
        RebalanceExecutionRequest, RebalanceExecutor, RebalanceRisk, RebalanceRuntimeLimits,
        RebalanceTracker, Route, V12RebalanceParityAdapter, rebalance_base_units_to_decimal,
        rebalance_decimal_to_base_units_floor, route_candidates_from_capital,
    },
    resource_balances::{EvmGasBalanceSource, RESOURCE_BALANCE_INTERVAL, ResourceBalanceMonitor},
    state::{QuoteApplyResult, RuntimePhase, RuntimeState, TopOfBook},
    strategy_runtime::{
        CompiledStrategyDependencyIndex, FairLatestOnlySizingScheduler, HotPathDecisionOwner,
        StrategyDependencyFault, StrategyEvaluator,
    },
    supervision::{DependencyFaultClass, DependencyScope, RootSupervisorPolicy, SupervisorAction},
    switchback::{
        ESP_SWITCHBACK_BLOCK_DURATION_SECONDS, ESP_SWITCHBACK_END_UNIX_SECONDS,
        ESP_SWITCHBACK_EXPERIMENT_ID, ESP_SWITCHBACK_HASH_ALGORITHM, ESP_SWITCHBACK_PAIR_ID,
        ESP_SWITCHBACK_SEED_VERSION, ESP_SWITCHBACK_START_UNIX_SECONDS,
        validate_production_switchback,
    },
    telemetry::{
        ARBITRAGE_RESULT_KIND, ExecutionLatencyTelemetry, PRIMARY_BINANCE_ACCOUNT_ID,
        TelemetryHandle, TelemetryWriter, execution_lane_id,
    },
    wallet::{
        EvmJournalScope, EvmWallet, OPTIMISM_RPC_URL_ENV, ReviewedConsumedNonceCollision,
        TokenBalanceRequest, WALLET_JOURNAL_PATH_ENV, hydrate_chain_wallet,
    },
};
use clap::Parser;
use futures_util::future::try_join_all;
use rust_decimal::Decimal;
use std::str::FromStr;
use tokio::time::MissedTickBehavior;
use tracing_subscriber::{EnvFilter, fmt};

const ARBITRAGE_WALLET_JOURNAL_PATH_ENV: &str = "ARBITRAGE_WALLET_JOURNAL_PATH";
const ARBITRAGE_ARBITRUM_WALLET_JOURNAL_PATH_ENV: &str = "ARBITRAGE_ARBITRUM_WALLET_JOURNAL_PATH";
const ARBITRAGE_LINEA_WALLET_JOURNAL_PATH_ENV: &str = "ARBITRAGE_LINEA_WALLET_JOURNAL_PATH";
const ARBITRAGE_BINANCE_ORDER_JOURNAL_PATH_ENV: &str = "ARBITRAGE_BINANCE_ORDER_JOURNAL_PATH";
const LINEA_CHAIN_ID: u64 = 59_144;
const BINANCE_CLOCK_SYNC_INTERVAL: Duration = Duration::from_secs(60);
const DEX_REVERT_DIAGNOSTIC_CHANNEL_CAPACITY: usize = 32;
const MAXIMUM_CONCURRENT_ADAPTIVE_SIZING_WORKERS: usize = 4;
const REBALANCE_QUOTE_RETRY_INITIAL_DELAY: Duration = Duration::from_secs(5);
const REBALANCE_QUOTE_RETRY_MAX_DELAY: Duration = Duration::from_secs(60);
const REBALANCE_SUPERVISOR_INTERVAL: Duration = Duration::from_secs(1);
const ACROSS_RECONCILIATION_INTERVAL: Duration = Duration::from_secs(30);
const LINEA_DECOMMISSION_APPROVAL_SESSION_ID: &str = "linea-usdt-usdc-decommission-20260808";
const LINEA_DECOMMISSION_MAXIMUM_BASE_UNITS: u64 = 2_600_000_000;
const LINEA_DECOMMISSION_MAXIMUM_FEE_BASE_UNITS: u64 = 5_000_000;

fn esp_evm_journal_scope(chain_id: u64) -> EvmJournalScope {
    let network_id = format!("eip155:{chain_id}");
    EvmJournalScope {
        schema_version: EvmJournalScope::SCHEMA_VERSION,
        wallet_id: format!("{network_id}:evm-wallet:primary"),
        network_id,
        strategy_id: "strategy:arbitrum-usdc-esp".to_owned(),
    }
}

fn linea_evm_journal_scope() -> EvmJournalScope {
    let network_id = format!("eip155:{LINEA_CHAIN_ID}");
    EvmJournalScope {
        schema_version: EvmJournalScope::SCHEMA_VERSION,
        wallet_id: format!("{network_id}:evm-wallet:primary"),
        network_id,
        strategy_id: "strategy:linea-usdt-usdc".to_owned(),
    }
}

fn reviewed_rebalance_nonce_collision(
    wallet_owner: Address,
) -> anyhow::Result<ReviewedConsumedNonceCollision> {
    Ok(ReviewedConsumedNonceCollision {
        operation_id: "rebalance-1516-6b2792a1b1a18931:deposit".to_owned(),
        chain_id: 10,
        wallet: wallet_owner,
        nonce: 76,
        rejected_transaction_hash:
            "0x34462b8a2f930da06b5196db6a4111b07941c25ecbe4e0ddc388716a4d41a482".parse()?,
        purpose: "rebalance_bridge_to_binance".to_owned(),
        target: "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85".parse()?,
        native_value: U256::ZERO,
        calldata_hash: "0x510c0580cb373c283aec526a40c38da97f863cdcbfe61f6b0c4ceffde0938c0d"
            .parse()?,
        replacement_transaction_hash:
            "0x2d22c304a0e0ca98e0684145dbff8a62925cb36c33b0af891dc56b8248fb73b4".parse()?,
        replacement_target: "0x97ccdbea4632140639ad5ea9b944aa034eb15fd4".parse()?,
        replacement_native_value: U256::from(26_138_677_603_673_219_u64),
        replacement_block_number: 155_207_427,
        scope: EvmJournalScope {
            schema_version: EvmJournalScope::SCHEMA_VERSION,
            network_id: "optimism".to_owned(),
            wallet_id: format!("wallet:{wallet_owner:#x}"),
            strategy_id: "rebalance-world-chain-v12".to_owned(),
        },
    })
}

fn allowance_operation_id(symbol: &str) -> String {
    format!("rustarb-esp-full-live-{symbol}-max-allowance")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RebalanceExecutionTarget {
    Primary,
    ArbitrumEsp,
    ArbitrumArb,
    Linea,
}

impl RebalanceExecutionTarget {
    fn other(self) -> Self {
        match self {
            Self::Primary => Self::ArbitrumEsp,
            Self::ArbitrumEsp => Self::ArbitrumArb,
            Self::ArbitrumArb => Self::Linea,
            Self::Linea => Self::Primary,
        }
    }

    fn is_arbitrum(self) -> bool {
        matches!(self, Self::ArbitrumEsp | Self::ArbitrumArb)
    }

    fn is_direct_full_live(self) -> bool {
        self.is_arbitrum() || self == Self::Linea
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RebalanceDispatchOutcome {
    NoWork,
    Deferred,
    Submitted,
}

fn apply_rebalance_dispatch_outcome(
    lane_busy: &mut bool,
    next_target: &mut RebalanceExecutionTarget,
    attempted_target: RebalanceExecutionTarget,
    outcome: RebalanceDispatchOutcome,
) -> bool {
    if outcome != RebalanceDispatchOutcome::Submitted {
        return false;
    }
    *lane_busy = true;
    *next_target = attempted_target.other();
    true
}

enum RebalanceExecutorEvent {
    Recovery {
        target: RebalanceExecutionTarget,
        result: Result<RebalanceExecutionOperation, String>,
        active_operation_after: bool,
        blocked_token: Option<String>,
        recovery_started: Option<Box<RebalanceExecutionOperation>>,
        next_recovery: Option<Box<RebalanceExecutionOperation>>,
    },
    Execution {
        target: RebalanceExecutionTarget,
        result: Result<RebalanceExecutionOperation, String>,
        active_operation_after: bool,
        blocked_token: Option<String>,
    },
    AcrossReconciliationIdle {
        attempted: bool,
        error: Option<String>,
    },
}

enum RebalanceExecutorCommand {
    Execute {
        target: RebalanceExecutionTarget,
        request: Box<RebalanceExecutionRequest>,
    },
    ReconcileAcross,
}

fn rebalance_target(operation: &RebalanceExecutionOperation) -> RebalanceExecutionTarget {
    if operation
        .intent
        .scope
        .as_ref()
        .is_some_and(|scope| scope.network_id == "chain:59144")
    {
        return RebalanceExecutionTarget::Linea;
    }
    if operation
        .intent
        .scope
        .as_ref()
        .is_some_and(|scope| scope.network_id == "chain:42161")
    {
        if operation.intent.token_symbol == "ARB" {
            RebalanceExecutionTarget::ArbitrumArb
        } else {
            RebalanceExecutionTarget::ArbitrumEsp
        }
    } else {
        RebalanceExecutionTarget::Primary
    }
}

fn emit_rebalance_risk(telemetry: &TelemetryHandle, engine_id: &str, executor: &RebalanceExecutor) {
    match executor.rebalance_risk() {
        Ok(risk) => telemetry.emit(
            "rebalance_risk_snapshot",
            serde_json::json!({
                "engine_id": engine_id,
                "approval_session_id": executor
                    .approval_session_id()
                    .unwrap_or("unconfigured"),
                "transfer_count": risk.transfer_count,
                "active_transfer_count": risk.active_transfer_count,
                "failed_transfer_count": risk.failed_transfer_count,
                "token_a_debit": risk.token_a_debit.to_string(),
                "token_b_debit": risk.token_b_debit.to_string(),
                "token_a_maximum_fee": risk.token_a_maximum_fee.to_string(),
                "token_b_maximum_fee": risk.token_b_maximum_fee.to_string(),
                "additional_token_debit": risk.additional_token_debit
                    .iter()
                    .map(|(symbol, amount)| (symbol, amount.to_string()))
                    .collect::<BTreeMap<_, _>>(),
                "additional_token_maximum_fee": risk.additional_token_maximum_fee
                    .iter()
                    .map(|(symbol, amount)| (symbol, amount.to_string()))
                    .collect::<BTreeMap<_, _>>(),
                "first_started_at_unix_ms": risk.first_started_at_unix_ms,
                "outcome": "success",
            }),
        ),
        Err(error) => telemetry.emit(
            "rebalance_risk_snapshot",
            serde_json::json!({
                "engine_id": engine_id,
                "approval_session_id": executor
                    .approval_session_id()
                    .unwrap_or("unconfigured"),
                "outcome": "failed",
                "error": format!("{error:#}"),
            }),
        ),
    }
}

fn emit_rebalance_saga(
    telemetry: &TelemetryHandle,
    engine_id: &str,
    target: RebalanceExecutionTarget,
    result: &Result<RebalanceExecutionOperation, String>,
    executor: &RebalanceExecutor,
    started_at: Instant,
    recovered: bool,
) {
    if !target.is_arbitrum() {
        return;
    }
    let operation = result
        .as_ref()
        .ok()
        .or_else(|| executor.active_operation().ok().flatten())
        .or_else(|| executor.latest_rebalance_operation());
    let saga_duration_us = started_at.elapsed().as_micros();
    telemetry.emit(
        "rebalance_saga",
        serde_json::json!({
            "engine_id": engine_id,
            "strategy_id": "rebalance-arbitrum-usdc-esp",
            "approval_session_id": executor
                .approval_session_id()
                .unwrap_or("unconfigured"),
            "operation_id": operation.map(|operation| &operation.intent.operation_id),
            "token": operation.map(|operation| &operation.intent.token_symbol),
            "amount_base_units": operation.map(|operation| operation.intent.amount.to_string()),
            "maximum_fee_base_units": operation.and_then(|operation| {
                operation.intent.maximum_fee_base_units.as_deref()
            }),
            "direction": operation.map(|operation| format!("{:?}", operation.intent.direction)),
            "progress": operation.map(|operation| format!("{:?}", operation.progress)),
            "saga_duration_us": saga_duration_us,
            "recovered": recovered,
            "outcome": if result.is_ok() { "success" } else { "failed" },
            "error": result.as_ref().err(),
        }),
    );
}

fn log_compiled_graph(summary: Option<&CompiledGraphSummary>) {
    let Some(summary) = summary else {
        return;
    };
    tracing::info!(
        bundle_id = %summary.bundle_id,
        compatibility_projection_id = %summary.projection_id,
        domain_config_sha256 = %summary.fingerprint_sha256,
        account_count = summary.accounts,
        instrument_count = summary.instruments,
        network_count = summary.networks,
        wallet_count = summary.wallets,
        venue_asset_count = summary.venue_assets,
        economic_asset_count = summary.economic_assets,
        pool_count = summary.pools,
        strategy_count = summary.strategies,
        compiled_domain_bundle_bytes = summary.bundle_bytes,
        compiled_domain_load_validation_us = summary.load_validation_us,
        compiled_domain_rss_before_bytes = ?summary.rss_before_bytes,
        compiled_domain_rss_after_bytes = ?summary.rss_after_bytes,
        compiled_domain_rss_delta_bytes = ?summary.rss_delta_bytes,
        "compiled domain graph validated before network startup"
    );
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let process_started_at = Instant::now();
    load_dotenv()?;
    init_tracing();

    let cli = Cli::parse();
    // Only the long-lived process that owns a readiness probe may clear a
    // marker left in the Pod's shared emptyDir. Operator subcommands run in
    // the live container too; they must never change the owner's readiness.
    if command_owns_runtime_readiness(&cli.command) {
        let runtime_ready_path = runtime_ready_marker_path()?;
        let mut runtime_ready_marked = runtime_ready_path
            .as_ref()
            .is_some_and(|path| path.exists());
        sync_runtime_ready_marker(
            runtime_ready_path.as_deref(),
            &mut runtime_ready_marked,
            false,
        )?;
    }
    cli.config.validate()?;

    match cli.command {
        Command::CompileDomain { manifest, output } => {
            let fingerprint = compile_manifest_to_path(&manifest, &output)?;
            tracing::info!(
                manifest_path = %manifest.display(),
                output_path = %output.display(),
                domain_config_sha256 = %fingerprint,
                "compiled canonical multi-pair domain bundle"
            );
            Ok(())
        }
        Command::ReplayCapacity {
            artifact,
            frames_per_pair,
            target_cpu_class,
        } => {
            let report = arb_bot::capacity_replay::run_capacity_replay(
                artifact,
                frames_per_pair,
                target_cpu_class.as_deref(),
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&report)
                    .context("failed to serialize capacity capacity replay report")?
            );
            Ok(())
        }
        Command::LineaTransportPreflight {
            rpc_url,
            ws_url,
            maximum_http_p95_ms,
            maximum_ws_subscribe_ms,
            maximum_head_wait_ms,
        } => {
            linea_transport_preflight(
                &rpc_url,
                &ws_url,
                maximum_http_p95_ms,
                maximum_ws_subscribe_ms,
                maximum_head_wait_ms,
            )
            .await
        }
        Command::Run => {
            let domain_validation_started_at = Instant::now();
            let selection = load_compatibility_domain(
                &cli.config.domain_config_path,
                CompatibilityRole::LiveRuntime,
                true,
            )?;
            log_compiled_graph(selection.graph_summary.as_ref());
            let binance_runtime = selection.binance_runtime.map(Arc::new);
            let network_runtime = selection.network_runtime;
            let hot_path_runtime = selection.hot_path_runtime;
            let portfolio_runtime = selection.portfolio_runtime;
            let domain_config = Arc::new(selection.config);
            let bootstrap = BootstrapTiming {
                process_started_at,
                domain_validation_complete_at: Instant::now(),
                domain_load_us: domain_validation_started_at.elapsed().as_micros(),
            };
            run(
                cli.config,
                domain_config,
                binance_runtime,
                network_runtime,
                hot_path_runtime,
                portfolio_runtime,
                bootstrap,
            )
            .await
        }
        Command::CollectPrices => {
            let selection = load_compatibility_domain(
                &cli.config.domain_config_path,
                CompatibilityRole::PublicPriceCollector,
                true,
            )?;
            log_compiled_graph(selection.graph_summary.as_ref());
            let network_runtime = selection.network_runtime;
            let domain_config = Arc::new(selection.config);
            collect_prices(cli.config, domain_config, network_runtime).await
        }
        Command::Migrate => TelemetryWriter::new(&cli.config).migrate().await,
        Command::Check => {
            let selection = load_compatibility_domain(
                &cli.config.domain_config_path,
                CompatibilityRole::LiveRuntime,
                false,
            )?;
            log_compiled_graph(selection.graph_summary.as_ref());
            let domain_config = selection.config;
            tracing::info!(
                service = %cli.config.service_name,
                engine_id = %cli.config.engine_id,
                gcp_project_id = %cli.config.gcp_project_id,
                gcp_region = %cli.config.gcp_region,
                domain_snapshot_id = %domain_config.snapshot().snapshot_id,
                domain_config_sha256 = %domain_config.fingerprint_sha256(),
                domain_config_path = %domain_config.path().display(),
                pair_ids = ?domain_config.pair_ids(),
                binance_symbols = ?domain_config.binance_symbols(),
                telemetry_enabled = cli.config.clickhouse_enabled(),
                "configuration is valid"
            );
            Ok(())
        }
        Command::Hydrate => {
            let domain_config = LoadedDomainConfig::load(&cli.config.domain_config_path)?;
            hydrate(&domain_config).await
        }
        Command::BinanceAccount => {
            let domain_config = LoadedDomainConfig::load(&cli.config.domain_config_path)?;
            let symbols = domain_config.binance_symbols();
            ensure!(
                symbols.len() == 1,
                "Binance account check currently requires exactly one enabled symbol"
            );
            let mut client = BinanceAccountClient::from_env(&cli.config)?;
            let state = client.hydrate(&symbols[0]).await?;
            validate_binance_account(&state)?;
            log_binance_account(&state);
            Ok(())
        }
        Command::BinanceCapital => {
            let mut client = BinanceAccountClient::from_env(&cli.config)?;
            client.synchronize_clock().await?;
            let coins = client.all_coin_information().await?;
            let wld = select_capital_routes(&coins, "WLD", "WLD", "OPTIMISM")?;
            let usdc = select_capital_routes(&coins, "USDC", "WLD", "OPTIMISM")?;
            log_binance_capital(&wld);
            log_binance_capital(&usdc);
            Ok(())
        }
        Command::BinanceCapitalRecovery {
            coin,
            network,
            deposit_transaction_hash,
            withdraw_order_id,
        } => {
            binance_capital_recovery(
                &cli.config,
                &coin,
                &network,
                deposit_transaction_hash.as_deref(),
                withdraw_order_id.as_deref(),
            )
            .await
        }
        Command::BinanceRecentValidationOrders { limit } => {
            binance_recent_validation_orders(&cli.config, limit).await
        }
        Command::BinanceOrderRoundTrip {
            order_type,
            quote_usdc,
            price_deviation_bps,
            journal_path,
            live_confirmation,
        } => {
            let domain_config = LoadedDomainConfig::load(&cli.config.domain_config_path)?;
            let kind = BinanceCanaryKind::parse(&order_type)?;
            let quote_usdc =
                Decimal::from_str(&quote_usdc).context("--quote-usdc must be an exact decimal")?;
            let outcome = execute_order_round_trip(
                &cli.config,
                &domain_config,
                kind,
                quote_usdc,
                price_deviation_bps,
                journal_path,
                &live_confirmation,
            )
            .await?;
            tracing::info!(
                order_type = outcome.kind.label(),
                buy_order_id = outcome.buy.order.order_id,
                buy_client_order_id = %outcome.buy.order.client_order_id,
                sell_order_id = outcome.sell.order.order_id,
                sell_client_order_id = %outcome.sell.order.client_order_id,
                fallback_sell_order_id = outcome
                    .fallback_sell
                    .as_ref()
                    .map(|order| order.order.order_id),
                wld_received = %outcome.wld_received,
                wld_sell_quantity = %outcome.wld_sell_quantity,
                wld_before = %outcome.before.wld,
                wld_after = %outcome.after.wld,
                usdc_before = %outcome.before.usdc,
                usdc_after = %outcome.after.usdc,
                "Binance live validation evidence"
            );
            Ok(())
        }
        Command::BootstrapArbInventory {
            quote_usdc,
            journal_path,
            live_confirmation,
        } => {
            let quote_usdc =
                Decimal::from_str(&quote_usdc).context("--quote-usdc must be an exact decimal")?;
            bootstrap_arb_inventory(&cli.config, quote_usdc, journal_path, &live_confirmation)
                .await?;
            Ok(())
        }
        Command::BinanceWithdrawalStatus {
            coin,
            withdraw_order_id,
        } => binance_withdrawal_status(&cli.config, &coin, &withdraw_order_id).await,
        Command::BinanceTravelRuleWithdrawalStatus { tr_id } => {
            binance_travel_rule_withdrawal_status(&cli.config, tr_id).await
        }
        Command::ArbitrageReconcileCex {
            plan_id,
            order_journal_path,
            live_confirmation,
        } => arbitrage_reconcile_cex(
            &cli.config,
            &plan_id,
            order_journal_path,
            &live_confirmation,
        ),
        Command::ArbitrageEmitResult {
            plan_id,
            engine_id,
            live_confirmation,
        } => arbitrage_emit_result(&cli.config, &plan_id, engine_id, &live_confirmation).await,
        Command::ArbitrageRecordOperatorRecovery {
            plan_id,
            dex_transaction_hash,
            wallet_journal_path,
            order_journal_path,
            mode,
            maximum_quote_usdc,
            actor,
            live_confirmation,
        } => {
            arbitrage_record_operator_recovery(
                &cli.config,
                &plan_id,
                &dex_transaction_hash,
                wallet_journal_path,
                order_journal_path,
                &mode,
                &maximum_quote_usdc,
                &actor,
                &live_confirmation,
            )
            .await
        }
        Command::AcrossUsdcQuote {
            origin_chain_id,
            amount,
        } => across_usdc_quote(&cli.config, origin_chain_id, amount).await,
        Command::AcrossLineaCapitalQuote {
            asset,
            origin_chain_id,
            amount,
            wallet_address,
        } => {
            across_linea_capital_quote(
                &cli.config,
                &asset,
                origin_chain_id,
                amount,
                &wallet_address,
            )
            .await
        }
        Command::LineaReturnCapital {
            mode,
            asset,
            live_confirmation,
        } => linea_return_capital(&cli.config, &mode, &asset, &live_confirmation).await,
        Command::WalletAddress => {
            let wallet = EvmWallet::from_env()?;
            tracing::info!(address = %wallet.address(), "EVM test wallet loaded");
            Ok(())
        }
        Command::WalletHydrate => {
            let domain_config = LoadedDomainConfig::load(&cli.config.domain_config_path)?;
            wallet_hydrate(&domain_config).await
        }
        Command::UniswapRoundTrip {
            protocol,
            amount_usdc_base_units,
            slippage_bps,
            additional_gas,
            confirmation_timeout_seconds,
            live_confirmation,
        } => {
            let domain_config = LoadedDomainConfig::load(&cli.config.domain_config_path)?;
            let protocol = match protocol.as_str() {
                "v3" => DexProtocol::UniswapV3,
                "v4" => DexProtocol::UniswapV4,
                _ => bail!("--protocol must be v3 or v4"),
            };
            let outcome = execute_round_trip(
                &domain_config,
                protocol,
                amount_usdc_base_units,
                slippage_bps,
                additional_gas,
                Duration::from_secs(confirmation_timeout_seconds),
                &live_confirmation,
            )
            .await?;
            tracing::info!(
                protocol = outcome.protocol.label(),
                wallet = %outcome.wallet,
                amount_usdc_in = %outcome.amount_usdc_in,
                amount_wld_received = %outcome.amount_wld_received,
                amount_usdc_received = %outcome.amount_usdc_received,
                buy_transaction_hash = %outcome.buy.transaction_hash,
                sell_transaction_hash = %outcome.sell.transaction_hash,
                usdc_before = %outcome.before.usdc,
                usdc_after = %outcome.after.usdc,
                wld_before = %outcome.before.wld,
                wld_after = %outcome.after.wld,
                "Uniswap live validation evidence"
            );
            Ok(())
        }
        Command::UniswapRecoverySell {
            protocol,
            amount_wld_base_units,
            slippage_bps,
            additional_gas,
            confirmation_timeout_seconds,
            live_confirmation,
        } => {
            let domain_config = LoadedDomainConfig::load(&cli.config.domain_config_path)?;
            let protocol = match protocol.as_str() {
                "v3" => DexProtocol::UniswapV3,
                "v4" => DexProtocol::UniswapV4,
                _ => bail!("--protocol must be v3 or v4"),
            };
            let outcome = execute_recovery_sell(
                &domain_config,
                protocol,
                U256::from(amount_wld_base_units),
                slippage_bps,
                additional_gas,
                Duration::from_secs(confirmation_timeout_seconds),
                &live_confirmation,
            )
            .await?;
            tracing::info!(
                protocol = outcome.protocol.label(),
                wallet = %outcome.wallet,
                amount_wld_in = %outcome.amount_wld_in,
                amount_usdc_received = %outcome.amount_usdc_received,
                transaction_hash = %outcome.sell.transaction_hash,
                "Uniswap recovery sell evidence"
            );
            Ok(())
        }
    }
}

fn command_owns_runtime_readiness(command: &Command) -> bool {
    matches!(command, Command::Run | Command::CollectPrices)
}

async fn linea_transport_preflight(
    rpc_url: &str,
    ws_url: &str,
    maximum_http_p95_ms: u64,
    maximum_ws_subscribe_ms: u64,
    maximum_head_wait_ms: u64,
) -> anyhow::Result<()> {
    ensure!(
        maximum_http_p95_ms > 0 && maximum_ws_subscribe_ms > 0 && maximum_head_wait_ms > 0,
        "Linea transport latency limits must be positive"
    );
    let rpc = JsonRpcClient::new(rpc_url)?;
    let mut http_samples_us = Vec::with_capacity(10);
    for _ in 0..5 {
        let started = Instant::now();
        ensure!(
            rpc.chain_id().await? == LINEA_CHAIN_ID,
            "Linea RPC chain id mismatch"
        );
        http_samples_us.push(started.elapsed().as_micros());
        let started = Instant::now();
        ensure!(
            rpc.gas_price().await? > 0,
            "Linea RPC returned a zero gas price"
        );
        http_samples_us.push(started.elapsed().as_micros());
    }
    http_samples_us.sort_unstable();
    let http_p95_us = http_samples_us[http_samples_us.len() - 1];
    ensure!(
        http_p95_us <= u128::from(maximum_http_p95_ms) * 1_000,
        "Linea HTTP p95 exceeds the deployment latency limit"
    );

    let pool: Address = "0x6e9ad0b8a41e2c148e7b0385d3ecbfdb8a216a9b".parse()?;
    let filter = EthLogFilter::new(vec![pool], vec![])?;
    let ws_started = Instant::now();
    let (mut stream, ws_connect_attempts) = tokio::time::timeout(
        Duration::from_millis(maximum_ws_subscribe_ms),
        connect_linea_transport_stream(ws_url, &filter),
    )
    .await
    .context("Linea WSS subscription retry budget timed out")??;
    let ws_subscribe_us = ws_started.elapsed().as_micros();
    let head_started = Instant::now();
    let event = tokio::time::timeout(
        Duration::from_millis(maximum_head_wait_ms),
        stream.receiver.recv(),
    )
    .await
    .context("Linea WSS first head timed out")?
    .context("Linea WSS ended before its first head")?;
    ensure!(
        matches!(event, DexStreamEvent::Head { .. }),
        "Linea WSS emitted a log before a canonical head"
    );
    let first_head_us = head_started.elapsed().as_micros();
    stream.task.abort();

    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({
            "schema_version": 1,
            "chain_id": LINEA_CHAIN_ID,
            "http_sample_count": http_samples_us.len(),
            "http_p95_us": http_p95_us,
            "maximum_http_p95_ms": maximum_http_p95_ms,
            "ws_subscribe_us": ws_subscribe_us,
            "ws_connect_attempts": ws_connect_attempts,
            "maximum_ws_subscribe_ms": maximum_ws_subscribe_ms,
            "first_head_us": first_head_us,
            "maximum_head_wait_ms": maximum_head_wait_ms,
            "network_mutations": 0,
            "gate": "pass"
        }))?
    );
    Ok(())
}

const LINEA_TRANSPORT_SUBSCRIPTION_ATTEMPTS: u32 = 3;

async fn connect_linea_transport_stream(
    ws_url: &str,
    filter: &EthLogFilter,
) -> anyhow::Result<(AlchemyDexStream, u32)> {
    let mut last_error = None;
    for attempt in 1..=LINEA_TRANSPORT_SUBSCRIPTION_ATTEMPTS {
        match connect_dex_stream(ws_url, std::slice::from_ref(filter), 16).await {
            Ok(stream) => return Ok((stream, attempt)),
            Err(error) => last_error = Some(error),
        }
        if attempt < LINEA_TRANSPORT_SUBSCRIPTION_ATTEMPTS {
            tokio::time::sleep(linea_transport_subscription_retry_delay(attempt)).await;
        }
    }
    Err(last_error.expect("at least one Linea subscription attempt ran")).with_context(|| {
        format!(
            "Linea WSS subscription failed after {LINEA_TRANSPORT_SUBSCRIPTION_ATTEMPTS} attempts"
        )
    })
}

fn linea_transport_subscription_retry_delay(completed_attempts: u32) -> Duration {
    if completed_attempts <= 1 {
        Duration::from_millis(250)
    } else {
        Duration::from_millis(500)
    }
}

async fn arbitrage_emit_result(
    config: &config::AppConfig,
    plan_id: &str,
    engine_id: Option<String>,
    live_confirmation: &str,
) -> anyhow::Result<()> {
    ensure!(
        live_confirmation == "EMIT_LIVE_ARBITRAGE_RESULT",
        "live arbitrage result emission requires ARBITRAGE_EMIT_RESULT_CONFIRMATION=EMIT_LIVE_ARBITRAGE_RESULT"
    );
    let coordinator = PaperTradeCoordinator::open(&config.arbitrage_trade_journal_path)?;
    let operation = coordinator
        .operation(plan_id)
        .with_context(|| format!("unknown arbitrage plan {plan_id}"))?;
    ensure!(
        matches!(
            operation.stage,
            TradeStage::BalancedProfit | TradeStage::BalancedLoss
        ),
        "arbitrage plan is not terminal balanced"
    );
    let engine_id = engine_id.unwrap_or_else(|| config.engine_id.clone());
    let mut payload = operation.result_telemetry_payload(&engine_id)?;
    let object = payload
        .as_object_mut()
        .context("live result payload is not an object")?;
    object.insert("simulation".to_owned(), serde_json::Value::Bool(false));
    object.insert(
        "includes_binance_fee".to_owned(),
        serde_json::Value::Bool(true),
    );
    object.insert("includes_gas".to_owned(), serde_json::Value::Bool(true));
    object.insert(
        "comparable_to_live".to_owned(),
        serde_json::Value::Bool(true),
    );
    TelemetryWriter::new(config)
        .emit_once(ARBITRAGE_RESULT_KIND, payload)
        .await?;
    tracing::info!(
        plan_id,
        engine_id,
        "terminal live arbitrage result emitted from trade journal"
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn arbitrage_record_operator_recovery(
    config: &config::AppConfig,
    plan_id: &str,
    dex_transaction_hash: &str,
    wallet_journal_path: PathBuf,
    order_journal_path: PathBuf,
    mode: &str,
    maximum_quote_usdc: &str,
    actor: &str,
    live_confirmation: &str,
) -> anyhow::Result<()> {
    ensure!(
        matches!(mode, "dry-run" | "execute"),
        "operator recovery --mode must be dry-run or execute"
    );
    if mode == "execute" {
        ensure!(
            live_confirmation == "RECORD_LIVE_ARBITRAGE_OPERATOR_RECOVERY",
            "operator recovery execute requires ARBITRAGE_OPERATOR_RECOVERY_CONFIRMATION=RECORD_LIVE_ARBITRAGE_OPERATOR_RECOVERY"
        );
        ensure!(
            config.arbitrage_entry_stop_file.exists(),
            "operator recovery execute requires the arbitrage entry-stop file"
        );
    }
    let maximum_quote_usdc = Decimal::from_str(maximum_quote_usdc)
        .context("--maximum-quote-usdc must be an exact decimal")?;
    ensure!(
        maximum_quote_usdc > Decimal::ZERO,
        "operator recovery maximum quote must be positive"
    );
    let expected_transaction_hash = dex_transaction_hash
        .parse::<alloy_primitives::B256>()
        .context("--dex-transaction-hash is invalid")?;

    let selection = load_compatibility_domain(
        &config.domain_config_path,
        CompatibilityRole::LiveRuntime,
        false,
    )?;
    let domain_config = selection.config;
    let mut coordinator = PaperTradeCoordinator::open(&config.arbitrage_trade_journal_path)?;
    let operation = coordinator
        .operation(plan_id)
        .with_context(|| format!("unknown arbitrage plan {plan_id}"))?
        .clone();
    ensure!(
        operation.stage.terminal()
            && operation.dex_dispatched
            && !operation.cex_dispatched
            && operation.recovery_results.is_empty()
            && operation.operator_recovery.is_none()
            && operation.dex_result.as_ref().is_some_and(|result| {
                result.status == LegStatus::Failed && result.venue_reference == "dex:expired-plan"
            }),
        "arbitrage plan is not the historical false-terminal expired-plan shape"
    );
    let pair = domain_config
        .snapshot()
        .pairs
        .iter()
        .find(|pair| pair.id == operation.intent.pair_id)
        .context("operator recovery pair is absent from the live domain")?;
    let scope = operation
        .intent
        .journal_scope
        .as_ref()
        .context("operator recovery trade has no journal scope")?;
    ensure!(
        scope.chain_id == pair.chain.chain_id && scope.symbol == pair.binance.symbol,
        "operator recovery journal scope differs from the live pair"
    );

    let endpoint = std::env::var(&pair.chain.rpc_url_env).with_context(|| {
        format!(
            "required environment variable {} is not set",
            pair.chain.rpc_url_env
        )
    })?;
    let wallet = EvmWallet::from_env()?;
    let mut dex_executor = DexExecutor::hydrate(
        JsonRpcClient::new(endpoint)?,
        wallet,
        pair.chain.chain_id,
        wallet_journal_path,
    )
    .await?;
    dex_executor.set_journal_scope(EvmJournalScope {
        schema_version: EvmJournalScope::SCHEMA_VERSION,
        network_id: scope.network_id.clone(),
        wallet_id: scope.wallet_id.clone(),
        strategy_id: scope.strategy_id.clone(),
    })?;
    let mut request = operation
        .intent
        .dex_plan
        .as_ref()
        .context("operator recovery trade has no DEX plan")?
        .execution_request(operation.intent.dex_operation_id.clone())?;
    request.reconciliation_only = true;
    let dex_outcome = dex_executor.execute_exact_input(request).await?;
    ensure!(
        dex_outcome.transaction_hash == expected_transaction_hash,
        "journaled DEX receipt hash differs from --dex-transaction-hash"
    );
    let admission = operation
        .intent
        .admission
        .as_ref()
        .context("operator recovery trade has no admission accounting")?;
    let gas = if admission.gas_conversion_price_token_a.is_zero() {
        0
    } else {
        native_gas_to_token_a_base_units(
            dex_outcome.gas_used,
            dex_outcome.effective_gas_price,
            dex_outcome.l1_fee,
            admission.gas_conversion_price_token_a,
            pair.token_a.decimals,
        )?
    };
    let dex_result = dex_leg_result(operation.intent.direction, dex_outcome, gas)?;

    let recovery_target = dex_result.token_b_delta_base_units.saturating_neg();
    ensure!(
        recovery_target != 0,
        "operator recovery DEX receipt has no token-B exposure"
    );
    let recovery_quantity =
        decimal_from_base_units(recovery_target.unsigned_abs(), pair.token_b.decimals)?;
    let forecast_quote = operator_recovery_top_quote(
        config,
        &pair.binance.symbol,
        recovery_target,
        recovery_quantity,
    )
    .await?;
    if recovery_target > 0 {
        ensure!(
            forecast_quote <= maximum_quote_usdc,
            "operator recovery current-ask forecast exceeds the hard quote cap"
        );
    }

    let mut account_client = BinanceAccountClient::from_env(config)?;
    let clock = account_client.synchronize_clock_observed().await?;
    let user_data_stream = UserDataStream::connect(config, clock.offset_ms).await?;
    let binance_api = user_data_stream.api();
    ensure!(
        query_operator_order(
            &binance_api,
            &pair.binance.symbol,
            &operation.intent.cex_client_order_id,
        )
        .await?
        .is_none(),
        "primary Binance client id exists; operator recovery cannot assume zero primary fill"
    );
    let mut order_journal = Some(BinanceOrderJournal::open(&order_journal_path)?);
    let resolution = resolve_operator_recovery_order(
        &binance_api,
        order_journal
            .as_mut()
            .expect("operator order journal is open"),
        &operation.intent.cex_client_order_id,
        &pair.binance.symbol,
        scope,
        recovery_target,
        recovery_quantity,
        mode == "execute",
    )
    .await?;
    let (recovery_client_id, order) = match resolution {
        OperatorOrderResolution::Filled {
            client_order_id,
            order,
        } => (client_order_id, *order),
        OperatorOrderResolution::Place { client_order_id } if mode == "dry-run" => {
            tracing::info!(
                plan_id,
                recovery_client_order_id = %client_order_id,
                recovery_target_token_b_base_units = recovery_target,
                recovery_quantity = %recovery_quantity,
                forecast_quote_usdc = %forecast_quote,
                maximum_quote_usdc = %maximum_quote_usdc,
                "operator recovery dry-run proved the primary and prior deterministic recovery ids absent; execute would place one MARKET order"
            );
            return Ok(());
        }
        OperatorOrderResolution::Place { client_order_id } => {
            let request_kind = if recovery_target > 0 {
                BinanceOrderRequestKind::MarketBuyQuantity {
                    quantity: recovery_quantity,
                }
            } else {
                BinanceOrderRequestKind::MarketSell {
                    quantity: recovery_quantity,
                }
            };
            let request = BinanceOrderRequest {
                operation_id: client_order_id.clone(),
                client_order_id: client_order_id.clone(),
                symbol: pair.binance.symbol.clone(),
                kind: request_kind,
                latency_origin: None,
            };
            request.validate()?;
            drop(order_journal.take());
            let service = BinanceExecutionService::spawn_scoped(
                binance_api,
                order_journal_path,
                1,
                BinanceOrderJournalScope {
                    schema_version: BinanceOrderJournalScope::SCHEMA_VERSION,
                    account_id: scope.account_id.clone(),
                    strategy_id: scope.strategy_id.clone(),
                },
            )
            .await?;
            let outcome = service
                .execute(request)
                .await
                .map_err(|error| anyhow::anyhow!(error))?;
            drop(service);
            (client_order_id, outcome.order)
        }
    };
    ensure!(
        order.status == "FILLED" && order.client_order_id == recovery_client_id,
        "operator recovery Binance order is not the deterministic filled order"
    );
    if order.cummulative_quote_qty > maximum_quote_usdc {
        tracing::error!(
            plan_id,
            actual_quote_usdc = %order.cummulative_quote_qty,
            maximum_quote_usdc = %maximum_quote_usdc,
            "filled operator recovery exceeded its pre-placement quote cap; the known fill remains authoritative and will still be journaled"
        );
    }
    let recovery_result = binance_leg_result(
        &order,
        &pair.binance.base_asset,
        pair.token_b.decimals,
        &pair.binance.quote_asset,
        pair.token_a.decimals,
        pair.binance
            .commission_asset
            .as_deref()
            .map(|asset| CommissionAssetValuation {
                asset,
                price_in_token_a: None,
            }),
    )?;
    ensure!(
        recovery_result.token_b_delta_base_units
            == dex_result.token_b_delta_base_units.saturating_neg(),
        "operator recovery Binance fill does not exactly neutralize the DEX token-B delta"
    );
    let recovered_at_unix_ms = order.transact_time.unwrap_or(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system time is before Unix epoch")?
            .as_millis()
            .try_into()
            .context("operator recovery timestamp exceeds u64")?,
    );
    let evidence = OperatorRecoveryEvidence {
        actor: actor.to_owned(),
        recovered_at_unix_ms,
        dex_transaction_hash: format!("{expected_transaction_hash:#x}"),
        binance_order_id: order.order_id,
        binance_client_order_id: recovery_client_id.clone(),
    };
    tracing::info!(
        plan_id,
        mode,
        dex_transaction_hash = %expected_transaction_hash,
        dex_token_b_delta_base_units = dex_result.token_b_delta_base_units,
        dex_token_a_delta_base_units = dex_result.token_a_delta_base_units,
        dex_gas_cost_token_a_base_units = dex_result.gas_cost_token_a_base_units,
        binance_order_id = order.order_id,
        binance_client_order_id = %recovery_client_id,
        binance_token_b_delta_base_units = recovery_result.token_b_delta_base_units,
        binance_token_a_delta_base_units = recovery_result.token_a_delta_base_units,
        binance_quote_spend = %order.cummulative_quote_qty,
        maximum_quote_usdc = %maximum_quote_usdc,
        "operator arbitrage recovery evidence validated"
    );
    drop(order_journal);
    drop(user_data_stream);
    drop(dex_executor);

    if mode == "execute" {
        coordinator.record_operator_recovery(plan_id, dex_result, recovery_result, evidence)?;
        tracing::info!(
            plan_id,
            "operator arbitrage recovery correction durably recorded"
        );
    }
    Ok(())
}

async fn query_operator_order(
    api: &arb_bot::binance::user_data::MultiplexedBinanceWsApi,
    symbol: &str,
    client_order_id: &str,
) -> anyhow::Result<Option<OrderResult>> {
    match api.query_order(symbol, client_order_id).await {
        Ok(order) => Ok(Some(order)),
        Err(WsApiError::Rejected {
            status: _,
            code: -2013,
            message: _,
        }) => Ok(None),
        Err(error) => Err(anyhow::anyhow!(error))
            .with_context(|| format!("Binance order.status is inconclusive for {client_order_id}")),
    }
}

enum OperatorOrderResolution {
    Filled {
        client_order_id: String,
        order: Box<OrderResult>,
    },
    Place {
        client_order_id: String,
    },
}

#[allow(clippy::too_many_arguments)]
async fn resolve_operator_recovery_order(
    api: &arb_bot::binance::user_data::MultiplexedBinanceWsApi,
    journal: &mut BinanceOrderJournal,
    primary_client_order_id: &str,
    symbol: &str,
    scope: &TradeJournalScope,
    recovery_target: i128,
    quantity: Decimal,
    execute: bool,
) -> anyhow::Result<OperatorOrderResolution> {
    let side = if recovery_target > 0 { "BUY" } else { "SELL" };
    for attempt in 1..=MAX_RECOVERY_ATTEMPTS {
        let client_order_id = recovery_client_order_id(primary_client_order_id, attempt)?;
        let intent = BinanceOrderIntent {
            scope: Some(BinanceOrderJournalScope {
                schema_version: BinanceOrderJournalScope::SCHEMA_VERSION,
                account_id: scope.account_id.clone(),
                strategy_id: scope.strategy_id.clone(),
            }),
            operation_id: client_order_id.clone(),
            client_order_id: client_order_id.clone(),
            symbol: symbol.to_owned(),
            side: side.to_owned(),
            order_type: "MARKET".to_owned(),
            quantity: Some(quantity.normalize().to_string()),
            quote_order_quantity: None,
            limit_price: None,
        };
        let existing = journal.operations().get(&client_order_id).cloned();
        if let Some(existing) = &existing {
            ensure!(
                existing.intent == intent,
                "operator recovery Binance journal intent changed"
            );
        }
        let discovered = query_operator_order(api, symbol, &client_order_id).await?;
        match (discovered, existing) {
            (Some(order), None) => {
                validate_operator_recovery_order(&intent, &order)?;
                if execute {
                    journal.record_discovered_terminal(intent, order.clone())?;
                }
                return Ok(OperatorOrderResolution::Filled {
                    client_order_id,
                    order: Box::new(order),
                });
            }
            (Some(order), Some(existing)) => {
                validate_operator_recovery_order(&intent, &order)?;
                match existing.progress {
                    BinanceOrderProgress::Terminal {
                        order: Some(journaled),
                        ..
                    } => ensure!(
                        journaled == order,
                        "Binance venue and order journal disagree on the recovery order"
                    ),
                    BinanceOrderProgress::Terminal { order: None, .. } => {}
                    BinanceOrderProgress::Rejected { .. } => anyhow::bail!(
                        "Binance venue has an order whose journal entry is terminal rejected"
                    ),
                    BinanceOrderProgress::IntentRecorded
                    | BinanceOrderProgress::Submitted { .. }
                    | BinanceOrderProgress::OutcomeUnknown { .. } => {
                        if execute {
                            journal.advance(
                                &client_order_id,
                                BinanceOrderProgress::Terminal {
                                    order_id: order.order_id,
                                    status: order.status.clone(),
                                    executed_quantity: order.executed_qty.to_string(),
                                    cumulative_quote_quantity: order
                                        .cummulative_quote_qty
                                        .to_string(),
                                    order: Some(order.clone()),
                                },
                            )?;
                        }
                    }
                }
                return Ok(OperatorOrderResolution::Filled {
                    client_order_id,
                    order: Box::new(order),
                });
            }
            (None, None) => {
                return Ok(OperatorOrderResolution::Place { client_order_id });
            }
            (None, Some(existing)) => match existing.progress {
                BinanceOrderProgress::Rejected { code: -2013, .. } => continue,
                BinanceOrderProgress::IntentRecorded
                | BinanceOrderProgress::OutcomeUnknown { .. } => {
                    if execute {
                        journal.advance(
                            &client_order_id,
                            BinanceOrderProgress::Rejected {
                                status: 400,
                                code: -2013,
                                reason:
                                    "operator order.status proved deterministic recovery absent"
                                        .to_owned(),
                            },
                        )?;
                    }
                    continue;
                }
                BinanceOrderProgress::Submitted { .. } => anyhow::bail!(
                    "submitted Binance recovery is absent from order.status; outcome remains unknown"
                ),
                BinanceOrderProgress::Terminal { .. } => anyhow::bail!(
                    "Binance order journal claims a recovery order that order.status reports absent"
                ),
                BinanceOrderProgress::Rejected { .. } => {
                    anyhow::bail!("operator recovery journal contains a non-absence rejection")
                }
            },
        }
    }
    anyhow::bail!("operator recovery exhausted all deterministic Binance attempts")
}

fn validate_operator_recovery_order(
    intent: &BinanceOrderIntent,
    order: &OrderResult,
) -> anyhow::Result<()> {
    ensure!(
        order.client_order_id == intent.client_order_id
            && order.symbol == intent.symbol
            && order.side == intent.side
            && order.order_type == intent.order_type
            && Some(order.orig_qty.normalize().to_string()) == intent.quantity,
        "discovered Binance recovery order differs from the immutable request"
    );
    Ok(())
}

async fn operator_recovery_top_quote(
    config: &config::AppConfig,
    symbol: &str,
    recovery_target_base_units: i128,
    quantity: Decimal,
) -> anyhow::Result<Decimal> {
    let endpoint = format!(
        "{}/api/v3/ticker/bookTicker",
        config.binance_rest_base_url.trim_end_matches('/')
    );
    let payload = reqwest::Client::new()
        .get(endpoint)
        .query(&[("symbol", symbol)])
        .send()
        .await
        .context("operator recovery Binance top request failed")?
        .error_for_status()
        .context("operator recovery Binance top request was rejected")?
        .json::<serde_json::Value>()
        .await
        .context("operator recovery Binance top response is invalid JSON")?;
    let field = if recovery_target_base_units > 0 {
        "askPrice"
    } else {
        "bidPrice"
    };
    let price = payload[field]
        .as_str()
        .with_context(|| format!("operator recovery Binance top omitted {field}"))?
        .parse::<Decimal>()
        .with_context(|| format!("operator recovery Binance {field} is not an exact decimal"))?;
    ensure!(
        price > Decimal::ZERO,
        "operator recovery Binance top is zero"
    );
    price
        .checked_mul(quantity)
        .context("operator recovery top quote overflow")
}

fn arbitrage_reconcile_cex(
    config: &config::AppConfig,
    plan_id: &str,
    order_journal_path: PathBuf,
    live_confirmation: &str,
) -> anyhow::Result<()> {
    ensure!(
        live_confirmation == "RECONCILE_LIVE_ARBITRAGE_CEX",
        "live arbitrage CEX reconciliation requires ARBITRAGE_RECONCILE_CONFIRMATION=RECONCILE_LIVE_ARBITRAGE_CEX"
    );
    let domain_config = LoadedDomainConfig::load(&config.domain_config_path)?;
    let execution_pairs = domain_config
        .snapshot()
        .pairs
        .iter()
        .filter(|pair| pair.execution_enabled)
        .collect::<Vec<_>>();
    ensure!(
        execution_pairs.len() == 1,
        "arbitrage CEX reconciliation requires exactly one execution-enabled pair"
    );
    let pair = execution_pairs[0];

    let mut coordinator = PaperTradeCoordinator::open(&config.arbitrage_trade_journal_path)?;
    let operation = coordinator
        .operation(plan_id)
        .with_context(|| format!("unknown arbitrage plan {plan_id}"))?
        .clone();
    ensure!(
        operation.stage == TradeStage::UnknownExposure,
        "arbitrage plan is not waiting for unknown-outcome reconciliation"
    );
    ensure!(
        operation.intent.pair_id == pair.id,
        "arbitrage plan pair does not match the execution-enabled domain pair"
    );
    ensure!(
        operation
            .cex_result
            .as_ref()
            .is_some_and(|result| result.status == LegStatus::Unknown),
        "arbitrage plan CEX leg is not unknown"
    );

    let order_journal = BinanceOrderJournal::open(order_journal_path)?;
    let order_operation = order_journal
        .operations()
        .get(&operation.intent.cex_client_order_id)
        .with_context(|| {
            format!(
                "Binance order journal is missing {}",
                operation.intent.cex_client_order_id
            )
        })?;
    ensure!(
        order_operation.intent.symbol == pair.binance.symbol,
        "Binance order symbol does not match domain pair"
    );
    let BinanceOrderProgress::Terminal {
        order_id,
        status,
        order: Some(order),
        ..
    } = &order_operation.progress
    else {
        anyhow::bail!("Binance order is not terminal with full order details in the journal");
    };
    ensure!(
        order.client_order_id == operation.intent.cex_client_order_id,
        "journaled Binance order client id does not match the arbitrage intent"
    );

    let result = binance_leg_result(
        order,
        &pair.binance.base_asset,
        pair.token_b.decimals,
        &pair.binance.quote_asset,
        pair.token_a.decimals,
        pair.binance
            .commission_asset
            .as_deref()
            .map(|asset| CommissionAssetValuation {
                asset,
                price_in_token_a: None,
            }),
    )?;
    coordinator.reconcile_unknown(plan_id, LegRole::Cex, result.clone())?;
    tracing::info!(
        plan_id,
        client_order_id = %operation.intent.cex_client_order_id,
        order_id,
        status,
        token_b_delta_base_units = result.token_b_delta_base_units,
        token_a_delta_base_units = result.token_a_delta_base_units,
        venue_reference = %result.venue_reference,
        "arbitrage CEX unknown exposure reconciled from Binance order journal"
    );
    Ok(())
}

async fn across_usdc_quote(
    config: &config::AppConfig,
    origin_chain_id: u64,
    amount: u128,
) -> anyhow::Result<()> {
    ensure!(
        amount > 0 && amount <= 100_000_000,
        "Across validation quote must be between 1 base unit and 100 USDC"
    );
    let (destination_chain_id, input_token, output_token) = match origin_chain_id {
        OPTIMISM_CHAIN_ID => (WORLD_CHAIN_CHAIN_ID, OPTIMISM_USDC, WORLD_CHAIN_USDC),
        WORLD_CHAIN_CHAIN_ID => (OPTIMISM_CHAIN_ID, WORLD_CHAIN_USDC, OPTIMISM_USDC),
        _ => bail!("Across validation only permits Optimism and World Chain"),
    };
    let wallet = EvmWallet::from_env()?;
    let request = AcrossQuoteRequest {
        origin_chain_id,
        destination_chain_id,
        input_token,
        output_token,
        amount,
        depositor: wallet.address(),
        recipient: wallet.address(),
    };
    let quote = AcrossClient::new(config)?.quote(&request).await?;
    validate_quote(&request, &quote)?;
    tracing::info!(
        quote_id = %quote.id,
        origin_chain_id,
        destination_chain_id,
        input_amount = %quote.input_amount,
        expected_output_amount = %quote.expected_output_amount,
        min_output_amount = %quote.min_output_amount,
        fee_amount = %quote.fees.total.amount,
        expected_fill_time_seconds = quote.expected_fill_time,
        quote_expiry_timestamp = quote.quote_expiry_timestamp,
        approval_transactions = quote.approval_txns.len(),
        swap_target = %quote.swap_tx.to,
        "public unauthenticated Across quote validated"
    );
    Ok(())
}

async fn across_linea_capital_quote(
    config: &config::AppConfig,
    asset: &str,
    origin_chain_id: u64,
    amount: u128,
    wallet_address: &str,
) -> anyhow::Result<()> {
    ensure!(
        amount > 0 && amount <= 10_000_000_000,
        "Across Linea capital quote must be between 1 base unit and 10,000 tokens"
    );
    let asset = asset.to_ascii_uppercase();
    ensure!(
        asset == "USDC" || asset == "USDT",
        "asset must be USDC or USDT"
    );
    let destination_chain_id = match origin_chain_id {
        OPTIMISM_CHAIN_ID => LINEA_CHAIN_ID,
        LINEA_CHAIN_ID => OPTIMISM_CHAIN_ID,
        _ => bail!("Across Linea capital quote only permits Optimism and Linea"),
    };
    let token = |chain_id| match (asset.as_str(), chain_id) {
        ("USDC", OPTIMISM_CHAIN_ID) => Ok(OPTIMISM_USDC),
        ("USDC", LINEA_CHAIN_ID) => Ok(LINEA_USDC),
        ("USDT", OPTIMISM_CHAIN_ID) => Ok(OPTIMISM_USDT),
        ("USDT", LINEA_CHAIN_ID) => Ok(LINEA_USDT),
        _ => bail!("unsupported Across Linea capital asset or chain"),
    };
    let wallet = wallet_address
        .parse::<Address>()
        .context("invalid public wallet address")?;
    ensure!(wallet != Address::ZERO, "public wallet address is zero");
    let request = AcrossQuoteRequest {
        origin_chain_id,
        destination_chain_id,
        input_token: token(origin_chain_id)?,
        output_token: token(destination_chain_id)?,
        amount,
        depositor: wallet,
        recipient: wallet,
    };
    let quote = AcrossClient::new(config)?.quote(&request).await?;
    validate_quote(&request, &quote)?;
    tracing::info!(
        quote_id = %quote.id,
        asset,
        origin_chain_id,
        destination_chain_id,
        input_amount = %quote.input_amount,
        expected_output_amount = %quote.expected_output_amount,
        min_output_amount = %quote.min_output_amount,
        fee_amount = %quote.fees.total.amount,
        expected_fill_time_seconds = quote.expected_fill_time,
        quote_expiry_timestamp = quote.quote_expiry_timestamp,
        approval_transactions = quote.approval_txns.len(),
        swap_target = %quote.swap_tx.to,
        "Across V4 Linea capital quote validated"
    );
    Ok(())
}

fn linea_decommission_policy(
    portfolio: &CompiledPortfolioRuntimePlan,
) -> anyhow::Result<CompiledCapitalPolicy> {
    let mut policy = portfolio
        .capital_policy
        .clone()
        .context("Linea decommission requires the compiled production capital policy")?;
    let economic_asset = |symbol: &str| -> anyhow::Result<EconomicAssetId> {
        portfolio
            .assets
            .iter()
            .find(|asset| asset.symbol == symbol)
            .map(|asset| asset.economic_asset_id.clone())
            .with_context(|| format!("compiled portfolio has no {symbol} economic asset"))
    };
    let token_policy = |symbol: &str| -> anyhow::Result<CompiledCapitalTokenPolicy> {
        Ok(CompiledCapitalTokenPolicy {
            economic_asset_id: economic_asset(symbol)?,
            maximum_debit: U256::from(LINEA_DECOMMISSION_MAXIMUM_BASE_UNITS),
            maximum_fee: U256::from(LINEA_DECOMMISSION_MAXIMUM_FEE_BASE_UNITS),
        })
    };
    policy.approval_session_id = LINEA_DECOMMISSION_APPROVAL_SESSION_ID.to_owned();
    policy.maximum_concurrent_transfers = 1;
    policy.maximum_unknown_reconciliation_queries = 1;
    policy.direct_route_only = false;
    policy.bridge_mutations_enabled = true;
    policy.external_mutation_authorized = true;
    policy.direct_networks.insert(
        LINEA_CHAIN_ID,
        CompiledCapitalNetworkPolicy {
            network_id: NetworkId::new(format!("eip155:{LINEA_CHAIN_ID}"))?,
            binance_network: "OPTIMISM".to_owned(),
            tokens: BTreeMap::from([
                ("USDC".to_owned(), token_policy("USDC")?),
                ("USDT".to_owned(), token_policy("USDT")?),
            ]),
        },
    );
    Ok(policy)
}

fn account_balance_base_units(
    account: &AccountInformation,
    asset: &str,
    decimals: u8,
) -> anyhow::Result<U256> {
    let balance = account
        .balances
        .iter()
        .find(|balance| balance.asset == asset);
    let total = balance.map_or(Decimal::ZERO, |balance| balance.free + balance.locked);
    rebalance_decimal_to_base_units_floor(total, decimals)
}

async fn linea_return_capital(
    config: &config::AppConfig,
    mode: &str,
    asset: &str,
    live_confirmation: &str,
) -> anyhow::Result<()> {
    ensure!(
        matches!(mode, "dry-run" | "execute"),
        "--mode must be dry-run or execute"
    );
    let asset = asset.to_ascii_uppercase();
    ensure!(
        matches!(asset.as_str(), "USDT" | "USDC" | "ALL"),
        "--asset must be USDT, USDC, or ALL"
    );
    let execute = mode == "execute";
    if execute {
        ensure!(
            live_confirmation == "RETURN_LINEA_USDT_USDC_TO_BINANCE",
            "Linea capital return requires LINEA_RETURN_CAPITAL_CONFIRMATION=RETURN_LINEA_USDT_USDC_TO_BINANCE"
        );
        ensure!(
            config.arbitrage_entry_stop_file.exists(),
            "Linea capital return requires the durable arbitrage entry-stop marker"
        );
    }

    let selection = load_compatibility_domain(
        &config.domain_config_path,
        CompatibilityRole::LiveRuntime,
        false,
    )?;
    let linea_strategy = selection
        .hot_path_runtime
        .as_ref()
        .context("compiled domain has no hot-path runtime")?
        .strategies
        .iter()
        .find(|strategy| strategy.pair_id == "linea-usdt-usdc")
        .context("compiled domain has no Linea strategy")?;
    let linea_pair = linea_strategy
        .domain_config
        .snapshot()
        .pairs
        .first()
        .context("compiled Linea strategy has no pair")?;
    ensure!(
        linea_strategy.observe
            && linea_strategy.plan
            && !linea_strategy.execute
            && linea_pair.market_data_enabled
            && !linea_pair.execution_enabled
            && !linea_pair.full_live
            && linea_pair.full_live_policy.is_none()
            && !linea_pair.rebalance.enabled,
        "Linea capital return requires the deployed pair to be observe-only and mutation-disabled"
    );
    ensure!(
        linea_pair.token_a.symbol == "USDT"
            && linea_pair.token_a.decimals == 6
            && linea_pair
                .token_a
                .contract
                .eq_ignore_ascii_case(&format!("{LINEA_USDT:#x}"))
            && linea_pair.token_b.symbol == "USDC"
            && linea_pair.token_b.decimals == 6
            && linea_pair
                .token_b
                .contract
                .eq_ignore_ascii_case(&format!("{LINEA_USDC:#x}")),
        "compiled stopped Linea token identity differs from the reviewed decommission route"
    );
    let portfolio = selection
        .portfolio_runtime
        .as_ref()
        .context("compiled domain has no portfolio runtime")?;
    let capital_policy = linea_decommission_policy(portfolio)?;

    let wallet = EvmWallet::from_env()?;
    let wallet_owner = wallet.address();
    ensure!(
        config.evm_wallet_address.parse::<Address>()? == wallet_owner,
        "Linea decommission signer differs from EVM_WALLET_ADDRESS"
    );
    let linea_endpoint = std::env::var("LINEA_RPC_URL")
        .context("LINEA_RPC_URL is required for Linea capital return")?;
    let world_endpoint = std::env::var("ALCHEMY_WORLDCHAIN_RPC_URL")
        .context("ALCHEMY_WORLDCHAIN_RPC_URL is required for Linea capital return")?;
    let optimism_endpoint = std::env::var(OPTIMISM_RPC_URL_ENV)
        .with_context(|| format!("{OPTIMISM_RPC_URL_ENV} is required for Linea capital return"))?;
    let linea_rpc = JsonRpcClient::new(linea_endpoint)?;
    let world_rpc = JsonRpcClient::new(world_endpoint)?;
    let optimism_rpc = JsonRpcClient::new(optimism_endpoint)?;
    ensure!(
        linea_rpc.chain_id().await? == LINEA_CHAIN_ID,
        "Linea capital return RPC has the wrong chain id"
    );

    let linea_wallet_journal = std::env::var(ARBITRAGE_LINEA_WALLET_JOURNAL_PATH_ENV)
        .with_context(|| {
            format!(
                "{ARBITRAGE_LINEA_WALLET_JOURNAL_PATH_ENV} is required for Linea capital return"
            )
        })?;
    let mut linea_dex_executor = DexExecutor::hydrate_with_gas_policy(
        linea_rpc.clone(),
        wallet,
        LINEA_CHAIN_ID,
        linea_wallet_journal.into(),
        CompiledNetworkGasPolicy::LineaMainnet {
            requires_fresh_rpc_gas_price: true,
            max_priority_fee_equals_gas_price: true,
            max_fee_headroom_bps: 12_000,
            includes_l1_fee: false,
        },
    )
    .await?;
    linea_dex_executor.set_journal_scope(linea_evm_journal_scope())?;
    let linea_execution_service = DexExecutionService::spawn(linea_dex_executor, 1)?;

    let mut balance_client = BinanceAccountClient::from_env(config)?;
    balance_client.synchronize_clock().await?;
    let trading_client = balance_client.clone();
    let treasury_client = BinanceAccountClient::from_treasury_env(config)?;
    let subaccount_email = std::env::var("BINANCE_SUBACCOUNT_EMAIL")
        .context("BINANCE_SUBACCOUNT_EMAIL is required for Linea capital return")?;
    let rebalance_wallet_journal = std::env::var(WALLET_JOURNAL_PATH_ENV).with_context(|| {
        format!("{WALLET_JOURNAL_PATH_ENV} is required for Linea capital return")
    })?;
    let maximum = Decimal::from(2_600_u64);
    let limits = RebalanceRuntimeLimits {
        maximum_wld: maximum,
        maximum_usdc: maximum,
        maximum_esp: maximum,
        maximum_arb: maximum,
        operation_timeout: Duration::from_secs(config.rebalance_executor_timeout_seconds),
    };
    let mut executor = RebalanceExecutor::hydrate(
        trading_client,
        treasury_client,
        subaccount_email,
        AcrossClient::new(config)?,
        world_rpc,
        optimism_rpc,
        BTreeMap::from([(LINEA_CHAIN_ID, linea_rpc.clone())]),
        EvmWallet::from_env()?,
        config.rebalance_executor_journal_path.clone(),
        rebalance_wallet_journal.into(),
        Some(reviewed_rebalance_nonce_collision(wallet_owner)?),
        limits,
    )
    .await?;
    executor.set_capital_policy(Some(capital_policy))?;
    executor
        .attach_linea_execution_owner(
            linea_execution_service.evm_execution_owner(),
            linea_rpc.clone(),
        )
        .await?;

    if execute {
        if let Some(operation) = executor.recover_active().await? {
            tracing::warn!(
                operation_id = %operation.intent.operation_id,
                token = operation.intent.token_symbol,
                progress = ?operation.progress,
                "recovered the previously active rebalance before Linea decommission"
            );
        }
    } else {
        ensure!(
            executor.active_operation()?.is_none(),
            "Linea capital return dry-run found an active rebalance operation"
        );
    }

    let assets: &[&str] = match asset.as_str() {
        "USDT" => &["USDT"],
        "USDC" => &["USDC"],
        "ALL" => &["USDT", "USDC"],
        _ => unreachable!("validated Linea decommission asset"),
    };
    for symbol in assets {
        let (origin_token, destination_token) = match *symbol {
            "USDT" => (LINEA_USDT, OPTIMISM_USDT),
            "USDC" => (LINEA_USDC, OPTIMISM_USDC),
            _ => unreachable!("validated Linea decommission asset"),
        };
        let wallet_balance = linea_rpc.erc20_balance(origin_token, wallet_owner).await?;
        if wallet_balance.is_zero() {
            tracing::info!(asset = *symbol, "Linea capital balance is already zero");
            continue;
        }
        ensure!(
            wallet_balance <= U256::from(LINEA_DECOMMISSION_MAXIMUM_BASE_UNITS),
            "{symbol} Linea balance exceeds the reviewed decommission cap"
        );
        let account = balance_client.account_information().await?;
        let binance_balance = account_balance_base_units(&account, symbol, 6)?;
        let quote_amount = u128::try_from(wallet_balance)
            .context("Linea capital balance does not fit the Across quote amount")?;
        let quote_request = AcrossQuoteRequest {
            origin_chain_id: LINEA_CHAIN_ID,
            destination_chain_id: OPTIMISM_CHAIN_ID,
            input_token: origin_token,
            output_token: destination_token,
            amount: quote_amount,
            depositor: wallet_owner,
            recipient: wallet_owner,
        };
        let quote = AcrossClient::new(config)?.quote(&quote_request).await?;
        validate_quote(&quote_request, &quote)?;
        tracing::info!(
            mode,
            asset = *symbol,
            wallet_balance_base_units = %wallet_balance,
            binance_balance_base_units = %binance_balance,
            expected_output_base_units = %quote.expected_output_amount,
            minimum_output_base_units = %quote.min_output_amount,
            quoted_fee_base_units = %quote.fees.total.amount,
            expected_fill_time_seconds = quote.expected_fill_time,
            "Linea capital return route validated"
        );
        if !execute {
            continue;
        }
        let operation = executor
            .execute(RebalanceExecutionRequest {
                authority: RebalanceExecutionAuthority::LineaFullLive,
                token_symbol: (*symbol).to_owned(),
                token_decimals: 6,
                token_contract: origin_token,
                wallet_owner,
                action: RebalanceAction {
                    direction: Direction::WalletToBinance,
                    amount: wallet_balance,
                    route: Route::Across {
                        binance_network: "OPTIMISM".to_owned(),
                        bridge_chain_id: OPTIMISM_CHAIN_ID,
                        wallet_chain_id: LINEA_CHAIN_ID,
                    },
                },
                binance_balance_before: binance_balance,
                wallet_balance_before: wallet_balance,
                revalidation_start_balance: U256::ONE,
                maximum_fee: Some(U256::ZERO),
                approval_session_id: Some(LINEA_DECOMMISSION_APPROVAL_SESSION_ID.to_owned()),
            })
            .await?;
        ensure!(
            matches!(
                operation.progress,
                arb_bot::rebalance::RebalanceExecutionProgress::Completed { .. }
            ),
            "Linea capital return did not reach completed state"
        );
        let remaining = linea_rpc.erc20_balance(origin_token, wallet_owner).await?;
        ensure!(
            remaining.is_zero(),
            "{symbol} remained on Linea after completed return"
        );
        tracing::info!(
            operation_id = %operation.intent.operation_id,
            asset = *symbol,
            returned_base_units = %wallet_balance,
            "Linea capital return completed and Binance credit was proven"
        );
    }
    Ok(())
}

async fn binance_withdrawal_status(
    config: &config::AppConfig,
    coin: &str,
    withdraw_order_id: &str,
) -> anyhow::Result<()> {
    let mut client = BinanceAccountClient::from_env(config)?;
    client.synchronize_clock().await?;
    let records = client.withdrawal_history(coin, withdraw_order_id).await?;
    ensure!(records.len() == 1, "expected exactly one withdrawal record");
    let record = &records[0];
    ensure!(
        record.withdraw_order_id == withdraw_order_id,
        "Binance returned an unexpected withdrawal client id"
    );
    log_withdrawal_record(record);
    Ok(())
}

async fn binance_capital_recovery(
    config: &config::AppConfig,
    coin: &str,
    network: &str,
    deposit_transaction_hash: Option<&str>,
    withdraw_order_id: Option<&str>,
) -> anyhow::Result<()> {
    let mut client = BinanceAccountClient::from_env(config)?;
    let snapshot = client
        .hydrate_capital_recovery(coin, network, deposit_transaction_hash, withdraw_order_id)
        .await?;
    log_capital_recovery_snapshot(&snapshot);
    Ok(())
}

fn log_capital_recovery_snapshot(snapshot: &CapitalRecoverySnapshot) {
    tracing::info!(
        coin = %snapshot.coin,
        network = %snapshot.network,
        deposit_address = %snapshot.deposit_address.address,
        matching_deposits = snapshot.deposits.len(),
        matching_withdrawals = snapshot.withdrawals.len(),
        "Binance capital recovery snapshot hydrated"
    );
    for deposit in &snapshot.deposits {
        tracing::info!(
            binance_deposit_id = %deposit.deposit_id,
            coin = %deposit.coin,
            network = %deposit.network,
            amount = %deposit.amount,
            transaction_id = %deposit.tx_id,
            status = deposit.credit_state().label(),
            questionnaire_required = deposit.questionnaire_required(),
            insert_time_ms = deposit.insert_time,
            confirmations = %deposit.confirm_times,
            "matching Binance deposit recovery record hydrated"
        );
    }
    for withdrawal in &snapshot.withdrawals {
        tracing::info!(
            binance_withdrawal_id = %withdrawal.id,
            withdraw_order_id = %withdrawal.withdraw_order_id,
            coin = %withdrawal.coin,
            network = %withdrawal.network,
            amount = %withdrawal.amount,
            transaction_fee = %withdrawal.transaction_fee,
            transaction_id = %withdrawal.tx_id,
            status = withdrawal.state().label(),
            terminal = withdrawal.state().is_terminal(),
            "matching Binance withdrawal recovery record hydrated"
        );
    }
}

async fn binance_travel_rule_withdrawal_status(
    config: &config::AppConfig,
    tr_id: i64,
) -> anyhow::Result<()> {
    let mut client = BinanceAccountClient::from_env(config)?;
    client.synchronize_clock().await?;
    let records = client.travel_rule_withdrawal_history(tr_id).await?;
    ensure!(
        records.len() == 1,
        "expected exactly one Travel Rule record"
    );
    let record = &records[0];
    ensure!(
        record.tr_id == tr_id,
        "Binance returned an unexpected Travel Rule id"
    );
    log_travel_rule_withdrawal_record(record);
    Ok(())
}

fn log_travel_rule_withdrawal_record(record: &TravelRuleWithdrawalRecord) {
    tracing::info!(
        travel_rule_id = record.tr_id,
        binance_withdrawal_id = %record.id,
        withdraw_order_id = %record.withdraw_order_id,
        coin = %record.coin,
        network = %record.network,
        amount = %record.amount,
        transaction_fee = %record.transaction_fee,
        withdrawal_status = ?record.withdrawal_status,
        travel_rule_status = record.travel_rule_status,
        destination = %record.address,
        transaction_id = %record.tx_id,
        info = %record.info,
        "Binance Travel Rule withdrawal status hydrated"
    );
}

fn log_withdrawal_record(record: &WithdrawalRecord) {
    tracing::info!(
        binance_withdrawal_id = %record.id,
        withdraw_order_id = %record.withdraw_order_id,
        coin = %record.coin,
        network = %record.network,
        amount = %record.amount,
        transaction_fee = %record.transaction_fee,
        status = record.status,
        destination = %record.address,
        transaction_id = %record.tx_id,
        info = %record.info,
        "Binance withdrawal status hydrated"
    );
}

async fn wallet_hydrate(domain_config: &LoadedDomainConfig) -> anyhow::Result<()> {
    let address = std::env::var("EVM_WALLET_ADDRESS")
        .context("required environment variable EVM_WALLET_ADDRESS is not set")?
        .parse::<Address>()
        .context("EVM_WALLET_ADDRESS is invalid")?;
    let pairs = &domain_config.snapshot().pairs;
    ensure!(
        pairs.len() == 1,
        "wallet hydration requires exactly one configured pair"
    );
    let pair = &pairs[0];
    ensure!(
        pair.chain.chain_id == 480,
        "configured execution pair must be on World Chain"
    );
    let world_endpoint = std::env::var(&pair.chain.rpc_url_env).with_context(|| {
        format!(
            "required environment variable {} is not set",
            pair.chain.rpc_url_env
        )
    })?;
    let optimism_endpoint = std::env::var(OPTIMISM_RPC_URL_ENV).with_context(|| {
        format!("required environment variable {OPTIMISM_RPC_URL_ENV} is not set")
    })?;
    let world_tokens = vec![
        TokenBalanceRequest {
            symbol: pair.token_a.symbol.clone(),
            contract: pair
                .token_a
                .contract
                .parse()
                .context("configured World Chain token_a address is invalid")?,
        },
        TokenBalanceRequest {
            symbol: pair.token_b.symbol.clone(),
            contract: pair
                .token_b
                .contract
                .parse()
                .context("configured World Chain token_b address is invalid")?,
        },
    ];
    let optimism_tokens = vec![
        TokenBalanceRequest {
            symbol: "USDC".to_owned(),
            contract: "0x0b2c639c533813f4aa9d7837caf62653d097ff85"
                .parse::<Address>()
                .expect("constant native Optimism USDC address is valid"),
        },
        TokenBalanceRequest {
            symbol: "USDC.e".to_owned(),
            contract: "0x7f5c764cbc14f9669b88837ca1490cca17c31607"
                .parse::<Address>()
                .expect("constant bridged Optimism USDC address is valid"),
        },
        TokenBalanceRequest {
            symbol: "WLD".to_owned(),
            contract: "0xdc6ff44d5d932cbd77b52e5612ba0529dc6226f1"
                .parse::<Address>()
                .expect("constant Optimism WLD address is valid"),
        },
    ];
    let (world, optimism) = tokio::try_join!(
        hydrate_chain_wallet(world_endpoint, 480, address, &world_tokens),
        hydrate_chain_wallet(optimism_endpoint, 10, address, &optimism_tokens),
    )?;
    log_chain_wallet_state(address, "World Chain", &world);
    log_chain_wallet_state(address, "Optimism", &optimism);
    Ok(())
}

fn log_chain_wallet_state(
    address: Address,
    chain_name: &str,
    state: &arb_bot::wallet::ChainWalletState,
) {
    tracing::info!(
        wallet_address = %address,
        chain = chain_name,
        chain_id = state.chain_id,
        block_number = state.block_number,
        latest_nonce = state.latest_nonce,
        pending_nonce = state.pending_nonce,
        has_pending_transactions = state.has_pending_transactions(),
        native_balance_wei = %state.native_balance_wei,
        rpc_http_requests = state.rpc_stats.http_requests,
        rpc_eth_calls = state.rpc_stats.eth_calls,
        "EVM wallet chain state hydrated"
    );
    for token in &state.token_balances {
        tracing::info!(
            wallet_address = %address,
            chain = chain_name,
            chain_id = state.chain_id,
            symbol = %token.symbol,
            contract = %token.contract,
            balance_base_units = %token.base_units,
            "EVM wallet token balance hydrated"
        );
    }
    for allowance in &state.token_allowances {
        tracing::info!(
            wallet_address = %address,
            chain = chain_name,
            chain_id = state.chain_id,
            symbol = %allowance.symbol,
            contract = %allowance.contract,
            spender = %allowance.spender,
            allowance_base_units = %allowance.base_units,
            "EVM wallet token allowance hydrated"
        );
    }
}

async fn binance_recent_validation_orders(
    config: &config::AppConfig,
    limit: u16,
) -> anyhow::Result<()> {
    let mut ws = BinanceWsApiClient::connect(config).await?;
    let orders = ws.recent_orders("WLDUSDC", limit).await?;
    let validation_orders = orders
        .iter()
        .filter(|order| order.client_order_id.starts_with("rustval"))
        .collect::<Vec<_>>();
    for order in &validation_orders {
        tracing::info!(
            symbol = %order.symbol,
            order_id = order.order_id,
            client_order_id = %order.client_order_id,
            side = %order.side,
            order_type = %order.order_type,
            status = %order.status,
            executed_base = %order.executed_qty,
            executed_quote = %order.cummulative_quote_qty,
            "Rust Binance validation order found"
        );
    }
    ensure!(
        !validation_orders.is_empty(),
        "no recent Rust validation orders found"
    );
    let open_orders = ws.open_orders("WLDUSDC").await?;
    let open_validation_orders = open_orders
        .iter()
        .filter(|order| order.client_order_id.starts_with("rustval"))
        .count();
    ensure!(
        open_validation_orders == 0,
        "a Rust Binance validation order is still open"
    );
    tracing::info!(
        validation_orders = validation_orders.len(),
        inspected_orders = orders.len(),
        open_orders = open_orders.len(),
        open_validation_orders,
        binance_ws_clock_offset_ms = ws.clock_offset_ms(),
        "recent Rust Binance validation order audit completed"
    );
    Ok(())
}

fn load_dotenv() -> anyhow::Result<()> {
    if let Some(path) = std::env::var_os("ENV_FILE") {
        dotenvy::from_path(&path)
            .with_context(|| format!("failed to load ENV_FILE {}", path.to_string_lossy()))?;
    } else {
        dotenvy::dotenv().ok();
    }
    Ok(())
}

async fn hydrate(domain_config: &LoadedDomainConfig) -> anyhow::Result<()> {
    let mut rpc_env_names = domain_config
        .snapshot()
        .pairs
        .iter()
        .filter(|pair| pair.market_data_enabled)
        .map(|pair| pair.chain.rpc_url_env.as_str());
    let rpc_env_name = rpc_env_names
        .next()
        .context("no enabled pair RPC endpoint")?;
    ensure!(
        rpc_env_names.all(|candidate| candidate == rpc_env_name),
        "hydrate command currently requires one shared RPC endpoint"
    );
    let endpoint = std::env::var(rpc_env_name)
        .with_context(|| format!("required environment variable {rpc_env_name} is not set"))?;
    let rpc = JsonRpcClient::new(endpoint)?;
    let state = DexHydrator::new(&rpc)
        .hydrate(domain_config.snapshot())
        .await?;

    for pool in &state.pools {
        tracing::info!(
            pair_id = %pool.pair_id,
            identity = ?pool.identity,
            token0 = %pool.token0,
            token1 = %pool.token1,
            tick = pool.pool.tick,
            liquidity = pool.pool.liquidity,
            initialized_ticks = pool.pool.initialized_tick_count(),
            "DEX pool hydrated"
        );
    }
    tracing::info!(
        block_number = state.block.number,
        block_hash = %state.block.hash,
        pools = state.pools.len(),
        unavailable = ?state.unavailable,
        rpc = ?rpc.stats(),
        "DEX hydration completed"
    );
    Ok(())
}

async fn collect_prices(
    config: config::AppConfig,
    domain_config: Arc<LoadedDomainConfig>,
    compiled_network_runtime: Option<CompiledNetworkRuntimePlan>,
) -> anyhow::Result<()> {
    ensure!(
        !domain_config.snapshot().live_trading_enabled
            && domain_config
                .snapshot()
                .pairs
                .iter()
                .all(|pair| !pair.execution_enabled && !pair.rebalance.enabled),
        "collect-prices requires execution and rebalancing to be disabled in the domain artifact"
    );
    ensure!(
        config.arbitrage_execution_mode == "disabled"
            && config.rebalance_execution_mode == "disabled",
        "collect-prices requires ARBITRAGE_EXECUTION_MODE=disabled and REBALANCE_EXECUTION_MODE=disabled"
    );
    let symbols = domain_config.binance_symbols();
    ensure!(
        symbols.len() == 1,
        "collect-prices currently requires exactly one enabled Binance symbol"
    );
    let pair = domain_config
        .snapshot()
        .pairs
        .iter()
        .find(|pair| pair.market_data_enabled)
        .context("collect-prices requires one enabled pair")?;

    let (telemetry, writer) = TelemetryWriter::new(&config).channel();
    let writer_task = tokio::spawn(writer.run());
    let network_registry = match compiled_network_runtime {
        Some(plan) => Some(
            NetworkRuntimeRegistry::connect(plan, telemetry.clone(), config.engine_id.clone())
                .await?,
        ),
        None => None,
    };
    let InitializedDex {
        mut mirror,
        stream,
        rpc: wallet_rpc,
        timings: _,
    } = initialize_dex(&config, domain_config.as_ref(), network_registry.as_ref()).await?;
    let mut opportunities = OpportunityEngine::new(domain_config.snapshot(), &mirror)?;
    let balance_telemetry = telemetry.clone();
    let pool_quote_telemetry = telemetry.clone();
    let (hot_telemetry, hot_telemetry_task) = hot_telemetry::channel(
        &config,
        opportunities.pairs(),
        &mirror,
        telemetry.clone(),
        // The public collect-prices sidecar has neither authenticated
        // per-symbol commissions nor an execution-owner gas cache. Emitting a
        // cost model from it would create a plausible-looking invalid cohort.
        PreTradeCostTelemetry::disabled(),
    )?;
    let hot_telemetry_task = tokio::spawn(hot_telemetry_task.run());

    let AlchemyDexStream {
        receiver: mut dex_receiver,
        task: mut dex_task,
    } = stream;
    let symbol = symbols[0].clone();
    let mut binance_feed = BookTickerFeed::new(&config, symbol.clone());
    let mut runtime_state = RuntimeState::new([Arc::<str>::from(symbol.as_str())]);
    let mut latest_quote: Option<TopOfBook> = None;
    let wallet_owner = config
        .evm_wallet_address
        .trim()
        .parse::<Address>()
        .context("collect-prices requires a valid EVM_WALLET_ADDRESS for balance sync")?;
    let wallet_tokens = vec![
        TokenBalanceRequest {
            symbol: pair.token_a.symbol.clone(),
            contract: pair
                .token_a
                .contract
                .parse()
                .context("configured token_a address is invalid")?,
        },
        TokenBalanceRequest {
            symbol: pair.token_b.symbol.clone(),
            contract: pair
                .token_b
                .contract
                .parse()
                .context("configured token_b address is invalid")?,
        },
    ];
    if let Some(registry) = network_registry.as_ref() {
        let _ = hydrate_network_wallet_registries(
            registry,
            wallet_owner,
            &telemetry,
            &config.engine_id,
        )
        .await?;
    }
    let (balance_heads, balance_head_receiver) = tokio::sync::watch::channel(mirror.latest_head());
    let balance_context = CollectorBalanceContext {
        telemetry: balance_telemetry,
        engine_id: config.engine_id.clone(),
        pair_id: pair.id.clone(),
        interval: Duration::from_millis(config.balance_sync_interval_ms),
    };
    let wallet_balance_task = tokio::spawn(run_collector_wallet_balance_sync(
        wallet_rpc,
        wallet_owner,
        pair.chain.chain_id,
        wallet_tokens,
        balance_head_receiver,
        balance_context.clone(),
    ));
    let binance_balance_task = collector_read_only_binance_client(&config)
        .await?
        .map(|client| {
            tokio::spawn(run_collector_binance_balance_sync(
                client,
                [pair.token_a.symbol.clone(), pair.token_b.symbol.clone()],
                balance_context,
            ))
        });
    let binance_balance_sync_enabled = binance_balance_task.is_some();
    let ready_path = runtime_ready_marker_path()?;
    let mut ready_marked = false;
    sync_runtime_ready_marker(ready_path.as_deref(), &mut ready_marked, false)?;

    tracing::info!(
        service = %config.service_name,
        engine_id = %config.engine_id,
        pair_id = %pair.id,
        chain_id = pair.chain.chain_id,
        symbol = %symbol,
        domain_snapshot_id = %domain_config.snapshot().snapshot_id,
        domain_config_sha256 = %domain_config.fingerprint_sha256(),
        pools = mirror.pool_count(),
        unavailable_pools = mirror.unavailable_count(),
        clickhouse_enabled = config.clickhouse_enabled(),
        binance_market_data_authenticated = false,
        binance_balance_sync_enabled,
        wallet_balance_sync_enabled = true,
        execution_enabled = false,
        rebalance_enabled = false,
        "public price collector started"
    );

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let mut health_tick = tokio::time::interval(Duration::from_secs(1));
    health_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut binance_event = Box::pin(binance_feed.next_event());

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => break,
            event = dex_receiver.recv() => {
                let Some(event) = event else {
                    bail!("Arbitrum DEX stream stopped; process restart will rehydrate state");
                };
                let new_head = match &event {
                    arb_bot::market_data::alchemy::DexStreamEvent::Head { head, .. } => Some(*head),
                    arb_bot::market_data::alchemy::DexStreamEvent::Log { .. } => None,
                };
                let changed = process_price_collector_dex_event(
                    &mut mirror,
                    &mut opportunities,
                    event,
                )?;
                if let Some(head) = new_head {
                    balance_heads.send_replace(head);
                }
                if changed
                    && let Some(quote) = latest_quote.as_ref()
                {
                    emit_price_collector_evaluation(
                        &mut opportunities,
                        CollectorPriceTelemetry {
                            hot: &hot_telemetry,
                            pool_quote: &pool_quote_telemetry,
                            engine_id: &config.engine_id,
                        },
                        &mirror,
                        quote,
                        mirror.latest_head().number,
                        "dex",
                    )?;
                }
            }
            event = &mut binance_event => {
                drop(binance_event);
                match event {
                    MarketEvent::FeedConnected {
                        symbol,
                        generation,
                        observed_at,
                    } => runtime_state.on_connected(&symbol, generation, observed_at),
                    MarketEvent::FeedDisconnected {
                        symbol,
                        generation,
                        ..
                    } => {
                        runtime_state.on_disconnected(&symbol, generation);
                        latest_quote = None;
                    }
                    MarketEvent::FeedHeartbeat {
                        symbol,
                        generation,
                        observed_at,
                    } => {
                        runtime_state.record_transport_activity(
                            &symbol,
                            generation,
                            observed_at,
                        );
                    }
                    MarketEvent::BinanceTopOfBook(quote) => {
                        let accepted =
                            runtime_state.apply_quote(quote.clone()) == QuoteApplyResult::Accepted;
                        let phase = runtime_state.refresh_phase(
                            std::time::Instant::now(),
                            pair.strategy.max_transport_silence_ms(),
                            mirror.is_fresh(
                                std::time::Instant::now(),
                                config.dex_head_max_age_ms,
                            ),
                        );
                        hot_telemetry.emit_binance_book(
                            &quote,
                            "strategy",
                            Some(phase),
                            if accepted { "evaluated" } else { "rejected" },
                        );
                        if accepted {
                            emit_price_collector_evaluation(
                                &mut opportunities,
                                CollectorPriceTelemetry {
                                    hot: &hot_telemetry,
                                    pool_quote: &pool_quote_telemetry,
                                    engine_id: &config.engine_id,
                                },
                                &mirror,
                                &quote,
                                mirror.latest_head().number,
                                "binance",
                            )?;
                            latest_quote = Some(quote);
                        }
                    }
                    MarketEvent::BinanceDepthApplied { .. } => {
                        bail!("public price collector unexpectedly received Binance depth");
                    }
                }
                binance_event = Box::pin(binance_feed.next_event());
            }
            _ = health_tick.tick() => {}
            result = &mut dex_task => {
                result.context("Arbitrum DEX connector task failed")??;
                bail!("Arbitrum DEX connector stopped; process restart will rehydrate state");
            }
        }

        let phase = runtime_state.refresh_phase(
            std::time::Instant::now(),
            pair.strategy.max_transport_silence_ms(),
            mirror.is_fresh(std::time::Instant::now(), config.dex_head_max_age_ms),
        );
        sync_runtime_ready_marker(
            ready_path.as_deref(),
            &mut ready_marked,
            phase == RuntimePhase::Ready,
        )?;
    }

    runtime_state.stop();
    sync_runtime_ready_marker(ready_path.as_deref(), &mut ready_marked, false)?;
    dex_task.abort();
    let _ = dex_task.await;
    wallet_balance_task.abort();
    let _ = wallet_balance_task.await;
    if let Some(task) = binance_balance_task {
        task.abort();
        let _ = task.await;
    }
    drop(pool_quote_telemetry);
    drop(hot_telemetry);
    hot_telemetry_task.await??;
    writer_task.await??;
    tracing::info!(
        pair_id = %pair.id,
        symbol = %symbol,
        "public price collector stopped"
    );
    Ok(())
}

async fn collector_read_only_binance_client(
    config: &config::AppConfig,
) -> anyhow::Result<Option<BinanceAccountClient>> {
    let api_key_present = std::env::var_os("BINANCE_READ_ONLY_API_KEY").is_some();
    let secret_key_present = std::env::var_os("BINANCE_READ_ONLY_SECRET_KEY").is_some();
    ensure!(
        api_key_present == secret_key_present,
        "BINANCE_READ_ONLY_API_KEY and BINANCE_READ_ONLY_SECRET_KEY must be configured together"
    );
    if !api_key_present {
        tracing::warn!(
            "read-only Binance credentials are absent; Binance balance sync is disabled"
        );
        return Ok(None);
    }

    let mut client = BinanceAccountClient::from_read_only_env(config)?;
    client.synchronize_clock().await?;
    let permissions = client.api_key_permissions().await?;
    ensure!(
        permissions.enable_reading
            && !permissions.enable_spot_and_margin_trading
            && !permissions.enable_withdrawals
            && !permissions.enable_internal_transfer
            && !permissions.permits_universal_transfer,
        "ESP shadow requires a read-only Binance key with every trading, withdrawal, and transfer permission disabled"
    );
    Ok(Some(client))
}

#[derive(Clone)]
struct CollectorBalanceContext {
    telemetry: arb_bot::telemetry::TelemetryHandle,
    engine_id: String,
    pair_id: String,
    interval: Duration,
}

#[derive(Clone, Copy)]
struct CollectorPriceTelemetry<'a> {
    hot: &'a arb_bot::hot_telemetry::HotTelemetryHandle,
    pool_quote: &'a TelemetryHandle,
    engine_id: &'a str,
}

async fn run_collector_wallet_balance_sync(
    rpc: JsonRpcClient,
    owner: Address,
    chain_id: u64,
    tokens: Vec<TokenBalanceRequest>,
    mut heads: tokio::sync::watch::Receiver<CanonicalBlock>,
    context: CollectorBalanceContext,
) -> anyhow::Result<()> {
    let mut tick = tokio::time::interval(context.interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        let head = *heads.borrow_and_update();
        match fetch_wallet_snapshot(&rpc, owner, chain_id, &tokens, head).await {
            Ok(snapshot) => context.telemetry.emit(
                "balance_snapshot",
                serde_json::json!({
                    "engine_id": context.engine_id,
                    "pair_id": context.pair_id,
                    "source": "wallet",
                    "owner": snapshot.owner.to_string(),
                    "chain_id": snapshot.chain_id,
                    "chain_block": snapshot.block_number,
                    "chain_block_hash": snapshot.block_hash.to_string(),
                    "request_duration_us": snapshot.request_duration_us,
                    "tokens": snapshot.token_balances.iter().map(|balance| {
                        serde_json::json!({
                            "symbol": balance.symbol.as_ref(),
                            "contract": balance.contract.to_string(),
                            "base_units": balance.base_units.to_string(),
                        })
                    }).collect::<Vec<_>>(),
                }),
            ),
            Err(error) => tracing::warn!(
                pair_id = %context.pair_id,
                chain_id,
                error = %format!("{error:#}"),
                "ESP wallet balance sync failed"
            ),
        }
        heads.changed().await.ok();
    }
}

async fn run_collector_binance_balance_sync(
    mut client: BinanceAccountClient,
    assets: [String; 2],
    context: CollectorBalanceContext,
) -> anyhow::Result<()> {
    let mut tick = tokio::time::interval(context.interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        let started_at = std::time::Instant::now();
        let account = match client.account_information().await {
            Ok(account) => account,
            Err(first_error) => {
                client.synchronize_clock().await.with_context(|| {
                    format!(
                        "Binance balance request failed: {first_error:#}; clock resynchronization also failed"
                    )
                })?;
                client.account_information().await?
            }
        };
        context.telemetry.emit(
            "balance_snapshot",
            serde_json::json!({
                "engine_id": context.engine_id,
                "pair_id": context.pair_id,
                "source": "binance",
                "account_type": account.account_type,
                "api_key_read_only": true,
                "account_update_time_ms": account.update_time,
                "request_duration_us": started_at.elapsed().as_micros(),
                "assets": assets.iter().map(|asset| {
                    let balance = account.balances.iter().find(|balance| balance.asset == *asset);
                    serde_json::json!({
                        "symbol": asset,
                        "free": balance.map_or(Decimal::ZERO, |balance| balance.free).to_string(),
                        "locked": balance.map_or(Decimal::ZERO, |balance| balance.locked).to_string(),
                    })
                }).collect::<Vec<_>>(),
            }),
        );
    }
}

fn process_price_collector_dex_event(
    mirror: &mut DexMirror,
    opportunities: &mut OpportunityEngine,
    event: arb_bot::market_data::alchemy::DexStreamEvent,
) -> anyhow::Result<bool> {
    match event {
        arb_bot::market_data::alchemy::DexStreamEvent::Log {
            log,
            block_timestamp,
            ..
        } => {
            let LogApplyResult::Applied {
                pool_index,
                refresh_required,
                ..
            } = mirror.apply_log_at_timestamp(&log, block_timestamp)?
            else {
                return Ok(false);
            };
            if !refresh_required {
                return Ok(false);
            }
            mirror.refresh_pool_for_publication(pool_index)?;
            let request = opportunities.request_pool_refresh(pool_index, mirror)?;
            let result = request.build()?;
            Ok(opportunities.finish_pool_refresh(result)?.is_some())
        }
        arb_bot::market_data::alchemy::DexStreamEvent::Head {
            head,
            timestamp,
            received_at,
        } => {
            let applied = mirror.apply_head_at(head, Some(timestamp), received_at)?;
            let Some(pool_index) = applied.refresh_pool_index else {
                return Ok(false);
            };
            let request = opportunities.request_pool_refresh(pool_index, mirror)?;
            let result = request.build()?;
            Ok(opportunities.finish_pool_refresh(result)?.is_some())
        }
    }
}

fn emit_price_collector_evaluation(
    opportunities: &mut OpportunityEngine,
    telemetry: CollectorPriceTelemetry<'_>,
    mirror: &DexMirror,
    quote: &TopOfBook,
    chain_block: u64,
    trigger: &'static str,
) -> anyhow::Result<()> {
    let started_at = std::time::Instant::now();
    if let Some(evaluation) = opportunities.evaluate(quote)? {
        let pair = opportunities.pair(evaluation.pair_index)?;
        let pair_id = pair.pair_id.clone();
        let chain_id = pair.chain_id;
        let symbol = pair.symbol.clone();
        let pool_indices = pair.all_pool_indices().to_vec();
        for pool_index in pool_indices {
            let pool = mirror.pool(pool_index)?;
            let (provider, pool_identity, fee_pips) = match pool.identity {
                PoolIdentity::V3 { address, fee_pips } => {
                    ("uniswap_v3", address.to_string(), Some(fee_pips))
                }
                PoolIdentity::PancakeV3 { address, fee_pips } => {
                    ("pancakeswap_v3", address.to_string(), Some(fee_pips))
                }
                PoolIdentity::CamelotV3 { address } => ("camelot_v3", address.to_string(), None),
                PoolIdentity::LynexAlgebraV1_9 { address } => {
                    ("lynex_algebra_v1_9", address.to_string(), None)
                }
                PoolIdentity::V4 { pool_id, fee_pips } => {
                    ("uniswap_v4", pool_id.to_string(), Some(fee_pips))
                }
            };
            for direction in [
                ArbitrageDirection::BuyTokenBOnDexSellOnCex,
                ArbitrageDirection::BuyTokenBOnCexSellOnDex,
            ] {
                let trade = opportunities.evaluate_exact_candidate(
                    evaluation.pair_index,
                    quote,
                    direction,
                    pool_index,
                    evaluation.baseline_token_b_amount,
                )?;
                telemetry.pool_quote.emit(
                    "dex_pool_quote",
                    serde_json::json!({
                        "engine_id": telemetry.engine_id,
                        "pair_id": &pair_id,
                        "chain_id": chain_id,
                        "chain_block": chain_block,
                        "symbol": &symbol,
                        "update_id": quote.update_id,
                        "provider": provider,
                        "pool_index": pool_index,
                        "pool_identity": pool_identity,
                        "pool_fee_pips": fee_pips,
                        "pool_tick_spacing": pool.pool.tick_spacing,
                        "pool_tick": pool.pool.tick,
                        "pool_sqrt_price_x96": pool.pool.sqrt_price_x96.to_string(),
                        "pool_liquidity": pool.pool.liquidity.to_string(),
                        "direction": direction.as_str(),
                        "quote_mode": match direction {
                            ArbitrageDirection::BuyTokenBOnDexSellOnCex => "exact_output_token_b",
                            ArbitrageDirection::BuyTokenBOnCexSellOnDex => "exact_input_token_b",
                        },
                        "token_b_base_units": evaluation.baseline_token_b_amount.to_string(),
                        "dex_token_a_base_units": trade.map(|trade| trade.dex_token_a_amount.to_string()),
                        "available": trade.is_some(),
                        "evaluation_trigger": trigger,
                    }),
                );
            }
        }
        telemetry.hot.emit_evaluation(
            quote,
            evaluation,
            chain_block,
            started_at.elapsed().as_micros(),
            200,
            trigger,
        );
    }
    Ok(())
}

fn market_event_symbol(event: &MarketEvent) -> &str {
    match event {
        MarketEvent::FeedConnected { symbol, .. }
        | MarketEvent::FeedDisconnected { symbol, .. }
        | MarketEvent::FeedHeartbeat { symbol, .. }
        | MarketEvent::BinanceDepthApplied { symbol, .. } => symbol,
        MarketEvent::BinanceTopOfBook(quote) => &quote.symbol,
    }
}

fn observed_market_event_fields(
    event: &MarketEvent,
) -> (hot_telemetry::SharedStreamEventKind, u64, u128, usize) {
    match event {
        MarketEvent::FeedConnected { generation, .. } => (
            hot_telemetry::SharedStreamEventKind::Connected,
            *generation,
            0,
            0,
        ),
        MarketEvent::FeedDisconnected { generation, .. } => (
            hot_telemetry::SharedStreamEventKind::Disconnected,
            *generation,
            0,
            0,
        ),
        MarketEvent::FeedHeartbeat { generation, .. } => (
            hot_telemetry::SharedStreamEventKind::Heartbeat,
            *generation,
            0,
            0,
        ),
        MarketEvent::BinanceTopOfBook(quote) => (
            hot_telemetry::SharedStreamEventKind::BookTicker,
            quote.connection_generation,
            quote.parse_time_us,
            quote.wire_frame_size_bytes,
        ),
        MarketEvent::BinanceDepthApplied {
            generation,
            parse_apply_time_us,
            wire_frame_size_bytes,
            ..
        } => (
            hot_telemetry::SharedStreamEventKind::Depth,
            *generation,
            *parse_apply_time_us,
            *wire_frame_size_bytes,
        ),
    }
}

async fn run(
    config: config::AppConfig,
    domain_config: Arc<LoadedDomainConfig>,
    compiled_binance_runtime: Option<Arc<CompiledBinanceRuntimePlan>>,
    compiled_network_runtime: Option<CompiledNetworkRuntimePlan>,
    compiled_hot_path_runtime: Option<CompiledHotPathRuntimePlan>,
    compiled_portfolio_runtime: Option<CompiledPortfolioRuntimePlan>,
    bootstrap: BootstrapTiming,
) -> anyhow::Result<()> {
    let (telemetry, writer) = TelemetryWriter::new(&config).channel();
    let writer_task = tokio::spawn(writer.run());
    let network_registry = match compiled_network_runtime {
        Some(plan) => Some(
            NetworkRuntimeRegistry::connect(plan, telemetry.clone(), config.engine_id.clone())
                .await?,
        ),
        None => None,
    };
    let hot_path_dependencies = compiled_hot_path_runtime
        .map(CompiledStrategyDependencyIndex::new)
        .transpose()?;
    if let Some(registry) = network_registry.as_ref() {
        let world = registry.get_by_chain_id(WORLD_CHAIN_CHAIN_ID)?;
        ensure!(
            world.execution().mutation_enabled()
                && matches!(
                    world.execution().gas_policy(),
                    CompiledNetworkGasPolicy::WorldChainV12 {
                        fallback_gas_price_wei: 100_000,
                        includes_l1_fee: true,
                    }
                ),
            "World Chain network runtime does not preserve reviewed v12 execution gas policy"
        );
        let arbitrum = registry.get_by_chain_id(42_161)?;
        ensure!(
            arbitrum.execution().mutation_enabled()
                && matches!(
                    arbitrum.execution().gas_policy(),
                    CompiledNetworkGasPolicy::ArbitrumOne {
                        requires_fresh_rpc_gas_price: true,
                        max_priority_fee_per_gas_wei: 0,
                        max_fee_headroom_bps,
                        includes_l1_fee: false,
                    } if *max_fee_headroom_bps >= 11_000
                ),
            "Arbitrum ESP execution policy must be mutation-enabled with fail-closed gas pricing"
        );
        let linea = registry.get_by_chain_id(LINEA_CHAIN_ID)?;
        ensure!(
            !linea.execution().mutation_enabled()
                && matches!(
                    linea.execution().gas_policy(),
                    CompiledNetworkGasPolicy::ReadOnly
                ),
            "stopped Linea execution policy must remain read-only"
        );
    }
    let shadow_strategy_plan = hot_path_dependencies.as_ref().and_then(|dependencies| {
        dependencies
            .plan()
            .strategies
            .iter()
            .find(|strategy| strategy.symbol == "ESPUSDC")
            .cloned()
    });
    let arb_strategy_plan = hot_path_dependencies.as_ref().and_then(|dependencies| {
        dependencies
            .plan()
            .strategies
            .iter()
            .find(|strategy| strategy.symbol == "ARBUSDC")
            .cloned()
    });
    let linea_strategy_plan = hot_path_dependencies.as_ref().and_then(|dependencies| {
        dependencies
            .plan()
            .strategies
            .iter()
            .find(|strategy| strategy.symbol == "USDCUSDT")
            .cloned()
    });
    if let Some(dependencies) = hot_path_dependencies.as_ref() {
        ensure!(
            dependencies
                .plan()
                .strategies
                .iter()
                .filter(|strategy| strategy.execute)
                .count()
                == 3,
            "production hot path requires exactly three executable strategies"
        );
        ensure!(
            dependencies
                .plan()
                .strategies
                .iter()
                .filter(|strategy| strategy.observe && !strategy.execute)
                .count()
                == 1,
            "production hot path requires exactly one observe-only stopped strategy"
        );
        let executable = dependencies
            .plan()
            .strategies
            .iter()
            .filter(|strategy| strategy.execute)
            .collect::<Vec<_>>();
        ensure!(
            executable.len() == 3
                && executable
                    .iter()
                    .any(|strategy| strategy.symbol == "WLDUSDC")
                && executable
                    .iter()
                    .any(|strategy| strategy.symbol == "ESPUSDC")
                && executable
                    .iter()
                    .any(|strategy| strategy.symbol == "ARBUSDC")
                && dependencies.plan().strategies.iter().any(|strategy| {
                    strategy.symbol == "USDCUSDT" && strategy.observe && !strategy.execute
                }),
            "production permits execution only for WLDUSDC, ESPUSDC, and ARBUSDC; USDCUSDT must be observe-only"
        );
        let account_id = compiled_binance_runtime
            .as_ref()
            .context("scoped ownership graph requires one Binance account")?
            .account_id
            .as_str();
        let evm_owner_count = network_registry
            .as_ref()
            .context("scoped ownership graph requires network runtimes")?
            .runtimes()
            .count();
        telemetry.emit(
            "execution_ownership_runtime_started",
            serde_json::json!({
                "engine_id": config.engine_id,
                "schema_version": 2,
                "account_id": account_id,
                "strategy_count": dependencies.plan().strategies.len(),
                "executable_strategy_count": executable.len(),
                "executable_symbols": executable.iter().map(|strategy| &strategy.symbol).collect::<Vec<_>>(),
                "evm_owner_count": evm_owner_count,
                "candidate_policy": "latest_per_strategy_round_robin",
                "portfolio_admission": "atomic_shared_owner",
                "parent_protocol": "fsync_before_child_dispatch",
                "binance_owner_count": 1,
                "global_trade_serialization": true,
                "rebalance_signer_access": false,
            }),
        );
        tracing::info!(
            account_id,
            strategy_count = dependencies.plan().strategies.len(),
            executable_symbols = ?executable.iter().map(|strategy| &strategy.symbol).collect::<Vec<_>>(),
            evm_owner_count,
            journal_schema_version = 2,
            candidate_policy = "latest_per_strategy_round_robin",
            global_trade_serialization = true,
            "scoped execution ownership graph validated"
        );
    }
    let (initialized_dex, shadow_initialized_dex, arb_initialized_dex, linea_initialized_dex) =
        if let (Some(shadow), Some(arb), Some(linea)) = (
            shadow_strategy_plan.as_ref(),
            arb_strategy_plan.as_ref(),
            linea_strategy_plan.as_ref(),
        ) {
            let (primary, observed, arb, linea) = tokio::try_join!(
                initialize_dex(&config, domain_config.as_ref(), network_registry.as_ref()),
                initialize_dex(&config, &shadow.domain_config, network_registry.as_ref()),
                initialize_dex(&config, &arb.domain_config, network_registry.as_ref()),
                initialize_dex(&config, &linea.domain_config, network_registry.as_ref()),
            )?;
            (primary, Some(observed), Some(arb), Some(linea))
        } else {
            (
                initialize_dex(&config, domain_config.as_ref(), network_registry.as_ref()).await?,
                None,
                None,
                None,
            )
        };
    let InitializedDex {
        mirror,
        stream,
        rpc: wallet_rpc,
        timings: dex_timings,
    } = initialized_dex;
    let initial_wallet_head = mirror.latest_head();
    let (receipt_heads, receipt_head_receiver) = tokio::sync::watch::channel(initial_wallet_head);
    let AlchemyDexStream {
        receiver: mut dex_receiver,
        task: mut dex_task,
    } = stream;
    emit_bootstrap_telemetry(
        &telemetry,
        &config,
        domain_config.as_ref(),
        bootstrap,
        dex_timings,
    );
    let pair = domain_config
        .snapshot()
        .pairs
        .iter()
        .find(|pair| pair.market_data_enabled)
        .context("balance synchronization requires one enabled pair")?;
    let binance_symbols = compiled_binance_runtime
        .as_ref()
        .map(|runtime| runtime.symbols.clone())
        .unwrap_or_else(|| domain_config.binance_symbols());
    ensure!(
        binance_symbols
            .iter()
            .any(|symbol| symbol == &pair.binance.symbol),
        "shared Binance runtime omitted execution symbol {}",
        pair.binance.symbol
    );
    if let Some(runtime) = compiled_binance_runtime.as_ref() {
        ensure!(
            runtime.stream_shards.len() == 1,
            "current production account requires one directly-polled Binance stream shard"
        );
        ensure!(
            runtime.stream_shards[0].symbols == binance_symbols,
            "compiled Binance stream shard and account symbol registry differ"
        );
        ensure!(
            runtime.executable_symbols.len() == 3
                && runtime.executable_symbols.contains(&pair.binance.symbol)
                && runtime.executable_symbols.contains("ESPUSDC")
                && runtime.executable_symbols.contains("ARBUSDC")
                && !runtime.executable_symbols.contains("USDCUSDT"),
            "compiled Binance capabilities must disable Linea stablecoin execution"
        );
    }
    let mut binance_account_client = BinanceAccountClient::from_env(&config)?;
    let startup_binance_clock_sync = binance_account_client.synchronize_clock_observed().await?;
    let mut user_data_stream =
        UserDataStream::connect(&config, startup_binance_clock_sync.offset_ms).await?;
    let shared_binance_account = binance_account_client
        .hydrate_symbols_after_subscription(binance_symbols.clone(), startup_binance_clock_sync)
        .await?;
    let esp_pair = shadow_strategy_plan
        .as_ref()
        .and_then(|strategy| strategy.domain_config.snapshot().pairs.first())
        .context("compiled ESP ESP strategy has no esp pair")?
        .clone();
    let arb_pair = arb_strategy_plan
        .as_ref()
        .and_then(|strategy| strategy.domain_config.snapshot().pairs.first())
        .context("compiled ARB strategy has no pair")?
        .clone();
    let linea_pair = linea_strategy_plan
        .as_ref()
        .and_then(|strategy| strategy.domain_config.snapshot().pairs.first())
        .context("compiled Linea strategy has no pair")?
        .clone();
    match shared_binance_account
        .symbol(&esp_pair.binance.symbol)
        .context("shared Binance account omitted the ESP esp symbol")
        .and_then(|state| validate_binance_readiness(&esp_pair, state))
    {
        Ok(readiness) => telemetry.emit(
            "live_readiness",
            serde_json::json!({
                "engine_id": config.engine_id,
                "stage": "binance_order_matrix",
                "pair_id": esp_pair.id,
                "network_id": "eip155:42161",
                "symbol": readiness.symbol,
                "buy_fee_bps": readiness.buy_fee_bps,
                "sell_fee_bps": readiness.sell_fee_bps,
                "validation_price": readiness.validation_price.to_string(),
                "validation_quantity": readiness.validation_quantity.to_string(),
                "configured_detector_notional": readiness.configured_detector_notional.to_string(),
                "effective_detector_notional": readiness.effective_detector_notional.to_string(),
                "request_fingerprints": readiness.request_fingerprints,
                "request_count": 4,
                "filters_ready": readiness.filters_ready,
                "external_mutation_authorized": readiness.external_mutation_authorized,
                "ready": true,
            }),
        ),
        Err(error) => {
            tracing::warn!(
                pair_id = esp_pair.id,
                error = %error,
                "ESP Binance readiness validation is incomplete; ESP fails closed"
            );
            telemetry.emit(
                "live_readiness",
                serde_json::json!({
                    "engine_id": config.engine_id,
                    "stage": "binance_order_matrix",
                    "pair_id": esp_pair.id,
                    "network_id": "eip155:42161",
                    "symbol": esp_pair.binance.symbol,
                    "external_mutation_authorized": false,
                    "ready": false,
                }),
            );
        }
    }
    match shared_binance_account
        .symbol(&arb_pair.binance.symbol)
        .context("shared Binance account omitted ARBUSDC")
        .and_then(|state| validate_binance_readiness(&arb_pair, state))
    {
        Ok(readiness) => telemetry.emit(
            "live_readiness",
            serde_json::json!({
                "engine_id": config.engine_id,
                "stage": "binance_order_matrix",
                "pair_id": arb_pair.id,
                "network_id": "eip155:42161",
                "symbol": readiness.symbol,
                "buy_fee_bps": readiness.buy_fee_bps,
                "sell_fee_bps": readiness.sell_fee_bps,
                "validation_price": readiness.validation_price.to_string(),
                "validation_quantity": readiness.validation_quantity.to_string(),
                "configured_detector_notional": readiness.configured_detector_notional.to_string(),
                "effective_detector_notional": readiness.effective_detector_notional.to_string(),
                "request_fingerprints": readiness.request_fingerprints,
                "request_count": 4,
                "filters_ready": readiness.filters_ready,
                "external_mutation_authorized": readiness.external_mutation_authorized,
                "ready": true,
            }),
        ),
        Err(error) => {
            tracing::warn!(pair_id = arb_pair.id, error = %error, "ARB Binance readiness is incomplete; ARB fails closed");
            telemetry.emit(
                "live_readiness",
                serde_json::json!({
                    "engine_id": config.engine_id,
                    "stage": "binance_order_matrix",
                    "pair_id": arb_pair.id,
                    "network_id": "eip155:42161",
                    "symbol": arb_pair.binance.symbol,
                    "external_mutation_authorized": false,
                    "ready": false,
                }),
            );
        }
    }
    match shared_binance_account
        .symbol(&linea_pair.binance.symbol)
        .context("shared Binance account omitted USDCUSDT")
        .and_then(|state| validate_binance_readiness(&linea_pair, state))
    {
        Ok(readiness) => telemetry.emit(
            "live_readiness",
            serde_json::json!({
                "engine_id": config.engine_id,
                "stage": "binance_order_matrix",
                "pair_id": linea_pair.id,
                "network_id": "eip155:59144",
                "symbol": readiness.symbol,
                "buy_fee_bps": readiness.buy_fee_bps,
                "sell_fee_bps": readiness.sell_fee_bps,
                "validation_price": readiness.validation_price.to_string(),
                "validation_quantity": readiness.validation_quantity.to_string(),
                "configured_detector_notional": readiness.configured_detector_notional.to_string(),
                "effective_detector_notional": readiness.effective_detector_notional.to_string(),
                "request_fingerprints": readiness.request_fingerprints,
                "request_count": 4,
                "filters_ready": readiness.filters_ready,
                "external_mutation_authorized": readiness.external_mutation_authorized,
                "ready": true,
            }),
        ),
        Err(error) => {
            tracing::warn!(pair_id = linea_pair.id, error = %error, "Linea Binance readiness is incomplete; Linea fails closed");
            telemetry.emit(
                "live_readiness",
                serde_json::json!({
                    "engine_id": config.engine_id,
                    "stage": "binance_order_matrix",
                    "pair_id": linea_pair.id,
                    "network_id": "eip155:59144",
                    "symbol": linea_pair.binance.symbol,
                    "external_mutation_authorized": false,
                    "ready": false,
                }),
            );
        }
    }
    let binance_account_generation = shared_binance_account.generation;
    let binance_account_snapshot_duration_us = shared_binance_account.account_snapshot_duration_us;
    let hydrated_binance_symbols: Vec<_> = shared_binance_account.symbols.keys().cloned().collect();
    let hydrated_binance_open_orders: usize = shared_binance_account
        .symbols
        .values()
        .map(|symbol| symbol.open_orders.len())
        .sum();
    let shared_binance_runtime = match compiled_binance_runtime.as_ref() {
        Some(runtime) => SharedBinanceRuntime::from_compiled(runtime, binance_account_generation)?,
        None => SharedBinanceRuntime::single_symbol(
            pair.binance.symbol.clone(),
            binance_account_generation,
        )?,
    };
    shared_binance_runtime.ensure_order_enabled(&pair.binance.symbol)?;
    shared_binance_runtime.ensure_order_enabled(&esp_pair.binance.symbol)?;
    shared_binance_runtime.ensure_order_enabled(&arb_pair.binance.symbol)?;
    let esp_symbol_state = shared_binance_account
        .symbol(&esp_pair.binance.symbol)
        .context("shared Binance account omitted ESPUSDC")?;
    let esp_buy_fee_bps = esp_symbol_state
        .commission
        .conservative_taker_fee_bps("BUY")?;
    let esp_sell_fee_bps = esp_symbol_state
        .commission
        .conservative_taker_fee_bps("SELL")?;
    ensure!(
        esp_symbol_state.symbol_rules.base_asset == esp_pair.binance.base_asset
            && esp_symbol_state.symbol_rules.quote_asset == esp_pair.binance.quote_asset,
        "ESPUSDC exchangeInfo assets differ from the ESP domain artifact"
    );
    let esp_execution_symbol_rules = esp_symbol_state
        .symbol_rules
        .with_compatible_price_step(
            Decimal::from_str(&esp_pair.binance.tick_size)
                .context("ESP ESP Binance tick_size is invalid")?,
        )
        .context("ESP ESP tick_size is incompatible with live PRICE_FILTER")?;
    ensure!(
        esp_symbol_state.symbol_rules.lot_size.step
            == Decimal::from_str(&esp_pair.binance.step_size)
                .context("ESP ESP Binance step_size is invalid")?,
        "ESP ESP step_size differs from live LOT_SIZE"
    );
    let arb_symbol_state = shared_binance_account
        .symbol(&arb_pair.binance.symbol)
        .context("shared Binance account omitted ARBUSDC")?;
    let arb_buy_fee_bps = arb_symbol_state
        .commission
        .conservative_taker_fee_bps("BUY")?;
    let arb_sell_fee_bps = arb_symbol_state
        .commission
        .conservative_taker_fee_bps("SELL")?;
    ensure!(
        arb_symbol_state.symbol_rules.base_asset == arb_pair.binance.base_asset
            && arb_symbol_state.symbol_rules.quote_asset == arb_pair.binance.quote_asset,
        "ARBUSDC exchangeInfo assets differ from the ARB domain artifact"
    );
    let arb_execution_symbol_rules = arb_symbol_state
        .symbol_rules
        .with_compatible_price_step(
            Decimal::from_str(&arb_pair.binance.tick_size)
                .context("ARB Binance tick_size is invalid")?,
        )
        .context("ARB tick_size is incompatible with live PRICE_FILTER")?;
    ensure!(
        arb_symbol_state.symbol_rules.lot_size.step
            == Decimal::from_str(&arb_pair.binance.step_size)
                .context("ARB Binance step_size is invalid")?,
        "ARB step_size differs from live LOT_SIZE"
    );
    let linea_symbol_state = shared_binance_account
        .symbol(&linea_pair.binance.symbol)
        .context("shared Binance account omitted USDCUSDT")?;
    let linea_buy_fee_bps = linea_symbol_state
        .commission
        .conservative_taker_fee_bps("BUY")?;
    let linea_sell_fee_bps = linea_symbol_state
        .commission
        .conservative_taker_fee_bps("SELL")?;
    ensure!(
        linea_symbol_state.symbol_rules.base_asset == linea_pair.binance.base_asset
            && linea_symbol_state.symbol_rules.quote_asset == linea_pair.binance.quote_asset,
        "USDCUSDT exchangeInfo assets differ from the Linea domain artifact"
    );
    linea_symbol_state
        .symbol_rules
        .with_compatible_price_step(
            Decimal::from_str(&linea_pair.binance.tick_size)
                .context("Linea Binance tick_size is invalid")?,
        )
        .context("Linea tick_size is incompatible with PRICE_FILTER")?;
    ensure!(
        linea_symbol_state.symbol_rules.lot_size.step
            == Decimal::from_str(&linea_pair.binance.step_size)
                .context("Linea Binance step_size is invalid")?,
        "Linea step_size differs from live LOT_SIZE"
    );
    let binance_account = shared_binance_account.into_symbol(&pair.binance.symbol)?;
    let binance_clock_sync_client = binance_account_client.clone();
    validate_binance_account(&binance_account)?;
    let binance_buy_fee_bps = binance_account
        .commission
        .conservative_taker_fee_bps("BUY")?;
    let binance_sell_fee_bps = binance_account
        .commission
        .conservative_taker_fee_bps("SELL")?;
    let mut binance_feed = if compiled_binance_runtime.is_some() {
        BookTickerFeed::new_shard_with_depth(
            &config,
            binance_symbols.clone(),
            pair.binance.symbol.clone(),
            binance_account_client.clone(),
        )?
    } else {
        BookTickerFeed::new_with_depth(
            &config,
            pair.binance.symbol.clone(),
            binance_account_client.clone(),
        )
    };
    ensure!(
        binance_account.symbol_rules.base_asset == pair.binance.base_asset
            && binance_account.symbol_rules.quote_asset == pair.binance.quote_asset,
        "Binance exchangeInfo assets {}/{} do not match domain assets {}/{}",
        binance_account.symbol_rules.base_asset,
        binance_account.symbol_rules.quote_asset,
        pair.binance.base_asset,
        pair.binance.quote_asset
    );
    let configured_binance_tick = Decimal::from_str(&pair.binance.tick_size)
        .context("domain Binance tick_size is invalid")?;
    let execution_symbol_rules = binance_account
        .symbol_rules
        .with_compatible_price_step(configured_binance_tick)
        .context("domain Binance tick_size is incompatible with live PRICE_FILTER")?;
    ensure!(
        binance_account.symbol_rules.lot_size.step
            == Decimal::from_str(&pair.binance.step_size)
                .context("domain Binance step_size is invalid")?,
        "domain Binance step_size differs from live LOT_SIZE"
    );
    let wld_detector_notional = validate_detector_control_notional(pair, &execution_symbol_rules)?;
    telemetry.emit(
        "live_readiness",
        serde_json::json!({
            "engine_id": config.engine_id,
            "stage": "detector_control_notional",
            "pair_id": pair.id,
            "network_id": "eip155:480",
            "symbol": pair.binance.symbol,
            "configured_detector_notional": wld_detector_notional.configured.to_string(),
            "validation_price": wld_detector_notional.validation_price.to_string(),
            "validation_quantity": wld_detector_notional.validation_quantity.to_string(),
            "effective_detector_notional": wld_detector_notional.effective.to_string(),
            "filters_ready": true,
            "hot_path_logic_added": false,
            "ready": true,
        }),
    );
    let gas_price_symbol = pair
        .chain
        .gas_price_binance_symbol
        .clone()
        .context("domain config has no Binance gas-price symbol")?;
    let mut gas_price_feed = BookTickerFeed::new(&config, gas_price_symbol.clone());
    let commission_asset = pair
        .binance
        .commission_asset
        .clone()
        .context("domain config has no Binance commission asset")?;
    let commission_price_symbol = pair
        .binance
        .commission_price_binance_symbol
        .clone()
        .context("domain config has no Binance commission-price symbol")?;
    let mut commission_price_feed = BookTickerFeed::new(&config, commission_price_symbol.clone());
    let capital_coins = binance_account_client.all_coin_information().await?;
    let rebalance_tracker = if pair.rebalance.enabled {
        match validate_rebalance_readiness(&esp_pair, &capital_coins) {
            Ok(readiness) => telemetry.emit(
                "live_readiness",
                serde_json::json!({
                    "engine_id": config.engine_id,
                    "stage": "arbitrum_rebalance_routes",
                    "pair_id": esp_pair.id,
                    "network_id": "eip155:42161",
                    "binance_network": readiness.network,
                    "asset_count": readiness.asset_count,
                    "direct_route_count": readiness.direct_route_count,
                    "deposit_enabled_assets": readiness.deposit_enabled_assets,
                    "withdrawal_enabled_assets": readiness.withdrawal_enabled_assets,
                    "external_mutation_authorized": readiness.external_mutation_authorized,
                    "ready": readiness.ready,
                }),
            ),
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "ESP Arbitrum rebalance readiness is incomplete; rebalance remains disabled"
                );
                telemetry.emit(
                    "live_readiness",
                    serde_json::json!({
                        "engine_id": config.engine_id,
                        "stage": "arbitrum_rebalance_routes",
                        "pair_id": esp_pair.id,
                        "network_id": "eip155:42161",
                        "external_mutation_authorized": false,
                        "ready": false,
                    }),
                );
            }
        }
        let mut routes = BTreeMap::new();
        for token in [&pair.token_a, &pair.token_b] {
            let capital = select_capital_routes(
                &capital_coins,
                &token.symbol,
                &pair.chain.binance_network_name,
                "OPTIMISM",
            )?;
            routes.insert(
                token.symbol.clone(),
                route_candidates_from_capital(&capital, token.decimals, pair.chain.chain_id)?,
            );
        }
        RebalanceTracker::new(pair, routes)?
    } else {
        RebalanceTracker::disabled()
    };
    let portfolio_runtime = compiled_portfolio_runtime
        .context("run requires the compiled portfolio portfolio runtime plan")?;
    let portfolio_catalog = Arc::new(PortfolioCatalog::from_compiled(&portfolio_runtime)?);
    ensure!(
        portfolio_catalog.live_rebalance_adapter() == "world_chain_v12_parity",
        "live WLD rebalance is not behind the reviewed v12 parity adapter"
    );
    let esp_rebalance_tracker =
        if portfolio_catalog.allocator_mode() == CompiledCapitalAllocatorMode::FullLive {
            ensure!(
                esp_pair.rebalance.enabled,
                "live rebalance allocator requires the ESP pair rebalance policy"
            );
            let mut routes = BTreeMap::new();
            for token in [&esp_pair.token_a, &esp_pair.token_b] {
                let capital = select_capital_routes(
                    &capital_coins,
                    &token.symbol,
                    &esp_pair.chain.binance_network_name,
                    "OPTIMISM",
                )?;
                let direct = capital
                    .direct
                    .as_ref()
                    .filter(|route| route.network == esp_pair.chain.binance_network_name)
                    .context("rebalance direct Arbitrum capital route is absent")?;
                ensure!(
                    capital.deposit_all_enabled
                        && capital.withdrawal_all_enabled
                        && direct.deposit_available()
                        && direct.withdrawal_available(),
                    "rebalance direct Arbitrum capital route is not fully available"
                );
                routes.insert(
                    token.symbol.clone(),
                    route_candidates_from_capital(
                        &CapitalRouteState {
                            coin: capital.coin.clone(),
                            deposit_all_enabled: capital.deposit_all_enabled,
                            withdrawal_all_enabled: capital.withdrawal_all_enabled,
                            direct: Some(direct.clone()),
                            fallback: None,
                        },
                        token.decimals,
                        ARBITRUM_CHAIN_ID,
                    )?,
                );
            }
            RebalanceTracker::new(&esp_pair, routes)?
        } else {
            RebalanceTracker::disabled()
        };
    let arb_rebalance_tracker =
        if portfolio_catalog.allocator_mode() == CompiledCapitalAllocatorMode::FullLive {
            ensure!(
                arb_pair.rebalance.enabled,
                "live rebalance allocator requires the ARB pair rebalance policy"
            );
            match validate_rebalance_readiness(&arb_pair, &capital_coins) {
                Ok(readiness) => telemetry.emit(
                    "live_readiness",
                    serde_json::json!({
                        "engine_id": config.engine_id,
                        "stage": "arbitrum_rebalance_routes",
                        "pair_id": arb_pair.id,
                        "network_id": "eip155:42161",
                        "binance_network": readiness.network,
                        "asset_count": readiness.asset_count,
                        "direct_route_count": readiness.direct_route_count,
                        "deposit_enabled_assets": readiness.deposit_enabled_assets,
                        "withdrawal_enabled_assets": readiness.withdrawal_enabled_assets,
                        "external_mutation_authorized": readiness.external_mutation_authorized,
                        "ready": readiness.ready,
                    }),
                ),
                Err(error) => anyhow::bail!("ARB rebalance readiness failed: {error:#}"),
            }
            let token = &arb_pair.token_b;
            let capital = select_capital_routes(
                &capital_coins,
                &token.symbol,
                &arb_pair.chain.binance_network_name,
                "OPTIMISM",
            )?;
            let direct = capital
                .direct
                .as_ref()
                .filter(|route| route.network == arb_pair.chain.binance_network_name)
                .context("ARB direct Arbitrum capital route is absent")?;
            ensure!(
                capital.deposit_all_enabled
                    && capital.withdrawal_all_enabled
                    && direct.deposit_available()
                    && direct.withdrawal_available(),
                "ARB direct Arbitrum capital route is not fully available"
            );
            let routes = BTreeMap::from([(
                token.symbol.clone(),
                route_candidates_from_capital(
                    &CapitalRouteState {
                        coin: capital.coin.clone(),
                        deposit_all_enabled: capital.deposit_all_enabled,
                        withdrawal_all_enabled: capital.withdrawal_all_enabled,
                        direct: Some(direct.clone()),
                        fallback: None,
                    },
                    token.decimals,
                    ARBITRUM_CHAIN_ID,
                )?,
            )]);
            RebalanceTracker::new_for_tokens(&arb_pair, routes, [&arb_pair.token_b.symbol])?
        } else {
            RebalanceTracker::disabled()
        };
    let linea_rebalance_tracker = if portfolio_catalog.allocator_mode()
        == CompiledCapitalAllocatorMode::FullLive
        && linea_pair.rebalance.enabled
    {
        let mut routes = BTreeMap::new();
        for token in [&linea_pair.token_a, &linea_pair.token_b] {
            let capital = select_capital_routes(
                &capital_coins,
                &token.symbol,
                &linea_pair.chain.binance_network_name,
                "OPTIMISM",
            )?;
            let fallback = capital
                .fallback
                .as_ref()
                .filter(|route| route.network == "OPTIMISM")
                .context("Linea Optimism capital fallback is absent")?;
            ensure!(
                capital.deposit_all_enabled
                    && capital.withdrawal_all_enabled
                    && fallback.deposit_available()
                    && fallback.withdrawal_available(),
                "Linea Optimism capital fallback is not fully available"
            );
            routes.insert(
                token.symbol.clone(),
                route_candidates_from_capital(
                    &CapitalRouteState {
                        coin: capital.coin.clone(),
                        deposit_all_enabled: capital.deposit_all_enabled,
                        withdrawal_all_enabled: capital.withdrawal_all_enabled,
                        direct: None,
                        fallback: Some(fallback.clone()),
                    },
                    token.decimals,
                    LINEA_CHAIN_ID,
                )?,
            );
        }
        telemetry.emit(
            "live_readiness",
            serde_json::json!({
                "engine_id": config.engine_id,
                "stage": "linea_rebalance_routes",
                "pair_id": linea_pair.id,
                "network_id": "eip155:59144",
                "binance_network": "OPTIMISM",
                "asset_count": 2,
                "direct_route_count": 0,
                "bridge_route_count": 2,
                "deposit_enabled_assets": 2,
                "withdrawal_enabled_assets": 2,
                "external_mutation_authorized": true,
                "ready": true,
            }),
        );
        RebalanceTracker::new(&linea_pair, routes)?
    } else {
        RebalanceTracker::disabled()
    };
    let wallet_address = config.evm_wallet_address.trim();
    ensure!(
        !wallet_address.is_empty(),
        "run requires EVM_WALLET_ADDRESS"
    );
    let wallet_owner = wallet_address
        .parse::<Address>()
        .context("run requires a valid EVM_WALLET_ADDRESS")?;
    let wallet_chain_id = wallet_rpc.chain_id().await?;
    ensure!(
        wallet_chain_id == pair.chain.chain_id,
        "wallet RPC returned chain id {wallet_chain_id}, expected {}",
        pair.chain.chain_id
    );
    let optimism_endpoint = std::env::var(OPTIMISM_RPC_URL_ENV).with_context(|| {
        format!("required environment variable {OPTIMISM_RPC_URL_ENV} is not set")
    })?;
    let optimism_rpc = JsonRpcClient::new(optimism_endpoint)?;
    let mut gas_balance_sources = if let Some(registry) = network_registry.as_ref() {
        registry
            .runtimes()
            .map(|runtime| {
                EvmGasBalanceSource::trading(
                    runtime.plan().network_id.as_str().to_owned(),
                    runtime.plan().chain_id,
                    runtime.plan().wallet_location_id.as_str().to_owned(),
                    runtime.rpc().clone(),
                )
            })
            .collect::<Vec<_>>()
    } else {
        vec![EvmGasBalanceSource::trading(
            arb_bot::telemetry::network_id(wallet_chain_id),
            wallet_chain_id,
            arb_bot::telemetry::wallet_location_id(wallet_chain_id),
            wallet_rpc.clone(),
        )]
    };
    gas_balance_sources.push(EvmGasBalanceSource::bridge(
        "eip155:10".to_owned(),
        OPTIMISM_CHAIN_ID,
        "eip155:10:evm-wallet:primary".to_owned(),
        optimism_rpc.clone(),
    ));
    let resource_balance_source_count = gas_balance_sources.len() + 1;
    let resource_balance_monitor = ResourceBalanceMonitor::new(
        telemetry.clone(),
        config.engine_id.clone(),
        wallet_owner,
        gas_balance_sources,
        binance_account_client.clone(),
    )?;
    let resource_balance_task = tokio::spawn(resource_balance_monitor.run());
    tracing::info!(
        interval_seconds = RESOURCE_BALANCE_INTERVAL.as_secs(),
        resource_count = resource_balance_source_count,
        consumption_window_hours = 24,
        consumption_model = "sum_of_balance_decreases_excluding_refills",
        readiness_gate = false,
        "background resource balance monitor started"
    );
    let wallet_tokens = vec![
        TokenBalanceRequest {
            symbol: pair.token_a.symbol.clone(),
            contract: pair
                .token_a
                .contract
                .parse()
                .context("configured token_a address is invalid")?,
        },
        TokenBalanceRequest {
            symbol: pair.token_b.symbol.clone(),
            contract: pair
                .token_b
                .contract
                .parse()
                .context("configured token_b address is invalid")?,
        },
    ];
    let portfolio_wallet_snapshots = if let Some(registry) = network_registry.as_ref() {
        hydrate_network_wallet_registries(registry, wallet_owner, &telemetry, &config.engine_id)
            .await?
    } else {
        Vec::new()
    };
    let (live_chain_readiness_probe, initial_chain_readiness_status) =
        if let Some(registry) = network_registry.as_ref() {
            let runtime = registry.get_by_chain_id(42_161)?;
            let snapshot = portfolio_wallet_snapshots
                .iter()
                .find(|snapshot| snapshot.chain_id == 42_161)
                .context("ESP Arbitrum wallet snapshot is missing")?;
            let probe = ChainReadinessProbe::new(&esp_pair, runtime, wallet_owner)?;
            match inspect_chain_readiness(&esp_pair, runtime, snapshot).await {
                Ok(readiness) => {
                    emit_chain_readiness(
                        &telemetry,
                        &config.engine_id,
                        &esp_pair,
                        &readiness,
                        "startup",
                    );
                    (Some(probe), Some(readiness.status()))
                }
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        "ESP Arbitrum chain readiness is incomplete; ESP fails closed"
                    );
                    emit_chain_readiness_failure(
                        &telemetry,
                        &config.engine_id,
                        &esp_pair,
                        "startup",
                        &error,
                    );
                    (Some(probe), Some(ChainReadinessStatus::ProbeFailed))
                }
            }
        } else {
            (None, None)
        };
    let esp_execution_ready = Arc::new(AtomicBool::new(matches!(
        initial_chain_readiness_status,
        Some(ChainReadinessStatus::Observed { ready: true, .. })
    )));
    let esp_market_data_ready = Arc::new(AtomicBool::new(true));
    let (arb_chain_readiness_probe, arb_initial_chain_readiness_status) = if let Some(registry) =
        network_registry.as_ref()
    {
        let runtime = registry.get_by_chain_id(ARBITRUM_CHAIN_ID)?;
        let snapshot = portfolio_wallet_snapshots
            .iter()
            .find(|snapshot| snapshot.chain_id == ARBITRUM_CHAIN_ID)
            .context("ARB Arbitrum wallet snapshot is missing")?;
        let probe = ChainReadinessProbe::new(&arb_pair, runtime, wallet_owner)?;
        match inspect_chain_readiness(&arb_pair, runtime, snapshot).await {
            Ok(readiness) => {
                emit_chain_readiness(
                    &telemetry,
                    &config.engine_id,
                    &arb_pair,
                    &readiness,
                    "startup",
                );
                (Some(probe), Some(readiness.status()))
            }
            Err(error) => {
                tracing::warn!(error = %error, "ARB chain readiness is incomplete; ARB fails closed");
                emit_chain_readiness_failure(
                    &telemetry,
                    &config.engine_id,
                    &arb_pair,
                    "startup",
                    &error,
                );
                (Some(probe), Some(ChainReadinessStatus::ProbeFailed))
            }
        }
    } else {
        (None, None)
    };
    let arb_execution_ready = Arc::new(AtomicBool::new(matches!(
        arb_initial_chain_readiness_status,
        Some(ChainReadinessStatus::Observed { ready: true, .. })
    )));
    let arb_market_data_ready = Arc::new(AtomicBool::new(true));
    let (
        linea_chain_readiness_probe,
        linea_initial_chain_readiness_status,
        _linea_allowance_mutations_ready,
    ) = if linea_pair.execution_enabled
        && let Some(registry) = network_registry.as_ref()
    {
        let runtime = registry.get_by_chain_id(LINEA_CHAIN_ID)?;
        let snapshot = portfolio_wallet_snapshots
            .iter()
            .find(|snapshot| snapshot.chain_id == LINEA_CHAIN_ID)
            .context("Linea wallet snapshot is missing")?;
        let probe = ChainReadinessProbe::new(&linea_pair, runtime, wallet_owner)?;
        match inspect_chain_readiness(&linea_pair, runtime, snapshot).await {
            Ok(readiness) => {
                emit_chain_readiness(
                    &telemetry,
                    &config.engine_id,
                    &linea_pair,
                    &readiness,
                    "startup",
                );
                let allowance_mutations_ready = readiness.allowance_mutations_ready();
                (
                    Some(probe),
                    Some(readiness.status()),
                    allowance_mutations_ready,
                )
            }
            Err(error) => {
                tracing::warn!(error = %error, "Linea chain readiness is incomplete; Linea fails closed");
                emit_chain_readiness_failure(
                    &telemetry,
                    &config.engine_id,
                    &linea_pair,
                    "startup",
                    &error,
                );
                (Some(probe), Some(ChainReadinessStatus::ProbeFailed), false)
            }
        }
    } else {
        (None, None, false)
    };
    let linea_execution_ready = Arc::new(AtomicBool::new(matches!(
        linea_initial_chain_readiness_status,
        Some(ChainReadinessStatus::Observed { ready: true, .. })
    )));
    let linea_market_data_ready = Arc::new(AtomicBool::new(true));
    let mut binance_asset_symbols = compiled_binance_runtime
        .as_ref()
        .map(|runtime| runtime.asset_symbols.clone())
        .unwrap_or_else(|| {
            vec![
                pair.binance.quote_asset.clone(),
                pair.binance.base_asset.clone(),
                commission_asset.clone(),
            ]
        });
    if !binance_asset_symbols
        .iter()
        .any(|asset| asset == &commission_asset)
    {
        binance_asset_symbols.push(commission_asset.clone());
    }
    binance_asset_symbols.sort();
    binance_asset_symbols.dedup();
    let binance_assets: Vec<_> = binance_asset_symbols
        .iter()
        .map(|asset| Arc::<str>::from(asset.as_str()))
        .collect();
    let wallet_reads = network_registry
        .as_ref()
        .map(|registry| {
            registry
                .get_by_chain_id(wallet_chain_id)
                .map(|runtime| WalletReadClient::Coordinated(runtime.reads().clone()))
        })
        .transpose()?
        .unwrap_or_else(|| WalletReadClient::Direct(wallet_rpc.clone()));
    let initial_wallet_balances = match &wallet_reads {
        WalletReadClient::Direct(rpc) => {
            fetch_wallet_snapshot(
                rpc,
                wallet_owner,
                wallet_chain_id,
                &wallet_tokens,
                initial_wallet_head,
            )
            .await?
        }
        WalletReadClient::Coordinated(reads) => {
            fetch_wallet_snapshot_coordinated(
                reads,
                wallet_owner,
                wallet_chain_id,
                &wallet_tokens,
                initial_wallet_head,
            )
            .await?
        }
    };
    let capital_policy = portfolio_catalog.capital_policy().cloned();
    let maximum_esp = capital_policy
        .as_ref()
        .filter(|policy| policy.external_mutation_authorized)
        .map(|policy| {
            rebalance_base_units_to_decimal(policy.maximum_token_b_debit, esp_pair.token_b.decimals)
        })
        .transpose()?
        .unwrap_or(Decimal::ZERO);
    let maximum_arb = capital_policy
        .as_ref()
        .filter(|policy| policy.external_mutation_authorized)
        .and_then(|policy| policy.additional_tokens.get("ARB"))
        .map(|policy| {
            rebalance_base_units_to_decimal(policy.maximum_debit, arb_pair.token_b.decimals)
        })
        .transpose()?
        .unwrap_or(Decimal::ZERO);
    let rebalance_runtime_limits = RebalanceRuntimeLimits {
        maximum_wld: config.rebalance_max_wld_amount,
        maximum_usdc: config.rebalance_max_usdc_amount,
        maximum_esp,
        maximum_arb,
        operation_timeout: Duration::from_secs(config.rebalance_executor_timeout_seconds),
    };
    let (mut full_rebalance_executor, rebalance_recovery_operation, quarantined_rebalance_tokens) =
        if config.rebalance_execution_mode == "full_live" {
            let wallet = EvmWallet::from_env()?;
            ensure!(
                wallet.address() == wallet_owner,
                "full rebalance signer does not match EVM_WALLET_ADDRESS"
            );
            let transaction_journal_path =
                std::env::var(WALLET_JOURNAL_PATH_ENV).with_context(|| {
                    format!("required environment variable {WALLET_JOURNAL_PATH_ENV} is not set")
                })?;
            let subaccount_email = std::env::var("BINANCE_SUBACCOUNT_EMAIL")
                .context("full rebalance requires BINANCE_SUBACCOUNT_EMAIL")?;
            let treasury_client = BinanceAccountClient::from_treasury_env(&config)?;
            let rebalance_journal_started_at = Instant::now();
            let mut executor = RebalanceExecutor::hydrate(
                binance_account_client.clone(),
                treasury_client,
                subaccount_email,
                AcrossClient::new(&config)?,
                wallet_rpc.clone(),
                optimism_rpc.clone(),
                BTreeMap::new(),
                wallet,
                config.rebalance_executor_journal_path.clone(),
                transaction_journal_path.into(),
                Some(reviewed_rebalance_nonce_collision(wallet_owner)?),
                rebalance_runtime_limits.clone(),
            )
            .await?;
            executor.set_capital_policy(capital_policy.clone())?;
            executor.set_telemetry(telemetry.clone(), config.engine_id.clone());
            match executor.reconcile_next_across_fill_quarantine().await {
                Ok(Some(operation)) => tracing::warn!(
                    operation_id = %operation.intent.operation_id,
                    progress = ?operation.progress,
                    "recovered a proven Across fill for asynchronous journal completion"
                ),
                Ok(None) => {}
                Err(error) => tracing::error!(
                    error = %error,
                    "quarantined Across timeout did not pass reconciliation-only recovery; token remains isolated"
                ),
            }
            match executor
                .reconcile_next_consumed_nonce_deposit_quarantine()
                .await
            {
                Ok(Some(operation)) => tracing::warn!(
                    operation_id = %operation.intent.operation_id,
                    progress = ?operation.progress,
                    "recovered a proven consumed-nonce deposit for asynchronous journal completion"
                ),
                Ok(None) => {}
                Err(error) => tracing::error!(
                    error = %error,
                    "consumed-nonce deposit quarantine did not pass recovery proof; token remains isolated"
                ),
            }
            executor.reopen_next_retryable_quarantine()?;
            telemetry.emit(
                "runtime_journal_recovery",
                serde_json::json!({
                    "engine_id": config.engine_id,
                    "owner": "rebalance_saga",
                    "journal_scope": "rebalance",
                    "duration_us": rebalance_journal_started_at.elapsed().as_micros(),
                    "active_operation_count": usize::from(executor.active_operation()?.is_some()),
                    "outcome": "success",
                }),
            );
            let recovery_operation = executor.active_operation()?.cloned();
            let quarantined_tokens = executor
                .quarantined_operations()
                .map(|operation| {
                    (
                        rebalance_target(operation),
                        operation.intent.token_symbol.clone(),
                        match &operation.progress {
                            arb_bot::rebalance::RebalanceExecutionProgress::Quarantined {
                                reason,
                            } => reason.clone(),
                            _ => unreachable!(
                                "quarantined operation iterator returned another state"
                            ),
                        },
                    )
                })
                .collect::<Vec<_>>();
            if let Some(operation) = recovery_operation.as_ref() {
                tracing::warn!(
                    operation_id = %operation.intent.operation_id,
                    progress = ?operation.progress,
                    "recovered active rebalance operation for asynchronous runtime recovery"
                );
            }
            (Some(executor), recovery_operation, quarantined_tokens)
        } else {
            (None, None, Vec::new())
        };
    let mut rebalance_recovery_operation = rebalance_recovery_operation;
    let mut quarantined_rebalance_tokens = quarantined_rebalance_tokens;
    let user_data_subscription_id = user_data_stream.subscription_id();
    let multiplexed_binance_api = user_data_stream.api();
    tracing::info!(
        binance_account_snapshot_generation = binance_account_generation,
        binance_hydrated_symbols = ?hydrated_binance_symbols,
        binance_open_orders = hydrated_binance_open_orders,
        binance_locked_assets = binance_account.account
            .balances
            .iter()
            .filter(|balance| !balance.locked.is_zero())
            .count(),
        "shared Binance account generation materialized; User Data and all symbols use one owner"
    );
    let initial_binance_balances = binance_snapshot(
        &binance_account.account,
        &binance_assets,
        binance_account_snapshot_duration_us,
    );
    let esp_initialized = shadow_initialized_dex
        .as_ref()
        .context("ESP ESP execution has no initialized Arbitrum DEX runtime")?;
    let esp_wallet_rpc = esp_initialized.rpc.clone();
    let esp_initial_head = esp_initialized.mirror.latest_head();
    let (esp_receipt_heads, esp_receipt_head_receiver) =
        tokio::sync::watch::channel(esp_initial_head);
    let esp_initial_wallet_balances = portfolio_wallet_snapshots
        .iter()
        .find(|snapshot| snapshot.chain_id == 42_161)
        .context("ESP ESP execution has no Arbitrum wallet snapshot")?
        .clone();
    let arb_initialized = arb_initialized_dex
        .as_ref()
        .context("ARB execution has no initialized Arbitrum DEX runtime")?;
    let arb_initial_head = arb_initialized.mirror.latest_head();
    let (arb_receipt_heads, _arb_receipt_head_receiver) =
        tokio::sync::watch::channel(arb_initial_head);
    let arb_initial_wallet_balances = esp_initial_wallet_balances.clone();
    let linea_initialized = linea_initialized_dex
        .as_ref()
        .context("Linea observe-only strategy has no initialized DEX runtime")?;
    let linea_initial_head = linea_initialized.mirror.latest_head();
    let (linea_receipt_heads, _linea_receipt_head_receiver) =
        tokio::sync::watch::channel(linea_initial_head);
    let linea_initial_wallet_balances = portfolio_wallet_snapshots
        .iter()
        .find(|snapshot| snapshot.chain_id == LINEA_CHAIN_ID)
        .context("Linea execution has no wallet snapshot")?
        .clone();
    let entry_preflight = EntryPreflightHandle::default();
    let primary_pretrade_cost_telemetry = PreTradeCostTelemetry::default();
    let esp_pretrade_cost_telemetry = PreTradeCostTelemetry::default();
    let arb_pretrade_cost_telemetry = PreTradeCostTelemetry::default();
    let linea_pretrade_cost_telemetry = PreTradeCostTelemetry::default();
    let mut shared_arbitrum_rebalance_owner_attached = false;
    let live_trade_runtime = if config.arbitrage_execution_mode == "full_live" {
        ensure!(
            domain_config.snapshot().live_trading_enabled
                && pair.execution_enabled
                && esp_pair.execution_enabled
                && arb_pair.execution_enabled
                && !linea_pair.execution_enabled
                && !linea_pair.rebalance.enabled,
            "composed live arbitrage requires three live pairs and a stopped Linea pair"
        );
        ensure!(
            esp_execution_ready.load(Ordering::Acquire),
            "ESP Arbitrum chain readiness must pass before allowance mutations"
        );
        validate_production_switchback()?;
        tracing::info!(
            pair_id = ESP_SWITCHBACK_PAIR_ID,
            experiment_id = ESP_SWITCHBACK_EXPERIMENT_ID,
            seed_version = ESP_SWITCHBACK_SEED_VERSION,
            hash_algorithm = ESP_SWITCHBACK_HASH_ALGORITHM,
            starts_at_unix_seconds = ESP_SWITCHBACK_START_UNIX_SECONDS,
            ends_at_unix_seconds = ESP_SWITCHBACK_END_UNIX_SECONDS,
            block_duration_seconds = ESP_SWITCHBACK_BLOCK_DURATION_SECONDS,
            control = "dex_first",
            treatment = "concurrent_hedged",
            sizing_policy = "production_adaptive_6_to_200_usdc",
            "ESP concurrent switchback full-live execution configured"
        );
        telemetry.emit(
            "arbitrage_switchback_configured",
            serde_json::json!({
                "engine_id": &config.engine_id,
                "pair_id": ESP_SWITCHBACK_PAIR_ID,
                "experiment_id": ESP_SWITCHBACK_EXPERIMENT_ID,
                "seed_version": ESP_SWITCHBACK_SEED_VERSION,
                "hash_algorithm": ESP_SWITCHBACK_HASH_ALGORITHM,
                "starts_at_unix_seconds": ESP_SWITCHBACK_START_UNIX_SECONDS,
                "ends_at_unix_seconds": ESP_SWITCHBACK_END_UNIX_SECONDS,
                "block_duration_seconds": ESP_SWITCHBACK_BLOCK_DURATION_SECONDS,
                "control": "dex_first",
                "treatment": "concurrent_hedged",
                "assignment_probability_bps": 5_000,
                "sizing_policy": "production_adaptive_6_to_200_usdc",
                "live_mutation_authorized": true,
            }),
        );
        let account_id = compiled_binance_runtime
            .as_ref()
            .context("ESP live execution requires a compiled Binance account")?
            .account_id
            .as_str()
            .to_owned();
        let strategy_plans = &hot_path_dependencies
            .as_ref()
            .context("ESP live execution requires compiled strategy ownership")?
            .plan()
            .strategies;
        let scope_for = |symbol: &str| -> anyhow::Result<TradeJournalScope> {
            let strategy = strategy_plans
                .iter()
                .find(|strategy| strategy.execute && strategy.symbol == symbol)
                .with_context(|| format!("ESP live execution has no {symbol} strategy"))?;
            let network = network_registry
                .as_ref()
                .context("ESP live execution requires the network registry")?
                .runtimes()
                .find(|runtime| runtime.plan().network_id == strategy.network_id)
                .context("ESP executable strategy has no EVM execution owner")?
                .plan();
            Ok(TradeJournalScope {
                schema_version: TradeJournalScope::SCHEMA_VERSION,
                account_id: account_id.clone(),
                network_id: network.network_id.as_str().to_owned(),
                chain_id: network.chain_id,
                wallet_id: network.wallet_location_id.as_str().to_owned(),
                strategy_id: strategy.strategy_id.as_str().to_owned(),
                symbol: strategy.symbol.clone(),
            })
        };
        let trade_journal_scope = scope_for("WLDUSDC")?;
        let esp_journal_scope = scope_for("ESPUSDC")?;
        let arb_journal_scope = scope_for("ARBUSDC")?;
        ensure!(
            EvmJournalScope {
                schema_version: EvmJournalScope::SCHEMA_VERSION,
                network_id: esp_journal_scope.network_id.clone(),
                wallet_id: esp_journal_scope.wallet_id.clone(),
                strategy_id: esp_journal_scope.strategy_id.clone(),
            } == esp_evm_journal_scope(ARBITRUM_CHAIN_ID),
            "compiled ESP journal identity differs from the production rebalance identity"
        );
        let wallet = EvmWallet::from_env()?;
        ensure!(
            wallet.address() == wallet_owner,
            "live arbitrage signer does not match EVM_WALLET_ADDRESS"
        );
        let wallet_journal_path =
            std::env::var(ARBITRAGE_WALLET_JOURNAL_PATH_ENV).with_context(|| {
                format!(
                    "required environment variable {ARBITRAGE_WALLET_JOURNAL_PATH_ENV} is not set"
                )
            })?;
        let esp_wallet_journal_path =
            std::env::var(ARBITRAGE_ARBITRUM_WALLET_JOURNAL_PATH_ENV).with_context(|| {
                format!(
                    "required environment variable {ARBITRAGE_ARBITRUM_WALLET_JOURNAL_PATH_ENV} is not set"
                )
            })?;
        let binance_journal_path = std::env::var(ARBITRAGE_BINANCE_ORDER_JOURNAL_PATH_ENV)
            .with_context(|| {
                format!(
                    "required environment variable {ARBITRAGE_BINANCE_ORDER_JOURNAL_PATH_ENV} is not set"
                )
            })?;
        let evm_journal_started_at = Instant::now();
        let mut dex_executor = DexExecutor::hydrate(
            wallet_rpc.clone(),
            wallet,
            wallet_chain_id,
            wallet_journal_path.into(),
        )
        .await?;
        dex_executor.set_journal_scope(EvmJournalScope {
            schema_version: EvmJournalScope::SCHEMA_VERSION,
            network_id: trade_journal_scope.network_id.clone(),
            wallet_id: trade_journal_scope.wallet_id.clone(),
            strategy_id: trade_journal_scope.strategy_id.clone(),
        })?;
        telemetry.emit(
            "runtime_journal_recovery",
            serde_json::json!({
                "engine_id": config.engine_id,
                "owner": "evm_execution",
                "journal_scope": arb_bot::telemetry::execution_lane_id(wallet_chain_id),
                "network_id": arb_bot::telemetry::network_id(wallet_chain_id),
                "wallet_id": arb_bot::telemetry::PRIMARY_EVM_WALLET_ID,
                "duration_us": evm_journal_started_at.elapsed().as_micros(),
                "outcome": "success",
            }),
        );
        dex_executor.set_receipt_heads(receipt_head_receiver.clone());
        let mut allowance_requirements = Vec::new();
        for token in &initial_wallet_balances.token_balances {
            let required = token.base_units.max(U256::ONE);
            if pair.dex.allowed_providers.contains(&DexProvider::UniswapV3) {
                allowance_requirements.push(AllowanceRequirement {
                    operation_id: format!("rustarb-setup-v3-{}", token.symbol),
                    protocol: DexProtocol::UniswapV3,
                    token: token.contract,
                    router: pair
                        .chain
                        .uniswap_v3_router_address
                        .as_deref()
                        .context("live V3 router is missing")?
                        .parse()
                        .context("live V3 router is invalid")?,
                    required,
                });
            }
            if pair
                .dex
                .pancakeswap_v3
                .as_ref()
                .is_some_and(|config| config.pools.iter().any(|pool| pool.selection_enabled))
            {
                allowance_requirements.push(AllowanceRequirement {
                    operation_id: format!("rustarb-setup-pancake-v3-{}", token.symbol),
                    protocol: DexProtocol::PancakeSwapV3,
                    token: token.contract,
                    router: pair
                        .chain
                        .pancakeswap_v3_router_address
                        .as_deref()
                        .context("live Pancake V3 router is missing")?
                        .parse()
                        .context("live Pancake V3 router is invalid")?,
                    required,
                });
            }
            if pair.dex.allowed_providers.contains(&DexProvider::UniswapV4) {
                allowance_requirements.push(AllowanceRequirement {
                    operation_id: format!("rustarb-setup-v4-{}", token.symbol),
                    protocol: DexProtocol::UniswapV4,
                    token: token.contract,
                    router: pair
                        .chain
                        .uniswap_v4_router_address
                        .as_deref()
                        .context("live V4 router is missing")?
                        .parse()
                        .context("live V4 router is invalid")?,
                    required,
                });
            }
        }
        dex_executor
            .prepare_and_lock_allowances(&allowance_requirements)
            .await?;
        let execution_latency_telemetry =
            ExecutionLatencyTelemetry::new(telemetry.clone(), config.engine_id.clone());
        dex_executor.set_latency_telemetry(execution_latency_telemetry.clone());
        dex_executor.set_pretrade_cost_telemetry(primary_pretrade_cost_telemetry.clone());
        dex_executor.spawn_pretrade_cost_receipt_bootstrap();
        let dex_service = DexExecutionService::spawn(
            dex_executor,
            config.arbitrage_leg_execution_channel_capacity,
        )?;
        let esp_evm_journal_started_at = Instant::now();
        let arbitrum_max_fee_headroom_bps = esp_pair
            .full_live_policy
            .as_ref()
            .map(|policy| policy.arbitrum_max_fee_headroom_bps)
            .context("Arbitrum gas policy is missing")?;
        let mut esp_dex_executor = DexExecutor::hydrate_with_gas_policy(
            esp_wallet_rpc.clone(),
            EvmWallet::from_env()?,
            42_161,
            esp_wallet_journal_path.into(),
            CompiledNetworkGasPolicy::ArbitrumOne {
                requires_fresh_rpc_gas_price: true,
                max_priority_fee_per_gas_wei: 0,
                max_fee_headroom_bps: arbitrum_max_fee_headroom_bps,
                includes_l1_fee: false,
            },
        )
        .await?;
        esp_dex_executor.set_journal_scope(EvmJournalScope {
            schema_version: EvmJournalScope::SCHEMA_VERSION,
            network_id: esp_journal_scope.network_id.clone(),
            wallet_id: esp_journal_scope.wallet_id.clone(),
            strategy_id: esp_journal_scope.strategy_id.clone(),
        })?;
        esp_dex_executor.set_receipt_heads(esp_receipt_head_receiver.clone());
        let esp_router = esp_pair
            .chain
            .uniswap_v3_router_address
            .as_deref()
            .context("ESP Arbitrum V3 router is missing")?
            .parse()
            .context("ESP Arbitrum V3 router is invalid")?;
        let token_a = esp_initial_wallet_balances
            .token_balances
            .iter()
            .find(|token| token.symbol.as_ref() == esp_pair.token_a.symbol)
            .context("ESP startup wallet snapshot is missing token_a")?;
        let token_b = esp_initial_wallet_balances
            .token_balances
            .iter()
            .find(|token| token.symbol.as_ref() == esp_pair.token_b.symbol)
            .context("ESP startup wallet snapshot is missing token_b")?;
        let arb_token = arb_initial_wallet_balances
            .token_balances
            .iter()
            .find(|token| token.symbol.as_ref() == arb_pair.token_b.symbol)
            .context("ARB startup wallet snapshot is missing ARB")?;
        {
            let mut esp_allowances = [
                (token_a, U256::MAX),
                (token_b, U256::MAX),
                (arb_token, U256::MAX),
            ]
            .into_iter()
            .map(|(token, required)| AllowanceRequirement {
                operation_id: allowance_operation_id(token.symbol.as_ref()),
                protocol: DexProtocol::UniswapV3,
                token: token.contract,
                router: esp_router,
                required,
            })
            .collect::<Vec<_>>();
            if arb_pair
                .dex
                .pancakeswap_v3
                .as_ref()
                .is_some_and(|config| config.pools.iter().any(|pool| pool.selection_enabled))
            {
                let pancake_router = arb_pair
                    .chain
                    .pancakeswap_v3_router_address
                    .as_deref()
                    .context("ARB Pancake V3 router is missing")?
                    .parse()
                    .context("ARB Pancake V3 router is invalid")?;
                for token in [token_a, arb_token] {
                    esp_allowances.push(AllowanceRequirement {
                        operation_id: allowance_operation_id(token.symbol.as_ref()),
                        protocol: DexProtocol::PancakeSwapV3,
                        token: token.contract,
                        router: pancake_router,
                        required: U256::MAX,
                    });
                }
            }
            let camelot_live = arb_pair
                .dex
                .camelot_v3
                .as_ref()
                .is_some_and(|config| config.pools.iter().any(|pool| pool.selection_enabled));
            if camelot_live {
                let camelot_router = arb_pair
                    .chain
                    .camelot_v3_router_address
                    .as_deref()
                    .context("ARB Camelot V3 router is missing")?
                    .parse()
                    .context("ARB Camelot V3 router is invalid")?;
                for token in [token_a, arb_token] {
                    esp_allowances.push(AllowanceRequirement {
                        operation_id: allowance_operation_id(token.symbol.as_ref()),
                        protocol: DexProtocol::CamelotV3,
                        token: token.contract,
                        router: camelot_router,
                        required: U256::MAX,
                    });
                }
            }
            esp_dex_executor
                .prepare_and_lock_allowances(&esp_allowances)
                .await?;
            if camelot_live {
                esp_dex_executor.enable_camelot_submissions_after_allowance_lock()?;
            }
        }
        esp_dex_executor.set_latency_telemetry(execution_latency_telemetry.clone());
        esp_dex_executor.set_pretrade_cost_telemetry(esp_pretrade_cost_telemetry.clone());
        esp_dex_executor.spawn_pretrade_cost_receipt_bootstrap();
        let esp_dex_service = Arc::new(DexExecutionService::spawn(
            esp_dex_executor,
            config.arbitrage_leg_execution_channel_capacity,
        )?);
        if let Some(executor) = full_rebalance_executor.as_mut() {
            executor
                .attach_arbitrum_execution_owner(
                    esp_dex_service.evm_execution_owner(),
                    esp_wallet_rpc.clone(),
                )
                .await?;
            shared_arbitrum_rebalance_owner_attached = true;
            match executor.reconcile_next_arbitrum_deposit_quarantine().await {
                Ok(Some(operation)) => {
                    rebalance_recovery_operation = Some(operation);
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        "quarantined Arbitrum deposit did not pass reconciliation-only recovery; token remains isolated"
                    );
                }
            }
            match executor
                .reconcile_next_post_credit_settlement_quarantine()
                .await
            {
                Ok(Some(operation)) => tracing::warn!(
                    operation_id = %operation.intent.operation_id,
                    progress = ?operation.progress,
                    "completed a proven post-credit settlement quarantine during startup"
                ),
                Ok(None) => {}
                Err(error) => tracing::error!(
                    error = %error,
                    "post-credit settlement quarantine did not pass read-only reconciliation; token remains isolated"
                ),
            }
            quarantined_rebalance_tokens = executor
                .quarantined_operations()
                .map(|operation| {
                    (
                        rebalance_target(operation),
                        operation.intent.token_symbol.clone(),
                        match &operation.progress {
                            arb_bot::rebalance::RebalanceExecutionProgress::Quarantined {
                                reason,
                            } => reason.clone(),
                            _ => unreachable!(
                                "quarantined operation iterator returned another state"
                            ),
                        },
                    )
                })
                .collect();
        }
        telemetry.emit(
            "runtime_journal_recovery",
            serde_json::json!({
                "engine_id": config.engine_id,
                "owner": "evm_execution",
                "journal_scope": arb_bot::telemetry::execution_lane_id(42_161),
                "network_id": arb_bot::telemetry::network_id(42_161),
                "wallet_id": arb_bot::telemetry::PRIMARY_EVM_WALLET_ID,
                "duration_us": esp_evm_journal_started_at.elapsed().as_micros(),
                "outcome": "success",
            }),
        );
        let binance_service = BinanceExecutionService::spawn_multi_scoped_instrumented(
            multiplexed_binance_api.clone(),
            binance_journal_path.into(),
            config.arbitrage_leg_execution_channel_capacity,
            execution_latency_telemetry,
            BTreeMap::from([
                (
                    trade_journal_scope.symbol.clone(),
                    BinanceOrderJournalScope {
                        schema_version: BinanceOrderJournalScope::SCHEMA_VERSION,
                        account_id: trade_journal_scope.account_id.clone(),
                        strategy_id: trade_journal_scope.strategy_id.clone(),
                    },
                ),
                (
                    esp_journal_scope.symbol.clone(),
                    BinanceOrderJournalScope {
                        schema_version: BinanceOrderJournalScope::SCHEMA_VERSION,
                        account_id: esp_journal_scope.account_id.clone(),
                        strategy_id: esp_journal_scope.strategy_id.clone(),
                    },
                ),
                (
                    arb_journal_scope.symbol.clone(),
                    BinanceOrderJournalScope {
                        schema_version: BinanceOrderJournalScope::SCHEMA_VERSION,
                        account_id: arb_journal_scope.account_id.clone(),
                        strategy_id: arb_journal_scope.strategy_id.clone(),
                    },
                ),
            ]),
        )
        .await?;
        let binance_service = Arc::new(binance_service);
        let (dex_revert_diagnostics, dex_revert_diagnostic_task) = dex_revert_diagnostic_channel(
            wallet_rpc.clone(),
            telemetry.clone(),
            config.engine_id.clone(),
            DEX_REVERT_DIAGNOSTIC_CHANNEL_CAPACITY,
        );
        let (esp_dex_revert_diagnostics, esp_dex_revert_diagnostic_task) =
            dex_revert_diagnostic_channel(
                esp_wallet_rpc.clone(),
                telemetry.clone(),
                config.engine_id.clone(),
                DEX_REVERT_DIAGNOSTIC_CHANNEL_CAPACITY,
            );
        let (arb_dex_revert_diagnostics, arb_dex_revert_diagnostic_task) =
            dex_revert_diagnostic_channel(
                esp_wallet_rpc.clone(),
                telemetry.clone(),
                config.engine_id.clone(),
                DEX_REVERT_DIAGNOSTIC_CHANNEL_CAPACITY,
            );
        let primary_executor = Arc::new(ComposedLiveLegExecutor::new(
            dex_service,
            Arc::clone(&binance_service),
            ComposedLiveLegExecutorConfig {
                rules: execution_symbol_rules.clone(),
                base_asset: pair.binance.base_asset.clone(),
                base_decimals: pair.token_b.decimals,
                quote_asset: pair.binance.quote_asset.clone(),
                quote_decimals: pair.token_a.decimals,
                commission_asset: commission_asset.clone(),
                commission_price_symbol: commission_price_symbol.clone(),
                market_state: entry_preflight.clone(),
                dex_revert_diagnostics,
                telemetry: telemetry.clone(),
                engine_id: config.engine_id.clone(),
            },
        )?);
        let esp_executor = Arc::new(ComposedLiveLegExecutor::new(
            Arc::clone(&esp_dex_service),
            Arc::clone(&binance_service),
            ComposedLiveLegExecutorConfig {
                rules: esp_execution_symbol_rules.clone(),
                base_asset: esp_pair.binance.base_asset.clone(),
                base_decimals: esp_pair.token_b.decimals,
                quote_asset: esp_pair.binance.quote_asset.clone(),
                quote_decimals: esp_pair.token_a.decimals,
                commission_asset: commission_asset.clone(),
                commission_price_symbol: commission_price_symbol.clone(),
                market_state: entry_preflight.clone(),
                dex_revert_diagnostics: esp_dex_revert_diagnostics,
                telemetry: telemetry.clone(),
                engine_id: config.engine_id.clone(),
            },
        )?);
        let arb_executor = Arc::new(ComposedLiveLegExecutor::new(
            Arc::clone(&esp_dex_service),
            Arc::clone(&binance_service),
            ComposedLiveLegExecutorConfig {
                rules: arb_execution_symbol_rules.clone(),
                base_asset: arb_pair.binance.base_asset.clone(),
                base_decimals: arb_pair.token_b.decimals,
                quote_asset: arb_pair.binance.quote_asset.clone(),
                quote_decimals: arb_pair.token_a.decimals,
                commission_asset: commission_asset.clone(),
                commission_price_symbol: commission_price_symbol.clone(),
                market_state: entry_preflight.clone(),
                dex_revert_diagnostics: arb_dex_revert_diagnostics,
                telemetry: telemetry.clone(),
                engine_id: config.engine_id.clone(),
            },
        )?);
        let executor = RoutedLiveLegExecutor::new(BTreeMap::from([
            (pair.id.clone(), primary_executor),
            (esp_pair.id.clone(), esp_executor),
            (arb_pair.id.clone(), arb_executor),
        ]))?;
        let full_live_sizing = esp_pair
            .adaptive_sizing
            .limits()
            .context("full-live adaptive sizing limits are missing")?;
        let arb_live_sizing = arb_pair
            .adaptive_sizing
            .limits()
            .context("ARB full-live adaptive sizing limits are missing")?;
        let parse_live_amount = |value: &str, label: &str| {
            value
                .parse::<u128>()
                .with_context(|| format!("ESP {label} is invalid"))
        };
        let (handle, task, events) = live_trade_channel(
            &config.arbitrage_trade_journal_path,
            executor,
            telemetry.clone(),
            config.engine_id.clone(),
            LiveRiskLimits {
                entry_stop_file: config.arbitrage_entry_stop_file.clone(),
                entry_preflight: entry_preflight.clone(),
                binance_symbol: pair.binance.symbol.clone(),
                binance_base_decimals: pair.token_b.decimals,
                journal_scope: trade_journal_scope,
                pair_policies: BTreeMap::from([
                    (
                        esp_pair.id.clone(),
                        LivePairPolicy {
                            journal_scope: esp_journal_scope,
                            binance_base_decimals: esp_pair.token_b.decimals,
                            maximum_trade_notional_token_a_base_units: parse_live_amount(
                                full_live_sizing.max_trade_notional,
                                "maximum trade notional",
                            )?,
                            maximum_unhedged_notional_token_a_base_units: parse_live_amount(
                                full_live_sizing.max_unhedged_notional,
                                "maximum unhedged notional",
                            )?,
                            maximum_realized_loss_token_a_base_units: parse_live_amount(
                                full_live_sizing.max_recovery_loss,
                                "maximum recovery loss",
                            )?,
                            maximum_concurrent_trades: 1,
                            readiness: Arc::clone(&esp_execution_ready),
                            market_data_readiness: Arc::clone(&esp_market_data_ready),
                        },
                    ),
                    (
                        arb_pair.id.clone(),
                        LivePairPolicy {
                            journal_scope: arb_journal_scope,
                            binance_base_decimals: arb_pair.token_b.decimals,
                            maximum_trade_notional_token_a_base_units: parse_live_amount(
                                arb_live_sizing.max_trade_notional,
                                "ARB maximum trade notional",
                            )?,
                            maximum_unhedged_notional_token_a_base_units: parse_live_amount(
                                arb_live_sizing.max_unhedged_notional,
                                "ARB maximum unhedged notional",
                            )?,
                            maximum_realized_loss_token_a_base_units: parse_live_amount(
                                arb_live_sizing.max_recovery_loss,
                                "ARB maximum recovery loss",
                            )?,
                            maximum_concurrent_trades: 1,
                            readiness: Arc::clone(&arb_execution_ready),
                            market_data_readiness: Arc::clone(&arb_market_data_ready),
                        },
                    ),
                ]),
            },
        )?;
        let diagnostic_task = tokio::spawn(async move {
            tokio::join!(
                dex_revert_diagnostic_task.run(),
                esp_dex_revert_diagnostic_task.run(),
                arb_dex_revert_diagnostic_task.run()
            );
            Ok::<(), anyhow::Error>(())
        });
        Some((handle, tokio::spawn(task.run()), events, diagnostic_task))
    } else {
        None
    };
    let BalanceSync {
        receiver: mut balance_receiver,
        wallet_heads,
        binance_task: mut binance_balance_task,
        wallet_task: mut wallet_balance_task,
    } = spawn_balance_sync(
        binance_account_client,
        binance_symbols.clone(),
        binance_assets,
        Duration::from_millis(config.balance_sync_interval_ms),
        wallet_reads,
        wallet_owner,
        wallet_chain_id,
        wallet_tokens,
        initial_wallet_head,
        config.balance_event_channel_capacity,
    );
    let esp_wallet_tokens = vec![
        TokenBalanceRequest {
            symbol: esp_pair.token_a.symbol.clone(),
            contract: esp_pair
                .token_a
                .contract
                .parse()
                .context("ESP token_a address is invalid")?,
        },
        TokenBalanceRequest {
            symbol: esp_pair.token_b.symbol.clone(),
            contract: esp_pair
                .token_b
                .contract
                .parse()
                .context("ESP token_b address is invalid")?,
        },
        TokenBalanceRequest {
            symbol: arb_pair.token_b.symbol.clone(),
            contract: arb_pair
                .token_b
                .contract
                .parse()
                .context("ARB token address is invalid")?,
        },
    ];
    let esp_wallet_reads = network_registry
        .as_ref()
        .context("ESP requires the Arbitrum network runtime")?
        .get_by_chain_id(42_161)?
        .reads()
        .clone();
    let arb_bot::balances::WalletBalanceSync {
        receiver: mut esp_wallet_balance_receiver,
        heads: esp_wallet_heads,
        task: mut esp_wallet_balance_task,
    } = spawn_wallet_balance_sync(
        WalletReadClient::Coordinated(esp_wallet_reads),
        wallet_owner,
        42_161,
        esp_wallet_tokens,
        esp_initial_head,
        config.balance_event_channel_capacity,
    );
    let linea_wallet_tokens = vec![
        TokenBalanceRequest {
            symbol: linea_pair.token_a.symbol.clone(),
            contract: linea_pair
                .token_a
                .contract
                .parse()
                .context("Linea token_a address is invalid")?,
        },
        TokenBalanceRequest {
            symbol: linea_pair.token_b.symbol.clone(),
            contract: linea_pair
                .token_b
                .contract
                .parse()
                .context("Linea token_b address is invalid")?,
        },
    ];
    let linea_wallet_reads = network_registry
        .as_ref()
        .context("Linea requires its network runtime")?
        .get_by_chain_id(LINEA_CHAIN_ID)?
        .reads()
        .clone();
    let arb_bot::balances::WalletBalanceSync {
        receiver: mut linea_wallet_balance_receiver,
        heads: linea_wallet_heads,
        task: mut linea_wallet_balance_task,
    } = spawn_wallet_balance_sync(
        WalletReadClient::Coordinated(linea_wallet_reads),
        wallet_owner,
        LINEA_CHAIN_ID,
        linea_wallet_tokens,
        linea_initial_head,
        config.balance_event_channel_capacity,
    );

    let paper_mode = match config.arbitrage_execution_mode.as_str() {
        "disabled" => None,
        "paper_dex_first" => Some(ExecutionMode::DexFirst),
        "paper_concurrent_hedged" => Some(ExecutionMode::ConcurrentHedged),
        "full_live" => None,
        _ => unreachable!("AppConfig validation rejects unknown arbitrage modes"),
    };
    let (
        paper_trades,
        mut paper_trade_task,
        mut paper_trade_events,
        mut dex_revert_diagnostic_task,
    ) = if let Some(runtime) = live_trade_runtime {
        let (handle, task, events, diagnostic_task) = runtime;
        (Some(handle), Some(task), events, Some(diagnostic_task))
    } else if let Some(mode) = paper_mode {
        let (handle, task, events) = paper_trade_channel(
            &config.arbitrage_trade_journal_path,
            mode,
            telemetry.clone(),
            config.engine_id.clone(),
        )?;
        (Some(handle), Some(tokio::spawn(task.run())), events, None)
    } else {
        let (_event_sender, events) = tokio::sync::mpsc::unbounded_channel();
        (None, None, events, None)
    };
    let shared_inventory = SharedInventoryReservations::default();
    let (portfolio_allocator, portfolio_allocator_task) = capital_allocator_channel(
        portfolio_catalog.as_ref(),
        telemetry.clone(),
        config.engine_id.clone(),
    );
    let (primary_engine, hot_telemetry) = TradingEngine::new(
        config.clone(),
        Arc::clone(&domain_config),
        mirror,
        telemetry.clone(),
        V12RebalanceParityAdapter::new(rebalance_tracker),
        arb_bot::engine::TradingExecutionHandles {
            paper_trades: paper_trades.clone(),
            entry_preflight: entry_preflight.clone(),
            binance_asset_decimals: compiled_binance_runtime
                .as_ref()
                .map(|runtime| runtime.asset_decimals.clone())
                .unwrap_or_default(),
            portfolio_catalog: Arc::clone(&portfolio_catalog),
            inventory: shared_inventory.clone(),
            capital_allocator: portfolio_allocator.clone(),
            pretrade_cost_telemetry: primary_pretrade_cost_telemetry,
        },
        BinanceFeeBps {
            buy: binance_buy_fee_bps,
            sell: binance_sell_fee_bps,
        },
    )?;
    let dependencies = hot_path_dependencies
        .context("run requires the compiled hot-path hot-path runtime plan")?;
    let shadow_plan =
        shadow_strategy_plan.context("compiled ESP hot path has no ESP esp strategy")?;
    let InitializedDex {
        mirror: shadow_mirror,
        stream: shadow_stream,
        rpc: _shadow_wallet_rpc,
        timings: _shadow_dex_timings,
    } = shadow_initialized_dex
        .context("compiled ESP ESP strategy has no initialized DEX runtime")?;
    let shadow_pair = shadow_plan
        .domain_config
        .snapshot()
        .pairs
        .first()
        .context("compiled ESP ESP strategy has no projected pair")?;
    let (mut esp_engine, esp_hot_telemetry) = TradingEngine::new(
        config.clone(),
        Arc::new(shadow_plan.domain_config.clone()),
        shadow_mirror,
        telemetry.clone(),
        V12RebalanceParityAdapter::new(esp_rebalance_tracker),
        arb_bot::engine::TradingExecutionHandles {
            paper_trades: paper_trades.clone(),
            entry_preflight: entry_preflight.clone(),
            binance_asset_decimals: compiled_binance_runtime
                .as_ref()
                .map(|runtime| runtime.asset_decimals.clone())
                .unwrap_or_default(),
            portfolio_catalog: Arc::clone(&portfolio_catalog),
            inventory: shared_inventory.clone(),
            capital_allocator: portfolio_allocator.clone(),
            pretrade_cost_telemetry: esp_pretrade_cost_telemetry,
        },
        BinanceFeeBps {
            buy: esp_buy_fee_bps,
            sell: esp_sell_fee_bps,
        },
    )?;
    let arb_plan = arb_strategy_plan.context("compiled ARB hot path has no strategy")?;
    let InitializedDex {
        mirror: arb_mirror,
        stream: arb_stream,
        rpc: _arb_wallet_rpc,
        timings: _arb_dex_timings,
    } = arb_initialized_dex.context("compiled ARB strategy has no initialized DEX runtime")?;
    let (mut arb_engine, arb_hot_telemetry) = TradingEngine::new(
        config.clone(),
        Arc::new(arb_plan.domain_config.clone()),
        arb_mirror,
        telemetry.clone(),
        V12RebalanceParityAdapter::new(arb_rebalance_tracker),
        arb_bot::engine::TradingExecutionHandles {
            paper_trades: paper_trades.clone(),
            entry_preflight: entry_preflight.clone(),
            binance_asset_decimals: compiled_binance_runtime
                .as_ref()
                .map(|runtime| runtime.asset_decimals.clone())
                .unwrap_or_default(),
            portfolio_catalog: Arc::clone(&portfolio_catalog),
            inventory: shared_inventory.clone(),
            capital_allocator: portfolio_allocator.clone(),
            pretrade_cost_telemetry: arb_pretrade_cost_telemetry,
        },
        BinanceFeeBps {
            buy: arb_buy_fee_bps,
            sell: arb_sell_fee_bps,
        },
    )?;
    let linea_plan = linea_strategy_plan.context("compiled Linea hot path has no strategy")?;
    let InitializedDex {
        mirror: linea_mirror,
        stream: linea_stream,
        rpc: _linea_wallet_rpc,
        timings: _linea_dex_timings,
    } = linea_initialized_dex.context("compiled Linea strategy has no initialized DEX runtime")?;
    let (mut linea_engine, linea_hot_telemetry) = TradingEngine::new(
        config.clone(),
        Arc::new(linea_plan.domain_config.clone()),
        linea_mirror,
        telemetry.clone(),
        V12RebalanceParityAdapter::new(linea_rebalance_tracker),
        arb_bot::engine::TradingExecutionHandles {
            paper_trades,
            entry_preflight: entry_preflight.clone(),
            binance_asset_decimals: compiled_binance_runtime
                .as_ref()
                .map(|runtime| runtime.asset_decimals.clone())
                .unwrap_or_default(),
            portfolio_catalog: Arc::clone(&portfolio_catalog),
            inventory: shared_inventory.clone(),
            capital_allocator: portfolio_allocator,
            pretrade_cost_telemetry: linea_pretrade_cost_telemetry,
        },
        BinanceFeeBps {
            buy: linea_buy_fee_bps,
            sell: linea_sell_fee_bps,
        },
    )?;
    let root_supervisor = RootSupervisorPolicy::new(
        dependencies
            .plan()
            .strategies
            .iter()
            .map(|strategy| {
                let chain_id = strategy
                    .domain_config
                    .snapshot()
                    .pairs
                    .first()
                    .context("supervised strategy has no projected pair")?
                    .chain
                    .chain_id;
                Ok(DependencyScope {
                    binance_account_id: PRIMARY_BINANCE_ACCOUNT_ID.to_owned(),
                    network_id: strategy.network_id.as_str().to_owned(),
                    strategy_id: strategy.strategy_id.as_str().to_owned(),
                    execution_lane_id: execution_lane_id(chain_id),
                    execution_enabled: strategy.execute,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?,
    )?;
    let mut engine = HotPathDecisionOwner::new_with_externally_routed_observers(
        primary_engine,
        Vec::new(),
        vec![linea_plan.strategy_id.clone()],
        dependencies,
    )?;
    tracing::info!(
        binance_account_id = PRIMARY_BINANCE_ACCOUNT_ID,
        live_strategy_id = %engine.strategy_id().as_str(),
        esp_strategy_id = %shadow_plan.strategy_id.as_str(),
        arb_strategy_id = %arb_plan.strategy_id.as_str(),
        linea_strategy_id = %linea_plan.strategy_id.as_str(),
        esp_network_id = %shadow_plan.network_id.as_str(),
        esp_execution_lane_id = %execution_lane_id(shadow_pair.chain.chain_id),
        shared_inventory_owner = true,
        shared_binance_order_owner = true,
        esp_rebalance_mutation_enabled =
            portfolio_catalog.allocator_mode() == CompiledCapitalAllocatorMode::FullLive,
        esp_external_mutation_authorized = true,
        root_supervisor_policy = "dependency_scoped_v1",
        "Arbitrum full-live production strategies configured"
    );
    if portfolio_catalog.allocator_mode() == CompiledCapitalAllocatorMode::FullLive {
        ensure!(
            shared_arbitrum_rebalance_owner_attached && full_rebalance_executor.is_some(),
            "live rebalance has no shared Arbitrum EVM execution owner"
        );
    }
    let policy = esp_pair
        .full_live_policy
        .as_ref()
        .context("ESP full-live policy is missing")?;
    tracing::info!(
            pair_id = esp_pair.id,
            strategy_id = %shadow_plan.strategy_id.as_str(),
            network_id = %shadow_plan.network_id.as_str(),
            chain_id = esp_pair.chain.chain_id,
            production_approval_actor = policy.production_approval_actor,
            production_approval_recorded_at_utc = policy.production_approval_recorded_at_utc,
            max_trade_notional_token_a_base_units = esp_pair
                .adaptive_sizing
                .limits()
                .map(|limits| limits.max_trade_notional),
            gas_policy = "fresh_eth_gas_price_fail_closed_no_world_fallback",
            allowance_policy = "max_uint256_then_locked",
            rebalance_policy = "continuous_direct_arbitrum_per_operation_caps",
            allocator_mode = ?portfolio_catalog.allocator_mode(),
            binance_network = policy.rebalance_binance_network,
            maximum_token_a_debit_base_units =
                policy.maximum_rebalance_token_a_debit_base_units,
            maximum_token_b_debit_base_units =
                policy.maximum_rebalance_token_b_debit_base_units,
            maximum_token_a_fee_base_units =
                policy.maximum_rebalance_token_a_fee_base_units,
            maximum_token_b_fee_base_units =
                policy.maximum_rebalance_token_b_fee_base_units,
            maximum_concurrent_transfers = 1,
            maximum_unknown_reconciliation_queries =
                policy.maximum_unknown_reconciliation_queries,
            direct_route_only = policy.direct_route_only,
            bridge_mutations_enabled = policy.bridge_mutations_enabled,
            shared_arbitrum_evm_owner = shared_arbitrum_rebalance_owner_attached,
            external_mutation_authorized = true,
            "ESP Arbitrum full-live execution configured"
    );
    let arb_policy = arb_pair
        .full_live_policy
        .as_ref()
        .context("ARB full-live policy is missing")?;
    tracing::info!(
        pair_id = arb_pair.id,
        strategy_id = %arb_plan.strategy_id.as_str(),
        production_approval_actor = arb_policy.production_approval_actor,
        production_approval_recorded_at_utc = arb_policy.production_approval_recorded_at_utc,
        max_trade_notional_token_a_base_units = arb_pair
            .adaptive_sizing
            .limits()
            .map(|limits| limits.max_trade_notional),
        shared_arbitrum_evm_owner = shared_arbitrum_rebalance_owner_attached,
        external_mutation_authorized = true,
        "ARB Arbitrum full-live execution configured"
    );
    tracing::info!(
        pair_id = linea_pair.id,
        strategy_id = %linea_plan.strategy_id.as_str(),
        network_id = %linea_plan.network_id.as_str(),
        chain_id = linea_pair.chain.chain_id,
        observe = true,
        plan = true,
        execute = false,
        rebalance = false,
        external_mutation_authorized = false,
        "Linea Lynex strategy is stopped and retained for read-only telemetry"
    );
    let AlchemyDexStream {
        receiver: mut shadow_dex_receiver,
        task: mut shadow_dex_task,
    } = shadow_stream;
    let AlchemyDexStream {
        receiver: mut arb_dex_receiver,
        task: mut arb_dex_task,
    } = arb_stream;
    let AlchemyDexStream {
        receiver: mut linea_dex_receiver,
        task: mut linea_dex_task,
    } = linea_stream;
    engine.on_binance_clock_sync(binance_account.clock_sync);
    esp_engine.on_binance_clock_sync(binance_account.clock_sync);
    arb_engine.on_binance_clock_sync(binance_account.clock_sync);
    linea_engine.on_binance_clock_sync(binance_account.clock_sync);
    let hot_telemetry_task = tokio::spawn(hot_telemetry.run());
    let portfolio_allocator_task = tokio::spawn(portfolio_allocator_task.run());
    let esp_hot_telemetry_task = tokio::spawn(esp_hot_telemetry.run());
    let arb_hot_telemetry_task = tokio::spawn(arb_hot_telemetry.run());
    let linea_hot_telemetry_task = tokio::spawn(linea_hot_telemetry.run());
    let live_chain_readiness_task = live_chain_readiness_probe.map(|probe| {
        tokio::spawn(run_chain_readiness_refresh(
            probe,
            telemetry.clone(),
            config.engine_id.clone(),
            esp_pair.clone(),
            initial_chain_readiness_status,
            Arc::clone(&esp_execution_ready),
        ))
    });
    let arb_chain_readiness_task = arb_chain_readiness_probe.map(|probe| {
        tokio::spawn(run_chain_readiness_refresh(
            probe,
            telemetry.clone(),
            config.engine_id.clone(),
            arb_pair.clone(),
            arb_initial_chain_readiness_status,
            Arc::clone(&arb_execution_ready),
        ))
    });
    let linea_chain_readiness_task = linea_chain_readiness_probe.map(|probe| {
        tokio::spawn(run_chain_readiness_refresh(
            probe,
            telemetry.clone(),
            config.engine_id.clone(),
            linea_pair.clone(),
            linea_initial_chain_readiness_status,
            Arc::clone(&linea_execution_ready),
        ))
    });
    let (binance_clock_sync_sender, mut binance_clock_sync_receiver) =
        tokio::sync::mpsc::channel(4);
    let binance_clock_sync_task = tokio::spawn(run_binance_clock_sync(
        binance_clock_sync_client,
        binance_clock_sync_sender,
    ));
    let mut binance_clock_sync_running = true;
    let (rebalance_sender, mut rebalance_receiver, mut rebalance_task, rebalance_risk_receiver) =
        if let Some(mut executor) = full_rebalance_executor.take() {
            let recover_on_start = rebalance_recovery_operation.is_some();
            let recovery_target = rebalance_recovery_operation
                .as_ref()
                .map(rebalance_target)
                .unwrap_or(RebalanceExecutionTarget::Primary);
            let (request_sender, mut request_receiver) = tokio::sync::mpsc::channel(1);
            let (result_sender, result_receiver) = tokio::sync::mpsc::channel(1);
            let (risk_sender, risk_receiver) =
                tokio::sync::watch::channel(executor.rebalance_risk()?);
            let rebalance_telemetry = telemetry.clone();
            let rebalance_engine_id = config.engine_id.clone();
            let task = tokio::spawn(async move {
                emit_rebalance_risk(&rebalance_telemetry, &rebalance_engine_id, &executor);
                if recover_on_start {
                    let mut current_target = recovery_target;
                    loop {
                        let saga_started_at = Instant::now();
                        let result = recover_rebalance_with_quote_retries(&mut executor).await;
                        let blocked_token = if let Err(error) = &result {
                            executor
                                .quarantine_active_operation(error)?
                                .map(|operation| operation.intent.token_symbol)
                        } else {
                            None
                        };
                        emit_rebalance_saga(
                            &rebalance_telemetry,
                            &rebalance_engine_id,
                            current_target,
                            &result,
                            &executor,
                            saga_started_at,
                            true,
                        );
                        emit_rebalance_risk(&rebalance_telemetry, &rebalance_engine_id, &executor);
                        risk_sender.send_replace(executor.rebalance_risk()?);
                        let next_recovery = executor.reopen_next_retryable_quarantine()?;
                        let active_operation_after = next_recovery.is_some();
                        let following_target = next_recovery.as_ref().map(rebalance_target);
                        if result_sender
                            .send(RebalanceExecutorEvent::Recovery {
                                target: current_target,
                                result,
                                active_operation_after,
                                blocked_token,
                                recovery_started: None,
                                next_recovery: next_recovery.map(Box::new),
                            })
                            .await
                            .is_err()
                        {
                            return Ok::<(), anyhow::Error>(());
                        }
                        let Some(target) = following_target else {
                            break;
                        };
                        current_target = target;
                    }
                }
                while let Some(command) = request_receiver.recv().await {
                    match command {
                        RebalanceExecutorCommand::Execute { target, request } => {
                            let saga_started_at = Instant::now();
                            let result =
                                execute_rebalance_with_quote_retries(&mut executor, *request).await;
                            let blocked_token = if let Err(error) = &result {
                                executor
                                    .quarantine_active_operation(error)?
                                    .map(|operation| operation.intent.token_symbol)
                            } else {
                                None
                            };
                            emit_rebalance_saga(
                                &rebalance_telemetry,
                                &rebalance_engine_id,
                                target,
                                &result,
                                &executor,
                                saga_started_at,
                                false,
                            );
                            emit_rebalance_risk(
                                &rebalance_telemetry,
                                &rebalance_engine_id,
                                &executor,
                            );
                            risk_sender.send_replace(executor.rebalance_risk()?);
                            let active_operation_after = executor.active_operation()?.is_some();
                            if result_sender
                                .send(RebalanceExecutorEvent::Execution {
                                    target,
                                    result,
                                    active_operation_after,
                                    blocked_token,
                                })
                                .await
                                .is_err()
                            {
                                return Ok::<(), anyhow::Error>(());
                            }
                        }
                        RebalanceExecutorCommand::ReconcileAcross => {
                            let reconciliation_started_at = Instant::now();
                            match executor
                                .reconcile_next_post_credit_settlement_quarantine()
                                .await
                            {
                                Ok(Some(completed)) => {
                                    let target = rebalance_target(&completed);
                                    let result = Ok(completed);
                                    emit_rebalance_saga(
                                        &rebalance_telemetry,
                                        &rebalance_engine_id,
                                        target,
                                        &result,
                                        &executor,
                                        reconciliation_started_at,
                                        true,
                                    );
                                    emit_rebalance_risk(
                                        &rebalance_telemetry,
                                        &rebalance_engine_id,
                                        &executor,
                                    );
                                    risk_sender.send_replace(executor.rebalance_risk()?);
                                    if result_sender
                                        .send(RebalanceExecutorEvent::Recovery {
                                            target,
                                            result,
                                            active_operation_after: false,
                                            blocked_token: None,
                                            recovery_started: None,
                                            next_recovery: None,
                                        })
                                        .await
                                        .is_err()
                                    {
                                        return Ok::<(), anyhow::Error>(());
                                    }
                                    continue;
                                }
                                Ok(None) => {}
                                Err(error) => {
                                    tracing::warn!(
                                        error = %format!("{error:#}"),
                                        retry_after_seconds =
                                            ACROSS_RECONCILIATION_INTERVAL.as_secs(),
                                        "post-credit settlement quarantine reconciliation will be retried"
                                    );
                                }
                            }
                            match executor
                                .reconcile_next_consumed_nonce_deposit_quarantine()
                                .await
                            {
                                Ok(Some(reopened)) => {
                                    let target = rebalance_target(&reopened);
                                    let result =
                                        recover_rebalance_with_quote_retries(&mut executor).await;
                                    let blocked_token = if let Err(error) = &result {
                                        let quarantine_reason = format!(
                                            "consumed-nonce deposit recovery attempt failed: {error:#}"
                                        );
                                        executor
                                            .quarantine_active_operation(&quarantine_reason)?
                                            .map(|operation| operation.intent.token_symbol)
                                    } else {
                                        None
                                    };
                                    emit_rebalance_saga(
                                        &rebalance_telemetry,
                                        &rebalance_engine_id,
                                        target,
                                        &result,
                                        &executor,
                                        reconciliation_started_at,
                                        true,
                                    );
                                    emit_rebalance_risk(
                                        &rebalance_telemetry,
                                        &rebalance_engine_id,
                                        &executor,
                                    );
                                    risk_sender.send_replace(executor.rebalance_risk()?);
                                    let active_operation_after =
                                        executor.active_operation()?.is_some();
                                    if result_sender
                                        .send(RebalanceExecutorEvent::Recovery {
                                            target,
                                            result,
                                            active_operation_after,
                                            blocked_token,
                                            recovery_started: Some(Box::new(reopened)),
                                            next_recovery: None,
                                        })
                                        .await
                                        .is_err()
                                    {
                                        return Ok::<(), anyhow::Error>(());
                                    }
                                    continue;
                                }
                                Ok(None) => {}
                                Err(error) => {
                                    let error = format!("{error:#}");
                                    tracing::warn!(
                                        error,
                                        retry_after_seconds =
                                            ACROSS_RECONCILIATION_INTERVAL.as_secs(),
                                        "consumed-nonce deposit quarantine reconciliation will be retried"
                                    );
                                    if result_sender
                                        .send(RebalanceExecutorEvent::AcrossReconciliationIdle {
                                            attempted: true,
                                            error: Some(error),
                                        })
                                        .await
                                        .is_err()
                                    {
                                        return Ok::<(), anyhow::Error>(());
                                    }
                                    continue;
                                }
                            }
                            if !executor.has_reconcilable_across_fill_quarantine()? {
                                if result_sender
                                    .send(RebalanceExecutorEvent::AcrossReconciliationIdle {
                                        attempted: false,
                                        error: None,
                                    })
                                    .await
                                    .is_err()
                                {
                                    return Ok::<(), anyhow::Error>(());
                                }
                                continue;
                            }
                            match executor.reconcile_next_across_fill_quarantine().await {
                                Ok(Some(reopened)) => {
                                    let target = rebalance_target(&reopened);
                                    let result =
                                        recover_rebalance_with_quote_retries(&mut executor).await;
                                    let blocked_token = if let Err(error) = &result {
                                        executor
                                            .quarantine_active_operation(error)?
                                            .map(|operation| operation.intent.token_symbol)
                                    } else {
                                        None
                                    };
                                    emit_rebalance_saga(
                                        &rebalance_telemetry,
                                        &rebalance_engine_id,
                                        target,
                                        &result,
                                        &executor,
                                        reconciliation_started_at,
                                        true,
                                    );
                                    emit_rebalance_risk(
                                        &rebalance_telemetry,
                                        &rebalance_engine_id,
                                        &executor,
                                    );
                                    risk_sender.send_replace(executor.rebalance_risk()?);
                                    let active_operation_after =
                                        executor.active_operation()?.is_some();
                                    if result_sender
                                        .send(RebalanceExecutorEvent::Recovery {
                                            target,
                                            result,
                                            active_operation_after,
                                            blocked_token,
                                            recovery_started: Some(Box::new(reopened)),
                                            next_recovery: None,
                                        })
                                        .await
                                        .is_err()
                                    {
                                        return Ok::<(), anyhow::Error>(());
                                    }
                                }
                                Ok(None) => {
                                    if result_sender
                                        .send(RebalanceExecutorEvent::AcrossReconciliationIdle {
                                            attempted: true,
                                            error: None,
                                        })
                                        .await
                                        .is_err()
                                    {
                                        return Ok::<(), anyhow::Error>(());
                                    }
                                }
                                Err(error) => {
                                    let error = format!("{error:#}");
                                    tracing::warn!(
                                        error,
                                        retry_after_seconds =
                                            ACROSS_RECONCILIATION_INTERVAL.as_secs(),
                                        "Across quarantine reconciliation will be retried"
                                    );
                                    if result_sender
                                        .send(RebalanceExecutorEvent::AcrossReconciliationIdle {
                                            attempted: true,
                                            error: Some(error),
                                        })
                                        .await
                                        .is_err()
                                    {
                                        return Ok::<(), anyhow::Error>(());
                                    }
                                }
                            }
                        }
                    }
                }
                Ok::<(), anyhow::Error>(())
            });
            (
                Some(request_sender),
                result_receiver,
                Some(task),
                risk_receiver,
            )
        } else {
            let (_request_sender, _request_receiver) =
                tokio::sync::mpsc::channel::<RebalanceExecutorCommand>(1);
            let (_result_sender, result_receiver) =
                tokio::sync::mpsc::channel::<RebalanceExecutorEvent>(1);
            let (_risk_sender, risk_receiver) =
                tokio::sync::watch::channel(RebalanceRisk::default());
            (None, result_receiver, None, risk_receiver)
        };
    for (target, token, reason) in &quarantined_rebalance_tokens {
        match target {
            RebalanceExecutionTarget::Primary => {
                engine.on_rebalance_token_quarantined(token, reason)?
            }
            RebalanceExecutionTarget::ArbitrumEsp => {
                esp_engine.on_rebalance_token_quarantined(token, reason)?
            }
            RebalanceExecutionTarget::ArbitrumArb => {
                arb_engine.on_rebalance_token_quarantined(token, reason)?
            }
            RebalanceExecutionTarget::Linea => {
                linea_engine.on_rebalance_token_quarantined(token, reason)?
            }
        }
    }
    if let Some(operation) = rebalance_recovery_operation.as_ref() {
        match rebalance_target(operation) {
            RebalanceExecutionTarget::Primary => engine.on_rebalance_recovery_started(operation)?,
            RebalanceExecutionTarget::ArbitrumEsp => {
                esp_engine.on_rebalance_recovery_started(operation)?
            }
            RebalanceExecutionTarget::ArbitrumArb => {
                arb_engine.on_rebalance_recovery_started(operation)?
            }
            RebalanceExecutionTarget::Linea => {
                linea_engine.on_rebalance_recovery_started(operation)?
            }
        }
    }
    engine.on_balance_event(BalanceEvent::Binance(initial_binance_balances.clone()))?;
    esp_engine
        .on_shared_binance_balance_event(BalanceEvent::Binance(initial_binance_balances.clone()))?;
    arb_engine
        .on_shared_binance_balance_event(BalanceEvent::Binance(initial_binance_balances.clone()))?;
    linea_engine
        .on_shared_binance_balance_event(BalanceEvent::Binance(initial_binance_balances))?;
    engine.on_balance_event(BalanceEvent::Wallet(initial_wallet_balances))?;
    esp_engine.on_balance_event(BalanceEvent::Wallet(esp_initial_wallet_balances.clone()))?;
    arb_engine.on_balance_event(BalanceEvent::Wallet(arb_initial_wallet_balances.clone()))?;
    linea_engine.on_balance_event(BalanceEvent::Wallet(linea_initial_wallet_balances.clone()))?;
    for snapshot in &portfolio_wallet_snapshots {
        if snapshot.chain_id != wallet_chain_id {
            engine.on_portfolio_wallet_snapshot(snapshot)?;
        }
    }
    engine.on_user_data_connected(user_data_subscription_id);
    esp_engine.on_shared_user_data_connected();
    arb_engine.on_shared_user_data_connected();
    linea_engine.on_shared_user_data_connected();
    // The executor and its durable journal are a single process-wide mutation
    // lane. Recovery owns that lane until it publishes a terminal result.
    let mut rebalance_lane_busy = rebalance_recovery_operation.is_some();
    let mut next_rebalance_target = rebalance_recovery_operation
        .as_ref()
        .map(rebalance_target)
        .map(RebalanceExecutionTarget::other)
        .unwrap_or(RebalanceExecutionTarget::Primary);
    dispatch_next_rebalance_execution(
        &mut rebalance_lane_busy,
        &mut next_rebalance_target,
        &mut engine,
        &mut esp_engine,
        &mut arb_engine,
        &mut linea_engine,
        rebalance_sender.as_ref(),
        pair,
        &esp_pair,
        &arb_pair,
        &linea_pair,
        wallet_owner,
        portfolio_catalog.capital_policy(),
        &rebalance_runtime_limits,
        &rebalance_risk_receiver,
    )
    .await?;
    engine.start();
    esp_engine.start();
    arb_engine.start();
    linea_engine.start();
    let mut first_ready_emitted = false;
    let mut longest_non_price_handler_us = 0_u128;
    let mut longest_non_price_handler = "none";
    let mut adaptive_sizing_tasks: tokio::task::JoinSet<AdaptiveSizingTaskResult> =
        tokio::task::JoinSet::new();
    let sizing_strategy_ids = engine
        .dependencies()
        .plan()
        .strategies
        .iter()
        .filter(|strategy| strategy.execute)
        .map(|strategy| strategy.strategy_id.clone())
        .collect::<Vec<_>>();
    let maximum_adaptive_sizing_workers =
        MAXIMUM_CONCURRENT_ADAPTIVE_SIZING_WORKERS.min(sizing_strategy_ids.len());
    let mut adaptive_sizing_slots: FairLatestOnlySizingScheduler<AdaptiveSizingJob> =
        FairLatestOnlySizingScheduler::new(sizing_strategy_ids, maximum_adaptive_sizing_workers)?;
    let mut pending_prepared_pool_builds = PreparedPoolBuildBatch::default();
    let (startup_primary_dex, startup_shadow_dex) = drain_startup_dex_backlog(
        &mut engine,
        &mut esp_engine,
        &mut pending_prepared_pool_builds,
        &mut dex_receiver,
        &mut shadow_dex_receiver,
        &wallet_heads,
        &receipt_heads,
        &esp_wallet_heads,
        &esp_receipt_heads,
    )?;
    report_strategy_dependency_faults(&mut engine, &root_supervisor)?;
    if startup_primary_dex.pool_build_count > 0 {
        engine.evaluate_after_dex_refreshes()?;
    }
    if startup_shadow_dex.pool_build_count > 0 {
        esp_engine.evaluate_after_dex_refreshes()?;
    }
    let mut startup_arb_event_count = 0_usize;
    let mut startup_arb_pool_build_count = 0_usize;
    while let Ok(event) = arb_dex_receiver.try_recv() {
        startup_arb_event_count += 1;
        let head = match &event {
            DexStreamEvent::Head { head, .. } => Some(*head),
            DexStreamEvent::Log { .. } => None,
        };
        if let Some(request) = arb_engine.on_dex_event(event)? {
            build_prepared_pool_inline(&mut arb_engine, request)?;
            startup_arb_pool_build_count += 1;
        }
        if let Some(head) = head {
            esp_wallet_heads.send_replace(head);
            arb_receipt_heads.send_replace(head);
        }
    }
    if startup_arb_pool_build_count > 0 {
        arb_engine.evaluate_after_dex_refreshes()?;
    }
    let mut startup_linea_event_count = 0_usize;
    let mut startup_linea_pool_build_count = 0_usize;
    while let Ok(event) = linea_dex_receiver.try_recv() {
        startup_linea_event_count += 1;
        let head = match &event {
            DexStreamEvent::Head { head, .. } => Some(*head),
            DexStreamEvent::Log { .. } => None,
        };
        if let Some(request) = linea_engine.on_dex_event(event)? {
            build_prepared_pool_inline(&mut linea_engine, request)?;
            startup_linea_pool_build_count += 1;
        }
        if let Some(head) = head {
            linea_wallet_heads.send_replace(head);
            linea_receipt_heads.send_replace(head);
        }
    }
    if startup_linea_pool_build_count > 0 {
        linea_engine.evaluate_after_dex_refreshes()?;
    }
    telemetry.emit(
        "startup_dex_backlog_drain",
        serde_json::json!({
            "engine_id": config.engine_id,
            "primary_event_count": startup_primary_dex.event_count,
            "primary_pool_build_count": startup_primary_dex.pool_build_count,
            "primary_max_queue_age_us": startup_primary_dex.max_queue_age_us,
            "esp_event_count": startup_shadow_dex.event_count,
            "esp_max_queue_age_us": startup_shadow_dex.max_queue_age_us,
            "arb_event_count": startup_arb_event_count,
            "arb_pool_build_count": startup_arb_pool_build_count,
            "linea_event_count": startup_linea_event_count,
            "linea_pool_build_count": startup_linea_pool_build_count,
            "backlog_empty_before_ready": true,
        }),
    );
    let network_runtime_ids = network_registry
        .as_ref()
        .map(|registry| {
            registry
                .runtimes()
                .map(|runtime| runtime.plan().network_id.as_str().to_owned())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let hot_path_strategy_ids = engine
        .dependencies()
        .plan()
        .strategies
        .iter()
        .map(|strategy| strategy.strategy_id.as_str())
        .collect::<Vec<_>>();

    tracing::info!(
        service = %config.service_name,
        engine_id = %config.engine_id,
        gcp_project_id = %config.gcp_project_id,
        gcp_region = %config.gcp_region,
        domain_snapshot_id = %domain_config.snapshot().snapshot_id,
        domain_config_sha256 = %domain_config.fingerprint_sha256(),
        network_runtime_ids = ?network_runtime_ids,
        network_runtime_count = network_runtime_ids.len(),
        hot_path_strategy_ids = ?hot_path_strategy_ids,
        hot_path_strategy_count = hot_path_strategy_ids.len(),
        hot_path_direct_binance_poll = true,
        hot_path_dependency_index = "compiled_exact_symbol_pool",
        hot_path_sizing_policy =
            "globally_bounded_round_robin_one_running_one_latest_pending_per_strategy",
        hot_path_maximum_adaptive_sizing_workers = maximum_adaptive_sizing_workers,
        secondary_hot_path_strategy_id = %shadow_plan.strategy_id.as_str(),
        secondary_hot_path_external_mutation_authorized = true,
        secondary_hot_path_rebalance_mutation_authorized =
            portfolio_catalog.allocator_mode() == CompiledCapitalAllocatorMode::FullLive,
        portfolio_inventory_key = "inventory_location+venue_asset_id",
        portfolio_location_count = portfolio_catalog.location_count(),
        portfolio_venue_asset_count = portfolio_catalog.asset_count(),
        portfolio_economic_asset_count = portfolio_catalog.economic_asset_count(),
        portfolio_allocator_mode = ?portfolio_catalog.allocator_mode(),
        portfolio_external_mutation_authorized = portfolio_catalog
            .capital_policy()
            .is_some_and(|policy| policy.external_mutation_authorized),
        live_rebalance_adapter = portfolio_catalog.live_rebalance_adapter(),
        arbitrum_execution_enabled = network_registry
            .as_ref()
            .and_then(|registry| registry.get_by_chain_id(42_161).ok())
            .is_some_and(|runtime| runtime.execution().mutation_enabled()),
        binance_symbols = ?binance_symbols,
        binance_account_snapshot_generation = binance_account_generation,
        binance_account_snapshot_duration_us,
        binance_hydrated_symbols = ?hydrated_binance_symbols,
        binance_stream_shards = ?compiled_binance_runtime
            .as_ref()
            .map(|runtime| &runtime.stream_shards),
        binance_runtime_account_id = %shared_binance_runtime.account_id(),
        binance_runtime_owner_count = shared_binance_runtime.owners().len(),
        binance_runtime_direct_market_data = ?shared_binance_runtime
            .owner(arb_bot::binance::runtime::BinanceOwnerKind::MarketData)?,
        binance_executable_symbols = ?compiled_binance_runtime
            .as_ref()
            .map(|runtime| &runtime.executable_symbols),
        binance_asset_symbols = ?binance_asset_symbols,
        binance_account_type = %binance_account.account.account_type,
        binance_can_trade = binance_account.account.can_trade,
        binance_permissions = ?binance_account.account.permissions,
        binance_nonzero_balances = binance_account.account.balances.len(),
        binance_clock_offset_ms = binance_account.clock_offset_ms,
        binance_clock_sync_rtt_us = binance_account.clock_sync.round_trip_us,
        binance_clock_sync_midpoint_uncertainty_us = binance_account.clock_sync.midpoint_uncertainty_us(),
        binance_standard_maker_fee = %binance_account.commission.standard_commission.maker,
        binance_standard_taker_fee = %binance_account.commission.standard_commission.taker,
        binance_commission_discount_enabled_for_account =
            binance_account.commission.discount.enabled_for_account,
        binance_commission_discount_enabled_for_symbol =
            binance_account.commission.discount.enabled_for_symbol,
        binance_commission_discount_asset = %binance_account.commission.discount.discount_asset,
        binance_commission_discount = %binance_account.commission.discount.discount,
        binance_buy_fee_bps,
        binance_sell_fee_bps,
        binance_symbol_status = %binance_account.symbol_rules.status,
        binance_price_tick = %binance_account.symbol_rules.price.step,
        binance_execution_price_tick = %execution_symbol_rules.price.step,
        binance_lot_step = %binance_account.symbol_rules.lot_size.step,
        binance_market_lot_step = %binance_account.symbol_rules.market_lot_size.step,
        binance_min_notional = %binance_account.symbol_rules.min_notional,
        binance_open_orders = binance_account.open_orders.len(),
        binance_order_rate_limits = ?binance_account.order_rate_limits,
        binance_gas_price_symbol = %gas_price_symbol,
        binance_commission_asset = %commission_asset,
        binance_commission_price_symbol = %commission_price_symbol,
        binance_strategy_max_transport_silence_ms = pair.strategy.max_transport_silence_ms(),
        binance_gas_price_gate_enabled = false,
        binance_wld_balance_present = binance_account.balance("WLD").is_some(),
        binance_usdc_balance_present = binance_account.balance("USDC").is_some(),
        binance_commission_balance_present = binance_account.balance(&commission_asset).is_some(),
        wallet_address = %wallet_owner,
        wallet_chain_id,
        balance_sync_interval_ms = config.balance_sync_interval_ms,
        balance_max_age_ms = config.balance_max_age_ms,
        dex_head_max_age_ms = config.dex_head_max_age_ms,
        wallet_sync_trigger = "alchemy_new_heads",
        clickhouse_enabled = config.clickhouse_enabled(),
        arbitrage_execution_mode = %config.arbitrage_execution_mode,
        rebalance_execution_mode = %config.rebalance_execution_mode,
        "arbitrage shadow service started with authenticated Binance account state"
    );
    let runtime_ready_file = mark_runtime_ready()?;

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let health_interval =
        Duration::from_millis((pair.strategy.max_transport_silence_ms() / 4).clamp(100, 1_000));
    let mut health_tick = tokio::time::interval(health_interval);
    health_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    health_tick.reset();
    let mut rebalance_supervisor_tick = tokio::time::interval(REBALANCE_SUPERVISOR_INTERVAL);
    rebalance_supervisor_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    rebalance_supervisor_tick.reset();
    let mut across_reconciliation_tick = tokio::time::interval(ACROSS_RECONCILIATION_INTERVAL);
    across_reconciliation_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    across_reconciliation_tick.reset();

    // These futures must survive unrelated select branches. Recreating
    // `next_event()` on every loop iteration cancels a multi-await depth
    // bootstrap or reconnect before it can commit the connected socket.
    let mut binance_market_event = Box::pin(binance_feed.next_event());
    let mut gas_market_event = Box::pin(gas_price_feed.next_event());
    let mut commission_market_event = Box::pin(commission_price_feed.next_event());
    let mut shadow_dex_running = true;
    let mut arb_dex_running = true;
    let linea_dex_running = true;

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => break,
            event = dex_receiver.recv() => {
                let handler_started_at = Instant::now();
                let Some(event) = event else {
                    bail!("Alchemy DEX stream stopped; process restart will rehydrate state");
                };
                if let Some(request) = process_dex_event_inline(
                    &mut engine,
                    event,
                    &wallet_heads,
                    &receipt_heads,
                )? {
                    pending_prepared_pool_builds.queue(request);
                }
                let (prepared_dex, additionally_drained) =
                    build_prepared_pools_interleaved(
                    &mut engine,
                    &mut pending_prepared_pool_builds,
                    &mut dex_receiver,
                    &wallet_heads,
                    &receipt_heads,
                )?;
                if prepared_dex {
                    engine.evaluate_after_dex_refreshes()?;
                }
                let handler_duration = handler_started_at.elapsed();
                engine.record_dex_drain(1 + additionally_drained, handler_duration);
                record_longest_handler(
                    &mut longest_non_price_handler_us,
                    &mut longest_non_price_handler,
                    "dex_drain",
                    handler_duration,
                );
            }
            event = shadow_dex_receiver.recv(), if shadow_dex_running => {
                let handler_started_at = Instant::now();
                let Some(event) = event else {
                    esp_market_data_ready.store(false, Ordering::Release);
                    tracing::error!(
                        strategy_id = %shadow_plan.strategy_id.as_str(),
                        "Arbitrum esp DEX stream stopped; new ESP entries are disabled"
                    );
                    shadow_dex_running = false;
                    continue;
                };
                let head = match &event {
                    DexStreamEvent::Head { head, .. } => Some(*head),
                    DexStreamEvent::Log { .. } => None,
                };
                if let Some(request) = esp_engine.on_dex_event(event)? {
                    build_prepared_pool_inline(&mut esp_engine, request)?;
                    esp_engine.evaluate_after_dex_refreshes()?;
                }
                if let Some(head) = head {
                    esp_wallet_heads.send_replace(head);
                    esp_receipt_heads.send_replace(head);
                }
                record_longest_handler(
                    &mut longest_non_price_handler_us,
                    &mut longest_non_price_handler,
                    "shadow_dex",
                    handler_started_at.elapsed(),
                );
            }
            event = arb_dex_receiver.recv(), if arb_dex_running => {
                let handler_started_at = Instant::now();
                let Some(event) = event else {
                    arb_market_data_ready.store(false, Ordering::Release);
                    tracing::error!(
                        strategy_id = %arb_plan.strategy_id.as_str(),
                        "Arbitrum ARB DEX stream stopped; new ARB entries are disabled"
                    );
                    arb_dex_running = false;
                    continue;
                };
                let head = match &event {
                    DexStreamEvent::Head { head, .. } => Some(*head),
                    DexStreamEvent::Log { .. } => None,
                };
                if let Some(request) = arb_engine.on_dex_event(event)? {
                    build_prepared_pool_inline(&mut arb_engine, request)?;
                    arb_engine.evaluate_after_dex_refreshes()?;
                }
                if let Some(head) = head {
                    esp_wallet_heads.send_replace(head);
                    arb_receipt_heads.send_replace(head);
                }
                record_longest_handler(
                    &mut longest_non_price_handler_us,
                    &mut longest_non_price_handler,
                    "arb_dex",
                    handler_started_at.elapsed(),
                );
            }
            event = linea_dex_receiver.recv(), if linea_dex_running => {
                let handler_started_at = Instant::now();
                let Some(event) = event else {
                    linea_market_data_ready.store(false, Ordering::Release);
                    bail!(
                        "Linea Lynex DEX stream stopped; process restart will rehydrate state"
                    );
                };
                let head = match &event {
                    DexStreamEvent::Head { head, .. } => Some(*head),
                    DexStreamEvent::Log { .. } => None,
                };
                if let Some(request) = linea_engine.on_dex_event(event)? {
                    build_prepared_pool_inline(&mut linea_engine, request)?;
                    linea_engine.evaluate_after_dex_refreshes()?;
                }
                if let Some(head) = head {
                    linea_wallet_heads.send_replace(head);
                    linea_receipt_heads.send_replace(head);
                }
                record_longest_handler(
                    &mut longest_non_price_handler_us,
                    &mut longest_non_price_handler,
                    "linea_dex",
                    handler_started_at.elapsed(),
                );
            }
            scheduled_at = health_tick.tick() => {
                let loop_lag_us = scheduled_at.elapsed().as_micros();
                engine.refresh_health();
                esp_engine.refresh_health();
                arb_engine.refresh_health();
                linea_engine.refresh_health();
                engine.record_owner_loop_health(
                    loop_lag_us,
                    longest_non_price_handler,
                    longest_non_price_handler_us,
                );
                longest_non_price_handler_us = 0;
                longest_non_price_handler = "none";
            },
            _ = rebalance_supervisor_tick.tick(), if rebalance_sender.is_some() => {
                let handler_started_at = Instant::now();
                dispatch_next_rebalance_execution(
                    &mut rebalance_lane_busy,
                    &mut next_rebalance_target,
                    &mut engine,
                    &mut esp_engine,
                    &mut arb_engine,
                    &mut linea_engine,
                    rebalance_sender.as_ref(),
                    pair,
                    &esp_pair,
                    &arb_pair,
                    &linea_pair,
                    wallet_owner,
                    portfolio_catalog.capital_policy(),
                    &rebalance_runtime_limits,
                    &rebalance_risk_receiver,
                )
                .await?;
                record_longest_handler(
                    &mut longest_non_price_handler_us,
                    &mut longest_non_price_handler,
                    "rebalance_supervisor",
                    handler_started_at.elapsed(),
                );
            },
            _ = across_reconciliation_tick.tick(), if rebalance_sender.is_some() => {
                let handler_started_at = Instant::now();
                dispatch_across_reconciliation(
                    &mut rebalance_lane_busy,
                    rebalance_sender.as_ref(),
                )?;
                record_longest_handler(
                    &mut longest_non_price_handler_us,
                    &mut longest_non_price_handler,
                    "across_reconciliation_dispatch",
                    handler_started_at.elapsed(),
                );
            },
            event = &mut binance_market_event => {
                drop(binance_market_event);
                let event_symbol = market_event_symbol(&event);
                if event_symbol == shadow_plan.symbol {
                    esp_engine.on_market_event(event, None)?;
                } else if event_symbol == arb_plan.symbol {
                    arb_engine.on_market_event(event, None)?;
                } else if event_symbol == linea_plan.symbol {
                    linea_engine.on_market_event(event, None)?;
                } else if engine.dependencies().for_symbol(event_symbol).next().is_some() {
                    let _summary =
                        engine.on_market_event(event, binance_feed.depth_book())?;
                    report_strategy_dependency_faults(&mut engine, &root_supervisor)?;
                } else {
                    let (event_kind, generation, parse_time_us, wire_frame_size_bytes) =
                        observed_market_event_fields(&event);
                    engine.record_shared_binance_stream_event(
                        event_symbol,
                        event_kind,
                        generation,
                        parse_time_us,
                        wire_frame_size_bytes,
                    );
                }
                binance_market_event = Box::pin(binance_feed.next_event());
            },
            event = &mut gas_market_event => {
                let handler_started_at = Instant::now();
                drop(gas_market_event);
                engine.on_gas_market_event(event.clone())?;
                esp_engine.on_gas_market_event(event.clone())?;
                arb_engine.on_gas_market_event(event.clone())?;
                linea_engine.on_gas_market_event(event)?;
                gas_market_event = Box::pin(gas_price_feed.next_event());
                record_longest_handler(
                    &mut longest_non_price_handler_us,
                    &mut longest_non_price_handler,
                    "native_conversion_price",
                    handler_started_at.elapsed(),
                );
            },
            event = &mut commission_market_event => {
                let handler_started_at = Instant::now();
                drop(commission_market_event);
                engine.on_commission_market_event(event.clone())?;
                esp_engine.on_commission_market_event(event.clone())?;
                arb_engine.on_commission_market_event(event.clone())?;
                linea_engine.on_commission_market_event(event)?;
                commission_market_event = Box::pin(commission_price_feed.next_event());
                record_longest_handler(
                    &mut longest_non_price_handler_us,
                    &mut longest_non_price_handler,
                    "commission_conversion_price",
                    handler_started_at.elapsed(),
                );
            },
            event = user_data_stream.next_event() => {
                let handler_started_at = Instant::now();
                let event = event?;
                match &event {
                    UserDataEvent::ExecutionReport(report)
                        if report.symbol == shadow_plan.symbol =>
                    {
                        esp_engine.on_user_data_event(event)?;
                    }
                    UserDataEvent::ExecutionReport(report)
                        if report.symbol == arb_plan.symbol =>
                    {
                        arb_engine.on_user_data_event(event)?;
                    }
                    UserDataEvent::ExecutionReport(report)
                        if report.symbol == linea_plan.symbol =>
                    {
                        linea_engine.on_user_data_event(event)?;
                    }
                    UserDataEvent::ExecutionReport(report)
                        if report.symbol == pair.binance.symbol =>
                    {
                        engine.on_user_data_event(event)?;
                    }
                    UserDataEvent::AccountPosition(_) | UserDataEvent::BalanceUpdate(_) => {
                        engine.on_user_data_event(event.clone())?;
                        esp_engine.on_shared_user_data_dirty();
                        arb_engine.on_shared_user_data_dirty();
                        linea_engine.on_shared_user_data_dirty();
                    }
                    UserDataEvent::ExecutionReport(_) => {
                        engine.on_user_data_event(event.clone())?;
                        esp_engine.on_shared_user_data_dirty();
                        arb_engine.on_shared_user_data_dirty();
                        linea_engine.on_shared_user_data_dirty();
                    }
                    UserDataEvent::StreamTerminated { .. } => {
                        engine.on_user_data_event(event.clone())?;
                        esp_engine.on_shared_user_data_disconnected();
                        arb_engine.on_shared_user_data_disconnected();
                        linea_engine.on_shared_user_data_disconnected();
                    }
                    UserDataEvent::Other { .. } => {
                        engine.on_user_data_event(event.clone())?;
                        esp_engine.on_shared_user_data_dirty();
                        arb_engine.on_shared_user_data_dirty();
                        linea_engine.on_shared_user_data_dirty();
                    }
                }
                record_longest_handler(
                    &mut longest_non_price_handler_us,
                    &mut longest_non_price_handler,
                    "binance_user_data",
                    handler_started_at.elapsed(),
                );
            },
            observation = binance_clock_sync_receiver.recv(), if binance_clock_sync_running => {
                let handler_started_at = Instant::now();
                match observation {
                    Some(Ok(clock_sync)) => {
                        engine.on_binance_clock_sync(clock_sync);
                        esp_engine.on_binance_clock_sync(clock_sync);
                        arb_engine.on_binance_clock_sync(clock_sync);
                        linea_engine.on_binance_clock_sync(clock_sync);
                    }
                    Some(Err(error)) => {
                        engine.on_binance_clock_sync_failure(&error);
                        esp_engine.on_binance_clock_sync_failure(&error);
                        arb_engine.on_binance_clock_sync_failure(&error);
                        linea_engine.on_binance_clock_sync_failure(&error);
                    }
                    None => {
                        binance_clock_sync_running = false;
                        engine.on_binance_clock_sync_failure(
                            "background Binance clock synchronization task stopped",
                        );
                        esp_engine.on_binance_clock_sync_failure(
                            "background Binance clock synchronization task stopped",
                        );
                        arb_engine.on_binance_clock_sync_failure(
                            "background Binance clock synchronization task stopped",
                        );
                        linea_engine.on_binance_clock_sync_failure(
                            "background Binance clock synchronization task stopped",
                        );
                    }
                }
                record_longest_handler(
                    &mut longest_non_price_handler_us,
                    &mut longest_non_price_handler,
                    "binance_clock_sync",
                    handler_started_at.elapsed(),
                );
            },
            event = balance_receiver.recv() => {
                let handler_started_at = Instant::now();
                let Some(event) = event else {
                    bail!("balance synchronization channel stopped unexpectedly");
                };
                match event {
                    BalanceEvent::Binance(snapshot) => {
                        engine.on_balance_event(BalanceEvent::Binance(snapshot.clone()))?;
                        esp_engine.on_shared_binance_balance_event(
                            BalanceEvent::Binance(snapshot.clone()),
                        )?;
                        arb_engine.on_shared_binance_balance_event(BalanceEvent::Binance(
                            snapshot.clone(),
                        ))?;
                        linea_engine.on_shared_binance_balance_event(BalanceEvent::Binance(snapshot))?;
                    }
                    BalanceEvent::Failed {
                        source: BalanceSource::Binance,
                        error,
                        observed_at,
                    } => {
                        engine.on_balance_event(BalanceEvent::Failed {
                            source: BalanceSource::Binance,
                            error: error.clone(),
                            observed_at,
                        })?;
                        esp_engine.on_shared_binance_balance_event(BalanceEvent::Failed {
                            source: BalanceSource::Binance,
                            error: error.clone(),
                            observed_at,
                        })?;
                        arb_engine.on_shared_binance_balance_event(BalanceEvent::Failed {
                            source: BalanceSource::Binance,
                            error: error.clone(),
                            observed_at,
                        })?;
                        linea_engine.on_shared_binance_balance_event(BalanceEvent::Failed {
                            source: BalanceSource::Binance,
                            error,
                            observed_at,
                        })?;
                    }
                    other => engine.on_balance_event(other)?,
                }
                dispatch_next_rebalance_execution(
                    &mut rebalance_lane_busy,
                    &mut next_rebalance_target,
                    &mut engine,
                    &mut esp_engine,
                    &mut arb_engine,
                    &mut linea_engine,
                    rebalance_sender.as_ref(),
                    pair,
                    &esp_pair,
                    &arb_pair,
                    &linea_pair,
                    wallet_owner,
                    portfolio_catalog.capital_policy(),
                    &rebalance_runtime_limits,
                    &rebalance_risk_receiver,
                )
                .await?;
                record_longest_handler(
                    &mut longest_non_price_handler_us,
                    &mut longest_non_price_handler,
                    "balance_publication",
                    handler_started_at.elapsed(),
                );
            }
            event = esp_wallet_balance_receiver.recv() => {
                let handler_started_at = Instant::now();
                let Some(event) = event else {
                    bail!("Arbitrum wallet balance synchronization channel stopped unexpectedly");
                };
                esp_engine.on_balance_event(event.clone())?;
                arb_engine.on_balance_event(event)?;
                dispatch_next_rebalance_execution(
                    &mut rebalance_lane_busy,
                    &mut next_rebalance_target,
                    &mut engine,
                    &mut esp_engine,
                    &mut arb_engine,
                    &mut linea_engine,
                    rebalance_sender.as_ref(),
                    pair,
                    &esp_pair,
                    &arb_pair,
                    &linea_pair,
                    wallet_owner,
                    portfolio_catalog.capital_policy(),
                    &rebalance_runtime_limits,
                    &rebalance_risk_receiver,
                )
                .await?;
                record_longest_handler(
                    &mut longest_non_price_handler_us,
                    &mut longest_non_price_handler,
                    "arbitrum_balance_publication",
                    handler_started_at.elapsed(),
                );
            }
            event = linea_wallet_balance_receiver.recv() => {
                let handler_started_at = Instant::now();
                let Some(event) = event else {
                    bail!("Linea wallet balance synchronization channel stopped unexpectedly");
                };
                linea_engine.on_balance_event(event)?;
                dispatch_next_rebalance_execution(
                    &mut rebalance_lane_busy,
                    &mut next_rebalance_target,
                    &mut engine,
                    &mut esp_engine,
                    &mut arb_engine,
                    &mut linea_engine,
                    rebalance_sender.as_ref(),
                    pair,
                    &esp_pair,
                    &arb_pair,
                    &linea_pair,
                    wallet_owner,
                    portfolio_catalog.capital_policy(),
                    &rebalance_runtime_limits,
                    &rebalance_risk_receiver,
                )
                .await?;
                record_longest_handler(
                    &mut longest_non_price_handler_us,
                    &mut longest_non_price_handler,
                    "linea_balance_publication",
                    handler_started_at.elapsed(),
                );
            }
            result = rebalance_receiver.recv(), if rebalance_sender.is_some() => {
                let handler_started_at = Instant::now();
                let Some(result) = result else {
                    bail!("rebalance executor result channel stopped unexpectedly");
                };
                let active_operation_after = match &result {
                    RebalanceExecutorEvent::Recovery {
                        active_operation_after,
                        ..
                    }
                    | RebalanceExecutorEvent::Execution {
                        active_operation_after,
                        ..
                    } => *active_operation_after,
                    RebalanceExecutorEvent::AcrossReconciliationIdle { .. } => false,
                };
                let next_recovery = match &result {
                    RebalanceExecutorEvent::Recovery { next_recovery, .. } => {
                        next_recovery.clone()
                    }
                    RebalanceExecutorEvent::Execution { .. }
                    | RebalanceExecutorEvent::AcrossReconciliationIdle { .. } => None,
                };
                let recovery_started = match &result {
                    RebalanceExecutorEvent::Recovery {
                        recovery_started,
                        ..
                    } => recovery_started.clone(),
                    RebalanceExecutorEvent::Execution { .. }
                    | RebalanceExecutorEvent::AcrossReconciliationIdle { .. } => None,
                };
                if let Some(operation) = recovery_started.as_deref() {
                    match rebalance_target(operation) {
                        RebalanceExecutionTarget::Primary => {
                            engine.on_rebalance_recovery_started(operation)?
                        }
                        RebalanceExecutionTarget::ArbitrumEsp => {
                            esp_engine.on_rebalance_recovery_started(operation)?
                        }
                        RebalanceExecutionTarget::ArbitrumArb => {
                            arb_engine.on_rebalance_recovery_started(operation)?
                        }
                        RebalanceExecutionTarget::Linea => {
                            linea_engine.on_rebalance_recovery_started(operation)?
                        }
                    }
                };
                match result {
                    RebalanceExecutorEvent::Recovery {
                        target,
                        result,
                        blocked_token,
                        ..
                    } => match (target, result, blocked_token.as_deref()) {
                        (RebalanceExecutionTarget::Primary, Ok(operation), blocked_token) => {
                            engine.on_rebalance_recovery_result(Ok(&operation), blocked_token)?
                        }
                        (RebalanceExecutionTarget::Primary, Err(error), blocked_token) => {
                            engine.on_rebalance_recovery_result(Err(&error), blocked_token)?
                        }
                        (RebalanceExecutionTarget::ArbitrumEsp, Ok(operation), blocked_token) => {
                            esp_engine
                                .on_rebalance_recovery_result(Ok(&operation), blocked_token)?
                        }
                        (RebalanceExecutionTarget::ArbitrumEsp, Err(error), blocked_token) => {
                            esp_engine.on_rebalance_recovery_result(Err(&error), blocked_token)?
                        }
                        (RebalanceExecutionTarget::ArbitrumArb, Ok(operation), blocked_token) => {
                            arb_engine.on_rebalance_recovery_result(Ok(&operation), blocked_token)?
                        }
                        (RebalanceExecutionTarget::ArbitrumArb, Err(error), blocked_token) => {
                            arb_engine.on_rebalance_recovery_result(Err(&error), blocked_token)?
                        }
                        (RebalanceExecutionTarget::Linea, Ok(operation), blocked_token) => {
                            linea_engine
                                .on_rebalance_recovery_result(Ok(&operation), blocked_token)?
                        }
                        (RebalanceExecutionTarget::Linea, Err(error), blocked_token) => {
                            linea_engine.on_rebalance_recovery_result(Err(&error), blocked_token)?
                        }
                    },
                    RebalanceExecutorEvent::Execution {
                        target,
                        result,
                        blocked_token,
                        ..
                    } => match (target, result, blocked_token.as_deref()) {
                        (RebalanceExecutionTarget::Primary, Ok(operation), blocked_token) => {
                            engine.on_rebalance_execution_result(Ok(&operation), blocked_token)?
                        }
                        (RebalanceExecutionTarget::Primary, Err(error), blocked_token) => {
                            engine.on_rebalance_execution_result(Err(&error), blocked_token)?
                        }
                        (RebalanceExecutionTarget::ArbitrumEsp, Ok(operation), blocked_token) => {
                            esp_engine
                                .on_rebalance_execution_result(Ok(&operation), blocked_token)?
                        }
                        (RebalanceExecutionTarget::ArbitrumEsp, Err(error), blocked_token) => {
                            esp_engine.on_rebalance_execution_result(Err(&error), blocked_token)?
                        }
                        (RebalanceExecutionTarget::ArbitrumArb, Ok(operation), blocked_token) => {
                            arb_engine.on_rebalance_execution_result(Ok(&operation), blocked_token)?
                        }
                        (RebalanceExecutionTarget::ArbitrumArb, Err(error), blocked_token) => {
                            arb_engine.on_rebalance_execution_result(Err(&error), blocked_token)?
                        }
                        (RebalanceExecutionTarget::Linea, Ok(operation), blocked_token) => {
                            linea_engine
                                .on_rebalance_execution_result(Ok(&operation), blocked_token)?
                        }
                        (RebalanceExecutionTarget::Linea, Err(error), blocked_token) => {
                            linea_engine.on_rebalance_execution_result(Err(&error), blocked_token)?
                        }
                    },
                    RebalanceExecutorEvent::AcrossReconciliationIdle { attempted, error } => {
                        if attempted {
                            telemetry.emit(
                                "rebalance_across_reconciliation",
                                serde_json::json!({
                                    "engine_id": config.engine_id,
                                    "outcome": if error.is_some() {
                                        "retryable_error"
                                    } else {
                                        "fill_pending"
                                    },
                                    "error": error,
                                    "retry_after_seconds": ACROSS_RECONCILIATION_INTERVAL.as_secs(),
                                    "external_mutation_authorized": false,
                                }),
                            );
                        }
                    }
                }
                if let Some(operation) = next_recovery.as_deref() {
                    match rebalance_target(operation) {
                        RebalanceExecutionTarget::Primary => {
                            engine.on_rebalance_recovery_started(operation)?
                        }
                        RebalanceExecutionTarget::ArbitrumEsp => {
                            esp_engine.on_rebalance_recovery_started(operation)?
                        }
                        RebalanceExecutionTarget::ArbitrumArb => {
                            arb_engine.on_rebalance_recovery_started(operation)?
                        }
                        RebalanceExecutionTarget::Linea => {
                            linea_engine.on_rebalance_recovery_started(operation)?
                        }
                    }
                }
                rebalance_lane_busy = active_operation_after;
                dispatch_next_rebalance_execution(
                    &mut rebalance_lane_busy,
                    &mut next_rebalance_target,
                    &mut engine,
                    &mut esp_engine,
                    &mut arb_engine,
                    &mut linea_engine,
                    rebalance_sender.as_ref(),
                    pair,
                    &esp_pair,
                    &arb_pair,
                    &linea_pair,
                    wallet_owner,
                    portfolio_catalog.capital_policy(),
                    &rebalance_runtime_limits,
                    &rebalance_risk_receiver,
                )
                .await?;
                record_longest_handler(
                    &mut longest_non_price_handler_us,
                    &mut longest_non_price_handler,
                    "rebalance_result",
                    handler_started_at.elapsed(),
                );
            }
            event = paper_trade_events.recv(), if paper_trade_task.is_some() => {
                let handler_started_at = Instant::now();
                let Some(event) = event else {
                    bail!("paper trade event channel stopped unexpectedly");
                };
                if event.pair_id == esp_pair.id {
                    let mut prepared_dex = false;
                    while let Ok(dex_event) = shadow_dex_receiver.try_recv() {
                        if let Some(request) = esp_engine.on_dex_event(dex_event)? {
                            build_prepared_pool_inline(&mut esp_engine, request)?;
                            prepared_dex = true;
                        }
                    }
                    let receipt_refresh =
                        esp_engine.apply_arbitrage_receipt_settlement(&event)?;
                    let receipt_applied = receipt_refresh.is_some();
                    if let Some(refresh) = receipt_refresh {
                        build_prepared_pool_inline(&mut esp_engine, refresh)?;
                    }
                    esp_engine.on_paper_trade_event(event)?;
                    if prepared_dex || receipt_applied {
                        esp_engine.evaluate_after_dex_refreshes()?;
                    }
                } else if event.pair_id == arb_pair.id {
                    let mut prepared_dex = false;
                    while let Ok(dex_event) = arb_dex_receiver.try_recv() {
                        if let Some(request) = arb_engine.on_dex_event(dex_event)? {
                            build_prepared_pool_inline(&mut arb_engine, request)?;
                            prepared_dex = true;
                        }
                    }
                    let receipt_refresh = arb_engine.apply_arbitrage_receipt_settlement(&event)?;
                    let receipt_applied = receipt_refresh.is_some();
                    if let Some(refresh) = receipt_refresh {
                        build_prepared_pool_inline(&mut arb_engine, refresh)?;
                    }
                    arb_engine.on_paper_trade_event(event)?;
                    if prepared_dex || receipt_applied {
                        arb_engine.evaluate_after_dex_refreshes()?;
                    }
                } else if event.pair_id == linea_pair.id {
                    let mut prepared_dex = false;
                    while let Ok(dex_event) = linea_dex_receiver.try_recv() {
                        if let Some(request) = linea_engine.on_dex_event(dex_event)? {
                            build_prepared_pool_inline(&mut linea_engine, request)?;
                            prepared_dex = true;
                        }
                    }
                    let receipt_refresh =
                        linea_engine.apply_arbitrage_receipt_settlement(&event)?;
                    let receipt_applied = receipt_refresh.is_some();
                    if let Some(refresh) = receipt_refresh {
                        build_prepared_pool_inline(&mut linea_engine, refresh)?;
                    }
                    linea_engine.on_paper_trade_event(event)?;
                    if prepared_dex || receipt_applied {
                        linea_engine.evaluate_after_dex_refreshes()?;
                    }
                } else {
                    drain_dex_events_inline(
                        &mut engine,
                        &mut pending_prepared_pool_builds,
                        &mut dex_receiver,
                        &wallet_heads,
                        &receipt_heads,
                    )?;
                    let receipt_refresh = engine.apply_arbitrage_receipt_settlement(&event)?;
                    let receipt_applied = receipt_refresh.is_some();
                    if let Some(refresh) = receipt_refresh {
                        pending_prepared_pool_builds.queue(refresh);
                    }
                    let (prepared_dex, _) = build_prepared_pools_interleaved(
                        &mut engine,
                        &mut pending_prepared_pool_builds,
                        &mut dex_receiver,
                        &wallet_heads,
                        &receipt_heads,
                    )?;
                    engine.on_paper_trade_event(event)?;
                    if prepared_dex || receipt_applied {
                        engine.evaluate_after_dex_refreshes()?;
                    }
                }
                record_longest_handler(
                    &mut longest_non_price_handler_us,
                    &mut longest_non_price_handler,
                    "trade_result_settlement",
                    handler_started_at.elapsed(),
                );
            }
            result = adaptive_sizing_tasks.join_next(), if !adaptive_sizing_tasks.is_empty() => {
                let handler_started_at = Instant::now();
                let result = result
                    .context("adaptive sizing worker join set stopped unexpectedly")?
                    .context("adaptive sizing worker panicked")?;
                let completed_strategy_id = result.strategy_id().clone();
                if completed_strategy_id == shadow_plan.strategy_id {
                    let mut prepared_dex = false;
                    while let Ok(dex_event) = shadow_dex_receiver.try_recv() {
                        let head = match &dex_event {
                            DexStreamEvent::Head { head, .. } => Some(*head),
                            DexStreamEvent::Log { .. } => None,
                        };
                        if let Some(request) = esp_engine.on_dex_event(dex_event)? {
                            build_prepared_pool_inline(&mut esp_engine, request)?;
                            prepared_dex = true;
                        }
                        if let Some(head) = head {
                            esp_wallet_heads.send_replace(head);
                            esp_receipt_heads.send_replace(head);
                        }
                    }
                    if prepared_dex {
                        esp_engine.evaluate_after_dex_refreshes()?;
                    }
                    esp_engine.on_adaptive_sizing_result(result)?;
                } else if completed_strategy_id == arb_plan.strategy_id {
                    let mut prepared_dex = false;
                    while let Ok(dex_event) = arb_dex_receiver.try_recv() {
                        let head = match &dex_event {
                            DexStreamEvent::Head { head, .. } => Some(*head),
                            DexStreamEvent::Log { .. } => None,
                        };
                        if let Some(request) = arb_engine.on_dex_event(dex_event)? {
                            build_prepared_pool_inline(&mut arb_engine, request)?;
                            prepared_dex = true;
                        }
                        if let Some(head) = head {
                            esp_wallet_heads.send_replace(head);
                            arb_receipt_heads.send_replace(head);
                        }
                    }
                    if prepared_dex {
                        arb_engine.evaluate_after_dex_refreshes()?;
                    }
                    arb_engine.on_adaptive_sizing_result(result)?;
                } else if completed_strategy_id == linea_plan.strategy_id {
                    let mut prepared_dex = false;
                    while let Ok(dex_event) = linea_dex_receiver.try_recv() {
                        let head = match &dex_event {
                            DexStreamEvent::Head { head, .. } => Some(*head),
                            DexStreamEvent::Log { .. } => None,
                        };
                        if let Some(request) = linea_engine.on_dex_event(dex_event)? {
                            build_prepared_pool_inline(&mut linea_engine, request)?;
                            prepared_dex = true;
                        }
                        if let Some(head) = head {
                            linea_wallet_heads.send_replace(head);
                            linea_receipt_heads.send_replace(head);
                        }
                    }
                    if prepared_dex {
                        linea_engine.evaluate_after_dex_refreshes()?;
                    }
                    linea_engine.on_adaptive_sizing_result(result)?;
                } else if completed_strategy_id == engine.strategy_id() {
                    let (prepared_dex, _) = build_prepared_pools_interleaved(
                        &mut engine,
                        &mut pending_prepared_pool_builds,
                        &mut dex_receiver,
                        &wallet_heads,
                        &receipt_heads,
                    )?;
                    if prepared_dex {
                        engine.evaluate_after_dex_refreshes()?;
                    }
                    engine.on_adaptive_sizing_result(result)?;
                } else {
                    bail!(
                        "adaptive sizing result belongs to unknown executable strategy {}",
                        completed_strategy_id.as_str()
                    );
                }
                adaptive_sizing_slots.complete(&completed_strategy_id)?;
                while let Some((_, next)) = adaptive_sizing_slots.take_ready() {
                    adaptive_sizing_tasks.spawn_blocking(move || next.run());
                }
                record_longest_handler(
                    &mut longest_non_price_handler_us,
                    &mut longest_non_price_handler,
                    "adaptive_sizing_result",
                    handler_started_at.elapsed(),
                );
            }
            result = &mut dex_task => {
                result.context("Alchemy DEX connector task failed")??;
                bail!("Alchemy DEX connector stopped; process restart will rehydrate state");
            }
            result = &mut shadow_dex_task, if shadow_dex_running => {
                esp_market_data_ready.store(false, Ordering::Release);
                tracing::error!(
                    strategy_id = %shadow_plan.strategy_id.as_str(),
                    result = ?result,
                    "Arbitrum esp DEX connector stopped; new ESP entries are disabled"
                );
                shadow_dex_running = false;
            }
            result = &mut arb_dex_task, if arb_dex_running => {
                arb_market_data_ready.store(false, Ordering::Release);
                tracing::error!(
                    strategy_id = %arb_plan.strategy_id.as_str(),
                    result = ?result,
                    "Arbitrum ARB DEX connector stopped; new ARB entries are disabled"
                );
                arb_dex_running = false;
            }
            result = &mut linea_dex_task, if linea_dex_running => {
                linea_market_data_ready.store(false, Ordering::Release);
                result.context("Linea Lynex DEX connector task failed")??;
                bail!("Linea Lynex DEX connector stopped; process restart will rehydrate state");
            }
            result = &mut binance_balance_task => {
                result.context("Binance balance synchronization task failed")??;
                bail!("Binance balance synchronization stopped unexpectedly");
            }
            result = &mut wallet_balance_task => {
                result.context("wallet balance synchronization task failed")??;
                bail!("wallet balance synchronization stopped unexpectedly");
            }
            result = &mut esp_wallet_balance_task => {
                result.context("Arbitrum wallet balance synchronization task failed")??;
                bail!("Arbitrum wallet balance synchronization stopped unexpectedly");
            }
            result = &mut linea_wallet_balance_task => {
                result.context("Linea wallet balance synchronization task failed")??;
                bail!("Linea wallet balance synchronization stopped unexpectedly");
            }
        }
        if !first_ready_emitted && engine.phase() == RuntimePhase::Ready {
            engine.record_runtime_first_ready(bootstrap.process_started_at.elapsed());
            first_ready_emitted = true;
        }
        let adaptive_sizing_jobs = engine
            .take_adaptive_sizing_jobs()
            .into_iter()
            .chain(esp_engine.take_adaptive_sizing_jobs())
            .chain(arb_engine.take_adaptive_sizing_jobs())
            .chain(linea_engine.take_adaptive_sizing_jobs());
        for job in adaptive_sizing_jobs {
            let strategy_id = job.strategy_id()?;
            let submission = adaptive_sizing_slots.submit(&strategy_id, job)?;
            if submission.replaced || submission.queued_behind_running {
                if strategy_id == shadow_plan.strategy_id {
                    esp_engine.record_adaptive_sizing_overload(
                        &strategy_id,
                        submission.replaced,
                        adaptive_sizing_slots.total_retained_work(),
                    );
                } else if strategy_id == arb_plan.strategy_id {
                    arb_engine.record_adaptive_sizing_overload(
                        &strategy_id,
                        submission.replaced,
                        adaptive_sizing_slots.total_retained_work(),
                    );
                } else if strategy_id == linea_plan.strategy_id {
                    linea_engine.record_adaptive_sizing_overload(
                        &strategy_id,
                        submission.replaced,
                        adaptive_sizing_slots.total_retained_work(),
                    );
                } else {
                    engine.record_adaptive_sizing_overload(
                        &strategy_id,
                        submission.replaced,
                        adaptive_sizing_slots.total_retained_work(),
                    );
                }
            }
        }
        while let Some((_, job)) = adaptive_sizing_slots.take_ready() {
            adaptive_sizing_tasks.spawn_blocking(move || job.run());
        }
    }

    engine.shutdown();
    drop(rebalance_sender);
    if let Some(task) = rebalance_task.take() {
        task.abort();
        let _ = task.await;
    }
    binance_balance_task.abort();
    wallet_balance_task.abort();
    esp_wallet_balance_task.abort();
    linea_wallet_balance_task.abort();
    binance_clock_sync_task.abort();
    resource_balance_task.abort();
    let _ = binance_balance_task.await;
    let _ = wallet_balance_task.await;
    let _ = esp_wallet_balance_task.await;
    let _ = linea_wallet_balance_task.await;
    let _ = binance_clock_sync_task.await;
    let _ = resource_balance_task.await;
    dex_task.abort();
    let _ = dex_task.await;
    shadow_dex_task.abort();
    let _ = shadow_dex_task.await;
    arb_dex_task.abort();
    let _ = arb_dex_task.await;
    linea_dex_task.abort();
    let _ = linea_dex_task.await;
    if let Some(task) = live_chain_readiness_task {
        task.abort();
        let _ = task.await;
    }
    if let Some(task) = arb_chain_readiness_task {
        task.abort();
        let _ = task.await;
    }
    if let Some(task) = linea_chain_readiness_task {
        task.abort();
        let _ = task.await;
    }
    adaptive_sizing_tasks.abort_all();
    while adaptive_sizing_tasks.join_next().await.is_some() {}
    esp_engine.shutdown();
    arb_engine.shutdown();
    linea_engine.shutdown();
    drop(engine);
    drop(esp_engine);
    drop(arb_engine);
    drop(linea_engine);
    if let Some(task) = paper_trade_task.take() {
        task.await??;
    }
    if let Some(task) = dex_revert_diagnostic_task.take() {
        task.await??;
    }
    hot_telemetry_task.await??;
    esp_hot_telemetry_task.await??;
    arb_hot_telemetry_task.await??;
    linea_hot_telemetry_task.await??;
    portfolio_allocator_task.await?;
    writer_task.await??;
    if let Some(path) = runtime_ready_file
        && let Err(error) = std::fs::remove_file(&path)
    {
        tracing::warn!(path = %path.display(), %error, "failed to remove runtime readiness marker");
    }
    tracing::info!(
        rebalance_execution_mode = %config.rebalance_execution_mode,
        "arbitrage shadow service stopped"
    );
    Ok(())
}

async fn run_binance_clock_sync(
    mut client: BinanceAccountClient,
    sender: tokio::sync::mpsc::Sender<Result<BinanceClockSync, String>>,
) {
    let start = tokio::time::Instant::now() + BINANCE_CLOCK_SYNC_INTERVAL;
    let mut interval = tokio::time::interval_at(start, BINANCE_CLOCK_SYNC_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let observation = client
            .synchronize_clock_observed()
            .await
            .map_err(|error| format!("{error:#}"));
        if sender.send(observation).await.is_err() {
            return;
        }
    }
}

fn emit_chain_readiness(
    telemetry: &TelemetryHandle,
    engine_id: &str,
    pair: &arb_bot::domain::config::PairConfig,
    readiness: &ChainReadiness,
    readiness_source: &'static str,
) {
    telemetry.emit(
        "live_readiness",
        serde_json::json!({
            "engine_id": engine_id,
            "stage": "arbitrum_chain",
            "pair_id": pair.id,
            "network_id": "eip155:42161",
            "chain_id": readiness.chain_id,
            "block_number": readiness.block_number,
            "exact_token_contracts": readiness.exact_token_contracts,
            "token_code_present": readiness.token_code_present,
            "router_code_present": readiness.router_code_present,
            "native_gas_funded": readiness.native_gas_funded,
            "token_a_funded": readiness.token_a_funded,
            "token_b_funded": readiness.token_b_funded,
            "fresh_rpc_gas_price": readiness.fresh_rpc_gas_price,
            "allowance_policy": readiness.allowance_policy,
            "receipt_l1_fee_mode": readiness.receipt_l1_fee_mode,
            "readiness_source": readiness_source,
            "external_mutation_authorized": readiness.external_mutation_authorized,
            "ready": readiness.ready,
        }),
    );
}

fn emit_chain_readiness_failure(
    telemetry: &TelemetryHandle,
    engine_id: &str,
    pair: &arb_bot::domain::config::PairConfig,
    readiness_source: &'static str,
    error: &anyhow::Error,
) {
    telemetry.emit(
        "live_readiness",
        serde_json::json!({
            "engine_id": engine_id,
            "stage": "arbitrum_chain",
            "pair_id": pair.id,
            "network_id": "eip155:42161",
            "readiness_source": readiness_source,
            "probe_error": format!("{error:#}"),
            "external_mutation_authorized": false,
            "ready": false,
        }),
    );
}

async fn run_chain_readiness_refresh(
    probe: ChainReadinessProbe,
    telemetry: TelemetryHandle,
    engine_id: String,
    pair: arb_bot::domain::config::PairConfig,
    mut last_status: Option<ChainReadinessStatus>,
    execution_ready: Arc<AtomicBool>,
) {
    let start = tokio::time::Instant::now() + CHAIN_READINESS_REFRESH_INTERVAL;
    let mut interval = tokio::time::interval_at(start, CHAIN_READINESS_REFRESH_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        match probe.inspect().await {
            Ok(readiness) => {
                let status = readiness.status();
                execution_ready.store(readiness.ready, Ordering::Release);
                if last_status == Some(status) {
                    continue;
                }
                emit_chain_readiness(
                    &telemetry,
                    &engine_id,
                    &pair,
                    &readiness,
                    "background_transition",
                );
                if readiness.ready {
                    tracing::info!(
                        pair_id = pair.id,
                        block_number = readiness.block_number,
                        external_mutation_capability = readiness.external_mutation_authorized,
                        new_entry_authorized = true,
                        "ESP Arbitrum chain readiness became ready"
                    );
                } else {
                    tracing::warn!(
                        pair_id = pair.id,
                        block_number = readiness.block_number,
                        native_gas_funded = readiness.native_gas_funded,
                        token_a_funded = readiness.token_a_funded,
                        token_b_funded = readiness.token_b_funded,
                        fresh_rpc_gas_price = readiness.fresh_rpc_gas_price,
                        external_mutation_capability = readiness.external_mutation_authorized,
                        new_entry_authorized = false,
                        "ESP Arbitrum chain readiness degraded; ESP fails closed"
                    );
                }
                last_status = Some(status);
            }
            Err(error) => {
                execution_ready.store(false, Ordering::Release);
                if last_status == Some(ChainReadinessStatus::ProbeFailed) {
                    continue;
                }
                tracing::warn!(
                    pair_id = pair.id,
                    error = %error,
                    external_mutation_authorized = false,
                    "ESP Arbitrum chain-readiness probe failed; ESP fails closed"
                );
                emit_chain_readiness_failure(
                    &telemetry,
                    &engine_id,
                    &pair,
                    "background_transition",
                    &error,
                );
                last_status = Some(ChainReadinessStatus::ProbeFailed);
            }
        }
    }
}

async fn execute_rebalance_with_quote_retries(
    executor: &mut RebalanceExecutor,
    request: RebalanceExecutionRequest,
) -> Result<RebalanceExecutionOperation, String> {
    let result = executor.execute(request).await;
    complete_rebalance_with_quote_retries(executor, result).await
}

async fn recover_rebalance_with_quote_retries(
    executor: &mut RebalanceExecutor,
) -> Result<RebalanceExecutionOperation, String> {
    let result = executor.recover_active().await.and_then(|operation| {
        operation.context("active rebalance operation disappeared before recovery")
    });
    complete_rebalance_with_quote_retries(executor, result).await
}

async fn complete_rebalance_with_quote_retries(
    executor: &mut RebalanceExecutor,
    mut result: anyhow::Result<RebalanceExecutionOperation>,
) -> Result<RebalanceExecutionOperation, String> {
    let mut retry_attempt = 0_u32;
    loop {
        match result {
            Ok(operation) => return Ok(operation),
            Err(error) if is_retryable_quote_error(&error) => {
                retry_attempt = retry_attempt.saturating_add(1);
                let operation = match executor.active_operation() {
                    Ok(Some(operation)) => operation,
                    Ok(None) => {
                        return Err(format!(
                            "{error:#}; retryable Across quote failure left no active rebalance operation"
                        ));
                    }
                    Err(journal_error) => {
                        return Err(format!(
                            "{error:#}; failed to inspect active rebalance operation: {journal_error:#}"
                        ));
                    }
                };
                let delay = rebalance_quote_retry_delay(retry_attempt);
                tracing::warn!(
                    operation_id = %operation.intent.operation_id,
                    retry_attempt,
                    retry_delay_ms = delay.as_millis(),
                    error = %format!("{error:#}"),
                    "rebalance Across quote retry scheduled"
                );
                tokio::time::sleep(delay).await;
                result = executor.recover_active().await.and_then(|operation| {
                    operation
                        .context("active rebalance operation disappeared before Across quote retry")
                });
            }
            Err(error) => return Err(format!("{error:#}")),
        }
    }
}

fn rebalance_quote_retry_delay(retry_attempt: u32) -> Duration {
    let exponent = retry_attempt.saturating_sub(1).min(4);
    REBALANCE_QUOTE_RETRY_INITIAL_DELAY
        .saturating_mul(1_u32 << exponent)
        .min(REBALANCE_QUOTE_RETRY_MAX_DELAY)
}

fn dispatch_across_reconciliation(
    lane_busy: &mut bool,
    sender: Option<&tokio::sync::mpsc::Sender<RebalanceExecutorCommand>>,
) -> anyhow::Result<bool> {
    if *lane_busy {
        return Ok(false);
    }
    let sender = sender.context("Across reconciliation has no rebalance executor")?;
    let permit = match sender.try_reserve() {
        Ok(permit) => permit,
        Err(tokio::sync::mpsc::error::TrySendError::Full(())) => {
            bail!("rebalance executor queue is full while its lane is idle")
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(())) => {
            bail!("rebalance executor queue is closed")
        }
    };
    permit.send(RebalanceExecutorCommand::ReconcileAcross);
    *lane_busy = true;
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_next_rebalance_execution(
    lane_busy: &mut bool,
    next_target: &mut RebalanceExecutionTarget,
    primary_engine: &mut TradingEngine,
    arbitrum_engine: &mut TradingEngine,
    arb_engine: &mut TradingEngine,
    linea_engine: &mut TradingEngine,
    sender: Option<&tokio::sync::mpsc::Sender<RebalanceExecutorCommand>>,
    primary_pair: &arb_bot::domain::config::PairConfig,
    arbitrum_pair: &arb_bot::domain::config::PairConfig,
    arb_pair: &arb_bot::domain::config::PairConfig,
    linea_pair: &arb_bot::domain::config::PairConfig,
    wallet_owner: Address,
    capital_policy: Option<&CompiledCapitalPolicy>,
    runtime_limits: &RebalanceRuntimeLimits,
    rebalance_risk: &tokio::sync::watch::Receiver<RebalanceRisk>,
) -> anyhow::Result<()> {
    if *lane_busy {
        return Ok(());
    }

    primary_engine.refresh_pending_rebalance_execution();
    arbitrum_engine.refresh_pending_rebalance_execution();
    arb_engine.refresh_pending_rebalance_execution();
    linea_engine.refresh_pending_rebalance_execution();

    for target in [
        *next_target,
        next_target.other(),
        next_target.other().other(),
        next_target.other().other().other(),
    ] {
        let outcome = match target {
            RebalanceExecutionTarget::Primary => {
                dispatch_rebalance_execution(
                    primary_engine,
                    sender,
                    primary_pair,
                    wallet_owner,
                    target,
                    None,
                    None,
                    runtime_limits,
                )
                .await?
            }
            RebalanceExecutionTarget::ArbitrumEsp => {
                dispatch_rebalance_execution(
                    arbitrum_engine,
                    sender,
                    arbitrum_pair,
                    wallet_owner,
                    target,
                    capital_policy,
                    Some(rebalance_risk),
                    runtime_limits,
                )
                .await?
            }
            RebalanceExecutionTarget::ArbitrumArb => {
                dispatch_rebalance_execution(
                    arb_engine,
                    sender,
                    arb_pair,
                    wallet_owner,
                    target,
                    capital_policy,
                    Some(rebalance_risk),
                    runtime_limits,
                )
                .await?
            }
            RebalanceExecutionTarget::Linea => {
                dispatch_rebalance_execution(
                    linea_engine,
                    sender,
                    linea_pair,
                    wallet_owner,
                    target,
                    capital_policy,
                    Some(rebalance_risk),
                    runtime_limits,
                )
                .await?
            }
        };
        if apply_rebalance_dispatch_outcome(lane_busy, next_target, target, outcome) {
            break;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_rebalance_execution(
    engine: &mut TradingEngine,
    sender: Option<&tokio::sync::mpsc::Sender<RebalanceExecutorCommand>>,
    pair: &arb_bot::domain::config::PairConfig,
    wallet_owner: Address,
    target: RebalanceExecutionTarget,
    capital_policy: Option<&CompiledCapitalPolicy>,
    rebalance_risk: Option<&tokio::sync::watch::Receiver<RebalanceRisk>>,
    runtime_limits: &RebalanceRuntimeLimits,
) -> anyhow::Result<RebalanceDispatchOutcome> {
    engine.refresh_pending_rebalance_execution();
    if engine.pending_rebalance_execution().is_none() {
        return Ok(RebalanceDispatchOutcome::NoWork);
    }
    let runtime_maximum = {
        let evaluation = engine
            .pending_rebalance_execution()
            .context("rebalance pending work disappeared before runtime limit")?;
        runtime_limits
            .maximum_base_units_for(&evaluation.token_symbol, evaluation.token_decimals)?
    };
    engine.cap_pending_rebalance_amount(runtime_maximum)?;
    let rebalance_remaining = if target.is_direct_full_live() {
        let Some(evaluation) = engine.pending_rebalance_execution() else {
            return Ok(RebalanceDispatchOutcome::NoWork);
        };
        let action = evaluation
            .plan
            .action
            .as_ref()
            .context("rebalance pending rebalance evaluation has no action")?;
        let policy = capital_policy.context("rebalance dispatch has no compiled capital policy")?;
        let risk = rebalance_risk
            .context("rebalance dispatch has no durable risk publication")?
            .borrow()
            .clone();
        let Some(remaining) = remaining_rebalance_authority_on_chain(
            policy,
            &risk,
            &evaluation.token_symbol,
            action.direction,
            pair.chain.chain_id,
        )?
        else {
            engine.defer_pending_rebalance_execution(
                "rebalance concurrency, value, or fee limit reached",
            );
            return Ok(RebalanceDispatchOutcome::Deferred);
        };
        engine.cap_pending_rebalance_amount(remaining.maximum_source_debit)?;
        Some(remaining)
    } else {
        None
    };
    let Some(pending) = engine.pending_rebalance_execution().cloned() else {
        return Ok(RebalanceDispatchOutcome::NoWork);
    };
    let action = pending
        .plan
        .action
        .clone()
        .context("rebalance execution evaluation has no action")?;
    let maximum_fee = if target.is_direct_full_live() {
        let policy = capital_policy.context("rebalance dispatch has no compiled capital policy")?;
        ensure!(
            policy.external_mutation_authorized,
            "rebalance dispatch has no external mutation authority"
        );
        let remaining_fee = rebalance_remaining
            .context("rebalance dispatch lost its durable remaining authority")?
            .maximum_fee;
        let authorized_fee = if action.direction == arb_bot::rebalance::Direction::WalletToBinance {
            U256::ZERO
        } else {
            let maximum_with_positive_credit = action.amount.checked_sub(U256::ONE).context(
                "rebalance Binance withdrawal cannot preserve positive destination credit",
            )?;
            let bounded = remaining_fee.min(maximum_with_positive_credit);
            ensure!(
                !bounded.is_zero(),
                "rebalance Binance withdrawal has no remaining positive fee authority"
            );
            bounded
        };
        let proposal = match engine
            .authorize_pending_rebalance_allocation(authorized_fee)
            .await
        {
            Ok(proposal) => proposal.context("rebalance capital allocator returned no proposal")?,
            Err(error) if transient_capital_allocator_inventory_mismatch(&error) => {
                tracing::info!(
                    error = %format!("{error:#}"),
                    "rebalance allocation deferred while an active inventory reservation settles"
                );
                engine.defer_pending_rebalance_execution(
                    "capital allocator inventory snapshot is transiently unsettled",
                );
                return Ok(RebalanceDispatchOutcome::Deferred);
            }
            Err(error) => return Err(error),
        };
        ensure!(
            proposal.external_mutation_authorized
                && proposal.source_debit == action.amount
                && proposal.fee == authorized_fee,
            "rebalance capital allocator proposal differs from the pending rebalance"
        );
        Some(authorized_fee)
    } else {
        None
    };
    // Capital planning is asynchronous and must not reserve the sole
    // execution queue slot while it waits. In particular, an rebalance allocator
    // observation cannot head-of-line block an already eligible WLD rebalance.
    let sender = sender.context("rebalance engine produced live work without an executor")?;
    let permit = match sender.try_reserve() {
        Ok(permit) => permit,
        Err(tokio::sync::mpsc::error::TrySendError::Full(())) => {
            engine.defer_pending_rebalance_execution("rebalance executor queue is full");
            return Ok(RebalanceDispatchOutcome::Deferred);
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(())) => {
            bail!("rebalance executor queue is closed")
        }
    };
    let Some(evaluation) = engine.take_rebalance_execution()? else {
        return Ok(RebalanceDispatchOutcome::Deferred);
    };
    ensure!(
        evaluation == pending,
        "pending rebalance changed during capital allocation"
    );
    let token = [&pair.token_a, &pair.token_b]
        .into_iter()
        .find(|token| token.symbol == evaluation.token_symbol)
        .context("rebalance execution token is absent from the domain pair")?;
    let token_contract = token
        .contract
        .parse::<Address>()
        .context("rebalance execution token contract is invalid")?;
    permit.send(RebalanceExecutorCommand::Execute {
        target,
        request: Box::new(RebalanceExecutionRequest {
            authority: match target {
                RebalanceExecutionTarget::Primary => RebalanceExecutionAuthority::WorldChainV12,
                RebalanceExecutionTarget::ArbitrumEsp | RebalanceExecutionTarget::ArbitrumArb => {
                    ensure!(
                        capital_policy.is_some(),
                        "Arbitrum rebalance requires the permanent full-live capital policy"
                    );
                    RebalanceExecutionAuthority::ArbitrumFullLive
                }
                RebalanceExecutionTarget::Linea => {
                    ensure!(
                        capital_policy.is_some(),
                        "Linea rebalance requires the permanent full-live capital policy"
                    );
                    RebalanceExecutionAuthority::LineaFullLive
                }
            },
            token_symbol: evaluation.token_symbol,
            token_decimals: evaluation.token_decimals,
            token_contract,
            wallet_owner,
            action,
            binance_balance_before: evaluation.plan.projected.binance,
            wallet_balance_before: evaluation.plan.projected.wallet,
            revalidation_start_balance: evaluation.plan.start_balance,
            maximum_fee,
            approval_session_id: if target.is_direct_full_live() {
                Some(
                    capital_policy
                        .context("rebalance dispatch has no compiled capital policy")?
                        .approval_session_id
                        .clone(),
                )
            } else {
                None
            },
        }),
    });
    Ok(RebalanceDispatchOutcome::Submitted)
}

fn transient_capital_allocator_inventory_mismatch(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        matches!(
            cause.to_string().as_str(),
            "reserved portfolio amount exceeds observed balance"
                | "portfolio reservations exceed observed economic asset"
                | "allocator reservations exceed observed source inventory"
        )
    })
}

fn mark_runtime_ready() -> anyhow::Result<Option<PathBuf>> {
    let Some(path) = runtime_ready_marker_path()? else {
        return Ok(None);
    };
    std::fs::write(&path, b"ready\n").with_context(|| {
        format!(
            "failed to write runtime readiness marker {}",
            path.display()
        )
    })?;
    Ok(Some(path))
}

fn runtime_ready_marker_path() -> anyhow::Result<Option<PathBuf>> {
    let Some(path) = std::env::var_os("RUNTIME_READY_FILE") else {
        return Ok(None);
    };
    let path = PathBuf::from(path);
    ensure!(
        !path.as_os_str().is_empty(),
        "RUNTIME_READY_FILE must not be empty"
    );
    Ok(Some(path))
}

fn sync_runtime_ready_marker(
    path: Option<&std::path::Path>,
    marked: &mut bool,
    ready: bool,
) -> anyhow::Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    if ready {
        if !*marked {
            std::fs::write(path, b"ready\n").with_context(|| {
                format!(
                    "failed to write runtime readiness marker {}",
                    path.display()
                )
            })?;
            *marked = true;
        }
    } else if *marked || path.exists() {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to remove runtime readiness marker {}",
                        path.display()
                    )
                });
            }
        }
        *marked = false;
    }
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = signal(SignalKind::terminate())
            .expect("SIGTERM handler must be installable before the runtime loop starts");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn validate_binance_account(state: &BinanceAccountState) -> anyhow::Result<()> {
    ensure!(
        state.account.account_type == "SPOT",
        "Binance account type is {}, expected SPOT",
        state.account.account_type
    );
    ensure!(
        state.account.can_trade,
        "Binance account does not permit trading"
    );
    ensure!(
        state.symbol_rules.symbol == state.commission.symbol,
        "Binance symbol rules and commission refer to different symbols"
    );
    ensure!(
        !state.order_rate_limits.is_empty(),
        "Binance returned no current order-rate limits"
    );
    for limit in &state.order_rate_limits {
        ensure!(
            limit.rate_limit_type == "ORDERS",
            "unexpected Binance order rate-limit type {}",
            limit.rate_limit_type
        );
        ensure!(
            limit.count < limit.limit,
            "Binance {} {} order limit is exhausted ({}/{})",
            limit.interval_num,
            limit.interval,
            limit.count,
            limit.limit
        );
    }
    Ok(())
}

fn log_binance_account(state: &BinanceAccountState) {
    tracing::info!(
        binance_account_type = %state.account.account_type,
        binance_can_trade = state.account.can_trade,
        binance_can_deposit = state.account.can_deposit,
        binance_can_withdraw = state.account.can_withdraw,
        binance_permissions = ?state.account.permissions,
        binance_nonzero_balances = state.account.balances.len(),
        binance_clock_offset_ms = state.clock_offset_ms,
        symbol = %state.commission.symbol,
        binance_standard_maker_fee = %state.commission.standard_commission.maker,
        binance_standard_taker_fee = %state.commission.standard_commission.taker,
        binance_commission_discount_enabled_for_account =
            state.commission.discount.enabled_for_account,
        binance_commission_discount_enabled_for_symbol =
            state.commission.discount.enabled_for_symbol,
        binance_commission_discount_asset = %state.commission.discount.discount_asset,
        binance_commission_discount = %state.commission.discount.discount,
        binance_symbol_status = %state.symbol_rules.status,
        binance_base_asset = %state.symbol_rules.base_asset,
        binance_quote_asset = %state.symbol_rules.quote_asset,
        binance_price_tick = %state.symbol_rules.price.step,
        binance_lot_step = %state.symbol_rules.lot_size.step,
        binance_market_lot_step = %state.symbol_rules.market_lot_size.step,
        binance_min_notional = %state.symbol_rules.min_notional,
        binance_max_num_orders = state.symbol_rules.max_num_orders,
        binance_max_num_algo_orders = state.symbol_rules.max_num_algo_orders,
        binance_open_orders = state.open_orders.len(),
        binance_order_rate_limits = ?state.order_rate_limits,
        binance_wld_balance_present = state.balance("WLD").is_some(),
        binance_usdc_balance_present = state.balance("USDC").is_some(),
        binance_bnb_balance_present = state.balance("BNB").is_some(),
        "authenticated Binance Spot account hydrated"
    );
}

fn log_binance_capital(state: &CapitalRouteState) {
    tracing::info!(
        coin = %state.coin,
        deposit_all_enabled = state.deposit_all_enabled,
        withdrawal_all_enabled = state.withdrawal_all_enabled,
        direct_network = state.direct.as_ref().map(|network| network.network.as_str()),
        direct_deposit_available = state.direct_deposit_available(),
        direct_withdrawal_available = state.direct_withdrawal_available(),
        fallback_network = state.fallback.as_ref().map(|network| network.network.as_str()),
        fallback_deposit_available = state.fallback_deposit_available(),
        fallback_withdrawal_available = state.fallback_withdrawal_available(),
        "Binance capital routes hydrated"
    );
}

fn build_prepared_pool_inline(
    engine: &mut TradingEngine,
    request: PreparedPoolBuildRequest,
) -> anyhow::Result<()> {
    let timing = request.timing_handle();
    timing.mark_request_dispatch_started();
    timing.mark_request_dispatch_finished();
    let result = request.build()?;
    let timing = result.timing_handle();
    timing.mark_result_send_started();
    timing.mark_result_send_finished();
    result.mark_owner_received();
    engine.on_prepared_pool(result)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct StartupDexDrainStats {
    event_count: usize,
    pool_build_count: usize,
    max_queue_age_us: u128,
}

impl StartupDexDrainStats {
    fn observe(&mut self, event: &DexStreamEvent) {
        let queue_age_us = match event {
            DexStreamEvent::Log { received_at, .. } | DexStreamEvent::Head { received_at, .. } => {
                received_at.elapsed().as_micros()
            }
        };
        self.event_count = self.event_count.saturating_add(1);
        self.max_queue_age_us = self.max_queue_age_us.max(queue_age_us);
    }

    fn merge(&mut self, other: Self) {
        self.event_count = self.event_count.saturating_add(other.event_count);
        self.pool_build_count = self.pool_build_count.saturating_add(other.pool_build_count);
        self.max_queue_age_us = self.max_queue_age_us.max(other.max_queue_age_us);
    }
}

#[allow(clippy::too_many_arguments)]
fn drain_startup_dex_backlog(
    engine: &mut HotPathDecisionOwner<TradingEngine>,
    esp_engine: &mut TradingEngine,
    pending: &mut PreparedPoolBuildBatch,
    dex_receiver: &mut tokio::sync::mpsc::Receiver<DexStreamEvent>,
    shadow_dex_receiver: &mut tokio::sync::mpsc::Receiver<DexStreamEvent>,
    wallet_heads: &tokio::sync::watch::Sender<CanonicalBlock>,
    receipt_heads: &tokio::sync::watch::Sender<CanonicalBlock>,
    esp_wallet_heads: &tokio::sync::watch::Sender<CanonicalBlock>,
    esp_receipt_heads: &tokio::sync::watch::Sender<CanonicalBlock>,
) -> anyhow::Result<(StartupDexDrainStats, StartupDexDrainStats)> {
    let mut primary_total = StartupDexDrainStats::default();
    let mut shadow_total = StartupDexDrainStats::default();
    loop {
        let primary = drain_startup_primary_dex_backlog(
            engine,
            pending,
            dex_receiver,
            wallet_heads,
            receipt_heads,
        )?;
        let shadow = drain_startup_secondary_dex_backlog(
            esp_engine,
            shadow_dex_receiver,
            esp_wallet_heads,
            esp_receipt_heads,
        )?;
        let drained_events = primary.event_count.saturating_add(shadow.event_count);
        primary_total.merge(primary);
        shadow_total.merge(shadow);
        if drained_events == 0 {
            return Ok((primary_total, shadow_total));
        }
    }
}

fn drain_startup_primary_dex_backlog(
    engine: &mut TradingEngine,
    pending: &mut PreparedPoolBuildBatch,
    dex_receiver: &mut tokio::sync::mpsc::Receiver<DexStreamEvent>,
    wallet_heads: &tokio::sync::watch::Sender<CanonicalBlock>,
    receipt_heads: &tokio::sync::watch::Sender<CanonicalBlock>,
) -> anyhow::Result<StartupDexDrainStats> {
    let mut stats = StartupDexDrainStats::default();
    loop {
        while let Ok(event) = dex_receiver.try_recv() {
            stats.observe(&event);
            let wallet_head = match &event {
                DexStreamEvent::Head { head, .. } => Some(*head),
                DexStreamEvent::Log { .. } => None,
            };
            if let Some(request) = engine.on_startup_dex_event(event)? {
                pending.queue(request);
            }
            if let Some(head) = wallet_head
                && *wallet_heads.borrow() != head
            {
                wallet_heads.send_replace(head);
            }
            if let Some(head) = wallet_head
                && *receipt_heads.borrow() != head
            {
                receipt_heads.send_replace(head);
            }
        }
        let Some(request) = pending.pop_next() else {
            return Ok(stats);
        };
        build_prepared_pool_inline(engine, request)?;
        stats.pool_build_count = stats.pool_build_count.saturating_add(1);
    }
}

fn drain_startup_secondary_dex_backlog(
    engine: &mut TradingEngine,
    shadow_dex_receiver: &mut tokio::sync::mpsc::Receiver<DexStreamEvent>,
    wallet_heads: &tokio::sync::watch::Sender<CanonicalBlock>,
    receipt_heads: &tokio::sync::watch::Sender<CanonicalBlock>,
) -> anyhow::Result<StartupDexDrainStats> {
    let mut stats = StartupDexDrainStats::default();
    while let Ok(event) = shadow_dex_receiver.try_recv() {
        stats.observe(&event);
        let head = match &event {
            DexStreamEvent::Head { head, .. } => Some(*head),
            DexStreamEvent::Log { .. } => None,
        };
        if let Some(request) = engine.on_startup_dex_event(event)? {
            build_prepared_pool_inline(engine, request)?;
            stats.pool_build_count = stats.pool_build_count.saturating_add(1);
        }
        if let Some(head) = head {
            wallet_heads.send_replace(head);
            receipt_heads.send_replace(head);
        }
    }
    Ok(stats)
}

fn drain_dex_events_inline(
    engine: &mut TradingEngine,
    pending: &mut PreparedPoolBuildBatch,
    dex_receiver: &mut tokio::sync::mpsc::Receiver<arb_bot::market_data::alchemy::DexStreamEvent>,
    wallet_heads: &tokio::sync::watch::Sender<CanonicalBlock>,
    receipt_heads: &tokio::sync::watch::Sender<CanonicalBlock>,
) -> anyhow::Result<usize> {
    let mut drained = 0;
    while let Ok(event) = dex_receiver.try_recv() {
        drained += 1;
        if let Some(request) = process_dex_event_inline(engine, event, wallet_heads, receipt_heads)?
        {
            pending.queue(request);
        }
    }
    Ok(drained)
}

fn build_prepared_pools_interleaved(
    engine: &mut TradingEngine,
    pending: &mut PreparedPoolBuildBatch,
    dex_receiver: &mut tokio::sync::mpsc::Receiver<arb_bot::market_data::alchemy::DexStreamEvent>,
    wallet_heads: &tokio::sync::watch::Sender<CanonicalBlock>,
    receipt_heads: &tokio::sync::watch::Sender<CanonicalBlock>,
) -> anyhow::Result<(bool, usize)> {
    let mut prepared = false;
    let mut drained = 0;
    loop {
        drained +=
            drain_dex_events_inline(engine, pending, dex_receiver, wallet_heads, receipt_heads)?;
        let Some(request) = pending.pop_next() else {
            return Ok((prepared, drained));
        };
        build_prepared_pool_inline(engine, request)?;
        prepared = true;
    }
}

fn process_dex_event_inline(
    engine: &mut TradingEngine,
    event: arb_bot::market_data::alchemy::DexStreamEvent,
    wallet_heads: &tokio::sync::watch::Sender<CanonicalBlock>,
    receipt_heads: &tokio::sync::watch::Sender<CanonicalBlock>,
) -> anyhow::Result<Option<PreparedPoolBuildRequest>> {
    let wallet_head = match &event {
        arb_bot::market_data::alchemy::DexStreamEvent::Head { head, .. } => Some(*head),
        arb_bot::market_data::alchemy::DexStreamEvent::Log { .. } => None,
    };
    let prepared = engine.on_dex_event(event)?;
    if let Some(head) = wallet_head
        && *wallet_heads.borrow() != head
    {
        wallet_heads.send_replace(head);
    }
    if let Some(head) = wallet_head
        && *receipt_heads.borrow() != head
    {
        receipt_heads.send_replace(head);
    }
    Ok(prepared)
}

struct InitializedDex {
    mirror: DexMirror,
    stream: AlchemyDexStream,
    rpc: JsonRpcClient,
    timings: DexInitializationTimings,
}

#[derive(Clone, Copy)]
struct BootstrapTiming {
    process_started_at: Instant,
    domain_validation_complete_at: Instant,
    domain_load_us: u128,
}

async fn hydrate_network_wallet_registries(
    registry: &NetworkRuntimeRegistry,
    owner: Address,
    telemetry: &TelemetryHandle,
    engine_id: &str,
) -> anyhow::Result<Vec<WalletBalanceSnapshot>> {
    let snapshots = try_join_all(registry.runtimes().map(|runtime| async move {
        let tokens = runtime
            .plan()
            .assets
            .iter()
            .filter_map(|asset| {
                asset.contract.as_ref().map(|contract| {
                    Ok(TokenBalanceRequest {
                        symbol: asset.symbol.clone(),
                        contract: contract.parse::<Address>().with_context(|| {
                            format!(
                                "network {} asset {} has invalid contract",
                                runtime.plan().network_id.as_str(),
                                asset.venue_asset_id.as_str()
                            )
                        })?,
                    })
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        ensure!(
            !tokens.is_empty(),
            "network {} has no configured ERC-20 wallet assets",
            runtime.plan().network_id.as_str()
        );
        let snapshot = fetch_wallet_snapshot_coordinated(
            runtime.reads(),
            owner,
            runtime.plan().chain_id,
            &tokens,
            runtime.initial_head(),
        )
        .await?;
        Ok::<_, anyhow::Error>((runtime, snapshot))
    }))
    .await?;
    let mut hydrated = Vec::with_capacity(snapshots.len());
    for (runtime, snapshot) in snapshots {
        emit_network_wallet_hydrated(telemetry, engine_id, runtime, &snapshot);
        hydrated.push(snapshot);
    }
    Ok(hydrated)
}

fn emit_network_wallet_hydrated(
    telemetry: &TelemetryHandle,
    engine_id: &str,
    runtime: &arb_bot::network_runtime::NetworkRuntime,
    snapshot: &WalletBalanceSnapshot,
) {
    telemetry.emit(
        "network_wallet_hydrated",
        serde_json::json!({
            "engine_id": engine_id,
            "network_id": runtime.plan().network_id.as_str(),
            "chain_id": snapshot.chain_id,
            "block_number": snapshot.block_number,
            "block_hash": format!("{:#x}", snapshot.block_hash),
            "asset_count": snapshot.token_balances.len(),
            "batch_complete": snapshot.batch_complete,
            "batch_coordinator_queue_us": snapshot.batch_coordinator_queue_us,
            "batch_provider_us": snapshot.batch_provider_us,
            "batch_rpc_decode_us": snapshot.batch_rpc_decode_us,
            "batch_decode_us": snapshot.batch_decode_us,
            "batch_chunk_count": snapshot.batch_chunk_count,
            "batch_response_bytes": snapshot.batch_response_bytes,
        }),
    );
}

#[derive(Clone, Copy)]
struct DexInitializationTimings {
    total_us: u128,
    canonical_block_selection_us: u128,
    hydration_us: u128,
    filter_build_us: u128,
    subscription_ack_us: u128,
    backfill_head_us: u128,
    backfill_provider_us: u128,
    backfill_apply_us: u128,
    backfill_log_count: usize,
    backfill_applied_count: usize,
}

async fn initialize_dex(
    config: &config::AppConfig,
    domain_config: &LoadedDomainConfig,
    network_registry: Option<&NetworkRuntimeRegistry>,
) -> anyhow::Result<InitializedDex> {
    let total_started_at = Instant::now();
    let chain_id = domain_config
        .snapshot()
        .pairs
        .iter()
        .find(|pair| pair.market_data_enabled)
        .context("no enabled pair network")?
        .chain
        .chain_id;
    let runtime = network_registry
        .map(|registry| registry.get_by_chain_id(chain_id))
        .transpose()?;
    let (rpc, ws_endpoint) = match runtime {
        Some(runtime) => (runtime.rpc().clone(), runtime.ws_endpoint().to_owned()),
        None => {
            let (rpc_endpoint, ws_endpoint) = chain_endpoints(domain_config)?;
            (JsonRpcClient::new(rpc_endpoint)?, ws_endpoint)
        }
    };
    let canonical_block_started_at = Instant::now();
    let hydration_block = match runtime {
        Some(runtime) => runtime.initial_head(),
        None => rpc.latest_block().await?,
    };
    let canonical_block_selection_us = canonical_block_started_at.elapsed().as_micros();
    let hydration_started_at = Instant::now();
    let hydrated = match runtime {
        Some(runtime) => {
            DexHydrator::new_coordinated(runtime.reads())
                .hydrate_at(domain_config.snapshot(), hydration_block)
                .await?
        }
        None => {
            DexHydrator::new(&rpc)
                .hydrate_at(domain_config.snapshot(), hydration_block)
                .await?
        }
    };
    let hydration_us = hydration_started_at.elapsed().as_micros();
    let hydration_block = hydrated.block;
    let filter_build_started_at = Instant::now();
    let filters = build_log_filters(domain_config.snapshot(), &hydrated)?;
    let filter_build_us = filter_build_started_at.elapsed().as_micros();
    let subscription_started_at = Instant::now();
    let stream =
        connect_dex_stream(&ws_endpoint, &filters, config.dex_event_channel_capacity).await?;
    let subscription_ack_us = subscription_started_at.elapsed().as_micros();

    // The subscription is live before the upper backfill bound is captured.
    // Logs emitted during hydration/subscription are recovered over HTTP;
    // duplicate WSS notifications at or below this bound are ignored.
    let backfill_head_started_at = Instant::now();
    let backfill_head = rpc.latest_block().await?;
    let backfill_head_us = backfill_head_started_at.elapsed().as_micros();
    let backfill_provider_started_at = Instant::now();
    let mut backfill = Vec::new();
    if backfill_head.number > hydration_block.number {
        for filter in &filters {
            backfill.extend(
                rpc.get_logs(filter, hydration_block.number + 1, backfill_head.number)
                    .await?,
            );
        }
    }
    backfill.sort_unstable_by_key(|log| log.position());
    backfill.dedup_by(|right, left| {
        right.position() == left.position()
            && right.address == left.address
            && right.block_hash == left.block_hash
    });
    let adaptive_algebra_addresses: BTreeSet<_> = hydrated
        .pools
        .iter()
        .filter_map(|pool| match pool.identity {
            PoolIdentity::CamelotV3 { address } | PoolIdentity::LynexAlgebraV1_9 { address } => {
                Some(address)
            }
            _ => None,
        })
        .collect();
    let mut adaptive_algebra_timestamps = BTreeMap::<B256, u32>::new();
    for log in backfill
        .iter()
        .filter(|log| adaptive_algebra_addresses.contains(&log.address))
    {
        if let std::collections::btree_map::Entry::Vacant(entry) =
            adaptive_algebra_timestamps.entry(log.block_hash)
        {
            let (_, timestamp) = rpc
                .canonical_block_by_hash(log.block_number, log.block_hash)
                .await?;
            entry.insert(u32::try_from(timestamp).context("Algebra log timestamp exceeds uint32")?);
        }
    }
    let backfill_head_timestamp = if adaptive_algebra_addresses.is_empty() {
        None
    } else {
        Some(
            u32::try_from(rpc.canonical_block_timestamp(backfill_head).await?)
                .context("Algebra backfill head timestamp exceeds uint32")?,
        )
    };
    let backfill_provider_us = backfill_provider_started_at.elapsed().as_micros();

    let backfill_apply_started_at = Instant::now();
    let mut mirror = DexMirror::new(hydrated)?;
    let mut applied = 0_usize;
    for log in &backfill {
        let result = if adaptive_algebra_addresses.contains(&log.address) {
            mirror.apply_log_at_timestamp(
                log,
                *adaptive_algebra_timestamps
                    .get(&log.block_hash)
                    .context("Algebra backfill log timestamp is missing")?,
            )?
        } else {
            mirror.apply_log(log)?
        };
        if matches!(result, LogApplyResult::Applied { .. }) {
            applied += 1;
        }
    }
    mirror.finish_backfill_at(backfill_head, backfill_head_timestamp)?;
    let backfill_apply_us = backfill_apply_started_at.elapsed().as_micros();
    tracing::info!(
        hydration_block = hydration_block.number,
        ready_block = backfill_head.number,
        backfill_logs = backfill.len(),
        applied_logs = applied,
        pools = mirror.pool_count(),
        unavailable = mirror.unavailable_count(),
        rpc = ?rpc.stats(),
        "DEX mirror hydrated, backfilled, and subscribed"
    );
    Ok(InitializedDex {
        mirror,
        stream,
        rpc,
        timings: DexInitializationTimings {
            total_us: total_started_at.elapsed().as_micros(),
            canonical_block_selection_us,
            hydration_us,
            filter_build_us,
            subscription_ack_us,
            backfill_head_us,
            backfill_provider_us,
            backfill_apply_us,
            backfill_log_count: backfill.len(),
            backfill_applied_count: applied,
        },
    })
}

fn emit_bootstrap_telemetry(
    telemetry: &TelemetryHandle,
    config: &config::AppConfig,
    domain_config: &LoadedDomainConfig,
    bootstrap: BootstrapTiming,
    dex: DexInitializationTimings,
) {
    let chain_id = domain_config
        .snapshot()
        .pairs
        .iter()
        .find(|pair| pair.market_data_enabled)
        .map(|pair| pair.chain.chain_id);
    let stages = [
        (
            "process_to_domain_validation",
            bootstrap
                .domain_validation_complete_at
                .saturating_duration_since(bootstrap.process_started_at)
                .as_micros(),
        ),
        ("domain_bundle_load_validation", bootstrap.domain_load_us),
        ("dex_total", dex.total_us),
        (
            "canonical_block_selection",
            dex.canonical_block_selection_us,
        ),
        ("pool_hydration", dex.hydration_us),
        ("log_filter_build", dex.filter_build_us),
        ("subscription_ack", dex.subscription_ack_us),
        ("backfill_head_selection", dex.backfill_head_us),
        ("backfill_provider", dex.backfill_provider_us),
        ("backfill_apply_publication", dex.backfill_apply_us),
    ];
    for (stage, duration_us) in stages {
        telemetry.emit(
            "runtime_bootstrap_stage",
            serde_json::json!({
                "engine_id": config.engine_id,
                "domain_snapshot_id": domain_config.snapshot().snapshot_id,
                "domain_config_sha256": domain_config.fingerprint_sha256(),
                "network_id": chain_id.map(arb_bot::telemetry::network_id),
                "chain_id": chain_id,
                "stage": stage,
                "duration_us": duration_us,
                "backfill_log_count": dex.backfill_log_count,
                "backfill_applied_count": dex.backfill_applied_count,
                "outcome": "success",
            }),
        );
    }
}

fn record_longest_handler(
    longest_us: &mut u128,
    longest_name: &mut &'static str,
    handler_name: &'static str,
    duration: Duration,
) {
    let duration_us = duration.as_micros();
    if duration_us > *longest_us {
        *longest_us = duration_us;
        *longest_name = handler_name;
    }
}

fn report_strategy_dependency_faults(
    engine: &mut HotPathDecisionOwner<TradingEngine>,
    supervisor: &RootSupervisorPolicy,
) -> anyhow::Result<()> {
    for fault in engine.take_dependency_faults() {
        report_strategy_dependency_fault(supervisor, fault)?;
    }
    Ok(())
}

fn report_strategy_dependency_fault(
    supervisor: &RootSupervisorPolicy,
    fault: StrategyDependencyFault,
) -> anyhow::Result<()> {
    let class = if fault.dependency.contains("network_ingestion") {
        DependencyFaultClass::NetworkIngestion
    } else {
        DependencyFaultClass::Strategy
    };
    let decision = supervisor.decide(fault.strategy_id.as_str(), class)?;
    ensure!(
        !decision.process_termination_required,
        "a non-critical dependency fault unexpectedly required process termination"
    );
    let action = match decision.action {
        SupervisorAction::DegradeStrategy => "degrade_strategy",
        SupervisorAction::ReconnectShard => "reconnect_shard",
        SupervisorAction::DegradeNetwork => "degrade_network",
        SupervisorAction::FailFast => "fail_fast",
        SupervisorAction::ObserveOnly => "observe_only",
    };
    tracing::error!(
        binance_account_id = %decision.scope.binance_account_id,
        network_id = %fault.network_id,
        strategy_id = %fault.strategy_id.as_str(),
        execution_lane_id = %decision.scope.execution_lane_id,
        symbol = %fault.symbol,
        dependency = fault.dependency,
        supervisor_action = action,
        closes_new_mutations = decision.closes_new_mutations,
        process_termination_required = decision.process_termination_required,
        error_class = "dependency_owner_reported_error",
        "runtime dependency fault"
    );
    Ok(())
}

fn chain_endpoints(domain_config: &LoadedDomainConfig) -> anyhow::Result<(String, String)> {
    let mut enabled = domain_config
        .snapshot()
        .pairs
        .iter()
        .filter(|pair| pair.market_data_enabled);
    let first = enabled.next().context("no enabled pair RPC endpoint")?;
    ensure!(
        enabled.all(|pair| {
            pair.chain.rpc_url_env == first.chain.rpc_url_env
                && pair.chain.ws_url_env == first.chain.ws_url_env
        }),
        "run currently requires one shared chain RPC/WSS endpoint"
    );
    let rpc = std::env::var(&first.chain.rpc_url_env).with_context(|| {
        format!(
            "required environment variable {} is not set",
            first.chain.rpc_url_env
        )
    })?;
    let ws = std::env::var(&first.chain.ws_url_env).with_context(|| {
        format!(
            "required environment variable {} is not set",
            first.chain.ws_url_env
        )
    })?;
    Ok((rpc, ws))
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt()
        .with_env_filter(filter)
        .json()
        .with_current_span(false)
        .init();
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use alloy_primitives::B256;
    use arb_bot::{
        chain::rpc::CanonicalBlock,
        domain::compiled::{CompatibilityRole, load_compatibility_domain},
        market_data::alchemy::DexStreamEvent,
    };

    use super::{
        ACROSS_RECONCILIATION_INTERVAL, Command, LINEA_CHAIN_ID, RebalanceDispatchOutcome,
        RebalanceExecutionTarget, RebalanceExecutorCommand, StartupDexDrainStats,
        apply_rebalance_dispatch_outcome, command_owns_runtime_readiness,
        dispatch_across_reconciliation, esp_evm_journal_scope, linea_evm_journal_scope,
        linea_transport_subscription_retry_delay, rebalance_quote_retry_delay,
        sync_runtime_ready_marker, transient_capital_allocator_inventory_mismatch,
    };

    #[test]
    fn esp_scope_matches_the_production_runtime_journal_identity() {
        let scope = esp_evm_journal_scope(42_161);
        assert_eq!(scope.schema_version, 2);
        assert_eq!(scope.network_id, "eip155:42161");
        assert_eq!(
            scope.wallet_id, "eip155:42161:evm-wallet:primary",
            "wallet location ids are network-qualified in the compiled runtime"
        );
        assert_eq!(scope.strategy_id, "strategy:arbitrum-usdc-esp");

        let production = load_compatibility_domain(
            "config/domain/compiled-multi-pair-production.v1.json",
            CompatibilityRole::LiveRuntime,
            false,
        )
        .unwrap();
        let runtime = production
            .network_runtime
            .unwrap()
            .networks
            .into_iter()
            .find(|network| network.chain_id == 42_161)
            .unwrap();
        assert_eq!(runtime.network_id.as_str(), scope.network_id);
        assert_eq!(runtime.wallet_location_id.as_str(), scope.wallet_id);
    }

    #[test]
    fn linea_scope_matches_the_production_runtime_journal_identity() {
        let scope = linea_evm_journal_scope();
        assert_eq!(scope.schema_version, 2);
        assert_eq!(scope.network_id, "eip155:59144");
        assert_eq!(scope.wallet_id, "eip155:59144:evm-wallet:primary");
        assert_eq!(scope.strategy_id, "strategy:linea-usdt-usdc");

        let production = load_compatibility_domain(
            "config/domain/compiled-multi-pair-production.v1.json",
            CompatibilityRole::LiveRuntime,
            false,
        )
        .unwrap();
        let runtime = production
            .network_runtime
            .unwrap()
            .networks
            .into_iter()
            .find(|network| network.chain_id == LINEA_CHAIN_ID)
            .unwrap();
        assert_eq!(runtime.network_id.as_str(), scope.network_id);
        assert_eq!(runtime.wallet_location_id.as_str(), scope.wallet_id);
        assert!(!runtime.execution_enabled);
    }

    #[test]
    fn process_start_removes_a_stale_runtime_readiness_marker() {
        let path = std::env::temp_dir().join(format!(
            "arb-bot-stale-ready-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, b"ready\n").unwrap();
        let mut marked = true;

        sync_runtime_ready_marker(Some(&path), &mut marked, false).unwrap();

        assert!(!marked);
        assert!(!path.exists());
    }

    #[test]
    fn operator_subcommand_does_not_own_runtime_readiness() {
        assert!(command_owns_runtime_readiness(&Command::Run));
        assert!(command_owns_runtime_readiness(&Command::CollectPrices));
        assert!(!command_owns_runtime_readiness(
            &Command::BinanceCapitalRecovery {
                coin: "USDC".to_owned(),
                network: "OPTIMISM".to_owned(),
                deposit_transaction_hash: Some("0x01".to_owned()),
                withdraw_order_id: None,
            }
        ));
    }

    #[test]
    fn rebalance_quote_retry_backoff_is_bounded() {
        assert_eq!(rebalance_quote_retry_delay(1), Duration::from_secs(5));
        assert_eq!(rebalance_quote_retry_delay(2), Duration::from_secs(10));
        assert_eq!(rebalance_quote_retry_delay(3), Duration::from_secs(20));
        assert_eq!(rebalance_quote_retry_delay(4), Duration::from_secs(40));
        assert_eq!(rebalance_quote_retry_delay(5), Duration::from_secs(60));
        assert_eq!(rebalance_quote_retry_delay(100), Duration::from_secs(60));
    }

    #[test]
    fn linea_transport_subscription_retry_backoff_is_bounded() {
        assert_eq!(
            linea_transport_subscription_retry_delay(1),
            Duration::from_millis(250)
        );
        assert_eq!(
            linea_transport_subscription_retry_delay(2),
            Duration::from_millis(500)
        );
        assert_eq!(
            linea_transport_subscription_retry_delay(100),
            Duration::from_millis(500)
        );
    }

    #[tokio::test]
    async fn across_reconciliation_poll_claims_only_an_idle_lane() {
        assert_eq!(ACROSS_RECONCILIATION_INTERVAL, Duration::from_secs(30));
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let mut lane_busy = false;

        assert!(dispatch_across_reconciliation(&mut lane_busy, Some(&sender)).unwrap());
        assert!(lane_busy);
        assert!(matches!(
            receiver.recv().await,
            Some(RebalanceExecutorCommand::ReconcileAcross)
        ));
        assert!(!dispatch_across_reconciliation(&mut lane_busy, Some(&sender)).unwrap());
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn transient_allocator_inventory_mismatch_is_deferred_without_hiding_other_errors() {
        assert!(transient_capital_allocator_inventory_mismatch(
            &anyhow::anyhow!("reserved portfolio amount exceeds observed balance")
        ));
        assert!(transient_capital_allocator_inventory_mismatch(
            &anyhow::anyhow!("allocator reservations exceed observed source inventory")
                .context("rebalance allocation failed")
        ));
        assert!(!transient_capital_allocator_inventory_mismatch(
            &anyhow::anyhow!("capital allocator returned a malformed proposal")
        ));
    }

    #[test]
    fn deferred_rebalance_remains_retryable_and_success_rotates_lane_fairness() {
        let mut lane_busy = false;
        let mut next_target = RebalanceExecutionTarget::ArbitrumEsp;

        assert!(!apply_rebalance_dispatch_outcome(
            &mut lane_busy,
            &mut next_target,
            RebalanceExecutionTarget::ArbitrumEsp,
            RebalanceDispatchOutcome::Deferred,
        ));
        assert!(!lane_busy);
        assert_eq!(next_target, RebalanceExecutionTarget::ArbitrumEsp);

        // This is the next supervisor tick after a temporary reservation has
        // settled: the same target is still eligible and can now be submitted.
        assert!(apply_rebalance_dispatch_outcome(
            &mut lane_busy,
            &mut next_target,
            RebalanceExecutionTarget::ArbitrumEsp,
            RebalanceDispatchOutcome::Submitted,
        ));
        assert!(lane_busy);
        assert_eq!(next_target, RebalanceExecutionTarget::ArbitrumArb);

        lane_busy = false;
        assert!(apply_rebalance_dispatch_outcome(
            &mut lane_busy,
            &mut next_target,
            RebalanceExecutionTarget::ArbitrumArb,
            RebalanceDispatchOutcome::Submitted,
        ));
        assert_eq!(next_target, RebalanceExecutionTarget::Linea);
    }

    #[test]
    fn startup_dex_backlog_has_separate_count_and_queue_age() {
        let mut first = StartupDexDrainStats::default();
        first.observe(&DexStreamEvent::Head {
            head: CanonicalBlock {
                number: 1,
                hash: B256::repeat_byte(1),
                parent_hash: B256::ZERO,
            },
            timestamp: 1,
            received_at: Instant::now() - Duration::from_millis(2),
        });
        first.pool_build_count = 1;
        let second = StartupDexDrainStats {
            event_count: 2,
            pool_build_count: 3,
            max_queue_age_us: 7_000,
        };
        first.merge(second);
        assert_eq!(first.event_count, 3);
        assert_eq!(first.pool_build_count, 4);
        assert!(first.max_queue_age_us >= 7_000);
    }
}

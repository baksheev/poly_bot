use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::Write,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use alloy_primitives::{Address, B256, U256};
use anyhow::{Context, bail, ensure};
use arb_bot::{
    across::{
        AcrossClient, AcrossQuoteRequest, OPTIMISM_CHAIN_ID, OPTIMISM_USDC, WORLD_CHAIN_CHAIN_ID,
        WORLD_CHAIN_USDC, is_retryable_quote_error, validate_quote,
    },
    arbitrage::{
        CanaryJournalRisk, EntryPreflightHandle, ExecutionMode, LegRole, LegStatus,
        PaperTradeCoordinator, TradeJournalScope, TradeStage, paper_trade_channel,
    },
    balances::{
        BalanceEvent, BalanceSource, BalanceSync, WalletBalanceSnapshot, WalletReadClient,
        binance_snapshot, fetch_wallet_snapshot, fetch_wallet_snapshot_coordinated,
        spawn_balance_sync, spawn_wallet_balance_sync,
    },
    binance::account::{BinanceAccountClient, BinanceAccountState, BinanceClockSync},
    binance::capital::{
        CapitalRecoverySnapshot, CapitalRouteState, TravelRuleWithdrawalRecord, WithdrawalRecord,
        select_capital_routes,
    },
    binance::{
        execution::BinanceExecutionService,
        order_journal::{BinanceOrderJournal, BinanceOrderJournalScope, BinanceOrderProgress},
        runtime::SharedBinanceRuntime,
        user_data::{UserDataEvent, UserDataStream},
        validation::{BinanceCanaryKind, execute_order_round_trip},
        ws_api::BinanceWsApiClient,
    },
    chain::rpc::{CanonicalBlock, JsonRpcClient},
    config::{self, Cli, Command},
    dex::{
        events::build_log_filters,
        execution::{AllowanceRequirement, DexExecutionService, DexExecutor, UniswapProtocol},
        hydration::{DexHydrator, PoolIdentity},
        mirror::{DexMirror, LogApplyResult},
        revert_diagnostics::dex_revert_diagnostic_channel,
        validation::{execute_recovery_sell, execute_round_trip},
    },
    domain::{
        compiled::{
            CompatibilityRole, CompiledBinanceRuntimePlan, CompiledCapitalAllocatorMode,
            CompiledCapitalCanaryPolicy, CompiledGraphSummary, CompiledHotPathRuntimePlan,
            CompiledNetworkGasPolicy, CompiledNetworkRuntimePlan, CompiledPortfolioRuntimePlan,
            compile_manifest_to_path, load_compatibility_domain, load_source_domain_for_pair,
        },
        config::{DexProvider, LiveCanaryConfig, LoadedDomainConfig},
    },
    engine::{AdaptiveSizingJob, AdaptiveSizingTaskResult, BinanceFeeBps, TradingEngine},
    execution_accounting::{CommissionAssetValuation, binance_leg_result},
    hot_telemetry,
    inventory::SharedInventoryReservations,
    live_execution::{
        ComposedLiveLegExecutor, ComposedLiveLegExecutorConfig, LiveCanaryPolicy, LiveRiskLimits,
        RoutedLiveLegExecutor, live_trade_channel,
    },
    m8_readiness::{
        ARBITRUM_CHAIN_ID, M8_CHAIN_READINESS_REFRESH_INTERVAL, M8ChainReadiness,
        M8ChainReadinessProbe, M8ChainReadinessStatus, inspect_chain_readiness,
        validate_binance_readiness, validate_rebalance_readiness,
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
    portfolio::{PortfolioCatalog, capital_allocator_channel, remaining_m10_rebalance_authority},
    rebalance::{
        ApprovedAbsentMasterTransferRecovery, ApprovedAbsentStandardWithdrawalRecovery,
        BinanceAddressVerificationTransferArtifact, RebalanceCanaryRisk,
        RebalanceExecutionAuthority, RebalanceExecutionOperation, RebalanceExecutionProgress,
        RebalanceExecutionRequest, RebalanceExecutor, RebalanceRuntimeLimits, RebalanceTracker,
        V12RebalanceParityAdapter, execute_binance_address_verification_transfer,
        plan_direct_prefunding, rebalance_base_units_to_decimal,
        rebalance_decimal_to_base_units_floor, route_candidates_from_capital,
    },
    state::{QuoteApplyResult, RuntimePhase, RuntimeState, TopOfBook},
    strategy_runtime::{
        CompiledStrategyDependencyIndex, FairLatestOnlySizingScheduler, HotPathDecisionOwner,
        StrategyDependencyFault, StrategyEvaluator,
    },
    supervision::{DependencyFaultClass, DependencyScope, RootSupervisorPolicy, SupervisorAction},
    telemetry::{
        ARBITRAGE_RESULT_KIND, ExecutionLatencyTelemetry, PRIMARY_BINANCE_ACCOUNT_ID,
        TelemetryHandle, TelemetryWriter, execution_lane_id,
    },
    wallet::{
        EvmJournalScope, EvmWallet, OPTIMISM_RPC_URL_ENV, ReviewedPrebroadcastRejection,
        TokenBalanceRequest, WALLET_JOURNAL_PATH_ENV, hydrate_chain_wallet,
        recover_exact_rejected_before_broadcast,
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
const ARBITRAGE_BINANCE_ORDER_JOURNAL_PATH_ENV: &str = "ARBITRAGE_BINANCE_ORDER_JOURNAL_PATH";
const BINANCE_CLOCK_SYNC_INTERVAL: Duration = Duration::from_secs(60);
const DEX_REVERT_DIAGNOSTIC_CHANNEL_CAPACITY: usize = 32;
const MAXIMUM_CONCURRENT_ADAPTIVE_SIZING_WORKERS: usize = 4;
const REBALANCE_QUOTE_RETRY_INITIAL_DELAY: Duration = Duration::from_secs(5);
const REBALANCE_QUOTE_RETRY_MAX_DELAY: Duration = Duration::from_secs(60);

fn m9_canary_evm_journal_scope(chain_id: u64) -> EvmJournalScope {
    let network_id = format!("eip155:{chain_id}");
    EvmJournalScope {
        schema_version: EvmJournalScope::SCHEMA_VERSION,
        wallet_id: format!("{network_id}:evm-wallet:primary"),
        network_id,
        strategy_id: "strategy:arbitrum-usdc-esp".to_owned(),
    }
}

fn m9_canary_allowance_requirements(
    canary: &LiveCanaryConfig,
    risk: CanaryJournalRisk,
    token_a_balance: U256,
    token_b_balance: U256,
    now_unix_us: u64,
) -> anyhow::Result<Option<(U256, U256)>> {
    let maximum_total = canary
        .max_total_notional_token_a_base_units
        .parse::<u128>()
        .context("M9 cumulative canary cap is invalid")?;
    let maximum_loss = canary
        .max_realized_loss_token_a_base_units
        .parse::<u128>()
        .context("M9 realized-loss cap is invalid")?;
    ensure!(
        risk.admitted_parent_count <= usize::from(canary.max_parent_trades)
            && risk.failed_parent_count <= usize::from(canary.max_failed_parent_trades)
            && risk.active_parent_count <= usize::from(canary.max_concurrent_trades)
            && risk.admitted_notional_token_a_base_units <= maximum_total
            && risk.realized_loss_token_a_base_units <= maximum_loss,
        "durable M9 risk exceeds the reviewed canary limits"
    );
    let remaining_notional = maximum_total
        .checked_sub(risk.admitted_notional_token_a_base_units)
        .context("durable M9 notional exceeds the reviewed canary cap")?;
    let rollout_window_open = risk.first_admitted_unix_us.is_none_or(|first| {
        now_unix_us.saturating_sub(first)
            <= canary.rollout_duration_seconds.saturating_mul(1_000_000)
    });
    let new_parent_authorized = remaining_notional > 0
        && risk.admitted_parent_count < usize::from(canary.max_parent_trades)
        && risk.failed_parent_count < usize::from(canary.max_failed_parent_trades)
        && risk.active_parent_count < usize::from(canary.max_concurrent_trades)
        && risk.realized_loss_token_a_base_units < maximum_loss
        && rollout_window_open;
    if !new_parent_authorized {
        return Ok(None);
    }

    let bootstrap_token_b = U256::from_str_radix(&canary.minimum_wallet_token_b_base_units, 10)
        .context("M9 ESP bootstrap target is invalid")?;
    let token_a_required = token_a_balance.min(U256::from(remaining_notional));
    let token_b_required = token_b_balance.min(bootstrap_token_b);
    ensure!(
        !token_a_required.is_zero() && !token_b_required.is_zero(),
        "M9 remaining allowance authority requires both wallet tokens"
    );
    Ok(Some((token_a_required, token_b_required)))
}

fn m9_allowance_operation_id(symbol: &str, required: U256) -> String {
    format!("rustarb-m9-setup-v3-{symbol}.v2-{required}")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RebalanceExecutionTarget {
    Primary,
    ArbitrumCanary,
}

enum RebalanceExecutorEvent {
    Recovery {
        target: RebalanceExecutionTarget,
        result: Result<RebalanceExecutionOperation, String>,
        active_operation_after: bool,
    },
    Execution {
        target: RebalanceExecutionTarget,
        result: Result<RebalanceExecutionOperation, String>,
        active_operation_after: bool,
    },
}

fn rebalance_target(operation: &RebalanceExecutionOperation) -> RebalanceExecutionTarget {
    if operation
        .intent
        .scope
        .as_ref()
        .is_some_and(|scope| scope.strategy_id == "rebalance-arbitrum-usdc-esp-m10")
    {
        RebalanceExecutionTarget::ArbitrumCanary
    } else {
        RebalanceExecutionTarget::Primary
    }
}

fn emit_m10_rebalance_risk(
    telemetry: &TelemetryHandle,
    engine_id: &str,
    executor: &RebalanceExecutor,
) {
    match executor.m10_canary_risk() {
        Ok(risk) => telemetry.emit(
            "m10_rebalance_risk_snapshot",
            serde_json::json!({
                "engine_id": engine_id,
                "approval_session_id": executor
                    .m10_approval_session_id()
                    .unwrap_or("unconfigured"),
                "transfer_count": risk.transfer_count,
                "active_transfer_count": risk.active_transfer_count,
                "failed_transfer_count": risk.failed_transfer_count,
                "token_a_debit": risk.token_a_debit.to_string(),
                "token_b_debit": risk.token_b_debit.to_string(),
                "token_a_maximum_fee": risk.token_a_maximum_fee.to_string(),
                "token_b_maximum_fee": risk.token_b_maximum_fee.to_string(),
                "first_started_at_unix_ms": risk.first_started_at_unix_ms,
                "outcome": "success",
            }),
        ),
        Err(error) => telemetry.emit(
            "m10_rebalance_risk_snapshot",
            serde_json::json!({
                "engine_id": engine_id,
                "approval_session_id": executor
                    .m10_approval_session_id()
                    .unwrap_or("unconfigured"),
                "outcome": "failed",
                "error": format!("{error:#}"),
            }),
        ),
    }
}

fn emit_m10_rebalance_saga(
    telemetry: &TelemetryHandle,
    engine_id: &str,
    target: RebalanceExecutionTarget,
    result: &Result<RebalanceExecutionOperation, String>,
    executor: &RebalanceExecutor,
    started_at: Instant,
    recovered: bool,
) {
    if target != RebalanceExecutionTarget::ArbitrumCanary {
        return;
    }
    let operation = result
        .as_ref()
        .ok()
        .or_else(|| executor.active_operation().ok().flatten())
        .or_else(|| executor.latest_m10_operation());
    let saga_duration_us = started_at.elapsed().as_micros();
    telemetry.emit(
        "m10_rebalance_saga",
        serde_json::json!({
            "engine_id": engine_id,
            "strategy_id": "rebalance-arbitrum-usdc-esp-m10",
            "approval_session_id": executor
                .m10_approval_session_id()
                .unwrap_or("unconfigured"),
            "operation_id": operation.map(|operation| &operation.intent.operation_id),
            "token": operation.map(|operation| &operation.intent.token_symbol),
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
    // RUNTIME_READY_FILE is supplied by the production environment, so clear
    // an emptyDir marker left by a crashed process before configuration,
    // tracing, domain validation, or the first await.
    let runtime_ready_path = runtime_ready_marker_path()?;
    let mut runtime_ready_marked = runtime_ready_path
        .as_ref()
        .is_some_and(|path| path.exists());
    sync_runtime_ready_marker(
        runtime_ready_path.as_deref(),
        &mut runtime_ready_marked,
        false,
    )?;
    load_dotenv()?;
    init_tracing();

    let cli = Cli::parse();
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
        Command::ReplayM11Capacity {
            artifact,
            frames_per_pair,
            target_cpu_class,
        } => {
            let report = arb_bot::capacity_replay::run_m11_capacity_replay(
                artifact,
                frames_per_pair,
                target_cpu_class.as_deref(),
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&report)
                    .context("failed to serialize M11 capacity replay report")?
            );
            Ok(())
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
        Command::BinanceWithdrawalStatus {
            coin,
            withdraw_order_id,
        } => binance_withdrawal_status(&cli.config, &coin, &withdraw_order_id).await,
        Command::BinanceTravelRuleWithdrawalStatus { tr_id } => {
            binance_travel_rule_withdrawal_status(&cli.config, tr_id).await
        }
        Command::PrefundArbitrumCanary {
            live_confirmation,
            marker_path,
        } => {
            prefund_arbitrum_canary(&cli.config, &live_confirmation, &marker_path, None, false)
                .await
        }
        Command::DiagnoseArbitrumEspWithdrawal { confirmation } => {
            ensure!(
                confirmation == "DIAGNOSE_ESP_031031",
                "ESP diagnostic requires ARBITRUM_ESP_DIAGNOSTIC_CONFIRMATION=DIAGNOSE_ESP_031031"
            );
            prefund_arbitrum_canary(
                &cli.config,
                "PREFUND_ARBITRUM_M9",
                std::path::Path::new("/var/lib/arb-bot/m9-prefunding-complete.json"),
                Some(std::path::Path::new(
                    "config/strategies/usdc-esp-arbitrum.v4.json",
                )),
                true,
            )
            .await
        }
        Command::BinanceEspAddressVerificationTransfer {
            artifact,
            confirmation,
            journal_path,
        } => {
            binance_esp_address_verification_transfer(
                &cli.config,
                &artifact,
                &confirmation,
                journal_path,
            )
            .await
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
        Command::AcrossUsdcQuote {
            origin_chain_id,
            amount,
        } => across_usdc_quote(&cli.config, origin_chain_id, amount).await,
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
                "v3" => UniswapProtocol::V3,
                "v4" => UniswapProtocol::V4,
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
                "v3" => UniswapProtocol::V3,
                "v4" => UniswapProtocol::V4,
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

async fn binance_esp_address_verification_transfer(
    config: &config::AppConfig,
    artifact_path: &std::path::Path,
    confirmation: &str,
    journal_path: PathBuf,
) -> anyhow::Result<()> {
    ensure!(
        confirmation == "SEND_998700_USDC_ARBITRUM_TO_BINANCE_VERIFY_20260730",
        "Binance ESP address verification transfer requires the exact production confirmation"
    );
    let artifact = BinanceAddressVerificationTransferArtifact::load(artifact_path)?;
    let wallet = EvmWallet::from_env()?;
    let configured_wallet = config
        .evm_wallet_address
        .parse::<Address>()
        .context("EVM_WALLET_ADDRESS is invalid")?;
    ensure!(
        wallet.address() == configured_wallet,
        "Binance address verification signer differs from EVM_WALLET_ADDRESS"
    );
    let arbitrum_endpoint = std::env::var("ARBITRUM_RPC_URL")
        .context("ARBITRUM_RPC_URL is required for Binance address verification")?;
    let outcome = execute_binance_address_verification_transfer(
        &artifact,
        JsonRpcClient::new(arbitrum_endpoint)?,
        wallet,
        journal_path,
        Duration::from_secs(10 * 60),
    )
    .await?;
    tracing::info!(
        operation_id = %outcome.operation_id,
        network = "ARBITRUM",
        token = "USDC",
        amount_base_units = %outcome.amount,
        recipient = %outcome.recipient,
        transaction_hash = %outcome.transaction_hash,
        bridge_used = false,
        "exact Binance ESP address verification deposit test completed"
    );
    Ok(())
}

async fn prefund_arbitrum_canary(
    config: &config::AppConfig,
    live_confirmation: &str,
    marker_path: &std::path::Path,
    source_config_override: Option<&std::path::Path>,
    recovery_only: bool,
) -> anyhow::Result<()> {
    ensure!(
        live_confirmation == "PREFUND_ARBITRUM_M9",
        "Arbitrum prefunding requires ARBITRUM_PREFUNDING_LIVE_CONFIRMATION=PREFUND_ARBITRUM_M9"
    );
    let source_domain = load_source_domain_for_pair(
        source_config_override.unwrap_or(&config.domain_config_path),
        "arbitrum-usdc-esp",
    )?;
    let pair = source_domain
        .snapshot()
        .pairs
        .iter()
        .find(|pair| pair.id == "arbitrum-usdc-esp")
        .context("compiled domain omitted the approved Arbitrum ESP pair")?;
    let canary = pair
        .live_canary
        .as_ref()
        .context("Arbitrum ESP pair omitted the live canary policy")?;
    let prefunding = canary
        .prefunding_rebalance
        .as_ref()
        .context("versioned artifact does not approve one-shot Arbitrum prefunding")?;
    ensure!(
        !canary.rebalance_mutations_enabled
            && !pair.rebalance.enabled
            && prefunding.binance_network == "ARBITRUM"
            && prefunding.maximum_transfer_count == 2,
        "one-shot prefunding cannot enable steady-state Arbitrum rebalance"
    );

    let wallet = EvmWallet::from_env()?;
    let configured_wallet = config
        .evm_wallet_address
        .parse::<Address>()
        .context("EVM_WALLET_ADDRESS is invalid")?;
    ensure!(
        wallet.address() == configured_wallet,
        "Arbitrum prefunding signer differs from EVM_WALLET_ADDRESS"
    );
    let arbitrum_endpoint = std::env::var(&pair.chain.rpc_url_env).with_context(|| {
        format!(
            "required environment variable {} is not set",
            pair.chain.rpc_url_env
        )
    })?;
    let arbitrum_rpc = JsonRpcClient::new(arbitrum_endpoint)?;
    ensure!(
        arbitrum_rpc.chain_id().await? == ARBITRUM_CHAIN_ID,
        "prefunding RPC is not Arbitrum One"
    );
    if let Some(recovery) = &prefunding.approved_evm_prebroadcast_rejection {
        let arbitrum_journal_path =
            std::env::var(ARBITRAGE_ARBITRUM_WALLET_JOURNAL_PATH_ENV).with_context(|| {
                format!(
                    "required environment variable {ARBITRAGE_ARBITRUM_WALLET_JOURNAL_PATH_ENV} is not set"
                )
            })?;
        let transaction_hash = recovery
            .transaction_hash
            .parse::<B256>()
            .context("approved EVM rejection transaction hash is invalid")?;
        let recovered = recover_exact_rejected_before_broadcast(
            &arbitrum_rpc,
            arbitrum_journal_path,
            &ReviewedPrebroadcastRejection {
                operation_id: recovery.operation_id.clone(),
                chain_id: ARBITRUM_CHAIN_ID,
                wallet: configured_wallet,
                nonce: recovery.nonce,
                transaction_hash,
                scope: m9_canary_evm_journal_scope(ARBITRUM_CHAIN_ID),
            },
        )
        .await?;
        tracing::info!(
            operation_id = recovery.operation_id,
            transaction_hash = %transaction_hash,
            nonce = recovery.nonce,
            rpc_error_code = recovery.rpc_error_code,
            recovered,
            "reviewed Arbitrum fee-cap rejection is proven absent and closed before M9 startup"
        );
    }
    if validate_prefunding_marker(
        marker_path,
        source_domain.fingerprint_sha256(),
        &source_domain.snapshot().snapshot_id,
        configured_wallet,
        &canary.minimum_wallet_token_a_base_units,
        &canary.minimum_wallet_token_b_base_units,
        &prefunding.production_approval_recorded_at_utc,
    )? {
        tracing::info!(
            marker_path = %marker_path.display(),
            wallet = %configured_wallet,
            "durable Arbitrum prefunding marker already exists; refusing to fund again"
        );
        return Ok(());
    }
    let world_endpoint = std::env::var("ALCHEMY_WORLDCHAIN_RPC_URL")
        .context("ALCHEMY_WORLDCHAIN_RPC_URL is required for journal recovery")?;
    let optimism_endpoint = std::env::var(OPTIMISM_RPC_URL_ENV).with_context(|| {
        format!("required environment variable {OPTIMISM_RPC_URL_ENV} is not set")
    })?;
    let world_rpc = JsonRpcClient::new(world_endpoint)?;
    let optimism_rpc = JsonRpcClient::new(optimism_endpoint)?;

    let mut trading_binance = BinanceAccountClient::from_env(config)?;
    trading_binance.synchronize_clock().await?;
    let coins = trading_binance.all_coin_information().await?;
    let trading_account = trading_binance.account_information().await?;
    let mut treasury_binance = BinanceAccountClient::from_treasury_env(config)?;
    let subaccount_email = std::env::var("BINANCE_SUBACCOUNT_EMAIL")
        .context("Arbitrum prefunding requires BINANCE_SUBACCOUNT_EMAIL")?;
    treasury_binance.synchronize_clock().await?;
    if !recovery_only {
        wait_for_binance_address_verification(
            &treasury_binance,
            configured_wallet,
            Duration::from_secs(10 * 60),
        )
        .await?;
    }
    if recovery_only {
        let questionnaire = treasury_binance
            .travel_rule_questionnaire_requirements()
            .await?;
        let address_verifications = treasury_binance.address_verification_list().await?;
        let master_account = treasury_binance.account_information().await?;
        let master_subaccount = treasury_binance
            .subaccount_spot_assets(&subaccount_email)
            .await?;
        for symbol in [&pair.token_a.symbol, &pair.token_b.symbol] {
            let capital = coins
                .iter()
                .find(|coin| coin.coin == *symbol)
                .with_context(|| format!("Binance omitted {symbol} capital state"))?;
            let trading_free = trading_account
                .balances
                .iter()
                .find(|balance| balance.asset == *symbol)
                .map_or(Decimal::ZERO, |balance| balance.free);
            let master_free = master_account
                .balances
                .iter()
                .find(|balance| balance.asset == *symbol)
                .map_or(Decimal::ZERO, |balance| balance.free);
            let master_view_subaccount_free = master_subaccount
                .balances
                .iter()
                .find(|balance| balance.asset == *symbol)
                .map_or(Decimal::ZERO, |balance| balance.free);
            tracing::info!(
                token = symbol,
                questionnaire_country_code = questionnaire
                    .questionnaire_country_code
                    .as_deref()
                    .unwrap_or("none"),
                trading_subaccount_free = %trading_free,
                master_spot_free = %master_free,
                master_view_subaccount_free = %master_view_subaccount_free,
                deposit_all_enabled = capital.deposit_all_enable,
                withdrawal_all_enabled = capital.withdraw_all_enable,
                "read-only ESP prefunding account and Travel Rule capability probe"
            );
            for network in &capital.network_list {
                tracing::info!(
                    token = symbol,
                    network = network.network,
                    network_name = network.name,
                    deposit_enabled = network.deposit_enable,
                    withdrawal_enabled = network.withdraw_enable,
                    busy = network.busy,
                    withdrawal_fee = %network.withdraw_fee,
                    withdrawal_minimum = %network.withdraw_min,
                    withdrawal_maximum = %network.withdraw_max,
                    withdrawal_multiple = %network.withdraw_integer_multiple,
                    "read-only Binance capital network capability"
                );
            }
        }
        let matching_address_verifications = address_verifications
            .iter()
            .filter(|record| {
                record
                    .wallet_address
                    .eq_ignore_ascii_case(&format!("{configured_wallet:#x}"))
            })
            .collect::<Vec<_>>();
        tracing::info!(
            wallet = %configured_wallet,
            matching_records = matching_address_verifications.len(),
            "read-only Binance address verification probe"
        );
        for record in matching_address_verifications {
            tracing::info!(
                wallet = %configured_wallet,
                token = record.token,
                network = record.network,
                status = record.status,
                send_to = ?record.address_questionnaire.send_to,
                is_address_owner = ?record.address_questionnaire.is_address_owner,
                verify_method = ?record.address_questionnaire.verify_method,
                satoshi_token = record.address_questionnaire.satoshi_token,
                "read-only matching Binance address verification record"
            );
        }
    }
    let transaction_journal_path = std::env::var(WALLET_JOURNAL_PATH_ENV).with_context(|| {
        format!("required environment variable {WALLET_JOURNAL_PATH_ENV} is not set")
    })?;
    let token_a_target = U256::from_str_radix(&canary.minimum_wallet_token_a_base_units, 10)
        .context("M9 token_a prefunding target is invalid")?;
    let token_b_target = U256::from_str_radix(&canary.minimum_wallet_token_b_base_units, 10)
        .context("M9 token_b prefunding target is invalid")?;
    let token_a_fee_cap =
        U256::from_str_radix(&prefunding.maximum_token_a_withdrawal_fee_base_units, 10)
            .context("M9 token_a withdrawal fee cap is invalid")?;
    let token_b_fee_cap =
        U256::from_str_radix(&prefunding.maximum_token_b_withdrawal_fee_base_units, 10)
            .context("M9 token_b withdrawal fee cap is invalid")?;
    let token_a_debit_cap = U256::from_str_radix(&prefunding.maximum_token_a_debit_base_units, 10)
        .context("M9 token_a debit cap is invalid")?;
    let token_b_debit_cap = U256::from_str_radix(&prefunding.maximum_token_b_debit_base_units, 10)
        .context("M9 token_b debit cap is invalid")?;
    let approved_recovery = prefunding.approved_travel_rule_recovery.as_ref();
    let approved_manual_credit = prefunding.approved_manual_token_b_credit.as_ref();

    let mut direct_networks = BTreeMap::new();
    for token in [&pair.token_a, &pair.token_b] {
        let capital = select_capital_routes(
            &coins,
            &token.symbol,
            &prefunding.binance_network,
            "OPTIMISM",
        )?;
        let network = capital
            .direct
            .filter(|network| network.network == prefunding.binance_network)
            .with_context(|| {
                format!(
                    "{} direct Arbitrum withdrawal route is absent",
                    token.symbol
                )
            })?;
        ensure!(
            capital.withdrawal_all_enabled && network.withdrawal_available(),
            "{} direct Arbitrum withdrawal is not live",
            token.symbol
        );
        direct_networks.insert(token.symbol.clone(), network);
    }

    let mut direct_read_rpcs = BTreeMap::new();
    direct_read_rpcs.insert(ARBITRUM_CHAIN_ID, arbitrum_rpc.clone());
    let mut executor = RebalanceExecutor::hydrate(
        trading_binance,
        treasury_binance,
        subaccount_email,
        AcrossClient::new(config)?,
        world_rpc,
        optimism_rpc,
        direct_read_rpcs,
        wallet,
        config.rebalance_executor_journal_path.clone(),
        transaction_journal_path.into(),
        RebalanceRuntimeLimits {
            maximum_wld: config.rebalance_max_wld_amount,
            maximum_usdc: rebalance_base_units_to_decimal(
                token_a_debit_cap,
                pair.token_a.decimals,
            )?,
            maximum_esp: rebalance_base_units_to_decimal(token_b_debit_cap, pair.token_b.decimals)?,
            operation_timeout: Duration::from_secs(config.rebalance_executor_timeout_seconds),
        },
    )
    .await?;
    let active = executor.active_operation()?.cloned();
    if let Some(active) = &active {
        ensure!(
            active.intent.token_symbol == pair.token_a.symbol
                || active.intent.token_symbol == pair.token_b.symbol,
            "active prefunding operation has an unexpected token"
        );
    }
    executor.log_active_operation_recovery_evidence().await?;
    let recovered = if recovery_only {
        match (approved_recovery, active) {
            (Some(recovery), Some(active))
                if active.intent.token_symbol == recovery.rejected_token_symbol
                    && matches!(
                        active.progress,
                        RebalanceExecutionProgress::BinanceTransferCompleted { .. }
                    ) =>
            {
                let rejected_amount =
                    U256::from_str_radix(&recovery.rejected_token_amount_base_units, 10)
                        .context("approved Travel Rule rejected amount is invalid")?;
                Some(
                    executor
                        .close_approved_travel_rule_rejection(
                            &recovery.rejected_token_symbol,
                            rejected_amount,
                            configured_wallet,
                            &prefunding.binance_network,
                            ARBITRUM_CHAIN_ID,
                            &format!(
                                "approved deterministic Travel Rule rejection HTTP {} code {}: {}",
                                recovery.rejected_http_status,
                                recovery.rejected_error_code,
                                recovery.rejected_error_message
                            ),
                        )
                        .await?,
                )
            }
            (Some(_), Some(_)) => {
                bail!("active rebalance operation differs from the approved ESP incident")
            }
            (Some(_), None) => None,
            (None, _) => bail!("versioned ESP incident recovery approval is absent"),
        }
    } else if let (Some(recovery), Some(active)) = (approved_manual_credit, active.as_ref())
        && active.intent.operation_id == recovery.operation_id
    {
        let gross_debit = U256::from_str_radix(&recovery.gross_debit_base_units, 10)
            .context("approved manual ESP gross debit is invalid")?;
        let expected_credit = U256::from_str_radix(&recovery.expected_credit_base_units, 10)
            .context("approved manual ESP credit is invalid")?;
        let expected_fee = U256::from_str_radix(&recovery.expected_fee_base_units, 10)
            .context("approved manual ESP fee is invalid")?;
        let wallet_balance_before =
            U256::from_str_radix(&recovery.wallet_balance_before_base_units, 10)
                .context("approved manual ESP wallet balance is invalid")?;
        let transaction_hash = recovery
            .transaction_hash
            .parse::<B256>()
            .context("approved manual ESP transaction hash is invalid")?;
        Some(
            executor
                .recover_approved_manual_direct_credit(
                    &recovery.operation_id,
                    &recovery.token_symbol,
                    gross_debit,
                    expected_credit,
                    expected_fee,
                    wallet_balance_before,
                    configured_wallet,
                    &prefunding.binance_network,
                    ARBITRUM_CHAIN_ID,
                    transaction_hash,
                )
                .await?,
        )
    } else if let (Some(recovery), Some(active)) = (approved_recovery, active) {
        let rejected_amount = U256::from_str_radix(&recovery.rejected_token_amount_base_units, 10)
            .context("approved Travel Rule rejected amount is invalid")?;
        if active.intent.token_symbol == recovery.rejected_token_symbol
            && active.intent.amount == rejected_amount
            && active.intent.wallet_owner == configured_wallet
            && active.intent.direction == arb_bot::rebalance::Direction::BinanceToWallet
            && active.intent.route
                == (arb_bot::rebalance::Route::Direct {
                    binance_network: prefunding.binance_network.clone(),
                    chain_id: ARBITRUM_CHAIN_ID,
                })
            && matches!(
                active.progress,
                RebalanceExecutionProgress::BinanceTransferCompleted { .. }
            )
        {
            Some(
                executor
                    .close_approved_travel_rule_rejection(
                        &recovery.rejected_token_symbol,
                        rejected_amount,
                        configured_wallet,
                        &prefunding.binance_network,
                        ARBITRUM_CHAIN_ID,
                        &format!(
                            "approved deterministic Travel Rule rejection HTTP {} code {}: {}",
                            recovery.rejected_http_status,
                            recovery.rejected_error_code,
                            recovery.rejected_error_message
                        ),
                    )
                    .await?,
            )
        } else {
            executor.recover_active().await?
        }
    } else {
        executor.recover_active().await?
    };
    if let Some(recovered) = recovered {
        tracing::info!(
            operation_id = %recovered.intent.operation_id,
            progress = ?recovered.progress,
            "recovered the sole durable rebalance operation before Arbitrum prefunding"
        );
    }
    if recovery_only {
        let recovery = approved_recovery.context("approved ESP incident recovery is absent")?;
        let rejected_amount = U256::from_str_radix(&recovery.rejected_token_amount_base_units, 10)
            .context("approved Travel Rule rejected amount is invalid")?;
        let closed = executor.operations().values().any(|operation| {
            operation.intent.token_symbol == recovery.rejected_token_symbol
                && operation.intent.amount == rejected_amount
                && operation.intent.wallet_owner == configured_wallet
                && matches!(
                    &operation.progress,
                    RebalanceExecutionProgress::Failed { reason }
                        if reason.contains("approved deterministic Travel Rule rejection")
                )
        });
        ensure!(
            closed,
            "approved ESP incident was not closed in the durable rebalance journal"
        );
        tracing::info!(
            token = recovery.rejected_token_symbol,
            rejected_error_code = recovery.rejected_error_code,
            new_withdrawal_submitted = false,
            "ESP Travel Rule incident closed after capital history and failed unbroadcast Travel Rule history proved no withdrawal"
        );
        return Ok(());
    }
    ensure!(
        approved_recovery.is_none() || prefunding.retry_after_verified_address,
        "ESP Travel Rule incident was closed without an approved retry after address verification"
    );

    let targets = [
        (
            &pair.token_a,
            token_a_target,
            token_a_fee_cap,
            token_a_debit_cap,
        ),
        (
            &pair.token_b,
            token_b_target,
            token_b_fee_cap,
            token_b_debit_cap,
        ),
    ];
    let mut transfer_count = 0_u16;
    for (token, target, fee_cap, debit_cap) in targets {
        let contract = token
            .contract
            .parse::<Address>()
            .with_context(|| format!("{} contract is invalid", token.symbol))?;
        let wallet_before = arbitrum_rpc
            .erc20_balance(contract, configured_wallet)
            .await?;
        let binance_before_decimal = trading_account
            .balances
            .iter()
            .find(|balance| balance.asset == token.symbol)
            .map_or(Decimal::ZERO, |balance| balance.free);
        let binance_before =
            rebalance_decimal_to_base_units_floor(binance_before_decimal, token.decimals)?;
        let network = direct_networks
            .get(&token.symbol)
            .with_context(|| format!("{} Arbitrum route disappeared", token.symbol))?;
        let Some(plan) = plan_direct_prefunding(
            target,
            wallet_before,
            token.decimals,
            network,
            ARBITRUM_CHAIN_ID,
            fee_cap,
            debit_cap,
        )?
        else {
            tracing::info!(
                token = token.symbol,
                wallet_balance = wallet_before.to_string(),
                target = target.to_string(),
                "Arbitrum canary prefunding target already satisfied"
            );
            continue;
        };
        if token.symbol == pair.token_b.symbol {
            let recovery =
                approved_recovery.context("approved ESP Travel Rule recovery is absent")?;
            let rejected_amount =
                U256::from_str_radix(&recovery.rejected_token_amount_base_units, 10)
                    .context("approved Travel Rule rejected amount is invalid")?;
            let completed = executor
                .retry_approved_failed_travel_rule_with_local_entity(
                    &recovery.rejected_token_symbol,
                    rejected_amount,
                    configured_wallet,
                    &prefunding.binance_network,
                    ARBITRUM_CHAIN_ID,
                )
                .await?;
            let wallet_after = match completed.progress {
                arb_bot::rebalance::RebalanceExecutionProgress::Completed {
                    wallet_balance_after,
                    ..
                } => wallet_balance_after,
                _ => bail!("approved ESP retry did not reach a terminal completed state"),
            };
            ensure!(
                wallet_after >= target,
                "{} prefunding completed below the approved wallet target",
                token.symbol
            );
            continue;
        }
        ensure!(
            binance_before >= plan.requested_debit,
            "{} Binance free balance is below the approved prefunding debit",
            token.symbol
        );
        transfer_count = transfer_count
            .checked_add(1)
            .context("prefunding transfer count overflow")?;
        ensure!(
            transfer_count <= prefunding.maximum_transfer_count,
            "prefunding would exceed the approved transfer count"
        );
        tracing::info!(
            token = token.symbol,
            wallet_balance_before = wallet_before.to_string(),
            target = target.to_string(),
            requested_debit = plan.requested_debit.to_string(),
            expected_credit = plan.expected_credit.to_string(),
            withdrawal_fee = plan.withdrawal_fee.to_string(),
            transfer_number = transfer_count,
            "executing approved one-shot Arbitrum canary prefunding transfer"
        );
        let completed = executor
            .execute(RebalanceExecutionRequest {
                authority: RebalanceExecutionAuthority::ArbitrumM9Prefunding,
                token_symbol: token.symbol.clone(),
                token_decimals: token.decimals,
                token_contract: contract,
                wallet_owner: configured_wallet,
                action: plan.action,
                binance_balance_before: binance_before,
                wallet_balance_before: wallet_before,
                canary_maximum_fee: None,
                canary_approval_session_id: None,
            })
            .await?;
        let wallet_after = match completed.progress {
            arb_bot::rebalance::RebalanceExecutionProgress::Completed {
                wallet_balance_after,
                ..
            } => wallet_balance_after,
            _ => bail!("prefunding rebalance did not reach a terminal completed state"),
        };
        ensure!(
            wallet_after >= target,
            "{} prefunding completed below the approved wallet target",
            token.symbol
        );
    }
    let final_usdc = arbitrum_rpc
        .erc20_balance(
            pair.token_a
                .contract
                .parse()
                .context("Arbitrum USDC contract is invalid")?,
            configured_wallet,
        )
        .await?;
    let final_esp = arbitrum_rpc
        .erc20_balance(
            pair.token_b
                .contract
                .parse()
                .context("Arbitrum ESP contract is invalid")?,
            configured_wallet,
        )
        .await?;
    ensure!(
        final_usdc >= token_a_target && final_esp >= token_b_target,
        "Arbitrum prefunding final balances are below the versioned M9 targets"
    );
    write_prefunding_marker(
        marker_path,
        source_domain.fingerprint_sha256(),
        &source_domain.snapshot().snapshot_id,
        configured_wallet,
        &canary.minimum_wallet_token_a_base_units,
        &canary.minimum_wallet_token_b_base_units,
        &prefunding.production_approval_recorded_at_utc,
    )?;
    tracing::info!(
        wallet = %configured_wallet,
        usdc_base_units = final_usdc.to_string(),
        esp_base_units = final_esp.to_string(),
        transfer_count,
        steady_state_rebalance_enabled = false,
        "approved one-shot Arbitrum canary prefunding completed"
    );
    Ok(())
}

async fn wait_for_binance_address_verification(
    treasury_binance: &BinanceAccountClient,
    wallet: Address,
    timeout: Duration,
) -> anyhow::Result<()> {
    ensure!(
        timeout >= Duration::from_secs(60) && timeout <= Duration::from_secs(15 * 60),
        "Binance address verification wait is outside the reviewed bounds"
    );
    let started_at = Instant::now();
    let mut last_status = None;
    loop {
        let records = treasury_binance.address_verification_list().await?;
        let matching = records
            .iter()
            .filter(|record| {
                record
                    .wallet_address
                    .eq_ignore_ascii_case(&format!("{wallet:#x}"))
                    && record.network == "ARBITRUM"
                    && record.token == "USDC"
            })
            .collect::<Vec<_>>();
        ensure!(
            matching.len() <= 1,
            "Binance returned multiple USDC Arbitrum verification records for the production wallet"
        );
        let status = matching
            .first()
            .map_or("MISSING", |record| record.status.as_str());
        if last_status.as_deref() != Some(status) {
            tracing::info!(
                wallet = %wallet,
                token = "USDC",
                network = "ARBITRUM",
                status,
                "waiting for the approved Binance address verification before direct ESP prefunding"
            );
            last_status = Some(status.to_owned());
        }
        if let Some(record) = matching.first()
            && record.status == "VERIFIED"
        {
            ensure!(
                record.address_questionnaire.is_address_owner == Some(1)
                    && record.address_questionnaire.verify_method == Some(1)
                    && record.address_questionnaire.satoshi_token == "USDC",
                "verified Binance address record differs from the approved Satoshi ownership test"
            );
            tracing::info!(
                wallet = %wallet,
                token = record.token,
                network = record.network,
                status = record.status,
                "Binance address ownership verification completed before direct ESP prefunding"
            );
            return Ok(());
        }
        ensure!(
            status == "MISSING" || status == "PENDING",
            "Binance address verification entered an unsupported terminal state"
        );
        ensure!(
            started_at.elapsed() < timeout,
            "Binance address verification did not complete within the reviewed wait"
        );
        tokio::time::sleep(Duration::from_secs(15)).await;
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_prefunding_marker(
    path: &std::path::Path,
    domain_fingerprint: &str,
    snapshot_id: &str,
    wallet: Address,
    token_a_target: &str,
    token_b_target: &str,
    approval_recorded_at_utc: &str,
) -> anyhow::Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect prefunding marker {}", path.display()))?;
    ensure!(
        metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
        "prefunding marker is not a regular file"
    );
    let marker: serde_json::Value = serde_json::from_slice(
        &std::fs::read(path)
            .with_context(|| format!("failed to read prefunding marker {}", path.display()))?,
    )
    .context("prefunding marker is invalid JSON")?;
    ensure!(
        !domain_fingerprint.is_empty()
            && marker["schema_version"] == 1
            && marker["domain_fingerprint_sha256"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
            && marker["snapshot_id"] == snapshot_id
            && marker["wallet"] == format!("{wallet:#x}")
            && marker["token_a_target_base_units"] == token_a_target
            && marker["token_b_target_base_units"] == token_b_target
            && marker["approval_recorded_at_utc"] == approval_recorded_at_utc
            && marker["completed"] == true
            && marker.as_object().is_some_and(|object| object.len() == 8),
        "prefunding marker differs from the approved M9 funding identity"
    );
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
fn write_prefunding_marker(
    path: &std::path::Path,
    domain_fingerprint: &str,
    snapshot_id: &str,
    wallet: Address,
    token_a_target: &str,
    token_b_target: &str,
    approval_recorded_at_utc: &str,
) -> anyhow::Result<()> {
    ensure!(
        !path.as_os_str().is_empty() && path.file_name().is_some(),
        "prefunding marker path is invalid"
    );
    let parent = path
        .parent()
        .context("prefunding marker has no parent directory")?;
    ensure!(
        parent.is_dir(),
        "prefunding marker parent directory does not exist"
    );
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_nanos();
    let temp_path = path.with_extension(format!("tmp-{suffix}"));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options
        .open(&temp_path)
        .with_context(|| format!("failed to create prefunding marker {}", temp_path.display()))?;
    let mut bytes = serde_json::to_vec(&serde_json::json!({
        "schema_version": 1,
        "domain_fingerprint_sha256": domain_fingerprint,
        "snapshot_id": snapshot_id,
        "wallet": format!("{wallet:#x}"),
        "token_a_target_base_units": token_a_target,
        "token_b_target_base_units": token_b_target,
        "approval_recorded_at_utc": approval_recorded_at_utc,
        "completed": true,
    }))?;
    bytes.push(b'\n');
    file.write_all(&bytes)
        .context("failed to write prefunding marker")?;
    file.sync_all()
        .context("failed to fsync prefunding marker")?;
    drop(file);
    std::fs::rename(&temp_path, path).with_context(|| {
        format!(
            "failed to atomically install prefunding marker {}",
            path.display()
        )
    })?;
    OpenOptions::new()
        .read(true)
        .open(parent)
        .context("failed to open prefunding marker directory")?
        .sync_all()
        .context("failed to fsync prefunding marker directory")?;
    Ok(())
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
    let (hot_telemetry, hot_telemetry_task) =
        hot_telemetry::channel(&config, opportunities.pairs(), &mirror, telemetry.clone())?;
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
        arb_bot::market_data::alchemy::DexStreamEvent::Log { log, .. } => {
            let LogApplyResult::Applied { pool_index, .. } = mirror.apply_log(&log)? else {
                return Ok(false);
            };
            let request = opportunities.request_pool_refresh(pool_index, mirror)?;
            let result = request.build()?;
            Ok(opportunities.finish_pool_refresh(result)?.is_some())
        }
        arb_bot::market_data::alchemy::DexStreamEvent::Head { head, received_at } => {
            mirror.apply_head(head, received_at)?;
            Ok(false)
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
        let pool_indices = pair.pool_indices().to_vec();
        for pool_index in pool_indices {
            let pool = mirror.pool(pool_index)?;
            let (provider, pool_identity, fee_pips) = match pool.identity {
                PoolIdentity::V3 { address, fee_pips } => {
                    ("uniswap_v3", address.to_string(), fee_pips)
                }
                PoolIdentity::V4 { pool_id, fee_pips } => {
                    ("uniswap_v4", pool_id.to_string(), fee_pips)
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
            "Arbitrum M9 execution policy must be mutation-enabled with fail-closed gas pricing"
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
    if let Some(dependencies) = hot_path_dependencies.as_ref() {
        ensure!(
            dependencies
                .plan()
                .strategies
                .iter()
                .filter(|strategy| strategy.execute)
                .count()
                == 2,
            "M9 production hot path requires exactly two executable strategies"
        );
        ensure!(
            dependencies
                .plan()
                .strategies
                .iter()
                .filter(|strategy| strategy.observe && !strategy.execute)
                .count()
                == 0,
            "M9 production hot path cannot retain a non-mutating ESP shadow capability"
        );
        let executable = dependencies
            .plan()
            .strategies
            .iter()
            .filter(|strategy| strategy.execute)
            .collect::<Vec<_>>();
        ensure!(
            executable.len() == 2
                && executable
                    .iter()
                    .any(|strategy| strategy.symbol == "WLDUSDC")
                && executable
                    .iter()
                    .any(|strategy| strategy.symbol == "ESPUSDC"),
            "M9 permits execution only for WLDUSDC and ESPUSDC"
        );
        let account_id = compiled_binance_runtime
            .as_ref()
            .context("M6 ownership graph requires one Binance account")?
            .account_id
            .as_str();
        let evm_owner_count = network_registry
            .as_ref()
            .context("M6 ownership graph requires network runtimes")?
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
            "M6 execution ownership graph validated"
        );
    }
    let (initialized_dex, shadow_initialized_dex) =
        if let Some(shadow) = shadow_strategy_plan.as_ref() {
            let (primary, observed) = tokio::try_join!(
                initialize_dex(&config, domain_config.as_ref(), network_registry.as_ref()),
                initialize_dex(&config, &shadow.domain_config, network_registry.as_ref()),
            )?;
            (primary, Some(observed))
        } else {
            (
                initialize_dex(&config, domain_config.as_ref(), network_registry.as_ref()).await?,
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
            runtime.executable_symbols.len() == 2
                && runtime.executable_symbols.contains(&pair.binance.symbol)
                && runtime.executable_symbols.contains("ESPUSDC"),
            "compiled Binance capabilities must enable the reviewed WLD and ESP symbols"
        );
    }
    let mut binance_account_client = BinanceAccountClient::from_env(&config)?;
    let startup_binance_clock_sync = binance_account_client.synchronize_clock_observed().await?;
    let mut user_data_stream =
        UserDataStream::connect(&config, startup_binance_clock_sync.offset_ms).await?;
    let shared_binance_account = binance_account_client
        .hydrate_symbols_after_subscription(binance_symbols.clone(), startup_binance_clock_sync)
        .await?;
    let m8_pair = shadow_strategy_plan
        .as_ref()
        .and_then(|strategy| strategy.domain_config.snapshot().pairs.first())
        .context("compiled M9 ESP strategy has no canary pair")?
        .clone();
    match shared_binance_account
        .symbol(&m8_pair.binance.symbol)
        .context("shared Binance account omitted the M9 canary symbol")
        .and_then(|state| validate_binance_readiness(&m8_pair, state))
    {
        Ok(readiness) => telemetry.emit(
            "m9_live_readiness",
            serde_json::json!({
                "engine_id": config.engine_id,
                "stage": "binance_order_matrix",
                "pair_id": m8_pair.id,
                "network_id": "eip155:42161",
                "symbol": readiness.symbol,
                "buy_fee_bps": readiness.buy_fee_bps,
                "sell_fee_bps": readiness.sell_fee_bps,
                "validation_price": readiness.validation_price.to_string(),
                "validation_quantity": readiness.validation_quantity.to_string(),
                "request_fingerprints": readiness.request_fingerprints,
                "request_count": 4,
                "filters_ready": readiness.filters_ready,
                "external_mutation_authorized": readiness.external_mutation_authorized,
                "ready": true,
            }),
        ),
        Err(error) => {
            tracing::warn!(
                pair_id = m8_pair.id,
                error = %error,
                "M9 Binance readiness validation is incomplete; ESP fails closed"
            );
            telemetry.emit(
                "m9_live_readiness",
                serde_json::json!({
                    "engine_id": config.engine_id,
                    "stage": "binance_order_matrix",
                    "pair_id": m8_pair.id,
                    "network_id": "eip155:42161",
                    "symbol": m8_pair.binance.symbol,
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
    shared_binance_runtime.ensure_order_enabled(&m8_pair.binance.symbol)?;
    let esp_symbol_state = shared_binance_account
        .symbol(&m8_pair.binance.symbol)
        .context("shared Binance account omitted ESPUSDC")?;
    let esp_buy_fee_bps = esp_symbol_state
        .commission
        .conservative_taker_fee_bps("BUY")?;
    let esp_sell_fee_bps = esp_symbol_state
        .commission
        .conservative_taker_fee_bps("SELL")?;
    ensure!(
        esp_symbol_state.symbol_rules.base_asset == m8_pair.binance.base_asset
            && esp_symbol_state.symbol_rules.quote_asset == m8_pair.binance.quote_asset,
        "ESPUSDC exchangeInfo assets differ from the M9 domain artifact"
    );
    let esp_execution_symbol_rules = esp_symbol_state
        .symbol_rules
        .with_compatible_price_step(
            Decimal::from_str(&m8_pair.binance.tick_size)
                .context("M9 ESP Binance tick_size is invalid")?,
        )
        .context("M9 ESP tick_size is incompatible with live PRICE_FILTER")?;
    ensure!(
        esp_symbol_state.symbol_rules.lot_size.step
            == Decimal::from_str(&m8_pair.binance.step_size)
                .context("M9 ESP Binance step_size is invalid")?,
        "M9 ESP step_size differs from live LOT_SIZE"
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
        match validate_rebalance_readiness(&m8_pair, &capital_coins) {
            Ok(readiness) => telemetry.emit(
                "m9_live_readiness",
                serde_json::json!({
                    "engine_id": config.engine_id,
                    "stage": "arbitrum_rebalance_routes",
                    "pair_id": m8_pair.id,
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
                    "M9 Arbitrum rebalance readiness is incomplete; rebalance remains disabled"
                );
                telemetry.emit(
                    "m9_live_readiness",
                    serde_json::json!({
                        "engine_id": config.engine_id,
                        "stage": "arbitrum_rebalance_routes",
                        "pair_id": m8_pair.id,
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
        .context("run requires the compiled M5 portfolio runtime plan")?;
    let portfolio_catalog = Arc::new(PortfolioCatalog::from_compiled(&portfolio_runtime)?);
    ensure!(
        portfolio_catalog.live_rebalance_adapter() == "world_chain_v12_parity",
        "live WLD rebalance is not behind the reviewed v12 parity adapter"
    );
    let canary_rebalance_tracker =
        if portfolio_catalog.allocator_mode() == CompiledCapitalAllocatorMode::LiveCanary {
            ensure!(
                m8_pair.rebalance.enabled,
                "live M10 allocator requires the ESP pair rebalance policy"
            );
            let mut routes = BTreeMap::new();
            for token in [&m8_pair.token_a, &m8_pair.token_b] {
                let capital = select_capital_routes(
                    &capital_coins,
                    &token.symbol,
                    &m8_pair.chain.binance_network_name,
                    "OPTIMISM",
                )?;
                let direct = capital
                    .direct
                    .as_ref()
                    .filter(|route| route.network == m8_pair.chain.binance_network_name)
                    .context("M10 direct Arbitrum capital route is absent")?;
                ensure!(
                    capital.deposit_all_enabled
                        && capital.withdrawal_all_enabled
                        && direct.deposit_available()
                        && direct.withdrawal_available(),
                    "M10 direct Arbitrum capital route is not fully available"
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
            RebalanceTracker::new(&m8_pair, routes)?
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
    let (m8_chain_readiness_probe, initial_m8_chain_readiness_status) =
        if let Some(registry) = network_registry.as_ref() {
            let runtime = registry.get_by_chain_id(42_161)?;
            let snapshot = portfolio_wallet_snapshots
                .iter()
                .find(|snapshot| snapshot.chain_id == 42_161)
                .context("M9 Arbitrum wallet snapshot is missing")?;
            let probe = M8ChainReadinessProbe::new(&m8_pair, runtime, wallet_owner)?;
            match inspect_chain_readiness(&m8_pair, runtime, snapshot).await {
                Ok(readiness) => {
                    emit_m8_chain_readiness(
                        &telemetry,
                        &config.engine_id,
                        &m8_pair,
                        &readiness,
                        "startup",
                    );
                    (Some(probe), Some(readiness.status()))
                }
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        "M9 Arbitrum chain readiness is incomplete; ESP fails closed"
                    );
                    emit_m8_chain_readiness_failure(
                        &telemetry,
                        &config.engine_id,
                        &m8_pair,
                        "startup",
                        &error,
                    );
                    (Some(probe), Some(M8ChainReadinessStatus::ProbeFailed))
                }
            }
        } else {
            (None, None)
        };
    let canary_execution_ready = Arc::new(AtomicBool::new(matches!(
        initial_m8_chain_readiness_status,
        Some(M8ChainReadinessStatus::Observed { ready: true, .. })
    )));
    let canary_market_data_ready = Arc::new(AtomicBool::new(true));
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
    let (mut full_rebalance_executor, rebalance_recovery_operation) = if config
        .rebalance_execution_mode
        == "full_live"
    {
        let wallet = EvmWallet::from_env()?;
        ensure!(
            wallet.address() == wallet_owner,
            "full rebalance signer does not match EVM_WALLET_ADDRESS"
        );
        let optimism_endpoint = std::env::var(OPTIMISM_RPC_URL_ENV).with_context(|| {
            format!("required environment variable {OPTIMISM_RPC_URL_ENV} is not set")
        })?;
        let transaction_journal_path =
            std::env::var(WALLET_JOURNAL_PATH_ENV).with_context(|| {
                format!("required environment variable {WALLET_JOURNAL_PATH_ENV} is not set")
            })?;
        let subaccount_email = std::env::var("BINANCE_SUBACCOUNT_EMAIL")
            .context("full rebalance requires BINANCE_SUBACCOUNT_EMAIL")?;
        let treasury_client = BinanceAccountClient::from_treasury_env(&config)?;
        let rebalance_journal_started_at = Instant::now();
        let m10_capital_policy = portfolio_catalog.capital_canary().cloned();
        let maximum_esp = m10_capital_policy
            .as_ref()
            .filter(|policy| policy.external_mutation_authorized)
            .map(|policy| {
                rebalance_base_units_to_decimal(
                    policy.maximum_token_b_debit,
                    m8_pair.token_b.decimals,
                )
            })
            .transpose()?
            .unwrap_or(Decimal::ZERO);
        let mut executor = RebalanceExecutor::hydrate(
            binance_account_client.clone(),
            treasury_client,
            subaccount_email,
            AcrossClient::new(&config)?,
            wallet_rpc.clone(),
            JsonRpcClient::new(optimism_endpoint)?,
            BTreeMap::new(),
            wallet,
            config.rebalance_executor_journal_path.clone(),
            transaction_journal_path.into(),
            RebalanceRuntimeLimits {
                maximum_wld: config.rebalance_max_wld_amount,
                maximum_usdc: config.rebalance_max_usdc_amount,
                maximum_esp,
                operation_timeout: Duration::from_secs(config.rebalance_executor_timeout_seconds),
            },
        )
        .await?;
        executor.set_capital_canary_policy(m10_capital_policy)?;
        if let Some(recovery) = m8_pair
            .live_canary
            .as_ref()
            .and_then(|canary| canary.prefunding_rebalance.as_ref())
            .and_then(|prefunding| prefunding.approved_absent_standard_withdrawal.as_ref())
            && executor
                .active_operation()?
                .is_some_and(|operation| operation.intent.operation_id == recovery.operation_id)
        {
            let recovery = ApprovedAbsentStandardWithdrawalRecovery {
                operation_id: recovery.operation_id.clone(),
                fingerprint: recovery.fingerprint.clone(),
                withdraw_order_id: recovery.withdraw_order_id.clone(),
                token_symbol: recovery.token_symbol.clone(),
                amount: U256::from_str_radix(&recovery.amount_base_units, 10)
                    .context("approved absent withdrawal amount is invalid")?,
                wallet_owner: Address::from_str(&recovery.wallet_address)
                    .context("approved absent withdrawal wallet is invalid")?,
                binance_network: recovery.binance_network.clone(),
                bridge_chain_id: recovery.bridge_chain_id,
                wallet_chain_id: recovery.wallet_chain_id,
                bridge_balance_before: U256::from_str_radix(
                    &recovery.bridge_balance_before_base_units,
                    10,
                )
                .context("approved absent withdrawal bridge balance is invalid")?,
                master_transfer_transaction_id: recovery.master_transfer_transaction_id,
                reconciliation_queries: recovery.reconciliation_queries,
                rejected_http_status: recovery.rejected_http_status,
                rejected_error_code: recovery.rejected_error_code,
                rejected_error_message: recovery.rejected_error_message.clone(),
            };
            executor
                .close_operator_confirmed_absent_standard_withdrawal(&recovery)
                .await?;
        }
        if let Some(recovery) = m8_pair
            .live_canary
            .as_ref()
            .and_then(|canary| canary.prefunding_rebalance.as_ref())
            .and_then(|prefunding| prefunding.approved_absent_master_transfer.as_ref())
            && executor
                .active_operation()?
                .is_some_and(|operation| operation.intent.operation_id == recovery.operation_id)
        {
            let recovery = ApprovedAbsentMasterTransferRecovery {
                operation_id: recovery.operation_id.clone(),
                fingerprint: recovery.fingerprint.clone(),
                withdraw_order_id: recovery.withdraw_order_id.clone(),
                token_symbol: recovery.token_symbol.clone(),
                amount: U256::from_str_radix(&recovery.amount_base_units, 10)
                    .context("approved absent master-transfer amount is invalid")?,
                wallet_owner: Address::from_str(&recovery.wallet_address)
                    .context("approved absent master-transfer wallet is invalid")?,
                binance_network: recovery.binance_network.clone(),
                bridge_chain_id: recovery.bridge_chain_id,
                wallet_chain_id: recovery.wallet_chain_id,
                binance_balance_before: U256::from_str_radix(
                    &recovery.binance_balance_before_base_units,
                    10,
                )
                .context("approved absent master-transfer Binance balance is invalid")?,
                wallet_balance_before: U256::from_str_radix(
                    &recovery.wallet_balance_before_base_units,
                    10,
                )
                .context("approved absent master-transfer wallet balance is invalid")?,
                first_absent_observed_at: UNIX_EPOCH + Duration::from_secs(1_785_464_033),
                minimum_evidence_age: Duration::from_secs(recovery.minimum_evidence_age_seconds),
            };
            executor
                .close_operator_confirmed_absent_master_transfer(&recovery)
                .await?;
        }
        executor.set_telemetry(telemetry.clone(), config.engine_id.clone());
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
        if let Some(operation) = recovery_operation.as_ref() {
            tracing::warn!(
                operation_id = %operation.intent.operation_id,
                progress = ?operation.progress,
                "recovered active rebalance operation for asynchronous runtime recovery"
            );
        }
        (Some(executor), recovery_operation)
    } else {
        (None, None)
    };
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
    let canary_initialized = shadow_initialized_dex
        .as_ref()
        .context("M9 ESP execution has no initialized Arbitrum DEX runtime")?;
    let canary_wallet_rpc = canary_initialized.rpc.clone();
    let canary_initial_head = canary_initialized.mirror.latest_head();
    let (canary_receipt_heads, canary_receipt_head_receiver) =
        tokio::sync::watch::channel(canary_initial_head);
    let canary_initial_wallet_balances = portfolio_wallet_snapshots
        .iter()
        .find(|snapshot| snapshot.chain_id == 42_161)
        .context("M9 ESP execution has no Arbitrum wallet snapshot")?
        .clone();
    let entry_preflight = EntryPreflightHandle::default();
    let mut shared_arbitrum_rebalance_owner_attached = false;
    let live_trade_runtime = if config.arbitrage_execution_mode == "full_live" {
        ensure!(
            domain_config.snapshot().live_trading_enabled
                && pair.execution_enabled
                && m8_pair.execution_enabled,
            "composed M9 live arbitrage requires both versioned execution gates"
        );
        ensure!(
            canary_execution_ready.load(Ordering::Acquire),
            "M9 Arbitrum chain and prefunding readiness must pass before bounded allowance mutations"
        );
        let account_id = compiled_binance_runtime
            .as_ref()
            .context("M9 live execution requires a compiled Binance account")?
            .account_id
            .as_str()
            .to_owned();
        let strategy_plans = &hot_path_dependencies
            .as_ref()
            .context("M9 live execution requires compiled strategy ownership")?
            .plan()
            .strategies;
        let scope_for = |symbol: &str| -> anyhow::Result<TradeJournalScope> {
            let strategy = strategy_plans
                .iter()
                .find(|strategy| strategy.execute && strategy.symbol == symbol)
                .with_context(|| format!("M9 live execution has no {symbol} strategy"))?;
            let network = network_registry
                .as_ref()
                .context("M9 live execution requires the network registry")?
                .runtimes()
                .find(|runtime| runtime.plan().network_id == strategy.network_id)
                .context("M9 executable strategy has no EVM execution owner")?
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
        let canary_journal_scope = scope_for("ESPUSDC")?;
        ensure!(
            EvmJournalScope {
                schema_version: EvmJournalScope::SCHEMA_VERSION,
                network_id: canary_journal_scope.network_id.clone(),
                wallet_id: canary_journal_scope.wallet_id.clone(),
                strategy_id: canary_journal_scope.strategy_id.clone(),
            } == m9_canary_evm_journal_scope(ARBITRUM_CHAIN_ID),
            "compiled M9 journal identity differs from the prefunding recovery identity"
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
        let canary_wallet_journal_path =
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
                    protocol: UniswapProtocol::V3,
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
            if pair.dex.allowed_providers.contains(&DexProvider::UniswapV4) {
                allowance_requirements.push(AllowanceRequirement {
                    operation_id: format!("rustarb-setup-v4-{}", token.symbol),
                    protocol: UniswapProtocol::V4,
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
        let dex_service = DexExecutionService::spawn(
            dex_executor,
            config.arbitrage_leg_execution_channel_capacity,
        )?;
        let canary_evm_journal_started_at = Instant::now();
        let mut canary_dex_executor = DexExecutor::hydrate_with_gas_policy(
            canary_wallet_rpc.clone(),
            EvmWallet::from_env()?,
            42_161,
            canary_wallet_journal_path.into(),
            CompiledNetworkGasPolicy::ArbitrumOne {
                requires_fresh_rpc_gas_price: true,
                max_priority_fee_per_gas_wei: 0,
                max_fee_headroom_bps: m8_pair
                    .live_canary
                    .as_ref()
                    .context("M9 canary policy is missing")?
                    .arbitrum_max_fee_headroom_bps,
                includes_l1_fee: false,
            },
        )
        .await?;
        canary_dex_executor.set_journal_scope(EvmJournalScope {
            schema_version: EvmJournalScope::SCHEMA_VERSION,
            network_id: canary_journal_scope.network_id.clone(),
            wallet_id: canary_journal_scope.wallet_id.clone(),
            strategy_id: canary_journal_scope.strategy_id.clone(),
        })?;
        canary_dex_executor.set_receipt_heads(canary_receipt_head_receiver.clone());
        let canary_router = m8_pair
            .chain
            .uniswap_v3_router_address
            .as_deref()
            .context("M9 Arbitrum V3 router is missing")?
            .parse()
            .context("M9 Arbitrum V3 router is invalid")?;
        let m9_canary = m8_pair
            .live_canary
            .as_ref()
            .context("M9 canary policy is missing")?;
        let token_a = canary_initial_wallet_balances
            .token_balances
            .iter()
            .find(|token| token.symbol.as_ref() == m8_pair.token_a.symbol)
            .context("M9 startup wallet snapshot is missing token_a")?;
        let token_b = canary_initial_wallet_balances
            .token_balances
            .iter()
            .find(|token| token.symbol.as_ref() == m8_pair.token_b.symbol)
            .context("M9 startup wallet snapshot is missing token_b")?;
        let canary_risk = PaperTradeCoordinator::open(&config.arbitrage_trade_journal_path)?
            .canary_journal_risk(&canary_journal_scope.strategy_id)?;
        let now_unix_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before Unix epoch")?
            .as_micros()
            .try_into()
            .context("current Unix timestamp exceeds u64")?;
        if let Some((token_a_required, token_b_required)) = m9_canary_allowance_requirements(
            m9_canary,
            canary_risk,
            token_a.base_units,
            token_b.base_units,
            now_unix_us,
        )? {
            let canary_allowances = [(token_a, token_a_required), (token_b, token_b_required)]
                .into_iter()
                .map(|(token, required)| AllowanceRequirement {
                    operation_id: m9_allowance_operation_id(token.symbol.as_ref(), required),
                    protocol: UniswapProtocol::V3,
                    token: token.contract,
                    router: canary_router,
                    required,
                })
                .collect::<Vec<_>>();
            canary_dex_executor
                .prepare_and_lock_allowances(&canary_allowances)
                .await?;
        } else {
            canary_dex_executor.lock_allowance_mutations_without_preparation()?;
            tracing::info!(
                pair_id = %m8_pair.id,
                admitted_parent_count = canary_risk.admitted_parent_count,
                failed_parent_count = canary_risk.failed_parent_count,
                active_parent_count = canary_risk.active_parent_count,
                admitted_notional_token_a_base_units =
                    canary_risk.admitted_notional_token_a_base_units,
                realized_loss_token_a_base_units =
                    canary_risk.realized_loss_token_a_base_units,
                "durable M9 stop condition locked allowance mutations without a new approval"
            );
        }
        canary_dex_executor.set_latency_telemetry(execution_latency_telemetry.clone());
        let canary_dex_service = DexExecutionService::spawn(
            canary_dex_executor,
            config.arbitrage_leg_execution_channel_capacity,
        )?;
        if let Some(executor) = full_rebalance_executor.as_mut() {
            executor
                .attach_arbitrum_execution_owner(
                    canary_dex_service.evm_execution_owner(),
                    canary_wallet_rpc.clone(),
                )
                .await?;
            shared_arbitrum_rebalance_owner_attached = true;
        }
        telemetry.emit(
            "runtime_journal_recovery",
            serde_json::json!({
                "engine_id": config.engine_id,
                "owner": "evm_execution",
                "journal_scope": arb_bot::telemetry::execution_lane_id(42_161),
                "network_id": arb_bot::telemetry::network_id(42_161),
                "wallet_id": arb_bot::telemetry::PRIMARY_EVM_WALLET_ID,
                "duration_us": canary_evm_journal_started_at.elapsed().as_micros(),
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
                    canary_journal_scope.symbol.clone(),
                    BinanceOrderJournalScope {
                        schema_version: BinanceOrderJournalScope::SCHEMA_VERSION,
                        account_id: canary_journal_scope.account_id.clone(),
                        strategy_id: canary_journal_scope.strategy_id.clone(),
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
        let (canary_dex_revert_diagnostics, canary_dex_revert_diagnostic_task) =
            dex_revert_diagnostic_channel(
                canary_wallet_rpc.clone(),
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
        let canary_executor = Arc::new(ComposedLiveLegExecutor::new(
            canary_dex_service,
            Arc::clone(&binance_service),
            ComposedLiveLegExecutorConfig {
                rules: esp_execution_symbol_rules.clone(),
                base_asset: m8_pair.binance.base_asset.clone(),
                base_decimals: m8_pair.token_b.decimals,
                quote_asset: m8_pair.binance.quote_asset.clone(),
                quote_decimals: m8_pair.token_a.decimals,
                commission_asset: commission_asset.clone(),
                commission_price_symbol: commission_price_symbol.clone(),
                market_state: entry_preflight.clone(),
                dex_revert_diagnostics: canary_dex_revert_diagnostics,
                telemetry: telemetry.clone(),
                engine_id: config.engine_id.clone(),
            },
        )?);
        let executor = RoutedLiveLegExecutor::new(BTreeMap::from([
            (pair.id.clone(), primary_executor),
            (m8_pair.id.clone(), canary_executor),
        ]))?;
        let m9_canary = m8_pair
            .live_canary
            .as_ref()
            .context("M9 canary policy is missing")?;
        let parse_canary_amount = |value: &str, label: &str| {
            value
                .parse::<u128>()
                .with_context(|| format!("M9 {label} is invalid"))
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
                canary_policies: BTreeMap::from([(
                    m8_pair.id.clone(),
                    LiveCanaryPolicy {
                        journal_scope: canary_journal_scope,
                        maximum_trade_notional_token_a_base_units: parse_canary_amount(
                            &m9_canary.max_trade_notional_token_a_base_units,
                            "maximum trade notional",
                        )?,
                        maximum_total_notional_token_a_base_units: parse_canary_amount(
                            &m9_canary.max_total_notional_token_a_base_units,
                            "maximum total notional",
                        )?,
                        maximum_unhedged_notional_token_a_base_units: parse_canary_amount(
                            &m9_canary.max_unhedged_notional_token_a_base_units,
                            "maximum unhedged notional",
                        )?,
                        maximum_realized_loss_token_a_base_units: parse_canary_amount(
                            &m9_canary.max_realized_loss_token_a_base_units,
                            "maximum realized loss",
                        )?,
                        maximum_parent_trades: usize::from(m9_canary.max_parent_trades),
                        maximum_failed_parent_trades: usize::from(
                            m9_canary.max_failed_parent_trades,
                        ),
                        maximum_concurrent_trades: usize::from(m9_canary.max_concurrent_trades),
                        rollout_duration: Duration::from_secs(m9_canary.rollout_duration_seconds),
                        readiness: Arc::clone(&canary_execution_ready),
                        market_data_readiness: Arc::clone(&canary_market_data_ready),
                    },
                )]),
            },
        )?;
        let diagnostic_task = tokio::spawn(async move {
            tokio::join!(
                dex_revert_diagnostic_task.run(),
                canary_dex_revert_diagnostic_task.run()
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
    let canary_wallet_tokens = vec![
        TokenBalanceRequest {
            symbol: m8_pair.token_a.symbol.clone(),
            contract: m8_pair
                .token_a
                .contract
                .parse()
                .context("M9 token_a address is invalid")?,
        },
        TokenBalanceRequest {
            symbol: m8_pair.token_b.symbol.clone(),
            contract: m8_pair
                .token_b
                .contract
                .parse()
                .context("M9 token_b address is invalid")?,
        },
    ];
    let canary_wallet_reads = network_registry
        .as_ref()
        .context("M9 requires the Arbitrum network runtime")?
        .get_by_chain_id(42_161)?
        .reads()
        .clone();
    let arb_bot::balances::WalletBalanceSync {
        receiver: mut canary_wallet_balance_receiver,
        heads: canary_wallet_heads,
        task: mut canary_wallet_balance_task,
    } = spawn_wallet_balance_sync(
        WalletReadClient::Coordinated(canary_wallet_reads),
        wallet_owner,
        42_161,
        canary_wallet_tokens,
        canary_initial_head,
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
        },
        BinanceFeeBps {
            buy: binance_buy_fee_bps,
            sell: binance_sell_fee_bps,
        },
    )?;
    let dependencies =
        hot_path_dependencies.context("run requires the compiled M4 hot-path runtime plan")?;
    let shadow_plan =
        shadow_strategy_plan.context("compiled M9 hot path has no ESP canary strategy")?;
    let InitializedDex {
        mirror: shadow_mirror,
        stream: shadow_stream,
        rpc: _shadow_wallet_rpc,
        timings: _shadow_dex_timings,
    } = shadow_initialized_dex
        .context("compiled M9 ESP strategy has no initialized DEX runtime")?;
    let shadow_pair = shadow_plan
        .domain_config
        .snapshot()
        .pairs
        .first()
        .context("compiled M9 ESP strategy has no projected pair")?;
    let (mut canary_engine, canary_hot_telemetry) = TradingEngine::new(
        config.clone(),
        Arc::new(shadow_plan.domain_config.clone()),
        shadow_mirror,
        telemetry.clone(),
        V12RebalanceParityAdapter::new(canary_rebalance_tracker),
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
        },
        BinanceFeeBps {
            buy: esp_buy_fee_bps,
            sell: esp_sell_fee_bps,
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
    let mut engine = HotPathDecisionOwner::new(primary_engine, Vec::new(), dependencies)?;
    tracing::info!(
        binance_account_id = PRIMARY_BINANCE_ACCOUNT_ID,
        live_strategy_id = %engine.strategy_id().as_str(),
        canary_strategy_id = %shadow_plan.strategy_id.as_str(),
        canary_network_id = %shadow_plan.network_id.as_str(),
        canary_execution_lane_id = %execution_lane_id(shadow_pair.chain.chain_id),
        shared_inventory_owner = true,
        shared_binance_order_owner = true,
        canary_rebalance_mutation_enabled =
            portfolio_catalog.allocator_mode() == CompiledCapitalAllocatorMode::LiveCanary,
        canary_external_mutation_authorized = true,
        root_supervisor_policy = "dependency_scoped_v1",
        "M9 bounded ESP production canary configured"
    );
    let m8_canary = m8_pair
        .live_canary
        .as_ref()
        .context("M9 live pair has no bounded canary policy")?;
    tracing::info!(
        pair_id = m8_pair.id,
        strategy_id = %shadow_plan.strategy_id.as_str(),
        network_id = %shadow_plan.network_id.as_str(),
        chain_id = m8_pair.chain.chain_id,
        router = %m8_pair
            .chain
            .uniswap_v3_router_address
            .as_deref()
            .context("M9 canary router is missing")?,
        approval_gate = "explicit_production_approved",
        max_trade_notional_token_a_base_units =
            %m8_canary.max_trade_notional_token_a_base_units,
        max_total_notional_token_a_base_units =
            %m8_canary.max_total_notional_token_a_base_units,
        minimum_wallet_token_a_base_units =
            %m8_canary.minimum_wallet_token_a_base_units,
        minimum_wallet_token_b_base_units =
            %m8_canary.minimum_wallet_token_b_base_units,
        minimum_runtime_wallet_token_a_base_units =
            %m8_canary.runtime_wallet_token_a_minimum(),
        minimum_runtime_wallet_token_b_base_units =
            %m8_canary.runtime_wallet_token_b_minimum(),
        max_parent_trades = m8_canary.max_parent_trades,
        max_failed_parent_trades = m8_canary.max_failed_parent_trades,
        max_concurrent_trades = m8_canary.max_concurrent_trades,
        rollout_duration_seconds = m8_canary.rollout_duration_seconds,
        gas_policy = "fresh_eth_gas_price_fail_closed_no_world_fallback",
        allowance_policy = "bounded_exact_canary_cap_then_locked",
        receipt_accounting = "effective_gas_price_includes_arbitrum_l1_component",
        execution_enabled = true,
        rebalance_enabled = m8_pair.rebalance.enabled,
        external_mutation_authorized = true,
        "M9 Arbitrum live canary configured"
    );
    let m10_policy = m8_canary
        .rebalance_live_canary
        .as_ref()
        .context("M10 rebalance policy is missing")?;
    if portfolio_catalog.allocator_mode() == CompiledCapitalAllocatorMode::LiveCanary {
        ensure!(
            shared_arbitrum_rebalance_owner_attached && full_rebalance_executor.is_some(),
            "live M10 rebalance has no shared Arbitrum EVM execution owner"
        );
    }
    tracing::info!(
        pair_id = m8_pair.id,
        strategy_id = "rebalance-arbitrum-usdc-esp-m10",
        network_id = "eip155:42161",
        approval_gate = ?m10_policy.approval_gate,
        production_approval_actor = ?m10_policy.production_approval_actor,
        production_approval_recorded_at_utc =
            ?m10_policy.production_approval_recorded_at_utc,
        approval_session_id = m10_policy.approval_session_id,
        allocator_mode = ?portfolio_catalog.allocator_mode(),
        binance_network = m10_policy.binance_network,
        maximum_transfer_count = m10_policy.maximum_transfer_count,
        maximum_concurrent_transfers = m10_policy.maximum_concurrent_transfers,
        maximum_failed_transfers = m10_policy.maximum_failed_transfers,
        maximum_token_a_debit_base_units =
            m10_policy.maximum_token_a_debit_base_units,
        maximum_token_b_debit_base_units =
            m10_policy.maximum_token_b_debit_base_units,
        maximum_token_a_fee_base_units = m10_policy.maximum_token_a_fee_base_units,
        maximum_token_b_fee_base_units = m10_policy.maximum_token_b_fee_base_units,
        rollout_duration_seconds = m10_policy.rollout_duration_seconds,
        maximum_unknown_reconciliation_queries =
            m10_policy.maximum_unknown_reconciliation_queries,
        direct_route_only = m10_policy.direct_route_only,
        bridge_mutations_enabled = m10_policy.bridge_mutations_enabled,
        shared_arbitrum_evm_owner = shared_arbitrum_rebalance_owner_attached,
        external_mutation_authorized = portfolio_catalog
            .capital_canary()
            .is_some_and(|policy| policy.external_mutation_authorized),
        "M10 Arbitrum rebalance live canary configured"
    );
    let AlchemyDexStream {
        receiver: mut shadow_dex_receiver,
        task: mut shadow_dex_task,
    } = shadow_stream;
    engine.on_binance_clock_sync(binance_account.clock_sync);
    canary_engine.on_binance_clock_sync(binance_account.clock_sync);
    let hot_telemetry_task = tokio::spawn(hot_telemetry.run());
    let portfolio_allocator_task = tokio::spawn(portfolio_allocator_task.run());
    let canary_hot_telemetry_task = tokio::spawn(canary_hot_telemetry.run());
    let m8_chain_readiness_task = m8_chain_readiness_probe.map(|probe| {
        tokio::spawn(run_m8_chain_readiness_refresh(
            probe,
            telemetry.clone(),
            config.engine_id.clone(),
            m8_pair.clone(),
            initial_m8_chain_readiness_status,
            Arc::clone(&canary_execution_ready),
        ))
    });
    let (binance_clock_sync_sender, mut binance_clock_sync_receiver) =
        tokio::sync::mpsc::channel(4);
    let binance_clock_sync_task = tokio::spawn(run_binance_clock_sync(
        binance_clock_sync_client,
        binance_clock_sync_sender,
    ));
    let mut binance_clock_sync_running = true;
    let (rebalance_sender, mut rebalance_receiver, mut rebalance_task, m10_risk_receiver) =
        if let Some(mut executor) = full_rebalance_executor.take() {
            let recover_on_start = rebalance_recovery_operation.is_some();
            let recovery_target = rebalance_recovery_operation
                .as_ref()
                .map(rebalance_target)
                .unwrap_or(RebalanceExecutionTarget::Primary);
            let (request_sender, mut request_receiver) = tokio::sync::mpsc::channel(1);
            let (result_sender, result_receiver) = tokio::sync::mpsc::channel(1);
            let (risk_sender, risk_receiver) =
                tokio::sync::watch::channel(executor.m10_canary_risk()?);
            let rebalance_telemetry = telemetry.clone();
            let rebalance_engine_id = config.engine_id.clone();
            let task = tokio::spawn(async move {
                emit_m10_rebalance_risk(&rebalance_telemetry, &rebalance_engine_id, &executor);
                if recover_on_start {
                    let saga_started_at = Instant::now();
                    let result = recover_rebalance_with_quote_retries(&mut executor).await;
                    emit_m10_rebalance_saga(
                        &rebalance_telemetry,
                        &rebalance_engine_id,
                        recovery_target,
                        &result,
                        &executor,
                        saga_started_at,
                        true,
                    );
                    emit_m10_rebalance_risk(&rebalance_telemetry, &rebalance_engine_id, &executor);
                    risk_sender.send_replace(executor.m10_canary_risk()?);
                    let active_operation_after = executor.active_operation()?.is_some();
                    if result_sender
                        .send(RebalanceExecutorEvent::Recovery {
                            target: recovery_target,
                            result,
                            active_operation_after,
                        })
                        .await
                        .is_err()
                    {
                        return Ok::<(), anyhow::Error>(());
                    }
                }
                while let Some((target, request)) = request_receiver.recv().await {
                    let saga_started_at = Instant::now();
                    let result = execute_rebalance_with_quote_retries(&mut executor, request).await;
                    emit_m10_rebalance_saga(
                        &rebalance_telemetry,
                        &rebalance_engine_id,
                        target,
                        &result,
                        &executor,
                        saga_started_at,
                        false,
                    );
                    emit_m10_rebalance_risk(&rebalance_telemetry, &rebalance_engine_id, &executor);
                    risk_sender.send_replace(executor.m10_canary_risk()?);
                    let active_operation_after = executor.active_operation()?.is_some();
                    if result_sender
                        .send(RebalanceExecutorEvent::Execution {
                            target,
                            result,
                            active_operation_after,
                        })
                        .await
                        .is_err()
                    {
                        return Ok::<(), anyhow::Error>(());
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
            let (_request_sender, _request_receiver) = tokio::sync::mpsc::channel::<(
                RebalanceExecutionTarget,
                RebalanceExecutionRequest,
            )>(1);
            let (_result_sender, result_receiver) =
                tokio::sync::mpsc::channel::<RebalanceExecutorEvent>(1);
            let (_risk_sender, risk_receiver) =
                tokio::sync::watch::channel(RebalanceCanaryRisk::default());
            (None, result_receiver, None, risk_receiver)
        };
    if let Some(operation) = rebalance_recovery_operation.as_ref() {
        match rebalance_target(operation) {
            RebalanceExecutionTarget::Primary => engine.on_rebalance_recovery_started(operation)?,
            RebalanceExecutionTarget::ArbitrumCanary => {
                canary_engine.on_rebalance_recovery_started(operation)?
            }
        }
    }
    engine.on_balance_event(BalanceEvent::Binance(initial_binance_balances.clone()))?;
    canary_engine
        .on_shared_binance_balance_event(BalanceEvent::Binance(initial_binance_balances))?;
    engine.on_balance_event(BalanceEvent::Wallet(initial_wallet_balances))?;
    canary_engine.on_balance_event(BalanceEvent::Wallet(canary_initial_wallet_balances.clone()))?;
    for snapshot in &portfolio_wallet_snapshots {
        if snapshot.chain_id != wallet_chain_id {
            engine.on_portfolio_wallet_snapshot(snapshot)?;
        }
    }
    engine.on_user_data_connected(user_data_subscription_id);
    canary_engine.on_shared_user_data_connected();
    // The executor and its durable journal are a single process-wide mutation
    // lane. Recovery owns that lane until it publishes a terminal result.
    let mut rebalance_lane_busy = rebalance_recovery_operation.is_some();
    if !rebalance_lane_busy {
        rebalance_lane_busy = dispatch_rebalance_execution(
            &mut engine,
            rebalance_sender.as_ref(),
            pair,
            wallet_owner,
            RebalanceExecutionTarget::Primary,
            None,
            None,
        )
        .await?;
    }
    if !rebalance_lane_busy && engine.pending_rebalance_execution().is_none() {
        rebalance_lane_busy = dispatch_rebalance_execution(
            &mut canary_engine,
            rebalance_sender.as_ref(),
            &m8_pair,
            wallet_owner,
            RebalanceExecutionTarget::ArbitrumCanary,
            portfolio_catalog.capital_canary(),
            Some(&m10_risk_receiver),
        )
        .await?;
    }
    engine.start();
    canary_engine.start();
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
        &mut canary_engine,
        &mut pending_prepared_pool_builds,
        &mut dex_receiver,
        &mut shadow_dex_receiver,
        &wallet_heads,
        &receipt_heads,
        &canary_wallet_heads,
        &canary_receipt_heads,
    )?;
    report_strategy_dependency_faults(&mut engine, &root_supervisor)?;
    if startup_primary_dex.pool_build_count > 0 {
        engine.evaluate_after_dex_refreshes()?;
    }
    if startup_shadow_dex.pool_build_count > 0 {
        canary_engine.evaluate_after_dex_refreshes()?;
    }
    telemetry.emit(
        "startup_dex_backlog_drain",
        serde_json::json!({
            "engine_id": config.engine_id,
            "primary_event_count": startup_primary_dex.event_count,
            "primary_pool_build_count": startup_primary_dex.pool_build_count,
            "primary_max_queue_age_us": startup_primary_dex.max_queue_age_us,
            "canary_event_count": startup_shadow_dex.event_count,
            "canary_max_queue_age_us": startup_shadow_dex.max_queue_age_us,
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
        hot_path_canary_strategy_id = %shadow_plan.strategy_id.as_str(),
        hot_path_canary_external_mutation_authorized = true,
        hot_path_canary_rebalance_mutation_authorized =
            portfolio_catalog.allocator_mode() == CompiledCapitalAllocatorMode::LiveCanary,
        portfolio_inventory_key = "inventory_location+venue_asset_id",
        portfolio_location_count = portfolio_catalog.location_count(),
        portfolio_venue_asset_count = portfolio_catalog.asset_count(),
        portfolio_economic_asset_count = portfolio_catalog.economic_asset_count(),
        portfolio_allocator_mode = ?portfolio_catalog.allocator_mode(),
        portfolio_external_mutation_authorized = portfolio_catalog
            .capital_canary()
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

    // These futures must survive unrelated select branches. Recreating
    // `next_event()` on every loop iteration cancels a multi-await depth
    // bootstrap or reconnect before it can commit the connected socket.
    let mut binance_market_event = Box::pin(binance_feed.next_event());
    let mut gas_market_event = Box::pin(gas_price_feed.next_event());
    let mut commission_market_event = Box::pin(commission_price_feed.next_event());
    let mut shadow_dex_running = true;

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
                    canary_market_data_ready.store(false, Ordering::Release);
                    tracing::error!(
                        strategy_id = %shadow_plan.strategy_id.as_str(),
                        "Arbitrum canary DEX stream stopped; new ESP entries are disabled"
                    );
                    shadow_dex_running = false;
                    continue;
                };
                let head = match &event {
                    DexStreamEvent::Head { head, .. } => Some(*head),
                    DexStreamEvent::Log { .. } => None,
                };
                if let Some(request) = canary_engine.on_dex_event(event)? {
                    build_prepared_pool_inline(&mut canary_engine, request)?;
                    canary_engine.evaluate_after_dex_refreshes()?;
                }
                if let Some(head) = head {
                    canary_wallet_heads.send_replace(head);
                    canary_receipt_heads.send_replace(head);
                }
                record_longest_handler(
                    &mut longest_non_price_handler_us,
                    &mut longest_non_price_handler,
                    "shadow_dex",
                    handler_started_at.elapsed(),
                );
            }
            scheduled_at = health_tick.tick() => {
                let loop_lag_us = scheduled_at.elapsed().as_micros();
                engine.refresh_health();
                canary_engine.refresh_health();
                engine.record_owner_loop_health(
                    loop_lag_us,
                    longest_non_price_handler,
                    longest_non_price_handler_us,
                );
                longest_non_price_handler_us = 0;
                longest_non_price_handler = "none";
            },
            event = &mut binance_market_event => {
                drop(binance_market_event);
                let event_symbol = market_event_symbol(&event);
                if event_symbol == shadow_plan.symbol {
                    canary_engine.on_market_event(event, None)?;
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
                canary_engine.on_gas_market_event(event)?;
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
                canary_engine.on_commission_market_event(event)?;
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
                        canary_engine.on_user_data_event(event)?;
                    }
                    UserDataEvent::ExecutionReport(report)
                        if report.symbol == pair.binance.symbol =>
                    {
                        engine.on_user_data_event(event)?;
                    }
                    UserDataEvent::AccountPosition(_) | UserDataEvent::BalanceUpdate(_) => {
                        engine.on_user_data_event(event)?;
                    }
                    UserDataEvent::ExecutionReport(_) => {
                        engine.on_user_data_event(event.clone())?;
                        canary_engine.on_shared_user_data_dirty();
                    }
                    UserDataEvent::StreamTerminated { .. } => {
                        engine.on_user_data_event(event)?;
                        canary_engine.on_shared_user_data_disconnected();
                    }
                    UserDataEvent::Other { .. } => {
                        engine.on_user_data_event(event)?;
                        canary_engine.on_shared_user_data_dirty();
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
                        canary_engine.on_binance_clock_sync(clock_sync);
                    }
                    Some(Err(error)) => {
                        engine.on_binance_clock_sync_failure(&error);
                        canary_engine.on_binance_clock_sync_failure(&error);
                    }
                    None => {
                        binance_clock_sync_running = false;
                        engine.on_binance_clock_sync_failure(
                            "background Binance clock synchronization task stopped",
                        );
                        canary_engine.on_binance_clock_sync_failure(
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
                        canary_engine.on_shared_binance_balance_event(
                            BalanceEvent::Binance(snapshot),
                        )?;
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
                        canary_engine.on_shared_binance_balance_event(BalanceEvent::Failed {
                            source: BalanceSource::Binance,
                            error,
                            observed_at,
                        })?;
                    }
                    other => engine.on_balance_event(other)?,
                }
                if !rebalance_lane_busy {
                    rebalance_lane_busy = dispatch_rebalance_execution(
                        &mut engine,
                        rebalance_sender.as_ref(),
                        pair,
                        wallet_owner,
                        RebalanceExecutionTarget::Primary,
                        None,
                        None,
                    )
                    .await?;
                }
                if !rebalance_lane_busy && engine.pending_rebalance_execution().is_none() {
                    rebalance_lane_busy = dispatch_rebalance_execution(
                        &mut canary_engine,
                        rebalance_sender.as_ref(),
                        &m8_pair,
                        wallet_owner,
                        RebalanceExecutionTarget::ArbitrumCanary,
                        portfolio_catalog.capital_canary(),
                        Some(&m10_risk_receiver),
                    )
                    .await?;
                }
                record_longest_handler(
                    &mut longest_non_price_handler_us,
                    &mut longest_non_price_handler,
                    "balance_publication",
                    handler_started_at.elapsed(),
                );
            }
            event = canary_wallet_balance_receiver.recv() => {
                let handler_started_at = Instant::now();
                let Some(event) = event else {
                    bail!("Arbitrum wallet balance synchronization channel stopped unexpectedly");
                };
                canary_engine.on_balance_event(event)?;
                if !rebalance_lane_busy && engine.pending_rebalance_execution().is_none() {
                    rebalance_lane_busy = dispatch_rebalance_execution(
                        &mut canary_engine,
                        rebalance_sender.as_ref(),
                        &m8_pair,
                        wallet_owner,
                        RebalanceExecutionTarget::ArbitrumCanary,
                        portfolio_catalog.capital_canary(),
                        Some(&m10_risk_receiver),
                    )
                    .await?;
                }
                record_longest_handler(
                    &mut longest_non_price_handler_us,
                    &mut longest_non_price_handler,
                    "arbitrum_balance_publication",
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
                };
                match result {
                    RebalanceExecutorEvent::Recovery {
                        target,
                        result,
                        ..
                    } => match (target, result) {
                        (RebalanceExecutionTarget::Primary, Ok(operation)) => {
                            engine.on_rebalance_recovery_result(Ok(&operation))?
                        }
                        (RebalanceExecutionTarget::Primary, Err(error)) => {
                            engine.on_rebalance_recovery_result(Err(&error))?
                        }
                        (RebalanceExecutionTarget::ArbitrumCanary, Ok(operation)) => {
                            canary_engine.on_rebalance_recovery_result(Ok(&operation))?
                        }
                        (RebalanceExecutionTarget::ArbitrumCanary, Err(error)) => {
                            canary_engine.on_rebalance_recovery_result(Err(&error))?
                        }
                    },
                    RebalanceExecutorEvent::Execution {
                        target,
                        result,
                        ..
                    } => match (target, result) {
                        (RebalanceExecutionTarget::Primary, Ok(operation)) => {
                            engine.on_rebalance_execution_result(Ok(&operation))?
                        }
                        (RebalanceExecutionTarget::Primary, Err(error)) => {
                            engine.on_rebalance_execution_result(Err(&error))?
                        }
                        (RebalanceExecutionTarget::ArbitrumCanary, Ok(operation)) => {
                            canary_engine.on_rebalance_execution_result(Ok(&operation))?
                        }
                        (RebalanceExecutionTarget::ArbitrumCanary, Err(error)) => {
                            canary_engine.on_rebalance_execution_result(Err(&error))?
                        }
                    },
                }
                rebalance_lane_busy = active_operation_after;
                if !rebalance_lane_busy {
                    rebalance_lane_busy = dispatch_rebalance_execution(
                        &mut engine,
                        rebalance_sender.as_ref(),
                        pair,
                        wallet_owner,
                        RebalanceExecutionTarget::Primary,
                        None,
                        None,
                    )
                    .await?;
                }
                if !rebalance_lane_busy && engine.pending_rebalance_execution().is_none() {
                    rebalance_lane_busy = dispatch_rebalance_execution(
                        &mut canary_engine,
                        rebalance_sender.as_ref(),
                        &m8_pair,
                        wallet_owner,
                        RebalanceExecutionTarget::ArbitrumCanary,
                        portfolio_catalog.capital_canary(),
                        Some(&m10_risk_receiver),
                    )
                    .await?;
                }
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
                if event.pair_id == m8_pair.id {
                    let mut prepared_dex = false;
                    while let Ok(dex_event) = shadow_dex_receiver.try_recv() {
                        if let Some(request) = canary_engine.on_dex_event(dex_event)? {
                            build_prepared_pool_inline(&mut canary_engine, request)?;
                            prepared_dex = true;
                        }
                    }
                    let receipt_refresh =
                        canary_engine.apply_arbitrage_receipt_settlement(&event)?;
                    let receipt_applied = receipt_refresh.is_some();
                    if let Some(refresh) = receipt_refresh {
                        build_prepared_pool_inline(&mut canary_engine, refresh)?;
                    }
                    canary_engine.on_paper_trade_event(event)?;
                    if prepared_dex || receipt_applied {
                        canary_engine.evaluate_after_dex_refreshes()?;
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
                canary_market_data_ready.store(false, Ordering::Release);
                tracing::error!(
                    strategy_id = %shadow_plan.strategy_id.as_str(),
                    result = ?result,
                    "Arbitrum canary DEX connector stopped; new ESP entries are disabled"
                );
                shadow_dex_running = false;
            }
            result = &mut binance_balance_task => {
                result.context("Binance balance synchronization task failed")??;
                bail!("Binance balance synchronization stopped unexpectedly");
            }
            result = &mut wallet_balance_task => {
                result.context("wallet balance synchronization task failed")??;
                bail!("wallet balance synchronization stopped unexpectedly");
            }
            result = &mut canary_wallet_balance_task => {
                result.context("Arbitrum wallet balance synchronization task failed")??;
                bail!("Arbitrum wallet balance synchronization stopped unexpectedly");
            }
        }
        if !first_ready_emitted && engine.phase() == RuntimePhase::Ready {
            engine.record_runtime_first_ready(bootstrap.process_started_at.elapsed());
            first_ready_emitted = true;
        }
        for job in engine.take_adaptive_sizing_jobs() {
            let strategy_id = job.strategy_id()?;
            let submission = adaptive_sizing_slots.submit(&strategy_id, job)?;
            if submission.replaced || submission.queued_behind_running {
                engine.record_adaptive_sizing_overload(
                    &strategy_id,
                    submission.replaced,
                    adaptive_sizing_slots.total_retained_work(),
                );
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
    canary_wallet_balance_task.abort();
    binance_clock_sync_task.abort();
    let _ = binance_balance_task.await;
    let _ = wallet_balance_task.await;
    let _ = canary_wallet_balance_task.await;
    let _ = binance_clock_sync_task.await;
    dex_task.abort();
    let _ = dex_task.await;
    shadow_dex_task.abort();
    let _ = shadow_dex_task.await;
    if let Some(task) = m8_chain_readiness_task {
        task.abort();
        let _ = task.await;
    }
    adaptive_sizing_tasks.abort_all();
    while adaptive_sizing_tasks.join_next().await.is_some() {}
    canary_engine.shutdown();
    drop(engine);
    drop(canary_engine);
    if let Some(task) = paper_trade_task.take() {
        task.await??;
    }
    if let Some(task) = dex_revert_diagnostic_task.take() {
        task.await??;
    }
    hot_telemetry_task.await??;
    canary_hot_telemetry_task.await??;
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

fn emit_m8_chain_readiness(
    telemetry: &TelemetryHandle,
    engine_id: &str,
    pair: &arb_bot::domain::config::PairConfig,
    readiness: &M8ChainReadiness,
    readiness_source: &'static str,
) {
    telemetry.emit(
        "m9_live_readiness",
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

fn emit_m8_chain_readiness_failure(
    telemetry: &TelemetryHandle,
    engine_id: &str,
    pair: &arb_bot::domain::config::PairConfig,
    readiness_source: &'static str,
    error: &anyhow::Error,
) {
    telemetry.emit(
        "m9_live_readiness",
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

async fn run_m8_chain_readiness_refresh(
    probe: M8ChainReadinessProbe,
    telemetry: TelemetryHandle,
    engine_id: String,
    pair: arb_bot::domain::config::PairConfig,
    mut last_status: Option<M8ChainReadinessStatus>,
    execution_ready: Arc<AtomicBool>,
) {
    let start = tokio::time::Instant::now() + M8_CHAIN_READINESS_REFRESH_INTERVAL;
    let mut interval = tokio::time::interval_at(start, M8_CHAIN_READINESS_REFRESH_INTERVAL);
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
                emit_m8_chain_readiness(
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
                        "M9 Arbitrum chain readiness became ready"
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
                        "M9 Arbitrum chain readiness degraded; ESP fails closed"
                    );
                }
                last_status = Some(status);
            }
            Err(error) => {
                execution_ready.store(false, Ordering::Release);
                if last_status == Some(M8ChainReadinessStatus::ProbeFailed) {
                    continue;
                }
                tracing::warn!(
                    pair_id = pair.id,
                    error = %error,
                    external_mutation_authorized = false,
                    "M9 Arbitrum chain-readiness probe failed; ESP fails closed"
                );
                emit_m8_chain_readiness_failure(
                    &telemetry,
                    &engine_id,
                    &pair,
                    "background_transition",
                    &error,
                );
                last_status = Some(M8ChainReadinessStatus::ProbeFailed);
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

async fn dispatch_rebalance_execution(
    engine: &mut TradingEngine,
    sender: Option<
        &tokio::sync::mpsc::Sender<(RebalanceExecutionTarget, RebalanceExecutionRequest)>,
    >,
    pair: &arb_bot::domain::config::PairConfig,
    wallet_owner: Address,
    target: RebalanceExecutionTarget,
    capital_policy: Option<&CompiledCapitalCanaryPolicy>,
    m10_risk: Option<&tokio::sync::watch::Receiver<RebalanceCanaryRisk>>,
) -> anyhow::Result<bool> {
    let m10_remaining = if target == RebalanceExecutionTarget::ArbitrumCanary {
        let Some(evaluation) = engine.pending_rebalance_execution() else {
            return Ok(false);
        };
        let action = evaluation
            .plan
            .action
            .as_ref()
            .context("M10 pending rebalance evaluation has no action")?;
        let policy = capital_policy.context("M10 dispatch has no compiled capital policy")?;
        let risk = m10_risk
            .context("M10 dispatch has no durable risk publication")?
            .borrow()
            .clone();
        let now_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before Unix epoch")?
            .as_millis()
            .try_into()
            .context("current Unix timestamp exceeds u64")?;
        let Some(remaining) = remaining_m10_rebalance_authority(
            policy,
            &risk,
            &evaluation.token_symbol,
            action.direction,
            now_unix_ms,
        )?
        else {
            engine.stop_pending_rebalance_creation(
                "M10 durable count, failure, concurrency, duration, value, or fee limit reached",
            );
            return Ok(false);
        };
        engine.cap_pending_rebalance_amount(remaining.maximum_source_debit)?;
        Some(remaining)
    } else {
        None
    };
    let Some(pending) = engine.pending_rebalance_execution().cloned() else {
        return Ok(false);
    };
    let action = pending
        .plan
        .action
        .clone()
        .context("rebalance execution evaluation has no action")?;
    let canary_maximum_fee = if target == RebalanceExecutionTarget::ArbitrumCanary {
        let policy = capital_policy.context("M10 dispatch has no compiled capital policy")?;
        ensure!(
            policy.external_mutation_authorized,
            "M10 dispatch has no external mutation authority"
        );
        let remaining_fee = m10_remaining
            .context("M10 dispatch lost its durable remaining authority")?
            .maximum_fee;
        let authorized_fee = if action.direction == arb_bot::rebalance::Direction::WalletToBinance {
            U256::ZERO
        } else {
            let maximum_with_positive_credit = action
                .amount
                .checked_sub(U256::ONE)
                .context("M10 Binance withdrawal cannot preserve positive destination credit")?;
            let bounded = remaining_fee.min(maximum_with_positive_credit);
            ensure!(
                !bounded.is_zero(),
                "M10 Binance withdrawal has no remaining positive fee authority"
            );
            bounded
        };
        let proposal = engine
            .authorize_pending_rebalance_allocation(authorized_fee)
            .await?
            .context("M10 capital allocator returned no proposal")?;
        ensure!(
            proposal.external_mutation_authorized
                && proposal.source_debit == action.amount
                && proposal.fee == authorized_fee,
            "M10 capital allocator proposal differs from the pending rebalance"
        );
        Some(authorized_fee)
    } else {
        None
    };
    // Capital planning is asynchronous and must not reserve the sole
    // execution queue slot while it waits. In particular, an M10 allocator
    // observation cannot head-of-line block an already eligible WLD rebalance.
    let sender = sender.context("rebalance engine produced live work without an executor")?;
    let permit = match sender.try_reserve() {
        Ok(permit) => permit,
        Err(tokio::sync::mpsc::error::TrySendError::Full(())) => return Ok(false),
        Err(tokio::sync::mpsc::error::TrySendError::Closed(())) => {
            bail!("rebalance executor queue is closed")
        }
    };
    let Some(evaluation) = engine.take_rebalance_execution()? else {
        return Ok(false);
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
    permit.send((
        target,
        RebalanceExecutionRequest {
            authority: match target {
                RebalanceExecutionTarget::Primary => RebalanceExecutionAuthority::WorldChainV12,
                RebalanceExecutionTarget::ArbitrumCanary => {
                    RebalanceExecutionAuthority::ArbitrumM10Canary
                }
            },
            token_symbol: evaluation.token_symbol,
            token_decimals: evaluation.token_decimals,
            token_contract,
            wallet_owner,
            action,
            binance_balance_before: evaluation.plan.projected.binance,
            wallet_balance_before: evaluation.plan.projected.wallet,
            canary_maximum_fee,
            canary_approval_session_id: if target == RebalanceExecutionTarget::ArbitrumCanary {
                Some(
                    capital_policy
                        .context("M10 dispatch has no compiled capital policy")?
                        .approval_session_id
                        .clone(),
                )
            } else {
                None
            },
        },
    ));
    Ok(true)
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
    canary_engine: &mut TradingEngine,
    pending: &mut PreparedPoolBuildBatch,
    dex_receiver: &mut tokio::sync::mpsc::Receiver<DexStreamEvent>,
    shadow_dex_receiver: &mut tokio::sync::mpsc::Receiver<DexStreamEvent>,
    wallet_heads: &tokio::sync::watch::Sender<CanonicalBlock>,
    receipt_heads: &tokio::sync::watch::Sender<CanonicalBlock>,
    canary_wallet_heads: &tokio::sync::watch::Sender<CanonicalBlock>,
    canary_receipt_heads: &tokio::sync::watch::Sender<CanonicalBlock>,
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
        let shadow = drain_startup_canary_dex_backlog(
            canary_engine,
            shadow_dex_receiver,
            canary_wallet_heads,
            canary_receipt_heads,
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

fn drain_startup_canary_dex_backlog(
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
    let backfill_provider_us = backfill_provider_started_at.elapsed().as_micros();

    let backfill_apply_started_at = Instant::now();
    let mut mirror = DexMirror::new(hydrated)?;
    let mut applied = 0_usize;
    for log in &backfill {
        if matches!(mirror.apply_log(log)?, LogApplyResult::Applied { .. }) {
            applied += 1;
        }
    }
    mirror.finish_backfill(backfill_head)?;
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

    use alloy_primitives::{Address, B256, U256};
    use arb_bot::{
        arbitrage::CanaryJournalRisk,
        chain::rpc::CanonicalBlock,
        domain::compiled::{CompatibilityRole, load_compatibility_domain},
        domain::config::LoadedDomainConfig,
        market_data::alchemy::DexStreamEvent,
    };

    use super::{
        StartupDexDrainStats, m9_allowance_operation_id, m9_canary_allowance_requirements,
        m9_canary_evm_journal_scope, rebalance_quote_retry_delay, sync_runtime_ready_marker,
        validate_prefunding_marker, write_prefunding_marker,
    };

    #[test]
    fn m9_recovery_scope_matches_the_production_runtime_journal_identity() {
        let scope = m9_canary_evm_journal_scope(42_161);
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
    fn m9_post_first_parent_restart_uses_durable_remaining_allowance_authority() {
        let domain =
            LoadedDomainConfig::load("config/strategies/usdc-esp-arbitrum.v5.json").unwrap();
        let canary = domain.snapshot().pairs[0].live_canary.as_ref().unwrap();
        let first_admitted_unix_us = 1_785_426_526_104_975_u64;
        let post_trade_usdc = U256::from(16_860_785_u64);
        let post_trade_esp = U256::from_str_radix("534000000000000000000", 10).unwrap();
        let mut risk = CanaryJournalRisk {
            admitted_parent_count: 1,
            active_parent_count: 0,
            failed_parent_count: 0,
            admitted_notional_token_a_base_units: 9_940_515,
            realized_loss_token_a_base_units: 0,
            first_admitted_unix_us: Some(first_admitted_unix_us),
        };

        let (usdc_required, esp_required) = m9_canary_allowance_requirements(
            canary,
            risk,
            post_trade_usdc,
            post_trade_esp,
            first_admitted_unix_us + 60_000_000,
        )
        .unwrap()
        .unwrap();
        assert_eq!(usdc_required, U256::from(10_059_485_u64));
        assert_eq!(
            esp_required,
            U256::from_str_radix("400000000000000000000", 10).unwrap()
        );
        assert_ne!(
            m9_allowance_operation_id("USDC", usdc_required),
            m9_allowance_operation_id("USDC", post_trade_usdc)
        );

        risk.failed_parent_count = 1;
        assert_eq!(
            m9_canary_allowance_requirements(
                canary,
                risk,
                post_trade_usdc,
                post_trade_esp,
                first_admitted_unix_us + 60_000_000,
            )
            .unwrap(),
            None
        );

        risk.failed_parent_count = 0;
        risk.admitted_parent_count = 2;
        assert_eq!(
            m9_canary_allowance_requirements(
                canary,
                risk,
                post_trade_usdc,
                post_trade_esp,
                first_admitted_unix_us + 60_000_000,
            )
            .unwrap(),
            None
        );

        risk.admitted_parent_count = 1;
        assert_eq!(
            m9_canary_allowance_requirements(
                canary,
                risk,
                post_trade_usdc,
                post_trade_esp,
                first_admitted_unix_us + 901_000_000,
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn prefunding_marker_is_atomic_exact_and_prevents_a_second_funding_run() {
        let directory = std::env::temp_dir().join(format!(
            "arb-bot-prefunding-marker-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&directory).unwrap();
        let marker = directory.join("m9-prefunding.json");
        let wallet = Address::repeat_byte(7);
        assert!(
            !validate_prefunding_marker(
                &marker,
                "fingerprint",
                "snapshot",
                wallet,
                "25000000",
                "400000000000000000000",
                "2026-07-30T06:16:57Z",
            )
            .unwrap()
        );
        write_prefunding_marker(
            &marker,
            "fingerprint",
            "snapshot",
            wallet,
            "25000000",
            "400000000000000000000",
            "2026-07-30T06:16:57Z",
        )
        .unwrap();
        assert!(
            validate_prefunding_marker(
                &marker,
                "fingerprint",
                "snapshot",
                wallet,
                "25000000",
                "400000000000000000000",
                "2026-07-30T06:16:57Z",
            )
            .unwrap()
        );
        assert!(
            validate_prefunding_marker(
                &marker,
                "another-fingerprint",
                "snapshot",
                wallet,
                "25000000",
                "400000000000000000000",
                "2026-07-30T06:16:57Z",
            )
            .unwrap()
        );
        assert!(
            validate_prefunding_marker(
                &marker,
                "another-fingerprint",
                "snapshot",
                wallet,
                "26000000",
                "400000000000000000000",
                "2026-07-30T06:16:57Z",
            )
            .is_err()
        );
        std::fs::remove_dir_all(&directory).unwrap();
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
    fn startup_dex_backlog_has_separate_count_and_queue_age() {
        let mut first = StartupDexDrainStats::default();
        first.observe(&DexStreamEvent::Head {
            head: CanonicalBlock {
                number: 1,
                hash: B256::repeat_byte(1),
                parent_hash: B256::ZERO,
            },
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

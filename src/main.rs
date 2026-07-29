use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_primitives::{Address, U256};
use anyhow::{Context, bail, ensure};
use arb_bot::{
    across::{
        AcrossClient, AcrossQuoteRequest, OPTIMISM_CHAIN_ID, OPTIMISM_USDC, WORLD_CHAIN_CHAIN_ID,
        WORLD_CHAIN_USDC, is_retryable_quote_error, validate_quote,
    },
    arbitrage::{
        EntryPreflightHandle, ExecutionMode, LegRole, LegStatus, PaperTradeCoordinator, TradeStage,
        paper_trade_channel,
    },
    balances::{
        BalanceEvent, BalanceSync, WalletBalanceSnapshot, WalletReadClient, binance_snapshot,
        fetch_wallet_snapshot, fetch_wallet_snapshot_coordinated, spawn_balance_sync,
    },
    binance::account::{BinanceAccountClient, BinanceAccountState, BinanceClockSync},
    binance::capital::{
        CapitalRecoverySnapshot, CapitalRouteState, TravelRuleWithdrawalRecord, WithdrawalRecord,
        select_capital_routes,
    },
    binance::{
        execution::BinanceExecutionService,
        order_journal::{BinanceOrderJournal, BinanceOrderProgress},
        runtime::SharedBinanceRuntime,
        user_data::UserDataStream,
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
            CompatibilityRole, CompiledBinanceRuntimePlan, CompiledGraphSummary,
            CompiledHotPathRuntimePlan, CompiledNetworkGasPolicy, CompiledNetworkRuntimePlan,
            CompiledPortfolioRuntimePlan, compile_manifest_to_path, load_compatibility_domain,
        },
        config::{DexProvider, LoadedDomainConfig},
    },
    engine::{AdaptiveSizingJob, AdaptiveSizingTaskResult, BinanceFeeBps, TradingEngine},
    execution_accounting::{CommissionAssetValuation, binance_leg_result},
    hot_telemetry,
    live_execution::{
        ComposedLiveLegExecutor, ComposedLiveLegExecutorConfig, LiveRiskLimits, live_trade_channel,
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
    portfolio::PortfolioCatalog,
    rebalance::{
        RebalanceExecutionOperation, RebalanceExecutionRequest, RebalanceExecutor,
        RebalanceRuntimeLimits, RebalanceTracker, V12RebalanceParityAdapter,
        route_candidates_from_capital,
    },
    state::{QuoteApplyResult, RuntimePhase, RuntimeState, TopOfBook},
    strategy_runtime::{
        CompiledStrategyDependencyIndex, HotPathDecisionOwner, LatestOnlySizingSlots,
        ShadowSizingJob, ShadowSizingTaskResult, ShadowStrategyEvaluator, SizingSubmission,
        TelemetryCoordinatorShadowSink,
    },
    telemetry::{
        ARBITRAGE_RESULT_KIND, ExecutionLatencyTelemetry, TelemetryHandle, TelemetryWriter,
    },
    wallet::{
        EvmWallet, OPTIMISM_RPC_URL_ENV, TokenBalanceRequest, WALLET_JOURNAL_PATH_ENV,
        hydrate_chain_wallet,
    },
};
use clap::Parser;
use futures_util::future::try_join_all;
use rust_decimal::Decimal;
use std::str::FromStr;
use tokio::time::MissedTickBehavior;
use tracing_subscriber::{EnvFilter, fmt};

const ARBITRAGE_WALLET_JOURNAL_PATH_ENV: &str = "ARBITRAGE_WALLET_JOURNAL_PATH";
const ARBITRAGE_BINANCE_ORDER_JOURNAL_PATH_ENV: &str = "ARBITRAGE_BINANCE_ORDER_JOURNAL_PATH";
const BINANCE_CLOCK_SYNC_INTERVAL: Duration = Duration::from_secs(60);
const DEX_REVERT_DIAGNOSTIC_CHANNEL_CAPACITY: usize = 32;
const REBALANCE_QUOTE_RETRY_INITIAL_DELAY: Duration = Duration::from_secs(5);
const REBALANCE_QUOTE_RETRY_MAX_DELAY: Duration = Duration::from_secs(60);

enum RebalanceExecutorEvent {
    Recovery(Result<RebalanceExecutionOperation, String>),
    Execution(Result<RebalanceExecutionOperation, String>),
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
        withdrawal_status = record.withdrawal_status,
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
            !arbitrum.execution().mutation_enabled()
                && matches!(
                    arbitrum.execution().gas_policy(),
                    CompiledNetworkGasPolicy::ReadOnly
                ),
            "Arbitrum network runtime must remain read-only in M3"
        );
    }
    let shadow_strategy_plan = hot_path_dependencies.as_ref().and_then(|dependencies| {
        dependencies
            .plan()
            .strategies
            .iter()
            .find(|strategy| strategy.observe && !strategy.execute)
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
                == 1,
            "M4 production hot path requires exactly one executable compatibility strategy"
        );
        ensure!(
            dependencies
                .plan()
                .strategies
                .iter()
                .filter(|strategy| strategy.observe && !strategy.execute)
                .count()
                == 1,
            "M4 production hot path requires exactly one non-mutating shadow strategy"
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
            runtime.executable_symbols.len() == 1
                && runtime.executable_symbols.contains(&pair.binance.symbol),
            "compiled Binance capabilities must enable only the reviewed WLD execution symbol"
        );
    }
    let mut binance_account_client = BinanceAccountClient::from_env(&config)?;
    let startup_binance_clock_sync = binance_account_client.synchronize_clock_observed().await?;
    let mut user_data_stream =
        UserDataStream::connect(&config, startup_binance_clock_sync.offset_ms).await?;
    let shared_binance_account = binance_account_client
        .hydrate_symbols_after_subscription(binance_symbols.clone(), startup_binance_clock_sync)
        .await?;
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
    for symbol in &binance_symbols {
        if symbol != &pair.binance.symbol {
            ensure!(
                shared_binance_runtime.ensure_order_enabled(symbol).is_err(),
                "non-reviewed Binance symbol {symbol} unexpectedly permits orders"
            );
        }
    }
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
    let rebalance_tracker = if pair.rebalance.enabled {
        let coins = binance_account_client.all_coin_information().await?;
        let mut routes = BTreeMap::new();
        for token in [&pair.token_a, &pair.token_b] {
            let capital = select_capital_routes(
                &coins,
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
        let executor = RebalanceExecutor::hydrate(
            binance_account_client.clone(),
            treasury_client,
            subaccount_email,
            AcrossClient::new(&config)?,
            wallet_rpc.clone(),
            JsonRpcClient::new(optimism_endpoint)?,
            wallet,
            config.rebalance_executor_journal_path.clone(),
            transaction_journal_path.into(),
            RebalanceRuntimeLimits {
                maximum_wld: config.rebalance_max_wld_amount,
                maximum_usdc: config.rebalance_max_usdc_amount,
                operation_timeout: Duration::from_secs(config.rebalance_executor_timeout_seconds),
                binance_withdrawal_api_mode: config.rebalance_binance_withdrawal_api_mode.clone(),
            },
        )
        .await?;
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
    let entry_preflight = EntryPreflightHandle::default();
    let live_trade_runtime = if config.arbitrage_execution_mode == "full_live" {
        ensure!(
            domain_config.snapshot().live_trading_enabled && pair.execution_enabled,
            "composed live arbitrage requires both versioned execution gates"
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
        let binance_service = BinanceExecutionService::spawn_instrumented(
            multiplexed_binance_api.clone(),
            binance_journal_path.into(),
            config.arbitrage_leg_execution_channel_capacity,
            execution_latency_telemetry,
        )
        .await?;
        let (dex_revert_diagnostics, dex_revert_diagnostic_task) = dex_revert_diagnostic_channel(
            wallet_rpc.clone(),
            telemetry.clone(),
            config.engine_id.clone(),
            DEX_REVERT_DIAGNOSTIC_CHANNEL_CAPACITY,
        );
        let executor = ComposedLiveLegExecutor::new(
            dex_service,
            binance_service,
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
        )?;
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
            },
        )?;
        Some((
            handle,
            tokio::spawn(task.run()),
            events,
            tokio::spawn(dex_revert_diagnostic_task.run()),
        ))
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
    let (primary_engine, hot_telemetry, portfolio_allocator) = TradingEngine::new(
        config.clone(),
        Arc::clone(&domain_config),
        mirror,
        telemetry.clone(),
        V12RebalanceParityAdapter::new(rebalance_tracker),
        arb_bot::engine::TradingExecutionHandles {
            paper_trades,
            entry_preflight,
            binance_asset_decimals: compiled_binance_runtime
                .as_ref()
                .map(|runtime| runtime.asset_decimals.clone())
                .unwrap_or_default(),
            portfolio_catalog: Arc::clone(&portfolio_catalog),
        },
        BinanceFeeBps {
            buy: binance_buy_fee_bps,
            sell: binance_sell_fee_bps,
        },
    )?;
    let dependencies =
        hot_path_dependencies.context("run requires the compiled M4 hot-path runtime plan")?;
    let shadow_plan =
        shadow_strategy_plan.context("compiled M4 hot path has no non-mutating shadow strategy")?;
    let InitializedDex {
        mirror: shadow_mirror,
        stream: shadow_stream,
        rpc: _shadow_wallet_rpc,
        timings: _shadow_dex_timings,
    } = shadow_initialized_dex
        .context("compiled M4 shadow strategy has no initialized DEX runtime")?;
    let shadow_opportunities =
        OpportunityEngine::new(shadow_plan.domain_config.snapshot(), &shadow_mirror)?;
    let (shadow_hot_telemetry, shadow_hot_telemetry_writer) = hot_telemetry::channel(
        &config,
        shadow_opportunities.pairs(),
        &shadow_mirror,
        telemetry.clone(),
    )?;
    let shadow_pair = shadow_plan
        .domain_config
        .snapshot()
        .pairs
        .first()
        .context("compiled M4 shadow strategy has no projected pair")?;
    let shadow_evaluator = ShadowStrategyEvaluator::new(
        shadow_plan.strategy_id.clone(),
        shadow_plan.symbol.clone(),
        shadow_plan.baseline_budget_us,
        shadow_pair.strategy.max_transport_silence_ms(),
        config.dex_head_max_age_ms,
        shadow_mirror,
        shadow_opportunities,
        shadow_hot_telemetry,
        Box::new(TelemetryCoordinatorShadowSink::new(
            telemetry.clone(),
            config.engine_id.clone(),
        )),
    );
    let mut engine = HotPathDecisionOwner::new(
        primary_engine,
        vec![Box::new(shadow_evaluator)],
        dependencies,
    )?;
    let AlchemyDexStream {
        receiver: mut shadow_dex_receiver,
        task: mut shadow_dex_task,
    } = shadow_stream;
    engine.on_binance_clock_sync(binance_account.clock_sync);
    let hot_telemetry_task = tokio::spawn(hot_telemetry.run());
    let portfolio_allocator_task = tokio::spawn(portfolio_allocator.run());
    let shadow_hot_telemetry_task = tokio::spawn(shadow_hot_telemetry_writer.run());
    let (binance_clock_sync_sender, mut binance_clock_sync_receiver) =
        tokio::sync::mpsc::channel(4);
    let binance_clock_sync_task = tokio::spawn(run_binance_clock_sync(
        binance_clock_sync_client,
        binance_clock_sync_sender,
    ));
    let mut binance_clock_sync_running = true;
    let (rebalance_sender, mut rebalance_receiver, mut rebalance_task) =
        if let Some(mut executor) = full_rebalance_executor.take() {
            let recover_on_start = rebalance_recovery_operation.is_some();
            let (request_sender, mut request_receiver) = tokio::sync::mpsc::channel(1);
            let (result_sender, result_receiver) = tokio::sync::mpsc::channel(1);
            let task = tokio::spawn(async move {
                if recover_on_start {
                    let result = recover_rebalance_with_quote_retries(&mut executor).await;
                    if result_sender
                        .send(RebalanceExecutorEvent::Recovery(result))
                        .await
                        .is_err()
                    {
                        return Ok::<(), anyhow::Error>(());
                    }
                }
                while let Some(request) = request_receiver.recv().await {
                    let result = execute_rebalance_with_quote_retries(&mut executor, request).await;
                    if result_sender
                        .send(RebalanceExecutorEvent::Execution(result))
                        .await
                        .is_err()
                    {
                        return Ok::<(), anyhow::Error>(());
                    }
                }
                Ok::<(), anyhow::Error>(())
            });
            (Some(request_sender), result_receiver, Some(task))
        } else {
            let (_request_sender, _request_receiver) =
                tokio::sync::mpsc::channel::<RebalanceExecutionRequest>(1);
            let (_result_sender, result_receiver) =
                tokio::sync::mpsc::channel::<RebalanceExecutorEvent>(1);
            (None, result_receiver, None)
        };
    if let Some(operation) = rebalance_recovery_operation.as_ref() {
        engine.on_rebalance_recovery_started(operation)?;
    }
    engine.on_balance_event(BalanceEvent::Binance(initial_binance_balances))?;
    engine.on_balance_event(BalanceEvent::Wallet(initial_wallet_balances))?;
    for snapshot in &portfolio_wallet_snapshots {
        if snapshot.chain_id != wallet_chain_id {
            engine.on_portfolio_wallet_snapshot(snapshot)?;
        }
    }
    engine.on_user_data_connected(user_data_subscription_id);
    dispatch_rebalance_execution(&mut engine, rebalance_sender.as_ref(), pair, wallet_owner)?;
    engine.start();
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
    let mut adaptive_sizing_slots: LatestOnlySizingSlots<AdaptiveSizingJob> =
        LatestOnlySizingSlots::new(sizing_strategy_ids)?;
    let mut shadow_sizing_tasks: tokio::task::JoinSet<ShadowSizingTaskResult> =
        tokio::task::JoinSet::new();
    let shadow_sizing_strategy_ids = engine
        .dependencies()
        .plan()
        .strategies
        .iter()
        .filter(|strategy| strategy.observe && !strategy.execute)
        .map(|strategy| strategy.strategy_id.clone())
        .collect::<Vec<_>>();
    let mut shadow_sizing_slots: LatestOnlySizingSlots<ShadowSizingJob> =
        LatestOnlySizingSlots::new(shadow_sizing_strategy_ids)?;
    let mut pending_prepared_pool_builds = PreparedPoolBuildBatch::default();
    let (startup_primary_dex, startup_shadow_dex) = drain_startup_dex_backlog(
        &mut engine,
        &shadow_plan.strategy_id,
        &mut pending_prepared_pool_builds,
        &mut dex_receiver,
        &mut shadow_dex_receiver,
        &wallet_heads,
        &receipt_heads,
    )?;
    if startup_primary_dex.pool_build_count > 0 {
        engine.evaluate_after_dex_refreshes()?;
    }
    telemetry.emit(
        "startup_dex_backlog_drain",
        serde_json::json!({
            "engine_id": config.engine_id,
            "primary_event_count": startup_primary_dex.event_count,
            "primary_pool_build_count": startup_primary_dex.pool_build_count,
            "primary_max_queue_age_us": startup_primary_dex.max_queue_age_us,
            "shadow_event_count": startup_shadow_dex.event_count,
            "shadow_max_queue_age_us": startup_shadow_dex.max_queue_age_us,
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
        hot_path_sizing_policy = "one_running_one_latest_pending_per_strategy",
        hot_path_shadow_strategy_id = %shadow_plan.strategy_id.as_str(),
        hot_path_shadow_external_mutation_authorized = false,
        portfolio_inventory_key = "inventory_location+venue_asset_id",
        portfolio_location_count = portfolio_catalog.location_count(),
        portfolio_venue_asset_count = portfolio_catalog.asset_count(),
        portfolio_economic_asset_count = portfolio_catalog.economic_asset_count(),
        portfolio_allocator_mode = ?portfolio_catalog.allocator_mode(),
        portfolio_external_mutation_authorized = false,
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
            event = shadow_dex_receiver.recv() => {
                let handler_started_at = Instant::now();
                let Some(event) = event else {
                    bail!("Arbitrum shadow DEX stream stopped; process restart will rehydrate state");
                };
                let _evaluation =
                    engine.on_shadow_dex_event(&shadow_plan.strategy_id, event)?;
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
                if engine.dependencies().for_symbol(event_symbol).next().is_some() {
                    let _summary =
                        engine.on_market_event(event, binance_feed.depth_book())?;
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
                engine.on_gas_market_event(event)?;
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
                engine.on_commission_market_event(event)?;
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
                engine.on_user_data_event(event?)?;
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
                    Some(Ok(clock_sync)) => engine.on_binance_clock_sync(clock_sync),
                    Some(Err(error)) => engine.on_binance_clock_sync_failure(&error),
                    None => {
                        binance_clock_sync_running = false;
                        engine.on_binance_clock_sync_failure(
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
                engine.on_balance_event(event)?;
                dispatch_rebalance_execution(&mut engine, rebalance_sender.as_ref(), pair, wallet_owner)?;
                record_longest_handler(
                    &mut longest_non_price_handler_us,
                    &mut longest_non_price_handler,
                    "balance_publication",
                    handler_started_at.elapsed(),
                );
            }
            result = rebalance_receiver.recv(), if rebalance_sender.is_some() => {
                let handler_started_at = Instant::now();
                let Some(result) = result else {
                    bail!("rebalance executor result channel stopped unexpectedly");
                };
                match result {
                    RebalanceExecutorEvent::Recovery(Ok(operation)) => {
                        engine.on_rebalance_recovery_result(Ok(&operation))?
                    }
                    RebalanceExecutorEvent::Recovery(Err(error)) => {
                        engine.on_rebalance_recovery_result(Err(&error))?
                    }
                    RebalanceExecutorEvent::Execution(Ok(operation)) => {
                        engine.on_rebalance_execution_result(Ok(&operation))?
                    }
                    RebalanceExecutorEvent::Execution(Err(error)) => {
                        engine.on_rebalance_execution_result(Err(&error))?
                    }
                }
                dispatch_rebalance_execution(&mut engine, rebalance_sender.as_ref(), pair, wallet_owner)?;
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
                if let Some(next) = adaptive_sizing_slots.complete(&completed_strategy_id)? {
                    adaptive_sizing_tasks.spawn_blocking(move || next.run());
                }
                record_longest_handler(
                    &mut longest_non_price_handler_us,
                    &mut longest_non_price_handler,
                    "adaptive_sizing_result",
                    handler_started_at.elapsed(),
                );
            }
            result = shadow_sizing_tasks.join_next(), if !shadow_sizing_tasks.is_empty() => {
                let handler_started_at = Instant::now();
                let result = result
                    .context("shadow sizing worker join set stopped unexpectedly")?
                    .context("shadow sizing worker panicked")?;
                let strategy_id = result.strategy_id().clone();
                let queue_time_us = result.queue_time_us();
                let worker_time_us = result.worker_time_us();
                let disposition = engine.on_shadow_sizing_result(result)?;
                telemetry.emit(
                    "strategy_sizing_task",
                    serde_json::json!({
                        "engine_id": config.engine_id,
                        "strategy_id": strategy_id.as_str(),
                        "work_class": "exhaustive_sizing",
                        "queue_policy": "one_running_one_latest_pending_per_strategy",
                        "queue_time_us": queue_time_us,
                        "worker_time_us": worker_time_us,
                        "disposition": disposition.as_str(),
                    }),
                );
                if let Some(next) = shadow_sizing_slots.complete(&strategy_id)? {
                    shadow_sizing_tasks.spawn_blocking(move || next.run());
                }
                record_longest_handler(
                    &mut longest_non_price_handler_us,
                    &mut longest_non_price_handler,
                    "shadow_sizing_result",
                    handler_started_at.elapsed(),
                );
            }
            result = &mut dex_task => {
                result.context("Alchemy DEX connector task failed")??;
                bail!("Alchemy DEX connector stopped; process restart will rehydrate state");
            }
            result = &mut shadow_dex_task => {
                result.context("Arbitrum shadow DEX connector task failed")??;
                bail!("Arbitrum shadow DEX connector stopped; process restart will rehydrate state");
            }
            result = &mut binance_balance_task => {
                result.context("Binance balance synchronization task failed")??;
                bail!("Binance balance synchronization stopped unexpectedly");
            }
            result = &mut wallet_balance_task => {
                result.context("wallet balance synchronization task failed")??;
                bail!("wallet balance synchronization stopped unexpectedly");
            }
        }
        if !first_ready_emitted && engine.phase() == RuntimePhase::Ready {
            engine.record_runtime_first_ready(bootstrap.process_started_at.elapsed());
            first_ready_emitted = true;
        }
        for job in engine.take_adaptive_sizing_jobs() {
            let strategy_id = job.strategy_id()?;
            match adaptive_sizing_slots.submit(&strategy_id, job)? {
                SizingSubmission::Start(job) => {
                    adaptive_sizing_tasks.spawn_blocking(move || job.run());
                }
                SizingSubmission::Pending { replaced } => {
                    engine.record_adaptive_sizing_overload(
                        &strategy_id,
                        replaced,
                        adaptive_sizing_slots.total_retained_work(),
                    );
                }
            }
        }
        while let Some(job) = engine.take_next_shadow_sizing_job() {
            let strategy_id = job.strategy_id().clone();
            match shadow_sizing_slots.submit(&strategy_id, job)? {
                SizingSubmission::Start(job) => {
                    shadow_sizing_tasks.spawn_blocking(move || job.run());
                }
                SizingSubmission::Pending { replaced } => {
                    engine.record_adaptive_sizing_overload(
                        &strategy_id,
                        replaced,
                        shadow_sizing_slots.total_retained_work(),
                    );
                }
            }
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
    binance_clock_sync_task.abort();
    let _ = binance_balance_task.await;
    let _ = wallet_balance_task.await;
    let _ = binance_clock_sync_task.await;
    dex_task.abort();
    let _ = dex_task.await;
    shadow_dex_task.abort();
    let _ = shadow_dex_task.await;
    adaptive_sizing_tasks.abort_all();
    while adaptive_sizing_tasks.join_next().await.is_some() {}
    shadow_sizing_tasks.abort_all();
    while shadow_sizing_tasks.join_next().await.is_some() {}
    drop(engine);
    if let Some(task) = paper_trade_task.take() {
        task.await??;
    }
    if let Some(task) = dex_revert_diagnostic_task.take() {
        task.await?;
    }
    hot_telemetry_task.await??;
    shadow_hot_telemetry_task.await??;
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

fn dispatch_rebalance_execution(
    engine: &mut TradingEngine,
    sender: Option<&tokio::sync::mpsc::Sender<RebalanceExecutionRequest>>,
    pair: &arb_bot::domain::config::PairConfig,
    wallet_owner: Address,
) -> anyhow::Result<()> {
    let Some(evaluation) = engine.take_rebalance_execution()? else {
        return Ok(());
    };
    let sender = sender.context("rebalance engine produced live work without an executor")?;
    let action = evaluation
        .plan
        .action
        .clone()
        .context("rebalance execution evaluation has no action")?;
    let token = [&pair.token_a, &pair.token_b]
        .into_iter()
        .find(|token| token.symbol == evaluation.token_symbol)
        .context("rebalance execution token is absent from the domain pair")?;
    let token_contract = token
        .contract
        .parse::<Address>()
        .context("rebalance execution token contract is invalid")?;
    sender
        .try_send(RebalanceExecutionRequest {
            token_symbol: evaluation.token_symbol,
            token_decimals: evaluation.token_decimals,
            token_contract,
            wallet_owner,
            action,
            binance_balance_before: evaluation.plan.projected.binance,
            wallet_balance_before: evaluation.plan.projected.wallet,
        })
        .context("rebalance executor queue is full or closed")?;
    Ok(())
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

fn drain_startup_dex_backlog(
    engine: &mut HotPathDecisionOwner<TradingEngine>,
    shadow_strategy_id: &arb_bot::domain::compiled::StrategyId,
    pending: &mut PreparedPoolBuildBatch,
    dex_receiver: &mut tokio::sync::mpsc::Receiver<DexStreamEvent>,
    shadow_dex_receiver: &mut tokio::sync::mpsc::Receiver<DexStreamEvent>,
    wallet_heads: &tokio::sync::watch::Sender<CanonicalBlock>,
    receipt_heads: &tokio::sync::watch::Sender<CanonicalBlock>,
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
        let shadow =
            drain_startup_shadow_dex_backlog(engine, shadow_strategy_id, shadow_dex_receiver)?;
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

fn drain_startup_shadow_dex_backlog(
    engine: &mut HotPathDecisionOwner<TradingEngine>,
    shadow_strategy_id: &arb_bot::domain::compiled::StrategyId,
    shadow_dex_receiver: &mut tokio::sync::mpsc::Receiver<DexStreamEvent>,
) -> anyhow::Result<StartupDexDrainStats> {
    let mut stats = StartupDexDrainStats::default();
    while let Ok(event) = shadow_dex_receiver.try_recv() {
        stats.observe(&event);
        let _evaluation = engine.on_shadow_startup_dex_event(shadow_strategy_id, event)?;
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
    use std::time::{Duration, Instant};

    use alloy_primitives::B256;
    use arb_bot::{chain::rpc::CanonicalBlock, market_data::alchemy::DexStreamEvent};

    use super::{StartupDexDrainStats, rebalance_quote_retry_delay};

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

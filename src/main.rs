use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};

use alloy_primitives::Address;
use anyhow::{Context, bail, ensure};
use arb_bot::{
    across::{
        AcrossClient, AcrossQuoteRequest, OPTIMISM_CHAIN_ID, OPTIMISM_USDC, WORLD_CHAIN_CHAIN_ID,
        WORLD_CHAIN_USDC, validate_quote,
    },
    balances::{
        BalanceEvent, BalanceSync, binance_snapshot, fetch_wallet_snapshot, spawn_balance_sync,
    },
    binance::account::{BinanceAccountClient, BinanceAccountState},
    binance::capital::{
        CapitalRecoverySnapshot, CapitalRouteState, TravelRuleWithdrawalRecord, WithdrawalRecord,
        select_capital_routes,
    },
    binance::ws_api::BinanceWsApiClient,
    chain::rpc::JsonRpcClient,
    config::{self, Cli, Command},
    dex::{
        events::build_log_filters,
        hydration::DexHydrator,
        mirror::{DexMirror, LogApplyResult},
    },
    domain::config::LoadedDomainConfig,
    engine::TradingEngine,
    market_data::{
        alchemy::{AlchemyDexStream, connect_dex_stream},
        binance::BookTickerFeed,
    },
    opportunity::{PreparedPoolBuildRequest, PreparedPoolBuildResult},
    rebalance::{
        RebalanceExecutionRequest, RebalanceExecutor, RebalanceRuntimeLimits, RebalanceTracker,
        route_candidates_from_capital,
    },
    telemetry::TelemetryWriter,
    wallet::{
        EvmWallet, OPTIMISM_RPC_URL_ENV, TokenBalanceRequest, WALLET_JOURNAL_PATH_ENV,
        hydrate_chain_wallet,
    },
};
use clap::Parser;
use tokio::time::MissedTickBehavior;
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    load_dotenv()?;
    init_tracing();

    let cli = Cli::parse();
    cli.config.validate()?;

    match cli.command {
        Command::Run => {
            let domain_config = Arc::new(LoadedDomainConfig::load(&cli.config.domain_config_path)?);
            run(cli.config, domain_config).await
        }
        Command::Migrate => TelemetryWriter::new(&cli.config).migrate().await,
        Command::Check => {
            let domain_config = LoadedDomainConfig::load(&cli.config.domain_config_path)?;
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
        Command::BinanceWithdrawalStatus {
            coin,
            withdraw_order_id,
        } => binance_withdrawal_status(&cli.config, &coin, &withdraw_order_id).await,
        Command::BinanceTravelRuleWithdrawalStatus { tr_id } => {
            binance_travel_rule_withdrawal_status(&cli.config, tr_id).await
        }
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
    }
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
    tracing::info!(
        validation_orders = validation_orders.len(),
        inspected_orders = orders.len(),
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

async fn run(
    config: config::AppConfig,
    domain_config: Arc<LoadedDomainConfig>,
) -> anyhow::Result<()> {
    let InitializedDex {
        mirror,
        stream,
        rpc: wallet_rpc,
    } = initialize_dex(&config, domain_config.as_ref()).await?;
    let initial_wallet_head = mirror.latest_head();
    let AlchemyDexStream {
        receiver: mut dex_receiver,
        task: mut dex_task,
    } = stream;
    let (telemetry, writer) = TelemetryWriter::new(&config).channel();
    let writer_task = tokio::spawn(writer.run());
    let binance_symbols = domain_config.binance_symbols();
    ensure!(
        binance_symbols.len() == 1,
        "direct Binance hot path currently requires exactly one enabled symbol"
    );
    let mut binance_account_client = BinanceAccountClient::from_env(&config)?;
    let binance_account = binance_account_client.hydrate(&binance_symbols[0]).await?;
    validate_binance_account(&binance_account)?;
    let mut binance_feed = BookTickerFeed::new(&config, binance_symbols[0].clone());

    let pair = domain_config
        .snapshot()
        .pairs
        .iter()
        .find(|pair| pair.market_data_enabled)
        .context("balance synchronization requires one enabled pair")?;
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
    let binance_assets = vec![
        Arc::<str>::from(pair.binance.quote_asset.as_str()),
        Arc::<str>::from(pair.binance.base_asset.as_str()),
    ];
    let mut initial_binance_balances =
        binance_snapshot(&binance_account.account, &binance_assets, 0);
    let mut initial_wallet_balances = fetch_wallet_snapshot(
        &wallet_rpc,
        wallet_owner,
        wallet_chain_id,
        &wallet_tokens,
        initial_wallet_head,
    )
    .await?;
    let mut full_rebalance_executor = if config.rebalance_execution_mode == "full_live" {
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
        let mut executor = RebalanceExecutor::hydrate(
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
        if let Some(recovered) = executor.recover_active().await? {
            tracing::warn!(
                operation_id = %recovered.intent.operation_id,
                progress = ?recovered.progress,
                "recovered active rebalance operation before runtime start"
            );
            let refreshed_account = binance_account_client.account_information().await?;
            initial_binance_balances = binance_snapshot(&refreshed_account, &binance_assets, 1);
            let refreshed_head = wallet_rpc.latest_block().await?;
            initial_wallet_balances = fetch_wallet_snapshot(
                &wallet_rpc,
                wallet_owner,
                wallet_chain_id,
                &wallet_tokens,
                refreshed_head,
            )
            .await?;
        }
        Some(executor)
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
        binance_assets,
        Duration::from_millis(config.balance_sync_interval_ms),
        wallet_rpc,
        wallet_owner,
        wallet_chain_id,
        wallet_tokens,
        initial_wallet_head,
        config.balance_event_channel_capacity,
    );

    let (mut engine, hot_telemetry) = TradingEngine::new(
        config.clone(),
        Arc::clone(&domain_config),
        mirror,
        telemetry,
        rebalance_tracker,
    )?;
    let hot_telemetry_task = tokio::spawn(hot_telemetry.run());
    let (rebalance_sender, mut rebalance_receiver, mut rebalance_task) =
        if let Some(mut executor) = full_rebalance_executor.take() {
            let (request_sender, mut request_receiver) = tokio::sync::mpsc::channel(1);
            let (result_sender, result_receiver) = tokio::sync::mpsc::channel(1);
            let task = tokio::spawn(async move {
                while let Some(request) = request_receiver.recv().await {
                    let result = executor
                        .execute(request)
                        .await
                        .map_err(|error| format!("{error:#}"));
                    if result_sender.send(result).await.is_err() {
                        return Ok::<(), anyhow::Error>(());
                    }
                }
                Ok::<(), anyhow::Error>(())
            });
            (Some(request_sender), result_receiver, Some(task))
        } else {
            let (_request_sender, _request_receiver) =
                tokio::sync::mpsc::channel::<RebalanceExecutionRequest>(1);
            let (_result_sender, result_receiver) = tokio::sync::mpsc::channel(1);
            (None, result_receiver, None)
        };
    engine.on_balance_event(BalanceEvent::Binance(initial_binance_balances));
    engine.on_balance_event(BalanceEvent::Wallet(initial_wallet_balances));
    dispatch_rebalance_execution(&mut engine, rebalance_sender.as_ref(), pair, wallet_owner)?;
    engine.start();
    let (prepared_sender, mut prepared_receiver, prepared_thread) =
        spawn_prepared_pool_builder(64)?;

    tracing::info!(
        service = %config.service_name,
        engine_id = %config.engine_id,
        gcp_project_id = %config.gcp_project_id,
        gcp_region = %config.gcp_region,
        domain_snapshot_id = %domain_config.snapshot().snapshot_id,
        domain_config_sha256 = %domain_config.fingerprint_sha256(),
        binance_symbols = ?binance_symbols,
        binance_account_type = %binance_account.account.account_type,
        binance_can_trade = binance_account.account.can_trade,
        binance_permissions = ?binance_account.account.permissions,
        binance_nonzero_balances = binance_account.account.balances.len(),
        binance_clock_offset_ms = binance_account.clock_offset_ms,
        binance_standard_maker_fee = %binance_account.commission.standard_commission.maker,
        binance_standard_taker_fee = %binance_account.commission.standard_commission.taker,
        binance_wld_balance_present = binance_account.balance("WLD").is_some(),
        binance_usdc_balance_present = binance_account.balance("USDC").is_some(),
        wallet_address = %wallet_owner,
        wallet_chain_id,
        balance_sync_interval_ms = config.balance_sync_interval_ms,
        balance_max_age_ms = config.balance_max_age_ms,
        wallet_sync_trigger = "alchemy_new_heads",
        clickhouse_enabled = config.clickhouse_enabled(),
        rebalance_execution_mode = %config.rebalance_execution_mode,
        "arbitrage shadow service started with authenticated Binance account state"
    );
    let runtime_ready_file = mark_runtime_ready()?;

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let health_interval =
        Duration::from_millis((config.market_data_max_age_ms / 4).clamp(100, 1_000));
    let mut health_tick = tokio::time::interval(health_interval);
    health_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    health_tick.reset();

    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            _ = health_tick.tick() => engine.refresh_health(),
            event = binance_feed.next_event() => engine.on_market_event(event)?,
            event = balance_receiver.recv() => {
                let Some(event) = event else {
                    bail!("balance synchronization channel stopped unexpectedly");
                };
                engine.on_balance_event(event);
                dispatch_rebalance_execution(&mut engine, rebalance_sender.as_ref(), pair, wallet_owner)?;
            }
            result = rebalance_receiver.recv(), if rebalance_sender.is_some() => {
                let Some(result) = result else {
                    bail!("rebalance executor result channel stopped unexpectedly");
                };
                match result {
                    Ok(operation) => engine.on_rebalance_execution_result(Ok(&operation.intent.operation_id)),
                    Err(error) => engine.on_rebalance_execution_result(Err(&error)),
                }
                dispatch_rebalance_execution(&mut engine, rebalance_sender.as_ref(), pair, wallet_owner)?;
            }
            event = dex_receiver.recv() => {
                let Some(event) = event else {
                    bail!("Alchemy DEX stream stopped; process restart will rehydrate state");
                };
                let wallet_head = match &event {
                    arb_bot::market_data::alchemy::DexStreamEvent::Head { head, .. } => Some(*head),
                    arb_bot::market_data::alchemy::DexStreamEvent::Log { .. } => None,
                };
                if let Some(request) = engine.on_dex_event(event)? {
                    prepared_sender
                        .try_send(request)
                        .context("prepared DEX builder queue is full or closed")?;
                }
                if let Some(head) = wallet_head
                    && *wallet_heads.borrow() != head
                {
                    wallet_heads.send_replace(head);
                }
            }
            result = prepared_receiver.recv() => {
                let Some(result) = result else {
                    bail!("prepared DEX builder stopped unexpectedly");
                };
                engine.on_prepared_pool(result?)?;
            }
            result = &mut dex_task => {
                result.context("Alchemy DEX connector task failed")??;
                bail!("Alchemy DEX connector stopped; process restart will rehydrate state");
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
    }

    engine.shutdown();
    drop(rebalance_sender);
    if let Some(task) = rebalance_task.take() {
        task.abort();
        let _ = task.await;
    }
    binance_balance_task.abort();
    wallet_balance_task.abort();
    let _ = binance_balance_task.await;
    let _ = wallet_balance_task.await;
    dex_task.abort();
    let _ = dex_task.await;
    drop(engine);
    drop(prepared_sender);
    drop(prepared_receiver);
    prepared_thread
        .join()
        .map_err(|_| anyhow::anyhow!("prepared DEX builder thread panicked"))?;

    hot_telemetry_task.await??;
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

fn dispatch_rebalance_execution(
    engine: &mut TradingEngine,
    sender: Option<&tokio::sync::mpsc::Sender<RebalanceExecutionRequest>>,
    pair: &arb_bot::domain::config::PairConfig,
    wallet_owner: Address,
) -> anyhow::Result<()> {
    let Some(evaluation) = engine.take_rebalance_execution() else {
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
    let Some(path) = std::env::var_os("RUNTIME_READY_FILE") else {
        return Ok(None);
    };
    let path = PathBuf::from(path);
    ensure!(
        !path.as_os_str().is_empty(),
        "RUNTIME_READY_FILE must not be empty"
    );
    std::fs::write(&path, b"ready\n").with_context(|| {
        format!(
            "failed to write runtime readiness marker {}",
            path.display()
        )
    })?;
    Ok(Some(path))
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
        binance_wld_balance_present = state.balance("WLD").is_some(),
        binance_usdc_balance_present = state.balance("USDC").is_some(),
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

type PreparedBuildResult = anyhow::Result<PreparedPoolBuildResult>;

fn spawn_prepared_pool_builder(
    capacity: usize,
) -> anyhow::Result<(
    tokio::sync::mpsc::Sender<PreparedPoolBuildRequest>,
    tokio::sync::mpsc::Receiver<PreparedBuildResult>,
    std::thread::JoinHandle<()>,
)> {
    let (request_sender, mut request_receiver) =
        tokio::sync::mpsc::channel::<PreparedPoolBuildRequest>(capacity);
    let (result_sender, result_receiver) =
        tokio::sync::mpsc::channel::<PreparedBuildResult>(capacity);
    let thread = std::thread::Builder::new()
        .name("dex-curve-builder".into())
        .spawn(move || {
            while let Some(request) = request_receiver.blocking_recv() {
                if result_sender.blocking_send(request.build()).is_err() {
                    break;
                }
            }
        })
        .context("failed to spawn prepared DEX builder thread")?;
    Ok((request_sender, result_receiver, thread))
}

struct InitializedDex {
    mirror: DexMirror,
    stream: AlchemyDexStream,
    rpc: JsonRpcClient,
}

async fn initialize_dex(
    config: &config::AppConfig,
    domain_config: &LoadedDomainConfig,
) -> anyhow::Result<InitializedDex> {
    let (rpc_endpoint, ws_endpoint) = chain_endpoints(domain_config)?;
    let rpc = JsonRpcClient::new(rpc_endpoint)?;
    let hydrated = DexHydrator::new(&rpc)
        .hydrate(domain_config.snapshot())
        .await?;
    let hydration_block = hydrated.block;
    let filters = build_log_filters(domain_config.snapshot(), &hydrated)?;
    let stream =
        connect_dex_stream(&ws_endpoint, &filters, config.dex_event_channel_capacity).await?;

    // The subscription is live before the upper backfill bound is captured.
    // Logs emitted during hydration/subscription are recovered over HTTP;
    // duplicate WSS notifications at or below this bound are ignored.
    let backfill_head = rpc.latest_block().await?;
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

    let mut mirror = DexMirror::new(hydrated)?;
    let mut applied = 0_usize;
    for log in &backfill {
        if matches!(mirror.apply_log(log)?, LogApplyResult::Applied { .. }) {
            applied += 1;
        }
    }
    mirror.finish_backfill(backfill_head)?;
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
    })
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

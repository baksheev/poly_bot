use std::{str::FromStr, sync::Arc, time::Duration};

use alloy_primitives::Address;
use anyhow::{Context, bail, ensure};
use arb_bot::{
    binance::account::{BinanceAccountClient, BinanceAccountState},
    binance::capital::{
        CapitalRouteState, NetworkInformation, TravelRuleWithdrawalRecord, WithdrawalRecord,
        select_capital_routes,
    },
    binance::ws_api::{BinanceWsApiClient, OrderResult, WsApiError},
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
    telemetry::TelemetryWriter,
    wallet::{OPTIMISM_RPC_URL_ENV, TestWallet, TokenBalanceRequest, hydrate_chain_wallet},
};
use clap::Parser;
use rust_decimal::Decimal;
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
        Command::BinanceManualRoundTrip {
            quote_amount,
            confirm_live,
        } => {
            let domain_config = LoadedDomainConfig::load(&cli.config.domain_config_path)?;
            binance_manual_round_trip(&cli.config, &domain_config, quote_amount, confirm_live).await
        }
        Command::BinanceRecentValidationOrders { limit } => {
            binance_recent_validation_orders(&cli.config, limit).await
        }
        Command::BinanceManualEthBuy {
            quote_amount,
            confirm_live,
        } => binance_manual_eth_buy(&cli.config, quote_amount, confirm_live).await,
        Command::BinanceManualWalletWithdraw {
            coin,
            network,
            amount,
            confirm_live,
        } => {
            binance_manual_wallet_withdraw(&cli.config, &coin, &network, amount, confirm_live).await
        }
        Command::BinanceWithdrawalStatus {
            coin,
            withdraw_order_id,
        } => binance_withdrawal_status(&cli.config, &coin, &withdraw_order_id).await,
        Command::BinanceTravelRuleWithdrawalStatus { tr_id } => {
            binance_travel_rule_withdrawal_status(&cli.config, tr_id).await
        }
        Command::WalletAddress => {
            let wallet = TestWallet::from_env()?;
            tracing::info!(address = %wallet.address(), "EVM test wallet loaded");
            Ok(())
        }
        Command::WalletHydrate => {
            let domain_config = LoadedDomainConfig::load(&cli.config.domain_config_path)?;
            wallet_hydrate(&domain_config).await
        }
    }
}

async fn binance_manual_wallet_withdraw(
    config: &config::AppConfig,
    coin: &str,
    network: &str,
    amount: Decimal,
    confirm_live: bool,
) -> anyhow::Result<()> {
    ensure!(
        confirm_live,
        "live Binance withdrawal requires explicit --confirm-live"
    );
    let cap = withdrawal_cap(coin, network)?;
    ensure!(
        amount > Decimal::ZERO && amount <= cap,
        "withdrawal amount exceeds the canary cap for this coin/network"
    );
    let wallet = TestWallet::from_env()?;
    let address = format!("{:#x}", wallet.address());
    let commission_symbol = match coin {
        "ETH" => "ETHUSDT",
        "WLD" | "USDC" => "WLDUSDC",
        _ => bail!("unsupported withdrawal coin"),
    };
    let mut client = BinanceAccountClient::from_env(config)?;
    let account = client.hydrate(commission_symbol).await?;
    validate_binance_account(&account)?;
    ensure!(
        account.account.can_withdraw,
        "Binance account does not permit withdrawals"
    );
    let coins = client.all_coin_information().await?;
    let coin_state = coins
        .iter()
        .find(|state| state.coin == coin)
        .with_context(|| format!("Binance capital state is missing {coin}"))?;
    ensure!(
        coin_state.withdraw_all_enable,
        "Binance withdrawals are globally disabled for this coin"
    );
    let network_state = coin_state
        .network_list
        .iter()
        .find(|state| state.network == network)
        .with_context(|| format!("Binance capital state is missing network {network}"))?;
    validate_withdrawal_amount(network_state, amount)?;
    let required_balance = amount + network_state.withdraw_fee;
    ensure!(
        free_balance(&account, coin) >= required_balance,
        "free Binance balance does not cover amount plus live withdrawal fee"
    );

    let withdraw_order_id = format!("rustwd{}", unix_timestamp_ms()?);
    let submission = client
        .withdraw(coin, network, &address, amount, &withdraw_order_id)
        .await?;
    ensure!(
        submission.accepted,
        "Binance rejected the Travel Rule withdrawal: {}",
        submission.info
    );
    tracing::info!(
        coin,
        network,
        amount = %amount,
        fee = %network_state.withdraw_fee,
        destination = %address,
        withdraw_order_id,
        travel_rule_id = %submission.tr_id,
        travel_rule_info = %submission.info,
        "capped Binance Travel Rule wallet withdrawal submitted"
    );

    tokio::time::sleep(Duration::from_secs(2)).await;
    let records = client.withdrawal_history(coin, &withdraw_order_id).await?;
    if let Some(record) = records.first() {
        validate_withdrawal_record(record, coin, network, &address, &withdraw_order_id)?;
        log_withdrawal_record(record);
    } else {
        tracing::warn!(
            coin,
            network,
            withdraw_order_id,
            travel_rule_id = %submission.tr_id,
            "withdrawal accepted but not visible in history yet"
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

fn withdrawal_cap(coin: &str, network: &str) -> anyhow::Result<Decimal> {
    match (coin, network) {
        ("ETH", "OPTIMISM") => Ok(Decimal::new(2, 2)),
        ("WLD", "WLD" | "OPTIMISM") => Ok(Decimal::from(50_u64)),
        ("USDC", "OPTIMISM") => Ok(Decimal::from(100_u64)),
        _ => bail!("coin/network is outside the manual withdrawal allowlist"),
    }
}

fn validate_withdrawal_amount(network: &NetworkInformation, amount: Decimal) -> anyhow::Result<()> {
    ensure!(
        network.withdrawal_available(),
        "Binance network withdrawal is disabled or busy"
    );
    ensure!(
        amount >= network.withdraw_min && amount <= network.withdraw_max,
        "withdrawal amount is outside Binance live network limits"
    );
    if network.withdraw_integer_multiple > Decimal::ZERO {
        ensure!(
            amount % network.withdraw_integer_multiple == Decimal::ZERO,
            "withdrawal amount does not match Binance integer multiple"
        );
    }
    Ok(())
}

fn validate_withdrawal_record(
    record: &WithdrawalRecord,
    coin: &str,
    network: &str,
    address: &str,
    withdraw_order_id: &str,
) -> anyhow::Result<()> {
    ensure!(record.coin == coin, "withdrawal history coin mismatch");
    ensure!(
        record.network == network,
        "withdrawal history network mismatch"
    );
    ensure!(
        record.address.eq_ignore_ascii_case(address),
        "withdrawal history destination mismatch"
    );
    ensure!(
        record.withdraw_order_id == withdraw_order_id,
        "withdrawal history client id mismatch"
    );
    Ok(())
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

async fn binance_manual_eth_buy(
    config: &config::AppConfig,
    quote_amount: Decimal,
    confirm_live: bool,
) -> anyhow::Result<()> {
    ensure!(
        confirm_live,
        "live Binance ETH purchase requires explicit --confirm-live"
    );
    ensure!(
        quote_amount > Decimal::ZERO && quote_amount <= Decimal::from(200_u64),
        "ETH gas purchase must be greater than zero and no more than 200 USDT"
    );
    let mut account_client = BinanceAccountClient::from_env(config)?;
    let before = account_client.hydrate("ETHUSDT").await?;
    validate_binance_account(&before)?;
    let usdt_before = free_balance(&before, "USDT");
    let eth_before = free_balance(&before, "ETH");
    ensure!(
        usdt_before >= quote_amount,
        "insufficient free USDT for capped ETH gas purchase"
    );

    let client_order_id = format!("rustgas{}B", unix_timestamp_ms()?);
    let mut ws = BinanceWsApiClient::connect(config).await?;
    ws.test_market_buy("ETHUSDT", quote_amount, &client_order_id)
        .await
        .context("Binance ETHUSDT MARKET buy order.test failed")?;
    let buy = match ws
        .place_market_buy("ETHUSDT", quote_amount, &client_order_id)
        .await
    {
        Ok(order) => order,
        Err(error) => reconcile_unknown_order(config, "ETHUSDT", &client_order_id, error)
            .await
            .context("Binance ETHUSDT MARKET buy failed and could not be reconciled")?,
    };
    validate_filled_order(&buy, "ETHUSDT", "BUY", &client_order_id)?;
    ensure!(
        buy.cummulative_quote_qty <= Decimal::from(200_u64),
        "Binance ETH purchase exceeded the absolute 200 USDT validation cap"
    );
    let reconciled = ws
        .query_order("ETHUSDT", &client_order_id)
        .await
        .context("failed to reconcile filled Binance ETH purchase")?;
    validate_filled_order(&reconciled, "ETHUSDT", "BUY", &client_order_id)?;

    let after = account_client.hydrate("ETHUSDT").await?;
    validate_binance_account(&after)?;
    let usdt_after = free_balance(&after, "USDT");
    let eth_after = free_balance(&after, "ETH");
    tracing::info!(
        order_id = buy.order_id,
        client_order_id = %buy.client_order_id,
        executed_eth = %buy.executed_qty,
        executed_usdt = %buy.cummulative_quote_qty,
        usdt_before = %usdt_before,
        usdt_after = %usdt_after,
        eth_before = %eth_before,
        eth_after = %eth_after,
        eth_delta = %(eth_after - eth_before),
        "capped live ETH gas purchase completed and reconciled"
    );
    Ok(())
}

async fn wallet_hydrate(domain_config: &LoadedDomainConfig) -> anyhow::Result<()> {
    let wallet = TestWallet::from_env()?;
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
    let address = wallet.address();
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
        pending_nonce = state.pending_nonce,
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

async fn binance_manual_round_trip(
    config: &config::AppConfig,
    domain_config: &LoadedDomainConfig,
    quote_amount: Decimal,
    confirm_live: bool,
) -> anyhow::Result<()> {
    ensure!(
        confirm_live,
        "live Binance validation requires explicit --confirm-live"
    );
    ensure!(
        quote_amount > Decimal::ZERO && quote_amount <= Decimal::from(100_u64),
        "quote amount must be greater than zero and no more than 100 USDC"
    );
    let pairs = &domain_config.snapshot().pairs;
    ensure!(
        pairs.len() == 1,
        "live Binance validation requires exactly one configured pair"
    );
    let pair = &pairs[0];
    ensure!(
        pair.binance.symbol == "WLDUSDC"
            && pair.binance.base_asset == "WLD"
            && pair.binance.quote_asset == "USDC",
        "live Binance validation is hard-limited to WLDUSDC"
    );
    let step_size = Decimal::from_str(&pair.binance.step_size)
        .context("validated Binance step size is invalid")?;

    let mut account_client = BinanceAccountClient::from_env(config)?;
    let before = account_client.hydrate(&pair.binance.symbol).await?;
    validate_binance_account(&before)?;
    let usdc_before = free_balance(&before, "USDC");
    let wld_before = free_balance(&before, "WLD");
    ensure!(
        usdc_before >= quote_amount,
        "insufficient free USDC for capped live validation"
    );

    let sequence = unix_timestamp_ms()?;
    let buy_client_id = format!("rustval{sequence}B");
    let sell_client_id = format!("rustval{sequence}S");
    let mut ws = BinanceWsApiClient::connect(config).await?;
    ws.test_market_buy(&pair.binance.symbol, quote_amount, &buy_client_id)
        .await
        .context("Binance MARKET buy order.test failed")?;
    let buy = match ws
        .place_market_buy(&pair.binance.symbol, quote_amount, &buy_client_id)
        .await
    {
        Ok(order) => order,
        Err(error) => reconcile_unknown_order(config, &pair.binance.symbol, &buy_client_id, error)
            .await
            .context("Binance MARKET buy failed and could not be reconciled")?,
    };
    validate_filled_order(&buy, &pair.binance.symbol, "BUY", &buy_client_id)?;
    ensure!(
        buy.cummulative_quote_qty <= Decimal::from(100_u64),
        "Binance buy exceeded the absolute 100 USDC validation cap"
    );

    let acquired_wld = buy.executed_qty - buy.commission_in("WLD");
    let sell_quantity = (acquired_wld / step_size).floor() * step_size;
    ensure!(
        sell_quantity > Decimal::ZERO,
        "buy produced no sellable WLD"
    );
    ensure!(
        sell_quantity <= acquired_wld,
        "rounded sell quantity exceeds acquired WLD"
    );

    ws.test_market_sell(&pair.binance.symbol, sell_quantity, &sell_client_id)
        .await
        .context("Binance MARKET sell order.test failed")?;
    let sell = match ws
        .place_market_sell(&pair.binance.symbol, sell_quantity, &sell_client_id)
        .await
    {
        Ok(order) => order,
        Err(error) => reconcile_unknown_order(config, &pair.binance.symbol, &sell_client_id, error)
            .await
            .context("Binance MARKET sell failed and could not be reconciled")?,
    };
    validate_filled_order(&sell, &pair.binance.symbol, "SELL", &sell_client_id)?;

    let reconciled_buy = ws
        .query_order(&pair.binance.symbol, &buy_client_id)
        .await
        .context("failed to reconcile filled Binance buy")?;
    let reconciled_sell = ws
        .query_order(&pair.binance.symbol, &sell_client_id)
        .await
        .context("failed to reconcile filled Binance sell")?;
    validate_filled_order(&reconciled_buy, &pair.binance.symbol, "BUY", &buy_client_id)?;
    validate_filled_order(
        &reconciled_sell,
        &pair.binance.symbol,
        "SELL",
        &sell_client_id,
    )?;

    let after = account_client.hydrate(&pair.binance.symbol).await?;
    validate_binance_account(&after)?;
    let usdc_after = free_balance(&after, "USDC");
    let wld_after = free_balance(&after, "WLD");
    tracing::info!(
        symbol = %pair.binance.symbol,
        quote_cap = %quote_amount,
        buy_order_id = buy.order_id,
        buy_client_order_id = %buy.client_order_id,
        buy_executed_base = %buy.executed_qty,
        buy_executed_quote = %buy.cummulative_quote_qty,
        sell_order_id = sell.order_id,
        sell_client_order_id = %sell.client_order_id,
        sell_executed_base = %sell.executed_qty,
        sell_executed_quote = %sell.cummulative_quote_qty,
        usdc_before = %usdc_before,
        usdc_after = %usdc_after,
        usdc_delta = %(usdc_after - usdc_before),
        wld_before = %wld_before,
        wld_after = %wld_after,
        wld_delta = %(wld_after - wld_before),
        binance_ws_clock_offset_ms = ws.clock_offset_ms(),
        "capped live Binance MARKET round trip completed and reconciled"
    );
    Ok(())
}

async fn reconcile_unknown_order(
    config: &config::AppConfig,
    symbol: &str,
    client_order_id: &str,
    placement_error: WsApiError,
) -> anyhow::Result<OrderResult> {
    if matches!(placement_error, WsApiError::Rejected { .. }) {
        bail!(placement_error);
    }
    let mut reconciliation = BinanceWsApiClient::connect(config)
        .await
        .context("failed to reconnect for order reconciliation")?;
    let order = reconciliation
        .query_order(symbol, client_order_id)
        .await
        .with_context(|| format!("placement outcome was unknown: {placement_error}"))?;
    tracing::warn!(
        order_id = order.order_id,
        status = %order.status,
        client_order_id,
        "Binance placement transport outcome was unknown; order recovered by client id"
    );
    Ok(order)
}

fn validate_filled_order(
    order: &OrderResult,
    symbol: &str,
    side: &str,
    client_order_id: &str,
) -> anyhow::Result<()> {
    ensure!(
        order.symbol == symbol,
        "Binance returned an unexpected symbol"
    );
    ensure!(order.side == side, "Binance returned an unexpected side");
    ensure!(
        order.client_order_id == client_order_id,
        "Binance returned an unexpected client order id"
    );
    ensure!(
        order.order_type == "MARKET",
        "Binance returned an unexpected order type"
    );
    ensure!(
        order.status == "FILLED",
        "Binance order was not fully filled"
    );
    ensure!(
        order.executed_qty > Decimal::ZERO,
        "Binance order executed no base quantity"
    );
    Ok(())
}

fn free_balance(state: &BinanceAccountState, asset: &str) -> Decimal {
    state
        .balance(asset)
        .map_or(Decimal::ZERO, |balance| balance.free)
}

fn unix_timestamp_ms() -> anyhow::Result<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_millis()
        .try_into()
        .context("timestamp overflow")
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
    let initialized_dex = initialize_dex(&config, domain_config.as_ref()).await?;
    let AlchemyDexStream {
        receiver: mut dex_receiver,
        task: mut dex_task,
    } = initialized_dex.stream;
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

    let (mut engine, hot_telemetry) = TradingEngine::new(
        config.clone(),
        Arc::clone(&domain_config),
        initialized_dex.mirror,
        telemetry,
    )?;
    let hot_telemetry_task = tokio::spawn(hot_telemetry.run());
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
        clickhouse_enabled = config.clickhouse_enabled(),
        "read-only arbitrage shadow service started with authenticated Binance account state"
    );

    let shutdown = tokio::signal::ctrl_c();
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
            event = dex_receiver.recv() => {
                let Some(event) = event else {
                    bail!("Alchemy DEX stream stopped; process restart will rehydrate state");
                };
                if let Some(request) = engine.on_dex_event(event)? {
                    prepared_sender
                        .try_send(request)
                        .context("prepared DEX builder queue is full or closed")?;
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
        }
    }

    engine.shutdown();
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
    tracing::info!("read-only arbitrage shadow service stopped");
    Ok(())
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
    Ok(InitializedDex { mirror, stream })
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

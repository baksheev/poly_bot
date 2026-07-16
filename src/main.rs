use std::{str::FromStr, sync::Arc, time::Duration};

use alloy_consensus::TxEip1559;
use alloy_primitives::{Address, Bytes, TxKind, U256};
use anyhow::{Context, bail, ensure};
use arb_bot::{
    across::{
        AcrossClient, AcrossQuoteRequest, NATIVE_ETH, OPTIMISM_CHAIN_ID, OPTIMISM_USDC,
        ValidatedNativeEthQuote, WORLD_CHAIN_CHAIN_ID, WORLD_CHAIN_USDC,
        validate_native_eth_deposit_status, validate_native_eth_quote, validate_quote,
    },
    balances::{
        BalanceEvent, BalanceSync, binance_snapshot, fetch_wallet_snapshot, spawn_balance_sync,
    },
    binance::account::{BinanceAccountClient, BinanceAccountState},
    binance::capital::{
        CapitalRouteState, NetworkInformation, TravelRuleWithdrawalRecord, WithdrawalRecord,
        select_capital_routes,
    },
    binance::ws_api::{BinanceWsApiClient, OrderResult, WsApiError},
    chain::rpc::{JsonRpcClient, TransactionCall, TransactionReceipt},
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
    opportunity::{PreparedPoolBuildRequest, PreparedPoolBuildResult, format_base_units},
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
        Command::AcrossUsdcQuote {
            origin_chain_id,
            amount,
        } => across_usdc_quote(&cli.config, origin_chain_id, amount).await,
        Command::AcrossManualEthToWorld { confirm_live } => {
            across_manual_eth_to_world(&cli.config, confirm_live).await
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

const RETAIN_OPTIMISM_BPS: u128 = 2_000;
const BASIS_POINTS: u128 = 10_000;
const GAS_LIMIT_MARGIN_NUMERATOR: u64 = 120;
const GAS_LIMIT_MARGIN_DENOMINATOR: u64 = 100;
const MAX_NATIVE_BRIDGE_GAS: u64 = 500_000;
const MAX_ORIGIN_GAS_COST_WEI: u128 = 100_000_000_000_000;

struct NativeBridgeGasPlan {
    gas_limit: u64,
    max_fee_per_gas: u128,
    max_priority_fee_per_gas: u128,
    maximum_cost: u128,
}

async fn across_manual_eth_to_world(
    config: &config::AppConfig,
    confirm_live: bool,
) -> anyhow::Result<()> {
    ensure!(
        confirm_live,
        "live Across native ETH bridge requires explicit --confirm-live"
    );
    let wallet = TestWallet::from_env()?;
    let owner = wallet.address();
    let optimism_endpoint = std::env::var(OPTIMISM_RPC_URL_ENV).with_context(|| {
        format!("required environment variable {OPTIMISM_RPC_URL_ENV} is not set")
    })?;
    let world_endpoint_name = "ALCHEMY_WORLDCHAIN_RPC_URL";
    let world_endpoint = std::env::var(world_endpoint_name).with_context(|| {
        format!("required environment variable {world_endpoint_name} is not set")
    })?;
    let optimism = JsonRpcClient::new(optimism_endpoint)?;
    let world = JsonRpcClient::new(world_endpoint)?;
    let (optimism_chain_id, world_chain_id) =
        tokio::try_join!(optimism.chain_id(), world.chain_id())?;
    ensure!(
        optimism_chain_id == OPTIMISM_CHAIN_ID,
        "Optimism RPC returned the wrong chain id"
    );
    ensure!(
        world_chain_id == WORLD_CHAIN_CHAIN_ID,
        "World Chain RPC returned the wrong chain id"
    );

    let (source_balance_u256, destination_balance_before, latest_nonce, pending_nonce, gas_price) =
        tokio::try_join!(
            optimism.native_balance(owner),
            world.native_balance(owner),
            optimism.latest_nonce(owner),
            optimism.pending_nonce(owner),
            optimism.gas_price(),
        )?;
    ensure!(
        latest_nonce == pending_nonce,
        "Optimism wallet has an unresolved pending transaction"
    );
    let source_balance = u128::try_from(source_balance_u256)
        .context("Optimism ETH balance exceeds the bridge canary representation")?;
    ensure!(
        destination_balance_before == U256::ZERO,
        "World Chain wallet is already funded; refusing to repeat the bootstrap bridge"
    );
    let retained = retained_optimism_balance(source_balance)?;
    let initial_amount = source_balance
        .checked_sub(retained)
        .context("Optimism ETH balance cannot cover the retained amount")?;
    ensure!(initial_amount > 0, "no Optimism ETH is available to bridge");

    let client = AcrossClient::new(config)?;
    let initial_request = native_eth_request(owner, initial_amount);
    let initial_quote = client.quote(&initial_request).await?;
    let initial_terms =
        validate_native_eth_quote(&initial_request, &initial_quote, source_balance)?;
    let initial_call = native_bridge_call(owner, &initial_terms);
    optimism.simulate_transaction(&initial_call).await?;
    let initial_estimate = optimism.estimate_gas(&initial_call).await?;
    let initial_gas = native_bridge_gas_plan(&initial_terms, initial_estimate, gas_price)?;
    let reserved_gas_cost = initial_gas
        .maximum_cost
        .checked_mul(2)
        .context("native ETH gas reserve overflow")?;
    ensure!(
        reserved_gas_cost <= MAX_ORIGIN_GAS_COST_WEI,
        "native ETH gas reserve exceeds the absolute canary cap"
    );

    let bridge_amount = initial_amount
        .checked_sub(reserved_gas_cost)
        .context("Optimism ETH balance cannot retain 20% plus maximum origin gas")?;
    ensure!(
        bridge_amount > 0,
        "native ETH bridge amount is zero after gas reserve"
    );
    let request = native_eth_request(owner, bridge_amount);
    let quote = client.quote(&request).await?;
    let terms = validate_native_eth_quote(&request, &quote, source_balance)?;
    let call = native_bridge_call(owner, &terms);
    optimism.simulate_transaction(&call).await?;
    let estimate = optimism.estimate_gas(&call).await?;
    let gas = native_bridge_gas_plan(&terms, estimate, gas_price)?;
    ensure!(
        gas.maximum_cost <= reserved_gas_cost,
        "fresh Across quote requires more gas than the doubled reserve"
    );
    let minimum_retained_after_max_gas = source_balance
        .checked_sub(bridge_amount)
        .and_then(|value| value.checked_sub(gas.maximum_cost))
        .context("native ETH bridge exceeds the observed Optimism balance")?;
    ensure!(
        minimum_retained_after_max_gas >= retained,
        "native ETH bridge would leave less than 20% on Optimism"
    );

    let transaction = TxEip1559 {
        chain_id: OPTIMISM_CHAIN_ID,
        nonce: pending_nonce,
        gas_limit: gas.gas_limit,
        max_fee_per_gas: gas.max_fee_per_gas,
        max_priority_fee_per_gas: gas.max_priority_fee_per_gas,
        to: TxKind::Call(terms.target),
        value: U256::from(bridge_amount),
        access_list: Default::default(),
        input: Bytes::from(terms.data.clone()),
    };
    let signed = wallet.sign_eip1559(transaction)?;
    tracing::info!(
        wallet = %owner,
        origin_chain_id = OPTIMISM_CHAIN_ID,
        destination_chain_id = WORLD_CHAIN_CHAIN_ID,
        source_balance = %format_base_units(source_balance_u256, 18),
        bridge_amount = %format_base_units(U256::from(bridge_amount), 18),
        retained_at_max_gas = %format_base_units(U256::from(minimum_retained_after_max_gas), 18),
        minimum_destination_amount = %format_base_units(U256::from(terms.minimum_output_amount), 18),
        route_fee = %format_base_units(U256::from(quote.fees.total.amount.parse::<u128>()?), 18),
        gas_limit = gas.gas_limit,
        max_fee_per_gas = gas.max_fee_per_gas,
        maximum_origin_gas_cost = %format_base_units(U256::from(gas.maximum_cost), 18),
        nonce = pending_nonce,
        transaction_hash = %signed.hash,
        "validated native ETH Across bridge ready for broadcast"
    );

    let submitted_hash = optimism.send_raw_transaction(&signed.raw).await?;
    ensure!(
        submitted_hash == signed.hash,
        "Optimism RPC returned a different transaction hash"
    );
    let receipt = wait_for_origin_receipt(&optimism, signed.hash).await?;
    ensure!(
        receipt.status == 1,
        "native ETH Across origin transaction reverted"
    );
    let filled = wait_for_across_fill(&client, signed.hash, terms.minimum_output_amount).await?;
    let destination_balance_after = world.native_balance(owner).await?;
    let destination_delta = destination_balance_after
        .checked_sub(destination_balance_before)
        .context("World Chain ETH balance decreased during Across canary")?;
    ensure!(
        destination_delta >= U256::from(terms.minimum_output_amount),
        "World Chain ETH balance increase is below Across minimum output"
    );
    let source_balance_after = optimism.native_balance(owner).await?;
    ensure!(
        source_balance_after >= U256::from(retained),
        "Optimism ETH retained balance fell below 20%"
    );
    let receipt_execution_gas_cost = u128::from(receipt.gas_used)
        .checked_mul(receipt.effective_gas_price)
        .context("origin gas cost overflow")?;
    let source_balance_after_u128 = u128::try_from(source_balance_after)
        .context("post-bridge Optimism balance exceeds u128")?;
    let actual_origin_gas_cost = source_balance
        .checked_sub(bridge_amount)
        .and_then(|value| value.checked_sub(source_balance_after_u128))
        .context("actual Optimism gas cost cannot be reconciled from balances")?;
    ensure!(
        actual_origin_gas_cost <= gas.maximum_cost,
        "actual Optimism gas cost exceeds the signed maximum"
    );
    tracing::info!(
        origin_transaction_hash = %signed.hash,
        destination_transaction_hash = %filled.fill_txn_ref.as_deref().unwrap_or_default(),
        origin_block = receipt.block_number,
        origin_gas_used = receipt.gas_used,
        receipt_execution_gas_cost = %format_base_units(U256::from(receipt_execution_gas_cost), 18),
        actual_origin_gas_cost = %format_base_units(U256::from(actual_origin_gas_cost), 18),
        optimism_balance_after = %format_base_units(source_balance_after, 18),
        world_balance_before = %format_base_units(destination_balance_before, 18),
        world_balance_after = %format_base_units(destination_balance_after, 18),
        world_balance_delta = %format_base_units(destination_delta, 18),
        "native ETH Across bridge completed and reconciled"
    );
    Ok(())
}

fn native_eth_request(owner: Address, amount: u128) -> AcrossQuoteRequest {
    AcrossQuoteRequest {
        origin_chain_id: OPTIMISM_CHAIN_ID,
        destination_chain_id: WORLD_CHAIN_CHAIN_ID,
        input_token: NATIVE_ETH,
        output_token: NATIVE_ETH,
        amount,
        depositor: owner,
        recipient: owner,
    }
}

fn native_bridge_call(owner: Address, terms: &ValidatedNativeEthQuote) -> TransactionCall {
    TransactionCall {
        from: owner,
        to: terms.target,
        data: terms.data.clone(),
        value: U256::from(terms.value),
    }
}

fn retained_optimism_balance(balance: u128) -> anyhow::Result<u128> {
    balance
        .checked_mul(RETAIN_OPTIMISM_BPS)
        .and_then(|value| value.checked_add(BASIS_POINTS - 1))
        .map(|value| value / BASIS_POINTS)
        .context("Optimism retained-balance calculation overflow")
}

fn native_bridge_gas_plan(
    terms: &ValidatedNativeEthQuote,
    rpc_estimate: u64,
    rpc_gas_price: u128,
) -> anyhow::Result<NativeBridgeGasPlan> {
    let estimated = terms.gas.max(rpc_estimate);
    let gas_limit = estimated
        .checked_mul(GAS_LIMIT_MARGIN_NUMERATOR)
        .and_then(|value| value.checked_add(GAS_LIMIT_MARGIN_DENOMINATOR - 1))
        .map(|value| value / GAS_LIMIT_MARGIN_DENOMINATOR)
        .context("native ETH gas-limit margin overflow")?;
    ensure!(
        gas_limit > 0 && gas_limit <= MAX_NATIVE_BRIDGE_GAS,
        "native ETH bridge gas limit exceeds the canary cap"
    );
    let max_fee_per_gas = terms
        .max_fee_per_gas
        .max(rpc_gas_price)
        .checked_mul(2)
        .context("native ETH max fee overflow")?;
    ensure!(
        max_fee_per_gas <= 100_000_000_000,
        "native ETH max fee exceeds the canary cap"
    );
    let max_priority_fee_per_gas = terms.max_priority_fee_per_gas;
    ensure!(
        max_priority_fee_per_gas <= max_fee_per_gas,
        "native ETH priority fee exceeds max fee"
    );
    let maximum_cost = u128::from(gas_limit)
        .checked_mul(max_fee_per_gas)
        .context("native ETH maximum gas cost overflow")?;
    ensure!(
        maximum_cost <= MAX_ORIGIN_GAS_COST_WEI,
        "native ETH origin gas cost exceeds the absolute canary cap"
    );
    Ok(NativeBridgeGasPlan {
        gas_limit,
        max_fee_per_gas,
        max_priority_fee_per_gas,
        maximum_cost,
    })
}

async fn wait_for_origin_receipt(
    rpc: &JsonRpcClient,
    transaction_hash: alloy_primitives::B256,
) -> anyhow::Result<TransactionReceipt> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
    loop {
        if let Some(receipt) = rpc.transaction_receipt(transaction_hash).await? {
            ensure!(
                receipt.transaction_hash == transaction_hash,
                "Optimism receipt transaction hash mismatch"
            );
            return Ok(receipt);
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for Optimism Across transaction receipt"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn wait_for_across_fill(
    client: &AcrossClient,
    transaction_hash: alloy_primitives::B256,
    minimum_output_amount: u128,
) -> anyhow::Result<arb_bot::across::AcrossDepositStatus> {
    let transaction_hash = format!("{transaction_hash:#x}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(600);
    loop {
        match client.deposit_status(&transaction_hash).await {
            Ok(status) => {
                if validate_native_eth_deposit_status(
                    &status,
                    &transaction_hash,
                    minimum_output_amount,
                )? {
                    return Ok(status);
                }
            }
            Err(error) => {
                tracing::warn!(error = %error, %transaction_hash, "Across fill status is not available yet");
            }
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for Across destination fill"
        );
        tokio::time::sleep(Duration::from_secs(5)).await;
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
    let wallet = TestWallet::from_env()?;
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
    let initial_binance_balances = binance_snapshot(&binance_account.account, &binance_assets, 0);
    let initial_wallet_balances = fetch_wallet_snapshot(
        &wallet_rpc,
        wallet_owner,
        wallet_chain_id,
        &wallet_tokens,
        initial_wallet_head,
    )
    .await?;
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
    )?;
    let hot_telemetry_task = tokio::spawn(hot_telemetry.run());
    engine.on_balance_event(BalanceEvent::Binance(initial_binance_balances));
    engine.on_balance_event(BalanceEvent::Wallet(initial_wallet_balances));
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
            event = balance_receiver.recv() => {
                let Some(event) = event else {
                    bail!("balance synchronization channel stopped unexpectedly");
                };
                engine.on_balance_event(event);
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

#[cfg(test)]
mod tests {
    use alloy_primitives::Address;

    use super::{native_bridge_gas_plan, retained_optimism_balance};
    use arb_bot::across::ValidatedNativeEthQuote;

    fn terms() -> ValidatedNativeEthQuote {
        ValidatedNativeEthQuote {
            target: Address::repeat_byte(0x11),
            data: vec![0x60, 0x9e, 0xa0, 0x81],
            value: 7_987_000_000_000_000,
            gas: 84_674,
            max_fee_per_gas: 1_000_536,
            max_priority_fee_per_gas: 1_000_000,
            minimum_output_amount: 7_982_000_000_000_000,
        }
    }

    #[test]
    fn retains_at_least_twenty_percent_with_rounding_up() {
        assert_eq!(
            retained_optimism_balance(9_985_000_000_000_000).unwrap(),
            1_997_000_000_000_000
        );
        assert_eq!(retained_optimism_balance(1).unwrap(), 1);
    }

    #[test]
    fn gas_plan_uses_larger_estimate_and_double_fee_headroom() {
        let plan = native_bridge_gas_plan(&terms(), 90_000, 1_000_400).unwrap();
        assert_eq!(plan.gas_limit, 108_000);
        assert_eq!(plan.max_fee_per_gas, 2_001_072);
        assert_eq!(plan.max_priority_fee_per_gas, 1_000_000);
        assert_eq!(plan.maximum_cost, 216_115_776_000);
    }

    #[test]
    fn gas_plan_rejects_excessive_estimate_or_fee() {
        assert!(native_bridge_gas_plan(&terms(), 500_000, 1_000_000).is_err());
        assert!(native_bridge_gas_plan(&terms(), 90_000, 100_000_000_000).is_err());
    }
}

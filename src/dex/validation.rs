use std::{path::PathBuf, str::FromStr, time::Duration};

use alloy_primitives::{Address, U256};
use anyhow::{Context, ensure};

use crate::{
    chain::rpc::{EthCall, JsonRpcClient},
    domain::config::{DexProvider, LoadedDomainConfig, PairConfig},
    wallet::{EvmWallet, WALLET_JOURNAL_PATH_ENV},
};

use super::{
    execution::{
        DexExecutionService, DexExecutor, ExactInputSwapRequest, SwapExecutionOutcome, SwapRoute,
        UniswapProtocol,
    },
    hydration::{DexHydrator, HydratedDexState, PoolIdentity},
    pool_id::V4PoolKey,
};

pub const LIVE_CONFIRMATION: &str = "I_UNDERSTAND_UNISWAP_LIVE_10_USDC";
const WORLD_CHAIN_ID: u64 = 480;
const USDC_DECIMALS: u8 = 6;
const MAX_USDC_BASE_UNITS: u64 = 10_000_000;
const MAX_SLIPPAGE_BPS: u16 = 50;
const DEADLINE_SECONDS: u64 = 10 * 60;
const BALANCE_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(30);
const BALANCE_POLL_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TokenBalances {
    pub usdc: U256,
    pub wld: U256,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoundTripOutcome {
    pub protocol: UniswapProtocol,
    pub wallet: Address,
    pub amount_usdc_in: U256,
    pub amount_wld_received: U256,
    pub amount_usdc_received: U256,
    pub buy: SwapExecutionOutcome,
    pub sell: SwapExecutionOutcome,
    pub before: TokenBalances,
    pub after: TokenBalances,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoverySellOutcome {
    pub protocol: UniswapProtocol,
    pub wallet: Address,
    pub amount_wld_in: U256,
    pub amount_usdc_received: U256,
    pub sell: SwapExecutionOutcome,
}

#[allow(clippy::too_many_arguments)]
pub async fn execute_round_trip(
    domain_config: &LoadedDomainConfig,
    protocol: UniswapProtocol,
    amount_usdc_base_units: u64,
    slippage_bps: u16,
    additional_gas: u64,
    confirmation_timeout: Duration,
    live_confirmation: &str,
) -> anyhow::Result<RoundTripOutcome> {
    ensure!(
        live_confirmation == LIVE_CONFIRMATION,
        "live Uniswap validation requires --live-confirmation {LIVE_CONFIRMATION}"
    );
    ensure!(
        amount_usdc_base_units > 0 && amount_usdc_base_units <= MAX_USDC_BASE_UNITS,
        "live validation amount must be between 1 base unit and 10 USDC"
    );
    ensure!(
        slippage_bps > 0 && slippage_bps <= MAX_SLIPPAGE_BPS,
        "live validation slippage must be between 1 and 50 bps"
    );
    ensure!(
        !confirmation_timeout.is_zero(),
        "confirmation timeout is zero"
    );

    let pair = only_pair(domain_config)?;
    validate_pair(pair, protocol)?;
    let token_usdc =
        Address::from_str(&pair.token_a.contract).context("configured USDC address is invalid")?;
    let token_wld =
        Address::from_str(&pair.token_b.contract).context("configured WLD address is invalid")?;
    let endpoint = std::env::var(&pair.chain.rpc_url_env).with_context(|| {
        format!(
            "required environment variable {} is not set",
            pair.chain.rpc_url_env
        )
    })?;
    let expected_wallet = std::env::var("EVM_WALLET_ADDRESS")
        .context("required environment variable EVM_WALLET_ADDRESS is not set")?
        .parse::<Address>()
        .context("EVM_WALLET_ADDRESS is invalid")?;
    let wallet = EvmWallet::from_env()?;
    ensure!(
        wallet.address() == expected_wallet,
        "EVM signer does not match the configured Rust wallet address"
    );
    let journal_path =
        PathBuf::from(std::env::var(WALLET_JOURNAL_PATH_ENV).with_context(|| {
            format!("required environment variable {WALLET_JOURNAL_PATH_ENV} is not set")
        })?);

    let read_rpc = JsonRpcClient::new(endpoint.clone())?;
    ensure!(
        read_rpc.chain_id().await? == WORLD_CHAIN_ID,
        "live validation RPC is not World Chain"
    );
    let latest_nonce = read_rpc.latest_nonce(expected_wallet).await?;
    let pending_nonce = read_rpc.pending_nonce(expected_wallet).await?;
    ensure!(
        latest_nonce == pending_nonce,
        "wallet has a pending transaction; refusing live validation"
    );
    let before = token_balances(&read_rpc, expected_wallet, token_usdc, token_wld).await?;
    let amount_usdc_in = U256::from(amount_usdc_base_units);
    ensure!(
        before.usdc >= amount_usdc_in,
        "wallet USDC balance is below the requested validation amount"
    );
    ensure!(
        !read_rpc.native_balance(expected_wallet).await?.is_zero(),
        "wallet has no native ETH for gas"
    );

    let initial_dex = DexHydrator::new(&read_rpc)
        .hydrate(domain_config.snapshot())
        .await?;
    let buy_route = select_route(
        &initial_dex,
        pair,
        protocol,
        token_usdc,
        token_wld,
        amount_usdc_in,
    )?;
    let buy_minimum = apply_slippage(buy_route.amount_out, slippage_bps)?;
    let run_id = format!("rustval-{}-{}", protocol.label(), unix_seconds()?);
    let executor = DexExecutor::hydrate(
        JsonRpcClient::new(endpoint)?,
        wallet,
        WORLD_CHAIN_ID,
        journal_path,
    )
    .await?;
    let service = DexExecutionService::spawn(executor, 1)?;
    let mut buy = ExactInputSwapRequest::with_rails_defaults(
        format!("{run_id}-buy"),
        buy_route.route,
        token_usdc,
        token_wld,
        amount_usdc_in,
        buy_minimum,
        unix_seconds()?.saturating_add(DEADLINE_SECONDS),
    );
    buy.additional_gas = additional_gas;
    buy.confirmation_timeout = confirmation_timeout;
    let buy_outcome = service.execute(buy).await?;

    let after_buy = wait_for_balances(
        &read_rpc,
        expected_wallet,
        token_usdc,
        token_wld,
        buy_outcome.block_number,
        |balances| balances.usdc < before.usdc && balances.wld > before.wld,
    )
    .await
    .context("buy receipt is visible but its wallet balance delta is not")?;
    let amount_wld_received = after_buy
        .wld
        .checked_sub(before.wld)
        .context("WLD balance delta underflow")?;

    // Rehydrate after the buy so the sell quote includes our own pool-state change.
    let sell_dex = DexHydrator::new(&read_rpc)
        .hydrate(domain_config.snapshot())
        .await?;
    let sell_route = select_route(
        &sell_dex,
        pair,
        protocol,
        token_wld,
        token_usdc,
        amount_wld_received,
    )?;
    let sell_minimum = apply_slippage(sell_route.amount_out, slippage_bps)?;
    let mut sell = ExactInputSwapRequest::with_rails_defaults(
        format!("{run_id}-sell"),
        sell_route.route,
        token_wld,
        token_usdc,
        amount_wld_received,
        sell_minimum,
        unix_seconds()?.saturating_add(DEADLINE_SECONDS),
    );
    sell.additional_gas = additional_gas;
    sell.confirmation_timeout = confirmation_timeout;
    let sell_outcome = service.execute(sell).await?;

    let after = wait_for_balances(
        &read_rpc,
        expected_wallet,
        token_usdc,
        token_wld,
        sell_outcome.block_number,
        |balances| balances.usdc > after_buy.usdc && balances.wld <= before.wld,
    )
    .await
    .context("sell receipt is visible but its wallet balance delta is not")?;
    let amount_usdc_received = after
        .usdc
        .checked_sub(after_buy.usdc)
        .context("USDC sell balance delta underflow")?;

    tracing::info!(
        protocol = protocol.label(),
        wallet = %expected_wallet,
        amount_usdc_in = %amount_usdc_in,
        amount_wld_received = %amount_wld_received,
        amount_usdc_received = %amount_usdc_received,
        buy_transaction_hash = %buy_outcome.transaction_hash,
        sell_transaction_hash = %sell_outcome.transaction_hash,
        buy_gas_used = buy_outcome.gas_used,
        sell_gas_used = sell_outcome.gas_used,
        "live Uniswap buy/sell validation completed"
    );
    Ok(RoundTripOutcome {
        protocol,
        wallet: expected_wallet,
        amount_usdc_in,
        amount_wld_received,
        amount_usdc_received,
        buy: buy_outcome,
        sell: sell_outcome,
        before,
        after,
    })
}

#[allow(clippy::too_many_arguments)]
pub async fn execute_recovery_sell(
    domain_config: &LoadedDomainConfig,
    protocol: UniswapProtocol,
    amount_wld_in: U256,
    slippage_bps: u16,
    additional_gas: u64,
    confirmation_timeout: Duration,
    live_confirmation: &str,
) -> anyhow::Result<RecoverySellOutcome> {
    ensure!(
        live_confirmation == LIVE_CONFIRMATION,
        "live Uniswap recovery requires --live-confirmation {LIVE_CONFIRMATION}"
    );
    ensure!(!amount_wld_in.is_zero(), "recovery WLD amount is zero");
    ensure!(
        slippage_bps > 0 && slippage_bps <= MAX_SLIPPAGE_BPS,
        "live validation slippage must be between 1 and 50 bps"
    );
    ensure!(
        !confirmation_timeout.is_zero(),
        "confirmation timeout is zero"
    );

    let pair = only_pair(domain_config)?;
    validate_pair(pair, protocol)?;
    let token_usdc =
        Address::from_str(&pair.token_a.contract).context("configured USDC address is invalid")?;
    let token_wld =
        Address::from_str(&pair.token_b.contract).context("configured WLD address is invalid")?;
    let endpoint = std::env::var(&pair.chain.rpc_url_env).with_context(|| {
        format!(
            "required environment variable {} is not set",
            pair.chain.rpc_url_env
        )
    })?;
    let expected_wallet = std::env::var("EVM_WALLET_ADDRESS")
        .context("required environment variable EVM_WALLET_ADDRESS is not set")?
        .parse::<Address>()
        .context("EVM_WALLET_ADDRESS is invalid")?;
    let wallet = EvmWallet::from_env()?;
    ensure!(
        wallet.address() == expected_wallet,
        "EVM signer does not match the configured Rust wallet address"
    );
    let journal_path =
        PathBuf::from(std::env::var(WALLET_JOURNAL_PATH_ENV).with_context(|| {
            format!("required environment variable {WALLET_JOURNAL_PATH_ENV} is not set")
        })?);
    let read_rpc = JsonRpcClient::new(endpoint.clone())?;
    ensure!(
        read_rpc.chain_id().await? == WORLD_CHAIN_ID,
        "live recovery RPC is not World Chain"
    );
    ensure!(
        read_rpc.latest_nonce(expected_wallet).await?
            == read_rpc.pending_nonce(expected_wallet).await?,
        "wallet has a pending transaction; refusing live recovery"
    );
    let before = token_balances(&read_rpc, expected_wallet, token_usdc, token_wld).await?;
    ensure!(
        before.wld >= amount_wld_in,
        "wallet WLD balance is below the recovery amount"
    );
    ensure!(
        !read_rpc.native_balance(expected_wallet).await?.is_zero(),
        "wallet has no native ETH for recovery gas"
    );

    let dex = DexHydrator::new(&read_rpc)
        .hydrate(domain_config.snapshot())
        .await?;
    let selected = select_route(&dex, pair, protocol, token_wld, token_usdc, amount_wld_in)?;
    ensure!(
        selected.amount_out <= U256::from(MAX_USDC_BASE_UNITS),
        "recovery sell quote exceeds the authorized 10 USDC envelope"
    );
    let minimum = apply_slippage(selected.amount_out, slippage_bps)?;
    let executor = DexExecutor::hydrate(
        JsonRpcClient::new(endpoint)?,
        wallet,
        WORLD_CHAIN_ID,
        journal_path,
    )
    .await?;
    let service = DexExecutionService::spawn(executor, 1)?;
    let mut request = ExactInputSwapRequest::with_rails_defaults(
        format!(
            "rustval-recovery-{}-{}-sell",
            protocol.label(),
            unix_seconds()?
        ),
        selected.route,
        token_wld,
        token_usdc,
        amount_wld_in,
        minimum,
        unix_seconds()?.saturating_add(DEADLINE_SECONDS),
    );
    request.additional_gas = additional_gas;
    request.confirmation_timeout = confirmation_timeout;
    let sell = service.execute(request).await?;
    let after = wait_for_balances(
        &read_rpc,
        expected_wallet,
        token_usdc,
        token_wld,
        sell.block_number,
        |balances| balances.usdc > before.usdc && balances.wld < before.wld,
    )
    .await
    .context("recovery sell receipt is visible but its wallet balance delta is not")?;
    let amount_usdc_received = after
        .usdc
        .checked_sub(before.usdc)
        .context("recovery USDC balance delta underflow")?;
    ensure!(
        amount_usdc_received <= U256::from(MAX_USDC_BASE_UNITS),
        "recovery received more than the authorized 10 USDC envelope"
    );
    Ok(RecoverySellOutcome {
        protocol,
        wallet: expected_wallet,
        amount_wld_in,
        amount_usdc_received,
        sell,
    })
}

struct SelectedRoute {
    route: SwapRoute,
    amount_out: U256,
}

fn select_route(
    state: &HydratedDexState,
    pair: &PairConfig,
    protocol: UniswapProtocol,
    token_in: Address,
    token_out: Address,
    amount_in: U256,
) -> anyhow::Result<SelectedRoute> {
    let mut best: Option<SelectedRoute> = None;
    for candidate in &state.pools {
        if candidate.pair_id != pair.id
            || !((candidate.token0 == token_in && candidate.token1 == token_out)
                || (candidate.token0 == token_out && candidate.token1 == token_in))
        {
            continue;
        }
        let zero_for_one = token_in == candidate.token0;
        let amount_out = match candidate
            .pool
            .quote_exact_in_amount_out(zero_for_one, amount_in)
        {
            Ok(amount_out) if !amount_out.is_zero() => amount_out,
            _ => continue,
        };
        let route = match (protocol, candidate.identity) {
            (UniswapProtocol::V3, PoolIdentity::V3 { address, fee_pips }) => SwapRoute::V3 {
                router: configured_address(
                    "uniswap_v3_router_address",
                    pair.chain.uniswap_v3_router_address.as_deref(),
                )?,
                pool: address,
                fee_pips,
            },
            (UniswapProtocol::V4, PoolIdentity::V4 { pool_id, fee_pips }) => {
                let pool_key = pair
                    .dex
                    .uniswap_v4
                    .as_ref()
                    .context("missing Uniswap V4 config")?
                    .pools
                    .iter()
                    .filter_map(|pool| {
                        let hooks = Address::from_str(&pool.hooks).ok()?;
                        V4PoolKey::new(
                            candidate.token0,
                            candidate.token1,
                            pool.fee_tier,
                            pool.tick_spacing,
                            hooks,
                        )
                        .ok()
                    })
                    .find(|key| key.pool_id() == pool_id)
                    .context("hydrated V4 pool id is absent from domain config")?;
                ensure!(
                    pool_key.fee_pips == fee_pips
                        && pool_key.tick_spacing == candidate.pool.tick_spacing,
                    "hydrated V4 pool metadata differs from its domain pool key"
                );
                SwapRoute::V4 {
                    router: configured_address(
                        "uniswap_v4_router_address",
                        pair.chain.uniswap_v4_router_address.as_deref(),
                    )?,
                    pool_key,
                }
            }
            _ => continue,
        };
        if best
            .as_ref()
            .is_none_or(|selected| amount_out > selected.amount_out)
        {
            best = Some(SelectedRoute { route, amount_out });
        }
    }
    best.with_context(|| {
        format!(
            "no liquid {} route for exact input {}",
            protocol.label(),
            amount_in
        )
    })
}

async fn token_balances(
    rpc: &JsonRpcClient,
    owner: Address,
    usdc: Address,
    wld: Address,
) -> anyhow::Result<TokenBalances> {
    let block = rpc.latest_block().await?;
    token_balances_at(rpc, owner, usdc, wld, block).await
}

async fn token_balances_at(
    rpc: &JsonRpcClient,
    owner: Address,
    usdc: Address,
    wld: Address,
    block: crate::chain::rpc::CanonicalBlock,
) -> anyhow::Result<TokenBalances> {
    let calls = [
        EthCall {
            to: usdc,
            data: balance_of_calldata(owner),
        },
        EthCall {
            to: wld,
            data: balance_of_calldata(owner),
        },
    ];
    let values = rpc.eth_call_batch(&calls, block).await?;
    ensure!(
        values.len() == 2 && values.iter().all(|value| value.len() == 32),
        "canonical token balance response is invalid"
    );
    let usdc = U256::from_be_slice(&values[0]);
    let wld = U256::from_be_slice(&values[1]);
    Ok(TokenBalances { usdc, wld })
}

async fn wait_for_balances(
    rpc: &JsonRpcClient,
    owner: Address,
    usdc: Address,
    wld: Address,
    minimum_block: u64,
    predicate: impl Fn(TokenBalances) -> bool,
) -> anyhow::Result<TokenBalances> {
    let deadline = tokio::time::Instant::now() + BALANCE_VISIBILITY_TIMEOUT;
    loop {
        let block = rpc.latest_block().await?;
        if block.number >= minimum_block {
            let balances = token_balances_at(rpc, owner, usdc, wld, block).await?;
            if predicate(balances) {
                return Ok(balances);
            }
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for canonical wallet balance delta after block {minimum_block}"
        );
        tokio::time::sleep(BALANCE_POLL_INTERVAL).await;
    }
}

fn balance_of_calldata(owner: Address) -> Vec<u8> {
    let mut data = Vec::with_capacity(36);
    data.extend_from_slice(&[0x70, 0xa0, 0x82, 0x31]);
    data.extend_from_slice(&[0_u8; 12]);
    data.extend_from_slice(owner.as_slice());
    data
}

fn apply_slippage(amount: U256, slippage_bps: u16) -> anyhow::Result<U256> {
    let minimum = amount
        .checked_mul(U256::from(10_000_u64 - u64::from(slippage_bps)))
        .and_then(|value| value.checked_div(U256::from(10_000_u64)))
        .context("slippage-adjusted output overflow")?;
    ensure!(!minimum.is_zero(), "slippage-adjusted output is zero");
    Ok(minimum)
}

fn only_pair(domain_config: &LoadedDomainConfig) -> anyhow::Result<&PairConfig> {
    ensure!(
        domain_config.snapshot().pairs.len() == 1,
        "live Uniswap validation requires exactly one configured pair"
    );
    Ok(&domain_config.snapshot().pairs[0])
}

fn validate_pair(pair: &PairConfig, protocol: UniswapProtocol) -> anyhow::Result<()> {
    ensure!(
        pair.chain.chain_id == WORLD_CHAIN_ID,
        "validation pair is not on World Chain"
    );
    ensure!(
        pair.token_a.symbol == "USDC",
        "validation token_a is not USDC"
    );
    ensure!(
        pair.token_a.decimals == USDC_DECIMALS,
        "validation USDC decimals are not six"
    );
    ensure!(
        pair.token_b.symbol == "WLD",
        "validation token_b is not WLD"
    );
    let provider = match protocol {
        UniswapProtocol::V3 => DexProvider::UniswapV3,
        UniswapProtocol::V4 => DexProvider::UniswapV4,
    };
    ensure!(
        pair.dex.allowed_providers.contains(&provider),
        "requested Uniswap protocol is not allowed by domain config"
    );
    Ok(())
}

fn configured_address(name: &str, address: Option<&str>) -> anyhow::Result<Address> {
    Address::from_str(address.with_context(|| format!("missing {name}"))?)
        .with_context(|| format!("invalid {name}"))
}

fn unix_seconds() -> anyhow::Result<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before Unix epoch")
        .map(|duration| duration.as_secs())
}

#[cfg(test)]
mod tests {
    use alloy_primitives::U256;

    use super::apply_slippage;

    #[test]
    fn slippage_rounds_minimum_output_down() {
        assert_eq!(
            apply_slippage(U256::from(1_000_001_u64), 5).unwrap(),
            U256::from(999_500_u64)
        );
    }
}

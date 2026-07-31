use std::{str::FromStr, time::Duration};

use alloy_primitives::{Address, U256};
use anyhow::{Context, ensure};
use rust_decimal::Decimal;

use crate::{
    balances::{WalletBalanceSnapshot, fetch_wallet_snapshot_coordinated},
    binance::{
        account::BinanceSymbolState,
        capital::{CoinInformation, select_capital_routes},
        execution::{BinanceOrderRequest, BinanceOrderRequestKind},
        order_plan::{
            MarketOrderPlan, base_units_from_decimal, decimal_from_base_units, plan_limit_ioc,
            plan_market_order,
        },
    },
    chain::rpc::{CanonicalBlock, JsonRpcClient},
    domain::config::PairConfig,
    network_runtime::{NetworkReadCoordinator, NetworkRuntime},
    rebalance::route_candidates_from_capital,
    wallet::TokenBalanceRequest,
};

pub const ARBITRUM_CHAIN_ID: u64 = 42_161;
pub const ARBITRUM_SWAP_ROUTER_02: &str = "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45";
pub const ARBITRUM_USDC: &str = "0xaf88d065e77c8cc2239327c5edb3a432268e5831";
pub const ARBITRUM_ESP: &str = "0x3b8db18e69d6686ad9371a423afe3dd1065c94f1";
pub const CHAIN_READINESS_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceReadiness {
    pub symbol: String,
    pub buy_fee_bps: u16,
    pub sell_fee_bps: u16,
    pub validation_price: Decimal,
    pub validation_quantity: Decimal,
    pub request_fingerprints: Vec<String>,
    pub filters_ready: bool,
    pub external_mutation_authorized: bool,
}

pub fn validate_binance_readiness(
    pair: &PairConfig,
    state: &BinanceSymbolState,
) -> anyhow::Result<BinanceReadiness> {
    validate_readiness_pair(pair)?;
    let rules = &state.symbol_rules;
    ensure!(
        rules.symbol == pair.binance.symbol
            && rules.base_asset == pair.binance.base_asset
            && rules.quote_asset == pair.binance.quote_asset
            && rules.status == "TRADING",
        "live Binance ESPUSDC identity or status differs from the readiness artifact"
    );
    ensure!(
        rules.lot_size.step == Decimal::from_str(&pair.binance.step_size)?
            && rules.price.step == Decimal::from_str(&pair.binance.tick_size)?,
        "live Binance ESPUSDC increments differ from the readiness artifact"
    );
    let maximum_notional_base_units = pair
        .adaptive_sizing
        .limits()
        .context("full-live ESP sizing limits are missing")?
        .max_trade_notional;
    let maximum_notional = decimal_from_base_units(
        U256::from_str_radix(maximum_notional_base_units, 10)?
            .try_into()
            .context("maximum trade notional exceeds u128")?,
        pair.token_a.decimals,
    )?;
    ensure!(
        rules.min_notional > Decimal::ZERO && rules.min_notional <= maximum_notional,
        "Binance minimum notional exceeds the ESP trade envelope"
    );

    let validation_price = aligned_validation_price(rules)?;
    let quantity = round_up(
        (rules.min_notional / validation_price)
            .max(rules.lot_size.min)
            .max(rules.market_lot_size.min),
        rules.lot_size.step,
    )?;
    ensure!(
        quantity > Decimal::ZERO
            && quantity * validation_price <= maximum_notional
            && quantity <= rules.lot_size.max,
        "no bounded ESP quantity satisfies the live Binance filters"
    );
    let absolute_base_units = base_units_from_decimal(quantity, pair.token_b.decimals)?;
    let absolute_base_units =
        i128::try_from(absolute_base_units).context("ESP validation quantity exceeds i128")?;

    let buy_ioc = plan_limit_ioc(
        "rustarb-esp-readiness-buy-ioc".to_owned(),
        "rustarbespreadbuy".to_owned(),
        absolute_base_units,
        pair.token_b.decimals,
        validation_price,
        rules,
    )?
    .context("bounded ESP BUY IOC rounded to dust")?;
    let sell_ioc = plan_limit_ioc(
        "rustarb-esp-readiness-sell-ioc".to_owned(),
        "rustarbespreadsell".to_owned(),
        -absolute_base_units,
        pair.token_b.decimals,
        validation_price,
        rules,
    )?
    .context("bounded ESP SELL IOC rounded to dust")?;
    let buy_market = submitted_market(
        plan_market_order(
            "rustarb-esp-readiness-buy-recovery".to_owned(),
            "rustarbespreadbuyr1".to_owned(),
            absolute_base_units,
            pair.token_b.decimals,
            validation_price,
            rules,
        )?,
        "BUY",
    )?;
    let sell_market = submitted_market(
        plan_market_order(
            "rustarb-esp-readiness-sell-recovery".to_owned(),
            "rustarbespreadsellr1".to_owned(),
            -absolute_base_units,
            pair.token_b.decimals,
            validation_price,
            rules,
        )?,
        "SELL",
    )?;
    let requests = [buy_ioc.request, sell_ioc.request, buy_market, sell_market];
    let request_fingerprints = requests
        .iter()
        .map(request_fingerprint)
        .collect::<anyhow::Result<Vec<_>>>()?;
    ensure!(
        request_fingerprints.len() == 4,
        "Binance request matrix is incomplete"
    );

    Ok(BinanceReadiness {
        symbol: rules.symbol.clone(),
        buy_fee_bps: state.commission.conservative_taker_fee_bps("BUY")?,
        sell_fee_bps: state.commission.conservative_taker_fee_bps("SELL")?,
        validation_price,
        validation_quantity: quantity,
        request_fingerprints,
        filters_ready: true,
        external_mutation_authorized: pair.execution_enabled,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainReadiness {
    pub chain_id: u64,
    pub block_number: u64,
    pub exact_token_contracts: bool,
    pub token_code_present: bool,
    pub router_code_present: bool,
    pub native_gas_funded: bool,
    pub token_a_funded: bool,
    pub token_b_funded: bool,
    pub fresh_rpc_gas_price: bool,
    pub allowance_policy: &'static str,
    pub receipt_l1_fee_mode: &'static str,
    pub external_mutation_authorized: bool,
    pub ready: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChainReadinessStatus {
    Observed {
        exact_token_contracts: bool,
        token_code_present: bool,
        router_code_present: bool,
        native_gas_funded: bool,
        token_a_funded: bool,
        token_b_funded: bool,
        fresh_rpc_gas_price: bool,
        ready: bool,
    },
    ProbeFailed,
}

impl ChainReadiness {
    pub const fn status(&self) -> ChainReadinessStatus {
        ChainReadinessStatus::Observed {
            exact_token_contracts: self.exact_token_contracts,
            token_code_present: self.token_code_present,
            router_code_present: self.router_code_present,
            native_gas_funded: self.native_gas_funded,
            token_a_funded: self.token_a_funded,
            token_b_funded: self.token_b_funded,
            fresh_rpc_gas_price: self.fresh_rpc_gas_price,
            ready: self.ready,
        }
    }
}

#[derive(Clone)]
pub struct ChainReadinessProbe {
    pair: PairConfig,
    reads: NetworkReadCoordinator,
    owner: Address,
}

impl ChainReadinessProbe {
    pub fn new(
        pair: &PairConfig,
        runtime: &NetworkRuntime,
        owner: Address,
    ) -> anyhow::Result<Self> {
        validate_readiness_pair(pair)?;
        ensure!(
            runtime.plan().chain_id == ARBITRUM_CHAIN_ID,
            "chain-readiness probe requires the Arbitrum runtime"
        );
        Ok(Self {
            pair: pair.clone(),
            reads: runtime.reads().clone(),
            owner,
        })
    }

    pub async fn inspect(&self) -> anyhow::Result<ChainReadiness> {
        let block = self.reads.rpc().latest_block().await?;
        let tokens = [
            (&self.pair.token_a.symbol, &self.pair.token_a.contract),
            (&self.pair.token_b.symbol, &self.pair.token_b.contract),
        ]
        .into_iter()
        .map(|(symbol, contract)| {
            Ok(TokenBalanceRequest {
                symbol: symbol.clone(),
                contract: contract
                    .parse()
                    .with_context(|| format!("token {symbol} has an invalid contract"))?,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
        let snapshot = fetch_wallet_snapshot_coordinated(
            &self.reads,
            self.owner,
            ARBITRUM_CHAIN_ID,
            &tokens,
            block,
        )
        .await?;
        inspect_chain_readiness_at(&self.pair, self.reads.rpc(), block, &snapshot).await
    }
}

pub async fn inspect_chain_readiness(
    pair: &PairConfig,
    runtime: &NetworkRuntime,
    snapshot: &WalletBalanceSnapshot,
) -> anyhow::Result<ChainReadiness> {
    inspect_chain_readiness_at(pair, runtime.rpc(), runtime.initial_head(), snapshot).await
}

async fn inspect_chain_readiness_at(
    pair: &PairConfig,
    rpc: &JsonRpcClient,
    block: CanonicalBlock,
    snapshot: &WalletBalanceSnapshot,
) -> anyhow::Result<ChainReadiness> {
    validate_readiness_pair(pair)?;
    ensure!(
        snapshot.chain_id == ARBITRUM_CHAIN_ID && snapshot.batch_complete,
        "chain readiness requires one complete Arbitrum wallet batch"
    );
    let token_a = Address::from_str(&pair.token_a.contract)?;
    let token_b = Address::from_str(&pair.token_b.contract)?;
    let router = Address::from_str(
        pair.chain
            .uniswap_v3_router_address
            .as_deref()
            .context("Arbitrum router is missing")?,
    )?;
    let exact_token_contracts = snapshot.token_balances.iter().any(|balance| {
        balance.symbol.as_ref() == pair.token_a.symbol && balance.contract == token_a
    }) && snapshot.token_balances.iter().any(|balance| {
        balance.symbol.as_ref() == pair.token_b.symbol && balance.contract == token_b
    });
    ensure!(
        block.number == snapshot.block_number && block.hash == snapshot.block_hash,
        "wallet and contract-code reads are not pinned to the same block"
    );
    let (token_a_code, token_b_code, router_code, native_balance, gas_price) = tokio::try_join!(
        rpc.contract_code_at(token_a, block),
        rpc.contract_code_at(token_b, block),
        rpc.contract_code_at(router, block),
        rpc.native_balance_at(snapshot.owner, block),
        rpc.gas_price(),
    )?;
    let token_code_present = !token_a_code.is_empty() && !token_b_code.is_empty();
    let router_code_present = !router_code.is_empty();
    let native_gas_funded = !native_balance.is_zero();
    let token_a_balance = snapshot.token_balances.iter().find_map(|balance| {
        (balance.symbol.as_ref() == pair.token_a.symbol && balance.contract == token_a)
            .then_some(balance.base_units)
    });
    let token_b_balance = snapshot.token_balances.iter().find_map(|balance| {
        (balance.symbol.as_ref() == pair.token_b.symbol && balance.contract == token_b)
            .then_some(balance.base_units)
    });
    let token_a_funded = token_a_balance.is_some_and(|balance| !balance.is_zero());
    let token_b_funded = token_b_balance.is_some_and(|balance| !balance.is_zero());
    let fresh_rpc_gas_price = gas_price > 0;
    let ready = exact_token_contracts
        && token_code_present
        && router_code_present
        && token_a_funded
        && token_b_funded
        && fresh_rpc_gas_price;
    Ok(ChainReadiness {
        chain_id: ARBITRUM_CHAIN_ID,
        block_number: block.number,
        exact_token_contracts,
        token_code_present,
        router_code_present,
        native_gas_funded,
        token_a_funded,
        token_b_funded,
        fresh_rpc_gas_price,
        allowance_policy: "max_uint256_then_locked",
        receipt_l1_fee_mode: "included_in_effective_gas_price_no_world_l1fee_addition",
        external_mutation_authorized: pair.execution_enabled,
        ready,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebalanceReadiness {
    pub network: String,
    pub asset_count: usize,
    pub direct_route_count: usize,
    pub deposit_enabled_assets: usize,
    pub withdrawal_enabled_assets: usize,
    pub external_mutation_authorized: bool,
    pub ready: bool,
}

pub fn validate_rebalance_readiness(
    pair: &PairConfig,
    coins: &[CoinInformation],
) -> anyhow::Result<RebalanceReadiness> {
    validate_readiness_pair(pair)?;
    let mut direct_route_count = 0;
    let mut deposit_enabled_assets = 0;
    let mut withdrawal_enabled_assets = 0;
    for token in [&pair.token_a, &pair.token_b] {
        let capital = select_capital_routes(
            coins,
            &token.symbol,
            &pair.chain.binance_network_name,
            "OPTIMISM",
        )?;
        let routes = route_candidates_from_capital(&capital, token.decimals, pair.chain.chain_id)?;
        let direct = routes
            .iter()
            .find(|route| matches!(route.route, crate::rebalance::Route::Direct { .. }))
            .with_context(|| format!("{} has no direct Arbitrum rebalance route", token.symbol))?;
        direct_route_count += 1;
        deposit_enabled_assets += usize::from(direct.binance_deposit_enabled);
        withdrawal_enabled_assets += usize::from(direct.binance_withdrawal_enabled);
    }
    Ok(RebalanceReadiness {
        network: pair.chain.binance_network_name.clone(),
        asset_count: 2,
        direct_route_count,
        deposit_enabled_assets,
        withdrawal_enabled_assets,
        external_mutation_authorized: pair.rebalance.enabled,
        ready: direct_route_count == 2,
    })
}

fn validate_readiness_pair(pair: &PairConfig) -> anyhow::Result<()> {
    let policy = pair
        .full_live_policy
        .as_ref()
        .context("Arbitrum full-live pair has no production policy")?;
    ensure!(
        pair.id == "arbitrum-usdc-esp"
            && pair.chain.chain_id == ARBITRUM_CHAIN_ID
            && pair
                .chain
                .uniswap_v3_router_address
                .as_deref()
                .is_some_and(|value| value.eq_ignore_ascii_case(ARBITRUM_SWAP_ROUTER_02))
            && pair.token_a.contract.eq_ignore_ascii_case(ARBITRUM_USDC)
            && pair.token_b.contract.eq_ignore_ascii_case(ARBITRUM_ESP)
            && pair.full_live
            && pair.execution_enabled
            && pair.rebalance.enabled
            && policy.rebalance_binance_network == "ARBITRUM"
            && policy.direct_route_only
            && !policy.bridge_mutations_enabled,
        "Arbitrum pair identity or rebalance policy differs from the production artifact"
    );
    Ok(())
}

fn aligned_validation_price(
    rules: &crate::binance::account::SymbolRules,
) -> anyhow::Result<Decimal> {
    let candidate = Decimal::ONE.max(rules.price.min);
    let price = round_up(candidate, rules.price.step)?;
    ensure!(
        price >= rules.price.min && price <= rules.price.max,
        "no deterministic validation price satisfies PRICE_FILTER"
    );
    Ok(price)
}

fn round_up(value: Decimal, increment: Decimal) -> anyhow::Result<Decimal> {
    ensure!(
        increment > Decimal::ZERO,
        "Binance increment is non-positive"
    );
    Ok((value / increment).ceil() * increment)
}

fn submitted_market(plan: MarketOrderPlan, side: &str) -> anyhow::Result<BinanceOrderRequest> {
    match plan {
        MarketOrderPlan::Submit(plan) => Ok(plan.request),
        MarketOrderPlan::ResidualDust(dust) => anyhow::bail!(
            "bounded ESP {side} MARKET recovery became {}",
            dust.reason.as_str()
        ),
    }
}

fn request_fingerprint(request: &BinanceOrderRequest) -> anyhow::Result<String> {
    let shape = match &request.kind {
        BinanceOrderRequestKind::LimitIoc {
            side,
            quantity,
            price,
        } => format!("limit_ioc:{side}:{quantity}:{price}"),
        BinanceOrderRequestKind::MarketBuyQuantity { quantity } => {
            format!("market_buy_quantity:{quantity}")
        }
        BinanceOrderRequestKind::MarketSell { quantity } => format!("market_sell:{quantity}"),
        _ => anyhow::bail!("request matrix contains an unreviewed Binance order shape"),
    };
    Ok(format!(
        "{}:{}:{}:{shape}",
        request.operation_id, request.client_order_id, request.symbol
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InjectedFailure {
    DexRevert,
    DexUnknownBroadcast,
    BinanceRejection,
    BinancePartialIoc,
    BinanceUnknownPlacement,
    BinanceZeroFillRecovery,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FailureDisposition {
    pub exposure_known: bool,
    pub retry_authorized: bool,
    pub maximum_market_attempts: u8,
}

pub const fn injected_failure_disposition(failure: InjectedFailure) -> FailureDisposition {
    match failure {
        InjectedFailure::DexRevert | InjectedFailure::BinanceRejection => FailureDisposition {
            exposure_known: true,
            retry_authorized: false,
            maximum_market_attempts: 0,
        },
        InjectedFailure::DexUnknownBroadcast | InjectedFailure::BinanceUnknownPlacement => {
            FailureDisposition {
                exposure_known: false,
                retry_authorized: false,
                maximum_market_attempts: 0,
            }
        }
        InjectedFailure::BinancePartialIoc | InjectedFailure::BinanceZeroFillRecovery => {
            FailureDisposition {
                exposure_known: true,
                retry_authorized: true,
                maximum_market_attempts: 3,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rust_decimal::Decimal;

    use crate::{
        binance::{
            account::BinanceSymbolState,
            account::{
                CommissionDiscount, CommissionRates, CommissionSideRates, DecimalFilter,
                SymbolRules,
            },
        },
        domain::config::LoadedDomainConfig,
    };

    use super::{
        CHAIN_READINESS_REFRESH_INTERVAL, ChainReadiness, ChainReadinessStatus, InjectedFailure,
        injected_failure_disposition, validate_binance_readiness, validate_readiness_pair,
    };

    fn rates() -> CommissionSideRates {
        CommissionSideRates {
            maker: Decimal::ZERO,
            taker: Decimal::new(1, 3),
            buyer: Decimal::ZERO,
            seller: Decimal::ZERO,
        }
    }

    fn state() -> BinanceSymbolState {
        BinanceSymbolState {
            commission: CommissionRates {
                symbol: "ESPUSDC".to_owned(),
                standard_commission: rates(),
                special_commission: rates_zero(),
                tax_commission: rates_zero(),
                discount: CommissionDiscount {
                    enabled_for_account: true,
                    enabled_for_symbol: true,
                    discount_asset: "BNB".to_owned(),
                    discount: Decimal::new(25, 2),
                },
            },
            symbol_rules: SymbolRules {
                symbol: "ESPUSDC".to_owned(),
                status: "TRADING".to_owned(),
                base_asset: "ESP".to_owned(),
                quote_asset: "USDC".to_owned(),
                price: DecimalFilter {
                    min: Decimal::new(1, 5),
                    max: Decimal::from(1_000),
                    step: Decimal::new(1, 5),
                },
                lot_size: DecimalFilter {
                    min: Decimal::ONE,
                    max: Decimal::from(100_000_000),
                    step: Decimal::ONE,
                },
                market_lot_size: DecimalFilter {
                    min: Decimal::ZERO,
                    max: Decimal::from(10_000_000),
                    step: Decimal::ZERO,
                },
                min_notional: Decimal::from(5),
                max_num_orders: 200,
                max_num_algo_orders: 5,
            },
            open_orders: Vec::new(),
        }
    }

    fn rates_zero() -> CommissionSideRates {
        CommissionSideRates {
            maker: Decimal::ZERO,
            taker: Decimal::ZERO,
            buyer: Decimal::ZERO,
            seller: Decimal::ZERO,
        }
    }

    #[test]
    fn esp_binance_primary_and_recovery_matrix_is_deterministic() {
        let domain =
            LoadedDomainConfig::load("config/strategies/usdc-esp-arbitrum.v6.json").unwrap();
        let first = validate_binance_readiness(&domain.snapshot().pairs[0], &state()).unwrap();
        let second = validate_binance_readiness(&domain.snapshot().pairs[0], &state()).unwrap();

        assert_eq!(first, second);
        assert!(first.filters_ready);
        assert!(first.external_mutation_authorized);
        assert_eq!(first.request_fingerprints.len(), 4);
        assert!(first.request_fingerprints[0].contains("limit_ioc:BUY"));
        assert!(first.request_fingerprints[1].contains("limit_ioc:SELL"));
        assert!(first.request_fingerprints[2].contains("market_buy_quantity"));
        assert!(first.request_fingerprints[3].contains("market_sell"));
    }

    #[test]
    fn failure_injection_matrix_never_retries_an_unknown_outcome() {
        for failure in [
            InjectedFailure::DexUnknownBroadcast,
            InjectedFailure::BinanceUnknownPlacement,
        ] {
            let disposition = injected_failure_disposition(failure);
            assert!(!disposition.exposure_known);
            assert!(!disposition.retry_authorized);
            assert_eq!(disposition.maximum_market_attempts, 0);
        }
        for failure in [
            InjectedFailure::BinancePartialIoc,
            InjectedFailure::BinanceZeroFillRecovery,
        ] {
            let disposition = injected_failure_disposition(failure);
            assert!(disposition.exposure_known);
            assert!(disposition.retry_authorized);
            assert_eq!(disposition.maximum_market_attempts, 3);
        }
        assert!(!injected_failure_disposition(InjectedFailure::DexRevert).retry_authorized);
        assert!(!injected_failure_disposition(InjectedFailure::BinanceRejection).retry_authorized);
    }

    #[test]
    fn chain_readiness_transition_ignores_block_height_and_detects_funding() {
        let readiness = ChainReadiness {
            chain_id: super::ARBITRUM_CHAIN_ID,
            block_number: 1,
            exact_token_contracts: true,
            token_code_present: true,
            router_code_present: true,
            native_gas_funded: false,
            token_a_funded: true,
            token_b_funded: true,
            fresh_rpc_gas_price: true,
            allowance_policy: "max_uint256_then_locked",
            receipt_l1_fee_mode: "included_in_effective_gas_price_no_world_l1fee_addition",
            external_mutation_authorized: true,
            ready: false,
        };
        let mut later = readiness.clone();
        later.block_number = 2;
        assert_eq!(readiness.status(), later.status());

        later.native_gas_funded = true;
        later.ready = true;
        assert_ne!(readiness.status(), later.status());
        assert!(matches!(
            later.status(),
            ChainReadinessStatus::Observed {
                native_gas_funded: true,
                ready: true,
                ..
            }
        ));
        assert!(CHAIN_READINESS_REFRESH_INTERVAL >= Duration::from_secs(30));
    }

    #[test]
    fn full_live_rebalance_is_a_valid_readiness_projection_and_partial_gates_fail() {
        let domain =
            LoadedDomainConfig::load("config/strategies/usdc-esp-arbitrum.v6.json").unwrap();
        let pair = &domain.snapshot().pairs[0];
        validate_readiness_pair(pair).unwrap();

        let mut missing_pair_gate = pair.clone();
        missing_pair_gate.rebalance.enabled = false;
        assert!(validate_readiness_pair(&missing_pair_gate).is_err());

        let mut missing_live_policy = pair.clone();
        missing_live_policy
            .full_live_policy
            .as_mut()
            .unwrap()
            .direct_route_only = false;
        assert!(validate_readiness_pair(&missing_live_policy).is_err());

        let mut bridge_enabled = pair.clone();
        bridge_enabled
            .full_live_policy
            .as_mut()
            .unwrap()
            .bridge_mutations_enabled = true;
        assert!(validate_readiness_pair(&bridge_enabled).is_err());
    }
}

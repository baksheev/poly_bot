use std::collections::BTreeMap;

use alloy_primitives::U256;
use anyhow::{Context, ensure};
use rust_decimal::Decimal;

use crate::{
    balances::{BinanceBalanceSnapshot, WalletBalanceSnapshot},
    binance::capital::{CapitalRouteState, NetworkInformation},
    domain::config::PairConfig,
};

use super::{
    BalanceSnapshot, RebalancePlan, RebalancePolicy, Route, RouteCandidate, WithdrawalRules,
    plan_rebalance,
};

const OPTIMISM_CHAIN_ID: u64 = 10;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebalanceEvaluation {
    pub token_symbol: String,
    pub token_decimals: u8,
    pub reference_captured: bool,
    pub plan: RebalancePlan,
}

#[derive(Debug)]
struct TokenTracker {
    symbol: String,
    decimals: u8,
    start_threshold_bps: u16,
    routes: Vec<RouteCandidate>,
    policy: Option<RebalancePolicy>,
    last_plan: Option<RebalancePlan>,
}

#[derive(Debug)]
pub struct RebalanceTracker {
    enabled: bool,
    tokens: Vec<TokenTracker>,
    ready: bool,
}

impl RebalanceTracker {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            tokens: Vec::new(),
            ready: true,
        }
    }

    pub fn new(
        pair: &PairConfig,
        routes: BTreeMap<String, Vec<RouteCandidate>>,
    ) -> anyhow::Result<Self> {
        if !pair.rebalance.enabled {
            return Ok(Self::disabled());
        }

        let tokens = [&pair.token_a, &pair.token_b]
            .into_iter()
            .map(|token| {
                let token_routes = routes
                    .get(&token.symbol)
                    .with_context(|| format!("missing rebalance routes for {}", token.symbol))?;
                ensure!(
                    !token_routes.is_empty(),
                    "rebalance routes for {} are empty",
                    token.symbol
                );
                Ok(TokenTracker {
                    symbol: token.symbol.clone(),
                    decimals: token.decimals,
                    start_threshold_bps: pair.rebalance.start_threshold_bps,
                    routes: token_routes.clone(),
                    policy: None,
                    last_plan: None,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        Ok(Self {
            enabled: true,
            tokens,
            ready: false,
        })
    }

    pub fn ready(&self) -> bool {
        self.ready
    }

    pub fn evaluate(
        &mut self,
        binance: &BinanceBalanceSnapshot,
        wallet: &WalletBalanceSnapshot,
    ) -> anyhow::Result<Vec<RebalanceEvaluation>> {
        if !self.enabled {
            return Ok(Vec::new());
        }

        let mut evaluations = Vec::new();
        let mut ready = true;
        for token in &mut self.tokens {
            let snapshot = token_snapshot(token, binance, wallet)?;
            let reference_captured = token.policy.is_none();
            if reference_captured {
                let reference_inventory = snapshot
                    .binance
                    .checked_add(snapshot.wallet)
                    .with_context(|| format!("{} reference inventory overflow", token.symbol))?;
                ensure!(
                    !reference_inventory.is_zero(),
                    "{} reference inventory is zero",
                    token.symbol
                );
                token.policy = Some(RebalancePolicy {
                    token_symbol: token.symbol.clone(),
                    reference_inventory,
                    start_threshold_bps: token.start_threshold_bps,
                    routes: token.routes.clone(),
                });
            }

            let plan = plan_rebalance(
                token.policy.as_ref().expect("policy initialized above"),
                snapshot,
                &[],
            )?;
            ready &= plan.action.is_none();
            if token.last_plan.as_ref() != Some(&plan) || reference_captured {
                token.last_plan = Some(plan.clone());
                evaluations.push(RebalanceEvaluation {
                    token_symbol: token.symbol.clone(),
                    token_decimals: token.decimals,
                    reference_captured,
                    plan,
                });
            }
        }
        self.ready = ready;
        Ok(evaluations)
    }

    pub fn mark_unready(&mut self) {
        if self.enabled {
            self.ready = false;
        }
    }

    pub fn pending_action(&self) -> Option<RebalanceEvaluation> {
        self.tokens.iter().find_map(|token| {
            let plan = token.last_plan.as_ref()?;
            plan.action.as_ref()?;
            Some(RebalanceEvaluation {
                token_symbol: token.symbol.clone(),
                token_decimals: token.decimals,
                reference_captured: false,
                plan: plan.clone(),
            })
        })
    }
}

fn token_snapshot(
    token: &TokenTracker,
    binance: &BinanceBalanceSnapshot,
    wallet: &WalletBalanceSnapshot,
) -> anyhow::Result<BalanceSnapshot> {
    let binance_balance = binance
        .balances
        .get(token.symbol.as_str())
        .with_context(|| format!("Binance balance snapshot is missing {}", token.symbol))?;
    let wallet_balance = wallet
        .token_balances
        .iter()
        .find(|balance| balance.symbol.as_ref() == token.symbol)
        .with_context(|| format!("wallet balance snapshot is missing {}", token.symbol))?;

    Ok(BalanceSnapshot {
        binance: decimal_to_base_units_floor(binance_balance.free, token.decimals)?,
        wallet: wallet_balance.base_units,
    })
}

pub fn route_candidates_from_capital(
    capital: &CapitalRouteState,
    token_decimals: u8,
    wallet_chain_id: u64,
) -> anyhow::Result<Vec<RouteCandidate>> {
    let mut routes = Vec::new();
    if let Some(network) = &capital.direct {
        routes.push(route_candidate(
            capital,
            network,
            Route::Direct {
                binance_network: network.network.clone(),
                chain_id: wallet_chain_id,
            },
            token_decimals,
            false,
        )?);
    }
    if let Some(network) = &capital.fallback {
        routes.push(route_candidate(
            capital,
            network,
            Route::Across {
                binance_network: network.network.clone(),
                bridge_chain_id: OPTIMISM_CHAIN_ID,
                wallet_chain_id,
            },
            token_decimals,
            true,
        )?);
    }
    ensure!(
        !routes.is_empty(),
        "{} has no rebalance routes",
        capital.coin
    );
    Ok(routes)
}

fn route_candidate(
    capital: &CapitalRouteState,
    network: &NetworkInformation,
    route: Route,
    token_decimals: u8,
    across: bool,
) -> anyhow::Result<RouteCandidate> {
    let multiple =
        decimal_to_base_units(network.withdraw_integer_multiple, token_decimals)?.max(U256::ONE);
    Ok(RouteCandidate {
        route,
        binance_deposit_enabled: capital.deposit_all_enabled && network.deposit_available(),
        binance_withdrawal_enabled: capital.withdrawal_all_enabled
            && network.withdrawal_available(),
        across_wallet_to_bridge_enabled: across,
        across_bridge_to_wallet_enabled: across,
        withdrawal: WithdrawalRules {
            minimum: decimal_to_base_units(network.withdraw_min, token_decimals)?,
            maximum: decimal_to_base_units(network.withdraw_max, token_decimals)?,
            multiple,
        },
    })
}

fn decimal_to_base_units(value: Decimal, decimals: u8) -> anyhow::Result<U256> {
    ensure!(
        value >= Decimal::ZERO,
        "decimal amount must not be negative"
    );
    let mantissa = value.mantissa();
    ensure!(mantissa >= 0, "decimal mantissa must not be negative");
    let numerator = U256::from(mantissa as u128)
        .checked_mul(pow10(decimals.into())?)
        .context("decimal base-unit numerator overflow")?;
    let denominator = pow10(value.scale())?;
    ensure!(
        numerator % denominator == U256::ZERO,
        "decimal amount has more precision than token base units"
    );
    Ok(numerator / denominator)
}

fn decimal_to_base_units_floor(value: Decimal, decimals: u8) -> anyhow::Result<U256> {
    ensure!(
        value >= Decimal::ZERO,
        "decimal balance must not be negative"
    );
    let mantissa = value.mantissa();
    ensure!(mantissa >= 0, "decimal mantissa must not be negative");
    let numerator = U256::from(mantissa as u128)
        .checked_mul(pow10(decimals.into())?)
        .context("decimal balance base-unit numerator overflow")?;
    Ok(numerator / pow10(value.scale())?)
}

fn pow10(exponent: u32) -> anyhow::Result<U256> {
    let mut result = U256::ONE;
    for _ in 0..exponent {
        result = result
            .checked_mul(U256::from(10))
            .context("decimal scale overflow")?;
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc, time::Instant};

    use alloy_primitives::{Address, B256, U256};
    use rust_decimal::Decimal;

    use crate::{
        balances::{
            BinanceAssetBalance, BinanceBalanceSnapshot, WalletBalanceSnapshot, WalletTokenBalance,
        },
        binance::capital::{CapitalRouteState, NetworkInformation},
        chain::rpc::RpcStats,
        domain::config::LoadedDomainConfig,
        rebalance::{Direction, Route, RouteCandidate, WithdrawalRules},
    };

    use super::{RebalanceTracker, route_candidates_from_capital};

    fn direct_route() -> RouteCandidate {
        RouteCandidate {
            route: Route::Direct {
                binance_network: "WLD".to_owned(),
                chain_id: 480,
            },
            binance_deposit_enabled: true,
            binance_withdrawal_enabled: true,
            across_wallet_to_bridge_enabled: false,
            across_bridge_to_wallet_enabled: false,
            withdrawal: WithdrawalRules {
                minimum: U256::ONE,
                maximum: U256::MAX,
                multiple: U256::ONE,
            },
        }
    }

    fn across_route() -> RouteCandidate {
        RouteCandidate {
            route: Route::Across {
                binance_network: "OPTIMISM".to_owned(),
                bridge_chain_id: 10,
                wallet_chain_id: 480,
            },
            binance_deposit_enabled: true,
            binance_withdrawal_enabled: true,
            across_wallet_to_bridge_enabled: true,
            across_bridge_to_wallet_enabled: true,
            withdrawal: WithdrawalRules {
                minimum: U256::ONE,
                maximum: U256::MAX,
                multiple: U256::ONE,
            },
        }
    }

    fn snapshots(
        binance_usdc: u64,
        wallet_usdc: u64,
    ) -> (BinanceBalanceSnapshot, WalletBalanceSnapshot) {
        budget_snapshots(
            Decimal::from(binance_usdc),
            U256::from(wallet_usdc) * U256::from(1_000_000),
            Decimal::from(5_000),
            U256::from(5_000) * U256::from(10).pow(U256::from(18)),
        )
    }

    fn budget_snapshots(
        binance_usdc: Decimal,
        wallet_usdc: U256,
        binance_wld: Decimal,
        wallet_wld: U256,
    ) -> (BinanceBalanceSnapshot, WalletBalanceSnapshot) {
        let observed_at = Instant::now();
        (
            BinanceBalanceSnapshot {
                account_update_time_ms: 1,
                account_type: "SPOT".to_owned(),
                can_trade: true,
                balances: BTreeMap::from([
                    (
                        Arc::from("USDC"),
                        BinanceAssetBalance {
                            free: binance_usdc,
                            locked: Decimal::ZERO,
                        },
                    ),
                    (
                        Arc::from("WLD"),
                        BinanceAssetBalance {
                            free: binance_wld,
                            locked: Decimal::ZERO,
                        },
                    ),
                ]),
                observed_at,
                request_duration_us: 1,
            },
            WalletBalanceSnapshot {
                owner: Address::ZERO,
                chain_id: 480,
                block_number: 1,
                block_hash: B256::ZERO,
                native_balance_wei: U256::ONE,
                token_balances: vec![
                    WalletTokenBalance {
                        symbol: Arc::from("USDC"),
                        contract: Address::ZERO,
                        base_units: wallet_usdc,
                    },
                    WalletTokenBalance {
                        symbol: Arc::from("WLD"),
                        contract: Address::ZERO,
                        base_units: wallet_wld,
                    },
                ],
                observed_at,
                request_duration_us: 1,
                rpc_stats: RpcStats {
                    http_requests: 1,
                    eth_calls: 2,
                    rate_limit_retries: 0,
                },
            },
        )
    }

    fn tracker() -> RebalanceTracker {
        let config = LoadedDomainConfig::load(format!(
            "{}/config/strategies/usdc-wld-world-chain.v3.json",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap();
        let pair = &config.snapshot().pairs[0];
        RebalanceTracker::new(
            pair,
            BTreeMap::from([
                ("USDC".to_owned(), vec![across_route()]),
                ("WLD".to_owned(), vec![direct_route()]),
            ]),
        )
        .unwrap()
    }

    #[test]
    fn captures_initial_total_as_reference_and_does_not_ratchet_it_down() {
        let mut tracker = tracker();
        let (binance, wallet) = snapshots(5_000, 5_000);
        let initial = tracker.evaluate(&binance, &wallet).unwrap();
        let usdc = initial
            .iter()
            .find(|evaluation| evaluation.token_symbol == "USDC")
            .unwrap();
        assert!(usdc.reference_captured);
        assert_eq!(
            usdc.plan.reference_inventory,
            U256::from(10_000_000_000_u64)
        );
        assert_eq!(usdc.plan.start_balance, U256::from(2_500_000_000_u64));
        assert!(tracker.ready());

        let (binance, wallet) = snapshots(2_000, 8_000);
        let changed = tracker.evaluate(&binance, &wallet).unwrap();
        let usdc = changed
            .iter()
            .find(|evaluation| evaluation.token_symbol == "USDC")
            .unwrap();
        assert!(!usdc.reference_captured);
        assert_eq!(
            usdc.plan.reference_inventory,
            U256::from(10_000_000_000_u64)
        );
        assert_eq!(
            usdc.plan.action.as_ref().unwrap().direction,
            Direction::WalletToBinance
        );
        assert_eq!(
            usdc.plan.action.as_ref().unwrap().amount,
            U256::from(3_000_000_000_u64)
        );
        assert!(!tracker.ready());
    }

    #[test]
    fn capital_routes_preserve_live_network_limits_in_base_units() {
        let network = NetworkInformation {
            network: "WLD".to_owned(),
            name: "World Chain".to_owned(),
            deposit_enable: true,
            withdraw_enable: true,
            busy: false,
            withdraw_fee: Decimal::ZERO,
            withdraw_min: Decimal::new(2, 1),
            withdraw_max: Decimal::from(10_000),
            withdraw_integer_multiple: Decimal::new(1, 2),
        };
        let capital = CapitalRouteState {
            coin: "WLD".to_owned(),
            deposit_all_enabled: true,
            withdrawal_all_enabled: true,
            direct: Some(network),
            fallback: None,
        };

        let routes = route_candidates_from_capital(&capital, 18, 480).unwrap();
        assert_eq!(routes.len(), 1);
        assert_eq!(
            routes[0].withdrawal.minimum,
            U256::from(200_000_000_000_000_000_u64)
        );
        assert_eq!(
            routes[0].withdrawal.multiple,
            U256::from(10_000_000_000_000_000_u64)
        );
    }

    #[test]
    fn floors_exchange_dust_beyond_on_chain_token_precision() {
        assert_eq!(
            super::decimal_to_base_units_floor(
                Decimal::from_str_exact("6170.80727184").unwrap(),
                6,
            )
            .unwrap(),
            U256::from(6_170_807_271_u64)
        );
        assert!(
            super::decimal_to_base_units(Decimal::from_str_exact("6170.80727184").unwrap(), 6,)
                .is_err()
        );
    }

    #[test]
    fn updates_two_token_budget_without_losing_the_second_action() {
        let scale_18 = U256::from(10).pow(U256::from(18));
        let mut tracker = tracker();
        let (binance, wallet) = budget_snapshots(
            Decimal::from(1_000),
            U256::ZERO,
            Decimal::from(2_500),
            U256::ZERO,
        );
        tracker.evaluate(&binance, &wallet).unwrap();
        let first = tracker.pending_action().unwrap();
        assert_eq!(first.token_symbol, "USDC");
        let first_action = first.plan.action.unwrap();
        assert_eq!(first_action.amount, U256::from(500_000_000));
        assert!(matches!(first_action.route, Route::Across { .. }));

        let (binance, wallet) = budget_snapshots(
            Decimal::from(500),
            U256::from(499_000_000),
            Decimal::from(2_500),
            U256::ZERO,
        );
        tracker.evaluate(&binance, &wallet).unwrap();
        let second = tracker.pending_action().unwrap();
        assert_eq!(second.token_symbol, "WLD");
        let second_action = second.plan.action.unwrap();
        assert_eq!(second_action.amount, U256::from(1_250) * scale_18);
        assert!(matches!(second_action.route, Route::Direct { .. }));

        let (binance, wallet) = budget_snapshots(
            Decimal::from(500),
            U256::from(499_000_000),
            Decimal::from(1_250),
            U256::from(1_249) * scale_18,
        );
        tracker.evaluate(&binance, &wallet).unwrap();
        assert!(tracker.pending_action().is_none());
        assert!(tracker.ready());
    }
}

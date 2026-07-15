use alloy_primitives::U256;
use anyhow::{Result, ensure};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Location {
    Binance,
    Wallet,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Direction {
    WalletToBinance,
    BinanceToWallet,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Route {
    Direct {
        binance_network: String,
        chain_id: u64,
    },
    Across {
        binance_network: String,
        bridge_chain_id: u64,
        wallet_chain_id: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WithdrawalRules {
    pub minimum: U256,
    pub maximum: U256,
    pub multiple: U256,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteCandidate {
    pub route: Route,
    pub binance_deposit_enabled: bool,
    pub binance_withdrawal_enabled: bool,
    pub across_wallet_to_bridge_enabled: bool,
    pub across_bridge_to_wallet_enabled: bool,
    pub withdrawal: WithdrawalRules,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebalancePolicy {
    pub token_symbol: String,
    pub binance_min: U256,
    pub wallet_min: U256,
    pub routes: Vec<RouteCandidate>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BalanceSnapshot {
    pub binance: U256,
    pub wallet: U256,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PendingTransfer {
    pub source: Location,
    pub destination: Location,
    pub amount: U256,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebalanceAction {
    pub direction: Direction,
    pub amount: U256,
    pub route: Route,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebalancePlan {
    pub projected: BalanceSnapshot,
    pub binance_target: U256,
    pub wallet_target: U256,
    pub action: Option<RebalanceAction>,
}

pub fn plan_rebalance(
    policy: &RebalancePolicy,
    snapshot: BalanceSnapshot,
    pending: &[PendingTransfer],
) -> Result<RebalancePlan> {
    validate_policy(policy)?;
    let projected = projected_balances(snapshot, pending)?;
    let total = projected
        .binance
        .checked_add(projected.wallet)
        .ok_or_else(|| anyhow::anyhow!("{} inventory overflow", policy.token_symbol))?;
    let required = policy
        .binance_min
        .checked_add(policy.wallet_min)
        .ok_or_else(|| anyhow::anyhow!("{} minimum inventory overflow", policy.token_symbol))?;

    ensure!(
        total >= required,
        "{} total inventory is below the configured minimum",
        policy.token_symbol
    );

    let binance_target = total / U256::from(2);
    let wallet_target = total - binance_target;
    let action = if projected.binance < policy.binance_min {
        plan_binance_refill(policy, projected, binance_target)?
    } else if projected.wallet < policy.wallet_min {
        plan_wallet_refill(policy, projected, wallet_target)?
    } else {
        None
    };

    Ok(RebalancePlan {
        projected,
        binance_target,
        wallet_target,
        action,
    })
}

fn validate_policy(policy: &RebalancePolicy) -> Result<()> {
    ensure!(
        !policy.token_symbol.trim().is_empty(),
        "rebalance token symbol is empty"
    );
    ensure!(
        !policy.routes.is_empty(),
        "{} has no configured rebalance routes",
        policy.token_symbol
    );
    for candidate in &policy.routes {
        ensure!(
            !candidate.withdrawal.multiple.is_zero(),
            "{} withdrawal multiple must be positive",
            policy.token_symbol
        );
        ensure!(
            candidate.withdrawal.minimum <= candidate.withdrawal.maximum,
            "{} withdrawal minimum exceeds maximum",
            policy.token_symbol
        );
    }
    Ok(())
}

fn projected_balances(
    snapshot: BalanceSnapshot,
    pending: &[PendingTransfer],
) -> Result<BalanceSnapshot> {
    let mut projected = snapshot;
    for transfer in pending {
        ensure!(
            transfer.source != transfer.destination,
            "pending transfer source and destination are identical"
        );
        match (transfer.source, transfer.destination) {
            (Location::Binance, Location::Wallet) => {
                projected.binance =
                    projected
                        .binance
                        .checked_sub(transfer.amount)
                        .ok_or_else(|| {
                            anyhow::anyhow!("pending Binance debit exceeds the observed balance")
                        })?;
                projected.wallet =
                    projected
                        .wallet
                        .checked_add(transfer.amount)
                        .ok_or_else(|| {
                            anyhow::anyhow!("pending wallet credit overflows the projected balance")
                        })?;
            }
            (Location::Wallet, Location::Binance) => {
                projected.wallet =
                    projected
                        .wallet
                        .checked_sub(transfer.amount)
                        .ok_or_else(|| {
                            anyhow::anyhow!("pending wallet debit exceeds the observed balance")
                        })?;
                projected.binance =
                    projected
                        .binance
                        .checked_add(transfer.amount)
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "pending Binance credit overflows the projected balance"
                            )
                        })?;
            }
            _ => unreachable!("identical locations are rejected above"),
        }
    }
    Ok(projected)
}

fn plan_binance_refill(
    policy: &RebalancePolicy,
    projected: BalanceSnapshot,
    binance_target: U256,
) -> Result<Option<RebalanceAction>> {
    let wallet_surplus = projected
        .wallet
        .checked_sub(policy.wallet_min)
        .unwrap_or(U256::ZERO);
    let target_deficit = binance_target
        .checked_sub(projected.binance)
        .unwrap_or(U256::ZERO);
    let amount = target_deficit.min(wallet_surplus);
    if amount.is_zero() {
        return Ok(None);
    }
    let candidate = select_route(policy, Direction::WalletToBinance)?;
    Ok(Some(RebalanceAction {
        direction: Direction::WalletToBinance,
        amount,
        route: candidate.route.clone(),
    }))
}

fn plan_wallet_refill(
    policy: &RebalancePolicy,
    projected: BalanceSnapshot,
    wallet_target: U256,
) -> Result<Option<RebalanceAction>> {
    let binance_surplus = projected
        .binance
        .checked_sub(policy.binance_min)
        .unwrap_or(U256::ZERO);
    let target_deficit = wallet_target
        .checked_sub(projected.wallet)
        .unwrap_or(U256::ZERO);
    let requested = target_deficit.min(binance_surplus);
    if requested.is_zero() {
        return Ok(None);
    }
    let candidate = select_route(policy, Direction::BinanceToWallet)?;
    let amount = constrain_withdrawal(requested, candidate.withdrawal);

    Ok(
        (!amount.is_zero() && amount <= binance_surplus).then(|| RebalanceAction {
            direction: Direction::BinanceToWallet,
            amount,
            route: candidate.route.clone(),
        }),
    )
}

fn select_route(policy: &RebalancePolicy, direction: Direction) -> Result<&RouteCandidate> {
    policy
        .routes
        .iter()
        .find(|candidate| candidate.supports(direction))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "{} has no currently available {:?} route",
                policy.token_symbol,
                direction
            )
        })
}

impl RouteCandidate {
    fn supports(&self, direction: Direction) -> bool {
        match (&self.route, direction) {
            (Route::Direct { .. }, Direction::WalletToBinance) => self.binance_deposit_enabled,
            (Route::Direct { .. }, Direction::BinanceToWallet) => self.binance_withdrawal_enabled,
            (Route::Across { .. }, Direction::WalletToBinance) => {
                self.binance_deposit_enabled && self.across_wallet_to_bridge_enabled
            }
            (Route::Across { .. }, Direction::BinanceToWallet) => {
                self.binance_withdrawal_enabled && self.across_bridge_to_wallet_enabled
            }
        }
    }
}

fn constrain_withdrawal(requested: U256, rules: WithdrawalRules) -> U256 {
    if requested.is_zero() {
        return U256::ZERO;
    }

    let bounded = requested.max(rules.minimum).min(rules.maximum);
    let rounded = bounded - (bounded % rules.multiple);
    if rounded < rules.minimum {
        U256::ZERO
    } else {
        rounded
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn direct_policy() -> RebalancePolicy {
        RebalancePolicy {
            token_symbol: "WLD".to_owned(),
            binance_min: U256::from(4_000),
            wallet_min: U256::from(4_000),
            routes: vec![direct_candidate(true, true)],
        }
    }

    fn direct_candidate(deposit_enabled: bool, withdrawal_enabled: bool) -> RouteCandidate {
        RouteCandidate {
            route: Route::Direct {
                binance_network: "WLD".to_owned(),
                chain_id: 480,
            },
            binance_deposit_enabled: deposit_enabled,
            binance_withdrawal_enabled: withdrawal_enabled,
            across_wallet_to_bridge_enabled: false,
            across_bridge_to_wallet_enabled: false,
            withdrawal: WithdrawalRules {
                minimum: U256::from(200),
                maximum: U256::from(8_700_000),
                multiple: U256::from(10),
            },
        }
    }

    fn across_candidate(deposit_enabled: bool, withdrawal_enabled: bool) -> RouteCandidate {
        RouteCandidate {
            route: Route::Across {
                binance_network: "OPTIMISM".to_owned(),
                bridge_chain_id: 10,
                wallet_chain_id: 480,
            },
            binance_deposit_enabled: deposit_enabled,
            binance_withdrawal_enabled: withdrawal_enabled,
            across_wallet_to_bridge_enabled: true,
            across_bridge_to_wallet_enabled: true,
            withdrawal: WithdrawalRules {
                minimum: U256::from(300),
                maximum: U256::from(8_000_000),
                multiple: U256::from(100),
            },
        }
    }

    #[test]
    fn does_nothing_when_both_locations_are_above_minimum() {
        let plan = plan_rebalance(
            &direct_policy(),
            BalanceSnapshot {
                binance: U256::from(5_000),
                wallet: U256::from(5_000),
            },
            &[],
        )
        .unwrap();

        assert_eq!(plan.action, None);
        assert_eq!(plan.binance_target, U256::from(5_000));
        assert_eq!(plan.wallet_target, U256::from(5_000));
    }

    #[test]
    fn refills_binance_from_wallet_toward_half_of_inventory() {
        let plan = plan_rebalance(
            &direct_policy(),
            BalanceSnapshot {
                binance: U256::from(3_000),
                wallet: U256::from(7_000),
            },
            &[],
        )
        .unwrap();

        assert_eq!(
            plan.action,
            Some(RebalanceAction {
                direction: Direction::WalletToBinance,
                amount: U256::from(2_000),
                route: direct_policy().routes[0].route.clone(),
            })
        );
    }

    #[test]
    fn refills_wallet_and_rounds_withdrawal_down_to_multiple() {
        let plan = plan_rebalance(
            &direct_policy(),
            BalanceSnapshot {
                binance: U256::from(7_005),
                wallet: U256::from(3_000),
            },
            &[],
        )
        .unwrap();

        assert_eq!(plan.action.unwrap().amount, U256::from(2_000));
    }

    #[test]
    fn raises_small_withdrawal_to_exchange_minimum_when_surplus_allows_it() {
        let plan = plan_rebalance(
            &direct_policy(),
            BalanceSnapshot {
                binance: U256::from(4_250),
                wallet: U256::from(3_900),
            },
            &[],
        )
        .unwrap();

        assert_eq!(plan.action.unwrap().amount, U256::from(200));
    }

    #[test]
    fn does_not_withdraw_exchange_minimum_when_it_exceeds_surplus() {
        let plan = plan_rebalance(
            &direct_policy(),
            BalanceSnapshot {
                binance: U256::from(4_100),
                wallet: U256::from(3_900),
            },
            &[],
        )
        .unwrap();

        assert_eq!(plan.action, None);
    }

    #[test]
    fn active_transfer_is_included_in_projected_balances() {
        let plan = plan_rebalance(
            &direct_policy(),
            BalanceSnapshot {
                binance: U256::from(3_000),
                wallet: U256::from(7_000),
            },
            &[PendingTransfer {
                source: Location::Wallet,
                destination: Location::Binance,
                amount: U256::from(2_000),
            }],
        )
        .unwrap();

        assert_eq!(plan.projected.binance, U256::from(5_000));
        assert_eq!(plan.projected.wallet, U256::from(5_000));
        assert_eq!(plan.action, None);
    }

    #[test]
    fn rejects_an_impossible_pending_debit() {
        let error = plan_rebalance(
            &direct_policy(),
            BalanceSnapshot {
                binance: U256::from(5_000),
                wallet: U256::from(5_000),
            },
            &[PendingTransfer {
                source: Location::Wallet,
                destination: Location::Binance,
                amount: U256::from(5_001),
            }],
        )
        .unwrap_err();

        assert!(error.to_string().contains("pending wallet debit"));
    }

    #[test]
    fn rejects_total_inventory_below_minimum() {
        let error = plan_rebalance(
            &direct_policy(),
            BalanceSnapshot {
                binance: U256::from(3_999),
                wallet: U256::from(4_000),
            },
            &[],
        )
        .unwrap_err();

        assert!(error.to_string().contains("total inventory"));
    }

    #[test]
    fn handles_eighteen_decimal_balances_without_narrowing() {
        let scale = U256::from(10).pow(U256::from(18));
        let mut policy = direct_policy();
        policy.binance_min *= scale;
        policy.wallet_min *= scale;
        policy.routes[0].withdrawal.minimum *= scale;
        policy.routes[0].withdrawal.maximum *= scale;
        policy.routes[0].withdrawal.multiple *= scale;

        let plan = plan_rebalance(
            &policy,
            BalanceSnapshot {
                binance: U256::from(3_000) * scale,
                wallet: U256::from(7_000) * scale,
            },
            &[],
        )
        .unwrap();

        assert_eq!(plan.action.unwrap().amount, U256::from(2_000) * scale);
    }

    #[test]
    fn preserves_across_route_in_action() {
        let mut policy = direct_policy();
        policy.routes = vec![across_candidate(true, true)];

        let plan = plan_rebalance(
            &policy,
            BalanceSnapshot {
                binance: U256::from(3_000),
                wallet: U256::from(7_000),
            },
            &[],
        )
        .unwrap();

        assert_eq!(plan.action.unwrap().route, policy.routes[0].route);
    }

    #[test]
    fn prefers_direct_withdrawal_when_the_wld_network_is_available() {
        let mut policy = direct_policy();
        policy.routes.push(across_candidate(true, true));

        let plan = plan_rebalance(
            &policy,
            BalanceSnapshot {
                binance: U256::from(7_000),
                wallet: U256::from(3_000),
            },
            &[],
        )
        .unwrap();

        assert!(matches!(
            plan.action.unwrap().route,
            Route::Direct { ref binance_network, .. } if binance_network == "WLD"
        ));
    }

    #[test]
    fn falls_back_to_optimism_withdrawal_when_wld_withdrawal_disappears() {
        let mut policy = direct_policy();
        policy.routes = vec![direct_candidate(true, false), across_candidate(true, true)];

        let plan = plan_rebalance(
            &policy,
            BalanceSnapshot {
                binance: U256::from(7_000),
                wallet: U256::from(3_000),
            },
            &[],
        )
        .unwrap();

        assert!(matches!(
            plan.action.unwrap().route,
            Route::Across { ref binance_network, bridge_chain_id: 10, .. }
                if binance_network == "OPTIMISM"
        ));
    }

    #[test]
    fn selects_deposit_route_independently_from_withdrawal_route() {
        let mut policy = direct_policy();
        policy.routes = vec![direct_candidate(false, true), across_candidate(true, true)];

        let plan = plan_rebalance(
            &policy,
            BalanceSnapshot {
                binance: U256::from(3_000),
                wallet: U256::from(7_000),
            },
            &[],
        )
        .unwrap();

        assert!(matches!(plan.action.unwrap().route, Route::Across { .. }));
    }

    #[test]
    fn fallback_uses_its_own_live_withdrawal_limits() {
        let mut policy = direct_policy();
        policy.routes = vec![direct_candidate(true, false), across_candidate(true, true)];

        let plan = plan_rebalance(
            &policy,
            BalanceSnapshot {
                binance: U256::from(4_250),
                wallet: U256::from(3_900),
            },
            &[],
        )
        .unwrap();

        assert_eq!(plan.action, None);
    }

    #[test]
    fn fails_closed_when_neither_direct_nor_bridge_route_is_available() {
        let mut policy = direct_policy();
        policy.routes = vec![direct_candidate(true, false), across_candidate(true, false)];

        let error = plan_rebalance(
            &policy,
            BalanceSnapshot {
                binance: U256::from(7_000),
                wallet: U256::from(3_000),
            },
            &[],
        )
        .unwrap_err();

        assert!(error.to_string().contains("no currently available"));
    }

    #[test]
    fn requires_across_direction_to_be_available_for_fallback() {
        let mut fallback = across_candidate(true, true);
        fallback.across_bridge_to_wallet_enabled = false;
        let mut policy = direct_policy();
        policy.routes = vec![direct_candidate(true, false), fallback];

        let error = plan_rebalance(
            &policy,
            BalanceSnapshot {
                binance: U256::from(7_000),
                wallet: U256::from(3_000),
            },
            &[],
        )
        .unwrap_err();

        assert!(error.to_string().contains("no currently available"));
    }
}

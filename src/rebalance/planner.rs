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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebalancePolicy {
    pub token_symbol: String,
    pub binance_min: U256,
    pub wallet_min: U256,
    pub withdrawal_min: U256,
    pub withdrawal_max: U256,
    pub withdrawal_multiple: U256,
    pub route: Route,
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
        plan_binance_refill(policy, projected, binance_target)
    } else if projected.wallet < policy.wallet_min {
        plan_wallet_refill(policy, projected, wallet_target)
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
        !policy.withdrawal_multiple.is_zero(),
        "{} withdrawal multiple must be positive",
        policy.token_symbol
    );
    ensure!(
        policy.withdrawal_min <= policy.withdrawal_max,
        "{} withdrawal minimum exceeds maximum",
        policy.token_symbol
    );
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
) -> Option<RebalanceAction> {
    let wallet_surplus = projected.wallet.checked_sub(policy.wallet_min)?;
    let target_deficit = binance_target.checked_sub(projected.binance)?;
    let amount = target_deficit.min(wallet_surplus);
    (!amount.is_zero()).then(|| RebalanceAction {
        direction: Direction::WalletToBinance,
        amount,
        route: policy.route.clone(),
    })
}

fn plan_wallet_refill(
    policy: &RebalancePolicy,
    projected: BalanceSnapshot,
    wallet_target: U256,
) -> Option<RebalanceAction> {
    let binance_surplus = projected.binance.checked_sub(policy.binance_min)?;
    let target_deficit = wallet_target.checked_sub(projected.wallet)?;
    let requested = target_deficit.min(binance_surplus);
    let amount = constrain_withdrawal(requested, policy);

    (!amount.is_zero() && amount <= binance_surplus).then(|| RebalanceAction {
        direction: Direction::BinanceToWallet,
        amount,
        route: policy.route.clone(),
    })
}

fn constrain_withdrawal(requested: U256, policy: &RebalancePolicy) -> U256 {
    if requested.is_zero() {
        return U256::ZERO;
    }

    let bounded = requested
        .max(policy.withdrawal_min)
        .min(policy.withdrawal_max);
    let rounded = bounded - (bounded % policy.withdrawal_multiple);
    if rounded < policy.withdrawal_min {
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
            withdrawal_min: U256::from(200),
            withdrawal_max: U256::from(8_700_000),
            withdrawal_multiple: U256::from(10),
            route: Route::Direct {
                binance_network: "WLD".to_owned(),
                chain_id: 480,
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
                route: direct_policy().route,
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
        policy.withdrawal_min *= scale;
        policy.withdrawal_max *= scale;
        policy.withdrawal_multiple *= scale;

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
        policy.route = Route::Across {
            binance_network: "OPTIMISM".to_owned(),
            bridge_chain_id: 10,
            wallet_chain_id: 480,
        };

        let plan = plan_rebalance(
            &policy,
            BalanceSnapshot {
                binance: U256::from(3_000),
                wallet: U256::from(7_000),
            },
            &[],
        )
        .unwrap();

        assert_eq!(plan.action.unwrap().route, policy.route);
    }
}

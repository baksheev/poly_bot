use std::{str::FromStr, time::Duration};

use alloy_primitives::{Address, B256, U256};
use anyhow::{Context, ensure};
use serde::{Deserialize, Serialize};

use crate::{
    arbitrage::ArbitrageDirection,
    dex::{
        execution::{ExactInputSwapRequest, SwapRoute, SwapSubmissionPolicy},
        hydration::{HydratedPool, PoolIdentity},
        pool_id::V4PoolKey,
    },
    domain::config::PairConfig,
    opportunity::TradeEvaluation,
};

pub const DEX_PLAN_TTL_SECONDS: u64 = 30;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "protocol", rename_all = "snake_case")]
pub enum DexRoutePlan {
    UniswapV3 {
        router: String,
        pool_address: String,
        fee_pips: u32,
    },
    UniswapV4 {
        router: String,
        pool_id: String,
        currency0: String,
        currency1: String,
        fee_pips: u32,
        tick_spacing: i32,
        hooks: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DexSwapPlan {
    pub route: DexRoutePlan,
    pub token_in: String,
    pub token_out: String,
    pub amount_in_base_units: u128,
    pub amount_out_minimum_base_units: u128,
    pub deadline_unix_seconds: u64,
}

impl DexSwapPlan {
    pub fn build(
        pair: &PairConfig,
        pool: &HydratedPool,
        direction: ArbitrageDirection,
        trade: TradeEvaluation,
        deadline_unix_seconds: u64,
    ) -> anyhow::Result<Self> {
        ensure!(
            pool.pair_id == pair.id,
            "selected DEX pool belongs to another pair"
        );
        ensure!(deadline_unix_seconds > 0, "DEX plan deadline is zero");
        let token_a = parse_address("token A", &pair.token_a.contract)?;
        let token_b = parse_address("token B", &pair.token_b.contract)?;
        ensure!(
            (pool.token0 == token_a && pool.token1 == token_b)
                || (pool.token0 == token_b && pool.token1 == token_a),
            "selected DEX pool tokens differ from the pair"
        );
        let (token_in, token_out, amount_in, amount_out_minimum) = match direction {
            ArbitrageDirection::BuyTokenBOnDexSellOnCex => {
                (token_a, token_b, trade.cost_token_a, trade.token_b_amount)
            }
            ArbitrageDirection::BuyTokenBOnCexSellOnDex => (
                token_b,
                token_a,
                trade.token_b_amount,
                trade.proceeds_token_a,
            ),
        };
        let route = match pool.identity {
            PoolIdentity::V3 { address, fee_pips } => DexRoutePlan::UniswapV3 {
                router: required_address(
                    "uniswap_v3_router_address",
                    pair.chain.uniswap_v3_router_address.as_deref(),
                )?
                .to_string(),
                pool_address: address.to_string(),
                fee_pips,
            },
            PoolIdentity::V4 { pool_id, fee_pips } => {
                let key = configured_v4_key(pair, pool, pool_id)?;
                ensure!(
                    key.fee_pips == fee_pips,
                    "V4 plan fee differs from pool identity"
                );
                DexRoutePlan::UniswapV4 {
                    router: required_address(
                        "uniswap_v4_router_address",
                        pair.chain.uniswap_v4_router_address.as_deref(),
                    )?
                    .to_string(),
                    pool_id: pool_id.to_string(),
                    currency0: key.currency0.to_string(),
                    currency1: key.currency1.to_string(),
                    fee_pips: key.fee_pips,
                    tick_spacing: key.tick_spacing,
                    hooks: key.hooks.to_string(),
                }
            }
        };
        let plan = Self {
            route,
            token_in: token_in.to_string(),
            token_out: token_out.to_string(),
            amount_in_base_units: u128::try_from(amount_in)
                .context("DEX plan input exceeds u128")?,
            amount_out_minimum_base_units: u128::try_from(amount_out_minimum)
                .context("DEX plan minimum output exceeds u128")?,
            deadline_unix_seconds,
        };
        plan.validate()?;
        Ok(plan)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(self.amount_in_base_units > 0, "DEX plan input is zero");
        ensure!(
            self.amount_out_minimum_base_units > 0,
            "DEX plan minimum output is zero"
        );
        ensure!(self.deadline_unix_seconds > 0, "DEX plan deadline is zero");
        let token_in = parse_address("DEX plan input token", &self.token_in)?;
        let token_out = parse_address("DEX plan output token", &self.token_out)?;
        ensure!(token_in != token_out, "DEX plan tokens are identical");
        match &self.route {
            DexRoutePlan::UniswapV3 {
                router,
                pool_address,
                fee_pips,
            } => {
                parse_address("DEX plan V3 router", router)?;
                parse_address("DEX plan V3 pool", pool_address)?;
                ensure!(*fee_pips > 0, "DEX plan V3 fee is zero");
            }
            DexRoutePlan::UniswapV4 {
                router,
                pool_id,
                currency0,
                currency1,
                fee_pips,
                tick_spacing,
                hooks,
            } => {
                parse_address("DEX plan V4 router", router)?;
                let expected_pool_id =
                    B256::from_str(pool_id).context("invalid DEX plan V4 pool id")?;
                let key = V4PoolKey::new(
                    parse_address("DEX plan V4 currency0", currency0)?,
                    parse_address("DEX plan V4 currency1", currency1)?,
                    *fee_pips,
                    *tick_spacing,
                    parse_hooks_address("DEX plan V4 hooks", hooks)?,
                )?;
                ensure!(
                    key.pool_id() == expected_pool_id,
                    "DEX plan V4 pool id mismatch"
                );
                ensure!(
                    (token_in == key.currency0 && token_out == key.currency1)
                        || (token_in == key.currency1 && token_out == key.currency0),
                    "DEX plan V4 tokens differ from pool key"
                );
            }
        }
        Ok(())
    }

    pub fn execution_request(
        &self,
        operation_id: impl Into<String>,
        maximum_fee_per_gas_wei: u128,
    ) -> anyhow::Result<ExactInputSwapRequest> {
        self.validate()?;
        let route = match &self.route {
            DexRoutePlan::UniswapV3 {
                router, fee_pips, ..
            } => SwapRoute::V3 {
                router: parse_address("DEX plan V3 router", router)?,
                fee_pips: *fee_pips,
            },
            DexRoutePlan::UniswapV4 {
                router,
                currency0,
                currency1,
                fee_pips,
                tick_spacing,
                hooks,
                ..
            } => SwapRoute::V4 {
                router: parse_address("DEX plan V4 router", router)?,
                pool_key: V4PoolKey::new(
                    parse_address("DEX plan V4 currency0", currency0)?,
                    parse_address("DEX plan V4 currency1", currency1)?,
                    *fee_pips,
                    *tick_spacing,
                    parse_hooks_address("DEX plan V4 hooks", hooks)?,
                )?,
            },
        };
        let mut request = ExactInputSwapRequest::with_rails_defaults(
            operation_id,
            route,
            parse_address("DEX plan input token", &self.token_in)?,
            parse_address("DEX plan output token", &self.token_out)?,
            U256::from(self.amount_in_base_units),
            U256::from(self.amount_out_minimum_base_units),
            self.deadline_unix_seconds,
        );
        request.maximum_fee_per_gas_wei = maximum_fee_per_gas_wei;
        request.confirmation_timeout = Duration::from_secs(5);
        request.submission_policy = SwapSubmissionPolicy::Immediate;
        request.validate()?;
        Ok(request)
    }
}

fn configured_v4_key(
    pair: &PairConfig,
    pool: &HydratedPool,
    pool_id: B256,
) -> anyhow::Result<V4PoolKey> {
    pair.dex
        .uniswap_v4
        .as_ref()
        .context("missing Uniswap V4 config")?
        .pools
        .iter()
        .filter_map(|configured| {
            V4PoolKey::new(
                pool.token0,
                pool.token1,
                configured.fee_tier,
                configured.tick_spacing,
                Address::from_str(&configured.hooks).ok()?,
            )
            .ok()
        })
        .find(|key| key.pool_id() == pool_id)
        .context("hydrated V4 pool is absent from versioned domain config")
}

fn required_address(name: &str, value: Option<&str>) -> anyhow::Result<Address> {
    parse_address(name, value.with_context(|| format!("missing {name}"))?)
}

fn parse_address(name: &str, value: &str) -> anyhow::Result<Address> {
    let address = Address::from_str(value).with_context(|| format!("invalid {name}"))?;
    ensure!(address != Address::ZERO, "{name} is zero");
    Ok(address)
}

fn parse_hooks_address(name: &str, value: &str) -> anyhow::Result<Address> {
    Address::from_str(value).with_context(|| format!("invalid {name}"))
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, U256};

    use crate::dex::{
        execution::{SwapRoute, SwapSubmissionPolicy},
        pool_id::V4PoolKey,
    };

    use super::{DexRoutePlan, DexSwapPlan};

    #[test]
    fn v3_plan_round_trips_into_an_exact_input_request() {
        let plan = DexSwapPlan {
            route: DexRoutePlan::UniswapV3 {
                router: Address::repeat_byte(0x11).to_string(),
                pool_address: Address::repeat_byte(0x22).to_string(),
                fee_pips: 3_000,
            },
            token_in: Address::repeat_byte(0x33).to_string(),
            token_out: Address::repeat_byte(0x44).to_string(),
            amount_in_base_units: 10_000_000,
            amount_out_minimum_base_units: 9_000_000,
            deadline_unix_seconds: 1_900_000_000,
        };

        let request = plan
            .execution_request("rustarb-plan.dex", 2_500_000)
            .unwrap();
        assert_eq!(request.amount_in, U256::from(10_000_000_u64));
        assert_eq!(request.amount_out_minimum, U256::from(9_000_000_u64));
        assert_eq!(request.maximum_fee_per_gas_wei, 2_500_000);
        assert_eq!(request.submission_policy, SwapSubmissionPolicy::Immediate);
    }

    #[test]
    fn v4_no_hooks_plan_round_trips_into_an_exact_input_request() {
        let currency0 = Address::repeat_byte(0x33);
        let currency1 = Address::repeat_byte(0x44);
        let key = V4PoolKey::new(currency0, currency1, 3_000, 60, Address::ZERO).unwrap();
        let plan = DexSwapPlan {
            route: DexRoutePlan::UniswapV4 {
                router: Address::repeat_byte(0x11).to_string(),
                pool_id: key.pool_id().to_string(),
                currency0: currency0.to_string(),
                currency1: currency1.to_string(),
                fee_pips: 3_000,
                tick_spacing: 60,
                hooks: Address::ZERO.to_string(),
            },
            token_in: currency0.to_string(),
            token_out: currency1.to_string(),
            amount_in_base_units: 10_000_000,
            amount_out_minimum_base_units: 9_000_000,
            deadline_unix_seconds: 1_900_000_000,
        };

        let request = plan
            .execution_request("rustarb-plan-v4.dex", 2_500_000)
            .unwrap();
        assert_eq!(request.amount_in, U256::from(10_000_000_u64));
        assert_eq!(request.amount_out_minimum, U256::from(9_000_000_u64));
        assert_eq!(request.maximum_fee_per_gas_wei, 2_500_000);
        assert_eq!(request.submission_policy, SwapSubmissionPolicy::Immediate);
        assert!(matches!(
            request.route,
            SwapRoute::V4 { pool_key, .. }
                if pool_key.hooks == Address::ZERO && pool_key.pool_id() == key.pool_id()
        ));
    }
}

use std::{str::FromStr, time::Duration};

use alloy_primitives::{Address, B256, U256};
use anyhow::{Context, ensure};
use serde::{Deserialize, Serialize};

use crate::{
    arbitrage::ArbitrageDirection,
    dex::{
        execution::{
            AllowanceRequirement, DexProtocol, ExactInputSwapRequest, SwapRoute,
            SwapSubmissionPolicy,
        },
        hydration::{HydratedPool, PoolIdentity},
        pool_id::V4PoolKey,
    },
    domain::config::PairConfig,
    opportunity::TradeEvaluation,
};

pub const DEX_PLAN_TTL_SECONDS: u64 = 30;

/// Exact startup-only allowance set for the first reviewed Linea Lynex route.
/// Building this value is read-only; P6 never passes it to an executor capable
/// of approval writes. P8 may do so only under the durable startup gate.
pub fn linea_lynex_allowance_requirements(
    pair: &PairConfig,
) -> anyhow::Result<[AllowanceRequirement; 2]> {
    ensure!(
        pair.id == "linea-usdt-usdc",
        "unexpected Lynex allowance pair"
    );
    ensure!(
        pair.chain.chain_id == 59_144,
        "Lynex allowance pair is not on Linea"
    );
    ensure!(
        pair.dex
            .allowed_providers
            .contains(&crate::domain::config::DexProvider::LynexAlgebraV1_9),
        "Lynex allowance provider is not enabled"
    );
    let router = required_address(
        "lynex_algebra_v1_9_router_address",
        pair.chain.lynex_algebra_v1_9_router_address.as_deref(),
    )?;
    let token_a = parse_address("Linea Lynex token A", &pair.token_a.contract)?;
    let token_b = parse_address("Linea Lynex token B", &pair.token_b.contract)?;
    ensure!(
        token_a != token_b,
        "Linea Lynex allowance tokens are identical"
    );
    Ok([
        AllowanceRequirement {
            operation_id: "rustarb-linea-usdt-usdc-USDT-max-allowance".to_owned(),
            protocol: DexProtocol::LynexAlgebraV1_9,
            token: token_a,
            router,
            required: U256::MAX,
        },
        AllowanceRequirement {
            operation_id: "rustarb-linea-usdt-usdc-USDC-max-allowance".to_owned(),
            protocol: DexProtocol::LynexAlgebraV1_9,
            token: token_b,
            router,
            required: U256::MAX,
        },
    ])
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "protocol", rename_all = "snake_case")]
pub enum DexRoutePlan {
    UniswapV3 {
        router: String,
        pool_address: String,
        fee_pips: u32,
    },
    PancakeSwapV3 {
        router: String,
        pool_address: String,
        fee_pips: u32,
    },
    CamelotV3 {
        router: String,
        pool_address: String,
        pool_generation: u64,
        fee_generation: u64,
        fee_zto_current_pips: u16,
        fee_otz_current_pips: u16,
        fee_zto_envelope_pips: u16,
        fee_otz_envelope_pips: u16,
        fee_horizon_first_unix_seconds: u32,
        fee_horizon_last_unix_seconds: u32,
    },
    LynexAlgebraV1_9 {
        router: String,
        pool_address: String,
        pool_generation: u64,
        fee_generation: u64,
        fee_current_pips: u16,
        fee_envelope_pips: u16,
        fee_horizon_first_unix_seconds: u32,
        fee_horizon_last_unix_seconds: u32,
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
        pool_generation: u64,
        fee_generation: u64,
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
        let (token_in, token_out) = match direction {
            ArbitrageDirection::BuyTokenBOnDexSellOnCex => (token_a, token_b),
            ArbitrageDirection::BuyTokenBOnCexSellOnDex => (token_b, token_a),
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
            PoolIdentity::PancakeV3 { address, fee_pips } => DexRoutePlan::PancakeSwapV3 {
                router: required_address(
                    "pancakeswap_v3_router_address",
                    pair.chain.pancakeswap_v3_router_address.as_deref(),
                )?
                .to_string(),
                pool_address: address.to_string(),
                fee_pips,
            },
            PoolIdentity::CamelotV3 { address } => {
                ensure!(pool_generation > 0, "Camelot pool generation is zero");
                ensure!(fee_generation > 0, "Camelot fee generation is zero");
                let fee = pool
                    .camelot_fee
                    .as_ref()
                    .context("Camelot fee state is unavailable for execution planning")?;
                ensure!(
                    deadline_unix_seconds >= u64::from(fee.envelope.first_timestamp)
                        && deadline_unix_seconds <= u64::from(fee.envelope.last_timestamp),
                    "Camelot deadline is outside the prepared fee horizon"
                );
                DexRoutePlan::CamelotV3 {
                    router: required_address(
                        "camelot_v3_router_address",
                        pair.chain.camelot_v3_router_address.as_deref(),
                    )?
                    .to_string(),
                    pool_address: address.to_string(),
                    pool_generation,
                    fee_generation,
                    fee_zto_current_pips: fee.state.current_fees.zero_for_one,
                    fee_otz_current_pips: fee.state.current_fees.one_for_zero,
                    fee_zto_envelope_pips: fee.envelope.maximum.zero_for_one,
                    fee_otz_envelope_pips: fee.envelope.maximum.one_for_zero,
                    fee_horizon_first_unix_seconds: fee.envelope.first_timestamp,
                    fee_horizon_last_unix_seconds: fee.envelope.last_timestamp,
                }
            }
            PoolIdentity::LynexAlgebraV1_9 { address } => {
                ensure!(pool_generation > 0, "Lynex pool generation is zero");
                ensure!(fee_generation > 0, "Lynex fee generation is zero");
                let fee = pool
                    .lynex_fee
                    .as_ref()
                    .context("Lynex fee state is unavailable for execution planning")?;
                ensure!(
                    fee.state.current_fees.zero_for_one == fee.state.current_fees.one_for_zero
                        && fee.envelope.maximum.zero_for_one == fee.envelope.maximum.one_for_zero,
                    "Lynex execution fee state became directional"
                );
                ensure!(
                    deadline_unix_seconds >= u64::from(fee.envelope.first_timestamp)
                        && deadline_unix_seconds <= u64::from(fee.envelope.last_timestamp),
                    "Lynex deadline is outside the prepared fee horizon"
                );
                DexRoutePlan::LynexAlgebraV1_9 {
                    router: required_address(
                        "lynex_algebra_v1_9_router_address",
                        pair.chain.lynex_algebra_v1_9_router_address.as_deref(),
                    )?
                    .to_string(),
                    pool_address: address.to_string(),
                    pool_generation,
                    fee_generation,
                    fee_current_pips: fee.state.current_fees.zero_for_one,
                    fee_envelope_pips: fee.envelope.maximum.zero_for_one,
                    fee_horizon_first_unix_seconds: fee.envelope.first_timestamp,
                    fee_horizon_last_unix_seconds: fee.envelope.last_timestamp,
                }
            }
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
            amount_in_base_units: u128::try_from(trade.dex_amount_in)
                .context("DEX plan input exceeds u128")?,
            amount_out_minimum_base_units: u128::try_from(trade.dex_amount_out_minimum)
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
            DexRoutePlan::PancakeSwapV3 {
                router,
                pool_address,
                fee_pips,
            } => {
                parse_address("DEX plan Pancake V3 router", router)?;
                parse_address("DEX plan Pancake V3 pool", pool_address)?;
                ensure!(*fee_pips > 0, "DEX plan Pancake V3 fee is zero");
            }
            DexRoutePlan::CamelotV3 {
                router,
                pool_address,
                pool_generation,
                fee_generation,
                fee_zto_current_pips,
                fee_otz_current_pips,
                fee_zto_envelope_pips,
                fee_otz_envelope_pips,
                fee_horizon_first_unix_seconds,
                fee_horizon_last_unix_seconds,
            } => {
                parse_address("DEX plan Camelot V3 router", router)?;
                parse_address("DEX plan Camelot V3 pool", pool_address)?;
                ensure!(
                    *pool_generation > 0,
                    "DEX plan Camelot pool generation is zero"
                );
                ensure!(
                    *fee_generation > 0,
                    "DEX plan Camelot fee generation is zero"
                );
                ensure!(
                    fee_zto_envelope_pips >= fee_zto_current_pips
                        && fee_otz_envelope_pips >= fee_otz_current_pips,
                    "DEX plan Camelot envelope is below its current fee"
                );
                ensure!(
                    fee_horizon_last_unix_seconds >= fee_horizon_first_unix_seconds,
                    "DEX plan Camelot fee horizon is reversed"
                );
                ensure!(
                    self.deadline_unix_seconds >= u64::from(*fee_horizon_first_unix_seconds)
                        && self.deadline_unix_seconds <= u64::from(*fee_horizon_last_unix_seconds),
                    "DEX plan Camelot deadline is outside its fee horizon"
                );
            }
            DexRoutePlan::LynexAlgebraV1_9 {
                router,
                pool_address,
                pool_generation,
                fee_generation,
                fee_current_pips,
                fee_envelope_pips,
                fee_horizon_first_unix_seconds,
                fee_horizon_last_unix_seconds,
            } => {
                parse_address("DEX plan Lynex router", router)?;
                parse_address("DEX plan Lynex pool", pool_address)?;
                ensure!(
                    *pool_generation > 0,
                    "DEX plan Lynex pool generation is zero"
                );
                ensure!(*fee_generation > 0, "DEX plan Lynex fee generation is zero");
                ensure!(
                    fee_envelope_pips >= fee_current_pips,
                    "DEX plan Lynex envelope is below its current fee"
                );
                ensure!(
                    fee_horizon_last_unix_seconds >= fee_horizon_first_unix_seconds,
                    "DEX plan Lynex fee horizon is reversed"
                );
                ensure!(
                    self.deadline_unix_seconds >= u64::from(*fee_horizon_first_unix_seconds)
                        && self.deadline_unix_seconds <= u64::from(*fee_horizon_last_unix_seconds),
                    "DEX plan Lynex deadline is outside its fee horizon"
                );
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
    ) -> anyhow::Result<ExactInputSwapRequest> {
        self.validate()?;
        let route = match &self.route {
            DexRoutePlan::UniswapV3 {
                router,
                pool_address,
                fee_pips,
            } => SwapRoute::UniswapV3 {
                router: parse_address("DEX plan V3 router", router)?,
                pool: parse_address("DEX plan V3 pool", pool_address)?,
                fee_pips: *fee_pips,
            },
            DexRoutePlan::PancakeSwapV3 {
                router,
                pool_address,
                fee_pips,
            } => SwapRoute::PancakeSwapV3 {
                router: parse_address("DEX plan Pancake V3 router", router)?,
                pool: parse_address("DEX plan Pancake V3 pool", pool_address)?,
                fee_pips: *fee_pips,
            },
            DexRoutePlan::CamelotV3 {
                router,
                pool_address,
                ..
            } => SwapRoute::CamelotV3 {
                router: parse_address("DEX plan Camelot V3 router", router)?,
                pool: parse_address("DEX plan Camelot V3 pool", pool_address)?,
            },
            DexRoutePlan::LynexAlgebraV1_9 {
                router,
                pool_address,
                ..
            } => SwapRoute::LynexAlgebraV1_9 {
                router: parse_address("DEX plan Lynex router", router)?,
                pool: parse_address("DEX plan Lynex pool", pool_address)?,
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
    use std::hint::black_box;

    use alloy_primitives::{Address, U256};

    use crate::dex::{
        execution::{DexProtocol, SwapRoute, SwapSubmissionPolicy},
        pool_id::V4PoolKey,
    };
    use crate::domain::config::LoadedDomainConfig;
    use crate::paired_benchmark::{
        assert_named_paired_non_regression, assert_paired_non_regression,
    };

    use super::{DexRoutePlan, DexSwapPlan, linea_lynex_allowance_requirements};

    #[test]
    fn linea_lynex_allowance_set_is_exact_provider_scoped_and_read_only_to_build() {
        let domain =
            LoadedDomainConfig::load("config/strategies/usdt-usdc-linea-lynex.v1.json").unwrap();
        let requirements = linea_lynex_allowance_requirements(&domain.snapshot().pairs[0]).unwrap();
        assert_eq!(requirements.len(), 2);
        assert_eq!(requirements[0].protocol, DexProtocol::LynexAlgebraV1_9);
        assert_eq!(requirements[1].protocol, DexProtocol::LynexAlgebraV1_9);
        assert_eq!(requirements[0].required, U256::MAX);
        assert_eq!(requirements[1].required, U256::MAX);
        assert_ne!(requirements[0].token, requirements[1].token);
        assert_eq!(requirements[0].router, requirements[1].router);
        assert_eq!(
            requirements[0].router,
            "0x3921e8cb45B17fC029A0a6dE958330ca4e583390"
                .parse::<Address>()
                .unwrap()
        );
    }

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

        let request = plan.execution_request("rustarb-plan.dex").unwrap();
        assert_eq!(request.amount_in, U256::from(10_000_000_u64));
        assert_eq!(request.amount_out_minimum, U256::from(9_000_000_u64));
        assert_eq!(request.submission_policy, SwapSubmissionPolicy::Immediate);
    }

    #[test]
    fn pancake_v3_plan_round_trips_without_crossing_provider_identity() {
        let router = Address::repeat_byte(0x11);
        let pool = Address::repeat_byte(0x22);
        let plan = DexSwapPlan {
            route: DexRoutePlan::PancakeSwapV3 {
                router: router.to_string(),
                pool_address: pool.to_string(),
                fee_pips: 500,
            },
            token_in: Address::repeat_byte(0x33).to_string(),
            token_out: Address::repeat_byte(0x44).to_string(),
            amount_in_base_units: 6_000_000,
            amount_out_minimum_base_units: 5_000_000,
            deadline_unix_seconds: 1_900_000_000,
        };

        let request = plan.execution_request("rustarb-pancake-plan.dex").unwrap();
        assert!(matches!(
            request.route,
            SwapRoute::PancakeSwapV3 {
                router: actual_router,
                pool: actual_pool,
                fee_pips: 500,
            } if actual_router == router && actual_pool == pool
        ));
        assert_eq!(request.submission_policy, SwapSubmissionPolicy::Immediate);
    }

    fn camelot_plan() -> DexSwapPlan {
        DexSwapPlan {
            route: DexRoutePlan::CamelotV3 {
                router: Address::repeat_byte(0x11).to_string(),
                pool_address: Address::repeat_byte(0x22).to_string(),
                pool_generation: 7,
                fee_generation: 6,
                fee_zto_current_pips: 104,
                fee_otz_current_pips: 105,
                fee_zto_envelope_pips: 117,
                fee_otz_envelope_pips: 118,
                fee_horizon_first_unix_seconds: 1_900_000_000,
                fee_horizon_last_unix_seconds: 1_900_000_002,
            },
            token_in: Address::repeat_byte(0x33).to_string(),
            token_out: Address::repeat_byte(0x44).to_string(),
            amount_in_base_units: 6_000_000,
            amount_out_minimum_base_units: 5_000_000,
            deadline_unix_seconds: 1_900_000_002,
        }
    }

    fn lynex_plan() -> DexSwapPlan {
        DexSwapPlan {
            route: DexRoutePlan::LynexAlgebraV1_9 {
                router: "0x3921e8cb45B17fC029A0a6dE958330ca4e583390".to_owned(),
                pool_address: "0x6e9ad0b8a41e2c148e7b0385d3ecbfdb8a216a9b".to_owned(),
                pool_generation: 8,
                fee_generation: 7,
                fee_current_pips: 50,
                fee_envelope_pips: 50,
                fee_horizon_first_unix_seconds: 1_900_000_000,
                fee_horizon_last_unix_seconds: 1_900_000_002,
            },
            token_in: "0xA219439258ca9da29e9cC4cE5596924745e12B93".to_owned(),
            token_out: "0x176211869cA2b568f2A7D4EE941E073a821EE1ff".to_owned(),
            amount_in_base_units: 6_000_000,
            amount_out_minimum_base_units: 5_900_000,
            deadline_unix_seconds: 1_900_000_002,
        }
    }

    #[test]
    fn lynex_plan_binds_single_fee_horizon_and_provider_identity() {
        let plan = lynex_plan();
        let encoded = serde_json::to_value(&plan).unwrap();
        assert_eq!(encoded["route"]["protocol"], "lynex_algebra_v1_9");
        assert_eq!(encoded["route"]["fee_generation"], 7);
        assert_eq!(encoded["route"]["fee_current_pips"], 50);
        assert_eq!(encoded["route"]["fee_envelope_pips"], 50);

        let request = plan.execution_request("rustarb-lynex-plan.dex").unwrap();
        assert!(matches!(
            request.route,
            SwapRoute::LynexAlgebraV1_9 { router, pool }
                if router
                    == "0x3921e8cb45B17fC029A0a6dE958330ca4e583390"
                        .parse::<Address>()
                        .unwrap()
                    && pool
                        == "0x6e9ad0b8a41e2c148e7b0385d3ecbfdb8a216a9b"
                            .parse::<Address>()
                            .unwrap()
        ));

        let mut expired = plan;
        expired.deadline_unix_seconds = 1_900_000_003;
        assert!(expired.validate().is_err());
    }

    #[test]
    #[ignore = "manual release-mode paired Lynex/Uniswap durable-plan benchmark"]
    fn benchmark_uniswap_and_lynex_plan_materialization() {
        let uniswap = DexSwapPlan {
            route: DexRoutePlan::UniswapV3 {
                router: Address::repeat_byte(0x11).to_string(),
                pool_address: Address::repeat_byte(0x22).to_string(),
                fee_pips: 500,
            },
            token_in: Address::repeat_byte(0x33).to_string(),
            token_out: Address::repeat_byte(0x44).to_string(),
            amount_in_base_units: 6_000_000,
            amount_out_minimum_base_units: 5_900_000,
            deadline_unix_seconds: 1_900_000_002,
        };
        let lynex = lynex_plan();
        assert_named_paired_non_regression(
            "lynex_algebra_v1_9_plan_materialization_benchmark",
            1.10,
            "uniswap_v3",
            "lynex_algebra_v1_9",
            || {
                black_box(uniswap.execution_request("bench-uniswap")).unwrap();
            },
            || {
                black_box(lynex.execution_request("bench-lynex")).unwrap();
            },
        );
    }

    #[test]
    fn camelot_plan_binds_fee_horizon_and_provider_identity() {
        let plan = camelot_plan();
        let encoded = serde_json::to_value(&plan).unwrap();
        assert_eq!(encoded["route"]["protocol"], "camelot_v3");
        assert_eq!(encoded["route"]["fee_generation"], 6);
        assert_eq!(encoded["route"]["fee_zto_envelope_pips"], 117);
        assert_eq!(
            encoded["route"]["fee_horizon_last_unix_seconds"],
            1_900_000_002_u64
        );

        let request = plan.execution_request("rustarb-camelot-plan.dex").unwrap();
        assert!(matches!(
            request.route,
            SwapRoute::CamelotV3 { router, pool }
                if router == Address::repeat_byte(0x11)
                    && pool == Address::repeat_byte(0x22)
        ));
        assert_eq!(request.deadline_unix_seconds, 1_900_000_002);

        let mut outside_horizon = plan;
        outside_horizon.deadline_unix_seconds = 1_900_000_003;
        assert!(outside_horizon.validate().is_err());
    }

    #[test]
    #[ignore = "manual release-mode paired Camelot/Uniswap durable-plan benchmark"]
    fn benchmark_uniswap_and_camelot_v3_plan_materialization() {
        let uniswap = DexSwapPlan {
            route: DexRoutePlan::UniswapV3 {
                router: Address::repeat_byte(0x11).to_string(),
                pool_address: Address::repeat_byte(0x22).to_string(),
                fee_pips: 500,
            },
            token_in: Address::repeat_byte(0x33).to_string(),
            token_out: Address::repeat_byte(0x44).to_string(),
            amount_in_base_units: 6_000_000,
            amount_out_minimum_base_units: 5_000_000,
            deadline_unix_seconds: 1_900_000_000,
        };
        let camelot = camelot_plan();
        assert_named_paired_non_regression(
            "camelot_v3_plan_materialization_benchmark",
            1.10,
            "uniswap_v3",
            "camelot_v3",
            || {
                black_box(uniswap.execution_request("bench-uniswap")).unwrap();
            },
            || {
                black_box(camelot.execution_request("bench-camelot")).unwrap();
            },
        );
    }

    #[test]
    #[ignore = "manual release-mode paired V3 durable-plan benchmark"]
    fn benchmark_uniswap_and_pancake_v3_plan_materialization() {
        let common = |route| DexSwapPlan {
            route,
            token_in: Address::repeat_byte(0x33).to_string(),
            token_out: Address::repeat_byte(0x44).to_string(),
            amount_in_base_units: 6_000_000,
            amount_out_minimum_base_units: 5_000_000,
            deadline_unix_seconds: 1_900_000_000,
        };
        let uniswap = common(DexRoutePlan::UniswapV3 {
            router: Address::repeat_byte(0x11).to_string(),
            pool_address: Address::repeat_byte(0x22).to_string(),
            fee_pips: 500,
        });
        let pancake = common(DexRoutePlan::PancakeSwapV3 {
            router: Address::repeat_byte(0x11).to_string(),
            pool_address: Address::repeat_byte(0x22).to_string(),
            fee_pips: 500,
        });
        assert_paired_non_regression(
            "v3_plan_materialization_benchmark",
            1.10,
            || {
                black_box(uniswap.execution_request("bench-uniswap")).unwrap();
            },
            || {
                black_box(pancake.execution_request("bench-pancake")).unwrap();
            },
        );
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

        let request = plan.execution_request("rustarb-plan-v4.dex").unwrap();
        assert_eq!(request.amount_in, U256::from(10_000_000_u64));
        assert_eq!(request.amount_out_minimum, U256::from(9_000_000_u64));
        assert_eq!(request.submission_policy, SwapSubmissionPolicy::Immediate);
        assert!(matches!(
            request.route,
            SwapRoute::V4 { pool_key, .. }
                if pool_key.hooks == Address::ZERO && pool_key.pool_id() == key.pool_id()
        ));
    }
}

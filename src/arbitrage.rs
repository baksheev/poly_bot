use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions, symlink_metadata},
    io::{BufRead, BufReader, Write},
    path::Path,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

use alloy_primitives::U256;
use anyhow::{Context, ensure};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::{Notify, mpsc};

use crate::{
    admission::EXPECTED_PROFIT_AFTER_GAS_THRESHOLD_BPS, execution_plan::DexSwapPlan,
    state::TopOfBook, telemetry::TelemetryHandle,
};

const JOURNAL_VERSION: u16 = 1;
const MAX_LINE_BYTES: usize = 64 * 1024;
const MAX_RECOVERY_ATTEMPTS: usize = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    DexFirst,
    ConcurrentHedged,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArbitrageDirection {
    BuyTokenBOnDexSellOnCex,
    BuyTokenBOnCexSellOnDex,
}

impl ArbitrageDirection {
    const fn dex_token_b_delta(self, amount: i128) -> i128 {
        match self {
            Self::BuyTokenBOnDexSellOnCex => amount,
            Self::BuyTokenBOnCexSellOnDex => -amount,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TradeIntent {
    pub plan_id: String,
    pub source_revision: String,
    pub pair_id: String,
    pub mode: ExecutionMode,
    pub direction: ArbitrageDirection,
    pub planned_token_b_base_units: i128,
    #[serde(default)]
    pub token_b_step_base_units: i128,
    pub expected_cost_token_a_base_units: i128,
    pub expected_proceeds_token_a_base_units: i128,
    pub dex_operation_id: String,
    pub cex_client_order_id: String,
    /// Present for newly admitted work. `None` is accepted only so version-1
    /// paper journals written before bounded admission remain recoverable.
    #[serde(default)]
    pub admission: Option<AdmissionRiskBounds>,
    #[serde(default)]
    pub dex_plan: Option<DexSwapPlan>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AdmissionRiskBounds {
    /// Persisted proof that the exact candidate crossed the configured gross
    /// venue-spread threshold before admission.
    #[serde(default, skip_serializing_if = "is_false")]
    pub opportunity_threshold_met: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depth_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depth_age_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depth_update_delta: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_matches: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_mismatch_reason: Option<String>,
    pub execution_slippage_bps: u16,
    pub cex_primary_limit_price: Decimal,
    #[serde(default, skip_serializing_if = "is_zero_decimal")]
    /// Non-zero only when admission was proven entirely from the relevant
    /// bookTicker level. Full-depth fallback cannot be revalidated from a
    /// top-of-book snapshot alone.
    pub cex_primary_top_quantity: Decimal,
    pub cex_recovery_limit_price: Decimal,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cex_recovery_sell_limit_price: Option<Decimal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cex_recovery_buy_limit_price: Option<Decimal>,
    pub recovery_quote_token_a_base_units: u128,
    #[serde(default, skip_serializing_if = "is_zero_u128")]
    pub recovery_sell_quote_token_a_base_units: u128,
    #[serde(default, skip_serializing_if = "is_zero_u128")]
    pub recovery_buy_quote_token_a_base_units: u128,
    pub maximum_recovery_loss_token_a_base_units: u128,
    pub maximum_fee_per_gas_wei: u128,
    pub gas_conversion_price_token_a: Decimal,
    pub maximum_gas_cost_token_a_base_units: u128,
    pub bounded_profit_token_a_base_units: u128,
}

impl AdmissionRiskBounds {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.depth_source.as_deref().is_none_or(|source| matches!(
                source,
                "sequence_matched_full_depth" | "recent_full_depth" | "top_of_book_only"
            )),
            "admission depth source is invalid"
        );
        ensure!(
            self.top_matches != Some(true) || self.top_mismatch_reason.is_none(),
            "matching admission top cannot have a mismatch reason"
        );
        ensure!(
            self.execution_slippage_bps <= 10_000,
            "execution slippage exceeds 100%"
        );
        ensure!(
            self.cex_primary_limit_price > Decimal::ZERO,
            "CEX primary limit price is non-positive"
        );
        ensure!(
            self.cex_primary_top_quantity >= Decimal::ZERO,
            "CEX primary quantity is negative"
        );
        ensure!(
            self.cex_recovery_limit_price > Decimal::ZERO,
            "CEX recovery limit price is non-positive"
        );
        ensure!(
            self.cex_recovery_sell_limit_price
                .is_none_or(|price| price > Decimal::ZERO)
                && self
                    .cex_recovery_buy_limit_price
                    .is_none_or(|price| price > Decimal::ZERO),
            "directional CEX recovery limit price is non-positive"
        );
        ensure!(
            self.recovery_quote_token_a_base_units > 0,
            "CEX recovery quote is zero"
        );
        ensure!(
            self.maximum_fee_per_gas_wei > 0,
            "maximum fee per gas is zero"
        );
        ensure!(
            self.gas_conversion_price_token_a > Decimal::ZERO,
            "gas conversion price is non-positive"
        );
        ensure!(
            self.maximum_gas_cost_token_a_base_units > 0,
            "maximum gas cost is zero"
        );
        Ok(())
    }

    fn recovery_quote_for_residual(&self, residual_token_b_base_units: i128) -> u128 {
        if residual_token_b_base_units > 0 {
            if self.recovery_sell_quote_token_a_base_units > 0 {
                self.recovery_sell_quote_token_a_base_units
            } else {
                self.recovery_quote_token_a_base_units
            }
        } else if self.recovery_buy_quote_token_a_base_units > 0 {
            self.recovery_buy_quote_token_a_base_units
        } else {
            self.recovery_quote_token_a_base_units
        }
    }
}

const fn is_zero_u128(value: &u128) -> bool {
    *value == 0
}

const fn is_false(value: &bool) -> bool {
    !*value
}

fn is_zero_decimal(value: &Decimal) -> bool {
    *value == Decimal::ZERO
}

const fn is_zero_i128(value: &i128) -> bool {
    *value == 0
}

impl TradeIntent {
    pub fn expected_profit_token_a_base_units(&self) -> i128 {
        self.expected_proceeds_token_a_base_units
            .saturating_sub(self.expected_cost_token_a_base_units)
    }

    fn expected_recovery_loss_token_a_base_units(&self) -> Option<i128> {
        self.admission.as_ref().map(|admission| {
            u128_to_i128_saturating(admission.maximum_recovery_loss_token_a_base_units)
        })
    }

    fn expected_gas_cost_token_a_base_units(&self) -> Option<i128> {
        self.admission
            .as_ref()
            .map(|admission| u128_to_i128_saturating(admission.maximum_gas_cost_token_a_base_units))
    }

    fn expected_gas_burdened_cost_token_a_base_units(&self) -> Option<i128> {
        let admission = self.admission.as_ref()?;
        Some(
            self.expected_cost_token_a_base_units
                .saturating_add(u128_to_i128_saturating(
                    admission.maximum_gas_cost_token_a_base_units,
                )),
        )
    }

    fn expected_fully_burdened_cost_token_a_base_units(&self) -> Option<i128> {
        let admission = self.admission.as_ref()?;
        Some(
            self.expected_cost_token_a_base_units
                .saturating_add(u128_to_i128_saturating(
                    admission.maximum_recovery_loss_token_a_base_units,
                ))
                .saturating_add(u128_to_i128_saturating(
                    admission.maximum_gas_cost_token_a_base_units,
                )),
        )
    }

    fn expected_bounded_profit_token_a_base_units(&self) -> Option<i128> {
        self.admission
            .as_ref()
            .map(|admission| u128_to_i128_saturating(admission.bounded_profit_token_a_base_units))
    }

    fn expected_profit_after_gas_token_a_base_units(&self) -> Option<i128> {
        self.expected_bounded_profit_token_a_base_units()
    }

    fn validate(&self) -> anyhow::Result<()> {
        validate_id("plan id", &self.plan_id, 96)?;
        validate_id("source revision", &self.source_revision, 96)?;
        validate_id("pair id", &self.pair_id, 96)?;
        validate_id("DEX operation id", &self.dex_operation_id, 120)?;
        validate_id("CEX client order id", &self.cex_client_order_id, 36)?;
        ensure!(
            self.planned_token_b_base_units > 0,
            "planned token-B amount must be positive"
        );
        if self.admission.is_some() {
            ensure!(
                self.token_b_step_base_units > 0,
                "newly admitted token-B step must be positive"
            );
        } else {
            ensure!(
                self.token_b_step_base_units >= 0,
                "historical token-B step cannot be negative"
            );
        }
        ensure!(
            self.expected_cost_token_a_base_units > 0,
            "expected token-A cost must be positive"
        );
        ensure!(
            self.expected_proceeds_token_a_base_units > 0,
            "expected token-A proceeds must be positive"
        );
        if let Some(admission) = &self.admission {
            admission.validate()?;
        }
        if let Some(plan) = &self.dex_plan {
            plan.validate()?;
        }
        Ok(())
    }
}

fn u128_to_i128_saturating(value: u128) -> i128 {
    i128::try_from(value).unwrap_or(i128::MAX)
}

fn optional_i128_string(value: Option<i128>) -> Option<String> {
    value.map(|value| value.to_string())
}

fn profit_bps_x100(profit: i128, cost: i128) -> Option<i128> {
    if cost <= 0 {
        return None;
    }
    Some(profit.saturating_mul(1_000_000).saturating_div(cost))
}

fn meets_expected_profit_after_gas_threshold(
    proceeds_token_a_base_units: i128,
    cost_token_a_base_units: i128,
    gas_cost_token_a_base_units: u128,
) -> anyhow::Result<bool> {
    let proceeds = u128::try_from(proceeds_token_a_base_units)
        .context("expected proceeds cannot be represented as unsigned base units")?;
    let cost = u128::try_from(cost_token_a_base_units)
        .context("expected cost cannot be represented as unsigned base units")?;
    let gas_burdened_cost = cost
        .checked_add(gas_cost_token_a_base_units)
        .context("expected after-gas cost overflow")?;
    let left = proceeds
        .checked_mul(10_000)
        .context("expected after-gas proceeds threshold overflow")?;
    let right = gas_burdened_cost
        .checked_mul(10_000 + u128::from(EXPECTED_PROFIT_AFTER_GAS_THRESHOLD_BPS))
        .context("expected after-gas threshold cost overflow")?;
    Ok(left >= right)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegRole {
    Dex,
    Cex,
    RecoveryCex,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegStatus {
    Filled,
    Failed,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LegResult {
    pub status: LegStatus,
    /// Signed venue balance delta: bought token B is positive, sold token B negative.
    pub token_b_delta_base_units: i128,
    /// Signed venue balance delta in token A, excluding gas.
    pub token_a_delta_base_units: i128,
    /// Gas converted to token A at the terminal accounting snapshot.
    pub gas_cost_token_a_base_units: u128,
    pub venue_reference: String,
}

impl LegResult {
    fn validate(&self) -> anyhow::Result<()> {
        validate_id("venue reference", &self.venue_reference, 128)?;
        if self.status == LegStatus::Failed {
            ensure!(
                self.token_b_delta_base_units == 0 && self.token_a_delta_base_units == 0,
                "a failed leg cannot claim venue balance changes"
            );
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalOutcome {
    BalancedProfit,
    BalancedLoss,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ArbitrageResult {
    pub expected_profit_token_a_base_units: i128,
    pub realized_profit_token_a_base_units: i128,
    #[serde(default, skip_serializing_if = "is_zero_i128")]
    pub residual_value_token_a_base_units: i128,
    #[serde(default, skip_serializing_if = "is_zero_i128")]
    pub comparable_profit_token_a_base_units: i128,
    pub token_b_residual_base_units: i128,
    pub gas_cost_token_a_base_units: u128,
    pub recovery_loss_token_a_base_units: i128,
    pub outcome: TerminalOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradeStage {
    Prepared,
    Executing,
    Recovering,
    BalancedProfit,
    BalancedLoss,
    UnknownExposure,
    Halted,
}

impl TradeStage {
    pub const fn terminal(&self) -> bool {
        matches!(self, Self::BalancedProfit | Self::BalancedLoss)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TradeOperation {
    pub intent: TradeIntent,
    pub stage: TradeStage,
    pub dex_dispatched: bool,
    pub cex_dispatched: bool,
    pub dex_result: Option<LegResult>,
    pub cex_result: Option<LegResult>,
    pub recovery_results: Vec<LegResult>,
    pub recovery_inflight: bool,
    pub result: Option<ArbitrageResult>,
    pub blocking_reason: Option<String>,
}

impl TradeOperation {
    fn prepared(intent: TradeIntent) -> Self {
        Self {
            intent,
            stage: TradeStage::Prepared,
            dex_dispatched: false,
            cex_dispatched: false,
            dex_result: None,
            cex_result: None,
            recovery_results: Vec::new(),
            recovery_inflight: false,
            result: None,
            blocking_reason: None,
        }
    }

    pub fn token_b_residual_base_units(&self) -> i128 {
        self.dex_result
            .iter()
            .chain(self.cex_result.iter())
            .chain(self.recovery_results.iter())
            .fold(0_i128, |total, result| {
                total.saturating_add(result.token_b_delta_base_units)
            })
    }

    fn actionable_token_b_residual_base_units(&self) -> i128 {
        let residual = self.token_b_residual_base_units();
        let step = self.intent.token_b_step_base_units;
        if step > 0 && residual.unsigned_abs() < step as u128 {
            0
        } else {
            residual
        }
    }

    fn realized_profit_token_a_base_units(&self) -> (i128, u128) {
        let mut token_a = 0_i128;
        let mut gas = 0_u128;
        for result in self
            .dex_result
            .iter()
            .chain(self.cex_result.iter())
            .chain(self.recovery_results.iter())
        {
            token_a = token_a.saturating_add(result.token_a_delta_base_units);
            gas = gas.saturating_add(result.gas_cost_token_a_base_units);
        }
        let gas_i128 = i128::try_from(gas).unwrap_or(i128::MAX);
        (token_a.saturating_sub(gas_i128), gas)
    }

    fn recovery_token_a_delta_base_units(&self) -> i128 {
        self.recovery_results.iter().fold(0_i128, |total, result| {
            total.saturating_add(result.token_a_delta_base_units)
        })
    }

    fn realized_primary_cost_and_proceeds_token_a_base_units(&self) -> Option<(i128, i128)> {
        let dex = self.dex_result.as_ref()?;
        let cex = self.cex_result.as_ref()?;
        let (cost, proceeds) = match self.intent.direction {
            ArbitrageDirection::BuyTokenBOnDexSellOnCex => (
                dex.token_a_delta_base_units.saturating_neg(),
                cex.token_a_delta_base_units,
            ),
            ArbitrageDirection::BuyTokenBOnCexSellOnDex => (
                cex.token_a_delta_base_units.saturating_neg(),
                dex.token_a_delta_base_units,
            ),
        };
        Some((cost.max(0), proceeds.max(0)))
    }

    fn has_unknown_leg(&self) -> bool {
        self.dex_result
            .iter()
            .chain(self.cex_result.iter())
            .chain(self.recovery_results.iter())
            .any(|result| result.status == LegStatus::Unknown)
    }

    pub fn result_telemetry_payload(&self, engine_id: &str) -> anyhow::Result<Value> {
        validate_id("engine id", engine_id, 96)?;
        let result = self
            .result
            .as_ref()
            .context("trade has no terminal arbitrage result")?;
        ensure!(
            matches!(
                self.stage,
                TradeStage::BalancedProfit | TradeStage::BalancedLoss
            ),
            "arbitrage result is not balanced"
        );
        let expected_profit = self.intent.expected_profit_token_a_base_units();
        let expected_recovery_loss = self.intent.expected_recovery_loss_token_a_base_units();
        let expected_gas_cost = self.intent.expected_gas_cost_token_a_base_units();
        let expected_gas_burdened_cost =
            self.intent.expected_gas_burdened_cost_token_a_base_units();
        let expected_fully_burdened_cost = self
            .intent
            .expected_fully_burdened_cost_token_a_base_units();
        let expected_bounded_profit = self.intent.expected_bounded_profit_token_a_base_units();
        let expected_profit_after_gas = self.intent.expected_profit_after_gas_token_a_base_units();
        let expected_profit_bps_x100 = profit_bps_x100(
            expected_profit,
            self.intent.expected_cost_token_a_base_units,
        );
        let expected_profit_after_gas_bps_x100 =
            expected_gas_burdened_cost.and_then(|expected_cost| {
                expected_profit_after_gas
                    .and_then(|expected_profit| profit_bps_x100(expected_profit, expected_cost))
            });
        let expected_bounded_profit_bps_x100 = expected_profit_after_gas_bps_x100;
        let (realized_primary_cost, realized_primary_proceeds) = self
            .realized_primary_cost_and_proceeds_token_a_base_units()
            .map_or((None, None), |(cost, proceeds)| {
                (Some(cost), Some(proceeds))
            });
        let realized_primary_profit = realized_primary_cost
            .zip(realized_primary_proceeds)
            .map(|(cost, proceeds)| proceeds.saturating_sub(cost));
        let realized_recovery_delta = self.recovery_token_a_delta_base_units();
        let realized_recovery_loss = result.recovery_loss_token_a_base_units;
        let realized_gas_cost = u128_to_i128_saturating(result.gas_cost_token_a_base_units);
        let realized_total_cost = realized_primary_cost.map(|cost| {
            cost.saturating_add(realized_recovery_loss)
                .saturating_add(realized_gas_cost)
        });
        let realized_bounded_profit = realized_primary_proceeds
            .zip(realized_total_cost)
            .map(|(proceeds, total_cost)| proceeds.saturating_sub(total_cost));
        let realized_primary_profit_bps_x100 = realized_primary_cost.and_then(|cost| {
            realized_primary_profit.and_then(|profit| profit_bps_x100(profit, cost))
        });
        let realized_bounded_profit_bps_x100 = realized_total_cost.and_then(|cost| {
            realized_bounded_profit.and_then(|profit| profit_bps_x100(profit, cost))
        });
        let primary_profit_error =
            realized_primary_profit.map(|realized| realized.saturating_sub(expected_profit));
        let bounded_profit_error = realized_bounded_profit
            .zip(expected_bounded_profit)
            .map(|(realized, expected)| realized.saturating_sub(expected));
        let gas_cost_error =
            expected_gas_cost.map(|expected| realized_gas_cost.saturating_sub(expected));
        let recovery_loss_error =
            expected_recovery_loss.map(|expected| realized_recovery_loss.saturating_sub(expected));
        let comparable_profit_vs_expected_bounded_error = expected_bounded_profit.map(|expected| {
            result
                .comparable_profit_token_a_base_units
                .saturating_sub(expected)
        });
        let mut payload = json!({
            "engine_id": engine_id,
            "plan_id": self.intent.plan_id,
            "source_revision": self.intent.source_revision,
            "pair_id": self.intent.pair_id,
            "execution_mode": enum_json(&self.intent.mode)?,
            "direction": enum_json(&self.intent.direction)?,
            "outcome": enum_json(&result.outcome)?,
            "expected_cost_token_a_base_units": self.intent.expected_cost_token_a_base_units.to_string(),
            "expected_proceeds_token_a_base_units": self.intent.expected_proceeds_token_a_base_units.to_string(),
            "expected_profit_token_a_base_units": result.expected_profit_token_a_base_units.to_string(),
            "expected_profit_bps_x100": optional_i128_string(expected_profit_bps_x100),
            "expected_recovery_loss_token_a_base_units": optional_i128_string(expected_recovery_loss),
            "expected_gas_cost_token_a_base_units": optional_i128_string(expected_gas_cost),
            "expected_fully_burdened_cost_token_a_base_units": optional_i128_string(expected_fully_burdened_cost),
            "expected_bounded_profit_token_a_base_units": optional_i128_string(expected_bounded_profit),
            "expected_bounded_profit_bps_x100": optional_i128_string(expected_bounded_profit_bps_x100),
            "realized_primary_cost_token_a_base_units": optional_i128_string(realized_primary_cost),
            "realized_primary_proceeds_token_a_base_units": optional_i128_string(realized_primary_proceeds),
            "realized_primary_profit_token_a_base_units": optional_i128_string(realized_primary_profit),
            "realized_primary_profit_bps_x100": optional_i128_string(realized_primary_profit_bps_x100),
            "realized_recovery_token_a_delta_base_units": realized_recovery_delta.to_string(),
            "realized_total_cost_token_a_base_units": optional_i128_string(realized_total_cost),
            "realized_bounded_profit_token_a_base_units": optional_i128_string(realized_bounded_profit),
            "realized_bounded_profit_bps_x100": optional_i128_string(realized_bounded_profit_bps_x100),
            "realized_profit_token_a_base_units": result.realized_profit_token_a_base_units.to_string(),
            "residual_value_token_a_base_units": result.residual_value_token_a_base_units.to_string(),
            "comparable_profit_token_a_base_units": result.comparable_profit_token_a_base_units.to_string(),
            "token_b_residual_base_units": result.token_b_residual_base_units.to_string(),
            "gas_cost_token_a_base_units": result.gas_cost_token_a_base_units.to_string(),
            "recovery_loss_token_a_base_units": result.recovery_loss_token_a_base_units.to_string(),
            "cost_model_primary_profit_error_token_a_base_units": optional_i128_string(primary_profit_error),
            "cost_model_bounded_profit_error_token_a_base_units": optional_i128_string(bounded_profit_error),
            "cost_model_gas_cost_error_token_a_base_units": optional_i128_string(gas_cost_error),
            "cost_model_recovery_loss_error_token_a_base_units": optional_i128_string(recovery_loss_error),
            "cost_model_comparable_profit_vs_expected_bounded_error_token_a_base_units": optional_i128_string(comparable_profit_vs_expected_bounded_error),
            "dex": self.dex_result.as_ref().map(leg_payload),
            "cex": self.cex_result.as_ref().map(leg_payload),
            "recoveries": self.recovery_results.iter().map(leg_payload).collect::<Vec<_>>(),
        });
        let object = payload
            .as_object_mut()
            .context("arbitrage result telemetry payload is not an object")?;
        let admission = self.intent.admission.as_ref();
        object.insert(
            "recovery_loss_bound_token_a_base_units".to_owned(),
            json!(optional_i128_string(expected_recovery_loss)),
        );
        object.insert(
            "expected_gas_burdened_cost_token_a_base_units".to_owned(),
            json!(optional_i128_string(expected_gas_burdened_cost)),
        );
        object.insert(
            "expected_profit_after_gas_token_a_base_units".to_owned(),
            json!(optional_i128_string(expected_profit_after_gas)),
        );
        object.insert(
            "expected_profit_after_gas_bps_x100".to_owned(),
            json!(optional_i128_string(expected_profit_after_gas_bps_x100)),
        );
        object.insert(
            "depth_source".to_owned(),
            json!(admission.and_then(|admission| admission.depth_source.as_deref())),
        );
        object.insert(
            "depth_age_ms".to_owned(),
            json!(admission.and_then(|admission| admission.depth_age_ms)),
        );
        object.insert(
            "depth_update_delta".to_owned(),
            json!(admission.and_then(|admission| admission.depth_update_delta)),
        );
        object.insert(
            "top_matches".to_owned(),
            json!(admission.and_then(|admission| admission.top_matches)),
        );
        object.insert(
            "top_mismatch_reason".to_owned(),
            json!(admission.and_then(|admission| admission.top_mismatch_reason.as_deref())),
        );
        Ok(payload)
    }
}

fn enum_json<T: Serialize>(value: &T) -> anyhow::Result<String> {
    serde_json::to_value(value)?
        .as_str()
        .map(str::to_owned)
        .context("enum did not serialize as a string")
}

fn leg_payload(result: &LegResult) -> Value {
    json!({
        "status": enum_json(&result.status).expect("LegStatus serializes as a string"),
        "token_b_delta_base_units": result.token_b_delta_base_units.to_string(),
        "token_a_delta_base_units": result.token_a_delta_base_units.to_string(),
        "gas_cost_token_a_base_units": result.gas_cost_token_a_base_units.to_string(),
        "venue_reference": result.venue_reference,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CoordinatorCommand {
    DispatchDex {
        operation_id: String,
        expected_token_b_delta_base_units: i128,
        plan: Option<Box<DexSwapPlan>>,
    },
    DispatchCex {
        client_order_id: String,
        target_token_b_delta_base_units: i128,
        limit_price: Option<Decimal>,
    },
    RecoverCex {
        attempt: usize,
        target_token_b_delta_base_units: i128,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaperOpportunity {
    pub source_revision: String,
    pub pair_id: String,
    pub symbol: String,
    pub update_id: u64,
    pub received_unix_us: u64,
    pub direction: ArbitrageDirection,
    pub dex_pool_index: usize,
    pub dex_pool_generation: u64,
    pub token_b_base_units: i128,
    pub token_b_step_base_units: i128,
    pub cost_token_a_base_units: i128,
    pub proceeds_token_a_base_units: i128,
    pub admission: AdmissionRiskBounds,
    pub dex_plan: DexSwapPlan,
}

impl PaperOpportunity {
    pub fn validate(&self) -> anyhow::Result<()> {
        validate_id("source revision", &self.source_revision, 96)?;
        validate_id("pair id", &self.pair_id, 96)?;
        validate_id("symbol", &self.symbol, 24)?;
        ensure!(self.update_id > 0, "paper opportunity update id is zero");
        ensure!(
            self.received_unix_us > 0,
            "paper opportunity receive timestamp is zero"
        );
        ensure!(
            self.token_b_base_units > 0,
            "paper opportunity token-B amount must be positive"
        );
        ensure!(
            self.dex_pool_generation > 0,
            "paper opportunity DEX pool generation is zero"
        );
        ensure!(
            self.token_b_step_base_units > 0,
            "paper opportunity token-B step must be positive"
        );
        ensure!(
            self.cost_token_a_base_units > 0 && self.proceeds_token_a_base_units > 0,
            "paper opportunity token-A economics must be positive"
        );
        self.admission.validate()?;
        self.dex_plan.validate()?;
        Ok(())
    }

    pub fn plan_id(&self) -> String {
        let direction = match self.direction {
            ArbitrageDirection::BuyTokenBOnDexSellOnCex => "ds",
            ArbitrageDirection::BuyTokenBOnCexSellOnDex => "cs",
        };
        format!(
            "paper-{}-{}-{direction}",
            self.received_unix_us, self.update_id
        )
    }

    pub(crate) fn intent(&self, mode: ExecutionMode) -> TradeIntent {
        let direction = match self.direction {
            ArbitrageDirection::BuyTokenBOnDexSellOnCex => "ds",
            ArbitrageDirection::BuyTokenBOnCexSellOnDex => "cs",
        };
        let plan_id = self.plan_id();
        let client_order_id = cex_client_order_id(
            &self.pair_id,
            self.received_unix_us,
            self.update_id,
            direction,
        );
        TradeIntent {
            dex_operation_id: format!("{plan_id}-dex"),
            plan_id,
            source_revision: self.source_revision.clone(),
            pair_id: self.pair_id.clone(),
            mode,
            direction: self.direction,
            planned_token_b_base_units: self.token_b_base_units,
            token_b_step_base_units: self.token_b_step_base_units,
            expected_cost_token_a_base_units: self.cost_token_a_base_units,
            expected_proceeds_token_a_base_units: self.proceeds_token_a_base_units,
            cex_client_order_id: client_order_id,
            admission: Some(self.admission.clone()),
            dex_plan: Some(self.dex_plan.clone()),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct EntryPreflightHandle {
    inner: Arc<RwLock<EntryPreflightState>>,
}

#[derive(Clone, Debug, Default)]
struct EntryPreflightState {
    quotes: BTreeMap<String, TopOfBook>,
    quote_max_age_ms: BTreeMap<String, u64>,
    dex_pool_generations: BTreeMap<usize, u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntryPreflightRejection {
    pub reason: &'static str,
    pub detail: String,
}

impl EntryPreflightHandle {
    pub fn configure_quote_max_age(&self, symbol: &str, max_age_ms: u64) {
        let Ok(mut state) = self.inner.write() else {
            return;
        };
        state.quote_max_age_ms.insert(symbol.to_owned(), max_age_ms);
    }

    pub fn update_quote(&self, quote: &TopOfBook) {
        let Ok(mut state) = self.inner.write() else {
            return;
        };
        let current = state.quotes.get(quote.symbol.as_ref());
        if current.is_none_or(|current| {
            quote.connection_generation > current.connection_generation
                || (quote.connection_generation == current.connection_generation
                    && quote.update_id >= current.update_id)
        }) {
            state
                .quotes
                .insert(quote.symbol.as_ref().to_owned(), quote.clone());
        }
    }

    pub fn update_dex_pool_generation(&self, pool_index: usize, generation: u64) {
        let Ok(mut state) = self.inner.write() else {
            return;
        };
        let current = state.dex_pool_generations.get(&pool_index).copied();
        if current.is_none_or(|current| generation >= current) {
            state.dex_pool_generations.insert(pool_index, generation);
        }
    }

    pub fn check(
        &self,
        opportunity: &PaperOpportunity,
    ) -> anyhow::Result<Option<EntryPreflightRejection>> {
        opportunity.validate()?;
        let state = self
            .inner
            .read()
            .map_err(|_| anyhow::anyhow!("entry preflight state is poisoned"))?;
        let Some(quote) = state.quotes.get(&opportunity.symbol) else {
            return Ok(Some(EntryPreflightRejection {
                reason: "missing_preflight_quote",
                detail: format!("no latest quote for {}", opportunity.symbol),
            }));
        };
        if let Some(max_age_ms) = state.quote_max_age_ms.get(&opportunity.symbol).copied()
            && quote.received_at.elapsed() > Duration::from_millis(max_age_ms)
        {
            return Ok(Some(EntryPreflightRejection {
                reason: "preflight_quote_age_exceeded",
                detail: format!(
                    "latest quote age {} ms exceeds configured {} ms",
                    quote.received_at.elapsed().as_millis(),
                    max_age_ms
                ),
            }));
        }
        if quote.update_id < opportunity.update_id {
            return Ok(Some(EntryPreflightRejection {
                reason: "stale_preflight_quote",
                detail: format!(
                    "latest update_id {} is older than admission update_id {}",
                    quote.update_id, opportunity.update_id
                ),
            }));
        }
        let primary = opportunity.admission.cex_primary_limit_price;
        match opportunity.direction {
            ArbitrageDirection::BuyTokenBOnDexSellOnCex if quote.bid_price < primary => {
                return Ok(Some(EntryPreflightRejection {
                    reason: "cex_price_moved_against_admission",
                    detail: format!("bid {} is below admission {}", quote.bid_price, primary),
                }));
            }
            ArbitrageDirection::BuyTokenBOnCexSellOnDex if quote.ask_price > primary => {
                return Ok(Some(EntryPreflightRejection {
                    reason: "cex_price_moved_against_admission",
                    detail: format!("ask {} is above admission {}", quote.ask_price, primary),
                }));
            }
            _ => {}
        }
        if opportunity.admission.cex_primary_top_quantity > Decimal::ZERO {
            let available = match opportunity.direction {
                ArbitrageDirection::BuyTokenBOnDexSellOnCex => quote.bid_quantity,
                ArbitrageDirection::BuyTokenBOnCexSellOnDex => quote.ask_quantity,
            };
            if available < opportunity.admission.cex_primary_top_quantity {
                return Ok(Some(EntryPreflightRejection {
                    reason: "cex_top_quantity_below_admission",
                    detail: format!(
                        "latest top quantity {} is below admission {}",
                        available, opportunity.admission.cex_primary_top_quantity
                    ),
                }));
            }
        }
        let Some(current_generation) = state
            .dex_pool_generations
            .get(&opportunity.dex_pool_index)
            .copied()
        else {
            return Ok(Some(EntryPreflightRejection {
                reason: "missing_preflight_dex_generation",
                detail: format!(
                    "no latest DEX generation for pool {}",
                    opportunity.dex_pool_index
                ),
            }));
        };
        if current_generation != opportunity.dex_pool_generation {
            return Ok(Some(EntryPreflightRejection {
                reason: "dex_pool_changed_after_quote",
                detail: format!(
                    "current generation {} differs from admission generation {}",
                    current_generation, opportunity.dex_pool_generation
                ),
            }));
        }
        let now_unix_seconds = unix_timestamp_ms()? / 1_000;
        if now_unix_seconds >= opportunity.dex_plan.deadline_unix_seconds {
            return Ok(Some(EntryPreflightRejection {
                reason: "dex_plan_expired",
                detail: format!(
                    "DEX deadline {} is not after current time {}",
                    opportunity.dex_plan.deadline_unix_seconds, now_unix_seconds
                ),
            }));
        }
        Ok(None)
    }
}

fn cex_client_order_id(
    pair_id: &str,
    received_unix_us: u64,
    update_id: u64,
    direction: &str,
) -> String {
    let digest = Sha256::digest(format!(
        "{pair_id}:{received_unix_us}:{update_id}:{direction}"
    ));
    let mut fingerprint = String::with_capacity(24);
    for byte in &digest[..12] {
        use std::fmt::Write as _;
        let _ = write!(fingerprint, "{byte:02x}");
    }
    format!("rustarb{fingerprint}{direction}")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PaperTradeEventState {
    Balanced,
    RejectedUnsubmitted,
    BlockedUnknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaperTradeEvent {
    pub plan_id: String,
    pub state: PaperTradeEventState,
    pub dex_filled: bool,
}

pub struct PaperTradeHandle {
    mailbox: Arc<LatestOpportunityMailbox>,
    discarded: Arc<AtomicU64>,
}

#[derive(Debug)]
pub enum PaperTradeSubmitResult {
    Accepted,
    Superseded(Box<PaperOpportunity>),
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExecutionLaneState {
    Available,
    Busy,
    BlockedUnknown,
}

#[derive(Debug)]
struct LatestOpportunityState {
    pending: Option<PaperOpportunity>,
    lane: ExecutionLaneState,
    senders: usize,
    receiver_open: bool,
}

#[derive(Debug)]
struct LatestOpportunityMailbox {
    state: Mutex<LatestOpportunityState>,
    notify: Notify,
}

pub(crate) struct LatestOpportunityReceiver {
    mailbox: Arc<LatestOpportunityMailbox>,
}

impl Clone for PaperTradeHandle {
    fn clone(&self) -> Self {
        if let Ok(mut state) = self.mailbox.state.lock() {
            state.senders = state.senders.saturating_add(1);
        }
        Self {
            mailbox: Arc::clone(&self.mailbox),
            discarded: Arc::clone(&self.discarded),
        }
    }
}

impl Drop for PaperTradeHandle {
    fn drop(&mut self) {
        if let Ok(mut state) = self.mailbox.state.lock() {
            state.senders = state.senders.saturating_sub(1);
        }
        self.mailbox.notify.notify_waiters();
    }
}

impl PaperTradeHandle {
    pub(crate) fn channel(
        initial_lane: ExecutionLaneState,
    ) -> (Self, LatestOpportunityReceiver, Arc<AtomicU64>) {
        let discarded = Arc::new(AtomicU64::new(0));
        let mailbox = Arc::new(LatestOpportunityMailbox {
            state: Mutex::new(LatestOpportunityState {
                pending: None,
                lane: initial_lane,
                senders: 1,
                receiver_open: true,
            }),
            notify: Notify::new(),
        });
        (
            Self {
                mailbox: Arc::clone(&mailbox),
                discarded: Arc::clone(&discarded),
            },
            LatestOpportunityReceiver { mailbox },
            discarded,
        )
    }

    /// Replaces an older pending opportunity without waiting or writing to disk.
    pub fn try_submit(&self, opportunity: PaperOpportunity) -> PaperTradeSubmitResult {
        let Ok(mut state) = self.mailbox.state.lock() else {
            self.discarded.fetch_add(1, Ordering::Relaxed);
            return PaperTradeSubmitResult::Unavailable;
        };
        if !state.receiver_open || state.lane == ExecutionLaneState::BlockedUnknown {
            self.discarded.fetch_add(1, Ordering::Relaxed);
            return PaperTradeSubmitResult::Unavailable;
        }
        let previous = state.pending.replace(opportunity);
        drop(state);
        self.mailbox.notify.notify_one();
        match previous {
            Some(previous) => {
                self.discarded.fetch_add(1, Ordering::Relaxed);
                PaperTradeSubmitResult::Superseded(Box::new(previous))
            }
            None => PaperTradeSubmitResult::Accepted,
        }
    }

    pub fn finish(&self, state: PaperTradeEventState) -> Option<PaperOpportunity> {
        let Ok(mut mailbox) = self.mailbox.state.lock() else {
            return None;
        };
        let discarded = if state == PaperTradeEventState::BlockedUnknown {
            mailbox.lane = ExecutionLaneState::BlockedUnknown;
            mailbox.pending.take()
        } else {
            mailbox.lane = ExecutionLaneState::Available;
            None
        };
        drop(mailbox);
        self.mailbox.notify.notify_waiters();
        discarded
    }

    /// Keeps the lane occupied while an external state transition settles.
    /// Any opportunity admitted against the pre-settlement state is invalid.
    pub fn hold_for_settlement(&self) -> Option<PaperOpportunity> {
        let Ok(mut mailbox) = self.mailbox.state.lock() else {
            return None;
        };
        if mailbox.lane != ExecutionLaneState::BlockedUnknown {
            mailbox.lane = ExecutionLaneState::Busy;
        }
        mailbox.pending.take()
    }
}

impl LatestOpportunityReceiver {
    pub(crate) async fn recv(&mut self) -> Option<PaperOpportunity> {
        loop {
            let notified = self.mailbox.notify.notified();
            {
                let mut state = self.mailbox.state.lock().ok()?;
                if state.lane == ExecutionLaneState::Available
                    && let Some(opportunity) = state.pending.take()
                {
                    state.lane = ExecutionLaneState::Busy;
                    return Some(opportunity);
                }
                if state.senders == 0 || !state.receiver_open {
                    return None;
                }
            }
            notified.await;
        }
    }
}

impl Drop for LatestOpportunityReceiver {
    fn drop(&mut self) {
        if let Ok(mut state) = self.mailbox.state.lock() {
            state.receiver_open = false;
            state.pending = None;
        }
        self.mailbox.notify.notify_waiters();
    }
}

pub struct PaperTradeTask {
    receiver: LatestOpportunityReceiver,
    coordinator: PaperTradeCoordinator,
    mode: ExecutionMode,
    telemetry: TelemetryHandle,
    engine_id: String,
    discarded: Arc<AtomicU64>,
    event_sender: mpsc::UnboundedSender<PaperTradeEvent>,
}

pub fn paper_trade_channel(
    path: impl AsRef<Path>,
    mode: ExecutionMode,
    telemetry: TelemetryHandle,
    engine_id: String,
) -> anyhow::Result<(
    PaperTradeHandle,
    PaperTradeTask,
    mpsc::UnboundedReceiver<PaperTradeEvent>,
)> {
    validate_id("engine id", &engine_id, 96)?;
    let coordinator = PaperTradeCoordinator::open(path)?;
    let initial_lane = initial_execution_lane(&coordinator);
    let (handle, receiver, discarded) = PaperTradeHandle::channel(initial_lane);
    let (event_sender, event_receiver) = mpsc::unbounded_channel();
    Ok((
        handle,
        PaperTradeTask {
            receiver,
            coordinator,
            mode,
            telemetry,
            engine_id,
            discarded,
            event_sender,
        },
        event_receiver,
    ))
}

impl PaperTradeTask {
    pub async fn run(mut self) -> anyhow::Result<()> {
        self.resume_active()?;
        while let Some(opportunity) = self.receiver.recv().await {
            let plan_id = opportunity.plan_id();
            if let Err(error) = self.execute(opportunity) {
                tracing::error!(error = %error, "paper arbitrage execution failed closed");
                let state = self.coordinator.operation(&plan_id).map_or(
                    PaperTradeEventState::RejectedUnsubmitted,
                    |operation| {
                        if operation.dex_dispatched || operation.cex_dispatched {
                            PaperTradeEventState::BlockedUnknown
                        } else {
                            PaperTradeEventState::RejectedUnsubmitted
                        }
                    },
                );
                self.publish_event(plan_id, state, false)?;
            }
        }
        let discarded = self.discarded.swap(0, Ordering::Relaxed);
        if discarded > 0 {
            tracing::warn!(
                discarded,
                "paper arbitrage opportunities superseded or rejected outside hot path"
            );
        }
        Ok(())
    }

    fn resume_active(&mut self) -> anyhow::Result<()> {
        let plan_ids = self
            .coordinator
            .active_operations()
            .into_iter()
            .filter(|operation| {
                matches!(
                    operation.stage,
                    TradeStage::Prepared
                        | TradeStage::Executing
                        | TradeStage::Recovering
                        | TradeStage::Halted
                )
            })
            .map(|operation| operation.intent.plan_id.clone())
            .collect::<Vec<_>>();
        for plan_id in plan_ids {
            self.resume_plan(&plan_id)?;
        }
        Ok(())
    }

    fn resume_plan(&mut self, plan_id: &str) -> anyhow::Result<()> {
        loop {
            let Some(command) = self.coordinator.resume_command(plan_id)? else {
                break;
            };
            let operation = self
                .coordinator
                .operation(plan_id)
                .context("paper trade disappeared during restart recovery")?
                .clone();
            let (role, result) = simulate_command(&operation.intent, &command)?;
            self.coordinator.record_result(plan_id, role, result)?;
        }
        self.drive(plan_id)
    }

    fn execute(&mut self, opportunity: PaperOpportunity) -> anyhow::Result<()> {
        opportunity.validate()?;
        let intent = opportunity.intent(self.mode);
        let plan_id = intent.plan_id.clone();
        self.coordinator.admit(intent)?;
        self.drive(&plan_id)
    }

    fn drive(&mut self, plan_id: &str) -> anyhow::Result<()> {
        loop {
            let commands = self.coordinator.take_commands(plan_id)?;
            if commands.is_empty() {
                let operation = self
                    .coordinator
                    .operation(plan_id)
                    .context("paper trade disappeared from coordinator")?;
                if operation.result.is_some() {
                    let mut payload = operation.result_telemetry_payload(&self.engine_id)?;
                    let object = payload
                        .as_object_mut()
                        .context("paper result payload is not an object")?;
                    object.insert("simulation".to_owned(), Value::Bool(true));
                    object.insert("includes_binance_fee".to_owned(), Value::Bool(true));
                    object.insert("includes_gas".to_owned(), Value::Bool(false));
                    object.insert("comparable_to_live".to_owned(), Value::Bool(false));
                    self.telemetry.emit("paper_arbitrage_result", payload);
                    self.publish_event(
                        plan_id.to_owned(),
                        PaperTradeEventState::Balanced,
                        dex_filled(operation),
                    )?;
                } else if matches!(
                    operation.stage,
                    TradeStage::UnknownExposure | TradeStage::Halted
                ) {
                    self.publish_event(
                        plan_id.to_owned(),
                        PaperTradeEventState::BlockedUnknown,
                        dex_filled(operation),
                    )?;
                }
                return Ok(());
            }
            let intent = self
                .coordinator
                .operation(plan_id)
                .context("paper trade disappeared after dispatch")?
                .intent
                .clone();
            for command in commands {
                let (role, result) = simulate_command(&intent, &command)?;
                self.coordinator.record_result(plan_id, role, result)?;
            }
        }
    }

    fn publish_event(
        &self,
        plan_id: String,
        state: PaperTradeEventState,
        dex_filled: bool,
    ) -> anyhow::Result<()> {
        self.event_sender
            .send(PaperTradeEvent {
                plan_id,
                state,
                dex_filled,
            })
            .map_err(|_| anyhow::anyhow!("paper trade event receiver is closed"))
    }
}

pub(crate) fn initial_execution_lane(coordinator: &PaperTradeCoordinator) -> ExecutionLaneState {
    let active = coordinator.active_operations();
    if active
        .iter()
        .any(|operation| operation.stage == TradeStage::UnknownExposure)
    {
        ExecutionLaneState::BlockedUnknown
    } else if active.is_empty() {
        ExecutionLaneState::Available
    } else {
        ExecutionLaneState::Busy
    }
}

fn dex_filled(operation: &TradeOperation) -> bool {
    operation.dex_result.as_ref().is_some_and(|result| {
        result.status == LegStatus::Filled
            && (result.token_b_delta_base_units != 0 || result.token_a_delta_base_units != 0)
    })
}

fn simulate_command(
    intent: &TradeIntent,
    command: &CoordinatorCommand,
) -> anyhow::Result<(LegRole, LegResult)> {
    match command {
        CoordinatorCommand::DispatchDex {
            expected_token_b_delta_base_units,
            ..
        } => {
            let token_a_delta = match intent.direction {
                ArbitrageDirection::BuyTokenBOnDexSellOnCex => {
                    -intent.expected_cost_token_a_base_units
                }
                ArbitrageDirection::BuyTokenBOnCexSellOnDex => {
                    intent.expected_proceeds_token_a_base_units
                }
            };
            Ok((
                LegRole::Dex,
                LegResult {
                    status: LegStatus::Filled,
                    token_b_delta_base_units: *expected_token_b_delta_base_units,
                    token_a_delta_base_units: token_a_delta,
                    gas_cost_token_a_base_units: 0,
                    venue_reference: format!("paper:dex:{}", intent.plan_id),
                },
            ))
        }
        CoordinatorCommand::DispatchCex {
            target_token_b_delta_base_units,
            ..
        } => Ok((
            LegRole::Cex,
            simulated_cex_result(intent, *target_token_b_delta_base_units, "primary")?,
        )),
        CoordinatorCommand::RecoverCex {
            attempt,
            target_token_b_delta_base_units,
            ..
        } => Ok((
            LegRole::RecoveryCex,
            simulated_cex_result(
                intent,
                *target_token_b_delta_base_units,
                &format!("recovery:market:{attempt}"),
            )?,
        )),
    }
}

fn simulated_cex_result(
    intent: &TradeIntent,
    token_b_delta: i128,
    role: &str,
) -> anyhow::Result<LegResult> {
    ensure!(token_b_delta != 0, "paper CEX quantity is zero");
    let planned = intent.planned_token_b_base_units;
    let reference_token_a = if token_b_delta > 0 {
        -intent.expected_cost_token_a_base_units
    } else {
        intent.expected_proceeds_token_a_base_units
    };
    let magnitude = reference_token_a
        .unsigned_abs()
        .checked_mul(token_b_delta.unsigned_abs())
        .context("paper CEX token-A scaling overflow")?
        / planned.unsigned_abs();
    let magnitude = i128::try_from(magnitude).context("paper CEX token-A amount overflow")?;
    let token_a_delta = if reference_token_a < 0 {
        -magnitude
    } else {
        magnitude
    };
    Ok(LegResult {
        status: LegStatus::Filled,
        token_b_delta_base_units: token_b_delta,
        token_a_delta_base_units: token_a_delta,
        gas_cost_token_a_base_units: 0,
        venue_reference: format!("paper:cex:{role}:{}", intent.plan_id),
    })
}

/// Paper coordinator with a production-shaped durable ownership boundary.
/// `take_commands` persists dispatch ownership before returning any command.
pub struct PaperTradeCoordinator {
    journal: TradeJournal,
}

impl PaperTradeCoordinator {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(Self {
            journal: TradeJournal::open(path)?,
        })
    }

    pub fn admit(&mut self, intent: TradeIntent) -> anyhow::Result<()> {
        intent.validate()?;
        ensure!(
            intent
                .admission
                .as_ref()
                .is_none_or(|admission| admission.opportunity_threshold_met),
            "opportunity threshold must be met before admission"
        );
        if let Some(admission) = intent.admission.as_ref() {
            ensure!(
                meets_expected_profit_after_gas_threshold(
                    intent.expected_proceeds_token_a_base_units,
                    intent.expected_cost_token_a_base_units,
                    admission.maximum_gas_cost_token_a_base_units,
                )?,
                "expected profit after gas must be at least {} bps before admission",
                EXPECTED_PROFIT_AFTER_GAS_THRESHOLD_BPS
            );
        }
        ensure!(
            self.journal.active_operations().is_empty(),
            "another trade is active or has unknown exposure"
        );
        self.journal.record_intent(intent)
    }

    pub fn operation(&self, plan_id: &str) -> Option<&TradeOperation> {
        self.journal.operations.get(plan_id)
    }

    pub fn active_operations(&self) -> Vec<&TradeOperation> {
        self.journal.active_operations()
    }

    /// Counts every durable parent intent in this journal, including terminal
    /// work. Live canary/launch caps therefore survive process restarts.
    pub fn admitted_operation_count(&self) -> usize {
        self.journal.operations.len()
    }

    pub fn cumulative_terminal_risk(&self) -> anyhow::Result<(u128, u128)> {
        let mut realized_loss = 0_u128;
        let mut recovery_loss = 0_u128;
        for result in self
            .journal
            .operations
            .values()
            .filter_map(|operation| operation.result.as_ref())
        {
            let comparable_profit = if result.comparable_profit_token_a_base_units != 0
                || result.residual_value_token_a_base_units != 0
            {
                result.comparable_profit_token_a_base_units
            } else {
                result.realized_profit_token_a_base_units
            };
            if comparable_profit < 0 {
                realized_loss = realized_loss
                    .checked_add(comparable_profit.unsigned_abs())
                    .context("cumulative realized loss overflow")?;
            }
            if result.recovery_loss_token_a_base_units > 0 {
                recovery_loss = recovery_loss
                    .checked_add(result.recovery_loss_token_a_base_units.unsigned_abs())
                    .context("cumulative recovery loss overflow")?;
            }
        }
        Ok((realized_loss, recovery_loss))
    }

    /// Reconstructs only a command whose dispatch ownership was already
    /// persisted before a restart. It never advances coordinator state.
    pub fn resume_command(&self, plan_id: &str) -> anyhow::Result<Option<CoordinatorCommand>> {
        let operation = self.journal.operation(plan_id)?;
        if operation.dex_dispatched && operation.dex_result.is_none() {
            return Ok(Some(CoordinatorCommand::DispatchDex {
                operation_id: operation.intent.dex_operation_id.clone(),
                expected_token_b_delta_base_units: operation
                    .intent
                    .direction
                    .dex_token_b_delta(operation.intent.planned_token_b_base_units),
                plan: operation.intent.dex_plan.clone().map(Box::new),
            }));
        }
        if operation.cex_dispatched && operation.cex_result.is_none() {
            let target = if operation.intent.mode == ExecutionMode::DexFirst {
                -operation
                    .dex_result
                    .as_ref()
                    .context("dex-first CEX dispatch has no DEX result")?
                    .token_b_delta_base_units
            } else {
                -operation
                    .intent
                    .direction
                    .dex_token_b_delta(operation.intent.planned_token_b_base_units)
            };
            return Ok(Some(CoordinatorCommand::DispatchCex {
                client_order_id: operation.intent.cex_client_order_id.clone(),
                target_token_b_delta_base_units: target,
                limit_price: operation
                    .intent
                    .admission
                    .as_ref()
                    .map(|bounds| bounds.cex_primary_limit_price),
            }));
        }
        if operation.recovery_inflight {
            let target = -operation.actionable_token_b_residual_base_units();
            let attempt = operation.recovery_results.len() + 1;
            return Ok(Some(CoordinatorCommand::RecoverCex {
                attempt,
                target_token_b_delta_base_units: target,
            }));
        }
        Ok(None)
    }

    pub fn take_commands(&mut self, plan_id: &str) -> anyhow::Result<Vec<CoordinatorCommand>> {
        let mut operation = self.journal.operation(plan_id)?.clone();
        ensure!(!operation.stage.terminal(), "trade is terminal");

        if operation.has_unknown_leg() {
            if operation.stage == TradeStage::UnknownExposure {
                return Ok(Vec::new());
            }
            operation.stage = TradeStage::UnknownExposure;
            operation.blocking_reason = Some("venue_outcome_unknown".to_owned());
            self.journal.append(operation)?;
            return Ok(Vec::new());
        }

        let mut commands = Vec::new();
        if !operation.dex_dispatched {
            operation.dex_dispatched = true;
            operation.stage = TradeStage::Executing;
            commands.push(CoordinatorCommand::DispatchDex {
                operation_id: operation.intent.dex_operation_id.clone(),
                expected_token_b_delta_base_units: operation
                    .intent
                    .direction
                    .dex_token_b_delta(operation.intent.planned_token_b_base_units),
                plan: operation.intent.dex_plan.clone().map(Box::new),
            });
            if operation.intent.mode == ExecutionMode::ConcurrentHedged {
                operation.cex_dispatched = true;
                commands.push(CoordinatorCommand::DispatchCex {
                    client_order_id: operation.intent.cex_client_order_id.clone(),
                    target_token_b_delta_base_units: -operation
                        .intent
                        .direction
                        .dex_token_b_delta(operation.intent.planned_token_b_base_units),
                    limit_price: operation
                        .intent
                        .admission
                        .as_ref()
                        .map(|bounds| bounds.cex_primary_limit_price),
                });
            }
            self.journal.append(operation)?;
            return Ok(commands);
        }

        if operation.intent.mode == ExecutionMode::DexFirst
            && !operation.cex_dispatched
            && let Some(dex) = &operation.dex_result
        {
            if dex.status == LegStatus::Filled && dex.token_b_delta_base_units != 0 {
                operation.cex_dispatched = true;
                commands.push(CoordinatorCommand::DispatchCex {
                    client_order_id: operation.intent.cex_client_order_id.clone(),
                    target_token_b_delta_base_units: -dex.token_b_delta_base_units,
                    limit_price: operation
                        .intent
                        .admission
                        .as_ref()
                        .map(|bounds| bounds.cex_primary_limit_price),
                });
                self.journal.append(operation)?;
                return Ok(commands);
            }
            if dex.status == LegStatus::Failed {
                finalize_balanced(&mut operation)?;
                self.journal.append(operation)?;
                return Ok(Vec::new());
            }
        }

        let primary_finished = operation.dex_result.is_some()
            && (operation.cex_result.is_some() || !operation.cex_dispatched);
        if primary_finished && !operation.recovery_inflight {
            let residual = operation.actionable_token_b_residual_base_units();
            if residual == 0 {
                finalize_balanced(&mut operation)?;
                self.journal.append(operation)?;
            } else if operation.recovery_results.len() < MAX_RECOVERY_ATTEMPTS {
                let target = -residual;
                if operation.intent.mode == ExecutionMode::DexFirst
                    && !dex_first_recovery_direction_is_valid(operation.intent.direction, target)
                {
                    operation.stage = TradeStage::Halted;
                    operation.blocking_reason =
                        Some("dex_first_recovery_direction_flipped".to_owned());
                    self.journal.append(operation)?;
                    return Ok(commands);
                }
                operation.stage = TradeStage::Recovering;
                operation.recovery_inflight = true;
                let attempt = operation.recovery_results.len() + 1;
                commands.push(CoordinatorCommand::RecoverCex {
                    attempt,
                    target_token_b_delta_base_units: target,
                });
                self.journal.append(operation)?;
            } else {
                operation.stage = TradeStage::Halted;
                operation.blocking_reason = Some("recovery_attempts_exhausted".to_owned());
                self.journal.append(operation)?;
            }
        }
        Ok(commands)
    }

    pub fn record_result(
        &mut self,
        plan_id: &str,
        role: LegRole,
        result: LegResult,
    ) -> anyhow::Result<()> {
        result.validate()?;
        let mut operation = self.journal.operation(plan_id)?.clone();
        ensure!(!operation.stage.terminal(), "trade is terminal");
        match role {
            LegRole::Dex => {
                ensure!(operation.dex_dispatched, "DEX leg was not dispatched");
                ensure!(
                    operation.dex_result.is_none(),
                    "DEX result already recorded"
                );
                operation.dex_result = Some(result);
            }
            LegRole::Cex => {
                ensure!(operation.cex_dispatched, "CEX leg was not dispatched");
                ensure!(
                    operation.cex_result.is_none(),
                    "CEX result already recorded"
                );
                operation.cex_result = Some(result);
            }
            LegRole::RecoveryCex => {
                ensure!(operation.recovery_inflight, "recovery was not dispatched");
                operation.recovery_inflight = false;
                operation.recovery_results.push(result);
            }
        }
        self.journal.append(operation)
    }

    /// Replaces only a previously journaled unknown outcome with venue-proven
    /// terminal data. It never authorizes a new child mutation.
    pub fn reconcile_unknown(
        &mut self,
        plan_id: &str,
        role: LegRole,
        result: LegResult,
    ) -> anyhow::Result<()> {
        result.validate()?;
        ensure!(
            result.status != LegStatus::Unknown,
            "reconciliation must prove a terminal venue outcome"
        );
        let mut operation = self.journal.operation(plan_id)?.clone();
        ensure!(
            operation.stage == TradeStage::UnknownExposure,
            "trade is not waiting for unknown-outcome reconciliation"
        );
        match role {
            LegRole::Dex => replace_unknown(&mut operation.dex_result, result)?,
            LegRole::Cex => replace_unknown(&mut operation.cex_result, result)?,
            LegRole::RecoveryCex => {
                let current = operation
                    .recovery_results
                    .last_mut()
                    .context("trade has no recovery result to reconcile")?;
                ensure!(
                    current.status == LegStatus::Unknown,
                    "latest recovery result is not unknown"
                );
                *current = result;
            }
        }
        operation.stage = if operation.recovery_results.is_empty() {
            TradeStage::Executing
        } else {
            TradeStage::Recovering
        };
        operation.blocking_reason = None;
        self.journal.append(operation)
    }
}

const fn dex_first_recovery_direction_is_valid(
    direction: ArbitrageDirection,
    target_token_b_delta_base_units: i128,
) -> bool {
    match direction {
        ArbitrageDirection::BuyTokenBOnDexSellOnCex => target_token_b_delta_base_units < 0,
        ArbitrageDirection::BuyTokenBOnCexSellOnDex => target_token_b_delta_base_units > 0,
    }
}

fn replace_unknown(slot: &mut Option<LegResult>, result: LegResult) -> anyhow::Result<()> {
    let current = slot.as_ref().context("trade has no result to reconcile")?;
    ensure!(
        current.status == LegStatus::Unknown,
        "venue result is not unknown"
    );
    *slot = Some(result);
    Ok(())
}

fn finalize_balanced(operation: &mut TradeOperation) -> anyhow::Result<()> {
    ensure!(
        operation.actionable_token_b_residual_base_units() == 0,
        "cannot finalize a trade with actionable token-B exposure"
    );
    let (realized_profit, gas) = operation.realized_profit_token_a_base_units();
    let recovery_token_a = operation
        .recovery_results
        .iter()
        .fold(0_i128, |total, result| {
            total.saturating_add(result.token_a_delta_base_units)
        });
    let residual_value = residual_value_token_a_base_units(operation)?;
    let comparable_profit = realized_profit
        .checked_add(residual_value)
        .context("comparable realized profit overflow")?;
    let outcome = if comparable_profit >= 0 {
        TerminalOutcome::BalancedProfit
    } else {
        TerminalOutcome::BalancedLoss
    };
    operation.stage = match outcome {
        TerminalOutcome::BalancedProfit => TradeStage::BalancedProfit,
        TerminalOutcome::BalancedLoss => TradeStage::BalancedLoss,
    };
    operation.result = Some(ArbitrageResult {
        expected_profit_token_a_base_units: operation.intent.expected_profit_token_a_base_units(),
        realized_profit_token_a_base_units: realized_profit,
        residual_value_token_a_base_units: residual_value,
        comparable_profit_token_a_base_units: comparable_profit,
        token_b_residual_base_units: operation.token_b_residual_base_units(),
        gas_cost_token_a_base_units: gas,
        recovery_loss_token_a_base_units: recovery_token_a.min(0).saturating_neg(),
        outcome,
    });
    Ok(())
}

fn residual_value_token_a_base_units(operation: &TradeOperation) -> anyhow::Result<i128> {
    let residual = operation.token_b_residual_base_units();
    if residual == 0 {
        return Ok(0);
    }
    let Some(admission) = operation.intent.admission.as_ref() else {
        return Ok(0);
    };
    let planned = operation.intent.planned_token_b_base_units.unsigned_abs();
    ensure!(planned > 0, "residual mark has zero planned token-B amount");
    let quote = admission.recovery_quote_for_residual(residual);
    ensure!(quote > 0, "residual mark has zero recovery quote");
    let numerator = U256::from(residual.unsigned_abs())
        .checked_mul(U256::from(quote))
        .context("residual mark numerator overflow")?;
    let denominator = U256::from(planned);
    let quotient = numerator / denominator;
    let magnitude = if residual < 0 && numerator % denominator != U256::ZERO {
        quotient
            .checked_add(U256::ONE)
            .context("residual liability rounding overflow")?
    } else {
        quotient
    };
    let magnitude = u128::try_from(magnitude).context("residual mark exceeds u128")?;
    let magnitude = i128::try_from(magnitude).context("residual mark exceeds i128")?;
    Ok(if residual > 0 { magnitude } else { -magnitude })
}

struct TradeJournal {
    file: File,
    operations: BTreeMap<String, TradeOperation>,
    next_sequence: u64,
    poisoned: bool,
}

impl TradeJournal {
    fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        ensure!(!path.as_os_str().is_empty(), "trade journal path is empty");
        let existed = path.exists();
        if existed {
            let metadata = symlink_metadata(&path)
                .with_context(|| format!("failed to inspect trade journal {}", path.display()))?;
            ensure!(
                !metadata.file_type().is_symlink(),
                "trade journal must not be a symlink"
            );
            ensure!(metadata.is_file(), "trade journal path is not a file");
        } else {
            let parent = path
                .parent()
                .filter(|value| !value.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            ensure!(
                parent.is_dir(),
                "trade journal parent directory does not exist"
            );
        }

        let mut options = OpenOptions::new();
        options.create(true).read(true).append(true);
        #[cfg(unix)]
        options.mode(0o600);
        let file = options
            .open(&path)
            .with_context(|| format!("failed to open trade journal {}", path.display()))?;
        validate_permissions(&file)?;
        file.try_lock()
            .context("trade journal is already locked by another process")?;
        if !existed {
            file.sync_all()
                .context("failed to sync new trade journal")?;
            sync_parent(&path)?;
        }

        let mut operations = BTreeMap::new();
        let mut expected_sequence = 0_u64;
        let mut reader = BufReader::new(file.try_clone().context("failed to clone trade journal")?);
        loop {
            let mut line = Vec::new();
            let bytes = reader
                .read_until(b'\n', &mut line)
                .context("failed to read trade journal")?;
            if bytes == 0 {
                break;
            }
            ensure!(
                line.len() <= MAX_LINE_BYTES,
                "trade journal record is too large"
            );
            ensure!(
                line.last() == Some(&b'\n'),
                "trade journal ends with a partial record"
            );
            line.pop();
            let record: WireRecord =
                serde_json::from_slice(&line).context("trade journal contains invalid JSON")?;
            record.validate_checksum()?;
            ensure!(
                record.payload.version == JOURNAL_VERSION,
                "unsupported trade journal version"
            );
            ensure!(
                record.payload.sequence == expected_sequence,
                "trade journal sequence mismatch"
            );
            apply_snapshot(&mut operations, &record.payload.operation)?;
            expected_sequence = expected_sequence
                .checked_add(1)
                .context("trade journal sequence overflow")?;
        }
        Ok(Self {
            file,
            operations,
            next_sequence: expected_sequence,
            poisoned: false,
        })
    }

    fn operation(&self, plan_id: &str) -> anyhow::Result<&TradeOperation> {
        self.operations
            .get(plan_id)
            .with_context(|| format!("unknown trade plan {plan_id}"))
    }

    fn active_operations(&self) -> Vec<&TradeOperation> {
        self.operations
            .values()
            .filter(|operation| {
                !matches!(
                    operation.stage,
                    TradeStage::BalancedProfit | TradeStage::BalancedLoss
                )
            })
            .collect()
    }

    fn record_intent(&mut self, intent: TradeIntent) -> anyhow::Result<()> {
        ensure!(
            !self.operations.contains_key(&intent.plan_id),
            "trade plan already exists"
        );
        self.append(TradeOperation::prepared(intent))
    }

    fn append(&mut self, operation: TradeOperation) -> anyhow::Result<()> {
        ensure!(!self.poisoned, "trade journal is poisoned");
        validate_operation(&operation)?;
        let payload = WirePayload {
            version: JOURNAL_VERSION,
            sequence: self.next_sequence,
            recorded_at_unix_ms: unix_timestamp_ms()?,
            operation,
        };
        let mut next = self.operations.clone();
        apply_snapshot(&mut next, &payload.operation)?;
        let record = WireRecord::new(payload)?;
        let mut encoded = serde_json::to_vec(&record).context("failed to encode trade journal")?;
        ensure!(
            encoded.len() < MAX_LINE_BYTES,
            "trade journal record is too large"
        );
        encoded.push(b'\n');
        if let Err(error) = self
            .file
            .write_all(&encoded)
            .and_then(|()| self.file.sync_data())
        {
            self.poisoned = true;
            return Err(error).context("failed to durably append trade journal record");
        }
        self.operations = next;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .context("trade journal sequence overflow")?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WirePayload {
    version: u16,
    sequence: u64,
    recorded_at_unix_ms: u64,
    operation: TradeOperation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WireRecord {
    payload: WirePayload,
    checksum_sha256: String,
}

impl WireRecord {
    fn new(payload: WirePayload) -> anyhow::Result<Self> {
        let checksum_sha256 = checksum(&payload)?;
        Ok(Self {
            payload,
            checksum_sha256,
        })
    }

    fn validate_checksum(&self) -> anyhow::Result<()> {
        ensure!(
            self.checksum_sha256 == checksum(&self.payload)?,
            "trade journal checksum mismatch"
        );
        Ok(())
    }
}

fn apply_snapshot(
    operations: &mut BTreeMap<String, TradeOperation>,
    operation: &TradeOperation,
) -> anyhow::Result<()> {
    validate_operation(operation)?;
    if let Some(previous) = operations.get(&operation.intent.plan_id) {
        ensure!(
            previous.intent == operation.intent,
            "trade journal intent changed"
        );
        ensure!(!previous.stage.terminal(), "trade is already terminal");
        ensure!(
            stage_transition_allowed(&previous.stage, &operation.stage),
            "illegal parent trade stage transition"
        );
        ensure!(
            !previous.dex_dispatched || operation.dex_dispatched,
            "DEX dispatch regressed"
        );
        ensure!(
            !previous.cex_dispatched || operation.cex_dispatched,
            "CEX dispatch regressed"
        );
        ensure!(
            result_is_unchanged_or_reconciled(&previous.dex_result, &operation.dex_result),
            "DEX result changed without unknown-outcome reconciliation"
        );
        ensure!(
            result_is_unchanged_or_reconciled(&previous.cex_result, &operation.cex_result),
            "CEX result changed without unknown-outcome reconciliation"
        );
        ensure!(
            recovery_results_are_append_only_or_reconciled(
                &previous.recovery_results,
                &operation.recovery_results,
            ),
            "recovery results changed"
        );
    } else {
        ensure!(
            operation.stage == TradeStage::Prepared,
            "first trade state must be prepared"
        );
    }
    operations.insert(operation.intent.plan_id.clone(), operation.clone());
    Ok(())
}

fn stage_transition_allowed(previous: &TradeStage, next: &TradeStage) -> bool {
    previous == next
        || matches!(
            (previous, next),
            (TradeStage::Prepared, TradeStage::Executing)
                | (
                    TradeStage::Executing,
                    TradeStage::Recovering
                        | TradeStage::BalancedProfit
                        | TradeStage::BalancedLoss
                        | TradeStage::UnknownExposure
                        | TradeStage::Halted
                )
                | (
                    TradeStage::Recovering,
                    TradeStage::BalancedProfit
                        | TradeStage::BalancedLoss
                        | TradeStage::UnknownExposure
                        | TradeStage::Halted
                )
                | (
                    TradeStage::UnknownExposure,
                    TradeStage::Executing | TradeStage::Recovering | TradeStage::Halted
                )
                | (TradeStage::Halted, TradeStage::Recovering)
        )
}

fn result_is_unchanged_or_reconciled(
    previous: &Option<LegResult>,
    next: &Option<LegResult>,
) -> bool {
    previous == next
        || matches!(
            (previous, next),
            (Some(previous), Some(next))
                if previous.status == LegStatus::Unknown && next.status != LegStatus::Unknown
        )
        || previous.is_none()
}

fn recovery_results_are_append_only_or_reconciled(
    previous: &[LegResult],
    next: &[LegResult],
) -> bool {
    if next.starts_with(previous) {
        return true;
    }
    previous.len() == next.len()
        && !previous.is_empty()
        && previous[..previous.len() - 1] == next[..next.len() - 1]
        && previous
            .last()
            .is_some_and(|result| result.status == LegStatus::Unknown)
        && next
            .last()
            .is_some_and(|result| result.status != LegStatus::Unknown)
}

fn validate_operation(operation: &TradeOperation) -> anyhow::Result<()> {
    operation.intent.validate()?;
    ensure!(
        operation.recovery_results.len() <= MAX_RECOVERY_ATTEMPTS,
        "too many recovery attempts"
    );
    for result in operation
        .dex_result
        .iter()
        .chain(operation.cex_result.iter())
        .chain(operation.recovery_results.iter())
    {
        result.validate()?;
    }
    ensure!(
        operation.result.is_none()
            != matches!(
                operation.stage,
                TradeStage::BalancedProfit | TradeStage::BalancedLoss
            ),
        "terminal result and stage disagree"
    );
    if operation.result.is_some() {
        ensure!(
            operation.actionable_token_b_residual_base_units() == 0,
            "terminal result has actionable token-B exposure"
        );
    }
    if let Some(reason) = &operation.blocking_reason {
        validate_id("blocking reason", reason, 256)?;
    }
    Ok(())
}

fn validate_id(name: &str, value: &str, maximum: usize) -> anyhow::Result<()> {
    ensure!(
        !value.is_empty() && value.len() <= maximum,
        "{name} has invalid length"
    );
    ensure!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric()
                || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')),
        "{name} contains unsupported characters"
    );
    Ok(())
}

fn checksum(payload: &WirePayload) -> anyhow::Result<String> {
    let digest = Sha256::digest(
        serde_json::to_vec(payload).context("failed to encode trade checksum input")?,
    );
    let mut result = String::with_capacity(64);
    use std::fmt::Write as _;
    for byte in digest {
        write!(&mut result, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(result)
}

fn unix_timestamp_ms() -> anyhow::Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time is before Unix epoch")?
        .as_millis()
        .try_into()
        .context("Unix timestamp does not fit u64")
}

#[cfg(unix)]
fn validate_permissions(file: &File) -> anyhow::Result<()> {
    let mode = file
        .metadata()
        .context("failed to inspect trade journal permissions")?
        .permissions()
        .mode();
    ensure!(mode & 0o077 == 0, "trade journal is group/world accessible");
    Ok(())
}

#[cfg(not(unix))]
fn validate_permissions(_file: &File) -> anyhow::Result<()> {
    Ok(())
}

fn sync_parent(path: &Path) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .context("failed to open trade journal parent")?
        .sync_all()
        .context("failed to sync trade journal parent")
}

#[cfg(test)]
mod tests {
    use std::fs;

    use rust_decimal::Decimal;

    use super::{
        ArbitrageDirection, CoordinatorCommand, ExecutionMode, LegResult, LegRole, LegStatus,
        PaperTradeCoordinator, TerminalOutcome, TradeIntent, TradeStage,
    };

    fn path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "poly-bot-trade-{name}-{}-{}.jsonl",
            std::process::id(),
            std::thread::current().name().unwrap_or("thread")
        ))
    }

    fn intent(mode: ExecutionMode) -> TradeIntent {
        TradeIntent {
            plan_id: format!("arb-plan-{mode:?}").to_lowercase(),
            source_revision: "abc123".to_owned(),
            pair_id: "world-chain-usdc-wld".to_owned(),
            mode,
            direction: ArbitrageDirection::BuyTokenBOnDexSellOnCex,
            planned_token_b_base_units: 100,
            token_b_step_base_units: 1,
            expected_cost_token_a_base_units: 1_000,
            expected_proceeds_token_a_base_units: 1_030,
            dex_operation_id: "arb-plan-dex".to_owned(),
            cex_client_order_id: "arbplancex".to_owned(),
            admission: Some(super::AdmissionRiskBounds {
                opportunity_threshold_met: true,
                depth_source: None,
                depth_age_ms: None,
                depth_update_delta: None,
                top_matches: None,
                top_mismatch_reason: None,
                execution_slippage_bps: 15,
                cex_primary_limit_price: Decimal::from(1),
                cex_primary_top_quantity: Decimal::from(100),
                cex_recovery_limit_price: Decimal::from(1),
                cex_recovery_sell_limit_price: Some(Decimal::new(99, 2)),
                cex_recovery_buy_limit_price: Some(Decimal::new(101, 2)),
                recovery_quote_token_a_base_units: 1_000,
                recovery_sell_quote_token_a_base_units: 990,
                recovery_buy_quote_token_a_base_units: 1_010,
                maximum_recovery_loss_token_a_base_units: 10,
                maximum_fee_per_gas_wei: 2_500_000,
                gas_conversion_price_token_a: Decimal::from(3_000),
                maximum_gas_cost_token_a_base_units: 1,
                bounded_profit_token_a_base_units: 19,
            }),
            dex_plan: None,
        }
    }

    fn filled(token_b: i128, token_a: i128, reference: &str) -> LegResult {
        LegResult {
            status: LegStatus::Filled,
            token_b_delta_base_units: token_b,
            token_a_delta_base_units: token_a,
            gas_cost_token_a_base_units: 0,
            venue_reference: reference.to_owned(),
        }
    }

    fn failed(reference: &str) -> LegResult {
        LegResult {
            status: LegStatus::Failed,
            token_b_delta_base_units: 0,
            token_a_delta_base_units: 0,
            gas_cost_token_a_base_units: 0,
            venue_reference: reference.to_owned(),
        }
    }

    #[test]
    fn newly_admitted_trade_requires_expected_profit_after_gas_threshold() {
        let mut intent = intent(ExecutionMode::DexFirst);
        intent.expected_proceeds_token_a_base_units = 1_001;
        intent
            .admission
            .as_mut()
            .unwrap()
            .bounded_profit_token_a_base_units = 0;

        intent.validate().unwrap();

        let path = path("after-gas-threshold-not-met");
        let _ = fs::remove_file(&path);
        let mut coordinator = PaperTradeCoordinator::open(&path).unwrap();
        let error = coordinator.admit(intent).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("expected profit after gas must be at least 5 bps"),
            "{error:#}"
        );
    }

    #[test]
    fn newly_admitted_trade_requires_persisted_opportunity_threshold_proof() {
        let mut intent = intent(ExecutionMode::DexFirst);
        intent.admission.as_mut().unwrap().opportunity_threshold_met = false;

        let path = path("opportunity-threshold-not-met");
        let _ = fs::remove_file(&path);
        let mut coordinator = PaperTradeCoordinator::open(&path).unwrap();
        let error = coordinator.admit(intent).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("opportunity threshold must be met before admission"),
            "{error:#}"
        );
    }

    #[test]
    fn legacy_journal_checksum_survives_missing_default_admission_fields() {
        let mut trade_intent = intent(ExecutionMode::DexFirst);
        let admission = trade_intent.admission.as_mut().unwrap();
        admission.cex_primary_top_quantity = Decimal::ZERO;
        admission.opportunity_threshold_met = false;
        let payload = super::WirePayload {
            version: super::JOURNAL_VERSION,
            sequence: 0,
            recorded_at_unix_ms: 1_800_000_000_000,
            operation: super::TradeOperation::prepared(trade_intent),
        };
        let record = super::WireRecord::new(payload).unwrap();
        let encoded = serde_json::to_vec(&record).unwrap();
        assert!(
            !String::from_utf8_lossy(&encoded).contains("cex_primary_top_quantity"),
            "legacy-compatible default must not change the checksum payload"
        );
        assert!(
            !String::from_utf8_lossy(&encoded).contains("opportunity_threshold_met"),
            "legacy-compatible default must not change the checksum payload"
        );
        assert!(
            !String::from_utf8_lossy(&encoded).contains("depth_source"),
            "missing legacy depth metadata must not change the checksum payload"
        );

        let decoded: super::WireRecord = serde_json::from_slice(&encoded).unwrap();
        decoded.validate_checksum().unwrap();
    }

    #[test]
    fn dex_first_hedges_the_actual_dex_delta_and_records_realized_profit() {
        let path = path("dex-first");
        let _ = fs::remove_file(&path);
        let mut coordinator = PaperTradeCoordinator::open(&path).unwrap();
        let mut intent = intent(ExecutionMode::DexFirst);
        let admission = intent.admission.as_mut().unwrap();
        admission.depth_source = Some("recent_full_depth".to_owned());
        admission.depth_age_ms = Some(635);
        admission.depth_update_delta = Some(5);
        admission.top_matches = Some(false);
        admission.top_mismatch_reason = Some("bid_quantity_mismatch".to_owned());
        let plan_id = intent.plan_id.clone();
        coordinator.admit(intent).unwrap();
        assert!(matches!(
            coordinator.take_commands(&plan_id).unwrap().as_slice(),
            [CoordinatorCommand::DispatchDex {
                expected_token_b_delta_base_units: 100,
                ..
            }]
        ));
        coordinator
            .record_result(&plan_id, LegRole::Dex, filled(97, -1_000, "dex:0x1"))
            .unwrap();
        assert!(matches!(
            coordinator.take_commands(&plan_id).unwrap().as_slice(),
            [CoordinatorCommand::DispatchCex {
                target_token_b_delta_base_units: -97,
                ..
            }]
        ));
        coordinator
            .record_result(&plan_id, LegRole::Cex, filled(-97, 1_025, "cex:1"))
            .unwrap();
        assert!(coordinator.take_commands(&plan_id).unwrap().is_empty());
        let operation = coordinator.operation(&plan_id).unwrap();
        assert_eq!(operation.stage, TradeStage::BalancedProfit);
        let result = operation.result.as_ref().unwrap();
        assert_eq!(result.realized_profit_token_a_base_units, 25);
        assert_eq!(result.residual_value_token_a_base_units, 0);
        assert_eq!(result.comparable_profit_token_a_base_units, 25);
        assert_eq!(result.expected_profit_token_a_base_units, 30);
        assert_eq!(result.outcome, TerminalOutcome::BalancedProfit);
        let payload = operation
            .result_telemetry_payload("arb-bot-rust-paper")
            .unwrap();
        assert_eq!(payload["realized_profit_token_a_base_units"], "25");
        assert_eq!(payload["comparable_profit_token_a_base_units"], "25");
        assert_eq!(payload["expected_cost_token_a_base_units"], "1000");
        assert_eq!(payload["expected_proceeds_token_a_base_units"], "1030");
        assert_eq!(payload["depth_source"], "recent_full_depth");
        assert_eq!(payload["depth_age_ms"], 635);
        assert_eq!(payload["depth_update_delta"], 5);
        assert_eq!(payload["top_matches"], false);
        assert_eq!(payload["top_mismatch_reason"], "bid_quantity_mismatch");
        assert_eq!(payload["expected_profit_bps_x100"], "30000");
        assert_eq!(payload["expected_recovery_loss_token_a_base_units"], "10");
        assert_eq!(payload["recovery_loss_bound_token_a_base_units"], "10");
        assert_eq!(payload["expected_gas_cost_token_a_base_units"], "1");
        assert_eq!(
            payload["expected_gas_burdened_cost_token_a_base_units"],
            "1001"
        );
        assert_eq!(
            payload["expected_fully_burdened_cost_token_a_base_units"],
            "1011"
        );
        assert_eq!(
            payload["expected_profit_after_gas_token_a_base_units"],
            "19"
        );
        assert_eq!(payload["expected_profit_after_gas_bps_x100"], "18981");
        assert_eq!(payload["expected_bounded_profit_token_a_base_units"], "19");
        assert_eq!(payload["expected_bounded_profit_bps_x100"], "18981");
        assert_eq!(payload["realized_primary_cost_token_a_base_units"], "1000");
        assert_eq!(
            payload["realized_primary_proceeds_token_a_base_units"],
            "1025"
        );
        assert_eq!(payload["realized_primary_profit_token_a_base_units"], "25");
        assert_eq!(payload["realized_primary_profit_bps_x100"], "25000");
        assert_eq!(payload["realized_recovery_token_a_delta_base_units"], "0");
        assert_eq!(payload["realized_total_cost_token_a_base_units"], "1000");
        assert_eq!(payload["realized_bounded_profit_token_a_base_units"], "25");
        assert_eq!(payload["realized_bounded_profit_bps_x100"], "25000");
        assert_eq!(
            payload["cost_model_primary_profit_error_token_a_base_units"],
            "-5"
        );
        assert_eq!(
            payload["cost_model_bounded_profit_error_token_a_base_units"],
            "6"
        );
        assert_eq!(
            payload["cost_model_gas_cost_error_token_a_base_units"],
            "-1"
        );
        assert_eq!(
            payload["cost_model_recovery_loss_error_token_a_base_units"],
            "-10"
        );
        assert_eq!(
            payload["cost_model_comparable_profit_vs_expected_bounded_error_token_a_base_units"],
            "6"
        );
        assert_eq!(payload["execution_mode"], "dex_first");
        assert_eq!(payload["dex"]["token_b_delta_base_units"], "97");
        let expected_result = operation.result.clone();
        drop(coordinator);
        let recovered = PaperTradeCoordinator::open(&path).unwrap();
        assert_eq!(
            recovered.operation(&plan_id).unwrap().result,
            expected_result
        );
        drop(recovered);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn dex_first_halts_instead_of_recovering_in_the_opposite_direction() {
        let path = path("dex-first-direction-flip");
        let _ = fs::remove_file(&path);
        let mut coordinator = PaperTradeCoordinator::open(&path).unwrap();
        let trade_intent = intent(ExecutionMode::DexFirst);
        let plan_id = trade_intent.plan_id.clone();
        coordinator.admit(trade_intent).unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        coordinator
            .record_result(&plan_id, LegRole::Dex, filled(100, -1_000, "dex:flip"))
            .unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        coordinator
            .record_result(&plan_id, LegRole::Cex, filled(-105, 1_080, "cex:flip"))
            .unwrap();

        assert!(coordinator.take_commands(&plan_id).unwrap().is_empty());
        let operation = coordinator.operation(&plan_id).unwrap();
        assert_eq!(operation.stage, TradeStage::Halted);
        assert_eq!(
            operation.blocking_reason.as_deref(),
            Some("dex_first_recovery_direction_flipped")
        );
        drop(coordinator);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn cumulative_terminal_risk_survives_restart_for_live_admission() {
        let path = path("cumulative-risk");
        let _ = fs::remove_file(&path);
        let mut coordinator = PaperTradeCoordinator::open(&path).unwrap();
        let trade_intent = intent(ExecutionMode::DexFirst);
        let plan_id = trade_intent.plan_id.clone();
        coordinator.admit(trade_intent).unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        let mut dex = filled(100, -1_000, "dex:risk");
        dex.gas_cost_token_a_base_units = 50;
        coordinator
            .record_result(&plan_id, LegRole::Dex, dex)
            .unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        coordinator
            .record_result(&plan_id, LegRole::Cex, filled(-100, 1_020, "cex:risk"))
            .unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        assert_eq!(coordinator.cumulative_terminal_risk().unwrap(), (30, 0));
        drop(coordinator);

        let recovered = PaperTradeCoordinator::open(&path).unwrap();
        assert_eq!(recovered.cumulative_terminal_risk().unwrap(), (30, 0));
        drop(recovered);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn sub_step_dex_residual_is_terminal_but_remains_visible() {
        let path = path("sub-step-dust");
        let _ = fs::remove_file(&path);
        let mut coordinator = PaperTradeCoordinator::open(&path).unwrap();
        let mut trade_intent = intent(ExecutionMode::DexFirst);
        trade_intent.token_b_step_base_units = 10;
        let plan_id = trade_intent.plan_id.clone();
        coordinator.admit(trade_intent).unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        coordinator
            .record_result(&plan_id, LegRole::Dex, filled(97, -1_000, "dex:dust"))
            .unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        coordinator
            .record_result(&plan_id, LegRole::Cex, filled(-90, 927, "cex:dust"))
            .unwrap();

        assert!(coordinator.take_commands(&plan_id).unwrap().is_empty());
        let operation = coordinator.operation(&plan_id).unwrap();
        assert_eq!(operation.stage, TradeStage::BalancedLoss);
        let result = operation.result.as_ref().unwrap();
        assert_eq!(result.token_b_residual_base_units, 7);
        assert_eq!(result.realized_profit_token_a_base_units, -73);
        assert_eq!(result.residual_value_token_a_base_units, 69);
        assert_eq!(result.comparable_profit_token_a_base_units, -4);
        drop(coordinator);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn negative_sub_step_residual_is_marked_at_buy_depth_and_rounded_against_us() {
        let path = path("negative-sub-step-dust");
        let _ = fs::remove_file(&path);
        let mut coordinator = PaperTradeCoordinator::open(&path).unwrap();
        let mut trade_intent = intent(ExecutionMode::ConcurrentHedged);
        trade_intent.token_b_step_base_units = 10;
        let plan_id = trade_intent.plan_id.clone();
        coordinator.admit(trade_intent).unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        coordinator
            .record_result(&plan_id, LegRole::Dex, filled(97, -1_000, "dex:dust-short"))
            .unwrap();
        coordinator
            .record_result(
                &plan_id,
                LegRole::Cex,
                filled(-104, 1_070, "cex:dust-short"),
            )
            .unwrap();

        assert!(coordinator.take_commands(&plan_id).unwrap().is_empty());
        let operation = coordinator.operation(&plan_id).unwrap();
        assert_eq!(operation.stage, TradeStage::BalancedLoss);
        let result = operation.result.as_ref().unwrap();
        assert_eq!(result.token_b_residual_base_units, -7);
        assert_eq!(result.realized_profit_token_a_base_units, 70);
        assert_eq!(result.residual_value_token_a_base_units, -71);
        assert_eq!(result.comparable_profit_token_a_base_units, -1);
        drop(coordinator);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn concurrent_mode_recovers_only_the_actual_residual() {
        let path = path("concurrent");
        let _ = fs::remove_file(&path);
        let mut coordinator = PaperTradeCoordinator::open(&path).unwrap();
        let intent = intent(ExecutionMode::ConcurrentHedged);
        let plan_id = intent.plan_id.clone();
        coordinator.admit(intent).unwrap();
        let commands = coordinator.take_commands(&plan_id).unwrap();
        assert_eq!(commands.len(), 2);
        coordinator
            .record_result(&plan_id, LegRole::Dex, filled(98, -1_000, "dex:0x2"))
            .unwrap();
        coordinator
            .record_result(&plan_id, LegRole::Cex, filled(-90, 950, "cex:2"))
            .unwrap();
        assert!(matches!(
            coordinator.take_commands(&plan_id).unwrap().as_slice(),
            [CoordinatorCommand::RecoverCex {
                attempt: 1,
                target_token_b_delta_base_units: -8,
                ..
            }]
        ));
        coordinator
            .record_result(
                &plan_id,
                LegRole::RecoveryCex,
                filled(-8, 82, "cex:recovery:1"),
            )
            .unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        let operation = coordinator.operation(&plan_id).unwrap();
        assert_eq!(operation.token_b_residual_base_units(), 0);
        assert_eq!(
            operation
                .result
                .as_ref()
                .unwrap()
                .realized_profit_token_a_base_units,
            32
        );
        drop(coordinator);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn limit_residual_goes_directly_to_market_closeout() {
        let path = path("market-closeout");
        let _ = fs::remove_file(&path);
        let mut coordinator = PaperTradeCoordinator::open(&path).unwrap();
        let trade_intent = intent(ExecutionMode::DexFirst);
        let plan_id = trade_intent.plan_id.clone();
        coordinator.admit(trade_intent).unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        coordinator
            .record_result(&plan_id, LegRole::Dex, filled(100, -1_000, "dex:0x-market"))
            .unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        coordinator
            .record_result(&plan_id, LegRole::Cex, failed("cex:expired-primary"))
            .unwrap();
        assert!(matches!(
            coordinator.take_commands(&plan_id).unwrap().as_slice(),
            [CoordinatorCommand::RecoverCex {
                attempt: 1,
                target_token_b_delta_base_units: -100,
                ..
            }]
        ));
        coordinator
            .record_result(
                &plan_id,
                LegRole::RecoveryCex,
                filled(-100, 990, "cex:market-closeout"),
            )
            .unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        let operation = coordinator.operation(&plan_id).unwrap();
        assert_eq!(operation.stage, TradeStage::BalancedLoss);
        assert_eq!(operation.token_b_residual_base_units(), 0);
        drop(coordinator);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn partial_market_closeout_retries_only_remaining_residual() {
        let path = path("partial-market-closeout");
        let _ = fs::remove_file(&path);
        let mut coordinator = PaperTradeCoordinator::open(&path).unwrap();
        let trade_intent = intent(ExecutionMode::DexFirst);
        let plan_id = trade_intent.plan_id.clone();
        coordinator.admit(trade_intent).unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        coordinator
            .record_result(
                &plan_id,
                LegRole::Dex,
                filled(100, -1_000, "dex:0x-partial-market"),
            )
            .unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        coordinator
            .record_result(&plan_id, LegRole::Cex, failed("cex:expired-primary"))
            .unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        coordinator
            .record_result(
                &plan_id,
                LegRole::RecoveryCex,
                filled(-40, 396, "cex:partial-market-r1"),
            )
            .unwrap();
        assert!(matches!(
            coordinator.take_commands(&plan_id).unwrap().as_slice(),
            [CoordinatorCommand::RecoverCex {
                attempt: 2,
                target_token_b_delta_base_units: -60,
                ..
            }]
        ));
        coordinator
            .record_result(
                &plan_id,
                LegRole::RecoveryCex,
                filled(-60, 594, "cex:market-closeout-r2"),
            )
            .unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        let operation = coordinator.operation(&plan_id).unwrap();
        assert_eq!(operation.stage, TradeStage::BalancedLoss);
        assert_eq!(operation.token_b_residual_base_units(), 0);
        drop(coordinator);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn legacy_halted_after_two_limits_resumes_market_closeout() {
        let path = path("legacy-halted-market-closeout");
        let _ = fs::remove_file(&path);
        let mut coordinator = PaperTradeCoordinator::open(&path).unwrap();
        let trade_intent = intent(ExecutionMode::DexFirst);
        let plan_id = trade_intent.plan_id.clone();
        coordinator.admit(trade_intent).unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        coordinator
            .record_result(
                &plan_id,
                LegRole::Dex,
                filled(100, -1_000, "dex:legacy-halted"),
            )
            .unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        coordinator
            .record_result(&plan_id, LegRole::Cex, failed("cex:expired-primary"))
            .unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        coordinator
            .record_result(&plan_id, LegRole::RecoveryCex, failed("cex:expired-r1"))
            .unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        coordinator
            .record_result(&plan_id, LegRole::RecoveryCex, failed("cex:expired-r2"))
            .unwrap();
        let mut legacy = coordinator.operation(&plan_id).unwrap().clone();
        legacy.stage = TradeStage::Halted;
        legacy.blocking_reason = Some("recovery_attempts_exhausted".to_owned());
        coordinator.journal.append(legacy).unwrap();
        drop(coordinator);

        let mut recovered = PaperTradeCoordinator::open(&path).unwrap();
        assert!(matches!(
            recovered.take_commands(&plan_id).unwrap().as_slice(),
            [CoordinatorCommand::RecoverCex {
                attempt: 3,
                target_token_b_delta_base_units: -100,
                ..
            }]
        ));
        recovered
            .record_result(
                &plan_id,
                LegRole::RecoveryCex,
                filled(-100, 990, "cex:market-closeout"),
            )
            .unwrap();
        recovered.take_commands(&plan_id).unwrap();
        assert_eq!(
            recovered.operation(&plan_id).unwrap().stage,
            TradeStage::BalancedLoss
        );
        drop(recovered);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn concurrent_overhedge_uses_buy_market_recovery_across_restart() {
        let path = path("concurrent-overhedge");
        let _ = fs::remove_file(&path);
        let mut coordinator = PaperTradeCoordinator::open(&path).unwrap();
        let trade_intent = intent(ExecutionMode::ConcurrentHedged);
        let plan_id = trade_intent.plan_id.clone();
        coordinator.admit(trade_intent).unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        coordinator
            .record_result(&plan_id, LegRole::Dex, filled(98, -1_000, "dex:overhedge"))
            .unwrap();
        coordinator
            .record_result(&plan_id, LegRole::Cex, filled(-105, 1_080, "cex:overhedge"))
            .unwrap();
        assert!(matches!(
            coordinator.take_commands(&plan_id).unwrap().as_slice(),
            [CoordinatorCommand::RecoverCex {
                attempt: 1,
                target_token_b_delta_base_units: 7,
                ..
            }]
        ));
        drop(coordinator);

        let mut coordinator = PaperTradeCoordinator::open(&path).unwrap();
        assert!(matches!(
            coordinator.resume_command(&plan_id).unwrap(),
            Some(CoordinatorCommand::RecoverCex {
                attempt: 1,
                target_token_b_delta_base_units: 7,
                ..
            })
        ));
        coordinator
            .record_result(
                &plan_id,
                LegRole::RecoveryCex,
                filled(7, -71, "cex:recovery-buy:1"),
            )
            .unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        let result = coordinator
            .operation(&plan_id)
            .unwrap()
            .result
            .as_ref()
            .unwrap();
        assert_eq!(result.token_b_residual_base_units, 0);
        assert_eq!(result.realized_profit_token_a_base_units, 9);
        assert_eq!(result.comparable_profit_token_a_base_units, 9);
        drop(coordinator);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn dex_first_supports_cex_buy_and_dex_sell_direction() {
        let path = path("reverse-direction");
        let _ = fs::remove_file(&path);
        let mut coordinator = PaperTradeCoordinator::open(&path).unwrap();
        let mut intent = intent(ExecutionMode::DexFirst);
        intent.plan_id = "arb-plan-reverse".to_owned();
        intent.dex_operation_id = "arb-plan-reverse-dex".to_owned();
        intent.cex_client_order_id = "arbplanreversecex".to_owned();
        intent.direction = ArbitrageDirection::BuyTokenBOnCexSellOnDex;
        let plan_id = intent.plan_id.clone();
        coordinator.admit(intent).unwrap();
        assert!(matches!(
            coordinator.take_commands(&plan_id).unwrap().as_slice(),
            [CoordinatorCommand::DispatchDex {
                expected_token_b_delta_base_units: -100,
                ..
            }]
        ));
        coordinator
            .record_result(&plan_id, LegRole::Dex, filled(-100, 1_030, "dex:0x3"))
            .unwrap();
        assert!(matches!(
            coordinator.take_commands(&plan_id).unwrap().as_slice(),
            [CoordinatorCommand::DispatchCex {
                target_token_b_delta_base_units: 100,
                ..
            }]
        ));
        coordinator
            .record_result(&plan_id, LegRole::Cex, filled(100, -1_000, "cex:3"))
            .unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        assert_eq!(
            coordinator
                .operation(&plan_id)
                .unwrap()
                .result
                .as_ref()
                .unwrap()
                .realized_profit_token_a_base_units,
            30
        );
        drop(coordinator);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn unknown_outcome_blocks_new_entries_across_restart() {
        let path = path("unknown");
        let _ = fs::remove_file(&path);
        let mut coordinator = PaperTradeCoordinator::open(&path).unwrap();
        let trade_intent = intent(ExecutionMode::DexFirst);
        let plan_id = trade_intent.plan_id.clone();
        coordinator.admit(trade_intent).unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        coordinator
            .record_result(
                &plan_id,
                LegRole::Dex,
                LegResult {
                    status: LegStatus::Unknown,
                    token_b_delta_base_units: 0,
                    token_a_delta_base_units: 0,
                    gas_cost_token_a_base_units: 0,
                    venue_reference: "dex:unknown".to_owned(),
                },
            )
            .unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        assert_eq!(
            coordinator.operation(&plan_id).unwrap().stage,
            TradeStage::UnknownExposure
        );
        assert!(
            coordinator
                .admit(intent(ExecutionMode::ConcurrentHedged))
                .is_err()
        );
        drop(coordinator);
        let mut recovered = PaperTradeCoordinator::open(&path).unwrap();
        assert_eq!(recovered.active_operations().len(), 1);
        recovered
            .reconcile_unknown(
                &plan_id,
                LegRole::Dex,
                LegResult {
                    status: LegStatus::Failed,
                    token_b_delta_base_units: 0,
                    token_a_delta_base_units: 0,
                    gas_cost_token_a_base_units: 4,
                    venue_reference: "dex:reconciled-revert".to_owned(),
                },
            )
            .unwrap();
        assert!(recovered.take_commands(&plan_id).unwrap().is_empty());
        assert_eq!(
            recovered.operation(&plan_id).unwrap().stage,
            TradeStage::BalancedLoss
        );
        drop(recovered);
        fs::remove_file(path).unwrap();
    }
}

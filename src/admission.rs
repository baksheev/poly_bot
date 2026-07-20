use std::str::FromStr;

use alloy_primitives::U256;
use anyhow::{Context, ensure};
use rust_decimal::Decimal;

use crate::{
    arbitrage::ArbitrageDirection,
    binance::depth::{DepthExecutionQuote, SpotDepthBook},
    execution_accounting::native_gas_to_token_a_base_units,
    opportunity::format_base_units,
    state::TopOfBook,
};

/// The executor refuses any swap whose resolved gas limit exceeds this value.
/// Admission uses the same ceiling so an estimate increase cannot make an
/// already-admitted trade exceed its gas budget.
pub const MAX_SWAP_GAS_LIMIT: u64 = 5_000_000;
pub const RAILS_PRIORITY_FEE_WEI: u128 = 1_500_000;
pub const MAX_FEE_PER_GAS_WEI: u128 = 100_000_000_000;
const BPS_DENOMINATOR: u64 = 10_000;
pub const EXPECTED_PROFIT_AFTER_GAS_THRESHOLD_BPS: u16 = 5;

#[derive(Clone, Copy, Debug)]
pub struct AdmissionInputs<'a> {
    pub symbol: &'a str,
    pub direction: ArbitrageDirection,
    pub token_b_amount: U256,
    pub token_b_step_base_units: U256,
    pub token_a_decimals: u8,
    pub token_b_decimals: u8,
    pub binance_buy_fee_bps: u16,
    pub binance_sell_fee_bps: u16,
    pub expected_cost_token_a: U256,
    pub expected_proceeds_token_a: U256,
    /// Exact `TradeEvaluation` verdict for the configured gross venue-spread
    /// threshold. Admission preserves this proof instead of recomputing the
    /// threshold from fee/reserve-adjusted execution amounts.
    pub opportunity_threshold_met: bool,
    pub network_gas_price_wei: u128,
    pub native_price_token_a: Decimal,
    pub wallet_native_balance_wei: U256,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdmissionEconomics {
    pub primary_quantity: Decimal,
    pub recovery_limit_price: Decimal,
    pub recovery_quote_token_a: U256,
    pub recovery_sell_limit_price: Option<Decimal>,
    pub recovery_sell_quote_token_a: U256,
    pub recovery_buy_limit_price: Option<Decimal>,
    pub recovery_buy_quote_token_a: U256,
    pub recovery_loss_token_a: U256,
    pub maximum_gas_wei: U256,
    pub maximum_fee_per_gas_wei: u128,
    pub maximum_gas_cost_token_a: U256,
    /// Expected primary spread profit after venue fees and the DEX execution
    /// reserve already embedded in the candidate cost/proceeds.
    pub expected_profit_token_a: U256,
    /// Candidate cost plus maximum gas. This is the admission profitability
    /// denominator; recovery loss is recorded separately as a risk bound.
    pub gas_burdened_cost_token_a: U256,
    /// Expected profit after the maximum gas budget. This is the hard broadcast
    /// gate. Recovery loss is deliberately not subtracted here.
    pub expected_profit_after_gas_token_a: U256,
    /// Worst-case capital-at-risk diagnostic. This is intentionally not the
    /// normal opportunity threshold or adaptive-sizing objective.
    pub fully_burdened_cost_token_a: U256,
    pub bounded_profit_token_a: U256,
    pub opportunity_threshold_met: bool,
    pub expected_profit_after_gas_threshold_bps: u16,
    pub expected_profit_after_gas_threshold_met: bool,
    pub native_gas_covered: bool,
    pub meets_threshold: bool,
}

/// Returns `None` when the local sequence-consistent book cannot fill the
/// entire recovery quantity. Arithmetic and invariant failures remain errors.
pub fn evaluate_admission(
    depth: &SpotDepthBook,
    inputs: AdmissionInputs<'_>,
) -> anyhow::Result<Option<AdmissionEconomics>> {
    ensure!(
        depth.symbol() == inputs.symbol,
        "admission depth symbol mismatch"
    );
    let base_quantity = validate_inputs_and_base_quantity(inputs)?;
    let recovery_buy_quantity = recovery_buy_base_quantity(inputs)?;
    let Some(sell_depth_quote) = depth.quote_market_sell(base_quantity)? else {
        return Ok(None);
    };
    let Some(buy_depth_quote) = depth.quote_market_buy(recovery_buy_quantity)? else {
        return Ok(None);
    };
    finish_admission(
        Some(sell_depth_quote),
        Some(buy_depth_quote),
        base_quantity,
        inputs,
    )
}

/// Fast admission for the production DEX-first coordinator. The primary IOC
/// is sized from the exact real-time top level, and a DEX-first residual can
/// only continue in that same CEX direction. The coordinator enforces that
/// direction invariant before dispatching recovery work.
pub fn evaluate_dex_first_admission(
    quote: &TopOfBook,
    inputs: AdmissionInputs<'_>,
) -> anyhow::Result<Option<AdmissionEconomics>> {
    ensure!(
        quote.symbol.as_ref() == inputs.symbol,
        "admission quote symbol mismatch"
    );
    let base_quantity = validate_inputs_and_base_quantity(inputs)?;
    let recovery_buy_quantity = recovery_buy_base_quantity(inputs)?;
    let (sell_quote, buy_quote) = match inputs.direction {
        ArbitrageDirection::BuyTokenBOnDexSellOnCex => {
            if quote.bid_quantity < base_quantity {
                return Ok(None);
            }
            (
                Some(DepthExecutionQuote {
                    base_quantity,
                    quote_quantity: base_quantity
                        .checked_mul(quote.bid_price)
                        .context("top-level sell quote overflow")?,
                    worst_price: quote.bid_price,
                    levels_consumed: 1,
                }),
                None,
            )
        }
        ArbitrageDirection::BuyTokenBOnCexSellOnDex => {
            if quote.ask_quantity < recovery_buy_quantity {
                return Ok(None);
            }
            (
                None,
                Some(DepthExecutionQuote {
                    base_quantity: recovery_buy_quantity,
                    quote_quantity: recovery_buy_quantity
                        .checked_mul(quote.ask_price)
                        .context("top-level buy quote overflow")?,
                    worst_price: quote.ask_price,
                    levels_consumed: 1,
                }),
            )
        }
    };
    finish_admission(sell_quote, buy_quote, base_quantity, inputs)
}

fn validate_inputs_and_base_quantity(inputs: AdmissionInputs<'_>) -> anyhow::Result<Decimal> {
    ensure!(
        !inputs.token_b_amount.is_zero(),
        "admission token-B amount is zero"
    );
    ensure!(
        !inputs.token_b_step_base_units.is_zero(),
        "admission token-B step is zero"
    );
    ensure!(
        inputs.token_b_amount % inputs.token_b_step_base_units == U256::ZERO,
        "admission token-B amount is not step aligned"
    );
    ensure!(
        inputs.network_gas_price_wei > 0,
        "network gas price is zero"
    );
    ensure!(
        inputs.native_price_token_a > Decimal::ZERO,
        "native token-A price is non-positive"
    );
    ensure!(
        inputs.expected_cost_token_a > U256::ZERO && inputs.expected_proceeds_token_a > U256::ZERO,
        "admission economics are non-positive"
    );
    if let Ok(amount) = u128::try_from(inputs.token_b_amount)
        && amount <= i128::MAX as u128
    {
        return Ok(Decimal::from_i128_with_scale(
            amount as i128,
            u32::from(inputs.token_b_decimals),
        ));
    }
    Decimal::from_str(&format_base_units(
        inputs.token_b_amount,
        inputs.token_b_decimals,
    ))
    .context("token-B amount exceeds Decimal admission range")
}

fn recovery_buy_base_quantity(inputs: AdmissionInputs<'_>) -> anyhow::Result<Decimal> {
    ensure!(
        inputs.binance_buy_fee_bps < 10_000,
        "Binance BUY fee must be below 100%"
    );
    let retention_bps = 10_000_u32
        .checked_sub(u32::from(inputs.binance_buy_fee_bps))
        .context("Binance BUY fee retention underflow")?;
    let numerator = inputs
        .token_b_amount
        .checked_mul(U256::from(10_000_u32))
        .context("recovery BUY gross quantity overflow")?;
    let denominator = U256::from(retention_bps);
    let gross = numerator / denominator;
    let gross = if numerator % denominator == U256::ZERO {
        gross
    } else {
        gross
            .checked_add(U256::ONE)
            .context("recovery BUY gross quantity rounding overflow")?
    };
    let step = inputs.token_b_step_base_units;
    let gross = if gross % step == U256::ZERO {
        gross
    } else {
        (gross / step)
            .checked_add(U256::ONE)
            .and_then(|steps| steps.checked_mul(step))
            .context("recovery BUY step rounding overflow")?
    };
    validate_inputs_and_base_quantity(AdmissionInputs {
        token_b_amount: gross,
        ..inputs
    })
}

fn finish_admission(
    sell_depth_quote: Option<DepthExecutionQuote>,
    buy_depth_quote: Option<DepthExecutionQuote>,
    primary_quantity: Decimal,
    inputs: AdmissionInputs<'_>,
) -> anyhow::Result<Option<AdmissionEconomics>> {
    let recovery_sell_quote_token_a = match sell_depth_quote.as_ref() {
        Some(quote) => subtract_bps_floor(
            decimal_to_base_units(quote.quote_quantity, inputs.token_a_decimals, false)?,
            inputs.binance_sell_fee_bps,
        )?,
        None => U256::ZERO,
    };
    let recovery_buy_quote_token_a = match buy_depth_quote.as_ref() {
        Some(quote) => decimal_to_base_units(quote.quote_quantity, inputs.token_a_decimals, true)?,
        None => U256::ZERO,
    };
    let (recovery_limit_price, recovery_quote_token_a, sell_loss, buy_loss) = match inputs.direction
    {
        ArbitrageDirection::BuyTokenBOnDexSellOnCex => (
            sell_depth_quote
                .as_ref()
                .context("DEX-buy admission has no sell quote")?
                .worst_price,
            recovery_sell_quote_token_a,
            inputs
                .expected_proceeds_token_a
                .saturating_sub(recovery_sell_quote_token_a),
            if buy_depth_quote.is_some() {
                recovery_buy_quote_token_a.saturating_sub(inputs.expected_proceeds_token_a)
            } else {
                U256::ZERO
            },
        ),
        ArbitrageDirection::BuyTokenBOnCexSellOnDex => (
            buy_depth_quote
                .as_ref()
                .context("DEX-sell admission has no buy quote")?
                .worst_price,
            recovery_buy_quote_token_a,
            if sell_depth_quote.is_some() {
                inputs
                    .expected_cost_token_a
                    .saturating_sub(recovery_sell_quote_token_a)
            } else {
                U256::ZERO
            },
            recovery_buy_quote_token_a.saturating_sub(inputs.expected_cost_token_a),
        ),
    };
    let recovery_loss_token_a = sell_loss.max(buy_loss);

    let maximum_fee_per_gas = inputs
        .network_gas_price_wei
        .checked_add(RAILS_PRIORITY_FEE_WEI)
        .context("admission max fee per gas overflow")?;
    ensure!(
        maximum_fee_per_gas <= MAX_FEE_PER_GAS_WEI,
        "admission max fee per gas exceeds executor cap"
    );
    let maximum_gas_wei = U256::from(MAX_SWAP_GAS_LIMIT)
        .checked_mul(U256::from(maximum_fee_per_gas))
        .context("admission maximum gas overflow")?;
    let maximum_gas_cost_token_a = U256::from(native_gas_to_token_a_base_units(
        MAX_SWAP_GAS_LIMIT,
        maximum_fee_per_gas,
        inputs.native_price_token_a,
        inputs.token_a_decimals,
    )?);
    let expected_profit_token_a = inputs
        .expected_proceeds_token_a
        .saturating_sub(inputs.expected_cost_token_a);
    let gas_burdened_cost_token_a = inputs
        .expected_cost_token_a
        .checked_add(maximum_gas_cost_token_a)
        .context("gas-burdened admission cost overflow")?;
    let fully_burdened_cost_token_a = inputs
        .expected_cost_token_a
        .checked_add(recovery_loss_token_a)
        .and_then(|cost| cost.checked_add(maximum_gas_cost_token_a))
        .context("fully burdened admission cost overflow")?;
    let expected_profit_after_gas_token_a = inputs
        .expected_proceeds_token_a
        .saturating_sub(gas_burdened_cost_token_a);
    let expected_profit_after_gas_threshold_met = meets_threshold(
        inputs.expected_proceeds_token_a,
        gas_burdened_cost_token_a,
        EXPECTED_PROFIT_AFTER_GAS_THRESHOLD_BPS,
    )?;
    let meets_threshold =
        inputs.opportunity_threshold_met && expected_profit_after_gas_threshold_met;

    Ok(Some(AdmissionEconomics {
        primary_quantity,
        recovery_limit_price,
        recovery_quote_token_a,
        recovery_sell_limit_price: sell_depth_quote.map(|quote| quote.worst_price),
        recovery_sell_quote_token_a,
        recovery_buy_limit_price: buy_depth_quote.map(|quote| quote.worst_price),
        recovery_buy_quote_token_a,
        recovery_loss_token_a,
        maximum_gas_wei,
        maximum_fee_per_gas_wei: maximum_fee_per_gas,
        maximum_gas_cost_token_a,
        expected_profit_token_a,
        gas_burdened_cost_token_a,
        expected_profit_after_gas_token_a,
        fully_burdened_cost_token_a,
        bounded_profit_token_a: expected_profit_after_gas_token_a,
        opportunity_threshold_met: inputs.opportunity_threshold_met,
        expected_profit_after_gas_threshold_bps: EXPECTED_PROFIT_AFTER_GAS_THRESHOLD_BPS,
        expected_profit_after_gas_threshold_met,
        native_gas_covered: inputs.wallet_native_balance_wei >= maximum_gas_wei,
        meets_threshold,
    }))
}

fn decimal_to_base_units(value: Decimal, decimals: u8, round_up: bool) -> anyhow::Result<U256> {
    ensure!(
        value >= Decimal::ZERO,
        "admission decimal amount is negative"
    );
    let mantissa =
        u128::try_from(value.mantissa()).context("admission decimal mantissa is negative")?;
    let numerator = U256::from(mantissa)
        .checked_mul(pow10(u32::from(decimals))?)
        .context("admission decimal numerator overflow")?;
    let denominator = pow10(value.scale())?;
    let quotient = numerator / denominator;
    if round_up && numerator % denominator != U256::ZERO {
        quotient
            .checked_add(U256::ONE)
            .context("admission rounded amount overflow")
    } else {
        Ok(quotient)
    }
}

fn subtract_bps_floor(value: U256, bps: u16) -> anyhow::Result<U256> {
    ensure!(bps <= 10_000, "admission fee exceeds 100%");
    value
        .checked_mul(U256::from(10_000_u64 - u64::from(bps)))
        .map(|value| value / U256::from(10_000_u64))
        .context("admission fee multiplication overflow")
}

fn meets_threshold(proceeds: U256, cost: U256, threshold_bps: u16) -> anyhow::Result<bool> {
    if let (Ok(proceeds), Ok(cost)) = (u128::try_from(proceeds), u128::try_from(cost))
        && let (Some(left), Some(right)) = (
            proceeds.checked_mul(u128::from(BPS_DENOMINATOR)),
            cost.checked_mul(u128::from(
                BPS_DENOMINATOR
                    .checked_add(u64::from(threshold_bps))
                    .context("admission threshold overflow")?,
            )),
        )
    {
        return Ok(left >= right);
    }
    let left = proceeds
        .checked_mul(U256::from(BPS_DENOMINATOR))
        .context("admission threshold proceeds overflow")?;
    let right = cost
        .checked_mul(U256::from(
            BPS_DENOMINATOR
                .checked_add(u64::from(threshold_bps))
                .context("admission threshold overflow")?,
        ))
        .context("admission threshold cost overflow")?;
    Ok(left >= right)
}

fn pow10(exponent: u32) -> anyhow::Result<U256> {
    let mut value = U256::ONE;
    for _ in 0..exponent {
        value = value
            .checked_mul(U256::from(10_u8))
            .context("admission decimal scale overflow")?;
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc, time::Instant};

    use alloy_primitives::U256;
    use rust_decimal::Decimal;

    use crate::{
        admission::{AdmissionInputs, evaluate_admission, evaluate_dex_first_admission},
        arbitrage::ArbitrageDirection,
        binance::depth::{DepthLevel, DepthSnapshot, SpotDepthBook},
        state::TopOfBook,
    };

    fn book() -> SpotDepthBook {
        SpotDepthBook::from_snapshot(
            "WLDUSDC".to_owned(),
            DepthSnapshot {
                last_update_id: 10,
                bids: vec![
                    DepthLevel {
                        price: Decimal::from_str("1.00").unwrap(),
                        quantity: Decimal::from(5),
                    },
                    DepthLevel {
                        price: Decimal::from_str("0.99").unwrap(),
                        quantity: Decimal::from(10),
                    },
                ],
                asks: vec![
                    DepthLevel {
                        price: Decimal::from_str("1.01").unwrap(),
                        quantity: Decimal::from(5),
                    },
                    DepthLevel {
                        price: Decimal::from_str("1.02").unwrap(),
                        quantity: Decimal::from(10),
                    },
                ],
            },
        )
        .unwrap()
    }

    fn inputs(direction: ArbitrageDirection) -> AdmissionInputs<'static> {
        AdmissionInputs {
            symbol: "WLDUSDC",
            direction,
            token_b_amount: U256::from(10_u8) * U256::from(10_u64).pow(U256::from(18)),
            token_b_step_base_units: U256::from(10_u64).pow(U256::from(17)),
            token_a_decimals: 6,
            token_b_decimals: 18,
            binance_buy_fee_bps: 10,
            binance_sell_fee_bps: 10,
            expected_cost_token_a: U256::from(10_000_000_u64),
            expected_proceeds_token_a: U256::from(10_300_000_u64),
            opportunity_threshold_met: true,
            network_gas_price_wei: 1_000_000,
            native_price_token_a: Decimal::from(3_000),
            wallet_native_balance_wei: U256::from(10_u8).pow(U256::from(18)),
        }
    }

    fn top_of_book(bid_quantity: Decimal, ask_quantity: Decimal) -> TopOfBook {
        TopOfBook::new(
            Arc::from("WLDUSDC"),
            11,
            Decimal::ONE,
            bid_quantity,
            Decimal::from_str("1.01").unwrap(),
            ask_quantity,
            None,
            None,
            Instant::now(),
            1_800_000_000_000_000,
            1,
        )
        .unwrap()
    }

    #[test]
    fn dex_first_sell_admission_uses_only_relevant_bid_top() {
        let quote = top_of_book(Decimal::from(10), Decimal::ONE);
        let economics = evaluate_dex_first_admission(
            &quote,
            inputs(ArbitrageDirection::BuyTokenBOnDexSellOnCex),
        )
        .unwrap()
        .unwrap();

        assert_eq!(economics.primary_quantity, Decimal::from(10));
        assert_eq!(economics.recovery_limit_price, Decimal::ONE);
        assert_eq!(economics.recovery_sell_limit_price, Some(Decimal::ONE));
        assert_eq!(economics.recovery_buy_limit_price, None);
        assert_eq!(
            economics.recovery_sell_quote_token_a,
            U256::from(9_990_000_u64)
        );
        assert_eq!(economics.recovery_buy_quote_token_a, U256::ZERO);
    }

    #[test]
    fn dex_first_admission_requires_positive_after_gas_profit() {
        let quote = top_of_book(Decimal::from(10), Decimal::ONE);
        let mut request = inputs(ArbitrageDirection::BuyTokenBOnDexSellOnCex);
        request.expected_cost_token_a = U256::from(10_010_000_u64);
        request.expected_proceeds_token_a = U256::from(10_000_000_u64);

        let economics = evaluate_dex_first_admission(&quote, request)
            .unwrap()
            .unwrap();

        assert!(economics.opportunity_threshold_met);
        assert!(!economics.expected_profit_after_gas_threshold_met);
        assert!(!economics.meets_threshold);
        assert_eq!(economics.expected_profit_token_a, U256::ZERO);
        assert_eq!(economics.expected_profit_after_gas_token_a, U256::ZERO);
        assert_eq!(economics.bounded_profit_token_a, U256::ZERO);
    }

    #[test]
    fn dex_first_admission_defers_when_relevant_top_is_too_small() {
        let quote = top_of_book(Decimal::from(9), Decimal::from(100));

        assert!(
            evaluate_dex_first_admission(
                &quote,
                inputs(ArbitrageDirection::BuyTokenBOnDexSellOnCex),
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn dex_first_buy_admission_uses_only_relevant_ask_top() {
        let quote = top_of_book(Decimal::ONE, Decimal::new(101, 1));
        let mut request = inputs(ArbitrageDirection::BuyTokenBOnCexSellOnDex);
        request.expected_proceeds_token_a = U256::from(10_500_000_u64);
        let economics = evaluate_dex_first_admission(&quote, request)
            .unwrap()
            .unwrap();

        assert_eq!(economics.primary_quantity, Decimal::from(10));
        assert_eq!(
            economics.recovery_limit_price,
            Decimal::from_str("1.01").unwrap()
        );
        assert_eq!(economics.recovery_sell_limit_price, None);
        assert_eq!(
            economics.recovery_buy_limit_price,
            Some(Decimal::from_str("1.01").unwrap())
        );
        assert_eq!(economics.recovery_sell_quote_token_a, U256::ZERO);
        assert_eq!(
            economics.recovery_buy_quote_token_a,
            U256::from(10_201_000_u64)
        );
    }

    #[test]
    fn charges_full_depth_recovery_and_conservative_maximum_gas() {
        let economics =
            evaluate_admission(&book(), inputs(ArbitrageDirection::BuyTokenBOnDexSellOnCex))
                .unwrap()
                .unwrap();

        // 5 * 1.00 + 5 * 0.99 = 9.95, then 10 bps sell commission.
        assert_eq!(economics.recovery_quote_token_a, U256::from(9_940_050_u64));
        assert_eq!(
            economics.recovery_sell_quote_token_a,
            U256::from(9_940_050_u64)
        );
        assert_eq!(
            economics.recovery_buy_quote_token_a,
            U256::from(10_252_000_u64)
        );
        assert_eq!(
            economics.recovery_limit_price,
            Decimal::from_str("0.99").unwrap()
        );
        assert_eq!(
            economics.recovery_sell_limit_price,
            Some(Decimal::from_str("0.99").unwrap())
        );
        assert_eq!(
            economics.recovery_buy_limit_price,
            Some(Decimal::from_str("1.02").unwrap())
        );
        assert_eq!(economics.recovery_loss_token_a, U256::from(359_950_u64));
        assert_eq!(
            economics.maximum_gas_wei,
            U256::from(12_500_000_000_000_u64)
        );
        assert_eq!(economics.maximum_gas_cost_token_a, U256::from(37_500_u64));
        assert_eq!(economics.expected_profit_token_a, U256::from(300_000_u64));
        assert_eq!(
            economics.gas_burdened_cost_token_a,
            U256::from(10_037_500_u64)
        );
        assert_eq!(
            economics.fully_burdened_cost_token_a,
            U256::from(10_397_450_u64)
        );
        assert_eq!(
            economics.expected_profit_after_gas_token_a,
            U256::from(262_500_u64)
        );
        assert_eq!(
            economics.bounded_profit_token_a,
            economics.expected_profit_after_gas_token_a
        );
        assert!(economics.opportunity_threshold_met);
        assert_eq!(economics.expected_profit_after_gas_threshold_bps, 5);
        assert!(economics.expected_profit_after_gas_threshold_met);
        assert!(economics.native_gas_covered);
        assert!(economics.meets_threshold);
    }

    #[test]
    fn after_gas_threshold_is_the_admission_profitability_gate() {
        let mut request = inputs(ArbitrageDirection::BuyTokenBOnDexSellOnCex);
        request.expected_proceeds_token_a = U256::from(10_001_000_u64);
        let rejected = evaluate_admission(&book(), request).unwrap().unwrap();

        assert!(rejected.opportunity_threshold_met);
        assert!(!rejected.expected_profit_after_gas_threshold_met);
        assert!(!rejected.meets_threshold);
        assert_eq!(rejected.expected_profit_token_a, U256::from(1_000_u64));
        assert_eq!(rejected.bounded_profit_token_a, U256::ZERO);

        request.expected_proceeds_token_a = U256::from(10_040_000_u64);
        let below_threshold = evaluate_admission(&book(), request).unwrap().unwrap();
        assert!(below_threshold.opportunity_threshold_met);
        assert_eq!(
            below_threshold.expected_profit_after_gas_token_a,
            U256::from(2_500_u64)
        );
        assert!(!below_threshold.expected_profit_after_gas_threshold_met);
        assert!(!below_threshold.meets_threshold);

        request.expected_proceeds_token_a = U256::from(10_088_000_u64);
        let admitted = evaluate_admission(&book(), request).unwrap().unwrap();
        assert!(admitted.opportunity_threshold_met);
        assert!(admitted.expected_profit_after_gas_threshold_met);
        assert!(admitted.meets_threshold);
        assert_eq!(
            admitted.expected_profit_after_gas_token_a,
            U256::from(50_500_u64)
        );

        request.opportunity_threshold_met = false;
        let gross_rejected = evaluate_admission(&book(), request).unwrap().unwrap();
        assert!(!gross_rejected.opportunity_threshold_met);
        assert!(gross_rejected.expected_profit_after_gas_threshold_met);
        assert!(!gross_rejected.meets_threshold);
    }

    #[test]
    fn buy_recovery_rounds_cost_up_and_requires_complete_depth() {
        let mut request = inputs(ArbitrageDirection::BuyTokenBOnCexSellOnDex);
        request.expected_cost_token_a = U256::from(10_000_000_u64);
        request.expected_proceeds_token_a = U256::from(10_500_000_u64);
        let economics = evaluate_admission(&book(), request).unwrap().unwrap();
        // A 10 WLD target with 10 bps base-asset commission grosses up to
        // 10.1 WLD at the 0.1 step: 5 * 1.01 + 5.1 * 1.02 = 10.252.
        assert_eq!(economics.recovery_quote_token_a, U256::from(10_252_000_u64));
        assert_eq!(
            economics.recovery_sell_quote_token_a,
            U256::from(9_940_050_u64)
        );
        assert_eq!(
            economics.recovery_buy_quote_token_a,
            U256::from(10_252_000_u64)
        );
        assert_eq!(
            economics.recovery_limit_price,
            Decimal::from_str("1.02").unwrap()
        );
        assert_eq!(economics.recovery_loss_token_a, U256::from(252_000_u64));

        request.token_b_amount = U256::from(16_u8) * U256::from(10_u64).pow(U256::from(18));
        assert!(evaluate_admission(&book(), request).unwrap().is_none());
    }

    #[test]
    fn reports_native_gas_reserve_shortage() {
        let mut request = inputs(ArbitrageDirection::BuyTokenBOnDexSellOnCex);
        request.wallet_native_balance_wei = U256::from(1_u8);
        let economics = evaluate_admission(&book(), request).unwrap().unwrap();
        assert!(!economics.native_gas_covered);
    }
}

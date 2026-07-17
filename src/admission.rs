use std::str::FromStr;

use alloy_primitives::U256;
use anyhow::{Context, ensure};
use rust_decimal::Decimal;

use crate::{
    arbitrage::ArbitrageDirection, binance::depth::SpotDepthBook,
    execution_accounting::native_gas_to_token_a_base_units, opportunity::format_base_units,
};

/// The executor refuses any swap whose resolved gas limit exceeds this value.
/// Admission uses the same ceiling so an estimate increase cannot make an
/// already-admitted trade exceed its gas budget.
pub const MAX_SWAP_GAS_LIMIT: u64 = 5_000_000;
pub const RAILS_PRIORITY_FEE_WEI: u128 = 1_500_000;
pub const MAX_FEE_PER_GAS_WEI: u128 = 100_000_000_000;

#[derive(Clone, Copy, Debug)]
pub struct AdmissionInputs<'a> {
    pub symbol: &'a str,
    pub direction: ArbitrageDirection,
    pub token_b_amount: U256,
    pub token_a_decimals: u8,
    pub token_b_decimals: u8,
    pub binance_buy_fee_bps: u16,
    pub binance_sell_fee_bps: u16,
    pub expected_cost_token_a: U256,
    pub expected_proceeds_token_a: U256,
    pub opportunity_threshold_bps: u16,
    pub network_gas_price_wei: u128,
    pub native_price_token_a: Decimal,
    pub wallet_native_balance_wei: U256,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdmissionEconomics {
    pub recovery_limit_price: Decimal,
    pub recovery_quote_token_a: U256,
    pub recovery_sell_limit_price: Decimal,
    pub recovery_sell_quote_token_a: U256,
    pub recovery_buy_limit_price: Decimal,
    pub recovery_buy_quote_token_a: U256,
    pub recovery_loss_token_a: U256,
    pub maximum_gas_wei: U256,
    pub maximum_fee_per_gas_wei: u128,
    pub maximum_gas_cost_token_a: U256,
    pub fully_burdened_cost_token_a: U256,
    pub bounded_profit_token_a: U256,
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
    ensure!(
        !inputs.token_b_amount.is_zero(),
        "admission token-B amount is zero"
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

    let base_quantity = Decimal::from_str(&format_base_units(
        inputs.token_b_amount,
        inputs.token_b_decimals,
    ))
    .context("token-B amount exceeds Decimal admission range")?;
    let Some(sell_depth_quote) = depth.quote_market_sell(base_quantity)? else {
        return Ok(None);
    };
    let Some(buy_depth_quote) = depth.quote_market_buy(base_quantity)? else {
        return Ok(None);
    };
    let recovery_sell_quote_token_a = subtract_bps_floor(
        decimal_to_base_units(
            sell_depth_quote.quote_quantity,
            inputs.token_a_decimals,
            false,
        )?,
        inputs.binance_sell_fee_bps,
    )?;
    let recovery_buy_quote_token_a = add_bps_ceil(
        decimal_to_base_units(
            buy_depth_quote.quote_quantity,
            inputs.token_a_decimals,
            true,
        )?,
        inputs.binance_buy_fee_bps,
    )?;
    let (recovery_limit_price, recovery_quote_token_a, sell_loss, buy_loss) = match inputs.direction
    {
        ArbitrageDirection::BuyTokenBOnDexSellOnCex => (
            sell_depth_quote.worst_price,
            recovery_sell_quote_token_a,
            inputs
                .expected_proceeds_token_a
                .saturating_sub(recovery_sell_quote_token_a),
            recovery_buy_quote_token_a.saturating_sub(inputs.expected_proceeds_token_a),
        ),
        ArbitrageDirection::BuyTokenBOnCexSellOnDex => (
            buy_depth_quote.worst_price,
            recovery_buy_quote_token_a,
            inputs
                .expected_cost_token_a
                .saturating_sub(recovery_sell_quote_token_a),
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
    let fully_burdened_cost_token_a = inputs
        .expected_cost_token_a
        .checked_add(recovery_loss_token_a)
        .and_then(|cost| cost.checked_add(maximum_gas_cost_token_a))
        .context("fully burdened admission cost overflow")?;
    let bounded_profit_token_a = inputs
        .expected_proceeds_token_a
        .saturating_sub(fully_burdened_cost_token_a);
    let meets_threshold = meets_threshold(
        inputs.expected_proceeds_token_a,
        fully_burdened_cost_token_a,
        inputs.opportunity_threshold_bps,
    )?;

    Ok(Some(AdmissionEconomics {
        recovery_limit_price,
        recovery_quote_token_a,
        recovery_sell_limit_price: sell_depth_quote.worst_price,
        recovery_sell_quote_token_a,
        recovery_buy_limit_price: buy_depth_quote.worst_price,
        recovery_buy_quote_token_a,
        recovery_loss_token_a,
        maximum_gas_wei,
        maximum_fee_per_gas_wei: maximum_fee_per_gas,
        maximum_gas_cost_token_a,
        fully_burdened_cost_token_a,
        bounded_profit_token_a,
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

fn add_bps_ceil(value: U256, bps: u16) -> anyhow::Result<U256> {
    ensure!(bps <= 10_000, "admission fee exceeds 100%");
    let numerator = value
        .checked_mul(U256::from(10_000_u64 + u64::from(bps)))
        .context("admission fee multiplication overflow")?;
    let denominator = U256::from(10_000_u64);
    let quotient = numerator / denominator;
    if numerator % denominator == U256::ZERO {
        Ok(quotient)
    } else {
        quotient
            .checked_add(U256::ONE)
            .context("admission fee rounding overflow")
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
    let left = proceeds
        .checked_mul(U256::from(10_000_u64))
        .context("admission threshold proceeds overflow")?;
    let right = cost
        .checked_mul(U256::from(10_000_u64 + u64::from(threshold_bps)))
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
    use std::str::FromStr;

    use alloy_primitives::U256;
    use rust_decimal::Decimal;

    use crate::{
        admission::{AdmissionInputs, evaluate_admission},
        arbitrage::ArbitrageDirection,
        binance::depth::{DepthLevel, DepthSnapshot, SpotDepthBook},
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
            token_a_decimals: 6,
            token_b_decimals: 18,
            binance_buy_fee_bps: 10,
            binance_sell_fee_bps: 10,
            expected_cost_token_a: U256::from(10_000_000_u64),
            expected_proceeds_token_a: U256::from(10_300_000_u64),
            opportunity_threshold_bps: 20,
            network_gas_price_wei: 1_000_000,
            native_price_token_a: Decimal::from(3_000),
            wallet_native_balance_wei: U256::from(10_u8).pow(U256::from(18)),
        }
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
            U256::from(10_160_150_u64)
        );
        assert_eq!(
            economics.recovery_limit_price,
            Decimal::from_str("0.99").unwrap()
        );
        assert_eq!(
            economics.recovery_sell_limit_price,
            Decimal::from_str("0.99").unwrap()
        );
        assert_eq!(
            economics.recovery_buy_limit_price,
            Decimal::from_str("1.02").unwrap()
        );
        assert_eq!(economics.recovery_loss_token_a, U256::from(359_950_u64));
        assert_eq!(
            economics.maximum_gas_wei,
            U256::from(12_500_000_000_000_u64)
        );
        assert_eq!(economics.maximum_gas_cost_token_a, U256::from(37_500_u64));
        assert!(economics.native_gas_covered);
        assert!(!economics.meets_threshold);
    }

    #[test]
    fn buy_recovery_rounds_cost_up_and_requires_complete_depth() {
        let mut request = inputs(ArbitrageDirection::BuyTokenBOnCexSellOnDex);
        request.expected_cost_token_a = U256::from(10_000_000_u64);
        request.expected_proceeds_token_a = U256::from(10_500_000_u64);
        let economics = evaluate_admission(&book(), request).unwrap().unwrap();
        // 5 * 1.01 + 5 * 1.02 = 10.15, then 10 bps buy commission, rounded up.
        assert_eq!(economics.recovery_quote_token_a, U256::from(10_160_150_u64));
        assert_eq!(
            economics.recovery_sell_quote_token_a,
            U256::from(9_940_050_u64)
        );
        assert_eq!(
            economics.recovery_buy_quote_token_a,
            U256::from(10_160_150_u64)
        );
        assert_eq!(
            economics.recovery_limit_price,
            Decimal::from_str("1.02").unwrap()
        );
        assert_eq!(economics.recovery_loss_token_a, U256::from(160_150_u64));

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

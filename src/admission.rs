use std::str::FromStr;

use alloy_primitives::U256;
use anyhow::{Context, ensure};
use rust_decimal::Decimal;

use crate::{arbitrage::ArbitrageDirection, opportunity::format_base_units, state::TopOfBook};

#[derive(Clone, Copy, Debug)]
pub struct AdmissionInputs<'a> {
    pub symbol: &'a str,
    pub direction: ArbitrageDirection,
    pub token_b_amount: U256,
    pub token_b_step_base_units: U256,
    pub token_b_decimals: u8,
    pub expected_cost_token_a: U256,
    pub expected_proceeds_token_a: U256,
    pub opportunity_threshold_met: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdmissionEconomics {
    pub primary_quantity: Decimal,
    /// Compatibility fields retained in the journal shape. They are not
    /// recovery forecasts and never gate admission.
    pub recovery_limit_price: Decimal,
    pub recovery_quote_token_a: U256,
    pub recovery_sell_limit_price: Option<Decimal>,
    pub recovery_sell_quote_token_a: U256,
    pub recovery_buy_limit_price: Option<Decimal>,
    pub recovery_buy_quote_token_a: U256,
    pub opportunity_threshold_met: bool,
}

/// Builds the immutable primary execution envelope without predicting a
/// recovery fill or consulting Binance depth. Recovery is reactive to the
/// actual primary result.
pub fn evaluate_execution_admission(
    quote: &TopOfBook,
    inputs: AdmissionInputs<'_>,
) -> anyhow::Result<AdmissionEconomics> {
    ensure!(
        quote.symbol.as_ref() == inputs.symbol,
        "admission quote symbol mismatch"
    );
    let primary_quantity = validate_inputs_and_base_quantity(inputs)?;
    let (recovery_limit_price, recovery_quote_token_a) = match inputs.direction {
        ArbitrageDirection::BuyTokenBOnDexSellOnCex => {
            (quote.bid_price, inputs.expected_proceeds_token_a)
        }
        ArbitrageDirection::BuyTokenBOnCexSellOnDex => {
            (quote.ask_price, inputs.expected_cost_token_a)
        }
    };
    Ok(AdmissionEconomics {
        primary_quantity,
        recovery_limit_price,
        recovery_quote_token_a,
        recovery_sell_limit_price: None,
        recovery_sell_quote_token_a: U256::ZERO,
        recovery_buy_limit_price: None,
        recovery_buy_quote_token_a: U256::ZERO,
        opportunity_threshold_met: inputs.opportunity_threshold_met,
    })
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

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc, time::Instant};

    use alloy_primitives::U256;
    use rust_decimal::Decimal;

    use crate::{
        admission::{AdmissionInputs, evaluate_execution_admission},
        arbitrage::ArbitrageDirection,
        state::TopOfBook,
    };

    fn top_of_book() -> TopOfBook {
        TopOfBook::new(
            Arc::from("WLDUSDC"),
            11,
            Decimal::ONE,
            Decimal::ONE,
            Decimal::from_str("1.01").unwrap(),
            Decimal::ONE,
            None,
            None,
            Instant::now(),
            1_800_000_000_000_000,
            1,
        )
        .unwrap()
    }

    fn inputs() -> AdmissionInputs<'static> {
        AdmissionInputs {
            symbol: "WLDUSDC",
            direction: ArbitrageDirection::BuyTokenBOnDexSellOnCex,
            token_b_amount: U256::from(10_u8) * U256::from(10_u64).pow(U256::from(18)),
            token_b_step_base_units: U256::from(10_u64).pow(U256::from(17)),
            token_b_decimals: 18,
            expected_cost_token_a: U256::from(10_000_000_u64),
            expected_proceeds_token_a: U256::from(10_300_000_u64),
            opportunity_threshold_met: true,
        }
    }

    #[test]
    fn admission_has_no_depth_recovery_or_gas_inputs() {
        let economics = evaluate_execution_admission(&top_of_book(), inputs()).unwrap();

        assert_eq!(economics.recovery_sell_quote_token_a, U256::ZERO);
        assert_eq!(economics.recovery_buy_quote_token_a, U256::ZERO);
        assert!(economics.opportunity_threshold_met);
    }
}

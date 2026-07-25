use std::str::FromStr;

use alloy_primitives::U256;
use anyhow::{Context, ensure};
use rust_decimal::Decimal;

use crate::{arbitrage::ArbitrageDirection, opportunity::format_base_units, state::TopOfBook};

/// The executor refuses any swap whose resolved gas limit exceeds this value.
/// Admission reserves the corresponding native amount but never converts it
/// into a sizing or profitability gate.
pub const MAX_SWAP_GAS_LIMIT: u64 = 5_000_000;
pub const RAILS_PRIORITY_FEE_WEI: u128 = 1_500_000;

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
    pub network_gas_price_wei: u128,
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
    pub maximum_gas_wei: U256,
    pub maximum_fee_per_gas_wei: u128,
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
    let maximum_fee_per_gas = inputs
        .network_gas_price_wei
        .checked_add(RAILS_PRIORITY_FEE_WEI)
        .context("admission max fee per gas overflow")?;
    let maximum_gas_wei = U256::from(MAX_SWAP_GAS_LIMIT)
        .checked_mul(U256::from(maximum_fee_per_gas))
        .context("admission maximum gas overflow")?;
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
        maximum_gas_wei,
        maximum_fee_per_gas_wei: maximum_fee_per_gas,
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
        inputs.network_gas_price_wei > 0,
        "network gas price is zero"
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
        admission::{AdmissionInputs, MAX_SWAP_GAS_LIMIT, evaluate_execution_admission},
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
            network_gas_price_wei: 1_000_000,
        }
    }

    #[test]
    fn admission_has_no_depth_recovery_or_gas_conversion_gate() {
        let economics = evaluate_execution_admission(&top_of_book(), inputs()).unwrap();

        assert_eq!(economics.recovery_sell_quote_token_a, U256::ZERO);
        assert_eq!(economics.recovery_buy_quote_token_a, U256::ZERO);
        assert!(economics.maximum_gas_wei > U256::ZERO);
    }

    #[test]
    fn gas_price_is_uncapped_and_only_sets_the_native_reservation() {
        let mut request = inputs();
        request.network_gas_price_wei = 6_000_000_000;

        let economics = evaluate_execution_admission(&top_of_book(), request).unwrap();

        assert_eq!(economics.maximum_fee_per_gas_wei, 6_001_500_000);
        assert_eq!(
            economics.maximum_gas_wei,
            U256::from(MAX_SWAP_GAS_LIMIT) * U256::from(6_001_500_000_u64)
        );
    }
}

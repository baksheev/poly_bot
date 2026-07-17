use alloy_primitives::U256;
use anyhow::{Context, ensure};
use rust_decimal::Decimal;

use crate::{
    arbitrage::{ArbitrageDirection, LegResult, LegStatus},
    binance::ws_api::OrderResult,
    dex::execution::SwapExecutionOutcome,
};

pub fn dex_leg_result(
    direction: ArbitrageDirection,
    outcome: SwapExecutionOutcome,
    gas_cost_token_a_base_units: u128,
) -> anyhow::Result<LegResult> {
    let token_in = u256_to_i128(outcome.token_in_spent, "DEX input delta")?;
    let token_out = u256_to_i128(outcome.token_out_received, "DEX output delta")?;
    let (token_a_delta_base_units, token_b_delta_base_units) = match direction {
        ArbitrageDirection::BuyTokenBOnDexSellOnCex => (-token_in, token_out),
        ArbitrageDirection::BuyTokenBOnCexSellOnDex => (token_out, -token_in),
    };
    Ok(LegResult {
        status: LegStatus::Filled,
        token_b_delta_base_units,
        token_a_delta_base_units,
        gas_cost_token_a_base_units,
        venue_reference: format!("dex:{:#x}", outcome.transaction_hash),
    })
}

pub fn binance_leg_result(
    order: &OrderResult,
    base_asset: &str,
    base_decimals: u8,
    quote_asset: &str,
    quote_decimals: u8,
) -> anyhow::Result<LegResult> {
    ensure!(
        matches!(order.status.as_str(), "FILLED" | "EXPIRED" | "CANCELED"),
        "Binance order is not terminal"
    );
    let changes = order
        .balance_changes(base_asset, quote_asset)
        .map_err(anyhow::Error::msg)?;
    for (asset, change) in &changes {
        ensure!(
            asset == base_asset || asset == quote_asset || change.is_zero(),
            "Binance commission in {asset} requires token-A conversion"
        );
    }
    let token_b_delta_base_units = decimal_to_signed_base_units(
        changes.get(base_asset).copied().unwrap_or(Decimal::ZERO),
        base_decimals,
    )?;
    let token_a_delta_base_units = decimal_to_signed_base_units(
        changes.get(quote_asset).copied().unwrap_or(Decimal::ZERO),
        quote_decimals,
    )?;
    let status = if token_b_delta_base_units == 0 && token_a_delta_base_units == 0 {
        LegStatus::Failed
    } else {
        LegStatus::Filled
    };
    Ok(LegResult {
        status,
        token_b_delta_base_units,
        token_a_delta_base_units,
        gas_cost_token_a_base_units: 0,
        venue_reference: format!("cex:{}", order.order_id),
    })
}

pub fn native_gas_to_token_a_base_units(
    gas_used: u64,
    effective_gas_price_wei: u128,
    native_price_token_a: Decimal,
    token_a_decimals: u8,
) -> anyhow::Result<u128> {
    ensure!(gas_used > 0, "gas used must be positive");
    ensure!(
        effective_gas_price_wei > 0,
        "effective gas price must be positive"
    );
    ensure!(
        native_price_token_a > Decimal::ZERO,
        "native-token price must be positive"
    );
    let price_mantissa = u128::try_from(native_price_token_a.mantissa())
        .context("native-token price mantissa is negative")?;
    let numerator = U256::from(gas_used)
        .checked_mul(U256::from(effective_gas_price_wei))
        .and_then(|value| value.checked_mul(U256::from(price_mantissa)))
        .and_then(|value| value.checked_mul(pow10_u256(u32::from(token_a_decimals)).ok()?))
        .context("gas token-A numerator overflow")?;
    let denominator = pow10_u256(18)?
        .checked_mul(pow10_u256(native_price_token_a.scale())?)
        .context("gas token-A denominator overflow")?;
    let quotient = numerator / denominator;
    let rounded = if numerator % denominator == U256::ZERO {
        quotient
    } else {
        quotient
            .checked_add(U256::ONE)
            .context("gas token-A rounding overflow")?
    };
    u128::try_from(rounded).context("gas token-A cost exceeds u128")
}

fn decimal_to_signed_base_units(value: Decimal, decimals: u8) -> anyhow::Result<i128> {
    let mantissa = value.mantissa();
    let scale = value.scale();
    if scale <= u32::from(decimals) {
        return mantissa
            .checked_mul(pow10_i128(u32::from(decimals) - scale)?)
            .context("signed base-unit conversion overflow");
    }
    let divisor = pow10_i128(scale - u32::from(decimals))?;
    ensure!(
        mantissa % divisor == 0,
        "decimal amount cannot be represented in configured base units"
    );
    Ok(mantissa / divisor)
}

fn pow10_i128(exponent: u32) -> anyhow::Result<i128> {
    10_i128
        .checked_pow(exponent)
        .context("signed base-unit decimal scale overflow")
}

fn pow10_u256(exponent: u32) -> anyhow::Result<U256> {
    let mut value = U256::ONE;
    for _ in 0..exponent {
        value = value
            .checked_mul(U256::from(10_u8))
            .context("U256 decimal scale overflow")?;
    }
    Ok(value)
}

fn u256_to_i128(value: U256, name: &str) -> anyhow::Result<i128> {
    let value = u128::try_from(value).with_context(|| format!("{name} exceeds u128"))?;
    i128::try_from(value).with_context(|| format!("{name} exceeds i128"))
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{B256, U256};
    use rust_decimal::Decimal;

    use crate::{
        arbitrage::{ArbitrageDirection, LegStatus},
        binance::ws_api::OrderResult,
        dex::execution::{SwapExecutionOutcome, UniswapProtocol},
    };

    use super::{binance_leg_result, dex_leg_result, native_gas_to_token_a_base_units};

    fn order(side: &str, status: &str, executed: &str, quote: &str) -> OrderResult {
        OrderResult {
            symbol: "WLDUSDC".to_owned(),
            order_id: 42,
            client_order_id: "rustarb42L".to_owned(),
            transact_time: Some(1),
            price: Decimal::ZERO,
            orig_qty: executed.parse().unwrap(),
            executed_qty: executed.parse().unwrap(),
            orig_quote_order_qty: Decimal::ZERO,
            cummulative_quote_qty: quote.parse().unwrap(),
            status: status.to_owned(),
            time_in_force: "IOC".to_owned(),
            order_type: "LIMIT".to_owned(),
            side: side.to_owned(),
            fills: vec![],
        }
    }

    #[test]
    fn dex_receipt_delta_maps_both_directions() {
        let outcome = SwapExecutionOutcome {
            protocol: UniswapProtocol::V3,
            transaction_hash: B256::repeat_byte(1),
            block_number: 10,
            gas_used: 100,
            effective_gas_price: 2,
            token_in_spent: U256::from(1_000_u16),
            token_out_received: U256::from(1_100_u16),
        };
        let buy = dex_leg_result(ArbitrageDirection::BuyTokenBOnDexSellOnCex, outcome, 7).unwrap();
        assert_eq!(buy.token_a_delta_base_units, -1_000);
        assert_eq!(buy.token_b_delta_base_units, 1_100);
        assert_eq!(buy.gas_cost_token_a_base_units, 7);

        let sell = dex_leg_result(ArbitrageDirection::BuyTokenBOnCexSellOnDex, outcome, 7).unwrap();
        assert_eq!(sell.token_a_delta_base_units, 1_100);
        assert_eq!(sell.token_b_delta_base_units, -1_000);
    }

    #[test]
    fn binance_partial_ioc_uses_actual_terminal_deltas() {
        let result = binance_leg_result(
            &order("SELL", "EXPIRED", "1.2", "0.975"),
            "WLD",
            18,
            "USDC",
            6,
        )
        .unwrap();
        assert_eq!(result.status, LegStatus::Filled);
        assert_eq!(result.token_b_delta_base_units, -1_200_000_000_000_000_000);
        assert_eq!(result.token_a_delta_base_units, 975_000);
    }

    #[test]
    fn zero_fill_expired_order_is_failed_without_fake_delta() {
        let result =
            binance_leg_result(&order("BUY", "EXPIRED", "0", "0"), "WLD", 18, "USDC", 6).unwrap();
        assert_eq!(result.status, LegStatus::Failed);
        assert_eq!(result.token_b_delta_base_units, 0);
        assert_eq!(result.token_a_delta_base_units, 0);
    }

    #[test]
    fn converts_native_gas_to_token_a_with_conservative_rounding() {
        assert_eq!(
            native_gas_to_token_a_base_units(21_000, 1_000_000_000, Decimal::new(3_000, 0), 6,)
                .unwrap(),
            63_000
        );
        assert_eq!(
            native_gas_to_token_a_base_units(1, 1, Decimal::new(1, 0), 6).unwrap(),
            1
        );
    }
}

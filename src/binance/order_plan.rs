use anyhow::{Context, ensure};
use rust_decimal::Decimal;

use crate::binance::{
    account::SymbolRules,
    execution::{BinanceOrderRequest, BinanceOrderRequestKind},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannedLimitIoc {
    pub request: BinanceOrderRequest,
    pub target_base_units: i128,
    pub submitted_base_units: i128,
}

/// Builds the same bounded LIMIT IOC shape for the primary hedge and every
/// recovery. A target below one exchange step returns `None` and is dust.
pub fn plan_limit_ioc(
    operation_id: String,
    client_order_id: String,
    target_base_units: i128,
    base_decimals: u8,
    limit_price: Decimal,
    rules: &SymbolRules,
) -> anyhow::Result<Option<PlannedLimitIoc>> {
    ensure!(target_base_units != 0, "Binance target delta is zero");
    ensure!(
        limit_price > Decimal::ZERO,
        "Binance limit price is non-positive"
    );
    ensure!(rules.status == "TRADING", "Binance symbol is not trading");
    let side = if target_base_units > 0 { "BUY" } else { "SELL" };
    let absolute = target_base_units.unsigned_abs();
    let quantity = decimal_from_base_units(absolute, base_decimals)?;
    let quantity = round_down(quantity, rules.lot_size.step)?;
    if quantity.is_zero() {
        return Ok(None);
    }
    ensure!(
        quantity >= rules.lot_size.min && quantity <= rules.lot_size.max,
        "Binance IOC quantity is outside LOT_SIZE"
    );
    let price = match side {
        "BUY" => round_up(limit_price, rules.price.step)?,
        "SELL" => round_down(limit_price, rules.price.step)?,
        _ => unreachable!(),
    };
    ensure!(
        price >= rules.price.min && price <= rules.price.max,
        "Binance IOC price is outside PRICE_FILTER"
    );
    let notional = quantity
        .checked_mul(price)
        .context("Binance IOC notional overflow")?;
    ensure!(
        notional >= rules.min_notional,
        "Binance IOC notional is below the exchange minimum"
    );
    let submitted_absolute = base_units_from_decimal(quantity, base_decimals)?;
    let submitted_absolute =
        i128::try_from(submitted_absolute).context("Binance submitted quantity exceeds i128")?;
    let submitted_base_units = if target_base_units > 0 {
        submitted_absolute
    } else {
        -submitted_absolute
    };
    let request = BinanceOrderRequest {
        operation_id,
        client_order_id,
        symbol: rules.symbol.clone(),
        kind: BinanceOrderRequestKind::LimitIoc {
            side: side.to_owned(),
            quantity,
            price,
        },
    };
    request.validate()?;
    Ok(Some(PlannedLimitIoc {
        request,
        target_base_units,
        submitted_base_units,
    }))
}

pub fn recovery_client_order_id(primary: &str, attempt: usize) -> anyhow::Result<String> {
    ensure!(
        attempt > 0 && attempt <= 9,
        "invalid Binance recovery attempt"
    );
    ensure!(
        primary.starts_with("rustarb") && primary.is_ascii(),
        "primary Binance client id is outside the Rust namespace"
    );
    let suffix = format!("r{attempt}");
    ensure!(
        primary.len() + suffix.len() <= 36,
        "primary Binance client id leaves no recovery suffix space"
    );
    Ok(format!("{primary}{suffix}"))
}

fn decimal_from_base_units(value: u128, decimals: u8) -> anyhow::Result<Decimal> {
    let scale = 10_u128
        .checked_pow(u32::from(decimals))
        .context("Binance base decimal scale overflow")?;
    let whole = value / scale;
    let fraction = value % scale;
    let text = if fraction == 0 {
        whole.to_string()
    } else {
        format!("{whole}.{fraction:0width$}", width = usize::from(decimals))
    };
    text.parse()
        .context("Binance base quantity exceeds Decimal range")
}

fn base_units_from_decimal(value: Decimal, decimals: u8) -> anyhow::Result<u128> {
    ensure!(value >= Decimal::ZERO, "Binance quantity is negative");
    let mantissa = u128::try_from(value.mantissa()).context("Binance quantity is negative")?;
    let target_scale = u32::from(decimals);
    if value.scale() <= target_scale {
        mantissa
            .checked_mul(
                10_u128
                    .checked_pow(target_scale - value.scale())
                    .context("Binance quantity scale overflow")?,
            )
            .context("Binance quantity base-unit overflow")
    } else {
        let divisor = 10_u128
            .checked_pow(value.scale() - target_scale)
            .context("Binance quantity divisor overflow")?;
        ensure!(
            mantissa % divisor == 0,
            "Binance quantity is not representable in base units"
        );
        Ok(mantissa / divisor)
    }
}

fn round_down(value: Decimal, increment: Decimal) -> anyhow::Result<Decimal> {
    ensure!(
        increment > Decimal::ZERO,
        "Binance increment is non-positive"
    );
    Ok((value / increment).floor() * increment)
}

fn round_up(value: Decimal, increment: Decimal) -> anyhow::Result<Decimal> {
    ensure!(
        increment > Decimal::ZERO,
        "Binance increment is non-positive"
    );
    Ok((value / increment).ceil() * increment)
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use crate::binance::{
        account::{DecimalFilter, SymbolRules},
        execution::BinanceOrderRequestKind,
        order_plan::{plan_limit_ioc, recovery_client_order_id},
    };

    fn rules() -> SymbolRules {
        SymbolRules {
            symbol: "WLDUSDC".to_owned(),
            status: "TRADING".to_owned(),
            base_asset: "WLD".to_owned(),
            quote_asset: "USDC".to_owned(),
            price: DecimalFilter {
                min: Decimal::new(1, 3),
                max: Decimal::from(1_000),
                step: Decimal::new(1, 3),
            },
            lot_size: DecimalFilter {
                min: Decimal::new(1, 1),
                max: Decimal::from(1_000_000),
                step: Decimal::new(1, 1),
            },
            market_lot_size: DecimalFilter {
                min: Decimal::ZERO,
                max: Decimal::from(1_000_000),
                step: Decimal::ZERO,
            },
            min_notional: Decimal::ONE,
            max_num_orders: 200,
            max_num_algo_orders: 5,
        }
    }

    #[test]
    fn rounds_quantity_down_and_prices_against_the_trader() {
        let buy = plan_limit_ioc(
            "rustarb-buy".to_owned(),
            "rustarb-buy".to_owned(),
            1_234_567_890_123_456_789,
            18,
            Decimal::new(10001, 4),
            &rules(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(buy.submitted_base_units, 1_200_000_000_000_000_000);
        assert!(matches!(
            buy.request.kind,
            BinanceOrderRequestKind::LimitIoc { side, quantity, price }
                if side == "BUY" && quantity == Decimal::new(12, 1) && price == Decimal::new(1001, 3)
        ));

        let sell = plan_limit_ioc(
            "rustarb-sell".to_owned(),
            "rustarb-sell".to_owned(),
            -1_234_567_890_123_456_789,
            18,
            Decimal::new(10009, 4),
            &rules(),
        )
        .unwrap()
        .unwrap();
        assert!(matches!(
            sell.request.kind,
            BinanceOrderRequestKind::LimitIoc { side, price, .. }
                if side == "SELL" && price == Decimal::ONE
        ));
    }

    #[test]
    fn sub_step_target_is_dust_and_recovery_ids_are_deterministic() {
        assert!(
            plan_limit_ioc(
                "rustarb-dust".to_owned(),
                "rustarb-dust".to_owned(),
                99_000_000_000_000_000,
                18,
                Decimal::ONE,
                &rules(),
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(
            recovery_client_order_id("rustarb123", 2).unwrap(),
            "rustarb123r2"
        );
    }
}

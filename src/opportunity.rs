use std::{collections::HashMap, str::FromStr};

use alloy_primitives::{Address, U256};
use anyhow::{Context, ensure};
use rust_decimal::Decimal;

use crate::{
    dex::{clmm::InsufficientLiquidity, mirror::DexMirror},
    domain::config::{DomainSnapshot, PairConfig},
    state::TopOfBook,
};

const BPS_DENOMINATOR: u64 = 10_000;
const PROFIT_BPS_SCALE: u64 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbitrageDirection {
    BuyTokenBOnDexSellOnCex,
    BuyTokenBOnCexSellOnDex,
}

impl ArbitrageDirection {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BuyTokenBOnDexSellOnCex => "buy_token_b_on_dex_sell_on_cex",
            Self::BuyTokenBOnCexSellOnDex => "buy_token_b_on_cex_sell_on_dex",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapacityLimiter {
    BinanceTopOfBook,
    ProfitThreshold,
    DexLiquidity,
}

impl CapacityLimiter {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BinanceTopOfBook => "binance_top_of_book",
            Self::ProfitThreshold => "profit_threshold",
            Self::DexLiquidity => "dex_liquidity",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeEvaluation {
    pub pool_index: usize,
    pub token_b_amount: U256,
    /// Raw CLMM amount before the configured conservative reserve.
    pub dex_token_a_amount: U256,
    pub cex_token_a_amount: U256,
    /// Cost and proceeds after applying the configured reserve.
    pub cost_token_a: U256,
    pub proceeds_token_a: U256,
    /// Signed hundredths of one basis point.
    pub profit_bps_x100: i64,
    pub meets_threshold: bool,
}

impl TradeEvaluation {
    pub fn absolute_profit_token_a(self) -> U256 {
        self.proceeds_token_a.saturating_sub(self.cost_token_a)
    }

    pub fn signed_profit_token_a(self) -> String {
        if self.proceeds_token_a >= self.cost_token_a {
            (self.proceeds_token_a - self.cost_token_a).to_string()
        } else {
            format!("-{}", self.cost_token_a - self.proceeds_token_a)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapacityEvaluation {
    pub trade: TradeEvaluation,
    pub limiter: CapacityLimiter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectionEvaluation {
    pub direction: ArbitrageDirection,
    pub cex_top_token_b_amount: U256,
    pub baseline: Option<TradeEvaluation>,
    /// Present only when the baseline clears the configured opportunity threshold.
    pub market_liquidity_capacity: Option<CapacityEvaluation>,
}

#[derive(Debug)]
pub struct PairEvaluation<'pair> {
    pub pair: &'pair PairRuntime,
    pub baseline_token_b_amount: U256,
    pub dex_buy_cex_sell: DirectionEvaluation,
    pub cex_buy_dex_sell: DirectionEvaluation,
}

#[derive(Debug)]
pub struct PairRuntime {
    pub pair_id: String,
    pub symbol: String,
    pub token_a_symbol: String,
    pub token_b_symbol: String,
    pub token_a_decimals: u8,
    pub token_b_decimals: u8,
    pub opportunity_threshold_bps: u16,
    pub dex_fee_reserve_bps: u16,
    baseline_token_a: U256,
    token_b_step: U256,
    token_a: Address,
    token_b: Address,
    pool_indices: Vec<usize>,
}

pub struct OpportunityEngine {
    pairs_by_symbol: HashMap<String, PairRuntime>,
}

impl OpportunityEngine {
    pub fn new(snapshot: &DomainSnapshot, dex: &DexMirror) -> anyhow::Result<Self> {
        let mut pairs_by_symbol = HashMap::new();
        for pair in snapshot
            .pairs
            .iter()
            .filter(|pair| pair.market_data_enabled)
        {
            let runtime = PairRuntime::new(pair, dex)?;
            ensure!(
                pairs_by_symbol
                    .insert(runtime.symbol.clone(), runtime)
                    .is_none(),
                "duplicate opportunity symbol {}",
                pair.binance.symbol
            );
        }
        Ok(Self { pairs_by_symbol })
    }

    pub fn evaluate<'pair>(
        &'pair self,
        quote: &TopOfBook,
        dex: &DexMirror,
    ) -> anyhow::Result<Option<PairEvaluation<'pair>>> {
        let Some(pair) = self.pairs_by_symbol.get(quote.symbol.as_ref()) else {
            return Ok(None);
        };

        let raw_baseline_token_b = token_a_to_token_b_floor(
            pair.baseline_token_a,
            quote.ask_price,
            pair.token_a_decimals,
            pair.token_b_decimals,
        )?;
        let baseline_token_b_amount = round_down_to_step(raw_baseline_token_b, pair.token_b_step);
        ensure!(
            !baseline_token_b_amount.is_zero(),
            "baseline token-B amount rounds to zero"
        );

        Ok(Some(PairEvaluation {
            pair,
            baseline_token_b_amount,
            dex_buy_cex_sell: evaluate_direction(
                pair,
                dex,
                quote,
                baseline_token_b_amount,
                ArbitrageDirection::BuyTokenBOnDexSellOnCex,
            )?,
            cex_buy_dex_sell: evaluate_direction(
                pair,
                dex,
                quote,
                baseline_token_b_amount,
                ArbitrageDirection::BuyTokenBOnCexSellOnDex,
            )?,
        }))
    }
}

impl PairRuntime {
    fn new(config: &PairConfig, dex: &DexMirror) -> anyhow::Result<Self> {
        let token_a = Address::from_str(&config.token_a.contract)
            .context("validated token_a address is invalid")?;
        let token_b = Address::from_str(&config.token_b.contract)
            .context("validated token_b address is invalid")?;
        let baseline_token_a = U256::from_str_radix(&config.quote_sizing.token_a_base_units, 10)
            .context("validated token-A baseline is invalid")?;
        let token_b_step = decimal_to_base_units(
            Decimal::from_str(&config.binance.step_size)
                .context("validated Binance step_size is invalid")?,
            config.token_b.decimals,
        )?;
        ensure!(!token_b_step.is_zero(), "Binance step_size rounds to zero");

        let mut pool_indices = Vec::new();
        for index in 0..dex.pool_count() {
            let pool = dex.pool(index)?;
            if pool.pair_id != config.id {
                continue;
            }
            ensure!(
                (pool.token0 == token_a && pool.token1 == token_b)
                    || (pool.token0 == token_b && pool.token1 == token_a),
                "DEX pool tokens differ from pair {}",
                config.id
            );
            pool_indices.push(index);
        }
        ensure!(
            !pool_indices.is_empty(),
            "pair {} has no hydrated DEX pools",
            config.id
        );

        Ok(Self {
            pair_id: config.id.clone(),
            symbol: config.binance.symbol.clone(),
            token_a_symbol: config.token_a.symbol.clone(),
            token_b_symbol: config.token_b.symbol.clone(),
            token_a_decimals: config.token_a.decimals,
            token_b_decimals: config.token_b.decimals,
            opportunity_threshold_bps: config.strategy.opportunity_threshold_bps,
            dex_fee_reserve_bps: config.strategy.dex_fee_reserve_bps,
            baseline_token_a,
            token_b_step,
            token_a,
            token_b,
            pool_indices,
        })
    }
}

fn evaluate_direction(
    pair: &PairRuntime,
    dex: &DexMirror,
    quote: &TopOfBook,
    baseline_token_b: U256,
    direction: ArbitrageDirection,
) -> anyhow::Result<DirectionEvaluation> {
    let cex_top = match direction {
        ArbitrageDirection::BuyTokenBOnDexSellOnCex => quote.bid_quantity,
        ArbitrageDirection::BuyTokenBOnCexSellOnDex => quote.ask_quantity,
    };
    let cex_top_token_b_amount = round_down_to_step(
        decimal_to_base_units(cex_top, pair.token_b_decimals)?,
        pair.token_b_step,
    );

    let mut best_baseline: Option<TradeEvaluation> = None;
    let mut best_capacity: Option<CapacityEvaluation> = None;
    if cex_top_token_b_amount >= baseline_token_b {
        for &pool_index in &pair.pool_indices {
            let Some(baseline) =
                evaluate_trade(pair, dex, quote, direction, pool_index, baseline_token_b)?
            else {
                continue;
            };
            if best_baseline
                .as_ref()
                .is_none_or(|best| baseline.profit_bps_x100 > best.profit_bps_x100)
            {
                best_baseline = Some(baseline);
            }
            if !baseline.meets_threshold {
                continue;
            }

            let capacity = size_pool(
                pair,
                dex,
                quote,
                direction,
                pool_index,
                baseline,
                cex_top_token_b_amount,
            )?;
            if best_capacity.as_ref().is_none_or(|best| {
                capacity.trade.absolute_profit_token_a() > best.trade.absolute_profit_token_a()
            }) {
                best_capacity = Some(capacity);
            }
        }
    }

    Ok(DirectionEvaluation {
        direction,
        cex_top_token_b_amount,
        baseline: best_baseline,
        market_liquidity_capacity: best_capacity,
    })
}

#[allow(clippy::too_many_arguments)]
fn size_pool(
    pair: &PairRuntime,
    dex: &DexMirror,
    quote: &TopOfBook,
    direction: ArbitrageDirection,
    pool_index: usize,
    baseline: TradeEvaluation,
    cex_top_token_b: U256,
) -> anyhow::Result<CapacityEvaluation> {
    let baseline_steps = baseline.token_b_amount / pair.token_b_step;
    let max_steps = cex_top_token_b / pair.token_b_step;
    ensure!(
        baseline_steps > U256::ZERO,
        "baseline has zero Binance steps"
    );
    ensure!(max_steps >= baseline_steps, "capacity is below baseline");

    if max_steps == baseline_steps {
        return Ok(CapacityEvaluation {
            trade: baseline,
            limiter: CapacityLimiter::BinanceTopOfBook,
        });
    }

    let max_amount = max_steps
        .checked_mul(pair.token_b_step)
        .context("maximum token-B amount overflow")?;
    if let Some(at_max) = evaluate_trade(pair, dex, quote, direction, pool_index, max_amount)?
        && at_max.meets_threshold
    {
        return Ok(CapacityEvaluation {
            trade: at_max,
            limiter: CapacityLimiter::BinanceTopOfBook,
        });
    }

    let mut low = baseline_steps;
    let mut high = max_steps;
    let mut best = baseline;
    while high - low > U256::ONE {
        let mid = low + ((high - low) / U256::from(2_u8));
        let amount = mid
            .checked_mul(pair.token_b_step)
            .context("sized token-B amount overflow")?;
        match evaluate_trade(pair, dex, quote, direction, pool_index, amount)? {
            Some(candidate) if candidate.meets_threshold => {
                low = mid;
                best = candidate;
            }
            _ => high = mid,
        }
    }

    let next_amount = high
        .checked_mul(pair.token_b_step)
        .context("next token-B amount overflow")?;
    let limiter = match evaluate_trade(pair, dex, quote, direction, pool_index, next_amount)? {
        None => CapacityLimiter::DexLiquidity,
        Some(_) => CapacityLimiter::ProfitThreshold,
    };
    Ok(CapacityEvaluation {
        trade: best,
        limiter,
    })
}

fn evaluate_trade(
    pair: &PairRuntime,
    dex: &DexMirror,
    quote: &TopOfBook,
    direction: ArbitrageDirection,
    pool_index: usize,
    token_b_amount: U256,
) -> anyhow::Result<Option<TradeEvaluation>> {
    let hydrated = dex.pool(pool_index)?;
    let pool = &hydrated.pool;

    let (dex_token_a_amount, cex_token_a_amount, cost_token_a, proceeds_token_a) = match direction {
        ArbitrageDirection::BuyTokenBOnDexSellOnCex => {
            let zero_for_one = hydrated.token0 == pair.token_a;
            let dex_cost = match pool.quote_exact_out_amount_in(zero_for_one, token_b_amount) {
                Ok(value) => value,
                Err(error) if error.downcast_ref::<InsufficientLiquidity>().is_some() => {
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
            let cex_proceeds = token_b_to_token_a(
                token_b_amount,
                quote.bid_price,
                pair.token_a_decimals,
                pair.token_b_decimals,
                false,
            )?;
            let reserved_cost = add_bps_ceil(dex_cost, pair.dex_fee_reserve_bps)?;
            (dex_cost, cex_proceeds, reserved_cost, cex_proceeds)
        }
        ArbitrageDirection::BuyTokenBOnCexSellOnDex => {
            let zero_for_one = hydrated.token0 == pair.token_b;
            let dex_proceeds = match pool.quote_exact_in_amount_out(zero_for_one, token_b_amount) {
                Ok(value) => value,
                Err(error) if error.downcast_ref::<InsufficientLiquidity>().is_some() => {
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
            let cex_cost = token_b_to_token_a(
                token_b_amount,
                quote.ask_price,
                pair.token_a_decimals,
                pair.token_b_decimals,
                true,
            )?;
            let reserved_proceeds = subtract_bps_floor(dex_proceeds, pair.dex_fee_reserve_bps)?;
            (dex_proceeds, cex_cost, cex_cost, reserved_proceeds)
        }
    };

    Ok(Some(TradeEvaluation {
        pool_index,
        token_b_amount,
        dex_token_a_amount,
        cex_token_a_amount,
        cost_token_a,
        proceeds_token_a,
        profit_bps_x100: signed_profit_bps_x100(proceeds_token_a, cost_token_a)?,
        meets_threshold: meets_threshold(
            proceeds_token_a,
            cost_token_a,
            pair.opportunity_threshold_bps,
        )?,
    }))
}

fn decimal_to_base_units(value: Decimal, decimals: u8) -> anyhow::Result<U256> {
    ensure!(
        value >= Decimal::ZERO,
        "decimal amount must not be negative"
    );
    let mantissa = value.mantissa();
    ensure!(mantissa >= 0, "decimal mantissa must not be negative");
    let numerator = U256::from(mantissa as u128)
        .checked_mul(pow10(decimals.into())?)
        .context("decimal base-unit numerator overflow")?;
    Ok(numerator / pow10(value.scale())?)
}

fn token_a_to_token_b_floor(
    token_a_amount: U256,
    price: Decimal,
    token_a_decimals: u8,
    token_b_decimals: u8,
) -> anyhow::Result<U256> {
    let mantissa = positive_decimal_mantissa(price)?;
    let exponent = i32::from(token_b_decimals) + price.scale() as i32 - i32::from(token_a_decimals);
    if exponent >= 0 {
        let numerator = scale_up(token_a_amount, exponent)?;
        Ok(numerator / U256::from(mantissa))
    } else {
        let denominator = U256::from(mantissa)
            .checked_mul(pow10((-exponent) as u32)?)
            .context("inverse price denominator overflow")?;
        Ok(token_a_amount / denominator)
    }
}

fn token_b_to_token_a(
    token_b_amount: U256,
    price: Decimal,
    token_a_decimals: u8,
    token_b_decimals: u8,
    round_up: bool,
) -> anyhow::Result<U256> {
    let mantissa = positive_decimal_mantissa(price)?;
    let product = token_b_amount
        .checked_mul(U256::from(mantissa))
        .context("token-B price product overflow")?;
    let exponent = i32::from(token_b_decimals) + price.scale() as i32 - i32::from(token_a_decimals);
    if exponent <= 0 {
        return scale_up(product, -exponent).context("token-A price scaling overflow");
    }

    let denominator = pow10(exponent as u32)?;
    let quotient = product / denominator;
    let remainder = product % denominator;
    if round_up && !remainder.is_zero() {
        quotient
            .checked_add(U256::ONE)
            .context("rounded token-A amount overflow")
    } else {
        Ok(quotient)
    }
}

fn positive_decimal_mantissa(value: Decimal) -> anyhow::Result<u128> {
    ensure!(value > Decimal::ZERO, "price must be positive");
    let mantissa = value.mantissa();
    ensure!(mantissa > 0, "price mantissa must be positive");
    Ok(mantissa as u128)
}

fn scale_up(value: U256, exponent: i32) -> anyhow::Result<U256> {
    ensure!(exponent >= 0, "scale exponent must not be negative");
    value
        .checked_mul(pow10(exponent as u32)?)
        .context("power-of-ten scaling overflow")
}

fn pow10(exponent: u32) -> anyhow::Result<U256> {
    let mut value = U256::ONE;
    for _ in 0..exponent {
        value = value
            .checked_mul(U256::from(10_u8))
            .context("power of ten exceeds U256")?;
    }
    Ok(value)
}

fn round_down_to_step(amount: U256, step: U256) -> U256 {
    (amount / step) * step
}

fn add_bps_ceil(amount: U256, bps: u16) -> anyhow::Result<U256> {
    let numerator = amount
        .checked_mul(U256::from(BPS_DENOMINATOR + u64::from(bps)))
        .context("cost reserve overflow")?;
    let denominator = U256::from(BPS_DENOMINATOR);
    let quotient = numerator / denominator;
    if (numerator % denominator).is_zero() {
        Ok(quotient)
    } else {
        quotient
            .checked_add(U256::ONE)
            .context("rounded cost reserve overflow")
    }
}

fn subtract_bps_floor(amount: U256, bps: u16) -> anyhow::Result<U256> {
    amount
        .checked_mul(U256::from(BPS_DENOMINATOR - u64::from(bps)))
        .context("proceeds reserve overflow")
        .map(|value| value / U256::from(BPS_DENOMINATOR))
}

fn meets_threshold(proceeds: U256, cost: U256, threshold_bps: u16) -> anyhow::Result<bool> {
    let left = proceeds
        .checked_mul(U256::from(BPS_DENOMINATOR))
        .context("threshold proceeds overflow")?;
    let right = cost
        .checked_mul(U256::from(BPS_DENOMINATOR + u64::from(threshold_bps)))
        .context("threshold cost overflow")?;
    Ok(left >= right)
}

fn signed_profit_bps_x100(proceeds: U256, cost: U256) -> anyhow::Result<i64> {
    ensure!(!cost.is_zero(), "profit cost must be positive");
    let (positive, delta) = if proceeds >= cost {
        (true, proceeds - cost)
    } else {
        (false, cost - proceeds)
    };
    let scaled = delta
        .checked_mul(U256::from(PROFIT_BPS_SCALE))
        .context("profit ratio overflow")?
        / cost;
    let magnitude: u64 = scaled.try_into().unwrap_or(u64::MAX);
    let magnitude = i64::try_from(magnitude).unwrap_or(i64::MAX);
    Ok(if positive { magnitude } else { -magnitude })
}

pub fn format_base_units(amount: U256, decimals: u8) -> String {
    if decimals == 0 {
        return amount.to_string();
    }
    let mut digits = amount.to_string();
    let decimals = usize::from(decimals);
    if digits.len() <= decimals {
        let mut prefixed = String::with_capacity(decimals + 2);
        prefixed.push_str("0.");
        prefixed.extend(std::iter::repeat_n('0', decimals - digits.len()));
        prefixed.push_str(&digits);
        digits = prefixed;
    } else {
        digits.insert(digits.len() - decimals, '.');
    }
    while digits.ends_with('0') {
        digits.pop();
    }
    if digits.ends_with('.') {
        digits.pop();
    }
    digits
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc, time::Instant};

    use alloy_primitives::{Address, B256, U256, address};
    use rust_decimal::Decimal;
    use uniswap_v3_math::tick_math::get_sqrt_ratio_at_tick;

    use crate::{
        chain::rpc::CanonicalBlock,
        dex::{
            clmm::ClmmPool,
            hydration::{HydratedDexState, HydratedPool, PoolIdentity},
            mirror::DexMirror,
        },
        state::TopOfBook,
    };

    use super::{
        ArbitrageDirection, CapacityLimiter, PairRuntime, decimal_to_base_units,
        evaluate_direction, format_base_units, token_a_to_token_b_floor, token_b_to_token_a,
    };

    fn hash(number: u64) -> B256 {
        B256::from(U256::from(number).to_be_bytes::<32>())
    }

    fn fixture() -> (PairRuntime, DexMirror) {
        fixture_with_liquidity(1_000_000_000_000_000_000_000_000)
    }

    fn fixture_with_liquidity(liquidity: u128) -> (PairRuntime, DexMirror) {
        let token_a = address!("0000000000000000000000000000000000000001");
        let token_b = address!("0000000000000000000000000000000000000002");
        let mut pool =
            ClmmPool::new(3_000, 60, get_sqrt_ratio_at_tick(0).unwrap(), 0, liquidity).unwrap();
        let liquidity_net = i128::try_from(liquidity).unwrap();
        pool.set_tick(-887_220, liquidity, liquidity_net).unwrap();
        pool.set_tick(887_220, liquidity, -liquidity_net).unwrap();
        let mirror = DexMirror::new(HydratedDexState {
            block: CanonicalBlock {
                number: 1,
                hash: hash(1),
                parent_hash: hash(0),
            },
            pools: vec![HydratedPool {
                pair_id: "test-pair".into(),
                identity: PoolIdentity::V3 {
                    address: Address::from([3_u8; 20]),
                    fee_pips: 3_000,
                },
                token0: token_a,
                token1: token_b,
                pool,
            }],
            unavailable: vec![],
        })
        .unwrap();
        let runtime = PairRuntime {
            pair_id: "test-pair".into(),
            symbol: "BA".into(),
            token_a_symbol: "A".into(),
            token_b_symbol: "B".into(),
            token_a_decimals: 18,
            token_b_decimals: 18,
            opportunity_threshold_bps: 20,
            dex_fee_reserve_bps: 4,
            baseline_token_a: U256::from(20_u8) * U256::from(10_u64).pow(U256::from(18_u8)),
            token_b_step: U256::from(10_u64).pow(U256::from(18_u8)),
            token_a,
            token_b,
            pool_indices: vec![0],
        };
        (runtime, mirror)
    }

    fn quote(bid: &str, bid_quantity: &str, ask: &str, ask_quantity: &str) -> TopOfBook {
        TopOfBook::new(
            Arc::from("BA"),
            1,
            Decimal::from_str(bid).unwrap(),
            Decimal::from_str(bid_quantity).unwrap(),
            Decimal::from_str(ask).unwrap(),
            Decimal::from_str(ask_quantity).unwrap(),
            None,
            None,
            Instant::now(),
            1,
            1,
        )
        .unwrap()
    }

    #[test]
    fn decimal_conversion_and_price_rounding_are_explicit() {
        assert_eq!(
            decimal_to_base_units(Decimal::from_str("12.3456789").unwrap(), 6).unwrap(),
            U256::from(12_345_678_u64)
        );
        assert_eq!(
            token_a_to_token_b_floor(
                U256::from(20_000_000_u64),
                Decimal::from_str("0.5").unwrap(),
                6,
                18,
            )
            .unwrap(),
            U256::from(40_u8) * U256::from(10_u64).pow(U256::from(18_u8))
        );
        let amount = U256::from(1_u8) * U256::from(10_u64).pow(U256::from(18_u8));
        assert_eq!(
            token_b_to_token_a(
                amount,
                Decimal::from_str("0.81234567").unwrap(),
                6,
                18,
                false,
            )
            .unwrap(),
            U256::from(812_345_u64)
        );
        assert_eq!(format_base_units(U256::from(12_340_000_u64), 6), "12.34");
    }

    #[test]
    fn sizes_dex_buy_to_the_full_profitable_cex_top() {
        let (pair, dex) = fixture();
        let quote = quote("1.02", "100.9", "1.03", "100");
        let baseline = U256::from(19_u8) * pair.token_b_step;
        let result = evaluate_direction(
            &pair,
            &dex,
            &quote,
            baseline,
            ArbitrageDirection::BuyTokenBOnDexSellOnCex,
        )
        .unwrap();

        let capacity = result.market_liquidity_capacity.unwrap();
        assert_eq!(capacity.limiter, CapacityLimiter::BinanceTopOfBook);
        assert_eq!(
            capacity.trade.token_b_amount,
            U256::from(100_u8) * pair.token_b_step
        );
        assert!(capacity.trade.meets_threshold);
    }

    #[test]
    fn sizes_cex_buy_and_rejects_the_other_direction() {
        let (pair, dex) = fixture();
        let quote = quote("0.98", "100", "0.99", "42.7");
        let baseline = U256::from(20_u8) * pair.token_b_step;
        let cex_buy = evaluate_direction(
            &pair,
            &dex,
            &quote,
            baseline,
            ArbitrageDirection::BuyTokenBOnCexSellOnDex,
        )
        .unwrap();
        let dex_buy = evaluate_direction(
            &pair,
            &dex,
            &quote,
            baseline,
            ArbitrageDirection::BuyTokenBOnDexSellOnCex,
        )
        .unwrap();

        let capacity = cex_buy.market_liquidity_capacity.unwrap();
        assert_eq!(
            capacity.trade.token_b_amount,
            U256::from(42_u8) * pair.token_b_step
        );
        assert_eq!(capacity.limiter, CapacityLimiter::BinanceTopOfBook);
        assert!(dex_buy.market_liquidity_capacity.is_none());
    }

    #[test]
    fn sizing_stops_at_the_profit_threshold_before_top_of_book() {
        let (pair, dex) = fixture_with_liquidity(10_000_000_000_000_000_000_000);
        let quote = quote("1.02", "1000", "1.03", "1000");
        let baseline = U256::from(19_u8) * pair.token_b_step;
        let result = evaluate_direction(
            &pair,
            &dex,
            &quote,
            baseline,
            ArbitrageDirection::BuyTokenBOnDexSellOnCex,
        )
        .unwrap();

        let capacity = result.market_liquidity_capacity.unwrap();
        assert_eq!(capacity.limiter, CapacityLimiter::ProfitThreshold);
        assert!(capacity.trade.token_b_amount >= baseline);
        assert!(capacity.trade.token_b_amount < result.cex_top_token_b_amount);
        assert!(capacity.trade.meets_threshold);
    }

    #[test]
    fn top_of_book_below_baseline_has_no_executable_evaluation() {
        let (pair, dex) = fixture();
        let quote = quote("1.02", "5", "1.03", "5");
        let baseline = U256::from(20_u8) * pair.token_b_step;
        let result = evaluate_direction(
            &pair,
            &dex,
            &quote,
            baseline,
            ArbitrageDirection::BuyTokenBOnDexSellOnCex,
        )
        .unwrap();

        assert!(result.baseline.is_none());
        assert!(result.market_liquidity_capacity.is_none());
    }
}

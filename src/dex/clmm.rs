use std::{collections::HashMap, error::Error, fmt};

use alloy_primitives::{I256, U256};
use anyhow::{Context, ensure};
use uniswap_v3_math::{
    liquidity_math::add_delta,
    swap_math::compute_swap_step,
    tick_bitmap::{flip_tick, next_initialized_tick_within_one_word},
    tick_math::{
        MAX_SQRT_RATIO, MAX_TICK, MIN_SQRT_RATIO, MIN_TICK, get_sqrt_ratio_at_tick,
        get_tick_at_sqrt_ratio,
    },
};

/// The complete state needed to quote a hookless Uniswap V3/V4 pool locally.
///
/// The maps are mutated only by the engine's single state owner. `quote_exact_in`
/// is read-only and performs no network I/O. V4 pools with swap-impacting hooks
/// must never be represented by this type.
#[derive(Debug, Clone)]
pub struct ClmmPool {
    pub fee_pips: u32,
    pub tick_spacing: i32,
    pub sqrt_price_x96: U256,
    pub tick: i32,
    pub liquidity: u128,
    tick_bitmap: HashMap<i16, U256>,
    ticks: HashMap<i32, TickLiquidity>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TickLiquidity {
    pub gross: u128,
    pub net: i128,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalQuote {
    pub amount_out: U256,
    pub sqrt_price_after_x96: U256,
    pub tick_after: i32,
    pub liquidity_after: u128,
    pub initialized_ticks_crossed: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InsufficientLiquidity;

impl fmt::Display for InsufficientLiquidity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("pool has insufficient hydrated liquidity")
    }
}

impl Error for InsufficientLiquidity {}

impl ClmmPool {
    pub fn new(
        fee_pips: u32,
        tick_spacing: i32,
        sqrt_price_x96: U256,
        tick: i32,
        liquidity: u128,
    ) -> anyhow::Result<Self> {
        ensure!(fee_pips < 1_000_000, "fee must be below 1_000_000 pips");
        ensure!(tick_spacing > 0, "tick spacing must be positive");
        ensure!(
            (MIN_TICK..=MAX_TICK).contains(&tick),
            "tick is out of range"
        );
        ensure!(
            sqrt_price_x96 >= MIN_SQRT_RATIO && sqrt_price_x96 < MAX_SQRT_RATIO,
            "sqrt price is out of range"
        );
        ensure!(liquidity > 0, "active liquidity must be positive");

        Ok(Self {
            fee_pips,
            tick_spacing,
            sqrt_price_x96,
            tick,
            liquidity,
            tick_bitmap: HashMap::new(),
            ticks: HashMap::new(),
        })
    }

    pub fn initialized_tick_count(&self) -> usize {
        self.ticks.len()
    }

    pub fn tick_liquidity(&self, index: i32) -> Option<TickLiquidity> {
        self.ticks.get(&index).copied()
    }

    /// Installs an absolute initialized-tick snapshot during hydration.
    pub fn set_tick(&mut self, index: i32, gross: u128, net: i128) -> anyhow::Result<()> {
        ensure!(
            index % self.tick_spacing == 0,
            "tick does not align to spacing"
        );
        ensure!(
            (MIN_TICK..=MAX_TICK).contains(&index),
            "tick is out of range"
        );

        let previous = self.ticks.get(&index).copied();
        match (previous, gross) {
            (None, 0) => {}
            (None, _) => {
                flip_tick(&mut self.tick_bitmap, index, self.tick_spacing)
                    .context("failed to initialize tick bitmap bit")?;
                self.ticks.insert(index, TickLiquidity { gross, net });
            }
            (Some(_), 0) => {
                flip_tick(&mut self.tick_bitmap, index, self.tick_spacing)
                    .context("failed to clear tick bitmap bit")?;
                self.ticks.remove(&index);
            }
            (Some(_), _) => {
                self.ticks.insert(index, TickLiquidity { gross, net });
            }
        }
        Ok(())
    }

    /// Applies the post-Swap head state emitted by either a V3 pool or V4 PoolManager.
    pub fn apply_swap_head(
        &mut self,
        sqrt_price_x96: U256,
        tick: i32,
        liquidity: u128,
    ) -> anyhow::Result<()> {
        ensure!(
            sqrt_price_x96 >= MIN_SQRT_RATIO && sqrt_price_x96 < MAX_SQRT_RATIO,
            "sqrt price is out of range"
        );
        ensure!(
            (MIN_TICK..=MAX_TICK).contains(&tick),
            "tick is out of range"
        );
        ensure!(liquidity > 0, "active liquidity must be positive");
        self.sqrt_price_x96 = sqrt_price_x96;
        self.tick = tick;
        self.liquidity = liquidity;
        Ok(())
    }

    /// Applies a Mint/Burn/ModifyLiquidity delta to the two range boundaries
    /// and to active liquidity when the current tick is inside the range.
    pub fn apply_liquidity_delta(
        &mut self,
        tick_lower: i32,
        tick_upper: i32,
        delta: i128,
    ) -> anyhow::Result<()> {
        ensure!(tick_lower < tick_upper, "liquidity range is empty");
        ensure!(
            tick_lower % self.tick_spacing == 0 && tick_upper % self.tick_spacing == 0,
            "liquidity range does not align to tick spacing"
        );
        ensure!(
            (MIN_TICK..=MAX_TICK).contains(&tick_lower)
                && (MIN_TICK..=MAX_TICK).contains(&tick_upper),
            "liquidity range is out of bounds"
        );
        if delta == 0 {
            return Ok(());
        }

        let amount = delta.unsigned_abs();
        let lower = updated_boundary(
            self.ticks.get(&tick_lower).copied(),
            amount,
            delta,
            delta > 0,
        )?;
        let upper_net_delta = delta.checked_neg().context("liquidity delta overflow")?;
        let upper = updated_boundary(
            self.ticks.get(&tick_upper).copied(),
            amount,
            upper_net_delta,
            delta > 0,
        )?;
        let active_liquidity = if tick_lower <= self.tick && self.tick < tick_upper {
            Some(add_delta(self.liquidity, delta).context("active liquidity update failed")?)
        } else {
            None
        };

        self.set_tick(tick_lower, lower.gross, lower.net)?;
        self.set_tick(tick_upper, upper.gross, upper.net)?;
        if let Some(active_liquidity) = active_liquidity {
            self.liquidity = active_liquidity;
        }
        Ok(())
    }

    /// Computes an exact-input quote entirely from the local pool mirror.
    ///
    /// `zero_for_one=true` sells token0 for token1. The result matches the core
    /// swap loop for vanilla V3 and hookless/static-fee V4 pools.
    pub fn quote_exact_in(
        &self,
        zero_for_one: bool,
        amount_in: U256,
    ) -> anyhow::Result<LocalQuote> {
        self.quote_exact_in_impl::<true>(zero_for_one, amount_in)
    }

    /// Hot-path variant that omits post-swap diagnostics not needed by a decision.
    #[inline]
    pub fn quote_exact_in_amount_out(
        &self,
        zero_for_one: bool,
        amount_in: U256,
    ) -> anyhow::Result<U256> {
        Ok(self
            .quote_exact_in_impl::<false>(zero_for_one, amount_in)?
            .amount_out)
    }

    /// Computes the input required for an exact output without mutating the pool.
    ///
    /// This is used to size the DEX-buy/CEX-sell leg to the exact Binance step,
    /// avoiding an unhedged token-B remainder caused by rounding an exact-input
    /// quote down after the fact.
    #[inline]
    pub fn quote_exact_out_amount_in(
        &self,
        zero_for_one: bool,
        amount_out: U256,
    ) -> anyhow::Result<U256> {
        ensure!(!amount_out.is_zero(), "amount out must be positive");
        ensure!(amount_out < (U256::ONE << 255), "amount out exceeds int256");

        let sqrt_price_limit_x96 = if zero_for_one {
            MIN_SQRT_RATIO + U256::ONE
        } else {
            MAX_SQRT_RATIO - U256::ONE
        };
        let mut amount_remaining = amount_out;
        let mut amount_in = U256::ZERO;
        let mut sqrt_price_x96 = self.sqrt_price_x96;
        let mut tick = self.tick;
        let mut liquidity = self.liquidity;

        while !amount_remaining.is_zero() && sqrt_price_x96 != sqrt_price_limit_x96 {
            let (mut tick_next, initialized) = next_initialized_tick_within_one_word(
                &self.tick_bitmap,
                tick,
                self.tick_spacing,
                zero_for_one,
            )
            .context("failed to find next initialized tick")?;
            tick_next = tick_next.clamp(MIN_TICK, MAX_TICK);

            let sqrt_price_next_x96 =
                get_sqrt_ratio_at_tick(tick_next).context("failed to price next tick")?;
            let target = if zero_for_one {
                sqrt_price_next_x96.max(sqrt_price_limit_x96)
            } else {
                sqrt_price_next_x96.min(sqrt_price_limit_x96)
            };
            let (sqrt_after, step_in, step_out, fee_amount) = compute_swap_step(
                sqrt_price_x96,
                target,
                liquidity,
                -I256::from_raw(amount_remaining),
                self.fee_pips,
            )
            .context("failed to compute exact-output swap step")?;

            amount_remaining = amount_remaining
                .checked_sub(step_out)
                .context("swap produced more than remaining output")?;
            amount_in = amount_in
                .checked_add(step_in)
                .and_then(|value| value.checked_add(fee_amount))
                .context("swap input overflow")?;
            sqrt_price_x96 = sqrt_after;

            if sqrt_after == sqrt_price_next_x96 {
                if initialized {
                    let tick_state = self
                        .ticks
                        .get(&tick_next)
                        .with_context(|| format!("bitmap references missing tick {tick_next}"))?;
                    let liquidity_net = if zero_for_one {
                        tick_state
                            .net
                            .checked_neg()
                            .context("liquidity net overflow")?
                    } else {
                        tick_state.net
                    };
                    liquidity = add_delta(liquidity, liquidity_net)
                        .context("failed to cross initialized tick")?;
                }
                tick = if zero_for_one {
                    tick_next.saturating_sub(1)
                } else {
                    tick_next
                };
            } else {
                break;
            }
        }

        if !amount_remaining.is_zero() {
            return Err(InsufficientLiquidity.into());
        }
        Ok(amount_in)
    }

    #[inline]
    fn quote_exact_in_impl<const INCLUDE_AFTER_STATE: bool>(
        &self,
        zero_for_one: bool,
        amount_in: U256,
    ) -> anyhow::Result<LocalQuote> {
        ensure!(!amount_in.is_zero(), "amount in must be positive");
        ensure!(amount_in < (U256::ONE << 255), "amount in exceeds int256");

        let sqrt_price_limit_x96 = if zero_for_one {
            MIN_SQRT_RATIO + U256::ONE
        } else {
            MAX_SQRT_RATIO - U256::ONE
        };
        let mut amount_remaining = amount_in;
        let mut amount_out = U256::ZERO;
        let mut sqrt_price_x96 = self.sqrt_price_x96;
        let mut tick = self.tick;
        let mut liquidity = self.liquidity;
        let mut initialized_ticks_crossed = 0_u32;

        while !amount_remaining.is_zero() && sqrt_price_x96 != sqrt_price_limit_x96 {
            let (mut tick_next, initialized) = next_initialized_tick_within_one_word(
                &self.tick_bitmap,
                tick,
                self.tick_spacing,
                zero_for_one,
            )
            .context("failed to find next initialized tick")?;
            tick_next = tick_next.clamp(MIN_TICK, MAX_TICK);

            let sqrt_price_next_x96 =
                get_sqrt_ratio_at_tick(tick_next).context("failed to price next tick")?;
            let target = if zero_for_one {
                sqrt_price_next_x96.max(sqrt_price_limit_x96)
            } else {
                sqrt_price_next_x96.min(sqrt_price_limit_x96)
            };
            let (sqrt_after, step_in, step_out, fee_amount) = compute_swap_step(
                sqrt_price_x96,
                target,
                liquidity,
                I256::from_raw(amount_remaining),
                self.fee_pips,
            )
            .context("failed to compute swap step")?;

            let consumed = step_in
                .checked_add(fee_amount)
                .context("swap input overflow")?;
            amount_remaining = amount_remaining
                .checked_sub(consumed)
                .context("swap consumed more than remaining input")?;
            amount_out = amount_out
                .checked_add(step_out)
                .context("swap output overflow")?;
            sqrt_price_x96 = sqrt_after;

            if sqrt_after == sqrt_price_next_x96 {
                if initialized {
                    let tick_state = self
                        .ticks
                        .get(&tick_next)
                        .with_context(|| format!("bitmap references missing tick {tick_next}"))?;
                    let liquidity_net = if zero_for_one {
                        tick_state
                            .net
                            .checked_neg()
                            .context("liquidity net overflow")?
                    } else {
                        tick_state.net
                    };
                    liquidity = add_delta(liquidity, liquidity_net)
                        .context("failed to cross initialized tick")?;
                    initialized_ticks_crossed += 1;
                }
                tick = if zero_for_one {
                    tick_next.saturating_sub(1)
                } else {
                    tick_next
                };
            } else {
                if INCLUDE_AFTER_STATE {
                    tick = get_tick_at_sqrt_ratio(sqrt_price_x96)
                        .context("failed to derive tick after partial swap step")?;
                }
                break;
            }
        }

        if !amount_remaining.is_zero() {
            return Err(InsufficientLiquidity.into());
        }
        Ok(LocalQuote {
            amount_out,
            sqrt_price_after_x96: sqrt_price_x96,
            tick_after: tick,
            liquidity_after: liquidity,
            initialized_ticks_crossed,
        })
    }
}

fn updated_boundary(
    current: Option<TickLiquidity>,
    amount: u128,
    net_delta: i128,
    adding: bool,
) -> anyhow::Result<TickLiquidity> {
    let current = current.unwrap_or(TickLiquidity { gross: 0, net: 0 });
    let gross = if adding {
        current
            .gross
            .checked_add(amount)
            .context("gross tick liquidity overflow")?
    } else {
        current
            .gross
            .checked_sub(amount)
            .context("removed more gross tick liquidity than hydrated")?
    };
    let net = current
        .net
        .checked_add(net_delta)
        .context("net tick liquidity overflow")?;
    ensure!(
        gross != 0 || net == 0,
        "zero gross tick liquidity has non-zero net liquidity"
    );
    Ok(TickLiquidity { gross, net })
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{U256, uint};
    use uniswap_v3_math::tick_math::get_sqrt_ratio_at_tick;

    use super::ClmmPool;

    fn pool() -> ClmmPool {
        let mut pool = ClmmPool::new(
            3_000,
            60,
            get_sqrt_ratio_at_tick(0).unwrap(),
            0,
            1_000_000_000_000_000_000,
        )
        .unwrap();
        pool.set_tick(
            -887_220,
            1_000_000_000_000_000_000,
            1_000_000_000_000_000_000,
        )
        .unwrap();
        pool.set_tick(
            887_220,
            1_000_000_000_000_000_000,
            -1_000_000_000_000_000_000,
        )
        .unwrap();
        pool
    }

    #[test]
    fn exact_input_quotes_both_directions_without_mutating_pool() {
        let pool = pool();
        let before = pool.sqrt_price_x96;

        let zero_for_one = pool
            .quote_exact_in(true, U256::from(1_000_000_u64))
            .unwrap();
        let one_for_zero = pool
            .quote_exact_in(false, U256::from(1_000_000_u64))
            .unwrap();

        assert_eq!(zero_for_one.amount_out, U256::from(996_999_u64));
        assert_eq!(one_for_zero.amount_out, U256::from(996_999_u64));
        assert_eq!(
            pool.quote_exact_in_amount_out(true, U256::from(1_000_000_u64))
                .unwrap(),
            zero_for_one.amount_out
        );
        assert_eq!(
            pool.quote_exact_in_amount_out(false, U256::from(1_000_000_u64))
                .unwrap(),
            one_for_zero.amount_out
        );
        assert!(zero_for_one.sqrt_price_after_x96 < before);
        assert!(one_for_zero.sqrt_price_after_x96 > before);
        assert!(zero_for_one.tick_after < 0);
        assert!(one_for_zero.tick_after >= 0);
        assert_eq!(pool.sqrt_price_x96, before);
    }

    #[test]
    fn exact_output_returns_the_step_aligned_input_requirement() {
        let pool = pool();
        let desired = U256::from(996_999_u64);

        for zero_for_one in [true, false] {
            let required = pool
                .quote_exact_out_amount_in(zero_for_one, desired)
                .unwrap();
            let delivered = pool
                .quote_exact_in_amount_out(zero_for_one, required)
                .unwrap();

            assert!(delivered >= desired);
            if required > U256::ONE {
                let previous = pool
                    .quote_exact_in_amount_out(zero_for_one, required - U256::ONE)
                    .unwrap();
                assert!(previous < desired);
            }
        }
    }

    #[test]
    fn quote_crosses_initialized_tick_and_changes_active_liquidity() {
        let mut pool = ClmmPool::new(
            3_000,
            60,
            get_sqrt_ratio_at_tick(0).unwrap(),
            0,
            1_000_000_000,
        )
        .unwrap();
        pool.set_tick(-60, 500_000_000, 500_000_000).unwrap();
        pool.set_tick(-120, 500_000_000, 500_000_000).unwrap();
        pool.set_tick(120, 1_000_000_000, -1_000_000_000).unwrap();

        let quote = pool.quote_exact_in(true, uint!(4_000_000_U256)).unwrap();

        assert!(quote.initialized_ticks_crossed >= 1);
        assert!(quote.tick_after < -60);
        assert_eq!(quote.liquidity_after, 500_000_000);
    }

    #[test]
    fn tick_bitmap_stays_consistent_when_tick_is_removed() {
        let mut pool = pool();
        let count = pool.initialized_tick_count();
        pool.set_tick(120, 10, -10).unwrap();
        assert_eq!(pool.initialized_tick_count(), count + 1);
        pool.set_tick(120, 0, 0).unwrap();
        assert_eq!(pool.initialized_tick_count(), count);
    }

    #[test]
    fn liquidity_events_update_both_boundaries_and_remove_them_atomically() {
        let mut pool = pool();
        let initial_count = pool.initialized_tick_count();
        let initial_liquidity = pool.liquidity;

        pool.apply_liquidity_delta(-120, 120, 500).unwrap();
        assert_eq!(pool.liquidity, initial_liquidity + 500);
        assert_eq!(
            pool.tick_liquidity(-120).unwrap(),
            super::TickLiquidity {
                gross: 500,
                net: 500
            }
        );
        assert_eq!(
            pool.tick_liquidity(120).unwrap(),
            super::TickLiquidity {
                gross: 500,
                net: -500
            }
        );
        assert_eq!(pool.initialized_tick_count(), initial_count + 2);

        pool.apply_liquidity_delta(-120, 120, -500).unwrap();
        assert_eq!(pool.liquidity, initial_liquidity);
        assert_eq!(pool.tick_liquidity(-120), None);
        assert_eq!(pool.tick_liquidity(120), None);
        assert_eq!(pool.initialized_tick_count(), initial_count);
    }

    #[test]
    fn matches_world_chain_v3_quoter_at_captured_block_across_tick() {
        // Pool 0xc19b...0684, QuoterV2, World Chain block 0x1ee7069.
        // Input is 20_000_000 USDC base units, USDC (token1) -> WLD (token0).
        let mut pool = ClmmPool::new(
            3_000,
            60,
            U256::from_str_radix("ab5d2274c6aa0f31de4", 16).unwrap(),
            -285_301,
            294_726_389_706_506_412,
        )
        .unwrap();
        let boundary_liquidity = u128::from_str_radix("2f70997e216661", 16).unwrap();
        pool.set_tick(-285_300, boundary_liquidity, boundary_liquidity as i128)
            .unwrap();

        let quote = pool
            .quote_exact_in(false, U256::from(20_000_000_u64))
            .unwrap();

        assert_eq!(
            quote.amount_out,
            U256::from_str_radix("2a6f4b44053c572fd", 16).unwrap()
        );
        assert_eq!(quote.initialized_ticks_crossed, 1);
    }
}

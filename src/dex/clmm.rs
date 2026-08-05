use std::{
    collections::HashMap,
    error::Error,
    fmt,
    sync::{Arc, Mutex, OnceLock},
};

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

const PREPARED_CURVE_INITIAL_SEGMENT_CAPACITY: usize = 128;

/// The complete state needed to quote a hookless Uniswap V3/V4 pool locally.
///
/// The maps are mutated only by the engine's single state owner. `quote_exact_in`
/// is read-only and performs no network I/O. V4 pools with swap-impacting hooks
/// must never be represented by this type.
#[derive(Debug, Clone)]
pub struct ClmmPool {
    /// Static fee for Uniswap-style pools and the zero-to-one directional fee
    /// for Algebra V1.9 pools. Use `fee_pips_for_direction` while quoting.
    pub fee_pips: u32,
    one_for_zero_fee_pips: u32,
    pub tick_spacing: i32,
    pub sqrt_price_x96: U256,
    pub tick: i32,
    pub liquidity: u128,
    tick_bitmap: HashMap<i16, U256>,
    ticks: HashMap<i32, TickLiquidity>,
    word_boundary_sqrt_ratios: Arc<HashMap<i32, U256>>,
    tick_traversal: TickTraversal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TickTraversal {
    SpacingCompressed,
    AlgebraRaw,
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

/// A version-local, immutable representation of one CLMM quote direction.
///
/// Building the curve performs the same word-boundary traversal as the core
/// swap loop. Quoting then needs only a binary search and at most one
/// `compute_swap_step`, while preserving the boundary-by-boundary rounding of
/// the on-chain algorithm.
#[derive(Debug, Clone)]
pub struct PreparedQuoteCurve {
    kind: PreparedQuoteKind,
    fee_pips: u32,
    segments: Vec<PreparedQuoteSegment>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PreparedQuoteKind {
    ExactInput,
    ExactOutput,
}

#[derive(Debug, Clone, Copy)]
struct PreparedQuoteSegment {
    specified_end: U256,
    result_end: U256,
    sqrt_price_start_x96: U256,
    sqrt_price_target_x96: U256,
    liquidity: u128,
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
        Self::new_with_profile(
            fee_pips,
            fee_pips,
            tick_spacing,
            sqrt_price_x96,
            tick,
            liquidity,
            TickTraversal::SpacingCompressed,
        )
    }

    /// Constructs a Camelot/Algebra V1.9 pool. Algebra stores actual ticks in
    /// each 256-bit row and selects a different fee for each swap direction.
    pub fn new_algebra_v1_9(
        fee_zero_for_one_pips: u32,
        fee_one_for_zero_pips: u32,
        tick_spacing: i32,
        sqrt_price_x96: U256,
        tick: i32,
        liquidity: u128,
    ) -> anyhow::Result<Self> {
        Self::new_with_profile(
            fee_zero_for_one_pips,
            fee_one_for_zero_pips,
            tick_spacing,
            sqrt_price_x96,
            tick,
            liquidity,
            TickTraversal::AlgebraRaw,
        )
    }

    fn new_with_profile(
        fee_zero_for_one_pips: u32,
        fee_one_for_zero_pips: u32,
        tick_spacing: i32,
        sqrt_price_x96: U256,
        tick: i32,
        liquidity: u128,
        tick_traversal: TickTraversal,
    ) -> anyhow::Result<Self> {
        ensure!(
            fee_zero_for_one_pips < 1_000_000 && fee_one_for_zero_pips < 1_000_000,
            "fee must be below 1_000_000 pips"
        );
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
            fee_pips: fee_zero_for_one_pips,
            one_for_zero_fee_pips: fee_one_for_zero_pips,
            tick_spacing,
            sqrt_price_x96,
            tick,
            liquidity,
            tick_bitmap: HashMap::new(),
            ticks: HashMap::new(),
            word_boundary_sqrt_ratios: word_boundary_sqrt_ratios(tick_spacing, tick_traversal)?,
            tick_traversal,
        })
    }

    #[inline]
    pub const fn fee_pips_for_direction(&self, zero_for_one: bool) -> u32 {
        if zero_for_one {
            self.fee_pips
        } else {
            self.one_for_zero_fee_pips
        }
    }

    pub const fn directional_fee_pips(&self) -> (u32, u32) {
        (self.fee_pips, self.one_for_zero_fee_pips)
    }

    pub fn set_algebra_directional_fees(
        &mut self,
        fee_zero_for_one_pips: u32,
        fee_one_for_zero_pips: u32,
    ) -> anyhow::Result<()> {
        ensure!(
            self.tick_traversal == TickTraversal::AlgebraRaw,
            "directional fees require an Algebra V1.9 pool"
        );
        ensure!(
            fee_zero_for_one_pips < 1_000_000 && fee_one_for_zero_pips < 1_000_000,
            "fee must be below 1_000_000 pips"
        );
        self.fee_pips = fee_zero_for_one_pips;
        self.one_for_zero_fee_pips = fee_one_for_zero_pips;
        Ok(())
    }

    pub fn initialized_tick_count(&self) -> usize {
        self.ticks.len()
    }

    pub fn tick_liquidity(&self, index: i32) -> Option<TickLiquidity> {
        self.ticks.get(&index).copied()
    }

    pub fn initialized_ticks(&self) -> impl Iterator<Item = (i32, TickLiquidity)> + '_ {
        self.ticks.iter().map(|(index, state)| (*index, *state))
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
                self.flip_tick(index)
                    .context("failed to initialize tick bitmap bit")?;
                self.ticks.insert(index, TickLiquidity { gross, net });
            }
            (Some(_), 0) => {
                self.flip_tick(index)
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
        self.quote_exact_out_amount_in_impl(zero_for_one, amount_out, true)
    }

    /// Computes the input capacity reached while walking toward a bounded
    /// exact-output amount. Unlike a quote, exhaustion returns the reachable
    /// input capacity, matching a bounded prepared curve's result capacity.
    #[cfg(test)]
    pub(crate) fn exact_output_result_capacity_bounded(
        &self,
        zero_for_one: bool,
        maximum_amount_out: U256,
    ) -> anyhow::Result<U256> {
        self.quote_exact_out_amount_in_impl(zero_for_one, maximum_amount_out, false)
    }

    fn quote_exact_out_amount_in_impl(
        &self,
        zero_for_one: bool,
        amount_out: U256,
        require_full_output: bool,
    ) -> anyhow::Result<U256> {
        ensure!(!amount_out.is_zero(), "amount out must be positive");
        ensure!(amount_out < (U256::ONE << 255), "amount out exceeds int256");
        let fee_pips = self.fee_pips_for_direction(zero_for_one);

        let sqrt_price_limit_x96 = if zero_for_one {
            MIN_SQRT_RATIO + U256::ONE
        } else {
            MAX_SQRT_RATIO - U256::ONE
        };
        let mut amount_remaining = amount_out;
        let mut amount_in = U256::ZERO;
        let mut prepared_result_capacity = U256::ZERO;
        let mut sqrt_price_x96 = self.sqrt_price_x96;
        let mut tick = self.tick;
        let mut liquidity = self.liquidity;

        while !amount_remaining.is_zero()
            && sqrt_price_x96 != sqrt_price_limit_x96
            && liquidity != 0
        {
            let (mut tick_next, initialized) = self.next_initialized_tick(tick, zero_for_one)?;
            tick_next = tick_next.clamp(MIN_TICK, MAX_TICK);

            let sqrt_price_next_x96 = self.sqrt_ratio_at_traversal_tick(tick_next, initialized)?;
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
                fee_pips,
            )
            .context("failed to compute exact-output swap step")?;

            amount_remaining = amount_remaining
                .checked_sub(step_out)
                .context("swap produced more than remaining output")?;
            amount_in = amount_in
                .checked_add(step_in)
                .and_then(|value| value.checked_add(fee_amount))
                .context("swap input overflow")?;
            if !step_out.is_zero() {
                prepared_result_capacity = amount_in;
            }
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

        if require_full_output && !amount_remaining.is_zero() {
            return Err(InsufficientLiquidity.into());
        }
        Ok(if require_full_output {
            amount_in
        } else {
            prepared_result_capacity
        })
    }

    /// Precomputes the exact-input path for one swap direction.
    pub fn prepare_exact_input_curve(
        &self,
        zero_for_one: bool,
    ) -> anyhow::Result<PreparedQuoteCurve> {
        self.prepare_exact_input_curve_bounded(zero_for_one, (U256::ONE << 255) - U256::ONE)
    }

    /// Precomputes the exact-input path only through `maximum_amount_in`.
    pub fn prepare_exact_input_curve_bounded(
        &self,
        zero_for_one: bool,
        maximum_amount_in: U256,
    ) -> anyhow::Result<PreparedQuoteCurve> {
        self.prepare_quote_curve(
            zero_for_one,
            PreparedQuoteKind::ExactInput,
            maximum_amount_in,
        )
    }

    /// Rebuilds an exact-input curve while retaining the previous curve's
    /// segment allocation. The previous contents are never observed.
    pub(crate) fn prepare_exact_input_curve_bounded_reusing(
        &self,
        zero_for_one: bool,
        maximum_amount_in: U256,
        previous: PreparedQuoteCurve,
    ) -> anyhow::Result<PreparedQuoteCurve> {
        self.prepare_quote_curve_reusing(
            zero_for_one,
            PreparedQuoteKind::ExactInput,
            maximum_amount_in,
            Some(previous),
        )
    }

    /// Builds the exact-input curve whose maximum input is the minimum input
    /// required to reach `maximum_amount_out`.
    ///
    /// The old hot rebuild first walked the pool as exact-output to discover
    /// that input limit and then walked the same sparse bitmap words again to
    /// build the exact-input curve. Full word-boundary steps have identical
    /// cumulative input/output under both views, so retain them directly and
    /// recompute only the final partial exact-input step where integer rounding
    /// can differ. This preserves exact quote semantics while removing one
    /// complete sparse traversal from every prepared-pool refresh.
    pub(crate) fn prepare_exact_input_curve_bounded_by_exact_output_reusing(
        &self,
        zero_for_one: bool,
        maximum_amount_out: U256,
        previous: Option<PreparedQuoteCurve>,
    ) -> anyhow::Result<PreparedQuoteCurve> {
        ensure!(
            !maximum_amount_out.is_zero(),
            "prepared curve output maximum must be positive"
        );
        ensure!(
            maximum_amount_out < (U256::ONE << 255),
            "prepared curve output maximum exceeds int256"
        );
        let fee_pips = self.fee_pips_for_direction(zero_for_one);
        let sqrt_price_limit_x96 = if zero_for_one {
            MIN_SQRT_RATIO + U256::ONE
        } else {
            MAX_SQRT_RATIO - U256::ONE
        };
        let mut segments = if let Some(previous) = previous {
            let mut segments = previous.segments;
            segments.clear();
            segments
        } else {
            Vec::with_capacity(PREPARED_CURVE_INITIAL_SEGMENT_CAPACITY)
        };
        let mut specified_total = U256::ZERO;
        let mut result_total = U256::ZERO;
        let mut result_remaining = maximum_amount_out;
        let mut sqrt_price_x96 = self.sqrt_price_x96;
        let mut tick = self.tick;
        let mut liquidity = self.liquidity;
        let mut last_productive_segment_count = 0;

        while !result_remaining.is_zero()
            && sqrt_price_x96 != sqrt_price_limit_x96
            && liquidity != 0
        {
            let (mut tick_next, initialized) = self.next_initialized_tick(tick, zero_for_one)?;
            tick_next = tick_next.clamp(MIN_TICK, MAX_TICK);
            let sqrt_price_next_x96 = self.sqrt_ratio_at_traversal_tick(tick_next, initialized)?;
            let target = if zero_for_one {
                sqrt_price_next_x96.max(sqrt_price_limit_x96)
            } else {
                sqrt_price_next_x96.min(sqrt_price_limit_x96)
            };
            let (sqrt_after, step_in, step_out, fee_amount) = compute_swap_step(
                sqrt_price_x96,
                target,
                liquidity,
                -I256::from_raw(result_remaining),
                fee_pips,
            )
            .context("failed to build fused exact-output boundary")?;
            let input_with_fee = step_in
                .checked_add(fee_amount)
                .context("fused prepared swap input overflow")?;

            // A partial exact-output step may round to an input whose
            // exact-input result differs by one base unit. Recompute that one
            // final step in the curve's actual quote mode; full boundary steps
            // retain the already exact cumulative values.
            let partial = sqrt_after != sqrt_price_next_x96;
            let (specified_step, result_step) = if partial {
                let (_, exact_input_step_in, exact_input_step_out, exact_input_fee) =
                    compute_swap_step(
                        sqrt_price_x96,
                        target,
                        liquidity,
                        I256::from_raw(input_with_fee),
                        fee_pips,
                    )
                    .context("failed to finalize fused exact-input segment")?;
                (
                    exact_input_step_in
                        .checked_add(exact_input_fee)
                        .context("fused exact-input specified amount overflow")?,
                    exact_input_step_out,
                )
            } else {
                (input_with_fee, step_out)
            };
            let specified_end = specified_total
                .checked_add(specified_step)
                .context("fused prepared specified amount overflow")?;
            let result_end = result_total
                .checked_add(result_step)
                .context("fused prepared result amount overflow")?;
            if !specified_step.is_zero() {
                segments.push(PreparedQuoteSegment {
                    specified_end,
                    result_end,
                    sqrt_price_start_x96: sqrt_price_x96,
                    sqrt_price_target_x96: target,
                    liquidity,
                });
            }
            specified_total = specified_end;
            result_total = result_end;
            if !step_out.is_zero() {
                last_productive_segment_count = segments.len();
            }
            result_remaining = result_remaining
                .checked_sub(step_out)
                .context("fused swap produced more than remaining output")?;
            sqrt_price_x96 = sqrt_after;

            if partial {
                break;
            }
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
                    .context("failed to cross fused prepared initialized tick")?;
            }
            tick = if zero_for_one {
                tick_next.saturating_sub(1)
            } else {
                tick_next
            };
        }
        // Match the old capacity pass: input spent after the last non-zero
        // output is outside the executable envelope and must not extend the
        // exact-input curve at extreme price boundaries.
        segments.truncate(last_productive_segment_count);

        Ok(PreparedQuoteCurve {
            kind: PreparedQuoteKind::ExactInput,
            fee_pips,
            segments,
        })
    }

    /// Precomputes the exact-output path for one swap direction.
    pub fn prepare_exact_output_curve(
        &self,
        zero_for_one: bool,
    ) -> anyhow::Result<PreparedQuoteCurve> {
        self.prepare_exact_output_curve_bounded(zero_for_one, (U256::ONE << 255) - U256::ONE)
    }

    /// Precomputes the exact-output path only through `maximum_amount_out`.
    pub fn prepare_exact_output_curve_bounded(
        &self,
        zero_for_one: bool,
        maximum_amount_out: U256,
    ) -> anyhow::Result<PreparedQuoteCurve> {
        self.prepare_quote_curve(
            zero_for_one,
            PreparedQuoteKind::ExactOutput,
            maximum_amount_out,
        )
    }

    /// Rebuilds an exact-output curve while retaining the previous curve's
    /// segment allocation. The previous contents are never observed.
    pub(crate) fn prepare_exact_output_curve_bounded_reusing(
        &self,
        zero_for_one: bool,
        maximum_amount_out: U256,
        previous: PreparedQuoteCurve,
    ) -> anyhow::Result<PreparedQuoteCurve> {
        self.prepare_quote_curve_reusing(
            zero_for_one,
            PreparedQuoteKind::ExactOutput,
            maximum_amount_out,
            Some(previous),
        )
    }

    fn prepare_quote_curve(
        &self,
        zero_for_one: bool,
        kind: PreparedQuoteKind,
        maximum_specified: U256,
    ) -> anyhow::Result<PreparedQuoteCurve> {
        self.prepare_quote_curve_reusing(zero_for_one, kind, maximum_specified, None)
    }

    fn prepare_quote_curve_reusing(
        &self,
        zero_for_one: bool,
        kind: PreparedQuoteKind,
        maximum_specified: U256,
        previous: Option<PreparedQuoteCurve>,
    ) -> anyhow::Result<PreparedQuoteCurve> {
        ensure!(
            !maximum_specified.is_zero(),
            "prepared curve maximum must be positive"
        );
        ensure!(
            maximum_specified < (U256::ONE << 255),
            "prepared curve maximum exceeds int256"
        );
        let fee_pips = self.fee_pips_for_direction(zero_for_one);
        let sqrt_price_limit_x96 = if zero_for_one {
            MIN_SQRT_RATIO + U256::ONE
        } else {
            MAX_SQRT_RATIO - U256::ONE
        };
        // Sparse V3 tails can cross more than one hundred empty bitmap words
        // inside the reviewed execution envelope. A bounded initial reserve
        // avoids allocator/copy tails during initial hydration. Refreshes
        // retain that allocation across pool generations.
        let mut segments = if let Some(previous) = previous {
            let mut segments = previous.segments;
            segments.clear();
            segments
        } else {
            Vec::with_capacity(PREPARED_CURVE_INITIAL_SEGMENT_CAPACITY)
        };
        let mut specified_total = U256::ZERO;
        let mut result_total = U256::ZERO;
        let mut sqrt_price_x96 = self.sqrt_price_x96;
        let mut tick = self.tick;
        let mut liquidity = self.liquidity;

        while specified_total < maximum_specified
            && sqrt_price_x96 != sqrt_price_limit_x96
            && liquidity != 0
        {
            let specified_remaining = maximum_specified
                .checked_sub(specified_total)
                .context("prepared specified amount exceeded its maximum")?;
            let specified_delta = match kind {
                PreparedQuoteKind::ExactInput => I256::from_raw(specified_remaining),
                PreparedQuoteKind::ExactOutput => -I256::from_raw(specified_remaining),
            };
            let (mut tick_next, initialized) = self.next_initialized_tick(tick, zero_for_one)?;
            tick_next = tick_next.clamp(MIN_TICK, MAX_TICK);
            let sqrt_price_next_x96 = self.sqrt_ratio_at_traversal_tick(tick_next, initialized)?;
            let target = if zero_for_one {
                sqrt_price_next_x96.max(sqrt_price_limit_x96)
            } else {
                sqrt_price_next_x96.min(sqrt_price_limit_x96)
            };
            let (sqrt_after, step_in, step_out, fee_amount) =
                compute_swap_step(sqrt_price_x96, target, liquidity, specified_delta, fee_pips)
                    .context("failed to build prepared swap segment")?;
            let input_with_fee = step_in
                .checked_add(fee_amount)
                .context("prepared swap input overflow")?;
            let (specified_step, result_step) = match kind {
                PreparedQuoteKind::ExactInput => (input_with_fee, step_out),
                PreparedQuoteKind::ExactOutput => (step_out, input_with_fee),
            };
            let specified_end = specified_total
                .checked_add(specified_step)
                .context("prepared specified amount overflow")?;
            let result_end = result_total
                .checked_add(result_step)
                .context("prepared result amount overflow")?;
            if !specified_step.is_zero() {
                segments.push(PreparedQuoteSegment {
                    specified_end,
                    result_end,
                    sqrt_price_start_x96: sqrt_price_x96,
                    sqrt_price_target_x96: target,
                    liquidity,
                });
            }
            specified_total = specified_end;
            result_total = result_end;
            sqrt_price_x96 = sqrt_after;

            if sqrt_after != sqrt_price_next_x96 {
                break;
            }
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
                    .context("failed to cross prepared initialized tick")?;
            }
            tick = if zero_for_one {
                tick_next.saturating_sub(1)
            } else {
                tick_next
            };
        }

        Ok(PreparedQuoteCurve {
            kind,
            fee_pips,
            segments,
        })
    }

    #[inline]
    fn quote_exact_in_impl<const INCLUDE_AFTER_STATE: bool>(
        &self,
        zero_for_one: bool,
        amount_in: U256,
    ) -> anyhow::Result<LocalQuote> {
        ensure!(!amount_in.is_zero(), "amount in must be positive");
        ensure!(amount_in < (U256::ONE << 255), "amount in exceeds int256");
        let fee_pips = self.fee_pips_for_direction(zero_for_one);

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
            let (mut tick_next, initialized) = self.next_initialized_tick(tick, zero_for_one)?;
            tick_next = tick_next.clamp(MIN_TICK, MAX_TICK);

            let sqrt_price_next_x96 = self.sqrt_ratio_at_traversal_tick(tick_next, initialized)?;
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
                fee_pips,
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

    #[inline]
    fn sqrt_ratio_at_traversal_tick(&self, tick: i32, initialized: bool) -> anyhow::Result<U256> {
        if !initialized {
            return self
                .word_boundary_sqrt_ratios
                .get(&tick)
                .copied()
                .with_context(|| format!("missing cached sqrt ratio for word boundary {tick}"));
        }
        get_sqrt_ratio_at_tick(tick).context("failed to price initialized tick")
    }

    fn flip_tick(&mut self, tick: i32) -> anyhow::Result<()> {
        match self.tick_traversal {
            TickTraversal::SpacingCompressed => {
                flip_tick(&mut self.tick_bitmap, tick, self.tick_spacing)
                    .context("failed to toggle spacing-compressed tick")
            }
            TickTraversal::AlgebraRaw => {
                let word = i16::try_from(tick >> 8).context("Algebra tick row exceeds int16")?;
                let bit =
                    u32::try_from(tick.rem_euclid(256)).context("Algebra tick bit is negative")?;
                let entry = self.tick_bitmap.entry(word).or_default();
                *entry ^= U256::ONE << bit;
                Ok(())
            }
        }
    }

    fn next_initialized_tick(&self, tick: i32, zero_for_one: bool) -> anyhow::Result<(i32, bool)> {
        match self.tick_traversal {
            TickTraversal::SpacingCompressed => next_initialized_tick_within_one_word(
                &self.tick_bitmap,
                tick,
                self.tick_spacing,
                zero_for_one,
            )
            .context("failed to find spacing-compressed tick"),
            TickTraversal::AlgebraRaw => Ok(next_algebra_tick_in_same_row(
                &self.tick_bitmap,
                tick,
                zero_for_one,
            )),
        }
    }
}

impl PreparedQuoteCurve {
    /// Returns the exact result for `specified_amount`, or an immediate
    /// insufficient-liquidity result when it exceeds the prepared curve.
    #[inline]
    pub fn quote(&self, specified_amount: U256) -> anyhow::Result<U256> {
        ensure!(
            !specified_amount.is_zero(),
            "specified amount must be positive"
        );
        ensure!(
            specified_amount < (U256::ONE << 255),
            "specified amount exceeds int256"
        );
        let segment_index = self
            .segments
            .partition_point(|segment| segment.specified_end < specified_amount);
        let Some(segment) = self.segments.get(segment_index) else {
            return Err(InsufficientLiquidity.into());
        };
        if specified_amount == segment.specified_end {
            return Ok(segment.result_end);
        }

        let (specified_start, result_start) = segment_index
            .checked_sub(1)
            .and_then(|previous| self.segments.get(previous))
            .map_or((U256::ZERO, U256::ZERO), |previous| {
                (previous.specified_end, previous.result_end)
            });
        let remaining = specified_amount
            .checked_sub(specified_start)
            .context("prepared quote amount precedes segment")?;
        let amount_remaining = match self.kind {
            PreparedQuoteKind::ExactInput => I256::from_raw(remaining),
            PreparedQuoteKind::ExactOutput => -I256::from_raw(remaining),
        };
        let (_, step_in, step_out, fee_amount) = compute_swap_step(
            segment.sqrt_price_start_x96,
            segment.sqrt_price_target_x96,
            segment.liquidity,
            amount_remaining,
            self.fee_pips,
        )
        .context("failed to quote prepared swap segment")?;
        let step_result = match self.kind {
            PreparedQuoteKind::ExactInput => step_out,
            PreparedQuoteKind::ExactOutput => step_in
                .checked_add(fee_amount)
                .context("prepared exact-output input overflow")?,
        };
        result_start
            .checked_add(step_result)
            .context("prepared quote result overflow")
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    pub fn specified_boundaries(&self) -> impl Iterator<Item = U256> + '_ {
        self.segments.iter().map(|segment| segment.specified_end)
    }

    pub fn specified_capacity(&self) -> U256 {
        self.segments
            .last()
            .map_or(U256::ZERO, |segment| segment.specified_end)
    }

    pub fn result_capacity(&self) -> U256 {
        self.segments
            .last()
            .map_or(U256::ZERO, |segment| segment.result_end)
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

fn next_algebra_tick_in_same_row(
    tick_table: &HashMap<i16, U256>,
    tick: i32,
    zero_for_one: bool,
) -> (i32, bool) {
    let search_tick = if zero_for_one {
        tick
    } else {
        tick.saturating_add(1)
    };
    let word = (search_tick >> 8) as i16;
    let bit = search_tick.rem_euclid(256) as u32;
    let row = tick_table.get(&word).copied().unwrap_or(U256::ZERO);
    let (next, initialized) = if zero_for_one {
        let mask = if bit == 255 {
            U256::MAX
        } else {
            (U256::ONE << (bit + 1)) - U256::ONE
        };
        let masked = row & mask;
        if masked.is_zero() {
            ((i32::from(word)) << 8, false)
        } else {
            (
                ((i32::from(word)) << 8) + (255 - masked.leading_zeros()) as i32,
                true,
            )
        }
    } else {
        let mask = U256::MAX << bit;
        let masked = row & mask;
        if masked.is_zero() {
            (((i32::from(word)) << 8) + 255, false)
        } else {
            (
                ((i32::from(word)) << 8) + masked.trailing_zeros() as i32,
                true,
            )
        }
    };
    (next.clamp(MIN_TICK, MAX_TICK), initialized)
}

fn word_boundary_sqrt_ratios(
    tick_spacing: i32,
    tick_traversal: TickTraversal,
) -> anyhow::Result<Arc<HashMap<i32, U256>>> {
    type Cache = HashMap<(i32, TickTraversal), Arc<HashMap<i32, U256>>>;
    static CACHE: OnceLock<Mutex<Cache>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(cached) = cache
        .lock()
        .map_err(|_| anyhow::anyhow!("word-boundary cache is poisoned"))?
        .get(&(tick_spacing, tick_traversal))
        .cloned()
    {
        return Ok(cached);
    }

    let (minimum_word, maximum_word, scale) = match tick_traversal {
        TickTraversal::SpacingCompressed => (
            MIN_TICK.div_euclid(tick_spacing) >> 8,
            MAX_TICK.div_euclid(tick_spacing) >> 8,
            i64::from(tick_spacing),
        ),
        TickTraversal::AlgebraRaw => (MIN_TICK >> 8, MAX_TICK >> 8, 1),
    };
    let mut ratios = HashMap::with_capacity(((maximum_word - minimum_word + 1) as usize) * 2 + 2);

    for word in minimum_word..=maximum_word {
        let word_start = i64::from(word) << 8;
        let lower = (word_start * scale).clamp(i64::from(MIN_TICK), i64::from(MAX_TICK)) as i32;
        let upper =
            ((word_start + 255) * scale).clamp(i64::from(MIN_TICK), i64::from(MAX_TICK)) as i32;
        for tick in [lower, upper] {
            ratios.entry(tick).or_insert(
                get_sqrt_ratio_at_tick(tick).context("failed to cache word-boundary price")?,
            );
        }
    }
    for tick in [MIN_TICK, MAX_TICK] {
        ratios
            .entry(tick)
            .or_insert(get_sqrt_ratio_at_tick(tick).context("failed to cache terminal price")?);
    }
    let ratios = Arc::new(ratios);
    let mut cache = cache
        .lock()
        .map_err(|_| anyhow::anyhow!("word-boundary cache is poisoned"))?;
    Ok(cache
        .entry((tick_spacing, tick_traversal))
        .or_insert_with(|| Arc::clone(&ratios))
        .clone())
}

#[cfg(test)]
mod tests {
    use std::hint::black_box;

    use alloy_primitives::{I256, U256, uint};
    use uniswap_v3_math::swap_math::compute_swap_step;
    use uniswap_v3_math::tick_math::get_sqrt_ratio_at_tick;

    use super::{ClmmPool, PreparedQuoteSegment};

    #[test]
    fn prepared_segment_keeps_only_non_derivable_state() {
        assert!(std::mem::size_of::<PreparedQuoteSegment>() <= 144);
    }

    #[test]
    fn algebra_price_movement_matches_official_v1_9_golden_vector() {
        // cryptoalgebra/AlgebraV1.9 PriceMovement.spec.ts: exact input capped
        // at sqrt(101/100), one-for-zero, 2e18 liquidity and 600 fee pips.
        let result = compute_swap_step(
            U256::from(79_228_162_514_264_337_593_543_950_336_u128),
            U256::from(79_623_317_895_830_914_510_639_640_423_u128),
            2_000_000_000_000_000_000,
            I256::from_raw(U256::from(1_000_000_000_000_000_000_u128)),
            600,
        )
        .unwrap();

        assert_eq!(
            result,
            (
                U256::from(79_623_317_895_830_914_510_639_640_423_u128),
                U256::from(9_975_124_224_178_055_u64),
                U256::from(9_925_619_580_021_728_u64),
                U256::from(5_988_667_735_148_u64),
            )
        );
    }

    #[test]
    fn algebra_tick_table_uses_raw_tick_rows() {
        let mut pool = ClmmPool::new_algebra_v1_9(
            117,
            219,
            10,
            get_sqrt_ratio_at_tick(300).unwrap(),
            300,
            1_000_000_000,
        )
        .unwrap();
        pool.set_tick(250, 100, 100).unwrap();
        pool.set_tick(500, 100, -100).unwrap();
        pool.set_tick(600, 100, -100).unwrap();

        assert_eq!(pool.next_initialized_tick(300, true).unwrap(), (256, false));
        assert_eq!(pool.next_initialized_tick(255, true).unwrap(), (250, true));
        assert_eq!(pool.next_initialized_tick(300, false).unwrap(), (500, true));
        assert_eq!(
            pool.next_initialized_tick(500, false).unwrap(),
            (511, false)
        );
        assert_eq!(pool.next_initialized_tick(511, false).unwrap(), (600, true));
    }

    #[test]
    fn algebra_directional_prepared_curves_match_iterative_quotes() {
        let mut pool = ClmmPool::new_algebra_v1_9(
            117,
            333,
            10,
            get_sqrt_ratio_at_tick(0).unwrap(),
            0,
            1_000_000_000,
        )
        .unwrap();
        pool.set_tick(-300, 500_000_000, 500_000_000).unwrap();
        pool.set_tick(300, 500_000_000, -500_000_000).unwrap();
        assert_eq!(pool.directional_fee_pips(), (117, 333));

        let maximum = U256::from(20_000_000_u64);
        for zero_for_one in [true, false] {
            let exact_in = pool
                .prepare_exact_input_curve_bounded(zero_for_one, maximum)
                .unwrap();
            let exact_out = pool
                .prepare_exact_output_curve_bounded(zero_for_one, maximum)
                .unwrap();
            for amount in [
                U256::ONE,
                U256::from(1_000_u64),
                U256::from(1_000_000_u64),
                maximum,
            ] {
                assert_eq!(
                    exact_in.quote(amount).unwrap(),
                    pool.quote_exact_in_amount_out(zero_for_one, amount)
                        .unwrap()
                );
                assert_eq!(
                    exact_out.quote(amount).unwrap(),
                    pool.quote_exact_out_amount_in(zero_for_one, amount)
                        .unwrap()
                );
            }
        }

        let low_fee_output = pool
            .quote_exact_in_amount_out(true, U256::from(1_000_000_u64))
            .unwrap();
        pool.set_algebra_directional_fees(5_000, 333).unwrap();
        let high_fee_output = pool
            .quote_exact_in_amount_out(true, U256::from(1_000_000_u64))
            .unwrap();
        assert!(high_fee_output < low_fee_output);
    }

    #[test]
    fn algebra_pools_share_the_immutable_word_boundary_cache() {
        let first =
            ClmmPool::new_algebra_v1_9(100, 100, 10, get_sqrt_ratio_at_tick(0).unwrap(), 0, 1_000)
                .unwrap();
        let second =
            ClmmPool::new_algebra_v1_9(200, 300, 10, get_sqrt_ratio_at_tick(1).unwrap(), 1, 2_000)
                .unwrap();

        assert!(std::sync::Arc::ptr_eq(
            &first.word_boundary_sqrt_ratios,
            &second.word_boundary_sqrt_ratios
        ));
    }

    #[test]
    #[ignore = "manual release-mode paired Camelot/Uniswap prepared-curve benchmark"]
    fn benchmark_uniswap_and_camelot_prepared_quote_and_build() {
        let uniswap = ClmmPool::new(
            500,
            10,
            get_sqrt_ratio_at_tick(0).unwrap(),
            0,
            1_000_000_000,
        )
        .unwrap();
        let camelot = ClmmPool::new_algebra_v1_9(
            500,
            500,
            10,
            get_sqrt_ratio_at_tick(0).unwrap(),
            0,
            1_000_000_000,
        )
        .unwrap();
        let maximum = U256::from(20_000_u64);
        let probe = U256::from(10_000_u64);
        let uniswap_curve = uniswap
            .prepare_exact_input_curve_bounded(true, maximum)
            .unwrap();
        let camelot_curve = camelot
            .prepare_exact_input_curve_bounded(true, maximum)
            .unwrap();
        assert_eq!(uniswap_curve.segment_count(), camelot_curve.segment_count());
        assert_eq!(
            uniswap_curve.quote(probe).unwrap(),
            camelot_curve.quote(probe).unwrap()
        );

        crate::paired_benchmark::assert_named_paired_non_regression(
            "v3_prepared_quote_benchmark",
            1.05,
            "uniswap_v3",
            "camelot_v3",
            || {
                black_box(uniswap_curve.quote(black_box(probe))).unwrap();
            },
            || {
                black_box(camelot_curve.quote(black_box(probe))).unwrap();
            },
        );
        crate::paired_benchmark::assert_named_paired_non_regression_with_work(
            "v3_prepared_curve_build_benchmark",
            1.20,
            "uniswap_v3",
            "camelot_v3",
            32,
            4_096,
            || {
                black_box(
                    uniswap
                        .prepare_exact_input_curve_bounded(true, black_box(maximum))
                        .unwrap(),
                );
            },
            || {
                black_box(
                    camelot
                        .prepare_exact_input_curve_bounded(true, black_box(maximum))
                        .unwrap(),
                );
            },
        );
    }

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
    fn prepared_curves_match_iterative_quotes_and_reject_above_capacity() {
        let mut pool = ClmmPool::new(
            3_000,
            60,
            get_sqrt_ratio_at_tick(0).unwrap(),
            0,
            1_000_000_000,
        )
        .unwrap();
        pool.set_tick(-120, 1_000_000_000, 1_000_000_000).unwrap();
        pool.set_tick(120, 1_000_000_000, -1_000_000_000).unwrap();

        for zero_for_one in [true, false] {
            let exact_in = pool.prepare_exact_input_curve(zero_for_one).unwrap();
            let exact_out = pool.prepare_exact_output_curve(zero_for_one).unwrap();
            assert!(!exact_in.specified_capacity().is_zero());
            assert!(!exact_out.specified_capacity().is_zero());
            assert!(exact_in.segment_count() >= 1);
            assert!(exact_out.segment_count() >= 1);

            for amount in [U256::ONE, U256::from(1_000_u64), U256::from(10_000_u64)] {
                assert_eq!(
                    exact_in.quote(amount).unwrap(),
                    pool.quote_exact_in_amount_out(zero_for_one, amount)
                        .unwrap()
                );
                assert_eq!(
                    exact_out.quote(amount).unwrap(),
                    pool.quote_exact_out_amount_in(zero_for_one, amount)
                        .unwrap()
                );
            }

            assert!(
                exact_in
                    .quote(exact_in.specified_capacity() + U256::ONE)
                    .is_err()
            );
            assert!(
                exact_out
                    .quote(exact_out.specified_capacity() + U256::ONE)
                    .is_err()
            );
        }
    }

    #[test]
    fn bounded_prepared_curves_stop_at_the_execution_envelope() {
        let pool = pool();
        let maximum = U256::from(20_000_u64);

        for zero_for_one in [true, false] {
            let exact_in = pool
                .prepare_exact_input_curve_bounded(zero_for_one, maximum)
                .unwrap();
            let exact_out = pool
                .prepare_exact_output_curve_bounded(zero_for_one, maximum)
                .unwrap();

            assert_eq!(exact_in.specified_capacity(), maximum);
            assert_eq!(exact_out.specified_capacity(), maximum);
            for amount in [U256::ONE, maximum / U256::from(2_u8), maximum] {
                assert_eq!(
                    exact_in.quote(amount).unwrap(),
                    pool.quote_exact_in_amount_out(zero_for_one, amount)
                        .unwrap()
                );
                assert_eq!(
                    exact_out.quote(amount).unwrap(),
                    pool.quote_exact_out_amount_in(zero_for_one, amount)
                        .unwrap()
                );
            }
            assert!(exact_in.quote(maximum + U256::ONE).is_err());
            assert!(exact_out.quote(maximum + U256::ONE).is_err());
            assert_eq!(exact_in.result_capacity(), exact_in.quote(maximum).unwrap());
            assert_eq!(
                exact_out.result_capacity(),
                exact_out.quote(maximum).unwrap()
            );
            assert_eq!(
                exact_out.result_capacity(),
                pool.exact_output_result_capacity_bounded(zero_for_one, maximum)
                    .unwrap()
            );
        }
    }

    #[test]
    fn bounded_exact_output_capacity_preserves_exhausted_liquidity_behavior() {
        let pool = pool();
        let maximum = (U256::ONE << 255) - U256::ONE;

        for zero_for_one in [true, false] {
            let prepared = pool
                .prepare_exact_output_curve_bounded(zero_for_one, maximum)
                .unwrap();
            assert!(prepared.specified_capacity() < maximum);
            assert_eq!(
                pool.exact_output_result_capacity_bounded(zero_for_one, maximum)
                    .unwrap(),
                prepared.result_capacity()
            );
            assert!(
                pool.quote_exact_out_amount_in(zero_for_one, maximum)
                    .is_err()
            );
        }
    }

    #[test]
    fn fused_output_bounded_exact_input_curve_matches_two_pass_path_exactly() {
        let mut pool = ClmmPool::new(
            500,
            10,
            get_sqrt_ratio_at_tick(0).unwrap(),
            0,
            1_000_000_000_000,
        )
        .unwrap();
        for (tick, net) in [
            (-1_100, 150_000_000_i128),
            (-500, -50_000_000_i128),
            (500, 50_000_000_i128),
            (1_100, -150_000_000_i128),
        ] {
            pool.set_tick(tick, net.unsigned_abs(), net).unwrap();
        }

        for zero_for_one in [true, false] {
            for maximum_output in [
                U256::from(20_000_u64),
                U256::from(2_000_000_u64),
                U256::from(200_000_000_u64),
                (U256::ONE << 255) - U256::ONE,
            ] {
                let legacy_input_limit = pool
                    .exact_output_result_capacity_bounded(zero_for_one, maximum_output)
                    .unwrap();
                let legacy = pool
                    .prepare_exact_input_curve_bounded(zero_for_one, legacy_input_limit)
                    .unwrap();
                let previous = pool
                    .prepare_exact_input_curve_bounded(zero_for_one, legacy_input_limit)
                    .unwrap();
                let retained_capacity = previous.segments.capacity();
                let fused = pool
                    .prepare_exact_input_curve_bounded_by_exact_output_reusing(
                        zero_for_one,
                        maximum_output,
                        Some(previous),
                    )
                    .unwrap();

                assert!(fused.segments.capacity() >= retained_capacity);
                assert_eq!(
                    fused.specified_capacity(),
                    legacy.specified_capacity(),
                    "specified capacity direction={zero_for_one} maximum_output={maximum_output}"
                );
                assert_eq!(
                    fused.result_capacity(),
                    legacy.result_capacity(),
                    "result capacity direction={zero_for_one} maximum_output={maximum_output}"
                );
                assert_eq!(
                    fused.segment_count(),
                    legacy.segment_count(),
                    "segment count direction={zero_for_one} maximum_output={maximum_output}"
                );
                let mut probes = vec![
                    U256::ONE,
                    fused.specified_capacity() / U256::from(2_u8),
                    fused.specified_capacity(),
                ];
                for segment in &legacy.segments {
                    probes.push(segment.specified_end);
                    if segment.specified_end > U256::ONE {
                        probes.push(segment.specified_end - U256::ONE);
                    }
                }
                probes.sort_unstable();
                probes.dedup();
                for amount in probes {
                    if amount.is_zero() {
                        continue;
                    }
                    assert_eq!(
                        fused.quote(amount).unwrap(),
                        legacy.quote(amount).unwrap(),
                        "direction={zero_for_one} maximum_output={maximum_output} amount={amount}"
                    );
                }
            }
        }
    }

    #[test]
    fn bounded_prepared_curve_rebuilds_retain_segment_allocations() {
        let mut pool = ClmmPool::new(
            3_000,
            60,
            get_sqrt_ratio_at_tick(0).unwrap(),
            0,
            1_000_000_000,
        )
        .unwrap();
        pool.set_tick(-120, 1_000_000_000, 1_000_000_000).unwrap();
        pool.set_tick(120, 1_000_000_000, -1_000_000_000).unwrap();
        let maximum = U256::from(20_000_u64);

        for zero_for_one in [true, false] {
            let previous_exact_input = pool
                .prepare_exact_input_curve_bounded(zero_for_one, maximum)
                .unwrap();
            let exact_input_capacity = previous_exact_input.segments.capacity();
            let previous_exact_output = pool
                .prepare_exact_output_curve_bounded(zero_for_one, maximum)
                .unwrap();
            let exact_output_capacity = previous_exact_output.segments.capacity();
            let next_tick = if zero_for_one { -1 } else { 1 };
            pool.apply_swap_head(
                get_sqrt_ratio_at_tick(next_tick).unwrap(),
                next_tick,
                1_000_000_000,
            )
            .unwrap();

            let rebuilt_exact_input = pool
                .prepare_exact_input_curve_bounded_reusing(
                    zero_for_one,
                    maximum,
                    previous_exact_input,
                )
                .unwrap();
            assert_eq!(
                rebuilt_exact_input.segments.capacity(),
                exact_input_capacity
            );
            let rebuilt_exact_output = pool
                .prepare_exact_output_curve_bounded_reusing(
                    zero_for_one,
                    maximum,
                    previous_exact_output,
                )
                .unwrap();
            assert_eq!(
                rebuilt_exact_output.segments.capacity(),
                exact_output_capacity
            );

            for amount in [U256::ONE, maximum / U256::from(2_u8), maximum] {
                assert_eq!(
                    rebuilt_exact_input.quote(amount).unwrap(),
                    pool.quote_exact_in_amount_out(zero_for_one, amount)
                        .unwrap()
                );
                assert_eq!(
                    rebuilt_exact_output.quote(amount).unwrap(),
                    pool.quote_exact_out_amount_in(zero_for_one, amount)
                        .unwrap()
                );
            }
        }
    }

    #[test]
    fn prepared_curves_preserve_rounding_across_every_boundary() {
        let mut pool = ClmmPool::new(
            3_000,
            60,
            get_sqrt_ratio_at_tick(0).unwrap(),
            0,
            1_000_000_000,
        )
        .unwrap();
        pool.set_tick(-120, 500_000_000, 500_000_000).unwrap();
        pool.set_tick(-60, 500_000_000, 500_000_000).unwrap();
        pool.set_tick(120, 1_000_000_000, -1_000_000_000).unwrap();

        for zero_for_one in [true, false] {
            let exact_in = pool.prepare_exact_input_curve(zero_for_one).unwrap();
            let mut specified_start = U256::ZERO;
            for segment in &exact_in.segments {
                for amount in boundary_samples(specified_start, segment.specified_end) {
                    assert_eq!(
                        exact_in.quote(amount).unwrap(),
                        pool.quote_exact_in_amount_out(zero_for_one, amount)
                            .unwrap()
                    );
                }
                specified_start = segment.specified_end;
            }

            let exact_out = pool.prepare_exact_output_curve(zero_for_one).unwrap();
            let mut specified_start = U256::ZERO;
            for segment in &exact_out.segments {
                for amount in boundary_samples(specified_start, segment.specified_end) {
                    assert_eq!(
                        exact_out.quote(amount).unwrap(),
                        pool.quote_exact_out_amount_in(zero_for_one, amount)
                            .unwrap()
                    );
                }
                specified_start = segment.specified_end;
            }
        }
    }

    fn boundary_samples(start: U256, end: U256) -> Vec<U256> {
        let mut samples = Vec::with_capacity(3);
        if start < end {
            samples.push(start + U256::ONE);
            if end - start > U256::ONE {
                samples.push(end - U256::ONE);
            }
            samples.push(end);
        }
        samples
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

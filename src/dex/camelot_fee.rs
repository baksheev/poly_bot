use std::collections::BTreeMap;

use alloy_primitives::{I256, U256};
use anyhow::{Context, ensure};

const WINDOW_SECONDS: u32 = 86_400;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveFeeConfiguration {
    pub alpha1: u16,
    pub alpha2: u16,
    pub beta1: u32,
    pub beta2: u32,
    pub gamma1: u16,
    pub gamma2: u16,
    pub volume_beta: u32,
    pub volume_gamma: u16,
    pub base_fee: u16,
}

impl AdaptiveFeeConfiguration {
    pub fn validate(self) -> anyhow::Result<Self> {
        ensure!(
            u32::from(self.alpha1) + u32::from(self.alpha2) + u32::from(self.base_fee)
                <= u32::from(u16::MAX),
            "Camelot adaptive fee maximum exceeds uint16"
        );
        ensure!(
            self.gamma1 != 0 && self.gamma2 != 0 && self.volume_gamma != 0,
            "Camelot adaptive fee gamma is zero"
        );
        Ok(self)
    }

    pub fn fee(self, volatility: u128, volume_per_liquidity: U256) -> anyhow::Result<u16> {
        self.validate()?;
        let first = sigmoid(
            U256::from(volatility),
            self.gamma1,
            self.alpha1,
            U256::from(self.beta1),
        )?;
        let second = sigmoid(
            U256::from(volatility),
            self.gamma2,
            self.alpha2,
            U256::from(self.beta2),
        )?;
        let sum = first
            .checked_add(second)
            .context("Camelot sigmoid sum overflow")?
            .min(U256::from(u16::MAX));
        let sum_u16: u16 = sum
            .try_into()
            .context("Camelot sigmoid sum does not fit uint16")?;
        let variable = sigmoid(
            volume_per_liquidity,
            self.volume_gamma,
            sum_u16,
            U256::from(self.volume_beta),
        )?;
        let variable: u16 = variable
            .try_into()
            .context("Camelot volume sigmoid does not fit uint16")?;
        self.base_fee
            .checked_add(variable)
            .context("Camelot adaptive fee overflow")
    }
}

fn sigmoid(x: U256, gamma: u16, alpha: u16, beta: U256) -> anyhow::Result<U256> {
    ensure!(gamma != 0, "Camelot sigmoid gamma is zero");
    let gamma_u256 = U256::from(gamma);
    let six_gamma = gamma_u256 * U256::from(6_u8);
    let gamma_eighth = power_eight(gamma_u256)?;
    if x > beta {
        let shifted = x - beta;
        if shifted >= six_gamma {
            return Ok(U256::from(alpha));
        }
        let exponent = exp_series(shifted, gamma_u256, gamma_eighth)?;
        Ok(U256::from(alpha) * exponent / (gamma_eighth + exponent))
    } else {
        let shifted = beta - x;
        if shifted >= six_gamma {
            return Ok(U256::ZERO);
        }
        let denominator = gamma_eighth
            .checked_add(exp_series(shifted, gamma_u256, gamma_eighth)?)
            .context("Camelot sigmoid denominator overflow")?;
        Ok(U256::from(alpha) * gamma_eighth / denominator)
    }
}

fn power_eight(value: U256) -> anyhow::Result<U256> {
    let squared = value
        .checked_mul(value)
        .context("Camelot gamma square overflow")?;
    let fourth = squared
        .checked_mul(squared)
        .context("Camelot gamma fourth power overflow")?;
    fourth
        .checked_mul(fourth)
        .context("Camelot gamma eighth power overflow")
}

fn exp_series(x: U256, gamma: U256, gamma_eighth: U256) -> anyhow::Result<U256> {
    let mut x_power = x;
    let mut gamma_power = gamma_eighth;
    let mut result = gamma_eighth;
    let divisors = [1_u64, 2, 6, 24, 120, 720];
    for divisor in divisors {
        gamma_power /= gamma;
        result = result
            .checked_add(
                x_power
                    .checked_mul(gamma_power)
                    .context("Camelot exponential term overflow")?
                    / U256::from(divisor),
            )
            .context("Camelot exponential sum overflow")?;
        x_power = x_power
            .checked_mul(x)
            .context("Camelot exponential power overflow")?;
    }
    let seventh = x_power;
    let eighth = seventh
        .checked_mul(x)
        .context("Camelot exponential eighth power overflow")?;
    result = result
        .checked_add(
            seventh
                .checked_mul(gamma)
                .context("Camelot exponential seventh term overflow")?
                / U256::from(5_040_u64),
        )
        .context("Camelot exponential sum overflow")?;
    result
        .checked_add(eighth / U256::from(40_320_u64))
        .context("Camelot exponential sum overflow")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Timepoint {
    pub initialized: bool,
    pub block_timestamp: u32,
    pub tick_cumulative: i128,
    pub seconds_per_liquidity_cumulative: U256,
    pub volatility_cumulative: u128,
    pub average_tick: i32,
    pub volume_per_liquidity_cumulative: U256,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectionalFees {
    pub zero_for_one: u16,
    pub one_for_zero: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeeEnvelope {
    pub current: DirectionalFees,
    pub maximum: DirectionalFees,
    pub first_timestamp: u32,
    pub last_timestamp: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeeProjectionState {
    pub head_timestamp: u32,
    pub latest_timepoint_timestamp: u32,
    pub tick: i32,
    pub liquidity: u128,
    pub index: u16,
    pub oldest_index: u16,
    pub current_fees: DirectionalFees,
    pub volume_per_liquidity_in_block: u128,
    pub zero_for_one_config: AdaptiveFeeConfiguration,
    pub one_for_zero_config: AdaptiveFeeConfiguration,
    pub timepoints: BTreeMap<u16, Timepoint>,
}

impl FeeProjectionState {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.zero_for_one_config.validate()?;
        self.one_for_zero_config.validate()?;
        ensure!(
            self.liquidity != 0,
            "Camelot fee projection liquidity is zero"
        );
        ensure!(
            self.timepoints.get(&self.index).is_some_and(|point| {
                point.initialized && point.block_timestamp == self.latest_timepoint_timestamp
            }),
            "Camelot latest timepoint is unavailable"
        );
        ensure!(
            self.timepoints
                .get(&self.oldest_index)
                .is_some_and(|point| point.initialized),
            "Camelot oldest timepoint is unavailable"
        );
        Ok(())
    }

    pub fn envelope(&self, horizon_seconds: u32) -> anyhow::Result<FeeEnvelope> {
        self.validate()?;
        let last_timestamp = self
            .head_timestamp
            .checked_add(horizon_seconds)
            .context("Camelot fee horizon exceeds uint32")?;
        let current = self.fees_at(self.head_timestamp)?;
        let mut maximum = current;
        for timestamp in self.head_timestamp.saturating_add(1)..=last_timestamp {
            let fees = self.fees_at(timestamp)?;
            maximum.zero_for_one = maximum.zero_for_one.max(fees.zero_for_one);
            maximum.one_for_zero = maximum.one_for_zero.max(fees.one_for_zero);
        }
        Ok(FeeEnvelope {
            current,
            maximum,
            first_timestamp: self.head_timestamp,
            last_timestamp,
        })
    }

    /// Applies the first pool action at a new block timestamp. Algebra writes
    /// the timepoint, asks the data-storage operator for both fees, emits
    /// `Fee`, and only then mutates pool price/liquidity. Keeping this as a
    /// separate transition lets the mirror enforce the same event order.
    pub fn apply_fee_timepoint(
        &mut self,
        timestamp: u32,
        emitted: DirectionalFees,
    ) -> anyhow::Result<()> {
        self.validate()?;
        let last = self.latest()?.clone();
        ensure!(
            timestamp > last.block_timestamp,
            "Camelot Fee does not advance the latest timepoint"
        );
        ensure!(
            timestamp >= self.head_timestamp,
            "Camelot Fee timestamp precedes canonical fee state"
        );
        let expected = self.fees_at(timestamp)?;
        ensure!(
            emitted == expected,
            "Camelot Fee event differs from local adaptive-fee projection"
        );
        let average_tick = self.average_tick_for_new_timepoint(timestamp, &last)?;
        let previous_tick = self.previous_tick(&last)?;
        let point = create_new_timepoint(
            &last,
            timestamp,
            self.tick,
            previous_tick,
            self.liquidity,
            average_tick,
            self.volume_per_liquidity_in_block,
        )?;
        let next_index = self.index.wrapping_add(1);
        self.index = next_index;
        self.timepoints.insert(next_index, point);
        self.oldest_index = self.oldest_index_after_write(next_index)?;
        self.latest_timepoint_timestamp = timestamp;
        self.current_fees = emitted;
        self.volume_per_liquidity_in_block = 0;
        self.head_timestamp = timestamp;
        Ok(())
    }

    pub fn apply_swap(
        &mut self,
        timestamp: u32,
        amount0: I256,
        amount1: I256,
        tick: i32,
        liquidity: u128,
    ) -> anyhow::Result<()> {
        ensure!(
            timestamp >= self.latest_timestamp()?,
            "Camelot Swap timestamp precedes latest timepoint"
        );
        self.apply_swap_after_timepoint_validation(timestamp, amount0, amount1, tick, liquidity)
    }

    pub(crate) fn apply_swap_after_timepoint_validation(
        &mut self,
        timestamp: u32,
        amount0: I256,
        amount1: I256,
        tick: i32,
        liquidity: u128,
    ) -> anyhow::Result<()> {
        ensure!(liquidity != 0, "Camelot Swap liquidity is zero");
        let volume = calculate_volume_per_liquidity_nonzero(liquidity, amount0, amount1);
        // The deployed 0.7.x arithmetic uses uint128 wrapping semantics.
        self.volume_per_liquidity_in_block =
            self.volume_per_liquidity_in_block.wrapping_add(volume);
        self.tick = tick;
        self.liquidity = liquidity;
        self.head_timestamp = self.head_timestamp.max(timestamp);
        Ok(())
    }

    pub fn apply_liquidity_head(
        &mut self,
        timestamp: u32,
        tick: i32,
        liquidity: u128,
    ) -> anyhow::Result<()> {
        self.validate()?;
        ensure!(
            timestamp >= self.latest()?.block_timestamp,
            "Camelot liquidity event timestamp precedes latest timepoint"
        );
        ensure!(liquidity != 0, "Camelot active liquidity became zero");
        self.tick = tick;
        self.liquidity = liquidity;
        self.head_timestamp = self.head_timestamp.max(timestamp);
        Ok(())
    }

    pub fn advance_head(&mut self, timestamp: u32) -> anyhow::Result<()> {
        self.validate()?;
        ensure!(
            timestamp >= self.head_timestamp,
            "Camelot canonical head timestamp moved backwards"
        );
        self.head_timestamp = timestamp;
        Ok(())
    }

    pub fn latest_timestamp(&self) -> anyhow::Result<u32> {
        Ok(self.latest_timepoint_timestamp)
    }

    fn oldest_index_after_write(&self, written_index: u16) -> anyhow::Result<u16> {
        let candidate = written_index.wrapping_add(1);
        let oldest = if self
            .timepoints
            .get(&candidate)
            .is_some_and(|point| point.initialized)
        {
            candidate
        } else {
            0
        };
        ensure!(
            self.timepoints
                .get(&oldest)
                .is_some_and(|point| point.initialized),
            "Camelot oldest timepoint after write is unavailable"
        );
        Ok(oldest)
    }

    pub fn fees_at(&self, timestamp: u32) -> anyhow::Result<DirectionalFees> {
        self.validate()?;
        ensure!(
            timestamp >= self.head_timestamp,
            "Camelot fee projection precedes canonical head"
        );
        let last = self.latest()?;
        if timestamp == last.block_timestamp {
            return Ok(self.current_fees);
        }
        ensure!(
            timestamp > last.block_timestamp,
            "Camelot fee projection timestamp precedes latest timepoint"
        );

        let (volatility, volume) = self.projected_averages_at(timestamp)?;
        let fee_volatility = volatility / 15;
        Ok(DirectionalFees {
            zero_for_one: self.zero_for_one_config.fee(fee_volatility, volume)?,
            one_for_zero: self.one_for_zero_config.fee(fee_volatility, volume)?,
        })
    }

    pub fn projected_averages_at(&self, timestamp: u32) -> anyhow::Result<(u128, U256)> {
        self.validate()?;
        let last = self.latest()?;
        ensure!(
            timestamp > last.block_timestamp,
            "Camelot projected averages require a new timepoint"
        );
        let average_tick = self.average_tick_for_new_timepoint(timestamp, last)?;
        let previous_tick = self.previous_tick(last)?;
        let projected = create_new_timepoint(
            last,
            timestamp,
            self.tick,
            previous_tick,
            self.liquidity,
            average_tick,
            self.volume_per_liquidity_in_block,
        )?;
        let projected_index = self.index.wrapping_add(1);
        let mut projected_state = self.clone();
        projected_state.index = projected_index;
        projected_state
            .timepoints
            .insert(projected_index, projected);
        projected_state.oldest_index = projected_state.oldest_index_after_write(projected_index)?;
        projected_state.averages(timestamp)
    }

    fn latest(&self) -> anyhow::Result<&Timepoint> {
        self.timepoints
            .get(&self.index)
            .context("Camelot latest timepoint is missing")
    }

    fn previous_tick(&self, last: &Timepoint) -> anyhow::Result<i32> {
        if self.index == self.oldest_index {
            return Ok(self.tick);
        }
        let previous = self
            .timepoints
            .get(&self.index.wrapping_sub(1))
            .context("Camelot previous timepoint is missing")?;
        let elapsed = last
            .block_timestamp
            .checked_sub(previous.block_timestamp)
            .context("Camelot previous timepoint timestamp is not ordered")?;
        ensure!(
            elapsed != 0,
            "Camelot adjacent timepoints share a timestamp"
        );
        let delta = last
            .tick_cumulative
            .checked_sub(previous.tick_cumulative)
            .context("Camelot tick cumulative delta overflow")?;
        i32::try_from(delta / i128::from(elapsed))
            .context("Camelot previous tick does not fit int24")
    }

    fn average_tick_for_new_timepoint(
        &self,
        timestamp: u32,
        last: &Timepoint,
    ) -> anyhow::Result<i32> {
        let oldest = self
            .timepoints
            .get(&self.oldest_index)
            .context("Camelot oldest timepoint is missing")?;
        let window_start = timestamp.wrapping_sub(WINDOW_SECONDS);
        let average = if lte_considering_overflow(oldest.block_timestamp, window_start, timestamp) {
            if lte_considering_overflow(last.block_timestamp, window_start, timestamp) {
                let start = self
                    .timepoints
                    .get(&self.index.wrapping_sub(1))
                    .context("Camelot average-tick start timepoint is missing")?;
                if start.initialized {
                    let elapsed = last
                        .block_timestamp
                        .checked_sub(start.block_timestamp)
                        .context("Camelot average-tick timestamps are not ordered")?;
                    ensure!(elapsed != 0, "Camelot average-tick interval is zero");
                    (last.tick_cumulative - start.tick_cumulative) / i128::from(elapsed)
                } else {
                    i128::from(self.tick)
                }
            } else {
                let start = self.single_timepoint(timestamp, WINDOW_SECONDS, self.index)?;
                let since_last = timestamp
                    .checked_sub(last.block_timestamp)
                    .context("Camelot average-tick timestamp is not ordered")?;
                let elapsed = WINDOW_SECONDS
                    .checked_sub(since_last)
                    .context("Camelot average-tick window underflow")?;
                ensure!(elapsed != 0, "Camelot average-tick window is zero");
                (last.tick_cumulative - start.tick_cumulative) / i128::from(elapsed)
            }
        } else if last.block_timestamp == oldest.block_timestamp {
            i128::from(self.tick)
        } else {
            let elapsed = last.block_timestamp - oldest.block_timestamp;
            (last.tick_cumulative - oldest.tick_cumulative) / i128::from(elapsed)
        };
        let average = i32::try_from(average).context("Camelot average tick does not fit int24")?;
        ensure!(
            (-8_388_608..=8_388_607).contains(&average),
            "Camelot average tick is outside int24"
        );
        Ok(average)
    }

    fn averages(&self, timestamp: u32) -> anyhow::Result<(u128, U256)> {
        let oldest = self
            .timepoints
            .get(&self.oldest_index)
            .context("Camelot oldest timepoint is missing")?;
        let end = self.single_timepoint(timestamp, 0, self.index)?;
        let window_start = timestamp.wrapping_sub(WINDOW_SECONDS);
        if lte_considering_overflow(oldest.block_timestamp, window_start, timestamp) {
            let start = self.single_timepoint(timestamp, WINDOW_SECONDS, self.index)?;
            Ok((
                (end.volatility_cumulative - start.volatility_cumulative)
                    / u128::from(WINDOW_SECONDS),
                (end.volume_per_liquidity_cumulative - start.volume_per_liquidity_cumulative) >> 57,
            ))
        } else if timestamp != oldest.block_timestamp {
            let elapsed = timestamp - oldest.block_timestamp;
            Ok((
                (end.volatility_cumulative - oldest.volatility_cumulative) / u128::from(elapsed),
                (end.volume_per_liquidity_cumulative - oldest.volume_per_liquidity_cumulative)
                    >> 57,
            ))
        } else {
            Ok((0, U256::ZERO))
        }
    }

    fn single_timepoint(
        &self,
        timestamp: u32,
        seconds_ago: u32,
        index: u16,
    ) -> anyhow::Result<Timepoint> {
        let target = timestamp.wrapping_sub(seconds_ago);
        let last = self
            .timepoints
            .get(&index)
            .context("Camelot requested latest timepoint is missing")?;
        if seconds_ago == 0 || lte_considering_overflow(last.block_timestamp, target, timestamp) {
            if last.block_timestamp == target {
                return Ok(last.clone());
            }
            let average_tick = self.average_tick_for_new_timepoint(timestamp, last)?;
            let previous_tick = self.previous_tick(last)?;
            return create_new_timepoint(
                last,
                target,
                self.tick,
                previous_tick,
                self.liquidity,
                average_tick,
                0,
            );
        }

        let mut before: Option<&Timepoint> = None;
        let mut after: Option<&Timepoint> = None;
        for point in self.timepoints.values().filter(|point| point.initialized) {
            if point.block_timestamp <= target
                && before.is_none_or(|candidate| candidate.block_timestamp < point.block_timestamp)
            {
                before = Some(point);
            }
            if point.block_timestamp >= target
                && after.is_none_or(|candidate| candidate.block_timestamp > point.block_timestamp)
            {
                after = Some(point);
            }
        }
        let before = before.context("Camelot timepoint before window target is missing")?;
        let after = after.context("Camelot timepoint after window target is missing")?;
        if target == after.block_timestamp || target == before.block_timestamp {
            return Ok(if target == after.block_timestamp {
                after.clone()
            } else {
                before.clone()
            });
        }
        interpolate_timepoint(before, after, target)
    }
}

/// Mirrors Algebra's `calculateVolumePerLiquidity`: two floor square roots,
/// their product shifted by 64 bits (or saturated on the Solidity overflow
/// branch), divided by final active liquidity, then capped at 100000 << 64.
pub fn calculate_volume_per_liquidity(
    liquidity: u128,
    amount0: I256,
    amount1: I256,
) -> anyhow::Result<u128> {
    ensure!(liquidity != 0, "Camelot volume liquidity is zero");
    Ok(calculate_volume_per_liquidity_nonzero(
        liquidity, amount0, amount1,
    ))
}

#[inline]
fn calculate_volume_per_liquidity_nonzero(liquidity: u128, amount0: I256, amount1: I256) -> u128 {
    let absolute0 = signed_abs(amount0);
    let absolute1 = signed_abs(amount1);
    if let (Ok(absolute0), Ok(absolute1)) = (u128::try_from(absolute0), u128::try_from(absolute1)) {
        let product = absolute0.isqrt() * absolute1.isqrt();
        if product <= u128::from(u64::MAX) {
            return ((product << 64) / liquidity).min(100_000_u128 << 64);
        }
    }
    let root0 = integer_sqrt(absolute0);
    let root1 = integer_sqrt(absolute1);
    let product = root0 * root1;
    let value = if product >= (U256::ONE << 192_usize) {
        U256::MAX / U256::from(liquidity)
    } else {
        (product << 64_usize) / U256::from(liquidity)
    };
    let capped = value.min(U256::from(100_000_u32) << 64_usize);
    capped
        .try_into()
        .expect("Camelot volume cap always fits uint128")
}

fn signed_abs(value: I256) -> U256 {
    let raw = value.into_raw();
    if value.is_negative() {
        (!raw).wrapping_add(U256::ONE)
    } else {
        raw
    }
}

fn integer_sqrt(value: U256) -> U256 {
    if value.is_zero() {
        return U256::ZERO;
    }
    // Preserve the deployed Sqrt.sol seed and fixed seven Newton iterations.
    let mut reduced = value;
    let mut root = U256::ONE;
    for (threshold_bits, root_bits) in [
        (128_usize, 64_usize),
        (64, 32),
        (32, 16),
        (16, 8),
        (8, 4),
        (4, 2),
        (3, 1),
    ] {
        if reduced >= (U256::ONE << threshold_bits) {
            reduced >>= threshold_bits;
            root <<= root_bits;
        }
    }
    for _ in 0..7 {
        root = (root + value / root) >> 1_usize;
    }
    root.min(value / root)
}

fn lte_considering_overflow(a: u32, b: u32, current_time: u32) -> bool {
    let a_overflowed = a > current_time;
    if a_overflowed == (b > current_time) {
        a <= b
    } else {
        a_overflowed
    }
}

fn create_new_timepoint(
    last: &Timepoint,
    timestamp: u32,
    tick: i32,
    previous_tick: i32,
    liquidity: u128,
    average_tick: i32,
    volume_per_liquidity: u128,
) -> anyhow::Result<Timepoint> {
    let delta = timestamp
        .checked_sub(last.block_timestamp)
        .context("Camelot new timepoint timestamp is not ordered")?;
    ensure!(delta != 0, "Camelot new timepoint delta is zero");
    Ok(Timepoint {
        initialized: true,
        block_timestamp: timestamp,
        tick_cumulative: last
            .tick_cumulative
            .checked_add(i128::from(tick) * i128::from(delta))
            .context("Camelot tick cumulative overflow")?,
        seconds_per_liquidity_cumulative: last.seconds_per_liquidity_cumulative
            + ((U256::from(delta) << 128) / U256::from(liquidity.max(1))),
        volatility_cumulative: last
            .volatility_cumulative
            .checked_add(volatility_on_range(
                delta,
                previous_tick,
                tick,
                last.average_tick,
                average_tick,
            )?)
            .context("Camelot volatility cumulative overflow")?,
        average_tick,
        volume_per_liquidity_cumulative: last.volume_per_liquidity_cumulative
            + U256::from(volume_per_liquidity),
    })
}

fn volatility_on_range(
    delta: u32,
    tick0: i32,
    tick1: i32,
    average_tick0: i32,
    average_tick1: i32,
) -> anyhow::Result<u128> {
    let dt = i128::from(delta);
    let k = (i128::from(tick1) - i128::from(tick0))
        - (i128::from(average_tick1) - i128::from(average_tick0));
    let b = (i128::from(tick0) - i128::from(average_tick0))
        .checked_mul(dt)
        .context("Camelot volatility B overflow")?;
    let sum_squares = dt
        .checked_mul(dt + 1)
        .and_then(|value| value.checked_mul(2 * dt + 1))
        .context("Camelot volatility square sequence overflow")?;
    let sum_sequence = dt
        .checked_mul(dt + 1)
        .context("Camelot volatility sequence overflow")?;
    let numerator = k
        .checked_mul(k)
        .and_then(|value| value.checked_mul(sum_squares))
        .and_then(|value| {
            b.checked_mul(k)
                .and_then(|cross| cross.checked_mul(sum_sequence))
                .and_then(|cross| cross.checked_mul(6))
                .and_then(|cross| value.checked_add(cross))
        })
        .and_then(|value| {
            b.checked_mul(b)
                .and_then(|square| square.checked_mul(dt))
                .and_then(|square| square.checked_mul(6))
                .and_then(|square| value.checked_add(square))
        })
        .context("Camelot volatility numerator overflow")?;
    let denominator = 6_i128
        .checked_mul(dt)
        .and_then(|value| value.checked_mul(dt))
        .context("Camelot volatility denominator overflow")?;
    ensure!(numerator >= 0, "Camelot volatility is negative");
    u128::try_from(numerator / denominator).context("Camelot volatility does not fit uint128")
}

fn interpolate_timepoint(
    before: &Timepoint,
    after: &Timepoint,
    target: u32,
) -> anyhow::Result<Timepoint> {
    let interval = after
        .block_timestamp
        .checked_sub(before.block_timestamp)
        .context("Camelot interpolation points are not ordered")?;
    ensure!(interval != 0, "Camelot interpolation interval is zero");
    let target_delta = target
        .checked_sub(before.block_timestamp)
        .context("Camelot interpolation target is not ordered")?;
    let interval_i128 = i128::from(interval);
    let target_i128 = i128::from(target_delta);
    Ok(Timepoint {
        initialized: before.initialized,
        block_timestamp: before.block_timestamp,
        tick_cumulative: before.tick_cumulative
            + ((after.tick_cumulative - before.tick_cumulative) / interval_i128) * target_i128,
        seconds_per_liquidity_cumulative: before.seconds_per_liquidity_cumulative
            + ((after.seconds_per_liquidity_cumulative - before.seconds_per_liquidity_cumulative)
                * U256::from(target_delta)
                / U256::from(interval)),
        volatility_cumulative: before.volatility_cumulative
            + ((after.volatility_cumulative - before.volatility_cumulative) / u128::from(interval))
                * u128::from(target_delta),
        average_tick: before.average_tick,
        volume_per_liquidity_cumulative: before.volume_per_liquidity_cumulative
            + ((after.volume_per_liquidity_cumulative - before.volume_per_liquidity_cumulative)
                / U256::from(interval))
                * U256::from(target_delta),
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, hint::black_box};

    use super::{
        AdaptiveFeeConfiguration, DirectionalFees, FeeProjectionState, I256, Timepoint, U256,
        calculate_volume_per_liquidity, exp_series, integer_sqrt, power_eight, sigmoid,
    };

    fn fixed_fee_state(timestamp: u32) -> FeeProjectionState {
        let config = AdaptiveFeeConfiguration {
            alpha1: 0,
            alpha2: 0,
            beta1: 0,
            beta2: 0,
            gamma1: 1,
            gamma2: 1,
            volume_beta: 0,
            volume_gamma: 1,
            base_fee: 100,
        };
        let mut timepoints = BTreeMap::new();
        timepoints.insert(
            0,
            Timepoint {
                initialized: true,
                block_timestamp: timestamp,
                tick_cumulative: 0,
                seconds_per_liquidity_cumulative: U256::ZERO,
                volatility_cumulative: 0,
                average_tick: 0,
                volume_per_liquidity_cumulative: U256::ZERO,
            },
        );
        FeeProjectionState {
            head_timestamp: timestamp,
            latest_timepoint_timestamp: timestamp,
            tick: 0,
            liquidity: 1_000,
            index: 0,
            oldest_index: 0,
            current_fees: DirectionalFees {
                zero_for_one: 100,
                one_for_zero: 100,
            },
            volume_per_liquidity_in_block: 7,
            zero_for_one_config: config,
            one_for_zero_config: config,
            timepoints,
        }
    }

    #[test]
    fn deployed_adaptive_fee_series_keeps_solidity_operation_order() {
        let gamma = U256::from(100_u16);
        let gamma_eighth = power_eight(gamma).unwrap();
        assert_eq!(gamma_eighth, U256::from(10_000_000_000_000_000_u64));
        assert_eq!(
            exp_series(U256::ZERO, gamma, gamma_eighth).unwrap(),
            gamma_eighth
        );
        assert_eq!(
            sigmoid(U256::from(100_u16), 100, 1_000, U256::from(100_u16)).unwrap(),
            U256::from(500_u16)
        );
        assert_eq!(
            sigmoid(U256::from(700_u16), 100, 1_000, U256::from(100_u16)).unwrap(),
            U256::from(1_000_u16)
        );
        assert_eq!(
            sigmoid(U256::ZERO, 100, 1_000, U256::from(600_u16)).unwrap(),
            U256::ZERO
        );
    }

    #[test]
    fn adaptive_fee_validates_deployed_uint16_constraints() {
        let valid = AdaptiveFeeConfiguration {
            alpha1: 2_900,
            alpha2: 12_000,
            beta1: 360,
            beta2: 60_000,
            gamma1: 59,
            gamma2: 8_500,
            volume_beta: 0,
            volume_gamma: 10,
            base_fee: 100,
        };
        assert!(valid.validate().is_ok());
        assert!(valid.fee(0, U256::ZERO).is_ok());

        let invalid = AdaptiveFeeConfiguration { gamma1: 0, ..valid };
        assert!(invalid.validate().is_err());
        let invalid = AdaptiveFeeConfiguration {
            alpha1: 60_000,
            alpha2: 6_000,
            ..valid
        };
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn volume_per_liquidity_matches_algebra_floor_roots_shift_and_cap() {
        assert_eq!(integer_sqrt(U256::from(15_u8)), U256::from(3_u8));
        assert_eq!(integer_sqrt(U256::from(16_u8)), U256::from(4_u8));
        assert_eq!(
            calculate_volume_per_liquidity(
                1,
                I256::try_from(4_i64).unwrap(),
                I256::try_from(-9_i64).unwrap(),
            )
            .unwrap(),
            6_u128 << 64
        );
        assert_eq!(
            calculate_volume_per_liquidity(
                1,
                I256::from_raw(U256::MAX),
                I256::from_raw(U256::MAX),
            )
            .unwrap(),
            1_u128 << 64
        );
        assert_eq!(
            calculate_volume_per_liquidity(
                1,
                I256::from_raw(U256::ONE << 254_usize),
                I256::from_raw(U256::ONE << 254_usize),
            )
            .unwrap(),
            100_000_u128 << 64
        );
    }

    #[test]
    fn fee_then_swap_mirrors_timepoint_reset_and_new_block_volume() {
        let mut state = fixed_fee_state(100);
        state
            .apply_fee_timepoint(
                101,
                DirectionalFees {
                    zero_for_one: 100,
                    one_for_zero: 100,
                },
            )
            .unwrap();
        assert_eq!(state.index, 1);
        assert_eq!(state.oldest_index, 0);
        assert_eq!(state.latest_timestamp().unwrap(), 101);
        assert_eq!(state.volume_per_liquidity_in_block, 0);
        state
            .apply_swap(
                101,
                I256::try_from(4_i64).unwrap(),
                I256::try_from(-9_i64).unwrap(),
                1,
                2_000,
            )
            .unwrap();
        assert_eq!(state.tick, 1);
        assert_eq!(state.liquidity, 2_000);
        assert_eq!(
            state.volume_per_liquidity_in_block,
            u128::try_from((U256::from(6_u8) << 64_usize) / U256::from(2_000_u16)).unwrap()
        );
    }

    #[test]
    fn fee_event_must_match_local_directional_projection() {
        let mut state = fixed_fee_state(100);
        assert!(
            state
                .apply_fee_timepoint(
                    101,
                    DirectionalFees {
                        zero_for_one: 101,
                        one_for_zero: 100,
                    },
                )
                .is_err()
        );
        assert_eq!(state.index, 0);
    }

    #[test]
    fn overwritten_oldest_falls_back_to_zero_when_successor_is_uninitialized() {
        let mut state = fixed_fee_state(100);
        state.oldest_index = 1;
        state.timepoints.insert(
            1,
            Timepoint {
                initialized: true,
                block_timestamp: 50,
                tick_cumulative: 0,
                seconds_per_liquidity_cumulative: U256::ZERO,
                volatility_cumulative: 0,
                average_tick: 0,
                volume_per_liquidity_cumulative: U256::ZERO,
            },
        );
        state.timepoints.insert(
            2,
            Timepoint {
                initialized: false,
                block_timestamp: 0,
                tick_cumulative: 0,
                seconds_per_liquidity_cumulative: U256::ZERO,
                volatility_cumulative: 0,
                average_tick: 0,
                volume_per_liquidity_cumulative: U256::ZERO,
            },
        );
        assert_eq!(state.oldest_index_after_write(1).unwrap(), 0);
    }

    #[test]
    fn full_ring_projection_advances_oldest_index_before_computing_averages() {
        fn point(timestamp: u32, volatility: u128, volume: U256) -> Timepoint {
            Timepoint {
                initialized: true,
                block_timestamp: timestamp,
                tick_cumulative: 0,
                seconds_per_liquidity_cumulative: U256::from(timestamp) << 128,
                volatility_cumulative: volatility,
                average_tick: 0,
                volume_per_liquidity_cumulative: volume,
            }
        }

        let unit: U256 = U256::ONE << 57_usize;
        let mut timepoints = BTreeMap::new();
        timepoints.insert(0, point(100_000, 0, U256::ZERO));
        timepoints.insert(1, point(110_000, 1_000, U256::from(100_u16) * unit));
        timepoints.insert(2, point(120_000, 101_000, U256::from(200_u16) * unit));
        timepoints.insert(
            65_534,
            point(199_980, 1_000_000, U256::from(1_000_u16) * unit),
        );
        timepoints.insert(
            65_535,
            point(199_990, 1_000_000, U256::from(1_000_u16) * unit),
        );
        let config = AdaptiveFeeConfiguration {
            alpha1: 150,
            alpha2: 500,
            beta1: 720,
            beta2: 60_000,
            gamma1: 59,
            gamma2: 8_500,
            volume_beta: 0,
            volume_gamma: 10,
            base_fee: 100,
        };
        let state = FeeProjectionState {
            head_timestamp: 200_000,
            latest_timepoint_timestamp: 199_990,
            tick: 0,
            liquidity: 1,
            index: 65_535,
            oldest_index: 0,
            current_fees: DirectionalFees {
                zero_for_one: 117,
                one_for_zero: 117,
            },
            volume_per_liquidity_in_block: u128::try_from(U256::from(50_u8) * unit).unwrap(),
            zero_for_one_config: config,
            one_for_zero_config: config,
            timepoints,
        };
        let (volatility, volume) = state.projected_averages_at(200_000).unwrap();
        let interpolated_volatility = 1_000 + ((101_000 - 1_000) / 10_000) * 3_600;
        assert_eq!(volatility, (1_000_000 - interpolated_volatility) / 86_400);
        assert!(volume > U256::ZERO);
        assert!(state.fees_at(200_000).is_ok());
    }

    #[test]
    #[ignore = "manual release-mode Camelot fee projection and curve-build benchmark"]
    fn benchmark_camelot_fee_projection_and_curve_build() {
        fn point(timestamp: u32, volatility: u128, volume: U256) -> Timepoint {
            Timepoint {
                initialized: true,
                block_timestamp: timestamp,
                tick_cumulative: 0,
                seconds_per_liquidity_cumulative: U256::from(timestamp) << 128,
                volatility_cumulative: volatility,
                average_tick: 0,
                volume_per_liquidity_cumulative: volume,
            }
        }
        let unit: U256 = U256::ONE << 57_usize;
        let mut timepoints = BTreeMap::new();
        for index in 100_u16..132 {
            timepoints.insert(
                index,
                point(
                    50_000 + u32::from(index),
                    u128::from(index),
                    U256::from(index) * unit,
                ),
            );
        }
        timepoints.insert(0, point(100_000, 0, U256::ZERO));
        timepoints.insert(1, point(110_000, 1_000, U256::from(100_u16) * unit));
        timepoints.insert(2, point(120_000, 101_000, U256::from(200_u16) * unit));
        timepoints.insert(
            65_534,
            point(199_980, 1_000_000, U256::from(1_000_u16) * unit),
        );
        timepoints.insert(
            65_535,
            point(199_990, 1_000_000, U256::from(1_000_u16) * unit),
        );
        let config = AdaptiveFeeConfiguration {
            alpha1: 150,
            alpha2: 500,
            beta1: 720,
            beta2: 60_000,
            gamma1: 59,
            gamma2: 8_500,
            volume_beta: 0,
            volume_gamma: 10,
            base_fee: 100,
        };
        let state = FeeProjectionState {
            head_timestamp: 200_000,
            latest_timepoint_timestamp: 199_990,
            tick: 0,
            liquidity: 1_000_000_000,
            index: 65_535,
            oldest_index: 0,
            current_fees: DirectionalFees {
                zero_for_one: 117,
                one_for_zero: 117,
            },
            volume_per_liquidity_in_block: u128::try_from(U256::from(50_u8) * unit).unwrap(),
            zero_for_one_config: config,
            one_for_zero_config: config,
            timepoints,
        };
        crate::paired_benchmark::assert_absolute_latency_with_work(
            "camelot_fee_projection_benchmark",
            10_000.0,
            10_000.0,
            32,
            4_096,
            || {
                black_box(state.envelope(2)).unwrap();
            },
        );
    }
}

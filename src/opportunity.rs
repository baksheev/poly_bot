use std::{collections::HashMap, str::FromStr, time::Instant};

use alloy_primitives::{Address, U256};
use anyhow::{Context, ensure};
use rust_decimal::Decimal;

use crate::{
    dex::{
        clmm::{ClmmPool, InsufficientLiquidity, PreparedQuoteCurve},
        mirror::DexMirror,
    },
    domain::config::{DomainSnapshot, PairConfig},
    state::TopOfBook,
};

const BPS_DENOMINATOR: u64 = 10_000;
const PROFIT_BPS_SCALE: u64 = 1_000_000;
const BASELINE_CACHE_ENTRIES_PER_DIRECTION: usize = 8;

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

    const fn cache_index(self) -> usize {
        match self {
            Self::BuyTokenBOnDexSellOnCex => 0,
            Self::BuyTokenBOnCexSellOnDex => 1,
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
    pub execution_slippage_bps: u16,
    /// Rails-style venue spread before execution reserves and commissions,
    /// expressed in signed hundredths of one basis point.
    pub gross_profit_bps_x100: i64,
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

#[derive(Debug, Clone, Copy)]
pub struct PairEvaluation {
    pub pair_index: usize,
    pub baseline_token_b_amount: U256,
    pub dex_buy_cex_sell: DirectionEvaluation,
    pub cex_buy_dex_sell: DirectionEvaluation,
    pub baseline_cache_hits: u16,
    pub baseline_cache_misses: u16,
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
    pub min_slippage_bps: u16,
    pub max_slippage_bps: u16,
    pub slippage_profit_share_bps: u16,
    pub binance_buy_fee_bps: u16,
    pub binance_sell_fee_bps: u16,
    baseline_token_a: U256,
    token_b_step: U256,
    token_a: Address,
    token_b: Address,
    pool_indices: Vec<usize>,
}

pub struct OpportunityEngine {
    pairs: Vec<PairRuntime>,
    pair_indices_by_symbol: HashMap<String, usize>,
    pair_index_by_pool: Vec<Option<usize>>,
    pool_generations: Vec<u64>,
    prepared_pools: Vec<Option<PreparedPoolQuotes>>,
    baseline_quote_cache: Vec<PoolBaselineQuoteCache>,
}

#[derive(Debug)]
struct PreparedPoolQuotes {
    by_direction: [PreparedQuoteCurve; 2],
    token_a_exact_input: PreparedQuoteCurve,
}

#[derive(Debug, Clone, Copy)]
pub struct PreparedPoolRefresh {
    pub pool_index: usize,
    pub generation: u64,
    pub exact_output_segments: usize,
    pub exact_input_segments: usize,
    pub token_a_exact_input_segments: usize,
    pub build_time_us: u128,
    pub total_time_us: u128,
}

pub struct PreparedPoolBuildRequest {
    pool_index: usize,
    generation: u64,
    pool: ClmmPool,
    exact_output_zero_for_one: bool,
    exact_input_zero_for_one: bool,
    requested_at: Instant,
}

pub struct PreparedPoolBuildResult {
    pool_index: usize,
    generation: u64,
    prepared: PreparedPoolQuotes,
    build_time_us: u128,
    requested_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DexQuoteOutcome {
    Available(U256),
    InsufficientLiquidity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BaselineQuoteCacheEntry {
    token_b_amount: U256,
    outcome: DexQuoteOutcome,
}

#[derive(Debug)]
struct PoolBaselineQuoteCache {
    by_direction: [DirectionBaselineQuoteCache; 2],
}

#[derive(Debug)]
struct DirectionBaselineQuoteCache {
    entries: [Option<BaselineQuoteCacheEntry>; BASELINE_CACHE_ENTRIES_PER_DIRECTION],
    next_replace: usize,
}

#[derive(Debug, Default)]
struct BaselineCacheUsage {
    hits: u16,
    misses: u16,
}

struct BaselineCacheContext<'a> {
    pools: &'a mut [PoolBaselineQuoteCache],
    usage: &'a mut BaselineCacheUsage,
}

impl OpportunityEngine {
    pub fn new(snapshot: &DomainSnapshot, dex: &DexMirror) -> anyhow::Result<Self> {
        let mut pairs = Vec::new();
        let mut pair_indices_by_symbol = HashMap::new();
        for pair in snapshot
            .pairs
            .iter()
            .filter(|pair| pair.market_data_enabled)
        {
            let runtime = PairRuntime::new(pair, dex)?;
            let pair_index = pairs.len();
            ensure!(
                pair_indices_by_symbol
                    .insert(runtime.symbol.clone(), pair_index)
                    .is_none(),
                "duplicate opportunity symbol {}",
                pair.binance.symbol
            );
            pairs.push(runtime);
        }
        let mut pair_index_by_pool = vec![None; dex.pool_count()];
        let mut prepared_pools: Vec<Option<PreparedPoolQuotes>> =
            (0..dex.pool_count()).map(|_| None).collect();
        for (pair_index, pair) in pairs.iter().enumerate() {
            for &pool_index in &pair.pool_indices {
                ensure!(
                    pair_index_by_pool[pool_index].replace(pair_index).is_none(),
                    "DEX pool belongs to more than one enabled pair"
                );
                prepared_pools[pool_index] = Some(prepare_pool_quotes(pair, dex, pool_index, 1)?);
            }
        }
        Ok(Self {
            pairs,
            pair_indices_by_symbol,
            pair_index_by_pool,
            pool_generations: prepared_pools
                .iter()
                .map(|prepared| u64::from(prepared.is_some()))
                .collect(),
            prepared_pools,
            baseline_quote_cache: (0..dex.pool_count())
                .map(|_| PoolBaselineQuoteCache::default())
                .collect(),
        })
    }

    pub fn evaluate(&mut self, quote: &TopOfBook) -> anyhow::Result<Option<PairEvaluation>> {
        let Some(&pair_index) = self.pair_indices_by_symbol.get(quote.symbol.as_ref()) else {
            return Ok(None);
        };
        let pair = &self.pairs[pair_index];

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
        let mut cache_usage = BaselineCacheUsage::default();

        Ok(Some(PairEvaluation {
            pair_index,
            baseline_token_b_amount,
            dex_buy_cex_sell: evaluate_direction_with_cache(
                pair,
                &self.prepared_pools,
                quote,
                baseline_token_b_amount,
                ArbitrageDirection::BuyTokenBOnDexSellOnCex,
                true,
                BaselineCacheContext {
                    pools: &mut self.baseline_quote_cache,
                    usage: &mut cache_usage,
                },
            )?,
            cex_buy_dex_sell: evaluate_direction_with_cache(
                pair,
                &self.prepared_pools,
                quote,
                baseline_token_b_amount,
                ArbitrageDirection::BuyTokenBOnCexSellOnDex,
                false,
                BaselineCacheContext {
                    pools: &mut self.baseline_quote_cache,
                    usage: &mut cache_usage,
                },
            )?,
            baseline_cache_hits: cache_usage.hits,
            baseline_cache_misses: cache_usage.misses,
        }))
    }

    pub fn pair(&self, index: usize) -> anyhow::Result<&PairRuntime> {
        self.pairs
            .get(index)
            .context("opportunity pair index is invalid")
    }

    pub fn set_binance_fee_bps(
        &mut self,
        symbol: &str,
        buy_fee_bps: u16,
        sell_fee_bps: u16,
    ) -> anyhow::Result<()> {
        ensure!(buy_fee_bps <= 10_000, "Binance BUY fee exceeds 100%");
        ensure!(sell_fee_bps <= 10_000, "Binance SELL fee exceeds 100%");
        let pair_index = self
            .pair_indices_by_symbol
            .get(symbol)
            .copied()
            .with_context(|| format!("unknown Binance fee symbol {symbol}"))?;
        let pair = self
            .pairs
            .get_mut(pair_index)
            .context("Binance fee pair index is invalid")?;
        pair.binance_buy_fee_bps = buy_fee_bps;
        pair.binance_sell_fee_bps = sell_fee_bps;
        Ok(())
    }

    pub fn pairs(&self) -> &[PairRuntime] {
        &self.pairs
    }

    pub fn request_pool_refresh(
        &mut self,
        pool_index: usize,
        dex: &DexMirror,
    ) -> anyhow::Result<PreparedPoolBuildRequest> {
        let pair_index = self
            .pair_index_by_pool
            .get(pool_index)
            .copied()
            .flatten()
            .context("opportunity pool has no enabled pair")?;
        let generation = self
            .pool_generations
            .get(pool_index)
            .copied()
            .context("opportunity pool generation index is invalid")?
            .saturating_add(1);
        self.pool_generations[pool_index] = generation;
        self.prepared_pools[pool_index] = None;
        self.invalidate_pool(pool_index)?;
        let pair = &self.pairs[pair_index];
        let hydrated = dex.pool(pool_index)?;
        Ok(PreparedPoolBuildRequest {
            pool_index,
            generation,
            pool: hydrated.pool.clone(),
            exact_output_zero_for_one: hydrated.token0 == pair.token_a,
            exact_input_zero_for_one: hydrated.token0 == pair.token_b,
            requested_at: Instant::now(),
        })
    }

    pub fn finish_pool_refresh(
        &mut self,
        result: PreparedPoolBuildResult,
    ) -> anyhow::Result<Option<PreparedPoolRefresh>> {
        let expected_generation = self
            .pool_generations
            .get(result.pool_index)
            .copied()
            .context("prepared pool result index is invalid")?;
        if result.generation != expected_generation {
            return Ok(None);
        }
        let refresh = PreparedPoolRefresh {
            pool_index: result.pool_index,
            generation: result.generation,
            exact_output_segments: result.prepared.by_direction[0].segment_count(),
            exact_input_segments: result.prepared.by_direction[1].segment_count(),
            token_a_exact_input_segments: result.prepared.token_a_exact_input.segment_count(),
            build_time_us: result.build_time_us,
            total_time_us: result.requested_at.elapsed().as_micros(),
        };
        self.prepared_pools[result.pool_index] = Some(result.prepared);
        Ok(Some(refresh))
    }

    pub fn is_ready(&self) -> bool {
        self.pair_index_by_pool
            .iter()
            .enumerate()
            .all(|(index, pair)| pair.is_none() || self.prepared_pools[index].is_some())
    }

    pub fn pool_generation(&self, pool_index: usize) -> anyhow::Result<u64> {
        self.pool_generations
            .get(pool_index)
            .copied()
            .context("opportunity pool generation index is invalid")
    }

    pub fn pool_generations(&self) -> impl Iterator<Item = (usize, u64)> + '_ {
        self.pool_generations.iter().copied().enumerate()
    }

    pub fn invalidate_pool(&mut self, pool_index: usize) -> anyhow::Result<()> {
        let cache = self
            .baseline_quote_cache
            .get_mut(pool_index)
            .context("opportunity pool cache index is invalid")?;
        *cache = PoolBaselineQuoteCache::default();
        Ok(())
    }
}

impl PreparedPoolBuildRequest {
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub fn build(self) -> anyhow::Result<PreparedPoolBuildResult> {
        let started = Instant::now();
        let prepared = prepare_pool_quotes_from_pool(
            &self.pool,
            self.exact_output_zero_for_one,
            self.exact_input_zero_for_one,
            self.generation,
        )?;
        Ok(PreparedPoolBuildResult {
            pool_index: self.pool_index,
            generation: self.generation,
            prepared,
            build_time_us: started.elapsed().as_micros(),
            requested_at: self.requested_at,
        })
    }
}

impl Default for PoolBaselineQuoteCache {
    fn default() -> Self {
        Self {
            by_direction: std::array::from_fn(|_| DirectionBaselineQuoteCache::default()),
        }
    }
}

impl Default for DirectionBaselineQuoteCache {
    fn default() -> Self {
        Self {
            entries: [None; BASELINE_CACHE_ENTRIES_PER_DIRECTION],
            next_replace: 0,
        }
    }
}

fn prepare_pool_quotes(
    pair: &PairRuntime,
    dex: &DexMirror,
    pool_index: usize,
    generation: u64,
) -> anyhow::Result<PreparedPoolQuotes> {
    let hydrated = dex.pool(pool_index)?;
    let exact_output_zero_for_one = hydrated.token0 == pair.token_a;
    let exact_input_zero_for_one = hydrated.token0 == pair.token_b;
    prepare_pool_quotes_from_pool(
        &hydrated.pool,
        exact_output_zero_for_one,
        exact_input_zero_for_one,
        generation,
    )
}

fn prepare_pool_quotes_from_pool(
    pool: &ClmmPool,
    exact_output_zero_for_one: bool,
    exact_input_zero_for_one: bool,
    _generation: u64,
) -> anyhow::Result<PreparedPoolQuotes> {
    Ok(PreparedPoolQuotes {
        by_direction: [
            pool.prepare_exact_output_curve(exact_output_zero_for_one)?,
            pool.prepare_exact_input_curve(exact_input_zero_for_one)?,
        ],
        token_a_exact_input: pool.prepare_exact_input_curve(exact_output_zero_for_one)?,
    })
}

impl PairRuntime {
    pub fn baseline_token_a(&self) -> U256 {
        self.baseline_token_a
    }

    pub fn token_b_step(&self) -> U256 {
        self.token_b_step
    }

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
            min_slippage_bps: config.strategy.min_slippage_bps,
            max_slippage_bps: config.strategy.max_slippage_bps,
            slippage_profit_share_bps: config.strategy.slippage_profit_share_bps,
            binance_buy_fee_bps: 0,
            binance_sell_fee_bps: 0,
            baseline_token_a,
            token_b_step,
            token_a,
            token_b,
            pool_indices,
        })
    }
}

#[cfg(test)]
fn evaluate_direction(
    pair: &PairRuntime,
    dex: &DexMirror,
    quote: &TopOfBook,
    baseline_token_b: U256,
    direction: ArbitrageDirection,
) -> anyhow::Result<DirectionEvaluation> {
    let mut prepared_pools: Vec<Option<PreparedPoolQuotes>> =
        (0..dex.pool_count()).map(|_| None).collect();
    for &pool_index in &pair.pool_indices {
        prepared_pools[pool_index] = Some(prepare_pool_quotes(pair, dex, pool_index, 1)?);
    }
    evaluate_direction_impl(
        pair,
        &prepared_pools,
        quote,
        baseline_token_b,
        direction,
        false,
        None,
    )
}

fn evaluate_direction_with_cache(
    pair: &PairRuntime,
    prepared_pools: &[Option<PreparedPoolQuotes>],
    quote: &TopOfBook,
    baseline_token_b: U256,
    direction: ArbitrageDirection,
    baseline_from_dex_token_a: bool,
    cache: BaselineCacheContext<'_>,
) -> anyhow::Result<DirectionEvaluation> {
    evaluate_direction_impl(
        pair,
        prepared_pools,
        quote,
        baseline_token_b,
        direction,
        baseline_from_dex_token_a,
        Some(cache),
    )
}

fn evaluate_direction_impl(
    pair: &PairRuntime,
    prepared_pools: &[Option<PreparedPoolQuotes>],
    quote: &TopOfBook,
    baseline_token_b: U256,
    direction: ArbitrageDirection,
    baseline_from_dex_token_a: bool,
    mut cache: Option<BaselineCacheContext<'_>>,
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
    for &pool_index in &pair.pool_indices {
        let baseline_token_b = if baseline_from_dex_token_a {
            let Some(amount) = quote_token_a_exact_input_baseline(
                prepared_pools,
                pool_index,
                pair.baseline_token_a,
            )?
            else {
                continue;
            };
            round_down_to_step(amount, pair.token_b_step)
        } else {
            baseline_token_b
        };
        if baseline_token_b.is_zero() || cex_top_token_b_amount < baseline_token_b {
            continue;
        }
        let baseline_cex_token_a = cex_token_a_amount(pair, quote, direction, baseline_token_b)?;
        let dex_quote = if let Some(cache) = cache.as_mut() {
            let pool_cache = cache
                .pools
                .get_mut(pool_index)
                .context("baseline quote cache index is invalid")?;
            pool_cache.quote(direction, baseline_token_b, cache.usage, || {
                quote_dex(prepared_pools, direction, pool_index, baseline_token_b)
            })?
        } else {
            quote_dex(prepared_pools, direction, pool_index, baseline_token_b)?
        };
        let Some(mut baseline) = evaluate_trade_with_dex_quote(
            pair,
            direction,
            pool_index,
            baseline_token_b,
            baseline_cex_token_a,
            dex_quote,
        )?
        else {
            continue;
        };
        finalize_trade_profit(&mut baseline)?;
        if best_baseline
            .as_ref()
            .is_none_or(|best| trade_is_better(&baseline, best))
        {
            best_baseline = Some(baseline);
        }
        if !baseline.meets_threshold {
            continue;
        }

        let capacity = size_pool(
            pair,
            prepared_pools,
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

    if let Some(capacity) = best_capacity.as_mut() {
        finalize_trade_profit(&mut capacity.trade)?;
    }

    Ok(DirectionEvaluation {
        direction,
        cex_top_token_b_amount,
        baseline: best_baseline,
        market_liquidity_capacity: best_capacity,
    })
}

impl PoolBaselineQuoteCache {
    fn quote(
        &mut self,
        direction: ArbitrageDirection,
        token_b_amount: U256,
        usage: &mut BaselineCacheUsage,
        load: impl FnOnce() -> anyhow::Result<DexQuoteOutcome>,
    ) -> anyhow::Result<DexQuoteOutcome> {
        let direction_cache = &mut self.by_direction[direction.cache_index()];
        if let Some(entry) = direction_cache
            .entries
            .iter()
            .flatten()
            .find(|entry| entry.token_b_amount == token_b_amount)
        {
            usage.hits = usage.hits.saturating_add(1);
            return Ok(entry.outcome);
        }

        let outcome = load()?;
        direction_cache.entries[direction_cache.next_replace] = Some(BaselineQuoteCacheEntry {
            token_b_amount,
            outcome,
        });
        direction_cache.next_replace =
            (direction_cache.next_replace + 1) % BASELINE_CACHE_ENTRIES_PER_DIRECTION;
        usage.misses = usage.misses.saturating_add(1);
        Ok(outcome)
    }
}

#[allow(clippy::too_many_arguments)]
fn size_pool(
    pair: &PairRuntime,
    prepared_pools: &[Option<PreparedPoolQuotes>],
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
    if let Some(at_max) = evaluate_trade(
        pair,
        prepared_pools,
        quote,
        direction,
        pool_index,
        max_amount,
    )? && at_max.meets_threshold
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
        match evaluate_trade(pair, prepared_pools, quote, direction, pool_index, amount)? {
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
    let limiter = match evaluate_trade(
        pair,
        prepared_pools,
        quote,
        direction,
        pool_index,
        next_amount,
    )? {
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
    prepared_pools: &[Option<PreparedPoolQuotes>],
    quote: &TopOfBook,
    direction: ArbitrageDirection,
    pool_index: usize,
    token_b_amount: U256,
) -> anyhow::Result<Option<TradeEvaluation>> {
    let dex_quote = quote_dex(prepared_pools, direction, pool_index, token_b_amount)?;
    let cex_token_a_amount = cex_token_a_amount(pair, quote, direction, token_b_amount)?;
    evaluate_trade_with_dex_quote(
        pair,
        direction,
        pool_index,
        token_b_amount,
        cex_token_a_amount,
        dex_quote,
    )
}

fn quote_dex(
    prepared_pools: &[Option<PreparedPoolQuotes>],
    direction: ArbitrageDirection,
    pool_index: usize,
    token_b_amount: U256,
) -> anyhow::Result<DexQuoteOutcome> {
    let prepared = prepared_pools
        .get(pool_index)
        .and_then(Option::as_ref)
        .context("prepared DEX pool is unavailable")?;
    let result = prepared.by_direction[direction.cache_index()].quote(token_b_amount);
    match result {
        Ok(value) => Ok(DexQuoteOutcome::Available(value)),
        Err(error) if error.downcast_ref::<InsufficientLiquidity>().is_some() => {
            Ok(DexQuoteOutcome::InsufficientLiquidity)
        }
        Err(error) => Err(error),
    }
}

fn quote_token_a_exact_input_baseline(
    prepared_pools: &[Option<PreparedPoolQuotes>],
    pool_index: usize,
    token_a_amount: U256,
) -> anyhow::Result<Option<U256>> {
    let prepared = prepared_pools
        .get(pool_index)
        .and_then(Option::as_ref)
        .context("prepared DEX pool is unavailable")?;
    match prepared.token_a_exact_input.quote(token_a_amount) {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.downcast_ref::<InsufficientLiquidity>().is_some() => Ok(None),
        Err(error) => Err(error),
    }
}

fn evaluate_trade_with_dex_quote(
    pair: &PairRuntime,
    direction: ArbitrageDirection,
    pool_index: usize,
    token_b_amount: U256,
    cex_token_a_amount: U256,
    dex_quote: DexQuoteOutcome,
) -> anyhow::Result<Option<TradeEvaluation>> {
    let DexQuoteOutcome::Available(dex_token_a_amount) = dex_quote else {
        return Ok(None);
    };

    // Rails derives the per-order slippage budget from the gross venue spread,
    // before commissions and execution reserves. Preserve that input exactly,
    // then apply the account-specific Binance fee and the DEX reserve to the
    // executable economics below.
    let (gross_cost_token_a, gross_proceeds_token_a) = match direction {
        ArbitrageDirection::BuyTokenBOnDexSellOnCex => (dex_token_a_amount, cex_token_a_amount),
        ArbitrageDirection::BuyTokenBOnCexSellOnDex => (cex_token_a_amount, dex_token_a_amount),
    };
    let gross_profit_bps_x100 = signed_profit_bps_x100(gross_proceeds_token_a, gross_cost_token_a)?;
    let gross_profit_bps =
        u16::try_from((gross_profit_bps_x100.max(0) / 100).min(i64::from(u16::MAX)))
            .context("gross opportunity profit bps exceed u16")?;
    let execution_slippage_bps = slippage_bps(pair, gross_profit_bps)?;

    let (cex_adjusted_cost_token_a, cex_adjusted_proceeds_token_a) = match direction {
        ArbitrageDirection::BuyTokenBOnDexSellOnCex => {
            let net_cex_proceeds =
                subtract_bps_floor(cex_token_a_amount, pair.binance_sell_fee_bps)?;
            (dex_token_a_amount, net_cex_proceeds)
        }
        ArbitrageDirection::BuyTokenBOnCexSellOnDex => {
            let gross_cex_cost = add_bps_ceil(cex_token_a_amount, pair.binance_buy_fee_bps)?;
            (gross_cex_cost, dex_token_a_amount)
        }
    };
    let total_dex_reserve_bps = pair
        .dex_fee_reserve_bps
        .checked_add(execution_slippage_bps)
        .context("DEX execution reserve bps overflow")?;
    ensure!(
        total_dex_reserve_bps <= 10_000,
        "DEX execution reserve exceeds 100%"
    );
    let (cost_token_a, proceeds_token_a) = match direction {
        ArbitrageDirection::BuyTokenBOnDexSellOnCex => (
            add_bps_ceil(dex_token_a_amount, total_dex_reserve_bps)?,
            cex_adjusted_proceeds_token_a,
        ),
        ArbitrageDirection::BuyTokenBOnCexSellOnDex => (
            cex_adjusted_cost_token_a,
            subtract_bps_floor(dex_token_a_amount, total_dex_reserve_bps)?,
        ),
    };

    Ok(Some(TradeEvaluation {
        pool_index,
        token_b_amount,
        dex_token_a_amount,
        cex_token_a_amount,
        cost_token_a,
        proceeds_token_a,
        execution_slippage_bps,
        gross_profit_bps_x100,
        profit_bps_x100: 0,
        meets_threshold: meets_threshold(
            gross_proceeds_token_a,
            gross_cost_token_a,
            pair.opportunity_threshold_bps,
        )?,
    }))
}

fn slippage_bps(pair: &PairRuntime, profit_bps: u16) -> anyhow::Result<u16> {
    let allocated = u32::from(profit_bps)
        .checked_mul(u32::from(pair.slippage_profit_share_bps))
        .context("slippage profit-share multiplication overflow")?
        / 10_000;
    let allocated: u16 = allocated
        .try_into()
        .context("allocated slippage exceeds u16")?;
    Ok(allocated
        .max(pair.min_slippage_bps)
        .min(pair.max_slippage_bps))
}

fn trade_is_better(candidate: &TradeEvaluation, current: &TradeEvaluation) -> bool {
    candidate.profit_bps_x100 > current.profit_bps_x100
        || (candidate.profit_bps_x100 == current.profit_bps_x100
            && candidate.token_b_amount > current.token_b_amount)
}

fn finalize_trade_profit(trade: &mut TradeEvaluation) -> anyhow::Result<()> {
    trade.profit_bps_x100 = signed_profit_bps_x100(trade.proceeds_token_a, trade.cost_token_a)?;
    Ok(())
}

fn cex_token_a_amount(
    pair: &PairRuntime,
    quote: &TopOfBook,
    direction: ArbitrageDirection,
    token_b_amount: U256,
) -> anyhow::Result<U256> {
    match direction {
        ArbitrageDirection::BuyTokenBOnDexSellOnCex => token_b_to_token_a(
            token_b_amount,
            quote.bid_price,
            pair.token_a_decimals,
            pair.token_b_decimals,
            false,
        ),
        ArbitrageDirection::BuyTokenBOnCexSellOnDex => token_b_to_token_a(
            token_b_amount,
            quote.ask_price,
            pair.token_a_decimals,
            pair.token_b_decimals,
            true,
        ),
    }
}

fn decimal_to_base_units(value: Decimal, decimals: u8) -> anyhow::Result<U256> {
    ensure!(
        value >= Decimal::ZERO,
        "decimal amount must not be negative"
    );
    let mantissa = value.mantissa();
    ensure!(mantissa >= 0, "decimal mantissa must not be negative");
    if let (Some(numerator_scale), Some(denominator)) =
        (pow10_u128(decimals.into()), pow10_u128(value.scale()))
        && let Some(numerator) = (mantissa as u128).checked_mul(numerator_scale)
    {
        return Ok(U256::from(numerator / denominator));
    }
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
    if let Ok(token_a_amount) = u128::try_from(token_a_amount) {
        let fast = if exponent >= 0 {
            pow10_u128(exponent as u32)
                .and_then(|scale| token_a_amount.checked_mul(scale))
                .map(|numerator| numerator / mantissa)
        } else {
            pow10_u128((-exponent) as u32)
                .and_then(|scale| mantissa.checked_mul(scale))
                .map(|denominator| token_a_amount / denominator)
        };
        if let Some(result) = fast {
            return Ok(U256::from(result));
        }
    }
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
    let exponent = i32::from(token_b_decimals) + price.scale() as i32 - i32::from(token_a_decimals);
    if let Ok(token_b_amount) = u128::try_from(token_b_amount)
        && let Some(product) = token_b_amount.checked_mul(mantissa)
    {
        let fast = if exponent <= 0 {
            pow10_u128((-exponent) as u32).and_then(|scale| product.checked_mul(scale))
        } else {
            pow10_u128(exponent as u32).and_then(|denominator| {
                let quotient = product / denominator;
                let remainder = product % denominator;
                if round_up && remainder != 0 {
                    quotient.checked_add(1)
                } else {
                    Some(quotient)
                }
            })
        };
        if let Some(result) = fast {
            return Ok(U256::from(result));
        }
    }
    let product = token_b_amount
        .checked_mul(U256::from(mantissa))
        .context("token-B price product overflow")?;
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

fn pow10_u128(exponent: u32) -> Option<u128> {
    10_u128.checked_pow(exponent)
}

fn round_down_to_step(amount: U256, step: U256) -> U256 {
    (amount / step) * step
}

fn add_bps_ceil(amount: U256, bps: u16) -> anyhow::Result<U256> {
    if let Ok(amount) = u128::try_from(amount)
        && let Some(numerator) = amount.checked_mul(u128::from(BPS_DENOMINATOR + u64::from(bps)))
    {
        let denominator = u128::from(BPS_DENOMINATOR);
        let quotient = numerator / denominator;
        let rounded = if numerator % denominator == 0 {
            Some(quotient)
        } else {
            quotient.checked_add(1)
        };
        if let Some(rounded) = rounded {
            return Ok(U256::from(rounded));
        }
    }
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
    if let Ok(amount) = u128::try_from(amount)
        && let Some(numerator) = amount.checked_mul(u128::from(BPS_DENOMINATOR - u64::from(bps)))
    {
        return Ok(U256::from(numerator / u128::from(BPS_DENOMINATOR)));
    }
    amount
        .checked_mul(U256::from(BPS_DENOMINATOR - u64::from(bps)))
        .context("proceeds reserve overflow")
        .map(|value| value / U256::from(BPS_DENOMINATOR))
}

fn meets_threshold(proceeds: U256, cost: U256, threshold_bps: u16) -> anyhow::Result<bool> {
    if let (Ok(proceeds), Ok(cost)) = (u128::try_from(proceeds), u128::try_from(cost))
        && let (Some(left), Some(right)) = (
            proceeds.checked_mul(u128::from(BPS_DENOMINATOR)),
            cost.checked_mul(u128::from(BPS_DENOMINATOR + u64::from(threshold_bps))),
        )
    {
        return Ok(left >= right);
    }
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
    if let (Ok(proceeds), Ok(cost)) = (u128::try_from(proceeds), u128::try_from(cost)) {
        let (positive, delta) = if proceeds >= cost {
            (true, proceeds - cost)
        } else {
            (false, cost - proceeds)
        };
        if let Some(scaled) = delta.checked_mul(u128::from(PROFIT_BPS_SCALE)) {
            let magnitude = i64::try_from(scaled / cost).unwrap_or(i64::MAX);
            return Ok(if positive { magnitude } else { -magnitude });
        }
    }
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
        ArbitrageDirection, BASELINE_CACHE_ENTRIES_PER_DIRECTION, BaselineCacheUsage,
        CapacityLimiter, DexQuoteOutcome, OpportunityEngine, PairRuntime, PoolBaselineQuoteCache,
        TradeEvaluation, add_bps_ceil, decimal_to_base_units, evaluate_direction,
        evaluate_trade_with_dex_quote, finalize_trade_profit, format_base_units, meets_threshold,
        signed_profit_bps_x100, subtract_bps_floor, token_a_to_token_b_floor, token_b_to_token_a,
        trade_is_better,
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
            min_slippage_bps: 5,
            max_slippage_bps: 50,
            slippage_profit_share_bps: 5_000,
            binance_buy_fee_bps: 0,
            binance_sell_fee_bps: 0,
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
    fn baseline_quote_cache_is_bounded_and_caches_insufficient_liquidity() {
        let mut cache = PoolBaselineQuoteCache::default();
        let mut usage = BaselineCacheUsage::default();
        let direction = ArbitrageDirection::BuyTokenBOnDexSellOnCex;
        let first_amount = U256::from(48_u8);

        assert_eq!(
            cache
                .quote(direction, first_amount, &mut usage, || {
                    Ok(DexQuoteOutcome::InsufficientLiquidity)
                })
                .unwrap(),
            DexQuoteOutcome::InsufficientLiquidity
        );
        assert_eq!(
            cache
                .quote(direction, first_amount, &mut usage, || {
                    panic!("a matching cached quote must not be recomputed")
                })
                .unwrap(),
            DexQuoteOutcome::InsufficientLiquidity
        );

        let second_amount = U256::from(49_u8);
        assert_eq!(
            cache
                .quote(direction, second_amount, &mut usage, || {
                    Ok(DexQuoteOutcome::Available(U256::from(20_u8)))
                })
                .unwrap(),
            DexQuoteOutcome::Available(U256::from(20_u8))
        );
        assert_eq!(usage.hits, 1);
        assert_eq!(usage.misses, 2);
        assert!(
            cache.by_direction[direction.cache_index()]
                .entries
                .iter()
                .flatten()
                .any(|entry| entry.token_b_amount == first_amount)
        );
        assert!(
            cache.by_direction[direction.cache_index()]
                .entries
                .iter()
                .flatten()
                .any(|entry| entry.token_b_amount == second_amount)
        );

        for amount in 50_u8..58 {
            cache
                .quote(direction, U256::from(amount), &mut usage, || {
                    Ok(DexQuoteOutcome::InsufficientLiquidity)
                })
                .unwrap();
        }
        assert_eq!(
            cache.by_direction[direction.cache_index()]
                .entries
                .iter()
                .flatten()
                .count(),
            BASELINE_CACHE_ENTRIES_PER_DIRECTION
        );
        assert!(
            cache.by_direction[direction.cache_index()]
                .entries
                .iter()
                .flatten()
                .all(|entry| entry.token_b_amount != first_amount)
        );
    }

    #[test]
    fn invalidating_one_pool_clears_both_direction_slots() {
        let entry = super::BaselineQuoteCacheEntry {
            token_b_amount: U256::from(48_u8),
            outcome: DexQuoteOutcome::Available(U256::from(20_u8)),
        };
        let populated_direction = super::DirectionBaselineQuoteCache {
            entries: std::array::from_fn(|index| (index == 0).then_some(entry)),
            next_replace: 1,
        };
        let mut engine = OpportunityEngine {
            pairs: Vec::new(),
            pair_indices_by_symbol: std::collections::HashMap::new(),
            pair_index_by_pool: vec![None],
            pool_generations: vec![0],
            prepared_pools: vec![None],
            baseline_quote_cache: vec![PoolBaselineQuoteCache {
                by_direction: [
                    populated_direction,
                    super::DirectionBaselineQuoteCache {
                        entries: std::array::from_fn(|index| (index == 0).then_some(entry)),
                        next_replace: 1,
                    },
                ],
            }],
        };

        engine.invalidate_pool(0).unwrap();
        assert!(
            engine.baseline_quote_cache[0]
                .by_direction
                .iter()
                .all(|direction| direction.entries.iter().all(Option::is_none))
        );
        assert!(engine.invalidate_pool(1).is_err());
    }

    #[test]
    fn prepared_pool_refresh_is_fail_closed_and_discards_superseded_generation() {
        let (pair, mirror) = fixture();
        let initial = super::prepare_pool_quotes(&pair, &mirror, 0, 1).unwrap();
        let mut engine = OpportunityEngine {
            pairs: vec![pair],
            pair_indices_by_symbol: std::collections::HashMap::from([("BA".into(), 0)]),
            pair_index_by_pool: vec![Some(0)],
            pool_generations: vec![1],
            prepared_pools: vec![Some(initial)],
            baseline_quote_cache: vec![PoolBaselineQuoteCache::default()],
        };
        assert!(engine.is_ready());

        let superseded = engine.request_pool_refresh(0, &mirror).unwrap();
        assert!(!engine.is_ready());
        let current = engine.request_pool_refresh(0, &mirror).unwrap();
        assert!(!engine.is_ready());

        assert!(
            engine
                .finish_pool_refresh(superseded.build().unwrap())
                .unwrap()
                .is_none()
        );
        assert!(!engine.is_ready());
        assert!(
            engine
                .finish_pool_refresh(current.build().unwrap())
                .unwrap()
                .is_some()
        );
        assert!(engine.is_ready());
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
    fn account_commission_is_applied_conservatively_in_both_directions() {
        let (mut pair, _) = fixture();
        pair.binance_sell_fee_bps = 10;
        let dex_buy = evaluate_trade_with_dex_quote(
            &pair,
            ArbitrageDirection::BuyTokenBOnDexSellOnCex,
            0,
            U256::from(100_u8),
            U256::from(1_000_u16),
            DexQuoteOutcome::Available(U256::from(900_u16)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(dex_buy.proceeds_token_a, U256::from(999_u16));

        pair.binance_buy_fee_bps = 10;
        let cex_buy = evaluate_trade_with_dex_quote(
            &pair,
            ArbitrageDirection::BuyTokenBOnCexSellOnDex,
            0,
            U256::from(100_u8),
            U256::from(1_000_u16),
            DexQuoteOutcome::Available(U256::from(1_100_u16)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(cex_buy.cost_token_a, U256::from(1_001_u16));
    }

    #[test]
    fn execution_slippage_matches_rails_gross_profit_share_and_bounds() {
        let (pair, _) = fixture();
        for (cex_proceeds, expected_slippage) in [(10_003_u16, 5_u16), (10_030, 15), (10_200, 50)] {
            let trade = evaluate_trade_with_dex_quote(
                &pair,
                ArbitrageDirection::BuyTokenBOnDexSellOnCex,
                0,
                U256::from(100_u8),
                U256::from(cex_proceeds),
                DexQuoteOutcome::Available(U256::from(10_000_u16)),
            )
            .unwrap()
            .unwrap();
            assert_eq!(trade.execution_slippage_bps, expected_slippage);
        }
    }

    #[test]
    fn slippage_is_derived_before_binance_commission_like_rails() {
        let (mut pair, _) = fixture();
        pair.binance_sell_fee_bps = 20;
        let mut trade = evaluate_trade_with_dex_quote(
            &pair,
            ArbitrageDirection::BuyTokenBOnDexSellOnCex,
            0,
            U256::from(100_u8),
            U256::from(10_030_u16),
            DexQuoteOutcome::Available(U256::from(10_000_u16)),
        )
        .unwrap()
        .unwrap();

        assert_eq!(trade.execution_slippage_bps, 15);
        assert_eq!(trade.gross_profit_bps_x100, 3_000);
        assert_eq!(trade.proceeds_token_a, U256::from(10_009_u16));
        assert!(trade.meets_threshold);
        finalize_trade_profit(&mut trade).unwrap();
        assert!(trade.profit_bps_x100 < i64::from(pair.opportunity_threshold_bps) * 100);
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
    fn dex_buy_baseline_uses_exact_input_dex_quote_like_rails() {
        let (pair, dex) = fixture();
        let prepared = super::prepare_pool_quotes(&pair, &dex, 0, 1).unwrap();
        let quote = quote("2.10", "100", "2.20", "100");
        let binance_ask_baseline = U256::from(9_u8) * pair.token_b_step;

        let result = super::evaluate_direction_impl(
            &pair,
            &[Some(prepared)],
            &quote,
            binance_ask_baseline,
            ArbitrageDirection::BuyTokenBOnDexSellOnCex,
            true,
            None,
        )
        .unwrap();

        assert_eq!(
            result.baseline.unwrap().token_b_amount,
            U256::from(19_u8) * pair.token_b_step
        );
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

    #[test]
    fn threshold_boundary_is_inclusive_without_floating_point() {
        let cost = U256::from(1_000_000_u64);
        assert!(meets_threshold(U256::from(1_002_000_u64), cost, 20).unwrap());
        assert!(!meets_threshold(U256::from(1_001_999_u64), cost, 20).unwrap());
    }

    #[test]
    fn reserve_rounding_is_conservative_in_both_directions() {
        assert_eq!(add_bps_ceil(U256::from(1_u8), 4).unwrap(), U256::from(2_u8));
        assert_eq!(subtract_bps_floor(U256::from(1_u8), 4).unwrap(), U256::ZERO);
        assert_eq!(
            add_bps_ceil(U256::from(10_000_u64), 4).unwrap(),
            U256::from(10_004_u64)
        );
        assert_eq!(
            subtract_bps_floor(U256::from(10_000_u64), 4).unwrap(),
            U256::from(9_996_u64)
        );
    }

    #[test]
    fn signed_profit_reports_profit_loss_and_rejects_zero_cost() {
        assert_eq!(
            signed_profit_bps_x100(U256::from(101_u8), U256::from(100_u8)).unwrap(),
            10_000
        );
        assert_eq!(
            signed_profit_bps_x100(U256::from(99_u8), U256::from(100_u8)).unwrap(),
            -10_000
        );
        assert_eq!(
            signed_profit_bps_x100(U256::from(100_u8), U256::from(100_u8)).unwrap(),
            0
        );
        assert!(signed_profit_bps_x100(U256::ONE, U256::ZERO).is_err());
    }

    #[test]
    fn provider_selection_prefers_direction_specific_economic_result() {
        let mut candidate = TradeEvaluation {
            pool_index: 1,
            token_b_amount: U256::from(10_u8),
            dex_token_a_amount: U256::from(90_u8),
            cex_token_a_amount: U256::from(100_u8),
            cost_token_a: U256::from(90_u8),
            proceeds_token_a: U256::from(110_u8),
            execution_slippage_bps: 5,
            gross_profit_bps_x100: 0,
            profit_bps_x100: 0,
            meets_threshold: true,
        };
        let mut current = TradeEvaluation {
            pool_index: 0,
            cost_token_a: U256::from(91_u8),
            proceeds_token_a: U256::from(109_u8),
            ..candidate
        };
        finalize_trade_profit(&mut candidate).unwrap();
        finalize_trade_profit(&mut current).unwrap();

        assert!(trade_is_better(&candidate, &current));
    }

    #[test]
    fn provider_selection_compares_rate_when_dex_buy_baseline_amounts_differ() {
        let mut low_liquidity = TradeEvaluation {
            pool_index: 3,
            token_b_amount: U256::from(500_000_000_000_000_000_u128),
            dex_token_a_amount: U256::from(1_433_245_u64),
            cex_token_a_amount: U256::from(191_750_u64),
            cost_token_a: U256::from(1_434_535_u64),
            proceeds_token_a: U256::from(191_558_u64),
            execution_slippage_bps: 5,
            gross_profit_bps_x100: 0,
            profit_bps_x100: 0,
            meets_threshold: false,
        };
        let mut healthy_liquidity = TradeEvaluation {
            pool_index: 0,
            token_b_amount: U256::from(52_100_000_000_000_000_000_u128),
            dex_token_a_amount: U256::from(20_050_000_u64),
            cex_token_a_amount: U256::from(19_995_980_u64),
            cost_token_a: U256::from(20_068_045_u64),
            proceeds_token_a: U256::from(19_975_984_u64),
            execution_slippage_bps: 5,
            gross_profit_bps_x100: 0,
            profit_bps_x100: 0,
            meets_threshold: false,
        };
        finalize_trade_profit(&mut low_liquidity).unwrap();
        finalize_trade_profit(&mut healthy_liquidity).unwrap();

        assert!(trade_is_better(&healthy_liquidity, &low_liquidity));
    }

    #[test]
    fn base_unit_formatting_handles_zero_and_leading_fractional_zeroes() {
        assert_eq!(format_base_units(U256::ZERO, 6), "0");
        assert_eq!(format_base_units(U256::from(1_u8), 6), "0.000001");
        assert_eq!(format_base_units(U256::from(1_230_000_u64), 6), "1.23");
        assert_eq!(format_base_units(U256::from(123_u8), 0), "123");
    }
}

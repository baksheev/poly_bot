use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use alloy_primitives::U256;
use anyhow::{Context, ensure};
use rust_decimal::Decimal;
use serde_json::{Value, json};

use crate::{
    admission::{AdmissionInputs, evaluate_execution_admission},
    arbitrage::{
        AdmissionRiskBounds, ArbitrageDirection as TradeDirection, EntryPreflightHandle,
        PaperOpportunity, PaperTradeEvent, PaperTradeEventState, PaperTradeHandle,
        PaperTradeSubmitResult,
    },
    balances::{BalanceEvent, BalanceSource},
    binance::{
        account::BinanceClockSync,
        depth::SpotDepthBook,
        user_data::{ExecutionReportEvent, UserDataEvent},
    },
    config::AppConfig,
    dex::{
        events::{PoolLocator, PoolUpdate, decode_pool_event, decode_pool_event_for_locator},
        mirror::{DexMirror, LogApplyResult},
    },
    domain::config::{AdaptiveSizingConfig, DexProvider, LoadedDomainConfig},
    execution_plan::{DEX_PLAN_TTL_SECONDS, DexSwapPlan},
    hot_telemetry::{
        HotTelemetryHandle, HotTelemetryTask, SharedStreamEventKind,
        channel as hot_telemetry_channel,
    },
    inventory::{
        InsufficientAvailableInventory, InventoryClaim, InventoryKey, InventoryLocation,
        ReservationPurpose, ReservationRequest, SharedInventoryReservations,
    },
    market_data::{MarketEvent, alchemy::DexStreamEvent},
    opportunity::{
        ArbitrageDirection, OpportunityEngine, PairEvaluation, PreparedPoolBuildRequest,
        PreparedPoolBuildResult, TradeEvaluation,
    },
    portfolio::{AllocationIntent, AllocationProposal, CapitalAllocatorHandle, PortfolioCatalog},
    rebalance::{
        Direction, RebalanceEvaluation, RebalanceExecutionOperation, RebalanceExecutionProgress,
        V12RebalanceParityAdapter,
    },
    state::{QuoteApplyResult, RuntimePhase, RuntimeState, TopOfBook},
    strategy_runtime::{StrategyEvaluation, StrategyEvaluator, measure_strategy_evaluation},
    telemetry::{
        PRIMARY_BINANCE_ACCOUNT_ID, PRIMARY_EVM_WALLET_ID, TelemetryHandle, execution_lane_id,
        instrument_id, network_id, strategy_id, wallet_location_id,
    },
};

impl StrategyEvaluator for TradingEngine {
    fn strategy_id(&self) -> crate::domain::compiled::StrategyId {
        let pair = self
            .opportunities
            .pairs()
            .first()
            .expect("validated trading engine must contain one compatibility pair");
        crate::domain::compiled::StrategyId::new(strategy_id(&pair.pair_id))
            .expect("telemetry strategy id must be a valid compiled id")
    }

    fn symbol(&self) -> &str {
        &self
            .opportunities
            .pairs()
            .first()
            .expect("validated trading engine must contain one compatibility pair")
            .symbol
    }

    fn on_market_event(
        &mut self,
        event: MarketEvent,
        depth: Option<&SpotDepthBook>,
    ) -> anyhow::Result<StrategyEvaluation> {
        let evaluated = matches!(event, MarketEvent::BinanceTopOfBook(_));
        measure_strategy_evaluation(200, || {
            TradingEngine::on_market_event(self, event, depth)?;
            Ok((evaluated, false))
        })
    }
}

pub struct TradingEngine {
    config: AppConfig,
    domain_config: Arc<LoadedDomainConfig>,
    state: RuntimeState,
    dex: DexMirror,
    opportunities: OpportunityEngine,
    rebalance: V12RebalanceParityAdapter,
    telemetry: TelemetryHandle,
    hot_telemetry: HotTelemetryHandle,
    paper_trades: Option<PaperTradeHandle>,
    inventory: SharedInventoryReservations,
    portfolio_catalog: Arc<PortfolioCatalog>,
    capital_allocator: CapitalAllocatorHandle,
    binance_asset_decimals: BTreeMap<String, u8>,
    binance_inventory_generation: u64,
    binance_user_data_connected: bool,
    binance_user_data_clean: bool,
    binance_orders: BTreeMap<String, ExecutionReportEvent>,
    last_sequence_matched_quote_update: BTreeMap<String, u64>,
    latest_sequence_matched_depth: BTreeMap<String, SpotDepthBook>,
    depth_health_by_symbol: BTreeMap<String, DepthHealthObservation>,
    strategy_price_transport_silence_limits_ms: BTreeMap<String, u64>,
    gas_price_symbol: String,
    gas_price_connected: bool,
    gas_price_generation: u64,
    gas_price_book: Option<TopOfBook>,
    commission_price_symbol: String,
    binance_clock_sync: Option<BinanceClockSync>,
    rebalance_inventory_reservations: BTreeMap<String, String>,
    next_inventory_reservation: u64,
    pending_rebalance: Option<RebalanceEvaluation>,
    rebalance_pending_since: Option<Instant>,
    rebalance_inflight: bool,
    rebalance_inflight_since: Option<Instant>,
    rebalance_blocked_tokens: BTreeSet<String>,
    rebalance_deferred_reason: Option<String>,
    rebalance_settlement: Option<RebalanceSettlementBarrier>,
    last_rebalance_health_log_at: Option<Instant>,
    last_depth_health_log_at: Option<Instant>,
    last_binance_price_health_log_at: Option<Instant>,
    last_inventory_blocked_alert_at: Option<Instant>,
    last_inventory_contention_log_at: Option<Instant>,
    entry_preflight: EntryPreflightHandle,
    arbitrage_plan_freshness: BTreeMap<String, ArbitragePlanFreshness>,
    terminal_child_observed_at: BTreeMap<String, Instant>,
    pending_adaptive_sizing: Vec<AdaptiveSizingJob>,
}

pub struct TradingExecutionHandles {
    pub paper_trades: Option<PaperTradeHandle>,
    pub entry_preflight: EntryPreflightHandle,
    pub binance_asset_decimals: BTreeMap<String, u8>,
    pub portfolio_catalog: Arc<PortfolioCatalog>,
    pub inventory: SharedInventoryReservations,
    pub capital_allocator: CapitalAllocatorHandle,
    pub pretrade_cost_telemetry: crate::pretrade_cost::PreTradeCostTelemetry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BinanceFeeBps {
    pub buy: u16,
    pub sell: u16,
}

const REBALANCE_HEALTH_LOG_INTERVAL: Duration = Duration::from_secs(60);
const REBALANCE_PENDING_TIMEOUT: Duration = Duration::from_secs(60);
const DEPTH_HEALTH_LOG_INTERVAL: Duration = Duration::from_secs(60);
const BINANCE_PRICE_HEALTH_LOG_INTERVAL: Duration = Duration::from_secs(60);
const BINANCE_JSON_TIME_RESOLUTION_US: u64 = 1_000;
const BINANCE_CLOCK_SYNC_MAX_AGE_MS: u64 = 180_000;
const TRADING_INVENTORY_ALERT_LOG_INTERVAL: Duration = Duration::from_secs(60);
const MINIMUM_REBALANCE_SETTLEMENT_TIMEOUT: Duration = Duration::from_secs(60);
const ADAPTIVE_OPTIMIZER_VERSION: &str = "maximum_slippage_slot_v2";
const MAX_ADAPTIVE_EXACT_EVALUATIONS: u16 = 128;
const STRATEGY_BASELINE_CALCULATION_BUDGET_US: u64 = 200;

#[derive(Debug)]
struct RebalanceSettlementBarrier {
    operation_id: String,
    strategy_id: String,
    token_symbol: String,
    direction: Direction,
    binance_after: Instant,
    wallet_after: Instant,
    settlement_locations: [InventoryLocation; 2],
    started_at: Instant,
}

#[derive(Clone, Copy)]
enum AdmissionLiquidity<'a> {
    DexFirstTop,
    FullDepth(&'a SpotDepthBook),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdaptiveDepthSource {
    SequenceMatchedFullDepth,
    RecentFullDepth,
    TopOfBookOnly,
}

impl AdaptiveDepthSource {
    const fn as_str(self) -> &'static str {
        match self {
            Self::SequenceMatchedFullDepth => "sequence_matched_full_depth",
            Self::RecentFullDepth => "recent_full_depth",
            Self::TopOfBookOnly => "top_of_book_only",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DepthObservation {
    age_ms: Option<u64>,
    update_delta: Option<u64>,
    top_matches: bool,
    top_mismatch_reason: Option<&'static str>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DepthHealthObservation {
    source: AdaptiveDepthSource,
    source_reason: &'static str,
    age_ms: Option<u64>,
    update_delta: Option<u64>,
    top_matches: bool,
    top_mismatch_reason: Option<&'static str>,
}

#[derive(Debug, Clone, Copy)]
struct AdaptiveSizingRuntimeLimits {
    max_trade_notional: U256,
    recent_full_depth_max_age_ms: u64,
    recent_full_depth_max_update_delta: u64,
}

impl AdaptiveSizingRuntimeLimits {
    fn parse(config: &AdaptiveSizingConfig) -> anyhow::Result<Option<Self>> {
        let Some(limits) = config.limits() else {
            return Ok(None);
        };
        let parse = |value: &str, name: &str| {
            U256::from_str_radix(value, 10)
                .with_context(|| format!("validated adaptive sizing {name} is invalid"))
        };
        Ok(Some(Self {
            max_trade_notional: parse(limits.max_trade_notional, "trade cap")?,
            recent_full_depth_max_age_ms: limits.depth_policy.recent_full_depth_max_age_ms,
            recent_full_depth_max_update_delta: limits
                .depth_policy
                .recent_full_depth_max_update_delta,
        }))
    }
}

#[derive(Debug, Clone, Copy)]
struct AdaptiveCandidate {
    direction: ArbitrageDirection,
    trade: TradeEvaluation,
    trade_notional: U256,
}

#[derive(Debug, Clone, Copy)]
struct AdaptiveProbe {
    candidate: Option<AdaptiveCandidate>,
    rejection: Option<&'static str>,
}

#[derive(Debug)]
struct AdaptivePoolSearch {
    cached_probes: Vec<(U256, AdaptiveProbe)>,
    rejection_counts: BTreeMap<&'static str, u32>,
    exact_evaluations: u16,
    limit_exhausted: bool,
    max_trade_notional: U256,
}

impl AdaptivePoolSearch {
    fn new(max_trade_notional: U256) -> Self {
        Self {
            cached_probes: Vec::with_capacity(32),
            rejection_counts: BTreeMap::new(),
            exact_evaluations: 0,
            limit_exhausted: false,
            max_trade_notional,
        }
    }

    fn record(&mut self, amount: U256, probe: AdaptiveProbe) {
        if let Some(reason) = probe.rejection {
            *self.rejection_counts.entry(reason).or_default() += 1;
        }
        self.cached_probes.push((amount, probe));
    }
}

#[derive(Debug)]
enum OwnedAdmissionLiquidity {
    DexFirstTop,
    FullDepth(SpotDepthBook),
}

impl OwnedAdmissionLiquidity {
    fn borrowed(&self) -> AdmissionLiquidity<'_> {
        match self {
            Self::DexFirstTop => AdmissionLiquidity::DexFirstTop,
            Self::FullDepth(depth) => AdmissionLiquidity::FullDepth(depth),
        }
    }
}

#[derive(Debug)]
struct PendingAdaptiveAdmission {
    quote: TopOfBook,
    evaluation: PairEvaluation,
    admission_liquidity: OwnedAdmissionLiquidity,
    depth: Option<SpotDepthBook>,
    evaluation_trigger: &'static str,
    evaluation_started_at: Instant,
}

#[derive(Clone)]
struct AdaptiveSizingSnapshot {
    opportunities: OpportunityEngine,
    domain_config: Arc<LoadedDomainConfig>,
    telemetry: TelemetryHandle,
    engine_id: String,
}

pub struct AdaptiveSizingJob {
    snapshot: AdaptiveSizingSnapshot,
    pending: PendingAdaptiveAdmission,
    limits: AdaptiveSizingRuntimeLimits,
    pool_generations: Vec<(usize, u64)>,
    snapshot_time_us: u64,
    queued_at: Instant,
}

pub struct AdaptiveSizingTaskResult {
    strategy_id: crate::domain::compiled::StrategyId,
    pending: PendingAdaptiveAdmission,
    limits: AdaptiveSizingRuntimeLimits,
    pool_generations: Vec<(usize, u64)>,
    snapshot_time_us: u64,
    result: anyhow::Result<Option<AdaptiveCandidate>>,
    queued_at: Instant,
    started_at: Instant,
    finished_at: Instant,
}

impl AdaptiveSizingJob {
    pub fn strategy_id(&self) -> anyhow::Result<crate::domain::compiled::StrategyId> {
        let pair = self
            .snapshot
            .opportunities
            .pair(self.pending.evaluation.pair_index)?;
        crate::domain::compiled::StrategyId::new(strategy_id(&pair.pair_id))
    }

    pub fn run(self) -> AdaptiveSizingTaskResult {
        let strategy_id = self
            .strategy_id()
            .expect("validated adaptive sizing job must retain its compiled strategy");
        let started_at = Instant::now();
        let result = self.snapshot.evaluate_adaptive_sizing(
            &self.pending.quote,
            self.pending.evaluation,
            self.limits,
            self.pending.evaluation_trigger,
        );
        AdaptiveSizingTaskResult {
            strategy_id,
            pending: self.pending,
            limits: self.limits,
            pool_generations: self.pool_generations,
            snapshot_time_us: self.snapshot_time_us,
            result,
            queued_at: self.queued_at,
            started_at,
            finished_at: Instant::now(),
        }
    }
}

impl AdaptiveSizingTaskResult {
    pub fn strategy_id(&self) -> &crate::domain::compiled::StrategyId {
        &self.strategy_id
    }
}

struct CompletedAdaptiveSizing {
    limits: AdaptiveSizingRuntimeLimits,
    result: anyhow::Result<Option<AdaptiveCandidate>>,
}

#[derive(Debug, Clone)]
struct ArbitragePlanFreshness {
    pair_id: String,
    pool_index: usize,
    pool_generation: u64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ReservationPrecheck {
    Vacant,
    Duplicate,
    Conflict,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct RebalanceHealthState {
    healthy: bool,
    pending_stuck: bool,
    inflight_stuck: bool,
    settlement_stuck: bool,
}

fn rebalance_health_state(
    blocked: bool,
    pending_age: Option<Duration>,
    inflight_age: Option<Duration>,
    settlement_age: Option<Duration>,
    pending_timeout: Duration,
    operation_timeout: Duration,
    settlement_timeout: Duration,
) -> RebalanceHealthState {
    let pending_stuck = pending_age.is_some_and(|age| age >= pending_timeout);
    let inflight_stuck = inflight_age.is_some_and(|age| age >= operation_timeout);
    let settlement_stuck = settlement_age.is_some_and(|age| age >= settlement_timeout);
    RebalanceHealthState {
        healthy: !blocked && !pending_stuck && !inflight_stuck && !settlement_stuck,
        pending_stuck,
        inflight_stuck,
        settlement_stuck,
    }
}

fn rebalance_planning_deferred_reason(
    inflight: bool,
    settlement_waiting: bool,
) -> Option<&'static str> {
    if inflight {
        Some("operation_inflight")
    } else if settlement_waiting {
        Some("settlement_waiting")
    } else {
        None
    }
}

fn adaptive_candidate_is_better(candidate: AdaptiveCandidate, current: AdaptiveCandidate) -> bool {
    candidate.trade_notional > current.trade_notional
        || (candidate.trade_notional == current.trade_notional
            && (candidate.trade.token_b_amount > current.trade.token_b_amount
                || (candidate.trade.token_b_amount == current.trade.token_b_amount
                    && (adaptive_direction_order(candidate.direction)
                        < adaptive_direction_order(current.direction)
                        || (candidate.direction == current.direction
                            && candidate.trade.pool_index < current.trade.pool_index)))))
}

fn adaptive_trade_notional(direction: ArbitrageDirection, trade: TradeEvaluation) -> U256 {
    match direction {
        ArbitrageDirection::BuyTokenBOnDexSellOnCex => {
            trade.dex_amount_in.max(trade.proceeds_token_a)
        }
        ArbitrageDirection::BuyTokenBOnCexSellOnDex => {
            trade.cost_token_a.max(trade.proceeds_token_a)
        }
    }
}

const fn adaptive_direction_order(direction: ArbitrageDirection) -> u8 {
    match direction {
        ArbitrageDirection::BuyTokenBOnDexSellOnCex => 0,
        ArbitrageDirection::BuyTokenBOnCexSellOnDex => 1,
    }
}

const fn adaptive_trade_direction(direction: ArbitrageDirection) -> TradeDirection {
    match direction {
        ArbitrageDirection::BuyTokenBOnDexSellOnCex => TradeDirection::BuyTokenBOnDexSellOnCex,
        ArbitrageDirection::BuyTokenBOnCexSellOnDex => TradeDirection::BuyTokenBOnCexSellOnDex,
    }
}

fn reservation_precheck(
    inventory: &SharedInventoryReservations,
    request: &ReservationRequest,
) -> ReservationPrecheck {
    let Some(existing) = inventory.reservation(&request.operation_id) else {
        return ReservationPrecheck::Vacant;
    };
    if existing.request == *request {
        ReservationPrecheck::Duplicate
    } else {
        ReservationPrecheck::Conflict
    }
}

fn mark_sequence_matched_update(
    last_updates: &mut BTreeMap<String, u64>,
    symbol: &str,
    update_id: u64,
) -> bool {
    if last_updates
        .get(symbol)
        .is_some_and(|last| update_id <= *last)
    {
        return false;
    }
    last_updates.insert(symbol.to_owned(), update_id);
    true
}

impl RebalanceSettlementBarrier {
    fn reconciled(&self, binance_observed_at: Instant, wallet_observed_at: Instant) -> bool {
        binance_observed_at > self.binance_after && wallet_observed_at > self.wallet_after
    }
}

#[derive(Debug, Clone, Copy)]
struct TradingReadiness {
    dex_ready: bool,
    balances_ready: bool,
}

impl TradingReadiness {
    const fn ready(self) -> bool {
        self.dex_ready && self.balances_ready
    }
}

impl TradingEngine {
    pub fn owns_arbitrage_plan(&self, plan_id: &str) -> bool {
        self.arbitrage_plan_freshness.contains_key(plan_id)
    }
    pub fn new(
        config: AppConfig,
        domain_config: Arc<LoadedDomainConfig>,
        dex: DexMirror,
        telemetry: TelemetryHandle,
        rebalance: V12RebalanceParityAdapter,
        execution: TradingExecutionHandles,
        binance_fee_bps: BinanceFeeBps,
    ) -> anyhow::Result<(Self, HotTelemetryTask)> {
        let strategy_price_transport_silence_limits_ms =
            domain_config.strategy_price_transport_silence_limits_ms();
        let symbols = strategy_price_transport_silence_limits_ms
            .keys()
            .cloned()
            .map(Arc::<str>::from);
        let gas_price_symbol = domain_config
            .snapshot()
            .pairs
            .iter()
            .find(|pair| pair.market_data_enabled)
            .and_then(|pair| pair.chain.gas_price_binance_symbol.clone())
            .context("enabled pair has no versioned gas-price Binance symbol")?;
        let commission_pair = domain_config
            .snapshot()
            .pairs
            .iter()
            .find(|pair| pair.market_data_enabled)
            .context("enabled pair is missing")?;
        let commission_price_symbol = commission_pair
            .binance
            .commission_price_binance_symbol
            .clone()
            .context("enabled pair has no versioned commission-price Binance symbol")?;
        execution.entry_preflight.configure_max_transport_silence(
            &commission_price_symbol,
            commission_pair.strategy.max_transport_silence_ms(),
        );
        let mut opportunities = OpportunityEngine::new(domain_config.snapshot(), &dex)?;
        for symbol in domain_config.binance_symbols() {
            opportunities.set_binance_fee_bps(
                &symbol,
                binance_fee_bps.buy,
                binance_fee_bps.sell,
            )?;
        }
        execution
            .entry_preflight
            .configure_dex_max_head_age(config.dex_head_max_age_ms);
        for pair in domain_config
            .snapshot()
            .pairs
            .iter()
            .filter(|pair| pair.market_data_enabled)
        {
            execution
                .entry_preflight
                .update_dex_head(&pair.id, dex.latest_head_received_at());
        }
        for (pool_index, _) in opportunities.pool_generations() {
            let pool = dex.pool(pool_index)?;
            let pair = domain_config
                .snapshot()
                .pairs
                .iter()
                .find(|pair| pair.id == pool.pair_id)
                .with_context(|| format!("DEX pool {} has no domain pair", pool.pair_id))?;
            let curves = opportunities
                .preflight_exact_input_curves(pool_index)?
                .context("initial prepared DEX pool is unavailable for preflight")?;
            execution
                .entry_preflight
                .update_dex_pool_with_fee_generation(
                    &pool.pair_id,
                    pool_index,
                    opportunities.pool_generation(pool_index)?,
                    opportunities.pool_fee_generation(pool_index)?,
                    pair.token_a.decimals,
                    pair.token_b.decimals,
                    curves,
                );
        }
        for pair in domain_config
            .snapshot()
            .pairs
            .iter()
            .filter(|pair| pair.execution_enabled)
        {
            let max_transport_silence_ms = *strategy_price_transport_silence_limits_ms
                .get(&pair.binance.symbol)
                .with_context(|| {
                    format!(
                        "execution pair {} has no Binance transport silence limit",
                        pair.id
                    )
                })?;
            execution
                .entry_preflight
                .configure_max_transport_silence(&pair.binance.symbol, max_transport_silence_ms);
        }
        let (hot_telemetry, hot_telemetry_task) = hot_telemetry_channel(
            &config,
            opportunities.pairs(),
            &dex,
            telemetry.clone(),
            execution.pretrade_cost_telemetry,
        )?;
        let portfolio_catalog = execution.portfolio_catalog;
        let require_binance_depth =
            requires_depth_for_runtime_phase(config.arbitrage_execution_mode.as_str());
        Ok((
            Self {
                config,
                domain_config,
                state: if require_binance_depth {
                    RuntimeState::new_with_depth(symbols)
                } else {
                    RuntimeState::new(symbols)
                },
                dex,
                opportunities,
                rebalance,
                telemetry,
                hot_telemetry,
                paper_trades: execution.paper_trades,
                inventory: execution.inventory,
                capital_allocator: execution.capital_allocator,
                portfolio_catalog,
                binance_asset_decimals: execution.binance_asset_decimals,
                binance_inventory_generation: 0,
                binance_user_data_connected: false,
                binance_user_data_clean: true,
                binance_orders: BTreeMap::new(),
                last_sequence_matched_quote_update: BTreeMap::new(),
                latest_sequence_matched_depth: BTreeMap::new(),
                depth_health_by_symbol: BTreeMap::new(),
                strategy_price_transport_silence_limits_ms,
                gas_price_symbol,
                gas_price_connected: false,
                gas_price_generation: 0,
                gas_price_book: None,
                commission_price_symbol,
                binance_clock_sync: None,
                rebalance_inventory_reservations: BTreeMap::new(),
                next_inventory_reservation: 0,
                pending_rebalance: None,
                rebalance_pending_since: None,
                rebalance_inflight: false,
                rebalance_inflight_since: None,
                rebalance_blocked_tokens: BTreeSet::new(),
                rebalance_deferred_reason: None,
                rebalance_settlement: None,
                last_rebalance_health_log_at: None,
                last_depth_health_log_at: None,
                last_binance_price_health_log_at: None,
                last_inventory_blocked_alert_at: None,
                last_inventory_contention_log_at: None,
                entry_preflight: execution.entry_preflight,
                arbitrage_plan_freshness: BTreeMap::new(),
                terminal_child_observed_at: BTreeMap::new(),
                pending_adaptive_sizing: Vec::new(),
            },
            hot_telemetry_task,
        ))
    }

    pub fn start(&mut self) {
        let unavailable_dex_pools: Vec<Value> = self
            .dex
            .unavailable_pools()
            .iter()
            .map(|pool| {
                json!({
                    "pair_id": pool.pair_id,
                    "protocol": match pool.protocol {
                        DexProvider::ZeroX => "zero_x",
                        DexProvider::UniswapV3 => "uniswap_v3",
                        DexProvider::UniswapV4 => "uniswap_v4",
                        DexProvider::PancakeSwapV3 => "pancakeswap_v3",
                        DexProvider::CamelotV3 => "camelot_v3",
                    },
                    "fee_pips": pool.fee_pips,
                    "address": pool.address.map(|address| format!("{address:?}")),
                    "pool_id": pool.pool_id.map(|pool_id| format!("{pool_id:?}")),
                    "reason": pool.reason.as_str(),
                })
            })
            .collect();
        self.telemetry.emit(
            "runtime_starting",
            json!({
                "engine_id": self.config.engine_id,
                "service": self.config.service_name,
                "gcp_project_id": self.config.gcp_project_id,
                "gcp_region": self.config.gcp_region,
                "domain_snapshot_id": self.domain_config.snapshot().snapshot_id,
                "domain_config_sha256": self.domain_config.fingerprint_sha256(),
                "domain_config_path": self.domain_config.path().display().to_string(),
                "pair_ids": self.domain_config.pair_ids(),
                "binance_symbols": self.domain_config.binance_symbols(),
                "dex_pools": self.dex.pool_count(),
                "dex_unavailable_pools": self.dex.unavailable_count(),
                "dex_unavailable_pool_details": unavailable_dex_pools,
                "world_chain_block": self.dex.latest_head().number,
            }),
        );
    }

    pub fn on_user_data_connected(&mut self, subscription_id: u64) {
        self.binance_user_data_connected = true;
        self.telemetry.emit(
            "binance_user_data_connected",
            json!({
                "engine_id": self.config.engine_id,
                "subscription_id": subscription_id,
            }),
        );
        self.refresh_phase(Instant::now());
    }

    pub fn on_shared_user_data_connected(&mut self) {
        self.binance_user_data_connected = true;
        self.refresh_phase(Instant::now());
    }

    pub fn on_shared_user_data_disconnected(&mut self) {
        self.binance_user_data_connected = false;
        self.refresh_phase(Instant::now());
    }

    pub fn on_shared_user_data_dirty(&mut self) {
        self.binance_user_data_clean = false;
        self.refresh_phase(Instant::now());
    }

    pub fn on_user_data_event(&mut self, event: UserDataEvent) -> anyhow::Result<()> {
        match event {
            UserDataEvent::AccountPosition(position) => {
                self.binance_inventory_generation = self
                    .binance_inventory_generation
                    .checked_add(1)
                    .context("Binance inventory generation overflow")?;
                let reservations_before = self
                    .inventory
                    .active_operation_ids()
                    .into_iter()
                    .collect::<Vec<_>>();
                let mut balances = Vec::new();
                let mut locked_assets = Vec::new();
                let observed_balances = position
                    .balances
                    .iter()
                    .map(|balance| {
                        json!({
                            "asset": &balance.asset,
                            "free": balance.free.to_string(),
                            "locked": balance.locked.to_string(),
                        })
                    })
                    .collect::<Vec<_>>();
                for balance in &position.balances {
                    if !balance.locked.is_zero() {
                        locked_assets.push(balance.asset.clone());
                    }
                    if let Ok(decimals) = self.token_decimals(&balance.asset) {
                        let key = self
                            .portfolio_key(&self.binance_inventory_location()?, &balance.asset)?;
                        balances.push((
                            key.venue_asset_id,
                            decimal_to_base_units_floor(balance.free, decimals)?,
                        ));
                    }
                }
                if !balances.is_empty() {
                    self.inventory.update_location_assets(
                        self.binance_inventory_location()?,
                        self.binance_inventory_generation,
                        balances,
                    )?;
                    self.reconcile_inventory_settlements(&reservations_before);
                }
                self.telemetry.emit(
                    "binance_user_account_position",
                    json!({
                        "engine_id": self.config.engine_id,
                        "event_time_ms": position.event_time_ms,
                        "last_account_update_ms": position.last_account_update_ms,
                        "changed_assets": position.balances.len(),
                        "locked_assets": locked_assets,
                        "balances": observed_balances,
                    }),
                );
            }
            UserDataEvent::ExecutionReport(report) => {
                let report = *report;
                let owned = report.client_order_id.starts_with("rust")
                    && self
                        .domain_config
                        .binance_symbols()
                        .iter()
                        .any(|symbol| symbol == &report.symbol);
                if !owned {
                    self.binance_user_data_clean = false;
                }
                self.telemetry.emit(
                    "binance_execution_report",
                    json!({
                        "engine_id": self.config.engine_id,
                        "event_time_ms": report.event_time_ms,
                        "transaction_time_ms": report.transaction_time_ms,
                        "symbol": &report.symbol,
                        "client_order_id": &report.client_order_id,
                        "order_id": report.order_id,
                        "side": &report.side,
                        "order_type": &report.order_type,
                        "execution_type": &report.execution_type,
                        "order_status": &report.order_status,
                        "reject_reason": &report.reject_reason,
                        "last_executed_quantity": report.last_executed_quantity.to_string(),
                        "cumulative_filled_quantity": report.cumulative_filled_quantity.to_string(),
                        "last_executed_price": report.last_executed_price.to_string(),
                        "commission": report.commission.to_string(),
                        "commission_asset": &report.commission_asset,
                        "trade_id": report.trade_id,
                        "owned": owned,
                    }),
                );
                self.binance_orders
                    .insert(report.client_order_id.clone(), report);
            }
            UserDataEvent::BalanceUpdate(update) => self.telemetry.emit(
                "binance_user_balance_update",
                json!({
                    "engine_id": self.config.engine_id,
                    "event_time_ms": update.event_time_ms,
                    "asset": update.asset,
                    "delta": update.delta.to_string(),
                    "clear_time_ms": update.clear_time_ms,
                }),
            ),
            UserDataEvent::StreamTerminated { event_time_ms } => {
                self.binance_user_data_connected = false;
                self.telemetry.emit(
                    "binance_user_data_terminated",
                    json!({
                        "engine_id": self.config.engine_id,
                        "event_time_ms": event_time_ms,
                    }),
                );
            }
            UserDataEvent::Other {
                event_type,
                event_time_ms,
            } => {
                self.binance_user_data_clean = false;
                self.telemetry.emit(
                    "binance_user_data_unhandled",
                    json!({
                        "engine_id": self.config.engine_id,
                        "event_type": event_type,
                        "event_time_ms": event_time_ms,
                    }),
                );
            }
        }
        self.refresh_phase(Instant::now());
        Ok(())
    }

    pub fn on_dex_event(
        &mut self,
        event: DexStreamEvent,
    ) -> anyhow::Result<Option<PreparedPoolBuildRequest>> {
        self.apply_dex_event(event, true)
    }

    pub fn on_startup_dex_event(
        &mut self,
        event: DexStreamEvent,
    ) -> anyhow::Result<Option<PreparedPoolBuildRequest>> {
        self.apply_dex_event(event, false)
    }

    fn apply_dex_event(
        &mut self,
        event: DexStreamEvent,
        emit_hot_path_latency: bool,
    ) -> anyhow::Result<Option<PreparedPoolBuildRequest>> {
        let request = match event {
            DexStreamEvent::Log {
                log,
                block_timestamp,
                received_at,
            } => {
                if let LogApplyResult::Applied {
                    pool_index,
                    kind,
                    refresh_required,
                } = self.dex.apply_log_at_timestamp(&log, block_timestamp)?
                {
                    let request = if refresh_required {
                        self.dex.refresh_pool_for_publication(pool_index)?;
                        Some(
                            self.opportunities
                                .request_pool_refresh(pool_index, &self.dex)?,
                        )
                    } else {
                        None
                    };
                    if emit_hot_path_latency {
                        self.hot_telemetry.emit_dex_pool_event(
                            pool_index,
                            kind,
                            log.block_number,
                            log.transaction_index,
                            log.log_index,
                            received_at.elapsed().as_micros(),
                            request.as_ref().map_or(
                                self.opportunities.pool_generation(pool_index)?,
                                PreparedPoolBuildRequest::generation,
                            ),
                        );
                    }
                    request
                } else {
                    None
                }
            }
            DexStreamEvent::Head {
                head,
                timestamp,
                received_at,
            } => {
                let applied = self.dex.apply_head_at(head, Some(timestamp), received_at)?;
                if applied.advanced && emit_hot_path_latency {
                    self.hot_telemetry
                        .emit_dex_head(head.number, received_at.elapsed().as_micros());
                }
                self.entry_preflight.update_dex_head(
                    self.domain_config
                        .snapshot()
                        .pairs
                        .first()
                        .expect("validated engine has a pair")
                        .id
                        .as_str(),
                    self.dex.latest_head_received_at(),
                );
                applied
                    .refresh_pool_index
                    .map(|pool_index| {
                        self.opportunities
                            .request_pool_refresh(pool_index, &self.dex)
                    })
                    .transpose()?
            }
        };
        self.refresh_phase(Instant::now());
        Ok(request)
    }

    pub fn on_prepared_pool(&mut self, result: PreparedPoolBuildResult) -> anyhow::Result<()> {
        let Some(prepared) = self.opportunities.finish_pool_refresh(result)? else {
            return Ok(());
        };
        self.refresh_preflight_dex_pool(prepared.pool_index)?;
        self.hot_telemetry.emit_dex_pool_prepared(prepared);
        self.refresh_phase(Instant::now());
        Ok(())
    }

    fn refresh_preflight_dex_pool(&self, pool_index: usize) -> anyhow::Result<()> {
        let pool = self.dex.pool(pool_index)?;
        let pair = self
            .domain_config
            .snapshot()
            .pairs
            .iter()
            .find(|pair| pair.id == pool.pair_id)
            .with_context(|| format!("DEX pool {} has no domain pair", pool.pair_id))?;
        self.entry_preflight.update_dex_pool_with_fee_generation(
            &pool.pair_id,
            pool_index,
            self.opportunities.pool_generation(pool_index)?,
            self.opportunities.pool_fee_generation(pool_index)?,
            pair.token_a.decimals,
            pair.token_b.decimals,
            self.opportunities
                .preflight_exact_input_curves(pool_index)?
                .context("prepared DEX pool is unavailable for preflight")?,
        );
        Ok(())
    }

    /// Evaluates the newest books only after the caller has drained every DEX
    /// event that was already queued. This prevents an expensive admission
    /// calculation from delaying publication of a newer pool generation.
    pub fn evaluate_after_dex_refreshes(&mut self) -> anyhow::Result<()> {
        if self.state.phase == RuntimePhase::Ready {
            let books: Vec<_> = self
                .state
                .binance_feeds
                .values()
                .filter_map(|feed| feed.book.clone())
                .collect();
            for quote in books {
                let depth = self.matching_cached_depth(&quote).cloned();
                let (admission, adaptive_depth) = if self.uses_dex_first_fast_path() {
                    (Some(AdmissionLiquidity::DexFirstTop), depth.as_ref())
                } else {
                    (
                        depth.as_ref().map(AdmissionLiquidity::FullDepth),
                        depth.as_ref(),
                    )
                };
                self.evaluate_ready_quote(&quote, "dex_prepared", admission, adaptive_depth)?;
            }
        }
        Ok(())
    }

    pub fn take_adaptive_sizing_jobs(&mut self) -> Vec<AdaptiveSizingJob> {
        std::mem::take(&mut self.pending_adaptive_sizing)
    }

    pub fn record_adaptive_sizing_overload(
        &self,
        strategy_id: &crate::domain::compiled::StrategyId,
        replaced_pending_snapshot: bool,
        total_retained_work: usize,
    ) {
        self.telemetry.emit(
            "strategy_calculation_overload",
            json!({
                "engine_id": self.config.engine_id,
                "strategy_id": strategy_id.as_str(),
                "work_class": "exhaustive_sizing",
                "policy": "one_running_one_latest_pending_per_strategy",
                "replaced_pending_snapshot": replaced_pending_snapshot,
                "total_retained_work": total_retained_work,
                "unbounded_queue": false,
            }),
        );
    }

    pub fn on_adaptive_sizing_result(
        &mut self,
        task: AdaptiveSizingTaskResult,
    ) -> anyhow::Result<()> {
        let pair = self
            .opportunities
            .pair(task.pending.evaluation.pair_index)?;
        let quote_is_current = self
            .state
            .binance_feeds
            .get(task.pending.quote.symbol.as_ref())
            .and_then(|feed| feed.book.as_ref())
            .is_some_and(|book| book == &task.pending.quote);
        let pools_are_current = task
            .pool_generations
            .iter()
            .all(|(pool_index, generation)| {
                self.opportunities
                    .pool_generation(*pool_index)
                    .is_ok_and(|current| current == *generation)
            });
        let stale_reason = if self.state.phase != RuntimePhase::Ready {
            Some("runtime_not_ready")
        } else if !quote_is_current {
            Some("binance_generation_changed")
        } else if !pools_are_current {
            Some("dex_generation_changed")
        } else {
            None
        };
        self.telemetry.emit(
            "arbitrage_adaptive_sizing_task",
            json!({
                "engine_id": self.config.engine_id,
                "pair_id": pair.pair_id,
                "strategy_id": strategy_id(&pair.pair_id),
                "binance_account_id": PRIMARY_BINANCE_ACCOUNT_ID,
                "instrument_id": instrument_id(&pair.symbol),
                "network_id": network_id(pair.chain_id),
                "symbol": task.pending.quote.symbol.as_ref(),
                "update_id": task.pending.quote.update_id,
                "outcome": if stale_reason.is_some() { "superseded" } else { "current" },
                "stale_reason": stale_reason,
                "queue_time_us": duration_us(task.started_at.saturating_duration_since(task.queued_at)),
                "worker_time_us": duration_us(task.finished_at.saturating_duration_since(task.started_at)),
                "result_handoff_time_us": duration_us(task.finished_at.elapsed()),
                "snapshot_time_us": task.snapshot_time_us,
            }),
        );
        if stale_reason.is_some() {
            return Ok(());
        }

        let pending = task.pending;
        let admission_liquidity = pending.admission_liquidity.borrowed();
        let depth = pending.depth.as_ref();
        self.submit_paper_opportunity_inner(
            &pending.quote,
            pending.evaluation,
            admission_liquidity,
            depth,
            pending.evaluation_trigger,
            pending.evaluation_started_at,
            Some(CompletedAdaptiveSizing {
                limits: task.limits,
                result: task.result,
            }),
        )?;
        Ok(())
    }

    pub fn on_market_event(
        &mut self,
        event: MarketEvent,
        depth: Option<&SpotDepthBook>,
    ) -> anyhow::Result<()> {
        match event {
            MarketEvent::FeedConnected {
                symbol,
                generation,
                observed_at,
            } => {
                self.state.on_connected(&symbol, generation, observed_at);
                self.entry_preflight
                    .on_feed_connected(symbol.as_ref(), generation, observed_at);
                self.last_sequence_matched_quote_update
                    .remove(symbol.as_ref());
                self.latest_sequence_matched_depth.remove(symbol.as_ref());
                self.depth_health_by_symbol.remove(symbol.as_ref());
                self.telemetry.emit(
                    "binance_feed_connected",
                    json!({
                        "engine_id": self.config.engine_id,
                        "product": "spot",
                        "symbol": symbol.as_ref(),
                        "generation": generation,
                        "observed_mono_age_us": observed_at.elapsed().as_micros(),
                    }),
                );
            }
            MarketEvent::FeedDisconnected {
                symbol,
                generation,
                reason,
                observed_at,
            } => {
                self.state.on_disconnected(&symbol, generation);
                self.entry_preflight
                    .on_feed_disconnected(symbol.as_ref(), generation);
                self.latest_sequence_matched_depth.remove(symbol.as_ref());
                self.depth_health_by_symbol.remove(symbol.as_ref());
                self.telemetry.emit(
                    "binance_feed_disconnected",
                    json!({
                        "engine_id": self.config.engine_id,
                        "product": "spot",
                        "symbol": symbol.as_ref(),
                        "generation": generation,
                        "reason": reason,
                        "observed_mono_age_us": observed_at.elapsed().as_micros(),
                    }),
                );
            }
            MarketEvent::FeedHeartbeat {
                symbol,
                generation,
                observed_at,
            } => {
                let accepted =
                    self.state
                        .record_transport_activity(symbol.as_ref(), generation, observed_at);
                if accepted {
                    self.entry_preflight.record_transport_activity(
                        symbol.as_ref(),
                        generation,
                        observed_at,
                    );
                }
                self.telemetry.emit(
                    "binance_feed_heartbeat",
                    json!({
                        "engine_id": self.config.engine_id,
                        "product": "spot",
                        "feed_role": "strategy_price",
                        "symbol": symbol.as_ref(),
                        "generation": generation,
                        "accepted": accepted,
                        "observed_mono_age_us": observed_at.elapsed().as_micros(),
                    }),
                );
            }
            MarketEvent::BinanceTopOfBook(quote) => {
                self.on_binance_quote(quote, depth)?;
            }
            MarketEvent::BinanceDepthApplied {
                symbol,
                generation,
                last_update_id,
                exchange_event_ts_ms,
                observed_at,
                received_unix_us,
                wire_frame_size_bytes,
                parse_apply_time_us,
            } => {
                let apply_result = self.state.apply_depth(
                    symbol.as_ref(),
                    generation,
                    last_update_id,
                    observed_at,
                );
                let clock_sync = self.binance_clock_sync;
                let clock_sync_age_ms = clock_sync.map(BinanceClockSync::age_ms);
                let estimate_valid = clock_sync_estimate_valid(clock_sync_age_ms);
                let valid_clock_sync = clock_sync.filter(|_| estimate_valid);
                let exchange_event_to_socket_estimate_us =
                    valid_clock_sync.and_then(|clock_sync| {
                        estimate_exchange_event_to_socket_us(
                            received_unix_us,
                            exchange_event_ts_ms,
                            clock_sync.offset_ms,
                        )
                    });
                let estimate_uncertainty_us = valid_clock_sync.map(|clock_sync| {
                    clock_sync
                        .midpoint_uncertainty_us()
                        .saturating_add(BINANCE_JSON_TIME_RESOLUTION_US.saturating_mul(2))
                });
                let estimate_invalid_reason = if clock_sync.is_none() {
                    Some("clock_sync_unavailable")
                } else if !estimate_valid {
                    Some("clock_sync_stale")
                } else {
                    None
                };
                self.telemetry.emit(
                    "binance_depth_applied",
                    json!({
                        "engine_id": self.config.engine_id,
                        "product": "spot",
                        "symbol": symbol.as_ref(),
                        "generation": generation,
                        "last_update_id": last_update_id,
                        "exchange_event_ts_ms": exchange_event_ts_ms,
                        "received_unix_us": received_unix_us,
                        "exchange_event_to_socket_estimate_us": exchange_event_to_socket_estimate_us,
                        "exchange_event_to_socket_uncertainty_us": estimate_uncertainty_us,
                        "exchange_event_to_socket_estimate_valid": estimate_valid,
                        "exchange_event_to_socket_estimate_invalid_reason": estimate_invalid_reason,
                        "exchange_timestamp_resolution_us": BINANCE_JSON_TIME_RESOLUTION_US,
                        "clock_offset_ms": clock_sync.map(|sync| sync.offset_ms),
                        "clock_offset_resolution_us": BINANCE_JSON_TIME_RESOLUTION_US,
                        "clock_sync_rtt_us": clock_sync.map(|sync| sync.round_trip_us),
                        "clock_sync_midpoint_uncertainty_us": clock_sync.map(BinanceClockSync::midpoint_uncertainty_us),
                        "clock_sync_age_ms": clock_sync_age_ms,
                        "clock_sync_max_age_ms": BINANCE_CLOCK_SYNC_MAX_AGE_MS,
                        "clock_sync_observed_unix_ms": clock_sync.map(|sync| sync.observed_unix_ms),
                        "wire_frame_size_bytes": wire_frame_size_bytes,
                        "parse_apply_time_us": parse_apply_time_us,
                        "observed_mono_age_us": observed_at.elapsed().as_micros(),
                        "apply_result": format!("{apply_result:?}"),
                    }),
                );
                self.refresh_phase(Instant::now());
                let quote = self
                    .state
                    .binance_feeds
                    .get(symbol.as_ref())
                    .and_then(|feed| feed.book.clone());
                if self.state.phase == RuntimePhase::Ready
                    && let (Some(depth), Some(quote)) = (depth, quote)
                    && depth.matches_top(
                        quote.symbol.as_ref(),
                        quote.update_id,
                        quote.bid_price,
                        quote.bid_quantity,
                        quote.ask_price,
                        quote.ask_quantity,
                    )
                {
                    self.latest_sequence_matched_depth
                        .insert(symbol.to_string(), depth.clone());
                    if !self.uses_dex_first_fast_path() {
                        self.evaluate_sequence_matched_quote(&quote, "binance_depth", depth)?;
                    }
                }
            }
        }
        self.refresh_phase(Instant::now());
        Ok(())
    }

    pub fn on_binance_clock_sync(&mut self, clock_sync: BinanceClockSync) {
        self.binance_clock_sync = Some(clock_sync);
        self.telemetry.emit(
            "binance_clock_sync",
            json!({
                "engine_id": self.config.engine_id,
                "product": "spot",
                "healthy": true,
                "clock_offset_ms": clock_sync.offset_ms,
                "clock_offset_resolution_us": BINANCE_JSON_TIME_RESOLUTION_US,
                "round_trip_us": clock_sync.round_trip_us,
                "midpoint_uncertainty_us": clock_sync.midpoint_uncertainty_us(),
                "observed_unix_ms": clock_sync.observed_unix_ms,
                "observation_age_ms": clock_sync.age_ms(),
                "estimate_max_clock_sync_age_ms": BINANCE_CLOCK_SYNC_MAX_AGE_MS,
            }),
        );
    }

    pub fn on_binance_clock_sync_failure(&self, error: &str) {
        self.telemetry.emit(
            "binance_clock_sync",
            json!({
                "engine_id": self.config.engine_id,
                "product": "spot",
                "healthy": false,
                "error": error,
                "retained_previous_observation": self.binance_clock_sync.is_some(),
                "previous_observation_age_ms": self.binance_clock_sync.map(BinanceClockSync::age_ms),
                "estimate_max_clock_sync_age_ms": BINANCE_CLOCK_SYNC_MAX_AGE_MS,
            }),
        );
    }

    pub fn on_gas_market_event(&mut self, event: MarketEvent) -> anyhow::Result<()> {
        match event {
            MarketEvent::FeedConnected {
                symbol,
                generation,
                observed_at: _,
            } => {
                ensure!(
                    symbol.as_ref() == self.gas_price_symbol,
                    "gas feed symbol mismatch"
                );
                if generation >= self.gas_price_generation {
                    self.gas_price_connected = true;
                    self.gas_price_generation = generation;
                }
            }
            MarketEvent::FeedDisconnected {
                symbol, generation, ..
            } => {
                ensure!(
                    symbol.as_ref() == self.gas_price_symbol,
                    "gas feed symbol mismatch"
                );
                if generation == self.gas_price_generation {
                    self.gas_price_connected = false;
                }
            }
            MarketEvent::FeedHeartbeat {
                symbol,
                generation,
                observed_at,
            } => {
                ensure!(
                    symbol.as_ref() == self.gas_price_symbol,
                    "gas heartbeat symbol mismatch"
                );
                let accepted = self.gas_price_connected && generation == self.gas_price_generation;
                self.telemetry.emit(
                    "binance_feed_heartbeat",
                    json!({
                        "engine_id": self.config.engine_id,
                        "product": "spot",
                        "feed_role": "gas_conversion",
                        "symbol": symbol.as_ref(),
                        "generation": generation,
                        "accepted": accepted,
                        "observed_mono_age_us": observed_at.elapsed().as_micros(),
                    }),
                );
            }
            MarketEvent::BinanceTopOfBook(quote) => {
                ensure!(
                    quote.symbol.as_ref() == self.gas_price_symbol,
                    "gas quote symbol mismatch"
                );
                if self.gas_price_connected
                    && quote.connection_generation == self.gas_price_generation
                    && self.gas_price_book.as_ref().is_none_or(|current| {
                        quote.connection_generation > current.connection_generation
                            || quote.update_id > current.update_id
                    })
                {
                    self.hot_telemetry
                        .emit_binance_book(&quote, "gas_conversion", None, "stored");
                    self.hot_telemetry
                        .publish_native_conversion(quote.received_unix_us, quote.ask_price);
                    self.gas_price_book = Some(quote);
                }
            }
            MarketEvent::BinanceDepthApplied { .. } => {
                anyhow::bail!("gas-price feed unexpectedly emitted depth")
            }
        }
        self.refresh_phase(Instant::now());
        Ok(())
    }

    pub fn native_price_token_a(&self) -> Option<Decimal> {
        self.gas_price_book.as_ref().map(|book| book.ask_price)
    }

    /// Maintains the diagnostic/accounting-only Binance commission valuation
    /// feed. It never changes runtime readiness, admission, sizing, or
    /// preflight; the live executor reads it only after an order has filled.
    pub fn on_commission_market_event(&mut self, event: MarketEvent) -> anyhow::Result<()> {
        match event {
            MarketEvent::FeedConnected {
                symbol,
                generation,
                observed_at,
            } => {
                ensure!(
                    symbol.as_ref() == self.commission_price_symbol,
                    "commission-price feed symbol mismatch"
                );
                self.entry_preflight
                    .on_feed_connected(symbol.as_ref(), generation, observed_at);
            }
            MarketEvent::FeedDisconnected {
                symbol, generation, ..
            } => {
                ensure!(
                    symbol.as_ref() == self.commission_price_symbol,
                    "commission-price feed symbol mismatch"
                );
                self.entry_preflight
                    .on_feed_disconnected(symbol.as_ref(), generation);
            }
            MarketEvent::FeedHeartbeat {
                symbol,
                generation,
                observed_at,
            } => {
                ensure!(
                    symbol.as_ref() == self.commission_price_symbol,
                    "commission-price heartbeat symbol mismatch"
                );
                self.entry_preflight.record_transport_activity(
                    symbol.as_ref(),
                    generation,
                    observed_at,
                );
                self.telemetry.emit(
                    "binance_feed_heartbeat",
                    json!({
                        "engine_id": self.config.engine_id,
                        "product": "spot",
                        "feed_role": "commission_conversion",
                        "symbol": symbol.as_ref(),
                        "generation": generation,
                    }),
                );
            }
            MarketEvent::BinanceTopOfBook(quote) => {
                ensure!(
                    quote.symbol.as_ref() == self.commission_price_symbol,
                    "commission-price quote symbol mismatch"
                );
                self.entry_preflight.update_quote(&quote);
                self.hot_telemetry.emit_binance_book(
                    &quote,
                    "commission_conversion",
                    None,
                    "stored",
                );
            }
            MarketEvent::BinanceDepthApplied { .. } => {
                anyhow::bail!("commission-price feed unexpectedly emitted depth")
            }
        }
        Ok(())
    }

    pub fn on_balance_event(&mut self, event: BalanceEvent) -> anyhow::Result<()> {
        let reservations_before = self
            .inventory
            .active_operation_ids()
            .into_iter()
            .collect::<Vec<_>>();
        match event {
            BalanceEvent::Binance(snapshot) => {
                self.binance_inventory_generation = self
                    .binance_inventory_generation
                    .checked_add(1)
                    .context("Binance inventory generation overflow")?;
                let balances = snapshot
                    .balances
                    .iter()
                    .map(|(asset, balance)| {
                        let decimals = self.token_decimals(asset.as_ref())?;
                        Ok((
                            asset.to_string(),
                            decimal_to_base_units_floor(balance.free, decimals)?,
                        ))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                let inventory_corrections = balances
                    .iter()
                    .filter_map(|(asset, rest_amount)| {
                        let key = self
                            .portfolio_key(&self.binance_inventory_location().ok()?, asset)
                            .ok()?;
                        let observed_amount = self.inventory.observed(&key)?;
                        (observed_amount != *rest_amount).then(|| {
                            json!({
                                "asset": asset,
                                "observed_before_base_units": observed_amount.to_string(),
                                "rest_base_units": rest_amount.to_string(),
                            })
                        })
                    })
                    .collect::<Vec<_>>();
                let binance_location = self.binance_inventory_location()?;
                let inventory_balances = balances
                    .iter()
                    .map(|(symbol, amount)| {
                        self.portfolio_key(&binance_location, symbol)
                            .map(|key| (key.venue_asset_id, *amount))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                self.inventory.update_location(
                    binance_location,
                    self.binance_inventory_generation,
                    inventory_balances,
                )?;
                // REST is the independent full reconciliation boundary. It
                // clears diagnostic User Data anomalies; neither foreign/open
                // orders nor locked balances are global trading gates.
                self.binance_user_data_clean = true;
                let balances = snapshot
                    .balances
                    .iter()
                    .map(|(asset, balance)| {
                        json!({
                            "asset": asset.as_ref(),
                            "free": balance.free.to_string(),
                            "locked": balance.locked.to_string(),
                        })
                    })
                    .collect::<Vec<_>>();
                self.telemetry.emit(
                    "binance_balance_snapshot",
                    json!({
                        "engine_id": self.config.engine_id,
                        "binance_account_id": PRIMARY_BINANCE_ACCOUNT_ID,
                        "account_update_time_ms": snapshot.account_update_time_ms,
                        "account_type": snapshot.account_type,
                        "can_trade": snapshot.can_trade,
                        "balances": balances,
                        "inventory_correction_count": inventory_corrections.len(),
                        "inventory_corrections": inventory_corrections,
                        "request_duration_us": snapshot.request_duration_us,
                    }),
                );
                self.state.balances.apply_binance(snapshot);
            }
            BalanceEvent::BinanceOpenOrders {
                client_order_ids,
                observed_at,
            } => {
                self.telemetry.emit(
                    "binance_open_orders_reconciled",
                    json!({
                        "engine_id": self.config.engine_id,
                        "open_order_count": client_order_ids.len(),
                        "client_order_ids": client_order_ids,
                        "engine_queue_age_us": observed_at.elapsed().as_micros(),
                    }),
                );
            }
            BalanceEvent::Wallet(snapshot) => {
                ensure!(
                    snapshot.batch_complete,
                    "partial wallet batch cannot advance inventory readiness"
                );
                let batch_queue_us = snapshot.observed_at.elapsed().as_micros();
                let publication_started_at = Instant::now();
                let wallet_inventory = snapshot
                    .token_balances
                    .iter()
                    .map(|balance| {
                        self.portfolio_key(
                            &self.wallet_inventory_location(snapshot.chain_id)?,
                            balance.symbol.as_ref(),
                        )
                        .map(|key| (key.venue_asset_id, balance.base_units))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                self.inventory.update_location(
                    self.wallet_inventory_location(snapshot.chain_id)?,
                    snapshot.block_number,
                    wallet_inventory,
                )?;
                let token_balances = snapshot
                    .token_balances
                    .iter()
                    .map(|balance| {
                        json!({
                            "symbol": balance.symbol.as_ref(),
                            "contract": format!("{:#x}", balance.contract),
                            "base_units": balance.base_units.to_string(),
                        })
                    })
                    .collect::<Vec<_>>();
                self.telemetry.emit(
                    "wallet_balance_snapshot",
                    json!({
                        "engine_id": self.config.engine_id,
                        "wallet_id": PRIMARY_EVM_WALLET_ID,
                        "network_id": network_id(snapshot.chain_id),
                        "wallet_location_id": wallet_location_id(snapshot.chain_id),
                        "execution_lane_id": execution_lane_id(snapshot.chain_id),
                        "owner": format!("{:#x}", snapshot.owner),
                        "chain_id": snapshot.chain_id,
                        "block_number": snapshot.block_number,
                        "block_hash": format!("{:#x}", snapshot.block_hash),
                        "token_balances": token_balances,
                        "request_duration_us": snapshot.request_duration_us,
                        "batch_build_us": snapshot.batch_build_us,
                        "batch_queue_us": batch_queue_us,
                        "batch_coordinator_queue_us": snapshot.batch_coordinator_queue_us,
                        "batch_provider_us": snapshot.batch_provider_us,
                        "batch_rpc_decode_us": snapshot.batch_rpc_decode_us,
                        "batch_decode_us": snapshot.batch_decode_us,
                        "batch_publication_us": publication_started_at.elapsed().as_micros(),
                        "batch_chunk_count": snapshot.batch_chunk_count,
                        "batch_response_bytes": snapshot.batch_response_bytes,
                        "batch_complete": snapshot.batch_complete,
                        "rpc_http_requests": snapshot.rpc_stats.http_requests,
                        "rpc_eth_calls": snapshot.rpc_stats.eth_calls,
                        "rpc_rate_limit_retries": snapshot.rpc_stats.rate_limit_retries,
                    }),
                );
                self.state.balances.apply_wallet(snapshot);
            }
            BalanceEvent::Failed {
                source,
                error,
                observed_at,
            } => {
                self.state.balances.record_failure(source);
                tracing::warn!(
                    source = source.as_str(),
                    error,
                    "balance synchronization failed"
                );
                self.telemetry.emit(
                    "balance_sync_failed",
                    json!({
                        "engine_id": self.config.engine_id,
                        "source": source.as_str(),
                        "error": error,
                        "engine_queue_age_us": observed_at.elapsed().as_micros(),
                    }),
                );
            }
        }
        self.reconcile_inventory_settlements(&reservations_before);
        self.evaluate_capital_allocator_shadow();
        self.evaluate_rebalance();
        self.refresh_phase(Instant::now());
        Ok(())
    }

    /// Applies the shared account owner's Binance health snapshot to a
    /// secondary strategy without publishing duplicate account telemetry or
    /// updating the process-wide inventory a second time.
    pub fn on_shared_binance_balance_event(&mut self, event: BalanceEvent) -> anyhow::Result<()> {
        match event {
            BalanceEvent::Binance(snapshot) => {
                self.binance_user_data_clean = true;
                self.state.balances.apply_binance(snapshot);
            }
            BalanceEvent::Failed {
                source: BalanceSource::Binance,
                ..
            } => self.state.balances.record_failure(BalanceSource::Binance),
            _ => anyhow::bail!("shared Binance balance publication received a non-Binance event"),
        }
        self.refresh_phase(Instant::now());
        Ok(())
    }

    pub fn take_rebalance_execution(&mut self) -> anyhow::Result<Option<RebalanceEvaluation>> {
        let Some(evaluation) = self.pending_rebalance.clone() else {
            return Ok(None);
        };
        if self
            .rebalance_inventory_reservations
            .contains_key(&evaluation.token_symbol)
        {
            tracing::info!(
                token = evaluation.token_symbol,
                "duplicate rebalance planning deferred while its inventory reservation settles"
            );
            self.telemetry.emit(
                "rebalance_inventory_deferred",
                json!({
                    "engine_id": self.config.engine_id,
                    "token": evaluation.token_symbol,
                    "reason": "active_token_reservation",
                }),
            );
            self.pending_rebalance = None;
            self.rebalance_pending_since = None;
            self.rebalance_inflight = false;
            self.rebalance_inflight_since = None;
            self.rebalance_deferred_reason = Some("active_token_reservation".to_owned());
            return Ok(None);
        }
        let action = evaluation
            .plan
            .action
            .as_ref()
            .context("rebalance execution has no action")?;
        let domain_snapshot = self.domain_config.snapshot();
        let executable_pair = domain_snapshot
            .pairs
            .iter()
            .find(|pair| pair.execution_enabled)
            .context("rebalance requires one executable pair")?;
        let pair_chain_id = executable_pair.chain.chain_id;
        let binance_location = self.binance_inventory_location()?;
        let wallet_location = self.wallet_inventory_location(pair_chain_id)?;
        let source_location = match action.direction {
            Direction::BinanceToWallet => binance_location.clone(),
            Direction::WalletToBinance => wallet_location.clone(),
        };
        let reservation_id =
            rebalance_reservation_id(&executable_pair.id, self.next_inventory_reservation);
        let next_inventory_reservation = self
            .next_inventory_reservation
            .checked_add(1)
            .context("inventory reservation sequence overflow")?;
        let request = ReservationRequest {
            operation_id: reservation_id.clone(),
            purpose: ReservationPurpose::Rebalance,
            claims: vec![InventoryClaim {
                key: self.portfolio_key(&source_location, &evaluation.token_symbol)?,
                amount: action.amount,
            }],
            settlement_locations: [binance_location, wallet_location].into_iter().collect(),
        };
        if let Err(error) = self.inventory.reserve(request) {
            let failure_kind = classify_inventory_admission_failure(&error);
            if failure_kind == InventoryAdmissionFailureKind::InvariantViolation {
                return Err(error);
            }
            tracing::info!(
                engine_id = %self.config.engine_id,
                pair_id = executable_pair.id,
                reservation_id,
                reason = failure_kind.telemetry_reason(),
                error = %format!("{error:#}"),
                "rebalance inventory reservation deferred"
            );
            self.telemetry.emit(
                "rebalance_inventory_deferred",
                json!({
                    "engine_id": self.config.engine_id,
                    "pair_id": executable_pair.id,
                    "operation_id": reservation_id,
                    "reason": failure_kind.telemetry_reason(),
                    "error": format!("{error:#}"),
                }),
            );
            self.rebalance_inflight = false;
            self.rebalance_inflight_since = None;
            self.rebalance_deferred_reason = Some(failure_kind.telemetry_reason().to_owned());
            return Ok(None);
        }
        self.next_inventory_reservation = next_inventory_reservation;
        self.pending_rebalance = None;
        self.rebalance_pending_since = None;
        self.rebalance_inflight = true;
        self.rebalance_inflight_since = Some(Instant::now());
        self.rebalance_deferred_reason = None;
        self.rebalance_inventory_reservations
            .insert(evaluation.token_symbol.clone(), reservation_id.clone());
        self.telemetry.emit(
            "inventory_reserved",
            json!({
                "engine_id": self.config.engine_id,
                "operation_id": reservation_id,
                "purpose": "rebalance",
                "inventory_location": source_location.stable_id(),
                "inventory_location_kind": source_location.kind_label(),
                "asset": evaluation.token_symbol,
                "amount_base_units": action.amount.to_string(),
            }),
        );
        Ok(Some(evaluation))
    }

    pub fn pending_rebalance_execution(&self) -> Option<&RebalanceEvaluation> {
        self.pending_rebalance.as_ref()
    }

    pub fn active_inventory_operation_count(&self) -> usize {
        self.inventory.active_operation_ids().len()
    }

    /// Rebuilds transient work from the latest in-memory balance plan.
    ///
    /// Rails gets this property from its recurring job: a temporary lock or
    /// queue conflict cannot consume the only planning edge. The Rust owner is
    /// event-driven, so the coordinator also calls this method from its
    /// independent supervisor tick. Pending work is never a durable stop and
    /// is always replaced by the newest plan before dispatch.
    pub fn refresh_pending_rebalance_execution(&mut self) {
        self.reconcile_owned_rebalance_reservations();
        if self.config.rebalance_execution_mode != "full_live"
            || self.rebalance_inflight
            || self.rebalance_settlement.is_some()
        {
            return;
        }
        let desired = self
            .rebalance
            .pending_action_excluding(&self.rebalance_blocked_tokens)
            .filter(|evaluation| {
                !self
                    .rebalance_inventory_reservations
                    .contains_key(&evaluation.token_symbol)
            });
        if self.pending_rebalance == desired {
            return;
        }
        let same_pending_intent = self
            .pending_rebalance
            .as_ref()
            .zip(desired.as_ref())
            .is_some_and(|(current, desired)| {
                current.token_symbol == desired.token_symbol
                    && current.plan.action.as_ref().map(|action| action.direction)
                        == desired.plan.action.as_ref().map(|action| action.direction)
            });
        let pending_since = if same_pending_intent {
            self.rebalance_pending_since
        } else {
            desired.as_ref().map(|_| Instant::now())
        };
        self.pending_rebalance = desired;
        self.rebalance_pending_since = pending_since;
        if !same_pending_intent {
            self.rebalance_deferred_reason = None;
        }
        if let Some(evaluation) = self.pending_rebalance.as_ref() {
            let action = evaluation
                .plan
                .action
                .as_ref()
                .expect("pending rebalance evaluations always contain an action");
            self.telemetry.emit(
                "rebalance_pending_refreshed",
                json!({
                    "engine_id": self.config.engine_id,
                    "token": evaluation.token_symbol,
                    "direction": format!("{:?}", action.direction),
                    "amount_base_units": action.amount.to_string(),
                }),
            );
        }
    }

    pub fn defer_pending_rebalance_execution(&mut self, reason: &str) {
        let reason_changed = self.rebalance_deferred_reason.as_deref() != Some(reason);
        self.rebalance_inflight = false;
        self.rebalance_inflight_since = None;
        self.rebalance_deferred_reason = Some(reason.to_owned());
        if !reason_changed {
            return;
        }
        self.telemetry.emit(
            "rebalance_dispatch_deferred",
            json!({
                "engine_id": self.config.engine_id,
                "token": self.pending_rebalance.as_ref().map(|evaluation| &evaluation.token_symbol),
                "reason": reason,
                "active_inventory_operation_count": self.active_inventory_operation_count(),
            }),
        );
    }

    pub fn cap_pending_rebalance_amount(&mut self, maximum: U256) -> anyhow::Result<()> {
        ensure!(!maximum.is_zero(), "rebalance dispatch maximum is zero");
        let Some(evaluation) = self.pending_rebalance.as_mut() else {
            return Ok(());
        };
        let action = evaluation
            .plan
            .action
            .as_mut()
            .context("pending rebalance evaluation has no action")?;
        action.amount = action.amount.min(maximum);
        ensure!(!action.amount.is_zero(), "bounded rebalance action is zero");
        Ok(())
    }

    pub async fn authorize_pending_rebalance_allocation(
        &self,
        transfer_fee: U256,
    ) -> anyhow::Result<Option<AllocationProposal>> {
        let Some(evaluation) = self.pending_rebalance.as_ref() else {
            return Ok(None);
        };
        let action = evaluation
            .plan
            .action
            .as_ref()
            .context("pending rebalance evaluation has no action")?;
        ensure!(
            transfer_fee <= action.amount,
            "rebalance transfer fee exceeds source debit"
        );
        let pair_chain_id = self
            .domain_config
            .snapshot()
            .pairs
            .iter()
            .find(|pair| pair.execution_enabled)
            .context("rebalance requires one executable pair")?
            .chain
            .chain_id;
        let binance = self.binance_inventory_location()?;
        let wallet = self.wallet_inventory_location(pair_chain_id)?;
        let (source_location, destination_location) = match action.direction {
            Direction::BinanceToWallet => (binance, wallet),
            Direction::WalletToBinance => (wallet, binance),
        };
        let source = self.portfolio_key(&source_location, &evaluation.token_symbol)?;
        let destination = self.portfolio_key(&destination_location, &evaluation.token_symbol)?;
        let intent = AllocationIntent {
            proposal_id: format!(
                "rebalance-allocation-{}-{}",
                evaluation.token_symbol,
                match action.direction {
                    Direction::BinanceToWallet => "binance-to-wallet",
                    Direction::WalletToBinance => "wallet-to-binance",
                }
            ),
            economic_asset_id: self
                .portfolio_catalog
                .economic_asset_id(&source)?
                .to_owned(),
            source,
            destination,
            destination_credit: action
                .amount
                .checked_sub(transfer_fee)
                .context("rebalance destination credit underflow")?,
            fee: transfer_fee,
        };
        let proposals = self
            .capital_allocator
            .plan(
                self.inventory.portfolio_snapshot(),
                Vec::new(),
                vec![intent],
            )
            .await?;
        ensure!(
            proposals.len() == 1,
            "capital allocator did not return exactly one rebalance proposal"
        );
        Ok(proposals.into_iter().next())
    }

    pub fn on_portfolio_wallet_snapshot(
        &mut self,
        snapshot: &crate::balances::WalletBalanceSnapshot,
    ) -> anyhow::Result<()> {
        ensure!(
            snapshot.batch_complete,
            "partial wallet batch cannot enter the portfolio owner"
        );
        let location = self.wallet_inventory_location(snapshot.chain_id)?;
        let balances = snapshot
            .token_balances
            .iter()
            .map(|balance| {
                self.portfolio_key(&location, balance.symbol.as_ref())
                    .map(|key| (key.venue_asset_id, balance.base_units))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        self.inventory
            .update_location(location.clone(), snapshot.block_number, balances)?;
        self.telemetry.emit(
            "portfolio_wallet_snapshot",
            json!({
                "engine_id": self.config.engine_id,
                "network_id": network_id(snapshot.chain_id),
                "inventory_location_id": location.stable_id(),
                "inventory_location_kind": location.kind_label(),
                "venue_asset_count": snapshot.token_balances.len(),
                "batch_complete": snapshot.batch_complete,
                "external_mutation_authorized": snapshot.chain_id == 480,
            }),
        );
        self.evaluate_capital_allocator_shadow();
        Ok(())
    }

    pub fn on_rebalance_recovery_started(
        &mut self,
        operation: &RebalanceExecutionOperation,
    ) -> anyhow::Result<()> {
        if self
            .rebalance_blocked_tokens
            .remove(&operation.intent.token_symbol)
        {
            tracing::info!(
                token = operation.intent.token_symbol,
                "rebalance token quarantine cleared when durable recovery reopened"
            );
            self.telemetry.emit(
                "rebalance_token_quarantine_cleared",
                json!({
                    "engine_id": self.config.engine_id,
                    "token": operation.intent.token_symbol,
                    "reason": "durable_recovery_reopened",
                    "blocked_token_count": self.rebalance_blocked_tokens.len(),
                }),
            );
        }
        self.pending_rebalance = None;
        self.rebalance_pending_since = None;
        self.rebalance_inflight = true;
        self.rebalance_inflight_since = Some(Instant::now());
        self.rebalance_deferred_reason = None;
        self.telemetry.emit(
            "rebalance_recovery_inflight",
            json!({
                "engine_id": self.config.engine_id,
                "operation_id": operation.intent.operation_id,
                "token": operation.intent.token_symbol,
                "direction": format!("{:?}", operation.intent.direction),
                "progress": format!("{:?}", operation.progress),
            }),
        );
        self.refresh_phase(Instant::now());
        Ok(())
    }

    pub fn on_rebalance_recovery_result(
        &mut self,
        result: Result<&RebalanceExecutionOperation, &str>,
        blocked_token: Option<&str>,
    ) -> anyhow::Result<()> {
        self.rebalance_inflight = false;
        self.rebalance_inflight_since = None;
        self.rebalance_deferred_reason = None;
        match result {
            Ok(operation) => {
                if self
                    .rebalance_blocked_tokens
                    .remove(&operation.intent.token_symbol)
                {
                    tracing::info!(
                        token = operation.intent.token_symbol,
                        "rebalance token quarantine cleared after durable recovery"
                    );
                    self.telemetry.emit(
                        "rebalance_token_quarantine_cleared",
                        json!({
                            "engine_id": self.config.engine_id,
                            "token": operation.intent.token_symbol,
                            "blocked_token_count": self.rebalance_blocked_tokens.len(),
                        }),
                    );
                }
                if matches!(
                    operation.progress,
                    RebalanceExecutionProgress::CancelledStale { .. }
                ) {
                    self.rebalance.mark_unbalanced();
                    tracing::info!(
                        operation_id = operation.intent.operation_id,
                        token = operation.intent.token_symbol,
                        "stale rebalance recovery cancelled after staged Binance inventory was restored"
                    );
                    self.telemetry.emit(
                        "rebalance_stale_intent_cancelled",
                        json!({
                            "engine_id": self.config.engine_id,
                            "operation_id": operation.intent.operation_id,
                            "token": operation.intent.token_symbol,
                            "recovered": true,
                        }),
                    );
                } else if let (Some(binance), Some(wallet)) = (
                    self.state.balances.binance.as_ref(),
                    self.state.balances.wallet.as_ref(),
                ) {
                    let settlement_locations = [
                        self.binance_inventory_location()?,
                        self.wallet_inventory_location(wallet.chain_id)?,
                    ];
                    self.rebalance_settlement = Some(RebalanceSettlementBarrier {
                        operation_id: operation.intent.operation_id.clone(),
                        strategy_id: operation
                            .intent
                            .scope
                            .as_ref()
                            .map_or("legacy", |scope| scope.strategy_id.as_str())
                            .to_owned(),
                        token_symbol: operation.intent.token_symbol.clone(),
                        direction: operation.intent.direction,
                        binance_after: binance.observed_at,
                        wallet_after: wallet.observed_at,
                        settlement_locations,
                        started_at: Instant::now(),
                    });
                }
                if !matches!(
                    operation.progress,
                    RebalanceExecutionProgress::CancelledStale { .. }
                ) {
                    self.telemetry.emit(
                        "rebalance_execution_completed",
                        json!({
                            "engine_id": self.config.engine_id,
                            "operation_id": operation.intent.operation_id,
                            "strategy_id": operation.intent.scope.as_ref().map(|scope| &scope.strategy_id),
                            "recovered": true,
                        }),
                    );
                    self.telemetry.emit(
                        "rebalance_settlement_waiting",
                        json!({
                            "engine_id": self.config.engine_id,
                            "operation_id": operation.intent.operation_id,
                            "token": operation.intent.token_symbol,
                            "direction": format!("{:?}", operation.intent.direction),
                            "recovered": true,
                        }),
                    );
                }
            }
            Err(error) => {
                if let Some(token) = blocked_token {
                    self.on_rebalance_token_quarantined(token, error)?;
                }
                self.rebalance.mark_unbalanced();
                tracing::error!(error, "rebalance recovery failed closed");
                self.telemetry.emit(
                    "rebalance_execution_failed",
                    json!({
                        "engine_id": self.config.engine_id,
                        "error": error,
                        "recovered": true,
                    }),
                );
            }
        }
        self.refresh_phase(Instant::now());
        Ok(())
    }

    pub fn on_rebalance_execution_result(
        &mut self,
        result: Result<&RebalanceExecutionOperation, &str>,
        blocked_token: Option<&str>,
    ) -> anyhow::Result<()> {
        self.rebalance_inflight = false;
        self.rebalance_inflight_since = None;
        self.rebalance_deferred_reason = None;
        match result {
            Ok(operation) => {
                let reservation_id = self
                    .rebalance_inventory_reservations
                    .get(&operation.intent.token_symbol)
                    .map(String::as_str)
                    .context("rebalance completed without an inventory reservation")?;
                self.inventory.mark_pending_settlement(reservation_id)?;
                if let (Some(binance), Some(wallet)) = (
                    self.state.balances.binance.as_ref(),
                    self.state.balances.wallet.as_ref(),
                ) {
                    let settlement_locations = [
                        self.binance_inventory_location()?,
                        self.wallet_inventory_location(wallet.chain_id)?,
                    ];
                    self.rebalance_settlement = Some(RebalanceSettlementBarrier {
                        operation_id: operation.intent.operation_id.clone(),
                        strategy_id: operation
                            .intent
                            .scope
                            .as_ref()
                            .map_or("legacy", |scope| scope.strategy_id.as_str())
                            .to_owned(),
                        token_symbol: operation.intent.token_symbol.clone(),
                        direction: operation.intent.direction,
                        binance_after: binance.observed_at,
                        wallet_after: wallet.observed_at,
                        settlement_locations,
                        started_at: Instant::now(),
                    });
                }
                self.telemetry.emit(
                    "rebalance_execution_completed",
                    json!({
                        "engine_id": self.config.engine_id,
                        "operation_id": operation.intent.operation_id,
                        "strategy_id": operation.intent.scope.as_ref().map(|scope| &scope.strategy_id),
                    }),
                );
                self.telemetry.emit(
                    "rebalance_settlement_waiting",
                    json!({
                        "engine_id": self.config.engine_id,
                        "operation_id": operation.intent.operation_id,
                        "token": operation.intent.token_symbol,
                        "direction": format!("{:?}", operation.intent.direction),
                    }),
                );
            }
            Err(error) => {
                if let Some(token) = blocked_token {
                    self.on_rebalance_token_quarantined(token, error)?;
                }
                self.rebalance.mark_unbalanced();
                tracing::error!(error, "rebalance executor failed closed");
                self.telemetry.emit(
                    "rebalance_execution_failed",
                    json!({
                        "engine_id": self.config.engine_id,
                        "error": error,
                    }),
                );
            }
        }
        self.refresh_phase(Instant::now());
        Ok(())
    }

    pub fn on_rebalance_token_quarantined(
        &mut self,
        token: &str,
        reason: &str,
    ) -> anyhow::Result<()> {
        self.token_decimals(token)?;
        if self.rebalance_blocked_tokens.insert(token.to_owned()) {
            tracing::error!(
                token,
                reason,
                "rebalance token quarantined; other tokens remain eligible"
            );
            self.telemetry.emit(
                "rebalance_token_quarantined",
                json!({
                    "engine_id": self.config.engine_id,
                    "token": token,
                    "reason": reason,
                    "blocked_token_count": self.rebalance_blocked_tokens.len(),
                }),
            );
        }
        Ok(())
    }

    fn token_decimals(&self, symbol: &str) -> anyhow::Result<u8> {
        if let Some(decimals) = self.binance_asset_decimals.get(symbol) {
            return Ok(*decimals);
        }
        let domain = self.domain_config.snapshot();
        domain
            .pairs
            .iter()
            .flat_map(|pair| [&pair.token_a, &pair.token_b])
            .find(|token| token.symbol == symbol)
            .map(|token| token.decimals)
            .or_else(|| {
                domain.pairs.iter().find_map(|pair| {
                    (pair.binance.commission_asset.as_deref() == Some(symbol))
                        .then_some(pair.binance.commission_asset_decimals)
                        .flatten()
                })
            })
            .with_context(|| format!("no configured decimals for inventory asset {symbol}"))
    }

    fn binance_inventory_location(&self) -> anyhow::Result<InventoryLocation> {
        InventoryLocation::binance(PRIMARY_BINANCE_ACCOUNT_ID)
    }

    fn wallet_inventory_location(&self, chain_id: u64) -> anyhow::Result<InventoryLocation> {
        InventoryLocation::evm_wallet(network_id(chain_id), wallet_location_id(chain_id))
    }

    fn portfolio_key(
        &self,
        location: &InventoryLocation,
        symbol: &str,
    ) -> anyhow::Result<InventoryKey> {
        self.portfolio_catalog.key(location, symbol)
    }

    fn reconcile_inventory_settlements(&mut self, reservations_before: &[String]) {
        for operation_id in reservations_before {
            if self.inventory.reservation(operation_id).is_some() {
                continue;
            }
            if let Some(terminal_observed_at) = self.terminal_child_observed_at.remove(operation_id)
            {
                self.emit_child_terminal_to_reservation_settled(operation_id, terminal_observed_at);
            }
            self.telemetry.emit(
                "inventory_settlement_reconciled",
                json!({
                    "engine_id": self.config.engine_id,
                    "operation_id": operation_id,
                }),
            );
            self.rebalance_inventory_reservations
                .retain(|_, reservation_id| reservation_id != operation_id);
        }
    }

    fn reconcile_owned_rebalance_reservations(&mut self) {
        let settled = settled_owned_rebalance_reservations(
            &self.inventory,
            &mut self.rebalance_inventory_reservations,
        );
        for (token, operation_id) in settled {
            self.telemetry.emit(
                "rebalance_owner_reservation_reconciled",
                json!({
                    "engine_id": self.config.engine_id,
                    "operation_id": operation_id,
                    "token": token,
                    "reason": "shared_inventory_settled",
                }),
            );
        }
    }

    fn emit_child_terminal_to_reservation_settled(
        &self,
        plan_id: &str,
        terminal_observed_at: Instant,
    ) {
        self.telemetry.emit(
            crate::telemetry::ARBITRAGE_EXECUTION_STAGE_KIND,
            json!({
                "engine_id": self.config.engine_id,
                "venue": "orchestrator",
                "plan_id": plan_id,
                "operation_id": plan_id,
                "stage": "child_terminal_to_reservation_settled",
                "duration_us": duration_us(terminal_observed_at.elapsed()),
                "outcome": "success",
            }),
        );
    }

    fn evaluate_rebalance(&mut self) {
        let calculation_started_at = Instant::now();
        let Some(binance) = self.state.balances.binance.as_ref() else {
            return;
        };
        let Some(wallet) = self.state.balances.wallet.as_ref() else {
            return;
        };
        if self
            .rebalance_settlement
            .as_ref()
            .is_some_and(|barrier| barrier.reconciled(binance.observed_at, wallet.observed_at))
            && let Some(barrier) = self.rebalance_settlement.take()
        {
            self.telemetry.emit(
                "rebalance_settlement_reconciled",
                json!({
                    "engine_id": self.config.engine_id,
                    "operation_id": barrier.operation_id,
                    "strategy_id": barrier.strategy_id,
                    "token": barrier.token_symbol,
                    "direction": format!("{:?}", barrier.direction),
                    "settlement_duration_us": duration_us(barrier.started_at.elapsed()),
                    "settlement_locations": barrier
                        .settlement_locations
                        .iter()
                        .map(InventoryLocation::stable_id)
                        .collect::<Vec<_>>(),
                }),
            );
        }
        if let Some(reason) = rebalance_planning_deferred_reason(
            self.rebalance_inflight,
            self.rebalance_settlement.is_some(),
        ) {
            self.telemetry.emit(
                "capital_allocation_evaluated",
                json!({
                    "engine_id": self.config.engine_id,
                    "allocator_mode": "v12_rebalance_compatibility",
                    "calculation_validation_us": duration_us(calculation_started_at.elapsed()),
                    "proposal_count": 0,
                    "outcome": "deferred",
                    "reason": reason,
                }),
            );
            return;
        }
        match self.rebalance.evaluate(binance, wallet) {
            Ok(evaluations) => {
                let calculation_us = duration_us(calculation_started_at.elapsed());
                self.telemetry.emit(
                    "capital_allocation_evaluated",
                    json!({
                        "engine_id": self.config.engine_id,
                        "allocator_mode": "v12_rebalance_compatibility",
                        "calculation_validation_us": calculation_us,
                        "proposal_count": evaluations.len(),
                        "outcome": "success",
                    }),
                );
                let mode = self.config.rebalance_execution_mode.as_str();
                for evaluation in evaluations {
                    let action = evaluation.plan.action.as_ref();
                    self.telemetry.emit(
                        "rebalance_plan_evaluated",
                        json!({
                            "engine_id": self.config.engine_id,
                            "calculation_validation_us": calculation_us,
                            "mode": mode,
                            "token": evaluation.token_symbol,
                            "token_decimals": evaluation.token_decimals,
                            "reference_captured": evaluation.reference_captured,
                            "reference_inventory_base_units": evaluation.plan.reference_inventory.to_string(),
                            "start_balance_base_units": evaluation.plan.start_balance.to_string(),
                            "binance_balance_base_units": evaluation.plan.projected.binance.to_string(),
                            "wallet_balance_base_units": evaluation.plan.projected.wallet.to_string(),
                            "binance_target_base_units": evaluation.plan.binance_target.to_string(),
                            "wallet_target_base_units": evaluation.plan.wallet_target.to_string(),
                            "action_direction": action.map(|action| format!("{:?}", action.direction)),
                            "action_amount_base_units": action.map(|action| action.amount.to_string()),
                            "action_route": action.map(|action| format!("{:?}", action.route)),
                        }),
                    );
                }
                self.refresh_pending_rebalance_execution();
            }
            Err(error) => {
                self.telemetry.emit(
                    "capital_allocation_evaluated",
                    json!({
                        "engine_id": self.config.engine_id,
                        "allocator_mode": "v12_rebalance_compatibility",
                        "calculation_validation_us": duration_us(calculation_started_at.elapsed()),
                        "proposal_count": 0,
                        "outcome": "failed",
                    }),
                );
                self.rebalance.mark_unbalanced();
                tracing::warn!(error = %error, "rebalance planning failed closed");
                self.telemetry.emit(
                    "rebalance_plan_failed",
                    json!({
                        "engine_id": self.config.engine_id,
                        "mode": self.config.rebalance_execution_mode,
                        "error": format!("{error:#}"),
                    }),
                );
            }
        }
    }

    fn evaluate_capital_allocator_shadow(&self) {
        self.capital_allocator
            .submit_snapshot(self.inventory.portfolio_snapshot());
    }

    fn on_binance_quote(
        &mut self,
        quote: TopOfBook,
        depth: Option<&SpotDepthBook>,
    ) -> anyhow::Result<()> {
        let result = self.state.apply_quote(quote.clone());
        match result {
            QuoteApplyResult::Accepted => {
                self.entry_preflight.update_quote(&quote);
                self.record_depth_health(&quote, depth, Instant::now())?;
                // The decision is evaluated only after all readiness inputs are
                // fresh. The calculation itself performs no RPC, I/O, or locks.
                self.refresh_phase(Instant::now());
                let decision_outcome = if self.state.phase == RuntimePhase::Ready {
                    if self.uses_dex_first_fast_path() {
                        if self.evaluate_ready_quote(
                            &quote,
                            "binance_book_ticker",
                            Some(AdmissionLiquidity::DexFirstTop),
                            depth,
                        )? {
                            "evaluated"
                        } else {
                            "ready_without_pair_evaluation"
                        }
                    } else if let Some(depth) = depth.filter(|depth| {
                        depth.matches_top(
                            quote.symbol.as_ref(),
                            quote.update_id,
                            quote.bid_price,
                            quote.bid_quantity,
                            quote.ask_price,
                            quote.ask_quantity,
                        )
                    }) {
                        if self.evaluate_sequence_matched_quote(
                            &quote,
                            "binance_book_ticker",
                            depth,
                        )? {
                            "evaluated"
                        } else {
                            "sequence_matched_update_already_evaluated"
                        }
                    } else {
                        self.telemetry.emit(
                            "binance_book_depth_mismatch",
                            json!({
                                "engine_id": self.config.engine_id,
                                "product": "spot",
                                "symbol": quote.symbol.as_ref(),
                                "book_ticker_update_id": quote.update_id,
                                "reason": "sequence_or_top_level_mismatch",
                            }),
                        );
                        "depth_mismatch"
                    }
                } else {
                    "runtime_not_ready"
                };

                // Raw market telemetry is deliberately serialized only after
                // the opportunity decision. It must never delay detection or
                // eventual order submission.
                self.hot_telemetry.emit_binance_book(
                    &quote,
                    "strategy_price",
                    Some(self.state.phase),
                    decision_outcome,
                );
            }
            rejected => self.telemetry.emit(
                "binance_book_ticker_rejected",
                json!({
                    "engine_id": self.config.engine_id,
                    "product": "spot",
                    "symbol": quote.symbol.as_ref(),
                    "update_id": quote.update_id,
                    "connection_generation": quote.connection_generation,
                    "reason": format!("{rejected:?}"),
                }),
            ),
        }
        Ok(())
    }

    fn evaluate_sequence_matched_quote(
        &mut self,
        quote: &TopOfBook,
        trigger: &'static str,
        depth: &SpotDepthBook,
    ) -> anyhow::Result<bool> {
        if !mark_sequence_matched_update(
            &mut self.last_sequence_matched_quote_update,
            quote.symbol.as_ref(),
            quote.update_id,
        ) {
            return Ok(false);
        }
        self.evaluate_ready_quote(
            quote,
            trigger,
            Some(AdmissionLiquidity::FullDepth(depth)),
            Some(depth),
        )
    }

    fn matching_cached_depth(&self, quote: &TopOfBook) -> Option<&SpotDepthBook> {
        self.latest_sequence_matched_depth
            .get(quote.symbol.as_ref())
            .filter(|depth| {
                depth.matches_top(
                    quote.symbol.as_ref(),
                    quote.update_id,
                    quote.bid_price,
                    quote.bid_quantity,
                    quote.ask_price,
                    quote.ask_quantity,
                )
            })
    }

    fn evaluate_ready_quote(
        &mut self,
        quote: &TopOfBook,
        trigger: &'static str,
        admission: Option<AdmissionLiquidity<'_>>,
        adaptive_depth: Option<&SpotDepthBook>,
    ) -> anyhow::Result<bool> {
        let calculation_started = Instant::now();
        if let Some(evaluation) = self.opportunities.evaluate(quote)? {
            self.hot_telemetry.emit_evaluation(
                quote,
                evaluation,
                self.dex.latest_head().number,
                calculation_started.elapsed().as_micros(),
                STRATEGY_BASELINE_CALCULATION_BUDGET_US,
                trigger,
            );
            if let Some(admission) = admission {
                self.submit_paper_opportunity(
                    quote,
                    evaluation,
                    admission,
                    adaptive_depth,
                    trigger,
                    calculation_started,
                )?;
            }
            return Ok(true);
        }
        Ok(false)
    }

    fn uses_dex_first_fast_path(&self) -> bool {
        matches!(
            self.config.arbitrage_execution_mode.as_str(),
            "full_live" | "paper_dex_first"
        )
    }

    fn record_depth_health(
        &mut self,
        quote: &TopOfBook,
        depth: Option<&SpotDepthBook>,
        now: Instant,
    ) -> anyhow::Result<()> {
        let pair_config = self
            .domain_config
            .snapshot()
            .pairs
            .iter()
            .find(|pair| pair.binance.symbol == quote.symbol.as_ref())
            .context("depth health symbol is absent from domain config")?;
        let limits = AdaptiveSizingRuntimeLimits::parse(&pair_config.adaptive_sizing)?;
        let observation = self.depth_observation(quote, depth, now);
        let health = classify_depth_health(observation, depth.is_some(), limits);
        self.depth_health_by_symbol
            .insert(quote.symbol.to_string(), health);
        Ok(())
    }

    fn depth_observation(
        &self,
        quote: &TopOfBook,
        depth: Option<&SpotDepthBook>,
        now: Instant,
    ) -> DepthObservation {
        let feed = self.state.binance_feeds.get(quote.symbol.as_ref());
        let age_ms = depth.and_then(|depth| {
            feed.and_then(|feed| {
                (feed.depth_update_id == Some(depth.last_update_id()))
                    .then_some(feed.depth_received_at)
                    .flatten()
            })
            .map(|received_at| {
                u64::try_from(now.saturating_duration_since(received_at).as_millis())
                    .unwrap_or(u64::MAX)
            })
        });
        let update_delta = depth.map(|depth| quote.update_id.abs_diff(depth.last_update_id()));
        let top_mismatch_reason = depth_top_mismatch_reason(quote, depth);
        DepthObservation {
            age_ms,
            update_delta,
            top_matches: top_mismatch_reason.is_none(),
            top_mismatch_reason,
        }
    }

    fn adaptive_sizing_snapshot(&self) -> AdaptiveSizingSnapshot {
        AdaptiveSizingSnapshot {
            opportunities: self.opportunities.clone(),
            domain_config: Arc::clone(&self.domain_config),
            telemetry: self.telemetry.clone(),
            engine_id: self.config.engine_id.clone(),
        }
    }
}

impl AdaptiveSizingSnapshot {
    fn evaluate_adaptive_sizing(
        &self,
        quote: &TopOfBook,
        evaluation: PairEvaluation,
        limits: AdaptiveSizingRuntimeLimits,
        evaluation_trigger: &'static str,
    ) -> anyhow::Result<Option<AdaptiveCandidate>> {
        let started = Instant::now();
        let pair = self.opportunities.pair(evaluation.pair_index)?;
        let pair_config = self
            .domain_config
            .snapshot()
            .pairs
            .iter()
            .find(|config| config.id == pair.pair_id)
            .context("adaptive sizing pair is absent from domain config")?;
        let directions = [evaluation.dex_buy_cex_sell, evaluation.cex_buy_dex_sell];
        let mut baseline_by_direction: [Option<AdaptiveCandidate>; 2] = [None, None];
        let mut winner: Option<AdaptiveCandidate> = None;
        let mut exact_evaluations = 0_u16;
        let mut limit_exhausted = false;
        let mut rejection_counts: BTreeMap<&'static str, u32> = BTreeMap::new();

        for (direction_index, direction_evaluation) in directions.into_iter().enumerate() {
            let Some(baseline_trade) = direction_evaluation
                .baseline
                .filter(|trade| trade.meets_threshold)
            else {
                continue;
            };
            baseline_by_direction[direction_index] = Some(AdaptiveCandidate {
                direction: direction_evaluation.direction,
                trade: baseline_trade,
                trade_notional: adaptive_trade_notional(
                    direction_evaluation.direction,
                    baseline_trade,
                ),
            });

            for &pool_index in pair.selectable_pool_indices() {
                let (pool_winner, search) = self.search_adaptive_pool(
                    quote,
                    evaluation.pair_index,
                    direction_evaluation.direction,
                    pool_index,
                    baseline_trade.token_b_amount,
                    limits,
                )?;
                exact_evaluations = exact_evaluations.saturating_add(search.exact_evaluations);
                limit_exhausted |= search.limit_exhausted;
                for (reason, count) in search.rejection_counts {
                    *rejection_counts.entry(reason).or_default() += count;
                }
                if let Some(candidate) = pool_winner
                    && winner
                        .as_ref()
                        .is_none_or(|current| adaptive_candidate_is_better(candidate, *current))
                {
                    winner = Some(candidate);
                }
            }
        }

        let direction_index = |direction| match direction {
            ArbitrageDirection::BuyTokenBOnDexSellOnCex => 0,
            ArbitrageDirection::BuyTokenBOnCexSellOnDex => 1,
        };
        let mut fallback_reason = "no_eligible_candidate";
        let mut selected = winner.filter(|candidate| {
            let Some(baseline) = baseline_by_direction[direction_index(candidate.direction)] else {
                fallback_reason = "baseline_unavailable";
                return false;
            };
            if candidate.trade.token_b_amount <= baseline.trade.token_b_amount {
                fallback_reason = "not_larger_than_baseline";
                return false;
            }
            true
        });
        if limit_exhausted {
            fallback_reason = "evaluation_limit";
            selected = None;
        }
        let baseline = selected
            .and_then(|candidate| baseline_by_direction[direction_index(candidate.direction)])
            .or_else(|| {
                baseline_by_direction
                    .into_iter()
                    .flatten()
                    .max_by_key(|candidate| candidate.trade_notional)
            });
        let selected_for_telemetry = selected.or(baseline);
        let execution_candidate = matches!(
            pair_config.adaptive_sizing,
            AdaptiveSizingConfig::Adaptive { .. }
        )
        .then_some(selected)
        .flatten();
        let mut shadow_candidates = Vec::new();
        for direction in [evaluation.dex_buy_cex_sell, evaluation.cex_buy_dex_sell] {
            let token_b_amount = direction
                .baseline
                .map_or(evaluation.baseline_token_b_amount, |trade| {
                    trade.token_b_amount
                });
            for &pool_index in pair.shadow_pool_indices() {
                let trade = self.opportunities.evaluate_exact_candidate(
                    evaluation.pair_index,
                    quote,
                    direction.direction,
                    pool_index,
                    token_b_amount,
                )?;
                shadow_candidates.push(json!({
                    "dex_protocol": "pancakeswap_v3",
                    "pool_index": pool_index,
                    "direction": direction.direction.as_str(),
                    "token_b_base_units": token_b_amount.to_string(),
                    "quotable": trade.is_some(),
                    "meets_threshold": trade.map(|candidate| candidate.meets_threshold),
                    "gross_profit_bps_x100": trade.map(|candidate| candidate.gross_profit_bps_x100.to_string()),
                    "cost_token_a_base_units": trade.map(|candidate| candidate.cost_token_a.to_string()),
                    "proceeds_token_a_base_units": trade.map(|candidate| candidate.proceeds_token_a.to_string()),
                }));
            }
        }
        let rejection_counts = rejection_counts
            .into_iter()
            .map(|(reason, count)| (reason.to_owned(), Value::from(count)))
            .collect::<serde_json::Map<_, _>>();
        let calculation_us = started.elapsed().as_micros();
        let mut payload = json!({
            "engine_id": self.engine_id,
            "pair_id": pair.pair_id,
            "strategy_id": strategy_id(&pair.pair_id),
            "binance_account_id": PRIMARY_BINANCE_ACCOUNT_ID,
            "instrument_id": instrument_id(&pair.symbol),
            "network_id": network_id(pair.chain_id),
            "symbol": quote.symbol.as_ref(),
            "update_id": quote.update_id,
            "configured_mode": pair_config.adaptive_sizing.mode(),
            "optimizer_version": ADAPTIVE_OPTIMIZER_VERSION,
            "search_mode": "maximum_monotone_whole_step",
            "max_exact_evaluations_per_pool": MAX_ADAPTIVE_EXACT_EVALUATIONS,
            "exact_evaluation_count": exact_evaluations,
            "max_trade_notional_token_a_base_units": limits.max_trade_notional.to_string(),
            "baseline_direction": baseline.map(|candidate| candidate.direction.as_str()),
            "baseline_pool_index": baseline.map(|candidate| candidate.trade.pool_index),
            "baseline_token_b_base_units": baseline.map(|candidate| candidate.trade.token_b_amount.to_string()),
            "baseline_cost_token_a_base_units": baseline.map(|candidate| candidate.trade.cost_token_a.to_string()),
            "baseline_proceeds_token_a_base_units": baseline.map(|candidate| candidate.trade.proceeds_token_a.to_string()),
            "baseline_gross_profit_bps_x100": baseline.map(|candidate| candidate.trade.gross_profit_bps_x100.to_string()),
            "evaluation_trigger": evaluation_trigger,
            "selected_sizing_mode": if selected.is_some() { "adaptive" } else { "baseline" },
            "selected_direction": selected_for_telemetry.map(|candidate| candidate.direction.as_str()),
            "selected_pool_index": selected_for_telemetry.map(|candidate| candidate.trade.pool_index),
            "selected_token_b_base_units": selected_for_telemetry.map(|candidate| candidate.trade.token_b_amount.to_string()),
            "selected_cost_token_a_base_units": selected_for_telemetry.map(|candidate| candidate.trade.cost_token_a.to_string()),
            "selected_proceeds_token_a_base_units": selected_for_telemetry.map(|candidate| candidate.trade.proceeds_token_a.to_string()),
            "selected_dex_amount_in_base_units": selected_for_telemetry.map(|candidate| candidate.trade.dex_amount_in.to_string()),
            "selected_dex_amount_out_minimum_base_units": selected_for_telemetry.map(|candidate| candidate.trade.dex_amount_out_minimum.to_string()),
            "selected_trade_notional_token_a_base_units": selected_for_telemetry.map(|candidate| candidate.trade_notional.to_string()),
            "selected_execution_slippage_bps": selected_for_telemetry.map(|candidate| candidate.trade.execution_slippage_bps),
            "selected_gross_profit_bps_x100": selected_for_telemetry.map(|candidate| candidate.trade.gross_profit_bps_x100.to_string()),
            "fallback_reason": selected.is_none().then_some(fallback_reason),
            "rejection_counts": Value::Object(rejection_counts),
            "calculation_us": calculation_us,
            "execution_size_changed": execution_candidate.is_some(),
            "shadow_candidates": shadow_candidates,
        });
        let object = payload
            .as_object_mut()
            .expect("adaptive sizing telemetry payload is an object");
        object.insert(
            "configured_max_trade_notional_token_a_base_units".to_owned(),
            json!(
                pair_config
                    .adaptive_sizing
                    .limits()
                    .map(|limits| limits.max_trade_notional)
            ),
        );
        object.insert(
            "sizing_inputs".to_owned(),
            json!([
                "dex_curve",
                "binance_price",
                "gross_20_bps",
                "execution_slippage",
                "trade_cap"
            ]),
        );
        self.telemetry
            .emit("arbitrage_adaptive_sizing_evaluated", payload);
        Ok(execution_candidate)
    }

    fn adaptive_probe(
        &self,
        search: &mut AdaptivePoolSearch,
        quote: &TopOfBook,
        pair_index: usize,
        direction: ArbitrageDirection,
        pool_index: usize,
        token_b_amount: U256,
    ) -> anyhow::Result<AdaptiveProbe> {
        if let Some((_, probe)) = search
            .cached_probes
            .iter()
            .find(|(amount, _)| *amount == token_b_amount)
        {
            return Ok(*probe);
        }
        if search.exact_evaluations >= MAX_ADAPTIVE_EXACT_EVALUATIONS {
            search.limit_exhausted = true;
            return Ok(AdaptiveProbe {
                candidate: None,
                rejection: Some("evaluation_limit"),
            });
        }
        search.exact_evaluations += 1;
        let rejection = |reason| AdaptiveProbe {
            candidate: None,
            rejection: Some(reason),
        };
        let Some(trade) = self.opportunities.evaluate_exact_candidate(
            pair_index,
            quote,
            direction,
            pool_index,
            token_b_amount,
        )?
        else {
            let probe = rejection("dex_liquidity");
            search.record(token_b_amount, probe);
            return Ok(probe);
        };
        if !trade.meets_threshold {
            let probe = rejection("gross_threshold");
            search.record(token_b_amount, probe);
            return Ok(probe);
        }
        let trade_notional = adaptive_trade_notional(direction, trade);
        if trade_notional > search.max_trade_notional {
            let probe = rejection("trade_cap");
            search.record(token_b_amount, probe);
            return Ok(probe);
        }
        let probe = AdaptiveProbe {
            candidate: Some(AdaptiveCandidate {
                direction,
                trade,
                trade_notional,
            }),
            rejection: None,
        };
        search.record(token_b_amount, probe);
        Ok(probe)
    }

    fn search_adaptive_pool(
        &self,
        quote: &TopOfBook,
        pair_index: usize,
        direction: ArbitrageDirection,
        pool_index: usize,
        baseline_amount: U256,
        limits: AdaptiveSizingRuntimeLimits,
    ) -> anyhow::Result<(Option<AdaptiveCandidate>, AdaptivePoolSearch)> {
        let step = self.opportunities.pair(pair_index)?.token_b_step();
        let low_steps = baseline_amount / step;
        let mut search = AdaptivePoolSearch::new(limits.max_trade_notional);
        if low_steps == U256::ZERO {
            return Ok((None, search));
        }
        let probe_steps = |steps: U256, search: &mut AdaptivePoolSearch| {
            let amount = steps
                .checked_mul(step)
                .context("adaptive candidate amount overflow")?;
            self.adaptive_probe(search, quote, pair_index, direction, pool_index, amount)
        };
        let Some(low_candidate) = probe_steps(low_steps, &mut search)?.candidate else {
            return Ok((None, search));
        };

        // Feasibility is monotone for one prepared pool curve: increasing the
        // amount can only consume more DEX liquidity/slippage and notional.
        // Find an exclusive rejected bound, then return the largest whole step
        // that still clears the gross spread and execution slippage contract.
        let mut low = low_steps;
        let mut high = low_steps
            .checked_mul(U256::from(2_u8))
            .context("adaptive upper step overflow")?;
        while probe_steps(high, &mut search)?.candidate.is_some() {
            low = high;
            high = high
                .checked_mul(U256::from(2_u8))
                .context("adaptive upper step overflow")?;
        }
        while high - low > U256::ONE {
            let mid = low + ((high - low) / U256::from(2_u8));
            if probe_steps(mid, &mut search)?.candidate.is_some() {
                low = mid;
            } else {
                high = mid;
            }
        }
        let mut winner = low_candidate;
        if let Some(candidate) = probe_steps(low, &mut search)?.candidate {
            winner = candidate;
        }
        Ok((Some(winner), search))
    }
}

impl TradingEngine {
    fn submit_paper_opportunity(
        &mut self,
        quote: &TopOfBook,
        evaluation: PairEvaluation,
        admission_liquidity: AdmissionLiquidity<'_>,
        depth: Option<&SpotDepthBook>,
        evaluation_trigger: &'static str,
        evaluation_started_at: Instant,
    ) -> anyhow::Result<bool> {
        self.submit_paper_opportunity_inner(
            quote,
            evaluation,
            admission_liquidity,
            depth,
            evaluation_trigger,
            evaluation_started_at,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn submit_paper_opportunity_inner(
        &mut self,
        quote: &TopOfBook,
        evaluation: PairEvaluation,
        admission_liquidity: AdmissionLiquidity<'_>,
        depth: Option<&SpotDepthBook>,
        evaluation_trigger: &'static str,
        evaluation_started_at: Instant,
        completed_adaptive: Option<CompletedAdaptiveSizing>,
    ) -> anyhow::Result<bool> {
        let admission_started = Instant::now();
        let Some(handle) = self.paper_trades.clone() else {
            return Ok(false);
        };
        let pair = self.opportunities.pair(evaluation.pair_index)?;
        let pair_id = pair.pair_id.clone();
        let pair_symbol = pair.symbol.clone();
        let pair_chain_id = pair.chain_id;
        let pair_config = self
            .domain_config
            .snapshot()
            .pairs
            .iter()
            .find(|config| config.id == pair_id)
            .context("paper opportunity pair is absent from domain config")?;
        let price_unchanged_for = quote.received_at.elapsed();
        let max_transport_silence_ms = pair_config.strategy.max_transport_silence_ms();
        if !self.state.binance_symbol_price_ready(
            quote.symbol.as_ref(),
            Instant::now(),
            max_transport_silence_ms,
        ) {
            self.telemetry.emit(
                "arbitrage_admission_rejected",
                json!({
                    "engine_id": self.config.engine_id,
                    "pair_id": pair_id,
                    "symbol": quote.symbol.as_ref(),
                    "update_id": quote.update_id,
                    "reason": "binance_transport_unavailable",
                    "evaluation_trigger": evaluation_trigger,
                    "price_unchanged_for_ms": duration_us(price_unchanged_for) / 1_000,
                    "max_transport_silence_ms": max_transport_silence_ms,
                    "trigger_to_rejection_us": duration_us(evaluation_started_at.elapsed()),
                }),
            );
            return Ok(false);
        }
        let adaptive_limits = completed_adaptive
            .as_ref()
            .map(|completed| Some(completed.limits))
            .unwrap_or(AdaptiveSizingRuntimeLimits::parse(
                &pair_config.adaptive_sizing,
            )?);
        let live_baseline_meets_threshold = evaluation
            .dex_buy_cex_sell
            .baseline
            .is_some_and(|trade| trade.meets_threshold)
            || evaluation
                .cex_buy_dex_sell
                .baseline
                .is_some_and(|trade| trade.meets_threshold);
        if !live_baseline_meets_threshold && pair.shadow_pool_indices().is_empty() {
            return Ok(false);
        }
        let token_b_decimals = pair.token_b_decimals;
        let baseline_token_a = pair.baseline_token_a();
        let token_b_step = pair.token_b_step();
        if completed_adaptive.is_none()
            && let Some(limits) = adaptive_limits
        {
            let pool_generations = pair
                .selectable_pool_indices()
                .iter()
                .copied()
                .map(|pool_index| {
                    self.opportunities
                        .pool_generation(pool_index)
                        .map(|generation| (pool_index, generation))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            let pending = PendingAdaptiveAdmission {
                quote: quote.clone(),
                evaluation,
                admission_liquidity: match admission_liquidity {
                    AdmissionLiquidity::DexFirstTop => OwnedAdmissionLiquidity::DexFirstTop,
                    AdmissionLiquidity::FullDepth(depth) => {
                        OwnedAdmissionLiquidity::FullDepth(depth.clone())
                    }
                },
                depth: depth.cloned(),
                evaluation_trigger,
                evaluation_started_at,
            };
            let snapshot_started = Instant::now();
            let snapshot = self.adaptive_sizing_snapshot();
            let snapshot_time_us = duration_us(snapshot_started.elapsed());
            self.pending_adaptive_sizing.push(AdaptiveSizingJob {
                snapshot,
                pending,
                limits,
                pool_generations,
                snapshot_time_us,
                queued_at: Instant::now(),
            });
            return Ok(false);
        }
        let adaptive_candidate = match completed_adaptive.map(|completed| completed.result) {
            Some(Ok(candidate)) => candidate,
            Some(Err(error)) => {
                let fallback_baseline = [evaluation.dex_buy_cex_sell, evaluation.cex_buy_dex_sell]
                    .into_iter()
                    .filter_map(|direction| {
                        direction
                            .baseline
                            .filter(|trade| trade.meets_threshold)
                            .map(|trade| (direction.direction, trade))
                    })
                    .max_by_key(|(direction, trade)| adaptive_trade_notional(*direction, *trade));
                self.telemetry.emit(
                    "arbitrage_adaptive_sizing_evaluated",
                    json!({
                        "engine_id": self.config.engine_id,
                        "pair_id": pair_id,
                        "strategy_id": strategy_id(&pair_id),
                        "binance_account_id": PRIMARY_BINANCE_ACCOUNT_ID,
                        "instrument_id": instrument_id(&pair_symbol),
                        "network_id": network_id(pair_chain_id),
                        "symbol": quote.symbol.as_ref(),
                        "update_id": quote.update_id,
                        "optimizer_version": ADAPTIVE_OPTIMIZER_VERSION,
                        "evaluation_trigger": evaluation_trigger,
                        "baseline_pool_index": fallback_baseline.map(|(_, trade)| trade.pool_index),
                        "baseline_token_b_base_units": fallback_baseline.map(|(_, trade)| trade.token_b_amount.to_string()),
                        "baseline_gross_profit_bps_x100": fallback_baseline.map(|(_, trade)| trade.gross_profit_bps_x100.to_string()),
                        "selected_sizing_mode": "baseline",
                        "fallback_reason": "optimizer_error",
                        "error": format!("{error:#}"),
                        "execution_size_changed": false,
                    }),
                );
                None
            }
            None => None,
        };

        let execution_depth_health = classify_depth_health(
            self.depth_observation(quote, depth, Instant::now()),
            depth.is_some(),
            adaptive_limits,
        );

        let mut candidates = Vec::with_capacity(2);
        if let Some(candidate) = adaptive_candidate {
            candidates.push((
                adaptive_trade_direction(candidate.direction),
                candidate.trade,
            ));
        }
        for direction in adaptive_candidate
            .is_none()
            .then_some([evaluation.dex_buy_cex_sell, evaluation.cex_buy_dex_sell])
            .into_iter()
            .flatten()
        {
            // Rails executes the fixed token-A minimum-buy baseline. The
            // larger market-liquidity capacity remains telemetry only and must
            // not silently change the comparison's order size.
            let Some(trade) = direction.baseline.filter(|trade| trade.meets_threshold) else {
                continue;
            };
            ensure!(
                trade.cost_token_a <= baseline_token_a.saturating_mul(U256::from(2_u8)),
                "baseline trade escaped the two-times token-A safety envelope"
            );
            let trade_direction = match direction.direction {
                ArbitrageDirection::BuyTokenBOnDexSellOnCex => {
                    TradeDirection::BuyTokenBOnDexSellOnCex
                }
                ArbitrageDirection::BuyTokenBOnCexSellOnDex => {
                    TradeDirection::BuyTokenBOnCexSellOnDex
                }
            };
            candidates.push((trade_direction, trade));
        }
        let candidate = candidates
            .into_iter()
            .max_by_key(|(_, trade)| trade.cost_token_a.max(trade.proceeds_token_a));
        let Some((direction, trade)) = candidate else {
            return Ok(false);
        };
        let candidate_selected_at = Instant::now();
        let native_price_token_a = self.native_price_token_a().unwrap_or(Decimal::ZERO);
        let economics = evaluate_execution_admission(
            quote,
            AdmissionInputs {
                symbol: &pair_symbol,
                direction,
                token_b_amount: trade.token_b_amount,
                token_b_step_base_units: token_b_step,
                token_b_decimals,
                expected_cost_token_a: trade.cost_token_a,
                expected_proceeds_token_a: trade.proceeds_token_a,
                opportunity_threshold_met: trade.meets_threshold,
            },
        )?;
        let liquidity_source = "dex_curve_only";
        let dex_pool_generation = self.opportunities.pool_generation(trade.pool_index)?;
        let dex_fee_generation = self.opportunities.pool_fee_generation(trade.pool_index)?;
        let token_a_symbol = pair_config.token_a.symbol.clone();
        let token_b_symbol = pair_config.token_b.symbol.clone();
        let selected_pool = self.dex.pool(trade.pool_index)?;
        let deadline_unix_seconds = if let Some(fee) = selected_pool.camelot_fee.as_ref() {
            u64::from(fee.envelope.last_timestamp)
        } else {
            admission_deadline_unix_seconds(quote.received_unix_us, quote.received_at.elapsed())?
        };
        let dex_plan = DexSwapPlan::build(
            pair_config,
            selected_pool,
            direction,
            trade,
            dex_pool_generation,
            dex_fee_generation,
            deadline_unix_seconds,
        )?;
        let mut opportunity = PaperOpportunity {
            source_revision: self.domain_config.snapshot().source.revision.clone(),
            pair_id: pair_id.clone(),
            symbol: pair_symbol.clone(),
            update_id: quote.update_id,
            received_unix_us: quote.received_unix_us,
            reservation_completed_unix_us: 0,
            direction,
            dex_pool_index: trade.pool_index,
            dex_pool_generation,
            dex_fee_generation,
            token_b_base_units: u256_to_i128(trade.token_b_amount, "paper token-B amount")?,
            token_b_step_base_units: u256_to_i128(token_b_step, "paper token-B step")?,
            cost_token_a_base_units: u256_to_i128(trade.cost_token_a, "paper token-A cost")?,
            proceeds_token_a_base_units: u256_to_i128(
                trade.proceeds_token_a,
                "paper token-A proceeds",
            )?,
            admission: AdmissionRiskBounds {
                opportunity_threshold_met: economics.opportunity_threshold_met,
                opportunity_threshold_bps: pair_config.strategy.opportunity_threshold_bps,
                depth_source: Some(execution_depth_health.source.as_str().to_owned()),
                depth_age_ms: execution_depth_health.age_ms,
                depth_update_delta: execution_depth_health.update_delta,
                top_matches: Some(execution_depth_health.top_matches),
                top_mismatch_reason: execution_depth_health
                    .top_mismatch_reason
                    .map(str::to_owned),
                execution_slippage_bps: trade.execution_slippage_bps,
                cex_primary_limit_price: match direction {
                    TradeDirection::BuyTokenBOnDexSellOnCex => quote.bid_price,
                    TradeDirection::BuyTokenBOnCexSellOnDex => quote.ask_price,
                },
                cex_primary_top_quantity: Decimal::ZERO,
                cex_recovery_limit_price: economics.recovery_limit_price,
                cex_recovery_sell_limit_price: economics.recovery_sell_limit_price,
                cex_recovery_buy_limit_price: economics.recovery_buy_limit_price,
                recovery_quote_token_a_base_units: u256_to_u128(
                    economics.recovery_quote_token_a,
                    "paper recovery quote",
                )?,
                recovery_sell_quote_token_a_base_units: u256_to_u128(
                    economics.recovery_sell_quote_token_a,
                    "paper recovery sell quote",
                )?,
                recovery_buy_quote_token_a_base_units: u256_to_u128(
                    economics.recovery_buy_quote_token_a,
                    "paper recovery buy quote",
                )?,
                maximum_recovery_loss_token_a_base_units: 0,
                // Journal-shape compatibility only. Transaction fees are
                // selected immediately before signing, outside admission.
                maximum_fee_per_gas_wei: 0,
                gas_conversion_price_token_a: native_price_token_a,
                maximum_gas_cost_token_a_base_units: 0,
                bounded_profit_token_a_base_units: 0,
            },
            dex_plan: dex_plan.clone(),
        };
        let plan_id = opportunity.plan_id();
        let cost_direction = match direction {
            TradeDirection::BuyTokenBOnDexSellOnCex => ArbitrageDirection::BuyTokenBOnDexSellOnCex,
            TradeDirection::BuyTokenBOnCexSellOnDex => ArbitrageDirection::BuyTokenBOnCexSellOnDex,
        };
        self.hot_telemetry.emit_pretrade_candidate(
            &plan_id,
            quote,
            evaluation.pair_index,
            cost_direction,
            trade,
        );
        let dex_input_claim = U256::from(dex_plan.amount_in_base_units);
        let (token_a_claim, token_b_claim) =
            exact_execution_envelope_amounts(direction, dex_input_claim, trade);
        let binance_location = self.binance_inventory_location()?;
        let wallet_location = self.wallet_inventory_location(pair_chain_id)?;
        let claims = match direction {
            TradeDirection::BuyTokenBOnDexSellOnCex => vec![
                InventoryClaim {
                    key: self.portfolio_key(&wallet_location, &token_a_symbol)?,
                    amount: token_a_claim,
                },
                InventoryClaim {
                    key: self.portfolio_key(&binance_location, &token_b_symbol)?,
                    amount: token_b_claim,
                },
            ],
            TradeDirection::BuyTokenBOnCexSellOnDex => vec![
                InventoryClaim {
                    key: self.portfolio_key(&binance_location, &token_a_symbol)?,
                    amount: token_a_claim,
                },
                InventoryClaim {
                    key: self.portfolio_key(&wallet_location, &token_b_symbol)?,
                    amount: token_b_claim,
                },
            ],
        };
        let request = ReservationRequest {
            operation_id: plan_id.clone(),
            purpose: ReservationPurpose::TradePrimary,
            claims: claims.clone(),
            settlement_locations: [binance_location, wallet_location].into_iter().collect(),
        };
        let reservation_started = Instant::now();
        match reservation_precheck(&self.inventory, &request) {
            ReservationPrecheck::Duplicate => {
                self.telemetry.emit(
                    "arbitrage_admission_rejected",
                    json!({
                        "engine_id": self.config.engine_id,
                        "plan_id": plan_id,
                        "pair_id": pair_id,
                        "symbol": pair_symbol,
                        "update_id": quote.update_id,
                        "direction": cost_direction.as_str(),
                        "gross_profit_bps_x100": trade.gross_profit_bps_x100,
                        "reason": "duplicate_plan_inflight",
                    }),
                );
                return Ok(false);
            }
            ReservationPrecheck::Conflict => {
                tracing::error!(
                    engine_id = %self.config.engine_id,
                    pair_id,
                    pair_symbol,
                    plan_id,
                    "arbitrage plan conflicts with its active inventory reservation"
                );
                self.telemetry.emit(
                    "arbitrage_admission_rejected",
                    json!({
                        "engine_id": self.config.engine_id,
                        "plan_id": plan_id,
                        "pair_id": pair_id,
                        "symbol": pair_symbol,
                        "update_id": quote.update_id,
                        "direction": cost_direction.as_str(),
                        "gross_profit_bps_x100": trade.gross_profit_bps_x100,
                        "reason": "inventory_reservation_conflict",
                    }),
                );
                return Ok(false);
            }
            ReservationPrecheck::Vacant => {}
        }
        if let Err(error) = self.inventory.reserve(request) {
            let claim_details = self.inventory_claim_details(&claims);
            let failure_kind = classify_inventory_admission_failure(&error);
            self.log_trading_inventory_blocked(
                &pair_id,
                &pair_symbol,
                &plan_id,
                &claim_details,
                failure_kind,
            );
            self.telemetry.emit(
                "arbitrage_admission_rejected",
                json!({
                    "engine_id": self.config.engine_id,
                    "plan_id": plan_id,
                    "pair_id": pair_id,
                    "symbol": pair_symbol,
                    "update_id": quote.update_id,
                    "direction": cost_direction.as_str(),
                    "gross_profit_bps_x100": trade.gross_profit_bps_x100,
                    "reason": failure_kind.telemetry_reason(),
                    "error": format!("{error:#}"),
                    "claims": claim_details,
                }),
            );
            return Ok(false);
        }
        let inventory_reservation_us = duration_us(reservation_started.elapsed());
        opportunity.reservation_completed_unix_us = unix_timestamp_us()?;
        let candidate_selected_to_reservation_complete_us =
            duration_us(candidate_selected_at.elapsed());
        self.arbitrage_plan_freshness.insert(
            plan_id.clone(),
            ArbitragePlanFreshness {
                pair_id: pair_id.clone(),
                pool_index: trade.pool_index,
                pool_generation: dex_pool_generation,
            },
        );
        let mailbox_submit_started = Instant::now();
        match handle.try_submit(opportunity) {
            PaperTradeSubmitResult::Accepted => {}
            PaperTradeSubmitResult::Superseded(previous) => {
                self.release_pending_opportunity(
                    *previous,
                    "execution_pending_superseded",
                    Some(&plan_id),
                )?;
            }
            PaperTradeSubmitResult::Unavailable => {
                self.arbitrage_plan_freshness.remove(&plan_id);
                self.inventory.release_unsubmitted(&plan_id)?;
                self.telemetry.emit(
                    "arbitrage_admission_rejected",
                    json!({
                        "engine_id": self.config.engine_id,
                        "plan_id": plan_id,
                        "pair_id": pair_id,
                        "symbol": pair_symbol,
                        "update_id": quote.update_id,
                        "direction": cost_direction.as_str(),
                        "gross_profit_bps_x100": trade.gross_profit_bps_x100,
                        "reason": "execution_lane_unavailable",
                    }),
                );
                return Ok(false);
            }
        }
        let mailbox_submit_us = duration_us(mailbox_submit_started.elapsed());
        let admitted_payload = json!({
            "engine_id": self.config.engine_id,
            "plan_id": &plan_id,
            "pair_id": pair_id,
            "strategy_id": strategy_id(&pair_id),
            "binance_account_id": PRIMARY_BINANCE_ACCOUNT_ID,
            "instrument_id": instrument_id(&pair_symbol),
            "network_id": network_id(pair_chain_id),
            "wallet_id": PRIMARY_EVM_WALLET_ID,
            "wallet_location_id": wallet_location_id(pair_chain_id),
            "execution_lane_id": execution_lane_id(pair_chain_id),
            "mode": self.config.arbitrage_execution_mode,
            "admission_liquidity_source": liquidity_source,
            "sizing_mode": if adaptive_candidate.is_some() { "adaptive" } else { "baseline" },
            "depth_source": execution_depth_health.source.as_str(),
            "depth_source_reason": execution_depth_health.source_reason,
            "depth_age_ms": execution_depth_health.age_ms,
            "depth_update_delta": execution_depth_health.update_delta,
            "top_matches": execution_depth_health.top_matches,
            "top_mismatch_reason": execution_depth_health.top_mismatch_reason,
            "inventory_reservation_policy": "exact_primary_execution_envelope_v3",
            "evaluation_trigger": evaluation_trigger,
            "update_id": quote.update_id,
            "opportunity_received_unix_us": quote.received_unix_us,
            "market_to_admitted_us": duration_us(quote.received_at.elapsed()),
            "trigger_to_admitted_us": duration_us(evaluation_started_at.elapsed()),
            "admission_total_us": duration_us(admission_started.elapsed()),
            "inventory_reservation_us": inventory_reservation_us,
            "candidate_selected_to_reservation_complete_us": candidate_selected_to_reservation_complete_us,
            "mailbox_submit_us": mailbox_submit_us,
            "inventory_claims": self.inventory_claim_details(&claims),
            "execution_slippage_bps": trade.execution_slippage_bps,
            "gross_cost_token_a_base_units": trade.cost_token_a.to_string(),
            "gross_proceeds_token_a_base_units": trade.proceeds_token_a.to_string(),
            "gross_profit_bps_x100": trade.gross_profit_bps_x100.to_string(),
            "cex_primary_limit_price": match direction {
                TradeDirection::BuyTokenBOnDexSellOnCex => quote.bid_price.to_string(),
                TradeDirection::BuyTokenBOnCexSellOnDex => quote.ask_price.to_string(),
            },
            "price_unchanged_for_us": duration_us(quote.received_at.elapsed()),
            "dex_plan": dex_plan_telemetry_value(&dex_plan),
        });
        self.telemetry.emit("arbitrage_admitted", admitted_payload);
        Ok(false)
    }

    fn log_trading_inventory_blocked(
        &mut self,
        pair_id: &str,
        pair_symbol: &str,
        plan_id: &str,
        claim_details: &[Value],
        failure_kind: InventoryAdmissionFailureKind,
    ) {
        let now = Instant::now();
        let last_log_at = if failure_kind == InventoryAdmissionFailureKind::ReservationContention {
            &mut self.last_inventory_contention_log_at
        } else {
            &mut self.last_inventory_blocked_alert_at
        };
        if last_log_at.is_some_and(|last| {
            now.saturating_duration_since(last) < TRADING_INVENTORY_ALERT_LOG_INTERVAL
        }) {
            return;
        }
        *last_log_at = Some(now);
        let claims = Value::Array(claim_details.to_vec());
        if failure_kind == InventoryAdmissionFailureKind::ReservationContention {
            tracing::info!(
                engine_id = %self.config.engine_id,
                pair_id,
                pair_symbol,
                plan_id,
                claims = %claims,
                "arbitrage admission skipped because inventory is reserved by an active operation"
            );
            return;
        }
        if failure_kind == InventoryAdmissionFailureKind::InvariantViolation {
            tracing::error!(
                engine_id = %self.config.engine_id,
                pair_id,
                pair_symbol,
                plan_id,
                claims = %claims,
                "arbitrage inventory reservation failed"
            );
            return;
        }
        let shortage_assets = inventory_shortage_asset_symbols(claim_details);
        let shortage_asset = shortage_assets.iter().next().map(String::as_str);
        let shortage_assets_json = Value::Array(
            shortage_assets
                .iter()
                .map(|asset| Value::String(asset.clone()))
                .collect(),
        );
        let shortage_locations = inventory_shortage_location_ids(claim_details);
        let shortage_location = shortage_locations.iter().next().map(String::as_str);
        let shortage_locations_json = Value::Array(
            shortage_locations
                .iter()
                .map(|location| Value::String(location.clone()))
                .collect(),
        );
        let pending_token = self
            .pending_rebalance
            .as_ref()
            .map(|evaluation| evaluation.token_symbol.as_str());
        let inflight_token = self.rebalance_inflight.then(|| {
            self.rebalance_inventory_reservations
                .keys()
                .find(|token| shortage_assets.contains(*token))
                .map(String::as_str)
        });
        let settlement_token = self
            .rebalance_settlement
            .as_ref()
            .map(|barrier| barrier.token_symbol.as_str());
        let (rebalance_phase, rebalance_transient) = rebalance_phase_for_shortage_assets(
            &shortage_assets,
            pending_token,
            inflight_token.flatten(),
            settlement_token,
            &self.rebalance_blocked_tokens,
        );
        if rebalance_transient {
            tracing::warn!(
                engine_id = %self.config.engine_id,
                pair_id,
                pair_symbol,
                plan_id,
                shortage_asset,
                shortage_assets = %shortage_assets_json,
                shortage_location,
                shortage_locations = %shortage_locations_json,
                rebalance_phase,
                rebalance_transient,
                claims = %claims,
                "arbitrage admission deferred while matching inventory rebalances"
            );
            return;
        }
        tracing::error!(
            engine_id = %self.config.engine_id,
            pair_id,
            pair_symbol,
            plan_id,
            shortage_asset,
            shortage_assets = %shortage_assets_json,
            shortage_location,
            shortage_locations = %shortage_locations_json,
            rebalance_phase,
            rebalance_transient,
            claims = %claims,
            "arbitrage admission blocked by insufficient inventory"
        );
    }

    fn inventory_claim_details(&self, claims: &[InventoryClaim]) -> Vec<Value> {
        claims
            .iter()
            .map(|claim| {
                let observed = self.inventory.observed(&claim.key);
                let reserved = self.inventory.reserved(&claim.key);
                let available = self.inventory.available(&claim.key).ok();
                let shortage = available.is_some_and(|amount| amount < claim.amount);
                let shortfall = available
                    .filter(|amount| *amount < claim.amount)
                    .map(|amount| claim.amount - amount);
                json!({
                    "inventory_location_kind": claim.key.location.kind_label(),
                    "inventory_location_id": claim.key.location.stable_id(),
                    "venue_asset_id": claim.key.venue_asset_id.as_str(),
                    "economic_asset_id": self
                        .portfolio_catalog
                        .economic_asset_id(&claim.key)
                        .ok(),
                    "required_base_units": claim.amount.to_string(),
                    "observed_base_units": observed.map(|amount| amount.to_string()),
                    "reserved_base_units": reserved.to_string(),
                    "available_base_units": available.map(|amount| amount.to_string()),
                    "shortage": shortage,
                    "shortfall_base_units": shortfall.map(|amount| amount.to_string()),
                })
            })
            .collect()
    }

    pub fn apply_arbitrage_receipt_settlement(
        &mut self,
        event: &PaperTradeEvent,
    ) -> anyhow::Result<Option<PreparedPoolBuildRequest>> {
        if event.state != PaperTradeEventState::Balanced || !event.dex_filled {
            return Ok(None);
        }
        let Some(target) = event.dex_settlement_log.as_ref() else {
            self.telemetry.emit(
                "arbitrage_receipt_settlement_unavailable",
                json!({
                    "engine_id": self.config.engine_id,
                    "plan_id": event.plan_id,
                    "reason": "receipt_swap_missing",
                }),
            );
            return Ok(None);
        };
        ensure!(!target.removed, "receipt settlement Swap event is removed");
        let freshness = self.arbitrage_plan_freshness.get(&event.plan_id);
        ensure!(
            freshness.is_some() || event.resumed_after_restart,
            "settlement proof has no admitted plan freshness"
        );
        let pair_id = freshness
            .map(|freshness| freshness.pair_id.clone())
            .unwrap_or_else(|| event.pair_id.clone());
        let admission_generation = freshness.map(|freshness| freshness.pool_generation);
        let is_camelot = self.dex.is_camelot_address(target.address);
        let decoded = if is_camelot {
            decode_pool_event_for_locator(target, PoolLocator::CamelotV3(target.address))?
        } else {
            decode_pool_event(target)?
        }
        .context("receipt settlement log is not a recognized pool event")?;
        ensure!(
            matches!(decoded.update, PoolUpdate::Swap { .. }),
            "receipt settlement log is not a Swap event"
        );
        let pool_index = self
            .dex
            .pool_index(decoded.locator)
            .context("settlement proof targets an unknown pool")?;
        if let Some(freshness) = freshness {
            ensure!(
                pool_index == freshness.pool_index,
                "settlement proof pool differs from the admitted pool"
            );
        }
        if target.block_number <= self.dex.backfilled_through() {
            self.telemetry.emit(
                "arbitrage_receipt_settlement_already_applied",
                json!({
                    "engine_id": self.config.engine_id,
                    "pair_id": pair_id,
                    "plan_id": event.plan_id,
                    "pool_index": pool_index,
                    "admission_generation": admission_generation,
                    "resumed_without_freshness": freshness.is_none(),
                    "block_number": target.block_number,
                    "transaction_index": target.transaction_index,
                    "log_index": target.log_index,
                    "source": "startup_backfill_before_receipt",
                }),
            );
            return Ok(None);
        }
        let block_timestamp = if is_camelot {
            let Some(fee) = event.dex_settlement_fee.as_ref() else {
                self.telemetry.emit(
                    "arbitrage_receipt_settlement_unavailable",
                    json!({
                        "engine_id": self.config.engine_id,
                        "pair_id": pair_id,
                        "plan_id": event.plan_id,
                        "pool_index": pool_index,
                        "reason": "camelot_receipt_fee_missing",
                    }),
                );
                return Ok(None);
            };
            ensure!(
                fee.pool == target.address
                    && fee.block_number == target.block_number
                    && fee.block_hash == target.block_hash
                    && fee.transaction_index == target.transaction_index
                    && fee.log_index < target.log_index,
                "Camelot receipt Fee is not positionally before Swap"
            );
            let Some(timestamp) = self.dex.canonical_timestamp_for_log(target) else {
                self.telemetry.emit(
                    "arbitrage_receipt_settlement_unavailable",
                    json!({
                        "engine_id": self.config.engine_id,
                        "pair_id": pair_id,
                        "plan_id": event.plan_id,
                        "pool_index": pool_index,
                        "reason": "canonical_block_timestamp_missing",
                    }),
                );
                return Ok(None);
            };
            match self.dex.apply_camelot_fee_receipt(*fee, timestamp)? {
                LogApplyResult::Applied {
                    pool_index: applied_pool_index,
                    kind: "fee",
                    refresh_required: false,
                } => ensure!(
                    applied_pool_index == pool_index,
                    "receipt settlement Fee applied another pool"
                ),
                LogApplyResult::Duplicate => {}
                LogApplyResult::Applied { .. } => {
                    anyhow::bail!("Camelot receipt Fee produced an invalid apply result")
                }
                LogApplyResult::Unknown => {
                    anyhow::bail!("Camelot receipt Fee targets an unknown pool")
                }
            }
            Some(timestamp)
        } else {
            ensure!(
                event.dex_settlement_fee.is_none(),
                "static-fee receipt unexpectedly carries a Camelot Fee"
            );
            None
        };
        let apply = if let Some(timestamp) = block_timestamp {
            self.dex.apply_log_at_timestamp(target, timestamp)?
        } else {
            self.dex.apply_static_fee_receipt_log(target)?
        };
        match apply {
            LogApplyResult::Applied {
                pool_index: applied_pool_index,
                kind,
                ..
            } => {
                ensure!(
                    applied_pool_index == pool_index,
                    "receipt settlement applied another pool"
                );
                self.dex.refresh_pool_for_publication(pool_index)?;
                let refresh = self
                    .opportunities
                    .request_pool_refresh(pool_index, &self.dex)?;
                self.telemetry.emit(
                    "arbitrage_receipt_settlement_applied",
                    json!({
                        "engine_id": self.config.engine_id,
                        "pair_id": pair_id,
                        "plan_id": event.plan_id,
                        "pool_index": pool_index,
                        "admission_generation": admission_generation,
                        "resumed_without_freshness": freshness.is_none(),
                        "prepared_generation": refresh.generation(),
                        "kind": kind,
                        "block_number": target.block_number,
                        "transaction_index": target.transaction_index,
                        "log_index": target.log_index,
                        "fee_log_index": event.dex_settlement_fee.as_ref().map(|fee| fee.log_index),
                        "source": "transaction_receipt",
                    }),
                );
                Ok(Some(refresh))
            }
            LogApplyResult::Duplicate => {
                self.telemetry.emit(
                    "arbitrage_receipt_settlement_already_applied",
                    json!({
                        "engine_id": self.config.engine_id,
                        "pair_id": pair_id,
                        "plan_id": event.plan_id,
                        "pool_index": pool_index,
                        "admission_generation": admission_generation,
                        "resumed_without_freshness": freshness.is_none(),
                        "block_number": target.block_number,
                        "transaction_index": target.transaction_index,
                        "log_index": target.log_index,
                        "source": "websocket_before_receipt",
                    }),
                );
                Ok(None)
            }
            LogApplyResult::Unknown => {
                anyhow::bail!("receipt settlement Swap targets an unknown pool")
            }
        }
    }

    pub fn on_paper_trade_event(&mut self, event: PaperTradeEvent) -> anyhow::Result<()> {
        match event.state {
            PaperTradeEventState::Balanced => {
                if self.inventory.reservation(&event.plan_id).is_some() {
                    self.terminal_child_observed_at
                        .insert(event.plan_id.clone(), event.terminal_observed_at);
                    self.inventory.mark_pending_settlement(&event.plan_id)?;
                } else {
                    tracing::warn!(
                        plan_id = %event.plan_id,
                        "balanced arbitrage event has no in-memory reservation after restart"
                    );
                }
                self.arbitrage_plan_freshness.remove(&event.plan_id);
            }
            PaperTradeEventState::RejectedUnsubmitted => {
                let terminal_observed_at = event.terminal_observed_at;
                self.arbitrage_plan_freshness.remove(&event.plan_id);
                if self.inventory.reservation(&event.plan_id).is_some() {
                    self.inventory.release_unsubmitted(&event.plan_id)?;
                } else {
                    tracing::warn!(
                        plan_id = %event.plan_id,
                        "rejected arbitrage event has no in-memory reservation after restart"
                    );
                }
                self.emit_child_terminal_to_reservation_settled(
                    &event.plan_id,
                    terminal_observed_at,
                );
            }
            PaperTradeEventState::BlockedUnknown => {
                self.arbitrage_plan_freshness.remove(&event.plan_id);
            }
        }
        if let Some(handle) = self.paper_trades.as_ref() {
            handle.finish(event.state);
        }
        self.telemetry.emit(
            "arbitrage_inventory_state",
            json!({
                "engine_id": self.config.engine_id,
                "plan_id": event.plan_id,
                "state": format!("{:?}", event.state),
                "reservation_held": self.inventory.reservation(&event.plan_id).is_some(),
            }),
        );
        Ok(())
    }

    fn release_pending_opportunity(
        &mut self,
        opportunity: PaperOpportunity,
        reason: &'static str,
        superseded_by_plan_id: Option<&str>,
    ) -> anyhow::Result<()> {
        let plan_id = opportunity.plan_id();
        self.arbitrage_plan_freshness.remove(&plan_id);
        if self.inventory.reservation(&plan_id).is_some() {
            self.inventory.release_unsubmitted(&plan_id)?;
        }
        self.telemetry.emit(
            "arbitrage_execution_pending_discarded",
            json!({
                "engine_id": self.config.engine_id,
                "plan_id": &plan_id,
                "reason": reason,
                "superseded_by_plan_id": superseded_by_plan_id,
            }),
        );
        self.telemetry.emit(
            "arbitrage_inventory_state",
            json!({
                "engine_id": self.config.engine_id,
                "plan_id": plan_id,
                "state": "RejectedUnsubmitted",
                "reservation_held": false,
            }),
        );
        Ok(())
    }

    pub fn refresh_health(&mut self) {
        let now = Instant::now();
        self.refresh_phase(now);
        self.log_binance_price_health(now);
        self.log_depth_health(now);
        self.log_rebalance_health(now);
    }

    fn log_binance_price_health(&mut self, now: Instant) {
        if self.last_binance_price_health_log_at.is_some_and(|last| {
            now.saturating_duration_since(last) < BINANCE_PRICE_HEALTH_LOG_INTERVAL
        }) {
            return;
        }

        let hot_telemetry_dropped_records = self.hot_telemetry.dropped_records();
        for (symbol, feed) in &self.state.binance_feeds {
            let max_transport_silence_ms = self
                .strategy_price_transport_silence_limits_ms
                .get(symbol.as_ref())
                .copied();
            let price_age_ms = feed
                .book
                .as_ref()
                .map(|book| now.saturating_duration_since(book.received_at).as_millis());
            let transport_age_ms = feed.last_transport_activity_at.map(|last_activity_at| {
                now.saturating_duration_since(last_activity_at).as_millis()
            });
            let transport_fresh = transport_age_ms
                .zip(max_transport_silence_ms)
                .is_some_and(|(age, maximum)| age <= u128::from(maximum));
            let healthy = feed.connected && feed.book.is_some() && transport_fresh;
            if healthy {
                tracing::info!(
                    healthy,
                    symbol = symbol.as_ref(),
                    generation = feed.connection_generation,
                    last_update_id = feed.last_update_id,
                    price_age_ms,
                    transport_age_ms,
                    accepted_updates = feed.accepted_updates,
                    rejected_updates = feed.rejected_updates,
                    hot_telemetry_dropped_records,
                    "Binance strategy price health heartbeat"
                );
            } else {
                tracing::warn!(
                    healthy,
                    symbol = symbol.as_ref(),
                    connected = feed.connected,
                    generation = feed.connection_generation,
                    last_update_id = feed.last_update_id,
                    price_age_ms,
                    transport_age_ms,
                    accepted_updates = feed.accepted_updates,
                    rejected_updates = feed.rejected_updates,
                    hot_telemetry_dropped_records,
                    "Binance strategy price health heartbeat"
                );
            }
            self.telemetry.emit(
                "binance_price_health",
                json!({
                    "engine_id": self.config.engine_id,
                    "product": "spot",
                    "symbol": symbol.as_ref(),
                    "healthy": healthy,
                    "connected": feed.connected,
                    "runtime_phase": self.state.phase,
                    "generation": feed.connection_generation,
                    "last_update_id": feed.last_update_id,
                    "price_age_ms": price_age_ms,
                    "transport_age_ms": transport_age_ms,
                    "max_transport_age_ms": max_transport_silence_ms,
                    "max_transport_silence_ms": max_transport_silence_ms,
                    "max_transport_silence_source": "domain_artifact",
                    "accepted_updates": feed.accepted_updates,
                    "rejected_updates": feed.rejected_updates,
                    "hot_telemetry_dropped_records": hot_telemetry_dropped_records,
                    "exchange_timestamp_available": feed.book.as_ref().is_some_and(|book| {
                        book.exchange_event_ts_ms.is_some()
                            || book.exchange_transaction_ts_ms.is_some()
                    }),
                }),
            );
        }
        self.last_binance_price_health_log_at = Some(now);
    }

    fn log_depth_health(&mut self, now: Instant) {
        if self
            .last_depth_health_log_at
            .is_some_and(|last| now.saturating_duration_since(last) < DEPTH_HEALTH_LOG_INTERVAL)
        {
            return;
        }

        for (symbol, health) in &self.depth_health_by_symbol {
            let healthy = !matches!(health.source, AdaptiveDepthSource::TopOfBookOnly);
            if healthy {
                tracing::info!(
                    healthy,
                    symbol,
                    depth_source = health.source.as_str(),
                    depth_source_reason = health.source_reason,
                    depth_age_ms = health.age_ms,
                    depth_update_delta = health.update_delta,
                    top_matches = health.top_matches,
                    top_mismatch_reason = health.top_mismatch_reason,
                    "Binance depth health heartbeat"
                );
            } else {
                tracing::warn!(
                    healthy,
                    symbol,
                    depth_source = health.source.as_str(),
                    depth_source_reason = health.source_reason,
                    depth_age_ms = health.age_ms,
                    depth_update_delta = health.update_delta,
                    top_matches = health.top_matches,
                    top_mismatch_reason = health.top_mismatch_reason,
                    "Binance depth health heartbeat"
                );
            }
            self.telemetry.emit(
                "binance_depth_health",
                json!({
                    "engine_id": self.config.engine_id,
                    "symbol": symbol,
                    "healthy": healthy,
                    "runtime_phase": self.state.phase,
                    "depth_source": health.source.as_str(),
                    "depth_source_reason": health.source_reason,
                    "depth_age_ms": health.age_ms,
                    "depth_update_delta": health.update_delta,
                    "top_matches": health.top_matches,
                    "top_mismatch_reason": health.top_mismatch_reason,
                }),
            );
        }
        self.last_depth_health_log_at = Some(now);
    }

    fn log_rebalance_health(&mut self, now: Instant) {
        if self
            .last_rebalance_health_log_at
            .is_some_and(|last| now.saturating_duration_since(last) < REBALANCE_HEALTH_LOG_INTERVAL)
        {
            return;
        }

        let pending_age = self
            .rebalance_pending_since
            .map(|started_at| now.saturating_duration_since(started_at));
        let inflight_age = self
            .rebalance_inflight_since
            .map(|started_at| now.saturating_duration_since(started_at));
        let settlement_age = self
            .rebalance_settlement
            .as_ref()
            .map(|barrier| now.saturating_duration_since(barrier.started_at));
        let settlement_timeout = Duration::from_millis(
            self.config
                .balance_max_age_ms
                .saturating_mul(12)
                .max(MINIMUM_REBALANCE_SETTLEMENT_TIMEOUT.as_millis() as u64),
        );
        let health = rebalance_health_state(
            !self.rebalance_blocked_tokens.is_empty(),
            pending_age,
            inflight_age,
            settlement_age,
            REBALANCE_PENDING_TIMEOUT,
            Duration::from_secs(self.config.rebalance_executor_timeout_seconds),
            settlement_timeout,
        );
        let pending_age_ms = pending_age.map(|age| age.as_millis());
        let inflight_age_ms = inflight_age.map(|age| age.as_millis());
        let settlement_age_ms = settlement_age.map(|age| age.as_millis());
        let pending_token = self
            .pending_rebalance
            .as_ref()
            .map(|evaluation| evaluation.token_symbol.as_str());
        let pending_direction = self.pending_rebalance.as_ref().and_then(|evaluation| {
            evaluation
                .plan
                .action
                .as_ref()
                .map(|action| format!("{:?}", action.direction))
        });
        if health.healthy {
            tracing::info!(
                healthy = true,
                rebalance_blocked = !self.rebalance_blocked_tokens.is_empty(),
                rebalance_blocked_tokens = ?self.rebalance_blocked_tokens,
                rebalance_pending = self.pending_rebalance.is_some(),
                pending_token,
                pending_direction,
                pending_age_ms,
                deferred_reason = self.rebalance_deferred_reason.as_deref(),
                rebalance_inflight = self.rebalance_inflight,
                inflight_age_ms,
                settlement_waiting = self.rebalance_settlement.is_some(),
                settlement_age_ms,
                "rebalance health heartbeat"
            );
        } else {
            tracing::error!(
                healthy = false,
                rebalance_blocked = !self.rebalance_blocked_tokens.is_empty(),
                rebalance_blocked_tokens = ?self.rebalance_blocked_tokens,
                rebalance_pending = self.pending_rebalance.is_some(),
                pending_stuck = health.pending_stuck,
                pending_token,
                pending_direction,
                pending_age_ms,
                deferred_reason = self.rebalance_deferred_reason.as_deref(),
                rebalance_inflight = self.rebalance_inflight,
                inflight_stuck = health.inflight_stuck,
                inflight_age_ms,
                settlement_waiting = self.rebalance_settlement.is_some(),
                settlement_stuck = health.settlement_stuck,
                settlement_age_ms,
                "rebalance health heartbeat"
            );
        }
        self.last_rebalance_health_log_at = Some(now);
    }

    fn refresh_phase(&mut self, now: Instant) {
        let previous = self.state.phase;
        let binance_ready = self.binance_strategy_prices_ready(now);
        let dex_mirror_ready = self.dex.is_fresh(now, self.config.dex_head_max_age_ms);
        let dex_prepared_ready = self.opportunities.is_ready();
        // Prepared DEX quote curves are a per-pool execution input, not a
        // process-wide health signal. A pool can be rebuilding for the latest
        // on-chain event while the rest of the runtime remains healthy and able
        // to evaluate other pools or top-of-book-only fast-path candidates.
        //
        // Keep the global phase tied to the live DEX mirror/head freshness and
        // let opportunity evaluation skip pools whose prepared curves are
        // temporarily unavailable. Otherwise every short CLMM rebuild creates a
        // misleading Ready->Degraded->Ready flap even though Kubernetes,
        // balances, user data, gas, and rebalance are all healthy.
        let dex_ready = dex_mirror_ready;
        let balances_ready = self
            .state
            .balances
            .is_fresh(now, self.config.balance_max_age_ms);
        // User Data is an event-driven acceleration and diagnostic path. REST
        // balance reconciliation is the recoverable account-state boundary:
        // its successful snapshots restore health, while missing/stale balance
        // generations close readiness. Concrete plans still reserve and check
        // exact available inventory.
        let trading_readiness = TradingReadiness {
            dex_ready,
            balances_ready,
        };
        let current = self
            .state
            .refresh_phase_from_inputs(binance_ready, trading_readiness.ready());
        if previous != current {
            let blocking_inputs = [
                (!binance_ready).then_some("binance_top"),
                (!dex_mirror_ready).then_some("dex_mirror"),
                (!balances_ready).then_some("balances"),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
            tracing::info!(?previous, ?current, "runtime phase changed");
            self.telemetry.emit(
                "runtime_phase_changed",
                json!({
                    "engine_id": self.config.engine_id,
                    "previous": previous,
                    "current": current,
                    "binance_top_ready": binance_ready,
                    "dex_mirror_ready": dex_mirror_ready,
                    "dex_prepared_ready": dex_prepared_ready,
                    "balances_ready": balances_ready,
                    "balances_gate_enabled": true,
                    "binance_user_data_connected": self.binance_user_data_connected,
                    "binance_user_data_clean": self.binance_user_data_clean,
                    "binance_user_data_gate_enabled": false,
                    "gas_price_connected": self.gas_price_connected,
                    "gas_conversion_available": self.gas_price_book.is_some(),
                    "gas_price_gate_enabled": false,
                    "blocking_inputs": blocking_inputs,
                }),
            );
        }
    }

    fn binance_strategy_prices_ready(&self, now: Instant) -> bool {
        !self.strategy_price_transport_silence_limits_ms.is_empty()
            && self.strategy_price_transport_silence_limits_ms.iter().all(
                |(symbol, max_transport_silence_ms)| {
                    self.state
                        .binance_symbol_price_ready(symbol, now, *max_transport_silence_ms)
                },
            )
    }

    pub fn phase(&self) -> RuntimePhase {
        self.state.phase
    }

    pub fn record_runtime_first_ready(&self, process_elapsed: Duration) {
        self.telemetry.emit(
            "runtime_first_ready",
            json!({
                "engine_id": self.config.engine_id,
                "process_start_to_first_ready_us": duration_us(process_elapsed),
                "runtime_phase": self.state.phase,
            }),
        );
    }

    pub fn record_owner_loop_health(
        &self,
        loop_lag_us: u128,
        longest_handler: &'static str,
        longest_non_price_handler_us: u128,
    ) {
        self.telemetry.emit(
            "decision_owner_health",
            json!({
                "engine_id": self.config.engine_id,
                "loop_lag_us": loop_lag_us,
                "longest_non_price_handler": longest_handler,
                "longest_non_price_handler_us": longest_non_price_handler_us,
                "hot_telemetry_dropped_records": self.hot_telemetry.dropped_records(),
            }),
        );
    }

    pub fn record_shared_binance_stream_event(
        &self,
        symbol: &str,
        event_kind: SharedStreamEventKind,
        generation: u64,
        parse_time_us: u128,
        wire_frame_size_bytes: usize,
    ) {
        self.hot_telemetry.emit_shared_stream_event(
            symbol,
            event_kind,
            generation,
            parse_time_us,
            wire_frame_size_bytes,
        );
    }

    pub fn record_dex_drain(&self, event_count: usize, duration: Duration) {
        self.telemetry.emit(
            "decision_owner_dex_drain",
            json!({
                "engine_id": self.config.engine_id,
                "event_count": event_count,
                "dependency_fanout_count": 1,
                "duration_us": duration_us(duration),
            }),
        );
    }

    pub fn shutdown(&mut self) {
        self.state.stop();
        self.telemetry.emit(
            "runtime_stopping",
            json!({
                "engine_id": self.config.engine_id,
                "processed_events": self.state.processed_events,
            }),
        );
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InventoryAdmissionFailureKind {
    ReservationContention,
    CapitalShortfall,
    InvariantViolation,
}

impl InventoryAdmissionFailureKind {
    const fn telemetry_reason(self) -> &'static str {
        match self {
            Self::ReservationContention => "inventory_reservation_contention",
            Self::CapitalShortfall => "insufficient_available_inventory",
            Self::InvariantViolation => "inventory_reservation_error",
        }
    }
}

fn classify_inventory_admission_failure(error: &anyhow::Error) -> InventoryAdmissionFailureKind {
    let Some(insufficient) = error.downcast_ref::<InsufficientAvailableInventory>() else {
        return InventoryAdmissionFailureKind::InvariantViolation;
    };
    if insufficient.caused_by_active_reservations() {
        InventoryAdmissionFailureKind::ReservationContention
    } else {
        InventoryAdmissionFailureKind::CapitalShortfall
    }
}

fn inventory_shortage_asset_symbols(claim_details: &[Value]) -> BTreeSet<String> {
    claim_details
        .iter()
        .filter(|claim| claim.get("shortage").and_then(Value::as_bool) == Some(true))
        .filter_map(|claim| claim.get("economic_asset_id").and_then(Value::as_str))
        .map(|asset| asset.strip_prefix("economic:").unwrap_or(asset).to_owned())
        .collect()
}

fn inventory_shortage_location_ids(claim_details: &[Value]) -> BTreeSet<String> {
    claim_details
        .iter()
        .filter(|claim| claim.get("shortage").and_then(Value::as_bool) == Some(true))
        .filter_map(|claim| claim.get("inventory_location_id").and_then(Value::as_str))
        .map(str::to_owned)
        .collect()
}

fn settled_owned_rebalance_reservations(
    inventory: &SharedInventoryReservations,
    owned: &mut BTreeMap<String, String>,
) -> Vec<(String, String)> {
    let settled = owned
        .iter()
        .filter(|(_, operation_id)| inventory.reservation(operation_id).is_none())
        .map(|(token, operation_id)| (token.clone(), operation_id.clone()))
        .collect::<Vec<_>>();
    for (token, _) in &settled {
        owned.remove(token);
    }
    settled
}

fn rebalance_phase_for_shortage_assets(
    shortage_assets: &BTreeSet<String>,
    pending_token: Option<&str>,
    inflight_token: Option<&str>,
    settlement_token: Option<&str>,
    blocked_tokens: &BTreeSet<String>,
) -> (&'static str, bool) {
    if shortage_assets
        .iter()
        .any(|asset| blocked_tokens.contains(asset))
    {
        return ("quarantined", false);
    }
    if pending_token.is_some_and(|token| shortage_assets.contains(token)) {
        return ("pending", true);
    }
    if inflight_token.is_some_and(|token| shortage_assets.contains(token)) {
        return ("inflight", true);
    }
    if settlement_token.is_some_and(|token| shortage_assets.contains(token)) {
        return ("settlement", true);
    }
    ("idle", false)
}

fn rebalance_reservation_id(pair_id: &str, sequence: u64) -> String {
    format!("rebalance-reservation-{pair_id}-{sequence}")
}

fn requires_depth_for_runtime_phase(arbitrage_execution_mode: &str) -> bool {
    matches!(arbitrage_execution_mode, "paper_concurrent_hedged")
}

fn classify_depth_health(
    observation: DepthObservation,
    depth_available: bool,
    limits: Option<AdaptiveSizingRuntimeLimits>,
) -> DepthHealthObservation {
    let recent_caps = limits.and_then(|limits| {
        (limits.recent_full_depth_max_age_ms > 0 && limits.recent_full_depth_max_update_delta > 0)
            .then_some((
                limits.recent_full_depth_max_age_ms,
                limits.recent_full_depth_max_update_delta,
            ))
    });
    let (source, source_reason) = if observation.top_matches {
        (
            AdaptiveDepthSource::SequenceMatchedFullDepth,
            "exact_top_match",
        )
    } else if !depth_available {
        (AdaptiveDepthSource::TopOfBookOnly, "depth_unavailable")
    } else if recent_caps.is_none() {
        (
            AdaptiveDepthSource::TopOfBookOnly,
            "recent_full_depth_disabled",
        )
    } else if observation.age_ms.is_none() {
        (AdaptiveDepthSource::TopOfBookOnly, "depth_age_unknown")
    } else if observation.age_ms > recent_caps.map(|(max_age_ms, _)| max_age_ms) {
        (AdaptiveDepthSource::TopOfBookOnly, "depth_age_cap_exceeded")
    } else if observation.update_delta.is_none() {
        (
            AdaptiveDepthSource::TopOfBookOnly,
            "depth_update_delta_unknown",
        )
    } else if observation.update_delta > recent_caps.map(|(_, max_update_delta)| max_update_delta) {
        (
            AdaptiveDepthSource::TopOfBookOnly,
            "depth_update_delta_cap_exceeded",
        )
    } else {
        (
            AdaptiveDepthSource::RecentFullDepth,
            "within_recent_depth_caps",
        )
    };
    DepthHealthObservation {
        source,
        source_reason,
        age_ms: observation.age_ms,
        update_delta: observation.update_delta,
        top_matches: observation.top_matches,
        top_mismatch_reason: observation.top_mismatch_reason,
    }
}

fn depth_top_mismatch_reason(
    quote: &TopOfBook,
    depth: Option<&SpotDepthBook>,
) -> Option<&'static str> {
    let Some(depth) = depth else {
        return Some("depth_unavailable");
    };
    if depth.symbol() != quote.symbol.as_ref() {
        return Some("symbol_mismatch");
    }
    if depth.last_update_id() < quote.update_id {
        return Some("depth_update_behind_book_ticker");
    }
    let Some(bid) = depth.best_bid() else {
        return Some("depth_bid_missing");
    };
    if bid.price != quote.bid_price {
        return Some("bid_price_mismatch");
    }
    if bid.quantity != quote.bid_quantity {
        return Some("bid_quantity_mismatch");
    }
    let Some(ask) = depth.best_ask() else {
        return Some("depth_ask_missing");
    };
    if ask.price != quote.ask_price {
        return Some("ask_price_mismatch");
    }
    if ask.quantity != quote.ask_quantity {
        return Some("ask_quantity_mismatch");
    }
    None
}

fn u256_to_i128(value: U256, name: &str) -> anyhow::Result<i128> {
    let value = u128::try_from(value).map_err(|_| anyhow::anyhow!("{name} exceeds u128"))?;
    i128::try_from(value).map_err(|_| anyhow::anyhow!("{name} exceeds i128"))
}

fn duration_us(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

fn estimate_exchange_event_to_socket_us(
    received_unix_us: u64,
    exchange_event_ts_ms: u64,
    clock_offset_ms: i64,
) -> Option<i64> {
    let received_on_binance_clock_us =
        i128::from(received_unix_us).checked_add(i128::from(clock_offset_ms) * 1_000)?;
    let exchange_event_us = i128::from(exchange_event_ts_ms).checked_mul(1_000)?;
    received_on_binance_clock_us
        .checked_sub(exchange_event_us)
        .and_then(|estimate| i64::try_from(estimate).ok())
}

fn clock_sync_estimate_valid(clock_sync_age_ms: Option<u64>) -> bool {
    clock_sync_age_ms.is_some_and(|age_ms| age_ms <= BINANCE_CLOCK_SYNC_MAX_AGE_MS)
}

fn admission_deadline_unix_seconds(
    price_received_unix_us: u64,
    price_unchanged_for: Duration,
) -> anyhow::Result<u64> {
    price_received_unix_us
        .checked_add(duration_us(price_unchanged_for))
        .and_then(|admission_unix_us| admission_unix_us.checked_div(1_000_000))
        .and_then(|seconds| seconds.checked_add(DEX_PLAN_TTL_SECONDS))
        .context("DEX plan deadline overflow")
}

fn unix_timestamp_us() -> anyhow::Result<u64> {
    let micros = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_micros();
    u64::try_from(micros).context("Unix timestamp exceeds u64")
}

fn exact_execution_envelope_amounts(
    direction: TradeDirection,
    dex_input: U256,
    trade: TradeEvaluation,
) -> (U256, U256) {
    let token_a = match direction {
        TradeDirection::BuyTokenBOnDexSellOnCex => dex_input,
        TradeDirection::BuyTokenBOnCexSellOnDex => trade.cost_token_a,
    };
    let token_b = match direction {
        // The live executor caps the hedgeable DEX credit at the immutable
        // planned amount. Favorable output above it stays in the wallet.
        TradeDirection::BuyTokenBOnDexSellOnCex => trade.token_b_amount,
        TradeDirection::BuyTokenBOnCexSellOnDex => dex_input,
    };
    (token_a, token_b)
}

fn dex_plan_telemetry_value(plan: &DexSwapPlan) -> Value {
    json!({
        "route": &plan.route,
        "token_in": &plan.token_in,
        "token_out": &plan.token_out,
        "amount_in_base_units": plan.amount_in_base_units.to_string(),
        "amount_out_minimum_base_units": plan.amount_out_minimum_base_units.to_string(),
        "deadline_unix_seconds": plan.deadline_unix_seconds,
    })
}

fn u256_to_u128(value: U256, name: &str) -> anyhow::Result<u128> {
    u128::try_from(value).map_err(|_| anyhow::anyhow!("{name} exceeds u128"))
}

fn decimal_to_base_units_floor(value: Decimal, decimals: u8) -> anyhow::Result<U256> {
    ensure!(value >= Decimal::ZERO, "inventory balance is negative");
    let mantissa = value.mantissa();
    let mantissa = u128::try_from(mantissa).context("inventory balance mantissa is negative")?;
    let numerator = U256::from(mantissa)
        .checked_mul(pow10(decimals.into())?)
        .context("inventory balance base-unit numerator overflow")?;
    Ok(numerator / pow10(value.scale())?)
}

fn pow10(exponent: u32) -> anyhow::Result<U256> {
    let mut value = U256::ONE;
    for _ in 0..exponent {
        value = value
            .checked_mul(U256::from(10))
            .context("inventory decimal scale overflow")?;
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        time::{Duration, Instant},
    };

    use crate::{
        arbitrage::ArbitrageDirection as TradeDirection,
        execution_plan::{DexRoutePlan, DexSwapPlan},
        inventory::{
            InsufficientAvailableInventory, InventoryClaim, InventoryKey, InventoryLocation,
            ReservationPurpose, ReservationRequest, SharedInventoryReservations,
        },
        opportunity::{ArbitrageDirection as SizingDirection, TradeEvaluation},
        rebalance::Direction,
        state::BalanceState,
    };
    use alloy_primitives::U256;
    use serde_json::json;

    use super::{
        AdaptiveCandidate, AdaptiveDepthSource, AdaptiveSizingRuntimeLimits, DepthObservation,
        InventoryAdmissionFailureKind, RebalanceSettlementBarrier, ReservationPrecheck,
        TradingReadiness, adaptive_candidate_is_better, admission_deadline_unix_seconds,
        classify_depth_health, classify_inventory_admission_failure, clock_sync_estimate_valid,
        estimate_exchange_event_to_socket_us, exact_execution_envelope_amounts,
        inventory_shortage_asset_symbols, inventory_shortage_location_ids,
        mark_sequence_matched_update, rebalance_health_state, rebalance_phase_for_shortage_assets,
        rebalance_planning_deferred_reason, rebalance_reservation_id,
        requires_depth_for_runtime_phase, reservation_precheck,
        settled_owned_rebalance_reservations,
    };

    #[test]
    fn exchange_latency_estimate_requires_a_recent_clock_sync() {
        assert!(!clock_sync_estimate_valid(None));
        assert!(clock_sync_estimate_valid(Some(180_000)));
        assert!(!clock_sync_estimate_valid(Some(180_001)));
    }

    #[test]
    fn rebalance_reservations_are_unique_across_pair_engines() {
        assert_ne!(
            rebalance_reservation_id("world-chain-usdc-wld", 0),
            rebalance_reservation_id("arbitrum-usdc-esp", 0)
        );
        assert_eq!(
            rebalance_reservation_id("arbitrum-usdc-esp", 7),
            "rebalance-reservation-arbitrum-usdc-esp-7"
        );
    }

    #[test]
    fn inventory_shortages_keep_structured_asset_identity() {
        let claims = vec![
            json!({
                "economic_asset_id": "economic:USDC",
                "shortage": false,
            }),
            json!({
                "economic_asset_id": "economic:ESP",
                "shortage": true,
                "shortfall_base_units": "1654763030778280882143",
            }),
        ];

        assert_eq!(
            inventory_shortage_asset_symbols(&claims),
            BTreeSet::from(["ESP".to_owned()])
        );
    }

    #[test]
    fn inventory_shortages_keep_structured_location_identity() {
        let claims = vec![
            json!({
                "inventory_location_id": "binance-spot:primary",
                "shortage": false,
            }),
            json!({
                "inventory_location_id": "eip155:42161:evm-wallet:primary",
                "shortage": true,
            }),
        ];

        assert_eq!(
            inventory_shortage_location_ids(&claims),
            BTreeSet::from(["eip155:42161:evm-wallet:primary".to_owned()])
        );
    }

    #[test]
    fn owner_releases_local_rebalance_marker_after_shared_inventory_settles() {
        let inventory = SharedInventoryReservations::default();
        let binance = InventoryLocation::binance("binance-spot:primary").unwrap();
        let wallet =
            InventoryLocation::evm_wallet("eip155:42161", "eip155:42161:evm-wallet:primary")
                .unwrap();
        let binance_asset = "binance-spot:primary:asset:ESP";
        let wallet_asset = "eip155:42161:erc20:esp";
        inventory
            .update_location(
                binance.clone(),
                1,
                [(binance_asset.to_owned(), U256::from(10_000))],
            )
            .unwrap();
        inventory
            .update_location(
                wallet.clone(),
                1,
                [(wallet_asset.to_owned(), U256::from(10_000))],
            )
            .unwrap();
        let operation_id = "rebalance-reservation-arbitrum-usdc-esp-0";
        inventory
            .reserve(ReservationRequest {
                operation_id: operation_id.to_owned(),
                purpose: ReservationPurpose::Rebalance,
                claims: vec![InventoryClaim {
                    key: InventoryKey::new(binance.clone(), binance_asset).unwrap(),
                    amount: U256::from(6_000),
                }],
                settlement_locations: [binance.clone(), wallet.clone()].into_iter().collect(),
            })
            .unwrap();
        inventory.mark_pending_settlement(operation_id).unwrap();
        let mut owned = BTreeMap::from([("ESP".to_owned(), operation_id.to_owned())]);

        inventory
            .update_location(wallet, 2, [(wallet_asset.to_owned(), U256::from(16_000))])
            .unwrap();
        inventory
            .update_location(binance, 2, [(binance_asset.to_owned(), U256::from(4_000))])
            .unwrap();

        assert!(inventory.reservation(operation_id).is_none());
        assert_eq!(owned.get("ESP").map(String::as_str), Some(operation_id));
        assert_eq!(
            settled_owned_rebalance_reservations(&inventory, &mut owned),
            vec![("ESP".to_owned(), operation_id.to_owned())]
        );
        assert!(owned.is_empty());
    }

    #[test]
    fn only_matching_active_rebalance_phases_are_transient() {
        let shortages = BTreeSet::from(["ESP".to_owned()]);
        let no_blocked_tokens = BTreeSet::new();

        assert_eq!(
            rebalance_phase_for_shortage_assets(
                &shortages,
                Some("ESP"),
                None,
                None,
                &no_blocked_tokens,
            ),
            ("pending", true)
        );
        assert_eq!(
            rebalance_phase_for_shortage_assets(
                &shortages,
                None,
                Some("ESP"),
                None,
                &no_blocked_tokens,
            ),
            ("inflight", true)
        );
        assert_eq!(
            rebalance_phase_for_shortage_assets(
                &shortages,
                None,
                None,
                Some("ESP"),
                &no_blocked_tokens,
            ),
            ("settlement", true)
        );
        assert_eq!(
            rebalance_phase_for_shortage_assets(
                &shortages,
                Some("USDC"),
                None,
                None,
                &no_blocked_tokens,
            ),
            ("idle", false)
        );
        assert_eq!(
            rebalance_phase_for_shortage_assets(
                &shortages,
                Some("ESP"),
                None,
                None,
                &BTreeSet::from(["ESP".to_owned()]),
            ),
            ("quarantined", false)
        );
    }

    #[test]
    fn exchange_event_latency_uses_the_binance_clock_offset() {
        assert_eq!(
            estimate_exchange_event_to_socket_us(1_700_000_000_125_000, 1_700_000_000_123, 1),
            Some(3_000)
        );
        assert_eq!(
            estimate_exchange_event_to_socket_us(1_700_000_000_123_000, 1_700_000_000_123, -1),
            Some(-1_000)
        );
    }

    #[test]
    fn dex_deadline_uses_admission_time_when_price_is_unchanged() {
        assert_eq!(
            admission_deadline_unix_seconds(1_800_000_000_000_000, Duration::from_secs(45))
                .unwrap(),
            1_800_000_075
        );
    }

    fn adaptive_depth_limits() -> AdaptiveSizingRuntimeLimits {
        AdaptiveSizingRuntimeLimits {
            max_trade_notional: U256::from(200_000_000_u64),
            recent_full_depth_max_age_ms: 750,
            recent_full_depth_max_update_delta: 8,
        }
    }

    #[test]
    fn dex_first_runtime_phase_does_not_require_depth() {
        assert!(!requires_depth_for_runtime_phase("full_live"));
        assert!(!requires_depth_for_runtime_phase("paper_dex_first"));
        assert!(requires_depth_for_runtime_phase("paper_concurrent_hedged"));
    }

    #[test]
    fn active_wld_reservations_are_contention_not_capital_shortfall() {
        let location = InventoryLocation::binance("binance-spot:primary").unwrap();
        let asset = "binance-spot:primary:asset:WLD";
        let observed = U256::from(2_228_078_371_150_000_000_000_u128);
        let inventory = SharedInventoryReservations::default();
        inventory
            .update_location(location.clone(), 1, [(asset.to_owned(), observed)])
            .unwrap();
        inventory
            .reserve(ReservationRequest {
                operation_id: "production-active-wld-reservations".to_owned(),
                purpose: ReservationPurpose::TradePrimary,
                claims: vec![InventoryClaim {
                    key: InventoryKey::new(location.clone(), asset).unwrap(),
                    amount: U256::from(1_931_500_000_000_000_000_000_u128),
                }],
                settlement_locations: [location.clone()].into_iter().collect(),
            })
            .unwrap();
        let error = inventory
            .reserve(ReservationRequest {
                operation_id: "production-rejected-wld-plan".to_owned(),
                purpose: ReservationPurpose::TradePrimary,
                claims: vec![InventoryClaim {
                    key: InventoryKey::new(location.clone(), asset).unwrap(),
                    amount: U256::from(643_900_000_000_000_000_000_u128),
                }],
                settlement_locations: [location].into_iter().collect(),
            })
            .unwrap_err();
        let insufficient = error
            .downcast_ref::<InsufficientAvailableInventory>()
            .unwrap();

        assert_eq!(
            classify_inventory_admission_failure(&error),
            InventoryAdmissionFailureKind::ReservationContention
        );
        assert_eq!(
            insufficient.available,
            U256::from(296_578_371_150_000_000_000_u128)
        );
        assert_eq!(
            InventoryAdmissionFailureKind::ReservationContention.telemetry_reason(),
            "inventory_reservation_contention"
        );
    }

    #[test]
    fn actual_inventory_shortfall_and_invariants_remain_actionable() {
        let location = InventoryLocation::binance("binance-spot:primary").unwrap();
        let error = anyhow::Error::new(InsufficientAvailableInventory {
            key: InventoryKey::new(location, "binance-spot:primary:asset:WLD").unwrap(),
            requested: U256::from(643_900_000_000_000_000_000_u128),
            observed: U256::from(296_578_371_150_000_000_000_u128),
            reserved: U256::ZERO,
            available: U256::from(296_578_371_150_000_000_000_u128),
        });

        assert_eq!(
            classify_inventory_admission_failure(&error),
            InventoryAdmissionFailureKind::CapitalShortfall
        );
        assert_eq!(
            classify_inventory_admission_failure(&anyhow::anyhow!("missing generation")),
            InventoryAdmissionFailureKind::InvariantViolation
        );
    }

    #[test]
    fn adaptive_depth_sources_degrade_by_explicit_caps() {
        let limits = adaptive_depth_limits();
        let exact = classify_depth_health(
            DepthObservation {
                age_ms: Some(900),
                update_delta: Some(12),
                top_matches: true,
                top_mismatch_reason: None,
            },
            true,
            Some(limits),
        );
        assert_eq!(exact.source, AdaptiveDepthSource::SequenceMatchedFullDepth);

        let recent = classify_depth_health(
            DepthObservation {
                age_ms: Some(635),
                update_delta: Some(5),
                top_matches: false,
                top_mismatch_reason: Some("bid_quantity_mismatch"),
            },
            true,
            Some(limits),
        );
        assert_eq!(recent.source, AdaptiveDepthSource::RecentFullDepth);

        let stale = classify_depth_health(
            DepthObservation {
                age_ms: Some(751),
                update_delta: Some(5),
                top_matches: false,
                top_mismatch_reason: Some("bid_quantity_mismatch"),
            },
            true,
            Some(limits),
        );
        assert_eq!(stale.source, AdaptiveDepthSource::TopOfBookOnly);
        assert_eq!(stale.source_reason, "depth_age_cap_exceeded");

        let too_many_updates = classify_depth_health(
            DepthObservation {
                age_ms: Some(500),
                update_delta: Some(9),
                top_matches: false,
                top_mismatch_reason: Some("ask_price_mismatch"),
            },
            true,
            Some(limits),
        );
        assert_eq!(too_many_updates.source, AdaptiveDepthSource::TopOfBookOnly);
        assert_eq!(
            too_many_updates.source_reason,
            "depth_update_delta_cap_exceeded"
        );

        let unavailable = classify_depth_health(
            DepthObservation {
                age_ms: None,
                update_delta: None,
                top_matches: false,
                top_mismatch_reason: Some("depth_unavailable"),
            },
            false,
            Some(limits),
        );
        assert_eq!(unavailable.source, AdaptiveDepthSource::TopOfBookOnly);
        assert_eq!(unavailable.source_reason, "depth_unavailable");
    }

    #[test]
    fn sequence_matched_market_updates_are_deduplicated_per_symbol() {
        let mut updates = BTreeMap::new();
        assert!(mark_sequence_matched_update(&mut updates, "WLDUSDC", 100));
        assert!(!mark_sequence_matched_update(&mut updates, "WLDUSDC", 100));
        assert!(!mark_sequence_matched_update(&mut updates, "WLDUSDC", 99));
        assert!(mark_sequence_matched_update(&mut updates, "WLDUSDC", 101));
        assert!(mark_sequence_matched_update(&mut updates, "ETHUSDT", 1));
    }

    #[test]
    fn active_identical_reservation_is_a_duplicate_not_an_inventory_shortage() {
        let inventory = SharedInventoryReservations::default();
        let location = InventoryLocation::binance("binance-spot:primary").unwrap();
        let asset = "binance-spot:primary:asset:USDC";
        inventory
            .update_location(location.clone(), 1, [(asset.to_owned(), U256::from(1_000))])
            .unwrap();
        let request = ReservationRequest {
            operation_id: "paper-plan-1".to_owned(),
            purpose: ReservationPurpose::TradePrimary,
            claims: vec![InventoryClaim {
                key: InventoryKey::new(location.clone(), asset).unwrap(),
                amount: U256::from(100),
            }],
            settlement_locations: [location].into_iter().collect(),
        };

        assert_eq!(
            reservation_precheck(&inventory, &request),
            ReservationPrecheck::Vacant
        );
        inventory.reserve(request.clone()).unwrap();
        assert_eq!(
            reservation_precheck(&inventory, &request),
            ReservationPrecheck::Duplicate
        );

        let mut conflicting = request;
        conflicting.claims[0].amount = U256::from(200);
        assert_eq!(
            reservation_precheck(&inventory, &conflicting),
            ReservationPrecheck::Conflict
        );
    }

    #[test]
    fn dex_plan_telemetry_serializes_large_base_units_as_strings() {
        let plan = DexSwapPlan {
            route: DexRoutePlan::UniswapV3 {
                router: "0x1111111111111111111111111111111111111111".to_owned(),
                pool_address: "0x2222222222222222222222222222222222222222".to_owned(),
                fee_pips: 3_000,
            },
            token_in: "0x3333333333333333333333333333333333333333".to_owned(),
            token_out: "0x4444444444444444444444444444444444444444".to_owned(),
            amount_in_base_units: u128::MAX,
            amount_out_minimum_base_units: u128::MAX - 1,
            deadline_unix_seconds: 1_800_000_030,
        };

        let payload = super::dex_plan_telemetry_value(&plan);
        let max_u128 = u128::MAX.to_string();
        let max_u128_minus_one = (u128::MAX - 1).to_string();

        assert_eq!(
            payload["amount_in_base_units"].as_str(),
            Some(max_u128.as_str())
        );
        assert_eq!(
            payload["amount_out_minimum_base_units"].as_str(),
            Some(max_u128_minus_one.as_str())
        );
    }

    #[test]
    fn exact_execution_envelope_reserves_only_primary_token_debits() {
        let trade = TradeEvaluation {
            pool_index: 0,
            token_b_amount: U256::from(100),
            dex_token_a_amount: U256::from(900),
            cex_token_a_amount: U256::from(1_000),
            cost_token_a: U256::from(1_010),
            proceeds_token_a: U256::from(1_030),
            dex_amount_in: U256::from(1_020),
            dex_amount_out_minimum: U256::from(100),
            execution_slippage_bps: 10,
            gross_profit_bps_x100: 2_000,
            meets_threshold: true,
        };
        assert_eq!(
            exact_execution_envelope_amounts(
                TradeDirection::BuyTokenBOnDexSellOnCex,
                U256::from(1_020),
                trade,
            ),
            (U256::from(1_020), U256::from(100))
        );
        assert_eq!(
            exact_execution_envelope_amounts(
                TradeDirection::BuyTokenBOnCexSellOnDex,
                U256::from(100),
                trade,
            ),
            (U256::from(1_010), U256::from(100))
        );
    }

    #[test]
    fn adaptive_optimizer_ranks_the_largest_executable_slot() {
        let candidate = adaptive_candidate_for_ranking(1_100, 110);
        let current = adaptive_candidate_for_ranking(1_000, 500);

        assert!(adaptive_candidate_is_better(candidate, current));
        assert!(!adaptive_candidate_is_better(current, candidate));
    }

    fn adaptive_candidate_for_ranking(notional: u64, token_b_amount: u64) -> AdaptiveCandidate {
        let trade = TradeEvaluation {
            pool_index: 0,
            token_b_amount: U256::from(token_b_amount),
            dex_token_a_amount: U256::from(900),
            cex_token_a_amount: U256::from(notional),
            cost_token_a: U256::from(notional),
            proceeds_token_a: U256::from(notional),
            dex_amount_in: U256::from(notional),
            dex_amount_out_minimum: U256::from(token_b_amount),
            execution_slippage_bps: 10,
            gross_profit_bps_x100: 2_000,
            meets_threshold: true,
        };
        AdaptiveCandidate {
            direction: SizingDirection::BuyTokenBOnDexSellOnCex,
            trade,
            trade_notional: U256::from(notional),
        }
    }

    #[test]
    fn rebalance_state_is_not_a_global_trading_readiness_input() {
        assert!(
            TradingReadiness {
                dex_ready: true,
                balances_ready: true,
            }
            .ready()
        );
    }

    #[test]
    fn stale_balance_generations_close_global_trading_readiness() {
        let stale_balances = BalanceState::default();
        assert!(!stale_balances.is_fresh(Instant::now(), 10_000));
        assert!(
            !TradingReadiness {
                dex_ready: true,
                balances_ready: false,
            }
            .ready()
        );
    }

    #[test]
    fn user_data_health_is_not_a_global_trading_readiness_input() {
        assert!(
            TradingReadiness {
                dex_ready: true,
                balances_ready: true,
            }
            .ready()
        );
    }

    #[test]
    fn stale_dex_or_transport_inputs_still_fail_closed() {
        for readiness in [
            TradingReadiness {
                dex_ready: false,
                balances_ready: true,
            },
            TradingReadiness {
                dex_ready: true,
                balances_ready: false,
            },
        ] {
            assert!(!readiness.ready());
        }
    }

    #[test]
    fn gas_price_health_is_not_a_global_trading_readiness_input() {
        assert!(
            TradingReadiness {
                dex_ready: true,
                balances_ready: true,
            }
            .ready()
        );
    }

    #[test]
    fn completed_rebalance_waits_for_both_continuous_balance_streams() {
        let now = Instant::now();
        let later = now + std::time::Duration::from_millis(1);
        let barrier = RebalanceSettlementBarrier {
            operation_id: "rebalance-wld-1".to_owned(),
            strategy_id: "rebalance-world-chain-v12".to_owned(),
            token_symbol: "WLD".to_owned(),
            direction: Direction::WalletToBinance,
            binance_after: now,
            wallet_after: now,
            settlement_locations: [
                InventoryLocation::binance("binance-spot:primary").unwrap(),
                InventoryLocation::evm_wallet("eip155:480", "eip155:480:wallet:primary").unwrap(),
            ],
            started_at: now,
        };

        assert!(!barrier.reconciled(now, now));
        assert!(!barrier.reconciled(later, now));
        assert!(!barrier.reconciled(now, later));
        assert!(barrier.reconciled(later, later));
    }

    #[test]
    fn rebalance_settlement_barrier_does_not_change_trading_readiness() {
        assert!(
            TradingReadiness {
                dex_ready: true,
                balances_ready: true,
            }
            .ready()
        );
    }

    #[test]
    fn rebalance_planning_defers_only_during_mutation_or_settlement() {
        assert_eq!(
            rebalance_planning_deferred_reason(true, false),
            Some("operation_inflight")
        );
        assert_eq!(
            rebalance_planning_deferred_reason(false, true),
            Some("settlement_waiting")
        );
        assert_eq!(
            rebalance_planning_deferred_reason(true, true),
            Some("operation_inflight")
        );
        assert_eq!(rebalance_planning_deferred_reason(false, false), None);
    }

    #[test]
    fn rebalance_health_detects_blocked_and_stuck_states_at_the_boundary() {
        let timeout = std::time::Duration::from_secs(60);

        assert!(rebalance_health_state(false, None, None, None, timeout, timeout, timeout).healthy);
        assert!(!rebalance_health_state(true, None, None, None, timeout, timeout, timeout).healthy);
        let pending =
            rebalance_health_state(false, Some(timeout), None, None, timeout, timeout, timeout);
        assert!(pending.pending_stuck);
        assert!(!pending.healthy);
        let inflight =
            rebalance_health_state(false, None, Some(timeout), None, timeout, timeout, timeout);
        assert!(inflight.inflight_stuck);
        assert!(!inflight.healthy);
        let settlement =
            rebalance_health_state(false, None, None, Some(timeout), timeout, timeout, timeout);
        assert!(settlement.settlement_stuck);
        assert!(!settlement.healthy);
    }
}

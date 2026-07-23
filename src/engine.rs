use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_primitives::U256;
use anyhow::{Context, ensure};
use rust_decimal::Decimal;
use serde_json::{Value, json};

use crate::{
    admission::{
        AdmissionEconomics, AdmissionInputs, evaluate_admission, evaluate_dex_first_admission,
    },
    arbitrage::{
        AdmissionRiskBounds, ArbitrageDirection as TradeDirection, EntryPreflightHandle,
        PaperOpportunity, PaperTradeEvent, PaperTradeEventState, PaperTradeHandle,
        PaperTradeSubmitResult,
    },
    balances::BalanceEvent,
    binance::{
        account::BinanceClockSync,
        depth::SpotDepthBook,
        user_data::{ExecutionReportEvent, UserDataEvent},
    },
    chain::logs::{ChainLog, EthLogFilter},
    config::AppConfig,
    dex::{
        events::{PoolUpdate, build_pool_log_filter, decode_pool_event},
        mirror::{DexMirror, LogApplyResult},
    },
    domain::config::{AdaptiveSizingConfig, DexProvider, LoadedDomainConfig},
    execution_plan::{DEX_PLAN_TTL_SECONDS, DexSwapPlan},
    hot_telemetry::{HotTelemetryHandle, HotTelemetryTask, channel as hot_telemetry_channel},
    inventory::{
        InventoryClaim, InventoryKey, InventoryReservations, InventoryVenue, ReservationPurpose,
        ReservationRequest,
    },
    market_data::{MarketEvent, alchemy::DexStreamEvent},
    opportunity::{
        ArbitrageDirection, OpportunityEngine, PairEvaluation, PreparedPoolBuildRequest,
        PreparedPoolBuildResult, TradeEvaluation,
    },
    rebalance::{Direction, RebalanceEvaluation, RebalanceExecutionOperation, RebalanceTracker},
    state::{QuoteApplyResult, RuntimePhase, RuntimeState, TopOfBook},
    telemetry::TelemetryHandle,
};

const EXPECTED_PROFIT_AFTER_GAS_THRESHOLD_BPS: u16 = 5;
const BPS_X100_SCALE: u64 = 1_000_000;

pub struct TradingEngine {
    config: AppConfig,
    domain_config: Arc<LoadedDomainConfig>,
    state: RuntimeState,
    dex: DexMirror,
    opportunities: OpportunityEngine,
    rebalance: RebalanceTracker,
    telemetry: TelemetryHandle,
    hot_telemetry: HotTelemetryHandle,
    paper_trades: Option<PaperTradeHandle>,
    inventory: InventoryReservations,
    binance_inventory_generation: u64,
    binance_user_data_connected: bool,
    binance_user_data_clean: bool,
    binance_orders: BTreeMap<String, ExecutionReportEvent>,
    last_sequence_matched_quote_update: BTreeMap<String, u64>,
    latest_sequence_matched_depth: BTreeMap<String, SpotDepthBook>,
    depth_health_by_symbol: BTreeMap<String, DepthHealthObservation>,
    gas_price_symbol: String,
    wallet_gas_symbol: String,
    gas_price_connected: bool,
    gas_price_generation: u64,
    gas_price_book: Option<TopOfBook>,
    gas_price_transport_activity_at: Option<Instant>,
    binance_clock_sync: Option<BinanceClockSync>,
    rebalance_inventory_reservation: Option<String>,
    next_inventory_reservation: u64,
    pending_rebalance: Option<RebalanceEvaluation>,
    rebalance_inflight: bool,
    rebalance_inflight_since: Option<Instant>,
    rebalance_blocked: bool,
    rebalance_settlement: Option<RebalanceSettlementBarrier>,
    last_rebalance_health_log_at: Option<Instant>,
    last_depth_health_log_at: Option<Instant>,
    last_binance_price_health_log_at: Option<Instant>,
    last_inventory_blocked_alert_at: Option<Instant>,
    entry_preflight: EntryPreflightHandle,
    arbitrage_plan_freshness: BTreeMap<String, ArbitragePlanFreshness>,
    arbitrage_settlement_barriers: BTreeMap<usize, ArbitrageSettlementBarrier>,
}

pub struct TradingExecutionHandles {
    pub paper_trades: Option<PaperTradeHandle>,
    pub entry_preflight: EntryPreflightHandle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BinanceFeeBps {
    pub buy: u16,
    pub sell: u16,
}

const REBALANCE_HEALTH_LOG_INTERVAL: Duration = Duration::from_secs(60);
const DEPTH_HEALTH_LOG_INTERVAL: Duration = Duration::from_secs(60);
const BINANCE_PRICE_HEALTH_LOG_INTERVAL: Duration = Duration::from_secs(60);
const BINANCE_JSON_TIME_RESOLUTION_US: u64 = 1_000;
const TRADING_INVENTORY_ALERT_LOG_INTERVAL: Duration = Duration::from_secs(60);
const MINIMUM_REBALANCE_SETTLEMENT_TIMEOUT: Duration = Duration::from_secs(60);
const ADAPTIVE_OPTIMIZER_VERSION: &str = "exhaustive_whole_step_v1";
const MAX_ADAPTIVE_EXACT_EVALUATIONS: u16 = 8_192;

#[derive(Debug)]
struct RebalanceSettlementBarrier {
    operation_id: String,
    token_symbol: String,
    direction: Direction,
    binance_after: Instant,
    wallet_after: Instant,
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

#[derive(Debug)]
struct AdaptiveDepthSelection {
    book: SpotDepthBook,
    health: DepthHealthObservation,
    max_trade_notional: U256,
}

#[derive(Debug, Clone, Copy)]
struct AdmissionRuntimeContext {
    network_gas_price_wei: u128,
    native_price_token_a: Decimal,
    wallet_native_balance_wei: U256,
}

#[derive(Debug, Clone, Copy)]
struct AdaptiveSizingRuntimeLimits {
    max_trade_notional: U256,
    max_unhedged_notional: U256,
    max_recovery_loss: U256,
    min_expected_profit: U256,
    min_incremental_expected_profit: U256,
    recent_full_depth_max_age_ms: u64,
    recent_full_depth_max_update_delta: u64,
    top_of_book_max_trade_notional: U256,
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
            max_unhedged_notional: parse(limits.max_unhedged_notional, "exposure cap")?,
            max_recovery_loss: parse(limits.max_recovery_loss, "recovery-loss cap")?,
            min_expected_profit: parse(limits.min_expected_profit, "expected-profit floor")?,
            min_incremental_expected_profit: parse(
                limits.min_incremental_expected_profit,
                "incremental-profit floor",
            )?,
            recent_full_depth_max_age_ms: limits.depth_policy.recent_full_depth_max_age_ms,
            recent_full_depth_max_update_delta: limits
                .depth_policy
                .recent_full_depth_max_update_delta,
            top_of_book_max_trade_notional: parse(
                &limits
                    .depth_policy
                    .top_of_book_max_trade_notional_token_a_base_units,
                "top-of-book trade cap",
            )?,
        }))
    }
}

#[derive(Debug, Clone, Copy)]
struct AdaptiveCandidate {
    direction: ArbitrageDirection,
    trade: TradeEvaluation,
    economics: AdmissionEconomics,
    trade_notional: U256,
    unhedged_notional: U256,
    reservation_fits: bool,
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
    cache_new_probes: bool,
    exact_evaluations: u16,
    limit_exhausted: bool,
}

impl AdaptivePoolSearch {
    fn new() -> Self {
        Self {
            cached_probes: Vec::with_capacity(32),
            rejection_counts: BTreeMap::new(),
            cache_new_probes: true,
            exact_evaluations: 0,
            limit_exhausted: false,
        }
    }

    fn record(&mut self, amount: U256, probe: AdaptiveProbe) {
        if let Some(reason) = probe.rejection {
            *self.rejection_counts.entry(reason).or_default() += 1;
        }
        if self.cache_new_probes {
            self.cached_probes.push((amount, probe));
        }
    }
}

#[derive(Debug, Clone)]
struct ArbitragePlanFreshness {
    pair_id: String,
    pool_index: usize,
    pool_generation: u64,
}

#[derive(Debug, Clone)]
struct ArbitrageSettlementBarrier {
    pair_id: String,
    plan_id: String,
    pool_generation: u64,
    started_at: Instant,
}

#[derive(Debug, Clone)]
pub struct ArbitrageSettlementCatchupRequest {
    pub filter: EthLogFilter,
    pub from_block: u64,
    pub through_block: u64,
    plan_id: String,
    pool_index: usize,
    locator: crate::dex::events::PoolLocator,
    target: ChainLog,
    started_at: Instant,
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
    inflight_stuck: bool,
    settlement_stuck: bool,
}

fn rebalance_health_state(
    blocked: bool,
    inflight_age: Option<Duration>,
    settlement_age: Option<Duration>,
    operation_timeout: Duration,
    settlement_timeout: Duration,
) -> RebalanceHealthState {
    let inflight_stuck = inflight_age.is_some_and(|age| age >= operation_timeout);
    let settlement_stuck = settlement_age.is_some_and(|age| age >= settlement_timeout);
    RebalanceHealthState {
        healthy: !blocked && !inflight_stuck && !settlement_stuck,
        inflight_stuck,
        settlement_stuck,
    }
}

fn inventory_venue_label(venue: InventoryVenue) -> &'static str {
    match venue {
        InventoryVenue::Binance => "binance",
        InventoryVenue::Wallet => "wallet",
    }
}

fn adaptive_candidate_is_better(candidate: AdaptiveCandidate, current: AdaptiveCandidate) -> bool {
    candidate.economics.expected_profit_token_a > current.economics.expected_profit_token_a
        || (candidate.economics.expected_profit_token_a
            == current.economics.expected_profit_token_a
            && (candidate.unhedged_notional < current.unhedged_notional
                || (candidate.unhedged_notional == current.unhedged_notional
                    && (candidate.trade_notional < current.trade_notional
                        || (candidate.trade_notional == current.trade_notional
                            && (candidate.trade.token_b_amount < current.trade.token_b_amount
                                || (candidate.trade.token_b_amount
                                    == current.trade.token_b_amount
                                    && (adaptive_direction_order(candidate.direction)
                                        < adaptive_direction_order(current.direction)
                                        || (candidate.direction == current.direction
                                            && candidate.trade.pool_index
                                                < current.trade.pool_index)))))))))
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
    inventory: &InventoryReservations,
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
    user_data_ready: bool,
    gas_price_ready: bool,
}

impl TradingReadiness {
    const fn ready(self) -> bool {
        self.dex_ready && self.balances_ready && self.user_data_ready && self.gas_price_ready
    }
}

impl TradingEngine {
    pub fn new(
        config: AppConfig,
        domain_config: Arc<LoadedDomainConfig>,
        dex: DexMirror,
        telemetry: TelemetryHandle,
        rebalance: RebalanceTracker,
        execution: TradingExecutionHandles,
        binance_fee_bps: BinanceFeeBps,
    ) -> anyhow::Result<(Self, HotTelemetryTask)> {
        let symbols = domain_config
            .binance_symbols()
            .into_iter()
            .map(Arc::<str>::from);
        let gas_price_symbol = domain_config
            .snapshot()
            .pairs
            .iter()
            .find(|pair| pair.market_data_enabled)
            .and_then(|pair| pair.chain.gas_price_binance_symbol.clone())
            .context("enabled pair has no versioned gas-price Binance symbol")?;
        let wallet_gas_symbol = domain_config
            .snapshot()
            .pairs
            .iter()
            .find(|pair| pair.market_data_enabled)
            .map(|pair| pair.chain.gas_symbol.clone())
            .context("enabled pair has no versioned wallet gas symbol")?;
        let mut opportunities = OpportunityEngine::new(domain_config.snapshot(), &dex)?;
        for symbol in domain_config.binance_symbols() {
            opportunities.set_binance_fee_bps(
                &symbol,
                binance_fee_bps.buy,
                binance_fee_bps.sell,
            )?;
        }
        for (pool_index, generation) in opportunities.pool_generations() {
            execution
                .entry_preflight
                .update_dex_pool_generation(pool_index, generation);
        }
        for pair in domain_config
            .snapshot()
            .pairs
            .iter()
            .filter(|pair| pair.execution_enabled)
        {
            execution.entry_preflight.configure_max_transport_silence(
                &pair.binance.symbol,
                pair.strategy.max_transport_silence_ms(),
            );
        }
        let (hot_telemetry, hot_telemetry_task) =
            hot_telemetry_channel(&config, opportunities.pairs(), &dex, telemetry.clone())?;
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
                inventory: InventoryReservations::default(),
                binance_inventory_generation: 0,
                binance_user_data_connected: false,
                binance_user_data_clean: true,
                binance_orders: BTreeMap::new(),
                last_sequence_matched_quote_update: BTreeMap::new(),
                latest_sequence_matched_depth: BTreeMap::new(),
                depth_health_by_symbol: BTreeMap::new(),
                gas_price_symbol,
                wallet_gas_symbol,
                gas_price_connected: false,
                gas_price_generation: 0,
                gas_price_book: None,
                gas_price_transport_activity_at: None,
                binance_clock_sync: None,
                rebalance_inventory_reservation: None,
                next_inventory_reservation: 0,
                pending_rebalance: None,
                rebalance_inflight: false,
                rebalance_inflight_since: None,
                rebalance_blocked: false,
                rebalance_settlement: None,
                last_rebalance_health_log_at: None,
                last_depth_health_log_at: None,
                last_binance_price_health_log_at: None,
                last_inventory_blocked_alert_at: None,
                entry_preflight: execution.entry_preflight,
                arbitrage_plan_freshness: BTreeMap::new(),
                arbitrage_settlement_barriers: BTreeMap::new(),
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
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                let mut balances = Vec::new();
                let mut locked_assets = Vec::new();
                for balance in &position.balances {
                    if !balance.locked.is_zero() {
                        locked_assets.push(balance.asset.clone());
                    }
                    if let Ok(decimals) = self.token_decimals(&balance.asset) {
                        balances.push((
                            balance.asset.clone(),
                            decimal_to_base_units_floor(balance.free, decimals)?,
                        ));
                    }
                }
                if !balances.is_empty() {
                    self.inventory.update_venue_assets(
                        InventoryVenue::Binance,
                        self.binance_inventory_generation,
                        balances,
                    )?;
                    self.reconcile_inventory_settlements(&reservations_before);
                }
                self.binance_user_data_clean &= locked_assets.is_empty();
                self.telemetry.emit(
                    "binance_user_account_position",
                    json!({
                        "engine_id": self.config.engine_id,
                        "event_time_ms": position.event_time_ms,
                        "last_account_update_ms": position.last_account_update_ms,
                        "changed_assets": position.balances.len(),
                        "locked_assets": locked_assets,
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
                self.refresh_phase(Instant::now());
                anyhow::bail!("Binance User Data Stream terminated at {event_time_ms}");
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
        let request = match event {
            DexStreamEvent::Log { log, received_at } => {
                if let LogApplyResult::Applied { pool_index, kind } = self.dex.apply_log(&log)? {
                    let request = self
                        .opportunities
                        .request_pool_refresh(pool_index, &self.dex)?;
                    let pool = self.dex.pool(pool_index)?;
                    self.telemetry.emit(
                        "dex_pool_event",
                        json!({
                            "engine_id": self.config.engine_id,
                            "pair_id": pool.pair_id,
                            "identity": format!("{:?}", pool.identity),
                            "kind": kind,
                            "block_number": log.block_number,
                            "transaction_index": log.transaction_index,
                            "log_index": log.log_index,
                            "engine_queue_age_us": received_at.elapsed().as_micros(),
                            "prepared_generation": request.generation(),
                            "prepared_state": "building",
                        }),
                    );
                    Some(request)
                } else {
                    None
                }
            }
            DexStreamEvent::Head { head, received_at } => {
                if self.dex.apply_head(head)? {
                    self.telemetry.emit(
                        "world_chain_head",
                        json!({
                            "engine_id": self.config.engine_id,
                            "block_number": head.number,
                            "engine_queue_age_us": received_at.elapsed().as_micros(),
                        }),
                    );
                }
                None
            }
        };
        self.refresh_phase(Instant::now());
        Ok(request)
    }

    pub fn on_prepared_pool(&mut self, result: PreparedPoolBuildResult) -> anyhow::Result<()> {
        let Some(prepared) = self.opportunities.finish_pool_refresh(result)? else {
            return Ok(());
        };
        let pool = self.dex.pool(prepared.pool_index)?;
        let pool_pair_id = pool.pair_id.clone();
        let pool_identity = format!("{:?}", pool.identity);
        self.entry_preflight
            .update_dex_pool_generation(prepared.pool_index, prepared.generation);
        self.reconcile_arbitrage_settlement(prepared.pool_index, prepared.generation);
        self.telemetry.emit(
            "dex_pool_prepared",
            json!({
                "engine_id": self.config.engine_id,
                "pair_id": pool_pair_id,
                "identity": pool_identity,
                "pool_index": prepared.pool_index,
                "prepared_generation": prepared.generation,
                "prepared_exact_output_segments": prepared.exact_output_segments,
                "prepared_exact_input_segments": prepared.exact_input_segments,
                "prepared_token_a_exact_input_segments": prepared.token_a_exact_input_segments,
                "build_time_us": prepared.build_time_us,
                "total_time_us": prepared.total_time_us,
            }),
        );
        self.refresh_phase(Instant::now());
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
                let exchange_event_to_socket_estimate_us = clock_sync.and_then(|clock_sync| {
                    estimate_exchange_event_to_socket_us(
                        received_unix_us,
                        exchange_event_ts_ms,
                        clock_sync.offset_ms,
                    )
                });
                let estimate_uncertainty_us = clock_sync.map(|clock_sync| {
                    clock_sync
                        .midpoint_uncertainty_us()
                        .saturating_add(BINANCE_JSON_TIME_RESOLUTION_US.saturating_mul(2))
                });
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
                        "exchange_timestamp_resolution_us": BINANCE_JSON_TIME_RESOLUTION_US,
                        "clock_offset_ms": clock_sync.map(|sync| sync.offset_ms),
                        "clock_offset_resolution_us": BINANCE_JSON_TIME_RESOLUTION_US,
                        "clock_sync_rtt_us": clock_sync.map(|sync| sync.round_trip_us),
                        "clock_sync_midpoint_uncertainty_us": clock_sync.map(BinanceClockSync::midpoint_uncertainty_us),
                        "clock_sync_age_ms": clock_sync.map(BinanceClockSync::age_ms),
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
            }),
        );
    }

    pub fn on_gas_market_event(&mut self, event: MarketEvent) -> anyhow::Result<()> {
        match event {
            MarketEvent::FeedConnected {
                symbol,
                generation,
                observed_at,
            } => {
                ensure!(
                    symbol.as_ref() == self.gas_price_symbol,
                    "gas feed symbol mismatch"
                );
                if generation >= self.gas_price_generation {
                    self.gas_price_connected = true;
                    self.gas_price_generation = generation;
                    self.gas_price_book = None;
                    self.gas_price_transport_activity_at = Some(observed_at);
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
                    self.gas_price_book = None;
                    self.gas_price_transport_activity_at = None;
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
                if accepted {
                    self.gas_price_transport_activity_at = Some(observed_at);
                }
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
                if quote.connection_generation == self.gas_price_generation
                    && self
                        .gas_price_book
                        .as_ref()
                        .is_none_or(|current| quote.update_id > current.update_id)
                {
                    self.gas_price_transport_activity_at = Some(quote.received_at);
                    self.hot_telemetry
                        .emit_binance_book(&quote, "gas_conversion", None, "stored");
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

    pub fn on_balance_event(&mut self, event: BalanceEvent) -> anyhow::Result<()> {
        let reservations_before = self
            .inventory
            .active_operation_ids()
            .into_iter()
            .map(str::to_owned)
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
                self.inventory.update_venue(
                    InventoryVenue::Binance,
                    self.binance_inventory_generation,
                    balances,
                )?;
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
                        "account_update_time_ms": snapshot.account_update_time_ms,
                        "account_type": snapshot.account_type,
                        "can_trade": snapshot.can_trade,
                        "balances": balances,
                        "request_duration_us": snapshot.request_duration_us,
                    }),
                );
                self.state.balances.apply_binance(snapshot);
            }
            BalanceEvent::BinanceOpenOrders {
                client_order_ids,
                observed_at,
            } => {
                if !client_order_ids.is_empty() {
                    self.binance_user_data_clean = false;
                }
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
                let wallet_inventory = snapshot
                    .token_balances
                    .iter()
                    .map(|balance| (balance.symbol.to_string(), balance.base_units))
                    .chain(std::iter::once((
                        self.wallet_gas_symbol.clone(),
                        snapshot.native_balance_wei,
                    )))
                    .collect::<Vec<_>>();
                self.inventory.update_venue(
                    InventoryVenue::Wallet,
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
                        "owner": format!("{:#x}", snapshot.owner),
                        "chain_id": snapshot.chain_id,
                        "block_number": snapshot.block_number,
                        "block_hash": format!("{:#x}", snapshot.block_hash),
                        "native_balance_wei": snapshot.native_balance_wei.to_string(),
                        "gas_price_wei": snapshot.gas_price_wei.to_string(),
                        "token_balances": token_balances,
                        "request_duration_us": snapshot.request_duration_us,
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
        self.evaluate_rebalance();
        self.refresh_phase(Instant::now());
        Ok(())
    }

    pub fn take_rebalance_execution(&mut self) -> anyhow::Result<Option<RebalanceEvaluation>> {
        let Some(evaluation) = self.pending_rebalance.take() else {
            return Ok(None);
        };
        ensure!(
            self.rebalance_inventory_reservation.is_none(),
            "a rebalance inventory reservation is already active"
        );
        let action = evaluation
            .plan
            .action
            .as_ref()
            .context("rebalance execution has no action")?;
        let venue = match action.direction {
            Direction::BinanceToWallet => InventoryVenue::Binance,
            Direction::WalletToBinance => InventoryVenue::Wallet,
        };
        let reservation_id = format!("rebalance-reservation-{}", self.next_inventory_reservation);
        self.next_inventory_reservation = self
            .next_inventory_reservation
            .checked_add(1)
            .context("inventory reservation sequence overflow")?;
        self.inventory.reserve(ReservationRequest {
            operation_id: reservation_id.clone(),
            purpose: ReservationPurpose::Rebalance,
            claims: vec![InventoryClaim {
                key: InventoryKey::new(venue, evaluation.token_symbol.clone())?,
                amount: action.amount,
            }],
            settlement_venues: [InventoryVenue::Binance, InventoryVenue::Wallet]
                .into_iter()
                .collect(),
        })?;
        self.rebalance_inventory_reservation = Some(reservation_id.clone());
        self.telemetry.emit(
            "inventory_reserved",
            json!({
                "engine_id": self.config.engine_id,
                "operation_id": reservation_id,
                "purpose": "rebalance",
                "venue": format!("{venue:?}"),
                "asset": evaluation.token_symbol,
                "amount_base_units": action.amount.to_string(),
            }),
        );
        Ok(Some(evaluation))
    }

    pub fn on_rebalance_recovery_started(
        &mut self,
        operation: &RebalanceExecutionOperation,
    ) -> anyhow::Result<()> {
        self.rebalance_inflight = true;
        self.rebalance_inflight_since = Some(Instant::now());
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
    ) -> anyhow::Result<()> {
        self.rebalance_inflight = false;
        self.rebalance_inflight_since = None;
        match result {
            Ok(operation) => {
                if let (Some(binance), Some(wallet)) = (
                    self.state.balances.binance.as_ref(),
                    self.state.balances.wallet.as_ref(),
                ) {
                    self.rebalance_settlement = Some(RebalanceSettlementBarrier {
                        operation_id: operation.intent.operation_id.clone(),
                        token_symbol: operation.intent.token_symbol.clone(),
                        direction: operation.intent.direction,
                        binance_after: binance.observed_at,
                        wallet_after: wallet.observed_at,
                        started_at: Instant::now(),
                    });
                }
                self.telemetry.emit(
                    "rebalance_execution_completed",
                    json!({
                        "engine_id": self.config.engine_id,
                        "operation_id": operation.intent.operation_id,
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
            Err(error) => {
                self.rebalance_blocked = true;
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
    ) -> anyhow::Result<()> {
        self.rebalance_inflight = false;
        self.rebalance_inflight_since = None;
        match result {
            Ok(operation) => {
                let reservation_id = self
                    .rebalance_inventory_reservation
                    .as_deref()
                    .context("rebalance completed without an inventory reservation")?;
                self.inventory.mark_pending_settlement(reservation_id)?;
                if let (Some(binance), Some(wallet)) = (
                    self.state.balances.binance.as_ref(),
                    self.state.balances.wallet.as_ref(),
                ) {
                    self.rebalance_settlement = Some(RebalanceSettlementBarrier {
                        operation_id: operation.intent.operation_id.clone(),
                        token_symbol: operation.intent.token_symbol.clone(),
                        direction: operation.intent.direction,
                        binance_after: binance.observed_at,
                        wallet_after: wallet.observed_at,
                        started_at: Instant::now(),
                    });
                }
                self.telemetry.emit(
                    "rebalance_execution_completed",
                    json!({
                        "engine_id": self.config.engine_id,
                        "operation_id": operation.intent.operation_id,
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
                self.rebalance_blocked = true;
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

    fn token_decimals(&self, symbol: &str) -> anyhow::Result<u8> {
        self.domain_config
            .snapshot()
            .pairs
            .iter()
            .flat_map(|pair| [&pair.token_a, &pair.token_b])
            .find(|token| token.symbol == symbol)
            .map(|token| token.decimals)
            .with_context(|| format!("no configured decimals for inventory asset {symbol}"))
    }

    fn reconcile_inventory_settlements(&mut self, reservations_before: &[String]) {
        for operation_id in reservations_before {
            if self.inventory.reservation(operation_id).is_some() {
                continue;
            }
            self.telemetry.emit(
                "inventory_settlement_reconciled",
                json!({
                    "engine_id": self.config.engine_id,
                    "operation_id": operation_id,
                }),
            );
            if self.rebalance_inventory_reservation.as_ref() == Some(operation_id) {
                self.rebalance_inventory_reservation = None;
            }
        }
    }

    fn evaluate_rebalance(&mut self) {
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
                    "token": barrier.token_symbol,
                    "direction": format!("{:?}", barrier.direction),
                }),
            );
        }
        match self.rebalance.evaluate(binance, wallet) {
            Ok(evaluations) => {
                let mode = self.config.rebalance_execution_mode.as_str();
                for evaluation in evaluations {
                    let action = evaluation.plan.action.as_ref();
                    self.telemetry.emit(
                        "rebalance_plan_evaluated",
                        json!({
                            "engine_id": self.config.engine_id,
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
                let pending_action = self.rebalance.pending_action();
                if mode == "full_live"
                    && !self.rebalance_inflight
                    && !self.rebalance_blocked
                    && self.rebalance_settlement.is_none()
                    && self.pending_rebalance.is_none()
                    && let Some(evaluation) = pending_action
                {
                    self.rebalance_inflight = true;
                    self.rebalance_inflight_since = Some(Instant::now());
                    self.pending_rebalance = Some(evaluation);
                }
            }
            Err(error) => {
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

    fn select_adaptive_depth(
        &self,
        quote: &TopOfBook,
        depth: Option<&SpotDepthBook>,
        limits: AdaptiveSizingRuntimeLimits,
        baseline_token_a: U256,
        now: Instant,
    ) -> anyhow::Result<AdaptiveDepthSelection> {
        let observation = self.depth_observation(quote, depth, now);
        let health = classify_depth_health(observation, depth.is_some(), Some(limits));
        let (book, max_trade_notional) = match health.source {
            AdaptiveDepthSource::SequenceMatchedFullDepth => (
                depth.context("sequence-matched depth disappeared")?.clone(),
                limits.max_trade_notional,
            ),
            AdaptiveDepthSource::RecentFullDepth => (
                depth
                    .context("recent full depth disappeared")?
                    .reconciled_with_top(
                        quote.update_id,
                        quote.bid_price,
                        quote.bid_quantity,
                        quote.ask_price,
                        quote.ask_quantity,
                    )?,
                limits.max_trade_notional,
            ),
            AdaptiveDepthSource::TopOfBookOnly => {
                let configured_cap = if limits.top_of_book_max_trade_notional.is_zero() {
                    baseline_token_a
                } else {
                    limits.top_of_book_max_trade_notional
                };
                (
                    SpotDepthBook::from_top(
                        quote.symbol.to_string(),
                        quote.update_id,
                        quote.bid_price,
                        quote.bid_quantity,
                        quote.ask_price,
                        quote.ask_quantity,
                    )?,
                    configured_cap.min(limits.max_trade_notional),
                )
            }
        };
        Ok(AdaptiveDepthSelection {
            book,
            health,
            max_trade_notional,
        })
    }

    fn evaluate_adaptive_sizing(
        &self,
        quote: &TopOfBook,
        selection: &AdaptiveDepthSelection,
        evaluation: PairEvaluation,
        mut limits: AdaptiveSizingRuntimeLimits,
        admission_context: AdmissionRuntimeContext,
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
        limits.max_trade_notional = selection.max_trade_notional;
        let depth = &selection.book;
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
            let baseline_inputs = AdmissionInputs {
                symbol: &pair.symbol,
                direction: adaptive_trade_direction(direction_evaluation.direction),
                token_b_amount: baseline_trade.token_b_amount,
                token_b_step_base_units: pair.token_b_step(),
                token_a_decimals: pair.token_a_decimals,
                token_b_decimals: pair.token_b_decimals,
                binance_buy_fee_bps: pair.binance_buy_fee_bps,
                binance_sell_fee_bps: pair.binance_sell_fee_bps,
                expected_cost_token_a: baseline_trade.cost_token_a,
                expected_proceeds_token_a: baseline_trade.proceeds_token_a,
                opportunity_threshold_met: baseline_trade.meets_threshold,
                network_gas_price_wei: admission_context.network_gas_price_wei,
                native_price_token_a: admission_context.native_price_token_a,
                wallet_native_balance_wei: admission_context.wallet_native_balance_wei,
            };
            if let Some(economics) = evaluate_admission(depth, baseline_inputs)? {
                baseline_by_direction[direction_index] = Some(AdaptiveCandidate {
                    direction: direction_evaluation.direction,
                    trade: baseline_trade,
                    economics,
                    trade_notional: baseline_trade
                        .cost_token_a
                        .max(baseline_trade.proceeds_token_a),
                    unhedged_notional: economics
                        .recovery_sell_quote_token_a
                        .max(economics.recovery_buy_quote_token_a),
                    reservation_fits: self.adaptive_exact_reservation_fits(
                        pair,
                        direction_evaluation.direction,
                        baseline_trade,
                        economics,
                    )?,
                });
            }

            for &pool_index in pair.pool_indices() {
                let (pool_winner, search) = self.search_adaptive_pool(
                    quote,
                    depth,
                    evaluation.pair_index,
                    direction_evaluation.direction,
                    pool_index,
                    baseline_trade.token_b_amount,
                    direction_evaluation.cex_top_token_b_amount,
                    limits,
                    admission_context.network_gas_price_wei,
                    admission_context.native_price_token_a,
                    admission_context.wallet_native_balance_wei,
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
                fallback_reason = "baseline_admission_unavailable";
                return false;
            };
            if candidate.trade.token_b_amount <= baseline.trade.token_b_amount {
                fallback_reason = "not_larger_than_baseline";
                return false;
            }
            let required = baseline
                .economics
                .expected_profit_token_a
                .checked_add(limits.min_incremental_expected_profit);
            if required
                .is_none_or(|required| candidate.economics.expected_profit_token_a < required)
            {
                fallback_reason = "incremental_profit_floor";
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
                    .max_by_key(|candidate| candidate.economics.expected_profit_token_a)
            });
        let selected_for_telemetry = selected.or(baseline);
        let execution_candidate = matches!(
            pair_config.adaptive_sizing,
            AdaptiveSizingConfig::Adaptive { .. }
        )
        .then_some(selected)
        .flatten();
        let rejection_counts = rejection_counts
            .into_iter()
            .map(|(reason, count)| (reason.to_owned(), Value::from(count)))
            .collect::<serde_json::Map<_, _>>();
        let calculation_us = started.elapsed().as_micros();
        let mut payload = json!({
            "engine_id": self.config.engine_id,
            "pair_id": pair.pair_id,
            "symbol": quote.symbol.as_ref(),
            "update_id": quote.update_id,
            "configured_mode": pair_config.adaptive_sizing.mode(),
            "optimizer_version": ADAPTIVE_OPTIMIZER_VERSION,
            "search_mode": "exhaustive_whole_step",
            "max_exact_evaluations_per_pool": MAX_ADAPTIVE_EXACT_EVALUATIONS,
            "exact_evaluation_count": exact_evaluations,
            "max_trade_notional_token_a_base_units": limits.max_trade_notional.to_string(),
            "max_unhedged_notional_token_a_base_units": limits.max_unhedged_notional.to_string(),
            "max_recovery_loss_token_a_base_units": limits.max_recovery_loss.to_string(),
            "min_expected_profit_token_a_base_units": limits.min_expected_profit.to_string(),
            "min_incremental_expected_profit_token_a_base_units": limits.min_incremental_expected_profit.to_string(),
            "baseline_direction": baseline.map(|candidate| candidate.direction.as_str()),
            "baseline_pool_index": baseline.map(|candidate| candidate.trade.pool_index),
            "baseline_token_b_base_units": baseline.map(|candidate| candidate.trade.token_b_amount.to_string()),
            "baseline_cost_token_a_base_units": baseline.map(|candidate| candidate.trade.cost_token_a.to_string()),
            "baseline_proceeds_token_a_base_units": baseline.map(|candidate| candidate.trade.proceeds_token_a.to_string()),
            "baseline_expected_profit_token_a_base_units": baseline.map(|candidate| candidate.economics.expected_profit_token_a.to_string()),
            "baseline_expected_profit_after_gas_token_a_base_units": baseline.map(|candidate| candidate.economics.expected_profit_after_gas_token_a.to_string()),
            "baseline_bounded_profit_token_a_base_units": baseline.map(|candidate| candidate.economics.bounded_profit_token_a.to_string()),
            "baseline_recovery_loss_bound_token_a_base_units": baseline.map(|candidate| candidate.economics.recovery_loss_token_a.to_string()),
            "selected_sizing_mode": if selected.is_some() { "adaptive" } else { "baseline" },
            "selected_direction": selected_for_telemetry.map(|candidate| candidate.direction.as_str()),
            "selected_pool_index": selected_for_telemetry.map(|candidate| candidate.trade.pool_index),
            "selected_token_b_base_units": selected_for_telemetry.map(|candidate| candidate.trade.token_b_amount.to_string()),
            "selected_cost_token_a_base_units": selected_for_telemetry.map(|candidate| candidate.trade.cost_token_a.to_string()),
            "selected_proceeds_token_a_base_units": selected_for_telemetry.map(|candidate| candidate.trade.proceeds_token_a.to_string()),
            "selected_expected_profit_token_a_base_units": selected_for_telemetry.map(|candidate| candidate.economics.expected_profit_token_a.to_string()),
            "selected_expected_profit_after_gas_token_a_base_units": selected_for_telemetry.map(|candidate| candidate.economics.expected_profit_after_gas_token_a.to_string()),
            "selected_bounded_profit_token_a_base_units": selected_for_telemetry.map(|candidate| candidate.economics.bounded_profit_token_a.to_string()),
            "selected_trade_notional_token_a_base_units": selected_for_telemetry.map(|candidate| candidate.trade_notional.to_string()),
            "selected_unhedged_notional_token_a_base_units": selected_for_telemetry.map(|candidate| candidate.unhedged_notional.to_string()),
            "selected_recovery_loss_token_a_base_units": selected_for_telemetry.map(|candidate| candidate.economics.recovery_loss_token_a.to_string()),
            "selected_recovery_loss_bound_token_a_base_units": selected_for_telemetry.map(|candidate| candidate.economics.recovery_loss_token_a.to_string()),
            "selected_reservation_fits": selected_for_telemetry.map(|candidate| candidate.reservation_fits),
            "fallback_reason": selected.is_none().then_some(fallback_reason),
            "rejection_counts": Value::Object(rejection_counts),
            "calculation_us": calculation_us,
            "execution_size_changed": execution_candidate.is_some(),
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
            "admission_liquidity_source".to_owned(),
            json!(selection.health.source.as_str()),
        );
        object.insert(
            "depth_source".to_owned(),
            json!(selection.health.source.as_str()),
        );
        object.insert(
            "depth_source_reason".to_owned(),
            json!(selection.health.source_reason),
        );
        object.insert("depth_age_ms".to_owned(), json!(selection.health.age_ms));
        object.insert(
            "depth_update_delta".to_owned(),
            json!(selection.health.update_delta),
        );
        object.insert(
            "top_matches".to_owned(),
            Value::Bool(selection.health.top_matches),
        );
        object.insert(
            "top_mismatch_reason".to_owned(),
            json!(selection.health.top_mismatch_reason),
        );
        self.telemetry
            .emit("arbitrage_adaptive_sizing_evaluated", payload);
        Ok(execution_candidate)
    }

    #[allow(clippy::too_many_arguments)]
    fn adaptive_probe(
        &self,
        search: &mut AdaptivePoolSearch,
        quote: &TopOfBook,
        depth: &SpotDepthBook,
        pair_index: usize,
        direction: ArbitrageDirection,
        pool_index: usize,
        token_b_amount: U256,
        limits: AdaptiveSizingRuntimeLimits,
        network_gas_price_wei: u128,
        native_price_token_a: Decimal,
        wallet_native_balance_wei: U256,
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
        let pair = self.opportunities.pair(pair_index)?;
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
        let trade_notional = trade.cost_token_a.max(trade.proceeds_token_a);
        if trade_notional > limits.max_trade_notional {
            let probe = rejection("trade_cap");
            search.record(token_b_amount, probe);
            return Ok(probe);
        }
        let inputs = AdmissionInputs {
            symbol: &pair.symbol,
            direction: adaptive_trade_direction(direction),
            token_b_amount,
            token_b_step_base_units: pair.token_b_step(),
            token_a_decimals: pair.token_a_decimals,
            token_b_decimals: pair.token_b_decimals,
            binance_buy_fee_bps: pair.binance_buy_fee_bps,
            binance_sell_fee_bps: pair.binance_sell_fee_bps,
            expected_cost_token_a: trade.cost_token_a,
            expected_proceeds_token_a: trade.proceeds_token_a,
            opportunity_threshold_met: trade.meets_threshold,
            network_gas_price_wei,
            native_price_token_a,
            wallet_native_balance_wei,
        };
        let Some(economics) = evaluate_admission(depth, inputs)? else {
            let probe = rejection("recovery_depth");
            search.record(token_b_amount, probe);
            return Ok(probe);
        };
        if !economics.native_gas_covered {
            let probe = rejection("gas");
            search.record(token_b_amount, probe);
            return Ok(probe);
        }
        let unhedged_notional = economics
            .recovery_sell_quote_token_a
            .max(economics.recovery_buy_quote_token_a);
        if unhedged_notional > limits.max_unhedged_notional {
            let probe = rejection("exposure_cap");
            search.record(token_b_amount, probe);
            return Ok(probe);
        }
        if economics.recovery_loss_token_a > limits.max_recovery_loss {
            let probe = rejection("recovery_loss_cap");
            search.record(token_b_amount, probe);
            return Ok(probe);
        }
        if economics.expected_profit_token_a < limits.min_expected_profit {
            let probe = rejection("expected_profit_floor");
            search.record(token_b_amount, probe);
            return Ok(probe);
        }
        let reservation_fits =
            self.adaptive_exact_reservation_fits(pair, direction, trade, economics)?;
        if !reservation_fits {
            let probe = rejection("inventory");
            search.record(token_b_amount, probe);
            return Ok(probe);
        }
        let probe = AdaptiveProbe {
            candidate: Some(AdaptiveCandidate {
                direction,
                trade,
                economics,
                trade_notional,
                unhedged_notional,
                reservation_fits,
            }),
            rejection: None,
        };
        search.record(token_b_amount, probe);
        Ok(probe)
    }

    fn adaptive_exact_reservation_fits(
        &self,
        pair: &crate::opportunity::PairRuntime,
        direction: ArbitrageDirection,
        trade: TradeEvaluation,
        economics: AdmissionEconomics,
    ) -> anyhow::Result<bool> {
        let token_a_amount = match direction {
            ArbitrageDirection::BuyTokenBOnDexSellOnCex => trade.cost_token_a,
            ArbitrageDirection::BuyTokenBOnCexSellOnDex => {
                trade.cost_token_a.max(economics.recovery_buy_quote_token_a)
            }
        };
        let (token_a_venue, token_b_venue) = match direction {
            ArbitrageDirection::BuyTokenBOnDexSellOnCex => {
                (InventoryVenue::Wallet, InventoryVenue::Binance)
            }
            ArbitrageDirection::BuyTokenBOnCexSellOnDex => {
                (InventoryVenue::Binance, InventoryVenue::Wallet)
            }
        };
        Ok(self
            .inventory
            .available_asset(token_a_venue, &pair.token_a_symbol)
            .is_ok_and(|available| available >= token_a_amount)
            && self
                .inventory
                .available_asset(token_b_venue, &pair.token_b_symbol)
                .is_ok_and(|available| available >= trade.token_b_amount)
            && self
                .inventory
                .available_asset(InventoryVenue::Wallet, &self.wallet_gas_symbol)
                .is_ok_and(|available| available >= economics.maximum_gas_wei))
    }

    #[allow(clippy::too_many_arguments)]
    fn search_adaptive_pool(
        &self,
        quote: &TopOfBook,
        depth: &SpotDepthBook,
        pair_index: usize,
        direction: ArbitrageDirection,
        pool_index: usize,
        baseline_amount: U256,
        cex_top_amount: U256,
        limits: AdaptiveSizingRuntimeLimits,
        network_gas_price_wei: u128,
        native_price_token_a: Decimal,
        wallet_native_balance_wei: U256,
    ) -> anyhow::Result<(Option<AdaptiveCandidate>, AdaptivePoolSearch)> {
        let step = self.opportunities.pair(pair_index)?.token_b_step();
        let low_steps = baseline_amount / step;
        let high_steps = cex_top_amount / step;
        let mut search = AdaptivePoolSearch::new();
        if low_steps == U256::ZERO || high_steps < low_steps {
            return Ok((None, search));
        }
        let probe_steps = |steps: U256, search: &mut AdaptivePoolSearch| {
            let amount = steps
                .checked_mul(step)
                .context("adaptive candidate amount overflow")?;
            self.adaptive_probe(
                search,
                quote,
                depth,
                pair_index,
                direction,
                pool_index,
                amount,
                limits,
                network_gas_price_wei,
                native_price_token_a,
                wallet_native_balance_wei,
            )
        };
        let Some(low_candidate) = probe_steps(low_steps, &mut search)?.candidate else {
            return Ok((None, search));
        };

        // Locate the monotone end of the feasible sizing domain first. The
        // exhaustive pass below still checks every whole step inside it.
        let upper_steps = if probe_steps(high_steps, &mut search)?.candidate.is_some() {
            high_steps
        } else {
            let mut low = low_steps;
            let mut high = high_steps;
            while high - low > U256::ONE {
                let mid = low + ((high - low) / U256::from(2_u8));
                if probe_steps(mid, &mut search)?.candidate.is_some() {
                    low = mid;
                } else {
                    high = mid;
                }
            }
            low
        };

        let domain_size = upper_steps
            .checked_sub(low_steps)
            .and_then(|delta| delta.checked_add(U256::ONE))
            .context("adaptive sizing domain overflow")?;
        let remaining_budget =
            MAX_ADAPTIVE_EXACT_EVALUATIONS.saturating_sub(search.exact_evaluations);
        if domain_size > U256::from(remaining_budget) {
            search.limit_exhausted = true;
            return Ok((None, search));
        }
        search.cache_new_probes = false;
        let mut winner = low_candidate;
        let mut steps = low_steps;
        while steps <= upper_steps {
            if let Some(candidate) = probe_steps(steps, &mut search)?.candidate
                && adaptive_candidate_is_better(candidate, winner)
            {
                winner = candidate;
            }
            steps = steps
                .checked_add(U256::ONE)
                .context("adaptive step index overflow")?;
        }
        Ok((Some(winner), search))
    }

    fn submit_paper_opportunity(
        &mut self,
        quote: &TopOfBook,
        evaluation: PairEvaluation,
        admission_liquidity: AdmissionLiquidity<'_>,
        depth: Option<&SpotDepthBook>,
        evaluation_trigger: &'static str,
        evaluation_started_at: Instant,
    ) -> anyhow::Result<bool> {
        let admission_started = Instant::now();
        let Some(handle) = self.paper_trades.clone() else {
            return Ok(false);
        };
        let pair = self.opportunities.pair(evaluation.pair_index)?;
        let pair_id = pair.pair_id.clone();
        let pair_symbol = pair.symbol.clone();
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
        let adaptive_limits = AdaptiveSizingRuntimeLimits::parse(&pair_config.adaptive_sizing)?;
        if !evaluation
            .dex_buy_cex_sell
            .baseline
            .is_some_and(|trade| trade.meets_threshold)
            && !evaluation
                .cex_buy_dex_sell
                .baseline
                .is_some_and(|trade| trade.meets_threshold)
        {
            return Ok(false);
        }
        let token_a_decimals = pair.token_a_decimals;
        let token_b_decimals = pair.token_b_decimals;
        let binance_buy_fee_bps = pair.binance_buy_fee_bps;
        let binance_sell_fee_bps = pair.binance_sell_fee_bps;
        let baseline_token_a = pair.baseline_token_a();
        let token_b_step = pair.token_b_step();
        let wallet = self
            .state
            .balances
            .wallet
            .as_ref()
            .context("admission has no wallet snapshot")?;
        let network_gas_price_wei = wallet.gas_price_wei;
        let wallet_native_balance_wei = wallet.native_balance_wei;
        let native_price_token_a = self
            .native_price_token_a()
            .context("admission has no native-token price")?;

        let adaptive_selection = adaptive_limits
            .map(|limits| {
                self.select_adaptive_depth(quote, depth, limits, baseline_token_a, Instant::now())
            })
            .transpose()?;
        let adaptive_candidate = if let (Some(selection), Some(limits)) =
            (adaptive_selection.as_ref(), adaptive_limits)
        {
            match self.evaluate_adaptive_sizing(
                quote,
                selection,
                evaluation,
                limits,
                AdmissionRuntimeContext {
                    network_gas_price_wei,
                    native_price_token_a,
                    wallet_native_balance_wei,
                },
            ) {
                Ok(candidate) => candidate,
                Err(error) => {
                    self.telemetry.emit(
                        "arbitrage_adaptive_sizing_evaluated",
                        json!({
                            "engine_id": self.config.engine_id,
                            "pair_id": pair_id,
                            "symbol": quote.symbol.as_ref(),
                            "update_id": quote.update_id,
                            "optimizer_version": ADAPTIVE_OPTIMIZER_VERSION,
                            "selected_sizing_mode": "baseline",
                            "fallback_reason": "optimizer_error",
                            "error": format!("{error:#}"),
                            "execution_size_changed": false,
                            "depth_source": selection.health.source.as_str(),
                            "depth_source_reason": selection.health.source_reason,
                            "depth_age_ms": selection.health.age_ms,
                            "depth_update_delta": selection.health.update_delta,
                            "top_matches": selection.health.top_matches,
                            "top_mismatch_reason": selection.health.top_mismatch_reason,
                        }),
                    );
                    None
                }
            }
        } else {
            None
        };

        let observed_depth_health = adaptive_selection
            .as_ref()
            .map(|selection| selection.health)
            .unwrap_or_else(|| {
                classify_depth_health(
                    self.depth_observation(quote, depth, Instant::now()),
                    depth.is_some(),
                    adaptive_limits,
                )
            });
        let execution_depth_health = if adaptive_candidate.is_some() {
            observed_depth_health
        } else if matches!(admission_liquidity, AdmissionLiquidity::DexFirstTop) {
            DepthHealthObservation {
                source: AdaptiveDepthSource::TopOfBookOnly,
                source_reason: "baseline_dex_first_fast_path",
                ..observed_depth_health
            }
        } else {
            observed_depth_health
        };

        let mut candidates = Vec::with_capacity(2);
        let mut needs_depth_fallback = false;
        if let Some(candidate) = adaptive_candidate {
            candidates.push((
                adaptive_trade_direction(candidate.direction),
                candidate.trade,
                candidate.economics,
                adaptive_selection
                    .as_ref()
                    .map(|selection| selection.health.source.as_str())
                    .unwrap_or("top_of_book_only"),
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
            let inputs = AdmissionInputs {
                symbol: &pair_symbol,
                direction: trade_direction,
                token_b_amount: trade.token_b_amount,
                token_b_step_base_units: token_b_step,
                token_a_decimals,
                token_b_decimals,
                binance_buy_fee_bps,
                binance_sell_fee_bps,
                expected_cost_token_a: trade.cost_token_a,
                expected_proceeds_token_a: trade.proceeds_token_a,
                opportunity_threshold_met: trade.meets_threshold,
                network_gas_price_wei,
                native_price_token_a,
                wallet_native_balance_wei,
            };
            let (economics, liquidity_source) = match admission_liquidity {
                AdmissionLiquidity::DexFirstTop => {
                    let Some(economics) = evaluate_dex_first_admission(quote, inputs)? else {
                        needs_depth_fallback = true;
                        self.telemetry.emit(
                            "arbitrage_admission_deferred",
                            json!({
                                "engine_id": self.config.engine_id,
                                "pair_id": pair_id,
                                "symbol": quote.symbol.as_ref(),
                                "update_id": quote.update_id,
                                "direction": match trade_direction {
                                    TradeDirection::BuyTokenBOnDexSellOnCex => "buy_token_b_on_dex_sell_on_cex",
                                    TradeDirection::BuyTokenBOnCexSellOnDex => "buy_token_b_on_cex_sell_on_dex",
                                },
                                "reason": "insufficient_relevant_top_quantity",
                            }),
                        );
                        continue;
                    };
                    (economics, "book_ticker_relevant_top")
                }
                AdmissionLiquidity::FullDepth(depth) => {
                    let Some(economics) = evaluate_admission(depth, inputs)? else {
                        self.emit_admission_risk_rejection(
                            quote,
                            &pair_id,
                            trade_direction,
                            "insufficient_recovery_depth",
                            None,
                        );
                        continue;
                    };
                    (economics, "sequence_matched_full_depth")
                }
            };
            if !economics.native_gas_covered {
                self.emit_admission_risk_rejection(
                    quote,
                    &pair_id,
                    trade_direction,
                    "insufficient_native_gas_reserve",
                    Some(economics),
                );
                continue;
            }
            if let Some(limits) = adaptive_limits {
                let trade_notional = trade.cost_token_a.max(trade.proceeds_token_a);
                let unhedged_notional = economics
                    .recovery_sell_quote_token_a
                    .max(economics.recovery_buy_quote_token_a);
                let rejection_reason = if trade_notional > limits.max_trade_notional {
                    Some("trade_notional_cap")
                } else if unhedged_notional > limits.max_unhedged_notional {
                    Some("unhedged_notional_cap")
                } else if economics.recovery_loss_token_a > limits.max_recovery_loss {
                    Some("recovery_loss_cap")
                } else if economics.expected_profit_token_a < limits.min_expected_profit {
                    Some("expected_profit_floor")
                } else {
                    None
                };
                if let Some(reason) = rejection_reason {
                    self.emit_admission_risk_rejection(
                        quote,
                        &pair_id,
                        trade_direction,
                        reason,
                        Some(economics),
                    );
                    continue;
                }
            }
            // Profitability is decided only by the Rails-compatible gross
            // spread proof carried by `trade.meets_threshold`. Recovery,
            // inventory, and gas coverage remain operational safety bounds.
            candidates.push((trade_direction, trade, economics, liquidity_source));
        }
        let candidate = candidates
            .into_iter()
            .max_by_key(|(_, _, economics, _)| economics.expected_profit_token_a);
        let Some((direction, trade, economics, liquidity_source)) = candidate else {
            return Ok(needs_depth_fallback);
        };
        let dex_pool_generation = self.opportunities.pool_generation(trade.pool_index)?;
        if let Some(barrier) = self.arbitrage_settlement_barriers.get(&trade.pool_index)
            && dex_pool_generation <= barrier.pool_generation
        {
            self.telemetry.emit(
                "arbitrage_admission_rejected",
                json!({
                    "engine_id": self.config.engine_id,
                    "pair_id": pair_id,
                    "symbol": quote.symbol.as_ref(),
                    "update_id": quote.update_id,
                    "plan_id": format!(
                        "candidate-{}-{}-p{}-g{}-{}",
                        quote.received_unix_us,
                        quote.update_id,
                        trade.pool_index,
                        dex_pool_generation,
                        match direction {
                            TradeDirection::BuyTokenBOnDexSellOnCex => "ds",
                            TradeDirection::BuyTokenBOnCexSellOnDex => "cs",
                        }
                    ),
                    "reason": "dex_settlement_waiting",
                    "blocked_by_plan_id": barrier.plan_id,
                    "pool_index": trade.pool_index,
                    "pool_generation": dex_pool_generation,
                    "barrier_generation": barrier.pool_generation,
                    "barrier_age_ms": barrier.started_at.elapsed().as_millis(),
                }),
            );
            return Ok(false);
        }
        let token_a_symbol = pair_config.token_a.symbol.clone();
        let token_b_symbol = pair_config.token_b.symbol.clone();
        let gas_symbol = pair_config.chain.gas_symbol.clone();
        let deadline_unix_seconds =
            admission_deadline_unix_seconds(quote.received_unix_us, quote.received_at.elapsed())?;
        let dex_plan = DexSwapPlan::build(
            pair_config,
            self.dex.pool(trade.pool_index)?,
            direction,
            trade,
            deadline_unix_seconds,
        )?;
        let opportunity = PaperOpportunity {
            source_revision: self.domain_config.snapshot().source.revision.clone(),
            pair_id: pair_id.clone(),
            symbol: pair_symbol.clone(),
            update_id: quote.update_id,
            received_unix_us: quote.received_unix_us,
            direction,
            dex_pool_index: trade.pool_index,
            dex_pool_generation,
            token_b_base_units: u256_to_i128(trade.token_b_amount, "paper token-B amount")?,
            token_b_step_base_units: u256_to_i128(token_b_step, "paper token-B step")?,
            cost_token_a_base_units: u256_to_i128(trade.cost_token_a, "paper token-A cost")?,
            proceeds_token_a_base_units: u256_to_i128(
                trade.proceeds_token_a,
                "paper token-A proceeds",
            )?,
            admission: AdmissionRiskBounds {
                opportunity_threshold_met: economics.opportunity_threshold_met,
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
                cex_primary_top_quantity: match admission_liquidity {
                    AdmissionLiquidity::DexFirstTop => economics.primary_quantity,
                    AdmissionLiquidity::FullDepth(_) => Decimal::ZERO,
                },
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
                maximum_recovery_loss_token_a_base_units: u256_to_u128(
                    economics.recovery_loss_token_a,
                    "paper maximum recovery loss",
                )?,
                maximum_fee_per_gas_wei: economics.maximum_fee_per_gas_wei,
                gas_conversion_price_token_a: native_price_token_a,
                maximum_gas_cost_token_a_base_units: u256_to_u128(
                    economics.maximum_gas_cost_token_a,
                    "paper maximum gas cost",
                )?,
                bounded_profit_token_a_base_units: u256_to_u128(
                    economics.bounded_profit_token_a,
                    "paper bounded profit",
                )?,
            },
            dex_plan: dex_plan.clone(),
        };
        let plan_id = opportunity.plan_id();
        let dex_input_claim = U256::from(dex_plan.amount_in_base_units);
        let (token_a_claim, token_b_claim, gas_claim) =
            exact_execution_envelope_amounts(direction, dex_input_claim, trade, economics);
        let claims = match direction {
            TradeDirection::BuyTokenBOnDexSellOnCex => vec![
                InventoryClaim {
                    key: InventoryKey::new(InventoryVenue::Wallet, token_a_symbol)?,
                    amount: token_a_claim,
                },
                InventoryClaim {
                    key: InventoryKey::new(InventoryVenue::Binance, token_b_symbol)?,
                    amount: token_b_claim,
                },
                InventoryClaim {
                    key: InventoryKey::new(InventoryVenue::Wallet, gas_symbol)?,
                    amount: gas_claim,
                },
            ],
            TradeDirection::BuyTokenBOnCexSellOnDex => vec![
                InventoryClaim {
                    key: InventoryKey::new(InventoryVenue::Binance, token_a_symbol)?,
                    amount: token_a_claim,
                },
                InventoryClaim {
                    key: InventoryKey::new(InventoryVenue::Wallet, token_b_symbol)?,
                    amount: token_b_claim,
                },
                InventoryClaim {
                    key: InventoryKey::new(InventoryVenue::Wallet, gas_symbol)?,
                    amount: gas_claim,
                },
            ],
        };
        let request = ReservationRequest {
            operation_id: plan_id.clone(),
            purpose: ReservationPurpose::TradePrimary,
            claims: claims.clone(),
            settlement_venues: [InventoryVenue::Binance, InventoryVenue::Wallet]
                .into_iter()
                .collect(),
        };
        let reservation_started = Instant::now();
        match reservation_precheck(&self.inventory, &request) {
            ReservationPrecheck::Duplicate => {
                self.telemetry.emit(
                    "arbitrage_admission_rejected",
                    json!({
                        "engine_id": self.config.engine_id,
                        "plan_id": plan_id,
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
                        "reason": "inventory_reservation_conflict",
                    }),
                );
                return Ok(false);
            }
            ReservationPrecheck::Vacant => {}
        }
        if let Err(error) = self.inventory.reserve(request) {
            let claim_details = self.inventory_claim_details(&claims);
            self.log_trading_inventory_blocked(&pair_id, &pair_symbol, &plan_id, &claim_details);
            self.telemetry.emit(
                "arbitrage_admission_rejected",
                json!({
                    "engine_id": self.config.engine_id,
                    "plan_id": plan_id,
                    "reason": "insufficient_available_inventory",
                    "error": format!("{error:#}"),
                    "claims": claim_details,
                }),
            );
            return Ok(false);
        }
        let inventory_reservation_us = duration_us(reservation_started.elapsed());
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
                        "reason": "execution_lane_unavailable",
                    }),
                );
                return Ok(false);
            }
        }
        let mailbox_submit_us = duration_us(mailbox_submit_started.elapsed());
        let expected_profit_after_gas_bps_x100 = profit_bps_x100_u256(
            economics.expected_profit_after_gas_token_a,
            economics.gas_burdened_cost_token_a,
        )?;
        let expected_profit_after_gas_bps =
            format_bps_x100_u256(expected_profit_after_gas_bps_x100)?;
        let expected_profit_after_gas_threshold_met = expected_profit_after_gas_bps_x100
            >= U256::from(u64::from(EXPECTED_PROFIT_AFTER_GAS_THRESHOLD_BPS) * 100);
        let mut admitted_payload = json!({
            "engine_id": self.config.engine_id,
            "plan_id": &plan_id,
            "mode": self.config.arbitrage_execution_mode,
            "admission_liquidity_source": liquidity_source,
            "sizing_mode": if adaptive_candidate.is_some() { "adaptive" } else { "baseline" },
            "depth_source": execution_depth_health.source.as_str(),
            "depth_source_reason": execution_depth_health.source_reason,
            "depth_age_ms": execution_depth_health.age_ms,
            "depth_update_delta": execution_depth_health.update_delta,
            "top_matches": execution_depth_health.top_matches,
            "top_mismatch_reason": execution_depth_health.top_mismatch_reason,
            "inventory_reservation_policy": "exact_execution_envelope_v1",
            "evaluation_trigger": evaluation_trigger,
            "market_to_admitted_us": duration_us(quote.received_at.elapsed()),
            "trigger_to_admitted_us": duration_us(evaluation_started_at.elapsed()),
            "admission_total_us": duration_us(admission_started.elapsed()),
            "inventory_reservation_us": inventory_reservation_us,
            "mailbox_submit_us": mailbox_submit_us,
            "inventory_claims": self.inventory_claim_details(&claims),
            "execution_slippage_bps": trade.execution_slippage_bps,
            "cex_primary_limit_price": match direction {
                TradeDirection::BuyTokenBOnDexSellOnCex => quote.bid_price.to_string(),
                TradeDirection::BuyTokenBOnCexSellOnDex => quote.ask_price.to_string(),
            },
            "cex_primary_top_quantity": match admission_liquidity {
                AdmissionLiquidity::DexFirstTop => Some(economics.primary_quantity.to_string()),
                AdmissionLiquidity::FullDepth(_) => None,
            },
            "recovery_limit_price": economics.recovery_limit_price.to_string(),
            "recovery_sell_limit_price": economics.recovery_sell_limit_price.map(|price| price.to_string()),
            "recovery_buy_limit_price": economics.recovery_buy_limit_price.map(|price| price.to_string()),
            "recovery_quote_token_a_base_units": economics.recovery_quote_token_a.to_string(),
            "recovery_sell_quote_token_a_base_units": economics.recovery_sell_quote_token_a.to_string(),
            "recovery_buy_quote_token_a_base_units": economics.recovery_buy_quote_token_a.to_string(),
            "recovery_loss_token_a_base_units": economics.recovery_loss_token_a.to_string(),
            "maximum_gas_wei": economics.maximum_gas_wei.to_string(),
            "maximum_fee_per_gas_wei": economics.maximum_fee_per_gas_wei.to_string(),
            "gas_conversion_price_token_a": native_price_token_a.to_string(),
            "maximum_gas_cost_token_a_base_units": economics.maximum_gas_cost_token_a.to_string(),
            "expected_profit_token_a_base_units": economics.expected_profit_token_a.to_string(),
            "fully_burdened_cost_token_a_base_units": economics.fully_burdened_cost_token_a.to_string(),
            "bounded_profit_token_a_base_units": economics.bounded_profit_token_a.to_string(),
            "expected_profit_after_gas_bps_x100": expected_profit_after_gas_bps_x100.to_string(),
            "expected_profit_after_gas_bps": expected_profit_after_gas_bps,
            "expected_profit_after_gas_threshold_bps": EXPECTED_PROFIT_AFTER_GAS_THRESHOLD_BPS,
            "expected_profit_after_gas_threshold_met": expected_profit_after_gas_threshold_met,
            "dex_plan": dex_plan_telemetry_value(&dex_plan),
        });
        let admitted_object = admitted_payload
            .as_object_mut()
            .context("arbitrage admission telemetry payload is not an object")?;
        admitted_object.insert(
            "price_unchanged_for_us".to_owned(),
            json!(duration_us(quote.received_at.elapsed())),
        );
        admitted_object.insert(
            "recovery_loss_bound_token_a_base_units".to_owned(),
            json!(economics.recovery_loss_token_a.to_string()),
        );
        admitted_object.insert(
            "gas_burdened_cost_token_a_base_units".to_owned(),
            json!(economics.gas_burdened_cost_token_a.to_string()),
        );
        admitted_object.insert(
            "expected_profit_after_gas_token_a_base_units".to_owned(),
            json!(economics.expected_profit_after_gas_token_a.to_string()),
        );
        self.telemetry.emit("arbitrage_admitted", admitted_payload);
        Ok(false)
    }

    fn emit_admission_risk_rejection(
        &self,
        quote: &TopOfBook,
        pair_id: &str,
        direction: TradeDirection,
        reason: &'static str,
        economics: Option<AdmissionEconomics>,
    ) {
        let expected_profit_after_gas_bps_x100 = economics.and_then(|value| {
            profit_bps_x100_u256(
                value.expected_profit_after_gas_token_a,
                value.gas_burdened_cost_token_a,
            )
            .ok()
        });
        let expected_profit_after_gas_bps =
            expected_profit_after_gas_bps_x100.and_then(|value| format_bps_x100_u256(value).ok());
        let expected_profit_after_gas_threshold_met =
            expected_profit_after_gas_bps_x100.map(|value| {
                value >= U256::from(u64::from(EXPECTED_PROFIT_AFTER_GAS_THRESHOLD_BPS) * 100)
            });
        self.telemetry.emit(
            "arbitrage_admission_rejected",
            json!({
                "engine_id": self.config.engine_id,
                "pair_id": pair_id,
                "symbol": quote.symbol.as_ref(),
                "update_id": quote.update_id,
                "direction": format!("{direction:?}"),
                "reason": reason,
                "recovery_limit_price": economics.map(|value| value.recovery_limit_price.to_string()),
                "recovery_sell_limit_price": economics.and_then(|value| value.recovery_sell_limit_price.map(|price| price.to_string())),
                "recovery_buy_limit_price": economics.and_then(|value| value.recovery_buy_limit_price.map(|price| price.to_string())),
                "recovery_quote_token_a_base_units": economics.map(|value| value.recovery_quote_token_a.to_string()),
                "recovery_sell_quote_token_a_base_units": economics.map(|value| value.recovery_sell_quote_token_a.to_string()),
                "recovery_buy_quote_token_a_base_units": economics.map(|value| value.recovery_buy_quote_token_a.to_string()),
                "recovery_loss_token_a_base_units": economics.map(|value| value.recovery_loss_token_a.to_string()),
                "recovery_loss_bound_token_a_base_units": economics.map(|value| value.recovery_loss_token_a.to_string()),
                "maximum_gas_wei": economics.map(|value| value.maximum_gas_wei.to_string()),
                "maximum_fee_per_gas_wei": economics.map(|value| value.maximum_fee_per_gas_wei.to_string()),
                "maximum_gas_cost_token_a_base_units": economics.map(|value| value.maximum_gas_cost_token_a.to_string()),
                "expected_profit_token_a_base_units": economics.map(|value| value.expected_profit_token_a.to_string()),
                "gas_burdened_cost_token_a_base_units": economics.map(|value| value.gas_burdened_cost_token_a.to_string()),
                "expected_profit_after_gas_token_a_base_units": economics.map(|value| value.expected_profit_after_gas_token_a.to_string()),
                "expected_profit_after_gas_bps_x100": expected_profit_after_gas_bps_x100.map(|value| value.to_string()),
                "expected_profit_after_gas_bps": expected_profit_after_gas_bps,
                "expected_profit_after_gas_threshold_bps": EXPECTED_PROFIT_AFTER_GAS_THRESHOLD_BPS,
                "expected_profit_after_gas_threshold_met": expected_profit_after_gas_threshold_met,
                "fully_burdened_cost_token_a_base_units": economics.map(|value| value.fully_burdened_cost_token_a.to_string()),
                "bounded_profit_token_a_base_units": economics.map(|value| value.bounded_profit_token_a.to_string()),
            }),
        );
    }

    fn log_trading_inventory_blocked(
        &mut self,
        pair_id: &str,
        pair_symbol: &str,
        plan_id: &str,
        claim_details: &[Value],
    ) {
        let now = Instant::now();
        if self.last_inventory_blocked_alert_at.is_some_and(|last| {
            now.saturating_duration_since(last) < TRADING_INVENTORY_ALERT_LOG_INTERVAL
        }) {
            return;
        }
        self.last_inventory_blocked_alert_at = Some(now);
        let claims = Value::Array(claim_details.to_vec());
        tracing::error!(
            engine_id = %self.config.engine_id,
            pair_id,
            pair_symbol,
            plan_id,
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
                json!({
                    "venue": inventory_venue_label(claim.key.venue),
                    "asset": claim.key.asset.as_str(),
                    "required_base_units": claim.amount.to_string(),
                    "observed_base_units": observed.map(|amount| amount.to_string()),
                    "reserved_base_units": reserved.to_string(),
                    "available_base_units": available.map(|amount| amount.to_string()),
                })
            })
            .collect()
    }

    pub fn prepare_arbitrage_settlement_catchup(
        &self,
        event: &PaperTradeEvent,
    ) -> anyhow::Result<Option<ArbitrageSettlementCatchupRequest>> {
        if event.state != PaperTradeEventState::Balanced || !event.dex_filled {
            return Ok(None);
        }
        let Some(target) = event.dex_settlement_log.as_ref() else {
            return Ok(None);
        };
        ensure!(!target.removed, "receipt settlement Swap event is removed");
        let decoded = decode_pool_event(target)?
            .context("receipt settlement log is not a recognized pool event")?;
        ensure!(
            matches!(decoded.update, PoolUpdate::Swap { .. }),
            "receipt settlement log is not a Swap event"
        );
        let freshness = self
            .arbitrage_plan_freshness
            .get(&event.plan_id)
            .context("settlement proof has no admitted plan freshness")?;
        let pool_index = self
            .dex
            .pool_index(decoded.locator)
            .context("settlement proof targets an unknown pool")?;
        ensure!(
            pool_index == freshness.pool_index,
            "settlement proof pool differs from the admitted pool"
        );
        let target_position = target.position();
        if target.block_number <= self.dex.backfilled_through()
            || self
                .dex
                .last_position(decoded.locator)
                .is_some_and(|position| position >= target_position)
        {
            return Ok(None);
        }
        let from_block = self
            .dex
            .last_position(decoded.locator)
            .map_or_else(
                || self.dex.backfilled_through().saturating_add(1),
                |position| position.block_number,
            )
            .min(target.block_number);
        Ok(Some(ArbitrageSettlementCatchupRequest {
            filter: build_pool_log_filter(decoded.locator, target.address)?,
            from_block,
            through_block: target.block_number,
            plan_id: event.plan_id.clone(),
            pool_index,
            locator: decoded.locator,
            target: target.clone(),
            started_at: Instant::now(),
        }))
    }

    pub fn defer_arbitrage_settlement_catchup(
        &self,
        request: &ArbitrageSettlementCatchupRequest,
        reason: &'static str,
    ) {
        self.telemetry.emit(
            "arbitrage_settlement_catchup_deferred",
            json!({
                "engine_id": self.config.engine_id,
                "plan_id": request.plan_id,
                "pool_index": request.pool_index,
                "reason": reason,
                "target_block": request.target.block_number,
                "target_transaction_index": request.target.transaction_index,
                "target_log_index": request.target.log_index,
                "catchup_age_us": request.started_at.elapsed().as_micros(),
            }),
        );
    }

    pub fn apply_arbitrage_settlement_catchup(
        &mut self,
        request: ArbitrageSettlementCatchupRequest,
        mut logs: Vec<ChainLog>,
    ) -> anyhow::Result<Option<PreparedPoolBuildRequest>> {
        let Some(barrier) = self.arbitrage_settlement_barriers.get(&request.pool_index) else {
            return Ok(None);
        };
        ensure!(
            barrier.plan_id == request.plan_id,
            "settlement catch-up plan differs from the active barrier"
        );
        logs.sort_unstable_by_key(ChainLog::position);
        logs.dedup_by(|right, left| {
            right.position() == left.position()
                && right.address == left.address
                && right.block_hash == left.block_hash
        });
        let proof_present = logs.iter().any(|log| {
            log.position() == request.target.position()
                && log.block_hash == request.target.block_hash
                && log.address == request.target.address
                && log.topics == request.target.topics
                && log.data == request.target.data
                && !log.removed
        });
        if !proof_present {
            self.telemetry.emit(
                "arbitrage_settlement_catchup_deferred",
                json!({
                    "engine_id": self.config.engine_id,
                    "plan_id": request.plan_id,
                    "pool_index": request.pool_index,
                    "reason": "receipt_swap_not_returned_by_eth_get_logs",
                    "target_block": request.target.block_number,
                    "target_transaction_index": request.target.transaction_index,
                    "target_log_index": request.target.log_index,
                    "fetched_logs": logs.len(),
                    "catchup_age_us": request.started_at.elapsed().as_micros(),
                }),
            );
            return Ok(None);
        }
        for log in &logs {
            let decoded = decode_pool_event(log)?
                .context("settlement catch-up returned an unrecognized pool log")?;
            ensure!(
                decoded.locator == request.locator,
                "settlement catch-up returned a log for another pool"
            );
            ensure!(!log.removed, "settlement catch-up returned a removed log");
        }
        let mut applied = 0_usize;
        for log in &logs {
            if let LogApplyResult::Applied { pool_index, .. } = self.dex.apply_log(log)? {
                ensure!(
                    pool_index == request.pool_index,
                    "settlement catch-up applied another pool"
                );
                applied += 1;
            }
        }
        ensure!(
            self.dex
                .last_position(request.locator)
                .is_some_and(|position| position >= request.target.position()),
            "settlement catch-up did not advance through the receipt Swap"
        );
        if applied == 0 {
            return Ok(None);
        }
        let refresh = self
            .opportunities
            .request_pool_refresh(request.pool_index, &self.dex)?;
        self.telemetry.emit(
            "arbitrage_settlement_catchup_applied",
            json!({
                "engine_id": self.config.engine_id,
                "plan_id": request.plan_id,
                "pool_index": request.pool_index,
                "target_block": request.target.block_number,
                "target_transaction_index": request.target.transaction_index,
                "target_log_index": request.target.log_index,
                "applied_logs": applied,
                "fetched_logs": logs.len(),
                "prepared_generation": refresh.generation(),
                "source": "receipt_http_catchup",
                "catchup_fetch_apply_us": request.started_at.elapsed().as_micros(),
            }),
        );
        Ok(Some(refresh))
    }

    fn reconcile_arbitrage_settlement(&mut self, pool_index: usize, prepared_generation: u64) {
        let Some(barrier) = self.arbitrage_settlement_barriers.get(&pool_index) else {
            return;
        };
        if prepared_generation <= barrier.pool_generation {
            return;
        }
        let barrier = self
            .arbitrage_settlement_barriers
            .remove(&pool_index)
            .expect("barrier existed above");
        self.telemetry.emit(
            "arbitrage_settlement_reconciled",
            json!({
                "engine_id": self.config.engine_id,
                "pair_id": barrier.pair_id,
                "plan_id": barrier.plan_id,
                "pool_index": pool_index,
                "barrier_generation": barrier.pool_generation,
                "prepared_generation": prepared_generation,
                "settlement_age_ms": barrier.started_at.elapsed().as_millis(),
            }),
        );
    }

    pub fn on_paper_trade_event(&mut self, event: PaperTradeEvent) -> anyhow::Result<()> {
        let mut settlement_barrier = None;
        match event.state {
            PaperTradeEventState::Balanced => {
                if self.inventory.reservation(&event.plan_id).is_some() {
                    self.inventory.mark_pending_settlement(&event.plan_id)?;
                } else {
                    tracing::warn!(
                        plan_id = %event.plan_id,
                        "balanced arbitrage event has no in-memory reservation after restart"
                    );
                }
                if event.dex_filled
                    && let Some(freshness) = self.arbitrage_plan_freshness.remove(&event.plan_id)
                {
                    let prepared_generation = self
                        .opportunities
                        .prepared_pool_generation(freshness.pool_index)?;
                    if !settlement_requires_refresh(freshness.pool_generation, prepared_generation)
                    {
                        self.telemetry.emit(
                            "arbitrage_settlement_reconciled",
                            json!({
                                "engine_id": self.config.engine_id,
                                "pair_id": freshness.pair_id,
                                "plan_id": event.plan_id,
                                "pool_index": freshness.pool_index,
                                "barrier_generation": freshness.pool_generation,
                                "prepared_generation": prepared_generation,
                                "settlement_age_ms": 0,
                                "source": "already_prepared_before_terminal_event",
                            }),
                        );
                    } else {
                        self.arbitrage_settlement_barriers.insert(
                            freshness.pool_index,
                            ArbitrageSettlementBarrier {
                                pair_id: freshness.pair_id,
                                plan_id: event.plan_id.clone(),
                                pool_generation: freshness.pool_generation,
                                started_at: Instant::now(),
                            },
                        );
                        settlement_barrier =
                            Some((freshness.pool_index, freshness.pool_generation));
                    }
                }
            }
            PaperTradeEventState::RejectedUnsubmitted => {
                self.arbitrage_plan_freshness.remove(&event.plan_id);
                if self.inventory.reservation(&event.plan_id).is_some() {
                    self.inventory.release_unsubmitted(&event.plan_id)?;
                } else {
                    tracing::warn!(
                        plan_id = %event.plan_id,
                        "rejected arbitrage event has no in-memory reservation after restart"
                    );
                }
            }
            PaperTradeEventState::BlockedUnknown => {
                self.arbitrage_plan_freshness.remove(&event.plan_id);
            }
        }
        let pending = self.paper_trades.as_ref().and_then(|handle| {
            if let Some((pool_index, pool_generation)) = settlement_barrier {
                handle.finish_for_settlement(pool_index, pool_generation)
            } else {
                handle.finish(event.state)
            }
        });
        if let Some(pending) = pending {
            self.release_pending_opportunity(
                pending,
                "execution_pending_invalidated_by_dex_settlement",
                None,
            )?;
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
            let price_age_ms = feed
                .book
                .as_ref()
                .map(|book| now.saturating_duration_since(book.received_at).as_millis());
            let transport_age_ms = feed.last_transport_activity_at.map(|last_activity_at| {
                now.saturating_duration_since(last_activity_at).as_millis()
            });
            let transport_fresh = transport_age_ms
                .is_some_and(|age| age <= u128::from(self.config.market_data_max_age_ms));
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
                    "max_transport_age_ms": self.config.market_data_max_age_ms,
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
            self.rebalance_blocked,
            inflight_age,
            settlement_age,
            Duration::from_secs(self.config.rebalance_executor_timeout_seconds),
            settlement_timeout,
        );
        let inflight_age_ms = inflight_age.map(|age| age.as_millis());
        let settlement_age_ms = settlement_age.map(|age| age.as_millis());
        if health.healthy {
            tracing::info!(
                healthy = true,
                rebalance_blocked = self.rebalance_blocked,
                rebalance_inflight = self.rebalance_inflight,
                inflight_age_ms,
                settlement_waiting = self.rebalance_settlement.is_some(),
                settlement_age_ms,
                "rebalance health heartbeat"
            );
        } else {
            tracing::error!(
                healthy = false,
                rebalance_blocked = self.rebalance_blocked,
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
        let binance_ready = self
            .state
            .binance_ready(now, self.config.market_data_max_age_ms);
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
        // Rebalancing is proactive inventory maintenance, not a global trading
        // lock. Its pending, in-flight, failed, and post-reconciliation states
        // serialize only rebalance operations. Trading remains gated by fresh
        // market/DEX/balance inputs; an execution coordinator must separately
        // reserve and validate the assets required by its concrete trade.
        let user_data_ready = self.binance_user_data_connected && self.binance_user_data_clean;
        let gas_price_transport_fresh =
            self.gas_price_transport_activity_at
                .is_some_and(|last_activity_at| {
                    now.saturating_duration_since(last_activity_at).as_millis()
                        <= u128::from(self.config.market_data_max_age_ms)
                });
        let gas_price_ready =
            self.gas_price_connected && self.gas_price_book.is_some() && gas_price_transport_fresh;
        let trading_readiness = TradingReadiness {
            dex_ready,
            balances_ready,
            user_data_ready,
            gas_price_ready,
        };
        let current = self.state.refresh_phase(
            now,
            self.config.market_data_max_age_ms,
            trading_readiness.ready(),
        );
        if previous != current {
            let blocking_inputs = [
                (!binance_ready).then_some("binance_top"),
                (!dex_mirror_ready).then_some("dex_mirror"),
                (!balances_ready).then_some("balances"),
                (!user_data_ready).then_some("binance_user_data"),
                (!gas_price_ready).then_some("gas_price"),
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
                    "binance_user_data_connected": self.binance_user_data_connected,
                    "binance_user_data_clean": self.binance_user_data_clean,
                    "binance_user_data_ready": user_data_ready,
                    "gas_price_connected": self.gas_price_connected,
                    "gas_price_transport_fresh": gas_price_transport_fresh,
                    "gas_price_ready": gas_price_ready,
                    "blocking_inputs": blocking_inputs,
                }),
            );
        }
    }

    pub fn phase(&self) -> RuntimePhase {
        self.state.phase
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

fn requires_depth_for_runtime_phase(arbitrage_execution_mode: &str) -> bool {
    matches!(arbitrage_execution_mode, "paper_concurrent_hedged")
}

const fn settlement_requires_refresh(
    admitted_generation: u64,
    prepared_generation: Option<u64>,
) -> bool {
    match prepared_generation {
        Some(prepared_generation) => prepared_generation <= admitted_generation,
        None => true,
    }
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

fn profit_bps_x100_u256(profit: U256, cost: U256) -> anyhow::Result<U256> {
    ensure!(!cost.is_zero(), "profit bps cost is zero");
    profit
        .checked_mul(U256::from(BPS_X100_SCALE))
        .map(|scaled| scaled / cost)
        .context("profit bps scaling overflow")
}

fn format_bps_x100_u256(value: U256) -> anyhow::Result<String> {
    let whole = value / U256::from(100_u8);
    let fractional = value % U256::from(100_u8);
    let fractional = u8::try_from(fractional).context("profit bps fractional part exceeds u8")?;
    Ok(format!("{whole}.{fractional:02}"))
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

fn exact_execution_envelope_amounts(
    direction: TradeDirection,
    dex_input: U256,
    trade: TradeEvaluation,
    economics: AdmissionEconomics,
) -> (U256, U256, U256) {
    let token_a = match direction {
        TradeDirection::BuyTokenBOnDexSellOnCex => dex_input,
        TradeDirection::BuyTokenBOnCexSellOnDex => {
            trade.cost_token_a.max(economics.recovery_buy_quote_token_a)
        }
    };
    let token_b = match direction {
        // The live executor caps the hedgeable DEX credit at the immutable
        // planned amount. Favorable output above it stays in the wallet.
        TradeDirection::BuyTokenBOnDexSellOnCex => trade.token_b_amount,
        TradeDirection::BuyTokenBOnCexSellOnDex => dex_input,
    };
    (token_a, token_b, economics.maximum_gas_wei)
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
        collections::BTreeMap,
        time::{Duration, Instant},
    };

    use alloy_primitives::U256;
    use rust_decimal::Decimal;

    use crate::{
        admission::AdmissionEconomics,
        arbitrage::ArbitrageDirection as TradeDirection,
        execution_plan::{DexRoutePlan, DexSwapPlan},
        inventory::{
            InventoryClaim, InventoryKey, InventoryReservations, InventoryVenue,
            ReservationPurpose, ReservationRequest,
        },
        opportunity::{ArbitrageDirection as SizingDirection, TradeEvaluation},
        rebalance::Direction,
    };

    use super::{
        AdaptiveCandidate, AdaptiveDepthSource, AdaptiveSizingRuntimeLimits, DepthObservation,
        EXPECTED_PROFIT_AFTER_GAS_THRESHOLD_BPS, RebalanceSettlementBarrier, ReservationPrecheck,
        TradingReadiness, adaptive_candidate_is_better, admission_deadline_unix_seconds,
        classify_depth_health, estimate_exchange_event_to_socket_us,
        exact_execution_envelope_amounts, format_bps_x100_u256, mark_sequence_matched_update,
        profit_bps_x100_u256, rebalance_health_state, requires_depth_for_runtime_phase,
        reservation_precheck, settlement_requires_refresh,
    };

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
            max_unhedged_notional: U256::from(220_000_000_u64),
            max_recovery_loss: U256::from(2_000_000_u64),
            min_expected_profit: U256::ZERO,
            min_incremental_expected_profit: U256::ZERO,
            recent_full_depth_max_age_ms: 750,
            recent_full_depth_max_update_delta: 8,
            top_of_book_max_trade_notional: U256::from(40_000_000_u64),
        }
    }

    #[test]
    fn dex_first_runtime_phase_does_not_require_depth() {
        assert!(!requires_depth_for_runtime_phase("full_live"));
        assert!(!requires_depth_for_runtime_phase("paper_dex_first"));
        assert!(requires_depth_for_runtime_phase("paper_concurrent_hedged"));
    }

    #[test]
    fn settlement_waits_only_until_a_new_pool_generation_is_prepared() {
        assert!(settlement_requires_refresh(41, None));
        assert!(settlement_requires_refresh(41, Some(41)));
        assert!(!settlement_requires_refresh(41, Some(42)));
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
        let mut inventory = InventoryReservations::default();
        inventory
            .update_venue(
                InventoryVenue::Binance,
                1,
                [("USDC".to_owned(), U256::from(1_000))],
            )
            .unwrap();
        let request = ReservationRequest {
            operation_id: "paper-plan-1".to_owned(),
            purpose: ReservationPurpose::TradePrimary,
            claims: vec![InventoryClaim {
                key: InventoryKey::new(InventoryVenue::Binance, "USDC").unwrap(),
                amount: U256::from(100),
            }],
            settlement_venues: [InventoryVenue::Binance].into_iter().collect(),
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
    fn after_gas_profit_bps_telemetry_uses_fixed_point_math() {
        let profit = U256::from(12_345_u64);
        let cost = U256::from(10_000_000_u64);
        let bps_x100 = profit_bps_x100_u256(profit, cost).unwrap();

        assert_eq!(bps_x100, U256::from(1_234_u64));
        assert_eq!(format_bps_x100_u256(bps_x100).unwrap(), "12.34");
        assert!(bps_x100 >= U256::from(u64::from(EXPECTED_PROFIT_AFTER_GAS_THRESHOLD_BPS) * 100));
    }

    #[test]
    fn exact_execution_envelope_has_no_multiplicative_reservation() {
        let trade = TradeEvaluation {
            pool_index: 0,
            token_b_amount: U256::from(100),
            dex_token_a_amount: U256::from(900),
            cex_token_a_amount: U256::from(1_000),
            cost_token_a: U256::from(1_010),
            proceeds_token_a: U256::from(1_030),
            execution_slippage_bps: 10,
            gross_profit_bps_x100: 2_000,
            profit_bps_x100: 1_000,
            meets_threshold: true,
        };
        let economics = AdmissionEconomics {
            primary_quantity: Decimal::from(100),
            recovery_limit_price: Decimal::ONE,
            recovery_quote_token_a: U256::from(1_050),
            recovery_sell_limit_price: Some(Decimal::ONE),
            recovery_sell_quote_token_a: U256::from(990),
            recovery_buy_limit_price: Some(Decimal::ONE),
            recovery_buy_quote_token_a: U256::from(1_075),
            recovery_loss_token_a: U256::from(65),
            maximum_gas_wei: U256::from(25),
            maximum_fee_per_gas_wei: 5,
            maximum_gas_cost_token_a: U256::from(2),
            expected_profit_token_a: U256::from(20),
            gas_burdened_cost_token_a: U256::from(1_012),
            expected_profit_after_gas_token_a: U256::from(18),
            fully_burdened_cost_token_a: U256::from(1_077),
            bounded_profit_token_a: U256::from(18),
            opportunity_threshold_met: true,
            native_gas_covered: true,
        };

        assert_eq!(
            exact_execution_envelope_amounts(
                TradeDirection::BuyTokenBOnDexSellOnCex,
                U256::from(1_020),
                trade,
                economics,
            ),
            (U256::from(1_020), U256::from(100), U256::from(25))
        );
        assert_eq!(
            exact_execution_envelope_amounts(
                TradeDirection::BuyTokenBOnCexSellOnDex,
                U256::from(100),
                trade,
                economics,
            ),
            (U256::from(1_075), U256::from(100), U256::from(25))
        );
    }

    #[test]
    fn adaptive_optimizer_ranks_expected_spread_profit_not_tail_scenario_profit() {
        let candidate = adaptive_candidate_for_ranking(100, 0);
        let current = adaptive_candidate_for_ranking(90, 50);

        assert!(adaptive_candidate_is_better(candidate, current));
        assert!(!adaptive_candidate_is_better(current, candidate));
    }

    fn adaptive_candidate_for_ranking(
        expected_profit: u64,
        bounded_profit: u64,
    ) -> AdaptiveCandidate {
        let trade = TradeEvaluation {
            pool_index: 0,
            token_b_amount: U256::from(100),
            dex_token_a_amount: U256::from(900),
            cex_token_a_amount: U256::from(1_000),
            cost_token_a: U256::from(1_000),
            proceeds_token_a: U256::from(1_000 + expected_profit),
            execution_slippage_bps: 10,
            gross_profit_bps_x100: 2_000,
            profit_bps_x100: 1_000,
            meets_threshold: true,
        };
        AdaptiveCandidate {
            direction: SizingDirection::BuyTokenBOnDexSellOnCex,
            trade,
            economics: AdmissionEconomics {
                primary_quantity: Decimal::from(100),
                recovery_limit_price: Decimal::ONE,
                recovery_quote_token_a: U256::from(1_050),
                recovery_sell_limit_price: Some(Decimal::ONE),
                recovery_sell_quote_token_a: U256::from(990),
                recovery_buy_limit_price: Some(Decimal::ONE),
                recovery_buy_quote_token_a: U256::from(1_075),
                recovery_loss_token_a: U256::from(65),
                maximum_gas_wei: U256::from(25),
                maximum_fee_per_gas_wei: 5,
                maximum_gas_cost_token_a: U256::from(2),
                expected_profit_token_a: U256::from(expected_profit),
                gas_burdened_cost_token_a: U256::from(1_002),
                expected_profit_after_gas_token_a: U256::from(bounded_profit),
                fully_burdened_cost_token_a: U256::from(1_077),
                bounded_profit_token_a: U256::from(bounded_profit),
                opportunity_threshold_met: true,
                native_gas_covered: true,
            },
            trade_notional: trade.proceeds_token_a,
            unhedged_notional: U256::from(1_075),
            reservation_fits: true,
        }
    }

    #[test]
    fn rebalance_state_is_not_a_global_trading_readiness_input() {
        assert!(
            TradingReadiness {
                dex_ready: true,
                balances_ready: true,
                user_data_ready: true,
                gas_price_ready: true,
            }
            .ready()
        );
    }

    #[test]
    fn stale_dex_or_balance_inputs_still_fail_closed() {
        for readiness in [
            TradingReadiness {
                dex_ready: false,
                balances_ready: true,
                user_data_ready: true,
                gas_price_ready: true,
            },
            TradingReadiness {
                dex_ready: true,
                balances_ready: false,
                user_data_ready: true,
                gas_price_ready: true,
            },
            TradingReadiness {
                dex_ready: true,
                balances_ready: true,
                user_data_ready: false,
                gas_price_ready: true,
            },
            TradingReadiness {
                dex_ready: true,
                balances_ready: true,
                user_data_ready: true,
                gas_price_ready: false,
            },
        ] {
            assert!(!readiness.ready());
        }
    }

    #[test]
    fn completed_rebalance_waits_for_both_continuous_balance_streams() {
        let now = Instant::now();
        let later = now + std::time::Duration::from_millis(1);
        let barrier = RebalanceSettlementBarrier {
            operation_id: "rebalance-wld-1".to_owned(),
            token_symbol: "WLD".to_owned(),
            direction: Direction::WalletToBinance,
            binance_after: now,
            wallet_after: now,
            started_at: now,
        };

        assert!(!barrier.reconciled(now, now));
        assert!(!barrier.reconciled(later, now));
        assert!(!barrier.reconciled(now, later));
        assert!(barrier.reconciled(later, later));
    }

    #[test]
    fn settlement_barrier_does_not_change_trading_readiness() {
        assert!(
            TradingReadiness {
                dex_ready: true,
                balances_ready: true,
                user_data_ready: true,
                gas_price_ready: true,
            }
            .ready()
        );
    }

    #[test]
    fn rebalance_health_detects_blocked_and_stuck_states_at_the_boundary() {
        let timeout = std::time::Duration::from_secs(60);

        assert!(rebalance_health_state(false, None, None, timeout, timeout).healthy);
        assert!(!rebalance_health_state(true, None, None, timeout, timeout).healthy);
        let inflight = rebalance_health_state(false, Some(timeout), None, timeout, timeout);
        assert!(inflight.inflight_stuck);
        assert!(!inflight.healthy);
        let settlement = rebalance_health_state(false, None, Some(timeout), timeout, timeout);
        assert!(settlement.settlement_stuck);
        assert!(!settlement.healthy);
    }
}

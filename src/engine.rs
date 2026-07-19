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
    admission::{AdmissionEconomics, AdmissionInputs, evaluate_admission},
    arbitrage::{
        AdmissionRiskBounds, ArbitrageDirection as TradeDirection, EntryPreflightHandle,
        PaperOpportunity, PaperTradeEvent, PaperTradeEventState, PaperTradeHandle,
    },
    balances::BalanceEvent,
    binance::{
        depth::SpotDepthBook,
        user_data::{ExecutionReportEvent, UserDataEvent},
    },
    config::AppConfig,
    dex::mirror::{DexMirror, LogApplyResult},
    domain::config::{DexProvider, LoadedDomainConfig},
    execution_plan::{DEX_PLAN_TTL_SECONDS, DexSwapPlan},
    hot_telemetry::{HotTelemetryHandle, HotTelemetryTask, channel as hot_telemetry_channel},
    inventory::{
        InventoryClaim, InventoryKey, InventoryReservations, InventoryVenue, ReservationPurpose,
        ReservationRequest,
    },
    market_data::{MarketEvent, alchemy::DexStreamEvent},
    opportunity::{
        ArbitrageDirection, OpportunityEngine, PairEvaluation, PreparedPoolBuildRequest,
        PreparedPoolBuildResult,
    },
    rebalance::{Direction, RebalanceEvaluation, RebalanceExecutionOperation, RebalanceTracker},
    state::{QuoteApplyResult, RuntimePhase, RuntimeState, TopOfBook},
    telemetry::TelemetryHandle,
};

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
    gas_price_symbol: String,
    gas_price_connected: bool,
    gas_price_generation: u64,
    gas_price_book: Option<TopOfBook>,
    rebalance_inventory_reservation: Option<String>,
    next_inventory_reservation: u64,
    pending_rebalance: Option<RebalanceEvaluation>,
    rebalance_inflight: bool,
    rebalance_inflight_since: Option<Instant>,
    rebalance_blocked: bool,
    rebalance_settlement: Option<RebalanceSettlementBarrier>,
    last_rebalance_health_log_at: Option<Instant>,
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
const MINIMUM_REBALANCE_SETTLEMENT_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug)]
struct RebalanceSettlementBarrier {
    operation_id: String,
    token_symbol: String,
    direction: Direction,
    binance_after: Instant,
    wallet_after: Instant,
    started_at: Instant,
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
        let (hot_telemetry, hot_telemetry_task) =
            hot_telemetry_channel(&config, opportunities.pairs(), &dex, telemetry.clone())?;
        Ok((
            Self {
                config,
                domain_config,
                state: RuntimeState::new_with_depth(symbols),
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
                gas_price_symbol,
                gas_price_connected: false,
                gas_price_generation: 0,
                gas_price_book: None,
                rebalance_inventory_reservation: None,
                next_inventory_reservation: 0,
                pending_rebalance: None,
                rebalance_inflight: false,
                rebalance_inflight_since: None,
                rebalance_blocked: false,
                rebalance_settlement: None,
                last_rebalance_health_log_at: None,
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
                self.evaluate_ready_quote(&quote, "dex_prepared", depth.as_ref())?;
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
                self.state.on_connected(&symbol, generation);
                self.last_sequence_matched_quote_update
                    .remove(symbol.as_ref());
                self.latest_sequence_matched_depth.remove(symbol.as_ref());
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
                self.latest_sequence_matched_depth.remove(symbol.as_ref());
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
            MarketEvent::BinanceTopOfBook(quote) => {
                let matching_depth = depth.filter(|depth| {
                    depth.matches_top(
                        quote.symbol.as_ref(),
                        quote.update_id,
                        quote.bid_price,
                        quote.bid_quantity,
                        quote.ask_price,
                        quote.ask_quantity,
                    )
                });
                self.on_binance_quote(quote, matching_depth)?;
            }
            MarketEvent::BinanceDepthApplied {
                symbol,
                generation,
                last_update_id,
                observed_at,
            } => {
                let apply_result = self.state.apply_depth(
                    symbol.as_ref(),
                    generation,
                    last_update_id,
                    observed_at,
                );
                self.telemetry.emit(
                    "binance_depth_applied",
                    json!({
                        "engine_id": self.config.engine_id,
                        "product": "spot",
                        "symbol": symbol.as_ref(),
                        "generation": generation,
                        "last_update_id": last_update_id,
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
                    self.evaluate_sequence_matched_quote(&quote, "binance_depth", depth)?;
                }
            }
        }
        self.refresh_phase(Instant::now());
        Ok(())
    }

    pub fn on_gas_market_event(&mut self, event: MarketEvent) -> anyhow::Result<()> {
        match event {
            MarketEvent::FeedConnected {
                symbol, generation, ..
            } => {
                ensure!(
                    symbol.as_ref() == self.gas_price_symbol,
                    "gas feed symbol mismatch"
                );
                if generation >= self.gas_price_generation {
                    self.gas_price_connected = true;
                    self.gas_price_generation = generation;
                    self.gas_price_book = None;
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
                }
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
                    self.hot_telemetry.emit_binance_book(&quote);
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
                self.inventory.update_venue(
                    InventoryVenue::Wallet,
                    snapshot.block_number,
                    snapshot
                        .token_balances
                        .iter()
                        .map(|balance| (balance.symbol.to_string(), balance.base_units)),
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
        matching_depth: Option<&SpotDepthBook>,
    ) -> anyhow::Result<()> {
        let result = self.state.apply_quote(quote.clone());
        match result {
            QuoteApplyResult::Accepted => {
                self.entry_preflight.update_quote(&quote);
                // The decision is evaluated only after all readiness inputs are
                // fresh. The calculation itself performs no RPC, I/O, or locks.
                self.refresh_phase(Instant::now());
                if self.state.phase == RuntimePhase::Ready
                    && let Some(depth) = matching_depth
                {
                    self.evaluate_sequence_matched_quote(&quote, "binance_book_ticker", depth)?;
                } else if self.state.phase == RuntimePhase::Ready {
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
                }

                // Raw market telemetry is deliberately serialized only after
                // the opportunity decision. It must never delay detection or
                // eventual order submission.
                self.hot_telemetry.emit_binance_book(&quote);
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
    ) -> anyhow::Result<()> {
        if !mark_sequence_matched_update(
            &mut self.last_sequence_matched_quote_update,
            quote.symbol.as_ref(),
            quote.update_id,
        ) {
            return Ok(());
        }
        self.evaluate_ready_quote(quote, trigger, Some(depth))
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
        depth: Option<&SpotDepthBook>,
    ) -> anyhow::Result<()> {
        let calculation_started = Instant::now();
        if let Some(evaluation) = self.opportunities.evaluate(quote)? {
            self.hot_telemetry.emit_evaluation(
                quote,
                evaluation,
                self.dex.latest_head().number,
                calculation_started.elapsed().as_micros(),
                trigger,
            );
            if let Some(depth) = depth {
                self.submit_paper_opportunity(quote, evaluation, depth)?;
            }
        }
        Ok(())
    }

    fn submit_paper_opportunity(
        &mut self,
        quote: &TopOfBook,
        evaluation: PairEvaluation,
        depth: &SpotDepthBook,
    ) -> anyhow::Result<()> {
        let Some(handle) = self.paper_trades.clone() else {
            return Ok(());
        };
        let pair = self.opportunities.pair(evaluation.pair_index)?;
        let pair_id = pair.pair_id.clone();
        let pair_symbol = pair.symbol.clone();
        let token_a_decimals = pair.token_a_decimals;
        let token_b_decimals = pair.token_b_decimals;
        let binance_buy_fee_bps = pair.binance_buy_fee_bps;
        let binance_sell_fee_bps = pair.binance_sell_fee_bps;
        let opportunity_threshold_bps = pair.opportunity_threshold_bps;
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

        let mut candidates = Vec::with_capacity(2);
        for direction in [evaluation.dex_buy_cex_sell, evaluation.cex_buy_dex_sell] {
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
            let Some(economics) = evaluate_admission(
                depth,
                AdmissionInputs {
                    symbol: &pair_symbol,
                    direction: trade_direction,
                    token_b_amount: trade.token_b_amount,
                    token_a_decimals,
                    token_b_decimals,
                    binance_buy_fee_bps,
                    binance_sell_fee_bps,
                    expected_cost_token_a: trade.cost_token_a,
                    expected_proceeds_token_a: trade.proceeds_token_a,
                    opportunity_threshold_bps,
                    network_gas_price_wei,
                    native_price_token_a,
                    wallet_native_balance_wei,
                },
            )?
            else {
                self.emit_admission_risk_rejection(
                    quote,
                    &pair_id,
                    trade_direction,
                    "insufficient_recovery_depth",
                    None,
                );
                continue;
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
            // Rails gates opportunity admission on the gross venue spread.
            // Keep the fully-burdened economics in telemetry and use them to
            // rank candidates, but do not turn them into a hidden Rust-only
            // threshold.
            candidates.push((trade_direction, trade, economics));
        }
        let candidate = candidates
            .into_iter()
            .max_by_key(|(_, _, economics)| economics.bounded_profit_token_a);
        let Some((direction, trade, economics)) = candidate else {
            return Ok(());
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
                        "paper-{}-{}-{}",
                        quote.received_unix_us,
                        quote.update_id,
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
            return Ok(());
        }
        let pair_config = self
            .domain_config
            .snapshot()
            .pairs
            .iter()
            .find(|config| config.id == pair_id)
            .context("paper opportunity pair is absent from domain config")?;
        let token_a_symbol = pair_config.token_a.symbol.clone();
        let token_b_symbol = pair_config.token_b.symbol.clone();
        let balance_safety_multiplier = pair_config.strategy.balance_safety_multiplier;
        let deadline_unix_seconds = quote
            .received_unix_us
            .checked_div(1_000_000)
            .and_then(|seconds| seconds.checked_add(DEX_PLAN_TTL_SECONDS))
            .context("DEX plan deadline overflow")?;
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
                execution_slippage_bps: trade.execution_slippage_bps,
                cex_primary_limit_price: match direction {
                    TradeDirection::BuyTokenBOnDexSellOnCex => quote.bid_price,
                    TradeDirection::BuyTokenBOnCexSellOnDex => quote.ask_price,
                },
                cex_recovery_limit_price: economics.recovery_limit_price,
                cex_recovery_sell_limit_price: Some(economics.recovery_sell_limit_price),
                cex_recovery_buy_limit_price: Some(economics.recovery_buy_limit_price),
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
        let safety_multiplier = U256::from(balance_safety_multiplier);
        let token_a_claim = trade
            .cost_token_a
            .max(economics.recovery_quote_token_a)
            .max(economics.recovery_sell_quote_token_a)
            .max(economics.recovery_buy_quote_token_a)
            .checked_mul(safety_multiplier)
            .context("paper token-A safety reservation overflow")?;
        let token_b_claim = trade
            .token_b_amount
            .checked_mul(safety_multiplier)
            .context("paper token-B safety reservation overflow")?;
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
            ],
        };
        let request = ReservationRequest {
            operation_id: plan_id.clone(),
            purpose: ReservationPurpose::TradePrimary,
            claims,
            settlement_venues: [InventoryVenue::Binance, InventoryVenue::Wallet]
                .into_iter()
                .collect(),
        };
        if let Err(error) = self.inventory.reserve(request) {
            self.telemetry.emit(
                "arbitrage_admission_rejected",
                json!({
                    "engine_id": self.config.engine_id,
                    "plan_id": plan_id,
                    "reason": "insufficient_available_inventory",
                    "error": format!("{error:#}"),
                }),
            );
            return Ok(());
        }
        self.arbitrage_plan_freshness.insert(
            plan_id.clone(),
            ArbitragePlanFreshness {
                pair_id: pair_id.clone(),
                pool_index: trade.pool_index,
                pool_generation: dex_pool_generation,
            },
        );
        if !handle.try_submit(opportunity) {
            self.arbitrage_plan_freshness.remove(&plan_id);
            self.inventory.release_unsubmitted(&plan_id)?;
            self.telemetry.emit(
                "arbitrage_admission_rejected",
                json!({
                    "engine_id": self.config.engine_id,
                    "plan_id": plan_id,
                    "reason": "paper_execution_queue_full",
                }),
            );
            return Ok(());
        }
        self.telemetry.emit(
            "arbitrage_admitted",
            json!({
                "engine_id": self.config.engine_id,
                "plan_id": plan_id,
                "mode": self.config.arbitrage_execution_mode,
                "balance_safety_multiplier": balance_safety_multiplier,
                "execution_slippage_bps": trade.execution_slippage_bps,
                "cex_primary_limit_price": match direction {
                    TradeDirection::BuyTokenBOnDexSellOnCex => quote.bid_price.to_string(),
                    TradeDirection::BuyTokenBOnCexSellOnDex => quote.ask_price.to_string(),
                },
                "recovery_limit_price": economics.recovery_limit_price.to_string(),
                "recovery_sell_limit_price": economics.recovery_sell_limit_price.to_string(),
                "recovery_buy_limit_price": economics.recovery_buy_limit_price.to_string(),
                "recovery_quote_token_a_base_units": economics.recovery_quote_token_a.to_string(),
                "recovery_sell_quote_token_a_base_units": economics.recovery_sell_quote_token_a.to_string(),
                "recovery_buy_quote_token_a_base_units": economics.recovery_buy_quote_token_a.to_string(),
                "recovery_loss_token_a_base_units": economics.recovery_loss_token_a.to_string(),
                "maximum_gas_wei": economics.maximum_gas_wei.to_string(),
                "maximum_fee_per_gas_wei": economics.maximum_fee_per_gas_wei.to_string(),
                "gas_conversion_price_token_a": native_price_token_a.to_string(),
                "maximum_gas_cost_token_a_base_units": economics.maximum_gas_cost_token_a.to_string(),
                "fully_burdened_cost_token_a_base_units": economics.fully_burdened_cost_token_a.to_string(),
                "bounded_profit_token_a_base_units": economics.bounded_profit_token_a.to_string(),
                "dex_plan": dex_plan_telemetry_value(&dex_plan),
            }),
        );
        Ok(())
    }

    fn emit_admission_risk_rejection(
        &self,
        quote: &TopOfBook,
        pair_id: &str,
        direction: TradeDirection,
        reason: &'static str,
        economics: Option<AdmissionEconomics>,
    ) {
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
                "recovery_sell_limit_price": economics.map(|value| value.recovery_sell_limit_price.to_string()),
                "recovery_buy_limit_price": economics.map(|value| value.recovery_buy_limit_price.to_string()),
                "recovery_quote_token_a_base_units": economics.map(|value| value.recovery_quote_token_a.to_string()),
                "recovery_sell_quote_token_a_base_units": economics.map(|value| value.recovery_sell_quote_token_a.to_string()),
                "recovery_buy_quote_token_a_base_units": economics.map(|value| value.recovery_buy_quote_token_a.to_string()),
                "recovery_loss_token_a_base_units": economics.map(|value| value.recovery_loss_token_a.to_string()),
                "maximum_gas_wei": economics.map(|value| value.maximum_gas_wei.to_string()),
                "maximum_fee_per_gas_wei": economics.map(|value| value.maximum_fee_per_gas_wei.to_string()),
                "maximum_gas_cost_token_a_base_units": economics.map(|value| value.maximum_gas_cost_token_a.to_string()),
                "fully_burdened_cost_token_a_base_units": economics.map(|value| value.fully_burdened_cost_token_a.to_string()),
                "bounded_profit_token_a_base_units": economics.map(|value| value.bounded_profit_token_a.to_string()),
            }),
        );
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
                    self.arbitrage_settlement_barriers.insert(
                        freshness.pool_index,
                        ArbitrageSettlementBarrier {
                            pair_id: freshness.pair_id,
                            plan_id: event.plan_id.clone(),
                            pool_generation: freshness.pool_generation,
                            started_at: Instant::now(),
                        },
                    );
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

    pub fn refresh_health(&mut self) {
        let now = Instant::now();
        self.refresh_phase(now);
        self.log_rebalance_health(now);
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
        let dex_ready = self.dex.is_fresh(now, self.config.dex_head_max_age_ms)
            && self.opportunities.is_ready();
        let balances_ready = self
            .state
            .balances
            .is_fresh(now, self.config.balance_max_age_ms);
        // Rebalancing is proactive inventory maintenance, not a global trading
        // lock. Its pending, in-flight, failed, and post-reconciliation states
        // serialize only rebalance operations. Trading remains gated by fresh
        // market/DEX/balance inputs; an execution coordinator must separately
        // reserve and validate the assets required by its concrete trade.
        let trading_readiness = TradingReadiness {
            dex_ready,
            balances_ready,
            user_data_ready: self.binance_user_data_connected && self.binance_user_data_clean,
            gas_price_ready: self.gas_price_connected
                && self.gas_price_book.as_ref().is_some_and(|book| {
                    now.saturating_duration_since(book.received_at).as_millis()
                        <= u128::from(self.config.market_data_max_age_ms)
                }),
        };
        let current = self.state.refresh_phase(
            now,
            self.config.market_data_max_age_ms,
            trading_readiness.ready(),
        );
        if previous != current {
            tracing::info!(?previous, ?current, "runtime phase changed");
            self.telemetry.emit(
                "runtime_phase_changed",
                json!({
                    "engine_id": self.config.engine_id,
                    "previous": previous,
                    "current": current,
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

fn u256_to_i128(value: U256, name: &str) -> anyhow::Result<i128> {
    let value = u128::try_from(value).map_err(|_| anyhow::anyhow!("{name} exceeds u128"))?;
    i128::try_from(value).map_err(|_| anyhow::anyhow!("{name} exceeds i128"))
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
    use std::{collections::BTreeMap, time::Instant};

    use crate::{
        execution_plan::{DexRoutePlan, DexSwapPlan},
        rebalance::Direction,
    };

    use super::{
        RebalanceSettlementBarrier, TradingReadiness, mark_sequence_matched_update,
        rebalance_health_state,
    };

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

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use alloy_primitives::Address;
use anyhow::Context;
use serde_json::json;
use tokio::sync::mpsc;

use crate::{
    config::AppConfig,
    dex::{hydration::PoolIdentity, mirror::DexMirror},
    execution_accounting::native_gas_to_token_a_base_units,
    opportunity::{
        ArbitrageDirection, DirectionEvaluation, PairEvaluation, PairRuntime, PreparedPoolRefresh,
        TradeEvaluation, format_base_units,
    },
    pretrade_cost::{
        DexPoolCostKey, DexRouteCostKey, GAS_PRICE_HISTORY_DEPTH, NATIVE_CONVERSION_HISTORY_DEPTH,
        PreTradeCostSnapshot, PreTradeCostTelemetry, RECEIPT_HISTORY_DEPTH,
    },
    state::{RuntimePhase, TopOfBook},
    telemetry::{
        PRIMARY_BINANCE_ACCOUNT_ID, TelemetryHandle, instrument_id, network_id, pool_id,
        strategy_id,
    },
};

#[derive(Clone)]
pub struct HotTelemetryHandle {
    book_sender: mpsc::Sender<HotBookTelemetry>,
    evaluation_sender: mpsc::Sender<HotEvaluationTelemetry>,
    dex_event_sender: mpsc::Sender<HotDexEventTelemetry>,
    prepared_pool_sender: mpsc::Sender<PreparedPoolRefresh>,
    shared_stream_sender: mpsc::Sender<HotSharedStreamTelemetry>,
    pretrade_candidate_sender: mpsc::Sender<HotPreTradeCandidateTelemetry>,
    dropped: Arc<AtomicU64>,
    auxiliary_book_last_emitted_unix_us: Arc<[AtomicU64; 2]>,
    pretrade_cost: PreTradeCostTelemetry,
}

pub struct HotTelemetryTask {
    book_receiver: mpsc::Receiver<HotBookTelemetry>,
    evaluation_receiver: mpsc::Receiver<HotEvaluationTelemetry>,
    dex_event_receiver: mpsc::Receiver<HotDexEventTelemetry>,
    prepared_pool_receiver: mpsc::Receiver<PreparedPoolRefresh>,
    shared_stream_receiver: mpsc::Receiver<HotSharedStreamTelemetry>,
    pretrade_candidate_receiver: mpsc::Receiver<HotPreTradeCandidateTelemetry>,
    dropped: Arc<AtomicU64>,
    telemetry: TelemetryHandle,
    context: HotTelemetryContext,
    pretrade_cost: PreTradeCostTelemetry,
    last_pretrade_cost_sampled_at: Vec<Option<Instant>>,
}

const AUXILIARY_BOOK_SAMPLE_INTERVAL_US: u64 = 1_000_000;

struct HotBookTelemetry {
    quote: TopOfBook,
    decision_complete_us: u128,
    queued_at: std::time::Instant,
    feed_role: &'static str,
    runtime_phase: Option<RuntimePhase>,
    decision_outcome: &'static str,
}

struct HotEvaluationTelemetry {
    quote: TopOfBook,
    evaluation: PairEvaluation,
    world_chain_block: u64,
    calculation_time_us: u128,
    calculation_budget_us: u64,
    decision_latency_us: u128,
    cost_as_of_unix_us: u64,
    trigger: &'static str,
    queued_at: std::time::Instant,
}

struct HotPreTradeCandidateTelemetry {
    plan_id: String,
    quote: TopOfBook,
    pair_index: usize,
    direction: ArbitrageDirection,
    trade: TradeEvaluation,
    cost_as_of_unix_us: u64,
    strategy_price_age_us: u64,
    queued_at: Instant,
}

enum HotDexEventTelemetry {
    PoolEvent {
        pool_index: usize,
        kind: &'static str,
        block_number: u64,
        transaction_index: u64,
        log_index: u64,
        engine_queue_age_us: u128,
        prepared_generation: u64,
    },
    Head {
        block_number: u64,
        engine_queue_age_us: u128,
    },
}

#[derive(Clone, Copy)]
pub enum SharedStreamEventKind {
    Connected,
    Disconnected,
    Heartbeat,
    BookTicker,
    Depth,
}

struct HotSharedStreamTelemetry {
    symbol: [u8; 16],
    symbol_len: u8,
    event_kind: SharedStreamEventKind,
    generation: u64,
    parse_time_us: u128,
    wire_frame_size_bytes: usize,
    queued_at: std::time::Instant,
}

struct HotTelemetryContext {
    engine_id: String,
    pairs: Vec<PairTelemetryContext>,
    pools: Vec<PoolTelemetryContext>,
    head_chain_id: Option<u64>,
}

struct PairTelemetryContext {
    pair_id: String,
    strategy_id: String,
    instrument_id: String,
    network_id: String,
    chain_id: u64,
    symbol: String,
    token_a_symbol: String,
    token_b_symbol: String,
    token_a_decimals: u8,
    token_b_decimals: u8,
    token_a_address: Address,
    token_b_address: Address,
    opportunity_threshold_bps: u16,
    min_slippage_bps: u16,
    max_slippage_bps: u16,
    slippage_profit_share_bps: u16,
    binance_buy_fee_bps: u16,
    binance_sell_fee_bps: u16,
}

struct PoolTelemetryContext {
    pair_id: String,
    strategy_id: String,
    network_id: String,
    identity: String,
    pool_id: String,
    fee_pips: u32,
    cost_pool_key: DexPoolCostKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DecisionDirectionProjection {
    direction: ArbitrageDirection,
    cex_top_token_b_amount: alloy_primitives::U256,
    baseline: Option<TradeEvaluation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DecisionProjection {
    pair_index: usize,
    baseline_token_b_amount: alloy_primitives::U256,
    directions: [DecisionDirectionProjection; 2],
}

impl DecisionProjection {
    fn candidate_count(self) -> usize {
        self.directions
            .iter()
            .filter(|direction| {
                direction
                    .baseline
                    .is_some_and(|trade| trade.meets_threshold)
            })
            .count()
    }
}

fn project_direction(direction: DirectionEvaluation) -> DecisionDirectionProjection {
    DecisionDirectionProjection {
        direction: direction.direction,
        cex_top_token_b_amount: direction.cex_top_token_b_amount,
        baseline: direction.baseline,
    }
}

fn legacy_decision_projection(evaluation: &PairEvaluation) -> DecisionProjection {
    DecisionProjection {
        pair_index: evaluation.pair_index,
        baseline_token_b_amount: evaluation.baseline_token_b_amount,
        directions: [
            DecisionDirectionProjection {
                direction: evaluation.dex_buy_cex_sell.direction,
                cex_top_token_b_amount: evaluation.dex_buy_cex_sell.cex_top_token_b_amount,
                baseline: evaluation.dex_buy_cex_sell.baseline,
            },
            DecisionDirectionProjection {
                direction: evaluation.cex_buy_dex_sell.direction,
                cex_top_token_b_amount: evaluation.cex_buy_dex_sell.cex_top_token_b_amount,
                baseline: evaluation.cex_buy_dex_sell.baseline,
            },
        ],
    }
}

fn ownership_graph_decision_projection(evaluation: &PairEvaluation) -> DecisionProjection {
    let routed = [evaluation.dex_buy_cex_sell, evaluation.cex_buy_dex_sell];
    DecisionProjection {
        pair_index: evaluation.pair_index,
        baseline_token_b_amount: evaluation.baseline_token_b_amount,
        directions: routed.map(project_direction),
    }
}

pub fn channel(
    config: &AppConfig,
    pairs: &[PairRuntime],
    dex: &DexMirror,
    telemetry: TelemetryHandle,
    pretrade_cost: PreTradeCostTelemetry,
) -> anyhow::Result<(HotTelemetryHandle, HotTelemetryTask)> {
    let mut pools = Vec::with_capacity(dex.pool_count());
    for index in 0..dex.pool_count() {
        let pool = dex.pool(index)?;
        let pair = pairs
            .iter()
            .find(|pair| pair.pair_id == pool.pair_id)
            .context("hot telemetry pool pair is invalid")?;
        let identity = format!("{:?}", pool.identity);
        let cost_pool_key = match pool.identity {
            PoolIdentity::V3 { address, .. } => DexPoolCostKey::UniswapV3(address),
            PoolIdentity::PancakeV3 { address, .. } => DexPoolCostKey::PancakeSwapV3(address),
            PoolIdentity::CamelotV3 { address } => DexPoolCostKey::CamelotV3(address),
            PoolIdentity::V4 { pool_id, .. } => DexPoolCostKey::UniswapV4(pool_id),
        };
        pools.push(PoolTelemetryContext {
            pair_id: pair.pair_id.clone(),
            strategy_id: strategy_id(&pair.pair_id),
            network_id: network_id(pair.chain_id),
            pool_id: pool_id(pair.chain_id, &identity),
            identity,
            fee_pips: pool.pool.fee_pips,
            cost_pool_key,
        });
    }
    let head_chain_id = pairs.first().map(|pair| pair.chain_id);
    let pairs = pairs
        .iter()
        .map(|pair| PairTelemetryContext {
            pair_id: pair.pair_id.clone(),
            strategy_id: strategy_id(&pair.pair_id),
            instrument_id: instrument_id(&pair.symbol),
            network_id: network_id(pair.chain_id),
            chain_id: pair.chain_id,
            symbol: pair.symbol.clone(),
            token_a_symbol: pair.token_a_symbol.clone(),
            token_b_symbol: pair.token_b_symbol.clone(),
            token_a_decimals: pair.token_a_decimals,
            token_b_decimals: pair.token_b_decimals,
            token_a_address: pair.token_a_address(),
            token_b_address: pair.token_b_address(),
            opportunity_threshold_bps: pair.opportunity_threshold_bps,
            min_slippage_bps: pair.min_slippage_bps,
            max_slippage_bps: pair.max_slippage_bps,
            slippage_profit_share_bps: pair.slippage_profit_share_bps,
            binance_buy_fee_bps: pair.binance_buy_fee_bps,
            binance_sell_fee_bps: pair.binance_sell_fee_bps,
        })
        .collect();
    let context = HotTelemetryContext {
        engine_id: config.engine_id.clone(),
        pairs,
        pools,
        head_chain_id,
    };
    let pair_count = context.pairs.len();
    let (book_sender, book_receiver) = mpsc::channel(config.telemetry_channel_capacity);
    let (evaluation_sender, evaluation_receiver) = mpsc::channel(config.telemetry_channel_capacity);
    let (dex_event_sender, dex_event_receiver) = mpsc::channel(config.telemetry_channel_capacity);
    let (prepared_pool_sender, prepared_pool_receiver) =
        mpsc::channel(config.telemetry_channel_capacity);
    let (shared_stream_sender, shared_stream_receiver) =
        mpsc::channel(config.telemetry_channel_capacity);
    let (pretrade_candidate_sender, pretrade_candidate_receiver) =
        mpsc::channel(config.telemetry_channel_capacity);
    let dropped = Arc::new(AtomicU64::new(0));
    let auxiliary_book_last_emitted_unix_us = Arc::new([AtomicU64::new(0), AtomicU64::new(0)]);
    Ok((
        HotTelemetryHandle {
            book_sender,
            evaluation_sender,
            dex_event_sender,
            prepared_pool_sender,
            shared_stream_sender,
            pretrade_candidate_sender,
            dropped: Arc::clone(&dropped),
            auxiliary_book_last_emitted_unix_us,
            pretrade_cost: pretrade_cost.clone(),
        },
        HotTelemetryTask {
            book_receiver,
            evaluation_receiver,
            dex_event_receiver,
            prepared_pool_receiver,
            shared_stream_receiver,
            pretrade_candidate_receiver,
            dropped,
            telemetry,
            context,
            pretrade_cost,
            last_pretrade_cost_sampled_at: vec![None; pair_count],
        },
    ))
}

impl HotTelemetryHandle {
    pub fn publish_native_conversion(
        &self,
        captured_unix_us: u64,
        price_token_a: rust_decimal::Decimal,
    ) {
        self.pretrade_cost
            .publish_native_conversion(captured_unix_us, price_token_a);
    }

    #[inline]
    pub fn emit_binance_book(
        &self,
        quote: &TopOfBook,
        feed_role: &'static str,
        runtime_phase: Option<RuntimePhase>,
        decision_outcome: &'static str,
    ) {
        let auxiliary_index = match feed_role {
            "gas_conversion" => Some(0),
            "commission_conversion" => Some(1),
            _ => None,
        };
        if let Some(index) = auxiliary_index
            && !claim_auxiliary_book_sample(
                &self.auxiliary_book_last_emitted_unix_us[index],
                quote.received_unix_us,
            )
        {
            return;
        }
        let decision_complete_us = quote.received_at.elapsed().as_micros();
        if self
            .book_sender
            .try_send(HotBookTelemetry {
                quote: quote.clone(),
                decision_complete_us,
                queued_at: std::time::Instant::now(),
                feed_role,
                runtime_phase,
                decision_outcome,
            })
            .is_err()
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn dropped_records(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn emit_evaluation(
        &self,
        quote: &TopOfBook,
        evaluation: PairEvaluation,
        chain_block: u64,
        calculation_time_us: u128,
        calculation_budget_us: u64,
        trigger: &'static str,
    ) {
        let decision_latency_us = quote.received_at.elapsed().as_micros();
        let cost_as_of_unix_us =
            decision_boundary_unix_us(quote.received_unix_us, decision_latency_us);
        if self
            .evaluation_sender
            .try_send(HotEvaluationTelemetry {
                quote: quote.clone(),
                evaluation,
                world_chain_block: chain_block,
                calculation_time_us,
                calculation_budget_us,
                decision_latency_us,
                cost_as_of_unix_us,
                trigger,
                queued_at: std::time::Instant::now(),
            })
            .is_err()
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Enqueue the exact selected candidate for a joinable background cost
    /// calculation. The trading owner performs no cost math or locking here.
    #[inline]
    pub fn emit_pretrade_candidate(
        &self,
        plan_id: &str,
        quote: &TopOfBook,
        pair_index: usize,
        direction: ArbitrageDirection,
        trade: TradeEvaluation,
    ) {
        let strategy_price_age_us =
            u64::try_from(quote.received_at.elapsed().as_micros()).unwrap_or(u64::MAX);
        let cost_as_of_unix_us =
            decision_boundary_unix_us(quote.received_unix_us, strategy_price_age_us.into());
        if self
            .pretrade_candidate_sender
            .try_send(HotPreTradeCandidateTelemetry {
                plan_id: plan_id.to_owned(),
                quote: quote.clone(),
                pair_index,
                direction,
                trade,
                cost_as_of_unix_us,
                strategy_price_age_us,
                queued_at: Instant::now(),
            })
            .is_err()
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub fn emit_dex_pool_event(
        &self,
        pool_index: usize,
        kind: &'static str,
        block_number: u64,
        transaction_index: u64,
        log_index: u64,
        engine_queue_age_us: u128,
        prepared_generation: u64,
    ) {
        if self
            .dex_event_sender
            .try_send(HotDexEventTelemetry::PoolEvent {
                pool_index,
                kind,
                block_number,
                transaction_index,
                log_index,
                engine_queue_age_us,
                prepared_generation,
            })
            .is_err()
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn emit_dex_head(&self, block_number: u64, engine_queue_age_us: u128) {
        if self
            .dex_event_sender
            .try_send(HotDexEventTelemetry::Head {
                block_number,
                engine_queue_age_us,
            })
            .is_err()
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn emit_dex_pool_prepared(&self, prepared: PreparedPoolRefresh) {
        if self.prepared_pool_sender.try_send(prepared).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn emit_shared_stream_event(
        &self,
        symbol: &str,
        event_kind: SharedStreamEventKind,
        generation: u64,
        parse_time_us: u128,
        wire_frame_size_bytes: usize,
    ) {
        let symbol_bytes = symbol.as_bytes();
        if symbol_bytes.len() > 16 {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let mut fixed_symbol = [0_u8; 16];
        fixed_symbol[..symbol_bytes.len()].copy_from_slice(symbol_bytes);
        if self
            .shared_stream_sender
            .try_send(HotSharedStreamTelemetry {
                symbol: fixed_symbol,
                symbol_len: symbol_bytes.len() as u8,
                event_kind,
                generation,
                parse_time_us,
                wire_frame_size_bytes,
                queued_at: std::time::Instant::now(),
            })
            .is_err()
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl HotTelemetryTask {
    fn sample_pretrade_cost(&mut self, pair_index: usize) -> Option<PreTradeCostSnapshot> {
        const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

        let snapshot = self.pretrade_cost.snapshot()?;
        let now = Instant::now();
        let last_sampled_at = self.last_pretrade_cost_sampled_at.get_mut(pair_index)?;
        if last_sampled_at
            .is_some_and(|sampled_at| now.saturating_duration_since(sampled_at) < SAMPLE_INTERVAL)
        {
            return None;
        }
        *last_sampled_at = Some(now);
        Some(snapshot)
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut books_open = true;
        let mut evaluations_open = true;
        let mut dex_events_open = true;
        let mut prepared_pools_open = true;
        let mut shared_stream_open = true;
        let mut pretrade_candidates_open = true;
        while books_open
            || evaluations_open
            || dex_events_open
            || prepared_pools_open
            || shared_stream_open
            || pretrade_candidates_open
        {
            tokio::select! {
                event = self.book_receiver.recv(), if books_open => match event {
                    Some(event) => self.emit_binance_book(
                        &event.quote,
                        event.decision_complete_us,
                        event.queued_at.elapsed().as_micros(),
                        event.feed_role,
                        event.runtime_phase,
                        event.decision_outcome,
                    ),
                    None => books_open = false,
                },
                event = self.evaluation_receiver.recv(), if evaluations_open => match event {
                    Some(event) => {
                        let cost_snapshot = self.sample_pretrade_cost(event.evaluation.pair_index);
                        self.emit_evaluation(
                            &event.quote,
                            &event.evaluation,
                            event.world_chain_block,
                            event.calculation_time_us,
                            event.calculation_budget_us,
                            event.decision_latency_us,
                            event.cost_as_of_unix_us,
                            event.trigger,
                            event.queued_at.elapsed().as_micros(),
                            cost_snapshot,
                        )?
                    },
                    None => evaluations_open = false,
                },
                event = self.dex_event_receiver.recv(), if dex_events_open => match event {
                    Some(event) => self.emit_dex_event(event)?,
                    None => dex_events_open = false,
                },
                event = self.prepared_pool_receiver.recv(), if prepared_pools_open => match event {
                    Some(prepared) => self.emit_prepared_pool(prepared)?,
                    None => prepared_pools_open = false,
                },
                event = self.shared_stream_receiver.recv(), if shared_stream_open => match event {
                    Some(event) => self.emit_shared_stream_event(event),
                    None => shared_stream_open = false,
                },
                event = self.pretrade_candidate_receiver.recv(), if pretrade_candidates_open => match event {
                    Some(event) => self.emit_pretrade_candidate(event)?,
                    None => pretrade_candidates_open = false,
                },
            }
        }
        let dropped = self.dropped.swap(0, Ordering::Relaxed);
        if dropped > 0 {
            tracing::warn!(
                dropped,
                "hot telemetry records dropped outside decision path"
            );
        }
        Ok(())
    }

    fn emit_pretrade_candidate(&self, event: HotPreTradeCandidateTelemetry) -> anyhow::Result<()> {
        let pair = self
            .context
            .pairs
            .get(event.pair_index)
            .context("pre-trade candidate pair index is invalid")?;
        let cost_snapshot = self.pretrade_cost.snapshot();
        let candidate = self.trade_payload(
            pair,
            event.direction,
            event.trade,
            &event.quote,
            cost_snapshot,
            event.cost_as_of_unix_us,
            event.strategy_price_age_us,
        )?;
        self.telemetry.emit(
            "pretrade_cost_candidate",
            json!({
                "engine_id": self.context.engine_id,
                "plan_id": event.plan_id,
                "pair_id": pair.pair_id,
                "strategy_id": pair.strategy_id,
                "binance_account_id": PRIMARY_BINANCE_ACCOUNT_ID,
                "instrument_id": pair.instrument_id,
                "network_id": pair.network_id,
                "chain_id": pair.chain_id,
                "symbol": pair.symbol,
                "update_id": event.quote.update_id,
                "opportunity_received_unix_us": event.quote.received_unix_us,
                "cost_as_of_unix_us": event.cost_as_of_unix_us,
                "strategy_price_age_us": event.strategy_price_age_us,
                "direction": event.direction.as_str(),
                "diagnostic_only": true,
                "decision_input": false,
                "candidate": candidate,
                "telemetry_queue_delay_us": event.queued_at.elapsed().as_micros(),
            }),
        );
        Ok(())
    }

    fn emit_shared_stream_event(&self, event: HotSharedStreamTelemetry) {
        let symbol = std::str::from_utf8(&event.symbol[..usize::from(event.symbol_len)])
            .unwrap_or("INVALID");
        let event_kind = match event.event_kind {
            SharedStreamEventKind::Connected => "connected",
            SharedStreamEventKind::Disconnected => "disconnected",
            SharedStreamEventKind::Heartbeat => "heartbeat",
            SharedStreamEventKind::BookTicker => "book_ticker",
            SharedStreamEventKind::Depth => "depth",
        };
        self.telemetry.emit(
            "binance_shared_stream_event",
            json!({
                "engine_id": self.context.engine_id,
                "account_scope": "primary_spot",
                "symbol": symbol,
                "event_kind": event_kind,
                "generation": event.generation,
                "wire_frame_size_bytes": event.wire_frame_size_bytes,
                "parse_time_us": event.parse_time_us,
                "readiness_scope": "symbol",
                "execution_enabled": false,
                "direct_owner_poll": true,
                "telemetry_queue_delay_us": event.queued_at.elapsed().as_micros(),
            }),
        );
    }

    fn emit_dex_event(&self, event: HotDexEventTelemetry) -> anyhow::Result<()> {
        match event {
            HotDexEventTelemetry::PoolEvent {
                pool_index,
                kind,
                block_number,
                transaction_index,
                log_index,
                engine_queue_age_us,
                prepared_generation,
            } => {
                let pool = self
                    .context
                    .pools
                    .get(pool_index)
                    .context("hot DEX telemetry pool index is invalid")?;
                self.telemetry.emit(
                    "dex_pool_event",
                    json!({
                        "engine_id": self.context.engine_id,
                        "pair_id": pool.pair_id,
                        "strategy_id": pool.strategy_id,
                        "network_id": pool.network_id,
                        "pool_id": pool.pool_id,
                        "identity": pool.identity,
                        "kind": kind,
                        "block_number": block_number,
                        "transaction_index": transaction_index,
                        "log_index": log_index,
                        "engine_queue_age_us": engine_queue_age_us,
                        "prepared_generation": prepared_generation,
                        "prepared_state": "building",
                    }),
                );
            }
            HotDexEventTelemetry::Head {
                block_number,
                engine_queue_age_us,
            } => {
                self.telemetry.emit(
                    "world_chain_head",
                    json!({
                        "engine_id": self.context.engine_id,
                        "network_id": self.context.head_chain_id.map(network_id),
                        "chain_id": self.context.head_chain_id,
                        "block_number": block_number,
                        "engine_queue_age_us": engine_queue_age_us,
                    }),
                );
            }
        }
        Ok(())
    }

    fn emit_prepared_pool(&self, prepared: PreparedPoolRefresh) -> anyhow::Result<()> {
        let pool = self
            .context
            .pools
            .get(prepared.pool_index)
            .context("hot prepared-pool telemetry index is invalid")?;
        self.telemetry.emit(
            "dex_pool_prepared",
            json!({
                "engine_id": self.context.engine_id,
                "pair_id": pool.pair_id,
                "strategy_id": pool.strategy_id,
                "network_id": pool.network_id,
                "pool_id": pool.pool_id,
                "identity": pool.identity,
                "pool_index": prepared.pool_index,
                "prepared_generation": prepared.generation,
                "prepared_exact_output_segments": prepared.exact_output_segments,
                "prepared_exact_input_segments": prepared.exact_input_segments,
                "prepared_token_a_exact_input_segments": prepared.token_a_exact_input_segments,
                "prepared_curve_scope": "execution_envelope_v1",
                "prepared_build_mode": "inline_owner_v1",
                "prepared_token_a_limit_base_units": prepared.token_a_limit.to_string(),
                "prepared_exact_output_token_b_limit_base_units": prepared.exact_output_token_b_limit.to_string(),
                "prepared_exact_input_token_b_limit_base_units": prepared.exact_input_token_b_limit.to_string(),
                "fee_generation": prepared.fee_generation,
                "fee_zero_for_one_pips": prepared.fee_zero_for_one_pips,
                "fee_one_for_zero_pips": prepared.fee_one_for_zero_pips,
                "build_time_us": prepared.build_time_us,
                "pre_dispatch_time_us": prepared.pre_dispatch_time_us,
                "request_send_time_us": prepared.request_send_time_us,
                "request_handoff_time_us": prepared.request_handoff_time_us,
                "builder_pre_build_time_us": prepared.builder_pre_build_time_us,
                "builder_post_build_time_us": prepared.builder_post_build_time_us,
                "result_send_time_us": prepared.result_send_time_us,
                "result_handoff_time_us": prepared.result_handoff_time_us,
                "owner_publish_time_us": prepared.owner_publish_time_us,
                "stage_timing_complete": prepared.timing_complete,
                "total_time_us": prepared.total_time_us,
            }),
        );
        Ok(())
    }

    fn emit_binance_book(
        &self,
        quote: &TopOfBook,
        decision_complete_us: u128,
        telemetry_queue_delay_us: u128,
        feed_role: &'static str,
        runtime_phase: Option<RuntimePhase>,
        decision_outcome: &'static str,
    ) {
        let sampling_interval_ms = matches!(feed_role, "gas_conversion" | "commission_conversion")
            .then_some(AUXILIARY_BOOK_SAMPLE_INTERVAL_US / 1_000);
        self.telemetry.emit(
            "binance_book_ticker",
            json!({
                "engine_id": self.context.engine_id,
                "binance_account_id": PRIMARY_BINANCE_ACCOUNT_ID,
                "instrument_id": instrument_id(quote.symbol.as_ref()),
                "product": "spot",
                "symbol": quote.symbol.as_ref(),
                "update_id": quote.update_id,
                "bid_price": quote.bid_price.to_string(),
                "bid_quantity": quote.bid_quantity.to_string(),
                "ask_price": quote.ask_price.to_string(),
                "ask_quantity": quote.ask_quantity.to_string(),
                "exchange_event_ts_ms": quote.exchange_event_ts_ms,
                "exchange_transaction_ts_ms": quote.exchange_transaction_ts_ms,
                "received_unix_us": quote.received_unix_us,
                "connection_generation": quote.connection_generation,
                "wire_frame_size_bytes": quote.wire_frame_size_bytes,
                "parse_time_us": quote.parse_time_us,
                "feed_role": feed_role,
                "runtime_phase": runtime_phase,
                "decision_outcome": decision_outcome,
                "telemetry_sampling_interval_ms": sampling_interval_ms,
                "exchange_timestamp_available": quote.exchange_event_ts_ms.is_some()
                    || quote.exchange_transaction_ts_ms.is_some(),
                "decision_complete_us": decision_complete_us,
                "engine_queue_age_us": decision_complete_us,
                "telemetry_queue_delay_us": telemetry_queue_delay_us,
            }),
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_evaluation(
        &self,
        quote: &TopOfBook,
        evaluation: &PairEvaluation,
        world_chain_block: u64,
        calculation_time_us: u128,
        calculation_budget_us: u64,
        decision_latency_us: u128,
        cost_as_of_unix_us: u64,
        trigger: &'static str,
        compatibility_queue_us: u128,
        cost_snapshot: Option<PreTradeCostSnapshot>,
    ) -> anyhow::Result<()> {
        let pair = self
            .context
            .pairs
            .get(evaluation.pair_index)
            .context("hot telemetry pair index is invalid")?;
        let strategy_price_age_us = u64::try_from(decision_latency_us).unwrap_or(u64::MAX);
        let directions = [
            self.direction_payload(
                pair,
                &evaluation.dex_buy_cex_sell,
                quote,
                cost_snapshot,
                cost_as_of_unix_us,
                strategy_price_age_us,
            )?,
            self.direction_payload(
                pair,
                &evaluation.cex_buy_dex_sell,
                quote,
                cost_snapshot,
                cost_as_of_unix_us,
                strategy_price_age_us,
            )?,
        ];
        self.telemetry.emit(
            "arbitrage_evaluation",
            json!({
                "engine_id": self.context.engine_id,
                "pair_id": pair.pair_id,
                "strategy_id": pair.strategy_id,
                "binance_account_id": PRIMARY_BINANCE_ACCOUNT_ID,
                "instrument_id": pair.instrument_id,
                "network_id": pair.network_id,
                "chain_id": pair.chain_id,
                "chain_block": world_chain_block,
                "symbol": pair.symbol,
                "update_id": quote.update_id,
                "world_chain_block": world_chain_block,
                "baseline_token_b_base_units": evaluation.baseline_token_b_amount.to_string(),
                "baseline_token_b": format_base_units(
                    evaluation.baseline_token_b_amount,
                    pair.token_b_decimals,
                ),
                "opportunity_threshold_bps": pair.opportunity_threshold_bps,
                "min_slippage_bps": pair.min_slippage_bps,
                "max_slippage_bps": pair.max_slippage_bps,
                "slippage_profit_share_bps": pair.slippage_profit_share_bps,
                "binance_book_product": "spot",
                "binance_execution_product": "spot",
                "sizing_model": "adaptive_dex_curve_slot",
                "includes_binance_fee": false,
                "binance_buy_fee_bps": pair.binance_buy_fee_bps,
                "binance_sell_fee_bps": pair.binance_sell_fee_bps,
                "includes_gas": false,
                "includes_inventory": false,
                "baseline_quote_cache_hits": evaluation.baseline_cache_hits,
                "baseline_quote_cache_misses": evaluation.baseline_cache_misses,
                "calculation_time_us": calculation_time_us,
                "calculation_budget_us": calculation_budget_us,
                "calculation_budget_exceeded":
                    calculation_time_us > u128::from(calculation_budget_us),
                "decision_latency_us": decision_latency_us,
                "cost_as_of_unix_us": cost_as_of_unix_us,
                "strategy_price_age_us": strategy_price_age_us,
                "telemetry_queue_delay_us": compatibility_queue_us,
                "pretrade_cost_sampled": cost_snapshot.is_some(),
                "pretrade_cost_sampling_interval_ms": 1_000,
                "evaluation_trigger": trigger,
                "dependency_fanout_count": 1,
                "directions": directions,
            }),
        );
        let old_baseline = legacy_decision_projection(evaluation);
        let ownership_graph = ownership_graph_decision_projection(evaluation);
        let decisions_match = old_baseline == ownership_graph;
        self.telemetry.emit(
            "strategy_decision_compatibility",
            json!({
                "engine_id": self.context.engine_id,
                "pair_id": pair.pair_id,
                "strategy_id": pair.strategy_id,
                "binance_account_id": PRIMARY_BINANCE_ACCOUNT_ID,
                "network_id": pair.network_id,
                "execution_lane_id": crate::telemetry::execution_lane_id(pair.chain_id),
                "symbol": pair.symbol,
                "update_id": quote.update_id,
                "comparison_mode": "background_immutable_decision_projection",
                "old_baseline_source": "v12_compatibility_evaluator",
                "new_path_source": "hot_path_decision_owner",
                "comparison_queue_us": compatibility_queue_us,
                "decisions_match": decisions_match,
                "old_baseline_candidate_count": old_baseline.candidate_count(),
                "new_path_candidate_count": ownership_graph.candidate_count(),
                "external_mutation_authorized": false,
            }),
        );
        if !decisions_match {
            tracing::error!(
                binance_account_id = PRIMARY_BINANCE_ACCOUNT_ID,
                network_id = %pair.network_id,
                strategy_id = %pair.strategy_id,
                execution_lane_id = %crate::telemetry::execution_lane_id(pair.chain_id),
                symbol = %pair.symbol,
                update_id = quote.update_id,
                dependency = "wld_decision_compatibility",
                supervisor_action = "close_new_strategy_mutations",
                "runtime dependency fault"
            );
        }

        for direction in [&evaluation.dex_buy_cex_sell, &evaluation.cex_buy_dex_sell] {
            if let Some(trade) = direction.baseline.filter(|trade| trade.meets_threshold) {
                self.telemetry.emit(
                    "arbitrage_opportunity",
                    json!({
                        "engine_id": self.context.engine_id,
                        "pair_id": pair.pair_id,
                        "strategy_id": pair.strategy_id,
                        "binance_account_id": PRIMARY_BINANCE_ACCOUNT_ID,
                        "instrument_id": pair.instrument_id,
                        "network_id": pair.network_id,
                        "chain_id": pair.chain_id,
                        "chain_block": world_chain_block,
                        "symbol": pair.symbol,
                        "update_id": quote.update_id,
                        "world_chain_block": world_chain_block,
                        "direction": direction.direction.as_str(),
                        "opportunity_threshold_bps": pair.opportunity_threshold_bps,
                        "min_slippage_bps": pair.min_slippage_bps,
                        "max_slippage_bps": pair.max_slippage_bps,
                        "slippage_profit_share_bps": pair.slippage_profit_share_bps,
                        "sizing_model": "adaptive_dex_curve_slot",
                        "execution_ready": false,
                        "includes_binance_fee": false,
                        "binance_buy_fee_bps": pair.binance_buy_fee_bps,
                        "binance_sell_fee_bps": pair.binance_sell_fee_bps,
                        "calculation_time_us": calculation_time_us,
                        "calculation_budget_us": calculation_budget_us,
                        "calculation_budget_exceeded":
                            calculation_time_us > u128::from(calculation_budget_us),
                        "decision_latency_us": decision_latency_us,
                        "evaluation_trigger": trigger,
                        "dependency_fanout_count": 1,
                        "baseline_pool_index": trade.pool_index,
                        "baseline_token_b_base_units": trade.token_b_amount.to_string(),
                        "baseline_gross_profit_bps_x100": trade.gross_profit_bps_x100,
                        "baseline": self.trade_payload(
                            pair,
                            direction.direction,
                            trade,
                            quote,
                            cost_snapshot,
                            cost_as_of_unix_us,
                            strategy_price_age_us,
                        )?,
                    }),
                );
            }
        }
        Ok(())
    }

    fn direction_payload(
        &self,
        pair: &PairTelemetryContext,
        direction: &DirectionEvaluation,
        quote: &TopOfBook,
        cost_snapshot: Option<PreTradeCostSnapshot>,
        cost_as_of_unix_us: u64,
        strategy_price_age_us: u64,
    ) -> anyhow::Result<serde_json::Value> {
        Ok(json!({
            "direction": direction.direction.as_str(),
            "cex_top_token_b_base_units": direction.cex_top_token_b_amount.to_string(),
            "cex_top_token_b": format_base_units(
                direction.cex_top_token_b_amount,
                pair.token_b_decimals,
            ),
            "baseline": direction
                .baseline
                .map(|trade| self.trade_payload(
                    pair,
                    direction.direction,
                    trade,
                    quote,
                    cost_snapshot,
                    cost_as_of_unix_us,
                    strategy_price_age_us,
                ))
                .transpose()?,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    fn trade_payload(
        &self,
        pair: &PairTelemetryContext,
        direction: ArbitrageDirection,
        trade: TradeEvaluation,
        quote: &TopOfBook,
        cost_snapshot: Option<PreTradeCostSnapshot>,
        cost_as_of_unix_us: u64,
        strategy_price_age_us: u64,
    ) -> anyhow::Result<serde_json::Value> {
        let pool = self
            .context
            .pools
            .get(trade.pool_index)
            .context("hot telemetry pool index is invalid")?;
        let profit = if trade.proceeds_token_a >= trade.cost_token_a {
            format_base_units(
                trade.proceeds_token_a - trade.cost_token_a,
                pair.token_a_decimals,
            )
        } else {
            format!(
                "-{}",
                format_base_units(
                    trade.cost_token_a - trade.proceeds_token_a,
                    pair.token_a_decimals,
                )
            )
        };
        let pretrade_cost = cost_snapshot
            .map(|snapshot| {
                pretrade_cost_payload(
                    pair,
                    pool,
                    direction,
                    trade,
                    quote,
                    snapshot,
                    cost_as_of_unix_us,
                    strategy_price_age_us,
                )
            })
            .transpose()?;
        Ok(json!({
            "pool_index": trade.pool_index,
            "pool_id": pool.pool_id,
            "pool_identity": pool.identity,
            "pool_fee_pips": pool.fee_pips,
            "token_b_symbol": pair.token_b_symbol,
            "token_b_base_units": trade.token_b_amount.to_string(),
            "token_b_amount": format_base_units(trade.token_b_amount, pair.token_b_decimals),
            "token_a_symbol": pair.token_a_symbol,
            "token_a_decimals": pair.token_a_decimals,
            "dex_token_a_base_units": trade.dex_token_a_amount.to_string(),
            "dex_token_a_amount": format_base_units(
                trade.dex_token_a_amount,
                pair.token_a_decimals,
            ),
            "cex_token_a_base_units": trade.cex_token_a_amount.to_string(),
            "cex_token_a_amount": format_base_units(
                trade.cex_token_a_amount,
                pair.token_a_decimals,
            ),
            "cost_token_a_base_units": trade.cost_token_a.to_string(),
            "proceeds_token_a_base_units": trade.proceeds_token_a.to_string(),
            "dex_amount_in_base_units": trade.dex_amount_in.to_string(),
            "dex_amount_out_minimum_base_units": trade.dex_amount_out_minimum.to_string(),
            "execution_slippage_bps": trade.execution_slippage_bps,
            "profit_token_a_base_units": trade.signed_profit_token_a(),
            "profit_token_a": profit,
            "gross_profit_bps_x100": trade.gross_profit_bps_x100,
            "gross_profit_bps": format_bps_x100(trade.gross_profit_bps_x100),
            "meets_threshold": trade.meets_threshold,
            "pretrade_cost": pretrade_cost,
        }))
    }
}

const PRETRADE_COST_MODEL_VERSION: &str = "diagnostic_net_edge_v3";
const HYPOTHETICAL_NET_EDGE_FLOOR_BPS: i64 = 5;
const GAS_PRICE_CACHE_TTL_US: u64 = 2_000_000;
const NATIVE_CONVERSION_CACHE_TTL_US: u64 = 30_000_000;
const HISTORICAL_SWAP_GAS_LIMIT: u64 = 250_000;

fn claim_auxiliary_book_sample(last: &AtomicU64, received_unix_us: u64) -> bool {
    let mut previous = last.load(Ordering::Relaxed);
    loop {
        if received_unix_us.saturating_sub(previous) < AUXILIARY_BOOK_SAMPLE_INTERVAL_US {
            return false;
        }
        match last.compare_exchange_weak(
            previous,
            received_unix_us,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return true,
            Err(current) => previous = current,
        }
    }
}

fn decision_boundary_unix_us(received_unix_us: u64, decision_latency_us: u128) -> u64 {
    received_unix_us.saturating_add(u64::try_from(decision_latency_us).unwrap_or(u64::MAX))
}

#[allow(clippy::too_many_arguments)]
fn pretrade_cost_payload(
    pair: &PairTelemetryContext,
    pool: &PoolTelemetryContext,
    direction: ArbitrageDirection,
    trade: TradeEvaluation,
    quote: &TopOfBook,
    snapshot: PreTradeCostSnapshot,
    cost_as_of_unix_us: u64,
    strategy_price_age_us: u64,
) -> anyhow::Result<serde_json::Value> {
    let binance_fee_bps = match direction {
        ArbitrageDirection::BuyTokenBOnDexSellOnCex => pair.binance_sell_fee_bps,
        ArbitrageDirection::BuyTokenBOnCexSellOnDex => pair.binance_buy_fee_bps,
    };
    let binance_commission = ceil_bps(trade.cex_token_a_amount, binance_fee_bps)?;

    let route = DexRouteCostKey {
        pool: pool.cost_pool_key,
        token_in: match direction {
            ArbitrageDirection::BuyTokenBOnDexSellOnCex => pair.token_a_address,
            ArbitrageDirection::BuyTokenBOnCexSellOnDex => pair.token_b_address,
        },
    };
    let gas_sample = snapshot.gas_price_at_or_before(cost_as_of_unix_us);
    let native_conversion = snapshot.native_conversion_at_or_before(cost_as_of_unix_us);
    let selected_receipt = snapshot.receipt_at_or_before(route, cost_as_of_unix_us);
    let receipt = selected_receipt.map(|selection| selection.sample);
    let gas_sample_age_us =
        gas_sample.map(|sample| cost_as_of_unix_us.saturating_sub(sample.captured_unix_us));
    let gas_sample_fresh = gas_sample_age_us.is_some_and(|age| age <= GAS_PRICE_CACHE_TTL_US);
    let native_conversion_sample_age_us =
        native_conversion.map(|sample| cost_as_of_unix_us.saturating_sub(sample.captured_unix_us));
    let native_conversion_fresh =
        native_conversion_sample_age_us.is_some_and(|age| age <= NATIVE_CONVERSION_CACHE_TTL_US);
    let gas_units = receipt
        .map(|sample| sample.gas_used)
        .unwrap_or(HISTORICAL_SWAP_GAS_LIMIT);
    let gas_units_source = match selected_receipt.map(|selection| selection.match_scope) {
        Some(crate::pretrade_cost::ReceiptCostMatchScope::ExactRoute) => {
            "last_exact_pool_and_input_token_receipt"
        }
        Some(crate::pretrade_cost::ReceiptCostMatchScope::SameProtocolBootstrap) => {
            "journal_same_protocol_bootstrap_fallback"
        }
        None => "historical_swap_gas_limit_fallback",
    };
    let includes_l1_fee = gas_sample.is_some_and(|sample| sample.includes_l1_fee);
    let l1_fee_available = !includes_l1_fee || receipt.is_some_and(|sample| sample.l1_fee_wei > 0);
    let l1_fee_wei = receipt.map_or(0, |sample| sample.l1_fee_wei);
    let gas_cost = match (
        gas_sample.filter(|_| gas_sample_fresh),
        native_conversion.filter(|_| native_conversion_fresh),
    ) {
        (Some(gas), Some(conversion)) if l1_fee_available => native_gas_to_token_a_base_units(
            gas_units,
            gas.maximum_fee_per_gas_wei,
            l1_fee_wei,
            conversion.price_token_a,
            pair.token_a_decimals,
        )
        .ok()
        .map(alloy_primitives::U256::from),
        _ => None,
    };
    let modeled_cost = gas_cost.and_then(|gas_cost| gas_cost.checked_add(binance_commission));
    let binance_commission_bps_x100 = unsigned_bps_x100(binance_commission, trade.cost_token_a)?;
    let gas_cost_bps_x100 = gas_cost
        .map(|cost| unsigned_bps_x100(cost, trade.cost_token_a))
        .transpose()?;
    let modeled_cost_bps_x100 = modeled_cost
        .map(|cost| unsigned_bps_x100(cost, trade.cost_token_a))
        .transpose()?;
    let gross_profit = signed_difference(trade.proceeds_token_a, trade.cost_token_a);
    let net_profit = modeled_cost.map(|modeled_cost| {
        signed_difference(
            trade.proceeds_token_a,
            trade.cost_token_a.saturating_add(modeled_cost),
        )
    });
    let net_profit_bps_x100 = net_profit
        .map(|profit| signed_bps_x100(profit, trade.cost_token_a))
        .transpose()?;
    let hypothetical_threshold_met =
        net_profit_bps_x100.map(|bps| bps >= HYPOTHETICAL_NET_EDGE_FLOOR_BPS * 100);

    let mut payload = json!({
        "model_version": PRETRADE_COST_MODEL_VERSION,
        "diagnostic_only": true,
        "decision_input": false,
        "cost_as_of_unix_us": cost_as_of_unix_us,
        "cost_as_of_source": "decision_complete_monotonic_projection",
        "opportunity_received_unix_us": quote.received_unix_us,
        "strategy_price_age_us": strategy_price_age_us,
        "fixed_threshold_met": trade.meets_threshold,
        "fixed_threshold_bps": pair.opportunity_threshold_bps,
        "hypothetical_net_edge_floor_bps": HYPOTHETICAL_NET_EDGE_FLOOR_BPS,
        "hypothetical_threshold_met": hypothetical_threshold_met,
        "hypothetical_new_capture": hypothetical_threshold_met
            .map(|met| met && !trade.meets_threshold),
        "gross_profit_token_a_base_units": signed_value(gross_profit),
        "gross_profit_bps_x100": trade.gross_profit_bps_x100,
        "dex_fee_model": "embedded_in_exact_clmm_curve_quote",
        "dex_pool_fee_pips": pool.fee_pips,
        "binance_commission_model": "conservative_taker_fee_bps_without_discount",
        "binance_commission_source": "authenticated_account_symbol_commission",
        "binance_side": match direction {
            ArbitrageDirection::BuyTokenBOnDexSellOnCex => "SELL",
            ArbitrageDirection::BuyTokenBOnCexSellOnDex => "BUY",
        },
        "binance_commission_bps": binance_fee_bps,
        "binance_commission_bps_x100": binance_commission_bps_x100,
        "binance_commission_token_a_base_units": binance_commission.to_string(),
    });
    payload
        .as_object_mut()
        .expect("pre-trade cost payload is an object")
        .extend(
            json!({
            "gas_model": "current_fee_cap_x_route_scoped_gas_used_plus_last_l1_fee",
            "gas_protocol": pool.cost_pool_key.protocol().label(),
            "gas_route_pool": pool.cost_pool_key.label(),
            "gas_route_token_in": format!("{:#x}", route.token_in),
            "gas_price_available_pretrade": gas_sample.is_some(),
            "gas_price_fresh": gas_sample_fresh,
            "gas_price_cache_ttl_us": GAS_PRICE_CACHE_TTL_US,
            "gas_price_history_depth": GAS_PRICE_HISTORY_DEPTH,
            "gas_price_sample_age_us": gas_sample_age_us,
            "gas_price_source": gas_sample.map(|sample| sample.source.label()),
            "gas_price_wei": gas_sample.map(|sample| sample.gas_price_wei.to_string()),
            "maximum_fee_per_gas_wei": gas_sample
                .map(|sample| sample.maximum_fee_per_gas_wei.to_string()),
            "native_conversion_available_pretrade": native_conversion.is_some(),
            "native_conversion_fresh": native_conversion_fresh,
            "native_conversion_cache_ttl_us": NATIVE_CONVERSION_CACHE_TTL_US,
            "native_conversion_history_depth": NATIVE_CONVERSION_HISTORY_DEPTH,
            "native_conversion_sample_age_us": native_conversion_sample_age_us,
            "native_conversion_price_token_a": native_conversion
                .map(|sample| sample.price_token_a.to_string()),
            "gas_units": gas_units,
            "gas_units_source": gas_units_source,
            "gas_units_observation_age_us": receipt.map(|sample| {
                cost_as_of_unix_us.saturating_sub(sample.captured_unix_us)
            }),
            "gas_units_event_age_us": receipt.and_then(|sample| {
                sample.source_event_unix_us.map(|event_unix_us| {
                    cost_as_of_unix_us.saturating_sub(event_unix_us)
                })
            }),
            "receipt_observed_unix_us": receipt.map(|sample| sample.captured_unix_us),
            "receipt_source_event_unix_us": receipt.and_then(|sample| sample.source_event_unix_us),
            "receipt_block_number": receipt.and_then(|sample| sample.block_number),
            "last_effective_gas_price_wei": receipt
                .map(|sample| sample.effective_gas_price_wei.to_string()),
            "receipt_cost_source": receipt.map(|sample| sample.source.label()),
            "receipt_match_scope": selected_receipt.map(|selection| selection.match_scope.label()),
            "receipt_history_depth": RECEIPT_HISTORY_DEPTH,
            "l1_fee_required": includes_l1_fee,
            "l1_fee_available": l1_fee_available,
            "l1_fee_wei": l1_fee_available.then(|| l1_fee_wei.to_string()),
            })
            .as_object()
            .expect("pre-trade gas payload is an object")
            .clone(),
        );
    payload
        .as_object_mut()
        .expect("pre-trade cost payload is an object")
        .extend(
            json!({
            "gas_cost_token_a_base_units": gas_cost.map(|value| value.to_string()),
            "gas_cost_bps_x100": gas_cost_bps_x100,
            "modeled_cost_token_a_base_units": modeled_cost.map(|value| value.to_string()),
            "modeled_cost_bps_x100": modeled_cost_bps_x100,
            "model_inputs_complete": modeled_cost.is_some(),
            "net_profit_token_a_base_units": net_profit.map(signed_value),
            "net_profit_bps_x100": net_profit_bps_x100,
            "excluded_from_model": [
                "binance_recovery",
                "inventory",
                "calldata_slippage_bound",
                "future_market_impact"
            ],
            })
            .as_object()
            .expect("pre-trade result payload is an object")
            .clone(),
        );
    Ok(payload)
}

#[derive(Clone, Copy)]
struct SignedU256 {
    negative: bool,
    magnitude: alloy_primitives::U256,
}

fn signed_difference(
    positive: alloy_primitives::U256,
    negative: alloy_primitives::U256,
) -> SignedU256 {
    if positive >= negative {
        SignedU256 {
            negative: false,
            magnitude: positive - negative,
        }
    } else {
        SignedU256 {
            negative: true,
            magnitude: negative - positive,
        }
    }
}

fn signed_value(value: SignedU256) -> String {
    if value.negative && !value.magnitude.is_zero() {
        format!("-{}", value.magnitude)
    } else {
        value.magnitude.to_string()
    }
}

fn signed_bps_x100(value: SignedU256, denominator: alloy_primitives::U256) -> anyhow::Result<i64> {
    anyhow::ensure!(!denominator.is_zero(), "pre-trade cost denominator is zero");
    let scaled = value
        .magnitude
        .checked_mul(alloy_primitives::U256::from(1_000_000_u64))
        .context("pre-trade net bps numerator overflow")?
        / denominator;
    let magnitude = i64::try_from(scaled).unwrap_or(i64::MAX);
    Ok(if value.negative {
        -magnitude
    } else {
        magnitude
    })
}

fn unsigned_bps_x100(
    value: alloy_primitives::U256,
    denominator: alloy_primitives::U256,
) -> anyhow::Result<u64> {
    anyhow::ensure!(!denominator.is_zero(), "pre-trade cost denominator is zero");
    let scaled = value
        .checked_mul(alloy_primitives::U256::from(1_000_000_u64))
        .context("pre-trade cost bps numerator overflow")?
        / denominator;
    Ok(u64::try_from(scaled).unwrap_or(u64::MAX))
}

fn ceil_bps(amount: alloy_primitives::U256, bps: u16) -> anyhow::Result<alloy_primitives::U256> {
    let numerator = amount
        .checked_mul(alloy_primitives::U256::from(bps))
        .and_then(|value| value.checked_add(alloy_primitives::U256::from(9_999_u64)))
        .context("pre-trade Binance commission overflow")?;
    Ok(numerator / alloy_primitives::U256::from(10_000_u64))
}

fn format_bps_x100(value: i64) -> String {
    let negative = value.is_negative();
    let magnitude = value.unsigned_abs();
    let sign = if negative { "-" } else { "" };
    format!("{sign}{}.{:02}", magnitude / 100, magnitude % 100)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use alloy_primitives::U256;

    use super::{
        HotDexEventTelemetry, HotSharedStreamTelemetry, ceil_bps, claim_auxiliary_book_sample,
        decision_boundary_unix_us, legacy_decision_projection, ownership_graph_decision_projection,
        signed_bps_x100, signed_difference,
    };
    use crate::opportunity::{
        ArbitrageDirection, DirectionEvaluation, PairEvaluation, PreparedPoolRefresh,
    };

    #[test]
    fn dex_hot_records_are_fixed_size_and_own_no_heap_allocation() {
        assert!(!std::mem::needs_drop::<HotDexEventTelemetry>());
        assert!(std::mem::size_of::<HotDexEventTelemetry>() <= 128);
        assert!(!std::mem::needs_drop::<PreparedPoolRefresh>());
        assert!(std::mem::size_of::<PreparedPoolRefresh>() <= 512);
        assert!(!std::mem::needs_drop::<HotSharedStreamTelemetry>());
        assert!(std::mem::size_of::<HotSharedStreamTelemetry>() <= 128);
    }

    #[test]
    fn diagnostic_cost_math_rounds_commission_up_and_keeps_signed_net_edge() {
        assert_eq!(
            ceil_bps(U256::from(10_001_u64), 10).unwrap(),
            U256::from(11)
        );
        let loss = signed_difference(U256::from(9_995_u64), U256::from(10_000_u64));
        assert_eq!(signed_bps_x100(loss, U256::from(10_000_u64)).unwrap(), -500);
    }

    #[test]
    fn decision_boundary_projects_receive_time_without_overflow() {
        assert_eq!(decision_boundary_unix_us(10_000, 275), 10_275);
        assert_eq!(decision_boundary_unix_us(u64::MAX - 1, 2), u64::MAX);
        assert_eq!(decision_boundary_unix_us(10_000, u128::MAX), u64::MAX);
    }

    #[test]
    fn auxiliary_book_sampling_claims_at_most_one_record_per_second() {
        let last = AtomicU64::new(0);
        assert!(claim_auxiliary_book_sample(&last, 10_000_000));
        assert!(!claim_auxiliary_book_sample(&last, 10_999_999));
        assert!(claim_auxiliary_book_sample(&last, 11_000_000));
        assert!(!claim_auxiliary_book_sample(&last, 10_500_000));
    }

    #[test]
    fn background_projection_preserves_the_complete_v12_baseline_decision() {
        let evaluation = PairEvaluation {
            pair_index: 0,
            baseline_token_b_amount: U256::from(200_u64),
            dex_buy_cex_sell: DirectionEvaluation {
                direction: ArbitrageDirection::BuyTokenBOnDexSellOnCex,
                cex_top_token_b_amount: U256::from(91_u64),
                baseline: None,
            },
            cex_buy_dex_sell: DirectionEvaluation {
                direction: ArbitrageDirection::BuyTokenBOnCexSellOnDex,
                cex_top_token_b_amount: U256::from(83_u64),
                baseline: None,
            },
            baseline_cache_hits: 2,
            baseline_cache_misses: 0,
        };

        assert_eq!(
            legacy_decision_projection(&evaluation),
            ownership_graph_decision_projection(&evaluation)
        );
    }
}

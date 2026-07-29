use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use anyhow::Context;
use serde_json::json;
use tokio::sync::mpsc;

use crate::{
    config::AppConfig,
    dex::mirror::DexMirror,
    opportunity::{
        DirectionEvaluation, PairEvaluation, PairRuntime, TradeEvaluation, format_base_units,
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
    dropped: Arc<AtomicU64>,
}

pub struct HotTelemetryTask {
    book_receiver: mpsc::Receiver<HotBookTelemetry>,
    evaluation_receiver: mpsc::Receiver<HotEvaluationTelemetry>,
    dropped: Arc<AtomicU64>,
    telemetry: TelemetryHandle,
    context: HotTelemetryContext,
}

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
    decision_latency_us: u128,
    trigger: &'static str,
}

struct HotTelemetryContext {
    engine_id: String,
    pairs: Vec<PairTelemetryContext>,
    pools: Vec<PoolTelemetryContext>,
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
    opportunity_threshold_bps: u16,
    min_slippage_bps: u16,
    max_slippage_bps: u16,
    slippage_profit_share_bps: u16,
    binance_buy_fee_bps: u16,
    binance_sell_fee_bps: u16,
}

struct PoolTelemetryContext {
    identity: String,
    pool_id: String,
    fee_pips: u32,
}

pub fn channel(
    config: &AppConfig,
    pairs: &[PairRuntime],
    dex: &DexMirror,
    telemetry: TelemetryHandle,
) -> anyhow::Result<(HotTelemetryHandle, HotTelemetryTask)> {
    let mut pools = Vec::with_capacity(dex.pool_count());
    for index in 0..dex.pool_count() {
        let pool = dex.pool(index)?;
        let pair = pairs
            .iter()
            .find(|pair| pair.pair_id == pool.pair_id)
            .context("hot telemetry pool pair is invalid")?;
        let identity = format!("{:?}", pool.identity);
        pools.push(PoolTelemetryContext {
            pool_id: pool_id(pair.chain_id, &identity),
            identity,
            fee_pips: pool.pool.fee_pips,
        });
    }
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
    };
    let (book_sender, book_receiver) = mpsc::channel(config.telemetry_channel_capacity);
    let (evaluation_sender, evaluation_receiver) = mpsc::channel(config.telemetry_channel_capacity);
    let dropped = Arc::new(AtomicU64::new(0));
    Ok((
        HotTelemetryHandle {
            book_sender,
            evaluation_sender,
            dropped: Arc::clone(&dropped),
        },
        HotTelemetryTask {
            book_receiver,
            evaluation_receiver,
            dropped,
            telemetry,
            context,
        },
    ))
}

impl HotTelemetryHandle {
    #[inline]
    pub fn emit_binance_book(
        &self,
        quote: &TopOfBook,
        feed_role: &'static str,
        runtime_phase: Option<RuntimePhase>,
        decision_outcome: &'static str,
    ) {
        if self
            .book_sender
            .try_send(HotBookTelemetry {
                quote: quote.clone(),
                decision_complete_us: quote.received_at.elapsed().as_micros(),
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
        trigger: &'static str,
    ) {
        if self
            .evaluation_sender
            .try_send(HotEvaluationTelemetry {
                quote: quote.clone(),
                evaluation,
                world_chain_block: chain_block,
                calculation_time_us,
                decision_latency_us: quote.received_at.elapsed().as_micros(),
                trigger,
            })
            .is_err()
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl HotTelemetryTask {
    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut books_open = true;
        let mut evaluations_open = true;
        while books_open || evaluations_open {
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
                    Some(event) => self.emit_evaluation(
                        &event.quote,
                        &event.evaluation,
                        event.world_chain_block,
                        event.calculation_time_us,
                        event.decision_latency_us,
                        event.trigger,
                    )?,
                    None => evaluations_open = false,
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

    fn emit_binance_book(
        &self,
        quote: &TopOfBook,
        decision_complete_us: u128,
        telemetry_queue_delay_us: u128,
        feed_role: &'static str,
        runtime_phase: Option<RuntimePhase>,
        decision_outcome: &'static str,
    ) {
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
                "exchange_timestamp_available": quote.exchange_event_ts_ms.is_some()
                    || quote.exchange_transaction_ts_ms.is_some(),
                "decision_complete_us": decision_complete_us,
                "engine_queue_age_us": decision_complete_us,
                "telemetry_queue_delay_us": telemetry_queue_delay_us,
            }),
        );
    }

    fn emit_evaluation(
        &self,
        quote: &TopOfBook,
        evaluation: &PairEvaluation,
        world_chain_block: u64,
        calculation_time_us: u128,
        decision_latency_us: u128,
        trigger: &'static str,
    ) -> anyhow::Result<()> {
        let pair = self
            .context
            .pairs
            .get(evaluation.pair_index)
            .context("hot telemetry pair index is invalid")?;
        let directions = [
            self.direction_payload(pair, &evaluation.dex_buy_cex_sell)?,
            self.direction_payload(pair, &evaluation.cex_buy_dex_sell)?,
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
                "decision_latency_us": decision_latency_us,
                "evaluation_trigger": trigger,
                "dependency_fanout_count": 1,
                "directions": directions,
            }),
        );

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
                        "decision_latency_us": decision_latency_us,
                        "evaluation_trigger": trigger,
                        "dependency_fanout_count": 1,
                        "baseline_pool_index": trade.pool_index,
                        "baseline_token_b_base_units": trade.token_b_amount.to_string(),
                        "baseline_gross_profit_bps_x100": trade.gross_profit_bps_x100,
                        "baseline": self.trade_payload(pair, trade)?,
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
                .map(|trade| self.trade_payload(pair, trade))
                .transpose()?,
        }))
    }

    fn trade_payload(
        &self,
        pair: &PairTelemetryContext,
        trade: TradeEvaluation,
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
        Ok(json!({
            "pool_index": trade.pool_index,
            "pool_id": pool.pool_id,
            "pool_identity": pool.identity,
            "pool_fee_pips": pool.fee_pips,
            "token_b_symbol": pair.token_b_symbol,
            "token_b_base_units": trade.token_b_amount.to_string(),
            "token_b_amount": format_base_units(trade.token_b_amount, pair.token_b_decimals),
            "token_a_symbol": pair.token_a_symbol,
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
        }))
    }
}

fn format_bps_x100(value: i64) -> String {
    let negative = value.is_negative();
    let magnitude = value.unsigned_abs();
    let sign = if negative { "-" } else { "" };
    format!("{sign}{}.{:02}", magnitude / 100, magnitude % 100)
}

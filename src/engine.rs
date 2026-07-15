use std::{sync::Arc, time::Instant};

use serde_json::json;

use crate::{
    config::AppConfig,
    dex::mirror::{DexMirror, LogApplyResult},
    domain::config::LoadedDomainConfig,
    market_data::{MarketEvent, alchemy::DexStreamEvent},
    opportunity::{
        CapacityEvaluation, DirectionEvaluation, OpportunityEngine, PairEvaluation, PairRuntime,
        TradeEvaluation, format_base_units,
    },
    state::{QuoteApplyResult, RuntimePhase, RuntimeState, TopOfBook},
    telemetry::TelemetryHandle,
};

pub struct TradingEngine {
    config: AppConfig,
    domain_config: Arc<LoadedDomainConfig>,
    state: RuntimeState,
    dex: DexMirror,
    opportunities: OpportunityEngine,
    telemetry: TelemetryHandle,
}

impl TradingEngine {
    pub fn new(
        config: AppConfig,
        domain_config: Arc<LoadedDomainConfig>,
        dex: DexMirror,
        telemetry: TelemetryHandle,
    ) -> anyhow::Result<Self> {
        let symbols = domain_config
            .binance_symbols()
            .into_iter()
            .map(Arc::<str>::from);
        let opportunities = OpportunityEngine::new(domain_config.snapshot(), &dex)?;
        Ok(Self {
            config,
            domain_config,
            state: RuntimeState::new(symbols),
            dex,
            opportunities,
            telemetry,
        })
    }

    pub fn start(&mut self) {
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
                "world_chain_block": self.dex.latest_head().number,
            }),
        );
    }

    pub fn on_dex_event(&mut self, event: DexStreamEvent) -> anyhow::Result<()> {
        match event {
            DexStreamEvent::Log { log, received_at } => {
                if let LogApplyResult::Applied { pool_index, kind } = self.dex.apply_log(&log)? {
                    self.opportunities.invalidate_pool(pool_index)?;
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
                        }),
                    );
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
            }
        }
        self.refresh_phase(Instant::now());
        Ok(())
    }

    pub fn on_market_event(&mut self, event: MarketEvent) -> anyhow::Result<()> {
        match event {
            MarketEvent::FeedConnected {
                symbol,
                generation,
                observed_at,
            } => {
                self.state.on_connected(&symbol, generation);
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
                self.on_binance_quote(quote)?;
            }
        }
        self.refresh_phase(Instant::now());
        Ok(())
    }

    fn on_binance_quote(&mut self, quote: TopOfBook) -> anyhow::Result<()> {
        let result = self.state.apply_quote(quote.clone());
        match result {
            QuoteApplyResult::Accepted => {
                self.telemetry.emit(
                    "binance_book_ticker",
                    json!({
                        "engine_id": self.config.engine_id,
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
                        "engine_queue_age_us": quote.received_at.elapsed().as_micros(),
                    }),
                );

                // The decision is evaluated only after all readiness inputs are
                // fresh. The calculation itself performs no RPC, I/O, or locks.
                self.refresh_phase(Instant::now());
                if self.state.phase == RuntimePhase::Ready {
                    let calculation_started = Instant::now();
                    if let Some(evaluation) = self.opportunities.evaluate(&quote, &self.dex)? {
                        let pair = self.opportunities.pair(evaluation.pair_index)?;
                        self.emit_arbitrage_evaluation(
                            &quote,
                            pair,
                            &evaluation,
                            calculation_started.elapsed().as_micros(),
                        )?;
                    }
                }
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

    fn emit_arbitrage_evaluation(
        &self,
        quote: &TopOfBook,
        pair: &PairRuntime,
        evaluation: &PairEvaluation,
        calculation_time_us: u128,
    ) -> anyhow::Result<()> {
        let decision_latency_us = quote.received_at.elapsed().as_micros();
        let directions = [
            self.direction_payload(pair, &evaluation.dex_buy_cex_sell)?,
            self.direction_payload(pair, &evaluation.cex_buy_dex_sell)?,
        ];
        self.telemetry.emit(
            "arbitrage_evaluation",
            json!({
                "engine_id": self.config.engine_id,
                "pair_id": pair.pair_id,
                "symbol": pair.symbol,
                "update_id": quote.update_id,
                "world_chain_block": self.dex.latest_head().number,
                "baseline_token_b_base_units": evaluation.baseline_token_b_amount.to_string(),
                "baseline_token_b": format_base_units(
                    evaluation.baseline_token_b_amount,
                    pair.token_b_decimals,
                ),
                "opportunity_threshold_bps": pair.opportunity_threshold_bps,
                "dex_fee_reserve_bps": pair.dex_fee_reserve_bps,
                "binance_book_product": "spot",
                "binance_execution_product": "spot",
                "capacity_model": "dex_liquidity_and_observed_spot_top_of_book",
                "includes_binance_fee": false,
                "includes_gas": false,
                "includes_inventory": false,
                "baseline_quote_cache_hits": evaluation.baseline_cache_hits,
                "baseline_quote_cache_misses": evaluation.baseline_cache_misses,
                "calculation_time_us": calculation_time_us,
                "decision_latency_us": decision_latency_us,
                "directions": directions,
            }),
        );

        for direction in [&evaluation.dex_buy_cex_sell, &evaluation.cex_buy_dex_sell] {
            if let Some(capacity) = direction.market_liquidity_capacity {
                self.telemetry.emit(
                    "arbitrage_opportunity",
                    json!({
                        "engine_id": self.config.engine_id,
                        "pair_id": pair.pair_id,
                        "symbol": pair.symbol,
                        "update_id": quote.update_id,
                        "world_chain_block": self.dex.latest_head().number,
                        "direction": direction.direction.as_str(),
                        "opportunity_threshold_bps": pair.opportunity_threshold_bps,
                        "dex_fee_reserve_bps": pair.dex_fee_reserve_bps,
                        "capacity_model": "dex_liquidity_and_observed_spot_top_of_book",
                        "execution_ready": false,
                        "execution_gaps": [
                            "binance_depth_beyond_top_not_observed",
                            "binance_fee_not_applied",
                            "gas_not_applied",
                            "inventory_not_hydrated",
                        ],
                        "calculation_time_us": calculation_time_us,
                        "decision_latency_us": quote.received_at.elapsed().as_micros(),
                        "market_liquidity_capacity": self.capacity_payload(pair, capacity)?,
                    }),
                );
            }
        }
        Ok(())
    }

    fn direction_payload(
        &self,
        pair: &PairRuntime,
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
            "market_liquidity_capacity": direction
                .market_liquidity_capacity
                .map(|capacity| self.capacity_payload(pair, capacity))
                .transpose()?,
        }))
    }

    fn capacity_payload(
        &self,
        pair: &PairRuntime,
        capacity: CapacityEvaluation,
    ) -> anyhow::Result<serde_json::Value> {
        Ok(json!({
            "limiter": capacity.limiter.as_str(),
            "trade": self.trade_payload(pair, capacity.trade)?,
        }))
    }

    fn trade_payload(
        &self,
        pair: &PairRuntime,
        trade: TradeEvaluation,
    ) -> anyhow::Result<serde_json::Value> {
        let pool = self.dex.pool(trade.pool_index)?;
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
            "pool_identity": format!("{:?}", pool.identity),
            "pool_fee_pips": pool.pool.fee_pips,
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
            "profit_token_a_base_units": trade.signed_profit_token_a(),
            "profit_token_a": profit,
            "profit_bps_x100": trade.profit_bps_x100,
            "profit_bps": format_bps_x100(trade.profit_bps_x100),
            "meets_threshold": trade.meets_threshold,
        }))
    }

    pub fn refresh_health(&mut self) {
        self.refresh_phase(Instant::now());
    }

    fn refresh_phase(&mut self, now: Instant) {
        let previous = self.state.phase;
        let dex_ready = self.dex.is_fresh(now, self.config.dex_head_max_age_ms);
        let current = self
            .state
            .refresh_phase(now, self.config.market_data_max_age_ms, dex_ready);
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

fn format_bps_x100(value: i64) -> String {
    let negative = value.is_negative();
    let magnitude = value.unsigned_abs();
    let sign = if negative { "-" } else { "" };
    format!("{sign}{}.{:02}", magnitude / 100, magnitude % 100)
}

use std::{sync::Arc, time::Instant};

use serde_json::json;

use crate::{
    config::AppConfig,
    dex::mirror::{DexMirror, LogApplyResult},
    domain::config::LoadedDomainConfig,
    hot_telemetry::{HotTelemetryHandle, HotTelemetryTask, channel as hot_telemetry_channel},
    market_data::{MarketEvent, alchemy::DexStreamEvent},
    opportunity::{OpportunityEngine, PreparedPoolBuildRequest, PreparedPoolBuildResult},
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
    hot_telemetry: HotTelemetryHandle,
}

impl TradingEngine {
    pub fn new(
        config: AppConfig,
        domain_config: Arc<LoadedDomainConfig>,
        dex: DexMirror,
        telemetry: TelemetryHandle,
    ) -> anyhow::Result<(Self, HotTelemetryTask)> {
        let symbols = domain_config
            .binance_symbols()
            .into_iter()
            .map(Arc::<str>::from);
        let opportunities = OpportunityEngine::new(domain_config.snapshot(), &dex)?;
        let (hot_telemetry, hot_telemetry_task) =
            hot_telemetry_channel(&config, opportunities.pairs(), &dex, telemetry.clone())?;
        Ok((
            Self {
                config,
                domain_config,
                state: RuntimeState::new(symbols),
                dex,
                opportunities,
                telemetry,
                hot_telemetry,
            },
            hot_telemetry_task,
        ))
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
        self.telemetry.emit(
            "dex_pool_prepared",
            json!({
                "engine_id": self.config.engine_id,
                "pair_id": pool.pair_id,
                "identity": format!("{:?}", pool.identity),
                "pool_index": prepared.pool_index,
                "prepared_generation": prepared.generation,
                "prepared_exact_output_segments": prepared.exact_output_segments,
                "prepared_exact_input_segments": prepared.exact_input_segments,
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
                self.evaluate_ready_quote(&quote, "dex_prepared")?;
            }
        }
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
                // The decision is evaluated only after all readiness inputs are
                // fresh. The calculation itself performs no RPC, I/O, or locks.
                self.refresh_phase(Instant::now());
                if self.state.phase == RuntimePhase::Ready {
                    self.evaluate_ready_quote(&quote, "binance_book_ticker")?;
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

    fn evaluate_ready_quote(
        &mut self,
        quote: &TopOfBook,
        trigger: &'static str,
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
        }
        Ok(())
    }

    pub fn refresh_health(&mut self) {
        self.refresh_phase(Instant::now());
    }

    fn refresh_phase(&mut self, now: Instant) {
        let previous = self.state.phase;
        let dex_ready = self.dex.is_fresh(now, self.config.dex_head_max_age_ms)
            && self.opportunities.is_ready();
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

use std::{sync::Arc, time::Instant};

use serde_json::json;

use crate::{
    config::AppConfig,
    dex::mirror::{DexMirror, LogApplyResult},
    domain::config::LoadedDomainConfig,
    market_data::{MarketEvent, alchemy::DexStreamEvent},
    state::{QuoteApplyResult, RuntimePhase, RuntimeState, TopOfBook},
    telemetry::TelemetryHandle,
};

pub struct TradingEngine {
    config: AppConfig,
    domain_config: Arc<LoadedDomainConfig>,
    state: RuntimeState,
    dex: DexMirror,
    telemetry: TelemetryHandle,
}

impl TradingEngine {
    pub fn new(
        config: AppConfig,
        domain_config: Arc<LoadedDomainConfig>,
        dex: DexMirror,
        telemetry: TelemetryHandle,
    ) -> Self {
        let symbols = domain_config
            .binance_symbols()
            .into_iter()
            .map(Arc::<str>::from);
        Self {
            config,
            domain_config,
            state: RuntimeState::new(symbols),
            dex,
            telemetry,
        }
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

    pub fn on_market_event(&mut self, event: MarketEvent) {
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
                        "symbol": symbol.as_ref(),
                        "generation": generation,
                        "reason": reason,
                        "observed_mono_age_us": observed_at.elapsed().as_micros(),
                    }),
                );
            }
            MarketEvent::BinanceTopOfBook(quote) => {
                self.on_binance_quote(quote);
            }
        }
        self.refresh_phase(Instant::now());
    }

    fn on_binance_quote(&mut self, quote: TopOfBook) {
        let result = self.state.apply_quote(quote.clone());
        match result {
            QuoteApplyResult::Accepted => self.telemetry.emit(
                "binance_book_ticker",
                json!({
                    "engine_id": self.config.engine_id,
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
            ),
            rejected => self.telemetry.emit(
                "binance_book_ticker_rejected",
                json!({
                    "engine_id": self.config.engine_id,
                    "symbol": quote.symbol.as_ref(),
                    "update_id": quote.update_id,
                    "connection_generation": quote.connection_generation,
                    "reason": format!("{rejected:?}"),
                }),
            ),
        }
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

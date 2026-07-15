use std::{sync::Arc, time::Instant};

use serde_json::json;

use crate::{
    config::AppConfig,
    market_data::MarketEvent,
    state::{QuoteApplyResult, RuntimePhase, RuntimeState, TopOfBook},
    telemetry::TelemetryHandle,
};

pub struct TradingEngine {
    config: AppConfig,
    state: RuntimeState,
    telemetry: TelemetryHandle,
}

impl TradingEngine {
    pub fn new(config: AppConfig, telemetry: TelemetryHandle) -> Self {
        let symbols = config
            .normalized_binance_symbols()
            .into_iter()
            .map(Arc::<str>::from);
        Self {
            config,
            state: RuntimeState::new(symbols),
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
                "binance_symbols": self.config.normalized_binance_symbols(),
            }),
        );
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
        let current = self
            .state
            .refresh_phase(now, self.config.market_data_max_age_ms);
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

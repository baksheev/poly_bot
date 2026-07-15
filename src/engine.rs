use serde_json::json;

use crate::{
    config::AppConfig,
    state::{PolymarketBook, RuntimePhase, RuntimeState, TopOfBook},
    telemetry::TelemetryHandle,
};

/// Owns all latency-sensitive state. Connectors will feed this object from the
/// same event loop; ClickHouse is deliberately kept behind `TelemetryHandle`.
pub struct TradingEngine {
    config: AppConfig,
    state: RuntimeState,
    telemetry: TelemetryHandle,
}

impl TradingEngine {
    pub fn new(config: AppConfig, telemetry: TelemetryHandle) -> Self {
        Self {
            config,
            state: RuntimeState::default(),
            telemetry,
        }
    }

    pub fn mark_ready(&mut self) {
        self.state.phase = RuntimePhase::Ready;
        self.telemetry.emit(
            "runtime_ready",
            json!({
                "service": self.config.service_name,
                "gcp_project_id": self.config.gcp_project_id,
                "gcp_region": self.config.gcp_region,
            }),
        );
    }

    #[allow(dead_code)]
    pub fn on_binance_quote(&mut self, quote: TopOfBook) {
        self.state.update_binance(quote);
        self.telemetry.emit(
            "binance_quote",
            serde_json::to_value(quote).unwrap_or_default(),
        );
    }

    #[allow(dead_code)]
    pub fn on_polymarket_book(&mut self, book: PolymarketBook) {
        self.state.update_polymarket(book.clone());
        self.telemetry.emit(
            "polymarket_book",
            serde_json::to_value(book).unwrap_or_default(),
        );
    }

    pub fn shutdown(&mut self) {
        self.state.phase = RuntimePhase::Stopping;
        self.telemetry.emit(
            "runtime_stopping",
            json!({"processed_events": self.state.processed_events}),
        );
    }
}

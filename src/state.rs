use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct TopOfBook {
    pub bid: f64,
    pub ask: f64,
    pub exchange_ts_ms: u64,
    pub received_ts_ms: u64,
}

impl TopOfBook {
    pub fn new(
        bid: f64,
        ask: f64,
        exchange_ts_ms: u64,
        received_ts_ms: u64,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(bid.is_finite() && bid > 0.0, "bid must be positive");
        anyhow::ensure!(ask.is_finite() && ask > 0.0, "ask must be positive");
        anyhow::ensure!(bid <= ask, "bid must not exceed ask");
        Ok(Self {
            bid,
            ask,
            exchange_ts_ms,
            received_ts_ms,
        })
    }

    pub fn mid(self) -> f64 {
        (self.bid + self.ask) / 2.0
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PolymarketBook {
    pub market_slug: String,
    pub up: TopOfBook,
    pub down: TopOfBook,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimePhase {
    Starting,
    Ready,
    Stopping,
}

#[derive(Debug, Serialize)]
pub struct RuntimeState {
    pub phase: RuntimePhase,
    pub binance_btc_usdt: Option<TopOfBook>,
    pub polymarket: Option<PolymarketBook>,
    pub processed_events: u64,
}

impl Default for RuntimeState {
    fn default() -> Self {
        Self {
            phase: RuntimePhase::Starting,
            binance_btc_usdt: None,
            polymarket: None,
            processed_events: 0,
        }
    }
}

impl RuntimeState {
    pub fn update_binance(&mut self, quote: TopOfBook) {
        self.binance_btc_usdt = Some(quote);
        self.processed_events += 1;
    }

    pub fn update_polymarket(&mut self, book: PolymarketBook) {
        self.polymarket = Some(book);
        self.processed_events += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::{RuntimeState, TopOfBook};

    #[test]
    fn quote_rejects_crossed_book() {
        assert!(TopOfBook::new(101.0, 100.0, 1, 2).is_err());
    }

    #[test]
    fn state_updates_without_external_io() {
        let mut state = RuntimeState::default();
        state.update_binance(TopOfBook::new(100.0, 102.0, 1, 2).unwrap());

        assert_eq!(state.processed_events, 1);
        assert_eq!(state.binance_btc_usdt.unwrap().mid(), 101.0);
    }
}

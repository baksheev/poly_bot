use std::{collections::HashMap, sync::Arc, time::Instant};

use anyhow::ensure;
use rust_decimal::Decimal;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq)]
pub struct TopOfBook {
    pub symbol: Arc<str>,
    pub update_id: u64,
    pub bid_price: Decimal,
    pub bid_quantity: Decimal,
    pub ask_price: Decimal,
    pub ask_quantity: Decimal,
    pub exchange_event_ts_ms: Option<u64>,
    pub exchange_transaction_ts_ms: Option<u64>,
    pub received_at: Instant,
    pub received_unix_us: u64,
    pub connection_generation: u64,
}

impl TopOfBook {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        symbol: Arc<str>,
        update_id: u64,
        bid_price: Decimal,
        bid_quantity: Decimal,
        ask_price: Decimal,
        ask_quantity: Decimal,
        exchange_event_ts_ms: Option<u64>,
        exchange_transaction_ts_ms: Option<u64>,
        received_at: Instant,
        received_unix_us: u64,
        connection_generation: u64,
    ) -> anyhow::Result<Self> {
        ensure!(bid_price > Decimal::ZERO, "bid price must be positive");
        ensure!(ask_price > Decimal::ZERO, "ask price must be positive");
        ensure!(
            bid_quantity > Decimal::ZERO,
            "bid quantity must be positive"
        );
        ensure!(
            ask_quantity > Decimal::ZERO,
            "ask quantity must be positive"
        );
        ensure!(
            bid_price <= ask_price,
            "bid price must not exceed ask price"
        );

        Ok(Self {
            symbol,
            update_id,
            bid_price,
            bid_quantity,
            ask_price,
            ask_quantity,
            exchange_event_ts_ms,
            exchange_transaction_ts_ms,
            received_at,
            received_unix_us,
            connection_generation,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimePhase {
    Starting,
    Ready,
    Degraded,
    Stopping,
}

#[derive(Debug, Default)]
pub struct BinanceFeedState {
    pub connected: bool,
    pub connection_generation: u64,
    pub last_update_id: Option<u64>,
    pub book: Option<TopOfBook>,
    pub accepted_updates: u64,
    pub rejected_updates: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteApplyResult {
    Accepted,
    UnknownSymbol,
    StaleGeneration,
    DuplicateOrRegressed,
}

#[derive(Debug)]
pub struct RuntimeState {
    pub phase: RuntimePhase,
    pub binance_feeds: HashMap<Arc<str>, BinanceFeedState>,
    pub processed_events: u64,
    ever_ready: bool,
}

impl RuntimeState {
    pub fn new(symbols: impl IntoIterator<Item = Arc<str>>) -> Self {
        Self {
            phase: RuntimePhase::Starting,
            binance_feeds: symbols
                .into_iter()
                .map(|symbol| (symbol, BinanceFeedState::default()))
                .collect(),
            processed_events: 0,
            ever_ready: false,
        }
    }

    pub fn on_connected(&mut self, symbol: &str, generation: u64) {
        let Some(feed) = self.binance_feeds.get_mut(symbol) else {
            return;
        };
        if generation < feed.connection_generation {
            return;
        }

        feed.connected = true;
        feed.connection_generation = generation;
        feed.last_update_id = None;
        feed.book = None;
        self.processed_events += 1;
    }

    pub fn on_disconnected(&mut self, symbol: &str, generation: u64) {
        let Some(feed) = self.binance_feeds.get_mut(symbol) else {
            return;
        };
        if generation != feed.connection_generation {
            return;
        }

        feed.connected = false;
        feed.book = None;
        self.processed_events += 1;
    }

    pub fn apply_quote(&mut self, quote: TopOfBook) -> QuoteApplyResult {
        let Some(feed) = self.binance_feeds.get_mut(quote.symbol.as_ref()) else {
            return QuoteApplyResult::UnknownSymbol;
        };
        if quote.connection_generation != feed.connection_generation {
            feed.rejected_updates += 1;
            return QuoteApplyResult::StaleGeneration;
        }
        if feed
            .last_update_id
            .is_some_and(|last_update_id| quote.update_id <= last_update_id)
        {
            feed.rejected_updates += 1;
            return QuoteApplyResult::DuplicateOrRegressed;
        }

        feed.last_update_id = Some(quote.update_id);
        feed.book = Some(quote);
        feed.accepted_updates += 1;
        self.processed_events += 1;
        QuoteApplyResult::Accepted
    }

    pub fn refresh_phase(&mut self, now: Instant, max_age_ms: u64) -> RuntimePhase {
        if self.phase == RuntimePhase::Stopping {
            return self.phase;
        }

        let ready = !self.binance_feeds.is_empty()
            && self.binance_feeds.values().all(|feed| {
                feed.connected
                    && feed.book.as_ref().is_some_and(|book| {
                        now.saturating_duration_since(book.received_at).as_millis()
                            <= u128::from(max_age_ms)
                    })
            });

        self.phase = if ready {
            self.ever_ready = true;
            RuntimePhase::Ready
        } else if self.ever_ready {
            RuntimePhase::Degraded
        } else {
            RuntimePhase::Starting
        };
        self.phase
    }

    pub fn stop(&mut self) {
        self.phase = RuntimePhase::Stopping;
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc, time::Duration};

    use rust_decimal::Decimal;

    use super::{QuoteApplyResult, RuntimePhase, RuntimeState, TopOfBook};

    fn quote(update_id: u64, generation: u64, received_at: std::time::Instant) -> TopOfBook {
        TopOfBook::new(
            Arc::from("WLDUSDC"),
            update_id,
            Decimal::from_str("0.8123").unwrap(),
            Decimal::from_str("100").unwrap(),
            Decimal::from_str("0.8125").unwrap(),
            Decimal::from_str("200").unwrap(),
            Some(10),
            Some(9),
            received_at,
            11,
            generation,
        )
        .unwrap()
    }

    #[test]
    fn quote_rejects_crossed_book() {
        let now = std::time::Instant::now();
        assert!(
            TopOfBook::new(
                Arc::from("WLDUSDC"),
                1,
                Decimal::from(101),
                Decimal::ONE,
                Decimal::from(100),
                Decimal::ONE,
                None,
                None,
                now,
                1,
                1,
            )
            .is_err()
        );
    }

    #[test]
    fn state_requires_connected_fresh_quote_before_ready() {
        let now = std::time::Instant::now();
        let mut state = RuntimeState::new([Arc::from("WLDUSDC")]);

        state.on_connected("WLDUSDC", 1);
        assert_eq!(state.refresh_phase(now, 5_000), RuntimePhase::Starting);
        assert_eq!(
            state.apply_quote(quote(10, 1, now)),
            QuoteApplyResult::Accepted
        );
        assert_eq!(state.refresh_phase(now, 5_000), RuntimePhase::Ready);
        assert_eq!(
            state.refresh_phase(now + Duration::from_secs(6), 5_000),
            RuntimePhase::Degraded
        );
    }

    #[test]
    fn reconnect_invalidates_old_generation_and_quote() {
        let now = std::time::Instant::now();
        let mut state = RuntimeState::new([Arc::from("WLDUSDC")]);
        state.on_connected("WLDUSDC", 1);
        assert_eq!(
            state.apply_quote(quote(10, 1, now)),
            QuoteApplyResult::Accepted
        );

        state.on_connected("WLDUSDC", 2);
        assert_eq!(
            state.apply_quote(quote(11, 1, now)),
            QuoteApplyResult::StaleGeneration
        );
        assert_eq!(
            state.apply_quote(quote(1, 2, now)),
            QuoteApplyResult::Accepted
        );
    }

    #[test]
    fn duplicate_or_regressed_update_is_rejected() {
        let now = std::time::Instant::now();
        let mut state = RuntimeState::new([Arc::from("WLDUSDC")]);
        state.on_connected("WLDUSDC", 1);
        assert_eq!(
            state.apply_quote(quote(10, 1, now)),
            QuoteApplyResult::Accepted
        );
        assert_eq!(
            state.apply_quote(quote(10, 1, now)),
            QuoteApplyResult::DuplicateOrRegressed
        );
    }
}

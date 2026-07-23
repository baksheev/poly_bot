use std::{collections::HashMap, sync::Arc, time::Instant};

use anyhow::ensure;
use rust_decimal::Decimal;
use serde::Serialize;

use crate::balances::{BalanceSource, BinanceBalanceSnapshot, WalletBalanceSnapshot};

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
    pub wire_frame_size_bytes: usize,
    pub parse_time_us: u128,
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
            wire_frame_size_bytes: 0,
            parse_time_us: 0,
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
    pub depth_update_id: Option<u64>,
    pub depth_received_at: Option<Instant>,
    pub last_transport_activity_at: Option<Instant>,
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
    pub balances: BalanceState,
    ever_ready: bool,
    require_binance_depth: bool,
}

#[derive(Debug, Default)]
pub struct BalanceState {
    pub binance: Option<BinanceBalanceSnapshot>,
    pub wallet: Option<WalletBalanceSnapshot>,
    pub binance_failures: u64,
    pub wallet_failures: u64,
}

impl BalanceState {
    pub fn apply_binance(&mut self, snapshot: BinanceBalanceSnapshot) {
        self.binance = Some(snapshot);
    }

    pub fn apply_wallet(&mut self, snapshot: WalletBalanceSnapshot) {
        if self
            .wallet
            .as_ref()
            .is_some_and(|current| current.block_number > snapshot.block_number)
        {
            return;
        }
        self.wallet = Some(snapshot);
    }

    pub fn record_failure(&mut self, source: BalanceSource) {
        match source {
            BalanceSource::Binance => self.binance_failures += 1,
            BalanceSource::Wallet => self.wallet_failures += 1,
        }
    }

    pub fn is_fresh(&self, now: Instant, max_age_ms: u64) -> bool {
        self.binance.as_ref().is_some_and(|snapshot| {
            snapshot.healthy()
                && now
                    .saturating_duration_since(snapshot.observed_at)
                    .as_millis()
                    <= u128::from(max_age_ms)
        }) && self.wallet.as_ref().is_some_and(|snapshot| {
            now.saturating_duration_since(snapshot.observed_at)
                .as_millis()
                <= u128::from(max_age_ms)
        })
    }
}

impl RuntimeState {
    pub fn new(symbols: impl IntoIterator<Item = Arc<str>>) -> Self {
        Self::with_depth_requirement(symbols, false)
    }

    pub fn new_with_depth(symbols: impl IntoIterator<Item = Arc<str>>) -> Self {
        Self::with_depth_requirement(symbols, true)
    }

    fn with_depth_requirement(
        symbols: impl IntoIterator<Item = Arc<str>>,
        require_binance_depth: bool,
    ) -> Self {
        Self {
            phase: RuntimePhase::Starting,
            binance_feeds: symbols
                .into_iter()
                .map(|symbol| (symbol, BinanceFeedState::default()))
                .collect(),
            processed_events: 0,
            balances: BalanceState::default(),
            ever_ready: false,
            require_binance_depth,
        }
    }

    pub fn on_connected(&mut self, symbol: &str, generation: u64, observed_at: Instant) {
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
        feed.depth_update_id = None;
        feed.depth_received_at = None;
        feed.last_transport_activity_at = Some(observed_at);
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
        feed.depth_update_id = None;
        feed.depth_received_at = None;
        feed.last_transport_activity_at = None;
        self.processed_events += 1;
    }

    pub fn record_transport_activity(
        &mut self,
        symbol: &str,
        generation: u64,
        observed_at: Instant,
    ) -> bool {
        let Some(feed) = self.binance_feeds.get_mut(symbol) else {
            return false;
        };
        if !feed.connected || generation != feed.connection_generation {
            return false;
        }
        feed.last_transport_activity_at = Some(observed_at);
        self.processed_events += 1;
        true
    }

    pub fn apply_quote(&mut self, quote: TopOfBook) -> QuoteApplyResult {
        let Some(feed) = self.binance_feeds.get_mut(quote.symbol.as_ref()) else {
            return QuoteApplyResult::UnknownSymbol;
        };
        if quote.connection_generation != feed.connection_generation {
            feed.rejected_updates += 1;
            return QuoteApplyResult::StaleGeneration;
        }
        feed.last_transport_activity_at = Some(quote.received_at);
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

    pub fn apply_depth(
        &mut self,
        symbol: &str,
        generation: u64,
        update_id: u64,
        received_at: Instant,
    ) -> QuoteApplyResult {
        let Some(feed) = self.binance_feeds.get_mut(symbol) else {
            return QuoteApplyResult::UnknownSymbol;
        };
        if generation != feed.connection_generation {
            feed.rejected_updates += 1;
            return QuoteApplyResult::StaleGeneration;
        }
        feed.last_transport_activity_at = Some(received_at);
        if feed
            .depth_update_id
            .is_some_and(|last_update_id| update_id <= last_update_id)
        {
            feed.rejected_updates += 1;
            return QuoteApplyResult::DuplicateOrRegressed;
        }
        feed.depth_update_id = Some(update_id);
        feed.depth_received_at = Some(received_at);
        self.processed_events += 1;
        QuoteApplyResult::Accepted
    }

    pub fn refresh_phase(
        &mut self,
        now: Instant,
        max_age_ms: u64,
        external_ready: bool,
    ) -> RuntimePhase {
        self.refresh_phase_from_inputs(self.binance_ready(now, max_age_ms), external_ready)
    }

    pub fn refresh_phase_from_inputs(
        &mut self,
        binance_ready: bool,
        external_ready: bool,
    ) -> RuntimePhase {
        if self.phase == RuntimePhase::Stopping {
            return self.phase;
        }

        let ready = external_ready && binance_ready;

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

    pub fn binance_ready(&self, now: Instant, max_age_ms: u64) -> bool {
        !self.binance_feeds.is_empty()
            && self.binance_feeds.values().all(|feed| {
                Self::feed_price_ready(feed, now, max_age_ms)
                    && (!self.require_binance_depth
                        || feed.depth_received_at.is_some_and(|received_at| {
                            now.saturating_duration_since(received_at).as_millis()
                                <= u128::from(max_age_ms)
                        }))
            })
    }

    pub fn binance_symbol_price_ready(
        &self,
        symbol: &str,
        now: Instant,
        max_transport_silence_ms: u64,
    ) -> bool {
        self.binance_feeds
            .get(symbol)
            .is_some_and(|feed| Self::feed_price_ready(feed, now, max_transport_silence_ms))
    }

    fn feed_price_ready(feed: &BinanceFeedState, now: Instant, max_age_ms: u64) -> bool {
        feed.connected
            && feed.book.is_some()
            && feed
                .last_transport_activity_at
                .is_some_and(|last_activity_at| {
                    now.saturating_duration_since(last_activity_at).as_millis()
                        <= u128::from(max_age_ms)
                })
    }

    pub fn stop(&mut self) {
        self.phase = RuntimePhase::Stopping;
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, str::FromStr, sync::Arc, time::Duration};

    use alloy_primitives::{Address, B256, U256};
    use rust_decimal::Decimal;

    use crate::{
        balances::{BalanceSource, BinanceBalanceSnapshot, WalletBalanceSnapshot},
        chain::rpc::RpcStats,
    };

    use super::{BalanceState, QuoteApplyResult, RuntimePhase, RuntimeState, TopOfBook};

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

    fn binance_balances(observed_at: std::time::Instant) -> BinanceBalanceSnapshot {
        BinanceBalanceSnapshot {
            account_update_time_ms: 1,
            account_type: "SPOT".to_owned(),
            can_trade: true,
            balances: BTreeMap::new(),
            observed_at,
            request_duration_us: 1,
        }
    }

    fn wallet_balances(
        block_number: u64,
        observed_at: std::time::Instant,
    ) -> WalletBalanceSnapshot {
        WalletBalanceSnapshot {
            owner: Address::ZERO,
            chain_id: 480,
            block_number,
            block_hash: B256::ZERO,
            native_balance_wei: U256::ZERO,
            gas_price_wei: 1,
            token_balances: Vec::new(),
            observed_at,
            request_duration_us: 1,
            rpc_stats: RpcStats {
                http_requests: 1,
                eth_calls: 0,
                rate_limit_retries: 0,
            },
        }
    }

    #[test]
    fn balance_state_requires_both_fresh_healthy_sources() {
        let now = std::time::Instant::now();
        let mut balances = BalanceState::default();
        assert!(!balances.is_fresh(now, 5_000));

        balances.apply_binance(binance_balances(now));
        assert!(!balances.is_fresh(now, 5_000));

        balances.apply_wallet(wallet_balances(10, now));
        assert!(balances.is_fresh(now + Duration::from_secs(5), 5_000));
        assert!(!balances.is_fresh(now + Duration::from_millis(5_001), 5_000));

        balances.record_failure(BalanceSource::Binance);
        assert_eq!(balances.binance_failures, 1);
        assert!(balances.binance.is_some());
    }

    #[test]
    fn balance_state_ignores_regressed_wallet_snapshots() {
        let now = std::time::Instant::now();
        let mut balances = BalanceState::default();
        balances.apply_wallet(wallet_balances(10, now));
        balances.apply_wallet(wallet_balances(9, now + Duration::from_secs(1)));
        assert_eq!(balances.wallet.unwrap().block_number, 10);
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
    fn state_requires_a_price_and_fresh_transport_before_ready() {
        let now = std::time::Instant::now();
        let mut state = RuntimeState::new([Arc::from("WLDUSDC")]);

        state.on_connected("WLDUSDC", 1, now);
        assert_eq!(
            state.refresh_phase(now, 5_000, true),
            RuntimePhase::Starting
        );
        assert_eq!(
            state.apply_quote(quote(10, 1, now)),
            QuoteApplyResult::Accepted
        );
        assert_eq!(state.refresh_phase(now, 5_000, true), RuntimePhase::Ready);
        assert!(state.record_transport_activity("WLDUSDC", 1, now + Duration::from_secs(4)));
        assert_eq!(
            state.refresh_phase(now + Duration::from_secs(6), 5_000, true),
            RuntimePhase::Ready
        );
        assert_eq!(
            state.refresh_phase(now + Duration::from_millis(9_001), 5_000, true),
            RuntimePhase::Degraded
        );
    }

    #[test]
    fn depth_required_state_waits_for_fresh_sequence_consistent_depth() {
        let now = std::time::Instant::now();
        let mut state = RuntimeState::new_with_depth([Arc::from("WLDUSDC")]);
        state.on_connected("WLDUSDC", 3, now);
        assert_eq!(
            state.apply_quote(quote(10, 3, now)),
            QuoteApplyResult::Accepted
        );
        assert_eq!(
            state.refresh_phase(now, 5_000, true),
            RuntimePhase::Starting
        );
        assert_eq!(
            state.apply_depth("WLDUSDC", 3, 20, now),
            QuoteApplyResult::Accepted
        );
        assert_eq!(state.refresh_phase(now, 5_000, true), RuntimePhase::Ready);
        assert_eq!(
            state.apply_depth("WLDUSDC", 3, 20, now),
            QuoteApplyResult::DuplicateOrRegressed
        );
        assert_eq!(
            state.refresh_phase(now + Duration::from_millis(5_001), 5_000, true),
            RuntimePhase::Degraded
        );
    }

    #[test]
    fn reconnect_invalidates_old_generation_and_quote() {
        let now = std::time::Instant::now();
        let mut state = RuntimeState::new([Arc::from("WLDUSDC")]);
        state.on_connected("WLDUSDC", 1, now);
        assert_eq!(
            state.apply_quote(quote(10, 1, now)),
            QuoteApplyResult::Accepted
        );
        assert_eq!(state.refresh_phase(now, 5_000, true), RuntimePhase::Ready);

        state.on_connected("WLDUSDC", 2, now);
        assert_eq!(
            state.refresh_phase(now, 5_000, true),
            RuntimePhase::Degraded
        );
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
        state.on_connected("WLDUSDC", 1, now);
        assert_eq!(
            state.apply_quote(quote(10, 1, now)),
            QuoteApplyResult::Accepted
        );
        assert_eq!(
            state.apply_quote(quote(10, 1, now)),
            QuoteApplyResult::DuplicateOrRegressed
        );
    }

    #[test]
    fn transport_freshness_boundary_is_inclusive_and_then_degrades() {
        let now = std::time::Instant::now();
        let mut state = RuntimeState::new([Arc::from("WLDUSDC")]);
        state.on_connected("WLDUSDC", 1, now);
        state.apply_quote(quote(1, 1, now));

        assert_eq!(
            state.refresh_phase(now + Duration::from_millis(5_000), 5_000, true),
            RuntimePhase::Ready
        );
        assert_eq!(
            state.refresh_phase(now + Duration::from_millis(5_001), 5_000, true),
            RuntimePhase::Degraded
        );
    }

    #[test]
    fn every_configured_symbol_must_be_fresh_before_ready() {
        let now = std::time::Instant::now();
        let mut state = RuntimeState::new([Arc::from("WLDUSDC"), Arc::from("ETHUSDT")]);
        state.on_connected("WLDUSDC", 1, now);
        state.on_connected("ETHUSDT", 1, now);
        state.apply_quote(quote(1, 1, now));

        assert_eq!(
            state.refresh_phase(now, 5_000, true),
            RuntimePhase::Starting
        );
    }

    #[test]
    fn unknown_symbol_and_stale_disconnect_do_not_mutate_live_feed() {
        let now = std::time::Instant::now();
        let mut state = RuntimeState::new([Arc::from("WLDUSDC")]);
        state.on_connected("WLDUSDC", 2, now);
        state.apply_quote(quote(1, 2, now));
        let processed = state.processed_events;

        let mut unknown = quote(1, 2, now);
        unknown.symbol = Arc::from("UNKNOWN");
        assert_eq!(state.apply_quote(unknown), QuoteApplyResult::UnknownSymbol);
        assert!(!state.record_transport_activity("WLDUSDC", 1, now + Duration::from_secs(1)));
        state.on_disconnected("WLDUSDC", 1);

        assert_eq!(state.processed_events, processed);
        assert!(state.binance_feeds["WLDUSDC"].connected);
        assert!(state.binance_feeds["WLDUSDC"].book.is_some());
    }

    #[test]
    fn stopping_phase_is_terminal() {
        let now = std::time::Instant::now();
        let mut state = RuntimeState::new([Arc::from("WLDUSDC")]);
        state.on_connected("WLDUSDC", 1, now);
        state.apply_quote(quote(1, 1, now));
        state.stop();

        assert_eq!(
            state.refresh_phase(now, 5_000, true),
            RuntimePhase::Stopping
        );
    }

    #[test]
    fn quote_rejects_zero_or_negative_prices_and_quantities() {
        let now = std::time::Instant::now();
        for (bid_price, bid_quantity, ask_price, ask_quantity) in [
            (Decimal::ZERO, Decimal::ONE, Decimal::ONE, Decimal::ONE),
            (Decimal::ONE, Decimal::ZERO, Decimal::ONE, Decimal::ONE),
            (Decimal::ONE, Decimal::ONE, Decimal::ZERO, Decimal::ONE),
            (Decimal::ONE, Decimal::ONE, Decimal::ONE, Decimal::ZERO),
            (-Decimal::ONE, Decimal::ONE, Decimal::ONE, Decimal::ONE),
        ] {
            assert!(
                TopOfBook::new(
                    Arc::from("WLDUSDC"),
                    1,
                    bid_price,
                    bid_quantity,
                    ask_price,
                    ask_quantity,
                    None,
                    None,
                    now,
                    1,
                    1,
                )
                .is_err()
            );
        }
    }
}

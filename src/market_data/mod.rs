pub mod alchemy;
pub mod binance;

use std::{sync::Arc, time::Instant};

use crate::state::TopOfBook;

#[derive(Debug)]
pub enum MarketEvent {
    FeedConnected {
        symbol: Arc<str>,
        generation: u64,
        observed_at: Instant,
    },
    FeedDisconnected {
        symbol: Arc<str>,
        generation: u64,
        reason: String,
        observed_at: Instant,
    },
    FeedHeartbeat {
        symbol: Arc<str>,
        generation: u64,
        observed_at: Instant,
    },
    BinanceTopOfBook(TopOfBook),
    BinanceDepthApplied {
        symbol: Arc<str>,
        generation: u64,
        last_update_id: u64,
        exchange_event_ts_ms: u64,
        observed_at: Instant,
        received_unix_us: u64,
        wire_frame_size_bytes: usize,
        parse_apply_time_us: u128,
    },
}

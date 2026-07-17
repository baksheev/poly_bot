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
    BinanceTopOfBook(TopOfBook),
    BinanceDepthApplied {
        symbol: Arc<str>,
        generation: u64,
        last_update_id: u64,
        observed_at: Instant,
    },
}

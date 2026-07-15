use std::{str::FromStr, sync::Arc, time::Duration};

use anyhow::{Context, anyhow, bail, ensure};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{config::AppConfig, market_data::MarketEvent, state::TopOfBook};

const INITIAL_RECONNECT_DELAY: Duration = Duration::from_secs(1);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const ROTATE_CONNECTION_AFTER: Duration = Duration::from_secs(23 * 60 * 60 + 55 * 60);

pub fn spawn_book_ticker_connectors(
    config: &AppConfig,
    symbols: &[String],
    sender: mpsc::Sender<MarketEvent>,
) -> Vec<JoinHandle<()>> {
    symbols
        .iter()
        .cloned()
        .map(|symbol| {
            let symbol: Arc<str> = Arc::from(symbol);
            let base_url = config.binance_ws_base_url.trim_end_matches('/').to_owned();
            let sender = sender.clone();
            tokio::spawn(async move {
                run_symbol(base_url, symbol, sender).await;
            })
        })
        .collect()
}

async fn run_symbol(base_url: String, symbol: Arc<str>, sender: mpsc::Sender<MarketEvent>) {
    let mut generation = 0_u64;
    let mut reconnect_delay = INITIAL_RECONNECT_DELAY;

    loop {
        generation = generation.saturating_add(1);
        let result = run_connection(
            &base_url,
            Arc::clone(&symbol),
            generation,
            &sender,
            &mut reconnect_delay,
        )
        .await;
        let reason = result.err().map_or_else(
            || "connection ended".to_owned(),
            |error| format!("{error:#}"),
        );

        tracing::warn!(
            symbol = symbol.as_ref(),
            generation,
            %reason,
            reconnect_delay_ms = reconnect_delay.as_millis(),
            "Binance bookTicker disconnected"
        );

        if sender
            .send(MarketEvent::FeedDisconnected {
                symbol: Arc::clone(&symbol),
                generation,
                reason,
                observed_at: std::time::Instant::now(),
            })
            .await
            .is_err()
        {
            return;
        }

        tokio::time::sleep(reconnect_delay).await;
        reconnect_delay = (reconnect_delay * 2).min(MAX_RECONNECT_DELAY);
    }
}

async fn run_connection(
    base_url: &str,
    symbol: Arc<str>,
    generation: u64,
    sender: &mpsc::Sender<MarketEvent>,
    reconnect_delay: &mut Duration,
) -> anyhow::Result<()> {
    let url = format!("{}/{}@bookTicker", base_url, symbol.to_ascii_lowercase());
    let (socket, response) = tokio::time::timeout(CONNECT_TIMEOUT, connect_async(&url))
        .await
        .context("Binance WebSocket connect timed out")??;

    ensure!(
        response.status().as_u16() == 101,
        "Binance WebSocket upgrade returned {}",
        response.status()
    );
    *reconnect_delay = INITIAL_RECONNECT_DELAY;

    tracing::info!(symbol = symbol.as_ref(), generation, %url, "Binance Spot bookTicker connected");
    sender
        .send(MarketEvent::FeedConnected {
            symbol: Arc::clone(&symbol),
            generation,
            observed_at: std::time::Instant::now(),
        })
        .await
        .map_err(|_| anyhow!("market event receiver closed"))?;

    let (mut writer, mut reader) = socket.split();
    let rotation = tokio::time::sleep(ROTATE_CONNECTION_AFTER);
    tokio::pin!(rotation);

    loop {
        tokio::select! {
            _ = &mut rotation => bail!("scheduled connection rotation before Binance 24-hour limit"),
            message = reader.next() => {
                let message = message.context("Binance WebSocket stream ended")??;
                match message {
                    Message::Text(payload) => {
                        forward_payload(payload.as_bytes(), Arc::clone(&symbol), generation, sender)?;
                    }
                    Message::Binary(payload) => {
                        forward_payload(payload.as_ref(), Arc::clone(&symbol), generation, sender)?;
                    }
                    Message::Ping(payload) => {
                        writer.send(Message::Pong(payload)).await.context("failed to send Binance pong")?;
                    }
                    Message::Pong(_) => {}
                    Message::Close(frame) => bail!("Binance closed WebSocket: {frame:?}"),
                    Message::Frame(_) => {}
                }
            }
        }
    }
}

fn forward_payload(
    payload: &[u8],
    symbol: Arc<str>,
    generation: u64,
    sender: &mpsc::Sender<MarketEvent>,
) -> anyhow::Result<()> {
    let received_at = std::time::Instant::now();
    let received_unix_us = unix_timestamp_us();
    let quote = parse_book_ticker(payload, symbol, generation, received_at, received_unix_us)?;

    sender
        .try_send(MarketEvent::BinanceTopOfBook(quote))
        .map_err(|error| anyhow!("critical market event channel unavailable: {error}"))?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct WireBookTicker<'a> {
    #[serde(rename = "e")]
    event_type: Option<&'a str>,
    #[serde(rename = "E")]
    event_time_ms: Option<u64>,
    #[serde(rename = "T")]
    transaction_time_ms: Option<u64>,
    #[serde(rename = "u")]
    update_id: u64,
    #[serde(rename = "s")]
    symbol: &'a str,
    #[serde(rename = "b")]
    bid_price: &'a str,
    #[serde(rename = "B")]
    bid_quantity: &'a str,
    #[serde(rename = "a")]
    ask_price: &'a str,
    #[serde(rename = "A")]
    ask_quantity: &'a str,
}

fn parse_book_ticker(
    payload: &[u8],
    expected_symbol: Arc<str>,
    generation: u64,
    received_at: std::time::Instant,
    received_unix_us: u64,
) -> anyhow::Result<TopOfBook> {
    let frame: WireBookTicker<'_> =
        serde_json::from_slice(payload).context("invalid Binance bookTicker JSON")?;

    if let Some(event_type) = frame.event_type {
        ensure!(
            event_type == "bookTicker",
            "unexpected Binance event {event_type}"
        );
    }
    ensure!(
        frame.symbol == expected_symbol.as_ref(),
        "received Binance symbol {}, expected {}",
        frame.symbol,
        expected_symbol
    );

    TopOfBook::new(
        expected_symbol,
        frame.update_id,
        Decimal::from_str(frame.bid_price).context("invalid Binance bid price")?,
        Decimal::from_str(frame.bid_quantity).context("invalid Binance bid quantity")?,
        Decimal::from_str(frame.ask_price).context("invalid Binance ask price")?,
        Decimal::from_str(frame.ask_quantity).context("invalid Binance ask quantity")?,
        frame.event_time_ms,
        frame.transaction_time_ms,
        received_at,
        received_unix_us,
        generation,
    )
}

fn unix_timestamp_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rust_decimal::Decimal;

    use super::parse_book_ticker;

    #[test]
    fn parses_spot_book_ticker_without_floating_point() {
        let payload = br#"{
            "e":"bookTicker",
            "u":400900217,
            "E":1568014460894,
            "T":1568014460893,
            "s":"WLDUSDC",
            "b":"0.81230000",
            "B":"31.21000000",
            "a":"0.81250000",
            "A":"40.66000000"
        }"#;

        let quote = parse_book_ticker(
            payload,
            Arc::from("WLDUSDC"),
            7,
            std::time::Instant::now(),
            123,
        )
        .unwrap();

        assert_eq!(quote.bid_price, Decimal::new(8_123, 4));
        assert_eq!(quote.ask_price, Decimal::new(8_125, 4));
        assert_eq!(quote.update_id, 400900217);
        assert_eq!(quote.connection_generation, 7);
        assert_eq!(quote.received_unix_us, 123);
    }

    #[test]
    fn supports_book_ticker_without_optional_event_timestamps() {
        let payload = br#"{
            "u":1,"s":"WLDUSDC","b":"0.8","B":"1","a":"0.9","A":"2"
        }"#;

        let quote = parse_book_ticker(
            payload,
            Arc::from("WLDUSDC"),
            1,
            std::time::Instant::now(),
            1,
        )
        .unwrap();
        assert_eq!(quote.exchange_event_ts_ms, None);
        assert_eq!(quote.exchange_transaction_ts_ms, None);
    }

    #[test]
    fn rejects_wrong_symbol_and_crossed_book() {
        let wrong_symbol = br#"{
            "u":1,"s":"BTCUSDT","b":"1","B":"1","a":"2","A":"1"
        }"#;
        assert!(
            parse_book_ticker(
                wrong_symbol,
                Arc::from("WLDUSDC"),
                1,
                std::time::Instant::now(),
                1,
            )
            .is_err()
        );

        let crossed = br#"{
            "u":1,"s":"WLDUSDC","b":"2","B":"1","a":"1","A":"1"
        }"#;
        assert!(
            parse_book_ticker(
                crossed,
                Arc::from("WLDUSDC"),
                1,
                std::time::Instant::now(),
                1,
            )
            .is_err()
        );
    }
}

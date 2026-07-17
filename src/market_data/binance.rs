use std::{collections::VecDeque, str::FromStr, sync::Arc, time::Duration};

use anyhow::{Context, ensure};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::json;
use tokio::time::Instant as TokioInstant;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::{
    binance::{
        account::BinanceAccountClient,
        depth::{DepthApplyResult, SpotDepthBook, parse_depth_update},
    },
    config::AppConfig,
    market_data::MarketEvent,
    state::TopOfBook,
};

const INITIAL_RECONNECT_DELAY: Duration = Duration::from_secs(1);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const ROTATE_CONNECTION_AFTER: Duration = Duration::from_secs(23 * 60 * 60 + 55 * 60);

type BinanceSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// Binance Spot reader polled directly by the single state-owner task.
///
/// Keeping the socket future and the opportunity engine in the same task
/// removes the market-data mpsc hop and its scheduler wakeup from the decision
/// path. Reconnects remain fail-closed through the normal feed state events.
pub struct BookTickerFeed {
    base_url: String,
    symbol: Arc<str>,
    generation: u64,
    socket: Option<BinanceSocket>,
    reconnect_delay: Duration,
    connect_not_before: TokioInstant,
    rotate_at: TokioInstant,
    depth_client: Option<BinanceAccountClient>,
    depth_book: Option<SpotDepthBook>,
}

impl BookTickerFeed {
    pub fn new(config: &AppConfig, symbol: String) -> Self {
        let now = TokioInstant::now();
        Self {
            base_url: config.binance_ws_base_url.trim_end_matches('/').to_owned(),
            symbol: Arc::from(symbol),
            generation: 0,
            socket: None,
            reconnect_delay: INITIAL_RECONNECT_DELAY,
            connect_not_before: now,
            rotate_at: now + ROTATE_CONNECTION_AFTER,
            depth_client: None,
            depth_book: None,
        }
    }

    pub fn new_with_depth(
        config: &AppConfig,
        symbol: String,
        depth_client: BinanceAccountClient,
    ) -> Self {
        let mut feed = Self::new(config, symbol);
        feed.depth_client = Some(depth_client);
        feed
    }

    pub fn depth_book(&self) -> Option<&SpotDepthBook> {
        self.depth_book.as_ref()
    }

    pub async fn next_event(&mut self) -> MarketEvent {
        loop {
            if self.socket.is_none() {
                tokio::time::sleep_until(self.connect_not_before).await;
                let next_generation = self.generation.saturating_add(1);
                match self.connect(next_generation).await {
                    Ok(socket) => {
                        self.generation = next_generation;
                        self.socket = Some(socket);
                        self.reconnect_delay = INITIAL_RECONNECT_DELAY;
                        self.rotate_at = TokioInstant::now() + ROTATE_CONNECTION_AFTER;
                        return MarketEvent::FeedConnected {
                            symbol: Arc::clone(&self.symbol),
                            generation: self.generation,
                            observed_at: std::time::Instant::now(),
                        };
                    }
                    Err(error) => {
                        return self.disconnect(format!("{error:#}"));
                    }
                }
            }

            let socket = self.socket.as_mut().expect("socket checked above");
            let rotation = tokio::time::sleep_until(self.rotate_at);
            tokio::pin!(rotation);
            tokio::select! {
                _ = &mut rotation => {
                    return self.disconnect(
                        "scheduled connection rotation before Binance 24-hour limit".to_owned(),
                    );
                }
                message = socket.next() => {
                    let message = match message {
                        Some(Ok(message)) => message,
                        Some(Err(error)) => return self.disconnect(format!("{error:#}")),
                        None => return self.disconnect("Binance WebSocket stream ended".to_owned()),
                    };
                    match message {
                        Message::Text(payload) => match self.parse_payload(payload.as_bytes()) {
                            Ok(event) => return event,
                            Err(error) => return self.disconnect(format!("{error:#}")),
                        },
                        Message::Binary(payload) => match self.parse_payload(payload.as_ref()) {
                            Ok(event) => return event,
                            Err(error) => return self.disconnect(format!("{error:#}")),
                        },
                        Message::Ping(payload) => {
                            if let Err(error) = socket.send(Message::Pong(payload)).await {
                                return self.disconnect(format!("failed to send Binance pong: {error:#}"));
                            }
                        }
                        Message::Pong(_) => {}
                        Message::Close(frame) => {
                            return self.disconnect(format!("Binance closed WebSocket: {frame:?}"));
                        }
                        Message::Frame(_) => {}
                    }
                }
            }
        }
    }

    async fn connect(&mut self, generation: u64) -> anyhow::Result<BinanceSocket> {
        let with_depth = self.depth_client.is_some();
        let url = if with_depth {
            self.base_url.clone()
        } else {
            format!(
                "{}/{}@bookTicker",
                self.base_url,
                self.symbol.to_ascii_lowercase()
            )
        };
        let (mut socket, response) = tokio::time::timeout(CONNECT_TIMEOUT, connect_async(&url))
            .await
            .context("Binance WebSocket connect timed out")??;
        ensure!(
            response.status().as_u16() == 101,
            "Binance WebSocket upgrade returned {}",
            response.status()
        );
        if with_depth {
            tokio::time::timeout(
                CONNECT_TIMEOUT,
                self.subscribe_and_bootstrap_depth(&mut socket, generation),
            )
            .await
            .context("Binance depth bootstrap timed out")??;
        }
        tracing::info!(
            symbol = self.symbol.as_ref(),
            generation,
            %url,
            "Binance Spot bookTicker connected"
        );
        Ok(socket)
    }

    async fn subscribe_and_bootstrap_depth(
        &mut self,
        socket: &mut BinanceSocket,
        generation: u64,
    ) -> anyhow::Result<()> {
        let stream_symbol = self.symbol.to_ascii_lowercase();
        socket
            .send(Message::Text(
                json!({
                    "method": "SUBSCRIBE",
                    "params": [
                        format!("{stream_symbol}@bookTicker"),
                        format!("{stream_symbol}@depth@100ms"),
                    ],
                    "id": generation,
                })
                .to_string()
                .into(),
            ))
            .await
            .context("failed to subscribe Binance market streams")?;
        let mut buffered = wait_for_subscription_ack(socket, generation).await?;

        let snapshot = self
            .depth_client
            .as_ref()
            .context("Binance depth client disappeared")?
            .depth_snapshot(&self.symbol, 5_000)
            .await?;
        let mut book = SpotDepthBook::from_snapshot(self.symbol.to_string(), snapshot)?;
        loop {
            let payload = match buffered.pop_front() {
                Some(payload) => payload,
                None => next_data_payload(socket).await?,
            };
            let envelope: WireEventType<'_> =
                serde_json::from_slice(&payload).context("invalid Binance stream JSON")?;
            match envelope.event_type {
                Some("depthUpdate") => {
                    let update = parse_depth_update(&payload, &self.symbol)?;
                    if book.apply(update)? == DepthApplyResult::Applied {
                        self.depth_book = Some(book);
                        return Ok(());
                    }
                }
                Some("bookTicker") | None => {}
                Some(other) => anyhow::bail!("unexpected Binance stream event {other}"),
            }
        }
    }

    fn parse_payload(&mut self, payload: &[u8]) -> anyhow::Result<MarketEvent> {
        let received_at = std::time::Instant::now();
        let received_unix_us = unix_timestamp_us();
        let envelope: WireEventType<'_> =
            serde_json::from_slice(payload).context("invalid Binance stream JSON")?;
        match envelope.event_type {
            Some("bookTicker") | None => Ok(MarketEvent::BinanceTopOfBook(parse_book_ticker(
                payload,
                Arc::clone(&self.symbol),
                self.generation,
                received_at,
                received_unix_us,
            )?)),
            Some("depthUpdate") => {
                let update = parse_depth_update(payload, &self.symbol)?;
                let book = self
                    .depth_book
                    .as_mut()
                    .context("Binance depth update arrived before bootstrap")?;
                let result = book.apply(update)?;
                ensure!(
                    result == DepthApplyResult::Applied,
                    "stale Binance depth event after bootstrap"
                );
                Ok(MarketEvent::BinanceDepthApplied {
                    symbol: Arc::clone(&self.symbol),
                    generation: self.generation,
                    last_update_id: book.last_update_id(),
                    observed_at: received_at,
                })
            }
            Some(other) => anyhow::bail!("unexpected Binance stream event {other}"),
        }
    }

    fn disconnect(&mut self, reason: String) -> MarketEvent {
        self.socket = None;
        self.depth_book = None;
        let delay = self.reconnect_delay;
        self.connect_not_before = TokioInstant::now() + delay;
        self.reconnect_delay = (delay * 2).min(MAX_RECONNECT_DELAY);
        tracing::warn!(
            symbol = self.symbol.as_ref(),
            generation = self.generation,
            %reason,
            reconnect_delay_ms = delay.as_millis(),
            "Binance bookTicker disconnected"
        );
        MarketEvent::FeedDisconnected {
            symbol: Arc::clone(&self.symbol),
            generation: self.generation,
            reason,
            observed_at: std::time::Instant::now(),
        }
    }
}

async fn wait_for_subscription_ack(
    socket: &mut BinanceSocket,
    expected_id: u64,
) -> anyhow::Result<VecDeque<Vec<u8>>> {
    tokio::time::timeout(CONNECT_TIMEOUT, async {
        let mut buffered = VecDeque::new();
        loop {
            let payload = next_data_payload(socket).await?;
            let response: WireOptionalResponseId = serde_json::from_slice(&payload)
                .context("invalid Binance subscription response JSON")?;
            if response.id.is_some() {
                let acknowledgement: WireSubscriptionAck = serde_json::from_slice(&payload)
                    .context("Binance rejected market stream subscription")?;
                ensure!(
                    acknowledgement.id == expected_id && acknowledgement.result.is_null(),
                    "Binance rejected or mismatched market stream subscription"
                );
                return Ok(buffered);
            }
            buffered.push_back(payload);
        }
    })
    .await
    .context("Binance subscription acknowledgement timed out")?
}

async fn next_data_payload(socket: &mut BinanceSocket) -> anyhow::Result<Vec<u8>> {
    loop {
        let message = socket
            .next()
            .await
            .context("Binance WebSocket stream ended")??;
        match message {
            Message::Text(payload) => return Ok(payload.as_bytes().to_vec()),
            Message::Binary(payload) => return Ok(payload.to_vec()),
            Message::Ping(payload) => socket
                .send(Message::Pong(payload))
                .await
                .context("failed to send Binance pong")?,
            Message::Pong(_) | Message::Frame(_) => {}
            Message::Close(frame) => anyhow::bail!("Binance closed WebSocket: {frame:?}"),
        }
    }
}

#[derive(Deserialize)]
struct WireEventType<'a> {
    #[serde(rename = "e")]
    event_type: Option<&'a str>,
}

#[derive(Deserialize)]
struct WireSubscriptionAck {
    result: serde_json::Value,
    id: u64,
}

#[derive(Deserialize)]
struct WireOptionalResponseId {
    id: Option<u64>,
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

    #[test]
    fn rejects_wrong_event_type_and_missing_required_fields() {
        let wrong_event = br#"{
            "e":"trade","u":1,"s":"WLDUSDC","b":"1","B":"1","a":"2","A":"1"
        }"#;
        assert!(
            parse_book_ticker(
                wrong_event,
                Arc::from("WLDUSDC"),
                1,
                std::time::Instant::now(),
                1,
            )
            .is_err()
        );

        let missing_ask = br#"{"u":1,"s":"WLDUSDC","b":"1","B":"1","A":"1"}"#;
        assert!(
            parse_book_ticker(
                missing_ask,
                Arc::from("WLDUSDC"),
                1,
                std::time::Instant::now(),
                1,
            )
            .is_err()
        );
        assert!(
            parse_book_ticker(
                b"not-json",
                Arc::from("WLDUSDC"),
                1,
                std::time::Instant::now(),
                1,
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_invalid_decimals_and_non_positive_liquidity() {
        let invalid_decimal = br#"{
            "u":1,"s":"WLDUSDC","b":"NaN","B":"1","a":"2","A":"1"
        }"#;
        assert!(
            parse_book_ticker(
                invalid_decimal,
                Arc::from("WLDUSDC"),
                1,
                std::time::Instant::now(),
                1,
            )
            .is_err()
        );

        let zero_bid_quantity = br#"{
            "u":1,"s":"WLDUSDC","b":"1","B":"0","a":"2","A":"1"
        }"#;
        assert!(
            parse_book_ticker(
                zero_bid_quantity,
                Arc::from("WLDUSDC"),
                1,
                std::time::Instant::now(),
                1,
            )
            .is_err()
        );
    }

    #[test]
    fn preserves_sub_satoshi_decimal_precision() {
        let payload = br#"{
            "u":1,"s":"WLDUSDC","b":"0.123456789123456789","B":"1.000000001",
            "a":"0.123456789123456790","A":"2.000000002"
        }"#;
        let quote = parse_book_ticker(
            payload,
            Arc::from("WLDUSDC"),
            1,
            std::time::Instant::now(),
            1,
        )
        .unwrap();

        assert_eq!(
            quote.bid_price,
            Decimal::from_str_exact("0.123456789123456789").unwrap()
        );
        assert_eq!(
            quote.ask_price,
            Decimal::from_str_exact("0.123456789123456790").unwrap()
        );
    }
}

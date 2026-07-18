use std::{collections::BTreeMap, fmt, str::FromStr, time::Duration};

use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::{binance::account::BinanceCredentials, config::AppConfig};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(12);
const RECV_WINDOW_MS: u64 = 5_000;

type BinanceSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

pub struct BinanceWsApiClient {
    socket: BinanceSocket,
    credentials: BinanceCredentials,
    clock_offset_ms: i64,
    next_request_id: u64,
}

impl BinanceWsApiClient {
    pub async fn connect(config: &AppConfig) -> Result<Self, WsApiError> {
        let credentials = BinanceCredentials::from_env()
            .map_err(|error| WsApiError::Protocol(error.to_string()))?;
        let (socket, response) =
            tokio::time::timeout(CONNECT_TIMEOUT, connect_async(&config.binance_ws_api_url))
                .await
                .map_err(|_| WsApiError::Transport("connection timed out".to_owned()))?
                .map_err(|_| WsApiError::Transport("connection failed".to_owned()))?;
        if response.status().as_u16() != 101 {
            return Err(WsApiError::Protocol(format!(
                "WebSocket upgrade returned {}",
                response.status().as_u16()
            )));
        }

        let mut client = Self {
            socket,
            credentials,
            clock_offset_ms: 0,
            next_request_id: 1,
        };
        client.synchronize_clock().await?;
        Ok(client)
    }

    pub async fn synchronize_clock(&mut self) -> Result<(), WsApiError> {
        let local_before = unix_timestamp_ms()?;
        let response: ServerTime = self.call("time", Map::new()).await?;
        let local_after = unix_timestamp_ms()?;
        let midpoint = local_before.saturating_add(local_after) / 2;
        self.clock_offset_ms = i128::from(response.server_time)
            .checked_sub(i128::from(midpoint))
            .and_then(|difference| i64::try_from(difference).ok())
            .ok_or_else(|| WsApiError::Protocol("clock offset overflow".to_owned()))?;
        Ok(())
    }

    pub async fn test_market_buy(
        &mut self,
        symbol: &str,
        quote_order_qty: Decimal,
        client_order_id: &str,
    ) -> Result<(), WsApiError> {
        let params = market_buy_params(symbol, quote_order_qty, client_order_id)?;
        let _: Value = self.signed_call("order.test", params).await?;
        Ok(())
    }

    pub async fn place_market_buy(
        &mut self,
        symbol: &str,
        quote_order_qty: Decimal,
        client_order_id: &str,
    ) -> Result<OrderResult, WsApiError> {
        let params = market_buy_params(symbol, quote_order_qty, client_order_id)?;
        self.signed_call("order.place", params).await
    }

    pub async fn test_market_sell(
        &mut self,
        symbol: &str,
        quantity: Decimal,
        client_order_id: &str,
    ) -> Result<(), WsApiError> {
        let params = market_sell_params(symbol, quantity, client_order_id)?;
        let _: Value = self.signed_call("order.test", params).await?;
        Ok(())
    }

    pub async fn place_market_sell(
        &mut self,
        symbol: &str,
        quantity: Decimal,
        client_order_id: &str,
    ) -> Result<OrderResult, WsApiError> {
        let params = market_sell_params(symbol, quantity, client_order_id)?;
        self.signed_call("order.place", params).await
    }

    pub async fn place_limit_ioc(
        &mut self,
        symbol: &str,
        side: &str,
        quantity: Decimal,
        price: Decimal,
        client_order_id: &str,
    ) -> Result<OrderResult, WsApiError> {
        let params = limit_ioc_params(symbol, side, quantity, price, client_order_id)?;
        self.signed_call("order.place", params).await
    }

    pub async fn query_order(
        &mut self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<OrderResult, WsApiError> {
        validate_symbol(symbol)?;
        validate_client_order_id(client_order_id)?;
        let mut params = Map::new();
        params.insert("symbol".to_owned(), Value::String(symbol.to_owned()));
        params.insert(
            "origClientOrderId".to_owned(),
            Value::String(client_order_id.to_owned()),
        );
        self.signed_call("order.status", params).await
    }

    pub async fn recent_orders(
        &mut self,
        symbol: &str,
        limit: u16,
    ) -> Result<Vec<OrderResult>, WsApiError> {
        validate_symbol(symbol)?;
        if !(1..=1_000).contains(&limit) {
            return Err(WsApiError::Protocol(
                "order history limit must be between 1 and 1000".to_owned(),
            ));
        }
        let mut params = Map::new();
        params.insert("symbol".to_owned(), Value::String(symbol.to_owned()));
        params.insert("limit".to_owned(), Value::from(limit));
        self.signed_call("allOrders", params).await
    }

    pub async fn open_orders(&mut self, symbol: &str) -> Result<Vec<OrderResult>, WsApiError> {
        validate_symbol(symbol)?;
        let mut params = Map::new();
        params.insert("symbol".to_owned(), Value::String(symbol.to_owned()));
        self.signed_call("openOrders.status", params).await
    }

    pub fn clock_offset_ms(&self) -> i64 {
        self.clock_offset_ms
    }

    async fn signed_call<T>(
        &mut self,
        method: &str,
        mut params: Map<String, Value>,
    ) -> Result<T, WsApiError>
    where
        T: for<'de> Deserialize<'de>,
    {
        let timestamp = apply_clock_offset(unix_timestamp_ms()?, self.clock_offset_ms)?;
        params.insert(
            "apiKey".to_owned(),
            Value::String(self.credentials.api_key().to_owned()),
        );
        params.insert("recvWindow".to_owned(), Value::from(RECV_WINDOW_MS));
        params.insert("timestamp".to_owned(), Value::from(timestamp));

        let payload = signature_payload(&params)?;
        let signature = self
            .credentials
            .sign(&payload)
            .map_err(|error| WsApiError::Protocol(error.to_string()))?;
        params.insert("signature".to_owned(), Value::String(signature));
        self.call(method, params).await
    }

    async fn call<T>(&mut self, method: &str, params: Map<String, Value>) -> Result<T, WsApiError>
    where
        T: for<'de> Deserialize<'de>,
    {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.saturating_add(1);
        let request = json!({
            "id": request_id,
            "method": method,
            "params": params,
        });
        self.socket
            .send(Message::Text(request.to_string().into()))
            .await
            .map_err(|_| WsApiError::Transport("request send failed".to_owned()))?;

        loop {
            let message = tokio::time::timeout(RESPONSE_TIMEOUT, self.socket.next())
                .await
                .map_err(|_| WsApiError::Transport("response timed out".to_owned()))?
                .ok_or_else(|| WsApiError::Transport("connection ended".to_owned()))?
                .map_err(|_| WsApiError::Transport("response read failed".to_owned()))?;
            let payload = match message {
                Message::Text(payload) => payload.as_bytes().to_vec(),
                Message::Binary(payload) => payload.to_vec(),
                Message::Ping(payload) => {
                    self.socket
                        .send(Message::Pong(payload))
                        .await
                        .map_err(|_| WsApiError::Transport("pong send failed".to_owned()))?;
                    continue;
                }
                Message::Pong(_) | Message::Frame(_) => continue,
                Message::Close(_) => {
                    return Err(WsApiError::Transport(
                        "connection closed before response".to_owned(),
                    ));
                }
            };
            let response: WireResponse = serde_json::from_slice(&payload)
                .map_err(|_| WsApiError::Protocol("invalid response JSON".to_owned()))?;
            let Some(response_id) = response.id.and_then(|id| id.as_u64()) else {
                // User Data Stream events have no request id and are consumed by
                // the execution coordinator in the persistent runtime client.
                continue;
            };
            if response_id != request_id {
                return Err(WsApiError::Protocol(format!(
                    "response id {response_id} does not match request {request_id}"
                )));
            }
            if response.status != 200 {
                let error = response.error.unwrap_or(WireError {
                    code: i64::from(response.status),
                    message: "request rejected without Binance error body".to_owned(),
                });
                return Err(WsApiError::Rejected {
                    status: response.status,
                    code: error.code,
                    message: sanitize_message(&error.message),
                });
            }
            let result = response.result.ok_or_else(|| {
                WsApiError::Protocol("successful response has no result".to_owned())
            })?;
            return serde_json::from_value(result)
                .map_err(|_| WsApiError::Protocol("invalid response result".to_owned()));
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OrderResult {
    pub symbol: String,
    pub order_id: u64,
    pub client_order_id: String,
    #[serde(default)]
    pub transact_time: Option<u64>,
    #[serde(deserialize_with = "deserialize_decimal")]
    pub price: Decimal,
    #[serde(deserialize_with = "deserialize_decimal")]
    pub orig_qty: Decimal,
    #[serde(deserialize_with = "deserialize_decimal")]
    pub executed_qty: Decimal,
    #[serde(deserialize_with = "deserialize_decimal")]
    pub orig_quote_order_qty: Decimal,
    #[serde(deserialize_with = "deserialize_decimal")]
    pub cummulative_quote_qty: Decimal,
    pub status: String,
    pub time_in_force: String,
    #[serde(rename = "type")]
    pub order_type: String,
    pub side: String,
    #[serde(default)]
    pub fills: Vec<OrderFill>,
}

impl OrderResult {
    pub fn commission_in(&self, asset: &str) -> Decimal {
        self.fills
            .iter()
            .filter(|fill| fill.commission_asset == asset)
            .map(|fill| fill.commission)
            .sum()
    }

    pub fn balance_changes(
        &self,
        base_asset: &str,
        quote_asset: &str,
    ) -> Result<BTreeMap<String, Decimal>, WsApiError> {
        validate_asset(base_asset)?;
        validate_asset(quote_asset)?;
        if base_asset == quote_asset {
            return Err(WsApiError::Protocol(
                "base and quote assets must differ".to_owned(),
            ));
        }
        if self.executed_qty < Decimal::ZERO || self.cummulative_quote_qty < Decimal::ZERO {
            return Err(WsApiError::Protocol(
                "executed order quantities must not be negative".to_owned(),
            ));
        }

        let mut changes = BTreeMap::new();
        match self.side.as_str() {
            "BUY" => {
                changes.insert(base_asset.to_owned(), self.executed_qty);
                changes.insert(quote_asset.to_owned(), -self.cummulative_quote_qty);
            }
            "SELL" => {
                changes.insert(base_asset.to_owned(), -self.executed_qty);
                changes.insert(quote_asset.to_owned(), self.cummulative_quote_qty);
            }
            _ => {
                return Err(WsApiError::Protocol(format!(
                    "unsupported Binance order side {}",
                    sanitize_message(&self.side)
                )));
            }
        }
        for fill in &self.fills {
            validate_asset(&fill.commission_asset)?;
            if fill.commission < Decimal::ZERO {
                return Err(WsApiError::Protocol(
                    "Binance fill commission must not be negative".to_owned(),
                ));
            }
            if !fill.commission.is_zero() {
                *changes
                    .entry(fill.commission_asset.clone())
                    .or_insert(Decimal::ZERO) -= fill.commission;
            }
        }
        Ok(changes)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OrderFill {
    #[serde(deserialize_with = "deserialize_decimal")]
    pub price: Decimal,
    #[serde(deserialize_with = "deserialize_decimal")]
    pub qty: Decimal,
    #[serde(deserialize_with = "deserialize_decimal")]
    pub commission: Decimal,
    pub commission_asset: String,
    pub trade_id: i64,
}

#[derive(Debug)]
pub enum WsApiError {
    Transport(String),
    Rejected {
        status: u16,
        code: i64,
        message: String,
    },
    Protocol(String),
}

impl fmt::Display for WsApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(message) => write!(formatter, "Binance WS transport: {message}"),
            Self::Rejected {
                status,
                code,
                message,
            } => write!(
                formatter,
                "Binance WS rejected request with status {status}, code {code}: {message}"
            ),
            Self::Protocol(message) => write!(formatter, "Binance WS protocol: {message}"),
        }
    }
}

impl std::error::Error for WsApiError {}

#[derive(Deserialize)]
struct WireResponse {
    id: Option<Value>,
    status: u16,
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<WireError>,
}

#[derive(Deserialize)]
struct WireError {
    code: i64,
    #[serde(rename = "msg")]
    message: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServerTime {
    server_time: u64,
}

pub(crate) fn market_buy_params(
    symbol: &str,
    quote_order_qty: Decimal,
    client_order_id: &str,
) -> Result<Map<String, Value>, WsApiError> {
    validate_symbol(symbol)?;
    validate_client_order_id(client_order_id)?;
    if quote_order_qty <= Decimal::ZERO {
        return Err(WsApiError::Protocol(
            "quote order quantity must be positive".to_owned(),
        ));
    }
    let mut params = common_order_params(symbol, "BUY", client_order_id);
    params.insert("type".to_owned(), Value::String("MARKET".to_owned()));
    params.insert(
        "quoteOrderQty".to_owned(),
        Value::String(quote_order_qty.normalize().to_string()),
    );
    Ok(params)
}

pub(crate) fn market_buy_quantity_params(
    symbol: &str,
    quantity: Decimal,
    client_order_id: &str,
) -> Result<Map<String, Value>, WsApiError> {
    validate_symbol(symbol)?;
    validate_client_order_id(client_order_id)?;
    if quantity <= Decimal::ZERO {
        return Err(WsApiError::Protocol(
            "order quantity must be positive".to_owned(),
        ));
    }
    let mut params = common_order_params(symbol, "BUY", client_order_id);
    params.insert("type".to_owned(), Value::String("MARKET".to_owned()));
    params.insert(
        "quantity".to_owned(),
        Value::String(quantity.normalize().to_string()),
    );
    Ok(params)
}

pub(crate) fn market_sell_params(
    symbol: &str,
    quantity: Decimal,
    client_order_id: &str,
) -> Result<Map<String, Value>, WsApiError> {
    validate_symbol(symbol)?;
    validate_client_order_id(client_order_id)?;
    if quantity <= Decimal::ZERO {
        return Err(WsApiError::Protocol(
            "order quantity must be positive".to_owned(),
        ));
    }
    let mut params = common_order_params(symbol, "SELL", client_order_id);
    params.insert("type".to_owned(), Value::String("MARKET".to_owned()));
    params.insert(
        "quantity".to_owned(),
        Value::String(quantity.normalize().to_string()),
    );
    Ok(params)
}

pub(crate) fn limit_ioc_params(
    symbol: &str,
    side: &str,
    quantity: Decimal,
    price: Decimal,
    client_order_id: &str,
) -> Result<Map<String, Value>, WsApiError> {
    validate_symbol(symbol)?;
    validate_client_order_id(client_order_id)?;
    if !matches!(side, "BUY" | "SELL") {
        return Err(WsApiError::Protocol(
            "limit order side must be BUY or SELL".to_owned(),
        ));
    }
    if quantity <= Decimal::ZERO || price <= Decimal::ZERO {
        return Err(WsApiError::Protocol(
            "limit order quantity and price must be positive".to_owned(),
        ));
    }
    let mut params = common_order_params(symbol, side, client_order_id);
    params.insert("type".to_owned(), Value::String("LIMIT".to_owned()));
    params.insert("timeInForce".to_owned(), Value::String("IOC".to_owned()));
    params.insert(
        "quantity".to_owned(),
        Value::String(quantity.normalize().to_string()),
    );
    params.insert(
        "price".to_owned(),
        Value::String(price.normalize().to_string()),
    );
    Ok(params)
}

fn common_order_params(symbol: &str, side: &str, client_order_id: &str) -> Map<String, Value> {
    let mut params = Map::new();
    params.insert("symbol".to_owned(), Value::String(symbol.to_owned()));
    params.insert("side".to_owned(), Value::String(side.to_owned()));
    params.insert(
        "newClientOrderId".to_owned(),
        Value::String(client_order_id.to_owned()),
    );
    params.insert(
        "newOrderRespType".to_owned(),
        Value::String("FULL".to_owned()),
    );
    params
}

fn signature_payload(params: &Map<String, Value>) -> Result<String, WsApiError> {
    let mut sorted = BTreeMap::new();
    for (name, value) in params {
        if name == "signature" {
            continue;
        }
        let encoded = match value {
            Value::String(value) => value.clone(),
            Value::Number(value) => value.to_string(),
            Value::Bool(value) => value.to_string(),
            _ => {
                return Err(WsApiError::Protocol(format!(
                    "unsupported signed parameter {name}"
                )));
            }
        };
        sorted.insert(name, encoded);
    }
    Ok(sorted
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&"))
}

pub(crate) fn validate_symbol(value: &str) -> Result<(), WsApiError> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    {
        return Err(WsApiError::Protocol(
            "symbol contains unsupported characters".to_owned(),
        ));
    }
    Ok(())
}

fn validate_asset(value: &str) -> Result<(), WsApiError> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    {
        return Err(WsApiError::Protocol(
            "asset contains unsupported characters".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_client_order_id(value: &str) -> Result<(), WsApiError> {
    if value.is_empty()
        || value.len() > 36
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
        })
    {
        return Err(WsApiError::Protocol(
            "client order id is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn unix_timestamp_ms() -> Result<u64, WsApiError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| WsApiError::Protocol("system clock is before Unix epoch".to_owned()))?
        .as_millis()
        .try_into()
        .map_err(|_| WsApiError::Protocol("timestamp overflow".to_owned()))
}

fn apply_clock_offset(timestamp: u64, offset: i64) -> Result<u64, WsApiError> {
    let adjusted = i128::from(timestamp) + i128::from(offset);
    adjusted
        .try_into()
        .map_err(|_| WsApiError::Protocol("adjusted timestamp overflow".to_owned()))
}

fn deserialize_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    Decimal::from_str(&value).map_err(serde::de::Error::custom)
}

fn sanitize_message(message: &str) -> String {
    message
        .chars()
        .filter(|character| !character.is_control())
        .take(256)
        .collect()
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use serde_json::{Map, Value};

    use super::{
        OrderResult, limit_ioc_params, market_buy_params, market_buy_quantity_params,
        market_sell_params, signature_payload,
    };

    fn order(side: &str, executed: &str, quote: &str, fills: serde_json::Value) -> OrderResult {
        serde_json::from_value(serde_json::json!({
            "symbol": "WLDUSDC",
            "orderId": 1,
            "clientOrderId": "rust-test",
            "price": "0",
            "origQty": executed,
            "executedQty": executed,
            "origQuoteOrderQty": quote,
            "cummulativeQuoteQty": quote,
            "status": "FILLED",
            "timeInForce": "GTC",
            "type": "MARKET",
            "side": side,
            "fills": fills,
        }))
        .unwrap()
    }

    fn fill(commission: &str, asset: &str, trade_id: i64) -> serde_json::Value {
        serde_json::json!({
            "price": "0.81234567",
            "qty": "1.0",
            "commission": commission,
            "commissionAsset": asset,
            "tradeId": trade_id,
        })
    }

    #[test]
    fn full_order_result_round_trips_through_the_durable_journal_shape() {
        let order = order(
            "BUY",
            "1.23456789",
            "2.34567891",
            serde_json::json!([fill("0.0001", "WLD", 7)]),
        );
        let encoded = serde_json::to_vec(&order).unwrap();
        let decoded: OrderResult = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, order);
    }

    #[test]
    fn signs_parameters_in_alphabetical_order() {
        let mut params = Map::new();
        params.insert("symbol".to_owned(), Value::String("LTCBTC".to_owned()));
        params.insert("side".to_owned(), Value::String("BUY".to_owned()));
        params.insert("timestamp".to_owned(), Value::from(1_499_827_319_559_u64));
        params.insert("apiKey".to_owned(), Value::String("key".to_owned()));

        assert_eq!(
            signature_payload(&params).unwrap(),
            "apiKey=key&side=BUY&symbol=LTCBTC&timestamp=1499827319559"
        );
    }

    #[test]
    fn market_buy_uses_exact_quote_quantity_and_full_response() {
        let params = market_buy_params("WLDUSDC", Decimal::new(100, 0), "rustval123B").unwrap();

        assert_eq!(params["quoteOrderQty"], "100");
        assert_eq!(params["newOrderRespType"], "FULL");
        assert_eq!(params["type"], "MARKET");
    }

    #[test]
    fn market_buy_can_use_exact_base_quantity_and_full_response() {
        let params =
            market_buy_quantity_params("WLDUSDC", Decimal::new(245, 1), "rustval123B").unwrap();

        assert_eq!(params["quantity"], "24.5");
        assert_eq!(params["newOrderRespType"], "FULL");
        assert_eq!(params["type"], "MARKET");
    }

    #[test]
    fn market_sell_uses_exact_base_quantity_and_full_response() {
        let params = market_sell_params("WLDUSDC", Decimal::new(245, 1), "rustval123S").unwrap();

        assert_eq!(params["quantity"], "24.5");
        assert_eq!(params["newOrderRespType"], "FULL");
        assert_eq!(params["type"], "MARKET");
    }

    #[test]
    fn limit_order_matches_rails_ioc_shape() {
        let params = limit_ioc_params(
            "WLDUSDC",
            "BUY",
            Decimal::new(261, 1),
            Decimal::new(382, 3),
            "rustval123LB",
        )
        .unwrap();

        assert_eq!(params["quantity"], "26.1");
        assert_eq!(params["price"], "0.382");
        assert_eq!(params["timeInForce"], "IOC");
        assert_eq!(params["newOrderRespType"], "FULL");
        assert_eq!(params["type"], "LIMIT");
    }

    #[test]
    fn computes_precise_buy_balance_changes_and_aggregates_third_asset_commission() {
        let order = order(
            "BUY",
            "100.123456789",
            "250.987654321",
            serde_json::json!([fill("0.000000001", "BNB", 1), fill("0.000000002", "BNB", 2)]),
        );
        let changes = order.balance_changes("WLD", "USDC").unwrap();

        assert_eq!(
            changes["WLD"],
            Decimal::from_str_exact("100.123456789").unwrap()
        );
        assert_eq!(
            changes["USDC"],
            Decimal::from_str_exact("-250.987654321").unwrap()
        );
        assert_eq!(
            changes["BNB"],
            Decimal::from_str_exact("-0.000000003").unwrap()
        );
    }

    #[test]
    fn subtracts_commission_from_received_or_spent_trading_asset() {
        let buy = order(
            "BUY",
            "100",
            "250",
            serde_json::json!([fill("0.1", "WLD", 1), fill("0.25", "USDC", 2)]),
        );
        let buy_changes = buy.balance_changes("WLD", "USDC").unwrap();
        assert_eq!(buy_changes["WLD"], Decimal::from_str_exact("99.9").unwrap());
        assert_eq!(
            buy_changes["USDC"],
            Decimal::from_str_exact("-250.25").unwrap()
        );

        let sell = order(
            "SELL",
            "31.10",
            "19.9662",
            serde_json::json!([fill("0.01896789", "USDC", 1), fill("0.1", "WLD", 2)]),
        );
        let sell_changes = sell.balance_changes("WLD", "USDC").unwrap();
        assert_eq!(
            sell_changes["USDC"],
            Decimal::from_str_exact("19.94723211").unwrap()
        );
        assert_eq!(
            sell_changes["WLD"],
            Decimal::from_str_exact("-31.20").unwrap()
        );
    }

    #[test]
    fn zero_commission_does_not_create_an_unrelated_balance_entry() {
        let order = order("BUY", "1", "2", serde_json::json!([fill("0", "BNB", 1)]));
        let changes = order.balance_changes("WLD", "USDC").unwrap();

        assert!(!changes.contains_key("BNB"));
        assert_eq!(changes["WLD"], Decimal::ONE);
        assert_eq!(changes["USDC"], Decimal::from(-2));
    }

    #[test]
    fn rejects_unknown_side_negative_quantities_and_negative_commission() {
        assert!(
            order("UNKNOWN", "1", "2", serde_json::json!([]))
                .balance_changes("WLD", "USDC")
                .is_err()
        );
        assert!(
            order("BUY", "-1", "2", serde_json::json!([]))
                .balance_changes("WLD", "USDC")
                .is_err()
        );
        assert!(
            order("BUY", "1", "2", serde_json::json!([fill("-0.1", "BNB", 1)]))
                .balance_changes("WLD", "USDC")
                .is_err()
        );
    }

    #[test]
    fn rejects_malformed_order_result_numbers_and_missing_required_fields() {
        let malformed = serde_json::json!({
            "symbol": "WLDUSDC",
            "orderId": 1,
            "clientOrderId": "rust-test",
            "price": "not-a-number",
            "origQty": "1",
            "executedQty": "1",
            "origQuoteOrderQty": "2",
            "cummulativeQuoteQty": "2",
            "status": "FILLED",
            "timeInForce": "GTC",
            "type": "MARKET",
            "side": "BUY"
        });
        assert!(serde_json::from_value::<OrderResult>(malformed).is_err());

        let missing_side = serde_json::json!({
            "symbol": "WLDUSDC",
            "orderId": 1,
            "clientOrderId": "rust-test",
            "price": "0",
            "origQty": "1",
            "executedQty": "1",
            "origQuoteOrderQty": "2",
            "cummulativeQuoteQty": "2",
            "status": "FILLED",
            "timeInForce": "GTC",
            "type": "MARKET"
        });
        assert!(serde_json::from_value::<OrderResult>(missing_side).is_err());
    }
}

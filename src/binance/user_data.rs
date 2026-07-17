use std::{fmt, str::FromStr, time::Duration};

use anyhow::{Context, ensure};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::{
    binance::{
        account::BinanceCredentials,
        ws_api::{
            OrderResult, WsApiError, limit_ioc_params, market_buy_params, market_sell_params,
            validate_client_order_id, validate_symbol,
        },
    },
    config::AppConfig,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(12);
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(12);
const RECV_WINDOW_MS: u64 = 5_000;
const COMMAND_CAPACITY: usize = 64;
const EVENT_CAPACITY: usize = 1_024;

type BinanceSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

pub struct UserDataStream {
    subscription_id: u64,
    receiver: mpsc::Receiver<anyhow::Result<UserDataEvent>>,
    api: MultiplexedBinanceWsApi,
}

#[derive(Clone)]
pub struct MultiplexedBinanceWsApi {
    sender: mpsc::Sender<ApiCommand>,
}

struct ApiCommand {
    method: &'static str,
    params: Map<String, Value>,
    response: oneshot::Sender<Result<Value, WsApiError>>,
}

impl UserDataStream {
    pub async fn connect(config: &AppConfig, clock_offset_ms: i64) -> anyhow::Result<Self> {
        let credentials = BinanceCredentials::from_env()?;
        let (mut socket, response) =
            tokio::time::timeout(CONNECT_TIMEOUT, connect_async(&config.binance_ws_api_url))
                .await
                .context("Binance User Data Stream connect timed out")??;
        ensure!(
            response.status().as_u16() == 101,
            "Binance User Data Stream upgrade returned {}",
            response.status()
        );

        let timestamp = apply_clock_offset(unix_timestamp_ms()?, clock_offset_ms)?;
        let mut parameters = Map::new();
        parameters.insert(
            "apiKey".to_owned(),
            Value::String(credentials.api_key().to_owned()),
        );
        parameters.insert("recvWindow".to_owned(), Value::from(RECV_WINDOW_MS));
        parameters.insert("timestamp".to_owned(), Value::from(timestamp));
        let signature = credentials.sign(&signature_payload(&parameters)?)?;
        parameters.insert("signature".to_owned(), Value::String(signature));
        socket
            .send(Message::Text(
                json!({
                    "id": 1,
                    "method": "userDataStream.subscribe.signature",
                    "params": parameters,
                })
                .to_string()
                .into(),
            ))
            .await
            .context("Binance User Data Stream subscribe send failed")?;

        let (subscription_id, pending) = tokio::time::timeout(SUBSCRIBE_TIMEOUT, async {
            let mut pending = Vec::new();
            loop {
                let payload = next_payload(&mut socket).await?;
                let envelope: WireEnvelope = serde_json::from_slice(&payload)
                    .context("invalid Binance User Data Stream envelope")?;
                if envelope.id.is_none() {
                    pending.push(payload);
                    continue;
                }
                ensure!(
                    envelope.id == Some(1),
                    "mismatched User Data Stream response ID"
                );
                ensure!(
                    envelope.status == Some(200),
                    "Binance User Data Stream subscription was rejected"
                );
                let result: SubscriptionResult = serde_json::from_value(
                    envelope
                        .result
                        .context("User Data Stream response omitted result")?,
                )
                .context("invalid User Data Stream subscription result")?;
                return Ok::<_, anyhow::Error>((result.subscription_id, pending));
            }
        })
        .await
        .context("Binance User Data Stream subscribe timed out")??;

        let (command_sender, command_receiver) = mpsc::channel(COMMAND_CAPACITY);
        let (event_sender, event_receiver) = mpsc::channel(EVENT_CAPACITY);
        for payload in pending {
            let event = parse_user_data_event(&payload, subscription_id)?;
            event_sender
                .try_send(Ok(event))
                .context("initial Binance User Data Stream event buffer is full")?;
        }
        tokio::spawn(run_multiplexed_session(
            socket,
            credentials,
            clock_offset_ms,
            subscription_id,
            command_receiver,
            event_sender,
        ));

        Ok(Self {
            subscription_id,
            receiver: event_receiver,
            api: MultiplexedBinanceWsApi {
                sender: command_sender,
            },
        })
    }

    pub fn subscription_id(&self) -> u64 {
        self.subscription_id
    }

    pub fn api(&self) -> MultiplexedBinanceWsApi {
        self.api.clone()
    }

    pub async fn next_event(&mut self) -> anyhow::Result<UserDataEvent> {
        self.receiver
            .recv()
            .await
            .context("multiplexed Binance User Data Stream stopped")?
    }
}

impl MultiplexedBinanceWsApi {
    pub async fn place_market_buy(
        &self,
        symbol: &str,
        quote_order_qty: Decimal,
        client_order_id: &str,
    ) -> Result<OrderResult, WsApiError> {
        self.call(
            "order.place",
            market_buy_params(symbol, quote_order_qty, client_order_id)?,
        )
        .await
    }

    pub async fn place_market_sell(
        &self,
        symbol: &str,
        quantity: Decimal,
        client_order_id: &str,
    ) -> Result<OrderResult, WsApiError> {
        self.call(
            "order.place",
            market_sell_params(symbol, quantity, client_order_id)?,
        )
        .await
    }

    pub async fn place_limit_ioc(
        &self,
        symbol: &str,
        side: &str,
        quantity: Decimal,
        price: Decimal,
        client_order_id: &str,
    ) -> Result<OrderResult, WsApiError> {
        self.call(
            "order.place",
            limit_ioc_params(symbol, side, quantity, price, client_order_id)?,
        )
        .await
    }

    pub async fn query_order(
        &self,
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
        self.call("order.status", params).await
    }

    async fn call<T>(
        &self,
        method: &'static str,
        params: Map<String, Value>,
    ) -> Result<T, WsApiError>
    where
        T: DeserializeOwned,
    {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(ApiCommand {
                method,
                params,
                response,
            })
            .await
            .map_err(|_| WsApiError::Transport("multiplexed session stopped".to_owned()))?;
        let value = receiver
            .await
            .map_err(|_| WsApiError::Transport("multiplexed response dropped".to_owned()))??;
        serde_json::from_value(value)
            .map_err(|_| WsApiError::Protocol("invalid response result".to_owned()))
    }
}

async fn run_multiplexed_session(
    mut socket: BinanceSocket,
    credentials: BinanceCredentials,
    clock_offset_ms: i64,
    subscription_id: u64,
    mut commands: mpsc::Receiver<ApiCommand>,
    events: mpsc::Sender<anyhow::Result<UserDataEvent>>,
) {
    let result = async {
        let mut next_request_id = 2_u64;
        loop {
            tokio::select! {
                command = commands.recv() => {
                    let Some(command) = command else {
                        return Ok(());
                    };
                    let request_id = next_request_id;
                    next_request_id = next_request_id.checked_add(1)
                        .context("Binance multiplexed request id overflow")?;
                    let response = execute_api_command(
                        &mut socket,
                        &credentials,
                        clock_offset_ms,
                        subscription_id,
                        request_id,
                        command.method,
                        command.params,
                        &events,
                    ).await;
                    let fatal = matches!(response, Err(WsApiError::Transport(_) | WsApiError::Protocol(_)));
                    let _ = command.response.send(response);
                    if fatal {
                        anyhow::bail!("multiplexed Binance request failed; process restart is required");
                    }
                }
                payload = next_payload(&mut socket) => {
                    let payload = payload?;
                    let envelope: WireEnvelope = serde_json::from_slice(&payload)
                        .context("invalid multiplexed Binance envelope")?;
                    ensure!(
                        envelope.id.is_none(),
                        "multiplexed Binance session received a response without a pending request"
                    );
                    let event = parse_user_data_event(&payload, subscription_id)?;
                    events.send(Ok(event)).await
                        .context("Binance User Data Stream consumer stopped")?;
                }
            }
        }
    }
    .await;

    if let Err(error) = result {
        let _ = events.send(Err(error)).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_api_command(
    socket: &mut BinanceSocket,
    credentials: &BinanceCredentials,
    clock_offset_ms: i64,
    subscription_id: u64,
    request_id: u64,
    method: &str,
    mut params: Map<String, Value>,
    events: &mpsc::Sender<anyhow::Result<UserDataEvent>>,
) -> Result<Value, WsApiError> {
    let timestamp = apply_clock_offset(unix_timestamp_ms_ws()?, clock_offset_ms)
        .map_err(|error| WsApiError::Protocol(error.to_string()))?;
    params.insert(
        "apiKey".to_owned(),
        Value::String(credentials.api_key().to_owned()),
    );
    params.insert("recvWindow".to_owned(), Value::from(RECV_WINDOW_MS));
    params.insert("timestamp".to_owned(), Value::from(timestamp));
    let signature = credentials
        .sign(&signature_payload_ws(&params)?)
        .map_err(|error| WsApiError::Protocol(error.to_string()))?;
    params.insert("signature".to_owned(), Value::String(signature));
    socket
        .send(Message::Text(
            json!({"id": request_id, "method": method, "params": params})
                .to_string()
                .into(),
        ))
        .await
        .map_err(|_| WsApiError::Transport("request send failed".to_owned()))?;

    loop {
        let payload = tokio::time::timeout(RESPONSE_TIMEOUT, next_payload(socket))
            .await
            .map_err(|_| WsApiError::Transport("response timed out".to_owned()))?
            .map_err(|_| WsApiError::Transport("response read failed".to_owned()))?;
        let envelope: WireEnvelope = serde_json::from_slice(&payload)
            .map_err(|_| WsApiError::Protocol("invalid response JSON".to_owned()))?;
        let Some(response_id) = envelope.id else {
            let event = parse_user_data_event(&payload, subscription_id)
                .map_err(|error| WsApiError::Protocol(error.to_string()))?;
            events.send(Ok(event)).await.map_err(|_| {
                WsApiError::Transport("User Data Stream consumer stopped".to_owned())
            })?;
            continue;
        };
        if response_id != request_id {
            return Err(WsApiError::Protocol(format!(
                "response id {response_id} does not match request {request_id}"
            )));
        }
        let status = envelope
            .status
            .ok_or_else(|| WsApiError::Protocol("response omitted status".to_owned()))?;
        if status != 200 {
            let error = envelope.error.unwrap_or(WireError {
                code: i64::from(status),
                message: "request rejected without Binance error body".to_owned(),
            });
            return Err(WsApiError::Rejected {
                status,
                code: error.code,
                message: sanitize_message(&error.message),
            });
        }
        return envelope
            .result
            .ok_or_else(|| WsApiError::Protocol("successful response has no result".to_owned()));
    }
}

fn signature_payload_ws(params: &Map<String, Value>) -> Result<String, WsApiError> {
    let mut sorted = params.iter().collect::<Vec<_>>();
    sorted.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    sorted
        .into_iter()
        .filter(|(name, _)| name.as_str() != "signature")
        .map(|(name, value)| match value {
            Value::String(value) => Ok(format!("{name}={value}")),
            Value::Number(value) => Ok(format!("{name}={value}")),
            Value::Bool(value) => Ok(format!("{name}={value}")),
            _ => Err(WsApiError::Protocol(format!(
                "unsupported signed parameter {name}"
            ))),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|parts| parts.join("&"))
}

fn unix_timestamp_ms_ws() -> Result<u64, WsApiError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| WsApiError::Protocol("system clock is before Unix epoch".to_owned()))?
        .as_millis()
        .try_into()
        .map_err(|_| WsApiError::Protocol("timestamp overflow".to_owned()))
}

fn sanitize_message(message: &str) -> String {
    message
        .chars()
        .filter(|character| !character.is_control())
        .take(512)
        .collect()
}

#[derive(Clone, Debug, PartialEq)]
pub enum UserDataEvent {
    AccountPosition(AccountPositionEvent),
    BalanceUpdate(BalanceUpdateEvent),
    ExecutionReport(Box<ExecutionReportEvent>),
    StreamTerminated {
        event_time_ms: u64,
    },
    Other {
        event_type: String,
        event_time_ms: u64,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct AccountPositionEvent {
    pub event_time_ms: u64,
    pub last_account_update_ms: u64,
    pub balances: Vec<UserBalance>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UserBalance {
    pub asset: String,
    pub free: Decimal,
    pub locked: Decimal,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BalanceUpdateEvent {
    pub event_time_ms: u64,
    pub asset: String,
    pub delta: Decimal,
    pub clear_time_ms: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExecutionReportEvent {
    pub event_time_ms: u64,
    pub transaction_time_ms: u64,
    pub symbol: String,
    pub client_order_id: String,
    pub side: String,
    pub order_type: String,
    pub execution_type: String,
    pub order_status: String,
    pub reject_reason: String,
    pub order_id: u64,
    pub last_executed_quantity: Decimal,
    pub cumulative_filled_quantity: Decimal,
    pub last_executed_price: Decimal,
    pub commission: Decimal,
    pub commission_asset: Option<String>,
    pub trade_id: i64,
}

pub fn parse_user_data_event(
    payload: &[u8],
    expected_subscription_id: u64,
) -> anyhow::Result<UserDataEvent> {
    let wrapper: WireUserEvent =
        serde_json::from_slice(payload).context("invalid Binance User Data Stream event JSON")?;
    ensure!(
        wrapper.subscription_id == expected_subscription_id,
        "unexpected Binance User Data Stream subscription ID"
    );
    let event_type = wrapper
        .event
        .get("e")
        .and_then(Value::as_str)
        .context("Binance User Data Stream event omitted type")?;
    match event_type {
        "outboundAccountPosition" => {
            let event: WireAccountPosition = serde_json::from_value(wrapper.event)
                .context("invalid outboundAccountPosition event")?;
            Ok(UserDataEvent::AccountPosition(AccountPositionEvent {
                event_time_ms: event.event_time_ms,
                last_account_update_ms: event.last_account_update_ms,
                balances: event
                    .balances
                    .into_iter()
                    .map(|balance| {
                        Ok(UserBalance {
                            asset: balance.asset,
                            free: Decimal::from_str(&balance.free)
                                .context("invalid User Data Stream free balance")?,
                            locked: Decimal::from_str(&balance.locked)
                                .context("invalid User Data Stream locked balance")?,
                        })
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?,
            }))
        }
        "balanceUpdate" => {
            let event: WireBalanceUpdate =
                serde_json::from_value(wrapper.event).context("invalid balanceUpdate event")?;
            Ok(UserDataEvent::BalanceUpdate(BalanceUpdateEvent {
                event_time_ms: event.event_time_ms,
                asset: event.asset,
                delta: Decimal::from_str(&event.delta)
                    .context("invalid User Data Stream balance delta")?,
                clear_time_ms: event.clear_time_ms,
            }))
        }
        "executionReport" => {
            let event: WireExecutionReport =
                serde_json::from_value(wrapper.event).context("invalid executionReport event")?;
            Ok(UserDataEvent::ExecutionReport(Box::new(
                ExecutionReportEvent {
                    event_time_ms: event.event_time_ms,
                    transaction_time_ms: event.transaction_time_ms,
                    symbol: event.symbol,
                    client_order_id: event.client_order_id,
                    side: event.side,
                    order_type: event.order_type,
                    execution_type: event.execution_type,
                    order_status: event.order_status,
                    reject_reason: event.reject_reason,
                    order_id: event.order_id,
                    last_executed_quantity: parse_decimal(
                        &event.last_executed_quantity,
                        "last quantity",
                    )?,
                    cumulative_filled_quantity: parse_decimal(
                        &event.cumulative_filled_quantity,
                        "cumulative quantity",
                    )?,
                    last_executed_price: parse_decimal(&event.last_executed_price, "last price")?,
                    commission: parse_decimal(&event.commission, "commission")?,
                    commission_asset: event.commission_asset,
                    trade_id: event.trade_id,
                },
            )))
        }
        "eventStreamTerminated" => {
            let event_time_ms = event_time(&wrapper.event)?;
            Ok(UserDataEvent::StreamTerminated { event_time_ms })
        }
        other => Ok(UserDataEvent::Other {
            event_type: other.to_owned(),
            event_time_ms: event_time(&wrapper.event)?,
        }),
    }
}

async fn next_payload(socket: &mut BinanceSocket) -> anyhow::Result<Vec<u8>> {
    loop {
        let message = socket
            .next()
            .await
            .context("Binance User Data Stream ended")??;
        match message {
            Message::Text(payload) => return Ok(payload.as_bytes().to_vec()),
            Message::Binary(payload) => return Ok(payload.to_vec()),
            Message::Ping(payload) => socket
                .send(Message::Pong(payload))
                .await
                .context("Binance User Data Stream pong failed")?,
            Message::Pong(_) | Message::Frame(_) => {}
            Message::Close(_) => anyhow::bail!("Binance User Data Stream closed"),
        }
    }
}

fn signature_payload(parameters: &Map<String, Value>) -> anyhow::Result<String> {
    let mut sorted = parameters.iter().collect::<Vec<_>>();
    sorted.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    sorted
        .into_iter()
        .map(|(name, value)| {
            let value = match value {
                Value::String(value) => value.clone(),
                Value::Number(value) => value.to_string(),
                _ => anyhow::bail!("unsupported User Data Stream signature parameter"),
            };
            Ok(format!("{name}={value}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()
        .map(|parts| parts.join("&"))
}

fn parse_decimal(value: &str, field: &str) -> anyhow::Result<Decimal> {
    Decimal::from_str(value).with_context(|| format!("invalid executionReport {field}"))
}

fn event_time(event: &Value) -> anyhow::Result<u64> {
    event
        .get("E")
        .and_then(Value::as_u64)
        .context("User Data Stream event omitted event time")
}

fn unix_timestamp_ms() -> anyhow::Result<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_millis()
        .try_into()
        .context("Unix timestamp does not fit into u64")
}

fn apply_clock_offset(timestamp: u64, offset: i64) -> anyhow::Result<u64> {
    (i128::from(timestamp) + i128::from(offset))
        .try_into()
        .context("adjusted Binance timestamp does not fit into u64")
}

impl fmt::Debug for UserDataStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UserDataStream")
            .field("subscription_id", &self.subscription_id)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
struct WireEnvelope {
    id: Option<u64>,
    status: Option<u16>,
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
struct SubscriptionResult {
    subscription_id: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireUserEvent {
    subscription_id: u64,
    event: Value,
}

#[derive(Deserialize)]
struct WireAccountPosition {
    #[serde(rename = "E")]
    event_time_ms: u64,
    #[serde(rename = "u")]
    last_account_update_ms: u64,
    #[serde(rename = "B")]
    balances: Vec<WireBalance>,
}

#[derive(Deserialize)]
struct WireBalance {
    #[serde(rename = "a")]
    asset: String,
    #[serde(rename = "f")]
    free: String,
    #[serde(rename = "l")]
    locked: String,
}

#[derive(Deserialize)]
struct WireBalanceUpdate {
    #[serde(rename = "E")]
    event_time_ms: u64,
    #[serde(rename = "a")]
    asset: String,
    #[serde(rename = "d")]
    delta: String,
    #[serde(rename = "T")]
    clear_time_ms: u64,
}

#[derive(Deserialize)]
struct WireExecutionReport {
    #[serde(rename = "E")]
    event_time_ms: u64,
    #[serde(rename = "T")]
    transaction_time_ms: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "c")]
    client_order_id: String,
    #[serde(rename = "S")]
    side: String,
    #[serde(rename = "o")]
    order_type: String,
    #[serde(rename = "x")]
    execution_type: String,
    #[serde(rename = "X")]
    order_status: String,
    #[serde(rename = "r")]
    reject_reason: String,
    #[serde(rename = "i")]
    order_id: u64,
    #[serde(rename = "l")]
    last_executed_quantity: String,
    #[serde(rename = "z")]
    cumulative_filled_quantity: String,
    #[serde(rename = "L")]
    last_executed_price: String,
    #[serde(rename = "n")]
    commission: String,
    #[serde(rename = "N")]
    commission_asset: Option<String>,
    #[serde(rename = "t")]
    trade_id: i64,
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rust_decimal::Decimal;

    use super::{UserDataEvent, parse_user_data_event};

    #[test]
    fn parses_account_position_exactly() {
        let event = parse_user_data_event(
            br#"{"subscriptionId":7,"event":{"e":"outboundAccountPosition","E":1564034571105,"u":1564034571073,"B":[{"a":"WLD","f":"100.123456789012345678","l":"2.5"}]}}"#,
            7,
        )
        .unwrap();
        let UserDataEvent::AccountPosition(position) = event else {
            panic!("wrong event")
        };
        assert_eq!(
            position.balances[0].free,
            Decimal::from_str("100.123456789012345678").unwrap()
        );
        assert_eq!(position.balances[0].locked, Decimal::new(25, 1));
    }

    #[test]
    fn parses_execution_report_fill_and_commission() {
        let event = parse_user_data_event(
            br#"{"subscriptionId":0,"event":{"e":"executionReport","E":1499405658658,"s":"WLDUSDC","c":"rustarb1L","S":"SELL","o":"LIMIT","x":"TRADE","X":"PARTIALLY_FILLED","r":"NONE","i":4293153,"l":"1.2","z":"1.2","L":"0.812","n":"0.000972","N":"USDC","T":1499405658657,"t":12345}}"#,
            0,
        )
        .unwrap();
        let UserDataEvent::ExecutionReport(report) = event else {
            panic!("wrong event")
        };
        assert_eq!(report.client_order_id, "rustarb1L");
        assert_eq!(report.order_status, "PARTIALLY_FILLED");
        assert_eq!(report.last_executed_quantity, Decimal::new(12, 1));
        assert_eq!(report.commission, Decimal::new(972, 6));
        assert_eq!(report.commission_asset.as_deref(), Some("USDC"));
    }

    #[test]
    fn rejects_event_for_another_subscription() {
        assert!(
            parse_user_data_event(
                br#"{"subscriptionId":2,"event":{"e":"eventStreamTerminated","E":1}}"#,
                1,
            )
            .is_err()
        );
    }
}

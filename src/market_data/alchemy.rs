use std::{collections::HashMap, time::Instant};

use alloy_primitives::B256;
use anyhow::{Context, anyhow, bail, ensure};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_tungstenite::{WebSocketStream, connect_async, tungstenite::Message};

use crate::chain::{
    logs::{ChainLog, EthLogFilter, WireChainLog, parse_quantity},
    rpc::CanonicalBlock,
};

#[derive(Debug)]
pub enum DexStreamEvent {
    Log {
        log: ChainLog,
        received_at: Instant,
    },
    Head {
        head: CanonicalBlock,
        received_at: Instant,
    },
}

pub struct AlchemyDexStream {
    pub receiver: mpsc::Receiver<DexStreamEvent>,
    pub task: JoinHandle<anyhow::Result<()>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubscriptionKind {
    Logs,
    NewHeads,
}

pub async fn connect_dex_stream(
    endpoint: &str,
    filters: &[EthLogFilter],
    channel_capacity: usize,
) -> anyhow::Result<AlchemyDexStream> {
    ensure!(channel_capacity > 0, "DEX event channel capacity is zero");
    ensure!(!filters.is_empty(), "Alchemy log filter set is empty");
    let parsed = reqwest::Url::parse(endpoint).context("Alchemy WSS endpoint is invalid")?;
    ensure!(parsed.scheme() == "wss", "Alchemy endpoint must use WSS");
    ensure!(
        parsed.host_str().is_some(),
        "Alchemy WSS endpoint has no host"
    );

    let (mut socket, _) = connect_async(endpoint)
        .await
        .map_err(|_| anyhow!("Alchemy WSS connection failed"))?;
    let mut pending = HashMap::with_capacity(filters.len() + 1);
    for (index, filter) in filters.iter().enumerate() {
        let id = u64::try_from(index + 1).expect("subscription count fits u64");
        pending.insert(id, SubscriptionKind::Logs);
        socket
            .send(Message::Text(
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "method": "eth_subscribe",
                    "params": ["logs", filter.subscription_json()],
                })
                .to_string()
                .into(),
            ))
            .await
            .map_err(|_| anyhow!("failed to send Alchemy log subscription"))?;
    }
    let head_id = u64::try_from(filters.len() + 1).expect("subscription count fits u64");
    pending.insert(head_id, SubscriptionKind::NewHeads);
    socket
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": head_id,
                "method": "eth_subscribe",
                "params": ["newHeads"],
            })
            .to_string()
            .into(),
        ))
        .await
        .map_err(|_| anyhow!("failed to send Alchemy head subscription"))?;

    let mut subscriptions = HashMap::with_capacity(pending.len());
    while !pending.is_empty() {
        let message = socket
            .next()
            .await
            .context("Alchemy WSS ended during subscription")?
            .map_err(|_| anyhow!("Alchemy WSS failed during subscription"))?;
        match message {
            Message::Text(payload) => {
                accept_subscription_response(payload.as_bytes(), &mut pending, &mut subscriptions)?;
            }
            Message::Binary(payload) => {
                accept_subscription_response(payload.as_ref(), &mut pending, &mut subscriptions)?;
            }
            Message::Ping(payload) => socket
                .send(Message::Pong(payload))
                .await
                .map_err(|_| anyhow!("failed to send Alchemy pong"))?,
            Message::Pong(_) => {}
            Message::Close(_) => bail!("Alchemy closed WSS during subscription"),
            Message::Frame(_) => {}
        }
    }

    let (sender, receiver) = mpsc::channel(channel_capacity);
    let task = tokio::spawn(run_stream(socket, subscriptions, sender));
    Ok(AlchemyDexStream { receiver, task })
}

fn accept_subscription_response(
    payload: &[u8],
    pending: &mut HashMap<u64, SubscriptionKind>,
    subscriptions: &mut HashMap<String, SubscriptionKind>,
) -> anyhow::Result<()> {
    let value: Value = serde_json::from_slice(payload).context("invalid Alchemy WSS JSON")?;
    let Some(id) = value.get("id").and_then(Value::as_u64) else {
        // Notifications received before every acknowledgement are covered by
        // the HTTP backfill captured after the final acknowledgement.
        return Ok(());
    };
    let kind = pending
        .remove(&id)
        .with_context(|| format!("unexpected Alchemy subscription response id {id}"))?;
    if let Some(error) = value.get("error") {
        let code = error
            .get("code")
            .and_then(Value::as_i64)
            .unwrap_or_default();
        bail!("Alchemy subscription failed with JSON-RPC code {code}");
    }
    let subscription = value
        .get("result")
        .and_then(Value::as_str)
        .context("Alchemy subscription response has no id")?;
    ensure!(
        subscriptions
            .insert(subscription.to_owned(), kind)
            .is_none(),
        "Alchemy returned a duplicate subscription id"
    );
    Ok(())
}

async fn run_stream<S>(
    mut socket: WebSocketStream<S>,
    subscriptions: HashMap<String, SubscriptionKind>,
    sender: mpsc::Sender<DexStreamEvent>,
) -> anyhow::Result<()>
where
    WebSocketStream<S>: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
        + futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error>
        + Unpin,
{
    while let Some(message) = socket.next().await {
        let message = message.map_err(|_| anyhow!("Alchemy WSS stream failed"))?;
        match message {
            Message::Text(payload) => {
                forward_notification(payload.as_bytes(), &subscriptions, &sender)?;
            }
            Message::Binary(payload) => {
                forward_notification(payload.as_ref(), &subscriptions, &sender)?;
            }
            Message::Ping(payload) => socket
                .send(Message::Pong(payload))
                .await
                .map_err(|_| anyhow!("failed to send Alchemy pong"))?,
            Message::Pong(_) => {}
            Message::Close(_) => bail!("Alchemy closed WSS stream"),
            Message::Frame(_) => {}
        }
    }
    bail!("Alchemy WSS stream ended")
}

fn forward_notification(
    payload: &[u8],
    subscriptions: &HashMap<String, SubscriptionKind>,
    sender: &mpsc::Sender<DexStreamEvent>,
) -> anyhow::Result<()> {
    let received_at = Instant::now();
    let notification: WireNotification =
        serde_json::from_slice(payload).context("invalid Alchemy notification")?;
    ensure!(
        notification.method == "eth_subscription",
        "unexpected Alchemy notification method"
    );
    let kind = subscriptions
        .get(&notification.params.subscription)
        .context("notification for unknown Alchemy subscription")?;
    let event = match kind {
        SubscriptionKind::Logs => {
            let wire: WireChainLog = serde_json::from_value(notification.params.result)
                .context("invalid Alchemy log notification")?;
            DexStreamEvent::Log {
                log: wire.try_into()?,
                received_at,
            }
        }
        SubscriptionKind::NewHeads => {
            let wire: WireHead = serde_json::from_value(notification.params.result)
                .context("invalid Alchemy newHeads notification")?;
            DexStreamEvent::Head {
                head: wire.try_into()?,
                received_at,
            }
        }
    };
    sender
        .try_send(event)
        .map_err(|error| anyhow!("critical DEX event channel unavailable: {error}"))?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct WireNotification {
    method: String,
    params: WireNotificationParams,
}

#[derive(Debug, Deserialize)]
struct WireNotificationParams {
    subscription: String,
    result: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireHead {
    number: String,
    hash: String,
    parent_hash: String,
}

impl TryFrom<WireHead> for CanonicalBlock {
    type Error = anyhow::Error;

    fn try_from(value: WireHead) -> Result<Self, Self::Error> {
        Ok(Self {
            number: parse_quantity("head.number", &value.number)?,
            hash: value.hash.parse::<B256>().context("invalid head hash")?,
            parent_hash: value
                .parent_hash
                .parse::<B256>()
                .context("invalid head parentHash")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tokio::sync::mpsc;

    use super::{DexStreamEvent, SubscriptionKind, forward_notification};

    #[test]
    fn parses_new_head_notification() {
        let mut subscriptions = HashMap::new();
        subscriptions.insert("heads".into(), SubscriptionKind::NewHeads);
        let (sender, mut receiver) = mpsc::channel(1);
        forward_notification(
            br#"{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"heads","result":{"number":"0xa","hash":"0x000000000000000000000000000000000000000000000000000000000000000a","parentHash":"0x0000000000000000000000000000000000000000000000000000000000000009"}}}"#,
            &subscriptions,
            &sender,
        )
        .unwrap();
        match receiver.try_recv().unwrap() {
            DexStreamEvent::Head { head, .. } => assert_eq!(head.number, 10),
            DexStreamEvent::Log { .. } => panic!("expected head"),
        }
    }
}

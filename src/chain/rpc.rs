use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use alloy_primitives::{Address, B256, hex};
use anyhow::{Context, anyhow, ensure};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};

const DEFAULT_BATCH_SIZE: usize = 100;
const MAX_RATE_LIMIT_RETRIES: u32 = 6;
const BASE_RETRY_DELAY_MS: u64 = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanonicalBlock {
    pub number: u64,
    pub hash: B256,
    pub parent_hash: B256,
}

impl CanonicalBlock {
    fn eip1898(self) -> Value {
        json!({
            "blockHash": format!("{:#x}", self.hash),
            "requireCanonical": true,
        })
    }
}

#[derive(Debug, Clone)]
pub struct EthCall {
    pub to: Address,
    pub data: Vec<u8>,
}

impl EthCall {
    fn json(&self) -> Value {
        json!({
            "to": format!("{:#x}", self.to),
            "data": format!("0x{}", hex::encode(&self.data)),
        })
    }
}

/// Reusable JSON-RPC client for hydration and recovery only.
///
/// The endpoint is intentionally omitted from `Debug` and every error message:
/// Alchemy credentials are commonly embedded in its path.
pub struct JsonRpcClient {
    client: Client,
    endpoint: String,
    next_id: AtomicU64,
    batch_size: usize,
    http_requests: AtomicU64,
    eth_calls: AtomicU64,
    rate_limit_retries: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RpcStats {
    pub http_requests: u64,
    pub eth_calls: u64,
    pub rate_limit_retries: u64,
}

impl std::fmt::Debug for JsonRpcClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JsonRpcClient")
            .field("endpoint", &"<redacted>")
            .field("batch_size", &self.batch_size)
            .finish_non_exhaustive()
    }
}

impl JsonRpcClient {
    pub fn new(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        let endpoint = endpoint.into();
        let parsed =
            reqwest::Url::parse(&endpoint).context("RPC endpoint must be an absolute URL")?;
        ensure!(
            matches!(parsed.scheme(), "http" | "https"),
            "RPC endpoint must use HTTP or HTTPS"
        );
        ensure!(
            parsed.host_str().is_some(),
            "RPC endpoint must include a host"
        );

        let client = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(20))
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_nodelay(true)
            .build()
            .map_err(|error| sanitized_transport_error("build", &error))?;
        Ok(Self {
            client,
            endpoint,
            next_id: AtomicU64::new(1),
            batch_size: DEFAULT_BATCH_SIZE,
            http_requests: AtomicU64::new(0),
            eth_calls: AtomicU64::new(0),
            rate_limit_retries: AtomicU64::new(0),
        })
    }

    #[cfg(test)]
    fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub async fn latest_block(&self) -> anyhow::Result<CanonicalBlock> {
        let value = self
            .request("eth_getBlockByNumber", json!(["latest", false]))
            .await?;
        let block: RpcBlock = serde_json::from_value(value)
            .context("eth_getBlockByNumber returned an invalid block")?;
        Ok(CanonicalBlock {
            number: parse_quantity_u64("block.number", &block.number)?,
            hash: parse_b256("block.hash", &block.hash)?,
            parent_hash: parse_b256("block.parentHash", &block.parent_hash)?,
        })
    }

    pub async fn eth_call_batch(
        &self,
        calls: &[EthCall],
        block: CanonicalBlock,
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        self.eth_calls
            .fetch_add(calls.len() as u64, Ordering::Relaxed);
        let mut results = Vec::with_capacity(calls.len());
        for chunk in calls.chunks(self.batch_size) {
            let mut requests = Vec::with_capacity(chunk.len());
            let mut ids = Vec::with_capacity(chunk.len());
            for call in chunk {
                let id = self.next_id.fetch_add(1, Ordering::Relaxed);
                ids.push(id);
                requests.push(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "method": "eth_call",
                    "params": [call.json(), block.eip1898()],
                }));
            }

            let body = Value::Array(requests);
            let mut attempt = 0;
            let mut by_id = loop {
                let values = self.send_json(body.clone()).await?;
                let responses = values
                    .as_array()
                    .context("JSON-RPC batch response is not an array")?;
                let mut decoded_by_id = HashMap::with_capacity(responses.len());
                let mut rate_limited = false;
                for response in responses {
                    let decoded: RpcResponse = serde_json::from_value(response.clone())
                        .context("invalid JSON-RPC batch response item")?;
                    rate_limited |= decoded
                        .error
                        .as_ref()
                        .is_some_and(|error| error.code == 429);
                    ensure!(
                        decoded_by_id.insert(decoded.id, decoded).is_none(),
                        "duplicate JSON-RPC response id"
                    );
                }
                if rate_limited && attempt < MAX_RATE_LIMIT_RETRIES {
                    self.rate_limit_retries.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(retry_delay(attempt)).await;
                    attempt += 1;
                    continue;
                }
                break decoded_by_id;
            };

            for id in ids {
                let response = by_id
                    .remove(&id)
                    .with_context(|| format!("missing JSON-RPC response id {id}"))?;
                results.push(decode_call_result(response)?);
            }
        }
        Ok(results)
    }

    async fn request(&self, method: &str, params: Value) -> anyhow::Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let response = self
            .send_json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": params,
            }))
            .await?;
        let decoded: RpcResponse =
            serde_json::from_value(response).context("invalid JSON-RPC response")?;
        ensure!(decoded.id == id, "JSON-RPC response id mismatch");
        decode_rpc_result(decoded)
    }

    async fn send_json(&self, body: Value) -> anyhow::Result<Value> {
        for attempt in 0..=MAX_RATE_LIMIT_RETRIES {
            self.http_requests.fetch_add(1, Ordering::Relaxed);
            let response = self
                .client
                .post(&self.endpoint)
                .json(&body)
                .send()
                .await
                .map_err(|error| sanitized_transport_error("send", &error))?;
            let status = response.status();
            if status == StatusCode::TOO_MANY_REQUESTS && attempt < MAX_RATE_LIMIT_RETRIES {
                self.rate_limit_retries.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(retry_delay(attempt)).await;
                continue;
            }
            ensure_success(status)?;
            return response
                .json::<Value>()
                .await
                .map_err(|error| sanitized_transport_error("decode", &error));
        }
        unreachable!("bounded RPC retry loop always returns on its final attempt")
    }

    pub fn stats(&self) -> RpcStats {
        RpcStats {
            http_requests: self.http_requests.load(Ordering::Relaxed),
            eth_calls: self.eth_calls.load(Ordering::Relaxed),
            rate_limit_retries: self.rate_limit_retries.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Deserialize)]
struct RpcBlock {
    number: String,
    hash: String,
    #[serde(rename = "parentHash")]
    parent_hash: String,
}

#[derive(Debug, Deserialize)]
struct RpcResponse {
    id: u64,
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<RpcError>,
}

#[derive(Debug, Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

fn decode_rpc_result(response: RpcResponse) -> anyhow::Result<Value> {
    if let Some(error) = response.error {
        return Err(anyhow!(
            "JSON-RPC error {}: {}",
            error.code,
            sanitize_rpc_message(&error.message)
        ));
    }
    response.result.context("JSON-RPC response has no result")
}

fn decode_call_result(response: RpcResponse) -> anyhow::Result<Vec<u8>> {
    let value = decode_rpc_result(response)?;
    let encoded = value
        .as_str()
        .context("eth_call result is not a hex string")?;
    parse_data_hex("eth_call result", encoded)
}

fn parse_data_hex(name: &str, value: &str) -> anyhow::Result<Vec<u8>> {
    let encoded = value
        .strip_prefix("0x")
        .with_context(|| format!("{name} is missing 0x prefix"))?;
    ensure!(encoded.len() % 2 == 0, "{name} has odd hex length");
    hex::decode(encoded).with_context(|| format!("{name} contains invalid hex"))
}

fn parse_quantity_u64(name: &str, value: &str) -> anyhow::Result<u64> {
    let encoded = value
        .strip_prefix("0x")
        .with_context(|| format!("{name} is missing 0x prefix"))?;
    ensure!(!encoded.is_empty(), "{name} is empty");
    u64::from_str_radix(encoded, 16).with_context(|| format!("{name} is invalid"))
}

fn parse_b256(name: &str, value: &str) -> anyhow::Result<B256> {
    value.parse().with_context(|| format!("{name} is invalid"))
}

fn ensure_success(status: StatusCode) -> anyhow::Result<()> {
    ensure!(
        status.is_success(),
        "RPC HTTP request failed with status {}",
        status.as_u16()
    );
    Ok(())
}

fn sanitized_transport_error(operation: &str, error: &reqwest::Error) -> anyhow::Error {
    let kind = if error.is_timeout() {
        "timeout"
    } else if error.is_connect() {
        "connect"
    } else if error.is_decode() {
        "decode"
    } else if error.is_body() {
        "body"
    } else if error.is_request() {
        "request"
    } else {
        "unknown"
    };
    anyhow!("RPC HTTP {operation} failed ({kind})")
}

fn sanitize_rpc_message(message: &str) -> String {
    const MAX_CHARS: usize = 256;
    message
        .chars()
        .filter(|character| !character.is_control())
        .take(MAX_CHARS)
        .collect()
}

fn retry_delay(attempt: u32) -> Duration {
    Duration::from_millis(BASE_RETRY_DELAY_MS.saturating_mul(1_u64 << attempt.min(4)))
}

#[cfg(test)]
mod tests {
    use super::{JsonRpcClient, parse_data_hex, parse_quantity_u64, sanitize_rpc_message};

    #[test]
    fn endpoint_is_redacted_from_debug() {
        let client = JsonRpcClient::new("https://example.invalid/v2/private-key")
            .unwrap()
            .with_batch_size(7);
        let debug = format!("{client:?}");
        assert!(!debug.contains("private-key"));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn parses_rpc_quantities_and_data() {
        assert_eq!(
            parse_quantity_u64("number", "0x1ee7069").unwrap(),
            32_403_561
        );
        assert_eq!(parse_data_hex("data", "0x00ff").unwrap(), [0, 255]);
        assert!(parse_data_hex("data", "ff").is_err());
    }

    #[test]
    fn rpc_messages_are_bounded_and_control_free() {
        let message = format!("bad\n{}", "x".repeat(1_000));
        let sanitized = sanitize_rpc_message(&message);
        assert!(!sanitized.contains('\n'));
        assert_eq!(sanitized.chars().count(), 256);
    }
}

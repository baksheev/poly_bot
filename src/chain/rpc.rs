use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use alloy_primitives::{Address, B256, U256, hex};
use anyhow::{Context, anyhow, ensure};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::chain::logs::{ChainLog, EthLogFilter, WireChainLog};

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
    pub(crate) fn eip1898(self) -> Value {
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

#[derive(Debug, Clone)]
pub struct TransactionCall {
    pub from: Address,
    pub to: Address,
    pub data: Vec<u8>,
    pub value: U256,
}

impl TransactionCall {
    fn json(&self) -> Value {
        json!({
            "from": format!("{:#x}", self.from),
            "to": format!("{:#x}", self.to),
            "data": format!("0x{}", hex::encode(&self.data)),
            "value": format!("{:#x}", self.value),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionReceipt {
    pub transaction_hash: B256,
    pub block_number: u64,
    pub status: u64,
    pub gas_used: u64,
    pub effective_gas_price: u128,
    pub logs: Vec<ReceiptLog>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiptLog {
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Vec<u8>,
    /// Canonical position is present for RPC receipts and absent only in
    /// synthetic/unit-test receipts.
    pub position: Option<ReceiptLogPosition>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiptLogPosition {
    pub transaction_hash: B256,
    pub block_number: u64,
    pub block_hash: B256,
    pub transaction_index: u64,
    pub log_index: u64,
    pub removed: bool,
}

impl ReceiptLog {
    pub fn chain_log(&self) -> anyhow::Result<ChainLog> {
        let position = self
            .position
            .context("receipt log has no canonical position")?;
        Ok(ChainLog {
            address: self.address,
            topics: self.topics.clone(),
            data: self.data.clone(),
            block_number: position.block_number,
            block_hash: position.block_hash,
            transaction_index: position.transaction_index,
            log_index: position.log_index,
            removed: position.removed,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcTransaction {
    pub hash: B256,
    pub chain_id: u64,
    pub nonce: u64,
    pub from: Address,
    pub to: Option<Address>,
    pub value: U256,
    pub input: Vec<u8>,
    pub block_number: Option<u64>,
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

impl Clone for JsonRpcClient {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            endpoint: self.endpoint.clone(),
            next_id: AtomicU64::new(self.next_id.load(Ordering::Relaxed)),
            batch_size: self.batch_size,
            http_requests: AtomicU64::new(0),
            eth_calls: AtomicU64::new(0),
            rate_limit_retries: AtomicU64::new(0),
        }
    }
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

    pub async fn chain_id(&self) -> anyhow::Result<u64> {
        let value = self.request("eth_chainId", json!([])).await?;
        parse_quantity_value_u64("eth_chainId", value)
    }

    pub async fn native_balance(&self, address: Address) -> anyhow::Result<U256> {
        let value = self
            .request("eth_getBalance", json!([format!("{address:#x}"), "latest"]))
            .await?;
        parse_quantity_value_u256("eth_getBalance", value)
    }

    pub async fn native_balance_at(
        &self,
        address: Address,
        block: CanonicalBlock,
    ) -> anyhow::Result<U256> {
        let value = self
            .request(
                "eth_getBalance",
                json!([format!("{address:#x}"), block.eip1898()]),
            )
            .await?;
        parse_quantity_value_u256("eth_getBalance", value)
    }

    pub async fn pending_nonce(&self, address: Address) -> anyhow::Result<u64> {
        self.nonce(address, "pending").await
    }

    pub async fn latest_nonce(&self, address: Address) -> anyhow::Result<u64> {
        self.nonce(address, "latest").await
    }

    async fn nonce(&self, address: Address, block: &str) -> anyhow::Result<u64> {
        let value = self
            .request(
                "eth_getTransactionCount",
                json!([format!("{address:#x}"), block]),
            )
            .await?;
        parse_quantity_value_u64("eth_getTransactionCount", value)
    }

    pub async fn gas_price(&self) -> anyhow::Result<u128> {
        let value = self.request("eth_gasPrice", json!([])).await?;
        parse_quantity_value_u128("eth_gasPrice", value)
    }

    pub async fn simulate_transaction(
        &self,
        transaction: &TransactionCall,
    ) -> anyhow::Result<Vec<u8>> {
        let value = self
            .request("eth_call", json!([transaction.json(), "pending"]))
            .await?;
        let encoded = value
            .as_str()
            .context("eth_call transaction result is not a hex string")?;
        parse_data_hex("eth_call transaction result", encoded)
    }

    pub async fn estimate_gas(&self, transaction: &TransactionCall) -> anyhow::Result<u64> {
        let value = self
            .request("eth_estimateGas", json!([transaction.json(), "pending"]))
            .await?;
        parse_quantity_value_u64("eth_estimateGas", value)
    }

    pub async fn send_raw_transaction(&self, raw: &[u8]) -> anyhow::Result<B256> {
        let value = self
            .request(
                "eth_sendRawTransaction",
                json!([format!("0x{}", hex::encode(raw))]),
            )
            .await?;
        let encoded = value
            .as_str()
            .context("eth_sendRawTransaction result is not a hash")?;
        parse_b256("eth_sendRawTransaction", encoded)
    }

    pub async fn transaction_receipt(
        &self,
        hash: B256,
    ) -> anyhow::Result<Option<TransactionReceipt>> {
        let value = self
            .request_optional("eth_getTransactionReceipt", json!([format!("{hash:#x}")]))
            .await?;
        let Some(value) = value else {
            return Ok(None);
        };
        let receipt: WireTransactionReceipt = serde_json::from_value(value)
            .context("eth_getTransactionReceipt returned an invalid receipt")?;
        let transaction_hash = parse_b256("receipt.transactionHash", &receipt.transaction_hash)?;
        let block_number = parse_quantity_u64("receipt.blockNumber", &receipt.block_number)?;
        Ok(Some(TransactionReceipt {
            transaction_hash,
            block_number,
            status: parse_quantity_u64("receipt.status", &receipt.status)?,
            gas_used: parse_quantity_u64("receipt.gasUsed", &receipt.gas_used)?,
            effective_gas_price: parse_quantity_u128(
                "receipt.effectiveGasPrice",
                &receipt.effective_gas_price,
            )?,
            logs: receipt
                .logs
                .into_iter()
                .map(|log| {
                    Ok(ReceiptLog {
                        address: log
                            .address
                            .parse()
                            .context("receipt log address is invalid")?,
                        topics: log
                            .topics
                            .into_iter()
                            .map(|topic| parse_b256("receipt log topic", &topic))
                            .collect::<anyhow::Result<Vec<_>>>()?,
                        data: parse_data_hex("receipt log data", &log.data)?,
                        position: Some(ReceiptLogPosition {
                            transaction_hash: {
                                let log_hash = parse_b256(
                                    "receipt log transactionHash",
                                    &log.transaction_hash,
                                )?;
                                ensure!(
                                    log_hash == transaction_hash,
                                    "receipt log transactionHash differs from its receipt"
                                );
                                log_hash
                            },
                            block_number: {
                                let log_block = parse_quantity_u64(
                                    "receipt log blockNumber",
                                    &log.block_number,
                                )?;
                                ensure!(
                                    log_block == block_number,
                                    "receipt log blockNumber differs from its receipt"
                                );
                                log_block
                            },
                            block_hash: parse_b256("receipt log blockHash", &log.block_hash)?,
                            transaction_index: parse_quantity_u64(
                                "receipt log transactionIndex",
                                &log.transaction_index,
                            )?,
                            log_index: parse_quantity_u64("receipt log logIndex", &log.log_index)?,
                            removed: log.removed,
                        }),
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?,
        }))
    }

    pub async fn transaction_by_hash(&self, hash: B256) -> anyhow::Result<Option<RpcTransaction>> {
        let value = self
            .request_optional("eth_getTransactionByHash", json!([format!("{hash:#x}")]))
            .await?;
        let Some(value) = value else {
            return Ok(None);
        };
        let transaction = decode_rpc_transaction(value)?;
        ensure!(
            transaction.hash == hash,
            "eth_getTransactionByHash returned a different transaction hash"
        );
        Ok(Some(transaction))
    }

    pub async fn erc20_balance(&self, token: Address, owner: Address) -> anyhow::Result<U256> {
        let block = self.latest_block().await?;
        let mut data = Vec::with_capacity(36);
        data.extend_from_slice(&[0x70, 0xa0, 0x82, 0x31]);
        data.extend_from_slice(&[0_u8; 12]);
        data.extend_from_slice(owner.as_slice());
        let mut values = self
            .eth_call_batch(&[EthCall { to: token, data }], block)
            .await?;
        let value = values
            .pop()
            .context("ERC-20 balance call returned no value")?;
        ensure!(
            value.len() == 32,
            "ERC-20 balance result is not one ABI word"
        );
        Ok(U256::from_be_slice(&value))
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

    pub async fn get_logs(
        &self,
        filter: &EthLogFilter,
        from_block: u64,
        to_block: u64,
    ) -> anyhow::Result<Vec<ChainLog>> {
        ensure!(
            from_block <= to_block,
            "eth_getLogs from_block exceeds to_block"
        );
        let value = self
            .request(
                "eth_getLogs",
                json!([filter.range_json(from_block, to_block)]),
            )
            .await?;
        let logs: Vec<WireChainLog> =
            serde_json::from_value(value).context("eth_getLogs returned invalid logs")?;
        logs.into_iter().map(ChainLog::try_from).collect()
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

    async fn request_optional(&self, method: &str, params: Value) -> anyhow::Result<Option<Value>> {
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
        if let Some(error) = decoded.error {
            return Err(anyhow!(
                "JSON-RPC error {}: {}",
                error.code,
                sanitize_rpc_message(&error.message)
            ));
        }
        Ok(decoded.result)
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
#[serde(rename_all = "camelCase")]
struct WireTransactionReceipt {
    transaction_hash: String,
    block_number: String,
    status: String,
    gas_used: String,
    effective_gas_price: String,
    #[serde(default)]
    logs: Vec<WireReceiptLog>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireReceiptLog {
    address: String,
    topics: Vec<String>,
    data: String,
    transaction_hash: String,
    block_number: String,
    block_hash: String,
    transaction_index: String,
    log_index: String,
    #[serde(default)]
    removed: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireRpcTransaction {
    hash: String,
    chain_id: String,
    nonce: String,
    from: String,
    to: Option<String>,
    value: String,
    input: String,
    block_number: Option<String>,
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

fn decode_rpc_transaction(value: Value) -> anyhow::Result<RpcTransaction> {
    let transaction: WireRpcTransaction = serde_json::from_value(value)
        .context("eth_getTransactionByHash returned an invalid transaction")?;
    Ok(RpcTransaction {
        hash: parse_b256("transaction.hash", &transaction.hash)?,
        chain_id: parse_quantity_u64("transaction.chainId", &transaction.chain_id)?,
        nonce: parse_quantity_u64("transaction.nonce", &transaction.nonce)?,
        from: transaction
            .from
            .parse()
            .context("transaction.from is invalid")?,
        to: transaction
            .to
            .map(|to| to.parse().context("transaction.to is invalid"))
            .transpose()?,
        value: parse_quantity_u256("transaction.value", &transaction.value)?,
        input: parse_data_hex("transaction.input", &transaction.input)?,
        block_number: transaction
            .block_number
            .map(|number| parse_quantity_u64("transaction.blockNumber", &number))
            .transpose()?,
    })
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

fn parse_quantity_u128(name: &str, value: &str) -> anyhow::Result<u128> {
    let encoded = value
        .strip_prefix("0x")
        .with_context(|| format!("{name} is missing 0x prefix"))?;
    ensure!(!encoded.is_empty(), "{name} is empty");
    u128::from_str_radix(encoded, 16).with_context(|| format!("{name} is invalid"))
}

fn parse_quantity_u256(name: &str, value: &str) -> anyhow::Result<U256> {
    let encoded = value
        .strip_prefix("0x")
        .with_context(|| format!("{name} is missing 0x prefix"))?;
    ensure!(!encoded.is_empty(), "{name} is empty");
    U256::from_str_radix(encoded, 16).with_context(|| format!("{name} is invalid"))
}

fn parse_quantity_value_u64(name: &str, value: Value) -> anyhow::Result<u64> {
    let encoded = value
        .as_str()
        .with_context(|| format!("{name} result is not a string"))?;
    parse_quantity_u64(name, encoded)
}

fn parse_quantity_value_u128(name: &str, value: Value) -> anyhow::Result<u128> {
    let encoded = value
        .as_str()
        .with_context(|| format!("{name} result is not a string"))?;
    parse_quantity_u128(name, encoded)
}

fn parse_quantity_value_u256(name: &str, value: Value) -> anyhow::Result<U256> {
    let encoded = value
        .as_str()
        .with_context(|| format!("{name} result is not a string"))?;
    parse_quantity_u256(name, encoded)
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
    use alloy_primitives::{Address, B256, U256};
    use serde_json::json;

    use super::{
        JsonRpcClient, decode_rpc_transaction, parse_data_hex, parse_quantity_u64,
        parse_quantity_u256, sanitize_rpc_message,
    };

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
        assert_eq!(
            parse_quantity_u256("balance", "0x100").unwrap(),
            U256::from(256)
        );
        assert!(parse_data_hex("data", "ff").is_err());
    }

    #[test]
    fn rpc_messages_are_bounded_and_control_free() {
        let message = format!("bad\n{}", "x".repeat(1_000));
        let sanitized = sanitize_rpc_message(&message);
        assert!(!sanitized.contains('\n'));
        assert_eq!(sanitized.chars().count(), 256);
    }

    #[test]
    fn decodes_transaction_for_strict_reconciliation() {
        let transaction = decode_rpc_transaction(json!({
            "hash": format!("{:#x}", B256::repeat_byte(0x11)),
            "chainId": "0x1e0",
            "nonce": "0x7",
            "from": format!("{:#x}", Address::repeat_byte(0x22)),
            "to": format!("{:#x}", Address::repeat_byte(0x33)),
            "value": "0x100",
            "input": "0x00ff",
            "blockNumber": null
        }))
        .unwrap();

        assert_eq!(transaction.hash, B256::repeat_byte(0x11));
        assert_eq!(transaction.chain_id, 480);
        assert_eq!(transaction.nonce, 7);
        assert_eq!(transaction.from, Address::repeat_byte(0x22));
        assert_eq!(transaction.to, Some(Address::repeat_byte(0x33)));
        assert_eq!(transaction.value, U256::from(256));
        assert_eq!(transaction.input, [0, 255]);
        assert_eq!(transaction.block_number, None);
    }

    #[test]
    fn rejects_incomplete_transaction_for_reconciliation() {
        assert!(
            decode_rpc_transaction(json!({
                "hash": format!("{:#x}", B256::repeat_byte(0x11)),
                "nonce": "0x7"
            }))
            .is_err()
        );
    }
}

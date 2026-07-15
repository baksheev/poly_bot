use alloy_primitives::{Address, B256, hex};
use anyhow::{Context, ensure};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainLog {
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Vec<u8>,
    pub block_number: u64,
    pub block_hash: B256,
    pub transaction_index: u64,
    pub log_index: u64,
    pub removed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogPosition {
    pub block_number: u64,
    pub transaction_index: u64,
    pub log_index: u64,
}

impl ChainLog {
    pub const fn position(&self) -> LogPosition {
        LogPosition {
            block_number: self.block_number,
            transaction_index: self.transaction_index,
            log_index: self.log_index,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EthLogFilter {
    pub addresses: Vec<Address>,
    /// Each item is one topic position. `None` is a wildcard and `Some` is an
    /// OR-list for that position, matching the JSON-RPC log filter contract.
    pub topics: Vec<Option<Vec<B256>>>,
}

impl EthLogFilter {
    pub fn new(addresses: Vec<Address>, topics: Vec<Option<Vec<B256>>>) -> anyhow::Result<Self> {
        ensure!(!addresses.is_empty(), "log filter has no addresses");
        ensure!(
            topics
                .iter()
                .all(|topic| topic.as_ref().is_none_or(|values| !values.is_empty())),
            "log filter contains an empty topic OR-list"
        );
        Ok(Self { addresses, topics })
    }

    pub fn subscription_json(&self) -> Value {
        self.json(None)
    }

    pub fn range_json(&self, from_block: u64, to_block: u64) -> Value {
        self.json(Some((from_block, to_block)))
    }

    fn json(&self, range: Option<(u64, u64)>) -> Value {
        let addresses: Vec<_> = self
            .addresses
            .iter()
            .map(|address| format!("{address:#x}"))
            .collect();
        let topics: Vec<_> = self
            .topics
            .iter()
            .map(|topic| match topic {
                None => Value::Null,
                Some(values) => Value::Array(
                    values
                        .iter()
                        .map(|value| Value::String(format!("{value:#x}")))
                        .collect(),
                ),
            })
            .collect();
        let mut filter = json!({
            "address": addresses,
            "topics": topics,
        });
        if let Some((from_block, to_block)) = range {
            filter["fromBlock"] = Value::String(format!("0x{from_block:x}"));
            filter["toBlock"] = Value::String(format!("0x{to_block:x}"));
        }
        filter
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WireChainLog {
    address: String,
    topics: Vec<String>,
    data: String,
    block_number: String,
    block_hash: String,
    transaction_index: String,
    log_index: String,
    #[serde(default)]
    removed: bool,
}

impl TryFrom<WireChainLog> for ChainLog {
    type Error = anyhow::Error;

    fn try_from(value: WireChainLog) -> Result<Self, Self::Error> {
        Ok(Self {
            address: value.address.parse().context("invalid log address")?,
            topics: value
                .topics
                .iter()
                .enumerate()
                .map(|(index, topic)| {
                    topic
                        .parse()
                        .with_context(|| format!("invalid log topic {index}"))
                })
                .collect::<anyhow::Result<_>>()?,
            data: parse_data(&value.data)?,
            block_number: parse_quantity("log.blockNumber", &value.block_number)?,
            block_hash: value.block_hash.parse().context("invalid log blockHash")?,
            transaction_index: parse_quantity("log.transactionIndex", &value.transaction_index)?,
            log_index: parse_quantity("log.logIndex", &value.log_index)?,
            removed: value.removed,
        })
    }
}

pub(crate) fn parse_quantity(name: &str, value: &str) -> anyhow::Result<u64> {
    let encoded = value
        .strip_prefix("0x")
        .with_context(|| format!("{name} is missing 0x prefix"))?;
    ensure!(!encoded.is_empty(), "{name} is empty");
    u64::from_str_radix(encoded, 16).with_context(|| format!("{name} is invalid"))
}

fn parse_data(value: &str) -> anyhow::Result<Vec<u8>> {
    let encoded = value
        .strip_prefix("0x")
        .context("log data is missing 0x prefix")?;
    ensure!(encoded.len() % 2 == 0, "log data has odd hex length");
    hex::decode(encoded).context("log data contains invalid hex")
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{address, b256};

    use super::EthLogFilter;

    #[test]
    fn serializes_address_and_topic_or_filters() {
        let filter = EthLogFilter::new(
            vec![address!("0000000000000000000000000000000000000001")],
            vec![Some(vec![b256!(
                "0000000000000000000000000000000000000000000000000000000000000002"
            )])],
        )
        .unwrap();
        let value = filter.range_json(10, 20);
        assert_eq!(value["fromBlock"], "0xa");
        assert_eq!(value["toBlock"], "0x14");
        assert_eq!(value["address"].as_array().unwrap().len(), 1);
        assert_eq!(value["topics"][0].as_array().unwrap().len(), 1);
    }
}

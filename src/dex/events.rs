use std::collections::BTreeSet;

use alloy_primitives::{Address, B256, U256, keccak256};
use anyhow::{Context, ensure};

use crate::{
    chain::logs::{ChainLog, EthLogFilter},
    dex::hydration::{HydratedDexState, PoolIdentity},
    domain::config::DomainSnapshot,
};

const V3_SWAP_SIGNATURE: &str = "Swap(address,address,int256,int256,uint160,uint128,int24)";
const V3_MINT_SIGNATURE: &str = "Mint(address,address,int24,int24,uint128,uint256,uint256)";
const V3_BURN_SIGNATURE: &str = "Burn(address,int24,int24,uint128,uint256,uint256)";
const V4_SWAP_SIGNATURE: &str = "Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)";
const V4_MODIFY_LIQUIDITY_SIGNATURE: &str =
    "ModifyLiquidity(bytes32,address,int24,int24,int256,bytes32)";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PoolLocator {
    V3(Address),
    V4(B256),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolUpdate {
    Swap {
        sqrt_price_x96: U256,
        tick: i32,
        liquidity: u128,
        fee_pips: Option<u32>,
    },
    Liquidity {
        tick_lower: i32,
        tick_upper: i32,
        delta: i128,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodedPoolEvent {
    pub locator: PoolLocator,
    pub update: PoolUpdate,
}

impl DecodedPoolEvent {
    pub const fn kind(self) -> &'static str {
        match self.update {
            PoolUpdate::Swap { .. } => "swap",
            PoolUpdate::Liquidity { delta, .. } if delta > 0 => "liquidity_added",
            PoolUpdate::Liquidity { delta, .. } if delta < 0 => "liquidity_removed",
            PoolUpdate::Liquidity { .. } => "liquidity_poke",
        }
    }
}

pub fn build_log_filters(
    snapshot: &DomainSnapshot,
    hydrated: &HydratedDexState,
) -> anyhow::Result<Vec<EthLogFilter>> {
    let mut v3_addresses = BTreeSet::new();
    let mut v4_pool_ids = BTreeSet::new();
    for pool in &hydrated.pools {
        match pool.identity {
            PoolIdentity::V3 { address, .. } => {
                v3_addresses.insert(address);
            }
            PoolIdentity::V4 { pool_id, .. } => {
                v4_pool_ids.insert(pool_id);
            }
        }
    }

    let mut filters = Vec::with_capacity(2);
    if !v3_addresses.is_empty() {
        filters.push(EthLogFilter::new(
            v3_addresses.into_iter().collect(),
            vec![Some(vec![
                v3_swap_topic(),
                v3_mint_topic(),
                v3_burn_topic(),
            ])],
        )?);
    }
    if !v4_pool_ids.is_empty() {
        let managers: BTreeSet<Address> = snapshot
            .pairs
            .iter()
            .filter(|pair| pair.market_data_enabled)
            .filter_map(|pair| pair.chain.uniswap_v4_pool_manager_address.as_deref())
            .map(|address| address.parse().context("invalid V4 PoolManager address"))
            .collect::<anyhow::Result<_>>()?;
        ensure!(
            managers.len() == 1,
            "enabled V4 pools must share exactly one PoolManager"
        );
        filters.push(EthLogFilter::new(
            managers.into_iter().collect(),
            vec![
                Some(vec![v4_swap_topic(), v4_modify_liquidity_topic()]),
                Some(v4_pool_ids.into_iter().collect()),
            ],
        )?);
    }
    ensure!(
        !filters.is_empty(),
        "hydrated state produced no log filters"
    );
    Ok(filters)
}

pub fn decode_pool_event(log: &ChainLog) -> anyhow::Result<Option<DecodedPoolEvent>> {
    let Some(signature) = log.topics.first().copied() else {
        return Ok(None);
    };
    if signature == v3_swap_topic() {
        ensure!(log.topics.len() == 3, "invalid V3 Swap topic count");
        ensure!(log.data.len() == 5 * 32, "invalid V3 Swap data length");
        return Ok(Some(DecodedPoolEvent {
            locator: PoolLocator::V3(log.address),
            update: PoolUpdate::Swap {
                sqrt_price_x96: decode_u256(&log.data, 2)?,
                liquidity: decode_u128(&log.data, 3)?,
                tick: decode_i24(&log.data, 4)?,
                fee_pips: None,
            },
        }));
    }
    if signature == v3_mint_topic() {
        ensure!(log.topics.len() == 4, "invalid V3 Mint topic count");
        ensure!(log.data.len() == 4 * 32, "invalid V3 Mint data length");
        let amount = decode_u128(&log.data, 1)?;
        return Ok(Some(DecodedPoolEvent {
            locator: PoolLocator::V3(log.address),
            update: PoolUpdate::Liquidity {
                tick_lower: decode_topic_i24(&log.topics[2]),
                tick_upper: decode_topic_i24(&log.topics[3]),
                delta: i128::try_from(amount).context("V3 Mint liquidity exceeds int128")?,
            },
        }));
    }
    if signature == v3_burn_topic() {
        ensure!(log.topics.len() == 4, "invalid V3 Burn topic count");
        ensure!(log.data.len() == 3 * 32, "invalid V3 Burn data length");
        let amount = decode_u128(&log.data, 0)?;
        let amount = i128::try_from(amount).context("V3 Burn liquidity exceeds int128")?;
        return Ok(Some(DecodedPoolEvent {
            locator: PoolLocator::V3(log.address),
            update: PoolUpdate::Liquidity {
                tick_lower: decode_topic_i24(&log.topics[2]),
                tick_upper: decode_topic_i24(&log.topics[3]),
                delta: amount.checked_neg().context("V3 Burn liquidity overflow")?,
            },
        }));
    }
    if signature == v4_swap_topic() {
        ensure!(log.topics.len() == 3, "invalid V4 Swap topic count");
        ensure!(log.data.len() == 6 * 32, "invalid V4 Swap data length");
        return Ok(Some(DecodedPoolEvent {
            locator: PoolLocator::V4(log.topics[1]),
            update: PoolUpdate::Swap {
                sqrt_price_x96: decode_u256(&log.data, 2)?,
                liquidity: decode_u128(&log.data, 3)?,
                tick: decode_i24(&log.data, 4)?,
                fee_pips: Some(decode_u24(&log.data, 5)?),
            },
        }));
    }
    if signature == v4_modify_liquidity_topic() {
        ensure!(
            log.topics.len() == 3,
            "invalid V4 ModifyLiquidity topic count"
        );
        ensure!(
            log.data.len() == 4 * 32,
            "invalid V4 ModifyLiquidity data length"
        );
        return Ok(Some(DecodedPoolEvent {
            locator: PoolLocator::V4(log.topics[1]),
            update: PoolUpdate::Liquidity {
                tick_lower: decode_i24(&log.data, 0)?,
                tick_upper: decode_i24(&log.data, 1)?,
                delta: decode_i256_as_i128(&log.data, 2)?,
            },
        }));
    }
    Ok(None)
}

pub fn v3_swap_topic() -> B256 {
    keccak256(V3_SWAP_SIGNATURE)
}

pub fn v3_mint_topic() -> B256 {
    keccak256(V3_MINT_SIGNATURE)
}

pub fn v3_burn_topic() -> B256 {
    keccak256(V3_BURN_SIGNATURE)
}

pub fn v4_swap_topic() -> B256 {
    keccak256(V4_SWAP_SIGNATURE)
}

pub fn v4_modify_liquidity_topic() -> B256 {
    keccak256(V4_MODIFY_LIQUIDITY_SIGNATURE)
}

fn decode_word(data: &[u8], index: usize) -> anyhow::Result<&[u8]> {
    let start = index.checked_mul(32).context("ABI word offset overflow")?;
    let end = start.checked_add(32).context("ABI word end overflow")?;
    data.get(start..end)
        .with_context(|| format!("event data is missing word {index}"))
}

fn decode_u256(data: &[u8], index: usize) -> anyhow::Result<U256> {
    Ok(U256::from_be_slice(decode_word(data, index)?))
}

fn decode_u128(data: &[u8], index: usize) -> anyhow::Result<u128> {
    decode_u256(data, index)?
        .try_into()
        .with_context(|| format!("event word {index} does not fit uint128"))
}

fn decode_u24(data: &[u8], index: usize) -> anyhow::Result<u32> {
    let word = decode_word(data, index)?;
    Ok(u32::from_be_bytes([0, word[29], word[30], word[31]]))
}

fn decode_i24(data: &[u8], index: usize) -> anyhow::Result<i32> {
    Ok(decode_i24_word(decode_word(data, index)?))
}

fn decode_topic_i24(topic: &B256) -> i32 {
    decode_i24_word(topic.as_slice())
}

fn decode_i24_word(word: &[u8]) -> i32 {
    let raw = i32::from_be_bytes([0, word[29], word[30], word[31]]);
    if raw & 0x80_0000 != 0 {
        raw | !0xff_ffff
    } else {
        raw
    }
}

fn decode_i256_as_i128(data: &[u8], index: usize) -> anyhow::Result<i128> {
    let word = decode_word(data, index)?;
    let value = i128::from_be_bytes(word[16..].try_into().expect("16 bytes"));
    let sign = if value < 0 { 0xff } else { 0x00 };
    ensure!(
        word[..16].iter().all(|byte| *byte == sign),
        "event word {index} does not fit int128"
    );
    Ok(value)
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, B256, U256, address};

    use crate::chain::logs::ChainLog;

    use super::{PoolLocator, PoolUpdate, decode_pool_event, v3_mint_topic, v4_swap_topic};

    fn word_u128(value: u128) -> [u8; 32] {
        let mut word = [0_u8; 32];
        word[16..].copy_from_slice(&value.to_be_bytes());
        word
    }

    fn word_i32(value: i32) -> [u8; 32] {
        let mut word = [if value < 0 { 0xff } else { 0 }; 32];
        word[28..].copy_from_slice(&value.to_be_bytes());
        word
    }

    fn log(address: Address, topics: Vec<B256>, data: Vec<u8>) -> ChainLog {
        ChainLog {
            address,
            topics,
            data,
            block_number: 10,
            block_hash: B256::ZERO,
            transaction_index: 1,
            log_index: 2,
            removed: false,
        }
    }

    #[test]
    fn decodes_v3_mint_boundaries_and_delta() {
        let pool = address!("0000000000000000000000000000000000000001");
        let mut data = vec![0_u8; 128];
        data[32..64].copy_from_slice(&word_u128(500));
        let event = decode_pool_event(&log(
            pool,
            vec![
                v3_mint_topic(),
                B256::ZERO,
                B256::from(word_i32(-120)),
                B256::from(word_i32(120)),
            ],
            data,
        ))
        .unwrap()
        .unwrap();
        assert_eq!(event.locator, PoolLocator::V3(pool));
        assert_eq!(
            event.update,
            PoolUpdate::Liquidity {
                tick_lower: -120,
                tick_upper: 120,
                delta: 500
            }
        );
    }

    #[test]
    fn decodes_v4_swap_head_and_fee() {
        let pool_id = B256::repeat_byte(7);
        let mut data = vec![0_u8; 192];
        data[64..96].copy_from_slice(&U256::from(123_u64).to_be_bytes::<32>());
        data[96..128].copy_from_slice(&word_u128(456));
        data[128..160].copy_from_slice(&word_i32(-42));
        data[160..192].copy_from_slice(&word_u128(3_000));
        let event = decode_pool_event(&log(
            Address::ZERO,
            vec![v4_swap_topic(), pool_id, B256::ZERO],
            data,
        ))
        .unwrap()
        .unwrap();
        assert_eq!(event.locator, PoolLocator::V4(pool_id));
        assert_eq!(
            event.update,
            PoolUpdate::Swap {
                sqrt_price_x96: U256::from(123_u64),
                tick: -42,
                liquidity: 456,
                fee_pips: Some(3_000)
            }
        );
    }
}

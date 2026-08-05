use std::{collections::BTreeSet, sync::LazyLock};

use alloy_primitives::{Address, B256, I256, U256, keccak256};
use anyhow::{Context, ensure};

use crate::{
    chain::logs::{ChainLog, EthLogFilter},
    dex::hydration::{HydratedDexState, PoolIdentity},
    domain::config::DomainSnapshot,
};

const V3_SWAP_SIGNATURE: &str = "Swap(address,address,int256,int256,uint160,uint128,int24)";
const PANCAKE_V3_SWAP_SIGNATURE: &str =
    "Swap(address,address,int256,int256,uint160,uint128,int24,uint128,uint128)";
const V3_MINT_SIGNATURE: &str = "Mint(address,address,int24,int24,uint128,uint256,uint256)";
const V3_BURN_SIGNATURE: &str = "Burn(address,int24,int24,uint128,uint256,uint256)";
const V4_SWAP_SIGNATURE: &str = "Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)";
const V4_MODIFY_LIQUIDITY_SIGNATURE: &str =
    "ModifyLiquidity(bytes32,address,int24,int24,int256,bytes32)";
const CAMELOT_FEE_SIGNATURE: &str = "Fee(uint16,uint16)";
const CAMELOT_TICK_SPACING_SIGNATURE: &str = "TickSpacing(int24)";
const CAMELOT_INCENTIVE_SIGNATURE: &str = "Incentive(address)";

static V3_SWAP_TOPIC: LazyLock<B256> = LazyLock::new(|| keccak256(V3_SWAP_SIGNATURE));
static PANCAKE_V3_SWAP_TOPIC: LazyLock<B256> =
    LazyLock::new(|| keccak256(PANCAKE_V3_SWAP_SIGNATURE));
static V3_MINT_TOPIC: LazyLock<B256> = LazyLock::new(|| keccak256(V3_MINT_SIGNATURE));
static V3_BURN_TOPIC: LazyLock<B256> = LazyLock::new(|| keccak256(V3_BURN_SIGNATURE));
static V4_SWAP_TOPIC: LazyLock<B256> = LazyLock::new(|| keccak256(V4_SWAP_SIGNATURE));
static V4_MODIFY_LIQUIDITY_TOPIC: LazyLock<B256> =
    LazyLock::new(|| keccak256(V4_MODIFY_LIQUIDITY_SIGNATURE));
static CAMELOT_FEE_TOPIC: LazyLock<B256> = LazyLock::new(|| keccak256(CAMELOT_FEE_SIGNATURE));
static CAMELOT_TICK_SPACING_TOPIC: LazyLock<B256> =
    LazyLock::new(|| keccak256(CAMELOT_TICK_SPACING_SIGNATURE));
static CAMELOT_INCENTIVE_TOPIC: LazyLock<B256> =
    LazyLock::new(|| keccak256(CAMELOT_INCENTIVE_SIGNATURE));

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PoolLocator {
    V3(Address),
    PancakeV3(Address),
    CamelotV3(Address),
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
    Fee {
        zero_for_one: u16,
        one_for_zero: u16,
    },
    TickSpacing {
        value: i32,
    },
    Incentive {
        address: Address,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodedPoolEvent {
    pub locator: PoolLocator,
    pub update: PoolUpdate,
}

/// Allocation-free receipt acceleration proof for Camelot's Fee event. The
/// successful receipt already owns and validates the transaction identity;
/// retaining only the canonical pool position and the two uint16 values keeps
/// the settlement handoff comparable to Uniswap's single Swap log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CamelotFeeReceiptProof {
    pub pool: Address,
    pub zero_for_one: u16,
    pub one_for_zero: u16,
    pub block_number: u64,
    pub block_hash: B256,
    pub transaction_index: u64,
    pub log_index: u64,
}

impl CamelotFeeReceiptProof {
    pub const fn position(self) -> crate::chain::logs::LogPosition {
        crate::chain::logs::LogPosition {
            block_number: self.block_number,
            transaction_index: self.transaction_index,
            log_index: self.log_index,
        }
    }
}

impl DecodedPoolEvent {
    pub const fn kind(self) -> &'static str {
        match self.update {
            PoolUpdate::Swap { .. } => "swap",
            PoolUpdate::Liquidity { delta, .. } if delta > 0 => "liquidity_added",
            PoolUpdate::Liquidity { delta, .. } if delta < 0 => "liquidity_removed",
            PoolUpdate::Liquidity { .. } => "liquidity_poke",
            PoolUpdate::Fee { .. } => "fee",
            PoolUpdate::TickSpacing { .. } => "tick_spacing",
            PoolUpdate::Incentive { .. } => "incentive",
        }
    }
}

pub fn build_log_filters(
    snapshot: &DomainSnapshot,
    hydrated: &HydratedDexState,
) -> anyhow::Result<Vec<EthLogFilter>> {
    let mut v3_addresses = BTreeSet::new();
    let mut pancake_v3_addresses = BTreeSet::new();
    let mut camelot_v3_addresses = BTreeSet::new();
    let mut v4_pool_ids = BTreeSet::new();
    for pool in &hydrated.pools {
        match pool.identity {
            PoolIdentity::V3 { address, .. } => {
                v3_addresses.insert(address);
            }
            PoolIdentity::PancakeV3 { address, .. } => {
                pancake_v3_addresses.insert(address);
            }
            PoolIdentity::CamelotV3 { address } => {
                camelot_v3_addresses.insert(address);
            }
            PoolIdentity::V4 { pool_id, .. } => {
                v4_pool_ids.insert(pool_id);
            }
        }
    }

    let mut filters = Vec::with_capacity(3);
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
    if !pancake_v3_addresses.is_empty() {
        filters.push(EthLogFilter::new(
            pancake_v3_addresses.into_iter().collect(),
            vec![Some(vec![
                pancake_v3_swap_topic(),
                v3_mint_topic(),
                v3_burn_topic(),
            ])],
        )?);
    }
    if !camelot_v3_addresses.is_empty() {
        filters.push(EthLogFilter::new(
            camelot_v3_addresses.into_iter().collect(),
            vec![Some(vec![
                v3_swap_topic(),
                v3_mint_topic(),
                v3_burn_topic(),
                camelot_fee_topic(),
                camelot_tick_spacing_topic(),
                camelot_incentive_topic(),
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

pub fn build_pool_log_filter(
    locator: PoolLocator,
    event_address: Address,
) -> anyhow::Result<EthLogFilter> {
    match locator {
        PoolLocator::V3(pool) => {
            ensure!(
                event_address == pool,
                "V3 settlement event address differs from its pool"
            );
            EthLogFilter::new(
                vec![pool],
                vec![Some(vec![
                    v3_swap_topic(),
                    v3_mint_topic(),
                    v3_burn_topic(),
                ])],
            )
        }
        PoolLocator::PancakeV3(pool) => {
            ensure!(
                event_address == pool,
                "PancakeSwap V3 settlement event address differs from its pool"
            );
            EthLogFilter::new(
                vec![pool],
                vec![Some(vec![
                    pancake_v3_swap_topic(),
                    v3_mint_topic(),
                    v3_burn_topic(),
                ])],
            )
        }
        PoolLocator::CamelotV3(pool) => {
            ensure!(
                event_address == pool,
                "Camelot V3 event address differs from its pool"
            );
            EthLogFilter::new(
                vec![pool],
                vec![Some(vec![
                    v3_swap_topic(),
                    v3_mint_topic(),
                    v3_burn_topic(),
                    camelot_fee_topic(),
                    camelot_tick_spacing_topic(),
                    camelot_incentive_topic(),
                ])],
            )
        }
        PoolLocator::V4(pool_id) => EthLogFilter::new(
            vec![event_address],
            vec![
                Some(vec![v4_swap_topic(), v4_modify_liquidity_topic()]),
                Some(vec![pool_id]),
            ],
        ),
    }
}

pub fn decode_pool_event(log: &ChainLog) -> anyhow::Result<Option<DecodedPoolEvent>> {
    decode_pool_event_with_locator(log, None)
}

pub fn decode_pool_event_for_locator(
    log: &ChainLog,
    locator: PoolLocator,
) -> anyhow::Result<Option<DecodedPoolEvent>> {
    decode_pool_event_with_locator(log, Some(locator))
}

pub fn decode_camelot_pool_event(
    log: &ChainLog,
    pool: Address,
) -> anyhow::Result<Option<DecodedPoolEvent>> {
    ensure!(
        log.address == pool,
        "Camelot event address differs from its pool"
    );
    if log.topics.first().copied() == Some(v3_swap_topic()) {
        ensure!(log.topics.len() == 3, "invalid Camelot Swap topic count");
        ensure!(log.data.len() == 5 * 32, "invalid Camelot Swap data length");
        return Ok(Some(DecodedPoolEvent {
            locator: PoolLocator::CamelotV3(pool),
            update: PoolUpdate::Swap {
                sqrt_price_x96: decode_u256(&log.data, 2)?,
                liquidity: decode_u128(&log.data, 3)?,
                tick: decode_i24(&log.data, 4)?,
                fee_pips: None,
            },
        }));
    }
    decode_pool_event_with_locator(log, Some(PoolLocator::CamelotV3(pool)))
}

/// Receipt-only accounting for Pancake's two trailing protocol-fee words.
/// The WebSocket mirror intentionally skips these fields in its hot path.
pub fn decode_pancake_v3_protocol_fees(log: &ChainLog) -> anyhow::Result<Option<(u128, u128)>> {
    if log.topics.first().copied() != Some(pancake_v3_swap_topic()) {
        return Ok(None);
    }
    ensure!(
        log.data.len() == 7 * 32,
        "invalid Pancake V3 Swap data length"
    );
    Ok(Some((
        decode_u128(&log.data, 5)?,
        decode_u128(&log.data, 6)?,
    )))
}

fn decode_pool_event_with_locator(
    log: &ChainLog,
    locator_hint: Option<PoolLocator>,
) -> anyhow::Result<Option<DecodedPoolEvent>> {
    let Some(signature) = log.topics.first().copied() else {
        return Ok(None);
    };
    if signature == v3_swap_topic() {
        if locator_hint == Some(PoolLocator::CamelotV3(log.address)) {
            ensure!(log.topics.len() == 3, "invalid Camelot Swap topic count");
            ensure!(log.data.len() == 5 * 32, "invalid Camelot Swap data length");
            return Ok(Some(DecodedPoolEvent {
                locator: PoolLocator::CamelotV3(log.address),
                update: PoolUpdate::Swap {
                    sqrt_price_x96: decode_u256(&log.data, 2)?,
                    liquidity: decode_u128(&log.data, 3)?,
                    tick: decode_i24(&log.data, 4)?,
                    fee_pips: None,
                },
            }));
        }
        ensure!(
            locator_hint.is_none_or(|locator| locator == PoolLocator::V3(log.address)),
            "Uniswap V3 Swap topic does not match its routed pool provider"
        );
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
    if signature == pancake_v3_swap_topic() {
        ensure!(
            locator_hint.is_none_or(|locator| locator == PoolLocator::PancakeV3(log.address)),
            "PancakeSwap V3 Swap topic does not match its routed pool provider"
        );
        ensure!(log.topics.len() == 3, "invalid Pancake V3 Swap topic count");
        ensure!(
            log.data.len() == 7 * 32,
            "invalid Pancake V3 Swap data length"
        );
        return Ok(Some(DecodedPoolEvent {
            locator: PoolLocator::PancakeV3(log.address),
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
        let locator = routed_v3_locator(log.address, locator_hint)?;
        return Ok(Some(DecodedPoolEvent {
            locator,
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
        let locator = routed_v3_locator(log.address, locator_hint)?;
        return Ok(Some(DecodedPoolEvent {
            locator,
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
    if signature == camelot_fee_topic() {
        ensure!(
            locator_hint == Some(PoolLocator::CamelotV3(log.address)),
            "Camelot Fee event lacks Camelot provider routing"
        );
        ensure!(log.topics.len() == 1, "invalid Camelot Fee topic count");
        ensure!(log.data.len() == 2 * 32, "invalid Camelot Fee data length");
        return Ok(Some(DecodedPoolEvent {
            locator: PoolLocator::CamelotV3(log.address),
            update: PoolUpdate::Fee {
                zero_for_one: decode_u16(&log.data, 0)?,
                one_for_zero: decode_u16(&log.data, 1)?,
            },
        }));
    }
    if signature == camelot_tick_spacing_topic() {
        ensure!(
            locator_hint == Some(PoolLocator::CamelotV3(log.address)),
            "Camelot TickSpacing event lacks Camelot provider routing"
        );
        ensure!(
            log.topics.len() == 1,
            "invalid Camelot TickSpacing topic count"
        );
        ensure!(
            log.data.len() == 32,
            "invalid Camelot TickSpacing data length"
        );
        return Ok(Some(DecodedPoolEvent {
            locator: PoolLocator::CamelotV3(log.address),
            update: PoolUpdate::TickSpacing {
                value: decode_i24(&log.data, 0)?,
            },
        }));
    }
    if signature == camelot_incentive_topic() {
        ensure!(
            locator_hint == Some(PoolLocator::CamelotV3(log.address)),
            "Camelot Incentive event lacks Camelot provider routing"
        );
        ensure!(
            log.topics.len() == 2,
            "invalid Camelot Incentive topic count"
        );
        ensure!(log.data.is_empty(), "invalid Camelot Incentive data length");
        ensure!(
            log.topics[1].as_slice()[..12] == [0_u8; 12],
            "Camelot Incentive indexed address has non-zero padding"
        );
        return Ok(Some(DecodedPoolEvent {
            locator: PoolLocator::CamelotV3(log.address),
            update: PoolUpdate::Incentive {
                address: Address::from_slice(&log.topics[1].as_slice()[12..]),
            },
        }));
    }
    Ok(None)
}

fn routed_v3_locator(
    address: Address,
    locator_hint: Option<PoolLocator>,
) -> anyhow::Result<PoolLocator> {
    let locator = locator_hint.unwrap_or(PoolLocator::V3(address));
    ensure!(
        matches!(locator, PoolLocator::V3(pool) | PoolLocator::PancakeV3(pool) | PoolLocator::CamelotV3(pool) if pool == address),
        "V3 liquidity event does not match its routed pool"
    );
    Ok(locator)
}

pub fn v3_swap_topic() -> B256 {
    *V3_SWAP_TOPIC
}

pub fn pancake_v3_swap_topic() -> B256 {
    *PANCAKE_V3_SWAP_TOPIC
}

pub fn v3_mint_topic() -> B256 {
    *V3_MINT_TOPIC
}

pub fn v3_burn_topic() -> B256 {
    *V3_BURN_TOPIC
}

pub fn v4_swap_topic() -> B256 {
    *V4_SWAP_TOPIC
}

pub fn v4_modify_liquidity_topic() -> B256 {
    *V4_MODIFY_LIQUIDITY_TOPIC
}

pub fn camelot_fee_topic() -> B256 {
    *CAMELOT_FEE_TOPIC
}

pub fn camelot_tick_spacing_topic() -> B256 {
    *CAMELOT_TICK_SPACING_TOPIC
}

pub fn camelot_incentive_topic() -> B256 {
    *CAMELOT_INCENTIVE_TOPIC
}

pub fn decode_camelot_v3_swap_amounts(log: &ChainLog) -> anyhow::Result<(I256, I256)> {
    ensure!(
        log.topics.first().copied() == Some(v3_swap_topic()),
        "Camelot Swap amount decode received another event"
    );
    ensure!(log.data.len() == 5 * 32, "invalid Camelot Swap data length");
    Ok(decode_camelot_v3_swap_amounts_after_event_validation(log))
}

pub(crate) fn decode_camelot_v3_swap_amounts_after_event_validation(
    log: &ChainLog,
) -> (I256, I256) {
    (
        I256::from_raw(U256::from_be_slice(&log.data[..32])),
        I256::from_raw(U256::from_be_slice(&log.data[32..64])),
    )
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

fn decode_u16(data: &[u8], index: usize) -> anyhow::Result<u16> {
    decode_u256(data, index)?
        .try_into()
        .with_context(|| format!("event word {index} does not fit uint16"))
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
    use std::hint::black_box;

    use alloy_primitives::{Address, B256, I256, U256, address};

    use crate::{chain::logs::ChainLog, paired_benchmark::assert_paired_non_regression};

    use super::{
        PoolLocator, PoolUpdate, camelot_fee_topic, camelot_incentive_topic,
        camelot_tick_spacing_topic, decode_camelot_pool_event, decode_camelot_v3_swap_amounts,
        decode_pancake_v3_protocol_fees, decode_pool_event, decode_pool_event_for_locator,
        pancake_v3_swap_topic, v3_mint_topic, v3_swap_topic, v4_swap_topic,
    };

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

    fn word_u16(value: u16) -> [u8; 32] {
        let mut word = [0_u8; 32];
        word[30..].copy_from_slice(&value.to_be_bytes());
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
                fee_pips: Some(3_000),
            }
        );
    }

    #[test]
    fn decodes_pancake_v3_swap_head_from_extended_event_layout() {
        let pool = address!("0000000000000000000000000000000000000002");
        let mut data = vec![0_u8; 224];
        data[64..96].copy_from_slice(&U256::from(123_u64).to_be_bytes::<32>());
        data[96..128].copy_from_slice(&word_u128(456));
        data[128..160].copy_from_slice(&word_i32(-42));
        data[160..192].copy_from_slice(&word_u128(7));
        data[192..224].copy_from_slice(&word_u128(11));
        let log = log(
            pool,
            vec![pancake_v3_swap_topic(), B256::ZERO, B256::ZERO],
            data,
        );
        let event = decode_pool_event_for_locator(&log, PoolLocator::PancakeV3(pool))
            .unwrap()
            .unwrap();
        assert_eq!(event.locator, PoolLocator::PancakeV3(pool));
        assert_eq!(
            event.update,
            PoolUpdate::Swap {
                sqrt_price_x96: U256::from(123_u64),
                tick: -42,
                liquidity: 456,
                fee_pips: None,
            }
        );
        assert_eq!(
            decode_pancake_v3_protocol_fees(&log).unwrap(),
            Some((7, 11))
        );
    }

    #[test]
    fn shared_mint_topic_retains_pancake_provider_identity() {
        let pool = address!("0000000000000000000000000000000000000002");
        let mut data = vec![0_u8; 128];
        data[32..64].copy_from_slice(&word_u128(500));
        let event = decode_pool_event_for_locator(
            &log(
                pool,
                vec![
                    v3_mint_topic(),
                    B256::ZERO,
                    B256::from(word_i32(-120)),
                    B256::from(word_i32(120)),
                ],
                data,
            ),
            PoolLocator::PancakeV3(pool),
        )
        .unwrap()
        .unwrap();
        assert_eq!(event.locator, PoolLocator::PancakeV3(pool));
    }

    #[test]
    fn camelot_topics_require_provider_routing_and_decode_exact_values() {
        let pool = address!("0000000000000000000000000000000000000003");
        let incentive = address!("0000000000000000000000000000000000000004");
        let mut fee_data = Vec::with_capacity(64);
        fee_data.extend_from_slice(&word_u16(117));
        fee_data.extend_from_slice(&word_u16(104));
        let fee_log = log(pool, vec![camelot_fee_topic()], fee_data);
        assert!(decode_pool_event(&fee_log).is_err());
        assert_eq!(
            decode_camelot_pool_event(&fee_log, pool)
                .unwrap()
                .unwrap()
                .update,
            PoolUpdate::Fee {
                zero_for_one: 117,
                one_for_zero: 104,
            }
        );

        assert_eq!(
            decode_camelot_pool_event(
                &log(
                    pool,
                    vec![camelot_tick_spacing_topic()],
                    word_i32(10).to_vec()
                ),
                pool,
            )
            .unwrap()
            .unwrap()
            .update,
            PoolUpdate::TickSpacing { value: 10 }
        );
        let mut incentive_topic = [0_u8; 32];
        incentive_topic[12..].copy_from_slice(incentive.as_slice());
        assert_eq!(
            decode_camelot_pool_event(
                &log(
                    pool,
                    vec![camelot_incentive_topic(), B256::from(incentive_topic)],
                    Vec::new(),
                ),
                pool,
            )
            .unwrap()
            .unwrap()
            .update,
            PoolUpdate::Incentive { address: incentive }
        );
    }

    #[test]
    fn shared_swap_topic_retains_camelot_provider_identity_and_amounts() {
        let pool = address!("0000000000000000000000000000000000000003");
        let mut data = vec![0_u8; 160];
        data[..32].copy_from_slice(
            &I256::try_from(4_i64)
                .unwrap()
                .into_raw()
                .to_be_bytes::<32>(),
        );
        data[32..64].copy_from_slice(
            &I256::try_from(-9_i64)
                .unwrap()
                .into_raw()
                .to_be_bytes::<32>(),
        );
        data[64..96].copy_from_slice(&U256::from(123_u64).to_be_bytes::<32>());
        data[96..128].copy_from_slice(&word_u128(456));
        data[128..160].copy_from_slice(&word_i32(-42));
        let log = log(pool, vec![v3_swap_topic(), B256::ZERO, B256::ZERO], data);
        let event = decode_camelot_pool_event(&log, pool).unwrap().unwrap();
        assert_eq!(event.locator, PoolLocator::CamelotV3(pool));
        assert!(matches!(event.update, PoolUpdate::Swap { .. }));
        assert_eq!(
            decode_camelot_v3_swap_amounts(&log).unwrap(),
            (
                I256::try_from(4_i64).unwrap(),
                I256::try_from(-9_i64).unwrap()
            )
        );
    }

    #[test]
    #[ignore = "manual release-mode paired V3 event decoder benchmark"]
    fn benchmark_uniswap_and_pancake_swap_decoders() {
        let pool = address!("0000000000000000000000000000000000000002");
        let mut uniswap_data = vec![0_u8; 160];
        uniswap_data[64..96].copy_from_slice(&U256::from(123_u64).to_be_bytes::<32>());
        uniswap_data[96..128].copy_from_slice(&word_u128(456));
        uniswap_data[128..160].copy_from_slice(&word_i32(-42));
        let uniswap = log(
            pool,
            vec![v3_swap_topic(), B256::ZERO, B256::ZERO],
            uniswap_data,
        );
        let mut pancake_data = vec![0_u8; 224];
        pancake_data[..160].copy_from_slice(&uniswap.data);
        pancake_data[160..192].copy_from_slice(&word_u128(7));
        pancake_data[192..224].copy_from_slice(&word_u128(11));
        let pancake = log(
            pool,
            vec![pancake_v3_swap_topic(), B256::ZERO, B256::ZERO],
            pancake_data,
        );

        assert_paired_non_regression(
            "v3_event_decode_benchmark",
            1.10,
            || {
                black_box(decode_pool_event_for_locator(
                    &uniswap,
                    PoolLocator::V3(pool),
                ))
                .unwrap();
            },
            || {
                black_box(decode_pool_event_for_locator(
                    &pancake,
                    PoolLocator::PancakeV3(pool),
                ))
                .unwrap();
            },
        );
    }
}

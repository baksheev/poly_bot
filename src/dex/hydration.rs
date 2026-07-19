use alloy_primitives::{Address, B256, U256, keccak256};
use anyhow::{Context, ensure};

use crate::{
    chain::rpc::{CanonicalBlock, EthCall, JsonRpcClient},
    dex::{clmm::ClmmPool, pool_id::V4PoolKey},
    domain::config::{DexProvider, DomainSnapshot, PairConfig, UniswapV4PoolConfig},
};

const MIN_TICK: i32 = -887_272;
const MAX_TICK: i32 = 887_272;

#[derive(Debug)]
pub struct HydratedDexState {
    pub block: CanonicalBlock,
    pub pools: Vec<HydratedPool>,
    pub unavailable: Vec<UnavailablePool>,
}

#[derive(Debug)]
pub struct HydratedPool {
    pub pair_id: String,
    pub identity: PoolIdentity,
    pub token0: Address,
    pub token1: Address,
    pub pool: ClmmPool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolIdentity {
    V3 { address: Address, fee_pips: u32 },
    V4 { pool_id: B256, fee_pips: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnavailablePool {
    pub pair_id: String,
    pub protocol: DexProvider,
    pub fee_pips: u32,
    pub address: Option<Address>,
    pub pool_id: Option<B256>,
    pub reason: UnavailableReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnavailableReason {
    NotCreated,
    Uninitialized,
    ZeroLiquidity,
}

impl UnavailableReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotCreated => "not_created",
            Self::Uninitialized => "uninitialized",
            Self::ZeroLiquidity => "zero_liquidity",
        }
    }
}

pub struct DexHydrator<'client> {
    rpc: &'client JsonRpcClient,
}

impl<'client> DexHydrator<'client> {
    pub const fn new(rpc: &'client JsonRpcClient) -> Self {
        Self { rpc }
    }

    pub async fn hydrate(&self, snapshot: &DomainSnapshot) -> anyhow::Result<HydratedDexState> {
        let block = self.rpc.latest_block().await?;
        let mut pools = Vec::new();
        let mut unavailable = Vec::new();

        for pair in &snapshot.pairs {
            if !pair.market_data_enabled {
                continue;
            }
            if pair.dex.allowed_providers.contains(&DexProvider::UniswapV3) {
                self.hydrate_v3(pair, block, &mut pools, &mut unavailable)
                    .await
                    .with_context(|| format!("failed to hydrate V3 pair {}", pair.id))?;
            }
            if pair.dex.allowed_providers.contains(&DexProvider::UniswapV4) {
                self.hydrate_v4(pair, block, &mut pools, &mut unavailable)
                    .await
                    .with_context(|| format!("failed to hydrate V4 pair {}", pair.id))?;
            }
        }

        ensure!(!pools.is_empty(), "no quotable DEX pools were hydrated");
        Ok(HydratedDexState {
            block,
            pools,
            unavailable,
        })
    }

    async fn hydrate_v3(
        &self,
        pair: &PairConfig,
        block: CanonicalBlock,
        pools: &mut Vec<HydratedPool>,
        unavailable: &mut Vec<UnavailablePool>,
    ) -> anyhow::Result<()> {
        let token_a = parse_address("token_a", &pair.token_a.contract)?;
        let token_b = parse_address("token_b", &pair.token_b.contract)?;
        let (token0, token1) = sort_tokens(token_a, token_b);
        let factory = parse_address(
            "uniswap_v3_factory_address",
            pair.chain
                .uniswap_v3_factory_address
                .as_deref()
                .context("missing V3 factory")?,
        )?;
        let config = pair.dex.uniswap_v3.as_ref().context("missing V3 config")?;

        let discovery_calls: Vec<_> = config
            .fee_tiers
            .iter()
            .map(|fee| EthCall {
                to: factory,
                data: encode_call(
                    "getPool(address,address,uint24)",
                    &[word_address(token0), word_address(token1), word_u32(*fee)],
                ),
            })
            .collect();
        let discovery = self.rpc.eth_call_batch(&discovery_calls, block).await?;

        for (fee_pips, output) in config.fee_tiers.iter().copied().zip(discovery) {
            let address = decode_address(&output, 0)?;
            if address.is_zero() {
                unavailable.push(UnavailablePool {
                    pair_id: pair.id.clone(),
                    protocol: DexProvider::UniswapV3,
                    fee_pips,
                    address: None,
                    pool_id: None,
                    reason: UnavailableReason::NotCreated,
                });
                continue;
            }

            let head = self
                .rpc
                .eth_call_batch(
                    &[
                        EthCall {
                            to: address,
                            data: encode_call("slot0()", &[]),
                        },
                        EthCall {
                            to: address,
                            data: encode_call("liquidity()", &[]),
                        },
                        EthCall {
                            to: address,
                            data: encode_call("tickSpacing()", &[]),
                        },
                    ],
                    block,
                )
                .await?;
            let sqrt_price_x96 = decode_u256(&head[0], 0)?;
            if sqrt_price_x96.is_zero() {
                unavailable.push(UnavailablePool {
                    pair_id: pair.id.clone(),
                    protocol: DexProvider::UniswapV3,
                    fee_pips,
                    address: Some(address),
                    pool_id: None,
                    reason: UnavailableReason::Uninitialized,
                });
                continue;
            }
            let tick = decode_i24(&head[0], 1)?;
            let liquidity = decode_u128(&head[1], 0)?;
            if liquidity == 0 {
                unavailable.push(UnavailablePool {
                    pair_id: pair.id.clone(),
                    protocol: DexProvider::UniswapV3,
                    fee_pips,
                    address: Some(address),
                    pool_id: None,
                    reason: UnavailableReason::ZeroLiquidity,
                });
                continue;
            }
            let tick_spacing = decode_i24(&head[2], 0)?;
            ensure!(tick_spacing > 0, "V3 pool returned invalid tick spacing");

            let ticks = self.hydrate_v3_ticks(address, tick_spacing, block).await?;
            let mut pool = ClmmPool::new(fee_pips, tick_spacing, sqrt_price_x96, tick, liquidity)?;
            install_ticks(&mut pool, ticks)?;
            pools.push(HydratedPool {
                pair_id: pair.id.clone(),
                identity: PoolIdentity::V3 { address, fee_pips },
                token0,
                token1,
                pool,
            });
        }
        Ok(())
    }

    async fn hydrate_v3_ticks(
        &self,
        pool: Address,
        tick_spacing: i32,
        block: CanonicalBlock,
    ) -> anyhow::Result<Vec<HydratedTick>> {
        let words = word_positions(tick_spacing)?;
        let bitmap_calls: Vec<_> = words
            .iter()
            .map(|word| EthCall {
                to: pool,
                data: encode_call("tickBitmap(int16)", &[word_i32(i32::from(*word))]),
            })
            .collect();
        let bitmaps = self.rpc.eth_call_batch(&bitmap_calls, block).await?;
        let initialized = initialized_ticks(&words, &bitmaps, tick_spacing)?;
        let tick_calls: Vec<_> = initialized
            .iter()
            .map(|tick| EthCall {
                to: pool,
                data: encode_call("ticks(int24)", &[word_i32(*tick)]),
            })
            .collect();
        let outputs = self.rpc.eth_call_batch(&tick_calls, block).await?;
        decode_ticks(&initialized, &outputs)
    }

    async fn hydrate_v4(
        &self,
        pair: &PairConfig,
        block: CanonicalBlock,
        pools: &mut Vec<HydratedPool>,
        unavailable: &mut Vec<UnavailablePool>,
    ) -> anyhow::Result<()> {
        let token_a = parse_address("token_a", &pair.token_a.contract)?;
        let token_b = parse_address("token_b", &pair.token_b.contract)?;
        let state_view = parse_address(
            "uniswap_v4_state_view_address",
            pair.chain
                .uniswap_v4_state_view_address
                .as_deref()
                .context("missing V4 StateView")?,
        )?;
        let config = pair.dex.uniswap_v4.as_ref().context("missing V4 config")?;

        for configured_pool in &config.pools {
            let hooks = parse_address("V4 hooks", &configured_pool.hooks)?;
            let key = V4PoolKey::new(
                token_a,
                token_b,
                configured_pool.fee_tier,
                configured_pool.tick_spacing,
                hooks,
            )?;
            let pool_id = key.pool_id();
            let head = self
                .rpc
                .eth_call_batch(
                    &[
                        EthCall {
                            to: state_view,
                            data: encode_call("getSlot0(bytes32)", &[word_b256(pool_id)]),
                        },
                        EthCall {
                            to: state_view,
                            data: encode_call("getLiquidity(bytes32)", &[word_b256(pool_id)]),
                        },
                    ],
                    block,
                )
                .await?;
            let sqrt_price_x96 = decode_u256(&head[0], 0)?;
            if sqrt_price_x96.is_zero() {
                unavailable.push(UnavailablePool {
                    pair_id: pair.id.clone(),
                    protocol: DexProvider::UniswapV4,
                    fee_pips: configured_pool.fee_tier,
                    address: None,
                    pool_id: Some(pool_id),
                    reason: UnavailableReason::Uninitialized,
                });
                continue;
            }
            let tick = decode_i24(&head[0], 1)?;
            let lp_fee = decode_u24(&head[0], 3)?;
            ensure!(
                lp_fee == configured_pool.fee_tier,
                "V4 static LP fee differs from configured pool fee"
            );
            let liquidity = decode_u128(&head[1], 0)?;
            if liquidity == 0 {
                unavailable.push(UnavailablePool {
                    pair_id: pair.id.clone(),
                    protocol: DexProvider::UniswapV4,
                    fee_pips: configured_pool.fee_tier,
                    address: None,
                    pool_id: Some(pool_id),
                    reason: UnavailableReason::ZeroLiquidity,
                });
                continue;
            }

            let ticks = self
                .hydrate_v4_ticks(state_view, pool_id, configured_pool, block)
                .await?;
            let mut pool = ClmmPool::new(
                configured_pool.fee_tier,
                configured_pool.tick_spacing,
                sqrt_price_x96,
                tick,
                liquidity,
            )?;
            install_ticks(&mut pool, ticks)?;
            pools.push(HydratedPool {
                pair_id: pair.id.clone(),
                identity: PoolIdentity::V4 {
                    pool_id,
                    fee_pips: configured_pool.fee_tier,
                },
                token0: key.currency0,
                token1: key.currency1,
                pool,
            });
        }
        Ok(())
    }

    async fn hydrate_v4_ticks(
        &self,
        state_view: Address,
        pool_id: B256,
        pool: &UniswapV4PoolConfig,
        block: CanonicalBlock,
    ) -> anyhow::Result<Vec<HydratedTick>> {
        let words = word_positions(pool.tick_spacing)?;
        let bitmap_calls: Vec<_> = words
            .iter()
            .map(|word| EthCall {
                to: state_view,
                data: encode_call(
                    "getTickBitmap(bytes32,int16)",
                    &[word_b256(pool_id), word_i32(i32::from(*word))],
                ),
            })
            .collect();
        let bitmaps = self.rpc.eth_call_batch(&bitmap_calls, block).await?;
        let initialized = initialized_ticks(&words, &bitmaps, pool.tick_spacing)?;
        let tick_calls: Vec<_> = initialized
            .iter()
            .map(|tick| EthCall {
                to: state_view,
                data: encode_call(
                    "getTickLiquidity(bytes32,int24)",
                    &[word_b256(pool_id), word_i32(*tick)],
                ),
            })
            .collect();
        let outputs = self.rpc.eth_call_batch(&tick_calls, block).await?;
        decode_ticks(&initialized, &outputs)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HydratedTick {
    index: i32,
    gross: u128,
    net: i128,
}

fn install_ticks(pool: &mut ClmmPool, ticks: Vec<HydratedTick>) -> anyhow::Result<()> {
    for tick in ticks {
        ensure!(tick.gross > 0, "initialized tick has zero gross liquidity");
        pool.set_tick(tick.index, tick.gross, tick.net)?;
    }
    Ok(())
}

fn decode_ticks(indices: &[i32], outputs: &[Vec<u8>]) -> anyhow::Result<Vec<HydratedTick>> {
    ensure!(
        indices.len() == outputs.len(),
        "tick response count mismatch"
    );
    indices
        .iter()
        .copied()
        .zip(outputs)
        .map(|(index, output)| {
            Ok(HydratedTick {
                index,
                gross: decode_u128(output, 0)?,
                net: decode_i128(output, 1)?,
            })
        })
        .collect()
}

fn word_positions(tick_spacing: i32) -> anyhow::Result<Vec<i16>> {
    ensure!(tick_spacing > 0, "tick spacing must be positive");
    let min_word = div_floor(MIN_TICK, tick_spacing) >> 8;
    let max_word = div_floor(MAX_TICK, tick_spacing) >> 8;
    ensure!(
        min_word >= i32::from(i16::MIN) && max_word <= i32::from(i16::MAX),
        "bitmap word position is outside int16"
    );
    Ok((min_word..=max_word).map(|word| word as i16).collect())
}

fn initialized_ticks(
    words: &[i16],
    outputs: &[Vec<u8>],
    tick_spacing: i32,
) -> anyhow::Result<Vec<i32>> {
    ensure!(
        words.len() == outputs.len(),
        "bitmap response count mismatch"
    );
    let mut ticks = Vec::new();
    for (word_position, output) in words.iter().copied().zip(outputs) {
        let bitmap = decode_u256(output, 0)?;
        if bitmap.is_zero() {
            continue;
        }
        for bit in 0_u16..256 {
            if bitmap.bit(usize::from(bit)) {
                let compressed = i32::from(word_position) * 256 + i32::from(bit);
                let tick = compressed
                    .checked_mul(tick_spacing)
                    .context("initialized tick overflow")?;
                if (MIN_TICK..=MAX_TICK).contains(&tick) {
                    ticks.push(tick);
                }
            }
        }
    }
    Ok(ticks)
}

fn div_floor(value: i32, divisor: i32) -> i32 {
    let quotient = value / divisor;
    let remainder = value % divisor;
    if remainder != 0 && (remainder < 0) != (divisor < 0) {
        quotient - 1
    } else {
        quotient
    }
}

fn encode_call(signature: &str, words: &[[u8; 32]]) -> Vec<u8> {
    let selector = keccak256(signature.as_bytes());
    let mut data = Vec::with_capacity(4 + words.len() * 32);
    data.extend_from_slice(&selector[..4]);
    for word in words {
        data.extend_from_slice(word);
    }
    data
}

fn word_address(value: Address) -> [u8; 32] {
    let mut word = [0_u8; 32];
    word[12..].copy_from_slice(value.as_slice());
    word
}

fn word_b256(value: B256) -> [u8; 32] {
    value.into()
}

fn word_u32(value: u32) -> [u8; 32] {
    let mut word = [0_u8; 32];
    word[28..].copy_from_slice(&value.to_be_bytes());
    word
}

fn word_i32(value: i32) -> [u8; 32] {
    let mut word = [if value < 0 { 0xff } else { 0 }; 32];
    word[28..].copy_from_slice(&value.to_be_bytes());
    word
}

fn decode_word(data: &[u8], index: usize) -> anyhow::Result<&[u8]> {
    let start = index.checked_mul(32).context("ABI word offset overflow")?;
    let end = start.checked_add(32).context("ABI word end overflow")?;
    data.get(start..end)
        .with_context(|| format!("ABI response is missing word {index}"))
}

fn decode_u256(data: &[u8], index: usize) -> anyhow::Result<U256> {
    Ok(U256::from_be_slice(decode_word(data, index)?))
}

fn decode_u128(data: &[u8], index: usize) -> anyhow::Result<u128> {
    let value = decode_u256(data, index)?;
    value
        .try_into()
        .with_context(|| format!("ABI word {index} does not fit uint128"))
}

fn decode_u24(data: &[u8], index: usize) -> anyhow::Result<u32> {
    let word = decode_word(data, index)?;
    Ok(u32::from_be_bytes([0, word[29], word[30], word[31]]))
}

fn decode_i24(data: &[u8], index: usize) -> anyhow::Result<i32> {
    let word = decode_word(data, index)?;
    let raw = i32::from_be_bytes([0, word[29], word[30], word[31]]);
    Ok(if raw & 0x80_0000 != 0 {
        raw | !0xff_ffff
    } else {
        raw
    })
}

fn decode_i128(data: &[u8], index: usize) -> anyhow::Result<i128> {
    let word = decode_word(data, index)?;
    Ok(i128::from_be_bytes(
        word[16..].try_into().expect("16 bytes"),
    ))
}

fn decode_address(data: &[u8], index: usize) -> anyhow::Result<Address> {
    let word = decode_word(data, index)?;
    Ok(Address::from_slice(&word[12..]))
}

fn parse_address(name: &str, value: &str) -> anyhow::Result<Address> {
    value.parse().with_context(|| format!("invalid {name}"))
}

fn sort_tokens(token_a: Address, token_b: Address) -> (Address, Address) {
    if token_a < token_b {
        (token_a, token_b)
    } else {
        (token_b, token_a)
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, U256, address, b256};

    use super::{
        decode_i24, decode_i128, decode_u128, encode_call, initialized_ticks, word_address,
        word_b256, word_i32, word_positions, word_u32,
    };

    #[test]
    fn abi_encodes_known_v3_get_pool_call() {
        let token0 = address!("2cfc85d8e48f8eab294be644d9e25c3030863003");
        let token1 = address!("79a02482a880bce3f13e09da970dc34db4cd24d1");
        let data = encode_call(
            "getPool(address,address,uint24)",
            &[word_address(token0), word_address(token1), word_u32(3_000)],
        );
        assert_eq!(&data[..4], &[0x16, 0x98, 0xee, 0x82]);
        assert_eq!(data.len(), 100);
    }

    #[test]
    fn abi_encodes_v4_pool_id_and_signed_word() {
        let data = encode_call(
            "getTickBitmap(bytes32,int16)",
            &[
                word_b256(b256!(
                    "081028d60635d39241285edb01f6d6503b244eed2547333649daf2fe27c4a5b4"
                )),
                word_i32(-19),
            ],
        );
        assert_eq!(data.len(), 68);
        assert!(data[36..64].iter().all(|byte| *byte == 0xff));
        assert_eq!(&data[64..], &(-19_i32).to_be_bytes());
    }

    #[test]
    fn v4_state_view_selectors_match_canonical_signatures() {
        let selectors = [
            encode_call("getSlot0(bytes32)", &[]),
            encode_call("getLiquidity(bytes32)", &[]),
            encode_call("getTickBitmap(bytes32,int16)", &[]),
            encode_call("getTickLiquidity(bytes32,int24)", &[]),
        ];
        let actual: Vec<[u8; 4]> = selectors
            .iter()
            .map(|data| data[..4].try_into().unwrap())
            .collect();
        assert_eq!(
            actual,
            [
                [0xc8, 0x15, 0x64, 0x1c],
                [0xfa, 0x67, 0x93, 0xd5],
                [0x1c, 0x7c, 0xcb, 0x4c],
                [0xca, 0xed, 0xab, 0x54],
            ]
        );
    }

    #[test]
    fn decodes_signed_and_unsigned_abi_words() {
        let mut data = vec![0_u8; 64];
        data[29..32].copy_from_slice(&[0xfb, 0xa5, 0x8b]);
        data[48..64].copy_from_slice(&(-123_i128).to_be_bytes());
        assert_eq!(decode_i24(&data, 0).unwrap(), -285_301);
        assert_eq!(decode_i128(&data, 1).unwrap(), -123);

        data[32..64].fill(0);
        data[48..64].copy_from_slice(&123_u128.to_be_bytes());
        assert_eq!(decode_u128(&data, 1).unwrap(), 123);
    }

    #[test]
    fn extracts_initialized_ticks_from_bitmap_words() {
        let words = [-19_i16];
        let bitmap: U256 = U256::ONE << 109_usize;
        let outputs = [bitmap.to_be_bytes_vec()];
        assert_eq!(initialized_ticks(&words, &outputs, 60).unwrap(), [-285_300]);
    }

    #[test]
    fn scans_full_tick_domain_for_common_spacings() {
        assert_eq!(word_positions(10).unwrap().len(), 694);
        assert_eq!(word_positions(60).unwrap().len(), 116);
        assert_eq!(word_positions(200).unwrap().len(), 36);
    }

    #[test]
    fn zero_address_is_supported_for_absent_v3_pool() {
        assert!(Address::ZERO.is_zero());
    }
}

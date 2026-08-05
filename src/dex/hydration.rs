use std::collections::BTreeMap;

use alloy_primitives::{Address, B256, U256, keccak256};
use anyhow::{Context, ensure};

use crate::{
    chain::rpc::{CanonicalBlock, EthCall, JsonRpcClient},
    dex::{
        camelot_fee::{
            AdaptiveFeeConfiguration, DirectionalFees, FeeEnvelope, FeeProjectionState, Timepoint,
        },
        clmm::ClmmPool,
        pool_id::V4PoolKey,
    },
    domain::config::{
        CamelotV3PoolConfig, DexProvider, DomainSnapshot, PairConfig, UniswapV4PoolConfig,
    },
    network_runtime::{NetworkReadClass, NetworkReadCoordinator},
};

const MIN_TICK: i32 = -887_272;
const MAX_TICK: i32 = 887_272;

#[derive(Debug)]
pub struct HydratedDexState {
    pub block: CanonicalBlock,
    pub pools: Vec<HydratedPool>,
    pub unavailable: Vec<UnavailablePool>,
}

#[derive(Debug, Clone)]
pub struct HydratedPool {
    pub pair_id: String,
    pub identity: PoolIdentity,
    pub token0: Address,
    pub token1: Address,
    pub pool: ClmmPool,
    pub camelot_fee: Option<HydratedCamelotFee>,
}

#[derive(Debug, Clone)]
pub struct HydratedCamelotFee {
    pub data_storage_operator: Address,
    pub state: FeeProjectionState,
    pub envelope: FeeEnvelope,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DecodedV3CoreHead {
    pub sqrt_price_x96: U256,
    pub tick: i32,
    pub liquidity: u128,
    pub tick_spacing: i32,
}

pub(crate) fn decode_v3_core_head(outputs: &[Vec<u8>]) -> anyhow::Result<DecodedV3CoreHead> {
    ensure!(
        outputs.len() == 3,
        "partial V3 core batch cannot be published"
    );
    let sqrt_price_x96 = decode_u256(&outputs[0], 0)?;
    let tick = decode_i24(&outputs[0], 1)?;
    let liquidity = decode_u128(&outputs[1], 0)?;
    let tick_spacing = decode_i24(&outputs[2], 0)?;
    Ok(DecodedV3CoreHead {
        sqrt_price_x96,
        tick,
        liquidity,
        tick_spacing,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DecodedCamelotHead {
    sqrt_price_x96: U256,
    tick: i32,
    fee_zero_for_one: u16,
    fee_one_for_zero: u16,
    timepoint_index: u16,
    liquidity: u128,
}

fn decode_camelot_head(
    global_state: &[u8],
    liquidity: &[u8],
) -> anyhow::Result<DecodedCamelotHead> {
    ensure!(
        global_state.len() == 8 * 32,
        "Camelot globalState response has an unexpected shape"
    );
    ensure!(
        decode_bool(global_state, 7)?,
        "Camelot pool is locked at pinned block"
    );
    Ok(DecodedCamelotHead {
        sqrt_price_x96: decode_u256(global_state, 0)?,
        tick: decode_i24(global_state, 1)?,
        fee_zero_for_one: decode_u16(global_state, 2)?,
        fee_one_for_zero: decode_u16(global_state, 3)?,
        timepoint_index: decode_u16(global_state, 4)?,
        liquidity: decode_u128(liquidity, 0)?,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolIdentity {
    V3 { address: Address, fee_pips: u32 },
    PancakeV3 { address: Address, fee_pips: u32 },
    CamelotV3 { address: Address },
    V4 { pool_id: B256, fee_pips: u32 },
}

#[derive(Clone, Copy)]
struct V3PoolTarget {
    fee_pips: u32,
    expected_address: Option<Address>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnavailablePool {
    pub pair_id: String,
    pub protocol: DexProvider,
    pub fee_pips: Option<u32>,
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
    reader: DexHydrationReader<'client>,
}

#[derive(Clone, Copy)]
enum DexHydrationReader<'client> {
    Direct(&'client JsonRpcClient),
    Coordinated(&'client NetworkReadCoordinator),
}

impl<'client> DexHydrator<'client> {
    pub const fn new(rpc: &'client JsonRpcClient) -> Self {
        Self {
            reader: DexHydrationReader::Direct(rpc),
        }
    }

    pub const fn new_coordinated(reads: &'client NetworkReadCoordinator) -> Self {
        Self {
            reader: DexHydrationReader::Coordinated(reads),
        }
    }

    fn rpc(&self) -> &JsonRpcClient {
        match self.reader {
            DexHydrationReader::Direct(rpc) => rpc,
            DexHydrationReader::Coordinated(reads) => reads.rpc(),
        }
    }

    async fn read_batch(
        &self,
        calls: &[EthCall],
        block: CanonicalBlock,
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        match self.reader {
            DexHydrationReader::Direct(rpc) => rpc.eth_call_batch(calls, block).await,
            DexHydrationReader::Coordinated(reads) => Ok(reads
                .eth_call_batch(NetworkReadClass::StartupPoolHydration, calls, block)
                .await?
                .outputs),
        }
    }

    async fn read_camelot_multicall(
        &self,
        multicall: Address,
        calls: &[EthCall],
        block: CanonicalBlock,
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        let aggregate_calls: Vec<_> = calls
            .chunks(500)
            .map(|chunk| EthCall {
                to: multicall,
                data: encode_multicall3_aggregate(chunk),
            })
            .collect();
        let aggregate_outputs = self.read_batch(&aggregate_calls, block).await?;
        let mut outputs = Vec::with_capacity(calls.len());
        for (chunk, encoded) in calls.chunks(500).zip(aggregate_outputs) {
            outputs.extend(decode_multicall3_aggregate(&encoded, chunk.len())?);
        }
        ensure!(
            outputs.len() == calls.len(),
            "Camelot Multicall3 response count mismatch"
        );
        Ok(outputs)
    }

    pub async fn hydrate(&self, snapshot: &DomainSnapshot) -> anyhow::Result<HydratedDexState> {
        let block = self.rpc().latest_block().await?;
        self.hydrate_at(snapshot, block).await
    }

    pub async fn hydrate_at(
        &self,
        snapshot: &DomainSnapshot,
        block: CanonicalBlock,
    ) -> anyhow::Result<HydratedDexState> {
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
            if pair
                .dex
                .allowed_providers
                .contains(&DexProvider::PancakeSwapV3)
            {
                self.hydrate_pancake_v3(pair, block, &mut pools, &mut unavailable)
                    .await
                    .with_context(|| {
                        format!("failed to hydrate PancakeSwap V3 pair {}", pair.id)
                    })?;
            }
            if pair.dex.allowed_providers.contains(&DexProvider::CamelotV3) {
                self.hydrate_camelot_v3(pair, block, &mut pools, &mut unavailable)
                    .await
                    .with_context(|| format!("failed to hydrate Camelot V3 pair {}", pair.id))?;
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
        let factory = parse_address(
            "uniswap_v3_factory_address",
            pair.chain
                .uniswap_v3_factory_address
                .as_deref()
                .context("missing V3 factory")?,
        )?;
        let config = pair.dex.uniswap_v3.as_ref().context("missing V3 config")?;
        let targets: Vec<_> = config
            .fee_tiers
            .iter()
            .copied()
            .map(|fee_pips| V3PoolTarget {
                fee_pips,
                expected_address: None,
            })
            .collect();
        self.hydrate_v3_targets(
            pair,
            block,
            DexProvider::UniswapV3,
            factory,
            &targets,
            pools,
            unavailable,
        )
        .await
    }

    async fn hydrate_pancake_v3(
        &self,
        pair: &PairConfig,
        block: CanonicalBlock,
        pools: &mut Vec<HydratedPool>,
        unavailable: &mut Vec<UnavailablePool>,
    ) -> anyhow::Result<()> {
        let factory = parse_address(
            "pancakeswap_v3_factory_address",
            pair.chain
                .pancakeswap_v3_factory_address
                .as_deref()
                .context("missing PancakeSwap V3 factory")?,
        )?;
        let config = pair
            .dex
            .pancakeswap_v3
            .as_ref()
            .context("missing PancakeSwap V3 config")?;
        let targets: Vec<_> = config
            .pools
            .iter()
            .map(|pool| {
                Ok(V3PoolTarget {
                    fee_pips: pool.fee_tier,
                    expected_address: Some(parse_address(
                        "PancakeSwap V3 expected pool",
                        &pool.expected_address,
                    )?),
                })
            })
            .collect::<anyhow::Result<_>>()?;
        self.hydrate_v3_targets(
            pair,
            block,
            DexProvider::PancakeSwapV3,
            factory,
            &targets,
            pools,
            unavailable,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn hydrate_v3_targets(
        &self,
        pair: &PairConfig,
        block: CanonicalBlock,
        protocol: DexProvider,
        factory: Address,
        targets: &[V3PoolTarget],
        pools: &mut Vec<HydratedPool>,
        unavailable: &mut Vec<UnavailablePool>,
    ) -> anyhow::Result<()> {
        ensure!(
            matches!(
                protocol,
                DexProvider::UniswapV3 | DexProvider::PancakeSwapV3
            ),
            "non-V3 provider entered V3 hydration"
        );
        let token_a = parse_address("token_a", &pair.token_a.contract)?;
        let token_b = parse_address("token_b", &pair.token_b.contract)?;
        let (token0, token1) = sort_tokens(token_a, token_b);

        let discovery_calls: Vec<_> = targets
            .iter()
            .map(|target| EthCall {
                to: factory,
                data: encode_call(
                    "getPool(address,address,uint24)",
                    &[
                        word_address(token0),
                        word_address(token1),
                        word_u32(target.fee_pips),
                    ],
                ),
            })
            .collect();
        let discovery = self.read_batch(&discovery_calls, block).await?;

        for (target, output) in targets.iter().copied().zip(discovery) {
            let fee_pips = target.fee_pips;
            let address = decode_address(&output, 0)?;
            if address.is_zero() {
                unavailable.push(UnavailablePool {
                    pair_id: pair.id.clone(),
                    protocol,
                    fee_pips: Some(fee_pips),
                    address: None,
                    pool_id: None,
                    reason: UnavailableReason::NotCreated,
                });
                continue;
            }
            if let Some(expected_address) = target.expected_address {
                ensure!(
                    address == expected_address,
                    "PancakeSwap V3 factory result differs from expected pool address"
                );
                let identity = self
                    .read_batch(
                        &[
                            EthCall {
                                to: address,
                                data: encode_call("token0()", &[]),
                            },
                            EthCall {
                                to: address,
                                data: encode_call("token1()", &[]),
                            },
                            EthCall {
                                to: address,
                                data: encode_call("fee()", &[]),
                            },
                            EthCall {
                                to: address,
                                data: encode_call("factory()", &[]),
                            },
                        ],
                        block,
                    )
                    .await?;
                ensure!(identity.len() == 4, "partial PancakeSwap V3 identity batch");
                ensure!(
                    decode_address(&identity[0], 0)? == token0
                        && decode_address(&identity[1], 0)? == token1,
                    "PancakeSwap V3 pool tokens differ from configured pair"
                );
                ensure!(
                    decode_u24(&identity[2], 0)? == fee_pips,
                    "PancakeSwap V3 pool fee differs from configured fee"
                );
                ensure!(
                    decode_address(&identity[3], 0)? == factory,
                    "PancakeSwap V3 pool factory differs from configured factory"
                );
                ensure!(
                    !self
                        .rpc()
                        .contract_code_at(address, block)
                        .await?
                        .is_empty(),
                    "PancakeSwap V3 pool has no bytecode at pinned block"
                );
            }

            let head = self
                .read_batch(
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
            let decoded = decode_v3_core_head(&head)?;
            let sqrt_price_x96 = decoded.sqrt_price_x96;
            if sqrt_price_x96.is_zero() {
                unavailable.push(UnavailablePool {
                    pair_id: pair.id.clone(),
                    protocol,
                    fee_pips: Some(fee_pips),
                    address: Some(address),
                    pool_id: None,
                    reason: UnavailableReason::Uninitialized,
                });
                continue;
            }
            let tick = decoded.tick;
            let liquidity = decoded.liquidity;
            if liquidity == 0 {
                unavailable.push(UnavailablePool {
                    pair_id: pair.id.clone(),
                    protocol,
                    fee_pips: Some(fee_pips),
                    address: Some(address),
                    pool_id: None,
                    reason: UnavailableReason::ZeroLiquidity,
                });
                continue;
            }
            let tick_spacing = decoded.tick_spacing;
            ensure!(tick_spacing > 0, "V3 pool returned invalid tick spacing");

            let ticks = self.hydrate_v3_ticks(address, tick_spacing, block).await?;
            let mut pool = ClmmPool::new(fee_pips, tick_spacing, sqrt_price_x96, tick, liquidity)?;
            install_ticks(&mut pool, ticks)?;
            pools.push(HydratedPool {
                pair_id: pair.id.clone(),
                identity: match protocol {
                    DexProvider::UniswapV3 => PoolIdentity::V3 { address, fee_pips },
                    DexProvider::PancakeSwapV3 => PoolIdentity::PancakeV3 { address, fee_pips },
                    _ => unreachable!("validated V3 provider"),
                },
                token0,
                token1,
                pool,
                camelot_fee: None,
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
        let bitmaps = self.read_batch(&bitmap_calls, block).await?;
        let initialized = initialized_ticks(&words, &bitmaps, tick_spacing)?;
        let tick_calls: Vec<_> = initialized
            .iter()
            .map(|tick| EthCall {
                to: pool,
                data: encode_call("ticks(int24)", &[word_i32(*tick)]),
            })
            .collect();
        let outputs = self.read_batch(&tick_calls, block).await?;
        decode_ticks(&initialized, &outputs)
    }

    async fn hydrate_camelot_v3(
        &self,
        pair: &PairConfig,
        block: CanonicalBlock,
        pools: &mut Vec<HydratedPool>,
        unavailable: &mut Vec<UnavailablePool>,
    ) -> anyhow::Result<()> {
        let factory = parse_address(
            "camelot_v3_factory_address",
            pair.chain
                .camelot_v3_factory_address
                .as_deref()
                .context("missing Camelot V3 factory")?,
        )?;
        let pool_deployer = parse_address(
            "camelot_v3_pool_deployer_address",
            pair.chain
                .camelot_v3_pool_deployer_address
                .as_deref()
                .context("missing Camelot V3 pool deployer")?,
        )?;
        let router = parse_address(
            "camelot_v3_router_address",
            pair.chain
                .camelot_v3_router_address
                .as_deref()
                .context("missing Camelot V3 router")?,
        )?;
        let quoter = parse_address(
            "camelot_v3_quoter_address",
            pair.chain
                .camelot_v3_quoter_address
                .as_deref()
                .context("missing Camelot V3 Quoter")?,
        )?;
        let multicall = parse_address("multicall3_address", &pair.chain.multicall3_address)?;
        let config = pair
            .dex
            .camelot_v3
            .as_ref()
            .context("missing Camelot V3 config")?;
        let token_a = parse_address("token_a", &pair.token_a.contract)?;
        let token_b = parse_address("token_b", &pair.token_b.contract)?;
        let (token0, token1) = sort_tokens(token_a, token_b);
        let head_timestamp = u32::try_from(self.rpc().canonical_block_timestamp(block).await?)
            .context("Camelot canonical block timestamp exceeds uint32")?;

        for configured_pool in &config.pools {
            self.hydrate_camelot_pool(
                pair,
                configured_pool,
                block,
                head_timestamp,
                factory,
                pool_deployer,
                router,
                quoter,
                multicall,
                token0,
                token1,
                pools,
                unavailable,
            )
            .await?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn hydrate_camelot_pool(
        &self,
        pair: &PairConfig,
        configured_pool: &CamelotV3PoolConfig,
        block: CanonicalBlock,
        head_timestamp: u32,
        factory: Address,
        pool_deployer: Address,
        router: Address,
        quoter: Address,
        multicall: Address,
        token0: Address,
        token1: Address,
        pools: &mut Vec<HydratedPool>,
        unavailable: &mut Vec<UnavailablePool>,
    ) -> anyhow::Result<()> {
        let expected_pool = parse_address(
            "Camelot V3 expected pool",
            &configured_pool.expected_address,
        )?;
        let required_incentive = parse_address(
            "Camelot V3 required active incentive",
            &configured_pool.required_active_incentive,
        )?;
        let identity = self
            .read_batch(
                &[
                    EthCall {
                        to: factory,
                        data: encode_call(
                            "poolByPair(address,address)",
                            &[word_address(token0), word_address(token1)],
                        ),
                    },
                    EthCall {
                        to: factory,
                        data: encode_call("poolDeployer()", &[]),
                    },
                    EthCall {
                        to: expected_pool,
                        data: encode_call("token0()", &[]),
                    },
                    EthCall {
                        to: expected_pool,
                        data: encode_call("token1()", &[]),
                    },
                    EthCall {
                        to: expected_pool,
                        data: encode_call("factory()", &[]),
                    },
                    EthCall {
                        to: expected_pool,
                        data: encode_call("dataStorageOperator()", &[]),
                    },
                    EthCall {
                        to: expected_pool,
                        data: encode_call("tickSpacing()", &[]),
                    },
                    EthCall {
                        to: expected_pool,
                        data: encode_call("globalState()", &[]),
                    },
                    EthCall {
                        to: expected_pool,
                        data: encode_call("liquidity()", &[]),
                    },
                    EthCall {
                        to: expected_pool,
                        data: encode_call("activeIncentive()", &[]),
                    },
                    EthCall {
                        to: expected_pool,
                        data: encode_call("liquidityCooldown()", &[]),
                    },
                    EthCall {
                        to: router,
                        data: encode_call("factory()", &[]),
                    },
                    EthCall {
                        to: quoter,
                        data: encode_call("factory()", &[]),
                    },
                ],
                block,
            )
            .await?;
        ensure!(identity.len() == 13, "partial Camelot V3 identity batch");
        let discovered_pool = decode_address(&identity[0], 0)?;
        if discovered_pool.is_zero() {
            unavailable.push(UnavailablePool {
                pair_id: pair.id.clone(),
                protocol: DexProvider::CamelotV3,
                fee_pips: None,
                address: None,
                pool_id: None,
                reason: UnavailableReason::NotCreated,
            });
            return Ok(());
        }
        ensure!(
            discovered_pool == expected_pool,
            "Camelot V3 factory result differs from expected pool address"
        );
        ensure!(
            decode_address(&identity[1], 0)? == pool_deployer,
            "Camelot V3 factory pool deployer differs from configuration"
        );
        ensure!(
            decode_address(&identity[2], 0)? == token0
                && decode_address(&identity[3], 0)? == token1,
            "Camelot V3 pool tokens differ from configured pair"
        );
        ensure!(
            decode_address(&identity[4], 0)? == factory,
            "Camelot V3 pool factory differs from configuration"
        );
        let data_storage_operator = decode_address(&identity[5], 0)?;
        ensure!(
            !data_storage_operator.is_zero(),
            "Camelot V3 data storage operator is zero"
        );
        let tick_spacing = decode_i24(&identity[6], 0)?;
        ensure!(
            tick_spacing == configured_pool.expected_tick_spacing,
            "Camelot V3 tick spacing differs from configuration"
        );
        let head = decode_camelot_head(&identity[7], &identity[8])?;
        ensure!(
            decode_address(&identity[9], 0)? == required_incentive,
            "Camelot V3 active incentive differs from required value"
        );
        ensure!(
            decode_u32(&identity[10], 0)? == 0,
            "Camelot V3 liquidity cooldown is unsupported"
        );
        ensure!(
            decode_address(&identity[11], 0)? == factory
                && decode_address(&identity[12], 0)? == factory,
            "Camelot V3 router or Quoter factory differs from pool factory"
        );

        for (name, address) in [
            ("factory", factory),
            ("pool deployer", pool_deployer),
            ("pool", expected_pool),
            ("router", router),
            ("Quoter", quoter),
            ("data storage operator", data_storage_operator),
            ("Multicall3", multicall),
        ] {
            ensure!(
                !self
                    .rpc()
                    .contract_code_at(address, block)
                    .await?
                    .is_empty(),
                "Camelot V3 {name} has no bytecode at pinned block"
            );
        }

        if head.sqrt_price_x96.is_zero() {
            unavailable.push(UnavailablePool {
                pair_id: pair.id.clone(),
                protocol: DexProvider::CamelotV3,
                fee_pips: None,
                address: Some(expected_pool),
                pool_id: None,
                reason: UnavailableReason::Uninitialized,
            });
            return Ok(());
        }
        if head.liquidity == 0 {
            unavailable.push(UnavailablePool {
                pair_id: pair.id.clone(),
                protocol: DexProvider::CamelotV3,
                fee_pips: None,
                address: Some(expected_pool),
                pool_id: None,
                reason: UnavailableReason::ZeroLiquidity,
            });
            return Ok(());
        }

        // PoolState packs `liquidity` followed by the internal
        // `volumePerLiquidityInBlock` into storage slot 3. Cross-checking the
        // public liquidity getter makes an incompatible layout fail closed.
        let packed_liquidity = self
            .rpc()
            .storage_at(expected_pool, U256::from(3_u8), block)
            .await?;
        let stored_liquidity: u128 = (packed_liquidity & U256::from(u128::MAX))
            .try_into()
            .context("Camelot packed liquidity does not fit uint128")?;
        ensure!(
            stored_liquidity == head.liquidity,
            "Camelot V3 PoolState storage layout does not match reviewed layout"
        );
        let volume_per_liquidity_in_block: u128 = (packed_liquidity >> 128_usize)
            .try_into()
            .context("Camelot packed volume does not fit uint128")?;

        let configs = self
            .read_batch(
                &[
                    EthCall {
                        to: data_storage_operator,
                        data: encode_call("feeConfigZto()", &[]),
                    },
                    EthCall {
                        to: data_storage_operator,
                        data: encode_call("feeConfigOtz()", &[]),
                    },
                ],
                block,
            )
            .await?;
        ensure!(
            configs.len() == 2,
            "partial Camelot fee configuration batch"
        );
        let zero_for_one_config = decode_fee_configuration(&configs[0])?.validate()?;
        let one_for_zero_config = decode_fee_configuration(&configs[1])?.validate()?;
        let horizon = u32::try_from(configured_pool.dynamic_fee_horizon_seconds)
            .context("Camelot fee horizon exceeds uint32")?;
        let (oldest_index, timepoints) = self
            .hydrate_camelot_timepoints(
                expected_pool,
                head.timepoint_index,
                head_timestamp,
                horizon,
                block,
            )
            .await?;
        let fee_state = FeeProjectionState {
            head_timestamp,
            latest_timepoint_timestamp: timepoints
                .get(&head.timepoint_index)
                .context("Camelot latest timepoint is missing after hydration")?
                .block_timestamp,
            tick: head.tick,
            liquidity: head.liquidity,
            index: head.timepoint_index,
            oldest_index,
            current_fees: DirectionalFees {
                zero_for_one: head.fee_zero_for_one,
                one_for_zero: head.fee_one_for_zero,
            },
            volume_per_liquidity_in_block,
            zero_for_one_config,
            one_for_zero_config,
            timepoints,
        };
        let envelope = fee_state.envelope(horizon)?;
        let ticks = self
            .hydrate_camelot_ticks(expected_pool, multicall, tick_spacing, block)
            .await?;
        let mut pool = ClmmPool::new_algebra_v1_9(
            u32::from(envelope.maximum.zero_for_one),
            u32::from(envelope.maximum.one_for_zero),
            tick_spacing,
            head.sqrt_price_x96,
            head.tick,
            head.liquidity,
        )?;
        install_ticks(&mut pool, ticks)?;
        pools.push(HydratedPool {
            pair_id: pair.id.clone(),
            identity: PoolIdentity::CamelotV3 {
                address: expected_pool,
            },
            token0,
            token1,
            pool,
            camelot_fee: Some(HydratedCamelotFee {
                data_storage_operator,
                state: fee_state,
                envelope,
            }),
        });
        Ok(())
    }

    async fn hydrate_camelot_ticks(
        &self,
        pool: Address,
        multicall: Address,
        tick_spacing: i32,
        block: CanonicalBlock,
    ) -> anyhow::Result<Vec<HydratedTick>> {
        let words = algebra_word_positions();
        let calls: Vec<_> = words
            .iter()
            .map(|word| EthCall {
                to: pool,
                data: encode_call("tickTable(int16)", &[word_i32(i32::from(*word))]),
            })
            .collect();
        let bitmaps = self
            .read_camelot_multicall(multicall, &calls, block)
            .await?;
        let initialized = initialized_algebra_ticks(&words, &bitmaps, tick_spacing)?;
        let tick_calls: Vec<_> = initialized
            .iter()
            .map(|tick| EthCall {
                to: pool,
                data: encode_call("ticks(int24)", &[word_i32(*tick)]),
            })
            .collect();
        let outputs = self
            .read_camelot_multicall(multicall, &tick_calls, block)
            .await?;
        decode_ticks(&initialized, &outputs)
    }

    async fn hydrate_camelot_timepoints(
        &self,
        pool: Address,
        index: u16,
        head_timestamp: u32,
        horizon: u32,
        block: CanonicalBlock,
    ) -> anyhow::Result<(u16, BTreeMap<u16, Timepoint>)> {
        let seed_indices = [
            index,
            index.wrapping_sub(1),
            index.wrapping_add(1),
            index.wrapping_add(2),
            0,
        ];
        let seed = self
            .read_camelot_timepoints(pool, &seed_indices, block)
            .await?;
        let mut points: BTreeMap<_, _> = seed_indices.into_iter().zip(seed).collect();
        let next_index = index.wrapping_add(1);
        let oldest_index = if points
            .get(&next_index)
            .is_some_and(|point| point.initialized)
        {
            next_index
        } else {
            0
        };
        ensure!(
            points
                .get(&oldest_index)
                .is_some_and(|point| point.initialized),
            "Camelot oldest timepoint is unavailable"
        );

        for offset in 0..=horizon {
            let target = head_timestamp
                .checked_add(offset)
                .context("Camelot fee horizon timestamp overflow")?
                .wrapping_sub(86_400);
            self.hydrate_camelot_timepoint_bracket(
                pool,
                index,
                oldest_index,
                head_timestamp,
                target,
                block,
                &mut points,
            )
            .await?;
        }
        Ok((oldest_index, points))
    }

    #[allow(clippy::too_many_arguments)]
    async fn hydrate_camelot_timepoint_bracket(
        &self,
        pool: Address,
        index: u16,
        oldest_index: u16,
        current_time: u32,
        target: u32,
        block: CanonicalBlock,
        points: &mut BTreeMap<u16, Timepoint>,
    ) -> anyhow::Result<()> {
        let oldest = points
            .get(&oldest_index)
            .context("Camelot oldest timepoint is missing")?;
        let latest = points
            .get(&index)
            .context("Camelot latest timepoint is missing")?;
        ensure!(
            lte_timestamp(oldest.block_timestamp, target, current_time)
                && lte_timestamp(target, latest.block_timestamp, current_time),
            "Camelot fee window target is outside hydrated history"
        );

        let mut left = u32::from(oldest_index);
        let mut right = if index >= oldest_index {
            u32::from(index)
        } else {
            u32::from(index) + 65_536
        };
        loop {
            ensure!(left <= right, "Camelot timepoint binary search exhausted");
            let middle = (left + right) >> 1;
            let before_index = middle as u16;
            let after_index = before_index.wrapping_add(1);
            let fetched = self
                .read_camelot_timepoints(pool, &[before_index, after_index], block)
                .await?;
            let before = fetched[0].clone();
            let after = fetched[1].clone();
            points.insert(before_index, before.clone());
            points.insert(after_index, after.clone());
            if before.initialized && lte_timestamp(before.block_timestamp, target, current_time) {
                if after.initialized && lte_timestamp(target, after.block_timestamp, current_time) {
                    return Ok(());
                }
                left = middle + 1;
            } else {
                ensure!(middle != 0, "Camelot timepoint binary search underflow");
                right = middle - 1;
            }
        }
    }

    async fn read_camelot_timepoints(
        &self,
        pool: Address,
        indices: &[u16],
        block: CanonicalBlock,
    ) -> anyhow::Result<Vec<Timepoint>> {
        let calls: Vec<_> = indices
            .iter()
            .map(|index| EthCall {
                to: pool,
                data: encode_call("timepoints(uint256)", &[word_u32(u32::from(*index))]),
            })
            .collect();
        self.read_batch(&calls, block)
            .await?
            .iter()
            .map(|output| decode_timepoint(output))
            .collect()
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
                .read_batch(
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
                    fee_pips: Some(configured_pool.fee_tier),
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
                    fee_pips: Some(configured_pool.fee_tier),
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
                camelot_fee: None,
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
        let bitmaps = self.read_batch(&bitmap_calls, block).await?;
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
        let outputs = self.read_batch(&tick_calls, block).await?;
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

fn algebra_word_positions() -> Vec<i16> {
    ((MIN_TICK >> 8)..=(MAX_TICK >> 8))
        .map(|word| word as i16)
        .collect()
}

fn initialized_algebra_ticks(
    words: &[i16],
    outputs: &[Vec<u8>],
    tick_spacing: i32,
) -> anyhow::Result<Vec<i32>> {
    ensure!(
        words.len() == outputs.len(),
        "Camelot tick-table response count mismatch"
    );
    let mut ticks = Vec::new();
    for (word_position, output) in words.iter().copied().zip(outputs) {
        let bitmap = decode_u256(output, 0)?;
        for bit in 0_u16..256 {
            if bitmap.bit(usize::from(bit)) {
                let tick = i32::from(word_position) * 256 + i32::from(bit);
                ensure!(
                    (MIN_TICK..=MAX_TICK).contains(&tick),
                    "Camelot tick table initialized a tick outside the supported domain"
                );
                ensure!(
                    tick % tick_spacing == 0,
                    "Camelot initialized tick does not align to current spacing"
                );
                ticks.push(tick);
            }
        }
    }
    Ok(ticks)
}

fn lte_timestamp(a: u32, b: u32, current_time: u32) -> bool {
    let a_overflowed = a > current_time;
    if a_overflowed == (b > current_time) {
        a <= b
    } else {
        a_overflowed
    }
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

fn encode_multicall3_aggregate(calls: &[EthCall]) -> Vec<u8> {
    let selector = keccak256("aggregate3((address,bool,bytes)[])".as_bytes());
    let tuple_encodings: Vec<Vec<u8>> = calls
        .iter()
        .map(|call| {
            let padded_length = call.data.len().div_ceil(32) * 32;
            let mut tuple = Vec::with_capacity(4 * 32 + padded_length);
            tuple.extend_from_slice(&word_address(call.to));
            tuple.extend_from_slice(&[0_u8; 32]);
            tuple.extend_from_slice(&word_u32(96));
            tuple.extend_from_slice(&word_u32(call.data.len() as u32));
            tuple.extend_from_slice(&call.data);
            tuple.resize(4 * 32 + padded_length, 0);
            tuple
        })
        .collect();
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&selector[..4]);
    encoded.extend_from_slice(&word_u32(32));
    encoded.extend_from_slice(&word_u32(calls.len() as u32));
    let mut offset = calls.len() * 32;
    for tuple in &tuple_encodings {
        encoded.extend_from_slice(&word_u32(offset as u32));
        offset += tuple.len();
    }
    for tuple in tuple_encodings {
        encoded.extend_from_slice(&tuple);
    }
    encoded
}

fn decode_multicall3_aggregate(
    encoded: &[u8],
    expected_count: usize,
) -> anyhow::Result<Vec<Vec<u8>>> {
    let root = decode_usize_at(encoded, 0)?;
    let count = decode_usize_at(encoded, root)?;
    ensure!(
        count == expected_count,
        "Camelot Multicall3 inner result count mismatch"
    );
    let heads = root
        .checked_add(32)
        .context("Multicall3 result head overflow")?;
    let mut outputs = Vec::with_capacity(count);
    for index in 0..count {
        let offset_position = heads
            .checked_add(index.checked_mul(32).context("Multicall3 index overflow")?)
            .context("Multicall3 offset position overflow")?;
        let tuple = heads
            .checked_add(decode_usize_at(encoded, offset_position)?)
            .context("Multicall3 tuple offset overflow")?;
        ensure!(
            decode_u256_at(encoded, tuple)? == U256::ONE,
            "Camelot Multicall3 inner call failed"
        );
        let bytes_start = tuple
            .checked_add(decode_usize_at(encoded, tuple + 32)?)
            .context("Multicall3 bytes offset overflow")?;
        let length = decode_usize_at(encoded, bytes_start)?;
        let data_start = bytes_start
            .checked_add(32)
            .context("Multicall3 bytes start overflow")?;
        let data_end = data_start
            .checked_add(length)
            .context("Multicall3 bytes end overflow")?;
        outputs.push(
            encoded
                .get(data_start..data_end)
                .context("Multicall3 bytes result is truncated")?
                .to_vec(),
        );
    }
    Ok(outputs)
}

fn decode_u256_at(data: &[u8], offset: usize) -> anyhow::Result<U256> {
    let end = offset.checked_add(32).context("ABI byte offset overflow")?;
    Ok(U256::from_be_slice(
        data.get(offset..end)
            .context("ABI byte range is truncated")?,
    ))
}

fn decode_usize_at(data: &[u8], offset: usize) -> anyhow::Result<usize> {
    decode_u256_at(data, offset)?
        .try_into()
        .context("ABI offset does not fit usize")
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

fn decode_u16(data: &[u8], index: usize) -> anyhow::Result<u16> {
    let word = decode_word(data, index)?;
    Ok(u16::from_be_bytes([word[30], word[31]]))
}

fn decode_u32(data: &[u8], index: usize) -> anyhow::Result<u32> {
    let word = decode_word(data, index)?;
    Ok(u32::from_be_bytes(word[28..].try_into().expect("4 bytes")))
}

fn decode_bool(data: &[u8], index: usize) -> anyhow::Result<bool> {
    let value = decode_u256(data, index)?;
    ensure!(value <= U256::ONE, "ABI bool is neither zero nor one");
    Ok(value == U256::ONE)
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

fn decode_i56(data: &[u8], index: usize) -> anyhow::Result<i128> {
    let word = decode_word(data, index)?;
    let raw = u64::from_be_bytes(word[24..].try_into().expect("8 bytes")) & 0x00ff_ffff_ffff_ffff;
    Ok(if raw & 0x0080_0000_0000_0000 != 0 {
        i128::from(raw) - (1_i128 << 56)
    } else {
        i128::from(raw)
    })
}

fn decode_fee_configuration(data: &[u8]) -> anyhow::Result<AdaptiveFeeConfiguration> {
    ensure!(
        data.len() == 9 * 32,
        "Camelot fee configuration has an unexpected shape"
    );
    Ok(AdaptiveFeeConfiguration {
        alpha1: decode_u16(data, 0)?,
        alpha2: decode_u16(data, 1)?,
        beta1: decode_u32(data, 2)?,
        beta2: decode_u32(data, 3)?,
        gamma1: decode_u16(data, 4)?,
        gamma2: decode_u16(data, 5)?,
        volume_beta: decode_u32(data, 6)?,
        volume_gamma: decode_u16(data, 7)?,
        base_fee: decode_u16(data, 8)?,
    })
}

fn decode_timepoint(data: &[u8]) -> anyhow::Result<Timepoint> {
    ensure!(
        data.len() == 7 * 32,
        "Camelot timepoint has an unexpected shape"
    );
    let seconds_per_liquidity_cumulative = decode_u256(data, 3)?;
    ensure!(
        seconds_per_liquidity_cumulative < (U256::ONE << 160),
        "Camelot seconds-per-liquidity exceeds uint160"
    );
    let volatility_cumulative: u128 = decode_u256(data, 4)?
        .try_into()
        .context("Camelot volatility cumulative does not fit uint128")?;
    ensure!(
        volatility_cumulative < (1_u128 << 88),
        "Camelot volatility cumulative exceeds uint88"
    );
    let volume_per_liquidity_cumulative = decode_u256(data, 6)?;
    ensure!(
        volume_per_liquidity_cumulative < (U256::ONE << 144),
        "Camelot volume-per-liquidity cumulative exceeds uint144"
    );
    Ok(Timepoint {
        initialized: decode_bool(data, 0)?,
        block_timestamp: decode_u32(data, 1)?,
        tick_cumulative: decode_i56(data, 2)?,
        seconds_per_liquidity_cumulative,
        volatility_cumulative,
        average_tick: decode_i24(data, 5)?,
        volume_per_liquidity_cumulative,
    })
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
        decode_i24, decode_i128, decode_multicall3_aggregate, decode_u128, decode_v3_core_head,
        encode_call, encode_multicall3_aggregate, initialized_algebra_ticks, initialized_ticks,
        word_address, word_b256, word_i32, word_positions, word_u32,
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
    fn v3_core_batch_rejects_partial_data_without_changing_unavailable_pool_semantics() {
        let slot0 = vec![0_u8; 64];
        let liquidity = vec![0_u8; 32];
        let mut tick_spacing = vec![0_u8; 32];
        tick_spacing[31] = 60;

        assert!(
            decode_v3_core_head(&[slot0.clone(), liquidity.clone()])
                .unwrap_err()
                .to_string()
                .contains("partial V3 core batch")
        );
        let decoded = decode_v3_core_head(&[slot0, liquidity, tick_spacing]).unwrap();
        assert!(decoded.sqrt_price_x96.is_zero());
        assert_eq!(decoded.liquidity, 0);
        assert_eq!(decoded.tick_spacing, 60);
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

    #[test]
    fn camelot_raw_tick_table_does_not_spacing_compress_bits() {
        let words = [-1_i16, 0_i16];
        let outputs = [
            (U256::ONE << 246_usize).to_be_bytes_vec(),
            (U256::ONE << 10_usize).to_be_bytes_vec(),
        ];
        assert_eq!(
            initialized_algebra_ticks(&words, &outputs, 10).unwrap(),
            [-10, 10]
        );
    }

    #[test]
    fn camelot_multicall3_round_trips_dynamic_result_shape() {
        let call = super::EthCall {
            to: address!("0000000000000000000000000000000000000001"),
            data: vec![0xde, 0xad, 0xbe, 0xef],
        };
        let encoded = encode_multicall3_aggregate(&[call]);
        assert_eq!(&encoded[..4], &[0x82, 0xad, 0x56, 0xcb]);
        assert_eq!(U256::from_be_slice(&encoded[4..36]), U256::from(32_u8));

        let mut response = Vec::new();
        for value in [32_u32, 1, 32, 1, 64, 32, 42] {
            response.extend_from_slice(&word_u32(value));
        }
        let outputs = decode_multicall3_aggregate(&response, 1).unwrap();
        assert_eq!(outputs, vec![word_u32(42).to_vec()]);
        response[127] = 0;
        assert!(decode_multicall3_aggregate(&response, 1).is_err());
    }
}

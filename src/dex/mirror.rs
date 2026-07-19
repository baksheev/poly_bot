use std::{collections::HashMap, time::Instant};

use alloy_primitives::{Address, B256};
use anyhow::{Context, ensure};

use crate::{
    chain::{
        logs::{ChainLog, LogPosition},
        rpc::CanonicalBlock,
    },
    dex::{
        events::{PoolLocator, PoolUpdate, decode_pool_event},
        hydration::{HydratedDexState, HydratedPool, PoolIdentity, UnavailablePool},
    },
};

pub struct DexMirror {
    pools: Vec<HydratedPool>,
    unavailable: Vec<UnavailablePool>,
    v3_indices: HashMap<Address, usize>,
    v4_indices: HashMap<B256, usize>,
    last_positions: HashMap<PoolLocator, LogPosition>,
    backfilled_through: u64,
    latest_head: CanonicalBlock,
    latest_head_received_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogApplyResult {
    Applied {
        pool_index: usize,
        kind: &'static str,
    },
    Duplicate,
    Unknown,
}

impl DexMirror {
    pub fn new(hydrated: HydratedDexState) -> anyhow::Result<Self> {
        let mut v3_indices = HashMap::new();
        let mut v4_indices = HashMap::new();
        for (index, pool) in hydrated.pools.iter().enumerate() {
            let previous = match pool.identity {
                PoolIdentity::V3 { address, .. } => v3_indices.insert(address, index),
                PoolIdentity::V4 { pool_id, .. } => v4_indices.insert(pool_id, index),
            };
            ensure!(previous.is_none(), "duplicate hydrated pool identity");
        }
        Ok(Self {
            pools: hydrated.pools,
            unavailable: hydrated.unavailable,
            v3_indices,
            v4_indices,
            last_positions: HashMap::new(),
            backfilled_through: hydrated.block.number,
            latest_head: hydrated.block,
            latest_head_received_at: Instant::now(),
        })
    }

    pub fn apply_log(&mut self, log: &ChainLog) -> anyhow::Result<LogApplyResult> {
        ensure!(!log.removed, "received removed log; rehydration required");
        if log.block_number <= self.backfilled_through {
            return Ok(LogApplyResult::Duplicate);
        }
        let Some(event) = decode_pool_event(log)? else {
            return Ok(LogApplyResult::Unknown);
        };
        if self
            .last_positions
            .get(&event.locator)
            .is_some_and(|position| log.position() <= *position)
        {
            return Ok(LogApplyResult::Duplicate);
        }
        let pool_index = match event.locator {
            PoolLocator::V3(address) => self.v3_indices.get(&address).copied(),
            PoolLocator::V4(pool_id) => self.v4_indices.get(&pool_id).copied(),
        };
        let Some(pool_index) = pool_index else {
            return Ok(LogApplyResult::Unknown);
        };
        let pool = &mut self.pools[pool_index].pool;
        match event.update {
            PoolUpdate::Swap {
                sqrt_price_x96,
                tick,
                liquidity,
                fee_pips,
            } => {
                if let Some(fee_pips) = fee_pips {
                    ensure!(
                        fee_pips == pool.fee_pips,
                        "V4 Swap fee differs from hydrated static fee"
                    );
                }
                pool.apply_swap_head(sqrt_price_x96, tick, liquidity)?;
            }
            PoolUpdate::Liquidity {
                tick_lower,
                tick_upper,
                delta,
            } => pool.apply_liquidity_delta(tick_lower, tick_upper, delta)?,
        }
        self.last_positions.insert(event.locator, log.position());
        Ok(LogApplyResult::Applied {
            pool_index,
            kind: event.kind(),
        })
    }

    pub fn finish_backfill(&mut self, head: CanonicalBlock) -> anyhow::Result<()> {
        ensure!(
            head.number >= self.latest_head.number,
            "backfill head predates hydration block"
        );
        self.backfilled_through = head.number;
        self.latest_head = head;
        self.latest_head_received_at = Instant::now();
        Ok(())
    }

    pub fn apply_head(&mut self, head: CanonicalBlock) -> anyhow::Result<bool> {
        if head.number < self.latest_head.number {
            return Ok(false);
        }
        if head.number == self.latest_head.number {
            ensure!(
                head.hash == self.latest_head.hash,
                "same-height World Chain head changed; rehydration required"
            );
            self.latest_head_received_at = Instant::now();
            return Ok(false);
        }
        ensure!(
            head.number == self.latest_head.number + 1,
            "World Chain head gap detected; rehydration required"
        );
        ensure!(
            head.parent_hash == self.latest_head.hash,
            "World Chain parent hash mismatch; rehydration required"
        );
        self.latest_head = head;
        self.latest_head_received_at = Instant::now();
        Ok(true)
    }

    pub fn is_fresh(&self, now: Instant, max_age_ms: u64) -> bool {
        now.saturating_duration_since(self.latest_head_received_at)
            .as_millis()
            <= u128::from(max_age_ms)
    }

    pub fn pool_count(&self) -> usize {
        self.pools.len()
    }

    pub fn unavailable_count(&self) -> usize {
        self.unavailable.len()
    }

    pub fn unavailable_pools(&self) -> &[UnavailablePool] {
        &self.unavailable
    }

    pub const fn latest_head(&self) -> CanonicalBlock {
        self.latest_head
    }

    pub fn pool(&self, index: usize) -> anyhow::Result<&HydratedPool> {
        self.pools.get(index).context("DEX pool index is invalid")
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, B256, U256, address};
    use uniswap_v3_math::tick_math::get_sqrt_ratio_at_tick;

    use crate::{
        chain::{logs::ChainLog, rpc::CanonicalBlock},
        dex::{
            clmm::ClmmPool,
            events::v3_swap_topic,
            hydration::{HydratedDexState, HydratedPool, PoolIdentity},
        },
    };

    use super::{DexMirror, LogApplyResult};

    fn hash(number: u64) -> B256 {
        B256::from(U256::from(number).to_be_bytes::<32>())
    }

    fn block(number: u64, parent: u64) -> CanonicalBlock {
        CanonicalBlock {
            number,
            hash: hash(number),
            parent_hash: hash(parent),
        }
    }

    fn test_mirror() -> (DexMirror, Address) {
        let address = address!("0000000000000000000000000000000000000001");
        let pool = ClmmPool::new(3_000, 60, get_sqrt_ratio_at_tick(0).unwrap(), 0, 1_000).unwrap();
        let hydrated = HydratedDexState {
            block: block(10, 9),
            pools: vec![HydratedPool {
                pair_id: "test".into(),
                identity: PoolIdentity::V3 {
                    address,
                    fee_pips: 3_000,
                },
                token0: Address::ZERO,
                token1: address,
                pool,
            }],
            unavailable: Vec::new(),
        };
        (DexMirror::new(hydrated).unwrap(), address)
    }

    fn swap_log(address: Address, block_number: u64) -> ChainLog {
        let mut data = vec![0_u8; 160];
        data[64..96].copy_from_slice(&get_sqrt_ratio_at_tick(1).unwrap().to_be_bytes::<32>());
        data[112..128].copy_from_slice(&2_000_u128.to_be_bytes());
        data[128..160].fill(0);
        data[156..160].copy_from_slice(&1_i32.to_be_bytes());
        ChainLog {
            address,
            topics: vec![v3_swap_topic(), B256::ZERO, B256::ZERO],
            data,
            block_number,
            block_hash: hash(block_number),
            transaction_index: 1,
            log_index: 2,
            removed: false,
        }
    }

    #[test]
    fn applies_ordered_logs_and_skips_the_backfilled_range() {
        let (mut mirror, address) = test_mirror();
        let log = swap_log(address, 11);
        assert!(matches!(
            mirror.apply_log(&log).unwrap(),
            LogApplyResult::Applied { .. }
        ));
        assert_eq!(mirror.pool(0).unwrap().pool.tick, 1);
        assert_eq!(mirror.pool(0).unwrap().pool.liquidity, 2_000);

        mirror.finish_backfill(block(11, 10)).unwrap();
        assert_eq!(mirror.apply_log(&log).unwrap(), LogApplyResult::Duplicate);
    }

    #[test]
    fn rejects_head_gaps_and_parent_mismatches() {
        let (mut mirror, _) = test_mirror();
        mirror.finish_backfill(block(11, 10)).unwrap();
        assert!(mirror.apply_head(block(12, 11)).unwrap());
        assert!(mirror.apply_head(block(14, 13)).is_err());

        let (mut mirror, _) = test_mirror();
        mirror.finish_backfill(block(11, 10)).unwrap();
        assert!(
            mirror
                .apply_head(CanonicalBlock {
                    number: 12,
                    hash: hash(12),
                    parent_hash: hash(999),
                })
                .is_err()
        );
    }
}

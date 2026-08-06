use std::{
    collections::{HashMap, VecDeque},
    time::Instant,
};

use alloy_primitives::{Address, B256};
use anyhow::{Context, ensure};

use crate::{
    chain::{
        logs::{ChainLog, LogPosition},
        rpc::CanonicalBlock,
    },
    dex::{
        camelot_fee::DirectionalFees,
        events::{
            CamelotFeeReceiptProof, PoolLocator, PoolUpdate, decode_camelot_pool_event,
            decode_camelot_v3_swap_amounts_after_event_validation, decode_pool_event,
            decode_pool_event_for_locator,
        },
        hydration::{HydratedDexState, HydratedPool, PoolIdentity, UnavailablePool},
    },
};

const RECENT_CANONICAL_TIMESTAMPS: usize = 128;

pub struct DexMirror {
    pools: Vec<HydratedPool>,
    unavailable: Vec<UnavailablePool>,
    v3_indices: HashMap<Address, usize>,
    pancake_v3_indices: HashMap<Address, usize>,
    camelot_v3_indices: HashMap<Address, usize>,
    v4_indices: HashMap<B256, usize>,
    last_positions: HashMap<PoolLocator, LogPosition>,
    last_block_hashes: HashMap<PoolLocator, B256>,
    pending_camelot_fees: Vec<Option<PendingCamelotFee>>,
    backfilled_through: u64,
    backfill_complete: bool,
    latest_head: CanonicalBlock,
    latest_head_timestamp: Option<u32>,
    recent_head_timestamps: VecDeque<(u64, B256, u32)>,
    latest_head_received_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogApplyResult {
    Applied {
        pool_index: usize,
        kind: &'static str,
        refresh_required: bool,
    },
    Duplicate,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogOrigin {
    CanonicalStream,
    ReceiptSettlement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PendingCamelotFee {
    position: LogPosition,
    block_timestamp: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeadApplyResult {
    pub advanced: bool,
    pub refresh_pool_index: Option<usize>,
}

impl DexMirror {
    pub fn new(hydrated: HydratedDexState) -> anyhow::Result<Self> {
        let mut v3_indices = HashMap::new();
        let mut pancake_v3_indices = HashMap::new();
        let mut camelot_v3_indices = HashMap::new();
        let mut v4_indices = HashMap::new();
        for (index, pool) in hydrated.pools.iter().enumerate() {
            let previous = match pool.identity {
                PoolIdentity::V3 { address, .. } => v3_indices.insert(address, index),
                PoolIdentity::PancakeV3 { address, .. } => {
                    pancake_v3_indices.insert(address, index)
                }
                PoolIdentity::CamelotV3 { address } => camelot_v3_indices.insert(address, index),
                PoolIdentity::V4 { pool_id, .. } => v4_indices.insert(pool_id, index),
            };
            ensure!(previous.is_none(), "duplicate hydrated pool identity");
        }
        let mut camelot_timestamps = hydrated.pools.iter().filter_map(|pool| {
            pool.camelot_fee
                .as_ref()
                .map(|fee| fee.state.head_timestamp)
        });
        let latest_head_timestamp = camelot_timestamps.next();
        ensure!(
            camelot_timestamps.all(|timestamp| Some(timestamp) == latest_head_timestamp),
            "hydrated Camelot pools disagree on canonical head timestamp"
        );
        let pool_count = hydrated.pools.len();
        let mut recent_head_timestamps = VecDeque::with_capacity(RECENT_CANONICAL_TIMESTAMPS);
        if let Some(timestamp) = latest_head_timestamp {
            recent_head_timestamps.push_back((
                hydrated.block.number,
                hydrated.block.hash,
                timestamp,
            ));
        }
        Ok(Self {
            pools: hydrated.pools,
            unavailable: hydrated.unavailable,
            v3_indices,
            pancake_v3_indices,
            camelot_v3_indices,
            v4_indices,
            last_positions: HashMap::new(),
            last_block_hashes: HashMap::new(),
            pending_camelot_fees: vec![None; pool_count],
            backfilled_through: hydrated.block.number,
            backfill_complete: false,
            latest_head: hydrated.block,
            latest_head_timestamp,
            recent_head_timestamps,
            latest_head_received_at: Instant::now(),
        })
    }

    pub fn apply_log(&mut self, log: &ChainLog) -> anyhow::Result<LogApplyResult> {
        ensure!(
            !self.camelot_v3_indices.contains_key(&log.address),
            "Camelot V3 log requires its canonical block timestamp"
        );
        self.apply_log_inner(log, None, LogOrigin::CanonicalStream)
    }

    pub fn apply_log_at_timestamp(
        &mut self,
        log: &ChainLog,
        block_timestamp: u32,
    ) -> anyhow::Result<LogApplyResult> {
        self.apply_log_inner(log, Some(block_timestamp), LogOrigin::CanonicalStream)
    }

    /// Applies a static-fee Swap proven by a mined transaction receipt.
    ///
    /// Receipt settlement intentionally runs after the already-queued DEX
    /// events are drained but before the corresponding `newHeads` notification
    /// is guaranteed to arrive. It may therefore advance the pool's positional
    /// frontier without advancing the mirror's canonical-head frontier. The
    /// later WebSocket copy and older static-fee positions retain the existing
    /// positional duplicate semantics. Camelot receipts use the
    /// timestamp-bound Fee+Swap path and may not bypass canonical-head
    /// validation.
    pub fn apply_static_fee_receipt_log(
        &mut self,
        log: &ChainLog,
    ) -> anyhow::Result<LogApplyResult> {
        ensure!(
            !self.camelot_v3_indices.contains_key(&log.address),
            "Camelot V3 receipt requires its canonical Fee and block timestamp"
        );
        self.apply_log_inner(log, None, LogOrigin::ReceiptSettlement)
    }

    pub fn apply_camelot_fee_receipt(
        &mut self,
        proof: CamelotFeeReceiptProof,
        block_timestamp: u32,
    ) -> anyhow::Result<LogApplyResult> {
        if self.backfill_complete {
            ensure!(
                proof.block_number <= self.latest_head.number,
                "receipt Fee arrived before its canonical head"
            );
            if proof.block_number == self.latest_head.number {
                ensure!(
                    proof.block_hash == self.latest_head.hash,
                    "receipt Fee block hash differs from canonical head"
                );
            }
        }
        if proof.block_number <= self.backfilled_through {
            return Ok(LogApplyResult::Duplicate);
        }
        let locator = PoolLocator::CamelotV3(proof.pool);
        let Some(pool_index) = self.camelot_v3_indices.get(&proof.pool).copied() else {
            return Ok(LogApplyResult::Unknown);
        };
        if let Some(position) = self.last_positions.get(&locator) {
            if proof.position() == *position {
                ensure!(
                    self.last_block_hashes.get(&locator) == Some(&proof.block_hash),
                    "duplicate receipt Fee position changed block hash; rehydration required"
                );
                return Ok(LogApplyResult::Duplicate);
            }
            ensure!(
                proof.position() > *position,
                "receipt Fee arrived out of canonical order; rehydration required"
            );
        }
        ensure!(
            self.pending_camelot_fees[pool_index].is_none(),
            "Camelot Fee was not consumed by the preceding pool action"
        );
        self.pools[pool_index]
            .camelot_fee
            .as_mut()
            .context("Camelot hydrated fee state is missing")?
            .state
            .apply_fee_timepoint(
                block_timestamp,
                DirectionalFees {
                    zero_for_one: proof.zero_for_one,
                    one_for_zero: proof.one_for_zero,
                },
            )?;
        self.pending_camelot_fees[pool_index] = Some(PendingCamelotFee {
            position: proof.position(),
            block_timestamp,
        });
        self.last_positions.insert(locator, proof.position());
        self.last_block_hashes.insert(locator, proof.block_hash);
        Ok(LogApplyResult::Applied {
            pool_index,
            kind: "fee",
            refresh_required: false,
        })
    }

    fn apply_log_inner(
        &mut self,
        log: &ChainLog,
        block_timestamp: Option<u32>,
        origin: LogOrigin,
    ) -> anyhow::Result<LogApplyResult> {
        ensure!(!log.removed, "received removed log; rehydration required");
        if self.backfill_complete && origin == LogOrigin::CanonicalStream {
            ensure!(
                log.block_number <= self.latest_head.number,
                "pool log arrived before its canonical head"
            );
            if log.block_number == self.latest_head.number {
                ensure!(
                    log.block_hash == self.latest_head.hash,
                    "pool log block hash differs from canonical head"
                );
            }
        }
        if log.block_number <= self.backfilled_through {
            return Ok(LogApplyResult::Duplicate);
        }
        let locator_hint = self
            .v3_indices
            .contains_key(&log.address)
            .then_some(PoolLocator::V3(log.address))
            .or_else(|| {
                self.pancake_v3_indices
                    .contains_key(&log.address)
                    .then_some(PoolLocator::PancakeV3(log.address))
            })
            .or_else(|| {
                self.camelot_v3_indices
                    .contains_key(&log.address)
                    .then_some(PoolLocator::CamelotV3(log.address))
            });
        let decoded = match locator_hint {
            Some(PoolLocator::CamelotV3(address)) => decode_camelot_pool_event(log, address)?,
            Some(locator) => decode_pool_event_for_locator(log, locator)?,
            None => decode_pool_event(log)?,
        };
        let Some(event) = decoded else {
            return Ok(LogApplyResult::Unknown);
        };
        if origin == LogOrigin::ReceiptSettlement {
            ensure!(
                matches!(event.update, PoolUpdate::Swap { .. }),
                "receipt settlement event is not a Swap"
            );
        }
        if let Some(position) = self.last_positions.get(&event.locator) {
            if log.position() == *position {
                ensure!(
                    self.last_block_hashes.get(&event.locator) == Some(&log.block_hash),
                    "duplicate pool position changed block hash; rehydration required"
                );
                return Ok(LogApplyResult::Duplicate);
            }
            if log.position() < *position && !matches!(event.locator, PoolLocator::CamelotV3(_)) {
                return Ok(LogApplyResult::Duplicate);
            }
            ensure!(
                log.position() > *position,
                "canonical pool event arrived out of order; rehydration required"
            );
        }
        let pool_index = match event.locator {
            PoolLocator::V3(address) => self.v3_indices.get(&address).copied(),
            PoolLocator::PancakeV3(address) => self.pancake_v3_indices.get(&address).copied(),
            PoolLocator::CamelotV3(address) => self.camelot_v3_indices.get(&address).copied(),
            PoolLocator::V4(pool_id) => self.v4_indices.get(&pool_id).copied(),
        };
        let Some(pool_index) = pool_index else {
            return Ok(LogApplyResult::Unknown);
        };
        let mut refresh_required = true;
        let hydrated = &mut self.pools[pool_index];
        match event.update {
            PoolUpdate::Swap {
                sqrt_price_x96,
                tick,
                liquidity,
                fee_pips,
            } => {
                if let Some(fee_pips) = fee_pips {
                    ensure!(
                        fee_pips == hydrated.pool.fee_pips,
                        "V4 Swap fee differs from hydrated static fee"
                    );
                }
                if let PoolLocator::CamelotV3(_) = event.locator {
                    let timestamp = block_timestamp
                        .context("Camelot V3 log has no canonical block timestamp")?;
                    let (amount0, amount1) =
                        decode_camelot_v3_swap_amounts_after_event_validation(log);
                    let HydratedPool {
                        pool, camelot_fee, ..
                    } = hydrated;
                    let state = &mut camelot_fee
                        .as_mut()
                        .context("Camelot hydrated fee state is missing")?
                        .state;
                    validate_camelot_timepoint_link(
                        &mut self.pending_camelot_fees[pool_index],
                        log,
                        timestamp,
                        state.latest_timepoint_timestamp,
                        true,
                    )?;
                    pool.apply_swap_head(sqrt_price_x96, tick, liquidity)?;
                    state.apply_swap_after_timepoint_validation(
                        timestamp, amount0, amount1, tick, liquidity,
                    )?;
                } else {
                    hydrated
                        .pool
                        .apply_swap_head(sqrt_price_x96, tick, liquidity)?;
                }
            }
            PoolUpdate::Liquidity {
                tick_lower,
                tick_upper,
                delta,
            } => {
                if let PoolLocator::CamelotV3(_) = event.locator {
                    let timestamp = block_timestamp
                        .context("Camelot V3 log has no canonical block timestamp")?;
                    let active = delta != 0
                        && tick_lower <= hydrated.pool.tick
                        && hydrated.pool.tick < tick_upper;
                    let latest_timestamp = hydrated
                        .camelot_fee
                        .as_ref()
                        .context("Camelot hydrated fee state is missing")?
                        .state
                        .latest_timepoint_timestamp;
                    validate_camelot_timepoint_link(
                        &mut self.pending_camelot_fees[pool_index],
                        log,
                        timestamp,
                        latest_timestamp,
                        active,
                    )?;
                    hydrated
                        .pool
                        .apply_liquidity_delta(tick_lower, tick_upper, delta)?;
                    if active {
                        let state = &mut hydrated
                            .camelot_fee
                            .as_mut()
                            .context("Camelot hydrated fee state is missing")?
                            .state;
                        state.apply_liquidity_head(
                            timestamp,
                            hydrated.pool.tick,
                            hydrated.pool.liquidity,
                        )?;
                    }
                } else {
                    hydrated
                        .pool
                        .apply_liquidity_delta(tick_lower, tick_upper, delta)?;
                }
            }
            PoolUpdate::Fee {
                zero_for_one,
                one_for_zero,
            } => {
                let PoolLocator::CamelotV3(_) = event.locator else {
                    anyhow::bail!("Camelot Fee was routed to another provider")
                };
                ensure!(
                    self.pending_camelot_fees[pool_index].is_none(),
                    "Camelot Fee was not consumed by the preceding pool action"
                );
                let timestamp =
                    block_timestamp.context("Camelot V3 log has no canonical block timestamp")?;
                hydrated
                    .camelot_fee
                    .as_mut()
                    .context("Camelot hydrated fee state is missing")?
                    .state
                    .apply_fee_timepoint(
                        timestamp,
                        DirectionalFees {
                            zero_for_one,
                            one_for_zero,
                        },
                    )?;
                self.pending_camelot_fees[pool_index] = Some(PendingCamelotFee {
                    position: log.position(),
                    block_timestamp: timestamp,
                });
                refresh_required = false;
            }
            PoolUpdate::TickSpacing { value } => {
                anyhow::bail!("Camelot TickSpacing changed to {value}; pinned rehydration required")
            }
            PoolUpdate::Incentive { address } => {
                anyhow::bail!(
                    "Camelot Incentive changed to {address:#x}; pinned rehydration required"
                )
            }
        }
        self.last_positions.insert(event.locator, log.position());
        self.last_block_hashes.insert(event.locator, log.block_hash);
        Ok(LogApplyResult::Applied {
            pool_index,
            kind: event.kind(),
            refresh_required,
        })
    }

    pub fn finish_backfill(&mut self, head: CanonicalBlock) -> anyhow::Result<()> {
        ensure!(
            self.camelot_v3_indices.is_empty(),
            "Camelot V3 backfill requires the canonical head timestamp"
        );
        self.finish_backfill_at(head, None)
    }

    pub fn finish_backfill_at(
        &mut self,
        head: CanonicalBlock,
        timestamp: Option<u32>,
    ) -> anyhow::Result<()> {
        ensure!(
            head.number >= self.latest_head.number,
            "backfill head predates hydration block"
        );
        ensure!(
            self.pending_camelot_fees.iter().all(Option::is_none),
            "backfill ended with an unconsumed Camelot Fee"
        );
        self.backfilled_through = head.number;
        self.backfill_complete = true;
        self.latest_head = head;
        self.latest_head_received_at = Instant::now();
        if !self.camelot_v3_indices.is_empty() {
            self.latest_head_timestamp = timestamp;
            self.record_canonical_timestamp(
                head,
                timestamp.context("Camelot backfill head timestamp is missing")?,
            )?;
            self.refresh_camelot_heads(
                timestamp.context("Camelot backfill head timestamp is missing")?,
            )?;
        }
        Ok(())
    }

    pub fn apply_head(
        &mut self,
        head: CanonicalBlock,
        received_at: Instant,
    ) -> anyhow::Result<bool> {
        ensure!(
            self.camelot_v3_indices.is_empty(),
            "Camelot V3 head requires its canonical timestamp"
        );
        Ok(self.apply_head_at(head, None, received_at)?.advanced)
    }

    pub fn apply_head_at(
        &mut self,
        head: CanonicalBlock,
        timestamp: Option<u32>,
        received_at: Instant,
    ) -> anyhow::Result<HeadApplyResult> {
        if head.number < self.latest_head.number {
            return Ok(HeadApplyResult {
                advanced: false,
                refresh_pool_index: None,
            });
        }
        if head.number == self.latest_head.number {
            ensure!(
                head.hash == self.latest_head.hash,
                "same-height World Chain head changed; rehydration required"
            );
            if !self.camelot_v3_indices.is_empty() {
                ensure!(
                    timestamp == self.latest_head_timestamp,
                    "same-height Camelot head changed timestamp; rehydration required"
                );
                self.record_canonical_timestamp(
                    head,
                    timestamp.context("Camelot canonical head timestamp is missing")?,
                )?;
            }
            self.latest_head_received_at = received_at;
            return Ok(HeadApplyResult {
                advanced: false,
                refresh_pool_index: None,
            });
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
        self.latest_head_timestamp = timestamp;
        self.latest_head_received_at = received_at;
        let refresh_pool_index = if self.camelot_v3_indices.is_empty() {
            None
        } else {
            let timestamp = timestamp.context("Camelot canonical head timestamp is missing")?;
            self.record_canonical_timestamp(head, timestamp)?;
            self.refresh_camelot_heads(timestamp)?
        };
        Ok(HeadApplyResult {
            advanced: true,
            refresh_pool_index,
        })
    }

    fn refresh_camelot_heads(&mut self, timestamp: u32) -> anyhow::Result<Option<usize>> {
        let mut changed = None;
        for index in self.camelot_v3_indices.values().copied() {
            let hydrated = &mut self.pools[index];
            hydrated
                .camelot_fee
                .as_mut()
                .context("Camelot hydrated fee state is missing")?
                .state
                .advance_head(timestamp)?;
            if refresh_camelot_envelope(hydrated)? {
                ensure!(
                    changed.replace(index).is_none(),
                    "one head changed multiple Camelot fee envelopes"
                );
            }
        }
        Ok(changed)
    }

    pub fn is_fresh(&self, now: Instant, max_age_ms: u64) -> bool {
        now.saturating_duration_since(self.latest_head_received_at)
            .as_millis()
            <= u128::from(max_age_ms)
    }

    pub fn pool_count(&self) -> usize {
        self.pools.len()
    }

    pub fn is_camelot_address(&self, address: Address) -> bool {
        self.camelot_v3_indices.contains_key(&address)
    }

    pub fn has_camelot_pools(&self) -> bool {
        !self.camelot_v3_indices.is_empty()
    }

    /// Fee projection and curve fee publication are intentionally separate
    /// from canonical event application so the event owner stays comparable
    /// to the existing Uniswap path. Callers invoke this before snapshotting a
    /// prepared-curve build request.
    pub fn refresh_pool_for_publication(&mut self, index: usize) -> anyhow::Result<bool> {
        let hydrated = self
            .pools
            .get_mut(index)
            .context("DEX pool index is invalid")?;
        if hydrated.camelot_fee.is_some() {
            refresh_camelot_envelope(hydrated)
        } else {
            Ok(false)
        }
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

    pub const fn latest_head_received_at(&self) -> Instant {
        self.latest_head_received_at
    }

    pub fn canonical_timestamp_for_log(&self, log: &ChainLog) -> Option<u32> {
        self.recent_head_timestamps
            .iter()
            .rev()
            .find(|(number, hash, _)| *number == log.block_number && *hash == log.block_hash)
            .map(|(_, _, timestamp)| *timestamp)
    }

    pub fn pool(&self, index: usize) -> anyhow::Result<&HydratedPool> {
        self.pools.get(index).context("DEX pool index is invalid")
    }

    pub fn pool_index(&self, locator: PoolLocator) -> Option<usize> {
        match locator {
            PoolLocator::V3(address) => self.v3_indices.get(&address).copied(),
            PoolLocator::PancakeV3(address) => self.pancake_v3_indices.get(&address).copied(),
            PoolLocator::CamelotV3(address) => self.camelot_v3_indices.get(&address).copied(),
            PoolLocator::V4(pool_id) => self.v4_indices.get(&pool_id).copied(),
        }
    }

    pub fn last_position(&self, locator: PoolLocator) -> Option<LogPosition> {
        self.last_positions.get(&locator).copied()
    }

    pub const fn backfilled_through(&self) -> u64 {
        self.backfilled_through
    }

    fn record_canonical_timestamp(
        &mut self,
        head: CanonicalBlock,
        timestamp: u32,
    ) -> anyhow::Result<()> {
        if let Some((number, hash, existing)) = self.recent_head_timestamps.back()
            && *number == head.number
        {
            ensure!(
                *hash == head.hash && *existing == timestamp,
                "cached canonical head changed hash or timestamp"
            );
            return Ok(());
        }
        self.recent_head_timestamps
            .push_back((head.number, head.hash, timestamp));
        if self.recent_head_timestamps.len() > RECENT_CANONICAL_TIMESTAMPS {
            self.recent_head_timestamps.pop_front();
        }
        Ok(())
    }
}

fn validate_camelot_timepoint_link(
    pending: &mut Option<PendingCamelotFee>,
    log: &ChainLog,
    timestamp: u32,
    latest_timestamp: u32,
    action_writes_timepoint: bool,
) -> anyhow::Result<()> {
    if pending.is_none() {
        if action_writes_timepoint {
            ensure!(
                latest_timestamp == timestamp,
                "Camelot pool action is missing its preceding Fee event"
            );
        } else {
            ensure!(
                latest_timestamp <= timestamp,
                "Camelot pool action timestamp precedes latest timepoint"
            );
        }
        return Ok(());
    }
    let fee = pending.take().expect("Camelot pending Fee was checked");
    ensure!(
        action_writes_timepoint
            && fee.block_timestamp == timestamp
            && fee.position.block_number == log.block_number
            && fee.position.transaction_index == log.transaction_index
            && fee.position < log.position(),
        "Camelot Fee is not positionally linked to its pool action"
    );
    ensure!(
        latest_timestamp == timestamp,
        "Camelot Fee did not install the action timepoint"
    );
    Ok(())
}

fn refresh_camelot_envelope(hydrated: &mut HydratedPool) -> anyhow::Result<bool> {
    let fee = hydrated
        .camelot_fee
        .as_mut()
        .context("Camelot hydrated fee state is missing")?;
    let horizon = fee
        .envelope
        .last_timestamp
        .checked_sub(fee.envelope.first_timestamp)
        .context("Camelot fee envelope timestamps are not ordered")?;
    let previous = fee.envelope.maximum;
    fee.envelope = fee.state.envelope(horizon)?;
    hydrated.pool.set_algebra_directional_fees(
        u32::from(fee.envelope.maximum.zero_for_one),
        u32::from(fee.envelope.maximum.one_for_zero),
    )?;
    Ok(previous != fee.envelope.maximum)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        time::{Duration, Instant},
    };

    use alloy_primitives::{Address, B256, I256, U256, address};
    use uniswap_v3_math::tick_math::get_sqrt_ratio_at_tick;

    use crate::{
        chain::{logs::ChainLog, rpc::CanonicalBlock},
        dex::{
            camelot_fee::{
                AdaptiveFeeConfiguration, DirectionalFees, FeeProjectionState, Timepoint,
            },
            clmm::ClmmPool,
            events::{
                CamelotFeeReceiptProof, camelot_fee_topic, camelot_tick_spacing_topic,
                v3_burn_topic, v3_mint_topic, v3_swap_topic,
            },
            hydration::{HydratedCamelotFee, HydratedDexState, HydratedPool, PoolIdentity},
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
                camelot_fee: None,
            }],
            unavailable: Vec::new(),
        };
        (DexMirror::new(hydrated).unwrap(), address)
    }

    fn camelot_mirror() -> (DexMirror, Address) {
        let address = address!("0000000000000000000000000000000000000003");
        let config = AdaptiveFeeConfiguration {
            alpha1: 0,
            alpha2: 0,
            beta1: 0,
            beta2: 0,
            gamma1: 1,
            gamma2: 1,
            volume_beta: 0,
            volume_gamma: 1,
            base_fee: 100,
        };
        let mut timepoints = BTreeMap::new();
        timepoints.insert(
            0,
            Timepoint {
                initialized: true,
                block_timestamp: 100,
                tick_cumulative: 0,
                seconds_per_liquidity_cumulative: U256::ZERO,
                volatility_cumulative: 0,
                average_tick: 0,
                volume_per_liquidity_cumulative: U256::ZERO,
            },
        );
        let state = FeeProjectionState {
            head_timestamp: 100,
            latest_timepoint_timestamp: 100,
            tick: 0,
            liquidity: 1_000,
            index: 0,
            oldest_index: 0,
            current_fees: DirectionalFees {
                zero_for_one: 100,
                one_for_zero: 100,
            },
            volume_per_liquidity_in_block: 0,
            zero_for_one_config: config,
            one_for_zero_config: config,
            timepoints,
        };
        let envelope = state.envelope(2).unwrap();
        let pool =
            ClmmPool::new_algebra_v1_9(100, 100, 10, get_sqrt_ratio_at_tick(0).unwrap(), 0, 1_000)
                .unwrap();
        let hydrated = HydratedDexState {
            block: block(10, 9),
            pools: vec![HydratedPool {
                pair_id: "camelot".into(),
                identity: PoolIdentity::CamelotV3 { address },
                token0: Address::ZERO,
                token1: address,
                pool,
                camelot_fee: Some(HydratedCamelotFee {
                    data_storage_operator: address!("0000000000000000000000000000000000000004"),
                    state,
                    envelope,
                }),
            }],
            unavailable: Vec::new(),
        };
        (DexMirror::new(hydrated).unwrap(), address)
    }

    fn camelot_fee_log(address: Address, block_number: u64, transaction_index: u64) -> ChainLog {
        let mut data = vec![0_u8; 64];
        data[30..32].copy_from_slice(&100_u16.to_be_bytes());
        data[62..64].copy_from_slice(&100_u16.to_be_bytes());
        ChainLog {
            address,
            topics: vec![camelot_fee_topic()],
            data,
            block_number,
            block_hash: hash(block_number),
            transaction_index,
            log_index: 1,
            removed: false,
        }
    }

    fn camelot_swap_log(
        address: Address,
        block_number: u64,
        transaction_index: u64,
        log_index: u64,
        tick: i32,
    ) -> ChainLog {
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
        data[64..96].copy_from_slice(&get_sqrt_ratio_at_tick(tick).unwrap().to_be_bytes::<32>());
        data[112..128].copy_from_slice(&2_000_u128.to_be_bytes());
        data[128..160].fill(if tick < 0 { 0xff } else { 0 });
        data[156..160].copy_from_slice(&tick.to_be_bytes());
        ChainLog {
            address,
            topics: vec![v3_swap_topic(), B256::ZERO, B256::ZERO],
            data,
            block_number,
            block_hash: hash(block_number),
            transaction_index,
            log_index,
            removed: false,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn camelot_liquidity_log(
        address: Address,
        block_number: u64,
        transaction_index: u64,
        log_index: u64,
        mint: bool,
        tick_lower: i32,
        tick_upper: i32,
        amount: u128,
    ) -> ChainLog {
        let mut lower = [if tick_lower < 0 { 0xff } else { 0 }; 32];
        lower[28..].copy_from_slice(&tick_lower.to_be_bytes());
        let mut upper = [if tick_upper < 0 { 0xff } else { 0 }; 32];
        upper[28..].copy_from_slice(&tick_upper.to_be_bytes());
        let mut data = vec![0_u8; if mint { 128 } else { 96 }];
        let amount_word = if mint { 1 } else { 0 };
        let offset = amount_word * 32 + 16;
        data[offset..offset + 16].copy_from_slice(&amount.to_be_bytes());
        ChainLog {
            address,
            topics: vec![
                if mint {
                    v3_mint_topic()
                } else {
                    v3_burn_topic()
                },
                B256::ZERO,
                B256::from(lower),
                B256::from(upper),
            ],
            data,
            block_number,
            block_hash: hash(block_number),
            transaction_index,
            log_index,
            removed: false,
        }
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
    fn static_fee_receipt_can_lead_head_and_its_websocket_copy_is_bounded() {
        let (mut mirror, address) = test_mirror();
        mirror.finish_backfill(block(10, 9)).unwrap();
        let mut receipt = swap_log(address, 11);
        receipt.transaction_index = 2;

        let stream_error = mirror.apply_log(&receipt).unwrap_err();
        assert!(
            stream_error
                .to_string()
                .contains("pool log arrived before its canonical head")
        );
        assert!(matches!(
            mirror.apply_static_fee_receipt_log(&receipt).unwrap(),
            LogApplyResult::Applied {
                kind: "swap",
                refresh_required: true,
                ..
            }
        ));
        assert_eq!(mirror.pool(0).unwrap().pool.tick, 1);
        assert_eq!(mirror.pool(0).unwrap().pool.liquidity, 2_000);

        mirror.apply_head(block(11, 10), Instant::now()).unwrap();
        assert_eq!(
            mirror.apply_log(&receipt).unwrap(),
            LogApplyResult::Duplicate
        );

        let earlier = swap_log(address, 11);
        assert!(earlier.position() < receipt.position());
        assert_eq!(
            mirror.apply_log(&earlier).unwrap(),
            LogApplyResult::Duplicate
        );
        assert_eq!(mirror.pool(0).unwrap().pool.tick, 1);
        assert_eq!(mirror.pool(0).unwrap().pool.liquidity, 2_000);

        let mut changed_hash = receipt;
        changed_hash.block_hash = B256::repeat_byte(0xff);
        assert!(mirror.apply_log(&changed_hash).is_err());
    }

    #[test]
    fn camelot_fee_before_swap_is_atomic_for_publication_and_updates_volume() {
        let (mut mirror, address) = camelot_mirror();
        mirror.finish_backfill_at(block(10, 9), Some(100)).unwrap();
        let head = mirror
            .apply_head_at(block(11, 10), Some(101), Instant::now())
            .unwrap();
        assert!(head.advanced);
        assert_eq!(head.refresh_pool_index, None);

        let fee = mirror
            .apply_log_at_timestamp(&camelot_fee_log(address, 11, 3), 101)
            .unwrap();
        assert!(matches!(
            fee,
            LogApplyResult::Applied {
                kind: "fee",
                refresh_required: false,
                ..
            }
        ));
        let swap = mirror
            .apply_log_at_timestamp(&camelot_swap_log(address, 11, 3, 2, 1), 101)
            .unwrap();
        assert!(matches!(
            swap,
            LogApplyResult::Applied {
                kind: "swap",
                refresh_required: true,
                ..
            }
        ));
        mirror.refresh_pool_for_publication(0).unwrap();
        let hydrated = mirror.pool(0).unwrap();
        assert_eq!(hydrated.pool.tick, 1);
        assert_eq!(hydrated.pool.liquidity, 2_000);
        let fee = hydrated.camelot_fee.as_ref().unwrap();
        assert_eq!(fee.state.index, 1);
        assert_eq!(fee.state.latest_timestamp().unwrap(), 101);
        assert!(fee.state.volume_per_liquidity_in_block > 0);
        assert_eq!(fee.envelope.maximum.zero_for_one, 100);
    }

    #[test]
    fn camelot_missing_fee_reorder_duplicate_and_reorg_fail_closed() {
        let (mut missing, address) = camelot_mirror();
        missing.finish_backfill_at(block(10, 9), Some(100)).unwrap();
        missing
            .apply_head_at(block(11, 10), Some(101), Instant::now())
            .unwrap();
        assert!(
            missing
                .apply_log_at_timestamp(&camelot_swap_log(address, 11, 3, 2, 1), 101)
                .is_err()
        );

        let (mut mirror, address) = camelot_mirror();
        mirror.finish_backfill_at(block(10, 9), Some(100)).unwrap();
        mirror
            .apply_head_at(block(11, 10), Some(101), Instant::now())
            .unwrap();
        let fee = camelot_fee_log(address, 11, 3);
        let swap = camelot_swap_log(address, 11, 3, 2, 1);
        mirror.apply_log_at_timestamp(&fee, 101).unwrap();
        mirror.apply_log_at_timestamp(&swap, 101).unwrap();
        assert_eq!(
            mirror.apply_log_at_timestamp(&swap, 101).unwrap(),
            LogApplyResult::Duplicate
        );
        assert!(mirror.apply_log_at_timestamp(&fee, 101).is_err());

        let mut changed_hash = swap.clone();
        changed_hash.block_hash = B256::repeat_byte(9);
        assert!(mirror.apply_log_at_timestamp(&changed_hash, 101).is_err());
    }

    #[test]
    fn camelot_unchanged_head_does_not_refresh_and_tick_spacing_fails_closed() {
        let (mut mirror, address) = camelot_mirror();
        mirror.finish_backfill_at(block(10, 9), Some(100)).unwrap();
        let first = mirror
            .apply_head_at(block(11, 10), Some(101), Instant::now())
            .unwrap();
        assert_eq!(first.refresh_pool_index, None);
        let same = mirror
            .apply_head_at(block(11, 10), Some(101), Instant::now())
            .unwrap();
        assert!(!same.advanced);

        let mut data = vec![0_u8; 32];
        data[31] = 20;
        let changed = ChainLog {
            address,
            topics: vec![camelot_tick_spacing_topic()],
            data,
            block_number: 11,
            block_hash: hash(11),
            transaction_index: 4,
            log_index: 1,
            removed: false,
        };
        assert!(mirror.apply_log_at_timestamp(&changed, 101).is_err());
    }

    #[test]
    fn camelot_receipt_can_reuse_bounded_canonical_head_timestamp() {
        let (mut mirror, address) = camelot_mirror();
        let hydration_log = camelot_swap_log(address, 10, 1, 1, 0);
        assert_eq!(
            mirror.canonical_timestamp_for_log(&hydration_log),
            Some(100)
        );

        mirror.finish_backfill_at(block(10, 9), Some(100)).unwrap();
        mirror
            .apply_head_at(block(11, 10), Some(101), Instant::now())
            .unwrap();
        let receipt_log = camelot_swap_log(address, 11, 3, 2, 1);
        assert_eq!(mirror.canonical_timestamp_for_log(&receipt_log), Some(101));

        let mut wrong_hash = receipt_log;
        wrong_hash.block_hash = B256::repeat_byte(0xff);
        assert_eq!(mirror.canonical_timestamp_for_log(&wrong_hash), None);
    }

    #[test]
    fn compact_receipt_fee_apply_matches_canonical_fee_log_apply() {
        let (mut canonical, address) = camelot_mirror();
        let (mut receipt, _) = camelot_mirror();
        for mirror in [&mut canonical, &mut receipt] {
            mirror.finish_backfill_at(block(10, 9), Some(100)).unwrap();
            mirror
                .apply_head_at(block(11, 10), Some(101), Instant::now())
                .unwrap();
        }
        let fee = camelot_fee_log(address, 11, 3);
        let swap = camelot_swap_log(address, 11, 3, 2, 1);
        canonical.apply_log_at_timestamp(&fee, 101).unwrap();
        canonical.apply_log_at_timestamp(&swap, 101).unwrap();
        receipt
            .apply_camelot_fee_receipt(
                CamelotFeeReceiptProof {
                    pool: address,
                    zero_for_one: 100,
                    one_for_zero: 100,
                    block_number: 11,
                    block_hash: hash(11),
                    transaction_index: 3,
                    log_index: 1,
                },
                101,
            )
            .unwrap();
        receipt.apply_log_at_timestamp(&swap, 101).unwrap();
        canonical.refresh_pool_for_publication(0).unwrap();
        receipt.refresh_pool_for_publication(0).unwrap();

        let canonical_pool = canonical.pool(0).unwrap();
        let receipt_pool = receipt.pool(0).unwrap();
        assert_eq!(
            receipt_pool.pool.sqrt_price_x96,
            canonical_pool.pool.sqrt_price_x96
        );
        assert_eq!(receipt_pool.pool.tick, canonical_pool.pool.tick);
        assert_eq!(receipt_pool.pool.liquidity, canonical_pool.pool.liquidity);
        let canonical_fee = canonical_pool.camelot_fee.as_ref().unwrap();
        let receipt_fee = receipt_pool.camelot_fee.as_ref().unwrap();
        assert_eq!(receipt_fee.state, canonical_fee.state);
        assert_eq!(receipt_fee.envelope, canonical_fee.envelope);
    }

    #[test]
    fn camelot_active_mint_and_burn_follow_fee_timepoint_semantics() {
        let (mut mirror, address) = camelot_mirror();
        mirror.finish_backfill_at(block(10, 9), Some(100)).unwrap();
        mirror
            .apply_head_at(block(11, 10), Some(101), Instant::now())
            .unwrap();
        mirror
            .apply_log_at_timestamp(&camelot_fee_log(address, 11, 5), 101)
            .unwrap();
        let mint = camelot_liquidity_log(address, 11, 5, 2, true, -10, 10, 100);
        assert!(matches!(
            mirror.apply_log_at_timestamp(&mint, 101).unwrap(),
            LogApplyResult::Applied {
                kind: "liquidity_added",
                refresh_required: true,
                ..
            }
        ));
        assert_eq!(mirror.pool(0).unwrap().pool.liquidity, 1_100);
        assert_eq!(
            mirror
                .pool(0)
                .unwrap()
                .camelot_fee
                .as_ref()
                .unwrap()
                .state
                .liquidity,
            1_100
        );

        let burn = camelot_liquidity_log(address, 11, 6, 3, false, -10, 10, 100);
        mirror.apply_log_at_timestamp(&burn, 101).unwrap();
        assert_eq!(mirror.pool(0).unwrap().pool.liquidity, 1_000);
        assert_eq!(
            mirror
                .pool(0)
                .unwrap()
                .camelot_fee
                .as_ref()
                .unwrap()
                .state
                .liquidity,
            1_000
        );
    }

    #[test]
    #[ignore = "manual release-mode Camelot/Uniswap event and publication benchmark"]
    fn benchmark_camelot_event_apply_and_publication() {
        use std::hint::black_box;

        use crate::paired_benchmark::{
            assert_absolute_latency_with_work, assert_named_paired_non_regression_with_work,
        };

        let (mut uniswap, uniswap_address) = test_mirror();
        let (mut camelot, camelot_address) = camelot_mirror();
        let mut matched_pool = camelot.pool(0).unwrap().clone();
        matched_pool.identity = PoolIdentity::V3 {
            address: address!("0000000000000000000000000000000000000005"),
            fee_pips: matched_pool.pool.fee_pips,
        };
        matched_pool.camelot_fee = None;
        let mut matched_uniswap = DexMirror::new(HydratedDexState {
            block: block(10, 9),
            pools: vec![matched_pool],
            unavailable: Vec::new(),
        })
        .unwrap();
        let mut uniswap_log = camelot_swap_log(uniswap_address, 11, 1, 1, 0);
        let mut camelot_log = camelot_swap_log(camelot_address, 11, 1, 1, 0);
        assert_named_paired_non_regression_with_work(
            "camelot_v3_event_decode_apply",
            1.10,
            "uniswap_v3",
            "camelot_v3",
            32,
            262_144,
            || {
                uniswap_log.log_index += 1;
                black_box(uniswap.apply_log(black_box(&uniswap_log))).unwrap();
            },
            || {
                camelot_log.log_index += 1;
                black_box(camelot.apply_log_at_timestamp(black_box(&camelot_log), black_box(100)))
                    .unwrap();
            },
        );

        assert_absolute_latency_with_work(
            "camelot_v3_fee_projection_and_curve_publication",
            200_000.0,
            200_000.0,
            32,
            4_096,
            || {
                black_box(camelot.refresh_pool_for_publication(0)).unwrap();
                let pool = &camelot.pool(0).unwrap().pool;
                black_box(
                    pool.prepare_exact_input_curve_bounded(true, U256::from(10_000_u64))
                        .unwrap(),
                );
                black_box(
                    pool.prepare_exact_input_curve_bounded(false, U256::from(10_000_u64))
                        .unwrap(),
                );
            },
        );

        assert_named_paired_non_regression_with_work(
            "camelot_v3_receipt_to_lane_release_publication",
            1.15,
            "uniswap_v3",
            "camelot_v3",
            32,
            4_096,
            || {
                black_box(matched_uniswap.refresh_pool_for_publication(0)).unwrap();
                let pool = &matched_uniswap.pool(0).unwrap().pool;
                black_box(
                    pool.prepare_exact_input_curve_bounded(true, U256::from(10_000_u64))
                        .unwrap(),
                );
                black_box(
                    pool.prepare_exact_input_curve_bounded(false, U256::from(10_000_u64))
                        .unwrap(),
                );
            },
            || {
                black_box(camelot.refresh_pool_for_publication(0)).unwrap();
                let pool = &camelot.pool(0).unwrap().pool;
                black_box(
                    pool.prepare_exact_input_curve_bounded(true, U256::from(10_000_u64))
                        .unwrap(),
                );
                black_box(
                    pool.prepare_exact_input_curve_bounded(false, U256::from(10_000_u64))
                        .unwrap(),
                );
            },
        );
    }

    #[test]
    fn rejects_head_gaps_and_parent_mismatches() {
        let (mut mirror, _) = test_mirror();
        mirror.finish_backfill(block(11, 10)).unwrap();
        assert!(mirror.apply_head(block(12, 11), Instant::now()).unwrap());
        assert!(mirror.apply_head(block(14, 13), Instant::now()).is_err());

        let (mut mirror, _) = test_mirror();
        mirror.finish_backfill(block(11, 10)).unwrap();
        assert!(
            mirror
                .apply_head(
                    CanonicalBlock {
                        number: 12,
                        hash: hash(12),
                        parent_hash: hash(999),
                    },
                    Instant::now(),
                )
                .is_err()
        );
    }

    #[test]
    fn head_activity_keeps_the_mirror_fresh_without_a_pool_price_change() {
        let (mut mirror, _) = test_mirror();
        mirror.finish_backfill(block(11, 10)).unwrap();
        let original_tick = mirror.pool(0).unwrap().pool.tick;
        let received_at = Instant::now();

        assert!(mirror.apply_head(block(12, 11), received_at).unwrap());
        assert_eq!(mirror.pool(0).unwrap().pool.tick, original_tick);
        assert!(mirror.is_fresh(received_at + Duration::from_millis(29_999), 30_000));
        assert!(!mirror.is_fresh(received_at + Duration::from_millis(30_001), 30_000));
    }
}

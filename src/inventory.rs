use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use alloy_primitives::U256;
use anyhow::{Context, ensure};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum InventoryLocation {
    BinanceAccount {
        account_id: String,
    },
    EvmWallet {
        network_id: String,
        wallet_location_id: String,
    },
}

impl InventoryLocation {
    pub fn binance(account_id: impl Into<String>) -> anyhow::Result<Self> {
        let account_id = account_id.into();
        validate_id("Binance inventory account", &account_id, 96)?;
        Ok(Self::BinanceAccount { account_id })
    }

    pub fn evm_wallet(
        network_id: impl Into<String>,
        wallet_location_id: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let network_id = network_id.into();
        let wallet_location_id = wallet_location_id.into();
        validate_id("inventory network", &network_id, 96)?;
        validate_id("wallet inventory location", &wallet_location_id, 120)?;
        Ok(Self::EvmWallet {
            network_id,
            wallet_location_id,
        })
    }

    pub const fn kind_label(&self) -> &'static str {
        match self {
            Self::BinanceAccount { .. } => "binance_account",
            Self::EvmWallet { .. } => "evm_wallet",
        }
    }

    pub fn stable_id(&self) -> &str {
        match self {
            Self::BinanceAccount { account_id } => account_id,
            Self::EvmWallet {
                wallet_location_id, ..
            } => wallet_location_id,
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct InventoryKey {
    pub location: InventoryLocation,
    pub venue_asset_id: String,
}

impl InventoryKey {
    pub fn new(
        location: InventoryLocation,
        venue_asset_id: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let venue_asset_id = venue_asset_id.into();
        validate_id("venue asset id", &venue_asset_id, 160)?;
        Ok(Self {
            location,
            venue_asset_id,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InventoryClaim {
    pub key: InventoryKey,
    pub amount: U256,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReservationPurpose {
    TradePrimary,
    TradeRecovery,
    Rebalance,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReservationRequest {
    pub operation_id: String,
    pub purpose: ReservationPurpose,
    pub claims: Vec<InventoryClaim>,
    pub settlement_locations: BTreeSet<InventoryLocation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReservationState {
    Active,
    PendingSettlement {
        location_generations: BTreeMap<InventoryLocation, u64>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InventoryReservation {
    pub request: ReservationRequest,
    pub state: ReservationState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InsufficientAvailableInventory {
    pub key: InventoryKey,
    pub requested: U256,
    pub observed: U256,
    pub reserved: U256,
    pub available: U256,
}

impl InsufficientAvailableInventory {
    pub fn caused_by_active_reservations(&self) -> bool {
        self.requested <= self.observed && self.requested > self.available
    }
}

impl std::fmt::Display for InsufficientAvailableInventory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "insufficient available {} inventory: requested {}, observed {}, reserved {}, available {}",
            self.key.venue_asset_id, self.requested, self.observed, self.reserved, self.available
        )
    }
}

impl std::error::Error for InsufficientAvailableInventory {}

/// Single atomic owner for every strategy and rebalance inventory claim.
///
/// A key is the exact `(inventory_location, venue_asset_id)` pair. Observed
/// balances remain authoritative; reservations only reduce availability and
/// are pre-aggregated so admission never scans all in-flight operations.
#[derive(Clone, Debug, Default)]
pub struct InventoryReservations {
    observed: BTreeMap<InventoryKey, U256>,
    location_generations: BTreeMap<InventoryLocation, u64>,
    reservations: BTreeMap<String, InventoryReservation>,
    reserved_totals: BTreeMap<InventoryKey, U256>,
}

#[derive(Clone, Debug, Default)]
pub struct SharedInventoryReservations {
    inner: Arc<Mutex<InventoryReservations>>,
}

impl SharedInventoryReservations {
    fn lock(&self) -> std::sync::MutexGuard<'_, InventoryReservations> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn update_location(
        &self,
        location: InventoryLocation,
        generation: u64,
        balances: impl IntoIterator<Item = (String, U256)>,
    ) -> anyhow::Result<bool> {
        self.lock().update_location(location, generation, balances)
    }

    pub fn update_location_assets(
        &self,
        location: InventoryLocation,
        generation: u64,
        balances: impl IntoIterator<Item = (String, U256)>,
    ) -> anyhow::Result<bool> {
        self.lock()
            .update_location_assets(location, generation, balances)
    }

    pub fn observed(&self, key: &InventoryKey) -> Option<U256> {
        self.lock().observed(key)
    }

    pub fn reserved(&self, key: &InventoryKey) -> U256 {
        self.lock().reserved(key)
    }

    pub fn available(&self, key: &InventoryKey) -> anyhow::Result<U256> {
        self.lock().available(key)
    }

    pub fn reservation(&self, operation_id: &str) -> Option<InventoryReservation> {
        self.lock().reservation(operation_id).cloned()
    }

    pub fn reserve(&self, request: ReservationRequest) -> anyhow::Result<()> {
        self.lock().reserve(request)
    }

    pub fn mark_pending_settlement(&self, operation_id: &str) -> anyhow::Result<()> {
        self.lock().mark_pending_settlement(operation_id)
    }

    pub fn release_unsubmitted(&self, operation_id: &str) -> anyhow::Result<()> {
        self.lock().release_unsubmitted(operation_id)
    }

    pub fn portfolio_snapshot(&self) -> InventoryPortfolioSnapshot {
        self.lock().portfolio_snapshot()
    }

    pub fn active_operation_ids(&self) -> Vec<String> {
        self.lock()
            .active_operation_ids()
            .into_iter()
            .map(str::to_owned)
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct InventoryPortfolioSnapshot {
    pub observed: BTreeMap<InventoryKey, U256>,
    pub reserved_totals: BTreeMap<InventoryKey, U256>,
}

impl InventoryReservations {
    pub fn update_location(
        &mut self,
        location: InventoryLocation,
        generation: u64,
        balances: impl IntoIterator<Item = (String, U256)>,
    ) -> anyhow::Result<bool> {
        ensure!(generation > 0, "inventory generation must be positive");
        if self
            .location_generations
            .get(&location)
            .is_some_and(|current| generation <= *current)
        {
            return Ok(false);
        }
        let mut replacement = BTreeMap::new();
        for (venue_asset_id, amount) in balances {
            let key = InventoryKey::new(location.clone(), venue_asset_id)?;
            ensure!(
                replacement.insert(key, amount).is_none(),
                "duplicate venue asset in inventory snapshot"
            );
        }
        ensure!(
            !replacement.is_empty(),
            "inventory snapshot must contain at least one venue asset"
        );
        self.observed.retain(|key, _| key.location != location);
        self.observed.extend(replacement);
        self.location_generations.insert(location, generation);
        self.release_reconciled();
        Ok(true)
    }

    pub fn update_location_assets(
        &mut self,
        location: InventoryLocation,
        generation: u64,
        balances: impl IntoIterator<Item = (String, U256)>,
    ) -> anyhow::Result<bool> {
        ensure!(generation > 0, "inventory generation must be positive");
        let current_generation = self
            .location_generations
            .get(&location)
            .copied()
            .context("partial inventory update requires a complete location snapshot")?;
        if generation <= current_generation {
            return Ok(false);
        }
        let mut updates = BTreeMap::new();
        for (venue_asset_id, amount) in balances {
            let key = InventoryKey::new(location.clone(), venue_asset_id)?;
            ensure!(
                updates.insert(key, amount).is_none(),
                "duplicate venue asset in partial inventory update"
            );
        }
        ensure!(!updates.is_empty(), "partial inventory update is empty");
        self.observed.extend(updates);
        self.location_generations.insert(location, generation);
        self.release_reconciled();
        Ok(true)
    }

    pub fn observed(&self, key: &InventoryKey) -> Option<U256> {
        self.observed.get(key).copied()
    }

    pub fn observed_balances(&self) -> &BTreeMap<InventoryKey, U256> {
        &self.observed
    }

    pub fn reserved(&self, key: &InventoryKey) -> U256 {
        self.reserved_totals.get(key).copied().unwrap_or(U256::ZERO)
    }

    pub fn reserved_totals(&self) -> &BTreeMap<InventoryKey, U256> {
        &self.reserved_totals
    }

    pub fn portfolio_snapshot(&self) -> InventoryPortfolioSnapshot {
        InventoryPortfolioSnapshot {
            observed: self.observed.clone(),
            reserved_totals: self.reserved_totals.clone(),
        }
    }

    pub fn available(&self, key: &InventoryKey) -> anyhow::Result<U256> {
        let observed = self.observed(key).with_context(|| {
            format!(
                "no observed inventory for {} at {}",
                key.venue_asset_id,
                key.location.stable_id()
            )
        })?;
        observed
            .checked_sub(self.reserved(key))
            .context("reservations exceed observed inventory")
    }

    pub fn reservation(&self, operation_id: &str) -> Option<&InventoryReservation> {
        self.reservations.get(operation_id)
    }

    pub fn reserve(&mut self, request: ReservationRequest) -> anyhow::Result<()> {
        validate_request(&request)?;
        ensure!(
            !self.reservations.contains_key(&request.operation_id),
            "inventory reservation operation already exists"
        );
        for claim in &request.claims {
            ensure!(
                self.location_generations.contains_key(&claim.key.location),
                "inventory location has no observed generation"
            );
            let observed = self.observed(&claim.key).with_context(|| {
                format!(
                    "no observed inventory for {} at {}",
                    claim.key.venue_asset_id,
                    claim.key.location.stable_id()
                )
            })?;
            let reserved = self.reserved(&claim.key);
            let available = observed
                .checked_sub(reserved)
                .context("reservations exceed observed inventory")?;
            if claim.amount > available {
                return Err(InsufficientAvailableInventory {
                    key: claim.key.clone(),
                    requested: claim.amount,
                    observed,
                    reserved,
                    available,
                }
                .into());
            }
        }
        for claim in &request.claims {
            let reserved = self
                .reserved_totals
                .entry(claim.key.clone())
                .or_insert(U256::ZERO);
            *reserved = reserved
                .checked_add(claim.amount)
                .context("inventory reserved total overflow")?;
        }
        self.reservations.insert(
            request.operation_id.clone(),
            InventoryReservation {
                request,
                state: ReservationState::Active,
            },
        );
        Ok(())
    }

    pub fn release_unsubmitted(&mut self, operation_id: &str) -> anyhow::Result<()> {
        let reservation = self
            .reservations
            .get(operation_id)
            .with_context(|| format!("unknown inventory reservation {operation_id}"))?;
        ensure!(
            reservation.state == ReservationState::Active,
            "only an active reservation can be released as unsubmitted"
        );
        self.remove_reservation(operation_id);
        Ok(())
    }

    pub fn mark_pending_settlement(&mut self, operation_id: &str) -> anyhow::Result<()> {
        let reservation = self
            .reservations
            .get_mut(operation_id)
            .with_context(|| format!("unknown inventory reservation {operation_id}"))?;
        ensure!(
            reservation.state == ReservationState::Active,
            "inventory reservation is not active"
        );
        let mut generations = BTreeMap::new();
        for location in &reservation.request.settlement_locations {
            let generation = self
                .location_generations
                .get(location)
                .copied()
                .context("settlement inventory location has no generation")?;
            generations.insert(location.clone(), generation);
        }
        reservation.state = ReservationState::PendingSettlement {
            location_generations: generations,
        };
        Ok(())
    }

    pub fn active_operation_ids(&self) -> Vec<&str> {
        self.reservations.keys().map(String::as_str).collect()
    }

    fn remove_reservation(&mut self, operation_id: &str) {
        let Some(reservation) = self.reservations.remove(operation_id) else {
            return;
        };
        for claim in reservation.request.claims {
            let remove = if let Some(reserved) = self.reserved_totals.get_mut(&claim.key) {
                *reserved = reserved
                    .checked_sub(claim.amount)
                    .expect("reserved total covers every admitted claim");
                reserved.is_zero()
            } else {
                false
            };
            if remove {
                self.reserved_totals.remove(&claim.key);
            }
        }
    }

    fn release_reconciled(&mut self) {
        let reconciled = self
            .reservations
            .iter()
            .filter_map(|(operation_id, reservation)| {
                let ReservationState::PendingSettlement {
                    location_generations,
                } = &reservation.state
                else {
                    return None;
                };
                location_generations
                    .iter()
                    .all(|(location, barrier)| {
                        self.location_generations
                            .get(location)
                            .is_some_and(|current| current > barrier)
                    })
                    .then(|| operation_id.clone())
            })
            .collect::<Vec<_>>();
        for operation_id in reconciled {
            self.remove_reservation(&operation_id);
        }
    }
}

fn validate_request(request: &ReservationRequest) -> anyhow::Result<()> {
    validate_id("reservation operation id", &request.operation_id, 120)?;
    ensure!(!request.claims.is_empty(), "reservation has no claims");
    ensure!(
        !request.settlement_locations.is_empty(),
        "reservation has no settlement locations"
    );
    let mut keys = BTreeSet::new();
    for claim in &request.claims {
        validate_id("venue asset id", &claim.key.venue_asset_id, 160)?;
        ensure!(!claim.amount.is_zero(), "inventory claim amount is zero");
        ensure!(
            keys.insert(claim.key.clone()),
            "reservation has duplicate inventory claims"
        );
        ensure!(
            request.settlement_locations.contains(&claim.key.location),
            "claim location is absent from settlement locations"
        );
    }
    Ok(())
}

fn validate_id(name: &str, value: &str, maximum: usize) -> anyhow::Result<()> {
    ensure!(
        !value.is_empty() && value.len() <= maximum,
        "{name} has invalid length"
    );
    ensure!(
        value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
        }),
        "{name} contains unsupported characters"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy_primitives::U256;

    use super::{
        InventoryClaim, InventoryKey, InventoryLocation, InventoryReservations, ReservationPurpose,
        ReservationRequest,
    };

    fn binance() -> InventoryLocation {
        InventoryLocation::binance("binance-spot:primary").unwrap()
    }

    fn wallet(chain_id: u64) -> InventoryLocation {
        InventoryLocation::evm_wallet(
            format!("eip155:{chain_id}"),
            format!("eip155:{chain_id}:wallet:primary"),
        )
        .unwrap()
    }

    fn key(location: InventoryLocation, venue_asset_id: &str) -> InventoryKey {
        InventoryKey::new(location, venue_asset_id).unwrap()
    }

    fn claim(location: InventoryLocation, venue_asset_id: &str, amount: u64) -> InventoryClaim {
        InventoryClaim {
            key: key(location, venue_asset_id),
            amount: U256::from(amount),
        }
    }

    #[test]
    fn two_pairs_cannot_double_spend_account_scoped_binance_usdc() {
        let location = binance();
        let usdc = "binance-spot:primary:asset:USDC";
        let mut inventory = InventoryReservations::default();
        inventory
            .update_location(location.clone(), 1, [(usdc.to_owned(), U256::from(1_000))])
            .unwrap();
        inventory
            .reserve(ReservationRequest {
                operation_id: "wld-trade".to_owned(),
                purpose: ReservationPurpose::TradePrimary,
                claims: vec![claim(location.clone(), usdc, 700)],
                settlement_locations: [location.clone()].into_iter().collect(),
            })
            .unwrap();
        assert!(
            inventory
                .reserve(ReservationRequest {
                    operation_id: "esp-trade".to_owned(),
                    purpose: ReservationPurpose::TradePrimary,
                    claims: vec![claim(location.clone(), usdc, 301)],
                    settlement_locations: [location].into_iter().collect(),
                })
                .is_err()
        );
    }

    #[test]
    fn world_and_arbitrum_usdc_never_collide() {
        let world = wallet(480);
        let arbitrum = wallet(42_161);
        let world_usdc = "eip155:480:erc20:0x79a02482a880bce3f13e09da970dc34db4cd24d1";
        let arbitrum_usdc = "eip155:42161:erc20:0xaf88d065e77c8cc2239327c5edb3a432268e5831";
        let mut inventory = InventoryReservations::default();
        inventory
            .update_location(
                world.clone(),
                10,
                [(world_usdc.to_owned(), U256::from(100))],
            )
            .unwrap();
        inventory
            .update_location(
                arbitrum.clone(),
                20,
                [(arbitrum_usdc.to_owned(), U256::from(200))],
            )
            .unwrap();
        assert_eq!(
            inventory.available(&key(world, world_usdc)).unwrap(),
            U256::from(100)
        );
        assert_eq!(
            inventory.available(&key(arbitrum, arbitrum_usdc)).unwrap(),
            U256::from(200)
        );
    }

    #[test]
    fn trade_and_rebalance_contend_through_one_atomic_owner() {
        let location = binance();
        let usdc = "binance-spot:primary:asset:USDC";
        let mut inventory = InventoryReservations::default();
        inventory
            .update_location(location.clone(), 1, [(usdc.to_owned(), U256::from(1_000))])
            .unwrap();
        inventory
            .reserve(ReservationRequest {
                operation_id: "trade".to_owned(),
                purpose: ReservationPurpose::TradePrimary,
                claims: vec![claim(location.clone(), usdc, 800)],
                settlement_locations: [location.clone()].into_iter().collect(),
            })
            .unwrap();
        assert!(
            inventory
                .reserve(ReservationRequest {
                    operation_id: "rebalance".to_owned(),
                    purpose: ReservationPurpose::Rebalance,
                    claims: vec![claim(location.clone(), usdc, 201)],
                    settlement_locations: [location].into_iter().collect(),
                })
                .is_err()
        );
    }

    #[test]
    fn pending_settlement_requires_every_exact_location_to_advance() {
        let account = binance();
        let world = wallet(480);
        let account_usdc = "binance-spot:primary:asset:USDC";
        let world_usdc = "eip155:480:erc20:0x79a02482a880bce3f13e09da970dc34db4cd24d1";
        let mut inventory = InventoryReservations::default();
        inventory
            .update_location(
                account.clone(),
                1,
                [(account_usdc.to_owned(), U256::from(1_000))],
            )
            .unwrap();
        inventory
            .update_location(
                world.clone(),
                10,
                [(world_usdc.to_owned(), U256::from(1_000))],
            )
            .unwrap();
        inventory
            .reserve(ReservationRequest {
                operation_id: "trade".to_owned(),
                purpose: ReservationPurpose::TradePrimary,
                claims: vec![claim(account.clone(), account_usdc, 100)],
                settlement_locations: [account.clone(), world.clone()].into_iter().collect(),
            })
            .unwrap();
        inventory.mark_pending_settlement("trade").unwrap();
        inventory
            .update_location(account, 2, [(account_usdc.to_owned(), U256::from(900))])
            .unwrap();
        assert!(inventory.reservation("trade").is_some());
        inventory
            .update_location(world, 11, [(world_usdc.to_owned(), U256::from(1_100))])
            .unwrap();
        assert!(inventory.reservation("trade").is_none());
        assert!(inventory.reserved_totals().is_empty());
    }

    #[test]
    fn partial_update_preserves_other_venue_assets() {
        let location = binance();
        let usdc = "binance-spot:primary:asset:USDC";
        let wld = "binance-spot:primary:asset:WLD";
        let mut inventory = InventoryReservations::default();
        inventory
            .update_location(
                location.clone(),
                1,
                [
                    (usdc.to_owned(), U256::from(1_000)),
                    (wld.to_owned(), U256::from(2_000)),
                ],
            )
            .unwrap();
        inventory
            .update_location_assets(location.clone(), 2, [(wld.to_owned(), U256::from(1_900))])
            .unwrap();
        assert_eq!(
            inventory.observed(&key(location.clone(), usdc)),
            Some(U256::from(1_000))
        );
        assert_eq!(
            inventory.observed(&key(location, wld)),
            Some(U256::from(1_900))
        );
    }
}

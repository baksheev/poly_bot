use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::U256;
use anyhow::{Context, ensure};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum InventoryVenue {
    Binance,
    Wallet,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct InventoryKey {
    pub venue: InventoryVenue,
    pub asset: String,
}

impl InventoryKey {
    pub fn new(venue: InventoryVenue, asset: impl Into<String>) -> anyhow::Result<Self> {
        let asset = asset.into();
        validate_id("inventory asset", &asset, 24)?;
        Ok(Self { venue, asset })
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
    pub settlement_venues: BTreeSet<InventoryVenue>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReservationState {
    Active,
    PendingSettlement {
        venue_generations: BTreeMap<InventoryVenue, u64>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InventoryReservation {
    pub request: ReservationRequest,
    pub state: ReservationState,
}

/// Single-owner accounting for process-scoped venue inventory.
///
/// Observed balances remain authoritative. Reservations only reduce available
/// balances; they never create inventory or project an external mutation as a
/// completed balance change.
#[derive(Debug, Default)]
pub struct InventoryReservations {
    observed: BTreeMap<InventoryKey, U256>,
    venue_generations: BTreeMap<InventoryVenue, u64>,
    reservations: BTreeMap<String, InventoryReservation>,
}

impl InventoryReservations {
    /// Atomically replaces all observed assets for one venue. Regressed
    /// generations are rejected without mutating state.
    pub fn update_venue(
        &mut self,
        venue: InventoryVenue,
        generation: u64,
        balances: impl IntoIterator<Item = (String, U256)>,
    ) -> anyhow::Result<bool> {
        ensure!(generation > 0, "inventory generation must be positive");
        if self
            .venue_generations
            .get(&venue)
            .is_some_and(|current| generation <= *current)
        {
            return Ok(false);
        }
        let mut replacement = BTreeMap::new();
        for (asset, amount) in balances {
            let key = InventoryKey::new(venue, asset)?;
            ensure!(
                replacement.insert(key, amount).is_none(),
                "duplicate asset in inventory snapshot"
            );
        }
        ensure!(
            !replacement.is_empty(),
            "inventory snapshot must contain at least one asset"
        );
        self.observed.retain(|key, _| key.venue != venue);
        self.observed.extend(replacement);
        self.venue_generations.insert(venue, generation);
        self.release_reconciled();
        Ok(true)
    }

    /// Applies a primary-stream partial update after a complete venue snapshot
    /// has established the initial asset set.
    pub fn update_venue_assets(
        &mut self,
        venue: InventoryVenue,
        generation: u64,
        balances: impl IntoIterator<Item = (String, U256)>,
    ) -> anyhow::Result<bool> {
        ensure!(generation > 0, "inventory generation must be positive");
        let current_generation = self
            .venue_generations
            .get(&venue)
            .copied()
            .context("partial inventory update requires a complete venue snapshot")?;
        if generation <= current_generation {
            return Ok(false);
        }
        let mut updates = BTreeMap::new();
        for (asset, amount) in balances {
            let key = InventoryKey::new(venue, asset)?;
            ensure!(
                updates.insert(key, amount).is_none(),
                "duplicate asset in partial inventory update"
            );
        }
        ensure!(!updates.is_empty(), "partial inventory update is empty");
        self.observed.extend(updates);
        self.venue_generations.insert(venue, generation);
        self.release_reconciled();
        Ok(true)
    }

    pub fn observed(&self, key: &InventoryKey) -> Option<U256> {
        self.observed.get(key).copied()
    }

    pub fn reserved(&self, key: &InventoryKey) -> U256 {
        self.reservations
            .values()
            .flat_map(|reservation| reservation.request.claims.iter())
            .filter(|claim| &claim.key == key)
            .fold(U256::ZERO, |total, claim| {
                total.saturating_add(claim.amount)
            })
    }

    pub fn available(&self, key: &InventoryKey) -> anyhow::Result<U256> {
        let observed = self
            .observed(key)
            .with_context(|| format!("no observed inventory for {}", key.asset))?;
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
                self.venue_generations.contains_key(&claim.key.venue),
                "inventory venue has no observed generation"
            );
            let available = self.available(&claim.key)?;
            ensure!(
                claim.amount <= available,
                "insufficient available {} inventory: requested {}, available {}",
                claim.key.asset,
                claim.amount,
                available
            );
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

    /// Releases an intent only when no external mutation was submitted.
    pub fn release_unsubmitted(&mut self, operation_id: &str) -> anyhow::Result<()> {
        let reservation = self
            .reservations
            .get(operation_id)
            .with_context(|| format!("unknown inventory reservation {operation_id}"))?;
        ensure!(
            reservation.state == ReservationState::Active,
            "only an active reservation can be released as unsubmitted"
        );
        self.reservations.remove(operation_id);
        Ok(())
    }

    /// Keeps inventory unavailable until every claimed venue publishes a
    /// strictly newer snapshot after a proven balanced terminal outcome.
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
        for venue in &reservation.request.settlement_venues {
            let generation = self
                .venue_generations
                .get(venue)
                .copied()
                .context("settlement inventory venue has no generation")?;
            generations.insert(*venue, generation);
        }
        reservation.state = ReservationState::PendingSettlement {
            venue_generations: generations,
        };
        Ok(())
    }

    /// Unknown or failed external outcomes deliberately have no release API.
    /// They remain Active until the parent coordinator proves a balanced
    /// outcome and calls `mark_pending_settlement`.
    pub fn active_operation_ids(&self) -> Vec<&str> {
        self.reservations.keys().map(String::as_str).collect()
    }

    fn release_reconciled(&mut self) {
        self.reservations.retain(|_, reservation| {
            let ReservationState::PendingSettlement { venue_generations } = &reservation.state
            else {
                return true;
            };
            !venue_generations.iter().all(|(venue, barrier)| {
                self.venue_generations
                    .get(venue)
                    .is_some_and(|current| current > barrier)
            })
        });
    }
}

fn validate_request(request: &ReservationRequest) -> anyhow::Result<()> {
    validate_id("reservation operation id", &request.operation_id, 120)?;
    ensure!(!request.claims.is_empty(), "reservation has no claims");
    ensure!(
        !request.settlement_venues.is_empty(),
        "reservation has no settlement venues"
    );
    let mut keys = BTreeSet::new();
    for claim in &request.claims {
        validate_id("inventory asset", &claim.key.asset, 24)?;
        ensure!(!claim.amount.is_zero(), "inventory claim amount is zero");
        ensure!(
            keys.insert(claim.key.clone()),
            "reservation has duplicate inventory claims"
        );
        ensure!(
            request.settlement_venues.contains(&claim.key.venue),
            "claim venue is absent from settlement venues"
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
        value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric()
                || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')),
        "{name} contains unsupported characters"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy_primitives::U256;

    use super::{
        InventoryClaim, InventoryKey, InventoryReservations, InventoryVenue, ReservationPurpose,
        ReservationRequest,
    };

    fn claim(venue: InventoryVenue, asset: &str, amount: u64) -> InventoryClaim {
        InventoryClaim {
            key: InventoryKey::new(venue, asset).unwrap(),
            amount: U256::from(amount),
        }
    }

    #[test]
    fn reservations_are_atomic_across_venues_and_prevent_overspend() {
        let mut inventory = InventoryReservations::default();
        inventory
            .update_venue(
                InventoryVenue::Wallet,
                10,
                [("USDC".to_owned(), U256::from(1_000))],
            )
            .unwrap();
        inventory
            .update_venue(
                InventoryVenue::Binance,
                20,
                [("WLD".to_owned(), U256::from(2_000))],
            )
            .unwrap();
        inventory
            .reserve(ReservationRequest {
                operation_id: "trade-1".to_owned(),
                purpose: ReservationPurpose::TradePrimary,
                claims: vec![
                    claim(InventoryVenue::Wallet, "USDC", 600),
                    claim(InventoryVenue::Binance, "WLD", 1_200),
                ],
                settlement_venues: [InventoryVenue::Wallet, InventoryVenue::Binance]
                    .into_iter()
                    .collect(),
            })
            .unwrap();
        assert_eq!(
            inventory
                .available(&InventoryKey::new(InventoryVenue::Wallet, "USDC").unwrap())
                .unwrap(),
            U256::from(400)
        );
        assert!(
            inventory
                .reserve(ReservationRequest {
                    operation_id: "trade-2".to_owned(),
                    purpose: ReservationPurpose::TradePrimary,
                    claims: vec![claim(InventoryVenue::Wallet, "USDC", 401)],
                    settlement_venues: [InventoryVenue::Wallet].into_iter().collect(),
                })
                .is_err()
        );
        assert!(inventory.reservation("trade-2").is_none());
    }

    #[test]
    fn balanced_operation_waits_for_source_and_destination_to_advance() {
        let mut inventory = InventoryReservations::default();
        inventory
            .update_venue(
                InventoryVenue::Wallet,
                10,
                [("USDC".to_owned(), U256::from(1_000))],
            )
            .unwrap();
        inventory
            .update_venue(
                InventoryVenue::Binance,
                20,
                [("WLD".to_owned(), U256::from(2_000))],
            )
            .unwrap();
        inventory
            .reserve(ReservationRequest {
                operation_id: "trade-1".to_owned(),
                purpose: ReservationPurpose::TradePrimary,
                claims: vec![claim(InventoryVenue::Wallet, "USDC", 600)],
                settlement_venues: [InventoryVenue::Wallet, InventoryVenue::Binance]
                    .into_iter()
                    .collect(),
            })
            .unwrap();
        inventory.mark_pending_settlement("trade-1").unwrap();
        inventory
            .update_venue(
                InventoryVenue::Wallet,
                11,
                [("USDC".to_owned(), U256::from(400))],
            )
            .unwrap();
        assert!(inventory.reservation("trade-1").is_some());
        inventory
            .update_venue(
                InventoryVenue::Binance,
                21,
                [("WLD".to_owned(), U256::from(800))],
            )
            .unwrap();
        assert!(inventory.reservation("trade-1").is_none());
    }

    #[test]
    fn unknown_outcome_has_no_accidental_release_path() {
        let mut inventory = InventoryReservations::default();
        inventory
            .update_venue(
                InventoryVenue::Binance,
                1,
                [("USDC".to_owned(), U256::from(1_000))],
            )
            .unwrap();
        inventory
            .reserve(ReservationRequest {
                operation_id: "unknown-1".to_owned(),
                purpose: ReservationPurpose::TradeRecovery,
                claims: vec![claim(InventoryVenue::Binance, "USDC", 500)],
                settlement_venues: [InventoryVenue::Binance].into_iter().collect(),
            })
            .unwrap();
        inventory
            .update_venue(
                InventoryVenue::Binance,
                2,
                [("USDC".to_owned(), U256::from(500))],
            )
            .unwrap();
        assert!(inventory.reservation("unknown-1").is_some());
        assert_eq!(
            inventory
                .available(&InventoryKey::new(InventoryVenue::Binance, "USDC").unwrap())
                .unwrap(),
            U256::ZERO
        );
    }

    #[test]
    fn regressed_snapshot_cannot_release_or_rewrite_inventory() {
        let mut inventory = InventoryReservations::default();
        inventory
            .update_venue(
                InventoryVenue::Wallet,
                10,
                [("WLD".to_owned(), U256::from(100))],
            )
            .unwrap();
        assert!(
            !inventory
                .update_venue(
                    InventoryVenue::Wallet,
                    9,
                    [("WLD".to_owned(), U256::from(999))],
                )
                .unwrap()
        );
        assert_eq!(
            inventory.observed(&InventoryKey::new(InventoryVenue::Wallet, "WLD").unwrap()),
            Some(U256::from(100))
        );
    }

    #[test]
    fn primary_stream_partial_update_preserves_unmentioned_assets() {
        let mut inventory = InventoryReservations::default();
        inventory
            .update_venue(
                InventoryVenue::Binance,
                1,
                [
                    ("USDC".to_owned(), U256::from(1_000)),
                    ("WLD".to_owned(), U256::from(2_000)),
                ],
            )
            .unwrap();
        inventory
            .update_venue_assets(
                InventoryVenue::Binance,
                2,
                [("WLD".to_owned(), U256::from(1_900))],
            )
            .unwrap();
        assert_eq!(
            inventory.observed(&InventoryKey::new(InventoryVenue::Binance, "USDC").unwrap()),
            Some(U256::from(1_000))
        );
        assert_eq!(
            inventory.observed(&InventoryKey::new(InventoryVenue::Binance, "WLD").unwrap()),
            Some(U256::from(1_900))
        );
    }
}

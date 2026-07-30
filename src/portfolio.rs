use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

use alloy_primitives::U256;
use anyhow::{Context, ensure};

use crate::{
    domain::compiled::{
        CompiledCapitalAllocatorMode, CompiledInventoryLocation, CompiledPortfolioRuntimePlan,
    },
    inventory::{
        InventoryKey, InventoryLocation, InventoryPortfolioSnapshot, InventoryReservations,
    },
    telemetry::TelemetryHandle,
};

#[derive(Clone, Debug)]
pub struct PortfolioCatalog {
    assets: BTreeMap<(InventoryLocation, String), InventoryKey>,
    economic_assets: BTreeMap<InventoryKey, String>,
    decimals: BTreeMap<InventoryKey, u8>,
    allocator_mode: CompiledCapitalAllocatorMode,
    live_rebalance_adapter: String,
}

impl PortfolioCatalog {
    pub fn from_compiled(plan: &CompiledPortfolioRuntimePlan) -> anyhow::Result<Self> {
        let mut assets = BTreeMap::new();
        let mut economic_assets = BTreeMap::new();
        let mut decimals = BTreeMap::new();
        for asset in &plan.assets {
            let location = compiled_location(&asset.location)?;
            let key = InventoryKey::new(location.clone(), asset.venue_asset_id.as_str())?;
            ensure!(
                assets
                    .insert((location, asset.symbol.clone()), key.clone())
                    .is_none(),
                "portfolio location repeats symbol {}",
                asset.symbol
            );
            ensure!(
                economic_assets
                    .insert(key.clone(), asset.economic_asset_id.as_str().to_owned())
                    .is_none(),
                "portfolio repeats venue asset {}",
                asset.venue_asset_id.as_str()
            );
            decimals.insert(key, asset.decimals);
        }
        ensure!(!assets.is_empty(), "portfolio catalog is empty");
        Ok(Self {
            assets,
            economic_assets,
            decimals,
            allocator_mode: plan.allocator_mode,
            live_rebalance_adapter: plan.live_rebalance_adapter.clone(),
        })
    }

    pub fn key(&self, location: &InventoryLocation, symbol: &str) -> anyhow::Result<InventoryKey> {
        self.assets
            .get(&(location.clone(), symbol.to_owned()))
            .cloned()
            .with_context(|| format!("portfolio has no {symbol} at {}", location.stable_id()))
    }

    pub fn economic_asset_id(&self, key: &InventoryKey) -> anyhow::Result<&str> {
        self.economic_assets
            .get(key)
            .map(String::as_str)
            .with_context(|| {
                format!(
                    "venue asset {} is not economically mapped",
                    key.venue_asset_id
                )
            })
    }

    pub fn decimals(&self, key: &InventoryKey) -> anyhow::Result<u8> {
        self.decimals
            .get(key)
            .copied()
            .with_context(|| format!("venue asset {} has no decimals", key.venue_asset_id))
    }

    pub const fn allocator_mode(&self) -> CompiledCapitalAllocatorMode {
        self.allocator_mode
    }

    pub fn live_rebalance_adapter(&self) -> &str {
        &self.live_rebalance_adapter
    }

    pub fn location_count(&self) -> usize {
        self.assets
            .keys()
            .map(|(location, _)| location)
            .collect::<BTreeSet<_>>()
            .len()
    }

    pub fn asset_count(&self) -> usize {
        self.economic_assets.len()
    }

    pub fn economic_asset_count(&self) -> usize {
        self.economic_assets.values().collect::<BTreeSet<_>>().len()
    }
}

fn compiled_location(location: &CompiledInventoryLocation) -> anyhow::Result<InventoryLocation> {
    match location {
        CompiledInventoryLocation::BinanceAccount { account_id } => {
            InventoryLocation::binance(account_id.as_str())
        }
        CompiledInventoryLocation::EvmWallet {
            network_id,
            wallet_location_id,
            ..
        } => InventoryLocation::evm_wallet(network_id.as_str(), wallet_location_id.as_str()),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AllocationIntent {
    pub proposal_id: String,
    pub economic_asset_id: String,
    pub source: InventoryKey,
    pub destination: InventoryKey,
    pub destination_credit: U256,
    pub fee: U256,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AllocationProposal {
    pub proposal_id: String,
    pub economic_asset_id: String,
    pub source: InventoryKey,
    pub destination: InventoryKey,
    pub source_debit: U256,
    pub destination_credit: U256,
    pub fee: U256,
    pub external_mutation_authorized: bool,
}

impl AllocationProposal {
    pub fn conserves(&self) -> bool {
        self.destination_credit
            .checked_add(self.fee)
            .is_some_and(|accounted| accounted == self.source_debit)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InFlightTransfer {
    pub economic_asset_id: String,
    pub source: InventoryKey,
    pub destination: InventoryKey,
    pub source_debit: U256,
    pub destination_credit: U256,
    pub fee: U256,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortfolioAudit {
    pub observed_by_economic_asset: BTreeMap<String, U256>,
    pub available_by_economic_asset: BTreeMap<String, U256>,
    pub reserved_by_economic_asset: BTreeMap<String, U256>,
    pub observed_location_assets: usize,
}

/// Account-wide planner. Production M5 uses `Shadow`; both modes are
/// structurally incapable of authorizing an external mutation.
#[derive(Clone, Debug)]
pub struct CapitalAllocator {
    mode: CompiledCapitalAllocatorMode,
    economic_assets: BTreeMap<InventoryKey, String>,
}

impl CapitalAllocator {
    pub fn new(catalog: &PortfolioCatalog) -> Self {
        Self {
            mode: catalog.allocator_mode,
            economic_assets: catalog.economic_assets.clone(),
        }
    }

    pub const fn mode(&self) -> CompiledCapitalAllocatorMode {
        self.mode
    }

    pub fn audit(&self, inventory: &InventoryReservations) -> anyhow::Result<PortfolioAudit> {
        self.audit_snapshot(&inventory.portfolio_snapshot())
    }

    pub fn audit_snapshot(
        &self,
        inventory: &InventoryPortfolioSnapshot,
    ) -> anyhow::Result<PortfolioAudit> {
        let mut observed = BTreeMap::<String, U256>::new();
        let mut reserved = BTreeMap::<String, U256>::new();
        for (key, amount) in &inventory.observed {
            let economic = self.mapping(key)?;
            add_amount(&mut observed, economic, *amount)?;
            let reserved_amount = inventory
                .reserved_totals
                .get(key)
                .copied()
                .unwrap_or(U256::ZERO);
            ensure!(
                reserved_amount <= *amount,
                "reserved portfolio amount exceeds observed balance"
            );
            add_amount(&mut reserved, economic, reserved_amount)?;
        }
        for key in inventory.reserved_totals.keys() {
            ensure!(
                inventory.observed.contains_key(key),
                "reservation references an unobserved portfolio asset"
            );
        }
        let available = observed
            .iter()
            .map(|(economic, total)| {
                let held = reserved.get(economic).copied().unwrap_or(U256::ZERO);
                total
                    .checked_sub(held)
                    .context("portfolio reservations exceed observed economic asset")
                    .map(|amount| (economic.clone(), amount))
            })
            .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
        Ok(PortfolioAudit {
            observed_by_economic_asset: observed,
            available_by_economic_asset: available,
            reserved_by_economic_asset: reserved,
            observed_location_assets: inventory.observed.len(),
        })
    }

    pub fn plan(
        &self,
        inventory: &InventoryReservations,
        in_flight: &[InFlightTransfer],
        intents: &[AllocationIntent],
    ) -> anyhow::Result<Vec<AllocationProposal>> {
        self.audit(inventory)?;
        for transfer in in_flight {
            self.validate_transfer(
                &transfer.economic_asset_id,
                &transfer.source,
                &transfer.destination,
                transfer.source_debit,
                transfer.destination_credit,
                transfer.fee,
            )?;
        }
        if self.mode == CompiledCapitalAllocatorMode::Disabled {
            return Ok(Vec::new());
        }
        let mut source_debits = BTreeMap::<InventoryKey, U256>::new();
        for transfer in in_flight {
            add_key_amount(&mut source_debits, &transfer.source, transfer.source_debit)?;
        }
        for (source, debit) in &source_debits {
            ensure!(
                *debit <= inventory.available(source)?,
                "in-flight transfers overspend source inventory"
            );
        }
        let mut proposals = Vec::with_capacity(intents.len());
        for intent in intents {
            let source_debit = intent
                .destination_credit
                .checked_add(intent.fee)
                .context("allocator proposal amount overflow")?;
            self.validate_transfer(
                &intent.economic_asset_id,
                &intent.source,
                &intent.destination,
                source_debit,
                intent.destination_credit,
                intent.fee,
            )?;
            add_key_amount(&mut source_debits, &intent.source, source_debit)?;
            ensure!(
                source_debits[&intent.source] <= inventory.available(&intent.source)?,
                "allocator proposals overspend source inventory"
            );
            let proposal = AllocationProposal {
                proposal_id: intent.proposal_id.clone(),
                economic_asset_id: intent.economic_asset_id.clone(),
                source: intent.source.clone(),
                destination: intent.destination.clone(),
                source_debit,
                destination_credit: intent.destination_credit,
                fee: intent.fee,
                external_mutation_authorized: false,
            };
            ensure!(
                proposal.conserves(),
                "allocator proposal does not conserve value"
            );
            proposals.push(proposal);
        }
        Ok(proposals)
    }

    fn validate_transfer(
        &self,
        economic_asset_id: &str,
        source: &InventoryKey,
        destination: &InventoryKey,
        source_debit: U256,
        destination_credit: U256,
        fee: U256,
    ) -> anyhow::Result<()> {
        ensure!(
            source != destination,
            "allocator source and destination are identical"
        );
        ensure!(
            self.mapping(source)? == economic_asset_id
                && self.mapping(destination)? == economic_asset_id,
            "allocator route crosses economic assets"
        );
        ensure!(
            destination_credit
                .checked_add(fee)
                .is_some_and(|accounted| accounted == source_debit),
            "allocator transfer is not conserved across credit and fee"
        );
        Ok(())
    }

    fn mapping(&self, key: &InventoryKey) -> anyhow::Result<&str> {
        self.economic_assets
            .get(key)
            .map(String::as_str)
            .with_context(|| format!("unreviewed portfolio venue asset {}", key.venue_asset_id))
    }
}

#[derive(Clone)]
pub struct CapitalAllocatorShadowHandle {
    sender: tokio::sync::watch::Sender<Option<QueuedPortfolioSnapshot>>,
}

impl CapitalAllocatorShadowHandle {
    pub fn submit(&self, inventory: &InventoryReservations) {
        let snapshot_started_at = Instant::now();
        let snapshot = inventory.portfolio_snapshot();
        let portfolio_snapshot_us = snapshot_started_at.elapsed().as_micros();
        self.sender.send_replace(Some(QueuedPortfolioSnapshot {
            snapshot,
            portfolio_snapshot_us,
            reservation_snapshot_us: 0,
            queued_at: Instant::now(),
        }));
    }

    pub fn submit_snapshot(&self, snapshot: InventoryPortfolioSnapshot) {
        self.sender.send_replace(Some(QueuedPortfolioSnapshot {
            snapshot,
            portfolio_snapshot_us: 0,
            reservation_snapshot_us: 0,
            queued_at: Instant::now(),
        }));
    }
}

struct QueuedPortfolioSnapshot {
    snapshot: InventoryPortfolioSnapshot,
    portfolio_snapshot_us: u128,
    reservation_snapshot_us: u128,
    queued_at: Instant,
}

pub struct CapitalAllocatorShadowTask {
    receiver: tokio::sync::watch::Receiver<Option<QueuedPortfolioSnapshot>>,
    allocator: CapitalAllocator,
    telemetry: TelemetryHandle,
    engine_id: String,
}

impl CapitalAllocatorShadowTask {
    pub async fn run(mut self) {
        while self.receiver.changed().await.is_ok() {
            let Some(queued) = self.receiver.borrow_and_update().clone() else {
                continue;
            };
            let scheduler_queue_us = queued.queued_at.elapsed().as_micros();
            let calculation_started_at = Instant::now();
            match self.allocator.audit_snapshot(&queued.snapshot) {
                Ok(audit) => self.telemetry.emit(
                    "portfolio_capital_allocator_evaluated",
                    serde_json::json!({
                        "engine_id": self.engine_id,
                        "allocator_mode": "shadow",
                        "external_mutation_authorized": false,
                        "scheduler_queue_us": scheduler_queue_us,
                        "portfolio_snapshot_us": queued.portfolio_snapshot_us,
                        "reservation_snapshot_us": queued.reservation_snapshot_us,
                        "allocator_calculation_validation_us":
                            calculation_started_at.elapsed().as_micros(),
                        "observed_location_assets": audit.observed_location_assets,
                        "economic_asset_count": audit.observed_by_economic_asset.len(),
                        "proposal_count": 0,
                        "conservation_checked": true,
                        "outcome": "success",
                    }),
                ),
                Err(error) => {
                    self.telemetry.emit(
                        "portfolio_capital_allocator_evaluated",
                        serde_json::json!({
                            "engine_id": self.engine_id,
                            "allocator_mode": "shadow",
                            "external_mutation_authorized": false,
                            "scheduler_queue_us": scheduler_queue_us,
                            "portfolio_snapshot_us": queued.portfolio_snapshot_us,
                            "reservation_snapshot_us": queued.reservation_snapshot_us,
                            "allocator_calculation_validation_us":
                                calculation_started_at.elapsed().as_micros(),
                            "proposal_count": 0,
                            "conservation_checked": false,
                            "outcome": "failed",
                            "error": format!("{error:#}"),
                        }),
                    );
                    tracing::warn!(error = %error, "shadow capital allocator audit failed closed");
                }
            }
        }
    }
}

impl Clone for QueuedPortfolioSnapshot {
    fn clone(&self) -> Self {
        Self {
            snapshot: self.snapshot.clone(),
            portfolio_snapshot_us: self.portfolio_snapshot_us,
            reservation_snapshot_us: self.reservation_snapshot_us,
            queued_at: self.queued_at,
        }
    }
}

pub fn capital_allocator_shadow_channel(
    catalog: &PortfolioCatalog,
    telemetry: TelemetryHandle,
    engine_id: String,
) -> (CapitalAllocatorShadowHandle, CapitalAllocatorShadowTask) {
    let (sender, receiver) = tokio::sync::watch::channel(None);
    (
        CapitalAllocatorShadowHandle { sender },
        CapitalAllocatorShadowTask {
            receiver,
            allocator: CapitalAllocator::new(catalog),
            telemetry,
            engine_id,
        },
    )
}

fn add_amount(totals: &mut BTreeMap<String, U256>, key: &str, amount: U256) -> anyhow::Result<()> {
    let total = totals.entry(key.to_owned()).or_insert(U256::ZERO);
    *total = total
        .checked_add(amount)
        .context("economic asset portfolio total overflow")?;
    Ok(())
}

fn add_key_amount(
    totals: &mut BTreeMap<InventoryKey, U256>,
    key: &InventoryKey,
    amount: U256,
) -> anyhow::Result<()> {
    let total = totals.entry(key.clone()).or_insert(U256::ZERO);
    *total = total
        .checked_add(amount)
        .context("allocator source debit overflow")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy_primitives::U256;
    use proptest::prelude::*;

    use crate::{
        domain::compiled::{
            BinanceAccountId, CompiledCapitalAllocatorMode, CompiledInventoryLocation,
            CompiledPortfolioAsset, CompiledPortfolioRuntimePlan, EconomicAssetId, NetworkId,
            VenueAssetId, WalletLocationId,
        },
        inventory::{InventoryKey, InventoryLocation, InventoryReservations},
    };

    use super::{
        AllocationIntent, CapitalAllocator, CapitalAllocatorShadowHandle, PortfolioCatalog,
    };

    fn plan(wallets: u8) -> CompiledPortfolioRuntimePlan {
        let mut assets = vec![CompiledPortfolioAsset {
            location: CompiledInventoryLocation::BinanceAccount {
                account_id: BinanceAccountId("binance-spot:primary".to_owned()),
            },
            venue_asset_id: VenueAssetId("binance-spot:primary:asset:USDC".to_owned()),
            economic_asset_id: EconomicAssetId("asset:USDC".to_owned()),
            symbol: "USDC".to_owned(),
            decimals: 6,
        }];
        for index in 0..wallets {
            let chain_id = 1_000 + u64::from(index);
            assets.push(CompiledPortfolioAsset {
                location: CompiledInventoryLocation::EvmWallet {
                    network_id: NetworkId(format!("eip155:{chain_id}")),
                    chain_id,
                    wallet_location_id: WalletLocationId(format!(
                        "eip155:{chain_id}:wallet:primary"
                    )),
                },
                venue_asset_id: VenueAssetId(format!("eip155:{chain_id}:erc20:0x{index:040x}")),
                economic_asset_id: EconomicAssetId("asset:USDC".to_owned()),
                symbol: "USDC".to_owned(),
                decimals: 6,
            });
        }
        CompiledPortfolioRuntimePlan {
            assets,
            allocator_mode: CompiledCapitalAllocatorMode::Shadow,
            live_rebalance_adapter: "world_chain_v12_parity".to_owned(),
        }
    }

    proptest! {
        #[test]
        fn binance_usdc_is_counted_once_for_any_number_of_wallet_targets(
            wallet_count in 1_u8..32,
            binance_balance in 1_u64..1_000_000,
        ) {
            let catalog = PortfolioCatalog::from_compiled(&plan(wallet_count)).unwrap();
            let allocator = CapitalAllocator::new(&catalog);
            let binance = InventoryLocation::binance("binance-spot:primary").unwrap();
            let binance_key = catalog.key(&binance, "USDC").unwrap();
            let mut inventory = InventoryReservations::default();
            inventory.update_location(
                binance,
                1,
                [(binance_key.venue_asset_id.clone(), U256::from(binance_balance))]
            ).unwrap();
            for index in 0..wallet_count {
                let chain_id = 1_000 + u64::from(index);
                let location = InventoryLocation::evm_wallet(
                    format!("eip155:{chain_id}"),
                    format!("eip155:{chain_id}:wallet:primary"),
                ).unwrap();
                let key = catalog.key(&location, "USDC").unwrap();
                inventory.update_location(
                    location,
                    1,
                    [(key.venue_asset_id, U256::ZERO)]
                ).unwrap();
            }
            let audit = allocator.audit(&inventory).unwrap();
            prop_assert_eq!(
                audit.observed_by_economic_asset["asset:USDC"],
                U256::from(binance_balance)
            );
        }
    }

    #[test]
    fn shadow_proposal_conserves_credit_fee_and_inflight() {
        let catalog = PortfolioCatalog::from_compiled(&plan(1)).unwrap();
        let allocator = CapitalAllocator::new(&catalog);
        let binance = InventoryLocation::binance("binance-spot:primary").unwrap();
        let wallet =
            InventoryLocation::evm_wallet("eip155:1000", "eip155:1000:wallet:primary").unwrap();
        let source = catalog.key(&binance, "USDC").unwrap();
        let destination = catalog.key(&wallet, "USDC").unwrap();
        let mut inventory = InventoryReservations::default();
        inventory
            .update_location(
                binance,
                1,
                [(source.venue_asset_id.clone(), U256::from(1_000))],
            )
            .unwrap();
        inventory
            .update_location(
                wallet,
                1,
                [(destination.venue_asset_id.clone(), U256::ZERO)],
            )
            .unwrap();
        let proposals = allocator
            .plan(
                &inventory,
                &[],
                &[AllocationIntent {
                    proposal_id: "move-usdc".to_owned(),
                    economic_asset_id: "asset:USDC".to_owned(),
                    source,
                    destination,
                    destination_credit: U256::from(890),
                    fee: U256::from(10),
                }],
            )
            .unwrap();
        assert_eq!(proposals.len(), 1);
        assert!(proposals[0].conserves());
        assert!(!proposals[0].external_mutation_authorized);
        assert_eq!(proposals[0].source_debit, U256::from(900));
    }

    #[test]
    fn allocator_rejects_economic_asset_crossing() {
        let catalog = PortfolioCatalog::from_compiled(&plan(1)).unwrap();
        let allocator = CapitalAllocator::new(&catalog);
        let source = InventoryKey::new(
            InventoryLocation::binance("binance-spot:primary").unwrap(),
            "binance-spot:primary:asset:USDC",
        )
        .unwrap();
        let destination = InventoryKey::new(
            InventoryLocation::evm_wallet("eip155:1000", "eip155:1000:wallet:primary").unwrap(),
            "eip155:1000:erc20:0x0000000000000000000000000000000000000000",
        )
        .unwrap();
        let mut inventory = InventoryReservations::default();
        inventory
            .update_location(
                source.location.clone(),
                1,
                [(source.venue_asset_id.clone(), U256::from(100))],
            )
            .unwrap();
        inventory
            .update_location(
                destination.location.clone(),
                1,
                [(destination.venue_asset_id.clone(), U256::ZERO)],
            )
            .unwrap();
        assert!(
            allocator
                .plan(
                    &inventory,
                    &[],
                    &[AllocationIntent {
                        proposal_id: "bad".to_owned(),
                        economic_asset_id: "asset:WLD".to_owned(),
                        source,
                        destination,
                        destination_credit: U256::from(10),
                        fee: U256::ZERO,
                    }],
                )
                .is_err()
        );
    }

    #[test]
    fn shadow_scheduler_is_bounded_latest_only_and_never_waits_for_worker() {
        let (sender, receiver) = tokio::sync::watch::channel(None);
        let handle = CapitalAllocatorShadowHandle { sender };
        let location = InventoryLocation::binance("binance-spot:primary").unwrap();
        let venue_asset_id = "binance-spot:primary:asset:USDC";
        let mut inventory = InventoryReservations::default();
        inventory
            .update_location(
                location.clone(),
                1,
                [(venue_asset_id.to_owned(), U256::from(1))],
            )
            .unwrap();
        handle.submit(&inventory);
        inventory
            .update_location(location, 2, [(venue_asset_id.to_owned(), U256::from(2))])
            .unwrap();
        handle.submit(&inventory);

        let latest = receiver.borrow().as_ref().unwrap().snapshot.clone();
        assert_eq!(
            latest.observed.values().copied().next(),
            Some(U256::from(2))
        );
    }
}

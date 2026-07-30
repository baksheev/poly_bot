use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

use alloy_primitives::U256;
use anyhow::{Context, ensure};

use crate::{
    domain::compiled::{
        CompiledCapitalAllocatorMode, CompiledCapitalCanaryPolicy, CompiledInventoryLocation,
        CompiledPortfolioRuntimePlan,
    },
    inventory::{
        InventoryKey, InventoryLocation, InventoryPortfolioSnapshot, InventoryReservations,
    },
    rebalance::{
        RebalanceCanaryRisk, RebalanceExecutionAuthority, RebalanceExecutionRequest, Route,
    },
    telemetry::TelemetryHandle,
};

#[derive(Clone, Debug)]
pub struct PortfolioCatalog {
    assets: BTreeMap<(InventoryLocation, String), InventoryKey>,
    economic_assets: BTreeMap<InventoryKey, String>,
    decimals: BTreeMap<InventoryKey, u8>,
    allocator_mode: CompiledCapitalAllocatorMode,
    capital_canary: Option<CompiledCapitalCanaryPolicy>,
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
        if let Some(policy) = &plan.capital_canary {
            ensure!(
                policy.token_a_economic_asset_id != policy.token_b_economic_asset_id
                    && economic_assets
                        .values()
                        .any(|asset| asset == policy.token_a_economic_asset_id.as_str())
                    && economic_assets
                        .values()
                        .any(|asset| asset == policy.token_b_economic_asset_id.as_str()),
                "M10 policy references an economic asset outside the portfolio"
            );
            ensure!(
                assets.keys().any(|(location, _)| matches!(
                    location,
                    InventoryLocation::EvmWallet { network_id, .. }
                        if network_id == policy.network_id.as_str()
                )),
                "M10 policy references a network outside the portfolio"
            );
            ensure!(
                plan.allocator_mode != CompiledCapitalAllocatorMode::LiveCanary
                    || policy.external_mutation_authorized,
                "live M10 allocator has no explicit mutation approval"
            );
        } else {
            ensure!(
                plan.allocator_mode != CompiledCapitalAllocatorMode::LiveCanary,
                "live M10 allocator has no versioned policy"
            );
        }
        Ok(Self {
            assets,
            economic_assets,
            decimals,
            allocator_mode: plan.allocator_mode,
            capital_canary: plan.capital_canary.clone(),
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

    pub fn capital_canary(&self) -> Option<&CompiledCapitalCanaryPolicy> {
        self.capital_canary.as_ref()
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
    capital_canary: Option<CompiledCapitalCanaryPolicy>,
}

impl CapitalAllocator {
    pub fn new(catalog: &PortfolioCatalog) -> Self {
        Self {
            mode: catalog.allocator_mode,
            economic_assets: catalog.economic_assets.clone(),
            capital_canary: catalog.capital_canary.clone(),
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
        self.plan_snapshot(&inventory.portfolio_snapshot(), in_flight, intents)
    }

    pub fn plan_snapshot(
        &self,
        inventory: &InventoryPortfolioSnapshot,
        in_flight: &[InFlightTransfer],
        intents: &[AllocationIntent],
    ) -> anyhow::Result<Vec<AllocationProposal>> {
        self.audit_snapshot(inventory)?;
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
        let capital_canary = self.capital_canary.as_ref();
        if self.mode == CompiledCapitalAllocatorMode::LiveCanary {
            let policy = capital_canary.context("live allocator has no M10 policy")?;
            ensure!(
                policy.external_mutation_authorized
                    && policy.maximum_concurrent_transfers == 1
                    && policy.direct_route_only
                    && !policy.bridge_mutations_enabled,
                "live allocator M10 policy is not mutation-authorized or direct-only"
            );
            ensure!(
                in_flight.len() < usize::from(policy.maximum_concurrent_transfers),
                "M10 permits only one external transfer at a time"
            );
            ensure!(
                intents.len() <= usize::from(policy.maximum_transfer_count),
                "M10 proposal count exceeds the transfer cap"
            );
        }
        let mut source_debits = BTreeMap::<InventoryKey, U256>::new();
        for transfer in in_flight {
            add_key_amount(&mut source_debits, &transfer.source, transfer.source_debit)?;
        }
        for (source, debit) in &source_debits {
            ensure!(
                *debit <= snapshot_available(inventory, source)?,
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
            if let Some(policy) = capital_canary {
                validate_canary_proposal(policy, intent, source_debit)?;
            }
            add_key_amount(&mut source_debits, &intent.source, source_debit)?;
            ensure!(
                source_debits[&intent.source] <= snapshot_available(inventory, &intent.source)?,
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
                external_mutation_authorized: self.mode == CompiledCapitalAllocatorMode::LiveCanary,
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

fn validate_canary_proposal(
    policy: &CompiledCapitalCanaryPolicy,
    intent: &AllocationIntent,
    source_debit: U256,
) -> anyhow::Result<()> {
    let economic_asset = intent.economic_asset_id.as_str();
    let (maximum_debit, maximum_fee) =
        if economic_asset == policy.token_a_economic_asset_id.as_str() {
            (policy.maximum_token_a_debit, policy.maximum_token_a_fee)
        } else if economic_asset == policy.token_b_economic_asset_id.as_str() {
            (policy.maximum_token_b_debit, policy.maximum_token_b_fee)
        } else {
            anyhow::bail!("M10 proposal uses an asset outside the approved canary");
        };
    ensure!(
        source_debit <= maximum_debit && intent.fee <= maximum_fee,
        "M10 proposal exceeds the asset value or fee cap"
    );
    let expected_network = policy.network_id.as_str();
    let route_is_direct = matches!(
        (&intent.source.location, &intent.destination.location),
        (
            InventoryLocation::BinanceAccount { .. },
            InventoryLocation::EvmWallet { network_id, .. },
        ) | (
            InventoryLocation::EvmWallet { network_id, .. },
            InventoryLocation::BinanceAccount { .. },
        ) if network_id == expected_network
    );
    ensure!(
        route_is_direct,
        "M10 proposal is not a direct Binance/Arbitrum route"
    );
    Ok(())
}

pub fn authorize_m10_rebalance_request(
    policy: &CompiledCapitalCanaryPolicy,
    risk: &RebalanceCanaryRisk,
    request: &RebalanceExecutionRequest,
    now_unix_ms: u64,
) -> anyhow::Result<()> {
    ensure!(
        policy.external_mutation_authorized
            && policy.maximum_concurrent_transfers == 1
            && policy.maximum_unknown_reconciliation_queries == 1
            && policy.direct_route_only
            && !policy.bridge_mutations_enabled,
        "M10 policy has no bounded direct mutation authority"
    );
    ensure!(
        request.authority == RebalanceExecutionAuthority::ArbitrumM10Canary,
        "M10 request has the wrong execution authority"
    );
    ensure!(
        matches!(
            &request.action.route,
            Route::Direct {
                chain_id: 42_161,
                binance_network,
            } if binance_network == &policy.binance_network
        ),
        "M10 request is not pinned to the approved direct Arbitrum route"
    );
    let maximum_fee = request
        .canary_maximum_fee
        .context("M10 request has no maximum fee authority")?;
    let remaining = remaining_m10_rebalance_authority(
        policy,
        risk,
        &request.token_symbol,
        request.action.direction,
        now_unix_ms,
    )?
    .context("M10 count, concurrency, failure, duration, value, or fee stop condition is closed")?;
    ensure!(
        request.action.amount <= remaining.maximum_source_debit
            && maximum_fee <= remaining.maximum_fee,
        "M10 cumulative value or fee cap would be exceeded"
    );
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct M10RemainingAuthority {
    pub maximum_source_debit: U256,
    pub maximum_fee: U256,
}

pub fn remaining_m10_rebalance_authority(
    policy: &CompiledCapitalCanaryPolicy,
    risk: &RebalanceCanaryRisk,
    token_symbol: &str,
    direction: crate::rebalance::Direction,
    now_unix_ms: u64,
) -> anyhow::Result<Option<M10RemainingAuthority>> {
    ensure!(
        policy.external_mutation_authorized
            && policy.maximum_concurrent_transfers == 1
            && policy.maximum_unknown_reconciliation_queries == 1
            && policy.direct_route_only
            && !policy.bridge_mutations_enabled,
        "M10 policy has no bounded direct mutation authority"
    );
    if risk.transfer_count >= policy.maximum_transfer_count
        || risk.active_transfer_count >= usize::from(policy.maximum_concurrent_transfers)
        || risk.failed_transfer_count >= usize::from(policy.maximum_failed_transfers)
    {
        return Ok(None);
    }
    if let Some(started_at) = risk.first_started_at_unix_ms {
        let elapsed = now_unix_ms
            .checked_sub(started_at)
            .context("M10 system time moved before the durable canary start")?;
        if elapsed > policy.rollout_duration_seconds.saturating_mul(1_000) {
            return Ok(None);
        }
    }
    let (used_debit, debit_cap, used_fee, fee_cap) = if token_symbol == policy.token_a_symbol {
        (
            risk.token_a_debit,
            policy.maximum_token_a_debit,
            risk.token_a_maximum_fee,
            policy.maximum_token_a_fee,
        )
    } else if token_symbol == policy.token_b_symbol {
        (
            risk.token_b_debit,
            policy.maximum_token_b_debit,
            risk.token_b_maximum_fee,
            policy.maximum_token_b_fee,
        )
    } else {
        anyhow::bail!("M10 request uses an asset outside the approved canary");
    };
    let remaining_debit = debit_cap
        .checked_sub(used_debit)
        .context("M10 durable debit exceeds its policy cap")?;
    let remaining_fee = fee_cap
        .checked_sub(used_fee)
        .context("M10 durable fee authority exceeds its policy cap")?;
    let maximum_fee = if direction == crate::rebalance::Direction::WalletToBinance {
        U256::ZERO
    } else {
        remaining_fee
    };
    if remaining_debit.is_zero()
        || (direction == crate::rebalance::Direction::BinanceToWallet && maximum_fee.is_zero())
    {
        return Ok(None);
    }
    Ok(Some(M10RemainingAuthority {
        maximum_source_debit: remaining_debit,
        maximum_fee,
    }))
}

#[derive(Clone)]
pub struct CapitalAllocatorHandle {
    sender: tokio::sync::watch::Sender<Option<QueuedPortfolioSnapshot>>,
    planner: tokio::sync::mpsc::Sender<CapitalPlanRequest>,
}

impl CapitalAllocatorHandle {
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

    pub async fn plan(
        &self,
        snapshot: InventoryPortfolioSnapshot,
        in_flight: Vec<InFlightTransfer>,
        intents: Vec<AllocationIntent>,
    ) -> anyhow::Result<Vec<AllocationProposal>> {
        let (response, receiver) = tokio::sync::oneshot::channel();
        self.planner
            .send(CapitalPlanRequest {
                snapshot,
                in_flight,
                intents,
                queued_at: Instant::now(),
                response,
            })
            .await
            .context("capital allocator owner stopped")?;
        receiver
            .await
            .context("capital allocator owner dropped its response")?
    }
}

struct QueuedPortfolioSnapshot {
    snapshot: InventoryPortfolioSnapshot,
    portfolio_snapshot_us: u128,
    reservation_snapshot_us: u128,
    queued_at: Instant,
}

pub struct CapitalAllocatorTask {
    receiver: tokio::sync::watch::Receiver<Option<QueuedPortfolioSnapshot>>,
    planner: tokio::sync::mpsc::Receiver<CapitalPlanRequest>,
    allocator: CapitalAllocator,
    telemetry: TelemetryHandle,
    engine_id: String,
}

impl CapitalAllocatorTask {
    pub async fn run(mut self) {
        let allocator_mode = match self.allocator.mode {
            CompiledCapitalAllocatorMode::Disabled => "disabled",
            CompiledCapitalAllocatorMode::Shadow => "shadow",
            CompiledCapitalAllocatorMode::LiveCanary => "live_canary",
        };
        let external_mutation_authorized =
            self.allocator.mode == CompiledCapitalAllocatorMode::LiveCanary;
        let mut audit_open = true;
        let mut planner_open = true;
        while audit_open || planner_open {
            tokio::select! {
                biased;
                request = self.planner.recv(), if planner_open => {
                    let Some(request) = request else {
                        planner_open = false;
                        continue;
                    };
                    let scheduler_queue_us = request.queued_at.elapsed().as_micros();
                    let calculation_started_at = Instant::now();
                    let result = self.allocator.plan_snapshot(
                        &request.snapshot,
                        &request.in_flight,
                        &request.intents,
                    );
                    self.telemetry.emit(
                        "portfolio_capital_allocator_planned",
                        serde_json::json!({
                            "engine_id": self.engine_id,
                            "allocator_mode": allocator_mode,
                            "external_mutation_authorized": external_mutation_authorized,
                            "scheduler_queue_us": scheduler_queue_us,
                            "allocator_calculation_validation_us":
                                calculation_started_at.elapsed().as_micros(),
                            "proposal_count": result.as_ref().map_or(0, Vec::len),
                            "conservation_checked": result.is_ok(),
                            "outcome": if result.is_ok() { "success" } else { "failed" },
                        }),
                    );
                    let _ = request.response.send(result);
                }
                changed = self.receiver.changed(), if audit_open => {
                    if changed.is_err() {
                        audit_open = false;
                        continue;
                    }
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
                        "allocator_mode": allocator_mode,
                        "external_mutation_authorized": external_mutation_authorized,
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
                            "allocator_mode": allocator_mode,
                            "external_mutation_authorized": external_mutation_authorized,
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
                    tracing::warn!(error = %error, "capital allocator audit failed closed");
                }
                    }
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

pub fn capital_allocator_channel(
    catalog: &PortfolioCatalog,
    telemetry: TelemetryHandle,
    engine_id: String,
) -> (CapitalAllocatorHandle, CapitalAllocatorTask) {
    let (sender, receiver) = tokio::sync::watch::channel(None);
    let (planner, planner_receiver) = tokio::sync::mpsc::channel(1);
    (
        CapitalAllocatorHandle { sender, planner },
        CapitalAllocatorTask {
            receiver,
            planner: planner_receiver,
            allocator: CapitalAllocator::new(catalog),
            telemetry,
            engine_id,
        },
    )
}

struct CapitalPlanRequest {
    snapshot: InventoryPortfolioSnapshot,
    in_flight: Vec<InFlightTransfer>,
    intents: Vec<AllocationIntent>,
    queued_at: Instant,
    response: tokio::sync::oneshot::Sender<anyhow::Result<Vec<AllocationProposal>>>,
}

fn snapshot_available(
    snapshot: &InventoryPortfolioSnapshot,
    key: &InventoryKey,
) -> anyhow::Result<U256> {
    let observed = snapshot
        .observed
        .get(key)
        .copied()
        .with_context(|| format!("allocator has no observed source {}", key.venue_asset_id))?;
    observed
        .checked_sub(
            snapshot
                .reserved_totals
                .get(key)
                .copied()
                .unwrap_or(U256::ZERO),
        )
        .context("allocator reservations exceed observed source inventory")
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
    use alloy_primitives::{Address, U256};
    use proptest::prelude::*;

    use crate::{
        domain::compiled::{
            BinanceAccountId, CompiledCapitalAllocatorMode, CompiledCapitalCanaryPolicy,
            CompiledInventoryLocation, CompiledPortfolioAsset, CompiledPortfolioRuntimePlan,
            EconomicAssetId, NetworkId, VenueAssetId, WalletLocationId,
        },
        inventory::{InventoryKey, InventoryLocation, InventoryReservations},
        rebalance::{
            Direction, RebalanceAction, RebalanceCanaryRisk, RebalanceExecutionAuthority,
            RebalanceExecutionRequest, Route,
        },
    };

    use super::{
        AllocationIntent, CapitalAllocator, CapitalAllocatorHandle, InFlightTransfer,
        PortfolioCatalog, authorize_m10_rebalance_request, remaining_m10_rebalance_authority,
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
            capital_canary: None,
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
    fn live_canary_authorizes_only_one_bounded_direct_transfer() {
        let mut runtime_plan = plan(0);
        runtime_plan.assets.extend([
            CompiledPortfolioAsset {
                location: CompiledInventoryLocation::EvmWallet {
                    network_id: NetworkId("eip155:42161".to_owned()),
                    chain_id: 42_161,
                    wallet_location_id: WalletLocationId("eip155:42161:wallet:primary".to_owned()),
                },
                venue_asset_id: VenueAssetId("eip155:42161:erc20:USDC".to_owned()),
                economic_asset_id: EconomicAssetId("asset:USDC".to_owned()),
                symbol: "USDC".to_owned(),
                decimals: 6,
            },
            CompiledPortfolioAsset {
                location: CompiledInventoryLocation::BinanceAccount {
                    account_id: BinanceAccountId("binance-spot:primary".to_owned()),
                },
                venue_asset_id: VenueAssetId("binance-spot:primary:asset:ESP".to_owned()),
                economic_asset_id: EconomicAssetId("asset:ESP".to_owned()),
                symbol: "ESP".to_owned(),
                decimals: 18,
            },
            CompiledPortfolioAsset {
                location: CompiledInventoryLocation::EvmWallet {
                    network_id: NetworkId("eip155:42161".to_owned()),
                    chain_id: 42_161,
                    wallet_location_id: WalletLocationId("eip155:42161:wallet:primary".to_owned()),
                },
                venue_asset_id: VenueAssetId("eip155:42161:erc20:ESP".to_owned()),
                economic_asset_id: EconomicAssetId("asset:ESP".to_owned()),
                symbol: "ESP".to_owned(),
                decimals: 18,
            },
        ]);
        runtime_plan.allocator_mode = CompiledCapitalAllocatorMode::LiveCanary;
        runtime_plan.capital_canary = Some(CompiledCapitalCanaryPolicy {
            network_id: NetworkId("eip155:42161".to_owned()),
            binance_network: "ARBITRUM".to_owned(),
            token_a_symbol: "USDC".to_owned(),
            token_b_symbol: "ESP".to_owned(),
            token_a_economic_asset_id: EconomicAssetId("asset:USDC".to_owned()),
            token_b_economic_asset_id: EconomicAssetId("asset:ESP".to_owned()),
            maximum_transfer_count: 2,
            maximum_concurrent_transfers: 1,
            maximum_failed_transfers: 1,
            maximum_token_a_debit: U256::from(1_000_u64),
            maximum_token_b_debit: U256::from(2_000_u64),
            maximum_token_a_fee: U256::from(100_u64),
            maximum_token_b_fee: U256::from(200_u64),
            rollout_duration_seconds: 900,
            maximum_unknown_reconciliation_queries: 1,
            direct_route_only: true,
            bridge_mutations_enabled: false,
            external_mutation_authorized: true,
        });
        let catalog = PortfolioCatalog::from_compiled(&runtime_plan).unwrap();
        let allocator = CapitalAllocator::new(&catalog);
        let binance = InventoryLocation::binance("binance-spot:primary").unwrap();
        let wallet =
            InventoryLocation::evm_wallet("eip155:42161", "eip155:42161:wallet:primary").unwrap();
        let source = catalog.key(&binance, "USDC").unwrap();
        let destination = catalog.key(&wallet, "USDC").unwrap();
        let mut inventory = InventoryReservations::default();
        inventory
            .update_location(
                binance,
                1,
                [(source.venue_asset_id.clone(), U256::from(1_000_u64))],
            )
            .unwrap();
        inventory
            .update_location(
                wallet,
                1,
                [(destination.venue_asset_id.clone(), U256::ZERO)],
            )
            .unwrap();
        let intent = AllocationIntent {
            proposal_id: "m10-usdc-direct".to_owned(),
            economic_asset_id: "asset:USDC".to_owned(),
            source: source.clone(),
            destination: destination.clone(),
            destination_credit: U256::from(900_u64),
            fee: U256::from(100_u64),
        };
        let proposals = allocator
            .plan(&inventory, &[], std::slice::from_ref(&intent))
            .unwrap();
        assert_eq!(proposals.len(), 1);
        assert!(proposals[0].external_mutation_authorized);

        let in_flight = InFlightTransfer {
            economic_asset_id: "asset:USDC".to_owned(),
            source,
            destination,
            source_debit: U256::from(1_000_u64),
            destination_credit: U256::from(900_u64),
            fee: U256::from(100_u64),
        };
        assert!(allocator.plan(&inventory, &[in_flight], &[intent]).is_err());

        let request = RebalanceExecutionRequest {
            authority: RebalanceExecutionAuthority::ArbitrumM10Canary,
            token_symbol: "USDC".to_owned(),
            token_decimals: 6,
            token_contract: Address::repeat_byte(0x11),
            wallet_owner: Address::repeat_byte(0x22),
            action: RebalanceAction {
                direction: Direction::BinanceToWallet,
                amount: U256::from(900_u64),
                route: Route::Direct {
                    binance_network: "ARBITRUM".to_owned(),
                    chain_id: 42_161,
                },
            },
            binance_balance_before: U256::from(1_000_u64),
            wallet_balance_before: U256::ZERO,
            canary_maximum_fee: Some(U256::from(100_u64)),
        };
        authorize_m10_rebalance_request(
            catalog.capital_canary().unwrap(),
            &RebalanceCanaryRisk::default(),
            &request,
            1_000,
        )
        .unwrap();

        let exhausted = RebalanceCanaryRisk {
            transfer_count: 2,
            ..RebalanceCanaryRisk::default()
        };
        assert!(
            authorize_m10_rebalance_request(
                catalog.capital_canary().unwrap(),
                &exhausted,
                &request,
                1_000,
            )
            .is_err()
        );

        let partially_used = RebalanceCanaryRisk {
            transfer_count: 1,
            token_a_debit: U256::from(750_u64),
            token_a_maximum_fee: U256::from(25_u64),
            first_started_at_unix_ms: Some(1_000),
            ..RebalanceCanaryRisk::default()
        };
        let remaining = remaining_m10_rebalance_authority(
            catalog.capital_canary().unwrap(),
            &partially_used,
            "USDC",
            Direction::BinanceToWallet,
            2_000,
        )
        .unwrap()
        .unwrap();
        assert_eq!(remaining.maximum_source_debit, U256::from(250_u64));
        assert_eq!(remaining.maximum_fee, U256::from(75_u64));
        assert!(
            remaining_m10_rebalance_authority(
                catalog.capital_canary().unwrap(),
                &partially_used,
                "USDC",
                Direction::BinanceToWallet,
                901_001,
            )
            .unwrap()
            .is_none()
        );
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
        let (planner, _planner_receiver) = tokio::sync::mpsc::channel(1);
        let handle = CapitalAllocatorHandle { sender, planner };
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

    #[tokio::test]
    async fn process_scoped_allocator_owner_validates_plans_from_a_shared_handle() {
        let catalog = PortfolioCatalog::from_compiled(&plan(1)).unwrap();
        let (handle, task) = super::capital_allocator_channel(
            &catalog,
            crate::telemetry::TelemetryHandle::disconnected_test_handle(),
            "test-engine".to_owned(),
        );
        let task = tokio::spawn(task.run());
        let source_location = InventoryLocation::binance("binance-spot:primary").unwrap();
        let destination_location =
            InventoryLocation::evm_wallet("eip155:1000", "eip155:1000:wallet:primary").unwrap();
        let source = catalog.key(&source_location, "USDC").unwrap();
        let destination = catalog.key(&destination_location, "USDC").unwrap();
        let mut inventory = InventoryReservations::default();
        inventory
            .update_location(
                source_location,
                1,
                [(source.venue_asset_id.clone(), U256::from(1_000_u64))],
            )
            .unwrap();
        inventory
            .update_location(
                destination_location,
                1,
                [(destination.venue_asset_id.clone(), U256::ZERO)],
            )
            .unwrap();

        let proposals = handle
            .plan(
                inventory.portfolio_snapshot(),
                Vec::new(),
                vec![AllocationIntent {
                    proposal_id: "shared-owner-plan".to_owned(),
                    economic_asset_id: "asset:USDC".to_owned(),
                    source,
                    destination,
                    destination_credit: U256::from(990_u64),
                    fee: U256::from(10_u64),
                }],
            )
            .await
            .unwrap();
        assert_eq!(proposals.len(), 1);
        assert!(proposals[0].conserves());
        assert!(!proposals[0].external_mutation_authorized);
        drop(handle);
        task.await.unwrap();
    }
}

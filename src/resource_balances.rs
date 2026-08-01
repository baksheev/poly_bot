use std::{
    collections::{BTreeMap, VecDeque},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use alloy_primitives::{Address, U256};
use anyhow::{Context, ensure};
use futures_util::future::join_all;
use rust_decimal::Decimal;
use tokio::time::MissedTickBehavior;

use crate::{
    binance::account::BinanceAccountClient,
    chain::rpc::JsonRpcClient,
    opportunity::format_base_units,
    telemetry::{PRIMARY_BINANCE_ACCOUNT_ID, PRIMARY_EVM_WALLET_ID, TelemetryHandle},
};

pub const RESOURCE_BALANCE_TELEMETRY_KIND: &str = "resource_balance_snapshot";
pub const RESOURCE_BALANCE_INTERVAL: Duration = Duration::from_secs(60);
const CONSUMPTION_WINDOW_MS: u64 = 24 * 60 * 60 * 1_000;
const EVM_NATIVE_DECIMALS: u8 = 18;
const BINANCE_BNB_DECIMALS: u8 = 8;

pub struct EvmGasBalanceSource {
    network_id: String,
    chain_id: u64,
    wallet_location_id: String,
    usage: &'static str,
    rpc: JsonRpcClient,
    chain_validated: bool,
}

impl EvmGasBalanceSource {
    pub fn trading(
        network_id: String,
        chain_id: u64,
        wallet_location_id: String,
        rpc: JsonRpcClient,
    ) -> Self {
        Self {
            network_id,
            chain_id,
            wallet_location_id,
            usage: "trading",
            rpc,
            chain_validated: true,
        }
    }

    pub fn bridge(
        network_id: String,
        chain_id: u64,
        wallet_location_id: String,
        rpc: JsonRpcClient,
    ) -> Self {
        Self {
            network_id,
            chain_id,
            wallet_location_id,
            usage: "bridge",
            rpc,
            chain_validated: false,
        }
    }

    fn resource_id(&self) -> String {
        format!("{}:native", self.wallet_location_id)
    }
}

pub struct ResourceBalanceMonitor {
    telemetry: TelemetryHandle,
    engine_id: String,
    wallet_owner: Address,
    evm_sources: Vec<EvmGasBalanceSource>,
    binance: BinanceAccountClient,
    tracker: ConsumptionTracker,
}

impl ResourceBalanceMonitor {
    pub fn new(
        telemetry: TelemetryHandle,
        engine_id: String,
        wallet_owner: Address,
        evm_sources: Vec<EvmGasBalanceSource>,
        binance: BinanceAccountClient,
    ) -> anyhow::Result<Self> {
        ensure!(!evm_sources.is_empty(), "gas balance sources are empty");
        let mut resource_ids = evm_sources
            .iter()
            .map(EvmGasBalanceSource::resource_id)
            .collect::<Vec<_>>();
        resource_ids.sort();
        resource_ids.dedup();
        ensure!(
            resource_ids.len() == evm_sources.len(),
            "gas balance resource ids must be unique"
        );
        Ok(Self {
            telemetry,
            engine_id,
            wallet_owner,
            evm_sources,
            binance,
            tracker: ConsumptionTracker::default(),
        })
    }

    pub async fn run(mut self) {
        let mut interval = tokio::time::interval(RESOURCE_BALANCE_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            self.poll_once().await;
        }
    }

    async fn poll_once(&mut self) {
        let wallet_owner = self.wallet_owner;
        let (evm_results, binance_result) = tokio::join!(
            observe_evm_sources(&mut self.evm_sources, wallet_owner),
            observe_binance_bnb(&mut self.binance),
        );
        for result in evm_results {
            match result {
                Ok(observation) => self.emit_observation(observation),
                Err(failure) => self.emit_failure(failure),
            }
        }
        match binance_result {
            Ok(observation) => self.emit_observation(observation),
            Err(failure) => self.emit_failure(failure),
        }
    }

    fn emit_observation(&mut self, observation: ResourceObservation) {
        let observed_at_ms = unix_timestamp_ms();
        let consumption = match self.tracker.observe(
            &observation.resource_id,
            observed_at_ms,
            observation.balance_base_units,
        ) {
            Ok(consumption) => consumption,
            Err(error) => {
                self.emit_failure(ResourceFailure {
                    metadata: observation.metadata,
                    request_duration_us: observation.request_duration_us,
                    error: format!("consumption calculation failed: {error:#}"),
                });
                return;
            }
        };
        let decimals = observation.metadata.decimals;
        self.telemetry.emit(
            RESOURCE_BALANCE_TELEMETRY_KIND,
            serde_json::json!({
                "engine_id": self.engine_id,
                "resource_id": observation.resource_id,
                "resource_kind": observation.metadata.resource_kind,
                "usage": observation.metadata.usage,
                "account_id": observation.metadata.account_id,
                "wallet_id": observation.metadata.wallet_id,
                "wallet_location_id": observation.metadata.wallet_location_id,
                "network_id": observation.metadata.network_id,
                "chain_id": observation.metadata.chain_id,
                "owner": observation.metadata.owner,
                "asset": observation.metadata.asset,
                "decimals": decimals,
                "balance_base_units": observation.balance_base_units.to_string(),
                "balance": format_base_units(observation.balance_base_units, decimals),
                "free_balance": observation.free_balance,
                "locked_balance": observation.locked_balance,
                "consumption_24h_base_units": consumption.consumption_24h.to_string(),
                "consumption_24h": format_base_units(consumption.consumption_24h, decimals),
                "average_daily_consumption_base_units": consumption.average_daily.to_string(),
                "average_daily_consumption": format_base_units(consumption.average_daily, decimals),
                "consumption_window_ms": consumption.window_ms,
                "consumption_window_complete": consumption.window_complete,
                "consumption_model": "sum_of_balance_decreases_excluding_refills",
                "request_duration_us": observation.request_duration_us.min(u128::from(u64::MAX)) as u64,
                "outcome": "success",
            }),
        );
    }

    fn emit_failure(&self, failure: ResourceFailure) {
        tracing::warn!(
            resource_id = %failure.metadata.resource_id,
            error = %failure.error,
            "background resource balance observation failed"
        );
        self.telemetry.emit(
            RESOURCE_BALANCE_TELEMETRY_KIND,
            serde_json::json!({
                "engine_id": self.engine_id,
                "resource_id": failure.metadata.resource_id,
                "resource_kind": failure.metadata.resource_kind,
                "usage": failure.metadata.usage,
                "account_id": failure.metadata.account_id,
                "wallet_id": failure.metadata.wallet_id,
                "wallet_location_id": failure.metadata.wallet_location_id,
                "network_id": failure.metadata.network_id,
                "chain_id": failure.metadata.chain_id,
                "owner": failure.metadata.owner,
                "asset": failure.metadata.asset,
                "decimals": failure.metadata.decimals,
                "request_duration_us": failure.request_duration_us.min(u128::from(u64::MAX)) as u64,
                "outcome": "failed",
                "error": failure.error,
            }),
        );
    }
}

async fn observe_evm_sources(
    sources: &mut [EvmGasBalanceSource],
    owner: Address,
) -> Vec<Result<ResourceObservation, ResourceFailure>> {
    join_all(
        sources
            .iter_mut()
            .map(|source| observe_evm_balance(source, owner)),
    )
    .await
}

async fn observe_evm_balance(
    source: &mut EvmGasBalanceSource,
    owner: Address,
) -> Result<ResourceObservation, ResourceFailure> {
    let metadata = ResourceMetadata {
        resource_id: source.resource_id(),
        resource_kind: "evm_native_gas",
        usage: source.usage,
        account_id: None,
        wallet_id: Some(PRIMARY_EVM_WALLET_ID.to_owned()),
        wallet_location_id: Some(source.wallet_location_id.clone()),
        network_id: Some(source.network_id.clone()),
        chain_id: Some(source.chain_id),
        owner: Some(format!("{owner:#x}")),
        asset: "ETH",
        decimals: EVM_NATIVE_DECIMALS,
    };
    let started_at = Instant::now();
    let result = async {
        if !source.chain_validated {
            let observed_chain_id = source.rpc.chain_id().await?;
            ensure!(
                observed_chain_id == source.chain_id,
                "RPC returned chain {observed_chain_id}, expected {}",
                source.chain_id
            );
            source.chain_validated = true;
        }
        source.rpc.native_balance(owner).await
    }
    .await;
    match result {
        Ok(balance_base_units) => Ok(ResourceObservation {
            resource_id: metadata.resource_id.clone(),
            metadata,
            balance_base_units,
            free_balance: None,
            locked_balance: None,
            request_duration_us: started_at.elapsed().as_micros(),
        }),
        Err(error) => Err(ResourceFailure {
            metadata,
            request_duration_us: started_at.elapsed().as_micros(),
            error: format!("{error:#}"),
        }),
    }
}

async fn observe_binance_bnb(
    client: &mut BinanceAccountClient,
) -> Result<ResourceObservation, ResourceFailure> {
    let metadata = ResourceMetadata {
        resource_id: format!("{PRIMARY_BINANCE_ACCOUNT_ID}:asset:BNB"),
        resource_kind: "binance_commission_balance",
        usage: "trading",
        account_id: Some(PRIMARY_BINANCE_ACCOUNT_ID.to_owned()),
        wallet_id: None,
        wallet_location_id: None,
        network_id: None,
        chain_id: None,
        owner: None,
        asset: "BNB",
        decimals: BINANCE_BNB_DECIMALS,
    };
    let started_at = Instant::now();
    let account = match client.account_information().await {
        Ok(account) => Ok(account),
        Err(first_error) => match client.synchronize_clock().await {
            Ok(()) => client
                .account_information()
                .await
                .with_context(|| format!("Binance BNB balance retry failed after {first_error:#}")),
            Err(clock_error) => Err(anyhow::anyhow!(
                "Binance BNB balance failed: {first_error:#}; clock synchronization failed: {clock_error:#}"
            )),
        },
    };
    let account = match account {
        Ok(account) => account,
        Err(error) => {
            return Err(ResourceFailure {
                metadata,
                request_duration_us: started_at.elapsed().as_micros(),
                error: format!("{error:#}"),
            });
        }
    };
    let balance = account
        .balances
        .iter()
        .find(|balance| balance.asset == "BNB");
    let free = balance.map_or(Decimal::ZERO, |balance| balance.free);
    let locked = balance.map_or(Decimal::ZERO, |balance| balance.locked);
    let total = match free.checked_add(locked) {
        Some(total) => total,
        None => {
            return Err(ResourceFailure {
                metadata,
                request_duration_us: started_at.elapsed().as_micros(),
                error: "BNB total balance overflow".to_owned(),
            });
        }
    };
    let balance_base_units = match decimal_to_base_units(total, BINANCE_BNB_DECIMALS) {
        Ok(value) => value,
        Err(error) => {
            return Err(ResourceFailure {
                metadata,
                request_duration_us: started_at.elapsed().as_micros(),
                error: format!("invalid BNB balance: {error:#}"),
            });
        }
    };
    Ok(ResourceObservation {
        resource_id: metadata.resource_id.clone(),
        metadata,
        balance_base_units,
        free_balance: Some(free.normalize().to_string()),
        locked_balance: Some(locked.normalize().to_string()),
        request_duration_us: started_at.elapsed().as_micros(),
    })
}

#[derive(Clone)]
struct ResourceMetadata {
    resource_id: String,
    resource_kind: &'static str,
    usage: &'static str,
    account_id: Option<String>,
    wallet_id: Option<String>,
    wallet_location_id: Option<String>,
    network_id: Option<String>,
    chain_id: Option<u64>,
    owner: Option<String>,
    asset: &'static str,
    decimals: u8,
}

struct ResourceObservation {
    resource_id: String,
    metadata: ResourceMetadata,
    balance_base_units: U256,
    free_balance: Option<String>,
    locked_balance: Option<String>,
    request_duration_us: u128,
}

struct ResourceFailure {
    metadata: ResourceMetadata,
    request_duration_us: u128,
    error: String,
}

#[derive(Default)]
struct ConsumptionTracker {
    histories: BTreeMap<String, ResourceHistory>,
}

impl ConsumptionTracker {
    fn observe(
        &mut self,
        resource_id: &str,
        observed_at_ms: u64,
        balance: U256,
    ) -> anyhow::Result<ConsumptionSnapshot> {
        let history = self
            .histories
            .entry(resource_id.to_owned())
            .or_insert_with(|| ResourceHistory::new(observed_at_ms, balance));
        if history.previous_balance > balance {
            history
                .decreases
                .push_back((observed_at_ms, history.previous_balance - balance));
        }
        history.previous_balance = balance;
        let cutoff = observed_at_ms.saturating_sub(CONSUMPTION_WINDOW_MS);
        while history
            .decreases
            .front()
            .is_some_and(|(at, _)| *at <= cutoff)
        {
            history.decreases.pop_front();
        }
        let consumption_24h = history
            .decreases
            .iter()
            .try_fold(U256::ZERO, |total, (_, decrease)| {
                total.checked_add(*decrease)
            })
            .context("24-hour consumption overflow")?;
        let window_ms = observed_at_ms
            .saturating_sub(history.started_at_ms)
            .min(CONSUMPTION_WINDOW_MS);
        let average_daily = if window_ms == 0 {
            U256::ZERO
        } else {
            consumption_24h
                .checked_mul(U256::from(CONSUMPTION_WINDOW_MS))
                .context("daily consumption normalization overflow")?
                / U256::from(window_ms)
        };
        Ok(ConsumptionSnapshot {
            consumption_24h,
            average_daily,
            window_ms,
            window_complete: observed_at_ms.saturating_sub(history.started_at_ms)
                >= CONSUMPTION_WINDOW_MS,
        })
    }
}

struct ResourceHistory {
    started_at_ms: u64,
    previous_balance: U256,
    decreases: VecDeque<(u64, U256)>,
}

impl ResourceHistory {
    fn new(started_at_ms: u64, balance: U256) -> Self {
        Self {
            started_at_ms,
            previous_balance: balance,
            decreases: VecDeque::new(),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ConsumptionSnapshot {
    consumption_24h: U256,
    average_daily: U256,
    window_ms: u64,
    window_complete: bool,
}

fn decimal_to_base_units(value: Decimal, decimals: u8) -> anyhow::Result<U256> {
    ensure!(value >= Decimal::ZERO, "balance is negative");
    let mantissa = u128::try_from(value.mantissa()).context("balance mantissa is negative")?;
    let numerator = U256::from(mantissa)
        .checked_mul(pow10(u32::from(decimals))?)
        .context("balance base-unit numerator overflow")?;
    let denominator = pow10(value.scale())?;
    ensure!(
        numerator % denominator == U256::ZERO,
        "balance exceeds {decimals}-decimal precision"
    );
    Ok(numerator / denominator)
}

fn pow10(exponent: u32) -> anyhow::Result<U256> {
    (0..exponent).try_fold(U256::ONE, |value, _| {
        value
            .checked_mul(U256::from(10))
            .context("decimal scale overflow")
    })
}

fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{CONSUMPTION_WINDOW_MS, ConsumptionTracker, decimal_to_base_units};
    use alloy_primitives::U256;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    #[test]
    fn consumption_sums_decreases_and_ignores_refills() {
        let mut tracker = ConsumptionTracker::default();
        let id = "wallet:gas";
        assert_eq!(
            tracker
                .observe(id, 0, U256::from(1_000))
                .unwrap()
                .consumption_24h,
            U256::ZERO
        );
        let first = tracker.observe(id, 3_600_000, U256::from(900)).unwrap();
        assert_eq!(first.consumption_24h, U256::from(100));
        assert_eq!(first.average_daily, U256::from(2_400));

        tracker.observe(id, 7_200_000, U256::from(1_200)).unwrap();
        let second = tracker.observe(id, 10_800_000, U256::from(1_150)).unwrap();
        assert_eq!(second.consumption_24h, U256::from(150));
        assert_eq!(second.average_daily, U256::from(1_200));
    }

    #[test]
    fn consumption_evicts_decreases_outside_the_rolling_day() {
        let mut tracker = ConsumptionTracker::default();
        let id = "wallet:gas";
        tracker.observe(id, 0, U256::from(1_000)).unwrap();
        tracker.observe(id, 1_000, U256::from(900)).unwrap();
        let snapshot = tracker
            .observe(id, CONSUMPTION_WINDOW_MS + 1_000, U256::from(900))
            .unwrap();
        assert_eq!(snapshot.consumption_24h, U256::ZERO);
        assert_eq!(snapshot.average_daily, U256::ZERO);
        assert!(snapshot.window_complete);
    }

    #[test]
    fn decimal_balance_conversion_is_exact() {
        assert_eq!(
            decimal_to_base_units(Decimal::from_str("1.23456789").unwrap(), 8).unwrap(),
            U256::from(123_456_789_u64)
        );
        assert!(decimal_to_base_units(Decimal::from_str("0.000000001").unwrap(), 8).is_err());
    }
}

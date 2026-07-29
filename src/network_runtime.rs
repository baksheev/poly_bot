use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, ensure};
use futures_util::future::try_join_all;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    chain::rpc::{CanonicalBlock, EthCall, JsonRpcClient},
    domain::compiled::{
        CompiledNetworkGasPolicy, CompiledNetworkPlan, CompiledNetworkRuntimePlan, ExecutionLaneId,
        NetworkId,
    },
    telemetry::TelemetryHandle,
};

const CONNECTION_GENERATION: u64 = 1;
const REVIEWED_BATCH_CHUNK_LIMIT: usize = 100;

#[derive(Debug, Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub enum NetworkReadClass {
    GapRepair,
    WalletBalance,
    StartupPoolHydration,
    StateReconciliation,
    QuoterParity,
}

impl NetworkReadClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GapRepair => "gap_repair",
            Self::WalletBalance => "wallet_balance",
            Self::StartupPoolHydration => "startup_pool_hydration",
            Self::StateReconciliation => "state_reconciliation",
            Self::QuoterParity => "quoter_parity",
        }
    }

    const fn policy(self) -> NetworkReadPolicy {
        match self {
            Self::GapRepair => NetworkReadPolicy::new(2, 8, 100, 15_000),
            Self::WalletBalance => NetworkReadPolicy::new(2, 8, 100, 5_000),
            Self::StartupPoolHydration => NetworkReadPolicy::new(2, 8, 100, 20_000),
            Self::StateReconciliation => NetworkReadPolicy::new(1, 4, 50, 10_000),
            Self::QuoterParity => NetworkReadPolicy::new(1, 2, 20, 2_000),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct NetworkReadPolicy {
    pub max_concurrency: usize,
    pub max_queued: usize,
    pub chunk_size: usize,
    pub timeout_ms: u64,
}

impl NetworkReadPolicy {
    const fn new(
        max_concurrency: usize,
        max_queued: usize,
        chunk_size: usize,
        timeout_ms: u64,
    ) -> Self {
        Self {
            max_concurrency,
            max_queued,
            chunk_size,
            timeout_ms,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProviderCapabilityProfile {
    pub id: String,
    pub supports_eip1898_block_hash: bool,
    pub max_batch_chunk_size: usize,
    pub multicall3_address: String,
    pub multicall3_code_identity: String,
}

#[derive(Debug)]
pub struct NetworkReadBatch {
    pub outputs: Vec<Vec<u8>>,
    pub queue_us: u128,
    pub provider_us: u128,
    pub decode_us: u128,
    pub publication_us: u128,
    pub chunk_count: usize,
    pub response_bytes: usize,
    pub requested_count: usize,
    pub returned_count: usize,
    pub complete: bool,
}

impl NetworkReadBatch {
    pub fn require_complete(self) -> anyhow::Result<Self> {
        ensure!(
            self.complete && self.requested_count == self.returned_count,
            "partial network read batch cannot be published"
        );
        Ok(self)
    }
}

struct ReadLane {
    semaphore: Arc<Semaphore>,
    queued: AtomicUsize,
    policy: NetworkReadPolicy,
}

impl ReadLane {
    fn new(class: NetworkReadClass) -> Self {
        let policy = class.policy();
        Self {
            semaphore: Arc::new(Semaphore::new(policy.max_concurrency)),
            queued: AtomicUsize::new(0),
            policy,
        }
    }
}

struct QueuedRead<'lane> {
    lane: &'lane ReadLane,
}

impl Drop for QueuedRead<'_> {
    fn drop(&mut self) {
        self.lane.queued.fetch_sub(1, Ordering::AcqRel);
    }
}

struct NetworkReadPermit {
    _permit: OwnedSemaphorePermit,
    queue_us: u128,
    policy: NetworkReadPolicy,
}

#[derive(Clone)]
pub struct NetworkReadCoordinator {
    inner: Arc<NetworkReadCoordinatorInner>,
}

struct NetworkReadCoordinatorInner {
    network_id: NetworkId,
    chain_id: u64,
    rpc: JsonRpcClient,
    lanes: BTreeMap<NetworkReadClass, ReadLane>,
    provider: ProviderCapabilityProfile,
    telemetry: TelemetryHandle,
    engine_id: String,
}

impl std::fmt::Debug for NetworkReadCoordinator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NetworkReadCoordinator")
            .field("network_id", &self.inner.network_id)
            .field("chain_id", &self.inner.chain_id)
            .field("provider", &self.inner.provider)
            .finish_non_exhaustive()
    }
}

impl NetworkReadCoordinator {
    fn new(
        plan: &CompiledNetworkPlan,
        rpc: JsonRpcClient,
        telemetry: TelemetryHandle,
        engine_id: String,
    ) -> Self {
        let classes = [
            NetworkReadClass::GapRepair,
            NetworkReadClass::WalletBalance,
            NetworkReadClass::StartupPoolHydration,
            NetworkReadClass::StateReconciliation,
            NetworkReadClass::QuoterParity,
        ];
        let lanes = classes
            .into_iter()
            .map(|class| (class, ReadLane::new(class)))
            .collect();
        Self {
            inner: Arc::new(NetworkReadCoordinatorInner {
                network_id: plan.network_id.clone(),
                chain_id: plan.chain_id,
                rpc,
                lanes,
                provider: ProviderCapabilityProfile {
                    id: format!("alchemy-json-rpc:{}", plan.network_id.as_str()),
                    supports_eip1898_block_hash: true,
                    max_batch_chunk_size: REVIEWED_BATCH_CHUNK_LIMIT,
                    multicall3_address: plan.multicall3_address.clone(),
                    multicall3_code_identity: "multicall3-canonical-ca11-v1".to_owned(),
                },
                telemetry,
                engine_id,
            }),
        }
    }

    pub fn rpc(&self) -> &JsonRpcClient {
        &self.inner.rpc
    }

    pub fn provider(&self) -> &ProviderCapabilityProfile {
        &self.inner.provider
    }

    async fn acquire(&self, class: NetworkReadClass) -> anyhow::Result<NetworkReadPermit> {
        let lane = self
            .inner
            .lanes
            .get(&class)
            .expect("all network read classes have a lane");
        let queued_before = lane.queued.fetch_add(1, Ordering::AcqRel);
        if queued_before >= lane.policy.max_queued {
            lane.queued.fetch_sub(1, Ordering::AcqRel);
            anyhow::bail!("{} network read queue is full", class.as_str());
        }
        let queued = QueuedRead { lane };
        let started_at = Instant::now();
        let permit = tokio::time::timeout(
            Duration::from_millis(lane.policy.timeout_ms),
            Arc::clone(&lane.semaphore).acquire_owned(),
        )
        .await
        .with_context(|| format!("{} network read queue timed out", class.as_str()))?
        .context("network read lane closed")?;
        drop(queued);
        Ok(NetworkReadPermit {
            _permit: permit,
            queue_us: started_at.elapsed().as_micros(),
            policy: lane.policy,
        })
    }

    pub async fn eth_call_batch(
        &self,
        class: NetworkReadClass,
        calls: &[EthCall],
        block: CanonicalBlock,
    ) -> anyhow::Result<NetworkReadBatch> {
        ensure!(
            self.inner.provider.supports_eip1898_block_hash,
            "provider does not prove EIP-1898 block-hash pinning"
        );
        let permit = self.acquire(class).await?;
        let requested_count = calls.len();
        let result = self
            .inner
            .rpc
            .eth_call_batch_bounded(calls, block, permit.policy.chunk_size)
            .await;
        match result {
            Ok(result) => {
                let publication_started = Instant::now();
                let returned_count = result.outputs.len();
                let complete = returned_count == requested_count;
                let publication_us = publication_started.elapsed().as_micros();
                self.emit_batch(
                    class,
                    block,
                    permit.queue_us,
                    result.provider_us,
                    result.decode_us,
                    publication_us,
                    result.chunk_count,
                    result.response_bytes,
                    requested_count,
                    returned_count,
                    complete,
                    "success",
                );
                NetworkReadBatch {
                    outputs: result.outputs,
                    queue_us: permit.queue_us,
                    provider_us: result.provider_us,
                    decode_us: result.decode_us,
                    publication_us,
                    chunk_count: result.chunk_count,
                    response_bytes: result.response_bytes,
                    requested_count,
                    returned_count,
                    complete,
                }
                .require_complete()
            }
            Err(error) => {
                self.emit_batch(
                    class,
                    block,
                    permit.queue_us,
                    0,
                    0,
                    0,
                    calls.len().div_ceil(permit.policy.chunk_size),
                    0,
                    calls.len(),
                    0,
                    false,
                    "failed",
                );
                Err(error)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_batch(
        &self,
        class: NetworkReadClass,
        block: CanonicalBlock,
        queue_us: u128,
        provider_us: u128,
        decode_us: u128,
        publication_us: u128,
        chunk_count: usize,
        response_bytes: usize,
        requested_count: usize,
        returned_count: usize,
        complete: bool,
        outcome: &'static str,
    ) {
        self.inner.telemetry.emit(
            "network_read_batch",
            serde_json::json!({
                "engine_id": self.inner.engine_id,
                "network_id": self.inner.network_id.as_str(),
                "chain_id": self.inner.chain_id,
                "connection_generation": CONNECTION_GENERATION,
                "read_class": class.as_str(),
                "provider_capability_profile": self.inner.provider.id.as_str(),
                "supports_eip1898_block_hash": self.inner.provider.supports_eip1898_block_hash,
                "multicall3_code_identity":
                    self.inner.provider.multicall3_code_identity.as_str(),
                "block_number": block.number,
                "block_hash": format!("{:#x}", block.hash),
                "queue_us": queue_us,
                "provider_us": provider_us,
                "decode_us": decode_us,
                "publication_us": publication_us,
                "chunk_count": chunk_count,
                "response_bytes": response_bytes,
                "requested_count": requested_count,
                "returned_count": returned_count,
                "complete": complete,
                "outcome": outcome,
            }),
        );
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum EvmExecutionCommandKind {
    Swap,
    Approval,
    Transfer,
    Bridge,
    Rebalance,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EvmExecutionCommand {
    pub operation_id: String,
    pub chain_id: u64,
    pub execution_lane_id: ExecutionLaneId,
    pub kind: EvmExecutionCommandKind,
}

pub trait EvmExecutionOwnerBoundary {
    fn authorize(&self, command: &EvmExecutionCommand) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub struct EvmExecutionOwner {
    chain_id: u64,
    execution_lane_id: ExecutionLaneId,
    mutation_enabled: bool,
    gas_policy: CompiledNetworkGasPolicy,
}

impl EvmExecutionOwner {
    pub fn mutation_enabled(&self) -> bool {
        self.mutation_enabled
    }

    pub fn gas_policy(&self) -> &CompiledNetworkGasPolicy {
        &self.gas_policy
    }
}

impl EvmExecutionOwnerBoundary for EvmExecutionOwner {
    fn authorize(&self, command: &EvmExecutionCommand) -> anyhow::Result<()> {
        ensure!(
            !command.operation_id.trim().is_empty(),
            "EVM execution operation id is empty"
        );
        ensure!(
            command.chain_id == self.chain_id
                && command.execution_lane_id == self.execution_lane_id,
            "EVM execution command is routed to the wrong owner"
        );
        ensure!(
            self.mutation_enabled,
            "EVM mutations are disabled for chain {}",
            self.chain_id
        );
        ensure!(
            matches!(
                self.gas_policy,
                CompiledNetworkGasPolicy::WorldChainV12 { .. }
                    | CompiledNetworkGasPolicy::ArbitrumOne { .. }
            ),
            "EVM execution owner has no reviewed live gas policy"
        );
        Ok(())
    }
}

pub struct NetworkRuntime {
    plan: CompiledNetworkPlan,
    ws_endpoint: String,
    initial_head: CanonicalBlock,
    reads: NetworkReadCoordinator,
    execution: EvmExecutionOwner,
}

impl std::fmt::Debug for NetworkRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NetworkRuntime")
            .field("network_id", &self.plan.network_id)
            .field("chain_id", &self.plan.chain_id)
            .field("ws_endpoint", &"<redacted>")
            .field("initial_head", &self.initial_head)
            .field("execution", &self.execution)
            .finish_non_exhaustive()
    }
}

impl NetworkRuntime {
    pub fn plan(&self) -> &CompiledNetworkPlan {
        &self.plan
    }

    pub fn rpc(&self) -> &JsonRpcClient {
        self.reads.rpc()
    }

    pub fn reads(&self) -> &NetworkReadCoordinator {
        &self.reads
    }

    pub fn ws_endpoint(&self) -> &str {
        &self.ws_endpoint
    }

    pub fn initial_head(&self) -> CanonicalBlock {
        self.initial_head
    }

    pub fn execution(&self) -> &EvmExecutionOwner {
        &self.execution
    }
}

pub struct NetworkRuntimeRegistry {
    runtimes: BTreeMap<NetworkId, NetworkRuntime>,
}

impl NetworkRuntimeRegistry {
    pub async fn connect(
        plan: CompiledNetworkRuntimePlan,
        telemetry: TelemetryHandle,
        engine_id: String,
    ) -> anyhow::Result<Self> {
        let runtimes = try_join_all(plan.networks.into_iter().map(|network| {
            let telemetry = telemetry.clone();
            let engine_id = engine_id.clone();
            async move {
                let rpc_endpoint = std::env::var(&network.rpc_url_env).with_context(|| {
                    format!(
                        "required environment variable {} is not set",
                        network.rpc_url_env
                    )
                })?;
                let ws_endpoint = std::env::var(&network.ws_url_env).with_context(|| {
                    format!(
                        "required environment variable {} is not set",
                        network.ws_url_env
                    )
                })?;
                let parsed_ws = reqwest::Url::parse(&ws_endpoint)
                    .context("network WebSocket endpoint must be an absolute URL")?;
                ensure!(
                    matches!(parsed_ws.scheme(), "ws" | "wss") && parsed_ws.host_str().is_some(),
                    "network WebSocket endpoint is invalid"
                );
                let rpc = JsonRpcClient::new(rpc_endpoint)?;
                let (observed_chain_id, initial_head) =
                    tokio::try_join!(rpc.chain_id(), rpc.latest_block())?;
                ensure!(
                    observed_chain_id == network.chain_id,
                    "network {} RPC returned chain {}",
                    network.network_id.as_str(),
                    observed_chain_id
                );
                let reads = NetworkReadCoordinator::new(
                    &network,
                    rpc,
                    telemetry.clone(),
                    engine_id.clone(),
                );
                let execution = EvmExecutionOwner {
                    chain_id: network.chain_id,
                    execution_lane_id: network.execution_lane_id.clone(),
                    mutation_enabled: network.execution_enabled,
                    gas_policy: network.gas_policy.clone(),
                };
                telemetry.emit(
                    "network_runtime_started",
                    serde_json::json!({
                        "engine_id": engine_id,
                        "network_id": network.network_id.as_str(),
                        "chain_id": network.chain_id,
                        "connection_generation": CONNECTION_GENERATION,
                        "initial_block_number": initial_head.number,
                        "initial_block_hash": format!("{:#x}", initial_head.hash),
                        "pool_count": network.pool_ids.len(),
                        "asset_count": network.assets.len(),
                        "wallet_location_id": network.wallet_location_id.as_str(),
                        "execution_lane_id": network.execution_lane_id.as_str(),
                        "execution_enabled": network.execution_enabled,
                        "gas_policy": match &network.gas_policy {
                            CompiledNetworkGasPolicy::WorldChainV12 { .. } => "world_chain_v12",
                            CompiledNetworkGasPolicy::ArbitrumOne { .. } =>
                                "arbitrum_one_fail_closed",
                            CompiledNetworkGasPolicy::ReadOnly => "read_only",
                        },
                        "provider_capability_profile": reads.provider().id.as_str(),
                        "supports_eip1898_block_hash":
                            reads.provider().supports_eip1898_block_hash,
                        "max_batch_chunk_size": reads.provider().max_batch_chunk_size,
                        "multicall3_address": reads.provider().multicall3_address.as_str(),
                        "multicall3_code_identity":
                            reads.provider().multicall3_code_identity.as_str(),
                    }),
                );
                Ok::<_, anyhow::Error>((
                    network.network_id.clone(),
                    NetworkRuntime {
                        plan: network,
                        ws_endpoint,
                        initial_head,
                        reads,
                        execution,
                    },
                ))
            }
        }))
        .await?
        .into_iter()
        .collect::<BTreeMap<_, _>>();
        ensure!(!runtimes.is_empty(), "network runtime registry is empty");
        Ok(Self { runtimes })
    }

    pub fn get_by_chain_id(&self, chain_id: u64) -> anyhow::Result<&NetworkRuntime> {
        self.runtimes
            .values()
            .find(|runtime| runtime.plan.chain_id == chain_id)
            .with_context(|| format!("network runtime for chain {chain_id} is missing"))
    }

    pub fn runtimes(&self) -> impl Iterator<Item = &NetworkRuntime> {
        self.runtimes.values()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::domain::compiled::{CompiledNetworkGasPolicy, ExecutionLaneId};

    use super::{
        EvmExecutionCommand, EvmExecutionCommandKind, EvmExecutionOwner, EvmExecutionOwnerBoundary,
        NetworkReadBatch, NetworkReadClass, ReadLane,
    };

    #[test]
    fn partial_batch_cannot_be_published() {
        let batch = NetworkReadBatch {
            outputs: vec![vec![1]],
            queue_us: 0,
            provider_us: 0,
            decode_us: 0,
            publication_us: 0,
            chunk_count: 1,
            response_bytes: 1,
            requested_count: 2,
            returned_count: 1,
            complete: false,
        };
        assert!(batch.require_complete().is_err());
    }

    #[test]
    fn read_classes_have_independent_capacity_and_bounded_chunks() {
        let gap = ReadLane::new(NetworkReadClass::GapRepair);
        let wallet = ReadLane::new(NetworkReadClass::WalletBalance);
        let quoter = ReadLane::new(NetworkReadClass::QuoterParity);
        assert!(!Arc::ptr_eq(&gap.semaphore, &wallet.semaphore));
        assert!(!Arc::ptr_eq(&wallet.semaphore, &quoter.semaphore));
        assert_eq!(gap.policy.chunk_size, 100);
        assert_eq!(wallet.policy.chunk_size, 100);
        assert_eq!(quoter.policy.chunk_size, 20);
        assert!(gap.policy.max_queued > 0);
        assert!(wallet.policy.max_queued > 0);
        assert!(quoter.policy.max_queued > 0);
    }

    #[test]
    fn execution_owner_routes_by_chain_and_lane_and_keeps_arbitrum_mutation_disabled() {
        let world_lane = ExecutionLaneId::new("lane-world-live").unwrap();
        let world = EvmExecutionOwner {
            chain_id: 480,
            execution_lane_id: world_lane.clone(),
            mutation_enabled: true,
            gas_policy: CompiledNetworkGasPolicy::WorldChainV12 {
                fallback_gas_price_wei: 100_000,
                includes_l1_fee: true,
            },
        };
        let command = EvmExecutionCommand {
            operation_id: "operation-1".to_owned(),
            chain_id: 480,
            execution_lane_id: world_lane,
            kind: EvmExecutionCommandKind::Swap,
        };
        assert!(world.authorize(&command).is_ok());

        let arbitrum = EvmExecutionOwner {
            chain_id: 42_161,
            execution_lane_id: ExecutionLaneId::new("lane-arbitrum-read-only").unwrap(),
            mutation_enabled: false,
            gas_policy: CompiledNetworkGasPolicy::ArbitrumOne {
                requires_fresh_rpc_gas_price: true,
                max_priority_fee_per_gas_wei: 0,
                includes_l1_fee: false,
            },
        };
        let command = EvmExecutionCommand {
            operation_id: "operation-2".to_owned(),
            chain_id: 42_161,
            execution_lane_id: ExecutionLaneId::new("lane-arbitrum-read-only").unwrap(),
            kind: EvmExecutionCommandKind::Swap,
        };
        assert!(arbitrum.authorize(&command).is_err());

        let reviewed_future_owner = EvmExecutionOwner {
            chain_id: 42_161,
            execution_lane_id: command.execution_lane_id.clone(),
            mutation_enabled: true,
            gas_policy: CompiledNetworkGasPolicy::ArbitrumOne {
                requires_fresh_rpc_gas_price: true,
                max_priority_fee_per_gas_wei: 0,
                includes_l1_fee: false,
            },
        };
        assert!(reviewed_future_owner.authorize(&command).is_ok());
    }
}

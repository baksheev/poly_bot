use std::{
    path::PathBuf,
    thread::JoinHandle,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use alloy_primitives::{Address, B256, U256, keccak256};
use anyhow::{Context, bail, ensure};
use tokio::sync::{mpsc, oneshot, watch};

use crate::{
    chain::{
        logs::ChainLog,
        rpc::{CanonicalBlock, JsonRpcClient, ReceiptLog, TransactionReceipt},
    },
    dex::events::{
        CamelotFeeReceiptProof, PoolLocator, camelot_fee_topic, pancake_v3_swap_topic,
        v3_swap_topic, v4_swap_topic,
    },
    domain::compiled::CompiledNetworkGasPolicy,
    pretrade_cost::{
        DexPoolCostKey, DexProtocol as CostTelemetryDexProtocol, DexRouteCostKey,
        GasPriceTelemetrySource, PreTradeCostTelemetry, ReceiptCostTelemetrySource,
    },
    telemetry::ExecutionLatencyTelemetry,
    wallet::{
        EvmJournalScope, EvmWallet, JournalStatus, NonceLane, NonceReconciliationOutcome,
        PROCESS_NONCE_LOCK_TTL, TransactionJournal, UnknownOutcomeReason, WalletCall,
        WalletTransactionParameters, acquire_process_nonce_lock, broadcast_signed_transaction,
    },
};

use super::calldata::{
    camelot_v3_exact_input_single, decode_permit2_allowance, pancake_v3_exact_input_single,
    permit2_allowance, permit2_approve, v3_exact_input, v4_exact_input_single,
};
use super::pool_id::V4PoolKey;

pub const PERMIT2_ADDRESS: Address = Address::new([
    0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0xd4, 0x73, 0x03, 0x0f, 0x11, 0x6d, 0xde, 0xe9, 0xf6, 0xb4,
    0x3a, 0xc7, 0x8b, 0xa3,
]);

const RAILS_PRIORITY_FEE_WEI: u128 = 1_500_000;
const RAILS_FALLBACK_GAS_PRICE_WEI: u128 = 100_000;
const RAILS_APPROVAL_DEFAULT_GAS_LIMIT: u64 = 800_000;
// One local-curve fallback for V3 and V4. It leaves roughly 27% headroom over
// the largest observed Rails V3/V4 receipt from 2026-05-25..2026-07-25.
const HISTORICAL_SWAP_GAS_LIMIT: u64 = 250_000;
// P6 starts with an explicit provider-scoped conservative fallback. P7 may
// tighten it only after pinned simulation and a reviewed receipt cohort.
// The exact pinned ARB/USDC route replays at 753,956 gas through its historical
// aggregation envelope. A one-million fallback safely covers that upper-bound
// observation when the immediate live path deliberately skips estimation;
// only gas actually consumed is charged.
const CAMELOT_V3_SWAP_GAS_LIMIT: u64 = 1_000_000;
const RAILS_PERMIT2_APPROVAL_GAS_LIMIT: u64 = 120_000;
const CAPITAL_TRANSFER_GAS_LIMIT: u64 = 200_000;
const GAS_PRICE_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const GAS_PRICE_CACHE_TTL: Duration = Duration::from_secs(2);
const DEFAULT_SWAP_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(5);
const APPROVAL_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(120);
const FAST_RECEIPT_POLL_INTERVAL: Duration = Duration::from_millis(50);
const FAST_RECEIPT_POLL_WINDOW: Duration = Duration::from_secs(1);
const SLOW_RECEIPT_POLL_INTERVAL: Duration = Duration::from_millis(250);
const MAX_GAS_LIMIT: u64 = 5_000_000;
const PERMIT2_APPROVAL_VALIDITY: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const PERMIT2_MIN_REMAINING_VALIDITY: Duration = Duration::from_secs(60 * 60);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DexProtocol {
    UniswapV3,
    PancakeSwapV3,
    CamelotV3,
    UniswapV4,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SwapSubmissionPolicy {
    SimulateAndEstimate,
    Immediate,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AllowanceRequirement {
    pub operation_id: String,
    pub protocol: DexProtocol,
    pub token: Address,
    pub router: Address,
    pub required: U256,
}

impl DexProtocol {
    pub const fn label(self) -> &'static str {
        match self {
            Self::UniswapV3 => "uniswap_v3",
            Self::PancakeSwapV3 => "pancakeswap_v3",
            Self::CamelotV3 => "camelot_v3",
            Self::UniswapV4 => "uniswap_v4",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SwapRoute {
    UniswapV3 {
        router: Address,
        pool: Address,
        fee_pips: u32,
    },
    PancakeSwapV3 {
        router: Address,
        pool: Address,
        fee_pips: u32,
    },
    CamelotV3 {
        router: Address,
        pool: Address,
    },
    V4 {
        router: Address,
        pool_key: V4PoolKey,
    },
}

impl SwapRoute {
    pub const fn protocol(self) -> DexProtocol {
        match self {
            Self::UniswapV3 { .. } => DexProtocol::UniswapV3,
            Self::PancakeSwapV3 { .. } => DexProtocol::PancakeSwapV3,
            Self::CamelotV3 { .. } => DexProtocol::CamelotV3,
            Self::V4 { .. } => DexProtocol::UniswapV4,
        }
    }

    pub const fn router(self) -> Address {
        match self {
            Self::UniswapV3 { router, .. }
            | Self::PancakeSwapV3 { router, .. }
            | Self::CamelotV3 { router, .. }
            | Self::V4 { router, .. } => router,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExactInputSwapRequest {
    pub operation_id: String,
    pub route: SwapRoute,
    pub token_in: Address,
    pub token_out: Address,
    pub amount_in: U256,
    pub amount_out_minimum: U256,
    /// Quoter gas returned by Rails-compatible quote construction, if known.
    pub quoted_gas: Option<u64>,
    /// Explicit gas added after the Rails v3/v4 multiplier.
    pub additional_gas: u64,
    pub deadline_unix_seconds: u64,
    pub confirmation_timeout: Duration,
    pub submission_policy: SwapSubmissionPolicy,
    /// Read-only recovery of an already journaled swap. This mode may inspect
    /// the canonical receipt, but it must never authorize signing or broadcast.
    pub reconciliation_only: bool,
}

impl ExactInputSwapRequest {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            !self.operation_id.is_empty()
                && self.operation_id.len() <= 120
                && self.operation_id.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')
                }),
            "DEX operation id contains invalid characters"
        );
        ensure!(
            self.route.router() != Address::ZERO,
            "Uniswap router is zero"
        );
        ensure!(self.token_in != Address::ZERO, "DEX input token is zero");
        ensure!(self.token_out != Address::ZERO, "DEX output token is zero");
        ensure!(self.token_in != self.token_out, "DEX tokens are identical");
        ensure!(!self.amount_in.is_zero(), "DEX input amount is zero");
        ensure!(
            !self.amount_out_minimum.is_zero(),
            "DEX minimum output amount is zero"
        );
        ensure!(
            self.additional_gas <= MAX_GAS_LIMIT,
            "additional DEX gas exceeds safety cap"
        );
        ensure!(
            !self.confirmation_timeout.is_zero(),
            "DEX confirmation timeout is zero"
        );
        match self.route {
            SwapRoute::UniswapV3 { pool, fee_pips, .. }
            | SwapRoute::PancakeSwapV3 { pool, fee_pips, .. } => {
                ensure!(pool != Address::ZERO, "V3 pool is zero");
                ensure!(fee_pips > 0 && fee_pips <= 0x00ff_ffff, "invalid V3 fee");
            }
            SwapRoute::CamelotV3 { pool, .. } => {
                ensure!(pool != Address::ZERO, "Camelot V3 pool is zero");
            }
            SwapRoute::V4 { pool_key, .. } => {
                ensure!(
                    pool_key.currency0 < pool_key.currency1,
                    "V4 pool key is unsorted"
                );
                ensure!(
                    (self.token_in == pool_key.currency0 && self.token_out == pool_key.currency1)
                        || (self.token_in == pool_key.currency1
                            && self.token_out == pool_key.currency0),
                    "V4 route tokens do not match its pool key"
                );
            }
        }
        Ok(())
    }

    pub fn with_rails_defaults(
        operation_id: impl Into<String>,
        route: SwapRoute,
        token_in: Address,
        token_out: Address,
        amount_in: U256,
        amount_out_minimum: U256,
        deadline_unix_seconds: u64,
    ) -> Self {
        Self {
            operation_id: operation_id.into(),
            route,
            token_in,
            token_out,
            amount_in,
            amount_out_minimum,
            quoted_gas: None,
            additional_gas: 0,
            deadline_unix_seconds,
            confirmation_timeout: DEFAULT_SWAP_CONFIRMATION_TIMEOUT,
            submission_policy: SwapSubmissionPolicy::SimulateAndEstimate,
            reconciliation_only: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SwapExecutionOutcome {
    pub protocol: DexProtocol,
    pub transaction_hash: B256,
    pub block_number: u64,
    pub gas_used: u64,
    pub effective_gas_price: u128,
    pub l1_fee: u128,
    pub token_in_spent: U256,
    pub token_out_received: U256,
    /// Camelot-only Fee event positionally preceding `settlement_log` in the
    /// same successful transaction. Both are absent when the receipt cannot
    /// provide a complete acceleration proof and the WebSocket mirror remains
    /// the settlement fallback.
    pub settlement_fee: Option<CamelotFeeReceiptProof>,
    pub settlement_log: Option<ChainLog>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ReceiptSettlementLogs {
    fee: Option<CamelotFeeReceiptProof>,
    swap: Option<ChainLog>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReceiptSettlementKind {
    Fee,
    Swap,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadOnlySwapSimulation {
    pub protocol: DexProtocol,
    pub wallet: Address,
    pub router: Address,
    pub calldata_hash: B256,
    pub selector: [u8; 4],
    pub estimated_gas: u64,
    pub policy_gas_limit: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GasLimitPolicy {
    multiplier: u64,
    minimum: u64,
    default: u64,
    additional: u64,
}

#[derive(Clone, Copy, Debug)]
struct ExecuteCallPolicy {
    gas: GasLimitPolicy,
    quoted_gas: Option<u64>,
    confirmation_timeout: Duration,
    submission_policy: SwapSubmissionPolicy,
    allow_new_submission: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GasPriceSource {
    Rpc,
    RailsFallback,
}

impl GasPriceSource {
    const fn label(self) -> &'static str {
        match self {
            Self::Rpc => "cached_rpc",
            Self::RailsFallback => "cached_rails_fallback",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct GasPriceSample {
    captured_at: Instant,
    wei: u128,
    source: GasPriceSource,
}

impl GasPriceSample {
    fn is_fresh(self) -> bool {
        self.captured_at.elapsed() < GAS_PRICE_CACHE_TTL
    }
}

impl GasLimitPolicy {
    const fn for_swap(protocol: DexProtocol, additional: u64) -> Self {
        match protocol {
            DexProtocol::UniswapV3 | DexProtocol::PancakeSwapV3 => Self {
                multiplier: 2,
                minimum: 0,
                default: HISTORICAL_SWAP_GAS_LIMIT,
                additional,
            },
            DexProtocol::CamelotV3 => Self {
                multiplier: 2,
                minimum: 0,
                default: CAMELOT_V3_SWAP_GAS_LIMIT,
                additional,
            },
            DexProtocol::UniswapV4 => Self {
                multiplier: 4,
                minimum: HISTORICAL_SWAP_GAS_LIMIT,
                default: HISTORICAL_SWAP_GAS_LIMIT,
                additional,
            },
        }
    }

    const fn fixed(limit: u64) -> Self {
        Self {
            multiplier: 1,
            minimum: limit,
            default: limit,
            additional: 0,
        }
    }

    fn resolve(self, quoted_gas: Option<u64>, estimated_gas: u64) -> anyhow::Result<u64> {
        ensure!(estimated_gas > 0, "RPC returned zero gas estimate");
        let multiplied_estimate = estimated_gas
            .checked_mul(self.multiplier)
            .context("estimated gas multiplier overflow")?;
        let rails_limit = match quoted_gas {
            Some(quoted_gas) => quoted_gas
                .checked_mul(self.multiplier)
                .context("Rails-compatible gas multiplier overflow")?,
            // Local quotes do not carry QuoterV2's gas field. Retain the
            // historical production floor while also applying the Rails
            // protocol multiplier to a fresh RPC estimate.
            None => self.default.max(multiplied_estimate),
        };
        let estimate_with_extra = estimated_gas
            .checked_add(self.additional)
            .context("estimated gas addition overflow")?;
        let rails_with_extra = rails_limit
            .max(self.minimum)
            .checked_add(self.additional)
            .context("Rails-compatible gas addition overflow")?;
        let limit = rails_with_extra.max(estimate_with_extra);
        ensure!(limit <= MAX_GAS_LIMIT, "DEX gas limit exceeds safety cap");
        Ok(limit)
    }

    fn resolve_without_estimate(self, quoted_gas: Option<u64>) -> anyhow::Result<u64> {
        let rails_limit = match quoted_gas {
            Some(quoted_gas) => quoted_gas
                .checked_mul(self.multiplier)
                .context("Rails-compatible gas multiplier overflow")?,
            None => self.default,
        };
        let limit = rails_limit
            .max(self.minimum)
            .checked_add(self.additional)
            .context("Rails-compatible gas addition overflow")?;
        ensure!(limit <= MAX_GAS_LIMIT, "DEX gas limit exceeds safety cap");
        Ok(limit)
    }
}

pub struct DexExecutor {
    rpc: JsonRpcClient,
    wallet: EvmWallet,
    nonce_lane: NonceLane,
    journal: TransactionJournal,
    gas_price: Option<GasPriceSample>,
    gas_policy: CompiledNetworkGasPolicy,
    allowance_mutations_enabled: bool,
    camelot_submissions_enabled: bool,
    last_terminal_receipt: Option<TransactionReceipt>,
    receipt_heads: Option<watch::Receiver<CanonicalBlock>>,
    latency_telemetry: Option<ExecutionLatencyTelemetry>,
    pretrade_cost_telemetry: Option<PreTradeCostTelemetry>,
}

impl std::fmt::Debug for DexExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DexExecutor")
            .field("wallet", &self.wallet.address())
            .field("chain_id", &self.nonce_lane.chain_id())
            .field("nonce_state", self.nonce_lane.state())
            .finish_non_exhaustive()
    }
}

impl DexExecutor {
    pub async fn hydrate(
        rpc: JsonRpcClient,
        wallet: EvmWallet,
        chain_id: u64,
        journal_path: PathBuf,
    ) -> anyhow::Result<Self> {
        Self::hydrate_with_gas_policy(
            rpc,
            wallet,
            chain_id,
            journal_path,
            CompiledNetworkGasPolicy::WorldChainV12 {
                fallback_gas_price_wei: RAILS_FALLBACK_GAS_PRICE_WEI,
                includes_l1_fee: true,
            },
        )
        .await
    }

    pub async fn hydrate_with_gas_policy(
        rpc: JsonRpcClient,
        wallet: EvmWallet,
        chain_id: u64,
        journal_path: PathBuf,
        gas_policy: CompiledNetworkGasPolicy,
    ) -> anyhow::Result<Self> {
        ensure!(
            matches!(
                (&gas_policy, chain_id),
                (CompiledNetworkGasPolicy::WorldChainV12 { .. }, 480)
                    | (CompiledNetworkGasPolicy::ArbitrumOne { .. }, 42_161)
            ),
            "DEX executor chain has no reviewed mutation fee policy"
        );
        ensure!(
            rpc.chain_id().await? == chain_id,
            "DEX RPC chain id mismatch"
        );
        let owner = wallet.address();
        let latest_nonce = rpc.latest_nonce(owner).await?;
        let pending_nonce = rpc.pending_nonce(owner).await?;
        let mut journal = TransactionJournal::open(journal_path)?;
        let reconciled = NonceLane::reconcile(
            &rpc,
            &mut journal,
            chain_id,
            owner,
            latest_nonce,
            pending_nonce,
        )
        .await?;
        let outcome_label = reconciled.outcome.label();
        let mut nonce_lane = reconciled.lane;
        if let NonceReconciliationOutcome::TransactionKnown {
            transaction_hash, ..
        } = reconciled.outcome
        {
            let receipt =
                wait_for_receipt(&rpc, None, transaction_hash, APPROVAL_CONFIRMATION_TIMEOUT)
                    .await
                    .context("failed to finish known DEX transaction recovery")?;
            nonce_lane.record_receipt(&mut journal, receipt)?;
        }
        ensure!(
            nonce_lane.ready(),
            "DEX nonce lane requires operator recovery ({outcome_label})"
        );
        Ok(Self {
            rpc,
            wallet,
            nonce_lane,
            journal,
            gas_price: None,
            gas_policy,
            allowance_mutations_enabled: true,
            camelot_submissions_enabled: false,
            last_terminal_receipt: None,
            receipt_heads: None,
            latency_telemetry: None,
            pretrade_cost_telemetry: None,
        })
    }

    pub fn set_latency_telemetry(&mut self, telemetry: ExecutionLatencyTelemetry) {
        self.latency_telemetry = Some(telemetry);
    }

    pub fn set_pretrade_cost_telemetry(&mut self, telemetry: PreTradeCostTelemetry) {
        self.pretrade_cost_telemetry = Some(telemetry);
    }

    /// Best-effort diagnostic bootstrap from the newest successful swap in
    /// the durable EVM journal. It runs independently of readiness and the
    /// execution owner, so RPC latency or failure cannot delay trading.
    pub fn spawn_pretrade_cost_receipt_bootstrap(&self) {
        let Some(telemetry) = self.pretrade_cost_telemetry.clone() else {
            return;
        };
        let rpc = self.rpc.clone();
        let includes_l1_fee = matches!(
            self.gas_policy,
            CompiledNetworkGasPolicy::WorldChainV12 {
                includes_l1_fee: true,
                ..
            }
        );
        for protocol in [
            CostTelemetryDexProtocol::UniswapV3,
            CostTelemetryDexProtocol::PancakeSwapV3,
            CostTelemetryDexProtocol::CamelotV3,
            CostTelemetryDexProtocol::UniswapV4,
        ] {
            let candidate = self
                .journal
                .operations()
                .values()
                .filter(|operation| operation.intent.purpose == protocol.label())
                .filter_map(|operation| {
                    let JournalStatus::MinedSuccess {
                        transaction_hash,
                        block_number,
                    } = operation.status
                    else {
                        return None;
                    };
                    Some((block_number, transaction_hash))
                })
                .max_by_key(|(block_number, _)| *block_number);
            let Some((block_number, transaction_hash)) = candidate else {
                continue;
            };
            let rpc = rpc.clone();
            let telemetry = telemetry.clone();
            tokio::spawn(async move {
                let lookup = tokio::time::timeout(
                    Duration::from_secs(5),
                    rpc.transaction_receipt(transaction_hash),
                )
                .await;
                match lookup {
                    Ok(Ok(Some(receipt))) if receipt.status == 1 => {
                        let source_event_unix_us = tokio::time::timeout(
                            Duration::from_secs(5),
                            rpc.block_timestamp(receipt.block_number),
                        )
                        .await
                        .ok()
                        .and_then(Result::ok)
                        .and_then(|seconds| seconds.checked_mul(1_000_000));
                        telemetry.publish_protocol_receipt_with_source(
                            protocol,
                            receipt.gas_used,
                            receipt.effective_gas_price,
                            if includes_l1_fee { receipt.l1_fee } else { 0 },
                            Some(receipt.block_number),
                            source_event_unix_us,
                            ReceiptCostTelemetrySource::JournalBootstrap,
                        );
                        tracing::info!(
                            block_number,
                            protocol = protocol.label(),
                            source_event_timestamp_available = source_event_unix_us.is_some(),
                            "pre-trade receipt-cost telemetry bootstrapped from journal"
                        );
                    }
                    Ok(Ok(_)) => tracing::warn!(
                        block_number,
                        protocol = protocol.label(),
                        "journal bootstrap receipt is unavailable or unsuccessful"
                    ),
                    Ok(Err(error)) => tracing::warn!(
                        block_number,
                        protocol = protocol.label(),
                        error = %error,
                        "journal bootstrap receipt lookup failed"
                    ),
                    Err(_) => tracing::warn!(
                        block_number,
                        protocol = protocol.label(),
                        "journal bootstrap receipt lookup timed out"
                    ),
                }
            });
        }
    }

    /// Wake receipt lookup from the process-wide Alchemy new-head stream.
    /// Timed HTTP polling remains the fallback for missed notifications.
    pub fn set_receipt_heads(&mut self, receiver: watch::Receiver<CanonicalBlock>) {
        self.receipt_heads = Some(receiver);
    }

    fn emit_latency_stage(
        &self,
        operation_id: &str,
        stage: &'static str,
        started_at: Instant,
        outcome: &'static str,
    ) {
        if let Some(telemetry) = &self.latency_telemetry {
            telemetry.emit_stage(
                "dex",
                operation_id,
                stage,
                duration_us(started_at.elapsed()),
                outcome,
            );
        }
    }

    pub fn wallet_address(&self) -> Address {
        self.wallet.address()
    }

    pub fn chain_id(&self) -> u64 {
        self.nonce_lane.chain_id()
    }

    pub fn set_journal_scope(&mut self, scope: EvmJournalScope) -> anyhow::Result<()> {
        self.nonce_lane.set_journal_scope(scope)
    }

    /// Performs any required approval writes before trading starts, then makes
    /// the execution worker permanently read-only with respect to allowances.
    pub async fn prepare_and_lock_allowances(
        &mut self,
        requirements: &[AllowanceRequirement],
    ) -> anyhow::Result<()> {
        ensure!(
            self.allowance_mutations_enabled,
            "DEX allowance preparation is already locked"
        );
        ensure!(
            !requirements.is_empty(),
            "DEX allowance requirement set is empty"
        );
        for requirement in requirements {
            ensure!(
                !requirement.operation_id.is_empty(),
                "DEX allowance operation id is empty"
            );
            ensure!(
                requirement.token != Address::ZERO,
                "DEX allowance token is zero"
            );
            ensure!(
                requirement.router != Address::ZERO,
                "DEX allowance router is zero"
            );
            ensure!(
                !requirement.required.is_zero(),
                "DEX allowance amount is zero"
            );
            match requirement.protocol {
                DexProtocol::UniswapV3 | DexProtocol::PancakeSwapV3 | DexProtocol::CamelotV3 => {
                    self.ensure_erc20_allowance(
                        &format!(
                            "{}.{}-router-approval",
                            requirement.operation_id,
                            requirement.protocol.label()
                        ),
                        requirement.token,
                        requirement.router,
                        requirement.required,
                    )
                    .await?;
                }
                DexProtocol::UniswapV4 => {
                    self.ensure_erc20_allowance(
                        &format!("{}.permit2-erc20-approval", requirement.operation_id),
                        requirement.token,
                        PERMIT2_ADDRESS,
                        requirement.required,
                    )
                    .await?;
                    self.ensure_permit2_allowance(
                        &format!("{}.permit2-router-approval", requirement.operation_id),
                        requirement.token,
                        requirement.router,
                        requirement.required,
                    )
                    .await?;
                }
            }
        }
        self.allowance_mutations_enabled = false;
        Ok(())
    }

    /// Permanently disables allowance writes when durable launch/canary risk
    /// proves that no new parent may be admitted. Existing journaled work can
    /// still reconcile, but startup must not create fresh approval authority.
    pub fn lock_allowance_mutations_without_preparation(&mut self) -> anyhow::Result<()> {
        ensure!(
            self.allowance_mutations_enabled,
            "DEX allowance preparation is already locked"
        );
        self.allowance_mutations_enabled = false;
        Ok(())
    }

    /// P6 keeps this unopened. The direct-live rollout may call it only after
    /// the exact Camelot token/router allowances have been prepared and the
    /// executor has permanently locked allowance mutation.
    pub fn enable_camelot_submissions_after_allowance_lock(&mut self) -> anyhow::Result<()> {
        ensure!(
            !self.allowance_mutations_enabled,
            "Camelot submission cannot open before allowances are locked"
        );
        self.camelot_submissions_enabled = true;
        Ok(())
    }

    /// Runs only `eth_call` and `eth_estimateGas` against the exact locally
    /// built call. It never reserves a nonce, writes the journal, signs,
    /// broadcasts, or mutates an allowance.
    pub async fn simulate_exact_input_read_only(
        &self,
        request: &ExactInputSwapRequest,
    ) -> anyhow::Result<ReadOnlySwapSimulation> {
        request.validate()?;
        let calldata = exact_input_calldata(request, self.wallet.address())?;
        let selector: [u8; 4] = calldata[..4]
            .try_into()
            .expect("validated DEX calldata always has a selector");
        let call =
            WalletCall::validated_contract_call(request.route.router(), U256::ZERO, calldata)?;
        let rpc_call = call.rpc_call(self.wallet.address());
        self.rpc
            .simulate_transaction(&rpc_call)
            .await
            .context("read-only DEX simulation reverted")?;
        let estimated_gas = self.rpc.estimate_gas(&rpc_call).await?;
        let policy_gas_limit =
            GasLimitPolicy::for_swap(request.route.protocol(), request.additional_gas)
                .resolve(request.quoted_gas, estimated_gas)?;
        Ok(ReadOnlySwapSimulation {
            protocol: request.route.protocol(),
            wallet: self.wallet.address(),
            router: request.route.router(),
            calldata_hash: keccak256(call.calldata()),
            selector,
            estimated_gas,
            policy_gas_limit,
        })
    }

    pub async fn execute_exact_input(
        &mut self,
        request: ExactInputSwapRequest,
    ) -> anyhow::Result<SwapExecutionOutcome> {
        self.execute_exact_input_instrumented(request, None).await
    }

    async fn execute_exact_input_instrumented(
        &mut self,
        request: ExactInputSwapRequest,
        enqueued_at: Option<Instant>,
    ) -> anyhow::Result<SwapExecutionOutcome> {
        self.last_terminal_receipt = None;
        request.validate()?;
        let protocol = request.route.protocol();
        if protocol == DexProtocol::CamelotV3 && !request.reconciliation_only {
            ensure!(
                self.camelot_submissions_enabled,
                "Camelot V3 broadcast is disabled until the direct-live allowance gate opens"
            );
        }
        let cost_route = DexRouteCostKey {
            pool: match request.route {
                SwapRoute::UniswapV3 { pool, .. } => DexPoolCostKey::UniswapV3(pool),
                SwapRoute::PancakeSwapV3 { pool, .. } => DexPoolCostKey::PancakeSwapV3(pool),
                SwapRoute::CamelotV3 { pool, .. } => DexPoolCostKey::CamelotV3(pool),
                SwapRoute::V4 { pool_key, .. } => DexPoolCostKey::UniswapV4(pool_key.pool_id()),
            },
            token_in: request.token_in,
        };
        if !request.reconciliation_only
            && matches!(
                protocol,
                DexProtocol::PancakeSwapV3 | DexProtocol::CamelotV3 | DexProtocol::UniswapV4
            )
        {
            ensure!(
                request.deadline_unix_seconds > unix_seconds()?,
                "deadline-bearing DEX request has expired"
            );
        }
        if request.reconciliation_only {
            // The exact calldata below is still rebuilt so execute_call can
            // prove it matches the durable journal identity. No allowance,
            // simulation, gas, nonce, signing, or broadcast work is allowed.
        } else if request.submission_policy == SwapSubmissionPolicy::Immediate {
            ensure!(
                !self.allowance_mutations_enabled,
                "immediate DEX submission requires startup-validated locked allowances"
            );
        } else {
            self.ensure_allowance(&request)
                .await
                .with_context(|| format!("{} input-token approval failed", protocol.label()))?;
        }

        let calldata = exact_input_calldata(&request, self.wallet.address())?;
        let call =
            WalletCall::validated_contract_call(request.route.router(), U256::ZERO, calldata)?;
        let operation_id = format!("{}.swap", request.operation_id);
        let receipt = self
            .execute_call(
                operation_id,
                protocol.label(),
                &call,
                ExecuteCallPolicy {
                    gas: GasLimitPolicy::for_swap(protocol, request.additional_gas),
                    quoted_gas: request.quoted_gas,
                    confirmation_timeout: request.confirmation_timeout,
                    submission_policy: request.submission_policy,
                    allow_new_submission: !request.reconciliation_only,
                },
                enqueued_at,
            )
            .await?;
        ensure!(
            receipt.status == 1,
            "{} transaction reverted",
            protocol.label()
        );
        let (token_in_received, token_in_sent) =
            wallet_transfer_totals(&receipt.logs, request.token_in, self.wallet.address())?;
        let (token_out_received, token_out_sent) =
            wallet_transfer_totals(&receipt.logs, request.token_out, self.wallet.address())?;
        let token_in_spent = token_in_sent
            .checked_sub(token_in_received)
            .context("DEX input-token receipt delta is not negative")?;
        let token_out_received = token_out_received
            .checked_sub(token_out_sent)
            .context("DEX output-token receipt delta is not positive")?;
        ensure!(
            token_in_spent == request.amount_in,
            "DEX receipt input-token delta differs from the submitted exact input"
        );
        ensure!(
            token_out_received >= request.amount_out_minimum,
            "DEX receipt output-token delta is below the submitted minimum"
        );
        let settlement = settlement_logs_for_route(&receipt, request.route)?;
        let l1_fee = self.accounted_l1_fee(receipt.l1_fee);
        if let Some(telemetry) = &self.pretrade_cost_telemetry {
            telemetry.publish_receipt(
                cost_route,
                receipt.gas_used,
                receipt.effective_gas_price,
                l1_fee,
                receipt.block_number,
            );
            let rpc = self.rpc.clone();
            let telemetry = telemetry.clone();
            let block_number = receipt.block_number;
            let gas_used = receipt.gas_used;
            let effective_gas_price = receipt.effective_gas_price;
            tokio::spawn(async move {
                let source_event_unix_us =
                    tokio::time::timeout(Duration::from_secs(5), rpc.block_timestamp(block_number))
                        .await
                        .ok()
                        .and_then(Result::ok)
                        .and_then(|seconds| seconds.checked_mul(1_000_000));
                if let Some(source_event_unix_us) = source_event_unix_us {
                    telemetry.publish_route_receipt_with_source(
                        cost_route,
                        gas_used,
                        effective_gas_price,
                        l1_fee,
                        Some(block_number),
                        Some(source_event_unix_us),
                        ReceiptCostTelemetrySource::LiveExecution,
                    );
                }
            });
        }
        Ok(SwapExecutionOutcome {
            protocol,
            transaction_hash: receipt.transaction_hash,
            block_number: receipt.block_number,
            gas_used: receipt.gas_used,
            effective_gas_price: receipt.effective_gas_price,
            l1_fee,
            token_in_spent,
            token_out_received,
            settlement_fee: settlement.fee,
            settlement_log: settlement.swap,
        })
    }

    fn classify_execution_error(
        &self,
        journal_operation_id: &str,
        reason: String,
    ) -> DexExecutionServiceError {
        let mut status = self
            .journal
            .operation(journal_operation_id)
            .map(|operation| &operation.status);
        for retry in 1..=3_u8 {
            let child_operation_id = format!("{journal_operation_id}.retry-{retry}");
            let Some(child) = self.journal.operation(&child_operation_id) else {
                break;
            };
            status = Some(&child.status);
        }
        match status {
            None
            | Some(JournalStatus::CancelledBeforeSigning)
            | Some(JournalStatus::RejectedBeforeBroadcast { .. }) => {
                DexExecutionServiceError::FailedBeforeSubmission { reason }
            }
            Some(JournalStatus::MinedReverted {
                transaction_hash, ..
            }) => match self
                .last_terminal_receipt
                .as_ref()
                .filter(|receipt| receipt.transaction_hash == *transaction_hash)
            {
                Some(receipt) => DexExecutionServiceError::Reverted {
                    transaction_hash: receipt.transaction_hash,
                    block_number: receipt.block_number,
                    gas_used: receipt.gas_used,
                    effective_gas_price: receipt.effective_gas_price,
                    l1_fee: self.accounted_l1_fee(receipt.l1_fee),
                    reason,
                },
                None => DexExecutionServiceError::OutcomeUnknown { reason },
            },
            Some(
                JournalStatus::IntentRecorded
                | JournalStatus::Signed { .. }
                | JournalStatus::Broadcast { .. }
                | JournalStatus::OutcomeUnknown { .. }
                | JournalStatus::MinedSuccess { .. },
            ) => DexExecutionServiceError::OutcomeUnknown { reason },
        }
    }

    async fn ensure_allowance(&mut self, request: &ExactInputSwapRequest) -> anyhow::Result<()> {
        match request.route {
            SwapRoute::UniswapV3 { router, .. } => {
                self.ensure_erc20_allowance(
                    &format!("{}.v3-router-approval", request.operation_id),
                    request.token_in,
                    router,
                    request.amount_in,
                )
                .await
            }
            SwapRoute::PancakeSwapV3 { router, .. } => {
                self.ensure_erc20_allowance(
                    &format!("{}.pancakeswap-v3-router-approval", request.operation_id),
                    request.token_in,
                    router,
                    request.amount_in,
                )
                .await
            }
            SwapRoute::CamelotV3 { router, .. } => {
                self.ensure_erc20_allowance(
                    &format!("{}.camelot-v3-router-approval", request.operation_id),
                    request.token_in,
                    router,
                    request.amount_in,
                )
                .await
            }
            SwapRoute::V4 { router, .. } => {
                self.ensure_erc20_allowance(
                    &format!("{}.permit2-erc20-approval", request.operation_id),
                    request.token_in,
                    PERMIT2_ADDRESS,
                    request.amount_in,
                )
                .await?;
                self.ensure_permit2_allowance(
                    &format!("{}.permit2-router-approval", request.operation_id),
                    request.token_in,
                    router,
                    request.amount_in,
                )
                .await
            }
        }
    }

    async fn ensure_erc20_allowance(
        &mut self,
        operation_id: &str,
        token: Address,
        spender: Address,
        required: U256,
    ) -> anyhow::Result<()> {
        let allowance_call = WalletCall::validated_contract_call(
            token,
            U256::ZERO,
            erc20_allowance_calldata(self.wallet.address(), spender),
        )?;
        let encoded = self
            .rpc
            .simulate_transaction(&allowance_call.rpc_call(self.wallet.address()))
            .await?;
        ensure!(
            encoded.len() == 32,
            "ERC-20 allowance result is not one ABI word"
        );
        if U256::from_be_slice(&encoded) >= required {
            return Ok(());
        }
        ensure!(
            self.allowance_mutations_enabled,
            "pre-locked ERC-20 allowance is insufficient"
        );

        let approval = WalletCall::erc20_approval(token, spender, self.allowance_grant(required))?;
        let receipt = self
            .execute_call(
                operation_id.to_owned(),
                "erc20_approval",
                &approval,
                ExecuteCallPolicy {
                    gas: GasLimitPolicy::fixed(RAILS_APPROVAL_DEFAULT_GAS_LIMIT),
                    quoted_gas: Some(RAILS_APPROVAL_DEFAULT_GAS_LIMIT),
                    confirmation_timeout: APPROVAL_CONFIRMATION_TIMEOUT,
                    submission_policy: SwapSubmissionPolicy::SimulateAndEstimate,
                    allow_new_submission: true,
                },
                None,
            )
            .await?;
        ensure!(receipt.status == 1, "ERC-20 approval reverted");
        Ok(())
    }

    async fn ensure_permit2_allowance(
        &mut self,
        operation_id: &str,
        token: Address,
        router: Address,
        required: U256,
    ) -> anyhow::Result<()> {
        let query = WalletCall::validated_contract_call(
            PERMIT2_ADDRESS,
            U256::ZERO,
            permit2_allowance(self.wallet.address(), token, router)?,
        )?;
        let encoded = self
            .rpc
            .simulate_transaction(&query.rpc_call(self.wallet.address()))
            .await?;
        let (allowance, expiration) = decode_permit2_allowance(&encoded)?;
        let now = unix_seconds()?;
        if allowance >= required
            && expiration >= now.saturating_add(PERMIT2_MIN_REMAINING_VALIDITY.as_secs())
        {
            return Ok(());
        }
        ensure!(
            self.allowance_mutations_enabled,
            "pre-locked Permit2 allowance is insufficient or expiring"
        );

        let expiration = now
            .checked_add(PERMIT2_APPROVAL_VALIDITY.as_secs())
            .context("Permit2 expiration overflow")?;
        let max_uint160 = (U256::from(1_u8) << 160) - U256::from(1_u8);
        let approval = WalletCall::validated_contract_call(
            PERMIT2_ADDRESS,
            U256::ZERO,
            permit2_approve(token, router, max_uint160, expiration)?,
        )?;
        let receipt = self
            .execute_call(
                operation_id.to_owned(),
                "permit2_approval",
                &approval,
                ExecuteCallPolicy {
                    gas: GasLimitPolicy::fixed(RAILS_PERMIT2_APPROVAL_GAS_LIMIT),
                    quoted_gas: Some(RAILS_PERMIT2_APPROVAL_GAS_LIMIT),
                    confirmation_timeout: APPROVAL_CONFIRMATION_TIMEOUT,
                    submission_policy: SwapSubmissionPolicy::SimulateAndEstimate,
                    allow_new_submission: true,
                },
                None,
            )
            .await?;
        ensure!(receipt.status == 1, "Permit2 approval reverted");
        Ok(())
    }

    async fn execute_call(
        &mut self,
        operation_id: String,
        purpose: &str,
        call: &WalletCall,
        policy: ExecuteCallPolicy,
        enqueued_at: Option<Instant>,
    ) -> anyhow::Result<TransactionReceipt> {
        let base_operation_id = operation_id;
        let mut operation_id = base_operation_id.clone();
        for retry in 0..=3_u8 {
            let Some(existing) = self.journal.operation(&operation_id) else {
                break;
            };
            ensure!(
                existing.intent.identity.chain_id == self.nonce_lane.chain_id()
                    && existing.intent.identity.wallet == self.wallet.address()
                    && existing.intent.purpose == purpose
                    && existing.intent.target == call.target()
                    && existing.intent.native_value == call.value()
                    && existing.intent.calldata_hash == keccak256(call.calldata()),
                "journaled DEX transaction does not match requested call"
            );
            return match existing.status {
                JournalStatus::MinedSuccess {
                    transaction_hash, ..
                } => {
                    let receipt = self
                        .rpc
                        .transaction_receipt(transaction_hash)
                        .await?
                        .context("journaled successful DEX receipt is unavailable")?;
                    self.last_terminal_receipt = Some(receipt.clone());
                    Ok(receipt)
                }
                JournalStatus::Broadcast { transaction_hash } => {
                    let receipt = self
                        .rpc
                        .transaction_receipt(transaction_hash)
                        .await?
                        .context("journaled broadcast DEX receipt is unavailable")?;
                    self.nonce_lane
                        .record_receipt(&mut self.journal, receipt.clone())?;
                    self.last_terminal_receipt = Some(receipt.clone());
                    Ok(receipt)
                }
                JournalStatus::MinedReverted {
                    transaction_hash, ..
                } => {
                    let receipt = self
                        .rpc
                        .transaction_receipt(transaction_hash)
                        .await?
                        .context("journaled reverted DEX receipt is unavailable")?;
                    self.last_terminal_receipt = Some(receipt.clone());
                    Ok(receipt)
                }
                JournalStatus::CancelledBeforeSigning => {
                    bail!("journaled DEX transaction was cancelled before signing")
                }
                JournalStatus::RejectedBeforeBroadcast { .. } if retry < 3 => {
                    operation_id = format!("{base_operation_id}.retry-{}", retry + 1);
                    continue;
                }
                JournalStatus::RejectedBeforeBroadcast { .. } => {
                    bail!("journaled DEX transaction exhausted pre-broadcast retries")
                }
                _ => bail!("journaled DEX transaction requires recovery"),
            };
        }
        ensure!(
            policy.allow_new_submission,
            "reconciliation-only DEX request has no matching journaled transaction"
        );
        ensure!(self.nonce_lane.ready(), "DEX nonce lane is not ready");
        let rpc_call = call.rpc_call(self.wallet.address());
        let preflight_started = Instant::now();
        let gas_limit_result: anyhow::Result<(u64, Option<u64>)> = match policy.submission_policy {
            SwapSubmissionPolicy::SimulateAndEstimate => {
                async {
                    self.rpc
                        .simulate_transaction(&rpc_call)
                        .await
                        .context("DEX preflight simulation reverted")?;
                    let estimated_gas = self.rpc.estimate_gas(&rpc_call).await?;
                    Ok((
                        policy.gas.resolve(policy.quoted_gas, estimated_gas)?,
                        Some(estimated_gas),
                    ))
                }
                .await
            }
            SwapSubmissionPolicy::Immediate => policy
                .gas
                .resolve_without_estimate(policy.quoted_gas)
                .map(|gas_limit| (gas_limit, None)),
        };
        self.emit_latency_stage(
            &operation_id,
            "preflight",
            preflight_started,
            if gas_limit_result.is_ok() {
                "success"
            } else {
                "failed"
            },
        );
        let (gas_limit, estimated_gas) = gas_limit_result?;

        let gas_price_started = Instant::now();
        let (gas_price, gas_price_source) = self
            .gas_price_for_submission(policy.submission_policy)
            .await?;
        let (max_fee_per_gas, max_priority_fee_per_gas) = self.transaction_fees(gas_price)?;
        let fee_parameters = WalletTransactionParameters {
            chain_id: self.nonce_lane.chain_id(),
            nonce: 0,
            gas_limit,
            max_fee_per_gas,
            max_priority_fee_per_gas,
        };
        // Native gas funding is an operator-maintained invariant for immediate
        // live swaps. The background execution-owner refresher keeps the
        // two-second gas-price cache out of admission and the latency-sensitive
        // path. Startup/manual simulated writes retain their direct RPC guard.
        if policy.submission_policy == SwapSubmissionPolicy::SimulateAndEstimate {
            let maximum_cost = call.maximum_native_cost(fee_parameters)?;
            ensure!(
                self.rpc.native_balance(self.wallet.address()).await? >= maximum_cost,
                "wallet native balance cannot cover maximum DEX gas"
            );
        }
        self.emit_latency_stage(
            &operation_id,
            "gas_price_cache",
            gas_price_started,
            gas_price_source,
        );

        let nonce_and_sign_started = Instant::now();
        let mut nonce_guard = acquire_process_nonce_lock(
            self.nonce_lane.chain_id(),
            self.wallet.address(),
            self.nonce_lane
                .next_nonce()
                .context("ready DEX nonce lane has no nonce")?,
        )
        .await?;
        let identity = self.nonce_lane.reserve_with_nonce(
            &mut self.journal,
            operation_id.clone(),
            purpose,
            call,
            nonce_guard.nonce(),
        )?;
        let signed = match self.wallet.sign_call(
            call,
            WalletTransactionParameters {
                nonce: identity.nonce,
                ..fee_parameters
            },
        ) {
            Ok(signed) => signed,
            Err(error) => {
                self.nonce_lane.cancel_before_signing(&mut self.journal)?;
                return Err(error);
            }
        };
        self.nonce_lane.record_signed(&mut self.journal, &signed)?;
        self.emit_latency_stage(
            &operation_id,
            "nonce_reserve_sign_journal",
            nonce_and_sign_started,
            "success",
        );
        if let Some(enqueued_at) = enqueued_at {
            self.emit_latency_stage(
                &operation_id,
                "enqueue_to_first_write",
                enqueued_at,
                "success",
            );
        }

        let broadcast_started = Instant::now();
        let broadcast_result = tokio::time::timeout(
            PROCESS_NONCE_LOCK_TTL,
            broadcast_signed_transaction(&self.rpc, &signed),
        )
        .await;
        self.emit_latency_stage(
            &operation_id,
            "broadcast_rpc",
            broadcast_started,
            if matches!(&broadcast_result, Ok(Ok(_))) {
                "success"
            } else {
                "failed"
            },
        );
        let submitted = match broadcast_result {
            Ok(Ok(hash)) => hash,
            Ok(Err(error)) => {
                if is_definitive_prebroadcast_rejection(&error) {
                    self.nonce_lane
                        .record_rejected_before_broadcast(&mut self.journal, signed.hash)?;
                    tracing::warn!(
                        operation_id,
                        transaction_hash = %signed.hash,
                        nonce = signed.nonce,
                        error = %error,
                        "DEX transaction was definitively rejected before broadcast and its nonce was released"
                    );
                } else {
                    self.nonce_lane.record_unknown_outcome(
                        &mut self.journal,
                        UnknownOutcomeReason::BroadcastTransport,
                    )?;
                    tracing::error!(
                        operation_id,
                        transaction_hash = %signed.hash,
                        nonce = signed.nonce,
                        error = %error,
                        "DEX transaction broadcast outcome is unknown and was journaled"
                    );
                }
                return Err(error);
            }
            Err(_elapsed) => {
                self.nonce_lane.record_unknown_outcome(
                    &mut self.journal,
                    UnknownOutcomeReason::BroadcastTransport,
                )?;
                bail!("DEX transaction broadcast timed out while holding nonce lock");
            }
        };
        self.nonce_lane
            .record_broadcast(&mut self.journal, submitted)?;
        nonce_guard.advance_after_broadcast(identity.nonce)?;
        drop(nonce_guard);
        tracing::info!(
            operation_id,
            transaction_hash = %submitted,
            nonce = signed.nonce,
            gas_limit,
            estimated_gas,
            quoted_gas = policy.quoted_gas,
            additional_gas = policy.gas.additional,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            "DEX transaction broadcast and journaled"
        );

        let confirmation_started = Instant::now();
        let receipt_result = wait_for_receipt(
            &self.rpc,
            self.receipt_heads.as_mut(),
            submitted,
            policy.confirmation_timeout,
        )
        .await;
        self.emit_latency_stage(
            &operation_id,
            "confirmation_rpc",
            confirmation_started,
            if receipt_result.is_ok() {
                "success"
            } else {
                "failed"
            },
        );
        let receipt = match receipt_result {
            Ok(receipt) => receipt,
            Err(error) => {
                tracing::error!(
                    operation_id,
                    transaction_hash = %submitted,
                    error = %error,
                    "DEX transaction confirmation timed out after broadcast; nonce lock is already released"
                );
                return Err(error);
            }
        };
        let receipt_journal_started = Instant::now();
        self.nonce_lane
            .record_receipt(&mut self.journal, receipt.clone())?;
        self.last_terminal_receipt = Some(receipt.clone());
        self.emit_latency_stage(
            &operation_id,
            "receipt_journal",
            receipt_journal_started,
            "success",
        );
        if receipt.status == 1 {
            tracing::info!(
                operation_id,
                transaction_hash = %receipt.transaction_hash,
                block_number = receipt.block_number,
                gas_used = receipt.gas_used,
                effective_gas_price = receipt.effective_gas_price,
                "DEX transaction mined successfully and was journaled"
            );
        } else {
            tracing::warn!(
                operation_id,
                transaction_hash = %receipt.transaction_hash,
                block_number = receipt.block_number,
                gas_used = receipt.gas_used,
                effective_gas_price = receipt.effective_gas_price,
                "DEX transaction reverted and was journaled"
            );
        }
        Ok(receipt)
    }

    fn allowance_grant(&self, required: U256) -> U256 {
        allowance_grant_for_policy(&self.gas_policy, required)
    }

    fn accounted_l1_fee(&self, receipt_l1_fee: u128) -> u128 {
        match self.gas_policy {
            CompiledNetworkGasPolicy::WorldChainV12 {
                includes_l1_fee: true,
                ..
            } => receipt_l1_fee,
            CompiledNetworkGasPolicy::ArbitrumOne {
                includes_l1_fee: false,
                ..
            }
            | CompiledNetworkGasPolicy::ReadOnly => 0,
            _ => receipt_l1_fee,
        }
    }

    fn transaction_fees(&self, gas_price: u128) -> anyhow::Result<(u128, u128)> {
        transaction_fees_for_policy(&self.gas_policy, gas_price)
    }

    async fn gas_price_for_submission(
        &mut self,
        submission_policy: SwapSubmissionPolicy,
    ) -> anyhow::Result<(u128, &'static str)> {
        if let Some(sample) = self.gas_price
            && sample.is_fresh()
        {
            return Ok((sample.wei, sample.source.label()));
        }
        if submission_policy == SwapSubmissionPolicy::SimulateAndEstimate {
            self.refresh_gas_price().await?;
            let sample = self
                .gas_price
                .expect("gas-price refresh always publishes a sample");
            return Ok((sample.wei, sample.source.label()));
        }
        match self.gas_policy {
            CompiledNetworkGasPolicy::WorldChainV12 {
                fallback_gas_price_wei,
                ..
            } => {
                tracing::warn!(
                    fallback_gas_price_wei,
                    gas_price_cache_ttl_ms = GAS_PRICE_CACHE_TTL.as_millis(),
                    "background gas-price cache is unavailable or stale; using the Rails fallback"
                );
                Ok((fallback_gas_price_wei, "stale_rails_fallback"))
            }
            CompiledNetworkGasPolicy::ArbitrumOne {
                requires_fresh_rpc_gas_price: true,
                ..
            } => anyhow::bail!(
                "fresh Arbitrum eth_gasPrice sample is unavailable; transaction fails closed"
            ),
            _ => anyhow::bail!("network has no executable gas-price policy"),
        }
    }

    async fn refresh_gas_price(&mut self) -> anyhow::Result<()> {
        let previous_source = self.gas_price.map(|sample| sample.source);
        let rpc_sample = self.rpc.gas_price().await;
        let (wei, source) = match rpc_sample {
            Ok(gas_price) if gas_price > 0 => {
                if previous_source == Some(GasPriceSource::RailsFallback) {
                    tracing::info!(
                        gas_price_wei = gas_price,
                        "background eth_gasPrice refresh recovered"
                    );
                }
                (gas_price, GasPriceSource::Rpc)
            }
            Ok(_)
                if matches!(
                    self.gas_policy,
                    CompiledNetworkGasPolicy::WorldChainV12 { .. }
                ) =>
            {
                if previous_source != Some(GasPriceSource::RailsFallback) {
                    tracing::warn!(
                        fallback_gas_price_wei = RAILS_FALLBACK_GAS_PRICE_WEI,
                        "background eth_gasPrice refresh returned zero; caching the Rails fallback"
                    );
                }
                (RAILS_FALLBACK_GAS_PRICE_WEI, GasPriceSource::RailsFallback)
            }
            Err(_)
                if matches!(
                    self.gas_policy,
                    CompiledNetworkGasPolicy::WorldChainV12 { .. }
                ) =>
            {
                if previous_source != Some(GasPriceSource::RailsFallback) {
                    tracing::warn!(
                        fallback_gas_price_wei = RAILS_FALLBACK_GAS_PRICE_WEI,
                        "background eth_gasPrice refresh failed; caching the Rails fallback"
                    );
                }
                (RAILS_FALLBACK_GAS_PRICE_WEI, GasPriceSource::RailsFallback)
            }
            Ok(_) => anyhow::bail!("Arbitrum eth_gasPrice returned zero; no fallback is permitted"),
            Err(error) => {
                return Err(error)
                    .context("Arbitrum eth_gasPrice refresh failed; no fallback is permitted");
            }
        };
        self.gas_price = Some(GasPriceSample {
            captured_at: Instant::now(),
            wei,
            source,
        });
        if let Some(telemetry) = &self.pretrade_cost_telemetry {
            let (maximum_fee_per_gas_wei, _) = transaction_fees_for_policy(&self.gas_policy, wei)?;
            let includes_l1_fee = matches!(
                self.gas_policy,
                CompiledNetworkGasPolicy::WorldChainV12 {
                    includes_l1_fee: true,
                    ..
                }
            );
            telemetry.publish_gas_price(
                wei,
                maximum_fee_per_gas_wei,
                match source {
                    GasPriceSource::Rpc => GasPriceTelemetrySource::Rpc,
                    GasPriceSource::RailsFallback => GasPriceTelemetrySource::RailsFallback,
                },
                includes_l1_fee,
            );
        }
        Ok(())
    }
}

fn allowance_grant_for_policy(policy: &CompiledNetworkGasPolicy, required: U256) -> U256 {
    match policy {
        CompiledNetworkGasPolicy::WorldChainV12 { .. } => U256::MAX,
        CompiledNetworkGasPolicy::ArbitrumOne { .. } => required,
        CompiledNetworkGasPolicy::ReadOnly => U256::ZERO,
    }
}

fn transaction_fees_for_policy(
    policy: &CompiledNetworkGasPolicy,
    gas_price: u128,
) -> anyhow::Result<(u128, u128)> {
    ensure!(gas_price > 0, "DEX gas price is zero");
    match policy {
        CompiledNetworkGasPolicy::WorldChainV12 { .. } => {
            let maximum = gas_price
                .checked_add(RAILS_PRIORITY_FEE_WEI)
                .context("DEX maximum fee overflow")?;
            Ok((maximum, RAILS_PRIORITY_FEE_WEI.min(maximum)))
        }
        CompiledNetworkGasPolicy::ArbitrumOne {
            max_priority_fee_per_gas_wei,
            max_fee_headroom_bps,
            ..
        } => {
            ensure!(
                *max_priority_fee_per_gas_wei == 0,
                "reviewed Arbitrum sequencer policy does not permit a priority tip"
            );
            ensure!(
                (10_000..=15_000).contains(max_fee_headroom_bps),
                "Arbitrum maximum-fee headroom is outside the reviewed bounds"
            );
            let maximum = gas_price
                .checked_mul(u128::from(*max_fee_headroom_bps))
                .and_then(|scaled| scaled.checked_add(9_999))
                .context("Arbitrum maximum fee headroom overflow")?
                / 10_000;
            Ok((maximum, 0))
        }
        CompiledNetworkGasPolicy::ReadOnly => {
            anyhow::bail!("read-only network cannot construct transaction fees")
        }
    }
}

fn is_definitive_prebroadcast_rejection(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.starts_with("json-rpc error")
        && (message.contains("max fee per gas less than block base fee")
            || message.contains("fee cap less than block base fee"))
}

fn exact_input_calldata(
    request: &ExactInputSwapRequest,
    recipient: Address,
) -> anyhow::Result<Vec<u8>> {
    match request.route {
        SwapRoute::UniswapV3 { fee_pips, .. } => v3_exact_input(
            request.token_in,
            request.token_out,
            fee_pips,
            recipient,
            request.amount_in,
            request.amount_out_minimum,
        ),
        SwapRoute::PancakeSwapV3 { fee_pips, .. } => pancake_v3_exact_input_single(
            request.token_in,
            request.token_out,
            fee_pips,
            recipient,
            request.deadline_unix_seconds,
            request.amount_in,
            request.amount_out_minimum,
        ),
        SwapRoute::CamelotV3 { .. } => camelot_v3_exact_input_single(
            request.token_in,
            request.token_out,
            recipient,
            request.deadline_unix_seconds,
            request.amount_in,
            request.amount_out_minimum,
        ),
        SwapRoute::V4 { pool_key, .. } => v4_exact_input_single(
            pool_key,
            request.token_in == pool_key.currency0,
            request.amount_in,
            request.amount_out_minimum,
            request.token_in,
            request.token_out,
            request.deadline_unix_seconds,
        ),
    }
}

#[cfg(test)]
fn settlement_log_for_route(
    receipt: &TransactionReceipt,
    route: SwapRoute,
) -> anyhow::Result<Option<ChainLog>> {
    Ok(settlement_logs_for_route(receipt, route)?.swap)
}

fn settlement_logs_for_route(
    receipt: &TransactionReceipt,
    route: SwapRoute,
) -> anyhow::Result<ReceiptSettlementLogs> {
    let expected = match route {
        SwapRoute::UniswapV3 { pool, .. } => PoolLocator::V3(pool),
        SwapRoute::PancakeSwapV3 { pool, .. } => PoolLocator::PancakeV3(pool),
        SwapRoute::CamelotV3 { pool, .. } => PoolLocator::CamelotV3(pool),
        SwapRoute::V4 { pool_key, .. } => PoolLocator::V4(pool_key.pool_id()),
    };
    let mut matched_fee = None;
    let mut matched_swap = None;
    for receipt_log in &receipt.logs {
        if let Some(position) = receipt_log.position {
            ensure!(
                position.transaction_hash == receipt.transaction_hash,
                "DEX receipt log belongs to another transaction"
            );
        }
        if matches!(
            expected,
            PoolLocator::V3(pool)
                | PoolLocator::PancakeV3(pool)
                | PoolLocator::CamelotV3(pool)
                if receipt_log.address != pool
        ) {
            continue;
        }
        let Some(kind) = receipt_settlement_kind(receipt_log, expected)? else {
            continue;
        };
        match kind {
            ReceiptSettlementKind::Fee => {
                ensure!(
                    matched_fee.is_none(),
                    "DEX receipt contains duplicate route Fee events"
                );
                let position = receipt_log
                    .position
                    .context("Camelot receipt Fee has no canonical position")?;
                matched_fee = Some(CamelotFeeReceiptProof {
                    pool: receipt_log.address,
                    zero_for_one: u16::from_be_bytes([receipt_log.data[30], receipt_log.data[31]]),
                    one_for_zero: u16::from_be_bytes([receipt_log.data[62], receipt_log.data[63]]),
                    block_number: position.block_number,
                    block_hash: position.block_hash,
                    transaction_index: position.transaction_index,
                    log_index: position.log_index,
                });
            }
            ReceiptSettlementKind::Swap => {
                let Ok(log) = receipt_log.chain_log() else {
                    continue;
                };
                ensure!(
                    matched_swap.is_none(),
                    "DEX receipt contains duplicate route Swap events"
                );
                matched_swap = Some(log);
            }
        }
    }
    if matches!(expected, PoolLocator::CamelotV3(_)) {
        let (Some(fee), Some(swap)) = (matched_fee.take(), matched_swap.take()) else {
            return Ok(ReceiptSettlementLogs::default());
        };
        ensure!(
            fee.pool == swap.address
                && fee.block_number == swap.block_number
                && fee.block_hash == swap.block_hash
                && fee.transaction_index == swap.transaction_index
                && fee.log_index < swap.log_index,
            "Camelot receipt Fee is not positionally before Swap in one transaction"
        );
        return Ok(ReceiptSettlementLogs {
            fee: Some(fee),
            swap: Some(swap),
        });
    }
    ensure!(
        matched_fee.is_none(),
        "static-fee route receipt contains a Camelot Fee event"
    );
    Ok(ReceiptSettlementLogs {
        fee: None,
        swap: matched_swap,
    })
}

fn receipt_settlement_kind(
    log: &ReceiptLog,
    expected: PoolLocator,
) -> anyhow::Result<Option<ReceiptSettlementKind>> {
    let Some(signature) = log.topics.first().copied() else {
        return Ok(None);
    };
    ensure!(
        !matches!(expected, PoolLocator::V3(_))
            || (signature != pancake_v3_swap_topic() && signature != camelot_fee_topic()),
        "receipt event topic does not match its routed Uniswap V3 provider"
    );
    ensure!(
        !matches!(expected, PoolLocator::PancakeV3(_))
            || (signature != v3_swap_topic() && signature != camelot_fee_topic()),
        "receipt event topic does not match its routed Pancake V3 provider"
    );
    ensure!(
        !matches!(expected, PoolLocator::CamelotV3(_)) || signature != pancake_v3_swap_topic(),
        "receipt event topic does not match its routed Camelot V3 provider"
    );
    match expected {
        PoolLocator::V3(_) if signature == v3_swap_topic() => {
            ensure!(log.topics.len() == 3, "invalid V3 Swap topic count");
            ensure!(log.data.len() == 5 * 32, "invalid V3 Swap data length");
            Ok(Some(ReceiptSettlementKind::Swap))
        }
        PoolLocator::PancakeV3(_) if signature == pancake_v3_swap_topic() => {
            ensure!(log.topics.len() == 3, "invalid Pancake V3 Swap topic count");
            ensure!(
                log.data.len() == 7 * 32,
                "invalid Pancake V3 Swap data length"
            );
            ensure!(
                log.data[5 * 32..5 * 32 + 16] == [0_u8; 16]
                    && log.data[6 * 32..6 * 32 + 16] == [0_u8; 16],
                "Pancake V3 protocol fee does not fit uint128"
            );
            Ok(Some(ReceiptSettlementKind::Swap))
        }
        PoolLocator::CamelotV3(_) if signature == camelot_fee_topic() => {
            ensure!(log.topics.len() == 1, "invalid Camelot Fee topic count");
            ensure!(log.data.len() == 2 * 32, "invalid Camelot Fee data length");
            ensure!(
                log.data[..30] == [0_u8; 30] && log.data[32..62] == [0_u8; 30],
                "Camelot Fee does not fit uint16"
            );
            Ok(Some(ReceiptSettlementKind::Fee))
        }
        PoolLocator::CamelotV3(_) if signature == v3_swap_topic() => {
            ensure!(log.topics.len() == 3, "invalid Camelot Swap topic count");
            ensure!(log.data.len() == 5 * 32, "invalid Camelot Swap data length");
            Ok(Some(ReceiptSettlementKind::Swap))
        }
        PoolLocator::V4(pool_id) if signature == v4_swap_topic() => {
            ensure!(log.topics.len() == 3, "invalid V4 Swap topic count");
            ensure!(log.data.len() == 6 * 32, "invalid V4 Swap data length");
            Ok((log.topics[1] == pool_id).then_some(ReceiptSettlementKind::Swap))
        }
        PoolLocator::V3(_)
        | PoolLocator::PancakeV3(_)
        | PoolLocator::CamelotV3(_)
        | PoolLocator::V4(_) => Ok(None),
    }
}

fn wallet_transfer_totals(
    logs: &[ReceiptLog],
    token: Address,
    wallet: Address,
) -> anyhow::Result<(U256, U256)> {
    let transfer_topic = keccak256("Transfer(address,address,uint256)");
    let mut received = U256::ZERO;
    let mut sent = U256::ZERO;
    for log in logs
        .iter()
        .filter(|log| log.address == token && log.topics.first() == Some(&transfer_topic))
    {
        ensure!(
            log.topics.len() == 3,
            "ERC-20 Transfer log has wrong topics"
        );
        ensure!(
            log.data.len() == 32,
            "ERC-20 Transfer log amount is not one word"
        );
        let from = Address::from_slice(&log.topics[1].as_slice()[12..]);
        let to = Address::from_slice(&log.topics[2].as_slice()[12..]);
        let amount = U256::from_be_slice(&log.data);
        if to == wallet {
            received = received
                .checked_add(amount)
                .context("received ERC-20 transfer sum overflow")?;
        }
        if from == wallet {
            sent = sent
                .checked_add(amount)
                .context("sent ERC-20 transfer sum overflow")?;
        }
    }
    Ok((received, sent))
}

struct WorkItem {
    request: ExactInputSwapRequest,
    enqueued_at: Instant,
    queue_depth_before_enqueue: usize,
    response: oneshot::Sender<Result<SwapExecutionOutcome, DexExecutionServiceError>>,
}

struct CapitalWorkItem {
    request: EvmExecutionRequest,
    reconciliation_only: bool,
    enqueued_at: Instant,
    response: oneshot::Sender<Result<TransactionReceipt, DexExecutionServiceError>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvmExecutionRequest {
    pub operation_id: String,
    pub purpose: String,
    pub call: WalletCall,
    pub confirmation_timeout: Duration,
}

impl EvmExecutionRequest {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            !self.operation_id.is_empty()
                && self.operation_id.len() <= 120
                && self.operation_id.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':')
                }),
            "EVM execution operation id is invalid"
        );
        ensure!(
            !self.purpose.is_empty()
                && self.purpose.len() <= 64
                && self
                    .purpose
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
            "EVM execution purpose is invalid"
        );
        ensure!(
            (Duration::from_secs(5)..=Duration::from_secs(300))
                .contains(&self.confirmation_timeout),
            "EVM execution confirmation timeout is outside 5..=300 seconds"
        );
        Ok(())
    }
}

#[derive(Clone)]
pub struct EvmExecutionOwnerHandle {
    sender: mpsc::Sender<CapitalWorkItem>,
    wallet_address: Address,
    chain_id: u64,
}

impl EvmExecutionOwnerHandle {
    pub fn wallet_address(&self) -> Address {
        self.wallet_address
    }

    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    pub async fn execute(
        &self,
        request: EvmExecutionRequest,
    ) -> Result<TransactionReceipt, DexExecutionServiceError> {
        self.dispatch(request, false).await
    }

    /// Reconciles one exact, already-journaled capital transaction. The
    /// execution owner may read its canonical receipt, but it must not reserve
    /// a nonce, sign, or broadcast when the operation is absent or mismatched.
    pub async fn reconcile(
        &self,
        request: EvmExecutionRequest,
    ) -> Result<TransactionReceipt, DexExecutionServiceError> {
        self.dispatch(request, true).await
    }

    async fn dispatch(
        &self,
        request: EvmExecutionRequest,
        reconciliation_only: bool,
    ) -> Result<TransactionReceipt, DexExecutionServiceError> {
        if let Err(error) = request.validate() {
            return Err(DexExecutionServiceError::FailedBeforeSubmission {
                reason: format!("{error:#}"),
            });
        }
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(CapitalWorkItem {
                request,
                reconciliation_only,
                enqueued_at: Instant::now(),
                response,
            })
            .await
            .map_err(|_| DexExecutionServiceError::OutcomeUnknown {
                reason: "EVM execution owner stopped".to_owned(),
            })?;
        receiver
            .await
            .map_err(|_| DexExecutionServiceError::OutcomeUnknown {
                reason: "EVM execution owner dropped its response".to_owned(),
            })?
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DexExecutionServiceError {
    FailedBeforeSubmission {
        reason: String,
    },
    Reverted {
        transaction_hash: B256,
        block_number: u64,
        gas_used: u64,
        effective_gas_price: u128,
        l1_fee: u128,
        reason: String,
    },
    OutcomeUnknown {
        reason: String,
    },
}

impl std::fmt::Display for DexExecutionServiceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FailedBeforeSubmission { reason } => {
                write!(formatter, "DEX rejected before submission: {reason}")
            }
            Self::Reverted {
                transaction_hash,
                reason,
                ..
            } => write!(
                formatter,
                "DEX transaction {transaction_hash:#x} reverted: {reason}"
            ),
            Self::OutcomeUnknown { reason } => write!(formatter, "DEX outcome unknown: {reason}"),
        }
    }
}

impl std::error::Error for DexExecutionServiceError {}

/// One bounded, single-owner execution lane running on a dedicated OS thread.
/// The thread owns the signer, nonce lane, RPC client and durable journal.
pub struct DexExecutionService {
    sender: Option<mpsc::Sender<WorkItem>>,
    capital_sender: Option<mpsc::Sender<CapitalWorkItem>>,
    shutdown_sender: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
    wallet_address: Address,
    chain_id: u64,
}

impl DexExecutionService {
    pub fn spawn(executor: DexExecutor, capacity: usize) -> anyhow::Result<Self> {
        ensure!(capacity > 0, "DEX execution channel capacity is zero");
        let wallet_address = executor.wallet_address();
        let chain_id = executor.chain_id();
        let (sender, mut receiver) = mpsc::channel::<WorkItem>(capacity);
        let (capital_sender, mut capital_receiver) = mpsc::channel::<CapitalWorkItem>(1);
        let (shutdown_sender, mut shutdown_receiver) = oneshot::channel();
        let thread = std::thread::Builder::new()
            .name("dex-executor".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        tracing::error!(error = %error, "failed to build DEX executor runtime");
                        return;
                    }
                };
                runtime.block_on(async move {
                    let mut executor = executor;
                    if executor.refresh_gas_price().await.is_err() {
                        tracing::warn!(
                            chain_id,
                            "initial DEX gas-price refresh failed; executable policy remains fail-closed"
                        );
                    }
                    let mut gas_price_refresh =
                        tokio::time::interval(GAS_PRICE_REFRESH_INTERVAL);
                    gas_price_refresh
                        .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    gas_price_refresh.reset();
                    let mut trade_open = true;
                    let mut capital_open = true;
                    while trade_open || capital_open {
                        tokio::select! {
                            biased;
                            _ = &mut shutdown_receiver => break,
                            _ = gas_price_refresh.tick() => {
                                if executor.refresh_gas_price().await.is_err() {
                                    tracing::warn!(
                                        chain_id,
                                        "background DEX gas-price refresh failed; executable policy remains fail-closed"
                                    );
                                }
                            }
                            work = receiver.recv(), if trade_open => {
                                let Some(work) = work else {
                                    trade_open = false;
                                    continue;
                                };
                                let operation_id = work.request.operation_id.clone();
                                let journal_operation_id = format!("{operation_id}.swap");
                                executor.emit_latency_stage(
                                    &operation_id,
                                    "worker_queue",
                                    work.enqueued_at,
                                    "success",
                                );
                                if let Some(telemetry) = &executor.latency_telemetry {
                                    telemetry.emit_queue_stage(
                                        "dex",
                                        &operation_id,
                                        "worker_queue_depth",
                                        duration_us(work.enqueued_at.elapsed()),
                                        work.queue_depth_before_enqueue,
                                        "success",
                                    );
                                }
                                let execution_started = Instant::now();
                                let result = executor
                                    .execute_exact_input_instrumented(
                                        work.request,
                                        Some(work.enqueued_at),
                                    )
                                    .await
                                    .map_err(|error| {
                                        executor.classify_execution_error(
                                            &journal_operation_id,
                                            format!("{error:#}"),
                                        )
                                    });
                                executor.emit_latency_stage(
                                    &operation_id,
                                    "worker_total",
                                    execution_started,
                                    if result.is_ok() { "success" } else { "failed" },
                                );
                                if let Err(error) = &result {
                                    match error {
                                        DexExecutionServiceError::Reverted { .. } => {
                                            tracing::warn!(
                                                operation_id,
                                                error = %error,
                                                "DEX execution request reached a known reverted receipt"
                                            );
                                        }
                                        DexExecutionServiceError::FailedBeforeSubmission {
                                            ..
                                        } => {
                                            tracing::warn!(
                                                operation_id,
                                                error = %error,
                                                "DEX execution request failed before submission"
                                            );
                                        }
                                        DexExecutionServiceError::OutcomeUnknown { .. } => {
                                            tracing::error!(
                                                operation_id,
                                                error = %error,
                                                "DEX execution outcome is unknown; inspect transaction journal before recovery"
                                            );
                                        }
                                    }
                                }
                                if work.response.send(result).is_err() {
                                    tracing::warn!(
                                        operation_id,
                                        "DEX execution caller dropped its response"
                                    );
                                }
                            }
                            work = capital_receiver.recv(), if capital_open => {
                                let Some(work) = work else {
                                    capital_open = false;
                                    continue;
                                };
                                let operation_id = work.request.operation_id.clone();
                                executor.emit_latency_stage(
                                    &operation_id,
                                    "capital_worker_queue",
                                    work.enqueued_at,
                                    "success",
                                );
                                let result = executor
                                    .execute_call(
                                        work.request.operation_id.clone(),
                                        &work.request.purpose,
                                        &work.request.call,
                                        ExecuteCallPolicy {
                                            gas: GasLimitPolicy::fixed(CAPITAL_TRANSFER_GAS_LIMIT),
                                            quoted_gas: None,
                                            confirmation_timeout: work.request.confirmation_timeout,
                                            submission_policy:
                                                SwapSubmissionPolicy::SimulateAndEstimate,
                                            allow_new_submission: !work.reconciliation_only,
                                        },
                                        Some(work.enqueued_at),
                                    )
                                    .await
                                    .and_then(|receipt| {
                                        ensure!(
                                            receipt.status == 1,
                                            "EVM capital transaction reverted"
                                        );
                                        Ok(receipt)
                                    })
                                    .map_err(|error| {
                                        executor.classify_execution_error(
                                            &operation_id,
                                            format!("{error:#}"),
                                        )
                                    });
                                if work.response.send(result).is_err() {
                                    tracing::warn!(
                                        operation_id,
                                        "EVM capital execution caller dropped its response"
                                    );
                                }
                            }
                        }
                    }
                });
            })
            .context("failed to spawn DEX executor thread")?;
        Ok(Self {
            sender: Some(sender),
            capital_sender: Some(capital_sender),
            shutdown_sender: Some(shutdown_sender),
            thread: Some(thread),
            wallet_address,
            chain_id,
        })
    }

    pub fn wallet_address(&self) -> Address {
        self.wallet_address
    }

    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    pub fn evm_execution_owner(&self) -> EvmExecutionOwnerHandle {
        EvmExecutionOwnerHandle {
            sender: self
                .capital_sender
                .as_ref()
                .expect("live DEX service still owns its capital sender")
                .clone(),
            wallet_address: self.wallet_address,
            chain_id: self.chain_id,
        }
    }

    pub async fn execute(
        &self,
        request: ExactInputSwapRequest,
    ) -> Result<SwapExecutionOutcome, DexExecutionServiceError> {
        let sender =
            self.sender
                .as_ref()
                .ok_or_else(|| DexExecutionServiceError::OutcomeUnknown {
                    reason: "DEX execution service is shut down".to_owned(),
                })?;
        let (response, receiver) = oneshot::channel();
        let queue_depth_before_enqueue = sender.max_capacity() - sender.capacity();
        sender
            .send(WorkItem {
                request,
                enqueued_at: Instant::now(),
                queue_depth_before_enqueue,
                response,
            })
            .await
            .map_err(|_| DexExecutionServiceError::OutcomeUnknown {
                reason: "DEX executor thread stopped".to_owned(),
            })?;
        receiver
            .await
            .map_err(|_| DexExecutionServiceError::OutcomeUnknown {
                reason: "DEX executor dropped its response".to_owned(),
            })?
    }
}

fn duration_us(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

impl Drop for DexExecutionService {
    fn drop(&mut self) {
        self.sender.take();
        self.capital_sender.take();
        if let Some(shutdown) = self.shutdown_sender.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take()
            && let Err(payload) = thread.join()
        {
            tracing::error!(?payload, "DEX executor thread panicked during shutdown");
        }
    }
}

fn erc20_allowance_calldata(owner: Address, spender: Address) -> Vec<u8> {
    let mut data = Vec::with_capacity(68);
    data.extend_from_slice(&[0xdd, 0x62, 0xed, 0x3e]);
    data.extend_from_slice(&[0_u8; 12]);
    data.extend_from_slice(owner.as_slice());
    data.extend_from_slice(&[0_u8; 12]);
    data.extend_from_slice(spender.as_slice());
    data
}

async fn wait_for_receipt(
    rpc: &JsonRpcClient,
    mut head_receiver: Option<&mut watch::Receiver<CanonicalBlock>>,
    transaction_hash: B256,
    timeout: Duration,
) -> anyhow::Result<TransactionReceipt> {
    let started_at = tokio::time::Instant::now();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(receipt) = rpc.transaction_receipt(transaction_hash).await? {
            return Ok(receipt);
        }
        let now = tokio::time::Instant::now();
        ensure!(
            now < deadline,
            "timed out waiting for DEX transaction receipt"
        );
        let interval = if now.duration_since(started_at) < FAST_RECEIPT_POLL_WINDOW {
            FAST_RECEIPT_POLL_INTERVAL
        } else {
            SLOW_RECEIPT_POLL_INTERVAL
        };
        let sleep = tokio::time::sleep(interval.min(deadline - now));
        tokio::pin!(sleep);
        let head_stream_closed = if let Some(receiver) = head_receiver.as_mut() {
            tokio::select! {
                result = receiver.changed() => result.is_err(),
                () = &mut sleep => false,
            }
        } else {
            sleep.await;
            false
        };
        if head_stream_closed {
            head_receiver = None;
        }
    }
}

fn unix_seconds() -> anyhow::Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")
        .map(|duration| duration.as_secs())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        hint::black_box,
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        thread::JoinHandle,
        time::Duration,
    };

    use alloy_primitives::{Address, B256, U256, address, hex, keccak256};
    use serde_json::{Value, json};

    use super::{
        CAMELOT_V3_SWAP_GAS_LIMIT, DexExecutionService, DexExecutionServiceError, DexExecutor,
        DexProtocol, EvmExecutionRequest, ExactInputSwapRequest, ExecuteCallPolicy, GasLimitPolicy,
        MAX_GAS_LIMIT, SwapRoute, SwapSubmissionPolicy, allowance_grant_for_policy,
        exact_input_calldata, is_definitive_prebroadcast_rejection, settlement_log_for_route,
        settlement_logs_for_route, transaction_fees_for_policy, wallet_transfer_totals,
    };
    use crate::dex::pool_id::V4PoolKey;
    use crate::{
        chain::rpc::{JsonRpcClient, ReceiptLog, ReceiptLogPosition, TransactionReceipt},
        dex::events::{camelot_fee_topic, pancake_v3_swap_topic, v3_swap_topic},
        domain::compiled::CompiledNetworkGasPolicy,
        paired_benchmark::{assert_named_paired_non_regression, assert_paired_non_regression},
        wallet::{
            EvmWallet, JournalIntent, JournalOperationIdentity, JournalStatus, TransactionJournal,
            UnknownOutcomeReason, WalletCall,
        },
    };

    const PRIVATE_KEY: &str = "0x59c6995e998f97a5a0044976f7d04f8b2b7f4e5b5d5f3e49f2f4e7838a2b0c19";
    static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn rails_v3_gas_multiplier_and_extra_are_applied() {
        let policy = GasLimitPolicy::for_swap(DexProtocol::UniswapV3, 25_000);
        assert_eq!(policy.resolve(Some(100_000), 110_000).unwrap(), 225_000);
        assert_eq!(policy.resolve(None, 110_000).unwrap(), 275_000);
        assert_eq!(
            GasLimitPolicy::for_swap(DexProtocol::UniswapV3, 0)
                .resolve_without_estimate(None)
                .unwrap(),
            250_000
        );
    }

    #[test]
    fn camelot_has_provider_scoped_gas_and_exact_call_identity() {
        let policy = GasLimitPolicy::for_swap(DexProtocol::CamelotV3, 0);
        assert_eq!(
            policy.resolve_without_estimate(None).unwrap(),
            CAMELOT_V3_SWAP_GAS_LIMIT
        );
        assert_eq!(policy.resolve(None, 175_000).unwrap(), 1_000_000);

        let request = ExactInputSwapRequest::with_rails_defaults(
            "camelot-read-only",
            SwapRoute::CamelotV3 {
                router: Address::repeat_byte(0x11),
                pool: Address::repeat_byte(0x22),
            },
            Address::repeat_byte(0x33),
            Address::repeat_byte(0x44),
            U256::from(6_000_000_u64),
            U256::from(5_000_000_u64),
            1_900_000_002,
        );
        let calldata = exact_input_calldata(&request, Address::repeat_byte(0x55)).unwrap();
        assert_eq!(&calldata[..4], &[0xbc, 0x65, 0x11, 0x88]);
        assert_eq!(calldata.len(), 4 + 7 * 32);
    }

    #[test]
    fn arbitrum_fee_and_allowance_policy_has_no_world_chain_fallback_or_tip() {
        let policy = CompiledNetworkGasPolicy::ArbitrumOne {
            requires_fresh_rpc_gas_price: true,
            max_priority_fee_per_gas_wei: 0,
            max_fee_headroom_bps: 12_000,
            includes_l1_fee: false,
        };
        assert_eq!(
            transaction_fees_for_policy(&policy, 12_345).unwrap(),
            (14_814, 0)
        );
        assert_eq!(
            allowance_grant_for_policy(&policy, U256::from(10_000_000_u64)),
            U256::from(10_000_000_u64)
        );
        assert!(transaction_fees_for_policy(&policy, 0).is_err());

        let world = CompiledNetworkGasPolicy::WorldChainV12 {
            fallback_gas_price_wei: 100_000,
            includes_l1_fee: true,
        };
        assert_eq!(allowance_grant_for_policy(&world, U256::ONE), U256::MAX);
        assert_ne!(
            transaction_fees_for_policy(&world, 12_345).unwrap(),
            (12_345, 0)
        );
    }

    #[test]
    fn rails_v4_gas_multiplier_minimum_and_extra_are_applied() {
        let policy = GasLimitPolicy::for_swap(DexProtocol::UniswapV4, 10_000);
        assert_eq!(policy.resolve(Some(50_000), 60_000).unwrap(), 260_000);
        assert_eq!(policy.resolve(Some(120_000), 130_000).unwrap(), 490_000);
        assert_eq!(
            GasLimitPolicy::for_swap(DexProtocol::UniswapV4, 0)
                .resolve_without_estimate(None)
                .unwrap(),
            250_000
        );
        assert!(
            GasLimitPolicy::for_swap(DexProtocol::UniswapV4, MAX_GAS_LIMIT)
                .resolve(Some(120_000), 130_000)
                .is_err()
        );
    }

    #[test]
    fn receipt_transfer_logs_produce_exact_wallet_delta() {
        fn address_topic(address: Address) -> alloy_primitives::B256 {
            let mut word = [0_u8; 32];
            word[12..].copy_from_slice(address.as_slice());
            word.into()
        }
        let token = Address::repeat_byte(0x11);
        let wallet = Address::repeat_byte(0x22);
        let router = Address::repeat_byte(0x33);
        let amount = U256::from(123_u16);
        let log = ReceiptLog {
            address: token,
            topics: vec![
                keccak256("Transfer(address,address,uint256)"),
                address_topic(router),
                address_topic(wallet),
            ],
            data: amount.to_be_bytes::<32>().to_vec(),
            position: None,
        };
        assert_eq!(
            wallet_transfer_totals(&[log], token, wallet).unwrap(),
            (amount, U256::ZERO)
        );
    }

    #[test]
    fn successful_receipt_proves_the_selected_pool_swap_position() {
        let pool = Address::repeat_byte(0x44);
        let mut data = vec![0_u8; 5 * 32];
        data[95] = 1;
        data[112..128].copy_from_slice(&1_000_u128.to_be_bytes());
        let receipt = TransactionReceipt {
            transaction_hash: alloy_primitives::B256::repeat_byte(0x55),
            block_number: 123,
            status: 1,
            gas_used: 90_000,
            effective_gas_price: 1_000_000,
            l1_fee: 0,
            logs: vec![ReceiptLog {
                address: pool,
                topics: vec![v3_swap_topic(), B256::ZERO, B256::ZERO],
                data,
                position: Some(ReceiptLogPosition {
                    transaction_hash: alloy_primitives::B256::repeat_byte(0x55),
                    block_number: 123,
                    block_hash: B256::repeat_byte(0x66),
                    transaction_index: 7,
                    log_index: 9,
                    removed: false,
                }),
            }],
        };

        let log = settlement_log_for_route(
            &receipt,
            SwapRoute::UniswapV3 {
                router: Address::repeat_byte(0x33),
                pool,
                fee_pips: 3_000,
            },
        )
        .unwrap()
        .unwrap();

        assert_eq!(log.block_number, 123);
        assert_eq!(log.transaction_index, 7);
        assert_eq!(log.log_index, 9);
        assert_eq!(log.address, pool);
    }

    #[test]
    fn pinned_camelot_arb_usdc_receipt_proves_fee_swap_and_wallet_deltas() {
        let pool = address!("fae2ae0a9f87fd35b5b0e24b47bac796a7eefea1");
        let arb = address!("912ce59144191c1204e64559fe8253a0e49e6548");
        let usdc = address!("af88d065e77c8cc2239327c5edb3a432268e5831");
        let wallet = address!("278d858f05b94576c1e6f73285886876ff6ef8d2");
        let transaction_hash = "0xb78c6166d764cc5c7075853d2eae19ae03780bc979158283215b11393bcbc20d"
            .parse::<B256>()
            .unwrap();
        let block_hash = "0x2f474c93b25d6c52a6b3114ebccdde3d3ce010e5ccdac659922336289feeca41"
            .parse::<B256>()
            .unwrap();
        let position = |log_index| {
            Some(ReceiptLogPosition {
                transaction_hash,
                block_number: 491_426_734,
                block_hash,
                transaction_index: 8,
                log_index,
                removed: false,
            })
        };
        let topic = |value: &str| value.parse::<B256>().unwrap();
        let transfer = keccak256("Transfer(address,address,uint256)");
        let logs = vec![
            ReceiptLog {
                address: pool,
                topics: vec![camelot_fee_topic()],
                data: hex::decode(concat!(
                    "0000000000000000000000000000000000000000000000000000000000000068",
                    "0000000000000000000000000000000000000000000000000000000000000068"
                ))
                .unwrap(),
                position: position(17),
            },
            ReceiptLog {
                address: usdc,
                topics: vec![
                    transfer,
                    topic("0x000000000000000000000000fae2ae0a9f87fd35b5b0e24b47bac796a7eefea1"),
                    topic("0x000000000000000000000000278d858f05b94576c1e6f73285886876ff6ef8d2"),
                ],
                data: hex::decode(
                    "0000000000000000000000000000000000000000000000000000000000ed91f4",
                )
                .unwrap(),
                position: position(18),
            },
            ReceiptLog {
                address: arb,
                topics: vec![
                    transfer,
                    topic("0x000000000000000000000000278d858f05b94576c1e6f73285886876ff6ef8d2"),
                    topic("0x000000000000000000000000fae2ae0a9f87fd35b5b0e24b47bac796a7eefea1"),
                ],
                data: hex::decode(
                    "00000000000000000000000000000000000000000000000a63c954375be9cce0",
                )
                .unwrap(),
                position: position(19),
            },
            ReceiptLog {
                address: arb,
                topics: vec![
                    transfer,
                    topic("0x000000000000000000000000fae2ae0a9f87fd35b5b0e24b47bac796a7eefea1"),
                    topic("0x00000000000000000000000058095979b412a366687ca05cbe85ff56241be21f"),
                ],
                data: hex::decode(
                    "000000000000000000000000000000000000000000000000000a9f437629b6b6",
                )
                .unwrap(),
                position: position(20),
            },
            ReceiptLog {
                address: pool,
                topics: vec![
                    v3_swap_topic(),
                    topic("0x000000000000000000000000278d858f05b94576c1e6f73285886876ff6ef8d2"),
                    topic("0x000000000000000000000000278d858f05b94576c1e6f73285886876ff6ef8d2"),
                ],
                data: hex::decode(concat!(
                    "00000000000000000000000000000000000000000000000a63c954375be9cce0",
                    "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffff126e0c",
                    "0000000000000000000000000000000000000000000004c7fa99952c976887d6",
                    "00000000000000000000000000000000000000000000000002075d7ed929db34",
                    "fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffb6687"
                ))
                .unwrap(),
                position: position(21),
            },
        ];
        let receipt = TransactionReceipt {
            transaction_hash,
            block_number: 491_426_734,
            status: 1,
            gas_used: 307_979,
            effective_gas_price: 20_084_000,
            l1_fee: 0,
            logs,
        };
        let route = SwapRoute::CamelotV3 {
            router: address!("1f721e2e82f6676fce4ea07a5958cf098d339e18"),
            pool,
        };
        let proof = settlement_logs_for_route(&receipt, route).unwrap();
        assert_eq!(proof.fee.as_ref().unwrap().log_index, 17);
        assert_eq!(proof.swap.as_ref().unwrap().log_index, 21);
        assert_eq!(
            wallet_transfer_totals(&receipt.logs, arb, wallet).unwrap(),
            (
                U256::ZERO,
                U256::from_be_slice(&hex::decode("0a63c954375be9cce0").unwrap())
            )
        );
        assert_eq!(
            wallet_transfer_totals(&receipt.logs, usdc, wallet).unwrap(),
            (U256::from(15_569_396_u64), U256::ZERO)
        );

        let mut incomplete = receipt;
        incomplete.logs.remove(0);
        assert_eq!(
            settlement_logs_for_route(&incomplete, route).unwrap(),
            super::ReceiptSettlementLogs::default()
        );
    }

    #[test]
    fn pancake_receipt_uses_extended_swap_layout_and_preserves_provider_locator() {
        let pool = Address::repeat_byte(0x45);
        let transaction_hash = B256::repeat_byte(0x56);
        let mut data = vec![0_u8; 7 * 32];
        data[95] = 1;
        data[112..128].copy_from_slice(&1_000_u128.to_be_bytes());
        data[191] = 7;
        data[223] = 11;
        let receipt = TransactionReceipt {
            transaction_hash,
            block_number: 124,
            status: 1,
            gas_used: 100_000,
            effective_gas_price: 1_000_000,
            l1_fee: 0,
            logs: vec![ReceiptLog {
                address: pool,
                topics: vec![pancake_v3_swap_topic(), B256::ZERO, B256::ZERO],
                data,
                position: Some(ReceiptLogPosition {
                    transaction_hash,
                    block_number: 124,
                    block_hash: B256::repeat_byte(0x67),
                    transaction_index: 8,
                    log_index: 10,
                    removed: false,
                }),
            }],
        };

        let log = settlement_log_for_route(
            &receipt,
            SwapRoute::PancakeSwapV3 {
                router: Address::repeat_byte(0x33),
                pool,
                fee_pips: 500,
            },
        )
        .unwrap()
        .unwrap();

        assert_eq!(log.address, pool);
        assert_eq!(log.block_number, 124);
        assert!(
            settlement_log_for_route(
                &receipt,
                SwapRoute::UniswapV3 {
                    router: Address::repeat_byte(0x33),
                    pool,
                    fee_pips: 500,
                },
            )
            .is_err()
        );
    }

    #[test]
    fn successful_receipt_without_positional_swap_proof_keeps_the_fallback_available() {
        let pool = Address::repeat_byte(0x44);
        let receipt = TransactionReceipt {
            transaction_hash: B256::repeat_byte(0x55),
            block_number: 123,
            status: 1,
            gas_used: 90_000,
            effective_gas_price: 1_000_000,
            l1_fee: 0,
            logs: vec![ReceiptLog {
                address: Address::repeat_byte(0x11),
                topics: vec![keccak256("Transfer(address,address,uint256)")],
                data: Vec::new(),
                position: None,
            }],
        };

        assert!(
            settlement_log_for_route(
                &receipt,
                SwapRoute::UniswapV3 {
                    router: Address::repeat_byte(0x33),
                    pool,
                    fee_pips: 3_000,
                },
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    #[ignore = "manual release-mode paired V3 receipt benchmark"]
    fn benchmark_uniswap_and_pancake_v3_receipt_proof() {
        let pool = Address::repeat_byte(0x45);
        let receipt = |topic, words: usize| {
            let transaction_hash = B256::repeat_byte(words as u8);
            let mut data = vec![0_u8; words * 32];
            data[95] = 1;
            data[112..128].copy_from_slice(&1_000_u128.to_be_bytes());
            TransactionReceipt {
                transaction_hash,
                block_number: 124,
                status: 1,
                gas_used: 100_000,
                effective_gas_price: 1_000_000,
                l1_fee: 0,
                logs: vec![ReceiptLog {
                    address: pool,
                    topics: vec![topic, B256::ZERO, B256::ZERO],
                    data,
                    position: Some(ReceiptLogPosition {
                        transaction_hash,
                        block_number: 124,
                        block_hash: B256::repeat_byte(0x67),
                        transaction_index: 8,
                        log_index: 10,
                        removed: false,
                    }),
                }],
            }
        };
        let uniswap = receipt(v3_swap_topic(), 5);
        let pancake = receipt(pancake_v3_swap_topic(), 7);
        let uniswap_route = SwapRoute::UniswapV3 {
            router: Address::repeat_byte(0x33),
            pool,
            fee_pips: 500,
        };
        let pancake_route = SwapRoute::PancakeSwapV3 {
            router: Address::repeat_byte(0x33),
            pool,
            fee_pips: 500,
        };
        assert_paired_non_regression(
            "v3_receipt_proof_benchmark",
            1.10,
            || {
                black_box(settlement_log_for_route(&uniswap, uniswap_route)).unwrap();
            },
            || {
                black_box(settlement_log_for_route(&pancake, pancake_route)).unwrap();
            },
        );
    }

    #[test]
    #[ignore = "manual release-mode paired Camelot/Uniswap receipt benchmark"]
    fn benchmark_uniswap_and_camelot_v3_receipt_proof() {
        let pool = Address::repeat_byte(0x45);
        let token_in = Address::repeat_byte(0x02);
        let token_out = Address::repeat_byte(0x03);
        let wallet = Address::ZERO;
        let transfer_topic = keccak256("Transfer(address,address,uint256)");
        let receipt = |camelot: bool| {
            let transaction_hash = B256::repeat_byte(if camelot { 0x56 } else { 0x55 });
            let position = |log_index| {
                Some(ReceiptLogPosition {
                    transaction_hash,
                    block_number: 124,
                    block_hash: B256::repeat_byte(0x67),
                    transaction_index: 8,
                    log_index,
                    removed: false,
                })
            };
            let mut swap_data = vec![0_u8; 5 * 32];
            swap_data[95] = 1;
            swap_data[112..128].copy_from_slice(&1_000_u128.to_be_bytes());
            let mut logs = Vec::with_capacity(5);
            if camelot {
                logs.push(ReceiptLog {
                    address: pool,
                    topics: vec![camelot_fee_topic()],
                    data: vec![0_u8; 2 * 32],
                    position: position(1),
                });
            } else {
                logs.push(ReceiptLog {
                    address: Address::repeat_byte(0x10),
                    topics: vec![B256::ZERO],
                    data: Vec::new(),
                    position: position(1),
                });
            }
            for index in 2..=4 {
                let mut data = vec![0_u8; 32];
                data[31] = index as u8;
                logs.push(ReceiptLog {
                    address: Address::repeat_byte(index as u8),
                    topics: vec![transfer_topic, B256::ZERO, B256::ZERO],
                    data,
                    position: position(index),
                });
            }
            logs.push(ReceiptLog {
                address: pool,
                topics: vec![v3_swap_topic(), B256::ZERO, B256::ZERO],
                data: swap_data,
                position: position(5),
            });
            TransactionReceipt {
                transaction_hash,
                block_number: 124,
                status: 1,
                gas_used: 100_000,
                effective_gas_price: 1_000_000,
                l1_fee: 0,
                logs,
            }
        };
        let uniswap = receipt(false);
        let camelot = receipt(true);
        let uniswap_route = SwapRoute::UniswapV3 {
            router: Address::repeat_byte(0x33),
            pool,
            fee_pips: 500,
        };
        let camelot_route = SwapRoute::CamelotV3 {
            router: Address::repeat_byte(0x33),
            pool,
        };
        assert_named_paired_non_regression(
            "camelot_v3_receipt_accounting_and_proof_benchmark",
            1.10,
            "uniswap_v3",
            "camelot_v3",
            || {
                black_box(wallet_transfer_totals(&uniswap.logs, token_in, wallet)).unwrap();
                black_box(wallet_transfer_totals(&uniswap.logs, token_out, wallet)).unwrap();
                black_box(settlement_logs_for_route(&uniswap, uniswap_route)).unwrap();
            },
            || {
                black_box(wallet_transfer_totals(&camelot.logs, token_in, wallet)).unwrap();
                black_box(wallet_transfer_totals(&camelot.logs, token_out, wallet)).unwrap();
                black_box(settlement_logs_for_route(&camelot, camelot_route)).unwrap();
            },
        );
    }

    #[test]
    fn request_rejects_mismatched_v4_tokens() {
        let currency0 = Address::repeat_byte(0x11);
        let currency1 = Address::repeat_byte(0x22);
        let request = ExactInputSwapRequest {
            operation_id: "validation-v4-buy".to_owned(),
            route: SwapRoute::V4 {
                router: Address::repeat_byte(0x33),
                pool_key: V4PoolKey::new(currency0, currency1, 500, 10, Address::ZERO).unwrap(),
            },
            token_in: currency0,
            token_out: Address::repeat_byte(0x44),
            amount_in: U256::from(10_000_000_u64),
            amount_out_minimum: U256::from(1_u8),
            quoted_gas: None,
            additional_gas: 0,
            deadline_unix_seconds: 1_800_000_000,
            confirmation_timeout: Duration::from_secs(5),
            submission_policy: SwapSubmissionPolicy::SimulateAndEstimate,
            reconciliation_only: false,
        };
        assert!(request.validate().is_err());
    }

    #[tokio::test]
    async fn dedicated_worker_journals_an_onchain_revert() {
        let (endpoint, server, _) = spawn_mock_rpc(MockOutcome::Revert);
        let path = journal_path("revert");
        let wallet = EvmWallet::from_private_key(PRIVATE_KEY).unwrap();
        let mut executor = DexExecutor::hydrate(
            JsonRpcClient::new(endpoint).unwrap(),
            wallet,
            480,
            path.clone(),
        )
        .await
        .unwrap();
        executor.allowance_mutations_enabled = false;
        let service = DexExecutionService::spawn(executor, 1).unwrap();
        let error = service
            .execute(v3_request("rustval-revert"))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            DexExecutionServiceError::Reverted {
                gas_used: 90_000,
                effective_gas_price: 1_000_000,
                l1_fee: 1_000,
                ..
            }
        ));
        drop(service);
        server.join().unwrap();

        let journal = TransactionJournal::open(&path).unwrap();
        assert!(matches!(
            journal.operation("rustval-revert.swap").unwrap().status,
            JournalStatus::MinedReverted {
                block_number: 123,
                ..
            }
        ));
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn dedicated_worker_signs_above_the_removed_rust_fee_cap() {
        let (endpoint, server, _) = spawn_mock_rpc(MockOutcome::RevertHighGas);
        let path = journal_path("high-gas");
        let wallet = EvmWallet::from_private_key(PRIVATE_KEY).unwrap();
        let mut executor = DexExecutor::hydrate(
            JsonRpcClient::new(endpoint).unwrap(),
            wallet,
            480,
            path.clone(),
        )
        .await
        .unwrap();
        executor.allowance_mutations_enabled = false;
        let service = DexExecutionService::spawn(executor, 1).unwrap();

        let error = service
            .execute(v3_request("rustval-high-gas"))
            .await
            .unwrap_err();

        assert!(matches!(error, DexExecutionServiceError::Reverted { .. }));
        drop(service);
        server.join().unwrap();
        fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn dedicated_worker_uses_rails_fallback_when_gas_price_rpc_fails() {
        let (endpoint, server, _) = spawn_mock_rpc(MockOutcome::RevertGasPriceUnavailable);
        let path = journal_path("gas-price-fallback");
        let wallet = EvmWallet::from_private_key(PRIVATE_KEY).unwrap();
        let mut executor = DexExecutor::hydrate(
            JsonRpcClient::new(endpoint).unwrap(),
            wallet,
            480,
            path.clone(),
        )
        .await
        .unwrap();
        executor.allowance_mutations_enabled = false;
        let service = DexExecutionService::spawn(executor, 1).unwrap();

        let error = service
            .execute(v3_request("rustval-gas-price-fallback"))
            .await
            .unwrap_err();

        assert!(matches!(error, DexExecutionServiceError::Reverted { .. }));
        drop(service);
        server.join().unwrap();
        fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn dedicated_worker_reuses_the_background_gas_price_sample() {
        let (endpoint, server, gas_price_requests) = spawn_mock_rpc(MockOutcome::TwoReverts);
        let path = journal_path("background-gas-price");
        let wallet = EvmWallet::from_private_key(PRIVATE_KEY).unwrap();
        let mut executor = DexExecutor::hydrate(
            JsonRpcClient::new(endpoint).unwrap(),
            wallet,
            480,
            path.clone(),
        )
        .await
        .unwrap();
        executor.allowance_mutations_enabled = false;
        let service = DexExecutionService::spawn(executor, 1).unwrap();

        assert!(
            matches!(
                service
                    .execute(v3_request("rustval-background-gas-price-1"))
                    .await,
                Err(DexExecutionServiceError::Reverted { .. })
            ),
            "first transaction should use the primed background sample"
        );
        assert!(
            matches!(
                service
                    .execute(v3_request("rustval-background-gas-price-2"))
                    .await,
                Err(DexExecutionServiceError::Reverted { .. })
            ),
            "second transaction should reuse the same fresh sample"
        );

        drop(service);
        server.join().unwrap();
        assert_eq!(gas_price_requests.load(Ordering::Relaxed), 1);
        fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn dedicated_worker_journals_an_unknown_broadcast_outcome() {
        let (endpoint, server, _) = spawn_mock_rpc(MockOutcome::BroadcastRejected);
        let path = journal_path("broadcast-rejected");
        let wallet = EvmWallet::from_private_key(PRIVATE_KEY).unwrap();
        let mut executor = DexExecutor::hydrate(
            JsonRpcClient::new(endpoint).unwrap(),
            wallet,
            480,
            path.clone(),
        )
        .await
        .unwrap();
        executor.allowance_mutations_enabled = false;
        let service = DexExecutionService::spawn(executor, 1).unwrap();
        let error = service
            .execute(v3_request("rustval-broadcast-rejected"))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            DexExecutionServiceError::OutcomeUnknown { .. }
        ));
        drop(service);
        server.join().unwrap();

        let journal = TransactionJournal::open(&path).unwrap();
        assert!(matches!(
            journal
                .operation("rustval-broadcast-rejected.swap")
                .unwrap()
                .status,
            JournalStatus::OutcomeUnknown {
                reason: UnknownOutcomeReason::BroadcastTransport,
                ..
            }
        ));
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn journaled_broadcast_is_reconciled_from_its_receipt_without_rebroadcast() {
        let path = journal_path("broadcast-receipt-reconciliation");
        let wallet = EvmWallet::from_private_key(PRIVATE_KEY).unwrap();
        let call = WalletCall::validated_contract_call(
            Address::repeat_byte(0x11),
            U256::ZERO,
            vec![0x12, 0x34],
        )
        .unwrap();
        let transaction_hash = B256::repeat_byte(0x42);
        let identity = JournalOperationIdentity {
            operation_id: "rustval-broadcast-receipt".to_owned(),
            chain_id: 480,
            wallet: wallet.address(),
            nonce: 7,
            scope: None,
        };
        let mut journal = TransactionJournal::open(&path).unwrap();
        journal
            .record_intent(&JournalIntent {
                identity: identity.clone(),
                purpose: "receipt_reconciliation".to_owned(),
                target: call.target(),
                native_value: call.value(),
                calldata_hash: keccak256(call.calldata()),
            })
            .unwrap();
        journal.record_signed(&identity, transaction_hash).unwrap();
        journal
            .record_broadcast(&identity, transaction_hash)
            .unwrap();
        drop(journal);

        let (endpoint, server) = spawn_known_receipt_rpc(transaction_hash);
        let mut executor = DexExecutor::hydrate(
            JsonRpcClient::new(endpoint).unwrap(),
            wallet,
            480,
            path.clone(),
        )
        .await
        .unwrap();
        let receipt = executor
            .execute_call(
                identity.operation_id.clone(),
                "receipt_reconciliation",
                &call,
                ExecuteCallPolicy {
                    gas: GasLimitPolicy::fixed(100_000),
                    quoted_gas: Some(100_000),
                    confirmation_timeout: Duration::from_secs(1),
                    submission_policy: SwapSubmissionPolicy::Immediate,
                    allow_new_submission: true,
                },
                None,
            )
            .await
            .unwrap();

        assert_eq!(receipt.transaction_hash, transaction_hash);
        assert_eq!(receipt.status, 1);
        drop(executor);
        server.join().unwrap();
        let journal = TransactionJournal::open(&path).unwrap();
        assert!(matches!(
            journal.operation(&identity.operation_id).unwrap().status,
            JournalStatus::MinedSuccess {
                block_number: 123,
                ..
            }
        ));
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn expired_swap_reconciliation_reads_exact_broadcast_without_new_submission() {
        fn address_topic(address: Address) -> String {
            let mut word = [0_u8; 32];
            word[12..].copy_from_slice(address.as_slice());
            format!("{:#x}", B256::from(word))
        }

        let path = journal_path("expired-swap-reconciliation");
        let wallet = EvmWallet::from_private_key(PRIVATE_KEY).unwrap();
        let mut request = v3_request("rustval-expired-swap-reconciliation");
        request.deadline_unix_seconds = 1;
        request.reconciliation_only = true;
        let SwapRoute::UniswapV3 {
            router, fee_pips, ..
        } = request.route
        else {
            unreachable!();
        };
        let calldata = super::v3_exact_input(
            request.token_in,
            request.token_out,
            fee_pips,
            wallet.address(),
            request.amount_in,
            request.amount_out_minimum,
        )
        .unwrap();
        let call = WalletCall::validated_contract_call(router, U256::ZERO, calldata).unwrap();
        let transaction_hash = B256::repeat_byte(0x43);
        let identity = JournalOperationIdentity {
            operation_id: format!("{}.swap", request.operation_id),
            chain_id: 480,
            wallet: wallet.address(),
            nonce: 7,
            scope: None,
        };
        let mut journal = TransactionJournal::open(&path).unwrap();
        journal
            .record_intent(&JournalIntent {
                identity: identity.clone(),
                purpose: "uniswap_v3".to_owned(),
                target: call.target(),
                native_value: call.value(),
                calldata_hash: keccak256(call.calldata()),
            })
            .unwrap();
        journal.record_signed(&identity, transaction_hash).unwrap();
        journal
            .record_broadcast(&identity, transaction_hash)
            .unwrap();
        drop(journal);

        let transfer_topic = format!("{:#x}", keccak256("Transfer(address,address,uint256)"));
        let block_hash = format!("{:#x}", B256::repeat_byte(0x55));
        let common = |address: Address, topics: Vec<String>, amount: U256, log_index: &str| {
            json!({
                "address": format!("{address:#x}"),
                "topics": topics,
                "data": format!("0x{:064x}", amount),
                "transactionHash": format!("{transaction_hash:#x}"),
                "blockNumber": "0x7b",
                "blockHash": block_hash,
                "transactionIndex": "0x0",
                "logIndex": log_index,
                "removed": false
            })
        };
        let logs = vec![
            common(
                request.token_in,
                vec![
                    transfer_topic.clone(),
                    address_topic(wallet.address()),
                    address_topic(router),
                ],
                request.amount_in,
                "0x0",
            ),
            common(
                request.token_out,
                vec![
                    transfer_topic,
                    address_topic(router),
                    address_topic(wallet.address()),
                ],
                request.amount_out_minimum,
                "0x1",
            ),
        ];
        let (endpoint, server) = spawn_known_receipt_rpc_with_logs(transaction_hash, logs);
        let mut executor = DexExecutor::hydrate(
            JsonRpcClient::new(endpoint).unwrap(),
            wallet,
            480,
            path.clone(),
        )
        .await
        .unwrap();

        let outcome = executor.execute_exact_input(request).await.unwrap();

        assert_eq!(outcome.transaction_hash, transaction_hash);
        assert_eq!(outcome.token_in_spent, U256::from(10_000_000_u64));
        assert_eq!(outcome.token_out_received, U256::from(1_000_000_u64));
        drop(executor);
        server.join().unwrap();
        let journal = TransactionJournal::open(&path).unwrap();
        assert!(matches!(
            journal.operation(&identity.operation_id).unwrap().status,
            JournalStatus::MinedSuccess { .. }
        ));
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn capital_handle_uses_the_same_nonce_journal_and_cannot_keep_service_alive() {
        let (endpoint, server, _) = spawn_mock_rpc(MockOutcome::CapitalSuccess);
        let path = journal_path("capital-success");
        let wallet = EvmWallet::from_private_key(PRIVATE_KEY).unwrap();
        let wallet_address = wallet.address();
        let mut executor = DexExecutor::hydrate(
            JsonRpcClient::new(endpoint).unwrap(),
            wallet,
            480,
            path.clone(),
        )
        .await
        .unwrap();
        executor.allowance_mutations_enabled = false;
        let service = DexExecutionService::spawn(executor, 1).unwrap();
        let owner = service.evm_execution_owner();
        let receipt = owner
            .execute(EvmExecutionRequest {
                operation_id: "rebalance-1:deposit".to_owned(),
                purpose: "rebalance_wallet_to_binance".to_owned(),
                call: WalletCall::erc20_transfer(
                    Address::repeat_byte(0x11),
                    Address::repeat_byte(0x22),
                    U256::from(10_u64),
                )
                .unwrap(),
                confirmation_timeout: Duration::from_secs(5),
            })
            .await
            .unwrap();
        assert_eq!(receipt.status, 1);
        assert_eq!(owner.chain_id(), 480);
        assert_eq!(owner.wallet_address(), wallet_address);

        drop(service);
        assert!(
            owner
                .execute(EvmExecutionRequest {
                    operation_id: "rebalance-2:deposit".to_owned(),
                    purpose: "rebalance_wallet_to_binance".to_owned(),
                    call: WalletCall::erc20_transfer(
                        Address::repeat_byte(0x11),
                        Address::repeat_byte(0x22),
                        U256::from(10_u64),
                    )
                    .unwrap(),
                    confirmation_timeout: Duration::from_secs(5),
                })
                .await
                .is_err()
        );
        server.join().unwrap();

        let journal = TransactionJournal::open(&path).unwrap();
        assert!(matches!(
            journal.operation("rebalance-1:deposit").unwrap().status,
            JournalStatus::MinedSuccess { .. }
        ));
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn capital_reconciliation_reads_mined_unknown_without_new_submission() {
        let path = journal_path("capital-reconcile-mined-unknown");
        let wallet = EvmWallet::from_private_key(PRIVATE_KEY).unwrap();
        let call = WalletCall::erc20_transfer(
            Address::repeat_byte(0x11),
            Address::repeat_byte(0x22),
            U256::from(10_u64),
        )
        .unwrap();
        let transaction_hash = B256::repeat_byte(0x42);
        let identity = JournalOperationIdentity {
            operation_id: "rebalance-unknown:deposit".to_owned(),
            chain_id: 480,
            wallet: wallet.address(),
            nonce: 7,
            scope: None,
        };
        let mut journal = TransactionJournal::open(&path).unwrap();
        journal
            .record_intent(&JournalIntent {
                identity: identity.clone(),
                purpose: "rebalance_wallet_to_binance".to_owned(),
                target: call.target(),
                native_value: call.value(),
                calldata_hash: keccak256(call.calldata()),
            })
            .unwrap();
        journal.record_signed(&identity, transaction_hash).unwrap();
        journal
            .record_unknown_outcome(
                &identity,
                transaction_hash,
                UnknownOutcomeReason::BroadcastTransport,
            )
            .unwrap();
        drop(journal);

        let (endpoint, server) =
            spawn_known_receipt_rpc_with_logs_and_requests(transaction_hash, Vec::new(), 6);
        let mut executor = DexExecutor::hydrate(
            JsonRpcClient::new(endpoint).unwrap(),
            wallet,
            480,
            path.clone(),
        )
        .await
        .unwrap();
        executor.allowance_mutations_enabled = false;
        let service = DexExecutionService::spawn(executor, 1).unwrap();
        let owner = service.evm_execution_owner();
        let receipt = owner
            .reconcile(EvmExecutionRequest {
                operation_id: identity.operation_id.clone(),
                purpose: "rebalance_wallet_to_binance".to_owned(),
                call: call.clone(),
                confirmation_timeout: Duration::from_secs(5),
            })
            .await
            .unwrap();
        assert_eq!(receipt.transaction_hash, transaction_hash);
        assert_eq!(receipt.status, 1);

        let absent = owner
            .reconcile(EvmExecutionRequest {
                operation_id: "rebalance-absent:deposit".to_owned(),
                purpose: "rebalance_wallet_to_binance".to_owned(),
                call,
                confirmation_timeout: Duration::from_secs(5),
            })
            .await
            .unwrap_err();
        assert!(matches!(
            absent,
            DexExecutionServiceError::FailedBeforeSubmission { .. }
        ));

        drop(service);
        server.join().unwrap();
        let journal = TransactionJournal::open(&path).unwrap();
        assert!(matches!(
            journal.operation(&identity.operation_id).unwrap().status,
            JournalStatus::MinedSuccess {
                block_number: 123,
                ..
            }
        ));
        assert!(journal.operation("rebalance-absent:deposit").is_none());
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    fn v3_request(operation_id: &str) -> ExactInputSwapRequest {
        let mut request = ExactInputSwapRequest::with_rails_defaults(
            operation_id,
            SwapRoute::UniswapV3 {
                router: Address::repeat_byte(0x33),
                pool: Address::repeat_byte(0x44),
                fee_pips: 3_000,
            },
            Address::repeat_byte(0x11),
            Address::repeat_byte(0x22),
            U256::from(10_000_000_u64),
            U256::from(1_000_000_u64),
            1_800_000_000,
        );
        request.quoted_gas = Some(100_000);
        request.confirmation_timeout = Duration::from_secs(2);
        request.submission_policy = SwapSubmissionPolicy::Immediate;
        request
    }

    #[test]
    fn only_an_explicit_fee_cap_error_is_a_definitive_prebroadcast_rejection() {
        let definitive = anyhow::anyhow!(
            "JSON-RPC error -32000: max fee per gas less than block base fee: maxFeePerGas: 20102000 baseFee: 20148000"
        );
        assert!(is_definitive_prebroadcast_rejection(&definitive));
        assert!(!is_definitive_prebroadcast_rejection(&anyhow::anyhow!(
            "JSON-RPC error -32000: already known"
        )));
        assert!(!is_definitive_prebroadcast_rejection(&anyhow::anyhow!(
            "transport connection reset"
        )));
    }

    fn journal_path(name: &str) -> PathBuf {
        let sequence = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "poly-bot-dex-execution-{name}-{}-{sequence}.jsonl",
            std::process::id()
        ))
    }

    #[derive(Clone, Copy)]
    enum MockOutcome {
        Revert,
        TwoReverts,
        RevertHighGas,
        RevertGasPriceUnavailable,
        BroadcastRejected,
        CapitalSuccess,
    }

    fn spawn_mock_rpc(outcome: MockOutcome) -> (String, JoinHandle<()>, Arc<AtomicU64>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let request_count = match outcome {
            MockOutcome::Revert
            | MockOutcome::RevertHighGas
            | MockOutcome::RevertGasPriceUnavailable => 6,
            MockOutcome::TwoReverts => 8,
            MockOutcome::BroadcastRejected => 5,
            MockOutcome::CapitalSuccess => 9,
        };
        let gas_price_requests = Arc::new(AtomicU64::new(0));
        let server_gas_price_requests = Arc::clone(&gas_price_requests);
        let thread = std::thread::spawn(move || {
            let mut transaction_hash = None;
            for _ in 0..request_count {
                let (mut stream, _) = listener.accept().unwrap();
                let request = read_request(&mut stream);
                let id = request["id"].clone();
                let method = request["method"].as_str().unwrap();
                let response = match method {
                    "eth_chainId" => rpc_result(id, json!("0x1e0")),
                    "eth_getTransactionCount" => rpc_result(id, json!("0x7")),
                    "eth_call" if matches!(outcome, MockOutcome::CapitalSuccess) => {
                        rpc_result(id, json!("0x"))
                    }
                    "eth_call" => panic!("immediate swap unexpectedly called eth_call"),
                    "eth_estimateGas" if matches!(outcome, MockOutcome::CapitalSuccess) => {
                        rpc_result(id, json!("0x15f90"))
                    }
                    "eth_estimateGas" => {
                        panic!("immediate swap unexpectedly called eth_estimateGas")
                    }
                    "eth_gasPrice" => {
                        server_gas_price_requests.fetch_add(1, Ordering::Relaxed);
                        match outcome {
                            MockOutcome::RevertHighGas => rpc_result(id, json!("0x2e90edd000")),
                            MockOutcome::RevertGasPriceUnavailable => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "error": {
                                    "code": -32000,
                                    "message": "gas price unavailable for test"
                                }
                            }),
                            _ => rpc_result(id, json!("0xf4240")),
                        }
                    }
                    "eth_getBalance" if matches!(outcome, MockOutcome::CapitalSuccess) => {
                        rpc_result(id, json!("0x1000000000000000000"))
                    }
                    "eth_getBalance" => panic!("immediate swap unexpectedly called eth_getBalance"),
                    "eth_sendRawTransaction" => match outcome {
                        MockOutcome::Revert
                        | MockOutcome::TwoReverts
                        | MockOutcome::RevertHighGas
                        | MockOutcome::RevertGasPriceUnavailable
                        | MockOutcome::CapitalSuccess => {
                            let raw = request["params"][0].as_str().unwrap();
                            let raw = hex::decode(raw.trim_start_matches("0x")).unwrap();
                            let hash = keccak256(raw);
                            transaction_hash = Some(hash);
                            rpc_result(id, json!(format!("{hash:#x}")))
                        }
                        MockOutcome::BroadcastRejected => json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "error": {
                                "code": -32000,
                                "message": "transaction rejected for test"
                            }
                        }),
                    },
                    "eth_getTransactionReceipt" => {
                        let hash = transaction_hash.unwrap();
                        rpc_result(
                            id,
                            json!({
                                "transactionHash": format!("{hash:#x}"),
                                "blockNumber": "0x7b",
                                "status": if matches!(outcome, MockOutcome::CapitalSuccess) {
                                    "0x1"
                                } else {
                                    "0x0"
                                },
                                "gasUsed": "0x15f90",
                                "effectiveGasPrice": "0xf4240",
                                "l1Fee": "0x3e8"
                            }),
                        )
                    }
                    _ => panic!("unexpected mock RPC method {method}"),
                };
                write_response(&mut stream, &response);
            }
        });
        (format!("http://{address}"), thread, gas_price_requests)
    }

    fn spawn_known_receipt_rpc(transaction_hash: B256) -> (String, JoinHandle<()>) {
        spawn_known_receipt_rpc_with_logs(transaction_hash, Vec::new())
    }

    fn spawn_known_receipt_rpc_with_logs(
        transaction_hash: B256,
        logs: Vec<Value>,
    ) -> (String, JoinHandle<()>) {
        spawn_known_receipt_rpc_with_logs_and_requests(transaction_hash, logs, 4)
    }

    fn spawn_known_receipt_rpc_with_logs_and_requests(
        transaction_hash: B256,
        logs: Vec<Value>,
        request_count: usize,
    ) -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let thread = std::thread::spawn(move || {
            for _ in 0..request_count {
                let (mut stream, _) = listener.accept().unwrap();
                let request = read_request(&mut stream);
                let id = request["id"].clone();
                let method = request["method"].as_str().unwrap();
                let response = match method {
                    "eth_chainId" => rpc_result(id, json!("0x1e0")),
                    "eth_getTransactionCount" => rpc_result(id, json!("0x8")),
                    "eth_gasPrice" => rpc_result(id, json!("0xf4240")),
                    "eth_getTransactionReceipt" => rpc_result(
                        id,
                        json!({
                            "transactionHash": format!("{transaction_hash:#x}"),
                            "blockNumber": "0x7b",
                            "status": "0x1",
                            "gasUsed": "0x15f90",
                            "effectiveGasPrice": "0xf4240",
                            "l1Fee": "0x0",
                            "logs": logs
                        }),
                    ),
                    _ => panic!("unexpected known-receipt RPC method {method}"),
                };
                write_response(&mut stream, &response);
            }
        });
        (format!("http://{address}"), thread)
    }

    fn rpc_result(id: Value, result: Value) -> Value {
        json!({"jsonrpc": "2.0", "id": id, "result": result})
    }

    fn read_request(stream: &mut TcpStream) -> Value {
        let mut encoded = Vec::new();
        let mut buffer = [0_u8; 4096];
        let header_end = loop {
            let read = stream.read(&mut buffer).unwrap();
            assert!(
                read > 0,
                "mock RPC connection closed before request headers"
            );
            encoded.extend_from_slice(&buffer[..read]);
            if let Some(position) = encoded.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let headers = std::str::from_utf8(&encoded[..header_end]).unwrap();
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap();
        while encoded.len() < header_end + content_length {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read > 0, "mock RPC connection closed before request body");
            encoded.extend_from_slice(&buffer[..read]);
        }
        serde_json::from_slice(&encoded[header_end..header_end + content_length]).unwrap()
    }

    fn write_response(stream: &mut TcpStream, response: &Value) {
        let body = serde_json::to_vec(response).unwrap();
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .unwrap();
        stream.write_all(&body).unwrap();
        stream.flush().unwrap();
    }
}

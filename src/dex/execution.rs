use std::{
    path::PathBuf,
    thread::JoinHandle,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use alloy_primitives::{Address, B256, U256, keccak256};
use anyhow::{Context, bail, ensure};
use tokio::sync::{mpsc, oneshot, watch};

use crate::{
    chain::rpc::{CanonicalBlock, JsonRpcClient, ReceiptLog, TransactionReceipt},
    telemetry::ExecutionLatencyTelemetry,
    wallet::{
        EvmWallet, JournalStatus, NonceLane, NonceReconciliationOutcome, PROCESS_NONCE_LOCK_TTL,
        TransactionJournal, UnknownOutcomeReason, WalletCall, WalletTransactionParameters,
        acquire_process_nonce_lock, broadcast_signed_transaction,
    },
};

use super::calldata::{
    decode_permit2_allowance, permit2_allowance, permit2_approve, v3_exact_input,
    v4_exact_input_single,
};
use super::pool_id::V4PoolKey;

pub const PERMIT2_ADDRESS: Address = Address::new([
    0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0xd4, 0x73, 0x03, 0x0f, 0x11, 0x6d, 0xde, 0xe9, 0xf6, 0xb4,
    0x3a, 0xc7, 0x8b, 0xa3,
]);

const RAILS_PRIORITY_FEE_WEI: u128 = crate::admission::RAILS_PRIORITY_FEE_WEI;
const RAILS_DEFAULT_GAS_LIMIT: u64 = 800_000;
const RAILS_V4_MIN_SWAP_GAS_LIMIT: u64 = 250_000;
const RAILS_PERMIT2_APPROVAL_GAS_LIMIT: u64 = 120_000;
const GAS_PRICE_CACHE_TTL: Duration = Duration::from_secs(5);
const DEFAULT_SWAP_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(5);
const APPROVAL_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(120);
const FAST_RECEIPT_POLL_INTERVAL: Duration = Duration::from_millis(50);
const FAST_RECEIPT_POLL_WINDOW: Duration = Duration::from_secs(1);
const SLOW_RECEIPT_POLL_INTERVAL: Duration = Duration::from_millis(250);
const MAX_GAS_LIMIT: u64 = 5_000_000;
const PERMIT2_APPROVAL_VALIDITY: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const PERMIT2_MIN_REMAINING_VALIDITY: Duration = Duration::from_secs(60 * 60);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UniswapProtocol {
    V3,
    V4,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SwapSubmissionPolicy {
    SimulateAndEstimate,
    Immediate,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AllowanceRequirement {
    pub operation_id: String,
    pub protocol: UniswapProtocol,
    pub token: Address,
    pub router: Address,
    pub required: U256,
}

impl UniswapProtocol {
    pub const fn label(self) -> &'static str {
        match self {
            Self::V3 => "uniswap_v3",
            Self::V4 => "uniswap_v4",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SwapRoute {
    V3 {
        router: Address,
        fee_pips: u32,
    },
    V4 {
        router: Address,
        pool_key: V4PoolKey,
    },
}

impl SwapRoute {
    pub const fn protocol(self) -> UniswapProtocol {
        match self {
            Self::V3 { .. } => UniswapProtocol::V3,
            Self::V4 { .. } => UniswapProtocol::V4,
        }
    }

    pub const fn router(self) -> Address {
        match self {
            Self::V3 { router, .. } | Self::V4 { router, .. } => router,
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
            SwapRoute::V3 { fee_pips, .. } => {
                ensure!(fee_pips > 0 && fee_pips <= 0x00ff_ffff, "invalid V3 fee");
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
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SwapExecutionOutcome {
    pub protocol: UniswapProtocol,
    pub transaction_hash: B256,
    pub block_number: u64,
    pub gas_used: u64,
    pub effective_gas_price: u128,
    pub token_in_spent: U256,
    pub token_out_received: U256,
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
}

impl GasLimitPolicy {
    const fn for_swap(protocol: UniswapProtocol, additional: u64) -> Self {
        match protocol {
            UniswapProtocol::V3 => Self {
                multiplier: 2,
                minimum: 0,
                default: RAILS_DEFAULT_GAS_LIMIT,
                additional,
            },
            UniswapProtocol::V4 => Self {
                multiplier: 4,
                minimum: RAILS_V4_MIN_SWAP_GAS_LIMIT,
                default: RAILS_V4_MIN_SWAP_GAS_LIMIT,
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
            // Local quotes do not carry QuoterV2's gas field. Retain the Rails
            // default while also applying its protocol multiplier to the RPC
            // estimate so V4 does not silently fall back to only 250k.
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
    gas_price: Option<(Instant, u128)>,
    allowance_mutations_enabled: bool,
    last_terminal_receipt: Option<TransactionReceipt>,
    receipt_heads: Option<watch::Receiver<CanonicalBlock>>,
    latency_telemetry: Option<ExecutionLatencyTelemetry>,
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
            allowance_mutations_enabled: true,
            last_terminal_receipt: None,
            receipt_heads: None,
            latency_telemetry: None,
        })
    }

    pub fn set_latency_telemetry(&mut self, telemetry: ExecutionLatencyTelemetry) {
        self.latency_telemetry = Some(telemetry);
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
                UniswapProtocol::V3 => {
                    self.ensure_erc20_allowance(
                        &format!("{}.v3-router-approval", requirement.operation_id),
                        requirement.token,
                        requirement.router,
                        requirement.required,
                    )
                    .await?;
                }
                UniswapProtocol::V4 => {
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

    pub async fn execute_exact_input(
        &mut self,
        request: ExactInputSwapRequest,
    ) -> anyhow::Result<SwapExecutionOutcome> {
        self.last_terminal_receipt = None;
        request.validate()?;
        ensure!(self.nonce_lane.ready(), "DEX nonce lane is not ready");
        let protocol = request.route.protocol();
        if protocol == UniswapProtocol::V4 {
            ensure!(
                request.deadline_unix_seconds > unix_seconds()?,
                "Uniswap V4 request deadline has expired"
            );
        }
        if request.submission_policy == SwapSubmissionPolicy::Immediate {
            ensure!(
                !self.allowance_mutations_enabled,
                "immediate DEX submission requires startup-validated locked allowances"
            );
        } else {
            self.ensure_allowance(&request)
                .await
                .with_context(|| format!("{} input-token approval failed", protocol.label()))?;
        }

        let calldata = match request.route {
            SwapRoute::V3 { fee_pips, .. } => v3_exact_input(
                request.token_in,
                request.token_out,
                fee_pips,
                self.wallet.address(),
                request.amount_in,
                request.amount_out_minimum,
            )?,
            SwapRoute::V4 { pool_key, .. } => v4_exact_input_single(
                pool_key,
                request.token_in == pool_key.currency0,
                request.amount_in,
                request.amount_out_minimum,
                request.token_in,
                request.token_out,
                request.deadline_unix_seconds,
            )?,
        };
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
                },
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
        Ok(SwapExecutionOutcome {
            protocol,
            transaction_hash: receipt.transaction_hash,
            block_number: receipt.block_number,
            gas_used: receipt.gas_used,
            effective_gas_price: receipt.effective_gas_price,
            token_in_spent,
            token_out_received,
        })
    }

    fn classify_execution_error(
        &self,
        journal_operation_id: &str,
        reason: String,
    ) -> DexExecutionServiceError {
        let status = self
            .journal
            .operation(journal_operation_id)
            .map(|operation| &operation.status);
        match status {
            None | Some(JournalStatus::CancelledBeforeSigning) => {
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
                    gas_used: receipt.gas_used,
                    effective_gas_price: receipt.effective_gas_price,
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
            SwapRoute::V3 { router, .. } => {
                self.ensure_erc20_allowance(
                    &format!("{}.v3-router-approval", request.operation_id),
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

        let approval = WalletCall::erc20_approval(token, spender, U256::MAX)?;
        let receipt = self
            .execute_call(
                operation_id.to_owned(),
                "erc20_approval",
                &approval,
                ExecuteCallPolicy {
                    gas: GasLimitPolicy::fixed(RAILS_DEFAULT_GAS_LIMIT),
                    quoted_gas: Some(RAILS_DEFAULT_GAS_LIMIT),
                    confirmation_timeout: APPROVAL_CONFIRMATION_TIMEOUT,
                    submission_policy: SwapSubmissionPolicy::SimulateAndEstimate,
                },
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
                },
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
    ) -> anyhow::Result<TransactionReceipt> {
        if let Some(existing) = self.journal.operation(&operation_id) {
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
                } => self
                    .rpc
                    .transaction_receipt(transaction_hash)
                    .await?
                    .context("journaled successful DEX receipt is unavailable"),
                JournalStatus::MinedReverted { .. } => {
                    bail!("journaled DEX transaction reverted")
                }
                JournalStatus::CancelledBeforeSigning => {
                    bail!("journaled DEX transaction was cancelled before signing")
                }
                _ => bail!("journaled DEX transaction requires recovery"),
            };
        }
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
        let gas_price = self.current_gas_price().await?;
        let max_fee_per_gas = gas_price
            .checked_add(RAILS_PRIORITY_FEE_WEI)
            .context("DEX maximum fee overflow")?;
        let fee_parameters = WalletTransactionParameters {
            chain_id: self.nonce_lane.chain_id(),
            nonce: 0,
            gas_limit,
            max_fee_per_gas,
            max_priority_fee_per_gas: RAILS_PRIORITY_FEE_WEI.min(max_fee_per_gas),
        };
        // Immediate live swaps already passed admission against the in-memory
        // wallet snapshot. Like Rails, signing uses the fresh RPC gas price
        // without treating the admission sample as a fee cap. Avoid repeating
        // eth_getBalance on the latency-sensitive path. Startup approval writes
        // retain the direct RPC guard.
        if policy.submission_policy == SwapSubmissionPolicy::SimulateAndEstimate {
            let maximum_cost = call.maximum_native_cost(fee_parameters)?;
            ensure!(
                self.rpc.native_balance(self.wallet.address()).await? >= maximum_cost,
                "wallet native balance cannot cover maximum DEX gas"
            );
        }
        self.emit_latency_stage(&operation_id, "gas_price_rpc", gas_price_started, "success");

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
                let reason = if error.to_string().starts_with("JSON-RPC error") {
                    UnknownOutcomeReason::BroadcastRejected
                } else {
                    UnknownOutcomeReason::BroadcastTransport
                };
                self.nonce_lane
                    .record_unknown_outcome(&mut self.journal, reason)?;
                tracing::error!(
                    operation_id,
                    transaction_hash = %signed.hash,
                    nonce = signed.nonce,
                    error = %error,
                    "DEX transaction broadcast outcome is unknown and was journaled"
                );
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
            max_priority_fee_per_gas = RAILS_PRIORITY_FEE_WEI,
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
            tracing::error!(
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

    async fn current_gas_price(&mut self) -> anyhow::Result<u128> {
        if let Some((captured_at, gas_price)) = self.gas_price
            && captured_at.elapsed() < GAS_PRICE_CACHE_TTL
        {
            return Ok(gas_price);
        }
        let gas_price = self.rpc.gas_price().await?;
        ensure!(gas_price > 0, "RPC returned zero gas price");
        self.gas_price = Some((Instant::now(), gas_price));
        Ok(gas_price)
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
    response: oneshot::Sender<Result<SwapExecutionOutcome, DexExecutionServiceError>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DexExecutionServiceError {
    FailedBeforeSubmission {
        reason: String,
    },
    Reverted {
        transaction_hash: B256,
        gas_used: u64,
        effective_gas_price: u128,
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
                let mut executor = executor;
                while let Some(work) = receiver.blocking_recv() {
                    let operation_id = work.request.operation_id.clone();
                    let journal_operation_id = format!("{operation_id}.swap");
                    executor.emit_latency_stage(
                        &operation_id,
                        "worker_queue",
                        work.enqueued_at,
                        "success",
                    );
                    let execution_started = Instant::now();
                    let result = runtime
                        .block_on(executor.execute_exact_input(work.request))
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
                        tracing::error!(
                            operation_id,
                            error = %error,
                            "DEX execution request failed; inspect transaction journal before retry"
                        );
                    }
                    if work.response.send(result).is_err() {
                        tracing::warn!(operation_id, "DEX execution caller dropped its response");
                    }
                }
            })
            .context("failed to spawn DEX executor thread")?;
        Ok(Self {
            sender: Some(sender),
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
        sender
            .send(WorkItem {
                request,
                enqueued_at: Instant::now(),
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
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
        thread::JoinHandle,
        time::Duration,
    };

    use alloy_primitives::{Address, U256, hex, keccak256};
    use serde_json::{Value, json};

    use super::{
        DexExecutionService, DexExecutionServiceError, DexExecutor, ExactInputSwapRequest,
        GasLimitPolicy, MAX_GAS_LIMIT, SwapRoute, SwapSubmissionPolicy, UniswapProtocol,
        wallet_transfer_totals,
    };
    use crate::dex::pool_id::V4PoolKey;
    use crate::{
        chain::rpc::{JsonRpcClient, ReceiptLog},
        wallet::{EvmWallet, JournalStatus, TransactionJournal, UnknownOutcomeReason},
    };

    const PRIVATE_KEY: &str = "0x59c6995e998f97a5a0044976f7d04f8b2b7f4e5b5d5f3e49f2f4e7838a2b0c19";
    static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn rails_v3_gas_multiplier_and_extra_are_applied() {
        let policy = GasLimitPolicy::for_swap(UniswapProtocol::V3, 25_000);
        assert_eq!(policy.resolve(Some(100_000), 110_000).unwrap(), 225_000);
        assert_eq!(policy.resolve(None, 110_000).unwrap(), 825_000);
    }

    #[test]
    fn rails_v4_gas_multiplier_minimum_and_extra_are_applied() {
        let policy = GasLimitPolicy::for_swap(UniswapProtocol::V4, 10_000);
        assert_eq!(policy.resolve(Some(50_000), 60_000).unwrap(), 260_000);
        assert_eq!(policy.resolve(Some(120_000), 130_000).unwrap(), 490_000);
        assert!(
            GasLimitPolicy::for_swap(UniswapProtocol::V4, MAX_GAS_LIMIT)
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
        };
        assert_eq!(
            wallet_transfer_totals(&[log], token, wallet).unwrap(),
            (amount, U256::ZERO)
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
        };
        assert!(request.validate().is_err());
    }

    #[tokio::test]
    async fn dedicated_worker_journals_an_onchain_revert() {
        let (endpoint, server) = spawn_mock_rpc(MockOutcome::Revert);
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
        let (endpoint, server) = spawn_mock_rpc(MockOutcome::RevertHighGas);
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
    async fn dedicated_worker_journals_an_unknown_broadcast_outcome() {
        let (endpoint, server) = spawn_mock_rpc(MockOutcome::BroadcastRejected);
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
                reason: UnknownOutcomeReason::BroadcastRejected,
                ..
            }
        ));
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    fn v3_request(operation_id: &str) -> ExactInputSwapRequest {
        let mut request = ExactInputSwapRequest::with_rails_defaults(
            operation_id,
            SwapRoute::V3 {
                router: Address::repeat_byte(0x33),
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
        RevertHighGas,
        BroadcastRejected,
    }

    fn spawn_mock_rpc(outcome: MockOutcome) -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let request_count = match outcome {
            MockOutcome::Revert | MockOutcome::RevertHighGas => 6,
            MockOutcome::BroadcastRejected => 5,
        };
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
                    "eth_call" => {
                        panic!("immediate swap unexpectedly called eth_call")
                    }
                    "eth_estimateGas" => {
                        panic!("immediate swap unexpectedly called eth_estimateGas")
                    }
                    "eth_gasPrice" => match outcome {
                        MockOutcome::RevertHighGas => rpc_result(id, json!("0x2e90edd000")),
                        _ => rpc_result(id, json!("0xf4240")),
                    },
                    "eth_getBalance" => {
                        panic!("immediate swap unexpectedly called eth_getBalance")
                    }
                    "eth_sendRawTransaction" => match outcome {
                        MockOutcome::Revert | MockOutcome::RevertHighGas => {
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
                                "status": "0x0",
                                "gasUsed": "0x15f90",
                                "effectiveGasPrice": "0xf4240"
                            }),
                        )
                    }
                    _ => panic!("unexpected mock RPC method {method}"),
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

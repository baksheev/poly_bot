use std::{
    collections::BTreeMap,
    path::PathBuf,
    str::FromStr,
    time::{Duration, Instant},
};

use crate::{
    across::{
        AcrossClient, AcrossQuoteRequest, OPTIMISM_CHAIN_ID, OPTIMISM_USDC, OPTIMISM_WLD,
        WORLD_CHAIN_CHAIN_ID, WORLD_CHAIN_USDC, WORLD_CHAIN_WLD, swap_calldata_is_stale,
        validate_deposit_status, validate_quote,
    },
    binance::{
        account::{AccountInformation, BinanceAccountClient, BinanceApiError},
        capital::{
            AddressVerificationRecord, DepositRecord, TravelRuleAddressOwnershipProof,
            TravelRuleWithdrawalRecord, WithdrawalRecord, select_capital_routes,
        },
        sub_account::{SubAccountAssetBalance, UniversalTransferRecord},
    },
    chain::rpc::{JsonRpcClient, TransactionReceipt},
    dex::execution::{EvmExecutionOwnerHandle, EvmExecutionRequest},
    domain::compiled::CompiledCapitalPolicy,
    live_readiness::{ARBITRUM_ARB, ARBITRUM_CHAIN_ID, ARBITRUM_ESP, ARBITRUM_USDC},
    portfolio::authorize_rebalance_request,
    telemetry::TelemetryHandle,
    wallet::{
        EvmJournalScope, EvmWallet, JournalStatus, NonceLane, NonceReconciliationOutcome,
        PROCESS_NONCE_LOCK_TTL, TransactionJournal, UnknownOutcomeReason, WalletCall,
        WalletTransactionParameters, acquire_process_nonce_lock, broadcast_signed_transaction,
    },
};
use alloy_primitives::{Address, B256, U256, keccak256};
use anyhow::{Context, bail, ensure};
use rust_decimal::Decimal;

use super::{
    Direction, RebalanceExecutionJournal, RebalanceExecutionOperation, RebalanceExecutionProgress,
    RebalanceExecutionRequest, Route, executor::MAX_TRAVEL_RULE_OWNERSHIP_REJECTION_RETRIES,
};

const GAS_LIMIT_MARGIN_NUMERATOR: u64 = 120;
const GAS_LIMIT_MARGIN_DENOMINATOR: u64 = 100;
const MAX_ERC20_GAS_LIMIT: u64 = 1_000_000;
const MAX_FEE_PER_GAS_WEI: u128 = 100_000_000_000;
const STANDARD_BINANCE_WITHDRAWAL_API_MODE: &str = "standard";
const TRAVEL_RULE_REQUIRED_API_MODE: &str = "travel_rule_required_after_standard_-4104";
const TRAVEL_RULE_BINANCE_WITHDRAWAL_API_MODE: &str = "travel_rule_ae_self_owned";
const UNKNOWN_WITHDRAWAL_ABSENCE_CONFIRMATION_DELAY: Duration = Duration::from_secs(5);
const SHARED_EVM_CONFIRMATION_TIMEOUT_MAX: Duration = Duration::from_secs(300);

#[derive(Clone, Debug)]
pub struct RebalanceRuntimeLimits {
    pub maximum_wld: Decimal,
    pub maximum_usdc: Decimal,
    pub maximum_esp: Decimal,
    pub maximum_arb: Decimal,
    pub operation_timeout: Duration,
}

impl RebalanceRuntimeLimits {
    fn maximum_for(&self, symbol: &str) -> anyhow::Result<Decimal> {
        let maximum = match symbol {
            "WLD" => self.maximum_wld,
            "USDC" => self.maximum_usdc,
            "ESP" => self.maximum_esp,
            "ARB" => self.maximum_arb,
            _ => bail!("full rebalance executor only permits WLD, USDC, ESP, and ARB"),
        };
        ensure!(
            maximum > Decimal::ZERO,
            "live rebalance maximum for {symbol} is disabled"
        );
        Ok(maximum)
    }
}

pub struct RebalanceExecutor {
    trading_binance: BinanceAccountClient,
    treasury_binance: BinanceAccountClient,
    subaccount_email: String,
    across: AcrossClient,
    execution_journal: RebalanceExecutionJournal,
    evm: RebalanceEvmExecutionOwner,
    limits: RebalanceRuntimeLimits,
    capital_policy: Option<CompiledCapitalPolicy>,
    telemetry: Option<RebalanceTelemetry>,
}

#[derive(Clone)]
struct RebalanceTelemetry {
    handle: TelemetryHandle,
    engine_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WithdrawalAbsenceEvidence {
    master_free_base_units: U256,
    master_locked_base_units: U256,
    trading_free_base_units: U256,
    trading_locked_base_units: U256,
    wallet_balance_base_units: U256,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WithdrawalAbsenceConfirmation {
    evidence: WithdrawalAbsenceEvidence,
    required_withdrawal_base_units: U256,
    stale: bool,
}

/// The only rebalancing component that can access signing material or nonce
/// lanes. The saga receives only typed call/chain commands.
struct RebalanceEvmExecutionOwner {
    world: JsonRpcClient,
    optimism: JsonRpcClient,
    direct_read_rpcs: BTreeMap<u64, JsonRpcClient>,
    wallet: EvmWallet,
    transaction_journal: TransactionJournal,
    world_nonce: NonceLane,
    optimism_nonce: NonceLane,
    arbitrum: Option<EvmExecutionOwnerHandle>,
}

impl RebalanceEvmExecutionOwner {
    fn wallet_address(&self) -> Address {
        self.wallet.address()
    }

    fn rpc(&self, chain_id: u64) -> anyhow::Result<&JsonRpcClient> {
        match chain_id {
            WORLD_CHAIN_CHAIN_ID => Ok(&self.world),
            OPTIMISM_CHAIN_ID => Ok(&self.optimism),
            chain_id => self.direct_read_rpcs.get(&chain_id).with_context(|| {
                format!("rebalance EVM owner has no read lane for chain {chain_id}")
            }),
        }
    }

    fn nonce_state(&self, chain_id: u64) -> anyhow::Result<&crate::wallet::NonceLaneState> {
        match chain_id {
            WORLD_CHAIN_CHAIN_ID => Ok(self.world_nonce.state()),
            OPTIMISM_CHAIN_ID => Ok(self.optimism_nonce.state()),
            _ => bail!("rebalance EVM owner has no nonce lane for chain {chain_id}"),
        }
    }

    async fn attach_arbitrum(
        &mut self,
        owner: EvmExecutionOwnerHandle,
        rpc: JsonRpcClient,
    ) -> anyhow::Result<()> {
        ensure!(
            owner.chain_id() == ARBITRUM_CHAIN_ID,
            "shared Arbitrum EVM owner returned the wrong chain id"
        );
        ensure!(
            owner.wallet_address() == self.wallet.address(),
            "shared Arbitrum EVM owner returned a different wallet"
        );
        ensure!(
            rpc.chain_id().await? == ARBITRUM_CHAIN_ID,
            "shared Arbitrum read RPC returned the wrong chain id"
        );
        ensure!(
            !self.direct_read_rpcs.contains_key(&ARBITRUM_CHAIN_ID) && self.arbitrum.is_none(),
            "shared Arbitrum EVM owner was attached twice"
        );
        self.direct_read_rpcs.insert(ARBITRUM_CHAIN_ID, rpc);
        self.arbitrum = Some(owner);
        Ok(())
    }

    async fn execute(
        &mut self,
        chain_id: u64,
        operation_id: String,
        purpose: &str,
        call: &WalletCall,
        timeout: Duration,
    ) -> anyhow::Result<B256> {
        if chain_id == ARBITRUM_CHAIN_ID {
            let receipt = self
                .arbitrum
                .as_ref()
                .context("rebalance EVM owner has no shared Arbitrum execution lane")?
                .execute(EvmExecutionRequest {
                    operation_id,
                    purpose: purpose.to_owned(),
                    call: call.clone(),
                    confirmation_timeout: shared_evm_confirmation_timeout(timeout),
                })
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            return Ok(receipt.transaction_hash);
        }
        let (rpc, nonce) = match chain_id {
            WORLD_CHAIN_CHAIN_ID => (&self.world, &mut self.world_nonce),
            OPTIMISM_CHAIN_ID => (&self.optimism, &mut self.optimism_nonce),
            _ => bail!("rebalance EVM owner has no execution lane for chain {chain_id}"),
        };
        execute_wallet_call(
            rpc,
            &self.wallet,
            nonce,
            &mut self.transaction_journal,
            operation_id,
            purpose,
            call,
            timeout,
        )
        .await
    }

    async fn reconcile_arbitrum(
        &mut self,
        operation_id: String,
        purpose: &str,
        call: &WalletCall,
        timeout: Duration,
    ) -> anyhow::Result<TransactionReceipt> {
        self.arbitrum
            .as_ref()
            .context("rebalance EVM owner has no shared Arbitrum execution lane")?
            .reconcile(EvmExecutionRequest {
                operation_id,
                purpose: purpose.to_owned(),
                call: call.clone(),
                confirmation_timeout: shared_evm_confirmation_timeout(timeout),
            })
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))
    }
}

fn shared_evm_confirmation_timeout(operation_timeout: Duration) -> Duration {
    operation_timeout.min(SHARED_EVM_CONFIRMATION_TIMEOUT_MAX)
}

impl std::fmt::Debug for RebalanceExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RebalanceExecutor")
            .field("wallet", &self.evm.wallet_address())
            .field("world_nonce", &self.evm.nonce_state(WORLD_CHAIN_CHAIN_ID))
            .field("optimism_nonce", &self.evm.nonce_state(OPTIMISM_CHAIN_ID))
            .field("arbitrum_shared_owner", &self.evm.arbitrum.is_some())
            .field("capital_policy", &self.capital_policy)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl RebalanceExecutor {
    #[allow(clippy::too_many_arguments)]
    pub async fn hydrate(
        mut trading_binance: BinanceAccountClient,
        mut treasury_binance: BinanceAccountClient,
        subaccount_email: String,
        across: AcrossClient,
        world: JsonRpcClient,
        optimism: JsonRpcClient,
        direct_read_rpcs: BTreeMap<u64, JsonRpcClient>,
        wallet: EvmWallet,
        execution_journal_path: PathBuf,
        transaction_journal_path: PathBuf,
        limits: RebalanceRuntimeLimits,
    ) -> anyhow::Result<Self> {
        ensure!(
            limits.operation_timeout >= Duration::from_secs(60),
            "rebalance timeout is too short"
        );
        ensure!(
            limits.operation_timeout <= Duration::from_secs(24 * 60 * 60),
            "rebalance timeout exceeds one day"
        );
        let owner = wallet.address();
        let (world_chain, optimism_chain) =
            tokio::try_join!(world.chain_id(), optimism.chain_id())?;
        ensure!(
            world_chain == WORLD_CHAIN_CHAIN_ID,
            "World RPC returned the wrong chain id"
        );
        ensure!(
            optimism_chain == OPTIMISM_CHAIN_ID,
            "Optimism RPC returned the wrong chain id"
        );
        for (expected_chain_id, rpc) in &direct_read_rpcs {
            ensure!(
                *expected_chain_id != WORLD_CHAIN_CHAIN_ID
                    && *expected_chain_id != OPTIMISM_CHAIN_ID,
                "additional direct-read RPC duplicates an owned execution lane"
            );
            ensure!(
                rpc.chain_id().await? == *expected_chain_id,
                "additional direct-read RPC returned the wrong chain id"
            );
        }
        ensure!(
            subaccount_email.contains('@') && subaccount_email.is_ascii(),
            "Binance sub-account email is invalid"
        );
        trading_binance.synchronize_clock().await?;
        treasury_binance.synchronize_clock().await?;
        let trading_account = trading_binance.account_information().await?;
        ensure!(
            trading_account.can_deposit,
            "Binance trading sub-account does not permit deposits"
        );
        let trading_permissions = trading_binance.api_key_permissions().await?;
        ensure!(
            trading_permissions.enable_reading,
            "Binance trading sub-account key does not permit reads"
        );
        ensure!(
            trading_permissions.ip_restrict,
            "Binance trading sub-account key is not IP restricted"
        );
        let treasury_account = treasury_binance.account_information().await?;
        ensure!(
            treasury_account.can_withdraw,
            "Binance master account does not permit withdrawals"
        );
        let treasury_permissions = treasury_binance.api_key_permissions().await?;
        ensure!(
            treasury_permissions.enable_reading,
            "Binance master treasury key does not permit reads"
        );
        ensure!(
            treasury_permissions.enable_withdrawals,
            "Binance master treasury key does not permit withdrawals"
        );
        ensure!(
            treasury_permissions.enable_internal_transfer,
            "Binance master treasury key does not permit internal transfers"
        );
        ensure!(
            treasury_permissions.permits_universal_transfer,
            "Binance master treasury key does not permit universal transfers"
        );
        ensure!(
            treasury_permissions.ip_restrict,
            "Binance master treasury key is not IP restricted"
        );
        let master_view = treasury_binance
            .subaccount_spot_assets(&subaccount_email)
            .await?;
        validate_master_subaccount_view(&trading_account, &master_view.balances)?;

        let mut transaction_journal = TransactionJournal::open(transaction_journal_path)?;
        let (world_latest, world_pending, optimism_latest, optimism_pending) = tokio::try_join!(
            world.latest_nonce(owner),
            world.pending_nonce(owner),
            optimism.latest_nonce(owner),
            optimism.pending_nonce(owner),
        )?;
        let world_reconciled = NonceLane::reconcile(
            &world,
            &mut transaction_journal,
            WORLD_CHAIN_CHAIN_ID,
            owner,
            world_latest,
            world_pending,
        )
        .await?;
        let optimism_reconciled = NonceLane::reconcile(
            &optimism,
            &mut transaction_journal,
            OPTIMISM_CHAIN_ID,
            owner,
            optimism_latest,
            optimism_pending,
        )
        .await?;
        let mut world_nonce = finish_known_pending_recovery(
            &world,
            &mut transaction_journal,
            world_reconciled,
            limits.operation_timeout,
        )
        .await?;
        let mut optimism_nonce = finish_known_pending_recovery(
            &optimism,
            &mut transaction_journal,
            optimism_reconciled,
            limits.operation_timeout,
        )
        .await?;
        let wallet_id = format!("wallet:{owner:#x}");
        world_nonce.set_journal_scope(EvmJournalScope {
            schema_version: EvmJournalScope::SCHEMA_VERSION,
            network_id: "world-chain".to_owned(),
            wallet_id: wallet_id.clone(),
            strategy_id: "rebalance-world-chain-v12".to_owned(),
        })?;
        optimism_nonce.set_journal_scope(EvmJournalScope {
            schema_version: EvmJournalScope::SCHEMA_VERSION,
            network_id: "optimism".to_owned(),
            wallet_id,
            strategy_id: "rebalance-world-chain-v12".to_owned(),
        })?;

        Ok(Self {
            trading_binance,
            treasury_binance,
            subaccount_email,
            across,
            execution_journal: RebalanceExecutionJournal::open(execution_journal_path)?,
            evm: RebalanceEvmExecutionOwner {
                world,
                optimism,
                direct_read_rpcs,
                wallet,
                transaction_journal,
                world_nonce,
                optimism_nonce,
                arbitrum: None,
            },
            limits,
            capital_policy: None,
            telemetry: None,
        })
    }

    pub fn set_capital_policy(
        &mut self,
        policy: Option<CompiledCapitalPolicy>,
    ) -> anyhow::Result<()> {
        ensure!(
            self.capital_policy.is_none(),
            "rebalance capital canary policy was configured twice"
        );
        self.capital_policy = policy;
        Ok(())
    }

    pub fn set_telemetry(&mut self, handle: TelemetryHandle, engine_id: String) {
        self.telemetry = Some(RebalanceTelemetry { handle, engine_id });
    }

    pub async fn attach_arbitrum_execution_owner(
        &mut self,
        owner: EvmExecutionOwnerHandle,
        rpc: JsonRpcClient,
    ) -> anyhow::Result<()> {
        self.evm.attach_arbitrum(owner, rpc).await
    }

    pub fn active_operation(&self) -> anyhow::Result<Option<&RebalanceExecutionOperation>> {
        self.execution_journal.active_operation()
    }

    pub fn quarantined_operations(&self) -> impl Iterator<Item = &RebalanceExecutionOperation> {
        self.execution_journal.quarantined_operations()
    }

    pub fn has_reconcilable_across_fill_quarantine(&self) -> anyhow::Result<bool> {
        Ok(self
            .execution_journal
            .next_reconcilable_across_fill_quarantine()?
            .is_some())
    }

    pub fn reopen_next_retryable_quarantine(
        &mut self,
    ) -> anyhow::Result<Option<RebalanceExecutionOperation>> {
        let reopened = self.execution_journal.reopen_next_retryable_quarantine()?;
        if let Some(operation) = reopened.as_ref() {
            tracing::warn!(
                operation_id = operation.intent.operation_id,
                token = operation.intent.token_symbol,
                progress = ?operation.progress,
                "reopened one previously quarantined rebalance after its false-positive guard was corrected"
            );
        }
        Ok(reopened)
    }

    /// Reconciles a quarantined Arbitrum wallet-to-Binance transfer against
    /// the shared EVM journal. The owner is explicitly reconciliation-only, so
    /// an absent or mismatched transaction cannot reserve a nonce or broadcast.
    pub async fn reconcile_next_arbitrum_deposit_quarantine(
        &mut self,
    ) -> anyhow::Result<Option<RebalanceExecutionOperation>> {
        let Some(operation) = self
            .execution_journal
            .next_reconcilable_arbitrum_deposit_quarantine()?
            .cloned()
        else {
            return Ok(None);
        };
        let (binance_network, chain_id) = match &operation.intent.route {
            Route::Direct {
                binance_network,
                chain_id,
            } => (binance_network.clone(), *chain_id),
            _ => unreachable!("reconcilable deposit quarantine must be direct"),
        };
        ensure!(
            chain_id == ARBITRUM_CHAIN_ID,
            "reconcilable deposit quarantine is not on Arbitrum"
        );
        let address = self
            .trading_binance
            .evm_deposit_address(&operation.intent.token_symbol, &binance_network)
            .await?;
        let call = WalletCall::erc20_transfer(
            operation.intent.token_contract,
            address.address,
            operation.intent.amount,
        )?;
        let receipt = self
            .evm
            .reconcile_arbitrum(
                format!("{}:deposit", operation.intent.operation_id),
                "rebalance_wallet_to_binance",
                &call,
                self.limits.operation_timeout,
            )
            .await?;
        ensure!(
            receipt.status == 1,
            "reconciled Arbitrum rebalance deposit reverted"
        );
        let reconciled = self.execution_journal.record_reconciled_arbitrum_deposit(
            &operation.intent.operation_id,
            receipt.transaction_hash,
        )?;
        tracing::warn!(
            operation_id = reconciled.intent.operation_id,
            token = reconciled.intent.token_symbol,
            transaction_hash = %receipt.transaction_hash,
            block_number = receipt.block_number,
            "reconciled quarantined Arbitrum deposit from the exact mined EVM transaction"
        );
        Ok(Some(reconciled))
    }

    /// Reconciles an Across timeout only when the API and destination-chain
    /// receipt prove that the already-mined bridge was filled. This path does
    /// not reserve a nonce, sign, or broadcast another transaction.
    pub async fn reconcile_next_across_fill_quarantine(
        &mut self,
    ) -> anyhow::Result<Option<RebalanceExecutionOperation>> {
        let Some(operation) = self
            .execution_journal
            .next_reconcilable_across_fill_quarantine()?
            .cloned()
        else {
            return Ok(None);
        };
        let RebalanceExecutionProgress::BridgeMined {
            origin_chain_id,
            transaction_hash,
            minimum_output_amount,
            ..
        } = self
            .execution_journal
            .progress_before_quarantine(&operation.intent.operation_id)
            .cloned()
            .context("reconcilable Across fill has no prior mined bridge")?
        else {
            bail!("reconcilable Across fill did not follow a mined bridge")
        };
        let (bridge_chain_id, wallet_chain_id) = match &operation.intent.route {
            Route::Across {
                bridge_chain_id,
                wallet_chain_id,
                ..
            } => (*bridge_chain_id, *wallet_chain_id),
            _ => unreachable!("reconcilable Across fill must use Across"),
        };
        let (expected_origin_chain_id, destination_chain_id) = match operation.intent.direction {
            Direction::BinanceToWallet => (bridge_chain_id, wallet_chain_id),
            Direction::WalletToBinance => (wallet_chain_id, bridge_chain_id),
        };
        ensure!(
            origin_chain_id == expected_origin_chain_id,
            "reconciled Across origin chain differs from the durable route"
        );
        let minimum =
            u128::try_from(minimum_output_amount).context("Across minimum exceeds u128")?;
        let origin_transaction_hash = format!("{transaction_hash:#x}");
        let status = self.across.deposit_status(&origin_transaction_hash).await?;
        if !validate_deposit_status(
            &status,
            origin_chain_id,
            &origin_transaction_hash,
            destination_chain_id,
            token_on_chain(&operation.intent.token_symbol, destination_chain_id)?,
            minimum,
        )? {
            return Ok(None);
        }
        let fill_hash = B256::from_str(
            status
                .fill_txn_ref
                .as_deref()
                .context("Across fill has no transaction hash")?,
        )?;
        let receipt = self
            .evm
            .rpc(destination_chain_id)?
            .transaction_receipt(fill_hash)
            .await?
            .context("Across reports filled but the destination receipt is unavailable")?;
        let received = validate_across_fill_receipt(
            &receipt,
            fill_hash,
            token_on_chain(&operation.intent.token_symbol, destination_chain_id)?,
            operation.intent.wallet_owner,
            minimum_output_amount,
        )?;
        let reconciled = self.execution_journal.record_reconciled_across_fill(
            &operation.intent.operation_id,
            fill_hash,
            received,
        )?;
        tracing::warn!(
            operation_id = reconciled.intent.operation_id,
            token = reconciled.intent.token_symbol,
            origin_chain_id,
            destination_chain_id,
            origin_transaction_hash = %transaction_hash,
            fill_transaction_hash = %fill_hash,
            received_base_units = %received,
            "reconciled quarantined Across timeout from the exact destination receipt"
        );
        Ok(Some(reconciled))
    }

    pub fn quarantine_active_operation(
        &mut self,
        reason: &str,
    ) -> anyhow::Result<Option<RebalanceExecutionOperation>> {
        let Some(operation) = self.execution_journal.active_operation()?.cloned() else {
            return Ok(None);
        };
        let reason = reason.chars().take(1_024).collect::<String>();
        let quarantined = self.execution_journal.advance(
            &operation.intent.operation_id,
            RebalanceExecutionProgress::Quarantined { reason },
        )?;
        tracing::error!(
            operation_id = quarantined.intent.operation_id,
            token = quarantined.intent.token_symbol,
            "quarantined unresolved rebalance operation without blocking other assets"
        );
        Ok(Some(quarantined))
    }

    pub fn rebalance_risk(&self) -> anyhow::Result<super::RebalanceRisk> {
        let approval_session_id = self
            .capital_policy
            .as_ref()
            .context("rebalance capital canary policy is not configured")?
            .approval_session_id
            .as_str();
        self.execution_journal.rebalance_risk(approval_session_id)
    }

    pub fn approval_session_id(&self) -> Option<&str> {
        self.capital_policy
            .as_ref()
            .map(|policy| policy.approval_session_id.as_str())
    }

    pub fn operations(&self) -> &std::collections::BTreeMap<String, RebalanceExecutionOperation> {
        self.execution_journal.operations()
    }

    pub fn latest_rebalance_operation(&self) -> Option<&RebalanceExecutionOperation> {
        let approval_session_id = self.capital_policy.as_ref()?.approval_session_id.as_str();
        self.execution_journal
            .latest_rebalance_operation(approval_session_id)
    }

    fn emit_rebalance_binance_child(
        &self,
        operation: &RebalanceExecutionOperation,
        stage: &str,
        started_at: Instant,
        outcome: &str,
        error: Option<&anyhow::Error>,
    ) {
        if operation
            .intent
            .scope
            .as_ref()
            .is_none_or(|scope| scope.network_id != "chain:42161")
        {
            return;
        }
        let Some(telemetry) = &self.telemetry else {
            return;
        };
        telemetry.handle.emit(
            "rebalance_child",
            serde_json::json!({
                "engine_id": telemetry.engine_id,
                "strategy_id": "rebalance-arbitrum-usdc-esp",
                "approval_session_id": operation.intent.approval_session_id,
                "operation_id": operation.intent.operation_id,
                "owner": "binance_capital",
                "stage": stage,
                "duration_us": started_at.elapsed().as_micros(),
                "outcome": outcome,
                "error": error.map(|error| format!("{error:#}")),
            }),
        );
    }

    fn emit_rebalance_risk_snapshot(&self) {
        let Some(telemetry) = &self.telemetry else {
            return;
        };
        match self.rebalance_risk() {
            Ok(risk) => telemetry.handle.emit(
                "rebalance_risk_snapshot",
                serde_json::json!({
                    "engine_id": telemetry.engine_id,
                    "approval_session_id": self.capital_policy.as_ref()
                        .map(|policy| policy.approval_session_id.as_str()),
                    "transfer_count": risk.transfer_count,
                    "active_transfer_count": risk.active_transfer_count,
                    "failed_transfer_count": risk.failed_transfer_count,
                    "token_a_debit": risk.token_a_debit.to_string(),
                    "token_b_debit": risk.token_b_debit.to_string(),
                    "token_a_maximum_fee": risk.token_a_maximum_fee.to_string(),
                    "token_b_maximum_fee": risk.token_b_maximum_fee.to_string(),
                    "first_started_at_unix_ms": risk.first_started_at_unix_ms,
                    "outcome": "success",
                }),
            ),
            Err(error) => telemetry.handle.emit(
                "rebalance_risk_snapshot",
                serde_json::json!({
                    "engine_id": telemetry.engine_id,
                    "approval_session_id": self.capital_policy.as_ref()
                        .map(|policy| policy.approval_session_id.as_str()),
                    "outcome": "failed",
                    "error": format!("{error:#}"),
                }),
            ),
        }
    }

    pub async fn log_active_operation_recovery_evidence(&self) -> anyhow::Result<()> {
        let Some(operation) = self.execution_journal.active_operation()? else {
            tracing::info!("no active rebalance operation requires recovery");
            return Ok(());
        };
        let network = match &operation.intent.route {
            Route::Direct {
                binance_network, ..
            }
            | Route::Across {
                binance_network, ..
            } => binance_network,
        };
        let (withdrawals, travel_rule, transfers, account, addresses, questionnaire) = tokio::try_join!(
            self.treasury_binance.withdrawal_history(
                &operation.intent.token_symbol,
                &operation.intent.withdraw_order_id,
            ),
            self.treasury_binance.travel_rule_withdrawal_history_v2(
                &operation.intent.token_symbol,
                network,
                &operation.intent.withdraw_order_id,
            ),
            self.treasury_binance.universal_transfer_history(
                &self.subaccount_email,
                &operation.intent.withdraw_order_id,
            ),
            self.treasury_binance.account_information(),
            self.treasury_binance.withdrawal_address_list(),
            self.treasury_binance
                .travel_rule_questionnaire_requirements(),
        )?;
        let balance = account
            .balances
            .iter()
            .find(|balance| balance.asset == operation.intent.token_symbol);
        let wallet = format!("{:#x}", operation.intent.wallet_owner);
        let matching_addresses = addresses
            .iter()
            .filter(|record| record.address.eq_ignore_ascii_case(&wallet))
            .collect::<Vec<_>>();
        tracing::info!(
            operation_id = operation.intent.operation_id,
            token = operation.intent.token_symbol,
            amount_base_units = operation.intent.amount.to_string(),
            progress = ?operation.progress,
            network,
            capital_withdrawal_count = withdrawals.len(),
            capital_transaction_present = withdrawals
                .iter()
                .any(|record| !record.tx_id.trim().is_empty()),
            travel_rule_withdrawal_count = travel_rule.len(),
            travel_rule_transaction_present = travel_rule
                .iter()
                .any(|record| !record.tx_id.trim().is_empty()),
            master_transfer_count = transfers.len(),
            master_transfer_status = transfers
                .first()
                .map_or("absent", |record| record.status.as_str()),
            master_free = %balance.map_or(Decimal::ZERO, |balance| balance.free),
            master_locked = %balance.map_or(Decimal::ZERO, |balance| balance.locked),
            questionnaire_country_code = questionnaire
                .questionnaire_country_code
                .as_deref()
                .unwrap_or("NIL"),
            matching_withdrawal_address_count = matching_addresses.len(),
            "hydrated sanitised evidence for the sole active rebalance operation"
        );
        for record in matching_addresses {
            tracing::info!(
                operation_id = operation.intent.operation_id,
                token = record.coin,
                network = record.network,
                white_status = record.white_status,
                origin = record.origin,
                origin_type = record.origin_type,
                "hydrated exact Binance withdrawal whitelist record for the active wallet"
            );
        }
        Ok(())
    }

    pub async fn close_approved_travel_rule_rejection(
        &mut self,
        token_symbol: &str,
        amount: U256,
        wallet_owner: Address,
        network: &str,
        chain_id: u64,
        incident_reason: &str,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        let operation = self
            .execution_journal
            .active_operation()?
            .cloned()
            .context("approved Travel Rule recovery has no active operation")?;
        ensure!(
            operation.intent.token_symbol == token_symbol
                && operation.intent.amount == amount
                && operation.intent.wallet_owner == wallet_owner
                && operation.intent.direction == Direction::BinanceToWallet
                && operation.intent.route
                    == Route::Direct {
                        binance_network: network.to_owned(),
                        chain_id,
                    }
                && matches!(
                    operation.progress,
                    RebalanceExecutionProgress::BinanceTransferCompleted { .. }
                ),
            "active operation does not match the approved Travel Rule rejection"
        );
        ensure!(
            self.treasury_binance
                .withdrawal_history(
                    &operation.intent.token_symbol,
                    &operation.intent.withdraw_order_id,
                )
                .await?
                .is_empty(),
            "approved Travel Rule rejection recovery found an indexed withdrawal"
        );
        let mut travel_rule_history = self
            .treasury_binance
            .travel_rule_withdrawal_history_v2(
                &operation.intent.token_symbol,
                network,
                &operation.intent.withdraw_order_id,
            )
            .await?;
        if travel_rule_history.is_empty() {
            let requested =
                base_units_to_decimal(operation.intent.amount, operation.intent.token_decimals)?;
            travel_rule_history = self
                .treasury_binance
                .travel_rule_withdrawal_history_v2_for_network(
                    &operation.intent.token_symbol,
                    network,
                )
                .await?
                .into_iter()
                .filter(|record| {
                    matches_travel_rule_record_identity_without_client_id(
                        record,
                        requested,
                        wallet_owner,
                        &operation.intent.withdraw_order_id,
                    )
                })
                .collect();
        }
        if let [record] = travel_rule_history.as_mut_slice()
            && record.withdrawal_status.is_none()
        {
            let detailed = self
                .treasury_binance
                .travel_rule_withdrawal_history(record.tr_id)
                .await?;
            ensure!(
                detailed.len() == 1,
                "approved Travel Rule rejection has no unique trId detail"
            );
            merge_travel_rule_withdrawal_detail(record, &detailed[0])?;
        }
        for record in &travel_rule_history {
            tracing::info!(
                operation_id = operation.intent.operation_id,
                token = operation.intent.token_symbol,
                travel_rule_record_id = record.tr_id,
                travel_rule_status = record.travel_rule_status,
                withdrawal_status = ?record.withdrawal_status,
                transaction_id_present = !record.tx_id.trim().is_empty(),
                indexed_client_id_present = !record.withdraw_order_id.trim().is_empty(),
                "hydrated an exact candidate for the approved Travel Rule rejection"
            );
        }
        let indexed_rejection = reconcile_approved_travel_rule_rejection(&travel_rule_history)?;
        if let Some(record) = indexed_rejection {
            tracing::info!(
                operation_id = operation.intent.operation_id,
                token = operation.intent.token_symbol,
                travel_rule_record_id = record.tr_id,
                travel_rule_status = record.travel_rule_status,
                withdrawal_status = ?record.withdrawal_status,
                transaction_broadcast = false,
                "reconciled the indexed unbroadcast Travel Rule submission"
            );
            if record.is_approved_without_withdrawal() {
                let requested = base_units_to_decimal(
                    operation.intent.amount,
                    operation.intent.token_decimals,
                )?;
                let account = self.treasury_binance.account_information().await?;
                let balance = account
                    .balances
                    .iter()
                    .find(|balance| balance.asset == operation.intent.token_symbol)
                    .context(
                        "approved Travel Rule rejection asset is absent from the master account",
                    )?;
                ensure!(
                    balance.free == requested && balance.locked == Decimal::ZERO,
                    "approved Travel Rule rejection did not preserve the exact master balance"
                );
                tracing::info!(
                    operation_id = operation.intent.operation_id,
                    token = operation.intent.token_symbol,
                    master_free = %balance.free,
                    master_locked = %balance.locked,
                    "proved the approved-without-withdrawal Travel Rule rejection preserved master inventory"
                );
            }
        } else {
            tracing::info!(
                operation_id = operation.intent.operation_id,
                token = operation.intent.token_symbol,
                rejected_http_status = 400,
                rejected_error_code = -4024,
                transaction_broadcast = false,
                "reconciled the synchronous Travel Rule rejection absent from withdrawal history"
            );
        }
        let transfer = self
            .treasury_binance
            .universal_transfer_history(&self.subaccount_email, &operation.intent.withdraw_order_id)
            .await?
            .into_iter()
            .next()
            .context("approved Travel Rule rejection lost its master transfer evidence")?;
        validate_master_transfer_record(&operation, &self.subaccount_email, &transfer)?;
        ensure!(
            transfer.status == "SUCCESS",
            "approved Travel Rule rejection master transfer is not successful"
        );
        self.execution_journal.advance(
            &operation.intent.operation_id,
            RebalanceExecutionProgress::Failed {
                reason: incident_reason.to_owned(),
            },
        )
    }

    pub async fn recover_active(&mut self) -> anyhow::Result<Option<RebalanceExecutionOperation>> {
        let Some(operation) = self.execution_journal.active_operation()?.cloned() else {
            return Ok(None);
        };
        validate_approved_asset(
            &operation.intent.token_symbol,
            operation.intent.token_decimals,
            operation.intent.token_contract,
            route_wallet_chain_id(&operation.intent.route),
        )?;
        ensure!(
            operation.intent.wallet_owner == self.evm.wallet_address(),
            "journaled rebalance wallet differs from signer"
        );
        self.process_with_travel_rule_ownership_retries(operation, false)
            .await
            .map(Some)
    }

    pub async fn execute(
        &mut self,
        request: RebalanceExecutionRequest,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        ensure!(
            request.wallet_owner == self.evm.wallet_address(),
            "rebalance request wallet differs from signer"
        );
        validate_approved_asset(
            &request.token_symbol,
            request.token_decimals,
            request.token_contract,
            route_wallet_chain_id(&request.action.route),
        )?;
        let requested = base_units_to_decimal(request.action.amount, request.token_decimals)?;
        ensure!(
            requested <= self.limits.maximum_for(&request.token_symbol)?,
            "rebalance request exceeds the configured live maximum"
        );
        if request.authority == super::RebalanceExecutionAuthority::ArbitrumFullLive {
            let policy = self
                .capital_policy
                .as_ref()
                .context("rebalance request has no compiled capital policy")?;
            let risk = self
                .execution_journal
                .rebalance_risk(&policy.approval_session_id)?;
            authorize_rebalance_request(policy, &risk, &request)?;
        }
        let operation = self.execution_journal.reserve(&request)?;
        self.emit_rebalance_risk_snapshot();
        self.process_with_travel_rule_ownership_retries(operation, true)
            .await
    }

    async fn process_with_travel_rule_ownership_retries(
        &mut self,
        mut operation: RebalanceExecutionOperation,
        mut created_here: bool,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        let operation_id = operation.intent.operation_id.clone();
        loop {
            match self.process(operation, created_here).await {
                Ok(operation) => return Ok(operation),
                Err(error) if is_retryable_travel_rule_ownership_rejection(&error) => {
                    let Some(reopened) = self
                        .execution_journal
                        .reopen_retryable_travel_rule_ownership_failure(&operation_id)?
                    else {
                        return Err(error);
                    };
                    tracing::warn!(
                        operation_id,
                        token = reopened.intent.token_symbol,
                        retry_limit = MAX_TRAVEL_RULE_OWNERSHIP_REJECTION_RETRIES,
                        error = %format!("{error:#}"),
                        "Binance Travel Rule ownership rejection will receive another proven retry"
                    );
                    operation = reopened;
                    created_here = false;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn process(
        &mut self,
        operation: RebalanceExecutionOperation,
        created_here: bool,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        match (&operation.intent.route, operation.intent.direction) {
            (Route::Direct { .. }, Direction::BinanceToWallet) => {
                self.direct_binance_to_wallet(operation, created_here).await
            }
            (Route::Direct { .. }, Direction::WalletToBinance) => {
                self.direct_wallet_to_binance(operation).await
            }
            (Route::Across { .. }, Direction::BinanceToWallet) => {
                self.across_binance_to_wallet(operation, created_here).await
            }
            (Route::Across { .. }, Direction::WalletToBinance) => {
                self.across_wallet_to_binance(operation).await
            }
        }
    }

    async fn direct_binance_to_wallet(
        &mut self,
        mut operation: RebalanceExecutionOperation,
        created_here: bool,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        let (binance_network, chain_id) = match &operation.intent.route {
            Route::Direct {
                binance_network,
                chain_id,
            } => (binance_network.clone(), *chain_id),
            _ => unreachable!(),
        };
        ensure!(
            matches!(chain_id, WORLD_CHAIN_CHAIN_ID | ARBITRUM_CHAIN_ID),
            "direct rebalance target chain is not approved"
        );
        let withdrawal_submission_safe = created_here
            || matches!(
                operation.progress,
                RebalanceExecutionProgress::IntentRecorded
                    | RebalanceExecutionProgress::BinanceTransferSubmitted { .. }
            );
        let observe_master_transfer = matches!(
            operation.progress,
            RebalanceExecutionProgress::IntentRecorded
                | RebalanceExecutionProgress::BinanceTransferSubmitted { .. }
        );
        let master_transfer_started_at = Instant::now();
        let master_transfer_observation = operation.clone();
        if matches!(
            operation.progress,
            RebalanceExecutionProgress::IntentRecorded
        ) {
            self.verify_route(&operation, true).await?;
            let bridge_before = self
                .evm
                .rpc(chain_id)?
                .erc20_balance(
                    operation.intent.token_contract,
                    operation.intent.wallet_owner,
                )
                .await?;
            operation = match self
                .begin_master_transfer(operation, created_here, bridge_before)
                .await
            {
                Ok(operation) => operation,
                Err(error) => {
                    self.emit_rebalance_binance_child(
                        &master_transfer_observation,
                        "master_transfer",
                        master_transfer_started_at,
                        "failed",
                        Some(&error),
                    );
                    return Err(error);
                }
            };
        }
        operation = match self.finish_master_transfer(operation).await {
            Ok(operation) => {
                if observe_master_transfer {
                    self.emit_rebalance_binance_child(
                        &operation,
                        "master_transfer",
                        master_transfer_started_at,
                        "success",
                        None,
                    );
                }
                operation
            }
            Err(error) => {
                if observe_master_transfer {
                    self.emit_rebalance_binance_child(
                        &master_transfer_observation,
                        "master_transfer",
                        master_transfer_started_at,
                        "failed",
                        Some(&error),
                    );
                }
                return Err(error);
            }
        };
        if matches!(
            operation.progress,
            RebalanceExecutionProgress::BinanceTransferCompleted { .. }
        ) {
            self.verify_route(&operation, true).await?;
        }
        let withdrawal_started_at = Instant::now();
        let withdrawal_observation = operation.clone();
        operation = match self
            .begin_binance_withdrawal(operation, withdrawal_submission_safe, &binance_network)
            .await
        {
            Ok(operation) => operation,
            Err(error) => {
                self.emit_rebalance_binance_child(
                    &withdrawal_observation,
                    "withdrawal",
                    withdrawal_started_at,
                    "failed",
                    Some(&error),
                );
                return Err(error);
            }
        };
        let record = match &operation.progress {
            RebalanceExecutionProgress::BinanceWithdrawalSubmitted { .. } => {
                match self.wait_withdrawal(&operation).await {
                    Ok(record) => record,
                    Err(error) => {
                        self.emit_rebalance_binance_child(
                            &operation,
                            "withdrawal",
                            withdrawal_started_at,
                            "failed",
                            Some(&error),
                        );
                        return Err(error);
                    }
                }
            }
            RebalanceExecutionProgress::Completed { .. } => {
                self.emit_rebalance_binance_child(
                    &operation,
                    "withdrawal",
                    withdrawal_started_at,
                    "success",
                    None,
                );
                return Ok(operation);
            }
            RebalanceExecutionProgress::CancelledStale { .. } => return Ok(operation),
            RebalanceExecutionProgress::Failed { reason } => {
                bail!("rebalance previously failed: {reason}")
            }
            _ => bail!("direct Binance-to-wallet operation has invalid recovery state"),
        };
        self.emit_rebalance_binance_child(
            &operation,
            "withdrawal",
            withdrawal_started_at,
            "success",
            None,
        );
        let received = withdrawal_received_base_units(&record, operation.intent.token_decimals)?;
        let wallet_after = self
            .wait_direct_withdrawal_credit(
                self.evm.rpc(chain_id)?,
                operation.intent.token_contract,
                operation.intent.wallet_owner,
                &record.tx_id,
                received,
            )
            .await?;
        let binance_after = self.binance_balance(&operation).await?;
        operation = self.execution_journal.advance(
            &operation.intent.operation_id,
            RebalanceExecutionProgress::Completed {
                binance_balance_after: binance_after,
                wallet_balance_after: wallet_after,
            },
        )?;
        Ok(operation)
    }

    async fn direct_wallet_to_binance(
        &mut self,
        mut operation: RebalanceExecutionOperation,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        let (binance_network, chain_id) = match &operation.intent.route {
            Route::Direct {
                binance_network,
                chain_id,
            } => (binance_network.clone(), *chain_id),
            _ => unreachable!(),
        };
        ensure!(
            matches!(chain_id, WORLD_CHAIN_CHAIN_ID | ARBITRUM_CHAIN_ID),
            "direct rebalance source chain is not approved"
        );
        if chain_id == ARBITRUM_CHAIN_ID {
            ensure!(
                operation
                    .intent
                    .scope
                    .as_ref()
                    .is_some_and(|scope| scope.network_id == "chain:42161"),
                "Arbitrum wallet-to-Binance transfer lacks rebalance authority"
            );
        }
        if matches!(
            operation.progress,
            RebalanceExecutionProgress::IntentRecorded
        ) {
            self.verify_route(&operation, false).await?;
            let address = self
                .trading_binance
                .evm_deposit_address(&operation.intent.token_symbol, &binance_network)
                .await?;
            let call = WalletCall::erc20_transfer(
                operation.intent.token_contract,
                address.address,
                operation.intent.amount,
            )?;
            let transaction_hash = self
                .evm
                .execute(
                    chain_id,
                    format!("{}:deposit", operation.intent.operation_id),
                    "rebalance_wallet_to_binance",
                    &call,
                    self.limits.operation_timeout,
                )
                .await?;
            operation = self.execution_journal.advance(
                &operation.intent.operation_id,
                RebalanceExecutionProgress::DepositTransferMined {
                    chain_id,
                    transaction_hash,
                },
            )?;
        }
        let deposit_started_at = Instant::now();
        let deposit_observation = operation.clone();
        operation = match self
            .finish_binance_deposit(operation, &binance_network)
            .await
        {
            Ok(operation) => {
                self.emit_rebalance_binance_child(
                    &operation,
                    "deposit_credit",
                    deposit_started_at,
                    "success",
                    None,
                );
                operation
            }
            Err(error) => {
                self.emit_rebalance_binance_child(
                    &deposit_observation,
                    "deposit_credit",
                    deposit_started_at,
                    "failed",
                    Some(&error),
                );
                return Err(error);
            }
        };
        Ok(operation)
    }

    async fn across_binance_to_wallet(
        &mut self,
        mut operation: RebalanceExecutionOperation,
        created_here: bool,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        let (binance_network, bridge_chain_id, wallet_chain_id) = match &operation.intent.route {
            Route::Across {
                binance_network,
                bridge_chain_id,
                wallet_chain_id,
            } => (binance_network.clone(), *bridge_chain_id, *wallet_chain_id),
            _ => unreachable!(),
        };
        ensure!(
            bridge_chain_id == OPTIMISM_CHAIN_ID && wallet_chain_id == WORLD_CHAIN_CHAIN_ID,
            "unsupported Across route"
        );
        let withdrawal_submission_safe = created_here
            || matches!(
                operation.progress,
                RebalanceExecutionProgress::IntentRecorded
                    | RebalanceExecutionProgress::BinanceTransferSubmitted { .. }
            );
        if matches!(
            operation.progress,
            RebalanceExecutionProgress::IntentRecorded
        ) {
            self.verify_route(&operation, true).await?;
            let bridge_before = self
                .evm
                .rpc(OPTIMISM_CHAIN_ID)?
                .erc20_balance(
                    token_on_chain(&operation.intent.token_symbol, OPTIMISM_CHAIN_ID)?,
                    operation.intent.wallet_owner,
                )
                .await?;
            operation = self
                .begin_master_transfer(operation, created_here, bridge_before)
                .await?;
        }
        operation = self.finish_master_transfer(operation).await?;
        if matches!(
            operation.progress,
            RebalanceExecutionProgress::BinanceTransferCompleted { .. }
        ) {
            self.verify_route(&operation, true).await?;
        }
        operation = self
            .begin_binance_withdrawal(operation, withdrawal_submission_safe, &binance_network)
            .await?;
        if matches!(
            operation.progress,
            RebalanceExecutionProgress::CancelledStale { .. }
        ) {
            return Ok(operation);
        }
        if let RebalanceExecutionProgress::BinanceWithdrawalSubmitted {
            bridge_balance_before,
            ..
        } = operation.progress
        {
            let record = self.wait_withdrawal(&operation).await?;
            let received =
                withdrawal_received_base_units(&record, operation.intent.token_decimals)?;
            self.wait_token_credit(
                self.evm.rpc(OPTIMISM_CHAIN_ID)?,
                token_on_chain(&operation.intent.token_symbol, OPTIMISM_CHAIN_ID)?,
                operation.intent.wallet_owner,
                bridge_balance_before,
                received,
            )
            .await?;
            operation = self.execution_journal.advance(
                &operation.intent.operation_id,
                RebalanceExecutionProgress::FundsOnBridge {
                    withdrawal_id: record.id,
                    transaction_id: record.tx_id,
                    received_base_units: received,
                },
            )?;
        }
        operation = self.bridge_across(operation, OPTIMISM_CHAIN_ID).await?;
        self.complete_across_to_wallet(operation).await
    }

    async fn across_wallet_to_binance(
        &mut self,
        mut operation: RebalanceExecutionOperation,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        let (binance_network, bridge_chain_id, wallet_chain_id) = match &operation.intent.route {
            Route::Across {
                binance_network,
                bridge_chain_id,
                wallet_chain_id,
            } => (binance_network.clone(), *bridge_chain_id, *wallet_chain_id),
            _ => unreachable!(),
        };
        ensure!(
            bridge_chain_id == OPTIMISM_CHAIN_ID && wallet_chain_id == WORLD_CHAIN_CHAIN_ID,
            "unsupported Across route"
        );
        if matches!(
            operation.progress,
            RebalanceExecutionProgress::IntentRecorded
                | RebalanceExecutionProgress::ApprovalMined { .. }
                | RebalanceExecutionProgress::BridgePrepared { .. }
        ) {
            self.verify_route(&operation, false).await?;
            operation = self.bridge_across(operation, WORLD_CHAIN_CHAIN_ID).await?;
        }
        if matches!(
            operation.progress,
            RebalanceExecutionProgress::BridgeMined { .. }
        ) {
            operation = self.wait_across_fill(operation).await?;
        }
        if let RebalanceExecutionProgress::AcrossFilled {
            received_base_units,
            ..
        } = operation.progress
        {
            self.verify_route(&operation, false).await?;
            let deposit_address = self
                .trading_binance
                .evm_deposit_address(&operation.intent.token_symbol, &binance_network)
                .await?;
            let call = WalletCall::erc20_transfer(
                token_on_chain(&operation.intent.token_symbol, OPTIMISM_CHAIN_ID)?,
                deposit_address.address,
                received_base_units,
            )?;
            let transaction_hash = self
                .evm
                .execute(
                    OPTIMISM_CHAIN_ID,
                    format!("{}:deposit", operation.intent.operation_id),
                    "rebalance_bridge_to_binance",
                    &call,
                    self.limits.operation_timeout,
                )
                .await?;
            operation = self.execution_journal.advance(
                &operation.intent.operation_id,
                RebalanceExecutionProgress::DepositTransferMined {
                    chain_id: OPTIMISM_CHAIN_ID,
                    transaction_hash,
                },
            )?;
        }
        self.finish_binance_deposit(operation, &binance_network)
            .await
    }

    async fn bridge_across(
        &mut self,
        mut operation: RebalanceExecutionOperation,
        origin_chain_id: u64,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        if let RebalanceExecutionProgress::BridgePrepared {
            origin_chain_id: prepared_chain_id,
            input_amount,
            calldata,
            calldata_hash,
            ..
        } = &operation.progress
        {
            ensure!(
                *prepared_chain_id == origin_chain_id,
                "journaled Across bridge uses the wrong origin chain"
            );
            ensure!(
                keccak256(calldata) == *calldata_hash,
                "journaled Across bridge calldata hash does not match"
            );
            let stale = swap_calldata_is_stale(calldata)?;
            tracing::warn!(
                operation_id = %operation.intent.operation_id,
                origin_chain_id,
                stale,
                "re-quoting journaled Across bridge calldata before broadcast"
            );
            let (_request, terms, destination_chain_id, output_token) = self
                .quote_across_bridge(&operation, origin_chain_id, *input_amount)
                .await?;
            ensure!(
                terms.approval.is_none(),
                "Across requires approval while re-quoting a prepared bridge"
            );
            let (target, calldata, minimum_output_amount, destination_balance_before) = self
                .materialize_across_bridge_terms(
                    &operation,
                    destination_chain_id,
                    output_token,
                    terms,
                )
                .await?;
            let call = WalletCall::validated_contract_call(target, U256::ZERO, calldata.clone())?;
            operation = self.execution_journal.advance(
                &operation.intent.operation_id,
                RebalanceExecutionProgress::BridgePrepared {
                    origin_chain_id,
                    input_amount: *input_amount,
                    target,
                    calldata_hash: keccak256(&calldata),
                    calldata,
                    minimum_output_amount,
                    destination_balance_before,
                },
            )?;
            let transaction_hash = self
                .execute_on_chain(
                    origin_chain_id,
                    format!("{}:bridge", operation.intent.operation_id),
                    "rebalance_across_bridge",
                    &call,
                )
                .await?;
            return self.execution_journal.advance(
                &operation.intent.operation_id,
                RebalanceExecutionProgress::BridgeMined {
                    origin_chain_id,
                    transaction_hash,
                    minimum_output_amount,
                    destination_balance_before,
                },
            );
        }
        let amount = match &operation.progress {
            RebalanceExecutionProgress::FundsOnBridge {
                received_base_units,
                ..
            } => *received_base_units,
            RebalanceExecutionProgress::ApprovalMined {
                chain_id,
                input_amount,
                ..
            } => {
                ensure!(
                    *chain_id == origin_chain_id,
                    "journaled Across approval uses the wrong origin chain"
                );
                if !input_amount.is_zero() {
                    *input_amount
                } else if operation.intent.direction == Direction::BinanceToWallet {
                    let record = self.wait_withdrawal(&operation).await?;
                    withdrawal_received_base_units(&record, operation.intent.token_decimals)?
                } else {
                    operation.intent.amount
                }
            }
            RebalanceExecutionProgress::IntentRecorded => operation.intent.amount,
            RebalanceExecutionProgress::BridgePrepared { .. } => unreachable!(),
            RebalanceExecutionProgress::BridgeMined { .. }
            | RebalanceExecutionProgress::AcrossFilled { .. } => return Ok(operation),
            _ => bail!("Across operation is not ready to bridge"),
        };
        let (request, mut terms, destination_chain_id, output_token) = self
            .quote_across_bridge(&operation, origin_chain_id, amount)
            .await?;
        if let Some(approval) = terms.approval.take() {
            let call =
                WalletCall::validated_contract_call(approval.target, U256::ZERO, approval.data)?;
            let hash = self
                .execute_on_chain(
                    origin_chain_id,
                    format!("{}:approval", operation.intent.operation_id),
                    "rebalance_across_approval",
                    &call,
                )
                .await?;
            if !matches!(
                operation.progress,
                RebalanceExecutionProgress::ApprovalMined { .. }
            ) {
                operation = self.execution_journal.advance(
                    &operation.intent.operation_id,
                    RebalanceExecutionProgress::ApprovalMined {
                        chain_id: origin_chain_id,
                        transaction_hash: hash,
                        input_amount: amount,
                    },
                )?;
            }
            let fresh = self.across.quote(&request).await?;
            terms = validate_quote(&request, &fresh)?;
            ensure!(
                terms.approval.is_none(),
                "Across still requires approval after mined approval"
            );
        }
        let (target, calldata, minimum_output_amount, destination_balance_before) = self
            .materialize_across_bridge_terms(&operation, destination_chain_id, output_token, terms)
            .await?;
        let call = WalletCall::validated_contract_call(target, U256::ZERO, calldata.clone())?;
        operation = self.execution_journal.advance(
            &operation.intent.operation_id,
            RebalanceExecutionProgress::BridgePrepared {
                origin_chain_id,
                input_amount: amount,
                target,
                calldata_hash: keccak256(&calldata),
                calldata,
                minimum_output_amount,
                destination_balance_before,
            },
        )?;
        let transaction_hash = self
            .execute_on_chain(
                origin_chain_id,
                format!("{}:bridge", operation.intent.operation_id),
                "rebalance_across_bridge",
                &call,
            )
            .await?;
        self.execution_journal.advance(
            &operation.intent.operation_id,
            RebalanceExecutionProgress::BridgeMined {
                origin_chain_id,
                transaction_hash,
                minimum_output_amount,
                destination_balance_before,
            },
        )
    }

    async fn quote_across_bridge(
        &self,
        operation: &RebalanceExecutionOperation,
        origin_chain_id: u64,
        amount: U256,
    ) -> anyhow::Result<(
        AcrossQuoteRequest,
        crate::across::ValidatedErc20Quote,
        u64,
        Address,
    )> {
        let amount_u128 = u128::try_from(amount).context("Across amount exceeds u128")?;
        let (destination_chain_id, input_token, output_token) = match origin_chain_id {
            OPTIMISM_CHAIN_ID => (
                WORLD_CHAIN_CHAIN_ID,
                token_on_chain(&operation.intent.token_symbol, OPTIMISM_CHAIN_ID)?,
                token_on_chain(&operation.intent.token_symbol, WORLD_CHAIN_CHAIN_ID)?,
            ),
            WORLD_CHAIN_CHAIN_ID => (
                OPTIMISM_CHAIN_ID,
                token_on_chain(&operation.intent.token_symbol, WORLD_CHAIN_CHAIN_ID)?,
                token_on_chain(&operation.intent.token_symbol, OPTIMISM_CHAIN_ID)?,
            ),
            _ => bail!("unsupported Across origin chain"),
        };
        let request = AcrossQuoteRequest {
            origin_chain_id,
            destination_chain_id,
            input_token,
            output_token,
            amount: amount_u128,
            depositor: operation.intent.wallet_owner,
            recipient: operation.intent.wallet_owner,
        };
        let quote = self.across.quote(&request).await?;
        let terms = validate_quote(&request, &quote)?;
        Ok((request, terms, destination_chain_id, output_token))
    }

    async fn materialize_across_bridge_terms(
        &self,
        operation: &RebalanceExecutionOperation,
        destination_chain_id: u64,
        output_token: Address,
        terms: crate::across::ValidatedErc20Quote,
    ) -> anyhow::Result<(Address, Vec<u8>, U256, U256)> {
        let destination_balance_before = match destination_chain_id {
            WORLD_CHAIN_CHAIN_ID => {
                self.evm
                    .rpc(WORLD_CHAIN_CHAIN_ID)?
                    .erc20_balance(output_token, operation.intent.wallet_owner)
                    .await?
            }
            OPTIMISM_CHAIN_ID => {
                self.evm
                    .rpc(OPTIMISM_CHAIN_ID)?
                    .erc20_balance(output_token, operation.intent.wallet_owner)
                    .await?
            }
            _ => unreachable!(),
        };
        Ok((
            terms.swap.target,
            terms.swap.data,
            U256::from(terms.minimum_output_amount),
            destination_balance_before,
        ))
    }

    async fn execute_on_chain(
        &mut self,
        chain_id: u64,
        operation_id: String,
        purpose: &str,
        call: &WalletCall,
    ) -> anyhow::Result<B256> {
        self.evm
            .execute(
                chain_id,
                operation_id,
                purpose,
                call,
                self.limits.operation_timeout,
            )
            .await
    }

    async fn wait_across_fill(
        &mut self,
        operation: RebalanceExecutionOperation,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        let RebalanceExecutionProgress::BridgeMined {
            origin_chain_id,
            transaction_hash,
            minimum_output_amount,
            destination_balance_before: _,
        } = operation.progress
        else {
            return Ok(operation);
        };
        let minimum =
            u128::try_from(minimum_output_amount).context("Across minimum exceeds u128")?;
        let deadline = tokio::time::Instant::now() + self.limits.operation_timeout;
        loop {
            match self
                .across
                .deposit_status(&format!("{transaction_hash:#x}"))
                .await
            {
                Ok(status)
                    if validate_deposit_status(
                        &status,
                        origin_chain_id,
                        &format!("{transaction_hash:#x}"),
                        if origin_chain_id == WORLD_CHAIN_CHAIN_ID {
                            OPTIMISM_CHAIN_ID
                        } else {
                            WORLD_CHAIN_CHAIN_ID
                        },
                        token_on_chain(
                            &operation.intent.token_symbol,
                            if origin_chain_id == WORLD_CHAIN_CHAIN_ID {
                                OPTIMISM_CHAIN_ID
                            } else {
                                WORLD_CHAIN_CHAIN_ID
                            },
                        )?,
                        minimum,
                    )? =>
                {
                    let fill_hash = B256::from_str(
                        status
                            .fill_txn_ref
                            .as_deref()
                            .context("Across fill has no transaction hash")?,
                    )?;
                    let destination_chain_id = if origin_chain_id == WORLD_CHAIN_CHAIN_ID {
                        OPTIMISM_CHAIN_ID
                    } else {
                        WORLD_CHAIN_CHAIN_ID
                    };
                    let rpc = self.evm.rpc(destination_chain_id)?;
                    let token =
                        token_on_chain(&operation.intent.token_symbol, destination_chain_id)?;
                    let receipt =
                        wait_receipt(rpc, fill_hash, self.limits.operation_timeout).await?;
                    let received = validate_across_fill_receipt(
                        &receipt,
                        fill_hash,
                        token,
                        operation.intent.wallet_owner,
                        minimum_output_amount,
                    )?;
                    return self.execution_journal.advance(
                        &operation.intent.operation_id,
                        RebalanceExecutionProgress::AcrossFilled {
                            fill_transaction_hash: fill_hash,
                            received_base_units: received,
                        },
                    );
                }
                Ok(_) | Err(_) => {}
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for Across fill"
            );
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    async fn complete_across_to_wallet(
        &mut self,
        mut operation: RebalanceExecutionOperation,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        if matches!(
            operation.progress,
            RebalanceExecutionProgress::BridgeMined { .. }
        ) {
            operation = self.wait_across_fill(operation).await?;
        }
        let RebalanceExecutionProgress::AcrossFilled {
            fill_transaction_hash,
            received_base_units,
        } = operation.progress
        else {
            return Ok(operation);
        };
        let receipt = wait_receipt(
            self.evm.rpc(WORLD_CHAIN_CHAIN_ID)?,
            fill_transaction_hash,
            self.limits.operation_timeout,
        )
        .await?;
        validate_across_fill_receipt(
            &receipt,
            fill_transaction_hash,
            operation.intent.token_contract,
            operation.intent.wallet_owner,
            received_base_units,
        )?;
        let wallet_after = self
            .evm
            .rpc(WORLD_CHAIN_CHAIN_ID)?
            .erc20_balance(
                operation.intent.token_contract,
                operation.intent.wallet_owner,
            )
            .await?;
        let binance_after = self.binance_balance(&operation).await?;
        self.execution_journal.advance(
            &operation.intent.operation_id,
            RebalanceExecutionProgress::Completed {
                binance_balance_after: binance_after,
                wallet_balance_after: wallet_after,
            },
        )
    }

    async fn finish_binance_deposit(
        &mut self,
        mut operation: RebalanceExecutionOperation,
        network: &str,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        let transaction_hash = match &operation.progress {
            RebalanceExecutionProgress::DepositTransferMined {
                transaction_hash, ..
            }
            | RebalanceExecutionProgress::DepositQuestionnaireSubmissionStarted {
                transaction_hash,
                ..
            } => Some(*transaction_hash),
            _ => None,
        };
        if let Some(transaction_hash) = transaction_hash {
            let (next_operation, deposit) = self
                .wait_binance_deposit(operation, transaction_hash, network)
                .await?;
            operation = next_operation;
            let credited = decimal_to_base_units(deposit.amount, operation.intent.token_decimals)?;
            operation = self.execution_journal.advance(
                &operation.intent.operation_id,
                RebalanceExecutionProgress::BinanceCredited {
                    deposit_id: deposit.deposit_id,
                    credited_base_units: credited,
                },
            )?;
        }
        if let RebalanceExecutionProgress::BinanceCredited {
            credited_base_units,
            ..
        } = operation.progress
        {
            let binance_after = self.binance_balance(&operation).await?;
            let expected_without_parallel_spend = operation
                .intent
                .binance_balance_before
                .checked_add(credited_base_units)
                .context("Binance balance target overflow")?;
            if binance_after < expected_without_parallel_spend {
                tracing::warn!(
                    operation_id = operation.intent.operation_id,
                    token = operation.intent.token_symbol,
                    binance_balance_after = binance_after.to_string(),
                    credited_base_units = credited_base_units.to_string(),
                    expected_without_parallel_spend = expected_without_parallel_spend.to_string(),
                    "Binance free balance is below pre-deposit balance plus credited deposit; treating Binance deposit history as settlement evidence because live trading may have consumed free balance"
                );
            }
            let wallet_chain_id = route_wallet_chain_id(&operation.intent.route);
            let wallet_after = self
                .evm
                .rpc(wallet_chain_id)?
                .erc20_balance(
                    operation.intent.token_contract,
                    operation.intent.wallet_owner,
                )
                .await?;
            operation = self.execution_journal.advance(
                &operation.intent.operation_id,
                RebalanceExecutionProgress::Completed {
                    binance_balance_after: binance_after,
                    wallet_balance_after: wallet_after,
                },
            )?;
        }
        Ok(operation)
    }

    async fn begin_master_transfer(
        &mut self,
        operation: RebalanceExecutionOperation,
        created_here: bool,
        bridge_balance_before: U256,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        let client_transaction_id = &operation.intent.withdraw_order_id;
        let existing = self
            .treasury_binance
            .universal_transfer_history(&self.subaccount_email, client_transaction_id)
            .await?;
        let transaction_id = if let Some(record) = existing.first() {
            validate_master_transfer_record(&operation, &self.subaccount_email, record)?;
            record.transaction_id
        } else {
            if !created_here {
                self.confirm_unindexed_master_transfer_absent(&operation)
                    .await?;
            }
            let amount =
                base_units_to_decimal(operation.intent.amount, operation.intent.token_decimals)?;
            let submission = self
                .treasury_binance
                .universal_transfer_from_subaccount(
                    &self.subaccount_email,
                    &operation.intent.token_symbol,
                    amount,
                    client_transaction_id,
                )
                .await;
            match submission {
                Ok(submission) => submission.transaction_id,
                Err(error) if !created_here => {
                    let indexed = self
                        .treasury_binance
                        .universal_transfer_history(&self.subaccount_email, client_transaction_id)
                        .await?;
                    let Some(record) = indexed.first() else {
                        return Err(error).context(
                            "idempotent Binance master-transfer retry failed without an indexed transfer",
                        );
                    };
                    validate_master_transfer_record(&operation, &self.subaccount_email, record)?;
                    record.transaction_id
                }
                Err(error) => return Err(error),
            }
        };
        self.execution_journal.advance(
            &operation.intent.operation_id,
            RebalanceExecutionProgress::BinanceTransferSubmitted {
                transaction_id,
                bridge_balance_before,
            },
        )
    }

    async fn confirm_unindexed_master_transfer_absent(
        &self,
        operation: &RebalanceExecutionOperation,
    ) -> anyhow::Result<()> {
        let first = self
            .observe_unindexed_master_transfer_absence(operation)
            .await?;
        tokio::time::sleep(UNKNOWN_WITHDRAWAL_ABSENCE_CONFIRMATION_DELAY).await;
        let second = self
            .observe_unindexed_master_transfer_absence(operation)
            .await?;
        ensure!(
            first.0.is_zero() && first.1.is_zero() && second.0.is_zero() && second.1.is_zero(),
            "unindexed Binance master-transfer retry found staged master inventory"
        );
        ensure!(
            first.2 >= operation.intent.amount && second.2 >= operation.intent.amount,
            "unindexed Binance master-transfer retry lacks sufficient source inventory"
        );
        tracing::warn!(
            operation_id = operation.intent.operation_id,
            token = operation.intent.token_symbol,
            client_transaction_id = operation.intent.withdraw_order_id,
            first_trading_free_base_units = first.2.to_string(),
            second_trading_free_base_units = second.2.to_string(),
            confirmation_delay_ms = UNKNOWN_WITHDRAWAL_ABSENCE_CONFIRMATION_DELAY.as_millis(),
            "proved an unindexed Binance master transfer absent; retrying its deterministic client id"
        );
        Ok(())
    }

    async fn observe_unindexed_master_transfer_absence(
        &self,
        operation: &RebalanceExecutionOperation,
    ) -> anyhow::Result<(U256, U256, U256)> {
        let (history, master, trading) = tokio::try_join!(
            self.treasury_binance.universal_transfer_history(
                &self.subaccount_email,
                &operation.intent.withdraw_order_id,
            ),
            self.treasury_binance.account_information(),
            self.trading_binance.account_information(),
        )?;
        ensure!(
            history.is_empty(),
            "unindexed Binance master-transfer retry found an indexed transfer"
        );
        let (master_free, master_locked) =
            account_asset_balance_or_zero(&master, &operation.intent.token_symbol);
        let (trading_free, _) =
            account_asset_balance_or_zero(&trading, &operation.intent.token_symbol);
        Ok((
            decimal_to_base_units(master_free, operation.intent.token_decimals)?,
            decimal_to_base_units(master_locked, operation.intent.token_decimals)?,
            decimal_to_base_units(trading_free, operation.intent.token_decimals)?,
        ))
    }

    async fn finish_master_transfer(
        &mut self,
        mut operation: RebalanceExecutionOperation,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        if let RebalanceExecutionProgress::BinanceTransferSubmitted {
            transaction_id,
            bridge_balance_before,
        } = operation.progress
        {
            let record = self
                .wait_master_transfer(&operation, transaction_id)
                .await?;
            operation = self.execution_journal.advance(
                &operation.intent.operation_id,
                RebalanceExecutionProgress::BinanceTransferCompleted {
                    transaction_id: record.transaction_id,
                    bridge_balance_before,
                },
            )?;
        }
        Ok(operation)
    }

    async fn begin_binance_withdrawal(
        &mut self,
        mut operation: RebalanceExecutionOperation,
        submission_safe: bool,
        network: &str,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        if matches!(
            operation.progress,
            RebalanceExecutionProgress::BinanceMasterReturnSubmissionStarted { .. }
                | RebalanceExecutionProgress::BinanceMasterReturnSubmitted { .. }
        ) {
            return self.resume_stale_withdrawal_cancellation(operation).await;
        }
        if matches!(
            operation.progress,
            RebalanceExecutionProgress::BinanceWithdrawalRetryAuthorized { .. }
        ) {
            return self
                .resume_authorized_withdrawal_retry(operation, network, true)
                .await;
        }
        if let RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
            api_mode,
            reconciliation_queries,
            ..
        } = &operation.progress
        {
            if api_mode == TRAVEL_RULE_REQUIRED_API_MODE {
                ensure!(
                    *reconciliation_queries == 0,
                    "Travel Rule-required withdrawal cannot carry reconciliation queries"
                );
                return self
                    .submit_required_travel_rule_withdrawal(operation, network)
                    .await;
            }
            if api_mode == TRAVEL_RULE_BINANCE_WITHDRAWAL_API_MODE {
                let requested = base_units_to_decimal(
                    operation.intent.amount,
                    operation.intent.token_decimals,
                )?;
                let existing = self
                    .treasury_binance
                    .travel_rule_withdrawal_history_v2(
                        &operation.intent.token_symbol,
                        network,
                        &operation.intent.withdraw_order_id,
                    )
                    .await?;
                for record in &existing {
                    validate_travel_rule_withdrawal_record(&operation, record, requested)?;
                }
                let viable = existing
                    .iter()
                    .filter(|record| {
                        !record.is_failed_without_broadcast()
                            && !record.is_approved_without_withdrawal()
                    })
                    .collect::<Vec<_>>();
                ensure!(
                    viable.len() <= 1,
                    "journaled Travel Rule withdrawal matched multiple viable submissions"
                );
                if let Some(record) = viable.first() {
                    return self.execution_journal.advance(
                        &operation.intent.operation_id,
                        RebalanceExecutionProgress::BinanceWithdrawalSubmitted {
                            submission_reference: record.tr_id.to_string(),
                            bridge_balance_before: match operation.progress {
                                RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                                    bridge_balance_before,
                                    ..
                                } => bridge_balance_before,
                                _ => unreachable!("the Travel Rule state was matched above"),
                            },
                        },
                    );
                }
                ensure!(
                    *reconciliation_queries <= 1,
                    "journaled Travel Rule withdrawal exceeded its reconciliation query authority"
                );
                return Box::pin(self.reconcile_unknown_withdrawal_and_retry(operation, network))
                    .await;
            }
            ensure!(
                api_mode == STANDARD_BINANCE_WITHDRAWAL_API_MODE,
                "journaled Binance withdrawal API mode is not the standard capital API"
            );
            ensure!(
                *reconciliation_queries <= 1,
                "journaled standard Binance withdrawal exceeded its reconciliation query authority"
            );
        }
        let bridge_balance_before = match &operation.progress {
            RebalanceExecutionProgress::BinanceTransferCompleted {
                bridge_balance_before,
                ..
            }
            | RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                bridge_balance_before,
                ..
            } => *bridge_balance_before,
            _ => return Ok(operation),
        };
        let existing = self
            .treasury_binance
            .withdrawal_history(
                &operation.intent.token_symbol,
                &operation.intent.withdraw_order_id,
            )
            .await?;
        let submission_reference = if let Some(record) = existing.first() {
            validate_withdrawal_record(&operation, record)?;
            record.id.clone()
        } else if let RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
            api_mode,
            reconciliation_queries,
            ..
        } = &operation.progress
        {
            ensure!(
                api_mode == STANDARD_BINANCE_WITHDRAWAL_API_MODE,
                "journaled Binance withdrawal API mode is not the standard capital API"
            );
            ensure!(
                *reconciliation_queries <= 1,
                "journaled standard Binance withdrawal exceeded its reconciliation query authority"
            );
            return self
                .reconcile_unknown_withdrawal_and_retry(operation, network)
                .await;
        } else {
            ensure!(
                submission_safe,
                "master transfer completed but no Binance withdrawal is indexed; operator review required"
            );
            operation = self.execution_journal.advance(
                &operation.intent.operation_id,
                RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                    api_mode: STANDARD_BINANCE_WITHDRAWAL_API_MODE.to_owned(),
                    bridge_balance_before,
                    reconciliation_queries: 0,
                },
            )?;
            let amount =
                base_units_to_decimal(operation.intent.amount, operation.intent.token_decimals)?;
            match self
                .submit_standard_binance_withdrawal(&operation, network, amount)
                .await
            {
                Ok(reference) => reference,
                Err(error) if is_travel_rule_required_rejection(&error) => {
                    operation = self.execution_journal.advance(
                        &operation.intent.operation_id,
                        RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                            api_mode: TRAVEL_RULE_REQUIRED_API_MODE.to_owned(),
                            bridge_balance_before,
                            reconciliation_queries: 0,
                        },
                    )?;
                    return self
                        .submit_required_travel_rule_withdrawal(operation, network)
                        .await;
                }
                Err(error) if is_terminal_binance_withdrawal_rejection(&error) => {
                    let reason = format!("terminal Binance standard withdrawal rejection: {error}");
                    self.execution_journal.advance(
                        &operation.intent.operation_id,
                        RebalanceExecutionProgress::Failed { reason },
                    )?;
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        };
        self.execution_journal.advance(
            &operation.intent.operation_id,
            RebalanceExecutionProgress::BinanceWithdrawalSubmitted {
                submission_reference,
                bridge_balance_before,
            },
        )
    }

    async fn cancel_stale_withdrawal_retry(
        &mut self,
        operation: RebalanceExecutionOperation,
        confirmation: WithdrawalAbsenceConfirmation,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        ensure!(confirmation.stale, "current withdrawal retry is not stale");
        let current_binance_balance = current_binance_balance(confirmation.evidence)?;
        let client_transaction_id =
            super::executor::stale_master_return_client_id(&operation.intent);
        let operation = self.execution_journal.advance(
            &operation.intent.operation_id,
            RebalanceExecutionProgress::BinanceMasterReturnSubmissionStarted {
                client_transaction_id,
                revalidation_binance_balance: current_binance_balance,
                revalidation_wallet_balance: confirmation.evidence.wallet_balance_base_units,
                revalidation_required_withdrawal: confirmation.required_withdrawal_base_units,
                reconciliation_queries: 0,
            },
        )?;
        self.resume_stale_withdrawal_cancellation(operation).await
    }

    async fn resume_stale_withdrawal_cancellation(
        &mut self,
        mut operation: RebalanceExecutionOperation,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        if let RebalanceExecutionProgress::BinanceMasterReturnSubmissionStarted {
            client_transaction_id,
            revalidation_binance_balance,
            revalidation_wallet_balance,
            revalidation_required_withdrawal,
            reconciliation_queries,
        } = operation.progress.clone()
        {
            ensure!(
                reconciliation_queries <= 1,
                "stale withdrawal master-return reconciliation limit exceeded"
            );
            let mut existing = self
                .treasury_binance
                .universal_transfer_history_to_subaccount(
                    &self.subaccount_email,
                    &client_transaction_id,
                )
                .await?;
            if reconciliation_queries == 0 {
                operation = self.execution_journal.advance(
                    &operation.intent.operation_id,
                    RebalanceExecutionProgress::BinanceMasterReturnSubmissionStarted {
                        client_transaction_id: client_transaction_id.clone(),
                        revalidation_binance_balance,
                        revalidation_wallet_balance,
                        revalidation_required_withdrawal,
                        reconciliation_queries: 1,
                    },
                )?;
                if existing.is_empty() {
                    tokio::time::sleep(UNKNOWN_WITHDRAWAL_ABSENCE_CONFIRMATION_DELAY).await;
                    existing = self
                        .treasury_binance
                        .universal_transfer_history_to_subaccount(
                            &self.subaccount_email,
                            &client_transaction_id,
                        )
                        .await?;
                }
            }
            let transaction_id = if let Some(record) = existing.first() {
                validate_master_return_record(
                    &operation,
                    &self.subaccount_email,
                    &client_transaction_id,
                    record,
                )?;
                record.transaction_id
            } else {
                self.verify_staged_master_inventory(&operation).await?;
                let amount = base_units_to_decimal(
                    operation.intent.amount,
                    operation.intent.token_decimals,
                )?;
                self.treasury_binance
                    .universal_transfer_to_subaccount(
                        &self.subaccount_email,
                        &operation.intent.token_symbol,
                        amount,
                        &client_transaction_id,
                    )
                    .await?
                    .transaction_id
            };
            operation = self.execution_journal.advance(
                &operation.intent.operation_id,
                RebalanceExecutionProgress::BinanceMasterReturnSubmitted {
                    client_transaction_id,
                    transaction_id,
                    revalidation_binance_balance,
                    revalidation_wallet_balance,
                    revalidation_required_withdrawal,
                },
            )?;
        }
        let (
            client_transaction_id,
            transaction_id,
            revalidation_binance_balance,
            revalidation_wallet_balance,
            revalidation_required_withdrawal,
        ) = match operation.progress.clone() {
            RebalanceExecutionProgress::BinanceMasterReturnSubmitted {
                client_transaction_id,
                transaction_id,
                revalidation_binance_balance,
                revalidation_wallet_balance,
                revalidation_required_withdrawal,
            } => (
                client_transaction_id,
                transaction_id,
                revalidation_binance_balance,
                revalidation_wallet_balance,
                revalidation_required_withdrawal,
            ),
            _ => bail!("stale withdrawal cancellation lacks a submitted master return"),
        };
        self.wait_master_return(&operation, &client_transaction_id, transaction_id)
            .await?;
        tracing::warn!(
            operation_id = operation.intent.operation_id,
            token = operation.intent.token_symbol,
            superseded_withdrawal_base_units = operation.intent.amount.to_string(),
            revalidation_required_withdrawal_base_units =
                revalidation_required_withdrawal.to_string(),
            master_return_transaction_id = transaction_id,
            "cancelled a stale proven-absent withdrawal and returned staged inventory to the trading sub-account"
        );
        self.execution_journal.advance(
            &operation.intent.operation_id,
            RebalanceExecutionProgress::CancelledStale {
                master_return_transaction_id: transaction_id,
                revalidation_binance_balance,
                revalidation_wallet_balance,
                revalidation_required_withdrawal,
            },
        )
    }

    async fn verify_staged_master_inventory(
        &self,
        operation: &RebalanceExecutionOperation,
    ) -> anyhow::Result<()> {
        let account = self.treasury_binance.account_information().await?;
        let balance = account
            .balances
            .iter()
            .find(|balance| balance.asset == operation.intent.token_symbol)
            .context("stale withdrawal asset is absent from the master account")?;
        ensure!(
            decimal_to_base_units(balance.free, operation.intent.token_decimals)?
                == operation.intent.amount,
            "stale withdrawal master return did not preserve exact free inventory"
        );
        ensure!(
            balance.locked == Decimal::ZERO,
            "stale withdrawal master return found locked master inventory"
        );
        Ok(())
    }

    async fn reconcile_unknown_withdrawal_and_retry(
        &mut self,
        mut operation: RebalanceExecutionOperation,
        network: &str,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        let (api_mode, bridge_balance_before, reconciliation_queries) = match &operation.progress {
            RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                api_mode,
                bridge_balance_before,
                reconciliation_queries,
            } => (
                api_mode.clone(),
                *bridge_balance_before,
                *reconciliation_queries,
            ),
            _ => bail!("unknown Binance withdrawal recovery lacks a submission intent"),
        };
        ensure!(
            matches!(
                api_mode.as_str(),
                STANDARD_BINANCE_WITHDRAWAL_API_MODE | TRAVEL_RULE_BINANCE_WITHDRAWAL_API_MODE
            ),
            "unknown Binance withdrawal recovery uses an unsupported API mode"
        );
        if reconciliation_queries == 0 {
            operation = self.execution_journal.advance(
                &operation.intent.operation_id,
                RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                    api_mode: api_mode.clone(),
                    bridge_balance_before,
                    reconciliation_queries: 1,
                },
            )?;
        }
        let confirmation = self
            .confirm_unknown_withdrawal_absence(&operation, network, bridge_balance_before)
            .await?;
        if confirmation.stale {
            return self
                .cancel_stale_withdrawal_retry(operation, confirmation)
                .await;
        }
        let evidence = confirmation.evidence;
        operation = self.execution_journal.advance(
            &operation.intent.operation_id,
            RebalanceExecutionProgress::BinanceWithdrawalRetryAuthorized {
                api_mode,
                bridge_balance_before,
                master_free_base_units: evidence.master_free_base_units,
                master_locked_base_units: evidence.master_locked_base_units,
                wallet_balance_base_units: evidence.wallet_balance_base_units,
            },
        )?;
        self.resume_authorized_withdrawal_retry(operation, network, false)
            .await
    }

    async fn resume_authorized_withdrawal_retry(
        &mut self,
        mut operation: RebalanceExecutionOperation,
        network: &str,
        revalidate: bool,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        let (
            api_mode,
            bridge_balance_before,
            master_free_base_units,
            master_locked_base_units,
            wallet_balance_base_units,
        ) = match &operation.progress {
            RebalanceExecutionProgress::BinanceWithdrawalRetryAuthorized {
                api_mode,
                bridge_balance_before,
                master_free_base_units,
                master_locked_base_units,
                wallet_balance_base_units,
            } => (
                api_mode.clone(),
                *bridge_balance_before,
                *master_free_base_units,
                *master_locked_base_units,
                *wallet_balance_base_units,
            ),
            _ => bail!("Binance withdrawal retry lacks durable authorization"),
        };
        if revalidate {
            let confirmation = self
                .confirm_unknown_withdrawal_absence(&operation, network, bridge_balance_before)
                .await?;
            if confirmation.stale {
                return self
                    .cancel_stale_withdrawal_retry(operation, confirmation)
                    .await;
            }
            let evidence = confirmation.evidence;
            ensure!(
                same_withdrawal_retry_authority(
                    evidence,
                    WithdrawalAbsenceEvidence {
                        master_free_base_units,
                        master_locked_base_units,
                        trading_free_base_units: evidence.trading_free_base_units,
                        trading_locked_base_units: evidence.trading_locked_base_units,
                        wallet_balance_base_units,
                    }
                ),
                "Binance withdrawal retry evidence changed after authorization"
            );
        }
        self.verify_route(&operation, true).await?;
        let ownership_proof = if api_mode == TRAVEL_RULE_BINANCE_WITHDRAWAL_API_MODE {
            Some(
                self.ensure_travel_rule_ae_self_owned(&operation, network)
                    .await?,
            )
        } else {
            None
        };
        operation = self.execution_journal.advance(
            &operation.intent.operation_id,
            RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                api_mode: api_mode.clone(),
                bridge_balance_before,
                reconciliation_queries: 0,
            },
        )?;
        let amount =
            base_units_to_decimal(operation.intent.amount, operation.intent.token_decimals)?;
        let submission_reference = if api_mode == TRAVEL_RULE_BINANCE_WITHDRAWAL_API_MODE {
            match self
                .submit_travel_rule_binance_withdrawal(
                    &operation,
                    network,
                    amount,
                    ownership_proof
                        .as_ref()
                        .context("Travel Rule withdrawal retry lost its ownership proof")?,
                )
                .await
            {
                Ok(reference) => reference,
                Err(error) if is_terminal_binance_withdrawal_rejection(&error) => {
                    let reason =
                        format!("terminal Binance Travel Rule withdrawal rejection: {error}");
                    self.execution_journal.advance(
                        &operation.intent.operation_id,
                        RebalanceExecutionProgress::Failed { reason },
                    )?;
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        } else {
            match self
                .submit_standard_binance_withdrawal(&operation, network, amount)
                .await
            {
                Ok(reference) => reference,
                Err(error) if is_travel_rule_required_rejection(&error) => {
                    operation = self.execution_journal.advance(
                        &operation.intent.operation_id,
                        RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                            api_mode: TRAVEL_RULE_REQUIRED_API_MODE.to_owned(),
                            bridge_balance_before,
                            reconciliation_queries: 0,
                        },
                    )?;
                    return self
                        .submit_required_travel_rule_withdrawal(operation, network)
                        .await;
                }
                Err(error) if is_terminal_binance_withdrawal_rejection(&error) => {
                    let reason = format!("terminal Binance standard withdrawal rejection: {error}");
                    self.execution_journal.advance(
                        &operation.intent.operation_id,
                        RebalanceExecutionProgress::Failed { reason },
                    )?;
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        };
        self.execution_journal.advance(
            &operation.intent.operation_id,
            RebalanceExecutionProgress::BinanceWithdrawalSubmitted {
                submission_reference,
                bridge_balance_before,
            },
        )
    }

    async fn confirm_unknown_withdrawal_absence(
        &self,
        operation: &RebalanceExecutionOperation,
        network: &str,
        bridge_balance_before: U256,
    ) -> anyhow::Result<WithdrawalAbsenceConfirmation> {
        let first = self
            .observe_unknown_withdrawal_absence(operation, network, bridge_balance_before)
            .await?;
        tokio::time::sleep(UNKNOWN_WITHDRAWAL_ABSENCE_CONFIRMATION_DELAY).await;
        let second = self
            .observe_unknown_withdrawal_absence(operation, network, bridge_balance_before)
            .await?;
        ensure!(
            same_withdrawal_retry_authority(first, second),
            "Binance withdrawal absence evidence changed during confirmation"
        );
        let first_required = current_required_withdrawal(first)?;
        let second_required = current_required_withdrawal(second)?;
        let first_stale = withdrawal_retry_is_stale(operation, first, first_required);
        let second_stale = withdrawal_retry_is_stale(operation, second, second_required);
        let stale = first_stale && second_stale;
        tracing::warn!(
            operation_id = operation.intent.operation_id,
            token = operation.intent.token_symbol,
            withdraw_order_id = operation.intent.withdraw_order_id,
            network,
            master_free_base_units = second.master_free_base_units.to_string(),
            master_locked_base_units = second.master_locked_base_units.to_string(),
            trading_free_base_units = second.trading_free_base_units.to_string(),
            trading_locked_base_units = second.trading_locked_base_units.to_string(),
            wallet_balance_base_units = second.wallet_balance_base_units.to_string(),
            first_required_withdrawal_base_units = first_required.to_string(),
            second_required_withdrawal_base_units = second_required.to_string(),
            durable_withdrawal_base_units = operation.intent.amount.to_string(),
            revalidation_start_balance_base_units =
                operation.intent.revalidation_start_balance.to_string(),
            stale,
            wallet_balance_changed_during_confirmation =
                first.wallet_balance_base_units != second.wallet_balance_base_units,
            confirmation_delay_ms = UNKNOWN_WITHDRAWAL_ABSENCE_CONFIRMATION_DELAY.as_millis(),
            "proved an unindexed Binance withdrawal absent and revalidated its current economic need"
        );
        Ok(WithdrawalAbsenceConfirmation {
            evidence: second,
            required_withdrawal_base_units: second_required,
            stale,
        })
    }

    async fn observe_unknown_withdrawal_absence(
        &self,
        operation: &RebalanceExecutionOperation,
        network: &str,
        bridge_balance_before: U256,
    ) -> anyhow::Result<WithdrawalAbsenceEvidence> {
        let requested =
            base_units_to_decimal(operation.intent.amount, operation.intent.token_decimals)?;
        let withdrawal_chain_id = route_withdrawal_chain_id(&operation.intent.route);
        let withdrawal_token = token_on_chain(&operation.intent.token_symbol, withdrawal_chain_id)?;
        let rpc = self.evm.rpc(withdrawal_chain_id)?;
        let (
            standard_history,
            exact_travel_rule_history,
            network_travel_rule_history,
            transfers,
            master_account,
            trading_account,
            wallet_balance,
        ) = tokio::try_join!(
            self.treasury_binance.withdrawal_history(
                &operation.intent.token_symbol,
                &operation.intent.withdraw_order_id,
            ),
            self.treasury_binance.travel_rule_withdrawal_history_v2(
                &operation.intent.token_symbol,
                network,
                &operation.intent.withdraw_order_id,
            ),
            self.treasury_binance
                .travel_rule_withdrawal_history_v2_for_network(
                    &operation.intent.token_symbol,
                    network,
                ),
            self.treasury_binance.universal_transfer_history(
                &self.subaccount_email,
                &operation.intent.withdraw_order_id,
            ),
            self.treasury_binance.account_information(),
            self.trading_binance.account_information(),
            rpc.erc20_balance(withdrawal_token, operation.intent.wallet_owner),
        )?;
        ensure!(
            standard_history.is_empty(),
            "unindexed Binance withdrawal retry found a standard withdrawal record"
        );
        let mut travel_rule_history = exact_travel_rule_history;
        for record in network_travel_rule_history.into_iter().filter(|record| {
            matches_travel_rule_record_identity_without_client_id(
                record,
                requested,
                operation.intent.wallet_owner,
                &operation.intent.withdraw_order_id,
            )
        }) {
            if !travel_rule_history
                .iter()
                .any(|existing| existing.tr_id == record.tr_id)
            {
                travel_rule_history.push(record);
            }
        }
        for record in &travel_rule_history {
            validate_travel_rule_withdrawal_record(operation, record, requested)?;
        }
        ensure!(
            travel_rule_history.iter().all(|record| {
                record.is_failed_without_broadcast() || record.is_approved_without_withdrawal()
            }),
            "unindexed Binance withdrawal retry found a viable Travel Rule submission"
        );
        ensure!(
            transfers.len() == 1,
            "unindexed Binance withdrawal retry lost unique master transfer evidence"
        );
        validate_master_transfer_record(operation, &self.subaccount_email, &transfers[0])?;
        ensure!(
            transfers[0].status == "SUCCESS",
            "unindexed Binance withdrawal retry master transfer is not successful"
        );
        let balance = master_account
            .balances
            .iter()
            .find(|balance| balance.asset == operation.intent.token_symbol)
            .context(
                "unindexed Binance withdrawal retry asset is absent from the master account",
            )?;
        let master_free_base_units =
            decimal_to_base_units(balance.free, operation.intent.token_decimals)?;
        let master_locked_base_units =
            decimal_to_base_units(balance.locked, operation.intent.token_decimals)?;
        ensure!(
            master_free_base_units == operation.intent.amount,
            "unindexed Binance withdrawal retry did not preserve the exact master balance"
        );
        ensure!(
            master_locked_base_units.is_zero(),
            "unindexed Binance withdrawal retry found locked master inventory"
        );
        let (trading_free, trading_locked) =
            account_asset_balance_or_zero(&trading_account, &operation.intent.token_symbol);
        let trading_free_base_units =
            decimal_to_base_units(trading_free, operation.intent.token_decimals)?;
        let trading_locked_base_units =
            decimal_to_base_units(trading_locked, operation.intent.token_decimals)?;
        let _ = bridge_balance_before;
        Ok(WithdrawalAbsenceEvidence {
            master_free_base_units,
            master_locked_base_units,
            trading_free_base_units,
            trading_locked_base_units,
            wallet_balance_base_units: wallet_balance,
        })
    }

    async fn submit_required_travel_rule_withdrawal(
        &mut self,
        mut operation: RebalanceExecutionOperation,
        network: &str,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        ensure!(
            matches!(
                &operation.progress,
                RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                    api_mode,
                    reconciliation_queries: 0,
                    ..
                } if api_mode == TRAVEL_RULE_REQUIRED_API_MODE
            ),
            "Travel Rule submission lacks the durable standard -4104 routing decision"
        );
        let ownership_proof = self
            .ensure_travel_rule_ae_self_owned(&operation, network)
            .await?;
        self.verify_route(&operation, true).await?;
        let bridge_balance_before = match operation.progress {
            RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                bridge_balance_before,
                ..
            } => bridge_balance_before,
            _ => unreachable!("the Travel Rule-required state was validated above"),
        };
        operation = self.execution_journal.advance(
            &operation.intent.operation_id,
            RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                api_mode: TRAVEL_RULE_BINANCE_WITHDRAWAL_API_MODE.to_owned(),
                bridge_balance_before,
                reconciliation_queries: 0,
            },
        )?;
        let amount =
            base_units_to_decimal(operation.intent.amount, operation.intent.token_decimals)?;
        let submission_reference = match self
            .submit_travel_rule_binance_withdrawal(&operation, network, amount, &ownership_proof)
            .await
        {
            Ok(reference) => reference,
            Err(error) if is_retryable_travel_rule_ownership_rejection(&error) => {
                tracing::warn!(
                    operation_id = operation.intent.operation_id,
                    token = operation.intent.token_symbol,
                    network,
                    error = %format!("{error:#}"),
                    retry_limit = MAX_TRAVEL_RULE_OWNERSHIP_REJECTION_RETRIES,
                    "Binance Travel Rule ownership rejection will be retried after absence proof"
                );
                return Box::pin(self.reconcile_unknown_withdrawal_and_retry(operation, network))
                    .await;
            }
            Err(error) if is_terminal_binance_withdrawal_rejection(&error) => {
                let reason = format!("terminal Binance Travel Rule withdrawal rejection: {error}");
                self.execution_journal.advance(
                    &operation.intent.operation_id,
                    RebalanceExecutionProgress::Failed { reason },
                )?;
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        self.execution_journal.advance(
            &operation.intent.operation_id,
            RebalanceExecutionProgress::BinanceWithdrawalSubmitted {
                submission_reference,
                bridge_balance_before,
            },
        )
    }

    async fn ensure_travel_rule_ae_self_owned(
        &self,
        operation: &RebalanceExecutionOperation,
        network: &str,
    ) -> anyhow::Result<TravelRuleAddressOwnershipProof> {
        let (requirements, records) = tokio::try_join!(
            self.treasury_binance
                .travel_rule_questionnaire_requirements(),
            self.treasury_binance.address_verification_list(),
        )?;
        ensure!(
            requirements.questionnaire_country_code.as_deref() == Some("AE"),
            "Binance Travel Rule questionnaire is not the reviewed AE self-owned-wallet form"
        );
        let wallet = format!("{:#x}", operation.intent.wallet_owner);
        let matching = records
            .iter()
            .filter(|record| verified_self_owned_evm_address_record(record, &wallet))
            .collect::<Vec<_>>();
        let record = matching.first().context(
            "Binance Travel Rule ownership verification is absent for the exact wallet and network",
        )?;
        tracing::info!(
            token = operation.intent.token_symbol,
            network,
            equivalent_verified_record_count = matching.len(),
            "selected one equivalent Binance Travel Rule self-owned-wallet proof"
        );
        Ok(TravelRuleAddressOwnershipProof {
            // Binance verifies ownership of an EVM address on a network. The
            // questionnaire's token answer belongs to this withdrawal, not to
            // whichever coin was used to create the reusable address proof.
            satoshi_token: operation.intent.token_symbol.clone(),
            verify_method: record
                .address_questionnaire
                .verify_method
                .expect("the matching verified record requires method 1"),
        })
    }

    async fn wait_master_transfer(
        &mut self,
        operation: &RebalanceExecutionOperation,
        transaction_id: u64,
    ) -> anyhow::Result<UniversalTransferRecord> {
        let deadline = tokio::time::Instant::now() + self.limits.operation_timeout;
        loop {
            if let Some(record) = self
                .treasury_binance
                .universal_transfer_history(
                    &self.subaccount_email,
                    &operation.intent.withdraw_order_id,
                )
                .await?
                .into_iter()
                .next()
            {
                validate_master_transfer_record(operation, &self.subaccount_email, &record)?;
                ensure!(
                    record.transaction_id == transaction_id,
                    "Binance master transfer id changed"
                );
                match record.status.as_str() {
                    "SUCCESS" => return Ok(record),
                    "FAILED" | "FAILURE" => {
                        self.execution_journal.advance(
                            &operation.intent.operation_id,
                            RebalanceExecutionProgress::Failed {
                                reason: format!(
                                    "Binance master transfer terminal status {}",
                                    record.status
                                ),
                            },
                        )?;
                        bail!(
                            "Binance master transfer failed with status {}",
                            record.status
                        );
                    }
                    _ => {}
                }
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for Binance master transfer"
            );
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    async fn wait_master_return(
        &self,
        operation: &RebalanceExecutionOperation,
        client_transaction_id: &str,
        transaction_id: u64,
    ) -> anyhow::Result<UniversalTransferRecord> {
        let deadline = tokio::time::Instant::now() + self.limits.operation_timeout;
        loop {
            if let Some(record) = self
                .treasury_binance
                .universal_transfer_history_to_subaccount(
                    &self.subaccount_email,
                    client_transaction_id,
                )
                .await?
                .into_iter()
                .next()
            {
                validate_master_return_record(
                    operation,
                    &self.subaccount_email,
                    client_transaction_id,
                    &record,
                )?;
                ensure!(
                    record.transaction_id == transaction_id,
                    "Binance master-return transfer id changed"
                );
                match record.status.as_str() {
                    "SUCCESS" => return Ok(record),
                    "FAILED" | "FAILURE" => bail!(
                        "Binance master-return transfer failed with status {}",
                        record.status
                    ),
                    _ => {}
                }
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for Binance master-return transfer"
            );
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    async fn submit_standard_binance_withdrawal(
        &self,
        operation: &RebalanceExecutionOperation,
        network: &str,
        amount: Decimal,
    ) -> anyhow::Result<String> {
        let address = format!("{:#x}", operation.intent.wallet_owner);
        let submission = self
            .treasury_binance
            .withdraw_standard(
                &operation.intent.token_symbol,
                network,
                &address,
                amount,
                &operation.intent.withdraw_order_id,
            )
            .await?;
        Ok(submission.id)
    }

    async fn submit_travel_rule_binance_withdrawal(
        &self,
        operation: &RebalanceExecutionOperation,
        network: &str,
        amount: Decimal,
        ownership_proof: &TravelRuleAddressOwnershipProof,
    ) -> anyhow::Result<String> {
        let address = format!("{:#x}", operation.intent.wallet_owner);
        let submission = self
            .treasury_binance
            .withdraw_travel_rule_ae_self_owned(
                &operation.intent.token_symbol,
                network,
                &address,
                amount,
                &operation.intent.withdraw_order_id,
                ownership_proof,
            )
            .await?;
        Ok(submission.tr_id)
    }

    async fn wait_withdrawal(
        &mut self,
        operation: &RebalanceExecutionOperation,
    ) -> anyhow::Result<WithdrawalRecord> {
        let deadline = tokio::time::Instant::now() + self.limits.operation_timeout;
        loop {
            if let Some(record) = self
                .treasury_binance
                .withdrawal_history(
                    &operation.intent.token_symbol,
                    &operation.intent.withdraw_order_id,
                )
                .await?
                .into_iter()
                .next()
            {
                validate_withdrawal_record(operation, &record)?;
                match record.status {
                    6 if !record.tx_id.is_empty() => return Ok(record),
                    1 | 3 | 5 => {
                        self.execution_journal.advance(
                            &operation.intent.operation_id,
                            RebalanceExecutionProgress::Failed {
                                reason: format!(
                                    "Binance withdrawal terminal status {}",
                                    record.status
                                ),
                            },
                        )?;
                        bail!("Binance withdrawal failed with status {}", record.status);
                    }
                    _ => {}
                }
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for Binance withdrawal"
            );
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    async fn wait_binance_deposit(
        &mut self,
        mut operation: RebalanceExecutionOperation,
        transaction_hash: B256,
        network: &str,
    ) -> anyhow::Result<(RebalanceExecutionOperation, DepositRecord)> {
        let transaction_hash_text = format!("{transaction_hash:#x}");
        let deadline = tokio::time::Instant::now() + self.limits.operation_timeout;
        loop {
            if let Some(record) = self
                .trading_binance
                .deposit_history(&operation.intent.token_symbol, &transaction_hash_text)
                .await?
                .into_iter()
                .next()
            {
                ensure!(
                    record.network == network,
                    "Binance credited deposit on a different network"
                );
                if record.questionnaire_required() {
                    if let RebalanceExecutionProgress::DepositQuestionnaireSubmissionStarted {
                        deposit_id,
                        ..
                    } = &operation.progress
                    {
                        ensure!(
                            deposit_id == &record.deposit_id,
                            "Binance changed the deposit id after questionnaire submission started"
                        );
                    } else {
                        let chain_id = route_wallet_chain_id(&operation.intent.route);
                        operation = self.execution_journal.advance(
                            &operation.intent.operation_id,
                            RebalanceExecutionProgress::DepositQuestionnaireSubmissionStarted {
                                chain_id,
                                transaction_hash,
                                deposit_id: record.deposit_id.clone(),
                            },
                        )?;
                        let submission = self
                            .trading_binance
                            .submit_deposit_questionnaire(&record.deposit_id)
                            .await?;
                        ensure!(
                            submission.accepted,
                            "Binance rejected deposit questionnaire: {}",
                            submission.info
                        );
                    }
                }
                // Match the Rails contract: inspect and submit the deposit
                // questionnaire first when Binance requests it, then accept
                // either credited state from the same observation. Status 6
                // means the funds are credited even if withdrawal remains
                // temporarily locked.
                if record.is_credited() {
                    return Ok((operation, record));
                }
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for Binance deposit credit"
            );
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    async fn wait_token_credit(
        &self,
        rpc: &JsonRpcClient,
        token: Address,
        owner: Address,
        before: U256,
        expected_delta: U256,
    ) -> anyhow::Result<U256> {
        let expected = before
            .checked_add(expected_delta)
            .context("token credit target overflow")?;
        let deadline = tokio::time::Instant::now() + self.limits.operation_timeout;
        loop {
            let balance = rpc.erc20_balance(token, owner).await?;
            if balance >= expected {
                return Ok(balance);
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for token credit"
            );
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    async fn wait_direct_withdrawal_credit(
        &self,
        rpc: &JsonRpcClient,
        token: Address,
        owner: Address,
        transaction_id: &str,
        expected_delta: U256,
    ) -> anyhow::Result<U256> {
        let transaction_hash = B256::from_str(transaction_id)
            .context("Binance withdrawal transaction id is not an EVM hash")?;
        let receipt = wait_receipt(rpc, transaction_hash, self.limits.operation_timeout).await?;
        validate_direct_withdrawal_receipt(
            &receipt,
            transaction_hash,
            token,
            owner,
            expected_delta,
        )?;
        rpc.erc20_balance(token, owner).await
    }

    async fn verify_route(
        &self,
        operation: &RebalanceExecutionOperation,
        withdrawal: bool,
    ) -> anyhow::Result<()> {
        let (network, direct_network) = match &operation.intent.route {
            Route::Direct {
                binance_network, ..
            } => (binance_network.as_str(), binance_network.as_str()),
            Route::Across {
                binance_network, ..
            } => (binance_network.as_str(), "WLD"),
        };
        let coins = if withdrawal {
            self.treasury_binance.all_coin_information().await?
        } else {
            self.trading_binance.all_coin_information().await?
        };
        let capital = select_capital_routes(
            &coins,
            &operation.intent.token_symbol,
            direct_network,
            "OPTIMISM",
        )?;
        let selected = capital
            .direct
            .as_ref()
            .filter(|candidate| candidate.network == network)
            .or_else(|| {
                capital
                    .fallback
                    .as_ref()
                    .filter(|candidate| candidate.network == network)
            })
            .context("pinned rebalance route disappeared")?;
        ensure!(
            if withdrawal {
                capital.withdrawal_all_enabled && selected.withdrawal_available()
            } else {
                capital.deposit_all_enabled && selected.deposit_available()
            },
            "pinned rebalance route is unavailable"
        );
        if withdrawal {
            let amount =
                base_units_to_decimal(operation.intent.amount, operation.intent.token_decimals)?;
            ensure!(
                amount >= selected.withdraw_min && amount <= selected.withdraw_max,
                "rebalance withdrawal is outside live limits"
            );
            ensure!(
                decimal_to_base_units(amount, operation.intent.token_decimals)?
                    % decimal_to_base_units(
                        selected.withdraw_integer_multiple,
                        operation.intent.token_decimals
                    )?
                    .max(U256::ONE)
                    == U256::ZERO,
                "rebalance withdrawal violates live integer multiple"
            );
            if operation
                .intent
                .scope
                .as_ref()
                .is_some_and(|scope| scope.network_id == "chain:42161")
            {
                let authorized_fee = operation
                    .intent
                    .maximum_fee_base_units
                    .as_deref()
                    .context("rebalance withdrawal has no durable fee authority")
                    .and_then(|value| {
                        U256::from_str(value).context("rebalance fee authority is not a uint256")
                    })?;
                let current_fee =
                    decimal_to_base_units(selected.withdraw_fee, operation.intent.token_decimals)?;
                ensure!(
                    current_fee <= authorized_fee,
                    "live Binance withdrawal fee exceeds rebalance durable authority"
                );
            }
        }
        Ok(())
    }

    async fn binance_balance(
        &self,
        operation: &RebalanceExecutionOperation,
    ) -> anyhow::Result<U256> {
        let account = self.trading_binance.account_information().await?;
        let balance = account
            .balances
            .iter()
            .find(|balance| balance.asset == operation.intent.token_symbol)
            .map_or(Decimal::ZERO, |balance| balance.free);
        decimal_to_base_units_floor(balance, operation.intent.token_decimals)
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_wallet_call(
    rpc: &JsonRpcClient,
    wallet: &EvmWallet,
    nonce_lane: &mut NonceLane,
    journal: &mut TransactionJournal,
    operation_id: String,
    purpose: &str,
    call: &WalletCall,
    timeout: Duration,
) -> anyhow::Result<B256> {
    if let Some(existing) = journal.operation(&operation_id) {
        ensure!(
            existing.intent.identity.chain_id == nonce_lane.chain_id()
                && existing.intent.identity.wallet == wallet.address()
                && existing.intent.purpose == purpose
                && existing.intent.target == call.target()
                && existing.intent.native_value == call.value()
                && existing.intent.calldata_hash == keccak256(call.calldata()),
            "journaled rebalance transaction intent does not match the requested call"
        );
        return match existing.status {
            JournalStatus::MinedSuccess {
                transaction_hash, ..
            } => Ok(transaction_hash),
            JournalStatus::MinedReverted { .. } => {
                bail!("journaled rebalance transaction reverted")
            }
            JournalStatus::CancelledBeforeSigning
            | JournalStatus::RejectedBeforeBroadcast { .. } => {
                bail!("journaled rebalance transaction was cancelled")
            }
            _ => bail!("journaled rebalance transaction still requires recovery"),
        };
    }
    ensure!(nonce_lane.ready(), "rebalance nonce lane is not ready");
    let rpc_call = call.rpc_call(wallet.address());
    rpc.simulate_transaction(&rpc_call).await?;
    let estimate = rpc.estimate_gas(&rpc_call).await?;
    let gas_limit = estimate
        .checked_mul(GAS_LIMIT_MARGIN_NUMERATOR)
        .and_then(|value| value.checked_add(GAS_LIMIT_MARGIN_DENOMINATOR - 1))
        .map(|value| value / GAS_LIMIT_MARGIN_DENOMINATOR)
        .context("rebalance gas margin overflow")?;
    ensure!(
        gas_limit > 0 && gas_limit <= MAX_ERC20_GAS_LIMIT,
        "rebalance gas estimate exceeds cap"
    );
    let gas_price = rpc.gas_price().await?;
    let max_fee_per_gas = gas_price.checked_mul(2).context("rebalance fee overflow")?;
    ensure!(
        max_fee_per_gas > 0 && max_fee_per_gas <= MAX_FEE_PER_GAS_WEI,
        "rebalance fee exceeds cap"
    );
    let fee_parameters = WalletTransactionParameters {
        chain_id: nonce_lane.chain_id(),
        nonce: 0,
        gas_limit,
        max_fee_per_gas,
        max_priority_fee_per_gas: gas_price.min(max_fee_per_gas),
    };
    let maximum_cost = call.maximum_native_cost(fee_parameters)?;
    ensure!(
        rpc.native_balance(wallet.address()).await? >= maximum_cost,
        "wallet native balance cannot cover rebalance gas"
    );
    let mut nonce_guard = acquire_process_nonce_lock(
        nonce_lane.chain_id(),
        wallet.address(),
        nonce_lane
            .next_nonce()
            .context("ready nonce lane has no nonce")?,
    )
    .await?;
    let identity =
        nonce_lane.reserve_with_nonce(journal, operation_id, purpose, call, nonce_guard.nonce())?;
    let signed = match wallet.sign_call(
        call,
        WalletTransactionParameters {
            nonce: identity.nonce,
            ..fee_parameters
        },
    ) {
        Ok(signed) => signed,
        Err(error) => {
            nonce_lane.cancel_before_signing(journal)?;
            return Err(error);
        }
    };
    nonce_lane.record_signed(journal, &signed)?;
    let submitted = match tokio::time::timeout(
        PROCESS_NONCE_LOCK_TTL,
        broadcast_signed_transaction(rpc, &signed),
    )
    .await
    {
        Ok(Ok(hash)) => hash,
        Ok(Err(error)) => {
            let reason = if error.to_string().starts_with("JSON-RPC error") {
                UnknownOutcomeReason::BroadcastRejected
            } else {
                UnknownOutcomeReason::BroadcastTransport
            };
            nonce_lane.record_unknown_outcome(journal, reason)?;
            return Err(error);
        }
        Err(_elapsed) => {
            nonce_lane.record_unknown_outcome(journal, UnknownOutcomeReason::BroadcastTransport)?;
            bail!("rebalance wallet transaction broadcast timed out while holding nonce lock");
        }
    };
    nonce_lane.record_broadcast(journal, submitted)?;
    nonce_guard.advance_after_broadcast(identity.nonce)?;
    drop(nonce_guard);
    let receipt = match wait_receipt(rpc, submitted, timeout).await {
        Ok(receipt) => receipt,
        Err(error) => return Err(error),
    };
    nonce_lane.record_receipt(journal, receipt.clone())?;
    ensure!(receipt.status == 1, "rebalance wallet transaction reverted");
    Ok(submitted)
}

async fn finish_known_pending_recovery(
    rpc: &JsonRpcClient,
    journal: &mut TransactionJournal,
    reconciled: crate::wallet::ReconciledNonceLane,
    timeout: Duration,
) -> anyhow::Result<NonceLane> {
    let outcome_label = reconciled.outcome.label();
    let mut lane = reconciled.lane;
    if let NonceReconciliationOutcome::TransactionKnown {
        transaction_hash, ..
    } = reconciled.outcome
    {
        let receipt = wait_receipt(rpc, transaction_hash, timeout).await?;
        lane.record_receipt(journal, receipt)?;
    }
    ensure!(
        lane.ready(),
        "wallet nonce lane requires recovery ({outcome_label})"
    );
    Ok(lane)
}

async fn wait_receipt(
    rpc: &JsonRpcClient,
    transaction_hash: B256,
    timeout: Duration,
) -> anyhow::Result<TransactionReceipt> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(receipt) = rpc.transaction_receipt(transaction_hash).await? {
            return Ok(receipt);
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for rebalance transaction receipt"
        );
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

fn validate_direct_withdrawal_receipt(
    receipt: &TransactionReceipt,
    expected_hash: B256,
    token: Address,
    owner: Address,
    expected_delta: U256,
) -> anyhow::Result<()> {
    let received = erc20_credit_from_receipt(receipt, expected_hash, token, owner)?;
    ensure!(
        received >= expected_delta,
        "withdrawal receipt did not transfer the expected token amount to the wallet"
    );
    Ok(())
}

fn validate_across_fill_receipt(
    receipt: &TransactionReceipt,
    expected_hash: B256,
    token: Address,
    owner: Address,
    minimum_output: U256,
) -> anyhow::Result<U256> {
    let received = erc20_credit_from_receipt(receipt, expected_hash, token, owner)?;
    ensure!(
        received >= minimum_output,
        "Across fill receipt did not transfer the minimum token amount to the wallet"
    );
    Ok(received)
}

fn erc20_credit_from_receipt(
    receipt: &TransactionReceipt,
    expected_hash: B256,
    token: Address,
    owner: Address,
) -> anyhow::Result<U256> {
    ensure!(
        receipt.transaction_hash == expected_hash,
        "credit receipt transaction hash changed"
    );
    ensure!(receipt.status == 1, "credit transaction reverted");

    let transfer_topic = keccak256("Transfer(address,address,uint256)");
    let mut received = U256::ZERO;
    for log in receipt
        .logs
        .iter()
        .filter(|log| log.address == token && log.topics.first() == Some(&transfer_topic))
    {
        ensure!(
            log.topics.len() == 3,
            "credit ERC-20 Transfer log has wrong topics"
        );
        ensure!(
            log.data.len() == 32,
            "credit ERC-20 Transfer log amount is not one word"
        );
        let recipient = Address::from_slice(&log.topics[2].as_slice()[12..]);
        if recipient == owner {
            received = received
                .checked_add(U256::from_be_slice(&log.data))
                .context("credit ERC-20 transfer sum overflow")?;
        }
    }
    Ok(received)
}

fn validate_withdrawal_record(
    operation: &RebalanceExecutionOperation,
    record: &WithdrawalRecord,
) -> anyhow::Result<()> {
    let expected_network = match &operation.intent.route {
        Route::Direct {
            binance_network, ..
        }
        | Route::Across {
            binance_network, ..
        } => binance_network,
    };
    ensure!(
        record.coin == operation.intent.token_symbol,
        "Binance withdrawal coin changed"
    );
    ensure!(
        record.network == *expected_network,
        "Binance withdrawal network changed"
    );
    ensure!(
        record.withdraw_order_id == operation.intent.withdraw_order_id,
        "Binance withdrawal client id changed"
    );
    ensure!(
        record
            .address
            .eq_ignore_ascii_case(&format!("{:#x}", operation.intent.wallet_owner)),
        "Binance withdrawal destination changed"
    );
    ensure!(
        withdrawal_requested_base_units(record, operation.intent.token_decimals)?
            == operation.intent.amount,
        "Binance withdrawal amount plus fee changed"
    );
    Ok(())
}

fn validate_master_transfer_record(
    operation: &RebalanceExecutionOperation,
    subaccount_email: &str,
    record: &UniversalTransferRecord,
) -> anyhow::Result<()> {
    ensure!(
        record.from_email.eq_ignore_ascii_case(subaccount_email),
        "Binance master transfer source sub-account changed"
    );
    ensure!(
        !record.to_email.trim().is_empty(),
        "Binance master transfer destination is empty"
    );
    ensure!(
        record.asset == operation.intent.token_symbol,
        "Binance master transfer asset changed"
    );
    ensure!(
        record.from_account_type == "SPOT" && record.to_account_type == "SPOT",
        "Binance master transfer account type changed"
    );
    ensure!(
        record.client_transaction_id == operation.intent.withdraw_order_id,
        "Binance master transfer client id changed"
    );
    ensure!(
        decimal_to_base_units(record.amount, operation.intent.token_decimals)?
            == operation.intent.amount,
        "Binance master transfer amount changed"
    );
    Ok(())
}

fn validate_master_return_record(
    operation: &RebalanceExecutionOperation,
    subaccount_email: &str,
    client_transaction_id: &str,
    record: &UniversalTransferRecord,
) -> anyhow::Result<()> {
    ensure!(
        !record.from_email.trim().is_empty(),
        "Binance master-return source account is empty"
    );
    ensure!(
        record.to_email.eq_ignore_ascii_case(subaccount_email),
        "Binance master-return destination sub-account changed"
    );
    ensure!(
        record.asset == operation.intent.token_symbol,
        "Binance master-return asset changed"
    );
    ensure!(
        record.from_account_type == "SPOT" && record.to_account_type == "SPOT",
        "Binance master-return account type changed"
    );
    ensure!(
        record.client_transaction_id == client_transaction_id,
        "Binance master-return client id changed"
    );
    ensure!(
        decimal_to_base_units(record.amount, operation.intent.token_decimals)?
            == operation.intent.amount,
        "Binance master-return amount changed"
    );
    Ok(())
}

fn current_binance_balance(evidence: WithdrawalAbsenceEvidence) -> anyhow::Result<U256> {
    evidence
        .master_free_base_units
        .checked_add(evidence.master_locked_base_units)
        .and_then(|balance| balance.checked_add(evidence.trading_free_base_units))
        .and_then(|balance| balance.checked_add(evidence.trading_locked_base_units))
        .context("current aggregate Binance balance overflow")
}

fn current_required_withdrawal(evidence: WithdrawalAbsenceEvidence) -> anyhow::Result<U256> {
    let binance = current_binance_balance(evidence)?;
    let total = binance
        .checked_add(evidence.wallet_balance_base_units)
        .context("current rebalance inventory overflow")?;
    let wallet_target = total
        .checked_add(U256::ONE)
        .context("current rebalance midpoint overflow")?
        / U256::from(2);
    Ok(wallet_target
        .checked_sub(evidence.wallet_balance_base_units)
        .unwrap_or(U256::ZERO))
}

fn withdrawal_retry_is_stale(
    operation: &RebalanceExecutionOperation,
    evidence: WithdrawalAbsenceEvidence,
    required_withdrawal: U256,
) -> bool {
    required_withdrawal < operation.intent.amount
        || (!operation.intent.revalidation_start_balance.is_zero()
            && evidence.wallet_balance_base_units >= operation.intent.revalidation_start_balance)
}

fn validate_master_subaccount_view(
    trading_account: &AccountInformation,
    master_balances: &[SubAccountAssetBalance],
) -> anyhow::Result<()> {
    for asset in ["ESP", "USDC", "WLD"] {
        let (trading_free, trading_locked) = account_asset_balance_or_zero(trading_account, asset);
        let master = master_balances
            .iter()
            .find(|balance| balance.asset == asset);
        let master_free = master.map_or(Decimal::ZERO, |balance| balance.free);
        let master_locked = master.map_or(Decimal::ZERO, |balance| balance.locked);
        ensure!(
            trading_free == master_free && trading_locked == master_locked,
            "Binance master key does not resolve to the configured trading sub-account"
        );
    }
    Ok(())
}

fn is_terminal_binance_withdrawal_rejection(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<BinanceApiError>()
        .is_some_and(BinanceApiError::is_known_pre_submission_withdrawal_rejection)
}

fn is_travel_rule_required_rejection(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<BinanceApiError>()
        .is_some_and(BinanceApiError::is_travel_rule_required_withdrawal_rejection)
}

fn is_retryable_travel_rule_ownership_rejection(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<BinanceApiError>()
        .is_some_and(BinanceApiError::is_retryable_travel_rule_ownership_withdrawal_rejection)
}

fn same_withdrawal_retry_authority(
    left: WithdrawalAbsenceEvidence,
    right: WithdrawalAbsenceEvidence,
) -> bool {
    left.master_free_base_units == right.master_free_base_units
        && left.master_locked_base_units == right.master_locked_base_units
}

fn verified_self_owned_evm_address_record(
    record: &AddressVerificationRecord,
    wallet: &str,
) -> bool {
    record.wallet_address.eq_ignore_ascii_case(wallet)
        && record.status == "VERIFIED"
        && record.address_questionnaire.is_address_owner == Some(1)
        && record.address_questionnaire.verify_method == Some(1)
}

fn validate_travel_rule_withdrawal_record(
    operation: &RebalanceExecutionOperation,
    record: &TravelRuleWithdrawalRecord,
    requested: Decimal,
) -> anyhow::Result<()> {
    ensure!(
        record.tr_id > 0
            && record.coin == operation.intent.token_symbol
            && (record.network.is_empty()
                || matches!(
                    &operation.intent.route,
                    Route::Direct {
                        binance_network,
                        ..
                    } | Route::Across {
                        binance_network,
                        ..
                    } if record.network == *binance_network
                ))
            && matches_travel_rule_record_identity_without_client_id(
                record,
                requested,
                operation.intent.wallet_owner,
                &operation.intent.withdraw_order_id,
            ),
        "Binance Travel Rule withdrawal record differs from the durable intent"
    );
    Ok(())
}

fn account_asset_balance_or_zero(account: &AccountInformation, asset: &str) -> (Decimal, Decimal) {
    account
        .balances
        .iter()
        .find(|balance| balance.asset == asset)
        .map_or((Decimal::ZERO, Decimal::ZERO), |balance| {
            (balance.free, balance.locked)
        })
}

fn withdrawal_received_base_units(record: &WithdrawalRecord, decimals: u8) -> anyhow::Result<U256> {
    ensure!(record.amount > Decimal::ZERO, "withdrawal receipt is zero");
    decimal_to_base_units(record.amount, decimals)
}

fn withdrawal_requested_base_units(
    record: &WithdrawalRecord,
    decimals: u8,
) -> anyhow::Result<U256> {
    ensure!(
        record.transaction_fee >= Decimal::ZERO,
        "withdrawal fee is negative"
    );
    let requested = record
        .amount
        .checked_add(record.transaction_fee)
        .context("withdrawal amount plus fee overflow")?;
    decimal_to_base_units(requested, decimals)
}

fn token_on_chain(symbol: &str, chain_id: u64) -> anyhow::Result<Address> {
    match (symbol, chain_id) {
        ("ESP", ARBITRUM_CHAIN_ID) => {
            Address::from_str(ARBITRUM_ESP).context("approved Arbitrum ESP address is invalid")
        }
        ("ARB", ARBITRUM_CHAIN_ID) => {
            Address::from_str(ARBITRUM_ARB).context("approved Arbitrum ARB address is invalid")
        }
        ("USDC", ARBITRUM_CHAIN_ID) => {
            Address::from_str(ARBITRUM_USDC).context("approved Arbitrum USDC address is invalid")
        }
        ("USDC", OPTIMISM_CHAIN_ID) => Ok(OPTIMISM_USDC),
        ("USDC", WORLD_CHAIN_CHAIN_ID) => Ok(WORLD_CHAIN_USDC),
        ("WLD", OPTIMISM_CHAIN_ID) => Ok(OPTIMISM_WLD),
        ("WLD", WORLD_CHAIN_CHAIN_ID) => Ok(WORLD_CHAIN_WLD),
        _ => bail!("unsupported rebalance token or chain"),
    }
}

fn route_wallet_chain_id(route: &Route) -> u64 {
    match route {
        Route::Direct { chain_id, .. } => *chain_id,
        Route::Across {
            wallet_chain_id, ..
        } => *wallet_chain_id,
    }
}

fn route_withdrawal_chain_id(route: &Route) -> u64 {
    match route {
        Route::Direct { chain_id, .. } => *chain_id,
        Route::Across {
            bridge_chain_id, ..
        } => *bridge_chain_id,
    }
}

fn validate_approved_asset(
    symbol: &str,
    decimals: u8,
    contract: Address,
    chain_id: u64,
) -> anyhow::Result<()> {
    let (expected_decimals, expected_contract) = match (symbol, chain_id) {
        ("WLD", WORLD_CHAIN_CHAIN_ID) => (18, WORLD_CHAIN_WLD),
        ("USDC", WORLD_CHAIN_CHAIN_ID) => (6, WORLD_CHAIN_USDC),
        ("USDC", ARBITRUM_CHAIN_ID) => (
            6,
            Address::from_str(ARBITRUM_USDC)
                .context("approved Arbitrum USDC address is invalid")?,
        ),
        ("ESP", ARBITRUM_CHAIN_ID) => (
            18,
            Address::from_str(ARBITRUM_ESP).context("approved Arbitrum ESP address is invalid")?,
        ),
        ("ARB", ARBITRUM_CHAIN_ID) => (
            18,
            Address::from_str(ARBITRUM_ARB).context("approved Arbitrum ARB address is invalid")?,
        ),
        _ => bail!("rebalance token is not approved on chain {chain_id}"),
    };
    ensure!(
        decimals == expected_decimals && contract == expected_contract,
        "rebalance token metadata differs from the approved chain asset"
    );
    Ok(())
}

fn decimal_to_base_units(value: Decimal, decimals: u8) -> anyhow::Result<U256> {
    ensure!(value >= Decimal::ZERO, "decimal amount is negative");
    let mantissa = value.mantissa();
    ensure!(mantissa >= 0, "decimal mantissa is negative");
    let numerator = U256::from(mantissa as u128)
        .checked_mul(pow10(decimals.into())?)
        .context("decimal base-unit overflow")?;
    let denominator = pow10(value.scale())?;
    ensure!(
        numerator % denominator == U256::ZERO,
        "decimal exceeds token precision"
    );
    Ok(numerator / denominator)
}

fn decimal_to_base_units_floor(value: Decimal, decimals: u8) -> anyhow::Result<U256> {
    ensure!(value >= Decimal::ZERO, "decimal balance is negative");
    let mantissa = value.mantissa();
    ensure!(mantissa >= 0, "decimal balance mantissa is negative");
    let numerator = U256::from(mantissa as u128)
        .checked_mul(pow10(decimals.into())?)
        .context("decimal balance base-unit overflow")?;
    Ok(numerator / pow10(value.scale())?)
}

pub fn rebalance_decimal_to_base_units_floor(value: Decimal, decimals: u8) -> anyhow::Result<U256> {
    decimal_to_base_units_floor(value, decimals)
}

pub fn rebalance_base_units_to_decimal(value: U256, decimals: u8) -> anyhow::Result<Decimal> {
    base_units_to_decimal(value, decimals)
}

fn base_units_to_decimal(value: U256, decimals: u8) -> anyhow::Result<Decimal> {
    ensure!(decimals <= 28, "Decimal cannot represent token precision");
    let digits = value.to_string();
    let encoded = if decimals == 0 {
        digits
    } else if digits.len() <= usize::from(decimals) {
        format!(
            "0.{}{}",
            "0".repeat(usize::from(decimals) - digits.len()),
            digits
        )
    } else {
        let split = digits.len() - usize::from(decimals);
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    Decimal::from_str_exact(&encoded).context("base-unit amount exceeds Decimal representation")
}

fn pow10(exponent: u32) -> anyhow::Result<U256> {
    let mut result = U256::ONE;
    for _ in 0..exponent {
        result = result
            .checked_mul(U256::from(10))
            .context("decimal scale overflow")?;
    }
    Ok(result)
}

fn matches_travel_rule_record_identity_without_client_id(
    record: &TravelRuleWithdrawalRecord,
    requested: Decimal,
    wallet_owner: Address,
    withdraw_order_id: &str,
) -> bool {
    let amount = Decimal::from_str(&record.amount).ok();
    let exact_debit = amount.is_some_and(|amount| {
        amount == requested
            || Decimal::from_str(&record.transaction_fee)
                .ok()
                .and_then(|fee| amount.checked_add(fee))
                == Some(requested)
    });
    exact_debit
        && record
            .address
            .eq_ignore_ascii_case(&format!("{wallet_owner:#x}"))
        && (record.withdraw_order_id.is_empty() || record.withdraw_order_id == withdraw_order_id)
}

fn merge_travel_rule_withdrawal_detail(
    record: &mut TravelRuleWithdrawalRecord,
    detailed: &TravelRuleWithdrawalRecord,
) -> anyhow::Result<()> {
    ensure!(
        record.tr_id == detailed.tr_id,
        "Travel Rule trId detail changed identity"
    );
    ensure!(
        record.coin == detailed.coin,
        "Travel Rule trId detail changed asset"
    );
    ensure!(
        record.network.is_empty()
            || detailed.network.is_empty()
            || record.network == detailed.network,
        "Travel Rule trId detail changed network"
    );
    ensure!(
        record.address.is_empty()
            || detailed.address.is_empty()
            || record.address.eq_ignore_ascii_case(&detailed.address),
        "Travel Rule trId detail changed destination"
    );
    for (name, indexed, hydrated) in [
        ("amount", &record.amount, &detailed.amount),
        (
            "transaction fee",
            &record.transaction_fee,
            &detailed.transaction_fee,
        ),
        (
            "withdraw order id",
            &record.withdraw_order_id,
            &detailed.withdraw_order_id,
        ),
        ("transaction id", &record.tx_id, &detailed.tx_id),
    ] {
        ensure!(
            indexed.is_empty() || hydrated.is_empty() || indexed == hydrated,
            "Travel Rule trId detail changed {name}"
        );
    }
    if record.id.is_empty() {
        record.id.clone_from(&detailed.id);
    }
    if record.amount.is_empty() {
        record.amount.clone_from(&detailed.amount);
    }
    if record.transaction_fee.is_empty() {
        record.transaction_fee.clone_from(&detailed.transaction_fee);
    }
    if record.network.is_empty() {
        record.network.clone_from(&detailed.network);
    }
    if record.address.is_empty() {
        record.address.clone_from(&detailed.address);
    }
    if record.withdraw_order_id.is_empty() {
        record
            .withdraw_order_id
            .clone_from(&detailed.withdraw_order_id);
    }
    if record.tx_id.is_empty() {
        record.tx_id.clone_from(&detailed.tx_id);
    }
    if record.info.is_empty() {
        record.info.clone_from(&detailed.info);
    }
    if record.withdrawal_status.is_none() {
        record.withdrawal_status = detailed.withdrawal_status;
    }
    Ok(())
}

fn reconcile_approved_travel_rule_rejection(
    records: &[TravelRuleWithdrawalRecord],
) -> anyhow::Result<Option<&TravelRuleWithdrawalRecord>> {
    ensure!(
        records.len() <= 1,
        "approved Travel Rule rejection matches multiple withdrawal records"
    );
    let Some(record) = records.first() else {
        // The reviewed HTTP 400/-4024 response was a synchronous validation
        // rejection and therefore may have no Travel Rule history row. The
        // caller separately proves that both withdrawal histories are empty
        // and that the successful master transfer is still durable.
        return Ok(None);
    };
    ensure!(
        record.is_failed_without_broadcast() || record.is_approved_without_withdrawal(),
        "approved Travel Rule rejection matches a non-failed or broadcast withdrawal"
    );
    Ok(Some(record))
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, time::Duration};

    use alloy_primitives::{Address, B256, U256, keccak256};
    use rust_decimal::Decimal;

    use crate::{
        binance::{
            account::{AccountInformation, AssetBalance},
            capital::{
                AddressVerificationQuestionnaire, AddressVerificationRecord,
                TravelRuleWithdrawalRecord, WithdrawalRecord,
            },
        },
        chain::rpc::{ReceiptLog, TransactionReceipt},
        rebalance::Route,
        rebalance::{
            Direction, RebalanceExecutionIntent, RebalanceExecutionOperation,
            RebalanceExecutionProgress,
        },
    };

    use super::{
        ARBITRUM_CHAIN_ID, WORLD_CHAIN_CHAIN_ID, WORLD_CHAIN_USDC, WORLD_CHAIN_WLD,
        WithdrawalAbsenceEvidence, account_asset_balance_or_zero, base_units_to_decimal,
        current_required_withdrawal, decimal_to_base_units, decimal_to_base_units_floor,
        matches_travel_rule_record_identity_without_client_id, merge_travel_rule_withdrawal_detail,
        reconcile_approved_travel_rule_rejection, route_wallet_chain_id,
        shared_evm_confirmation_timeout, validate_across_fill_receipt, validate_approved_asset,
        validate_direct_withdrawal_receipt, verified_self_owned_evm_address_record,
        withdrawal_received_base_units, withdrawal_requested_base_units, withdrawal_retry_is_stale,
    };

    #[test]
    fn production_saga_timeout_is_bounded_for_one_shared_evm_child() {
        assert_eq!(
            shared_evm_confirmation_timeout(Duration::from_secs(1_800)),
            Duration::from_secs(300)
        );
        assert_eq!(
            shared_evm_confirmation_timeout(Duration::from_secs(60)),
            Duration::from_secs(60)
        );
    }

    #[test]
    fn midpoint_revalidation_cancels_the_production_round_trip_shape() {
        let required = current_required_withdrawal(WithdrawalAbsenceEvidence {
            master_free_base_units: U256::from(2_994_u64),
            master_locked_base_units: U256::ZERO,
            trading_free_base_units: U256::from(808_u64),
            trading_locked_base_units: U256::ZERO,
            wallet_balance_base_units: U256::from(6_210_u64),
        })
        .unwrap();
        assert_eq!(required, U256::ZERO);
    }

    #[test]
    fn midpoint_revalidation_preserves_a_still_needed_withdrawal() {
        let required = current_required_withdrawal(WithdrawalAbsenceEvidence {
            master_free_base_units: U256::from(2_000_u64),
            master_locked_base_units: U256::ZERO,
            trading_free_base_units: U256::from(6_000_u64),
            trading_locked_base_units: U256::ZERO,
            wallet_balance_base_units: U256::from(2_000_u64),
        })
        .unwrap();
        assert_eq!(required, U256::from(3_000_u64));
    }

    #[test]
    fn threshold_revalidation_cancels_when_the_destination_has_recovered() {
        let evidence = WithdrawalAbsenceEvidence {
            master_free_base_units: U256::from(1_000_u64),
            master_locked_base_units: U256::ZERO,
            trading_free_base_units: U256::from(7_000_u64),
            trading_locked_base_units: U256::ZERO,
            wallet_balance_base_units: U256::from(4_000_u64),
        };
        let operation = RebalanceExecutionOperation {
            intent: RebalanceExecutionIntent {
                scope: None,
                operation_id: "rebalance-threshold-test".to_owned(),
                fingerprint: "1".repeat(64),
                withdraw_order_id: "rb111111111111111111111111111111".to_owned(),
                token_symbol: "ESP".to_owned(),
                token_decimals: 18,
                token_contract: Address::repeat_byte(0x11),
                wallet_owner: Address::repeat_byte(0x22),
                direction: Direction::BinanceToWallet,
                route: Route::Direct {
                    binance_network: "ARBITRUM".to_owned(),
                    chain_id: ARBITRUM_CHAIN_ID,
                },
                amount: U256::from(1_000_u64),
                binance_balance_before: U256::from(8_000_u64),
                wallet_balance_before: U256::from(2_000_u64),
                revalidation_start_balance: U256::from(4_000_u64),
                maximum_fee_base_units: None,
                approval_session_id: None,
            },
            progress: RebalanceExecutionProgress::IntentRecorded,
        };
        let required = current_required_withdrawal(evidence).unwrap();
        assert_eq!(required, U256::from(2_000_u64));
        assert!(withdrawal_retry_is_stale(&operation, evidence, required));
    }

    #[test]
    fn verified_evm_address_ownership_is_reusable_for_another_token() {
        let record = AddressVerificationRecord {
            status: "VERIFIED".to_owned(),
            token: "WLD".to_owned(),
            network: "ARBITRUM".to_owned(),
            wallet_address: "0x1111111111111111111111111111111111111111".to_owned(),
            address_questionnaire: AddressVerificationQuestionnaire {
                send_to: Some(1),
                satoshi_token: "WLD".to_owned(),
                is_address_owner: Some(1),
                verify_method: Some(1),
            },
        };

        assert!(verified_self_owned_evm_address_record(
            &record,
            "0x1111111111111111111111111111111111111111",
        ));
        assert!(!verified_self_owned_evm_address_record(
            &record,
            "0x2222222222222222222222222222222222222222",
        ));
    }

    #[test]
    fn direct_arbitrum_deposit_settlement_keeps_the_pinned_wallet_chain() {
        assert_eq!(
            route_wallet_chain_id(&Route::Direct {
                binance_network: "ARBITRUM".to_owned(),
                chain_id: ARBITRUM_CHAIN_ID,
            }),
            ARBITRUM_CHAIN_ID
        );
    }

    #[test]
    fn omitted_zero_master_asset_is_a_zero_balance() {
        let mut account = AccountInformation {
            can_trade: true,
            can_withdraw: true,
            can_deposit: true,
            brokered: false,
            require_self_trade_prevention: false,
            update_time: 0,
            account_type: "SPOT".to_owned(),
            balances: Vec::new(),
            permissions: vec!["SPOT".to_owned()],
        };
        assert_eq!(
            account_asset_balance_or_zero(&account, "ESP"),
            (Decimal::ZERO, Decimal::ZERO)
        );

        account.balances.push(AssetBalance {
            asset: "ESP".to_owned(),
            free: Decimal::from(1),
            locked: Decimal::from(2),
        });
        assert_eq!(
            account_asset_balance_or_zero(&account, "ESP"),
            (Decimal::from(1), Decimal::from(2))
        );
    }

    #[test]
    fn travel_rule_record_without_client_id_matches_exact_debit_and_wallet() {
        let wallet = Address::from_str("0x90d990c81320221d2882de32beea78923c1e77a3").unwrap();
        let record = TravelRuleWithdrawalRecord {
            id: String::new(),
            tr_id: 65_865_741,
            amount: "400".to_owned(),
            transaction_fee: "1.2".to_owned(),
            coin: "ESP".to_owned(),
            withdrawal_status: Some(3),
            travel_rule_status: 2,
            address: format!("{wallet:#x}"),
            tx_id: String::new(),
            network: "ARBITRUM".to_owned(),
            withdraw_order_id: String::new(),
            info: "[031031] User does not own this currency.".to_owned(),
        };
        assert!(matches_travel_rule_record_identity_without_client_id(
            &record,
            Decimal::from_str_exact("401.2").unwrap(),
            wallet,
            "rust-rebalance-client-id",
        ));

        let mut exact_gross_without_fee = record.clone();
        exact_gross_without_fee.amount = "401.2".to_owned();
        exact_gross_without_fee.transaction_fee = String::new();
        assert!(matches_travel_rule_record_identity_without_client_id(
            &exact_gross_without_fee,
            Decimal::from_str_exact("401.2").unwrap(),
            wallet,
            "rust-rebalance-client-id",
        ));

        let mut net_without_fee = record.clone();
        net_without_fee.transaction_fee = String::new();
        assert!(!matches_travel_rule_record_identity_without_client_id(
            &net_without_fee,
            Decimal::from_str_exact("401.2").unwrap(),
            wallet,
            "rust-rebalance-client-id",
        ));

        let mut wrong_wallet = record.clone();
        wrong_wallet.address = "0x1111111111111111111111111111111111111111".to_owned();
        assert!(!matches_travel_rule_record_identity_without_client_id(
            &wrong_wallet,
            Decimal::from_str_exact("401.2").unwrap(),
            wallet,
            "rust-rebalance-client-id",
        ));
    }

    #[test]
    fn synchronous_travel_rule_rejection_may_be_absent_from_history() {
        assert!(
            reconcile_approved_travel_rule_rejection(&[])
                .unwrap()
                .is_none()
        );

        let broadcast = TravelRuleWithdrawalRecord {
            id: "withdrawal-id".to_owned(),
            tr_id: 65_865_742,
            amount: "400".to_owned(),
            transaction_fee: "1.2".to_owned(),
            coin: "ESP".to_owned(),
            withdrawal_status: Some(6),
            travel_rule_status: 0,
            address: "0x1111111111111111111111111111111111111111".to_owned(),
            tx_id: "0xabc".to_owned(),
            network: "ARBITRUM".to_owned(),
            withdraw_order_id: String::new(),
            info: String::new(),
        };
        assert!(reconcile_approved_travel_rule_rejection(&[broadcast]).is_err());
    }

    #[test]
    fn approved_travel_rule_record_without_a_withdrawal_is_unbroadcast() {
        let record = TravelRuleWithdrawalRecord {
            id: String::new(),
            tr_id: 67_181_540,
            amount: "400".to_owned(),
            transaction_fee: "1.2".to_owned(),
            coin: "ESP".to_owned(),
            withdrawal_status: None,
            travel_rule_status: 4,
            address: "0x1111111111111111111111111111111111111111".to_owned(),
            tx_id: String::new(),
            network: "ARBITRUM".to_owned(),
            withdraw_order_id: "rustwd5".to_owned(),
            info: "[031031] User does not own this currency.".to_owned(),
        };
        assert!(record.is_approved_without_withdrawal());
        assert!(
            reconcile_approved_travel_rule_rejection(std::slice::from_ref(&record))
                .unwrap()
                .is_some()
        );

        let mut completed = record.clone();
        completed.withdrawal_status = Some(6);
        assert!(
            reconcile_approved_travel_rule_rejection(std::slice::from_ref(&completed)).is_err()
        );

        let mut broadcast = record;
        broadcast.tx_id = "0xabc".to_owned();
        assert!(
            reconcile_approved_travel_rule_rejection(std::slice::from_ref(&broadcast)).is_err()
        );
    }

    #[test]
    fn travel_rule_detail_only_fills_omitted_fields_and_preserves_identity() {
        let mut indexed = TravelRuleWithdrawalRecord {
            id: String::new(),
            tr_id: 67_181_540,
            amount: "400".to_owned(),
            transaction_fee: "1.2".to_owned(),
            coin: "ESP".to_owned(),
            withdrawal_status: None,
            travel_rule_status: 4,
            address: "0x1111111111111111111111111111111111111111".to_owned(),
            tx_id: String::new(),
            network: "ARBITRUM".to_owned(),
            withdraw_order_id: "rustwd5".to_owned(),
            info: String::new(),
        };
        let detailed = TravelRuleWithdrawalRecord {
            id: "detail-id".to_owned(),
            info: "[031031] User does not own this currency.".to_owned(),
            ..indexed.clone()
        };
        merge_travel_rule_withdrawal_detail(&mut indexed, &detailed).unwrap();
        assert_eq!(indexed.id, "detail-id");
        assert_eq!(indexed.info, "[031031] User does not own this currency.");
        assert_eq!(indexed.withdrawal_status, None);

        let mut mismatched = detailed;
        mismatched.address = "0x2222222222222222222222222222222222222222".to_owned();
        assert!(merge_travel_rule_withdrawal_detail(&mut indexed, &mismatched).is_err());
    }

    #[test]
    fn exact_decimal_conversion_round_trips_executor_limits() {
        let amounts = [
            (U256::from(1_234_567_u64), 6_u8, "1.234567"),
            (U256::from(1_000_000_000_000_000_000_u128), 18_u8, "1"),
            (U256::ONE, 18_u8, "0.000000000000000001"),
        ];
        for (base_units, decimals, expected) in amounts {
            let decimal = base_units_to_decimal(base_units, decimals).unwrap();
            assert_eq!(decimal, Decimal::from_str_exact(expected).unwrap());
            assert_eq!(
                decimal_to_base_units(decimal, decimals).unwrap(),
                base_units
            );
        }
    }

    #[test]
    fn permits_only_exact_world_chain_token_metadata() {
        validate_approved_asset("WLD", 18, WORLD_CHAIN_WLD, WORLD_CHAIN_CHAIN_ID).unwrap();
        validate_approved_asset("USDC", 6, WORLD_CHAIN_USDC, WORLD_CHAIN_CHAIN_ID).unwrap();
        assert!(validate_approved_asset("WLD", 6, WORLD_CHAIN_WLD, WORLD_CHAIN_CHAIN_ID).is_err());
        assert!(
            validate_approved_asset("USDT", 6, Address::repeat_byte(1), WORLD_CHAIN_CHAIN_ID)
                .is_err()
        );
    }

    #[test]
    fn floors_binance_dust_but_keeps_transaction_conversion_exact() {
        let balance = Decimal::from_str_exact("6170.80727184").unwrap();
        assert_eq!(
            decimal_to_base_units_floor(balance, 6).unwrap(),
            U256::from(6_170_807_271_u64)
        );
        assert!(decimal_to_base_units(balance, 6).is_err());
    }

    #[test]
    fn treats_binance_withdrawal_amount_as_net_of_fee() {
        let record = WithdrawalRecord {
            id: "withdrawal-id".to_owned(),
            amount: Decimal::from_str_exact("499.95").unwrap(),
            transaction_fee: Decimal::from_str_exact("0.05").unwrap(),
            coin: "USDC".to_owned(),
            status: 6,
            address: format!("{:#x}", Address::repeat_byte(1)),
            tx_id: "0xabc".to_owned(),
            network: "OPTIMISM".to_owned(),
            withdraw_order_id: "rb1".to_owned(),
            info: String::new(),
        };

        assert_eq!(
            withdrawal_requested_base_units(&record, 6).unwrap(),
            U256::from(500_000_000_u64)
        );
        assert_eq!(
            withdrawal_received_base_units(&record, 6).unwrap(),
            U256::from(499_950_000_u64)
        );

        let wld = WithdrawalRecord {
            amount: Decimal::from_str_exact("875.429").unwrap(),
            transaction_fee: Decimal::from_str_exact("0.071").unwrap(),
            coin: "WLD".to_owned(),
            network: "OPTIMISM".to_owned(),
            ..record
        };
        assert_eq!(
            withdrawal_requested_base_units(&wld, 18).unwrap(),
            U256::from(875_500_000_000_000_000_000_u128)
        );
        assert_eq!(
            withdrawal_received_base_units(&wld, 18).unwrap(),
            U256::from(875_429_000_000_000_000_000_u128)
        );
    }

    #[test]
    fn direct_withdrawal_receipt_proves_credit_despite_later_wallet_spending() {
        fn address_topic(address: Address) -> B256 {
            let mut word = [0_u8; 32];
            word[12..].copy_from_slice(address.as_slice());
            word.into()
        }

        let transaction_hash = B256::repeat_byte(0x44);
        let token = Address::repeat_byte(0x11);
        let wallet = Address::repeat_byte(0x22);
        let received = U256::from(1_133_000_u64);
        let receipt = TransactionReceipt {
            transaction_hash,
            block_number: 123,
            status: 1,
            gas_used: 50_000,
            effective_gas_price: 1,
            l1_fee: 0,
            logs: vec![ReceiptLog {
                address: token,
                topics: vec![
                    keccak256("Transfer(address,address,uint256)"),
                    address_topic(Address::repeat_byte(0x33)),
                    address_topic(wallet),
                ],
                data: received.to_be_bytes::<32>().to_vec(),
                position: None,
            }],
        };

        validate_direct_withdrawal_receipt(&receipt, transaction_hash, token, wallet, received)
            .unwrap();
        assert!(
            validate_direct_withdrawal_receipt(
                &receipt,
                transaction_hash,
                token,
                wallet,
                received + U256::ONE,
            )
            .is_err()
        );
        assert!(
            validate_direct_withdrawal_receipt(
                &receipt,
                transaction_hash,
                token,
                Address::repeat_byte(0x55),
                received,
            )
            .is_err()
        );
    }

    #[test]
    fn across_fill_receipt_proves_credit_when_original_wallet_snapshot_is_stale() {
        fn address_topic(address: Address) -> B256 {
            let mut word = [0_u8; 32];
            word[12..].copy_from_slice(address.as_slice());
            word.into()
        }

        // Production incident rebalance-45-2ded2cfb1cf635d1: a concurrent
        // arbitrage spent 199.443407 USDC after the rebalance intent snapshot
        // but before the bridge captured its destination balance.
        let original_wallet_snapshot = U256::from(1_241_799_768_u64);
        let destination_balance_before = U256::from(1_042_356_361_u64);
        let received = U256::from(1_260_763_057_u64);
        let wallet_after = U256::from(2_303_119_418_u64);
        assert_eq!(wallet_after, destination_balance_before + received);
        assert!(wallet_after < original_wallet_snapshot + received);

        let fill_hash = B256::repeat_byte(0x44);
        let token = WORLD_CHAIN_USDC;
        let wallet = Address::repeat_byte(0x22);
        let receipt = TransactionReceipt {
            transaction_hash: fill_hash,
            block_number: 32_629_600,
            status: 1,
            gas_used: 150_000,
            effective_gas_price: 1,
            l1_fee: 0,
            logs: vec![ReceiptLog {
                address: token,
                topics: vec![
                    keccak256("Transfer(address,address,uint256)"),
                    address_topic(Address::repeat_byte(0x33)),
                    address_topic(wallet),
                ],
                data: received.to_be_bytes::<32>().to_vec(),
                position: None,
            }],
        };

        assert_eq!(
            validate_across_fill_receipt(&receipt, fill_hash, token, wallet, received).unwrap(),
            received
        );
        assert!(
            validate_across_fill_receipt(&receipt, fill_hash, token, wallet, received + U256::ONE,)
                .is_err()
        );
    }
}

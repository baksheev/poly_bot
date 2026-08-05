use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions, symlink_metadata},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

use alloy_primitives::{Address, B256, U256, keccak256};
use anyhow::{Context, ensure};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

use super::{Direction, Location, PendingTransfer, RebalanceAction, Route};

const VERSION: u16 = 1;
const MAX_LINE_BYTES: usize = 64 * 1024;
const MAX_REASON_BYTES: usize = 1_024;
const MAX_CORRECTED_QUARANTINE_REOPENS: u8 = 4;
pub const MAX_TRAVEL_RULE_OWNERSHIP_REJECTION_RETRIES: u8 = 3;
const MAX_TRAVEL_RULE_OWNERSHIP_REJECTION_REOPENS: u8 =
    MAX_TRAVEL_RULE_OWNERSHIP_REJECTION_RETRIES - 1;
const TRAVEL_RULE_BINANCE_WITHDRAWAL_API_MODE: &str = "travel_rule_ae_self_owned";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebalanceExecutionRequest {
    pub authority: RebalanceExecutionAuthority,
    pub token_symbol: String,
    pub token_decimals: u8,
    pub token_contract: Address,
    pub wallet_owner: Address,
    pub action: RebalanceAction,
    pub binance_balance_before: U256,
    pub wallet_balance_before: U256,
    pub revalidation_start_balance: U256,
    pub maximum_fee: Option<U256>,
    pub approval_session_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RebalanceExecutionAuthority {
    WorldChainV12,
    ArbitrumFullLive,
}

impl RebalanceExecutionAuthority {
    fn strategy_id(self) -> &'static str {
        match self {
            Self::WorldChainV12 => "rebalance-world-chain-v12",
            Self::ArbitrumFullLive => "rebalance-arbitrum-usdc-esp",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RebalanceExecutionIntent {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<RebalanceJournalScope>,
    pub operation_id: String,
    pub fingerprint: String,
    pub withdraw_order_id: String,
    pub token_symbol: String,
    pub token_decimals: u8,
    #[serde(with = "address_serde")]
    pub token_contract: Address,
    #[serde(with = "address_serde")]
    pub wallet_owner: Address,
    pub direction: Direction,
    pub route: Route,
    #[serde(with = "u256_serde")]
    pub amount: U256,
    #[serde(with = "u256_serde")]
    pub binance_balance_before: U256,
    #[serde(with = "u256_serde")]
    pub wallet_balance_before: U256,
    #[serde(default, with = "u256_serde", skip_serializing_if = "U256::is_zero")]
    pub revalidation_start_balance: U256,
    #[serde(
        default,
        alias = "canary_maximum_fee_base_units",
        skip_serializing_if = "Option::is_none"
    )]
    pub maximum_fee_base_units: Option<String>,
    #[serde(
        default,
        alias = "canary_approval_session_id",
        skip_serializing_if = "Option::is_none"
    )]
    pub approval_session_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RebalanceJournalScope {
    pub schema_version: u16,
    pub account_id: String,
    pub network_id: String,
    pub strategy_id: String,
}

impl RebalanceJournalScope {
    pub const SCHEMA_VERSION: u16 = 2;
}

impl RebalanceExecutionIntent {
    pub fn pending_transfer(&self) -> PendingTransfer {
        let (source, destination) = match self.direction {
            Direction::BinanceToWallet => (Location::Binance, Location::Wallet),
            Direction::WalletToBinance => (Location::Wallet, Location::Binance),
        };
        PendingTransfer {
            source,
            destination,
            amount: self.amount,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RebalanceExecutionProgress {
    IntentRecorded,
    BinanceTransferSubmitted {
        transaction_id: u64,
        #[serde(with = "u256_serde")]
        bridge_balance_before: U256,
    },
    BinanceTransferCompleted {
        transaction_id: u64,
        #[serde(with = "u256_serde")]
        bridge_balance_before: U256,
    },
    BinanceWithdrawalSubmissionStarted {
        api_mode: String,
        #[serde(with = "u256_serde")]
        bridge_balance_before: U256,
        #[serde(default)]
        reconciliation_queries: u16,
    },
    BinanceWithdrawalRetryAuthorized {
        api_mode: String,
        #[serde(with = "u256_serde")]
        bridge_balance_before: U256,
        #[serde(with = "u256_serde")]
        master_free_base_units: U256,
        #[serde(with = "u256_serde")]
        master_locked_base_units: U256,
        #[serde(with = "u256_serde")]
        wallet_balance_base_units: U256,
    },
    BinanceMasterReturnSubmissionStarted {
        client_transaction_id: String,
        #[serde(with = "u256_serde")]
        revalidation_binance_balance: U256,
        #[serde(with = "u256_serde")]
        revalidation_wallet_balance: U256,
        #[serde(with = "u256_serde")]
        revalidation_required_withdrawal: U256,
        #[serde(default)]
        reconciliation_queries: u16,
    },
    BinanceMasterReturnSubmitted {
        client_transaction_id: String,
        transaction_id: u64,
        #[serde(with = "u256_serde")]
        revalidation_binance_balance: U256,
        #[serde(with = "u256_serde")]
        revalidation_wallet_balance: U256,
        #[serde(with = "u256_serde")]
        revalidation_required_withdrawal: U256,
    },
    BinanceWithdrawalSubmitted {
        submission_reference: String,
        #[serde(with = "u256_serde")]
        bridge_balance_before: U256,
    },
    FundsOnBridge {
        withdrawal_id: String,
        transaction_id: String,
        #[serde(with = "u256_serde")]
        received_base_units: U256,
    },
    ApprovalMined {
        chain_id: u64,
        #[serde(with = "b256_serde")]
        transaction_hash: B256,
        /// Exact amount approved for the subsequent bridge call. Zero is
        /// accepted only when reading journals written before this field was
        /// introduced; the runtime rehydrates the amount before preparing a
        /// bridge in that case.
        #[serde(default, with = "u256_serde", skip_serializing_if = "U256::is_zero")]
        input_amount: U256,
    },
    BridgePrepared {
        origin_chain_id: u64,
        #[serde(with = "u256_serde")]
        input_amount: U256,
        #[serde(with = "address_serde")]
        target: Address,
        calldata: Vec<u8>,
        #[serde(with = "b256_serde")]
        calldata_hash: B256,
        #[serde(with = "u256_serde")]
        minimum_output_amount: U256,
        #[serde(with = "u256_serde")]
        destination_balance_before: U256,
    },
    BridgeMined {
        origin_chain_id: u64,
        #[serde(with = "b256_serde")]
        transaction_hash: B256,
        #[serde(with = "u256_serde")]
        minimum_output_amount: U256,
        #[serde(with = "u256_serde")]
        destination_balance_before: U256,
    },
    AcrossFilled {
        #[serde(with = "b256_serde")]
        fill_transaction_hash: B256,
        #[serde(with = "u256_serde")]
        received_base_units: U256,
    },
    DepositTransferMined {
        chain_id: u64,
        #[serde(with = "b256_serde")]
        transaction_hash: B256,
    },
    DepositQuestionnaireSubmissionStarted {
        chain_id: u64,
        #[serde(with = "b256_serde")]
        transaction_hash: B256,
        deposit_id: String,
    },
    BinanceCredited {
        deposit_id: String,
        #[serde(with = "u256_serde")]
        credited_base_units: U256,
    },
    Completed {
        #[serde(with = "u256_serde")]
        binance_balance_after: U256,
        #[serde(with = "u256_serde")]
        wallet_balance_after: U256,
    },
    CancelledStale {
        master_return_transaction_id: u64,
        #[serde(with = "u256_serde")]
        revalidation_binance_balance: U256,
        #[serde(with = "u256_serde")]
        revalidation_wallet_balance: U256,
        #[serde(with = "u256_serde")]
        revalidation_required_withdrawal: U256,
    },
    Failed {
        reason: String,
    },
    Quarantined {
        reason: String,
    },
}

impl RebalanceExecutionProgress {
    pub fn terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed { .. }
                | Self::CancelledStale { .. }
                | Self::Failed { .. }
                | Self::Quarantined { .. }
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RebalanceExecutionOperation {
    pub intent: RebalanceExecutionIntent,
    pub progress: RebalanceExecutionProgress,
}

pub struct RebalanceExecutionJournal {
    path: PathBuf,
    file: File,
    operations: BTreeMap<String, RebalanceExecutionOperation>,
    operation_started_at_unix_ms: BTreeMap<String, u64>,
    progress_before_quarantine: BTreeMap<String, RebalanceExecutionProgress>,
    quarantine_reopen_counts: BTreeMap<String, u8>,
    travel_rule_ownership_reopen_counts: BTreeMap<String, u8>,
    next_sequence: u64,
    poisoned: bool,
}

impl std::fmt::Debug for RebalanceExecutionJournal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RebalanceExecutionJournal")
            .field("path", &self.path)
            .field("operations", &self.operations.len())
            .field("next_sequence", &self.next_sequence)
            .field("poisoned", &self.poisoned)
            .finish()
    }
}

impl RebalanceExecutionJournal {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        ensure!(
            !path.as_os_str().is_empty(),
            "rebalance executor journal path is empty"
        );
        let existed = path.exists();
        if existed {
            let metadata = symlink_metadata(&path).with_context(|| {
                format!(
                    "failed to inspect rebalance executor journal {}",
                    path.display()
                )
            })?;
            ensure!(
                !metadata.file_type().is_symlink(),
                "rebalance executor journal must not be a symbolic link"
            );
            ensure!(
                metadata.is_file(),
                "rebalance executor journal path is not a file"
            );
        } else {
            let parent = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            ensure!(
                parent.is_dir(),
                "rebalance executor journal parent directory does not exist"
            );
        }

        let mut options = OpenOptions::new();
        options.create(true).read(true).append(true);
        #[cfg(unix)]
        options.mode(0o600);
        let file = options.open(&path).with_context(|| {
            format!(
                "failed to open rebalance executor journal {}",
                path.display()
            )
        })?;
        validate_permissions(&file)?;
        file.try_lock()
            .context("rebalance executor journal is already locked by another process")?;
        if !existed {
            file.sync_all()
                .context("failed to sync new rebalance executor journal")?;
            sync_parent(&path)?;
        }

        let mut operations: BTreeMap<String, RebalanceExecutionOperation> = BTreeMap::new();
        let mut operation_started_at_unix_ms = BTreeMap::new();
        let mut progress_before_quarantine = BTreeMap::new();
        let mut quarantine_reopen_counts = BTreeMap::new();
        let mut travel_rule_ownership_reopen_counts = BTreeMap::new();
        let mut expected_sequence = 0_u64;
        let mut reader = BufReader::new(
            file.try_clone()
                .context("failed to clone rebalance executor journal handle")?,
        );
        loop {
            let mut line = Vec::new();
            let bytes = reader
                .read_until(b'\n', &mut line)
                .context("failed to read rebalance executor journal")?;
            if bytes == 0 {
                break;
            }
            ensure!(
                line.len() <= MAX_LINE_BYTES,
                "rebalance executor journal record is too large"
            );
            ensure!(
                line.last() == Some(&b'\n'),
                "rebalance executor journal ends with a partial record"
            );
            line.pop();
            let record: RawWireRecord<'_> = serde_json::from_slice(&line)
                .context("rebalance executor journal contains invalid JSON")?;
            record.validate_checksum()?;
            let payload: WirePayload = serde_json::from_str(record.payload.get())
                .context("rebalance executor journal payload is invalid")?;
            ensure!(
                payload.version == VERSION,
                "unsupported rebalance executor journal version"
            );
            ensure!(
                payload.sequence == expected_sequence,
                "rebalance executor journal sequence mismatch"
            );
            operation_started_at_unix_ms
                .entry(payload.operation.intent.operation_id.clone())
                .or_insert(payload.recorded_at_unix_ms);
            if let Some(previous) = operations.get(&payload.operation.intent.operation_id) {
                if matches!(
                    payload.operation.progress,
                    RebalanceExecutionProgress::Quarantined { .. }
                ) {
                    progress_before_quarantine.insert(
                        payload.operation.intent.operation_id.clone(),
                        previous.progress.clone(),
                    );
                } else if matches!(
                    previous.progress,
                    RebalanceExecutionProgress::Quarantined { .. }
                ) {
                    let count = quarantine_reopen_counts
                        .entry(payload.operation.intent.operation_id.clone())
                        .or_insert(0_u8);
                    *count = count
                        .checked_add(1)
                        .context("rebalance quarantine reopen count overflow")?;
                }
                if retryable_travel_rule_ownership_reopen(
                    &previous.progress,
                    &payload.operation.progress,
                ) {
                    let count = travel_rule_ownership_reopen_counts
                        .entry(payload.operation.intent.operation_id.clone())
                        .or_insert(0_u8);
                    *count = count
                        .checked_add(1)
                        .context("Travel Rule ownership rejection reopen count overflow")?;
                }
            }
            apply_snapshot(
                &mut operations,
                &payload.operation,
                TransitionOrigin::JournalReplay,
            )?;
            expected_sequence = expected_sequence
                .checked_add(1)
                .context("rebalance executor journal sequence overflow")?;
        }

        Ok(Self {
            path,
            file,
            operations,
            operation_started_at_unix_ms,
            progress_before_quarantine,
            quarantine_reopen_counts,
            travel_rule_ownership_reopen_counts,
            next_sequence: expected_sequence,
            poisoned: false,
        })
    }

    pub fn operations(&self) -> &BTreeMap<String, RebalanceExecutionOperation> {
        &self.operations
    }

    pub fn active_operation(&self) -> anyhow::Result<Option<&RebalanceExecutionOperation>> {
        let mut active = self
            .operations
            .values()
            .filter(|operation| !operation.progress.terminal());
        let operation = active.next();
        ensure!(
            active.next().is_none(),
            "multiple active rebalance operations in journal"
        );
        Ok(operation)
    }

    pub fn quarantined_operations(&self) -> impl Iterator<Item = &RebalanceExecutionOperation> {
        self.operations.values().filter(|operation| {
            matches!(
                operation.progress,
                RebalanceExecutionProgress::Quarantined { .. }
            )
        })
    }

    pub fn next_reconcilable_arbitrum_deposit_quarantine(
        &self,
    ) -> anyhow::Result<Option<&RebalanceExecutionOperation>> {
        if self.active_operation()?.is_some() {
            return Ok(None);
        }
        Ok(self.operations.values().find(|operation| {
            let RebalanceExecutionProgress::Quarantined { reason } = &operation.progress else {
                return false;
            };
            reason.starts_with("DEX outcome unknown:")
                && operation.intent.direction == Direction::WalletToBinance
                && matches!(
                    &operation.intent.route,
                    Route::Direct {
                        chain_id: 42_161,
                        binance_network,
                    } if binance_network == "ARBITRUM"
                )
                && self
                    .progress_before_quarantine
                    .get(&operation.intent.operation_id)
                    == Some(&RebalanceExecutionProgress::IntentRecorded)
        }))
    }

    pub fn next_reconcilable_across_fill_quarantine(
        &self,
    ) -> anyhow::Result<Option<&RebalanceExecutionOperation>> {
        if self.active_operation()?.is_some() {
            return Ok(None);
        }
        Ok(self.operations.values().find(|operation| {
            let RebalanceExecutionProgress::Quarantined { reason } = &operation.progress else {
                return false;
            };
            across_fill_timeout_quarantine(reason)
                && matches!(&operation.intent.route, Route::Across { .. })
                && matches!(
                    self.progress_before_quarantine
                        .get(&operation.intent.operation_id),
                    Some(RebalanceExecutionProgress::BridgeMined { .. })
                )
        }))
    }

    pub fn progress_before_quarantine(
        &self,
        operation_id: &str,
    ) -> Option<&RebalanceExecutionProgress> {
        self.progress_before_quarantine.get(operation_id)
    }

    pub fn record_reconciled_arbitrum_deposit(
        &mut self,
        operation_id: &str,
        transaction_hash: B256,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        let current = self
            .operations
            .get(operation_id)
            .with_context(|| format!("unknown rebalance operation {operation_id}"))?;
        ensure!(
            self.progress_before_quarantine.get(operation_id)
                == Some(&RebalanceExecutionProgress::IntentRecorded),
            "reconciled Arbitrum deposit quarantine did not follow an exact recorded intent"
        );
        let progress = RebalanceExecutionProgress::DepositTransferMined {
            chain_id: 42_161,
            transaction_hash,
        };
        ensure!(
            reconciled_arbitrum_deposit_transition(&current.intent, &current.progress, &progress,),
            "rebalance quarantine is not an approved reconciled Arbitrum deposit"
        );
        let next = RebalanceExecutionOperation {
            intent: current.intent.clone(),
            progress,
        };
        self.append(next.clone())?;
        Ok(next)
    }

    pub fn record_reconciled_across_fill(
        &mut self,
        operation_id: &str,
        fill_transaction_hash: B256,
        received_base_units: U256,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        let current = self
            .operations
            .get(operation_id)
            .with_context(|| format!("unknown rebalance operation {operation_id}"))?;
        let previous = self
            .progress_before_quarantine
            .get(operation_id)
            .context("reconciled Across fill quarantine has no prior durable progress")?;
        let RebalanceExecutionProgress::BridgeMined {
            minimum_output_amount,
            ..
        } = previous
        else {
            anyhow::bail!("reconciled Across fill quarantine did not follow a mined bridge")
        };
        ensure!(
            received_base_units >= *minimum_output_amount,
            "reconciled Across fill is below the journaled minimum"
        );
        let progress = RebalanceExecutionProgress::AcrossFilled {
            fill_transaction_hash,
            received_base_units,
        };
        ensure!(
            reconciled_across_fill_transition(&current.intent, &current.progress, &progress),
            "rebalance quarantine is not an approved reconciled Across fill"
        );
        let next = RebalanceExecutionOperation {
            intent: current.intent.clone(),
            progress,
        };
        self.append(next.clone())?;
        Ok(next)
    }

    pub fn reopen_next_retryable_quarantine(
        &mut self,
    ) -> anyhow::Result<Option<RebalanceExecutionOperation>> {
        // Startup recovery must finish the single already-active mutation
        // owner before reopening a different token's quarantined operation.
        // Returning no candidate preserves that ownership ordering without
        // turning a valid multi-token journal into a process-fatal error.
        if self.active_operation()?.is_some() {
            return Ok(None);
        }
        let terminal_candidate = self
            .operations
            .values()
            .find(|operation| {
                matches!(
                    &operation.progress,
                    RebalanceExecutionProgress::Failed { reason }
                        if retryable_travel_rule_ownership_failure(reason)
                ) && self
                    .travel_rule_ownership_reopen_counts
                    .get(&operation.intent.operation_id)
                    .copied()
                    .unwrap_or(0)
                    < MAX_TRAVEL_RULE_OWNERSHIP_REJECTION_REOPENS
            })
            .map(|operation| operation.intent.operation_id.clone());
        if let Some(operation_id) = terminal_candidate {
            return self.reopen_retryable_travel_rule_ownership_failure(&operation_id);
        }
        let candidate = self.operations.values().find_map(|operation| {
            let RebalanceExecutionProgress::Quarantined { reason } = &operation.progress else {
                return None;
            };
            if self
                .quarantine_reopen_counts
                .get(&operation.intent.operation_id)
                .copied()
                .unwrap_or(0)
                >= MAX_CORRECTED_QUARANTINE_REOPENS
            {
                return None;
            }
            let previous = self
                .progress_before_quarantine
                .get(&operation.intent.operation_id)?;
            if !corrected_guard_quarantine(reason)
                && !corrected_across_deposit_chain_quarantine(&operation.intent, reason, previous)
            {
                return None;
            }
            Some((operation.intent.operation_id.clone(), previous.clone()))
        });
        let Some((operation_id, progress)) = candidate else {
            return Ok(None);
        };
        let reopened = self.advance(&operation_id, progress)?;
        Ok(Some(reopened))
    }

    pub fn reopen_retryable_travel_rule_ownership_failure(
        &mut self,
        operation_id: &str,
    ) -> anyhow::Result<Option<RebalanceExecutionOperation>> {
        if self.active_operation()?.is_some() {
            return Ok(None);
        }
        let Some(operation) = self.operations.get(operation_id) else {
            return Ok(None);
        };
        let retryable = matches!(
            &operation.progress,
            RebalanceExecutionProgress::Failed { reason }
                if retryable_travel_rule_ownership_failure(reason)
        );
        let reopen_count = self
            .travel_rule_ownership_reopen_counts
            .get(operation_id)
            .copied()
            .unwrap_or(0);
        if !retryable || reopen_count >= MAX_TRAVEL_RULE_OWNERSHIP_REJECTION_REOPENS {
            return Ok(None);
        }
        let bridge_balance_before = operation.intent.wallet_balance_before;
        let reopened = self.advance(
            operation_id,
            RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                api_mode: TRAVEL_RULE_BINANCE_WITHDRAWAL_API_MODE.to_owned(),
                bridge_balance_before,
                reconciliation_queries: 0,
            },
        )?;
        Ok(Some(reopened))
    }

    pub fn rebalance_risk(&self, approval_session_id: &str) -> anyhow::Result<RebalanceRisk> {
        let mut risk = RebalanceRisk::default();
        for operation in self.operations.values().filter(|operation| {
            operation
                .intent
                .scope
                .as_ref()
                .is_some_and(|scope| scope.network_id == "chain:42161")
                && operation.intent.approval_session_id.as_deref() == Some(approval_session_id)
        }) {
            risk.transfer_count = risk
                .transfer_count
                .checked_add(1)
                .context("rebalance transfer count overflow")?;
            risk.active_transfer_count += usize::from(!operation.progress.terminal());
            risk.failed_transfer_count += usize::from(matches!(
                operation.progress,
                RebalanceExecutionProgress::Failed { .. }
                    | RebalanceExecutionProgress::Quarantined { .. }
            ));
            let total = match operation.intent.token_symbol.as_str() {
                "USDC" => &mut risk.token_a_debit,
                "ESP" => &mut risk.token_b_debit,
                "ARB" => risk
                    .additional_token_debit
                    .entry("ARB".to_owned())
                    .or_insert(U256::ZERO),
                _ => anyhow::bail!("rebalance journal contains an unapproved asset"),
            };
            *total = total
                .checked_add(operation.intent.amount)
                .context("rebalance cumulative debit overflow")?;
            let maximum_fee = operation
                .intent
                .maximum_fee_base_units
                .as_deref()
                .context("rebalance journal operation has no fee authority")
                .and_then(|value| {
                    U256::from_str(value).context("rebalance fee authority is not a uint256")
                })?;
            let fee_total = match operation.intent.token_symbol.as_str() {
                "USDC" => &mut risk.token_a_maximum_fee,
                "ESP" => &mut risk.token_b_maximum_fee,
                "ARB" => risk
                    .additional_token_maximum_fee
                    .entry("ARB".to_owned())
                    .or_insert(U256::ZERO),
                _ => unreachable!("asset was validated above"),
            };
            *fee_total = fee_total
                .checked_add(maximum_fee)
                .context("rebalance cumulative fee authority overflow")?;
            let started_at = self
                .operation_started_at_unix_ms
                .get(&operation.intent.operation_id)
                .copied()
                .context("rebalance operation has no durable start timestamp")?;
            risk.first_started_at_unix_ms = Some(
                risk.first_started_at_unix_ms
                    .map_or(started_at, |current| current.min(started_at)),
            );
        }
        Ok(risk)
    }

    pub fn latest_rebalance_operation(
        &self,
        approval_session_id: &str,
    ) -> Option<&RebalanceExecutionOperation> {
        self.operations
            .values()
            .filter(|operation| {
                operation
                    .intent
                    .scope
                    .as_ref()
                    .is_some_and(|scope| scope.network_id == "chain:42161")
                    && operation.intent.approval_session_id.as_deref() == Some(approval_session_id)
            })
            .max_by_key(|operation| {
                (
                    self.operation_started_at_unix_ms
                        .get(&operation.intent.operation_id)
                        .copied()
                        .unwrap_or_default(),
                    operation.intent.operation_id.as_str(),
                )
            })
    }

    pub fn reserve(
        &mut self,
        request: &RebalanceExecutionRequest,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        validate_request(request)?;
        ensure!(
            self.active_operation()?.is_none(),
            "another rebalance operation is active"
        );
        let fingerprint = request_fingerprint(request)?;
        let operation_id = format!("rebalance-{}-{}", self.next_sequence, &fingerprint[..16]);
        let withdraw_order_id = format!("rb{}", &fingerprint[..30]);
        let operation = RebalanceExecutionOperation {
            intent: RebalanceExecutionIntent {
                scope: Some(RebalanceJournalScope {
                    schema_version: RebalanceJournalScope::SCHEMA_VERSION,
                    account_id: "binance:trading-subaccount".to_owned(),
                    network_id: match &request.action.route {
                        Route::Direct { chain_id, .. } => format!("chain:{chain_id}"),
                        Route::Across {
                            bridge_chain_id, ..
                        } => format!("chain:{bridge_chain_id}"),
                    },
                    strategy_id: request.authority.strategy_id().to_owned(),
                }),
                operation_id,
                fingerprint,
                withdraw_order_id,
                token_symbol: request.token_symbol.clone(),
                token_decimals: request.token_decimals,
                token_contract: request.token_contract,
                wallet_owner: request.wallet_owner,
                direction: request.action.direction,
                route: request.action.route.clone(),
                amount: request.action.amount,
                binance_balance_before: request.binance_balance_before,
                wallet_balance_before: request.wallet_balance_before,
                revalidation_start_balance: request.revalidation_start_balance,
                maximum_fee_base_units: request.maximum_fee.map(|value| value.to_string()),
                approval_session_id: request.approval_session_id.clone(),
            },
            progress: RebalanceExecutionProgress::IntentRecorded,
        };
        self.append(operation.clone())?;
        Ok(operation)
    }

    pub fn advance(
        &mut self,
        operation_id: &str,
        progress: RebalanceExecutionProgress,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        let current = self
            .operations
            .get(operation_id)
            .with_context(|| format!("unknown rebalance operation {operation_id}"))?;
        if matches!(
            current.progress,
            RebalanceExecutionProgress::Quarantined { .. }
        ) {
            ensure!(
                self.progress_before_quarantine.get(operation_id) == Some(&progress),
                "quarantined rebalance may only reopen its exact prior durable progress"
            );
        }
        validate_transition(
            &current.intent,
            &current.progress,
            &progress,
            TransitionOrigin::LiveAppend,
        )?;
        let next = RebalanceExecutionOperation {
            intent: current.intent.clone(),
            progress,
        };
        self.append(next.clone())?;
        Ok(next)
    }

    fn append(&mut self, operation: RebalanceExecutionOperation) -> anyhow::Result<()> {
        ensure!(!self.poisoned, "rebalance executor journal is poisoned");
        validate_operation(&operation)?;
        let payload = WirePayload {
            version: VERSION,
            sequence: self.next_sequence,
            recorded_at_unix_ms: unix_timestamp_ms()?,
            operation,
        };
        let mut next_operations = self.operations.clone();
        let previous_progress = next_operations
            .get(&payload.operation.intent.operation_id)
            .map(|operation| operation.progress.clone());
        apply_snapshot(
            &mut next_operations,
            &payload.operation,
            TransitionOrigin::LiveAppend,
        )?;
        let mut next_started_at = self.operation_started_at_unix_ms.clone();
        next_started_at
            .entry(payload.operation.intent.operation_id.clone())
            .or_insert(payload.recorded_at_unix_ms);
        let appended_operation_id = payload.operation.intent.operation_id.clone();
        let appended_progress = payload.operation.progress.clone();
        let record = WireRecord::new(payload)?;
        let mut encoded = serde_json::to_vec(&record)
            .context("failed to encode rebalance executor journal record")?;
        ensure!(
            encoded.len() < MAX_LINE_BYTES,
            "rebalance executor journal record is too large"
        );
        encoded.push(b'\n');
        if let Err(error) = self
            .file
            .write_all(&encoded)
            .and_then(|()| self.file.sync_data())
        {
            self.poisoned = true;
            return Err(error)
                .context("failed to durably append rebalance executor journal record");
        }
        self.operations = next_operations;
        if let Some(previous_progress) = previous_progress {
            if matches!(
                appended_progress,
                RebalanceExecutionProgress::Quarantined { .. }
            ) {
                self.progress_before_quarantine
                    .insert(appended_operation_id.clone(), previous_progress.clone());
            } else if matches!(
                previous_progress,
                RebalanceExecutionProgress::Quarantined { .. }
            ) {
                let count = self
                    .quarantine_reopen_counts
                    .entry(appended_operation_id.clone())
                    .or_insert(0);
                *count = count
                    .checked_add(1)
                    .context("rebalance quarantine reopen count overflow")?;
            }
            if retryable_travel_rule_ownership_reopen(&previous_progress, &appended_progress) {
                let count = self
                    .travel_rule_ownership_reopen_counts
                    .entry(appended_operation_id.clone())
                    .or_insert(0);
                *count = count
                    .checked_add(1)
                    .context("Travel Rule ownership rejection reopen count overflow")?;
            }
        }
        self.operation_started_at_unix_ms = next_started_at;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .context("rebalance executor journal sequence overflow")?;
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RebalanceRisk {
    pub transfer_count: u16,
    pub active_transfer_count: usize,
    pub failed_transfer_count: usize,
    pub token_a_debit: U256,
    pub token_b_debit: U256,
    pub token_a_maximum_fee: U256,
    pub token_b_maximum_fee: U256,
    pub additional_token_debit: BTreeMap<String, U256>,
    pub additional_token_maximum_fee: BTreeMap<String, U256>,
    pub first_started_at_unix_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WirePayload {
    version: u16,
    sequence: u64,
    recorded_at_unix_ms: u64,
    operation: RebalanceExecutionOperation,
}

#[derive(Clone, Debug, Serialize)]
struct WireRecord {
    payload: WirePayload,
    checksum_sha256: String,
}

impl WireRecord {
    fn new(payload: WirePayload) -> anyhow::Result<Self> {
        let checksum_sha256 = checksum(&payload)?;
        Ok(Self {
            payload,
            checksum_sha256,
        })
    }
}

#[derive(Debug, Deserialize)]
struct RawWireRecord<'a> {
    #[serde(borrow)]
    payload: &'a RawValue,
    checksum_sha256: String,
}

impl RawWireRecord<'_> {
    fn validate_checksum(&self) -> anyhow::Result<()> {
        ensure!(
            self.checksum_sha256 == checksum_bytes(self.payload.get().as_bytes()),
            "rebalance executor journal checksum mismatch"
        );
        Ok(())
    }
}

fn apply_snapshot(
    operations: &mut BTreeMap<String, RebalanceExecutionOperation>,
    operation: &RebalanceExecutionOperation,
    origin: TransitionOrigin,
) -> anyhow::Result<()> {
    validate_operation(operation)?;
    match operations.get(&operation.intent.operation_id) {
        Some(previous) => {
            ensure!(
                previous.intent == operation.intent,
                "rebalance operation intent changed"
            );
            validate_transition(
                &operation.intent,
                &previous.progress,
                &operation.progress,
                origin,
            )?;
        }
        None => ensure!(
            matches!(
                operation.progress,
                RebalanceExecutionProgress::IntentRecorded
            ),
            "rebalance operation does not begin with an intent"
        ),
    }
    operations.insert(operation.intent.operation_id.clone(), operation.clone());
    Ok(())
}

fn validate_request(request: &RebalanceExecutionRequest) -> anyhow::Result<()> {
    ensure!(
        !request.token_symbol.is_empty()
            && request.token_symbol.len() <= 16
            && request
                .token_symbol
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit()),
        "rebalance executor token symbol is invalid"
    );
    ensure!(
        request.token_decimals <= 36,
        "rebalance executor token decimals are invalid"
    );
    ensure!(
        request.token_contract != Address::ZERO,
        "rebalance executor token contract is zero"
    );
    ensure!(
        request.wallet_owner != Address::ZERO,
        "rebalance executor wallet owner is zero"
    );
    ensure!(
        !request.action.amount.is_zero(),
        "rebalance executor amount is zero"
    );
    ensure!(
        !request.revalidation_start_balance.is_zero(),
        "rebalance executor revalidation start balance is zero"
    );
    let authority_matches_route = match (&request.authority, &request.action.route) {
        (
            RebalanceExecutionAuthority::WorldChainV12,
            Route::Direct { chain_id: 480, .. }
            | Route::Across {
                wallet_chain_id: 480,
                ..
            },
        ) => true,
        (
            RebalanceExecutionAuthority::ArbitrumFullLive,
            Route::Direct {
                chain_id: 42_161,
                binance_network,
            },
        ) => binance_network == "ARBITRUM",
        _ => false,
    };
    ensure!(
        authority_matches_route,
        "rebalance execution authority does not own the selected route"
    );
    ensure!(
        (request.authority == RebalanceExecutionAuthority::ArbitrumFullLive)
            == (request.maximum_fee.is_some() && request.approval_session_id.is_some()),
        "only Arbitrum production rebalance requests carry fee and approval-session authority"
    );
    ensure!(
        request.authority == RebalanceExecutionAuthority::ArbitrumFullLive
            || (request.maximum_fee.is_none() && request.approval_session_id.is_none()),
        "non-Arbitrum rebalance request carries Arbitrum production authority"
    );
    if let Some(maximum_fee) = request.maximum_fee {
        ensure!(
            request.action.direction == Direction::WalletToBinance || !maximum_fee.is_zero(),
            "rebalance Binance withdrawal maximum fee is zero"
        );
    }
    match request.action.direction {
        Direction::BinanceToWallet => ensure!(
            request.action.amount <= request.binance_balance_before,
            "rebalance executor amount exceeds Binance balance"
        ),
        Direction::WalletToBinance => ensure!(
            request.action.amount <= request.wallet_balance_before,
            "rebalance executor amount exceeds wallet balance"
        ),
    }
    let total = request
        .binance_balance_before
        .checked_add(request.wallet_balance_before)
        .context("rebalance executor request balance overflow")?;
    let required_total = request
        .revalidation_start_balance
        .checked_mul(U256::from(2))
        .context("rebalance executor revalidation threshold overflow")?;
    ensure!(
        required_total <= total,
        "rebalance executor revalidation threshold exceeds total inventory"
    );
    Ok(())
}

fn validate_operation(operation: &RebalanceExecutionOperation) -> anyhow::Result<()> {
    let intent = &operation.intent;
    ensure!(
        !intent.operation_id.is_empty() && intent.operation_id.len() <= 96,
        "rebalance operation id is invalid"
    );
    ensure!(
        intent.fingerprint.len() == 64
            && intent
                .fingerprint
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit()),
        "rebalance operation fingerprint is invalid"
    );
    ensure!(
        intent.withdraw_order_id.len() >= 8
            && intent.withdraw_order_id.len() <= 64
            && intent
                .withdraw_order_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric()),
        "rebalance withdrawal client id is invalid"
    );
    ensure!(
        !intent.amount.is_zero(),
        "rebalance operation amount is zero"
    );
    ensure!(
        !intent.token_symbol.is_empty()
            && intent.token_symbol.len() <= 16
            && intent
                .token_symbol
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit()),
        "rebalance operation token symbol is invalid"
    );
    ensure!(
        intent.token_decimals <= 36,
        "rebalance operation token decimals are invalid"
    );
    ensure!(
        intent.token_contract != Address::ZERO && intent.wallet_owner != Address::ZERO,
        "rebalance operation token contract or wallet is zero"
    );
    match intent.direction {
        Direction::BinanceToWallet => ensure!(
            intent.amount <= intent.binance_balance_before,
            "rebalance operation amount exceeds Binance balance"
        ),
        Direction::WalletToBinance => ensure!(
            intent.amount <= intent.wallet_balance_before,
            "rebalance operation amount exceeds wallet balance"
        ),
    }
    if !intent.revalidation_start_balance.is_zero() {
        let total = intent
            .binance_balance_before
            .checked_add(intent.wallet_balance_before)
            .context("rebalance operation balance overflow")?;
        let required_total = intent
            .revalidation_start_balance
            .checked_mul(U256::from(2))
            .context("rebalance operation revalidation threshold overflow")?;
        ensure!(
            required_total <= total,
            "rebalance operation revalidation threshold exceeds total inventory"
        );
    }
    if let RebalanceExecutionProgress::Failed { reason }
    | RebalanceExecutionProgress::Quarantined { reason } = &operation.progress
    {
        ensure!(
            !reason.is_empty() && reason.len() <= MAX_REASON_BYTES,
            "rebalance failure reason is invalid"
        );
    }
    if let Some(maximum_fee) = &intent.maximum_fee_base_units {
        let maximum_fee =
            U256::from_str(maximum_fee).context("rebalance canary maximum fee is not a uint256")?;
        ensure!(
            intent.direction == Direction::WalletToBinance || !maximum_fee.is_zero(),
            "rebalance canary Binance withdrawal maximum fee is zero"
        );
        ensure!(
            intent
                .scope
                .as_ref()
                .is_some_and(|scope| scope.network_id == "chain:42161"),
            "Arbitrum rebalance fee authority has the wrong journal scope"
        );
        if let Some(approval_session_id) = intent.approval_session_id.as_deref() {
            ensure!(
                (8..=64).contains(&approval_session_id.len())
                    && approval_session_id.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':')
                    }),
                "rebalance approval session id is invalid"
            );
        }
    } else {
        ensure!(
            intent.approval_session_id.is_none(),
            "rebalance intent has an approval session without canary fee authority"
        );
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransitionOrigin {
    JournalReplay,
    LiveAppend,
}

fn ownership_guard_quarantine(reason: &str) -> bool {
    matches!(
        reason,
        "Binance Travel Rule ownership verification is not unique for the exact wallet and network"
            | "Binance Travel Rule ownership verification is absent for the exact wallet, network, and token"
            | "Binance Travel Rule ownership verification is absent for the exact wallet and network"
    )
}

fn signature_encoding_quarantine(reason: &str) -> bool {
    reason
        == "Binance Travel Rule withdrawal submission failed with HTTP 400 Bad Request, code -1022: Signature for this request is not valid."
}

fn retryable_travel_rule_ownership_failure(reason: &str) -> bool {
    reason
        == "terminal Binance Travel Rule withdrawal rejection: Binance Travel Rule withdrawal submission failed with HTTP 400 Bad Request, code -4024: [031031] User does not own this currency."
}

fn retryable_travel_rule_ownership_reopen(
    previous: &RebalanceExecutionProgress,
    next: &RebalanceExecutionProgress,
) -> bool {
    matches!(
        (previous, next),
        (
            RebalanceExecutionProgress::Failed { reason },
            RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                api_mode,
                reconciliation_queries: 0,
                ..
            },
        ) if retryable_travel_rule_ownership_failure(reason)
            && api_mode == TRAVEL_RULE_BINANCE_WITHDRAWAL_API_MODE
    )
}

fn corrected_guard_quarantine(reason: &str) -> bool {
    reason == "unindexed Binance withdrawal retry found a destination-wallet balance change"
        || reason
            == "rebalance intent has no indexed Binance master transfer; operator review required"
        || ownership_guard_quarantine(reason)
        || signature_encoding_quarantine(reason)
}

fn corrected_across_deposit_chain_quarantine(
    intent: &RebalanceExecutionIntent,
    reason: &str,
    progress_before_quarantine: &RebalanceExecutionProgress,
) -> bool {
    matches!(
        (&intent.route, intent.direction, progress_before_quarantine),
        (
            Route::Across {
                bridge_chain_id,
                wallet_chain_id,
                ..
            },
            Direction::WalletToBinance,
            RebalanceExecutionProgress::DepositTransferMined { chain_id, .. },
        ) if reason == "illegal rebalance executor state transition"
            && bridge_chain_id != wallet_chain_id
            && chain_id == bridge_chain_id
    )
}

fn across_fill_timeout_quarantine(reason: &str) -> bool {
    reason == "timed out waiting for Across fill"
}

fn reconciled_arbitrum_deposit_transition(
    intent: &RebalanceExecutionIntent,
    previous: &RebalanceExecutionProgress,
    next: &RebalanceExecutionProgress,
) -> bool {
    matches!(
        (&intent.route, intent.direction, previous, next),
        (
            Route::Direct {
                chain_id: 42_161,
                binance_network,
            },
            Direction::WalletToBinance,
            RebalanceExecutionProgress::Quarantined { reason },
            RebalanceExecutionProgress::DepositTransferMined {
                chain_id: 42_161,
                transaction_hash,
            },
        ) if binance_network == "ARBITRUM"
            && reason.starts_with("DEX outcome unknown:")
            && *transaction_hash != B256::ZERO
    )
}

fn reconciled_across_fill_transition(
    intent: &RebalanceExecutionIntent,
    previous: &RebalanceExecutionProgress,
    next: &RebalanceExecutionProgress,
) -> bool {
    matches!(
        (&intent.route, previous, next),
        (
            Route::Across { .. },
            RebalanceExecutionProgress::Quarantined { reason },
            RebalanceExecutionProgress::AcrossFilled {
                fill_transaction_hash,
                received_base_units,
            },
        ) if across_fill_timeout_quarantine(reason)
            && *fill_transaction_hash != B256::ZERO
            && !received_base_units.is_zero()
    )
}

#[allow(clippy::match_like_matches_macro)]
fn validate_transition(
    intent: &RebalanceExecutionIntent,
    previous: &RebalanceExecutionProgress,
    next: &RebalanceExecutionProgress,
    origin: TransitionOrigin,
) -> anyhow::Result<()> {
    let approved_terminal_retry = matches!(
        (previous, next),
        (
            RebalanceExecutionProgress::Failed { reason },
            RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                api_mode,
                ..
            },
        ) if
            (
                reason.contains("approved deterministic Travel Rule rejection")
                    && (
                        api_mode == "standard"
                            || (
                                origin == TransitionOrigin::JournalReplay
                                    && matches!(api_mode.as_str(), "local_entity" | "travel_rule")
                            )
                    )
            )
            || (
                reason == "terminal Binance local-entity withdrawal rejection: Binance local-entity withdrawal submission failed with HTTP 400 Bad Request, code -4024: [031031] User does not own this currency."
                    && api_mode == "standard"
            )
            || (retryable_travel_rule_ownership_failure(reason)
                && api_mode == TRAVEL_RULE_BINANCE_WITHDRAWAL_API_MODE)
    );
    let approved_terminal_manual_completion = matches!(
        (previous, next),
        (
            RebalanceExecutionProgress::Failed { reason },
            RebalanceExecutionProgress::Completed { .. },
        ) if reason == "terminal Binance standard withdrawal rejection after approved local-entity endpoint correction: Binance standard withdrawal submission failed with HTTP 400 Bad Request, code -4104: Please note that withdrawals are not permitted due to travel rule restrictions. To facilitate the withdrawal process, please refer to Travel Rule documentation."
    );
    let approved_quarantine_retry = matches!(
        (previous, next),
        (
            RebalanceExecutionProgress::Quarantined { reason },
            RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                api_mode,
                reconciliation_queries,
                ..
            },
        ) if (reason == "unindexed Binance withdrawal retry found a destination-wallet balance change"
            && api_mode == "travel_rule_ae_self_owned"
            && *reconciliation_queries == 1)
            || (ownership_guard_quarantine(reason)
                && matches!(
                    api_mode.as_str(),
                    "travel_rule_required_after_standard_-4104" | "travel_rule_ae_self_owned"
                )
                && *reconciliation_queries == 0)
            || (signature_encoding_quarantine(reason)
                && api_mode == "travel_rule_ae_self_owned"
                && *reconciliation_queries == 0)
    ) || matches!(
        (previous, next),
        (
            RebalanceExecutionProgress::Quarantined { reason },
            RebalanceExecutionProgress::BinanceWithdrawalRetryAuthorized { api_mode, .. },
        ) if ownership_guard_quarantine(reason)
            && api_mode == "travel_rule_ae_self_owned"
    ) || matches!(
        (previous, next),
        (
            RebalanceExecutionProgress::Quarantined { reason },
            RebalanceExecutionProgress::IntentRecorded,
        ) if reason
            == "rebalance intent has no indexed Binance master transfer; operator review required"
    ) || reconciled_arbitrum_deposit_transition(
        intent, previous, next,
    ) || reconciled_across_fill_transition(intent, previous, next)
        || matches!(previous, RebalanceExecutionProgress::Quarantined { reason }
            if corrected_across_deposit_chain_quarantine(intent, reason, next));
    ensure!(
        !previous.terminal()
            || approved_terminal_retry
            || approved_terminal_manual_completion
            || approved_quarantine_retry,
        "rebalance operation is already terminal"
    );
    if approved_quarantine_retry {
        return validate_progress_evidence(intent, next);
    }
    if matches!(
        next,
        RebalanceExecutionProgress::Failed { .. } | RebalanceExecutionProgress::Quarantined { .. }
    ) {
        return Ok(());
    }
    use RebalanceExecutionProgress as P;
    let allowed = match (&intent.route, intent.direction, previous, next) {
        (
            Route::Direct { .. },
            Direction::BinanceToWallet,
            P::IntentRecorded,
            P::BinanceTransferSubmitted { .. },
        ) => true,
        (
            Route::Direct { .. },
            Direction::BinanceToWallet,
            P::BinanceTransferSubmitted { .. },
            P::BinanceTransferCompleted { .. },
        ) => true,
        (
            Route::Direct { .. },
            Direction::BinanceToWallet,
            P::BinanceTransferCompleted { .. },
            P::BinanceWithdrawalSubmissionStarted { .. },
        ) => true,
        // Journals written before the submission-intent state was introduced
        // legitimately advanced straight to Submitted. New runtime code never
        // emits this transition, but replay must remain backward compatible.
        (
            Route::Direct { .. },
            Direction::BinanceToWallet,
            P::BinanceTransferCompleted { .. },
            P::BinanceWithdrawalSubmitted { .. },
        ) => true,
        (
            Route::Direct { .. },
            Direction::BinanceToWallet,
            P::BinanceWithdrawalSubmissionStarted { .. },
            P::BinanceWithdrawalSubmitted { .. },
        ) => true,
        (
            Route::Direct { .. } | Route::Across { .. },
            Direction::BinanceToWallet,
            P::BinanceWithdrawalSubmissionStarted {
                api_mode: previous_mode,
                reconciliation_queries: previous,
                ..
            },
            P::BinanceWithdrawalSubmissionStarted {
                api_mode: next_mode,
                reconciliation_queries: next,
                ..
            },
        ) => {
            (previous_mode == next_mode && *previous == 0 && *next == 1)
                || (previous_mode == "standard"
                    && next_mode == "travel_rule_required_after_standard_-4104"
                    && *previous == 0
                    && *next == 0)
                || (previous_mode == "travel_rule_required_after_standard_-4104"
                    && next_mode == "travel_rule_ae_self_owned"
                    && *previous == 0
                    && *next == 0)
        }
        (
            Route::Direct { .. } | Route::Across { .. },
            Direction::BinanceToWallet,
            P::BinanceWithdrawalSubmissionStarted {
                api_mode: previous_mode,
                reconciliation_queries: 1,
                ..
            },
            P::BinanceWithdrawalRetryAuthorized {
                api_mode: next_mode,
                ..
            },
        ) => previous_mode == next_mode,
        (
            Route::Direct { .. } | Route::Across { .. },
            Direction::BinanceToWallet,
            P::BinanceWithdrawalRetryAuthorized {
                api_mode: previous_mode,
                ..
            },
            P::BinanceWithdrawalSubmissionStarted {
                api_mode: next_mode,
                reconciliation_queries: 0,
                ..
            },
        ) => previous_mode == next_mode,
        (
            Route::Direct { .. } | Route::Across { .. },
            Direction::BinanceToWallet,
            P::BinanceWithdrawalSubmissionStarted {
                reconciliation_queries: 1,
                ..
            }
            | P::BinanceWithdrawalRetryAuthorized { .. },
            P::BinanceMasterReturnSubmissionStarted {
                reconciliation_queries: 0,
                ..
            },
        ) => true,
        (
            Route::Direct { .. } | Route::Across { .. },
            Direction::BinanceToWallet,
            P::BinanceMasterReturnSubmissionStarted {
                client_transaction_id: previous_client_id,
                revalidation_binance_balance: previous_binance,
                revalidation_wallet_balance: previous_wallet,
                revalidation_required_withdrawal: previous_required,
                reconciliation_queries: 0,
            },
            P::BinanceMasterReturnSubmissionStarted {
                client_transaction_id: next_client_id,
                revalidation_binance_balance: next_binance,
                revalidation_wallet_balance: next_wallet,
                revalidation_required_withdrawal: next_required,
                reconciliation_queries: 1,
            },
        ) => {
            previous_client_id == next_client_id
                && previous_binance == next_binance
                && previous_wallet == next_wallet
                && previous_required == next_required
        }
        (
            Route::Direct { .. } | Route::Across { .. },
            Direction::BinanceToWallet,
            P::BinanceMasterReturnSubmissionStarted {
                client_transaction_id: previous_client_id,
                revalidation_binance_balance: previous_binance,
                revalidation_wallet_balance: previous_wallet,
                revalidation_required_withdrawal: previous_required,
                reconciliation_queries: 1,
            },
            P::BinanceMasterReturnSubmitted {
                client_transaction_id: next_client_id,
                revalidation_binance_balance: next_binance,
                revalidation_wallet_balance: next_wallet,
                revalidation_required_withdrawal: next_required,
                ..
            },
        ) => {
            previous_client_id == next_client_id
                && previous_binance == next_binance
                && previous_wallet == next_wallet
                && previous_required == next_required
        }
        (
            Route::Direct { .. } | Route::Across { .. },
            Direction::BinanceToWallet,
            P::BinanceMasterReturnSubmitted {
                transaction_id,
                revalidation_binance_balance: previous_binance,
                revalidation_wallet_balance: previous_wallet,
                revalidation_required_withdrawal: previous_required,
                ..
            },
            P::CancelledStale {
                master_return_transaction_id,
                revalidation_binance_balance: next_binance,
                revalidation_wallet_balance: next_wallet,
                revalidation_required_withdrawal: next_required,
            },
        ) => {
            transaction_id == master_return_transaction_id
                && previous_binance == next_binance
                && previous_wallet == next_wallet
                && previous_required == next_required
        }
        // A separately approved operator withdrawal can satisfy a fail-closed
        // unindexed submission. Its recovery validates the exact Binance
        // record and on-chain receipt before appending this single terminal
        // transition, so a crash cannot leave a synthetic Submitted state.
        (
            Route::Direct { .. },
            Direction::BinanceToWallet,
            P::BinanceWithdrawalSubmissionStarted { .. },
            P::Completed { .. },
        ) => true,
        (
            Route::Direct { .. },
            Direction::BinanceToWallet,
            P::Failed { .. },
            P::BinanceWithdrawalSubmissionStarted { .. },
        ) => approved_terminal_retry,
        (
            Route::Direct { .. },
            Direction::BinanceToWallet,
            P::Failed { .. },
            P::Completed { .. },
        ) => approved_terminal_manual_completion,
        (
            Route::Direct { .. },
            Direction::BinanceToWallet,
            P::BinanceWithdrawalSubmitted { .. },
            P::Completed { .. },
        ) => true,
        (
            Route::Direct { .. },
            Direction::WalletToBinance,
            P::IntentRecorded,
            P::DepositTransferMined { .. },
        ) => true,
        (
            Route::Direct { .. },
            Direction::WalletToBinance,
            P::DepositTransferMined { .. },
            P::BinanceCredited { .. },
        ) => true,
        (
            Route::Direct { .. },
            Direction::WalletToBinance,
            P::DepositTransferMined {
                chain_id,
                transaction_hash,
            },
            P::DepositQuestionnaireSubmissionStarted {
                chain_id: next_chain_id,
                transaction_hash: next_transaction_hash,
                ..
            },
        ) => chain_id == next_chain_id && transaction_hash == next_transaction_hash,
        (
            Route::Direct { .. },
            Direction::WalletToBinance,
            P::DepositQuestionnaireSubmissionStarted { deposit_id, .. },
            P::BinanceCredited {
                deposit_id: credited_deposit_id,
                ..
            },
        ) => deposit_id == credited_deposit_id,
        (
            Route::Direct { .. },
            Direction::WalletToBinance,
            P::BinanceCredited { .. },
            P::Completed { .. },
        ) => true,
        (
            Route::Across { .. },
            Direction::BinanceToWallet,
            P::IntentRecorded,
            P::BinanceTransferSubmitted { .. },
        ) => true,
        (
            Route::Across { .. },
            Direction::BinanceToWallet,
            P::BinanceTransferSubmitted { .. },
            P::BinanceTransferCompleted { .. },
        ) => true,
        (
            Route::Across { .. },
            Direction::BinanceToWallet,
            P::BinanceTransferCompleted { .. },
            P::BinanceWithdrawalSubmissionStarted { .. },
        ) => true,
        // Legacy replay compatibility; new submissions persist Started first.
        (
            Route::Across { .. },
            Direction::BinanceToWallet,
            P::BinanceTransferCompleted { .. },
            P::BinanceWithdrawalSubmitted { .. },
        ) => true,
        (
            Route::Across { .. },
            Direction::BinanceToWallet,
            P::BinanceWithdrawalSubmissionStarted { .. },
            P::BinanceWithdrawalSubmitted { .. },
        ) => true,
        (
            Route::Across { .. },
            Direction::BinanceToWallet,
            P::BinanceWithdrawalSubmitted { .. },
            P::FundsOnBridge { .. },
        ) => true,
        (
            Route::Across { .. },
            Direction::BinanceToWallet,
            P::FundsOnBridge { .. },
            P::ApprovalMined { .. },
        ) => true,
        (
            Route::Across { .. },
            Direction::BinanceToWallet,
            P::FundsOnBridge { .. },
            P::BridgePrepared { .. },
        ) => true,
        (
            Route::Across { .. },
            Direction::BinanceToWallet,
            P::ApprovalMined { .. },
            P::BridgePrepared { .. },
        ) => true,
        (
            Route::Across { .. },
            Direction::WalletToBinance,
            P::IntentRecorded,
            P::ApprovalMined { .. },
        ) => true,
        (
            Route::Across { .. },
            Direction::WalletToBinance,
            P::IntentRecorded,
            P::BridgePrepared { .. },
        ) => true,
        (
            Route::Across { .. },
            Direction::WalletToBinance,
            P::ApprovalMined { .. },
            P::BridgePrepared { .. },
        ) => true,
        (Route::Across { .. }, _, P::BridgePrepared { .. }, P::BridgeMined { .. }) => true,
        (Route::Across { .. }, _, P::BridgePrepared { .. }, P::BridgePrepared { .. }) => true,
        (Route::Across { .. }, _, P::BridgeMined { .. }, P::AcrossFilled { .. }) => true,
        (
            Route::Across { .. },
            Direction::BinanceToWallet,
            P::AcrossFilled { .. },
            P::Completed { .. },
        ) => true,
        (
            Route::Across { .. },
            Direction::WalletToBinance,
            P::AcrossFilled { .. },
            P::DepositTransferMined { .. },
        ) => true,
        (
            Route::Across { .. },
            Direction::WalletToBinance,
            P::DepositTransferMined { .. },
            P::BinanceCredited { .. },
        ) => true,
        (
            Route::Across { .. },
            Direction::WalletToBinance,
            P::DepositTransferMined {
                chain_id,
                transaction_hash,
            },
            P::DepositQuestionnaireSubmissionStarted {
                chain_id: next_chain_id,
                transaction_hash: next_transaction_hash,
                ..
            },
        ) => chain_id == next_chain_id && transaction_hash == next_transaction_hash,
        (
            Route::Across { .. },
            Direction::WalletToBinance,
            P::DepositQuestionnaireSubmissionStarted { deposit_id, .. },
            P::BinanceCredited {
                deposit_id: credited_deposit_id,
                ..
            },
        ) => deposit_id == credited_deposit_id,
        (
            Route::Across { .. },
            Direction::WalletToBinance,
            P::BinanceCredited { .. },
            P::Completed { .. },
        ) => true,
        _ => false,
    };
    ensure!(allowed, "illegal rebalance executor state transition");
    validate_progress_evidence(intent, next)
}

fn validate_progress_evidence(
    intent: &RebalanceExecutionIntent,
    progress: &RebalanceExecutionProgress,
) -> anyhow::Result<()> {
    use RebalanceExecutionProgress as P;
    match progress {
        P::BinanceTransferSubmitted { transaction_id, .. }
        | P::BinanceTransferCompleted { transaction_id, .. } => {
            ensure!(*transaction_id > 0, "rebalance Binance transfer id is zero")
        }
        P::BinanceWithdrawalSubmissionStarted {
            api_mode,
            reconciliation_queries,
            ..
        } => {
            ensure!(
                matches!(
                    api_mode.as_str(),
                    "local_entity"
                        | "standard"
                        | "travel_rule"
                        | "travel_rule_required_after_standard_-4104"
                        | "travel_rule_ae_self_owned"
                ),
                "rebalance Binance withdrawal submission API mode is invalid"
            );
            ensure!(
                *reconciliation_queries <= 1,
                "rebalance Binance withdrawal reconciliation query limit exceeded"
            );
        }
        P::BinanceWithdrawalRetryAuthorized {
            api_mode,
            master_free_base_units,
            master_locked_base_units,
            ..
        } => {
            ensure!(
                matches!(api_mode.as_str(), "standard" | "travel_rule_ae_self_owned"),
                "rebalance Binance withdrawal retry API mode is invalid"
            );
            ensure!(
                *master_free_base_units == intent.amount,
                "rebalance Binance withdrawal retry did not preserve exact master inventory"
            );
            ensure!(
                master_locked_base_units.is_zero(),
                "rebalance Binance withdrawal retry retained locked master inventory"
            );
        }
        P::BinanceMasterReturnSubmissionStarted {
            client_transaction_id,
            revalidation_binance_balance,
            revalidation_wallet_balance,
            revalidation_required_withdrawal,
            reconciliation_queries,
        } => {
            validate_master_return_evidence(
                intent,
                client_transaction_id,
                *revalidation_binance_balance,
                *revalidation_wallet_balance,
                *revalidation_required_withdrawal,
            )?;
            ensure!(
                *reconciliation_queries <= 1,
                "rebalance master-return reconciliation query limit exceeded"
            );
        }
        P::BinanceMasterReturnSubmitted {
            client_transaction_id,
            transaction_id,
            revalidation_binance_balance,
            revalidation_wallet_balance,
            revalidation_required_withdrawal,
        } => {
            validate_master_return_evidence(
                intent,
                client_transaction_id,
                *revalidation_binance_balance,
                *revalidation_wallet_balance,
                *revalidation_required_withdrawal,
            )?;
            ensure!(*transaction_id > 0, "rebalance master-return id is zero");
        }
        P::BinanceWithdrawalSubmitted {
            submission_reference,
            ..
        } => {
            ensure!(
                !submission_reference.is_empty() && submission_reference.len() <= 128,
                "rebalance Binance withdrawal submission reference is invalid"
            );
        }
        P::FundsOnBridge {
            withdrawal_id,
            transaction_id,
            received_base_units,
        } => {
            ensure!(
                !withdrawal_id.is_empty(),
                "rebalance withdrawal id is empty"
            );
            validate_hash_text(transaction_id)?;
            ensure!(
                !received_base_units.is_zero() && *received_base_units <= intent.amount,
                "rebalance bridge receipt amount is invalid"
            );
        }
        P::ApprovalMined {
            chain_id,
            input_amount,
            ..
        } => {
            ensure!(*chain_id > 0, "rebalance transaction chain id is zero");
            ensure!(
                *input_amount <= intent.amount,
                "rebalance approval input exceeds the operation amount"
            );
        }
        P::DepositTransferMined { chain_id, .. } => {
            ensure!(*chain_id > 0, "rebalance transaction chain id is zero")
        }
        P::DepositQuestionnaireSubmissionStarted {
            chain_id,
            deposit_id,
            ..
        } => {
            ensure!(*chain_id > 0, "rebalance transaction chain id is zero");
            ensure!(
                !deposit_id.is_empty() && deposit_id.len() <= 128,
                "rebalance deposit questionnaire id is invalid"
            );
        }
        P::BridgeMined {
            origin_chain_id,
            minimum_output_amount,
            ..
        } => {
            ensure!(
                *origin_chain_id > 0,
                "rebalance bridge origin chain is zero"
            );
            ensure!(
                !minimum_output_amount.is_zero(),
                "rebalance bridge minimum output is zero"
            );
        }
        P::BridgePrepared {
            origin_chain_id,
            input_amount,
            target,
            calldata,
            calldata_hash,
            minimum_output_amount,
            ..
        } => {
            ensure!(
                *origin_chain_id > 0,
                "rebalance bridge origin chain is zero"
            );
            ensure!(!input_amount.is_zero(), "rebalance bridge input is zero");
            ensure!(*target != Address::ZERO, "rebalance bridge target is zero");
            ensure!(!calldata.is_empty(), "rebalance bridge calldata is empty");
            ensure!(
                keccak256(calldata) == *calldata_hash,
                "rebalance bridge calldata hash does not match"
            );
            ensure!(
                !minimum_output_amount.is_zero() && minimum_output_amount <= input_amount,
                "rebalance bridge minimum output is invalid"
            );
        }
        P::AcrossFilled {
            received_base_units,
            ..
        } => ensure!(
            !received_base_units.is_zero(),
            "rebalance Across receipt is zero"
        ),
        P::BinanceCredited {
            deposit_id,
            credited_base_units,
        } => {
            ensure!(
                !deposit_id.is_empty(),
                "rebalance Binance deposit id is empty"
            );
            ensure!(
                !credited_base_units.is_zero(),
                "rebalance Binance credit is zero"
            );
        }
        P::Completed {
            binance_balance_after,
            wallet_balance_after,
        } => {
            let total = binance_balance_after
                .checked_add(*wallet_balance_after)
                .context("rebalance completed balance overflow")?;
            ensure!(!total.is_zero(), "rebalance completed balances are zero");
        }
        P::CancelledStale {
            master_return_transaction_id,
            revalidation_binance_balance,
            revalidation_wallet_balance,
            revalidation_required_withdrawal,
        } => {
            validate_master_return_evidence(
                intent,
                &stale_master_return_client_id(intent),
                *revalidation_binance_balance,
                *revalidation_wallet_balance,
                *revalidation_required_withdrawal,
            )?;
            ensure!(
                *master_return_transaction_id > 0,
                "rebalance cancelled-stale master-return id is zero"
            );
        }
        P::IntentRecorded | P::Failed { .. } | P::Quarantined { .. } => {}
    }
    Ok(())
}

fn validate_master_return_evidence(
    intent: &RebalanceExecutionIntent,
    client_transaction_id: &str,
    revalidation_binance_balance: U256,
    revalidation_wallet_balance: U256,
    revalidation_required_withdrawal: U256,
) -> anyhow::Result<()> {
    ensure!(
        intent.direction == Direction::BinanceToWallet,
        "rebalance master-return evidence belongs to the wrong direction"
    );
    ensure!(
        client_transaction_id == stale_master_return_client_id(intent),
        "rebalance master-return client id is not deterministic"
    );
    ensure!(
        revalidation_required_withdrawal < intent.amount
            || (!intent.revalidation_start_balance.is_zero()
                && revalidation_wallet_balance >= intent.revalidation_start_balance),
        "rebalance stale cancellation is still required by current balances"
    );
    let total = revalidation_binance_balance
        .checked_add(revalidation_wallet_balance)
        .context("rebalance stale-cancellation balance overflow")?;
    ensure!(
        !total.is_zero(),
        "rebalance stale-cancellation balances are zero"
    );
    Ok(())
}

pub fn stale_master_return_client_id(intent: &RebalanceExecutionIntent) -> String {
    format!("rc{}", &intent.fingerprint[..30])
}

fn request_fingerprint(request: &RebalanceExecutionRequest) -> anyhow::Result<String> {
    let encoded = serde_json::to_vec(&serde_json::json!({
        "token": request.token_symbol,
        "authority": request.authority,
        "decimals": request.token_decimals,
        "contract": format!("{:#x}", request.token_contract),
        "wallet": format!("{:#x}", request.wallet_owner),
        "direction": request.action.direction,
        "route": request.action.route,
        "amount": request.action.amount.to_string(),
        "binance_before": request.binance_balance_before.to_string(),
        "wallet_before": request.wallet_balance_before.to_string(),
        "revalidation_start_balance": request.revalidation_start_balance.to_string(),
        "maximum_fee": request.maximum_fee.map(|value| value.to_string()),
        "approval_session_id": request.approval_session_id,
    }))?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

fn checksum(payload: &WirePayload) -> anyhow::Result<String> {
    Ok(checksum_bytes(&serde_json::to_vec(payload)?))
}

fn checksum_bytes(payload: &[u8]) -> String {
    format!("{:x}", Sha256::digest(payload))
}

fn validate_hash_text(value: &str) -> anyhow::Result<()> {
    ensure!(
        value.len() == 66
            && value.starts_with("0x")
            && value[2..].bytes().all(|byte| byte.is_ascii_hexdigit()),
        "rebalance transaction hash is invalid"
    );
    Ok(())
}

fn unix_timestamp_ms() -> anyhow::Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_millis()
        .try_into()
        .context("system timestamp exceeds u64")
}

#[cfg(unix)]
fn validate_permissions(file: &File) -> anyhow::Result<()> {
    let mode = file.metadata()?.permissions().mode();
    ensure!(
        mode & 0o077 == 0,
        "rebalance executor journal is group/world accessible"
    );
    Ok(())
}

#[cfg(not(unix))]
fn validate_permissions(_file: &File) -> anyhow::Result<()> {
    Ok(())
}

fn sync_parent(path: &Path) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)?
        .sync_all()
        .context("failed to sync rebalance executor journal parent")
}

mod u256_serde {
    use alloy_primitives::U256;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &U256, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<U256, D::Error> {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

mod address_serde {
    use std::str::FromStr;

    use alloy_primitives::Address;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &Address, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("{value:#x}"))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Address, D::Error> {
        let value = String::deserialize(deserializer)?;
        Address::from_str(&value).map_err(serde::de::Error::custom)
    }
}

mod b256_serde {
    use std::str::FromStr;

    use alloy_primitives::B256;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &B256, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("{value:#x}"))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<B256, D::Error> {
        let value = String::deserialize(deserializer)?;
        B256::from_str(&value).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, OpenOptions},
        io::Write,
        time::{SystemTime, UNIX_EPOCH},
    };

    use alloy_primitives::{Address, B256, U256, keccak256};

    use super::{
        RebalanceExecutionAuthority, RebalanceExecutionJournal, RebalanceExecutionProgress,
        RebalanceExecutionRequest, TRAVEL_RULE_BINANCE_WITHDRAWAL_API_MODE, WirePayload,
        WireRecord, stale_master_return_client_id,
    };
    use crate::rebalance::{Direction, RebalanceAction, Route};

    fn path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "poly-bot-executor-{name}-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn request(direction: Direction, route: Route) -> RebalanceExecutionRequest {
        let is_arbitrum = matches!(
            &route,
            Route::Direct {
                chain_id: 42_161,
                ..
            }
        );
        RebalanceExecutionRequest {
            authority: if is_arbitrum {
                RebalanceExecutionAuthority::ArbitrumFullLive
            } else {
                RebalanceExecutionAuthority::WorldChainV12
            },
            token_symbol: "USDC".to_owned(),
            token_decimals: 6,
            token_contract: Address::repeat_byte(0x11),
            wallet_owner: Address::repeat_byte(0x22),
            action: RebalanceAction {
                direction,
                amount: U256::from(2_000_000_u64),
                route,
            },
            binance_balance_before: U256::from(8_000_000_u64),
            wallet_balance_before: U256::from(8_000_000_u64),
            revalidation_start_balance: U256::from(3_200_000_u64),
            maximum_fee: is_arbitrum.then(|| U256::from(100_000_u64)),
            approval_session_id: is_arbitrum.then(|| "esp-usdc-arbitrum-full-live".to_owned()),
        }
    }

    fn write_replay_fixture(
        path: &std::path::Path,
        operations: Vec<super::RebalanceExecutionOperation>,
    ) {
        drop(RebalanceExecutionJournal::open(path).unwrap());
        let mut file = OpenOptions::new().append(true).open(path).unwrap();
        for (sequence, operation) in operations.into_iter().enumerate() {
            let record = WireRecord::new(WirePayload {
                version: super::VERSION,
                sequence: u64::try_from(sequence).unwrap(),
                recorded_at_unix_ms: u64::try_from(sequence + 1).unwrap(),
                operation,
            })
            .unwrap();
            let mut encoded = serde_json::to_vec(&record).unwrap();
            encoded.push(b'\n');
            file.write_all(&encoded).unwrap();
        }
        file.sync_all().unwrap();
    }

    #[test]
    fn rebalance_parent_is_account_network_and_strategy_scoped() {
        let path = path("scoped-scope");
        let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
        let operation = journal
            .reserve(&request(Direction::WalletToBinance, across()))
            .unwrap();
        let scope = operation.intent.scope.unwrap();
        assert_eq!(scope.schema_version, 2);
        assert_eq!(scope.account_id, "binance:trading-subaccount");
        assert_eq!(scope.network_id, "chain:10");
        assert_eq!(scope.strategy_id, "rebalance-world-chain-v12");
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    fn across() -> Route {
        Route::Across {
            binance_network: "OPTIMISM".to_owned(),
            bridge_chain_id: 10,
            wallet_chain_id: 480,
        }
    }

    fn direct_arbitrum() -> Route {
        Route::Direct {
            binance_network: "ARBITRUM".to_owned(),
            chain_id: 42_161,
        }
    }

    fn advance_to_across_deposit(
        journal: &mut RebalanceExecutionJournal,
        operation_id: &str,
        deposit_chain_id: u64,
    ) {
        journal
            .advance(
                operation_id,
                RebalanceExecutionProgress::BridgePrepared {
                    origin_chain_id: 480,
                    input_amount: U256::from(2_000_000_u64),
                    target: Address::repeat_byte(0x35),
                    calldata: vec![0x36],
                    calldata_hash: keccak256([0x36]),
                    minimum_output_amount: U256::from(1_990_000_u64),
                    destination_balance_before: U256::from(10_000_000_u64),
                },
            )
            .unwrap();
        journal
            .advance(
                operation_id,
                RebalanceExecutionProgress::BridgeMined {
                    origin_chain_id: 480,
                    transaction_hash: B256::repeat_byte(0x32),
                    minimum_output_amount: U256::from(1_990_000_u64),
                    destination_balance_before: U256::from(10_000_000_u64),
                },
            )
            .unwrap();
        journal
            .advance(
                operation_id,
                RebalanceExecutionProgress::AcrossFilled {
                    fill_transaction_hash: B256::repeat_byte(0x33),
                    received_base_units: U256::from(1_995_000_u64),
                },
            )
            .unwrap();
        journal
            .advance(
                operation_id,
                RebalanceExecutionProgress::DepositTransferMined {
                    chain_id: deposit_chain_id,
                    transaction_hash: B256::repeat_byte(0x34),
                },
            )
            .unwrap();
    }

    #[test]
    fn proven_absent_stale_withdrawal_returns_master_inventory_before_cancellation() {
        let path = path("stale-withdrawal-cancellation");
        let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
        let operation = journal
            .reserve(&request(Direction::BinanceToWallet, direct_arbitrum()))
            .unwrap();
        let operation_id = operation.intent.operation_id.clone();
        let return_client_id = stale_master_return_client_id(&operation.intent);
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceTransferSubmitted {
                    transaction_id: 10,
                    bridge_balance_before: U256::from(100),
                },
            )
            .unwrap();
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceTransferCompleted {
                    transaction_id: 10,
                    bridge_balance_before: U256::from(100),
                },
            )
            .unwrap();
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                    api_mode: "standard".to_owned(),
                    bridge_balance_before: U256::from(100),
                    reconciliation_queries: 0,
                },
            )
            .unwrap();
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                    api_mode: "standard".to_owned(),
                    bridge_balance_before: U256::from(100),
                    reconciliation_queries: 1,
                },
            )
            .unwrap();
        let start_return = |reconciliation_queries| {
            RebalanceExecutionProgress::BinanceMasterReturnSubmissionStarted {
                client_transaction_id: return_client_id.clone(),
                revalidation_binance_balance: U256::from(6_000_000_u64),
                revalidation_wallet_balance: U256::from(10_000_000_u64),
                revalidation_required_withdrawal: U256::ZERO,
                reconciliation_queries,
            }
        };
        journal.advance(&operation_id, start_return(0)).unwrap();
        journal.advance(&operation_id, start_return(1)).unwrap();
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceMasterReturnSubmitted {
                    client_transaction_id: return_client_id,
                    transaction_id: 11,
                    revalidation_binance_balance: U256::from(6_000_000_u64),
                    revalidation_wallet_balance: U256::from(10_000_000_u64),
                    revalidation_required_withdrawal: U256::ZERO,
                },
            )
            .unwrap();
        let cancelled = journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::CancelledStale {
                    master_return_transaction_id: 11,
                    revalidation_binance_balance: U256::from(6_000_000_u64),
                    revalidation_wallet_balance: U256::from(10_000_000_u64),
                    revalidation_required_withdrawal: U256::ZERO,
                },
            )
            .unwrap();
        assert!(cancelled.progress.terminal());
        assert!(journal.active_operation().unwrap().is_none());
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn only_approved_travel_rule_failure_can_reopen_into_standard_submission_intent() {
        let path = path("approved-standard-retry");
        let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
        let operation = journal
            .reserve(&request(Direction::BinanceToWallet, direct_arbitrum()))
            .unwrap();
        let operation_id = operation.intent.operation_id;
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::Failed {
                    reason: "approved deterministic Travel Rule rejection HTTP 400 code -4024"
                        .to_owned(),
                },
            )
            .unwrap();
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                    api_mode: "standard".to_owned(),
                    bridge_balance_before: U256::ZERO,
                    reconciliation_queries: 0,
                },
            )
            .unwrap();
        assert!(matches!(
            journal.operations()[&operation_id].progress,
            RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted { .. }
        ));
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn exact_local_entity_031031_failure_can_reopen_only_into_standard_submission() {
        let path = path("approved-031031-standard-retry");
        let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
        let operation = journal
            .reserve(&request(Direction::BinanceToWallet, direct_arbitrum()))
            .unwrap();
        let operation_id = operation.intent.operation_id;
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::Failed {
                    reason: "terminal Binance local-entity withdrawal rejection: Binance local-entity withdrawal submission failed with HTTP 400 Bad Request, code -4024: [031031] User does not own this currency.".to_owned(),
                },
            )
            .unwrap();
        assert!(
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                        api_mode: "local_entity".to_owned(),
                        bridge_balance_before: U256::ZERO,
                        reconciliation_queries: 0,
                    },
                )
                .is_err()
        );
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                    api_mode: "standard".to_owned(),
                    bridge_balance_before: U256::ZERO,
                    reconciliation_queries: 0,
                },
            )
            .unwrap();
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn exact_travel_rule_031031_failure_allows_three_total_proven_retries() {
        let path = path("travel-rule-031031-bounded-retry");
        let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
        let operation = journal
            .reserve(&request(Direction::BinanceToWallet, direct_arbitrum()))
            .unwrap();
        let operation_id = operation.intent.operation_id;
        let bridge_balance_before = operation.intent.wallet_balance_before;
        let failure = "terminal Binance Travel Rule withdrawal rejection: Binance Travel Rule withdrawal submission failed with HTTP 400 Bad Request, code -4024: [031031] User does not own this currency.";
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::Failed {
                    reason: failure.to_owned(),
                },
            )
            .unwrap();

        let reopened = journal
            .reopen_next_retryable_quarantine()
            .unwrap()
            .expect("the reviewed ownership rejection should reopen once");
        assert!(matches!(
            reopened.progress,
            RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                api_mode,
                bridge_balance_before: observed_bridge_balance,
                reconciliation_queries: 0,
            } if api_mode == TRAVEL_RULE_BINANCE_WITHDRAWAL_API_MODE
                && observed_bridge_balance == bridge_balance_before
        ));
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::Failed {
                    reason: failure.to_owned(),
                },
            )
            .unwrap();
        let reopened_again = journal
            .reopen_next_retryable_quarantine()
            .unwrap()
            .expect("the reviewed ownership rejection should reopen twice");
        assert!(matches!(
            reopened_again.progress,
            RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                api_mode,
                reconciliation_queries: 0,
                ..
            } if api_mode == TRAVEL_RULE_BINANCE_WITHDRAWAL_API_MODE
        ));
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::Failed {
                    reason: failure.to_owned(),
                },
            )
            .unwrap();
        drop(journal);

        let mut replayed = RebalanceExecutionJournal::open(&path).unwrap();
        assert!(
            replayed
                .reopen_next_retryable_quarantine()
                .unwrap()
                .is_none(),
            "the fourth retry must remain terminal across restart"
        );
        drop(replayed);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn exact_standard_4104_routes_durably_before_travel_rule_submission() {
        let path = path("standard-4104-travel-rule-routing");
        let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
        let operation = journal
            .reserve(&request(Direction::BinanceToWallet, direct_arbitrum()))
            .unwrap();
        let operation_id = operation.intent.operation_id;
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceTransferSubmitted {
                    transaction_id: 1,
                    bridge_balance_before: U256::ZERO,
                },
            )
            .unwrap();
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceTransferCompleted {
                    transaction_id: 1,
                    bridge_balance_before: U256::ZERO,
                },
            )
            .unwrap();
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                    api_mode: "standard".to_owned(),
                    bridge_balance_before: U256::ZERO,
                    reconciliation_queries: 0,
                },
            )
            .unwrap();
        assert!(
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                        api_mode: "travel_rule_ae_self_owned".to_owned(),
                        bridge_balance_before: U256::ZERO,
                        reconciliation_queries: 0,
                    },
                )
                .is_err()
        );
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                    api_mode: "travel_rule_required_after_standard_-4104".to_owned(),
                    bridge_balance_before: U256::ZERO,
                    reconciliation_queries: 0,
                },
            )
            .unwrap();
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                    api_mode: "travel_rule_ae_self_owned".to_owned(),
                    bridge_balance_before: U256::ZERO,
                    reconciliation_queries: 0,
                },
            )
            .unwrap();
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn exact_free_unlocked_master_balance_durably_authorizes_one_withdrawal_retry() {
        let path = path("balance-proven-withdrawal-retry");
        let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
        let operation = journal
            .reserve(&request(Direction::BinanceToWallet, direct_arbitrum()))
            .unwrap();
        let operation_id = operation.intent.operation_id;
        let amount = operation.intent.amount;
        let wallet_before = U256::from(7_000_000_u64);
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceTransferSubmitted {
                    transaction_id: 1,
                    bridge_balance_before: wallet_before,
                },
            )
            .unwrap();
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceTransferCompleted {
                    transaction_id: 1,
                    bridge_balance_before: wallet_before,
                },
            )
            .unwrap();
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                    api_mode: "travel_rule_ae_self_owned".to_owned(),
                    bridge_balance_before: wallet_before,
                    reconciliation_queries: 0,
                },
            )
            .unwrap();
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                    api_mode: "travel_rule_ae_self_owned".to_owned(),
                    bridge_balance_before: wallet_before,
                    reconciliation_queries: 1,
                },
            )
            .unwrap();
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceWithdrawalRetryAuthorized {
                    api_mode: "travel_rule_ae_self_owned".to_owned(),
                    bridge_balance_before: wallet_before,
                    master_free_base_units: amount,
                    master_locked_base_units: U256::ZERO,
                    wallet_balance_base_units: wallet_before,
                },
            )
            .unwrap();
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                    api_mode: "travel_rule_ae_self_owned".to_owned(),
                    bridge_balance_before: wallet_before,
                    reconciliation_queries: 0,
                },
            )
            .unwrap();
        drop(journal);

        let replayed = RebalanceExecutionJournal::open(&path).unwrap();
        assert!(matches!(
            &replayed.operations()[&operation_id].progress,
            RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                api_mode,
                reconciliation_queries: 0,
                ..
            } if api_mode == "travel_rule_ae_self_owned"
        ));
        drop(replayed);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn withdrawal_retry_authorization_rejects_locked_or_changed_master_inventory() {
        let path = path("invalid-balance-proven-withdrawal-retry");
        let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
        let operation = journal
            .reserve(&request(Direction::BinanceToWallet, direct_arbitrum()))
            .unwrap();
        let operation_id = operation.intent.operation_id;
        let amount = operation.intent.amount;
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceTransferSubmitted {
                    transaction_id: 1,
                    bridge_balance_before: U256::from(9),
                },
            )
            .unwrap();
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceTransferCompleted {
                    transaction_id: 1,
                    bridge_balance_before: U256::from(9),
                },
            )
            .unwrap();
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                    api_mode: "standard".to_owned(),
                    bridge_balance_before: U256::from(9),
                    reconciliation_queries: 0,
                },
            )
            .unwrap();
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                    api_mode: "standard".to_owned(),
                    bridge_balance_before: U256::from(9),
                    reconciliation_queries: 1,
                },
            )
            .unwrap();
        for invalid in [
            RebalanceExecutionProgress::BinanceWithdrawalRetryAuthorized {
                api_mode: "standard".to_owned(),
                bridge_balance_before: U256::from(9),
                master_free_base_units: amount,
                master_locked_base_units: U256::ONE,
                wallet_balance_base_units: U256::from(9),
            },
            RebalanceExecutionProgress::BinanceWithdrawalRetryAuthorized {
                api_mode: "standard".to_owned(),
                bridge_balance_before: U256::from(9),
                master_free_base_units: amount - U256::ONE,
                master_locked_base_units: U256::ZERO,
                wallet_balance_base_units: U256::from(9),
            },
        ] {
            assert!(journal.advance(&operation_id, invalid).is_err());
        }
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceWithdrawalRetryAuthorized {
                    api_mode: "standard".to_owned(),
                    bridge_balance_before: U256::from(9),
                    master_free_base_units: amount,
                    master_locked_base_units: U256::ZERO,
                    wallet_balance_base_units: U256::from(10),
                },
            )
            .expect("wallet movement is diagnostic when the exact master amount remains free");
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn mined_arbitrum_deposit_reconciles_exact_unknown_quarantine() {
        let path = path("arbitrum-deposit-unknown-reconciliation");
        let transaction_hash = B256::repeat_byte(0x42);
        let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
        let operation = journal
            .reserve(&request(Direction::WalletToBinance, direct_arbitrum()))
            .unwrap();
        let operation_id = operation.intent.operation_id;
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::Quarantined {
                    reason: "DEX outcome unknown: JSON-RPC error -32000: nonce too low".to_owned(),
                },
            )
            .unwrap();

        assert_eq!(
            journal
                .next_reconcilable_arbitrum_deposit_quarantine()
                .unwrap()
                .unwrap()
                .intent
                .operation_id,
            operation_id
        );
        assert!(
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::DepositTransferMined {
                        chain_id: 42_161,
                        transaction_hash,
                    },
                )
                .is_err(),
            "only the evidence-gated reconciliation method may bypass quarantine"
        );
        let reconciled = journal
            .record_reconciled_arbitrum_deposit(&operation_id, transaction_hash)
            .unwrap();
        assert_eq!(
            reconciled.progress,
            RebalanceExecutionProgress::DepositTransferMined {
                chain_id: 42_161,
                transaction_hash,
            }
        );
        drop(journal);

        let replayed = RebalanceExecutionJournal::open(&path).unwrap();
        assert_eq!(
            replayed.active_operation().unwrap().unwrap().progress,
            reconciled.progress
        );
        assert!(
            replayed
                .next_reconcilable_arbitrum_deposit_quarantine()
                .unwrap()
                .is_none()
        );
        drop(replayed);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn unrelated_quarantine_cannot_be_recorded_as_reconciled_deposit() {
        let path = path("arbitrum-deposit-unrelated-quarantine");
        let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
        let operation = journal
            .reserve(&request(Direction::WalletToBinance, direct_arbitrum()))
            .unwrap();
        journal
            .advance(
                &operation.intent.operation_id,
                RebalanceExecutionProgress::Quarantined {
                    reason: "operator review required".to_owned(),
                },
            )
            .unwrap();
        assert!(
            journal
                .next_reconcilable_arbitrum_deposit_quarantine()
                .unwrap()
                .is_none()
        );
        assert!(
            journal
                .record_reconciled_arbitrum_deposit(
                    &operation.intent.operation_id,
                    B256::repeat_byte(0x42),
                )
                .is_err()
        );
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn proven_across_fill_reconciles_exact_timeout_quarantine() {
        let path = path("across-fill-timeout-reconciliation");
        let origin_hash = B256::repeat_byte(0x41);
        let fill_hash = B256::repeat_byte(0x42);
        let minimum_output_amount = U256::from(1_990_000_u64);
        let received_base_units = U256::from(1_995_000_u64);
        let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
        let operation = journal
            .reserve(&request(Direction::WalletToBinance, across()))
            .unwrap();
        let operation_id = operation.intent.operation_id;
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BridgePrepared {
                    origin_chain_id: 480,
                    input_amount: U256::from(2_000_000_u64),
                    target: Address::repeat_byte(0x35),
                    calldata: vec![0x36],
                    calldata_hash: keccak256([0x36]),
                    minimum_output_amount,
                    destination_balance_before: U256::from(10_000_000_u64),
                },
            )
            .unwrap();
        let mined = RebalanceExecutionProgress::BridgeMined {
            origin_chain_id: 480,
            transaction_hash: origin_hash,
            minimum_output_amount,
            destination_balance_before: U256::from(10_000_000_u64),
        };
        journal.advance(&operation_id, mined.clone()).unwrap();
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::Quarantined {
                    reason: "timed out waiting for Across fill".to_owned(),
                },
            )
            .unwrap();

        assert_eq!(
            journal.progress_before_quarantine(&operation_id),
            Some(&mined)
        );
        assert_eq!(
            journal
                .next_reconcilable_across_fill_quarantine()
                .unwrap()
                .unwrap()
                .intent
                .operation_id,
            operation_id
        );
        assert!(
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::AcrossFilled {
                        fill_transaction_hash: fill_hash,
                        received_base_units,
                    },
                )
                .is_err(),
            "only the evidence-gated reconciliation method may bypass quarantine"
        );
        assert!(
            journal
                .record_reconciled_across_fill(
                    &operation_id,
                    fill_hash,
                    minimum_output_amount - U256::ONE,
                )
                .is_err()
        );
        let reconciled = journal
            .record_reconciled_across_fill(&operation_id, fill_hash, received_base_units)
            .unwrap();
        assert_eq!(
            reconciled.progress,
            RebalanceExecutionProgress::AcrossFilled {
                fill_transaction_hash: fill_hash,
                received_base_units,
            }
        );
        drop(journal);

        let replayed = RebalanceExecutionJournal::open(&path).unwrap();
        assert_eq!(
            replayed.active_operation().unwrap().unwrap().progress,
            reconciled.progress
        );
        assert!(
            replayed
                .next_reconcilable_across_fill_quarantine()
                .unwrap()
                .is_none()
        );
        drop(replayed);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn corrected_false_positive_quarantine_has_four_bounded_durable_reopens() {
        let path = path("bounded-quarantine-recovery");
        let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
        let operation = journal
            .reserve(&request(Direction::BinanceToWallet, direct_arbitrum()))
            .unwrap();
        let operation_id = operation.intent.operation_id;
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceTransferSubmitted {
                    transaction_id: 1,
                    bridge_balance_before: U256::from(9),
                },
            )
            .unwrap();
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceTransferCompleted {
                    transaction_id: 1,
                    bridge_balance_before: U256::from(9),
                },
            )
            .unwrap();
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                    api_mode: "travel_rule_ae_self_owned".to_owned(),
                    bridge_balance_before: U256::from(9),
                    reconciliation_queries: 1,
                },
            )
            .unwrap();
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::Quarantined {
                    reason: "unindexed Binance withdrawal retry found a destination-wallet balance change"
                        .to_owned(),
                },
            )
            .unwrap();
        drop(journal);

        let mut replayed = RebalanceExecutionJournal::open(&path).unwrap();
        let reopened = replayed
            .reopen_next_retryable_quarantine()
            .unwrap()
            .expect("the reviewed false positive should reopen");
        assert!(matches!(
            &reopened.progress,
            RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                api_mode,
                reconciliation_queries: 1,
                ..
            } if api_mode == "travel_rule_ae_self_owned"
        ));
        replayed
            .advance(
                &operation_id,
                RebalanceExecutionProgress::Quarantined {
                    reason: "unindexed Binance withdrawal retry found a destination-wallet balance change"
                        .to_owned(),
                },
            )
            .unwrap();
        let reopened_again = replayed
            .reopen_next_retryable_quarantine()
            .unwrap()
            .expect("the second reviewed guard correction should reopen once more");
        assert_eq!(reopened_again.progress, reopened.progress);
        replayed
            .advance(
                &operation_id,
                RebalanceExecutionProgress::Quarantined {
                    reason: "unindexed Binance withdrawal retry found a destination-wallet balance change"
                        .to_owned(),
                },
            )
            .unwrap();
        let reopened_third = replayed
            .reopen_next_retryable_quarantine()
            .unwrap()
            .expect("the third reviewed guard correction should reopen once more");
        assert_eq!(reopened_third.progress, reopened.progress);
        replayed
            .advance(
                &operation_id,
                RebalanceExecutionProgress::Quarantined {
                    reason: "unindexed Binance withdrawal retry found a destination-wallet balance change"
                        .to_owned(),
                },
            )
            .unwrap();
        let reopened_fourth = replayed
            .reopen_next_retryable_quarantine()
            .unwrap()
            .expect("the fourth reviewed guard correction should reopen once more");
        assert_eq!(reopened_fourth.progress, reopened.progress);
        replayed
            .advance(
                &operation_id,
                RebalanceExecutionProgress::Quarantined {
                    reason: "unindexed Binance withdrawal retry found a destination-wallet balance change"
                        .to_owned(),
                },
            )
            .unwrap();
        assert!(
            replayed
                .reopen_next_retryable_quarantine()
                .unwrap()
                .is_none()
        );
        drop(replayed);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn legacy_across_questionnaire_chain_quarantine_reopens_exact_mined_deposit() {
        let path = path("across-questionnaire-chain-reopen");
        let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
        let operation = journal
            .reserve(&request(Direction::WalletToBinance, across()))
            .unwrap();
        let operation_id = operation.intent.operation_id;
        advance_to_across_deposit(&mut journal, &operation_id, 10);
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::Quarantined {
                    reason: "illegal rebalance executor state transition".to_owned(),
                },
            )
            .unwrap();
        drop(journal);

        let mut replayed = RebalanceExecutionJournal::open(&path).unwrap();
        let reopened = replayed
            .reopen_next_retryable_quarantine()
            .unwrap()
            .expect("the historical destination-chain mismatch should reopen");
        assert_eq!(
            reopened.progress,
            RebalanceExecutionProgress::DepositTransferMined {
                chain_id: 10,
                transaction_hash: B256::repeat_byte(0x34),
            }
        );
        drop(replayed);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn unrelated_illegal_transition_quarantine_stays_closed() {
        let path = path("unrelated-illegal-transition-stays-closed");
        let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
        let operation = journal
            .reserve(&request(Direction::WalletToBinance, across()))
            .unwrap();
        let operation_id = operation.intent.operation_id;
        advance_to_across_deposit(&mut journal, &operation_id, 480);
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::Quarantined {
                    reason: "illegal rebalance executor state transition".to_owned(),
                },
            )
            .unwrap();
        drop(journal);

        let mut replayed = RebalanceExecutionJournal::open(&path).unwrap();
        assert!(
            replayed
                .reopen_next_retryable_quarantine()
                .unwrap()
                .is_none()
        );
        drop(replayed);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn unindexed_master_transfer_quarantine_reopens_the_exact_recorded_intent() {
        let path = path("unindexed-master-transfer-reopen");
        let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
        let operation = journal
            .reserve(&request(Direction::BinanceToWallet, direct_arbitrum()))
            .unwrap();
        let operation_id = operation.intent.operation_id;
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::Quarantined {
                    reason: "rebalance intent has no indexed Binance master transfer; operator review required"
                        .to_owned(),
                },
            )
            .unwrap();
        drop(journal);

        let mut replayed = RebalanceExecutionJournal::open(&path).unwrap();
        let reopened = replayed
            .reopen_next_retryable_quarantine()
            .unwrap()
            .expect("an unindexed deterministic master transfer should reopen");
        assert_eq!(
            reopened.progress,
            RebalanceExecutionProgress::IntentRecorded
        );
        drop(replayed);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn ownership_guard_reopens_exact_travel_rule_submission_progress() {
        let path = path("ownership-quarantine-travel-rule-progress");
        let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
        let operation = journal
            .reserve(&request(Direction::BinanceToWallet, direct_arbitrum()))
            .unwrap();
        let operation_id = operation.intent.operation_id;
        for progress in [
            RebalanceExecutionProgress::BinanceTransferSubmitted {
                transaction_id: 1,
                bridge_balance_before: U256::from(9),
            },
            RebalanceExecutionProgress::BinanceTransferCompleted {
                transaction_id: 1,
                bridge_balance_before: U256::from(9),
            },
            RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                api_mode: "standard".to_owned(),
                bridge_balance_before: U256::from(9),
                reconciliation_queries: 0,
            },
            RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                api_mode: "travel_rule_required_after_standard_-4104".to_owned(),
                bridge_balance_before: U256::from(9),
                reconciliation_queries: 0,
            },
            RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                api_mode: "travel_rule_ae_self_owned".to_owned(),
                bridge_balance_before: U256::from(9),
                reconciliation_queries: 0,
            },
            RebalanceExecutionProgress::Quarantined {
                reason: "Binance Travel Rule ownership verification is not unique for the exact wallet and network"
                    .to_owned(),
            },
        ] {
            journal.advance(&operation_id, progress).unwrap();
        }
        drop(journal);

        let mut replayed = RebalanceExecutionJournal::open(&path).unwrap();
        let reopened = replayed
            .reopen_next_retryable_quarantine()
            .unwrap()
            .expect("the exact Travel Rule submission progress should reopen");
        let expected_progress = reopened.progress.clone();
        assert!(matches!(
            reopened.progress,
            RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                ref api_mode,
                reconciliation_queries: 0,
                ..
            } if api_mode == "travel_rule_ae_self_owned"
        ));
        for (reason, expected_reopen) in [
            (
                "Binance Travel Rule ownership verification is not unique for the exact wallet and network",
                true,
            ),
            (
                "Binance Travel Rule ownership verification is absent for the exact wallet, network, and token",
                true,
            ),
            (
                "Binance Travel Rule ownership verification is absent for the exact wallet and network",
                true,
            ),
            (
                "Binance Travel Rule ownership verification is absent for the exact wallet and network",
                false,
            ),
        ] {
            replayed
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::Quarantined {
                        reason: reason.to_owned(),
                    },
                )
                .unwrap();
            let next = replayed.reopen_next_retryable_quarantine().unwrap();
            assert_eq!(next.is_some(), expected_reopen);
            if let Some(next) = next {
                assert_eq!(next.progress, expected_progress);
            }
        }
        drop(replayed);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn ownership_guard_reopens_exact_authorized_retry_after_prior_recovery() {
        let path = path("ownership-quarantine-authorized-retry");
        let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
        let operation = journal
            .reserve(&request(Direction::BinanceToWallet, direct_arbitrum()))
            .unwrap();
        let retry_amount = operation.intent.amount;
        let operation_id = operation.intent.operation_id;
        for progress in [
            RebalanceExecutionProgress::BinanceTransferSubmitted {
                transaction_id: 1,
                bridge_balance_before: U256::from(9),
            },
            RebalanceExecutionProgress::BinanceTransferCompleted {
                transaction_id: 1,
                bridge_balance_before: U256::from(9),
            },
            RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                api_mode: "travel_rule_ae_self_owned".to_owned(),
                bridge_balance_before: U256::from(9),
                reconciliation_queries: 1,
            },
            RebalanceExecutionProgress::Quarantined {
                reason:
                    "unindexed Binance withdrawal retry found a destination-wallet balance change"
                        .to_owned(),
            },
        ] {
            journal.advance(&operation_id, progress).unwrap();
        }
        let reopened_submission = journal
            .reopen_next_retryable_quarantine()
            .unwrap()
            .expect("the first reviewed guard should reopen its exact submission state");
        assert!(matches!(
            reopened_submission.progress,
            RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                ref api_mode,
                reconciliation_queries: 1,
                ..
            } if api_mode == "travel_rule_ae_self_owned"
        ));
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceWithdrawalRetryAuthorized {
                    api_mode: "travel_rule_ae_self_owned".to_owned(),
                    bridge_balance_before: U256::from(9),
                    master_free_base_units: retry_amount,
                    master_locked_base_units: U256::ZERO,
                    wallet_balance_base_units: U256::from(11),
                },
            )
            .unwrap();
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::Quarantined {
                    reason: "Binance Travel Rule ownership verification is not unique for the exact wallet and network"
                        .to_owned(),
                },
            )
            .unwrap();
        drop(journal);

        let mut replayed = RebalanceExecutionJournal::open(&path).unwrap();
        let reopened_retry = replayed
            .reopen_next_retryable_quarantine()
            .unwrap()
            .expect("the second guard correction should restore its exact retry authority");
        let expected_retry_progress = reopened_retry.progress.clone();
        assert!(matches!(
            reopened_retry.progress,
            RebalanceExecutionProgress::BinanceWithdrawalRetryAuthorized {
                ref api_mode,
                master_free_base_units,
                master_locked_base_units,
                wallet_balance_base_units,
                ..
            } if api_mode == "travel_rule_ae_self_owned"
                && master_free_base_units == retry_amount
                && master_locked_base_units.is_zero()
                && wallet_balance_base_units == U256::from(11)
        ));
        replayed
            .advance(
                &operation_id,
                RebalanceExecutionProgress::Quarantined {
                    reason: "Binance Travel Rule ownership verification is absent for the exact wallet, network, and token"
                        .to_owned(),
                },
            )
            .unwrap();
        let reopened_after_token_scope_correction = replayed
            .reopen_next_retryable_quarantine()
            .unwrap()
            .expect("the exact retry authority should survive the token-scope correction");
        assert_eq!(
            reopened_after_token_scope_correction.progress,
            expected_retry_progress
        );
        replayed
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                    api_mode: "travel_rule_ae_self_owned".to_owned(),
                    bridge_balance_before: U256::from(9),
                    reconciliation_queries: 0,
                },
            )
            .unwrap();
        replayed
            .advance(
                &operation_id,
                RebalanceExecutionProgress::Quarantined {
                    reason: "Binance Travel Rule withdrawal submission failed with HTTP 400 Bad Request, code -1022: Signature for this request is not valid."
                        .to_owned(),
                },
            )
            .unwrap();
        drop(replayed);

        let mut replayed = RebalanceExecutionJournal::open(&path).unwrap();
        let reopened_after_signature_correction = replayed
            .reopen_next_retryable_quarantine()
            .unwrap()
            .expect("the exact submission should survive the signature-encoding correction");
        assert!(matches!(
            reopened_after_signature_correction.progress,
            RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                ref api_mode,
                reconciliation_queries: 0,
                ..
            } if api_mode == "travel_rule_ae_self_owned"
        ));
        replayed
            .advance(
                &operation_id,
                RebalanceExecutionProgress::Quarantined {
                    reason: "Binance Travel Rule withdrawal submission failed with HTTP 400 Bad Request, code -1022: Signature for this request is not valid."
                        .to_owned(),
                },
            )
            .unwrap();
        assert!(
            replayed
                .reopen_next_retryable_quarantine()
                .unwrap()
                .is_none()
        );
        drop(replayed);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn unresolved_operation_quarantines_only_its_token_and_releases_active_lane() {
        let path = path("asset-scoped-quarantine");
        let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
        let operation = journal
            .reserve(&request(Direction::BinanceToWallet, direct_arbitrum()))
            .unwrap();
        let operation_id = operation.intent.operation_id;
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceTransferSubmitted {
                    transaction_id: 1,
                    bridge_balance_before: U256::from(9),
                },
            )
            .unwrap();
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::Quarantined {
                    reason: "asset-scoped unresolved outcome".to_owned(),
                },
            )
            .unwrap();

        assert!(journal.active_operation().unwrap().is_none());
        let quarantined = journal.quarantined_operations().collect::<Vec<_>>();
        assert_eq!(quarantined.len(), 1);
        assert_eq!(quarantined[0].intent.token_symbol, "USDC");
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn active_recovery_defers_retryable_quarantine_until_the_lane_is_terminal() {
        let path = path("active-recovery-defers-quarantine");
        let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
        let quarantined = journal
            .reserve(&request(Direction::BinanceToWallet, direct_arbitrum()))
            .unwrap();
        for progress in [
            RebalanceExecutionProgress::BinanceTransferSubmitted {
                transaction_id: 1,
                bridge_balance_before: U256::from(9),
            },
            RebalanceExecutionProgress::BinanceTransferCompleted {
                transaction_id: 1,
                bridge_balance_before: U256::from(9),
            },
            RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                api_mode: "travel_rule_ae_self_owned".to_owned(),
                bridge_balance_before: U256::from(9),
                reconciliation_queries: 1,
            },
        ] {
            journal
                .advance(&quarantined.intent.operation_id, progress)
                .unwrap();
        }
        journal
            .advance(
                &quarantined.intent.operation_id,
                RebalanceExecutionProgress::Quarantined {
                    reason: "unindexed Binance withdrawal retry found a destination-wallet balance change"
                        .to_owned(),
                },
            )
            .unwrap();

        let active = journal
            .reserve(&request(Direction::BinanceToWallet, direct_arbitrum()))
            .unwrap();
        assert_eq!(
            journal
                .active_operation()
                .unwrap()
                .map(|operation| { operation.intent.operation_id.as_str() }),
            Some(active.intent.operation_id.as_str())
        );
        assert!(
            journal
                .reopen_next_retryable_quarantine()
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            journal
                .operations()
                .get(&quarantined.intent.operation_id)
                .unwrap()
                .progress,
            RebalanceExecutionProgress::Quarantined { .. }
        ));

        journal
            .advance(
                &active.intent.operation_id,
                RebalanceExecutionProgress::Failed {
                    reason: "active recovery completed terminally for test".to_owned(),
                },
            )
            .unwrap();
        let reopened = journal
            .reopen_next_retryable_quarantine()
            .unwrap()
            .expect("retryable quarantine should reopen after the active lane is terminal");
        assert_eq!(
            reopened.intent.operation_id,
            quarantined.intent.operation_id
        );

        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn terminal_standard_failure_can_close_only_with_proven_receipt_completion() {
        let path = path("manual_recovery-manual-receipt-completion");
        let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
        let operation = journal
            .reserve(&request(Direction::BinanceToWallet, direct_arbitrum()))
            .unwrap();
        let operation_id = operation.intent.operation_id;
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::Failed {
                    reason: "terminal Binance standard withdrawal rejection after approved local-entity endpoint correction: Binance standard withdrawal submission failed with HTTP 400 Bad Request, code -4104: Please note that withdrawals are not permitted due to travel rule restrictions. To facilitate the withdrawal process, please refer to Travel Rule documentation.".to_owned(),
                },
            )
            .unwrap();
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::Completed {
                    binance_balance_after: U256::ZERO,
                    wallet_balance_after: U256::from(1),
                },
            )
            .unwrap();
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn legacy_approved_terminal_retry_modes_are_replay_only() {
        for legacy_api_mode in ["local_entity", "travel_rule"] {
            let path = path(&format!("legacy-approved-retry-{legacy_api_mode}"));
            let legacy_operation;
            {
                let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
                let operation = journal
                    .reserve(&request(Direction::BinanceToWallet, direct_arbitrum()))
                    .unwrap();
                let operation_id = operation.intent.operation_id;
                journal
                    .advance(
                        &operation_id,
                        RebalanceExecutionProgress::Failed {
                            reason:
                                "approved deterministic Travel Rule rejection HTTP 400 code -4024"
                                    .to_owned(),
                        },
                    )
                    .unwrap();
                let legacy_progress =
                    RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                        api_mode: legacy_api_mode.to_owned(),
                        bridge_balance_before: U256::ZERO,
                        reconciliation_queries: 0,
                    };
                assert!(
                    journal
                        .advance(&operation_id, legacy_progress.clone())
                        .is_err(),
                    "live append unexpectedly accepted legacy mode {legacy_api_mode}"
                );
                legacy_operation = super::RebalanceExecutionOperation {
                    intent: journal.operations()[&operation_id].intent.clone(),
                    progress: legacy_progress,
                };
            }

            let record = WireRecord::new(WirePayload {
                version: super::VERSION,
                sequence: 2,
                recorded_at_unix_ms: 3,
                operation: legacy_operation,
            })
            .unwrap();
            let mut encoded = serde_json::to_vec(&record).unwrap();
            encoded.push(b'\n');
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&encoded).unwrap();
            file.sync_all().unwrap();
            drop(file);

            let journal = RebalanceExecutionJournal::open(&path).unwrap();
            assert!(matches!(
                &journal.operations().values().next().unwrap().progress,
                RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                    api_mode,
                    reconciliation_queries: 0,
                    ..
                } if api_mode == legacy_api_mode
            ));
            drop(journal);
            fs::remove_file(path).unwrap();
        }
    }

    #[test]
    fn production_derived_journal_suffix_replays_before_deploy() {
        let path = path("production-derived-rebalance-replay");
        let wallet: Address = "0x90D990C81320221D2882De32beeA78923c1e77A3"
            .parse()
            .unwrap();
        let esp_intent = super::RebalanceExecutionIntent {
            scope: Some(super::RebalanceJournalScope {
                schema_version: 2,
                account_id: "binance:trading-subaccount".to_owned(),
                network_id: "chain:42161".to_owned(),
                strategy_id: "rebalance-arbitrum-usdc-esp".to_owned(),
            }),
            operation_id: "rebalance-268-15f59bc55dcaed54".to_owned(),
            fingerprint: "15f59bc55dcaed549afe3267c3988827e838a58bf900ec6b522333f8b07e3e8f"
                .to_owned(),
            withdraw_order_id: "rb15f59bc55dcaed549afe3267c39888".to_owned(),
            token_symbol: "ESP".to_owned(),
            token_decimals: 18,
            token_contract: "0x3b8db18e69d6686ad9371a423afe3dd1065c94f1"
                .parse()
                .unwrap(),
            wallet_owner: wallet,
            direction: Direction::BinanceToWallet,
            route: direct_arbitrum(),
            amount: U256::from(401_200_u64) * U256::from(10_u64).pow(U256::from(15_u64)),
            binance_balance_before: U256::from(10_000_u64)
                * U256::from(10_u64).pow(U256::from(18_u64)),
            wallet_balance_before: U256::ZERO,
            revalidation_start_balance: U256::ZERO,
            maximum_fee_base_units: None,
            approval_session_id: None,
        };
        let usdc_intent = super::RebalanceExecutionIntent {
            scope: Some(super::RebalanceJournalScope {
                schema_version: 2,
                account_id: "binance:trading-subaccount".to_owned(),
                network_id: "chain:10".to_owned(),
                strategy_id: "rebalance-world-chain-v12".to_owned(),
            }),
            operation_id: "rebalance-296-96fd53e70c1ab390".to_owned(),
            fingerprint: "96fd53e70c1ab390ae3e62eb434cd19f5c5e9e1434754bbbddc34d932f0efb50"
                .to_owned(),
            withdraw_order_id: "rb96fd53e70c1ab390ae3e62eb434cd1".to_owned(),
            token_symbol: "USDC".to_owned(),
            token_decimals: 6,
            token_contract: "0x79a02482a880bce3f13e09da970dc34db4cd24d1"
                .parse()
                .unwrap(),
            wallet_owner: wallet,
            direction: Direction::BinanceToWallet,
            route: across(),
            amount: U256::from(1_197_503_244_u64),
            binance_balance_before: U256::from(3_075_000_679_u64),
            wallet_balance_before: U256::from(679_994_191_u64),
            revalidation_start_balance: U256::ZERO,
            maximum_fee_base_units: None,
            approval_session_id: None,
        };
        let operation = |intent: &super::RebalanceExecutionIntent, progress| {
            super::RebalanceExecutionOperation {
                intent: intent.clone(),
                progress,
            }
        };
        write_replay_fixture(
            &path,
            vec![
                operation(&esp_intent, RebalanceExecutionProgress::IntentRecorded),
                operation(
                    &esp_intent,
                    RebalanceExecutionProgress::BinanceTransferSubmitted {
                        transaction_id: 395_702_159_719,
                        bridge_balance_before: U256::ZERO,
                    },
                ),
                operation(
                    &esp_intent,
                    RebalanceExecutionProgress::BinanceTransferCompleted {
                        transaction_id: 395_702_159_719,
                        bridge_balance_before: U256::ZERO,
                    },
                ),
                operation(
                    &esp_intent,
                    RebalanceExecutionProgress::Failed {
                        reason: "approved deterministic Travel Rule rejection HTTP 400 code -4024: [031031] User does not own this currency.".to_owned(),
                    },
                ),
                operation(
                    &esp_intent,
                    RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                        api_mode: "standard".to_owned(),
                        bridge_balance_before: U256::ZERO,
                        reconciliation_queries: 0,
                    },
                ),
                operation(
                    &esp_intent,
                    RebalanceExecutionProgress::Completed {
                        binance_balance_after: U256::from(9_598_800_u64)
                            * U256::from(10_u64).pow(U256::from(15_u64)),
                        wallet_balance_after: U256::from(400_u64)
                            * U256::from(10_u64).pow(U256::from(18_u64)),
                    },
                ),
                operation(&usdc_intent, RebalanceExecutionProgress::IntentRecorded),
                operation(
                    &usdc_intent,
                    RebalanceExecutionProgress::BinanceTransferSubmitted {
                        transaction_id: 395_924_104_268,
                        bridge_balance_before: U256::from(508_u64),
                    },
                ),
                operation(
                    &usdc_intent,
                    RebalanceExecutionProgress::BinanceTransferCompleted {
                        transaction_id: 395_924_104_268,
                        bridge_balance_before: U256::from(508_u64),
                    },
                ),
                operation(
                    &usdc_intent,
                    RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                        api_mode: "standard".to_owned(),
                        bridge_balance_before: U256::from(508_u64),
                        reconciliation_queries: 0,
                    },
                ),
                operation(
                    &usdc_intent,
                    RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                        api_mode: "standard".to_owned(),
                        bridge_balance_before: U256::from(508_u64),
                        reconciliation_queries: 1,
                    },
                ),
            ],
        );

        let journal = RebalanceExecutionJournal::open(&path).unwrap();
        assert!(matches!(
            journal.operations()[&esp_intent.operation_id].progress,
            RebalanceExecutionProgress::Completed { .. }
        ));
        let active = journal.active_operation().unwrap().unwrap();
        assert_eq!(active.intent, usdc_intent);
        assert!(matches!(
            &active.progress,
            RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                api_mode,
                reconciliation_queries: 1,
                bridge_balance_before,
            } if api_mode == "standard" && *bridge_balance_before == U256::from(508_u64)
        ));
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn legacy_direct_withdrawal_without_submission_intent_remains_replayable() {
        let path = path("legacy-direct-withdrawal");
        let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
        let operation = journal
            .reserve(&request(Direction::BinanceToWallet, direct_arbitrum()))
            .unwrap();
        let operation_id = operation.intent.operation_id;
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceTransferSubmitted {
                    transaction_id: 17,
                    bridge_balance_before: U256::from(8_000_000_u64),
                },
            )
            .unwrap();
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceTransferCompleted {
                    transaction_id: 17,
                    bridge_balance_before: U256::from(8_000_000_u64),
                },
            )
            .unwrap();
        journal
            .advance(
                &operation_id,
                RebalanceExecutionProgress::BinanceWithdrawalSubmitted {
                    submission_reference: "legacy-withdrawal".to_owned(),
                    bridge_balance_before: U256::from(8_000_000_u64),
                },
            )
            .unwrap();
        drop(journal);

        let replayed = RebalanceExecutionJournal::open(&path).unwrap();
        assert!(matches!(
            replayed.operations()[&operation_id].progress,
            RebalanceExecutionProgress::BinanceWithdrawalSubmitted { .. }
        ));
        drop(replayed);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn legacy_defaulted_progress_field_validates_the_stored_payload_bytes() {
        let path = path("legacy-defaulted-progress-checksum");
        let operation_id;
        {
            let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
            let operation = journal
                .reserve(&request(Direction::BinanceToWallet, direct_arbitrum()))
                .unwrap();
            operation_id = operation.intent.operation_id;
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::BinanceTransferSubmitted {
                        transaction_id: 17,
                        bridge_balance_before: U256::ZERO,
                    },
                )
                .unwrap();
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::BinanceTransferCompleted {
                        transaction_id: 17,
                        bridge_balance_before: U256::ZERO,
                    },
                )
                .unwrap();
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                        api_mode: "standard".to_owned(),
                        bridge_balance_before: U256::ZERO,
                        reconciliation_queries: 0,
                    },
                )
                .unwrap();
        }

        let mut lines = fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let mut legacy_record: serde_json::Value =
            serde_json::from_str(lines.last().unwrap()).unwrap();
        legacy_record["payload"]["operation"]["progress"]
            .as_object_mut()
            .unwrap()
            .remove("reconciliation_queries");
        let legacy_payload = serde_json::to_string(&legacy_record["payload"]).unwrap();
        legacy_record["checksum_sha256"] =
            serde_json::Value::String(super::checksum_bytes(legacy_payload.as_bytes()));
        lines.pop();
        lines.push(serde_json::to_string(&legacy_record).unwrap());
        fs::write(&path, format!("{}\n", lines.join("\n"))).unwrap();

        let replayed = RebalanceExecutionJournal::open(&path).unwrap();
        assert!(matches!(
            replayed.operations()[&operation_id].progress,
            RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                reconciliation_queries: 0,
                ..
            }
        ));
        drop(replayed);

        let contents = fs::read_to_string(&path).unwrap();
        fs::write(&path, contents.replace("\"standard\"", "\"travel_rule\"")).unwrap();
        assert!(RebalanceExecutionJournal::open(&path).is_err());
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn legacy_approval_recovers_with_an_unset_input_amount() {
        let progress: RebalanceExecutionProgress = serde_json::from_value(serde_json::json!({
            "state": "approval_mined",
            "chain_id": 10,
            "transaction_hash": format!("{:#x}", B256::repeat_byte(0x31)),
        }))
        .unwrap();
        assert!(matches!(
            &progress,
            RebalanceExecutionProgress::ApprovalMined {
                input_amount,
                ..
            } if input_amount.is_zero()
        ));
        let serialized = serde_json::to_value(progress).unwrap();
        assert_eq!(serialized.get("input_amount"), None);
    }

    #[test]
    fn persists_and_recovers_full_wallet_to_binance_lifecycle() {
        let path = path("lifecycle");
        let operation_id;
        {
            let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
            let operation = journal
                .reserve(&request(Direction::WalletToBinance, across()))
                .unwrap();
            operation_id = operation.intent.operation_id.clone();
            assert_eq!(
                operation.intent.pending_transfer().amount,
                U256::from(2_000_000_u64)
            );
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::ApprovalMined {
                        chain_id: 480,
                        transaction_hash: B256::repeat_byte(0x31),
                        input_amount: U256::from(2_000_000_u64),
                    },
                )
                .unwrap();
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::BridgePrepared {
                        origin_chain_id: 480,
                        input_amount: U256::from(2_000_000_u64),
                        target: Address::repeat_byte(0x35),
                        calldata: vec![0x36],
                        calldata_hash: keccak256([0x36]),
                        minimum_output_amount: U256::from(1_990_000_u64),
                        destination_balance_before: U256::from(10_000_000_u64),
                    },
                )
                .unwrap();
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::BridgeMined {
                        origin_chain_id: 480,
                        transaction_hash: B256::repeat_byte(0x32),
                        minimum_output_amount: U256::from(1_990_000_u64),
                        destination_balance_before: U256::from(10_000_000_u64),
                    },
                )
                .unwrap();
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::AcrossFilled {
                        fill_transaction_hash: B256::repeat_byte(0x33),
                        received_base_units: U256::from(1_995_000_u64),
                    },
                )
                .unwrap();
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::DepositTransferMined {
                        chain_id: 10,
                        transaction_hash: B256::repeat_byte(0x34),
                    },
                )
                .unwrap();
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::DepositQuestionnaireSubmissionStarted {
                        chain_id: 10,
                        transaction_hash: B256::repeat_byte(0x34),
                        deposit_id: "deposit-1".to_owned(),
                    },
                )
                .unwrap();
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::BinanceCredited {
                        deposit_id: "deposit-1".to_owned(),
                        credited_base_units: U256::from(1_995_000_u64),
                    },
                )
                .unwrap();
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::Completed {
                        binance_balance_after: U256::from(9_995_000_u64),
                        wallet_balance_after: U256::from(6_000_000_u64),
                    },
                )
                .unwrap();
        }
        let journal = RebalanceExecutionJournal::open(&path).unwrap();
        assert!(journal.active_operation().unwrap().is_none());
        assert!(matches!(
            journal.operations()[&operation_id].progress,
            RebalanceExecutionProgress::Completed { .. }
        ));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn recovers_exact_prepared_across_call_after_restart() {
        let path = path("prepared-bridge");
        let calldata = vec![0xad, 0x54, 0x25, 0xc6, 0x01, 0x02];
        let operation_id;
        {
            let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
            let operation = journal
                .reserve(&request(Direction::WalletToBinance, across()))
                .unwrap();
            operation_id = operation.intent.operation_id.clone();
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::BridgePrepared {
                        origin_chain_id: 480,
                        input_amount: U256::from(2_000_000_u64),
                        target: Address::repeat_byte(0x35),
                        calldata_hash: keccak256(&calldata),
                        calldata: calldata.clone(),
                        minimum_output_amount: U256::from(1_990_000_u64),
                        destination_balance_before: U256::from(10_000_000_u64),
                    },
                )
                .unwrap();
        }

        let journal = RebalanceExecutionJournal::open(&path).unwrap();
        let active = journal.active_operation().unwrap().unwrap();
        assert_eq!(active.intent.operation_id, operation_id);
        assert!(matches!(
            &active.progress,
            RebalanceExecutionProgress::BridgePrepared {
                calldata: recovered,
                calldata_hash,
                ..
            } if recovered == &calldata && *calldata_hash == keccak256(&calldata)
        ));
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn allows_replacing_a_prepared_across_quote_before_broadcast() {
        let path = path("replace-prepared-bridge");
        let operation_id;
        {
            let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
            let operation = journal
                .reserve(&request(Direction::WalletToBinance, across()))
                .unwrap();
            operation_id = operation.intent.operation_id.clone();
            let first_calldata = vec![0xad, 0x54, 0x25, 0xc6, 0x01];
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::BridgePrepared {
                        origin_chain_id: 10,
                        input_amount: U256::from(2_000_000_u64),
                        target: Address::repeat_byte(0x35),
                        calldata_hash: keccak256(&first_calldata),
                        calldata: first_calldata,
                        minimum_output_amount: U256::from(1_990_000_u64),
                        destination_balance_before: U256::from(10_000_000_u64),
                    },
                )
                .unwrap();
            let replacement_calldata = vec![0x8e, 0x02, 0x50, 0xee, 0x02];
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::BridgePrepared {
                        origin_chain_id: 10,
                        input_amount: U256::from(2_000_000_u64),
                        target: Address::repeat_byte(0x36),
                        calldata_hash: keccak256(&replacement_calldata),
                        calldata: replacement_calldata.clone(),
                        minimum_output_amount: U256::from(1_985_000_u64),
                        destination_balance_before: U256::from(10_000_500_u64),
                    },
                )
                .unwrap();
            assert!(matches!(
                &journal.operations()[&operation_id].progress,
                RebalanceExecutionProgress::BridgePrepared {
                    target,
                    calldata,
                    minimum_output_amount,
                    ..
                } if *target == Address::repeat_byte(0x36)
                    && calldata == &replacement_calldata
                    && *minimum_output_amount == U256::from(1_985_000_u64)
            ));
        }
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn persists_master_transfer_before_binance_withdrawal() {
        let path = path("master-transfer");
        let operation_id;
        {
            let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
            let operation = journal
                .reserve(&request(Direction::BinanceToWallet, across()))
                .unwrap();
            operation_id = operation.intent.operation_id.clone();
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::BinanceTransferSubmitted {
                        transaction_id: 42,
                        bridge_balance_before: U256::from(8_000_000_u64),
                    },
                )
                .unwrap();
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::BinanceTransferCompleted {
                        transaction_id: 42,
                        bridge_balance_before: U256::from(8_000_000_u64),
                    },
                )
                .unwrap();
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                        api_mode: "local_entity".to_owned(),
                        bridge_balance_before: U256::from(8_000_000_u64),
                        reconciliation_queries: 0,
                    },
                )
                .unwrap();
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::BinanceWithdrawalSubmitted {
                        submission_reference: "withdrawal-1".to_owned(),
                        bridge_balance_before: U256::from(8_000_000_u64),
                    },
                )
                .unwrap();
        }

        let journal = RebalanceExecutionJournal::open(&path).unwrap();
        assert!(matches!(
            journal.operations()[&operation_id].progress,
            RebalanceExecutionProgress::BinanceWithdrawalSubmitted { .. }
        ));
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn restart_preserves_unknown_local_entity_withdrawal_without_resubmission_authority() {
        let path = path("local-entity-withdrawal-unknown");
        let operation_id;
        {
            let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
            let operation = journal
                .reserve(&request(Direction::BinanceToWallet, direct_arbitrum()))
                .unwrap();
            operation_id = operation.intent.operation_id.clone();
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::BinanceTransferSubmitted {
                        transaction_id: 42,
                        bridge_balance_before: U256::ZERO,
                    },
                )
                .unwrap();
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::BinanceTransferCompleted {
                        transaction_id: 42,
                        bridge_balance_before: U256::ZERO,
                    },
                )
                .unwrap();
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                        api_mode: "local_entity".to_owned(),
                        bridge_balance_before: U256::ZERO,
                        reconciliation_queries: 0,
                    },
                )
                .unwrap();
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                        api_mode: "local_entity".to_owned(),
                        bridge_balance_before: U256::ZERO,
                        reconciliation_queries: 1,
                    },
                )
                .unwrap();
            assert!(
                journal
                    .advance(
                        &operation_id,
                        RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                            api_mode: "local_entity".to_owned(),
                            bridge_balance_before: U256::ZERO,
                            reconciliation_queries: 2,
                        },
                    )
                    .is_err()
            );
        }

        let journal = RebalanceExecutionJournal::open(&path).unwrap();
        let active = journal.active_operation().unwrap().unwrap();
        assert_eq!(active.intent.operation_id, operation_id);
        assert!(matches!(
            active.progress,
            RebalanceExecutionProgress::BinanceWithdrawalSubmissionStarted {
                ref api_mode,
                reconciliation_queries: 1,
                ..
            } if api_mode == "local_entity"
        ));
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn cumulative_risk_is_derived_from_the_durable_saga_after_restart() {
        let path = path("rebalance-risk");
        let operation_id;
        {
            let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
            let mut request = request(Direction::BinanceToWallet, direct_arbitrum());
            request.authority = RebalanceExecutionAuthority::ArbitrumFullLive;
            request.action.amount = U256::from(900_000_u64);
            request.binance_balance_before = U256::from(1_000_000_u64);
            request.maximum_fee = Some(U256::from(100_000_u64));
            request.approval_session_id = Some("esp-usdc-arbitrum-rebalance-test-r2".to_owned());
            operation_id = journal.reserve(&request).unwrap().intent.operation_id;
            let risk = journal
                .rebalance_risk("esp-usdc-arbitrum-rebalance-test-r2")
                .unwrap();
            assert_eq!(risk.transfer_count, 1);
            assert_eq!(risk.active_transfer_count, 1);
            assert_eq!(risk.token_a_debit, U256::from(900_000_u64));
            assert_eq!(risk.token_a_maximum_fee, U256::from(100_000_u64));
            assert!(risk.first_started_at_unix_ms.is_some());
            journal
                .advance(
                    &operation_id,
                    RebalanceExecutionProgress::Failed {
                        reason: "reviewed terminal failure".to_owned(),
                    },
                )
                .unwrap();
        }

        let journal = RebalanceExecutionJournal::open(&path).unwrap();
        let risk = journal
            .rebalance_risk("esp-usdc-arbitrum-rebalance-test-r2")
            .unwrap();
        assert_eq!(risk.transfer_count, 1);
        assert_eq!(risk.active_transfer_count, 0);
        assert_eq!(risk.failed_transfer_count, 1);
        assert!(journal.active_operation().unwrap().is_none());
        assert_eq!(
            journal
                .latest_rebalance_operation("esp-usdc-arbitrum-rebalance-test-r2")
                .unwrap()
                .intent
                .operation_id,
            operation_id
        );
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn wallet_deposit_has_zero_token_fee_but_still_consumes_transfer_authority() {
        let path = path("rebalance-wallet-deposit");
        let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
        let mut request = request(Direction::WalletToBinance, direct_arbitrum());
        request.authority = RebalanceExecutionAuthority::ArbitrumFullLive;
        request.action.amount = U256::from(900_000_u64);
        request.wallet_balance_before = U256::from(1_000_000_u64);
        request.maximum_fee = Some(U256::ZERO);
        request.approval_session_id = Some("esp-usdc-arbitrum-rebalance-test-r2".to_owned());
        journal.reserve(&request).unwrap();

        let risk = journal
            .rebalance_risk("esp-usdc-arbitrum-rebalance-test-r2")
            .unwrap();
        assert_eq!(risk.transfer_count, 1);
        assert_eq!(risk.active_transfer_count, 1);
        assert_eq!(risk.token_a_debit, U256::from(900_000_u64));
        assert_eq!(risk.token_a_maximum_fee, U256::ZERO);
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn full_live_authority_retains_the_stable_esp_owner_and_session_scoped_risk() {
        let path = path("esp-full-live-stable-owner");
        let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
        let mut request = request(Direction::BinanceToWallet, direct_arbitrum());
        request.authority = RebalanceExecutionAuthority::ArbitrumFullLive;
        request.maximum_fee = Some(U256::from(100_000_u64));
        request.approval_session_id = Some("esp-usdc-arbitrum-full-live".to_owned());
        let operation = journal.reserve(&request).unwrap();

        assert_eq!(
            operation.intent.scope.as_ref().unwrap().strategy_id,
            "rebalance-arbitrum-usdc-esp"
        );
        let risk = journal
            .rebalance_risk("esp-usdc-arbitrum-full-live")
            .unwrap();
        assert_eq!(risk.transfer_count, 1);
        assert_eq!(risk.active_transfer_count, 1);
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn production_authority_cannot_select_a_bridge_or_another_network() {
        let path = path("rebalance-direct-only");
        let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
        let mut bridged = request(Direction::BinanceToWallet, across());
        bridged.authority = RebalanceExecutionAuthority::ArbitrumFullLive;
        bridged.maximum_fee = Some(U256::from(100_000_u64));
        bridged.approval_session_id = Some("esp-usdc-arbitrum-rebalance-test-r2".to_owned());
        assert!(journal.reserve(&bridged).is_err());

        let mut wrong_network = request(Direction::BinanceToWallet, direct_arbitrum());
        wrong_network.authority = RebalanceExecutionAuthority::ArbitrumFullLive;
        wrong_network.maximum_fee = Some(U256::from(100_000_u64));
        let Route::Direct {
            binance_network, ..
        } = &mut wrong_network.action.route
        else {
            unreachable!()
        };
        *binance_network = "OPTIMISM".to_owned();
        assert!(journal.reserve(&wrong_network).is_err());
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn rejects_duplicate_owner_corruption_and_illegal_transitions() {
        let path = path("safety");
        let mut journal = RebalanceExecutionJournal::open(&path).unwrap();
        let operation = journal
            .reserve(&request(Direction::BinanceToWallet, across()))
            .unwrap();
        assert!(RebalanceExecutionJournal::open(&path).is_err());
        assert!(
            journal
                .reserve(&request(Direction::WalletToBinance, across()))
                .is_err()
        );
        assert!(
            journal
                .advance(
                    &operation.intent.operation_id,
                    RebalanceExecutionProgress::DepositTransferMined {
                        chain_id: 10,
                        transaction_hash: B256::repeat_byte(0x44),
                    }
                )
                .is_err()
        );
        drop(journal);
        let contents = fs::read_to_string(&path).unwrap();
        fs::write(&path, contents.replace("USDC", "USDT")).unwrap();
        assert!(RebalanceExecutionJournal::open(&path).is_err());
        fs::remove_file(path).unwrap();
    }
}

use alloy_primitives::{Address, B256, keccak256};
use anyhow::{Context, ensure};

use crate::chain::rpc::{JsonRpcClient, RpcTransaction, TransactionReceipt};

use super::{
    JournalIntent, JournalOperationIdentity, SignedTransaction, TransactionJournal,
    UnknownOutcomeReason, WalletCall,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NonceLaneState {
    Ready {
        next_nonce: u64,
    },
    Reserved {
        identity: JournalOperationIdentity,
    },
    Signed {
        identity: JournalOperationIdentity,
        transaction_hash: B256,
    },
    Broadcast {
        identity: JournalOperationIdentity,
        transaction_hash: B256,
    },
    RecoveryRequired {
        identity: JournalOperationIdentity,
        transaction_hash: Option<B256>,
    },
    UntrackedPending {
        latest_nonce: u64,
        pending_nonce: u64,
    },
}

#[derive(Debug)]
pub struct NonceLane {
    chain_id: u64,
    wallet: Address,
    state: NonceLaneState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NonceReconciliationOutcome {
    Ready {
        next_nonce: u64,
    },
    MinedSuccess {
        operation_id: String,
        transaction_hash: B256,
        block_number: u64,
    },
    MinedReverted {
        operation_id: String,
        transaction_hash: B256,
        block_number: u64,
    },
    TransactionKnown {
        operation_id: String,
        transaction_hash: B256,
        block_number: Option<u64>,
    },
    SignedTransactionAbsent {
        operation_id: String,
        transaction_hash: B256,
    },
    NonceConsumedWithoutMatchingTransaction {
        operation_id: String,
        transaction_hash: B256,
        latest_nonce: u64,
    },
    NonceOccupiedWithoutMatchingTransaction {
        operation_id: String,
        transaction_hash: B256,
        pending_nonce: u64,
    },
    UnsignedIntentNeedsReview {
        operation_id: String,
        nonce: u64,
    },
    UntrackedPending {
        latest_nonce: u64,
        pending_nonce: u64,
    },
}

impl NonceReconciliationOutcome {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Ready { .. } => "ready",
            Self::MinedSuccess { .. } => "mined_success",
            Self::MinedReverted { .. } => "mined_reverted",
            Self::TransactionKnown { .. } => "transaction_known",
            Self::SignedTransactionAbsent { .. } => "signed_transaction_absent",
            Self::NonceConsumedWithoutMatchingTransaction { .. } => {
                "nonce_consumed_without_matching_transaction"
            }
            Self::NonceOccupiedWithoutMatchingTransaction { .. } => {
                "nonce_occupied_without_matching_transaction"
            }
            Self::UnsignedIntentNeedsReview { .. } => "unsigned_intent_needs_review",
            Self::UntrackedPending { .. } => "untracked_pending",
        }
    }
}

#[derive(Debug)]
pub struct ReconciledNonceLane {
    pub lane: NonceLane,
    pub outcome: NonceReconciliationOutcome,
}

impl NonceLane {
    /// Rebuilds the lane from the journal and conservatively reconciles its one
    /// unresolved operation against canonical RPC evidence.
    pub async fn reconcile(
        rpc: &JsonRpcClient,
        journal: &mut TransactionJournal,
        chain_id: u64,
        wallet: Address,
        latest_nonce: u64,
        pending_nonce: u64,
    ) -> anyhow::Result<ReconciledNonceLane> {
        let lane = Self::hydrate(chain_id, wallet, latest_nonce, pending_nonce, journal)?;
        let NonceLaneState::RecoveryRequired {
            transaction_hash, ..
        } = lane.state()
        else {
            return Ok(ReconciledNonceLane {
                outcome: outcome_for_non_recovery_state(lane.state())?,
                lane,
            });
        };
        let Some(transaction_hash) = *transaction_hash else {
            return apply_recovery_observation(
                lane,
                journal,
                latest_nonce,
                pending_nonce,
                None,
                None,
            );
        };

        let receipt = rpc.transaction_receipt(transaction_hash).await?;
        if receipt.is_some() {
            return apply_recovery_observation(
                lane,
                journal,
                latest_nonce,
                pending_nonce,
                receipt,
                None,
            );
        }
        let transaction = rpc.transaction_by_hash(transaction_hash).await?;
        apply_recovery_observation(
            lane,
            journal,
            latest_nonce,
            pending_nonce,
            None,
            transaction,
        )
    }

    pub fn hydrate(
        chain_id: u64,
        wallet: Address,
        latest_nonce: u64,
        pending_nonce: u64,
        journal: &TransactionJournal,
    ) -> anyhow::Result<Self> {
        ensure!(chain_id > 0, "nonce lane chain id is zero");
        ensure!(wallet != Address::ZERO, "nonce lane wallet is zero");
        ensure!(
            latest_nonce <= pending_nonce,
            "latest nonce exceeds pending nonce"
        );
        if let Some(consumed_nonce) = journal.highest_consumed_nonce(chain_id, wallet) {
            ensure!(
                pending_nonce > consumed_nonce,
                "pending nonce has not advanced past a journaled mined transaction"
            );
        }

        let unresolved = journal.unresolved_for(chain_id, wallet);
        ensure!(
            unresolved.len() <= 1,
            "nonce lane has multiple unresolved journal operations"
        );
        let state = if let Some(operation) = unresolved.first() {
            ensure!(
                operation.intent.identity.nonce <= pending_nonce,
                "journal operation nonce is ahead of the RPC pending nonce"
            );
            NonceLaneState::RecoveryRequired {
                identity: operation.intent.identity.clone(),
                transaction_hash: operation.status.transaction_hash(),
            }
        } else if latest_nonce != pending_nonce {
            NonceLaneState::UntrackedPending {
                latest_nonce,
                pending_nonce,
            }
        } else {
            NonceLaneState::Ready {
                next_nonce: pending_nonce,
            }
        };

        Ok(Self {
            chain_id,
            wallet,
            state,
        })
    }

    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    pub fn wallet(&self) -> Address {
        self.wallet
    }

    pub fn state(&self) -> &NonceLaneState {
        &self.state
    }

    pub fn ready(&self) -> bool {
        matches!(self.state, NonceLaneState::Ready { .. })
    }

    pub fn next_nonce(&self) -> Option<u64> {
        match self.state {
            NonceLaneState::Ready { next_nonce } => Some(next_nonce),
            _ => None,
        }
    }

    /// Reserves the lane only after the operation intent is durably fsynced.
    pub fn reserve(
        &mut self,
        journal: &mut TransactionJournal,
        operation_id: impl Into<String>,
        purpose: impl Into<String>,
        call: &WalletCall,
    ) -> anyhow::Result<JournalOperationIdentity> {
        let NonceLaneState::Ready { next_nonce } = self.state else {
            anyhow::bail!("nonce lane is not ready for a new operation");
        };
        let identity = JournalOperationIdentity {
            operation_id: operation_id.into(),
            chain_id: self.chain_id,
            wallet: self.wallet,
            nonce: next_nonce,
        };
        let intent = JournalIntent {
            identity: identity.clone(),
            purpose: purpose.into(),
            target: call.target(),
            native_value: call.value(),
            calldata_hash: keccak256(call.calldata()),
        };
        journal.record_intent(&intent)?;
        self.state = NonceLaneState::Reserved {
            identity: identity.clone(),
        };
        Ok(identity)
    }

    pub fn record_signed(
        &mut self,
        journal: &mut TransactionJournal,
        transaction: &SignedTransaction,
    ) -> anyhow::Result<()> {
        let identity = match &self.state {
            NonceLaneState::Reserved { identity } => identity.clone(),
            _ => anyhow::bail!("nonce lane has no reserved operation to sign"),
        };
        ensure!(
            transaction.chain_id == identity.chain_id,
            "signed transaction chain does not match nonce reservation"
        );
        ensure!(
            transaction.nonce == identity.nonce,
            "signed transaction nonce does not match nonce reservation"
        );
        journal.record_signed(&identity, transaction.hash)?;
        self.state = NonceLaneState::Signed {
            identity,
            transaction_hash: transaction.hash,
        };
        Ok(())
    }

    pub fn record_broadcast(
        &mut self,
        journal: &mut TransactionJournal,
        transaction_hash: B256,
    ) -> anyhow::Result<()> {
        let (identity, signed_hash) = match &self.state {
            NonceLaneState::Signed {
                identity,
                transaction_hash,
            } => (identity.clone(), *transaction_hash),
            _ => anyhow::bail!("nonce lane has no signed operation to broadcast"),
        };
        ensure!(
            transaction_hash == signed_hash,
            "broadcast hash does not match signed transaction"
        );
        journal.record_broadcast(&identity, transaction_hash)?;
        self.state = NonceLaneState::Broadcast {
            identity,
            transaction_hash,
        };
        Ok(())
    }

    pub fn record_unknown_outcome(
        &mut self,
        journal: &mut TransactionJournal,
        reason: UnknownOutcomeReason,
    ) -> anyhow::Result<()> {
        let (identity, transaction_hash) = match &self.state {
            NonceLaneState::Signed {
                identity,
                transaction_hash,
            }
            | NonceLaneState::Broadcast {
                identity,
                transaction_hash,
            } => (identity.clone(), *transaction_hash),
            _ => anyhow::bail!("nonce lane has no transaction with an unknown outcome"),
        };
        journal.record_unknown_outcome(&identity, transaction_hash, reason)?;
        self.state = NonceLaneState::RecoveryRequired {
            identity,
            transaction_hash: Some(transaction_hash),
        };
        Ok(())
    }

    pub fn record_receipt(
        &mut self,
        journal: &mut TransactionJournal,
        receipt: TransactionReceipt,
    ) -> anyhow::Result<()> {
        let (identity, expected_hash) = match &self.state {
            NonceLaneState::Signed {
                identity,
                transaction_hash,
            }
            | NonceLaneState::Broadcast {
                identity,
                transaction_hash,
            } => (identity.clone(), *transaction_hash),
            NonceLaneState::RecoveryRequired {
                identity,
                transaction_hash: Some(transaction_hash),
            } => (identity.clone(), *transaction_hash),
            _ => anyhow::bail!("nonce lane has no hashed transaction to reconcile"),
        };
        ensure!(
            receipt.transaction_hash == expected_hash,
            "receipt hash does not match nonce lane transaction"
        );
        ensure!(
            receipt.status <= 1,
            "receipt status is neither success nor revert"
        );
        journal.record_mined(
            &identity,
            receipt.transaction_hash,
            receipt.block_number,
            receipt.status == 1,
        )?;
        let next_nonce = identity
            .nonce
            .checked_add(1)
            .context("nonce lane overflow after mined transaction")?;
        self.state = NonceLaneState::Ready { next_nonce };
        Ok(())
    }

    pub fn cancel_before_signing(
        &mut self,
        journal: &mut TransactionJournal,
    ) -> anyhow::Result<()> {
        let identity = match &self.state {
            NonceLaneState::Reserved { identity } => identity.clone(),
            _ => anyhow::bail!("only a reserved unsigned nonce can be cancelled"),
        };
        journal.record_cancelled_before_signing(&identity)?;
        self.state = NonceLaneState::Ready {
            next_nonce: identity.nonce,
        };
        Ok(())
    }
}

fn outcome_for_non_recovery_state(
    state: &NonceLaneState,
) -> anyhow::Result<NonceReconciliationOutcome> {
    match state {
        NonceLaneState::Ready { next_nonce } => Ok(NonceReconciliationOutcome::Ready {
            next_nonce: *next_nonce,
        }),
        NonceLaneState::UntrackedPending {
            latest_nonce,
            pending_nonce,
        } => Ok(NonceReconciliationOutcome::UntrackedPending {
            latest_nonce: *latest_nonce,
            pending_nonce: *pending_nonce,
        }),
        _ => anyhow::bail!("nonce lane state unexpectedly requires active-process recovery"),
    }
}

fn apply_recovery_observation(
    mut lane: NonceLane,
    journal: &mut TransactionJournal,
    latest_nonce: u64,
    pending_nonce: u64,
    receipt: Option<TransactionReceipt>,
    transaction: Option<RpcTransaction>,
) -> anyhow::Result<ReconciledNonceLane> {
    let (identity, transaction_hash) = match lane.state() {
        NonceLaneState::RecoveryRequired {
            identity,
            transaction_hash,
        } => (identity.clone(), *transaction_hash),
        _ => anyhow::bail!("nonce lane does not require startup recovery"),
    };
    ensure!(
        receipt.is_none() || transaction.is_none(),
        "recovery observation contains both a receipt and a transaction"
    );

    if let Some(receipt) = receipt {
        let outcome = if receipt.status == 1 {
            NonceReconciliationOutcome::MinedSuccess {
                operation_id: identity.operation_id.clone(),
                transaction_hash: receipt.transaction_hash,
                block_number: receipt.block_number,
            }
        } else {
            NonceReconciliationOutcome::MinedReverted {
                operation_id: identity.operation_id.clone(),
                transaction_hash: receipt.transaction_hash,
                block_number: receipt.block_number,
            }
        };
        lane.record_receipt(journal, receipt)?;
        return Ok(ReconciledNonceLane { lane, outcome });
    }

    let Some(expected_hash) = transaction_hash else {
        return Ok(ReconciledNonceLane {
            outcome: NonceReconciliationOutcome::UnsignedIntentNeedsReview {
                operation_id: identity.operation_id.clone(),
                nonce: identity.nonce,
            },
            lane,
        });
    };

    if let Some(transaction) = transaction {
        validate_recovered_transaction(journal, &identity, expected_hash, &transaction)?;
        return Ok(ReconciledNonceLane {
            outcome: NonceReconciliationOutcome::TransactionKnown {
                operation_id: identity.operation_id.clone(),
                transaction_hash: expected_hash,
                block_number: transaction.block_number,
            },
            lane,
        });
    }

    let outcome = if latest_nonce > identity.nonce {
        NonceReconciliationOutcome::NonceConsumedWithoutMatchingTransaction {
            operation_id: identity.operation_id.clone(),
            transaction_hash: expected_hash,
            latest_nonce,
        }
    } else if pending_nonce > identity.nonce {
        NonceReconciliationOutcome::NonceOccupiedWithoutMatchingTransaction {
            operation_id: identity.operation_id.clone(),
            transaction_hash: expected_hash,
            pending_nonce,
        }
    } else {
        NonceReconciliationOutcome::SignedTransactionAbsent {
            operation_id: identity.operation_id.clone(),
            transaction_hash: expected_hash,
        }
    };
    Ok(ReconciledNonceLane { lane, outcome })
}

fn validate_recovered_transaction(
    journal: &TransactionJournal,
    identity: &JournalOperationIdentity,
    expected_hash: B256,
    transaction: &RpcTransaction,
) -> anyhow::Result<()> {
    let operation = journal
        .operation(&identity.operation_id)
        .context("recovery operation is missing from the transaction journal")?;
    ensure!(transaction.hash == expected_hash, "recovered hash mismatch");
    ensure!(
        transaction.chain_id == identity.chain_id,
        "recovered transaction chain mismatch"
    );
    ensure!(
        transaction.nonce == identity.nonce,
        "recovered transaction nonce mismatch"
    );
    ensure!(
        transaction.from == identity.wallet,
        "recovered transaction sender mismatch"
    );
    ensure!(
        transaction.to == Some(operation.intent.target),
        "recovered transaction target mismatch"
    );
    ensure!(
        transaction.value == operation.intent.native_value,
        "recovered transaction value mismatch"
    );
    ensure!(
        keccak256(&transaction.input) == operation.intent.calldata_hash,
        "recovered transaction calldata mismatch"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
    };

    use alloy_primitives::{Address, B256, U256};

    use crate::{
        chain::rpc::{RpcTransaction, TransactionReceipt},
        wallet::{
            EvmWallet, NonceLaneState, NonceReconciliationOutcome, SignedTransaction,
            TransactionJournal, UnknownOutcomeReason, WalletCall, WalletTransactionParameters,
        },
    };

    use super::{NonceLane, apply_recovery_observation};

    const PRIVATE_KEY: &str = "0x59c6995e998f97a5a0044976f7d04f8b2b7f4e5b5d5f3e49f2f4e7838a2b0c19";
    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    fn journal_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "poly-bot-nonce-lane-{}-{name}-{}.jsonl",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn signed(
        wallet: &EvmWallet,
        call: &WalletCall,
        nonce: u64,
    ) -> crate::wallet::SignedTransaction {
        wallet
            .sign_call(
                call,
                WalletTransactionParameters {
                    chain_id: 480,
                    nonce,
                    gas_limit: 80_000,
                    max_fee_per_gas: 2_000_000,
                    max_priority_fee_per_gas: 1_000_000,
                },
            )
            .unwrap()
    }

    fn unknown_recovery(
        name: &str,
        latest_nonce: u64,
        pending_nonce: u64,
    ) -> (
        std::path::PathBuf,
        TransactionJournal,
        NonceLane,
        EvmWallet,
        WalletCall,
        SignedTransaction,
    ) {
        let path = journal_path(name);
        let wallet = EvmWallet::from_private_key(PRIVATE_KEY).unwrap();
        let call = WalletCall::erc20_transfer(
            Address::repeat_byte(0x22),
            Address::repeat_byte(0x33),
            U256::from(1_000_000),
        )
        .unwrap();
        let transaction;
        {
            let mut journal = TransactionJournal::open(&path).unwrap();
            let mut lane = NonceLane::hydrate(480, wallet.address(), 7, 7, &journal).unwrap();
            let identity = lane
                .reserve(
                    &mut journal,
                    format!("recovery:{name}"),
                    "recovery_test",
                    &call,
                )
                .unwrap();
            transaction = signed(&wallet, &call, identity.nonce);
            lane.record_signed(&mut journal, &transaction).unwrap();
            lane.record_unknown_outcome(&mut journal, UnknownOutcomeReason::ProcessInterrupted)
                .unwrap();
        }
        let journal = TransactionJournal::open(&path).unwrap();
        let lane = NonceLane::hydrate(480, wallet.address(), latest_nonce, pending_nonce, &journal)
            .unwrap();
        (path, journal, lane, wallet, call, transaction)
    }

    fn recovered_transaction(
        wallet: &EvmWallet,
        call: &WalletCall,
        transaction: &SignedTransaction,
    ) -> RpcTransaction {
        RpcTransaction {
            hash: transaction.hash,
            chain_id: transaction.chain_id,
            nonce: transaction.nonce,
            from: wallet.address(),
            to: Some(call.target()),
            value: call.value(),
            input: call.calldata().to_vec(),
            block_number: None,
        }
    }

    #[test]
    fn journals_intent_before_reserving_and_advances_only_after_receipt() {
        let path = journal_path("lifecycle");
        let mut journal = TransactionJournal::open(&path).unwrap();
        let wallet = EvmWallet::from_private_key(PRIVATE_KEY).unwrap();
        let mut lane = NonceLane::hydrate(480, wallet.address(), 7, 7, &journal).unwrap();
        let call = WalletCall::erc20_transfer(
            Address::repeat_byte(0x22),
            Address::repeat_byte(0x33),
            U256::from(1_000_000),
        )
        .unwrap();

        let identity = lane
            .reserve(
                &mut journal,
                "rebalance:wld:1",
                "rebalance_wallet_to_binance",
                &call,
            )
            .unwrap();
        assert_eq!(identity.nonce, 7);
        assert!(journal.operation(&identity.operation_id).is_some());
        assert!(matches!(lane.state(), NonceLaneState::Reserved { .. }));

        let transaction = signed(&wallet, &call, identity.nonce);
        lane.record_signed(&mut journal, &transaction).unwrap();
        lane.record_broadcast(&mut journal, transaction.hash)
            .unwrap();
        assert!(!lane.ready());
        lane.record_receipt(
            &mut journal,
            TransactionReceipt {
                transaction_hash: transaction.hash,
                block_number: 12_345,
                status: 1,
                gas_used: 50_000,
                effective_gas_price: 1_000_000,
                logs: vec![],
            },
        )
        .unwrap();
        assert_eq!(lane.state(), &NonceLaneState::Ready { next_nonce: 8 });
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn restart_with_unknown_outcome_requires_recovery_and_reconciles_receipt() {
        let path = journal_path("recovery");
        let wallet = EvmWallet::from_private_key(PRIVATE_KEY).unwrap();
        let call = WalletCall::native_transfer(Address::repeat_byte(0x44), U256::ONE).unwrap();
        let transaction;
        {
            let mut journal = TransactionJournal::open(&path).unwrap();
            let mut lane = NonceLane::hydrate(480, wallet.address(), 7, 7, &journal).unwrap();
            let identity = lane
                .reserve(&mut journal, "rebalance:eth:2", "bridge_native_eth", &call)
                .unwrap();
            transaction = signed(&wallet, &call, identity.nonce);
            lane.record_signed(&mut journal, &transaction).unwrap();
            lane.record_unknown_outcome(&mut journal, UnknownOutcomeReason::BroadcastTransport)
                .unwrap();
        }

        let mut journal = TransactionJournal::open(&path).unwrap();
        let mut lane = NonceLane::hydrate(480, wallet.address(), 7, 8, &journal).unwrap();
        assert!(matches!(
            lane.state(),
            NonceLaneState::RecoveryRequired {
                transaction_hash: Some(hash),
                ..
            } if *hash == transaction.hash
        ));
        assert!(
            lane.reserve(&mut journal, "another", "another", &call)
                .is_err()
        );
        lane.record_receipt(
            &mut journal,
            TransactionReceipt {
                transaction_hash: transaction.hash,
                block_number: 12_346,
                status: 0,
                gas_used: 21_000,
                effective_gas_price: 1_000_000,
                logs: vec![],
            },
        )
        .unwrap();
        assert_eq!(lane.state(), &NonceLaneState::Ready { next_nonce: 8 });
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn untracked_pending_nonce_blocks_new_work() {
        let path = journal_path("untracked");
        let journal = TransactionJournal::open(&path).unwrap();
        let lane = NonceLane::hydrate(480, Address::repeat_byte(0x11), 7, 8, &journal).unwrap();
        assert_eq!(
            lane.state(),
            &NonceLaneState::UntrackedPending {
                latest_nonce: 7,
                pending_nonce: 8,
            }
        );
        assert!(!lane.ready());
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn unsigned_reservation_can_be_cancelled_but_signed_nonce_cannot() {
        let path = journal_path("cancel");
        let mut journal = TransactionJournal::open(&path).unwrap();
        let wallet = EvmWallet::from_private_key(PRIVATE_KEY).unwrap();
        let call = WalletCall::native_transfer(Address::repeat_byte(0x44), U256::ONE).unwrap();
        let mut lane = NonceLane::hydrate(480, wallet.address(), 7, 7, &journal).unwrap();
        lane.reserve(&mut journal, "cancel:1", "cancel_test", &call)
            .unwrap();
        lane.cancel_before_signing(&mut journal).unwrap();
        assert_eq!(lane.state(), &NonceLaneState::Ready { next_nonce: 7 });

        let identity = lane
            .reserve(&mut journal, "cancel:2", "cancel_test", &call)
            .unwrap();
        let transaction = signed(&wallet, &call, identity.nonce);
        lane.record_signed(&mut journal, &transaction).unwrap();
        assert!(lane.cancel_before_signing(&mut journal).is_err());
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn rejects_receipt_for_another_hash() {
        let path = journal_path("wrong-hash");
        let mut journal = TransactionJournal::open(&path).unwrap();
        let wallet = EvmWallet::from_private_key(PRIVATE_KEY).unwrap();
        let call = WalletCall::native_transfer(Address::repeat_byte(0x44), U256::ONE).unwrap();
        let mut lane = NonceLane::hydrate(480, wallet.address(), 7, 7, &journal).unwrap();
        let identity = lane
            .reserve(&mut journal, "receipt:1", "receipt_test", &call)
            .unwrap();
        let transaction = signed(&wallet, &call, identity.nonce);
        lane.record_signed(&mut journal, &transaction).unwrap();
        assert!(
            lane.record_receipt(
                &mut journal,
                TransactionReceipt {
                    transaction_hash: B256::repeat_byte(0x99),
                    block_number: 1,
                    status: 1,
                    gas_used: 21_000,
                    effective_gas_price: 1,
                    logs: vec![],
                },
            )
            .is_err()
        );
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn startup_reconciliation_closes_a_matching_receipt() {
        let (path, mut journal, lane, _wallet, _call, transaction) =
            unknown_recovery("startup-receipt", 7, 8);
        let reconciled = apply_recovery_observation(
            lane,
            &mut journal,
            7,
            8,
            Some(TransactionReceipt {
                transaction_hash: transaction.hash,
                block_number: 88,
                status: 1,
                gas_used: 50_000,
                effective_gas_price: 1_000_000,
                logs: vec![],
            }),
            None,
        )
        .unwrap();

        assert_eq!(
            reconciled.lane.state(),
            &NonceLaneState::Ready { next_nonce: 8 }
        );
        assert_eq!(
            reconciled.outcome,
            NonceReconciliationOutcome::MinedSuccess {
                operation_id: "recovery:startup-receipt".to_owned(),
                transaction_hash: transaction.hash,
                block_number: 88,
            }
        );
        assert!(
            journal
                .unresolved_for(480, reconciled.lane.wallet())
                .is_empty()
        );
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn startup_reconciliation_validates_known_transaction_and_stays_blocked() {
        let (path, mut journal, lane, wallet, call, signed) =
            unknown_recovery("startup-known", 7, 8);
        let transaction = recovered_transaction(&wallet, &call, &signed);
        let reconciled =
            apply_recovery_observation(lane, &mut journal, 7, 8, None, Some(transaction)).unwrap();

        assert!(!reconciled.lane.ready());
        assert_eq!(
            reconciled.outcome,
            NonceReconciliationOutcome::TransactionKnown {
                operation_id: "recovery:startup-known".to_owned(),
                transaction_hash: signed.hash,
                block_number: None,
            }
        );
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn startup_reconciliation_rejects_transaction_that_does_not_match_intent() {
        let (path, mut journal, lane, wallet, call, signed) =
            unknown_recovery("startup-mismatch", 7, 8);
        let mut transaction = recovered_transaction(&wallet, &call, &signed);
        transaction.value = U256::ONE;

        let error = apply_recovery_observation(lane, &mut journal, 7, 8, None, Some(transaction))
            .unwrap_err();
        assert!(error.to_string().contains("value mismatch"));
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn startup_reconciliation_classifies_missing_hash_without_unlocking_lane() {
        let cases = [
            ("startup-absent", 7, 7, "signed_transaction_absent"),
            (
                "startup-occupied",
                7,
                8,
                "nonce_occupied_without_matching_transaction",
            ),
            (
                "startup-consumed",
                8,
                8,
                "nonce_consumed_without_matching_transaction",
            ),
        ];
        for (name, latest_nonce, pending_nonce, expected_label) in cases {
            let (path, mut journal, lane, _wallet, _call, _signed) =
                unknown_recovery(name, latest_nonce, pending_nonce);
            let reconciled = apply_recovery_observation(
                lane,
                &mut journal,
                latest_nonce,
                pending_nonce,
                None,
                None,
            )
            .unwrap();
            assert!(!reconciled.lane.ready());
            assert_eq!(reconciled.outcome.label(), expected_label);
            drop(journal);
            fs::remove_file(path).unwrap();
        }
    }

    #[test]
    fn startup_reconciliation_keeps_unsigned_intent_blocked_for_review() {
        let path = journal_path("startup-unsigned");
        let wallet = EvmWallet::from_private_key(PRIVATE_KEY).unwrap();
        let call = WalletCall::native_transfer(Address::repeat_byte(0x44), U256::ONE).unwrap();
        {
            let mut journal = TransactionJournal::open(&path).unwrap();
            let mut lane = NonceLane::hydrate(480, wallet.address(), 7, 7, &journal).unwrap();
            lane.reserve(&mut journal, "unsigned:1", "unsigned_test", &call)
                .unwrap();
        }
        let mut journal = TransactionJournal::open(&path).unwrap();
        let lane = NonceLane::hydrate(480, wallet.address(), 7, 7, &journal).unwrap();
        let reconciled = apply_recovery_observation(lane, &mut journal, 7, 7, None, None).unwrap();
        assert_eq!(
            reconciled.outcome,
            NonceReconciliationOutcome::UnsignedIntentNeedsReview {
                operation_id: "unsigned:1".to_owned(),
                nonce: 7,
            }
        );
        assert!(!reconciled.lane.ready());
        drop(journal);
        fs::remove_file(path).unwrap();
    }
}

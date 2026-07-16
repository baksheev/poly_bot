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

use alloy_primitives::{Address, B256, U256};
use anyhow::{Context, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const JOURNAL_VERSION: u16 = 1;
const MAX_JOURNAL_LINE_BYTES: usize = 64 * 1024;
const MAX_OPERATION_ID_BYTES: usize = 160;
const MAX_PURPOSE_BYTES: usize = 96;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalOperationIdentity {
    pub operation_id: String,
    pub chain_id: u64,
    pub wallet: Address,
    pub nonce: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalIntent {
    pub identity: JournalOperationIdentity,
    pub purpose: String,
    pub target: Address,
    pub native_value: U256,
    pub calldata_hash: B256,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UnknownOutcomeReason {
    BroadcastTransport,
    BroadcastRejected,
    ConfirmationTimeout,
    ProcessInterrupted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JournalStatus {
    IntentRecorded,
    Signed {
        transaction_hash: B256,
    },
    Broadcast {
        transaction_hash: B256,
    },
    OutcomeUnknown {
        transaction_hash: B256,
        reason: UnknownOutcomeReason,
    },
    MinedSuccess {
        transaction_hash: B256,
        block_number: u64,
    },
    MinedReverted {
        transaction_hash: B256,
        block_number: u64,
    },
    CancelledBeforeSigning,
}

impl JournalStatus {
    pub fn is_unresolved(&self) -> bool {
        matches!(
            self,
            Self::IntentRecorded
                | Self::Signed { .. }
                | Self::Broadcast { .. }
                | Self::OutcomeUnknown { .. }
        )
    }

    pub fn transaction_hash(&self) -> Option<B256> {
        match self {
            Self::Signed { transaction_hash }
            | Self::Broadcast { transaction_hash }
            | Self::OutcomeUnknown {
                transaction_hash, ..
            }
            | Self::MinedSuccess {
                transaction_hash, ..
            }
            | Self::MinedReverted {
                transaction_hash, ..
            } => Some(*transaction_hash),
            Self::IntentRecorded | Self::CancelledBeforeSigning => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalOperation {
    pub intent: JournalIntent,
    pub status: JournalStatus,
}

pub struct TransactionJournal {
    path: PathBuf,
    file: File,
    operations: BTreeMap<String, JournalOperation>,
    next_sequence: u64,
    poisoned: bool,
}

impl std::fmt::Debug for TransactionJournal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransactionJournal")
            .field("path", &self.path)
            .field("operations", &self.operations.len())
            .field("next_sequence", &self.next_sequence)
            .field("poisoned", &self.poisoned)
            .finish()
    }
}

impl TransactionJournal {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        ensure!(
            !path.as_os_str().is_empty(),
            "transaction journal path is empty"
        );
        let existed = path.exists();
        if existed {
            let metadata = symlink_metadata(&path).with_context(|| {
                format!("failed to inspect transaction journal {}", path.display())
            })?;
            ensure!(
                !metadata.file_type().is_symlink(),
                "transaction journal must not be a symbolic link"
            );
            ensure!(metadata.is_file(), "transaction journal path is not a file");
        } else {
            let parent = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            ensure!(
                parent.is_dir(),
                "transaction journal parent directory does not exist"
            );
        }

        let mut options = OpenOptions::new();
        options.create(true).read(true).append(true);
        #[cfg(unix)]
        options.mode(0o600);
        let file = options
            .open(&path)
            .with_context(|| format!("failed to open transaction journal {}", path.display()))?;
        validate_permissions(&file)?;
        file.try_lock()
            .context("transaction journal is already locked by another process")?;
        if !existed {
            sync_new_file_and_parent(&file, &path)?;
        }

        let mut operations = BTreeMap::new();
        let mut expected_sequence = 0_u64;
        let mut reader = BufReader::new(
            file.try_clone()
                .context("failed to clone transaction journal handle")?,
        );
        loop {
            let mut line = Vec::new();
            let bytes = reader
                .read_until(b'\n', &mut line)
                .context("failed to read transaction journal")?;
            if bytes == 0 {
                break;
            }
            ensure!(
                line.len() <= MAX_JOURNAL_LINE_BYTES,
                "transaction journal record exceeds size limit"
            );
            ensure!(
                line.last() == Some(&b'\n'),
                "transaction journal ends with a partial record"
            );
            line.pop();
            ensure!(
                !line.is_empty(),
                "transaction journal contains an empty record"
            );
            let record: WireRecord = serde_json::from_slice(&line)
                .context("transaction journal contains invalid JSON")?;
            record.validate_checksum()?;
            ensure!(
                record.payload.version == JOURNAL_VERSION,
                "unsupported transaction journal version {}",
                record.payload.version
            );
            ensure!(
                record.payload.sequence == expected_sequence,
                "transaction journal sequence mismatch: found {}, expected {expected_sequence}",
                record.payload.sequence
            );
            apply_payload(&mut operations, &record.payload)?;
            expected_sequence = expected_sequence
                .checked_add(1)
                .context("transaction journal sequence overflow")?;
        }

        Ok(Self {
            path,
            file,
            operations,
            next_sequence: expected_sequence,
            poisoned: false,
        })
    }

    pub fn operations(&self) -> &BTreeMap<String, JournalOperation> {
        &self.operations
    }

    pub fn operation(&self, operation_id: &str) -> Option<&JournalOperation> {
        self.operations.get(operation_id)
    }

    pub fn unresolved_for(&self, chain_id: u64, wallet: Address) -> Vec<&JournalOperation> {
        self.operations
            .values()
            .filter(|operation| {
                operation.intent.identity.chain_id == chain_id
                    && operation.intent.identity.wallet == wallet
                    && operation.status.is_unresolved()
            })
            .collect()
    }

    pub fn highest_consumed_nonce(&self, chain_id: u64, wallet: Address) -> Option<u64> {
        self.operations
            .values()
            .filter(|operation| {
                operation.intent.identity.chain_id == chain_id
                    && operation.intent.identity.wallet == wallet
                    && matches!(
                        operation.status,
                        JournalStatus::MinedSuccess { .. } | JournalStatus::MinedReverted { .. }
                    )
            })
            .map(|operation| operation.intent.identity.nonce)
            .max()
    }

    pub fn record_intent(&mut self, intent: &JournalIntent) -> anyhow::Result<()> {
        validate_intent(intent)?;
        self.append(
            &intent.identity,
            WireEvent::IntentRecorded {
                purpose: intent.purpose.clone(),
                target: format!("{:#x}", intent.target),
                native_value: intent.native_value.to_string(),
                calldata_hash: format!("{:#x}", intent.calldata_hash),
            },
        )
    }

    pub fn record_signed(
        &mut self,
        identity: &JournalOperationIdentity,
        transaction_hash: B256,
    ) -> anyhow::Result<()> {
        self.append(
            identity,
            WireEvent::Signed {
                transaction_hash: format!("{transaction_hash:#x}"),
            },
        )
    }

    pub fn record_broadcast(
        &mut self,
        identity: &JournalOperationIdentity,
        transaction_hash: B256,
    ) -> anyhow::Result<()> {
        self.append(
            identity,
            WireEvent::Broadcast {
                transaction_hash: format!("{transaction_hash:#x}"),
            },
        )
    }

    pub fn record_unknown_outcome(
        &mut self,
        identity: &JournalOperationIdentity,
        transaction_hash: B256,
        reason: UnknownOutcomeReason,
    ) -> anyhow::Result<()> {
        self.append(
            identity,
            WireEvent::OutcomeUnknown {
                transaction_hash: format!("{transaction_hash:#x}"),
                reason,
            },
        )
    }

    pub fn record_mined(
        &mut self,
        identity: &JournalOperationIdentity,
        transaction_hash: B256,
        block_number: u64,
        success: bool,
    ) -> anyhow::Result<()> {
        let event = if success {
            WireEvent::MinedSuccess {
                transaction_hash: format!("{transaction_hash:#x}"),
                block_number,
            }
        } else {
            WireEvent::MinedReverted {
                transaction_hash: format!("{transaction_hash:#x}"),
                block_number,
            }
        };
        self.append(identity, event)
    }

    pub fn record_cancelled_before_signing(
        &mut self,
        identity: &JournalOperationIdentity,
    ) -> anyhow::Result<()> {
        self.append(identity, WireEvent::CancelledBeforeSigning)
    }

    fn append(
        &mut self,
        identity: &JournalOperationIdentity,
        event: WireEvent,
    ) -> anyhow::Result<()> {
        ensure!(!self.poisoned, "transaction journal is poisoned");
        validate_identity(identity)?;
        let payload = WirePayload {
            version: JOURNAL_VERSION,
            sequence: self.next_sequence,
            recorded_at_unix_ms: unix_timestamp_ms()?,
            operation_id: identity.operation_id.clone(),
            chain_id: identity.chain_id,
            wallet: format!("{:#x}", identity.wallet),
            nonce: identity.nonce,
            event,
        };

        let mut next_operations = self.operations.clone();
        apply_payload(&mut next_operations, &payload)?;
        let record = WireRecord::new(payload)?;
        let mut encoded =
            serde_json::to_vec(&record).context("failed to encode transaction journal record")?;
        ensure!(
            encoded.len() < MAX_JOURNAL_LINE_BYTES,
            "transaction journal record exceeds size limit"
        );
        encoded.push(b'\n');

        if let Err(error) = self
            .file
            .write_all(&encoded)
            .and_then(|()| self.file.sync_data())
        {
            self.poisoned = true;
            return Err(error).context("failed to durably append transaction journal record");
        }

        self.operations = next_operations;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .context("transaction journal sequence overflow")?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WireRecord {
    payload: WirePayload,
    checksum_sha256: String,
}

impl WireRecord {
    fn new(payload: WirePayload) -> anyhow::Result<Self> {
        let checksum_sha256 = payload_checksum(&payload)?;
        Ok(Self {
            payload,
            checksum_sha256,
        })
    }

    fn validate_checksum(&self) -> anyhow::Result<()> {
        ensure!(
            self.checksum_sha256 == payload_checksum(&self.payload)?,
            "transaction journal checksum mismatch"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WirePayload {
    version: u16,
    sequence: u64,
    recorded_at_unix_ms: u64,
    operation_id: String,
    chain_id: u64,
    wallet: String,
    nonce: u64,
    event: WireEvent,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WireEvent {
    IntentRecorded {
        purpose: String,
        target: String,
        native_value: String,
        calldata_hash: String,
    },
    Signed {
        transaction_hash: String,
    },
    Broadcast {
        transaction_hash: String,
    },
    OutcomeUnknown {
        transaction_hash: String,
        reason: UnknownOutcomeReason,
    },
    MinedSuccess {
        transaction_hash: String,
        block_number: u64,
    },
    MinedReverted {
        transaction_hash: String,
        block_number: u64,
    },
    CancelledBeforeSigning,
}

fn apply_payload(
    operations: &mut BTreeMap<String, JournalOperation>,
    payload: &WirePayload,
) -> anyhow::Result<()> {
    ensure!(payload.recorded_at_unix_ms > 0, "journal timestamp is zero");
    let identity = JournalOperationIdentity {
        operation_id: payload.operation_id.clone(),
        chain_id: payload.chain_id,
        wallet: Address::from_str(&payload.wallet).context("journal wallet address is invalid")?,
        nonce: payload.nonce,
    };
    validate_identity(&identity)?;

    match &payload.event {
        WireEvent::IntentRecorded {
            purpose,
            target,
            native_value,
            calldata_hash,
        } => {
            ensure!(
                !operations.contains_key(&identity.operation_id),
                "journal operation id already exists"
            );
            ensure!(
                !operations.values().any(|operation| {
                    operation.intent.identity.chain_id == identity.chain_id
                        && operation.intent.identity.wallet == identity.wallet
                        && operation.intent.identity.nonce == identity.nonce
                        && !matches!(operation.status, JournalStatus::CancelledBeforeSigning)
                }),
                "journal nonce is already owned by another operation"
            );
            let intent = JournalIntent {
                identity,
                purpose: purpose.clone(),
                target: Address::from_str(target).context("journal target address is invalid")?,
                native_value: U256::from_str(native_value)
                    .context("journal native value is invalid")?,
                calldata_hash: B256::from_str(calldata_hash)
                    .context("journal calldata hash is invalid")?,
            };
            validate_intent(&intent)?;
            operations.insert(
                intent.identity.operation_id.clone(),
                JournalOperation {
                    intent,
                    status: JournalStatus::IntentRecorded,
                },
            );
        }
        event => {
            let operation = operations
                .get_mut(&identity.operation_id)
                .context("journal event has no matching intent")?;
            ensure!(
                operation.intent.identity == identity,
                "journal operation identity changed"
            );
            apply_transition(operation, event)?;
        }
    }
    Ok(())
}

fn apply_transition(operation: &mut JournalOperation, event: &WireEvent) -> anyhow::Result<()> {
    let previous = &operation.status;
    let next = match event {
        WireEvent::Signed { transaction_hash } => {
            ensure!(
                matches!(previous, JournalStatus::IntentRecorded),
                "journal signed transition requires a recorded intent"
            );
            JournalStatus::Signed {
                transaction_hash: parse_hash(transaction_hash)?,
            }
        }
        WireEvent::Broadcast { transaction_hash } => {
            let hash = parse_hash(transaction_hash)?;
            ensure_hash_transition(previous, hash, "broadcast")?;
            ensure!(
                matches!(previous, JournalStatus::Signed { .. }),
                "journal broadcast transition requires a signed transaction"
            );
            JournalStatus::Broadcast {
                transaction_hash: hash,
            }
        }
        WireEvent::OutcomeUnknown {
            transaction_hash,
            reason,
        } => {
            let hash = parse_hash(transaction_hash)?;
            ensure_hash_transition(previous, hash, "unknown outcome")?;
            ensure!(
                matches!(
                    previous,
                    JournalStatus::Signed { .. } | JournalStatus::Broadcast { .. }
                ),
                "journal unknown outcome requires a signed or broadcast transaction"
            );
            JournalStatus::OutcomeUnknown {
                transaction_hash: hash,
                reason: *reason,
            }
        }
        WireEvent::MinedSuccess {
            transaction_hash,
            block_number,
        } => mined_status(previous, transaction_hash, *block_number, true)?,
        WireEvent::MinedReverted {
            transaction_hash,
            block_number,
        } => mined_status(previous, transaction_hash, *block_number, false)?,
        WireEvent::CancelledBeforeSigning => {
            ensure!(
                matches!(previous, JournalStatus::IntentRecorded),
                "only an unsigned journal intent can be cancelled"
            );
            JournalStatus::CancelledBeforeSigning
        }
        WireEvent::IntentRecorded { .. } => unreachable!("intent handled before transition"),
    };
    operation.status = next;
    Ok(())
}

fn mined_status(
    previous: &JournalStatus,
    transaction_hash: &str,
    block_number: u64,
    success: bool,
) -> anyhow::Result<JournalStatus> {
    ensure!(block_number > 0, "journal mined block number is zero");
    let hash = parse_hash(transaction_hash)?;
    ensure_hash_transition(previous, hash, "mined")?;
    ensure!(
        matches!(
            previous,
            JournalStatus::Signed { .. }
                | JournalStatus::Broadcast { .. }
                | JournalStatus::OutcomeUnknown { .. }
        ),
        "journal mined transition requires a transaction hash"
    );
    Ok(if success {
        JournalStatus::MinedSuccess {
            transaction_hash: hash,
            block_number,
        }
    } else {
        JournalStatus::MinedReverted {
            transaction_hash: hash,
            block_number,
        }
    })
}

fn ensure_hash_transition(
    previous: &JournalStatus,
    hash: B256,
    transition: &str,
) -> anyhow::Result<()> {
    ensure!(
        previous.transaction_hash() == Some(hash),
        "journal {transition} transaction hash changed"
    );
    Ok(())
}

fn validate_identity(identity: &JournalOperationIdentity) -> anyhow::Result<()> {
    validate_identifier(
        "operation id",
        &identity.operation_id,
        MAX_OPERATION_ID_BYTES,
    )?;
    ensure!(identity.chain_id > 0, "journal chain id is zero");
    ensure!(identity.wallet != Address::ZERO, "journal wallet is zero");
    Ok(())
}

fn validate_intent(intent: &JournalIntent) -> anyhow::Result<()> {
    validate_identity(&intent.identity)?;
    validate_identifier("purpose", &intent.purpose, MAX_PURPOSE_BYTES)?;
    ensure!(intent.target != Address::ZERO, "journal target is zero");
    Ok(())
}

fn validate_identifier(name: &str, value: &str, maximum: usize) -> anyhow::Result<()> {
    ensure!(!value.is_empty(), "journal {name} is empty");
    ensure!(value.len() <= maximum, "journal {name} is too long");
    ensure!(
        value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':' | b'.')
        }),
        "journal {name} contains invalid characters"
    );
    Ok(())
}

fn parse_hash(value: &str) -> anyhow::Result<B256> {
    B256::from_str(value).context("journal transaction hash is invalid")
}

fn payload_checksum(payload: &WirePayload) -> anyhow::Result<String> {
    let encoded = serde_json::to_vec(payload).context("failed to encode journal checksum input")?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

fn unix_timestamp_ms() -> anyhow::Result<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?;
    u64::try_from(duration.as_millis()).context("Unix timestamp exceeds u64")
}

fn validate_permissions(file: &File) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        let mode = file
            .metadata()
            .context("failed to inspect transaction journal permissions")?
            .permissions()
            .mode();
        ensure!(
            mode & 0o077 == 0,
            "transaction journal permissions expose it to group or other users"
        );
    }
    Ok(())
}

fn sync_new_file_and_parent(file: &File, path: &Path) -> anyhow::Result<()> {
    file.sync_all()
        .context("failed to sync new transaction journal")?;
    #[cfg(unix)]
    {
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        File::open(parent)
            .context("failed to open transaction journal parent directory")?
            .sync_all()
            .context("failed to sync transaction journal parent directory")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, OpenOptions},
        io::Write,
        sync::atomic::{AtomicU64, Ordering},
    };

    use alloy_primitives::{Address, B256, U256, keccak256};

    use super::{
        JournalIntent, JournalOperationIdentity, JournalStatus, TransactionJournal,
        UnknownOutcomeReason,
    };

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    fn journal_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "poly-bot-wallet-journal-{}-{name}-{}.jsonl",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn identity() -> JournalOperationIdentity {
        JournalOperationIdentity {
            operation_id: "rebalance:usdc:42".to_owned(),
            chain_id: 480,
            wallet: Address::repeat_byte(0x11),
            nonce: 7,
        }
    }

    fn intent() -> JournalIntent {
        JournalIntent {
            identity: identity(),
            purpose: "rebalance_wallet_to_binance".to_owned(),
            target: Address::repeat_byte(0x22),
            native_value: U256::ZERO,
            calldata_hash: keccak256([1, 2, 3]),
        }
    }

    #[test]
    fn persists_and_recovers_the_full_transaction_lifecycle() {
        let path = journal_path("lifecycle");
        let tx_hash = B256::repeat_byte(0x33);
        {
            let mut journal = TransactionJournal::open(&path).unwrap();
            journal.record_intent(&intent()).unwrap();
            journal.record_signed(&identity(), tx_hash).unwrap();
            journal.record_broadcast(&identity(), tx_hash).unwrap();
            journal
                .record_mined(&identity(), tx_hash, 12_345, true)
                .unwrap();
        }

        let journal = TransactionJournal::open(&path).unwrap();
        let operation = journal.operation(&identity().operation_id).unwrap();
        assert_eq!(operation.intent, intent());
        assert_eq!(
            operation.status,
            JournalStatus::MinedSuccess {
                transaction_hash: tx_hash,
                block_number: 12_345,
            }
        );
        assert!(journal.unresolved_for(480, identity().wallet).is_empty());
        assert_eq!(
            journal.highest_consumed_nonce(480, identity().wallet),
            Some(7)
        );
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn recovers_unknown_outcome_and_never_stores_raw_transaction() {
        let path = journal_path("unknown");
        let tx_hash = B256::repeat_byte(0x44);
        let raw_secret_marker = "raw-signed-payload-must-not-appear";
        {
            let mut journal = TransactionJournal::open(&path).unwrap();
            journal.record_intent(&intent()).unwrap();
            journal.record_signed(&identity(), tx_hash).unwrap();
            journal
                .record_unknown_outcome(
                    &identity(),
                    tx_hash,
                    UnknownOutcomeReason::ConfirmationTimeout,
                )
                .unwrap();
        }

        let contents = fs::read_to_string(&path).unwrap();
        assert!(!contents.contains(raw_secret_marker));
        let journal = TransactionJournal::open(&path).unwrap();
        assert_eq!(journal.unresolved_for(480, identity().wallet).len(), 1);
        assert!(matches!(
            journal.operation(&identity().operation_id).unwrap().status,
            JournalStatus::OutcomeUnknown {
                reason: UnknownOutcomeReason::ConfirmationTimeout,
                ..
            }
        ));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn rejects_duplicate_intents_and_illegal_or_changed_hash_transitions() {
        let path = journal_path("transitions");
        let mut journal = TransactionJournal::open(&path).unwrap();
        journal.record_intent(&intent()).unwrap();
        assert!(journal.record_intent(&intent()).is_err());
        assert!(
            journal
                .record_broadcast(&identity(), B256::repeat_byte(0x11))
                .is_err()
        );
        journal
            .record_signed(&identity(), B256::repeat_byte(0x22))
            .unwrap();
        assert!(
            journal
                .record_broadcast(&identity(), B256::repeat_byte(0x23))
                .is_err()
        );
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn nonce_cannot_be_reused_unless_the_previous_intent_was_cancelled_unsigned() {
        let path = journal_path("nonce-owner");
        let mut journal = TransactionJournal::open(&path).unwrap();
        journal.record_intent(&intent()).unwrap();
        let mut another = intent();
        another.identity.operation_id = "rebalance:usdc:43".to_owned();
        assert!(journal.record_intent(&another).is_err());

        journal
            .record_cancelled_before_signing(&identity())
            .unwrap();
        journal.record_intent(&another).unwrap();
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn permits_cancellation_only_before_signing() {
        let path = journal_path("cancel");
        let mut journal = TransactionJournal::open(&path).unwrap();
        journal.record_intent(&intent()).unwrap();
        journal
            .record_cancelled_before_signing(&identity())
            .unwrap();
        assert_eq!(
            journal.operation(&identity().operation_id).unwrap().status,
            JournalStatus::CancelledBeforeSigning
        );
        assert!(
            journal
                .record_signed(&identity(), B256::repeat_byte(0x22))
                .is_err()
        );
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn fails_closed_on_partial_or_corrupted_records() {
        let partial_path = journal_path("partial");
        drop(TransactionJournal::open(&partial_path).unwrap());
        let mut partial = OpenOptions::new().append(true).open(&partial_path).unwrap();
        partial.write_all(b"{\"partial\":true}").unwrap();
        partial.sync_all().unwrap();
        drop(partial);
        assert!(TransactionJournal::open(&partial_path).is_err());
        fs::remove_file(partial_path).unwrap();

        let corrupt_path = journal_path("corrupt");
        {
            let mut journal = TransactionJournal::open(&corrupt_path).unwrap();
            journal.record_intent(&intent()).unwrap();
        }
        let contents = fs::read_to_string(&corrupt_path).unwrap();
        fs::write(&corrupt_path, contents.replace("rebalance", "rebxlance")).unwrap();
        assert!(TransactionJournal::open(&corrupt_path).is_err());
        fs::remove_file(corrupt_path).unwrap();
    }

    #[test]
    fn prevents_a_second_process_owner_from_opening_the_same_journal() {
        let path = journal_path("exclusive");
        let journal = TransactionJournal::open(&path).unwrap();
        assert!(TransactionJournal::open(&path).is_err());
        drop(journal);
        assert!(TransactionJournal::open(&path).is_ok());
        fs::remove_file(path).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn rejects_an_existing_journal_with_insecure_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let path = journal_path("permissions");
        drop(TransactionJournal::open(&path).unwrap());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(TransactionJournal::open(&path).is_err());
        fs::remove_file(path).unwrap();
    }
}

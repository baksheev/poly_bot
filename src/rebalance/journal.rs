use std::{
    fs::{File, OpenOptions, symlink_metadata},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

use alloy_primitives::{Address, U256};
use anyhow::{Context, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const VERSION: u16 = 1;
const MAX_LINE_BYTES: usize = 32 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebalanceCanaryIntent {
    pub operation_id: String,
    pub coin: String,
    pub network: String,
    pub amount_base_units: U256,
    pub destination: Address,
    pub withdraw_order_id: String,
    pub wallet_balance_before: U256,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RebalanceCanaryStatus {
    IntentRecorded,
    Submitted {
        travel_rule_id: i64,
    },
    WithdrawalObserved {
        withdrawal_id: String,
        transaction_id: String,
        status: u8,
    },
    Completed {
        transaction_id: String,
        wallet_balance_after: U256,
    },
    Failed {
        status: u8,
    },
}

impl RebalanceCanaryStatus {
    pub fn terminal(&self) -> bool {
        matches!(self, Self::Completed { .. } | Self::Failed { .. })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebalanceCanaryOperation {
    pub intent: RebalanceCanaryIntent,
    pub status: RebalanceCanaryStatus,
}

pub struct RebalanceCanaryJournal {
    path: PathBuf,
    file: File,
    operation: Option<RebalanceCanaryOperation>,
    next_sequence: u64,
    poisoned: bool,
}

impl std::fmt::Debug for RebalanceCanaryJournal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RebalanceCanaryJournal")
            .field("path", &self.path)
            .field("has_operation", &self.operation.is_some())
            .field("next_sequence", &self.next_sequence)
            .field("poisoned", &self.poisoned)
            .finish()
    }
}

impl RebalanceCanaryJournal {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        ensure!(
            !path.as_os_str().is_empty(),
            "rebalance journal path is empty"
        );
        let existed = path.exists();
        if existed {
            let metadata = symlink_metadata(&path).with_context(|| {
                format!("failed to inspect rebalance journal {}", path.display())
            })?;
            ensure!(
                !metadata.file_type().is_symlink(),
                "rebalance journal must not be a symbolic link"
            );
            ensure!(metadata.is_file(), "rebalance journal path is not a file");
        } else {
            let parent = path.parent().unwrap_or_else(|| Path::new("."));
            ensure!(
                parent.is_dir(),
                "rebalance journal parent directory does not exist"
            );
        }

        let mut options = OpenOptions::new();
        options.create(true).read(true).append(true);
        #[cfg(unix)]
        options.mode(0o600);
        let file = options
            .open(&path)
            .with_context(|| format!("failed to open rebalance journal {}", path.display()))?;
        validate_permissions(&file)?;
        file.try_lock()
            .context("rebalance journal is already locked by another process")?;
        if !existed {
            file.sync_all()
                .context("failed to sync new rebalance journal")?;
            sync_parent(&path)?;
        }

        let mut operation = None;
        let mut expected_sequence = 0_u64;
        let mut reader = BufReader::new(
            file.try_clone()
                .context("failed to clone rebalance journal handle")?,
        );
        loop {
            let mut line = Vec::new();
            let bytes = reader
                .read_until(b'\n', &mut line)
                .context("failed to read rebalance journal")?;
            if bytes == 0 {
                break;
            }
            ensure!(
                line.len() <= MAX_LINE_BYTES,
                "rebalance journal record is too large"
            );
            ensure!(
                line.last() == Some(&b'\n'),
                "rebalance journal ends with a partial record"
            );
            line.pop();
            let record: WireRecord =
                serde_json::from_slice(&line).context("rebalance journal contains invalid JSON")?;
            record.validate_checksum()?;
            ensure!(
                record.payload.version == VERSION,
                "unsupported rebalance journal version"
            );
            ensure!(
                record.payload.sequence == expected_sequence,
                "rebalance journal sequence mismatch"
            );
            apply_event(&mut operation, &record.payload.event)?;
            expected_sequence = expected_sequence
                .checked_add(1)
                .context("rebalance journal sequence overflow")?;
        }
        Ok(Self {
            path,
            file,
            operation,
            next_sequence: expected_sequence,
            poisoned: false,
        })
    }

    pub fn operation(&self) -> Option<&RebalanceCanaryOperation> {
        self.operation.as_ref()
    }

    pub fn record_intent(&mut self, intent: &RebalanceCanaryIntent) -> anyhow::Result<()> {
        validate_intent(intent)?;
        ensure!(
            self.operation.is_none(),
            "rebalance canary operation already exists"
        );
        self.append(WireEvent::Intent {
            operation_id: intent.operation_id.clone(),
            coin: intent.coin.clone(),
            network: intent.network.clone(),
            amount_base_units: intent.amount_base_units.to_string(),
            destination: format!("{:#x}", intent.destination),
            withdraw_order_id: intent.withdraw_order_id.clone(),
            wallet_balance_before: intent.wallet_balance_before.to_string(),
        })
    }

    pub fn record_submitted(&mut self, travel_rule_id: i64) -> anyhow::Result<()> {
        ensure!(travel_rule_id > 0, "Travel Rule id must be positive");
        let operation_id = self.operation_id()?.to_owned();
        self.append(WireEvent::Submitted {
            operation_id,
            travel_rule_id,
        })
    }

    pub fn record_withdrawal(
        &mut self,
        withdrawal_id: &str,
        transaction_id: &str,
        status: u8,
    ) -> anyhow::Result<()> {
        ensure!(!withdrawal_id.is_empty(), "Binance withdrawal id is empty");
        let operation_id = self.operation_id()?.to_owned();
        self.append(WireEvent::WithdrawalObserved {
            operation_id,
            withdrawal_id: withdrawal_id.to_owned(),
            transaction_id: transaction_id.to_owned(),
            status,
        })
    }

    pub fn record_completed(
        &mut self,
        transaction_id: &str,
        wallet_balance_after: U256,
    ) -> anyhow::Result<()> {
        ensure!(
            !transaction_id.is_empty(),
            "completed withdrawal transaction id is empty"
        );
        let operation_id = self.operation_id()?.to_owned();
        self.append(WireEvent::Completed {
            operation_id,
            transaction_id: transaction_id.to_owned(),
            wallet_balance_after: wallet_balance_after.to_string(),
        })
    }

    pub fn record_failed(&mut self, status: u8) -> anyhow::Result<()> {
        ensure!(
            matches!(status, 1 | 3 | 5),
            "withdrawal failure status is not terminal"
        );
        let operation_id = self.operation_id()?.to_owned();
        self.append(WireEvent::Failed {
            operation_id,
            status,
        })
    }

    fn operation_id(&self) -> anyhow::Result<&str> {
        self.operation
            .as_ref()
            .map(|operation| operation.intent.operation_id.as_str())
            .context("rebalance canary intent is missing")
    }

    fn append(&mut self, event: WireEvent) -> anyhow::Result<()> {
        ensure!(
            !self.poisoned,
            "rebalance journal is poisoned after a failed append"
        );
        let payload = WirePayload {
            version: VERSION,
            sequence: self.next_sequence,
            timestamp_ms: unix_timestamp_ms()?,
            event,
        };
        let record = WireRecord::new(payload)?;
        let mut encoded =
            serde_json::to_vec(&record).context("failed to encode rebalance journal record")?;
        ensure!(
            encoded.len() < MAX_LINE_BYTES,
            "rebalance journal record is too large"
        );
        encoded.push(b'\n');
        if let Err(error) = self
            .file
            .write_all(&encoded)
            .and_then(|_| self.file.sync_data())
        {
            self.poisoned = true;
            return Err(error).context("failed to durably append rebalance journal record");
        }
        apply_event(&mut self.operation, &record.payload.event)?;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .context("rebalance journal sequence overflow")?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct WireRecord {
    payload: WirePayload,
    checksum: String,
}

impl WireRecord {
    fn new(payload: WirePayload) -> anyhow::Result<Self> {
        let checksum = payload_checksum(&payload)?;
        Ok(Self { payload, checksum })
    }

    fn validate_checksum(&self) -> anyhow::Result<()> {
        ensure!(
            self.checksum == payload_checksum(&self.payload)?,
            "rebalance journal checksum mismatch"
        );
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct WirePayload {
    version: u16,
    sequence: u64,
    timestamp_ms: u64,
    event: WireEvent,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WireEvent {
    Intent {
        operation_id: String,
        coin: String,
        network: String,
        amount_base_units: String,
        destination: String,
        withdraw_order_id: String,
        wallet_balance_before: String,
    },
    Submitted {
        operation_id: String,
        travel_rule_id: i64,
    },
    WithdrawalObserved {
        operation_id: String,
        withdrawal_id: String,
        transaction_id: String,
        status: u8,
    },
    Completed {
        operation_id: String,
        transaction_id: String,
        wallet_balance_after: String,
    },
    Failed {
        operation_id: String,
        status: u8,
    },
}

fn apply_event(
    operation: &mut Option<RebalanceCanaryOperation>,
    event: &WireEvent,
) -> anyhow::Result<()> {
    match event {
        WireEvent::Intent {
            operation_id,
            coin,
            network,
            amount_base_units,
            destination,
            withdraw_order_id,
            wallet_balance_before,
        } => {
            ensure!(operation.is_none(), "duplicate rebalance canary intent");
            let intent = RebalanceCanaryIntent {
                operation_id: operation_id.clone(),
                coin: coin.clone(),
                network: network.clone(),
                amount_base_units: U256::from_str(amount_base_units)
                    .context("invalid rebalance intent amount")?,
                destination: Address::from_str(destination)
                    .context("invalid rebalance intent destination")?,
                withdraw_order_id: withdraw_order_id.clone(),
                wallet_balance_before: U256::from_str(wallet_balance_before)
                    .context("invalid rebalance intent wallet balance")?,
            };
            validate_intent(&intent)?;
            *operation = Some(RebalanceCanaryOperation {
                intent,
                status: RebalanceCanaryStatus::IntentRecorded,
            });
        }
        WireEvent::Submitted {
            operation_id,
            travel_rule_id,
        } => {
            let current = matching_operation(operation, operation_id)?;
            ensure!(
                matches!(current.status, RebalanceCanaryStatus::IntentRecorded),
                "illegal rebalance submitted transition"
            );
            ensure!(*travel_rule_id > 0, "invalid journaled Travel Rule id");
            current.status = RebalanceCanaryStatus::Submitted {
                travel_rule_id: *travel_rule_id,
            };
        }
        WireEvent::WithdrawalObserved {
            operation_id,
            withdrawal_id,
            transaction_id,
            status,
        } => {
            let current = matching_operation(operation, operation_id)?;
            ensure!(
                matches!(
                    current.status,
                    RebalanceCanaryStatus::IntentRecorded
                        | RebalanceCanaryStatus::Submitted { .. }
                        | RebalanceCanaryStatus::WithdrawalObserved { .. }
                ),
                "illegal rebalance withdrawal observation transition"
            );
            ensure!(
                !withdrawal_id.is_empty(),
                "journaled withdrawal id is empty"
            );
            current.status = RebalanceCanaryStatus::WithdrawalObserved {
                withdrawal_id: withdrawal_id.clone(),
                transaction_id: transaction_id.clone(),
                status: *status,
            };
        }
        WireEvent::Completed {
            operation_id,
            transaction_id,
            wallet_balance_after,
        } => {
            let current = matching_operation(operation, operation_id)?;
            let observed_tx = match &current.status {
                RebalanceCanaryStatus::WithdrawalObserved {
                    transaction_id,
                    status: 6,
                    ..
                } => transaction_id,
                _ => anyhow::bail!("rebalance completion lacks a completed withdrawal"),
            };
            ensure!(
                observed_tx == transaction_id,
                "rebalance completion transaction mismatch"
            );
            let wallet_balance_after =
                U256::from_str(wallet_balance_after).context("invalid completed wallet balance")?;
            ensure!(
                wallet_balance_after > current.intent.wallet_balance_before,
                "completed wallet balance did not increase"
            );
            current.status = RebalanceCanaryStatus::Completed {
                transaction_id: transaction_id.clone(),
                wallet_balance_after,
            };
        }
        WireEvent::Failed {
            operation_id,
            status,
        } => {
            let current = matching_operation(operation, operation_id)?;
            ensure!(
                matches!(
                    current.status,
                    RebalanceCanaryStatus::WithdrawalObserved { .. }
                ),
                "rebalance failure lacks a withdrawal observation"
            );
            ensure!(
                matches!(status, 1 | 3 | 5),
                "invalid terminal withdrawal failure status"
            );
            current.status = RebalanceCanaryStatus::Failed { status: *status };
        }
    }
    Ok(())
}

fn matching_operation<'a>(
    operation: &'a mut Option<RebalanceCanaryOperation>,
    operation_id: &str,
) -> anyhow::Result<&'a mut RebalanceCanaryOperation> {
    let current = operation
        .as_mut()
        .context("rebalance event precedes intent")?;
    ensure!(
        current.intent.operation_id == operation_id,
        "rebalance operation id changed"
    );
    Ok(current)
}

fn validate_intent(intent: &RebalanceCanaryIntent) -> anyhow::Result<()> {
    ensure!(
        !intent.operation_id.is_empty() && intent.operation_id.len() <= 64,
        "rebalance operation id is invalid"
    );
    ensure!(intent.coin == "WLD", "rebalance canary only permits WLD");
    ensure!(
        intent.network == "WLD",
        "rebalance canary only permits direct WLD network"
    );
    ensure!(
        !intent.amount_base_units.is_zero(),
        "rebalance canary amount is zero"
    );
    ensure!(
        intent.destination != Address::ZERO,
        "rebalance canary destination is zero"
    );
    ensure!(
        !intent.withdraw_order_id.is_empty()
            && intent.withdraw_order_id.len() <= 64
            && intent
                .withdraw_order_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric()),
        "rebalance withdrawal client id is invalid"
    );
    Ok(())
}

fn payload_checksum(payload: &WirePayload) -> anyhow::Result<String> {
    let encoded =
        serde_json::to_vec(payload).context("failed to encode rebalance checksum payload")?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

fn unix_timestamp_ms() -> anyhow::Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_millis()
        .try_into()
        .context("rebalance journal timestamp overflow")
}

#[cfg(unix)]
fn validate_permissions(file: &File) -> anyhow::Result<()> {
    let mode = file
        .metadata()
        .context("failed to inspect rebalance journal permissions")?
        .permissions()
        .mode()
        & 0o777;
    ensure!(
        mode & 0o077 == 0,
        "rebalance journal is group/world accessible"
    );
    Ok(())
}

#[cfg(not(unix))]
fn validate_permissions(_file: &File) -> anyhow::Result<()> {
    Ok(())
}

fn sync_parent(path: &Path) -> anyhow::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .context("failed to sync rebalance journal parent directory")
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::Write,
        sync::atomic::{AtomicU64, Ordering},
    };

    use alloy_primitives::{Address, U256};

    use super::{RebalanceCanaryIntent, RebalanceCanaryJournal, RebalanceCanaryStatus};

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    fn path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "poly-bot-rebalance-{name}-{}-{}.jsonl",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn intent() -> RebalanceCanaryIntent {
        RebalanceCanaryIntent {
            operation_id: "directwldcanaryv1".to_owned(),
            coin: "WLD".to_owned(),
            network: "WLD".to_owned(),
            amount_base_units: U256::from(10).pow(U256::from(18)),
            destination: Address::repeat_byte(0x11),
            withdraw_order_id: "rustrebwldcanary1".to_owned(),
            wallet_balance_before: U256::ZERO,
        }
    }

    #[test]
    fn persists_complete_canary_lifecycle() {
        let path = path("lifecycle");
        {
            let mut journal = RebalanceCanaryJournal::open(&path).unwrap();
            journal.record_intent(&intent()).unwrap();
            journal.record_submitted(42).unwrap();
            journal.record_withdrawal("uuid", "0xabc", 4).unwrap();
            journal.record_withdrawal("uuid", "0xabc", 6).unwrap();
            journal
                .record_completed("0xabc", U256::from(10).pow(U256::from(18)))
                .unwrap();
        }
        let journal = RebalanceCanaryJournal::open(&path).unwrap();
        assert!(matches!(
            journal.operation().unwrap().status,
            RebalanceCanaryStatus::Completed { .. }
        ));
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn locks_single_owner_and_rejects_corruption() {
        let path = path("lock");
        let mut journal = RebalanceCanaryJournal::open(&path).unwrap();
        journal.record_intent(&intent()).unwrap();
        assert!(RebalanceCanaryJournal::open(&path).is_err());
        drop(journal);
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"partial")
            .unwrap();
        assert!(RebalanceCanaryJournal::open(&path).is_err());
        fs::remove_file(path).unwrap();
    }
}

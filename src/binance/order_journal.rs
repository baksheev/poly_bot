use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions, symlink_metadata},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

use anyhow::{Context, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::binance::ws_api::OrderResult;

const VERSION: u16 = 1;
const MAX_LINE_BYTES: usize = 32 * 1024;
const MAX_REASON_BYTES: usize = 1_024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BinanceOrderIntent {
    pub operation_id: String,
    pub client_order_id: String,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub quantity: Option<String>,
    pub quote_order_quantity: Option<String>,
    pub limit_price: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum BinanceOrderProgress {
    IntentRecorded,
    Submitted {
        order_id: u64,
        status: String,
        executed_quantity: String,
        cumulative_quote_quantity: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        order: Option<OrderResult>,
    },
    OutcomeUnknown {
        reason: String,
    },
    Rejected {
        status: u16,
        code: i64,
        reason: String,
    },
    Terminal {
        order_id: u64,
        status: String,
        executed_quantity: String,
        cumulative_quote_quantity: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        order: Option<OrderResult>,
    },
}

impl BinanceOrderProgress {
    pub fn terminal(&self) -> bool {
        matches!(self, Self::Rejected { .. } | Self::Terminal { .. })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BinanceOrderOperation {
    pub intent: BinanceOrderIntent,
    pub progress: BinanceOrderProgress,
}

pub struct BinanceOrderJournal {
    path: PathBuf,
    file: File,
    operations: BTreeMap<String, BinanceOrderOperation>,
    next_sequence: u64,
    poisoned: bool,
}

impl std::fmt::Debug for BinanceOrderJournal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BinanceOrderJournal")
            .field("path", &self.path)
            .field("operations", &self.operations.len())
            .field("next_sequence", &self.next_sequence)
            .field("poisoned", &self.poisoned)
            .finish()
    }
}

impl BinanceOrderJournal {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        ensure!(
            !path.as_os_str().is_empty(),
            "Binance order journal path is empty"
        );
        let existed = path.exists();
        if existed {
            let metadata = symlink_metadata(&path).with_context(|| {
                format!("failed to inspect Binance order journal {}", path.display())
            })?;
            ensure!(
                !metadata.file_type().is_symlink(),
                "Binance order journal must not be a symbolic link"
            );
            ensure!(
                metadata.is_file(),
                "Binance order journal path is not a file"
            );
        } else {
            let parent = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            ensure!(
                parent.is_dir(),
                "Binance order journal parent directory does not exist"
            );
        }

        let mut options = OpenOptions::new();
        options.create(true).read(true).append(true);
        #[cfg(unix)]
        options.mode(0o600);
        let file = options
            .open(&path)
            .with_context(|| format!("failed to open Binance order journal {}", path.display()))?;
        validate_permissions(&file)?;
        file.try_lock()
            .context("Binance order journal is already locked by another process")?;
        if !existed {
            file.sync_all()
                .context("failed to sync new Binance order journal")?;
            sync_parent(&path)?;
        }

        let mut operations = BTreeMap::new();
        let mut expected_sequence = 0_u64;
        let mut reader = BufReader::new(
            file.try_clone()
                .context("failed to clone Binance order journal handle")?,
        );
        loop {
            let mut line = Vec::new();
            let bytes = reader
                .read_until(b'\n', &mut line)
                .context("failed to read Binance order journal")?;
            if bytes == 0 {
                break;
            }
            ensure!(
                line.len() <= MAX_LINE_BYTES,
                "Binance order journal record is too large"
            );
            ensure!(
                line.last() == Some(&b'\n'),
                "Binance order journal ends with a partial record"
            );
            line.pop();
            let record: WireRecord = serde_json::from_slice(&line)
                .context("Binance order journal contains invalid JSON")?;
            record.validate_checksum()?;
            ensure!(
                record.payload.version == VERSION,
                "unsupported Binance order journal version"
            );
            ensure!(
                record.payload.sequence == expected_sequence,
                "Binance order journal sequence mismatch"
            );
            apply_snapshot(&mut operations, &record.payload.operation)?;
            expected_sequence = expected_sequence
                .checked_add(1)
                .context("Binance order journal sequence overflow")?;
        }

        Ok(Self {
            path,
            file,
            operations,
            next_sequence: expected_sequence,
            poisoned: false,
        })
    }

    pub fn operations(&self) -> &BTreeMap<String, BinanceOrderOperation> {
        &self.operations
    }

    pub fn active_operations(&self) -> Vec<&BinanceOrderOperation> {
        self.operations
            .values()
            .filter(|operation| !operation.progress.terminal())
            .collect()
    }

    pub fn record_intent(&mut self, intent: BinanceOrderIntent) -> anyhow::Result<()> {
        ensure!(
            !self.operations.contains_key(&intent.client_order_id),
            "Binance client order id already exists in journal"
        );
        self.append(BinanceOrderOperation {
            intent,
            progress: BinanceOrderProgress::IntentRecorded,
        })
    }

    pub fn advance(
        &mut self,
        client_order_id: &str,
        progress: BinanceOrderProgress,
    ) -> anyhow::Result<()> {
        let current = self
            .operations
            .get(client_order_id)
            .with_context(|| format!("unknown Binance order {client_order_id}"))?;
        validate_transition(&current.progress, &progress)?;
        self.append(BinanceOrderOperation {
            intent: current.intent.clone(),
            progress,
        })
    }

    fn append(&mut self, operation: BinanceOrderOperation) -> anyhow::Result<()> {
        ensure!(!self.poisoned, "Binance order journal is poisoned");
        validate_operation(&operation)?;
        let payload = WirePayload {
            version: VERSION,
            sequence: self.next_sequence,
            recorded_at_unix_ms: unix_timestamp_ms()?,
            operation,
        };
        let mut next_operations = self.operations.clone();
        apply_snapshot(&mut next_operations, &payload.operation)?;
        let record = WireRecord::new(payload)?;
        let mut encoded =
            serde_json::to_vec(&record).context("failed to encode Binance order journal")?;
        ensure!(
            encoded.len() < MAX_LINE_BYTES,
            "Binance order journal record is too large"
        );
        encoded.push(b'\n');
        if let Err(error) = self
            .file
            .write_all(&encoded)
            .and_then(|()| self.file.sync_data())
        {
            self.poisoned = true;
            return Err(error).context("failed to durably append Binance order journal record");
        }
        self.operations = next_operations;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .context("Binance order journal sequence overflow")?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WirePayload {
    version: u16,
    sequence: u64,
    recorded_at_unix_ms: u64,
    operation: BinanceOrderOperation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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

    fn validate_checksum(&self) -> anyhow::Result<()> {
        ensure!(
            self.checksum_sha256 == checksum(&self.payload)?,
            "Binance order journal checksum mismatch"
        );
        Ok(())
    }
}

fn apply_snapshot(
    operations: &mut BTreeMap<String, BinanceOrderOperation>,
    operation: &BinanceOrderOperation,
) -> anyhow::Result<()> {
    validate_operation(operation)?;
    if let Some(previous) = operations.get(&operation.intent.client_order_id) {
        ensure!(
            previous.intent == operation.intent,
            "Binance order journal intent changed"
        );
        validate_transition(&previous.progress, &operation.progress)?;
    } else {
        ensure!(
            matches!(operation.progress, BinanceOrderProgress::IntentRecorded),
            "first Binance order journal state must be intent_recorded"
        );
    }
    operations.insert(operation.intent.client_order_id.clone(), operation.clone());
    Ok(())
}

fn validate_transition(
    previous: &BinanceOrderProgress,
    next: &BinanceOrderProgress,
) -> anyhow::Result<()> {
    ensure!(!previous.terminal(), "Binance order is already terminal");
    ensure!(
        !matches!(next, BinanceOrderProgress::IntentRecorded),
        "Binance order intent cannot be recorded twice"
    );
    ensure!(
        !matches!(previous, BinanceOrderProgress::OutcomeUnknown { .. })
            || matches!(
                next,
                BinanceOrderProgress::Submitted { .. }
                    | BinanceOrderProgress::Terminal { .. }
                    | BinanceOrderProgress::OutcomeUnknown { .. }
            ),
        "an unknown Binance outcome can only be reconciled from the exchange"
    );
    Ok(())
}

fn validate_operation(operation: &BinanceOrderOperation) -> anyhow::Result<()> {
    let intent = &operation.intent;
    validate_text("operation id", &intent.operation_id, 96)?;
    validate_text("client order id", &intent.client_order_id, 36)?;
    ensure!(
        intent.client_order_id.starts_with("rustval")
            || intent.client_order_id.starts_with("rustarb"),
        "Binance client id must use a Rust-owned namespace"
    );
    validate_text("symbol", &intent.symbol, 24)?;
    ensure!(
        matches!(intent.side.as_str(), "BUY" | "SELL"),
        "invalid side"
    );
    ensure!(
        matches!(intent.order_type.as_str(), "MARKET" | "LIMIT"),
        "invalid order type"
    );
    ensure!(
        intent.quantity.is_some() ^ intent.quote_order_quantity.is_some(),
        "exactly one Binance quantity field is required"
    );
    if intent.order_type == "LIMIT" {
        ensure!(intent.limit_price.is_some(), "LIMIT order requires a price");
        ensure!(
            intent.quantity.is_some(),
            "LIMIT order requires base quantity"
        );
    } else {
        ensure!(
            intent.limit_price.is_none(),
            "MARKET order cannot have a price"
        );
    }
    match &operation.progress {
        BinanceOrderProgress::OutcomeUnknown { reason }
        | BinanceOrderProgress::Rejected { reason, .. } => {
            ensure!(
                !reason.is_empty() && reason.len() <= MAX_REASON_BYTES,
                "Binance journal reason is invalid"
            );
        }
        BinanceOrderProgress::Submitted { status, .. }
        | BinanceOrderProgress::Terminal { status, .. } => {
            validate_text("order status", status, 24)?;
        }
        BinanceOrderProgress::IntentRecorded => {}
    }
    Ok(())
}

fn validate_text(name: &str, value: &str, maximum: usize) -> anyhow::Result<()> {
    ensure!(!value.is_empty(), "Binance journal {name} is empty");
    ensure!(value.len() <= maximum, "Binance journal {name} is too long");
    ensure!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric()
                || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')),
        "Binance journal {name} contains unsupported characters"
    );
    Ok(())
}

fn checksum(payload: &WirePayload) -> anyhow::Result<String> {
    let encoded = serde_json::to_vec(payload).context("failed to encode Binance checksum input")?;
    let digest = Sha256::digest(encoded);
    let mut result = String::with_capacity(64);
    use std::fmt::Write as _;
    for byte in digest {
        write!(&mut result, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(result)
}

fn unix_timestamp_ms() -> anyhow::Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time is before Unix epoch")?
        .as_millis()
        .try_into()
        .context("Unix timestamp does not fit u64")
}

#[cfg(unix)]
fn validate_permissions(file: &File) -> anyhow::Result<()> {
    let mode = file
        .metadata()
        .context("failed to inspect Binance order journal permissions")?
        .permissions()
        .mode();
    ensure!(
        mode & 0o077 == 0,
        "Binance order journal is group/world accessible"
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
    File::open(parent)
        .context("failed to open Binance order journal parent")?
        .sync_all()
        .context("failed to sync Binance order journal parent")
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{BinanceOrderIntent, BinanceOrderJournal, BinanceOrderProgress};

    fn path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "poly-bot-binance-order-{name}-{}-{}.jsonl",
            std::process::id(),
            std::thread::current().name().unwrap_or("thread")
        ))
    }

    fn intent() -> BinanceOrderIntent {
        BinanceOrderIntent {
            operation_id: "rustval-limit-buy".to_owned(),
            client_order_id: "rustval123LB".to_owned(),
            symbol: "WLDUSDC".to_owned(),
            side: "BUY".to_owned(),
            order_type: "LIMIT".to_owned(),
            quantity: Some("26.1".to_owned()),
            quote_order_quantity: None,
            limit_price: Some("0.382".to_owned()),
        }
    }

    #[test]
    fn persists_terminal_order_lifecycle() {
        let path = path("terminal");
        let _ = fs::remove_file(&path);
        {
            let mut journal = BinanceOrderJournal::open(&path).unwrap();
            journal.record_intent(intent()).unwrap();
            journal
                .advance(
                    "rustval123LB",
                    BinanceOrderProgress::Terminal {
                        order_id: 42,
                        status: "FILLED".to_owned(),
                        executed_quantity: "26.1".to_owned(),
                        cumulative_quote_quantity: "9.9702".to_owned(),
                        order: None,
                    },
                )
                .unwrap();
        }
        let journal = BinanceOrderJournal::open(&path).unwrap();
        assert!(journal.active_operations().is_empty());
        assert!(matches!(
            journal.operations()["rustval123LB"].progress,
            BinanceOrderProgress::Terminal { .. }
        ));
        drop(journal);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn unknown_outcome_remains_active_and_locks_the_file() {
        let path = path("unknown");
        let _ = fs::remove_file(&path);
        let mut journal = BinanceOrderJournal::open(&path).unwrap();
        journal.record_intent(intent()).unwrap();
        journal
            .advance(
                "rustval123LB",
                BinanceOrderProgress::OutcomeUnknown {
                    reason: "response timed out".to_owned(),
                },
            )
            .unwrap();
        assert_eq!(journal.active_operations().len(), 1);
        assert!(BinanceOrderJournal::open(&path).is_err());
        drop(journal);
        fs::remove_file(path).unwrap();
    }
}

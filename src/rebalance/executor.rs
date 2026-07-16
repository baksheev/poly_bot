use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions, symlink_metadata},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

use alloy_primitives::{Address, B256, U256, keccak256};
use anyhow::{Context, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{Direction, Location, PendingTransfer, RebalanceAction, Route};

const VERSION: u16 = 1;
const MAX_LINE_BYTES: usize = 64 * 1024;
const MAX_REASON_BYTES: usize = 1_024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebalanceExecutionRequest {
    pub token_symbol: String,
    pub token_decimals: u8,
    pub token_contract: Address,
    pub wallet_owner: Address,
    pub action: RebalanceAction,
    pub binance_balance_before: U256,
    pub wallet_balance_before: U256,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RebalanceExecutionIntent {
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
    BinanceWithdrawalSubmitted {
        travel_rule_id: i64,
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
    Failed {
        reason: String,
    },
}

impl RebalanceExecutionProgress {
    pub fn terminal(&self) -> bool {
        matches!(self, Self::Completed { .. } | Self::Failed { .. })
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

        let mut operations = BTreeMap::new();
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
            let record: WireRecord = serde_json::from_slice(&line)
                .context("rebalance executor journal contains invalid JSON")?;
            record.validate_checksum()?;
            ensure!(
                record.payload.version == VERSION,
                "unsupported rebalance executor journal version"
            );
            ensure!(
                record.payload.sequence == expected_sequence,
                "rebalance executor journal sequence mismatch"
            );
            apply_snapshot(&mut operations, &record.payload.operation)?;
            expected_sequence = expected_sequence
                .checked_add(1)
                .context("rebalance executor journal sequence overflow")?;
        }

        Ok(Self {
            path,
            file,
            operations,
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
        validate_transition(&current.intent, &current.progress, &progress)?;
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
        apply_snapshot(&mut next_operations, &payload.operation)?;
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
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .context("rebalance executor journal sequence overflow")?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WirePayload {
    version: u16,
    sequence: u64,
    recorded_at_unix_ms: u64,
    operation: RebalanceExecutionOperation,
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
            "rebalance executor journal checksum mismatch"
        );
        Ok(())
    }
}

fn apply_snapshot(
    operations: &mut BTreeMap<String, RebalanceExecutionOperation>,
    operation: &RebalanceExecutionOperation,
) -> anyhow::Result<()> {
    validate_operation(operation)?;
    match operations.get(&operation.intent.operation_id) {
        Some(previous) => {
            ensure!(
                previous.intent == operation.intent,
                "rebalance operation intent changed"
            );
            validate_transition(&operation.intent, &previous.progress, &operation.progress)?;
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
    if let RebalanceExecutionProgress::Failed { reason } = &operation.progress {
        ensure!(
            !reason.is_empty() && reason.len() <= MAX_REASON_BYTES,
            "rebalance failure reason is invalid"
        );
    }
    Ok(())
}

#[allow(clippy::match_like_matches_macro)]
fn validate_transition(
    intent: &RebalanceExecutionIntent,
    previous: &RebalanceExecutionProgress,
    next: &RebalanceExecutionProgress,
) -> anyhow::Result<()> {
    ensure!(
        !previous.terminal(),
        "rebalance operation is already terminal"
    );
    if matches!(next, RebalanceExecutionProgress::Failed { .. }) {
        return Ok(());
    }
    use RebalanceExecutionProgress as P;
    let allowed = match (&intent.route, intent.direction, previous, next) {
        (
            Route::Direct { .. },
            Direction::BinanceToWallet,
            P::IntentRecorded,
            P::BinanceWithdrawalSubmitted { .. },
        ) => true,
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
            P::BinanceCredited { .. },
            P::Completed { .. },
        ) => true,
        (
            Route::Across { .. },
            Direction::BinanceToWallet,
            P::IntentRecorded,
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
        P::BinanceWithdrawalSubmitted { travel_rule_id, .. } => {
            ensure!(*travel_rule_id > 0, "rebalance Travel Rule id is invalid");
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
        P::ApprovalMined { chain_id, .. } | P::DepositTransferMined { chain_id, .. } => {
            ensure!(*chain_id > 0, "rebalance transaction chain id is zero")
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
        P::IntentRecorded | P::Failed { .. } => {}
    }
    Ok(())
}

fn request_fingerprint(request: &RebalanceExecutionRequest) -> anyhow::Result<String> {
    let encoded = serde_json::to_vec(&serde_json::json!({
        "token": request.token_symbol,
        "decimals": request.token_decimals,
        "contract": format!("{:#x}", request.token_contract),
        "wallet": format!("{:#x}", request.wallet_owner),
        "direction": request.action.direction,
        "route": request.action.route,
        "amount": request.action.amount.to_string(),
        "binance_before": request.binance_balance_before.to_string(),
        "wallet_before": request.wallet_balance_before.to_string(),
    }))?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

fn checksum(payload: &WirePayload) -> anyhow::Result<String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(payload)?)
    ))
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
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use alloy_primitives::{Address, B256, U256, keccak256};

    use super::{RebalanceExecutionJournal, RebalanceExecutionProgress, RebalanceExecutionRequest};
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
        RebalanceExecutionRequest {
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
        }
    }

    fn across() -> Route {
        Route::Across {
            binance_network: "OPTIMISM".to_owned(),
            bridge_chain_id: 10,
            wallet_chain_id: 480,
        }
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

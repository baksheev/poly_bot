use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use alloy_primitives::{Address, B256, U256, keccak256};
use anyhow::{Context, bail, ensure};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    across::{
        AcrossClient, AcrossQuoteRequest, OPTIMISM_CHAIN_ID, OPTIMISM_USDC, OPTIMISM_WLD,
        WORLD_CHAIN_CHAIN_ID, WORLD_CHAIN_USDC, WORLD_CHAIN_WLD, swap_calldata_is_stale,
        validate_deposit_status, validate_quote,
    },
    binance::{
        account::{AccountInformation, BinanceAccountClient},
        capital::{
            DepositRecord, NetworkInformation, TravelRuleWithdrawalRecord, WithdrawalRecord,
            select_capital_routes,
        },
        sub_account::{SubAccountAssetBalance, UniversalTransferRecord},
    },
    chain::rpc::{JsonRpcClient, TransactionReceipt},
    m8_readiness::{ARBITRUM_CHAIN_ID, ARBITRUM_ESP, ARBITRUM_USDC},
    wallet::{
        EvmJournalScope, EvmWallet, JournalStatus, NonceLane, NonceReconciliationOutcome,
        PROCESS_NONCE_LOCK_TTL, TransactionJournal, UnknownOutcomeReason, WalletCall,
        WalletTransactionParameters, acquire_process_nonce_lock, broadcast_signed_transaction,
    },
};

use super::{
    Direction, RebalanceAction, RebalanceExecutionJournal, RebalanceExecutionOperation,
    RebalanceExecutionProgress, RebalanceExecutionRequest, Route,
};

const GAS_LIMIT_MARGIN_NUMERATOR: u64 = 120;
const GAS_LIMIT_MARGIN_DENOMINATOR: u64 = 100;
const MAX_ERC20_GAS_LIMIT: u64 = 1_000_000;
const MAX_FEE_PER_GAS_WEI: u128 = 100_000_000_000;

#[derive(Clone, Debug)]
pub struct RebalanceRuntimeLimits {
    pub maximum_wld: Decimal,
    pub maximum_usdc: Decimal,
    pub maximum_esp: Decimal,
    pub operation_timeout: Duration,
    pub binance_withdrawal_api_mode: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectPrefundingPlan {
    pub action: RebalanceAction,
    pub requested_debit: U256,
    pub expected_credit: U256,
    pub withdrawal_fee: U256,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BinanceAddressVerificationTransferArtifact {
    pub schema_version: String,
    pub operation_id: String,
    pub approval_gate: String,
    pub production_approval_actor: String,
    pub production_approval_recorded_at_utc: String,
    pub expires_at_unix_seconds: u64,
    pub chain_id: u64,
    pub network: String,
    pub token_symbol: String,
    pub token_contract: String,
    pub token_decimals: u8,
    pub amount_base_units: String,
    pub recipient: String,
    pub source_wallet: String,
    pub initial_source_balance_base_units: String,
    pub initial_recipient_balance_base_units: String,
    pub maximum_transfer_count: u16,
    pub bridge_allowed: bool,
}

impl BinanceAddressVerificationTransferArtifact {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let bytes = fs::read(path).with_context(|| {
            format!(
                "failed to read Binance address verification artifact {}",
                path.display()
            )
        })?;
        serde_json::from_slice(&bytes)
            .context("Binance address verification artifact is invalid JSON")
    }

    fn validate(
        &self,
        wallet: Address,
        now_unix_seconds: u64,
        existing_operation: bool,
    ) -> anyhow::Result<(Address, Address, U256, U256, U256)> {
        ensure!(
            self.schema_version == "binance_address_verification_transfer_v1"
                && self.operation_id == "m9-binance-esp-address-verification-usdc-20260730"
                && self.approval_gate == "explicit_production_approved"
                && self.production_approval_actor == "operator"
                && !self.production_approval_recorded_at_utc.trim().is_empty()
                && self.chain_id == ARBITRUM_CHAIN_ID
                && self.network == "ARBITRUM"
                && self.token_symbol == "USDC"
                && self.token_contract.eq_ignore_ascii_case(ARBITRUM_USDC)
                && self.token_decimals == 6
                && self.amount_base_units == "998700"
                && self
                    .recipient
                    .eq_ignore_ascii_case("0x64d62673799a8dc69825ff1cc0d624b1065dab39")
                && self
                    .source_wallet
                    .eq_ignore_ascii_case("0x90d990c81320221d2882de32beea78923c1e77a3")
                && self.initial_source_balance_base_units == "25000000"
                && self.initial_recipient_balance_base_units == "0"
                && self.maximum_transfer_count == 1
                && !self.bridge_allowed,
            "Binance address verification artifact differs from the approved direct transfer"
        );
        ensure!(
            wallet
                == Address::from_str(&self.source_wallet)
                    .context("approved address verification source wallet is invalid")?,
            "address verification signer differs from the approved source wallet"
        );
        if !existing_operation {
            ensure!(
                now_unix_seconds < self.expires_at_unix_seconds,
                "Binance address verification transfer approval has expired"
            );
        }
        Ok((
            Address::from_str(&self.token_contract)
                .context("approved address verification token is invalid")?,
            Address::from_str(&self.recipient)
                .context("approved address verification recipient is invalid")?,
            U256::from_str_radix(&self.amount_base_units, 10)
                .context("approved address verification amount is invalid")?,
            U256::from_str_radix(&self.initial_source_balance_base_units, 10)
                .context("approved initial source balance is invalid")?,
            U256::from_str_radix(&self.initial_recipient_balance_base_units, 10)
                .context("approved initial recipient balance is invalid")?,
        ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceAddressVerificationTransferOutcome {
    pub operation_id: String,
    pub transaction_hash: B256,
    pub amount: U256,
    pub recipient: Address,
}

pub async fn execute_binance_address_verification_transfer(
    artifact: &BinanceAddressVerificationTransferArtifact,
    rpc: JsonRpcClient,
    wallet: EvmWallet,
    journal_path: PathBuf,
    timeout: Duration,
) -> anyhow::Result<BinanceAddressVerificationTransferOutcome> {
    ensure!(
        timeout >= Duration::from_secs(60) && timeout <= Duration::from_secs(15 * 60),
        "Binance address verification transfer timeout is outside the reviewed bounds"
    );
    ensure!(
        rpc.chain_id().await? == ARBITRUM_CHAIN_ID,
        "address verification RPC is not Arbitrum One"
    );
    let mut journal = TransactionJournal::open(journal_path)?;
    let existing_operation = journal.operation(&artifact.operation_id).is_some();
    let now_unix_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time precedes Unix epoch")?
        .as_secs();
    let (token, recipient, amount, initial_source_balance, initial_recipient_balance) =
        artifact.validate(wallet.address(), now_unix_seconds, existing_operation)?;
    if !existing_operation {
        let (source_balance, recipient_balance) = tokio::try_join!(
            rpc.erc20_balance(token, wallet.address()),
            rpc.erc20_balance(token, recipient),
        )?;
        ensure!(
            source_balance == initial_source_balance,
            "address verification source USDC balance changed from the approved snapshot"
        );
        ensure!(
            recipient_balance == initial_recipient_balance,
            "address verification recipient USDC balance changed from the approved snapshot"
        );
        ensure!(
            source_balance >= amount,
            "address verification source has insufficient USDC"
        );
    }
    let (latest_nonce, pending_nonce) = tokio::try_join!(
        rpc.latest_nonce(wallet.address()),
        rpc.pending_nonce(wallet.address()),
    )?;
    let reconciled = NonceLane::reconcile(
        &rpc,
        &mut journal,
        ARBITRUM_CHAIN_ID,
        wallet.address(),
        latest_nonce,
        pending_nonce,
    )
    .await?;
    let mut nonce_lane =
        finish_known_pending_recovery(&rpc, &mut journal, reconciled, timeout).await?;
    nonce_lane.set_journal_scope(EvmJournalScope {
        schema_version: EvmJournalScope::SCHEMA_VERSION,
        network_id: "arbitrum-one".to_owned(),
        wallet_id: format!("wallet:{:#x}", wallet.address()),
        strategy_id: "binance-esp-address-verification".to_owned(),
    })?;
    let call = WalletCall::erc20_transfer(token, recipient, amount)?;
    let transaction_hash = execute_wallet_call(
        &rpc,
        &wallet,
        &mut nonce_lane,
        &mut journal,
        artifact.operation_id.clone(),
        "binance_esp_address_verification_usdc",
        &call,
        timeout,
    )
    .await?;
    let receipt = rpc
        .transaction_receipt(transaction_hash)
        .await?
        .context("address verification transfer receipt disappeared")?;
    ensure!(
        receipt.status == 1
            && erc20_credit_from_receipt(&receipt, transaction_hash, token, recipient)? == amount,
        "address verification receipt does not prove the exact approved USDC transfer"
    );
    Ok(BinanceAddressVerificationTransferOutcome {
        operation_id: artifact.operation_id.clone(),
        transaction_hash,
        amount,
        recipient,
    })
}

#[allow(clippy::too_many_arguments)]
pub fn plan_direct_prefunding(
    target_wallet_balance: U256,
    current_wallet_balance: U256,
    token_decimals: u8,
    network: &NetworkInformation,
    chain_id: u64,
    maximum_fee: U256,
    maximum_debit: U256,
) -> anyhow::Result<Option<DirectPrefundingPlan>> {
    if current_wallet_balance >= target_wallet_balance {
        return Ok(None);
    }
    ensure!(
        network.network == "ARBITRUM"
            && chain_id == ARBITRUM_CHAIN_ID
            && network.withdrawal_available(),
        "prefunding requires the live direct Arbitrum withdrawal route"
    );
    let deficit = target_wallet_balance - current_wallet_balance;
    let fee = decimal_to_base_units(network.withdraw_fee, token_decimals)?;
    ensure!(
        fee <= maximum_fee,
        "live Binance withdrawal fee exceeds the approved prefunding cap"
    );
    let minimum = decimal_to_base_units(network.withdraw_min, token_decimals)?;
    let maximum = decimal_to_base_units(network.withdraw_max, token_decimals)?;
    let multiple =
        decimal_to_base_units(network.withdraw_integer_multiple, token_decimals)?.max(U256::ONE);
    let needed = deficit
        .checked_add(fee)
        .context("prefunding target plus withdrawal fee overflow")?
        .max(minimum);
    let remainder = needed % multiple;
    let requested_debit = if remainder.is_zero() {
        needed
    } else {
        needed
            .checked_add(multiple - remainder)
            .context("prefunding withdrawal rounding overflow")?
    };
    ensure!(
        requested_debit <= maximum && requested_debit <= maximum_debit,
        "required prefunding debit exceeds an approved or live withdrawal maximum"
    );
    let expected_credit = requested_debit
        .checked_sub(fee)
        .context("prefunding withdrawal fee exceeds debit")?;
    ensure!(
        current_wallet_balance
            .checked_add(expected_credit)
            .context("prefunding wallet target overflow")?
            >= target_wallet_balance,
        "prefunding withdrawal cannot reach the approved wallet target"
    );
    Ok(Some(DirectPrefundingPlan {
        action: RebalanceAction {
            direction: Direction::BinanceToWallet,
            amount: requested_debit,
            route: Route::Direct {
                binance_network: network.network.clone(),
                chain_id,
            },
        },
        requested_debit,
        expected_credit,
        withdrawal_fee: fee,
    }))
}

impl RebalanceRuntimeLimits {
    fn maximum_for(&self, symbol: &str) -> anyhow::Result<Decimal> {
        let maximum = match symbol {
            "WLD" => self.maximum_wld,
            "USDC" => self.maximum_usdc,
            "ESP" => self.maximum_esp,
            _ => bail!("full rebalance executor only permits WLD, USDC, and ESP"),
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

    async fn execute(
        &mut self,
        chain_id: u64,
        operation_id: String,
        purpose: &str,
        call: &WalletCall,
        timeout: Duration,
    ) -> anyhow::Result<B256> {
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
}

impl std::fmt::Debug for RebalanceExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RebalanceExecutor")
            .field("wallet", &self.evm.wallet_address())
            .field("world_nonce", &self.evm.nonce_state(WORLD_CHAIN_CHAIN_ID))
            .field("optimism_nonce", &self.evm.nonce_state(OPTIMISM_CHAIN_ID))
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
        ensure!(
            matches!(
                limits.binance_withdrawal_api_mode.as_str(),
                "standard" | "travel_rule"
            ),
            "rebalance Binance withdrawal API mode is invalid"
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
            },
            limits,
        })
    }

    pub fn active_operation(&self) -> anyhow::Result<Option<&RebalanceExecutionOperation>> {
        self.execution_journal.active_operation()
    }

    pub fn operations(&self) -> &std::collections::BTreeMap<String, RebalanceExecutionOperation> {
        self.execution_journal.operations()
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
        self.process(operation, false).await.map(Some)
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
        let operation = self.execution_journal.reserve(&request)?;
        self.process(operation, true).await
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
            operation = self
                .begin_master_transfer(operation, created_here, bridge_before)
                .await?;
        }
        operation = self.finish_master_transfer(operation).await?;
        operation = self
            .begin_binance_withdrawal(operation, withdrawal_submission_safe, &binance_network)
            .await?;
        let record = match &operation.progress {
            RebalanceExecutionProgress::BinanceWithdrawalSubmitted { .. } => {
                self.wait_withdrawal(&operation).await?
            }
            RebalanceExecutionProgress::Completed { .. } => return Ok(operation),
            RebalanceExecutionProgress::Failed { reason } => {
                bail!("rebalance previously failed: {reason}")
            }
            _ => bail!("direct Binance-to-wallet operation has invalid recovery state"),
        };
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
            chain_id == WORLD_CHAIN_CHAIN_ID,
            "direct rebalance source is not World Chain"
        );
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
                    WORLD_CHAIN_CHAIN_ID,
                    format!("{}:deposit", operation.intent.operation_id),
                    "rebalance_wallet_to_binance",
                    &call,
                    self.limits.operation_timeout,
                )
                .await?;
            operation = self.execution_journal.advance(
                &operation.intent.operation_id,
                RebalanceExecutionProgress::DepositTransferMined {
                    chain_id: WORLD_CHAIN_CHAIN_ID,
                    transaction_hash,
                },
            )?;
        }
        operation = self
            .finish_binance_deposit(operation, &binance_network)
            .await?;
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
        operation = self
            .begin_binance_withdrawal(operation, withdrawal_submission_safe, &binance_network)
            .await?;
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
        if let RebalanceExecutionProgress::DepositTransferMined {
            transaction_hash, ..
        } = operation.progress
        {
            let deposit = self
                .wait_binance_deposit(&operation, transaction_hash, network)
                .await?;
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
            let wallet_after = self
                .evm
                .rpc(WORLD_CHAIN_CHAIN_ID)?
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
            ensure!(
                created_here,
                "rebalance intent has no indexed Binance master transfer; operator review required"
            );
            let amount =
                base_units_to_decimal(operation.intent.amount, operation.intent.token_decimals)?;
            self.treasury_binance
                .universal_transfer_from_subaccount(
                    &self.subaccount_email,
                    &operation.intent.token_symbol,
                    amount,
                    client_transaction_id,
                )
                .await?
                .transaction_id
        };
        self.execution_journal.advance(
            &operation.intent.operation_id,
            RebalanceExecutionProgress::BinanceTransferSubmitted {
                transaction_id,
                bridge_balance_before,
            },
        )
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
        operation: RebalanceExecutionOperation,
        submission_safe: bool,
        network: &str,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        let RebalanceExecutionProgress::BinanceTransferCompleted {
            bridge_balance_before,
            ..
        } = operation.progress
        else {
            return Ok(operation);
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
        } else {
            ensure!(
                submission_safe,
                "master transfer completed but no Binance withdrawal is indexed; operator review required"
            );
            let amount =
                base_units_to_decimal(operation.intent.amount, operation.intent.token_decimals)?;
            self.submit_binance_withdrawal(&operation, network, amount)
                .await?
        };
        self.execution_journal.advance(
            &operation.intent.operation_id,
            RebalanceExecutionProgress::BinanceWithdrawalSubmitted {
                submission_reference,
                bridge_balance_before,
            },
        )
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

    async fn submit_binance_withdrawal(
        &self,
        operation: &RebalanceExecutionOperation,
        network: &str,
        amount: Decimal,
    ) -> anyhow::Result<String> {
        let address = format!("{:#x}", operation.intent.wallet_owner);
        match self.limits.binance_withdrawal_api_mode.as_str() {
            "standard" => {
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
            "travel_rule" => {
                let submission = self
                    .treasury_binance
                    .withdraw(
                        &operation.intent.token_symbol,
                        network,
                        &address,
                        amount,
                        &operation.intent.withdraw_order_id,
                    )
                    .await?;
                ensure!(
                    submission.accepted,
                    "Binance rejected rebalance withdrawal: {}",
                    submission.info
                );
                Ok(submission.tr_id.to_string())
            }
            _ => bail!("unsupported Binance withdrawal API mode"),
        }
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
        &self,
        operation: &RebalanceExecutionOperation,
        transaction_hash: B256,
        network: &str,
    ) -> anyhow::Result<DepositRecord> {
        let transaction_hash = format!("{transaction_hash:#x}");
        let deadline = tokio::time::Instant::now() + self.limits.operation_timeout;
        loop {
            if let Some(record) = self
                .trading_binance
                .deposit_history(&operation.intent.token_symbol, &transaction_hash)
                .await?
                .into_iter()
                .next()
            {
                ensure!(
                    record.network == network,
                    "Binance credited deposit on a different network"
                );
                if record.questionnaire_required() {
                    let submission = self
                        .trading_binance
                        .submit_deposit_questionnaire(&record.deposit_id)
                        .await?;
                    ensure!(
                        submission.accepted,
                        "Binance rejected deposit questionnaire: {}",
                        submission.info
                    );
                } else if record.is_credited() {
                    return Ok(record);
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
        let coins = self.trading_binance.all_coin_information().await?;
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
            JournalStatus::CancelledBeforeSigning => {
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

fn validate_master_subaccount_view(
    trading_account: &AccountInformation,
    master_balances: &[SubAccountAssetBalance],
) -> anyhow::Result<()> {
    for asset in ["ESP", "USDC", "WLD"] {
        let trading = trading_account
            .balances
            .iter()
            .find(|balance| balance.asset == asset);
        let master = master_balances
            .iter()
            .find(|balance| balance.asset == asset);
        let trading_free = trading.map_or(Decimal::ZERO, |balance| balance.free);
        let trading_locked = trading.map_or(Decimal::ZERO, |balance| balance.locked);
        let master_free = master.map_or(Decimal::ZERO, |balance| balance.free);
        let master_locked = master.map_or(Decimal::ZERO, |balance| balance.locked);
        ensure!(
            trading_free == master_free && trading_locked == master_locked,
            "Binance master key does not resolve to the configured trading sub-account"
        );
    }
    Ok(())
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
    let fee = Decimal::from_str(&record.transaction_fee).ok();
    let exact_debit = match (amount, fee) {
        (Some(amount), Some(fee)) => {
            amount == requested || amount.checked_add(fee) == Some(requested)
        }
        _ => false,
    };
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
    use std::str::FromStr;

    use alloy_primitives::{Address, B256, U256, keccak256};
    use rust_decimal::Decimal;

    use crate::{
        binance::capital::{NetworkInformation, TravelRuleWithdrawalRecord, WithdrawalRecord},
        chain::rpc::{ReceiptLog, TransactionReceipt},
    };

    use super::{
        ARBITRUM_CHAIN_ID, BinanceAddressVerificationTransferArtifact, WORLD_CHAIN_CHAIN_ID,
        WORLD_CHAIN_USDC, WORLD_CHAIN_WLD, base_units_to_decimal, decimal_to_base_units,
        decimal_to_base_units_floor, matches_travel_rule_record_identity_without_client_id,
        merge_travel_rule_withdrawal_detail, plan_direct_prefunding,
        reconcile_approved_travel_rule_rejection, validate_across_fill_receipt,
        validate_approved_asset, validate_direct_withdrawal_receipt,
        withdrawal_received_base_units, withdrawal_requested_base_units,
    };

    fn arbitrum_network(fee: &str) -> NetworkInformation {
        NetworkInformation {
            network: "ARBITRUM".to_owned(),
            name: "Arbitrum One".to_owned(),
            deposit_enable: true,
            withdraw_enable: true,
            busy: false,
            withdraw_fee: Decimal::from_str_exact(fee).unwrap(),
            withdraw_min: Decimal::from_str_exact("2").unwrap(),
            withdraw_max: Decimal::from_str_exact("1000000").unwrap(),
            withdraw_integer_multiple: Decimal::from_str_exact("0.01").unwrap(),
        }
    }

    #[test]
    fn address_verification_artifact_is_exact_expiring_and_disallows_bridge() {
        let mut artifact: BinanceAddressVerificationTransferArtifact = serde_json::from_str(
            include_str!("../../config/operations/binance-esp-address-verification.v1.json"),
        )
        .unwrap();
        let wallet = Address::from_str("0x90d990c81320221d2882de32beea78923c1e77a3").unwrap();
        let (_, recipient, amount, source_balance, recipient_balance) = artifact
            .validate(wallet, artifact.expires_at_unix_seconds - 1, false)
            .unwrap();
        assert_eq!(
            recipient,
            Address::from_str("0x64d62673799a8dc69825ff1cc0d624b1065dab39").unwrap()
        );
        assert_eq!(amount, U256::from(998_700_u64));
        assert_eq!(source_balance, U256::from(25_000_000_u64));
        assert_eq!(recipient_balance, U256::ZERO);
        assert!(
            artifact
                .validate(wallet, artifact.expires_at_unix_seconds, false)
                .is_err()
        );
        assert!(
            artifact
                .validate(wallet, artifact.expires_at_unix_seconds, true)
                .is_ok()
        );

        artifact.bridge_allowed = true;
        assert!(
            artifact
                .validate(wallet, artifact.expires_at_unix_seconds - 1, false)
                .is_err()
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
    fn direct_prefunding_adds_the_live_fee_and_reaches_the_exact_target() {
        let plan = plan_direct_prefunding(
            U256::from(25_000_000_u64),
            U256::ZERO,
            6,
            &arbitrum_network("1"),
            ARBITRUM_CHAIN_ID,
            U256::from(5_000_000_u64),
            U256::from(30_000_000_u64),
        )
        .unwrap()
        .unwrap();
        assert_eq!(plan.requested_debit, U256::from(26_000_000_u64));
        assert_eq!(plan.withdrawal_fee, U256::from(1_000_000_u64));
        assert_eq!(plan.expected_credit, U256::from(25_000_000_u64));
    }

    #[test]
    fn direct_prefunding_is_noop_when_funded_and_fails_closed_on_fee_growth() {
        assert!(
            plan_direct_prefunding(
                U256::from(25_000_000_u64),
                U256::from(25_000_000_u64),
                6,
                &arbitrum_network("100"),
                ARBITRUM_CHAIN_ID,
                U256::from(5_000_000_u64),
                U256::from(30_000_000_u64),
            )
            .unwrap()
            .is_none()
        );
        assert!(
            plan_direct_prefunding(
                U256::from(25_000_000_u64),
                U256::ZERO,
                6,
                &arbitrum_network("5.01"),
                ARBITRUM_CHAIN_ID,
                U256::from(5_000_000_u64),
                U256::from(30_000_000_u64),
            )
            .is_err()
        );
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

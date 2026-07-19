use std::{path::PathBuf, str::FromStr, time::Duration};

use alloy_primitives::{Address, B256, U256, keccak256};
use anyhow::{Context, bail, ensure};
use rust_decimal::Decimal;

use crate::{
    across::{
        AcrossClient, AcrossQuoteRequest, OPTIMISM_CHAIN_ID, OPTIMISM_USDC, OPTIMISM_WLD,
        WORLD_CHAIN_CHAIN_ID, WORLD_CHAIN_USDC, WORLD_CHAIN_WLD, swap_calldata_is_stale,
        validate_deposit_status, validate_quote,
    },
    binance::{
        account::{AccountInformation, BinanceAccountClient},
        capital::{DepositRecord, WithdrawalRecord, select_capital_routes},
        sub_account::{SubAccountAssetBalance, UniversalTransferRecord},
    },
    chain::rpc::{JsonRpcClient, TransactionReceipt},
    wallet::{
        EvmWallet, JournalStatus, NonceLane, NonceReconciliationOutcome, PROCESS_NONCE_LOCK_TTL,
        TransactionJournal, UnknownOutcomeReason, WalletCall, WalletTransactionParameters,
        acquire_process_nonce_lock, broadcast_signed_transaction,
    },
};

use super::{
    Direction, RebalanceExecutionJournal, RebalanceExecutionOperation, RebalanceExecutionProgress,
    RebalanceExecutionRequest, Route,
};

const GAS_LIMIT_MARGIN_NUMERATOR: u64 = 120;
const GAS_LIMIT_MARGIN_DENOMINATOR: u64 = 100;
const MAX_ERC20_GAS_LIMIT: u64 = 1_000_000;
const MAX_FEE_PER_GAS_WEI: u128 = 100_000_000_000;

#[derive(Clone, Debug)]
pub struct RebalanceRuntimeLimits {
    pub maximum_wld: Decimal,
    pub maximum_usdc: Decimal,
    pub operation_timeout: Duration,
    pub binance_withdrawal_api_mode: String,
}

impl RebalanceRuntimeLimits {
    fn maximum_for(&self, symbol: &str) -> anyhow::Result<Decimal> {
        let maximum = match symbol {
            "WLD" => self.maximum_wld,
            "USDC" => self.maximum_usdc,
            _ => bail!("full rebalance executor only permits WLD and USDC"),
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
    world: JsonRpcClient,
    optimism: JsonRpcClient,
    wallet: EvmWallet,
    execution_journal: RebalanceExecutionJournal,
    transaction_journal: TransactionJournal,
    world_nonce: NonceLane,
    optimism_nonce: NonceLane,
    limits: RebalanceRuntimeLimits,
}

impl std::fmt::Debug for RebalanceExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RebalanceExecutor")
            .field("wallet", &self.wallet.address())
            .field("world_nonce", &self.world_nonce.state())
            .field("optimism_nonce", &self.optimism_nonce.state())
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
        let world_nonce = finish_known_pending_recovery(
            &world,
            &mut transaction_journal,
            world_reconciled,
            limits.operation_timeout,
        )
        .await?;
        let optimism_nonce = finish_known_pending_recovery(
            &optimism,
            &mut transaction_journal,
            optimism_reconciled,
            limits.operation_timeout,
        )
        .await?;

        Ok(Self {
            trading_binance,
            treasury_binance,
            subaccount_email,
            across,
            world,
            optimism,
            wallet,
            execution_journal: RebalanceExecutionJournal::open(execution_journal_path)?,
            transaction_journal,
            world_nonce,
            optimism_nonce,
            limits,
        })
    }

    pub fn active_operation(&self) -> anyhow::Result<Option<&RebalanceExecutionOperation>> {
        self.execution_journal.active_operation()
    }

    pub async fn recover_active(&mut self) -> anyhow::Result<Option<RebalanceExecutionOperation>> {
        let Some(operation) = self.execution_journal.active_operation()?.cloned() else {
            return Ok(None);
        };
        validate_approved_world_asset(
            &operation.intent.token_symbol,
            operation.intent.token_decimals,
            operation.intent.token_contract,
        )?;
        ensure!(
            operation.intent.wallet_owner == self.wallet.address(),
            "journaled rebalance wallet differs from signer"
        );
        self.process(operation, false).await.map(Some)
    }

    pub async fn execute(
        &mut self,
        request: RebalanceExecutionRequest,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        ensure!(
            request.wallet_owner == self.wallet.address(),
            "rebalance request wallet differs from signer"
        );
        validate_approved_world_asset(
            &request.token_symbol,
            request.token_decimals,
            request.token_contract,
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
            chain_id == WORLD_CHAIN_CHAIN_ID,
            "direct rebalance target is not World Chain"
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
                .world
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
                &self.world,
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
            let transaction_hash = execute_wallet_call(
                &self.world,
                &self.wallet,
                &mut self.world_nonce,
                &mut self.transaction_journal,
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
                .optimism
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
                &self.optimism,
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
            let transaction_hash = execute_wallet_call(
                &self.optimism,
                &self.wallet,
                &mut self.optimism_nonce,
                &mut self.transaction_journal,
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
                self.world
                    .erc20_balance(output_token, operation.intent.wallet_owner)
                    .await?
            }
            OPTIMISM_CHAIN_ID => {
                self.optimism
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
        match chain_id {
            WORLD_CHAIN_CHAIN_ID => {
                execute_wallet_call(
                    &self.world,
                    &self.wallet,
                    &mut self.world_nonce,
                    &mut self.transaction_journal,
                    operation_id,
                    purpose,
                    call,
                    self.limits.operation_timeout,
                )
                .await
            }
            OPTIMISM_CHAIN_ID => {
                execute_wallet_call(
                    &self.optimism,
                    &self.wallet,
                    &mut self.optimism_nonce,
                    &mut self.transaction_journal,
                    operation_id,
                    purpose,
                    call,
                    self.limits.operation_timeout,
                )
                .await
            }
            _ => bail!("unsupported rebalance transaction chain"),
        }
    }

    async fn wait_across_fill(
        &mut self,
        operation: RebalanceExecutionOperation,
    ) -> anyhow::Result<RebalanceExecutionOperation> {
        let RebalanceExecutionProgress::BridgeMined {
            origin_chain_id,
            transaction_hash,
            minimum_output_amount,
            destination_balance_before,
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
                    let rpc = if destination_chain_id == OPTIMISM_CHAIN_ID {
                        &self.optimism
                    } else {
                        &self.world
                    };
                    let token =
                        token_on_chain(&operation.intent.token_symbol, destination_chain_id)?;
                    let after = rpc
                        .erc20_balance(token, operation.intent.wallet_owner)
                        .await?;
                    let received = after
                        .checked_sub(destination_balance_before)
                        .context("Across destination balance decreased")?;
                    ensure!(
                        received >= minimum_output_amount,
                        "Across destination balance delta is below minimum output"
                    );
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
            received_base_units,
            ..
        } = operation.progress
        else {
            return Ok(operation);
        };
        let wallet_after = self
            .world
            .erc20_balance(
                operation.intent.token_contract,
                operation.intent.wallet_owner,
            )
            .await?;
        ensure!(
            wallet_after
                >= operation
                    .intent
                    .wallet_balance_before
                    .checked_add(received_base_units)
                    .context("wallet balance target overflow")?,
            "World Chain balance did not receive Across output"
        );
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
                .world
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
    ensure!(
        receipt.transaction_hash == expected_hash,
        "withdrawal receipt transaction hash changed"
    );
    ensure!(receipt.status == 1, "withdrawal transaction reverted");

    let transfer_topic = keccak256("Transfer(address,address,uint256)");
    let mut received = U256::ZERO;
    for log in receipt
        .logs
        .iter()
        .filter(|log| log.address == token && log.topics.first() == Some(&transfer_topic))
    {
        ensure!(
            log.topics.len() == 3,
            "withdrawal ERC-20 Transfer log has wrong topics"
        );
        ensure!(
            log.data.len() == 32,
            "withdrawal ERC-20 Transfer log amount is not one word"
        );
        let recipient = Address::from_slice(&log.topics[2].as_slice()[12..]);
        if recipient == owner {
            received = received
                .checked_add(U256::from_be_slice(&log.data))
                .context("withdrawal ERC-20 transfer sum overflow")?;
        }
    }
    ensure!(
        received >= expected_delta,
        "withdrawal receipt did not transfer the expected token amount to the wallet"
    );
    Ok(())
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
    for asset in ["USDC", "WLD"] {
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
        ("USDC", OPTIMISM_CHAIN_ID) => Ok(OPTIMISM_USDC),
        ("USDC", WORLD_CHAIN_CHAIN_ID) => Ok(WORLD_CHAIN_USDC),
        ("WLD", OPTIMISM_CHAIN_ID) => Ok(OPTIMISM_WLD),
        ("WLD", WORLD_CHAIN_CHAIN_ID) => Ok(WORLD_CHAIN_WLD),
        _ => bail!("unsupported rebalance token or chain"),
    }
}

fn validate_approved_world_asset(
    symbol: &str,
    decimals: u8,
    contract: Address,
) -> anyhow::Result<()> {
    let (expected_decimals, expected_contract) = match symbol {
        "WLD" => (18, WORLD_CHAIN_WLD),
        "USDC" => (6, WORLD_CHAIN_USDC),
        _ => bail!("full rebalance executor only permits WLD and USDC"),
    };
    ensure!(
        decimals == expected_decimals && contract == expected_contract,
        "rebalance token metadata differs from the approved World Chain asset"
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

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, B256, U256, keccak256};
    use rust_decimal::Decimal;

    use crate::{
        binance::capital::WithdrawalRecord,
        chain::rpc::{ReceiptLog, TransactionReceipt},
    };

    use super::{
        WORLD_CHAIN_USDC, WORLD_CHAIN_WLD, base_units_to_decimal, decimal_to_base_units,
        decimal_to_base_units_floor, validate_approved_world_asset,
        validate_direct_withdrawal_receipt, withdrawal_received_base_units,
        withdrawal_requested_base_units,
    };

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
        validate_approved_world_asset("WLD", 18, WORLD_CHAIN_WLD).unwrap();
        validate_approved_world_asset("USDC", 6, WORLD_CHAIN_USDC).unwrap();
        assert!(validate_approved_world_asset("WLD", 6, WORLD_CHAIN_WLD).is_err());
        assert!(validate_approved_world_asset("USDT", 6, Address::repeat_byte(1)).is_err());
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
            logs: vec![ReceiptLog {
                address: token,
                topics: vec![
                    keccak256("Transfer(address,address,uint256)"),
                    address_topic(Address::repeat_byte(0x33)),
                    address_topic(wallet),
                ],
                data: received.to_be_bytes::<32>().to_vec(),
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
}

use std::{
    collections::BTreeMap,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use anyhow::{Context, ensure};
use rust_decimal::Decimal;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::{
    arbitrage::{
        CoordinatorCommand, EntryPreflightHandle, ExecutionMode, FreshBinanceTopSnapshot,
        LatestOpportunityReceiver, LegResult, LegRole, LegStatus, MAX_RECOVERY_ATTEMPTS,
        PaperOpportunity, PaperTradeCoordinator, PaperTradeEvent, PaperTradeEventState,
        PaperTradeHandle, TradeIntent, TradeOperation, TradeStage, execution_failure_event_state,
        initial_execution_lane,
    },
    binance::{
        account::SymbolRules,
        execution::{
            BinanceExecutionService, BinanceExecutionServiceError, BinanceOrderRequest,
            BinanceOrderRequestKind,
        },
        order_plan::{
            decimal_from_base_units, plan_limit_ioc, plan_market_order, recovery_client_order_id,
        },
        ws_api::OrderResult,
    },
    dex::{
        execution::{DexExecutionService, DexExecutionServiceError, SwapRoute},
        revert_diagnostics::{DexRevertDiagnosticHandle, DexRevertDiagnosticRequest},
    },
    execution_accounting::{
        CommissionAssetValuation, binance_leg_result, dex_leg_result,
        native_gas_to_token_a_base_units,
    },
    telemetry::{
        ARBITRAGE_BINANCE_ORDER_KIND, ARBITRAGE_DEX_REVERT_KIND, ARBITRAGE_EXECUTION_STAGE_KIND,
        ARBITRAGE_RESULT_KIND, TelemetryHandle,
    },
};

type LegFuture<'a> = Pin<Box<dyn Future<Output = (LegRole, LegResult)> + Send + 'a>>;

pub trait LiveLegExecutor: Send + Sync + 'static {
    fn execute<'a>(
        &'a self,
        intent: &'a TradeIntent,
        command: &'a CoordinatorCommand,
    ) -> LegFuture<'a>;
}

pub struct ComposedLiveLegExecutor {
    dex: DexExecutionService,
    binance: BinanceExecutionService,
    rules: SymbolRules,
    base_asset: String,
    base_decimals: u8,
    quote_asset: String,
    quote_decimals: u8,
    commission_asset: String,
    commission_price_symbol: String,
    market_state: EntryPreflightHandle,
    dex_revert_diagnostics: DexRevertDiagnosticHandle,
    telemetry: TelemetryHandle,
    engine_id: String,
    dex_receipt_observed_at: Mutex<BTreeMap<String, Instant>>,
}

#[derive(Clone, Debug)]
struct BinanceOrderPlacementObservation {
    started_at: Instant,
    memory_top: Option<FreshBinanceTopSnapshot>,
    recovery_attempt: Option<usize>,
    recovery_limit_counterfactual: Option<RecoveryLimitCounterfactual>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecoveryLimitCounterfactual {
    price: rust_decimal::Decimal,
    top_quantity: rust_decimal::Decimal,
    submitted_quantity: rust_decimal::Decimal,
    top_covers_quantity: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DexRevertContext {
    protocol: String,
    pool_reference: String,
    amount_in_base_units: String,
    amount_out_minimum_base_units: String,
    deadline_unix_seconds: u64,
}

pub struct ComposedLiveLegExecutorConfig {
    pub rules: SymbolRules,
    pub base_asset: String,
    pub base_decimals: u8,
    pub quote_asset: String,
    pub quote_decimals: u8,
    pub commission_asset: String,
    pub commission_price_symbol: String,
    pub market_state: EntryPreflightHandle,
    pub dex_revert_diagnostics: DexRevertDiagnosticHandle,
    pub telemetry: TelemetryHandle,
    pub engine_id: String,
}

impl ComposedLiveLegExecutor {
    pub fn new(
        dex: DexExecutionService,
        binance: BinanceExecutionService,
        config: ComposedLiveLegExecutorConfig,
    ) -> anyhow::Result<Self> {
        let ComposedLiveLegExecutorConfig {
            rules,
            base_asset,
            base_decimals,
            quote_asset,
            quote_decimals,
            commission_asset,
            commission_price_symbol,
            market_state,
            dex_revert_diagnostics,
            telemetry,
            engine_id,
        } = config;
        ensure!(
            rules.symbol == format!("{base_asset}{quote_asset}"),
            "live symbol mismatch"
        );
        ensure!(rules.base_asset == base_asset, "live base asset mismatch");
        ensure!(
            rules.quote_asset == quote_asset,
            "live quote asset mismatch"
        );
        ensure!(
            base_decimals <= 36 && quote_decimals <= 36,
            "live token decimals invalid"
        );
        ensure!(
            commission_asset == "BNB",
            "live Binance commissions must be paid in BNB"
        );
        ensure!(
            commission_price_symbol.starts_with(&commission_asset)
                && commission_price_symbol
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit()),
            "live Binance commission price symbol is invalid"
        );
        ensure!(!engine_id.is_empty(), "live telemetry engine id is empty");
        Ok(Self {
            dex,
            binance,
            rules,
            base_asset,
            base_decimals,
            quote_asset,
            quote_decimals,
            commission_asset,
            commission_price_symbol,
            market_state,
            dex_revert_diagnostics,
            telemetry,
            engine_id,
            dex_receipt_observed_at: Mutex::new(BTreeMap::new()),
        })
    }

    async fn execute_inner(
        &self,
        intent: &TradeIntent,
        command: &CoordinatorCommand,
    ) -> (LegRole, LegResult) {
        match command {
            CoordinatorCommand::DispatchDex {
                operation_id, plan, ..
            } => {
                let role = LegRole::Dex;
                let Some(bounds) = intent.admission.as_ref() else {
                    return failed(role, "dex:missing-admission");
                };
                let Some(plan) = plan.as_ref() else {
                    return failed(role, "dex:missing-plan");
                };
                if unix_seconds().is_none_or(|now| now >= plan.deadline_unix_seconds) {
                    return failed(role, "dex:expired-plan");
                }
                let request = match plan.execution_request(operation_id.clone()) {
                    Ok(request) => request,
                    Err(error) => {
                        tracing::error!(operation_id, error = %error, "journaled DEX plan is invalid");
                        return failed(role, "dex:invalid-plan");
                    }
                };
                let revert_context = DexRevertContext::from_request(&request);
                match self.dex.execute(request).await {
                    Ok(outcome) => {
                        let gas = if bounds.gas_conversion_price_token_a.is_zero() {
                            tracing::warn!(
                                operation_id,
                                gas_used = outcome.gas_used,
                                effective_gas_price = outcome.effective_gas_price,
                                l1_fee = outcome.l1_fee,
                                "DEX gas token-A conversion is unavailable; execution remains valid"
                            );
                            0
                        } else {
                            match native_gas_to_token_a_base_units(
                                outcome.gas_used,
                                outcome.effective_gas_price,
                                outcome.l1_fee,
                                bounds.gas_conversion_price_token_a,
                                self.quote_decimals,
                            ) {
                                Ok(gas) => gas,
                                Err(error) => {
                                    tracing::error!(operation_id, error = %error, "DEX gas accounting is unknown");
                                    return unknown(role, "dex:accounting-unknown");
                                }
                            }
                        };
                        match dex_leg_result(intent.direction, outcome, gas) {
                            Ok(mut result) => {
                                if let Some(surplus) = cap_dex_credit_to_execution_envelope(
                                    intent.direction,
                                    intent.planned_token_b_base_units,
                                    &mut result,
                                ) {
                                    tracing::info!(
                                        operation_id,
                                        surplus_token_b_base_units = surplus,
                                        "favorable DEX output above the immutable hedge envelope remains in wallet inventory"
                                    );
                                }
                                if let Ok(mut timings) = self.dex_receipt_observed_at.lock() {
                                    timings.insert(intent.plan_id.clone(), Instant::now());
                                } else {
                                    tracing::warn!(
                                        plan_id = intent.plan_id,
                                        "DEX receipt latency telemetry lock is poisoned"
                                    );
                                }
                                (role, result)
                            }
                            Err(error) => {
                                tracing::error!(operation_id, error = %error, "DEX receipt accounting is unknown");
                                unknown(role, "dex:accounting-unknown")
                            }
                        }
                    }
                    Err(DexExecutionServiceError::FailedBeforeSubmission { reason }) => {
                        tracing::warn!(operation_id, reason, "DEX leg failed before submission");
                        failed(role, "dex:unsubmitted")
                    }
                    Err(DexExecutionServiceError::Reverted {
                        transaction_hash,
                        block_number,
                        gas_used,
                        effective_gas_price,
                        l1_fee,
                        reason,
                    }) => {
                        let diagnostic_submit =
                            self.dex_revert_diagnostics
                                .try_submit(DexRevertDiagnosticRequest {
                                    plan_id: intent.plan_id.clone(),
                                    operation_id: operation_id.clone(),
                                    pair_id: intent.pair_id.clone(),
                                    source_revision: intent.source_revision.clone(),
                                    direction: arbitrage_direction_label(intent.direction)
                                        .to_owned(),
                                    protocol: revert_context.protocol.clone(),
                                    pool_reference: revert_context.pool_reference.clone(),
                                    transaction_hash,
                                    block_number,
                                    gas_used,
                                    effective_gas_price,
                                    l1_fee,
                                    amount_in_base_units: revert_context
                                        .amount_in_base_units
                                        .clone(),
                                    amount_out_minimum_base_units: revert_context
                                        .amount_out_minimum_base_units
                                        .clone(),
                                    deadline_unix_seconds: revert_context.deadline_unix_seconds,
                                    execution_reason: reason.clone(),
                                });
                        self.telemetry.emit(
                            ARBITRAGE_DEX_REVERT_KIND,
                            serde_json::json!({
                                "engine_id": self.engine_id,
                                "phase": "receipt",
                                "diagnostic_status": diagnostic_submit.label(),
                                "plan_id": intent.plan_id,
                                "operation_id": operation_id,
                                "pair_id": intent.pair_id,
                                "source_revision": intent.source_revision,
                                "direction": arbitrage_direction_label(intent.direction),
                                "protocol": revert_context.protocol,
                                "pool_reference": revert_context.pool_reference,
                                "transaction_hash": format!("{transaction_hash:#x}"),
                                "block_number": block_number,
                                "gas_used": gas_used,
                                "effective_gas_price": effective_gas_price.to_string(),
                                "l1_fee": l1_fee.to_string(),
                                "amount_in_base_units": revert_context.amount_in_base_units,
                                "amount_out_minimum_base_units":
                                    revert_context.amount_out_minimum_base_units,
                                "deadline_unix_seconds": revert_context.deadline_unix_seconds,
                                "execution_reason": bounded_telemetry_reason(&reason),
                            }),
                        );
                        let gas = if bounds.gas_conversion_price_token_a.is_zero() {
                            tracing::warn!(
                                operation_id,
                                gas_used,
                                effective_gas_price,
                                l1_fee,
                                "reverted DEX gas token-A conversion is unavailable"
                            );
                            0
                        } else {
                            match native_gas_to_token_a_base_units(
                                gas_used,
                                effective_gas_price,
                                l1_fee,
                                bounds.gas_conversion_price_token_a,
                                self.quote_decimals,
                            ) {
                                Ok(gas) => gas,
                                Err(error) => {
                                    tracing::error!(operation_id, error = %error, "reverted DEX gas accounting is unknown");
                                    return unknown(role, "dex:revert-accounting-unknown");
                                }
                            }
                        };
                        tracing::warn!(
                            operation_id,
                            transaction_hash = %transaction_hash,
                            reason,
                            gas_cost_token_a_base_units = gas,
                            l1_fee,
                            "DEX transaction reverted with a known zero-token outcome"
                        );
                        failed_with_gas(role, gas, &format!("dex:{transaction_hash:#x}:reverted"))
                    }
                    Err(DexExecutionServiceError::OutcomeUnknown { reason }) => {
                        tracing::error!(
                            operation_id,
                            reason,
                            "DEX child outcome requires journal reconciliation"
                        );
                        unknown(role, "dex:child-unknown")
                    }
                }
            }
            CoordinatorCommand::DispatchCex {
                client_order_id,
                target_token_b_delta_base_units,
                limit_price,
            } => {
                self.execute_cex_limit(
                    intent,
                    LegRole::Cex,
                    client_order_id.clone(),
                    *target_token_b_delta_base_units,
                    *limit_price,
                )
                .await
            }
            CoordinatorCommand::RecoverCex {
                attempt,
                target_token_b_delta_base_units,
            } => {
                let client_order_id =
                    match recovery_client_order_id(&intent.cex_client_order_id, *attempt) {
                        Ok(value) => value,
                        Err(error) => {
                            tracing::error!(error = %error, "recovery client order id is invalid");
                            return failed(LegRole::RecoveryCex, "cex:invalid-recovery-id");
                        }
                    };
                self.execute_cex_market(
                    intent,
                    LegRole::RecoveryCex,
                    client_order_id,
                    *attempt,
                    *target_token_b_delta_base_units,
                )
                .await
            }
        }
    }

    async fn execute_cex_limit(
        &self,
        intent: &TradeIntent,
        role: LegRole,
        client_order_id: String,
        target_token_b_delta_base_units: i128,
        limit_price: Option<rust_decimal::Decimal>,
    ) -> (LegRole, LegResult) {
        let Some(limit_price) = limit_price else {
            return failed(role, "cex:missing-limit");
        };
        let mut planned = match plan_limit_ioc(
            client_order_id.clone(),
            client_order_id.clone(),
            target_token_b_delta_base_units,
            self.base_decimals,
            limit_price,
            &self.rules,
        ) {
            Ok(Some(planned)) => planned,
            Ok(None) => {
                self.emit_binance_order_error(
                    intent,
                    role,
                    &client_order_id,
                    None,
                    "filtered_before_submission",
                    "LIMIT IOC quantity rounded below one Binance step",
                );
                return failed(role, "cex:sub-step-command");
            }
            Err(error) => {
                let reason = format!("{error:#}");
                self.emit_binance_order_error(
                    intent,
                    role,
                    &client_order_id,
                    None,
                    "filtered_before_submission",
                    &reason,
                );
                tracing::error!(client_order_id, error = %error, "bounded Binance IOC plan is invalid");
                return failed(role, "cex:invalid-plan");
            }
        };
        planned.request.latency_origin = match self.dex_receipt_observed_at.lock() {
            Ok(mut timings) => timings.remove(&intent.plan_id),
            Err(_) => {
                tracing::warn!(
                    plan_id = intent.plan_id,
                    "DEX receipt latency telemetry lock is poisoned"
                );
                None
            }
        };
        let placement = self.emit_binance_order_plan(
            intent,
            role,
            planned.target_base_units,
            planned.submitted_base_units,
            &planned.request,
            None,
        );
        match self.binance.execute(planned.request).await {
            Ok(outcome) => {
                let commission_top = self.commission_price_top(&outcome.order);
                self.emit_binance_order_result(
                    intent,
                    role,
                    &outcome.order,
                    outcome.reconciled_after_unknown,
                    commission_top.as_ref(),
                    &placement,
                );
                match binance_leg_result(
                    &outcome.order,
                    &self.base_asset,
                    self.base_decimals,
                    &self.quote_asset,
                    self.quote_decimals,
                    Some(CommissionAssetValuation {
                        asset: &self.commission_asset,
                        price_in_token_a: commission_top.as_ref().map(|top| top.bid_price),
                    }),
                ) {
                    Ok(result) => (role, result),
                    Err(error) => {
                        tracing::error!(client_order_id, error = %error, "Binance fill accounting is unknown");
                        unknown(role, "cex:accounting-unknown")
                    }
                }
            }
            Err(BinanceExecutionServiceError::FailedBeforeSubmission { reason }) => {
                self.emit_binance_order_error(
                    intent,
                    role,
                    &client_order_id,
                    None,
                    "unsubmitted",
                    &reason,
                );
                tracing::warn!(
                    client_order_id,
                    reason,
                    "Binance leg failed before submission"
                );
                failed(role, "cex:unsubmitted")
            }
            Err(BinanceExecutionServiceError::Rejected { reason }) => {
                self.emit_binance_order_error(
                    intent,
                    role,
                    &client_order_id,
                    None,
                    "rejected",
                    &reason,
                );
                tracing::warn!(
                    client_order_id,
                    reason,
                    "Binance order was deterministically rejected"
                );
                failed(role, "cex:rejected")
            }
            Err(BinanceExecutionServiceError::OutcomeUnknown { reason }) => {
                self.emit_binance_order_error(
                    intent,
                    role,
                    &client_order_id,
                    None,
                    "outcome_unknown",
                    &reason,
                );
                tracing::error!(
                    client_order_id,
                    reason,
                    "Binance child outcome requires journal reconciliation"
                );
                unknown(role, "cex:child-unknown")
            }
        }
    }

    async fn execute_cex_market(
        &self,
        intent: &TradeIntent,
        role: LegRole,
        client_order_id: String,
        recovery_attempt: usize,
        target_token_b_delta_base_units: i128,
    ) -> (LegRole, LegResult) {
        let reference_price =
            match self.recovery_market_reference_price(intent, target_token_b_delta_base_units) {
                Ok(reference_price) => reference_price,
                Err(error) => {
                    let reason = format!("{error:#}");
                    self.emit_binance_order_error(
                        intent,
                        role,
                        &client_order_id,
                        Some(recovery_attempt),
                        "filtered_before_submission",
                        &reason,
                    );
                    tracing::error!(
                        client_order_id,
                        error = %error,
                        "Binance MARKET closeout has no valid filter reference price"
                    );
                    return failed(role, "cex:market-filter-rejected");
                }
            };
        let planned = match plan_market_order(
            client_order_id.clone(),
            client_order_id.clone(),
            target_token_b_delta_base_units,
            self.base_decimals,
            reference_price,
            &self.rules,
        ) {
            Ok(Some(planned)) => planned,
            Ok(None) => {
                self.emit_binance_order_error(
                    intent,
                    role,
                    &client_order_id,
                    Some(recovery_attempt),
                    "filtered_before_submission",
                    "MARKET quantity rounded below one Binance step",
                );
                return failed(role, "cex:market-sub-step-command");
            }
            Err(error) => {
                let reason = format!("{error:#}");
                self.emit_binance_order_error(
                    intent,
                    role,
                    &client_order_id,
                    Some(recovery_attempt),
                    "filtered_before_submission",
                    &reason,
                );
                tracing::error!(client_order_id, error = %error, "Binance MARKET closeout plan is invalid");
                return failed(role, "cex:market-filter-rejected");
            }
        };
        let placement = self.emit_binance_order_plan(
            intent,
            role,
            planned.target_base_units,
            planned.submitted_base_units,
            &planned.request,
            Some(recovery_attempt),
        );
        match self.binance.execute(planned.request).await {
            Ok(outcome) => {
                let commission_top = self.commission_price_top(&outcome.order);
                self.emit_binance_order_result(
                    intent,
                    role,
                    &outcome.order,
                    outcome.reconciled_after_unknown,
                    commission_top.as_ref(),
                    &placement,
                );
                match binance_leg_result(
                    &outcome.order,
                    &self.base_asset,
                    self.base_decimals,
                    &self.quote_asset,
                    self.quote_decimals,
                    Some(CommissionAssetValuation {
                        asset: &self.commission_asset,
                        price_in_token_a: commission_top.as_ref().map(|top| top.bid_price),
                    }),
                ) {
                    Ok(mut result) => {
                        if result.status == LegStatus::Failed {
                            result.venue_reference =
                                format!("cex:market-zero-fill:{}", outcome.order.order_id);
                        }
                        (role, result)
                    }
                    Err(error) => {
                        tracing::error!(client_order_id, error = %error, "Binance market fill accounting is unknown");
                        unknown(role, "cex:market-accounting-unknown")
                    }
                }
            }
            Err(BinanceExecutionServiceError::FailedBeforeSubmission { reason }) => {
                self.emit_binance_order_error(
                    intent,
                    role,
                    &client_order_id,
                    Some(recovery_attempt),
                    "unsubmitted",
                    &reason,
                );
                tracing::warn!(
                    client_order_id,
                    reason,
                    "Binance market closeout failed before submission"
                );
                failed(role, "cex:market-unsubmitted")
            }
            Err(BinanceExecutionServiceError::Rejected { reason }) => {
                self.emit_binance_order_error(
                    intent,
                    role,
                    &client_order_id,
                    Some(recovery_attempt),
                    "rejected",
                    &reason,
                );
                tracing::warn!(
                    client_order_id,
                    reason,
                    "Binance market closeout was deterministically rejected"
                );
                failed(role, "cex:market-rejected")
            }
            Err(BinanceExecutionServiceError::OutcomeUnknown { reason }) => {
                self.emit_binance_order_error(
                    intent,
                    role,
                    &client_order_id,
                    Some(recovery_attempt),
                    "outcome_unknown",
                    &reason,
                );
                tracing::error!(
                    client_order_id,
                    reason,
                    "Binance market closeout outcome requires journal reconciliation"
                );
                unknown(role, "cex:market-child-unknown")
            }
        }
    }

    fn recovery_market_reference_price(
        &self,
        intent: &TradeIntent,
        target_token_b_delta_base_units: i128,
    ) -> anyhow::Result<Decimal> {
        ensure!(
            target_token_b_delta_base_units != 0,
            "recovery target is zero"
        );
        if let Some(top) = self.market_state.fresh_binance_top(&self.rules.symbol) {
            return Ok(if target_token_b_delta_base_units > 0 {
                top.ask_price
            } else {
                top.bid_price
            });
        }
        let admission = intent
            .admission
            .as_ref()
            .context("recovery has no admission bounds")?;
        Ok(if target_token_b_delta_base_units > 0 {
            admission
                .cex_recovery_buy_limit_price
                .unwrap_or(admission.cex_recovery_limit_price)
        } else {
            admission
                .cex_recovery_sell_limit_price
                .unwrap_or(admission.cex_recovery_limit_price)
        })
    }

    fn emit_binance_order_plan(
        &self,
        intent: &TradeIntent,
        role: LegRole,
        target_base_units: i128,
        submitted_base_units: i128,
        request: &BinanceOrderRequest,
        recovery_attempt: Option<usize>,
    ) -> BinanceOrderPlacementObservation {
        let started_at = Instant::now();
        let (side, order_type, quantity, base_quantity, limit_price) = match &request.kind {
            BinanceOrderRequestKind::LimitIoc {
                side,
                quantity,
                price,
            } => (
                side.as_str(),
                "LIMIT_IOC",
                quantity.to_string(),
                Some(*quantity),
                Some(*price),
            ),
            BinanceOrderRequestKind::MarketBuyQuantity { quantity } => {
                ("BUY", "MARKET", quantity.to_string(), Some(*quantity), None)
            }
            BinanceOrderRequestKind::MarketSell { quantity } => (
                "SELL",
                "MARKET",
                quantity.to_string(),
                Some(*quantity),
                None,
            ),
            BinanceOrderRequestKind::MarketBuy { quote_quantity } => (
                "BUY",
                "MARKET_QUOTE",
                quote_quantity.to_string(),
                None,
                None,
            ),
        };
        let top = self.market_state.fresh_binance_top(&self.rules.symbol);
        let recovery_limit_counterfactual = base_quantity
            .and_then(|quantity| recovery_limit_counterfactual(role, side, quantity, top.as_ref()));
        let marketable_at_memory_top = limit_price.and_then(|limit| {
            top.as_ref().map(|top| match side {
                "SELL" => top.bid_price >= limit,
                "BUY" => top.ask_price <= limit,
                _ => false,
            })
        });
        self.telemetry.emit(
            ARBITRAGE_BINANCE_ORDER_KIND,
            serde_json::json!({
                "engine_id": self.engine_id,
                "phase": "planned",
                "plan_id": intent.plan_id,
                "operation_id": request.operation_id,
                "client_order_id": request.client_order_id,
                "role": leg_role_label(role),
                "recovery_attempt": recovery_attempt,
                "maximum_recovery_attempts":
                    recovery_attempt.map(|_| MAX_RECOVERY_ATTEMPTS),
                "symbol": request.symbol,
                "side": side,
                "order_type": order_type,
                "target_token_b_base_units": target_base_units.to_string(),
                "submitted_token_b_base_units": submitted_base_units.to_string(),
                "requested_quantity": quantity,
                "limit_price": limit_price.map(|price| price.to_string()),
                "limit_marketable_at_memory_top": marketable_at_memory_top,
                "memory_top": binance_top_payload(top.clone()),
                "recovery_limit_counterfactual": recovery_limit_counterfactual.as_ref().map(
                    recovery_limit_counterfactual_payload
                ),
            }),
        );
        BinanceOrderPlacementObservation {
            started_at,
            memory_top: top,
            recovery_attempt,
            recovery_limit_counterfactual,
        }
    }

    fn emit_binance_order_result(
        &self,
        intent: &TradeIntent,
        role: LegRole,
        order: &OrderResult,
        reconciled_after_unknown: bool,
        commission_top: Option<&FreshBinanceTopSnapshot>,
        placement: &BinanceOrderPlacementObservation,
    ) {
        let average_execution_price = (!order.executed_qty.is_zero())
            .then(|| order.cummulative_quote_qty.checked_div(order.executed_qty))
            .flatten();
        let fill_class = if order.executed_qty.is_zero() {
            "zero"
        } else if !order.orig_qty.is_zero() && order.executed_qty < order.orig_qty {
            "partial"
        } else {
            "full"
        };
        let third_asset_commission = order.commission_in(&self.commission_asset);
        let third_asset_commission_value = commission_top.and_then(|top| {
            third_asset_commission
                .checked_mul(top.bid_price)
                .map(|value| value.to_string())
        });
        let terminal_memory_top = self.market_state.fresh_binance_top(&self.rules.symbol);
        let counterfactual =
            placement
                .recovery_limit_counterfactual
                .as_ref()
                .map(|counterfactual| {
                    recovery_limit_terminal_payload(counterfactual, order, average_execution_price)
                });
        let mut payload = serde_json::json!({
            "engine_id": self.engine_id,
            "phase": "terminal",
            "plan_id": intent.plan_id,
            "operation_id": order.client_order_id,
            "client_order_id": order.client_order_id,
            "role": leg_role_label(role),
            "symbol": order.symbol,
            "side": order.side,
            "order_type": order.order_type,
            "time_in_force": order.time_in_force,
            "order_id": order.order_id,
            "status": order.status,
            "fill_class": fill_class,
            "exchange_transact_time_ms": order.transact_time,
            "exchange_order_price": order.price.to_string(),
            "original_quantity": order.orig_qty.to_string(),
            "executed_quantity": order.executed_qty.to_string(),
            "cumulative_quote_quantity": order.cummulative_quote_qty.to_string(),
            "average_execution_price": average_execution_price.map(|price| price.to_string()),
            "base_commission": order.commission_in(&self.base_asset).to_string(),
            "quote_commission": order.commission_in(&self.quote_asset).to_string(),
            "third_asset_commission_asset": self.commission_asset,
            "third_asset_commission": third_asset_commission.to_string(),
            "third_asset_commission_price_symbol": self.commission_price_symbol,
            "third_asset_commission_bid_price": commission_top.map(|top| top.bid_price.to_string()),
            "third_asset_commission_value_token_a": third_asset_commission_value,
            "third_asset_commission_price_top": binance_top_payload(commission_top.cloned()),
            "third_asset_commission_valuation_complete":
                third_asset_commission.is_zero() || commission_top.is_some(),
            "reconciled_after_unknown": reconciled_after_unknown,
            "planned_to_terminal_us": duration_us(placement.started_at.elapsed()),
            "placement_memory_top": binance_top_payload(placement.memory_top.clone()),
            "terminal_memory_top": binance_top_payload(terminal_memory_top),
            "recovery_limit_counterfactual": counterfactual,
            "fills": order.fills.iter().map(|fill| serde_json::json!({
                "price": fill.price.to_string(),
                "quantity": fill.qty.to_string(),
                "commission": fill.commission.to_string(),
                "commission_asset": fill.commission_asset,
                "trade_id": fill.trade_id,
            })).collect::<Vec<_>>(),
        });
        if let Some(object) = payload.as_object_mut() {
            object.insert(
                "recovery_attempt".to_owned(),
                serde_json::json!(placement.recovery_attempt),
            );
            object.insert(
                "maximum_recovery_attempts".to_owned(),
                serde_json::json!(placement.recovery_attempt.map(|_| MAX_RECOVERY_ATTEMPTS)),
            );
        }
        self.telemetry.emit(ARBITRAGE_BINANCE_ORDER_KIND, payload);
    }

    fn commission_price_top(&self, order: &OrderResult) -> Option<FreshBinanceTopSnapshot> {
        (!order.commission_in(&self.commission_asset).is_zero())
            .then(|| {
                self.market_state
                    .fresh_binance_top(&self.commission_price_symbol)
            })
            .flatten()
    }

    fn emit_binance_order_error(
        &self,
        intent: &TradeIntent,
        role: LegRole,
        client_order_id: &str,
        recovery_attempt: Option<usize>,
        outcome: &'static str,
        reason: &str,
    ) {
        self.telemetry.emit(
            ARBITRAGE_BINANCE_ORDER_KIND,
            serde_json::json!({
                "engine_id": self.engine_id,
                "phase": "error",
                "plan_id": intent.plan_id,
                "operation_id": client_order_id,
                "client_order_id": client_order_id,
                "role": leg_role_label(role),
                "recovery_attempt": recovery_attempt,
                "maximum_recovery_attempts":
                    recovery_attempt.map(|_| MAX_RECOVERY_ATTEMPTS),
                "symbol": self.rules.symbol,
                "outcome": outcome,
                "error_reason": bounded_telemetry_reason(reason),
            }),
        );
    }
}

impl DexRevertContext {
    fn from_request(request: &crate::dex::execution::ExactInputSwapRequest) -> Self {
        let pool_reference = match request.route {
            SwapRoute::V3 { pool, .. } => format!("{pool:#x}"),
            SwapRoute::V4 { pool_key, .. } => format!("{:#x}", pool_key.pool_id()),
        };
        Self {
            protocol: request.route.protocol().label().to_owned(),
            pool_reference,
            amount_in_base_units: request.amount_in.to_string(),
            amount_out_minimum_base_units: request.amount_out_minimum.to_string(),
            deadline_unix_seconds: request.deadline_unix_seconds,
        }
    }
}

impl LiveLegExecutor for ComposedLiveLegExecutor {
    fn execute<'a>(
        &'a self,
        intent: &'a TradeIntent,
        command: &'a CoordinatorCommand,
    ) -> LegFuture<'a> {
        Box::pin(self.execute_inner(intent, command))
    }
}

pub struct LiveTradeTask<E> {
    receiver: LatestOpportunityReceiver,
    coordinator: PaperTradeCoordinator,
    executor: Arc<E>,
    telemetry: TelemetryHandle,
    engine_id: String,
    event_sender: mpsc::UnboundedSender<PaperTradeEvent>,
    risk_limits: LiveRiskLimits,
}

#[derive(Clone, Debug)]
pub struct LiveRiskLimits {
    pub entry_stop_file: PathBuf,
    pub entry_preflight: EntryPreflightHandle,
    pub binance_symbol: String,
    pub binance_base_decimals: u8,
}

impl LiveRiskLimits {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            !self.entry_stop_file.as_os_str().is_empty(),
            "live entry-stop path is empty"
        );
        ensure!(
            !self.binance_symbol.is_empty()
                && self
                    .binance_symbol
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit()),
            "live Binance symbol is invalid"
        );
        ensure!(
            self.binance_base_decimals <= 28,
            "live Binance base decimals exceed Decimal precision"
        );
        Ok(())
    }
}

pub fn live_trade_channel<E: LiveLegExecutor>(
    path: impl AsRef<Path>,
    executor: E,
    telemetry: TelemetryHandle,
    engine_id: String,
    risk_limits: LiveRiskLimits,
) -> anyhow::Result<(
    PaperTradeHandle,
    LiveTradeTask<E>,
    mpsc::UnboundedReceiver<PaperTradeEvent>,
)> {
    risk_limits.validate()?;
    let journal_started = Instant::now();
    let coordinator = PaperTradeCoordinator::open(path)?;
    telemetry.emit(
        "runtime_journal_recovery",
        serde_json::json!({
            "engine_id": engine_id,
            "owner": "trade_saga",
            "journal_scope": "trade",
            "duration_us": duration_us(journal_started.elapsed()),
            "active_operation_count": coordinator.active_operations().len(),
            "outcome": "success",
        }),
    );
    let initial_lane = initial_execution_lane(&coordinator);
    let (handle, receiver, _discarded) = PaperTradeHandle::channel(initial_lane);
    let (event_sender, event_receiver) = mpsc::unbounded_channel();
    Ok((
        handle,
        LiveTradeTask {
            receiver,
            coordinator,
            executor: Arc::new(executor),
            telemetry,
            engine_id,
            event_sender,
            risk_limits,
        },
        event_receiver,
    ))
}

impl<E: LiveLegExecutor> LiveTradeTask<E> {
    pub async fn run(mut self) -> anyhow::Result<()> {
        self.resume_active().await?;
        while let Some(opportunity) = self.receiver.recv().await {
            let plan_id = opportunity.plan_id();
            let received_unix_us = opportunity.received_unix_us;
            let operation_existed_before_attempt = self.coordinator.operation(&plan_id).is_some();
            let live_task_started = Instant::now();
            self.emit_live_stage(
                &plan_id,
                &plan_id,
                "mailbox_wait",
                elapsed_since_unix_us(received_unix_us),
                "success",
                None,
            );
            let execution_result = self.execute(opportunity).await;
            let outcome = if execution_result.is_ok() {
                "success"
            } else {
                "failed"
            };
            self.emit_live_stage(
                &plan_id,
                &plan_id,
                "live_task_total",
                duration_us(live_task_started.elapsed()),
                outcome,
                None,
            );
            self.emit_live_stage(
                &plan_id,
                &plan_id,
                "market_to_terminal",
                elapsed_since_unix_us(received_unix_us),
                outcome,
                None,
            );
            if let Err(error) = execution_result {
                tracing::error!(plan_id, error = %error, "live arbitrage execution failed closed");
                let state = execution_failure_event_state(
                    &self.coordinator,
                    &plan_id,
                    operation_existed_before_attempt,
                );
                self.publish_event(plan_id, state, false, None)?;
            }
        }
        Ok(())
    }

    async fn resume_active(&mut self) -> anyhow::Result<()> {
        let plan_ids = self
            .coordinator
            .active_operations()
            .into_iter()
            .filter(|operation| {
                matches!(
                    operation.stage,
                    TradeStage::Prepared
                        | TradeStage::Executing
                        | TradeStage::Recovering
                        | TradeStage::UnknownExposure
                        | TradeStage::Halted
                )
            })
            .map(|operation| operation.intent.plan_id.clone())
            .collect::<Vec<_>>();
        for plan_id in plan_ids {
            while let Some(command) = self.coordinator.resume_command(&plan_id)? {
                let intent = self
                    .coordinator
                    .operation(&plan_id)
                    .context("live trade disappeared during restart")?
                    .intent
                    .clone();
                let (role, result) = self.execute_leg_timed(&intent, &command).await;
                self.coordinator.record_result(&plan_id, role, result)?;
            }
            self.drive(&plan_id, true).await?;
        }
        Ok(())
    }

    async fn execute(&mut self, opportunity: PaperOpportunity) -> anyhow::Result<()> {
        let plan_id = opportunity.plan_id();
        let preflight_started = Instant::now();
        let preflight_result = opportunity
            .validate()
            .and_then(|()| self.authorize_entry(&opportunity));
        let preflight_outcome = if preflight_result.is_ok() {
            "success"
        } else {
            "failed"
        };
        self.emit_live_stage(
            &plan_id,
            &plan_id,
            "entry_validation_preflight",
            duration_us(preflight_started.elapsed()),
            preflight_outcome,
            None,
        );
        self.emit_live_stage(
            &plan_id,
            &plan_id,
            "reservation_to_preflight_proof",
            elapsed_since_unix_us(opportunity.reservation_completed_unix_us),
            preflight_outcome,
            None,
        );
        preflight_result?;
        let intent = opportunity.intent(ExecutionMode::DexFirst);
        ensure!(
            intent.admission.is_some() && intent.dex_plan.is_some(),
            "live intent is incomplete"
        );
        let coordinator_admit_started = Instant::now();
        let admit_result = self.coordinator.admit(intent);
        self.emit_live_stage(
            &plan_id,
            &plan_id,
            "coordinator_admit_journal",
            duration_us(coordinator_admit_started.elapsed()),
            if admit_result.is_ok() {
                "success"
            } else {
                "failed"
            },
            None,
        );
        self.emit_live_stage(
            &plan_id,
            &plan_id,
            "preflight_proof_to_parent_fsync",
            duration_us(coordinator_admit_started.elapsed()),
            if admit_result.is_ok() {
                "success"
            } else {
                "failed"
            },
            None,
        );
        admit_result?;
        self.drive(&plan_id, false).await
    }

    fn authorize_entry(&mut self, opportunity: &PaperOpportunity) -> anyhow::Result<()> {
        ensure!(
            !self.risk_limits.entry_stop_file.exists(),
            "live entry stop is active"
        );
        if let Some(rejection) = self.risk_limits.entry_preflight.check(opportunity)? {
            self.telemetry.emit(
                "arbitrage_entry_preflight_rejected",
                serde_json::json!({
                    "engine_id": self.engine_id,
                    "plan_id": opportunity.plan_id(),
                    "pair_id": opportunity.pair_id,
                    "symbol": opportunity.symbol,
                    "update_id": opportunity.update_id,
                    "dex_pool_index": opportunity.dex_pool_index,
                    "dex_pool_generation": opportunity.dex_pool_generation,
                    "reason": rejection.reason,
                    "detail": rejection.detail,
                }),
            );
            anyhow::bail!("live entry preflight rejected: {}", rejection.reason);
        }
        Ok(())
    }

    async fn drive(&mut self, plan_id: &str, resumed_after_restart: bool) -> anyhow::Result<()> {
        loop {
            self.prepare_primary_cex_limit_price(plan_id)?;
            let take_commands_started = Instant::now();
            let commands_result = self.coordinator.take_commands(plan_id);
            self.emit_live_stage(
                plan_id,
                plan_id,
                "coordinator_take_commands_journal",
                duration_us(take_commands_started.elapsed()),
                if commands_result.is_ok() {
                    "success"
                } else {
                    "failed"
                },
                None,
            );
            let commands = commands_result?;
            if commands.is_empty() {
                if let Some((expected_role, command)) = self
                    .coordinator
                    .unknown_binance_reconciliation_command(plan_id)?
                {
                    let intent = self
                        .coordinator
                        .operation(plan_id)
                        .context("live trade disappeared before Binance reconciliation")?
                        .intent
                        .clone();
                    let (role, result) = self.execute_leg_timed(&intent, &command).await;
                    ensure!(
                        role == expected_role,
                        "Binance reconciliation returned the wrong leg role"
                    );
                    if result.status != LegStatus::Unknown {
                        self.coordinator.reconcile_unknown(plan_id, role, result)?;
                        continue;
                    }
                }
                if let Some((attempt, delay)) = self.coordinator.recovery_retry_wait(plan_id)? {
                    let client_order_id = recovery_client_order_id(
                        &self
                            .coordinator
                            .operation(plan_id)
                            .context("live trade disappeared before recovery retry")?
                            .intent
                            .cex_client_order_id,
                        attempt,
                    )?;
                    self.telemetry.emit(
                        ARBITRAGE_BINANCE_ORDER_KIND,
                        serde_json::json!({
                            "engine_id": self.engine_id,
                            "phase": "retry_scheduled",
                            "plan_id": plan_id,
                            "operation_id": client_order_id,
                            "client_order_id": client_order_id,
                            "role": leg_role_label(LegRole::RecoveryCex),
                            "recovery_attempt": attempt,
                            "maximum_recovery_attempts": MAX_RECOVERY_ATTEMPTS,
                            "remaining_backoff_ms":
                                delay.as_millis().min(u128::from(u64::MAX)) as u64,
                        }),
                    );
                    let backoff_started = Instant::now();
                    tokio::time::sleep(delay).await;
                    self.emit_live_stage(
                        plan_id,
                        &client_order_id,
                        "recovery_retry_backoff",
                        duration_us(backoff_started.elapsed()),
                        "success",
                        None,
                    );
                    continue;
                }
                let operation = self
                    .coordinator
                    .operation(plan_id)
                    .context("live trade disappeared from coordinator")?;
                if operation.result.is_some() {
                    let mut payload = operation.result_telemetry_payload(&self.engine_id)?;
                    let object = payload
                        .as_object_mut()
                        .context("live result payload is not an object")?;
                    object.insert("simulation".to_owned(), Value::Bool(false));
                    object.insert("includes_binance_fee".to_owned(), Value::Bool(true));
                    object.insert("includes_gas".to_owned(), Value::Bool(true));
                    object.insert("comparable_to_live".to_owned(), Value::Bool(true));
                    object.insert(
                        "resumed_after_restart".to_owned(),
                        Value::Bool(resumed_after_restart),
                    );
                    self.telemetry.emit(ARBITRAGE_RESULT_KIND, payload);
                    self.publish_event(
                        plan_id.to_owned(),
                        PaperTradeEventState::Balanced,
                        dex_filled(operation),
                        dex_settlement_log(operation),
                    )?;
                } else if matches!(
                    operation.stage,
                    TradeStage::UnknownExposure | TradeStage::Halted
                ) {
                    self.publish_event(
                        plan_id.to_owned(),
                        PaperTradeEventState::BlockedUnknown,
                        dex_filled(operation),
                        None,
                    )?;
                }
                return Ok(());
            }
            let intent = self
                .coordinator
                .operation(plan_id)
                .context("live trade disappeared after dispatch")?
                .intent
                .clone();
            let results = match commands.as_slice() {
                [command] => vec![self.execute_leg_timed(&intent, command).await],
                [first, second] => {
                    let (first, second) = tokio::join!(
                        self.execute_leg_timed(&intent, first),
                        self.execute_leg_timed(&intent, second),
                    );
                    vec![first, second]
                }
                _ => anyhow::bail!("coordinator emitted an invalid command count"),
            };
            for (role, result) in results {
                let record_started = Instant::now();
                let status = result.status;
                let record_result = self.coordinator.record_result(plan_id, role, result);
                self.emit_live_stage(
                    plan_id,
                    plan_id,
                    "coordinator_record_result_journal",
                    duration_us(record_started.elapsed()),
                    if record_result.is_ok() {
                        "success"
                    } else {
                        "failed"
                    },
                    Some((role, status)),
                );
                record_result?;
            }
        }
    }

    fn prepare_primary_cex_limit_price(&mut self, plan_id: &str) -> anyhow::Result<()> {
        let Some((direction, admission_price, client_order_id, target_base_units)) =
            self.coordinator.operation(plan_id).and_then(|operation| {
                if operation.intent.mode != ExecutionMode::DexFirst
                    || operation.cex_dispatched
                    || operation.cex_execution_limit_price.is_some()
                {
                    return None;
                }
                let dex_result = operation.dex_result.as_ref()?;
                if dex_result.status != LegStatus::Filled
                    || dex_result.token_b_delta_base_units == 0
                {
                    return None;
                }
                let bounds = operation.intent.admission.as_ref()?;
                Some((
                    operation.intent.direction,
                    bounds.cex_primary_limit_price,
                    operation.intent.cex_client_order_id.clone(),
                    dex_result.token_b_delta_base_units.unsigned_abs(),
                ))
            })
        else {
            return Ok(());
        };
        let target_quantity =
            decimal_from_base_units(target_base_units, self.risk_limits.binance_base_decimals)?;
        let selection = self
            .risk_limits
            .entry_preflight
            .favorable_primary_limit_price(
                &self.risk_limits.binance_symbol,
                direction,
                admission_price,
                target_quantity,
            );
        self.coordinator
            .select_primary_cex_limit_price(plan_id, selection.selected_price)?;
        self.telemetry.emit(
            ARBITRAGE_BINANCE_ORDER_KIND,
            serde_json::json!({
                "engine_id": self.engine_id,
                "phase": "primary_price_selection",
                "plan_id": plan_id,
                "operation_id": client_order_id,
                "client_order_id": client_order_id,
                "role": "cex",
                "symbol": self.risk_limits.binance_symbol,
                "direction": match direction {
                    crate::arbitrage::ArbitrageDirection::BuyTokenBOnDexSellOnCex => "sell",
                    crate::arbitrage::ArbitrageDirection::BuyTokenBOnCexSellOnDex => "buy",
                },
                "admission_limit_price": admission_price.to_string(),
                "observed_fresh_top_price": selection.observed_price.map(|price| price.to_string()),
                "observed_fresh_top_quantity": selection.observed_quantity.map(|quantity| quantity.to_string()),
                "target_quantity": selection.target_quantity.to_string(),
                "top_covers_target": selection.top_covers_target,
                "price_improvement_available": selection.price_improvement_available,
                "selection_reason": selection.reason,
                "selected_limit_price": selection.selected_price.to_string(),
                "improved": selection.selected_price != admission_price,
                "memory_top": binance_top_payload(selection.memory_top),
            }),
        );
        Ok(())
    }

    async fn execute_leg_timed(
        &self,
        intent: &TradeIntent,
        command: &CoordinatorCommand,
    ) -> (LegRole, LegResult) {
        let operation_id = command_operation_id(intent, command);
        let started_at = Instant::now();
        let (role, result) = self.executor.execute(intent, command).await;
        self.emit_live_stage(
            &intent.plan_id,
            &operation_id,
            "leg_execution_total",
            duration_us(started_at.elapsed()),
            leg_status_label(result.status),
            Some((role, result.status)),
        );
        (role, result)
    }

    fn emit_live_stage(
        &self,
        plan_id: &str,
        operation_id: &str,
        stage: &'static str,
        duration_us: u64,
        outcome: &str,
        leg: Option<(LegRole, LegStatus)>,
    ) {
        self.telemetry.emit(
            ARBITRAGE_EXECUTION_STAGE_KIND,
            serde_json::json!({
                "engine_id": self.engine_id,
                "venue": "orchestrator",
                "plan_id": plan_id,
                "operation_id": operation_id,
                "stage": stage,
                "duration_us": duration_us,
                "outcome": outcome,
                "leg_role": leg.map(|(role, _)| leg_role_label(role)),
                "leg_status": leg.map(|(_, status)| leg_status_label(status)),
            }),
        );
    }

    fn publish_event(
        &self,
        plan_id: String,
        state: PaperTradeEventState,
        dex_filled: bool,
        dex_settlement_log: Option<crate::chain::logs::ChainLog>,
    ) -> anyhow::Result<()> {
        self.event_sender
            .send(PaperTradeEvent {
                plan_id,
                state,
                dex_filled,
                dex_settlement_log,
                terminal_observed_at: Instant::now(),
            })
            .map_err(|_| anyhow::anyhow!("live trade event receiver is closed"))
    }
}

fn command_operation_id(intent: &TradeIntent, command: &CoordinatorCommand) -> String {
    match command {
        CoordinatorCommand::DispatchDex { operation_id, .. } => operation_id.clone(),
        CoordinatorCommand::DispatchCex {
            client_order_id, ..
        } => client_order_id.clone(),
        CoordinatorCommand::RecoverCex { attempt, .. } => {
            recovery_client_order_id(&intent.cex_client_order_id, *attempt)
                .unwrap_or_else(|_| format!("{}-recovery-{attempt}", intent.plan_id))
        }
    }
}

const fn arbitrage_direction_label(
    direction: crate::arbitrage::ArbitrageDirection,
) -> &'static str {
    match direction {
        crate::arbitrage::ArbitrageDirection::BuyTokenBOnDexSellOnCex => "dex_buy_cex_sell",
        crate::arbitrage::ArbitrageDirection::BuyTokenBOnCexSellOnDex => "cex_buy_dex_sell",
    }
}

const fn leg_role_label(role: LegRole) -> &'static str {
    match role {
        LegRole::Dex => "dex",
        LegRole::Cex => "cex",
        LegRole::RecoveryCex => "recovery_cex",
    }
}

const fn leg_status_label(status: LegStatus) -> &'static str {
    match status {
        LegStatus::Filled => "filled",
        LegStatus::Failed => "failed",
        LegStatus::Unknown => "unknown",
    }
}

fn duration_us(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

fn bounded_telemetry_reason(reason: &str) -> String {
    reason
        .chars()
        .filter(|character| !character.is_control())
        .take(1_024)
        .collect()
}

fn binance_top_payload(top: Option<FreshBinanceTopSnapshot>) -> Value {
    top.map_or(Value::Null, |top| {
        serde_json::json!({
            "update_id": top.update_id,
            "connection_generation": top.connection_generation,
            "bid_price": top.bid_price.to_string(),
            "bid_quantity": top.bid_quantity.to_string(),
            "ask_price": top.ask_price.to_string(),
            "ask_quantity": top.ask_quantity.to_string(),
            "price_age_ms": top.price_age_ms,
            "transport_silence_ms": top.transport_silence_ms,
        })
    })
}

fn recovery_limit_counterfactual(
    role: LegRole,
    side: &str,
    submitted_quantity: rust_decimal::Decimal,
    top: Option<&FreshBinanceTopSnapshot>,
) -> Option<RecoveryLimitCounterfactual> {
    if role != LegRole::RecoveryCex {
        return None;
    }
    let top = top?;
    let (price, top_quantity) = match side {
        "SELL" => (top.bid_price, top.bid_quantity),
        "BUY" => (top.ask_price, top.ask_quantity),
        _ => return None,
    };
    Some(RecoveryLimitCounterfactual {
        price,
        top_quantity,
        submitted_quantity,
        top_covers_quantity: top_quantity >= submitted_quantity,
    })
}

fn recovery_limit_counterfactual_payload(counterfactual: &RecoveryLimitCounterfactual) -> Value {
    serde_json::json!({
        "basis": "same_side_memory_top",
        "price": counterfactual.price.to_string(),
        "top_quantity": counterfactual.top_quantity.to_string(),
        "submitted_quantity": counterfactual.submitted_quantity.to_string(),
        "top_covers_quantity": counterfactual.top_covers_quantity,
        "marketable_at_snapshot": true,
    })
}

fn recovery_limit_terminal_payload(
    counterfactual: &RecoveryLimitCounterfactual,
    order: &OrderResult,
    average_execution_price: Option<rust_decimal::Decimal>,
) -> Value {
    let average_price_advantage = average_execution_price
        .and_then(|average| market_price_advantage(&order.side, average, counterfactual.price));
    let average_price_advantage_bps = average_price_advantage.and_then(|advantage| {
        advantage
            .checked_div(counterfactual.price)
            .and_then(|ratio| ratio.checked_mul(rust_decimal::Decimal::from(10_000_u32)))
    });
    let average_respects_limit = average_execution_price
        .and_then(|average| price_respects_limit(&order.side, average, counterfactual.price));
    let all_reported_fills_respect_limit = (!order.fills.is_empty()).then(|| {
        order.fills.iter().all(|fill| {
            price_respects_limit(&order.side, fill.price, counterfactual.price).unwrap_or(false)
        })
    });
    let market_filled_submitted_quantity = order.executed_qty >= counterfactual.submitted_quantity;
    let snapshot_and_market_path_success_proxy = counterfactual.top_covers_quantity
        && market_filled_submitted_quantity
        && average_respects_limit == Some(true)
        && all_reported_fills_respect_limit.unwrap_or(true);
    serde_json::json!({
        "basis": "same_side_memory_top",
        "price": counterfactual.price.to_string(),
        "top_quantity": counterfactual.top_quantity.to_string(),
        "submitted_quantity": counterfactual.submitted_quantity.to_string(),
        "top_covers_quantity": counterfactual.top_covers_quantity,
        "marketable_at_snapshot": true,
        "market_filled_submitted_quantity": market_filled_submitted_quantity,
        "market_average_price_respects_limit": average_respects_limit,
        "all_reported_market_fills_respect_limit": all_reported_fills_respect_limit,
        "market_average_price_advantage_token_a":
            average_price_advantage.map(|value| value.to_string()),
        "market_average_price_advantage_bps":
            average_price_advantage_bps.map(|value| value.to_string()),
        "snapshot_and_market_path_success_proxy": snapshot_and_market_path_success_proxy,
    })
}

/// Positive means the MARKET average was better than the hypothetical LIMIT
/// for the trader; negative means the LIMIT would have protected a better
/// price if it had filled.
fn market_price_advantage(
    side: &str,
    market_average: rust_decimal::Decimal,
    limit_price: rust_decimal::Decimal,
) -> Option<rust_decimal::Decimal> {
    match side {
        "SELL" => market_average.checked_sub(limit_price),
        "BUY" => limit_price.checked_sub(market_average),
        _ => None,
    }
}

fn price_respects_limit(
    side: &str,
    execution_price: rust_decimal::Decimal,
    limit_price: rust_decimal::Decimal,
) -> Option<bool> {
    match side {
        "SELL" => Some(execution_price >= limit_price),
        "BUY" => Some(execution_price <= limit_price),
        _ => None,
    }
}

fn elapsed_since_unix_us(received_unix_us: u64) -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_micros().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(received_unix_us)
        .saturating_sub(received_unix_us)
}

fn dex_filled(operation: &TradeOperation) -> bool {
    operation.dex_result.as_ref().is_some_and(|result| {
        result.status == LegStatus::Filled
            && (result.token_b_delta_base_units != 0 || result.token_a_delta_base_units != 0)
    })
}

fn dex_settlement_log(operation: &TradeOperation) -> Option<crate::chain::logs::ChainLog> {
    operation
        .dex_result
        .as_ref()
        .and_then(|result| result.dex_settlement_log.clone())
}

/// Keeps every Binance sell command reachable from a DEX-buy plan inside the
/// immutable WLD reservation. Favorable DEX output is real wallet inventory,
/// but it is outside this trade's hedge/recovery graph and is reconciled by the
/// next wallet snapshot and rebalance cycle.
fn cap_dex_credit_to_execution_envelope(
    direction: crate::arbitrage::ArbitrageDirection,
    planned_token_b_base_units: i128,
    result: &mut LegResult,
) -> Option<i128> {
    if direction != crate::arbitrage::ArbitrageDirection::BuyTokenBOnDexSellOnCex
        || result.token_b_delta_base_units <= planned_token_b_base_units
    {
        return None;
    }
    let surplus = result
        .token_b_delta_base_units
        .saturating_sub(planned_token_b_base_units);
    result.token_b_delta_base_units = planned_token_b_base_units;
    Some(surplus)
}

fn failed(role: LegRole, reference: &str) -> (LegRole, LegResult) {
    failed_with_gas(role, 0, reference)
}

fn failed_with_gas(role: LegRole, gas_cost: u128, reference: &str) -> (LegRole, LegResult) {
    (
        role,
        LegResult {
            status: LegStatus::Failed,
            executed_token_b_delta_base_units: None,
            token_b_delta_base_units: 0,
            token_a_delta_base_units: 0,
            third_asset_deltas: Default::default(),
            third_asset_prices_token_a: Default::default(),
            gas_cost_token_a_base_units: gas_cost,
            venue_reference: reference.to_owned(),
            dex_settlement_log: None,
        },
    )
}

fn unknown(role: LegRole, reference: &str) -> (LegRole, LegResult) {
    (
        role,
        LegResult {
            status: LegStatus::Unknown,
            executed_token_b_delta_base_units: None,
            token_b_delta_base_units: 0,
            token_a_delta_base_units: 0,
            third_asset_deltas: Default::default(),
            third_asset_prices_token_a: Default::default(),
            gas_cost_token_a_base_units: 0,
            venue_reference: reference.to_owned(),
            dex_settlement_log: None,
        },
    )
}

fn unix_seconds() -> Option<u64> {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()?
        .as_secs()
        .into()
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, fs, path::PathBuf, sync::Mutex};

    use alloy_primitives::U256;
    use rust_decimal::Decimal;

    use crate::{
        arbitrage::{
            AdmissionRiskBounds, ArbitrageDirection, CoordinatorCommand, EntryPreflightHandle,
            ExecutionMode, FreshBinanceTopSnapshot, LegResult, LegRole, LegStatus,
            PaperOpportunity, PaperTradeCoordinator, PaperTradeEventState, PaperTradeSubmitResult,
            TerminalOutcome, TradeIntent, execution_failure_event_state,
        },
        binance::ws_api::{OrderFill, OrderResult},
        dex::clmm::ClmmPool,
        execution_plan::{DexRoutePlan, DexSwapPlan},
        live_execution::{
            LegFuture, LiveLegExecutor, LiveRiskLimits, cap_dex_credit_to_execution_envelope,
            failed, failed_with_gas, live_trade_channel, unknown,
        },
        telemetry::TelemetryHandle,
    };

    struct ScriptedExecutor {
        results: Mutex<VecDeque<LegResult>>,
    }

    impl LiveLegExecutor for ScriptedExecutor {
        fn execute<'a>(
            &'a self,
            _intent: &'a TradeIntent,
            command: &'a CoordinatorCommand,
        ) -> LegFuture<'a> {
            let role = match command {
                CoordinatorCommand::DispatchDex { .. } => LegRole::Dex,
                CoordinatorCommand::DispatchCex { .. } => LegRole::Cex,
                CoordinatorCommand::RecoverCex { .. } => LegRole::RecoveryCex,
            };
            let result = self.results.lock().unwrap().pop_front().unwrap();
            Box::pin(async move { (role, result) })
        }
    }

    fn opportunity() -> PaperOpportunity {
        PaperOpportunity {
            source_revision: "test-revision".to_owned(),
            pair_id: "world-chain-usdc-wld".to_owned(),
            symbol: "WLDUSDC".to_owned(),
            update_id: 7,
            received_unix_us: 1_800_000_000_000_000,
            reservation_completed_unix_us: 1_800_000_000_000_001,
            direction: ArbitrageDirection::BuyTokenBOnDexSellOnCex,
            dex_pool_index: 0,
            dex_pool_generation: 1,
            token_b_base_units: 100,
            token_b_step_base_units: 1,
            cost_token_a_base_units: 1_000,
            proceeds_token_a_base_units: 1_030,
            admission: AdmissionRiskBounds {
                opportunity_threshold_met: true,
                opportunity_threshold_bps: 20,
                depth_source: None,
                depth_age_ms: None,
                depth_update_delta: None,
                top_matches: None,
                top_mismatch_reason: None,
                execution_slippage_bps: 15,
                cex_primary_limit_price: Decimal::new(101, 2),
                cex_primary_top_quantity: Decimal::from(100),
                cex_recovery_limit_price: Decimal::ONE,
                cex_recovery_sell_limit_price: Some(Decimal::new(99, 2)),
                cex_recovery_buy_limit_price: Some(Decimal::new(101, 2)),
                recovery_quote_token_a_base_units: 1_000,
                recovery_sell_quote_token_a_base_units: 990,
                recovery_buy_quote_token_a_base_units: 1_010,
                maximum_recovery_loss_token_a_base_units: 0,
                maximum_fee_per_gas_wei: 2_500_000,
                gas_conversion_price_token_a: Decimal::from(3_000),
                maximum_gas_cost_token_a_base_units: 0,
                bounded_profit_token_a_base_units: 0,
            },
            dex_plan: DexSwapPlan {
                route: DexRoutePlan::UniswapV3 {
                    router: "0x1111111111111111111111111111111111111111".to_owned(),
                    pool_address: "0x2222222222222222222222222222222222222222".to_owned(),
                    fee_pips: 3_000,
                },
                token_in: "0x3333333333333333333333333333333333333333".to_owned(),
                token_out: "0x4444444444444444444444444444444444444444".to_owned(),
                amount_in_base_units: 1_000,
                amount_out_minimum_base_units: 100,
                deadline_unix_seconds: 1_800_000_030,
            },
        }
    }

    #[test]
    fn reservation_timestamp_does_not_change_plan_identity() {
        let mut first = opportunity();
        let plan_id = first.plan_id();
        first.reservation_completed_unix_us += 99;
        assert_eq!(first.plan_id(), plan_id);
    }

    fn result(token_b: i128, token_a: i128, gas: u128, reference: &str) -> LegResult {
        LegResult {
            status: LegStatus::Filled,
            executed_token_b_delta_base_units: Some(token_b),
            token_b_delta_base_units: token_b,
            token_a_delta_base_units: token_a,
            third_asset_deltas: Default::default(),
            third_asset_prices_token_a: Default::default(),
            gas_cost_token_a_base_units: gas,
            venue_reference: reference.to_owned(),
            dex_settlement_log: None,
        }
    }

    fn risk_limits(stop_file: std::path::PathBuf) -> LiveRiskLimits {
        LiveRiskLimits {
            entry_stop_file: stop_file,
            entry_preflight: default_preflight(),
            binance_symbol: "WLDUSDC".to_owned(),
            binance_base_decimals: 18,
        }
    }

    fn default_preflight() -> EntryPreflightHandle {
        let handle = EntryPreflightHandle::default();
        let quote = preflight_quote(Decimal::new(101, 2), Decimal::new(102, 2), 7);
        handle.update_quote(&quote);
        configure_fresh_dex(&handle, preflight_pool(U256::ONE << 96, 0));
        handle
    }

    fn configure_fresh_dex(handle: &EntryPreflightHandle, pool: ClmmPool) {
        handle.configure_max_transport_silence("WLDUSDC", 30_000);
        handle.configure_dex_max_head_age(30_000);
        handle.update_dex_head(std::time::Instant::now());
        handle.update_dex_pool(0, 1, 0, 0, preflight_curves(&pool));
    }

    fn preflight_pool(sqrt_price_x96: U256, tick: i32) -> ClmmPool {
        ClmmPool::new(0, 1, sqrt_price_x96, tick, 1_000_000_000_000_000_000).unwrap()
    }

    fn preflight_curves(pool: &ClmmPool) -> [crate::dex::clmm::PreparedQuoteCurve; 2] {
        [
            pool.prepare_exact_input_curve_bounded(true, U256::from(1_000_000_u64))
                .unwrap(),
            pool.prepare_exact_input_curve_bounded(false, U256::from(1_000_000_u64))
                .unwrap(),
        ]
    }

    #[test]
    fn opportunity_identity_changes_with_the_dex_generation() {
        let first = opportunity();
        let mut second = first.clone();
        second.dex_pool_generation += 1;

        assert_ne!(first.plan_id(), second.plan_id());
        assert_ne!(
            first.intent(ExecutionMode::DexFirst).cex_client_order_id,
            second.intent(ExecutionMode::DexFirst).cex_client_order_id
        );
    }

    #[test]
    fn recovery_limit_counterfactual_uses_same_side_top_and_scores_market_fill() {
        let top = FreshBinanceTopSnapshot {
            update_id: 9,
            connection_generation: 3,
            bid_price: Decimal::new(100, 2),
            bid_quantity: Decimal::from(12),
            ask_price: Decimal::new(101, 2),
            ask_quantity: Decimal::from(8),
            price_age_ms: 4,
            transport_silence_ms: 6,
        };
        let sell = super::recovery_limit_counterfactual(
            LegRole::RecoveryCex,
            "SELL",
            Decimal::from(10),
            Some(&top),
        )
        .unwrap();
        assert_eq!(sell.price, Decimal::ONE);
        assert!(sell.top_covers_quantity);

        let order = OrderResult {
            symbol: "WLDUSDC".to_owned(),
            order_id: 1,
            client_order_id: "rustarbrecovery".to_owned(),
            transact_time: Some(1_800_000_000_000),
            price: Decimal::ZERO,
            orig_qty: Decimal::from(10),
            executed_qty: Decimal::from(10),
            orig_quote_order_qty: Decimal::ZERO,
            cummulative_quote_qty: Decimal::new(1002, 2),
            status: "FILLED".to_owned(),
            time_in_force: "GTC".to_owned(),
            order_type: "MARKET".to_owned(),
            side: "SELL".to_owned(),
            fills: vec![OrderFill {
                price: Decimal::new(1002, 3),
                qty: Decimal::from(10),
                commission: Decimal::ZERO,
                commission_asset: "BNB".to_owned(),
                trade_id: 2,
            }],
        };
        let payload =
            super::recovery_limit_terminal_payload(&sell, &order, Some(Decimal::new(1002, 3)));
        assert_eq!(payload["market_average_price_respects_limit"], true);
        assert_eq!(payload["all_reported_market_fills_respect_limit"], true);
        assert_eq!(payload["snapshot_and_market_path_success_proxy"], true);
        assert_eq!(
            super::market_price_advantage("SELL", Decimal::new(998, 3), Decimal::ONE),
            Some(Decimal::new(-2, 3))
        );

        let buy = super::recovery_limit_counterfactual(
            LegRole::RecoveryCex,
            "BUY",
            Decimal::from(10),
            Some(&top),
        )
        .unwrap();
        assert_eq!(buy.price, Decimal::new(101, 2));
        assert!(!buy.top_covers_quantity);
        assert_eq!(
            super::market_price_advantage("BUY", Decimal::new(1005, 3), buy.price),
            Some(Decimal::new(5, 3))
        );
        assert!(
            super::recovery_limit_counterfactual(LegRole::Cex, "SELL", Decimal::ONE, Some(&top))
                .is_none()
        );
    }

    #[test]
    fn duplicate_of_a_terminal_operation_is_rejected_without_unknown_exposure() {
        let journal = std::env::temp_dir().join(format!(
            "poly-bot-live-terminal-duplicate-{}-{}.jsonl",
            std::process::id(),
            std::thread::current().name().unwrap_or("thread")
        ));
        let _ = fs::remove_file(&journal);
        let opportunity = opportunity();
        let plan_id = opportunity.plan_id();
        let mut coordinator = PaperTradeCoordinator::open(&journal).unwrap();
        coordinator
            .admit(opportunity.intent(ExecutionMode::DexFirst))
            .unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        coordinator
            .record_result(
                &plan_id,
                LegRole::Dex,
                failed(LegRole::Dex, "dex:reverted").1,
            )
            .unwrap();
        assert!(coordinator.take_commands(&plan_id).unwrap().is_empty());

        assert_eq!(
            execution_failure_event_state(&coordinator, &plan_id, true),
            PaperTradeEventState::RejectedUnsubmitted
        );

        drop(coordinator);
        fs::remove_file(journal).unwrap();
    }

    fn preflight_quote(bid: Decimal, ask: Decimal, update_id: u64) -> crate::state::TopOfBook {
        preflight_quote_with_quantities(
            bid,
            Decimal::new(100, 0),
            ask,
            Decimal::new(100, 0),
            update_id,
        )
    }

    fn preflight_quote_with_quantities(
        bid: Decimal,
        bid_quantity: Decimal,
        ask: Decimal,
        ask_quantity: Decimal,
        update_id: u64,
    ) -> crate::state::TopOfBook {
        crate::state::TopOfBook::new(
            std::sync::Arc::from("WLDUSDC"),
            update_id,
            bid,
            bid_quantity,
            ask,
            ask_quantity,
            None,
            None,
            std::time::Instant::now(),
            1_800_000_000_000_000,
            1,
        )
        .unwrap()
    }

    #[test]
    fn entry_preflight_rejects_price_drift_after_admission() {
        let handle = default_preflight();
        let quote = preflight_quote(Decimal::new(99, 2), Decimal::new(101, 2), 8);
        handle.update_quote(&quote);

        let rejection = handle.check(&opportunity()).unwrap().unwrap();

        assert_eq!(rejection.reason, "preflight_spread_below_threshold");
    }

    #[test]
    fn post_dex_primary_price_only_moves_in_the_favorable_direction() {
        let handle = default_preflight();
        handle.update_quote(&preflight_quote(
            Decimal::new(102, 2),
            Decimal::new(103, 2),
            8,
        ));

        let sell = handle.favorable_primary_limit_price(
            "WLDUSDC",
            ArbitrageDirection::BuyTokenBOnDexSellOnCex,
            Decimal::new(101, 2),
            Decimal::from(100),
        );
        assert_eq!(sell.selected_price, Decimal::new(102, 2));
        assert_eq!(
            sell.memory_top.as_ref().unwrap().bid_price,
            Decimal::new(102, 2)
        );
        assert_eq!(sell.top_covers_target, Some(true));
        assert_eq!(sell.reason, "fresh_top_price_improved_with_full_coverage");

        let buy = handle.favorable_primary_limit_price(
            "WLDUSDC",
            ArbitrageDirection::BuyTokenBOnCexSellOnDex,
            Decimal::new(104, 2),
            Decimal::from(100),
        );
        assert_eq!(buy.selected_price, Decimal::new(103, 2));
        assert_eq!(
            buy.memory_top.as_ref().unwrap().ask_price,
            Decimal::new(103, 2)
        );
        assert_eq!(buy.top_covers_target, Some(true));

        handle.update_quote(&preflight_quote(
            Decimal::new(99, 2),
            Decimal::new(105, 2),
            9,
        ));
        assert_eq!(
            handle
                .favorable_primary_limit_price(
                    "WLDUSDC",
                    ArbitrageDirection::BuyTokenBOnDexSellOnCex,
                    Decimal::new(101, 2),
                    Decimal::from(100),
                )
                .selected_price,
            Decimal::new(101, 2)
        );
        assert_eq!(
            handle
                .favorable_primary_limit_price(
                    "WLDUSDC",
                    ArbitrageDirection::BuyTokenBOnCexSellOnDex,
                    Decimal::new(104, 2),
                    Decimal::from(100),
                )
                .selected_price,
            Decimal::new(104, 2)
        );
    }

    #[test]
    fn post_dex_primary_price_keeps_admission_boundary_when_favorable_top_is_thin() {
        let handle = default_preflight();
        handle.update_quote(&preflight_quote_with_quantities(
            Decimal::new(102, 2),
            Decimal::from(99),
            Decimal::new(103, 2),
            Decimal::from(99),
            8,
        ));

        let sell = handle.favorable_primary_limit_price(
            "WLDUSDC",
            ArbitrageDirection::BuyTokenBOnDexSellOnCex,
            Decimal::new(101, 2),
            Decimal::from(100),
        );
        assert_eq!(sell.selected_price, Decimal::new(101, 2));
        assert!(sell.price_improvement_available);
        assert_eq!(sell.top_covers_target, Some(false));
        assert_eq!(sell.reason, "fresh_top_quantity_below_target");

        let buy = handle.favorable_primary_limit_price(
            "WLDUSDC",
            ArbitrageDirection::BuyTokenBOnCexSellOnDex,
            Decimal::new(104, 2),
            Decimal::from(100),
        );
        assert_eq!(buy.selected_price, Decimal::new(104, 2));
        assert!(buy.price_improvement_available);
        assert_eq!(buy.top_covers_target, Some(false));
        assert_eq!(buy.reason, "fresh_top_quantity_below_target");
    }

    #[test]
    fn entry_preflight_uses_transport_liveness_not_unchanged_price_age() {
        let handle = EntryPreflightHandle::default();
        let mut quote = preflight_quote(Decimal::new(101, 2), Decimal::new(102, 2), 8);
        quote.received_at = std::time::Instant::now() - std::time::Duration::from_millis(1_001);
        handle.update_quote(&quote);
        handle.configure_max_transport_silence("WLDUSDC", 1_000);
        handle.configure_dex_max_head_age(30_000);
        handle.update_dex_head(std::time::Instant::now());
        handle.update_dex_pool(
            0,
            1,
            0,
            0,
            preflight_curves(&preflight_pool(U256::ONE << 96, 0)),
        );

        let rejection = handle.check(&opportunity()).unwrap().unwrap();
        assert_eq!(rejection.reason, "preflight_price_not_fresh");

        handle.record_transport_activity(
            "WLDUSDC",
            quote.connection_generation,
            std::time::Instant::now(),
        );

        assert!(handle.check(&opportunity()).unwrap().is_none());

        handle.on_feed_disconnected("WLDUSDC", quote.connection_generation);
        let rejection = handle.check(&opportunity()).unwrap().unwrap();
        assert_eq!(rejection.reason, "preflight_price_not_fresh");
    }

    #[test]
    fn entry_preflight_requires_a_fresh_dex_head() {
        let handle = EntryPreflightHandle::default();
        handle.update_quote(&preflight_quote(
            Decimal::new(101, 2),
            Decimal::new(102, 2),
            8,
        ));
        handle.configure_max_transport_silence("WLDUSDC", 30_000);
        handle.configure_dex_max_head_age(1_000);
        handle.update_dex_head(std::time::Instant::now() - std::time::Duration::from_millis(1_001));
        handle.update_dex_pool(
            0,
            1,
            0,
            0,
            preflight_curves(&preflight_pool(U256::ONE << 96, 0)),
        );

        let rejection = handle.check(&opportunity()).unwrap().unwrap();

        assert_eq!(rejection.reason, "preflight_price_not_fresh");
    }

    #[test]
    fn entry_preflight_requotes_the_current_dex_pool() {
        let handle = default_preflight();
        handle.update_dex_pool(
            0,
            2,
            0,
            0,
            preflight_curves(&preflight_pool(U256::ONE << 95, -13_864)),
        );

        let rejection = handle.check(&opportunity()).unwrap().unwrap();

        assert_eq!(rejection.reason, "preflight_spread_below_threshold");
    }

    #[test]
    fn entry_preflight_requotes_cex_buy_dex_sell_direction() {
        let handle = default_preflight();
        handle.update_quote(&preflight_quote(
            Decimal::new(98, 2),
            Decimal::new(99, 2),
            8,
        ));
        let mut reverse = opportunity();
        reverse.direction = ArbitrageDirection::BuyTokenBOnCexSellOnDex;
        reverse.admission.cex_primary_limit_price = Decimal::new(99, 2);
        reverse.dex_plan.token_in = "0x4444444444444444444444444444444444444444".to_owned();
        reverse.dex_plan.token_out = "0x3333333333333333333333333333333333333333".to_owned();

        assert!(handle.check(&reverse).unwrap().is_none());

        handle.update_quote(&preflight_quote(Decimal::ONE, Decimal::new(101, 2), 9));
        let rejection = handle.check(&reverse).unwrap().unwrap();
        assert_eq!(rejection.reason, "preflight_spread_below_threshold");
    }

    #[test]
    fn entry_preflight_does_not_gate_on_top_quantity() {
        let handle = default_preflight();
        let quote = preflight_quote_with_quantities(
            Decimal::new(101, 2),
            Decimal::ONE,
            Decimal::new(102, 2),
            Decimal::from(100),
            8,
        );
        handle.update_quote(&quote);

        assert!(handle.check(&opportunity()).unwrap().is_none());
    }

    #[test]
    fn entry_preflight_does_not_compare_admission_update_identity() {
        let handle = default_preflight();
        let mut quote = preflight_quote(Decimal::new(101, 2), Decimal::new(102, 2), 1);
        quote.connection_generation = 2;
        handle.update_quote(&quote);

        assert!(handle.check(&opportunity()).unwrap().is_none());
    }

    #[test]
    fn entry_preflight_skips_duplicate_requote_when_prices_are_unchanged() {
        let handle = default_preflight();
        let pool = preflight_pool(U256::ONE << 96, 0);
        handle.update_dex_pool(
            0,
            1,
            0,
            0,
            [
                pool.prepare_exact_input_curve_bounded(true, U256::ONE)
                    .unwrap(),
                pool.prepare_exact_input_curve_bounded(false, U256::ONE)
                    .unwrap(),
            ],
        );

        assert!(handle.check(&opportunity()).unwrap().is_none());
    }

    #[test]
    fn entry_preflight_does_not_gate_on_the_transaction_deadline() {
        let handle = default_preflight();
        let mut expired = opportunity();
        expired.dex_plan.deadline_unix_seconds = 1;

        assert!(handle.check(&expired).unwrap().is_none());
    }

    #[test]
    fn child_failures_preserve_mutation_certainty() {
        assert_eq!(
            failed(LegRole::Dex, "dex:preflight").1.status,
            crate::arbitrage::LegStatus::Failed
        );
        assert_eq!(
            unknown(LegRole::Cex, "cex:unknown").1.status,
            crate::arbitrage::LegStatus::Unknown
        );
        assert_eq!(
            failed_with_gas(LegRole::Dex, 123, "dex:reverted")
                .1
                .gas_cost_token_a_base_units,
            123
        );
    }

    #[test]
    fn favorable_dex_buy_surplus_stays_outside_the_cex_execution_envelope() {
        let mut dex_result = result(125, -1_000, 5, "dex:surplus");

        let surplus = cap_dex_credit_to_execution_envelope(
            ArbitrageDirection::BuyTokenBOnDexSellOnCex,
            100,
            &mut dex_result,
        );

        assert_eq!(surplus, Some(25));
        assert_eq!(dex_result.token_b_delta_base_units, 100);

        let mut dex_sell = result(-100, 1_025, 5, "dex:sell");
        assert_eq!(
            cap_dex_credit_to_execution_envelope(
                ArbitrageDirection::BuyTokenBOnCexSellOnDex,
                100,
                &mut dex_sell,
            ),
            None
        );
        assert_eq!(dex_sell.token_b_delta_base_units, -100);
    }

    #[test]
    fn live_entry_controls_require_an_entry_stop_path() {
        let valid = LiveRiskLimits {
            entry_stop_file: "/tmp/arb-bot-entry.stop".into(),
            entry_preflight: default_preflight(),
            binance_symbol: "WLDUSDC".to_owned(),
            binance_base_decimals: 18,
        };
        valid.validate().unwrap();
        let mut invalid = valid;
        invalid.entry_stop_file = PathBuf::new();
        assert!(invalid.validate().is_err());
    }

    #[tokio::test]
    async fn live_execution_mailbox_keeps_only_the_latest_pending_opportunity() {
        let journal = std::env::temp_dir().join(format!(
            "poly-bot-live-latest-mailbox-{}-{}.jsonl",
            std::process::id(),
            std::thread::current().name().unwrap_or("thread")
        ));
        let stop_file = journal.with_extension("stop");
        let _ = fs::remove_file(&journal);
        let _ = fs::remove_file(&stop_file);
        let executor = ScriptedExecutor {
            results: Mutex::new(VecDeque::new()),
        };
        let (handle, mut task, _events) = live_trade_channel(
            &journal,
            executor,
            TelemetryHandle::disconnected_test_handle(),
            "test-engine".to_owned(),
            risk_limits(stop_file),
        )
        .unwrap();

        let first = opportunity();
        assert!(matches!(
            handle.try_submit(first.clone()),
            PaperTradeSubmitResult::Accepted
        ));
        assert_eq!(
            task.receiver.recv().await.unwrap().plan_id(),
            first.plan_id()
        );

        let mut second = opportunity();
        second.received_unix_us += 1;
        second.update_id += 1;
        assert!(matches!(
            handle.try_submit(second.clone()),
            PaperTradeSubmitResult::Accepted
        ));

        let mut latest = opportunity();
        latest.received_unix_us += 2;
        latest.update_id += 2;
        let superseded = match handle.try_submit(latest.clone()) {
            PaperTradeSubmitResult::Superseded(opportunity) => opportunity,
            other => panic!("expected a superseded opportunity, got {other:?}"),
        };
        assert_eq!(superseded.plan_id(), second.plan_id());

        assert!(handle.finish(PaperTradeEventState::Balanced).is_none());
        assert_eq!(
            task.receiver.recv().await.unwrap().plan_id(),
            latest.plan_id()
        );

        drop(handle);
        drop(task);
        fs::remove_file(journal).unwrap();
    }

    #[tokio::test]
    async fn unknown_outcome_releases_the_lane_and_preserves_pending_work() {
        let journal = std::env::temp_dir().join(format!(
            "poly-bot-live-unknown-mailbox-{}-{}.jsonl",
            std::process::id(),
            std::thread::current().name().unwrap_or("thread")
        ));
        let stop_file = journal.with_extension("stop");
        let _ = fs::remove_file(&journal);
        let _ = fs::remove_file(&stop_file);
        let executor = ScriptedExecutor {
            results: Mutex::new(VecDeque::new()),
        };
        let (handle, mut task, _events) = live_trade_channel(
            &journal,
            executor,
            TelemetryHandle::disconnected_test_handle(),
            "test-engine".to_owned(),
            risk_limits(stop_file),
        )
        .unwrap();

        let first = opportunity();
        assert!(matches!(
            handle.try_submit(first),
            PaperTradeSubmitResult::Accepted
        ));
        task.receiver.recv().await.unwrap();

        let mut pending = opportunity();
        pending.received_unix_us += 1;
        pending.update_id += 1;
        assert!(matches!(
            handle.try_submit(pending.clone()),
            PaperTradeSubmitResult::Accepted
        ));
        assert!(
            handle
                .finish(PaperTradeEventState::BlockedUnknown)
                .is_none()
        );
        assert_eq!(
            task.receiver.recv().await.unwrap().plan_id(),
            pending.plan_id()
        );

        let mut next = opportunity();
        next.received_unix_us += 2;
        next.update_id += 2;
        assert!(matches!(
            handle.try_submit(next.clone()),
            PaperTradeSubmitResult::Accepted
        ));
        assert!(handle.finish(PaperTradeEventState::Balanced).is_none());
        assert_eq!(
            task.receiver.recv().await.unwrap().plan_id(),
            next.plan_id()
        );

        drop(handle);
        drop(task);
        fs::remove_file(journal).unwrap();
    }

    #[tokio::test]
    async fn dex_settlement_releases_the_lane_and_keeps_pending_work_for_preflight() {
        let journal = std::env::temp_dir().join(format!(
            "poly-bot-live-settlement-mailbox-{}-{}.jsonl",
            std::process::id(),
            std::thread::current().name().unwrap_or("thread")
        ));
        let stop_file = journal.with_extension("stop");
        let _ = fs::remove_file(&journal);
        let _ = fs::remove_file(&stop_file);
        let executor = ScriptedExecutor {
            results: Mutex::new(VecDeque::new()),
        };
        let (handle, mut task, _events) = live_trade_channel(
            &journal,
            executor,
            TelemetryHandle::disconnected_test_handle(),
            "test-engine".to_owned(),
            risk_limits(stop_file),
        )
        .unwrap();

        assert!(matches!(
            handle.try_submit(opportunity()),
            PaperTradeSubmitResult::Accepted
        ));
        task.receiver.recv().await.unwrap();

        let mut stale = opportunity();
        stale.received_unix_us += 1;
        stale.update_id += 1;
        assert!(matches!(
            handle.try_submit(stale.clone()),
            PaperTradeSubmitResult::Accepted
        ));
        assert!(handle.finish(PaperTradeEventState::Balanced).is_none());
        assert_eq!(
            task.receiver.recv().await.unwrap().plan_id(),
            stale.plan_id()
        );

        drop(handle);
        drop(task);
        fs::remove_file(journal).unwrap();
    }

    #[tokio::test]
    async fn composed_task_recovers_only_the_actual_residual_and_finishes_balanced() {
        let journal = std::env::temp_dir().join(format!(
            "poly-bot-live-composed-{}-{}.jsonl",
            std::process::id(),
            std::thread::current().name().unwrap_or("thread")
        ));
        let stop_file = journal.with_extension("stop");
        let _ = fs::remove_file(&journal);
        let _ = fs::remove_file(&stop_file);
        let opportunity = opportunity();
        let plan_id = opportunity.plan_id();
        let executor = ScriptedExecutor {
            results: Mutex::new(VecDeque::from([
                result(97, -1_000, 5, "dex:filled"),
                result(-90, 950, 0, "cex:partial"),
                result(-7, 80, 0, "cex:recovery"),
            ])),
        };
        let (_handle, mut task, _events) = live_trade_channel(
            &journal,
            executor,
            TelemetryHandle::disconnected_test_handle(),
            "test-engine".to_owned(),
            risk_limits(stop_file),
        )
        .unwrap();

        task.execute(opportunity).await.unwrap();
        let operation = task.coordinator.operation(&plan_id).unwrap();
        assert_eq!(operation.recovery_results.len(), 1);
        assert_eq!(
            operation
                .result
                .as_ref()
                .unwrap()
                .token_b_residual_base_units,
            0
        );
        assert_eq!(
            operation.result.as_ref().unwrap().outcome,
            TerminalOutcome::BalancedProfit
        );
        drop(task);
        fs::remove_file(journal).unwrap();
    }

    #[tokio::test]
    async fn composed_cex_reject_recovers_the_proven_dex_exposure() {
        let journal = std::env::temp_dir().join(format!(
            "poly-bot-live-cex-reject-{}-{}.jsonl",
            std::process::id(),
            std::thread::current().name().unwrap_or("thread")
        ));
        let stop_file = journal.with_extension("stop");
        let _ = fs::remove_file(&journal);
        let _ = fs::remove_file(&stop_file);
        let opportunity = opportunity();
        let plan_id = opportunity.plan_id();
        let executor = ScriptedExecutor {
            results: Mutex::new(VecDeque::from([
                result(100, -1_000, 5, "dex:filled-before-reject"),
                failed(LegRole::Cex, "cex:rejected").1,
                result(-100, 990, 0, "cex:recovery-after-reject"),
            ])),
        };
        let (_handle, mut task, _events) = live_trade_channel(
            &journal,
            executor,
            TelemetryHandle::disconnected_test_handle(),
            "test-engine".to_owned(),
            risk_limits(stop_file),
        )
        .unwrap();

        task.execute(opportunity).await.unwrap();
        let operation = task.coordinator.operation(&plan_id).unwrap();
        assert_eq!(operation.recovery_results.len(), 1);
        assert_eq!(
            operation
                .result
                .as_ref()
                .unwrap()
                .token_b_residual_base_units,
            0
        );
        assert_eq!(
            operation
                .result
                .as_ref()
                .unwrap()
                .realized_profit_token_a_base_units,
            -15
        );
        drop(task);
        fs::remove_file(journal).unwrap();
    }

    #[tokio::test]
    async fn composed_task_waits_and_retries_a_proven_zero_fill_recovery() {
        let journal = std::env::temp_dir().join(format!(
            "poly-bot-live-recovery-retry-{}-{}.jsonl",
            std::process::id(),
            std::thread::current().name().unwrap_or("thread")
        ));
        let stop_file = journal.with_extension("stop");
        let _ = fs::remove_file(&journal);
        let _ = fs::remove_file(&stop_file);
        let opportunity = opportunity();
        let plan_id = opportunity.plan_id();
        let executor = ScriptedExecutor {
            results: Mutex::new(VecDeque::from([
                result(100, -1_000, 5, "dex:filled"),
                failed(LegRole::Cex, "cex:primary-rejected").1,
                failed(LegRole::RecoveryCex, "cex:market-unsubmitted").1,
                result(-100, 990, 0, "cex:recovery-r2"),
            ])),
        };
        let (_handle, mut task, _events) = live_trade_channel(
            &journal,
            executor,
            TelemetryHandle::disconnected_test_handle(),
            "test-engine".to_owned(),
            risk_limits(stop_file),
        )
        .unwrap();

        task.execute(opportunity).await.unwrap();
        let operation = task.coordinator.operation(&plan_id).unwrap();
        assert_eq!(operation.recovery_results.len(), 2);
        assert_eq!(
            operation
                .result
                .as_ref()
                .unwrap()
                .token_b_residual_base_units,
            0
        );
        assert!(operation.recovery_retry_not_before_unix_ms.is_none());

        drop(task);
        fs::remove_file(journal).unwrap();
    }

    #[tokio::test]
    async fn composed_dex_revert_finishes_without_dispatching_cex() {
        let journal = std::env::temp_dir().join(format!(
            "poly-bot-live-dex-revert-{}-{}.jsonl",
            std::process::id(),
            std::thread::current().name().unwrap_or("thread")
        ));
        let stop_file = journal.with_extension("stop");
        let _ = fs::remove_file(&journal);
        let _ = fs::remove_file(&stop_file);
        let opportunity = opportunity();
        let plan_id = opportunity.plan_id();
        let executor = ScriptedExecutor {
            results: Mutex::new(VecDeque::from([failed_with_gas(
                LegRole::Dex,
                5,
                "dex:reverted",
            )
            .1])),
        };
        let (_handle, mut task, _events) = live_trade_channel(
            &journal,
            executor,
            TelemetryHandle::disconnected_test_handle(),
            "test-engine".to_owned(),
            risk_limits(stop_file),
        )
        .unwrap();

        task.execute(opportunity).await.unwrap();
        let operation = task.coordinator.operation(&plan_id).unwrap();
        assert!(!operation.cex_dispatched);
        assert!(operation.recovery_results.is_empty());
        assert_eq!(
            operation
                .result
                .as_ref()
                .unwrap()
                .realized_profit_token_a_base_units,
            -5
        );
        drop(task);
        fs::remove_file(journal).unwrap();
    }

    #[tokio::test]
    async fn composed_cex_unknown_reconciles_before_market_recovery() {
        let journal = std::env::temp_dir().join(format!(
            "poly-bot-live-cex-unknown-{}-{}.jsonl",
            std::process::id(),
            std::thread::current().name().unwrap_or("thread")
        ));
        let stop_file = journal.with_extension("stop");
        let _ = fs::remove_file(&journal);
        let _ = fs::remove_file(&stop_file);
        let opportunity = opportunity();
        let plan_id = opportunity.plan_id();
        let executor = ScriptedExecutor {
            results: Mutex::new(VecDeque::from([
                result(100, -1_000, 5, "dex:filled-before-unknown"),
                unknown(LegRole::Cex, "cex:placement-unknown").1,
                failed(LegRole::Cex, "cex:confirmed-absent").1,
                result(-100, 990, 0, "cex:market-recovery"),
            ])),
        };
        let (_handle, mut task, mut events) = live_trade_channel(
            &journal,
            executor,
            TelemetryHandle::disconnected_test_handle(),
            "test-engine".to_owned(),
            risk_limits(stop_file),
        )
        .unwrap();

        task.execute(opportunity).await.unwrap();
        let operation = task.coordinator.operation(&plan_id).unwrap();
        assert_eq!(operation.stage, crate::arbitrage::TradeStage::BalancedLoss);
        assert_eq!(operation.recovery_results.len(), 1);
        assert_eq!(
            operation
                .result
                .as_ref()
                .unwrap()
                .token_b_residual_base_units,
            0
        );
        assert_eq!(
            events.try_recv().unwrap().state,
            crate::arbitrage::PaperTradeEventState::Balanced
        );
        drop(task);
        fs::remove_file(journal).unwrap();
    }

    #[tokio::test]
    async fn restart_resumes_journaled_cex_without_replaying_dex() {
        let journal = std::env::temp_dir().join(format!(
            "poly-bot-live-restart-cex-{}-{}.jsonl",
            std::process::id(),
            std::thread::current().name().unwrap_or("thread")
        ));
        let stop_file = journal.with_extension("stop");
        let _ = fs::remove_file(&journal);
        let _ = fs::remove_file(&stop_file);
        let opportunity = opportunity();
        let plan_id = opportunity.plan_id();
        let mut coordinator = PaperTradeCoordinator::open(&journal).unwrap();
        coordinator
            .admit(opportunity.intent(ExecutionMode::DexFirst))
            .unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        coordinator
            .record_result(
                &plan_id,
                LegRole::Dex,
                result(100, -1_000, 5, "dex:before-cex-restart"),
            )
            .unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        drop(coordinator);

        let executor = ScriptedExecutor {
            results: Mutex::new(VecDeque::from([result(
                -100,
                1_030,
                0,
                "cex:after-restart",
            )])),
        };
        let (_handle, mut task, _events) = live_trade_channel(
            &journal,
            executor,
            TelemetryHandle::disconnected_test_handle(),
            "test-engine".to_owned(),
            risk_limits(stop_file),
        )
        .unwrap();

        task.resume_active().await.unwrap();
        let operation = task.coordinator.operation(&plan_id).unwrap();
        assert_eq!(operation.recovery_results.len(), 0);
        assert_eq!(
            operation
                .result
                .as_ref()
                .unwrap()
                .token_b_residual_base_units,
            0
        );
        drop(task);
        fs::remove_file(journal).unwrap();
    }

    #[tokio::test]
    async fn restart_resumes_only_the_journaled_recovery() {
        let journal = std::env::temp_dir().join(format!(
            "poly-bot-live-restart-recovery-{}-{}.jsonl",
            std::process::id(),
            std::thread::current().name().unwrap_or("thread")
        ));
        let stop_file = journal.with_extension("stop");
        let _ = fs::remove_file(&journal);
        let _ = fs::remove_file(&stop_file);
        let opportunity = opportunity();
        let plan_id = opportunity.plan_id();
        let mut coordinator = PaperTradeCoordinator::open(&journal).unwrap();
        coordinator
            .admit(opportunity.intent(ExecutionMode::DexFirst))
            .unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        coordinator
            .record_result(
                &plan_id,
                LegRole::Dex,
                result(97, -1_000, 5, "dex:before-recovery-restart"),
            )
            .unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        coordinator
            .record_result(
                &plan_id,
                LegRole::Cex,
                result(-90, 950, 0, "cex:partial-before-restart"),
            )
            .unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        drop(coordinator);

        let executor = ScriptedExecutor {
            results: Mutex::new(VecDeque::from([result(
                -7,
                80,
                0,
                "cex:recovery-after-restart",
            )])),
        };
        let (_handle, mut task, _events) = live_trade_channel(
            &journal,
            executor,
            TelemetryHandle::disconnected_test_handle(),
            "test-engine".to_owned(),
            risk_limits(stop_file),
        )
        .unwrap();

        task.resume_active().await.unwrap();
        let operation = task.coordinator.operation(&plan_id).unwrap();
        assert_eq!(operation.recovery_results.len(), 1);
        assert_eq!(
            operation
                .result
                .as_ref()
                .unwrap()
                .token_b_residual_base_units,
            0
        );
        drop(task);
        fs::remove_file(journal).unwrap();
    }

    #[tokio::test]
    async fn entry_stop_does_not_block_restart_recovery() {
        let journal = std::env::temp_dir().join(format!(
            "poly-bot-live-stop-recovery-{}-{}.jsonl",
            std::process::id(),
            std::thread::current().name().unwrap_or("thread")
        ));
        let stop_file = journal.with_extension("stop");
        let _ = fs::remove_file(&journal);
        let _ = fs::remove_file(&stop_file);
        let opportunity = opportunity();
        let plan_id = opportunity.plan_id();
        let mut coordinator = PaperTradeCoordinator::open(&journal).unwrap();
        coordinator
            .admit(opportunity.intent(ExecutionMode::DexFirst))
            .unwrap();
        coordinator.take_commands(&plan_id).unwrap();
        drop(coordinator);
        fs::write(&stop_file, b"stop new entries\n").unwrap();

        let executor = ScriptedExecutor {
            results: Mutex::new(VecDeque::from([
                result(100, -1_000, 5, "dex:restart"),
                result(-100, 1_030, 0, "cex:restart"),
            ])),
        };
        let (_handle, mut task, _events) = live_trade_channel(
            &journal,
            executor,
            TelemetryHandle::disconnected_test_handle(),
            "test-engine".to_owned(),
            risk_limits(stop_file.clone()),
        )
        .unwrap();

        task.resume_active().await.unwrap();
        assert_eq!(
            task.coordinator
                .operation(&plan_id)
                .unwrap()
                .result
                .as_ref()
                .unwrap()
                .outcome,
            TerminalOutcome::BalancedProfit
        );
        drop(task);
        fs::remove_file(stop_file).unwrap();
        fs::remove_file(journal).unwrap();
    }
}

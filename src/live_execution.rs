use std::{
    collections::VecDeque,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use anyhow::{Context, ensure};
use serde_json::Value;
use tokio::sync::mpsc;

use crate::{
    arbitrage::{
        CoordinatorCommand, ExecutionMode, LegResult, LegRole, LegStatus, PaperOpportunity,
        PaperTradeCoordinator, PaperTradeEvent, PaperTradeEventState, PaperTradeHandle,
        TradeIntent, TradeStage,
    },
    binance::{
        account::SymbolRules,
        execution::{BinanceExecutionService, BinanceExecutionServiceError},
        order_plan::{plan_limit_ioc, recovery_client_order_id},
    },
    dex::execution::{DexExecutionService, DexExecutionServiceError},
    execution_accounting::{binance_leg_result, dex_leg_result, native_gas_to_token_a_base_units},
    telemetry::{ARBITRAGE_RESULT_KIND, TelemetryHandle},
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
}

impl ComposedLiveLegExecutor {
    pub fn new(
        dex: DexExecutionService,
        binance: BinanceExecutionService,
        rules: SymbolRules,
        base_asset: String,
        base_decimals: u8,
        quote_asset: String,
        quote_decimals: u8,
    ) -> anyhow::Result<Self> {
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
        Ok(Self {
            dex,
            binance,
            rules,
            base_asset,
            base_decimals,
            quote_asset,
            quote_decimals,
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
                let request = match plan
                    .execution_request(operation_id.clone(), bounds.maximum_fee_per_gas_wei)
                {
                    Ok(request) => request,
                    Err(error) => {
                        tracing::error!(operation_id, error = %error, "journaled DEX plan is invalid");
                        return failed(role, "dex:invalid-plan");
                    }
                };
                match self.dex.execute(request).await {
                    Ok(outcome) => {
                        let gas = match native_gas_to_token_a_base_units(
                            outcome.gas_used,
                            outcome.effective_gas_price,
                            bounds.gas_conversion_price_token_a,
                            self.quote_decimals,
                        ) {
                            Ok(gas) => gas,
                            Err(error) => {
                                tracing::error!(operation_id, error = %error, "DEX gas accounting is unknown");
                                return unknown(role, "dex:accounting-unknown");
                            }
                        };
                        if gas > bounds.maximum_gas_cost_token_a_base_units {
                            tracing::error!(
                                operation_id,
                                actual_gas_token_a_base_units = gas,
                                admitted_gas_token_a_base_units =
                                    bounds.maximum_gas_cost_token_a_base_units,
                                "DEX gas exceeded admission bound after execution"
                            );
                        }
                        match dex_leg_result(intent.direction, outcome, gas) {
                            Ok(result) => (role, result),
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
                        gas_used,
                        effective_gas_price,
                        reason,
                    }) => {
                        let gas = match native_gas_to_token_a_base_units(
                            gas_used,
                            effective_gas_price,
                            bounds.gas_conversion_price_token_a,
                            self.quote_decimals,
                        ) {
                            Ok(gas) => gas,
                            Err(error) => {
                                tracing::error!(operation_id, error = %error, "reverted DEX gas accounting is unknown");
                                return unknown(role, "dex:revert-accounting-unknown");
                            }
                        };
                        tracing::warn!(
                            operation_id,
                            transaction_hash = %transaction_hash,
                            reason,
                            gas_cost_token_a_base_units = gas,
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
                self.execute_cex(
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
                limit_price,
            } => {
                let client_order_id =
                    match recovery_client_order_id(&intent.cex_client_order_id, *attempt) {
                        Ok(value) => value,
                        Err(error) => {
                            tracing::error!(error = %error, "recovery client order id is invalid");
                            return failed(LegRole::RecoveryCex, "cex:invalid-recovery-id");
                        }
                    };
                self.execute_cex(
                    LegRole::RecoveryCex,
                    client_order_id,
                    *target_token_b_delta_base_units,
                    *limit_price,
                )
                .await
            }
        }
    }

    async fn execute_cex(
        &self,
        role: LegRole,
        client_order_id: String,
        target_token_b_delta_base_units: i128,
        limit_price: Option<rust_decimal::Decimal>,
    ) -> (LegRole, LegResult) {
        let Some(limit_price) = limit_price else {
            return failed(role, "cex:missing-limit");
        };
        let planned = match plan_limit_ioc(
            client_order_id.clone(),
            client_order_id.clone(),
            target_token_b_delta_base_units,
            self.base_decimals,
            limit_price,
            &self.rules,
        ) {
            Ok(Some(planned)) => planned,
            Ok(None) => return failed(role, "cex:sub-step-command"),
            Err(error) => {
                tracing::error!(client_order_id, error = %error, "bounded Binance IOC plan is invalid");
                return failed(role, "cex:invalid-plan");
            }
        };
        match self.binance.execute(planned.request).await {
            Ok(outcome) => match binance_leg_result(
                &outcome.order,
                &self.base_asset,
                self.base_decimals,
                &self.quote_asset,
                self.quote_decimals,
            ) {
                Ok(result) => (role, result),
                Err(error) => {
                    tracing::error!(client_order_id, error = %error, "Binance fill accounting is unknown");
                    unknown(role, "cex:accounting-unknown")
                }
            },
            Err(BinanceExecutionServiceError::FailedBeforeSubmission { reason }) => {
                tracing::warn!(
                    client_order_id,
                    reason,
                    "Binance leg failed before submission"
                );
                failed(role, "cex:unsubmitted")
            }
            Err(BinanceExecutionServiceError::Rejected { reason }) => {
                tracing::warn!(
                    client_order_id,
                    reason,
                    "Binance order was deterministically rejected"
                );
                failed(role, "cex:rejected")
            }
            Err(BinanceExecutionServiceError::OutcomeUnknown { reason }) => {
                tracing::error!(
                    client_order_id,
                    reason,
                    "Binance child outcome requires journal reconciliation"
                );
                unknown(role, "cex:child-unknown")
            }
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
    receiver: mpsc::Receiver<PaperOpportunity>,
    coordinator: PaperTradeCoordinator,
    executor: Arc<E>,
    telemetry: TelemetryHandle,
    engine_id: String,
    event_sender: mpsc::UnboundedSender<PaperTradeEvent>,
    risk_limits: LiveRiskLimits,
    entry_times: VecDeque<Instant>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveRiskLimits {
    pub maximum_plan_cost_token_a_base_units: u128,
    pub maximum_recovery_loss_token_a_base_units: u128,
    pub maximum_cumulative_loss_token_a_base_units: u128,
    pub maximum_cumulative_recovery_loss_token_a_base_units: u128,
    pub maximum_total_entries: usize,
    pub maximum_entries_per_minute: usize,
    pub entry_stop_file: PathBuf,
}

impl LiveRiskLimits {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.maximum_plan_cost_token_a_base_units > 0,
            "live maximum plan cost is zero"
        );
        ensure!(
            self.maximum_recovery_loss_token_a_base_units > 0,
            "live maximum recovery loss is zero"
        );
        ensure!(
            self.maximum_cumulative_loss_token_a_base_units > 0,
            "live maximum cumulative loss is zero"
        );
        ensure!(
            self.maximum_cumulative_recovery_loss_token_a_base_units > 0,
            "live maximum cumulative recovery loss is zero"
        );
        ensure!(
            self.maximum_total_entries > 0,
            "live maximum total entries is zero"
        );
        ensure!(
            self.maximum_entries_per_minute > 0,
            "live maximum entry rate is zero"
        );
        ensure!(
            !self.entry_stop_file.as_os_str().is_empty(),
            "live entry-stop path is empty"
        );
        Ok(())
    }
}

pub fn live_trade_channel<E: LiveLegExecutor>(
    path: impl AsRef<Path>,
    capacity: usize,
    executor: E,
    telemetry: TelemetryHandle,
    engine_id: String,
    risk_limits: LiveRiskLimits,
) -> anyhow::Result<(
    PaperTradeHandle,
    LiveTradeTask<E>,
    mpsc::UnboundedReceiver<PaperTradeEvent>,
)> {
    ensure!(capacity > 0, "live trade channel capacity is zero");
    risk_limits.validate()?;
    let coordinator = PaperTradeCoordinator::open(path)?;
    let entry_times =
        std::iter::repeat_n(Instant::now(), risk_limits.maximum_entries_per_minute).collect();
    let (handle, receiver, _dropped) = PaperTradeHandle::channel(capacity);
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
            entry_times,
        },
        event_receiver,
    ))
}

impl<E: LiveLegExecutor> LiveTradeTask<E> {
    pub async fn run(mut self) -> anyhow::Result<()> {
        self.resume_active().await?;
        while let Some(opportunity) = self.receiver.recv().await {
            let plan_id = opportunity.plan_id();
            if let Err(error) = self.execute(opportunity).await {
                tracing::error!(plan_id, error = %error, "live arbitrage execution failed closed");
                let state = self.coordinator.operation(&plan_id).map_or(
                    PaperTradeEventState::RejectedUnsubmitted,
                    |operation| {
                        if operation.dex_dispatched || operation.cex_dispatched {
                            PaperTradeEventState::BlockedUnknown
                        } else {
                            PaperTradeEventState::RejectedUnsubmitted
                        }
                    },
                );
                self.publish_event(plan_id, state)?;
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
                    TradeStage::Prepared | TradeStage::Executing | TradeStage::Recovering
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
                let (role, result) = self.executor.execute(&intent, &command).await;
                self.coordinator.record_result(&plan_id, role, result)?;
            }
            self.drive(&plan_id).await?;
        }
        Ok(())
    }

    async fn execute(&mut self, opportunity: PaperOpportunity) -> anyhow::Result<()> {
        opportunity.validate()?;
        self.authorize_entry(&opportunity)?;
        let intent = opportunity.intent(ExecutionMode::DexFirst);
        let plan_id = intent.plan_id.clone();
        ensure!(
            intent.admission.is_some() && intent.dex_plan.is_some(),
            "live intent is incomplete"
        );
        self.coordinator.admit(intent)?;
        self.entry_times.push_back(Instant::now());
        self.drive(&plan_id).await
    }

    fn authorize_entry(&mut self, opportunity: &PaperOpportunity) -> anyhow::Result<()> {
        ensure!(
            !self.risk_limits.entry_stop_file.exists(),
            "live entry stop is active"
        );
        ensure!(
            self.coordinator.admitted_operation_count() < self.risk_limits.maximum_total_entries,
            "live durable total-entry limit is exhausted"
        );
        ensure!(
            opportunity.cost_token_a_base_units > 0
                && opportunity.cost_token_a_base_units.unsigned_abs()
                    <= self.risk_limits.maximum_plan_cost_token_a_base_units,
            "live plan exceeds the maximum token-A cost"
        );
        let admission = &opportunity.admission;
        ensure!(
            admission.maximum_recovery_loss_token_a_base_units
                <= self.risk_limits.maximum_recovery_loss_token_a_base_units,
            "live plan exceeds the maximum recovery loss"
        );
        let (cumulative_loss, cumulative_recovery_loss) =
            self.coordinator.cumulative_terminal_risk()?;
        let plan_loss_bound = admission
            .maximum_recovery_loss_token_a_base_units
            .checked_add(admission.maximum_gas_cost_token_a_base_units)
            .context("live plan loss bound overflow")?;
        ensure!(
            projected_risk_within_limit(
                cumulative_loss,
                plan_loss_bound,
                self.risk_limits.maximum_cumulative_loss_token_a_base_units,
            ),
            "live plan would exceed the cumulative loss limit"
        );
        ensure!(
            projected_risk_within_limit(
                cumulative_recovery_loss,
                admission.maximum_recovery_loss_token_a_base_units,
                self.risk_limits
                    .maximum_cumulative_recovery_loss_token_a_base_units,
            ),
            "live plan would exceed the cumulative recovery-loss limit"
        );
        let now = Instant::now();
        while self
            .entry_times
            .front()
            .is_some_and(|entry| now.duration_since(*entry) >= Duration::from_secs(60))
        {
            self.entry_times.pop_front();
        }
        ensure!(
            self.entry_times.len() < self.risk_limits.maximum_entries_per_minute,
            "live entry-rate limit is exhausted"
        );
        Ok(())
    }

    async fn drive(&mut self, plan_id: &str) -> anyhow::Result<()> {
        loop {
            let commands = self.coordinator.take_commands(plan_id)?;
            if commands.is_empty() {
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
                    self.telemetry.emit(ARBITRAGE_RESULT_KIND, payload);
                    self.publish_event(plan_id.to_owned(), PaperTradeEventState::Balanced)?;
                } else if matches!(
                    operation.stage,
                    TradeStage::UnknownExposure | TradeStage::Halted
                ) {
                    self.publish_event(plan_id.to_owned(), PaperTradeEventState::BlockedUnknown)?;
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
                [command] => vec![self.executor.execute(&intent, command).await],
                [first, second] => {
                    let (first, second) = tokio::join!(
                        self.executor.execute(&intent, first),
                        self.executor.execute(&intent, second),
                    );
                    vec![first, second]
                }
                _ => anyhow::bail!("coordinator emitted an invalid command count"),
            };
            for (role, result) in results {
                self.coordinator.record_result(plan_id, role, result)?;
            }
        }
    }

    fn publish_event(&self, plan_id: String, state: PaperTradeEventState) -> anyhow::Result<()> {
        self.event_sender
            .send(PaperTradeEvent { plan_id, state })
            .map_err(|_| anyhow::anyhow!("live trade event receiver is closed"))
    }
}

fn projected_risk_within_limit(current: u128, additional: u128, limit: u128) -> bool {
    current
        .checked_add(additional)
        .is_some_and(|projected| projected <= limit)
}

fn failed(role: LegRole, reference: &str) -> (LegRole, LegResult) {
    failed_with_gas(role, 0, reference)
}

fn failed_with_gas(role: LegRole, gas_cost: u128, reference: &str) -> (LegRole, LegResult) {
    (
        role,
        LegResult {
            status: LegStatus::Failed,
            token_b_delta_base_units: 0,
            token_a_delta_base_units: 0,
            gas_cost_token_a_base_units: gas_cost,
            venue_reference: reference.to_owned(),
        },
    )
}

fn unknown(role: LegRole, reference: &str) -> (LegRole, LegResult) {
    (
        role,
        LegResult {
            status: LegStatus::Unknown,
            token_b_delta_base_units: 0,
            token_a_delta_base_units: 0,
            gas_cost_token_a_base_units: 0,
            venue_reference: reference.to_owned(),
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
    use std::{collections::VecDeque, fs, sync::Mutex};

    use rust_decimal::Decimal;

    use crate::{
        arbitrage::{
            AdmissionRiskBounds, ArbitrageDirection, CoordinatorCommand, ExecutionMode, LegResult,
            LegRole, LegStatus, PaperOpportunity, PaperTradeCoordinator, TerminalOutcome,
            TradeIntent,
        },
        execution_plan::{DexRoutePlan, DexSwapPlan},
        live_execution::{
            LegFuture, LiveLegExecutor, LiveRiskLimits, failed, failed_with_gas,
            live_trade_channel, projected_risk_within_limit, unknown,
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
            update_id: 7,
            received_unix_us: 1_800_000_000_000_000,
            direction: ArbitrageDirection::BuyTokenBOnDexSellOnCex,
            token_b_base_units: 100,
            token_b_step_base_units: 1,
            cost_token_a_base_units: 1_000,
            proceeds_token_a_base_units: 1_030,
            admission: AdmissionRiskBounds {
                execution_slippage_bps: 15,
                cex_primary_limit_price: Decimal::ONE,
                cex_recovery_limit_price: Decimal::ONE,
                cex_recovery_sell_limit_price: Some(Decimal::new(99, 2)),
                cex_recovery_buy_limit_price: Some(Decimal::new(101, 2)),
                recovery_quote_token_a_base_units: 1_000,
                recovery_sell_quote_token_a_base_units: 990,
                recovery_buy_quote_token_a_base_units: 1_010,
                maximum_recovery_loss_token_a_base_units: 10,
                maximum_fee_per_gas_wei: 2_500_000,
                gas_conversion_price_token_a: Decimal::from(3_000),
                maximum_gas_cost_token_a_base_units: 5,
                bounded_profit_token_a_base_units: 15,
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

    fn result(token_b: i128, token_a: i128, gas: u128, reference: &str) -> LegResult {
        LegResult {
            status: LegStatus::Filled,
            token_b_delta_base_units: token_b,
            token_a_delta_base_units: token_a,
            gas_cost_token_a_base_units: gas,
            venue_reference: reference.to_owned(),
        }
    }

    fn risk_limits(stop_file: std::path::PathBuf) -> LiveRiskLimits {
        LiveRiskLimits {
            maximum_plan_cost_token_a_base_units: 2_000,
            maximum_recovery_loss_token_a_base_units: 20,
            maximum_cumulative_loss_token_a_base_units: 100,
            maximum_cumulative_recovery_loss_token_a_base_units: 100,
            maximum_total_entries: 10,
            maximum_entries_per_minute: 2,
            entry_stop_file: stop_file,
        }
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
    fn live_risk_limits_fail_closed_on_zero_or_missing_controls() {
        let valid = LiveRiskLimits {
            maximum_plan_cost_token_a_base_units: 20_000_000,
            maximum_recovery_loss_token_a_base_units: 1_000_000,
            maximum_cumulative_loss_token_a_base_units: 10_000_000,
            maximum_cumulative_recovery_loss_token_a_base_units: 5_000_000,
            maximum_total_entries: 100,
            maximum_entries_per_minute: 3,
            entry_stop_file: "/tmp/arb-bot-entry.stop".into(),
        };
        valid.validate().unwrap();
        let mut invalid = valid;
        invalid.maximum_entries_per_minute = 0;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn projected_risk_must_fit_before_a_new_entry_is_admitted() {
        assert!(projected_risk_within_limit(90, 10, 100));
        assert!(!projected_risk_within_limit(91, 10, 100));
        assert!(!projected_risk_within_limit(u128::MAX, 1, u128::MAX));
    }

    #[tokio::test]
    async fn durable_total_entry_limit_survives_restart() {
        let journal = std::env::temp_dir().join(format!(
            "poly-bot-live-total-cap-{}-{}.jsonl",
            std::process::id(),
            std::thread::current().name().unwrap_or("thread")
        ));
        let stop_file = journal.with_extension("stop");
        let _ = fs::remove_file(&journal);
        let _ = fs::remove_file(&stop_file);
        let opportunity = opportunity();
        let mut limits = risk_limits(stop_file.clone());
        limits.maximum_total_entries = 1;
        let executor = ScriptedExecutor {
            results: Mutex::new(VecDeque::from([
                result(100, -1_000, 5, "dex:total-cap"),
                result(-100, 1_030, 0, "cex:total-cap"),
            ])),
        };
        let (_handle, mut task, _events) = live_trade_channel(
            &journal,
            4,
            executor,
            TelemetryHandle::disconnected_test_handle(),
            "test-engine".to_owned(),
            limits.clone(),
        )
        .unwrap();
        task.entry_times.clear();
        task.execute(opportunity.clone()).await.unwrap();
        drop(task);

        let executor = ScriptedExecutor {
            results: Mutex::new(VecDeque::new()),
        };
        let (_handle, mut task, _events) = live_trade_channel(
            &journal,
            4,
            executor,
            TelemetryHandle::disconnected_test_handle(),
            "test-engine".to_owned(),
            limits,
        )
        .unwrap();
        task.entry_times.clear();
        assert!(
            task.authorize_entry(&opportunity)
                .unwrap_err()
                .to_string()
                .contains("durable total-entry limit")
        );
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
            4,
            executor,
            TelemetryHandle::disconnected_test_handle(),
            "test-engine".to_owned(),
            risk_limits(stop_file),
        )
        .unwrap();
        task.entry_times.clear();

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
            4,
            executor,
            TelemetryHandle::disconnected_test_handle(),
            "test-engine".to_owned(),
            risk_limits(stop_file),
        )
        .unwrap();
        task.entry_times.clear();

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
            4,
            executor,
            TelemetryHandle::disconnected_test_handle(),
            "test-engine".to_owned(),
            risk_limits(stop_file),
        )
        .unwrap();
        task.entry_times.clear();

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
    async fn composed_cex_unknown_blocks_without_guessing_a_recovery() {
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
            ])),
        };
        let (_handle, mut task, mut events) = live_trade_channel(
            &journal,
            4,
            executor,
            TelemetryHandle::disconnected_test_handle(),
            "test-engine".to_owned(),
            risk_limits(stop_file),
        )
        .unwrap();
        task.entry_times.clear();

        task.execute(opportunity).await.unwrap();
        let operation = task.coordinator.operation(&plan_id).unwrap();
        assert_eq!(
            operation.stage,
            crate::arbitrage::TradeStage::UnknownExposure
        );
        assert!(operation.recovery_results.is_empty());
        assert_eq!(
            events.try_recv().unwrap().state,
            crate::arbitrage::PaperTradeEventState::BlockedUnknown
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
            4,
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
            4,
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
            4,
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

use std::{
    path::PathBuf,
    thread::JoinHandle,
    time::{Duration, Instant},
};

use anyhow::{Context, ensure};
use rust_decimal::Decimal;
use tokio::sync::{mpsc, oneshot};

use crate::binance::{
    order_journal::{BinanceOrderIntent, BinanceOrderJournal, BinanceOrderProgress},
    user_data::MultiplexedBinanceWsApi,
    ws_api::{OrderResult, WsApiError},
};
use crate::telemetry::ExecutionLatencyTelemetry;

const RECONCILIATION_ATTEMPTS: usize = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BinanceOrderRequestKind {
    MarketBuy {
        quote_quantity: Decimal,
    },
    MarketBuyQuantity {
        quantity: Decimal,
    },
    MarketSell {
        quantity: Decimal,
    },
    LimitIoc {
        side: String,
        quantity: Decimal,
        price: Decimal,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceOrderRequest {
    pub operation_id: String,
    pub client_order_id: String,
    pub symbol: String,
    pub kind: BinanceOrderRequestKind,
}

impl BinanceOrderRequest {
    pub fn validate(&self) -> anyhow::Result<()> {
        let valid_operation_namespace =
            self.operation_id.starts_with("rustval") || self.operation_id.starts_with("rustarb");
        let valid_client_namespace = self.client_order_id.starts_with("rustval")
            || self.client_order_id.starts_with("rustarb");
        ensure!(
            valid_operation_namespace,
            "Binance operation id is outside the Rust-owned namespace"
        );
        ensure!(
            valid_client_namespace && self.client_order_id.len() <= 36,
            "Binance client order id is outside the Rust-owned namespace"
        );
        ensure!(
            !self.symbol.is_empty()
                && self
                    .symbol
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit()),
            "Binance symbol is invalid"
        );
        match &self.kind {
            BinanceOrderRequestKind::MarketBuy { quote_quantity } => {
                ensure!(
                    *quote_quantity > Decimal::ZERO,
                    "market BUY quote quantity must be positive"
                );
            }
            BinanceOrderRequestKind::MarketBuyQuantity { quantity } => {
                ensure!(
                    *quantity > Decimal::ZERO,
                    "market BUY quantity must be positive"
                );
            }
            BinanceOrderRequestKind::MarketSell { quantity } => {
                ensure!(
                    *quantity > Decimal::ZERO,
                    "market SELL quantity must be positive"
                );
            }
            BinanceOrderRequestKind::LimitIoc {
                side,
                quantity,
                price,
            } => {
                ensure!(
                    matches!(side.as_str(), "BUY" | "SELL"),
                    "invalid LIMIT side"
                );
                ensure!(
                    *quantity > Decimal::ZERO && *price > Decimal::ZERO,
                    "LIMIT quantity and price must be positive"
                );
            }
        }
        Ok(())
    }

    fn intent(&self) -> BinanceOrderIntent {
        let (side, order_type, quantity, quote_order_quantity, limit_price) = match &self.kind {
            BinanceOrderRequestKind::MarketBuy { quote_quantity } => (
                "BUY".to_owned(),
                "MARKET".to_owned(),
                None,
                Some(decimal_string(*quote_quantity)),
                None,
            ),
            BinanceOrderRequestKind::MarketBuyQuantity { quantity } => (
                "BUY".to_owned(),
                "MARKET".to_owned(),
                Some(decimal_string(*quantity)),
                None,
                None,
            ),
            BinanceOrderRequestKind::MarketSell { quantity } => (
                "SELL".to_owned(),
                "MARKET".to_owned(),
                Some(decimal_string(*quantity)),
                None,
                None,
            ),
            BinanceOrderRequestKind::LimitIoc {
                side,
                quantity,
                price,
            } => (
                side.clone(),
                "LIMIT".to_owned(),
                Some(decimal_string(*quantity)),
                None,
                Some(decimal_string(*price)),
            ),
        };
        BinanceOrderIntent {
            operation_id: self.operation_id.clone(),
            client_order_id: self.client_order_id.clone(),
            symbol: self.symbol.clone(),
            side,
            order_type,
            quantity,
            quote_order_quantity,
            limit_price,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BinanceOrderOutcome {
    pub order: OrderResult,
    pub reconciled_after_unknown: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BinanceExecutionServiceError {
    FailedBeforeSubmission { reason: String },
    Rejected { reason: String },
    OutcomeUnknown { reason: String },
}

impl std::fmt::Display for BinanceExecutionServiceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FailedBeforeSubmission { reason } => {
                write!(formatter, "Binance rejected before submission: {reason}")
            }
            Self::Rejected { reason } => write!(formatter, "Binance order rejected: {reason}"),
            Self::OutcomeUnknown { reason } => {
                write!(formatter, "Binance outcome unknown: {reason}")
            }
        }
    }
}

impl std::error::Error for BinanceExecutionServiceError {}

struct BinanceExecutor {
    client: MultiplexedBinanceWsApi,
    journal: BinanceOrderJournal,
    latency_telemetry: Option<ExecutionLatencyTelemetry>,
}

impl BinanceExecutor {
    async fn initialize(
        client: MultiplexedBinanceWsApi,
        journal_path: PathBuf,
        latency_telemetry: Option<ExecutionLatencyTelemetry>,
    ) -> anyhow::Result<Self> {
        let journal = BinanceOrderJournal::open(journal_path)?;
        let mut executor = Self {
            client,
            journal,
            latency_telemetry,
        };
        executor.reconcile_startup().await?;
        Ok(executor)
    }

    fn emit_latency_stage(
        &self,
        operation_id: &str,
        stage: &'static str,
        started_at: Instant,
        outcome: &'static str,
    ) {
        if let Some(telemetry) = &self.latency_telemetry {
            telemetry.emit_stage(
                "binance",
                operation_id,
                stage,
                duration_us(started_at.elapsed()),
                outcome,
            );
        }
    }

    async fn reconcile_startup(&mut self) -> anyhow::Result<()> {
        let active = self
            .journal
            .active_operations()
            .into_iter()
            .map(|operation| {
                (
                    operation.intent.operation_id.clone(),
                    operation.intent.symbol.clone(),
                    operation.intent.client_order_id.clone(),
                    matches!(
                        operation.progress,
                        BinanceOrderProgress::IntentRecorded
                            | BinanceOrderProgress::OutcomeUnknown { .. }
                    ),
                )
            })
            .collect::<Vec<_>>();
        for (operation_id, symbol, client_order_id, confirm_absent) in active {
            let Some(order) = self
                .query_after_reconnect(&operation_id, &symbol, &client_order_id, confirm_absent)
                .await
                .with_context(|| {
                    format!("unresolved Binance order {client_order_id}; journal remains blocked")
                })?
            else {
                tracing::warn!(
                    operation_id,
                    client_order_id,
                    "Binance startup reconciliation proved that the journaled order was absent"
                );
                continue;
            };
            self.record_order(&client_order_id, &order)?;
        }
        ensure!(
            self.journal.active_operations().is_empty(),
            "Binance journal still has a non-terminal order after reconciliation"
        );
        Ok(())
    }

    async fn execute(
        &mut self,
        request: BinanceOrderRequest,
    ) -> anyhow::Result<BinanceOrderOutcome> {
        request.validate()?;
        let request_intent = request.intent();
        let client_order_id = request.client_order_id.clone();
        let symbol = request.symbol.clone();
        if let Some(existing) = self.journal.operations().get(&client_order_id).cloned() {
            ensure!(
                existing.intent == request_intent,
                "journaled Binance order does not match the replayed request"
            );
            match existing.progress {
                BinanceOrderProgress::Terminal {
                    order: Some(order), ..
                } => {
                    validate_response(&request, &order)?;
                    return Ok(BinanceOrderOutcome {
                        order,
                        reconciled_after_unknown: true,
                    });
                }
                BinanceOrderProgress::Terminal { order: None, .. } => {
                    let order = self
                        .query_after_reconnect(
                            &request.operation_id,
                            &symbol,
                            &client_order_id,
                            false,
                        )
                        .await?
                        .context("terminal Binance order disappeared during reconciliation")?;
                    validate_response(&request, &order)?;
                    ensure!(
                        terminal_status(&order.status),
                        "replayed Binance order is not terminal"
                    );
                    return Ok(BinanceOrderOutcome {
                        order,
                        reconciled_after_unknown: true,
                    });
                }
                BinanceOrderProgress::Rejected {
                    status,
                    code,
                    reason,
                } => {
                    anyhow::bail!(
                        "journaled Binance order was rejected with HTTP status {status}, code {code}: {reason}"
                    )
                }
                BinanceOrderProgress::IntentRecorded
                | BinanceOrderProgress::OutcomeUnknown { .. } => {
                    let order = self
                        .query_after_reconnect(
                            &request.operation_id,
                            &symbol,
                            &client_order_id,
                            true,
                        )
                        .await?
                        .context("journaled Binance order was confirmed absent")?;
                    validate_response(&request, &order)?;
                    self.record_order(&client_order_id, &order)?;
                    return Ok(BinanceOrderOutcome {
                        order,
                        reconciled_after_unknown: true,
                    });
                }
                BinanceOrderProgress::Submitted { .. } => {
                    let order = self
                        .query_after_reconnect(
                            &request.operation_id,
                            &symbol,
                            &client_order_id,
                            false,
                        )
                        .await?
                        .context("submitted Binance order disappeared during reconciliation")?;
                    validate_response(&request, &order)?;
                    self.record_order(&client_order_id, &order)?;
                    return Ok(BinanceOrderOutcome {
                        order,
                        reconciled_after_unknown: true,
                    });
                }
            }
        }
        let journal_intent_started = Instant::now();
        let journal_intent_result = self.journal.record_intent(request_intent);
        self.emit_latency_stage(
            &request.operation_id,
            "intent_journal",
            journal_intent_started,
            if journal_intent_result.is_ok() {
                "success"
            } else {
                "failed"
            },
        );
        journal_intent_result?;
        let placement_started = Instant::now();
        let result = match &request.kind {
            BinanceOrderRequestKind::MarketBuy { quote_quantity } => {
                self.client
                    .place_market_buy(&symbol, *quote_quantity, &client_order_id)
                    .await
            }
            BinanceOrderRequestKind::MarketBuyQuantity { quantity } => {
                self.client
                    .place_market_buy_quantity(&symbol, *quantity, &client_order_id)
                    .await
            }
            BinanceOrderRequestKind::MarketSell { quantity } => {
                self.client
                    .place_market_sell(&symbol, *quantity, &client_order_id)
                    .await
            }
            BinanceOrderRequestKind::LimitIoc {
                side,
                quantity,
                price,
            } => {
                self.client
                    .place_limit_ioc(&symbol, side, *quantity, *price, &client_order_id)
                    .await
            }
        };
        self.emit_latency_stage(
            &request.operation_id,
            "placement_ws_api",
            placement_started,
            if result.is_ok() { "success" } else { "failed" },
        );

        match result {
            Ok(order) => {
                if let Err(error) = validate_response(&request, &order) {
                    let reason = bounded_reason(&format!("{error:#}"));
                    self.journal.advance(
                        &client_order_id,
                        BinanceOrderProgress::OutcomeUnknown {
                            reason: reason.clone(),
                        },
                    )?;
                    tracing::error!(
                        operation_id = request.operation_id,
                        client_order_id,
                        reason,
                        "Binance returned an inconsistent order; outcome journaled as unknown"
                    );
                    return Err(error);
                }
                self.record_order(&client_order_id, &order)?;
                let order = if terminal_status(&order.status) {
                    order
                } else {
                    let reconciliation_started = Instant::now();
                    let reconciled = self
                        .reconcile_known_order(&request.operation_id, &symbol, &client_order_id)
                        .await;
                    self.emit_latency_stage(
                        &request.operation_id,
                        "terminal_reconciliation",
                        reconciliation_started,
                        if reconciled.is_ok() {
                            "success"
                        } else {
                            "failed"
                        },
                    );
                    reconciled?
                };
                tracing::info!(
                    operation_id = request.operation_id,
                    client_order_id,
                    order_id = order.order_id,
                    status = %order.status,
                    executed_base = %order.executed_qty,
                    executed_quote = %order.cummulative_quote_qty,
                    "Binance order reached a journaled terminal state"
                );
                Ok(BinanceOrderOutcome {
                    order,
                    reconciled_after_unknown: false,
                })
            }
            Err(WsApiError::Rejected {
                status,
                code,
                message,
            }) => {
                if rejection_outcome_unknown(status, code) {
                    let reason = bounded_reason(&message);
                    self.journal.advance(
                        &client_order_id,
                        BinanceOrderProgress::OutcomeUnknown {
                            reason: reason.clone(),
                        },
                    )?;
                    tracing::error!(
                        operation_id = request.operation_id,
                        client_order_id,
                        status,
                        code,
                        reason,
                        "Binance reported an ambiguous placement error; reconciling by client order id"
                    );
                    let order = self
                        .query_after_reconnect(
                            &request.operation_id,
                            &symbol,
                            &client_order_id,
                            true,
                        )
                        .await
                        .with_context(|| {
                            format!(
                                "Binance order {client_order_id} remains outcome_unknown; do not retry"
                            )
                        })?
                        .context("ambiguous Binance placement was confirmed absent")?;
                    validate_response(&request, &order)?;
                    self.record_order(&client_order_id, &order)?;
                    ensure!(
                        terminal_status(&order.status),
                        "reconciled Binance order is not terminal"
                    );
                    return Ok(BinanceOrderOutcome {
                        order,
                        reconciled_after_unknown: true,
                    });
                }
                let reason = bounded_reason(&message);
                self.journal.advance(
                    &client_order_id,
                    BinanceOrderProgress::Rejected {
                        status,
                        code,
                        reason: reason.clone(),
                    },
                )?;
                tracing::error!(
                    operation_id = request.operation_id,
                    client_order_id,
                    status,
                    code,
                    reason,
                    "Binance order was rejected and journaled"
                );
                anyhow::bail!(
                    "Binance order rejected with HTTP status {status}, code {code}: {reason}"
                )
            }
            Err(error) => {
                let reason = bounded_reason(&error.to_string());
                self.journal.advance(
                    &client_order_id,
                    BinanceOrderProgress::OutcomeUnknown {
                        reason: reason.clone(),
                    },
                )?;
                tracing::error!(
                    operation_id = request.operation_id,
                    client_order_id,
                    reason,
                    "Binance placement outcome is unknown; reconciling by client order id"
                );
                let order = self
                    .query_after_reconnect(&request.operation_id, &symbol, &client_order_id, true)
                    .await
                    .with_context(|| {
                        format!(
                            "Binance order {client_order_id} remains outcome_unknown; do not retry"
                        )
                    })?
                    .context("ambiguous Binance placement was confirmed absent")?;
                validate_response(&request, &order)?;
                self.record_order(&client_order_id, &order)?;
                ensure!(
                    terminal_status(&order.status),
                    "reconciled Binance order is not terminal"
                );
                Ok(BinanceOrderOutcome {
                    order,
                    reconciled_after_unknown: true,
                })
            }
        }
    }

    async fn reconcile_known_order(
        &mut self,
        operation_id: &str,
        symbol: &str,
        client_order_id: &str,
    ) -> anyhow::Result<OrderResult> {
        let mut last_error = None;
        for index in 0..RECONCILIATION_ATTEMPTS {
            let query_started = Instant::now();
            let result = self.client.query_order(symbol, client_order_id).await;
            self.emit_latency_stage(
                operation_id,
                "order_status_reconciliation",
                query_started,
                if result.is_ok() { "success" } else { "failed" },
            );
            match result {
                Ok(order) => {
                    last_error = None;
                    self.record_order(client_order_id, &order)?;
                    if terminal_status(&order.status) {
                        return Ok(order);
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        operation_id,
                        client_order_id,
                        attempt = index + 1,
                        maximum_attempts = RECONCILIATION_ATTEMPTS,
                        error = %error,
                        "Binance terminal status reconciliation attempt failed"
                    );
                    last_error = Some(error);
                }
            }
        }
        let reason = last_error
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "terminal status confirmation timed out".to_owned());
        self.journal.advance(
            client_order_id,
            BinanceOrderProgress::OutcomeUnknown {
                reason: bounded_reason(&reason),
            },
        )?;
        Err(last_error.map(anyhow::Error::from).unwrap_or_else(|| {
            anyhow::anyhow!("Binance order terminal status confirmation timed out")
        }))
    }

    async fn query_after_reconnect(
        &mut self,
        operation_id: &str,
        symbol: &str,
        client_order_id: &str,
        confirm_absent: bool,
    ) -> anyhow::Result<Option<OrderResult>> {
        let mut last_error = None;
        let mut every_response_was_not_found = true;
        let mut last_not_found = None;
        for index in 0..RECONCILIATION_ATTEMPTS {
            let query_started = Instant::now();
            let result = self.client.query_order(symbol, client_order_id).await;
            self.emit_latency_stage(
                operation_id,
                "order_status_reconciliation",
                query_started,
                if result.is_ok() { "success" } else { "failed" },
            );
            match result {
                Ok(order) => {
                    last_error = None;
                    every_response_was_not_found = false;
                    if terminal_status(&order.status) {
                        return Ok(Some(order));
                    }
                }
                Err(error) => {
                    if let Some(not_found) = order_not_found_details(&error) {
                        last_not_found = Some(not_found);
                    } else {
                        every_response_was_not_found = false;
                    }
                    tracing::warn!(
                        operation_id,
                        client_order_id,
                        attempt = index + 1,
                        maximum_attempts = RECONCILIATION_ATTEMPTS,
                        error = %error,
                        "Binance unknown-outcome reconciliation attempt failed"
                    );
                    last_error = Some(error);
                }
            }
        }
        if confirm_absent
            && every_response_was_not_found
            && let Some((status, code, message)) = last_not_found
        {
            self.journal.advance(
                client_order_id,
                BinanceOrderProgress::Rejected {
                    status,
                    code,
                    reason: bounded_reason(&format!(
                        "order.status confirmed absent after {} attempts: {message}",
                        RECONCILIATION_ATTEMPTS
                    )),
                },
            )?;
            tracing::warn!(
                operation_id,
                client_order_id,
                attempts = RECONCILIATION_ATTEMPTS,
                "Binance order was confirmed absent; parent recovery may continue"
            );
            return Ok(None);
        }
        let reason = last_error
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "Binance reconciliation returned no terminal result".to_owned());
        if self
            .journal
            .operations()
            .get(client_order_id)
            .is_some_and(|operation| !operation.progress.terminal())
        {
            self.journal.advance(
                client_order_id,
                BinanceOrderProgress::OutcomeUnknown {
                    reason: bounded_reason(&reason),
                },
            )?;
        }
        Err(last_error
            .map(anyhow::Error::from)
            .unwrap_or_else(|| anyhow::anyhow!("Binance reconciliation returned no result")))
    }

    fn record_order(&mut self, client_order_id: &str, order: &OrderResult) -> anyhow::Result<()> {
        let progress = if terminal_status(&order.status) {
            BinanceOrderProgress::Terminal {
                order_id: order.order_id,
                status: order.status.clone(),
                executed_quantity: decimal_string(order.executed_qty),
                cumulative_quote_quantity: decimal_string(order.cummulative_quote_qty),
                order: Some(order.clone()),
            }
        } else {
            BinanceOrderProgress::Submitted {
                order_id: order.order_id,
                status: order.status.clone(),
                executed_quantity: decimal_string(order.executed_qty),
                cumulative_quote_quantity: decimal_string(order.cummulative_quote_qty),
                order: Some(order.clone()),
            }
        };
        self.journal.advance(client_order_id, progress)
    }

    fn classify_execution_error(
        &self,
        client_order_id: &str,
        reason: String,
    ) -> BinanceExecutionServiceError {
        classify_execution_error(
            self.journal
                .operations()
                .get(client_order_id)
                .map(|operation| &operation.progress),
            reason,
        )
    }
}

fn classify_execution_error(
    progress: Option<&BinanceOrderProgress>,
    reason: String,
) -> BinanceExecutionServiceError {
    match progress {
        None => BinanceExecutionServiceError::FailedBeforeSubmission { reason },
        Some(BinanceOrderProgress::Rejected { .. }) => {
            BinanceExecutionServiceError::Rejected { reason }
        }
        Some(
            BinanceOrderProgress::IntentRecorded
            | BinanceOrderProgress::Submitted { .. }
            | BinanceOrderProgress::OutcomeUnknown { .. }
            | BinanceOrderProgress::Terminal { .. },
        ) => BinanceExecutionServiceError::OutcomeUnknown { reason },
    }
}

struct WorkItem {
    request: BinanceOrderRequest,
    enqueued_at: Instant,
    response: oneshot::Sender<Result<BinanceOrderOutcome, BinanceExecutionServiceError>>,
}

/// A bounded single-owner Binance execution lane on a dedicated OS thread.
/// The worker owns the authenticated WebSocket session and durable order journal.
pub struct BinanceExecutionService {
    sender: Option<mpsc::Sender<WorkItem>>,
    thread: Option<JoinHandle<()>>,
}

impl BinanceExecutionService {
    pub async fn spawn(
        client: MultiplexedBinanceWsApi,
        journal_path: PathBuf,
        capacity: usize,
    ) -> anyhow::Result<Self> {
        Self::spawn_inner(client, journal_path, capacity, None).await
    }

    pub async fn spawn_instrumented(
        client: MultiplexedBinanceWsApi,
        journal_path: PathBuf,
        capacity: usize,
        latency_telemetry: ExecutionLatencyTelemetry,
    ) -> anyhow::Result<Self> {
        Self::spawn_inner(client, journal_path, capacity, Some(latency_telemetry)).await
    }

    async fn spawn_inner(
        client: MultiplexedBinanceWsApi,
        journal_path: PathBuf,
        capacity: usize,
        latency_telemetry: Option<ExecutionLatencyTelemetry>,
    ) -> anyhow::Result<Self> {
        ensure!(capacity > 0, "Binance execution channel capacity is zero");
        let (sender, mut receiver) = mpsc::channel::<WorkItem>(capacity);
        let (startup_sender, startup_receiver) = oneshot::channel();
        let thread = std::thread::Builder::new()
            .name("binance-executor".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = startup_sender.send(Err(format!("{error:#}")));
                        return;
                    }
                };
                let mut executor = match runtime.block_on(BinanceExecutor::initialize(
                    client,
                    journal_path,
                    latency_telemetry,
                )) {
                    Ok(executor) => executor,
                    Err(error) => {
                        let _ = startup_sender.send(Err(format!("{error:#}")));
                        return;
                    }
                };
                if startup_sender.send(Ok(())).is_err() {
                    return;
                }
                while let Some(work) = receiver.blocking_recv() {
                    let operation_id = work.request.operation_id.clone();
                    let client_order_id = work.request.client_order_id.clone();
                    executor.emit_latency_stage(
                        &operation_id,
                        "worker_queue",
                        work.enqueued_at,
                        "success",
                    );
                    let execution_started = Instant::now();
                    let result =
                        runtime
                            .block_on(executor.execute(work.request))
                            .map_err(|error| {
                                executor.classify_execution_error(
                                    &client_order_id,
                                    format!("{error:#}"),
                                )
                            });
                    executor.emit_latency_stage(
                        &operation_id,
                        "worker_total",
                        execution_started,
                        if result.is_ok() { "success" } else { "failed" },
                    );
                    if let Err(error) = &result {
                        tracing::error!(
                            operation_id,
                            error = %error,
                            "Binance execution failed; inspect order journal before retry"
                        );
                    }
                    if work.response.send(result).is_err() {
                        tracing::warn!(operation_id, "Binance execution caller dropped response");
                    }
                }
            })
            .context("failed to spawn Binance executor thread")?;
        startup_receiver
            .await
            .context("Binance executor stopped during startup")?
            .map_err(anyhow::Error::msg)?;
        Ok(Self {
            sender: Some(sender),
            thread: Some(thread),
        })
    }

    pub async fn execute(
        &self,
        request: BinanceOrderRequest,
    ) -> Result<BinanceOrderOutcome, BinanceExecutionServiceError> {
        let sender =
            self.sender
                .as_ref()
                .ok_or_else(|| BinanceExecutionServiceError::OutcomeUnknown {
                    reason: "Binance execution service is shut down".to_owned(),
                })?;
        let (response, receiver) = oneshot::channel();
        sender
            .send(WorkItem {
                request,
                enqueued_at: Instant::now(),
                response,
            })
            .await
            .map_err(|_| BinanceExecutionServiceError::OutcomeUnknown {
                reason: "Binance executor thread stopped".to_owned(),
            })?;
        receiver
            .await
            .map_err(|_| BinanceExecutionServiceError::OutcomeUnknown {
                reason: "Binance executor dropped its response".to_owned(),
            })?
    }
}

fn duration_us(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

impl Drop for BinanceExecutionService {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(thread) = self.thread.take()
            && let Err(payload) = thread.join()
        {
            tracing::error!(?payload, "Binance executor thread panicked during shutdown");
        }
    }
}

fn validate_response(request: &BinanceOrderRequest, order: &OrderResult) -> anyhow::Result<()> {
    let intent = request.intent();
    ensure!(
        order.symbol == intent.symbol,
        "Binance response symbol mismatch"
    );
    ensure!(
        order.client_order_id == intent.client_order_id,
        "Binance response client order id mismatch"
    );
    ensure!(order.side == intent.side, "Binance response side mismatch");
    ensure!(
        order.order_type == intent.order_type,
        "Binance response order type mismatch"
    );
    ensure!(
        order.executed_qty >= Decimal::ZERO && order.cummulative_quote_qty >= Decimal::ZERO,
        "Binance response has a negative execution quantity"
    );
    match &request.kind {
        BinanceOrderRequestKind::MarketBuy { quote_quantity } => ensure!(
            order.cummulative_quote_qty <= *quote_quantity,
            "MARKET buy exceeded its quote quantity cap"
        ),
        BinanceOrderRequestKind::MarketBuyQuantity { quantity } => ensure!(
            order.executed_qty <= *quantity,
            "MARKET buy exceeded its base quantity"
        ),
        BinanceOrderRequestKind::MarketSell { quantity } => ensure!(
            order.executed_qty <= *quantity,
            "MARKET sell exceeded its base quantity"
        ),
        BinanceOrderRequestKind::LimitIoc {
            quantity, price, ..
        } => {
            ensure!(order.time_in_force == "IOC", "LIMIT response is not IOC");
            ensure!(
                order.executed_qty <= *quantity,
                "LIMIT execution exceeded requested quantity"
            );
            if intent.side == "BUY" {
                ensure!(
                    order.cummulative_quote_qty <= *quantity * *price,
                    "LIMIT buy exceeded its price cap"
                );
            }
        }
    }
    Ok(())
}

fn terminal_status(status: &str) -> bool {
    matches!(
        status,
        "FILLED" | "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" | "REJECTED"
    )
}

fn rejection_outcome_unknown(status: u16, code: i64) -> bool {
    status >= 500 || matches!(code, -1000 | -1001 | -1006 | -1007)
}

fn order_not_found_details(error: &WsApiError) -> Option<(u16, i64, String)> {
    match error {
        WsApiError::Rejected {
            status,
            code: -2013,
            message,
        } => Some((*status, -2013, message.clone())),
        WsApiError::Transport(_) | WsApiError::Rejected { .. } | WsApiError::Protocol(_) => None,
    }
}

fn decimal_string(value: Decimal) -> String {
    value.normalize().to_string()
}

fn bounded_reason(reason: &str) -> String {
    reason
        .chars()
        .filter(|character| !character.is_control())
        .take(1_024)
        .collect()
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use crate::binance::ws_api::WsApiError;

    use super::{
        BinanceExecutionServiceError, BinanceOrderProgress, BinanceOrderRequest,
        BinanceOrderRequestKind, RECONCILIATION_ATTEMPTS, classify_execution_error,
        order_not_found_details, rejection_outcome_unknown, terminal_status,
    };

    #[test]
    fn rejects_non_positive_and_non_validation_requests() {
        let request = BinanceOrderRequest {
            operation_id: "trade".to_owned(),
            client_order_id: "order".to_owned(),
            symbol: "WLDUSDC".to_owned(),
            kind: BinanceOrderRequestKind::MarketBuy {
                quote_quantity: Decimal::ZERO,
            },
        };
        assert!(request.validate().is_err());
    }

    #[test]
    fn recognizes_all_terminal_spot_statuses() {
        for status in [
            "FILLED",
            "CANCELED",
            "EXPIRED",
            "EXPIRED_IN_MATCH",
            "REJECTED",
        ] {
            assert!(terminal_status(status));
        }
        assert!(!terminal_status("NEW"));
        assert!(!terminal_status("PARTIALLY_FILLED"));
    }

    #[test]
    fn treats_binance_unknown_execution_codes_as_non_terminal() {
        for code in [-1000, -1001, -1006, -1007] {
            assert!(rejection_outcome_unknown(400, code));
        }
        assert!(rejection_outcome_unknown(500, -1100));
        assert!(!rejection_outcome_unknown(400, -1013));
    }

    #[test]
    fn unknown_reconciliation_uses_one_immediate_status_lookup() {
        assert_eq!(RECONCILIATION_ATTEMPTS, 1);
    }

    #[test]
    fn only_no_such_order_can_prove_an_ambiguous_placement_absent() {
        assert!(
            order_not_found_details(&WsApiError::Rejected {
                status: 400,
                code: -2013,
                message: "Order does not exist.".to_owned(),
            })
            .is_some()
        );
        assert!(order_not_found_details(&WsApiError::Transport("timeout".to_owned())).is_none());
        assert!(
            order_not_found_details(&WsApiError::Rejected {
                status: 500,
                code: -1007,
                message: "execution status unknown".to_owned(),
            })
            .is_none()
        );
    }

    #[test]
    fn child_error_classification_distinguishes_unsubmitted_rejected_and_unknown() {
        assert!(matches!(
            classify_execution_error(None, "invalid request".to_owned()),
            BinanceExecutionServiceError::FailedBeforeSubmission { .. }
        ));
        assert!(matches!(
            classify_execution_error(
                Some(&BinanceOrderProgress::Rejected {
                    status: 400,
                    code: -1013,
                    reason: "filter".to_owned(),
                }),
                "filter".to_owned(),
            ),
            BinanceExecutionServiceError::Rejected { .. }
        ));
        assert!(matches!(
            classify_execution_error(
                Some(&BinanceOrderProgress::OutcomeUnknown {
                    reason: "timeout".to_owned(),
                }),
                "timeout".to_owned(),
            ),
            BinanceExecutionServiceError::OutcomeUnknown { .. }
        ));
    }
}

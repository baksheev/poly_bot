use std::{path::PathBuf, thread::JoinHandle, time::Duration};

use anyhow::{Context, ensure};
use rust_decimal::Decimal;
use tokio::sync::{mpsc, oneshot};

use crate::binance::{
    order_journal::{BinanceOrderIntent, BinanceOrderJournal, BinanceOrderProgress},
    user_data::MultiplexedBinanceWsApi,
    ws_api::{OrderResult, WsApiError},
};

const RECONCILIATION_ATTEMPTS: usize = 8;
const RECONCILIATION_DELAY: Duration = Duration::from_millis(250);

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
}

impl BinanceExecutor {
    async fn initialize(
        client: MultiplexedBinanceWsApi,
        journal_path: PathBuf,
    ) -> anyhow::Result<Self> {
        let journal = BinanceOrderJournal::open(journal_path)?;
        let mut executor = Self { client, journal };
        executor.reconcile_startup().await?;
        Ok(executor)
    }

    async fn reconcile_startup(&mut self) -> anyhow::Result<()> {
        let active = self
            .journal
            .active_operations()
            .into_iter()
            .map(|operation| {
                (
                    operation.intent.symbol.clone(),
                    operation.intent.client_order_id.clone(),
                )
            })
            .collect::<Vec<_>>();
        for (symbol, client_order_id) in active {
            let order = self
                .query_after_reconnect(&symbol, &client_order_id)
                .await
                .with_context(|| {
                    format!("unresolved Binance order {client_order_id}; journal remains blocked")
                })?;
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
                        .query_after_reconnect(&symbol, &client_order_id)
                        .await?;
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
                BinanceOrderProgress::Rejected { code, .. } => {
                    anyhow::bail!("journaled Binance order was rejected with code {code}")
                }
                _ => anyhow::bail!("journaled Binance order still requires reconciliation"),
            }
        }
        self.journal.record_intent(request_intent)?;
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
                    self.reconcile_known_order(&symbol, &client_order_id)
                        .await?
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
                        .query_after_reconnect(&symbol, &client_order_id)
                        .await
                        .with_context(|| {
                            format!(
                                "Binance order {client_order_id} remains outcome_unknown; do not retry"
                            )
                        })?;
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
                self.journal.advance(
                    &client_order_id,
                    BinanceOrderProgress::Rejected {
                        status,
                        code,
                        reason: bounded_reason(&message),
                    },
                )?;
                tracing::error!(
                    operation_id = request.operation_id,
                    client_order_id,
                    status,
                    code,
                    reason = message,
                    "Binance order was rejected and journaled"
                );
                anyhow::bail!("Binance order rejected with code {code}")
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
                    .query_after_reconnect(&symbol, &client_order_id)
                    .await
                    .with_context(|| {
                        format!(
                            "Binance order {client_order_id} remains outcome_unknown; do not retry"
                        )
                    })?;
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
        symbol: &str,
        client_order_id: &str,
    ) -> anyhow::Result<OrderResult> {
        for _ in 0..RECONCILIATION_ATTEMPTS {
            tokio::time::sleep(RECONCILIATION_DELAY).await;
            match self.client.query_order(symbol, client_order_id).await {
                Ok(order) => {
                    self.record_order(client_order_id, &order)?;
                    if terminal_status(&order.status) {
                        return Ok(order);
                    }
                }
                Err(error) => {
                    self.journal.advance(
                        client_order_id,
                        BinanceOrderProgress::OutcomeUnknown {
                            reason: bounded_reason(&error.to_string()),
                        },
                    )?;
                    return Err(error.into());
                }
            }
        }
        self.journal.advance(
            client_order_id,
            BinanceOrderProgress::OutcomeUnknown {
                reason: "terminal status confirmation timed out".to_owned(),
            },
        )?;
        anyhow::bail!("Binance order terminal status confirmation timed out")
    }

    async fn query_after_reconnect(
        &mut self,
        symbol: &str,
        client_order_id: &str,
    ) -> anyhow::Result<OrderResult> {
        let mut last_error = None;
        for _ in 0..RECONCILIATION_ATTEMPTS {
            match self.client.query_order(symbol, client_order_id).await {
                Ok(order) => return Ok(order),
                Err(error) => last_error = Some(error),
            }
            tokio::time::sleep(RECONCILIATION_DELAY).await;
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
                let mut executor =
                    match runtime.block_on(BinanceExecutor::initialize(client, journal_path)) {
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
                    let result =
                        runtime
                            .block_on(executor.execute(work.request))
                            .map_err(|error| {
                                executor.classify_execution_error(
                                    &client_order_id,
                                    format!("{error:#}"),
                                )
                            });
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
            .send(WorkItem { request, response })
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

    use super::{
        BinanceExecutionServiceError, BinanceOrderProgress, BinanceOrderRequest,
        BinanceOrderRequestKind, classify_execution_error, rejection_outcome_unknown,
        terminal_status,
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

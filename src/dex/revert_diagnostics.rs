use std::time::{Duration, Instant};

use alloy_primitives::B256;
use tokio::sync::mpsc;

use crate::{
    chain::rpc::{JsonRpcClient, TransactionRevertDiagnostic},
    telemetry::{ARBITRAGE_DEX_REVERT_KIND, TelemetryHandle},
};

const DIAGNOSTIC_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DexRevertDiagnosticRequest {
    pub plan_id: String,
    pub operation_id: String,
    pub pair_id: String,
    pub source_revision: String,
    pub direction: String,
    pub protocol: String,
    pub pool_reference: String,
    pub transaction_hash: B256,
    pub block_number: u64,
    pub gas_used: u64,
    pub effective_gas_price: u128,
    pub l1_fee: u128,
    pub amount_in_base_units: String,
    pub amount_out_minimum_base_units: String,
    pub deadline_unix_seconds: u64,
    pub execution_reason: String,
}

#[derive(Clone)]
pub struct DexRevertDiagnosticHandle {
    sender: mpsc::Sender<DexRevertDiagnosticRequest>,
}

pub struct DexRevertDiagnosticTask {
    rpc: JsonRpcClient,
    telemetry: TelemetryHandle,
    engine_id: String,
    receiver: mpsc::Receiver<DexRevertDiagnosticRequest>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DexRevertDiagnosticSubmit {
    Queued,
    QueueFull,
    WorkerClosed,
}

pub fn dex_revert_diagnostic_channel(
    rpc: JsonRpcClient,
    telemetry: TelemetryHandle,
    engine_id: String,
    capacity: usize,
) -> (DexRevertDiagnosticHandle, DexRevertDiagnosticTask) {
    let (sender, receiver) = mpsc::channel(capacity.max(1));
    (
        DexRevertDiagnosticHandle { sender },
        DexRevertDiagnosticTask {
            rpc,
            telemetry,
            engine_id,
            receiver,
        },
    )
}

impl DexRevertDiagnosticHandle {
    /// Never awaits: trace availability cannot delay execution-lane release.
    pub fn try_submit(&self, request: DexRevertDiagnosticRequest) -> DexRevertDiagnosticSubmit {
        match self.sender.try_send(request) {
            Ok(()) => DexRevertDiagnosticSubmit::Queued,
            Err(mpsc::error::TrySendError::Full(_)) => DexRevertDiagnosticSubmit::QueueFull,
            Err(mpsc::error::TrySendError::Closed(_)) => DexRevertDiagnosticSubmit::WorkerClosed,
        }
    }
}

impl DexRevertDiagnosticSubmit {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::QueueFull => "queue_full",
            Self::WorkerClosed => "worker_closed",
        }
    }
}

impl DexRevertDiagnosticTask {
    pub async fn run(mut self) {
        while let Some(request) = self.receiver.recv().await {
            let started_at = Instant::now();
            let result = tokio::time::timeout(
                DIAGNOSTIC_TIMEOUT,
                self.rpc.diagnose_transaction_revert(
                    request.transaction_hash,
                    request.block_number,
                    request.gas_used,
                ),
            )
            .await;
            let duration_us = started_at.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
            match result {
                Ok(Ok(diagnostic)) => {
                    self.emit_diagnostic(&request, duration_us, "completed", Some(diagnostic), None)
                }
                Ok(Err(error)) => self.emit_diagnostic(
                    &request,
                    duration_us,
                    "failed",
                    None,
                    Some(bounded_reason(&format!("{error:#}"))),
                ),
                Err(_) => self.emit_diagnostic(
                    &request,
                    duration_us,
                    "timed_out",
                    None,
                    Some("revert diagnostic exceeded 5 seconds".to_owned()),
                ),
            }
        }
    }

    fn emit_diagnostic(
        &self,
        request: &DexRevertDiagnosticRequest,
        duration_us: u64,
        diagnostic_status: &'static str,
        diagnostic: Option<TransactionRevertDiagnostic>,
        diagnostic_error: Option<String>,
    ) {
        self.telemetry.emit(
            ARBITRAGE_DEX_REVERT_KIND,
            serde_json::json!({
                "engine_id": self.engine_id,
                "phase": "diagnostic",
                "diagnostic_status": diagnostic_status,
                "diagnostic_duration_us": duration_us,
                "plan_id": request.plan_id,
                "operation_id": request.operation_id,
                "pair_id": request.pair_id,
                "source_revision": request.source_revision,
                "direction": request.direction,
                "protocol": request.protocol,
                "pool_reference": request.pool_reference,
                "transaction_hash": format!("{:#x}", request.transaction_hash),
                "block_number": request.block_number,
                "gas_used": request.gas_used,
                "effective_gas_price": request.effective_gas_price.to_string(),
                "l1_fee": request.l1_fee.to_string(),
                "amount_in_base_units": request.amount_in_base_units,
                "amount_out_minimum_base_units": request.amount_out_minimum_base_units,
                "deadline_unix_seconds": request.deadline_unix_seconds,
                "execution_reason": bounded_reason(&request.execution_reason),
                "diagnostic_source": diagnostic.as_ref().map(|value| value.source.as_str()),
                "classification": diagnostic.as_ref().map(|value| value.classification.as_str()),
                "reason": diagnostic.as_ref().and_then(|value| value.reason.as_deref()),
                "revert_selector": diagnostic.as_ref().and_then(|value| value.selector.as_deref()),
                "transaction_gas_limit": diagnostic.as_ref().and_then(|value| value.gas_limit),
                "gas_used_equals_limit": diagnostic
                    .as_ref()
                    .and_then(|value| value.gas_used_equals_limit),
                "trace_error": diagnostic
                    .as_ref()
                    .and_then(|value| value.trace_error.as_deref()),
                "diagnostic_error": diagnostic_error,
            }),
        );
    }
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
    use super::bounded_reason;

    #[test]
    fn telemetry_reason_is_bounded_and_control_free() {
        let reason = format!("bad\n{}", "x".repeat(2_000));
        let bounded = bounded_reason(&reason);
        assert!(!bounded.contains('\n'));
        assert_eq!(bounded.chars().count(), 1_024);
    }
}

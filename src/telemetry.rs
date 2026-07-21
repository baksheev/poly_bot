use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use bytes::Bytes;
use ch_client::{Client, Compression};
use serde::Serialize;
use serde_json::Value;
use tokio::{sync::mpsc, time::MissedTickBehavior};

use crate::config::AppConfig;

pub const ARBITRAGE_RESULT_KIND: &str = "arbitrage_result";
pub const ARBITRAGE_EXECUTION_STAGE_KIND: &str = "arbitrage_execution_stage";

#[derive(Clone)]
pub struct TelemetryHandle {
    sender: mpsc::Sender<TelemetryRecord>,
    dropped_records: Arc<AtomicU64>,
}

impl TelemetryHandle {
    /// Never awaits: telemetry cannot block the trading path.
    pub fn emit(&self, kind: &'static str, payload: Value) {
        let record = TelemetryRecord {
            observed_at_ms: unix_timestamp_ms(),
            kind,
            payload_json: payload.to_string(),
        };

        if self.sender.try_send(record).is_err() {
            self.dropped_records.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[cfg(test)]
    pub(crate) fn disconnected_test_handle() -> Self {
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        Self {
            sender,
            dropped_records: Arc::new(AtomicU64::new(0)),
        }
    }
}

/// Cheap, non-blocking execution-stage telemetry shared with the dedicated
/// DEX and Binance worker threads. `operation_id` is the venue id; orchestration
/// events additionally carry `plan_id` so the two namespaces can be joined.
#[derive(Clone)]
pub struct ExecutionLatencyTelemetry {
    handle: TelemetryHandle,
    engine_id: String,
}

impl ExecutionLatencyTelemetry {
    pub fn new(handle: TelemetryHandle, engine_id: String) -> Self {
        Self { handle, engine_id }
    }

    pub fn emit_stage(
        &self,
        venue: &'static str,
        operation_id: &str,
        stage: &'static str,
        duration_us: u64,
        outcome: &'static str,
    ) {
        self.handle.emit(
            ARBITRAGE_EXECUTION_STAGE_KIND,
            serde_json::json!({
                "engine_id": self.engine_id,
                "venue": venue,
                "operation_id": operation_id,
                "stage": stage,
                "duration_us": duration_us,
                "outcome": outcome,
            }),
        );
    }
}

#[derive(Debug, Serialize)]
struct TelemetryRecord {
    observed_at_ms: u64,
    kind: &'static str,
    payload_json: String,
}

pub struct TelemetryWriter {
    client: Option<Client>,
    database: String,
    channel_capacity: usize,
    batch_size: usize,
    flush_interval: Duration,
}

impl TelemetryWriter {
    pub fn new(config: &AppConfig) -> Self {
        let client = config.clickhouse_enabled().then(|| {
            Client::default()
                .with_url(config.clickhouse_url.trim_end_matches('/'))
                .with_user(config.clickhouse_user.clone())
                .with_password(config.clickhouse_password.clone())
                .with_compression(Compression::Lz4)
                .with_validation(false)
        });

        Self {
            client,
            database: config.clickhouse_database.clone(),
            channel_capacity: config.telemetry_channel_capacity,
            batch_size: config.telemetry_batch_size,
            flush_interval: Duration::from_millis(config.telemetry_flush_interval_ms),
        }
    }

    pub fn channel(self) -> (TelemetryHandle, TelemetryTask) {
        let (sender, receiver) = mpsc::channel(self.channel_capacity);
        let dropped_records = Arc::new(AtomicU64::new(0));
        (
            TelemetryHandle {
                sender,
                dropped_records: Arc::clone(&dropped_records),
            },
            TelemetryTask {
                writer: self,
                receiver,
                dropped_records,
            },
        )
    }

    pub async fn emit_once(&self, kind: &'static str, payload: Value) -> anyhow::Result<()> {
        let record = TelemetryRecord {
            observed_at_ms: unix_timestamp_ms(),
            kind,
            payload_json: payload.to_string(),
        };
        self.insert(&[record]).await
    }

    pub async fn migrate(&self) -> anyhow::Result<()> {
        let client = self
            .client
            .as_ref()
            .context("CLICKHOUSE_URL is required for migrate")?;
        validate_identifier(&self.database)?;

        client
            .query(&format!("CREATE DATABASE IF NOT EXISTS {}", self.database))
            .execute()
            .await
            .context("failed to create ClickHouse database")?;
        client
            .query(&format!(
                r#"
CREATE TABLE IF NOT EXISTS {}.runtime_telemetry
(
    observed_at_ms UInt64,
    kind LowCardinality(String),
    payload_json String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toDate(fromUnixTimestamp64Milli(observed_at_ms))
ORDER BY (kind, observed_at_ms)
TTL toDateTime(fromUnixTimestamp64Milli(observed_at_ms)) + INTERVAL 30 DAY
"#,
                self.database
            ))
            .execute()
            .await
            .context("failed to create ClickHouse telemetry table")?;
        client
            .query(&format!(
                r#"
CREATE TABLE IF NOT EXISTS {}.arbitrage_results
(
    observed_at_ms UInt64,
    engine_id LowCardinality(String),
    plan_id String,
    source_revision String,
    pair_id LowCardinality(String),
    execution_mode LowCardinality(String),
    direction LowCardinality(String),
    outcome LowCardinality(String),
    expected_profit_token_a_base_units String,
    realized_profit_token_a_base_units String,
    token_b_residual_base_units String,
    gas_cost_token_a_base_units String,
    recovery_loss_token_a_base_units String,
    payload_json String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toDate(fromUnixTimestamp64Milli(observed_at_ms))
ORDER BY (pair_id, observed_at_ms, plan_id)
"#,
                self.database
            ))
            .execute()
            .await
            .context("failed to create ClickHouse arbitrage results table")?;
        client
            .query(&format!(
                r#"
CREATE MATERIALIZED VIEW IF NOT EXISTS {}.arbitrage_results_from_telemetry
TO {}.arbitrage_results
AS SELECT
    observed_at_ms,
    JSONExtractString(payload_json, 'engine_id') AS engine_id,
    JSONExtractString(payload_json, 'plan_id') AS plan_id,
    JSONExtractString(payload_json, 'source_revision') AS source_revision,
    JSONExtractString(payload_json, 'pair_id') AS pair_id,
    JSONExtractString(payload_json, 'execution_mode') AS execution_mode,
    JSONExtractString(payload_json, 'direction') AS direction,
    JSONExtractString(payload_json, 'outcome') AS outcome,
    JSONExtractString(payload_json, 'expected_profit_token_a_base_units') AS expected_profit_token_a_base_units,
    JSONExtractString(payload_json, 'realized_profit_token_a_base_units') AS realized_profit_token_a_base_units,
    JSONExtractString(payload_json, 'token_b_residual_base_units') AS token_b_residual_base_units,
    JSONExtractString(payload_json, 'gas_cost_token_a_base_units') AS gas_cost_token_a_base_units,
    JSONExtractString(payload_json, 'recovery_loss_token_a_base_units') AS recovery_loss_token_a_base_units,
    payload_json
FROM {}.runtime_telemetry
WHERE kind = '{ARBITRAGE_RESULT_KIND}'
"#,
                self.database, self.database, self.database
            ))
            .execute()
            .await
            .context("failed to create ClickHouse arbitrage results materialized view")?;
        Ok(())
    }

    async fn insert(&self, rows: &[TelemetryRecord]) -> anyhow::Result<()> {
        let Some(client) = &self.client else {
            for row in rows {
                tracing::debug!(kind = row.kind, payload = row.payload_json, "telemetry");
            }
            return Ok(());
        };

        let mut body = String::new();
        for row in rows {
            body.push_str(&serde_json::to_string(row)?);
            body.push('\n');
        }

        let query = format!(
            "INSERT INTO {}.runtime_telemetry FORMAT JSONEachRow",
            self.database
        );
        let mut insert = client.insert_formatted_with(query);
        insert.send(Bytes::from(body)).await?;
        insert.end().await?;
        Ok(())
    }
}

pub struct TelemetryTask {
    writer: TelemetryWriter,
    receiver: mpsc::Receiver<TelemetryRecord>,
    dropped_records: Arc<AtomicU64>,
}

impl TelemetryTask {
    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut batch = Vec::with_capacity(self.writer.batch_size);
        let mut interval = tokio::time::interval(self.writer.flush_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                record = self.receiver.recv() => {
                    match record {
                        Some(record) => {
                            batch.push(record);
                            if batch.len() >= self.writer.batch_size {
                                self.flush(&mut batch).await;
                            }
                        }
                        None => {
                            self.flush(&mut batch).await;
                            return Ok(());
                        }
                    }
                }
                _ = interval.tick() => {
                    self.report_drops();
                    self.flush(&mut batch).await;
                },
            }
        }
    }

    fn report_drops(&self) {
        let dropped = self.dropped_records.swap(0, Ordering::Relaxed);
        if dropped > 0 {
            tracing::warn!(dropped, "telemetry records dropped outside the hot path");
        }
    }

    async fn flush(&self, batch: &mut Vec<TelemetryRecord>) {
        if batch.is_empty() {
            return;
        }

        if let Err(error) = self.writer.insert(batch).await {
            tracing::error!(rows = batch.len(), %error, "ClickHouse telemetry flush failed");
        }
        batch.clear();
    }
}

fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn validate_identifier(value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !value.is_empty()
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
        "invalid ClickHouse identifier: {value}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ARBITRAGE_RESULT_KIND, validate_identifier};

    #[test]
    fn clickhouse_identifier_is_restricted() {
        assert!(validate_identifier("arb_bot_prod").is_ok());
        assert!(validate_identifier("arb_bot; DROP DATABASE x").is_err());
    }

    #[test]
    fn live_result_kind_matches_the_materialized_view_contract() {
        assert_eq!(ARBITRAGE_RESULT_KIND, "arbitrage_result");
    }
}

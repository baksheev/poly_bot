use anyhow::{Context, ensure};
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(author, version, about)]
pub struct Cli {
    #[command(flatten)]
    pub config: AppConfig,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run the single-process trading service.
    Run,
    /// Create ClickHouse telemetry tables.
    Migrate,
    /// Validate configuration without connecting to external systems.
    Check,
}

#[derive(Parser, Debug, Clone)]
pub struct AppConfig {
    #[arg(long, env = "SERVICE_NAME", default_value = "poly-bot-trading-engine")]
    pub service_name: String,

    #[arg(long, env = "GCP_PROJECT_ID", default_value = "poly-bot-502515")]
    pub gcp_project_id: String,

    #[arg(long, env = "GCP_REGION", default_value = "us-east1")]
    pub gcp_region: String,

    #[arg(
        long,
        env = "BINANCE_WS_URL",
        default_value = "wss://stream.binance.com:9443/ws"
    )]
    pub binance_ws_url: String,

    #[arg(
        long,
        env = "POLYMARKET_CLOB_URL",
        default_value = "https://clob.polymarket.com"
    )]
    pub polymarket_clob_url: String,

    #[arg(long, env = "CLICKHOUSE_URL", default_value = "")]
    pub clickhouse_url: String,

    #[arg(long, env = "CLICKHOUSE_DATABASE", default_value = "poly_bot")]
    pub clickhouse_database: String,

    #[arg(long, env = "CLICKHOUSE_USER", default_value = "default")]
    pub clickhouse_user: String,

    #[arg(long, env = "CLICKHOUSE_PASSWORD", default_value = "")]
    pub clickhouse_password: String,

    #[arg(long, env = "TELEMETRY_CHANNEL_CAPACITY", default_value_t = 8192)]
    pub telemetry_channel_capacity: usize,

    #[arg(long, env = "TELEMETRY_BATCH_SIZE", default_value_t = 200)]
    pub telemetry_batch_size: usize,

    #[arg(long, env = "TELEMETRY_FLUSH_INTERVAL_MS", default_value_t = 100)]
    pub telemetry_flush_interval_ms: u64,
}

impl AppConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            !self.service_name.trim().is_empty(),
            "SERVICE_NAME is empty"
        );
        ensure!(
            !self.gcp_project_id.trim().is_empty(),
            "GCP_PROJECT_ID is empty"
        );
        ensure!(!self.gcp_region.trim().is_empty(), "GCP_REGION is empty");
        ensure!(
            self.telemetry_channel_capacity > 0,
            "TELEMETRY_CHANNEL_CAPACITY must be greater than zero"
        );
        ensure!(
            self.telemetry_batch_size > 0,
            "TELEMETRY_BATCH_SIZE must be greater than zero"
        );
        ensure!(
            self.telemetry_flush_interval_ms > 0,
            "TELEMETRY_FLUSH_INTERVAL_MS must be greater than zero"
        );

        validate_url("BINANCE_WS_URL", &self.binance_ws_url, &["ws", "wss"])?;
        validate_url(
            "POLYMARKET_CLOB_URL",
            &self.polymarket_clob_url,
            &["http", "https"],
        )?;
        if self.clickhouse_enabled() {
            validate_url("CLICKHOUSE_URL", &self.clickhouse_url, &["http", "https"])?;
        }
        validate_identifier("CLICKHOUSE_DATABASE", &self.clickhouse_database)?;

        Ok(())
    }

    pub fn clickhouse_enabled(&self) -> bool {
        !self.clickhouse_url.trim().is_empty()
    }
}

fn validate_url(name: &str, value: &str, allowed_schemes: &[&str]) -> anyhow::Result<()> {
    let (scheme, rest) = value
        .split_once("://")
        .with_context(|| format!("{name} must be an absolute URL"))?;
    ensure!(
        allowed_schemes.contains(&scheme),
        "{name} has unsupported scheme {scheme}"
    );
    ensure!(!rest.trim().is_empty(), "{name} must include a host");
    Ok(())
}

fn validate_identifier(name: &str, value: &str) -> anyhow::Result<()> {
    ensure!(
        !value.is_empty()
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
        "{name} must contain only ASCII letters, digits, or underscores"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::AppConfig;

    fn config() -> AppConfig {
        AppConfig {
            service_name: "poly-bot-trading-engine".into(),
            gcp_project_id: "poly-bot-502515".into(),
            gcp_region: "us-east1".into(),
            binance_ws_url: "wss://stream.binance.com:9443/ws".into(),
            polymarket_clob_url: "https://clob.polymarket.com".into(),
            clickhouse_url: String::new(),
            clickhouse_database: "poly_bot".into(),
            clickhouse_user: "default".into(),
            clickhouse_password: String::new(),
            telemetry_channel_capacity: 8192,
            telemetry_batch_size: 200,
            telemetry_flush_interval_ms: 100,
        }
    }

    #[test]
    fn default_shape_is_valid() {
        config().validate().unwrap();
    }

    #[test]
    fn rejects_zero_sized_telemetry_channel() {
        let mut config = config();
        config.telemetry_channel_capacity = 0;
        assert!(config.validate().is_err());
    }
}

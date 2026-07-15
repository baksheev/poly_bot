use std::path::PathBuf;

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
    /// Run the read-only market-data shadow service.
    Run,
    /// Create ClickHouse telemetry tables.
    Migrate,
    /// Validate configuration without connecting to external systems.
    Check,
    /// Hydrate DEX pools at one canonical block without starting the service.
    Hydrate,
    /// Validate Binance credentials and hydrate sanitized Spot account metadata.
    BinanceAccount,
    /// Hydrate sanitized Binance direct and Optimism fallback network state.
    BinanceCapital,
}

#[derive(Parser, Debug, Clone)]
pub struct AppConfig {
    #[arg(long, env = "SERVICE_NAME", default_value = "arb-bot-rust-shadow")]
    pub service_name: String,

    #[arg(long, env = "ENGINE_ID", default_value = "arb-bot-rust-shadow-local")]
    pub engine_id: String,

    #[arg(long, env = "GCP_PROJECT_ID", default_value = "poly-bot-502515")]
    pub gcp_project_id: String,

    #[arg(long, env = "GCP_REGION", default_value = "asia-southeast1")]
    pub gcp_region: String,

    #[arg(
        long,
        env = "BINANCE_WS_BASE_URL",
        default_value = "wss://stream.binance.com:9443/ws"
    )]
    pub binance_ws_base_url: String,

    #[arg(
        long,
        env = "BINANCE_REST_BASE_URL",
        default_value = "https://api.binance.com"
    )]
    pub binance_rest_base_url: String,

    #[arg(
        long,
        env = "DOMAIN_CONFIG_PATH",
        default_value = "config/strategies/usdc-wld-world-chain.v2.json"
    )]
    pub domain_config_path: PathBuf,

    #[arg(long, env = "MARKET_DATA_MAX_AGE_MS", default_value_t = 5_000)]
    pub market_data_max_age_ms: u64,

    #[arg(long, env = "DEX_EVENT_CHANNEL_CAPACITY", default_value_t = 8192)]
    pub dex_event_channel_capacity: usize,

    #[arg(long, env = "DEX_HEAD_MAX_AGE_MS", default_value_t = 10_000)]
    pub dex_head_max_age_ms: u64,

    #[arg(long, env = "CLICKHOUSE_URL", default_value = "")]
    pub clickhouse_url: String,

    #[arg(long, env = "CLICKHOUSE_DATABASE", default_value = "arb_bot")]
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
        validate_non_empty("SERVICE_NAME", &self.service_name)?;
        validate_runtime_identifier("ENGINE_ID", &self.engine_id)?;
        validate_non_empty("GCP_PROJECT_ID", &self.gcp_project_id)?;
        validate_non_empty("GCP_REGION", &self.gcp_region)?;

        ensure!(
            self.market_data_max_age_ms > 0,
            "MARKET_DATA_MAX_AGE_MS must be greater than zero"
        );
        ensure!(
            self.dex_event_channel_capacity > 0,
            "DEX_EVENT_CHANNEL_CAPACITY must be greater than zero"
        );
        ensure!(
            self.dex_head_max_age_ms > 0,
            "DEX_HEAD_MAX_AGE_MS must be greater than zero"
        );
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

        validate_url(
            "BINANCE_WS_BASE_URL",
            &self.binance_ws_base_url,
            &["ws", "wss"],
        )?;
        validate_binance_spot_ws_base_url(&self.binance_ws_base_url)?;
        validate_url(
            "BINANCE_REST_BASE_URL",
            &self.binance_rest_base_url,
            &["https"],
        )?;
        validate_binance_spot_rest_base_url(&self.binance_rest_base_url)?;
        ensure!(
            !self.domain_config_path.as_os_str().is_empty(),
            "DOMAIN_CONFIG_PATH is empty"
        );

        if self.clickhouse_enabled() {
            validate_url("CLICKHOUSE_URL", &self.clickhouse_url, &["http", "https"])?;
        }
        validate_sql_identifier("CLICKHOUSE_DATABASE", &self.clickhouse_database)?;

        Ok(())
    }

    pub fn clickhouse_enabled(&self) -> bool {
        !self.clickhouse_url.trim().is_empty()
    }
}

fn validate_non_empty(name: &str, value: &str) -> anyhow::Result<()> {
    ensure!(!value.trim().is_empty(), "{name} is empty");
    Ok(())
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

fn validate_runtime_identifier(name: &str, value: &str) -> anyhow::Result<()> {
    ensure!(
        !value.is_empty()
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')),
        "{name} must contain only ASCII letters, digits, underscores, or hyphens"
    );
    Ok(())
}

fn validate_binance_spot_ws_base_url(value: &str) -> anyhow::Result<()> {
    let normalized = value.trim_end_matches('/');
    ensure!(
        matches!(
            normalized,
            "wss://stream.binance.com:9443/ws" | "wss://stream.binance.com:443/ws"
        ),
        "BINANCE_WS_BASE_URL must use the Binance Spot raw-stream endpoint"
    );
    Ok(())
}

fn validate_binance_spot_rest_base_url(value: &str) -> anyhow::Result<()> {
    let normalized = value.trim_end_matches('/');
    ensure!(
        matches!(
            normalized,
            "https://api.binance.com"
                | "https://api-gcp.binance.com"
                | "https://api1.binance.com"
                | "https://api2.binance.com"
                | "https://api3.binance.com"
                | "https://api4.binance.com"
        ),
        "BINANCE_REST_BASE_URL must use an official Binance Spot production endpoint"
    );
    Ok(())
}

fn validate_sql_identifier(name: &str, value: &str) -> anyhow::Result<()> {
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
            service_name: "arb-bot-rust-shadow".into(),
            engine_id: "arb-bot-rust-shadow-test".into(),
            gcp_project_id: "poly-bot-502515".into(),
            gcp_region: "asia-southeast1".into(),
            binance_ws_base_url: "wss://stream.binance.com:9443/ws".into(),
            binance_rest_base_url: "https://api.binance.com".into(),
            domain_config_path: "config/strategies/usdc-wld-world-chain.v2.json".into(),
            market_data_max_age_ms: 5_000,
            dex_event_channel_capacity: 8192,
            dex_head_max_age_ms: 10_000,
            clickhouse_url: String::new(),
            clickhouse_database: "arb_bot".into(),
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
    fn rejects_empty_domain_config_path() {
        let mut config = config();
        config.domain_config_path = "".into();
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_unsafe_clickhouse_database_identifier() {
        let mut config = config();
        config.clickhouse_database = "arb-bot".into();
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_binance_futures_stream() {
        let mut config = config();
        config.binance_ws_base_url = "wss://fstream.binance.com/public/ws".into();
        assert!(config.validate().is_err());
    }
}

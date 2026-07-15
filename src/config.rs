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
        default_value = "wss://fstream.binance.com/public/ws"
    )]
    pub binance_ws_base_url: String,

    #[arg(
        long,
        env = "BINANCE_SYMBOLS",
        value_delimiter = ',',
        default_value = "WLDUSDC"
    )]
    pub binance_symbols: Vec<String>,

    #[arg(long, env = "MARKET_EVENT_CHANNEL_CAPACITY", default_value_t = 8192)]
    pub market_event_channel_capacity: usize,

    #[arg(long, env = "MARKET_DATA_MAX_AGE_MS", default_value_t = 5_000)]
    pub market_data_max_age_ms: u64,

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
            self.market_event_channel_capacity > 0,
            "MARKET_EVENT_CHANNEL_CAPACITY must be greater than zero"
        );
        ensure!(
            self.market_data_max_age_ms > 0,
            "MARKET_DATA_MAX_AGE_MS must be greater than zero"
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
        ensure!(
            !self.binance_symbols.is_empty(),
            "BINANCE_SYMBOLS must contain at least one symbol"
        );
        for symbol in &self.binance_symbols {
            validate_binance_symbol(symbol)?;
        }

        if self.clickhouse_enabled() {
            validate_url("CLICKHOUSE_URL", &self.clickhouse_url, &["http", "https"])?;
        }
        validate_sql_identifier("CLICKHOUSE_DATABASE", &self.clickhouse_database)?;

        Ok(())
    }

    pub fn clickhouse_enabled(&self) -> bool {
        !self.clickhouse_url.trim().is_empty()
    }

    pub fn normalized_binance_symbols(&self) -> Vec<String> {
        self.binance_symbols
            .iter()
            .map(|symbol| symbol.trim().to_ascii_uppercase())
            .collect()
    }
}

fn validate_non_empty(name: &str, value: &str) -> anyhow::Result<()> {
    ensure!(!value.trim().is_empty(), "{name} is empty");
    Ok(())
}

fn validate_binance_symbol(value: &str) -> anyhow::Result<()> {
    let symbol = value.trim();
    ensure!(
        !symbol.is_empty(),
        "BINANCE_SYMBOLS contains an empty symbol"
    );
    ensure!(
        symbol.len() <= 32 && symbol.bytes().all(|byte| byte.is_ascii_alphanumeric()),
        "invalid Binance symbol {symbol}"
    );
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
            binance_ws_base_url: "wss://fstream.binance.com/public/ws".into(),
            binance_symbols: vec!["wldusdc".into()],
            market_event_channel_capacity: 8192,
            market_data_max_age_ms: 5_000,
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
    fn symbols_are_normalized_once_at_startup() {
        assert_eq!(config().normalized_binance_symbols(), ["WLDUSDC"]);
    }

    #[test]
    fn rejects_zero_sized_market_channel() {
        let mut config = config();
        config.market_event_channel_capacity = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_invalid_symbol() {
        let mut config = config();
        config.binance_symbols = vec!["WLD/USDC".into()];
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_unsafe_clickhouse_database_identifier() {
        let mut config = config();
        config.clickhouse_database = "arb-bot".into();
        assert!(config.validate().is_err());
    }
}

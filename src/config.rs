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
    /// Hydrate one EVM deposit address and optional deposit/withdrawal recovery evidence.
    BinanceCapitalRecovery {
        #[arg(long)]
        coin: String,
        #[arg(long)]
        network: String,
        #[arg(long)]
        deposit_transaction_hash: Option<String>,
        #[arg(long)]
        withdraw_order_id: Option<String>,
    },
    /// Read recent WLDUSDC orders created by the Rust validation client.
    BinanceRecentValidationOrders {
        #[arg(long, default_value_t = 20)]
        limit: u16,
    },
    /// Read one Binance withdrawal by its deterministic client id.
    BinanceWithdrawalStatus {
        #[arg(long)]
        coin: String,
        #[arg(long)]
        withdraw_order_id: String,
    },
    /// Read one Binance Travel Rule withdrawal by its travel-rule id.
    BinanceTravelRuleWithdrawalStatus {
        #[arg(long)]
        tr_id: i64,
    },
    /// Fetch and validate a public unauthenticated Across USDC quote.
    AcrossUsdcQuote {
        /// Origin chain: 10 (Optimism) or 480 (World Chain).
        #[arg(long)]
        origin_chain_id: u64,
        /// Exact input in USDC base units (1 USDC = 1,000,000).
        #[arg(long)]
        amount: u128,
    },
    /// Derive and print only the public address of the configured EVM wallet.
    WalletAddress,
    /// Hydrate nonce, native gas, and WLD/USDC balances on World Chain and Optimism.
    WalletHydrate,
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
        env = "BINANCE_WS_API_URL",
        default_value = "wss://ws-api.binance.com:443/ws-api/v3"
    )]
    pub binance_ws_api_url: String,

    #[arg(
        long,
        env = "ACROSS_API_BASE_URL",
        default_value = "https://app.across.to/api"
    )]
    pub across_api_base_url: String,

    #[arg(
        long,
        env = "DOMAIN_CONFIG_PATH",
        default_value = "config/strategies/usdc-wld-world-chain.v3.json"
    )]
    pub domain_config_path: PathBuf,

    #[arg(long, env = "MARKET_DATA_MAX_AGE_MS", default_value_t = 5_000)]
    pub market_data_max_age_ms: u64,

    #[arg(long, env = "DEX_EVENT_CHANNEL_CAPACITY", default_value_t = 8192)]
    pub dex_event_channel_capacity: usize,

    #[arg(long, env = "DEX_HEAD_MAX_AGE_MS", default_value_t = 10_000)]
    pub dex_head_max_age_ms: u64,

    #[arg(long, env = "BALANCE_SYNC_INTERVAL_MS", default_value_t = 1_000)]
    pub balance_sync_interval_ms: u64,

    #[arg(long, env = "BALANCE_MAX_AGE_MS", default_value_t = 5_000)]
    pub balance_max_age_ms: u64,

    #[arg(long, env = "BALANCE_EVENT_CHANNEL_CAPACITY", default_value_t = 16)]
    pub balance_event_channel_capacity: usize,

    #[arg(long, env = "REBALANCE_EXECUTION_MODE", default_value = "disabled")]
    pub rebalance_execution_mode: String,

    #[arg(
        long,
        env = "REBALANCE_EXECUTOR_JOURNAL_PATH",
        default_value = "/var/lib/arb-bot/rebalance-executor.jsonl"
    )]
    pub rebalance_executor_journal_path: PathBuf,

    #[arg(
        long,
        env = "REBALANCE_EXECUTOR_TIMEOUT_SECONDS",
        default_value_t = 1_800
    )]
    pub rebalance_executor_timeout_seconds: u64,

    #[arg(long, env = "REBALANCE_MAX_WLD_AMOUNT", default_value = "0")]
    pub rebalance_max_wld_amount: rust_decimal::Decimal,

    #[arg(long, env = "REBALANCE_MAX_USDC_AMOUNT", default_value = "0")]
    pub rebalance_max_usdc_amount: rust_decimal::Decimal,

    #[arg(long, env = "REBALANCE_LIVE_CONFIRMATION", default_value = "")]
    pub rebalance_live_confirmation: String,

    #[arg(
        long,
        env = "REBALANCE_BINANCE_WITHDRAWAL_API_MODE",
        default_value = "standard"
    )]
    pub rebalance_binance_withdrawal_api_mode: String,

    #[arg(long, env = "EVM_WALLET_ADDRESS", default_value = "")]
    pub evm_wallet_address: String,

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
            self.balance_sync_interval_ms > 0,
            "BALANCE_SYNC_INTERVAL_MS must be greater than zero"
        );
        ensure!(
            self.balance_max_age_ms > self.balance_sync_interval_ms,
            "BALANCE_MAX_AGE_MS must be greater than BALANCE_SYNC_INTERVAL_MS"
        );
        ensure!(
            self.balance_event_channel_capacity > 0,
            "BALANCE_EVENT_CHANNEL_CAPACITY must be greater than zero"
        );
        ensure!(
            matches!(
                self.rebalance_execution_mode.as_str(),
                "disabled" | "full_live"
            ),
            "REBALANCE_EXECUTION_MODE must be disabled or full_live"
        );
        ensure!(
            !self.rebalance_executor_journal_path.as_os_str().is_empty(),
            "REBALANCE_EXECUTOR_JOURNAL_PATH is empty"
        );
        ensure!(
            (60..=86_400).contains(&self.rebalance_executor_timeout_seconds),
            "REBALANCE_EXECUTOR_TIMEOUT_SECONDS must be between 60 and 86400"
        );
        ensure!(
            self.rebalance_max_wld_amount >= rust_decimal::Decimal::ZERO
                && self.rebalance_max_usdc_amount >= rust_decimal::Decimal::ZERO,
            "rebalance live amount limits must not be negative"
        );
        ensure!(
            matches!(
                self.rebalance_binance_withdrawal_api_mode.as_str(),
                "standard" | "travel_rule"
            ),
            "REBALANCE_BINANCE_WITHDRAWAL_API_MODE must be standard or travel_rule"
        );
        if self.rebalance_execution_mode == "full_live" {
            ensure!(
                self.rebalance_live_confirmation == "ENABLE_FULL_REBALANCE",
                "full_live rebalance requires REBALANCE_LIVE_CONFIRMATION=ENABLE_FULL_REBALANCE"
            );
            ensure!(
                self.rebalance_max_wld_amount > rust_decimal::Decimal::ZERO
                    && self.rebalance_max_usdc_amount > rust_decimal::Decimal::ZERO,
                "full_live rebalance requires positive WLD and USDC amount limits"
            );
        }
        if !self.evm_wallet_address.trim().is_empty() {
            self.evm_wallet_address
                .parse::<alloy_primitives::Address>()
                .context("EVM_WALLET_ADDRESS is invalid")?;
        }
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
        validate_url("BINANCE_WS_API_URL", &self.binance_ws_api_url, &["wss"])?;
        validate_binance_spot_ws_api_url(&self.binance_ws_api_url)?;
        validate_url("ACROSS_API_BASE_URL", &self.across_api_base_url, &["https"])?;
        ensure!(
            self.across_api_base_url.trim_end_matches('/') == "https://app.across.to/api",
            "ACROSS_API_BASE_URL must use the Rails-compatible public Across endpoint"
        );
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

fn validate_binance_spot_ws_api_url(value: &str) -> anyhow::Result<()> {
    let normalized = value.trim_end_matches('/');
    ensure!(
        matches!(
            normalized,
            "wss://ws-api.binance.com:443/ws-api/v3" | "wss://ws-api.binance.com:9443/ws-api/v3"
        ),
        "BINANCE_WS_API_URL must use an official Binance Spot WebSocket API endpoint"
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
            binance_ws_api_url: "wss://ws-api.binance.com:443/ws-api/v3".into(),
            across_api_base_url: "https://app.across.to/api".into(),
            domain_config_path: "config/strategies/usdc-wld-world-chain.v3.json".into(),
            market_data_max_age_ms: 5_000,
            dex_event_channel_capacity: 8192,
            dex_head_max_age_ms: 10_000,
            balance_sync_interval_ms: 1_000,
            balance_max_age_ms: 5_000,
            balance_event_channel_capacity: 16,
            rebalance_execution_mode: "disabled".into(),
            rebalance_executor_journal_path: "/tmp/rebalance-executor.jsonl".into(),
            rebalance_executor_timeout_seconds: 1_800,
            rebalance_max_wld_amount: rust_decimal::Decimal::ZERO,
            rebalance_max_usdc_amount: rust_decimal::Decimal::ZERO,
            rebalance_live_confirmation: String::new(),
            rebalance_binance_withdrawal_api_mode: "standard".into(),
            evm_wallet_address: String::new(),
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
    fn full_rebalance_requires_confirmation_and_positive_limits() {
        let mut value = config();
        value.rebalance_execution_mode = "full_live".into();
        assert!(value.validate().is_err());

        value.rebalance_live_confirmation = "ENABLE_FULL_REBALANCE".into();
        value.rebalance_max_wld_amount = rust_decimal::Decimal::ONE;
        value.rebalance_max_usdc_amount = rust_decimal::Decimal::from(100);
        value.validate().unwrap();
    }

    #[test]
    fn retired_canary_execution_mode_is_rejected() {
        let mut value = config();
        value.rebalance_execution_mode = "direct_wld_canary".into();
        assert!(value.validate().is_err());
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

    #[test]
    fn rejects_non_official_across_endpoint() {
        let mut config = config();
        config.across_api_base_url = "https://example.com/api".into();

        assert!(config.validate().is_err());
    }

    #[test]
    fn validates_configured_public_wallet_address() {
        let mut config = config();
        config.evm_wallet_address = "not-an-address".into();
        assert!(config.validate().is_err());

        config.evm_wallet_address = "0x90D990C81320221D2882De32beeA78923c1e77A3".into();
        config.validate().unwrap();
    }
}

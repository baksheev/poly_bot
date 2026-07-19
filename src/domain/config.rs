use std::{
    collections::HashSet,
    fmt::Write,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, ensure};
use rust_decimal::Decimal;
use serde::Deserialize;
use sha2::{Digest, Sha256};

pub const SUPPORTED_SCHEMA_VERSION: u32 = 1;

#[derive(Debug)]
pub struct LoadedDomainConfig {
    path: PathBuf,
    fingerprint_sha256: String,
    snapshot: DomainSnapshot,
}

impl LoadedDomainConfig {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let bytes = fs::read(path)
            .with_context(|| format!("failed to read domain config {}", path.display()))?;
        Self::from_bytes(path, &bytes)
    }

    fn from_bytes(path: impl AsRef<Path>, bytes: &[u8]) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let snapshot: DomainSnapshot = serde_json::from_slice(bytes)
            .with_context(|| format!("failed to parse domain config {}", path.display()))?;
        snapshot
            .validate()
            .with_context(|| format!("invalid domain config {}", path.display()))?;

        let digest = Sha256::digest(bytes);
        let mut fingerprint_sha256 = String::with_capacity(digest.len() * 2);
        for byte in digest {
            write!(&mut fingerprint_sha256, "{byte:02x}")
                .expect("writing a SHA-256 digest to String cannot fail");
        }

        Ok(Self {
            path: path.to_owned(),
            fingerprint_sha256,
            snapshot,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn fingerprint_sha256(&self) -> &str {
        &self.fingerprint_sha256
    }

    pub fn snapshot(&self) -> &DomainSnapshot {
        &self.snapshot
    }

    pub fn binance_symbols(&self) -> Vec<String> {
        self.snapshot
            .pairs
            .iter()
            .filter(|pair| pair.market_data_enabled)
            .map(|pair| pair.binance.symbol.clone())
            .collect()
    }

    pub fn pair_ids(&self) -> Vec<&str> {
        self.snapshot
            .pairs
            .iter()
            .map(|pair| pair.id.as_str())
            .collect()
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DomainSnapshot {
    pub schema_version: u32,
    pub snapshot_id: String,
    pub source: SnapshotSource,
    pub live_trading_enabled: bool,
    pub pairs: Vec<PairConfig>,
}

impl DomainSnapshot {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.schema_version == SUPPORTED_SCHEMA_VERSION,
            "unsupported schema_version {}; expected {}",
            self.schema_version,
            SUPPORTED_SCHEMA_VERSION
        );
        validate_runtime_id("snapshot_id", &self.snapshot_id)?;
        self.source.validate()?;
        ensure!(!self.pairs.is_empty(), "pairs must not be empty");

        let mut pair_ids = HashSet::new();
        let mut binance_symbols = HashSet::new();
        let mut enabled_market_data_pairs = 0_usize;
        let mut enabled_execution_pairs = 0_usize;
        for pair in &self.pairs {
            pair.validate()?;
            ensure!(pair_ids.insert(&pair.id), "duplicate pair id {}", pair.id);
            ensure!(
                binance_symbols.insert(&pair.binance.symbol),
                "duplicate Binance symbol {}",
                pair.binance.symbol
            );
            enabled_market_data_pairs += usize::from(pair.market_data_enabled);
            enabled_execution_pairs += usize::from(pair.execution_enabled);
        }
        ensure!(
            enabled_market_data_pairs > 0,
            "at least one pair must have market_data_enabled"
        );
        ensure!(
            self.live_trading_enabled == (enabled_execution_pairs > 0),
            "live_trading_enabled must exactly match whether any pair enables execution"
        );
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotSource {
    pub repository: String,
    pub revision: String,
    pub rails_pair_id: u64,
    pub rails_pair_updated_at_utc: String,
    pub captured_at_utc: String,
    pub evidence: Vec<String>,
}

impl SnapshotSource {
    fn validate(&self) -> anyhow::Result<()> {
        validate_non_empty("source.repository", &self.repository)?;
        ensure!(
            self.revision.len() == 40 && self.revision.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "source.revision must be a 40-character Git commit"
        );
        ensure!(
            self.rails_pair_id > 0,
            "source.rails_pair_id must be positive"
        );
        validate_non_empty(
            "source.rails_pair_updated_at_utc",
            &self.rails_pair_updated_at_utc,
        )?;
        validate_non_empty("source.captured_at_utc", &self.captured_at_utc)?;
        ensure!(
            !self.evidence.is_empty(),
            "source.evidence must not be empty"
        );
        for item in &self.evidence {
            validate_non_empty("source.evidence item", item)?;
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PairConfig {
    pub id: String,
    pub market_data_enabled: bool,
    pub execution_enabled: bool,
    pub chain: ChainConfig,
    pub token_a: TokenConfig,
    pub token_b: TokenConfig,
    pub binance: BinanceConfig,
    pub quote_sizing: QuoteSizingConfig,
    pub strategy: StrategyConfig,
    #[serde(default)]
    pub rebalance: RebalanceConfig,
    pub dex: DexConfig,
}

impl PairConfig {
    fn validate(&self) -> anyhow::Result<()> {
        validate_runtime_id("pair.id", &self.id)?;
        ensure!(
            !self.execution_enabled || self.market_data_enabled,
            "pair {} cannot enable execution without market data",
            self.id
        );
        self.chain.validate()?;
        self.token_a.validate("token_a", self.chain.chain_id)?;
        self.token_b.validate("token_b", self.chain.chain_id)?;
        ensure!(
            self.token_a.contract != self.token_b.contract,
            "pair {} token contracts must differ",
            self.id
        );
        self.binance.validate(&self.token_a, &self.token_b)?;
        self.quote_sizing.validate()?;
        self.strategy.validate()?;
        self.rebalance.validate()?;
        self.dex.validate(&self.chain)?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebalanceConfig {
    pub enabled: bool,
    pub start_threshold_bps: u16,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            start_threshold_bps: 2_500,
        }
    }
}

impl RebalanceConfig {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.start_threshold_bps > 0 && self.start_threshold_bps < 5_000,
            "rebalance.start_threshold_bps must be between 1 and 4999"
        );
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainConfig {
    pub name: String,
    pub chain_id: u64,
    pub rpc_url_env: String,
    pub ws_url_env: String,
    pub binance_network_name: String,
    pub gas_symbol: String,
    pub gas_decimals: u8,
    pub gas_price_binance_symbol: Option<String>,
    pub multicall3_address: String,
    pub uniswap_v3_factory_address: Option<String>,
    pub uniswap_v3_quoter_address: Option<String>,
    pub uniswap_v3_router_address: Option<String>,
    pub uniswap_v4_quoter_address: Option<String>,
    pub uniswap_v4_router_address: Option<String>,
    pub uniswap_v4_pool_manager_address: Option<String>,
    pub uniswap_v4_state_view_address: Option<String>,
}

impl ChainConfig {
    fn validate(&self) -> anyhow::Result<()> {
        validate_non_empty("chain.name", &self.name)?;
        ensure!(self.chain_id > 0, "chain.chain_id must be positive");
        validate_env_name("chain.rpc_url_env", &self.rpc_url_env)?;
        validate_env_name("chain.ws_url_env", &self.ws_url_env)?;
        validate_symbol("chain.binance_network_name", &self.binance_network_name)?;
        validate_symbol("chain.gas_symbol", &self.gas_symbol)?;
        ensure!(self.gas_decimals > 0, "chain.gas_decimals must be positive");
        if let Some(symbol) = &self.gas_price_binance_symbol {
            validate_symbol("chain.gas_price_binance_symbol", symbol)?;
        }
        validate_evm_address("chain.multicall3_address", &self.multicall3_address)?;
        validate_optional_address(
            "chain.uniswap_v3_factory_address",
            self.uniswap_v3_factory_address.as_deref(),
        )?;
        validate_optional_address(
            "chain.uniswap_v3_quoter_address",
            self.uniswap_v3_quoter_address.as_deref(),
        )?;
        validate_optional_address(
            "chain.uniswap_v3_router_address",
            self.uniswap_v3_router_address.as_deref(),
        )?;
        validate_optional_address(
            "chain.uniswap_v4_quoter_address",
            self.uniswap_v4_quoter_address.as_deref(),
        )?;
        validate_optional_address(
            "chain.uniswap_v4_router_address",
            self.uniswap_v4_router_address.as_deref(),
        )?;
        validate_optional_address(
            "chain.uniswap_v4_pool_manager_address",
            self.uniswap_v4_pool_manager_address.as_deref(),
        )?;
        validate_optional_address(
            "chain.uniswap_v4_state_view_address",
            self.uniswap_v4_state_view_address.as_deref(),
        )?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenConfig {
    pub symbol: String,
    pub contract: String,
    pub decimals: u8,
}

impl TokenConfig {
    fn validate(&self, name: &str, chain_id: u64) -> anyhow::Result<()> {
        validate_symbol(&format!("{name}.symbol"), &self.symbol)?;
        validate_evm_address(&format!("{name}.contract"), &self.contract)?;
        ensure!(
            self.decimals <= 36,
            "{name}.decimals {} is implausible on chain {chain_id}",
            self.decimals
        );
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BinanceConfig {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub market_data_product: BinanceProduct,
    pub execution_product: BinanceProduct,
    pub step_size: String,
    pub tick_size: String,
}

impl BinanceConfig {
    fn validate(&self, token_a: &TokenConfig, token_b: &TokenConfig) -> anyhow::Result<()> {
        validate_symbol("binance.symbol", &self.symbol)?;
        validate_symbol("binance.base_asset", &self.base_asset)?;
        validate_symbol("binance.quote_asset", &self.quote_asset)?;
        ensure!(
            self.base_asset == token_b.symbol,
            "Binance base_asset must match token_b"
        );
        ensure!(
            self.quote_asset == token_a.symbol,
            "Binance quote_asset must match token_a"
        );
        ensure!(
            self.symbol == format!("{}{}", self.base_asset, self.quote_asset),
            "Binance symbol must equal base_asset + quote_asset"
        );
        ensure!(
            self.market_data_product == BinanceProduct::Spot,
            "opportunity sizing requires Binance Spot market data"
        );
        ensure!(
            self.execution_product == BinanceProduct::Spot,
            "arb_bot execution parity requires Binance spot"
        );
        validate_positive_decimal("binance.step_size", &self.step_size)?;
        validate_positive_decimal("binance.tick_size", &self.tick_size)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BinanceProduct {
    Spot,
    UsdMFutures,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuoteSizingConfig {
    pub token_a_base_units: String,
    pub token_b: TokenBQuoteSizing,
}

impl QuoteSizingConfig {
    fn validate(&self) -> anyhow::Result<()> {
        validate_positive_base_units("quote_sizing.token_a_base_units", &self.token_a_base_units)?;
        match self.token_b {
            TokenBQuoteSizing::DeriveFromBinanceAsk => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum TokenBQuoteSizing {
    DeriveFromBinanceAsk,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyConfig {
    pub kind: ArbitrageStrategy,
    pub opportunity_threshold_bps: u16,
    pub max_quote_age_ms: u64,
    pub min_slippage_bps: u16,
    pub max_slippage_bps: u16,
    pub slippage_profit_share_bps: u16,
    pub dex_fee_reserve_bps: u16,
    pub balance_safety_multiplier: u16,
}

impl StrategyConfig {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.kind == ArbitrageStrategy::ProfitTokenA,
            "production clone snapshot must use profit_token_a"
        );
        validate_bps_positive(
            "strategy.opportunity_threshold_bps",
            self.opportunity_threshold_bps,
        )?;
        ensure!(
            self.max_quote_age_ms > 0,
            "strategy.max_quote_age_ms must be positive"
        );
        validate_bps("strategy.min_slippage_bps", self.min_slippage_bps)?;
        validate_bps("strategy.max_slippage_bps", self.max_slippage_bps)?;
        ensure!(
            self.min_slippage_bps <= self.max_slippage_bps,
            "strategy min_slippage_bps exceeds max_slippage_bps"
        );
        validate_bps(
            "strategy.slippage_profit_share_bps",
            self.slippage_profit_share_bps,
        )?;
        validate_bps("strategy.dex_fee_reserve_bps", self.dex_fee_reserve_bps)?;
        ensure!(
            self.balance_safety_multiplier > 0,
            "strategy.balance_safety_multiplier must be positive"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArbitrageStrategy {
    Legacy,
    ProfitTokenA,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DexConfig {
    pub allowed_providers: Vec<DexProvider>,
    pub uniswap_v3: Option<UniswapV3Config>,
    pub uniswap_v4: Option<UniswapV4Config>,
}

impl DexConfig {
    fn validate(&self, chain: &ChainConfig) -> anyhow::Result<()> {
        ensure!(
            !self.allowed_providers.is_empty(),
            "dex.allowed_providers must not be empty"
        );
        let unique: HashSet<_> = self.allowed_providers.iter().copied().collect();
        ensure!(
            unique.len() == self.allowed_providers.len(),
            "dex.allowed_providers contains duplicates"
        );

        if unique.contains(&DexProvider::UniswapV3) {
            ensure!(
                chain.uniswap_v3_factory_address.is_some(),
                "Uniswap V3 requires chain.uniswap_v3_factory_address"
            );
            self.uniswap_v3
                .as_ref()
                .context("Uniswap V3 provider requires dex.uniswap_v3")?
                .validate()?;
        } else {
            ensure!(
                self.uniswap_v3.is_none(),
                "dex.uniswap_v3 is configured but not allowed"
            );
        }

        if unique.contains(&DexProvider::UniswapV4) {
            ensure!(
                chain.uniswap_v4_pool_manager_address.is_some(),
                "Uniswap V4 requires chain.uniswap_v4_pool_manager_address"
            );
            ensure!(
                chain.uniswap_v4_state_view_address.is_some(),
                "Uniswap V4 requires chain.uniswap_v4_state_view_address"
            );
            self.uniswap_v4
                .as_ref()
                .context("Uniswap V4 provider requires dex.uniswap_v4")?
                .validate()?;
        } else {
            ensure!(
                self.uniswap_v4.is_none(),
                "dex.uniswap_v4 is configured but not allowed"
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Hash, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DexProvider {
    ZeroX,
    UniswapV3,
    UniswapV4,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UniswapV3Config {
    pub fee_tiers: Vec<u32>,
}

impl UniswapV3Config {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            !self.fee_tiers.is_empty(),
            "Uniswap V3 fee_tiers must not be empty"
        );
        let unique: HashSet<_> = self.fee_tiers.iter().copied().collect();
        ensure!(
            unique.len() == self.fee_tiers.len(),
            "Uniswap V3 fee_tiers contains duplicates"
        );
        ensure!(
            self.fee_tiers
                .iter()
                .all(|fee| *fee > 0 && *fee <= 1_000_000),
            "invalid Uniswap V3 fee tier"
        );
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UniswapV4Config {
    pub pools: Vec<UniswapV4PoolConfig>,
}

impl UniswapV4Config {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(!self.pools.is_empty(), "Uniswap V4 pools must not be empty");
        for pool in &self.pools {
            pool.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UniswapV4PoolConfig {
    pub fee_tier: u32,
    pub tick_spacing: i32,
    pub hooks: String,
}

impl UniswapV4PoolConfig {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.fee_tier > 0 && self.fee_tier <= 1_000_000,
            "invalid Uniswap V4 fee tier"
        );
        ensure!(
            self.tick_spacing > 0,
            "Uniswap V4 tick_spacing must be positive"
        );
        validate_evm_address("Uniswap V4 hooks", &self.hooks)
    }
}

fn validate_non_empty(name: &str, value: &str) -> anyhow::Result<()> {
    ensure!(!value.trim().is_empty(), "{name} is empty");
    Ok(())
}

fn validate_runtime_id(name: &str, value: &str) -> anyhow::Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')),
        "{name} contains invalid characters"
    );
    Ok(())
}

fn validate_symbol(name: &str, value: &str) -> anyhow::Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 32
            && value
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit()),
        "{name} must contain only uppercase ASCII letters or digits"
    );
    Ok(())
}

fn validate_env_name(name: &str, value: &str) -> anyhow::Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
            && value.as_bytes().first().is_some_and(u8::is_ascii_uppercase),
        "{name} is not a valid uppercase environment variable name"
    );
    Ok(())
}

fn validate_evm_address(name: &str, value: &str) -> anyhow::Result<()> {
    ensure!(
        value.len() == 42
            && value.starts_with("0x")
            && value[2..].bytes().all(|byte| byte.is_ascii_hexdigit()),
        "{name} is not a valid EVM address"
    );
    Ok(())
}

fn validate_optional_address(name: &str, value: Option<&str>) -> anyhow::Result<()> {
    if let Some(value) = value {
        validate_evm_address(name, value)?;
    }
    Ok(())
}

fn validate_positive_decimal(name: &str, value: &str) -> anyhow::Result<()> {
    let parsed = value
        .parse::<Decimal>()
        .with_context(|| format!("{name} is not a decimal string"))?;
    ensure!(parsed > Decimal::ZERO, "{name} must be positive");
    Ok(())
}

fn validate_positive_base_units(name: &str, value: &str) -> anyhow::Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 78
            && value.bytes().all(|byte| byte.is_ascii_digit())
            && !value.starts_with('0'),
        "{name} must be a positive canonical uint256 decimal string"
    );
    Ok(())
}

fn validate_bps(name: &str, value: u16) -> anyhow::Result<()> {
    ensure!(value <= 10_000, "{name} must be <= 10000");
    Ok(())
}

fn validate_bps_positive(name: &str, value: u16) -> anyhow::Result<()> {
    ensure!(value > 0, "{name} must be positive");
    validate_bps(name, value)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serde_json::Value;

    use super::{ArbitrageStrategy, BinanceProduct, LoadedDomainConfig, TokenBQuoteSizing};

    const CONFIG: &str = include_str!("../../config/strategies/usdc-wld-world-chain.v4.json");
    const LIVE_CONFIG: &str = include_str!("../../config/strategies/usdc-wld-world-chain.v5.json");

    fn load(bytes: &[u8]) -> anyhow::Result<LoadedDomainConfig> {
        LoadedDomainConfig::from_bytes(PathBuf::from("fixture.json"), bytes)
    }

    fn mutate(mutator: impl FnOnce(&mut Value)) -> Vec<u8> {
        let mut value: Value = serde_json::from_str(CONFIG).unwrap();
        mutator(&mut value);
        serde_json::to_vec(&value).unwrap()
    }

    #[test]
    fn committed_production_snapshot_is_valid_and_typed() {
        let loaded = load(CONFIG.as_bytes()).unwrap();
        let pair = &loaded.snapshot().pairs[0];

        assert_eq!(loaded.binance_symbols(), ["WLDUSDC"]);
        assert_eq!(pair.chain.chain_id, 480);
        assert_eq!(pair.binance.market_data_product, BinanceProduct::Spot);
        assert_eq!(pair.binance.execution_product, BinanceProduct::Spot);
        assert_eq!(
            pair.quote_sizing.token_b,
            TokenBQuoteSizing::DeriveFromBinanceAsk
        );
        assert_eq!(pair.strategy.kind, ArbitrageStrategy::ProfitTokenA);
        assert!(!pair.execution_enabled);
        assert_eq!(loaded.fingerprint_sha256().len(), 64);
    }

    #[test]
    fn fingerprint_is_stable_for_exact_artifact_bytes() {
        let first = load(CONFIG.as_bytes()).unwrap();
        let second = load(CONFIG.as_bytes()).unwrap();
        assert_eq!(first.fingerprint_sha256(), second.fingerprint_sha256());
        assert_eq!(
            first.fingerprint_sha256(),
            "0af151e7f264a8c4e383fe17552a77551f4be381367cbe6a6d2ce8da93f4267f"
        );
    }

    #[test]
    fn committed_live_snapshot_has_both_explicit_gates_and_a_stable_fingerprint() {
        let loaded = load(LIVE_CONFIG.as_bytes()).unwrap();
        assert!(loaded.snapshot().live_trading_enabled);
        assert!(loaded.snapshot().pairs[0].execution_enabled);
        assert_eq!(
            loaded.fingerprint_sha256(),
            "c5d21367d0e0ef37519d0e2dd30fedf828961e32704b0d16ba9a5cbae5c1657d"
        );
    }

    #[test]
    fn rejects_unknown_fields() {
        let bytes = mutate(|value| value["unexpected"] = Value::Bool(true));
        assert!(load(&bytes).is_err());
    }

    #[test]
    fn live_execution_gates_must_be_enabled_together() {
        let global_only = mutate(|value| value["live_trading_enabled"] = Value::Bool(true));
        assert!(load(&global_only).is_err());

        let pair_only = mutate(|value| value["pairs"][0]["execution_enabled"] = Value::Bool(true));
        assert!(load(&pair_only).is_err());

        let both = mutate(|value| {
            value["live_trading_enabled"] = Value::Bool(true);
            value["pairs"][0]["execution_enabled"] = Value::Bool(true);
        });
        assert!(load(&both).is_ok());
    }

    #[test]
    fn rejects_duplicate_binance_symbols() {
        let bytes = mutate(|value| {
            let duplicate = value["pairs"][0].clone();
            value["pairs"].as_array_mut().unwrap().push(duplicate);
        });
        assert!(load(&bytes).is_err());
    }

    #[test]
    fn rejects_futures_market_data_for_spot_execution() {
        let bytes = mutate(|value| {
            value["pairs"][0]["binance"]["market_data_product"] =
                Value::String("usd_m_futures".into());
        });
        assert!(load(&bytes).is_err());
    }

    #[test]
    fn rejects_credential_bearing_rpc_field() {
        let bytes = mutate(|value| {
            value["pairs"][0]["chain"]["rpc_url"] =
                Value::String("https://example.invalid/secret".into());
        });
        assert!(load(&bytes).is_err());
    }

    #[test]
    fn rejects_invalid_evm_address() {
        let bytes = mutate(|value| {
            value["pairs"][0]["token_a"]["contract"] = Value::String("0x1234".into());
        });
        assert!(load(&bytes).is_err());
    }

    #[test]
    fn rejects_rebalance_threshold_without_hysteresis() {
        let bytes = mutate(|value| {
            value["pairs"][0]["rebalance"]["start_threshold_bps"] = Value::from(5_000);
        });
        assert!(load(&bytes).is_err());
    }
}

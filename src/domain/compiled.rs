use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write,
    fs,
    path::{Path, PathBuf},
    str::FromStr,
    time::Instant,
};

use alloy_primitives::{Address, U256};
use anyhow::{Context, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::dex::pool_id::V4PoolKey;

use super::config::{DomainSnapshot, LoadedDomainConfig, PairConfig};

pub const COMPILED_DOMAIN_SCHEMA_VERSION: u32 = 1;
pub const COMPILED_DOMAIN_KIND: &str = "compiled_multi_pair_domain";

macro_rules! typed_id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl $name {
            pub fn new(value: impl Into<String>) -> anyhow::Result<Self> {
                let value = value.into();
                validate_id(stringify!($name), &value)?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

typed_id!(BinanceAccountId);
typed_id!(InstrumentId);
typed_id!(StrategyId);
typed_id!(NetworkId);
typed_id!(WalletId);
typed_id!(WalletLocationId);
typed_id!(ExecutionLaneId);
typed_id!(PoolId);
typed_id!(VenueAssetId);
typed_id!(EconomicAssetId);

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DomainCompilerManifest {
    pub schema_version: u32,
    pub bundle_id: String,
    pub compiler_version: String,
    pub source_paths: Vec<PathBuf>,
    pub accounts: Vec<AccountManifest>,
    pub wallets: Vec<WalletManifest>,
    pub v3_pools: Vec<V3PoolManifest>,
    pub reviewed_live_strategies: Vec<StrategyId>,
    pub stream_shard_symbol_capacity: usize,
    pub required_environment: Vec<EnvironmentRequirement>,
    pub journals: Vec<JournalManifest>,
}

impl DomainCompilerManifest {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let bytes = fs::read(path).with_context(|| {
            format!("failed to read domain compiler manifest {}", path.display())
        })?;
        let manifest: Self = serde_json::from_slice(&bytes).with_context(|| {
            format!(
                "failed to parse domain compiler manifest {}",
                path.display()
            )
        })?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn load_sources(&self, base: impl AsRef<Path>) -> anyhow::Result<Vec<LoadedDomainConfig>> {
        self.source_paths
            .iter()
            .map(|path| {
                let resolved = if path.is_absolute() {
                    path.clone()
                } else {
                    base.as_ref().join(path)
                };
                LoadedDomainConfig::load(resolved)
            })
            .collect()
    }

    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.schema_version == COMPILED_DOMAIN_SCHEMA_VERSION,
            "unsupported compiler manifest schema_version {}",
            self.schema_version
        );
        validate_id("bundle_id", &self.bundle_id)?;
        validate_id("compiler_version", &self.compiler_version)?;
        ensure!(
            !self.source_paths.is_empty(),
            "source_paths must not be empty"
        );
        ensure!(!self.accounts.is_empty(), "accounts must not be empty");
        ensure!(!self.wallets.is_empty(), "wallets must not be empty");
        ensure!(
            self.stream_shard_symbol_capacity > 0,
            "stream_shard_symbol_capacity must be positive"
        );
        unique_by(
            self.source_paths
                .iter()
                .map(|path| path.display().to_string()),
            "source path",
        )?;
        unique_by(
            self.accounts.iter().map(|item| item.id.0.clone()),
            "account id",
        )?;
        unique_by(
            self.wallets.iter().map(|item| item.id.0.clone()),
            "wallet id",
        )?;
        unique_by(
            self.v3_pools.iter().map(V3PoolManifest::key),
            "V3 pool resolution",
        )?;
        unique_by(
            self.reviewed_live_strategies
                .iter()
                .map(|item| item.0.clone()),
            "reviewed live strategy",
        )?;
        unique_by(
            self.required_environment
                .iter()
                .map(|item| item.projection_id.clone()),
            "environment projection",
        )?;
        unique_by(
            self.journals
                .iter()
                .map(|item| format!("{}:{}", item.owner_id, item.path_env)),
            "journal assignment",
        )?;
        for account in &self.accounts {
            account.validate()?;
        }
        for wallet in &self.wallets {
            wallet.validate()?;
        }
        for pool in &self.v3_pools {
            pool.validate()?;
        }
        for requirement in &self.required_environment {
            requirement.validate()?;
        }
        for journal in &self.journals {
            journal.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AccountManifest {
    pub id: BinanceAccountId,
    pub product: String,
    pub order_api_key_env: String,
    pub order_secret_key_env: String,
    pub treasury_api_key_env: String,
    pub treasury_secret_key_env: String,
    pub subaccount_email_env: String,
}

impl AccountManifest {
    fn validate(&self) -> anyhow::Result<()> {
        validate_id("account id", self.id.as_str())?;
        ensure!(
            self.product == "spot",
            "only Binance spot accounts are supported"
        );
        for name in [
            &self.order_api_key_env,
            &self.order_secret_key_env,
            &self.treasury_api_key_env,
            &self.treasury_secret_key_env,
            &self.subaccount_email_env,
        ] {
            validate_env_name(name)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WalletManifest {
    pub id: WalletId,
    pub address_env: String,
    pub private_key_env: String,
}

impl WalletManifest {
    fn validate(&self) -> anyhow::Result<()> {
        validate_id("wallet id", self.id.as_str())?;
        validate_env_name(&self.address_env)?;
        validate_env_name(&self.private_key_env)
    }
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct V3PoolManifest {
    pub pair_id: String,
    pub fee_pips: u32,
    pub address: String,
    pub lifecycle: PoolLifecycle,
}

impl V3PoolManifest {
    fn key(&self) -> String {
        format!("{}:{}", self.pair_id, self.fee_pips)
    }

    fn validate(&self) -> anyhow::Result<()> {
        validate_id("V3 pair id", &self.pair_id)?;
        ensure!(
            self.fee_pips > 0 && self.fee_pips < 1_000_000,
            "invalid V3 fee"
        );
        parse_address(&self.address).map(|_| ())
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PoolLifecycle {
    Validated,
    ExecutionEligible,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentRequirement {
    pub projection_id: String,
    pub names: Vec<String>,
}

impl EnvironmentRequirement {
    fn validate(&self) -> anyhow::Result<()> {
        validate_id("environment projection_id", &self.projection_id)?;
        ensure!(
            !self.names.is_empty(),
            "environment names must not be empty"
        );
        unique_by(self.names.iter().cloned(), "environment name")?;
        for name in &self.names {
            validate_env_name(name)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JournalManifest {
    pub owner_id: String,
    pub path_env: String,
}

impl JournalManifest {
    fn validate(&self) -> anyhow::Result<()> {
        validate_id("journal owner_id", &self.owner_id)?;
        validate_env_name(&self.path_env)
    }
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompiledDomainBundle {
    pub bundle_kind: String,
    pub schema_version: u32,
    pub bundle_id: String,
    pub compiler_version: String,
    pub sources: Vec<CompiledSource>,
    pub accounts: Vec<AccountNode>,
    pub instruments: Vec<InstrumentNode>,
    pub networks: Vec<NetworkNode>,
    pub wallets: Vec<WalletNode>,
    pub wallet_locations: Vec<WalletLocationNode>,
    pub venue_assets: Vec<VenueAssetNode>,
    pub economic_assets: Vec<EconomicAssetNode>,
    pub asset_mappings: Vec<AssetMapping>,
    pub pools: Vec<PoolNode>,
    pub strategies: Vec<StrategyNode>,
    pub dependencies: Vec<StrategyDependency>,
    pub stream_shards: Vec<StreamShard>,
    pub owners: Vec<OwnerAssignment>,
    pub journals: Vec<JournalAssignment>,
    pub capabilities: Vec<CapabilityEntry>,
    pub compatibility_projections: Vec<CompatibilityProjection>,
    pub required_environment: Vec<EnvironmentRequirement>,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompiledSource {
    pub snapshot_id: String,
    pub source_fingerprint_sha256: String,
    pub snapshot: DomainSnapshot,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AccountNode {
    pub id: BinanceAccountId,
    pub product: String,
    pub order_api_key_env: String,
    pub order_secret_key_env: String,
    pub treasury_api_key_env: String,
    pub treasury_secret_key_env: String,
    pub subaccount_email_env: String,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InstrumentNode {
    pub id: InstrumentId,
    pub account_id: BinanceAccountId,
    pub symbol: String,
    pub base_asset: VenueAssetId,
    pub quote_asset: VenueAssetId,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkNode {
    pub id: NetworkId,
    pub chain_id: u64,
    pub name: String,
    pub rpc_url_env: String,
    pub ws_url_env: String,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WalletNode {
    pub id: WalletId,
    pub address_env: String,
    pub private_key_env: String,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WalletLocationNode {
    pub id: WalletLocationId,
    pub network_id: NetworkId,
    pub wallet_id: WalletId,
    pub execution_lane_id: ExecutionLaneId,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VenueAssetKind {
    BinanceSpot,
    Erc20,
    Native,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VenueAssetNode {
    pub id: VenueAssetId,
    pub kind: VenueAssetKind,
    pub symbol: String,
    pub account_id: Option<BinanceAccountId>,
    pub network_id: Option<NetworkId>,
    pub contract: Option<String>,
    pub decimals: Option<u8>,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EconomicAssetNode {
    pub id: EconomicAssetId,
    pub symbol: String,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AssetMapping {
    pub venue_asset_id: VenueAssetId,
    pub economic_asset_id: EconomicAssetId,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PoolProtocol {
    UniswapV3,
    UniswapV4,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PoolNode {
    pub id: PoolId,
    pub pair_id: String,
    pub network_id: NetworkId,
    pub protocol: PoolProtocol,
    pub canonical_identity: String,
    pub fee_pips: u32,
    pub tick_spacing: Option<i32>,
    pub hooks: Option<String>,
    pub lifecycle: PoolLifecycle,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyNode {
    pub id: StrategyId,
    pub pair_id: String,
    pub source_snapshot_id: String,
    pub account_id: BinanceAccountId,
    pub instrument_id: InstrumentId,
    pub network_id: NetworkId,
    pub wallet_location_id: WalletLocationId,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyDependency {
    pub strategy_id: StrategyId,
    pub instrument_id: InstrumentId,
    pub pool_ids: Vec<PoolId>,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StreamShard {
    pub id: String,
    pub account_id: BinanceAccountId,
    pub instrument_ids: Vec<InstrumentId>,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OwnerKind {
    HotPathDecision,
    BinanceAccountState,
    BinanceOrderExecution,
    BinanceCapitalSaga,
    TradeCoordinator,
    RebalanceSaga,
    NetworkRuntime,
    Portfolio,
    EvmExecution,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerAssignment {
    pub owner_id: String,
    pub kind: OwnerKind,
    pub owned_ids: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JournalAssignment {
    pub owner_id: String,
    pub path_env: String,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityEntry {
    pub strategy_id: StrategyId,
    pub observe: bool,
    pub plan: bool,
    pub execute: bool,
    pub rebalance: bool,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompatibilityRole {
    LiveRuntime,
    PublicPriceCollector,
}

impl CompatibilityRole {
    pub const fn projection_id(self) -> &'static str {
        match self {
            Self::LiveRuntime => "compat-live-runtime",
            Self::PublicPriceCollector => "compat-public-price-collector",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompatibilityProjection {
    pub id: String,
    pub role: CompatibilityRole,
    pub source_snapshot_id: String,
    pub pair_ids: Vec<String>,
}

#[derive(Debug)]
pub struct CompiledDomainGraph {
    bundle: CompiledDomainBundle,
    fingerprint_sha256: String,
    accounts: BTreeMap<BinanceAccountId, usize>,
    instruments: BTreeMap<InstrumentId, usize>,
    networks: BTreeMap<NetworkId, usize>,
    wallets: BTreeMap<WalletId, usize>,
    venue_assets: BTreeMap<VenueAssetId, usize>,
    economic_assets: BTreeMap<EconomicAssetId, usize>,
    pools: BTreeMap<PoolId, usize>,
    strategies: BTreeMap<StrategyId, usize>,
}

#[derive(Debug)]
pub struct CompatibilitySelection {
    pub config: LoadedDomainConfig,
    pub graph_summary: Option<CompiledGraphSummary>,
    pub binance_runtime: Option<CompiledBinanceRuntimePlan>,
    pub network_runtime: Option<CompiledNetworkRuntimePlan>,
    pub hot_path_runtime: Option<CompiledHotPathRuntimePlan>,
    pub portfolio_runtime: Option<CompiledPortfolioRuntimePlan>,
}

/// Immutable account-scoped Binance topology consumed by the M2 runtime.
///
/// Compatibility projections still select which strategy may execute, while
/// this plan deliberately retains every instrument on the shared account so
/// market data and authenticated account state are not rebuilt per pair.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CompiledBinanceRuntimePlan {
    pub account_id: BinanceAccountId,
    pub stream_shards: Vec<CompiledBinanceStreamShard>,
    pub symbols: Vec<String>,
    pub asset_symbols: Vec<String>,
    pub asset_decimals: BTreeMap<String, u8>,
    pub executable_symbols: BTreeSet<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CompiledBinanceStreamShard {
    pub id: String,
    pub symbols: Vec<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CompiledNetworkRuntimePlan {
    pub networks: Vec<CompiledNetworkPlan>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CompiledCapitalAllocatorMode {
    Disabled,
    Shadow,
    LiveCanary,
    FullLive,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CompiledCapitalCanaryPolicy {
    pub full_live: bool,
    pub approval_session_id: String,
    pub network_id: NetworkId,
    pub binance_network: String,
    pub token_a_symbol: String,
    pub token_b_symbol: String,
    pub token_a_economic_asset_id: EconomicAssetId,
    pub token_b_economic_asset_id: EconomicAssetId,
    pub maximum_transfer_count: u16,
    pub maximum_concurrent_transfers: u16,
    pub maximum_failed_transfers: u16,
    pub maximum_token_a_debit: U256,
    pub maximum_token_b_debit: U256,
    pub maximum_token_a_fee: U256,
    pub maximum_token_b_fee: U256,
    pub rollout_duration_seconds: u64,
    pub maximum_unknown_reconciliation_queries: u16,
    pub direct_route_only: bool,
    pub bridge_mutations_enabled: bool,
    pub external_mutation_authorized: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CompiledInventoryLocation {
    BinanceAccount {
        account_id: BinanceAccountId,
    },
    EvmWallet {
        network_id: NetworkId,
        chain_id: u64,
        wallet_location_id: WalletLocationId,
    },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CompiledPortfolioAsset {
    pub location: CompiledInventoryLocation,
    pub venue_asset_id: VenueAssetId,
    pub economic_asset_id: EconomicAssetId,
    pub symbol: String,
    pub decimals: u8,
}

/// Process-scoped M5 account and wallet ownership plan. Every venue asset has
/// exactly one reviewed economic mapping and one exact inventory location.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CompiledPortfolioRuntimePlan {
    pub assets: Vec<CompiledPortfolioAsset>,
    pub allocator_mode: CompiledCapitalAllocatorMode,
    pub capital_canary: Option<CompiledCapitalCanaryPolicy>,
    pub live_rebalance_adapter: String,
}

/// Process-scoped M4 routing plan. It is compiled from the same authoritative
/// graph as account and network ownership, so the hot path does not reconstruct
/// symbol or pool allowlists from environment variables.
#[derive(Debug, Clone)]
pub struct CompiledHotPathRuntimePlan {
    pub strategies: Vec<CompiledHotPathStrategyPlan>,
}

#[derive(Debug, Clone)]
pub struct CompiledHotPathStrategyPlan {
    pub strategy_id: StrategyId,
    pub pair_id: String,
    pub instrument_id: InstrumentId,
    pub symbol: String,
    pub network_id: NetworkId,
    pub pool_ids: Vec<PoolId>,
    pub observe: bool,
    pub plan: bool,
    pub execute: bool,
    pub baseline_budget_us: u64,
    pub domain_config: LoadedDomainConfig,
}

const DEFAULT_BASELINE_CALCULATION_BUDGET_US: u64 = 200;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CompiledNetworkPlan {
    pub network_id: NetworkId,
    pub chain_id: u64,
    pub name: String,
    pub rpc_url_env: String,
    pub ws_url_env: String,
    pub wallet_location_id: WalletLocationId,
    pub execution_lane_id: ExecutionLaneId,
    pub pool_ids: Vec<PoolId>,
    pub assets: Vec<CompiledNetworkAsset>,
    pub multicall3_address: String,
    pub gas_policy: CompiledNetworkGasPolicy,
    pub execution_enabled: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CompiledNetworkAsset {
    pub venue_asset_id: VenueAssetId,
    pub symbol: String,
    pub contract: Option<String>,
    pub decimals: u8,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CompiledNetworkGasPolicy {
    WorldChainV12 {
        fallback_gas_price_wei: u128,
        includes_l1_fee: bool,
    },
    ArbitrumOne {
        requires_fresh_rpc_gas_price: bool,
        max_priority_fee_per_gas_wei: u128,
        max_fee_headroom_bps: u16,
        includes_l1_fee: bool,
    },
    ReadOnly,
}

#[derive(Debug, Clone)]
pub struct CompiledGraphSummary {
    pub bundle_id: String,
    pub projection_id: String,
    pub fingerprint_sha256: String,
    pub accounts: usize,
    pub instruments: usize,
    pub networks: usize,
    pub wallets: usize,
    pub venue_assets: usize,
    pub economic_assets: usize,
    pub pools: usize,
    pub strategies: usize,
    pub bundle_bytes: usize,
    pub load_validation_us: u128,
    pub rss_before_bytes: Option<u64>,
    pub rss_after_bytes: Option<u64>,
    pub rss_delta_bytes: Option<i64>,
}

impl CompiledDomainGraph {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let bytes = fs::read(path)
            .with_context(|| format!("failed to read compiled domain {}", path.display()))?;
        let bundle: CompiledDomainBundle = serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to parse compiled domain {}", path.display()))?;
        Self::from_bundle(bundle)
            .with_context(|| format!("invalid compiled domain {}", path.display()))
    }

    pub fn from_bundle(bundle: CompiledDomainBundle) -> anyhow::Result<Self> {
        ensure!(
            bundle.bundle_kind == COMPILED_DOMAIN_KIND,
            "unexpected bundle_kind {}",
            bundle.bundle_kind
        );
        ensure!(
            bundle.schema_version == COMPILED_DOMAIN_SCHEMA_VERSION,
            "unsupported compiled domain schema_version {}",
            bundle.schema_version
        );
        validate_id("bundle_id", &bundle.bundle_id)?;
        validate_id("compiler_version", &bundle.compiler_version)?;

        let canonical =
            serde_json::to_vec(&bundle).context("failed to serialize canonical compiled domain")?;
        let fingerprint_sha256 = sha256_hex(&canonical);

        let accounts = index_ids(&bundle.accounts, |item| item.id.clone(), "account")?;
        let instruments = index_ids(&bundle.instruments, |item| item.id.clone(), "instrument")?;
        let networks = index_ids(&bundle.networks, |item| item.id.clone(), "network")?;
        let wallets = index_ids(&bundle.wallets, |item| item.id.clone(), "wallet")?;
        let venue_assets = index_ids(&bundle.venue_assets, |item| item.id.clone(), "venue asset")?;
        let economic_assets = index_ids(
            &bundle.economic_assets,
            |item| item.id.clone(),
            "economic asset",
        )?;
        let pools = index_ids(&bundle.pools, |item| item.id.clone(), "pool")?;
        let strategies = index_ids(&bundle.strategies, |item| item.id.clone(), "strategy")?;

        ensure!(!accounts.is_empty(), "compiled accounts must not be empty");
        ensure!(
            !instruments.is_empty(),
            "compiled instruments must not be empty"
        );
        ensure!(!networks.is_empty(), "compiled networks must not be empty");
        ensure!(!wallets.is_empty(), "compiled wallets must not be empty");
        ensure!(
            !strategies.is_empty(),
            "compiled strategies must not be empty"
        );

        unique_by(
            bundle.sources.iter().map(|item| item.snapshot_id.clone()),
            "compiled source snapshot",
        )?;
        for source in &bundle.sources {
            ensure!(
                source.snapshot.snapshot_id == source.snapshot_id,
                "compiled source snapshot id mismatch"
            );
            source.snapshot.validate_for_compiler()?;
            validate_sha256(
                "source_fingerprint_sha256",
                &source.source_fingerprint_sha256,
            )?;
        }
        let source_ids: BTreeSet<_> = bundle
            .sources
            .iter()
            .map(|source| source.snapshot_id.as_str())
            .collect();

        for account in &bundle.accounts {
            AccountManifest {
                id: account.id.clone(),
                product: account.product.clone(),
                order_api_key_env: account.order_api_key_env.clone(),
                order_secret_key_env: account.order_secret_key_env.clone(),
                treasury_api_key_env: account.treasury_api_key_env.clone(),
                treasury_secret_key_env: account.treasury_secret_key_env.clone(),
                subaccount_email_env: account.subaccount_email_env.clone(),
            }
            .validate()?;
        }
        for network in &bundle.networks {
            ensure!(network.chain_id > 0, "network chain_id must be positive");
            ensure!(
                network.id.as_str() == format!("eip155:{}", network.chain_id),
                "network id does not encode chain_id"
            );
            validate_env_name(&network.rpc_url_env)?;
            validate_env_name(&network.ws_url_env)?;
        }
        for wallet in &bundle.wallets {
            WalletManifest {
                id: wallet.id.clone(),
                address_env: wallet.address_env.clone(),
                private_key_env: wallet.private_key_env.clone(),
            }
            .validate()?;
        }
        for location in &bundle.wallet_locations {
            ensure!(
                networks.contains_key(&location.network_id),
                "wallet location {} references unknown network",
                location.id.as_str()
            );
            ensure!(
                wallets.contains_key(&location.wallet_id),
                "wallet location {} references unknown wallet",
                location.id.as_str()
            );
            ensure!(
                location.id.as_str() == location.execution_lane_id.as_str(),
                "wallet location and execution lane compatibility ids differ"
            );
        }
        unique_by(
            bundle.wallet_locations.iter().map(|item| item.id.0.clone()),
            "wallet location",
        )?;
        unique_by(
            bundle
                .wallet_locations
                .iter()
                .map(|item| item.execution_lane_id.0.clone()),
            "execution lane",
        )?;

        for instrument in &bundle.instruments {
            ensure!(
                accounts.contains_key(&instrument.account_id),
                "instrument {} references unknown account",
                instrument.id.as_str()
            );
            ensure!(
                venue_assets.contains_key(&instrument.base_asset)
                    && venue_assets.contains_key(&instrument.quote_asset),
                "instrument {} references unknown asset",
                instrument.id.as_str()
            );
        }
        for asset in &bundle.venue_assets {
            match asset.kind {
                VenueAssetKind::BinanceSpot => {
                    ensure!(asset.account_id.is_some(), "Binance asset lacks account");
                    ensure!(asset.network_id.is_none(), "Binance asset has network");
                }
                VenueAssetKind::Erc20 => {
                    ensure!(asset.network_id.is_some(), "ERC20 asset lacks network");
                    ensure!(asset.contract.is_some(), "ERC20 asset lacks contract");
                    ensure!(asset.decimals.is_some(), "ERC20 asset lacks decimals");
                }
                VenueAssetKind::Native => {
                    ensure!(asset.network_id.is_some(), "native asset lacks network");
                    ensure!(asset.contract.is_none(), "native asset has contract");
                    ensure!(asset.decimals.is_some(), "native asset lacks decimals");
                }
            }
        }
        unique_by(
            bundle
                .asset_mappings
                .iter()
                .map(|item| item.venue_asset_id.0.clone()),
            "asset mapping",
        )?;
        for mapping in &bundle.asset_mappings {
            ensure!(
                venue_assets.contains_key(&mapping.venue_asset_id),
                "asset mapping references unknown venue asset"
            );
            ensure!(
                economic_assets.contains_key(&mapping.economic_asset_id),
                "asset mapping references unknown economic asset"
            );
        }
        ensure!(
            bundle.asset_mappings.len() == bundle.venue_assets.len(),
            "every venue asset must have exactly one economic mapping"
        );

        for pool in &bundle.pools {
            ensure!(
                networks.contains_key(&pool.network_id),
                "pool {} references unknown network",
                pool.id.as_str()
            );
            ensure!(
                source_pair(&bundle.sources, &pool.pair_id).is_some(),
                "pool {} references unknown pair",
                pool.id.as_str()
            );
        }
        unique_by(
            bundle
                .capabilities
                .iter()
                .map(|item| item.strategy_id.0.clone()),
            "capability strategy",
        )?;
        let capabilities: BTreeMap<_, _> = bundle
            .capabilities
            .iter()
            .map(|item| (item.strategy_id.clone(), item))
            .collect();
        for strategy in &bundle.strategies {
            ensure!(
                source_ids.contains(strategy.source_snapshot_id.as_str()),
                "strategy {} references unknown source",
                strategy.id.as_str()
            );
            ensure!(
                accounts.contains_key(&strategy.account_id)
                    && instruments.contains_key(&strategy.instrument_id)
                    && networks.contains_key(&strategy.network_id),
                "strategy {} has an unresolved registry reference",
                strategy.id.as_str()
            );
            ensure!(
                bundle
                    .wallet_locations
                    .iter()
                    .any(|item| item.id == strategy.wallet_location_id),
                "strategy {} references unknown wallet location",
                strategy.id.as_str()
            );
            let pair = source_pair(&bundle.sources, &strategy.pair_id)
                .context("strategy references unknown source pair")?;
            let capability = capabilities
                .get(&strategy.id)
                .context("strategy lacks capability matrix entry")?;
            ensure!(
                capability.observe && capability.plan,
                "strategy must observe and plan"
            );
            ensure!(
                capability.execute == pair.execution_enabled,
                "strategy execution capability differs from source artifact"
            );
            ensure!(
                capability.rebalance == pair.rebalance.enabled,
                "strategy rebalance capability differs from source artifact"
            );
        }
        ensure!(
            bundle.capabilities.len() == bundle.strategies.len(),
            "capability matrix must cover every strategy exactly once"
        );

        unique_by(
            bundle
                .dependencies
                .iter()
                .map(|item| item.strategy_id.0.clone()),
            "dependency strategy",
        )?;
        for dependency in &bundle.dependencies {
            ensure!(
                strategies.contains_key(&dependency.strategy_id),
                "dependency references unknown strategy"
            );
            ensure!(
                instruments.contains_key(&dependency.instrument_id),
                "dependency references unknown instrument"
            );
            ensure!(!dependency.pool_ids.is_empty(), "dependency has no pools");
            for pool_id in &dependency.pool_ids {
                ensure!(
                    pools.contains_key(pool_id),
                    "dependency references unknown pool"
                );
            }
        }
        ensure!(
            bundle.dependencies.len() == bundle.strategies.len(),
            "dependency index must cover every strategy"
        );

        let mut sharded_instruments = BTreeSet::new();
        for shard in &bundle.stream_shards {
            validate_id("stream shard id", &shard.id)?;
            ensure!(
                accounts.contains_key(&shard.account_id),
                "stream shard references unknown account"
            );
            ensure!(!shard.instrument_ids.is_empty(), "empty stream shard");
            for instrument_id in &shard.instrument_ids {
                ensure!(
                    instruments.contains_key(instrument_id),
                    "stream shard references unknown instrument"
                );
                ensure!(
                    sharded_instruments.insert(instrument_id.clone()),
                    "instrument assigned to multiple stream shards"
                );
            }
        }
        ensure!(
            sharded_instruments.len() == instruments.len(),
            "stream shards must cover every instrument"
        );

        unique_by(
            bundle.owners.iter().map(|item| item.owner_id.clone()),
            "owner assignment",
        )?;
        let owner_ids: BTreeSet<_> = bundle
            .owners
            .iter()
            .map(|item| item.owner_id.as_str())
            .collect();
        unique_by(
            bundle
                .journals
                .iter()
                .map(|item| format!("{}:{}", item.owner_id, item.path_env)),
            "journal assignment",
        )?;
        unique_by(
            bundle.journals.iter().map(|item| item.path_env.clone()),
            "journal environment",
        )?;
        for journal in &bundle.journals {
            ensure!(
                owner_ids.contains(journal.owner_id.as_str()),
                "journal references unknown owner {}",
                journal.owner_id
            );
            validate_env_name(&journal.path_env)?;
        }
        unique_by(
            bundle
                .compatibility_projections
                .iter()
                .map(|item| item.id.clone()),
            "compatibility projection",
        )?;
        unique_by(
            bundle
                .compatibility_projections
                .iter()
                .map(|item| format!("{:?}", item.role)),
            "compatibility role",
        )?;
        for projection in &bundle.compatibility_projections {
            ensure!(
                source_ids.contains(projection.source_snapshot_id.as_str()),
                "projection references unknown source"
            );
            let source = bundle
                .sources
                .iter()
                .find(|item| item.snapshot_id == projection.source_snapshot_id)
                .expect("source id checked above");
            let projected: BTreeSet<_> = projection.pair_ids.iter().collect();
            ensure!(
                !projected.is_empty()
                    && projected.iter().all(|pair_id| source
                        .snapshot
                        .pairs
                        .iter()
                        .any(|pair| &pair.id == *pair_id)),
                "projection contains a pair outside its source snapshot"
            );
            match projection.role {
                CompatibilityRole::LiveRuntime => ensure!(
                    source.snapshot.live_trading_enabled,
                    "live projection must preserve a live source artifact"
                ),
                // Public projection construction below always scrubs trading,
                // rebalance, approval and credential-bearing capabilities.
                // The reviewed source may itself become live after M10
                // approval; source liveness must not make that projection
                // impossible to compile.
                CompatibilityRole::PublicPriceCollector => {}
            }
        }
        unique_by(
            bundle
                .required_environment
                .iter()
                .map(|item| item.projection_id.clone()),
            "environment projection",
        )?;
        for requirement in &bundle.required_environment {
            requirement.validate()?;
            ensure!(
                bundle
                    .compatibility_projections
                    .iter()
                    .any(|item| item.id == requirement.projection_id),
                "environment requirements reference unknown projection"
            );
        }

        Ok(Self {
            bundle,
            fingerprint_sha256,
            accounts,
            instruments,
            networks,
            wallets,
            venue_assets,
            economic_assets,
            pools,
            strategies,
        })
    }

    pub fn bundle(&self) -> &CompiledDomainBundle {
        &self.bundle
    }

    pub fn fingerprint_sha256(&self) -> &str {
        &self.fingerprint_sha256
    }

    pub fn project(
        &self,
        path: impl AsRef<Path>,
        role: CompatibilityRole,
        validate_environment: bool,
    ) -> anyhow::Result<CompatibilitySelection> {
        let projection = self
            .bundle
            .compatibility_projections
            .iter()
            .find(|item| item.role == role)
            .with_context(|| format!("compiled domain has no {role:?} projection"))?;
        if validate_environment {
            let requirement = self
                .bundle
                .required_environment
                .iter()
                .find(|item| item.projection_id == projection.id)
                .context("projection has no environment requirements")?;
            let missing: Vec<_> = requirement
                .names
                .iter()
                .filter(|name| std::env::var_os(name).is_none())
                .cloned()
                .collect();
            ensure!(
                missing.is_empty(),
                "compiled domain projection {} is missing required environment variables: {}",
                projection.id,
                missing.join(", ")
            );
        }
        let source = self
            .bundle
            .sources
            .iter()
            .find(|item| item.snapshot_id == projection.source_snapshot_id)
            .expect("compiled graph validation checked projection source");
        let selected: BTreeSet<_> = projection.pair_ids.iter().map(String::as_str).collect();
        let mut snapshot = source.snapshot.clone();
        snapshot
            .pairs
            .retain(|pair| selected.contains(pair.id.as_str()));
        if role == CompatibilityRole::PublicPriceCollector {
            for pair in &mut snapshot.pairs {
                pair.execution_enabled = false;
                pair.full_live = false;
                pair.full_live_policy = None;
                if let Some(canary) = &mut pair.live_canary {
                    canary.approval_gate =
                        crate::domain::config::LiveCanaryApprovalGate::ExplicitProductionApprovalRequired;
                    canary.production_approval_actor = None;
                    canary.production_approval_recorded_at_utc = None;
                    canary.prefunding_rebalance = None;
                    canary.rebalance_mutations_enabled = false;
                    if let Some(rebalance) = &mut canary.rebalance_live_canary {
                        rebalance.approval_gate =
                            crate::domain::config::LiveCanaryApprovalGate::ExplicitProductionApprovalRequired;
                        rebalance.production_approval_actor = None;
                        rebalance.production_approval_recorded_at_utc = None;
                    }
                }
                pair.rebalance.enabled = false;
            }
        }
        snapshot.live_trading_enabled = snapshot.pairs.iter().any(|pair| pair.execution_enabled);
        let config = LoadedDomainConfig::from_projected_snapshot(
            path.as_ref(),
            self.fingerprint_sha256.clone(),
            snapshot,
        )?;
        Ok(CompatibilitySelection {
            config,
            binance_runtime: Some(self.binance_runtime_plan()?),
            network_runtime: Some(self.network_runtime_plan(role)?),
            hot_path_runtime: Some(self.hot_path_runtime_plan(path)?),
            portfolio_runtime: Some(self.portfolio_runtime_plan(role)?),
            graph_summary: Some(CompiledGraphSummary {
                bundle_id: self.bundle.bundle_id.clone(),
                projection_id: projection.id.clone(),
                fingerprint_sha256: self.fingerprint_sha256.clone(),
                accounts: self.accounts.len(),
                instruments: self.instruments.len(),
                networks: self.networks.len(),
                wallets: self.wallets.len(),
                venue_assets: self.venue_assets.len(),
                economic_assets: self.economic_assets.len(),
                pools: self.pools.len(),
                strategies: self.strategies.len(),
                bundle_bytes: 0,
                load_validation_us: 0,
                rss_before_bytes: None,
                rss_after_bytes: None,
                rss_delta_bytes: None,
            }),
        })
    }

    fn binance_runtime_plan(&self) -> anyhow::Result<CompiledBinanceRuntimePlan> {
        ensure!(
            self.bundle.accounts.len() == 1,
            "M2 Binance runtime currently requires exactly one compiled account"
        );
        let account_id = self.bundle.accounts[0].id.clone();
        let instruments_by_id: BTreeMap<_, _> = self
            .bundle
            .instruments
            .iter()
            .map(|instrument| (instrument.id.clone(), instrument))
            .collect();
        let strategies_by_id: BTreeMap<_, _> = self
            .bundle
            .strategies
            .iter()
            .map(|strategy| (strategy.id.clone(), strategy))
            .collect();
        let stream_shards = self
            .bundle
            .stream_shards
            .iter()
            .filter(|shard| shard.account_id == account_id)
            .map(|shard| {
                let symbols = shard
                    .instrument_ids
                    .iter()
                    .map(|instrument_id| {
                        instruments_by_id
                            .get(instrument_id)
                            .map(|instrument| instrument.symbol.clone())
                            .with_context(|| {
                                format!(
                                    "stream shard {} references missing instrument {}",
                                    shard.id,
                                    instrument_id.as_str()
                                )
                            })
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                Ok(CompiledBinanceStreamShard {
                    id: shard.id.clone(),
                    symbols,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let symbols = self
            .bundle
            .instruments
            .iter()
            .filter(|instrument| instrument.account_id == account_id)
            .map(|instrument| instrument.symbol.clone())
            .collect();
        let asset_symbols = self
            .bundle
            .venue_assets
            .iter()
            .filter(|asset| {
                asset.kind == VenueAssetKind::BinanceSpot
                    && asset.account_id.as_ref() == Some(&account_id)
            })
            .map(|asset| asset.symbol.clone())
            .collect();
        let mut asset_decimals = BTreeMap::new();
        for pair in self
            .bundle
            .sources
            .iter()
            .flat_map(|source| source.snapshot.pairs.iter())
        {
            for token in [&pair.token_a, &pair.token_b] {
                insert_same(
                    &mut asset_decimals,
                    token.symbol.clone(),
                    token.decimals,
                    "Binance asset decimals",
                )?;
            }
            if let (Some(asset), Some(decimals)) = (
                pair.binance.commission_asset.as_ref(),
                pair.binance.commission_asset_decimals,
            ) {
                insert_same(
                    &mut asset_decimals,
                    asset.clone(),
                    decimals,
                    "Binance commission asset decimals",
                )?;
            }
        }
        for symbol in &asset_symbols {
            ensure!(
                asset_decimals.contains_key(symbol),
                "Binance asset {symbol} has no compiled decimals"
            );
        }
        let executable_symbols = self
            .bundle
            .capabilities
            .iter()
            .filter(|capability| capability.execute)
            .map(|capability| {
                let strategy = strategies_by_id
                    .get(&capability.strategy_id)
                    .expect("compiled validation checked capability strategy");
                instruments_by_id
                    .get(&strategy.instrument_id)
                    .expect("compiled validation checked strategy instrument")
                    .symbol
                    .clone()
            })
            .collect();
        Ok(CompiledBinanceRuntimePlan {
            account_id,
            stream_shards,
            symbols,
            asset_symbols,
            asset_decimals,
            executable_symbols,
        })
    }

    fn network_runtime_plan(
        &self,
        role: CompatibilityRole,
    ) -> anyhow::Result<CompiledNetworkRuntimePlan> {
        let projection = self
            .bundle
            .compatibility_projections
            .iter()
            .find(|projection| projection.role == role)
            .context("compiled network runtime projection is missing")?;
        let selected_networks: BTreeSet<_> = match role {
            CompatibilityRole::LiveRuntime => self
                .bundle
                .networks
                .iter()
                .map(|item| item.id.clone())
                .collect(),
            CompatibilityRole::PublicPriceCollector => self
                .bundle
                .strategies
                .iter()
                .filter(|strategy| projection.pair_ids.contains(&strategy.pair_id))
                .map(|strategy| strategy.network_id.clone())
                .collect(),
        };
        let strategies_by_id: BTreeMap<_, _> = self
            .bundle
            .strategies
            .iter()
            .map(|strategy| (strategy.id.clone(), strategy))
            .collect();
        let executable_networks: BTreeSet<_> = self
            .bundle
            .capabilities
            .iter()
            .filter(|capability| capability.execute)
            .map(|capability| {
                strategies_by_id
                    .get(&capability.strategy_id)
                    .expect("compiled validation checked strategy capability")
                    .network_id
                    .clone()
            })
            .collect();
        let mut networks = Vec::new();
        for network in self
            .bundle
            .networks
            .iter()
            .filter(|network| selected_networks.contains(&network.id))
        {
            let location = self
                .bundle
                .wallet_locations
                .iter()
                .find(|location| location.network_id == network.id)
                .with_context(|| {
                    format!(
                        "network {} has no compiled wallet location",
                        network.id.as_str()
                    )
                })?;
            let mut pool_ids = self
                .bundle
                .pools
                .iter()
                .filter(|pool| pool.network_id == network.id)
                .map(|pool| pool.id.clone())
                .collect::<Vec<_>>();
            pool_ids.sort();
            pool_ids.dedup();
            let mut assets = self
                .bundle
                .venue_assets
                .iter()
                .filter(|asset| asset.network_id.as_ref() == Some(&network.id))
                .map(|asset| {
                    Ok(CompiledNetworkAsset {
                        venue_asset_id: asset.id.clone(),
                        symbol: asset.symbol.clone(),
                        contract: asset.contract.clone(),
                        decimals: asset.decimals.context("network asset lacks decimals")?,
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            assets.sort_by(|left, right| left.venue_asset_id.cmp(&right.venue_asset_id));

            let pairs = self
                .bundle
                .sources
                .iter()
                .flat_map(|source| source.snapshot.pairs.iter())
                .filter(|pair| pair.chain.chain_id == network.chain_id)
                .collect::<Vec<_>>();
            ensure!(
                !pairs.is_empty(),
                "compiled network {} has no source pairs",
                network.id.as_str()
            );
            let multicall3_address = pairs[0].chain.multicall3_address.clone();
            ensure!(
                pairs.iter().all(|pair| {
                    pair.chain
                        .multicall3_address
                        .eq_ignore_ascii_case(&multicall3_address)
                }),
                "network {} has inconsistent Multicall3 addresses",
                network.id.as_str()
            );
            ensure!(
                multicall3_address
                    .eq_ignore_ascii_case("0xcA11bde05977b3631167028862bE2a173976CA11"),
                "network {} does not use the reviewed canonical Multicall3 deployment",
                network.id.as_str()
            );

            let execution_enabled = executable_networks.contains(&network.id);
            let canary_readiness = pairs.iter().any(|pair| pair.live_canary.is_some());
            let gas_policy = match (network.chain_id, execution_enabled, canary_readiness) {
                (480, true, _) => CompiledNetworkGasPolicy::WorldChainV12 {
                    fallback_gas_price_wei: 100_000,
                    includes_l1_fee: true,
                },
                (42_161, true, _) => {
                    let max_fee_headroom_bps = pairs
                        .iter()
                        .filter_map(|pair| {
                            pair.full_live_policy
                                .as_ref()
                                .map(|policy| policy.arbitrum_max_fee_headroom_bps)
                                .or_else(|| {
                                    pair.live_canary
                                        .as_ref()
                                        .map(|canary| canary.arbitrum_max_fee_headroom_bps)
                                })
                        })
                        .next()
                        .context("Arbitrum live gas headroom is missing")?;
                    ensure!(
                        pairs
                            .iter()
                            .filter_map(|pair| {
                                pair.full_live_policy
                                    .as_ref()
                                    .map(|policy| policy.arbitrum_max_fee_headroom_bps)
                                    .or_else(|| {
                                        pair.live_canary
                                            .as_ref()
                                            .map(|canary| canary.arbitrum_max_fee_headroom_bps)
                                    })
                            })
                            .all(|headroom| headroom == max_fee_headroom_bps),
                        "network {} has inconsistent Arbitrum max-fee headroom",
                        network.id.as_str()
                    );
                    CompiledNetworkGasPolicy::ArbitrumOne {
                        requires_fresh_rpc_gas_price: true,
                        max_priority_fee_per_gas_wei: 0,
                        max_fee_headroom_bps,
                        includes_l1_fee: false,
                    }
                }
                (_, false, false) => CompiledNetworkGasPolicy::ReadOnly,
                _ => anyhow::bail!(
                    "network {} has no reviewed live gas policy",
                    network.id.as_str()
                ),
            };
            networks.push(CompiledNetworkPlan {
                network_id: network.id.clone(),
                chain_id: network.chain_id,
                name: network.name.clone(),
                rpc_url_env: network.rpc_url_env.clone(),
                ws_url_env: network.ws_url_env.clone(),
                wallet_location_id: location.id.clone(),
                execution_lane_id: location.execution_lane_id.clone(),
                pool_ids,
                assets,
                multicall3_address: multicall3_address.to_ascii_lowercase(),
                gas_policy,
                execution_enabled,
            });
        }
        ensure!(
            !networks.is_empty(),
            "compiled network runtime plan is empty"
        );
        Ok(CompiledNetworkRuntimePlan { networks })
    }

    fn hot_path_runtime_plan(
        &self,
        path: impl AsRef<Path>,
    ) -> anyhow::Result<CompiledHotPathRuntimePlan> {
        let instruments: BTreeMap<_, _> = self
            .bundle
            .instruments
            .iter()
            .map(|instrument| (instrument.id.clone(), instrument))
            .collect();
        let dependencies: BTreeMap<_, _> = self
            .bundle
            .dependencies
            .iter()
            .map(|dependency| (dependency.strategy_id.clone(), dependency))
            .collect();
        let capabilities: BTreeMap<_, _> = self
            .bundle
            .capabilities
            .iter()
            .map(|capability| (capability.strategy_id.clone(), capability))
            .collect();
        let mut strategies = Vec::with_capacity(self.bundle.strategies.len());
        for strategy in &self.bundle.strategies {
            let instrument = instruments.get(&strategy.instrument_id).with_context(|| {
                format!(
                    "hot-path strategy {} has no compiled instrument",
                    strategy.id.as_str()
                )
            })?;
            let dependency = dependencies.get(&strategy.id).with_context(|| {
                format!(
                    "hot-path strategy {} has no compiled dependency",
                    strategy.id.as_str()
                )
            })?;
            let capability = capabilities.get(&strategy.id).with_context(|| {
                format!(
                    "hot-path strategy {} has no compiled capability",
                    strategy.id.as_str()
                )
            })?;
            let source = self
                .bundle
                .sources
                .iter()
                .find(|source| source.snapshot_id == strategy.source_snapshot_id)
                .expect("compiled graph validation checked strategy source");
            let mut snapshot = source.snapshot.clone();
            snapshot.pairs.retain(|pair| pair.id == strategy.pair_id);
            ensure!(
                snapshot.pairs.len() == 1,
                "hot-path strategy {} did not select exactly one source pair",
                strategy.id.as_str()
            );
            snapshot.live_trading_enabled = capability.execute;
            let domain_config = LoadedDomainConfig::from_projected_snapshot(
                path.as_ref(),
                self.fingerprint_sha256.clone(),
                snapshot,
            )?;
            ensure!(
                !dependency.pool_ids.is_empty(),
                "hot-path strategy {} has no pools",
                strategy.id.as_str()
            );
            let mut pool_ids = dependency.pool_ids.clone();
            pool_ids.sort();
            pool_ids.dedup();
            ensure!(
                pool_ids.len() == dependency.pool_ids.len(),
                "hot-path strategy {} repeats a pool dependency",
                strategy.id.as_str()
            );
            strategies.push(CompiledHotPathStrategyPlan {
                strategy_id: strategy.id.clone(),
                pair_id: strategy.pair_id.clone(),
                instrument_id: strategy.instrument_id.clone(),
                symbol: instrument.symbol.clone(),
                network_id: strategy.network_id.clone(),
                pool_ids,
                observe: capability.observe,
                plan: capability.plan,
                execute: capability.execute,
                baseline_budget_us: DEFAULT_BASELINE_CALCULATION_BUDGET_US,
                domain_config,
            });
        }
        strategies.sort_by(|left, right| left.strategy_id.cmp(&right.strategy_id));
        ensure!(
            !strategies.is_empty(),
            "compiled hot-path runtime plan is empty"
        );
        Ok(CompiledHotPathRuntimePlan { strategies })
    }

    fn portfolio_runtime_plan(
        &self,
        role: CompatibilityRole,
    ) -> anyhow::Result<CompiledPortfolioRuntimePlan> {
        let mappings = self
            .bundle
            .asset_mappings
            .iter()
            .map(|mapping| {
                (
                    mapping.venue_asset_id.clone(),
                    mapping.economic_asset_id.clone(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let wallet_locations = self
            .bundle
            .wallet_locations
            .iter()
            .map(|location| (location.network_id.clone(), location))
            .collect::<BTreeMap<_, _>>();
        let networks = self
            .bundle
            .networks
            .iter()
            .map(|network| (network.id.clone(), network))
            .collect::<BTreeMap<_, _>>();
        let binance_decimals = self.binance_runtime_plan()?.asset_decimals;
        let mut assets = Vec::with_capacity(self.bundle.venue_assets.len());
        for venue_asset in &self.bundle.venue_assets {
            let economic_asset_id = mappings
                .get(&venue_asset.id)
                .with_context(|| {
                    format!(
                        "portfolio venue asset {} has no reviewed economic mapping",
                        venue_asset.id.as_str()
                    )
                })?
                .clone();
            let location = match venue_asset.kind {
                VenueAssetKind::BinanceSpot => CompiledInventoryLocation::BinanceAccount {
                    account_id: venue_asset
                        .account_id
                        .clone()
                        .context("Binance venue asset has no account")?,
                },
                VenueAssetKind::Erc20 | VenueAssetKind::Native => {
                    let network_id = venue_asset
                        .network_id
                        .clone()
                        .context("wallet venue asset has no network")?;
                    let network = networks
                        .get(&network_id)
                        .context("wallet venue asset references unknown network")?;
                    let wallet_location = wallet_locations
                        .get(&network_id)
                        .context("wallet venue asset has no wallet location")?;
                    CompiledInventoryLocation::EvmWallet {
                        network_id,
                        chain_id: network.chain_id,
                        wallet_location_id: wallet_location.id.clone(),
                    }
                }
            };
            let decimals = venue_asset
                .decimals
                .or_else(|| binance_decimals.get(&venue_asset.symbol).copied())
                .with_context(|| {
                    format!(
                        "portfolio venue asset {} has no exact decimals",
                        venue_asset.id.as_str()
                    )
                })?;
            assets.push(CompiledPortfolioAsset {
                location,
                venue_asset_id: venue_asset.id.clone(),
                economic_asset_id,
                symbol: venue_asset.symbol.clone(),
                decimals,
            });
        }
        assets.sort_by(|left, right| left.venue_asset_id.cmp(&right.venue_asset_id));
        ensure!(
            assets.len() == self.bundle.asset_mappings.len(),
            "portfolio plan does not cover every reviewed asset mapping"
        );
        let full_live_policies = self
            .bundle
            .sources
            .iter()
            .flat_map(|source| source.snapshot.pairs.iter())
            .filter_map(|pair| pair.full_live_policy.as_ref().map(|policy| (pair, policy)))
            .collect::<Vec<_>>();
        ensure!(
            full_live_policies.len() <= 1,
            "compiled portfolio has multiple full-live capital policies"
        );
        let capital_canaries = self
            .bundle
            .sources
            .iter()
            .flat_map(|source| source.snapshot.pairs.iter())
            .filter_map(|pair| {
                pair.live_canary
                    .as_ref()?
                    .rebalance_live_canary
                    .as_ref()
                    .map(|policy| (pair, policy))
            })
            .collect::<Vec<_>>();
        ensure!(
            capital_canaries.len() <= 1,
            "compiled portfolio has multiple M10 capital canaries"
        );
        let economic_asset_id = |symbol: &str| {
            self.bundle
                .economic_assets
                .iter()
                .find(|asset| asset.symbol == symbol)
                .map(|asset| asset.id.clone())
                .with_context(|| format!("compiled capital token {symbol} has no economic asset"))
        };
        let capital_canary = if let Some((pair, policy)) = full_live_policies.first() {
            Some(CompiledCapitalCanaryPolicy {
                full_live: true,
                approval_session_id: "esp-usdc-arbitrum-full-live".to_owned(),
                network_id: NetworkId::new(format!("eip155:{}", pair.chain.chain_id))?,
                binance_network: policy.rebalance_binance_network.clone(),
                token_a_symbol: pair.token_a.symbol.clone(),
                token_b_symbol: pair.token_b.symbol.clone(),
                token_a_economic_asset_id: economic_asset_id(&pair.token_a.symbol)?,
                token_b_economic_asset_id: economic_asset_id(&pair.token_b.symbol)?,
                maximum_transfer_count: 1,
                maximum_concurrent_transfers: 1,
                maximum_failed_transfers: 1,
                maximum_token_a_debit: U256::from_str_radix(
                    &policy.maximum_rebalance_token_a_debit_base_units,
                    10,
                )
                .context("compiled full-live token_a debit cap is invalid")?,
                maximum_token_b_debit: U256::from_str_radix(
                    &policy.maximum_rebalance_token_b_debit_base_units,
                    10,
                )
                .context("compiled full-live token_b debit cap is invalid")?,
                maximum_token_a_fee: U256::from_str_radix(
                    &policy.maximum_rebalance_token_a_fee_base_units,
                    10,
                )
                .context("compiled full-live token_a fee cap is invalid")?,
                maximum_token_b_fee: U256::from_str_radix(
                    &policy.maximum_rebalance_token_b_fee_base_units,
                    10,
                )
                .context("compiled full-live token_b fee cap is invalid")?,
                rollout_duration_seconds: 0,
                maximum_unknown_reconciliation_queries: policy
                    .maximum_unknown_reconciliation_queries,
                direct_route_only: policy.direct_route_only,
                bridge_mutations_enabled: policy.bridge_mutations_enabled,
                external_mutation_authorized: role == CompatibilityRole::LiveRuntime,
            })
        } else {
            capital_canaries.first().map(|(pair, policy)| {
                Ok::<_, anyhow::Error>(CompiledCapitalCanaryPolicy {
                    full_live: pair.full_live,
                    approval_session_id: policy.approval_session_id.clone(),
                    network_id: NetworkId::new(format!("eip155:{}", pair.chain.chain_id))?,
                    binance_network: policy.binance_network.clone(),
                    token_a_symbol: pair.token_a.symbol.clone(),
                    token_b_symbol: pair.token_b.symbol.clone(),
                    token_a_economic_asset_id: economic_asset_id(&pair.token_a.symbol)?,
                    token_b_economic_asset_id: economic_asset_id(&pair.token_b.symbol)?,
                    maximum_transfer_count: policy.maximum_transfer_count,
                    maximum_concurrent_transfers: policy.maximum_concurrent_transfers,
                    maximum_failed_transfers: policy.maximum_failed_transfers,
                    maximum_token_a_debit: U256::from_str_radix(
                        &policy.maximum_token_a_debit_base_units,
                        10,
                    )
                    .context("compiled M10 token_a debit cap is invalid")?,
                    maximum_token_b_debit: U256::from_str_radix(
                        &policy.maximum_token_b_debit_base_units,
                        10,
                    )
                    .context("compiled M10 token_b debit cap is invalid")?,
                    maximum_token_a_fee: U256::from_str_radix(
                        &policy.maximum_token_a_fee_base_units,
                        10,
                    )
                    .context("compiled M10 token_a fee cap is invalid")?,
                    maximum_token_b_fee: U256::from_str_radix(
                        &policy.maximum_token_b_fee_base_units,
                        10,
                    )
                    .context("compiled M10 token_b fee cap is invalid")?,
                    rollout_duration_seconds: policy.rollout_duration_seconds,
                    maximum_unknown_reconciliation_queries: policy
                        .maximum_unknown_reconciliation_queries,
                    direct_route_only: policy.direct_route_only,
                    bridge_mutations_enabled: policy.bridge_mutations_enabled,
                    external_mutation_authorized: role == CompatibilityRole::LiveRuntime
                        && policy.approval_gate
                            == crate::domain::config::LiveCanaryApprovalGate::ExplicitProductionApproved,
                })
            }).transpose()?
        };
        let allocator_mode = match role {
            CompatibilityRole::PublicPriceCollector => CompiledCapitalAllocatorMode::Disabled,
            CompatibilityRole::LiveRuntime
                if capital_canary.as_ref().is_some_and(|policy| {
                    policy.full_live && policy.external_mutation_authorized
                }) =>
            {
                CompiledCapitalAllocatorMode::FullLive
            }
            CompatibilityRole::LiveRuntime
                if capital_canary
                    .as_ref()
                    .is_some_and(|policy| policy.external_mutation_authorized) =>
            {
                CompiledCapitalAllocatorMode::LiveCanary
            }
            CompatibilityRole::LiveRuntime => CompiledCapitalAllocatorMode::Shadow,
        };
        Ok(CompiledPortfolioRuntimePlan {
            assets,
            allocator_mode,
            capital_canary,
            live_rebalance_adapter: "world_chain_v12_parity".to_owned(),
        })
    }
}

pub fn load_compatibility_domain(
    path: impl AsRef<Path>,
    role: CompatibilityRole,
    validate_environment: bool,
) -> anyhow::Result<CompatibilitySelection> {
    let path = path.as_ref();
    let started = Instant::now();
    let rss_before_bytes = linux_rss_bytes();
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read domain config {}", path.display()))?;
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse domain config {}", path.display()))?;
    if value.get("bundle_kind").and_then(serde_json::Value::as_str) == Some(COMPILED_DOMAIN_KIND) {
        let bundle: CompiledDomainBundle = serde_json::from_value(value)
            .with_context(|| format!("failed to parse compiled domain {}", path.display()))?;
        let graph = CompiledDomainGraph::from_bundle(bundle)
            .with_context(|| format!("invalid compiled domain {}", path.display()))?;
        let mut selection = graph.project(path, role, validate_environment)?;
        let rss_after_bytes = linux_rss_bytes();
        let summary = selection
            .graph_summary
            .as_mut()
            .expect("compiled selection has summary");
        summary.bundle_bytes = bytes.len();
        summary.load_validation_us = started.elapsed().as_micros();
        summary.rss_before_bytes = rss_before_bytes;
        summary.rss_after_bytes = rss_after_bytes;
        summary.rss_delta_bytes = rss_before_bytes
            .zip(rss_after_bytes)
            .map(|(before, after)| after as i64 - before as i64);
        Ok(selection)
    } else {
        let config = LoadedDomainConfig::from_bytes(path, &bytes)?;
        Ok(CompatibilitySelection {
            config,
            graph_summary: None,
            binance_runtime: None,
            network_runtime: None,
            hot_path_runtime: None,
            portfolio_runtime: None,
        })
    }
}

pub fn load_source_domain_for_pair(
    path: impl AsRef<Path>,
    pair_id: &str,
) -> anyhow::Result<LoadedDomainConfig> {
    let path = path.as_ref();
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read domain config {}", path.display()))?;
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse domain config {}", path.display()))?;
    if value.get("bundle_kind").and_then(serde_json::Value::as_str) == Some(COMPILED_DOMAIN_KIND) {
        let bundle: CompiledDomainBundle = serde_json::from_value(value)
            .with_context(|| format!("failed to parse compiled domain {}", path.display()))?;
        let graph = CompiledDomainGraph::from_bundle(bundle)
            .with_context(|| format!("invalid compiled domain {}", path.display()))?;
        let mut matching = graph
            .bundle
            .sources
            .iter()
            .filter(|source| source.snapshot.pairs.iter().any(|pair| pair.id == pair_id));
        let source = matching
            .next()
            .with_context(|| format!("compiled domain has no source for pair {pair_id}"))?;
        ensure!(
            matching.next().is_none(),
            "compiled domain has multiple sources for pair {pair_id}"
        );
        LoadedDomainConfig::from_projected_snapshot(
            path,
            graph.fingerprint_sha256.clone(),
            source.snapshot.clone(),
        )
    } else {
        let config = LoadedDomainConfig::from_bytes(path, &bytes)?;
        ensure!(
            config
                .snapshot()
                .pairs
                .iter()
                .any(|pair| pair.id == pair_id),
            "domain config has no pair {pair_id}"
        );
        Ok(config)
    }
}

fn linux_rss_bytes() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let kib = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))?
        .split_ascii_whitespace()
        .next()?
        .parse::<u64>()
        .ok()?;
    kib.checked_mul(1024)
}

pub fn compile_domain(
    manifest: &DomainCompilerManifest,
    sources: &[LoadedDomainConfig],
) -> anyhow::Result<CompiledDomainBundle> {
    manifest.validate()?;
    ensure!(
        sources.len() == manifest.source_paths.len(),
        "compiler source count differs from manifest"
    );
    ensure!(
        manifest.accounts.len() == 1,
        "M1 requires exactly one shared Binance account"
    );
    ensure!(
        manifest.wallets.len() == 1,
        "M1 requires exactly one configured signer identity"
    );
    let account = &manifest.accounts[0];
    let wallet = &manifest.wallets[0];

    let mut compiled_sources: Vec<_> = sources
        .iter()
        .map(|source| CompiledSource {
            snapshot_id: source.snapshot().snapshot_id.clone(),
            source_fingerprint_sha256: source.fingerprint_sha256().to_owned(),
            snapshot: source.snapshot().clone(),
        })
        .collect();
    compiled_sources.sort_by(|left, right| left.snapshot_id.cmp(&right.snapshot_id));
    unique_by(
        compiled_sources
            .iter()
            .map(|source| source.snapshot_id.clone()),
        "source snapshot id",
    )?;

    let mut pairs: Vec<(&str, &PairConfig)> = compiled_sources
        .iter()
        .flat_map(|source| {
            source
                .snapshot
                .pairs
                .iter()
                .map(move |pair| (source.snapshot_id.as_str(), pair))
        })
        .collect();
    pairs.sort_by(|left, right| left.1.id.cmp(&right.1.id));
    unique_by(
        pairs.iter().map(|(_, pair)| pair.id.clone()),
        "compiled pair id",
    )?;

    let reviewed: BTreeSet<_> = manifest.reviewed_live_strategies.iter().cloned().collect();
    let actual_live: BTreeSet<_> = pairs
        .iter()
        .filter(|(_, pair)| pair.execution_enabled)
        .map(|(_, pair)| StrategyId(format!("strategy:{}", pair.id)))
        .collect();
    ensure!(
        actual_live == reviewed,
        "reviewed_live_strategies must exactly match execution-enabled source artifacts"
    );

    let accounts = manifest
        .accounts
        .iter()
        .map(|item| AccountNode {
            id: item.id.clone(),
            product: item.product.clone(),
            order_api_key_env: item.order_api_key_env.clone(),
            order_secret_key_env: item.order_secret_key_env.clone(),
            treasury_api_key_env: item.treasury_api_key_env.clone(),
            treasury_secret_key_env: item.treasury_secret_key_env.clone(),
            subaccount_email_env: item.subaccount_email_env.clone(),
        })
        .collect::<Vec<_>>();
    let wallets = manifest
        .wallets
        .iter()
        .map(|item| WalletNode {
            id: item.id.clone(),
            address_env: item.address_env.clone(),
            private_key_env: item.private_key_env.clone(),
        })
        .collect::<Vec<_>>();

    let mut networks_by_id = BTreeMap::<NetworkId, NetworkNode>::new();
    let mut venue_assets_by_id = BTreeMap::<VenueAssetId, VenueAssetNode>::new();
    let mut economic_assets_by_id = BTreeMap::<EconomicAssetId, EconomicAssetNode>::new();
    let mut asset_mappings_by_venue = BTreeMap::<VenueAssetId, AssetMapping>::new();
    let mut instruments = Vec::new();
    let mut strategies = Vec::new();
    let mut pools = Vec::new();
    let mut capabilities = Vec::new();
    let mut dependencies = Vec::new();

    for (source_snapshot_id, pair) in &pairs {
        let network_id = NetworkId(format!("eip155:{}", pair.chain.chain_id));
        insert_same(
            &mut networks_by_id,
            network_id.clone(),
            NetworkNode {
                id: network_id.clone(),
                chain_id: pair.chain.chain_id,
                name: pair.chain.name.clone(),
                rpc_url_env: pair.chain.rpc_url_env.clone(),
                ws_url_env: pair.chain.ws_url_env.clone(),
            },
            "network",
        )?;

        for (symbol, contract, decimals) in [
            (
                pair.token_a.symbol.as_str(),
                pair.token_a.contract.as_str(),
                pair.token_a.decimals,
            ),
            (
                pair.token_b.symbol.as_str(),
                pair.token_b.contract.as_str(),
                pair.token_b.decimals,
            ),
        ] {
            let economic_id = economic_asset_id(symbol);
            economic_assets_by_id
                .entry(economic_id.clone())
                .or_insert_with(|| EconomicAssetNode {
                    id: economic_id.clone(),
                    symbol: symbol.to_owned(),
                });
            let venue_id = VenueAssetId(format!(
                "{}:erc20:{}",
                network_id.as_str(),
                contract.to_ascii_lowercase()
            ));
            insert_same(
                &mut venue_assets_by_id,
                venue_id.clone(),
                VenueAssetNode {
                    id: venue_id.clone(),
                    kind: VenueAssetKind::Erc20,
                    symbol: symbol.to_owned(),
                    account_id: None,
                    network_id: Some(network_id.clone()),
                    contract: Some(contract.to_ascii_lowercase()),
                    decimals: Some(decimals),
                },
                "venue asset",
            )?;
            asset_mappings_by_venue.insert(
                venue_id.clone(),
                AssetMapping {
                    venue_asset_id: venue_id,
                    economic_asset_id: economic_id,
                },
            );
        }

        let gas_economic_id = economic_asset_id(&pair.chain.gas_symbol);
        economic_assets_by_id
            .entry(gas_economic_id.clone())
            .or_insert_with(|| EconomicAssetNode {
                id: gas_economic_id.clone(),
                symbol: pair.chain.gas_symbol.clone(),
            });
        let gas_venue_id = VenueAssetId(format!(
            "{}:native:{}",
            network_id.as_str(),
            pair.chain.gas_symbol
        ));
        insert_same(
            &mut venue_assets_by_id,
            gas_venue_id.clone(),
            VenueAssetNode {
                id: gas_venue_id.clone(),
                kind: VenueAssetKind::Native,
                symbol: pair.chain.gas_symbol.clone(),
                account_id: None,
                network_id: Some(network_id.clone()),
                contract: None,
                decimals: Some(pair.chain.gas_decimals),
            },
            "venue asset",
        )?;
        asset_mappings_by_venue.insert(
            gas_venue_id.clone(),
            AssetMapping {
                venue_asset_id: gas_venue_id,
                economic_asset_id: gas_economic_id,
            },
        );

        for symbol in [
            Some(pair.binance.base_asset.as_str()),
            Some(pair.binance.quote_asset.as_str()),
            pair.binance.commission_asset.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            let economic_id = economic_asset_id(symbol);
            economic_assets_by_id
                .entry(economic_id.clone())
                .or_insert_with(|| EconomicAssetNode {
                    id: economic_id.clone(),
                    symbol: symbol.to_owned(),
                });
            let venue_id = VenueAssetId(format!("{}:asset:{symbol}", account.id.as_str()));
            insert_same(
                &mut venue_assets_by_id,
                venue_id.clone(),
                VenueAssetNode {
                    id: venue_id.clone(),
                    kind: VenueAssetKind::BinanceSpot,
                    symbol: symbol.to_owned(),
                    account_id: Some(account.id.clone()),
                    network_id: None,
                    contract: None,
                    decimals: None,
                },
                "venue asset",
            )?;
            asset_mappings_by_venue.insert(
                venue_id.clone(),
                AssetMapping {
                    venue_asset_id: venue_id,
                    economic_asset_id: economic_id,
                },
            );
        }

        let instrument_id =
            InstrumentId(format!("{}:{}", account.id.as_str(), pair.binance.symbol));
        instruments.push(InstrumentNode {
            id: instrument_id.clone(),
            account_id: account.id.clone(),
            symbol: pair.binance.symbol.clone(),
            base_asset: VenueAssetId(format!(
                "{}:asset:{}",
                account.id.as_str(),
                pair.binance.base_asset
            )),
            quote_asset: VenueAssetId(format!(
                "{}:asset:{}",
                account.id.as_str(),
                pair.binance.quote_asset
            )),
        });

        let strategy_id = StrategyId(format!("strategy:{}", pair.id));
        let wallet_location_id =
            WalletLocationId(format!("{}:{}", network_id.as_str(), wallet.id.as_str()));
        strategies.push(StrategyNode {
            id: strategy_id.clone(),
            pair_id: pair.id.clone(),
            source_snapshot_id: (*source_snapshot_id).to_owned(),
            account_id: account.id.clone(),
            instrument_id: instrument_id.clone(),
            network_id: network_id.clone(),
            wallet_location_id,
        });
        capabilities.push(CapabilityEntry {
            strategy_id: strategy_id.clone(),
            observe: pair.market_data_enabled,
            plan: pair.market_data_enabled,
            execute: pair.execution_enabled,
            rebalance: pair.rebalance.enabled,
        });

        let mut strategy_pool_ids = Vec::new();
        if let Some(v3) = &pair.dex.uniswap_v3 {
            for fee_pips in &v3.fee_tiers {
                let resolution = manifest
                    .v3_pools
                    .iter()
                    .find(|pool| pool.pair_id == pair.id && pool.fee_pips == *fee_pips)
                    .with_context(|| {
                        format!(
                            "missing V3 pool resolution for {} fee {}",
                            pair.id, fee_pips
                        )
                    })?;
                let address = parse_address(&resolution.address)?;
                let identity = format!("V3 {{ address: {address}, fee_pips: {fee_pips} }}");
                let pool_id = PoolId(format!("{}:pool:{identity}", network_id.as_str()));
                pools.push(PoolNode {
                    id: pool_id.clone(),
                    pair_id: pair.id.clone(),
                    network_id: network_id.clone(),
                    protocol: PoolProtocol::UniswapV3,
                    canonical_identity: identity,
                    fee_pips: *fee_pips,
                    tick_spacing: None,
                    hooks: None,
                    lifecycle: resolution.lifecycle,
                });
                strategy_pool_ids.push(pool_id);
            }
        }
        if let Some(v4) = &pair.dex.uniswap_v4 {
            let token_a = parse_address(&pair.token_a.contract)?;
            let token_b = parse_address(&pair.token_b.contract)?;
            for configured_pool in &v4.pools {
                let hooks = parse_address(&configured_pool.hooks)?;
                let key = V4PoolKey::new(
                    token_a,
                    token_b,
                    configured_pool.fee_tier,
                    configured_pool.tick_spacing,
                    hooks,
                )?;
                let v4_pool_id = key.pool_id();
                let identity = format!(
                    "V4 {{ pool_id: {v4_pool_id}, fee_pips: {} }}",
                    configured_pool.fee_tier
                );
                let pool_id = PoolId(format!("{}:pool:{identity}", network_id.as_str()));
                pools.push(PoolNode {
                    id: pool_id.clone(),
                    pair_id: pair.id.clone(),
                    network_id: network_id.clone(),
                    protocol: PoolProtocol::UniswapV4,
                    canonical_identity: identity,
                    fee_pips: configured_pool.fee_tier,
                    tick_spacing: Some(configured_pool.tick_spacing),
                    hooks: Some(configured_pool.hooks.to_ascii_lowercase()),
                    lifecycle: if pair.execution_enabled {
                        PoolLifecycle::ExecutionEligible
                    } else {
                        PoolLifecycle::Validated
                    },
                });
                strategy_pool_ids.push(pool_id);
            }
        }
        strategy_pool_ids.sort();
        dependencies.push(StrategyDependency {
            strategy_id,
            instrument_id,
            pool_ids: strategy_pool_ids,
        });
    }

    let configured_v3: BTreeSet<_> = pairs
        .iter()
        .flat_map(|(_, pair)| {
            pair.dex
                .uniswap_v3
                .iter()
                .flat_map(|v3| v3.fee_tiers.iter())
                .map(move |fee| format!("{}:{fee}", pair.id))
        })
        .collect();
    let resolved_v3: BTreeSet<_> = manifest.v3_pools.iter().map(V3PoolManifest::key).collect();
    ensure!(
        configured_v3 == resolved_v3,
        "V3 pool resolutions must exactly cover configured V3 pools"
    );

    instruments.sort_by(|left, right| left.id.cmp(&right.id));
    strategies.sort_by(|left, right| left.id.cmp(&right.id));
    pools.sort_by(|left, right| left.id.cmp(&right.id));
    capabilities.sort_by(|left, right| left.strategy_id.cmp(&right.strategy_id));
    dependencies.sort_by(|left, right| left.strategy_id.cmp(&right.strategy_id));

    let networks: Vec<_> = networks_by_id.into_values().collect();
    let wallet_locations: Vec<_> = networks
        .iter()
        .map(|network| {
            let id = format!("{}:{}", network.id.as_str(), wallet.id.as_str());
            WalletLocationNode {
                id: WalletLocationId(id.clone()),
                network_id: network.id.clone(),
                wallet_id: wallet.id.clone(),
                execution_lane_id: ExecutionLaneId(id),
            }
        })
        .collect();

    let stream_shards = instruments
        .chunks(manifest.stream_shard_symbol_capacity)
        .enumerate()
        .map(|(index, chunk)| StreamShard {
            id: format!("stream:{}:{index}", account.id.as_str()),
            account_id: account.id.clone(),
            instrument_ids: chunk.iter().map(|item| item.id.clone()).collect(),
        })
        .collect();

    let mut owners = vec![
        OwnerAssignment {
            owner_id: "owner:hot-path-decision".to_owned(),
            kind: OwnerKind::HotPathDecision,
            owned_ids: strategies.iter().map(|item| item.id.0.clone()).collect(),
        },
        OwnerAssignment {
            owner_id: "owner:binance-account-state".to_owned(),
            kind: OwnerKind::BinanceAccountState,
            owned_ids: vec![account.id.0.clone()],
        },
        OwnerAssignment {
            owner_id: "owner:binance-order-execution".to_owned(),
            kind: OwnerKind::BinanceOrderExecution,
            owned_ids: vec![account.id.0.clone()],
        },
        OwnerAssignment {
            owner_id: "owner:binance-capital-saga".to_owned(),
            kind: OwnerKind::BinanceCapitalSaga,
            owned_ids: vec![account.id.0.clone()],
        },
        OwnerAssignment {
            owner_id: "owner:trade-coordinator".to_owned(),
            kind: OwnerKind::TradeCoordinator,
            owned_ids: strategies.iter().map(|item| item.id.0.clone()).collect(),
        },
        OwnerAssignment {
            owner_id: "owner:rebalance-saga".to_owned(),
            kind: OwnerKind::RebalanceSaga,
            owned_ids: strategies
                .iter()
                .filter(|strategy| {
                    capabilities.iter().any(|capability| {
                        capability.strategy_id == strategy.id && capability.rebalance
                    })
                })
                .map(|item| item.id.0.clone())
                .collect(),
        },
        OwnerAssignment {
            owner_id: "owner:portfolio".to_owned(),
            kind: OwnerKind::Portfolio,
            owned_ids: economic_assets_by_id
                .keys()
                .map(|id| id.0.clone())
                .collect(),
        },
    ];
    for network in &networks {
        owners.push(OwnerAssignment {
            owner_id: format!("owner:network:{}", network.id.as_str()),
            kind: OwnerKind::NetworkRuntime,
            owned_ids: pools
                .iter()
                .filter(|pool| pool.network_id == network.id)
                .map(|pool| pool.id.0.clone())
                .collect(),
        });
    }
    for location in &wallet_locations {
        owners.push(OwnerAssignment {
            owner_id: format!(
                "owner:evm-execution:{}",
                location.execution_lane_id.as_str()
            ),
            kind: OwnerKind::EvmExecution,
            owned_ids: vec![location.execution_lane_id.0.clone()],
        });
    }
    owners.sort_by(|left, right| left.owner_id.cmp(&right.owner_id));
    for owner in &mut owners {
        owner.owned_ids.sort();
    }

    let journals = manifest
        .journals
        .iter()
        .map(|item| JournalAssignment {
            owner_id: item.owner_id.clone(),
            path_env: item.path_env.clone(),
        })
        .collect::<Vec<_>>();

    let live_source = compiled_sources
        .iter()
        .find(|source| {
            source
                .snapshot
                .pairs
                .iter()
                .any(|pair| pair.id == "world-chain-usdc-wld" && pair.execution_enabled)
        })
        .context("compiled domain requires the reviewed WLD live compatibility source")?;
    let collector_source = compiled_sources
        .iter()
        .find(|source| {
            source
                .snapshot
                .pairs
                .iter()
                .any(|pair| pair.id == "arbitrum-usdc-esp")
        })
        .context("compiled domain requires the reviewed ESP collector compatibility source")?;
    let compatibility_projections = vec![
        CompatibilityProjection {
            id: CompatibilityRole::LiveRuntime.projection_id().to_owned(),
            role: CompatibilityRole::LiveRuntime,
            source_snapshot_id: live_source.snapshot_id.clone(),
            pair_ids: live_source
                .snapshot
                .pairs
                .iter()
                .map(|pair| pair.id.clone())
                .collect(),
        },
        CompatibilityProjection {
            id: CompatibilityRole::PublicPriceCollector
                .projection_id()
                .to_owned(),
            role: CompatibilityRole::PublicPriceCollector,
            source_snapshot_id: collector_source.snapshot_id.clone(),
            pair_ids: collector_source
                .snapshot
                .pairs
                .iter()
                .map(|pair| pair.id.clone())
                .collect(),
        },
    ];

    let mut required_environment = manifest.required_environment.clone();
    required_environment.sort_by(|left, right| left.projection_id.cmp(&right.projection_id));
    for requirement in &mut required_environment {
        requirement.names.sort();
    }
    for projection in &compatibility_projections {
        let source = compiled_sources
            .iter()
            .find(|source| source.snapshot_id == projection.source_snapshot_id)
            .expect("compatibility source selected above");
        let selected_pairs: BTreeSet<_> = projection.pair_ids.iter().map(String::as_str).collect();
        let mut minimum = BTreeSet::new();
        minimum.insert(wallet.address_env.clone());
        for pair in source
            .snapshot
            .pairs
            .iter()
            .filter(|pair| selected_pairs.contains(pair.id.as_str()))
        {
            minimum.insert(pair.chain.rpc_url_env.clone());
            minimum.insert(pair.chain.ws_url_env.clone());
            if pair.execution_enabled && projection.role == CompatibilityRole::LiveRuntime {
                minimum.insert(account.order_api_key_env.clone());
                minimum.insert(account.order_secret_key_env.clone());
                minimum.insert(wallet.private_key_env.clone());
            }
            if pair.rebalance.enabled && projection.role == CompatibilityRole::LiveRuntime {
                minimum.insert(account.treasury_api_key_env.clone());
                minimum.insert(account.treasury_secret_key_env.clone());
                minimum.insert(account.subaccount_email_env.clone());
            }
        }
        if projection.role == CompatibilityRole::LiveRuntime {
            minimum.extend(
                manifest
                    .journals
                    .iter()
                    .map(|journal| journal.path_env.clone()),
            );
        }
        let requirement = required_environment
            .iter()
            .find(|requirement| requirement.projection_id == projection.id)
            .with_context(|| {
                format!(
                    "missing required_environment for compatibility projection {}",
                    projection.id
                )
            })?;
        let configured: BTreeSet<_> = requirement.names.iter().cloned().collect();
        let missing: Vec<_> = minimum.difference(&configured).cloned().collect();
        ensure!(
            missing.is_empty(),
            "projection {} omits derived environment requirements: {}",
            projection.id,
            missing.join(", ")
        );
    }
    let mut journals = journals;
    journals.sort_by(|left, right| {
        (&left.owner_id, &left.path_env).cmp(&(&right.owner_id, &right.path_env))
    });

    let bundle = CompiledDomainBundle {
        bundle_kind: COMPILED_DOMAIN_KIND.to_owned(),
        schema_version: COMPILED_DOMAIN_SCHEMA_VERSION,
        bundle_id: manifest.bundle_id.clone(),
        compiler_version: manifest.compiler_version.clone(),
        sources: compiled_sources,
        accounts,
        instruments,
        networks,
        wallets,
        wallet_locations,
        venue_assets: venue_assets_by_id.into_values().collect(),
        economic_assets: economic_assets_by_id.into_values().collect(),
        asset_mappings: asset_mappings_by_venue.into_values().collect(),
        pools,
        strategies,
        dependencies,
        stream_shards,
        owners,
        journals,
        capabilities,
        compatibility_projections,
        required_environment,
    };
    CompiledDomainGraph::from_bundle(bundle.clone())?;
    Ok(bundle)
}

pub fn compile_manifest_to_path(
    manifest_path: impl AsRef<Path>,
    output_path: impl AsRef<Path>,
) -> anyhow::Result<String> {
    let manifest_path = manifest_path.as_ref();
    let manifest = DomainCompilerManifest::load(manifest_path)?;
    let repository_root = manifest_path
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .context("compiler manifest must be under config/domain")?;
    let sources = manifest.load_sources(repository_root)?;
    let bundle = compile_domain(&manifest, &sources)?;
    let canonical =
        serde_json::to_vec_pretty(&bundle).context("failed to serialize compiled domain")?;
    fs::write(output_path.as_ref(), &canonical).with_context(|| {
        format!(
            "failed to write compiled domain {}",
            output_path.as_ref().display()
        )
    })?;
    Ok(CompiledDomainGraph::from_bundle(bundle)?
        .fingerprint_sha256()
        .to_owned())
}

fn economic_asset_id(symbol: &str) -> EconomicAssetId {
    EconomicAssetId(format!("economic:{symbol}"))
}

fn source_pair<'a>(sources: &'a [CompiledSource], pair_id: &str) -> Option<&'a PairConfig> {
    sources
        .iter()
        .flat_map(|source| source.snapshot.pairs.iter())
        .find(|pair| pair.id == pair_id)
}

fn parse_address(value: &str) -> anyhow::Result<Address> {
    Address::from_str(value).with_context(|| format!("invalid EVM address {value}"))
}

fn validate_id(name: &str, value: &str) -> anyhow::Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 256
            && value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric()
                    || matches!(byte, b'_' | b'-' | b'.' | b':' | b'{' | b'}' | b',' | b' ')
            }),
        "{name} contains invalid characters"
    );
    Ok(())
}

fn validate_env_name(value: &str) -> anyhow::Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
            && value.as_bytes().first().is_some_and(u8::is_ascii_uppercase),
        "{value} is not a valid environment variable name"
    );
    Ok(())
}

fn validate_sha256(name: &str, value: &str) -> anyhow::Result<()> {
    ensure!(
        value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "{name} must be a SHA-256 hex digest"
    );
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut fingerprint = String::with_capacity(64);
    for byte in digest {
        write!(&mut fingerprint, "{byte:02x}").expect("writing SHA-256 into a String cannot fail");
    }
    fingerprint
}

fn unique_by(values: impl IntoIterator<Item = String>, name: &str) -> anyhow::Result<()> {
    let mut seen = BTreeSet::new();
    for value in values {
        ensure!(seen.insert(value.clone()), "duplicate {name} {value}");
    }
    Ok(())
}

fn index_ids<T, Id: Ord + Clone>(
    values: &[T],
    id: impl Fn(&T) -> Id,
    name: &str,
) -> anyhow::Result<BTreeMap<Id, usize>> {
    let mut index = BTreeMap::new();
    for (position, value) in values.iter().enumerate() {
        let key = id(value);
        ensure!(index.insert(key, position).is_none(), "duplicate {name} id");
    }
    Ok(index)
}

fn insert_same<K, V>(map: &mut BTreeMap<K, V>, key: K, value: V, name: &str) -> anyhow::Result<()>
where
    K: Ord,
    V: Eq,
{
    if let Some(existing) = map.get(&key) {
        ensure!(existing == &value, "conflicting {name} definition");
    } else {
        map.insert(key, value);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::{
        CompatibilityRole, CompiledCapitalAllocatorMode, CompiledDomainBundle, CompiledDomainGraph,
        CompiledInventoryLocation, CompiledNetworkGasPolicy, DomainCompilerManifest, PoolLifecycle,
        compile_domain,
    };
    use crate::domain::config::LoadedDomainConfig;

    fn root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).to_owned()
    }

    fn fixture() -> (
        DomainCompilerManifest,
        Vec<LoadedDomainConfig>,
        CompiledDomainBundle,
    ) {
        let root = root();
        let manifest = DomainCompilerManifest::load(
            root.join("config/domain/multi-pair-production.v1.sources.json"),
        )
        .unwrap();
        let sources = manifest.load_sources(&root).unwrap();
        let bundle = compile_domain(&manifest, &sources).unwrap();
        (manifest, sources, bundle)
    }

    #[test]
    fn compiles_exact_existing_pair_graph_and_typed_compatibility_ids() {
        let (_, _, bundle) = fixture();
        assert_eq!(
            bundle
                .instruments
                .iter()
                .map(|item| item.symbol.as_str())
                .collect::<Vec<_>>(),
            ["ESPUSDC", "WLDUSDC"]
        );
        assert_eq!(
            bundle
                .networks
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            ["eip155:42161", "eip155:480"]
        );
        assert_eq!(bundle.accounts[0].id.as_str(), "binance-spot:primary");
        assert_eq!(bundle.wallets[0].id.as_str(), "evm-wallet:primary");
        assert_eq!(bundle.strategies.len(), 2);
        assert_eq!(bundle.pools.len(), 5);
        let runtime = CompiledDomainGraph::from_bundle(bundle.clone())
            .unwrap()
            .binance_runtime_plan()
            .unwrap();
        assert_eq!(runtime.symbols, ["ESPUSDC", "WLDUSDC"]);
        assert_eq!(runtime.stream_shards.len(), 1);
        assert_eq!(runtime.stream_shards[0].symbols, ["ESPUSDC", "WLDUSDC"]);
        assert_eq!(
            runtime.executable_symbols,
            std::collections::BTreeSet::from(["ESPUSDC".to_owned(), "WLDUSDC".to_owned()])
        );
        assert!(runtime.asset_symbols.contains(&"BNB".to_owned()));
        assert!(runtime.asset_symbols.contains(&"ESP".to_owned()));
        assert_eq!(runtime.asset_decimals["USDC"], 6);
        assert_eq!(runtime.asset_decimals["ESP"], 18);
        assert_eq!(runtime.asset_decimals["WLD"], 18);
        assert_eq!(runtime.asset_decimals["BNB"], 8);
        assert_eq!(
            bundle
                .wallet_locations
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            [
                "eip155:42161:evm-wallet:primary",
                "eip155:480:evm-wallet:primary"
            ]
        );
        assert_eq!(bundle.stream_shards.len(), 1);
        assert_eq!(bundle.stream_shards[0].instrument_ids.len(), 2);
        assert!(bundle.required_environment.iter().all(|requirement| {
            requirement.names.iter().all(|name| {
                !["_SYMBOLS", "_PAIRS", "_POOLS", "_NETWORKS", "_ALLOWLIST"]
                    .iter()
                    .any(|suffix| name.ends_with(suffix))
            })
        }));
        assert!(
            bundle
                .pools
                .iter()
                .filter(|pool| pool.pair_id == "world-chain-usdc-wld")
                .all(|pool| pool.lifecycle == PoolLifecycle::ExecutionEligible)
        );
        assert!(
            bundle
                .pools
                .iter()
                .filter(|pool| pool.pair_id == "arbitrum-usdc-esp")
                .all(|pool| pool.lifecycle == PoolLifecycle::Validated)
        );
    }

    #[test]
    fn source_order_does_not_change_bundle_or_semantic_fingerprint() {
        let (manifest, mut sources, expected) = fixture();
        sources.reverse();
        let reversed = compile_domain(&manifest, &sources).unwrap();
        assert_eq!(reversed, expected);
        assert_eq!(
            CompiledDomainGraph::from_bundle(reversed)
                .unwrap()
                .fingerprint_sha256(),
            CompiledDomainGraph::from_bundle(expected)
                .unwrap()
                .fingerprint_sha256()
        );
    }

    #[test]
    fn checked_in_bundle_is_exact_compiler_output() {
        let (_, _, expected) = fixture();
        let bytes =
            std::fs::read(root().join("config/domain/compiled-multi-pair-production.v1.json"))
                .unwrap();
        let checked_in: CompiledDomainBundle = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(checked_in, expected);
    }

    #[test]
    fn compatibility_projections_round_trip_both_source_artifacts() {
        let (_, sources, bundle) = fixture();
        let graph = CompiledDomainGraph::from_bundle(bundle).unwrap();
        let live = graph
            .project("compiled.json", CompatibilityRole::LiveRuntime, false)
            .unwrap();
        let collector = graph
            .project(
                "compiled.json",
                CompatibilityRole::PublicPriceCollector,
                false,
            )
            .unwrap();
        let original_live = sources
            .iter()
            .find(|source| {
                source
                    .snapshot()
                    .pairs
                    .iter()
                    .any(|pair| pair.id == "world-chain-usdc-wld")
            })
            .unwrap();
        let original_collector = sources
            .iter()
            .find(|source| {
                source
                    .snapshot()
                    .pairs
                    .iter()
                    .any(|pair| pair.id == "arbitrum-usdc-esp")
            })
            .unwrap();
        assert_eq!(live.config.snapshot(), original_live.snapshot());
        assert_eq!(
            collector.config.snapshot().snapshot_id,
            original_collector.snapshot().snapshot_id
        );
        assert!(!collector.config.snapshot().live_trading_enabled);
        let collector_pair = &collector.config.snapshot().pairs[0];
        assert!(!collector_pair.full_live);
        assert!(collector_pair.full_live_policy.is_none());
        assert!(collector_pair.live_canary.is_none());
        let live_networks = live.network_runtime.unwrap();
        assert_eq!(
            live_networks
                .networks
                .iter()
                .map(|network| network.chain_id)
                .collect::<Vec<_>>(),
            [42_161, 480]
        );
        let world = live_networks
            .networks
            .iter()
            .find(|network| network.chain_id == 480)
            .unwrap();
        assert!(world.execution_enabled);
        assert_eq!(
            world.gas_policy,
            CompiledNetworkGasPolicy::WorldChainV12 {
                fallback_gas_price_wei: 100_000,
                includes_l1_fee: true,
            }
        );
        let arbitrum = live_networks
            .networks
            .iter()
            .find(|network| network.chain_id == 42_161)
            .unwrap();
        assert!(arbitrum.execution_enabled);
        assert_eq!(
            arbitrum.gas_policy,
            CompiledNetworkGasPolicy::ArbitrumOne {
                requires_fresh_rpc_gas_price: true,
                max_priority_fee_per_gas_wei: 0,
                max_fee_headroom_bps: 12_000,
                includes_l1_fee: false,
            }
        );
        assert_eq!(
            collector
                .network_runtime
                .unwrap()
                .networks
                .iter()
                .map(|network| network.chain_id)
                .collect::<Vec<_>>(),
            [42_161]
        );
        assert_eq!(
            live.config.fingerprint_sha256(),
            collector.config.fingerprint_sha256()
        );
        let hot_path = live.hot_path_runtime.unwrap();
        assert_eq!(hot_path.strategies.len(), 2);
        let wld = hot_path
            .strategies
            .iter()
            .find(|strategy| strategy.symbol == "WLDUSDC")
            .unwrap();
        assert!(wld.observe && wld.plan && wld.execute);
        assert_eq!(wld.network_id.as_str(), "eip155:480");
        assert_eq!(wld.pool_ids.len(), 4);
        assert!(wld.domain_config.snapshot().live_trading_enabled);
        let esp = hot_path
            .strategies
            .iter()
            .find(|strategy| strategy.symbol == "ESPUSDC")
            .unwrap();
        assert!(esp.observe && esp.plan && esp.execute);
        assert_eq!(esp.network_id.as_str(), "eip155:42161");
        assert_eq!(esp.pool_ids.len(), 1);
        assert!(esp.domain_config.snapshot().live_trading_enabled);
        assert_ne!(
            live.config.fingerprint_sha256(),
            original_live.fingerprint_sha256()
        );
        let portfolio = live.portfolio_runtime.unwrap();
        assert_eq!(
            portfolio.allocator_mode,
            CompiledCapitalAllocatorMode::FullLive
        );
        let capital_canary = portfolio.capital_canary.as_ref().unwrap();
        assert!(capital_canary.full_live);
        assert_eq!(capital_canary.network_id.as_str(), "eip155:42161");
        assert_eq!(capital_canary.maximum_transfer_count, 1);
        assert!(capital_canary.external_mutation_authorized);
        assert!(capital_canary.direct_route_only);
        assert!(!capital_canary.bridge_mutations_enabled);
        assert_eq!(portfolio.live_rebalance_adapter, "world_chain_v12_parity");
        assert_eq!(portfolio.assets.len(), 10);
        assert!(portfolio.assets.iter().any(|asset| {
            asset.symbol == "USDC"
                && matches!(
                    &asset.location,
                    CompiledInventoryLocation::EvmWallet { chain_id: 480, .. }
                )
        }));
        assert!(portfolio.assets.iter().any(|asset| {
            asset.symbol == "USDC"
                && matches!(
                    &asset.location,
                    CompiledInventoryLocation::EvmWallet {
                        chain_id: 42_161,
                        ..
                    }
                )
        }));
    }

    #[test]
    fn checked_in_esp_promotion_compiles_full_live_authority_and_public_projection_scrubs_it() {
        let (mut manifest, sources, bundle) = fixture();
        let esp_index = sources
            .iter()
            .position(|source| {
                source
                    .snapshot()
                    .pairs
                    .iter()
                    .any(|pair| pair.id == "arbitrum-usdc-esp")
            })
            .unwrap();
        let graph = CompiledDomainGraph::from_bundle(bundle).unwrap();
        let projected = graph
            .project("esp-full-live.json", CompatibilityRole::LiveRuntime, false)
            .unwrap();
        let portfolio = projected.portfolio_runtime.unwrap();
        assert_eq!(
            portfolio.allocator_mode,
            CompiledCapitalAllocatorMode::FullLive
        );
        assert!(
            portfolio
                .capital_canary
                .as_ref()
                .is_some_and(|policy| policy.external_mutation_authorized)
        );
        let esp = projected
            .hot_path_runtime
            .unwrap()
            .strategies
            .into_iter()
            .find(|strategy| strategy.symbol == "ESPUSDC")
            .unwrap();
        let pair = &esp.domain_config.snapshot().pairs[0];
        assert!(pair.execution_enabled && pair.full_live);
        assert!(pair.rebalance.enabled);
        let approved_policy = pair.full_live_policy.as_ref().unwrap();
        assert_eq!(approved_policy.production_approval_actor, "operator");
        assert!(pair.live_canary.is_none());
        assert_eq!(
            portfolio
                .capital_canary
                .as_ref()
                .unwrap()
                .maximum_token_b_debit,
            alloy_primitives::U256::from(10_000_u64)
                * alloy_primitives::U256::from(10_u64).pow(alloy_primitives::U256::from(18_u64))
        );

        let public = graph
            .project(
                "esp-full-live-public.json",
                CompatibilityRole::PublicPriceCollector,
                false,
            )
            .unwrap();
        assert!(
            public
                .portfolio_runtime
                .as_ref()
                .and_then(|portfolio| portfolio.capital_canary.as_ref())
                .is_some_and(|policy| !policy.external_mutation_authorized)
        );
        let public_pair = &public.config.snapshot().pairs[0];
        assert!(!public_pair.execution_enabled);
        assert!(!public_pair.rebalance.enabled);
        assert!(!public_pair.full_live);
        assert!(public_pair.full_live_policy.is_none());
        assert!(public_pair.live_canary.is_none());

        let mut disabled = serde_json::to_value(sources[esp_index].snapshot()).unwrap();
        disabled["live_trading_enabled"] = serde_json::Value::Bool(false);
        disabled["pairs"][0]["execution_enabled"] = serde_json::Value::Bool(false);
        disabled["pairs"][0]["full_live"] = serde_json::Value::Bool(false);
        disabled["pairs"][0]
            .as_object_mut()
            .unwrap()
            .remove("full_live_policy");
        disabled["pairs"][0]["rebalance"]["enabled"] = serde_json::Value::Bool(false);
        manifest
            .reviewed_live_strategies
            .retain(|strategy| strategy.as_str() != "strategy:arbitrum-usdc-esp");
        let mut disabled_sources = sources;
        disabled_sources[esp_index] = LoadedDomainConfig::from_bytes(
            disabled_sources[esp_index].path(),
            &serde_json::to_vec(&disabled).unwrap(),
        )
        .unwrap();
        let disabled_bundle = compile_domain(&manifest, &disabled_sources).unwrap();
        let disabled_projected = CompiledDomainGraph::from_bundle(disabled_bundle)
            .unwrap()
            .project("esp-disabled.json", CompatibilityRole::LiveRuntime, false)
            .unwrap();
        let disabled_portfolio = disabled_projected.portfolio_runtime.unwrap();
        assert_eq!(
            disabled_portfolio.allocator_mode,
            CompiledCapitalAllocatorMode::Shadow
        );
        assert!(disabled_portfolio.capital_canary.is_none());
    }

    #[test]
    fn selecting_bundle_cannot_grant_esp_execution() {
        let (_, _, bundle) = fixture();
        let graph = CompiledDomainGraph::from_bundle(bundle).unwrap();
        let collector = graph
            .project(
                "compiled.json",
                CompatibilityRole::PublicPriceCollector,
                false,
            )
            .unwrap();
        assert!(!collector.config.snapshot().live_trading_enabled);
        assert!(
            collector
                .config
                .snapshot()
                .pairs
                .iter()
                .all(|pair| !pair.execution_enabled && !pair.rebalance.enabled)
        );
    }

    #[test]
    fn rejects_unreviewed_live_capability_and_broken_references() {
        let (manifest, sources, bundle) = fixture();
        let mut unreviewed = manifest.clone();
        unreviewed.reviewed_live_strategies.clear();
        assert!(compile_domain(&unreviewed, &sources).is_err());

        let mut incomplete_environment = manifest;
        incomplete_environment.required_environment[0]
            .names
            .retain(|name| name != "ARBITRAGE_TRADE_JOURNAL_PATH");
        assert!(compile_domain(&incomplete_environment, &sources).is_err());

        let mut broken = bundle;
        broken.strategies[0].network_id.0 = "eip155:999".to_owned();
        assert!(CompiledDomainGraph::from_bundle(broken).is_err());
    }
}

use std::{
    collections::{BTreeMap, HashSet},
    fmt::Write,
    fs,
    path::{Path, PathBuf},
};

use alloy_primitives::{Address, B256, U256};
use anyhow::{Context, ensure};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const SUPPORTED_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone)]
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

    pub(crate) fn from_bytes(path: impl AsRef<Path>, bytes: &[u8]) -> anyhow::Result<Self> {
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

    pub(crate) fn from_projected_snapshot(
        path: impl AsRef<Path>,
        fingerprint_sha256: String,
        snapshot: DomainSnapshot,
    ) -> anyhow::Result<Self> {
        let path = path.as_ref();
        snapshot
            .validate()
            .with_context(|| format!("invalid projected domain config {}", path.display()))?;
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

    pub fn strategy_price_transport_silence_limits_ms(&self) -> BTreeMap<String, u64> {
        self.snapshot
            .pairs
            .iter()
            .filter(|pair| pair.market_data_enabled)
            .map(|pair| {
                (
                    pair.binance.symbol.clone(),
                    pair.strategy.max_transport_silence_ms(),
                )
            })
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

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
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

    pub(crate) fn validate_for_compiler(&self) -> anyhow::Result<()> {
        self.validate()
    }
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotSource {
    pub repository: String,
    pub revision: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rails_pair_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rails_pair_updated_at_utc: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rails_pair_candidate_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rails_pair_candidate_updated_at_utc: Option<String>,
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
        match (
            self.rails_pair_id,
            self.rails_pair_updated_at_utc.as_deref(),
            self.rails_pair_candidate_id,
            self.rails_pair_candidate_updated_at_utc.as_deref(),
        ) {
            (Some(id), Some(updated_at), None, None) => {
                ensure!(id > 0, "source.rails_pair_id must be positive");
                validate_non_empty("source.rails_pair_updated_at_utc", updated_at)?;
            }
            (None, None, Some(id), Some(updated_at)) => {
                ensure!(id > 0, "source.rails_pair_candidate_id must be positive");
                validate_non_empty("source.rails_pair_candidate_updated_at_utc", updated_at)?;
            }
            _ => anyhow::bail!(
                "source must identify exactly one Rails pair or pair candidate with its updated_at timestamp"
            ),
        }
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

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PairConfig {
    pub id: String,
    pub market_data_enabled: bool,
    pub execution_enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_canary: Option<LiveCanaryConfig>,
    pub chain: ChainConfig,
    pub token_a: TokenConfig,
    pub token_b: TokenConfig,
    pub binance: BinanceConfig,
    pub quote_sizing: QuoteSizingConfig,
    #[serde(default)]
    pub adaptive_sizing: AdaptiveSizingConfig,
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
        if let Some(canary) = &self.live_canary {
            canary.validate(self)?;
        }
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
        self.adaptive_sizing
            .validate(&self.quote_sizing.token_a_base_units)?;
        self.strategy.validate()?;
        self.rebalance.validate()?;
        self.dex.validate(&self.chain)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LiveCanaryConfig {
    pub approval_gate: LiveCanaryApprovalGate,
    #[serde(default)]
    pub production_approval_actor: Option<String>,
    #[serde(default)]
    pub production_approval_recorded_at_utc: Option<String>,
    pub max_trade_notional_token_a_base_units: String,
    pub max_total_notional_token_a_base_units: String,
    pub max_unhedged_notional_token_a_base_units: String,
    pub max_realized_loss_token_a_base_units: String,
    pub minimum_native_gas_wei: String,
    #[serde(default = "default_zero_base_units")]
    pub minimum_wallet_token_a_base_units: String,
    #[serde(default = "default_zero_base_units")]
    pub minimum_wallet_token_b_base_units: String,
    #[serde(default)]
    pub minimum_runtime_wallet_token_a_base_units: Option<String>,
    #[serde(default)]
    pub minimum_runtime_wallet_token_b_base_units: Option<String>,
    pub max_parent_trades: u16,
    #[serde(default = "default_live_canary_failure_limit")]
    pub max_failed_parent_trades: u16,
    pub max_concurrent_trades: u16,
    pub rollout_duration_seconds: u64,
    pub rebalance_mutations_enabled: bool,
    #[serde(default = "default_arbitrum_max_fee_headroom_bps")]
    pub arbitrum_max_fee_headroom_bps: u16,
    #[serde(default)]
    pub prefunding_rebalance: Option<LiveCanaryPrefundingRebalanceConfig>,
    #[serde(default)]
    pub rebalance_live_canary: Option<LiveCanaryRebalanceConfig>,
}

impl LiveCanaryConfig {
    pub fn runtime_wallet_token_a_minimum(&self) -> &str {
        self.minimum_runtime_wallet_token_a_base_units
            .as_deref()
            .unwrap_or(&self.minimum_wallet_token_a_base_units)
    }

    pub fn runtime_wallet_token_b_minimum(&self) -> &str {
        self.minimum_runtime_wallet_token_b_base_units
            .as_deref()
            .unwrap_or(&self.minimum_wallet_token_b_base_units)
    }

    fn validate(&self, pair: &PairConfig) -> anyhow::Result<()> {
        ensure!(
            pair.id == "arbitrum-usdc-esp"
                && pair.chain.chain_id == 42_161
                && pair.binance.symbol == "ESPUSDC"
                && pair.token_a.symbol == "USDC"
                && pair
                    .token_a
                    .contract
                    .eq_ignore_ascii_case("0xaf88d065e77c8cc2239327c5edb3a432268e5831")
                && pair.token_b.symbol == "ESP"
                && pair
                    .token_b
                    .contract
                    .eq_ignore_ascii_case("0x3b8db18e69d6686ad9371a423afe3dd1065c94f1")
                && pair
                    .chain
                    .uniswap_v3_router_address
                    .as_deref()
                    .is_some_and(|address| address
                        .eq_ignore_ascii_case("0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45")),
            "live canary is restricted to the reviewed ESPUSDC Arbitrum contracts and SwapRouter02"
        );
        match self.approval_gate {
            LiveCanaryApprovalGate::ExplicitProductionApprovalRequired => {
                ensure!(
                    !pair.execution_enabled
                        && self.production_approval_actor.is_none()
                        && self.production_approval_recorded_at_utc.is_none(),
                    "pair {} readiness artifact cannot enable execution or record approval before explicit approval",
                    pair.id
                );
            }
            LiveCanaryApprovalGate::ExplicitProductionApproved => {
                ensure!(
                    pair.execution_enabled
                        && self
                            .production_approval_actor
                            .as_deref()
                            .is_some_and(|value| !value.trim().is_empty())
                        && self
                            .production_approval_recorded_at_utc
                            .as_deref()
                            .is_some_and(|value| value.ends_with('Z')),
                    "pair {} live canary requires an auditable explicit approval record",
                    pair.id
                );
            }
        }
        if let Some(rebalance) = &self.rebalance_live_canary {
            rebalance.validate(pair, self)?;
        } else {
            ensure!(
                !self.rebalance_mutations_enabled && !pair.rebalance.enabled,
                "pair {} cannot enable rebalance mutations without an M10 policy",
                pair.id
            );
        }
        if let Some(prefunding) = &self.prefunding_rebalance {
            prefunding.validate(pair, self)?;
        }
        let maximum_trade = parse_base_units_u256(
            &self.max_trade_notional_token_a_base_units,
            "live_canary.max_trade_notional_token_a_base_units",
        )?;
        let maximum_total = parse_base_units_u256(
            &self.max_total_notional_token_a_base_units,
            "live_canary.max_total_notional_token_a_base_units",
        )?;
        let maximum_unhedged = parse_base_units_u256(
            &self.max_unhedged_notional_token_a_base_units,
            "live_canary.max_unhedged_notional_token_a_base_units",
        )?;
        let maximum_loss = parse_base_units_u256(
            &self.max_realized_loss_token_a_base_units,
            "live_canary.max_realized_loss_token_a_base_units",
        )?;
        let minimum_native_gas = parse_base_units_u256(
            &self.minimum_native_gas_wei,
            "live_canary.minimum_native_gas_wei",
        )?;
        let minimum_wallet_token_a = parse_base_units_u256(
            &self.minimum_wallet_token_a_base_units,
            "live_canary.minimum_wallet_token_a_base_units",
        )?;
        let minimum_wallet_token_b = parse_base_units_u256(
            &self.minimum_wallet_token_b_base_units,
            "live_canary.minimum_wallet_token_b_base_units",
        )?;
        let minimum_runtime_wallet_token_a = parse_base_units_u256(
            self.runtime_wallet_token_a_minimum(),
            "live_canary.minimum_runtime_wallet_token_a_base_units",
        )?;
        let minimum_runtime_wallet_token_b = parse_base_units_u256(
            self.runtime_wallet_token_b_minimum(),
            "live_canary.minimum_runtime_wallet_token_b_base_units",
        )?;
        ensure!(
            !maximum_trade.is_zero()
                && maximum_trade <= maximum_total
                && maximum_unhedged <= maximum_trade
                && maximum_loss <= maximum_total
                && !minimum_native_gas.is_zero(),
            "pair {} live canary monetary limits are inconsistent",
            pair.id
        );
        if self.approval_gate == LiveCanaryApprovalGate::ExplicitProductionApproved {
            ensure!(
                minimum_wallet_token_a >= maximum_total
                    && !minimum_wallet_token_b.is_zero()
                    && !minimum_runtime_wallet_token_a.is_zero()
                    && minimum_runtime_wallet_token_a <= maximum_trade
                    && minimum_runtime_wallet_token_a <= minimum_wallet_token_a
                    && !minimum_runtime_wallet_token_b.is_zero()
                    && minimum_runtime_wallet_token_b <= minimum_wallet_token_b,
                "pair {} approved live canary requires prefunding for both trade directions",
                pair.id
            );
        }
        ensure!(
            self.max_parent_trades > 0
                && self.max_parent_trades <= 10
                && self.max_failed_parent_trades > 0
                && self.max_failed_parent_trades <= self.max_parent_trades
                && self.max_concurrent_trades == 1,
            "pair {} live canary trade limits are outside the reviewed bounds",
            pair.id
        );
        ensure!(
            (60..=3_600).contains(&self.rollout_duration_seconds),
            "pair {} live canary duration is outside 60..=3600 seconds",
            pair.id
        );
        ensure!(
            (10_000..=15_000).contains(&self.arbitrum_max_fee_headroom_bps)
                && (self.approval_gate != LiveCanaryApprovalGate::ExplicitProductionApproved
                    || self.arbitrum_max_fee_headroom_bps >= 11_000),
            "pair {} Arbitrum maximum-fee headroom is outside the reviewed bounds",
            pair.id
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LiveCanaryRebalanceConfig {
    pub approval_gate: LiveCanaryApprovalGate,
    #[serde(default)]
    pub production_approval_actor: Option<String>,
    #[serde(default)]
    pub production_approval_recorded_at_utc: Option<String>,
    #[serde(default = "legacy_m10_approval_session_id")]
    pub approval_session_id: String,
    pub binance_network: String,
    pub maximum_transfer_count: u16,
    pub maximum_concurrent_transfers: u16,
    pub maximum_failed_transfers: u16,
    pub maximum_token_a_debit_base_units: String,
    pub maximum_token_b_debit_base_units: String,
    pub maximum_token_a_fee_base_units: String,
    pub maximum_token_b_fee_base_units: String,
    pub rollout_duration_seconds: u64,
    pub maximum_unknown_reconciliation_queries: u16,
    pub direct_route_only: bool,
    pub bridge_mutations_enabled: bool,
    #[serde(default)]
    pub approved_standard_withdrawal_recovery: Option<LiveCanaryStandardWithdrawalRecoveryConfig>,
    #[serde(default)]
    pub approved_manual_withdrawal_recovery: Option<LiveCanaryManualWithdrawalRecoveryConfig>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LiveCanaryStandardWithdrawalRecoveryConfig {
    pub production_approval_actor: String,
    pub production_approval_recorded_at_utc: String,
    pub operation_id: String,
    pub fingerprint: String,
    pub withdraw_order_id: String,
    pub token_symbol: String,
    pub amount_base_units: String,
    pub wallet_address: String,
    pub binance_network: String,
    pub master_transfer_transaction_id: u64,
    pub rejected_api_mode: String,
    pub retry_api_mode: String,
    pub rejected_http_status: u16,
    pub rejected_error_code: i64,
    pub rejected_error_message: String,
    pub capital_history_match_count: u16,
    pub capital_history_checked_at_utc: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LiveCanaryManualWithdrawalRecoveryConfig {
    pub production_approval_actor: String,
    pub production_approval_recorded_at_utc: String,
    pub operation_id: String,
    pub fingerprint: String,
    pub withdraw_order_id: String,
    pub token_symbol: String,
    pub gross_debit_base_units: String,
    pub expected_credit_base_units: String,
    pub expected_fee_base_units: String,
    pub wallet_balance_before_base_units: String,
    pub wallet_address: String,
    pub binance_network: String,
    pub master_transfer_transaction_id: u64,
    pub rejected_local_entity_travel_rule_id: i64,
    pub rejected_standard_travel_rule_id: i64,
    pub withdrawal_id: String,
    pub transaction_hash: String,
    pub apply_time_utc: String,
    pub complete_time_utc: String,
}

impl LiveCanaryRebalanceConfig {
    fn validate(&self, pair: &PairConfig, canary: &LiveCanaryConfig) -> anyhow::Result<()> {
        ensure!(
            (8..=64).contains(&self.approval_session_id.len())
                && self.approval_session_id.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':')
                }),
            "pair {} M10 approval session id is invalid",
            pair.id
        );
        match self.approval_gate {
            LiveCanaryApprovalGate::ExplicitProductionApprovalRequired => {
                ensure!(
                    !canary.rebalance_mutations_enabled
                        && !pair.rebalance.enabled
                        && self.production_approval_actor.is_none()
                        && self.production_approval_recorded_at_utc.is_none(),
                    "pair {} unapproved M10 policy cannot enable rebalance mutations",
                    pair.id
                );
            }
            LiveCanaryApprovalGate::ExplicitProductionApproved => {
                ensure!(
                    canary.rebalance_mutations_enabled
                        && pair.rebalance.enabled
                        && self
                            .production_approval_actor
                            .as_deref()
                            .is_some_and(|value| !value.trim().is_empty())
                        && self
                            .production_approval_recorded_at_utc
                            .as_deref()
                            .is_some_and(|value| value.ends_with('Z')),
                    "pair {} M10 live rebalance requires an auditable explicit approval",
                    pair.id
                );
            }
        }
        ensure!(
            pair.chain.chain_id == 42_161
                && self.binance_network == "ARBITRUM"
                && self.binance_network == pair.chain.binance_network_name
                && self.maximum_transfer_count == 2
                && self.maximum_concurrent_transfers == 1
                && self.maximum_failed_transfers == 1
                && self.rollout_duration_seconds == 900
                && self.maximum_unknown_reconciliation_queries == 1
                && self.direct_route_only
                && !self.bridge_mutations_enabled,
            "pair {} M10 route, concurrency, duration, or recovery bounds are invalid",
            pair.id
        );
        let token_a_debit = parse_base_units_u256(
            &self.maximum_token_a_debit_base_units,
            "live_canary.rebalance_live_canary.maximum_token_a_debit_base_units",
        )?;
        let token_b_debit = parse_base_units_u256(
            &self.maximum_token_b_debit_base_units,
            "live_canary.rebalance_live_canary.maximum_token_b_debit_base_units",
        )?;
        let token_a_fee = parse_base_units_u256(
            &self.maximum_token_a_fee_base_units,
            "live_canary.rebalance_live_canary.maximum_token_a_fee_base_units",
        )?;
        let token_b_fee = parse_base_units_u256(
            &self.maximum_token_b_fee_base_units,
            "live_canary.rebalance_live_canary.maximum_token_b_fee_base_units",
        )?;
        let reviewed_caps = match self.approval_session_id.as_str() {
            "esp-usdc-arbitrum-rebalance-20260730-r1" => {
                ensure!(
                    self.approved_standard_withdrawal_recovery.is_none()
                        && self.approved_manual_withdrawal_recovery.is_none(),
                    "pair {} R1 cannot approve a later endpoint recovery",
                    pair.id
                );
                (
                    U256::from(25_000_000_u64),
                    U256::from(401_200_000_000_000_000_000_u128),
                )
            }
            "esp-usdc-arbitrum-rebalance-20260731-r2" => {
                let recovery = self
                    .approved_standard_withdrawal_recovery
                    .as_ref()
                    .context("R2 requires the exact approved standard-withdrawal recovery")?;
                recovery.validate(pair)?;
                self.approved_manual_withdrawal_recovery
                    .as_ref()
                    .context("R2 requires the exact approved manual-withdrawal recovery")?
                    .validate(pair)?;
                (
                    U256::from(2_600_000_000_u64),
                    U256::from(10_000_u64) * U256::from(10_u64).pow(U256::from(18_u64)),
                )
            }
            _ => {
                anyhow::bail!(
                    "pair {} M10 approval session is not a reviewed production session",
                    pair.id
                )
            }
        };
        ensure!(
            token_a_debit == reviewed_caps.0
                && token_b_debit == reviewed_caps.1
                && token_a_fee == U256::from(5_000_000_u64)
                && token_b_fee == U256::from(2_000_000_000_000_000_000_u128)
                && token_a_fee < token_a_debit
                && token_b_fee < token_b_debit,
            "pair {} M10 value or fee caps differ from the reviewed approval session",
            pair.id
        );
        Ok(())
    }
}

impl LiveCanaryManualWithdrawalRecoveryConfig {
    fn validate(&self, pair: &PairConfig) -> anyhow::Result<()> {
        ensure!(
            self.production_approval_actor == "operator"
                && self.production_approval_recorded_at_utc == "2026-07-31T09:54:36Z"
                && self.operation_id == "rebalance-324-8b62a7c14f4ef643"
                && self.fingerprint
                    == "8b62a7c14f4ef6434a88c384bbb83c73ea919f7e59139db972f10ef7fc1ee43a"
                && self.withdraw_order_id == "rb8b62a7c14f4ef6434a88c384bbb83c"
                && self.token_symbol == pair.token_b.symbol
                && self.gross_debit_base_units == "4464938180550000000000"
                && self.expected_credit_base_units == "4463838180550000000000"
                && self.expected_fee_base_units == "1100000000000000000"
                && self.wallet_balance_before_base_units == "534923638887482447575"
                && self
                    .wallet_address
                    .eq_ignore_ascii_case("0x90D990C81320221D2882De32beeA78923c1e77A3")
                && self.binance_network == "ARBITRUM"
                && self.master_transfer_transaction_id == 396_036_135_710
                && self.rejected_local_entity_travel_rule_id == 67_294_348
                && self.rejected_standard_travel_rule_id == 67_298_920
                && self.withdrawal_id == "e02357b25de24e1ba9965bf524db37f7"
                && self.transaction_hash
                    == "0x553d9635dab1477c6aab9a17fc4ab860040e44db8ca085cb894a6b3184bc27fd"
                && self.transaction_hash.parse::<B256>().is_ok()
                && self.apply_time_utc == "2026-07-31T09:50:51Z"
                && self.complete_time_utc == "2026-07-31T09:52:10Z",
            "pair {} manual M12 withdrawal recovery differs from the exact reviewed receipt",
            pair.id
        );
        Ok(())
    }
}

impl LiveCanaryStandardWithdrawalRecoveryConfig {
    fn validate(&self, pair: &PairConfig) -> anyhow::Result<()> {
        ensure!(
            self.production_approval_actor == "operator"
                && self.production_approval_recorded_at_utc == "2026-07-31T09:01:31Z"
                && self.operation_id == "rebalance-324-8b62a7c14f4ef643"
                && self.fingerprint
                    == "8b62a7c14f4ef6434a88c384bbb83c73ea919f7e59139db972f10ef7fc1ee43a"
                && self.withdraw_order_id == "rb8b62a7c14f4ef6434a88c384bbb83c"
                && self.token_symbol == "ESP"
                && self.amount_base_units == "4464938180550000000000"
                && self
                    .wallet_address
                    .eq_ignore_ascii_case("0x90D990C81320221D2882De32beeA78923c1e77A3")
                && self.binance_network == "ARBITRUM"
                && self.master_transfer_transaction_id == 396_036_135_710
                && self.rejected_api_mode == "local_entity"
                && self.retry_api_mode == "standard"
                && self.rejected_http_status == 400
                && self.rejected_error_code == -4024
                && self.rejected_error_message == "[031031] User does not own this currency."
                && self.capital_history_match_count == 0
                && self.capital_history_checked_at_utc == "2026-07-31T09:02:15Z",
            "pair {} standard-withdrawal recovery differs from the exact reviewed incident",
            pair.id
        );
        Ok(())
    }
}

fn legacy_m10_approval_session_id() -> String {
    "esp-usdc-arbitrum-rebalance-20260730-r1".to_owned()
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LiveCanaryPrefundingRebalanceConfig {
    pub approval_gate: LiveCanaryApprovalGate,
    pub production_approval_actor: String,
    pub production_approval_recorded_at_utc: String,
    pub binance_network: String,
    pub withdrawal_api_mode: String,
    pub maximum_transfer_count: u16,
    pub maximum_token_a_withdrawal_fee_base_units: String,
    pub maximum_token_b_withdrawal_fee_base_units: String,
    pub maximum_token_a_debit_base_units: String,
    pub maximum_token_b_debit_base_units: String,
    #[serde(default)]
    pub retry_after_verified_address: bool,
    #[serde(default)]
    pub approved_travel_rule_recovery: Option<LiveCanaryTravelRuleRecoveryConfig>,
    #[serde(default)]
    pub approved_manual_token_b_credit: Option<LiveCanaryManualCreditRecoveryConfig>,
    #[serde(default)]
    pub approved_evm_prebroadcast_rejection:
        Option<LiveCanaryEvmPrebroadcastRejectionRecoveryConfig>,
    #[serde(default)]
    pub approved_absent_standard_withdrawal:
        Option<LiveCanaryAbsentStandardWithdrawalRecoveryConfig>,
    #[serde(default)]
    pub approved_absent_master_transfer: Option<LiveCanaryAbsentMasterTransferRecoveryConfig>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LiveCanaryTravelRuleRecoveryConfig {
    pub production_approval_actor: String,
    pub production_approval_recorded_at_utc: String,
    pub rejected_token_symbol: String,
    pub rejected_token_amount_base_units: String,
    pub rejected_http_status: u16,
    pub rejected_error_code: i64,
    pub rejected_error_message: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LiveCanaryManualCreditRecoveryConfig {
    pub production_approval_actor: String,
    pub production_approval_recorded_at_utc: String,
    pub operation_id: String,
    pub token_symbol: String,
    pub gross_debit_base_units: String,
    pub expected_credit_base_units: String,
    pub expected_fee_base_units: String,
    pub wallet_balance_before_base_units: String,
    pub transaction_hash: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LiveCanaryEvmPrebroadcastRejectionRecoveryConfig {
    pub production_approval_actor: String,
    pub production_approval_recorded_at_utc: String,
    pub operation_id: String,
    pub transaction_hash: String,
    pub nonce: u64,
    pub rpc_error_code: i64,
    pub rpc_error_message: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LiveCanaryAbsentStandardWithdrawalRecoveryConfig {
    pub production_approval_actor: String,
    pub production_approval_recorded_at_utc: String,
    pub operation_id: String,
    pub fingerprint: String,
    pub withdraw_order_id: String,
    pub token_symbol: String,
    pub amount_base_units: String,
    pub wallet_address: String,
    pub binance_network: String,
    pub bridge_chain_id: u64,
    pub wallet_chain_id: u64,
    pub bridge_balance_before_base_units: String,
    pub master_transfer_transaction_id: u64,
    pub reconciliation_queries: u16,
    pub rejected_http_status: u16,
    pub rejected_error_code: i64,
    pub rejected_error_message: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LiveCanaryAbsentMasterTransferRecoveryConfig {
    pub production_approval_actor: String,
    pub production_approval_recorded_at_utc: String,
    pub operation_id: String,
    pub fingerprint: String,
    pub withdraw_order_id: String,
    pub token_symbol: String,
    pub amount_base_units: String,
    pub wallet_address: String,
    pub binance_network: String,
    pub bridge_chain_id: u64,
    pub wallet_chain_id: u64,
    pub binance_balance_before_base_units: String,
    pub wallet_balance_before_base_units: String,
    pub first_absent_observed_at_utc: String,
    pub minimum_evidence_age_seconds: u64,
}

impl LiveCanaryPrefundingRebalanceConfig {
    fn validate(&self, pair: &PairConfig, canary: &LiveCanaryConfig) -> anyhow::Result<()> {
        let expected_withdrawal_api_mode =
            if canary
                .rebalance_live_canary
                .as_ref()
                .is_some_and(|rebalance| {
                    rebalance.approval_session_id == "esp-usdc-arbitrum-rebalance-20260731-r2"
                })
            {
                "standard"
            } else {
                "local_entity"
            };
        ensure!(
            self.approval_gate == LiveCanaryApprovalGate::ExplicitProductionApproved
                && canary.approval_gate == LiveCanaryApprovalGate::ExplicitProductionApproved
                && !self.production_approval_actor.trim().is_empty()
                && self.production_approval_recorded_at_utc.ends_with('Z')
                && self.binance_network == "ARBITRUM"
                && self.withdrawal_api_mode == expected_withdrawal_api_mode
                && pair.chain.chain_id == 42_161
                && pair.chain.binance_network_name == self.binance_network
                && self.maximum_transfer_count == 2,
            "pair {} prefunding rebalance approval or route is invalid",
            pair.id
        );
        let token_a_fee = parse_base_units_u256(
            &self.maximum_token_a_withdrawal_fee_base_units,
            "live_canary.prefunding_rebalance.maximum_token_a_withdrawal_fee_base_units",
        )?;
        let token_b_fee = parse_base_units_u256(
            &self.maximum_token_b_withdrawal_fee_base_units,
            "live_canary.prefunding_rebalance.maximum_token_b_withdrawal_fee_base_units",
        )?;
        let token_a_debit = parse_base_units_u256(
            &self.maximum_token_a_debit_base_units,
            "live_canary.prefunding_rebalance.maximum_token_a_debit_base_units",
        )?;
        let token_b_debit = parse_base_units_u256(
            &self.maximum_token_b_debit_base_units,
            "live_canary.prefunding_rebalance.maximum_token_b_debit_base_units",
        )?;
        let token_a_target = parse_base_units_u256(
            &canary.minimum_wallet_token_a_base_units,
            "live_canary.minimum_wallet_token_a_base_units",
        )?;
        let token_b_target = parse_base_units_u256(
            &canary.minimum_wallet_token_b_base_units,
            "live_canary.minimum_wallet_token_b_base_units",
        )?;
        ensure!(
            !token_a_fee.is_zero()
                && token_a_fee <= U256::from(5_000_000_u64)
                && !token_b_fee.is_zero()
                && token_b_fee <= U256::from(100_000_000_000_000_000_000_u128)
                && token_a_debit >= token_a_target
                && token_a_debit <= token_a_target + token_a_fee
                && token_b_debit >= token_b_target
                && token_b_debit <= token_b_target + token_b_fee,
            "pair {} prefunding fee or debit caps exceed the reviewed bounds",
            pair.id
        );
        if let Some(recovery) = &self.approved_travel_rule_recovery {
            recovery.validate(pair, canary, self)?;
        }
        if let Some(recovery) = &self.approved_manual_token_b_credit {
            recovery.validate(pair, canary)?;
        }
        if let Some(recovery) = &self.approved_evm_prebroadcast_rejection {
            recovery.validate(pair)?;
        }
        if let Some(recovery) = &self.approved_absent_standard_withdrawal {
            recovery.validate(pair)?;
        }
        if let Some(recovery) = &self.approved_absent_master_transfer {
            recovery.validate(pair)?;
        }
        ensure!(
            !self.retry_after_verified_address || self.approved_travel_rule_recovery.is_some(),
            "pair {} cannot retry after address verification without the exact rejected incident",
            pair.id
        );
        Ok(())
    }
}

impl LiveCanaryAbsentMasterTransferRecoveryConfig {
    fn validate(&self, pair: &PairConfig) -> anyhow::Result<()> {
        let amount = parse_base_units_u256(
            &self.amount_base_units,
            "live_canary.prefunding_rebalance.approved_absent_master_transfer.amount_base_units",
        )?;
        let binance_balance_before = parse_base_units_u256(
            &self.binance_balance_before_base_units,
            "live_canary.prefunding_rebalance.approved_absent_master_transfer.binance_balance_before_base_units",
        )?;
        let wallet_balance_before = parse_base_units_u256(
            &self.wallet_balance_before_base_units,
            "live_canary.prefunding_rebalance.approved_absent_master_transfer.wallet_balance_before_base_units",
        )?;
        ensure!(
            self.production_approval_actor == "operator"
                && self.production_approval_recorded_at_utc == "2026-07-31T02:18:00Z"
                && self.operation_id == "rebalance-294-96fd53e70c1ab390"
                && self.fingerprint
                    == "96fd53e70c1ab390ae3e62eb434cd19f5c5e9e1434754bbbddc34d932f0efb50"
                && self.withdraw_order_id == "rb96fd53e70c1ab390ae3e62eb434cd1"
                && self.token_symbol == "USDC"
                && amount == U256::from(1_197_503_244_u64)
                && self
                    .wallet_address
                    .eq_ignore_ascii_case("0x90D990C81320221D2882De32beeA78923c1e77A3")
                && self.wallet_address.parse::<Address>().is_ok()
                && self.binance_network == "OPTIMISM"
                && self.bridge_chain_id == 10
                && self.wallet_chain_id == 480
                && binance_balance_before == U256::from(3_075_000_679_u64)
                && wallet_balance_before == U256::from(679_994_191_u64)
                && self.first_absent_observed_at_utc == "2026-07-31T02:13:53Z"
                && self.minimum_evidence_age_seconds == 300
                && pair.chain.chain_id == 42_161,
            "pair {} absent master-transfer recovery identity is invalid",
            pair.id
        );
        Ok(())
    }
}

impl LiveCanaryAbsentStandardWithdrawalRecoveryConfig {
    fn validate(&self, pair: &PairConfig) -> anyhow::Result<()> {
        let amount = parse_base_units_u256(
            &self.amount_base_units,
            "live_canary.prefunding_rebalance.approved_absent_standard_withdrawal.amount_base_units",
        )?;
        let bridge_balance_before = parse_base_units_u256(
            &self.bridge_balance_before_base_units,
            "live_canary.prefunding_rebalance.approved_absent_standard_withdrawal.bridge_balance_before_base_units",
        )?;
        ensure!(
            self.production_approval_actor == "operator"
                && self.production_approval_recorded_at_utc == "2026-07-31T03:03:36Z"
                && self.operation_id == "rebalance-296-96fd53e70c1ab390"
                && self.fingerprint
                    == "96fd53e70c1ab390ae3e62eb434cd19f5c5e9e1434754bbbddc34d932f0efb50"
                && self.withdraw_order_id == "rb96fd53e70c1ab390ae3e62eb434cd1"
                && self.token_symbol == "USDC"
                && amount == U256::from(1_197_503_244_u64)
                && self
                    .wallet_address
                    .eq_ignore_ascii_case("0x90D990C81320221D2882De32beeA78923c1e77A3")
                && self.wallet_address.parse::<Address>().is_ok()
                && self.binance_network == "OPTIMISM"
                && self.bridge_chain_id == 10
                && self.wallet_chain_id == 480
                && bridge_balance_before == U256::from(508_u64)
                && self.master_transfer_transaction_id == 395_924_104_268
                && self.reconciliation_queries == 1
                && self.rejected_http_status == 400
                && self.rejected_error_code == -4104
                && self.rejected_error_message
                    == "Please note that withdrawals are not permitted due to travel rule restrictions. To facilitate the withdrawal process, please refer to Travel Rule documentation."
                && pair.chain.chain_id == 42_161,
            "pair {} absent standard-withdrawal recovery identity is invalid",
            pair.id
        );
        Ok(())
    }
}

impl LiveCanaryEvmPrebroadcastRejectionRecoveryConfig {
    fn validate(&self, pair: &PairConfig) -> anyhow::Result<()> {
        ensure!(
            !self.production_approval_actor.trim().is_empty()
                && self.production_approval_recorded_at_utc.ends_with('Z')
                && pair.chain.chain_id == 42_161
                && self.operation_id == "rustarb-m9-setup-v3-ESP.v3-router-approval"
                && self.transaction_hash
                    == "0xbdfaa80920ebd8513a01d9a368f581ae8b552e8f4528be54586eeb0963079977"
                && self.transaction_hash.parse::<B256>().is_ok()
                && self.nonce == 1
                && self.rpc_error_code == -32_000
                && self.rpc_error_message
                    == "max fee per gas less than block base fee: address 0x90D990C81320221D2882De32beeA78923c1e77A3, maxFeePerGas: 20102000 baseFee: 20148000",
            "pair {} EVM pre-broadcast rejection recovery identity is invalid",
            pair.id
        );
        Ok(())
    }
}

impl LiveCanaryManualCreditRecoveryConfig {
    fn validate(&self, pair: &PairConfig, canary: &LiveCanaryConfig) -> anyhow::Result<()> {
        let gross_debit = parse_base_units_u256(
            &self.gross_debit_base_units,
            "live_canary.prefunding_rebalance.approved_manual_token_b_credit.gross_debit_base_units",
        )?;
        let expected_credit = parse_base_units_u256(
            &self.expected_credit_base_units,
            "live_canary.prefunding_rebalance.approved_manual_token_b_credit.expected_credit_base_units",
        )?;
        let expected_fee = parse_base_units_u256(
            &self.expected_fee_base_units,
            "live_canary.prefunding_rebalance.approved_manual_token_b_credit.expected_fee_base_units",
        )?;
        let wallet_balance_before = parse_base_units_u256(
            &self.wallet_balance_before_base_units,
            "live_canary.prefunding_rebalance.approved_manual_token_b_credit.wallet_balance_before_base_units",
        )?;
        let target = parse_base_units_u256(
            &canary.minimum_wallet_token_b_base_units,
            "live_canary.minimum_wallet_token_b_base_units",
        )?;
        ensure!(
            !self.production_approval_actor.trim().is_empty()
                && self.production_approval_recorded_at_utc.ends_with('Z')
                && self.operation_id == "rebalance-268-15f59bc55dcaed54"
                && self.token_symbol == pair.token_b.symbol
                && gross_debit == U256::from(401_200_000_000_000_000_000_u128)
                && expected_credit == target
                && expected_fee == U256::from(1_200_000_000_000_000_000_u128)
                && gross_debit == expected_credit + expected_fee
                && wallet_balance_before.is_zero()
                && self.transaction_hash.parse::<B256>().is_ok(),
            "pair {} manual ESP credit recovery identity is invalid",
            pair.id
        );
        Ok(())
    }
}

impl LiveCanaryTravelRuleRecoveryConfig {
    fn validate(
        &self,
        pair: &PairConfig,
        canary: &LiveCanaryConfig,
        prefunding: &LiveCanaryPrefundingRebalanceConfig,
    ) -> anyhow::Result<()> {
        ensure!(
            prefunding.maximum_transfer_count == 2
                && !self.production_approval_actor.trim().is_empty()
                && self.production_approval_recorded_at_utc.ends_with('Z')
                && self.rejected_token_symbol == pair.token_b.symbol
                && self.rejected_http_status == 400
                && self.rejected_error_code == -4024
                && self.rejected_error_message == "[031031] User does not own this currency.",
            "pair {} Travel Rule recovery approval or incident identity is invalid",
            pair.id
        );
        let rejected_amount = parse_base_units_u256(
            &self.rejected_token_amount_base_units,
            "live_canary.prefunding_rebalance.approved_travel_rule_recovery.rejected_token_amount_base_units",
        )?;
        let token_b_target = parse_base_units_u256(
            &canary.minimum_wallet_token_b_base_units,
            "live_canary.minimum_wallet_token_b_base_units",
        )?;
        ensure!(
            rejected_amount == token_b_target + U256::from(1_200_000_000_000_000_000_u128),
            "pair {} Travel Rule incident amount differs from the rejected ESP debit",
            pair.id
        );
        Ok(())
    }
}

const fn default_live_canary_failure_limit() -> u16 {
    1
}

const fn default_arbitrum_max_fee_headroom_bps() -> u16 {
    10_000
}

fn default_zero_base_units() -> String {
    "0".to_owned()
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveCanaryApprovalGate {
    ExplicitProductionApprovalRequired,
    ExplicitProductionApproved,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum AdaptiveSizingConfig {
    BaselineOnly,
    Shadow {
        max_trade_notional_token_a_base_units: String,
        max_unhedged_notional_token_a_base_units: String,
        max_recovery_loss_token_a_base_units: String,
        #[serde(alias = "min_bounded_profit_token_a_base_units")]
        min_expected_profit_token_a_base_units: String,
        #[serde(alias = "min_incremental_bounded_profit_token_a_base_units")]
        min_incremental_expected_profit_token_a_base_units: String,
        #[serde(default)]
        depth_policy: AdaptiveDepthPolicy,
    },
    Adaptive {
        max_trade_notional_token_a_base_units: String,
        max_unhedged_notional_token_a_base_units: String,
        max_recovery_loss_token_a_base_units: String,
        #[serde(alias = "min_bounded_profit_token_a_base_units")]
        min_expected_profit_token_a_base_units: String,
        #[serde(alias = "min_incremental_bounded_profit_token_a_base_units")]
        min_incremental_expected_profit_token_a_base_units: String,
        #[serde(default)]
        depth_policy: AdaptiveDepthPolicy,
    },
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdaptiveDepthPolicy {
    pub recent_full_depth_max_age_ms: u64,
    pub recent_full_depth_max_update_delta: u64,
    pub top_of_book_max_trade_notional_token_a_base_units: String,
}

impl Default for AdaptiveDepthPolicy {
    fn default() -> Self {
        Self {
            recent_full_depth_max_age_ms: 0,
            recent_full_depth_max_update_delta: 0,
            top_of_book_max_trade_notional_token_a_base_units: "0".to_owned(),
        }
    }
}

impl Default for AdaptiveSizingConfig {
    fn default() -> Self {
        Self::BaselineOnly
    }
}

impl AdaptiveSizingConfig {
    pub const fn mode(&self) -> &'static str {
        match self {
            Self::BaselineOnly => "baseline_only",
            Self::Shadow { .. } => "shadow",
            Self::Adaptive { .. } => "adaptive",
        }
    }

    pub fn limits(&self) -> Option<AdaptiveSizingLimits<'_>> {
        let (
            max_trade_notional,
            max_unhedged_notional,
            max_recovery_loss,
            min_expected_profit,
            min_incremental_expected_profit,
            depth_policy,
        ) = match self {
            Self::BaselineOnly => return None,
            Self::Shadow {
                max_trade_notional_token_a_base_units,
                max_unhedged_notional_token_a_base_units,
                max_recovery_loss_token_a_base_units,
                min_expected_profit_token_a_base_units,
                min_incremental_expected_profit_token_a_base_units,
                depth_policy,
            }
            | Self::Adaptive {
                max_trade_notional_token_a_base_units,
                max_unhedged_notional_token_a_base_units,
                max_recovery_loss_token_a_base_units,
                min_expected_profit_token_a_base_units,
                min_incremental_expected_profit_token_a_base_units,
                depth_policy,
            } => (
                max_trade_notional_token_a_base_units,
                max_unhedged_notional_token_a_base_units,
                max_recovery_loss_token_a_base_units,
                min_expected_profit_token_a_base_units,
                min_incremental_expected_profit_token_a_base_units,
                depth_policy,
            ),
        };
        Some(AdaptiveSizingLimits {
            max_trade_notional,
            max_unhedged_notional,
            max_recovery_loss,
            min_expected_profit,
            min_incremental_expected_profit,
            depth_policy,
        })
    }

    fn validate(&self, baseline_token_a: &str) -> anyhow::Result<()> {
        let Some(limits) = self.limits() else {
            return Ok(());
        };
        validate_positive_base_units(
            "adaptive_sizing.max_trade_notional_token_a_base_units",
            limits.max_trade_notional,
        )?;
        validate_positive_base_units(
            "adaptive_sizing.max_unhedged_notional_token_a_base_units",
            limits.max_unhedged_notional,
        )?;
        validate_non_negative_base_units(
            "adaptive_sizing.max_recovery_loss_token_a_base_units",
            limits.max_recovery_loss,
        )?;
        validate_non_negative_base_units(
            "adaptive_sizing.min_expected_profit_token_a_base_units",
            limits.min_expected_profit,
        )?;
        validate_non_negative_base_units(
            "adaptive_sizing.min_incremental_expected_profit_token_a_base_units",
            limits.min_incremental_expected_profit,
        )?;
        let parse_u256 = |value: &str, name: &str| {
            U256::from_str_radix(value, 10).with_context(|| format!("{name} exceeds uint256"))
        };
        let max_trade = parse_u256(limits.max_trade_notional, "adaptive max trade notional")?;
        let max_unhedged = parse_u256(
            limits.max_unhedged_notional,
            "adaptive max unhedged notional",
        )?;
        parse_u256(limits.max_recovery_loss, "adaptive max recovery loss")?;
        parse_u256(limits.min_expected_profit, "adaptive min expected profit")?;
        parse_u256(
            limits.min_incremental_expected_profit,
            "adaptive min incremental expected profit",
        )?;
        ensure!(
            (limits.depth_policy.recent_full_depth_max_age_ms == 0)
                == (limits.depth_policy.recent_full_depth_max_update_delta == 0),
            "adaptive recent full-depth age and update-delta caps must both be zero or both be positive"
        );
        let top_of_book_max_trade = parse_u256(
            &limits
                .depth_policy
                .top_of_book_max_trade_notional_token_a_base_units,
            "adaptive top-of-book max trade notional",
        )?;
        let baseline = parse_u256(baseline_token_a, "quote sizing baseline")?;
        ensure!(
            max_trade >= baseline,
            "adaptive max trade notional must be at least the baseline"
        );
        ensure!(
            max_unhedged >= max_trade,
            "adaptive max unhedged notional must be at least the max trade notional"
        );
        ensure!(
            top_of_book_max_trade.is_zero() || top_of_book_max_trade >= baseline,
            "adaptive top-of-book max trade notional must be zero or at least the baseline"
        );
        ensure!(
            top_of_book_max_trade <= max_trade,
            "adaptive top-of-book max trade notional must not exceed the global max trade notional"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AdaptiveSizingLimits<'a> {
    pub max_trade_notional: &'a str,
    pub max_unhedged_notional: &'a str,
    pub max_recovery_loss: &'a str,
    pub min_expected_profit: &'a str,
    pub min_incremental_expected_profit: &'a str,
    pub depth_policy: &'a AdaptiveDepthPolicy,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
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

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChainConfig {
    pub name: String,
    pub chain_id: u64,
    pub rpc_url_env: String,
    pub ws_url_env: String,
    pub binance_network_name: String,
    pub gas_symbol: String,
    pub gas_decimals: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gas_price_binance_symbol: Option<String>,
    pub multicall3_address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uniswap_v3_factory_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uniswap_v3_quoter_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uniswap_v3_router_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uniswap_v4_quoter_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uniswap_v4_router_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uniswap_v4_pool_manager_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
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

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
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

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BinanceConfig {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    /// Optional discounted-fee asset. Historical artifacts omit it; live
    /// production config declares BNB explicitly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commission_asset: Option<String>,
    /// Decimal precision used to normalize the commission-asset balance into
    /// the process-scoped inventory ledger.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commission_asset_decimals: Option<u8>,
    /// Spot symbol whose bid values one commission asset in token-A-equivalent
    /// quote units, matching Rails' BNBUSDT valuation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commission_price_binance_symbol: Option<String>,
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
            self.commission_asset.is_some() == self.commission_asset_decimals.is_some()
                && self.commission_asset.is_some()
                    == self.commission_price_binance_symbol.is_some(),
            "binance commission_asset, commission_asset_decimals, and commission_price_binance_symbol must be configured together"
        );
        if let (Some(asset), Some(decimals), Some(symbol)) = (
            self.commission_asset.as_deref(),
            self.commission_asset_decimals,
            self.commission_price_binance_symbol.as_deref(),
        ) {
            validate_symbol("binance.commission_asset", asset)?;
            ensure!(
                decimals <= 36,
                "binance.commission_asset_decimals {decimals} is implausible"
            );
            validate_symbol("binance.commission_price_binance_symbol", symbol)?;
            ensure!(
                symbol.starts_with(asset),
                "Binance commission price symbol must use commission_asset as its base"
            );
        }
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

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BinanceProduct {
    Spot,
    UsdMFutures,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
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

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum TokenBQuoteSizing {
    DeriveFromBinanceAsk,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyConfig {
    pub kind: ArbitrageStrategy,
    pub opportunity_threshold_bps: u16,
    /// Retained for deterministic deserialization of historical artifacts.
    /// Production v12 uses `max_transport_silence_ms`; older artifacts fall
    /// back to this value so transport readiness remains fail-closed.
    pub max_quote_age_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_transport_silence_ms: Option<u64>,
    pub min_slippage_bps: u16,
    pub max_slippage_bps: u16,
    pub slippage_profit_share_bps: u16,
    /// Deserializes historical artifacts that copied the 0x-only four-basis-
    /// point reserve. Uniswap V3/V4 execution never reads this value, and the
    /// production v12 artifact omits it.
    #[serde(
        default,
        rename = "dex_fee_reserve_bps",
        skip_serializing_if = "Option::is_none"
    )]
    pub legacy_dex_fee_reserve_bps: Option<u16>,
    /// Read-only compatibility for pre-adaptive artifacts. Rust inventory
    /// reservations use an exact execution envelope and never multiply claims.
    #[serde(
        default = "default_legacy_balance_safety_multiplier",
        skip_serializing_if = "is_default_legacy_balance_safety_multiplier"
    )]
    pub balance_safety_multiplier: u16,
}

const fn default_legacy_balance_safety_multiplier() -> u16 {
    1
}

fn is_default_legacy_balance_safety_multiplier(value: &u16) -> bool {
    *value == default_legacy_balance_safety_multiplier()
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
        ensure!(
            self.max_transport_silence_ms() > 0,
            "strategy.max_transport_silence_ms must be positive"
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
        if let Some(legacy_dex_fee_reserve_bps) = self.legacy_dex_fee_reserve_bps {
            validate_bps("strategy.dex_fee_reserve_bps", legacy_dex_fee_reserve_bps)?;
        }
        ensure!(
            self.balance_safety_multiplier > 0,
            "strategy.balance_safety_multiplier must be positive"
        );
        Ok(())
    }

    pub fn max_transport_silence_ms(&self) -> u64 {
        self.max_transport_silence_ms
            .unwrap_or(self.max_quote_age_ms)
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArbitrageStrategy {
    Legacy,
    ProfitTokenA,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DexConfig {
    pub allowed_providers: Vec<DexProvider>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uniswap_v3: Option<UniswapV3Config>,
    #[serde(skip_serializing_if = "Option::is_none")]
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

#[derive(Debug, Clone, Copy, Deserialize, Hash, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DexProvider {
    ZeroX,
    UniswapV3,
    UniswapV4,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
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

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
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

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
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

fn parse_base_units_u256(value: &str, name: &str) -> anyhow::Result<U256> {
    U256::from_str_radix(value, 10).with_context(|| format!("{name} exceeds uint256"))
}

fn validate_non_negative_base_units(name: &str, value: &str) -> anyhow::Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 78
            && value.bytes().all(|byte| byte.is_ascii_digit())
            && (value == "0" || !value.starts_with('0')),
        "{name} must be a non-negative canonical uint256 decimal string"
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

    use super::{
        AdaptiveSizingConfig, ArbitrageStrategy, BinanceProduct, DexProvider,
        LiveCanaryApprovalGate, LoadedDomainConfig, TokenBQuoteSizing,
    };

    const CONFIG: &str = include_str!("../../config/strategies/usdc-wld-world-chain.v4.json");
    const LEGACY_LIVE_CONFIG: &str =
        include_str!("../../config/strategies/usdc-wld-world-chain.v6.json");
    const SHADOW_LIVE_CONFIG: &str =
        include_str!("../../config/strategies/usdc-wld-world-chain.v7.json");
    const V8_LIVE_CONFIG: &str =
        include_str!("../../config/strategies/usdc-wld-world-chain.v8.json");
    const V9_LIVE_CONFIG: &str =
        include_str!("../../config/strategies/usdc-wld-world-chain.v9.json");
    const V10_LIVE_CONFIG: &str =
        include_str!("../../config/strategies/usdc-wld-world-chain.v10.json");
    const LIVE_CONFIG: &str = include_str!("../../config/strategies/usdc-wld-world-chain.v12.json");
    const ESP_SHADOW_CONFIG: &str =
        include_str!("../../config/strategies/usdc-esp-arbitrum.v2.json");
    const ESP_READINESS_CONFIG: &str =
        include_str!("../../config/strategies/usdc-esp-arbitrum.v3.json");
    const ESP_CANARY_CONFIG: &str =
        include_str!("../../config/strategies/usdc-esp-arbitrum.v5.json");

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
        assert_eq!(pair.adaptive_sizing, AdaptiveSizingConfig::BaselineOnly);
        assert!(!pair.execution_enabled);
        assert_eq!(loaded.fingerprint_sha256().len(), 64);
    }

    #[test]
    fn committed_esp_shadow_snapshot_is_public_market_data_only() {
        let loaded = load(ESP_SHADOW_CONFIG.as_bytes()).unwrap();
        let pair = &loaded.snapshot().pairs[0];

        assert_eq!(loaded.binance_symbols(), ["ESPUSDC"]);
        assert_eq!(loaded.snapshot().source.rails_pair_candidate_id, Some(3144));
        assert_eq!(pair.chain.chain_id, 42_161);
        assert!(pair.market_data_enabled);
        assert!(!pair.execution_enabled);
        assert!(!pair.rebalance.enabled);
        assert!(!loaded.snapshot().live_trading_enabled);
        assert!(pair.chain.uniswap_v3_router_address.is_none());
        assert!(pair.chain.uniswap_v4_quoter_address.is_none());
        assert!(pair.chain.uniswap_v4_router_address.is_none());
        assert!(pair.chain.uniswap_v4_pool_manager_address.is_none());
        assert!(pair.chain.uniswap_v4_state_view_address.is_none());
        assert_eq!(pair.dex.allowed_providers, [DexProvider::UniswapV3]);
        assert_eq!(pair.dex.uniswap_v3.as_ref().unwrap().fee_tiers, [100]);
        assert!(pair.dex.uniswap_v4.is_none());
    }

    #[test]
    fn committed_esp_readiness_snapshot_is_bounded_and_cannot_mutate() {
        let loaded = load(ESP_READINESS_CONFIG.as_bytes()).unwrap();
        let pair = &loaded.snapshot().pairs[0];
        let canary = pair.live_canary.as_ref().unwrap();

        assert_eq!(pair.chain.chain_id, 42_161);
        assert_eq!(
            pair.chain.uniswap_v3_router_address.as_deref(),
            Some("0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45")
        );
        assert_eq!(
            canary.approval_gate,
            LiveCanaryApprovalGate::ExplicitProductionApprovalRequired
        );
        assert_eq!(canary.max_parent_trades, 2);
        assert_eq!(canary.max_concurrent_trades, 1);
        assert!(!canary.rebalance_mutations_enabled);
        assert!(!pair.execution_enabled);
        assert!(!pair.rebalance.enabled);
        assert!(!loaded.snapshot().live_trading_enabled);
    }

    #[test]
    fn esp_readiness_artifact_rejects_execution_before_approval() {
        let mut value: Value = serde_json::from_str(ESP_READINESS_CONFIG).unwrap();
        value["pairs"][0]["execution_enabled"] = Value::Bool(true);
        assert!(load(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut value: Value = serde_json::from_str(ESP_READINESS_CONFIG).unwrap();
        value["pairs"][0]["live_canary"]["max_concurrent_trades"] = Value::from(2);
        assert!(load(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut value: Value = serde_json::from_str(ESP_READINESS_CONFIG).unwrap();
        value["pairs"][0]["live_canary"]["rebalance_mutations_enabled"] = Value::Bool(true);
        assert!(load(&serde_json::to_vec(&value).unwrap()).is_err());
    }

    #[test]
    fn committed_esp_canary_has_versioned_m10_approval_and_bounds() {
        let loaded = load(ESP_CANARY_CONFIG.as_bytes()).unwrap();
        let pair = &loaded.snapshot().pairs[0];
        let canary = pair.live_canary.as_ref().unwrap();

        assert_eq!(
            canary.approval_gate,
            LiveCanaryApprovalGate::ExplicitProductionApproved
        );
        assert_eq!(canary.minimum_wallet_token_a_base_units, "25000000");
        assert_eq!(
            canary.minimum_wallet_token_b_base_units,
            "400000000000000000000"
        );
        assert_eq!(canary.runtime_wallet_token_a_minimum(), "1");
        assert_eq!(canary.runtime_wallet_token_b_minimum(), "1");
        let prefunding = canary.prefunding_rebalance.as_ref().unwrap();
        assert_eq!(prefunding.binance_network, "ARBITRUM");
        assert_eq!(prefunding.withdrawal_api_mode, "standard");
        assert_eq!(prefunding.maximum_transfer_count, 2);
        assert_eq!(prefunding.maximum_token_a_debit_base_units, "30000000");
        assert_eq!(
            prefunding.maximum_token_b_debit_base_units,
            "500000000000000000000"
        );
        assert!(prefunding.retry_after_verified_address);
        let recovery = prefunding.approved_travel_rule_recovery.as_ref().unwrap();
        assert_eq!(recovery.rejected_token_symbol, "ESP");
        assert_eq!(recovery.rejected_http_status, 400);
        assert_eq!(recovery.rejected_error_code, -4024);
        let manual = prefunding.approved_manual_token_b_credit.as_ref().unwrap();
        assert_eq!(manual.operation_id, "rebalance-268-15f59bc55dcaed54");
        assert_eq!(manual.token_symbol, "ESP");
        assert_eq!(manual.expected_credit_base_units, "400000000000000000000");
        assert_eq!(
            manual.transaction_hash,
            "0xc65237273346c647f2e47e04ad67b81e7002eedf6da779d04a5b3c49e2fd129b"
        );
        assert_eq!(canary.arbitrum_max_fee_headroom_bps, 12_000);
        let rebalance = canary.rebalance_live_canary.as_ref().unwrap();
        assert_eq!(
            rebalance.approval_gate,
            LiveCanaryApprovalGate::ExplicitProductionApproved
        );
        assert_eq!(
            rebalance.production_approval_actor.as_deref(),
            Some("operator")
        );
        assert_eq!(
            rebalance.production_approval_recorded_at_utc.as_deref(),
            Some("2026-07-31T08:09:37Z")
        );
        assert_eq!(
            rebalance.approval_session_id,
            "esp-usdc-arbitrum-rebalance-20260731-r2"
        );
        assert_eq!(rebalance.binance_network, "ARBITRUM");
        assert_eq!(rebalance.maximum_transfer_count, 2);
        assert_eq!(rebalance.maximum_concurrent_transfers, 1);
        assert_eq!(rebalance.maximum_failed_transfers, 1);
        assert_eq!(rebalance.maximum_token_a_debit_base_units, "2600000000");
        assert_eq!(
            rebalance.maximum_token_b_debit_base_units,
            "10000000000000000000000"
        );
        assert_eq!(rebalance.maximum_token_a_fee_base_units, "5000000");
        assert_eq!(
            rebalance.maximum_token_b_fee_base_units,
            "2000000000000000000"
        );
        assert_eq!(rebalance.rollout_duration_seconds, 900);
        assert_eq!(rebalance.maximum_unknown_reconciliation_queries, 1);
        assert!(rebalance.direct_route_only);
        assert!(!rebalance.bridge_mutations_enabled);
        let endpoint_recovery = rebalance
            .approved_standard_withdrawal_recovery
            .as_ref()
            .unwrap();
        assert_eq!(
            endpoint_recovery.operation_id,
            "rebalance-324-8b62a7c14f4ef643"
        );
        assert_eq!(
            endpoint_recovery.withdraw_order_id,
            "rb8b62a7c14f4ef6434a88c384bbb83c"
        );
        assert_eq!(
            endpoint_recovery.amount_base_units,
            "4464938180550000000000"
        );
        assert_eq!(endpoint_recovery.retry_api_mode, "standard");
        assert_eq!(endpoint_recovery.capital_history_match_count, 0);
        let manual_recovery = rebalance
            .approved_manual_withdrawal_recovery
            .as_ref()
            .unwrap();
        assert_eq!(
            manual_recovery.withdrawal_id,
            "e02357b25de24e1ba9965bf524db37f7"
        );
        assert_eq!(
            manual_recovery.transaction_hash,
            "0x553d9635dab1477c6aab9a17fc4ab860040e44db8ca085cb894a6b3184bc27fd"
        );
        assert_eq!(
            manual_recovery.expected_fee_base_units,
            "1100000000000000000"
        );
        assert_eq!(
            manual_recovery.rejected_local_entity_travel_rule_id,
            67_294_348
        );
        assert_eq!(manual_recovery.rejected_standard_travel_rule_id, 67_298_920);
        assert!(canary.rebalance_mutations_enabled);
        let evm_recovery = prefunding
            .approved_evm_prebroadcast_rejection
            .as_ref()
            .unwrap();
        assert_eq!(evm_recovery.nonce, 1);
        assert_eq!(
            evm_recovery.transaction_hash,
            "0xbdfaa80920ebd8513a01d9a368f581ae8b552e8f4528be54586eeb0963079977"
        );
        let absent_withdrawal = prefunding
            .approved_absent_standard_withdrawal
            .as_ref()
            .unwrap();
        assert_eq!(
            absent_withdrawal.operation_id,
            "rebalance-296-96fd53e70c1ab390"
        );
        assert_eq!(
            absent_withdrawal.withdraw_order_id,
            "rb96fd53e70c1ab390ae3e62eb434cd1"
        );
        assert_eq!(absent_withdrawal.amount_base_units, "1197503244");
        assert_eq!(absent_withdrawal.binance_network, "OPTIMISM");
        assert_eq!(
            absent_withdrawal.master_transfer_transaction_id,
            395_924_104_268
        );
        assert_eq!(absent_withdrawal.reconciliation_queries, 1);
        assert_eq!(absent_withdrawal.rejected_http_status, 400);
        assert_eq!(absent_withdrawal.rejected_error_code, -4104);
        let absent_master_transfer = prefunding.approved_absent_master_transfer.as_ref().unwrap();
        assert_eq!(
            absent_master_transfer.operation_id,
            "rebalance-294-96fd53e70c1ab390"
        );
        assert_eq!(
            absent_master_transfer.fingerprint,
            "96fd53e70c1ab390ae3e62eb434cd19f5c5e9e1434754bbbddc34d932f0efb50"
        );
        assert_eq!(
            absent_master_transfer.withdraw_order_id,
            "rb96fd53e70c1ab390ae3e62eb434cd1"
        );
        assert_eq!(absent_master_transfer.amount_base_units, "1197503244");
        assert_eq!(absent_master_transfer.minimum_evidence_age_seconds, 300);
        assert!(pair.execution_enabled);
        assert!(pair.rebalance.enabled);

        let mut value: Value = serde_json::from_str(ESP_CANARY_CONFIG).unwrap();
        value["pairs"][0]["live_canary"]["minimum_wallet_token_a_base_units"] =
            Value::String("19999999".to_owned());
        assert!(load(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut value: Value = serde_json::from_str(ESP_CANARY_CONFIG).unwrap();
        value["pairs"][0]["live_canary"]["minimum_runtime_wallet_token_a_base_units"] =
            Value::String("0".to_owned());
        assert!(load(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut value: Value = serde_json::from_str(ESP_CANARY_CONFIG).unwrap();
        value["pairs"][0]["live_canary"]["minimum_runtime_wallet_token_b_base_units"] =
            Value::String("400000000000000000001".to_owned());
        assert!(load(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut value: Value = serde_json::from_str(ESP_CANARY_CONFIG).unwrap();
        value["pairs"][0]["live_canary"]["prefunding_rebalance"]["maximum_token_a_withdrawal_fee_base_units"] =
            Value::String("5000001".to_owned());
        assert!(load(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut value: Value = serde_json::from_str(ESP_CANARY_CONFIG).unwrap();
        value["pairs"][0]["live_canary"]["prefunding_rebalance"]["approved_manual_token_b_credit"]
            ["transaction_hash"] = Value::String("0xdeadbeef".to_owned());
        assert!(load(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut value: Value = serde_json::from_str(ESP_CANARY_CONFIG).unwrap();
        value["pairs"][0]["live_canary"]["prefunding_rebalance"]["withdrawal_api_mode"] =
            Value::String("local_entity".to_owned());
        assert!(load(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut value: Value = serde_json::from_str(ESP_CANARY_CONFIG).unwrap();
        value["pairs"][0]["live_canary"]["rebalance_live_canary"]["approved_standard_withdrawal_recovery"]
            ["withdraw_order_id"] = Value::String("rbwrong".to_owned());
        assert!(load(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut value: Value = serde_json::from_str(ESP_CANARY_CONFIG).unwrap();
        value["pairs"][0]["live_canary"]["rebalance_live_canary"]["approved_manual_withdrawal_recovery"]
            ["expected_fee_base_units"] = Value::String("1200000000000000000".to_owned());
        assert!(load(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut value: Value = serde_json::from_str(ESP_CANARY_CONFIG).unwrap();
        value["pairs"][0]["live_canary"]["rebalance_live_canary"]["approved_manual_withdrawal_recovery"]
            ["rejected_standard_travel_rule_id"] = Value::from(67_294_348);
        assert!(load(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut value: Value = serde_json::from_str(ESP_CANARY_CONFIG).unwrap();
        value["pairs"][0]["live_canary"]["rebalance_live_canary"]["approval_gate"] =
            Value::String("explicit_production_approval_required".to_owned());
        assert!(load(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut value: Value = serde_json::from_str(ESP_CANARY_CONFIG).unwrap();
        value["pairs"][0]["live_canary"]["rebalance_live_canary"]["production_approval_actor"] =
            Value::String(String::new());
        assert!(load(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut value: Value = serde_json::from_str(ESP_CANARY_CONFIG).unwrap();
        value["pairs"][0]["live_canary"]["rebalance_mutations_enabled"] = Value::Bool(false);
        assert!(load(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut value: Value = serde_json::from_str(ESP_CANARY_CONFIG).unwrap();
        value["pairs"][0]["rebalance"]["enabled"] = Value::Bool(false);
        assert!(load(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut value: Value = serde_json::from_str(ESP_CANARY_CONFIG).unwrap();
        value["pairs"][0]["live_canary"]["rebalance_live_canary"]["direct_route_only"] =
            Value::Bool(false);
        assert!(load(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut value: Value = serde_json::from_str(ESP_CANARY_CONFIG).unwrap();
        value["pairs"][0]["live_canary"]["rebalance_live_canary"]["bridge_mutations_enabled"] =
            Value::Bool(true);
        assert!(load(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut value: Value = serde_json::from_str(ESP_CANARY_CONFIG).unwrap();
        value["pairs"][0]["live_canary"]["rebalance_live_canary"]["approval_session_id"] =
            Value::String("esp-usdc-arbitrum-rebalance-unreviewed".to_owned());
        assert!(load(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut value: Value = serde_json::from_str(ESP_CANARY_CONFIG).unwrap();
        value["pairs"][0]["live_canary"]["rebalance_live_canary"]["maximum_token_b_debit_base_units"] =
            Value::String("10001000000000000000000".to_owned());
        assert!(load(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut value: Value = serde_json::from_str(ESP_CANARY_CONFIG).unwrap();
        value["pairs"][0]["live_canary"]["rebalance_live_canary"]["approval_gate"] =
            Value::String("explicit_production_approval_required".to_owned());
        value["pairs"][0]["live_canary"]["rebalance_live_canary"]
            .as_object_mut()
            .unwrap()
            .remove("production_approval_actor");
        value["pairs"][0]["live_canary"]["rebalance_live_canary"]
            .as_object_mut()
            .unwrap()
            .remove("production_approval_recorded_at_utc");
        value["pairs"][0]["live_canary"]["rebalance_mutations_enabled"] = Value::Bool(false);
        value["pairs"][0]["rebalance"]["enabled"] = Value::Bool(false);
        assert!(load(&serde_json::to_vec(&value).unwrap()).is_ok());
    }

    #[test]
    fn adaptive_sizing_is_mode_tagged_and_validated() {
        let shadow = mutate(|value| {
            value["pairs"][0]["adaptive_sizing"] = serde_json::json!({
                "mode": "shadow",
                "max_trade_notional_token_a_base_units": "200000000",
                "max_unhedged_notional_token_a_base_units": "220000000",
                "max_recovery_loss_token_a_base_units": "2000000",
                "min_bounded_profit_token_a_base_units": "0",
                "min_incremental_bounded_profit_token_a_base_units": "0"
            });
        });
        let loaded = load(&shadow).unwrap();
        assert_eq!(loaded.snapshot().pairs[0].adaptive_sizing.mode(), "shadow");

        let below_baseline = mutate(|value| {
            value["pairs"][0]["adaptive_sizing"] = serde_json::json!({
                "mode": "shadow",
                "max_trade_notional_token_a_base_units": "19999999",
                "max_unhedged_notional_token_a_base_units": "220000000",
                "max_recovery_loss_token_a_base_units": "2000000",
                "min_bounded_profit_token_a_base_units": "0",
                "min_incremental_bounded_profit_token_a_base_units": "0"
            });
        });
        assert!(load(&below_baseline).is_err());

        let unknown = mutate(|value| {
            value["pairs"][0]["adaptive_sizing"] = serde_json::json!({
                "mode": "elastic"
            });
        });
        assert!(load(&unknown).is_err());

        let active = mutate(|value| {
            value["pairs"][0]["adaptive_sizing"] = serde_json::json!({
                "mode": "adaptive",
                "max_trade_notional_token_a_base_units": "200000000",
                "max_unhedged_notional_token_a_base_units": "220000000",
                "max_recovery_loss_token_a_base_units": "2000000",
                "min_bounded_profit_token_a_base_units": "0",
                "min_incremental_bounded_profit_token_a_base_units": "0",
                "depth_policy": {
                    "recent_full_depth_max_age_ms": 750,
                    "recent_full_depth_max_update_delta": 8,
                    "top_of_book_max_trade_notional_token_a_base_units": "40000000"
                }
            });
        });
        let loaded = load(&active).unwrap();
        assert_eq!(
            loaded.snapshot().pairs[0].adaptive_sizing.mode(),
            "adaptive"
        );
        let limits = loaded.snapshot().pairs[0].adaptive_sizing.limits().unwrap();
        assert_eq!(limits.depth_policy.recent_full_depth_max_age_ms, 750);
        assert_eq!(limits.depth_policy.recent_full_depth_max_update_delta, 8);
        assert_eq!(
            limits
                .depth_policy
                .top_of_book_max_trade_notional_token_a_base_units,
            "40000000"
        );
        let mismatched_caps = mutate(|value| {
            value["pairs"][0]["adaptive_sizing"] = serde_json::json!({
                "mode": "adaptive",
                "max_trade_notional_token_a_base_units": "200000000",
                "max_unhedged_notional_token_a_base_units": "220000000",
                "max_recovery_loss_token_a_base_units": "2000000",
                "min_expected_profit_token_a_base_units": "0",
                "min_incremental_expected_profit_token_a_base_units": "0",
                "depth_policy": {
                    "recent_full_depth_max_age_ms": 750,
                    "recent_full_depth_max_update_delta": 0,
                    "top_of_book_max_trade_notional_token_a_base_units": "40000000"
                }
            });
        });
        assert!(load(&mismatched_caps).is_err());
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
            loaded.snapshot().pairs[0].adaptive_sizing.mode(),
            "adaptive"
        );
        let limits = loaded.snapshot().pairs[0].adaptive_sizing.limits().unwrap();
        assert_eq!(limits.depth_policy.recent_full_depth_max_age_ms, 750);
        assert_eq!(limits.depth_policy.recent_full_depth_max_update_delta, 8);
        assert_eq!(
            limits
                .depth_policy
                .top_of_book_max_trade_notional_token_a_base_units,
            "40000000"
        );
        assert_eq!(
            loaded.snapshot().pairs[0]
                .strategy
                .balance_safety_multiplier,
            1
        );
        assert_eq!(loaded.snapshot().pairs[0].strategy.max_quote_age_ms, 30_000);
        assert_eq!(
            loaded.snapshot().pairs[0]
                .strategy
                .max_transport_silence_ms(),
            30_000
        );
        assert_eq!(
            loaded.strategy_price_transport_silence_limits_ms(),
            std::collections::BTreeMap::from([("WLDUSDC".to_owned(), 30_000)])
        );
        assert_eq!(
            loaded.snapshot().pairs[0].binance.tick_size,
            "0.000100000000000"
        );
        assert_eq!(
            loaded.snapshot().pairs[0]
                .binance
                .commission_asset
                .as_deref(),
            Some("BNB")
        );
        assert_eq!(
            loaded.snapshot().pairs[0].binance.commission_asset_decimals,
            Some(8)
        );
        assert_eq!(
            loaded.snapshot().pairs[0]
                .binance
                .commission_price_binance_symbol
                .as_deref(),
            Some("BNBUSDT")
        );
        assert_eq!(
            loaded.fingerprint_sha256(),
            "f4f8533c6349d41a2086033582598a14ee6a47918aebb952168d4c425db91d56"
        );
    }

    #[test]
    fn v10_live_snapshot_retains_the_one_second_quote_age() {
        let loaded = load(V10_LIVE_CONFIG.as_bytes()).unwrap();
        assert_eq!(loaded.snapshot().pairs[0].strategy.max_quote_age_ms, 1_000);
        assert_eq!(
            loaded.fingerprint_sha256(),
            "19ac100b29724f7269a053aca566776168ebe5cdd919a63d50aeb7d962a404fe"
        );
    }

    #[test]
    fn v6_live_snapshot_remains_baseline_only_when_field_is_absent() {
        let loaded = load(LEGACY_LIVE_CONFIG.as_bytes()).unwrap();
        assert_eq!(
            loaded.snapshot().pairs[0].adaptive_sizing,
            AdaptiveSizingConfig::BaselineOnly
        );
    }

    #[test]
    fn v7_live_snapshot_remains_readable_as_adaptive_shadow() {
        let loaded = load(SHADOW_LIVE_CONFIG.as_bytes()).unwrap();
        assert_eq!(loaded.snapshot().pairs[0].adaptive_sizing.mode(), "shadow");
        assert_eq!(
            loaded.snapshot().pairs[0]
                .strategy
                .balance_safety_multiplier,
            3
        );
    }

    #[test]
    fn v8_live_snapshot_remains_readable_with_legacy_profit_floor_names() {
        let loaded = load(V8_LIVE_CONFIG.as_bytes()).unwrap();
        let limits = loaded.snapshot().pairs[0].adaptive_sizing.limits().unwrap();
        assert_eq!(limits.min_expected_profit, "0");
        assert_eq!(limits.min_incremental_expected_profit, "0");
    }

    #[test]
    fn v9_live_snapshot_remains_readable_with_exact_depth_only_defaults() {
        let loaded = load(V9_LIVE_CONFIG.as_bytes()).unwrap();
        let limits = loaded.snapshot().pairs[0].adaptive_sizing.limits().unwrap();
        assert_eq!(limits.depth_policy.recent_full_depth_max_age_ms, 0);
        assert_eq!(limits.depth_policy.recent_full_depth_max_update_delta, 0);
        assert_eq!(
            limits
                .depth_policy
                .top_of_book_max_trade_notional_token_a_base_units,
            "0"
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

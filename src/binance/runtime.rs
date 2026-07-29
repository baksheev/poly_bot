use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, ensure};

use crate::domain::compiled::{CompiledBinanceRuntimePlan, CompiledBinanceStreamShard};

/// Supervision topology for one Binance Spot account.
///
/// The concrete WebSocket/REST implementations remain separately testable,
/// but all authenticated and public resources are registered under this one
/// account owner. Slow capital and account-state work therefore has no route
/// into the directly-polled market-data future.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SharedBinanceRuntime {
    account_id: String,
    account_snapshot_generation: u64,
    symbols: BTreeSet<String>,
    executable_symbols: BTreeSet<String>,
    stream_shards: Vec<CompiledBinanceStreamShard>,
    owners: BTreeMap<BinanceOwnerKind, BinanceOwnerBoundary>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum BinanceOwnerKind {
    MarketData,
    AccountState,
    UserData,
    OrderExecution,
    RateLimit,
    OpenOrderReconciliation,
    CapitalSaga,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceOwnerBoundary {
    pub owner_id: String,
    pub blocking_rest_allowed: bool,
    pub credential_scope: CredentialScope,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialScope {
    Public,
    Trading,
    Treasury,
}

impl SharedBinanceRuntime {
    pub fn from_compiled(
        plan: &CompiledBinanceRuntimePlan,
        account_snapshot_generation: u64,
    ) -> anyhow::Result<Self> {
        Self::new(
            plan.account_id.as_str(),
            plan.symbols.iter().cloned(),
            plan.executable_symbols.clone(),
            plan.stream_shards.clone(),
            account_snapshot_generation,
        )
    }

    pub fn single_symbol(symbol: String, account_snapshot_generation: u64) -> anyhow::Result<Self> {
        Self::new(
            "compat-primary",
            [symbol.clone()],
            BTreeSet::from([symbol.clone()]),
            vec![CompiledBinanceStreamShard {
                id: "compat-single-symbol".to_owned(),
                symbols: vec![symbol],
            }],
            account_snapshot_generation,
        )
    }

    fn new(
        account_id: &str,
        symbols: impl IntoIterator<Item = String>,
        executable_symbols: BTreeSet<String>,
        stream_shards: Vec<CompiledBinanceStreamShard>,
        account_snapshot_generation: u64,
    ) -> anyhow::Result<Self> {
        ensure!(
            account_snapshot_generation > 0,
            "Binance account snapshot generation must be positive"
        );
        let symbols: BTreeSet<_> = symbols.into_iter().collect();
        ensure!(!symbols.is_empty(), "Binance runtime has no symbols");
        ensure!(
            executable_symbols.is_subset(&symbols),
            "Binance executable symbols are outside the account registry"
        );
        let mut sharded = BTreeSet::new();
        for shard in &stream_shards {
            ensure!(!shard.symbols.is_empty(), "Binance stream shard is empty");
            for symbol in &shard.symbols {
                ensure!(
                    symbols.contains(symbol),
                    "Binance shard {} contains unknown symbol {symbol}",
                    shard.id
                );
                ensure!(
                    sharded.insert(symbol.clone()),
                    "Binance symbol {symbol} appears in multiple stream shards"
                );
            }
        }
        ensure!(
            sharded == symbols,
            "Binance stream shards do not cover the account symbol registry"
        );

        let owner =
            |suffix: &str, blocking_rest_allowed: bool, credential_scope: CredentialScope| {
                BinanceOwnerBoundary {
                    owner_id: format!("owner:binance:{account_id}:{suffix}"),
                    blocking_rest_allowed,
                    credential_scope,
                }
            };
        let owners = BTreeMap::from([
            (
                BinanceOwnerKind::MarketData,
                owner("market-data", false, CredentialScope::Public),
            ),
            (
                BinanceOwnerKind::AccountState,
                owner("account-state", true, CredentialScope::Trading),
            ),
            (
                BinanceOwnerKind::UserData,
                owner("user-data", false, CredentialScope::Trading),
            ),
            (
                BinanceOwnerKind::OrderExecution,
                owner("order-execution", false, CredentialScope::Trading),
            ),
            (
                BinanceOwnerKind::RateLimit,
                owner("rate-limit", false, CredentialScope::Trading),
            ),
            (
                BinanceOwnerKind::OpenOrderReconciliation,
                owner("open-orders", true, CredentialScope::Trading),
            ),
            (
                BinanceOwnerKind::CapitalSaga,
                owner("capital-saga", true, CredentialScope::Treasury),
            ),
        ]);

        Ok(Self {
            account_id: account_id.to_owned(),
            account_snapshot_generation,
            symbols,
            executable_symbols,
            stream_shards,
            owners,
        })
    }

    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    pub fn account_snapshot_generation(&self) -> u64 {
        self.account_snapshot_generation
    }

    pub fn symbols(&self) -> &BTreeSet<String> {
        &self.symbols
    }

    pub fn stream_shards(&self) -> &[CompiledBinanceStreamShard] {
        &self.stream_shards
    }

    pub fn owners(&self) -> &BTreeMap<BinanceOwnerKind, BinanceOwnerBoundary> {
        &self.owners
    }

    pub fn ensure_order_enabled(&self, symbol: &str) -> anyhow::Result<()> {
        ensure!(
            self.executable_symbols.contains(symbol),
            "Binance order placement is disabled for symbol {symbol}"
        );
        Ok(())
    }

    pub fn owner(&self, kind: BinanceOwnerKind) -> anyhow::Result<&BinanceOwnerBoundary> {
        self.owners
            .get(&kind)
            .with_context(|| format!("Binance runtime owner {kind:?} is missing"))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{BinanceOwnerKind, CredentialScope, SharedBinanceRuntime};
    use crate::domain::compiled::CompiledBinanceStreamShard;

    #[test]
    fn isolates_hot_path_and_treasury_credentials() {
        let runtime = SharedBinanceRuntime::new(
            "primary",
            ["ESPUSDC".to_owned(), "WLDUSDC".to_owned()],
            BTreeSet::from(["WLDUSDC".to_owned()]),
            vec![CompiledBinanceStreamShard {
                id: "shard-0".to_owned(),
                symbols: vec!["ESPUSDC".to_owned(), "WLDUSDC".to_owned()],
            }],
            1,
        )
        .unwrap();

        assert!(
            !runtime
                .owner(BinanceOwnerKind::MarketData)
                .unwrap()
                .blocking_rest_allowed
        );
        assert_eq!(
            runtime
                .owner(BinanceOwnerKind::CapitalSaga)
                .unwrap()
                .credential_scope,
            CredentialScope::Treasury
        );
        runtime.ensure_order_enabled("WLDUSDC").unwrap();
        assert!(runtime.ensure_order_enabled("ESPUSDC").is_err());
    }

    #[test]
    fn rejects_duplicate_or_incomplete_stream_ownership() {
        let duplicate = vec![
            CompiledBinanceStreamShard {
                id: "a".to_owned(),
                symbols: vec!["WLDUSDC".to_owned()],
            },
            CompiledBinanceStreamShard {
                id: "b".to_owned(),
                symbols: vec!["WLDUSDC".to_owned(), "ESPUSDC".to_owned()],
            },
        ];
        assert!(
            SharedBinanceRuntime::new(
                "primary",
                ["ESPUSDC".to_owned(), "WLDUSDC".to_owned()],
                BTreeSet::new(),
                duplicate,
                1,
            )
            .is_err()
        );
    }
}

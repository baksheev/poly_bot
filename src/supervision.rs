use std::collections::BTreeMap;

use anyhow::{Context, ensure};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependencyScope {
    pub binance_account_id: String,
    pub network_id: String,
    pub strategy_id: String,
    pub execution_lane_id: String,
    pub execution_enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DependencyFaultClass {
    Strategy,
    PublicStream,
    NetworkIngestion,
    CriticalOwner,
    Telemetry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisorAction {
    DegradeStrategy,
    ReconnectShard,
    DegradeNetwork,
    FailFast,
    ObserveOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisionDecision {
    pub scope: DependencyScope,
    pub action: SupervisorAction,
    pub closes_new_mutations: bool,
    pub process_termination_required: bool,
}

/// Pure policy used by the runtime and by deterministic fault injection.
///
/// It deliberately owns no task handles or mutation capability. The main
/// runtime applies its decision to the corresponding owner boundary.
#[derive(Debug)]
pub struct RootSupervisorPolicy {
    scopes: BTreeMap<String, DependencyScope>,
}

impl RootSupervisorPolicy {
    pub fn new(scopes: impl IntoIterator<Item = DependencyScope>) -> anyhow::Result<Self> {
        let mut indexed = BTreeMap::new();
        for scope in scopes {
            ensure!(
                !scope.binance_account_id.is_empty()
                    && !scope.network_id.is_empty()
                    && !scope.strategy_id.is_empty()
                    && !scope.execution_lane_id.is_empty(),
                "supervisor dependency scope contains an empty identifier"
            );
            ensure!(
                indexed.insert(scope.strategy_id.clone(), scope).is_none(),
                "supervisor dependency scope repeats a strategy"
            );
        }
        ensure!(
            !indexed.is_empty(),
            "root supervisor has no dependency scopes"
        );
        Ok(Self { scopes: indexed })
    }

    pub fn decide(
        &self,
        strategy_id: &str,
        class: DependencyFaultClass,
    ) -> anyhow::Result<SupervisionDecision> {
        let scope = self
            .scopes
            .get(strategy_id)
            .cloned()
            .with_context(|| format!("fault references unknown strategy {strategy_id}"))?;
        let (action, closes_new_mutations, process_termination_required) = match class {
            DependencyFaultClass::Strategy => (
                SupervisorAction::DegradeStrategy,
                scope.execution_enabled,
                false,
            ),
            DependencyFaultClass::PublicStream => (
                SupervisorAction::ReconnectShard,
                scope.execution_enabled,
                false,
            ),
            DependencyFaultClass::NetworkIngestion => (
                SupervisorAction::DegradeNetwork,
                scope.execution_enabled,
                false,
            ),
            DependencyFaultClass::CriticalOwner => (SupervisorAction::FailFast, true, true),
            DependencyFaultClass::Telemetry => (SupervisorAction::ObserveOnly, false, false),
        };
        Ok(SupervisionDecision {
            scope,
            action,
            closes_new_mutations,
            process_termination_required,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope(strategy: &str, network: &str, execute: bool) -> DependencyScope {
        DependencyScope {
            binance_account_id: "binance-spot:primary".to_owned(),
            network_id: network.to_owned(),
            strategy_id: strategy.to_owned(),
            execution_lane_id: format!("lane:{network}"),
            execution_enabled: execute,
        }
    }

    fn policy() -> RootSupervisorPolicy {
        RootSupervisorPolicy::new([
            scope("strategy:world-chain-usdc-wld", "eip155:480", true),
            scope("strategy:arbitrum-usdc-esp", "eip155:42161", false),
        ])
        .unwrap()
    }

    #[test]
    fn esp_network_fault_degrades_only_its_non_mutating_scope() {
        let decision = policy()
            .decide(
                "strategy:arbitrum-usdc-esp",
                DependencyFaultClass::NetworkIngestion,
            )
            .unwrap();

        assert_eq!(decision.action, SupervisorAction::DegradeNetwork);
        assert!(!decision.closes_new_mutations);
        assert!(!decision.process_termination_required);
        assert_eq!(decision.scope.network_id, "eip155:42161");

        let wld = policy()
            .decide(
                "strategy:world-chain-usdc-wld",
                DependencyFaultClass::NetworkIngestion,
            )
            .unwrap();
        assert!(wld.closes_new_mutations);
        assert!(!wld.process_termination_required);
    }

    #[test]
    fn critical_owner_fault_is_fail_fast_in_every_scope() {
        for strategy in [
            "strategy:world-chain-usdc-wld",
            "strategy:arbitrum-usdc-esp",
        ] {
            let decision = policy()
                .decide(strategy, DependencyFaultClass::CriticalOwner)
                .unwrap();
            assert_eq!(decision.action, SupervisorAction::FailFast);
            assert!(decision.closes_new_mutations);
            assert!(decision.process_termination_required);
        }
    }

    #[test]
    fn telemetry_fault_never_changes_readiness_or_mutation_state() {
        let decision = policy()
            .decide(
                "strategy:world-chain-usdc-wld",
                DependencyFaultClass::Telemetry,
            )
            .unwrap();
        assert_eq!(decision.action, SupervisorAction::ObserveOnly);
        assert!(!decision.closes_new_mutations);
        assert!(!decision.process_termination_required);
    }
}

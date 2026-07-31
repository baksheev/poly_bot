use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    ops::{Deref, DerefMut},
    time::Instant,
};

use alloy_primitives::U256;
use anyhow::{Context, ensure};

use crate::{
    binance::depth::SpotDepthBook,
    dex::mirror::{DexMirror, LogApplyResult},
    domain::compiled::{CompiledHotPathRuntimePlan, PoolId, StrategyId},
    hot_telemetry::HotTelemetryHandle,
    market_data::{MarketEvent, alchemy::DexStreamEvent},
    opportunity::{ArbitrageDirection, OpportunityEngine, PairEvaluation, TradeEvaluation},
    state::{QuoteApplyResult, RuntimeState, TopOfBook},
    telemetry::TelemetryHandle,
};

/// Immutable dependency lookup compiled once before any socket is started.
///
/// Event routing is exact: a Binance symbol or DEX pool can reach only the
/// strategies that declared it in the authoritative compiled graph.
#[derive(Debug)]
pub struct CompiledStrategyDependencyIndex {
    plan: CompiledHotPathRuntimePlan,
    by_symbol: HashMap<String, Vec<usize>>,
    by_pool: BTreeMap<PoolId, Vec<usize>>,
    by_strategy: BTreeMap<StrategyId, usize>,
}

impl CompiledStrategyDependencyIndex {
    pub fn new(plan: CompiledHotPathRuntimePlan) -> anyhow::Result<Self> {
        ensure!(
            !plan.strategies.is_empty(),
            "hot-path strategy plan is empty"
        );
        let mut by_symbol: HashMap<String, Vec<usize>> = HashMap::new();
        let mut by_pool: BTreeMap<PoolId, Vec<usize>> = BTreeMap::new();
        let mut by_strategy = BTreeMap::new();
        for (index, strategy) in plan.strategies.iter().enumerate() {
            ensure!(
                by_strategy
                    .insert(strategy.strategy_id.clone(), index)
                    .is_none(),
                "duplicate hot-path strategy {}",
                strategy.strategy_id.as_str()
            );
            by_symbol
                .entry(strategy.symbol.clone())
                .or_default()
                .push(index);
            for pool_id in &strategy.pool_ids {
                by_pool.entry(pool_id.clone()).or_default().push(index);
            }
        }
        for indices in by_symbol.values_mut().chain(by_pool.values_mut()) {
            indices.sort_unstable_by_key(|index| {
                plan.strategies[*index].strategy_id.as_str().to_owned()
            });
            indices.dedup();
        }
        Ok(Self {
            plan,
            by_symbol,
            by_pool,
            by_strategy,
        })
    }

    pub fn plan(&self) -> &CompiledHotPathRuntimePlan {
        &self.plan
    }

    pub fn strategy(
        &self,
        strategy_id: &StrategyId,
    ) -> anyhow::Result<&crate::domain::compiled::CompiledHotPathStrategyPlan> {
        self.by_strategy
            .get(strategy_id)
            .and_then(|index| self.plan.strategies.get(*index))
            .with_context(|| format!("unknown hot-path strategy {}", strategy_id.as_str()))
    }

    pub fn for_symbol(&self, symbol: &str) -> impl Iterator<Item = usize> + '_ {
        self.by_symbol
            .get(symbol)
            .into_iter()
            .flat_map(|indices| indices.iter().copied())
    }

    pub fn symbol_indices(&self, symbol: &str) -> &[usize] {
        self.by_symbol.get(symbol).map_or(&[], Vec::as_slice)
    }

    pub fn for_pool(&self, pool_id: &PoolId) -> impl Iterator<Item = usize> + '_ {
        self.by_pool
            .get(pool_id)
            .into_iter()
            .flat_map(|indices| indices.iter().copied())
    }

    pub fn strategy_at(
        &self,
        index: usize,
    ) -> anyhow::Result<&crate::domain::compiled::CompiledHotPathStrategyPlan> {
        self.plan
            .strategies
            .get(index)
            .context("hot-path strategy index is invalid")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategySnapshotStamp {
    pub strategy_id: StrategyId,
    pub connection_generation: u64,
    pub update_id: u64,
    pub pool_generations: Vec<(PoolId, u64)>,
}

impl StrategySnapshotStamp {
    pub fn is_current(
        &self,
        connection_generation: u64,
        update_id: u64,
        pool_generations: &BTreeMap<PoolId, u64>,
    ) -> bool {
        self.connection_generation == connection_generation
            && self.update_id == update_id
            && self
                .pool_generations
                .iter()
                .all(|(pool_id, generation)| pool_generations.get(pool_id) == Some(generation))
    }
}

#[derive(Debug)]
pub struct StrategyEvaluation {
    pub evaluated: bool,
    pub candidate_produced: bool,
    pub calculation_time_us: u128,
    pub budget_us: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StrategyDispatchSummary {
    pub routed_strategies: u16,
    pub evaluated_strategies: u16,
    pub produced_candidates: u16,
    pub budget_exceeded: u16,
    pub maximum_calculation_time_us: u128,
}

#[derive(Debug)]
pub struct StrategyDependencyFault {
    pub strategy_id: StrategyId,
    pub network_id: String,
    pub symbol: String,
    pub dependency: &'static str,
    pub error: anyhow::Error,
}

impl StrategyEvaluation {
    pub fn budget_exceeded(&self) -> bool {
        self.calculation_time_us > u128::from(self.budget_us)
    }
}

/// Synchronous baseline interface. Implementations may create an immutable
/// snapshot for exhaustive sizing, but exhaustive work itself must not run in
/// this call.
pub trait StrategyEvaluator {
    fn strategy_id(&self) -> StrategyId;
    fn symbol(&self) -> &str;
    fn on_market_event(
        &mut self,
        event: MarketEvent,
        depth: Option<&SpotDepthBook>,
    ) -> anyhow::Result<StrategyEvaluation>;

    fn on_dex_event(&mut self, _event: DexStreamEvent) -> anyhow::Result<StrategyEvaluation> {
        anyhow::bail!(
            "strategy {} does not accept DEX events through the shadow boundary",
            self.strategy_id().as_str()
        )
    }

    fn on_startup_dex_event(
        &mut self,
        event: DexStreamEvent,
    ) -> anyhow::Result<StrategyEvaluation> {
        self.on_dex_event(event)
    }

    fn take_sizing_job(&mut self) -> Option<ShadowSizingJob> {
        None
    }

    fn on_sizing_result(
        &mut self,
        _result: ShadowSizingTaskResult,
    ) -> anyhow::Result<ShadowSizingDisposition> {
        anyhow::bail!(
            "strategy {} does not accept shadow sizing results",
            self.strategy_id().as_str()
        )
    }
}

/// Owns the executable compatibility evaluator and every non-mutating shadow
/// evaluator. It is deliberately not a task and has no input channel: the
/// socket-owning Tokio task calls it synchronously.
pub struct HotPathDecisionOwner<P> {
    primary_strategy_id: StrategyId,
    primary: P,
    shadows: BTreeMap<StrategyId, Box<dyn StrategyEvaluator + Send>>,
    degraded_shadows: BTreeSet<StrategyId>,
    dependency_faults: Vec<StrategyDependencyFault>,
    dependencies: CompiledStrategyDependencyIndex,
}

impl<P: StrategyEvaluator> HotPathDecisionOwner<P> {
    pub fn new(
        primary: P,
        shadows: Vec<Box<dyn StrategyEvaluator + Send>>,
        dependencies: CompiledStrategyDependencyIndex,
    ) -> anyhow::Result<Self> {
        let primary_strategy_id = primary.strategy_id();
        let primary_plan = dependencies.strategy(&primary_strategy_id)?;
        ensure!(
            primary_plan.execute,
            "primary hot-path strategy {} is not executable",
            primary_strategy_id.as_str()
        );
        ensure!(
            primary.symbol() == primary_plan.symbol,
            "primary evaluator symbol differs from compiled dependency"
        );
        let mut indexed_shadows = BTreeMap::new();
        for shadow in shadows {
            let strategy_id = shadow.strategy_id();
            let plan = dependencies.strategy(&strategy_id)?;
            ensure!(
                !plan.execute,
                "shadow strategy {} unexpectedly has execute capability",
                strategy_id.as_str()
            );
            ensure!(
                shadow.symbol() == plan.symbol,
                "shadow evaluator symbol differs from compiled dependency"
            );
            ensure!(
                indexed_shadows
                    .insert(strategy_id.clone(), shadow)
                    .is_none(),
                "duplicate shadow evaluator {}",
                strategy_id.as_str()
            );
        }
        ensure!(
            dependencies
                .plan()
                .strategies
                .iter()
                .filter(|strategy| strategy.observe && !strategy.execute)
                .all(|strategy| { indexed_shadows.contains_key(&strategy.strategy_id) }),
            "one or more non-executable observable strategies have no hot-path evaluator"
        );
        Ok(Self {
            primary_strategy_id,
            primary,
            shadows: indexed_shadows,
            degraded_shadows: BTreeSet::new(),
            dependency_faults: Vec::new(),
            dependencies,
        })
    }

    pub fn on_market_event(
        &mut self,
        event: MarketEvent,
        depth: Option<&SpotDepthBook>,
    ) -> anyhow::Result<StrategyDispatchSummary> {
        let symbol = market_event_symbol(&event);
        let mut summary = StrategyDispatchSummary::default();
        let Self {
            primary_strategy_id,
            primary,
            shadows,
            degraded_shadows,
            dependency_faults,
            dependencies,
        } = self;
        let indices = dependencies.symbol_indices(symbol);
        let mut event = Some(event);
        for (position, &index) in indices.iter().enumerate() {
            let strategy = dependencies.strategy_at(index)?;
            let routed_event = if position + 1 == indices.len() {
                event
                    .take()
                    .expect("last hot-path route must own the market event")
            } else {
                event
                    .as_ref()
                    .expect("shared hot-path route must retain the market event")
                    .clone()
            };
            let evaluation = if strategy.strategy_id == *primary_strategy_id {
                primary.on_market_event(routed_event, depth)?
            } else if degraded_shadows.contains(&strategy.strategy_id) {
                continue;
            } else {
                let evaluator = shadows
                    .get_mut(&strategy.strategy_id)
                    .context("compiled shadow route has no evaluator")?;
                match evaluator.on_market_event(routed_event, None) {
                    Ok(evaluation) => evaluation,
                    Err(error) => {
                        degraded_shadows.insert(strategy.strategy_id.clone());
                        dependency_faults.push(StrategyDependencyFault {
                            strategy_id: strategy.strategy_id.clone(),
                            network_id: strategy.network_id.as_str().to_owned(),
                            symbol: strategy.symbol.clone(),
                            dependency: "strategy_evaluator",
                            error,
                        });
                        continue;
                    }
                }
            };
            summary.routed_strategies = summary.routed_strategies.saturating_add(1);
            summary.evaluated_strategies = summary
                .evaluated_strategies
                .saturating_add(u16::from(evaluation.evaluated));
            summary.produced_candidates = summary
                .produced_candidates
                .saturating_add(u16::from(evaluation.candidate_produced));
            summary.budget_exceeded = summary
                .budget_exceeded
                .saturating_add(u16::from(evaluation.budget_exceeded()));
            summary.maximum_calculation_time_us = summary
                .maximum_calculation_time_us
                .max(evaluation.calculation_time_us);
        }
        Ok(summary)
    }

    pub fn dependencies(&self) -> &CompiledStrategyDependencyIndex {
        &self.dependencies
    }

    pub fn on_shadow_dex_event(
        &mut self,
        strategy_id: &StrategyId,
        event: DexStreamEvent,
    ) -> anyhow::Result<StrategyEvaluation> {
        ensure!(
            strategy_id != &self.primary_strategy_id,
            "primary DEX events must use the existing compatibility coordinator path"
        );
        if self.degraded_shadows.contains(strategy_id) {
            return Ok(idle_strategy_evaluation());
        }
        match self
            .shadows
            .get_mut(strategy_id)
            .context("shadow DEX route has no evaluator")?
            .on_dex_event(event)
        {
            Ok(evaluation) => Ok(evaluation),
            Err(error) => {
                self.degrade_shadow_strategy(strategy_id, "network_ingestion", error)?;
                Ok(idle_strategy_evaluation())
            }
        }
    }

    pub fn on_shadow_startup_dex_event(
        &mut self,
        strategy_id: &StrategyId,
        event: DexStreamEvent,
    ) -> anyhow::Result<StrategyEvaluation> {
        ensure!(
            strategy_id != &self.primary_strategy_id,
            "primary DEX events must use the existing compatibility coordinator path"
        );
        if self.degraded_shadows.contains(strategy_id) {
            return Ok(idle_strategy_evaluation());
        }
        match self
            .shadows
            .get_mut(strategy_id)
            .context("shadow DEX route has no evaluator")?
            .on_startup_dex_event(event)
        {
            Ok(evaluation) => Ok(evaluation),
            Err(error) => {
                self.degrade_shadow_strategy(strategy_id, "startup_network_ingestion", error)?;
                Ok(idle_strategy_evaluation())
            }
        }
    }

    pub fn degrade_shadow_strategy(
        &mut self,
        strategy_id: &StrategyId,
        dependency: &'static str,
        error: anyhow::Error,
    ) -> anyhow::Result<()> {
        ensure!(
            strategy_id != &self.primary_strategy_id,
            "primary strategy cannot be dependency-degraded"
        );
        let plan = self.dependencies.strategy(strategy_id)?;
        if self.degraded_shadows.insert(strategy_id.clone()) {
            self.dependency_faults.push(StrategyDependencyFault {
                strategy_id: strategy_id.clone(),
                network_id: plan.network_id.as_str().to_owned(),
                symbol: plan.symbol.clone(),
                dependency,
                error,
            });
        }
        Ok(())
    }

    pub fn take_dependency_faults(&mut self) -> Vec<StrategyDependencyFault> {
        std::mem::take(&mut self.dependency_faults)
    }

    pub fn shadow_is_degraded(&self, strategy_id: &StrategyId) -> bool {
        self.degraded_shadows.contains(strategy_id)
    }

    pub fn take_next_shadow_sizing_job(&mut self) -> Option<ShadowSizingJob> {
        self.shadows
            .values_mut()
            .find_map(|evaluator| evaluator.take_sizing_job())
    }

    pub fn on_shadow_sizing_result(
        &mut self,
        result: ShadowSizingTaskResult,
    ) -> anyhow::Result<ShadowSizingDisposition> {
        let strategy_id = result.strategy_id().clone();
        self.shadows
            .get_mut(&strategy_id)
            .context("shadow sizing result has no evaluator")?
            .on_sizing_result(result)
    }
}

fn idle_strategy_evaluation() -> StrategyEvaluation {
    StrategyEvaluation {
        evaluated: false,
        candidate_produced: false,
        calculation_time_us: 0,
        budget_us: 0,
    }
}

impl<P> Deref for HotPathDecisionOwner<P> {
    type Target = P;

    fn deref(&self) -> &Self::Target {
        &self.primary
    }
}

impl<P> DerefMut for HotPathDecisionOwner<P> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.primary
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShadowCandidate {
    pub source: &'static str,
    pub direction: ArbitrageDirection,
    pub trade: TradeEvaluation,
    pub connection_generation: u64,
    pub update_id: u64,
    pub pool_generation: u64,
}

/// A shadow sink is intentionally observation-only. It has no order, wallet,
/// reservation, nonce, or coordinator command handle.
pub trait CoordinatorShadowSink: Send {
    fn publish(&self, strategy_id: &StrategyId, candidate: ShadowCandidate);
}

pub struct TelemetryCoordinatorShadowSink {
    telemetry: TelemetryHandle,
    engine_id: String,
    binance_account_id: String,
    network_id: String,
    execution_lane_id: String,
}

impl TelemetryCoordinatorShadowSink {
    pub fn new(
        telemetry: TelemetryHandle,
        engine_id: String,
        binance_account_id: String,
        network_id: String,
        execution_lane_id: String,
    ) -> Self {
        Self {
            telemetry,
            engine_id,
            binance_account_id,
            network_id,
            execution_lane_id,
        }
    }
}

impl CoordinatorShadowSink for TelemetryCoordinatorShadowSink {
    fn publish(&self, strategy_id: &StrategyId, candidate: ShadowCandidate) {
        self.telemetry.emit(
            "coordinator_shadow_candidate",
            serde_json::json!({
                "engine_id": self.engine_id,
                "strategy_id": strategy_id.as_str(),
                "binance_account_id": self.binance_account_id,
                "network_id": self.network_id,
                "execution_lane_id": self.execution_lane_id,
                "sink_mode": "non_mutating",
                "candidate_source": candidate.source,
                "external_mutation_authorized": false,
                "direction": candidate.direction.as_str(),
                "pool_index": candidate.trade.pool_index,
                "connection_generation": candidate.connection_generation,
                "update_id": candidate.update_id,
                "pool_generation": candidate.pool_generation,
                "token_b_base_units": candidate.trade.token_b_amount.to_string(),
                "dex_amount_in": candidate.trade.dex_amount_in.to_string(),
                "dex_amount_out_minimum": candidate.trade.dex_amount_out_minimum.to_string(),
                "gross_profit_bps_x100": candidate.trade.gross_profit_bps_x100,
            }),
        );
        let (wallet_debit_asset, wallet_debit_amount, binance_debit_asset, binance_debit_amount) =
            match candidate.direction {
                ArbitrageDirection::BuyTokenBOnDexSellOnCex => (
                    "USDC",
                    candidate.trade.dex_amount_in,
                    "ESP",
                    candidate.trade.token_b_amount,
                ),
                ArbitrageDirection::BuyTokenBOnCexSellOnDex => (
                    "ESP",
                    candidate.trade.dex_amount_in,
                    "USDC",
                    candidate.trade.cex_token_a_amount,
                ),
            };
        self.telemetry.emit(
            "shadow_reservation_plan",
            serde_json::json!({
                "engine_id": self.engine_id,
                "strategy_id": strategy_id.as_str(),
                "binance_account_id": self.binance_account_id,
                "network_id": self.network_id,
                "execution_lane_id": self.execution_lane_id,
                "source": candidate.source,
                "claim_count": 2,
                "wallet_debit_asset": wallet_debit_asset,
                "wallet_debit_base_units": wallet_debit_amount.to_string(),
                "binance_debit_asset": binance_debit_asset,
                "binance_debit_base_units": binance_debit_amount.to_string(),
                "reservation_mode": "pure_shadow_proposal",
                "reservation_created": false,
                "external_mutation_authorized": false,
            }),
        );
        self.telemetry.emit(
            "shadow_rebalance_plan",
            serde_json::json!({
                "engine_id": self.engine_id,
                "strategy_id": strategy_id.as_str(),
                "binance_account_id": self.binance_account_id,
                "network_id": self.network_id,
                "execution_lane_id": self.execution_lane_id,
                "candidate_direction": candidate.direction.as_str(),
                "planning_trigger": "post_trade_authoritative_balance_generation",
                "route_validation": "network_scoped_shadow_only",
                "plan_materialized": false,
                "execution_enabled": false,
                "external_mutation_authorized": false,
            }),
        );
    }
}

/// Read-only evaluator used to bring an observed strategy into the primary
/// process. It owns its local mirror and prepared curves, receives market data
/// synchronously from the socket owner, and can publish only to a shadow sink.
pub struct ShadowStrategyEvaluator {
    strategy_id: StrategyId,
    symbol: String,
    baseline_budget_us: u64,
    max_transport_silence_ms: u64,
    dex_head_max_age_ms: u64,
    runtime: RuntimeState,
    mirror: DexMirror,
    opportunities: OpportunityEngine,
    hot_telemetry: HotTelemetryHandle,
    sink: Box<dyn CoordinatorShadowSink>,
    latest_quote: Option<TopOfBook>,
    pending_sizing: Option<ShadowSizingJob>,
}

impl ShadowStrategyEvaluator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        strategy_id: StrategyId,
        symbol: String,
        baseline_budget_us: u64,
        max_transport_silence_ms: u64,
        dex_head_max_age_ms: u64,
        mirror: DexMirror,
        opportunities: OpportunityEngine,
        hot_telemetry: HotTelemetryHandle,
        sink: Box<dyn CoordinatorShadowSink>,
    ) -> Self {
        Self {
            strategy_id,
            runtime: RuntimeState::new([std::sync::Arc::from(symbol.as_str())]),
            symbol,
            baseline_budget_us,
            max_transport_silence_ms,
            dex_head_max_age_ms,
            mirror,
            opportunities,
            hot_telemetry,
            sink,
            latest_quote: None,
            pending_sizing: None,
        }
    }

    pub fn on_dex_event(&mut self, event: DexStreamEvent) -> anyhow::Result<StrategyEvaluation> {
        self.apply_dex_event(event, true)
    }

    pub fn on_startup_dex_event(
        &mut self,
        event: DexStreamEvent,
    ) -> anyhow::Result<StrategyEvaluation> {
        self.apply_dex_event(event, false)
    }

    fn apply_dex_event(
        &mut self,
        event: DexStreamEvent,
        emit_hot_path_latency: bool,
    ) -> anyhow::Result<StrategyEvaluation> {
        let mut changed = false;
        match event {
            DexStreamEvent::Log { log, received_at } => {
                if let LogApplyResult::Applied { pool_index, kind } = self.mirror.apply_log(&log)? {
                    let request = self
                        .opportunities
                        .request_pool_refresh(pool_index, &self.mirror)?;
                    if emit_hot_path_latency {
                        self.hot_telemetry.emit_dex_pool_event(
                            pool_index,
                            kind,
                            log.block_number,
                            log.transaction_index,
                            log.log_index,
                            received_at.elapsed().as_micros(),
                            request.generation(),
                        );
                    }
                    let timing = request.timing_handle();
                    timing.mark_request_dispatch_started();
                    timing.mark_request_dispatch_finished();
                    let result = request.build()?;
                    let result_timing = result.timing_handle();
                    result_timing.mark_result_send_started();
                    result_timing.mark_result_send_finished();
                    result.mark_owner_received();
                    if let Some(prepared) = self.opportunities.finish_pool_refresh(result)? {
                        self.hot_telemetry.emit_dex_pool_prepared(prepared);
                        changed = true;
                    }
                }
            }
            DexStreamEvent::Head { head, received_at } => {
                if self.mirror.apply_head(head, received_at)? && emit_hot_path_latency {
                    self.hot_telemetry
                        .emit_dex_head(head.number, received_at.elapsed().as_micros());
                }
            }
        }
        let now = Instant::now();
        self.runtime.refresh_phase(
            now,
            self.max_transport_silence_ms,
            self.mirror.is_fresh(now, self.dex_head_max_age_ms),
        );
        if changed && let Some(quote) = self.latest_quote.clone() {
            return self.evaluate_quote(&quote, "dex_prepared");
        }
        Ok(StrategyEvaluation {
            evaluated: false,
            candidate_produced: false,
            calculation_time_us: 0,
            budget_us: self.baseline_budget_us,
        })
    }

    pub fn mirror(&self) -> &DexMirror {
        &self.mirror
    }

    pub fn take_sizing_job(&mut self) -> Option<ShadowSizingJob> {
        self.pending_sizing.take()
    }

    pub fn on_sizing_result(
        &mut self,
        result: ShadowSizingTaskResult,
    ) -> anyhow::Result<ShadowSizingDisposition> {
        let quote_is_current = self.latest_quote.as_ref().is_some_and(|quote| {
            quote.connection_generation == result.connection_generation
                && quote.update_id == result.update_id
        });
        let pools_are_current = result
            .pool_generations
            .iter()
            .all(|(pool_index, generation)| {
                self.opportunities
                    .pool_generation(*pool_index)
                    .is_ok_and(|current| current == *generation)
            });
        if !quote_is_current || !pools_are_current {
            return Ok(ShadowSizingDisposition::Superseded);
        }
        if let Some(candidate) = result.candidate? {
            self.sink.publish(&self.strategy_id, candidate);
            Ok(ShadowSizingDisposition::Published)
        } else {
            Ok(ShadowSizingDisposition::NoCandidate)
        }
    }

    fn evaluate_quote(
        &mut self,
        quote: &TopOfBook,
        trigger: &'static str,
    ) -> anyhow::Result<StrategyEvaluation> {
        let started_at = Instant::now();
        let evaluation = self.opportunities.evaluate(quote)?;
        let calculation_time_us = started_at.elapsed().as_micros();
        let mut candidate_produced = false;
        if let Some(evaluation) = evaluation {
            self.hot_telemetry.emit_evaluation(
                quote,
                evaluation,
                self.mirror.latest_head().number,
                calculation_time_us,
                self.baseline_budget_us,
                trigger,
            );
            if let Some(candidate) = best_shadow_candidate(
                &self.opportunities,
                evaluation,
                quote.connection_generation,
                quote.update_id,
            )? {
                self.sink.publish(&self.strategy_id, candidate);
                candidate_produced = true;
            }
            self.pending_sizing = Some(ShadowSizingJob {
                strategy_id: self.strategy_id.clone(),
                opportunities: self.opportunities.clone(),
                quote: quote.clone(),
                evaluation,
                pool_generations: self.opportunities.pool_generations().collect(),
                queued_at: Instant::now(),
            });
        }
        Ok(StrategyEvaluation {
            evaluated: evaluation.is_some(),
            candidate_produced,
            calculation_time_us,
            budget_us: self.baseline_budget_us,
        })
    }
}

impl StrategyEvaluator for ShadowStrategyEvaluator {
    fn strategy_id(&self) -> StrategyId {
        self.strategy_id.clone()
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn on_market_event(
        &mut self,
        event: MarketEvent,
        _depth: Option<&SpotDepthBook>,
    ) -> anyhow::Result<StrategyEvaluation> {
        let idle = || StrategyEvaluation {
            evaluated: false,
            candidate_produced: false,
            calculation_time_us: 0,
            budget_us: self.baseline_budget_us,
        };
        match event {
            MarketEvent::FeedConnected {
                symbol,
                generation,
                observed_at,
            } => {
                self.runtime.on_connected(&symbol, generation, observed_at);
                Ok(idle())
            }
            MarketEvent::FeedDisconnected {
                symbol, generation, ..
            } => {
                self.runtime.on_disconnected(&symbol, generation);
                self.latest_quote = None;
                Ok(idle())
            }
            MarketEvent::FeedHeartbeat {
                symbol,
                generation,
                observed_at,
            } => {
                self.runtime
                    .record_transport_activity(&symbol, generation, observed_at);
                Ok(idle())
            }
            MarketEvent::BinanceTopOfBook(quote) => {
                let accepted =
                    self.runtime.apply_quote(quote.clone()) == QuoteApplyResult::Accepted;
                let now = Instant::now();
                let phase = self.runtime.refresh_phase(
                    now,
                    self.max_transport_silence_ms,
                    self.mirror.is_fresh(now, self.dex_head_max_age_ms),
                );
                let evaluation = if accepted {
                    self.latest_quote = Some(quote.clone());
                    self.evaluate_quote(&quote, "binance")?
                } else {
                    idle()
                };
                self.hot_telemetry.emit_binance_book(
                    &quote,
                    "strategy",
                    Some(phase),
                    if accepted { "evaluated" } else { "rejected" },
                );
                Ok(evaluation)
            }
            MarketEvent::BinanceDepthApplied { .. } => {
                anyhow::bail!("shadow strategy unexpectedly received Binance depth")
            }
        }
    }

    fn on_dex_event(&mut self, event: DexStreamEvent) -> anyhow::Result<StrategyEvaluation> {
        ShadowStrategyEvaluator::on_dex_event(self, event)
    }

    fn on_startup_dex_event(
        &mut self,
        event: DexStreamEvent,
    ) -> anyhow::Result<StrategyEvaluation> {
        ShadowStrategyEvaluator::on_startup_dex_event(self, event)
    }

    fn take_sizing_job(&mut self) -> Option<ShadowSizingJob> {
        ShadowStrategyEvaluator::take_sizing_job(self)
    }

    fn on_sizing_result(
        &mut self,
        result: ShadowSizingTaskResult,
    ) -> anyhow::Result<ShadowSizingDisposition> {
        ShadowStrategyEvaluator::on_sizing_result(self, result)
    }
}

fn best_shadow_candidate(
    opportunities: &OpportunityEngine,
    evaluation: PairEvaluation,
    connection_generation: u64,
    update_id: u64,
) -> anyhow::Result<Option<ShadowCandidate>> {
    let candidates = [
        (
            evaluation.dex_buy_cex_sell.direction,
            evaluation.dex_buy_cex_sell.baseline,
        ),
        (
            evaluation.cex_buy_dex_sell.direction,
            evaluation.cex_buy_dex_sell.baseline,
        ),
    ];
    let best = candidates
        .into_iter()
        .filter_map(|(direction, trade)| trade.map(|trade| (direction, trade)))
        .filter(|(_, trade)| trade.meets_threshold)
        .max_by(|left, right| {
            left.1
                .absolute_profit_token_a()
                .cmp(&right.1.absolute_profit_token_a())
                .then_with(|| right.1.pool_index.cmp(&left.1.pool_index))
        });
    let Some((direction, trade)) = best else {
        return Ok(None);
    };
    Ok(Some(ShadowCandidate {
        source: "baseline",
        direction,
        trade,
        connection_generation,
        update_id,
        pool_generation: opportunities.pool_generation(trade.pool_index)?,
    }))
}

const MAX_SHADOW_EXACT_EVALUATIONS: u16 = 128;

pub struct ShadowSizingJob {
    strategy_id: StrategyId,
    opportunities: OpportunityEngine,
    quote: TopOfBook,
    evaluation: PairEvaluation,
    pool_generations: Vec<(usize, u64)>,
    queued_at: Instant,
}

pub struct ShadowSizingTaskResult {
    strategy_id: StrategyId,
    connection_generation: u64,
    update_id: u64,
    pool_generations: Vec<(usize, u64)>,
    candidate: anyhow::Result<Option<ShadowCandidate>>,
    queued_at: Instant,
    started_at: Instant,
    finished_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShadowSizingDisposition {
    Published,
    NoCandidate,
    Superseded,
}

impl ShadowSizingDisposition {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Published => "published",
            Self::NoCandidate => "no_candidate",
            Self::Superseded => "superseded",
        }
    }
}

impl ShadowSizingJob {
    pub fn strategy_id(&self) -> &StrategyId {
        &self.strategy_id
    }

    pub fn run(self) -> ShadowSizingTaskResult {
        let started_at = Instant::now();
        let candidate = evaluate_shadow_sizing(
            &self.opportunities,
            &self.quote,
            self.evaluation,
            &self.pool_generations,
        );
        ShadowSizingTaskResult {
            strategy_id: self.strategy_id,
            connection_generation: self.quote.connection_generation,
            update_id: self.quote.update_id,
            pool_generations: self.pool_generations,
            candidate,
            queued_at: self.queued_at,
            started_at,
            finished_at: Instant::now(),
        }
    }
}

impl ShadowSizingTaskResult {
    pub fn strategy_id(&self) -> &StrategyId {
        &self.strategy_id
    }

    pub fn queue_time_us(&self) -> u128 {
        self.started_at
            .saturating_duration_since(self.queued_at)
            .as_micros()
    }

    pub fn worker_time_us(&self) -> u128 {
        self.finished_at
            .saturating_duration_since(self.started_at)
            .as_micros()
    }
}

pub(crate) fn evaluate_shadow_sizing(
    opportunities: &OpportunityEngine,
    quote: &TopOfBook,
    evaluation: PairEvaluation,
    pool_generations: &[(usize, u64)],
) -> anyhow::Result<Option<ShadowCandidate>> {
    let pair = opportunities.pair(evaluation.pair_index)?;
    let step = pair.token_b_step();
    let mut evaluations = 0_u16;
    let mut best: Option<(ArbitrageDirection, TradeEvaluation, u64)> = None;
    for direction in [
        ArbitrageDirection::BuyTokenBOnDexSellOnCex,
        ArbitrageDirection::BuyTokenBOnCexSellOnDex,
    ] {
        for &pool_index in pair.pool_indices() {
            let Some(capacity) = opportunities.exact_candidate_capacity(
                evaluation.pair_index,
                direction,
                pool_index,
            )?
            else {
                continue;
            };
            let maximum_steps = capacity / step;
            if maximum_steps.is_zero() {
                continue;
            }
            let mut low = U256::ZERO;
            let mut high = maximum_steps;
            let mut direction_best = None;
            while low < high && evaluations < MAX_SHADOW_EXACT_EVALUATIONS {
                let midpoint = (low + high + U256::ONE) / U256::from(2_u8);
                let amount = midpoint * step;
                evaluations = evaluations.saturating_add(1);
                let trade = opportunities.evaluate_exact_candidate(
                    evaluation.pair_index,
                    quote,
                    direction,
                    pool_index,
                    amount,
                )?;
                if trade.is_some_and(|trade| trade.meets_threshold) {
                    low = midpoint;
                    direction_best = trade;
                } else {
                    high = midpoint.saturating_sub(U256::ONE);
                }
            }
            if direction_best.is_none()
                && !low.is_zero()
                && evaluations < MAX_SHADOW_EXACT_EVALUATIONS
            {
                let amount = low * step;
                evaluations = evaluations.saturating_add(1);
                direction_best = opportunities
                    .evaluate_exact_candidate(
                        evaluation.pair_index,
                        quote,
                        direction,
                        pool_index,
                        amount,
                    )?
                    .filter(|trade| trade.meets_threshold);
            }
            if let Some(trade) = direction_best {
                let generation = pool_generations
                    .iter()
                    .find_map(|(candidate_pool, generation)| {
                        (*candidate_pool == pool_index).then_some(*generation)
                    })
                    .context("shadow sizing snapshot omitted candidate pool generation")?;
                let replace = best.as_ref().is_none_or(|(_, current, _)| {
                    trade.token_b_amount > current.token_b_amount
                        || (trade.token_b_amount == current.token_b_amount
                            && trade.absolute_profit_token_a() > current.absolute_profit_token_a())
                });
                if replace {
                    best = Some((direction, trade, generation));
                }
            }
            if evaluations >= MAX_SHADOW_EXACT_EVALUATIONS {
                break;
            }
        }
    }
    Ok(
        best.map(|(direction, trade, pool_generation)| ShadowCandidate {
            source: "exhaustive_sizing",
            direction,
            trade,
            connection_generation: quote.connection_generation,
            update_id: quote.update_id,
            pool_generation,
        }),
    )
}

fn market_event_symbol(event: &MarketEvent) -> &str {
    match event {
        MarketEvent::FeedConnected { symbol, .. }
        | MarketEvent::FeedDisconnected { symbol, .. }
        | MarketEvent::FeedHeartbeat { symbol, .. }
        | MarketEvent::BinanceDepthApplied { symbol, .. } => symbol,
        MarketEvent::BinanceTopOfBook(quote) => &quote.symbol,
    }
}

#[derive(Debug)]
struct SizingSlot<S> {
    running: bool,
    pending: Option<S>,
    replacements: u64,
}

/// One running and one latest pending snapshot per strategy. There is no
/// unbounded work queue and one saturated pair cannot consume another pair's
/// slot.
#[derive(Debug)]
pub struct LatestOnlySizingSlots<S> {
    slots: BTreeMap<StrategyId, SizingSlot<S>>,
}

#[derive(Debug)]
struct FairSizingSlot<S> {
    running: bool,
    pending: Option<S>,
    replacements: u64,
}

/// Globally bounded latest-only work with deterministic round-robin dispatch.
///
/// capacity can retain at most one running and one pending snapshot per strategy
/// without allowing a continuously updated symbol to reacquire the next free
/// worker ahead of quieter strategies.
#[derive(Debug)]
pub struct FairLatestOnlySizingScheduler<S> {
    strategy_ids: Vec<StrategyId>,
    index_by_strategy: BTreeMap<StrategyId, usize>,
    slots: Vec<FairSizingSlot<S>>,
    next_index: usize,
    running: usize,
    maximum_running: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FairSizingSubmission {
    pub replaced: bool,
    pub queued_behind_running: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SizingSubmission<S> {
    Start(S),
    Pending { replaced: bool },
}

impl<S> FairLatestOnlySizingScheduler<S> {
    pub fn new(
        strategy_ids: impl IntoIterator<Item = StrategyId>,
        maximum_running: usize,
    ) -> anyhow::Result<Self> {
        ensure!(
            maximum_running > 0,
            "fair sizing scheduler worker limit must be positive"
        );
        let mut ordered = strategy_ids.into_iter().collect::<Vec<_>>();
        ensure!(!ordered.is_empty(), "fair sizing scheduler is empty");
        ordered.sort();
        let mut index_by_strategy = BTreeMap::new();
        for (index, strategy_id) in ordered.iter().enumerate() {
            ensure!(
                index_by_strategy
                    .insert(strategy_id.clone(), index)
                    .is_none(),
                "duplicate fair sizing strategy {}",
                strategy_id.as_str()
            );
        }
        ensure!(
            maximum_running <= ordered.len(),
            "fair sizing worker limit exceeds the strategy count"
        );
        let slots = ordered
            .iter()
            .map(|_| FairSizingSlot {
                running: false,
                pending: None,
                replacements: 0,
            })
            .collect();
        Ok(Self {
            strategy_ids: ordered,
            index_by_strategy,
            slots,
            next_index: 0,
            running: 0,
            maximum_running,
        })
    }

    pub fn submit(
        &mut self,
        strategy_id: &StrategyId,
        snapshot: S,
    ) -> anyhow::Result<FairSizingSubmission> {
        let index = *self
            .index_by_strategy
            .get(strategy_id)
            .with_context(|| format!("unknown fair sizing strategy {}", strategy_id.as_str()))?;
        let slot = &mut self.slots[index];
        let queued_behind_running = slot.running;
        let replaced = slot.pending.replace(snapshot).is_some();
        if replaced {
            slot.replacements = slot.replacements.saturating_add(1);
        }
        Ok(FairSizingSubmission {
            replaced,
            queued_behind_running,
        })
    }

    pub fn take_ready(&mut self) -> Option<(StrategyId, S)> {
        if self.running >= self.maximum_running {
            return None;
        }
        for offset in 0..self.slots.len() {
            let index = (self.next_index + offset) % self.slots.len();
            let slot = &mut self.slots[index];
            if slot.running {
                continue;
            }
            let Some(snapshot) = slot.pending.take() else {
                continue;
            };
            slot.running = true;
            self.running += 1;
            self.next_index = (index + 1) % self.slots.len();
            return Some((self.strategy_ids[index].clone(), snapshot));
        }
        None
    }

    pub fn complete(&mut self, strategy_id: &StrategyId) -> anyhow::Result<()> {
        let index = *self
            .index_by_strategy
            .get(strategy_id)
            .with_context(|| format!("unknown fair sizing strategy {}", strategy_id.as_str()))?;
        let slot = &mut self.slots[index];
        ensure!(
            slot.running,
            "fair sizing slot completed without running work"
        );
        slot.running = false;
        self.running = self
            .running
            .checked_sub(1)
            .context("fair sizing running count underflow")?;
        Ok(())
    }

    pub fn replacements(&self, strategy_id: &StrategyId) -> anyhow::Result<u64> {
        self.index_by_strategy
            .get(strategy_id)
            .map(|index| self.slots[*index].replacements)
            .with_context(|| format!("unknown fair sizing strategy {}", strategy_id.as_str()))
    }

    pub fn running(&self) -> usize {
        self.running
    }

    pub fn total_retained_work(&self) -> usize {
        self.slots
            .iter()
            .map(|slot| usize::from(slot.running) + usize::from(slot.pending.is_some()))
            .sum()
    }
}

impl<S> LatestOnlySizingSlots<S> {
    pub fn new(strategy_ids: impl IntoIterator<Item = StrategyId>) -> anyhow::Result<Self> {
        let mut slots = BTreeMap::new();
        for strategy_id in strategy_ids {
            ensure!(
                slots
                    .insert(
                        strategy_id.clone(),
                        SizingSlot {
                            running: false,
                            pending: None,
                            replacements: 0,
                        },
                    )
                    .is_none(),
                "duplicate sizing slot {}",
                strategy_id.as_str()
            );
        }
        ensure!(!slots.is_empty(), "latest-only sizing slots are empty");
        Ok(Self { slots })
    }

    pub fn submit(
        &mut self,
        strategy_id: &StrategyId,
        snapshot: S,
    ) -> anyhow::Result<SizingSubmission<S>> {
        let slot = self
            .slots
            .get_mut(strategy_id)
            .with_context(|| format!("unknown sizing strategy {}", strategy_id.as_str()))?;
        if !slot.running {
            slot.running = true;
            Ok(SizingSubmission::Start(snapshot))
        } else {
            let replaced = slot.pending.replace(snapshot).is_some();
            if replaced {
                slot.replacements = slot.replacements.saturating_add(1);
            }
            Ok(SizingSubmission::Pending { replaced })
        }
    }

    pub fn complete(&mut self, strategy_id: &StrategyId) -> anyhow::Result<Option<S>> {
        let slot = self
            .slots
            .get_mut(strategy_id)
            .with_context(|| format!("unknown sizing strategy {}", strategy_id.as_str()))?;
        ensure!(slot.running, "sizing slot completed without running work");
        if let Some(next) = slot.pending.take() {
            Ok(Some(next))
        } else {
            slot.running = false;
            Ok(None)
        }
    }

    pub fn replacements(&self, strategy_id: &StrategyId) -> anyhow::Result<u64> {
        self.slots
            .get(strategy_id)
            .map(|slot| slot.replacements)
            .with_context(|| format!("unknown sizing strategy {}", strategy_id.as_str()))
    }

    pub fn total_retained_work(&self) -> usize {
        self.slots
            .values()
            .map(|slot| usize::from(slot.running) + usize::from(slot.pending.is_some()))
            .sum()
    }
}

pub fn measure_strategy_evaluation(
    budget_us: u64,
    evaluate: impl FnOnce() -> anyhow::Result<(bool, bool)>,
) -> anyhow::Result<StrategyEvaluation> {
    let started_at = Instant::now();
    let (evaluated, candidate_produced) = evaluate()?;
    Ok(StrategyEvaluation {
        evaluated,
        candidate_produced,
        calculation_time_us: started_at.elapsed().as_micros(),
        budget_us,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::Instant,
    };

    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        domain::compiled::{CompiledHotPathStrategyPlan, InstrumentId, NetworkId},
        domain::config::LoadedDomainConfig,
        state::TopOfBook,
    };

    fn config() -> LoadedDomainConfig {
        LoadedDomainConfig::load("config/strategies/usdc-wld-world-chain.v12.json").unwrap()
    }

    fn strategy(id: &str, symbol: &str, pool: &str, execute: bool) -> CompiledHotPathStrategyPlan {
        CompiledHotPathStrategyPlan {
            strategy_id: StrategyId::new(id).unwrap(),
            pair_id: id.trim_start_matches("strategy:").to_owned(),
            instrument_id: InstrumentId::new(format!("instrument:{symbol}")).unwrap(),
            symbol: symbol.to_owned(),
            network_id: NetworkId::new(if execute {
                "eip155:480"
            } else {
                "eip155:42161"
            })
            .unwrap(),
            pool_ids: vec![PoolId::new(pool).unwrap()],
            observe: true,
            plan: true,
            execute,
            baseline_budget_us: 200,
            domain_config: config(),
        }
    }

    fn quote(symbol: &str, update_id: u64) -> TopOfBook {
        TopOfBook::new(
            Arc::from(symbol),
            update_id,
            Decimal::ONE,
            Decimal::ONE,
            Decimal::ONE,
            Decimal::ONE,
            None,
            None,
            Instant::now(),
            1,
            1,
        )
        .unwrap()
    }

    struct CountingEvaluator {
        strategy_id: StrategyId,
        symbol: String,
        evaluations: Arc<AtomicU64>,
    }

    struct FaultingEvaluator {
        strategy_id: StrategyId,
        symbol: String,
        calls: Arc<AtomicU64>,
    }

    impl StrategyEvaluator for FaultingEvaluator {
        fn strategy_id(&self) -> StrategyId {
            self.strategy_id.clone()
        }

        fn symbol(&self) -> &str {
            &self.symbol
        }

        fn on_market_event(
            &mut self,
            _event: MarketEvent,
            _depth: Option<&SpotDepthBook>,
        ) -> anyhow::Result<StrategyEvaluation> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            anyhow::bail!("injected shadow evaluator failure")
        }
    }

    impl StrategyEvaluator for CountingEvaluator {
        fn strategy_id(&self) -> StrategyId {
            self.strategy_id.clone()
        }

        fn symbol(&self) -> &str {
            &self.symbol
        }

        fn on_market_event(
            &mut self,
            event: MarketEvent,
            _depth: Option<&SpotDepthBook>,
        ) -> anyhow::Result<StrategyEvaluation> {
            let evaluated = matches!(event, MarketEvent::BinanceTopOfBook(_));
            if evaluated {
                self.evaluations.fetch_add(1, Ordering::Relaxed);
            }
            Ok(StrategyEvaluation {
                evaluated,
                candidate_produced: false,
                calculation_time_us: 1,
                budget_us: 200,
            })
        }
    }

    #[test]
    fn compiled_dependencies_route_only_related_symbol_and_pool() {
        let wld = strategy(
            "strategy:world-chain-usdc-wld",
            "WLDUSDC",
            "eip155:480:pool:wld",
            true,
        );
        let esp = strategy(
            "strategy:arbitrum-usdc-esp",
            "ESPUSDC",
            "eip155:42161:pool:esp",
            false,
        );
        let index = CompiledStrategyDependencyIndex::new(CompiledHotPathRuntimePlan {
            strategies: vec![esp.clone(), wld.clone()],
        })
        .unwrap();

        let wld_symbol: Vec<_> = index
            .for_symbol("WLDUSDC")
            .map(|strategy_index| {
                index
                    .strategy_at(strategy_index)
                    .unwrap()
                    .strategy_id
                    .clone()
            })
            .collect();
        let esp_pool: Vec<_> = index
            .for_pool(&esp.pool_ids[0])
            .map(|strategy_index| {
                index
                    .strategy_at(strategy_index)
                    .unwrap()
                    .strategy_id
                    .clone()
            })
            .collect();

        assert_eq!(wld_symbol, vec![wld.strategy_id]);
        assert_eq!(esp_pool, vec![esp.strategy_id]);
        assert_eq!(index.for_symbol("BNBUSDT").count(), 0);
        assert_eq!(
            index
                .for_pool(&PoolId::new("eip155:480:pool:other").unwrap())
                .count(),
            0
        );
    }

    #[test]
    fn latest_only_slots_are_bounded_and_isolated_per_strategy() {
        let wld = StrategyId::new("strategy:world-chain-usdc-wld").unwrap();
        let esp = StrategyId::new("strategy:arbitrum-usdc-esp").unwrap();
        let mut slots = LatestOnlySizingSlots::new([wld.clone(), esp.clone()]).unwrap();

        assert_eq!(slots.submit(&esp, 1).unwrap(), SizingSubmission::Start(1));
        for snapshot in 2..=10_000 {
            assert!(matches!(
                slots.submit(&esp, snapshot).unwrap(),
                SizingSubmission::Pending { .. }
            ));
        }
        assert_eq!(slots.total_retained_work(), 2);
        assert_eq!(slots.submit(&wld, 7).unwrap(), SizingSubmission::Start(7));
        assert_eq!(slots.total_retained_work(), 3);
        assert_eq!(slots.complete(&esp).unwrap(), Some(10_000));
        assert_eq!(slots.complete(&wld).unwrap(), None);
        assert_eq!(slots.replacements(&esp).unwrap(), 9_998);
    }

    #[test]
    fn fair_latest_only_scheduler_bounds_workers_and_prevents_noisy_starvation() {
        let strategies = (0..20)
            .map(|index| StrategyId::new(format!("strategy:capacity-{index:02}")).unwrap())
            .collect::<Vec<_>>();
        let noisy = strategies[0].clone();
        let mut scheduler = FairLatestOnlySizingScheduler::new(strategies.clone(), 1).unwrap();
        for (index, strategy_id) in strategies.iter().enumerate() {
            let submission = scheduler.submit(strategy_id, index).unwrap();
            assert!(!submission.replaced);
            assert!(!submission.queued_behind_running);
        }

        let (first, _) = scheduler.take_ready().unwrap();
        assert_eq!(first, noisy);
        assert_eq!(scheduler.running(), 1);
        for replacement in 1..=10_000 {
            let submission = scheduler.submit(&noisy, replacement).unwrap();
            assert!(submission.queued_behind_running);
            assert_eq!(submission.replaced, replacement > 1);
        }
        assert_eq!(scheduler.total_retained_work(), 21);

        let mut dispatched = vec![first];
        while dispatched.len() < strategies.len() {
            let completed = dispatched.last().unwrap().clone();
            scheduler.complete(&completed).unwrap();
            let (strategy_id, _) = scheduler.take_ready().unwrap();
            dispatched.push(strategy_id);
        }
        assert_eq!(dispatched, strategies);
        assert_eq!(scheduler.replacements(&noisy).unwrap(), 9_999);
        assert_eq!(scheduler.total_retained_work(), 2);

        scheduler.complete(dispatched.last().unwrap()).unwrap();
        let (next, snapshot) = scheduler.take_ready().unwrap();
        assert_eq!(next, noisy);
        assert_eq!(snapshot, 10_000);
    }

    #[test]
    fn owner_routes_esp_and_unrelated_symbols_without_evaluating_wld() {
        let wld = strategy(
            "strategy:world-chain-usdc-wld",
            "WLDUSDC",
            "eip155:480:pool:wld",
            true,
        );
        let esp = strategy(
            "strategy:arbitrum-usdc-esp",
            "ESPUSDC",
            "eip155:42161:pool:esp",
            false,
        );
        let dependencies = CompiledStrategyDependencyIndex::new(CompiledHotPathRuntimePlan {
            strategies: vec![esp.clone(), wld.clone()],
        })
        .unwrap();
        let wld_count = Arc::new(AtomicU64::new(0));
        let esp_count = Arc::new(AtomicU64::new(0));
        let primary = CountingEvaluator {
            strategy_id: wld.strategy_id,
            symbol: wld.symbol,
            evaluations: Arc::clone(&wld_count),
        };
        let shadow = CountingEvaluator {
            strategy_id: esp.strategy_id,
            symbol: esp.symbol,
            evaluations: Arc::clone(&esp_count),
        };
        let mut owner =
            HotPathDecisionOwner::new(primary, vec![Box::new(shadow)], dependencies).unwrap();

        let esp_summary = owner
            .on_market_event(MarketEvent::BinanceTopOfBook(quote("ESPUSDC", 1)), None)
            .unwrap();
        let unrelated = owner
            .on_market_event(MarketEvent::BinanceTopOfBook(quote("BNBUSDT", 2)), None)
            .unwrap();
        let wld_summary = owner
            .on_market_event(MarketEvent::BinanceTopOfBook(quote("WLDUSDC", 3)), None)
            .unwrap();

        assert_eq!(esp_summary.routed_strategies, 1);
        assert_eq!(unrelated.routed_strategies, 0);
        assert_eq!(wld_summary.routed_strategies, 1);
        assert_eq!(wld_count.load(Ordering::Relaxed), 1);
        assert_eq!(esp_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn injected_esp_fault_degrades_only_esp_and_wld_keeps_evaluating() {
        let wld = strategy(
            "strategy:world-chain-usdc-wld",
            "WLDUSDC",
            "eip155:480:pool:wld",
            true,
        );
        let esp = strategy(
            "strategy:arbitrum-usdc-esp",
            "ESPUSDC",
            "eip155:42161:pool:esp",
            false,
        );
        let dependencies = CompiledStrategyDependencyIndex::new(CompiledHotPathRuntimePlan {
            strategies: vec![esp.clone(), wld.clone()],
        })
        .unwrap();
        let wld_calls = Arc::new(AtomicU64::new(0));
        let esp_calls = Arc::new(AtomicU64::new(0));
        let primary = CountingEvaluator {
            strategy_id: wld.strategy_id,
            symbol: wld.symbol,
            evaluations: Arc::clone(&wld_calls),
        };
        let shadow = FaultingEvaluator {
            strategy_id: esp.strategy_id.clone(),
            symbol: esp.symbol,
            calls: Arc::clone(&esp_calls),
        };
        let mut owner =
            HotPathDecisionOwner::new(primary, vec![Box::new(shadow)], dependencies).unwrap();

        owner
            .on_market_event(MarketEvent::BinanceTopOfBook(quote("ESPUSDC", 1)), None)
            .unwrap();
        owner
            .on_market_event(MarketEvent::BinanceTopOfBook(quote("ESPUSDC", 2)), None)
            .unwrap();
        owner
            .on_market_event(MarketEvent::BinanceTopOfBook(quote("WLDUSDC", 3)), None)
            .unwrap();

        assert!(owner.shadow_is_degraded(&esp.strategy_id));
        assert_eq!(owner.take_dependency_faults().len(), 1);
        assert_eq!(esp_calls.load(Ordering::Relaxed), 1);
        assert_eq!(wld_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn stale_snapshot_rejection_is_exact_and_generation_scoped() {
        let strategy_id = StrategyId::new("strategy:world-chain-usdc-wld").unwrap();
        let pool = PoolId::new("eip155:480:pool:wld").unwrap();
        let stamp = StrategySnapshotStamp {
            strategy_id,
            connection_generation: 3,
            update_id: 90,
            pool_generations: vec![(pool.clone(), 8)],
        };
        let generations = BTreeMap::from([(pool.clone(), 8)]);

        assert!(stamp.is_current(3, 90, &generations));
        assert!(!stamp.is_current(4, 90, &generations));
        assert!(!stamp.is_current(3, 91, &generations));
        assert!(!stamp.is_current(3, 90, &BTreeMap::from([(pool, 9)])));
    }

    #[test]
    fn quote_fixture_is_valid_for_router_evaluator_tests() {
        assert_eq!(quote("WLDUSDC", 9).update_id, 9);
    }
}

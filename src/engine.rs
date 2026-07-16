use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use serde_json::json;

use crate::{
    balances::BalanceEvent,
    config::AppConfig,
    dex::mirror::{DexMirror, LogApplyResult},
    domain::config::LoadedDomainConfig,
    hot_telemetry::{HotTelemetryHandle, HotTelemetryTask, channel as hot_telemetry_channel},
    market_data::{MarketEvent, alchemy::DexStreamEvent},
    opportunity::{OpportunityEngine, PreparedPoolBuildRequest, PreparedPoolBuildResult},
    rebalance::{Direction, RebalanceEvaluation, RebalanceExecutionOperation, RebalanceTracker},
    state::{QuoteApplyResult, RuntimePhase, RuntimeState, TopOfBook},
    telemetry::TelemetryHandle,
};

pub struct TradingEngine {
    config: AppConfig,
    domain_config: Arc<LoadedDomainConfig>,
    state: RuntimeState,
    dex: DexMirror,
    opportunities: OpportunityEngine,
    rebalance: RebalanceTracker,
    telemetry: TelemetryHandle,
    hot_telemetry: HotTelemetryHandle,
    pending_rebalance: Option<RebalanceEvaluation>,
    rebalance_inflight: bool,
    rebalance_inflight_since: Option<Instant>,
    rebalance_blocked: bool,
    rebalance_settlement: Option<RebalanceSettlementBarrier>,
    last_rebalance_health_log_at: Option<Instant>,
}

const REBALANCE_HEALTH_LOG_INTERVAL: Duration = Duration::from_secs(60);
const MINIMUM_REBALANCE_SETTLEMENT_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug)]
struct RebalanceSettlementBarrier {
    operation_id: String,
    token_symbol: String,
    direction: Direction,
    binance_after: Instant,
    wallet_after: Instant,
    started_at: Instant,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct RebalanceHealthState {
    healthy: bool,
    inflight_stuck: bool,
    settlement_stuck: bool,
}

fn rebalance_health_state(
    blocked: bool,
    inflight_age: Option<Duration>,
    settlement_age: Option<Duration>,
    operation_timeout: Duration,
    settlement_timeout: Duration,
) -> RebalanceHealthState {
    let inflight_stuck = inflight_age.is_some_and(|age| age >= operation_timeout);
    let settlement_stuck = settlement_age.is_some_and(|age| age >= settlement_timeout);
    RebalanceHealthState {
        healthy: !blocked && !inflight_stuck && !settlement_stuck,
        inflight_stuck,
        settlement_stuck,
    }
}

impl RebalanceSettlementBarrier {
    fn reconciled(&self, binance_observed_at: Instant, wallet_observed_at: Instant) -> bool {
        binance_observed_at > self.binance_after && wallet_observed_at > self.wallet_after
    }
}

#[derive(Debug, Clone, Copy)]
struct TradingReadiness {
    dex_ready: bool,
    balances_ready: bool,
}

impl TradingReadiness {
    const fn ready(self) -> bool {
        self.dex_ready && self.balances_ready
    }
}

impl TradingEngine {
    pub fn new(
        config: AppConfig,
        domain_config: Arc<LoadedDomainConfig>,
        dex: DexMirror,
        telemetry: TelemetryHandle,
        rebalance: RebalanceTracker,
    ) -> anyhow::Result<(Self, HotTelemetryTask)> {
        let symbols = domain_config
            .binance_symbols()
            .into_iter()
            .map(Arc::<str>::from);
        let opportunities = OpportunityEngine::new(domain_config.snapshot(), &dex)?;
        let (hot_telemetry, hot_telemetry_task) =
            hot_telemetry_channel(&config, opportunities.pairs(), &dex, telemetry.clone())?;
        Ok((
            Self {
                config,
                domain_config,
                state: RuntimeState::new(symbols),
                dex,
                opportunities,
                rebalance,
                telemetry,
                hot_telemetry,
                pending_rebalance: None,
                rebalance_inflight: false,
                rebalance_inflight_since: None,
                rebalance_blocked: false,
                rebalance_settlement: None,
                last_rebalance_health_log_at: None,
            },
            hot_telemetry_task,
        ))
    }

    pub fn start(&mut self) {
        self.telemetry.emit(
            "runtime_starting",
            json!({
                "engine_id": self.config.engine_id,
                "service": self.config.service_name,
                "gcp_project_id": self.config.gcp_project_id,
                "gcp_region": self.config.gcp_region,
                "domain_snapshot_id": self.domain_config.snapshot().snapshot_id,
                "domain_config_sha256": self.domain_config.fingerprint_sha256(),
                "domain_config_path": self.domain_config.path().display().to_string(),
                "pair_ids": self.domain_config.pair_ids(),
                "binance_symbols": self.domain_config.binance_symbols(),
                "dex_pools": self.dex.pool_count(),
                "dex_unavailable_pools": self.dex.unavailable_count(),
                "world_chain_block": self.dex.latest_head().number,
            }),
        );
    }

    pub fn on_dex_event(
        &mut self,
        event: DexStreamEvent,
    ) -> anyhow::Result<Option<PreparedPoolBuildRequest>> {
        let request = match event {
            DexStreamEvent::Log { log, received_at } => {
                if let LogApplyResult::Applied { pool_index, kind } = self.dex.apply_log(&log)? {
                    let request = self
                        .opportunities
                        .request_pool_refresh(pool_index, &self.dex)?;
                    let pool = self.dex.pool(pool_index)?;
                    self.telemetry.emit(
                        "dex_pool_event",
                        json!({
                            "engine_id": self.config.engine_id,
                            "pair_id": pool.pair_id,
                            "identity": format!("{:?}", pool.identity),
                            "kind": kind,
                            "block_number": log.block_number,
                            "transaction_index": log.transaction_index,
                            "log_index": log.log_index,
                            "engine_queue_age_us": received_at.elapsed().as_micros(),
                            "prepared_generation": request.generation(),
                            "prepared_state": "building",
                        }),
                    );
                    Some(request)
                } else {
                    None
                }
            }
            DexStreamEvent::Head { head, received_at } => {
                if self.dex.apply_head(head)? {
                    self.telemetry.emit(
                        "world_chain_head",
                        json!({
                            "engine_id": self.config.engine_id,
                            "block_number": head.number,
                            "engine_queue_age_us": received_at.elapsed().as_micros(),
                        }),
                    );
                }
                None
            }
        };
        self.refresh_phase(Instant::now());
        Ok(request)
    }

    pub fn on_prepared_pool(&mut self, result: PreparedPoolBuildResult) -> anyhow::Result<()> {
        let Some(prepared) = self.opportunities.finish_pool_refresh(result)? else {
            return Ok(());
        };
        let pool = self.dex.pool(prepared.pool_index)?;
        self.telemetry.emit(
            "dex_pool_prepared",
            json!({
                "engine_id": self.config.engine_id,
                "pair_id": pool.pair_id,
                "identity": format!("{:?}", pool.identity),
                "pool_index": prepared.pool_index,
                "prepared_generation": prepared.generation,
                "prepared_exact_output_segments": prepared.exact_output_segments,
                "prepared_exact_input_segments": prepared.exact_input_segments,
                "build_time_us": prepared.build_time_us,
                "total_time_us": prepared.total_time_us,
            }),
        );
        self.refresh_phase(Instant::now());
        if self.state.phase == RuntimePhase::Ready {
            let books: Vec<_> = self
                .state
                .binance_feeds
                .values()
                .filter_map(|feed| feed.book.clone())
                .collect();
            for quote in books {
                self.evaluate_ready_quote(&quote, "dex_prepared")?;
            }
        }
        Ok(())
    }

    pub fn on_market_event(&mut self, event: MarketEvent) -> anyhow::Result<()> {
        match event {
            MarketEvent::FeedConnected {
                symbol,
                generation,
                observed_at,
            } => {
                self.state.on_connected(&symbol, generation);
                self.telemetry.emit(
                    "binance_feed_connected",
                    json!({
                        "engine_id": self.config.engine_id,
                        "product": "spot",
                        "symbol": symbol.as_ref(),
                        "generation": generation,
                        "observed_mono_age_us": observed_at.elapsed().as_micros(),
                    }),
                );
            }
            MarketEvent::FeedDisconnected {
                symbol,
                generation,
                reason,
                observed_at,
            } => {
                self.state.on_disconnected(&symbol, generation);
                self.telemetry.emit(
                    "binance_feed_disconnected",
                    json!({
                        "engine_id": self.config.engine_id,
                        "product": "spot",
                        "symbol": symbol.as_ref(),
                        "generation": generation,
                        "reason": reason,
                        "observed_mono_age_us": observed_at.elapsed().as_micros(),
                    }),
                );
            }
            MarketEvent::BinanceTopOfBook(quote) => {
                self.on_binance_quote(quote)?;
            }
        }
        self.refresh_phase(Instant::now());
        Ok(())
    }

    pub fn on_balance_event(&mut self, event: BalanceEvent) {
        match event {
            BalanceEvent::Binance(snapshot) => {
                let balances = snapshot
                    .balances
                    .iter()
                    .map(|(asset, balance)| {
                        json!({
                            "asset": asset.as_ref(),
                            "free": balance.free.to_string(),
                            "locked": balance.locked.to_string(),
                        })
                    })
                    .collect::<Vec<_>>();
                self.telemetry.emit(
                    "binance_balance_snapshot",
                    json!({
                        "engine_id": self.config.engine_id,
                        "account_update_time_ms": snapshot.account_update_time_ms,
                        "account_type": snapshot.account_type,
                        "can_trade": snapshot.can_trade,
                        "balances": balances,
                        "request_duration_us": snapshot.request_duration_us,
                    }),
                );
                self.state.balances.apply_binance(snapshot);
            }
            BalanceEvent::Wallet(snapshot) => {
                let token_balances = snapshot
                    .token_balances
                    .iter()
                    .map(|balance| {
                        json!({
                            "symbol": balance.symbol.as_ref(),
                            "contract": format!("{:#x}", balance.contract),
                            "base_units": balance.base_units.to_string(),
                        })
                    })
                    .collect::<Vec<_>>();
                self.telemetry.emit(
                    "wallet_balance_snapshot",
                    json!({
                        "engine_id": self.config.engine_id,
                        "owner": format!("{:#x}", snapshot.owner),
                        "chain_id": snapshot.chain_id,
                        "block_number": snapshot.block_number,
                        "block_hash": format!("{:#x}", snapshot.block_hash),
                        "native_balance_wei": snapshot.native_balance_wei.to_string(),
                        "token_balances": token_balances,
                        "request_duration_us": snapshot.request_duration_us,
                        "rpc_http_requests": snapshot.rpc_stats.http_requests,
                        "rpc_eth_calls": snapshot.rpc_stats.eth_calls,
                        "rpc_rate_limit_retries": snapshot.rpc_stats.rate_limit_retries,
                    }),
                );
                self.state.balances.apply_wallet(snapshot);
            }
            BalanceEvent::Failed {
                source,
                error,
                observed_at,
            } => {
                self.state.balances.record_failure(source);
                tracing::warn!(
                    source = source.as_str(),
                    error,
                    "balance synchronization failed"
                );
                self.telemetry.emit(
                    "balance_sync_failed",
                    json!({
                        "engine_id": self.config.engine_id,
                        "source": source.as_str(),
                        "error": error,
                        "engine_queue_age_us": observed_at.elapsed().as_micros(),
                    }),
                );
            }
        }
        self.evaluate_rebalance();
        self.refresh_phase(Instant::now());
    }

    pub fn take_rebalance_execution(&mut self) -> Option<RebalanceEvaluation> {
        self.pending_rebalance.take()
    }

    pub fn on_rebalance_execution_result(
        &mut self,
        result: Result<&RebalanceExecutionOperation, &str>,
    ) {
        self.rebalance_inflight = false;
        self.rebalance_inflight_since = None;
        match result {
            Ok(operation) => {
                if let (Some(binance), Some(wallet)) = (
                    self.state.balances.binance.as_ref(),
                    self.state.balances.wallet.as_ref(),
                ) {
                    self.rebalance_settlement = Some(RebalanceSettlementBarrier {
                        operation_id: operation.intent.operation_id.clone(),
                        token_symbol: operation.intent.token_symbol.clone(),
                        direction: operation.intent.direction,
                        binance_after: binance.observed_at,
                        wallet_after: wallet.observed_at,
                        started_at: Instant::now(),
                    });
                }
                self.telemetry.emit(
                    "rebalance_execution_completed",
                    json!({
                        "engine_id": self.config.engine_id,
                        "operation_id": operation.intent.operation_id,
                    }),
                );
                self.telemetry.emit(
                    "rebalance_settlement_waiting",
                    json!({
                        "engine_id": self.config.engine_id,
                        "operation_id": operation.intent.operation_id,
                        "token": operation.intent.token_symbol,
                        "direction": format!("{:?}", operation.intent.direction),
                    }),
                );
            }
            Err(error) => {
                self.rebalance_blocked = true;
                self.rebalance.mark_unbalanced();
                tracing::error!(error, "rebalance executor failed closed");
                self.telemetry.emit(
                    "rebalance_execution_failed",
                    json!({
                        "engine_id": self.config.engine_id,
                        "error": error,
                    }),
                );
            }
        }
        self.refresh_phase(Instant::now());
    }

    fn evaluate_rebalance(&mut self) {
        let Some(binance) = self.state.balances.binance.as_ref() else {
            return;
        };
        let Some(wallet) = self.state.balances.wallet.as_ref() else {
            return;
        };
        if self
            .rebalance_settlement
            .as_ref()
            .is_some_and(|barrier| barrier.reconciled(binance.observed_at, wallet.observed_at))
            && let Some(barrier) = self.rebalance_settlement.take()
        {
            self.telemetry.emit(
                "rebalance_settlement_reconciled",
                json!({
                    "engine_id": self.config.engine_id,
                    "operation_id": barrier.operation_id,
                    "token": barrier.token_symbol,
                    "direction": format!("{:?}", barrier.direction),
                }),
            );
        }
        match self.rebalance.evaluate(binance, wallet) {
            Ok(evaluations) => {
                let mode = self.config.rebalance_execution_mode.as_str();
                for evaluation in evaluations {
                    let action = evaluation.plan.action.as_ref();
                    self.telemetry.emit(
                        "rebalance_plan_evaluated",
                        json!({
                            "engine_id": self.config.engine_id,
                            "mode": mode,
                            "token": evaluation.token_symbol,
                            "token_decimals": evaluation.token_decimals,
                            "reference_captured": evaluation.reference_captured,
                            "reference_inventory_base_units": evaluation.plan.reference_inventory.to_string(),
                            "start_balance_base_units": evaluation.plan.start_balance.to_string(),
                            "binance_balance_base_units": evaluation.plan.projected.binance.to_string(),
                            "wallet_balance_base_units": evaluation.plan.projected.wallet.to_string(),
                            "binance_target_base_units": evaluation.plan.binance_target.to_string(),
                            "wallet_target_base_units": evaluation.plan.wallet_target.to_string(),
                            "action_direction": action.map(|action| format!("{:?}", action.direction)),
                            "action_amount_base_units": action.map(|action| action.amount.to_string()),
                            "action_route": action.map(|action| format!("{:?}", action.route)),
                        }),
                    );
                }
                let pending_action = self.rebalance.pending_action();
                if mode == "full_live"
                    && !self.rebalance_inflight
                    && !self.rebalance_blocked
                    && self.rebalance_settlement.is_none()
                    && self.pending_rebalance.is_none()
                    && let Some(evaluation) = pending_action
                {
                    self.rebalance_inflight = true;
                    self.rebalance_inflight_since = Some(Instant::now());
                    self.pending_rebalance = Some(evaluation);
                }
            }
            Err(error) => {
                self.rebalance.mark_unbalanced();
                tracing::warn!(error = %error, "rebalance planning failed closed");
                self.telemetry.emit(
                    "rebalance_plan_failed",
                    json!({
                        "engine_id": self.config.engine_id,
                        "mode": self.config.rebalance_execution_mode,
                        "error": format!("{error:#}"),
                    }),
                );
            }
        }
    }

    fn on_binance_quote(&mut self, quote: TopOfBook) -> anyhow::Result<()> {
        let result = self.state.apply_quote(quote.clone());
        match result {
            QuoteApplyResult::Accepted => {
                // The decision is evaluated only after all readiness inputs are
                // fresh. The calculation itself performs no RPC, I/O, or locks.
                self.refresh_phase(Instant::now());
                if self.state.phase == RuntimePhase::Ready {
                    self.evaluate_ready_quote(&quote, "binance_book_ticker")?;
                }

                // Raw market telemetry is deliberately serialized only after
                // the opportunity decision. It must never delay detection or
                // eventual order submission.
                self.hot_telemetry.emit_binance_book(&quote);
            }
            rejected => self.telemetry.emit(
                "binance_book_ticker_rejected",
                json!({
                    "engine_id": self.config.engine_id,
                    "product": "spot",
                    "symbol": quote.symbol.as_ref(),
                    "update_id": quote.update_id,
                    "connection_generation": quote.connection_generation,
                    "reason": format!("{rejected:?}"),
                }),
            ),
        }
        Ok(())
    }

    fn evaluate_ready_quote(
        &mut self,
        quote: &TopOfBook,
        trigger: &'static str,
    ) -> anyhow::Result<()> {
        let calculation_started = Instant::now();
        if let Some(evaluation) = self.opportunities.evaluate(quote)? {
            self.hot_telemetry.emit_evaluation(
                quote,
                evaluation,
                self.dex.latest_head().number,
                calculation_started.elapsed().as_micros(),
                trigger,
            );
        }
        Ok(())
    }

    pub fn refresh_health(&mut self) {
        let now = Instant::now();
        self.refresh_phase(now);
        self.log_rebalance_health(now);
    }

    fn log_rebalance_health(&mut self, now: Instant) {
        if self
            .last_rebalance_health_log_at
            .is_some_and(|last| now.saturating_duration_since(last) < REBALANCE_HEALTH_LOG_INTERVAL)
        {
            return;
        }

        let inflight_age = self
            .rebalance_inflight_since
            .map(|started_at| now.saturating_duration_since(started_at));
        let settlement_age = self
            .rebalance_settlement
            .as_ref()
            .map(|barrier| now.saturating_duration_since(barrier.started_at));
        let settlement_timeout = Duration::from_millis(
            self.config
                .balance_max_age_ms
                .saturating_mul(12)
                .max(MINIMUM_REBALANCE_SETTLEMENT_TIMEOUT.as_millis() as u64),
        );
        let health = rebalance_health_state(
            self.rebalance_blocked,
            inflight_age,
            settlement_age,
            Duration::from_secs(self.config.rebalance_executor_timeout_seconds),
            settlement_timeout,
        );
        let inflight_age_ms = inflight_age.map(|age| age.as_millis());
        let settlement_age_ms = settlement_age.map(|age| age.as_millis());
        if health.healthy {
            tracing::info!(
                healthy = true,
                rebalance_blocked = self.rebalance_blocked,
                rebalance_inflight = self.rebalance_inflight,
                inflight_age_ms,
                settlement_waiting = self.rebalance_settlement.is_some(),
                settlement_age_ms,
                "rebalance health heartbeat"
            );
        } else {
            tracing::error!(
                healthy = false,
                rebalance_blocked = self.rebalance_blocked,
                rebalance_inflight = self.rebalance_inflight,
                inflight_stuck = health.inflight_stuck,
                inflight_age_ms,
                settlement_waiting = self.rebalance_settlement.is_some(),
                settlement_stuck = health.settlement_stuck,
                settlement_age_ms,
                "rebalance health heartbeat"
            );
        }
        self.last_rebalance_health_log_at = Some(now);
    }

    fn refresh_phase(&mut self, now: Instant) {
        let previous = self.state.phase;
        let dex_ready = self.dex.is_fresh(now, self.config.dex_head_max_age_ms)
            && self.opportunities.is_ready();
        let balances_ready = self
            .state
            .balances
            .is_fresh(now, self.config.balance_max_age_ms);
        // Rebalancing is proactive inventory maintenance, not a global trading
        // lock. Its pending, in-flight, failed, and post-reconciliation states
        // serialize only rebalance operations. Trading remains gated by fresh
        // market/DEX/balance inputs; an execution coordinator must separately
        // reserve and validate the assets required by its concrete trade.
        let trading_readiness = TradingReadiness {
            dex_ready,
            balances_ready,
        };
        let current = self.state.refresh_phase(
            now,
            self.config.market_data_max_age_ms,
            trading_readiness.ready(),
        );
        if previous != current {
            tracing::info!(?previous, ?current, "runtime phase changed");
            self.telemetry.emit(
                "runtime_phase_changed",
                json!({
                    "engine_id": self.config.engine_id,
                    "previous": previous,
                    "current": current,
                }),
            );
        }
    }

    pub fn phase(&self) -> RuntimePhase {
        self.state.phase
    }

    pub fn shutdown(&mut self) {
        self.state.stop();
        self.telemetry.emit(
            "runtime_stopping",
            json!({
                "engine_id": self.config.engine_id,
                "processed_events": self.state.processed_events,
            }),
        );
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use crate::rebalance::Direction;

    use super::{RebalanceSettlementBarrier, TradingReadiness, rebalance_health_state};

    #[test]
    fn rebalance_state_is_not_a_global_trading_readiness_input() {
        assert!(
            TradingReadiness {
                dex_ready: true,
                balances_ready: true,
            }
            .ready()
        );
    }

    #[test]
    fn stale_dex_or_balance_inputs_still_fail_closed() {
        for readiness in [
            TradingReadiness {
                dex_ready: false,
                balances_ready: true,
            },
            TradingReadiness {
                dex_ready: true,
                balances_ready: false,
            },
        ] {
            assert!(!readiness.ready());
        }
    }

    #[test]
    fn completed_rebalance_waits_for_both_continuous_balance_streams() {
        let now = Instant::now();
        let later = now + std::time::Duration::from_millis(1);
        let barrier = RebalanceSettlementBarrier {
            operation_id: "rebalance-wld-1".to_owned(),
            token_symbol: "WLD".to_owned(),
            direction: Direction::WalletToBinance,
            binance_after: now,
            wallet_after: now,
            started_at: now,
        };

        assert!(!barrier.reconciled(now, now));
        assert!(!barrier.reconciled(later, now));
        assert!(!barrier.reconciled(now, later));
        assert!(barrier.reconciled(later, later));
    }

    #[test]
    fn settlement_barrier_does_not_change_trading_readiness() {
        assert!(
            TradingReadiness {
                dex_ready: true,
                balances_ready: true,
            }
            .ready()
        );
    }

    #[test]
    fn rebalance_health_detects_blocked_and_stuck_states_at_the_boundary() {
        let timeout = std::time::Duration::from_secs(60);

        assert!(rebalance_health_state(false, None, None, timeout, timeout).healthy);
        assert!(!rebalance_health_state(true, None, None, timeout, timeout).healthy);
        let inflight = rebalance_health_state(false, Some(timeout), None, timeout, timeout);
        assert!(inflight.inflight_stuck);
        assert!(!inflight.healthy);
        let settlement = rebalance_health_state(false, None, Some(timeout), timeout, timeout);
        assert!(settlement.settlement_stuck);
        assert!(!settlement.healthy);
    }
}

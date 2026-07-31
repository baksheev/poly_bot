use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use alloy_primitives::hex;
use anyhow::{Context, ensure};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    binance::depth::SpotDepthBook,
    dex::{clmm::ClmmPool, hydration::decode_v3_core_head},
    domain::{
        compiled::{
            CompiledHotPathRuntimePlan, CompiledHotPathStrategyPlan, InstrumentId, NetworkId,
            PoolId, StrategyId,
        },
        config::LoadedDomainConfig,
    },
    market_data::MarketEvent,
    state::TopOfBook,
    strategy_runtime::{
        CompiledStrategyDependencyIndex, FairLatestOnlySizingScheduler, HotPathDecisionOwner,
        StrategyEvaluation, StrategyEvaluator,
    },
};

const SUPPORTED_SCHEMA_VERSION: u32 = 1;
const REQUIRED_MODE: &str = "capacity_replay_only";
const MINIMUM_PAIR_COUNT: usize = 10;
const MAXIMUM_PAIR_COUNT: usize = 20;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct M11CapacityReplayArtifact {
    pub schema_version: u32,
    pub artifact_id: String,
    pub mode: String,
    pub network_io_enabled: bool,
    pub external_mutation_authorized: bool,
    pub source: M11CapacitySource,
    pub frames_per_pair: u64,
    pub reconnect_bursts: u32,
    pub maximum_sizing_workers: usize,
    pub rehydration_fixture: M11RehydrationFixture,
    pub pairs: Vec<M11CapacityPair>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct M11CapacitySource {
    pub kind: String,
    pub run_id: u64,
    pub captured_at: String,
    pub warning: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct M11CapacityPair {
    pub pair_id: String,
    pub symbol: String,
    pub network_id: String,
    pub candidate_ids: Vec<u64>,
    pub pool_count: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct M11RehydrationFixture {
    pub source: String,
    pub fee_pips: u32,
    pub slot0: String,
    pub liquidity: String,
    pub tick_spacing: String,
}

impl M11CapacityReplayArtifact {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let bytes = fs::read(path)
            .with_context(|| format!("failed to read M11 replay artifact {}", path.display()))?;
        let artifact = serde_json::from_slice::<Self>(&bytes)
            .with_context(|| format!("failed to parse M11 replay artifact {}", path.display()))?;
        artifact
            .validate()
            .with_context(|| format!("invalid M11 replay artifact {}", path.display()))?;
        Ok(artifact)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.schema_version == SUPPORTED_SCHEMA_VERSION,
            "unsupported M11 replay schema_version {}",
            self.schema_version
        );
        ensure!(
            self.mode == REQUIRED_MODE,
            "M11 replay mode must be {REQUIRED_MODE}"
        );
        ensure!(
            !self.network_io_enabled,
            "M11 capacity replay must not enable network I/O"
        );
        ensure!(
            !self.external_mutation_authorized,
            "M11 capacity replay must not authorize external mutation"
        );
        ensure!(
            (MINIMUM_PAIR_COUNT..=MAXIMUM_PAIR_COUNT).contains(&self.pairs.len()),
            "M11 capacity replay requires {MINIMUM_PAIR_COUNT}..={MAXIMUM_PAIR_COUNT} pairs"
        );
        ensure!(
            self.frames_per_pair > 0,
            "M11 frames_per_pair must be positive"
        );
        ensure!(
            self.reconnect_bursts > 0,
            "M11 reconnect_bursts must be positive"
        );
        ensure!(
            self.maximum_sizing_workers > 0 && self.maximum_sizing_workers <= self.pairs.len(),
            "M11 maximum_sizing_workers is outside the pair bound"
        );
        ensure!(
            !self.source.kind.is_empty()
                && !self.source.captured_at.is_empty()
                && !self.source.warning.is_empty(),
            "M11 capacity source provenance is incomplete"
        );
        ensure!(
            !self.rehydration_fixture.source.is_empty() && self.rehydration_fixture.fee_pips > 0,
            "M11 rehydration fixture provenance is incomplete"
        );
        for (name, value) in [
            ("slot0", &self.rehydration_fixture.slot0),
            ("liquidity", &self.rehydration_fixture.liquidity),
            ("tick_spacing", &self.rehydration_fixture.tick_spacing),
        ] {
            decode_hex(name, value)?;
        }

        let mut pair_ids = BTreeSet::new();
        let mut symbols = BTreeSet::new();
        let mut candidate_ids = BTreeSet::new();
        for pair in &self.pairs {
            ensure!(
                pair_ids.insert(pair.pair_id.clone()),
                "duplicate M11 pair_id {}",
                pair.pair_id
            );
            ensure!(
                !pair.symbol.is_empty()
                    && pair
                        .symbol
                        .bytes()
                        .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit()),
                "invalid M11 symbol {}",
                pair.symbol
            );
            ensure!(
                symbols.insert(pair.symbol.clone()),
                "duplicate M11 symbol {}",
                pair.symbol
            );
            NetworkId::new(pair.network_id.clone())?;
            ensure!(pair.pool_count > 0, "M11 pair has no pools");
            ensure!(
                !pair.candidate_ids.is_empty(),
                "M11 pair has no source candidate ids"
            );
            for candidate_id in &pair.candidate_ids {
                ensure!(
                    candidate_ids.insert(*candidate_id),
                    "duplicate M11 source candidate id {candidate_id}"
                );
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct M11LatencySummary {
    pub samples: usize,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub maximum_ns: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct M11FairnessSummary {
    pub maximum_workers: usize,
    pub maximum_observed_running: usize,
    pub maximum_retained_work: usize,
    pub unique_strategies_before_noisy_repeat: usize,
    pub noisy_replacements: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct M11RehydrationSummary {
    pub cycles: u32,
    pub pool_publications: usize,
    pub partial_batches_rejected: u64,
    pub captured_batch_materialization_latency: M11LatencySummary,
    pub decode_latency: M11LatencySummary,
    pub pool_build_latency: M11LatencySummary,
    pub publication_latency: M11LatencySummary,
}

#[derive(Debug, Clone, Serialize)]
pub struct M11CapacityReplayReport {
    pub schema_version: u32,
    pub artifact_id: String,
    pub mode: String,
    pub pair_count: usize,
    pub pool_count: usize,
    pub frames_per_pair: u64,
    pub total_strategy_frames: u64,
    pub reconnect_bursts: u32,
    pub total_reconnect_events: u64,
    pub exact_single_strategy_routes: u64,
    pub route_failures: u64,
    pub dependency_faults: usize,
    pub evaluations_by_symbol: BTreeMap<String, u64>,
    pub decision_owner_latency: M11LatencySummary,
    pub elapsed_ns: u64,
    pub frames_per_second: u64,
    pub rss_before_bytes: Option<u64>,
    pub rss_after_bytes: Option<u64>,
    pub rss_high_water_bytes: Option<u64>,
    pub target_cpu_class: Option<String>,
    pub fairness: M11FairnessSummary,
    pub rehydration: M11RehydrationSummary,
    pub network_io_performed: bool,
    pub external_mutations: u64,
    pub gate: String,
}

#[derive(Debug)]
struct ReplayEvaluator {
    strategy_id: StrategyId,
    symbol: String,
    evaluations: Arc<AtomicU64>,
}

impl StrategyEvaluator for ReplayEvaluator {
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
            calculation_time_us: 0,
            budget_us: 200,
        })
    }
}

pub fn run_m11_capacity_replay(
    artifact_path: impl AsRef<Path>,
    frames_per_pair_override: Option<u64>,
    target_cpu_class: Option<&str>,
) -> anyhow::Result<M11CapacityReplayReport> {
    let mut artifact = M11CapacityReplayArtifact::load(artifact_path)?;
    if let Some(frames_per_pair) = frames_per_pair_override {
        ensure!(
            frames_per_pair > 0,
            "M11 frames-per-pair override must be positive"
        );
        artifact.frames_per_pair = frames_per_pair;
    }

    let base_config = LoadedDomainConfig::load("config/strategies/usdc-wld-world-chain.v12.json")?;
    let mut strategies = Vec::with_capacity(artifact.pairs.len());
    let mut counters = BTreeMap::new();
    let mut evaluators = Vec::with_capacity(artifact.pairs.len());
    for (pair_index, pair) in artifact.pairs.iter().enumerate() {
        let strategy_id = StrategyId::new(format!("strategy:m11:{}", pair.pair_id))?;
        let evaluations = Arc::new(AtomicU64::new(0));
        counters.insert(pair.symbol.clone(), Arc::clone(&evaluations));
        let evaluator = ReplayEvaluator {
            strategy_id: strategy_id.clone(),
            symbol: pair.symbol.clone(),
            evaluations,
        };
        let pool_ids = (0..pair.pool_count)
            .map(|pool_index| PoolId::new(format!("pool:m11:{}:{pool_index}", pair.pair_id)))
            .collect::<anyhow::Result<Vec<_>>>()?;
        strategies.push(CompiledHotPathStrategyPlan {
            strategy_id,
            pair_id: pair.pair_id.clone(),
            instrument_id: InstrumentId::new(format!(
                "instrument:m11:{}",
                pair.symbol.to_ascii_lowercase()
            ))?,
            symbol: pair.symbol.clone(),
            network_id: NetworkId::new(pair.network_id.clone())?,
            pool_ids,
            observe: true,
            plan: false,
            // HotPathDecisionOwner requires one structural primary. The replay
            // evaluator cannot produce candidates and the artifact independently
            // forbids network I/O and every external mutation.
            execute: pair_index == 0,
            baseline_budget_us: 200,
            domain_config: base_config.clone(),
        });
        evaluators.push(evaluator);
    }
    let plan = CompiledHotPathRuntimePlan { strategies };
    let dependencies = CompiledStrategyDependencyIndex::new(plan)?;
    let primary = evaluators.remove(0);
    let shadows = evaluators
        .into_iter()
        .map(|evaluator| Box::new(evaluator) as Box<dyn StrategyEvaluator + Send>)
        .collect();
    let mut owner = HotPathDecisionOwner::new(primary, shadows, dependencies)?;

    let rss_before_bytes = linux_status_bytes("VmRSS:");
    let expected_frames = artifact
        .frames_per_pair
        .checked_mul(artifact.pairs.len() as u64)
        .context("M11 frame count overflow")?;
    let expected_reconnect_events = u64::from(artifact.reconnect_bursts)
        .checked_mul(2)
        .and_then(|events| events.checked_mul(artifact.pairs.len() as u64))
        .context("M11 reconnect event count overflow")?;
    let mut latencies = Vec::with_capacity(
        usize::try_from(expected_frames).context("M11 frame count exceeds address space")?,
    );
    let mut exact_single_strategy_routes = 0_u64;
    let mut route_failures = 0_u64;
    let replay_started = Instant::now();

    for (pair_index, pair) in artifact.pairs.iter().enumerate() {
        let symbol: Arc<str> = Arc::from(pair.symbol.as_str());
        for burst in 0..artifact.reconnect_bursts {
            let generation = u64::from(burst) + 1;
            for event in [
                MarketEvent::FeedDisconnected {
                    symbol: Arc::clone(&symbol),
                    generation,
                    reason: "m11_capacity_replay".to_owned(),
                    observed_at: Instant::now(),
                },
                MarketEvent::FeedConnected {
                    symbol: Arc::clone(&symbol),
                    generation: generation + 1,
                    observed_at: Instant::now(),
                },
            ] {
                let summary = owner.on_market_event(event, None)?;
                if summary.routed_strategies == 1 {
                    exact_single_strategy_routes += 1;
                } else {
                    route_failures += 1;
                }
            }
        }
        let generation = u64::from(artifact.reconnect_bursts) + 1;
        for frame_index in 0..artifact.frames_per_pair {
            let update_id = ((pair_index as u64) << 48) | (frame_index + 1);
            let event = MarketEvent::BinanceTopOfBook(TopOfBook::new(
                Arc::clone(&symbol),
                update_id,
                Decimal::ONE,
                Decimal::ONE,
                Decimal::ONE,
                Decimal::ONE,
                None,
                None,
                Instant::now(),
                1,
                generation,
            )?);
            let started = Instant::now();
            let summary = owner.on_market_event(event, None)?;
            let elapsed = started.elapsed().as_nanos();
            latencies.push(u64::try_from(elapsed).unwrap_or(u64::MAX));
            if summary.routed_strategies == 1
                && summary.evaluated_strategies == 1
                && summary.produced_candidates == 0
            {
                exact_single_strategy_routes += 1;
            } else {
                route_failures += 1;
            }
        }
    }
    let elapsed_ns = u64::try_from(replay_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let evaluations_by_symbol = counters
        .into_iter()
        .map(|(symbol, counter)| (symbol, counter.load(Ordering::Relaxed)))
        .collect::<BTreeMap<_, _>>();
    ensure!(
        evaluations_by_symbol
            .values()
            .all(|evaluations| *evaluations == artifact.frames_per_pair),
        "M11 evaluator counts differ from the exact per-pair frame count"
    );
    ensure!(route_failures == 0, "M11 replay had route failures");
    let dependency_faults = owner.take_dependency_faults().len();
    ensure!(
        dependency_faults == 0,
        "M11 replay degraded one or more strategy dependencies"
    );
    let fairness = exercise_fairness(&owner, &artifact)?;
    let rehydration = exercise_rehydration(&artifact)?;
    let decision_owner_latency = latency_summary(&mut latencies)?;
    let frames_per_second = if elapsed_ns == 0 {
        expected_frames
    } else {
        expected_frames
            .saturating_mul(1_000_000_000)
            .checked_div(elapsed_ns)
            .unwrap_or(0)
    };

    let rss_after_bytes = linux_status_bytes("VmRSS:");
    let rss_high_water_bytes = linux_status_bytes("VmHWM:");
    let gate = match target_cpu_class {
        Some(cpu_class) => {
            ensure!(
                cpu_class == "c4-highcpu-8",
                "M11 target evidence requires c4-highcpu-8"
            );
            ensure!(
                rss_before_bytes.is_some()
                    && rss_after_bytes.is_some()
                    && rss_high_water_bytes.is_some(),
                "M11 target evidence requires Linux RSS and high-water metrics"
            );
            "target_c4_replay_ready"
        }
        None => "local_replay_ready_target_c4_required",
    };

    Ok(M11CapacityReplayReport {
        schema_version: SUPPORTED_SCHEMA_VERSION,
        artifact_id: artifact.artifact_id,
        mode: artifact.mode,
        pair_count: artifact.pairs.len(),
        pool_count: artifact.pairs.iter().map(|pair| pair.pool_count).sum(),
        frames_per_pair: artifact.frames_per_pair,
        total_strategy_frames: expected_frames,
        reconnect_bursts: artifact.reconnect_bursts,
        total_reconnect_events: expected_reconnect_events,
        exact_single_strategy_routes,
        route_failures,
        dependency_faults,
        evaluations_by_symbol,
        decision_owner_latency,
        elapsed_ns,
        frames_per_second,
        rss_before_bytes,
        rss_after_bytes,
        rss_high_water_bytes,
        target_cpu_class: target_cpu_class.map(str::to_owned),
        fairness,
        rehydration,
        network_io_performed: false,
        external_mutations: 0,
        gate: gate.to_owned(),
    })
}

fn exercise_rehydration(
    artifact: &M11CapacityReplayArtifact,
) -> anyhow::Result<M11RehydrationSummary> {
    let captured = [
        decode_hex("slot0", &artifact.rehydration_fixture.slot0)?,
        decode_hex("liquidity", &artifact.rehydration_fixture.liquidity)?,
        decode_hex("tick_spacing", &artifact.rehydration_fixture.tick_spacing)?,
    ];
    ensure!(
        decode_v3_core_head(&captured[..2]).is_err(),
        "partial M11 hydration batch was unexpectedly publishable"
    );

    let cycles = artifact.reconnect_bursts.saturating_add(1);
    let pool_count = artifact
        .pairs
        .iter()
        .map(|pair| pair.pool_count)
        .sum::<usize>();
    let sample_count = pool_count
        .checked_mul(cycles as usize)
        .context("M11 hydration sample count overflow")?;
    let mut materialization_latencies = Vec::with_capacity(sample_count);
    let mut decode_latencies = Vec::with_capacity(sample_count);
    let mut build_latencies = Vec::with_capacity(sample_count);
    let mut publication_latencies = Vec::with_capacity(sample_count);
    let mut pool_publications = 0;

    for _ in 0..cycles {
        let mut published = Vec::with_capacity(pool_count);
        for _ in 0..pool_count {
            let started = Instant::now();
            let batch = captured.clone();
            materialization_latencies
                .push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));

            let started = Instant::now();
            let decoded = decode_v3_core_head(&batch)?;
            decode_latencies.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
            ensure!(
                !decoded.sqrt_price_x96.is_zero()
                    && decoded.liquidity > 0
                    && decoded.tick_spacing > 0,
                "captured M11 hydration batch is not quotable"
            );

            let started = Instant::now();
            let pool = ClmmPool::new(
                artifact.rehydration_fixture.fee_pips,
                decoded.tick_spacing,
                decoded.sqrt_price_x96,
                decoded.tick,
                decoded.liquidity,
            )?;
            build_latencies.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));

            let started = Instant::now();
            published.push(pool);
            publication_latencies
                .push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        }
        ensure!(
            published.len() == pool_count,
            "partial M11 hydration cycle cannot become ready"
        );
        pool_publications += published.len();
    }
    ensure!(
        pool_publications == sample_count,
        "M11 hydration publication count is incomplete"
    );
    ensure!(
        pool_publications >= 100,
        "M11 hydration percentile cohort must contain at least 100 pools"
    );

    Ok(M11RehydrationSummary {
        cycles,
        pool_publications,
        partial_batches_rejected: 1,
        captured_batch_materialization_latency: latency_summary(&mut materialization_latencies)?,
        decode_latency: latency_summary(&mut decode_latencies)?,
        pool_build_latency: latency_summary(&mut build_latencies)?,
        publication_latency: latency_summary(&mut publication_latencies)?,
    })
}

fn exercise_fairness(
    owner: &HotPathDecisionOwner<ReplayEvaluator>,
    artifact: &M11CapacityReplayArtifact,
) -> anyhow::Result<M11FairnessSummary> {
    let mut strategy_ids = owner
        .dependencies()
        .plan()
        .strategies
        .iter()
        .map(|strategy| strategy.strategy_id.clone())
        .collect::<Vec<_>>();
    strategy_ids.sort();
    let mut scheduler =
        FairLatestOnlySizingScheduler::new(strategy_ids.clone(), artifact.maximum_sizing_workers)?;
    for strategy_id in &strategy_ids {
        scheduler.submit(strategy_id, 0_u64)?;
    }

    let mut running = Vec::new();
    let mut maximum_observed_running = 0;
    let mut maximum_retained_work = scheduler.total_retained_work();
    while running.len() < artifact.maximum_sizing_workers {
        let (strategy_id, _) = scheduler
            .take_ready()
            .context("M11 scheduler did not fill its worker bound")?;
        running.push(strategy_id);
        maximum_observed_running = maximum_observed_running.max(scheduler.running());
    }
    let noisy = running[0].clone();
    for generation in 1..=artifact.frames_per_pair {
        scheduler.submit(&noisy, generation)?;
        maximum_retained_work = maximum_retained_work.max(scheduler.total_retained_work());
    }

    let mut unique_before_noisy_repeat = running.iter().cloned().collect::<BTreeSet<_>>();
    let mut dispatches = running.clone();
    let mut completion_index = 0;
    loop {
        let completed = dispatches[completion_index].clone();
        scheduler.complete(&completed)?;
        completion_index += 1;
        let Some((strategy_id, _)) = scheduler.take_ready() else {
            if completion_index == dispatches.len() {
                break;
            }
            continue;
        };
        maximum_observed_running = maximum_observed_running.max(scheduler.running());
        maximum_retained_work = maximum_retained_work.max(scheduler.total_retained_work());
        if strategy_id == noisy {
            break;
        }
        unique_before_noisy_repeat.insert(strategy_id.clone());
        dispatches.push(strategy_id);
    }

    ensure!(
        unique_before_noisy_repeat.len() == strategy_ids.len(),
        "noisy M11 strategy repeated before every quiet strategy was dispatched"
    );
    ensure!(
        maximum_observed_running <= artifact.maximum_sizing_workers,
        "M11 scheduler exceeded its worker bound"
    );
    ensure!(
        maximum_retained_work <= strategy_ids.len() + artifact.maximum_sizing_workers,
        "M11 scheduler exceeded one running plus one pending item per strategy"
    );

    Ok(M11FairnessSummary {
        maximum_workers: artifact.maximum_sizing_workers,
        maximum_observed_running,
        maximum_retained_work,
        unique_strategies_before_noisy_repeat: unique_before_noisy_repeat.len(),
        noisy_replacements: scheduler.replacements(&noisy)?,
    })
}

fn latency_summary(samples: &mut [u64]) -> anyhow::Result<M11LatencySummary> {
    ensure!(!samples.is_empty(), "M11 latency sample is empty");
    samples.sort_unstable();
    Ok(M11LatencySummary {
        samples: samples.len(),
        p50_ns: percentile(samples, 50),
        p95_ns: percentile(samples, 95),
        p99_ns: percentile(samples, 99),
        maximum_ns: *samples.last().expect("latency sample is non-empty"),
    })
}

fn percentile(samples: &[u64], percentile: usize) -> u64 {
    let rank = samples
        .len()
        .saturating_mul(percentile)
        .saturating_add(99)
        .checked_div(100)
        .unwrap_or(1)
        .max(1);
    samples[rank.saturating_sub(1).min(samples.len() - 1)]
}

fn linux_status_bytes(field: &str) -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let kib = status
        .lines()
        .find_map(|line| line.strip_prefix(field))?
        .split_ascii_whitespace()
        .next()?
        .parse::<u64>()
        .ok()?;
    kib.checked_mul(1024)
}

fn decode_hex(name: &str, value: &str) -> anyhow::Result<Vec<u8>> {
    let encoded = value
        .strip_prefix("0x")
        .with_context(|| format!("M11 {name} is missing 0x prefix"))?;
    ensure!(encoded.len() % 2 == 0, "M11 {name} has odd hex length");
    hex::decode(encoded).with_context(|| format!("M11 {name} contains invalid hex"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ARTIFACT: &str = "config/capacity/m11-maximum-pair-replay.v1.json";

    #[test]
    fn artifact_is_exactly_maximum_size_and_strictly_read_only() {
        let artifact = M11CapacityReplayArtifact::load(ARTIFACT).unwrap();
        assert_eq!(artifact.pairs.len(), MAXIMUM_PAIR_COUNT);
        assert_eq!(artifact.frames_per_pair, 100_000);
        assert!(!artifact.network_io_enabled);
        assert!(!artifact.external_mutation_authorized);
    }

    #[test]
    fn replay_routes_exactly_and_noisy_pair_cannot_starve_quiet_pairs() {
        let report = run_m11_capacity_replay(ARTIFACT, Some(1_000), None).unwrap();
        assert_eq!(report.total_strategy_frames, 20_000);
        assert_eq!(report.route_failures, 0);
        assert_eq!(report.dependency_faults, 0);
        assert_eq!(report.fairness.unique_strategies_before_noisy_repeat, 20);
        assert_eq!(report.fairness.maximum_observed_running, 4);
        assert_eq!(report.rehydration.cycles, 5);
        assert_eq!(report.rehydration.pool_publications, 115);
        assert_eq!(report.rehydration.partial_batches_rejected, 1);
        assert_eq!(report.external_mutations, 0);
    }

    #[test]
    fn artifact_rejects_any_network_or_mutation_authority() {
        let mut artifact = M11CapacityReplayArtifact::load(ARTIFACT).unwrap();
        artifact.network_io_enabled = true;
        assert!(
            artifact
                .validate()
                .unwrap_err()
                .to_string()
                .contains("network I/O")
        );

        artifact.network_io_enabled = false;
        artifact.external_mutation_authorized = true;
        assert!(
            artifact
                .validate()
                .unwrap_err()
                .to_string()
                .contains("external mutation")
        );
    }

    #[test]
    fn artifact_rejects_ambiguous_routes_and_unbounded_workers() {
        let mut artifact = M11CapacityReplayArtifact::load(ARTIFACT).unwrap();
        artifact.pairs[1].symbol = artifact.pairs[0].symbol.clone();
        assert!(
            artifact
                .validate()
                .unwrap_err()
                .to_string()
                .contains("duplicate M11 symbol")
        );

        let mut artifact = M11CapacityReplayArtifact::load(ARTIFACT).unwrap();
        artifact.maximum_sizing_workers = artifact.pairs.len() + 1;
        assert!(
            artifact
                .validate()
                .unwrap_err()
                .to_string()
                .contains("maximum_sizing_workers")
        );
    }
}

use std::{sync::Mutex, time::Instant};

use serde::Serialize;

const ROUNDS: usize = 32;
const ITERATIONS_PER_ROUND: u32 = 262_144;
static BENCHMARK_OWNER: Mutex<()> = Mutex::new(());

#[derive(Clone, Copy, Debug, Serialize)]
struct Percentiles {
    p50_ns: f64,
    p95_ns: f64,
    p99_ns: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct PairedBenchmarkReport {
    schema_version: u8,
    label: String,
    control: String,
    candidate: String,
    rounds: usize,
    iterations_per_provider: u64,
    maximum_ratio: f64,
    control_latency: Percentiles,
    candidate_latency: Percentiles,
    p95_ratio: f64,
    p99_ratio: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct AbsoluteBenchmarkReport {
    schema_version: u8,
    label: String,
    rounds: usize,
    iterations: u64,
    maximum_p95_ns: f64,
    maximum_p99_ns: f64,
    latency: Percentiles,
}

pub fn assert_absolute_latency_with_work<O>(
    label: &str,
    maximum_p95_ns: f64,
    maximum_p99_ns: f64,
    rounds: usize,
    iterations_per_round: u32,
    mut operation: O,
) -> AbsoluteBenchmarkReport
where
    O: FnMut(),
{
    assert!(rounds >= 4, "absolute benchmark requires four rounds");
    assert!(iterations_per_round > 0, "absolute benchmark work is zero");
    let _owner = BENCHMARK_OWNER
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for _ in 0..10_000 {
        operation();
    }
    let latency = percentiles(
        (0..rounds)
            .map(|_| measure(&mut operation, iterations_per_round))
            .collect(),
    );
    let report = AbsoluteBenchmarkReport {
        schema_version: 1,
        label: label.to_owned(),
        rounds,
        iterations: u64::from(iterations_per_round) * rounds as u64,
        maximum_p95_ns,
        maximum_p99_ns,
        latency,
    };
    eprintln!(
        "ABSOLUTE_BENCHMARK_JSON={}",
        serde_json::to_string(&report).expect("absolute benchmark report serializes"),
    );
    assert!(
        latency.p95_ns <= maximum_p95_ns,
        "{label} p95 exceeded budget"
    );
    assert!(
        latency.p99_ns <= maximum_p99_ns,
        "{label} p99 exceeded budget"
    );
    report
}

pub(crate) fn assert_paired_non_regression<U, P>(
    label: &str,
    maximum_ratio: f64,
    mut uniswap: U,
    mut pancake: P,
) where
    U: FnMut(),
    P: FnMut(),
{
    assert_named_paired_non_regression(
        label,
        maximum_ratio,
        "uniswap_v3",
        "pancake_swap_v3",
        &mut uniswap,
        &mut pancake,
    );
}

pub(crate) fn assert_named_paired_non_regression<C, N>(
    label: &str,
    maximum_ratio: f64,
    control_name: &str,
    candidate_name: &str,
    control: C,
    candidate: N,
) -> PairedBenchmarkReport
where
    C: FnMut(),
    N: FnMut(),
{
    assert_named_paired_non_regression_with_work(
        label,
        maximum_ratio,
        control_name,
        candidate_name,
        ROUNDS,
        ITERATIONS_PER_ROUND,
        control,
        candidate,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn assert_named_paired_non_regression_with_work<C, N>(
    label: &str,
    maximum_ratio: f64,
    control_name: &str,
    candidate_name: &str,
    rounds: usize,
    iterations_per_round: u32,
    mut control: C,
    mut candidate: N,
) -> PairedBenchmarkReport
where
    C: FnMut(),
    N: FnMut(),
{
    assert!(
        rounds >= 4,
        "paired benchmark requires at least four rounds"
    );
    assert!(
        iterations_per_round > 0,
        "paired benchmark requires at least one iteration per round"
    );
    let _owner = BENCHMARK_OWNER
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for _ in 0..10_000 {
        control();
        candidate();
    }

    let mut control_rounds = Vec::with_capacity(rounds);
    let mut candidate_rounds = Vec::with_capacity(rounds);
    for round in 0..rounds {
        if round % 2 == 0 {
            control_rounds.push(measure(&mut control, iterations_per_round));
            candidate_rounds.push(measure(&mut candidate, iterations_per_round));
        } else {
            candidate_rounds.push(measure(&mut candidate, iterations_per_round));
            control_rounds.push(measure(&mut control, iterations_per_round));
        }
    }
    let control_latency = percentiles(control_rounds);
    let candidate_latency = percentiles(candidate_rounds);
    let p95_ratio = candidate_latency.p95_ns / control_latency.p95_ns;
    let p99_ratio = candidate_latency.p99_ns / control_latency.p99_ns;
    let report = PairedBenchmarkReport {
        schema_version: 1,
        label: label.to_owned(),
        control: control_name.to_owned(),
        candidate: candidate_name.to_owned(),
        rounds,
        iterations_per_provider: u64::from(iterations_per_round) * rounds as u64,
        maximum_ratio,
        control_latency,
        candidate_latency,
        p95_ratio,
        p99_ratio,
    };
    eprintln!(
        "PAIRED_BENCHMARK_JSON={}",
        serde_json::to_string(&report).expect("paired benchmark report serializes"),
    );
    assert!(
        p95_ratio <= maximum_ratio,
        "{label} p95 ratio {p95_ratio:.4} exceeds {maximum_ratio:.2}"
    );
    assert!(
        p99_ratio <= maximum_ratio,
        "{label} p99 ratio {p99_ratio:.4} exceeds {maximum_ratio:.2}"
    );
    report
}

fn measure(operation: &mut impl FnMut(), iterations: u32) -> f64 {
    let started = Instant::now();
    for _ in 0..iterations {
        operation();
    }
    started.elapsed().as_nanos() as f64 / f64::from(iterations)
}

fn percentiles(mut samples: Vec<f64>) -> Percentiles {
    samples.sort_by(f64::total_cmp);
    Percentiles {
        p50_ns: percentile(&samples, 50),
        p95_ns: percentile(&samples, 95),
        p99_ns: percentile(&samples, 99),
    }
}

fn percentile(samples: &[f64], percentile: usize) -> f64 {
    let rank = (percentile * samples.len()).div_ceil(100).saturating_sub(1);
    samples[rank]
}

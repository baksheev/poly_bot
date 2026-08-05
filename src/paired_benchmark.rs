use std::{sync::Mutex, time::Instant};

const ROUNDS: usize = 32;
const ITERATIONS_PER_ROUND: u32 = 32_768;
static BENCHMARK_OWNER: Mutex<()> = Mutex::new(());

#[derive(Clone, Copy, Debug)]
struct Percentiles {
    p50: f64,
    p95: f64,
    p99: f64,
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
    let _owner = BENCHMARK_OWNER.lock().expect("paired benchmark mutex");
    for _ in 0..10_000 {
        uniswap();
        pancake();
    }

    let mut uniswap_rounds = Vec::with_capacity(ROUNDS);
    let mut pancake_rounds = Vec::with_capacity(ROUNDS);
    for round in 0..ROUNDS {
        if round % 2 == 0 {
            uniswap_rounds.push(measure(&mut uniswap));
            pancake_rounds.push(measure(&mut pancake));
        } else {
            pancake_rounds.push(measure(&mut pancake));
            uniswap_rounds.push(measure(&mut uniswap));
        }
    }
    let uniswap = percentiles(uniswap_rounds);
    let pancake = percentiles(pancake_rounds);
    let p95_ratio = pancake.p95 / uniswap.p95;
    let p99_ratio = pancake.p99 / uniswap.p99;
    eprintln!(
        "{label} rounds={ROUNDS} iterations_per_provider={} uniswap_p50_ns={:.1} uniswap_p95_ns={:.1} uniswap_p99_ns={:.1} pancake_p50_ns={:.1} pancake_p95_ns={:.1} pancake_p99_ns={:.1} p95_ratio={p95_ratio:.4} p99_ratio={p99_ratio:.4}",
        u64::from(ITERATIONS_PER_ROUND) * ROUNDS as u64,
        uniswap.p50,
        uniswap.p95,
        uniswap.p99,
        pancake.p50,
        pancake.p95,
        pancake.p99,
    );
    assert!(
        p95_ratio <= maximum_ratio,
        "{label} p95 ratio {p95_ratio:.4} exceeds {maximum_ratio:.2}"
    );
    assert!(
        p99_ratio <= maximum_ratio,
        "{label} p99 ratio {p99_ratio:.4} exceeds {maximum_ratio:.2}"
    );
}

fn measure(operation: &mut impl FnMut()) -> f64 {
    let started = Instant::now();
    for _ in 0..ITERATIONS_PER_ROUND {
        operation();
    }
    started.elapsed().as_nanos() as f64 / f64::from(ITERATIONS_PER_ROUND)
}

fn percentiles(mut samples: Vec<f64>) -> Percentiles {
    samples.sort_by(f64::total_cmp);
    Percentiles {
        p50: percentile(&samples, 50),
        p95: percentile(&samples, 95),
        p99: percentile(&samples, 99),
    }
}

fn percentile(samples: &[f64], percentile: usize) -> f64 {
    let rank = (percentile * samples.len()).div_ceil(100).saturating_sub(1);
    samples[rank]
}

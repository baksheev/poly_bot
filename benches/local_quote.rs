use std::{hint::black_box, time::Instant};

use alloy_primitives::U256;
use arb_bot::dex::clmm::ClmmPool;
use uniswap_v3_math::tick_math::get_sqrt_ratio_at_tick;

const ITERATIONS: u32 = 2_000_000;

fn main() {
    let pool = full_range_pool();
    let prepared_exact_in_zero_for_one = pool.prepare_exact_input_curve(true).unwrap();
    let prepared_exact_in_one_for_zero = pool.prepare_exact_input_curve(false).unwrap();
    let prepared_exact_out_zero_for_one = pool.prepare_exact_output_curve(true).unwrap();
    let prepared_exact_out_one_for_zero = pool.prepare_exact_output_curve(false).unwrap();
    measure("exact_in_no_cross_zero_for_one", || {
        pool.quote_exact_in_amount_out(true, black_box(U256::from(20_000_000_u64)))
            .unwrap()
    });
    measure("exact_in_no_cross_one_for_zero", || {
        pool.quote_exact_in_amount_out(false, black_box(U256::from(20_000_000_u64)))
            .unwrap()
    });
    measure("exact_out_no_cross_zero_for_one", || {
        pool.quote_exact_out_amount_in(true, black_box(U256::from(19_000_000_u64)))
            .unwrap()
    });
    measure("exact_out_no_cross_one_for_zero", || {
        pool.quote_exact_out_amount_in(false, black_box(U256::from(19_000_000_u64)))
            .unwrap()
    });
    measure("prepared_exact_in_no_cross_zero_for_one", || {
        prepared_exact_in_zero_for_one
            .quote(black_box(U256::from(20_000_000_u64)))
            .unwrap()
    });
    measure("prepared_exact_in_no_cross_one_for_zero", || {
        prepared_exact_in_one_for_zero
            .quote(black_box(U256::from(20_000_000_u64)))
            .unwrap()
    });
    measure("prepared_exact_out_no_cross_zero_for_one", || {
        prepared_exact_out_zero_for_one
            .quote(black_box(U256::from(19_000_000_u64)))
            .unwrap()
    });
    measure("prepared_exact_out_no_cross_one_for_zero", || {
        prepared_exact_out_one_for_zero
            .quote(black_box(U256::from(19_000_000_u64)))
            .unwrap()
    });

    let sparse = sparse_pool();
    let prepared_sparse = sparse.prepare_exact_output_curve(true).unwrap();
    let above_capacity = prepared_sparse.specified_capacity() + U256::ONE;
    measure_iterations("iterative_exact_out_above_sparse_capacity", 20_000, || {
        sparse
            .quote_exact_out_amount_in(true, black_box(above_capacity))
            .is_err()
    });
    measure("prepared_exact_out_above_sparse_capacity", || {
        prepared_sparse.quote(black_box(above_capacity)).is_err()
    });
}

fn measure<T>(label: &str, mut quote: impl FnMut() -> T) {
    measure_iterations(label, ITERATIONS, &mut quote);
}

fn measure_iterations<T>(label: &str, iterations: u32, mut quote: impl FnMut() -> T) {
    for _ in 0..10_000 {
        black_box(quote());
    }
    let started = Instant::now();
    for _ in 0..iterations {
        black_box(quote());
    }
    let elapsed = started.elapsed();
    let ns_per_quote = elapsed.as_nanos() as f64 / f64::from(iterations);
    println!("{label}: {ns_per_quote:.1} ns/quote ({iterations} iterations)");
}

fn full_range_pool() -> ClmmPool {
    let liquidity = 10_000_000_000_000_000_000_u128;
    let mut pool =
        ClmmPool::new(3_000, 60, get_sqrt_ratio_at_tick(0).unwrap(), 0, liquidity).unwrap();
    let liquidity_net = i128::try_from(liquidity).unwrap();
    pool.set_tick(-887_220, liquidity, liquidity_net).unwrap();
    pool.set_tick(887_220, liquidity, -liquidity_net).unwrap();
    pool
}

fn sparse_pool() -> ClmmPool {
    let liquidity = 1_000_000_u128;
    let mut pool =
        ClmmPool::new(3_000, 10, get_sqrt_ratio_at_tick(0).unwrap(), 0, liquidity).unwrap();
    pool.set_tick(-100_000, liquidity, i128::try_from(liquidity).unwrap())
        .unwrap();
    pool.set_tick(100_000, liquidity, -i128::try_from(liquidity).unwrap())
        .unwrap();
    pool
}

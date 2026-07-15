use std::{hint::black_box, time::Instant};

use alloy_primitives::U256;
use arb_bot::dex::clmm::ClmmPool;
use uniswap_v3_math::tick_math::get_sqrt_ratio_at_tick;

const ITERATIONS: u32 = 2_000_000;

fn main() {
    let pool = full_range_pool();
    measure("exact_in_no_cross_zero_for_one", || {
        pool.quote_exact_in_amount_out(true, black_box(U256::from(20_000_000_u64)))
            .unwrap()
    });
    measure("exact_in_no_cross_one_for_zero", || {
        pool.quote_exact_in_amount_out(false, black_box(U256::from(20_000_000_u64)))
            .unwrap()
    });
}

fn measure(label: &str, mut quote: impl FnMut() -> U256) {
    for _ in 0..10_000 {
        black_box(quote());
    }
    let started = Instant::now();
    for _ in 0..ITERATIONS {
        black_box(quote());
    }
    let elapsed = started.elapsed();
    let ns_per_quote = elapsed.as_nanos() as f64 / f64::from(ITERATIONS);
    println!("{label}: {ns_per_quote:.1} ns/quote ({ITERATIONS} iterations)");
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

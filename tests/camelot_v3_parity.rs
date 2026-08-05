use std::{collections::BTreeMap, hint::black_box, str::FromStr, time::Instant};

use alloy_primitives::{Address, B256, U256, keccak256};
use anyhow::{Context, ensure};
use arb_bot::{
    chain::rpc::{CanonicalBlock, EthCall, JsonRpcClient, TransactionCall},
    dex::{
        calldata::{
            camelot_v3_exact_input_single, camelot_v3_quote_exact_input_single,
            camelot_v3_quote_exact_output_single, decode_camelot_v3_quote,
        },
        clmm::ClmmPool,
        events::{build_log_filters, camelot_fee_topic, v3_swap_topic},
        hydration::{DexHydrator, PoolIdentity},
        mirror::{DexMirror, LogApplyResult},
    },
    domain::config::{
        CamelotV3Config, CamelotV3PoolConfig, DexProvider, DomainSnapshot, LoadedDomainConfig,
    },
};

const FACTORY: &str = "0x1a3c9B1d2F0529D97f2afC5136Cc23e58f1FD35B";
const POOL_DEPLOYER: &str = "0x6Dd3FB9653B10e806650F107C3B5A0a6fF974F65";
const QUOTER: &str = "0x0Fc73040b26E9bC8514fA028D998E73A254Fa76E";
const ROUTER: &str = "0x1F721E2E82F6676FCE4eA07A5958cF098D339e18";
const POOL: &str = "0xfae2ae0a9f87fd35b5b0e24b47bac796a7eefea1";
const PINNED_BLOCK_NUMBER: u64 = 491_383_703;
const PINNED_BLOCK_HASH: &str =
    "0xb74c400c8e68aeca18ece2ca02adf0aca8185f1e36a5c12a6dd2e7279cb2cc43";
const PINNED_PARENT_HASH: &str =
    "0xe4e922193bb4c86a46760ec9dae60112796e7f4e7371bcb80ad7ba62a1611d8f";
const TRANSITION_BLOCK_NUMBER: u64 = 491_426_734;
const TRANSITION_BLOCK_HASH: &str =
    "0x2f474c93b25d6c52a6b3114ebccdde3d3ce010e5ccdac659922336289feeca41";
const TRANSITION_PARENT_HASH: &str =
    "0x4804f5f097a27247a57725ca5c6f0531b3d568d7926439da66abfe5455336960";

#[tokio::test]
#[ignore = "explicit Camelot V3 archival-RPC amount and fee parity gate"]
async fn camelot_v3_arb_usdc_matches_quoter_at_reviewed_block() -> anyhow::Result<()> {
    let (snapshot, token_a, token_b) = reviewed_snapshot()?;
    let endpoint = std::env::var("ARBITRUM_RPC_URL")
        .unwrap_or_else(|_| "https://arbitrum-one.public.blastapi.io".to_owned());
    let rpc = JsonRpcClient::new(endpoint)?;
    let block = CanonicalBlock {
        number: PINNED_BLOCK_NUMBER,
        hash: B256::from_str(PINNED_BLOCK_HASH)?,
        parent_hash: B256::from_str(PINNED_PARENT_HASH)?,
    };
    let hydrated = DexHydrator::new(&rpc).hydrate_at(&snapshot, block).await?;
    let pool = hydrated
        .pools
        .iter()
        .find(|pool| matches!(pool.identity, PoolIdentity::CamelotV3 { .. }))
        .context("reviewed Camelot V3 pool was not hydrated")?;
    let fee = pool
        .camelot_fee
        .as_ref()
        .context("Camelot directional fee state was not hydrated")?;
    ensure!(
        fee.envelope.last_timestamp - fee.envelope.first_timestamp == 2,
        "Camelot fee horizon differs from configuration"
    );
    let current = fee.state.fees_at(fee.state.head_timestamp)?;
    let adaptive_inputs = fee.state.projected_averages_at(fee.state.head_timestamp)?;
    let cumulative = rpc
        .eth_call_batch(
            &[EthCall {
                to: Address::from_str(POOL)?,
                data: get_timepoints_zero_and_window(),
            }],
            block,
        )
        .await?
        .pop()
        .context("Camelot getTimepoints returned no result")?;
    let volatility = decode_dynamic_u256_pair(&cumulative, 2)?;
    let volume = decode_dynamic_u256_pair(&cumulative, 3)?;
    let on_chain_volatility_average = (volatility.0 - volatility.1) / U256::from(86_400_u32);
    let on_chain_volume_average =
        (volume.0 + U256::from(fee.state.volume_per_liquidity_in_block) - volume.1) >> 57;
    assert_eq!(
        U256::from(adaptive_inputs.0),
        on_chain_volatility_average,
        "local Camelot volatility average differs from pool.getTimepoints"
    );
    assert_eq!(
        adaptive_inputs.1, on_chain_volume_average,
        "local Camelot volume average differs from pool.getTimepoints"
    );
    eprintln!(
        "Camelot fee state head={} latest={} global={:?} projected={:?} adaptive_inputs=({},{}) chain_inputs=({},{}) envelope={:?} configs=({:?}, {:?}) volume={}",
        fee.state.head_timestamp,
        fee.state.timepoints[&fee.state.index].block_timestamp,
        fee.state.current_fees,
        current,
        adaptive_inputs.0,
        adaptive_inputs.1,
        on_chain_volatility_average,
        on_chain_volume_average,
        fee.envelope,
        fee.state.zero_for_one_config,
        fee.state.one_for_zero_config,
        fee.state.volume_per_liquidity_in_block,
    );
    let mut local_pool = pool.pool.clone();
    local_pool.set_algebra_directional_fees(
        u32::from(current.zero_for_one),
        u32::from(current.one_for_zero),
    )?;
    if std::env::var("CAMELOT_REAL_POOL_BENCHMARK").as_deref() == Ok("1") {
        run_real_pool_benchmarks(&local_pool)?;
    }
    for (zero_for_one, input_limit, output_limit) in [
        (
            true,
            U256::from(160_u8) * U256::from(10_u64).pow(U256::from(18_u8)),
            U256::from(200_000_000_u64),
        ),
        (
            false,
            U256::from(200_000_000_u64),
            U256::from(160_u8) * U256::from(10_u64).pow(U256::from(18_u8)),
        ),
    ] {
        let exact_input =
            local_pool.prepare_exact_input_curve_bounded(zero_for_one, input_limit)?;
        for boundary in exact_input.specified_boundaries() {
            for amount in adjacent_amounts(boundary, exact_input.specified_capacity()) {
                assert_eq!(
                    exact_input.quote(amount)?,
                    local_pool.quote_exact_in_amount_out(zero_for_one, amount)?,
                    "Camelot prepared exact-input boundary mismatch"
                );
            }
        }
        let exact_output =
            local_pool.prepare_exact_output_curve_bounded(zero_for_one, output_limit)?;
        for boundary in exact_output.specified_boundaries() {
            for amount in adjacent_amounts(boundary, exact_output.specified_capacity()) {
                assert_eq!(
                    exact_output.quote(amount)?,
                    local_pool.quote_exact_out_amount_in(zero_for_one, amount)?,
                    "Camelot prepared exact-output boundary mismatch"
                );
            }
        }
    }
    let quoter = Address::from_str(QUOTER)?;
    let arb_unit = U256::from(10_u64).pow(U256::from(18_u8));
    let exact_inputs = [
        (token_a, token_b, U256::from(6_000_000_u64)),
        (token_a, token_b, U256::from(50_000_000_u64)),
        (token_a, token_b, U256::from(200_000_000_u64)),
        (token_b, token_a, U256::from(5_u8) * arb_unit),
        (token_b, token_a, U256::from(40_u8) * arb_unit),
        (token_b, token_a, U256::from(160_u8) * arb_unit),
    ];
    for (token_in, token_out, amount_in) in exact_inputs {
        let zero_for_one = token_in == pool.token0;
        let local = local_pool.quote_exact_in_amount_out(zero_for_one, amount_in)?;
        let encoded = rpc
            .eth_call_batch(
                &[EthCall {
                    to: quoter,
                    data: camelot_v3_quote_exact_input_single(token_in, token_out, amount_in)?,
                }],
                block,
            )
            .await?
            .pop()
            .context("Camelot Quoter returned no exact-input result")?;
        let (on_chain, returned_fee) = decode_camelot_v3_quote(&encoded)?;
        let expected_fee = if zero_for_one {
            current.zero_for_one
        } else {
            current.one_for_zero
        };
        assert_eq!(returned_fee, expected_fee, "exact-input fee mismatch");
        assert_eq!(
            local, on_chain,
            "exact-input amount mismatch token_in={token_in:#x} amount={amount_in} fee={returned_fee}"
        );
    }

    let exact_outputs = [
        (token_a, token_b, U256::from(5_u8) * arb_unit),
        (token_a, token_b, U256::from(40_u8) * arb_unit),
        (token_a, token_b, U256::from(160_u8) * arb_unit),
        (token_b, token_a, U256::from(6_000_000_u64)),
        (token_b, token_a, U256::from(50_000_000_u64)),
        (token_b, token_a, U256::from(200_000_000_u64)),
    ];
    for (token_in, token_out, amount_out) in exact_outputs {
        let zero_for_one = token_in == pool.token0;
        let local = local_pool.quote_exact_out_amount_in(zero_for_one, amount_out)?;
        let encoded = rpc
            .eth_call_batch(
                &[EthCall {
                    to: quoter,
                    data: camelot_v3_quote_exact_output_single(token_in, token_out, amount_out)?,
                }],
                block,
            )
            .await?
            .pop()
            .context("Camelot Quoter returned no exact-output result")?;
        let (on_chain, returned_fee) = decode_camelot_v3_quote(&encoded)?;
        let expected_fee = if zero_for_one {
            current.zero_for_one
        } else {
            current.one_for_zero
        };
        assert_eq!(returned_fee, expected_fee, "exact-output fee mismatch");
        assert_eq!(
            local, on_chain,
            "exact-output amount mismatch token_in={token_in:#x} amount={amount_out} fee={returned_fee}"
        );
    }

    let transition_block = CanonicalBlock {
        number: TRANSITION_BLOCK_NUMBER,
        hash: B256::from_str(TRANSITION_BLOCK_HASH)?,
        parent_hash: B256::from_str(TRANSITION_PARENT_HASH)?,
    };
    let transition_hydrated = DexHydrator::new(&rpc)
        .hydrate_at(&snapshot, transition_block)
        .await?;
    let transition_pool = transition_hydrated
        .pools
        .iter()
        .find(|pool| matches!(pool.identity, PoolIdentity::CamelotV3 { .. }))
        .context("transition Camelot V3 pool was not hydrated")?;
    let transition_fee = transition_pool
        .camelot_fee
        .as_ref()
        .context("transition Camelot fee state was not hydrated")?;
    let transition_current = transition_fee
        .state
        .fees_at(transition_fee.state.head_timestamp)?;
    assert_eq!(
        transition_current.zero_for_one, 104,
        "reviewed Fee transition did not hydrate"
    );
    assert_eq!(transition_current.one_for_zero, 104);
    ensure!(
        transition_current != current,
        "reviewed blocks do not exercise a fee transition"
    );
    let mut transition_local = transition_pool.pool.clone();
    transition_local.set_algebra_directional_fees(
        u32::from(transition_current.zero_for_one),
        u32::from(transition_current.one_for_zero),
    )?;
    for (token_in, token_out, amount_in) in [
        (token_a, token_b, U256::from(200_000_000_u64)),
        (token_b, token_a, U256::from(160_u8) * arb_unit),
    ] {
        let zero_for_one = token_in == transition_pool.token0;
        let local = transition_local.quote_exact_in_amount_out(zero_for_one, amount_in)?;
        let encoded = rpc
            .eth_call_batch(
                &[EthCall {
                    to: quoter,
                    data: camelot_v3_quote_exact_input_single(token_in, token_out, amount_in)?,
                }],
                transition_block,
            )
            .await?
            .pop()
            .context("Camelot transition Quoter returned no result")?;
        let (on_chain, returned_fee) = decode_camelot_v3_quote(&encoded)?;
        assert_eq!(returned_fee, 104);
        assert_eq!(local, on_chain, "Camelot fee-transition amount mismatch");
    }
    Ok(())
}

#[tokio::test]
#[ignore = "explicit Camelot V3 canonical event replay against pinned post-state"]
async fn camelot_v3_transition_events_reproduce_pinned_post_state() -> anyhow::Result<()> {
    let (snapshot, _, _) = reviewed_snapshot()?;
    let endpoint = std::env::var("ARBITRUM_RPC_URL")
        .unwrap_or_else(|_| "https://arbitrum-one.public.blastapi.io".to_owned());
    let rpc = JsonRpcClient::new(endpoint)?;
    let transition = CanonicalBlock {
        number: TRANSITION_BLOCK_NUMBER,
        hash: B256::from_str(TRANSITION_BLOCK_HASH)?,
        parent_hash: B256::from_str(TRANSITION_PARENT_HASH)?,
    };
    let (parent, parent_timestamp) = rpc
        .canonical_block_by_hash(transition.number - 1, transition.parent_hash)
        .await?;
    let transition_timestamp = u32::try_from(rpc.canonical_block_timestamp(transition).await?)
        .context("transition timestamp exceeds uint32")?;
    let parent_timestamp =
        u32::try_from(parent_timestamp).context("parent timestamp exceeds uint32")?;

    let pre = DexHydrator::new(&rpc)
        .hydrate_at(&snapshot, parent)
        .await
        .context("hydrate pre-transition state")?;
    let filters = build_log_filters(&snapshot, &pre)?;
    let mut logs = Vec::new();
    for filter in &filters {
        logs.extend(
            rpc.get_logs(filter, transition.number, transition.number)
                .await?,
        );
    }
    logs.sort_unstable_by_key(|log| log.position());
    logs.dedup_by(|right, left| {
        right.position() == left.position()
            && right.address == left.address
            && right.block_hash == left.block_hash
    });
    ensure!(
        !logs.is_empty(),
        "reviewed transition block has no Camelot events"
    );

    let mut mirror = DexMirror::new(pre)?;
    mirror
        .finish_backfill_at(parent, Some(parent_timestamp))
        .context("finish parent backfill")?;
    mirror
        .apply_head_at(transition, Some(transition_timestamp), Instant::now())
        .context("apply transition head")?;
    let mut fee_seen = false;
    let mut swap_seen = false;
    for log in &logs {
        if let LogApplyResult::Applied { kind, .. } = mirror
            .apply_log_at_timestamp(log, transition_timestamp)
            .with_context(|| {
                format!(
                    "apply transition log tx={} log={} topic={:#x}",
                    log.transaction_index, log.log_index, log.topics[0]
                )
            })?
        {
            fee_seen |= kind == "fee";
            swap_seen |= kind == "swap";
        }
    }
    ensure!(
        fee_seen && swap_seen,
        "transition does not contain Fee-before-Swap"
    );
    mirror.refresh_pool_for_publication(0)?;

    let post = DexHydrator::new(&rpc)
        .hydrate_at(&snapshot, transition)
        .await
        .context("hydrate post-transition state")?;
    let expected = post
        .pools
        .iter()
        .find(|pool| matches!(pool.identity, PoolIdentity::CamelotV3 { .. }))
        .context("post-state Camelot pool is missing")?;
    let actual = mirror.pool(0)?;
    assert_eq!(actual.pool.sqrt_price_x96, expected.pool.sqrt_price_x96);
    assert_eq!(actual.pool.tick, expected.pool.tick);
    assert_eq!(actual.pool.liquidity, expected.pool.liquidity);
    assert_eq!(
        actual.pool.directional_fee_pips(),
        expected.pool.directional_fee_pips()
    );
    let actual_ticks: BTreeMap<_, _> = actual.pool.initialized_ticks().collect();
    let expected_ticks: BTreeMap<_, _> = expected.pool.initialized_ticks().collect();
    assert_eq!(actual_ticks, expected_ticks);

    let actual_fee = actual
        .camelot_fee
        .as_ref()
        .context("mirrored Camelot fee state is missing")?;
    let expected_fee = expected
        .camelot_fee
        .as_ref()
        .context("post-state Camelot fee state is missing")?;
    assert_eq!(actual_fee.state.index, expected_fee.state.index);
    assert_eq!(
        actual_fee.state.current_fees,
        expected_fee.state.current_fees
    );
    assert_eq!(
        actual_fee.state.volume_per_liquidity_in_block,
        expected_fee.state.volume_per_liquidity_in_block
    );
    assert_eq!(actual_fee.state.tick, expected_fee.state.tick);
    assert_eq!(actual_fee.state.liquidity, expected_fee.state.liquidity);
    assert_eq!(actual_fee.envelope, expected_fee.envelope);
    eprintln!(
        "CANONICAL_EVENT_REPLAY_JSON={}",
        serde_json::json!({
            "block": transition.number,
            "logs": logs.len(),
            "fee_before_swap": true,
            "fee_zto": actual_fee.state.current_fees.zero_for_one,
            "fee_otz": actual_fee.state.current_fees.one_for_zero,
            "volume_per_liquidity_in_block": actual_fee.state.volume_per_liquidity_in_block.to_string(),
            "tick": actual.pool.tick,
            "liquidity": actual.pool.liquidity.to_string(),
            "post_state_parity": "byte_exact",
        })
    );
    Ok(())
}

#[tokio::test]
#[ignore = "explicit Camelot V3 pinned read-only router simulation gate"]
async fn camelot_v3_exact_router_call_simulates_at_reviewed_parent() -> anyhow::Result<()> {
    let endpoint = std::env::var("ARBITRUM_RPC_URL")
        .unwrap_or_else(|_| "https://arbitrum-one.public.blastapi.io".to_owned());
    let rpc = JsonRpcClient::new(endpoint)?;
    // Reconstruct the exact successful transaction
    // 0xbfbcffe0ff83c8429ce3cab943e0fec9ef4ab1c5c786a477329dba46b7c6f82b.
    // The outer multicall wraps native ETH; its sole inner call is the same
    // reviewed seven-word exactInputSingle used by the ARB/USDC route.
    let sender = Address::from_str("0x777336ae2cef9ddc261a61a97cbfb4e0aa7d1329")?;
    let inner = camelot_v3_exact_input_single(
        Address::from_str("0x82af49447d8a07e3bd95bd0d56f35241523fbab1")?,
        Address::from_str("0x7f9fbf9bdd3f4105c478b996b648fe6e828a1e98")?,
        sender,
        1_729_409_505,
        U256::from_str_radix("f8b0a10e470000", 16)?,
        U256::from_str_radix("7d2a62f9df3e14a0a", 16)?,
    )?;
    let mut calldata = vec![0xac, 0x96, 0x50, 0xd8];
    for word in [32_u64, 1, 32, inner.len() as u64] {
        calldata.extend_from_slice(&U256::from(word).to_be_bytes::<32>());
    }
    calldata.extend_from_slice(&inner);
    calldata.resize(4 + 4 * 32 + 8 * 32, 0);
    let output = rpc
        .simulate_transaction_at(
            &TransactionCall {
                from: sender,
                to: Address::from_str(ROUTER)?,
                data: calldata,
                value: U256::from_str_radix("f8b0a10e470000", 16)?,
            },
            CanonicalBlock {
                number: 265_700_003,
                hash: B256::from_str(
                    "0x385c26273b3724e38b54efdd361af3360aa9932f15dd75789234742f416c7d8a",
                )?,
                parent_hash: B256::from_str(
                    "0x216d46d3b48f70a84e7224802a7f9203acb319263b485dd66523eddea40f3d65",
                )?,
            },
        )
        .await?;
    ensure!(
        !output.is_empty(),
        "Camelot router multicall returned no output"
    );
    eprintln!(
        "CAMELOT_READ_ONLY_SIMULATION_JSON={}",
        serde_json::json!({
            "block": 265_700_003,
            "block_hash": "0x385c26273b3724e38b54efdd361af3360aa9932f15dd75789234742f416c7d8a",
            "source_transaction": "0xbfbcffe0ff83c8429ce3cab943e0fec9ef4ab1c5c786a477329dba46b7c6f82b",
            "sender": format!("{sender:#x}"),
            "router": ROUTER,
            "selector": "0xbc651188",
            "calldata_words": 7,
            "result_bytes": output.len(),
            "signing": false,
            "broadcast": false,
            "allowance_mutation": false
        })
    );
    Ok(())
}

#[tokio::test]
#[ignore = "explicit Camelot V3 ARB/USDC historical eth_call and gas-estimation gate"]
async fn camelot_v3_arb_usdc_historical_route_replays_read_only() -> anyhow::Result<()> {
    let endpoint = std::env::var("ARBITRUM_RPC_URL")
        .unwrap_or_else(|_| "https://arbitrum-one.public.blastapi.io".to_owned());
    let rpc = JsonRpcClient::new(endpoint)?;
    let source_hash =
        B256::from_str("0x42cc0e4d929640dd22e42a1d273128f3e8afe55b0dc18b63cf175462de25f6fc")?;
    let source = rpc
        .transaction_by_hash(source_hash)
        .await?
        .context("reviewed Camelot ARB/USDC transaction is unavailable")?;
    ensure!(source.block_number == Some(400_150_197));
    let parent = CanonicalBlock {
        number: 400_150_196,
        hash: B256::from_str("0x96b854e6c3708c5cda4eb46c631d511e4233276e302e84cb3cee6fa8e5c517de")?,
        parent_hash: B256::from_str(
            "0x0acaa5f32649f070402dc353162c28f7ea82f67eba97b38d882708e974489ffb",
        )?,
    };
    let router = Address::from_str(ROUTER)?;
    let pool = Address::from_str(POOL)?;
    let arb = Address::from_str("0x912ce59144191c1204e64559fe8253a0e49e6548")?;
    let usdc = Address::from_str("0xaf88d065e77c8cc2239327c5edb3a432268e5831")?;
    for address in [router, arb, usdc] {
        ensure!(
            source
                .input
                .windows(Address::len_bytes())
                .any(|window| window == address.as_slice()),
            "reviewed route calldata omits {address:#x}"
        );
    }
    let call = TransactionCall {
        from: source.from,
        to: source
            .to
            .context("reviewed transaction has no destination")?,
        data: source.input,
        value: source.value,
    };
    let output = rpc.simulate_transaction_at(&call, parent).await?;
    let estimated_gas = rpc.estimate_gas_at(&call, parent).await?;
    ensure!(estimated_gas > 0, "pinned gas estimate is zero");

    let receipt = rpc
        .transaction_receipt(source_hash)
        .await?
        .context("reviewed Camelot ARB/USDC receipt is unavailable")?;
    let fee_position = receipt
        .logs
        .iter()
        .find(|log| log.address == pool && log.topics.first() == Some(&camelot_fee_topic()))
        .and_then(|log| log.position)
        .context("reviewed receipt has no Camelot Fee")?;
    let swap_position = receipt
        .logs
        .iter()
        .find(|log| log.address == pool && log.topics.first() == Some(&v3_swap_topic()))
        .and_then(|log| log.position)
        .context("reviewed receipt has no Camelot Swap")?;
    ensure!(
        fee_position.transaction_hash == source_hash
            && swap_position.transaction_hash == source_hash
            && fee_position.transaction_index == swap_position.transaction_index
            && fee_position.log_index < swap_position.log_index,
        "reviewed receipt lacks positional Fee-before-Swap proof"
    );
    eprintln!(
        "CAMELOT_ARB_USDC_READ_ONLY_REPLAY_JSON={}",
        serde_json::json!({
            "source_transaction": format!("{source_hash:#x}"),
            "parent_block": parent.number,
            "parent_block_hash": format!("{:#x}", parent.hash),
            "router": ROUTER,
            "pool": POOL,
            "token_in": format!("{arb:#x}"),
            "token_out": format!("{usdc:#x}"),
            "fee_log_index": fee_position.log_index,
            "swap_log_index": swap_position.log_index,
            "eth_call_result_bytes": output.len(),
            "estimated_gas": estimated_gas,
            "signing": false,
            "broadcast": false,
            "allowance_mutation": false,
            "account_mutation": false
        })
    );
    Ok(())
}

fn reviewed_snapshot() -> anyhow::Result<(DomainSnapshot, Address, Address)> {
    let domain = LoadedDomainConfig::load("config/strategies/usdc-arb-arbitrum.v2.json")?;
    let mut snapshot = domain.snapshot().clone();
    let pair = &mut snapshot.pairs[0];
    pair.chain.camelot_v3_factory_address = Some(FACTORY.to_owned());
    pair.chain.camelot_v3_pool_deployer_address = Some(POOL_DEPLOYER.to_owned());
    pair.chain.camelot_v3_quoter_address = Some(QUOTER.to_owned());
    pair.chain.camelot_v3_router_address = Some(ROUTER.to_owned());
    pair.dex.allowed_providers = vec![DexProvider::CamelotV3];
    pair.dex.uniswap_v3 = None;
    pair.dex.camelot_v3 = Some(CamelotV3Config {
        pools: vec![CamelotV3PoolConfig {
            expected_address: POOL.to_owned(),
            selection_enabled: true,
            required_active_incentive: Address::ZERO.to_string(),
            expected_tick_spacing: 10,
            dynamic_fee_horizon_seconds: 2,
        }],
    });
    let token_a = Address::from_str(&pair.token_a.contract)?;
    let token_b = Address::from_str(&pair.token_b.contract)?;
    Ok((snapshot, token_a, token_b))
}

fn run_real_pool_benchmarks(camelot: &ClmmPool) -> anyhow::Result<()> {
    for (zero_for_one, maximum) in [
        (
            true,
            U256::from(160_u8) * U256::from(10_u64).pow(U256::from(18_u8)),
        ),
        (false, U256::from(200_000_000_u64)),
    ] {
        let fee = camelot.fee_pips_for_direction(zero_for_one);
        let mut uniswap = ClmmPool::new(
            fee,
            camelot.tick_spacing,
            camelot.sqrt_price_x96,
            camelot.tick,
            camelot.liquidity,
        )?;
        for (index, state) in camelot.initialized_ticks() {
            uniswap.set_tick(index, state.gross, state.net)?;
        }
        let uniswap_curve = uniswap.prepare_exact_input_curve_bounded(zero_for_one, maximum)?;
        let camelot_curve = camelot.prepare_exact_input_curve_bounded(zero_for_one, maximum)?;
        ensure!(
            uniswap_curve.segment_count() == camelot_curve.segment_count(),
            "matched real-pool curves have different segment counts"
        );
        let probe = maximum / U256::from(2_u8);
        assert_eq!(uniswap_curve.quote(probe)?, camelot_curve.quote(probe)?);
        let direction = if zero_for_one { "zto" } else { "otz" };
        assert_real_pool_paired(
            &format!("camelot_real_pool_prepared_quote_{direction}"),
            1.05,
            "matched_uniswap_v3",
            "camelot_v3",
            32,
            262_144,
            || {
                black_box(uniswap_curve.quote(black_box(probe))).unwrap();
            },
            || {
                black_box(camelot_curve.quote(black_box(probe))).unwrap();
            },
        );
        assert_real_pool_paired(
            &format!("camelot_real_pool_curve_build_{direction}"),
            1.20,
            "matched_uniswap_v3",
            "camelot_v3",
            32,
            4_096,
            || {
                black_box(
                    uniswap
                        .prepare_exact_input_curve_bounded(zero_for_one, black_box(maximum))
                        .unwrap(),
                );
            },
            || {
                black_box(
                    camelot
                        .prepare_exact_input_curve_bounded(zero_for_one, black_box(maximum))
                        .unwrap(),
                );
            },
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn assert_real_pool_paired<C, N>(
    label: &str,
    maximum_ratio: f64,
    control_name: &str,
    candidate_name: &str,
    rounds: usize,
    iterations: u32,
    mut control: C,
    mut candidate: N,
) where
    C: FnMut(),
    N: FnMut(),
{
    for _ in 0..10_000 {
        control();
        candidate();
    }
    let mut control_samples = Vec::with_capacity(rounds);
    let mut candidate_samples = Vec::with_capacity(rounds);
    for round in 0..rounds {
        if round % 2 == 0 {
            control_samples.push(measure_operation(&mut control, iterations));
            candidate_samples.push(measure_operation(&mut candidate, iterations));
        } else {
            candidate_samples.push(measure_operation(&mut candidate, iterations));
            control_samples.push(measure_operation(&mut control, iterations));
        }
    }
    control_samples.sort_by(f64::total_cmp);
    candidate_samples.sort_by(f64::total_cmp);
    let p95_index = (95 * rounds).div_ceil(100).saturating_sub(1);
    let p99_index = (99 * rounds).div_ceil(100).saturating_sub(1);
    let p95_ratio = candidate_samples[p95_index] / control_samples[p95_index];
    let p99_ratio = candidate_samples[p99_index] / control_samples[p99_index];
    eprintln!(
        "REAL_POOL_BENCHMARK_JSON={}",
        serde_json::json!({
            "schema_version": 1,
            "label": label,
            "control": control_name,
            "candidate": candidate_name,
            "rounds": rounds,
            "iterations_per_provider": u64::from(iterations) * rounds as u64,
            "maximum_ratio": maximum_ratio,
            "control_p95_ns": control_samples[p95_index],
            "control_p99_ns": control_samples[p99_index],
            "candidate_p95_ns": candidate_samples[p95_index],
            "candidate_p99_ns": candidate_samples[p99_index],
            "p95_ratio": p95_ratio,
            "p99_ratio": p99_ratio,
        })
    );
    assert!(p95_ratio <= maximum_ratio, "{label} p95 ratio failed");
    assert!(p99_ratio <= maximum_ratio, "{label} p99 ratio failed");
}

fn measure_operation(operation: &mut impl FnMut(), iterations: u32) -> f64 {
    let started = std::time::Instant::now();
    for _ in 0..iterations {
        operation();
    }
    started.elapsed().as_nanos() as f64 / f64::from(iterations)
}

fn adjacent_amounts(boundary: U256, capacity: U256) -> Vec<U256> {
    let mut values = Vec::with_capacity(3);
    if boundary > U256::ONE {
        values.push(boundary - U256::ONE);
    }
    values.push(boundary);
    if boundary < capacity {
        values.push(boundary + U256::ONE);
    }
    values
}

fn get_timepoints_zero_and_window() -> Vec<u8> {
    let selector = keccak256("getTimepoints(uint32[])".as_bytes());
    let mut data = selector[..4].to_vec();
    for value in [32_u32, 2, 0, 86_400] {
        let mut word = [0_u8; 32];
        word[28..].copy_from_slice(&value.to_be_bytes());
        data.extend_from_slice(&word);
    }
    data
}

fn decode_dynamic_u256_pair(data: &[u8], output_index: usize) -> anyhow::Result<(U256, U256)> {
    let offset_start = output_index * 32;
    let offset: usize = U256::from_be_slice(
        data.get(offset_start..offset_start + 32)
            .context("Camelot dynamic output offset is truncated")?,
    )
    .try_into()
    .context("Camelot dynamic output offset does not fit usize")?;
    let length = U256::from_be_slice(
        data.get(offset..offset + 32)
            .context("Camelot dynamic output length is truncated")?,
    );
    ensure!(
        length == U256::from(2_u8),
        "Camelot dynamic output length differs from two"
    );
    Ok((
        U256::from_be_slice(
            data.get(offset + 32..offset + 64)
                .context("Camelot first dynamic output is truncated")?,
        ),
        U256::from_be_slice(
            data.get(offset + 64..offset + 96)
                .context("Camelot second dynamic output is truncated")?,
        ),
    ))
}

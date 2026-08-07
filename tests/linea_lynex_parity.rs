use std::{collections::BTreeMap, str::FromStr, sync::Arc, time::Instant};

use alloy_primitives::{Address, B256, U256, keccak256};
use anyhow::{Context, ensure};
use arb_bot::{
    arbitrage::ArbitrageDirection as ExecutionDirection,
    chain::rpc::{CanonicalBlock, EthCall, JsonRpcClient},
    dex::{
        calldata::{
            decode_lynex_algebra_v1_9_quote, lynex_algebra_v1_9_exact_input_single,
            lynex_algebra_v1_9_quote_exact_input_single,
            lynex_algebra_v1_9_quote_exact_output_single,
        },
        events::{AdaptiveFeeReceiptProof, PoolLocator, build_log_filters},
        execution::{SwapRoute, settlement_logs_for_route},
        hydration::{DexHydrator, PoolIdentity},
        mirror::{DexMirror, LogApplyResult},
    },
    domain::config::LoadedDomainConfig,
    execution_plan::{DexRoutePlan, DexSwapPlan},
    opportunity::{ArbitrageDirection, OpportunityEngine},
    state::TopOfBook,
    wallet::WalletCall,
};
use rust_decimal::Decimal;

const FACTORY: &str = "0x622b2c98123D303ae067DB4925CD6282B3A08D0F";
const POOL_DEPLOYER: &str = "0x9A89490F1056A7BC607EC53F93b921fE666A2C48";
const QUOTER: &str = "0x851d97Fd7823E44193d227682e32234ef8CaC83e";
const ROUTER: &str = "0x3921e8cb45B17fC029A0a6dE958330ca4e583390";
const POOL: &str = "0x6e9ad0b8a41e2c148e7b0385d3ecbfdb8a216a9b";
const USDC: &str = "0x176211869cA2b568f2A7D4EE941E073a821EE1ff";
const USDT: &str = "0xA219439258ca9da29e9cC4cE5596924745e12B93";
const MULTICALL3: &str = "0xcA11bde05977b3631167028862bE2a173976CA11";
const PINNED_BLOCK_NUMBER: u64 = 31_631_634;
const PINNED_BLOCK_HASH: &str =
    "0xd5bb0a033143a1f0dd7a3df0018349bb4cf8d2ae2bb3056c8021b076f0896ed5";
const PINNED_PARENT_HASH: &str =
    "0x23c9e193be30d6f80834e400968c35aab0e460648c3082137c1dd081953bbd3f";
const TRANSITION_BLOCK_NUMBER: u64 = 31_630_242;
const TRANSITION_BLOCK_HASH: &str =
    "0x1aac8782da68d166c9557442342bd4e02c9818e19094c9f3e94e11ffe6ea5ea8";
const TRANSITION_PARENT_HASH: &str =
    "0x34a743b072d6ffcd78db0505bd49310f9ddb84ed06ad4f2529603c63718193de";
const TRANSITION_PARENT_PARENT_HASH: &str =
    "0xc6590e4d493c350323db5e58ea74ddfab57d76049d7326a3373c7babd925046f";
const TRANSITION_TIMESTAMP: u32 = 1_785_974_595;
const TRANSITION_PARENT_TIMESTAMP: u32 = 1_785_974_591;
const SIMULATION_BLOCK_NUMBER: u64 = 28_694_095;
const SIMULATION_BLOCK_HASH: &str =
    "0x46bda9a8412b54375a839207138e2aa843bf977894956280da9a07678b71e503";
const SIMULATION_PARENT_HASH: &str =
    "0x256837e35f0b9bfc9cc5edb6f163ba1aff5f40e1532e298001847b561950b84b";
const SIMULATION_SENDER: &str = "0xd82def4400793894fb175f3b1ba6e4402c92c98c";
const SIMULATION_DEADLINE: u64 = 3_000_000_000;
const TYPE_TWO_EVIDENCE_TRANSACTION: &str =
    "0x2af4f98a942fc5d1acb6c0fbd16e78e472719c11d13530042bf8a2a49fda48ab";
const OPPOSITE_DIRECTION_RECEIPT_TRANSACTION: &str =
    "0xa4bd7e210af4c0fe7640c553e4ba4ab7df1df14bd7b118efe7b8cb1c803a2f1b";

#[tokio::test]
#[ignore = "explicit Linea Lynex successful-receipt Fee-before-Swap evidence gate"]
async fn linea_lynex_successful_receipts_prove_typed_fee_and_both_swap_directions()
-> anyhow::Result<()> {
    let endpoint =
        std::env::var("LINEA_RPC_URL").unwrap_or_else(|_| "https://rpc.linea.build".to_owned());
    let rpc = JsonRpcClient::new(endpoint)?;
    ensure!(rpc.chain_id().await? == 59_144, "RPC is not Linea mainnet");
    let pool = Address::from_str(POOL)?;
    let route = SwapRoute::LynexAlgebraV1_9 {
        router: Address::from_str(ROUTER)?,
        pool,
    };
    let cases = [
        (
            TYPE_TWO_EVIDENCE_TRANSACTION,
            31_631_452_u64,
            7_u64,
            11_u64,
            true,
        ),
        (
            OPPOSITE_DIRECTION_RECEIPT_TRANSACTION,
            31_630_542_u64,
            8_u64,
            11_u64,
            false,
        ),
    ];
    for (transaction, block_number, fee_log_index, swap_log_index, zero_for_one) in cases {
        let hash = B256::from_str(transaction)?;
        let receipt = rpc
            .transaction_receipt(hash)
            .await?
            .context("reviewed Linea Lynex receipt is unavailable")?;
        ensure!(receipt.status == 1, "reviewed Lynex transaction reverted");
        assert_eq!(receipt.block_number, block_number);
        let proof = settlement_logs_for_route(&receipt, route)?;
        let fee = proof.fee.context("reviewed Lynex receipt Fee is missing")?;
        assert_eq!(fee.log_index(), fee_log_index);
        assert!(matches!(
            fee,
            AdaptiveFeeReceiptProof::LynexAlgebraV1_9(proof)
                if proof.pool == pool && proof.fee == 50
        ));
        let swap = proof
            .swap
            .context("reviewed Lynex receipt Swap is missing")?;
        assert_eq!(swap.log_index, swap_log_index);
        assert_eq!(swap.address, pool);
        assert_eq!(swap.data[0] == 0xff, zero_for_one);
        assert_eq!(swap.data[32] == 0x00, zero_for_one);
    }
    Ok(())
}

#[tokio::test]
#[ignore = "explicit Linea type-2 fee-field and receipt-accounting evidence gate"]
async fn linea_accepts_type_two_fee_fields_and_receipt_has_no_additional_l1_fee()
-> anyhow::Result<()> {
    let endpoint =
        std::env::var("LINEA_RPC_URL").unwrap_or_else(|_| "https://rpc.linea.build".to_owned());
    let rpc = JsonRpcClient::new(endpoint)?;
    let hash = B256::from_str(TYPE_TWO_EVIDENCE_TRANSACTION)?;
    let transaction = rpc
        .transaction_by_hash(hash)
        .await?
        .context("reviewed Linea type-2 transaction is unavailable")?;
    assert_eq!(transaction.chain_id, 59_144);
    assert_eq!(transaction.transaction_type, Some(2));
    assert_eq!(transaction.gas_price, Some(36_213_382));
    assert_eq!(transaction.max_fee_per_gas, Some(36_213_382));
    assert_eq!(transaction.max_priority_fee_per_gas, Some(36_213_375));
    let receipt = rpc
        .transaction_receipt(hash)
        .await?
        .context("reviewed Linea type-2 receipt is unavailable")?;
    assert_eq!(receipt.status, 1);
    assert_eq!(receipt.block_number, 31_631_452);
    assert_eq!(receipt.gas_used, 831_878);
    assert_eq!(receipt.effective_gas_price, 36_213_382);
    assert_eq!(receipt.l1_fee, 0);
    Ok(())
}

#[tokio::test]
#[ignore = "explicit Linea Lynex direct-router block-pinned eth_call/eth_estimateGas gate"]
async fn linea_lynex_direct_exact_input_simulates_both_directions_without_mutation()
-> anyhow::Result<()> {
    let endpoint =
        std::env::var("LINEA_RPC_URL").unwrap_or_else(|_| "https://rpc.linea.build".to_owned());
    let rpc = JsonRpcClient::new(endpoint)?;
    ensure!(rpc.chain_id().await? == 59_144, "RPC is not Linea mainnet");
    let block = CanonicalBlock {
        number: SIMULATION_BLOCK_NUMBER,
        hash: B256::from_str(SIMULATION_BLOCK_HASH)?,
        parent_hash: B256::from_str(SIMULATION_PARENT_HASH)?,
    };
    let sender = Address::from_str(SIMULATION_SENDER)?;
    let router = Address::from_str(ROUTER)?;
    let amount_in = U256::from(6_000_000_u64);
    let cases = [
        (USDT, USDC, 5_995_292_u64, 311_190_u64),
        (USDC, USDT, 6_004_110_u64, 315_170_u64),
    ];
    for (token_in, token_out, expected_out, expected_gas) in cases {
        let calldata = lynex_algebra_v1_9_exact_input_single(
            Address::from_str(token_in)?,
            Address::from_str(token_out)?,
            sender,
            SIMULATION_DEADLINE,
            amount_in,
            U256::ONE,
        )?;
        ensure!(&calldata[..4] == [0xbc, 0x65, 0x11, 0x88]);
        let call =
            WalletCall::validated_contract_call(router, U256::ZERO, calldata)?.rpc_call(sender);
        let result = rpc.simulate_transaction_at(&call, block).await?;
        ensure!(
            result.len() == 32,
            "Lynex direct swap result is not one ABI word"
        );
        assert_eq!(U256::from_be_slice(&result), U256::from(expected_out));
        assert_eq!(rpc.estimate_gas_at(&call, block).await?, expected_gas);
    }
    eprintln!(
        "LINEA_LYNEX_P6_SIMULATION_JSON={}",
        serde_json::json!({
            "schema_version": 1,
            "chain_id": 59144,
            "block_number": block.number,
            "block_hash": format!("{:#x}", block.hash),
            "parent_hash": format!("{:#x}", block.parent_hash),
            "sender": SIMULATION_SENDER,
            "router": ROUTER,
            "pool": POOL,
            "selector": "0xbc651188",
            "amount_in_base_units": amount_in.to_string(),
            "directions": [
                {"token_in": USDT, "token_out": USDC, "amount_out": "5995292", "estimated_gas": 311190},
                {"token_in": USDC, "token_out": USDT, "amount_out": "6004110", "estimated_gas": 315170}
            ],
            "network_calls": ["eth_call", "eth_estimateGas"],
            "signing": false,
            "broadcast": false,
            "allowance_mutation": false
        })
    );
    Ok(())
}

#[tokio::test]
#[ignore = "explicit Linea Lynex Algebra V1.9 archival-RPC amount and fee parity gate"]
async fn linea_lynex_usdc_usdt_matches_quoter_at_reviewed_block() -> anyhow::Result<()> {
    let domain = LoadedDomainConfig::load("config/strategies/usdt-usdc-linea-lynex.v2.json")?;
    let snapshot = domain.snapshot();
    let pair = &snapshot.pairs[0];
    let token_a = Address::from_str(&pair.token_a.contract)?;
    let token_b = Address::from_str(&pair.token_b.contract)?;
    ensure!(token_a == Address::from_str(USDT)? && token_b == Address::from_str(USDC)?);

    let endpoint =
        std::env::var("LINEA_RPC_URL").unwrap_or_else(|_| "https://rpc.linea.build".to_owned());
    let rpc = JsonRpcClient::new(endpoint)?;
    ensure!(rpc.chain_id().await? == 59_144, "RPC is not Linea mainnet");
    let block = CanonicalBlock {
        number: PINNED_BLOCK_NUMBER,
        hash: B256::from_str(PINNED_BLOCK_HASH)?,
        parent_hash: B256::from_str(PINNED_PARENT_HASH)?,
    };
    let hydrated = DexHydrator::new(&rpc).hydrate_at(snapshot, block).await?;
    ensure!(
        hydrated.unavailable.is_empty(),
        "reviewed Lynex pool is unavailable"
    );
    let pool = hydrated
        .pools
        .iter()
        .find(|pool| matches!(pool.identity, PoolIdentity::LynexAlgebraV1_9 { .. }))
        .context("reviewed Lynex pool was not hydrated")?;
    ensure!(
        pool.identity
            == PoolIdentity::LynexAlgebraV1_9 {
                address: Address::from_str(POOL)?,
            }
    );
    let fee = pool
        .lynex_fee
        .as_ref()
        .context("Lynex single-fee state was not hydrated")?;
    let current = fee.state.fees_at(fee.state.head_timestamp)?;
    assert_eq!(current.zero_for_one, current.one_for_zero);
    assert_eq!(
        fee.envelope.current.zero_for_one,
        fee.envelope.current.one_for_zero
    );
    assert_eq!(
        fee.envelope.maximum.zero_for_one,
        fee.envelope.maximum.one_for_zero
    );
    assert_eq!(
        fee.envelope.last_timestamp - fee.envelope.first_timestamp,
        2
    );

    let mut local_pool = pool.pool.clone();
    local_pool.set_algebra_single_fee(current.zero_for_one)?;
    for zero_for_one in [true, false] {
        let exact_input = local_pool
            .prepare_exact_input_curve_bounded(zero_for_one, U256::from(200_000_000_u64))?;
        for boundary in exact_input.specified_boundaries() {
            for amount in adjacent_amounts(boundary, exact_input.specified_capacity()) {
                assert_eq!(
                    exact_input.quote(amount)?,
                    local_pool.quote_exact_in_amount_out(zero_for_one, amount)?,
                    "Lynex prepared exact-input boundary mismatch"
                );
            }
        }
        let exact_output = local_pool
            .prepare_exact_output_curve_bounded(zero_for_one, U256::from(200_000_000_u64))?;
        for boundary in exact_output.specified_boundaries() {
            for amount in adjacent_amounts(boundary, exact_output.specified_capacity()) {
                assert_eq!(
                    exact_output.quote(amount)?,
                    local_pool.quote_exact_out_amount_in(zero_for_one, amount)?,
                    "Lynex prepared exact-output boundary mismatch"
                );
            }
        }
    }

    let quoter = Address::from_str(QUOTER)?;
    let amounts = [6_000_000_u64, 50_000_000, 200_000_000];
    for (token_in, token_out) in [(token_a, token_b), (token_b, token_a)] {
        let zero_for_one = token_in == pool.token0;
        for amount in amounts.map(U256::from) {
            let local = local_pool.quote_exact_in_amount_out(zero_for_one, amount)?;
            let encoded = rpc
                .eth_call_batch(
                    &[EthCall {
                        to: quoter,
                        data: lynex_algebra_v1_9_quote_exact_input_single(
                            token_in, token_out, amount,
                        )?,
                    }],
                    block,
                )
                .await?
                .pop()
                .context("Lynex Quoter returned no exact-input result")?;
            let (on_chain, returned_fee) = decode_lynex_algebra_v1_9_quote(&encoded)?;
            assert_eq!(returned_fee, current.zero_for_one);
            assert_eq!(
                local, on_chain,
                "exact-input amount mismatch token_in={token_in:#x} amount={amount}"
            );

            let local = local_pool.quote_exact_out_amount_in(zero_for_one, amount)?;
            let encoded = rpc
                .eth_call_batch(
                    &[EthCall {
                        to: quoter,
                        data: lynex_algebra_v1_9_quote_exact_output_single(
                            token_in, token_out, amount,
                        )?,
                    }],
                    block,
                )
                .await?
                .pop()
                .context("Lynex Quoter returned no exact-output result")?;
            let (on_chain, returned_fee) = decode_lynex_algebra_v1_9_quote(&encoded)?;
            assert_eq!(returned_fee, current.zero_for_one);
            assert_eq!(
                local, on_chain,
                "exact-output amount mismatch token_in={token_in:#x} amount={amount}"
            );
        }
    }

    let mut code_hashes = BTreeMap::new();
    for (name, address) in [
        ("factory", FACTORY),
        ("pool_deployer", POOL_DEPLOYER),
        ("router", ROUTER),
        ("quoter", QUOTER),
        ("pool", POOL),
        (
            "data_storage_operator",
            &format!("{:#x}", fee.data_storage_operator),
        ),
        ("usdc", USDC),
        ("usdt", USDT),
        ("multicall3", MULTICALL3),
    ] {
        let address = Address::from_str(address)?;
        let code = rpc.contract_code_at(address, block).await?;
        ensure!(!code.is_empty(), "{name} has no runtime code");
        code_hashes.insert(name, format!("{:#x}", keccak256(&code)));
    }

    let pinned_tick = pool.pool.tick;
    let pinned_liquidity = pool.pool.liquidity;
    let execution_pool = pool.clone();
    let execution_deadline = u64::from(
        execution_pool
            .lynex_fee
            .as_ref()
            .context("Lynex execution fee state is unavailable")?
            .envelope
            .last_timestamp,
    );
    let mirror = DexMirror::new(hydrated)?;
    let mut opportunities = OpportunityEngine::new(snapshot, &mirror)?;
    assert_eq!(opportunities.pair(0)?.selectable_pool_indices(), [0]);
    assert!(opportunities.pair(0)?.shadow_pool_indices().is_empty());
    for (book, direction) in [
        (
            top_of_book("1.01000", "1.01001")?,
            ArbitrageDirection::BuyTokenBOnDexSellOnCex,
        ),
        (
            top_of_book("0.98999", "0.99000")?,
            ArbitrageDirection::BuyTokenBOnCexSellOnDex,
        ),
    ] {
        let baseline = opportunities
            .evaluate(&book)?
            .context("reviewed six-USDT detector did not produce an evaluation")?;
        let selected = match direction {
            ArbitrageDirection::BuyTokenBOnDexSellOnCex => baseline.dex_buy_cex_sell.baseline,
            ArbitrageDirection::BuyTokenBOnCexSellOnDex => baseline.cex_buy_dex_sell.baseline,
        };
        ensure!(
            selected.is_some(),
            "reviewed direction did not clear the 20 bps gate"
        );
        for amount in [6_000_000_u64, 50_000_000] {
            let trade = opportunities
                .evaluate_exact_candidate(0, &book, direction, 0, U256::from(amount))?
                .with_context(|| {
                    format!(
                        "step-aligned Lynex candidate is unavailable direction={direction:?} amount={amount}"
                    )
                })?;
            if amount == 6_000_000 {
                let plan = DexSwapPlan::build(
                    pair,
                    &execution_pool,
                    match direction {
                        ArbitrageDirection::BuyTokenBOnDexSellOnCex => {
                            ExecutionDirection::BuyTokenBOnDexSellOnCex
                        }
                        ArbitrageDirection::BuyTokenBOnCexSellOnDex => {
                            ExecutionDirection::BuyTokenBOnCexSellOnDex
                        }
                    },
                    trade,
                    1,
                    1,
                    execution_deadline,
                )?;
                assert!(matches!(plan.route, DexRoutePlan::LynexAlgebraV1_9 { .. }));
                let request = plan.execution_request(format!("linea-lynex-{direction:?}"))?;
                assert_eq!(request.route.router(), Address::from_str(ROUTER)?);
            }
        }
        let capacity = opportunities
            .exact_candidate_capacity(0, direction, 0)?
            .context("Lynex prepared candidate capacity is unavailable")?;
        let largest_step_aligned = capacity / U256::from(1_000_000_u64) * U256::from(1_000_000_u64);
        ensure!(
            largest_step_aligned >= U256::from(199_000_000_u64)
                && largest_step_aligned <= U256::from(200_000_000_u64),
            "Lynex largest step-aligned candidate differs from the 200-USDT cap"
        );
        ensure!(
            opportunities
                .evaluate_exact_candidate(0, &book, direction, 0, largest_step_aligned)?
                .is_some(),
            "largest step-aligned Lynex candidate is unavailable"
        );
        assert!(
            opportunities
                .evaluate_exact_candidate(0, &book, direction, 0, U256::from(6_000_001_u64))
                .is_err()
        );
    }
    eprintln!(
        "LINEA_LYNEX_PARITY_JSON={}",
        serde_json::json!({
            "schema_version": 1,
            "chain_id": 59144,
            "block_number": block.number,
            "block_hash": format!("{:#x}", block.hash),
            "parent_hash": format!("{:#x}", block.parent_hash),
            "pool": POOL,
            "fee_pips": current.zero_for_one,
            "tick": pinned_tick,
            "liquidity": pinned_liquidity.to_string(),
            "samples_per_mode": 6,
            "exact_input": "byte_exact",
            "exact_output": "byte_exact",
            "prepared_boundaries": "byte_exact",
            "code_hashes": code_hashes,
            "signing": false,
            "broadcast": false,
            "allowance_mutation": false
        })
    );
    Ok(())
}

fn top_of_book(bid: &str, ask: &str) -> anyhow::Result<TopOfBook> {
    TopOfBook::new(
        Arc::from("USDCUSDT"),
        1,
        Decimal::from_str(bid)?,
        Decimal::from(1_000_u64),
        Decimal::from_str(ask)?,
        Decimal::from(1_000_u64),
        None,
        None,
        Instant::now(),
        1,
        1,
    )
}

#[tokio::test]
#[ignore = "explicit Linea Lynex Algebra V1.9 canonical Fee-before-Swap replay gate"]
async fn linea_lynex_transition_events_reproduce_pinned_post_state() -> anyhow::Result<()> {
    let domain = LoadedDomainConfig::load("config/strategies/usdt-usdc-linea-lynex.v2.json")?;
    let snapshot = domain.snapshot();
    let endpoint =
        std::env::var("LINEA_RPC_URL").unwrap_or_else(|_| "https://rpc.linea.build".to_owned());
    let rpc = JsonRpcClient::new(endpoint)?;
    let parent = CanonicalBlock {
        number: TRANSITION_BLOCK_NUMBER - 1,
        hash: B256::from_str(TRANSITION_PARENT_HASH)?,
        parent_hash: B256::from_str(TRANSITION_PARENT_PARENT_HASH)?,
    };
    let transition = CanonicalBlock {
        number: TRANSITION_BLOCK_NUMBER,
        hash: B256::from_str(TRANSITION_BLOCK_HASH)?,
        parent_hash: B256::from_str(TRANSITION_PARENT_HASH)?,
    };
    assert_eq!(
        rpc.canonical_block_timestamp(parent).await?,
        u64::from(TRANSITION_PARENT_TIMESTAMP)
    );
    assert_eq!(
        rpc.canonical_block_timestamp(transition).await?,
        u64::from(TRANSITION_TIMESTAMP)
    );

    let pre = DexHydrator::new(&rpc).hydrate_at(snapshot, parent).await?;
    let filters = build_log_filters(snapshot, &pre)?;
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
        logs.len() == 2,
        "reviewed Lynex transition must contain Fee and Swap"
    );

    let mut mirror = DexMirror::new(pre)?;
    mirror.finish_backfill_at(parent, Some(TRANSITION_PARENT_TIMESTAMP))?;
    mirror.apply_head_at(transition, Some(TRANSITION_TIMESTAMP), Instant::now())?;
    let mut kinds = Vec::new();
    for log in &logs {
        if let LogApplyResult::Applied {
            pool_index,
            kind,
            refresh_required,
        } = mirror.apply_log_at_timestamp(log, TRANSITION_TIMESTAMP)?
        {
            assert_eq!(pool_index, 0);
            kinds.push((kind, refresh_required));
        }
    }
    assert_eq!(kinds, [("fee", false), ("swap", true)]);
    mirror.refresh_pool_for_publication(0)?;

    let post = DexHydrator::new(&rpc)
        .hydrate_at(snapshot, transition)
        .await?;
    let expected = post
        .pools
        .iter()
        .find(|pool| matches!(pool.identity, PoolIdentity::LynexAlgebraV1_9 { .. }))
        .context("post-state Lynex pool is missing")?;
    let actual = mirror.pool(0)?;
    assert_eq!(
        mirror.pool_index(PoolLocator::LynexAlgebraV1_9(Address::from_str(POOL)?)),
        Some(0)
    );
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
        .lynex_fee
        .as_ref()
        .context("mirrored Lynex fee is missing")?;
    let expected_fee = expected
        .lynex_fee
        .as_ref()
        .context("post Lynex fee is missing")?;
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
        "LINEA_LYNEX_EVENT_REPLAY_JSON={}",
        serde_json::json!({
            "schema_version": 1,
            "block_number": transition.number,
            "block_hash": format!("{:#x}", transition.hash),
            "logs": logs.len(),
            "ordered_kinds": ["fee", "swap"],
            "fee_before_swap": true,
            "fee_pips": actual_fee.state.current_fees.zero_for_one,
            "tick": actual.pool.tick,
            "liquidity": actual.pool.liquidity.to_string(),
            "post_state_parity": "byte_exact"
        })
    );
    Ok(())
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

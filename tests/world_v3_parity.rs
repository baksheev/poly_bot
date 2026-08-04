use std::str::FromStr;

use alloy_primitives::{Address, U256};
use anyhow::{Context, ensure};
use arb_bot::{
    chain::rpc::{EthCall, JsonRpcClient},
    dex::{
        calldata::{decode_v3_quote_exact_input_single, v3_quote_exact_input_single},
        hydration::{DexHydrator, PoolIdentity},
    },
    domain::config::{DexProvider, LoadedDomainConfig},
};

const REVIEWED_POOL: &str = "0x610e319b3a3ab56a0ed5562927d37c233774ba39";
const REVIEWED_FEE_PIPS: u32 = 10_000;

#[tokio::test]
#[ignore = "explicit World Chain archival-RPC parity gate"]
async fn world_v3_one_percent_local_quotes_match_quoter_v2_at_one_pinned_block()
-> anyhow::Result<()> {
    let domain = LoadedDomainConfig::load("config/strategies/usdc-wld-world-chain.v14.json")?;
    let mut snapshot = domain.snapshot().clone();
    let (rpc_url_env, quoter_address, token_a_contract, token_b_contract) = {
        let pair = &mut snapshot.pairs[0];
        pair.dex.allowed_providers = vec![DexProvider::UniswapV3];
        pair.dex.uniswap_v3.as_mut().unwrap().fee_tiers = vec![REVIEWED_FEE_PIPS];
        pair.dex.uniswap_v4 = None;
        (
            pair.chain.rpc_url_env.clone(),
            pair.chain
                .uniswap_v3_quoter_address
                .clone()
                .context("World Chain V3 QuoterV2 is missing")?,
            pair.token_a.contract.clone(),
            pair.token_b.contract.clone(),
        )
    };

    let rpc_endpoint = std::env::var(rpc_url_env)?;
    let rpc = JsonRpcClient::new(rpc_endpoint)?;
    let block = rpc.latest_block().await?;
    eprintln!(
        "World Chain parity block={} hash={:#x}",
        block.number, block.hash
    );
    let hydrated = DexHydrator::new(&rpc).hydrate_at(&snapshot, block).await?;
    let quoter = Address::from_str(&quoter_address)?;
    let reviewed_address = Address::from_str(REVIEWED_POOL)?;
    let pool = hydrated
        .pools
        .iter()
        .find(|pool| {
            matches!(
                pool.identity,
                PoolIdentity::V3 {
                    address,
                    fee_pips: REVIEWED_FEE_PIPS
                } if address == reviewed_address
            )
        })
        .context("reviewed World Chain V3 1% pool was not hydrated")?;

    let token_a = Address::from_str(&token_a_contract)?;
    let token_b = Address::from_str(&token_b_contract)?;
    let samples = [
        (token_a, token_b, U256::from(6_000_000_u64)),
        (token_a, token_b, U256::from(200_000_000_u64)),
        (
            token_b,
            token_a,
            U256::from(20_u64) * U256::from(10_u128.pow(18)),
        ),
        (
            token_b,
            token_a,
            U256::from(600_u64) * U256::from(10_u128.pow(18)),
        ),
    ];
    for (token_in, token_out, amount_in) in samples {
        let zero_for_one = token_in == pool.token0;
        ensure!(
            zero_for_one || token_in == pool.token1,
            "quote input is not in the reviewed World Chain pool"
        );
        let local = pool
            .pool
            .quote_exact_in_amount_out(zero_for_one, amount_in)?;
        let encoded = rpc
            .eth_call_batch(
                &[EthCall {
                    to: quoter,
                    data: v3_quote_exact_input_single(
                        token_in,
                        token_out,
                        amount_in,
                        REVIEWED_FEE_PIPS,
                    )?,
                }],
                block,
            )
            .await?
            .pop()
            .context("World Chain QuoterV2 returned no result")?;
        let on_chain = decode_v3_quote_exact_input_single(&encoded)?;
        assert_eq!(
            local, on_chain,
            "World Chain local/QuoterV2 mismatch at block {} for amount {}",
            block.number, amount_in
        );
    }
    Ok(())
}

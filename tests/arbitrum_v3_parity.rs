use std::str::FromStr;

use alloy_primitives::{Address, U256};
use anyhow::{Context, ensure};
use arb_bot::{
    chain::rpc::{EthCall, JsonRpcClient},
    dex::{
        calldata::{decode_v3_quote_exact_input_single, v3_quote_exact_input_single},
        hydration::{DexHydrator, PoolIdentity},
    },
    domain::config::LoadedDomainConfig,
};

#[tokio::test]
#[ignore = "explicit Arbitrum archival-RPC parity gate"]
async fn arbitrum_v3_local_quotes_match_quoter_v2_at_one_pinned_block() -> anyhow::Result<()> {
    let domain = LoadedDomainConfig::load("config/strategies/usdc-esp-arbitrum.v7.json")?;
    let pair = &domain.snapshot().pairs[0];
    let rpc_endpoint = std::env::var(&pair.chain.rpc_url_env)?;
    let rpc = JsonRpcClient::new(rpc_endpoint)?;
    let block = rpc.latest_block().await?;
    eprintln!(
        "Arbitrum parity block={} hash={:#x}",
        block.number, block.hash
    );
    let hydrated = DexHydrator::new(&rpc)
        .hydrate_at(domain.snapshot(), block)
        .await?;
    let quoter = Address::from_str(
        pair.chain
            .uniswap_v3_quoter_address
            .as_deref()
            .context("Arbitrum V3 QuoterV2 is missing")?,
    )?;
    let pool = hydrated
        .pools
        .iter()
        .find(|pool| matches!(pool.identity, PoolIdentity::V3 { fee_pips: 100, .. }))
        .context("reviewed Arbitrum V3 0.01% pool was not hydrated")?;

    let samples = [
        (
            Address::from_str(&pair.token_a.contract)?,
            Address::from_str(&pair.token_b.contract)?,
            U256::from(10_000_000_u64),
        ),
        (
            Address::from_str(&pair.token_b.contract)?,
            Address::from_str(&pair.token_a.contract)?,
            U256::from(100_u64) * U256::from(10_u128.pow(18)),
        ),
    ];
    for (token_in, token_out, amount_in) in samples {
        let zero_for_one = token_in == pool.token0;
        ensure!(
            zero_for_one || token_in == pool.token1,
            "quote input is not in the reviewed Arbitrum pool"
        );
        let local = pool
            .pool
            .quote_exact_in_amount_out(zero_for_one, amount_in)?;
        let encoded = rpc
            .eth_call_batch(
                &[EthCall {
                    to: quoter,
                    data: v3_quote_exact_input_single(token_in, token_out, amount_in, 100)?,
                }],
                block,
            )
            .await?
            .pop()
            .context("Arbitrum QuoterV2 returned no result")?;
        let on_chain = decode_v3_quote_exact_input_single(&encoded)?;
        assert_eq!(
            local, on_chain,
            "Arbitrum local/QuoterV2 mismatch at block {}",
            block.number
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "explicit Arbitrum ARB/USDC archival-RPC parity gate"]
async fn arbitrum_arb_five_bps_local_quotes_match_quoter_v2_at_one_pinned_block()
-> anyhow::Result<()> {
    const FEE_PIPS: u32 = 500;
    const REVIEWED_POOL: &str = "0xb0f6ca40411360c03d41c5ffc5f179b8403cdcf8";

    let domain = LoadedDomainConfig::load("config/strategies/usdc-arb-arbitrum.v2.json")?;
    let pair = &domain.snapshot().pairs[0];
    let rpc_endpoint = std::env::var(&pair.chain.rpc_url_env)?;
    let rpc = JsonRpcClient::new(rpc_endpoint)?;
    let block = rpc.latest_block().await?;
    eprintln!(
        "Arbitrum ARB/USDC five-bps parity block={} hash={:#x}",
        block.number, block.hash
    );
    let hydrated = DexHydrator::new(&rpc)
        .hydrate_at(domain.snapshot(), block)
        .await?;
    let quoter = Address::from_str(
        pair.chain
            .uniswap_v3_quoter_address
            .as_deref()
            .context("Arbitrum V3 QuoterV2 is missing")?,
    )?;
    let reviewed_pool = Address::from_str(REVIEWED_POOL)?;
    let pool = hydrated
        .pools
        .iter()
        .find(|pool| {
            matches!(
                pool.identity,
                PoolIdentity::V3 {
                    address,
                    fee_pips: FEE_PIPS,
                } if address == reviewed_pool
            )
        })
        .context("reviewed Arbitrum ARB/USDC V3 0.05% pool was not hydrated")?;

    let token_a = Address::from_str(&pair.token_a.contract)?;
    let token_b = Address::from_str(&pair.token_b.contract)?;
    let token_b_unit = U256::from(10_u128.pow(18));
    let samples = [
        (token_a, token_b, U256::from(6_000_000_u64)),
        (token_a, token_b, U256::from(50_000_000_u64)),
        (token_a, token_b, U256::from(200_000_000_u64)),
        (token_b, token_a, U256::from(75_u64) * token_b_unit),
        (token_b, token_a, U256::from(625_u64) * token_b_unit),
        (token_b, token_a, U256::from(2_500_u64) * token_b_unit),
    ];
    for (token_in, token_out, amount_in) in samples {
        let zero_for_one = token_in == pool.token0;
        ensure!(
            zero_for_one || token_in == pool.token1,
            "quote input is not in the reviewed Arbitrum ARB/USDC pool"
        );
        let local = pool
            .pool
            .quote_exact_in_amount_out(zero_for_one, amount_in)?;
        let encoded = rpc
            .eth_call_batch(
                &[EthCall {
                    to: quoter,
                    data: v3_quote_exact_input_single(token_in, token_out, amount_in, FEE_PIPS)?,
                }],
                block,
            )
            .await?
            .pop()
            .context("Arbitrum QuoterV2 returned no result")?;
        let on_chain = decode_v3_quote_exact_input_single(&encoded)?;
        assert_eq!(
            local, on_chain,
            "Arbitrum ARB/USDC five-bps local/QuoterV2 mismatch at block {}",
            block.number
        );
    }
    Ok(())
}

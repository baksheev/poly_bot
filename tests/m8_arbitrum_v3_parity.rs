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
#[ignore = "explicit Arbitrum archival-RPC M8 parity gate"]
async fn arbitrum_v3_local_quotes_match_quoter_v2_at_one_pinned_block() -> anyhow::Result<()> {
    let domain = LoadedDomainConfig::load("config/strategies/usdc-esp-arbitrum.v3.json")?;
    let pair = &domain.snapshot().pairs[0];
    let rpc_endpoint = std::env::var(&pair.chain.rpc_url_env)?;
    let rpc = JsonRpcClient::new(rpc_endpoint)?;
    let block = rpc.latest_block().await?;
    eprintln!(
        "M8 Arbitrum parity block={} hash={:#x}",
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

use std::str::FromStr;

use alloy_primitives::{Address, U256};
use anyhow::{Context, ensure};
use arb_bot::{
    chain::rpc::{EthCall, JsonRpcClient},
    dex::{
        calldata::{decode_v4_quote_exact_input_single, v4_quote_exact_input_single},
        hydration::{DexHydrator, PoolIdentity},
        pool_id::V4PoolKey,
    },
    domain::config::LoadedDomainConfig,
};

#[tokio::test]
#[ignore = "explicit archival-RPC parity gate"]
async fn world_v4_local_quotes_match_quoter_at_one_pinned_block() -> anyhow::Result<()> {
    let domain = LoadedDomainConfig::load("config/strategies/usdc-wld-world-chain.v13.json")?;
    let pair = &domain.snapshot().pairs[0];
    let rpc_endpoint = std::env::var(&pair.chain.rpc_url_env)?;
    let rpc = JsonRpcClient::new(rpc_endpoint)?;
    let block = rpc.latest_block().await?;
    let hydrated = DexHydrator::new(&rpc)
        .hydrate_at(domain.snapshot(), block)
        .await?;
    let quoter = Address::from_str(
        pair.chain
            .uniswap_v4_quoter_address
            .as_deref()
            .context("World Chain V4 quoter is missing")?,
    )?;
    let token_in = Address::from_str(&pair.token_a.contract)?;
    let amount_in = U256::from(20_000_000_u64);

    let mut checked = 0;
    for pool in &hydrated.pools {
        let PoolIdentity::V4 { fee_pips, .. } = pool.identity else {
            continue;
        };
        let configured = pair
            .dex
            .uniswap_v4
            .as_ref()
            .context("World Chain V4 config is missing")?
            .pools
            .iter()
            .find(|configured| configured.fee_tier == fee_pips)
            .context("hydrated V4 fee tier is absent from config")?;
        let key = V4PoolKey::new(
            pool.token0,
            pool.token1,
            configured.fee_tier,
            configured.tick_spacing,
            Address::from_str(&configured.hooks)?,
        )?;
        let zero_for_one = token_in == pool.token0;
        ensure!(
            zero_for_one || token_in == pool.token1,
            "quote input is not in V4 pool"
        );
        let local = pool
            .pool
            .quote_exact_in_amount_out(zero_for_one, amount_in)?;
        let calldata = v4_quote_exact_input_single(key, zero_for_one, amount_in)?;
        let mut results = rpc
            .eth_call_batch(
                &[EthCall {
                    to: quoter,
                    data: calldata,
                }],
                block,
            )
            .await?;
        let encoded = results.pop().context("V4 quoter returned no result")?;
        let (oracle, _) = decode_v4_quote_exact_input_single(&encoded)?;
        assert_eq!(
            local, oracle,
            "V4 local/Quoter mismatch at block {} fee {}",
            block.number, fee_pips
        );
        checked += 1;
    }
    ensure!(checked == 2, "expected two World Chain V4 parity samples");
    Ok(())
}

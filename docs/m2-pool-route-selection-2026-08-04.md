# M2 DEX pool and route selection — 2026-08-04

## Decision

The implemented v14 production release adds exactly one route in M2:

- `WLD/USDC` on World Chain, Uniswap V3, fee `10000` (1%), pool
  `0x610e319b3a3ab56a0ed5562927d37c233774ba39`.

The existing production pools remain unchanged beside that addition. There is
no ESP route in this milestone and no new
Uniswap V4 or PancakeSwap route from the current candidate set.

The selected pool is the only new candidate whose observed activity is on the
same scale as the productive current pools. Over the 30 complete UTC days from
2026-07-05 through 2026-08-03 it carried approximately `$2.305M` of volume,
more than either current World Chain V3 pool individually. Adding it increases
the observed 30-day volume surface of the current WLD pool set from about
`$2.769M` to `$5.075M`, an `83.24%` increase. It would account for `45.43%` of
that enlarged surface.

Production authorization remains tied to the v14 pre-deploy review and the
digest-pinned `Deploy GKE` workflow. The selected pool reuses the existing V3
identity, local-curve, calldata, receipt, self-impact, allowance,
revert-diagnosis, and restart-recovery implementation.

## Scope and method

The search was deliberately restricted to direct pools containing the exact
funded canonical assets already used by production:

| Pair | Network | Token A | Token B |
| --- | --- | --- | --- |
| ESP/USDC | Arbitrum One | `0xaf88d065e77c8cc2239327c5edb3a432268e5831` | `0x3b8db18e69d6686ad9371a423afe3dd1065c94f1` |
| WLD/USDC | World Chain | `0x79a02482a880bce3f13e09da970dc34db4cd24d1` | `0x2cfc85d8e48f8eab294be644d9e25c3030863003` |

GeckoTerminal labels the World Chain token at `0x79a0...24d1` as `USDC.e`,
while the reviewed domain artifact and World Chain documentation call that
exact address `USDC`. The address, not the display symbol, is authoritative.

Multi-hop WLD/WETH, ESP/USDT, and other intermediate-token routes were excluded.
They change the funded asset set and recovery model and therefore are not the
first M2 direct-route expansion.

Candidate discovery used two independent views:

1. Canonical Uniswap V3 factory `getPool` calls for the standard `100`, `500`,
   `3000`, and `10000` fee tiers on each network.
2. GeckoTerminal token-pool discovery across indexed DEXes. The material
   shortlist includes every exact-pair pool with at least `$1,000` snapshot
   liquidity or `$1,000` observed 30-day volume, plus every canonical V3
   standard-fee pool and the only direct alternative-protocol ESP pool.

A direct latest-state read of the selected pool returned token0
`0x2cfc...3003` (WLD), token1 `0x79a0...24d1` (USDC), fee `10000`, tick
spacing `200`, and factory `0x7a50...25a`. The canonical factory `getPool`
call for those tokens and fee returned the same pool address. These reads
confirm the discovery identity; the implementation must repeat them at a
recorded validation block before changing the allowlist.

The comparison window is 30 complete daily candles: `2026-07-05T00:00:00Z`
through `2026-08-03T23:59:59Z`. Volume is the sum of GeckoTerminal daily USD
OHLCV volume for the exact same window for every pool. Missing days count as
zero. Snapshot liquidity and rolling 24-hour volume were read during the
2026-08-04 discovery pass and are context only; the decision uses the 30-day
comparison where the indexer provides daily candles.

GeckoTerminal is a discovery and screening source, not a production source of
truth. Canonical identities are accepted only after direct chain validation.
Trading volume is also a route-priority proxy, not a substitute for executable
local curves or realized arbitrage economics.

## WLD/USDC candidates

| Status | Protocol | Fee | Pool address or ID | 30-day volume | Snapshot liquidity | 24-hour volume | Decision |
| --- | --- | ---: | --- | ---: | ---: | ---: | --- |
| candidate | Uniswap V3 | 1.00% | `0x610e319b3a3ab56a0ed5562927d37c233774ba39` | `$2,305,239` | `$514,878` | `$57,116` | **Add in M2** |
| current | Uniswap V3 | 0.30% | `0xc19bc89ac024426f5a23c5bb8bc91d8017c90684` | `$2,090,601` | `$353,387` | `$62,977` | Keep |
| current | Uniswap V3 | 0.05% | `0x02371da6173cf95623da4189e68912233cc7107c` | `$678,664` | `$3,810` | `$12,577` | Keep |
| candidate | Uniswap V4 | 1.00% | `0x081028d60635d39241285edb01f6d6503b244eed2547333649daf2fe27c4a5b4` | `$95,977` | `$112,383` | `$1,272` | Defer |
| candidate | Uniswap V4 | 0.14% | `0x132db01ffd6a7d8446666c5fa5689a9556a384bdaa6bf68aecce7949efba649c` | `$65,553` | `$12,844` | `$431` | Defer |
| candidate | Uniswap V4 | 0.25% | `0x5d850b4608563224d2f1eafec23211f7c2682394c64733fc018b6208d5a72872` | `$2,965` | `$1,255` | `$74` | Do not add |
| candidate | Uniswap V3 | 0.01% | `0xda1eb0112f0d1b63e05dd5b83a88b2aeb692eb1f` | `$123` | `$1` | `$1` | Do not add |
| current | Uniswap V4 | 0.30% | `0x4ca0af0122d384225dc0be3702e42cfc81d22d442fd595ddd0edfc5d1c2b23cf` | `<$1` | `$1,827` | `<$1` | Keep for this additive milestone; review separately |
| current | Uniswap V4 | 0.05% | `0xbf415332fad64886704ccbaeff2cc16e505ea746a042dba9af9be9dd23da91ff` | `<$1` | `$86` | `$0` | Keep for this additive milestone; review separately |

The V3 1% pool is a high-confidence incremental route because it uses the
already implemented World Chain V3 factory/router/event/calldata path and its
30-day activity is `24.0x` the V4 1% candidate and `35.2x` the V4 0.14%
candidate. The two V4 candidates remain worth retaining in the discovery list,
but neither justifies joining the first one-pool M2 production change.

The current V4 pools should not be removed solely from this volume screen.
Removal is a separate production change and must first check whether joined
candidate and exact-quote telemetry shows useful executable edge despite their
low external swap volume.

## ESP/USDC candidates

| Status | Protocol | Fee | Pool address or ID | 30-day volume | Snapshot liquidity | 24-hour volume | Decision |
| --- | --- | ---: | --- | ---: | ---: | ---: | --- |
| current | Uniswap V3 | 0.01% | `0x15eb51a325cbce6c1cc8202a6f8a76224c5b7540` | `$26,219,880` | `$466,537` | `$966,742` | Keep |
| candidate | Uniswap V4 | 1.999% | `0xdf8b795e70dbb3ec1a1937a788b15341632e4c9fdcde9b1f03de251246a7f642` | `$0` | `$52,452` | `$0` | Do not add |
| candidate | PancakeSwap V3 | 0.01% | `0xf418c6f8e537b8ea4d3b37470ac2f81b44632de0` | `<$0.01` | `<$1` | `<$0.01` | Do not add |
| candidate | Uniswap V3 | 0.30% | `0xc3ab8764278a5c47e6b05ba4c52cbc0b99a06092` | `$0` | `<$0.01` | `$0` | Do not add |
| candidate | Uniswap V3 | 1.00% | `0xa316c42a72f4593b0ce65c0a4bb7446b89067246` | `$0` | `<$0.01` | `$0` | Do not add |

The remaining indexed direct ESP/USDC V4 tail contains arbitrary fee pools
from `0.5%` through `66%`. Every one had less than `$600` snapshot liquidity
and zero 24-hour volume, so none passed the material-shortlist rule. The
largest omitted tail member was the 2.88% pool
`0x748b2cd7d476353f82aab61f3921b30f759391a922f924d8d14d1e29e9f72dcd`
with about `$597` liquidity.

There is no economically credible ESP addition in the observed set. The only
pool with non-trivial snapshot liquidity, V4 1.999%, produced no daily OHLCV
candle in the comparison window. Supporting it would add a new Arbitrum V4
deployment/runtime surface without evidence of route demand.

## Non-Uniswap follow-up

A second discovery pass explicitly removed the Uniswap-only filter. It queried
the DexScreener token-pairs endpoint for each exact token contract and compared
the result with GeckoTerminal's per-network DEX and token-pool indexes. This
found only three direct non-Uniswap pools with both canonical endpoint tokens:

| Pair | DEX | Pool | 30-day volume | Snapshot liquidity | 24-hour volume | Decision |
| --- | --- | --- | ---: | ---: | ---: | --- |
| WLD/USDC, World Chain | DYORSwap | `0x445cbb4c9b09812eb73d3f8ba029c47ef72ca5d3` | No indexed daily candles | `$0.02` | `<$0.01` | Do not add |
| WLD/USDC, World Chain | WorldSwap | `0x3b64d7a108bfb0946f194339428bf76334ee8eec` | No indexed daily candles | `$0` | `$0` | Do not add |
| ESP/USDC, Arbitrum | PancakeSwap V3 | `0xf418c6f8e537b8ea4d3b37470ac2f81b44632de0` | `<$0.01` | `<$1` | `$0` | Do not add |

The DYORSwap pair has the exact World Chain WLD and USDC contracts, but only
about two cents of indexed liquidity. GeckoTerminal knows the pool but returns
no daily OHLCV series for it. DexScreener reports many dust-sized transactions
whose combined rolling volume remains below one cent; transaction count is not
evidence of executable liquidity.

WorldSwap describes itself as both a DEX and an aggregator that routes through
native and integrated liquidity sources. Its indexed direct WLD/USDC pool has
zero liquidity and volume. A WorldSwap quote that ultimately selects one of the
same Uniswap pools is not an additional local liquidity source and must not be
counted twice.

On World Chain the indexed DEX catalog also contains DackieSwap V2/V3, Multex,
RealSwap, and Uniswap V2, but neither independent indexer found an exact
canonical WLD/USDC pool on those venues. On Arbitrum the catalog includes
Camelot V2/V3, Ramses V1/V2, SushiSwap V2/V3, Curve, Balancer, KyberSwap,
Swapr, LFJ, and many smaller venues; none had a direct pool for the exact
canonical ESP and USDC contracts. Symbol-only search results were discarded.

### Aerodrome and Velodrome

Aerodrome was checked explicitly. The protocol's own documentation identifies
it as the liquidity hub of Base. It has no World Chain or Arbitrum deployment
in either network catalog and no pool using the production token contracts.
Search results that look similar on Base refer to different tokens or contracts
and would require a new network, wallet, gas asset, bridge/rebalance route, and
recovery scope. That is not an M2 addition to either existing pair.

Velodrome is the related Optimism/Superchain protocol the name may be confused
with. No Velodrome deployment or exact WLD/USDC pool was found on World Chain,
and no Velodrome ESP/USDC pool was found on Arbitrum. Being an OP Stack chain
does not make a pool deployed on Optimism available on World Chain.

### Revised support decision

The non-Uniswap pass does not change the selected M2 route:

- add World Chain Uniswap V3 WLD/USDC 1%;
- keep the existing reviewed pools during the additive milestone;
- add no new ESP pool;
- implement no Aerodrome, Velodrome, DYORSwap, WorldSwap, PancakeSwap, Camelot,
  Ramses, Curve, Balancer, or aggregator adapter for these pairs.

Re-run the contract-address discovery before a later route-expansion review.
A new non-Uniswap venue becomes a candidate only when it has a canonical direct
pool with material executable liquidity and an activity window comparable to
the current productive pools. An aggregator route is insufficient: M2 requires
an allowlisted pool identity, ordered canonical events, and an exact local
curve with no remote quote in evaluation or preflight.

## Implemented boundary for the selected route

The implementation change is limited to the World Chain WLD/USDC
Uniswap V3 1% pool:

1. Resolve the address again from the reviewed World Chain V3 factory and the
   exact ordered token identities; reject any mismatch.
2. Add fee `10000` and only the resolved pool to a new versioned domain source.
3. Prove read-only hydration and block-pinned exact local quote parity in both
   directions at the 6 USDC baseline and representative adaptive sizes through
   200 USDC.
4. Exercise the existing V3 allowance, calldata, positional `Swap` receipt,
   self-impact, revert-diagnosis, and restart fixtures for the new pool.
5. Run `scripts/quality.sh`, local quote benchmarks, and replay-capacity checks.
6. Deliver the single-route artifact through `main` and `Deploy GKE`, then use
   joinable pool-level candidate, selection, execution, and accounting
   telemetry for the economic review.

No split route, remote Quoter dependency, new intermediate asset, new router,
or new protocol implementation belongs in this M2 revision.

## Reproduction sources

- [GeckoTerminal API documentation](https://apiguide.geckoterminal.com/)
- [DexScreener API reference](https://docs.dexscreener.com/api/reference)
- [CoinGecko guide to GeckoTerminal pool discovery and OHLCV](https://www.coingecko.com/learn/dex-data-api)
- [Selected World Chain WLD/USDC V3 1% pool](https://www.geckoterminal.com/world-chain/pools/0x610e319b3a3ab56a0ed5562927d37c233774ba39)
- [Current Arbitrum ESP/USDC V3 0.01% pool](https://www.geckoterminal.com/arbitrum/pools/0x15eb51a325cbce6c1cc8202a6f8a76224c5b7540)
- [Aerodrome documentation](https://aerodrome.finance/docs)
- [WorldSwap](https://worldswap.org/)
- [World Chain network information and public RPC](https://docs.world.org/world-chain/quick-start/info)
- [World Chain canonical tokens and Uniswap V3 deployments](https://docs.world.org/world-chain/reference/useful-contracts)
- [Uniswap V3 subgraph query examples](https://docs.uniswap.org/api/subgraph/guides/v3-examples)

The pool-specific screening calls use these forms:

```text
GET https://api.geckoterminal.com/api/v2/networks/{network}/tokens/{token}/pools
GET https://api.geckoterminal.com/api/v2/networks/{network}/pools/{pool}/ohlcv/day
    ?aggregate=1
    &before_timestamp=1785801599
    &limit=100
    &currency=usd
    &token=base
```

For every response, retain only candles whose timestamp is between
`1783209600` and `1785715200`, inclusive, and sum element `5` (USD volume).
Dividing the sum by 30 gives a calendar-day average; do not divide by the number
of returned candles because inactive pools omit zero-volume days.

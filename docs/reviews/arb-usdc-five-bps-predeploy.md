# ARB/USDC Uniswap V3 0.05% production pre-deploy review

Status: additive route implementation and local safety gates complete. The
exact tested `main` revision is delivered only through the audited `Deploy GKE`
workflow and its immutable image digest.

## Reviewed identity and activity

- Network: Arbitrum One, chain ID `42161`.
- Exact funded contracts: ARB
  `0x912ce59144191c1204e64559fe8253a0e49e6548` and native USDC
  `0xaf88d065e77c8cc2239327c5edb3a432268e5831`.
- Canonical Uniswap V3 fee-500 pool:
  `0xb0f6ca40411360c03d41c5ffc5f179b8403cdcf8`.
- At block `491301908`, hash
  `0x0b1fba238075738a6531037c13ef060f7c1a26be6df714e356bbf027e6de2ee2`,
  the canonical factory `getPool(ARB, USDC, 500)` returned that exact address.
  Direct reads returned token0 ARB, token1 USDC, fee `500`, tick spacing `10`,
  and the canonical factory.
- GeckoTerminal daily exact-pool candles for the 30 complete UTC days from
  2026-07-06 through 2026-08-04 sum to approximately `$66,798.24` volume.
  Snapshot screening on 2026-08-05 showed about `$45.1K` liquidity.

External indexed activity prioritizes the route but does not authorize a
trade. Production admission still requires the exact local pool curve to clear
the unchanged 20 bps gross gate at the selected Binance-step-aligned size.

## Scope and safety

- `usdc-arb-arbitrum.v1.json` remains immutable release provenance. The new v2
  source adds fee tier `500` beside the existing fee tier `3000`.
- Both pools use the same canonical Uniswap V3 factory, QuoterV2, SwapRouter02,
  positional Swap receipt, local CLMM curve, allowance, wallet, nonce lane,
  Binance account, recovery policy, and ARB/USDC inventory.
- No new token, venue account, signer, router, rebalance route, risk limit, or
  execution mode is introduced. The detector remains 6 USDC, adaptive sizing
  remains capped at 200 USDC, and the gross entry gate remains 20 bps.
- The opportunity engine evaluates both immutable pool generations and selects
  one exact execution candidate; it does not split a trade across pools.
- The deployment workflow asserts the v2 snapshot, ordered fee tiers, both
  canonical pool identities, and the fee-500 strategy dependency before it can
  authenticate to GCP.

## Verification and rollout

- The explicit archival-RPC parity test hydrates the fee-500 pool at one pinned
  block and compares local exact-input quotes with canonical QuoterV2 in both
  directions at representative 6, 50, and 200 USDC-scale amounts.
- `scripts/quality.sh` must pass and the checked-in combined domain must be the
  exact deterministic compiler output.
- Rollout verification must show one ready GKE owner, zero application
  restarts, the GCE rollback owner terminated, ARB readiness healthy, both pool
  identities hydrated, and fee-500 pool evaluation telemetry.
- Rollback is the prior immutable image digest and v1 ARB source. No inventory
  migration is required because the added route uses the same ARB and USDC
  locations.

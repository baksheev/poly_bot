# ARB/USDC production pre-deploy review

Status: implementation and local safety gates complete; production bootstrap,
rollout, and post-rollout observation are performed by the audited `Deploy GKE`
workflow for the exact tested `main` revision.

## Reviewed market identity

- Binance Spot symbol: `ARBUSDC`, status `TRADING`, base step `0.1`, price tick
  `0.0001`, and minimum notional `5 USDC` at review time.
- Arbitrum One token contracts: native USDC
  `0xaf88d065e77c8cc2239327c5edb3a432268e5831` and ARB
  `0x912ce59144191c1204e64559fe8253a0e49e6548`.
- Canonical Uniswap V3 0.3% pool:
  `0xaebdca1bc8d89177ebe2308d62af5e74885dccc3`.
- The v1 artifact keeps the 6 USDC detector, 20 bps gross entry gate, 200 USDC
  adaptive trade cap, 220 USDC unhedged bound, 2 USDC recovery-loss bound, and
  30-second strategy-price transport-silence limit.

## Ownership and mutation safety

- WLD, ESP, and ARB use one process-scoped Binance account/order owner and one
  globally serialized trade coordinator.
- ESP and ARB share the existing Arbitrum `DexExecutionService`, signer, durable
  EVM journal, nonce owner, gas cache, and rebalance execution lane. No second
  wallet or nonce process is introduced.
- The compiled capital policy adds exact ARB debit and fee bounds while keeping
  one active external transfer, direct Arbitrum routes only, one unknown-status
  query, and no bridge mutation authority.
- ARB chain readiness is pair-scoped and fail-closed. A DEX stream or readiness
  failure disables new ARB entries without disabling WLD or ESP.

## Initial inventory bootstrap

- The deployment workflow first verifies current authenticated ARB and USDC
  direct Arbitrum deposit/withdrawal routes.
- It activates the durable entry stop, waits for zero active operations, scales
  the application owner to zero, and submits exactly one 500 USDC MARKET BUY
  with deterministic operation/client IDs and a persistent scoped journal.
- An interrupted or repeated workflow reconciles the same journaled order; it
  has no authority to submit a second bootstrap order.
- The workflow restores the old owner and clears the entry stop before rollout,
  records a deployment annotation only after a terminal fill, and skips the
  bootstrap on later releases.
- After the new revision starts, continuous direct rebalancing moves half of
  the captured ARB inventory to the Arbitrum wallet. Trading remains fail-closed
  until both venue inventories and pair readiness are healthy.

## Verification and observation

- The checked-in compiled bundle is exact compiler output and the deployment
  workflow asserts the ARB symbol, filters, capability, source approval, pool,
  fee, detector, live caps, and rebalance gates.
- Unit and integration tests cover the three-strategy graph, one shared
  Arbitrum owner, fair three-target rebalance dispatch, additional-token capital
  authority, bootstrap quiescence, and the existing Unknown/restart contracts.
- Post-rollout evidence must show a healthy one-Pod GKE owner, terminated GCE
  rollback VM, terminal bootstrap fill, ARB on both venues, ARB readiness, and
  joinable candidate/order/result telemetry before the release is considered
  complete.

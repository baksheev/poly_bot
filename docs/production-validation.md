# Production validation ledger

This ledger records the manual canary required before each execution component
can be enabled in the autonomous runtime. A successful unit or integration test
does not replace a real operation against the production endpoint.

## Safety envelope

- DEX/CEX test inventory is limited to 100 USDC plus the corresponding WLD.
- ETH purchased only for gas is limited to 200 USDT, matching the explicitly
  approved bootstrap operation; canaries should use materially less.
- Binance orders use deterministic `rustval...` client IDs and are reconciled
  through a signed status/history request.
- Binance withdrawals may target only the address derived from
  `EVM_WALLET_PRIVATE_KEY`. The manual CLI must not accept a destination address.
- The test wallet private key lives in Secret Manager and must never appear in
  command arguments, logs, telemetry, source, or this ledger.
- Every mutating command requires `--confirm-live` and enforces its cap before
  opening an authenticated trading or withdrawal connection.
- This exposed test key must be replaced before unrestricted production trading.

## Evidence

| Component | Production operation | Status | Evidence |
| --- | --- | --- | --- |
| Binance Spot WS API | MARKET buy WLDUSDC | Passed | order `455189372`, `FILLED`, 24.5 WLD / 9.96415 USDC |
| Binance Spot WS API | MARKET sell WLDUSDC | Passed | order `455189375`, `FILLED`, 24.5 WLD / 9.95925 USDC |
| Binance reconciliation | signed `allOrders` query | Passed | both deterministic `rustval...` IDs returned `FILLED` |
| EVM signer | derive public address from Secret Manager value | Passed | `0x90D990C81320221D2882De32beeA78923c1e77A3` |
| World Chain RPC | chain ID, latest block, pending nonce, ETH/WLD/USDC | Passed | chain `480`; empty wallet; pending nonce `0` |
| Optimism RPC | chain ID, latest block, pending nonce, ETH/WLD/USDC/USDC.e | Passed | chain `10`; empty wallet; pending nonce `0` |
| Binance IOC | place/cancel/partial-fill reconciliation | Pending | — |
| Binance recovery | LIMIT unwind, then MARKET loss fixation | Pending | — |
| Binance withdrawal | ETH to Optimism | Pending | — |
| Binance withdrawal | WLD direct to World Chain | Pending | — |
| Binance fallback | WLD to Optimism when World route is unavailable | Pending | — |
| Across | Optimism to World Chain | Pending | — |
| Across | World Chain to Optimism | Pending | — |
| DEX | USDC to WLD | Pending | — |
| DEX | WLD to USDC | Pending | — |
| EVM recovery | reverted/replaced transaction and nonce recovery | Pending | — |
| Concurrent execution | DEX and CEX legs start concurrently | Pending | — |
| Compensation | DEX failure unwinds CEX | Pending | — |
| Compensation | CEX failure retries MARKET and records loss | Pending | — |
| Rebalance | direct route end-to-end | Pending | — |
| Rebalance | Optimism + Across fallback end-to-end | Pending | — |

The wallet hydration evidence above intentionally records no secret material.
At the time of the check all native and ERC-20 balances were zero, so no signed
chain transaction can be tested until the bootstrap withdrawal completes.

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
| Binance subaccount | signed account and WLDUSDC commission hydration from diagnostic VM | Passed | Spot, `canTrade=true`, two nonzero balances, WLD and USDC present; static egress `34.143.148.4` |
| Binance capital routes | signed `capital/config/getall` from diagnostic VM | Passed | WLD direct and Optimism deposit/withdrawal available; USDC Optimism deposit/withdrawal available and no direct route |
| Dedicated subaccount order audit | signed `allOrders` query from diagnostic VM | Passed | no recent `rustval...` orders; a new capped execution canary is still required |
| Dedicated subaccount Spot WS API | capped MARKET buy/sell WLDUSDC | Pending | funded account is ready for an explicitly approved canary |
| Pre-isolation Binance Spot WS API | MARKET buy WLDUSDC | Historical | order `455189372`, `FILLED`, 24.5 WLD / 9.96415 USDC; does not validate the dedicated subaccount |
| Pre-isolation Binance Spot WS API | MARKET sell WLDUSDC | Historical | order `455189375`, `FILLED`, 24.5 WLD / 9.95925 USDC; does not validate the dedicated subaccount |
| Pre-isolation Binance reconciliation | signed `allOrders` query | Historical | both deterministic `rustval...` IDs returned `FILLED`; rerun on the dedicated subaccount |
| EVM signer | derive public address from Secret Manager value | Passed | `0x90D990C81320221D2882De32beeA78923c1e77A3` |
| World Chain RPC | chain ID, latest block, pending nonce, ETH/WLD/USDC | Passed | chain `480`; native balance `0.007982721314804481 ETH` after Across fill |
| Optimism RPC | chain ID, latest block, pending nonce, ETH/WLD/USDC/USDC.e | Passed | chain `10`; native balance `0.001997329279441474 ETH`; pending nonce `1` |
| Binance IOC | place/cancel/partial-fill reconciliation | Pending | — |
| Binance recovery | LIMIT unwind, then MARKET loss fixation | Pending | — |
| Binance withdrawal | ETH to Optimism | Passed | `0.01 ETH` requested; `0.009985 ETH` received after Binance fee |
| Binance withdrawal | WLD direct to World Chain | Pending | — |
| Binance fallback | WLD to Optimism when World route is unavailable | Pending | — |
| Across native ETH | Optimism to World Chain | Passed | origin `0x8a6d9da68dd5b9ed9f4bbcc6e7d736f8d249a773e8b9921003eeceb89eb57f86`; fill `0x09bcd6beb4ed5f188df7cc3b2b23f7d215a95111c05bfef390cf3632adfb7877`; sent `0.00798759334452456 ETH`; received `0.007982721314804481 ETH`; retained more than 20% on Optimism |
| Across USDC | Optimism to World Chain | Pending | — |
| Across USDC | World Chain to Optimism | Pending | — |
| DEX | USDC to WLD | Pending | — |
| DEX | WLD to USDC | Pending | — |
| EVM recovery | reverted/replaced transaction and nonce recovery | Pending | — |
| Concurrent execution | DEX and CEX legs start concurrently | Pending | — |
| Compensation | DEX failure unwinds CEX | Pending | — |
| Compensation | CEX failure retries MARKET and records loss | Pending | — |
| Rebalance | direct route end-to-end | Pending | — |
| Rebalance | Optimism + Across fallback end-to-end | Pending | — |
| GKE full-live preflight | secrets, signer, caps, isolated inventory | Passed | GKE NAT authenticated the latest shared key against exactly 1,000 USDC and 2,500 WLD; WLD direct/fallback and USDC fallback routes pass; signer and both RPCs pass; GitHub caps are 500 USDC and 1,250 WLD; operator selected explicit shared-key mode |

The wallet and Binance hydration evidence above intentionally records no secret
material or raw authenticated request. The bootstrap ETH withdrawal and native
Across bridge are complete; ERC-20 inventory and DEX execution canaries remain
pending. The dedicated Binance subaccount is funded and read-verified, but its
first capped order-placement and reconciliation canary remains pending.

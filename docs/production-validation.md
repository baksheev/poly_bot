# Production validation ledger

Last reviewed: 2026-07-17

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
- Production trading credentials must remain isolated from Rails and may be
  enabled only after the full launch gate passes.

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
| Binance withdrawal | WLD direct to World Chain | Passed | exercised by the recoverable Binance-to-wallet rebalance route |
| Binance fallback | WLD to Optimism when World route is unavailable | Passed | fallback withdrawal completed and was bridged to World Chain |
| Across native ETH | Optimism to World Chain | Passed | origin `0x8a6d9da68dd5b9ed9f4bbcc6e7d736f8d249a773e8b9921003eeceb89eb57f86`; fill `0x09bcd6beb4ed5f188df7cc3b2b23f7d215a95111c05bfef390cf3632adfb7877`; sent `0.00798759334452456 ETH`; received `0.007982721314804481 ETH`; retained more than 20% on Optimism |
| Across USDC | Optimism to World Chain | Passed | production rebalance Binance to wallet completed |
| Across USDC | World Chain to Optimism | Passed | production rebalance wallet to Binance completed and Binance credited the deposit |
| DEX | USDC to WLD | Pending | — |
| DEX | WLD to USDC | Pending | — |
| EVM recovery | reverted/replaced transaction and nonce recovery | Pending | — |
| Concurrent execution | DEX and CEX legs start concurrently | Pending | — |
| Compensation | DEX failure unwinds CEX | Pending | — |
| Compensation | CEX failure retries MARKET and records loss | Pending | — |
| Rebalance | WLD direct route end-to-end | Passed | Binance-to-wallet withdrawal and wallet-to-Binance deposit both completed |
| Rebalance | USDC and WLD Optimism + Across fallback end-to-end | Passed | all four fallback direction/token combinations completed; final WLD operations were `rebalance-27-6022301e0756c887` and `rebalance-37-ba77134a174e0d45` |
| Rebalance settlement | fresh Binance and wallet snapshots after completion | Passed | the normal planner restarted with no action after both streams advanced; final balances are recorded in `docs/rebalancing.md` |
| Rebalance monitoring | Cloud Monitoring email alerting | Passed | one-minute structured heartbeat plus immediate fault and five-minute missing-heartbeat policies are enabled for `baksheev@me.com`; a synthetic fault matched the log metric |
| GKE subaccount preflight | trading key, signer, caps, isolated inventory | Passed | GKE NAT authenticated the isolated subaccount against exactly 1,000 USDC and 2,500 WLD; WLD direct/fallback and USDC fallback routes pass; signer and both RPCs pass; GitHub caps are 500 USDC and 1,250 WLD. |
| GKE first full-live attempt | 500 USDC fallback intent | Failed closed | `localentity` withdrawal returned HTTP 401 / Binance `-1002`; durable journal remained at `intent_recorded`, wallet nonce journal stayed empty, and no withdrawal was indexed. The account is configured for the standard capital withdrawal API before retry. |
| GKE standard withdrawal retry | 500 USDC fallback intent | Failed closed | Standard `capital/withdraw/apply` also returned HTTP 401 / Binance `-1002` before any withdrawal was indexed, proving the shared API key itself lacks withdrawal authorization even though the Spot account reports `canWithdraw=true`. Startup now checks `account/apiRestrictions` before creating a journal. |
| GKE master-treasury preflight | master key permissions and subaccount mapping | Passed | Revision `e077b2bab2a9498352afc12a3fdedb87237845f8` verified Reading, Withdrawals, Universal Transfer, the production IP allowlist, and the master's exact WLD/USDC view of the configured subaccount before opening journals. |
| GKE master internal transfer | 500 USDC subaccount Spot to master Spot | Failed closed | Binance rejected `universalTransfer` with HTTP 401 / `-1002`; the journal contains only the deterministic `intent_recorded` event, the wallet journal is empty, and no withdrawal was submitted. The endpoint also requires the distinct `enableInternalTransfer` API-key flag; startup now checks both transfer flags before opening journals. |

The failed-closed rows are retained because they verify important recovery
boundaries; later successful operations supersede them as route-readiness
evidence. The wallet and Binance hydration evidence intentionally records no
secret material or raw authenticated request. ERC-20 rebalance validation is
complete. DEX trade execution and the dedicated Binance subaccount's capped
order-placement/reconciliation canaries remain pending and keep the full live
arbitrage gate closed. The authoritative gate is
`docs/rails-test-gap-analysis.md`.

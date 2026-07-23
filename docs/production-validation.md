# Production validation ledger

Last reviewed: 2026-07-17

Status: historical component-canary ledger. Current production decisions and
global invariants are defined in `docs/rust-production-architecture.md`.

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
- Every mutating command requires its exact live-confirmation phrase and
  enforces its cap before opening an authenticated trading, signing, or
  withdrawal connection.
- Production trading credentials must remain isolated from Rails and may be
  enabled only after the full launch gate passes.

## Evidence

| Component | Production operation | Status | Evidence |
| --- | --- | --- | --- |
| Binance subaccount | signed account and WLDUSDC commission hydration from diagnostic VM | Passed | Spot, `canTrade=true`, two nonzero balances, WLD and USDC present; static egress `34.143.148.4` |
| Binance capital routes | signed `capital/config/getall` from diagnostic VM | Passed | WLD direct and Optimism deposit/withdrawal available; USDC Optimism deposit/withdrawal available and no direct route |
| Dedicated subaccount order audit | signed `allOrders` query | Passed | independently returned all four 2026-07-17 `rustval...` canary orders with matching IDs, sides, types, quantities and `FILLED` status |
| Dedicated subaccount Spot WS API | capped MARKET buy/sell WLDUSDC | Passed | BUY order `455789048`: `26.2 WLD` for `9.96386 USDC`; SELL order `455789056`: `26.2 WLD` for `9.96124 USDC`; final WLD exactly matched the starting balance |
| Pre-isolation Binance Spot WS API | MARKET buy WLDUSDC | Historical | order `455189372`, `FILLED`, 24.5 WLD / 9.96415 USDC; does not validate the dedicated subaccount |
| Pre-isolation Binance Spot WS API | MARKET sell WLDUSDC | Historical | order `455189375`, `FILLED`, 24.5 WLD / 9.95925 USDC; does not validate the dedicated subaccount |
| Pre-isolation Binance reconciliation | signed `allOrders` query | Historical | both deterministic `rustval...` IDs returned `FILLED`; rerun on the dedicated subaccount |
| EVM signer | derive public address from Secret Manager value | Passed | `0x90D990C81320221D2882De32beeA78923c1e77A3` |
| World Chain RPC | chain ID, latest block, pending nonce, ETH/WLD/USDC | Passed | chain `480`; native balance `0.007982721314804481 ETH` after Across fill |
| Optimism RPC | chain ID, latest block, pending nonce, ETH/WLD/USDC/USDC.e | Passed | chain `10`; native balance `0.001997329279441474 ETH`; pending nonce `1` |
| Binance LIMIT IOC | capped full-fill buy/sell WLDUSDC | Passed | BUY order `455788994`: `26.1 WLD` for `9.92583 USDC`; SELL order `455788998`: `26.1 WLD` for `9.92061 USDC`; both independently reconciled as `FILLED` |
| Binance IOC partial fill | cancelled and filled quantity reconciliation | Pending | full-fill IOC path passed; a controlled partial-fill canary is still required |
| Binance recovery | LIMIT unwind, then MARKET loss fixation | Pending | — |
| Binance withdrawal | ETH to Optimism | Passed | `0.01 ETH` requested; `0.009985 ETH` received after Binance fee |
| Binance withdrawal | WLD direct to World Chain | Passed | exercised by the recoverable Binance-to-wallet rebalance route |
| Binance fallback | WLD to Optimism when World route is unavailable | Passed | fallback withdrawal completed and was bridged to World Chain |
| Across native ETH | Optimism to World Chain | Passed | origin `0x8a6d9da68dd5b9ed9f4bbcc6e7d736f8d249a773e8b9921003eeceb89eb57f86`; fill `0x09bcd6beb4ed5f188df7cc3b2b23f7d215a95111c05bfef390cf3632adfb7877`; sent `0.00798759334452456 ETH`; received `0.007982721314804481 ETH`; retained more than 20% on Optimism |
| Across USDC | Optimism to World Chain | Passed | production rebalance Binance to wallet completed |
| Across USDC | World Chain to Optimism | Passed | production rebalance wallet to Binance completed and Binance credited the deposit |
| DEX Uniswap V3 | 10 USDC to WLD | Passed | swap `0xc56005476e0acf9b0f1bf6dbb3c05be11b5fb6f90f7fd2a9a962a95305b985c3`; received `26.118979277000460343 WLD`; gas used `132,596`; Rails-compatible limit `850,000` includes `50,000` explicit additional gas |
| DEX Uniswap V3 | exact purchased WLD to USDC | Passed | recovery swap `0xf196478dd5c1e435b5c3254413feb6df34a96ed99dccd45abbbcab43c76527fc`; sold `26.118979277000460343 WLD`; received `9.940091 USDC`; gas used `125,888` |
| DEX Uniswap V4 | 10 USDC to WLD | Passed | swap `0xb9dcd46ec62ee73f01c2c6e83e4ebf1e5c2f385b7083b348287d6a9515032e0b`; received `26.105429068306941392 WLD`; gas used `135,247`; Rails-compatible limit `599,952` includes `50,000` explicit additional gas |
| DEX Uniswap V4 | exact purchased WLD to USDC | Passed | swap `0x77211148b5abd9384b3749201376bca1bbee689c2c9cc4a81f7684e5383c5fe8`; sold `26.105429068306941392 WLD`; received `9.801022 USDC`; gas used `127,771`; final WLD balance exactly matched the pre-canary balance |
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

The V3 buy receipt initially raced an HTTP RPC node whose `latest` balance
snapshot was one block behind. The command failed closed before the sell; the
measured `26.118979277000460343 WLD` delta was then sold through the capped
recovery command. Balance verification now waits for a canonical block at or
above the receipt block and reads both ERC-20 balances at that same block. The
V4 round trip subsequently completed end to end through that corrected path.
All ten approval/swap operations in the canary journal end in
`mined_success`; final World Chain nonce is `15/15` with no pending transaction.

The failed-closed rows are retained because they verify important recovery
boundaries; later successful operations supersede them as route-readiness
evidence. The wallet and Binance hydration evidence intentionally records no
secret material or raw authenticated request. ERC-20 rebalance validation is
complete. Standalone DEX V3/V4 execution and fully filled dedicated-account
Binance LIMIT IOC/MARKET order placement are complete. Partial-fill,
unknown-outcome, User Data Stream, restart, and composed two-leg rows above
preserve the state of this historical canary ledger; they are not a current
statement that production is disabled. Current live behavior, accepted risk
boundaries, and remaining architecture debt are recorded in
`docs/rust-production-architecture.md`.

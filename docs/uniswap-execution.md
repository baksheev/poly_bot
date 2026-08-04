# Uniswap V3/V4 execution

The Rust runtime has one typed exact-input execution boundary for buying and
selling the configured pair through Uniswap V3 and V4. The v14 production
artifact enables live arbitrage; the manual `uniswap-round-trip` command is a
separate historical validation tool and is not a routine production path.

## Rails gas parity

Rails receives a gas estimate from the executable Uniswap quote. It uses:

- V3: quoted gas multiplied by `2`;
- V4: quoted gas multiplied by `4`, with a `250,000` minimum;
- `additional_gas`, when explicitly supplied, after that multiplier.

Rust's production local curves do not contain the Quoter gas field, and the
immediate live path deliberately does not add `eth_estimateGas` latency between
dispatch and signing. Its static fallback therefore comes from Rails production
history rather than a generic default. For receipts dated
2026-05-25 through 2026-07-25:

| Protocol and direction | Executed quote p50 | Executed quote p95 | Actual gas p95 | Actual gas max | Rust live fallback |
| --- | ---: | ---: | ---: | ---: | ---: |
| V3 USDC -> WLD | 89,263 | 95,176 | 132,682 | 196,889 | 250,000 |
| V3 WLD -> USDC | 92,938 | 98,887 | 126,001 | 156,138 | 250,000 |
| V4 USDC -> WLD | 37,009 | 37,169 | 135,363 | 190,956 | 250,000 |
| V4 WLD -> USDC | 37,229 | 37,361 | 128,707 | 150,137 | 250,000 |

The unified `250,000` fallback leaves about 27% headroom over the largest
observed V3 receipt and 31% over the largest V4 receipt. It also preserves the
Rails V4 minimum. This fallback covered every one of the 83,577 V3 and 30,410
V4 historical receipts in that window. It is a versioned empirical fallback,
not a claim that gas use can never grow; new router bytecode or materially
different routes require remeasurement.

The manual simulation path still combines a fresh `eth_estimateGas` result
with the same protocol multiplier and historical floor.

Fee construction and accounting remain Rails-compatible:

- EIP-1559 priority fee is `1,500,000 wei` and
  `max_fee_per_gas = eth_gasPrice + priority_fee`, matching
  `EthWalletService`.
- The dedicated execution owner refreshes `eth_gasPrice` every second and
  caches the resulting sample for two seconds. Live transaction construction
  performs only a cache lookup; it never waits for this RPC.
- A zero or failed refresh publishes the Rails World Chain fallback
  `100,000 wei` into the same two-second cache. The next one-second background
  tick retries RPC without delaying execution.
- Arbitrum has no fallback and no priority tip. Its reviewed M9 policy turns
  the fresh RPC sample into a `12,000 bps` EIP-1559 maximum-fee envelope so a
  small next-block base-fee move cannot invalidate an otherwise current
  sample; the receipt still charges only its effective price.
- Receipt cost is `gasUsed * effectiveGasPrice + l1Fee`. The OP Stack
  `l1Fee` is the L1 data-publication charge; it changes realized cost but
  cannot prevent or cause an EVM revert.
- Gas limit retains an independent safety cap. Production admission does not
  sample or reserve the wallet's native balance; gas funding is an
  operator-maintained invariant. The separate manual validation command still
  checks native funding before it mutates the wallet.

Gas limits retain Rust safety ceilings. Immediately before signing, the live
executor uses the at-most-two-second cached RPC or fallback sample plus the
configured priority fee. Startup/manual mutation may refresh synchronously if
the cache is unavailable. Arbitrum fails closed instead. There is no
admission-time or absolute economic fee cap.

## Single owner and safe outcomes

`DexExecutionService` runs on the dedicated `dex-executor` OS thread. A bounded
channel feeds a single owner of the process-scoped signer, HTTP RPC client,
nonce lane, and append-only transaction journal. Approval and swap calls cannot
race each other inside the service.

Approval and manual validation transactions follow this order:

1. Validate addresses, amounts, pool identity, deadline, and slippage floor.
2. Simulate with `eth_call`, estimate gas, resolve the current fee, and check the ETH balance.
3. Fsync the intent to the wallet journal.
4. Sign and fsync the transaction hash.
5. Broadcast and fsync the broadcast state.
6. Poll the receipt and fsync either `mined_success` or `mined_reverted`.

Latency-sensitive live arbitrage swaps use immediate submission. Their route,
amount, slippage floor, admission gas budget, deadline, inventory, and allowance are
validated before dispatch, then the executor uses the Rails-compatible quoted
or fallback gas limit and proceeds directly to fresh fee resolution, journaling,
signing, and broadcast. It does not call `eth_call` or `eth_estimateGas` between
dispatch and nonce reservation; an on-chain revert is journaled and charged to
the parent result. Immediate submission is accepted only after startup has
validated and permanently locked the required router allowances.

Receipt availability can lead an RPC provider's `latest` state by a block. The
validation path therefore waits until `latest.number >= receipt.block_number`
and reads USDC and WLD through one block-pinned batch before accepting a balance
delta. It never treats an immediately stale balance as transaction failure.

The reusable execution service also parses the receipt's OP Stack `l1Fee` and
canonical ERC-20 `Transfer` logs
from the successful receipt. It requires the wallet's net input-token delta to
equal the submitted exact input and the net output-token delta to clear the
submitted minimum, then returns both base-unit deltas with gas used and
effective gas price. Both the L2 execution fee and `l1Fee` enter realized
token-A accounting for successful and reverted transactions. This gives the
parent coordinator actual DEX amounts
without a race against a later balance snapshot; post-trade snapshots remain
the independent settlement check.

For a successful arbitrage swap, the executor also extracts the selected
pool's canonical `Swap` event and its `(block, transactionIndex, logIndex)`
position from the receipt. The engine applies that positional event directly
to the local mirror after non-blockingly draining DEX WebSocket events already
queued at terminal delivery. It rebuilds the affected prepared curves inline
and then releases the execution lane. Receipt settlement never calls
`eth_getLogs` and does not create a pool or global settlement barrier. A later
WebSocket copy of the same event is discarded by canonical log position.

Pending opportunities are retained for entry preflight. Immediately before
dispatch, preflight requotes the immutable DEX input against the latest
published pool generation and combines it with the latest Binance bid/ask. It
requires the current DEX output to cover the immutable transaction minimum,
requires both price paths to be inside their 30-second freshness boundaries,
and rejects when the recomputed gross spread is below 20 bps. The minimum check
is transaction feasibility only and does not introduce another profitability
model. If the relevant Binance price and published DEX generation are unchanged
since admission, it reuses the admission proof without repeating the quote.

The receipt Swap is the authoritative immediate self-impact update. The
process-scoped WebSocket remains the ongoing source of new external pool
events. Missing or malformed positional receipt settlement is telemetry and
the normal WebSocket stream remains available; it must not introduce a
post-trade wait in the owner loop.

Transport failures and confirmation timeouts are recorded as
`outcome_unknown`. The unresolved operation keeps its deterministic identity,
nonce claim, and exact reservation until canonical RPC reconciliation proves
the result; it must not become a global parent dead end. A revert is logged
immediately as `arbitrage_dex_revert` with `phase=receipt`, `plan_id`,
operation ID, protocol, pool, transaction hash, block, calldata amount bounds,
deadline, gas used, effective gas price, and `l1Fee`. This receipt event releases
the execution lane without waiting for diagnosis.

A separate bounded background worker then calls `debug_traceTransaction` with
a five-second diagnostic timeout. If the provider does not expose tracing, it
falls back to a historical `eth_call` of the mined transaction. A second
`arbitrage_dex_revert` event with `phase=diagnostic` records the diagnostic
source and status, decoded `Error(string)`, decoded `Panic(uint256)`, or the
four-byte custom-error selector. It also records whether gas usage exhausted
the submitted limit when the transaction lookup exposes that limit. Trace
failure, timeout, or queue saturation is telemetry incompleteness only: it
cannot change the known revert, hold the lane, or enter trading decisions. Raw
signed payloads, full calldata, credentials, and unbounded provider responses
are never journaled or logged.

V3 checks and, if necessary, grants the router ERC-20 allowance. V4 performs
both required stages: ERC-20 allowance to Permit2 and Permit2 allowance to the
Universal Router. Approval transactions use the same nonce journal.

## Capped live round trip

The manual command buys WLD with at most 10 USDC, measures the actual wallet
balance delta, and sells exactly that WLD back to USDC through the same protocol.
It rehydrates all pools before each leg and chooses the best local exact-input
route for the requested version.

```bash
UNISWAP_LIVE_CONFIRMATION=I_UNDERSTAND_UNISWAP_LIVE_10_USDC \
  cargo run --release -- uniswap-round-trip \
  --protocol v3 \
  --amount-usdc-base-units 10000000 \
  --slippage-bps 50 \
  --additional-gas 0
```

Use `--protocol v4` for the V4 round trip. The command refuses to run when:

- the signer differs from `EVM_WALLET_ADDRESS`;
- the chain is not World Chain (`480`);
- the wallet already has a pending nonce;
- balances or native gas are insufficient;
- the transaction journal is locked by another process;
- USDC input exceeds 10 USDC or slippage exceeds 50 bps.

The GKE full-live process owns the same dedicated Rust wallet. A manual canary
must never create a second nonce owner beside it. Any future rerun requires a
reviewed operational workflow that first removes production ownership and
proves there is no in-flight trade or rebalance; it must not be run ad hoc from
a workstation.

If a completed buy is followed by a fail-closed interruption before its sell,
the exact measured WLD delta can be unwound with `uniswap-recovery-sell`. The
command accepts a WLD base-unit amount but refuses the operation when its local
USDC quote exceeds the 10 USDC authorization envelope.

## Production canary evidence

On 2026-07-17 the dedicated Rust wallet completed both protocol canaries on
World Chain with `50,000` explicit additional gas:

- V3 buy: `0xc56005476e0acf9b0f1bf6dbb3c05be11b5fb6f90f7fd2a9a962a95305b985c3`.
- V3 sell: `0xf196478dd5c1e435b5c3254413feb6df34a96ed99dccd45abbbcab43c76527fc`.
- V4 buy: `0xb9dcd46ec62ee73f01c2c6e83e4ebf1e5c2f385b7083b348287d6a9515032e0b`.
- V4 sell: `0x77211148b5abd9384b3749201376bca1bbee689c2c9cc4a81f7684e5383c5fe8`.

The V3 round trip returned `9.940091 USDC`; the V4 round trip returned
`9.801022 USDC`. Both sold the exact WLD received by their buy. Ten total
approval/swap transactions ended in `mined_success`, the final WLD balance
matched the starting balance, and the wallet finished at nonce `15/15` with no
pending transaction. The GKE runtime remained available in observer-only mode
during the canary and was restored to one healthy `full_live` replica afterward.

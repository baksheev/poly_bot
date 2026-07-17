# Uniswap V3/V4 execution

The Rust runtime has one typed exact-input execution boundary for buying and
selling the configured pair through Uniswap V3 and V4. Live arbitrage remains
disabled by the domain artifact; the manual `uniswap-round-trip` command is a
separate capped canary.

## Rails gas parity

The implementation follows the current Rails services rather than generic gas
defaults:

- V3 uses the quoted gas multiplied by `2`.
- V4 uses the quoted gas multiplied by `4`, with a `250,000` minimum.
- A caller can add `additional_gas` after that multiplier.
- If the local Rust quote has no Quoter gas field, the same multiplier is
  applied to `eth_estimateGas`; the Rails fallback (`800,000` for V3 and
  `250,000` for V4) remains a floor.
- EIP-1559 priority fee is `1,500,000 wei` and
  `max_fee_per_gas = eth_gasPrice + priority_fee`, matching
  `EthWalletService`.
- `eth_gasPrice` is cached for five seconds inside the execution owner.
- Gas limit and fee have independent safety caps, and the wallet's native
  balance must cover the maximum signed cost.

The caps are intentionally Rust-only safety checks. A value outside the cap is
rejected before nonce reservation and signing.

## Single owner and safe outcomes

`DexExecutionService` runs on the dedicated `dex-executor` OS thread. A bounded
channel feeds a single owner of the process-scoped signer, HTTP RPC client,
nonce lane, and append-only transaction journal. Approval and swap calls cannot
race each other inside the service.

Every submitted transaction follows this order:

1. Validate addresses, amounts, pool identity, deadline, and slippage floor.
2. Simulate with `eth_call`, estimate gas, check the fee cap and ETH balance.
3. Fsync the intent to the wallet journal.
4. Sign and fsync the transaction hash.
5. Broadcast and fsync the broadcast state.
6. Poll the receipt and fsync either `mined_success` or `mined_reverted`.

Receipt availability can lead an RPC provider's `latest` state by a block. The
validation path therefore waits until `latest.number >= receipt.block_number`
and reads USDC and WLD through one block-pinned batch before accepting a balance
delta. It never treats an immediately stale balance as transaction failure.

The reusable execution service also parses canonical ERC-20 `Transfer` logs
from the successful receipt. It requires the wallet's net input-token delta to
equal the submitted exact input and the net output-token delta to clear the
submitted minimum, then returns both base-unit deltas with gas used and
effective gas price. This gives the parent coordinator actual DEX amounts
without a race against a later balance snapshot; post-trade snapshots remain
the independent settlement check.

Transport failures and confirmation timeouts are recorded as
`outcome_unknown`. The nonce lane then stays blocked until canonical RPC
reconciliation proves the result. A revert is logged with operation ID,
transaction hash, block, gas used, and effective gas price. Raw signed payloads
and credentials are never journaled or logged.

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

The GKE full-live rebalancer currently uses the same dedicated Rust wallet. A
manual canary must therefore run only after confirming no rebalance is in
flight and switching that runtime to observer-only mode. The market-data
service can remain available, but two process-local mutating nonce owners must
never run concurrently even if both independently observe the same pending
nonce. Restore `full_live` and verify a fresh healthy heartbeat afterward.

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

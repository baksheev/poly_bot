# Low-latency Uniswap quoting

Status: first local calculation slice implemented  
Last reviewed: 2026-07-15

## Decision

The trading hot path never calls Alchemy, Multicall3, a Quoter contract, or a
database. Alchemy WebSocket delivers pool changes; the Rust process mirrors all
quote-relevant state and evaluates exact-input swaps locally when a Binance
update arrives.

```text
Alchemy WSS logs ─> ordered pool-state updates ─┐
                                                ├─> single state owner
Binance bookTicker ─────────────────────────────┘          │
                                                          ├─> local V3/V4 quotes
                                                          ├─> opportunity
                                                          └─> execution gate

Alchemy HTTP ─> startup hydration / gap repair / sampled parity only
```

Alchemy does not provide a stream of executable Uniswap quotes. The useful
subscription primitive is `eth_subscribe` over WebSocket for logs and heads.
Ordinary state reads use the reusable HTTP client outside the hot path.

## State held per pool

Hookless Uniswap V3 and static-fee, hookless V4 use the same concentrated
liquidity swap loop. Each candidate therefore uses one compact local model:

- current `sqrtPriceX96`, tick, and active liquidity;
- fee in pips and tick spacing;
- initialized tick bitmap words;
- `liquidityGross` and `liquidityNet` for each initialized tick;
- last applied block number/hash, transaction index, and log index;
- hydration generation and ready/degraded state.

The implemented `ClmmPool` quote loop walks bitmap words, crosses ticks, updates
simulated liquidity, and returns exact integer output. The trading API returns
only `amount_out`; the diagnostic API additionally derives post-swap state for
parity tests. Both are read-only, so the same state serves both directions
without cloning or locking.

V4 pools with hooks that can affect a swap are rejected. The current production
snapshot has zero hooks, so the shared CLMM path is valid.

## World Chain contracts

The versioned snapshot contains the discovery and hydration contracts, not
credentials:

- V3 Factory `0x7a5028BDa40e7B173C278C5342087826455ea25a`;
- V4 PoolManager `0xb1860d529182ac3bc1f51fa2abd56662b7d13f33`;
- V4 StateView `0x51d394718bc09297262e368c1a481217fdeb71eb`;
- separate environment-variable names for Alchemy HTTP and WSS endpoints.

At review time, on-chain V3 `getPool` returns pools for fee 3000
(`0xc19bc89ac024426f5a23c5bb8bc91d8017c90684`) and fee 10000
(`0x610e319b3a3ab56a0ed5562927d37c233774ba39`); fee 5000 is absent. Runtime
discovery remains authoritative because a new candidate can be created later.

## Race-free startup

1. Connect WSS and start buffering matching logs and heads.
2. Choose a canonical block `B` and hydrate every candidate at exactly `B` over
   HTTP. V3 reads the pool head, bitmap words, and initialized ticks; V4 reads
   the equivalent state through StateView.
3. Apply buffered logs after `B` in `(block, transaction_index, log_index)`
   order, verifying the parent/hash chain.
4. Mark DEX state ready only when all enabled candidates are coherent at the
   same observed head. Until then, new live entries remain disabled.

V3 `Swap` replaces the head fields. `Mint` and `Burn` update the two boundary
ticks and bitmap. V4 `Swap` replaces the head fields; `ModifyLiquidity` updates
the affected boundary ticks. Events update the in-memory mirror before the next
decision is accepted.

## Gaps and reorgs

A WSS reconnect, subscription error, block discontinuity, removed log, unknown
tick, or parent-hash mismatch immediately makes DEX quoting unavailable. The
service repairs the exact missing block range with `eth_getLogs` or fully
rehydrates at a new pinned block. It never mixes head data from one block with
ticks from another.

This intentionally prefers a short fail-closed interval over a plausible but
incorrect quote. A small block journal may later support cheap rollback, but is
not required before shadow mode.

## Quoter and parity role

V3 QuoterV2 and V4 Quoter are retained as correctness oracles only. A bounded
background sampler compares local results with `eth_call` at the same block and
amount. Any mismatch marks that pool unhealthy and records the state fingerprint
and result in telemetry; it never delays the current market-data event.

Golden tests must cover both directions, fee rounding, word boundaries,
initialized-tick crossing, no liquidity, and V3/V4 Quoter parity against
captured World Chain blocks. The first committed fixture matches the 20 USDC
V3 fee-3000 QuoterV2 output at World Chain block `0x1ee7069` exactly.

## Performance contract

Measurements are made in an optimized container on the target Worker Pool CPU,
not inferred from a laptop debug build. The initial budget is:

- zero network calls and zero locks per quote;
- zero heap allocation in the steady-state calculation loop;
- one candidate quote p99 below 3 microseconds when no tick is crossed;
- both directions across all current candidates p99 below 25 microseconds;
- Binance event dequeue to completed opportunity decision p99 below 100
  microseconds, excluding external network propagation.

These are acceptance thresholds, not current benchmark claims. A release-mode
benchmark and production histograms must prove them before paper execution.

The first local arm64 release baseline (2026-07-15, two million iterations,
full-range fixture, hot `amount_out` path, no tick crossing) measured 550 ns for
token0 to token1 and 291 ns for token1 to token0. Treat these as
development-machine averages, not Worker Pool p99 results.

Run the allocation-free calculation baseline with:

```bash
cargo bench --bench local_quote
```

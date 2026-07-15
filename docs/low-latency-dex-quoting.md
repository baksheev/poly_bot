# Low-latency Uniswap quoting

Status: local opportunity calculation and market-liquidity sizing implemented
Last reviewed: 2026-07-16

## Decision

The trading hot path never calls Alchemy, Multicall3, a Quoter contract, or a
database. Alchemy WebSocket delivers pool changes; the Rust process mirrors all
quote-relevant state and evaluates exact-input swaps locally when a Binance
update arrives.

```text
Alchemy WSS logs ─> ordered pool-state updates ─┐
                                                ├─> single state owner
Binance bookTicker WSS ─> direct socket poll ───┘          │
                                                          ├─> local V3/V4 quotes
                                                          ├─> opportunity
                                                          └─> execution gate

Alchemy HTTP ─> startup hydration / gap repair / sampled parity only
```

The Binance socket future is polled by the same Tokio task that owns strategy
state. A parsed Spot frame is applied and evaluated immediately, without an
intermediate market-data channel or a second task wakeup. DEX I/O, curve
building, and telemetry remain outside that owner and communicate through
bounded channels because they are not triggered for every Binance frame.

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

The hot API also supports exact-output quotes. DEX-buy/CEX-sell sizing asks the
pool for the exact token-A input required for a Binance-step-aligned token-B
output. This prevents a residual token-B position caused by quoting an
arbitrary token-A input and rounding its output down after the swap.

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

1. Choose a canonical block `B` and hydrate every candidate at exactly `B` over
   HTTP. V3 reads the pool head, bitmap words, and initialized ticks; V4 reads
   the equivalent state through StateView.
2. Subscribe to narrowly filtered V3/V4 logs and `newHeads` over WSS.
3. After every subscription is acknowledged, capture canonical head `C`, fetch
   matching logs for `(B, C]` over HTTP, and apply them in
   `(block, transaction_index, log_index)` order. WSS is already buffering, so
   events after `C` cannot fall into the snapshot/subscription race window.
4. Ignore buffered duplicates at or below `C` and mark DEX state ready only
   when all candidates are coherent at the same observed head. Until then, new
   live entries remain disabled.

V3 `Swap` replaces the head fields. `Mint` and `Burn` update the two boundary
ticks and bitmap. V4 `Swap` replaces the head fields; `ModifyLiquidity` updates
the affected boundary ticks. Events update the in-memory mirror before the next
decision is accepted.

## Opportunity and capacity model

For each accepted Binance update, the single state owner evaluates every
hydrated pool in both directions using a common token-B amount:

1. Derive the baseline token-B quantity from the configured 20 USDC notional
   and current Binance ask, then round down to the Binance step size.
2. DEX buy / CEX sell: exact-output quote that token-B amount on the DEX and
   compare the reserved token-A cost with Binance bid proceeds.
3. CEX buy / DEX sell: price the same token-B amount at Binance ask and compare
   it with the reserved exact-input DEX proceeds.
4. If the baseline clears 20 bps, binary-search whole Binance steps over an
   immutable prepared swap curve until the next step fails the profit
   threshold, exceeds hydrated DEX liquidity, or reaches the observed Binance
   top-of-book quantity. Each probe is a segment lookup plus at most one swap
   step; it never replays the CLMM bitmap walk.
5. Across qualifying pools, retain the capacity candidate with the greatest
   absolute token-A profit. No RPC, database call, lock, or pool clone occurs.

The repeated baseline DEX quote is held in a fixed ring of eight entries per
`(pool, direction)`. The token-B amount is part of each entry, so small Binance
price oscillations reuse recent step-aligned amounts without unbounded growth.
Both successful quotes and insufficient-liquidity results are cached.

Each pool also has two immutable prepared curves: exact-output for DEX buy and
exact-input for DEX sell. Curve construction performs the exact Uniswap
word-boundary traversal once and stores contiguous cumulative segments with
the original rounding. A quote then performs a binary search and at most one
`compute_swap_step`; an amount above the directional capacity fails in constant
time. Capacity-search probes use the curve directly and do not need their own
cache.

Any applied `Swap`, `Mint`, `Burn`, or `ModifyLiquidity` event immediately
marks only the affected pool stale, clears its rings, and transfers a cloned
versioned pool state to a bounded builder thread. Decisions fail closed while
any required curve is missing. Only a result matching the latest requested
generation is published; superseded builds are discarded. Once published, the
last Binance Spot book is evaluated immediately instead of waiting for another
exchange tick.

Uniswap LP fees are already included by the CLMM swap math. The configured
four-basis-point DEX reserve is then applied conservatively: costs round up and
proceeds round down. All amount, threshold, and sizing math is checked integer
math; `f64` is not used.

The service writes one `arbitrage_evaluation` for every calculation and a
separate `arbitrage_opportunity` for each direction that clears the threshold.
Each record contains baseline economics, selected pool, signed profit,
hundredths-of-basis-point edge, capacity, limiter, calculation latency, and
end-to-end decision latency. `baseline_quote_cache_hits` and
`baseline_quote_cache_misses` make every recomputation visible in telemetry.
Raw Binance and evaluation payloads are formatted on a separate bounded task.
The decision path only transfers typed in-memory records, and captures decision
latency before JSON construction or ClickHouse channel work.

The capacity is deliberately named `market_liquidity_capacity`, not executable
size. Both market data and eventual execution use Binance Spot, but bookTicker
contains only the best price level. Deeper Spot depth and fees, gas,
wallet/Binance inventory, concurrency reservations, and risk caps are not
applied yet. Those must become hard minimum constraints before paper or live
execution.

## Gaps and reorgs

A WSS disconnect, subscription error, block discontinuity, removed log, invalid
liquidity delta, or parent-hash mismatch immediately makes DEX quoting
unavailable. The current read-only implementation exits so systemd restarts
the container, fully hydrates at a new pinned block, and backfills the startup
gap with `eth_getLogs`. In-process reconnect and exact-range repair are the
next recovery slice. The service never continues with a plausible but
unverified pool mirror.

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

Measurements are made in an optimized container on the target Compute Engine
CPU, not inferred from a laptop debug build. The initial budget is:

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
development-machine averages, not production VM p99 results.

After adding exact-output sizing, the 2026-07-16 local arm64 release benchmark
measured 537/283 ns for exact-input and 395/386 ns for exact-output in the two
directions. A short live Spot release run over all five hydrated pools produced
90 full baseline evaluations with calculation p50 288 us, p95 987 us, and p99
3,711 us. No opportunity crossed the threshold, so those figures exclude the
conditional binary sizing path. This development-machine end-to-end result is
above the 100 us acceptance threshold; production VM histograms and
profiling must drive the next optimization rather than treating the single-call
microbenchmark as sufficient evidence.

After adding the bounded baseline cache, a production-shaped local arm64
benchmark over the five live hydrated pools measured 200.2 us with all ten
pool-direction entries cold and 1.80 us with all ten entries hot, a 111x
steady-state speedup. A separate 50-event live Binance Spot + Alchemy run
recorded 46 fully warm evaluations at p50 10 us, p95 52 us, p99/max 94 us. Four
events recomputed one or more entries after startup or a pool update; including
those cold events, the sample was p50 10 us, p95 78 us, and p99/max 2,445 us.
These are development-machine measurements and did not replace the production
baseline until the Singapore production validation below.

The Singapore Cloud Run validation used a fixed 666-evaluation window after
deploying source `c798349c8c8f`. Overall calculation latency fell from the
pre-cache 453/560/911 us p50/p95/p99 to 12/25/630 us. The 628 fully warm
evaluations measured 11/19/51 us and 94 us maximum. The 38 evaluations with a
state-driven cache miss measured 25/792/1,106 us; these recomputations now
account for the overall p99. The observed cache hit rate was 97.4%. The warm
p99 is much better but still above the 25 us all-candidate target, so the
performance contract remains an active target rather than being relaxed.

The prepared-curve local benchmark reduced an exact-output request above a
sparse pool's capacity from 68,327 ns to 60.2 ns (about 1,135x). No-cross
prepared quotes measured 267-483 ns depending on direction and mode. A live
five-pool run after moving repeated CEX arithmetic out of the pool loop measured
the fully warm opportunity calculation at 2/12/19 us p50/p95/p99. These local
figures remain secondary to the production window recorded in the deployment
runbook.

The subsequent Singapore production window contained 667 Binance-triggered
evaluations. The complete calculation measured 3/7/13 us p50/p95/p99; its 664
fully warm rows measured 3/7/10 us. This satisfies the 25 us calculation p99
contract. Frame-receipt-to-decision latency measured 51/87/201 us. Slow rows
were dominated by time waiting to enter the state owner: the 3,899 us maximum
had a 3,911 us engine queue age and only 5 us of calculation. The next latency
boundary was therefore the Binance-reader task/channel, not CLMM math. This
evidence motivated the direct socket-poll/state-owner path described above.

A 339-event local live validation of that direct path measured 2/5/10 us for
calculation and 7/12/60 us from accepted frame to completed decision. The clean
revision-tagged Singapore production window then measured 460 unique updates:
2/6/18 us calculation and 5/15/50 us decision p50/p95/p99. Calculation maxed at
41 us and decision at 92 us; no event exceeded 100 us. The 457 fully warm rows
were faster still at 2/6/9 us calculation and 5/15/45 us decision. Both latency
acceptance thresholds passed on the former 8-vCPU Worker Pool. The service was
subsequently moved to Compute Engine to remove Cloud Run scheduling from future
execution work and make CPU placement, process priority, networking, and host
tuning explicit.

The same binary and image were then run concurrently on `c4-highcpu-8` and
Cloud Run over 531 identical unique Spot updates. On the 526 fully warm rows,
the dedicated VM measured 3/6/6 us calculation and 9/17/18 us complete decision
p50/p95/p99. Cloud Run measured 2/8/17 us and 7/18/39 us respectively. VM
decision latency maxed at 53 us with no row over 100 us, versus 124 us on Cloud
Run. This fixed comparison justified the Compute Engine cutover.

Run the allocation-free calculation baseline with:

```bash
cargo bench --bench local_quote
```

# Camelot V3 ARB/USDC support specification

Status: implemented for direct full-live rollout; production verification is
performed by the immutable-image Deploy GKE workflow

Last reviewed: 2026-08-05

## Decision

Add Camelot V3 as a distinct DEX provider and add exactly one initial direct
ARB/native-USDC route on Arbitrum One:

- protocol: `camelot_v3`;
- implementation family: Camelot's Algebra V1.9 directional-fee deployment;
- pool: `0xfae2ae0a9f87fd35b5b0e24b47bac796a7eefea1`;
- token0 ARB: `0x912ce59144191c1204e64559fe8253a0e49e6548`;
- token1 native USDC: `0xaf88d065e77c8cc2239327c5edb3a432268e5831`.

The route joins the existing ARB/USDC strategy as another immutable candidate.
It does not create a second strategy, split an order across pools, change the
6 USDC detector notional, change the 20 bps raw gross-spread gate, or raise the
200 USDC adaptive execution cap. Candidate ranking remains the largest exact
Binance-step-aligned size that clears the existing gate, with deterministic
stable-pool-ID tie breaking.

The first production revision containing Camelot enables this route directly
in `full_live`, at the same authority and limits as the existing Uniswap
ARB/USDC routes. There is no production observe-only, paper, reduced-size, or
canary phase. The first eligible Camelot opportunity may execute the full
adaptive size up to 200 USDC and then use the existing Binance hedge/recovery
flow. Correctness and performance evidence is completed before that rollout,
not collected by exposing a deliberately limited live cohort.

Camelot V3 is not represented as `uniswap_v3`. It may share low-level Q64.96,
tick, liquidity, and prepared-curve primitives only where byte-exact parity has
been proved. It has its own provider identity, Algebra state profile,
directional dynamic-fee model, hydration profile, event set, router calldata,
Quoter oracle, receipt proof, allowance, gas evidence, and telemetry.

Every implementation step is stop-the-line gated by correctness and paired
performance evidence. After each step, the existing Uniswap control is measured
again and Camelot is compared with it. A failed gate is fixed or the step is
reverted before work continues; passing only a final aggregate benchmark is not
enough.

This document does not authorize a production configuration change, allowance,
signature, transaction, rollout, or live trade.

## Reviewed deployment and route identity

The Camelot V3 deployment used by this specification is:

| Contract | Arbitrum One address | Runtime role |
| --- | --- | --- |
| AlgebraFactory | `0x1a3c9B1d2F0529D97f2afC5136Cc23e58f1FD35B` | startup discovery and identity proof |
| AlgebraPoolDeployer | `0x6Dd3FB9653B10e806650F107C3B5A0a6fF974F65` | deployment identity proof |
| Quoter | `0x0Fc73040b26E9bC8514fA028D998E73A254Fa76E` | pinned parity oracle only |
| SwapRouter | `0x1F721E2E82F6676FCE4eA07A5958cF098D339e18` | direct exact-input execution |

The Camelot Yak aggregator, AMM V2 router, and AMM V4/Algebra Integral contracts
are excluded. The selected V3 router resolves exactly one Algebra pool from the
two tokens, accepts a deadline, and avoids enabling aggregation, multi-hop,
V2, V4, or mixed-protocol execution.

Read-only verification at Arbitrum block `491383703`, hash
`0xb74c400c8e68aeca18ece2ca02adf0aca8185f1e36a5c12a6dd2e7279cb2cc43`,
timestamp `2026-08-05T14:09:37Z`, proved:

- `factory.poolByPair(ARB, USDC)` returned the selected pool;
- the pool returned the exact token0/token1 addresses above;
- `factory()` returned the reviewed AlgebraFactory;
- `tickSpacing()` returned `10`;
- `globalState()` returned directional fees `117`/`117` fee units at that
  block, where one unit is `1e-6`, and an unlocked pool;
- `dataStorageOperator()` returned
  `0x2998a7232ce882fdc52c9c14a4b035bc18515f44`;
- `activeIncentive()` and `liquidityCooldown()` both returned zero;
- router and Quoter returned the reviewed factory;
- factory, pool, router, and Quoter all had non-empty runtime bytecode.

The values above establish identity, not a permanent fee, spacing, or operating
assumption. Camelot can change directional fees during swaps and exposes a
permissioned tick-spacing change. Runtime startup repeats all canonical checks
at one pinned block, and the expected pool address remains mandatory in the
versioned domain artifact.

## Protocol compatibility boundary

Camelot V3 is based on Algebra V1.9 with directional dynamic fees. It resembles
Uniswap V3, but its state and execution semantics are not fee-tier-compatible.

### Reusable primitives

The following concepts can remain common after parity tests prove the exact
rounding and boundary behavior:

- Q64.96 square-root prices and the base-1.0001 tick domain;
- active liquidity plus initialized boundary liquidity deltas;
- exact-input and exact-output prepared-curve interfaces;
- immutable execution-envelope curves, baseline quote rings, adaptive sizing,
  admission, reservation, Binance hedge, bounded recovery, nonce, journal, and
  accounting orchestration;
- the seven-argument positional `Swap` payload types;
- integer-only opportunity and execution math.

Shared concepts do not imply a shared unchecked implementation. Algebra's
`PriceMovementMath`, tick-table traversal, and rounding are compared against
the Camelot Quoter at pinned blocks before any common code is selected.

### Provider-specific state and ABI

The following differences remain explicit:

1. The factory exposes one `poolByPair(tokenA, tokenB)` pool rather than one
   pool per fee tier. Camelot pool identity therefore has no fee-tier field.
2. `globalState` exposes `price`, `tick`, directional `feeZto` and `feeOtz`,
   a timepoint index, two community-fee values, and the lock bit. It is not
   Uniswap `slot0`.
3. Initialized ticks are exposed through Algebra `tickTable` and `ticks` with
   `liquidityTotal`/`liquidityDelta`. The implementation must not deserialize
   them as Uniswap `tickBitmap`/`liquidityGross`/`liquidityNet` without an
   explicit checked conversion.
4. Tick spacing is mutable. A `TickSpacing(int24)` event immediately makes the
   route unavailable and requires pinned full rehydration before reuse.
5. Fees are directional and dynamic. On the first swap that writes a new
   timepoint, the pool obtains new fees from its data storage operator, stores
   both values, and emits `Fee(uint16,uint16)` before `Swap`. A fixed fee copied
   from startup or from the last Swap is not executable quote state.
6. The Camelot `Swap` event has the same Solidity type signature as the
   seven-argument Uniswap V3 event, but it is decoded only after the compiled
   address-to-provider lookup identifies the pool as Camelot. `Fee`,
   `TickSpacing`, and `Incentive` remain Camelot-only topics.
7. The router accepts
   `exactInputSingle((address,address,address,uint256,uint256,uint256,uint160))`
   with selector `0xbc651188`: token in, token out, recipient, deadline,
   amount in, minimum output, and square-root-price limit. There is no fee
   field. It must not reuse either current Uniswap SwapRouter02 calldata or
   PancakeSwap calldata.
8. The Camelot Quoter accepts
   `quoteExactInputSingle(address,address,uint256,uint160)` and
   `quoteExactOutputSingle(address,address,uint256,uint160)`, returning the
   amount and the fee used. It is an oracle outside the trading hot path.
9. ERC-20 allowance is granted only to the reviewed Camelot SwapRouter by the
   first direct-full-live production revision.

`activeIncentive` is required to remain zero for the initial route. A non-zero
`Incentive` event or startup value fails this route closed and requires a new
review; the initial implementation does not silently assume that a virtual
pool cannot affect traversal behavior.

## Dynamic-fee correctness contract

Dynamic fee handling is part of quote correctness, not telemetry.

The runtime mirrors the minimal Algebra timepoint and data-storage state needed
to reproduce `getFees` for both directions. It also mirrors the fee
configuration and `volumePerLiquidityInBlock` inputs used by the deployed
Camelot code. All arithmetic uses the deployed integer widths and rounding.
No `f64`, wall-clock approximation, constant fallback fee, or remote call is
allowed in sizing, admission, preflight, or transaction construction.

For canonical pool state at head timestamp `T`, the prepared-curve builder
computes a directional fee envelope for every integer timestamp from `T`
through the immutable transaction-validity horizon. It uses the greatest
executable fee for each direction and records:

- the canonical pool and fee-state generations;
- `fee_zto_current` and `fee_otz_current`;
- the two envelope fees used to build curves;
- the first and last timestamps in the envelope;
- the timepoint/configuration fingerprint.

The transaction deadline must be inside that exact horizon. Opportunity,
adaptive sizing, admission, and preflight use the envelope curve, so the 20 bps
gate remains one raw venue-economics model with the Camelot fee included. The
envelope is conservative execution state, not an expected-profit floor, gas
deduction, recovery forecast, or second profitability gate.

On every canonical Camelot `Fee`, `Swap`, `Mint`, or `Burn`, and whenever a new
head moves the validity horizon enough to change either fee envelope, only the
affected prepared curves are invalidated and rebuilt. A head that leaves the
integer envelope unchanged does not publish a new curve generation. Fee
projection runs outside the Binance frame-to-decision path and must not make
every Arbitrum head rebuild the pool.

Before DEX dispatch, preflight drains already queued canonical events, checks
the latest canonical head, recomputes the envelope if required, and requotes
the exact immutable input. If the deadline is outside the prepared horizon,
fee state is incomplete, the data-storage fingerprint changes, or parity is
unhealthy, the entry is rejected.

An external swap can still land between preflight and the bot's transaction,
just as an external Uniswap swap can change price. The calldata minimum output
is the final on-chain protection. Receipt telemetry compares the planned fee
envelope, any positional `Fee` log, actual pool and wallet deltas, and actual
output; a known revert follows the existing immediate receipt and bounded
diagnostics path.

## Domain and type model

The versioned source schema gains explicit optional Camelot fields. A suitable
shape is:

```json
{
  "chain": {
    "camelot_v3_factory_address": "0x1a3c...D35B",
    "camelot_v3_pool_deployer_address": "0x6Dd3...F65",
    "camelot_v3_quoter_address": "0x0Fc7...a76E",
    "camelot_v3_router_address": "0x1F72...9e18"
  },
  "dex": {
    "allowed_providers": [
      "uniswap_v3",
      "pancake_swap_v3",
      "camelot_v3"
    ],
    "camelot_v3": {
      "pools": [
        {
          "expected_address": "0xfae2...fea1",
          "selection_enabled": true,
          "required_active_incentive": "0x0000000000000000000000000000000000000000",
          "expected_tick_spacing": 10,
          "dynamic_fee_horizon_seconds": 2
        }
      ]
    }
  }
}
```

The exact horizon is fixed only after P3 parity and timing evidence. It must be
at least the maximum age permitted by the immutable DEX plan and may not be
extended at runtime without rebuilding the fee envelope.

The compiler emits `DexProvider::CamelotV3` and a stable
`PoolIdentity::CamelotV3 { address }`. Provider identity survives hydration,
mirror lookup, fee state, prepared generation, opportunity selection, durable
plan, execution request, receipt proof, cost telemetry, and realized
accounting. A durable/public Camelot route cannot be replayed against a
Uniswap or Pancake router.

Adding Camelot must not introduce string scans into the hot path. The compiler
builds stable typed IDs, provider profiles, dependency adjacency, and
address/topic routing indexes once at startup. Uniswap-only source artifacts
must compile byte-for-byte identically until their source schema version is
intentionally changed.

## Startup, ingestion, and recovery

Startup retains the race-free snapshot/subscription/backfill sequence:

1. Select canonical Arbitrum block `B`.
2. At exactly `B`, resolve `poolByPair`, compare the mandatory expected
   address, and validate tokens, factory, deployer identity, router, Quoter,
   runtime code, tick spacing, zero active incentive, and configured limits.
3. At `B`, hydrate `globalState`, liquidity, tick table, initialized ticks,
   directional fees, data-storage operator, fee configuration, required
   timepoints, and per-block volume state as one unpublished generation.
4. Subscribe over the existing process-scoped Arbitrum WSS connection to the
   selected address and the exact Camelot topics: Swap, Mint, Burn, Fee,
   TickSpacing, and Incentive.
5. Capture head `C`, backfill `(B, C]`, apply logs in canonical position order,
   discard buffered duplicates, build fee envelopes and prepared curves, and
   only then mark the route ready.

The deployed pool exposes no getter for `volumePerLiquidityInBlock`. In the
reviewed directional-fee `PoolState` layout, storage slot 3 packs public
`liquidity` in the low 128 bits and the internal volume accumulator in the
high 128 bits. Hydration reads that slot at canonical block hash `B` and
requires the low half to equal the separately called `liquidity()` result
before accepting the high half. Any layout mismatch fails closed.

The 6,932 raw Algebra tick-table rows are read through the configured canonical
Multicall3 in chunks of at most 500 inner calls. Multicall3 bytecode is checked
at `B`; every inner call is mandatory, and a partial or failed aggregate is
discarded. This aggregation is Camelot-specific and does not alter the
Uniswap hydration calls, batching policy, or publication path.

Pool events are routed by compiled address/provider before decoding. Within a
transaction, a Camelot Fee emitted before Swap is applied before rebuilding or
publishing. No generic decoder loop may try every V3-like ABI.

Gap, removed-log, parent-hash mismatch, invalid liquidity delta, a missing Fee
event when the reconstructed timepoint transition requires one, unsupported
configuration change, non-zero incentive, fee arithmetic error, or Quoter
parity mismatch fails Camelot closed and starts pinned rehydration. It must not
corrupt coherent Uniswap/Pancake pools or stop unrelated WLD/ESP strategies.
Once Camelot is an executable ARB/USDC dependency, existing pair-level
fail-closed semantics determine whether new ARB entries pause until recovery.

## Quoting, selection, and execution

The selected Camelot pool participates in both existing ARB/USDC directions
with no provider preference:

- ARB to USDC uses the prepared zero-to-one curve and its `fee_zto` envelope;
- USDC to ARB uses the prepared one-to-zero curve and its `fee_otz` envelope.

The hot quote API remains a borrowed, allocation-free segment lookup plus at
most one exact swap step. Adaptive sizing runs on the existing bounded worker
against immutable prepared snapshots. No RPC, Quoter call, database access,
lock, JSON construction, serialization, wall-clock fee calculation, pool
clone, or new task handoff occurs in the Binance frame-to-baseline path.

Execution uses a direct single-pool exact-input Camelot router call. The plan's
input is unchanged and slippage only reduces `amount_out_minimum`. The plan
includes provider, pool, pool generation, fee generation, directional fee
envelope, horizon, router profile, and deadline. The router cannot select a fee
tier, alternate pool, multi-hop path, or protocol.

The receipt must prove exactly one positional selected-pool Swap and exact
wallet transfer deltas. If the transaction emits a Fee event, its position and
values are proved and applied before the receipt Swap. On success, the
execution owner non-blockingly drains already queued Arbitrum events, applies
any receipt Fee and the receipt Swap directly to the local mirror, and rebuilds
only affected curves before releasing the execution lane. It never waits for a
second `eth_getLogs` copy and creates no pool-wide or global settlement barrier.

Known reverts, unknown EVM outcomes, Binance IOC protection, MARKET recovery,
commission accounting, inventory drift, and journal reconciliation retain the
existing semantics. A new provider does not get another wallet, Binance
account, signer, nonce lane, or recovery policy.

## Gas and allowance evidence

The Uniswap or Pancake gas fallback is not inherited by name. Before live
execution, collect Camelot direct-swap receipt gas for both directions and
exercise pinned read-only simulation/estimation against the exact seven-word
calldata. The provider-specific fallback must cover the observed maximum with
an explicit margin while remaining below the executor safety ceiling. At least
100 representative receipts are required for a tail claim; a smaller cohort
reports exact samples and maxima without claiming p99.

Arbitrum's existing gas-price cache, maximum-fee headroom, zero priority tip,
native-funding invariant, and receipt accounting remain unchanged. Gas is not
a sizing or admission input.

No Camelot approval is authorized during implementation or pre-production
verification. The direct-full-live production revision prepares max allowance
for exactly ARB and native USDC to the reviewed Camelot router under
provider-specific durable idempotency keys, verifies it, and permanently locks
allowance mutation before the route becomes execution-ready.

## Performance preservation contract

### Meaning of comparable

"Camelot performance is comparable to Uniswap" requires all three proofs:

1. **Matched operation cost.** Synthetic states with the same ticks,
   liquidity, direction, fee, and requested amount compare Uniswap and Camelot
   quote lookup, event application, plan/calldata, and receipt operations.
2. **Real-pool cost.** Pinned ARB/USDC pools are measured with actual tick
   density, directional fee state, and segment counts. Curve build is reported
   absolute and per segment; dynamic-fee projection is reported separately.
3. **Runtime non-regression.** Adding Camelot must not regress the original
   Uniswap cohorts, ARB combined decision path, unrelated strategies, queue
   drops, CPU throttling, or memory beyond the gates below.

External RPC, sequencer inclusion, and venue gas distributions are reported by
provider but do not excuse a local queue, lock, allocation, or handoff.

### Measurement protocol after every step

Every milestone checks in a machine-readable report under a versioned schema.
It records source revision, domain artifact, Rust/toolchain, build profile, CPU,
kernel, warmups, samples, paired round order, pool and fee fingerprints,
segment counts, allocation counts, and p50/p95/p99/max.

Local comparison uses paired, interleaved release measurements on the same
otherwise-idle host: at least 32 alternating rounds and at least one million
timed operations per provider for every microbenchmark. The report uses the
median of round percentiles and includes dispersion. Target claims use the
exact immutable image and the pre-rollout replay on the existing fixed
`c4-highcpu-8`; local Docker is never used.

After **each** P0-P8 milestone:

1. run `scripts/quality.sh`;
2. run the paired Camelot/Uniswap microbenchmark suite;
3. rerun the unchanged frozen Uniswap control suite;
4. run the maximum-pair capacity replay with network I/O and external mutation
   disabled;
5. compare with both the previous milestone and frozen pre-Camelot reports;
6. attach the machine-readable report and investigate every repeatable change;
7. stop, fix, or revert if any gate fails.

The common gates are:

- zero network calls, locks, serialization, or allocations in steady-state
  prepared quote and Binance-frame-to-baseline paths;
- byte-identical results against each provider's oracle/golden vectors for
  matched state and explicit fee; any legitimate cross-provider rounding
  difference is preserved by provider-specific expected output;
- Camelot/Uniswap p95 and p99 at most `1.05x` for prepared quote lookup;
- Camelot/Uniswap p95 and p99 at most `1.10x` for matched event decode/apply,
  plan materialization, calldata build, and receipt decode/apply;
- normalized real-pool curve-build p99 per segment at most `1.20x` the matched
  Uniswap fee-500 cohort, including both Camelot directions;
- fee-envelope calculation p99 below `25 us`, no unchanged-envelope rebuild,
  and combined fee projection plus curve publication p99 below `200 us`;
- one candidate prepared quote p99 below `3 us`;
- deterministic combined ARB decision replay p99 at most `1.05x` the previous
  milestone and below the existing `25 us` hard ceiling;
- frozen Uniswap-only quote, event, curve-build, and decision p95/p99 no more
  than `1.05x` their pre-Camelot report;
- existing target-runtime p95 no more than `1.15x` and p99 no more than
  `1.20x` the frozen production reference while satisfying every independent
  hard ceiling in `docs/multi-pair-multi-network-runtime.md`;
- zero hot telemetry, canonical DEX event, execution command, and unknown queue
  drops;
- no increase in existing Uniswap allocations, RPC calls, durable barriers, or
  per-frame candidate work when Camelot is absent from an artifact.

The relative margins are non-inferiority noise allowances, not budgets to
spend. A repeatable slowdown below a ceiling is still explained and recorded.
If host noise makes the ratio unstable, increase rounds or move the comparison
to the target C4; do not widen the gate.

## Implementation milestones and per-step gates

### P0 — Freeze Uniswap controls and extend the harness

- Freeze current source, exact compiled artifacts, local report, and target-C4
  report before Camelot code.
- Add read-only matched Algebra/Uniswap fixtures and report schema fields for
  directional fees and fee projection.
- Keep benchmark-only code out of production paths.

Exit gate: repeated control runs enforce the common gates; the harness leaves
the existing capacity replay within `1.05x` and under every hard ceiling.

### P1 — Provider types, schema, and compiler

- Add explicit Camelot provider/config/compiled-protocol/cost identities. The
  durable execution-route shape remains deliberately unavailable until P6.
- Compile test artifacts containing the expected pool without changing the
  checked-in production artifact.
- Preserve typed constant-time indexes and unchanged Uniswap-only artifacts.

Exit gate: deterministic compiler tests pass; existing artifacts are
byte-for-byte unchanged; load time, memory, Uniswap controls, and capacity
replay meet every common gate.

### P2 — Algebra arithmetic and prepared-curve core

- Implement or isolate Algebra V1.9 tick traversal and price movement behind
  the existing quote/curve interfaces.
- Support explicit directional fee input; do not implement adaptive fee in the
  quote loop.
- Prove exact-input/output behavior in both directions at boundaries, adjacent
  base units, tick crossings, rounding edges, and insufficient liquidity.

Exit gate: byte-exact matched-fixture parity; prepared quote p95/p99 at most
`1.05x` Uniswap; curve build meets normalized `1.20x`; no Uniswap regression.

### P3 — Pinned discovery, hydration, and dynamic-fee parity

- Add same-block factory/pool/deployer/router/Quoter identity checks.
- Hydrate Algebra head, tick table/ticks, timepoints, data-storage configuration,
  and volume state atomically.
- Reproduce both directional fees and the timestamp envelope locally.
- Compare local exact-input/output results and returned fee with Camelot Quoter
  in both directions at 6, 50, and 200 USDC-scale amounts, every prepared
  boundary, adjacent base units, multiple timestamps, and fee transitions.

Exit gate: byte-exact amount and fee parity at pinned blocks; partial state is
never published; no extra Uniswap RPC calls; fee projection and normalized
build gates pass.

### P4 — Canonical events, fee state, mirror, and refresh

Status: complete. The reviewed local evidence is frozen in
`docs/performance/camelot-v3-arb-usdc-p4-local.json`; canonical transition
replay is byte-exact, event/apply is within `1.10x` Uniswap, publication is
below `200 us`, unchanged envelopes do not rebuild, and the full quality gate
passes.

- Route and apply Camelot Fee, Swap, Mint, Burn, TickSpacing, and Incentive
  events in canonical order.
- Derive the mirrored timepoint/volume state required by the next fee update.
- Add gap, duplicate, reorder, reorg, same-transaction Fee-before-Swap,
  reconnect, unchanged-head, and unsupported-change fixtures.

Exit gate: matched event p95/p99 at most `1.10x` Uniswap; publication below
`200 us`; no rebuild for unchanged envelopes; zero event drops; all existing
Uniswap event percentiles and fixtures remain within gates.

### P5 — Opportunity and adaptive sizing integration

Status: complete. The reviewed local evidence is frozen in
`docs/performance/camelot-v3-arb-usdc-p5-local.json`; Camelot is in the normal
provider-stable candidate set, dynamic fee generation is bound into telemetry,
durable opportunity identity, and entry preflight, and the 2M-frame mixed
capacity replay retains the P4 `83 ns` decision p99 with zero route failures.

- Publish Camelot curves into the normal candidate set in deterministic local
  and replay fixtures.
- Add provider-stable selection, fee-generation telemetry, and preflight.
- Extend maximum-pair replay with mixed Uniswap/Pancake/Camelot dependencies and
  bursty same-stream events and heads.

Exit gate: prepared quotes at most `1.05x` Uniswap; combined ARB replay at most
`1.05x` P4 and below `25 us`; no allocation/lock/I/O; unrelated strategies and
Uniswap controls remain within gates.

### P6 — Plan, calldata, allowance model, and disabled executor

Status: complete. The reviewed local evidence is frozen in
`docs/performance/camelot-v3-arb-usdc-p6-local.json`; the durable route binds
provider, pool/fee generations, directional fee envelope, horizon, and
deadline, the router call is the exact seven-word `0xbc651188` tuple, the
pinned historical call succeeds through read-only `eth_call`, and new Camelot
broadcast remains fail-closed until the direct-live startup allowance lock in
P8. Calldata and plan p99 are respectively `0.936x` and `1.021x` Uniswap, the
2M-frame mixed replay remains below the frozen ceiling, and the full quality
gate passes.

- Add durable Camelot route and selector `0xbc651188` with exact seven-word
  tuple, immutable deadline, and fee horizon.
- Add provider-specific allowance requirements and gas-policy evidence.
- Keep broadcast and allowance mutation disabled; prove signing identity and
  exact call through local golden vectors and pinned read-only simulation.

Exit gate: matched plan/calldata p95/p99 at most `1.10x`; journal replay cannot
cross providers; enqueue controls and the entire existing executor suite do not
regress.

### P7 — Receipt proof, self-impact, and recovery composition

Status: complete. The reviewed local evidence is frozen in
`docs/performance/camelot-v3-arb-usdc-p7-local.json`; a pinned successful
ARB/USDC transaction replays through read-only `eth_call` and
`eth_estimateGas`, its receipt proves exact wallet deltas and positional
`Fee(6) -> Swap(11)`, and the compact typed Fee proof applies byte-identically
to the canonical log path. The owner drains queued WebSocket events before
receipt apply, reuses a bounded canonical timestamp cache, publishes the
affected curves, and does not perform a second log RPC or durable settlement
barrier. Receipt accounting/proof p99 is `1.026x`, event/apply stays within
`1.10x`, and matched receipt-to-lane-release publication is `0.767x` Uniswap.

- Prove positional Fee/Swap logs, wallet deltas, and direct local settlement.
- Exercise success, known revert, timeout, unknown receipt, restart
  reconciliation, and DEX-success/Binance-partial recovery fixtures.
- Produce complete immutable plans and exercise pinned read-only `eth_call` and
  gas estimation without signing, allowance mutation, broadcast, synthetic
  fills, or a production paper deployment.

Exit gate: receipt/apply/rebuild p95/p99 at most `1.10x` matched Uniswap and
receipt-to-lane-release at most `1.15x`; no second RPC wait or durable barrier;
all Uniswap and Binance recovery controls remain unchanged; read-only
simulation matches local quote and fee state exactly; there is no account,
nonce, balance, or recovery mutation.

### P8 — Direct production full-live rollout

Status: implementation and local pre-rollout gates complete. The reviewed
evidence is frozen in
`docs/performance/camelot-v3-arb-usdc-p8-local.json`. The versioned v5 source
makes the route immediately `execution_eligible`; there is no canary. The exact
image must still pass its embedded paired Camelot/Uniswap quote/build gate and
the 2M-frame capacity replay on the fixed C4 before the workflow changes the
running Deployment.

The operator decision recorded on 2026-08-05 is to enable Camelot directly in
full-live ARB/USDC production without an observe-only, paper, reduced-size, or
canary deployment.

- Record canonical identity, all P0-P7 reports, exact allowance set, gas
  fallback, previous image digest, reconciliation, and rollback procedure.
- Publish one new versioned domain source with Camelot selection and execution
  enabled for the existing `full_live` ARB/USDC strategy.
- Keep the existing 6 USDC detector, 20 bps raw gross gate, Binance-step-aligned
  adaptive sizing up to 200 USDC, exact inventory reservations, one serialized
  EVM lane, immutable IOC protection, and bounded MARKET recovery. Camelot gets
  no smaller initial cap and no separate economic model.
- Build the exact immutable image through CI and run paired provider benchmarks,
  the frozen Uniswap controls, and the maximum-pair replay on the fixed target
  C4 before rollout. Any failed gate stops deployment before production changes.
- Prepare and lock the exact Camelot router allowances during reviewed startup,
  before the route becomes ready.
- Deliver only through `main` and the immutable-image `Deploy GKE` workflow;
  keep GCE terminated and do not add a second Pod.
- The first eligible Camelot opportunity may submit a real transaction at the
  full adaptive size. Monitor provider-separated correctness and performance
  from that transaction onward without calling the interval a canary.
- A parity fault, unknown exposure, relevant queue drop, hard latency failure,
  or repeatable regression closes new Camelot entries, reconciles every active
  operation, and rolls back through a new reviewed `main` revision to the
  previous digest. Already granted Camelot allowances remain inert and locked.

Exit gate: the exact pre-rollout image passes every functional and performance
gate; joined production selection/execution/accounting evidence remains
complete; Camelot local stages stay comparable with Uniswap; existing Uniswap
performance remains within frozen gates; rollback needs no inventory migration
or second owner.

## Telemetry contract

Every Camelot pool/event/evaluation/selection/plan/execution/receipt record
carries:

- `dex_protocol=camelot_v3`, network, pair, strategy, stable pool ID, and
  canonical address;
- pool generation, fee generation, source block/log position, tick spacing,
  directional current fees, envelope fees, horizon, and state fingerprint;
- event profile and canonical position, including Fee-before-Swap linkage;
- prepared segment counts and fee/build/publication stage timings;
- candidate/selected status and exact quote inputs/outputs;
- typed router/calldata profile without arbitrary signed calldata dumps;
- receipt Fee/Swap proof, wallet deltas, actual output, gas, and outcome;
- local queue, decision, execution, and settlement stages used by gates.

Reports group by provider and pool before aggregating. A fast Uniswap or
Pancake cohort cannot hide a slow Camelot cohort, and adding Camelot cannot hide
a regression in existing providers. Formatting stays on the bounded background
telemetry owner.

## Functional verification matrix

| Area | Required proof |
| --- | --- |
| identity | same-block factory/pool/deployer, expected address, tokens, router, Quoter, bytecode |
| hydration | pinned global state, tick table/ticks, timepoints, data operator/config, partial rejection |
| fee | exact directional update and horizon envelope parity across timestamps and transitions |
| math | Quoter exact-input/output amount and returned-fee parity in both directions and boundaries |
| events | Fee-before-Swap, Swap, Mint/Burn, gap/reorder/reorg, TickSpacing/Incentive fail-close |
| planning | explicit provider/router/pool/generations/horizon/deadline and exact amounts |
| calldata | selector `0xbc651188`, golden ABI vectors, pinned `eth_call` simulation |
| allowance | exact spender/token set, idempotent journal, direct-live startup preparation and lock |
| receipt | positional Fee/Swap plus exact wallet deltas and actual output |
| self-impact | queued drain, receipt apply, affected rebuild, no second-log wait |
| recovery | known revert, unknown EVM outcome, restart, Binance partial/zero IOC |
| isolation | Camelot failure cannot corrupt existing providers or unrelated strategies |
| performance | every P0-P8 report, paired Camelot/Uniswap comparison, frozen Uniswap non-regression |

Before every handoff, `scripts/quality.sh` passes. Before any rollout, the
compiled domain is deterministic and the exact image passes target-C4 replay.
Deployment verification asserts Camelot lifecycle, provider dependency,
directional fee readiness, and provider-separated performance fields.

## Non-goals

- Camelot AMM V2, AMM V4/Algebra Integral, Yak aggregation, or fee-on-transfer
  swaps;
- multi-hop ARB/WETH/USDC, USDC.e, split routing, or selecting another pool;
- treating the last observed fee as permanently executable;
- remote Quoter, RPC, or wall-clock adaptive-fee work in decision, sizing,
  admission, preflight, or transaction construction;
- changing the opportunity threshold, detector notional, execution cap,
  slippage model, execution ordering, recovery, wallet, signer, or nonce lane;
- relaxing an existing latency ceiling to accommodate Camelot;
- a production observe-only, paper, reduced-size, or canary phase before
  direct `full_live` enablement.

## Sources

- [Camelot Arbitrum One deployments](https://docs.camelot.exchange/contracts/arbitrum/one-mainnet)
- [Camelot AMM V3 overview](https://docs.camelot.exchange/protocol/amm-v3)
- [Algebra Camelot V1.9 directional-fee deployment](https://docs.algebra.finance/algebra-integral-documentation/overview-faq/partners/algebra-v1.9/camelot)
- [Algebra V1.9 source](https://github.com/cryptoalgebra/AlgebraV1.9)
- [Algebra V1.9 router interface](https://github.com/cryptoalgebra/AlgebraV1.9/blob/main/src/periphery/contracts/interfaces/ISwapRouter.sol)
- [Algebra V1.9 Quoter interface](https://github.com/cryptoalgebra/AlgebraV1.9/blob/main/src/periphery/contracts/interfaces/IQuoter.sol)
- [Reviewed Camelot ARB/USDC pool](https://arbiscan.io/address/0xfae2ae0a9f87fd35b5b0e24b47bac796a7eefea1)
- [Reviewed Camelot V3 router](https://arbiscan.io/address/0x1F721E2E82F6676FCE4eA07A5958cF098D339e18)

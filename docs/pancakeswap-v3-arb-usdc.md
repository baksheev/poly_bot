# PancakeSwap V3 ARB/USDC support specification

Status: proposed; specification only, no runtime or production change

Last reviewed: 2026-08-05

## Decision

Add PancakeSwap V3 as a distinct DEX provider and add exactly one initial
direct ARB/USDC route on Arbitrum One:

- protocol: `pancakeswap_v3`;
- fee: `500` (0.05%);
- pool: `0x9ffca51d23ac7f7df82da414865ef1055e5afcc3`;
- token0 ARB: `0x912ce59144191c1204e64559fe8253a0e49e6548`;
- token1 native USDC: `0xaf88d065e77c8cc2239327c5edb3a432268e5831`.

The route joins the existing ARB/USDC strategy as another immutable candidate.
It does not create a second strategy, split an order across pools, change the
6 USDC detector notional, change the 20 bps gross gate, or raise the 200 USDC
adaptive execution cap. Candidate ranking remains the largest exact
Binance-step-aligned size that clears the existing raw venue-economics gate.

PancakeSwap V3 reuses the existing allocation-free `ClmmPool`, prepared curves,
adaptive sizing, admission, reservation, Binance hedge, bounded recovery, nonce,
journal, and accounting semantics. It has its own provider identity, canonical
contracts, router calldata profile, Swap event decoder, receipt proof, gas
evidence, telemetry dimensions, and configuration fields.

Every implementation step is stop-the-line gated by functional correctness and
paired performance evidence. A failed gate is fixed or the step is reverted
before work continues. Passing only the final aggregate benchmark is not enough.

## Reviewed deployment and route identity

The PancakeSwap deployment used by this specification is:

| Contract | Arbitrum One address | Runtime role |
| --- | --- | --- |
| PancakeV3Factory | `0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865` | startup discovery and identity proof |
| SwapRouter (V3-only) | `0x1b81D678ffb9C0263b24A97847620C99d213eB14` | exact-input execution |
| QuoterV2 | `0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997` | pinned parity oracle only |

The broader PancakeSwap Smart Router at
`0x32226588378236Fd0c7c4053999F88aC0e5cAc77` is deliberately excluded. The
V3-only router has the narrower reviewed execution surface, supports a deadline,
and avoids enabling V2, stable-swap, mixed-route, or aggregation behavior.

Read-only verification at Arbitrum block `491332082`, hash
`0x07e933034e181695966aafb447778cf783a8e2ca7ae978b62e82842a38c32dc6`,
timestamp `2026-08-05T10:35:05Z`, proved:

- `factory.getPool(ARB, USDC, 500)` returned the selected pool;
- the pool returned the exact token0/token1 addresses above;
- `fee()` returned `500`, `tickSpacing()` returned `10`, and `factory()`
  returned the reviewed PancakeV3Factory;
- factory, router, QuoterV2, and pool all had non-empty runtime bytecode;
- router and QuoterV2 both exposed the reviewed factory address.

The same factory also returned ARB/USDC pools at fees 100, 2500, and 10000.
They are not part of the initial route:

| Fee | Pool | 30 complete days volume, 2026-07-06..2026-08-04 | 2026-08-05 screening |
| ---: | --- | ---: | --- |
| 100 | `0x93cce474015007b38da0ecea96671ee4dc3d40ad` | about $1,068.64 | about $2.1K liquidity; defer |
| 500 | `0x9ffca51d23ac7f7df82da414865ef1055e5afcc3` | about $451,571.43 | about $19.0K liquidity; **select** |
| 2500 | `0x7795d58e3f941592d07f7a7026e93b6a03ea8865` | no material activity | dust liquidity; reject |
| 10000 | `0x86a1d7d8cbddad1fdb931182d52624950ca1d066` | no material activity | dust liquidity; reject |

External volume and liquidity only select which route is worth implementing.
They never enter readiness, sizing, admission, preflight, or execution. The
versioned artifact, canonical chain reads, local curve, and current Binance
price remain authoritative.

## Protocol compatibility boundary

PancakeSwap V3 is close enough to Uniswap V3 to share mathematical state and
quote code, but not close enough to masquerade as `uniswap_v3`.

### Shared core

The following pool reads and state have compatible types and are implemented by
the existing V3 hydration/CLMM path:

- `slot0`, `liquidity`, `tickSpacing`, `tickBitmap`, and `ticks`;
- Q64.96 price, tick, liquidity, tick crossing, fee-pip, exact-input, and
  exact-output math;
- prepared execution-envelope curves and baseline quote rings;
- `Mint` and `Burn` event layouts;
- exact integer output and insufficient-liquidity behavior.

There is one shared implementation, not a copied Pancake quote loop. Exact
QuoterV2 parity is nevertheless required because shared source ancestry is not
a correctness proof.

### Provider-specific ABI

The following differences remain explicit:

1. PancakeSwap's pool `Swap` event is
   `Swap(address,address,int256,int256,uint160,uint128,int24,uint128,uint128)`.
   The two final values are protocol fees. Uniswap V3's current decoder expects
   the seven-argument event and five data words. PancakeSwap therefore needs a
   distinct topic and seven-data-word decoder. The price, liquidity, and tick
   remain at data positions 2, 3, and 4.
2. The selected V3-only Pancake router accepts
   `exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))`.
   The fields are token in/out, fee, recipient, deadline, amount in, minimum
   output, and zero sqrt-price limit. It must not reuse the current Uniswap
   SwapRouter02 `exactInput((bytes,address,uint256,uint256))` encoder.
3. Pancake QuoterV2 uses the compatible
   `quoteExactInputSingle((address,address,uint256,uint24,uint160))` and
   exact-output tuple layouts, but its address and provider identity remain
   distinct.
4. ERC-20 allowance is granted only to the Pancake V3-only router, validated at
   startup, and then allowance mutation is permanently locked exactly as for
   the current immediate execution path.
5. Protocol fees reported in Pancake Swap events are a subdivision of the
   already quoted pool fee. They are telemetry only and are not deducted a
   second time from opportunity economics.

## Domain and type model

The versioned source schema gains explicit optional fields:

```json
{
  "chain": {
    "pancakeswap_v3_factory_address": "0x0BFb...1865",
    "pancakeswap_v3_quoter_address": "0xB048...997",
    "pancakeswap_v3_router_address": "0x1b81...B14"
  },
  "dex": {
    "allowed_providers": ["uniswap_v3", "pancakeswap_v3"],
    "uniswap_v3": { "fee_tiers": [500, 3000] },
    "pancakeswap_v3": {
      "pools": [
        {
          "fee_tier": 500,
          "expected_address": "0x9ffc...fcc3"
        }
      ]
    }
  }
}
```

The expected address is mandatory and must equal the same-block factory result.
The compiler emits a `PoolProtocol::PancakeSwapV3` node and a stable pool ID
whose protocol component is not `uniswap_v3`. Strategy dependencies contain
both existing Uniswap pool IDs and the one selected Pancake pool ID.

Runtime types preserve provider identity through hydration, mirror lookup,
prepared generation, opportunity selection, durable plan, execution request,
receipt proof, cost telemetry, and realized accounting. A suitable internal
shape is `PoolIdentity::V3 { protocol, address, fee_pips }`, where `protocol` is
a compact closed enum. Durable/public types still expose explicit
`UniswapV3` and `PancakeSwapV3` route variants so a journal cannot be replayed
against the wrong router.

Adding PancakeSwap must not turn protocol or pool lookup into a string-key scan.
The compiler assigns stable typed IDs and builds the existing adjacency indexes
once at startup.

## Startup, ingestion, and recovery

Startup retains the current race-free sequence:

1. Select canonical block `B` for Arbitrum.
2. At exactly `B`, resolve the configured Pancake pool from its factory and
   compare the expected address, token order, fee, tick spacing, factory, and
   bytecode.
3. Hydrate the head, bitmap, and initialized ticks through the process-scoped
   Arbitrum read coordinator. Shared compatible calls may be batched with
   Uniswap reads; partial batches are never published.
4. Subscribe over the existing process-scoped Arbitrum WSS connection to the
   selected pool's Pancake Swap topic plus the shared Mint/Burn topics.
5. Capture head `C`, backfill `(B, C]`, apply canonical order, discard buffered
   duplicates, build the three prepared curves, and only then mark this pool
   ready.

While Pancake is observe-only, its failure is isolated from the executable
Uniswap-only ARB strategy. Once the Pancake provider is enabled as an execution
candidate, the existing fail-closed rule applies: every configured candidate
must be coherent, so an unavailable pool, gap, removed log, parent mismatch,
invalid event, or parity failure disables new ARB entries until pinned
rehydration completes. The fault must not corrupt other pool state or stop
unrelated WLD/ESP strategies. Existing account, wallet, Binance, or
network-level faults retain their broader fail-closed scope.

Pool events are routed by the compiled address-to-pool index before decoding.
This provides the provider profile needed to reject a Uniswap Swap topic from a
Pancake address and vice versa. No generic "try every V3 decoder" loop is
allowed in the event path.

## Quoting, selection, and execution

The selected Pancake pool participates in the same two-direction baseline and
adaptive-size calculation as each Uniswap pool. It receives no provider
priority and no linearized quote shortcut. Ties use the existing deterministic
ordering extended with the stable pool ID.

The hot path remains:

- local and integer-only;
- free of RPC, Quoter, database, lock, JSON construction, serialization, and
  steady-state heap allocation;
- a borrowed single-owner path for baseline evaluation;
- generation-checked before accepting asynchronous sizing and again at entry
  preflight.

Execution uses a direct single-pool exact-input call to the V3-only router. The
deadline from the immutable DEX plan is encoded and must still be in the future
before signing. `amount_in` is unchanged; slippage only reduces
`amount_out_minimum`. The router cannot select a different pool or protocol.

The receipt must prove exactly one positional Pancake Swap event from the
planned pool and exact wallet transfer deltas. On success, the execution owner
non-blockingly drains already queued Arbitrum events, applies the receipt's
Pancake Swap directly to the local mirror, and rebuilds only the affected
prepared curves before releasing the execution lane. It never waits for a
second `eth_getLogs` copy and introduces no pool-wide or global settlement
barrier.

Known reverts, unknown outcomes, Binance IOC protection, MARKET recovery,
commission accounting, and inventory drift retain the existing behavior. A DEX
provider does not get its own wallet, Binance account, recovery policy, or
nonce lane merely because it has a different router.

## Gas and allowance evidence

The existing Uniswap gas fallback is not automatically inherited. Before live
execution, collect Pancake fee-500 direct-swap receipt gas from both directions
and exercise read-only simulation/estimation against the exact V3-only calldata.
The reviewed provider-specific fallback must cover the observed maximum with an
explicit margin while remaining below the executor safety ceiling. At least 100
representative receipts are required for a tail claim; a smaller cohort is
reported without claiming a p99.

Arbitrum's existing gas-price cache, 12,000 bps maximum-fee headroom, zero
priority tip, native-funding invariant, and receipt accounting remain
unchanged. Gas is neither a sizing nor an admission input.

Startup prepares max allowance for ARB and USDC to the exact Pancake V3-only
router under separately journaled idempotency keys, verifies it, and locks all
further allowance mutation before immediate execution is enabled. No approval
is authorized while the route is observe-only. The first approval belongs to
the same reviewed production revision that enables direct live execution.

## Performance preservation contract

### Meaning of comparable

"PancakeSwap performance is comparable to Uniswap" has three separate proofs:

1. **Adapter overhead:** against identical synthetic V3 state, Pancake and
   Uniswap quote, event, plan, calldata, and receipt operations have the same
   asymptotic behavior and the ratios below.
2. **Real-pool cost:** pinned real ARB/USDC pools are measured with their actual
   tick density and segment counts. Curve-build time is reported both absolute
   and per published segment, so liquidity topology is not confused with
   adapter overhead.
3. **Runtime non-regression:** adding the candidate does not violate existing
   ARB/USDC or global hot-path relative gates, hard ceilings, isolation, or drop
   counters.

External RPC confirmation, sequencer inclusion, and venue gas distributions are
reported per provider but are not used to claim local-code superiority. A slow
external sample cannot excuse a new local queue, lock, allocation, or handoff.

### Measurement protocol

Each milestone records a machine-readable report under a versioned benchmark
schema. The report contains source revision, artifact ID, Rust/toolchain
version, build profile, CPU model/class, kernel, sample counts, warmups, round
order, pool identity, segment count, allocation count, and p50/p95/p99/max.

Local development uses paired, interleaved release measurements on the same
otherwise-idle host. Each microbenchmark has at least 30 rounds and at least
one million timed operations in total. The comparison uses the median of round
percentiles and reports dispersion; one unusually fast run may not become the
baseline. Target-node claims use the exact immutable image and the existing
pre-rollout replay gate on the fixed `c4-highcpu-8`; local Docker is never used.

For every step:

1. run `scripts/quality.sh`;
2. run the provider-paired microbenchmark suite;
3. run the current maximum-pair/capacity replay with network I/O and external
   mutation disabled;
4. compare with both the previous milestone report and the frozen Uniswap
   control report;
5. attach the report and explain every regression beyond noise;
6. stop if any required gate fails.

The common gates are:

- zero network calls, locks, serialization, or allocations in steady-state
  quote and Binance-frame-to-baseline paths;
- identical quote results for the shared fixture;
- Pancake/Uniswap p95 and p99 ratio at most `1.05` for prepared quote lookup;
- Pancake/Uniswap p95 and p99 ratio at most `1.10` for matched event
  decode/apply, plan/calldata build, and receipt decode/apply;
- normalized real-pool curve build p99 per segment at most `1.10x` the matched
  Uniswap fee-500 cohort;
- one candidate prepared quote p99 below `3 us` and prepared-curve publication
  p99 below `200 us`;
- deterministic combined ARB decision replay p99 no more than `1.05x` the
  previous milestone and no more than the `25 us` hard ceiling;
- existing target-runtime p95 no more than `1.15x` and p99 no more than
  `1.20x` the frozen production reference, while still meeting all independent
  hard ceilings in `docs/multi-pair-multi-network-runtime.md`;
- zero hot telemetry, canonical DEX event, execution command, and unknown queue
  drops;
- no increase in existing Uniswap fixture allocations, RPC call counts, or
  durable barriers.

The 1.05 deterministic margin is a non-inferiority noise allowance, not a
performance budget to spend. A repeatable slowdown below that margin is still
investigated and recorded.

## Implementation milestones and per-step gates

### P0 — Freeze the control and add the benchmark harness

- Freeze the current ARB/USDC Uniswap-only source revision and exact compiled
  artifact.
- Add read-only matched V3 fixtures and machine-readable latency/allocation
  output. The harness is not linked into the production runtime path.
- Capture local release and target C4 control reports before provider code.

Exit gate: repeated control runs are stable enough to enforce the common gates;
the harness itself leaves the existing capacity replay within `1.05x` and under
all hard ceilings.

### P1 — Provider types, schema, and compiler

- Add explicit Pancake provider/config/protocol/route/cost identities.
- Compile the selected expected pool into the ARB strategy dependency graph.
- Keep the production source observe-only and execution-disabled at this step.

Exit gate: deterministic compiled-artifact tests pass; Uniswap-only artifacts
are byte-for-byte unchanged; control quote, decision replay, load time, and
memory meet the common gates.

### P2 — Pinned discovery and hydration

- Share the compatible V3 hydration implementation behind a typed provider
  profile.
- Validate the factory result and every reviewed pool field at one block.
- Add Pancake QuoterV2 exact-input and exact-output parity in both directions at
  6, 50, and 200 USDC-scale amounts, every prepared segment boundary, adjacent
  base units, tick crossings, and insufficient liquidity.

Exit gate: byte-exact parity, no additional Uniswap RPC calls, no partial
publication, normalized decode/build at most `1.10x`, and unchanged decision
replay.

### P3 — Canonical events, mirror, and curve refresh

- Add the Pancake-specific Swap topic/layout and provider-directed decoder.
- Reuse compatible Mint/Burn application and generation rules.
- Add gap, duplicate, reorder, reorg, coalescing, and reconnect fixtures.

Exit gate: matched event decode/apply p99 at most `1.10x` Uniswap; real pool
publication p99 below `200 us`; DEX receive-to-owner p99 below `175 us`; no
event drops; Uniswap event fixtures and percentiles do not regress.

### P4 — Opportunity and adaptive sizing integration

- Publish Pancake prepared curves into the existing pool candidate set.
- Add provider-stable tie breaking, selection telemetry, and generation
  preflight.
- Extend maximum-pair replay with mixed Uniswap/Pancake dependencies and bursty
  updates to both providers on the same Arbitrum stream.

Exit gate: matched prepared quotes at most `1.05x` Uniswap, combined ARB replay
p99 at most `1.05x` the P3 report and below `25 us`, fairness is unchanged, and
all no-allocation/no-lock/no-I/O assertions pass.

### P5 — Plan, calldata, allowance, and executor support

- Add the durable Pancake route and the V3-only `exactInputSingle` encoder with
  deadline.
- Add provider-specific allowance requirements and gas policy evidence.
- Keep runtime execution disabled; prove the signing identity and exact call
  locally without broadcasting or introducing a simulated trading mode.

Exit gate: matched plan/calldata p99 at most `1.10x`; journal replay cannot
cross providers; no new hot-path work; executor queue and enqueue-to-first-write
control cohorts stay within existing relative gates.

### P6 — Receipt proof, self-impact, and recovery composition

- Prove the Pancake positional Swap, wallet deltas, protocol-fee fields, and
  direct mirror settlement.
- Exercise success, revert, timeout, unknown receipt, restart reconciliation,
  and DEX-success/Binance-partial recovery fixtures.

Exit gate: matched receipt decode/apply/rebuild p99 at most `1.10x`; local
receipt-to-lane-release p99 at most `1.15x` Uniswap with equal fixture work;
there is no extra RPC wait or durable barrier; all existing Uniswap and Binance
recovery tests remain unchanged.

### P7 — Production-shaped observe-only shadow

- Deploy only through `main` and the reviewed `Deploy GKE` workflow, initially
  with Pancake selection/execution disabled.
- Hydrate and follow the pool, build curves, evaluate shadow candidates, and
  emit provider-separated telemetry from the sole GKE owner.
- Do not create another Pod or let the stopped GCE owner control the account.

The comparison window requires at least 100,000 ARBUSDC strategy frames, 1,000
Pancake pool events/curve publications, zero relevant drops, and stable target
CPU/throttling/memory evidence. If 1,000 pool events are not available, the
window remains collecting and is supplemented by target-C4 captured replay; it
does not make a live tail claim.

Exit gate: all frozen production relative and hard gates pass for ARB and for
unrelated WLD/ESP strategies; Pancake event/build distributions satisfy the
same absolute V3 bounds; no background cohort moves decision tails.

### P8 — Direct production live canary and full-live continuation

There is no paper-trading phase. After P7 observe-only evidence passes, the next
reviewed revision enables the Pancake fee-500 candidate directly in production
with real ARB/USDC inventory and the existing live DEX-first/Binance-recovery
coordinator.

- Record the pre-deploy review of the canonical contracts, exact router
  allowance set, provider gas fallback, P7 performance report, previous image
  digest, and reconciliation procedure.
- Publish one new versioned domain source that enables Pancake selection and
  execution for the existing full-live ARB/USDC strategy. The first eligible
  Pancake opportunity may therefore submit a real production transaction; no
  paper, synthetic-order, or non-broadcast trading stage sits between P7 and
  live execution.
- Retain the existing live bounds without a separate economic model: 6 USDC
  detector, 20 bps raw gross gate, Binance-step-aligned adaptive sizing, 200
  USDC maximum DEX notional, exact inventory reservations, one serialized EVM
  lane, immutable IOC protection, and bounded MARKET recovery.
- Prepare and lock the exact Pancake router allowances during reviewed startup
  before the route becomes ready. Approval and trading mutations use distinct
  durable idempotency keys and the same single nonce owner.
- Deliver the exact revision only through `main` and the immutable-image
  `Deploy GKE` workflow. Never use a workstation deployment, local Docker,
  direct `kubectl` rollout, GCE restart, or a second live owner.
- Monitor provider-separated selection, calldata, receipt, self-impact,
  Binance hedge/recovery, gas, and performance telemetry from the first live
  transaction onward. A correctness fault, unknown exposure, drop, hard
  latency-gate failure, or repeatable regression beyond the comparison margin
  closes new Pancake entries and triggers reconciliation before rollback to the
  previous image digest.
- Continue full-live operation after the canary window only while P7's hot-path
  gates still pass and the live Pancake execution/accounting cohort contains no
  unresolved outcome. A cohort below 100 executions reports exact samples and
  maxima without claiming p99.

Exit gate: joined production selection/execution/accounting evidence is
complete; external latencies and gas are reported with exact cohort sizes;
Pancake local stages remain comparable with Uniswap; existing Uniswap and
Binance recovery controls remain within gates; and rollback to the prior image
digest requires no inventory migration.

## Telemetry contract

Every pool/event/evaluation/selection/plan/execution/receipt/cost record carries:

- `dex_protocol` (`uniswap_v3`, `pancakeswap_v3`, or `uniswap_v4`);
- stable network, pair, strategy, pool ID, canonical pool identity, and fee;
- pool generation and source block/log position;
- prepared segment counts and build/publication stages;
- candidate/selected status and exact quote inputs/outputs;
- router and calldata profile as non-secret typed identifiers, not arbitrary
  calldata dumps;
- receipt Swap topic/profile, protocol fees, wallet deltas, gas, and outcome;
- local queue/build/decision/execution stages needed for the performance gates.

Reports group by provider and pool before aggregating. A fast Uniswap cohort may
not hide a slow Pancake cohort, and adding Pancake may not hide a regression in
the original Uniswap pools. Telemetry formatting remains on the bounded
background owner.

## Functional verification matrix

| Area | Required proof |
| --- | --- |
| identity | same-block factory, expected address, tokens, fee, spacing, factory, bytecode |
| hydration | pinned complete bitmap/ticks; partial batch rejection; unavailable states |
| math | QuoterV2 exact-input/output parity in both directions and at boundaries |
| events | Pancake Swap with two protocol-fee words; Mint/Burn; order/gap/reorg |
| planning | explicit provider/router/pool; direct route; deadline; exact amounts |
| calldata | golden selector/ABI vectors and pinned `eth_call` simulation |
| allowance | exact spender/token set, idempotent journal, startup lock |
| receipt | one positional selected-pool Swap plus exact wallet deltas |
| self-impact | queued drain, direct receipt apply, affected-pool rebuild, no second log wait |
| recovery | known revert, unknown outcome, restart, Binance partial/zero IOC |
| isolation | Pancake degradation does not corrupt coherent Uniswap pools |
| performance | every P0-P8 exit gate and the common paired comparison |

Before each handoff, `scripts/quality.sh` must pass. Before production rollout,
the checked-in compiled domain must be deterministic, the exact image must pass
the existing target-C4 replay before rollout, and deployment verification must
assert the selected Pancake pool/provider dependency and provider-separated
readiness fields.

## Non-goals

- PancakeSwap V2, Infinity, Smart Router aggregation, mixed or stable routes;
- multi-hop ARB/WETH/USDC or USDC.e routes;
- remote Quoter calls in decision, sizing, admission, or preflight;
- split execution across Pancake and Uniswap;
- new wallets, Binance accounts, signers, nonce lanes, or recovery semantics;
- adding the fee-100, fee-2500, or fee-10000 pools without a separate route
  review and the same performance sequence;
- relaxing existing latency ceilings to accommodate another pool.

## Sources

- [PancakeSwap V3 deployment addresses](https://docs.pancakeswap.finance/to-delete/smart-contracts/pancakeswap-exchange/v3-contracts)
- [Pancake V3 pool interface](https://docs.pancakeswap.finance/developers/smart-contracts/pancakeswap-exchange/v3-contracts/pancakev3pool)
- [Pancake V3-only SwapRouter source](https://github.com/pancakeswap/pancake-v3-contracts/blob/main/projects/v3-periphery/contracts/SwapRouter.sol)
- [Pancake V3 router interface and deadline-bearing parameter structs](https://github.com/pancakeswap/pancake-v3-contracts/blob/main/projects/v3-periphery/contracts/interfaces/ISwapRouter.sol)
- [Pancake V3 pool event interface](https://github.com/pancakeswap/pancake-v3-contracts/blob/main/projects/v3-core/contracts/interfaces/pool/IPancakeV3PoolEvents.sol)
- [Pancake QuoterV2 interface](https://github.com/pancakeswap/pancake-v3-contracts/blob/main/projects/v3-periphery/contracts/interfaces/IQuoterV2.sol)
- [GeckoTerminal API documentation](https://apiguide.geckoterminal.com/)

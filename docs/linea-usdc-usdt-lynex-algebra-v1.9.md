# Linea USDC/USDT Lynex Algebra V1.9 production support specification

Status: approved for full-live production; implementation and release evidence
are in progress

Last reviewed: 2026-08-06

## Decision

Add one new full-live strategy on Linea Mainnet and one direct Lynex Algebra
V1.9 pool:

- product name: Linea USDC/USDT;
- internal pair ID: `linea-usdt-usdc`;
- strategy ID: `strategy:linea-usdt-usdc`;
- chain ID: `59144` (`eip155:59144`);
- Binance Spot symbol: `USDCUSDT`;
- token A / quote asset: USDT;
- token B / base asset: USDC;
- DEX provider: `lynex_algebra_v1_9`;
- pool: `0x6e9ad0b8a41e2c148e7b0385d3ecbfdb8a216a9b`;
- token0 USDC: `0x176211869cA2b568f2A7D4EE941E073a821EE1ff`;
- token1 USDT: `0xA219439258ca9da29e9cC4cE5596924745e12B93`.

The internal order is deliberately USDT/USDC because the runtime models token
A as the quote asset and Binance exposes USDC as base and USDT as quote. This
keeps DEX and Binance prices in the same quote units without inversion in the
hot path.

The first production revision containing the strategy enables arbitrage and
rebalancing directly in `full_live`. There is no observe-only production
revision, paper trading, reduced-size phase, live canary, or first-trade cap.
The first eligible opportunity may execute the normal adaptive size up to 200
USDT-equivalent and then use the existing Binance hedge and bounded recovery
flow.

Skipping paper and canary does not skip correctness evidence. All identity,
quote parity, event replay, calldata simulation, receipt proof, unknown-outcome
recovery, performance, funding, exact capital-route, and deployment gates in
this document must pass before the production artifact can select the pool.

The strategy uses the same reviewed economics as ARB/USDC:

- 6 USDT detector/control notional;
- 20 bps raw gross-spread gate;
- largest Binance-step-aligned exact DEX-curve candidate that clears the gate;
- 200 USDT adaptive execution cap;
- Rails-compatible 5-50 bps dynamic slippage only in DEX calldata bounds;
- DEX first, Binance LIMIT IOC hedge, then the existing bounded immutable
  MARKET recovery target;
- gas, commissions, recovery forecasts, Binance depth, top quantity, and
  inventory do not create sizing or admission gates.

This document specifies the implementation and records the requested rollout
shape. It does not by itself authorize an allowance, signature, transaction,
secret change, production artifact change, rollout, or live trade.

## Reviewed deployment and preliminary route identity

The Algebra-maintained Lynex deployment page identifies the following Linea
contracts:

| Contract | Address | Runtime role |
| --- | --- | --- |
| AlgebraFactory | `0x622b2c98123D303ae067DB4925CD6282B3A08D0F` | canonical discovery and identity |
| AlgebraPoolDeployer | `0x9A89490F1056A7BC607EC53F93b921fE666A2C48` | deployment identity |
| Quoter | `0x851d97Fd7823E44193d227682e32234ef8CaC83e` | pinned parity oracle only |
| SwapRouter | `0x3921e8cb45B17fC029A0a6dE958330ca4e583390` | direct exact-input execution |

The deployment also publishes WETH
`0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f` and pool init-code hash
`0xc65e01e65f37c1ec2735556a24a9c10e4c33b2613ad486dd8209d465524bc3f4`.
Neither value is inferred from Camelot.

A preliminary read-only RPC observation at Linea block `31631634`, hash
`0xd5bb0a033143a1f0dd7a3df0018349bb4cf8d2ae2bb3056c8021b076f0896ed5`,
timestamp `2026-08-06T04:31:23Z`, established:

- `factory.poolByPair(USDC, USDT)` returned the selected pool;
- `factory.poolDeployer()` returned the reviewed deployer;
- pool `token0`, `token1`, and `factory` matched the identities above;
- router and Quoter returned the reviewed factory and pool deployer;
- token symbols were `USDC` and `USDT`, both with 6 decimals;
- `tickSpacing()` returned `1`;
- `globalState()` returned tick `8`, one fee value `50` (0.005%), timepoint
  index `29932`, community fees `30`/`30`, and unlocked state;
- `dataStorageOperator()` returned
  `0x6d959b341e57e6305867d433dc1b6f00f757b944`;
- `activeIncentive()` and `liquidityCooldown()` returned zero;
- factory, deployer, Quoter, router, pool, and both tokens had non-empty code.

This observation fixes the initial target and exposes the correct ABI profile;
it is not the final release proof. Pinned P3/P6/P7 evidence must repeat all
reads at a reviewed block hash through the production provider and record
runtime-code hashes, proxy implementations where applicable, storage-layout
proof, Quoter parity, router simulation, and receipt behavior.

External identity references:

- Algebra partner deployment:
  <https://docs-v1.algebra.finance/en/docs/contracts/partners/algebra-v1.9/lynex/>;
- Algebra V1.9 source revision linked by that deployment:
  <https://github.com/cryptoalgebra/AlgebraV1.9/tree/ve3.3>;
- Circle's Linea USDC address list:
  <https://developers.circle.com/stablecoins/usdc-contract-addresses>.

The production artifact must prove through authenticated Binance capital
metadata that both assets have enabled Optimism deposit and withdrawal routes.
It must independently validate Across V4 calldata between Optimism and Linea
against these exact token mappings; symbol equality alone is insufficient.

## Protocol compatibility boundary

Lynex must not be represented as `uniswap_v3` or `camelot_v3`. It receives a
stable typed provider identity `DexProvider::LynexAlgebraV1_9` and pool identity
`PoolIdentity::LynexAlgebraV1_9 { address }` through hydration, event routing,
prepared curves, selection, durable plan, calldata, journal, settlement,
telemetry, accounting, and replay.

Lynex and Camelot both derive from Algebra V1.9, but their deployed profiles
are not ABI-identical:

1. Lynex `globalState()` returns seven words with one `uint16 fee`. The current
   Camelot directional-fee decoder expects eight words and two fee values.
2. Lynex emits `Fee(uint16)`. Camelot emits `Fee(uint16,uint16)`.
3. Lynex applies one fee to both directions. Camelot maintains independent
   zero-to-one and one-to-zero fees.
4. Both deployments expose Algebra tick tables, mutable tick spacing,
   timepoints, data storage, one pool per token pair, and the same direct
   seven-word exact-input router tuple, but this is verified rather than
   assumed.

Reusable code is restricted to byte-proved primitives:

- Q64.96 price and base-1.0001 tick math;
- Algebra raw tick-table traversal and checked tick conversion;
- exact-input/output prepared-curve interfaces;
- immutable fee-envelope interface;
- router calldata tuple and Quoter request shape after selector/ABI parity;
- existing opportunity, sizing, admission, reservation, Binance hedge,
  recovery, nonce, journal, and accounting orchestration.

Provider-specific code remains explicit for deployment identity, global-state
decode, fee state, fee event, fee projection fingerprint, event topics,
readiness, allowance, gas evidence, receipt proof, telemetry label, and durable
route identity. A Lynex plan cannot replay through the Camelot router even if
the current calldata selector matches.

## Single dynamic-fee correctness contract

The observed fee `50` is identity evidence, not a permanent configuration
constant. The runtime locally mirrors the Lynex Algebra V1.9 timepoints,
adaptive-fee configuration, per-block volume state, and the exact integer
arithmetic required to reproduce the deployed `getFee` result. No RPC, Quoter,
`f64`, expected-fee shortcut, fixed-fee fallback, or wall-clock approximation
is permitted in sizing, admission, preflight, or transaction construction.

For canonical pool head timestamp `T`, the prepared-curve builder computes one
conservative fee envelope for every integer timestamp from `T` through the
immutable transaction-validity horizon. The prepared generation records:

- canonical pool generation and fee-state generation;
- current fee and envelope fee;
- first and last valid timestamps;
- timepoint/configuration/volume-state fingerprint;
- source block number and hash.

The transaction deadline must remain inside that horizon. Opportunity,
adaptive sizing, admission, and preflight all use the same envelope curve, so
the 20 bps decision remains one raw venue-economics model. The envelope is not
a profit floor, gas model, recovery model, or second gate.

On every canonical Lynex `Fee`, `Swap`, `Mint`, or `Burn`, and whenever a head
changes the envelope, only affected curves are rebuilt. `TickSpacing`,
`Incentive`, unsupported data-storage/configuration changes, a missing required
Fee event, or a parity fault fail the route closed and trigger pinned
rehydration. A non-zero incentive is unsupported in the first release.

Preflight non-blockingly drains queued canonical events, uses the latest
published head, refreshes the fee envelope if needed, and requotes the exact
immutable input. It rejects an expired horizon, incomplete fee state, stale
generation, changed fingerprint, or unhealthy parity. The calldata minimum
output remains the final on-chain protection against a swap landing between
preflight and inclusion.

## Pair, Binance, and economic model

The public Binance check on 2026-08-06 observed `USDCUSDT` in `TRADING` state,
Spot enabled, with base `USDC`, quote `USDT`, price tick `0.00001000`, quantity
step `1.00000000`, and minimum notional `5.00000000`. These are observed
exchange rules, not forever constants. Authenticated startup validation must
fetch current filters and reject an incompatible artifact before new entries.
The public source is
<https://api.binance.com/api/v3/exchangeInfo?symbol=USDCUSDT>.

The 6 USDT detector is intentionally one USDT above the observed minimum. With
a one-USDC Binance step, every candidate and hedge target is rounded down to a
whole-USDC quantity before exact DEX evaluation. The 200 USDT cap therefore
means a maximum candidate selected by exact curve price and the current step,
not an unconditional 200 USDC order.

The two directions are:

- spend USDT for USDC on Lynex, then SELL the exact USDC hedge target on
  Binance;
- spend USDC for USDT on Lynex, then BUY the exact USDC hedge target on
  Binance.

After DEX success, IOC price selection retains the production rule. A fresh
same-side Binance top may improve the immutable admission boundary only when
its visible quantity covers the complete hedge target: a covered SELL uses the
higher price, a covered BUY the lower price. An adverse, unavailable, or
insufficient top keeps the admission price. Top quantity never enters sizing,
readiness, admission, or entry preflight.

The stablecoin pair adds no depeg oracle, parity clamp, expected-profit floor,
inventory-ratio gate, or synthetic one-dollar price. A genuine USDC/USDT basis
is venue economics. Exact token identity and Binance network mapping are
mandatory, but neither token's assumed redemption value is an execution input.

Binance commission remains paid from the configured BNB balance and accounted
as an exact BNB delta valued with the existing BNBUSDT bid. A missing BNB price
makes valuation telemetry incomplete but cannot change a known USDC fill into
unknown exposure or block bounded recovery.

## Domain and compiled model

Create a new immutable source artifact, initially named
`config/strategies/usdt-usdc-linea.v1.json`, and add it to the compiled
multi-pair production bundle. A representative source shape is:

```json
{
  "schema_version": 1,
  "snapshot_id": "arb-bot-production-usdt-usdc-linea-v1-lynex-algebra-v1-9-live",
  "live_trading_enabled": true,
  "pairs": [
    {
      "id": "linea-usdt-usdc",
      "market_data_enabled": true,
      "execution_enabled": true,
      "full_live": true,
      "chain": {
        "chain_id": 59144,
        "binance_network_name": "LINEA",
        "gas_symbol": "ETH",
        "gas_decimals": 18,
        "lynex_algebra_v1_9_factory_address": "0x622b...8D0F",
        "lynex_algebra_v1_9_pool_deployer_address": "0x9A89...2C48",
        "lynex_algebra_v1_9_quoter_address": "0x851d...C83e",
        "lynex_algebra_v1_9_router_address": "0x3921...3390"
      },
      "token_a": {
        "symbol": "USDT",
        "contract": "0xA219...2B93",
        "decimals": 6
      },
      "token_b": {
        "symbol": "USDC",
        "contract": "0x1762...E1ff",
        "decimals": 6
      },
      "binance": {
        "symbol": "USDCUSDT",
        "base_asset": "USDC",
        "quote_asset": "USDT",
        "market_data_product": "spot",
        "execution_product": "spot",
        "step_size": "1.00000000",
        "tick_size": "0.00001000"
      },
      "quote_sizing": {
        "token_a_base_units": "6000000"
      },
      "adaptive_sizing": {
        "enabled": true,
        "max_token_a_base_units": "200000000"
      },
      "dex": {
        "allowed_providers": ["lynex_algebra_v1_9"],
        "lynex_algebra_v1_9": {
          "pools": [
            {
              "expected_address": "0x6e9a...6a9b",
              "selection_enabled": true,
              "required_active_incentive": "0x0000000000000000000000000000000000000000",
              "expected_tick_spacing": 1,
              "dynamic_fee_horizon_seconds": 2
            }
          ]
        }
      }
    }
  ]
}
```

Field names and schema version may change during P1, but the semantic identity
must not. The source records the explicit operator decision for immediate
full-live deployment without paper/canary, exact pre-release evidence, and the
production approval timestamp. Historical artifacts remain immutable.

The compiler emits typed network, wallet, lane, assets, instrument, strategy,
provider, pool, and journal IDs. Required identities include:

- network `eip155:59144`;
- wallet location `eip155:59144:evm-wallet:primary`;
- one Linea EVM execution lane and durable journal distinct from World Chain
  and Arbitrum;
- venue assets keyed by exact Linea token contracts;
- Binance instrument `USDCUSDT` on the existing isolated Rust Spot account;
- pool ID containing the Lynex provider and exact pool address.

The current `FullLivePolicy`, readiness, executor, and startup composition are
hard-restricted to reviewed Arbitrum pairs and chain IDs 480/42161. P1-P6 must
replace chain- and pair-name conditionals with typed reviewed capability
profiles while preserving all existing WLD/ESP/ARB behavior byte-for-byte.
The Arbitrum-named fee-headroom field must become a network gas policy rather
than being copied to Linea under a misleading name.

## Runtime ownership and process topology

Production remains one application Pod on the fixed GKE C4 node. Adding Linea
must not create a second Pod, GCE owner, process, Binance owner, or database
dependency.

The process adds exactly one process-scoped Linea runtime containing:

- reused HTTP RPC and WSS clients;
- one canonical Linea mirror owner;
- one Linea read coordinator and bounded background lanes;
- one dedicated Rust Linea wallet, signer, nonce lane, and journal not used by
  Rails or the stopped GCE rollback VM;
- one Linea DEX execution owner and bounded command channel;
- one-second `eth_gasPrice` refresh into a two-second cache;
- background wallet, telemetry, journal, and receipt-accounting paths.

The existing process-scoped Binance WebSocket/API/account owner adds
`USDCUSDT`; no per-symbol HTTP, WS, or signing client is created. The domain
artifact remains the only Binance symbol allowlist.

The strategy has a separate EVM lane because Linea has independent nonces and
canonical state. Binance commands still pass through the shared serialized
account owner, deterministic client IDs, journal, capital allocator, and
unknown-outcome reconciliation. Cross-chain opportunities may evaluate in
parallel, but each EVM lane and the shared Binance mutation owner remain
single-owner.

## Startup, ingestion, and recovery

Linea startup follows the existing race-free sequence:

1. Validate the immutable source and compiled artifact before connections.
2. Select canonical Linea block `B` and verify chain ID, block hash, provider
   capabilities, Multicall3 identity, all token/contract code, factory,
   deployer, router, Quoter, pool, tick spacing, zero incentive, and limits at
   exactly `B`.
3. Hydrate `globalState`, liquidity, tick table, initialized ticks, timepoints,
   adaptive-fee configuration, and packed per-block volume state as one
   unpublished generation.
4. Subscribe on the process-scoped Linea WSS connection to the selected address
   and exact Lynex topics: Swap, Mint, Burn, `Fee(uint16)`, TickSpacing,
   Incentive, and every configuration event proved relevant by the linked
   source revision.
5. Capture head `C`, backfill `(B, C]`, apply logs in canonical position order,
   discard buffered duplicates, build the fee envelope and prepared curves,
   and only then make the route ready.

The storage-layout proof must independently establish the slot used to hydrate
`volumePerLiquidityInBlock`; it may not inherit Camelot's slot number. The
low-half liquidity cross-check and all Multicall inner calls remain mandatory.
A partial batch is discarded.

Gap, removed log, parent mismatch, duplicate with different payload, invalid
liquidity delta, required Fee omission, unsupported configuration change,
non-zero incentive, fee error, or Quoter parity fault fails only the Lynex
route closed and begins pinned rehydration. It must not corrupt or globally
pause coherent World Chain or Arbitrum strategies.

## Quoting, execution, and settlement

The hot quote remains a borrowed, allocation-free prepared-curve lookup plus
at most one exact swap step. No RPC, Quoter call, database, lock, JSON,
serialization, pool clone, wall-clock fee calculation, or new task handoff is
allowed between accepted Binance top parsing and baseline evaluation.

Execution uses only the direct Lynex `SwapRouter.exactInputSingle` tuple:

```text
exactInputSingle((address,address,address,uint256,uint256,uint256,uint160))
```

The reviewed selector is `0xbc651188`. The tuple is token in, token out,
recipient, deadline, exact amount in, minimum amount out, and square-root-price
limit. There is no fee word. Aggregators, V2, multi-hop, exact-output, fee-on-
transfer, permit, sweep, unwrap, and arbitrary multicall execution are out of
scope.

The immutable plan binds provider, pool, pool generation, single-fee
generation, envelope, horizon, router profile, exact input, minimum output,
recipient, and deadline. The router cannot select another pool or protocol.

A successful receipt must prove exactly one positional selected-pool Swap,
exact wallet transfer deltas, and any positional `Fee(uint16)` before Swap.
After success, the execution owner non-blockingly drains queued Linea events,
applies the receipt Fee if present and the receipt Swap directly to the local
mirror, rebuilds affected curves, and then releases the Linea lane. It never
waits for a second `eth_getLogs` copy and creates no pool or global settlement
barrier.

Known reverts emit receipt telemetry immediately and may enqueue bounded
background trace/historical-call diagnostics. Trace availability, latency, and
decoded errors remain diagnostic. Unknown transaction outcomes retain the
existing nonce/journal reconciliation and no-resubmission rule.

## Gas, receipt fees, and allowances

Add a typed `CompiledNetworkGasPolicy::LineaMainnet` only after pinned evidence
proves transaction type, accepted fee fields, gas-price sampling, estimation,
replacement behavior, and receipt accounting on the production provider.
Neither World Chain's 100,000-wei fallback nor Arbitrum's headroom policy is
inherited by name.

The Linea execution owner refreshes `eth_gasPrice` once per second and keeps a
sample valid for two seconds. Transaction construction reads only that cache.
The reviewed Linea policy must define fail-closed behavior or a measured
provider-specific fallback for a zero/failed refresh before broadcast is
enabled.

Pinned receipts must determine whether all Linea execution/data cost is already
included in `gasUsed * effectiveGasPrice` or whether an additional receipt
field must be added. Accounting implements exactly that proof and prevents
double counting. Gas remains telemetry/accounting and never enters sizing or
admission.

Native ETH funding is an operator-maintained invariant, not a readiness,
balance-sync, admission, reservation, or sizing input. Receipt and send failures
still fail the affected operation closed through normal execution recovery.

No Lynex allowance is created during implementation or read-only verification.
The direct-live production revision prepares max allowance for exactly Linea
USDT and USDC to the reviewed Lynex router under provider- and chain-specific
durable idempotency keys, verifies both, and permanently locks allowance
mutation before Lynex submissions are enabled. No other router or token is
approved.

## Full-live rebalancing and capital

Rebalancing is `full_live` from the first production revision, matching the
requested ARB/USDC operating model. Binance Linea deposit and withdrawal are
not assumed to exist. The singular reviewed route is Binance Optimism plus
Across V4 between chain `10` and Linea chain `59144`, in both directions.

Before release, authenticated Binance capital metadata must prove for both
USDC and USDT:

- network name is exactly `OPTIMISM` and the route is available for chain 10;
- deposit and withdrawal are enabled for the production subaccount;
- precision, minimum withdrawal, fee, confirmations, and address format are
  represented exactly;
- deposit and withdrawal reconciliation use deterministic durable IDs;
- an Unknown outcome never authorizes a second withdrawal or transfer.

The approved Across token map is exact and bidirectional:

| Asset | Optimism (`10`) | Linea (`59144`) |
| --- | --- | --- |
| USDC | `0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85` | `0x176211869cA2b568f2A7D4EE941E073a821EE1ff` |
| USDT | `0x94b008aA00579c1307B0EF2c499aD98a8ce58e58` | `0xA219439258ca9da29E9Cc4cE5596924745e12B93` |

Every quote uses the reviewed Poly Bot Across integrator ID `0x5042`. Before
rollout, a read-only production preflight requests and strictly validates all
four asset/direction combinations. Validation pins exact input, depositor,
recipient, origin/destination chains, token addresses, allowance spender,
approval, `depositV3` calldata, minimum output, deadline, zero native value,
and bounded response size/fill time. A missing or changed route blocks rollout.
The runtime must not silently substitute Ethereum, Arbitrum, World Chain,
another stablecoin representation, CCTP, or any other bridge route.

Capital remains in the existing inventory ledger. Reservations cover only the
exact primary DEX input token; there is no legacy `3x` multiplier, native-gas
reservation, hypothetical recovery reservation, or stablecoin-parity netting.
USDC on Linea, USDC on other chains, Binance USDC, Linea USDT, and Binance USDT
remain distinct venue assets joined only by explicit economic-asset mappings.

Production limits for Binance transfers, Across bridge transactions, fees,
cumulative authority, and one-operation-at-a-time ownership must be explicitly
approved in the v1 full-live policy. They are not inferred from ARB/USDC
amounts.

## Performance preservation contract

Adding Linea and Lynex must preserve existing hot-path and target-node limits.
Every milestone runs `scripts/quality.sh`, paired provider benchmarks, frozen
Uniswap/Camelot controls, and the maximum-pair capacity replay.

Performance verification is a mandatory exit gate after **every** P0-P8
milestone, not a final aggregate check. Each milestone checks in a
machine-readable report containing the exact revision, artifact fingerprint,
build profile, host/CPU, image digest when applicable, sample count, warmup,
paired-run order, p50/p95/p99/max, throughput, allocations, queue drops, CPU,
memory, and throttling. The report compares the completed milestone with both:

- the immediately preceding accepted milestone, to attribute the newest diff;
- the frozen P0 baseline, to detect cumulative degradation hidden by several
  individually small changes.

The next milestone must not start until its report passes. A noisy or ambiguous
result is rerun with at least five interleaved samples on an otherwise idle
host; it is not accepted by widening a threshold. A repeatable slowdown is
profiled and explained even when it remains inside the numerical allowance.
An unexplained repeatable regression is fixed or the milestone is reverted.

"Not significantly slower" means all of the following:

- existing production prepared-quote, Binance-frame-to-decision, event/apply,
  plan/calldata, receipt/apply, and curve-build p95/p99 are no more than
  `1.05x` both the preceding milestone and P0 baseline;
- maximum-pair replay throughput is at least `0.95x` both references and its
  decision p99 remains below `25 us`;
- on the fixed target C4, end-to-end p95 is no more than `1.15x` and p99 no
  more than `1.20x` the frozen production reference while every independent
  hard ceiling still passes;
- allocations, network calls, lock acquisition, queue drops, CPU throttling,
  OOM/restart behavior, and existing-provider work do not increase in a
  repeatable way without a reviewed cause and bound.

These ratios are noise/non-inferiority allowances, not performance budgets. A
5% slowdown at several consecutive milestones cannot compound silently because
every stage is also compared directly with P0.

Required gates are:

- zero network calls, locks, serialization, or allocations in steady-state
  prepared quote and Binance-frame-to-baseline paths;
- byte-exact local/Quoter amount and fee parity for both directions;
- Lynex prepared quote p95/p99 at most `1.05x` the matched Algebra/Uniswap
  control;
- matched event, plan, calldata, and receipt p95/p99 at most `1.10x` control;
- normalized real-pool curve-build p99 per segment at most `1.20x` matched
  control;
- fee-envelope calculation p99 below `25 us`, unchanged envelopes causing no
  rebuild, and fee projection plus publication p99 below `200 us`;
- one candidate prepared quote p99 below `3 us`;
- combined maximum-pair decision replay p99 below `25 us` and no more than
  `1.05x` the frozen pre-Lynex reference;
- zero hot telemetry, canonical event, execution command, and unknown queue
  drops;
- no repeatable regression in existing provider controls, CPU throttling,
  memory, or restart recovery.

Local measurements use paired interleaved release runs. Target measurements use
the exact immutable image on the existing fixed C4 through CI; local Docker is
not used.

## Implementation milestones

### P0 — Freeze controls and evidence schema

- Freeze current compiled production artifact, quality result, provider
  benchmarks, capacity replay, and target-C4 reference.
- Add matched single-fee Algebra fixtures and report fields without production
  runtime changes.

Exit: controls are repeatable and the harness itself stays within all gates.

### P1 — Network, provider, schema, and compiler types

- Add Linea typed network/gas/read profiles and `LynexAlgebraV1_9` identities.
- Add the v1 source as non-production test input; do not add it to the running
  bundle yet.
- Generalize hardcoded Arbitrum full-live/readiness policy into reviewed typed
  capabilities without widening existing authority.

Exit: deterministic compiler tests pass; existing artifacts compile unchanged;
no new production secret or mutation path exists.

### P2 — Single-fee Algebra arithmetic and curves

- Reuse only parity-proved Algebra tick/price primitives.
- Add seven-word global state, one fee, `Fee(uint16)`, exact boundary rounding,
  and single-fee envelopes.
- Test adjacent base units, ticks, crossings, insufficient liquidity, and both
  exact-input/output directions.

Exit: byte-exact golden vectors and all quote/build performance gates pass.

### P3 — Pinned Lynex discovery, hydration, and Quoter parity

- Prove all contracts, source/runtime hashes, pool/tokens, proxy identities,
  tick spacing, zero incentive, data-storage layout, configuration, timepoints,
  and volume state at one canonical block hash.
- Compare local exact-input/output amount and returned fee with the reviewed
  Quoter at 6, 50, and 200 USDT scale, every prepared boundary, adjacent base
  units, multiple timestamps, and a real fee transition.

Exit: complete same-block hydration and byte-exact amount/fee parity; partial
state never publishes.

### P4 — Linea canonical ingestion and mirror

- Add Linea subscription/backfill/reorg recovery and Lynex provider-routed
  events.
- Exercise gaps, duplicates, reorder, removed logs, parent mismatch,
  Fee-before-Swap, reconnect, unchanged head, and unsupported changes.

Exit: deterministic pinned event replay reaches the exact post-state with zero
drops and all publication/performance gates pass.

### P5 — Opportunity, sizing, Binance, and preflight integration

- Add `USDCUSDT` to the compiled subscription/account plan.
- Integrate the pool with deterministic provider selection, 6-USDT detection,
  one-USDC step-aligned adaptive sizing, exact reservations, and latest-
  generation preflight.
- Extend the maximum-pair replay with Linea events, heads, and USDCUSDT frames.

Exit: both trade directions and all economic boundaries are deterministic;
combined replay and existing strategies remain within gates.

### P6 — Disabled executor, calldata, gas, allowance, and journals

- Add a separate Linea signer/nonce/journal owner and typed gas policy.
- Materialize the exact direct router call and provider-specific allowance set.
- Keep signing, broadcast, and allowance mutation disabled.
- Prove exact calldata with pinned `eth_call` and `eth_estimateGas` in both
  directions using read-only state overrides or funded historical senders.

Exit: no mutation is possible; journal replay cannot cross network/provider;
calldata, estimation, and fee-field evidence pass.

### P7 — Receipt proof, accounting, and recovery composition

- Replay representative successful Lynex transactions and receipts in both
  directions.
- Prove positional Fee/Swap, wallet deltas, gas/data-fee accounting, direct
  mirror settlement, known revert, timeout, Unknown outcome, restart, DEX-
  success/Binance-partial recovery, and one-query Binance Unknown placement.
- Validate authenticated Binance Optimism metadata and strict Across quotes in
  both directions without making a transfer.

Exit: receipt-to-lane-release and recovery tests pass with no second log wait,
no duplicate order/transaction authority, and no external mutation.

### P8 — Full-live production rollout

- Record the operator approval, all P0-P7 evidence, exact capital and allowance
  limits, funded isolated Linea wallet, funded Binance balances/BNB, previous
  image digest, reconciliation report, and rollback revision.
- Add the new v1 source to the compiled production bundle with observe, plan,
  execute, and rebalance all true.
- Add exact deployment assertions for chain, strategy, tokens, symbol, provider,
  pool, contracts, 6-USDT detector, 20 bps gate, 200-USDT cap, full-live policy,
  journals, required Linea environment, Binance Optimism routes, and all four
  exact Across Linea quote routes.
- Build and test through `.github/workflows/deploy-gke.yml` on `main`, obtain
  production approval, and roll the exact digest onto the existing node.
- During startup, reconcile journals and balances, prepare and lock exactly two
  Lynex allowances, then enable Lynex submissions.
- Allow the first eligible opportunity to execute the full adaptive size. This
  is normal full-live operation, not a canary cohort.
- After rollout, produce the P8 performance report from one fixed half-open
  production window and compare it with the final pre-rollout image cohort and
  P0. Include Binance parse/socket-to-decision, adaptive calculation, sizing
  queue/worker/handoff, Linea event/apply, DEX receive/build/settlement, CPU,
  memory, throttling, restarts, and all queue drops. This is release acceptance
  monitoring, not a canary or a reduced-authority phase.

Exit: the workflow verifies startup fields and rollout health; GCE remains
`TERMINATED`; there is exactly one Pod and no unreconciled operation. The P8
performance report must also pass the per-stage non-regression gate; otherwise
new Linea entries close and the reviewed rollback path is used.

### P8 rollout recovery evidence

The first P8 rollout built and preflighted the intended image but failed closed
before trading. Linea Alchemy URLs had been derived from the World Chain key;
that key returned HTTP 403 for Linea. PublicNode was then selected, but a later
provider outage timed out both automatic rollout attempts before the capacity
replay. The corrected runtime uses the official `rpc.linea.build` HTTP endpoint
and the public dRPC Linea WSS endpoint. The deployment runs a read-only transport
gate on the fixed Singapore C4 before rollout: ten HTTP chain/gas samples must
have p95 at or below 500 ms, WSS log/head subscription must complete within
three seconds, and a canonical head must arrive within eight seconds.

The rollback also exposed a pre-existing Optimism rebalance journal collision.
Local operation `rebalance-1516-6b2792a1b1a18931:deposit` had signed hash
`0x34462b8a2f930da06b5196db6a4111b07941c25ecbe4e0ddc388716a4d41a482`
at nonce 76 after that nonce had already been consumed by successful canonical
transaction
`0x2d22c304a0e0ca98e0684145dbff8a62925cb36c33b0af891dc56b8248fb73b4`.
Startup may close only this exact local rejection after two observations prove
the rejected hash and receipt absent, the replacement transaction and success
receipt unchanged, the nonce consumed, and every journal identity/scope field
unchanged. That reviewed migration remains the one-time evidence for the
historical journal entry. Later nonce-too-low incidents use the generic bounded
recovery: canonical absence closes the rejected child, the shared allocator is
advanced, and the parent may create a fresh deterministic deposit child only
after the bridged token balance and pinned Binance route are revalidated.
Read-only Binance preflight failures preserve that recovery lineage so a later
periodic pass can repeat the proof without authorizing an extra transaction
attempt.

The P8d local release A/B retained 42 ns decision p95 and improved median
capacity throughput from 14,523,532 to 14,628,040 frames/s (`1.0072x`). Target
C4 capacity and transport evidence remain mandatory before the corrected
rollout can proceed.

## Rollback and stop conditions

Rollback is a new reviewed `main` revision through `Deploy GKE`, not a local
`kubectl` change, workstation image, GCE start, or force push. The rollback
artifact removes Linea selection/execution/rebalancing while preserving all
other production strategies and durable journals.

Any of the following closes new Linea entries immediately and requires complete
reconciliation before rollout of the rollback revision:

- token, pool, factory, deployer, router, Quoter, runtime-code, or Binance
  network identity mismatch;
- fee/amount parity fault or unsupported Algebra configuration/incentive;
- canonical event gap/reorg that cannot be repaired coherently;
- unknown DEX exposure, unknown Binance exposure after the one allowed status
  query, or journal/nonce ownership mismatch;
- relevant queue drop, failed allowance lock, hard latency ceiling, repeatable
  performance regression, or runtime resource failure;
- inability to prove exact Binance Optimism and Across Linea routes for both
  assets and both directions.

Already granted router allowances remain inert and locked; rollback does not
introduce a new allowance mutation path. Active operations are reconciled by
their immutable journal identities before the old artifact can own new work.

## Explicit non-goals

- No paper mode, production shadow route, reduced sizing, or canary.
- No Lynex V2, Algebra Integral/V2, aggregator, multi-hop, or split execution.
- No additional DEX pools in the first release.
- No stablecoin depeg oracle or one-dollar clamp.
- No bridge route other than the exact reviewed Optimism/Across V4/Linea path.
- No Postgres, ClickHouse, Rails, or remote Quoter in the critical path.
- No second application Pod, active GCE owner, local production build, or
  workstation deployment.

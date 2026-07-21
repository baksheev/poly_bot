# Adaptive arbitrage sizing

Status: v11 Rails-age-parity tiered-depth adaptive execution implemented for GKE production
Last reviewed: 2026-07-21

## Implementation status

The v11 production artifact enables `mode = adaptive` with a 200 USDC maximum
trade-notional cap and tiered Binance depth. Rust evaluates exact Binance-step
quantities for every enabled pool against prepared DEX curves using, in order:
sequence-matched full depth; recent full depth within 750 ms and update delta 8;
or a synthetic top-of-book-only book capped at 40 USDC. A recent book has its
top replaced by the current bookTicker and discards stale levels better than
that top before it is used. Every selected size must still fit the relevant
current top and all gas, recovery-loss, exposure, freshness, and exact
reservation checks.

The configured 20 USDC amount remains the detector/control and DEX-first fast
path. Full-depth freshness is reported separately and does not move a
`full_live`/DEX-first runtime from `Ready` to `Degraded`.

The first shadow optimizer is the exhaustive whole-step reference oracle. It
is capped at 8,192 exact evaluations per pool; exhausting the cap selects no
partial result and falls back to baseline. The breakpoint optimizer remains a
later latency optimization and must match this oracle on fixtures and replay.

The Stage 1 production gate is adaptive calculation p99 at or below 50 ms and
total decision p99 no worse than the larger of 2x baseline p99 or baseline p99
plus 50 ms. The local optimized microbenchmark measured a 5,501-step full-depth
admission batch at about 3.7 ms (676 ns/candidate), while prepared CLMM quotes
measured 237--360 ns/candidate. Production telemetry, not the workstation
microbenchmark, decides whether the exhaustive oracle remains enabled.

Trade reservations use `exact_execution_envelope_v1`. The envelope reserves
the immutable DEX input, the maximum reachable Binance debit, and maximum
native gas under the same single-owner ledger. The legacy Rails `3x` field is
readable only for old-artifact compatibility and never multiplies Rust claims.

## Decision

Replace baseline-only execution with dynamic, Binance-step-aligned notional
sizing derived from the current prepared pool curves and in-memory Binance
market state. The configured 20 USDC trade remains the opportunity detector,
Rails comparison anchor, and fallback. When that baseline clears the existing
opportunity gate, Rust searches every feasible size up to a configured risk cap
and executes the size with the highest conservative absolute profit, provided
that it improves on the baseline and passes exact depth, gas, inventory,
recovery, freshness, and exposure checks.

This is a single-wallet throughput change:

```text
same nonce lane * more expected profit per safe transaction
```

It is not a concurrency change. Multi-wallet execution and concurrent DEX/CEX
dispatch remain separate designs and rollouts.

## Terminology

- **Baseline**: the direction-specific executable trade derived from the
  configured `quote_sizing.token_a_base_units`, currently 20 USDC.
- **Sizing domain**: the step-aligned token-B interval between the executable
  baseline and the smallest market, inventory, and risk upper bound.
- **Candidate**: an exact `TradeEvaluation` produced for one
  `(direction, pool, token_b_steps)` point. Its actual cost/proceeds and full
  admission economics are authoritative.
- **Selected candidate**: the candidate that passes all hard gates and wins the
  deterministic ranking.
- **Sizing cap**: a configured maximum risk/notional bound. The 40, 100, and
  200 USDC rollout values are caps and telemetry buckets, not permitted-size
  slots.
- **Market-liquidity capacity**: the existing quote-engine estimate bounded by
  one Binance top-of-book level, DEX liquidity, and the gross opportunity
  threshold. It remains telemetry and an optional search bound; it is not an
  executable size.
- **Expected spread profit**: selected candidate proceeds minus cost after the
  venue fees and DEX execution reserve already embedded in those values. This
  is the adaptive-sizing objective after the configured 20 bps gate passes.
- **Bounded profit**: a worst-case diagnostic after maximum recovery loss and
  maximum DEX gas. It is not the normal-entry profitability gate.

## Current behavior and verified constraints

The current path in `src/engine.rs`:

1. evaluates both directions at the Rails-compatible 20 USDC baseline;
2. calculates `market_liquidity_capacity` for telemetry;
3. uses the baseline crossing to open the adaptive search;
4. evaluates every feasible whole Binance step against both sides of
   sequence-consistent full depth and every prepared DEX pool;
5. calculates maximum recovery loss and maximum DEX gas cost;
6. atomically reserves the exact wallet, Binance, and native-gas execution
   envelope with no multiplicative safety factor;
7. publishes the immutable plan into a latest-pending execution mailbox.

Important implementation facts:

- `market_liquidity_capacity` uses only the observed Binance top-of-book
  quantity during opportunity sizing. Full depth, reservations, native gas,
  recovery loss, and global risk caps are applied later.
- `TradeEvaluation.cost_token_a` and `proceeds_token_a` already include the
  conservative Binance commission and DEX slippage/fee reserve. Admission must
  not subtract those charges a second time.
- `TradeEvaluation.meets_threshold` compares the exact gross venue proceeds
  and cost at the configured 20 bps before the Rails-compatible commission and
  execution reserve. Admission persists that exact verdict instead of
  recomputing it from a different cost basis. Maximum gas and recovery loss do
  not alter the verdict; they remain hard coverage/cap inputs.
- `balance_safety_multiplier = 3` is a conservative Rails compatibility rule,
  not a Rust runtime invariant. Rails needed headroom around slow balance
  refresh and overlapping jobs. Rust has a single inventory owner, continuous
  venue updates, explicit in-flight plans, and deterministic recovery bounds;
  Rust therefore ignores it for all trade claims. Old artifacts remain
  readable, while v8 and v9 omit the field entirely.
- The execution input is a latest-pending mailbox with lane states `available`,
  `busy`, and `blocked_unknown`. It holds at most one pending opportunity.
  A newer admission currently supersedes the pending opportunity
  unconditionally; the engine releases the superseded reservation and emits
  `arbitrage_execution_pending_discarded`.
- Live preflight rejects a queued plan if the Binance price moved against its
  admitted limit or the DEX pool generation changed.
- One wallet owns one World Chain nonce lane shared with other wallet writes.
  A larger trade must not weaken nonce or DEX settlement barriers.

The 2026-07-20 draft observation (`06:31-06:46 UTC`) reported 2,189
threshold-crossing direction evaluations, 1,370 admissions, and 27 completed
trades. Before using those counts as rollout evidence, preserve the ClickHouse
query and split the missing conversions by at least pending supersession,
execution-lane unavailability, preflight rejection, inventory rejection,
settlement barrier, active execution, balanced result, and unknown result. The
adaptive-sizing decision does not rely on a single explanation for all missing
conversions.

## Goals

- Increase expected bounded and realized token-A profit per wallet nonce.
- Preserve baseline behavior as a control and a safe fallback.
- Evaluate every selected size from exact local DEX curves and exact Binance
  step/depth rules; never linearly scale baseline basis points.
- Bound selected notional, worst unhedged notional, recovery loss, gas loss,
  inventory reservation, and pending-mailbox ownership.
- Replace heuristic trade reservation multipliers with exact worst-case debit
  envelopes derived from the immutable execution state machine.
- Keep the decision path local, deterministic, fixed-point, and free of RPC,
  database, ClickHouse, signing, or other network calls.
- Make shadow comparison, rollout, and rollback explicit in versioned config.

## Non-goals

- Multi-wallet scheduling or additional nonce lanes.
- Switching the production default from `dex_first` to `concurrent_hedged`.
- Removing or relaxing DEX settlement, quote freshness, balance freshness,
  readiness, signer, live-entry, or recovery gates.
- Changing latest-pending replacement from newest-wins to an economic policy in
  the same change.
- Learning probability estimates or optimizing expected value from live data
  in the first version.
- Reading Rails Postgres, Redis, ClickHouse, or an RPC endpoint during sizing.

## Safety invariants

Adaptive mode must preserve all of the following:

1. A larger size is considered only when the normal 20 USDC baseline for that
   direction clears the existing gross opportunity threshold.
2. Every candidate is a concrete, step-aligned `TradeEvaluation` against one
   immutable prepared pool generation and one Binance book generation.
3. The final plan quantity, DEX calldata, primary CEX limit, both recovery
   bounds, gas cap, and inventory reservation are derived from the same
   selected candidate. The executor never resizes a plan.
4. Only the final selected candidate is reserved. Candidate evaluation and
   shadow mode create no speculative reservations.
5. A selected candidate cannot exceed any configured notional, recovery-loss,
   available-inventory, order-filter, or native-gas cap.
6. Unknown submitted outcomes retain their nonce, inventory, and recovery
   ownership exactly as baseline plans do.
7. Reconfiguration or rollback never cancels, resizes, or releases an already
   admitted plan. It affects new entries only.
8. No adaptive calculation uses `f64`. Base-unit integers and validated
   decimals retain the existing conservative rounding directions.
9. Every command reachable from an admitted plan fits inside its remaining
   venue/asset reservation. A balance refresh never substitutes for ownership
   of an in-flight or unknown outcome.

## Configuration contract

Add an optional pair-level `adaptive_sizing` object. Its absence means
`baseline_only`, so v1-v6 artifacts retain their existing behavior. V7 records
the shadow predecessor, v8 records exact-envelope activation, and reviewed v9
uses active adaptive mode with the spread threshold separated from tail risk.

```json
"adaptive_sizing": {
  "mode": "adaptive",
  "max_trade_notional_token_a_base_units": "200000000",
  "max_unhedged_notional_token_a_base_units": "220000000",
  "max_recovery_loss_token_a_base_units": "2000000",
  "min_expected_profit_token_a_base_units": "0",
  "min_incremental_expected_profit_token_a_base_units": "0",
  "depth_policy": {
    "recent_full_depth_max_age_ms": 750,
    "recent_full_depth_max_update_delta": 8,
    "top_of_book_max_trade_notional_token_a_base_units": "40000000"
  }
}
```

Modes:

- `baseline_only`: do not search or execute a larger size. This variant has
  no sizing fields.
- `shadow`: evaluate and emit the adaptive selection, but execute the baseline
  under the current baseline admission contract.
- `adaptive`: execute the selected adaptive candidate, with baseline fallback.

Represent the object as a mode-tagged enum with `deny_unknown_fields` on every
variant. The `shadow` and `adaptive` variants share the sizing fields shown
above. This makes the rollback form `{ "mode": "baseline_only" }` complete and
valid rather than an abbreviated partial config.

The numeric values above illustrate the schema, not approved live limits.
`max_recovery_loss` and the two expected-profit values must be calibrated from
shadow data before `adaptive` is committed.

V7-v8 artifacts using the former `min_bounded_profit_*` names remain readable
as aliases. New artifacts must use the expected-profit names so configuration
does not imply that worst-case recovery economics control normal entry.

For `shadow` and `adaptive`, validation must reject the artifact unless:

- `max_trade_notional` is at least `quote_sizing.token_a_base_units`;
- all caps and floors are non-negative base-unit integers within `U256` and the
  narrower execution/journal representations;
- `max_unhedged_notional >= max_trade_notional` unless a reviewed stricter
  relationship is intentionally supported;
- `adaptive` is not combined with disabled pair/global execution gates in a
  live artifact;
- unknown fields and unknown modes fail closed.

The mode is loaded once at startup. Changing the artifact requires the normal
reviewed deployment and restart; it is not a dynamic control-plane toggle.

## Dynamic sizing domain

Search in whole Binance token-B steps, not in fixed token-A notionals. For each
pool and direction, let `k` be the integer number of token-B steps and construct
the exact candidate at `q = k * token_b_step`.

The lower bound is that pool's direction-specific baseline quantity. The upper
bound is the smallest of:

```text
prepared DEX curve liquidity
relevant bookTicker quantity at the admitted primary IOC price
configured maximum trade notional
configured maximum unhedged notional
full-depth recovery availability
exact execution-envelope inventory availability
venue order/filter limits
```

Some bounds are naturally token-B quantities. Convert token-A risk and
inventory caps conservatively through exact candidate evaluation; never divide
by a floating-point or unbounded indicative price.

`bookTicker` is necessary but not sufficient. It defines the current primary
price and immediately executable quantity for the aggressive IOC. The
sequence-consistent depth book is still required to price both recovery
directions and reject a size whose full recovery quantity is unavailable.
Prepared pool curves remain the only DEX pricing source. No network request is
made during the search.

### Exact candidate evaluation

For DEX-buy/CEX-sell at quantity `q`:

1. quote exact-output `q` token B on the prepared pool curve;
2. apply the configured DEX slippage/fee reserve;
3. calculate Binance sell proceeds at the admitted primary price and commission;
4. calculate full admission, recovery, gas, caps, and reservation economics.

For CEX-buy/DEX-sell at quantity `q`:

1. calculate the Binance buy cost at the admitted primary price and commission;
2. quote exact-input `q` token B on the prepared pool curve;
3. apply the configured DEX slippage/fee reserve;
4. calculate full admission, recovery, gas, caps, and reservation economics.

Every evaluated point is therefore executable as-is after preflight. Dynamic
sizing never scales the 20 USDC baseline or interpolates its profit bps.

### Optimizer contract

For each pool and direction, select:

```text
argmax over feasible whole-step quantities q of
  expected_profit_token_a(q)
```

Then compare the per-pool winners across both directions. The proposed 200 USDC
value is only the shadow maximum-notional cap.

The hot-path optimizer must be bounded without introducing fixed notional
slots. Use the prepared DEX segment boundaries, Binance recovery-depth
cumulative-quantity boundaries, risk/inventory cap boundaries, and their
adjacent token-B steps. Inside an interval where those inputs are unchanged,
use a discrete marginal-profit search only after monotonic marginal economics
for that interval are established.

An exhaustive whole-step scan is the reference oracle and is acceptable in
production if benchmarks fit the predeclared latency budget. Otherwise the
breakpoint-aware optimizer must return the same candidate as exhaustive search
over deterministic fixtures and captured replays. A plain binary or ternary
search over the whole domain is not acceptable without proving discrete
unimodality after slippage clamps, integer rounding, recovery loss, and
inventory constraints.

Cap the number of prepared-curve/depth breakpoints and exact evaluations with
compile-time limits. If the optimizer cannot prove a result within those
limits, emit a stable rejection/fallback reason and use the baseline; never
silently use a partially searched maximum.

The best pool can change with size. Search every enabled pool independently;
the single pool stored in the current `market_liquidity_capacity` result is not
an execution-size oracle. That result can provide an initial gross-profit upper
bound, but every final candidate must pass the exact dynamic evaluation above.

## Admission economics

For each exact candidate, call the existing full-depth admission calculation
using the candidate token-B amount and expected cost/proceeds.

Normal-entry profitability is:

```text
expected_profit_token_a =
  max(candidate.proceeds_token_a - candidate.cost_token_a, 0)

meets_threshold =
  candidate.gross_venue_proceeds_token_a * 10_000
    >= candidate.gross_venue_cost_token_a
         * (10_000 + opportunity_threshold_bps)
```

The exact `TradeEvaluation` computes this verdict before applying the
Rails-compatible commission and DEX execution reserve, then admission persists
the boolean proof in the immutable plan. It must not approximate or recompute
the threshold from fee/reserve-adjusted amounts.

The separate worst-case diagnostic remains:

```text
fully_burdened_cost_token_a =
    candidate.cost_token_a
  + maximum_recovery_loss_token_a
  + maximum_gas_cost_token_a

bounded_profit_token_a =
  max(candidate.proceeds_token_a - fully_burdened_cost_token_a, 0)
```

`candidate.cost_token_a` and `candidate.proceeds_token_a` already contain
primary venue commission and the DEX execution reserve. They must not be added
again in admission.

Define the hard-cap measures as:

```text
trade_notional_token_a =
  max(candidate.cost_token_a, candidate.proceeds_token_a)

unhedged_notional_token_a =
  max(recovery_sell_quote_token_a, recovery_buy_quote_token_a)
```

An adaptive candidate is eligible only if:

- its exact gross `TradeEvaluation` still meets the normal opportunity
  threshold;
- full Binance sell and buy recovery depth exists and venue filters pass;
- native gas is covered at the admission-time gas-price sample; the signer
  follows Rails and resolves a fresh uncapped fee immediately before signing;
- `trade_notional`, `unhedged_notional`, and `maximum_recovery_loss` are within
  their config caps;
- expected spread profit is at least `min_expected_profit` (zero in v9-v11); the
  configured 20 bps threshold is the authoritative profitability gate;
- the exact direction-specific reservation fits currently available inventory;
- the quote, pool generation, balances, account state, order counters, nonce
  owner, and settlement barrier pass the existing readiness checks.

Baseline and adaptive candidates use the same 20 bps profitability contract.
Neither requires the full failure/recovery scenario to remain profitable.

## Inventory reservation

Do not multiply selected amounts by `balance_safety_multiplier`. Derive one
exact execution envelope from the immutable plan and coordinator state machine.

For every `(venue, asset)` key, define the admission claim as:

```text
claim(venue, asset) =
  max over every reachable execution branch and every prefix in that branch of
    cumulative possible debits(venue, asset)
    - credits already proven available at that venue before the next command
```

The envelope must include:

- the DEX router's exact `amount_in` or admitted `amount_in_maximum`, using the
  same conservative calldata bounds as the executor;
- the primary Binance IOC maximum base/quote debit after tick/step rounding and
  the admitted commission bound;
- every reachable partial-fill and recovery command, counting sequential
  debits cumulatively but not summing mutually exclusive branches;
- the maximum native-gas debit for all on-chain commands that can coexist in a
  reachable branch;
- any fee charged in a third asset as its own `(Binance, asset)` claim when the
  hydrated commission mode permits it.

For DEX-buy/CEX-sell, the executor accounts at most the immutable planned
token-B amount as hedgeable output; favorable output above that bound remains
in wallet inventory. Therefore the primary plus residual-recovery Binance WLD
debit cannot exceed the planned amount. The claims are the exact wallet USDC
input, planned Binance WLD, and admitted maximum wallet gas. For
CEX-buy/DEX-sell, the Binance USDC claim is the greater of the admitted primary
cost and the full-quantity fee-grossed recovery-buy quote; wallet WLD is the
immutable DEX input, and wallet gas is claimed separately.

Take the maximum cumulative debit across branch prefixes. Taking only the
largest individual order can under-reserve sequential primary/recovery debits;
summing every recovery template can over-reserve mutually exclusive paths.
Cross-venue proceeds never offset a claim unless a completed, reconciled
transfer makes them available at the spending venue.

Fast balance refresh does not eliminate reservations. Binance UDS, REST
snapshots, World Chain heads, transaction receipts, and rebalance settlement
advance independently, and unknown submissions may mutate external inventory
before the local balance changes. Reservations represent process ownership of
those possible debits; observed balances remain the source of truth for what
exists.

Do not use a multiplicative margin to preserve operational liquidity. If the
strategy needs capital that no trade may consume, configure an explicit
per-venue/asset `minimum_free_after_reservation` floor and apply it after the
exact claim. This makes the reason and amount visible instead of coupling it to
trade size.

Candidate ranking should use a read-only `reservation_fits` calculation against
the single owner's observed balances minus existing reservations. After
selection, create one atomic reservation. Because selection and reservation
run under the same state owner, failure after a successful fit is an invariant
violation and must fail closed with telemetry.

Shadow mode calculates whether a reservation would fit but never mutates the
inventory ledger.

The first exact-envelope implementation may retain the full claim until the
existing balanced-settlement barrier advances both venue generations. Later
claim shrinking is allowed only when coordinator evidence proves that a branch
or debit is no longer reachable. Unknown outcomes retain the full remaining
envelope until explicit reconciliation.

### Migration from the Rails multiplier

The v8 artifact records the reservation migration in its immutable snapshot ID
and source evidence. `exact_execution_envelope_v1` is a Rust safety invariant rather than
an operator-selectable policy, so configuration cannot switch live execution
back to the legacy multiplier. Explicit per-venue minimum-free floors may be
added later if an operator decides to segregate working capital.

- Apply the exact-envelope policy to baseline and adaptive trades through the
  same coordinator boundary; do not maintain two reservation algorithms.
- Keep `balance_safety_multiplier` readable only for old artifact compatibility
  while older snapshots remain replayable. V8-v9 omit it, and Rust does not use
  it for baseline or adaptive claims.
- Rebalance continues to reserve its exact source debit through the shared
  inventory owner.
- Emit the exact claims with each admission so production reconciliation can
  compare the immutable envelope with realized venue debits.

## Capital budget for trading during rebalance

The 25% `start_balance` is a soft rebalance trigger, not a trading floor. After
rebalancing starts, trading should continue to consume that remaining inventory
while exact reservations and the explicit hard minimum still permit it. Dynamic
sizing therefore does not cap candidates at `start_balance`.

Derive the runway `H`, do not choose it arbitrarily. `H_q` is the `q`-quantile
of maximum-size adverse plan equivalents that the bot could execute after a
rebalance starts and before the full required two-token rebalance cycle settles.
The budget must cover the spending side of both USDC and WLD because one
arbitrage direction depletes wallet USDC together with Binance WLD, while the
reverse direction depletes Binance USDC together with wallet WLD.

### Measuring `H`

Build route-specific empirical rebalance windows:

```text
window start = rebalance source inventory is reserved / execution begins
window end   = all required token operations complete and the final
               Binance + wallet settlement barrier reconciles
```

Do not stop the window at the first transfer completion. The executor handles
one rebalance operation at a time, so a USDC and WLD correction can form one
longer serial cycle. Keep separate distributions for direct World Chain routes
and the slower Optimism/Across routes; production capital should use the route
class that can actually be selected in the depleted direction.

Add a durable `rebalance_cycle_id` shared by every token operation in one
correction cycle and emit `rebalance_cycle_started` / `rebalance_cycle_settled`
with trigger balances, routes, operation IDs, and monotonic/UTC durations.
Without an explicit cycle boundary, pairing individual transfer events can
understate the serial two-token interval.

For every historical window `i`, replay the captured market/DEX stream through
the production-shaped adaptive optimizer, latest-pending mailbox, nonce lane,
preflight, settlement barrier, and exact reservations. Do not count raw Binance
ticks or all `meets_threshold` observations: Binance can produce far more
updates than the wallet lane can execute.

Record both:

```text
N_i = number of additional plans that would complete during the window

A_i,a = maximum prefix drawdown of depleted asset a during the window,
        including unreflected active/pending exact debits and subtracting only
        proven healing credits at the same venue
```

Dynamic sizes make `A_i,a` more accurate than transaction count. Express the
window as maximum-size plan equivalents only for reporting:

```text
H_i = max over depleted assets a of ceil(A_i,a / D_a)
H_P80 = quantile_0.80(H_i)
H_P95 = quantile_0.95(H_i)
```

Use P80 when accepting inventory exhaustion or sizing pauses in roughly 20% of
rebalance windows is an intentional capital-efficiency tradeoff. Use P95 as the
default production planning target. Report P50/P80/P95/P99 and sample count;
do not publish a quantile from too few route-comparable completed windows.

### Historical-data readiness as of 2026-07-20

The current ClickHouse history is not yet sufficient to select `H_P80` or
`H_P95`. It is sufficient to estimate the present approximately 20 USDC
executor service time and to show that a constant such as ten must not be used
as a planning value.

The production telemetry currently contains:

| Observation | Current sample | What it can establish |
|---|---:|---|
| Rebalance reservations with a matching inventory settlement | 9 of 13 | Individual successful token-operation windows only; four reservations are censored or unmatched |
| Successful Across/Optimism token-operation windows | 5 | Four wallet-to-Binance and one Binance-to-wallet; not enough for a route-and-direction percentile |
| Across/Optimism operation duration | mean 92.0 s; P50 73.5 s; P80 94.6 s; P95/max 155.9 s | Descriptive leg timings only; with five samples P95 is merely the maximum |
| Admission-to-result arbitrage operations | 242 of 243 results paired | Preliminary service-time distribution: P50 0.826 s, P80 1.274 s, P95 2.407 s |
| Admission-to-result outliers | 1 operation at 1,550.011 s | Recovery/unknown-outcome work must be classified separately from normal service time |
| Completed arbitrages inside the five Across windows | 8 total: `[0, 0, 2, 5, 1]` | Only one arbitrage direction is represented; the observed maximum of five is not `H_P95` |
| Executed arbitrage notional | P50 20.01 USDC; P95 20.09 USDC; max 20.39 USDC | No measured execution latency, fill behaviour, or recovery rate near the proposed 200 USDC cap |
| Capacity observations inside Across windows | 199 events in 3 windows; maximum observed capacity 181.40 USDC | All were emitted with `execution_ready = false`, so they cannot be counted as executable plans |

These are individual token-operation windows, not the required full two-token
cycle windows. Some adjacent USDC and WLD operations appear to belong to one
serial correction, but there is no durable cycle identifier with which to join
them. Treating the five Across legs as five complete cycles would systematically
understate rebalance duration.

A time-only calculation illustrates why the available data does not identify
`H`. Under a continuously saturated executor, dividing the 92.0-second mean
Across leg by the observed 2.407-second P95 service time gives 38 operations;
using the 155.9-second observed maximum gives 64. Using the slower direction's
3.563-second P95 service time still gives 25 and 43 operations respectively.
The actual completed counts were zero through five because profitable arrival
rate, preflight, the latest-pending mailbox, reservations, and recovery also
matter. Neither the mechanical range of 25--64 nor the observed maximum of five
is a valid runway estimate.

For an empirical P95, 20 independent route-comparable full cycles provide only
one observation in the upper 5% tail. Treat that as the absolute collection
minimum, not as a stable production estimate. Target at least 60 completed
cycles per selectable route and direction for an initial P95 (three tail
observations), and prefer 100 or more (five tail observations). Include failed
and recovered cycles rather than conditioning the duration distribution on
successful settlements. P80 can be reported earlier, but should remain
provisional until there are at least 20 comparable full cycles.

Before collecting that sample, add the shared `rebalance_cycle_id`, explicit
cycle start/end events, and replay inputs described above. Also record an
adaptive candidate's exact per-venue debits, executable/not-executable reason,
mailbox disposition, executor start/result timestamps, and whether each debit
is already reflected in the observed balance. Without those fields, more rows
will not turn the current observations into a defensible `H_i` distribution.

### Interim canary budget before `H` is measurable

Use **10,000 USDC-equivalent combined trading capital** as the temporary
canary budget. This is an operating limit, not a measured P80 or P95.

The temporary policy sets `H_interim = 5`, equal to the largest observed count
of completed arbitrages in an Across leg, and conservatively treats every one
as a future maximum-size 200 USDC plan even though the observed executions were
approximately 20 USDC. Add one maximum-plan envelope for trigger overshoot:

```text
temporary combined requirement = 1,600 * (H_interim + 1)
                               = 1,600 * 6
                               = 9,600 USDC-equivalent
rounded canary budget           = 10,000 USDC-equivalent
```

Start each venue at the 50% target:

| Inventory bucket | Initial target |
|---|---:|
| Binance USDC | 2,500 USDC |
| Wallet USDC | 2,500 USDC |
| Binance WLD | 2,500 USDC-equivalent |
| Wallet WLD | 2,500 USDC-equivalent |

Convert each WLD target with the conservative live conversion and configured
rounding rules; the budget is a value target rather than a persisted WLD unit
count. Keep exact reservation-aware admission and the explicit hard minimum
enabled: this budget does not promise uninterrupted trading during a saturated
opportunity burst. A time-only lower mechanical scenario of `H = 25` would
require 41,600 USDC-equivalent after the overshoot envelope, but current data
does not justify parking that capital.

After a capital injection, keep `start_threshold_bps = 2,500`; refresh the
in-memory reference inventory from the new settled Binance and wallet totals.
The current tracker captures reference inventory only once at process startup
and intentionally does not ratchet it upward. Waiting without a controlled
reference refresh leaves the 25% trigger anchored to the old budget and does
not redistribute the new capital. With the 2026-07-20 funded balances, the
expected values after refresh are:

| Token | Reference inventory | 25% start per venue | 50% target per venue |
|---|---:|---:|---:|
| USDC | 5,005.055540 | 1,251.263885 | 2,502.527770 |
| WLD | 13,848.7458266695 | 3,462.1864566674 | 6,924.3729133348 |

Do not enable the 200 USDC adaptive cap until the new reference is visible in
telemetry and the resulting Binance-to-wallet operations have settled. A
future operator-safe capital-change flow should recapture reference explicitly
without relying on an application rollout.

Recalculate the canary budget after 20 comparable full cycles and replace it
with the measured route-and-direction P95 target after at least 60, preferably
100, comparable cycles.

Prefer the empirical joint distribution above. Multiplying P95 rebalance
duration by P95 transaction rate combines two separate tails and ignores their
correlation. It is acceptable only as a conservative fallback when replayable
windows are insufficient:

```text
H_fallback = ceil(
  P95(full two-token Optimism/Across duration)
  * P95(executable same-direction plan rate)
)
```

For asset `a` at the venue that a direction spends:

```text
post_trigger_runway_a =
    observed_balance_at_rebalance_start_a
  - minimum_free_after_reservation_a
  - admitted_but_unreflected_debits_a

additional_plans_a = floor(post_trigger_runway_a / worst_plan_debit_a)
```

For capacity planning, let:

- `alpha = start_threshold_bps / 10_000`, currently `0.25`;
- `D_a` be the worst exact maximum-size plan debit of asset `a`;
- `O_a` be maximum trigger overshoot below `start_balance`;
- `R_a` be active/pending exact debits not already reflected in the observed
  balance or counted in the replayed `H_q` drawdown;
- `F_a` be the explicit hard minimum plus fee/rounding reserve.

The required total reference inventory for quantile target `H_q` is:

```text
reference_inventory_a >= (H_q * D_a + O_a + R_a + F_a) / alpha
```

In the idealized case where rebalance starts exactly at 25%, every admitted
debit is already reflected or included in `M`, and the hard floor is negligible:

```text
required total USDC          >= 4 * H_q * 200 USDC
required total WLD value     >= 4 * H_q * 200 USDC
combined trading capital     >= 8 * H_q * 200 USDC
                            = 1,600 * H_q USDC
```

| Measured runway `H_q` | Ideal combined capital | Per token | Initial amount per venue/token |
|---:|---:|---:|---:|
| 1 | 1,600 USDC | 800 USDC | 400 USDC |
| 5 | 8,000 USDC | 4,000 USDC | 2,000 USDC |
| 10 | 16,000 USDC | 8,000 USDC | 4,000 USDC |
| 20 | 32,000 USDC | 16,000 USDC | 8,000 USDC |

`Per token` means actual USDC plus WLD whose conservative value equals that
amount. Convert WLD value to token units using a conservative live WLDUSDC ask
and round WLD up to the Binance step. Do not persist a fixed WLD unit count in
the versioned artifact.

### Trigger overshoot

The current planner notices `< 25%` only after an observed balance update. A
single maximum-size trade can move the balance from just above the threshold to
as much as one plan debit below it. Therefore `O_a` is conservatively one
maximum plan debit unless the planner becomes reservation-aware and starts the
rebalance early enough to preserve the complete 25% runway.

With one trigger-overshoot envelope and no separate pending envelope:

```text
combined capital >= 1,600 * (H_q + 1) USDC
```

If the measured `H_P95` is ten, the ideal budget is 16,000 USDC combined and
the current-trigger-safe budget is 17,600 USDC combined. If one additional
pending plan is absent from the replayed drawdown, reserve another 1,600 USDC
of combined capital, for 19,200 USDC total. Ten is an example, not a selected
planning value.

The better runtime change is to make the rebalance trigger use projected
available inventory:

```text
observed
- exact active/pending trade reservations
- maximum next-plan debit
< start_balance
```

This starts the cold-path rebalance before the next maximum trade can consume
the intended 25% post-trigger runway. It does not block trading or reserve a
heuristic multiplier; it only changes when rebalancing begins.

The current documented approximately 1,000 USDC reference inventory leaves
about 250 USDC at the nominal threshold. It can fund only one ideal 200 USDC
post-trigger plan, and trigger overshoot can leave less than 200 USDC. It is not
enough for reliable trading throughout a multi-minute rebalance.

Native gas is separate from the two-token value above:

```text
native_gas_budget >=
  exact gas envelopes for active + pending plans + minimum native floor
```

Do not value native gas as tradable WLD/USDC inventory or rely on a future
rebalance to make an already admitted DEX command executable.

## Selection algorithm

For each threshold-crossing evaluation:

```text
baseline = current direction baseline

for each direction whose baseline clears the existing threshold:
  for each enabled pool:
    build the feasible whole-step sizing domain
    find the exact expected-profit maximum within that domain
    retain the pool winner

adaptive winner = max eligible candidate by:
  1. expected_profit_token_a descending
  2. unhedged_notional_token_a ascending
  3. trade_notional_token_a ascending
  4. token_b_amount ascending
  5. direction stable enum order
  6. pool index ascending

select adaptive winner only if:
  winner is larger than its direction baseline
  AND winner expected profit >= baseline expected profit
      + min_incremental_expected_profit

otherwise select baseline fallback
```

Select the adaptive depth tier at the decision instant:

1. use `sequence_matched_full_depth` when update and top are exact;
2. otherwise use `recent_full_depth` only when both configured age and update
   delta caps pass, after reconciling its top with the current bookTicker;
3. otherwise use `top_of_book_only`, search only liquidity covered by both
   observed top levels, and apply its separate notional cap.

Do not defer a threshold-clearing baseline merely because a larger adaptive
size cannot be calculated.

The objective is maximum expected absolute spread profit among candidates that
each pass the 20 bps threshold and all hard safety caps, not maximum size or
maximum profit bps. A larger candidate with lower expected profit must not win.
The smaller-risk tie breakers keep replay deterministic and avoid taking extra
exposure for no modeled benefit.

When full depth is available, the baseline comparison uses admission economics
recalculated from the same depth/gas snapshot as the adaptive candidates. On
the DEX-first fast path without matched depth, the baseline uses the current
relevant bookTicker top level and the same gas/risk checks instead. Adaptive
depth availability does not change whether the baseline itself is allowed to
execute.

## Immutable plan and restart contract

Persist the following with every admitted plan and copy it into result
telemetry:

- `sizing_mode`: `baseline` or `adaptive`;
- `depth_source`, `depth_age_ms`, `depth_update_delta`, `top_matches`, and
  `top_mismatch_reason`;
- configured notional cap, optimizer version, search upper bound, exact
  evaluation count, and fallback reason when applicable;
- baseline token-B amount, cost, proceeds, gross profit bps, and bounded profit;
- selected token-B amount, cost, proceeds, gross profit bps, bounded profit,
  trade notional, unhedged notional, and maximum recovery loss;
- pool index/generation, Binance update/depth generation, config fingerprint,
  and all existing admission bounds.

Recovery after restart uses only the persisted selected plan. It must not
re-run dynamic sizing against new market data.

## Interaction with the latest-pending mailbox

The existing mailbox already bounds backlog to one pending opportunity behind
the active wallet lane. Its mutex-protected replacement transfers ownership
atomically, and the engine releases the superseded plan's inventory reservation
and freshness state. A blocked-unknown lane accepts no new work; DEX settlement
invalidates pending work admitted against the pre-settlement generation. The
normal settlement path now extracts the canonical pool Swap position from the
successful receipt, catches that pool up through the receipt block with
`eth_getLogs`, and rebuilds prepared curves immediately. WebSocket delivery
retains ownership as the fallback when the receipt proof cannot be applied.

Adaptive v1 retains the current newest-wins replacement rule. This isolates the
sizing experiment from a scheduling-policy change, but it means that a fresher
smaller candidate may replace a pending larger candidate with higher expected
profit. Extend discard telemetry with both candidates' selected quantity,
notional, expected profit, market generation, and pending age so the opportunity
cost is visible.

An economic replacement policy can be a later reviewed change. It must define
freshness precedence, a minimum expected-profit improvement, expiry handling,
reservation ownership transfer, and deterministic behavior when submission
races with receiver pickup. It must never keep a pending plan whose preflight
inputs are already known to be stale.

## Hot-path and latency contract

- No RPC, REST, WebSocket request, database read, filesystem access, signing,
  or telemetry serialization is allowed during candidate construction.
- Use immutable prepared DEX curves, the sequence-consistent in-memory depth
  book, in-memory balances/reservations, and hydrated fee/gas snapshots.
- Bound curve/depth breakpoints and exact optimizer evaluations with
  compile-time limits and reuse fixed-capacity scratch storage; avoid per-probe
  heap allocation and pool cloning.
- Capture decision latency before JSON construction or ClickHouse enqueue.
- A missing prepared curve, stale generation, incomplete depth book, arithmetic
  overflow, or representation overflow fails closed.

Before adaptive shadow is enabled in production shape, benchmark baseline-only,
exhaustive-reference, and maximum-breakpoint optimizer cases at the 200 USDC
cap. Predeclare the acceptable absolute p99 budget and relative regression from
the current production benchmark; do not choose the budget after observing the
result.

## Telemetry and measurement

Add `arbitrage_adaptive_sizing_evaluated` with:

- baseline identity and economics;
- configured mode/caps, optimizer version, sizing-domain bounds, breakpoint
  count, exact evaluation count, and exhaustive-or-optimized search mode;
- selected direction, pool, exact token-B amount, notional, and sizing mode;
- selected expected profit, incremental expected profit, bounded-profit tail
  diagnostic, trade notional, unhedged notional, recovery loss, and reservation
  fit;
- rejection counts by stable reason, including gross threshold, DEX liquidity,
  primary/recovery depth, gas, trade cap, exposure cap, recovery-loss cap,
  expected-profit floor, incremental-profit floor, inventory, freshness, and
  settlement barrier;
- calculation and total decision latency.

Extend `arbitrage_admitted`, the persisted intent, and `arbitrage_result` with
the immutable baseline/sizing/selected fields listed above. Extend pending-
supersession and lane-unavailable telemetry with the economic fields described
above. Keep JSON construction on the existing bounded background telemetry
path.

For every rollout window, report:

```text
threshold-crossing directions
baseline admissions and results
shadow-selected/adaptive admissions and results
pending supersessions, lane-unavailable, and preflight rejections
selected notional p50/p95/max by direction
expected bounded profit and realized profit by notional bucket
expected-vs-realized error by notional bucket and direction
maximum recovery loss and actual recovery result
reservation failures, held reservations, and hold duration
projected post-trigger runway in maximum-size plans by direction
actual trigger overshoot and plans completed during rebalance
rebalance duration and executable-plan-count P50/P80/P95/P99 by route class
H P50/P80/P95/P99 and route-comparable sample count
decision latency p50/p95/p99
```

Preserve the exact ClickHouse query, UTC interval, source revision, config
fingerprint, execution mode, wallet/account identity, and result cutoff for
each comparison.

## Rollout

V8 is the explicitly reviewed capped-live artifact. It activates the 200 USDC
cap directly by operator decision; the stages below remain the evidence model
and rollback checklist rather than an assertion that every intermediate cap
was a separate production release.

### Stage 0: implementation without execution-size changes

- Add config, types, dynamic sizing, selection, and telemetry.
- Keep all committed production artifacts `baseline_only`.
- Prove old artifacts deserialize to baseline-only behavior.
- Compute legacy and exact-envelope claims side by side without changing the
  active reservation or admitted plan.
- Prove baseline plan bytes and execution behavior are unchanged; explain every
  admission-count difference projected by exact-envelope shadow telemetry.

### Stage 1: production-shaped shadow

- Commit a reviewed artifact with `mode = shadow` and a 200 USDC maximum
  notional cap, but continue executing only baseline.
- Collect enough observations in every intended direction and size bucket to
  calibrate recovery-loss and incremental-profit floors.
- Verify deterministic replay and quantify p99 decision-latency regression.
- Verify shadow creates no reservations, orders, nonces, or different baseline
  plans.

### Stage 2: paper adaptive

- Run the selected candidates through the complete paper coordinator and forced
  DEX/CEX failure matrix.
- Switch paper reservations from legacy `3x` to the exact execution envelope
  before enabling dynamic sizes above baseline.
- Exercise pending supersession, lane-unavailable, stale preflight, inventory
  shortage, balance refresh, settlement barrier, restart, and unknown-outcome
  paths at the baseline, optimizer interior, every breakpoint type, and the
  configured cap.

### Stage 3: capped live canary

- Use separate Rust wallet/account inventory and the existing explicit live
  gates.
- Start with a 40 USDC maximum notional cap. The selected size can be any
  Binance-step-aligned quantity between baseline and that cap.
- Predeclare sample size, maximum capital at risk, maximum loss, observation
  window, and stop conditions in the trading runbook before deployment.
- Do not raise the cap while any unknown outcome, reservation leak, nonce
  conflict, cap breach, unreconciled balance, or recovery invariant remains.

### Stage 4: controlled cap increases

- Raise the maximum notional cap to 80/100 USDC, then at most 200 USDC, through
  separate reviewed artifact revisions.
- Promote only when aggregate realized PnL is positive, expected-vs-realized
  error stays inside the predeclared shadow envelope, recovery and rebalance
  remain healthy, and the full-launch test gate stays green.

Changing `dex_first`/`concurrent_hedged` assignment or adding wallets during
these stages invalidates the isolated adaptive-sizing comparison.

## DEX hot-path freshness and confirmation

Admission uses two distinct clocks and must report both:

- `market_to_admitted_us` is the age of the Binance top-of-book used for the
  decision;
- `trigger_to_admitted_us` starts when the event that caused reevaluation is
  handled (`binance_book_ticker` or `dex_prepared`).

The production artifact sets `strategy.max_quote_age_ms = 30000`, matching the
Rails `MAX_QUOTE_AGE_SECONDS = 30` gate. This is an
execution gate, not telemetry: a DEX-triggered reevaluation of an older Binance
quote is rejected with `reason = quote_age_exceeded`. Runtime readiness can use
an independently configured market-data health window; production also sets it
to 30 seconds so readiness cannot silently narrow the admission window.

The immediate live DEX path performs local request validation and resolves the
predeclared gas envelope without `eth_call` or `eth_estimateGas`. Its native-gas
capacity is checked during admission against the in-memory wallet snapshot and
atomically reserved in the exact execution envelope. It therefore must not
repeat `eth_getBalance` before broadcast. Startup approval transactions retain
simulation, estimation, and a direct native-balance check.

Receipt detection uses the process-scoped Alchemy `newHeads` subscription to
wake a lookup immediately. HTTP receipt lookup remains a fail-safe: every 50 ms
for the first second, then every 250 ms until the bounded confirmation timeout.
The canonical receipt remains the only source of executed amounts and success
or revert status.

Waiting for a receipt need not permanently serialize all future DEX execution,
but multiple in-flight swaps must not be enabled by merely releasing the
executor mailbox after broadcast. A bounded pipeline first requires:

1. nonce-indexed pending transaction ownership and unknown-outcome recovery;
2. exact inventory and gas reservations for every in-flight plan;
3. an in-memory pending-pool overlay that applies each broadcast swap's
   conservative self-impact before sizing the next opportunity;
4. ordered canonical receipt application with rollback/rebuild on reorg or
   revert;
5. per-plan DEX-first Binance hedging only after that plan's canonical receipt;
6. a configured maximum in-flight count and a fail-closed stop when the overlay,
   head stream, nonce lane, or any outcome is uncertain.

Until these invariants exist, the single DEX lane remains the safety boundary.

## Rollback

For an immediate safety stop, activate the existing live entry-stop control.
Active and unknown plans continue reconciliation/recovery.

For a sizing rollback, publish a reviewed artifact with:

```json
"adaptive_sizing": {
  "mode": "baseline_only"
}
```

and deploy it through `.github/workflows/deploy-gke.yml`. The object shown is
the complete baseline-only variant. A sizing rollback requires restart because
config is loaded once. It does not require a code rollback and must not cancel
existing plans.

## Implementation slices

1. Add `AdaptiveSizingConfig`, validation, old-artifact compatibility, and a v7
   shadow artifact. Complete.
2. Add pure direction-specific exact candidate evaluation over every prepared
   pool and whole-step quantity.
3. Compile exact per-key reservation envelopes from the persisted coordinator
   graph, including native gas. Complete in v8.
4. Add exact admission/cap evaluation and deterministic selection. Complete.
5. Add shadow telemetry and replay/latency benchmarks. Complete in v7.
6. Add plan/journal/result fields and baseline/adaptive coordinator fixtures.
7. Extend latest-pending supersession telemetry with old/new sizing economics.
8. Enable exact-envelope reservations and adaptive sizing in the shared
   paper/live coordinator path. Complete in v8; v9 separates spread admission
   from worst-case envelope economics.
9. Make the rebalance trigger account for active/pending trade reservations and
   the maximum next-plan debit; emit projected/realized post-trigger runway.
10. Add durable rebalance cycle IDs and a replay/report that calculates joint
    P50/P80/P95/P99 duration, drawdown, plan count, and `H` by route class.
11. Set calibrated floors/caps in a reviewed canary artifact and update the
   trading runbook.

Primary code touch points:

- `src/domain/config.rs` for schema and validation;
- `src/opportunity.rs` for exact per-pool candidate evaluation and prepared-
  curve breakpoint exposure;
- `src/admission.rs` for reusable cap measures if they remain pure economics;
- `src/inventory.rs` for exact envelope claims, native/fee assets, and explicit
  minimum-free floors;
- `src/engine.rs` for selection, readiness, inventory fit/reservation, and
  telemetry;
- `src/arbitrage.rs` and `src/live_execution.rs` for immutable plan fields and
  entry-channel behavior;
- `src/hot_telemetry.rs` and result payloads;
- `config/strategies/usdc-wld-world-chain.v11.json`;
- monitor/comparison scripts and `docs/trading-runbook.md`.

## Verification and acceptance

Required tests before shadow:

- config absence defaults to baseline-only; unknown modes/fields fail closed;
- baseline/cap relationships and integer-overflow validation;
- direction-specific token-B derivation and conservative step rounding;
- pool choice can change with size and remains deterministic;
- a larger size is rejected independently by DEX liquidity, gross threshold,
  primary depth, reverse depth, gas, every cap, expected-profit floor,
  incremental-profit floor, and inventory;
- dynamic optimizer matches exhaustive whole-step argmax fixtures and captured
  replays, including DEX/depth breakpoints and rounding plateaus;
- optimizer limit exhaustion falls back to baseline with a stable reason;
- selection maximizes bounded absolute profit rather than size or bps;
- exact reservation claims equal the maximum cumulative debit across every
  reachable branch prefix, with no multiplicative safety factor;
- sequential debits are accumulated, mutually exclusive recovery branches are
  not summed, and cross-venue proceeds are not netted prematurely;
- every emitted primary/recovery command fits within its remaining reservation;
- native gas and possible third-asset commission have explicit claims;
- legacy-versus-exact reservation comparison covers baseline, optimizer
  interior candidates, and every rollout cap;
- shadow produces no reservation, plan, nonce, or order side effect;
- baseline-only produces the same plan and result fixtures as before;
- plan restart preserves selected size without re-selection;
- pending supersession releases exactly the replaced reservation and retains
  exactly the new one;
- rebalance starts early enough that active/pending reservations plus one
  maximum next-plan debit cannot consume the configured 25% runway unnoticed;
- post-trigger runway fixtures cover ideal, one-plan overshoot, pending-plan,
  and explicit minimum-free-floor cases;
- rebalance cycle IDs span every serial token operation through final settlement
  and cannot merge unrelated cycles;
- the `H` report is deterministic, route-stratified, and matches hand-calculated
  P80/P95 fixtures for variable-size and healing-direction plans;
- unknown outcomes retain reservations and block unsafe new entries;
- allocation/latency benchmark covers the maximum-breakpoint, all-pool 200 USDC
  case;
- `scripts/quality.sh` passes.

Before live adaptive mode, also require deterministic forced partial fill,
reject, timeout, DEX revert, receipt-unknown, CEX-unknown, restart, rebalance
overlap, and balance-reconciliation tests for every rollout notional bucket. The
authoritative live verdict remains `docs/rails-test-gap-analysis.md` and the
operator procedure remains `docs/trading-runbook.md`.

# Adaptive arbitrage sizing

Status: production architecture
Last reviewed: 2026-07-26

## Decision

Adaptive sizing answers one question:

> What is the largest Binance-step-aligned trade that the current local DEX
> curve can execute while the exact candidate still clears the configured
> 20 bps gross spread and the 200 USDC notional cap?

It does not predict failure recovery, inspect Binance depth, price native gas,
rank expected post-cost profit, or inspect balances. Those inputs do not change
the requested trade size.

The configured 20 USDC quote remains the cheap opportunity detector. Once that
baseline clears 20 bps, the adaptive worker searches the same prepared DEX
generation for the maximum executable slot. In `adaptive` mode the larger
candidate is executed; if no larger candidate exists, execution falls back to
the baseline.

This is a fixed architectural decision. Recovery is reactive to actual fills,
not a hypothetical input to normal trade sizing.

## Inputs and gates

Sizing uses only:

- the current Binance bid or ask price;
- one immutable prepared DEX pool generation;
- the Binance token-B step size;
- the exact DEX quote and Rails-compatible execution slippage for the candidate;
- the configured 20 bps gross opportunity threshold;
- `max_trade_notional_token_a_base_units`, currently 200 USDC.

Sizing does not use:

- Binance top quantity or full depth;
- hypothetical recovery price, depth, loss, or exposure;
- expected profit after commission, reserves, or gas;
- native-token conversion price or gas coverage;
- wallet or Binance inventory.

Binance depth remains diagnostic telemetry only. Silence or inconsistency in
that diagnostic feed cannot change runtime readiness, sizing, admission,
preflight, or execution eligibility.

## Search contract

For every enabled pool and each direction whose baseline clears 20 bps:

1. Start at the direction-specific baseline token-B amount.
2. Evaluate exact candidates at whole Binance steps against the prepared DEX
   curve.
3. Double the step count until the candidate fails because DEX liquidity,
   gross spread, or the 200 USDC cap is exhausted.
4. Binary-search that monotone boundary.
5. Select the largest valid candidate across pools and directions with stable
   deterministic tie-breaking.

The candidate is valid when:

```text
DEX exact quote exists
and gross_profit_bps >= 20
and exact DEX input notional <= 200 USDC
```

`TradeEvaluation.cost_token_a` and `proceeds_token_a` are raw venue economics.
They do not include Binance commission, gas, recovery, or slippage reserves.
DEX calldata uses separate `dex_amount_in` and `dex_amount_out_minimum` fields.

Rails-compatible slippage is derived as:

```text
slippage_bps = clamp(floor(gross_profit_bps * 0.5), 5, 50)
```

V3/V4 pool fees are already included by the local CLMM quote. Both directions
execute as exact-input swaps: the selected `dex_amount_in` is immutable and is
never increased by slippage or a provider fee reserve. Rust quotes that exact
input against the prepared curve and sets `dex_amount_out_minimum` to the
quoted output reduced by the configured slippage. Slippage is therefore only a
calldata validity bound, not an economic deduction or an input-size multiplier.

Rails' Uniswap services use the same exact-input router contract and reduce
only `min_buy_amount`, but the upstream Rails detector still increases DEX-buy
input by both slippage and the legacy `ZERO_X_FEE_BPS`. That provider-agnostic
uplift is not copied: it originated in the 0x path, while Uniswap fees are
already represented in the pool quote.

The optimizer is bounded to 128 exact evaluations. A limit breach falls back
to the baseline and emits a stable `evaluation_limit` reason.

## Admission and balances

Admission does not repeat the sizing calculation against Binance depth and
does not apply recovery-loss, unhedged-exposure, expected-profit-after-gas, or
native-gas-coverage gates.

After selection, Rust builds one immutable DEX plan and atomically reserves
only the primary execution debits:

- DEX-buy / CEX-sell: exact wallet token-A input and planned Binance token-B
  sell;
- CEX-buy / DEX-sell: planned Binance token-A buy cost, exact wallet token-B
  input.

The reservation uses observed free balance minus active exact reservations.
Insufficient balance is the only candidate-specific resource rejection.
There is no Rails `3x` multiplier and no speculative recovery reservation.

Native gas funding is an operator-maintained invariant. Native balance and RPC
gas price are absent from balance synchronization, admission, and inventory
reservations. The dedicated execution owner refreshes RPC gas price every
second and transaction construction reads its at-most-two-second cached RPC or
Rails-fallback sample. Receipt accounting includes both the L2 execution charge
(`gasUsed * effectiveGasPrice`) and World Chain's `l1Fee`; neither is a sizing
or admission gate. If the native-token conversion price is unavailable,
token-A gas accounting is recorded as unavailable/zero without blocking the
trade.

## Execution and recovery

The executor does not resize an admitted plan.

DEX-first execution sends the DEX transaction and hedges the actual filled DEX
delta on Binance. If the primary Binance IOC is partial or zero-fill, recovery
creates one immutable MARKET target equal to the primary hedge target minus the
primary executed quantity. A proven zero-fill/unsubmitted/rejected child may
retry that same target up to three total attempts after persisted 250 ms and
500 ms backoff. Partial/full fills and Unknown outcomes never retry. Recovery
results are not used to recalculate a new residual target. Any remaining WLD
delta is retained as inventory and marked in result accounting. Sizing and
admission do not attempt to forecast recovery.

Freshness and preflight remain separate execution-validity checks:

- Binance strategy-price transport and the DEX head must be fresh within the
  reviewed 30-second boundary;
- preflight rejects only if current prices no longer satisfy 20 bps or market
  data is stale;
- the DEX generation/settlement mechanism prevents using a local pool state
  that predates this bot's own fill.

## Telemetry

`arbitrage_adaptive_sizing_evaluated` records:

- the stable pair, strategy, Binance account, instrument, and network
  compatibility IDs used by the M0 per-strategy report;
- optimizer version `maximum_slippage_slot_v2`;
- configured and selected sizing modes;
- exact evaluation count and calculation latency;
- evaluation trigger;
- baseline direction, pool, token-B amount, and gross spread;
- selected direction, pool, token-B amount, cost, and proceeds;
- selected trade notional, execution slippage, and gross profit bps;
- rejection counts for `dex_liquidity`, `gross_threshold`, `trade_cap`, or
  `evaluation_limit`;
- the explicit sizing input list.

`arbitrage_opportunity` is emitted directly from a baseline direction when
`baseline.meets_threshold` becomes true. It carries the same trigger, baseline
pool, amount, and gross spread. There is no theoretical-capacity payload;
adaptive sizing is the only owner of executable-size calculation.

`arbitrage_adaptive_sizing_task` carries the same stable identity projection
and separately records queue, worker, result handoff, and snapshot timings. A
result is superseded only by a newer Binance quote, a newer prepared DEX
generation, or runtime freshness loss. Balance, gas, and depth changes do not
invalidate an in-progress sizing result.

## Active control

The only adaptive sizing amount control is:

```json
{
  "mode": "adaptive",
  "max_trade_notional_token_a_base_units": "200000000"
}
```

## Invariants

1. All strategy and execution math uses fixed-point integers or validated
   decimals; no `f64` enters the decision.
2. The prepared DEX generation and Binance quote used by the worker must still
   be current when its result returns.
3. Every candidate is an exact quote, never a linear scaling of baseline bps.
4. Only the final plan mutates reservations.
5. Post-admission balance ownership remains atomic across venues and assets.
6. Unknown submitted outcomes retain their nonce and remaining reservations
   until reconciliation, but never close the global execution lane.
7. Post-fill recovery remains available and operates on realized deltas only.

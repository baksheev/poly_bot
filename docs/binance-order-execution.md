# Binance bounded execution

Last reviewed: 2026-07-28

The Rust runtime now has a typed, single-owner Binance Spot order boundary for
`WLDUSDC`. Autonomous DEX-first arbitrage is enabled in the isolated GKE
production runtime. The manual `binance-order-round-trip` command is a
separately gated historical validation tool hard-capped at 10 USDC.

## Rails parity and transport

The manual canary preserves both Rails LIMIT and MARKET request shapes. The
autonomous arbitrage path is stricter:

- LIMIT orders use `timeInForce=IOC`;
- quantities are rounded down to the configured `0.1 WLD` step;
- BUY protection is rounded up and SELL protection down to the configured
  `0.0001 USDC` live exchange tick;
- MARKET recovery is rounded down to `MARKET_LOT_SIZE` (falling back to
  `LOT_SIZE`) and then checked against its quantity bounds and `MIN_NOTIONAL`
  using the fresh same-side top, with the persisted recovery price as a
  restart-safe fallback;
- a partial or zero IOC execution creates one immutable MARKET recovery target
  equal to `primary hedge target - primary executed quantity`;
- a proven zero-fill, unsubmitted, or deterministically rejected recovery child
  retries that same target at most three total attempts, after persisted
  250 ms and 500 ms backoff;
- partial/full recovery fills and unresolved Unknown outcomes never create
  another child;
- a local exchange-filter rejection is terminal for that immutable target and
  is not retried, because the same step-aligned below-minimum request cannot
  become safer through transport backoff;
- a target that rounds below the market step, minimum quantity, or
  `MIN_NOTIONAL` is classified as `residual_inventory_drift`, emitted through
  the `skipped` telemetry phase at INFO severity, and retained in result/PnL
  accounting; it is an expected non-mutation, not an execution error;
- deterministic client order IDs are queried through `order.status` after an
  ambiguous placement response.

An Unknown placement is recovered inside the same hedge operation. Rust never
blindly submits another order: it queries the same deterministic client ID once
immediately. If a terminal order appears, its actual fills decide whether any
MARKET recovery is needed. A Binance `-2013 NO_SUCH_ORDER` response records the
placement as absent/rejected and immediately continues the ordinary bounded
MARKET recovery. A timeout, disconnect, 5xx, or protocol error from the status
query remains Unknown because it does not prove zero execution.

Autonomous MARKET recovery is permitted only because a bounded DEX-created
exposure already exists; it may reduce that exposure but must never enlarge or
reverse it. Production commissions are paid from the account's BNB balance, so
MARKET BUY is never increased by a base-asset taker-fee allowance: BUY and SELL
both submit the recovery target rounded down to the exchange step. Actual BNB
commission remains part of reconciliation and realized PnL. In the separately
gated manual canary, MARKET BUY uses
`quoteOrderQty`, so the exchange-side input is capped directly. MARKET SELL
uses the exact post-BUY WLD balance delta rounded down to one exchange step. A
fresh top-of-book must show enough best-level quantity before the sell is
submitted.

Startup diagnostics record Binance's `enabledForAccount`, `enabledForSymbol`,
discount asset and discount multiplier alongside BNB balance presence. This
makes the account's BNB-fee configuration auditable before trading; the
terminal fill remains the source of truth for the asset Binance actually
charged.

The implementation follows Binance's documented rule that timeout or an
unexpected matching-engine response is an unknown execution result, not proof
of failure. Codes `-1006` and `-1007`, disconnect/internal errors, and 5xx
responses enter reconciliation and never authorize a duplicate placement. See
the official [Binance Spot API reliability guidance](https://developers.binance.com/en/docs/products/spot/rest-api).

## Hedge price telemetry

The autonomous path emits `arbitrage_binance_order` records for the complete
hedge chain. `plan_id` joins the DEX parent and `client_order_id` joins price
selection, the exact submitted order, and its terminal result.

- `primary_price_selection`: admission limit, fresh in-memory top, selected
  one-way-improved limit, exact target quantity, observed same-side quantity,
  whether that top covers the complete target, the selection reason, and
  `improved`. A favorable but insufficient top keeps the admission limit;
- `planned`: side/type, exact target and submitted quantities, limit price,
  full placement-time bid/ask snapshot with transport silence, and
  `limit_marketable_at_memory_top`. For the recovery MARKET child it also
  records a `recovery_limit_counterfactual` at the same-side in-memory top:
  hypothetical limit price, visible top quantity, submitted quantity, and
  whether that top covered the complete order;
- `terminal`: exchange transaction time, status and zero/partial/full class,
  actual base/quote fill, average execution price, commissions, individual
  fills, BNBUSDT bid used to value a BNB commission, valuation completeness,
  and reconciliation state. Recovery terminal telemetry repeats the exact
  placement counterfactual and adds:
  - whether the MARKET average and every reported fill respected that limit;
  - signed MARKET price advantage versus the hypothetical limit in token A and
    bps (positive means MARKET was better, negative means the limit protected a
    better price);
  - whether MARKET filled the submitted quantity;
  - a `snapshot_and_market_path_success_proxy`;
  - placement and terminal memory tops plus `planned_to_terminal_us`;
- `error`: unsubmitted, locally filtered, exchange-rejected, or unresolved
  placement, including the bounded `error_reason` returned by validation,
  Binance, or reconciliation.
- `skipped`: the immutable residual, rounded quantity, reference price,
  notional, live minimums, and exact dust reason when no Binance-compliant
  MARKET child can be submitted without enlarging the target.

The same `planned` and `terminal` phases cover every bounded recovery child, so
each placement-time top can be compared directly with its actual average fill
price. They record `recovery_attempt` and `maximum_recovery_attempts`; attempts
use deterministic suffixes `r1`, `r2`, and `r3`. The retry deadline is
journaled before waiting, and `retry_scheduled` plus
`recovery_retry_backoff` telemetry exposes the selected attempt and actual
delay. Only a proven zero execution can advance to the next child; an ambiguous
outcome remains on the same child ID until reconciled. A partial/full recovery
fill or exhaustion of three proven failures closes recovery. Any remaining WLD
inventory delta is recorded and marked in PnL, but never causes a residual-based
balance order. This dataset is the required evidence for any later proposal to
remove MARKET recovery and submit one more aggressive in-memory-top-priced
order.

The counterfactual is diagnostic, not proof that a LIMIT IOC would have filled:
top quantity can disappear and queue position is unknown between the local
snapshot and matching. A future analysis must report the top-coverage cohort,
the market-path proxy, price advantage, and zero/partial/full outcomes
separately. When a deterministic client ID has multiple `planned` rows after
restart reconciliation, use the earliest row as the original placement
snapshot and retain `reconciled_after_unknown` as a separate cohort.
The record timestamp measures the end-to-end gap between the LIMIT result and
the recovery request; the existing `arbitrage_execution_stage` events, joined
by `operation_id`, retain microsecond `worker_queue`, `placement_ws_api`, and
`worker_total` durations.

BNB fee accounting uses a separate Spot `BNBUSDT` bookTicker. It is
accounting-only: it cannot affect strategy readiness, admission, preflight,
sizing, or whether an existing exposure is recovered. The exact BNB debit is
always retained. A fresh BNBUSDT bid is used as the token-A-equivalent value
with the same USDT/USDC numeric-parity assumption as Rails. Missing valuation
marks telemetry incomplete instead of turning a known Binance fill into
Unknown.

## Ownership and durable recovery

`BinanceExecutionService` runs on the dedicated `binance-executor` OS thread.
A bounded channel feeds its append-only order journal, while the process-scoped
multiplexing actor owns the authenticated WebSocket for both order RPC and UDS
events. This prevents an unsolicited account event from being consumed and
discarded while an order response is pending.

Before `order.place`, the worker fsyncs an intent containing the deterministic
client ID, symbol, side, type, quantity, and optional price. It then records one
of:

- `terminal` for `FILLED`, `EXPIRED`, `CANCELED`, `EXPIRED_IN_MATCH`, or
  exchange `REJECTED` status;
- `rejected` for an unambiguous request rejection;
- `submitted` while a known order is non-terminal;
- `outcome_unknown` when submission may have reached the matching engine.

The journal is checksum-protected, mode `0600`, process-locked, and fsynced.
Credentials and signed request payloads are never stored. Startup reads and
validates every record and applies the same bounded same-ID reconciliation.
`order_status_reconciliation` measures the status query. A still-ambiguous operation remains
attached only to its parent plan; it cannot authorize a duplicate order. The
parent trade journal replays that same command in reconciliation mode after a
restart, consumes the Binance journal's terminal/confirmed-absent result, and
then resumes the ordinary hedge/recovery state machine.

## Manual capped canary

```bash
BINANCE_LIVE_CONFIRMATION=I_UNDERSTAND_BINANCE_LIVE_10_USDC \
BINANCE_ORDER_JOURNAL_PATH=/secure/path/binance-orders.jsonl \
  cargo run --release -- binance-order-round-trip \
  --order-type limit \
  --quote-usdc 10 \
  --price-deviation-bps 50
```

Use `--order-type market` for the MARKET round trip. The command refuses to run
when the account is not trade-enabled Spot, WLD/USDC is locked, an open
`WLDUSDC` order exists, the journal has an unresolved operation, the BUY cap is
above 10 USDC, or LIMIT protection is wider than 50 bps.

## Production evidence

On 2026-07-17 the dedicated Binance subaccount completed four live orders:

| Type | Side | Order ID | Executed WLD | Executed USDC | Status |
| --- | --- | ---: | ---: | ---: | --- |
| LIMIT IOC | BUY | `455788994` | `26.10000000` | `9.92583000` | `FILLED` |
| LIMIT IOC | SELL | `455788998` | `26.10000000` | `9.92061000` | `FILLED` |
| MARKET | BUY | `455789048` | `26.20000000` | `9.96386000` | `FILLED` |
| MARKET | SELL | `455789056` | `26.20000000` | `9.96124000` | `FILLED` |

The LIMIT round trip cost `0.00522000 USDC`; the MARKET round trip cost
`0.00262000 USDC`. WLD returned exactly to `15642.68043503` after each round
trip. Independent `allOrders` reconciliation returned the same four terminal
orders. The journal contains four operations, all terminal, and no active
operation. The GKE service remained 1/1 Ready and its rebalancer stayed healthy
with no in-flight or blocked operation.

This validates fully filled LIMIT IOC and MARKET placement. Forced partial
fill, live ambiguous-placement recovery, User Data Stream agreement, and
LIMIT-to-MARKET residual recovery remain separate canaries before autonomous
arbitrage can be enabled.

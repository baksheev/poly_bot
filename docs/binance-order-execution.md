# Binance bounded execution

Last reviewed: 2026-07-23

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
- a partial or zero IOC execution is followed by one persisted MARKET recovery
  for only the exact actionable residual;
- deterministic client order IDs are queried through `order.status` after an
  ambiguous placement response.

Autonomous MARKET recovery is permitted only because a bounded DEX-created
exposure already exists; it may reduce that exposure but must never enlarge or
reverse it. In the separately gated manual canary, MARKET BUY uses
`quoteOrderQty`, so the exchange-side input is capped directly. MARKET SELL
uses the exact post-BUY WLD balance delta rounded down to one exchange step. A
fresh top-of-book must show enough best-level quantity before the sell is
submitted.

The implementation follows Binance's documented rule that timeout or an
unexpected matching-engine response is an unknown execution result, not proof
of failure. Codes `-1006` and `-1007`, disconnect/internal errors, and 5xx
responses enter reconciliation and never authorize a duplicate placement. See
the official [Binance Spot API reliability guidance](https://developers.binance.com/en/docs/products/spot/rest-api).

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
validates every record; a non-terminal operation must reconcile by client ID or
the execution lane remains blocked.

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

# Binance strategy price liveness and telemetry

Status: current production contract

This document covers the first step of the trading pipeline: receiving and
applying the Binance strategy price. The authoritative ownership and execution
rules are defined in
[`rust-production-architecture.md`](rust-production-architecture.md).

## Problem being solved

Binance Spot `bookTicker` is event-driven. It publishes a new frame when the
best bid, ask, or their quantities change; it is not a periodic price
heartbeat. The absence of a price frame can therefore mean either:

- the top of book is unchanged and the WebSocket is healthy; or
- the transport is disconnected or no longer delivering data.

The age of the last price change cannot distinguish those cases. Using it as a
readiness or preflight gate can pause valid trading during a quiet market.
Production therefore separates price-content age from transport liveness.

## Current production path

```text
Binance WLDUSDC Spot bookTicker frame
  → process-scoped persistent WebSocket
  → exact JSON/Decimal parse
  → symbol + generation + update-ID validation
  → single-owner in-memory top-of-book apply
  → readiness check
  → local opportunity evaluation
  → bounded asynchronous telemetry

Binance server Ping
  → client Pong
  → transport activity for the same connection generation
```

There is no RPC, REST request, channel, database, or second task wake-up
between a price frame and the decision. Binance depth is complementary sizing
state and is not the strategy-price trigger.

## Liveness contract

The latest accepted top remains current while all of the following are true:

1. its WebSocket connection generation is still connected;
2. the generation has received a price frame, depth frame, or server Ping
   within `max_transport_silence_ms = 30000`;
3. the runtime's other independent readiness inputs are healthy.

`strategy.max_transport_silence_ms` in the versioned domain artifact is the
only source of this boundary for strategy-price runtime readiness, opportunity
admission, and entry preflight. There is no environment-variable override for
the strategy price. The gas-conversion feed has a separate
`GAS_PRICE_MAX_TRANSPORT_SILENCE_MS` runtime setting because it is not a
strategy-price source.

Price and depth frames advance transport activity. A Binance server Ping is
answered with Pong and also advances transport activity. The documented
20-second server Ping cadence keeps a healthy quiet connection inside the
reviewed 30-second silence boundary.

The runtime fails closed when:

- the WebSocket disconnects;
- a new connection generation replaces the current generation;
- transport activity is silent for more than 30 seconds; or
- the current generation has not yet supplied its first valid price.

Reconnect never reuses a price from an earlier generation. Runtime readiness,
opportunity admission, and entry preflight all use the same transport-liveness
semantics.

The time since the last price change is telemetry only. It must not become a
readiness, admission, or preflight gate.

## Exchange-to-socket diagnostic

The JSON Spot
[`bookTicker`](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#individual-symbol-book-ticker-streams)
payload does not include an exchange event or transaction timestamp. Direct
exchange-to-socket latency for that frame is therefore unavailable.

The already subscribed JSON
[`depthUpdate`](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#diff-depth-stream)
stream does include Binance event time `E`. Production retains that field and
enriches each
`binance_depth_applied` event with:

- `exchange_event_ts_ms` from Binance;
- `received_unix_us` captured before JSON parsing;
- `exchange_event_to_socket_estimate_us`;
- `exchange_event_to_socket_uncertainty_us`;
- estimate validity, invalid reason, and maximum permitted clock-sync age;
- clock offset, offset resolution, synchronization RTT, midpoint uncertainty,
  synchronization age, and observation time;
- wire-frame size and parse-plus-apply duration.

The estimate is:

```text
local_receive_us
  + binance_clock_offset_us
  - exchange_event_time_us
```

The published uncertainty includes half of the clock-synchronization RTT plus
the millisecond resolution of both the JSON event timestamp and clock offset.
Negative or very small estimates are retained rather than clamped because
they expose clock uncertainty.

The runtime refreshes the diagnostic Binance clock observation every 60
seconds through the process-scoped REST client. A failed refresh emits an
unhealthy `binance_clock_sync` event and retains the last successful raw
observation.

An exchange-to-socket estimate is valid only while that successful clock
observation is at most 180 seconds old. Each depth event records
`exchange_event_to_socket_estimate_valid`, the 180-second maximum, and an
explicit invalid reason. When the observation is unavailable or older than the
maximum:

- raw exchange and local receipt timestamps remain available;
- clock offset, RTT, and observation age remain available for diagnosis;
- the estimate and its uncertainty are emitted as `null`.

Clock-sync health and estimate validity are diagnostic only. They never change
readiness, price state, admission, preflight, or execution.

This metric describes the JSON depth publication path. It is a proxy for
Binance market-data transport health, not the latency of the strategy
`bookTicker` frame and not a matching-engine transaction timestamp.

## Execution deadline

The DEX plan deadline is derived from admission time, not from the timestamp of
the last price change. A valid unchanged top therefore cannot create an
already-expired plan.

## Required telemetry

Every accepted `binance_book_ticker` event records:

- feed role, symbol, connection generation, and update ID;
- wire-frame size and local parse duration;
- local receipt timestamp and exchange timestamps when Binance supplies them;
- runtime phase and exact decision outcome;
- frame-receipt to completed-decision duration;
- background telemetry-queue delay.

Every accepted server Ping emits `binance_feed_heartbeat` with:

- feed role and symbol;
- connection generation;
- acceptance status;
- local observation age.

The minute-level `binance_price_health` event records:

- connection health and generation;
- last update ID and current price age;
- current transport age and the configured 30-second maximum;
- the domain-artifact source of the strategy-price silence boundary;
- cumulative accepted and rejected updates;
- runtime phase;
- cumulative dropped hot-telemetry records;
- whether the current payload exposes an exchange timestamp.

Telemetry uses bounded asynchronous channels and must never delay price
application, opportunity evaluation, admission, or execution.

## Future improvement

Binance SBE
[`@bestBidAsk`](https://github.com/binance/binance-spot-api-docs/blob/master/sbe-market-data-streams.md#best-bidask-streams)
is a possible parallel diagnostic observer because it is the best-bid/ask
equivalent of JSON `bookTicker` and includes microsecond `eventTime`. It may be
evaluated later with a separate market-data-only Ed25519 API key.

Any SBE work must start as a non-authoritative observer. It must not replace
JSON `bookTicker`, change readiness, or enter opportunity evaluation until a
separate architectural decision reviews event equivalence, auto-culling,
sequence behavior, latency, credentials, failure isolation, and rollback.

## Operational interpretation

- A price gap longer than 30 seconds is valid while transport activity remains
  within 30 seconds.
- A `binance_top` degradation caused only by price-content age is a contract
  violation.
- A `binance_top` degradation caused by disconnect, generation replacement, no
  first price, or transport silence beyond 30 seconds is expected fail-closed
  behavior.
- Rejected heartbeats, rejected price updates, dropped telemetry, and transport
  intervals approaching 30 seconds require investigation.

ClickHouse arrival time must never be substituted for local socket receipt
time. Local receipt-to-decision latency remains fully observable independently
of the depth-based exchange-to-socket diagnostic.

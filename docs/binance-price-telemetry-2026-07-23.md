# Binance strategy price: 24-hour telemetry baseline

Status: immutable production evidence
Window: `[2026-07-22T07:01:40Z, 2026-07-23T07:01:40Z)`
Created: 2026-07-23

This report describes only step 1 of the trading pipeline: receiving and
applying the Binance strategy price. The architecture is locked in
[`rust-production-architecture.md`](rust-production-architecture.md); these
measurements are used to observe it, not to choose another price path.

## Path being measured

```text
Binance WLDUSDC Spot bookTicker frame
  → persistent WebSocket read
  → server Ping / client Pong transport heartbeat
  → exact JSON/Decimal parse
  → symbol + generation + update-ID validation
  → single-owner in-memory top-of-book apply
  → readiness check
  → local opportunity evaluation
  → asynchronous telemetry
```

There is no RPC, REST, channel, database, or second task wake-up before the
decision. Binance depth is complementary sizing state and not the price
trigger.

## Continuity and volume

| Metric | 24-hour result |
| --- | ---: |
| WLDUSDC `bookTicker` records | 257,019 |
| Unique `(generation, update_id)` records | 257,019 |
| Rejected/regressed strategy-price records | 0 |
| Book/depth mismatch events | 0 |
| Direct book-triggered evaluations | 256,511 |
| Secondary evaluations after DEX curve preparation | 7,640 |
| Feed disconnects | 1 |
| Feed reconnects | 1 |

The only disconnect was the intentional rotation before Binance's 24-hour
connection limit:

```text
2026-07-23 05:44:21.457 UTC  generation 1 disconnected
2026-07-23 05:44:24.257 UTC  generation 2 connected
```

The connection recovered in 2.8 seconds, below the 30-second transport-silence
limit.
There were no malformed, stale-generation, duplicate, or regressed price
events in telemetry.

Before the new per-frame outcome field, 508 accepted price records had no
matching direct evaluation row. Runtime phase transitions show that evaluation
was intentionally skipped during degraded periods, but the old schema cannot
prove the reason for each individual frame. New telemetry records
`decision_outcome` on the price record itself.

## Local latency

Durations start when the WebSocket payload is handed to the local parser.

| Measurement | p50 | p95 | p99 | max |
| --- | ---: | ---: | ---: | ---: |
| Frame receipt → completed engine decision path | 12.8 µs | 36.2 µs | 2,350.5 µs | 20,527 µs |
| Frame receipt → evaluation emitted | 8.8 µs | 25 µs | 31 µs | 1,531 µs |
| Opportunity calculation only | 5.3 µs | 15 µs | 21.8 µs | 1,523 µs |

The first row includes synchronous readiness, admission, reservation, and
latest-mailbox work performed before raw price telemetry is enqueued. The p99
tail therefore does not mean JSON parsing or opportunity math took 2.35 ms.
The new schema separates local parse time, completed decision time, and
background telemetry-queue delay.

## Update cadence and freshness

| Measurement | Result |
| --- | ---: |
| Inter-arrival gap p50 | 5.862 ms |
| Inter-arrival gap p95 | 1.682 s |
| Inter-arrival gap p99 | 5.291 s |
| Inter-arrival gap p99.9 | 14.264 s |
| Maximum gap | 52.469 s |
| Gaps longer than 30 seconds | 20 |

Those 20 gaps exactly match the 20 runtime degradations whose sole blocking
input was `binance_top`. They caused 143.632 seconds of degraded time after the
old 30-second price-age allowance was exhausted. The median degraded remainder
was 6.654 seconds.

This was a false degradation, not expected behavior: `bookTicker` emits when
the top changes, so silence can mean an unchanged market while the WebSocket is
healthy. A post-window check found another seven identical degradations and a
maximum 61.476-second update gap. This evidence motivated the v12 transport
liveness decision.

Across all readiness inputs there were 29 `Ready → Degraded` transitions:

| Blocking input | Periods | Total degraded time |
| --- | ---: | ---: |
| Binance strategy top | 20 | 143.632 s |
| Balances | 6 | 40.789 s |
| ETH gas-conversion price | 1 | 26.498 s |
| Balances and DEX mirror | 2 | 0.154 s |

## What cannot be measured from this stream

All 257,019 WLDUSDC events had neither an exchange event timestamp nor an
exchange transaction timestamp. That is the actual payload contract observed
for this Spot `bookTicker` stream. Consequently:

- exchange matching-engine → local socket one-way latency is unavailable;
- ClickHouse `observed_at` cannot be substituted because telemetry is
  deliberately asynchronous;
- inter-arrival gaps describe update cadence, not network latency.

This limitation does not justify changing the strategy price source. If
transport RTT is needed, it must be measured by a separate diagnostic probe
that cannot become a price source or enter the decision path.

## Telemetry added after this baseline

Every `binance_book_ticker` event now includes:

- `feed_role`;
- `wire_frame_size_bytes`;
- `parse_time_us`;
- `runtime_phase`;
- `decision_outcome`;
- `decision_complete_us`;
- `telemetry_queue_delay_us`;
- `exchange_timestamp_available`.

The existing `engine_queue_age_us` field remains as a compatibility alias for
`decision_complete_us`.

A new minute-level `binance_price_health` event records:

- connection health and generation;
- last update ID and current price age;
- cumulative accepted and rejected updates;
- configured maximum transport silence;
- runtime phase;
- cumulative dropped hot-telemetry records;
- whether the current Binance payload exposes an exchange timestamp.

All additions use the bounded asynchronous telemetry path. They do not change
the WebSocket, price validation, runtime readiness, opportunity calculation,
admission, or execution behavior.

## Architecture decision after the baseline

Production v12 separates content age from transport health:

- the last accepted top remains current while its connection generation is
  connected and transport activity is no older than 30 seconds;
- an incoming Binance server Ping is answered with Pong and advances transport
  activity;
- price and depth frames also advance transport activity;
- disconnect, generation replacement, or transport silence beyond 30 seconds
  closes readiness and entry preflight;
- reconnect requires a new first price before readiness returns;
- `price_age_ms` and `price_unchanged_for_us` remain telemetry only.

The DEX deadline is derived from admission time, not from the timestamp of the
last price change. An unchanged-but-live price therefore cannot create an
already expired plan.

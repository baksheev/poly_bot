# Rust production trading architecture

Status: **authoritative**
Last reviewed: 2026-07-23
Applies to: production arbitrage, recovery, settlement, inventory, rebalancing,
and delivery

This is the repository's primary architectural decision document. Every change
to the Rust trading path must be checked against it. Component documents may
provide implementation detail, experiments, or historical evidence, but this
document wins when their guidance conflicts.

Code, the reviewed versioned domain artifact, and the deployment workflow remain
the executable sources of truth. A change that intentionally alters a decision
below must update this document in the same commit.

## Objective

Run an autonomous, low-latency Rust implementation of the Rails arbitrage
strategy while keeping execution ownership, capital, credentials, nonces, and
recovery completely isolated from Rails.

Rust should preserve economically relevant Rails behavior where it is proven
useful, but it must not copy Rails' database/job topology or its weak ownership
boundaries.

The priority order is:

1. no duplicate or uncontrolled external mutation;
2. no unresolved exposure blocking unrelated capital or future work;
3. exact balance, nonce, fill, and PnL accounting;
4. positive comparable PnL;
5. low decision and execution latency;
6. throughput.

Throughput must never be increased by weakening items 1–3.

## Production topology

- Production is one Pod in the private zonal GKE Standard cluster `arb-bot`,
  zone `asia-southeast1-b`, on the existing fixed `c4-highcpu-8` node.
- The GKE Deployment uses `Recreate`; there is exactly one live process owner.
- `arb-bot-rust-shadow-gce` is a stopped rollback target. It must remain
  `TERMINATED` while GKE has a nonzero application replica count.
- Rails remains independent and controls different wallets, Binance
  credentials, orders, journals, nonces, and inventory.
- Production application delivery goes only through
  `.github/workflows/deploy-gke.yml` from `main`, after CI and production
  approval. Workstations do not build or roll out production images.
- ClickHouse, Postgres, and Rails services are outside the critical trading
  path.

## Runtime ownership

One Rust process owns:

- Binance market data and sequence state;
- local DEX pool mirrors and prepared quote curves;
- strategy state and the latest opportunity mailbox;
- Binance and wallet balance snapshots;
- inventory reservations;
- Binance order and EVM nonce ownership;
- parent trade, child order, transaction, recovery, and rebalance journals;
- execution and settlement state.

Network clients, signers, connection pools, parsed configuration, and journals
are process-scoped. No per-tick or per-order code may construct a new general
RPC, HTTP, WebSocket, signer, or database client.

## End-to-end pipeline

```text
Binance bookTicker/depth ─┐
Alchemy logs/newHeads ────┼─> single in-memory state owner
balances/user data/gas ───┘              │
                                        v
                            exact opportunity evaluation
                                        │
                                        v
                           admission + exact reservation
                                        │
                                        v
                             latest-only execution mailbox
                                        │
                                        v
                                  entry preflight
                                        │
                                        v
                           durable DEX-first coordinator
                              │                    │
                              v                    v
                     DEX receipt/accounting  Binance IOC hedge
                                                   │
                                                   v
                                            residual recovery
                              │                    │
                              └─────────┬──────────┘
                                        v
                               balanced result/PnL
                                        │
                         ┌──────────────┴──────────────┐
                         v                             v
                pool-state settlement       balance settlement
                                                       │
                                                       v
                                                 rebalancing
```

## Market data and readiness decisions

### Locked decision: Binance strategy price

The Binance price path is final production architecture, not an experiment:

- WLDUSDC Spot `bookTicker` over one process-scoped persistent WebSocket is the
  source of the executable Binance bid, ask, and best-level quantities.
- The WebSocket future is polled directly by the Tokio task that owns strategy
  state. There is no channel, job queue, REST request, database write, or task
  hand-off between frame receipt and opportunity evaluation.
- A frame is parsed with exact decimal arithmetic, checked against the expected
  symbol, connection generation, and monotonically increasing update ID, then
  applied to in-memory state.
- An accepted frame immediately triggers local opportunity evaluation whenever
  normal runtime readiness is satisfied.
- Binance server Ping frames are answered with Pong and recorded as transport
  activity through the same single-owner event boundary. Binance documents a
  20-second server Ping cadence; the reviewed 30-second silence threshold
  preserves a bounded margin without depending on price changes. See the
  [Binance Spot WebSocket contract](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md).
- Because `bookTicker` is event-driven, an unchanged top remains the current
  top while the same connection generation is connected and has transport
  activity within 30 seconds. The age of the last price change is telemetry,
  not a readiness or admission gate.
- Sequence-consistent depth may increase sizing confidence and capacity, but it
  does not replace `bookTicker` as the price source or DEX-first trigger.
- Spot REST is bootstrap, recovery, and diagnostics only. Futures prices,
  Rails relays, Postgres, Redis, ClickHouse, and a second symbol allowlist are
  forbidden as strategy price sources.

Do not redesign, replace, or insert another owner into this path. Do not restore
a wall-clock age gate on an unchanged `bookTicker` value. Future work
on this step is limited to telemetry, connection reliability, parsing
validation, and performance improvements that preserve the exact ownership and
decision semantics above.

Telemetry for every accepted strategy-price frame must make these boundaries
observable without delaying the decision:

- wire-frame size and local parse duration;
- connection generation, update ID, receipt timestamp, and any exchange
  timestamps actually supplied by Binance;
- runtime phase and the exact decision outcome (`evaluated`, not ready, depth
  mismatch, or no pair evaluation);
- frame-receipt to completed decision duration;
- delay in the background telemetry queue;
- cumulative accepted/rejected update counts, price age, connection health,
  and dropped hot-telemetry records in a periodic health event.

The current Spot `bookTicker` payload does not supply an exchange event or
transaction timestamp. Therefore exchange-to-socket one-way latency is
unobservable from this stream and must be reported as unavailable, never
invented from ClickHouse arrival time. Local receipt-to-decision latency remains
fully measurable.

The frozen pre-instrumentation baseline is
[`binance-price-telemetry-2026-07-23.md`](binance-price-telemetry-2026-07-23.md).

### Other market-data decisions

1. Update IDs and reconnect generations must be monotonic.
2. Sequence-consistent full depth is used for adaptive sizing when healthy.
   Top-of-book remains an explicitly capped production fallback.
3. DEX quotes come from the local CLMM mirror. RPC Quoter calls are validation
   and replay tools, not hot-path dependencies.
4. A DEX log updates the mirror in canonical order and creates a new prepared
   pool generation. Prepared curves are immutable once published.
5. New entries require fresh Binance top, DEX mirror/head, balances, Binance
   user data, gas price, and execution ownership. Full-depth health alone does
   not change `RuntimePhase::Ready` in DEX-first production.
6. The Binance market-data limit is 30 seconds of transport silence. A
   disconnect, generation change, or heartbeat timeout closes readiness and
   preflight. The time since the last top change does not.

## Opportunity and admission decisions

1. Evaluate both supported directions with fixed-point arithmetic:
   buy token B on DEX/sell on CEX, and sell token B on DEX/buy on CEX.
2. The configured 20 bps gross venue spread remains the Rails-compatible
   opportunity gate.
3. Binance commission, DEX execution reserve, gas, recovery bounds, inventory,
   and notional caps must still be calculated and persisted for every admitted
   plan.
4. Gas and worst-case recovery are safety/reservation inputs. Recovery is not
   required to be profitable.
5. Baseline sizing preserves the reviewed Rails-compatible notional. Adaptive
   sizing may select only an exact Binance-step-aligned amount within the
   versioned 200 USDC cap, depth age/update-delta limits, and the 40 USDC
   top-only cap.
6. Candidate ranking uses executable expected primary economics, not raw spread
   alone. A change to the profitability gate or ranking objective requires a
   reviewed config version and an equal-window comparison.
7. Admission atomically reserves `exact_execution_envelope_v1`: DEX input,
   Binance hedge inventory, and native gas. The legacy Rails `3x` multiplier is
   forbidden.

## Scheduling and preflight decisions

1. The execution queue is latest-only, never FIFO. A newer pending plan replaces
   an older unsubmitted plan and releases the old reservation.
2. Production retains one global mutation lane. Parallel market calculation and
   pool preparation are allowed; overlapping live mutations require a separate
   reviewed execution-mode experiment.
3. When the lane becomes available, the selected plan must be revalidated
   against:
   - latest Binance price and relevant top quantity;
   - matching connected generation and fresh Binance transport heartbeat;
   - exact DEX pool generation;
   - plan deadline;
   - current entry-stop state;
   - its still-active inventory reservation.
4. A failed preflight releases an unsubmitted reservation and cannot become an
   unresolved exposure.
5. DEX-first is the production control. `concurrent_hedged` remains behind the
   coordinator boundary and cannot become the default without the predeclared
   randomized switchback experiment.

## DEX execution decisions

1. The admitted DEX plan is immutable: route, token direction, exact input,
   minimum output, deadline, operation ID, and pool generation are journaled.
2. Allowances are prepared and locked before live entry. The immediate path
   must not add approval writes.
3. The nonce owner journals intent, signed transaction, broadcast, and receipt.
   Ambiguous broadcast or receipt outcomes are `UNKNOWN`, never a known failure.
4. Actual execution amounts come only from the canonical receipt's token
   transfers. The receipt status decides success or revert.
5. Fresh RPC gas price plus the configured priority fee is used at signing.
   There is no admission-time DEX fee cap.
6. The receipt's positional pool Swap is sufficient to apply this process's
   self-impact immediately to the local mirror. WebSocket logs remain the
   canonical ordering and reorg-correction stream. A second `eth_getLogs` copy
   must not be required before local self-impact is visible.

The last item is the target architecture. The 2026-07-23 production evidence
shows that the current HTTP proof path still defers most receipt settlements;
this is tracked as priority debt below.

## Binance hedge and recovery decisions

1. The primary Binance leg is derived from the actual DEX token-B delta, not
   only the planned quantity.
2. Quantity is rounded conservatively to Binance filters. Commission is part of
   the signed venue delta.
3. The primary hedge is a deterministic LIMIT IOC.
4. After the DEX receipt, the coordinator must observe the latest Binance top
   before placing the IOC. The admission-time price remains an immutable
   economic reference, not necessarily the only executable price.
5. Any repricing must stay inside the admitted recovery/exposure envelope. It
   cannot create a larger unreserved liability.
6. Partial or zero primary fills are measured from the terminal Binance order.
   Recovery acts only on the exact actionable residual.
7. DEX-first recovery may only reduce the existing exposure. A recovery that
   flips direction halts that parent but does not halt unrelated plans.
8. MARKET recovery is allowed for the bounded residual because exposure already
   exists. It uses a deterministic client ID and durable journal.

Post-receipt IOC repricing is a target decision. The current implementation
still places the admission-time price; changing it requires tests and measured
comparison, but does not require revisiting DEX-first ordering.

## Unknown outcomes and dead-end policy

An unresolved plan must never close the global execution lane permanently.

- `UNKNOWN` holds only that plan's exact reservation and journal identity.
- Later plans may execute when their own required inventory is available.
- The same child mutation is never retried under a new identity.
- Venue reconciliation may replace an unknown child result only with proven
  terminal data.
- A known DEX revert with zero token movement is a terminal loss equal to gas,
  not an unknown exposure.
- `Halted` and `UnknownExposure` are observable parent states, not global
  runtime phases.

For an otherwise ready runtime, insufficient *available* inventory after
reservations is the only completed-plan consequence that may prevent a later
independent transaction. Market-data, credential, signer, nonce, entry-stop,
and runtime-readiness failures remain legitimate global safety gates.

## Accounting and settlement decisions

1. A balanced result has no actionable token-B residual. Sub-step dust remains
   visible and receives a conservative token-A mark.
2. Comparable PnL includes actual DEX/CEX deltas, Binance commissions, recovery,
   and DEX gas exactly once.
3. Favorable DEX output outside the immutable hedge envelope remains inventory
   and must be visible in subsequent wallet snapshots and rebalance accounting.
4. A balanced reservation becomes `PendingSettlement`; it is released only
   after every claimed venue publishes a strictly newer balance generation.
5. Pool settlement is pool-scoped. It may invalidate a pending plan derived
   from the old generation, but it must not keep the global mutation lane busy.
6. ClickHouse receives results and state transitions asynchronously. It is never
   read to decide entry, execution, recovery, restart, or settlement.

## Rebalancing decisions

- Rebalancing is proactive inventory maintenance, not a global trading phase.
- It uses the same in-memory inventory owner and EVM nonce owner as trading.
- A rebalance reserves only its exact source amount.
- Trading may continue during a transfer when its own exact claims remain
  available.
- One rebalance operation is active at a time. Completion requires venue
  evidence and fresh balance settlement.
- Direct World Chain routes are preferred where supported; reviewed
  Optimism/Across routes are fallback.

## Durability and restart

Every external mutation has a deterministic operation/client ID and is
journaled before dispatch. Restart behavior is:

1. hydrate venue and chain state;
2. reconcile active child journals by their original identities;
3. resume only commands whose dispatch ownership was already persisted;
4. never create a replacement order or transaction until the previous outcome
   is proven;
5. open new entries only after runtime readiness and ownership are restored.

## Telemetry and comparison contract

Every plan must be traceable by `plan_id` through:

- opportunity and admission;
- pending supersession or preflight rejection;
- parent and child execution stages;
- DEX/CEX/recovery results;
- inventory state;
- DEX and balance settlement;
- terminal comparable PnL.

Production comparisons use equal half-open UTC windows. Rails rows must be
joined to trade status so zero-PnL failed attempts are not mislabeled as
profitable. Report at minimum:

- admitted, mailbox-received, preflight-rejected, balanced, and unknown counts;
- DEX fill/failure;
- primary IOC sufficient/partial/zero-fill;
- recovery count and PnL;
- comparable total/average PnL and token-B residual;
- settlement rejection count and p50/p95;
- source revision and production engine identity.

The current frozen baseline is
[`rust-rails-comparison-2026-07-23.md`](rust-rails-comparison-2026-07-23.md).

## Priority architectural debt

1. **Receipt self-impact:** apply the receipt Swap directly, then reconcile
   canonical WebSocket ordering/reorgs. The 24-hour baseline deferred 254 of
   265 filled-plan catch-ups and rejected 2,820 candidate admission attempts
   during settlement.
2. **Post-receipt hedge decision:** record latest top at receipt and placement,
   then use bounded current-top IOC repricing inside the admission envelope.
3. **Execution cohort quality:** explain why only 95 of 317 executed plans had
   positive expected primary economics although 4,360 of 6,872 admissions did.
   Preserve latest-only semantics; improve selection and stability evidence.
4. **Unknown root causes:** the eight unknowns did not block later work, but
   their exact DEX/Binance transport causes and held reservations must be
   reconciled and monitored.

## Change-review checklist

Every trading-path change must answer:

- Does it preserve one owner for mutable execution state?
- Does it add network, filesystem, Postgres, or ClickHouse work to the hot path?
- Does it change the 20 bps gate, sizing, ranking, price bound, or recovery
  envelope? If yes, is the versioned artifact updated?
- Can an unknown outcome duplicate a child mutation or block unrelated work?
- Are exact inventory and gas claims reserved before dispatch?
- Are actual receipt/order deltas used instead of planned amounts?
- Is the latest Binance state checked at the last responsible moment?
- Can pool settlement use receipt self-impact without waiting for another
  provider copy?
- Are journals restart-safe and client IDs deterministic?
- Are PnL, residual, recovery, and settlement telemetry still comparable?
- Does `scripts/quality.sh` pass?
- If production behavior changes, is delivery through the reviewed GKE workflow
  on `main` and is the equal-window observation plan explicit?

## Supporting documents

- [`trading-runbook.md`](trading-runbook.md): operator stop, recovery, rollout,
  and rollback.
- [`gke-deployment.md`](gke-deployment.md): production infrastructure and
  delivery.
- [`adaptive-arbitrage-sizing.md`](adaptive-arbitrage-sizing.md): sizing and
  exact reservation details.
- [`low-latency-dex-quoting.md`](low-latency-dex-quoting.md): local CLMM mirror.
- [`uniswap-execution.md`](uniswap-execution.md): DEX transaction and receipt
  mechanics.
- [`binance-order-execution.md`](binance-order-execution.md): bounded Binance
  order execution.
- [`rebalancing.md`](rebalancing.md): capital movement state machines.
- [`arbitrage-results.md`](arbitrage-results.md): result schema and comparison
  queries.
- [`concurrent-execution.md`](concurrent-execution.md): proposed controlled
  experiment; not the production default.

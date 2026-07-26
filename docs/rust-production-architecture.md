# Rust production trading architecture

Status: **authoritative**
Last reviewed: 2026-07-26
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
                                         bounded recovery
                              │                    │
                              └─────────┬──────────┘
                                        v
                                terminal result/PnL
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

The current JSON Spot `bookTicker` payload does not supply an exchange event or
transaction timestamp. Direct exchange-to-socket latency is unavailable for
that frame and must never be invented from ClickHouse arrival time. The
existing JSON depth stream supplies exchange event time `E`; production records
a clock-corrected depth exchange-to-socket estimate, its clock/timestamp
uncertainty, frame size, and parse-plus-apply time. This remains asynchronous
diagnostic telemetry and must not become a strategy, readiness, admission, or
execution input. The estimate is valid only for a successful clock observation
no older than 180 seconds; stale observations preserve raw inputs but produce a
null estimate. Local receipt-to-decision latency remains independently
measurable.

The current telemetry and liveness contract is
[`binance-price-telemetry.md`](binance-price-telemetry.md).

### Other market-data decisions

1. Update IDs and reconnect generations must be monotonic.
2. Sequence-consistent full depth is used for adaptive sizing when healthy.
   Top-of-book remains an explicitly capped production fallback.
3. DEX quotes come from the local CLMM mirror. RPC Quoter calls are validation
   and replay tools, not hot-path dependencies.
4. A DEX log updates the mirror in canonical order and creates a new prepared
   pool generation. The builder proactively prepares only the executable
   envelope: the configured 200 USDC trade cap is covered by the reviewed
   220 USDC unhedged-notional bound, from which direction-specific token-B
   limits are derived using the updated pool. It does not traverse liquidity
   that no admissible plan can use. Prepared curves are immutable once
   published; only the latest requested generation may be installed.
5. New entries require fresh Binance strategy-price transport, DEX mirror/head,
   execution ownership, balance generations no older than 10 seconds, and
   sufficient free inventory after exact reservations. Binance User Data,
   open orders, locked balances, the native-token conversion feed, and
   full-depth health remain observable but do not change
   `RuntimePhase::Ready` in DEX-first production. A successful five-second REST
   account reconciliation clears the diagnostic User Data anomaly flag;
   connection status remains separately observable.
6. The Binance market-data limit is 30 seconds of transport silence. A
   disconnect, generation change, or heartbeat timeout closes readiness and
   preflight. The time since the last top change does not.
7. DEX mirror liveness uses the receipt time of the latest canonical World
   Chain head with the same 30-second boundary. Every valid head refreshes
   liveness even when it contains no event for a configured pool and therefore
   does not change a DEX price or prepared generation. Pool-price age is not a
   runtime-readiness input.
8. The native-token conversion stream is not a market-data safety boundary.
   Disconnect, transport silence, and quote age never degrade the runtime or
   invalidate an opportunity or adaptive-sizing task. One initial conversion
   observation hydrates gas-cost accounting; the last observed conversion is
   then retained across reconnects. The current World Chain RPC fee remains a
   transaction-construction input, and the native wallet balance still has to
   cover the exact gas reservation; neither changes the Rails-compatible 20 bps
   profitability decision.

### Step 3: data readiness compared with Rails

Rails performs readiness checks inside each `DetectArbitrageJob`; it does not
maintain a process-wide market-data phase. Rust keeps the equivalent live inputs
in memory and exposes one `RuntimePhase`, but that phase is intentionally limited
to inputs whose absence means that the current venue state is unknown.

| Input or boundary | Rails | Rust production decision |
| --- | --- | --- |
| Binance top | Requires a snapshot and rejects it when its `created_at` is more than 30 seconds old. An unchanged event-driven top can therefore age out. | Requires a price from the current connection generation and transport activity within 30 seconds. Ping/Pong or another valid frame keeps an unchanged top current; the age of the last price change is telemetry only. |
| DEX state | Requires available DEX quote snapshots for both directions and applies the 30-second snapshot-age limit before opportunity calculation. Execution later requests an executable quote again. | Requires a hydrated local CLMM mirror and a canonical World Chain head observed within 30 seconds. Every accepted head refreshes liveness even when no configured pool changed. Prepared curves are per-pool calculation inputs, not a global readiness gate. |
| Balance observations | The selected wallet token and native-balance observations use a 10-second maximum age. The legacy Binance balance cache has a 10-minute TTL and no equivalent observation-age check in `DetectArbitrageJob`. | Both the Binance account and World Chain ERC-20 wallet generations must exist and be no older than 10 seconds. Binance REST reconciliation runs every five seconds; canonical heads trigger block-pinned wallet token refreshes. Native balance and RPC gas price are not part of that snapshot. Missing or stale token generations degrade the runtime. |
| Balance sufficiency | Applies the legacy `3x` token multiplier and may reject a candidate when the wallet execution lane is busy. | Is not a global readiness decision. Each candidate uses `free - active exact reservations` and reserves only its exact primary token debits. Native gas is an operator-maintained invariant and is not synchronized or reserved. Insufficient available token balance rejects that candidate only. |
| Binance User Data and account hygiene | Has no User Data Stream readiness boundary in the arbitrage path. | User Data accelerates balance/order observations but is not a readiness gate. Disconnects, unknown events, foreign or open orders, and nonzero locked balances remain diagnostic. A successful REST account snapshot reconciles state and clears the anomaly flag; only `free` balance is spendable. |
| Gas-price data | Has no independent gas-price freshness gate for opportunity detection. | Has no gas-price, native-balance, or native-token-conversion readiness/admission gate. The current RPC fee is obtained only when constructing the EIP-1559 transaction. Realized receipt accounting includes both `gasUsed * effectiveGasPrice` and World Chain `l1Fee`. |
| Full Binance depth | Does not require a local sequence-consistent depth book. | Depth can support adaptive sizing, but its health does not change DEX-first runtime readiness. The reviewed top-only fallback remains independently capped. |
| Execution lane | A busy wallet lane can reject the detected attempt. | Lane availability is scheduling, not data readiness. A candidate enters the latest-only mailbox while the lane is busy; a newer candidate supersedes it, and the newest pending candidate is considered as soon as the lane is released. |

The resulting Rust global readiness predicate is therefore exactly:

```text
fresh Binance strategy-price transport
AND fresh canonical DEX mirror/head
AND fresh Binance and World Chain balance generations
```

Startup configuration, process ownership, credentials, commissions, exchange
filters, journals, and nonce reconciliation must still succeed before live mode
can start. They are initialization and execution-authorization invariants, not
recurring market-data freshness inputs.

When a pending candidate reaches the execution lane, entry preflight does not
repeat all readiness and admission checks. It always verifies the two 30-second
market-data freshness boundaries because the candidate may have waited behind a
previous execution or restart recovery. It reuses the persisted admission
20 bps proof when the relevant Binance price and DEX pool generation are
unchanged; a changed relevant Binance price or DEX generation triggers a
current local DEX requote and another 20 bps comparison. The entry stop and the
candidate's exact reservation remain separate authorization controls.

## Opportunity and admission decisions

1. Evaluate both supported directions with fixed-point arithmetic:
   buy token B on DEX/sell on CEX, and sell token B on DEX/buy on CEX.
2. The configured 20 bps gross venue spread remains the Rails-compatible
   opportunity gate.
3. Uniswap pool fees are already included in the exact local CLMM quote.
   Execution slippage changes only `amount_out_minimum`; it never increases
   `amount_in`, changes gross economics, or introduces a provider fee reserve.
4. Native gas funding is an operator-maintained invariant. Native balance and
   gas price are not balance-sync, admission, or reservation inputs. Neither
   gas converted to USDC nor recovery profitability is an opportunity
   threshold: the configured 20 bps gross venue spread is the only
   profitability gate.
5. Baseline sizing preserves the reviewed 20 USDC detector. Adaptive sizing
   may select only the largest exact Binance-step-aligned DEX-curve amount
   within the versioned 200 USDC cap. Binance top quantity, full depth,
   recovery forecasts, gas economics, and inventory are not sizing inputs.
6. Candidate selection maximizes the valid exact-input notional after the
   baseline direction clears 20 bps. There is no expected-profit ranking or
   theoretical capacity pass.
7. Admission atomically reserves only the exact DEX input and Binance primary
   debit. Native gas, hypothetical recovery, and the legacy Rails `3x`
   multiplier are forbidden reservation inputs.

## Scheduling and preflight decisions

1. The execution queue is latest-only, never FIFO. A newer pending plan replaces
   an older unsubmitted plan and releases the old reservation.
2. Production retains one global mutation lane. Parallel market calculation and
   pool preparation are allowed; overlapping live mutations require a separate
   reviewed execution-mode experiment.
3. When the lane becomes available, market preflight has exactly two decisions:
   - Binance transport and the latest canonical World Chain head must both be
     inside the same reviewed 30-second freshness boundaries as runtime
     readiness;
   - the immutable DEX input is requoted against the latest local CLMM state and
     combined with the latest Binance bid/ask; the resulting gross venue spread
     must still meet the configured 20 bps threshold.
   When the relevant Binance price and published DEX generation are unchanged
   since admission, the persisted 20 bps proof is reused and the duplicate
   requote is skipped. Generation identity is an optimization signal, not a
   rejection condition.
   A Binance update ID, connection-generation identity, top-level quantity,
   DEX generation identity, and transaction deadline are not independent
   market-preflight gates. Connection state matters only as evidence that the
   latest Binance price is fresh. The operator entry stop and exact inventory
   reservation remain separate execution-authorization controls.
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
5. The dedicated execution owner refreshes RPC gas price every second. Signing
   uses the at-most-two-second cached RPC or Rails-fallback sample plus the
   configured priority fee. There is no admission-time DEX fee cap and no gas
   RPC in the live execution hot path.
6. The receipt's positional pool Swap is sufficient to apply this process's
   self-impact immediately to the local mirror. Already-queued DEX WebSocket
   events are drained without waiting first, then the affected prepared curves
   are rebuilt before the execution lane is released. A second `eth_getLogs`
   copy and a post-trade settlement barrier are forbidden.
7. Pending work is not discarded merely because its admitted pool generation
   predates the receipt update. Entry preflight requotes it against the latest
   published generation and rejects it only if the fresh venue prices no longer
   clear the configured 20 bps gross threshold or the price feeds are outside
   their 30-second freshness boundaries.
8. WebSocket logs remain the continuous source of subsequent external pool
   updates. The receipt event's canonical position deduplicates its later
   WebSocket copy without delaying owner-loop processing.

## Binance hedge and recovery decisions

1. The primary Binance leg is derived from the actual DEX token-B delta, not
   only the planned quantity.
2. Quantity is rounded conservatively to Binance filters. Commission is part of
   the signed venue delta.
3. The primary hedge is a deterministic LIMIT IOC.
4. The admission-time price is the immutable protection boundary. After the
   DEX receipt, a fresh current in-memory top may improve the IOC price in one
   direction only:
   - SELL uses `max(admission bid, current bid)`;
   - BUY uses `min(admission ask, current ask)`.
   An absent, stale, or adverse current top keeps the admission price. It never
   weakens the limit and never blocks closure of an already-created exposure.
5. The selected price is journaled before CEX dispatch so restart replay uses
   the identical order intent and deterministic client ID.
6. Repricing changes price only. It cannot increase the exact hedge quantity
   or create a larger unreserved liability.
7. Partial or zero primary fills are measured from the terminal Binance order.
   The coordinator freezes one MARKET recovery target equal to
   `primary hedge target - primary executed quantity`.
8. A proven zero-fill/unsubmitted/rejected child may retry that same immutable
   target at most three total attempts. Backoff deadlines of 250 ms then 500 ms
   are persisted before waiting; child IDs are deterministic `r1`–`r3`.
   Before the first MARKET attempt, Rust rounds down to the live market step
   and validates quantity bounds and `MIN_NOTIONAL` at the fresh same-side
   price. A local filter rejection is non-retryable and leaves the remaining
   exposure as marked inventory drift.
9. Partial/full fills and Unknown outcomes never advance to another attempt.
   Recovery results never recalculate a residual target. Exhaustion finishes
   the parent; any remaining WLD delta is result and inventory telemetry.
10. Every primary and recovery order emits joinable
    `arbitrage_binance_order` telemetry keyed by `plan_id` and
    `client_order_id`:
    - `primary_price_selection` records admission price, fresh in-memory top,
      selected price, and whether the price was improved;
    - `planned` records the exact LIMIT/MARKET request, target and submitted
      quantity, the in-memory bid/ask immediately before placement, and whether
      the LIMIT was marketable at that top;
    - `terminal` records exchange transaction time, status and
      zero/partial/full class, executed and quote quantities, average execution
      price, every fill, commissions, and unknown-outcome reconciliation;
    - `error` records unsubmitted, locally filtered, rejected, or unresolved
      outcomes together with the bounded validation/exchange reason.
    For the MARKET fallback, `planned` additionally records a hypothetical
    same-side LIMIT at the fresh in-memory bid/ask and whether visible top
    quantity covered the order. `terminal` records MARKET price advantage
    versus that limit, whether the average and all reported fills respected the
    limit, placement-to-terminal duration, the terminal memory top, and an
    explicitly diagnostic success proxy. The proxy is not evidence of queue
    position or guaranteed LIMIT execution.

The primary IOC is still attempted when the current market is worse than the
admission boundary. A partial or zero fill proceeds to bounded MARKET
fallback; adverse repricing is never used to make the LIMIT more
aggressive.

MARKET recovery remains the production control until this telemetry has a
representative sample. A future reviewed experiment may replace the two-step
LIMIT-then-MARKET path with one order priced from the best fresh in-memory top.
That decision must compare IOC full/partial/zero-fill rates, the
`limit_marketable_at_memory_top` cohort, LIMIT-to-MARKET latency, and MARKET
average execution price versus the placement-time top. Continuous
`binance_book_ticker` telemetry provides the intervening top evolution. The
joinable `arbitrage_execution_stage` events provide microsecond worker queue,
WebSocket placement, and total execution durations. The
experiment must not be inferred from a single order. It must separately compare
top-covered and non-top-covered recovery cohorts and unknown-reconciliation
cohorts.

## Unknown outcomes and dead-end policy

An unresolved plan must never close the global execution lane permanently.

- `UNKNOWN` holds only that plan's exact reservation and journal identity.
- Later plans may execute when their own required inventory is available.
- The same child mutation is never blindly retried under a new identity.
- Binance reconciliation queries the same deterministic client ID once. A
  terminal order contributes its actual fills. `-2013 NO_SUCH_ORDER` proves
  that the child is absent and immediately allows ordinary recovery; a
  timeout/5xx/transport/protocol failure of the status query remains Unknown
  and cannot authorize a new mutation.
- Venue reconciliation may replace an unknown child result only with terminal
  data or the bounded Binance proof that the order is absent.
- After restart, an `UnknownExposure` parent reconstructs the exact same CEX
  command. The Binance journal treats it as a status lookup, never as a new
  placement, and the parent then resumes from the reconciled result.
- A known DEX revert with zero token movement is a terminal loss equal to gas,
  not an unknown exposure.
- `Halted` and `UnknownExposure` are observable parent states, not global
  runtime phases.

For an otherwise ready runtime, insufficient *available* inventory after
reservations is the only completed-plan consequence that may prevent a later
independent transaction. Market-data, credential, signer, nonce, entry-stop,
and runtime-readiness failures remain legitimate global safety gates.

## Accounting and settlement decisions

1. `balanced_profit` and `balanced_loss` are legacy terminal stage names. A
   terminal result may retain a token-B inventory delta of any size. The signed
   delta remains visible and receives a conservative token-A mark, but it is
   accounting only and never triggers another order.
2. Comparable PnL includes actual DEX/CEX deltas, Binance commissions, recovery,
   and DEX gas exactly once.
   A BNB-discount commission is retained as an exact negative BNB delta and
   valued with the fresh accounting-only BNBUSDT bid, matching Rails'
   USDT/USDC parity convention. The BNB feed cannot enter readiness, admission,
   sizing, preflight, or recovery. Missing valuation marks PnL incomplete but
   never changes a known Binance fill into Unknown exposure.
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
- two-phase DEX revert evidence: immediate receipt facts and an asynchronous
  trace/replay diagnosis joined by `plan_id`, `operation_id`, and transaction
  hash;
- inventory state;
- DEX and balance settlement;
- terminal comparable PnL.

Production comparisons use equal half-open UTC windows. Rails rows must be
joined to trade status so zero-PnL failed attempts are not mislabeled as
profitable. Rust results must have a matching `arbitrage_admitted` event inside
the same window; journal reconciliation completed after restart is retained as
accounting telemetry, tagged with `resumed_after_restart`, and attributed by
its persisted `opportunity_received_unix_us`, not treated as a new admission.
Report at minimum:

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

1. **DEX revert cohort:** accumulate decoded trace/replay classifications by
   protocol, pool, direction, amount bounds, and revision before changing
   slippage or calldata policy. Missing diagnostics are counted explicitly and
   never treated as a different execution outcome.
2. **Execution cohort quality:** explain why only 95 of 317 executed plans had
   positive expected primary economics although 4,360 of 6,872 admissions did.
   Preserve latest-only semantics; improve selection and stability evidence.
3. **Unknown root causes:** the eight unknowns did not block later work, but
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

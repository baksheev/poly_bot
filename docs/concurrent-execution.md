# Concurrent DEX/CEX execution

Status: proposed; paper implementation required before any live use
Last reviewed: 2026-07-16

## Decision

Add `concurrent_hedged` as a separate execution mode. It dispatches the prepared
DEX transaction and the initial Binance Spot order concurrently, then reconciles
actual results and immediately flattens any unmatched exposure. The existing
DEX-first behavior remains a control mode and fallback until production evidence
shows that concurrent execution has better net expected value.

Concurrent execution is not atomic. It deliberately exchanges the price drift
between sequential legs for a higher probability of an orphaned or partially
filled leg. It may be enabled only when the service can prove current balances,
nonce ownership, Binance order state, full Spot depth, recovery liquidity, and
all risk limits entirely from in-memory state.

The first implementation is paper-only. It must not attach a wallet signer or a
Binance key with trading permission, and it must not weaken the existing live
trading gate.

## Goals

- Minimize the dispatch skew between DEX and CEX legs.
- Bound the size and duration of every unmatched position.
- Recover deterministically from rejects, reverts, partial fills, timeouts,
  disconnects, late acknowledgements, and process restarts.
- Never interpret transport failure as proof that an order or transaction was
  rejected.
- Measure realized recovery losses separately from the expected arbitrage
  profit.
- Keep network I/O, signing, allocation, ClickHouse, and logging outside the
  latency-sensitive opportunity calculation.

## Non-goals

- Atomic settlement across Binance and World Chain; no such primitive exists.
- Guaranteed zero-loss recovery. `MARKET` recovery explicitly accepts a bounded
  loss in exchange for removing directional exposure.
- Transferring assets between venues in the critical path. Both venues must be
  prefunded for both configured directions.
- Using ClickHouse or Postgres as an execution coordinator or recovery source of
  truth.
- Enabling live trading as part of the first implementation.

## Directions and inventory

The coordinator supports both existing opportunity directions:

| Direction | DEX leg | CEX leg | Required prefunding |
|---|---|---|---|
| `buy_token_b_on_dex_sell_on_cex` | spend token A, receive token B | sell token B | token A on DEX and token B on Binance |
| `buy_token_b_on_cex_sell_on_dex` | spend token B, receive token A | buy token B | token B on DEX and token A on Binance |

Inventory is reserved before dispatch and released only after the coordinator
reaches a balanced terminal state. Two plans must never reserve the same funds,
wallet nonce, or Binance order namespace.

## Preconditions

A plan may enter `prepared` only when all of the following are true:

- the Binance Spot book is sequence-complete, fresh, and deep enough for both
  the primary order and its emergency reverse;
- the authenticated Binance session and user-data event stream are healthy;
- current Binance balances, open orders, commissions, filters, and order-rate
  limits are hydrated;
- every required DEX pool is coherent at the current accepted World Chain head;
- wallet balances, pending nonce, reserved nonces, gas estimate, and fee policy
  are current;
- no older plan has `unknown` exposure or an unresolved nonce/order status;
- primary execution and worst-case recovery both fit their configured loss,
  inventory, exposure, and freshness limits;
- the process is in paper mode, or a separate explicit live gate is enabled.

Any failed precondition suppresses the new entry. Recovery of an existing plan
always has priority over creating another one.

## Executable size and admission

The proposed size is the minimum of:

```text
profitable DEX capacity
CEX primary-side Spot depth within the planned price bound
CEX reverse-side Spot depth within max_recovery_loss
available DEX inventory after reservations
available CEX inventory after reservations
per-pair and global risk limits
```

Admission uses net economics, not the visible spread:

```text
expected_net_profit =
    expected_primary_proceeds
  - expected_primary_cost
  - Binance fee
  - DEX fee
  - gas
  - configured reserves

expected_value =
    P(both succeed) * expected_net_profit
  - P(DEX orphan) * expected_CEX_reverse_loss
  - P(CEX orphan) * expected_CEX_market_hedge_loss
  - expected_failure_gas_and_fees
```

Probability estimates are learned from paper and canary telemetry by direction,
pair, size bucket, and latency regime. Until enough observations exist, admission
uses conservative configured upper bounds rather than optimistic probabilities.

## Immutable execution plan

The opportunity engine produces an immutable `ExecutionPlan` containing at
least:

- globally unique `plan_id`, source revision, pair, direction, and creation
  timestamps;
- exact base-unit quantity and all Binance tick/step rounding;
- DEX pool identity, router/calldata, exact-input or exact-output bounds, gas
  limit, fee policy, wallet, chain ID, and reserved nonce;
- initial CEX `LIMIT IOC` side, quantity, limit price, expected fills, fee, and
  deterministic client order ID;
- break-even reverse `LIMIT IOC` and emergency `MARKET` templates for either
  possible residual direction;
- expected profit, maximum recovery loss, maximum unhedged notional, and all
  deadlines;
- fingerprints of the Binance book, DEX generation, balances, fees, and risk
  snapshot used to admit the plan.

The DEX transaction and Binance request should be encoded and signed before the
dispatch barrier. The coordinator may not silently change their quantity or
economics after admission; a material change creates a new plan.

## Initial dispatch

The initial Binance order is an aggressive `LIMIT IOC` rather than a resting
`GTC` order:

- immediately available liquidity fills at or better than the plan's price;
- an unfilled remainder expires instead of remaining in the book;
- a partial fill is accepted and reconciled explicitly;
- the price bound prevents the initial CEX leg from becoming an unbounded market
  order.

After all reservations and encodings succeed, the single coordinator releases
the DEX and CEX send futures from one dispatch barrier. Each adapter records a
monotonic timestamp immediately before its first socket write and immediately
after the write completes. `dispatch_skew_us` is the absolute difference between
the two first-write timestamps.

Dispatch completion does not mean venue acceptance. The coordinator continues
from venue events and reconciliation results, not from the return value of the
socket write.

## Venue state machines

### DEX leg

```text
prepared
  -> dispatching
  -> pending(tx_hash)
  -> confirmed(receipt, actual_amounts)
  -> reverted(receipt)
  -> replaced(replacement_hash)
  -> cancelled(replacement_receipt)
  -> unknown
```

`confirmed`, `reverted`, and `cancelled` require a receipt on the accepted
canonical chain. A socket error, RPC timeout, missing receipt, or expired local
deadline is not a terminal failure. The already-signed transaction hash is known
before submission and remains the reconciliation key.

If inclusion is late, policy may resubmit the identical raw transaction or
replace it with the same nonce and a higher fee. If policy chooses cancellation,
the cancellation replacement must be confirmed before the original DEX leg is
treated as failed. Otherwise the original transaction can still land after the
CEX leg has been reversed and recreate exposure.

### CEX leg

```text
prepared
  -> dispatching
  -> acknowledged
  -> partially_filled(actual_qty, actual_quote)
  -> filled(actual_qty, actual_quote)
  -> expired(actual_qty, actual_quote)
  -> rejected
  -> cancelled(actual_qty, actual_quote)
  -> unknown
```

The authenticated Binance event stream is the primary low-latency source of
fills. Missing events are reconciled by deterministic client order ID. A request
timeout, disconnect, HTTP 5xx, or missing acknowledgement is `unknown`, not
`rejected`: Binance documents that a timed-out request may still have reached
the matching engine.

No retry may use a new client order ID until the previous ID has a known final
state. All recovery orders have their own deterministic IDs derived from
`plan_id`, leg role, and attempt number.

## Coordinator states

```text
prepared
  -> dispatched
  -> reconciling
     -> balanced_profit
     -> recovering_dex_orphan
     -> recovering_cex_orphan
     -> recovering_residual
     -> balanced_loss
     -> unknown_exposure
     -> halted
```

`balanced_profit` and `balanced_loss` are the only successful terminal states.
They mean the absolute token-B exposure is zero or below an explicitly accepted
dust threshold, not merely that both adapters returned success.

`unknown_exposure` immediately blocks all new entries. `halted` additionally
requires operator acknowledgement after reconciliation or a configured hard
risk limit has been breached.

## Exposure accounting

The coordinator derives exposure only from actual venue results:

```text
net_token_b =
    signed DEX token-B transfer
  + signed initial CEX filled quantity
  + signed recovery CEX filled quantities
```

Expected quote amounts, requested quantities, acknowledgements, and local book
depth are not fills. DEX amounts come from the canonical receipt/logs. CEX
amounts come from execution events and reconciled order status. Every transition
recomputes `net_token_b`; recovery always targets only this residual.

## Recovery policies

### DEX terminal failure, CEX filled

1. Compute the exact CEX-filled token-B residual.
2. Submit the opposite `LIMIT IOC` at the initial fill VWAP, rounded in the
   conservative direction to Binance tick size.
3. Reconcile its actual fill.
4. Submit `MARKET` for the remaining residual.
5. If the market order remains rejected or unknown after reconciliation, enter
   `halted`; do not continue opening risk.

The break-even limit is one immediate attempt, not a resting GTC order. Waiting
for a passive fill leaves the service directionally exposed for an unbounded
period.

### CEX terminal failure, DEX confirmed

1. Derive actual token-B exposure from the DEX receipt.
2. Confirm that the initial CEX order is terminal and account for any partial
   fill.
3. Submit a CEX `MARKET` order for the remaining residual.
4. Reconcile actual fills and repeat only for a proven residual, never for the
   original requested quantity.
5. Halt if the residual cannot be removed inside the loss or time limit.

### Both legs succeed with different quantities

Compute the signed residual and submit one CEX `MARKET` recovery in the required
direction. This covers CEX partial fills, DEX exact-input output variance, fee
rounding, and accepted dust policy.

### DEX pending or unknown

Do not reverse a filled CEX leg merely because the local DEX deadline expired.
First reconcile the known transaction hash and nonce, then either accelerate the
same transaction or confirm a same-nonce cancellation. If neither outcome can be
proved, enter `unknown_exposure` and block new entries.

### CEX unknown

Do not send a replacement `MARKET` order blindly. Continue reading execution
events and query by the original client order ID. A duplicate hedge can double
the position if the initial order filled despite a transport timeout.

### Both legs unknown

Stop new entries, reconcile both venue identities, and continuously recompute
the worst possible exposure. Operator notification is immediate. The process
must not restart into an apparently clean state.

## Deadlines and loss limits

All values are configuration with conservative defaults established by paper
measurements:

- `max_dispatch_skew_us`;
- `cex_ack_deadline_ms` and `cex_reconcile_deadline_ms`;
- `dex_inclusion_deadline_ms` and `dex_reconcile_deadline_ms`;
- `max_unhedged_time_ms`;
- `max_unhedged_notional_token_a`;
- `max_recovery_loss_token_a` per plan;
- rolling recovery-loss limits per pair and globally;
- maximum consecutive recovery events and unknown outcomes.

A deadline selects a reconciliation or recovery action; it does not rewrite an
unknown venue state into a failure. Breaching a loss, exposure, or unknown-state
limit opens the circuit breaker.

## Crash and restart recovery

ClickHouse is asynchronous telemetry and is never the source of execution truth.
After any process restart, new live entries remain disabled until the service
has reconciled:

- Binance balances, open orders, recent trades, and every bot client-order-ID
  namespace that can still be active;
- wallet balances, canonical pending nonce, reserved nonce range, known
  transaction hashes, and recent receipts;
- any net token exposure implied by those venue states.

The venues and deterministic identifiers are authoritative. A minimal local
append-only journal may reduce recovery time, but corruption or absence of that
journal must not cause the bot to assume that no orders exist.

## Adapter boundary

The coordinator is a pure state machine driven by typed commands and venue
events. Network adapters own persistent connections and translate between venue
protocols and those types.

```text
Opportunity engine -> immutable ExecutionPlan -> coordinator
                                              -> DEX adapter
                                              -> Binance adapter

DEX receipts/events --------------------------> coordinator
Binance execution events ---------------------> coordinator
coordinator commands --------------------------> adapters
```

The Binance adapter should start with the authenticated WebSocket API and user
data events over persistent connections. REST is reserved for startup and gap
reconciliation. FIX/SBE can be evaluated later against measured WebSocket
latency. The DEX adapter reuses process-scoped Alchemy connections, pre-encodes
and signs transactions, and never performs quote RPCs during execution.

## Telemetry

Every plan emits typed records through the bounded background telemetry path:

- plan admitted/rejected and every binding constraint;
- reservation, signing, dispatch, first-byte, acknowledgement, inclusion, and
  confirmation timestamps;
- DEX transaction hash/nonce and non-secret Binance client order IDs;
- requested, filled, residual, and recovered quantities in base units;
- dispatch skew and time spent unhedged;
- expected versus realized prices, fees, gas, profit, and recovery loss;
- every venue and coordinator state transition with its reason;
- circuit-breaker activation and operator recovery requirements.

Secrets, raw signed transactions, signatures, API headers, and credential-bearing
URLs must never be logged.

Core operational distributions are:

- dispatch skew p50/p95/p99/max;
- DEX submit-to-inclusion and CEX submit-to-first-fill;
- probability of both-success, each orphan direction, partial fill, and unknown
  status;
- unhedged duration and notional;
- recovery loss by cause and size bucket;
- concurrent realized net profit compared with a replayed DEX-first control.

## Paper implementation and tests

The first implementation contains no live adapters. A deterministic simulator
drives the coordinator through at least:

1. both legs fully succeed in either arrival order;
2. DEX reverts before and after a full or partial CEX fill;
3. CEX rejects or expires before and after DEX confirmation;
4. partial fills on the initial and recovery IOC orders;
5. initial CEX timeout followed by a late fill event;
6. DEX submission timeout followed by a late receipt;
7. DEX fee replacement and same-nonce cancellation races;
8. mismatch caused by DEX exact-input output and exchange rounding;
9. market recovery rejection, partial fill, and unknown result;
10. process restart with open CEX orders, pending nonce, and unknown exposure;
11. telemetry backpressure or outage during every state;
12. duplicate and out-of-order venue events.

Property tests must prove:

- duplicate events and retries cannot create duplicate economic orders;
- recovery quantity never exceeds the proven residual;
- no state marked balanced retains exposure above the dust threshold;
- unknown venue status never becomes a silent success or failure;
- every risk-limit breach blocks new entries;
- ClickHouse failure never blocks dispatch or recovery.

## Rollout

1. Implement the pure coordinator, deterministic identifiers, reservations, and
   fault-injection tests.
2. Feed real shadow opportunities into simulated venue adapters using measured
   DEX/CEX latency and failure distributions.
3. Add read-only Binance account hydration and validate restart reconciliation.
4. Add paper adapters that construct exact requests and transactions but cannot
   submit them.
5. Test supported non-production environments where venue behavior is
   representative.
6. Provision a separate wallet and Binance account, tiny balances, hard loss
   limits, and an explicit live-canary gate.
7. Compare `concurrent_hedged` with DEX-first on realized profit, orphan rate,
   unhedged duration, and recovery loss before increasing size.

## References

- [Binance Spot REST API general information](https://developers.binance.com/en/docs/products/spot/rest-api)
  documents unknown execution status after timeouts and the need to reconcile
  order state.
- [Binance Developer Documentation](https://developers.binance.com/en/docs/introduction)
  describes persistent WebSocket, FIX, and SBE interfaces for trading systems.


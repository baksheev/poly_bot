# Concurrent DEX/CEX execution

Status: proposed experiment; DEX-first remains the production control
Last reviewed: 2026-07-23

This proposal is subordinate to `rust-production-architecture.md`. In
particular, an unresolved parent may retain its own exposure and reservations
but must not permanently close the global execution lane.

## Decision

Add `concurrent_hedged` as a separate execution mode. It dispatches the prepared
DEX transaction and the initial Binance Spot order concurrently, then reconciles
actual results and immediately flattens any unmatched exposure. The existing
DEX-first behavior remains a control mode and fallback until production evidence
shows that concurrent execution has better net expected value.

Both modes must be implemented behind the same Rust coordinator boundary:

- `dex_first` is the control and reproduces the current production ordering:
  complete the DEX leg, submit a Binance limit hedge for the actual received
  amount, then use the existing market fallback and reconciliation behavior;
- `concurrent_hedged` is the treatment described by this document.

The independently running Rails bot is a useful operational benchmark, but it
is not the statistical control. Different code, infrastructure, accounts,
inventory, and observation windows would confound the comparison. Confirmatory
results come from randomized assignment between the two Rust modes under one
versioned experiment protocol.

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

`unknown_exposure` retains that parent's exact reservations and continues venue
reconciliation. `halted` stops that parent after a configured hard risk limit
has been breached. Neither state alone is a global runtime phase; later plans
remain admissible when their own exact inventory and normal readiness gates
pass.

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
same transaction or confirm a same-nonce cancellation. If neither outcome can
be proved, enter `unknown_exposure`, retain the exact claim, and continue
reconciliation without duplicating the mutation.

### CEX unknown

Do not send a replacement `MARKET` order blindly. Continue reading execution
events and query by the original client order ID. A duplicate hedge can double
the position if the initial order filled despite a transport timeout.

### Both legs unknown

Retain both exact claims, reconcile both venue identities, and continuously
recompute the worst possible exposure. Operator notification is immediate.
Later entries may use only inventory that remains available outside those
claims. The process must not restart into an apparently clean state.

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

## Comparative experiment

### Hypothesis

The confirmatory hypothesis is:

```text
H0: concurrent_hedged mean realized net PnL per assigned opportunity
    - dex_first mean realized net PnL per assigned opportunity
    <= superiority_margin

H1: the difference is greater than superiority_margin
```

`superiority_margin` is the minimum improvement that pays for the treatment's
additional orphan-leg and operational risk. It is denominated in token A per
assigned opportunity and fixed before the confirmatory run. A statistically
positive difference smaller than this margin is not sufficient to change the
production default.

The primary outcome is realized net PnL after all DEX and Binance fees, gas,
slippage, recovery trades, and failed-attempt costs. Mean PnL is primary because
it is economically additive. Median PnL and basis points per executed notional
are descriptive secondary metrics and cannot replace an unfavorable primary
result.

### Shared opportunity and isolation

Every candidate first becomes one immutable `ExperimentOpportunity` with:

- unique `opportunity_id` and `experiment_id`;
- identical market, pool-generation, fee, balance, risk, and source-revision
  fingerprints for both policies;
- one common direction, quantity, profit threshold, and maximum loss budget;
- both policy plans or an explicit reason why either policy could not construct
  a valid plan.

Both planners run for every opportunity. They may differ only where execution
ordering requires it; market inputs, math, fees, risk caps, and proposed size
remain common. Only the randomly assigned mode may send live commands. The
unassigned mode remains shadow-only, so the experiment never doubles capital or
lets both modes consume the same liquidity.

Only one plan may own the global execution lane at a time during the first
canary. The opportunity engine continues recording deduplicated shared
candidates while that lane is busy. A candidate suppressed by an active plan is
assigned to the currently active switchback arm with zero execution PnL and an
explicit `busy_suppressed` outcome. Otherwise a slower policy could hide its
throughput cost by making later opportunities disappear from the dataset.

An opportunity must be deduplicated before assignment. Repeated Binance ticks
showing the same still-open edge are not independent experiment units and must
not be randomized as separate chances to trade. The deduplication and cooldown
rules are versioned parts of the experiment protocol.

### Eligibility and intent to treat

The pre-assignment gate checks only a common observable market opportunity:
market/DEX freshness, common size, economics, and global hard risk configuration.
This produces `shared_market_eligible`. Global-lane availability, inventory
consumed by earlier trades, recovery state, and other policy-caused readiness
conditions are deliberately not part of this gate; their suppression is an
outcome of the assigned arm.

The experiment may be globally paused for a predeclared exogenous condition that
affects both modes, such as planned maintenance or loss of both venue feeds.
Policy-triggered unknown exposure, circuit breaking, inventory depletion, or
long recovery is never relabeled as an exogenous pause.

Every shared-market-eligible opportunity is assigned and included in the primary
intent-to-treat dataset. If the assigned policy subsequently declines, fails to
dispatch, hits a safety gate, or recovers at a loss, its actual outcome remains
in that arm. Dropping such rows would systematically hide operational failures.

A secondary `both_policy_eligible` analysis may isolate execution mechanics, but
it is not allowed to override the intent-to-treat result. Pre-assignment rejects
are retained for funnel diagnostics but do not enter the primary estimator.

### Random assignment

The first live canary uses a randomized switchback design on one wallet and
Binance account. A fixed-duration time block runs exactly one mode, and every
shared candidate in that block belongs to that arm. This preserves one global
execution lane while attributing busy time, recovery duration, and inventory
carryover to the policy that caused them.

The block sequence is deterministic, auditable, and balanced in randomized
`AB`/`BA` pairs:

```text
pair_order = hash(experiment_seed, experiment_id, block_pair_id) mod 2
0 -> [dex_first, concurrent_hedged]
1 -> [concurrent_hedged, dex_first]
```

The secret-free seed, hash algorithm, block duration, and schedule are committed
before enrollment. Assignment is immutable and recoverable after restart without
ClickHouse. Blocks are long relative to normal execution/recovery time but short
enough to distribute intraday market regimes across both modes; the duration is
selected from pilot autocorrelation and then frozen.

At a boundary, the current block stops opening entries and enters a bounded
washout. The next block begins only after all exposure, orders, and nonces are
reconciled. Washout time and delayed starts are attributed to the outgoing arm.
Common, mode-independent inventory targets and rebalancing rules minimize
carryover.

Opportunity rows remain nested observations. The independent randomization and
analysis unit is the switchback block, not an individual Binance tick. Direction,
fixed size, expected edge, and volatility regime are recorded as adjustment and
post-stratification fields rather than pretending that temporally adjacent rows
are independent assignments.

The first live experiment should use one fixed minimum size where possible. This
reduces variance and makes token-A PnL per candidate interpretable. Both modes
use the same account, fee tier, network path, balances, and runtime, removing
those as confounders.

If two genuinely isolated execution lanes are later provisioned, opportunity-
level randomization can increase power. Each lane then needs matched capital,
fee tier, latency, and risk limits plus a crossover schedule that periodically
swaps modes between wallets/accounts; otherwise lane differences are confounded
with policy.

### Shadow counterfactual

The unassigned planner records its plan and then consumes the captured market
stream in paper mode. This produces a paired counterfactual useful for debugging,
capacity analysis, and variance reduction.

It is not treated as a live outcome. The unassigned order did not enter either
venue, could not affect available liquidity, and did not experience actual
matching-engine priority. Only randomized live outcomes support the causal
production decision.

### Metrics

Primary metric:

- realized net token-A PnL per assigned opportunity, including zero dispatches,
  gas-only failures, and recovery losses after assignment.

Each switchback block contributes its total realized PnL and count of assigned
shared-market-eligible opportunities. The confirmatory estimator compares arms
at the randomized block level while weighting only according to the predeclared
analysis plan; it does not promote thousands of correlated ticks into thousands
of independent samples. Blocks with no shared opportunity contribute to uptime
and PnL-per-hour reporting but contain no per-opportunity outcome.

Secondary metrics:

- realized net PnL in basis points of assigned and executed notional;
- realized net PnL per experiment-active hour, including washout;
- profitable completion and primary-leg fill rates;
- DEX and CEX latency, dispatch skew, and opportunity-to-terminal duration;
- orphan, partial-fill, recovery, unknown-status, and circuit-breaker rates;
- unhedged duration and maximum unhedged token-A notional;
- recovery PnL, gas, Binance fee, DEX fee, and slippage by cause;
- opportunity throughput and rejection reasons.

Hard safety guardrails:

- unresolved exposure count must return to zero;
- no plan may exceed configured maximum loss, notional, or unhedged duration;
- treatment recovery/unknown rates must remain within predeclared
  non-inferiority margins versus control;
- daily recovery loss and drawdown remain below hard circuit-breaker limits;
- process restart and venue disconnect tests must reconcile before new entries.

The treatment cannot win on mean PnL while failing a hard guardrail. Tail losses
are reported without removing or winsorizing adverse outcomes from the primary
metric.

### Power and stopping rule

The experiment declares before enrollment:

- two-sided significance level `alpha = 0.05` for estimation and the equivalent
  one-sided superiority decision;
- target power of at least `80%`;
- economically meaningful superiority margin `delta`;
- fixed enrollment horizon or approved group-sequential checkpoints;
- clustering interval and all safety non-inferiority margins.

After a paper pilot estimates the standard deviation `sigma` of the block-level
primary outcome, the approximate starting number of independent blocks per arm
is:

```text
n ~= 2 * (z_(1-alpha/2) + z_power)^2 * sigma^2 / delta^2
```

This is increased for unequal block sizes, empty blocks, and the observed rate
of unusable opportunities. Because trading outcomes are autocorrelated and
heavy-tailed, the final confidence interval uses switchback randomization
inference or a predeclared block bootstrap. Individual opportunities and ticks
inside a block are never counted as independent sample-size units.

Safety metrics are monitored continuously and may stop the experiment at any
time. Efficacy is not repeatedly tested after every trade. A fixed-horizon test
is preferred initially; if interim efficacy checks are needed, their checkpoints
and alpha-spending boundaries are frozen in advance.

The treatment becomes the default only when all conditions hold:

1. the planned sample size and minimum runtime are complete;
2. the lower confidence bound for the primary PnL difference exceeds the
   predeclared superiority margin;
3. safety guardrails pass their hard limits and non-inferiority checks;
4. enough orphan and recovery observations exist to make the tail comparison
   meaningful;
5. results reproduce in a second holdout period or deliberately staged scale
   increase.

If the result is inconclusive, the control remains the default. The margin,
outcome definition, strata, and stopping rule may be changed only in a new
experiment ID.

### Experiment telemetry

Each shared candidate emits one experiment envelope containing:

- experiment/opportunity IDs, block and block-pair IDs, assignment arm,
  assignment probability, seed version, analysis strata, and eligibility result;
- both policy plan fingerprints, proposed economics, and rejection reasons;
- the assigned live plan ID and unassigned shadow plan ID;
- every actual execution/recovery event and realized primary outcome;
- active/washout timing and all metric inputs in integer base units.

ClickHouse receives this envelope asynchronously for analysis. Assignment and
live safety never depend on ClickHouse availability. A reproducible analysis
query or script must regenerate arm counts, exclusions, PnL, confidence
intervals, and guardrail results from the immutable envelopes.

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
12. duplicate and out-of-order venue events;
13. both planners receiving an identical opportunity fingerprint and only the
    assigned planner emitting commands;
14. deterministic `AB`/`BA` block assignment across restart, washout, and empty
    blocks;
15. attribution of busy-suppressed opportunities and delayed block transitions
    to the responsible arm.

Property tests must prove:

- duplicate events and retries cannot create duplicate economic orders;
- recovery quantity never exceeds the proven residual;
- no state marked balanced retains exposure above the dust threshold;
- unknown venue status never becomes a silent success or failure;
- every risk-limit breach blocks new entries;
- ClickHouse failure never blocks dispatch or recovery;
- no post-assignment reject or recovery loss can disappear from the
  intent-to-treat dataset;
- the analysis counts switchback blocks, not nested ticks, as independent units.

## Rollout

1. Implement both `dex_first` and `concurrent_hedged` behind the same pure
   coordinator interfaces, identifiers, reservations, and fault-injection
   harness.
2. Run both planners on every real shadow opportunity and feed them into
   simulated venue adapters using measured DEX/CEX latency and failure
   distributions.
3. Freeze the first experiment protocol, eligibility rules, superiority margin,
   pilot/confirmatory split, and analysis code.
4. Add read-only Binance account hydration and validate restart reconciliation.
5. Add paper adapters that construct exact requests and transactions but cannot
   submit them.
6. Test supported non-production environments where venue behavior is
   representative.
7. Provision one separate clone wallet and Binance account, tiny balances, hard
   loss limits, and an explicit randomized live-canary gate.
8. Complete the predeclared sample and holdout checks before changing the default
   mode or increasing size.

## References

- [Binance Spot REST API general information](https://developers.binance.com/en/docs/products/spot/rest-api)
  documents unknown execution status after timeouts and the need to reconcile
  order state.
- [Binance Developer Documentation](https://developers.binance.com/en/docs/introduction)
  describes persistent WebSocket, FIX, and SBE interfaces for trading systems.

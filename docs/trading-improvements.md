# Trading improvement roadmap

Status: M1 and M2 implemented for the next reviewed release; later milestones remain proposed
Last reviewed: 2026-08-04
Applies to: opportunity detection, candidate scheduling, DEX and Binance
execution, pair and route expansion, execution capacity, wallets, capital
allocation, and predictive scoring

This roadmap is subordinate to
[`rust-production-architecture.md`](rust-production-architecture.md). The
production architecture, versioned domain artifact, external-mutation review,
journal/recovery rules, and GKE delivery workflow remain authoritative. Each
milestone below that changes live behavior requires its own reviewed artifact
and production observation plan; checking this document in does not authorize
an external mutation.

## Decision

Optimize for time to a trustworthy production cohort rather than for a long
shadow observation period or a one-trade canary mechanism. The runtime
currently has limited turnover, so the preferred learning loop is:

```text
deterministic and fault-injection tests
  -> exact release-quality verification
  -> normal full_live production deployment
  -> continuous trading under existing risk and recovery controls
  -> production cohort report from existing telemetry
  -> keep, revise, or roll back in the next reviewed release
```

Shadow remains useful for proving that a planner constructs the expected
counterfactual and for collecting otherwise unavailable features. It is not a
mandatory long-duration economic gate when normal minimum-notional production
can produce the authoritative outcome. The roadmap does not add an automatic
entry stop, a one-parent release mode, a cohort controller, or another runtime
gate. The existing entry stop remains an operator and incident-recovery tool.

The first enabling change fixes the detector/control notional at `6.00 USDC`.
Current Binance rules specify a `5.00 USDC` minimum for WLDUSDC and ESPUSDC,
leaving a reviewed `1.00 USDC` startup-validated gap. The adaptive execution
cap remains 200 USDC and the configured 20 bps raw venue-spread gate remains
unchanged unless a later reviewed milestone explicitly changes either decision.

## Goals

- Reduce idea-to-production-data time without weakening deterministic recovery
  or external-mutation ownership.
- Discover opportunities at the smallest valid detector/control notional while
  retaining adaptive exact-curve sizing up to the reviewed cap.
- Add profitable DEX routes and pairs with one reusable runtime rather than a
  process, account, or wallet per strategy.
- Compare DEX-first and concurrent DEX/CEX execution using actual production
  cohorts.
- Improve hedge and recovery quality using joinable order telemetry.
- Add execution capacity, wallets, allocation, or predictive scoring only when
  measured bottlenecks justify their time-to-market cost.
- Judge improvements by additive comparable PnL and bounded downside, not by
  opportunity or trade count alone.

## Non-goals

- Replacing the production 20 bps gate with a second profitability model in the
  first milestones.
- Lowering the adaptive 200 USDC cap.
- Treating a DEX as having a guessed static minimum notional. DEX eligibility is
  proved by an exact local quote and valid calldata bounds.
- Enabling two production processes, Pods, GCE/GKE owners, or Rails/Rust owners
  for the same wallet, Binance account, nonce, order, or journal namespace.
- Adding network, RPC, database, ClickHouse, or remote-model inference to the
  decision hot path.
- Building multiwallet, parallel execution, or ML infrastructure before a
  measured constraint or data cohort exists.
- Changing recovery targets, retry authority, or Unknown-outcome semantics as
  an incidental part of another experiment.

## Priority model

The ordering is a time-to-market priority, not a claim about theoretical
maximum upside. A small, isolated change may rank above a larger project when
it can produce a trustworthy live verdict sooner.

| Rank | Milestone | Expected effect | Confidence | Delivery/risk cost | Priority |
| ---: | --- | --- | --- | --- | --- |
| 1 | Minimum detector/control notional | medium | high | low | P0 |
| 2 | Additional DEX pools/routes for existing pairs | high | medium | medium | P1 |
| 3 | Additional reviewed pairs | very high | medium | medium | P1 |
| 4 | Concurrent DEX/CEX execution | very high | medium | very high | P1 |
| 5 | Binance IOC/recovery execution | high | high | medium | P1 |
| 6 | DEX broadcast/inclusion latency | medium-high | medium | medium | P2 |
| 7 | Economic candidate scheduling | high when contended | high | medium | P2, trigger-based |
| 8 | Parallel independent execution lanes | high when contended | medium | high | P2, trigger-based |
| 9 | Dynamic portfolio allocation | medium when fragmented | medium | medium | P2, trigger-based |
| 10 | Multiwallet | high when wallet-bound | low-medium | very high | P3, trigger-based |
| 11 | Predictive/ML scoring | potentially high | low at current volume | high | P3, data-triggered |

The priority table is reviewed after every material production cohort. A later
rank change must cite the observed constraint; it must not be based only on the
amount of code already written for a milestone.

## Common outcome contract

Every live improvement reports the same primary outcome:

```text
comparable PnL per active production hour
and comparable PnL per admitted and terminal parent,
compared with the preceding equal-window production cohort
```

Comparable PnL includes actual DEX and Binance deltas, Binance commission, DEX
fees, gas, recovery, failed-attempt costs, and the terminal residual mark under
the existing [`arbitrage-results.md`](arbitrage-results.md) contract.

Secondary funnel and safety metrics are:

- selected, admitted, preflight-rejected, dispatched, and terminal parents;
- selected and executed notional distributions;
- DEX fill, known revert, Unknown, and diagnosis availability rates;
- primary IOC sufficient, partial, zero-fill, and Unknown rates;
- MARKET recovery count, duration, loss, and residual inventory;
- busy-suppressed candidates and execution-lane occupancy;
- journal/venue reconciliation coverage and restart-resumed operations;
- maximum unmatched exposure, drawdown, and circuit-breaker activation;
- hot-path latency and bounded-channel drops;
- source revision, image digest, domain fingerprint, pair, direction, and
  execution mode.

Time-to-market is also reported:

- reviewed revision to first terminal live outcome;
- deployment approval to first eligible opportunity;
- reviewed revision to a representative production cohort;
- number of operator interventions required during normal cohort collection.

An improvement is not accepted because it increases trade count, gross spread,
or fill rate while reducing comparable PnL or worsening a declared safety
boundary.

## M1: minimum detector/control notional

### Purpose

Replace the fixed 20 USDC baseline with a fixed `6.00 USDC` baseline. The
smaller baseline may discover an edge that a 20 USDC exact DEX quote already
crosses away. The adaptive worker still searches the same immutable prepared
curve for the largest valid Binance-step-aligned candidate up to 200 USDC.

On 2026-08-04 the public Binance `exchangeInfo` response reported a 5 USDC
`NOTIONAL.minNotional` for both `WLDUSDC` and `ESPUSDC`. The versioned artifact
owns the `6.00 USDC` value; live startup state owns validation of the exchange
minimum and quantity filters. A future filter change fails closed and requires
a reviewed artifact change rather than dynamically changing strategy size.

### Configuration and startup contract

Both production source artifacts carry:

```json
{
  "quote_sizing": { "token_a_base_units": "6000000" }
}
```

Requirements:

1. Domain and deployment validation require exactly `6_000_000` token-A base
   units for both production pairs.
2. Startup parses the value with checked fixed-point conversions; no `f64`
   enters strategy or execution math.
3. Live Binance rules must report a positive minimum notional no greater than
   `5.00 USDC`, preserving at least `1.00 USDC` configured headroom.
4. At an aligned deterministic validation price, the quantity rounded down to
   `LOT_SIZE.stepSize` must satisfy LOT_SIZE, MARKET_LOT_SIZE, and minimum
   notional filters without exceeding `6.00 USDC`.
5. Each direction remains eligible only when its exact local DEX quote exists,
   its calldata bounds are nonzero and valid, and the resulting Binance request
   can satisfy live price, quantity, and notional filters.
6. A DEX quote failure or zero result makes only that direction ineligible. It
   does not invent a larger DEX minimum.
7. The baseline remains fixed for the process epoch. A symbol-rule change that
   invalidates it closes new entries until a reviewed configuration release.
8. Adaptive sizing continues to select the largest exact candidate that clears
   the configured 20 bps spread and the 200 USDC cap.
9. A selected candidate that cannot grow beyond the minimum remains eligible
   under the current gross-spread architecture, but it is reported as a
   dedicated minimum-size realized-PnL cohort.
10. The change adds no network read, allocation, lock, dynamic policy
    resolution, or arithmetic to the per-tick decision and execution hot paths.

### Telemetry

Startup readiness exposes:

- configured token-A baseline;
- validation price and rounded validation quantity;
- effective validation notional;
- whether live quantity and minimum-notional filters pass.

Existing candidate telemetry continues to expose:

- direction-specific derived token-B amount;
- whether adaptive sizing remained at the baseline or grew the candidate;
- exact selected notional and eventual submitted notional;
- any below-minimum preflight or order-construction rejection.

These fields are non-blocking telemetry. ClickHouse is never read to validate
the baseline.

### Verification and rollout

1. Add boundary tests for exact `6.00 USDC`, the reviewed `5.00 USDC` floor,
   insufficient headroom, quantity rounding, and both production artifacts.
2. Prove at startup that the baseline can produce a filter-valid rounded
   quantity.
3. Run existing adaptive-sizing, reservation, Unknown, recovery, restart, and
   accounting suites unchanged except for versioned fixture values.
4. Compare release-mode `local_quote` and `replay-capacity` results before and
   after the configuration change. No new code may enter the hot path, prepared
   quote latency must stay within benchmark noise, decision p99 must remain
   below `25 us`, and replay throughput must not regress by more than 5%.
5. Deliver through the normal `main` and `Deploy GKE` workflow.
6. Start directly in permanent `full_live` DEX-first mode with the fixed
   `6.00 USDC` baseline and existing 200 USDC adaptive cap.
7. Accumulate a production cohort using existing joinable telemetry and the
   normal terminal accounting contract. Do not stop automatically after the
   first trade or add a cohort-specific runtime controller.
8. Compare minimum-size, adaptively grown, recovery, and failure cohorts before
   keeping or revising the policy in a later reviewed release.

Initial success means startup validates the fixed baseline, the deployment
remains healthy, no command violates live Binance filters, adaptive trades
preserve the 200 USDC cap, and production latency distributions do not worsen.
Economic success is decided from the production cohort, not from a one-trade
verdict.

## M2: additional DEX pools and routes

Implementation decision: add exactly the canonical World Chain Uniswap V3
WLD/USDC 1% pool selected in
[`m2-pool-route-selection-2026-08-04.md`](m2-pool-route-selection-2026-08-04.md)
through the v14 WLD artifact. No new ESP, V4, non-Uniswap, multi-hop, or split
route is part of this milestone.

### Purpose

Increase executable edge for an existing Binance instrument and funded asset
set before paying the operational cost of a new pair.

### Requirements

- Add only allowlisted protocol, router, factory/manager, pool, fee, and token
  identities through a versioned domain source.
- Hydrate each pool once per network and maintain its quote locally from ordered
  canonical events.
- Compare exact executable curves with deterministic tie-breaking; do not use a
  remote Quoter in evaluation or preflight.
- Preserve one transaction and one immutable selected route per parent.
- Keep split routing out of the first route-expansion milestone.
- Prove receipt event decoding, positional pool identity, allowance, calldata,
  revert diagnosis, self-impact application, and restart recovery for each new
  route type.
- Degrade only strategies whose minimum healthy pool dependency cannot be met.

### Rollout

Use read-only hydration and exact local quote parity as construction
verification, then enable the reviewed route directly in permanent `full_live`
mode. Do not require a long shadow window when calldata, receipt decoding, and
recovery are already proven deterministically for the same protocol
implementation. Keep, revise, or remove the route after its production cohort
is joinable and economically interpretable.

## M3: additional pairs

### Purpose

Monetize the implemented 10-20 pair runtime without creating a process, Binance
account runtime, user-data stream, wallet, or rebalancer per pair.

### Selection funnel

```text
public market and pool discovery
  -> exact local CEX/DEX economics
  -> symbol filters and commission identity
  -> pool and token identity validation
  -> wallet inventory and allowance plan
  -> deposit/withdrawal/rebalance route and recovery validation
  -> permanent minimum-notional full_live policy
  -> production cohort review
```

Read-only discovery may rank candidates, but no score authorizes execution.
Each selected pair requires an explicit domain entry, funding decision, external
side-effect matrix, deterministic order/transaction identities, and terminal
accounting coverage.

Enable pairs one at a time or in a small causally interpretable group. Pair
expansion must report whether the shared Binance balance, global dispatch lane,
rate limits, or network execution lane becomes the next bottleneck.

## M4: concurrent DEX/CEX execution

### Purpose

Reduce price drift between the two legs by running `concurrent_hedged` in the
reviewed production scope and comparing its accumulated cohort with the prior
`dex_first` production cohort at the same sizing policy.

[`concurrent-execution.md`](concurrent-execution.md) remains the authoritative
experiment design. Its 2026-08-08 ESP/USDC v1 protocol authorizes a real-money
randomized production switchback after the earlier paper-only implementation.
The production revision distinguishes:

- mandatory deterministic planner, request-construction, fault-injection, and
  restart verification;
- a bounded paper construction check that has no mutation credentials;
- optional developer diagnostics that are not a production economic gate;
- a versioned permanent `full_live` concurrent policy for the selected pair;
- before/after production cohort analysis and its known market-regime
  limitations.

The live treatment must retain the existing immutable mismatch target,
idempotent child IDs, one status query for Unknown Binance placement, bounded
MARKET recovery, exact reservations, and lane release rules. Exactly one
versioned mode sends commands for a pair; the rollout never runs both modes
against the same opportunity or liquidity.

The concurrent revision starts directly in normal `full_live` with existing
adaptive sizing and continues under the existing risk, quarantine, recovery,
entry-stop, and rollback controls. A deterministic 30-minute `AB`/`BA`
switchback assigns exactly one live mode per block. DEX-first remains the
post-window and revision rollback mode until the treatment has acceptable
comparable PnL and safety tails.

## M5: Binance IOC and recovery execution quality

### Purpose

Reduce primary IOC shortfalls and expensive MARKET recovery without changing
the immutable hedge target or allowing repeated exposure creation.

### Production change funnel

1. Use current joinable telemetry to classify top-covered and non-covered
   primaries, selected IOC price, marketability, primary fills, recovery fills,
   terminal top, commissions, and reconciliation.
2. Identify one mechanism to change: IOC price selection, marketable LIMIT
   construction, or a separately reviewed one-order policy.
3. Freeze the new request construction in the versioned production artifact.
4. Preserve the target quantity and risk envelope from the preceding control
   revision.
5. Deploy the change directly in `full_live` and compare equal production
   windows before deciding whether to keep or revise it.
6. Keep current MARKET recovery behavior until a representative production
   cohort justifies a separately reviewed replacement.

No experiment may retry a partial or full child, turn Unknown into absence, or
gross up MARKET BUY for a hypothetical base-asset commission.

## M6: DEX broadcast and inclusion latency

### Purpose

Reduce opportunity decay between admission and canonical inclusion without
adding another nonce owner or transaction intent.

### Requirements

- Measure signed-command acceptance, first broadcast, provider acknowledgement,
  mempool/sequencer observation where available, inclusion, and receipt times.
- Compare only reviewed providers reachable from the production region.
- Reuse one signed transaction, nonce, operation ID, calldata, and hash when
  broadcasting through more than one endpoint.
- Treat conflicting hash or transaction construction as a correctness failure.
- Never create a second transaction to race the first.
- Keep provider errors and telemetry outside readiness and strategy economics
  except where the authoritative execution owner cannot submit its existing
  command.

A reviewed change is deployed directly in `full_live` and retained only when
its production cohort improves inclusion or realized execution without
increasing Unknown transactions, nonce recovery, reverts, or provider coupling.

## Trigger-based scale milestones

The following projects remain specified but do not enter implementation merely
because they could be useful at higher volume.

### M7: economic candidate scheduling

Trigger: fresh candidates from different strategies regularly compete for the
single dispatch lane, or busy suppression creates material missed comparable
PnL.

The scheduler keeps at most one latest candidate per strategy and preserves a
starvation bound. A reviewed deterministic policy may rank comparable economic
value or stability. Hash-map order, task wake order, and telemetry completion
must never decide the winner. Scoring runs from already-owned immutable state
and cannot add I/O to the hot path.

### M8: parallel independent execution lanes

Trigger: lane occupancy or busy suppression remains material after candidate
selection improves, and operations can prove disjoint EVM mutation lanes plus
sufficient shared Binance inventory and rate-limit capacity.

Start with already distinct `(chain_id, wallet_id)` lanes before adding another
wallet. `PortfolioOwner` must atomically reserve shared Binance assets, and the
Binance order owner must reconcile multiple known parents without weakening
account-level Unknown handling. Each EVM lane retains exactly one nonce and
journal owner.

### M9: dynamic portfolio allocation

Trigger: missed opportunities are materially attributable to inventory being
in the wrong validated location rather than to strategy economics or lane
contention.

Allocation observes realized PnL, opportunity rate, missed inventory rejects,
turnover, transfer cost, transfer recovery, and location risk. It runs as
latest-only background work against an immutable snapshot and proposes at most
the reviewed number of durable capital operations. It never enters opportunity
evaluation.

### M10: multiwallet

Trigger: a wallet's nonce lane, risk isolation, or funded location is a measured
constraint after existing independent chain lanes are used.

Multiwallet requires:

- multiple configured `WalletId` values and explicit signer capabilities;
- inventory, allowance, gas, nonce, journal, and recovery state per
  `(chain_id, wallet_id)`;
- deterministic wallet selection with atomic portfolio reservation;
- globally unique operation IDs and lane-local transaction IDs;
- location-aware rebalancing and conservation checks;
- restart fixtures at every wallet-selection and child-acceptance boundary;
- proof that no other process, Rails owner, GCE VM, or Pod controls any selected
  wallet.

One wallet per pair is not the default topology. Wallets are execution and risk
isolation resources selected by the portfolio and lane owners.

### M11: predictive and ML scoring

Trigger: the live dataset contains representative point-in-time outcomes across
enough pairs, directions, sizes, latency regimes, DEX protocols, and execution
modes to maintain a holdout cohort.

Do not begin with one binary "bad trade" label. Model separately:

- known DEX revert probability;
- primary IOC zero/partial-fill probability;
- expected recovery loss;
- expected comparable PnL;
- Unknown or long-settlement probability.

The first comparison is an interpretable deterministic/logistic baseline. A
more complex model must beat that baseline on held-out time periods and remain
calibrated by pair and direction. Features must exist before the decision and
carry event/generation timestamps so no receipt, terminal top, or later market
state leaks into training.

Initial model comparison runs offline against point-in-time production records;
the runtime does not add a permanent shadow-inference path. If the offline
model beats the deterministic baseline, its first runtime use is a versioned
`full_live` ranking policy among already eligible candidates after M7 is
justified. Rejection/admission authority is a later, separately reviewed
architecture change. Any live inference is local, versioned, deterministic,
bounded, and uses validated integer/fixed-point inputs and outputs; it never
calls a remote model service.

## Recommended delivery sequence

### Wave A: minimum-notional production learning

1. Implement M1 minimum detector/control notional.
2. Deploy it directly in permanent DEX-first `full_live` mode.
3. Accumulate and review the dedicated minimum-size and adaptively-grown
   production cohorts.

### Wave B: market surface

4. Add one high-confidence DEX route under M2 directly in `full_live`.
5. Add one fully validated pair under M3 directly in `full_live`.
6. Record which resource, if any, becomes the measured bottleneck.

### Wave C: execution mechanics

7. Amend the authoritative protocol and implement M4 concurrent execution as a
   versioned permanent production mode.
8. Accumulate its production cohort before keeping or revising the mode.
9. Run at most one M5 Binance-order production change at a time.
10. Benchmark and, if justified, deploy one M6 broadcast change.

### Wave D: measured scaling

11. Implement M7-M10 only when their declared triggers are met.

### Wave E: predictive ranking

12. Build M11 only after the data trigger is met and the deterministic scheduler
    is an established baseline.

## Review checklist

Every milestone answers the canonical production checklist plus these roadmap
questions:

- What production cohort, window, and comparison will decide whether to keep or
  revise the change?
- Is any requested shadow work necessary for implementation correctness, or can
  existing tests and production telemetry answer the question sooner?
- Does the release change one economically interpretable behavior, or does its
  cohort need explicit attribution for several bundled changes?
- What immutable amount, price, route, execution mode, and policy generation is
  journaled before the first command?
- Which owner may perform each external mutation?
- Can any timeout or Unknown outcome authorize a duplicate order or transaction?
- Do the existing incident stop, recovery, and rollback controls remain
  sufficient without adding a cohort-specific runtime gate?
- Which venue evidence proves terminal reconciliation without reading
  ClickHouse in the execution path?
- What is the primary comparable-PnL outcome and the predeclared downside
  boundary?
- What observation would defer a scale project instead of expanding its scope?
- Does the versioned domain artifact and deployment workflow verify every
  changed startup field?
- Does `scripts/quality.sh` pass before handoff?

## References

- [`rust-production-architecture.md`](rust-production-architecture.md) —
  authoritative production invariants and change review.
- [`adaptive-arbitrage-sizing.md`](adaptive-arbitrage-sizing.md) — current 20
  USDC detector, exact adaptive sizing, and 200 USDC cap.
- [`multi-pair-multi-network-runtime.md`](multi-pair-multi-network-runtime.md) —
  pair, network, wallet-location, portfolio, and execution-lane ownership.
- [`concurrent-execution.md`](concurrent-execution.md) — DEX-first control and
  concurrent treatment experiment.
- [`binance-order-execution.md`](binance-order-execution.md) — symbol filters,
  IOC, MARKET recovery, and dust behavior.
- [`arbitrage-results.md`](arbitrage-results.md) — terminal comparable-PnL
  contract.
- [`trading-runbook.md`](trading-runbook.md) — entry stop, recovery, release,
  and rollback.
- [`rust-rails-comparison-2026-07-23.md`](rust-rails-comparison-2026-07-23.md) —
  frozen execution-funnel and recovery evidence.

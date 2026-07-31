# M13 pre-deploy review: permanent ESP/USDC full-live

Status: implementation reviewed locally and authoritative production cohort
verified for revision `06d913c9e6c05edbdd4df5fa0bb707fabbc1be72`.

The operator approved immediate permanent ESP/USDC trading and rebalancing.
V6 removes rollout-only cumulative/count/window stops after M9–M12 completed,
while retaining per-parent trading and per-operation capital safety envelopes.

## External mutation matrix

- [x] ESP/USDC arbitrage remains DEX-first on Arbitrum followed by the existing
  shared Binance hedge/recovery owner; no execution ordering or endpoint was
  changed.
- [x] Each parent remains capped by adaptive sizing at `200 USDC`, at most
  `220 USDC` unhedged, `2 USDC` recovery loss, and one concurrent parent.
- [x] Rebalance remains direct Binance/Arbitrum only. Each operation is capped
  at `2,600 USDC` or `10,000 ESP`, with fee authority capped at `5 USDC` or
  `2 ESP`, one concurrent operation, and one unknown-outcome query.
- [x] Historical cumulative transfer count, failure count, amount totals, and
  rollout time no longer close future authority; the caps are reapplied from
  zero independently to each new operation.
- [x] Bridge and Optimism mutations remain disabled. No additional wallet,
  Binance account, nonce lane, asset, token contract, or router is introduced.
- [x] The dedicated wallet grants the fixed reviewed SwapRouter02 max-uint256
  ERC-20 allowances once during fail-closed startup, matching the Rails V3
  executor. The running executor then permanently locks allowance mutation.

## Unknown-outcome and restart matrix

- [x] A durable active trade or rebalance saga is recovered before new work;
  full-live does not bypass the one-owner or one-concurrent-operation gates.
- [x] Historical V5 and older source artifacts remain checked in so their
  canary sessions and incident recovery records deserialize exactly.
- [x] Full-live capital operations retain stable strategy owner
  `rebalance-arbitrum-usdc-esp-m10` and use permanent approval session
  `esp-usdc-arbitrum-full-live`. Old cumulative risk is observable but cannot
  consume a new operation's debit or fee envelope.
- [x] An active capital operation still closes new authority even when old
  terminal counts, failures, and rollout timestamps are ignored.
- [x] Standard withdrawal is always attempted first; only exact synchronous
  Binance `-4104` routes the same durable request through Travel Rule.
  Empty-fee history remains valid only when Binance reports the gross debit
  directly, preserving the M12 recovery correction.
- [x] Unknown Binance and EVM outcomes retain their existing deterministic IDs,
  one-query reconciliation, nonce journals, and no-resubmission rules.

## Versioned artifact semantic diff

- [x] V6 is a new immutable source; V5 is not rewritten or deleted.
- [x] `full_live` and its typed policy must appear together, are restricted to
  ESPUSDC on chain `42161`, require the exact approved actor/timestamp, direct
  `ARBITRUM`, max-uint256-then-locked allowance mode, one reconciliation query,
  and no bridge.
- [x] The compiled live projection publishes `FullLive`; the public collector
  projection removes execution and full-live authority.
- [x] Arbitrum gas headroom remains explicit at `12,000 bps` and fails
  compilation if missing or inconsistent; there is no implicit default.
- [x] The deployment workflow asserts the exact V6 snapshot, trade envelope,
  allowance mode, rebalance per-operation caps, route, and absence of legacy
  `live_canary` before rollout.
- [x] The source-derived compiled bundle is regenerated deterministically and
  all-target compilation compares it with the checked-in artifact.

## Latency and resource observation plan

- [x] The revision adds no filesystem, RPC, Binance, Postgres, or ClickHouse
  work to the price/decision hot path. Full-live checks use in-memory readiness,
  journal risk, and fixed integer comparisons.
- [x] Startup may submit at most two idempotent allowance updates if the
  existing allowance is below max uint256; nonce recovery must complete before
  readiness and no allowance writes are possible afterward.
- [x] Production observation starts at container start and checks startup,
  allowance receipts/recovery, ESP readiness, arbitrage/rebalance sagas,
  unknown exposure, and absence of duplicate withdrawals or orders.
- [x] The same half-open window compares WLD parse, socket-to-decision, DEX
  receive/build/total, telemetry drops, errors, CPU, throttling, memory, OOM,
  and restarts against the accepted M12 cohort.
- [x] Rollback is the previous immutable digest/source V5 only after every
  active trade, Binance order, rebalance operation, and Arbitrum nonce is
  terminal; automatic rollback must refuse an unreconciled durable schema.

## Final diff review

- [x] Historical M8–M12 reports, SQL, tests, artifacts, and review documents
  remain present and executable.
- [x] Tests cover missing full-live fields, public projection scrubbing,
  explicit gas policy, permanent allowance mode, historical risk not consuming
  new authority, active-operation exclusion, and per-operation overflow
  rejection.
- [x] `scripts/predeploy-review docs/reviews/m13-predeploy.md origin/main`
  passes on the final diff.
- [x] `scripts/quality.sh` passes on the same final diff.
- [x] The final diff is clean, fast-forwardable from `origin/main`, and will be
  delivered as one reviewed push before a single exact-revision deployment.

## Production review result

- [x] CI `30628511615` and Deploy GKE `30628862643` succeeded for exact
  revision `06d913c9e6c05edbdd4df5fa0bb707fabbc1be72`; immutable image digest
  `sha256:f894135b29ecb708458e51ec09ac1c64ab43d931eaf97c8d288b0c097243e08f`
  ran as the sole GKE owner while GCE remained `TERMINATED`.
- [x] Both max-uint256 router approvals were mined before readiness, the
  running allowance policy was locked, and no duplicate withdrawal, order,
  transfer saga, or nonce mutation appeared.
- [x] In `[2026-07-31T12:06:07Z, 2026-07-31T12:21:30Z)`, M13 was `armed`:
  four successful rebalance evaluations, zero actions because inventory was
  already within the 25% threshold, `4,395` allocator audits with zero
  failures, and no active/failed transfer or limit breach.
- [x] WLD parse/socket p99 was `8/49 μs`; fee-500 receive/build/total p99 was
  `112/159/170 μs`, hot telemetry drops and production `ERROR` were zero,
  both containers stayed Ready with zero restarts, CPU max was `0.017536`
  core, memory peak was `60,354,560` bytes, and throttling/OOM counters were
  zero.
- [x] The production cohort exposed and corrected a reporting-only mismatch:
  M13 now joins the actual `rebalance_plan_evaluated` mutation planner and the
  `portfolio_capital_allocator_evaluated` audit stream. The correction changes
  no runtime authority and requires no application redeploy.

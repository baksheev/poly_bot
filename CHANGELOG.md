# Changelog

## 2026-07-22

- Replace the normal post-arbitrage WebSocket settlement wait with a
  receipt-proven, pool-scoped HTTP log catch-up. The engine applies canonical
  events through the fill block, rebuilds prepared curves, and releases the
  lane only after a newer generation is published; WebSocket delivery remains
  the fallback for incomplete or temporarily unavailable receipt proof.
- Removed the Rust-only admission-time and absolute gas-price caps from live
  arbitrage DEX execution. As in Rails, the signer now uses fresh
  `eth_gasPrice` plus the configured priority fee; admission retains its gas
  sample only for reservation economics and telemetry.

This file records operator-visible changes shipped by `poly_bot`. Source
revisions and immutable image digests in the GKE deployment history identify
the exact artifact used by each rollout.

## [Unreleased]

### Changed

- Start the versioned ESP/USDC real-money production switchback between
  `dex_first` and `concurrent_hedged`. Deterministic 30-minute SHA-256 `AB`/`BA`
  blocks use the existing adaptive 6-200 USDC sizing, reservations, journals,
  Unknown reconciliation, MARKET recovery, and entry-stop controls; WLD, ARB,
  and Linea retain DEX-first execution.
- Keep a DEX pool fail-closed instead of terminating the multi-strategy process
  when an on-chain liquidity update temporarily leaves its prepared execution
  envelope with zero capacity. A later Mint or Swap rebuilds and republishes
  the pool normally, while unrelated strategies and the ESP price collector
  remain running.
- Add the canonical Arbitrum Uniswap V3 ARB/USDC 0.05% pool to the immutable
  v2 production domain. The existing multi-pool local V3 curve, adaptive
  selection, calldata, receipt, recovery, and shared ARB inventory paths are
  reused unchanged alongside the 0.3% control pool.
- Add ARB/USDC on the canonical Arbitrum Uniswap V3 0.3% pool as the third
  permanent adaptive `full_live` strategy. The release uses one idempotent,
  quiesced 500 USDC Binance inventory bootstrap, then the existing direct
  Arbitrum rebalance owner splits ARB inventory across Binance and the wallet.
  ARB and ESP share one EVM signer/nonce lane, Binance account owner, trade
  coordinator, 20 bps entry gate, and 200 USDC per-trade cap.
- Add the canonical World Chain Uniswap V3 WLD/USDC 1% pool to the immutable
  v14 production domain. The existing local V3 curve, calldata, receipt,
  self-impact, recovery, and single-route selection paths are reused unchanged.
- Promote diagnostic pre-trade cost telemetry to v3: bind source selection to
  decision completion, retain bounded burst histories, learn receipt gas and
  L1 fees per exact pool/input-token route, preserve bootstrap provenance and
  receipt event time, capture candidates before inventory reservation with
  joinable rejection context, and sample auxiliary raw books at one Hz. The
  production 20 bps gate and all trading inputs remain unchanged.
- Make diagnostic pre-trade cost telemetry decision-grade without entering the
  trading path: disable invalid unauthenticated collector cohorts, retain two
  temporal source samples to avoid refresh look-ahead gaps, enforce a
  diagnostic native-conversion TTL, bootstrap World Chain L1-fee evidence from
  the durable receipt journal in a bounded background task, and emit exact
  `plan_id`/`update_id` candidate-to-result joins for estimator validation.
- Add one-Hz diagnostic-only pre-trade net-edge sampling to background
  arbitrage evaluations. It records conservative Binance commission, current
  cached gas fee cap, same-protocol realized gas/L1 inputs, source ages, and a
  hypothetical 5 bps net result without changing the production 20 bps gate or
  adding records to the bounded ClickHouse channel.
- Add the M5 account-wide portfolio owner. Inventory is now keyed by exact
  account or chain/wallet location plus reviewed venue asset ID, trade and
  rebalance claims share pre-aggregated atomic reservations, settlement
  barriers name their locations, and World/Arbitrum assets cannot collide.
  The conservation-checked allocator runs non-mutating in shadow while the
  live World Chain rebalance remains behind the exact v12 parity adapter.
- Classify a journaled DEX `status=0` receipt and a proven pre-submission
  rejection as expected terminal warnings while retaining `ERROR` for unknown
  DEX outcomes, accounting failures, and confirmation failures. Receipt
  telemetry, gas accounting, revert diagnostics, and trading semantics are
  unchanged.
- Add the M4 multi-strategy hot-path owner. The compiled domain now supplies
  exact symbol/pool dependencies for WLD and ESP; the primary process directly
  evaluates both strategy-price streams, keeps ESP read-only behind a
  non-mutating coordinator sink, shares immutable generation-tagged prepared
  curves with sizing snapshots, and gives each strategy one running plus one
  latest pending exhaustive-sizing slot. WLD continues through the existing
  compatibility coordinator and execution path. DEX events received while the
  remaining account startup runs are applied and prepared before readiness as
  a separately measured startup backlog, so they cannot enter the steady-state
  DEX receive-latency cohort.
- Introduce the compiled World Chain/Arbitrum `NetworkRuntime` registry with
  reusable clients, canonical block-hash-pinned bounded batches, independently
  backpressured read classes, network-scoped wallet hydration, and a generic
  execution-owner boundary. World Chain preserves the v12 gas policy and
  Arbitrum remains read-only.
- Remove the unusable ESP/USDC Arbitrum V4 2.88% pool from the production
  price shadow after it remained on one tick, could not quote one direction,
  and returned a one-way executable quote far below V3 and Binance. The new
  immutable v2 artifact collects only the viable V3 0.01% pool.
- Make the versioned domain artifact the single
  `strategy.max_transport_silence_ms` source for strategy-price runtime
  readiness, admission, and preflight. Rename the independent gas-conversion
  setting to `GAS_PRICE_MAX_TRANSPORT_SILENCE_MS`.
- Expire diagnostic Binance exchange-to-socket estimates after 180 seconds
  without a successful clock observation. Raw timestamps and clock evidence
  remain visible, while the estimate and uncertainty become null and explicitly
  invalid without affecting trading.
- Preserve Binance JSON depth event time and publish a clock-corrected
  exchange-to-socket estimate with synchronization RTT, age, timestamp
  resolution, and explicit uncertainty. The diagnostic remains asynchronous
  and has no effect on strategy, readiness, admission, or execution.
- Promote the immutable v12 adaptive-live artifact. Binance top-of-book
  admission, runtime readiness, and preflight now treat an unchanged
  event-driven price as current while its connection generation has transport
  activity within 30 seconds. Disconnects, generation changes, and transport
  silence remain fail-closed; v11 remains immutable quote-age provenance.
- Added a one-minute production rebalance health heartbeat plus Google Cloud
  Monitoring email alerts for explicit planner/executor faults, blocked or
  stuck operations, stuck settlement, and five minutes without a heartbeat.
  The GitHub deployment idempotently targets `baksheev@me.com`.
- Rebalancing is production-enabled on GKE for WLD and USDC. Direct WLD and
  Optimism/Across fallback routes have completed in both directions; USDC has
  completed in both directions through its only live Binance route, Optimism.
- The rebalance documentation now describes the deployed planner, route matrix,
  treasury boundary, exact four route state machines, journals, recovery,
  telemetry, production evidence, and current operator workflow.
- Production withdrawal mode is documented and deployed as Binance Travel Rule.

### Fixed

- Fuse the DEX-sell exact-output capacity pass with construction of its
  exact-input prepared curve. Full sparse-word boundary steps are reused
  directly and only the final partial step is recomputed, removing one complete
  106-segment traversal from the production fee-500 pool refresh.
- Interleave newly arrived canonical DEX events between coalesced prepared-pool
  builds. A multi-pool swap burst can no longer make later logs wait behind the
  sum of every inline curve build before the owner observes them.
- Prevent unknown or halted arbitrage operations from dead-ending the global
  execution lane. Their exact inventory reservations remain held and reduce
  available balance while independent plans continue; only a plan that is
  actively dispatching legs serializes the single execution owner. Plan and
  Binance client-order identities now include the DEX pool generation and a
  full opportunity fingerprint, so a fresh DEX candidate cannot collide with
  a completed plan that reused the same Binance update.
- Scope post-DEX settlement invalidation to pending candidates prepared from
  the affected pool generation. Receipt catch-up or a later pool update guards
  that pool, while the global execution lane and unrelated pools remain free.
- Keep market-data processing and opportunity evaluation ready while a
  rebalance is pending, executing, failed, or waiting for post-operation
  snapshots. Rebalance state now serializes only rebalance operations; stale
  market/balance inputs remain fail-closed, and future orders must use
  direction-specific inventory reservations.
- Replace the Rails-style completed-transfer TTL with a state-based settlement
  barrier: after the executor confirms the destination credit, another
  rebalance waits until both continuous balance streams advance.
- Preserve the second token budget after the first token rebalance completes.
- Treat Binance withdrawal history amount as net received and approve or bridge
  that net amount after the withdrawal fee.
- Accept current Across filled responses without legacy output fields while
  preserving origin, destination, transaction, and minimum-output validation.
- Use the singular network-scoped Binance deposit-address endpoint and reconcile
  exact credited amounts, including exchange precision residue.
- Preserve legacy executor journal checksums and approval recovery across rollout.

### Removed

- Retired the direct-WLD canary execution mode, canary amount and journal flags,
  forced WLD Across route flag, and the obsolete canary journal implementation.
- Removed one-off mutating CLI commands for MARKET round trips, gas purchases,
  manual wallet withdrawals, and native-ETH bootstrap bridging. Financial
  mutations now go through the recoverable executor.
- Removed the single-value Binance credential-mode flag; the separate master
  treasury identity is now an unconditional production invariant.

## [0.2.0] - 2026-07-16

### Added

- A zonal GKE Standard production topology in `asia-southeast1-b` with a
  dedicated `c4-highcpu-8` node pool, static CPU allocation, private nodes,
  Dataplane V2, and Cloud NAT reusing the allowlisted GCE static IP
  `34.21.220.162`.
- Immutable node-pool replacement: production has one fixed C4 node with
  Cluster Autoscaler disabled; a release explicitly creates one SHA-named
  replacement pool, waits for readiness, and then deletes the previous pool.
- A schedulable Guaranteed runtime budget of six exclusive CPUs and 10 GiB on
  C4-8, leaving capacity for required single-node GKE system Pods.
- GitHub Actions deployment after the main CI gate, authenticated to Google
  Cloud with OIDC Workload Identity Federation and protected by the GitHub
  `production` environment.
- Digest-pinned container deployment, Kubernetes rollout verification, and
  automatic restoration of the previous Deployment revision on failure.
- Direct Secret Manager CSI mounts through Workload Identity Federation for
  GKE. Runtime secrets do not pass through GitHub Actions or Kubernetes Secret
  objects.
- Kubernetes startup/readiness signaling after DEX, Binance, wallet, and
  initial balance hydration, plus graceful `SIGTERM` handling.
- Process-scoped paper rebalance tracking based on the v3 domain snapshot. It
  captures startup inventory, detects the configured 25% floor, targets a
  50/50 location split, closes readiness when action is required, and emits
  telemetry without transferring or signing anything.

### Changed

- The active World Chain `USDC-WLD` configuration is now
  `usdc-wld-world-chain.v3.json`.
- The package version is now `0.2.0`.
- Production deployment configuration moved from a mutable singleton GCE
  process toward a Kubernetes Deployment with immutable revisions. The old VM
  is stopped and retained as a rollback target while its former static IP is
  assigned to GKE Cloud NAT.
- GKE capacity is fixed rather than utilization-autoscaled. A failed release
  cannot trigger uncontrolled scale-up and leaves the previous one-node pool
  available for rollback.

### Security

- GKE worker nodes have no public IP addresses and accept no inbound workload
  traffic.
- The fixed release pool remains available to required GKE system Pods; the
  application uses an exact node-pool selector instead of a `NoSchedule` taint.
- The GitHub deploy identity is restricted to the main branch, the production
  namespace, image publication, and cluster discovery; it receives no runtime
  secrets.
- Live execution remains disabled. A future live rollout must add exclusive
  execution fencing before two overlapping Pods can share an account, wallet,
  or nonce space.

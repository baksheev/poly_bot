# Changelog

This file records operator-visible changes shipped by `poly_bot`. Source
revisions and immutable image digests in the GKE deployment history identify
the exact artifact used by each rollout.

## [Unreleased]

### Changed

- Rebalancing is production-enabled on GKE for WLD and USDC. Direct WLD and
  Optimism/Across fallback routes have completed in both directions; USDC has
  completed in both directions through its only live Binance route, Optimism.
- The rebalance documentation now describes the deployed planner, route matrix,
  treasury boundary, exact four route state machines, journals, recovery,
  telemetry, production evidence, and current operator workflow.
- Production withdrawal mode is documented and deployed as Binance Travel Rule.

### Fixed

- Keep market-data processing and opportunity evaluation ready while a
  rebalance is pending, executing, failed, or waiting for post-operation
  snapshots. Rebalance state now serializes only rebalance operations; stale
  market/balance inputs remain fail-closed, and future orders must use
  direction-specific inventory reservations.
- Match the Rails completed-transfer guard with a 10-second in-memory lock on
  the same rebalance token and direction, in addition to the existing single
  active operation and fresh Binance-plus-wallet snapshot barrier.
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

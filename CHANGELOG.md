# Changelog

This file records operator-visible changes shipped by `poly_bot`. Source
revisions and immutable image digests in the GKE deployment history identify
the exact artifact used by each rollout.

## [Unreleased]

### Added

- An off-by-default full rebalance executor implements direct and
  Optimism/Across routes in both directions for WLD and USDC. A checksummed,
  fsynced state-machine journal pins every operation, deterministic Binance
  withdrawal ID, exact Across calldata, confirmation, and final balance
  reconciliation; a bounded worker keeps the network-heavy flow outside the
  market-data loop.
- `full_live` requires an isolated subaccount trading credential and a separate
  master treasury credential, an exact operator acknowledgement, positive WLD
  and USDC caps, a wallet signer, dual-chain RPC hydration, and durable
  high-level plus nonce journals.
- Binance-to-wallet execution first performs a master-authorized universal
  transfer from the isolated subaccount with a deterministic `clientTranId`,
  reconciles that transfer, and only then submits the external withdrawal from
  the master account. Both steps survive restart without blind retries.
- The GKE template mounts the eleven secrets needed by the two-account flow,
  persists both executor journals, and obtains positive
  WLD/USDC limits from a reviewer-protected GitHub production environment.
  Deployment validation rejects absent or zero limits before authentication or
  rollout.
- The GKE journal PVC uses C4-compatible `dynamic-rwo` Hyperdisk Balanced
  storage instead of the unsupported `pd-balanced` class.
- Full rebalance withdrawal submission supports explicit `standard` and
  `travel_rule` Binance API modes. GKE selects `standard` after the isolated
  subaccount rejected the local-entity endpoint before any withdrawal.
- Full-live startup checks the subaccount key's read/IP flags, the master key's
  read/withdrawal/universal-transfer/IP flags, and verifies the master's view
  of the configured subaccount balances before opening either journal.

- Reusable EVM wallet primitives for canonical-block balance and allowance
  hydration, latest/pending nonce observation, native and ERC-20 transfer or
  approval calls, checked EIP-1559 signing, and hash-verified broadcast. The
  explicitly gated Across native-ETH canary and full rebalance executor use the
  shared wallet API; general execution remains off by default.
- A single-owner EVM nonce lane and checksummed, fsynced JSONL transaction
  journal preserve intent, signed hash, broadcast, unknown outcome, and mined
  state across restart. Rails wallet regressions for duplicate nonces,
  `already known`, receipt timeout, and premature reservation release are
  covered by Rust failure tests.
- Conservative startup reconciliation for unresolved EVM operations. Matching
  receipts close the journaled operation; pending transactions must match the
  full journaled identity and call, while absent, replaced, or unsigned cases
  keep the nonce lane blocked for review.
- A read-only Binance capital recovery snapshot hydrates an exact EVM deposit
  address plus optional Travel Rule deposit and withdrawal evidence. It uses
  decimal arithmetic, typed statuses, transaction-hash matching, and local
  deterministic `withdrawOrderId` matching without submitting mutations.
- A one-shot production direct-WLD rebalance canary reserves at most 1 WLD in a
  checksummed fsynced journal, submits one deterministic Binance withdrawal,
  recovers it through withdrawal history, and completes only after the World
  Chain wallet balance increases. GKE now uses a Recreate rollout and a zonal
  ReadWriteOnce journal disk so live-capable revisions never overlap.

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

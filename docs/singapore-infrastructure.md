# Singapore deployment and ClickHouse cutover

Status: read-only production worker deployed and verified
Last reviewed: 2026-07-16

## Decision

- Run the Rust worker in GCP `asia-southeast1` (Singapore).
- Use ClickHouse Cloud on GCP `asia-southeast1`.
- Do not deploy the trading worker in a US region because Binance is not
  available from US infrastructure.
- Keep ClickHouse outside the critical trading path. The process writes to it
  only through the bounded background telemetry channel.

The selected ClickHouse endpoint and password live only in ignored local env
files and GCP Secret Manager. Credential-bearing URLs and passwords must never
be committed or logged.

## Runtime layout

The trading runtime is VM `arb-bot-rust-shadow-gce` in
`asia-southeast1-b`. It uses `c4-highcpu-8`, Container-Optimized OS, a 20 GB
balanced Hyperdisk boot disk, and a digest-pinned container image. This gives
the process eight dedicated vCPUs and 16 GiB RAM without Cloud Run's shared
scheduler. It is still a normal VM, not a sole-tenant physical host.

The VM is attached to the isolated custom VPC `arb-bot-low-latency`, subnet
`arb-bot-singapore` (`10.42.0.0/24`). There are no ingress firewall rules. Its
Premium-tier static egress IP is `34.21.220.162`; use that address for future
exchange allowlists.

The attached runtime service account obtains short-lived metadata-server
tokens. The boot script reads ClickHouse and Alchemy values directly from
Secret Manager, writes a root-only environment file, authenticates to Artifact
Registry with an ephemeral Docker config, and removes that config after the
image pull. No long-lived service-account key or credential is stored in
instance metadata.

## Binance diagnostic VM

Manual authenticated Binance checks run from the separate
`arb-bot-binance-test` VM. It uses a small `e2-small` instance in the Singapore
subnet and its own Premium-tier static egress address `34.143.148.4`. The
production VM keeps its no-ingress policy unchanged. SSH to the diagnostic VM
is allowed only through Google IAP (`35.235.240.0/20`) and only to instances
carrying the `arb-bot-binance-test` network tag; direct internet SSH remains
blocked.

The diagnostic VM has a dedicated service account. It can read only the two
Binance secrets and the pinned container image; it has no wallet, Alchemy, or
ClickHouse access. Its root-owned wrapper permits only read-only Binance CLI
commands. No long-running trading process is installed.

Provision the VM from an already-published digest-pinned image:

```bash
scripts/create-gce-binance-test-vm IMAGE SOURCE_REVISION
```

After adding the printed static IP to the Binance API-key whitelist, run a
sanitized account and capital-route check through IAP:

```bash
scripts/gce-binance-test binance-account
scripts/gce-binance-test binance-capital
scripts/gce-binance-test binance-recent-validation-orders --limit 20
```

The local and remote wrappers both reject commands outside the read-only
allowlist. Do not extend that allowlist with order, withdrawal, wallet, or
bridge commands. Those operations retain their separate caps, explicit live
confirmation, and recovery requirements.

The validated diagnostic image is source revision `1c6eb17a6954`, pinned to
digest
`sha256:a2325f44b3907c782656dbc15198c3806a427197f5404a969ba4732e8d0fab22`.
To update an existing diagnostic VM, publish an image built from a committed
revision, resolve its immutable digest, and run:

```bash
scripts/update-gce-binance-test-image IMAGE@sha256:DIGEST SOURCE_REVISION
```

This updates only the diagnostic VM metadata, pulls the pinned image through
its dedicated service account, and reruns the startup setup over IAP. It does
not restart or mutate the production trading VM.

systemd owns the container and restarts it after a process failure. Docker uses
host networking and forwards `SIGINT` for graceful shutdown. The service has a
large file-descriptor limit, elevated scheduler priority, and a strongly
negative OOM score. Live trading remains disabled and no wallet, signing, or
Binance execution secrets are attached.

Provision a committed, already-published image with:

```bash
scripts/create-gce-worker IMAGE SOURCE_REVISION
```

The script refuses a dirty worktree or replacement of an existing VM. It
creates the isolated network, subnet, static address, IAM bindings, and VM when
missing. Replacing the production instance must be an explicit blue/green
operation; do not silently mutate it in place.

## Production checks

- Binance WebSocket connects from the actual VM egress and remains
  fresh across reconnects.
- Alchemy p50/p95/p99 latency is measured from the same VM before DEX
  quoting becomes a readiness dependency.
- ClickHouse slowdown or outage increments telemetry drop/failure metrics but
  does not increase market-event queue age or stop the engine.
- No trading secrets are attached while the service is read-only.

## Current production baseline

The pre-migration baseline was Worker Pool `arb-bot-rust-shadow`, revision
`arb-bot-rust-shadow-direct-dcfc5e0`, from source revision `dcfc5e056dae` and
image digest
`sha256:2b51afd185e012893d6904aa4ae5346d7774c494f1493a513197dd41f75d26cc`.

- Cloud Run reports the revision `Ready` with one manually scaled instance,
  8 vCPU, 16 GiB RAM, and CPU idle disabled.
- The process hydrated five configured Uniswap pools at World Chain block
  `32409580`, completed its race-free backfill, and
  established filtered Alchemy WebSocket subscriptions.
- The process connected to the Binance Spot raw stream
  `wss://stream.binance.com:9443/ws/wldusdc@bookTicker`; both the market-data
  and execution products in the active domain snapshot are `spot`.
- Before the cache, the fixed production window from `2026-07-15 20:49:18 UTC`
  through `20:52:00 UTC` contained 288 evaluations. In-memory opportunity
  calculation latency was 453 us p50, 560 us p95, 911 us p99, and 1,715 us
  maximum.
- After the cache, the fixed window from `2026-07-15 21:16:50 UTC` through
  `21:21:30 UTC` contained 666 evaluations. Overall calculation latency was
  12 us p50, 25 us p95, 630 us p99, and 1,106 us maximum: 37.8x, 22.4x, and
  1.45x faster at p50, p95, and p99 respectively.
- Of those evaluations, 628 were fully warm: 11 us p50, 19 us p95, 51 us p99,
  and 94 us maximum. Compared with the pre-cache distribution, the warm path
  is 41.2x, 29.5x, and 17.9x faster at p50, p95, and p99.
- The remaining 38 evaluations followed an applied DEX event and recomputed at
  least one invalidated entry. They measured 25 us p50, 792 us p95, 1,106 us
  p99/max. Across all 6,550 cache lookups, 6,382 hit and 168 missed (97.4% hit
  rate). These visible, state-driven recomputations explain the overall p99;
  capacity-search quotes remain deliberately uncached.
- This calculation timer measures the complete two-direction, five-pool
  evaluation including conditional capacity search, but excludes network
  latency and telemetry insertion.
- The prepared-curve revision was measured over the fixed window from
  `2026-07-15 22:08:26 UTC` through `22:13:00 UTC`. Its 667 Binance-triggered
  evaluations measured 3 us p50, 7 us p95, 13 us p99, and 97 us maximum for
  the complete two-direction, five-pool calculation. Compared with the
  pre-cache baseline this is 151x, 80x, and 70x faster at p50, p95, and p99.
- Of those evaluations, 664 were fully warm at 3/7/10 us p50/p95/p99. Three
  state-driven rebuild evaluations measured 20/77/77 us. Across 6,670
  baseline lookups, 6,648 hit and 22 missed (99.67% hit rate).
- Eighteen production curve builds measured 155/229/229 us p50/p95/p99 for
  construction and 314/682/682 us from request to publication. Decisions fail
  closed during this bounded interval and reevaluate the latest Spot book as
  soon as the matching generation is published.
- End-to-end frame-receipt-to-decision latency was 51/87/201 us p50/p95/p99.
  Seventeen events (2.55%) exceeded 100 us, six exceeded 250 us, and one
  exceeded 1 ms. The 3,899 us maximum had a 3,911 us engine queue age while
  its calculation took 5 us; the other slow rows show the same correlation.
  The remaining tail is therefore before opportunity calculation, in the
  Binance-task to state-owner wakeup and Cloud Run scheduling path.
- Raw Binance and opportunity JSON formatting now runs behind separate bounded
  telemetry channels and is not included in the decision timer. Calculation
  meets the 25 us p99 contract. The remaining tail in this intermediate
  revision motivated removal of the Binance task/channel handoff.
- The direct-read revision polls the Binance WebSocket from the same Tokio task
  that owns strategy state. Its revision-tagged fixed window from
  `2026-07-15 22:33:21.184 UTC` through `22:35:45 UTC` contained 460 unique
  Binance updates with no duplicate `update_id` values. Complete calculation
  latency was 2/6/18 us p50/p95/p99 and 41 us maximum; frame receipt through
  completed decision was 5/15/50 us and 92 us maximum. No event exceeded
  100 us.
- The 457 fully warm direct-path rows measured 2/6/9 us calculation and
  5/15/45 us decision p50/p95/p99. Three state-driven rebuild rows measured
  20/41/41 us calculation and 21/92/92 us decision. Across 4,590 baseline
  lookups, 4,568 hit and 22 missed (99.52% hit rate).
- Nine curve builds in the direct-path window measured 157/256/256 us
  construction and 279/444/444 us request-to-publication p50/p95/p99. The
  fail-closed refresh interval remains below one millisecond in this sample.
- Compared with the immediately preceding prepared-curve revision, removing
  the Binance channel improved decision latency by 10.2x at p50, 5.8x at p95,
  and 4.0x at p99. Both production latency contracts passed on the 8-vCPU
  Cloud Run Worker Pool. Dedicated compute was selected anyway because the
  execution phase needs explicit CPU placement, process priority, host
  networking, and a stable exchange-allowlist IP.
- No Worker Pool warning or error logs appeared during its startup check.
- No wallet, signing, or Binance trading credentials are attached.

## Compute Engine cutover

The dedicated VM runs the same `dcfc5e056dae` binary and immutable image digest
as the final Cloud Run baseline. Its telemetry identity is
`arb-bot-rust-shadow-gce-dcfc5e056dae`, so the two runtimes were compared over
the same external market window without mixing rows.

The fixed comparison window from `2026-07-15 22:46:30 UTC` through
`22:50:20 UTC` contained 531 unique Binance Spot updates on each runtime with
no duplicate update IDs. The 526 fully warm rows produced:

- Compute Engine: calculation 3/6/6 us p50/p95/p99, 9 us maximum; complete
  decision 9/17/18 us, 53 us maximum.
- Cloud Run: calculation 2/8/17 us p50/p95/p99, 73 us maximum; complete
  decision 7/18/39 us, 124 us maximum.

The VM therefore kept median decision latency within 2 us while improving
decision p99 by 2.2x and the observed maximum by 2.3x. No VM decision exceeded
100 us. Both runtimes also saw five state-driven rebuild rows; VM rebuild
calculation was 14/25/25 us p50/p95/p99 versus 15/26/26 us on Cloud Run.

After verifying current Binance, World Chain, DEX preparation, and ClickHouse
telemetry on the VM, the stateless Cloud Run Worker Pool was deleted on
2026-07-16. The digest-pinned image and deployment script remain available for
rollback, but only the GCE runtime is active.

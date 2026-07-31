# M11 pre-deploy review: maximum-pair capacity

Status: complete; production verified on 2026-07-31.

M11 was reviewed as one consolidated change. No implementation commit was
pushed to `main` until the complete local gate below passed. The first
deployment contained the complete target-node replay and reporting path; it
was not used to discover ordinary compile, fixture, routing, restart, or limit
errors.

## Scope and authority

`config/capacity/m11-maximum-pair-replay.v1.json` is an immutable,
strictly read-only capacity fixture:

- exactly 20 unique Binance symbols and 23 synthetic pool dependencies;
- 100,000 strategy frames per pair and four disconnect/connect bursts;
- 115 captured block-pinned pool decode/build/publication samples across
  startup plus four rehydration cycles; partial batches are rejected;
- at most four simultaneous sizing workers, with one running and one pending
  snapshot per strategy;
- `network_io_enabled=false`;
- `external_mutation_authorized=false`.

The candidate IDs come from Rails monitoring run 4, captured on 2026-07-05.
That export has empty score and transfer-capability fields. It is useful only
for maximum-count/network-distribution shape and is explicitly ineligible for
live strategy selection. M11 does not add these symbols to the production
domain bundle, subscribe to them, provision inventory, authorize orders, or
authorize EVM transactions.

The replay constructs the real compiled dependency index and
`HotPathDecisionOwner`. Its structural primary evaluator cannot produce a
candidate. Every evaluator is otherwise identical and non-mutating. Exact
symbol routing therefore exercises the production owner boundary without
claiming that synthetic evaluator timing is full CLMM strategy timing.

## Mandatory pre-deploy gate

All items must pass on the same clean revision:

1. strict artifact validation rejects network I/O, mutation authority,
   duplicate routes, invalid pair counts, and worker counts above the pair
   count;
2. full optimized replay processes exactly 2,000,000 strategy frames plus 160
   reconnect events;
3. every frame routes to exactly one evaluator; route failures, dependency
   faults, candidates, network calls, and external mutations remain zero;
4. the global latest-only scheduler never exceeds four running jobs or one
   running plus one pending job per strategy;
5. after continuous replacement of the noisy strategy, all 20 unique
   strategies dispatch before that strategy can dispatch again;
6. captured batch materialization, decode, pool build, and publication have
   exact `n>=100`; partial batches cannot publish;
7. the report enforces a 25,000 ns p99 ceiling for routing, batch
   materialization, decode, and publication, plus 200,000 ns for pool build;
8. existing multi-symbol order identity, known/unknown recovery, restart,
   journal integrity, risk-limit, and telemetry-loss tests remain green;
9. `scripts/quality.sh` passes in full;
10. the diff is reviewed together for hot-path allocation, all-strategy scans,
   accidental production-domain changes, credentials, and mutation authority.

The initial local optimized runs on 2026-07-31 processed 2,000,000/2,000,000
frames and 120/120 reconnect events with zero failures before the rehydration
gate was added. Observed owner routing
p99 was 84 ns; the two observed maxima were 13,583 ns and 20,875 ns.
Throughput in the first run was 11,434,307 frames/s. Fairness was 20/20 unique
strategies before noisy repeat, four/four maximum workers, 21 retained work
items, and 99,999 expected noisy replacements. These workstation numbers are
diagnostic, not target-node evidence. The final consolidated evidence must
supersede these initial numbers and include all 115 hydration samples.

The final consolidated local run processed 2,000,000 frames, 160 reconnect
events, and 115/115 pool publications with zero route/dependency failures and
one/one deliberately rejected partial batch. Routing p95/p99/max was
83/84/21,875 ns. Captured-batch materialization p99/max was 500/1,916 ns,
decode 83/125 ns, pool build 16,958/21,167 ns, and publication 42/42 ns.
Fairness remained 20/20 with four/four workers and 21 retained items. This is
the pre-deploy workstation gate; the workflow still requires the exact image
to independently pass on the fixed C4.

The final repository gate passed 447 library tests, six binary tests, all
deployment/reporting/monitoring integration tests, formatting, clippy with
warnings denied, and the dependency audit with the same three allowed
unmaintained-transitive warnings.

The first exact-image workflow attempt (`30607789568`) stopped before rollout:
the deploy identity cannot create or delete Jobs. The original idempotency path
tried an eager `kubectl delete job --ignore-not-found`, which Kubernetes
authorizes before it checks whether the named object exists. A read-only RBAC
review then confirmed the complete allowed surface before another deployment:
the identity may create/get/delete Deployments and get/list/watch Pods. The
corrected gate therefore uses a unique temporary Deployment whose init
container runs the bounded replay and exposes the report through its
termination message; an inert container holds the Pod only long enough for the
workflow to inspect that message. The temporary Deployment is deleted through
an already-authorized verb. The same review found that the identity cannot read
Node objects, so the gate does not attempt that unnecessary operation: it
requires a non-empty scheduled `nodeName`, verifies the Pod's immutable
node-pool selector, and independently verifies that selected pool is
`c4-highcpu-8` through the already-authorized GKE API. It has no service-account
token, secrets, PVC, or mutation authority. The failed attempt did not change
the application Deployment or the production owner.

The second exact-image attempt (`30608750760`) also stopped before rollout.
Kubernetes admission rejects `activeDeadlineSeconds` inside a Deployment's
ReplicaSet Pod template even though client-side `kubectl` dry-run accepts the
generic PodSpec field. The field was redundant with the 300-second inert hold
and workflow cleanup, so it was removed. The corrected exact manifest must
pass server-side dry-run against the production API before another commit is
pushed; this validates admission, schema, and caller RBAC without persisting an
object.

The corrected manifest then passed server-side dry-run against the production
API using exact image digest
`sha256:80a42387370ea6e08560b5f265648198ea28f4eae9685b36db717818108bad54`.
A follow-up get confirmed that no dry-run Deployment was persisted. The
termination report is 2,194 bytes, below Kubernetes' 4,096-byte termination
message limit, and the runtime image contains the Debian `/bin/sh`, `sleep`,
and `tee` commands used by the bounded wrapper.

The third exact-image attempt (`30609557844`) passed admission but remained
Pending and therefore also stopped before rollout. The fixed node had 7,910m
allocatable CPU and 7,241m requested, leaving 669m; the replay's original
1,000m request could not be scheduled even though the live process was using
only about 0.01 core. The replay now requests 250m and retains its one-core CPU
limit, so it fits the measured request budget and may burst to one otherwise
idle C4 core. This changes scheduling reservation only, not the measured CPU
class or mutation authority. The polling loop now treats an explicit
`Unschedulable` condition as an immediate gate failure instead of waiting for
the full bounded execution timeout. The final 250m manifest passed production
API server-side dry-run, and the failed 1,000m Deployment was removed by the
workflow cleanup trap.

## Target-C4 gate

M11 cannot exit on workstation evidence. The exact release binary and fixture
must run on the fixed `c4-highcpu-8` production CPU class with Linux RSS/high
water reporting. The target run must include reconnect bursts and must be
paired with the unchanged production WLD tables:

- WLD parse, socket-to-decision, DEX receive/build/total and sizing tails;
- CPU peak, throttling, RSS/high-water, memory high/max/OOM;
- canonical, hot telemetry, execution-command, and unknown queue drops;
- Binance rate-limit observations;
- RPC batch and pool-build tails;
- journal recovery and restart evidence.

Running an ad-hoc load beside the live Pod or scaling the owner down from a
workstation is prohibited. The target replay must be encoded in the reviewed
GitHub deployment workflow and fail before the new application owner becomes
Ready. If target replay or production comparisons fail, the revision is not
accepted and no additional pair authority is enabled.

## Authoritative production evidence

Revision `158b48c2d544bddc080d5662670c43c98d463a3b` passed CI run
`30610466763` and Deploy GKE run `30610767701`. The immutable application
image is
`sha256:c75799af20a71a95b766ea221ca06a27581639de93ba813bc642c5cb4595b284`.
Before rollout, that exact image ran the M11 fixture on the fixed
`c4-highcpu-8` node and produced `target_c4_replay_ready`:

- 20 pairs, 23 pools, 2,000,000 strategy frames, and 160 reconnect events;
- 2,000,160 exact single-strategy routes, zero route failures, and zero
  dependency faults;
- decision-owner p50/p95/p99 `73/78/97 ns`, 6,859,108 frames/second;
- four/four maximum sizing workers, 20/20 strategies before noisy repeat,
  21 retained work items, and 99,999 expected noisy replacements;
- five hydration cycles, 115 pool publications, and one/one rejected partial
  batch;
- batch materialization p99 `105 ns`, decode p99 `52 ns`, pool build p99
  `31,330 ns`, and publication p99 `46 ns`;
- RSS before/after/high-water `8,314,880/25,116,672/25,313,280` bytes;
- no network I/O and zero external mutations.

Rollout then created the sole application Pod
`arb-bot-64d564b5b9-cxn98`. Both containers started at
`2026-07-31T06:56:58Z`, became Ready at `06:57:09Z`, use the exact digest
above, and had zero restarts. The GCE rollback owner remained `TERMINATED`.
The production domain remained exactly ESP/USDC plus WLD/USDC; M11 added no
pair, subscription, inventory, order, transfer, or EVM authority.

The initial half-open live comparison window
`[2026-07-31T06:56:58Z, 2026-07-31T07:01:30Z)` had zero hot-telemetry drops
and zero production errors. Its WLD strategy-price sample had `n=284`,
parse p50/p95/p99/max `1/6/9/11 us`, and socket-to-decision
`24/41/51/63 us`. WLD pool-event receive p99 was `40 us`; fee-500 prepared
build/total p99 was `27/37 us`, and fee-3000 build/total p99 was `18/23 us`.
This short live smoke cohort is not used to claim a tail improvement; the
maximum-load percentile evidence is the exact 100,000-frame-per-pair target
replay above. The live values remain inside the reviewed p99 ceilings and the
WLD socket p99 remains within 20% of the frozen `46 us` reference.

Over `[2026-07-31T06:56:58Z, 2026-07-31T07:00:30Z)`, application CPU peaked
at `0.012993` core. Cgroup CPU throttling was zero. Monitoring memory peaked
at `44,363,776` bytes and the later cgroup high-water was `54,812,672`
bytes; memory high/max/OOM/OOM-kill counters were all zero. Startup recovered
the durable trade, EVM, and rebalance journals, reported healthy account rate
limits, and retained the reviewed globally bounded round-robin scheduler.
The only warnings were the bounded pre-connection health sample, the known
ESP top-only depth telemetry, and the expected absence of trading credentials
in the separate read-only ESP collector.

## Remaining live-selection gate

Capacity qualification is not live-pair approval. A later immutable production
bundle may include a new pair only after fresh source export, token/network
identity checks, pool code and hydration checks, Binance symbol/filter and
deposit/withdraw capability checks, inventory/funding, per-pair and
account-wide exposure caps, and explicit production risk approval. The stale
20-pair replay fixture can never satisfy that gate.

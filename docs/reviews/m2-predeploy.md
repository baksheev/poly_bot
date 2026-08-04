# M2 pre-deploy review: World Chain WLD/USDC V3 1% pool

Status: implementation, chain identity, exact-quote parity, and local capacity
gates reviewed for the next production release; production cohort remains to
be collected after normal full-live delivery.

M2 adds one canonical World Chain Uniswap V3 WLD/USDC pool at fee `10000`:
`0x610e319b3a3ab56a0ed5562927d37c233774ba39`. It adds no token, pair,
network, protocol, router, wallet, account, split route, or mutation owner.

## External mutation matrix

- [x] The new route uses the existing World Chain V3 router, allowance,
  exact-input calldata, receipt event, self-impact, nonce, and DEX-first
  execution owners; no endpoint or request shape changes.
- [x] Each parent still selects exactly one immutable pool and submits one DEX
  transaction before the existing Binance IOC and bounded MARKET recovery.
- [x] The 6 USDC detector, 200 USDC adaptive cap, 220 USDC unhedged bound,
  2 USDC recovery-loss bound, 20 bps gate, and one-parent concurrency remain
  unchanged.
- [x] The deployment workflow admits exactly fee `10000` and canonical pool
  `0x610e...ba39`; it does not authorize another 1% pool by fee alone.

## Unknown-outcome and restart matrix

- [x] DEX and Binance Unknown outcomes retain their durable transaction/order
  identities, single status-query authority, reconciliation rules, and
  prohibition on speculative resubmission.
- [x] Partial and zero primary IOC behavior retains one immutable recovery
  target and the reviewed 250 ms / 500 ms bounded retry schedule.
- [x] Restart rehydrates all five WLD pool dependencies from the immutable v14
  source before readiness, while already journaled parents retain their
  immutable selected route and amounts.
- [x] A failure to hydrate the new execution-eligible pool degrades the WLD
  strategy through the existing dependency-health boundary; it cannot silently
  substitute an unreviewed identity.

## Versioned artifact semantic diff

- [x] WLD v14 is a new immutable source; v13 remains byte-stable and readable
  with fingerprint `82448f00a6ea1f3f16f212422e4d12466e55458da41296b0cd12cabf65c3ef90`.
- [x] After removing snapshot ID, capture/evidence provenance, and V3 fee tiers,
  normalized v13 and v14 JSON are identical. The only behavioral source change
  is V3 fees `[500,3000]` to `[500,3000,10000]`.
- [x] The compiler manifest maps fee `10000` to exactly the factory-validated
  address and regenerates a six-pool bundle with five WLD dependencies.
- [x] On World Chain block `33246018`, hash
  `0xd3e4c8345443aa0239cdea002953dba7b14352df197d0d5369eefd313ea25fc4`,
  local V3 exact-input output equalled QuoterV2 in both directions for 6 and
  200 USDC plus 20 and 600 WLD samples.
- [x] The deployment workflow asserts v14, fees `[500,3000,10000]`, and the
  exact canonical address in both the pool graph and strategy dependency list.

## Latency and resource observation plan

- [x] No network read, remote quote, allocation policy, lock, channel, or new
  arithmetic enters evaluation or preflight. The additional pool is hydrated
  once and then uses the existing prepared local V3 curve.
- [x] Post-change `cargo bench --bench local_quote` measured prepared exact-in
  at 236-359 ns, prepared exact-out at 245-335 ns, sparse prepared-curve build
  at 12.96 us, and prepared sparse-capacity exact-out at 31.8 ns.
- [x] `scripts/report-capacity-replay` passed its larger 20-pair/23-pool gate at
  11,922,382 frames/s, decision p99 84 ns, pool-build p99 16.292 us, zero route
  or dependency failures, no network I/O, and zero external mutations.
- [x] Production observation compares per-pool candidate, selection, dispatch,
  fill, failure, realized PnL, DEX receive/build/total latency, CPU, throttling,
  memory, OOM, restart, and telemetry-drop distributions with the preceding
  equal active-hours cohort.
- [x] Acceptance requires decision p99 below 25 us, no material p95/p99
  regression, no new drops/throttling/restart instability, and economically
  interpretable pool-level joins. Regression is corrected or rolled back by a
  reviewed release rather than by adding an unaudited runtime gate.

## Final diff review

- [x] Tests cover the exact v14 fingerprint, v13 provenance, six-pool compiled
  graph, five-pool WLD dependency, deployment assertions, and ignored explicit
  block-pinned World Chain parity gate.
- [x] The existing V3 identity, local quote, allowance, calldata, positional
  Swap receipt, self-impact, known-revert diagnosis, Unknown, restart,
  recovery, and accounting suites remain the implementation contract; no
  route-type code changed.
- [x] `scripts/predeploy-review docs/reviews/m2-predeploy.md origin/main` passes
  on the final committed diff.
- [x] `scripts/quality.sh` passes on the same final diff.
- [x] Production delivery remains the audited `main` / `Deploy GKE` workflow;
  no workstation image build, direct GKE mutation, GCE activation, or new
  mutation authority is introduced.

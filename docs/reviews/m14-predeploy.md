# M14 pre-deploy review: 6 USDC detector/control notional

Status: implementation and local latency gates reviewed for the next production
release; production cohort remains to be collected after normal full-live
delivery.

M14 changes only the WLDUSDC and ESPUSDC detector/control baseline from
`20.00 USDC` to `6.00 USDC`. Adaptive sizing remains capped at `200 USDC`, the
gross spread gate remains 20 bps, and DEX-first execution and recovery are
unchanged.

## External mutation matrix

- [x] A qualifying parent uses the existing DEX transaction, Binance LIMIT IOC,
  and bounded MARKET recovery path; no endpoint, request shape, execution order,
  or idempotency identity changes.
- [x] The smaller detector can increase the number of selected candidates, but
  it does not increase the `200 USDC` per-parent cap, `220 USDC` unhedged bound,
  `2 USDC` recovery-loss bound, or one-concurrent-parent limit.
- [x] No wallet, Binance account, API key, nonce lane, journal, pool, router,
  allowance, token, pair, rebalance route, or external mutation authority is
  added.
- [x] Live startup requires exactly `6.00 USDC`, at least `1.00 USDC` headroom
  above Binance minimum notional, and a filter-valid rounded quantity before
  new entries can be ready.

## Unknown-outcome and restart matrix

- [x] DEX and Binance Unknown outcomes retain their existing durable IDs,
  reconciliation queries, nonce/order ownership, and no-resubmission rules.
- [x] Partial and zero primary IOC behavior retains one immutable MARKET target
  and the reviewed bounded retry schedule; the smaller detector does not create
  another recovery target.
- [x] Restart reloads the versioned 6 USDC artifact and validates current
  Binance filters before new work; already journaled operations recover from
  their immutable amounts independently of the new detector value.
- [x] Historical WLD v12 and ESP v6 artifacts remain checked in unchanged and
  readable as release provenance.

## Versioned artifact semantic diff

- [x] WLD v13 and ESP v7 are new immutable sources; v12 and v6 are not rewritten
  or deleted.
- [x] Both new sources change `quote_sizing.token_a_base_units` from `20000000`
  to `6000000`; adaptive caps, spread, slippage, routes, identities, execution,
  rebalance, and recovery fields are unchanged.
- [x] The compiled multi-pair bundle is regenerated from v13/v7 and records their
  new fingerprints. Compatibility projections retain their prior authority.
- [x] The deployment workflow asserts the exact v13/v7 snapshot IDs and both
  `6000000` baseline values before rollout.
- [x] Startup emits configured and effective validation notional without reading
  ClickHouse or adding a runtime policy resolver.

## Latency and resource observation plan

- [x] No network read, RPC, allocation, lock, channel, JSON construction, or new
  arithmetic was added between accepted price parsing and baseline evaluation.
  The new validation and telemetry execute once during startup.
- [x] Before/after `cargo bench --bench local_quote` changed prepared and
  iterative quote samples by `-0.2%` to `+2.4%`, within the declared 5% noise
  bound. The slow prepared-curve build changed by `+1.2%`.
- [x] Five post-change `replay-capacity` samples produced median throughput
  `12,401,626 frames/s`, `2.5%` below the pre-change `12,725,827 frames/s` and
  within the 5% gate. Decision p99 remained `83 ns`; pool-build p99 remained
  below `20 us`; replay performed no network I/O or external mutation.
- [x] The full-live production cohort must compare the same half-open window's
  Binance parse, socket-to-decision, adaptive calculation, exact-evaluation
  count, sizing queue/worker/handoff, DEX receive/build/total, telemetry drops,
  CPU, throttling, memory, OOM, and restart distributions with the preceding
  accepted cohort.
- [x] Release acceptance requires no statistically material p95/p99 regression
  in decision or adaptive calculation latency, decision p99 below `25 us`, and
  no new telemetry drops or throttling. Exact evaluation count is retained as
  an explanatory work metric. A speed regression is corrected or rolled back
  in the next reviewed release; no cohort-specific runtime controller is added.

## Final diff review

- [x] Tests cover the exact 6 USDC value, the 5 USDC exchange floor, insufficient
  headroom, deterministic rounded quantity, compiled artifacts, and deployment
  assertions.
- [x] Existing adaptive sizing, recovery, restart, accounting, portfolio, and
  capital safety suites remain green.
- [x] `scripts/predeploy-review docs/reviews/m14-predeploy.md origin/main` passes
  on the final diff.
- [x] `scripts/quality.sh` passes on the same final diff.
- [x] Production delivery remains the audited `main` / `Deploy GKE` workflow;
  no workstation image build, direct GKE mutation, GCE activation, or automatic
  live-canary controller is introduced.

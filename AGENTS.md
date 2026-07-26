# Agent Notes

This repository is an autonomous low-latency Rust clone of the Rails arbitrage
application at `/Users/baksheev/code/arb_bot`, built beside it one component at
a time.

## Runtime architecture

- Production is one application Pod on the private zonal GKE Standard cluster
  `arb-bot` in `asia-southeast1-b`. It runs on one fixed `c4-highcpu-8` node;
  Cluster Autoscaler is disabled and application releases reuse that allocated
  node rather than creating a replacement node pool.
- The `arb-bot-rust-shadow-gce` VM is a stopped rollback target only. It must
  remain `TERMINATED` while the GKE Deployment has a nonzero replica count.
  Never let GCE and GKE control the same wallet, Binance account, orders,
  journals, or nonces concurrently.
- Cloud Run is not the latency-sensitive runtime.
- Keep Binance and DEX market data, strategy state, balances, reservations,
  nonces, positions, and execution context in memory.
- Postgres and ClickHouse are never part of the critical trading path.
  ClickHouse receives telemetry and state journals through bounded background
  channels; a failed or slow write must not delay a decision or order.
- Reuse process-scoped WebSocket, HTTP, RPC, signing, and connection-pool
  clients. Do not construct clients inside per-tick or per-order code.
- Preserve a single owner for latency-sensitive mutable state unless profiling
  proves another topology is better.
- Load strategy/chain/token/instrument configuration once from a versioned,
  validated artifact. Rails Postgres is an operator-only export source and must
  never be a runtime dependency or a production runtime secret.
- Derive Binance subscriptions from the domain artifact; do not create a second
  symbol allowlist in environment variables.
- Use fixed-point integer or validated decimal representations for financial
  values. Do not use `f64` for strategy or execution math.
- The production GCP region is `asia-southeast1` (Singapore). US regions are
  excluded because Binance access is unavailable there. Re-evaluate the exact
  topology with measured Binance, Alchemy, and target-chain tail latency before
  scaling live trading.
- ClickHouse is also in GCP `asia-southeast1`. A future ClickHouse migration or
  outage must never block or delay the in-memory trading loop.
- The JSON Binance depth stream may provide exchange event time only for
  diagnostic exchange-to-socket estimates. Record clock offset, synchronization
  RTT/age, timestamp resolution, and estimate uncertainty asynchronously. This
  diagnostic must never become a strategy-price, readiness, admission,
  preflight, or execution input.
- Use `./scripts/gcloud-local` for every local gcloud command. Its
  repository-local `.gcloud/` configuration keeps this project's account,
  project, and ADC isolated from the global Google Cloud SDK configuration.

## Production delivery

- Do not use local Docker for this repository, including builds, tests, tags,
  pushes, or production image inspection that requires pulling an image.
- Deliver every production application revision through
  `.github/workflows/deploy-gke.yml` on `main`. The GitHub Action must build and
  push the production image, resolve its immutable digest, and roll that exact
  digest out to the existing fixed GKE node only after CI passes and the
  `production` environment is approved.
- Do not open a pull request for routine production changes. After the intended
  scope is committed and `scripts/quality.sh` passes, fetch `origin/main`,
  require a clean fast-forward integration, push the resulting commit directly
  to `origin/main`, trigger the `Deploy GKE` workflow for that exact `main`
  revision, and monitor it through rollout verification. Never force-push or
  discard remote commits; stop and report if `main` cannot be fast-forwarded or
  branch protection rejects the direct push.
- The production GKE Pod must use the reviewed v12 adaptive-live domain artifact
  with both arbitrage and rebalancing in `full_live`. Adaptive sizing selects
  the largest Binance-step-aligned exact DEX-curve candidate that clears the
  configured 20 bps gross spread and the 200 USDC cap. Binance top quantity,
  full depth, recovery forecasts, expected-profit floors, gas economics, and
  inventory are not sizing or admission gates. Full-depth health is telemetry
  and a log-based metric only. Inventory reservations cover only the exact
  primary token debits and must never apply the legacy Rails `3x` multiplier,
  reserve native gas, or reserve a hypothetical recovery. Native gas funding
  is an operator-maintained invariant, not a readiness, balance-sync,
  admission, or reservation input. The dedicated execution owner refreshes
  `eth_gasPrice` every second and keeps each RPC or Rails-fallback sample valid
  for two seconds. Transaction construction reads only that cache in the live
  hot path and uses the Rails `100,000 wei` World Chain fallback after a zero
  or failed refresh. Receipt accounting includes both the L2 execution fee and
  World Chain `l1Fee`; neither is a sizing or admission gate. After a
  successful DEX fill, non-blockingly drain already
  queued DEX WebSocket events, apply the receipt's positional pool `Swap`
  directly to the local mirror, and rebuild the affected prepared curves before
  releasing the execution lane. Never wait for a second `eth_getLogs` copy and
  never create a post-trade pool or global settlement barrier. Pending work
  from an older pool generation remains eligible for the existing entry
  preflight, which requotes it against the latest published generation and
  fresh Binance price before dispatch. A known DEX revert must emit immediate
  receipt telemetry and may enqueue a bounded background
  `debug_traceTransaction`/historical-`eth_call` diagnosis. Trace availability,
  latency, decoded reason, and custom-error selector are diagnostic only and
  must never delay lane release or enter readiness, admission, preflight,
  recovery, or execution. After a partial or zero primary Binance IOC, the
  coordinator
  creates one immutable MARKET recovery target equal to `primary hedge target
  - primary executed quantity`. A proven zero-fill/unsubmitted/rejected child
  may retry that same target at most three total attempts, with persisted
  250 ms then 500 ms backoff and deterministic child IDs. Partial/full fills
  never start another transaction attempt. An Unknown Binance placement is
  part of the same recovery state machine but may only query `order.status`
  once for the same deterministic client ID. A discovered order uses its
  actual fills; `-2013 NO_SUCH_ORDER` proves absence and immediately allows the
  normal next recovery step. A timeout/5xx/transport/protocol failure of that
  status query remains Unknown and must not authorize another order. Remaining WLD drift is PnL and
  inventory telemetry only. The 20 bps gate uses raw
  venue economics. Rails-compatible 5-50 bps dynamic slippage affects only the
  DEX calldata input/output bounds; Binance commission, gas, and recovery never
  create a second profitability model. After the DEX receipt, the primary
  Binance LIMIT IOC keeps the admission price as its immutable protection
  boundary and may use a fresh in-memory top only to improve execution: SELL
  uses the higher price and BUY the lower price. An adverse or unavailable top
  keeps the admission price, followed by bounded MARKET recovery after a
  partial or zero fill. The selected IOC price must be journaled before
  dispatch. Primary selection and every LIMIT/MARKET order must emit joinable
  non-blocking telemetry containing the placement-time in-memory top, exact
  request, marketability at that top, terminal fill/average price, commissions,
  and reconciliation outcome. MARKET fallback telemetry must also persist the
  counterfactual same-side in-memory LIMIT price, visible top coverage, actual
  MARKET price advantage, fill-price compatibility, placement-to-terminal
  duration, and terminal top. This counterfactual is diagnostic and must never
  enter execution. MARKET recovery remains the control until a
  representative cohort justifies a separately reviewed one-order experiment.
  Production Binance commissions are paid from the account's BNB balance.
  MARKET BUY recovery must never be grossed up for a hypothetical base-asset
  commission; both BUY and SELL submit the immutable recovery target rounded
  down to the exchange step. Discounted fees are tracked as exact BNB balance
  deltas and valued for realized PnL with the configured BNBUSDT bid, matching
  Rails' USDT/USDC parity convention. This auxiliary feed is accounting-only
  and must never enter readiness, admission, sizing, preflight, or recovery. A
  missing price makes valuation telemetry incomplete; it must not turn a known
  fill into an unknown exposure or block bounded recovery.
  An unchanged event-driven Binance top
  remains current while its connection generation has fresh transport
  activity. Admission, preflight, and runtime market-data readiness use the
  reviewed 30-second maximum transport silence from the versioned domain
  artifact, not the age of the last price change. That artifact is the only
  strategy-price liveness source.
  The separate native-token conversion feed is diagnostic/accounting input:
  its connection, quote age, and transport activity must never change
  readiness, admission, preflight, or execution eligibility. The deployment
  workflow must verify the strategy-price startup fields before it reports
  success.
- Do not use `.github/workflows/deploy-gce.yml` for routine production delivery.
  It is retained only for an explicitly reviewed rollback after the GKE owner
  is scaled to zero and all active operations are reconciled.
- Do not run `scripts/update-gce-worker`, build or push a production image from
  a workstation, directly restart GCE, run `kubectl apply`/`rollout`/`scale`
  locally, or create/delete GKE node pools for an application release. Encode
  production changes in the workflow, commit them, let CI pass, and use the
  `Deploy GKE` GitHub Action.
- Application releases must not allocate a fresh C4 node. Replacing or
  upgrading the fixed node pool is a separate infrastructure change requiring
  an explicit capacity plan, rollback plan, and reviewed GitHub Action.
- Local GCP access is for read-only inspection and explicitly requested
  bootstrap or recovery work only. Routine production mutations belong in a
  GitHub Action so the actor, revision, logs, and outcome are auditable.

## Clone boundaries

- The Rails application keeps running independently. Do not move partial live
  ownership from Rails into Rust as components are built.
- The Rust runtime must not read Rails Postgres/Redis or call Rails services.
  Existing code and captured fixtures are a behavioral specification only.
- Build and verify each Rust component behind typed interfaces before composing
  it into the complete clone.
- Hydrate Rust from its own versioned startup configuration and external
  sources of truth, validate them, and keep operational state in memory.
- Preserve the existing DEX-first, Binance-hedge recovery semantics until a
  separate design explicitly changes execution ordering.
- Keep `dex_first` and `concurrent_hedged` behind the same coordinator boundary.
  Treat DEX-first as the control; change the production default only through the
  predeclared randomized switchback experiment in
  `docs/concurrent-execution.md`.
- Before live canary, provision separate EVM wallets, nonce space, Binance
  account/API keys, inventory, secrets, limits, and recovery scope. Never let
  the two bots control the same funds, orders, or nonces.

## Safety

- Never commit or log private keys, API secrets, signing payloads, raw
  credential-bearing RPC URLs, or authenticated Binance requests.
- Read-only clone stages must not receive trading or signing credentials.
- `ARB_BOT_DATABASE_URL` is local migration tooling only. Never log it, commit
  it, upload it to Secret Manager, or expose it to the production Rust service.
- New live entries remain disabled until configuration, market data, wallet
  state, Binance state, reservations/nonces, and risk controls are hydrated and
  healthy.
- Start execution work in paper mode and add an explicit live-trading gate.
- Every write or order command needs an idempotency key and a recovery path for
  unknown outcomes.

## Verification

Before handing off code changes, run:

```bash
scripts/quality.sh
```

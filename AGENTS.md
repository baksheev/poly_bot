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
  reviewed GitHub Action so the actor, revision, logs, and outcome are auditable.

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

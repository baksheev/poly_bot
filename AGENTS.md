# Agent Notes

This repository is an autonomous low-latency Rust clone of the Rails arbitrage
application at `/Users/baksheev/code/arb_bot`, built beside it one component at
a time.

## Runtime architecture

- Production is one GCP Compute Engine process on a `c4-highcpu-8` VM in
  `asia-southeast1-b`. Cloud Run is not the latency-sensitive runtime.
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
  never be a runtime dependency or a GCE runtime secret.
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

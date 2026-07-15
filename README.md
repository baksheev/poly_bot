# poly_bot

Low-latency Rust service for trading Polymarket **BTC 5-minute Up/Down**
markets. This first version is an executable architecture scaffold; it does
not connect to exchanges or place orders yet.

## Architecture

```text
Binance WS ─┐
            ├─> one Rust event loop ─> in-memory state ─> strategy ─> execution
Polymarket ─┘                               │
                                           └─> bounded async queue ─> ClickHouse
```

The hot path never reads from or waits for ClickHouse. Telemetry uses
`try_send`; if the bounded queue is full, the event is dropped and trading can
continue. The service is intended to run as one Cloud Run Worker Pool instance.

The initial region is **GCP `us-east1`**, matching the current `pump_bot`
ClickHouse region. This is a provisional placement decision and should be
validated with latency measurements from GCP to Binance and Polymarket before
enabling live execution.

The GCP project is `poly-bot-502515`. Local Google Cloud authentication is
repository-scoped: always use `./scripts/gcloud-local` so this repository's
active account, project, and ADC do not change the global `gcloud` setup. See
[docs/gcp-local-auth.md](docs/gcp-local-auth.md).

## What is included

- Rust 1.90 / edition 2024 binary with `run`, `check`, and `migrate` commands;
- single-owner in-memory runtime state;
- non-blocking, batched ClickHouse telemetry channel;
- ClickHouse migration for `runtime_telemetry`;
- production multi-stage Docker image;
- GitHub Actions quality and Docker build gate;
- GCP Worker Pool configuration template.

## Local setup

```bash
cp .env.example .env
cargo run -- check
cargo run -- run
```

Without `CLICKHOUSE_URL`, `run` uses log-only telemetry. To create the table in
a configured ClickHouse instance:

```bash
cargo run -- migrate
```

Quality gate:

```bash
scripts/quality.sh
```

## Repository bootstrap

After creating an empty GitHub repository:

```bash
git remote add origin git@github.com:YOUR_ORG/poly_bot.git
git push -u origin main
```

Before the first GCP deployment, copy
`infra/gcp/workloads.example.json` to `infra/gcp/workloads.json` and replace the
service-account placeholder. Deployment automation is intentionally deferred
until that identity and the GitHub repository exist.

## Next implementation slices

1. Discover the active BTC 5-minute market and resolve Up/Down token IDs.
2. Add Binance book-ticker and Polymarket CLOB WebSocket connectors.
3. Record synchronized market data to ClickHouse for research and replay.
4. Add a deterministic replay/paper-trading engine and latency telemetry.
5. Add signing, risk limits, reconciliation, and an explicit live-trading gate.

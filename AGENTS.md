# Agent Notes

This repository is a low-latency Rust trading service for Polymarket BTC
5-minute Up/Down markets.

## Runtime architecture

- Production is one GCP Cloud Run Worker Pool process.
- Keep Binance/Polymarket market data, strategy state, positions, and execution
  context in memory.
- ClickHouse is never part of the critical trading path. Send telemetry and
  state journals through the bounded background channel; a failed or slow
  ClickHouse write must not delay a decision or order.
- Reuse process-scoped network clients and connection pools. Do not construct
  clients inside per-tick or per-order code.
- Preserve a single owner for latency-sensitive mutable state unless profiling
  proves another topology is better.
- The initial GCP region is `us-east1`, matching `pump_bot` and its ClickHouse
  deployment. Re-evaluate with measured Binance, Polymarket, and ClickHouse
  latency before live trading.
- Use `./scripts/gcloud-local` for every local gcloud command. Its repository-local
  `.gcloud/` configuration keeps this project's account, project, and ADC
  isolated from the global Google Cloud SDK configuration.

## Safety

- Never commit or log private keys, API secrets, signing payloads, or
  credential-bearing URLs.
- New live entries must remain disabled until market discovery, market data,
  wallet state, open positions, and risk controls are hydrated and healthy.
- Start execution work in paper mode and add an explicit live-trading gate.

## Verification

Before handing off code changes, run:

```bash
scripts/quality.sh
```

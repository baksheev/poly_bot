# Permanent runtime cleanup review

This release removes completed rollout scaffolding from executable code while preserving the current WLD/USDC World Chain and ESP/USDC Arbitrum full-live behavior. Historical architecture and production evidence remain in `docs/`.

## External mutation matrix

- [x] WLD/USDC arbitrage and rebalance authority remains `WorldChainV12`.
- [x] ESP/USDC arbitrage and direct Arbitrum rebalance authority remains `ArbitrumFullLive`.
- [x] Arbitrum rebalance remains direct-only, one concurrent transfer, one unknown-outcome reconciliation query, and bridge mutations disabled.
- [x] Per-operation debit and fee caps remain unchanged in the v6 production artifact.
- [x] Removed commands cannot create prefunding, address-verification, diagnostic, or manual incident-recovery mutations.
- [x] Deployment ownership remains one GKE application Pod with the GCE rollback target stopped.

## Unknown-outcome and restart matrix

- [x] Active rebalance operations remain globally unique and recover before new work.
- [x] Old journal records remain deserializable through neutral scope/network checks and serde field aliases.
- [x] A standard withdrawal unknown still receives one reconciliation query and cannot be replayed without proof of absence.
- [x] Exact `-4104` routing to the Travel Rule endpoint remains durable and idempotent.
- [x] Terminal and unknown EVM/Binance operations keep their existing restart behavior.

## Versioned artifact semantic diff

- [x] Production strategy inputs remain v12 WLD and v6 ESP with unchanged trading, sizing, risk, token, network, pool, and approval values.
- [x] The compiled domain differs only by the neutral compiler version `production-v1`.
- [x] The capacity artifact differs only by its neutral artifact ID and path.
- [x] Historical v3-v5 ESP rollout artifacts and the one-shot address-verification operation artifact are removed from runtime inputs.
- [x] The checked-in compiled bundle is byte-for-byte equal to fresh compiler output.

## Latency and resource observation plan

- [x] Run the immutable 20-pair capacity replay on the fixed `c4-highcpu-8` before rollout.
- [x] Require two million frames, zero routing/dependency faults, decision-owner p99 at or below 25 microseconds, bounded fairness, and complete rehydration publication.
- [x] After rollout, compare WLD socket/parse/decision and DEX receive/total tails with the prior production cohort.
- [x] Check CPU, memory, throttling, restarts, production ERROR logs, readiness, and rebalance telemetry for the exact deployed engine ID.

## Final diff review

- [x] No executable/config/workflow/test path outside historical docs contains rollout labels for completed stages.
- [x] Permanent reporting and capacity commands use stable names.
- [x] Standard withdrawal remains the first path; deposit questionnaire logic remains conditional and separate.
- [x] No secrets, credential-bearing URLs, private keys, signed payloads, or authenticated requests are added.
- [x] `cargo test`, targeted journal/routing tests, compiled-domain equality, and deployment assertions pass.
- [x] `scripts/quality.sh` and `scripts/predeploy-review` must pass again immediately before commit.

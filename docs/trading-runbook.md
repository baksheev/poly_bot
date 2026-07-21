# Live arbitrage operator runbook

Last reviewed: 2026-07-20

This runbook applies only to the isolated WLDUSDC Rust identities owned by the
single production Pod in the private zonal GKE cluster `arb-bot` in
`asia-southeast1-b`. Rails continues to own separate wallets, Binance account,
orders, and nonces. The stopped `arb-bot-rust-shadow-gce` VM is rollback-only
and must never run while the GKE Deployment has a nonzero replica count.

## Immutable launch inputs

- digest-pinned image built from a clean committed revision;
- v10 adaptive-live artifact: pair 3, World Chain 480, WLDUSDC Spot, 20 USDC detector/fallback, 200 USDC global cap, 750 ms / delta 8 recent-depth caps, and 40 USDC top-only cap,
  WLD step 0.1, live exchange tick 0.0001, `profit_token_a`, 20 bps, V3/V4;
- dedicated GCE static egress `34.21.220.162` on the Binance key allowlist;
- the dedicated wallet and Binance subaccount verified at startup;
- persistent `/var/lib/arb-bot` parent, Binance-order, and wallet/nonce
  journals;
- no open Binance orders, no locked balance, no unresolved wallet nonce, no
  active rebalance, and fresh Binance top-of-book/DEX/balance/gas inputs; full
  depth health is observed separately and does not gate DEX-first readiness;
- fixed full-live v10 adaptive deployment, tiered depth, 20 bps spread admission, exact execution-envelope reservations, single-owner enforcement, and entry-stop
  recovery controls.

Run `scripts/quality.sh`, fetch `origin/main`, require a clean fast-forward,
push the validated commit directly to `main`, approve the `production`
environment when requested, and deploy only with the `Deploy GKE` workflow. Do
not open a routine production PR, force-push, or overwrite remote commits. The
workflow builds the image, resolves its immutable digest, reuses the fixed node,
and verifies the v10/full-live runtime config. Do not deploy from a workstation
or use the GCE updater.

```bash
gh workflow run deploy-gke.yml --ref main
```

## Entry stop and recovery

The recoverable kill switch is the persistent-volume file
`/var/lib/arb-bot/arbitrage-entry.stop`. Creating it blocks new parent intents
but deliberately leaves restart reconciliation and residual recovery enabled:

Enable or clear it only through an approved GitHub Actions operational change
targeting the GKE Pod and its mounted state volume. Never SSH to the rollback
GCE VM or mutate the production Pod from a workstation; that would operate on
the wrong owner or bypass the audited delivery boundary.

Do not stop the process merely because an order/transaction is ambiguous. Keep
the entry stop active, inspect the parent/child state, and prove the venue
outcome by deterministic Binance client order ID or World Chain transaction
hash. Never edit, truncate, copy over, or delete a journal. An `Unknown` parent
is not balanced and must not be included in PnL.

A hard service stop is allowed only after confirming there is no unresolved
parent, order, nonce, transaction, or residual exposure. Scale or stop the GKE
owner only through a reviewed, approved GitHub Actions recovery change.

Removing the entry-stop file is a new-entry authorization. Do it only after
venue and journal reconciliation.

## Canary and 100-trade run

For the first composed live canary, enable `full_live`, wait for one balanced
parent result, then immediately activate the entry stop and verify venue
history, journals, balances, and accounting. The canary must use the same
strategy parameters as Rails; do not add cost, loss, total-entry, or rate caps
that Rails does not have.

After the canary is balanced, venue-verified, and economically accounted,
remove the entry stop and run the same live journal until the watcher observes
100 balanced results. Do not replace or clear the journal between phases.

The 2026-07-17 Rails reference snapshot for the most recent 100 pair-3
`profit_token_a` results was:

- total estimated profit: `2.711123 USDC`;
- mean: `0.027111 USDC/trade`;
- profitable: `89/100`;
- summed absolute WLD residual: `0`.

The final verdict uses one equal UTC half-open interval and the queries in
`docs/arbitrage-results.md`. Rust must have 100 balanced admitted parents,
zero blocked unknown parents, zero actionable residual, and total plus average
`comparable_profit_token_a_base_units` no worse than the Rails population for
the same interval. Report cash realized, residual mark, gas, and recovery loss
separately.

## Rollback

First activate the entry stop. Allow any already journaled recovery to finish,
then verify venue state and scale the GKE Deployment to zero through an approved
GitHub Actions recovery change. Only after GKE is stopped and has no unresolved
ownership may the rollback VM or another runtime be started. Rollback never
reuses a Rails identity and never restores from a deleted journal.

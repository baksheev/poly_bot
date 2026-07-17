# Live arbitrage operator runbook

Last reviewed: 2026-07-17

This runbook applies only to the isolated WLDUSDC Rust identities on
`arb-bot-rust-shadow-gce` in `asia-southeast1-b`. Rails must continue to own its
own wallets, Binance account, orders, and nonces. Never run GCE live arbitrage
while the GKE `arb-bot` deployment has a nonzero replica count: it currently
owns the Rust rebalance wallet/account. The deployment wrapper refuses this
overlap.

## Immutable launch inputs

- digest-pinned image built from a clean committed revision;
- v5 domain artifact: pair 3, World Chain 480, WLDUSDC Spot, 20 USDC baseline,
  WLD step 0.1, tick 0.001, `profit_token_a`, 20 bps, V3/V4;
- dedicated GCE static egress `34.21.220.162` on the Binance key allowlist;
- the dedicated wallet and Binance subaccount verified at startup;
- persistent `/var/lib/arb-bot` parent, Binance-order, and wallet/nonce
  journals;
- no open Binance orders, no locked balance, no unresolved wallet nonce, no
  active rebalance, and fresh Binance/depth/DEX/balance/gas inputs;
- exact live confirmation, v5 domain selection, single-owner deployment, and
  entry-stop recovery controls.

Run `scripts/quality.sh`, then deploy paper mode first:

```bash
scripts/update-gce-worker IMAGE@sha256:DIGEST SOURCE_REVISION paper_dex_first
```

Paper results use a separate journal and the `paper_arbitrage_result` telemetry
kind. They never count toward the 100-trade goal.

## Entry stop and recovery

The recoverable kill switch is the host file
`/var/lib/arb-bot/arbitrage-entry.stop`. Creating it blocks new parent intents
but deliberately leaves restart reconciliation and residual recovery enabled:

```bash
./scripts/gcloud-local compute ssh arb-bot-rust-shadow-gce \
  --zone=asia-southeast1-b \
  --tunnel-through-iap \
  --command='sudo touch /var/lib/arb-bot/arbitrage-entry.stop'
```

Do not stop the process merely because an order/transaction is ambiguous. Keep
the entry stop active, inspect the parent/child state, and prove the venue
outcome by deterministic Binance client order ID or World Chain transaction
hash. Never edit, truncate, copy over, or delete a journal. An `Unknown` parent
is not balanced and must not be included in PnL.

A hard service stop is allowed only after confirming there is no unresolved
parent, order, nonce, transaction, or residual exposure:

```bash
./scripts/gcloud-local compute ssh arb-bot-rust-shadow-gce \
  --zone=asia-southeast1-b \
  --tunnel-through-iap \
  --command='sudo systemctl stop arb-bot.service'
```

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
then verify venue state and stop GCE. Only after GCE is stopped and has no
unresolved ownership may another runtime be scaled up. Rollback never reuses a
Rails identity and never restores from a deleted journal.

# Live arbitrage operator runbook

Last reviewed: 2026-08-02

This runbook applies only to the isolated WLDUSDC Rust identities owned by the
single production Pod in the private zonal GKE cluster `arb-bot` in
`asia-southeast1-b`. Rails continues to own separate wallets, Binance account,
orders, and nonces. The stopped `arb-bot-rust-shadow-gce` VM is rollback-only
and must never run while the GKE Deployment has a nonzero replica count.

## Immutable launch inputs

- digest-pinned image built from a clean committed revision;
- v14 adaptive-live artifact: pair 3, World Chain 480, WLDUSDC Spot, 6 USDC
  detector/fallback, 200 USDC execution cap, WLD step 0.1, live exchange tick
  0.0001, `profit_token_a`, 20 bps, V3/V4 including the canonical V3 1% pool,
  a 30-second maximum Binance transport silence, and a 30-second maximum age
  of the latest received canonical World Chain head;
- dedicated GCE static egress `34.21.220.162` on the Binance key allowlist;
- the dedicated wallet and Binance subaccount verified at startup;
- persistent `/var/lib/arb-bot` parent, Binance-order, and wallet/nonce
  journals;
- no unresolved wallet nonce, hydrated balance state, and fresh Binance
  strategy-price/DEX inputs; Binance and wallet balance generations must be no
  older than 10 seconds. Open Binance orders and locked balances are allowed:
  only free inventory after exact reservations is admissible. User Data,
  native-token conversion-feed health, and full-depth health are observed
  separately and do not gate DEX-first readiness;
- fixed full-live v14/v7 adaptive deployment, DEX-curve-only maximum-slot sizing,
  20 bps spread admission, exact primary reservations, single-owner
  enforcement, and entry-stop recovery controls.

Run `scripts/quality.sh`, fetch `origin/main`, require a clean fast-forward,
push the validated commit directly to `main`, approve the `production`
environment when requested, and deploy only with the `Deploy GKE` workflow. Do
not open a routine production PR, force-push, or overwrite remote commits. The
workflow builds the image, resolves its immutable digest, reuses the fixed node,
and verifies the v14/v7 full-live runtime config. Do not deploy from a workstation
or use the GCE updater.

```bash
gh workflow run deploy-gke.yml --ref main
```

## Entry stop and recovery

The recoverable kill switch is the persistent-volume file
`/var/lib/arb-bot/arbitrage-entry.stop`. Creating it blocks new parent intents
but deliberately leaves restart reconciliation and any already-journaled
bounded MARKET recovery/backoff enabled:

Enable or clear it only through an approved GitHub Actions operational change
targeting the GKE Pod and its mounted state volume. Never SSH to the rollback
GCE VM or mutate the production Pod from a workstation; that would operate on
the wrong owner or bypass the audited delivery boundary.

Use `Operate GKE Recovery` with `activate-entry-stop` to block admission. The
running owner publishes
`/var/lib/arb-bot/arbitrage-entry.stop.recovery-safe` only after the stop is
active and its constant-time durable active-parent count is zero. Recovery
operations refuse to scale the Deployment down until that exact marker is
present in the Ready production Pod. The workflow records the handoff on the
Deployment before scaling to zero, so a failed one-shot command can be retried
without bringing up a second owner.

Do not stop the process merely because an order/transaction is ambiguous. Keep
the entry stop active, inspect the parent/child state, and prove the venue
outcome by deterministic Binance client order ID or World Chain transaction
hash. Never edit, truncate, copy over, or delete a journal. An `Unknown` parent
is not balanced and must not be included in PnL.

A hard service stop is allowed only after confirming there is no unresolved
parent, order, nonce, transaction, or fallback ownership. A recorded WLD
inventory delta in a terminal result is not unresolved mutation ownership.
Scale or stop the GKE owner only through a reviewed, approved GitHub Actions
recovery change.

Removing the entry-stop file is a new-entry authorization. Do it only after
venue and journal reconciliation.

### Historical `dex:expired-plan` operator recovery

The dedicated command is only for the legacy false-terminal shape where the
trade journal says `dex:expired-plan`, while the exact EVM transaction journal
and chain prove that the DEX swap filled. It refuses every other terminal
shape. Run it only as a reviewed one-shot recovery owner with the production
Deployment stopped and the same persistent journals mounted; the normal Pod
and the recovery command must never hold the wallet, Binance account, nonce, or
journal concurrently.

Keep `/var/lib/arb-bot/arbitrage-entry.stop` present. First run the complete
read-only proof:

```bash
arb_bot arbitrage-record-operator-recovery \
  --plan-id PLAN_ID \
  --dex-transaction-hash 0xTRANSACTION_HASH \
  --wallet-journal-path /var/lib/arb-bot/arbitrage-arbitrum-wallet.jsonl \
  --order-journal-path /var/lib/arb-bot/arbitrage-binance-orders.jsonl \
  --mode dry-run \
  --maximum-quote-usdc 250
```

The dry run rebuilds the exact calldata, reconciles only the already-journaled
receipt, fetches the current same-side Binance top, checks the primary and `r1`
client IDs once, and prints the immutable MARKET target. It cannot sign or
broadcast a DEX transaction and cannot place a Binance order.

After reviewing that evidence, repeat with `--mode execute` and
`ARBITRAGE_OPERATOR_RECOVERY_CONFIRMATION=RECORD_LIVE_ARBITRAGE_OPERATOR_RECOVERY`.
Execute either adopts the same deterministic terminal Binance order already
found at the venue or, only when both IDs are proven absent, records the intent
and places one exact-quantity MARKET order through the normal Binance execution
owner. The command then appends the chain fill, Binance fill, actor, timestamp,
and venue IDs to the trade journal and recomputes the terminal result. A filled
order remains authoritative even if realized slippage exceeds the pre-placement
quote cap; the command records it and emits an error instead of recreating an
unknown exposure.

Invoke those commands through `Operate GKE Recovery`, not from a workstation:

```bash
gh workflow run operate-gke-recovery.yml --ref main \
  -f operation=recovery-dry-run \
  -f plan_id=PLAN_ID \
  -f dex_transaction_hash=0xTRANSACTION_HASH \
  -f maximum_quote_usdc=250

gh workflow run operate-gke-recovery.yml --ref main \
  -f operation=recovery-execute \
  -f plan_id=PLAN_ID \
  -f dex_transaction_hash=0xTRANSACTION_HASH \
  -f maximum_quote_usdc=250 \
  -f confirmation=EXECUTE
```

The workflow reuses the digest-pinned production image and Pod template,
proves the GCE owner is `TERMINATED`, waits for the quiescent handoff, scales
the Deployment to zero, and mounts the same PVC and secrets into one Job. A
failed execute deliberately leaves the Deployment at zero with the entry stop
active; rerun dry-run/execute to resolve the same deterministic IDs. A
successful execute restarts the normal owner with the entry stop still active.

Restart the Deployment through the reviewed recovery workflow, verify that the
strategy quarantine is clear and balances are synchronized, and only then
remove the entry stop. Never hand-edit or replace any journal.

Release admission only through `Operate GKE Recovery` with
`release-entry-stop` and `confirmation=RELEASE`. It requires a Ready normal Pod
and a fresh zero-active-parent marker before deleting the stop file.

## Canary and 100-trade run

For the first composed live canary, enable `full_live`, wait for one terminal
parent result, then immediately activate the entry stop and verify venue
history, journals, balances, and accounting. The canary must use the same
strategy parameters as Rails; do not add cost, loss, total-entry, or rate caps
that Rails does not have.

After the canary is terminal, venue-verified, and economically accounted,
remove the entry stop and run the same live journal until the watcher observes
100 terminal results. The persisted stage names remain `balanced_profit` and
`balanced_loss` for journal compatibility. Do not replace or clear the journal
between phases.

The 2026-07-17 Rails reference snapshot for the most recent 100 pair-3
`profit_token_a` results was:

- total estimated profit: `2.711123 USDC`;
- mean: `0.027111 USDC/trade`;
- profitable: `89/100`;
- summed absolute WLD residual: `0`.

The final verdict uses one equal UTC half-open interval and the queries in
`docs/arbitrage-results.md`. Rust must have at least 100 terminal admitted
parents. Report aggregate signed and absolute WLD inventory drift and its PnL
mark; drift is an observed result, not a completion gate. Report unknown
parents separately and verify that each held only its own reservation and did
not block later independent work. Total and average
`comparable_profit_token_a_base_units` are compared with Rails for the same
interval; cash realized, residual mark, gas, and recovery loss are reported
separately.

## Rollback

First activate the entry stop. Allow any already journaled recovery to finish,
then verify venue state and scale the GKE Deployment to zero through an approved
GitHub Actions recovery change. Only after GKE is stopped and has no unresolved
ownership may the rollback VM or another runtime be started. Rollback never
reuses a Rails identity and never restores from a deleted journal.

# Balance synchronization

The runtime keeps Binance and World Chain wallet balances in memory under the
same owner as market and strategy state. Neither Postgres nor ClickHouse is a
balance source.

## Binance

After startup account and commission hydration, a dedicated async task calls
the signed Spot account endpoint every `BALANCE_SYNC_INTERVAL_MS` (5 seconds by
default). It reuses one HTTP client and its connection pool. Only account
information and open orders are refreshed on the steady-state path;
commissions are not fetched again every interval. A failed request triggers one
clock resynchronization and retry, then emits a failure event and waits for the
next interval.

The account endpoint omits zero balances. The synchronizer therefore materializes
every configured pair asset and treats an omitted asset as exact decimal zero.
The snapshot records both free and locked amounts.

## World Chain wallet

The existing Alchemy WebSocket `newHeads` subscription is the trigger. After an
accepted canonical head, a dedicated task reads native ETH and batches all
configured ERC-20 `balanceOf` calls through the reusable HTTP JSON-RPC client.
Every query uses the exact block hash through EIP-1898, so the token values in a
snapshot cannot accidentally span two blocks.

Standard balance calls are intentionally not sent over WebSocket. Alchemy
recommends WebSockets for subscriptions and HTTP for ordinary JSON-RPC because
HTTP preserves status codes and can be load-balanced to a fast backend. A
separate EVM gRPC balance stream is not available for this World Chain path;
the existing `newHeads` subscription plus block-pinned HTTP batch is both
portable and reorg-aware.

References:

- [Alchemy `newHeads`](https://www.alchemy.com/docs/reference/newheads)
- [Alchemy WebSocket guidance](https://www.alchemy.com/docs/reference/subscription-api)
- [EIP-1898 block-hash state queries](https://eips.ethereum.org/EIPS/eip-1898)

## Readiness and failures

Production startup still requires successful initial Binance and wallet
snapshots so the in-memory inventory begins from known generations. Thereafter,
both latest balance generations must be no older than `BALANCE_MAX_AGE_MS`
(10 seconds by default). An older or missing generation changes
`RuntimePhase::Ready` to `Degraded`; a successful REST/account or canonical
wallet refresh restores it. A transient refresh failure retains the last known
snapshot until it crosses that boundary. Failures and successful snapshots go
through bounded background telemetry and do not block market-data processing.

Every concrete trade is admitted against the latest in-memory balances minus
exact active reservations. Insufficient available inventory rejects only that
plan. Open orders and locked balances do not close readiness: locked amounts
are recorded but only `free` inventory is available to admission. User Data
events update changed assets immediately; each full REST snapshot independently
reconciles them and clears the diagnostic User Data anomaly flag. User Data
connection status remains separately observable and is not a readiness gate.

`binance_balance_snapshot` records `inventory_correction_count` and the exact
per-asset before/REST values whenever reconciliation corrects the
User-Data-maintained inventory. This is the source for measuring whether the
five-second interval is sufficient.

The balance reader uses the configured public wallet address. In production,
the process also owns the isolated signer required by `full_live` DEX execution
and rebalancing; signing is outside the balance synchronization task.

## Rebalance planning

The first complete Binance and wallet snapshot is also the process-scoped
reference maximum for each token's production rebalance policy. With the v12
artifact, a location becomes deficient below 25% of that combined reference
inventory.
The planner then targets half of the latest combined balance, matching Rails,
and caps the transfer so the source remains above the same start limit.

Planning runs in the single state owner after balance application. A required,
pending, failed, or settling rebalance does not close the global trading
readiness gate. Each trade is admitted against currently available inventory
after exact reservations; insufficient balance for that plan rejects the plan.
The planner itself performs no network I/O or mutation in the engine event
path.

# Balance synchronization

The runtime keeps Binance and World Chain wallet balances in memory under the
same owner as market and strategy state. Neither Postgres nor ClickHouse is a
balance source.

## Binance

After startup account and commission hydration, a dedicated async task calls
the signed Spot account endpoint every `BALANCE_SYNC_INTERVAL_MS` (1 second by
default). It reuses one HTTP client and its connection pool. Only account
information is refreshed on the steady-state path; commissions are not fetched
again every second. A failed request triggers one clock resynchronization and
retry, then emits a failure event and waits for the next interval.

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

Both initial snapshots must succeed before the runtime can become `Ready`.
Thereafter, each snapshot must be younger than `BALANCE_MAX_AGE_MS` (5 seconds
by default), and the Binance account must remain a trade-enabled Spot account.
A transient failure retains the last known snapshot, but the runtime becomes
`Degraded` when that snapshot ages out. Failures and successful snapshots go
through bounded background telemetry and do not block market-data processing.

The observer accepts only `EVM_WALLET_ADDRESS`; it never loads a wallet private
key or signer.

## Rebalance planning

The first complete Binance and wallet snapshot is also the process-scoped
reference maximum for each token's paper rebalance policy. With the v3 artifact,
a location becomes deficient below 25% of that combined reference inventory.
The planner then targets half of the latest combined balance, matching Rails,
and caps the transfer so the source remains above the same start limit.

Planning runs in the single state owner after balance application. A required
action or planning error closes the readiness gate and emits bounded telemetry.
It never performs network I/O or mutation in the engine event path.

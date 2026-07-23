# Versioned domain configuration

Status: v4 read/paper default and separately gated v12 adaptive-live artifact implemented
Last reviewed: 2026-07-23

## Runtime boundary

The Rust service loads one immutable JSON artifact at startup. It never reads
Rails Postgres, Redis, or Rails APIs while running. The artifact is validated
before any market-data connection starts and its SHA-256 fingerprint is added
to runtime telemetry.

The default artifact is
`config/strategies/usdc-wld-world-chain.v4.json`. It starts from the Rails
production pair `id=3` at the source timestamp stored in the file and records
one deliberate Rust-clone divergence: both Binance market data and eventual
execution use Spot. The v7 artifact contains the same pair/economics, uses the
live Binance price tick, enables adaptive sizing in shadow with a 200 USDC cap,
and has the global and pair execution gates enabled. Selecting v7 alone cannot
trade: the production GKE deployment fixes runtime execution to `full_live`, and
signer/order journals, single-owner deployment, and startup health checks are
independently required. Earlier artifacts remain provenance for prior shadow
stages.

Production Postgres is an export-time source only. The ignored
`.env.production` may contain `ARB_BOT_DATABASE_URL` for operator-driven
comparisons, but that credential must not be copied to GCP Secret Manager or
attached to the production runtime.

## Captured behavior

The v4-v12 snapshots record:

- World Chain `chain_id=480`, V3 Factory, V4 PoolManager/StateView, Quoters,
  routers, and other public contract addresses;
- USDC as token A and WLD as token B, with base-unit decimals;
- Binance Spot `WLDUSDC` market data and eventual Spot execution, with exact
  step/tick size;
- fixed 20 USDC detector/control notional; v10-v12 execute adaptive whole-step
  sizing from sequence-matched depth, capped recent depth, or a 40 USDC-capped
  top-only book up to the global 200 USDC cap, while retaining immediate
  bookTicker admission for a threshold-clearing baseline;
- token-B quote sizing derived from the latest Binance ask, matching
  `UpdateMinBuyAmountJob` without its database update loop;
- opportunity capacity expressed as whole Binance token-B steps, starting at
  that derived baseline and bounded by DEX liquidity, the profit threshold,
  and observed top-of-book quantity;
- `profit_token_a`, 20 bps opportunity threshold, market-data liveness,
  slippage reserve, DEX fee reserve, and exact execution-envelope inventory
  reservations; v9-v12
  make this 20 bps spread the entry verdict independently of worst-case gas
  and recovery coverage;
- event-driven price content is separate from transport liveness: an unchanged
  top remains current while its connection generation has activity within
  `max_transport_silence_ms = 30000`;
- paper rebalance enablement and a 2500 bps start threshold derived from the
  process's initial combined inventory;
- the production Uniswap V3/V4 allowlist, fee tiers, and V4 pool configs.

Wallets, balances, bridge state, private keys, RPC URLs, and execution
credentials are deliberately absent. The artifact contains only environment
variable names for the Alchemy HTTP and WebSocket endpoints.

## Fail-closed validation

Startup rejects:

- unknown schema versions and unknown JSON fields;
- malformed Git revisions, symbols, environment names, addresses, decimals,
  base-unit integers, fee tiers, or basis-point values;
- duplicate pair IDs or Binance symbols;
- inconsistent token/Binance base and quote assets;
- enabled providers without required chain contracts/config;
- inconsistent global/pair execution gates, including execution without market
  data.

The committed v4 default has both execution gates false. The v12 artifact has
both true and is valid only for the explicitly confirmed GKE live path. Older
artifacts remain immutable release provenance and deserialize according to
their committed schemas.

## Refreshing the source data

Read the current active Rails pair through an explicit read-only transaction:

```bash
scripts/read-rails-pair-config WLDUSDC
```

The script prints only whitelisted non-secret configuration fields. Updating
the committed artifact remains a reviewed code change: copy the intended
values, update source timestamps/revision, run `cargo test`, and explicitly
update the expected SHA-256 test value.

The production query output is not consumed automatically because an unnoticed
Rails configuration change must not silently alter a running Rust strategy.

The pair was re-read from production on 2026-07-17 after its latest
`updated_at`. It still specifies pair `id=3`, World Chain `480`, active
`USDC/WLD`, Spot `WLDUSDC`, 20 USDC minimum buy amount, WLD step `0.1`, price
tick `0.001`, `profit_token_a`, and the V3/V4 provider set. The older Rails seed
value of 10 USDC is not authoritative; the versioned artifact follows the live
production row and records its exact update timestamp.

Binance advertises a WLDUSDC `PRICE_FILTER` tick of `0.0001`. The v7 live
artifact deliberately uses that venue tick instead of the coarser Rails
`0.001` value, so IOC protection no longer loses a full millitick to parity
rounding. Startup still requires the configured tick to be an exact integer
multiple of the live venue tick; a non-aligned future filter change is
fail-closed.

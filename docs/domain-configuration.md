# Versioned domain configuration

Status: implemented for the first read-only production pair snapshot
Last reviewed: 2026-07-15

## Runtime boundary

The Rust service loads one immutable JSON artifact at startup. It never reads
Rails Postgres, Redis, or Rails APIs while running. The artifact is validated
before any market-data connection starts and its SHA-256 fingerprint is added
to runtime telemetry.

The first artifact is
`config/strategies/usdc-wld-world-chain.v1.json`. It represents the active
Rails production pair `id=3` at the source timestamp stored in the file.

Production Postgres is an export-time source only. The ignored
`.env.production` may contain `ARB_BOT_DATABASE_URL` for operator-driven
comparisons, but that credential must not be copied to GCP Secret Manager or
attached to the Worker Pool.

## Captured behavior

The v1 snapshot records:

- World Chain `chain_id=480` and public contract addresses;
- USDC as token A and WLD as token B, with base-unit decimals;
- Binance `WLDUSDC`, exact step/tick size, USD-M Futures market data, and Spot
  execution parity with Rails;
- fixed 20 USDC quote notional;
- token-B quote sizing derived from the latest Binance ask, matching
  `UpdateMinBuyAmountJob` without its database update loop;
- `profit_token_a`, 20 bps opportunity threshold, quote age, slippage reserve,
  DEX fee reserve, and balance safety multiplier;
- the production Uniswap V3/V4 allowlist, fee tiers, and V4 pool configs.

Wallets, balances, bridge state, private keys, RPC URLs, and execution
credentials are deliberately absent. The artifact contains only an environment
variable name for the future Alchemy endpoint.

## Fail-closed validation

Startup rejects:

- unknown schema versions and unknown JSON fields;
- malformed Git revisions, symbols, environment names, addresses, decimals,
  base-unit integers, fee tiers, or basis-point values;
- duplicate pair IDs or Binance symbols;
- inconsistent token/Binance base and quote assets;
- enabled providers without required chain contracts/config;
- every `live_trading_enabled=true` or pair `execution_enabled=true` value.

This binary is structurally unable to accept a live execution snapshot.

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

/*
Latest native-gas and Binance BNB balances for the operations dashboard.
Run with arb_bot_prod selected as the current database.
*/
SELECT
    resource_id,
    argMax(resource_kind, observed_at_ms) AS resource_kind,
    argMax(usage, observed_at_ms) AS usage,
    argMax(network_id, observed_at_ms) AS network_id,
    argMax(chain_id, observed_at_ms) AS chain_id,
    argMax(asset, observed_at_ms) AS asset,
    argMax(balance, observed_at_ms) AS balance,
    argMax(consumption_24h, observed_at_ms) AS consumption_24h,
    argMax(average_daily_consumption, observed_at_ms) AS average_daily_consumption,
    argMax(consumption_window_ms, observed_at_ms) AS consumption_window_ms,
    argMax(consumption_window_complete, observed_at_ms) AS consumption_window_complete,
    fromUnixTimestamp64Milli(max(observed_at_ms)) AS observed_at
FROM resource_balance_snapshots
WHERE observed_at_ms >= toUnixTimestamp64Milli(now64(3) - INTERVAL 2 DAY)
GROUP BY resource_id
ORDER BY resource_kind, network_id, resource_id;

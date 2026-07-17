BEGIN TRANSACTION READ ONLY;

SELECT
  'rails' AS runtime,
  count(*) AS completed_trades,
  count(*) FILTER (WHERE estimated_profit >= 0) AS profitable_trades,
  round(coalesce(sum(estimated_profit), 0), 6) AS comparable_usdc,
  round(coalesce(avg(estimated_profit), 0), 6) AS avg_comparable_usdc,
  round(coalesce(sum(abs(token_b_balance_change)), 0), 10) AS absolute_wld_residual
FROM arbitrage_results
WHERE trading_pair_id = 3
  AND arbitrage_strategy = 1
  AND created_at >= :'start_utc'::timestamptz
  AND created_at < :'end_utc'::timestamptz;

ROLLBACK;

WITH
    toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}, 3, 'UTC')) AS start_ms,
    toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}, 3, 'UTC')) AS end_ms
SELECT
    JSONExtractString(payload_json, 'engine_id') AS engine_id,
    kind,
    JSONExtractString(payload_json, 'strategy_id') AS strategy_id,
    JSONExtractString(payload_json, 'instrument_id') AS instrument_id,
    JSONExtractString(payload_json, 'network_id') AS network_id,
    count() AS records,
    uniqExactIf(JSONExtractString(payload_json, 'plan_id'), JSONExtractString(payload_json, 'plan_id') != '') AS plans,
    max(JSONExtractUInt(payload_json, 'hot_telemetry_dropped_records')) AS maximum_hot_telemetry_drops
FROM runtime_telemetry
WHERE observed_at_ms >= start_ms
  AND observed_at_ms < end_ms
  AND kind IN (
      'runtime_starting',
      'binance_book_ticker',
      'arbitrage_evaluation',
      'arbitrage_opportunity',
      'arbitrage_admitted',
      'arbitrage_execution_pending_discarded',
      'arbitrage_entry_preflight_rejected',
      'arbitrage_result',
      'binance_price_health',
      'decision_owner_health'
  )
GROUP BY engine_id, kind, strategy_id, instrument_id, network_id
ORDER BY engine_id, kind, strategy_id, instrument_id, network_id
FORMAT TabSeparatedWithNames

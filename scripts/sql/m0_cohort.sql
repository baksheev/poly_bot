WITH
    toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}, 3, 'UTC')) AS start_ms,
    toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}, 3, 'UTC')) AS end_ms,
    samples AS
    (
        SELECT
            JSONExtractString(payload_json, 'engine_id') AS engine_id,
            JSONExtractString(payload_json, 'symbol') AS symbol,
            kind,
            JSONExtractString(payload_json, 'strategy_id') AS strategy_id,
            JSONExtractString(payload_json, 'instrument_id') AS instrument_id,
            JSONExtractUInt(payload_json, 'hot_telemetry_dropped_records') AS hot_drops
        FROM runtime_telemetry
        WHERE observed_at_ms >= start_ms
          AND observed_at_ms < end_ms
          AND (
              (
                  kind = 'binance_book_ticker'
                  AND JSONExtractString(payload_json, 'feed_role') IN ('strategy', 'strategy_price')
              )
              OR kind IN (
                  'arbitrage_evaluation',
                  'arbitrage_adaptive_sizing_evaluated',
                  'decision_owner_health'
              )
          )
    ),
    per_symbol AS
    (
        SELECT
            engine_id,
            symbol,
            countIf(kind = 'binance_book_ticker') AS strategy_frames,
            countIf(kind = 'arbitrage_adaptive_sizing_evaluated') AS adaptive_tasks,
            anyIf(strategy_id, strategy_id != '') AS strategy_id,
            anyIf(instrument_id, instrument_id != '') AS instrument_id
        FROM samples
        WHERE symbol != ''
        GROUP BY engine_id, symbol
    ),
    per_engine_health AS
    (
        SELECT
            engine_id,
            max(hot_drops) AS maximum_hot_telemetry_drops
        FROM samples
        WHERE kind = 'decision_owner_health'
        GROUP BY engine_id
    )
SELECT
    per_symbol.engine_id AS engine_id,
    per_symbol.symbol AS symbol,
    per_symbol.strategy_id AS strategy_id,
    per_symbol.instrument_id AS instrument_id,
    per_symbol.strategy_frames AS strategy_frames,
    per_symbol.adaptive_tasks AS adaptive_tasks,
    coalesce(per_engine_health.maximum_hot_telemetry_drops, 0)
        AS maximum_hot_telemetry_drops,
    multiIf(
        per_symbol.symbol != 'WLDUSDC', 'informational',
        per_symbol.strategy_frames >= 100000
            AND per_symbol.adaptive_tasks >= 1000
            AND coalesce(per_engine_health.maximum_hot_telemetry_drops, 0) = 0,
            'ready',
        'collecting'
    ) AS m0_gate
FROM per_symbol
LEFT JOIN per_engine_health USING (engine_id)
ORDER BY per_symbol.engine_id, per_symbol.symbol
FORMAT TabSeparatedWithNames

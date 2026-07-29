WITH
    toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}, 3, 'UTC')) AS start_ms,
    toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}, 3, 'UTC')) AS end_ms,
    samples AS
    (
        SELECT
            JSONExtractString(payload_json, 'engine_id') AS engine_id,
            arrayJoin([
                if(kind = 'binance_book_ticker' AND JSONExtractString(payload_json, 'feed_role') IN ('strategy', 'strategy_price'),
                    tuple('binance_json_parse', JSONExtractUInt(payload_json, 'parse_time_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'binance_book_ticker' AND JSONExtractString(payload_json, 'feed_role') IN ('strategy', 'strategy_price'),
                    tuple('socket_to_decision', JSONExtractUInt(payload_json, 'decision_complete_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'arbitrage_evaluation' AND JSONExtractString(payload_json, 'evaluation_trigger') = 'binance',
                    tuple('baseline_calculation', JSONExtractUInt(payload_json, 'calculation_time_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'arbitrage_evaluation' AND JSONExtractString(payload_json, 'evaluation_trigger') = 'binance',
                    tuple('receipt_to_evaluation', JSONExtractUInt(payload_json, 'decision_latency_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'binance_depth_applied',
                    tuple('depth_parse_apply', JSONExtractUInt(payload_json, 'parse_apply_time_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'dex_pool_event',
                    tuple('dex_event_receive_to_owner', JSONExtractUInt(payload_json, 'engine_queue_age_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'world_chain_head',
                    tuple('head_receive_to_owner', JSONExtractUInt(payload_json, 'engine_queue_age_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'dex_pool_prepared',
                    tuple('prepared_curve_total', JSONExtractUInt(payload_json, 'total_time_us')),
                    tuple('', toUInt64(0)))
            ]) AS sample
        FROM runtime_telemetry
        WHERE observed_at_ms >= start_ms
          AND observed_at_ms < end_ms
          AND kind IN (
              'binance_book_ticker',
              'arbitrage_evaluation',
              'binance_depth_applied',
              'dex_pool_event',
              'world_chain_head',
              'dex_pool_prepared'
          )
    )
SELECT
    engine_id,
    tupleElement(sample, 1) AS stage,
    count() AS n,
    quantileExact(0.50)(tupleElement(sample, 2)) AS p50_us,
    quantileExact(0.95)(tupleElement(sample, 2)) AS p95_us,
    quantileExact(0.99)(tupleElement(sample, 2)) AS p99_us,
    max(tupleElement(sample, 2)) AS max_us
FROM samples
WHERE stage != ''
GROUP BY engine_id, stage
ORDER BY engine_id, stage
FORMAT TabSeparatedWithNames

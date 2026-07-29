WITH
    toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}, 3, 'UTC')) AS start_ms,
    toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}, 3, 'UTC')) AS end_ms
SELECT
    JSONExtractString(payload_json, 'engine_id') AS engine_id,
    JSONExtractString(payload_json, 'venue') AS venue,
    JSONExtractString(payload_json, 'stage') AS stage,
    count() AS n,
    quantileExact(0.50)(JSONExtractUInt(payload_json, 'duration_us')) AS p50_us,
    quantileExact(0.95)(JSONExtractUInt(payload_json, 'duration_us')) AS p95_us,
    quantileExact(0.99)(JSONExtractUInt(payload_json, 'duration_us')) AS p99_us,
    max(JSONExtractUInt(payload_json, 'duration_us')) AS max_us,
    max(JSONExtractUInt(payload_json, 'queue_depth_before_enqueue')) AS max_queue_depth_before_enqueue
FROM runtime_telemetry
WHERE observed_at_ms >= start_ms
  AND observed_at_ms < end_ms
  AND kind = 'arbitrage_execution_stage'
GROUP BY engine_id, venue, stage
ORDER BY engine_id, venue, stage
FORMAT TabSeparatedWithNames

WITH shared AS
(
    SELECT
        JSONExtractString(payload_json, 'engine_id') AS engine_id,
        JSONExtractString(payload_json, 'symbol') AS symbol,
        JSONExtractString(payload_json, 'event_kind') AS event_kind,
        JSONExtractBool(payload_json, 'execution_enabled') AS execution_enabled,
        JSONExtractBool(payload_json, 'direct_owner_poll') AS direct_owner_poll,
        JSONExtractUInt(payload_json, 'generation') AS generation,
        JSONExtractUInt(payload_json, 'parse_time_us') AS parse_time_us,
        JSONExtractUInt(payload_json, 'wire_frame_size_bytes') AS wire_frame_size_bytes
    FROM runtime_telemetry
    WHERE kind = 'binance_shared_stream_event'
      AND observed_at_ms >= toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}))
      AND observed_at_ms < toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}))
)
SELECT
    engine_id,
    symbol,
    event_kind,
    execution_enabled,
    direct_owner_poll,
    generation,
    count() AS frames,
    quantileExact(0.99)(parse_time_us) AS parse_p99_us,
    max(parse_time_us) AS parse_max_us,
    max(wire_frame_size_bytes) AS wire_max_bytes
FROM shared
GROUP BY
    engine_id,
    symbol,
    event_kind,
    execution_enabled,
    direct_owner_poll,
    generation
ORDER BY
    engine_id,
    symbol,
    event_kind,
    generation
FORMAT PrettyCompactMonoBlock

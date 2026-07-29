WITH
    toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}, 3, 'UTC')) AS start_ms,
    toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}, 3, 'UTC')) AS end_ms,
    samples AS
    (
        SELECT
            JSONExtractString(payload_json, 'engine_id') AS engine_id,
            JSONExtractString(payload_json, 'pair_id') AS pair_id,
            JSONExtractString(payload_json, 'strategy_id') AS strategy_id,
            JSONExtractString(payload_json, 'network_id') AS network_id,
            JSONExtractString(payload_json, 'pool_id') AS pool_id,
            JSONExtractString(payload_json, 'identity') AS identity,
            arrayJoin([
                if(kind = 'dex_pool_event',
                    tuple(
                        'dex_event_receive_to_owner',
                        JSONExtractUInt(payload_json, 'engine_queue_age_us'),
                        false,
                        toUInt64(0),
                        toUInt64(0),
                        toUInt64(0)
                    ),
                    tuple('', toUInt64(0), false, toUInt64(0), toUInt64(0), toUInt64(0))),
                if(kind = 'dex_pool_prepared',
                    tuple(
                        'prepared_curve_build',
                        JSONExtractUInt(payload_json, 'build_time_us'),
                        JSONExtractBool(payload_json, 'stage_timing_complete'),
                        JSONExtractUInt(payload_json, 'prepared_exact_output_segments'),
                        JSONExtractUInt(payload_json, 'prepared_exact_input_segments'),
                        JSONExtractUInt(payload_json, 'prepared_token_a_exact_input_segments')
                    ),
                    tuple('', toUInt64(0), false, toUInt64(0), toUInt64(0), toUInt64(0))),
                if(kind = 'dex_pool_prepared',
                    tuple(
                        'prepared_curve_total',
                        JSONExtractUInt(payload_json, 'total_time_us'),
                        JSONExtractBool(payload_json, 'stage_timing_complete'),
                        JSONExtractUInt(payload_json, 'prepared_exact_output_segments'),
                        JSONExtractUInt(payload_json, 'prepared_exact_input_segments'),
                        JSONExtractUInt(payload_json, 'prepared_token_a_exact_input_segments')
                    ),
                    tuple('', toUInt64(0), false, toUInt64(0), toUInt64(0), toUInt64(0)))
            ]) AS sample
        FROM runtime_telemetry
        WHERE observed_at_ms >= start_ms
          AND observed_at_ms < end_ms
          AND kind IN ('dex_pool_event', 'dex_pool_prepared')
    )
SELECT
    engine_id,
    pair_id,
    strategy_id,
    network_id,
    pool_id,
    identity,
    tupleElement(sample, 1) AS stage,
    count() AS n,
    quantileExact(0.50)(tupleElement(sample, 2)) AS p50_us,
    quantileExact(0.95)(tupleElement(sample, 2)) AS p95_us,
    quantileExact(0.99)(tupleElement(sample, 2)) AS p99_us,
    max(tupleElement(sample, 2)) AS max_us,
    countIf(tupleElement(sample, 3)) AS stage_timing_complete_records,
    max(tupleElement(sample, 4)) AS max_exact_output_segments,
    max(tupleElement(sample, 5)) AS max_exact_input_segments,
    max(tupleElement(sample, 6)) AS max_token_a_exact_input_segments
FROM samples
WHERE stage != ''
GROUP BY engine_id, pair_id, strategy_id, network_id, pool_id, identity, stage
ORDER BY engine_id, pair_id, network_id, pool_id, stage
FORMAT TabSeparatedWithNames

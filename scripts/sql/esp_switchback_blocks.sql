WITH
    toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}, 3, 'UTC')) AS start_ms,
    toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}, 3, 'UTC')) AS end_ms,
    assignments AS
    (
        SELECT
            JSONExtractString(payload_json, 'plan_id') AS plan_id,
            argMax(JSONExtractString(payload_json, 'execution_mode_assignment'), observed_at_ms)
                AS assigned_mode,
            argMax(JSONExtractUInt(payload_json, 'switchback_block_id'), observed_at_ms)
                AS block_id,
            argMax(JSONExtractUInt(payload_json, 'switchback_block_pair_id'), observed_at_ms)
                AS block_pair_id,
            argMax(JSONExtractUInt(payload_json, 'switchback_block_position'), observed_at_ms)
                AS block_position
        FROM runtime_telemetry
        WHERE kind = 'arbitrage_admitted'
          AND observed_at_ms >= start_ms
          AND observed_at_ms < end_ms
          AND JSONExtractString(payload_json, 'experiment_id')
              = 'esp-usdc-concurrent-full-live-v1'
          AND JSONExtractString(payload_json, 'experiment_enrollment_status') = 'enrolled'
        GROUP BY plan_id
    ),
    results AS
    (
        SELECT
            JSONExtractString(payload_json, 'plan_id') AS plan_id,
            argMax(
                toInt128OrZero(
                    JSONExtractString(payload_json, 'comparable_profit_token_a_base_units')
                ),
                observed_at_ms
            ) AS comparable_profit
        FROM runtime_telemetry
        WHERE kind = 'arbitrage_result'
          AND observed_at_ms >= start_ms
          AND observed_at_ms < end_ms + 3600000
          AND JSONExtractString(payload_json, 'pair_id') = 'arbitrum-usdc-esp'
        GROUP BY plan_id
    )
SELECT
    assignments.block_pair_id,
    assignments.block_id,
    assignments.block_position,
    assignments.assigned_mode,
    count() AS assigned_opportunities,
    countIf(notEmpty(results.plan_id)) AS terminal_results,
    round(sum(ifNull(results.comparable_profit, 0)) / 1000000, 6)
        AS intent_to_treat_comparable_usdc
FROM assignments
LEFT JOIN results USING (plan_id)
GROUP BY
    assignments.block_pair_id,
    assignments.block_id,
    assignments.block_position,
    assignments.assigned_mode
ORDER BY assignments.block_id
FORMAT TabSeparatedWithNames

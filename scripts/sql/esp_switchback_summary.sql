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
                AS block_pair_id
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
            argMax(JSONExtractString(payload_json, 'execution_mode'), observed_at_ms)
                AS executed_mode,
            argMax(
                toInt128OrZero(
                    JSONExtractString(payload_json, 'comparable_profit_token_a_base_units')
                ),
                observed_at_ms
            ) AS comparable_profit,
            argMax(
                JSONExtractString(JSONExtractRaw(payload_json, 'dex'), 'status'),
                observed_at_ms
            ) AS dex_status,
            argMax(
                JSONExtractString(JSONExtractRaw(payload_json, 'cex'), 'status'),
                observed_at_ms
            ) AS cex_status,
            argMax(length(JSONExtractArrayRaw(payload_json, 'recoveries')), observed_at_ms)
                AS recovery_count
        FROM runtime_telemetry
        WHERE kind = 'arbitrage_result'
          AND observed_at_ms >= start_ms
          AND observed_at_ms < end_ms + 3600000
          AND JSONExtractString(payload_json, 'pair_id') = 'arbitrum-usdc-esp'
        GROUP BY plan_id
    )
SELECT
    assignments.assigned_mode,
    count() AS assigned_opportunities,
    countIf(notEmpty(results.plan_id)) AS terminal_results,
    countIf(empty(results.plan_id)) AS assigned_zero_execution_rows,
    countIf(
        notEmpty(results.plan_id)
        AND results.executed_mode != assignments.assigned_mode
    ) AS assignment_mode_mismatches,
    countIf(notEmpty(results.plan_id) AND results.comparable_profit < 0) AS losing_results,
    countIf(notEmpty(results.plan_id) AND results.dex_status = 'failed') AS dex_failures,
    countIf(notEmpty(results.plan_id) AND results.cex_status = 'failed') AS primary_cex_failures,
    countIf(notEmpty(results.plan_id) AND results.recovery_count > 0) AS recovery_results,
    round(sum(ifNull(results.comparable_profit, 0)) / 1000000, 6)
        AS intent_to_treat_comparable_usdc,
    round(
        avgIf(results.comparable_profit, notEmpty(results.plan_id)) / 1000000,
        6
    ) AS terminal_average_comparable_usdc,
    uniqExact(assignments.block_id) AS enrolled_blocks,
    uniqExact(assignments.block_pair_id) AS enrolled_block_pairs
FROM assignments
LEFT JOIN results USING (plan_id)
GROUP BY assignments.assigned_mode
ORDER BY assignments.assigned_mode
FORMAT TabSeparatedWithNames

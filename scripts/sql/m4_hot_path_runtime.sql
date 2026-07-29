WITH events AS
(
    SELECT
        JSONExtractString(payload_json, 'engine_id') AS engine_id,
        JSONExtractString(payload_json, 'strategy_id') AS strategy_id,
        kind,
        multiIf(
            kind = 'arbitrage_evaluation', 'baseline_calculation',
            kind = 'strategy_sizing_task', 'exhaustive_sizing',
            kind = 'strategy_calculation_overload', 'sizing_overload',
            kind = 'coordinator_shadow_candidate', 'coordinator_shadow_candidate',
            'unknown'
        ) AS stage,
        multiIf(
            kind = 'arbitrage_evaluation',
                JSONExtractUInt(payload_json, 'calculation_time_us'),
            kind = 'strategy_sizing_task',
                JSONExtractUInt(payload_json, 'worker_time_us'),
            toUInt64(0)
        ) AS work_us,
        multiIf(
            kind = 'strategy_sizing_task',
                JSONExtractUInt(payload_json, 'queue_time_us'),
            toUInt64(0)
        ) AS queue_us,
        JSONExtractUInt(payload_json, 'calculation_budget_us') AS budget_us,
        JSONExtractBool(payload_json, 'calculation_budget_exceeded') AS budget_exceeded,
        JSONExtractBool(payload_json, 'replaced_pending_snapshot') AS replaced_pending_snapshot,
        JSONExtractString(payload_json, 'disposition') AS disposition,
        JSONExtractString(payload_json, 'sink_mode') AS sink_mode,
        JSONExtractBool(payload_json, 'external_mutation_authorized') AS mutation_authorized
    FROM runtime_telemetry
    WHERE kind IN
    (
        'arbitrage_evaluation',
        'strategy_sizing_task',
        'strategy_calculation_overload',
        'coordinator_shadow_candidate'
    )
      AND observed_at_ms >= toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}))
      AND observed_at_ms < toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}))
)
SELECT
    engine_id,
    strategy_id,
    stage,
    count() AS records,
    countIf(budget_exceeded) AS budget_exceeded_records,
    countIf(replaced_pending_snapshot) AS replaced_pending_records,
    countIf(disposition = 'superseded') AS superseded_records,
    countIf(sink_mode = 'non_mutating' AND NOT mutation_authorized)
        AS proven_non_mutating_candidates,
    max(budget_us) AS calculation_budget_us,
    quantileExact(0.50)(work_us) AS work_p50_us,
    quantileExact(0.95)(work_us) AS work_p95_us,
    quantileExact(0.99)(work_us) AS work_p99_us,
    max(work_us) AS work_max_us,
    quantileExact(0.95)(queue_us) AS queue_p95_us,
    quantileExact(0.99)(queue_us) AS queue_p99_us,
    max(queue_us) AS queue_max_us
FROM events
WHERE stage != 'unknown'
GROUP BY
    engine_id,
    strategy_id,
    stage
ORDER BY
    engine_id,
    strategy_id,
    stage
FORMAT PrettyCompactMonoBlock

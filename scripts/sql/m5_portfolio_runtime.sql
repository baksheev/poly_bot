WITH events AS
(
    SELECT
        JSONExtractString(payload_json, 'engine_id') AS engine_id,
        kind,
        multiIf(
            kind = 'portfolio_capital_allocator_evaluated', 'portfolio_allocator',
            kind = 'capital_allocation_evaluated', 'v12_parity_adapter',
            kind = 'arbitrage_admitted', 'trade_reservation',
            'unknown'
        ) AS stage,
        multiIf(
            kind = 'portfolio_capital_allocator_evaluated',
                JSONExtractUInt(payload_json, 'allocator_calculation_validation_us'),
            kind = 'capital_allocation_evaluated',
                JSONExtractUInt(payload_json, 'calculation_validation_us'),
            kind = 'arbitrage_admitted',
                JSONExtractUInt(payload_json, 'inventory_reservation_us'),
            toUInt64(0)
        ) AS duration_us,
        JSONExtractUInt(payload_json, 'scheduler_queue_us') AS scheduler_queue_us,
        JSONExtractUInt(payload_json, 'portfolio_snapshot_us') AS portfolio_snapshot_us,
        JSONExtractUInt(payload_json, 'reservation_snapshot_us') AS reservation_snapshot_us,
        JSONExtractString(payload_json, 'outcome') AS outcome,
        JSONExtractBool(payload_json, 'conservation_checked') AS conservation_checked,
        JSONExtractBool(payload_json, 'external_mutation_authorized') AS mutation_authorized
    FROM runtime_telemetry
    WHERE kind IN
    (
        'portfolio_capital_allocator_evaluated',
        'capital_allocation_evaluated',
        'arbitrage_admitted'
    )
      AND observed_at_ms >= toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}))
      AND observed_at_ms < toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}))
)
SELECT
    engine_id,
    stage,
    count() AS records,
    countIf(outcome = 'failed') AS failed_records,
    countIf(stage = 'portfolio_allocator' AND conservation_checked)
        AS conservation_checked_records,
    countIf(mutation_authorized) AS mutation_authorized_records,
    quantileExact(0.50)(duration_us) AS duration_p50_us,
    quantileExact(0.95)(duration_us) AS duration_p95_us,
    quantileExact(0.99)(duration_us) AS duration_p99_us,
    max(duration_us) AS duration_max_us,
    max(scheduler_queue_us) AS scheduler_queue_max_us,
    max(portfolio_snapshot_us) AS portfolio_snapshot_max_us,
    max(reservation_snapshot_us) AS reservation_snapshot_max_us
FROM events
WHERE stage != 'unknown'
GROUP BY
    engine_id,
    stage
ORDER BY
    engine_id,
    stage
FORMAT PrettyCompactMonoBlock

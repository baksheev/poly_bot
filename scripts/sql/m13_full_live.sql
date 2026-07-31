WITH risk AS
(
    SELECT
        observed_at_ms,
        JSONExtractString(payload_json, 'engine_id') AS engine_id,
        JSONExtractUInt(payload_json, 'transfer_count') AS transfer_count,
        JSONExtractUInt(payload_json, 'active_transfer_count') AS active_transfer_count,
        JSONExtractUInt(payload_json, 'failed_transfer_count') AS failed_transfer_count,
        JSONExtractString(payload_json, 'outcome') AS outcome
    FROM runtime_telemetry
    WHERE kind = 'm10_rebalance_risk_snapshot'
      AND observed_at_ms >= toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}))
      AND observed_at_ms < toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}))
      AND startsWith(JSONExtractString(payload_json, 'engine_id'), 'arb-bot-rust-shadow-gke-')
      AND JSONExtractString(payload_json, 'approval_session_id')
          = 'esp-usdc-arbitrum-full-live'
),
latest AS
(
    SELECT
        engine_id,
        argMax(transfer_count, observed_at_ms) AS transfer_count,
        argMax(active_transfer_count, observed_at_ms) AS active_transfer_count,
        argMax(failed_transfer_count, observed_at_ms) AS failed_transfer_count,
        countIf(outcome != 'success') AS risk_snapshot_failures
    FROM risk
    GROUP BY engine_id
),
allocator AS
(
    SELECT
        JSONExtractString(payload_json, 'engine_id') AS engine_id,
        count() AS allocator_plans,
        countIf(JSONExtractString(payload_json, 'allocator_mode') = 'full_live')
            AS full_live_allocator_plans,
        countIf(JSONExtractBool(payload_json, 'external_mutation_authorized'))
            AS authorized_allocator_plans,
        countIf(JSONExtractString(payload_json, 'outcome') != 'success')
            AS allocator_failures,
        quantileExact(0.99)(JSONExtractUInt(payload_json, 'scheduler_queue_us'))
            AS allocator_queue_p99_us,
        max(JSONExtractUInt(payload_json, 'scheduler_queue_us'))
            AS allocator_queue_max_us,
        quantileExact(0.99)(
            JSONExtractUInt(payload_json, 'allocator_calculation_validation_us')
        ) AS allocator_calculation_p99_us,
        max(JSONExtractUInt(payload_json, 'allocator_calculation_validation_us'))
            AS allocator_calculation_max_us
    FROM runtime_telemetry
    WHERE kind = 'portfolio_capital_allocator_planned'
      AND observed_at_ms >= toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}))
      AND observed_at_ms < toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}))
    GROUP BY engine_id
),
sagas AS
(
    SELECT
        JSONExtractString(payload_json, 'engine_id') AS engine_id,
        count() AS saga_count,
        countIf(JSONExtractString(payload_json, 'outcome') != 'success') AS saga_failures,
        countIf(
            (JSONExtractString(payload_json, 'token') = 'USDC'
                AND (
                    toUInt256OrZero(JSONExtractString(payload_json, 'amount_base_units'))
                        > toUInt256('2600000000')
                    OR toUInt256OrZero(JSONExtractString(payload_json, 'maximum_fee_base_units'))
                        > toUInt256('5000000')
                ))
            OR (JSONExtractString(payload_json, 'token') = 'ESP'
                AND (
                    toUInt256OrZero(JSONExtractString(payload_json, 'amount_base_units'))
                        > toUInt256('10000000000000000000000')
                    OR toUInt256OrZero(JSONExtractString(payload_json, 'maximum_fee_base_units'))
                        > toUInt256('2000000000000000000')
                ))
            OR JSONExtractString(payload_json, 'token') NOT IN ('USDC', 'ESP')
        ) AS per_operation_limit_breaches,
        quantileExact(0.99)(JSONExtractUInt(payload_json, 'saga_duration_us'))
            AS saga_p99_us,
        max(JSONExtractUInt(payload_json, 'saga_duration_us')) AS saga_max_us
    FROM runtime_telemetry
    WHERE kind = 'm10_rebalance_saga'
      AND observed_at_ms >= toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}))
      AND observed_at_ms < toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}))
      AND JSONExtractString(payload_json, 'strategy_id')
          = 'rebalance-arbitrum-usdc-esp-m10'
      AND JSONExtractString(payload_json, 'approval_session_id')
          = 'esp-usdc-arbitrum-full-live'
    GROUP BY engine_id
),
children AS
(
    SELECT
        JSONExtractString(payload_json, 'engine_id') AS engine_id,
        count() AS binance_capital_children,
        countIf(JSONExtractString(payload_json, 'outcome') != 'success')
            AS binance_capital_child_failures,
        quantileExact(0.99)(JSONExtractUInt(payload_json, 'duration_us'))
            AS binance_capital_child_p99_us,
        max(JSONExtractUInt(payload_json, 'duration_us'))
            AS binance_capital_child_max_us
    FROM runtime_telemetry
    WHERE kind = 'm10_rebalance_child'
      AND observed_at_ms >= toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}))
      AND observed_at_ms < toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}))
      AND JSONExtractString(payload_json, 'strategy_id')
          = 'rebalance-arbitrum-usdc-esp-m10'
      AND JSONExtractString(payload_json, 'approval_session_id')
          = 'esp-usdc-arbitrum-full-live'
    GROUP BY engine_id
)
SELECT
    latest.engine_id,
    transfer_count,
    active_transfer_count,
    failed_transfer_count,
    risk_snapshot_failures,
    ifNull(allocator.allocator_plans, 0) AS allocator_plans,
    ifNull(allocator.full_live_allocator_plans, 0) AS full_live_allocator_plans,
    ifNull(allocator.authorized_allocator_plans, 0) AS authorized_allocator_plans,
    ifNull(allocator.allocator_failures, 0) AS allocator_failures,
    ifNull(allocator.allocator_queue_p99_us, 0) AS allocator_queue_p99_us,
    ifNull(allocator.allocator_queue_max_us, 0) AS allocator_queue_max_us,
    ifNull(allocator.allocator_calculation_p99_us, 0) AS allocator_calculation_p99_us,
    ifNull(allocator.allocator_calculation_max_us, 0) AS allocator_calculation_max_us,
    ifNull(sagas.saga_count, 0) AS saga_count,
    ifNull(sagas.saga_failures, 0) AS saga_failures,
    ifNull(sagas.per_operation_limit_breaches, 0) AS per_operation_limit_breaches,
    ifNull(sagas.saga_p99_us, 0) AS saga_p99_us,
    ifNull(sagas.saga_max_us, 0) AS saga_max_us,
    ifNull(children.binance_capital_children, 0) AS binance_capital_children,
    ifNull(children.binance_capital_child_failures, 0)
        AS binance_capital_child_failures,
    ifNull(children.binance_capital_child_p99_us, 0)
        AS binance_capital_child_p99_us,
    ifNull(children.binance_capital_child_max_us, 0)
        AS binance_capital_child_max_us,
    multiIf(
        risk_snapshot_failures != 0
            OR active_transfer_count > 1
            OR allocator_failures != 0
            OR saga_failures != 0
            OR per_operation_limit_breaches != 0
            OR binance_capital_child_failures != 0,
        'limit_or_execution_breach',
        active_transfer_count = 1,
        'active',
        saga_count = 0,
        'armed',
        'full_live_observed'
    ) AS m13_gate
FROM latest
LEFT JOIN allocator USING (engine_id)
LEFT JOIN sagas USING (engine_id)
LEFT JOIN children USING (engine_id)
ORDER BY engine_id
FORMAT PrettyCompactMonoBlock

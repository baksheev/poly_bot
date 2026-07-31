WITH risk AS
(
    SELECT
        observed_at_ms,
        JSONExtractString(payload_json, 'engine_id') AS engine_id,
        JSONExtractUInt(payload_json, 'transfer_count') AS transfer_count,
        JSONExtractUInt(payload_json, 'active_transfer_count') AS active_transfer_count,
        JSONExtractUInt(payload_json, 'failed_transfer_count') AS failed_transfer_count,
        toUInt256OrZero(JSONExtractString(payload_json, 'token_a_debit')) AS token_a_debit,
        toUInt256OrZero(JSONExtractString(payload_json, 'token_b_debit')) AS token_b_debit,
        toUInt256OrZero(JSONExtractString(payload_json, 'token_a_maximum_fee'))
            AS token_a_maximum_fee,
        toUInt256OrZero(JSONExtractString(payload_json, 'token_b_maximum_fee'))
            AS token_b_maximum_fee,
        JSONExtractString(payload_json, 'outcome') AS outcome
    FROM runtime_telemetry
    WHERE kind = 'm10_rebalance_risk_snapshot'
      AND observed_at_ms >= toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}))
      AND observed_at_ms < toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}))
      AND startsWith(JSONExtractString(payload_json, 'engine_id'), 'arb-bot-rust-shadow-gke-')
      AND JSONExtractString(payload_json, 'approval_session_id')
          = 'esp-usdc-arbitrum-rebalance-20260731-r2'
),
allocator AS
(
    SELECT
        JSONExtractString(payload_json, 'engine_id') AS engine_id,
        count() AS allocator_plans,
        countIf(JSONExtractString(payload_json, 'allocator_mode') = 'live_canary')
            AS live_allocator_plans,
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
        groupUniqArrayIf(
            JSONExtractString(payload_json, 'operation_id'),
            JSONExtractString(payload_json, 'operation_id') != ''
        ) AS operation_ids,
        count() AS saga_count,
        countIf(JSONExtractString(payload_json, 'outcome') != 'success') AS saga_failures,
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
          = 'esp-usdc-arbitrum-rebalance-20260731-r2'
    GROUP BY engine_id
),
binance_children AS
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
          = 'esp-usdc-arbitrum-rebalance-20260731-r2'
      AND JSONExtractString(payload_json, 'owner') = 'binance_capital'
    GROUP BY engine_id
),
saga_operations AS
(
    SELECT
        engine_id,
        arrayJoin(operation_ids) AS operation_id
    FROM sagas
),
evm AS
(
    SELECT
        JSONExtractString(stage.payload_json, 'engine_id') AS engine_id,
        countIf(JSONExtractString(stage.payload_json, 'stage') = 'capital_worker_queue')
            AS evm_capital_children,
        quantileExactIf(0.99)(
            JSONExtractUInt(stage.payload_json, 'duration_us'),
            JSONExtractString(stage.payload_json, 'stage') = 'capital_worker_queue'
        ) AS evm_queue_p99_us,
        maxIf(
            JSONExtractUInt(stage.payload_json, 'duration_us'),
            JSONExtractString(stage.payload_json, 'stage') = 'capital_worker_queue'
        ) AS evm_queue_max_us,
        quantileExactIf(0.99)(
            JSONExtractUInt(stage.payload_json, 'duration_us'),
            JSONExtractString(stage.payload_json, 'stage') = 'broadcast_rpc'
        ) AS evm_provider_p99_us,
        maxIf(
            JSONExtractUInt(stage.payload_json, 'duration_us'),
            JSONExtractString(stage.payload_json, 'stage') = 'broadcast_rpc'
        ) AS evm_provider_max_us,
        quantileExactIf(0.99)(
            JSONExtractUInt(stage.payload_json, 'duration_us'),
            JSONExtractString(stage.payload_json, 'stage') = 'confirmation_rpc'
        ) AS evm_receipt_p99_us,
        maxIf(
            JSONExtractUInt(stage.payload_json, 'duration_us'),
            JSONExtractString(stage.payload_json, 'stage') = 'confirmation_rpc'
        ) AS evm_receipt_max_us,
        countIf(JSONExtractString(stage.payload_json, 'outcome') != 'success')
            AS evm_stage_failures
    FROM runtime_telemetry AS stage
    INNER JOIN saga_operations AS operation
        ON operation.engine_id = JSONExtractString(stage.payload_json, 'engine_id')
    WHERE stage.kind = 'arbitrage_execution_stage'
      AND startsWith(
          JSONExtractString(stage.payload_json, 'operation_id'),
          concat(operation.operation_id, ':')
      )
      AND stage.observed_at_ms
          >= toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}))
      AND stage.observed_at_ms
          < toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}))
    GROUP BY engine_id
),
settlement AS
(
    SELECT
        JSONExtractString(stage.payload_json, 'engine_id') AS engine_id,
        count() AS settlement_count,
        quantileExact(0.99)(JSONExtractUInt(stage.payload_json, 'settlement_duration_us'))
            AS settlement_p99_us,
        max(JSONExtractUInt(stage.payload_json, 'settlement_duration_us'))
            AS settlement_max_us
    FROM runtime_telemetry AS stage
    INNER JOIN saga_operations AS operation
        ON operation.engine_id = JSONExtractString(stage.payload_json, 'engine_id')
       AND operation.operation_id = JSONExtractString(stage.payload_json, 'operation_id')
    WHERE stage.kind = 'rebalance_settlement_reconciled'
      AND stage.observed_at_ms
          >= toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}))
      AND stage.observed_at_ms
          < toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}))
      AND JSONExtractString(stage.payload_json, 'strategy_id')
          = 'rebalance-arbitrum-usdc-esp-m10'
    GROUP BY engine_id
),
latest AS
(
    SELECT
        engine_id,
        argMax(transfer_count, observed_at_ms) AS transfer_count,
        argMax(active_transfer_count, observed_at_ms) AS active_transfer_count,
        argMax(failed_transfer_count, observed_at_ms) AS failed_transfer_count,
        argMax(token_a_debit, observed_at_ms) AS token_a_debit,
        argMax(token_b_debit, observed_at_ms) AS token_b_debit,
        argMax(token_a_maximum_fee, observed_at_ms) AS token_a_maximum_fee,
        argMax(token_b_maximum_fee, observed_at_ms) AS token_b_maximum_fee,
        countIf(outcome != 'success') AS risk_snapshot_failures
    FROM risk
    GROUP BY engine_id
)
SELECT
    latest.engine_id,
    transfer_count,
    active_transfer_count,
    failed_transfer_count,
    token_a_debit,
    token_b_debit,
    token_a_maximum_fee,
    token_b_maximum_fee,
    risk_snapshot_failures,
    ifNull(allocator.allocator_plans, 0) AS allocator_plans,
    ifNull(allocator.live_allocator_plans, 0) AS live_allocator_plans,
    ifNull(allocator.authorized_allocator_plans, 0) AS authorized_allocator_plans,
    ifNull(allocator.allocator_failures, 0) AS allocator_failures,
    ifNull(allocator.allocator_queue_p99_us, 0) AS allocator_queue_p99_us,
    ifNull(allocator.allocator_queue_max_us, 0) AS allocator_queue_max_us,
    ifNull(allocator.allocator_calculation_p99_us, 0) AS allocator_calculation_p99_us,
    ifNull(allocator.allocator_calculation_max_us, 0) AS allocator_calculation_max_us,
    ifNull(sagas.saga_count, 0) AS saga_count,
    ifNull(sagas.saga_failures, 0) AS saga_failures,
    ifNull(sagas.saga_p99_us, 0) AS saga_p99_us,
    ifNull(sagas.saga_max_us, 0) AS saga_max_us,
    ifNull(binance_children.binance_capital_children, 0) AS binance_capital_children,
    ifNull(binance_children.binance_capital_child_failures, 0)
        AS binance_capital_child_failures,
    ifNull(binance_children.binance_capital_child_p99_us, 0)
        AS binance_capital_child_p99_us,
    ifNull(binance_children.binance_capital_child_max_us, 0)
        AS binance_capital_child_max_us,
    ifNull(evm.evm_capital_children, 0) AS evm_capital_children,
    ifNull(evm.evm_queue_p99_us, 0) AS evm_queue_p99_us,
    ifNull(evm.evm_queue_max_us, 0) AS evm_queue_max_us,
    ifNull(evm.evm_provider_p99_us, 0) AS evm_provider_p99_us,
    ifNull(evm.evm_provider_max_us, 0) AS evm_provider_max_us,
    ifNull(evm.evm_receipt_p99_us, 0) AS evm_receipt_p99_us,
    ifNull(evm.evm_receipt_max_us, 0) AS evm_receipt_max_us,
    ifNull(evm.evm_stage_failures, 0) AS evm_stage_failures,
    ifNull(settlement.settlement_count, 0) AS settlement_count,
    ifNull(settlement.settlement_p99_us, 0) AS settlement_p99_us,
    ifNull(settlement.settlement_max_us, 0) AS settlement_max_us,
    multiIf(
        risk_snapshot_failures != 0
            OR transfer_count > 2
            OR active_transfer_count > 1
            OR failed_transfer_count > 1
            OR token_a_debit > toUInt256('2600000000')
            OR token_b_debit > toUInt256('10000000000000000000000')
            OR token_a_maximum_fee > toUInt256('5000000')
            OR token_b_maximum_fee > toUInt256('2000000000000000000')
            OR allocator_failures != 0
            OR saga_failures != 0
            OR binance_capital_child_failures != 0
            OR evm_stage_failures != 0,
        'limit_breach',
        transfer_count = 0,
        'armed',
        active_transfer_count = 1,
        'active',
        'canary_observed'
    ) AS m10_gate
FROM latest
LEFT JOIN allocator USING (engine_id)
LEFT JOIN sagas USING (engine_id)
LEFT JOIN binance_children USING (engine_id)
LEFT JOIN evm USING (engine_id)
LEFT JOIN settlement USING (engine_id)
ORDER BY engine_id
FORMAT PrettyCompactMonoBlock

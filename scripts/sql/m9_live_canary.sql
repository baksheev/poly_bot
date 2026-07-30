WITH readiness AS
(
    SELECT
        observed_at_ms,
        JSONExtractString(payload_json, 'engine_id') AS engine_id,
        JSONExtractString(payload_json, 'stage') AS stage,
        JSONExtractBool(payload_json, 'ready') AS ready,
        JSONExtractBool(payload_json, 'external_mutation_authorized') AS mutation_authorized,
        JSONExtractBool(payload_json, 'token_a_funded') AS token_a_funded,
        JSONExtractBool(payload_json, 'token_b_funded') AS token_b_funded,
        JSONExtractUInt(payload_json, 'request_count') AS request_count,
        JSONExtractUInt(payload_json, 'direct_route_count') AS direct_route_count
    FROM runtime_telemetry
    WHERE kind = 'm9_live_readiness'
      AND observed_at_ms >= toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}))
      AND observed_at_ms < toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}))
      AND startsWith(JSONExtractString(payload_json, 'engine_id'), 'arb-bot-rust-shadow-gke-')
),
readiness_latest AS
(
    SELECT
        engine_id,
        stage,
        argMax(ready, observed_at_ms) AS ready,
        argMax(mutation_authorized, observed_at_ms) AS mutation_authorized,
        argMax(token_a_funded, observed_at_ms) AS token_a_funded,
        argMax(token_b_funded, observed_at_ms) AS token_b_funded,
        argMax(request_count, observed_at_ms) AS request_count,
        argMax(direct_route_count, observed_at_ms) AS direct_route_count
    FROM readiness
    GROUP BY engine_id, stage
),
canary AS
(
    SELECT
        JSONExtractString(payload_json, 'engine_id') AS engine_id,
        JSONExtractString(payload_json, 'decision') AS decision,
        JSONExtractString(payload_json, 'plan_id') AS plan_id,
        JSONExtractUInt(payload_json, 'admitted_parent_count_after') AS admitted_parent_count,
        JSONExtractUInt(payload_json, 'admitted_parent_count_after')
            AS unique_admitted_parent_count,
        toUInt64OrZero(
            JSONExtractString(
                payload_json,
                'admitted_notional_token_a_base_units_after'
            )
        ) AS admitted_notional
    FROM runtime_telemetry
    WHERE kind = 'm9_canary_gate'
      AND observed_at_ms >= toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}))
      AND observed_at_ms < toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}))
      AND JSONExtractString(payload_json, 'pair_id') = 'arbitrum-usdc-esp'
),
canary_risk_snapshot AS
(
    SELECT
        JSONExtractString(payload_json, 'engine_id') AS engine_id,
        'snapshot' AS decision,
        '' AS plan_id,
        JSONExtractUInt(payload_json, 'admitted_parent_count') AS admitted_parent_count,
        JSONExtractUInt(payload_json, 'unique_admitted_parent_count')
            AS unique_admitted_parent_count,
        toUInt64OrZero(
            JSONExtractString(
                payload_json,
                'admitted_notional_token_a_base_units'
            )
        ) AS admitted_notional
    FROM runtime_telemetry
    WHERE kind = 'm9_canary_risk_snapshot'
      AND observed_at_ms >= toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}))
      AND observed_at_ms < toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}))
      AND JSONExtractString(payload_json, 'pair_id') = 'arbitrum-usdc-esp'
),
canary_authority AS
(
    SELECT * FROM canary
    UNION ALL
    SELECT * FROM canary_risk_snapshot
),
canary_by_engine AS
(
    SELECT
        engine_id,
        max(admitted_parent_count) AS admitted_parents,
        max(unique_admitted_parent_count) AS unique_admitted_parents,
        max(admitted_notional) AS admitted_notional,
        countIf(decision = 'reject') AS rejected_entries
    FROM canary_authority
    GROUP BY engine_id
),
network AS
(
    SELECT
        JSONExtractString(payload_json, 'engine_id') AS engine_id,
        argMax(
            JSONExtractBool(payload_json, 'execution_enabled'),
            observed_at_ms
        ) AS execution_enabled,
        argMax(JSONExtractString(payload_json, 'gas_policy'), observed_at_ms) AS gas_policy
    FROM runtime_telemetry
    WHERE kind = 'network_runtime_started'
      AND JSONExtractUInt(payload_json, 'chain_id') = 42161
      AND observed_at_ms >= toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}))
      AND observed_at_ms < toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}))
    GROUP BY engine_id
),
readiness_by_engine AS
(
    SELECT
        engine_id,
        uniqExact(stage) AS readiness_stage_count,
        uniqExactIf(stage, ready) AS ready_stage_count,
        maxIf(request_count, stage = 'binance_order_matrix') AS binance_request_count,
        maxIf(mutation_authorized, stage = 'binance_order_matrix') AS binance_mutation_enabled,
        maxIf(mutation_authorized, stage = 'arbitrum_chain') AS chain_mutation_enabled,
        maxIf(token_a_funded, stage = 'arbitrum_chain') AS token_a_funded,
        maxIf(token_b_funded, stage = 'arbitrum_chain') AS token_b_funded,
        maxIf(mutation_authorized, stage = 'arbitrum_rebalance_routes')
            AS rebalance_mutation_enabled,
        maxIf(direct_route_count, stage = 'arbitrum_rebalance_routes')
            AS direct_rebalance_routes
    FROM readiness_latest
    GROUP BY engine_id
)
SELECT
    readiness_by_engine.engine_id,
    readiness_stage_count,
    ready_stage_count,
    binance_request_count,
    binance_mutation_enabled,
    chain_mutation_enabled,
    token_a_funded,
    token_b_funded,
    rebalance_mutation_enabled,
    direct_rebalance_routes,
    network.execution_enabled AS arbitrum_execution_enabled,
    network.gas_policy AS arbitrum_gas_policy,
    ifNull(canary_by_engine.admitted_parents, 0) AS admitted_parents,
    ifNull(canary_by_engine.unique_admitted_parents, 0) AS unique_admitted_parents,
    ifNull(canary_by_engine.admitted_notional, 0) AS admitted_notional,
    ifNull(canary_by_engine.rejected_entries, 0) AS rejected_entries,
    multiIf(
        readiness_stage_count != 3
            OR ready_stage_count != 3
            OR binance_request_count != 4
            OR NOT binance_mutation_enabled
            OR NOT chain_mutation_enabled
            OR NOT token_a_funded
            OR NOT token_b_funded
            OR rebalance_mutation_enabled
            OR direct_rebalance_routes != 2
            OR NOT arbitrum_execution_enabled
            OR arbitrum_gas_policy != 'arbitrum_one_fail_closed',
        'not_ready',
        admitted_parents > 2
            OR admitted_parents != unique_admitted_parents
            OR admitted_notional > 20000000,
        'limit_breach',
        admitted_parents = 0,
        'armed',
        'canary_observed'
    ) AS m9_gate
FROM readiness_by_engine
LEFT JOIN network USING (engine_id)
LEFT JOIN canary_by_engine USING (engine_id)
ORDER BY engine_id
FORMAT PrettyCompactMonoBlock

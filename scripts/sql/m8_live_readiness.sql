WITH readiness AS
(
    SELECT
        observed_at_ms,
        JSONExtractString(payload_json, 'engine_id') AS engine_id,
        JSONExtractString(payload_json, 'stage') AS stage,
        JSONExtractBool(payload_json, 'ready') AS ready,
        JSONExtractBool(payload_json, 'external_mutation_authorized')
            AS mutation_authorized,
        JSONExtractUInt(payload_json, 'request_count') AS request_count,
        JSONExtractBool(payload_json, 'filters_ready') AS filters_ready,
        JSONExtractBool(payload_json, 'exact_token_contracts') AS exact_token_contracts,
        JSONExtractBool(payload_json, 'token_code_present') AS token_code_present,
        JSONExtractBool(payload_json, 'router_code_present') AS router_code_present,
        JSONExtractBool(payload_json, 'native_gas_funded') AS native_gas_funded,
        JSONExtractBool(payload_json, 'fresh_rpc_gas_price') AS fresh_rpc_gas_price,
        JSONExtractUInt(payload_json, 'direct_route_count') AS direct_route_count
    FROM runtime_telemetry
    WHERE kind = 'm8_live_readiness'
      AND observed_at_ms >= toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}))
      AND observed_at_ms < toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}))
      AND startsWith(
          JSONExtractString(payload_json, 'engine_id'),
          'arb-bot-rust-shadow-gke-'
      )
),
readiness_latest AS
(
    SELECT
        engine_id,
        stage,
        argMax(ready, observed_at_ms) AS ready,
        argMax(request_count, observed_at_ms) AS request_count,
        argMax(filters_ready, observed_at_ms) AS filters_ready,
        argMax(exact_token_contracts, observed_at_ms) AS exact_token_contracts,
        argMax(token_code_present, observed_at_ms) AS token_code_present,
        argMax(router_code_present, observed_at_ms) AS router_code_present,
        argMax(native_gas_funded, observed_at_ms) AS native_gas_funded,
        argMax(fresh_rpc_gas_price, observed_at_ms) AS fresh_rpc_gas_price,
        argMax(direct_route_count, observed_at_ms) AS direct_route_count
    FROM readiness
    GROUP BY
        engine_id,
        stage
),
mutation_by_engine AS
(
    SELECT
        engine_id,
        countIf(mutation_authorized) AS mutation_capability_records
    FROM readiness
    GROUP BY engine_id
),
network AS
(
    SELECT
        JSONExtractString(payload_json, 'engine_id') AS engine_id,
        JSONExtractString(payload_json, 'gas_policy') AS gas_policy,
        JSONExtractBool(payload_json, 'execution_enabled') AS execution_enabled
    FROM runtime_telemetry
    WHERE kind = 'network_runtime_started'
      AND JSONExtractUInt(payload_json, 'chain_id') = 42161
      AND observed_at_ms >= toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}))
      AND observed_at_ms < toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}))
      AND startsWith(
          JSONExtractString(payload_json, 'engine_id'),
          'arb-bot-rust-shadow-gke-'
      )
),
readiness_by_engine AS
(
    SELECT
        readiness_latest.engine_id AS engine_id,
        uniqExact(stage) AS readiness_stage_count,
        uniqExactIf(stage, ready) AS ready_stage_count,
        mutation_capability_records,
        maxIf(request_count, stage = 'binance_order_matrix') AS binance_request_count,
        maxIf(filters_ready, stage = 'binance_order_matrix') AS binance_filters_ready,
        maxIf(exact_token_contracts, stage = 'arbitrum_chain') AS exact_token_contracts,
        maxIf(token_code_present, stage = 'arbitrum_chain') AS token_code_present,
        maxIf(router_code_present, stage = 'arbitrum_chain') AS router_code_present,
        maxIf(native_gas_funded, stage = 'arbitrum_chain') AS native_gas_funded,
        maxIf(fresh_rpc_gas_price, stage = 'arbitrum_chain') AS fresh_rpc_gas_price,
        maxIf(direct_route_count, stage = 'arbitrum_rebalance_routes')
            AS direct_rebalance_routes
    FROM readiness_latest
    INNER JOIN mutation_by_engine USING (engine_id)
    GROUP BY
        readiness_latest.engine_id,
        mutation_capability_records
)
SELECT
    readiness_by_engine.engine_id AS engine_id,
    readiness_stage_count,
    ready_stage_count,
    mutation_capability_records,
    binance_request_count,
    binance_filters_ready,
    exact_token_contracts,
    token_code_present,
    router_code_present,
    native_gas_funded,
    fresh_rpc_gas_price,
    direct_rebalance_routes,
    any(network.gas_policy) AS arbitrum_gas_policy,
    max(network.execution_enabled) AS arbitrum_execution_enabled,
    if(
        readiness_stage_count = 3
        AND ready_stage_count = 3
        AND mutation_capability_records = 0
        AND binance_request_count = 4
        AND binance_filters_ready
        AND exact_token_contracts
        AND token_code_present
        AND router_code_present
        AND native_gas_funded
        AND fresh_rpc_gas_price
        AND direct_rebalance_routes = 2
        AND arbitrum_gas_policy = 'arbitrum_one_fail_closed'
        AND NOT arbitrum_execution_enabled,
        'ready',
        'not_ready'
    ) AS m8_gate
FROM readiness_by_engine
LEFT JOIN network USING (engine_id)
GROUP BY
    readiness_by_engine.engine_id,
    readiness_stage_count,
    ready_stage_count,
    mutation_capability_records,
    binance_request_count,
    binance_filters_ready,
    exact_token_contracts,
    token_code_present,
    router_code_present,
    native_gas_funded,
    fresh_rpc_gas_price,
    direct_rebalance_routes
ORDER BY engine_id
FORMAT PrettyCompactMonoBlock

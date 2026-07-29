WITH events AS
(
    SELECT
        JSONExtractString(payload_json, 'engine_id') AS engine_id,
        kind,
        JSONExtractString(payload_json, 'strategy_id') AS strategy_id,
        JSONExtractBool(payload_json, 'decisions_match') AS decisions_match,
        JSONExtractUInt(payload_json, 'comparison_queue_us') AS comparison_queue_us,
        JSONExtractBool(payload_json, 'external_mutation_authorized') AS mutation_authorized,
        JSONExtractBool(payload_json, 'reservation_created') AS reservation_created,
        JSONExtractBool(payload_json, 'execution_enabled') AS execution_enabled
    FROM runtime_telemetry
    WHERE kind IN
    (
        'strategy_decision_compatibility',
        'coordinator_shadow_candidate',
        'shadow_reservation_plan',
        'shadow_rebalance_plan'
    )
      AND observed_at_ms >= toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}))
      AND observed_at_ms < toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}))
      AND startsWith(
          JSONExtractString(payload_json, 'engine_id'),
          'arb-bot-rust-shadow-gke-'
      )
)
SELECT
    engine_id,
    countIf(
        kind = 'strategy_decision_compatibility'
        AND strategy_id = 'strategy:world-chain-usdc-wld'
    ) AS wld_comparisons,
    countIf(
        kind = 'strategy_decision_compatibility'
        AND strategy_id = 'strategy:world-chain-usdc-wld'
        AND decisions_match
    ) AS wld_comparison_matches,
    countIf(
        kind = 'strategy_decision_compatibility'
        AND strategy_id = 'strategy:world-chain-usdc-wld'
        AND NOT decisions_match
    ) AS wld_comparison_mismatches,
    quantileExactIf(0.95)(
        comparison_queue_us,
        kind = 'strategy_decision_compatibility'
        AND strategy_id = 'strategy:world-chain-usdc-wld'
    ) AS comparison_queue_p95_us,
    quantileExactIf(0.99)(
        comparison_queue_us,
        kind = 'strategy_decision_compatibility'
        AND strategy_id = 'strategy:world-chain-usdc-wld'
    ) AS comparison_queue_p99_us,
    maxIf(
        comparison_queue_us,
        kind = 'strategy_decision_compatibility'
        AND strategy_id = 'strategy:world-chain-usdc-wld'
    ) AS comparison_queue_max_us,
    countIf(
        kind = 'coordinator_shadow_candidate'
        AND strategy_id = 'strategy:arbitrum-usdc-esp'
    ) AS esp_shadow_candidates,
    countIf(
        kind = 'shadow_reservation_plan'
        AND strategy_id = 'strategy:arbitrum-usdc-esp'
    ) AS esp_reservation_plans,
    countIf(
        kind = 'shadow_rebalance_plan'
        AND strategy_id = 'strategy:arbitrum-usdc-esp'
    ) AS esp_rebalance_plans,
    countIf(mutation_authorized OR reservation_created OR execution_enabled)
        AS shadow_mutation_capability_records,
    if(
        wld_comparisons > 0
        AND wld_comparisons = wld_comparison_matches
        AND wld_comparison_mismatches = 0
        AND shadow_mutation_capability_records = 0,
        'ready',
        'not_ready'
    ) AS m7_gate
FROM events
GROUP BY engine_id
ORDER BY engine_id
FORMAT PrettyCompactMonoBlock

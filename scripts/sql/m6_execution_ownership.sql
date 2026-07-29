WITH events AS
(
    SELECT
        JSONExtractString(payload_json, 'engine_id') AS engine_id,
        kind,
        JSONExtractString(payload_json, 'owner') AS owner,
        JSONExtractString(payload_json, 'journal_scope') AS journal_scope,
        JSONExtractString(payload_json, 'stage') AS stage,
        multiIf(
            kind = 'runtime_journal_recovery',
                JSONExtractUInt(payload_json, 'duration_us'),
            kind = 'arbitrage_execution_stage',
                JSONExtractUInt(payload_json, 'duration_us'),
            toUInt64(0)
        ) AS duration_us,
        JSONExtractUInt(payload_json, 'schema_version') AS schema_version,
        JSONExtractUInt(payload_json, 'binance_owner_count') AS binance_owner_count,
        JSONExtractUInt(payload_json, 'evm_owner_count') AS evm_owner_count,
        JSONExtractUInt(payload_json, 'executable_strategy_count') AS executable_strategy_count,
        JSONExtractBool(payload_json, 'global_trade_serialization') AS global_serialization,
        JSONExtractBool(payload_json, 'rebalance_signer_access') AS rebalance_signer_access
    FROM runtime_telemetry
    WHERE kind IN
    (
        'execution_ownership_runtime_started',
        'runtime_journal_recovery',
        'arbitrage_execution_stage'
    )
      AND observed_at_ms >= toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}))
      AND observed_at_ms < toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}))
)
SELECT
    engine_id,
    kind,
    owner,
    journal_scope,
    stage,
    count() AS records,
    max(schema_version) AS schema_version,
    max(binance_owner_count) AS binance_owner_count,
    max(evm_owner_count) AS evm_owner_count,
    max(executable_strategy_count) AS executable_strategy_count,
    countIf(global_serialization) AS globally_serialized_records,
    countIf(rebalance_signer_access) AS rebalance_signer_access_records,
    quantileExact(0.95)(duration_us) AS duration_p95_us,
    quantileExact(0.99)(duration_us) AS duration_p99_us,
    max(duration_us) AS duration_max_us
FROM events
WHERE kind != 'arbitrage_execution_stage'
   OR stage IN
      ('coordinator_admit_journal', 'preflight_proof_to_parent_fsync', 'intent_journal')
GROUP BY
    engine_id,
    kind,
    owner,
    journal_scope,
    stage
ORDER BY
    engine_id,
    kind,
    owner,
    stage
FORMAT PrettyCompactMonoBlock

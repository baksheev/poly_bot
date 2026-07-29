WITH
    toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}, 3, 'UTC')) AS start_ms,
    toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}, 3, 'UTC')) AS end_ms,
    samples AS
    (
        SELECT
            JSONExtractString(payload_json, 'engine_id') AS engine_id,
            arrayJoin([
                if(kind = 'binance_balance_snapshot',
                    tuple('binance_account_rest', JSONExtractUInt(payload_json, 'request_duration_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'wallet_balance_snapshot',
                    tuple('wallet_batch_total', JSONExtractUInt(payload_json, 'request_duration_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'wallet_balance_snapshot' AND JSONHas(payload_json, 'batch_provider_us'),
                    tuple('wallet_batch_provider', JSONExtractUInt(payload_json, 'batch_provider_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'wallet_balance_snapshot' AND JSONHas(payload_json, 'batch_build_us'),
                    tuple('wallet_batch_build', JSONExtractUInt(payload_json, 'batch_build_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'wallet_balance_snapshot' AND JSONHas(payload_json, 'batch_decode_us'),
                    tuple('wallet_batch_decode', JSONExtractUInt(payload_json, 'batch_decode_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'wallet_balance_snapshot' AND JSONHas(payload_json, 'batch_queue_us'),
                    tuple('wallet_batch_queue', JSONExtractUInt(payload_json, 'batch_queue_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'wallet_balance_snapshot' AND JSONHas(payload_json, 'batch_publication_us'),
                    tuple('wallet_batch_publication', JSONExtractUInt(payload_json, 'batch_publication_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'decision_owner_health',
                    tuple('decision_owner_loop_lag', JSONExtractUInt(payload_json, 'loop_lag_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'decision_owner_health',
                    tuple('longest_non_price_handler', JSONExtractUInt(payload_json, 'longest_non_price_handler_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'decision_owner_dex_drain',
                    tuple('dependency_scoped_dex_drain', JSONExtractUInt(payload_json, 'duration_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'capital_allocation_evaluated',
                    tuple('capital_allocation', JSONExtractUInt(payload_json, 'calculation_validation_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'runtime_bootstrap_stage',
                    tuple(concat('bootstrap:', JSONExtractString(payload_json, 'stage')), JSONExtractUInt(payload_json, 'duration_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'runtime_journal_recovery',
                    tuple(concat('journal:', JSONExtractString(payload_json, 'owner')), JSONExtractUInt(payload_json, 'duration_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'runtime_first_ready',
                    tuple('process_to_first_ready', JSONExtractUInt(payload_json, 'process_start_to_first_ready_us')),
                    tuple('', toUInt64(0)))
            ]) AS sample
        FROM runtime_telemetry
        WHERE observed_at_ms >= start_ms
          AND observed_at_ms < end_ms
          AND kind IN (
              'binance_balance_snapshot',
              'wallet_balance_snapshot',
              'decision_owner_health',
              'decision_owner_dex_drain',
              'capital_allocation_evaluated',
              'runtime_bootstrap_stage',
              'runtime_journal_recovery',
              'runtime_first_ready'
          )
    )
SELECT
    engine_id,
    tupleElement(sample, 1) AS stage,
    count() AS n,
    quantileExact(0.50)(tupleElement(sample, 2)) AS p50_us,
    quantileExact(0.95)(tupleElement(sample, 2)) AS p95_us,
    quantileExact(0.99)(tupleElement(sample, 2)) AS p99_us,
    max(tupleElement(sample, 2)) AS max_us
FROM samples
WHERE stage != ''
GROUP BY engine_id, stage
ORDER BY engine_id, stage
FORMAT TabSeparatedWithNames

WITH
    toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}, 3, 'UTC')) AS start_ms,
    toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}, 3, 'UTC')) AS end_ms,
    samples AS
    (
        SELECT
            JSONExtractString(payload_json, 'engine_id') AS engine_id,
            arrayJoin([
                if(kind = 'arbitrage_adaptive_sizing_task',
                    tuple('sizing_snapshot', JSONExtractUInt(payload_json, 'snapshot_time_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'arbitrage_adaptive_sizing_task',
                    tuple('sizing_worker_queue', JSONExtractUInt(payload_json, 'queue_time_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'arbitrage_adaptive_sizing_task',
                    tuple('sizing_worker_calculation', JSONExtractUInt(payload_json, 'worker_time_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'arbitrage_adaptive_sizing_task',
                    tuple('sizing_result_handoff', JSONExtractUInt(payload_json, 'result_handoff_time_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'arbitrage_adaptive_sizing_evaluated',
                    tuple('optimizer_calculation', JSONExtractUInt(payload_json, 'calculation_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'arbitrage_admitted',
                    tuple('trigger_to_admitted', JSONExtractUInt(payload_json, 'trigger_to_admitted_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'arbitrage_admitted',
                    tuple('admission_total', JSONExtractUInt(payload_json, 'admission_total_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'arbitrage_admitted',
                    tuple('inventory_reservation', JSONExtractUInt(payload_json, 'inventory_reservation_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'arbitrage_admitted',
                    tuple('mailbox_submit', JSONExtractUInt(payload_json, 'mailbox_submit_us')),
                    tuple('', toUInt64(0))),
                if(kind = 'arbitrage_admitted' AND JSONHas(payload_json, 'candidate_selected_to_reservation_complete_us'),
                    tuple('candidate_to_reservation', JSONExtractUInt(payload_json, 'candidate_selected_to_reservation_complete_us')),
                    tuple('', toUInt64(0)))
            ]) AS sample
        FROM runtime_telemetry
        WHERE observed_at_ms >= start_ms
          AND observed_at_ms < end_ms
          AND kind IN (
              'arbitrage_adaptive_sizing_task',
              'arbitrage_adaptive_sizing_evaluated',
              'arbitrage_admitted'
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

WITH
    toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}, 3, 'UTC')) AS start_ms,
    toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}, 3, 'UTC')) AS end_ms,
    evaluations AS
    (
        SELECT
            observed_at_ms,
            JSONExtractString(payload_json, 'engine_id') AS engine_id,
            JSONExtractString(payload_json, 'pair_id') AS pair_id,
            JSONExtractString(payload_json, 'symbol') AS symbol,
            JSONExtractUInt(payload_json, 'update_id') AS update_id,
            JSONExtractUInt(payload_json, 'telemetry_queue_delay_us') AS telemetry_queue_delay_us,
            JSONExtractBool(payload_json, 'pretrade_cost_sampled') AS pretrade_cost_sampled,
            arrayJoin(JSONExtractArrayRaw(payload_json, 'directions')) AS direction_json
        FROM runtime_telemetry
        WHERE observed_at_ms >= start_ms
          AND observed_at_ms < end_ms
          AND kind = 'arbitrage_evaluation'
    ),
    coverage AS
    (
        SELECT
            engine_id,
            pair_id,
            symbol,
            count() / 2 AS evaluation_records,
            countIf(pretrade_cost_sampled) / 2 AS sampled_records
        FROM evaluations
        GROUP BY engine_id, pair_id, symbol
    ),
    modeled AS
    (
        SELECT
            *,
            JSONExtractString(direction_json, 'direction') AS direction,
            JSONExtractRaw(direction_json, 'baseline') AS baseline_json,
            JSONExtractRaw(JSONExtractRaw(direction_json, 'baseline'), 'pretrade_cost') AS cost_json
        FROM evaluations
        WHERE JSONExtractRaw(direction_json, 'baseline') NOT IN ('', 'null')
    )
SELECT
    engine_id,
    pair_id,
    symbol,
    direction,
    JSONExtractString(cost_json, 'model_version') AS model_version,
    coverage.evaluation_records,
    coverage.sampled_records,
    round(100 * coverage.sampled_records / coverage.evaluation_records, 2) AS sampled_record_percent,
    count() AS evaluations,
    countIf(JSONExtractBool(cost_json, 'model_inputs_complete')) AS complete_evaluations,
    round(100 * complete_evaluations / evaluations, 2) AS complete_evaluation_percent,
    countIf(JSONExtractBool(cost_json, 'fixed_threshold_met')) AS fixed_threshold_candidates,
    countIf(JSONExtractBool(cost_json, 'hypothetical_threshold_met')) AS hypothetical_net_5bps_candidates,
    countIf(JSONExtractBool(cost_json, 'hypothetical_new_capture')) AS hypothetical_new_captures,
    round(avg(JSONExtractInt(cost_json, 'gross_profit_bps_x100')) / 100, 2) AS average_gross_bps,
    round(avg(JSONExtractUInt(cost_json, 'binance_commission_bps_x100')) / 100, 2) AS average_binance_commission_bps,
    round(
        avgIf(
            JSONExtractUInt(cost_json, 'gas_cost_bps_x100'),
            JSONExtractBool(cost_json, 'model_inputs_complete')
        ) / 100,
        2
    ) AS average_complete_gas_cost_bps,
    round(
        avgIf(
            JSONExtractUInt(cost_json, 'modeled_cost_bps_x100'),
            JSONExtractBool(cost_json, 'model_inputs_complete')
        ) / 100,
        2
    ) AS average_complete_total_cost_bps,
    round(
        avgIf(
            JSONExtractInt(cost_json, 'net_profit_bps_x100'),
            JSONExtractBool(cost_json, 'model_inputs_complete')
        ) / 100,
        2
    ) AS average_complete_net_bps,
    quantilesIf(0.1, 0.5, 0.9)(
        JSONExtractInt(cost_json, 'net_profit_bps_x100') / 100,
        JSONExtractBool(cost_json, 'model_inputs_complete')
    ) AS complete_net_bps_p10_p50_p90,
    round(avg(telemetry_queue_delay_us), 2) AS average_telemetry_queue_delay_us,
    max(telemetry_queue_delay_us) AS maximum_telemetry_queue_delay_us,
    countIf(NOT JSONExtractBool(cost_json, 'gas_price_available_pretrade')) AS missing_pretrade_gas_samples,
    countIf(NOT JSONExtractBool(cost_json, 'gas_price_fresh')) AS stale_pretrade_gas_samples,
    countIf(NOT JSONExtractBool(cost_json, 'native_conversion_available_pretrade')) AS missing_native_conversion_samples,
    countIf(NOT JSONExtractBool(cost_json, 'native_conversion_fresh')) AS stale_native_conversion_samples,
    countIf(
        JSONExtractBool(cost_json, 'l1_fee_required')
        AND NOT JSONExtractBool(cost_json, 'l1_fee_available')
    ) AS missing_l1_fee_models,
    countIf(JSONExtractString(cost_json, 'receipt_cost_source') = 'journal_bootstrap_receipt')
        AS journal_bootstrap_receipt_samples,
    countIf(JSONExtractString(cost_json, 'receipt_cost_source') = 'live_execution_receipt')
        AS live_execution_receipt_samples,
    uniqExact(update_id) AS unique_book_updates
FROM modeled
INNER JOIN coverage USING (engine_id, pair_id, symbol)
WHERE JSONExtractString(cost_json, 'model_version') = 'diagnostic_net_edge_v2'
GROUP BY engine_id, pair_id, symbol, direction, model_version,
    coverage.evaluation_records, coverage.sampled_records
ORDER BY engine_id, pair_id, symbol, direction
FORMAT TabSeparatedWithNames

WITH
    toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}, 3, 'UTC')) AS start_ms,
    toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}, 3, 'UTC')) AS end_ms,
    expanded AS
    (
        SELECT
            observed_at_ms,
            JSONExtractString(payload_json, 'engine_id') AS engine_id,
            JSONExtractString(payload_json, 'pair_id') AS pair_id,
            JSONExtractString(payload_json, 'symbol') AS symbol,
            JSONExtractUInt(payload_json, 'update_id') AS update_id,
            JSONExtractUInt(payload_json, 'telemetry_queue_delay_us') AS telemetry_queue_delay_us,
            arrayJoin(JSONExtractArrayRaw(payload_json, 'directions')) AS direction_json
        FROM runtime_telemetry
        WHERE observed_at_ms >= start_ms
          AND observed_at_ms < end_ms
          AND kind = 'arbitrage_evaluation'
    ),
    modeled AS
    (
        SELECT
            *,
            JSONExtractString(direction_json, 'direction') AS direction,
            JSONExtractRaw(direction_json, 'baseline') AS baseline_json,
            JSONExtractRaw(JSONExtractRaw(direction_json, 'baseline'), 'pretrade_cost') AS cost_json
        FROM expanded
        WHERE JSONExtractRaw(direction_json, 'baseline') NOT IN ('', 'null')
    )
SELECT
    engine_id,
    pair_id,
    symbol,
    direction,
    JSONExtractString(cost_json, 'model_version') AS model_version,
    count() AS evaluations,
    countIf(JSONExtractBool(cost_json, 'model_inputs_complete')) AS complete_evaluations,
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
    countIf(
        JSONExtractBool(cost_json, 'l1_fee_required')
        AND NOT JSONExtractBool(cost_json, 'l1_fee_available')
    ) AS missing_l1_fee_models,
    uniqExact(update_id) AS unique_book_updates
FROM modeled
WHERE JSONExtractString(cost_json, 'model_version') = 'diagnostic_net_edge_v1'
GROUP BY engine_id, pair_id, symbol, direction, model_version
ORDER BY engine_id, pair_id, symbol, direction
FORMAT TabSeparatedWithNames

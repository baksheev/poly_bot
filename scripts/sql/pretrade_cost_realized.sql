WITH
    toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}, 3, 'UTC')) AS start_ms,
    toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}, 3, 'UTC')) AS end_ms,
    candidates AS
    (
        SELECT
            JSONExtractString(payload_json, 'engine_id') AS engine_id,
            JSONExtractString(payload_json, 'plan_id') AS plan_id,
            JSONExtractString(payload_json, 'pair_id') AS pair_id,
            JSONExtractString(payload_json, 'symbol') AS symbol,
            JSONExtractString(payload_json, 'direction') AS direction,
            JSONExtractUInt(payload_json, 'update_id') AS update_id,
            JSONExtractRaw(payload_json, 'candidate') AS candidate_json,
            JSONExtractUInt(JSONExtractRaw(payload_json, 'candidate'), 'token_a_decimals')
                AS token_a_decimals,
            JSONExtractRaw(JSONExtractRaw(payload_json, 'candidate'), 'pretrade_cost') AS cost_json,
            JSONExtractUInt(payload_json, 'telemetry_queue_delay_us') AS telemetry_queue_delay_us
        FROM runtime_telemetry
        WHERE observed_at_ms >= start_ms
          AND observed_at_ms < end_ms
          AND kind = 'pretrade_cost_candidate'
    ),
    admissions AS
    (
        SELECT
            JSONExtractString(payload_json, 'plan_id') AS plan_id,
            argMax(JSONExtractUInt(payload_json, 'update_id'), observed_at_ms) AS update_id
        FROM runtime_telemetry
        WHERE observed_at_ms >= start_ms
          AND observed_at_ms < end_ms
          AND kind = 'arbitrage_admitted'
        GROUP BY plan_id
    ),
    rejections AS
    (
        SELECT
            JSONExtractString(payload_json, 'plan_id') AS plan_id,
            argMax(JSONExtractString(payload_json, 'reason'), observed_at_ms) AS reason,
            argMax(JSONExtractString(payload_json, 'pair_id'), observed_at_ms) AS pair_id,
            argMax(JSONExtractString(payload_json, 'symbol'), observed_at_ms) AS symbol,
            argMax(JSONExtractString(payload_json, 'direction'), observed_at_ms) AS direction,
            argMax(JSONExtractUInt(payload_json, 'update_id'), observed_at_ms) AS update_id
        FROM runtime_telemetry
        WHERE observed_at_ms >= start_ms
          AND observed_at_ms < end_ms
          AND kind = 'arbitrage_admission_rejected'
          AND notEmpty(JSONExtractString(payload_json, 'plan_id'))
        GROUP BY plan_id
    ),
    orders AS
    (
        SELECT
            JSONExtractString(payload_json, 'plan_id') AS plan_id,
            argMax(
                JSONExtractString(payload_json, 'third_asset_commission_value_token_a'),
                observed_at_ms
            ) AS commission_value_token_a,
            argMax(
                JSONExtractBool(payload_json, 'third_asset_commission_valuation_complete'),
                observed_at_ms
            ) AS commission_valuation_complete
        FROM runtime_telemetry
        WHERE observed_at_ms >= start_ms
          AND observed_at_ms < end_ms
          AND kind = 'arbitrage_binance_order'
          AND JSONExtractString(payload_json, 'phase') = 'terminal'
          AND JSONExtractString(payload_json, 'role') = 'cex'
        GROUP BY plan_id
    ),
    results AS
    (
        SELECT
            JSONExtractString(payload_json, 'plan_id') AS plan_id,
            JSONExtractUInt(payload_json, 'opportunity_update_id') AS opportunity_update_id,
            toInt64OrZero(JSONExtractString(payload_json, 'gas_cost_token_a_base_units'))
                AS realized_gas_cost,
            toInt64OrZero(
                JSONExtractString(
                    payload_json,
                    'realized_primary_profit_vs_gross_error_token_a_base_units'
                )
            ) AS realized_primary_profit_vs_gross_error
        FROM runtime_telemetry
        WHERE observed_at_ms >= start_ms
          AND observed_at_ms < end_ms
          AND kind = 'arbitrage_result'
    )
SELECT
    candidates.engine_id,
    candidates.pair_id,
    candidates.symbol,
    candidates.direction,
    count() AS selected_candidates,
    countIf(JSONExtractBool(cost_json, 'model_inputs_complete')) AS complete_selected_candidates,
    countIf(notEmpty(admissions.plan_id)) AS admitted_candidates,
    countIf(notEmpty(rejections.plan_id)) AS rejected_candidates,
    countIf(
        notEmpty(rejections.plan_id)
        AND rejections.reason = 'insufficient_available_inventory'
    ) AS insufficient_inventory_rejections,
    countIf(
        notEmpty(rejections.plan_id)
        AND rejections.reason != 'insufficient_available_inventory'
    ) AS other_rejections,
    countIf(
        notEmpty(rejections.plan_id)
        AND rejections.pair_id = candidates.pair_id
        AND rejections.symbol = candidates.symbol
        AND rejections.direction = candidates.direction
        AND rejections.update_id = candidates.update_id
    ) AS exact_rejection_context_joins,
    countIf(notEmpty(admissions.plan_id) AND notEmpty(results.plan_id)) AS terminal_results,
    countIf(
        notEmpty(admissions.plan_id)
        AND
        notEmpty(results.plan_id)
        AND results.opportunity_update_id = candidates.update_id
    ) AS exact_update_joins,
    countIf(
        notEmpty(admissions.plan_id)
        AND
        notEmpty(results.plan_id)
        AND JSONExtractBool(cost_json, 'model_inputs_complete')
    ) AS complete_realized_joins,
    countIf(
        notEmpty(orders.plan_id)
        AND orders.commission_valuation_complete
    ) AS complete_commission_joins,
    round(avg(telemetry_queue_delay_us), 2) AS average_candidate_queue_delay_us,
    max(telemetry_queue_delay_us) AS maximum_candidate_queue_delay_us,
    round(
        avgIf(
            toFloat64(toInt64OrZero(JSONExtractString(cost_json, 'gas_cost_token_a_base_units'))),
            notEmpty(results.plan_id)
                AND JSONExtractBool(cost_json, 'model_inputs_complete')
        ),
        2
    ) AS average_predicted_gas_cost_base_units,
    round(
        avgIf(
            toFloat64(results.realized_gas_cost),
            notEmpty(results.plan_id)
                AND JSONExtractBool(cost_json, 'model_inputs_complete')
        ),
        2
    ) AS average_realized_gas_cost_base_units,
    round(
        avgIf(
            toFloat64(results.realized_gas_cost)
                - toFloat64(toInt64OrZero(JSONExtractString(cost_json, 'gas_cost_token_a_base_units'))),
            notEmpty(results.plan_id)
                AND JSONExtractBool(cost_json, 'model_inputs_complete')
        ),
        2
    ) AS average_gas_prediction_error_base_units,
    round(
        avgIf(
            toFloat64(
                toInt64OrZero(
                    JSONExtractString(cost_json, 'binance_commission_token_a_base_units')
                )
            ),
            notEmpty(orders.plan_id)
                AND orders.commission_valuation_complete
        ),
        2
    ) AS average_predicted_commission_base_units,
    round(
        avgIf(
            toFloat64OrZero(orders.commission_value_token_a)
                * pow(10, candidates.token_a_decimals),
            notEmpty(orders.plan_id)
                AND orders.commission_valuation_complete
        ),
        2
    ) AS average_realized_commission_base_units,
    round(
        avgIf(
            toFloat64OrZero(orders.commission_value_token_a)
                * pow(10, candidates.token_a_decimals)
                - toFloat64(
                    toInt64OrZero(
                        JSONExtractString(cost_json, 'binance_commission_token_a_base_units')
                    )
                ),
            notEmpty(orders.plan_id)
                AND orders.commission_valuation_complete
        ),
        2
    ) AS average_commission_prediction_error_base_units,
    round(
        avgIf(
            toFloat64(-results.realized_primary_profit_vs_gross_error),
            notEmpty(results.plan_id)
        ),
        2
    ) AS average_realized_primary_drag_base_units
FROM candidates
LEFT JOIN admissions USING (plan_id)
LEFT JOIN rejections USING (plan_id)
LEFT JOIN results USING (plan_id)
LEFT JOIN orders USING (plan_id)
WHERE JSONExtractString(cost_json, 'model_version') = 'diagnostic_net_edge_v3'
GROUP BY candidates.engine_id, candidates.pair_id, candidates.symbol, candidates.direction
ORDER BY candidates.engine_id, candidates.pair_id, candidates.symbol, candidates.direction
FORMAT TabSeparatedWithNames

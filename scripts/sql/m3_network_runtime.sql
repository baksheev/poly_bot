WITH batches AS
(
    SELECT
        JSONExtractString(payload_json, 'engine_id') AS engine_id,
        JSONExtractString(payload_json, 'network_id') AS network_id,
        JSONExtractUInt(payload_json, 'chain_id') AS chain_id,
        JSONExtractUInt(payload_json, 'connection_generation') AS generation,
        JSONExtractString(payload_json, 'read_class') AS read_class,
        JSONExtractString(payload_json, 'provider_capability_profile') AS provider_profile,
        JSONExtractBool(payload_json, 'supports_eip1898_block_hash') AS block_hash_pinned,
        JSONExtractString(payload_json, 'multicall3_code_identity') AS multicall3_code_identity,
        JSONExtractString(payload_json, 'outcome') AS outcome,
        JSONExtractBool(payload_json, 'complete') AS complete,
        JSONExtractUInt(payload_json, 'queue_us') AS queue_us,
        JSONExtractUInt(payload_json, 'provider_us') AS provider_us,
        JSONExtractUInt(payload_json, 'decode_us') AS decode_us,
        JSONExtractUInt(payload_json, 'publication_us') AS publication_us,
        JSONExtractUInt(payload_json, 'chunk_count') AS chunk_count,
        JSONExtractUInt(payload_json, 'response_bytes') AS response_bytes,
        JSONExtractUInt(payload_json, 'requested_count') AS requested_count,
        JSONExtractUInt(payload_json, 'returned_count') AS returned_count,
        JSONExtractString(payload_json, 'block_hash') AS block_hash
    FROM runtime_telemetry
    WHERE kind = 'network_read_batch'
      AND observed_at_ms >= toUnixTimestamp64Milli(parseDateTime64BestEffort({start_utc:String}))
      AND observed_at_ms < toUnixTimestamp64Milli(parseDateTime64BestEffort({end_utc:String}))
)
SELECT
    engine_id,
    network_id,
    chain_id,
    generation,
    read_class,
    provider_profile,
    block_hash_pinned,
    multicall3_code_identity,
    outcome,
    complete,
    count() AS rounds,
    uniqExact(block_hash) AS pinned_blocks,
    sum(requested_count) AS requested_calls,
    sum(returned_count) AS returned_calls,
    quantileExact(0.50)(queue_us) AS queue_p50_us,
    quantileExact(0.95)(queue_us) AS queue_p95_us,
    quantileExact(0.99)(queue_us) AS queue_p99_us,
    max(queue_us) AS queue_max_us,
    quantileExact(0.50)(provider_us) AS provider_p50_us,
    quantileExact(0.95)(provider_us) AS provider_p95_us,
    quantileExact(0.99)(provider_us) AS provider_p99_us,
    max(provider_us) AS provider_max_us,
    quantileExact(0.50)(decode_us) AS decode_p50_us,
    quantileExact(0.95)(decode_us) AS decode_p95_us,
    quantileExact(0.99)(decode_us) AS decode_p99_us,
    max(decode_us) AS decode_max_us,
    quantileExact(0.50)(publication_us) AS publication_p50_us,
    quantileExact(0.95)(publication_us) AS publication_p95_us,
    quantileExact(0.99)(publication_us) AS publication_p99_us,
    max(publication_us) AS publication_max_us,
    max(chunk_count) AS max_chunks,
    max(response_bytes) AS max_response_bytes
FROM batches
GROUP BY
    engine_id,
    network_id,
    chain_id,
    generation,
    read_class,
    provider_profile,
    block_hash_pinned,
    multicall3_code_identity,
    outcome,
    complete
ORDER BY
    engine_id,
    network_id,
    read_class,
    generation
FORMAT PrettyCompactMonoBlock

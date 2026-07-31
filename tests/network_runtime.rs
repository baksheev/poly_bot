use std::{fs, path::PathBuf};

use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct BatchFixture {
    fixture_version: u32,
    source: String,
    chain_id: u64,
    block_number: u64,
    block_hash: String,
    pool: String,
    calls: Vec<CapturedCall>,
    individual_results: Vec<String>,
    batch_results: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CapturedCall {
    identity: String,
    selector: String,
}

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn captured_pinned_batch_matches_individual_reads_exactly() {
    let bytes = fs::read(repository_root().join("tests/fixtures/world-v3-batch-pinned.v1.json"))
        .expect("captured network fixture must be readable");
    let fixture: BatchFixture =
        serde_json::from_slice(&bytes).expect("captured network fixture must be valid");

    assert_eq!(fixture.fixture_version, 1);
    assert!(fixture.source.contains("captured"));
    assert_eq!(fixture.chain_id, 480);
    assert_eq!(fixture.block_number, 0x1ee7069);
    assert_eq!(fixture.block_hash.len(), 66);
    assert_eq!(fixture.pool.len(), 42);
    assert_eq!(fixture.calls.len(), 4);
    assert_eq!(fixture.calls.len(), fixture.individual_results.len());
    assert_eq!(fixture.individual_results, fixture.batch_results);
    assert_eq!(
        fixture
            .calls
            .iter()
            .map(|call| call.identity.as_str())
            .collect::<Vec<_>>(),
        ["slot0", "liquidity", "tickSpacing", "fee"]
    );
    assert!(
        fixture
            .calls
            .iter()
            .all(|call| call.selector.starts_with("0x") && call.selector.len() == 10)
    );
}

#[test]
fn runtime_source_keeps_block_hash_pinning_and_partial_batch_gate() {
    let runtime = fs::read_to_string(repository_root().join("src/network_runtime.rs"))
        .expect("network runtime source must be readable");
    let rpc = fs::read_to_string(repository_root().join("src/chain/rpc.rs"))
        .expect("RPC source must be readable");

    assert!(runtime.contains("supports_eip1898_block_hash"));
    assert!(runtime.contains("partial network read batch cannot be published"));
    assert!(runtime.contains("NetworkReadClass::GapRepair"));
    assert!(runtime.contains("NetworkReadClass::WalletBalance"));
    assert!(runtime.contains("NetworkReadClass::QuoterParity"));
    assert!(rpc.contains("block.eip1898()"));
}

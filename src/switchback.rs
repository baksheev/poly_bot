use anyhow::{Context, ensure};
use sha2::{Digest, Sha256};

use crate::arbitrage::ExecutionMode;

pub const ESP_SWITCHBACK_PAIR_ID: &str = "arbitrum-usdc-esp";
pub const ESP_SWITCHBACK_EXPERIMENT_ID: &str = "esp-usdc-concurrent-full-live-v1";
pub const ESP_SWITCHBACK_SEED_VERSION: &str = "esp-usdc-switchback-seed-v1";
pub const ESP_SWITCHBACK_HASH_ALGORITHM: &str = "sha256";
pub const ESP_SWITCHBACK_START_UNIX_SECONDS: u64 = 1_786_158_000;
pub const ESP_SWITCHBACK_END_UNIX_SECONDS: u64 = 1_786_762_800;
pub const ESP_SWITCHBACK_BLOCK_DURATION_SECONDS: u64 = 30 * 60;

const ESP_SWITCHBACK_SEED: &str = "poly-bot:esp-usdc:concurrent-full-live:v1:2026-08-08";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProductionExecutionAssignment {
    pub execution_mode: ExecutionMode,
    pub enrollment_status: &'static str,
    pub experiment_id: Option<&'static str>,
    pub seed_version: Option<&'static str>,
    pub block_id: Option<u64>,
    pub block_pair_id: Option<u64>,
    pub block_position: Option<u8>,
    pub pair_order: Option<&'static str>,
    pub assignment_hash_prefix: Option<String>,
}

impl ProductionExecutionAssignment {
    pub fn is_enrolled(&self) -> bool {
        matches!(self.enrollment_status, "enrolled")
    }
}

pub fn validate_production_switchback() -> anyhow::Result<()> {
    ensure!(
        ESP_SWITCHBACK_START_UNIX_SECONDS < ESP_SWITCHBACK_END_UNIX_SECONDS,
        "ESP switchback window is empty"
    );
    ensure!(
        ESP_SWITCHBACK_BLOCK_DURATION_SECONDS > 0,
        "ESP switchback block duration is zero"
    );
    let duration = ESP_SWITCHBACK_END_UNIX_SECONDS
        .checked_sub(ESP_SWITCHBACK_START_UNIX_SECONDS)
        .context("ESP switchback window underflow")?;
    let pair_duration = ESP_SWITCHBACK_BLOCK_DURATION_SECONDS
        .checked_mul(2)
        .context("ESP switchback pair duration overflow")?;
    ensure!(
        duration % pair_duration == 0,
        "ESP switchback window does not contain complete block pairs"
    );
    Ok(())
}

pub fn production_execution_assignment(
    pair_id: &str,
    opportunity_received_unix_us: u64,
) -> anyhow::Result<ProductionExecutionAssignment> {
    if pair_id != ESP_SWITCHBACK_PAIR_ID {
        return Ok(fixed_dex_first("not_enrolled"));
    }
    validate_production_switchback()?;
    let received_unix_seconds = opportunity_received_unix_us / 1_000_000;
    if received_unix_seconds < ESP_SWITCHBACK_START_UNIX_SECONDS {
        return Ok(fixed_dex_first("before_enrollment"));
    }
    if received_unix_seconds >= ESP_SWITCHBACK_END_UNIX_SECONDS {
        return Ok(fixed_dex_first("after_enrollment"));
    }

    let block_id = received_unix_seconds
        .checked_sub(ESP_SWITCHBACK_START_UNIX_SECONDS)
        .context("ESP switchback block offset underflow")?
        / ESP_SWITCHBACK_BLOCK_DURATION_SECONDS;
    let block_pair_id = block_id / 2;
    let block_position = u8::try_from(block_id % 2).context("invalid switchback block position")?;
    let digest = assignment_digest(block_pair_id);
    let concurrent_first = digest[0] & 1 == 1;
    let execution_mode = match (concurrent_first, block_position) {
        (false, 0) | (true, 1) => ExecutionMode::DexFirst,
        (true, 0) | (false, 1) => ExecutionMode::ConcurrentHedged,
        _ => unreachable!("switchback block position is binary"),
    };
    let pair_order = if concurrent_first {
        "concurrent_hedged,dex_first"
    } else {
        "dex_first,concurrent_hedged"
    };

    Ok(ProductionExecutionAssignment {
        execution_mode,
        enrollment_status: "enrolled",
        experiment_id: Some(ESP_SWITCHBACK_EXPERIMENT_ID),
        seed_version: Some(ESP_SWITCHBACK_SEED_VERSION),
        block_id: Some(block_id),
        block_pair_id: Some(block_pair_id),
        block_position: Some(block_position),
        pair_order: Some(pair_order),
        assignment_hash_prefix: Some(hex_prefix(&digest)),
    })
}

fn fixed_dex_first(enrollment_status: &'static str) -> ProductionExecutionAssignment {
    ProductionExecutionAssignment {
        execution_mode: ExecutionMode::DexFirst,
        enrollment_status,
        experiment_id: (enrollment_status != "not_enrolled")
            .then_some(ESP_SWITCHBACK_EXPERIMENT_ID),
        seed_version: (enrollment_status != "not_enrolled").then_some(ESP_SWITCHBACK_SEED_VERSION),
        block_id: None,
        block_pair_id: None,
        block_position: None,
        pair_order: None,
        assignment_hash_prefix: None,
    }
}

fn assignment_digest(block_pair_id: u64) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(ESP_SWITCHBACK_SEED.as_bytes());
    hasher.update([0]);
    hasher.update(ESP_SWITCHBACK_EXPERIMENT_ID.as_bytes());
    hasher.update([0]);
    hasher.update(block_pair_id.to_be_bytes());
    hasher.finalize().into()
}

fn hex_prefix(digest: &[u8; 32]) -> String {
    let mut prefix = String::with_capacity(16);
    for byte in &digest[..8] {
        use std::fmt::Write as _;
        let _ = write!(prefix, "{byte:02x}");
    }
    prefix
}

#[cfg(test)]
mod tests {
    use super::{
        ESP_SWITCHBACK_BLOCK_DURATION_SECONDS, ESP_SWITCHBACK_END_UNIX_SECONDS,
        ESP_SWITCHBACK_PAIR_ID, ESP_SWITCHBACK_START_UNIX_SECONDS, production_execution_assignment,
        validate_production_switchback,
    };
    use crate::arbitrage::ExecutionMode;

    #[test]
    fn production_protocol_contains_complete_balanced_pairs() {
        validate_production_switchback().unwrap();
        let blocks = (ESP_SWITCHBACK_END_UNIX_SECONDS - ESP_SWITCHBACK_START_UNIX_SECONDS)
            / ESP_SWITCHBACK_BLOCK_DURATION_SECONDS;
        assert_eq!(blocks, 336);
        assert_eq!(blocks % 2, 0);
    }

    #[test]
    fn every_block_pair_assigns_each_live_mode_once() {
        for block_pair_id in 0..168 {
            let first_seconds = ESP_SWITCHBACK_START_UNIX_SECONDS
                + block_pair_id * 2 * ESP_SWITCHBACK_BLOCK_DURATION_SECONDS;
            let second_seconds = first_seconds + ESP_SWITCHBACK_BLOCK_DURATION_SECONDS;
            let first =
                production_execution_assignment(ESP_SWITCHBACK_PAIR_ID, first_seconds * 1_000_000)
                    .unwrap();
            let second =
                production_execution_assignment(ESP_SWITCHBACK_PAIR_ID, second_seconds * 1_000_000)
                    .unwrap();
            assert!(first.is_enrolled());
            assert!(second.is_enrolled());
            assert_ne!(first.execution_mode, second.execution_mode);
            assert_eq!(first.block_pair_id, second.block_pair_id);
            assert_eq!(first.pair_order, second.pair_order);
        }
    }

    #[test]
    fn assignment_is_deterministic_across_restart_and_inside_a_block() {
        let received = (ESP_SWITCHBACK_START_UNIX_SECONDS + 123) * 1_000_000;
        let first = production_execution_assignment(ESP_SWITCHBACK_PAIR_ID, received).unwrap();
        let restarted = production_execution_assignment(ESP_SWITCHBACK_PAIR_ID, received).unwrap();
        let later =
            production_execution_assignment(ESP_SWITCHBACK_PAIR_ID, received + 1_000_000).unwrap();
        assert_eq!(first, restarted);
        assert_eq!(first, later);
        assert_eq!(
            first.assignment_hash_prefix.as_deref(),
            Some("1f16a1d67d867574")
        );
    }

    #[test]
    fn enrollment_boundaries_fail_back_to_dex_first() {
        let before = production_execution_assignment(
            ESP_SWITCHBACK_PAIR_ID,
            (ESP_SWITCHBACK_START_UNIX_SECONDS - 1) * 1_000_000,
        )
        .unwrap();
        let after = production_execution_assignment(
            ESP_SWITCHBACK_PAIR_ID,
            ESP_SWITCHBACK_END_UNIX_SECONDS * 1_000_000,
        )
        .unwrap();
        assert_eq!(before.execution_mode, ExecutionMode::DexFirst);
        assert_eq!(before.enrollment_status, "before_enrollment");
        assert_eq!(after.execution_mode, ExecutionMode::DexFirst);
        assert_eq!(after.enrollment_status, "after_enrollment");
    }

    #[test]
    fn other_pairs_remain_fixed_dex_first() {
        let assignment = production_execution_assignment(
            "world-chain-usdc-wld",
            ESP_SWITCHBACK_START_UNIX_SECONDS * 1_000_000,
        )
        .unwrap();
        assert_eq!(assignment.execution_mode, ExecutionMode::DexFirst);
        assert_eq!(assignment.enrollment_status, "not_enrolled");
        assert_eq!(assignment.experiment_id, None);
    }
}

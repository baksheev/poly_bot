use alloy_primitives::{Address, B256, keccak256};
use anyhow::ensure;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V4PoolKey {
    pub currency0: Address,
    pub currency1: Address,
    pub fee_pips: u32,
    pub tick_spacing: i32,
    pub hooks: Address,
}

impl V4PoolKey {
    pub fn new(
        currency_a: Address,
        currency_b: Address,
        fee_pips: u32,
        tick_spacing: i32,
        hooks: Address,
    ) -> anyhow::Result<Self> {
        ensure!(currency_a != currency_b, "pool currencies must differ");
        ensure!(
            fee_pips < 1_000_000,
            "static fee must be below 1_000_000 pips"
        );
        ensure!(tick_spacing > 0, "tick spacing must be positive");
        let (currency0, currency1) = if currency_a < currency_b {
            (currency_a, currency_b)
        } else {
            (currency_b, currency_a)
        };
        Ok(Self {
            currency0,
            currency1,
            fee_pips,
            tick_spacing,
            hooks,
        })
    }

    /// Computes `PoolId.wrap(keccak256(abi.encode(PoolKey)))` without heap allocation.
    pub fn pool_id(self) -> B256 {
        let mut encoded = [0_u8; 160];
        encoded[12..32].copy_from_slice(self.currency0.as_slice());
        encoded[44..64].copy_from_slice(self.currency1.as_slice());

        let fee = self.fee_pips.to_be_bytes();
        encoded[93..96].copy_from_slice(&fee[1..]);

        let spacing = self.tick_spacing.to_be_bytes();
        encoded[96..125].fill(if self.tick_spacing < 0 { 0xff } else { 0 });
        encoded[125..128].copy_from_slice(&spacing[1..]);
        encoded[140..160].copy_from_slice(self.hooks.as_slice());
        keccak256(encoded)
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, address, b256};

    use super::V4PoolKey;

    const USDC: Address = address!("79a02482a880bce3f13e09da970dc34db4cd24d1");
    const WLD: Address = address!("2cfc85d8e48f8eab294be644d9e25c3030863003");

    #[test]
    fn sorts_currencies_and_computes_world_chain_pool_id() {
        let key = V4PoolKey::new(USDC, WLD, 10_000, 200, Address::ZERO).unwrap();
        assert_eq!(key.currency0, WLD);
        assert_eq!(key.currency1, USDC);
        assert_eq!(
            key.pool_id(),
            b256!("081028d60635d39241285edb01f6d6503b244eed2547333649daf2fe27c4a5b4")
        );
    }
}

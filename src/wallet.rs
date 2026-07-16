use std::str::FromStr;

use alloy_primitives::{Address, U256};
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, ensure};

use crate::chain::rpc::{JsonRpcClient, RpcStats};

pub const WALLET_PRIVATE_KEY_ENV: &str = "EVM_WALLET_PRIVATE_KEY";
pub const OPTIMISM_RPC_URL_ENV: &str = "ALCHEMY_OPTIMISM_RPC_URL";

#[derive(Clone, Debug)]
pub struct TokenBalanceRequest {
    pub symbol: String,
    pub contract: Address,
}

#[derive(Debug)]
pub struct TokenBalance {
    pub symbol: String,
    pub contract: Address,
    pub base_units: U256,
}

#[derive(Debug)]
pub struct ChainWalletState {
    pub chain_id: u64,
    pub block_number: u64,
    pub pending_nonce: u64,
    pub native_balance_wei: U256,
    pub token_balances: Vec<TokenBalance>,
    pub rpc_stats: RpcStats,
}

pub struct TestWallet {
    signer: PrivateKeySigner,
}

pub async fn hydrate_chain_wallet(
    endpoint: String,
    expected_chain_id: u64,
    owner: Address,
    tokens: &[TokenBalanceRequest],
) -> anyhow::Result<ChainWalletState> {
    let rpc = JsonRpcClient::new(endpoint)?;
    let chain_id = rpc.chain_id().await?;
    ensure!(
        chain_id == expected_chain_id,
        "RPC returned chain id {chain_id}, expected {expected_chain_id}"
    );
    let (block, pending_nonce, native_balance_wei) = tokio::try_join!(
        rpc.latest_block(),
        rpc.pending_nonce(owner),
        rpc.native_balance(owner),
    )?;
    let mut token_balances = Vec::with_capacity(tokens.len());
    for token in tokens {
        token_balances.push(TokenBalance {
            symbol: token.symbol.clone(),
            contract: token.contract,
            base_units: rpc.erc20_balance(token.contract, owner).await?,
        });
    }
    Ok(ChainWalletState {
        chain_id,
        block_number: block.number,
        pending_nonce,
        native_balance_wei,
        token_balances,
        rpc_stats: rpc.stats(),
    })
}

impl TestWallet {
    pub fn from_env() -> anyhow::Result<Self> {
        let private_key = std::env::var(WALLET_PRIVATE_KEY_ENV).with_context(|| {
            format!("required environment variable {WALLET_PRIVATE_KEY_ENV} is not set")
        })?;
        ensure!(
            !private_key.trim().is_empty(),
            "{WALLET_PRIVATE_KEY_ENV} is empty"
        );
        let signer = PrivateKeySigner::from_str(private_key.trim())
            .context("EVM wallet private key is invalid")?;
        Ok(Self { signer })
    }

    pub fn address(&self) -> Address {
        self.signer.address()
    }
}

impl std::fmt::Debug for TestWallet {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TestWallet")
            .field("address", &self.address())
            .field("signer", &"[REDACTED]")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::TestWallet;

    #[test]
    fn debug_never_contains_private_key() {
        let wallet: TestWallet = format!(
            "0x{}",
            "59c6995e998f97a5a0044976f7d04f8b2b7f4e5b5d5f3e49f2f4e7838a2b0c19"
        )
        .parse::<alloy_signer_local::PrivateKeySigner>()
        .map(|signer| TestWallet { signer })
        .unwrap();

        let debug = format!("{wallet:?}");
        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains("59c6995e"));
    }
}

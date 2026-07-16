use std::str::FromStr;

use alloy_consensus::{SignableTransaction, TxEip1559};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, B256, Bytes, TxKind, U256, keccak256};
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, ensure};

use crate::chain::rpc::{EthCall, JsonRpcClient, RpcStats, TransactionCall};

mod journal;
mod nonce;

pub use journal::{
    JournalIntent, JournalOperation, JournalOperationIdentity, JournalStatus, TransactionJournal,
    UnknownOutcomeReason,
};
pub use nonce::{NonceLane, NonceLaneState, NonceReconciliationOutcome, ReconciledNonceLane};

pub const WALLET_PRIVATE_KEY_ENV: &str = "EVM_WALLET_PRIVATE_KEY";
pub const WALLET_JOURNAL_PATH_ENV: &str = "EVM_WALLET_JOURNAL_PATH";
pub const OPTIMISM_RPC_URL_ENV: &str = "ALCHEMY_OPTIMISM_RPC_URL";

const ERC20_BALANCE_OF_SELECTOR: [u8; 4] = [0x70, 0xa0, 0x82, 0x31];
const ERC20_ALLOWANCE_SELECTOR: [u8; 4] = [0xdd, 0x62, 0xed, 0x3e];
const ERC20_TRANSFER_SELECTOR: [u8; 4] = [0xa9, 0x05, 0x9c, 0xbb];
const ERC20_APPROVE_SELECTOR: [u8; 4] = [0x09, 0x5e, 0xa7, 0xb3];

#[derive(Clone, Debug)]
pub struct TokenBalanceRequest {
    pub symbol: String,
    pub contract: Address,
}

#[derive(Clone, Debug)]
pub struct TokenAllowanceRequest {
    pub symbol: String,
    pub contract: Address,
    pub spender: Address,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenBalance {
    pub symbol: String,
    pub contract: Address,
    pub base_units: U256,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenAllowance {
    pub symbol: String,
    pub contract: Address,
    pub spender: Address,
    pub base_units: U256,
}

#[derive(Debug)]
pub struct ChainWalletState {
    pub chain_id: u64,
    pub block_number: u64,
    pub latest_nonce: u64,
    pub pending_nonce: u64,
    pub native_balance_wei: U256,
    pub token_balances: Vec<TokenBalance>,
    pub token_allowances: Vec<TokenAllowance>,
    pub rpc_stats: RpcStats,
}

impl ChainWalletState {
    pub fn has_pending_transactions(&self) -> bool {
        self.latest_nonce != self.pending_nonce
    }
}

/// A validated call that can be simulated before it is signed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalletCall {
    target: Address,
    value: U256,
    calldata: Vec<u8>,
}

impl WalletCall {
    pub fn native_transfer(recipient: Address, amount: U256) -> anyhow::Result<Self> {
        ensure!(
            recipient != Address::ZERO,
            "native transfer recipient is zero"
        );
        ensure!(!amount.is_zero(), "native transfer amount is zero");
        Ok(Self {
            target: recipient,
            value: amount,
            calldata: Vec::new(),
        })
    }

    pub fn erc20_transfer(
        token: Address,
        recipient: Address,
        amount: U256,
    ) -> anyhow::Result<Self> {
        ensure!(token != Address::ZERO, "ERC-20 transfer token is zero");
        ensure!(
            recipient != Address::ZERO,
            "ERC-20 transfer recipient is zero"
        );
        ensure!(!amount.is_zero(), "ERC-20 transfer amount is zero");
        Ok(Self {
            target: token,
            value: U256::ZERO,
            calldata: encode_address_u256(ERC20_TRANSFER_SELECTOR, recipient, amount),
        })
    }

    /// Builds an exact ERC-20 allowance update. A zero amount is permitted so
    /// callers can safely reset tokens that require approve(0) first.
    pub fn erc20_approval(token: Address, spender: Address, amount: U256) -> anyhow::Result<Self> {
        ensure!(token != Address::ZERO, "ERC-20 approval token is zero");
        ensure!(spender != Address::ZERO, "ERC-20 approval spender is zero");
        Ok(Self {
            target: token,
            value: U256::ZERO,
            calldata: encode_address_u256(ERC20_APPROVE_SELECTOR, spender, amount),
        })
    }

    /// Wraps calldata that has already been validated by a protocol-specific
    /// component (for example, the Across quote validator).
    pub fn validated_contract_call(
        target: Address,
        value: U256,
        calldata: Vec<u8>,
    ) -> anyhow::Result<Self> {
        ensure!(target != Address::ZERO, "contract call target is zero");
        ensure!(!calldata.is_empty(), "contract call calldata is empty");
        Ok(Self {
            target,
            value,
            calldata,
        })
    }

    pub fn target(&self) -> Address {
        self.target
    }

    pub fn value(&self) -> U256 {
        self.value
    }

    pub fn calldata(&self) -> &[u8] {
        &self.calldata
    }

    pub fn rpc_call(&self, owner: Address) -> TransactionCall {
        TransactionCall {
            from: owner,
            to: self.target,
            data: self.calldata.clone(),
            value: self.value,
        }
    }

    pub fn maximum_native_cost(
        &self,
        parameters: WalletTransactionParameters,
    ) -> anyhow::Result<U256> {
        parameters.validate()?;
        U256::from(parameters.gas_limit)
            .checked_mul(U256::from(parameters.max_fee_per_gas))
            .and_then(|gas| gas.checked_add(self.value))
            .context("wallet transaction maximum native cost overflow")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalletTransactionParameters {
    pub chain_id: u64,
    pub nonce: u64,
    pub gas_limit: u64,
    pub max_fee_per_gas: u128,
    pub max_priority_fee_per_gas: u128,
}

impl WalletTransactionParameters {
    fn validate(self) -> anyhow::Result<()> {
        ensure!(self.chain_id != 0, "wallet transaction chain id is zero");
        ensure!(self.gas_limit != 0, "wallet transaction gas limit is zero");
        ensure!(
            self.max_fee_per_gas != 0,
            "wallet transaction maximum fee is zero"
        );
        ensure!(
            self.max_priority_fee_per_gas <= self.max_fee_per_gas,
            "wallet transaction priority fee exceeds maximum fee"
        );
        Ok(())
    }
}

/// Process-local signer. It owns key material but performs no network I/O.
pub struct EvmWallet {
    signer: PrivateKeySigner,
}

pub async fn hydrate_chain_wallet(
    endpoint: String,
    expected_chain_id: u64,
    owner: Address,
    tokens: &[TokenBalanceRequest],
) -> anyhow::Result<ChainWalletState> {
    hydrate_chain_wallet_with_allowances(endpoint, expected_chain_id, owner, tokens, &[]).await
}

pub async fn hydrate_chain_wallet_with_allowances(
    endpoint: String,
    expected_chain_id: u64,
    owner: Address,
    tokens: &[TokenBalanceRequest],
    allowances: &[TokenAllowanceRequest],
) -> anyhow::Result<ChainWalletState> {
    let rpc = JsonRpcClient::new(endpoint)?;
    hydrate_chain_wallet_from_rpc(&rpc, expected_chain_id, owner, tokens, allowances).await
}

/// Hydrates one wallet through a reusable process-scoped RPC client. All
/// balance and allowance calls are pinned to the same canonical block.
pub async fn hydrate_chain_wallet_from_rpc(
    rpc: &JsonRpcClient,
    expected_chain_id: u64,
    owner: Address,
    tokens: &[TokenBalanceRequest],
    allowances: &[TokenAllowanceRequest],
) -> anyhow::Result<ChainWalletState> {
    ensure!(expected_chain_id != 0, "expected wallet chain id is zero");
    ensure!(owner != Address::ZERO, "wallet owner is zero");
    validate_hydration_requests(tokens, allowances)?;

    let chain_id = rpc.chain_id().await?;
    ensure!(
        chain_id == expected_chain_id,
        "RPC returned chain id {chain_id}, expected {expected_chain_id}"
    );
    let block = rpc.latest_block().await?;
    let calls = tokens
        .iter()
        .map(|token| EthCall {
            to: token.contract,
            data: erc20_balance_of_calldata(owner),
        })
        .chain(allowances.iter().map(|allowance| EthCall {
            to: allowance.contract,
            data: erc20_allowance_calldata(owner, allowance.spender),
        }))
        .collect::<Vec<_>>();
    let (latest_nonce, pending_nonce, native_balance_wei, encoded_values) = tokio::try_join!(
        rpc.latest_nonce(owner),
        rpc.pending_nonce(owner),
        rpc.native_balance_at(owner, block),
        rpc.eth_call_batch(&calls, block),
    )?;
    ensure!(
        encoded_values.len() == calls.len(),
        "wallet ERC-20 response count mismatch"
    );

    let mut values = encoded_values.into_iter();
    let token_balances = tokens
        .iter()
        .map(|token| {
            Ok(TokenBalance {
                symbol: token.symbol.clone(),
                contract: token.contract,
                base_units: decode_abi_u256(
                    "ERC-20 balance",
                    values.next().context("missing ERC-20 balance response")?,
                )?,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let token_allowances = allowances
        .iter()
        .map(|allowance| {
            Ok(TokenAllowance {
                symbol: allowance.symbol.clone(),
                contract: allowance.contract,
                spender: allowance.spender,
                base_units: decode_abi_u256(
                    "ERC-20 allowance",
                    values.next().context("missing ERC-20 allowance response")?,
                )?,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    ensure!(values.next().is_none(), "unexpected wallet ERC-20 response");

    Ok(ChainWalletState {
        chain_id,
        block_number: block.number,
        latest_nonce,
        pending_nonce,
        native_balance_wei,
        token_balances,
        token_allowances,
        rpc_stats: rpc.stats(),
    })
}

impl EvmWallet {
    pub fn from_env() -> anyhow::Result<Self> {
        let private_key = std::env::var(WALLET_PRIVATE_KEY_ENV).with_context(|| {
            format!("required environment variable {WALLET_PRIVATE_KEY_ENV} is not set")
        })?;
        Self::from_private_key(private_key.trim())
    }

    pub fn from_private_key(private_key: &str) -> anyhow::Result<Self> {
        ensure!(!private_key.is_empty(), "EVM wallet private key is empty");
        let signer =
            PrivateKeySigner::from_str(private_key).context("EVM wallet private key is invalid")?;
        Ok(Self { signer })
    }

    pub fn address(&self) -> Address {
        self.signer.address()
    }

    pub fn sign_call(
        &self,
        call: &WalletCall,
        parameters: WalletTransactionParameters,
    ) -> anyhow::Result<SignedTransaction> {
        parameters.validate()?;
        call.maximum_native_cost(parameters)?;
        self.sign_eip1559(TxEip1559 {
            chain_id: parameters.chain_id,
            nonce: parameters.nonce,
            gas_limit: parameters.gas_limit,
            max_fee_per_gas: parameters.max_fee_per_gas,
            max_priority_fee_per_gas: parameters.max_priority_fee_per_gas,
            to: TxKind::Call(call.target),
            value: call.value,
            access_list: Default::default(),
            input: Bytes::copy_from_slice(&call.calldata),
        })
    }

    fn sign_eip1559(&self, mut transaction: TxEip1559) -> anyhow::Result<SignedTransaction> {
        ensure!(
            transaction.chain_id != 0,
            "EIP-1559 transaction chain id is zero"
        );
        let chain_id = transaction.chain_id;
        let nonce = transaction.nonce;
        let signature = self
            .signer
            .sign_transaction_sync(&mut transaction)
            .context("failed to sign EIP-1559 transaction")?;
        let raw = transaction.into_signed(signature).encoded_2718();
        Ok(SignedTransaction {
            chain_id,
            nonce,
            hash: keccak256(&raw),
            raw,
        })
    }
}

pub struct SignedTransaction {
    pub chain_id: u64,
    pub nonce: u64,
    pub hash: B256,
    pub raw: Vec<u8>,
}

pub async fn broadcast_signed_transaction(
    rpc: &JsonRpcClient,
    transaction: &SignedTransaction,
) -> anyhow::Result<B256> {
    let submitted_hash = match rpc.send_raw_transaction(&transaction.raw).await {
        Ok(hash) => hash,
        Err(error) if rpc_already_knows_transaction(&error) => transaction.hash,
        Err(error) => return Err(error),
    };
    ensure!(
        submitted_hash == transaction.hash,
        "RPC returned a different transaction hash"
    );
    Ok(submitted_hash)
}

fn rpc_already_knows_transaction(error: &anyhow::Error) -> bool {
    error
        .to_string()
        .to_ascii_lowercase()
        .contains("already known")
}

impl std::fmt::Debug for SignedTransaction {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SignedTransaction")
            .field("chain_id", &self.chain_id)
            .field("nonce", &self.nonce)
            .field("hash", &self.hash)
            .field("raw", &"[REDACTED]")
            .finish()
    }
}

impl std::fmt::Debug for EvmWallet {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EvmWallet")
            .field("address", &self.address())
            .field("signer", &"[REDACTED]")
            .finish()
    }
}

fn validate_hydration_requests(
    tokens: &[TokenBalanceRequest],
    allowances: &[TokenAllowanceRequest],
) -> anyhow::Result<()> {
    for token in tokens {
        ensure!(
            !token.symbol.trim().is_empty(),
            "wallet token symbol is empty"
        );
        ensure!(
            token.contract != Address::ZERO,
            "wallet token contract is zero"
        );
    }
    for allowance in allowances {
        ensure!(
            !allowance.symbol.trim().is_empty(),
            "wallet allowance token symbol is empty"
        );
        ensure!(
            allowance.contract != Address::ZERO,
            "wallet allowance token contract is zero"
        );
        ensure!(
            allowance.spender != Address::ZERO,
            "wallet allowance spender is zero"
        );
    }
    Ok(())
}

fn erc20_balance_of_calldata(owner: Address) -> Vec<u8> {
    encode_address(ERC20_BALANCE_OF_SELECTOR, owner)
}

fn erc20_allowance_calldata(owner: Address, spender: Address) -> Vec<u8> {
    let mut data = Vec::with_capacity(68);
    data.extend_from_slice(&ERC20_ALLOWANCE_SELECTOR);
    push_address_word(&mut data, owner);
    push_address_word(&mut data, spender);
    data
}

fn encode_address(selector: [u8; 4], address: Address) -> Vec<u8> {
    let mut data = Vec::with_capacity(36);
    data.extend_from_slice(&selector);
    push_address_word(&mut data, address);
    data
}

fn encode_address_u256(selector: [u8; 4], address: Address, amount: U256) -> Vec<u8> {
    let mut data = Vec::with_capacity(68);
    data.extend_from_slice(&selector);
    push_address_word(&mut data, address);
    data.extend_from_slice(&amount.to_be_bytes::<32>());
    data
}

fn push_address_word(data: &mut Vec<u8>, address: Address) {
    data.extend_from_slice(&[0_u8; 12]);
    data.extend_from_slice(address.as_slice());
}

fn decode_abi_u256(name: &str, encoded: Vec<u8>) -> anyhow::Result<U256> {
    ensure!(encoded.len() == 32, "{name} result is not one ABI word");
    Ok(U256::from_be_slice(&encoded))
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, U256, keccak256};

    use super::{
        EvmWallet, WalletCall, WalletTransactionParameters, decode_abi_u256,
        erc20_allowance_calldata, erc20_balance_of_calldata, rpc_already_knows_transaction,
    };

    const PRIVATE_KEY: &str = "0x59c6995e998f97a5a0044976f7d04f8b2b7f4e5b5d5f3e49f2f4e7838a2b0c19";

    fn parameters() -> WalletTransactionParameters {
        WalletTransactionParameters {
            chain_id: 480,
            nonce: 7,
            gas_limit: 80_000,
            max_fee_per_gas: 2_000_000,
            max_priority_fee_per_gas: 1_000_000,
        }
    }

    #[test]
    fn encodes_balance_and_allowance_calls() {
        let owner = Address::repeat_byte(0x11);
        let spender = Address::repeat_byte(0x22);
        let balance = erc20_balance_of_calldata(owner);
        assert_eq!(&balance[..4], &[0x70, 0xa0, 0x82, 0x31]);
        assert_eq!(balance.len(), 36);
        assert_eq!(&balance[16..], owner.as_slice());

        let allowance = erc20_allowance_calldata(owner, spender);
        assert_eq!(&allowance[..4], &[0xdd, 0x62, 0xed, 0x3e]);
        assert_eq!(allowance.len(), 68);
        assert_eq!(&allowance[16..36], owner.as_slice());
        assert_eq!(&allowance[48..68], spender.as_slice());
    }

    #[test]
    fn encodes_exact_erc20_transfer_and_approval() {
        let token = Address::repeat_byte(0x33);
        let counterparty = Address::repeat_byte(0x44);
        let amount = U256::from(12_345_678_u64);

        let transfer = WalletCall::erc20_transfer(token, counterparty, amount).unwrap();
        assert_eq!(transfer.target(), token);
        assert_eq!(transfer.value(), U256::ZERO);
        assert_eq!(&transfer.calldata()[..4], &[0xa9, 0x05, 0x9c, 0xbb]);
        assert_eq!(&transfer.calldata()[16..36], counterparty.as_slice());
        assert_eq!(U256::from_be_slice(&transfer.calldata()[36..68]), amount);

        let approval = WalletCall::erc20_approval(token, counterparty, amount).unwrap();
        assert_eq!(&approval.calldata()[..4], &[0x09, 0x5e, 0xa7, 0xb3]);
        assert_eq!(&approval.calldata()[16..36], counterparty.as_slice());
        assert_eq!(U256::from_be_slice(&approval.calldata()[36..]), amount);
    }

    #[test]
    fn rejects_unsafe_transfer_inputs_but_permits_allowance_reset() {
        let token = Address::repeat_byte(0x33);
        let recipient = Address::repeat_byte(0x44);
        assert!(WalletCall::native_transfer(Address::ZERO, U256::ONE).is_err());
        assert!(WalletCall::native_transfer(recipient, U256::ZERO).is_err());
        assert!(WalletCall::erc20_transfer(Address::ZERO, recipient, U256::ONE).is_err());
        assert!(WalletCall::erc20_transfer(token, recipient, U256::ZERO).is_err());
        assert!(WalletCall::erc20_approval(token, recipient, U256::ZERO).is_ok());
        assert!(WalletCall::erc20_approval(token, Address::ZERO, U256::ONE).is_err());
    }

    #[test]
    fn computes_maximum_native_cost_with_checked_integer_math() {
        let call = WalletCall::native_transfer(Address::repeat_byte(0x55), U256::from(9)).unwrap();
        assert_eq!(
            call.maximum_native_cost(parameters()).unwrap(),
            U256::from(160_000_000_009_u64)
        );
    }

    #[test]
    fn signs_validated_call_with_chain_and_nonce_identity() {
        let wallet = EvmWallet::from_private_key(PRIVATE_KEY).unwrap();
        let call = WalletCall::erc20_transfer(
            Address::repeat_byte(0x33),
            Address::repeat_byte(0x44),
            U256::from(1_000_000),
        )
        .unwrap();
        let signed = wallet.sign_call(&call, parameters()).unwrap();

        assert_eq!(signed.chain_id, 480);
        assert_eq!(signed.nonce, 7);
        assert_eq!(signed.raw[0], 0x02);
        assert_eq!(signed.hash, keccak256(&signed.raw));
    }

    #[test]
    fn rejects_invalid_transaction_parameters() {
        let wallet = EvmWallet::from_private_key(PRIVATE_KEY).unwrap();
        let call = WalletCall::native_transfer(Address::repeat_byte(0x55), U256::ONE).unwrap();
        let mut invalid = parameters();
        invalid.chain_id = 0;
        assert!(wallet.sign_call(&call, invalid).is_err());
        invalid = parameters();
        invalid.gas_limit = 0;
        assert!(wallet.sign_call(&call, invalid).is_err());
        invalid = parameters();
        invalid.max_priority_fee_per_gas = invalid.max_fee_per_gas + 1;
        assert!(wallet.sign_call(&call, invalid).is_err());
    }

    #[test]
    fn debug_never_contains_private_key_or_raw_payload() {
        let wallet = EvmWallet::from_private_key(PRIVATE_KEY).unwrap();
        let call = WalletCall::native_transfer(Address::repeat_byte(0x55), U256::ONE).unwrap();
        let signed = wallet.sign_call(&call, parameters()).unwrap();

        let wallet_debug = format!("{wallet:?}");
        assert!(wallet_debug.contains("REDACTED"));
        assert!(!wallet_debug.contains("59c6995e"));
        let signed_debug = format!("{signed:?}");
        assert!(signed_debug.contains("REDACTED"));
        assert!(!signed_debug.contains(&alloy_primitives::hex::encode(&signed.raw)));
    }

    #[test]
    fn decodes_only_one_word_erc20_results() {
        let amount = U256::from(123_456_u64);
        assert_eq!(
            decode_abi_u256("balance", amount.to_be_bytes::<32>().to_vec()).unwrap(),
            amount
        );
        assert!(decode_abi_u256("balance", vec![0; 31]).is_err());
        assert!(decode_abi_u256("balance", vec![0; 33]).is_err());
    }

    #[test]
    fn treats_only_already_known_rpc_error_as_idempotent_broadcast() {
        assert!(rpc_already_knows_transaction(&anyhow::anyhow!(
            "JSON-RPC error -32000: already known"
        )));
        assert!(!rpc_already_knows_transaction(&anyhow::anyhow!(
            "JSON-RPC error -32000: nonce too low"
        )));
        assert!(!rpc_already_knows_transaction(&anyhow::anyhow!(
            "RPC HTTP send failed (timeout)"
        )));
    }
}

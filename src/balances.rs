use std::{collections::BTreeMap, sync::Arc, time::Instant};

use alloy_primitives::{Address, B256, U256};
use anyhow::ensure;
use rust_decimal::Decimal;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time::{MissedTickBehavior, interval_at},
};

use crate::{
    binance::account::{AccountInformation, BinanceAccountClient},
    chain::rpc::{CanonicalBlock, EthCall, JsonRpcClient, RpcStats},
    wallet::TokenBalanceRequest,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BalanceSource {
    Binance,
    Wallet,
}

impl BalanceSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Wallet => "wallet",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BinanceAssetBalance {
    pub free: Decimal,
    pub locked: Decimal,
}

#[derive(Debug, Clone)]
pub struct BinanceBalanceSnapshot {
    pub account_update_time_ms: u64,
    pub account_type: String,
    pub can_trade: bool,
    pub balances: BTreeMap<Arc<str>, BinanceAssetBalance>,
    pub observed_at: Instant,
    pub request_duration_us: u128,
}

impl BinanceBalanceSnapshot {
    pub fn healthy(&self) -> bool {
        self.account_type == "SPOT" && self.can_trade
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletTokenBalance {
    pub symbol: Arc<str>,
    pub contract: Address,
    pub base_units: U256,
}

#[derive(Debug, Clone)]
pub struct WalletBalanceSnapshot {
    pub owner: Address,
    pub chain_id: u64,
    pub block_number: u64,
    pub block_hash: B256,
    pub native_balance_wei: U256,
    pub token_balances: Vec<WalletTokenBalance>,
    pub observed_at: Instant,
    pub request_duration_us: u128,
    pub rpc_stats: RpcStats,
}

#[derive(Debug)]
pub enum BalanceEvent {
    Binance(BinanceBalanceSnapshot),
    Wallet(WalletBalanceSnapshot),
    Failed {
        source: BalanceSource,
        error: String,
        observed_at: Instant,
    },
}

pub struct BalanceSync {
    pub receiver: mpsc::Receiver<BalanceEvent>,
    pub wallet_heads: watch::Sender<CanonicalBlock>,
    pub binance_task: JoinHandle<anyhow::Result<()>>,
    pub wallet_task: JoinHandle<anyhow::Result<()>>,
}

#[allow(clippy::too_many_arguments)]
pub fn spawn_balance_sync(
    mut binance: BinanceAccountClient,
    binance_assets: Vec<Arc<str>>,
    binance_interval: std::time::Duration,
    wallet_rpc: JsonRpcClient,
    wallet_owner: Address,
    wallet_chain_id: u64,
    wallet_tokens: Vec<TokenBalanceRequest>,
    initial_head: CanonicalBlock,
    channel_capacity: usize,
) -> BalanceSync {
    let (sender, receiver) = mpsc::channel(channel_capacity);
    let (wallet_heads, mut wallet_head_receiver) = watch::channel(initial_head);

    let binance_sender = sender.clone();
    let binance_task = tokio::spawn(async move {
        let start = tokio::time::Instant::now() + binance_interval;
        let mut tick = interval_at(start, binance_interval);
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            let started = Instant::now();
            let account = match binance.account_information().await {
                Ok(account) => Ok(account),
                Err(first_error) => match binance.synchronize_clock().await {
                    Ok(()) => binance.account_information().await,
                    Err(clock_error) => Err(anyhow::anyhow!(
                        "{first_error:#}; Binance clock resynchronization failed: {clock_error:#}"
                    )),
                },
            };
            let event = match account {
                Ok(account) => BalanceEvent::Binance(binance_snapshot(
                    &account,
                    &binance_assets,
                    started.elapsed().as_micros(),
                )),
                Err(error) => BalanceEvent::Failed {
                    source: BalanceSource::Binance,
                    error: format!("{error:#}"),
                    observed_at: Instant::now(),
                },
            };
            if binance_sender.send(event).await.is_err() {
                return Ok(());
            }
        }
    });

    let wallet_task = tokio::spawn(async move {
        while wallet_head_receiver.changed().await.is_ok() {
            let head = *wallet_head_receiver.borrow_and_update();
            let event = match fetch_wallet_snapshot(
                &wallet_rpc,
                wallet_owner,
                wallet_chain_id,
                &wallet_tokens,
                head,
            )
            .await
            {
                Ok(snapshot) => BalanceEvent::Wallet(snapshot),
                Err(error) => BalanceEvent::Failed {
                    source: BalanceSource::Wallet,
                    error: format!("{error:#}"),
                    observed_at: Instant::now(),
                },
            };
            if sender.send(event).await.is_err() {
                return Ok(());
            }
        }
        Ok(())
    });

    BalanceSync {
        receiver,
        wallet_heads,
        binance_task,
        wallet_task,
    }
}

pub fn binance_snapshot(
    account: &AccountInformation,
    expected_assets: &[Arc<str>],
    request_duration_us: u128,
) -> BinanceBalanceSnapshot {
    let balances = expected_assets
        .iter()
        .map(|asset| {
            let observed = account
                .balances
                .iter()
                .find(|balance| balance.asset == asset.as_ref());
            (
                Arc::clone(asset),
                BinanceAssetBalance {
                    free: observed.map_or(Decimal::ZERO, |balance| balance.free),
                    locked: observed.map_or(Decimal::ZERO, |balance| balance.locked),
                },
            )
        })
        .collect();
    BinanceBalanceSnapshot {
        account_update_time_ms: account.update_time,
        account_type: account.account_type.clone(),
        can_trade: account.can_trade,
        balances,
        observed_at: Instant::now(),
        request_duration_us,
    }
}

pub async fn fetch_wallet_snapshot(
    rpc: &JsonRpcClient,
    owner: Address,
    chain_id: u64,
    tokens: &[TokenBalanceRequest],
    block: CanonicalBlock,
) -> anyhow::Result<WalletBalanceSnapshot> {
    ensure!(!tokens.is_empty(), "wallet token set is empty");
    let started = Instant::now();
    let calls = tokens
        .iter()
        .map(|token| EthCall {
            to: token.contract,
            data: erc20_balance_of_call(owner),
        })
        .collect::<Vec<_>>();
    let (native_balance_wei, encoded_balances) = tokio::try_join!(
        rpc.native_balance_at(owner, block),
        rpc.eth_call_batch(&calls, block),
    )?;
    ensure!(
        encoded_balances.len() == tokens.len(),
        "wallet token balance response count mismatch"
    );
    let token_balances = tokens
        .iter()
        .zip(encoded_balances)
        .map(|(token, encoded)| {
            ensure!(
                encoded.len() == 32,
                "ERC-20 balance result for {} is not one ABI word",
                token.symbol
            );
            Ok(WalletTokenBalance {
                symbol: Arc::from(token.symbol.as_str()),
                contract: token.contract,
                base_units: U256::from_be_slice(&encoded),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(WalletBalanceSnapshot {
        owner,
        chain_id,
        block_number: block.number,
        block_hash: block.hash,
        native_balance_wei,
        token_balances,
        observed_at: Instant::now(),
        request_duration_us: started.elapsed().as_micros(),
        rpc_stats: rpc.stats(),
    })
}

fn erc20_balance_of_call(owner: Address) -> Vec<u8> {
    let mut data = Vec::with_capacity(36);
    data.extend_from_slice(&[0x70, 0xa0, 0x82, 0x31]);
    data.extend_from_slice(&[0_u8; 12]);
    data.extend_from_slice(owner.as_slice());
    data
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rust_decimal::Decimal;

    use crate::binance::account::{AccountInformation, AssetBalance};

    use super::{binance_snapshot, erc20_balance_of_call};

    #[test]
    fn binance_snapshot_includes_expected_zero_balances() {
        let account = AccountInformation {
            can_trade: true,
            can_withdraw: true,
            can_deposit: true,
            brokered: false,
            require_self_trade_prevention: false,
            update_time: 12,
            account_type: "SPOT".to_owned(),
            balances: vec![AssetBalance {
                asset: "USDC".to_owned(),
                free: Decimal::from(5),
                locked: Decimal::ONE,
            }],
            permissions: vec!["SPOT".to_owned()],
        };
        let snapshot = binance_snapshot(&account, &[Arc::from("USDC"), Arc::from("WLD")], 7);

        assert_eq!(snapshot.balances["USDC"].free, Decimal::from(5));
        assert_eq!(snapshot.balances["WLD"].free, Decimal::ZERO);
        assert_eq!(snapshot.request_duration_us, 7);
        assert!(snapshot.healthy());
    }

    #[test]
    fn encodes_erc20_balance_of_without_floating_point() {
        let owner = "0x00000000000000000000000000000000000000ab"
            .parse()
            .unwrap();
        let encoded = erc20_balance_of_call(owner);
        assert_eq!(&encoded[..4], &[0x70, 0xa0, 0x82, 0x31]);
        assert_eq!(encoded.len(), 36);
        assert_eq!(&encoded[16..], owner.as_slice());
    }
}

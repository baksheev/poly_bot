use std::str::FromStr;

use alloy_primitives::{Address, B256};
use anyhow::{Context, bail, ensure};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;

use super::account::BinanceAccountClient;

const DEPOSIT_ADDRESS_ENDPOINT: &str = "/sapi/v1/capital/deposit/address";
const DEPOSIT_TRAVEL_RULE_ENDPOINT: &str = "/sapi/v2/localentity/deposit/provide-info";
const STANDARD_WITHDRAWAL_ENDPOINT: &str = "/sapi/v1/capital/withdraw/apply";

impl BinanceAccountClient {
    pub async fn all_coin_information(&self) -> anyhow::Result<Vec<CoinInformation>> {
        let query = self.signed_query(&[])?;
        self.signed_get(
            "/sapi/v1/capital/config/getall",
            &query,
            "capital configuration",
        )
        .await
    }

    pub async fn capital_routes(
        &self,
        coin: &str,
        direct_network: &str,
        fallback_network: &str,
    ) -> anyhow::Result<CapitalRouteState> {
        let coins = self.all_coin_information().await?;
        select_capital_routes(&coins, coin, direct_network, fallback_network)
    }

    pub async fn hydrate_capital_recovery(
        &mut self,
        coin: &str,
        network: &str,
        deposit_transaction_hash: Option<&str>,
        withdraw_order_id: Option<&str>,
    ) -> anyhow::Result<CapitalRecoverySnapshot> {
        validate_symbol("coin", coin)?;
        validate_symbol("network", network)?;
        self.synchronize_clock().await?;
        let deposit_address = self.evm_deposit_address(coin, network).await?;
        let deposits = match deposit_transaction_hash {
            Some(hash) => self.deposit_history(coin, hash).await?,
            None => Vec::new(),
        };
        ensure!(
            deposits.iter().all(|deposit| deposit.coin == coin),
            "Binance deposit history returned a different coin"
        );
        ensure!(
            deposits.iter().all(|deposit| deposit.network == network),
            "Binance deposit history returned a different network"
        );
        let withdrawals = match withdraw_order_id {
            Some(order_id) => self.withdrawal_history(coin, order_id).await?,
            None => Vec::new(),
        };
        ensure!(
            withdrawals.iter().all(|withdrawal| withdrawal.coin == coin),
            "Binance withdrawal history returned a different coin"
        );
        ensure!(
            withdrawals
                .iter()
                .all(|withdrawal| withdrawal.network == network),
            "Binance withdrawal history returned a different network"
        );
        Ok(CapitalRecoverySnapshot {
            coin: coin.to_owned(),
            network: network.to_owned(),
            deposit_address,
            deposits,
            withdrawals,
        })
    }

    pub async fn evm_deposit_address(
        &self,
        coin: &str,
        network: &str,
    ) -> anyhow::Result<EvmDepositAddress> {
        validate_symbol("coin", coin)?;
        validate_symbol("network", network)?;
        let query = self.signed_query(&[
            ("coin", coin.to_owned()),
            ("network", network.to_owned()),
            ("recvWindow", "5000".to_owned()),
        ])?;
        let address: DepositAddressRecord = self
            .signed_get(DEPOSIT_ADDRESS_ENDPOINT, &query, "deposit address")
            .await?;
        select_evm_deposit_address(&address, coin, network)
    }

    pub async fn deposit_history(
        &self,
        coin: &str,
        transaction_hash: &str,
    ) -> anyhow::Result<Vec<DepositRecord>> {
        validate_symbol("coin", coin)?;
        let expected_hash = validate_evm_transaction_hash(transaction_hash)?;
        let query = self.signed_query(&[
            ("coin", coin.to_owned()),
            ("txId", transaction_hash.to_owned()),
            ("retrieveQuestionnaire", "true".to_owned()),
            ("recvWindow", "5000".to_owned()),
        ])?;
        let records: Vec<DepositRecord> = self
            .signed_get(
                "/sapi/v2/localentity/deposit/history",
                &query,
                "Travel Rule deposit history",
            )
            .await?;
        matching_deposits(records, coin, expected_hash)
    }

    pub async fn submit_deposit_questionnaire(
        &self,
        deposit_id: &str,
    ) -> anyhow::Result<WithdrawalSubmission> {
        ensure!(
            !deposit_id.is_empty()
                && deposit_id.len() <= 128
                && deposit_id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
            "Binance deposit id is invalid"
        );
        let query = self.signed_query(&[
            ("depositId", deposit_id.to_owned()),
            (
                "questionnaire",
                serde_json::json!({
                    "depositOriginator": 1,
                    "receiveFrom": 1,
                })
                .to_string(),
            ),
            ("recvWindow", "5000".to_owned()),
        ])?;
        self.signed_put(
            DEPOSIT_TRAVEL_RULE_ENDPOINT,
            &query,
            "Travel Rule deposit questionnaire",
        )
        .await
    }

    pub async fn withdraw_standard(
        &self,
        coin: &str,
        network: &str,
        address: &str,
        amount: Decimal,
        withdraw_order_id: &str,
    ) -> anyhow::Result<StandardWithdrawalSubmission> {
        validate_symbol("coin", coin)?;
        validate_symbol("network", network)?;
        ensure!(
            address.starts_with("0x")
                && address.len() == 42
                && address[2..].bytes().all(|byte| byte.is_ascii_hexdigit()),
            "Binance withdrawal address must be an EVM address"
        );
        ensure!(amount > Decimal::ZERO, "withdrawal amount must be positive");
        validate_withdraw_order_id(withdraw_order_id)?;
        let query = self.signed_query(&[
            ("coin", coin.to_owned()),
            ("address", address.to_owned()),
            ("amount", amount.normalize().to_string()),
            ("withdrawOrderId", withdraw_order_id.to_owned()),
            ("network", network.to_owned()),
            ("walletType", "0".to_owned()),
            ("recvWindow", "5000".to_owned()),
        ])?;
        let submission: StandardWithdrawalSubmission = self
            .signed_post(
                STANDARD_WITHDRAWAL_ENDPOINT,
                &query,
                "standard withdrawal submission",
            )
            .await?;
        ensure!(
            !submission.id.trim().is_empty(),
            "Binance standard withdrawal returned an empty id"
        );
        Ok(submission)
    }

    pub async fn withdrawal_history(
        &self,
        coin: &str,
        withdraw_order_id: &str,
    ) -> anyhow::Result<Vec<WithdrawalRecord>> {
        validate_symbol("coin", coin)?;
        validate_withdraw_order_id(withdraw_order_id)?;
        let query =
            self.signed_query(&[("coin", coin.to_owned()), ("recvWindow", "5000".to_owned())])?;
        let records: Vec<WithdrawalRecord> = self
            .signed_get(
                "/sapi/v1/capital/withdraw/history",
                &query,
                "withdrawal history",
            )
            .await?;
        matching_withdrawals(records, coin, withdraw_order_id)
    }

    pub async fn withdrawal_history_for_coin(
        &self,
        coin: &str,
    ) -> anyhow::Result<Vec<WithdrawalRecord>> {
        validate_symbol("coin", coin)?;
        let query =
            self.signed_query(&[("coin", coin.to_owned()), ("recvWindow", "5000".to_owned())])?;
        let records: Vec<WithdrawalRecord> = self
            .signed_get(
                "/sapi/v1/capital/withdraw/history",
                &query,
                "withdrawal history",
            )
            .await?;
        ensure!(
            records.iter().all(|record| record.coin == coin),
            "Binance withdrawal history returned another coin"
        );
        Ok(records)
    }

    pub async fn withdrawal_address_list(&self) -> anyhow::Result<Vec<WithdrawalAddressRecord>> {
        let query = self.signed_query(&[("recvWindow", "5000".to_owned())])?;
        self.signed_get(
            "/sapi/v1/capital/withdraw/address/list",
            &query,
            "withdrawal address list",
        )
        .await
    }

    pub async fn travel_rule_withdrawal_history(
        &self,
        tr_id: i64,
    ) -> anyhow::Result<Vec<TravelRuleWithdrawalRecord>> {
        ensure!(tr_id > 0, "Travel Rule id must be positive");
        let query = self.signed_query(&[
            ("trId", tr_id.to_string()),
            ("recvWindow", "5000".to_owned()),
        ])?;
        self.signed_get(
            "/sapi/v1/localentity/withdraw/history",
            &query,
            "Travel Rule withdrawal history",
        )
        .await
    }

    pub async fn travel_rule_questionnaire_requirements(
        &self,
    ) -> anyhow::Result<TravelRuleQuestionnaireRequirements> {
        let query = self.signed_query(&[("recvWindow", "5000".to_owned())])?;
        self.signed_get(
            "/sapi/v1/localentity/questionnaire-requirements",
            &query,
            "Travel Rule questionnaire requirements",
        )
        .await
    }

    pub async fn address_verification_list(
        &self,
    ) -> anyhow::Result<Vec<AddressVerificationRecord>> {
        let query = self.signed_query(&[("recvWindow", "5000".to_owned())])?;
        let response: Value = self
            .signed_get(
                "/sapi/v1/addressVerify/list",
                &query,
                "Travel Rule address verification list",
            )
            .await?;
        parse_address_verification_list(response)
    }

    pub async fn travel_rule_withdrawal_history_v2(
        &self,
        coin: &str,
        network: &str,
        withdraw_order_id: &str,
    ) -> anyhow::Result<Vec<TravelRuleWithdrawalRecord>> {
        validate_symbol("coin", coin)?;
        validate_symbol("network", network)?;
        validate_withdraw_order_id(withdraw_order_id)?;
        let query = self.signed_query(&[
            ("coin", coin.to_owned()),
            ("network", network.to_owned()),
            ("withdrawOrderId", withdraw_order_id.to_owned()),
            ("recvWindow", "5000".to_owned()),
        ])?;
        let records: Vec<TravelRuleWithdrawalRecord> = self
            .signed_get(
                "/sapi/v2/localentity/withdraw/history",
                &query,
                "Travel Rule withdrawal history v2",
            )
            .await?;
        let matching = records
            .into_iter()
            .filter(|record| record.coin == coin && record.withdraw_order_id == withdraw_order_id)
            .collect::<Vec<_>>();
        ensure!(
            matching.len() <= 1,
            "Binance returned duplicate Travel Rule withdrawals for one client id"
        );
        if let Some(record) = matching.first() {
            ensure!(
                record.network.is_empty() || record.network == network,
                "Binance returned a Travel Rule withdrawal for the client id on another network"
            );
        }
        Ok(matching)
    }

    pub async fn travel_rule_withdrawal_history_v2_for_network(
        &self,
        coin: &str,
        network: &str,
    ) -> anyhow::Result<Vec<TravelRuleWithdrawalRecord>> {
        validate_symbol("coin", coin)?;
        validate_symbol("network", network)?;
        let query = self.signed_query(&[
            ("coin", coin.to_owned()),
            ("network", network.to_owned()),
            ("recvWindow", "5000".to_owned()),
        ])?;
        let records: Vec<TravelRuleWithdrawalRecord> = self
            .signed_get(
                "/sapi/v2/localentity/withdraw/history",
                &query,
                "Travel Rule withdrawal history v2",
            )
            .await?;
        for record in &records {
            ensure!(
                record.coin == coin && (record.network.is_empty() || record.network == network),
                "Binance returned a Travel Rule withdrawal outside the requested asset or network"
            );
        }
        Ok(records)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TravelRuleQuestionnaireRequirements {
    #[serde(default)]
    pub questionnaire_country_code: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AddressVerificationRecord {
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub network: String,
    #[serde(default)]
    pub wallet_address: String,
    #[serde(default)]
    pub address_questionnaire: AddressVerificationQuestionnaire,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AddressVerificationQuestionnaire {
    pub send_to: Option<i64>,
    #[serde(default)]
    pub satoshi_token: String,
    pub is_address_owner: Option<i64>,
    pub verify_method: Option<i64>,
}

fn parse_address_verification_list(
    response: Value,
) -> anyhow::Result<Vec<AddressVerificationRecord>> {
    let records = match response {
        Value::Array(records) => Value::Array(records),
        Value::Object(mut envelope) => {
            let array_fields = envelope
                .iter()
                .filter_map(|(key, value)| value.is_array().then_some(key.clone()))
                .collect::<Vec<_>>();
            ensure!(
                array_fields.len() == 1,
                "Binance Travel Rule address verification response envelope must contain exactly one record array"
            );
            let records = envelope
                .remove(&array_fields[0])
                .expect("the selected response envelope field exists");
            ensure!(
                envelope.values().all(|value| {
                    value.is_null() || value.is_boolean() || value.is_number() || value.is_string()
                }),
                "Binance Travel Rule address verification response envelope contains unsupported fields"
            );
            records
        }
        _ => bail!("Binance Travel Rule address verification response is not a list or envelope"),
    };
    serde_json::from_value(records)
        .context("invalid Binance Travel Rule address verification record list")
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapitalRecoverySnapshot {
    pub coin: String,
    pub network: String,
    pub deposit_address: EvmDepositAddress,
    pub deposits: Vec<DepositRecord>,
    pub withdrawals: Vec<WithdrawalRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvmDepositAddress {
    pub coin: String,
    pub network: String,
    pub address: Address,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct DepositAddressRecord {
    coin: String,
    address: String,
    #[serde(default)]
    tag: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DepositRecord {
    #[serde(alias = "tranId")]
    pub deposit_id: String,
    #[serde(deserialize_with = "deserialize_decimal")]
    pub amount: Decimal,
    pub network: String,
    pub coin: String,
    pub deposit_status: u8,
    #[serde(default, alias = "travelRuleStatus")]
    pub travel_rule_req_status: Option<u8>,
    pub address: String,
    #[serde(default)]
    pub address_tag: String,
    pub tx_id: String,
    pub insert_time: u64,
    #[serde(default)]
    pub transfer_type: Option<u8>,
    #[serde(default)]
    pub confirm_times: String,
    #[serde(default)]
    pub require_questionnaire: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DepositCreditState {
    Pending,
    Credited,
    CreditedWithdrawalLocked,
    Unknown(u8),
}

impl DepositRecord {
    pub fn credit_state(&self) -> DepositCreditState {
        match self.deposit_status {
            0 => DepositCreditState::Pending,
            1 => DepositCreditState::Credited,
            6 => DepositCreditState::CreditedWithdrawalLocked,
            status => DepositCreditState::Unknown(status),
        }
    }

    pub fn is_credited(&self) -> bool {
        matches!(
            self.credit_state(),
            DepositCreditState::Credited | DepositCreditState::CreditedWithdrawalLocked
        )
    }

    pub fn questionnaire_required(&self) -> bool {
        self.require_questionnaire && self.travel_rule_req_status != Some(0)
    }
}

impl DepositCreditState {
    pub fn label(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Credited => "credited",
            Self::CreditedWithdrawalLocked => "credited_withdrawal_locked",
            Self::Unknown(_) => "unknown",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WithdrawalState {
    EmailSent,
    Cancelled,
    AwaitingApproval,
    Rejected,
    Processing,
    Failed,
    Completed,
    Unknown(u8),
}

impl WithdrawalState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Cancelled | Self::Rejected | Self::Failed | Self::Completed
        )
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::EmailSent => "email_sent",
            Self::Cancelled => "cancelled",
            Self::AwaitingApproval => "awaiting_approval",
            Self::Rejected => "rejected",
            Self::Processing => "processing",
            Self::Failed => "failed",
            Self::Completed => "completed",
            Self::Unknown(_) => "unknown",
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WithdrawalSubmission {
    pub tr_id: i64,
    pub accepted: bool,
    #[serde(default)]
    pub info: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct StandardWithdrawalSubmission {
    pub id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WithdrawalAddressRecord {
    #[serde(default)]
    pub address: String,
    #[serde(default)]
    pub address_tag: String,
    #[serde(default)]
    pub coin: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub network: String,
    #[serde(default)]
    pub origin: String,
    #[serde(default)]
    pub origin_type: String,
    #[serde(default)]
    pub white_status: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TravelRuleWithdrawalRecord {
    #[serde(default)]
    pub id: String,
    pub tr_id: i64,
    #[serde(default)]
    pub amount: String,
    #[serde(default)]
    pub transaction_fee: String,
    #[serde(default)]
    pub coin: String,
    #[serde(default)]
    pub withdrawal_status: Option<i64>,
    #[serde(default)]
    pub travel_rule_status: i64,
    #[serde(default)]
    pub address: String,
    #[serde(default)]
    pub tx_id: String,
    #[serde(default)]
    pub network: String,
    #[serde(default)]
    pub withdraw_order_id: String,
    #[serde(default)]
    pub info: String,
}

impl TravelRuleWithdrawalRecord {
    pub fn is_failed_without_broadcast(&self) -> bool {
        self.travel_rule_status == 2
            && self.withdrawal_status != Some(6)
            && self.tx_id.trim().is_empty()
    }

    pub fn is_approved_without_withdrawal(&self) -> bool {
        self.travel_rule_status == 4
            && self.withdrawal_status.is_none()
            && self.tx_id.trim().is_empty()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WithdrawalRecord {
    pub id: String,
    #[serde(deserialize_with = "deserialize_decimal")]
    pub amount: Decimal,
    #[serde(deserialize_with = "deserialize_decimal")]
    pub transaction_fee: Decimal,
    pub coin: String,
    pub status: u8,
    pub address: String,
    #[serde(default)]
    pub tx_id: String,
    pub network: String,
    #[serde(default)]
    pub withdraw_order_id: String,
    #[serde(default)]
    pub info: String,
}

impl WithdrawalRecord {
    pub fn state(&self) -> WithdrawalState {
        match self.status {
            0 => WithdrawalState::EmailSent,
            1 => WithdrawalState::Cancelled,
            2 => WithdrawalState::AwaitingApproval,
            3 => WithdrawalState::Rejected,
            4 => WithdrawalState::Processing,
            5 => WithdrawalState::Failed,
            6 => WithdrawalState::Completed,
            status => WithdrawalState::Unknown(status),
        }
    }
}

fn select_evm_deposit_address(
    record: &DepositAddressRecord,
    coin: &str,
    network: &str,
) -> anyhow::Result<EvmDepositAddress> {
    ensure!(
        record.coin == coin,
        "Binance returned a deposit address for a different coin"
    );
    ensure!(
        record.tag.is_empty(),
        "Binance EVM deposit address unexpectedly requires a tag"
    );
    let address = record
        .address
        .parse::<Address>()
        .context("Binance deposit address is not an EVM address")?;
    ensure!(address != Address::ZERO, "Binance deposit address is zero");
    Ok(EvmDepositAddress {
        coin: record.coin.clone(),
        network: network.to_owned(),
        address,
    })
}

fn matching_deposits(
    records: Vec<DepositRecord>,
    coin: &str,
    expected_hash: B256,
) -> anyhow::Result<Vec<DepositRecord>> {
    let mut matching = Vec::new();
    for record in records {
        ensure!(
            record.amount > Decimal::ZERO,
            "Binance deposit amount is not positive"
        );
        let transaction_hash = validate_evm_transaction_hash(&record.tx_id)
            .context("Binance deposit history contains an invalid transaction hash")?;
        if transaction_hash == expected_hash {
            ensure!(record.coin == coin, "Binance deposit history coin mismatch");
            matching.push(record);
        }
    }
    ensure!(
        matching.len() <= 1,
        "Binance returned duplicate deposits for one transaction hash"
    );
    Ok(matching)
}

fn matching_withdrawals(
    records: Vec<WithdrawalRecord>,
    coin: &str,
    withdraw_order_id: &str,
) -> anyhow::Result<Vec<WithdrawalRecord>> {
    let matching = records
        .into_iter()
        .filter(|record| record.withdraw_order_id == withdraw_order_id)
        .collect::<Vec<_>>();
    ensure!(
        matching.len() <= 1,
        "Binance returned duplicate withdrawals for one client id"
    );
    ensure!(
        matching.iter().all(|record| record.coin == coin),
        "Binance withdrawal history coin mismatch"
    );
    Ok(matching)
}

pub fn select_capital_routes(
    coins: &[CoinInformation],
    coin: &str,
    direct_network: &str,
    fallback_network: &str,
) -> anyhow::Result<CapitalRouteState> {
    validate_symbol("coin", coin)?;
    validate_symbol("direct network", direct_network)?;
    validate_symbol("fallback network", fallback_network)?;

    let coin_state = coins
        .iter()
        .find(|candidate| candidate.coin == coin)
        .with_context(|| format!("Binance capital configuration is missing coin {coin}"))?;
    let direct = coin_state
        .network_list
        .iter()
        .find(|network| network.network == direct_network)
        .cloned();
    let fallback = coin_state
        .network_list
        .iter()
        .find(|network| network.network == fallback_network)
        .cloned();

    ensure!(
        direct.is_some() || fallback.is_some(),
        "Binance capital configuration has neither {direct_network} nor {fallback_network} for {coin}"
    );
    Ok(CapitalRouteState {
        coin: coin_state.coin.clone(),
        deposit_all_enabled: coin_state.deposit_all_enable,
        withdrawal_all_enabled: coin_state.withdraw_all_enable,
        direct,
        fallback,
    })
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CoinInformation {
    pub coin: String,
    pub deposit_all_enable: bool,
    pub withdraw_all_enable: bool,
    #[serde(default)]
    pub network_list: Vec<NetworkInformation>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NetworkInformation {
    pub network: String,
    pub name: String,
    pub deposit_enable: bool,
    pub withdraw_enable: bool,
    pub busy: bool,
    #[serde(deserialize_with = "deserialize_decimal")]
    pub withdraw_fee: Decimal,
    #[serde(deserialize_with = "deserialize_decimal")]
    pub withdraw_min: Decimal,
    #[serde(deserialize_with = "deserialize_decimal")]
    pub withdraw_max: Decimal,
    #[serde(deserialize_with = "deserialize_decimal")]
    pub withdraw_integer_multiple: Decimal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapitalRouteState {
    pub coin: String,
    pub deposit_all_enabled: bool,
    pub withdrawal_all_enabled: bool,
    pub direct: Option<NetworkInformation>,
    pub fallback: Option<NetworkInformation>,
}

impl CapitalRouteState {
    pub fn direct_deposit_available(&self) -> bool {
        self.deposit_all_enabled
            && self
                .direct
                .as_ref()
                .is_some_and(NetworkInformation::deposit_available)
    }

    pub fn direct_withdrawal_available(&self) -> bool {
        self.withdrawal_all_enabled
            && self
                .direct
                .as_ref()
                .is_some_and(NetworkInformation::withdrawal_available)
    }

    pub fn fallback_deposit_available(&self) -> bool {
        self.deposit_all_enabled
            && self
                .fallback
                .as_ref()
                .is_some_and(NetworkInformation::deposit_available)
    }

    pub fn fallback_withdrawal_available(&self) -> bool {
        self.withdrawal_all_enabled
            && self
                .fallback
                .as_ref()
                .is_some_and(NetworkInformation::withdrawal_available)
    }
}

impl NetworkInformation {
    pub fn deposit_available(&self) -> bool {
        self.deposit_enable && !self.busy
    }

    pub fn withdrawal_available(&self) -> bool {
        self.withdraw_enable && !self.busy
    }
}

fn validate_symbol(name: &str, value: &str) -> anyhow::Result<()> {
    ensure!(
        !value.is_empty()
            && value
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit()),
        "Binance {name} must contain only uppercase ASCII letters and digits"
    );
    Ok(())
}

fn validate_withdraw_order_id(value: &str) -> anyhow::Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 64
            && value.bytes().all(|byte| byte.is_ascii_alphanumeric()),
        "withdrawal client id is invalid"
    );
    Ok(())
}

fn validate_evm_transaction_hash(value: &str) -> anyhow::Result<B256> {
    let hash = value
        .parse::<B256>()
        .context("EVM transaction hash is invalid")?;
    ensure!(hash != B256::ZERO, "EVM transaction hash is zero");
    Ok(hash)
}

fn deserialize_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    Decimal::from_str(&value).map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, B256};

    use super::{
        AddressVerificationRecord, CoinInformation, DEPOSIT_ADDRESS_ENDPOINT,
        DEPOSIT_TRAVEL_RULE_ENDPOINT, DepositAddressRecord, DepositCreditState, DepositRecord,
        NetworkInformation, STANDARD_WITHDRAWAL_ENDPOINT, StandardWithdrawalSubmission,
        TravelRuleQuestionnaireRequirements, TravelRuleWithdrawalRecord, WithdrawalAddressRecord,
        WithdrawalRecord, WithdrawalState, WithdrawalSubmission, matching_deposits,
        matching_withdrawals, parse_address_verification_list, select_capital_routes,
        select_evm_deposit_address,
    };

    const WLD: &str = r#"{
      "coin":"WLD",
      "depositAllEnable":true,
      "withdrawAllEnable":true,
      "networkList":[
        {
          "network":"OPTIMISM",
          "name":"Optimism",
          "depositEnable":true,
          "withdrawEnable":true,
          "busy":false,
          "withdrawFee":"0.068",
          "withdrawMin":"0.14",
          "withdrawMax":"5005005",
          "withdrawIntegerMultiple":"0.00000001"
        },
        {
          "network":"WLD",
          "name":"World Chain",
          "depositEnable":true,
          "withdrawEnable":false,
          "busy":false,
          "withdrawFee":"0.06",
          "withdrawMin":"0.2",
          "withdrawMax":"8700000",
          "withdrawIntegerMultiple":"0.00000001"
        }
      ]
    }"#;

    #[test]
    fn parses_numeric_travel_rule_submission_id() {
        let submission: WithdrawalSubmission = serde_json::from_str(
            r#"{"trId":65865740,"accepted":true,"info":"Withdrawal request accepted"}"#,
        )
        .unwrap();

        assert_eq!(submission.tr_id, 65_865_740);
        assert!(submission.accepted);
    }

    #[test]
    fn parses_standard_withdrawal_submission_id() {
        let submission: StandardWithdrawalSubmission =
            serde_json::from_str(r#"{"id":"7213fea8e94b4a5593d507237e5a555b"}"#).unwrap();

        assert_eq!(submission.id, "7213fea8e94b4a5593d507237e5a555b");
    }

    #[test]
    fn parses_account_specific_travel_rule_questionnaire_country() {
        let requirements: TravelRuleQuestionnaireRequirements =
            serde_json::from_str(r#"{"questionnaireCountryCode":"SG"}"#).unwrap();
        assert_eq!(
            requirements.questionnaire_country_code.as_deref(),
            Some("SG")
        );

        let absent: TravelRuleQuestionnaireRequirements = serde_json::from_str(r#"{}"#).unwrap();
        assert_eq!(absent.questionnaire_country_code, None);
    }

    #[test]
    fn parses_address_verification_and_failed_travel_rule_history() {
        let verification: AddressVerificationRecord = serde_json::from_str(
            r#"{
              "status":"VERIFIED","token":"ESP","network":"ARBITRUM",
              "walletAddress":"0x1111111111111111111111111111111111111111",
              "addressQuestionnaire":{
                "sendTo":1,"satoshiToken":"ESP","isAddressOwner":1,"verifyMethod":2
              }
            }"#,
        )
        .unwrap();
        assert_eq!(verification.status, "VERIFIED");
        assert_eq!(verification.address_questionnaire.send_to, Some(1));
        assert_eq!(verification.address_questionnaire.is_address_owner, Some(1));

        let failed: TravelRuleWithdrawalRecord = serde_json::from_str(
            r#"{
              "trId":65865741,"coin":"ESP","withdrawalStatus":3,
              "travelRuleStatus":2,"network":"ARBITRUM",
              "withdrawOrderId":"rustwd3","txId":""
            }"#,
        )
        .unwrap();
        assert!(failed.is_failed_without_broadcast());

        let completed: TravelRuleWithdrawalRecord = serde_json::from_str(
            r#"{
              "trId":65865742,"coin":"ESP","withdrawalStatus":6,
              "travelRuleStatus":0,"network":"ARBITRUM",
              "withdrawOrderId":"rustwd4","txId":"0xabc"
            }"#,
        )
        .unwrap();
        assert!(!completed.is_failed_without_broadcast());

        let approved_without_withdrawal: TravelRuleWithdrawalRecord = serde_json::from_str(
            r#"{
              "trId":67181540,"coin":"ESP","amount":"400","transactionFee":"1.2",
              "travelRuleStatus":4,"network":"ARBITRUM",
              "withdrawOrderId":"rustwd5","txId":""
            }"#,
        )
        .unwrap();
        assert_eq!(approved_without_withdrawal.withdrawal_status, None);
        assert!(approved_without_withdrawal.is_approved_without_withdrawal());
    }

    #[test]
    fn parses_documented_and_live_enveloped_address_verification_lists() {
        let record = serde_json::json!({
            "status":"VERIFIED","token":"ESP","network":"ARBITRUM",
            "walletAddress":"0x1111111111111111111111111111111111111111",
            "addressQuestionnaire":{
                "sendTo":1,"satoshiToken":"ESP","isAddressOwner":1,"verifyMethod":2
            }
        });
        let documented = parse_address_verification_list(serde_json::json!([record.clone()]))
            .expect("the documented response list should parse");
        let live_envelope = parse_address_verification_list(serde_json::json!({
            "data":[record],
            "success":true,
            "total":1
        }))
        .expect("the live response envelope should parse");

        assert_eq!(documented, live_envelope);
        assert_eq!(live_envelope[0].token, "ESP");
    }

    #[test]
    fn rejects_ambiguous_or_nested_address_verification_envelopes() {
        assert!(
            parse_address_verification_list(serde_json::json!({
                "data":[],
                "records":[]
            }))
            .is_err()
        );
        assert!(
            parse_address_verification_list(serde_json::json!({
                "data":[],
                "metadata":{"page":1}
            }))
            .is_err()
        );
    }

    #[test]
    fn parses_coin_scoped_withdrawal_whitelist_records() {
        let records: Vec<WithdrawalAddressRecord> = serde_json::from_str(
            r#"[{
                "address":"0x90D990C81320221D2882De32beeA78923c1e77A3",
                "addressTag":"",
                "coin":"USDC",
                "name":"Arbitrum canary",
                "network":"ARBITRUM",
                "origin":"others",
                "originType":"others",
                "whiteStatus":true
            }]"#,
        )
        .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].coin, "USDC");
        assert_eq!(records[0].network, "ARBITRUM");
        assert!(records[0].white_status);
    }

    #[test]
    fn preserves_rejected_travel_rule_submission_for_fail_closed_handling() {
        let submission: WithdrawalSubmission =
            serde_json::from_str(r#"{"trId":65865741,"accepted":false,"info":"Rejected"}"#)
                .unwrap();

        assert!(!submission.accepted);
        assert_eq!(submission.info, "Rejected");
    }

    #[test]
    fn parses_capital_and_travel_rule_history_without_floating_point() {
        let capital: WithdrawalRecord = serde_json::from_str(
            r#"{
              "id":"withdrawal-id","amount":"0.009985","transactionFee":"0.000015",
              "coin":"ETH","status":6,"address":"0x1111111111111111111111111111111111111111",
              "txId":"0xabc","network":"OPTIMISM","withdrawOrderId":"rustwd1","info":""
            }"#,
        )
        .unwrap();
        assert_eq!(capital.amount.to_string(), "0.009985");
        assert_eq!(capital.transaction_fee.to_string(), "0.000015");

        let travel_rule: TravelRuleWithdrawalRecord = serde_json::from_str(
            r#"{
              "id":"withdrawal-id","trId":65865740,"amount":"0.009985",
              "transactionFee":"0.000015","coin":"ETH","withdrawalStatus":6,
              "travelRuleStatus":4,"address":"0x1111111111111111111111111111111111111111",
              "txId":"0xabc","network":"OPTIMISM","withdrawOrderId":"rustwd1","info":""
            }"#,
        )
        .unwrap();
        assert_eq!(travel_rule.tr_id, 65_865_740);
        assert_eq!(travel_rule.withdrawal_status, Some(6));
        assert_eq!(travel_rule.travel_rule_status, 4);
    }

    #[test]
    fn rejects_missing_coin_and_invalid_symbol_in_route_selection() {
        let coin: CoinInformation = serde_json::from_str(WLD).unwrap();
        assert!(
            select_capital_routes(std::slice::from_ref(&coin), "USDC", "WLD", "OPTIMISM").is_err()
        );
        assert!(select_capital_routes(&[coin], "wld", "WLD", "OPTIMISM").is_err());
    }

    #[test]
    fn parses_live_wld_network_fields_without_floating_point() {
        let coin: CoinInformation = serde_json::from_str(WLD).unwrap();
        let optimism = coin
            .network_list
            .iter()
            .find(|network| network.network == "OPTIMISM")
            .unwrap();

        assert_eq!(optimism.withdraw_fee.to_string(), "0.068");
        assert_eq!(optimism.withdraw_integer_multiple.to_string(), "0.00000001");
        assert!(optimism.withdrawal_available());
    }

    #[test]
    fn disabled_or_busy_network_is_not_available() {
        let coin: CoinInformation = serde_json::from_str(WLD).unwrap();
        let world = coin
            .network_list
            .iter()
            .find(|network| network.network == "WLD")
            .unwrap();
        assert!(!world.withdrawal_available());

        let mut optimism: NetworkInformation = coin.network_list[0].clone();
        optimism.busy = true;
        assert!(!optimism.deposit_available());
        assert!(!optimism.withdrawal_available());
    }

    #[test]
    fn selects_direct_and_fallback_from_one_capital_snapshot() {
        let coin: CoinInformation = serde_json::from_str(WLD).unwrap();
        let state = select_capital_routes(&[coin], "WLD", "WLD", "OPTIMISM").unwrap();

        assert!(!state.direct_withdrawal_available());
        assert!(state.direct_deposit_available());
        assert!(state.fallback_withdrawal_available());
        assert!(state.fallback_deposit_available());
    }

    #[test]
    fn selects_the_exact_network_scoped_evm_deposit_address() {
        let record: DepositAddressRecord = serde_json::from_str(
            r#"{"coin":"USDC","address":"0x2222222222222222222222222222222222222222",
                 "tag":"","url":"https://optimistic.etherscan.io/address/0x22"}"#,
        )
        .unwrap();

        let selected = select_evm_deposit_address(&record, "USDC", "OPTIMISM").unwrap();
        assert_eq!(selected.coin, "USDC");
        assert_eq!(selected.network, "OPTIMISM");
        assert_eq!(selected.address, Address::repeat_byte(0x22));
    }

    #[test]
    fn uses_the_singular_network_scoped_deposit_address_endpoint() {
        assert_eq!(DEPOSIT_ADDRESS_ENDPOINT, "/sapi/v1/capital/deposit/address");
        assert!(!DEPOSIT_ADDRESS_ENDPOINT.ends_with("/list"));
    }

    #[test]
    fn rejects_wrong_coin_tagged_or_non_evm_deposit_addresses() {
        let wrong_coin: DepositAddressRecord = serde_json::from_str(
            r#"{"coin":"ETH","address":"0x2222222222222222222222222222222222222222",
                 "tag":""}"#,
        )
        .unwrap();
        assert!(select_evm_deposit_address(&wrong_coin, "USDC", "OPTIMISM").is_err());

        let tagged: DepositAddressRecord = serde_json::from_str(
            r#"{"coin":"USDC","address":"0x2222222222222222222222222222222222222222",
                 "tag":"memo"}"#,
        )
        .unwrap();
        assert!(select_evm_deposit_address(&tagged, "USDC", "OPTIMISM").is_err());

        let invalid: DepositAddressRecord =
            serde_json::from_str(r#"{"coin":"USDC","address":"not-an-address","tag":""}"#).unwrap();
        assert!(select_evm_deposit_address(&invalid, "USDC", "OPTIMISM").is_err());
    }

    #[test]
    fn parses_travel_rule_deposit_status_without_floating_point() {
        let record: DepositRecord = serde_json::from_str(
            r#"{
              "depositId":"4615328107052018946","amount":"10.50","network":"OPTIMISM",
              "coin":"USDC","depositStatus":6,"travelRuleReqStatus":3,
              "address":"0x64d62673799a8dc69825ff1cc0d624b1065dab39","addressTag":"",
              "txId":"0x519f3a47cec440e3bff25d069785a8c3d07911d774316dcde0701b3dcd90c343",
              "transferType":0,"confirmTimes":"2/1","requireQuestionnaire":true,
              "insertTime":1765735358000
            }"#,
        )
        .unwrap();

        assert_eq!(record.amount.to_string(), "10.50");
        assert_eq!(
            record.credit_state(),
            DepositCreditState::CreditedWithdrawalLocked
        );
        assert!(record.is_credited());
        assert!(record.questionnaire_required());
    }

    #[test]
    fn deposit_travel_rule_is_driven_only_by_the_deposit_record() {
        let mut record: DepositRecord = serde_json::from_str(
            r#"{
              "depositId":"1","amount":"0.1","network":"ARBITRUM","coin":"ESP",
              "depositStatus":1,"travelRuleReqStatus":3,
              "address":"0x64d62673799a8dc69825ff1cc0d624b1065dab39","addressTag":"",
              "txId":"0x519f3a47cec440e3bff25d069785a8c3d07911d774316dcde0701b3dcd90c343",
              "transferType":0,"confirmTimes":"1/1","requireQuestionnaire":false,
              "insertTime":1765735358000
            }"#,
        )
        .unwrap();

        assert!(!record.questionnaire_required());
        record.require_questionnaire = true;
        assert!(record.questionnaire_required());
        record.travel_rule_req_status = Some(0);
        assert!(!record.questionnaire_required());
    }

    #[test]
    fn withdrawal_and_deposit_travel_rule_endpoints_are_distinct_invariants() {
        assert_eq!(
            STANDARD_WITHDRAWAL_ENDPOINT,
            "/sapi/v1/capital/withdraw/apply"
        );
        assert_eq!(
            DEPOSIT_TRAVEL_RULE_ENDPOINT,
            "/sapi/v2/localentity/deposit/provide-info"
        );
        assert!(!STANDARD_WITHDRAWAL_ENDPOINT.contains("localentity"));
        assert!(DEPOSIT_TRAVEL_RULE_ENDPOINT.contains("/deposit/"));
    }

    #[test]
    fn matches_deposit_by_case_insensitive_evm_hash_and_rejects_duplicates() {
        let expected = B256::repeat_byte(0xab);
        let json = format!(
            r#"{{
              "depositId":"1","amount":"1.25","network":"OPTIMISM","coin":"USDC",
              "depositStatus":1,"address":"0x1111111111111111111111111111111111111111",
              "txId":"{:#X}","insertTime":1
            }}"#,
            expected
        );
        let record: DepositRecord = serde_json::from_str(&json).unwrap();
        let matching = matching_deposits(vec![record.clone()], "USDC", expected).unwrap();
        assert_eq!(matching, vec![record.clone()]);
        assert!(matching_deposits(vec![record.clone(), record], "USDC", expected).is_err());
    }

    #[test]
    fn matches_withdrawal_locally_by_client_id_and_types_terminal_statuses() {
        let completed: WithdrawalRecord = serde_json::from_str(
            r#"{
              "id":"withdrawal-id","amount":"0.009985","transactionFee":"0.000015",
              "coin":"ETH","status":6,"address":"0x1111111111111111111111111111111111111111",
              "txId":"0xabc","network":"OPTIMISM","withdrawOrderId":"rustwd2","info":""
            }"#,
        )
        .unwrap();
        let mut other = completed.clone();
        other.withdraw_order_id = "rustwd1".to_owned();

        let matching =
            matching_withdrawals(vec![other, completed.clone()], "ETH", "rustwd2").unwrap();
        assert_eq!(matching, vec![completed.clone()]);
        assert_eq!(completed.state(), WithdrawalState::Completed);
        assert!(completed.state().is_terminal());
        assert!(!WithdrawalState::Processing.is_terminal());
    }
}

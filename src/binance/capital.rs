use std::str::FromStr;

use anyhow::{Context, ensure};
use rust_decimal::Decimal;
use serde::Deserialize;

use super::account::BinanceAccountClient;

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

    pub async fn withdraw(
        &self,
        coin: &str,
        network: &str,
        address: &str,
        amount: Decimal,
        withdraw_order_id: &str,
    ) -> anyhow::Result<WithdrawalSubmission> {
        validate_symbol("coin", coin)?;
        validate_symbol("network", network)?;
        ensure!(
            address.starts_with("0x")
                && address.len() == 42
                && address[2..].bytes().all(|byte| byte.is_ascii_hexdigit()),
            "Binance withdrawal address must be an EVM address"
        );
        ensure!(amount > Decimal::ZERO, "withdrawal amount must be positive");
        ensure!(
            !withdraw_order_id.is_empty()
                && withdraw_order_id.len() <= 64
                && withdraw_order_id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric()),
            "withdrawal client id is invalid"
        );
        let query = self.signed_query(&[
            ("coin", coin.to_owned()),
            ("address", address.to_owned()),
            ("amount", amount.normalize().to_string()),
            ("withdrawOrderId", withdraw_order_id.to_owned()),
            ("network", network.to_owned()),
            ("walletType", "0".to_owned()),
            (
                "questionnaire",
                serde_json::json!({
                    "isAddressOwner": 1,
                    "sendTo": 1,
                })
                .to_string(),
            ),
            ("recvWindow", "5000".to_owned()),
        ])?;
        self.signed_post(
            "/sapi/v1/localentity/withdraw/apply",
            &query,
            "Travel Rule withdrawal submission",
        )
        .await
    }

    pub async fn withdrawal_history(
        &self,
        coin: &str,
        withdraw_order_id: &str,
    ) -> anyhow::Result<Vec<WithdrawalRecord>> {
        validate_symbol("coin", coin)?;
        let query = self.signed_query(&[
            ("coin", coin.to_owned()),
            ("withdrawOrderId", withdraw_order_id.to_owned()),
            ("recvWindow", "5000".to_owned()),
        ])?;
        self.signed_get(
            "/sapi/v1/capital/withdraw/history",
            &query,
            "withdrawal history",
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
    pub withdrawal_status: i64,
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

#[derive(Clone, Debug, Deserialize)]
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

fn deserialize_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    Decimal::from_str(&value).map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::{
        CoinInformation, NetworkInformation, TravelRuleWithdrawalRecord, WithdrawalRecord,
        WithdrawalSubmission, select_capital_routes,
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
        assert_eq!(travel_rule.withdrawal_status, 6);
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
}

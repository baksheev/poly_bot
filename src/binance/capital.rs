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
    use super::{CoinInformation, NetworkInformation, select_capital_routes};

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

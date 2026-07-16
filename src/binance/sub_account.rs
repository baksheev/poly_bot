use std::str::FromStr;

use anyhow::{Context, ensure};
use rust_decimal::Decimal;
use serde::Deserialize;

use super::account::BinanceAccountClient;

impl BinanceAccountClient {
    pub async fn subaccount_spot_assets(&self, email: &str) -> anyhow::Result<SubAccountAssets> {
        validate_email(email)?;
        let query = self.signed_query(&[
            ("email", email.to_owned()),
            ("recvWindow", "5000".to_owned()),
        ])?;
        self.signed_get("/sapi/v4/sub-account/assets", &query, "sub-account assets")
            .await
    }

    pub async fn universal_transfer_from_subaccount(
        &self,
        from_email: &str,
        asset: &str,
        amount: Decimal,
        client_transaction_id: &str,
    ) -> anyhow::Result<UniversalTransferSubmission> {
        validate_email(from_email)?;
        validate_asset(asset)?;
        validate_client_transaction_id(client_transaction_id)?;
        ensure!(amount > Decimal::ZERO, "transfer amount must be positive");
        let query = self.signed_query(&[
            ("fromEmail", from_email.to_owned()),
            ("fromAccountType", "SPOT".to_owned()),
            ("toAccountType", "SPOT".to_owned()),
            ("asset", asset.to_owned()),
            ("amount", amount.normalize().to_string()),
            ("clientTranId", client_transaction_id.to_owned()),
            ("recvWindow", "5000".to_owned()),
        ])?;
        let submission: UniversalTransferSubmission = self
            .signed_post(
                "/sapi/v1/sub-account/universalTransfer",
                &query,
                "sub-account universal transfer",
            )
            .await?;
        ensure!(submission.transaction_id > 0, "transfer id is zero");
        ensure!(
            submission.client_transaction_id == client_transaction_id,
            "Binance returned a different transfer client id"
        );
        Ok(submission)
    }

    pub async fn universal_transfer_history(
        &self,
        from_email: &str,
        client_transaction_id: &str,
    ) -> anyhow::Result<Vec<UniversalTransferRecord>> {
        validate_email(from_email)?;
        validate_client_transaction_id(client_transaction_id)?;
        let query = self.signed_query(&[
            ("fromEmail", from_email.to_owned()),
            ("clientTranId", client_transaction_id.to_owned()),
            ("page", "1".to_owned()),
            ("limit", "10".to_owned()),
            ("recvWindow", "5000".to_owned()),
        ])?;
        let response: UniversalTransferHistory = self
            .signed_get(
                "/sapi/v1/sub-account/universalTransfer",
                &query,
                "sub-account universal transfer history",
            )
            .await?;
        let matching = response
            .records
            .into_iter()
            .filter(|record| record.client_transaction_id == client_transaction_id)
            .collect::<Vec<_>>();
        ensure!(
            matching.len() <= 1,
            "Binance returned duplicate universal transfers for one client id"
        );
        Ok(matching)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UniversalTransferSubmission {
    #[serde(rename = "tranId")]
    pub transaction_id: u64,
    #[serde(rename = "clientTranId")]
    pub client_transaction_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UniversalTransferRecord {
    #[serde(rename = "tranId")]
    pub transaction_id: u64,
    pub from_email: String,
    pub to_email: String,
    pub asset: String,
    #[serde(deserialize_with = "deserialize_decimal")]
    pub amount: Decimal,
    pub from_account_type: String,
    pub to_account_type: String,
    pub status: String,
    #[serde(rename = "clientTranId")]
    pub client_transaction_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UniversalTransferHistory {
    #[serde(rename = "result")]
    records: Vec<UniversalTransferRecord>,
}

#[derive(Debug, Deserialize)]
pub struct SubAccountAssets {
    #[serde(deserialize_with = "deserialize_balances")]
    pub balances: Vec<SubAccountAssetBalance>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubAccountAssetBalance {
    pub asset: String,
    pub free: Decimal,
    pub locked: Decimal,
}

#[derive(Deserialize)]
struct WireAssetBalance {
    asset: String,
    free: String,
    locked: String,
}

fn validate_email(email: &str) -> anyhow::Result<()> {
    let (local, domain) = email
        .split_once('@')
        .context("Binance sub-account email is invalid")?;
    ensure!(
        !local.is_empty()
            && !domain.is_empty()
            && email.len() <= 254
            && email.is_ascii()
            && !domain.contains('@'),
        "Binance sub-account email is invalid"
    );
    Ok(())
}

fn validate_asset(asset: &str) -> anyhow::Result<()> {
    ensure!(
        !asset.is_empty()
            && asset.len() <= 16
            && asset
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit()),
        "Binance transfer asset is invalid"
    );
    Ok(())
}

fn validate_client_transaction_id(value: &str) -> anyhow::Result<()> {
    ensure!(
        (8..=32).contains(&value.len()) && value.bytes().all(|byte| byte.is_ascii_alphanumeric()),
        "Binance transfer client id is invalid"
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

fn deserialize_balances<'de, D>(deserializer: D) -> Result<Vec<SubAccountAssetBalance>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Vec::<WireAssetBalance>::deserialize(deserializer)?
        .into_iter()
        .map(|balance| {
            Ok(SubAccountAssetBalance {
                asset: balance.asset,
                free: Decimal::from_str(&balance.free).map_err(serde::de::Error::custom)?,
                locked: Decimal::from_str(&balance.locked).map_err(serde::de::Error::custom)?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::{
        SubAccountAssets, UniversalTransferHistory, UniversalTransferSubmission, validate_email,
    };

    #[test]
    fn parses_transfer_submission_and_history_exactly() {
        let submission: UniversalTransferSubmission =
            serde_json::from_str(r#"{"tranId":123456789,"clientTranId":"rb12345678"}"#).unwrap();
        assert_eq!(submission.transaction_id, 123456789);
        assert_eq!(submission.client_transaction_id, "rb12345678");

        let history: UniversalTransferHistory = serde_json::from_str(
            r#"{"result":[{"tranId":123456789,"fromEmail":"sub@example.com","toEmail":"master@example.com","asset":"USDC","amount":"500.000001","fromAccountType":"SPOT","toAccountType":"SPOT","status":"SUCCESS","clientTranId":"rb12345678"}],"totalCount":1}"#,
        )
        .unwrap();
        assert_eq!(history.records.len(), 1);
        assert_eq!(history.records[0].amount, Decimal::new(500_000_001, 6));
    }

    #[test]
    fn parses_master_view_of_subaccount_assets() {
        let assets: SubAccountAssets = serde_json::from_str(
            r#"{"balances":[{"asset":"USDC","free":"1000.0","locked":"0.0"}]}"#,
        )
        .unwrap();
        assert_eq!(assets.balances[0].free, Decimal::from(1_000));
    }

    #[test]
    fn rejects_malformed_subaccount_email() {
        assert!(validate_email("sub@example.com").is_ok());
        assert!(validate_email("subexample.com").is_err());
        assert!(validate_email("sub@example@com").is_err());
    }
}

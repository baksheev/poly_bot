use std::{fmt, str::FromStr, time::Duration};

use anyhow::{Context, bail, ensure};
use hmac::{Hmac, Mac};
use reqwest::{Client, StatusCode};
use rust_decimal::Decimal;
use serde::Deserialize;
use sha2::Sha256;

use crate::config::AppConfig;

const API_KEY_HEADER: &str = "X-MBX-APIKEY";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const RECV_WINDOW_MS: u64 = 5_000;

type HmacSha256 = Hmac<Sha256>;

pub struct BinanceCredentials {
    api_key: String,
    secret_key: String,
}

impl BinanceCredentials {
    pub fn from_env() -> anyhow::Result<Self> {
        let api_key = std::env::var("BINANCE_API_KEY")
            .context("required environment variable BINANCE_API_KEY is not set")?;
        let secret_key = std::env::var("BINANCE_SECRET_KEY")
            .context("required environment variable BINANCE_SECRET_KEY is not set")?;
        ensure!(!api_key.trim().is_empty(), "BINANCE_API_KEY is empty");
        ensure!(!secret_key.trim().is_empty(), "BINANCE_SECRET_KEY is empty");
        Ok(Self {
            api_key,
            secret_key,
        })
    }

    #[cfg(test)]
    fn new(api_key: &str, secret_key: &str) -> Self {
        Self {
            api_key: api_key.to_owned(),
            secret_key: secret_key.to_owned(),
        }
    }
}

impl fmt::Debug for BinanceCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BinanceCredentials")
            .field("api_key", &"<redacted>")
            .field("secret_key", &"<redacted>")
            .finish()
    }
}

pub struct BinanceAccountClient {
    http: Client,
    base_url: String,
    credentials: BinanceCredentials,
    clock_offset_ms: i64,
}

impl BinanceAccountClient {
    pub fn from_env(config: &AppConfig) -> anyhow::Result<Self> {
        Self::new(
            &config.binance_rest_base_url,
            BinanceCredentials::from_env()?,
        )
    }

    fn new(base_url: &str, credentials: BinanceCredentials) -> anyhow::Result<Self> {
        let http = Client::builder()
            .connect_timeout(REQUEST_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_nodelay(true)
            .build()
            .context("failed to construct Binance HTTP client")?;
        Ok(Self {
            http,
            base_url: base_url.trim_end_matches('/').to_owned(),
            credentials,
            clock_offset_ms: 0,
        })
    }

    pub async fn hydrate(&mut self, symbol: &str) -> anyhow::Result<BinanceAccountState> {
        self.synchronize_clock().await?;
        let account = self.account_information().await?;
        let commission = self.commission_rates(symbol).await?;
        ensure!(
            commission.symbol == symbol,
            "Binance commission response returned symbol {}, expected {symbol}",
            commission.symbol
        );
        Ok(BinanceAccountState {
            clock_offset_ms: self.clock_offset_ms,
            account,
            commission,
        })
    }

    pub async fn synchronize_clock(&mut self) -> anyhow::Result<()> {
        let local_before = unix_timestamp_ms()?;
        let response = self
            .http
            .get(format!("{}/api/v3/time", self.base_url))
            .send()
            .await
            .map_err(|error| {
                anyhow::anyhow!(
                    "Binance server-time request failed: {}",
                    error.without_url()
                )
            })?;
        let server_time: ServerTime = decode_response(response, "server time").await?;
        let local_after = unix_timestamp_ms()?;
        let local_midpoint = local_before.saturating_add(local_after) / 2;
        self.clock_offset_ms = signed_difference(server_time.server_time, local_midpoint)?;
        Ok(())
    }

    pub async fn account_information(&self) -> anyhow::Result<AccountInformation> {
        let query = self.signed_query(&[
            ("omitZeroBalances", "true".to_owned()),
            ("recvWindow", RECV_WINDOW_MS.to_string()),
        ])?;
        self.signed_get("/api/v3/account", &query, "account information")
            .await
    }

    pub async fn commission_rates(&self, symbol: &str) -> anyhow::Result<CommissionRates> {
        ensure!(
            symbol
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit()),
            "Binance symbol must contain only uppercase ASCII letters and digits"
        );
        let query = self.signed_query(&[("symbol", symbol.to_owned())])?;
        self.signed_get("/api/v3/account/commission", &query, "account commission")
            .await
    }

    pub(super) async fn signed_get<T>(
        &self,
        path: &str,
        query: &str,
        operation: &str,
    ) -> anyhow::Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let response = self
            .http
            .get(format!("{}{}", self.base_url, path))
            .header(API_KEY_HEADER, &self.credentials.api_key)
            .query(&parse_query_pairs(query))
            .query(&[("signature", sign_hex(&self.credentials.secret_key, query)?)])
            .send()
            .await
            .map_err(|error| {
                anyhow::anyhow!(
                    "Binance {operation} request failed: {}",
                    error.without_url()
                )
            })?;
        decode_response(response, operation).await
    }

    pub(super) fn signed_query(&self, parameters: &[(&str, String)]) -> anyhow::Result<String> {
        let local_timestamp = unix_timestamp_ms()?;
        let timestamp = apply_clock_offset(local_timestamp, self.clock_offset_ms)?;
        let mut query = parameters
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>();
        query.push(format!("timestamp={timestamp}"));
        Ok(query.join("&"))
    }
}

#[derive(Debug)]
pub struct BinanceAccountState {
    pub clock_offset_ms: i64,
    pub account: AccountInformation,
    pub commission: CommissionRates,
}

impl BinanceAccountState {
    pub fn balance(&self, asset: &str) -> Option<&AssetBalance> {
        self.account
            .balances
            .iter()
            .find(|balance| balance.asset == asset)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountInformation {
    pub can_trade: bool,
    pub can_withdraw: bool,
    pub can_deposit: bool,
    pub brokered: bool,
    pub require_self_trade_prevention: bool,
    pub update_time: u64,
    pub account_type: String,
    #[serde(deserialize_with = "deserialize_balances")]
    pub balances: Vec<AssetBalance>,
    #[serde(default)]
    pub permissions: Vec<String>,
}

#[derive(Debug)]
pub struct AssetBalance {
    pub asset: String,
    pub free: Decimal,
    pub locked: Decimal,
}

#[derive(Debug, Deserialize)]
struct WireAssetBalance {
    asset: String,
    free: String,
    locked: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommissionRates {
    pub symbol: String,
    pub standard_commission: CommissionSideRates,
    pub special_commission: CommissionSideRates,
    pub tax_commission: CommissionSideRates,
    pub discount: CommissionDiscount,
}

#[derive(Debug, Deserialize)]
pub struct CommissionSideRates {
    #[serde(deserialize_with = "deserialize_decimal")]
    pub maker: Decimal,
    #[serde(deserialize_with = "deserialize_decimal")]
    pub taker: Decimal,
    #[serde(deserialize_with = "deserialize_decimal")]
    pub buyer: Decimal,
    #[serde(deserialize_with = "deserialize_decimal")]
    pub seller: Decimal,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommissionDiscount {
    pub enabled_for_account: bool,
    pub enabled_for_symbol: bool,
    pub discount_asset: String,
    #[serde(deserialize_with = "deserialize_decimal")]
    pub discount: Decimal,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServerTime {
    server_time: u64,
}

#[derive(Deserialize)]
struct BinanceError {
    code: i64,
    msg: String,
}

async fn decode_response<T>(response: reqwest::Response, operation: &str) -> anyhow::Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let status = response.status();
    let body = response
        .bytes()
        .await
        .with_context(|| format!("failed to read Binance {operation} response"))?;
    if status != StatusCode::OK {
        if let Ok(error) = serde_json::from_slice::<BinanceError>(&body) {
            bail!(
                "Binance {operation} failed with HTTP {status}, code {}: {}",
                error.code,
                error.msg
            );
        }
        bail!("Binance {operation} failed with HTTP {status}");
    }
    serde_json::from_slice(&body)
        .with_context(|| format!("invalid Binance {operation} response JSON"))
}

fn deserialize_balances<'de, D>(deserializer: D) -> Result<Vec<AssetBalance>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let balances = Vec::<WireAssetBalance>::deserialize(deserializer)?;
    balances
        .into_iter()
        .map(|balance| {
            Ok(AssetBalance {
                asset: balance.asset,
                free: Decimal::from_str(&balance.free).map_err(serde::de::Error::custom)?,
                locked: Decimal::from_str(&balance.locked).map_err(serde::de::Error::custom)?,
            })
        })
        .collect()
}

fn deserialize_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    Decimal::from_str(&value).map_err(serde::de::Error::custom)
}

fn sign_hex(secret: &str, payload: &str) -> anyhow::Result<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|_| anyhow::anyhow!("failed to initialize Binance HMAC signer"))?;
    mac.update(payload.as_bytes());
    let bytes = mac.finalize().into_bytes();
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(encoded)
}

fn parse_query_pairs(query: &str) -> Vec<(&str, &str)> {
    query
        .split('&')
        .map(|pair| pair.split_once('=').expect("signed query pair has equals"))
        .collect()
}

fn unix_timestamp_ms() -> anyhow::Result<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_millis()
        .try_into()
        .context("Unix timestamp does not fit into u64")
}

fn signed_difference(left: u64, right: u64) -> anyhow::Result<i64> {
    let difference = i128::from(left) - i128::from(right);
    difference
        .try_into()
        .context("Binance clock difference does not fit into i64")
}

fn apply_clock_offset(timestamp: u64, offset: i64) -> anyhow::Result<u64> {
    let adjusted = i128::from(timestamp) + i128::from(offset);
    adjusted
        .try_into()
        .context("adjusted Binance timestamp does not fit into u64")
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::{BinanceCredentials, CommissionRates, apply_clock_offset, sign_hex};

    #[test]
    fn produces_binance_hmac_example_signature() {
        let payload = "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559";
        let secret = "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j";
        assert_eq!(
            sign_hex(secret, payload).unwrap(),
            "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71"
        );
    }

    #[test]
    fn credential_debug_is_redacted() {
        let credentials = BinanceCredentials::new("api-value", "secret-value");
        let rendered = format!("{credentials:?}");
        assert!(!rendered.contains("api-value"));
        assert!(!rendered.contains("secret-value"));
    }

    #[test]
    fn parses_commission_decimals_exactly() {
        let commission: CommissionRates = serde_json::from_str(
            r#"{
              "symbol":"WLDUSDC",
              "standardCommission":{"maker":"0.001","taker":"0.002","buyer":"0","seller":"0"},
              "specialCommission":{"maker":"0","taker":"0","buyer":"0","seller":"0"},
              "taxCommission":{"maker":"0","taker":"0","buyer":"0","seller":"0"},
              "discount":{"enabledForAccount":true,"enabledForSymbol":true,"discountAsset":"BNB","discount":"0.75"}
            }"#,
        )
        .unwrap();
        assert_eq!(commission.standard_commission.taker, Decimal::new(2, 3));
        assert_eq!(commission.discount.discount, Decimal::new(75, 2));
    }

    #[test]
    fn applies_positive_and_negative_clock_offsets() {
        assert_eq!(apply_clock_offset(1_000, 25).unwrap(), 1_025);
        assert_eq!(apply_clock_offset(1_000, -25).unwrap(), 975);
    }
}

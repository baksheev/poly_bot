use std::{
    fmt,
    str::FromStr,
    time::{Duration, Instant},
};

use anyhow::{Context, bail, ensure};
use hmac::{Hmac, Mac};
use reqwest::{Client, StatusCode};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;
use sha2::Sha256;

use crate::{
    binance::depth::{DepthSnapshot, parse_depth_snapshot},
    config::AppConfig,
};

const API_KEY_HEADER: &str = "X-MBX-APIKEY";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const RECV_WINDOW_MS: u64 = 5_000;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct BinanceCredentials {
    api_key: String,
    secret_key: String,
}

impl BinanceCredentials {
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_env_names("BINANCE_API_KEY", "BINANCE_SECRET_KEY")
    }

    fn from_env_names(api_key_name: &str, secret_key_name: &str) -> anyhow::Result<Self> {
        let api_key = std::env::var(api_key_name)
            .with_context(|| format!("required environment variable {api_key_name} is not set"))?;
        let secret_key = std::env::var(secret_key_name).with_context(|| {
            format!("required environment variable {secret_key_name} is not set")
        })?;
        ensure!(!api_key.trim().is_empty(), "{api_key_name} is empty");
        ensure!(!secret_key.trim().is_empty(), "{secret_key_name} is empty");
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

    pub(super) fn api_key(&self) -> &str {
        &self.api_key
    }

    pub(super) fn sign(&self, payload: &str) -> anyhow::Result<String> {
        sign_hex(&self.secret_key, payload)
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

impl Clone for BinanceAccountClient {
    fn clone(&self) -> Self {
        Self {
            http: self.http.clone(),
            base_url: self.base_url.clone(),
            credentials: self.credentials.clone(),
            clock_offset_ms: self.clock_offset_ms,
        }
    }
}

impl BinanceAccountClient {
    pub fn from_env(config: &AppConfig) -> anyhow::Result<Self> {
        Self::new(
            &config.binance_rest_base_url,
            BinanceCredentials::from_env()?,
        )
    }

    pub fn from_treasury_env(config: &AppConfig) -> anyhow::Result<Self> {
        Self::new(
            &config.binance_rest_base_url,
            BinanceCredentials::from_env_names(
                "BINANCE_TREASURY_API_KEY",
                "BINANCE_TREASURY_SECRET_KEY",
            )?,
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
        let clock_sync = self.synchronize_clock_observed().await?;
        let (account, commission, exchange_information, open_orders, order_rate_limits) = tokio::try_join!(
            self.account_information(),
            self.commission_rates(symbol),
            self.exchange_information(symbol),
            self.open_orders(symbol),
            self.order_rate_limits(),
        )?;
        ensure!(
            commission.symbol == symbol,
            "Binance commission response returned symbol {}, expected {symbol}",
            commission.symbol
        );
        let symbol_rules = exchange_information.symbol_rules(symbol)?;
        Ok(BinanceAccountState {
            clock_offset_ms: self.clock_offset_ms,
            clock_sync,
            account,
            commission,
            symbol_rules,
            open_orders,
            order_rate_limits,
        })
    }

    pub async fn synchronize_clock(&mut self) -> anyhow::Result<()> {
        self.synchronize_clock_observed().await.map(|_| ())
    }

    pub async fn synchronize_clock_observed(&mut self) -> anyhow::Result<BinanceClockSync> {
        let round_trip_started = Instant::now();
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
        Ok(BinanceClockSync {
            offset_ms: self.clock_offset_ms,
            round_trip_us: round_trip_started
                .elapsed()
                .as_micros()
                .min(u128::from(u64::MAX)) as u64,
            observed_at: Instant::now(),
            observed_unix_ms: local_after,
        })
    }

    pub async fn account_information(&self) -> anyhow::Result<AccountInformation> {
        let query = self.signed_query(&[
            ("omitZeroBalances", "true".to_owned()),
            ("recvWindow", RECV_WINDOW_MS.to_string()),
        ])?;
        self.signed_get("/api/v3/account", &query, "account information")
            .await
    }

    pub async fn api_key_permissions(&self) -> anyhow::Result<ApiKeyPermissions> {
        let query = self.signed_query(&[("recvWindow", "5000".to_owned())])?;
        self.signed_get(
            "/sapi/v1/account/apiRestrictions",
            &query,
            "API key permissions",
        )
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

    pub async fn exchange_information(&self, symbol: &str) -> anyhow::Result<ExchangeInformation> {
        validate_symbol(symbol)?;
        let response = self
            .http
            .get(format!("{}/api/v3/exchangeInfo", self.base_url))
            .query(&[("symbol", symbol)])
            .send()
            .await
            .map_err(|error| {
                anyhow::anyhow!(
                    "Binance exchange information request failed: {}",
                    error.without_url()
                )
            })?;
        decode_response(response, "exchange information").await
    }

    pub async fn open_orders(&self, symbol: &str) -> anyhow::Result<Vec<OpenOrder>> {
        validate_symbol(symbol)?;
        let query = self.signed_query(&[("symbol", symbol.to_owned())])?;
        self.signed_get("/api/v3/openOrders", &query, "open orders")
            .await
    }

    pub async fn order_rate_limits(&self) -> anyhow::Result<Vec<OrderRateLimit>> {
        let query = self.signed_query(&[])?;
        self.signed_get("/api/v3/rateLimit/order", &query, "order rate limits")
            .await
    }

    pub async fn depth_snapshot(&self, symbol: &str, limit: u16) -> anyhow::Result<DepthSnapshot> {
        validate_symbol(symbol)?;
        ensure!(
            [5_u16, 10, 20, 50, 100, 500, 1_000, 5_000].contains(&limit),
            "unsupported Binance depth snapshot limit {limit}"
        );
        let response = self
            .http
            .get(format!("{}/api/v3/depth", self.base_url))
            .query(&[("symbol", symbol.to_owned()), ("limit", limit.to_string())])
            .send()
            .await
            .map_err(|error| {
                anyhow::anyhow!(
                    "Binance depth snapshot request failed: {}",
                    error.without_url()
                )
            })?;
        let body = decode_response_body(response, "depth snapshot").await?;
        parse_depth_snapshot(&body)
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
        let signature = sign_hex(&self.credentials.secret_key, query)?;
        let response = self
            .http
            .get(format!(
                "{}{}?{}&signature={}",
                self.base_url, path, query, signature
            ))
            .header(API_KEY_HEADER, &self.credentials.api_key)
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

    pub(super) async fn signed_post<T>(
        &self,
        path: &str,
        query: &str,
        operation: &str,
    ) -> anyhow::Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let signature = sign_hex(&self.credentials.secret_key, query)?;
        let response = self
            .http
            .post(format!(
                "{}{}?{}&signature={}",
                self.base_url, path, query, signature
            ))
            .header(API_KEY_HEADER, &self.credentials.api_key)
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

    pub(super) async fn signed_put<T>(
        &self,
        path: &str,
        query: &str,
        operation: &str,
    ) -> anyhow::Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let signature = sign_hex(&self.credentials.secret_key, query)?;
        let response = self
            .http
            .put(format!(
                "{}{}?{}&signature={}",
                self.base_url, path, query, signature
            ))
            .header(API_KEY_HEADER, &self.credentials.api_key)
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
        let mut url = reqwest::Url::parse("http://localhost")
            .context("failed to initialize Binance query encoder")?;
        {
            let mut query = url.query_pairs_mut();
            for (name, value) in parameters {
                query.append_pair(name, value);
            }
            query.append_pair("timestamp", &timestamp.to_string());
        }
        url.query()
            .map(str::to_owned)
            .context("Binance signed query encoder returned an empty query")
    }
}

#[derive(Debug)]
pub struct BinanceAccountState {
    pub clock_offset_ms: i64,
    pub clock_sync: BinanceClockSync,
    pub account: AccountInformation,
    pub commission: CommissionRates,
    pub symbol_rules: SymbolRules,
    pub open_orders: Vec<OpenOrder>,
    pub order_rate_limits: Vec<OrderRateLimit>,
}

#[derive(Clone, Copy, Debug)]
pub struct BinanceClockSync {
    pub offset_ms: i64,
    pub round_trip_us: u64,
    pub observed_at: Instant,
    pub observed_unix_ms: u64,
}

impl BinanceClockSync {
    pub fn age_ms(self) -> u64 {
        self.observed_at
            .elapsed()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64
    }

    pub fn midpoint_uncertainty_us(self) -> u64 {
        self.round_trip_us.saturating_add(1) / 2
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct SymbolRules {
    pub symbol: String,
    pub status: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub price: DecimalFilter,
    pub lot_size: DecimalFilter,
    pub market_lot_size: DecimalFilter,
    pub min_notional: Decimal,
    pub max_num_orders: u32,
    pub max_num_algo_orders: u32,
}

impl SymbolRules {
    /// Applies the reviewed execution price increment when it is exactly
    /// aligned to Binance's current PRICE_FILTER. This permits a deliberately
    /// coarser strategy increment while rejecting prices the venue cannot
    /// represent.
    pub fn with_compatible_price_step(&self, configured_step: Decimal) -> anyhow::Result<Self> {
        ensure!(
            configured_step > Decimal::ZERO,
            "configured Binance price step is non-positive"
        );
        ensure!(
            self.price.step > Decimal::ZERO,
            "live Binance PRICE_FILTER step is non-positive"
        );
        ensure!(
            configured_step % self.price.step == Decimal::ZERO,
            "configured Binance price step is not aligned to live PRICE_FILTER"
        );
        let mut execution_rules = self.clone();
        execution_rules.price.step = configured_step;
        Ok(execution_rules)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DecimalFilter {
    pub min: Decimal,
    pub max: Decimal,
    pub step: Decimal,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeInformation {
    pub symbols: Vec<ExchangeSymbol>,
}

impl ExchangeInformation {
    pub fn symbol_rules(&self, expected_symbol: &str) -> anyhow::Result<SymbolRules> {
        let mut matches = self
            .symbols
            .iter()
            .filter(|symbol| symbol.symbol == expected_symbol);
        let symbol = matches
            .next()
            .with_context(|| format!("Binance exchangeInfo omitted {expected_symbol}"))?;
        ensure!(
            matches.next().is_none(),
            "Binance exchangeInfo duplicated {expected_symbol}"
        );
        symbol.compile_rules()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeSymbol {
    pub symbol: String,
    pub status: String,
    pub base_asset: String,
    pub quote_asset: String,
    #[serde(default)]
    pub order_types: Vec<String>,
    #[serde(default)]
    pub is_spot_trading_allowed: bool,
    pub filters: Vec<Value>,
}

impl ExchangeSymbol {
    fn compile_rules(&self) -> anyhow::Result<SymbolRules> {
        ensure!(self.status == "TRADING", "{} is not TRADING", self.symbol);
        ensure!(
            self.is_spot_trading_allowed,
            "{} does not allow Spot trading",
            self.symbol
        );
        for required in ["LIMIT", "MARKET"] {
            ensure!(
                self.order_types
                    .iter()
                    .any(|order_type| order_type == required),
                "{} does not allow {required} orders",
                self.symbol
            );
        }
        let price = decimal_filter(
            self.filter("PRICE_FILTER")?,
            "minPrice",
            "maxPrice",
            "tickSize",
        )?;
        let lot_size = decimal_filter(self.filter("LOT_SIZE")?, "minQty", "maxQty", "stepSize")?;
        let market_lot_size = decimal_filter(
            self.filter("MARKET_LOT_SIZE")?,
            "minQty",
            "maxQty",
            "stepSize",
        )?;
        let min_notional_filter = self
            .filters
            .iter()
            .find(|filter| filter.get("filterType").and_then(Value::as_str) == Some("NOTIONAL"))
            .or_else(|| {
                self.filters.iter().find(|filter| {
                    filter.get("filterType").and_then(Value::as_str) == Some("MIN_NOTIONAL")
                })
            })
            .context("Binance exchangeInfo omitted NOTIONAL/MIN_NOTIONAL")?;
        let min_notional = decimal_field(min_notional_filter, "minNotional")?;
        let max_num_orders = integer_filter(self.filter("MAX_NUM_ORDERS")?, "maxNumOrders")?;
        let max_num_algo_orders =
            integer_filter(self.filter("MAX_NUM_ALGO_ORDERS")?, "maxNumAlgoOrders")?;
        ensure!(
            price.step > Decimal::ZERO,
            "Binance tick size must be positive"
        );
        ensure!(
            lot_size.step > Decimal::ZERO,
            "Binance lot step must be positive"
        );
        ensure!(
            market_lot_size.step >= Decimal::ZERO,
            "Binance market lot step must not be negative"
        );
        ensure!(
            min_notional >= Decimal::ZERO,
            "Binance min notional is negative"
        );
        Ok(SymbolRules {
            symbol: self.symbol.clone(),
            status: self.status.clone(),
            base_asset: self.base_asset.clone(),
            quote_asset: self.quote_asset.clone(),
            price,
            lot_size,
            market_lot_size,
            min_notional,
            max_num_orders,
            max_num_algo_orders,
        })
    }

    fn filter(&self, filter_type: &str) -> anyhow::Result<&Value> {
        self.filters
            .iter()
            .find(|filter| filter.get("filterType").and_then(Value::as_str) == Some(filter_type))
            .with_context(|| format!("Binance exchangeInfo omitted {filter_type}"))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OpenOrder {
    pub symbol: String,
    pub order_id: u64,
    pub client_order_id: String,
    #[serde(deserialize_with = "deserialize_decimal")]
    pub price: Decimal,
    #[serde(deserialize_with = "deserialize_decimal")]
    pub orig_qty: Decimal,
    #[serde(deserialize_with = "deserialize_decimal")]
    pub executed_qty: Decimal,
    pub status: String,
    pub time_in_force: String,
    #[serde(rename = "type")]
    pub order_type: String,
    pub side: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OrderRateLimit {
    pub rate_limit_type: String,
    pub interval: String,
    pub interval_num: u32,
    pub limit: u32,
    pub count: u32,
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ApiKeyPermissions {
    pub ip_restrict: bool,
    pub enable_reading: bool,
    pub enable_withdrawals: bool,
    #[serde(default)]
    pub enable_internal_transfer: bool,
    #[serde(default)]
    pub permits_universal_transfer: bool,
    #[serde(default)]
    pub enable_spot_and_margin_trading: bool,
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

impl CommissionRates {
    pub fn conservative_taker_fee_bps(&self, side: &str) -> anyhow::Result<u16> {
        let side_fee = |rates: &CommissionSideRates| match side {
            "BUY" => Ok(rates.buyer),
            "SELL" => Ok(rates.seller),
            _ => bail!("Binance commission side must be BUY or SELL"),
        };
        let rate = self.standard_commission.taker
            + side_fee(&self.standard_commission)?
            + self.special_commission.taker
            + side_fee(&self.special_commission)?
            + self.tax_commission.taker
            + side_fee(&self.tax_commission)?;
        ensure!(rate >= Decimal::ZERO, "Binance commission rate is negative");
        let mantissa =
            u128::try_from(rate.mantissa()).context("Binance commission mantissa is negative")?;
        let numerator = mantissa
            .checked_mul(10_000)
            .context("Binance commission bps numerator overflow")?;
        let denominator = 10_u128
            .checked_pow(rate.scale())
            .context("Binance commission decimal scale overflow")?;
        let bps = numerator
            .checked_add(denominator.saturating_sub(1))
            .context("Binance commission rounding overflow")?
            / denominator;
        ensure!(bps <= 10_000, "Binance commission exceeds 100%");
        bps.try_into().context("Binance commission bps exceed u16")
    }
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
    let body = decode_response_body(response, operation).await?;
    serde_json::from_slice(&body)
        .with_context(|| format!("invalid Binance {operation} response JSON"))
}

async fn decode_response_body(
    response: reqwest::Response,
    operation: &str,
) -> anyhow::Result<Vec<u8>> {
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
    Ok(body.to_vec())
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

fn validate_symbol(symbol: &str) -> anyhow::Result<()> {
    ensure!(
        !symbol.is_empty()
            && symbol
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit()),
        "Binance symbol must contain only uppercase ASCII letters and digits"
    );
    Ok(())
}

fn decimal_filter(
    filter: &Value,
    min_field: &str,
    max_field: &str,
    step_field: &str,
) -> anyhow::Result<DecimalFilter> {
    Ok(DecimalFilter {
        min: decimal_field(filter, min_field)?,
        max: decimal_field(filter, max_field)?,
        step: decimal_field(filter, step_field)?,
    })
}

fn decimal_field(value: &Value, field: &str) -> anyhow::Result<Decimal> {
    let raw = value
        .get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("Binance exchangeInfo filter omitted {field}"))?;
    Decimal::from_str(raw).with_context(|| format!("invalid Binance {field}"))
}

fn integer_filter(value: &Value, field: &str) -> anyhow::Result<u32> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .with_context(|| format!("Binance exchangeInfo filter omitted {field}"))?
        .try_into()
        .with_context(|| format!("Binance {field} exceeds u32"))
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
    use std::time::Instant;

    use rust_decimal::Decimal;

    use super::{
        ApiKeyPermissions, BinanceAccountClient, BinanceClockSync, BinanceCredentials,
        CommissionRates, ExchangeInformation, apply_clock_offset, sign_hex,
    };

    #[test]
    fn clock_sync_reports_conservative_midpoint_uncertainty() {
        let sync = BinanceClockSync {
            offset_ms: 2,
            round_trip_us: 5,
            observed_at: Instant::now(),
            observed_unix_ms: 1_700_000_000_000,
        };

        assert_eq!(sync.midpoint_uncertainty_us(), 3);
        assert!(sync.age_ms() <= 1);
    }

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
        assert_eq!(commission.conservative_taker_fee_bps("BUY").unwrap(), 20);
        assert_eq!(commission.conservative_taker_fee_bps("SELL").unwrap(), 20);
    }

    #[test]
    fn compiles_required_spot_filters_without_floating_point() {
        let information: ExchangeInformation = serde_json::from_str(
            r#"{
              "symbols":[{
                "symbol":"WLDUSDC",
                "status":"TRADING",
                "baseAsset":"WLD",
                "quoteAsset":"USDC",
                "orderTypes":["LIMIT","MARKET"],
                "isSpotTradingAllowed":true,
                "filters":[
                  {"filterType":"PRICE_FILTER","minPrice":"0.00010000","maxPrice":"1000.00000000","tickSize":"0.00010000"},
                  {"filterType":"LOT_SIZE","minQty":"0.10000000","maxQty":"100000.00000000","stepSize":"0.10000000"},
                  {"filterType":"MARKET_LOT_SIZE","minQty":"0.00000000","maxQty":"120000.00000000","stepSize":"0.00000000"},
                  {"filterType":"NOTIONAL","minNotional":"5.00000000","applyMinToMarket":true,"maxNotional":"9000000.00000000","applyMaxToMarket":false},
                  {"filterType":"MAX_NUM_ORDERS","maxNumOrders":200},
                  {"filterType":"MAX_NUM_ALGO_ORDERS","maxNumAlgoOrders":5}
                ]
              }]
            }"#,
        )
        .unwrap();

        let rules = information.symbol_rules("WLDUSDC").unwrap();
        assert_eq!(rules.price.step, Decimal::new(1, 4));
        assert_eq!(rules.lot_size.step, Decimal::new(1, 1));
        assert_eq!(rules.market_lot_size.step, Decimal::ZERO);
        assert_eq!(rules.min_notional, Decimal::new(5, 0));
        assert_eq!(rules.max_num_orders, 200);
        assert_eq!(rules.max_num_algo_orders, 5);

        let rails_compatible = rules
            .with_compatible_price_step(Decimal::new(1, 3))
            .unwrap();
        assert_eq!(rails_compatible.price.step, Decimal::new(1, 3));
        assert_eq!(rules.price.step, Decimal::new(1, 4));
        assert!(
            rules
                .with_compatible_price_step(Decimal::new(15, 5))
                .is_err()
        );
        assert!(
            rules
                .with_compatible_price_step(Decimal::new(1, 5))
                .is_err()
        );
    }

    #[test]
    fn parses_withdrawal_and_ip_restriction_permissions() {
        let permissions: ApiKeyPermissions = serde_json::from_str(
            r#"{"ipRestrict":true,"enableReading":true,"enableWithdrawals":false,"enableInternalTransfer":true,"permitsUniversalTransfer":true,"enableSpotAndMarginTrading":true}"#,
        )
        .unwrap();

        assert!(permissions.ip_restrict);
        assert!(permissions.enable_reading);
        assert!(!permissions.enable_withdrawals);
        assert!(permissions.enable_internal_transfer);
        assert!(permissions.permits_universal_transfer);
        assert!(permissions.enable_spot_and_margin_trading);
    }

    #[test]
    fn applies_positive_and_negative_clock_offsets() {
        assert_eq!(apply_clock_offset(1_000, 25).unwrap(), 1_025);
        assert_eq!(apply_clock_offset(1_000, -25).unwrap(), 975);
    }

    #[test]
    fn percent_encodes_values_before_signing() {
        let client = BinanceAccountClient::new(
            "https://api.binance.com",
            BinanceCredentials::new("api", "secret"),
        )
        .unwrap();
        let query = client
            .signed_query(&[(
                "questionnaire",
                r#"{"isAddressOwner":1,"sendTo":1}"#.to_owned(),
            )])
            .unwrap();

        assert!(query.starts_with(
            "questionnaire=%7B%22isAddressOwner%22%3A1%2C%22sendTo%22%3A1%7D&timestamp="
        ));
    }
}

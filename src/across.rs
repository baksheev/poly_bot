use std::{str::FromStr, time::Duration};

use alloy_primitives::Address;
use anyhow::{Context, ensure};
use reqwest::{Client, StatusCode};
use serde::Deserialize;

use crate::config::AppConfig;

pub const OPTIMISM_CHAIN_ID: u64 = 10;
pub const WORLD_CHAIN_CHAIN_ID: u64 = 480;
pub const OPTIMISM_USDC: Address =
    alloy_primitives::address!("0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85");
pub const WORLD_CHAIN_USDC: Address =
    alloy_primitives::address!("0x79A02482A880bCE3F13e09Da970dC34db4CD24d1");

const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_RESPONSE_BYTES: usize = 1_048_576;

pub struct AcrossClient {
    http: Client,
    base_url: String,
}

impl AcrossClient {
    pub fn new(config: &AppConfig) -> anyhow::Result<Self> {
        let http = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("failed to build Across HTTP client")?;
        Ok(Self {
            http,
            base_url: config.across_api_base_url.trim_end_matches('/').to_owned(),
        })
    }

    pub async fn quote(&self, request: &AcrossQuoteRequest) -> anyhow::Result<AcrossQuote> {
        let response = self
            .http
            .get(format!("{}/swap/approval", self.base_url))
            .query(&[
                ("tradeType", "exactInput".to_owned()),
                ("amount", request.amount.to_string()),
                ("inputToken", format!("{:#x}", request.input_token)),
                ("outputToken", format!("{:#x}", request.output_token)),
                ("originChainId", request.origin_chain_id.to_string()),
                (
                    "destinationChainId",
                    request.destination_chain_id.to_string(),
                ),
                ("depositor", format!("{:#x}", request.depositor)),
                ("recipient", format!("{:#x}", request.recipient)),
                ("slippage", "auto".to_owned()),
            ])
            .send()
            .await
            .map_err(|error| {
                anyhow::anyhow!("Across quote request failed: {}", error.without_url())
            })?;
        let status = response.status();
        let content_length = response.content_length().unwrap_or(0);
        ensure!(
            content_length <= MAX_RESPONSE_BYTES as u64,
            "Across quote response exceeds the size limit"
        );
        let body = response
            .bytes()
            .await
            .context("failed to read Across quote response")?;
        ensure!(
            body.len() <= MAX_RESPONSE_BYTES,
            "Across quote response exceeds the size limit"
        );
        ensure!(
            status == StatusCode::OK,
            "Across quote failed closed with HTTP {status}"
        );
        serde_json::from_slice(&body).context("invalid Across quote response JSON")
    }
}

#[derive(Clone, Debug)]
pub struct AcrossQuoteRequest {
    pub origin_chain_id: u64,
    pub destination_chain_id: u64,
    pub input_token: Address,
    pub output_token: Address,
    pub amount: u128,
    pub depositor: Address,
    pub recipient: Address,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AcrossQuote {
    pub amount_type: String,
    pub checks: AcrossChecks,
    #[serde(default)]
    pub approval_txns: Vec<AcrossTransaction>,
    pub input_token: AcrossToken,
    pub output_token: AcrossToken,
    pub fees: AcrossFees,
    pub input_amount: String,
    pub max_input_amount: String,
    pub expected_output_amount: String,
    pub min_output_amount: String,
    pub expected_fill_time: u64,
    pub swap_tx: AcrossTransaction,
    pub quote_expiry_timestamp: u64,
    pub id: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AcrossChecks {
    pub allowance: AcrossAllowanceCheck,
    pub balance: AcrossBalanceCheck,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AcrossAllowanceCheck {
    pub token: String,
    pub spender: String,
    pub actual: String,
    pub expected: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AcrossBalanceCheck {
    pub token: String,
    pub actual: String,
    pub expected: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AcrossToken {
    pub decimals: u8,
    pub symbol: String,
    pub address: String,
    pub chain_id: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AcrossFees {
    pub total: AcrossFee,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AcrossFee {
    pub amount: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AcrossTransaction {
    pub chain_id: u64,
    pub to: String,
    pub data: String,
    #[serde(default)]
    pub value: serde_json::Value,
    #[serde(default)]
    pub gas: serde_json::Value,
}

pub fn validate_quote(request: &AcrossQuoteRequest, quote: &AcrossQuote) -> anyhow::Result<()> {
    ensure!(
        quote.amount_type == "exactInput",
        "Across changed the quote amount type"
    );
    ensure!(
        !quote.id.is_empty() && quote.id.len() <= 256,
        "Across quote id is invalid"
    );
    ensure_token(
        &quote.input_token,
        request.origin_chain_id,
        request.input_token,
    )?;
    ensure_token(
        &quote.output_token,
        request.destination_chain_id,
        request.output_token,
    )?;
    ensure!(
        quote.input_token.decimals == quote.output_token.decimals,
        "Across token decimals differ"
    );
    ensure!(
        quote.input_token.symbol == "USDC" && quote.output_token.symbol == "USDC",
        "Across returned a non-USDC token"
    );

    let input_amount = parse_amount("inputAmount", &quote.input_amount)?;
    let max_input = parse_amount("maxInputAmount", &quote.max_input_amount)?;
    let expected_output = parse_amount("expectedOutputAmount", &quote.expected_output_amount)?;
    let min_output = parse_amount("minOutputAmount", &quote.min_output_amount)?;
    let fee = parse_amount("fees.total.amount", &quote.fees.total.amount)?;
    ensure!(
        input_amount == request.amount && max_input == request.amount,
        "Across changed the exact input amount"
    );
    ensure!(
        expected_output > 0 && expected_output <= request.amount,
        "Across expected output is invalid"
    );
    ensure!(
        min_output > 0 && min_output <= expected_output,
        "Across minimum output is invalid"
    );
    ensure!(
        fee < request.amount,
        "Across fee consumes the full input amount"
    );
    ensure!(
        quote.expected_fill_time <= 600,
        "Across expected fill time exceeds the safety bound"
    );
    let now = unix_timestamp_seconds()?;
    ensure!(
        quote.quote_expiry_timestamp > now,
        "Across quote is already expired"
    );
    ensure!(
        quote.quote_expiry_timestamp <= now + 7_200,
        "Across quote expiry is outside the safety bound"
    );

    ensure_check_token(&quote.checks.allowance.token, request.input_token)?;
    ensure_check_token(&quote.checks.balance.token, request.input_token)?;
    ensure!(
        parse_amount("allowance.expected", &quote.checks.allowance.expected)? == request.amount,
        "Across allowance expectation changed"
    );
    ensure!(
        parse_amount("balance.expected", &quote.checks.balance.expected)? == request.amount,
        "Across balance expectation changed"
    );
    let spender = parse_address("allowance.spender", &quote.checks.allowance.spender)?;

    ensure!(
        quote.swap_tx.chain_id == request.origin_chain_id,
        "Across swap chain mismatch"
    );
    ensure!(
        parse_address("swapTx.to", &quote.swap_tx.to)? == spender,
        "Across swap target differs from allowance spender"
    );
    validate_swap_calldata(request, &quote.swap_tx.data, min_output)?;
    validate_approvals(request, quote, spender)?;
    Ok(())
}

fn ensure_token(token: &AcrossToken, chain_id: u64, address: Address) -> anyhow::Result<()> {
    ensure!(token.chain_id == chain_id, "Across token chain mismatch");
    ensure!(
        parse_address("token.address", &token.address)? == address,
        "Across token address mismatch"
    );
    ensure!(token.decimals == 6, "Across USDC token decimals changed");
    Ok(())
}

fn ensure_check_token(value: &str, expected: Address) -> anyhow::Result<()> {
    ensure!(
        parse_address("check.token", value)? == expected,
        "Across check token mismatch"
    );
    Ok(())
}

fn validate_approvals(
    request: &AcrossQuoteRequest,
    quote: &AcrossQuote,
    spender: Address,
) -> anyhow::Result<()> {
    let actual = parse_amount("allowance.actual", &quote.checks.allowance.actual)?;
    if quote.approval_txns.is_empty() {
        ensure!(
            actual >= request.amount,
            "Across omitted approval for insufficient allowance"
        );
        return Ok(());
    }
    ensure!(
        quote.approval_txns.len() == 1,
        "Across returned multiple approval transactions"
    );
    let approval = &quote.approval_txns[0];
    ensure!(
        approval.chain_id == request.origin_chain_id,
        "Across approval chain mismatch"
    );
    ensure!(
        parse_address("approval.to", &approval.to)? == request.input_token,
        "Across approval token mismatch"
    );
    let bytes = decode_calldata("approval.data", &approval.data)?;
    ensure!(
        bytes.len() == 68 && bytes[..4] == [0x09, 0x5e, 0xa7, 0xb3],
        "Across approval calldata is not ERC20 approve"
    );
    ensure!(
        address_word(&bytes, 0)? == spender,
        "Across approval spender mismatch"
    );
    ensure!(
        u256_word_is_at_least_u128(&bytes, 1, request.amount)?,
        "Across approval amount is too small"
    );
    Ok(())
}

fn validate_swap_calldata(
    request: &AcrossQuoteRequest,
    data: &str,
    min_output: u128,
) -> anyhow::Result<()> {
    let bytes = decode_calldata("swapTx.data", data)?;
    ensure!(
        bytes.len() >= 4 + 6 * 32 && bytes.len() <= 16_384,
        "Across swap calldata length is invalid"
    );
    ensure!(
        address_word(&bytes, 0)? == request.depositor,
        "Across calldata depositor mismatch"
    );
    ensure!(
        address_word(&bytes, 1)? == request.recipient,
        "Across calldata recipient mismatch"
    );
    ensure!(
        address_word(&bytes, 2)? == request.input_token,
        "Across calldata input token mismatch"
    );
    ensure!(
        address_word(&bytes, 3)? == request.output_token,
        "Across calldata output token mismatch"
    );
    ensure!(
        u256_word_fits_u128(&bytes, 4)? == request.amount,
        "Across calldata input amount mismatch"
    );
    ensure!(
        u256_word_fits_u128(&bytes, 5)? == min_output,
        "Across calldata minimum output mismatch"
    );
    Ok(())
}

fn decode_calldata(name: &str, value: &str) -> anyhow::Result<Vec<u8>> {
    let hex = value
        .strip_prefix("0x")
        .with_context(|| format!("{name} is not hex data"))?;
    ensure!(hex.len() % 2 == 0, "{name} has odd hex length");
    (0..hex.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&hex[index..index + 2], 16)
                .with_context(|| format!("{name} contains invalid hex"))
        })
        .collect()
}

fn address_word(bytes: &[u8], index: usize) -> anyhow::Result<Address> {
    let word = word(bytes, index)?;
    ensure!(
        word[..12].iter().all(|byte| *byte == 0),
        "Across calldata address is not canonical"
    );
    Ok(Address::from_slice(&word[12..]))
}

fn u256_word_fits_u128(bytes: &[u8], index: usize) -> anyhow::Result<u128> {
    let word = word(bytes, index)?;
    ensure!(
        word[..16].iter().all(|byte| *byte == 0),
        "Across calldata integer exceeds u128"
    );
    Ok(u128::from_be_bytes(
        word[16..].try_into().expect("word tail is 16 bytes"),
    ))
}

fn u256_word_is_at_least_u128(bytes: &[u8], index: usize, minimum: u128) -> anyhow::Result<bool> {
    let word = word(bytes, index)?;
    if word[..16].iter().any(|byte| *byte != 0) {
        return Ok(true);
    }
    Ok(u128::from_be_bytes(word[16..].try_into().expect("word tail is 16 bytes")) >= minimum)
}

fn word(bytes: &[u8], index: usize) -> anyhow::Result<&[u8]> {
    let start = 4 + index * 32;
    bytes
        .get(start..start + 32)
        .context("Across calldata is truncated")
}

fn parse_address(name: &str, value: &str) -> anyhow::Result<Address> {
    Address::from_str(value).with_context(|| format!("Across {name} is not an EVM address"))
}

fn parse_amount(name: &str, value: &str) -> anyhow::Result<u128> {
    value
        .parse()
        .with_context(|| format!("Across {name} is not an unsigned integer"))
}

fn unix_timestamp_seconds() -> anyhow::Result<u64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_secs())
}

#[cfg(test)]
mod tests {
    use super::{decode_calldata, u256_word_is_at_least_u128};

    #[test]
    fn max_uint256_approval_covers_u128_amount() {
        let mut calldata = vec![0_u8; 4 + 2 * 32];
        calldata[4 + 32..].fill(0xff);

        assert!(u256_word_is_at_least_u128(&calldata, 1, 100_000_000).unwrap());
    }

    #[test]
    fn rejects_non_hex_calldata() {
        assert!(decode_calldata("swapTx.data", "not-hex").is_err());
        assert!(decode_calldata("swapTx.data", "0x0").is_err());
    }
}

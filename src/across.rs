use std::{str::FromStr, time::Duration};

use alloy_primitives::Address;
use anyhow::{Context, ensure};
use reqwest::{Client, Response, StatusCode};
use serde::{Deserialize, de::DeserializeOwned};

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
        decode_response(response, "quote").await
    }

    pub async fn deposit_status(
        &self,
        deposit_txn_ref: &str,
    ) -> anyhow::Result<AcrossDepositStatus> {
        validate_transaction_hash(deposit_txn_ref)?;
        let response = self
            .http
            .get(format!("{}/deposit/status", self.base_url))
            .query(&[("depositTxnRef", deposit_txn_ref)])
            .send()
            .await
            .map_err(|error| {
                anyhow::anyhow!(
                    "Across deposit status request failed: {}",
                    error.without_url()
                )
            })?;
        decode_response(response, "deposit status").await
    }
}

async fn decode_response<T: DeserializeOwned>(
    response: Response,
    operation: &str,
) -> anyhow::Result<T> {
    let status = response.status();
    let content_length = response.content_length().unwrap_or(0);
    ensure!(
        content_length <= MAX_RESPONSE_BYTES as u64,
        "Across {operation} response exceeds the size limit"
    );
    let body = response
        .bytes()
        .await
        .with_context(|| format!("failed to read Across {operation} response"))?;
    ensure!(
        body.len() <= MAX_RESPONSE_BYTES,
        "Across {operation} response exceeds the size limit"
    );
    ensure!(
        status == StatusCode::OK,
        "Across {operation} failed closed with HTTP {status}"
    );
    serde_json::from_slice(&body)
        .with_context(|| format!("invalid Across {operation} response JSON"))
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

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AcrossDepositStatus {
    pub status: String,
    pub fill_txn_ref: Option<String>,
    pub destination_chain_id: u64,
    pub output_token: String,
    pub output_amount: Option<String>,
    pub fill_time: Option<u64>,
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
    ensure!(
        transaction_integer("swapTx.value", &quote.swap_tx.value)? == 0,
        "Across ERC20 swap transaction has non-zero native value"
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
    ensure!(
        transaction_integer("approval.value", &approval.value)? == 0,
        "Across approval transaction has non-zero native value"
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
        bytes[..4] == [0xad, 0x54, 0x25, 0xc6],
        "Across swap calldata selector changed"
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

fn transaction_integer(name: &str, value: &serde_json::Value) -> anyhow::Result<u128> {
    match value {
        serde_json::Value::Null => Ok(0),
        serde_json::Value::Number(value) => value
            .as_u64()
            .map(u128::from)
            .with_context(|| format!("Across {name} is not an unsigned integer")),
        serde_json::Value::String(value) => {
            if let Some(hex) = value.strip_prefix("0x") {
                u128::from_str_radix(hex, 16)
                    .with_context(|| format!("Across {name} is not a hex integer"))
            } else {
                parse_amount(name, value)
            }
        }
        _ => anyhow::bail!("Across {name} is not an unsigned integer"),
    }
}

pub fn validate_deposit_status(
    status: &AcrossDepositStatus,
    destination_chain_id: u64,
    output_token: Address,
    minimum_output_amount: u128,
) -> anyhow::Result<bool> {
    ensure!(
        status.destination_chain_id == destination_chain_id,
        "Across status destination chain mismatch"
    );
    ensure!(
        parse_address("status.outputToken", &status.output_token)? == output_token,
        "Across status output token mismatch"
    );
    match status.status.as_str() {
        "pending" => {
            ensure!(
                status.fill_txn_ref.is_none(),
                "pending Across status has a fill transaction"
            );
            ensure!(
                status.output_amount.is_none(),
                "pending Across status has an output amount"
            );
            ensure!(
                status.fill_time.is_none(),
                "pending Across status has a fill time"
            );
            Ok(false)
        }
        "filled" => {
            validate_transaction_hash(
                status
                    .fill_txn_ref
                    .as_deref()
                    .context("filled Across status has no fill transaction")?,
            )?;
            let output_amount = parse_amount(
                "status.outputAmount",
                status
                    .output_amount
                    .as_deref()
                    .context("filled Across status has no output amount")?,
            )?;
            ensure!(
                output_amount >= minimum_output_amount,
                "Across fill output is below the reserved minimum"
            );
            ensure!(
                status.fill_time.is_some(),
                "filled Across status has no fill time"
            );
            Ok(true)
        }
        _ => anyhow::bail!("unsupported Across deposit status {}", status.status),
    }
}

fn validate_transaction_hash(value: &str) -> anyhow::Result<()> {
    ensure!(
        value.len() == 66
            && value.starts_with("0x")
            && value[2..].bytes().all(|byte| byte.is_ascii_hexdigit()),
        "Across transaction hash is invalid"
    );
    Ok(())
}

fn unix_timestamp_seconds() -> anyhow::Result<u64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_secs())
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, address};
    use serde_json::json;

    use super::{
        AcrossAllowanceCheck, AcrossBalanceCheck, AcrossChecks, AcrossDepositStatus, AcrossFee,
        AcrossFees, AcrossQuote, AcrossQuoteRequest, AcrossToken, AcrossTransaction,
        OPTIMISM_CHAIN_ID, OPTIMISM_USDC, WORLD_CHAIN_CHAIN_ID, WORLD_CHAIN_USDC, decode_calldata,
        transaction_integer, u256_word_is_at_least_u128, validate_deposit_status, validate_quote,
    };

    const DEPOSITOR: Address = address!("0x1111111111111111111111111111111111111111");
    const SPENDER: Address = address!("0x2222222222222222222222222222222222222222");

    fn request() -> AcrossQuoteRequest {
        AcrossQuoteRequest {
            origin_chain_id: OPTIMISM_CHAIN_ID,
            destination_chain_id: WORLD_CHAIN_CHAIN_ID,
            input_token: OPTIMISM_USDC,
            output_token: WORLD_CHAIN_USDC,
            amount: 1_000_000,
            depositor: DEPOSITOR,
            recipient: DEPOSITOR,
        }
    }

    fn valid_quote() -> AcrossQuote {
        let request = request();
        AcrossQuote {
            amount_type: "exactInput".to_owned(),
            checks: AcrossChecks {
                allowance: AcrossAllowanceCheck {
                    token: format!("{:#x}", request.input_token),
                    spender: format!("{SPENDER:#x}"),
                    actual: "0".to_owned(),
                    expected: request.amount.to_string(),
                },
                balance: AcrossBalanceCheck {
                    token: format!("{:#x}", request.input_token),
                    actual: request.amount.to_string(),
                    expected: request.amount.to_string(),
                },
            },
            approval_txns: vec![AcrossTransaction {
                chain_id: request.origin_chain_id,
                to: format!("{:#x}", request.input_token),
                data: approval_calldata(SPENDER),
                value: json!("0"),
                gas: json!("0"),
            }],
            input_token: token(request.origin_chain_id, request.input_token),
            output_token: token(request.destination_chain_id, request.output_token),
            fees: AcrossFees {
                total: AcrossFee {
                    amount: "500".to_owned(),
                },
            },
            input_amount: request.amount.to_string(),
            max_input_amount: request.amount.to_string(),
            expected_output_amount: "999500".to_owned(),
            min_output_amount: "999400".to_owned(),
            expected_fill_time: 2,
            swap_tx: AcrossTransaction {
                chain_id: request.origin_chain_id,
                to: format!("{SPENDER:#x}"),
                data: swap_calldata(&request, 999_400),
                value: json!("0x0"),
                gas: json!(0),
            },
            quote_expiry_timestamp: super::unix_timestamp_seconds().unwrap() + 60,
            id: "test-quote".to_owned(),
        }
    }

    fn token(chain_id: u64, address: Address) -> AcrossToken {
        AcrossToken {
            decimals: 6,
            symbol: "USDC".to_owned(),
            address: format!("{address:#x}"),
            chain_id,
        }
    }

    fn approval_calldata(spender: Address) -> String {
        let mut bytes = vec![0x09, 0x5e, 0xa7, 0xb3];
        push_address_word(&mut bytes, spender);
        bytes.extend([0xff; 32]);
        encode_hex(&bytes)
    }

    fn swap_calldata(request: &AcrossQuoteRequest, minimum_output: u128) -> String {
        let mut bytes = vec![0xad, 0x54, 0x25, 0xc6];
        push_address_word(&mut bytes, request.depositor);
        push_address_word(&mut bytes, request.recipient);
        push_address_word(&mut bytes, request.input_token);
        push_address_word(&mut bytes, request.output_token);
        push_u128_word(&mut bytes, request.amount);
        push_u128_word(&mut bytes, minimum_output);
        encode_hex(&bytes)
    }

    fn push_address_word(bytes: &mut Vec<u8>, address: Address) {
        bytes.extend([0_u8; 12]);
        bytes.extend(address.as_slice());
    }

    fn push_u128_word(bytes: &mut Vec<u8>, value: u128) {
        bytes.extend([0_u8; 16]);
        bytes.extend(value.to_be_bytes());
    }

    fn encode_hex(bytes: &[u8]) -> String {
        use std::fmt::Write as _;

        let mut encoded = String::from("0x");
        for byte in bytes {
            write!(&mut encoded, "{byte:02x}").unwrap();
        }
        encoded
    }

    #[test]
    fn validates_rails_compatible_quote_and_approval() {
        validate_quote(&request(), &valid_quote()).unwrap();
    }

    #[test]
    fn permits_missing_approval_only_when_allowance_is_sufficient() {
        let mut quote = valid_quote();
        quote.approval_txns.clear();
        assert!(validate_quote(&request(), &quote).is_err());

        quote.checks.allowance.actual = request().amount.to_string();
        validate_quote(&request(), &quote).unwrap();
    }

    #[test]
    fn rejects_quote_when_reserved_recipient_or_amount_changes() {
        let mut changed_recipient = valid_quote();
        let mut other_request = request();
        other_request.recipient = Address::repeat_byte(0x33);
        changed_recipient.swap_tx.data = swap_calldata(&other_request, 999_400);
        assert!(validate_quote(&request(), &changed_recipient).is_err());

        let mut changed_amount = valid_quote();
        changed_amount.input_amount = "999999".to_owned();
        assert!(validate_quote(&request(), &changed_amount).is_err());
    }

    #[test]
    fn rejects_changed_swap_selector_and_nonzero_native_value() {
        let mut selector = valid_quote();
        selector.swap_tx.data.replace_range(2..10, "00000000");
        assert!(validate_quote(&request(), &selector).is_err());

        let mut value = valid_quote();
        value.swap_tx.value = json!("0x1");
        assert!(validate_quote(&request(), &value).is_err());
    }

    #[test]
    fn rejects_approval_for_another_spender() {
        let mut quote = valid_quote();
        quote.approval_txns[0].data = approval_calldata(Address::repeat_byte(0x44));
        assert!(validate_quote(&request(), &quote).is_err());
    }

    #[test]
    fn parses_decimal_hex_and_numeric_transaction_values() {
        assert_eq!(transaction_integer("value", &json!("42")).unwrap(), 42);
        assert_eq!(transaction_integer("value", &json!("0x2a")).unwrap(), 42);
        assert_eq!(transaction_integer("value", &json!(42)).unwrap(), 42);
        assert!(transaction_integer("value", &json!(-1)).is_err());
    }

    #[test]
    fn validates_pending_and_filled_deposit_status() {
        let pending = AcrossDepositStatus {
            status: "pending".to_owned(),
            fill_txn_ref: None,
            destination_chain_id: WORLD_CHAIN_CHAIN_ID,
            output_token: format!("{WORLD_CHAIN_USDC:#x}"),
            output_amount: None,
            fill_time: None,
        };
        assert!(
            !validate_deposit_status(&pending, WORLD_CHAIN_CHAIN_ID, WORLD_CHAIN_USDC, 999_400)
                .unwrap()
        );

        let filled = AcrossDepositStatus {
            status: "filled".to_owned(),
            fill_txn_ref: Some(format!("0x{}", "ab".repeat(32))),
            destination_chain_id: WORLD_CHAIN_CHAIN_ID,
            output_token: format!("{WORLD_CHAIN_USDC:#x}"),
            output_amount: Some("999500".to_owned()),
            fill_time: Some(1_784_192_400),
        };
        assert!(
            validate_deposit_status(&filled, WORLD_CHAIN_CHAIN_ID, WORLD_CHAIN_USDC, 999_400)
                .unwrap()
        );
    }

    #[test]
    fn rejects_filled_status_below_reserved_minimum() {
        let status = AcrossDepositStatus {
            status: "filled".to_owned(),
            fill_txn_ref: Some(format!("0x{}", "ab".repeat(32))),
            destination_chain_id: WORLD_CHAIN_CHAIN_ID,
            output_token: format!("{WORLD_CHAIN_USDC:#x}"),
            output_amount: Some("999399".to_owned()),
            fill_time: Some(1_784_192_400),
        };
        assert!(
            validate_deposit_status(&status, WORLD_CHAIN_CHAIN_ID, WORLD_CHAIN_USDC, 999_400)
                .is_err()
        );
    }

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

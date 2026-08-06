use alloy_primitives::{Address, U256, keccak256};
use anyhow::{Context, ensure};

use super::pool_id::V4PoolKey;

const WORD_BYTES: usize = 32;
const V4_SWAP_COMMAND: u8 = 0x10;
const V4_ACTION_SWAP_EXACT_IN_SINGLE: u8 = 0x06;
const V4_ACTION_SETTLE_ALL: u8 = 0x0c;
const V4_ACTION_TAKE_ALL: u8 = 0x0f;

pub fn v3_exact_input(
    token_in: Address,
    token_out: Address,
    fee: u32,
    recipient: Address,
    amount_in: U256,
    amount_out_minimum: U256,
) -> anyhow::Result<Vec<u8>> {
    validate_currency_pair(token_in, token_out)?;
    ensure!(recipient != Address::ZERO, "Uniswap V3 recipient is zero");
    ensure!(!amount_in.is_zero(), "Uniswap V3 input amount is zero");
    ensure!(
        fee > 0 && fee <= 0x00ff_ffff,
        "Uniswap V3 fee does not fit uint24"
    );

    let mut path = Vec::with_capacity(43);
    path.extend_from_slice(token_in.as_slice());
    path.extend_from_slice(&fee.to_be_bytes()[1..]);
    path.extend_from_slice(token_out.as_slice());

    // exactInput((bytes,address,uint256,uint256)). The tuple is dynamic because
    // its first member is bytes, so the top-level argument is an offset.
    let mut encoded = selector("exactInput((bytes,address,uint256,uint256))").to_vec();
    push_usize_word(&mut encoded, WORD_BYTES);
    push_usize_word(&mut encoded, 4 * WORD_BYTES);
    push_address_word(&mut encoded, recipient);
    push_u256_word(&mut encoded, amount_in);
    push_u256_word(&mut encoded, amount_out_minimum);
    encoded.extend_from_slice(&encode_bytes(&path));
    Ok(encoded)
}

pub fn pancake_v3_exact_input_single(
    token_in: Address,
    token_out: Address,
    fee: u32,
    recipient: Address,
    deadline: u64,
    amount_in: U256,
    amount_out_minimum: U256,
) -> anyhow::Result<Vec<u8>> {
    validate_currency_pair(token_in, token_out)?;
    ensure!(recipient != Address::ZERO, "Pancake V3 recipient is zero");
    ensure!(deadline > 0, "Pancake V3 deadline is zero");
    ensure!(!amount_in.is_zero(), "Pancake V3 input amount is zero");
    ensure!(
        !amount_out_minimum.is_zero(),
        "Pancake V3 minimum output amount is zero"
    );
    ensure!(
        fee > 0 && fee <= 0x00ff_ffff,
        "Pancake V3 fee does not fit uint24"
    );

    // Pancake V3 SwapRouter.exactInputSingle((address,address,uint24,address,
    // uint256,uint256,uint256,uint160)). This is the reviewed V3-only router,
    // not the Smart Router or Universal Router surface.
    let mut encoded = selector(
        "exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))",
    )
    .to_vec();
    push_address_word(&mut encoded, token_in);
    push_address_word(&mut encoded, token_out);
    push_u256_word(&mut encoded, U256::from(fee));
    push_address_word(&mut encoded, recipient);
    push_u256_word(&mut encoded, U256::from(deadline));
    push_u256_word(&mut encoded, amount_in);
    push_u256_word(&mut encoded, amount_out_minimum);
    push_u256_word(&mut encoded, U256::ZERO);
    Ok(encoded)
}

pub fn camelot_v3_exact_input_single(
    token_in: Address,
    token_out: Address,
    recipient: Address,
    deadline: u64,
    amount_in: U256,
    amount_out_minimum: U256,
) -> anyhow::Result<Vec<u8>> {
    validate_currency_pair(token_in, token_out)?;
    ensure!(recipient != Address::ZERO, "Camelot V3 recipient is zero");
    ensure!(deadline > 0, "Camelot V3 deadline is zero");
    ensure!(!amount_in.is_zero(), "Camelot V3 input amount is zero");
    ensure!(
        !amount_out_minimum.is_zero(),
        "Camelot V3 minimum output amount is zero"
    );

    // Algebra V1.9 SwapRouter.exactInputSingle((address,address,address,
    // uint256,uint256,uint256,uint160)). The selected pool is resolved only
    // from the two tokens; there is deliberately no Uniswap-style fee word.
    let mut encoded =
        selector("exactInputSingle((address,address,address,uint256,uint256,uint256,uint160))")
            .to_vec();
    ensure!(
        encoded.as_slice() == [0xbc, 0x65, 0x11, 0x88],
        "Camelot V3 exactInputSingle selector differs from the reviewed router"
    );
    push_address_word(&mut encoded, token_in);
    push_address_word(&mut encoded, token_out);
    push_address_word(&mut encoded, recipient);
    push_u256_word(&mut encoded, U256::from(deadline));
    push_u256_word(&mut encoded, amount_in);
    push_u256_word(&mut encoded, amount_out_minimum);
    push_u256_word(&mut encoded, U256::ZERO);
    Ok(encoded)
}

pub fn v3_quote_exact_input_single(
    token_in: Address,
    token_out: Address,
    amount_in: U256,
    fee: u32,
) -> anyhow::Result<Vec<u8>> {
    validate_currency_pair(token_in, token_out)?;
    ensure!(!amount_in.is_zero(), "Uniswap V3 quote input is zero");
    ensure!(
        fee > 0 && fee <= 0x00ff_ffff,
        "Uniswap V3 quote fee does not fit uint24"
    );

    // QuoterV2.quoteExactInputSingle((address,address,uint256,uint24,uint160)).
    let mut encoded =
        selector("quoteExactInputSingle((address,address,uint256,uint24,uint160))").to_vec();
    push_address_word(&mut encoded, token_in);
    push_address_word(&mut encoded, token_out);
    push_u256_word(&mut encoded, amount_in);
    push_u256_word(&mut encoded, U256::from(fee));
    push_u256_word(&mut encoded, U256::ZERO);
    Ok(encoded)
}

pub fn decode_v3_quote_exact_input_single(encoded: &[u8]) -> anyhow::Result<U256> {
    ensure!(
        encoded.len() >= 4 * WORD_BYTES,
        "Uniswap V3 QuoterV2 result is truncated"
    );
    Ok(U256::from_be_slice(&encoded[..WORD_BYTES]))
}

pub fn v3_quote_exact_output_single(
    token_in: Address,
    token_out: Address,
    amount_out: U256,
    fee: u32,
) -> anyhow::Result<Vec<u8>> {
    validate_currency_pair(token_in, token_out)?;
    ensure!(
        !amount_out.is_zero(),
        "V3 exact-output quote amount is zero"
    );
    ensure!(
        fee > 0 && fee <= 0x00ff_ffff,
        "V3 exact-output quote fee does not fit uint24"
    );

    // PancakeSwap and Uniswap QuoterV2 share this exact static tuple shape.
    let mut encoded =
        selector("quoteExactOutputSingle((address,address,uint256,uint24,uint160))").to_vec();
    push_address_word(&mut encoded, token_in);
    push_address_word(&mut encoded, token_out);
    push_u256_word(&mut encoded, amount_out);
    push_u256_word(&mut encoded, U256::from(fee));
    push_u256_word(&mut encoded, U256::ZERO);
    Ok(encoded)
}

pub fn decode_v3_quote_exact_output_single(encoded: &[u8]) -> anyhow::Result<U256> {
    ensure!(
        encoded.len() >= 4 * WORD_BYTES,
        "V3 exact-output QuoterV2 result is truncated"
    );
    Ok(U256::from_be_slice(&encoded[..WORD_BYTES]))
}

pub fn camelot_v3_quote_exact_input_single(
    token_in: Address,
    token_out: Address,
    amount_in: U256,
) -> anyhow::Result<Vec<u8>> {
    validate_currency_pair(token_in, token_out)?;
    ensure!(!amount_in.is_zero(), "Camelot V3 quote input is zero");
    let mut encoded = selector("quoteExactInputSingle(address,address,uint256,uint160)").to_vec();
    push_address_word(&mut encoded, token_in);
    push_address_word(&mut encoded, token_out);
    push_u256_word(&mut encoded, amount_in);
    push_u256_word(&mut encoded, U256::ZERO);
    Ok(encoded)
}

pub fn camelot_v3_quote_exact_output_single(
    token_in: Address,
    token_out: Address,
    amount_out: U256,
) -> anyhow::Result<Vec<u8>> {
    validate_currency_pair(token_in, token_out)?;
    ensure!(
        !amount_out.is_zero(),
        "Camelot V3 exact-output quote amount is zero"
    );
    let mut encoded = selector("quoteExactOutputSingle(address,address,uint256,uint160)").to_vec();
    push_address_word(&mut encoded, token_in);
    push_address_word(&mut encoded, token_out);
    push_u256_word(&mut encoded, amount_out);
    push_u256_word(&mut encoded, U256::ZERO);
    Ok(encoded)
}

pub fn decode_camelot_v3_quote(encoded: &[u8]) -> anyhow::Result<(U256, u16)> {
    ensure!(
        encoded.len() == 2 * WORD_BYTES,
        "Camelot V3 Quoter result has an unexpected shape"
    );
    let fee_word = U256::from_be_slice(&encoded[WORD_BYTES..]);
    let fee = u16::try_from(fee_word).context("Camelot V3 Quoter fee does not fit uint16")?;
    Ok((U256::from_be_slice(&encoded[..WORD_BYTES]), fee))
}

/// Lynex uses the upstream single-fee Algebra V1.9 router tuple. Keep a typed
/// entry point even though its bytes match the reviewed Camelot router today;
/// provider selection must never infer ABI compatibility from that coincidence.
pub fn lynex_algebra_v1_9_exact_input_single(
    token_in: Address,
    token_out: Address,
    recipient: Address,
    deadline: u64,
    amount_in: U256,
    amount_out_minimum: U256,
) -> anyhow::Result<Vec<u8>> {
    camelot_v3_exact_input_single(
        token_in,
        token_out,
        recipient,
        deadline,
        amount_in,
        amount_out_minimum,
    )
}

pub fn lynex_algebra_v1_9_quote_exact_input_single(
    token_in: Address,
    token_out: Address,
    amount_in: U256,
) -> anyhow::Result<Vec<u8>> {
    camelot_v3_quote_exact_input_single(token_in, token_out, amount_in)
}

pub fn lynex_algebra_v1_9_quote_exact_output_single(
    token_in: Address,
    token_out: Address,
    amount_out: U256,
) -> anyhow::Result<Vec<u8>> {
    camelot_v3_quote_exact_output_single(token_in, token_out, amount_out)
}

pub fn decode_lynex_algebra_v1_9_quote(encoded: &[u8]) -> anyhow::Result<(U256, u16)> {
    decode_camelot_v3_quote(encoded)
}

pub fn v4_exact_input_single(
    pool_key: V4PoolKey,
    zero_for_one: bool,
    amount_in: U256,
    amount_out_minimum: U256,
    currency_in: Address,
    currency_out: Address,
    deadline: u64,
) -> anyhow::Result<Vec<u8>> {
    validate_currency_pair(currency_in, currency_out)?;
    ensure!(!amount_in.is_zero(), "Uniswap V4 input amount is zero");
    ensure!(deadline > 0, "Uniswap V4 deadline is zero");
    ensure!(
        pool_key.currency0 < pool_key.currency1,
        "Uniswap V4 currencies are not sorted"
    );
    ensure!(
        pool_key.fee_pips > 0 && pool_key.fee_pips <= 0x00ff_ffff,
        "Uniswap V4 fee does not fit uint24"
    );
    ensure!(
        (-8_388_608..=8_388_607).contains(&pool_key.tick_spacing),
        "Uniswap V4 tick spacing does not fit int24"
    );
    ensure!(
        (zero_for_one && currency_in == pool_key.currency0 && currency_out == pool_key.currency1)
            || (!zero_for_one
                && currency_in == pool_key.currency1
                && currency_out == pool_key.currency0),
        "Uniswap V4 swap direction does not match the pool key"
    );
    ensure!(
        amount_in <= U256::from(u128::MAX) && amount_out_minimum <= U256::from(u128::MAX),
        "Uniswap V4 amount does not fit uint128"
    );

    // abi.encode((PoolKey,bool,uint128,uint128,bytes))
    let mut swap = Vec::with_capacity(11 * WORD_BYTES);
    push_usize_word(&mut swap, WORD_BYTES);
    push_address_word(&mut swap, pool_key.currency0);
    push_address_word(&mut swap, pool_key.currency1);
    push_u256_word(&mut swap, U256::from(pool_key.fee_pips));
    push_signed_i32_word(&mut swap, pool_key.tick_spacing);
    push_address_word(&mut swap, pool_key.hooks);
    push_bool_word(&mut swap, zero_for_one);
    push_u256_word(&mut swap, amount_in);
    push_u256_word(&mut swap, amount_out_minimum);
    push_usize_word(&mut swap, 9 * WORD_BYTES);
    swap.extend_from_slice(&encode_bytes(&[]));

    let settle = encode_address_u256(currency_in, amount_in);
    let take = encode_address_u256(currency_out, amount_out_minimum);
    let actions = [
        V4_ACTION_SWAP_EXACT_IN_SINGLE,
        V4_ACTION_SETTLE_ALL,
        V4_ACTION_TAKE_ALL,
    ];
    let v4_input = encode_bytes_and_bytes_array(&actions, &[swap, settle, take]);

    // UniversalRouter.execute(bytes,bytes[],uint256)
    let commands = encode_bytes(&[V4_SWAP_COMMAND]);
    let inputs = encode_bytes_array(&[v4_input]);
    let mut encoded = selector("execute(bytes,bytes[],uint256)").to_vec();
    push_usize_word(&mut encoded, 3 * WORD_BYTES);
    push_usize_word(&mut encoded, 3 * WORD_BYTES + commands.len());
    push_u256_word(&mut encoded, U256::from(deadline));
    encoded.extend_from_slice(&commands);
    encoded.extend_from_slice(&inputs);
    Ok(encoded)
}

pub fn v4_quote_exact_input_single(
    pool_key: V4PoolKey,
    zero_for_one: bool,
    amount_in: U256,
) -> anyhow::Result<Vec<u8>> {
    ensure!(!amount_in.is_zero(), "Uniswap V4 quote input is zero");
    ensure!(
        amount_in <= U256::from(u128::MAX),
        "Uniswap V4 quote input does not fit uint128"
    );
    ensure!(
        pool_key.currency0 < pool_key.currency1,
        "Uniswap V4 quote currencies are not sorted"
    );

    // quoteExactInputSingle(((address,address,uint24,int24,address),bool,uint128,bytes)).
    // The only top-level tuple is dynamic because hookData is bytes.
    let mut encoded = selector(
        "quoteExactInputSingle(((address,address,uint24,int24,address),bool,uint128,bytes))",
    )
    .to_vec();
    push_usize_word(&mut encoded, WORD_BYTES);
    push_address_word(&mut encoded, pool_key.currency0);
    push_address_word(&mut encoded, pool_key.currency1);
    push_u256_word(&mut encoded, U256::from(pool_key.fee_pips));
    push_signed_i32_word(&mut encoded, pool_key.tick_spacing);
    push_address_word(&mut encoded, pool_key.hooks);
    push_bool_word(&mut encoded, zero_for_one);
    push_u256_word(&mut encoded, amount_in);
    push_usize_word(&mut encoded, 8 * WORD_BYTES);
    encoded.extend_from_slice(&encode_bytes(&[]));
    Ok(encoded)
}

pub fn decode_v4_quote_exact_input_single(encoded: &[u8]) -> anyhow::Result<(U256, U256)> {
    ensure!(
        encoded.len() >= 2 * WORD_BYTES,
        "Uniswap V4 quote result is truncated"
    );
    Ok((
        U256::from_be_slice(&encoded[..WORD_BYTES]),
        U256::from_be_slice(&encoded[WORD_BYTES..2 * WORD_BYTES]),
    ))
}

pub fn permit2_allowance(
    owner: Address,
    token: Address,
    spender: Address,
) -> anyhow::Result<Vec<u8>> {
    ensure!(owner != Address::ZERO, "Permit2 allowance owner is zero");
    validate_currency_pair(token, spender)?;
    let mut encoded = selector("allowance(address,address,address)").to_vec();
    push_address_word(&mut encoded, owner);
    push_address_word(&mut encoded, token);
    push_address_word(&mut encoded, spender);
    Ok(encoded)
}

pub fn permit2_approve(
    token: Address,
    spender: Address,
    amount: U256,
    expiration: u64,
) -> anyhow::Result<Vec<u8>> {
    validate_currency_pair(token, spender)?;
    ensure!(
        amount <= (U256::from(1_u8) << 160) - U256::from(1_u8),
        "Permit2 amount does not fit uint160"
    );
    ensure!(
        expiration < (1_u64 << 48),
        "Permit2 expiration does not fit uint48"
    );
    let mut encoded = selector("approve(address,address,uint160,uint48)").to_vec();
    push_address_word(&mut encoded, token);
    push_address_word(&mut encoded, spender);
    push_u256_word(&mut encoded, amount);
    push_u256_word(&mut encoded, U256::from(expiration));
    Ok(encoded)
}

pub fn decode_permit2_allowance(encoded: &[u8]) -> anyhow::Result<(U256, u64)> {
    ensure!(
        encoded.len() >= 3 * WORD_BYTES,
        "Permit2 allowance result is truncated"
    );
    let amount = U256::from_be_slice(&encoded[..WORD_BYTES]);
    ensure!(
        amount <= (U256::from(1_u8) << 160) - U256::from(1_u8),
        "Permit2 returned an invalid uint160 allowance"
    );
    let expiration = U256::from_be_slice(&encoded[WORD_BYTES..2 * WORD_BYTES]);
    let expiration = u64::try_from(expiration).context("Permit2 expiration does not fit u64")?;
    ensure!(
        expiration < (1_u64 << 48),
        "Permit2 returned an invalid uint48 expiration"
    );
    Ok((amount, expiration))
}

fn validate_currency_pair(left: Address, right: Address) -> anyhow::Result<()> {
    ensure!(left != Address::ZERO, "currency address is zero");
    ensure!(right != Address::ZERO, "currency address is zero");
    ensure!(left != right, "currency addresses are identical");
    Ok(())
}

fn selector(signature: &str) -> [u8; 4] {
    keccak256(signature.as_bytes())[..4]
        .try_into()
        .expect("function selector is four bytes")
}

fn encode_address_u256(address: Address, amount: U256) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(2 * WORD_BYTES);
    push_address_word(&mut encoded, address);
    push_u256_word(&mut encoded, amount);
    encoded
}

fn encode_bytes(bytes: &[u8]) -> Vec<u8> {
    let padded = bytes.len().div_ceil(WORD_BYTES) * WORD_BYTES;
    let mut encoded = Vec::with_capacity(WORD_BYTES + padded);
    push_usize_word(&mut encoded, bytes.len());
    encoded.extend_from_slice(bytes);
    encoded.resize(WORD_BYTES + padded, 0);
    encoded
}

fn encode_bytes_array(values: &[Vec<u8>]) -> Vec<u8> {
    let tails = values
        .iter()
        .map(|value| encode_bytes(value))
        .collect::<Vec<_>>();
    let mut encoded = Vec::new();
    push_usize_word(&mut encoded, values.len());
    let mut offset = values.len() * WORD_BYTES;
    for tail in &tails {
        push_usize_word(&mut encoded, offset);
        offset += tail.len();
    }
    for tail in tails {
        encoded.extend_from_slice(&tail);
    }
    encoded
}

fn encode_bytes_and_bytes_array(bytes: &[u8], values: &[Vec<u8>]) -> Vec<u8> {
    let bytes = encode_bytes(bytes);
    let values = encode_bytes_array(values);
    let mut encoded = Vec::with_capacity(2 * WORD_BYTES + bytes.len() + values.len());
    push_usize_word(&mut encoded, 2 * WORD_BYTES);
    push_usize_word(&mut encoded, 2 * WORD_BYTES + bytes.len());
    encoded.extend_from_slice(&bytes);
    encoded.extend_from_slice(&values);
    encoded
}

fn push_address_word(encoded: &mut Vec<u8>, address: Address) {
    encoded.extend_from_slice(&[0_u8; 12]);
    encoded.extend_from_slice(address.as_slice());
}

fn push_u256_word(encoded: &mut Vec<u8>, value: U256) {
    encoded.extend_from_slice(&value.to_be_bytes::<32>());
}

fn push_usize_word(encoded: &mut Vec<u8>, value: usize) {
    push_u256_word(encoded, U256::from(value));
}

fn push_bool_word(encoded: &mut Vec<u8>, value: bool) {
    push_u256_word(encoded, U256::from(u8::from(value)));
}

fn push_signed_i32_word(encoded: &mut Vec<u8>, value: i32) {
    let fill = if value < 0 { 0xff } else { 0x00 };
    encoded.extend_from_slice(&[fill; 28]);
    encoded.extend_from_slice(&value.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use std::{hint::black_box, str::FromStr};

    use alloy_primitives::{Address, U256, hex, keccak256};

    use crate::paired_benchmark::{
        assert_named_paired_non_regression, assert_paired_non_regression,
    };

    use super::{
        camelot_v3_exact_input_single, camelot_v3_quote_exact_input_single,
        camelot_v3_quote_exact_output_single, decode_camelot_v3_quote,
        decode_lynex_algebra_v1_9_quote, decode_permit2_allowance,
        decode_v3_quote_exact_input_single, lynex_algebra_v1_9_exact_input_single,
        lynex_algebra_v1_9_quote_exact_input_single, lynex_algebra_v1_9_quote_exact_output_single,
        pancake_v3_exact_input_single, permit2_approve, v3_exact_input,
        v3_quote_exact_input_single, v4_exact_input_single,
    };
    use crate::dex::pool_id::V4PoolKey;

    fn address(value: &str) -> Address {
        Address::from_str(value).unwrap()
    }

    #[test]
    fn v3_path_and_dynamic_tuple_match_swap_router_abi() {
        let wld = address("0x2cfc85d8e48f8eab294be644d9e25c3030863003");
        let usdc = address("0x79a02482a880bce3f13e09da970dc34db4cd24d1");
        let calldata = v3_exact_input(
            wld,
            usdc,
            10_000,
            wld,
            U256::from(10_u128.pow(19)),
            U256::from(3_000_000),
        )
        .unwrap();
        assert_eq!(&calldata[..4], &[0xb8, 0x58, 0x18, 0x3f]);
        assert_eq!(calldata.len(), 4 + 8 * 32);
        assert_eq!(&calldata[4 + 6 * 32..4 + 6 * 32 + 20], wld.as_slice());
        assert_eq!(
            &calldata[4 + 6 * 32 + 20..4 + 6 * 32 + 23],
            &[0x00, 0x27, 0x10]
        );
        assert_eq!(&calldata[4 + 6 * 32 + 23..4 + 6 * 32 + 43], usdc.as_slice());
    }

    #[test]
    fn arbitrum_v3_quoter_v2_calldata_is_exact_and_decodes_amount_out() {
        let usdc = address("0xaf88d065e77c8cc2239327c5edb3a432268e5831");
        let esp = address("0x3b8db18e69d6686ad9371a423afe3dd1065c94f1");
        let calldata =
            v3_quote_exact_input_single(usdc, esp, U256::from(10_000_000_u64), 100).unwrap();
        assert_eq!(&calldata[..4], &[0xc6, 0xa5, 0x02, 0x6a]);
        assert_eq!(calldata.len(), 4 + 5 * 32);
        assert_eq!(
            format!("{:#x}", keccak256(&calldata)),
            "0xfc58465ddd636b3bad7ec219ebf7b0c2243c14493d09d6f72cc7f79411a07e73"
        );

        let mut response = vec![0_u8; 4 * 32];
        response[..32].copy_from_slice(&U256::from(123_u64).to_be_bytes::<32>());
        assert_eq!(
            decode_v3_quote_exact_input_single(&response).unwrap(),
            U256::from(123_u64)
        );
    }

    #[test]
    fn pancake_v3_exact_input_single_matches_reviewed_v3_only_router_abi() {
        let usdc = address("0xaf88d065e77c8cc2239327c5edb3a432268e5831");
        let arb = address("0x912ce59144191c1204e64559fe8253a0e49e6548");
        let recipient = Address::repeat_byte(0x44);
        let calldata = pancake_v3_exact_input_single(
            usdc,
            arb,
            500,
            recipient,
            1_800_000_000,
            U256::from(6_000_000_u64),
            U256::from(7_000_000_000_000_000_000_u128),
        )
        .unwrap();

        assert_eq!(&calldata[..4], &[0x41, 0x4b, 0xf3, 0x89]);
        assert_eq!(calldata.len(), 4 + 8 * 32);
        assert_eq!(&calldata[4 + 12..4 + 32], usdc.as_slice());
        assert_eq!(&calldata[4 + 32 + 12..4 + 2 * 32], arb.as_slice());
        assert_eq!(
            U256::from_be_slice(&calldata[4 + 4 * 32..4 + 5 * 32]),
            U256::from(1_800_000_000_u64)
        );
        assert_eq!(
            U256::from_be_slice(&calldata[4 + 7 * 32..4 + 8 * 32]),
            U256::ZERO
        );
    }

    #[test]
    fn camelot_v3_quoter_calldata_and_directional_fee_result_are_exact() {
        let usdc = address("0xaf88d065e77c8cc2239327c5edb3a432268e5831");
        let arb = address("0x912ce59144191c1204e64559fe8253a0e49e6548");
        let input =
            camelot_v3_quote_exact_input_single(usdc, arb, U256::from(200_000_000_u64)).unwrap();
        let output = camelot_v3_quote_exact_output_single(
            usdc,
            arb,
            U256::from(160_u8) * U256::from(10_u64).pow(U256::from(18_u8)),
        )
        .unwrap();
        assert_eq!(&input[..4], &[0x2d, 0x9e, 0xbd, 0x1d]);
        assert_eq!(&output[..4], &[0x9e, 0x73, 0xc8, 0x1d]);
        assert_eq!(input.len(), 4 + 4 * 32);
        assert_eq!(output.len(), 4 + 4 * 32);

        let mut response = vec![0_u8; 64];
        response[..32].copy_from_slice(&U256::from(123_u8).to_be_bytes::<32>());
        response[32..].copy_from_slice(&U256::from(117_u8).to_be_bytes::<32>());
        assert_eq!(
            decode_camelot_v3_quote(&response).unwrap(),
            (U256::from(123_u8), 117)
        );
        response.push(0);
        assert!(decode_camelot_v3_quote(&response).is_err());
    }

    #[test]
    fn camelot_v3_exact_input_single_is_the_reviewed_seven_word_tuple() {
        let usdc = address("0xaf88d065e77c8cc2239327c5edb3a432268e5831");
        let arb = address("0x912ce59144191c1204e64559fe8253a0e49e6548");
        let recipient = Address::repeat_byte(0x44);
        let calldata = camelot_v3_exact_input_single(
            usdc,
            arb,
            recipient,
            1_900_000_002,
            U256::from(200_000_000_u64),
            U256::from(150_u8) * U256::from(10_u64).pow(U256::from(18_u8)),
        )
        .unwrap();

        assert_eq!(&calldata[..4], &[0xbc, 0x65, 0x11, 0x88]);
        assert_eq!(calldata.len(), 4 + 7 * 32);
        assert_eq!(&calldata[16..36], usdc.as_slice());
        assert_eq!(&calldata[48..68], arb.as_slice());
        assert_eq!(&calldata[80..100], recipient.as_slice());
        assert_eq!(
            U256::from_be_slice(&calldata[100..132]),
            U256::from(1_900_000_002_u64)
        );
        assert_eq!(U256::from_be_slice(&calldata[196..228]), U256::ZERO);
    }

    #[test]
    fn lynex_single_fee_algebra_router_and_quoter_abis_are_byte_exact() {
        let usdc = address("0x176211869cA2b568f2A7D4EE941E073a821EE1ff");
        let usdt = address("0xA219439258ca9da29e9cC4cE5596924745e12B93");
        let recipient = Address::repeat_byte(0x59);
        let amount_in = U256::from(200_000_000_u64);
        let minimum_out = U256::from(199_000_000_u64);

        let swap = lynex_algebra_v1_9_exact_input_single(
            usdc,
            usdt,
            recipient,
            1_900_000_003,
            amount_in,
            minimum_out,
        )
        .unwrap();
        let quote_in = lynex_algebra_v1_9_quote_exact_input_single(usdc, usdt, amount_in).unwrap();
        let quote_out =
            lynex_algebra_v1_9_quote_exact_output_single(usdc, usdt, minimum_out).unwrap();

        assert_eq!(&swap[..4], &[0xbc, 0x65, 0x11, 0x88]);
        assert_eq!(&quote_in[..4], &[0x2d, 0x9e, 0xbd, 0x1d]);
        assert_eq!(&quote_out[..4], &[0x9e, 0x73, 0xc8, 0x1d]);
        assert_eq!(swap.len(), 4 + 7 * 32);
        assert_eq!(quote_in.len(), 4 + 4 * 32);
        assert_eq!(quote_out.len(), 4 + 4 * 32);
        assert_eq!(&swap[4 + 12..4 + 32], usdc.as_slice());
        assert_eq!(&swap[4 + 32 + 12..4 + 2 * 32], usdt.as_slice());
        assert_eq!(
            U256::from_be_slice(&swap[4 + 4 * 32..4 + 5 * 32]),
            amount_in
        );
        assert_eq!(
            U256::from_be_slice(&swap[4 + 5 * 32..4 + 6 * 32]),
            minimum_out
        );

        let mut response = vec![0_u8; 64];
        response[..32].copy_from_slice(&U256::from(199_500_000_u64).to_be_bytes::<32>());
        response[32..].copy_from_slice(&U256::from(50_u8).to_be_bytes::<32>());
        assert_eq!(
            decode_lynex_algebra_v1_9_quote(&response).unwrap(),
            (U256::from(199_500_000_u64), 50)
        );
        response[32] = 1;
        assert!(decode_lynex_algebra_v1_9_quote(&response).is_err());
    }

    #[test]
    #[ignore = "manual release-mode paired Lynex/Uniswap calldata benchmark"]
    fn benchmark_uniswap_and_lynex_calldata_builders() {
        let usdc = address("0x176211869cA2b568f2A7D4EE941E073a821EE1ff");
        let usdt = address("0xA219439258ca9da29e9cC4cE5596924745e12B93");
        let recipient = Address::repeat_byte(0x59);
        assert_named_paired_non_regression(
            "lynex_algebra_v1_9_calldata_build_benchmark",
            1.10,
            "uniswap_v3",
            "lynex_algebra_v1_9",
            || {
                black_box(v3_exact_input(
                    usdt,
                    usdc,
                    50,
                    recipient,
                    U256::from(6_000_000_u64),
                    U256::from(5_900_000_u64),
                ))
                .unwrap();
            },
            || {
                black_box(lynex_algebra_v1_9_exact_input_single(
                    usdt,
                    usdc,
                    recipient,
                    1_900_000_002,
                    U256::from(6_000_000_u64),
                    U256::from(5_900_000_u64),
                ))
                .unwrap();
            },
        );
    }

    #[test]
    #[ignore = "manual release-mode paired Camelot/Uniswap calldata benchmark"]
    fn benchmark_uniswap_and_camelot_v3_calldata_builders() {
        let usdc = address("0xaf88d065e77c8cc2239327c5edb3a432268e5831");
        let arb = address("0x912ce59144191c1204e64559fe8253a0e49e6548");
        let recipient = Address::repeat_byte(0x44);
        assert_named_paired_non_regression(
            "camelot_v3_calldata_build_benchmark",
            1.10,
            "uniswap_v3",
            "camelot_v3",
            || {
                black_box(v3_exact_input(
                    usdc,
                    arb,
                    500,
                    recipient,
                    U256::from(6_000_000_u64),
                    U256::from(7_000_000_000_000_000_000_u128),
                ))
                .unwrap();
            },
            || {
                black_box(camelot_v3_exact_input_single(
                    usdc,
                    arb,
                    recipient,
                    1_900_000_002,
                    U256::from(6_000_000_u64),
                    U256::from(7_000_000_000_000_000_000_u128),
                ))
                .unwrap();
            },
        );
    }

    #[test]
    #[ignore = "manual release-mode paired V3 calldata benchmark"]
    fn benchmark_uniswap_and_pancake_v3_calldata_builders() {
        let usdc = address("0xaf88d065e77c8cc2239327c5edb3a432268e5831");
        let arb = address("0x912ce59144191c1204e64559fe8253a0e49e6548");
        let recipient = Address::repeat_byte(0x44);
        assert_paired_non_regression(
            "v3_calldata_build_benchmark",
            1.10,
            || {
                black_box(v3_exact_input(
                    usdc,
                    arb,
                    500,
                    recipient,
                    U256::from(6_000_000_u64),
                    U256::from(7_000_000_000_000_000_000_u128),
                ))
                .unwrap();
            },
            || {
                black_box(pancake_v3_exact_input_single(
                    usdc,
                    arb,
                    500,
                    recipient,
                    1_800_000_000,
                    U256::from(6_000_000_u64),
                    U256::from(7_000_000_000_000_000_000_u128),
                ))
                .unwrap();
            },
        );
    }

    #[test]
    fn v4_exact_input_matches_rails_sdk_fixture_shape() {
        let wld = address("0x2cfc85d8e48f8eab294be644d9e25c3030863003");
        let usdc = address("0x79a02482a880bce3f13e09da970dc34db4cd24d1");
        let calldata = v4_exact_input_single(
            V4PoolKey::new(wld, usdc, 10_000, 200, Address::ZERO).unwrap(),
            true,
            U256::from(10_u128.pow(19)),
            U256::from(3_000_000),
            wld,
            usdc,
            1_800_000_000,
        )
        .unwrap();
        assert_eq!(&calldata[..4], &[0x35, 0x93, 0x56, 0x4c]);
        assert_eq!(calldata.len(), 1_092);
        assert_eq!(hex::encode(&calldata[4..4 + 32]), format!("{:064x}", 0x60));
        assert!(
            calldata
                .windows(3)
                .any(|window| window == [0x06, 0x0c, 0x0f])
        );
        assert_eq!(
            format!("{:#x}", keccak256(&calldata)),
            "0x636e5f3505a18a7653c1d1cc710947f98b75a44d866bb916dc1549dd2f70999b"
        );
    }

    #[test]
    fn permit2_values_are_bounded_and_decoded() {
        let token = Address::repeat_byte(0x11);
        let spender = Address::repeat_byte(0x22);
        let calldata = permit2_approve(
            token,
            spender,
            (U256::from(1_u8) << 160) - U256::from(1_u8),
            1_800_000_000,
        )
        .unwrap();
        assert_eq!(&calldata[..4], &[0x87, 0x51, 0x7c, 0x45]);

        let mut response = vec![0_u8; 96];
        response[..32].copy_from_slice(&U256::from(50_u8).to_be_bytes::<32>());
        response[32..64].copy_from_slice(&U256::from(1_800_000_000_u64).to_be_bytes::<32>());
        assert_eq!(
            decode_permit2_allowance(&response).unwrap(),
            (U256::from(50_u8), 1_800_000_000)
        );
    }
}

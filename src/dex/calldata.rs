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
    use std::str::FromStr;

    use alloy_primitives::{Address, U256, hex, keccak256};

    use super::{decode_permit2_allowance, permit2_approve, v3_exact_input, v4_exact_input_single};
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

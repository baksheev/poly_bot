use alloy_primitives::{B256, U256, keccak256};
use anyhow::{Context, ensure};

const ABI_WORD_BYTES: usize = 32;
const SINGLE_FEE_GLOBAL_STATE_WORDS: usize = 7;
const SINGLE_FEE_EVENT_SIGNATURE: &str = "Fee(uint16)";

/// The seven-word Algebra V1.9 global state used by the reviewed Lynex pool.
///
/// This is deliberately distinct from Camelot's eight-word, directional-fee
/// profile. Keeping the ABI shapes separate makes an upstream protocol change
/// fail before a pool can be published to the strategy owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SingleFeeGlobalState {
    pub sqrt_price_x96: U256,
    pub tick: i32,
    pub fee_pips: u16,
    pub timepoint_index: u16,
    pub community_fee_token0: u8,
    pub community_fee_token1: u8,
    pub unlocked: bool,
}

pub fn decode_single_fee_global_state(encoded: &[u8]) -> anyhow::Result<SingleFeeGlobalState> {
    ensure!(
        encoded.len() == SINGLE_FEE_GLOBAL_STATE_WORDS * ABI_WORD_BYTES,
        "single-fee Algebra V1.9 globalState response must contain seven words"
    );
    let unlocked = decode_bool(encoded, 6)?;
    ensure!(unlocked, "single-fee Algebra V1.9 pool is locked");
    Ok(SingleFeeGlobalState {
        sqrt_price_x96: decode_uint(encoded, 0, 160)?,
        tick: decode_i24(encoded, 1)?,
        fee_pips: u16::try_from(decode_uint(encoded, 2, 16)?)
            .context("single-fee Algebra V1.9 fee does not fit uint16")?,
        timepoint_index: u16::try_from(decode_uint(encoded, 3, 16)?)
            .context("single-fee Algebra V1.9 timepoint index does not fit uint16")?,
        community_fee_token0: u8::try_from(decode_uint(encoded, 4, 8)?)
            .context("single-fee Algebra V1.9 token0 community fee does not fit uint8")?,
        community_fee_token1: u8::try_from(decode_uint(encoded, 5, 8)?)
            .context("single-fee Algebra V1.9 token1 community fee does not fit uint8")?,
        unlocked,
    })
}

pub fn single_fee_event_topic() -> B256 {
    keccak256(SINGLE_FEE_EVENT_SIGNATURE)
}

pub fn decode_single_fee_event(encoded: &[u8]) -> anyhow::Result<u16> {
    ensure!(
        encoded.len() == ABI_WORD_BYTES,
        "single-fee Algebra V1.9 Fee event must contain one word"
    );
    u16::try_from(decode_uint(encoded, 0, 16)?)
        .context("single-fee Algebra V1.9 Fee event does not fit uint16")
}

fn decode_word(encoded: &[u8], index: usize) -> anyhow::Result<&[u8]> {
    let start = index
        .checked_mul(ABI_WORD_BYTES)
        .context("single-fee Algebra V1.9 ABI word offset overflow")?;
    let end = start
        .checked_add(ABI_WORD_BYTES)
        .context("single-fee Algebra V1.9 ABI word end overflow")?;
    encoded
        .get(start..end)
        .with_context(|| format!("single-fee Algebra V1.9 ABI word {index} is missing"))
}

fn decode_uint(encoded: &[u8], index: usize, bits: usize) -> anyhow::Result<U256> {
    ensure!(
        bits > 0 && bits <= 256 && bits.is_multiple_of(8),
        "invalid single-fee Algebra V1.9 integer width"
    );
    let word = decode_word(encoded, index)?;
    let value_bytes = bits / 8;
    ensure!(
        word[..ABI_WORD_BYTES - value_bytes]
            .iter()
            .all(|byte| *byte == 0),
        "single-fee Algebra V1.9 uint{bits} word has non-zero padding"
    );
    Ok(U256::from_be_slice(word))
}

fn decode_i24(encoded: &[u8], index: usize) -> anyhow::Result<i32> {
    let word = decode_word(encoded, index)?;
    let negative = word[29] & 0x80 != 0;
    let padding = if negative { 0xff } else { 0x00 };
    ensure!(
        word[..29].iter().all(|byte| *byte == padding),
        "single-fee Algebra V1.9 int24 word has invalid sign extension"
    );
    let raw = i32::from_be_bytes([0, word[29], word[30], word[31]]);
    Ok(if negative { raw | !0x00ff_ffff } else { raw })
}

fn decode_bool(encoded: &[u8], index: usize) -> anyhow::Result<bool> {
    let value = decode_uint(encoded, index, 8)?;
    ensure!(
        value == U256::ZERO || value == U256::ONE,
        "single-fee Algebra V1.9 bool word is not zero or one"
    );
    Ok(value == U256::ONE)
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{B256, U256, b256};

    use super::{decode_single_fee_event, decode_single_fee_global_state, single_fee_event_topic};

    fn push_word(encoded: &mut Vec<u8>, value: U256) {
        encoded.extend_from_slice(&value.to_be_bytes::<32>());
    }

    fn reviewed_global_state() -> Vec<u8> {
        let mut encoded = Vec::with_capacity(7 * 32);
        push_word(
            &mut encoded,
            U256::from(79_260_977_712_710_489_041_547_899_366_u128),
        );
        push_word(&mut encoded, U256::from(8_u8));
        push_word(&mut encoded, U256::from(50_u8));
        push_word(&mut encoded, U256::from(29_932_u16));
        push_word(&mut encoded, U256::from(30_u8));
        push_word(&mut encoded, U256::from(30_u8));
        push_word(&mut encoded, U256::ONE);
        encoded
    }

    #[test]
    fn decodes_reviewed_seven_word_single_fee_global_state() {
        let state = decode_single_fee_global_state(&reviewed_global_state()).unwrap();
        assert_eq!(
            state.sqrt_price_x96,
            U256::from(79_260_977_712_710_489_041_547_899_366_u128)
        );
        assert_eq!(state.tick, 8);
        assert_eq!(state.fee_pips, 50);
        assert_eq!(state.timepoint_index, 29_932);
        assert_eq!(state.community_fee_token0, 30);
        assert_eq!(state.community_fee_token1, 30);
        assert!(state.unlocked);
    }

    #[test]
    fn global_state_rejects_directional_shape_padding_lock_and_invalid_bool() {
        let mut eight_words = reviewed_global_state();
        push_word(&mut eight_words, U256::ZERO);
        assert!(decode_single_fee_global_state(&eight_words).is_err());

        let mut malformed_fee = reviewed_global_state();
        malformed_fee[2 * 32] = 1;
        assert!(decode_single_fee_global_state(&malformed_fee).is_err());

        let mut locked = reviewed_global_state();
        locked[7 * 32 - 1] = 0;
        assert!(decode_single_fee_global_state(&locked).is_err());

        let mut invalid_bool = reviewed_global_state();
        invalid_bool[7 * 32 - 1] = 2;
        assert!(decode_single_fee_global_state(&invalid_bool).is_err());
    }

    #[test]
    fn global_state_decodes_negative_int24_and_rejects_bad_sign_extension() {
        let mut encoded = reviewed_global_state();
        encoded[32..64].fill(0xff);
        encoded[61..64].copy_from_slice(&[0xff, 0xff, 0xf8]);
        assert_eq!(decode_single_fee_global_state(&encoded).unwrap().tick, -8);

        encoded[32] = 0;
        assert!(decode_single_fee_global_state(&encoded).is_err());
    }

    #[test]
    fn fee_uint16_topic_and_word_are_exact_and_fail_closed() {
        const EXPECTED_TOPIC: B256 =
            b256!("598b9f043c813aa6be3426ca60d1c65d17256312890be5118dab55b0775ebe2a");
        assert_eq!(single_fee_event_topic(), EXPECTED_TOPIC);

        let mut encoded = vec![0_u8; 32];
        encoded[30..].copy_from_slice(&50_u16.to_be_bytes());
        assert_eq!(decode_single_fee_event(&encoded).unwrap(), 50);
        encoded.insert(0, 0);
        assert!(decode_single_fee_event(&encoded).is_err());

        let mut overflow = vec![0_u8; 32];
        overflow[0] = 1;
        assert!(decode_single_fee_event(&overflow).is_err());
    }
}

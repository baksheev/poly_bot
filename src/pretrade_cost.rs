use std::{
    sync::{Arc, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};

use rust_decimal::Decimal;

/// Diagnostic-only inputs for the pre-trade cost model. The trading owner
/// never reads this state: producers publish into it and the background hot
/// telemetry task takes snapshots when it serializes an evaluation.
#[derive(Clone)]
pub struct PreTradeCostTelemetry {
    inner: Arc<RwLock<PreTradeCostInputs>>,
    enabled: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DexProtocol {
    UniswapV3,
    UniswapV4,
}

impl DexProtocol {
    pub const fn label(self) -> &'static str {
        match self {
            Self::UniswapV3 => "uniswap_v3",
            Self::UniswapV4 => "uniswap_v4",
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::UniswapV3 => 0,
            Self::UniswapV4 => 1,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GasPriceTelemetrySource {
    Rpc,
    RailsFallback,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReceiptCostTelemetrySource {
    LiveExecution,
    JournalBootstrap,
}

impl ReceiptCostTelemetrySource {
    pub const fn label(self) -> &'static str {
        match self {
            Self::LiveExecution => "live_execution_receipt",
            Self::JournalBootstrap => "journal_bootstrap_receipt",
        }
    }
}

impl GasPriceTelemetrySource {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Rpc => "cached_rpc",
            Self::RailsFallback => "cached_rails_fallback",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GasPriceTelemetrySample {
    pub captured_unix_us: u64,
    pub gas_price_wei: u128,
    pub maximum_fee_per_gas_wei: u128,
    pub source: GasPriceTelemetrySource,
    pub includes_l1_fee: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeConversionTelemetrySample {
    pub captured_unix_us: u64,
    pub price_token_a: Decimal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DexReceiptCostTelemetrySample {
    pub captured_unix_us: u64,
    pub gas_used: u64,
    pub effective_gas_price_wei: u128,
    pub l1_fee_wei: u128,
    pub source: ReceiptCostTelemetrySource,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PreTradeCostSnapshot {
    gas_prices: [Option<GasPriceTelemetrySample>; 2],
    native_conversions: [Option<NativeConversionTelemetrySample>; 2],
    receipts: [[Option<DexReceiptCostTelemetrySample>; 2]; 2],
}

#[derive(Clone, Copy, Debug, Default)]
struct PreTradeCostInputs {
    gas_prices: [Option<GasPriceTelemetrySample>; 2],
    native_conversions: [Option<NativeConversionTelemetrySample>; 2],
    receipts: [[Option<DexReceiptCostTelemetrySample>; 2]; 2],
}

impl Default for PreTradeCostTelemetry {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(PreTradeCostInputs::default())),
            enabled: true,
        }
    }
}

impl PreTradeCostTelemetry {
    pub fn disabled() -> Self {
        Self {
            inner: Arc::new(RwLock::new(PreTradeCostInputs::default())),
            enabled: false,
        }
    }

    pub fn publish_gas_price(
        &self,
        gas_price_wei: u128,
        maximum_fee_per_gas_wei: u128,
        source: GasPriceTelemetrySource,
        includes_l1_fee: bool,
    ) {
        if !self.enabled {
            return;
        }
        let sample = GasPriceTelemetrySample {
            captured_unix_us: unix_timestamp_us(),
            gas_price_wei,
            maximum_fee_per_gas_wei,
            source,
            includes_l1_fee,
        };
        insert_temporal_sample(&mut self.write().gas_prices, sample, |sample| {
            sample.captured_unix_us
        });
    }

    pub fn publish_native_conversion(&self, captured_unix_us: u64, price_token_a: Decimal) {
        if !self.enabled || price_token_a <= Decimal::ZERO {
            return;
        }
        let sample = NativeConversionTelemetrySample {
            captured_unix_us,
            price_token_a,
        };
        insert_temporal_sample(&mut self.write().native_conversions, sample, |sample| {
            sample.captured_unix_us
        });
    }

    pub fn publish_receipt(
        &self,
        protocol: DexProtocol,
        gas_used: u64,
        effective_gas_price_wei: u128,
        l1_fee_wei: u128,
    ) {
        self.publish_receipt_with_source(
            protocol,
            gas_used,
            effective_gas_price_wei,
            l1_fee_wei,
            ReceiptCostTelemetrySource::LiveExecution,
        );
    }

    pub fn publish_receipt_with_source(
        &self,
        protocol: DexProtocol,
        gas_used: u64,
        effective_gas_price_wei: u128,
        l1_fee_wei: u128,
        source: ReceiptCostTelemetrySource,
    ) {
        if !self.enabled || gas_used == 0 || effective_gas_price_wei == 0 {
            return;
        }
        let sample = DexReceiptCostTelemetrySample {
            captured_unix_us: unix_timestamp_us(),
            gas_used,
            effective_gas_price_wei,
            l1_fee_wei,
            source,
        };
        insert_temporal_sample(
            &mut self.write().receipts[protocol.index()],
            sample,
            |sample| sample.captured_unix_us,
        );
    }

    pub fn snapshot(&self) -> Option<PreTradeCostSnapshot> {
        if !self.enabled {
            return None;
        }
        let inputs = *self.read();
        Some(PreTradeCostSnapshot {
            gas_prices: inputs.gas_prices,
            native_conversions: inputs.native_conversions,
            receipts: inputs.receipts,
        })
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, PreTradeCostInputs> {
        self.inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, PreTradeCostInputs> {
        self.inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl PreTradeCostSnapshot {
    pub fn gas_price_at_or_before(self, captured_unix_us: u64) -> Option<GasPriceTelemetrySample> {
        sample_at_or_before(self.gas_prices, captured_unix_us, |sample| {
            sample.captured_unix_us
        })
    }

    pub fn native_conversion_at_or_before(
        self,
        captured_unix_us: u64,
    ) -> Option<NativeConversionTelemetrySample> {
        sample_at_or_before(self.native_conversions, captured_unix_us, |sample| {
            sample.captured_unix_us
        })
    }

    pub fn receipt_at_or_before(
        self,
        protocol: DexProtocol,
        captured_unix_us: u64,
    ) -> Option<DexReceiptCostTelemetrySample> {
        sample_at_or_before(
            self.receipts[protocol.index()],
            captured_unix_us,
            |sample| sample.captured_unix_us,
        )
    }
}

fn insert_temporal_sample<T: Copy>(
    history: &mut [Option<T>; 2],
    sample: T,
    captured_unix_us: impl Fn(&T) -> u64,
) {
    let sample_time = captured_unix_us(&sample);
    match history[0] {
        None => history[0] = Some(sample),
        Some(current) if sample_time >= captured_unix_us(&current) => {
            if sample_time != captured_unix_us(&current) {
                history[1] = history[0];
            }
            history[0] = Some(sample);
        }
        Some(_) => match history[1] {
            None => history[1] = Some(sample),
            Some(previous) if sample_time >= captured_unix_us(&previous) => {
                history[1] = Some(sample);
            }
            Some(_) => {}
        },
    }
}

fn sample_at_or_before<T: Copy>(
    history: [Option<T>; 2],
    captured_unix_us: u64,
    sample_time: impl Fn(&T) -> u64,
) -> Option<T> {
    history
        .into_iter()
        .flatten()
        .filter(|sample| sample_time(sample) <= captured_unix_us)
        .max_by_key(|sample| sample_time(sample))
}

fn unix_timestamp_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::{DexProtocol, GasPriceTelemetrySource, PreTradeCostTelemetry};

    #[test]
    fn snapshot_keeps_protocol_receipts_separate() {
        let telemetry = PreTradeCostTelemetry::default();
        telemetry.publish_gas_price(100, 200, GasPriceTelemetrySource::Rpc, true);
        telemetry.publish_native_conversion(10, Decimal::new(3_500, 0));
        telemetry.publish_receipt(DexProtocol::UniswapV3, 101, 99, 7);
        telemetry.publish_receipt(DexProtocol::UniswapV4, 202, 88, 6);

        let snapshot = telemetry.snapshot().unwrap();
        assert_eq!(
            snapshot
                .gas_price_at_or_before(u64::MAX)
                .unwrap()
                .maximum_fee_per_gas_wei,
            200
        );
        assert_eq!(
            snapshot
                .native_conversion_at_or_before(u64::MAX)
                .unwrap()
                .captured_unix_us,
            10
        );
        assert_eq!(
            snapshot
                .receipt_at_or_before(DexProtocol::UniswapV3, u64::MAX)
                .unwrap()
                .gas_used,
            101
        );
        assert_eq!(
            snapshot
                .receipt_at_or_before(DexProtocol::UniswapV4, u64::MAX)
                .unwrap()
                .gas_used,
            202
        );
    }

    #[test]
    fn snapshot_keeps_previous_samples_for_no_lookahead_selection() {
        let telemetry = PreTradeCostTelemetry::default();
        telemetry.publish_native_conversion(10, Decimal::new(3_500, 0));
        telemetry.publish_native_conversion(30, Decimal::new(3_600, 0));

        let snapshot = telemetry.snapshot().unwrap();
        assert_eq!(
            snapshot
                .native_conversion_at_or_before(20)
                .unwrap()
                .price_token_a,
            Decimal::new(3_500, 0)
        );
        assert!(snapshot.native_conversion_at_or_before(9).is_none());
    }

    #[test]
    fn disabled_telemetry_never_produces_a_snapshot() {
        let telemetry = PreTradeCostTelemetry::disabled();
        telemetry.publish_native_conversion(10, Decimal::new(3_500, 0));
        assert!(telemetry.snapshot().is_none());
    }
}

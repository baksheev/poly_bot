use std::{
    sync::{Arc, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};

use rust_decimal::Decimal;

/// Diagnostic-only inputs for the pre-trade cost model. The trading owner
/// never reads this state: producers publish into it and the background hot
/// telemetry task takes snapshots when it serializes an evaluation.
#[derive(Clone, Default)]
pub struct PreTradeCostTelemetry {
    inner: Arc<RwLock<PreTradeCostInputs>>,
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
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PreTradeCostSnapshot {
    pub gas_price: Option<GasPriceTelemetrySample>,
    pub native_conversion: Option<NativeConversionTelemetrySample>,
    receipts: [Option<DexReceiptCostTelemetrySample>; 2],
}

#[derive(Clone, Copy, Debug, Default)]
struct PreTradeCostInputs {
    gas_price: Option<GasPriceTelemetrySample>,
    native_conversion: Option<NativeConversionTelemetrySample>,
    receipts: [Option<DexReceiptCostTelemetrySample>; 2],
}

impl PreTradeCostTelemetry {
    pub fn publish_gas_price(
        &self,
        gas_price_wei: u128,
        maximum_fee_per_gas_wei: u128,
        source: GasPriceTelemetrySource,
        includes_l1_fee: bool,
    ) {
        self.write().gas_price = Some(GasPriceTelemetrySample {
            captured_unix_us: unix_timestamp_us(),
            gas_price_wei,
            maximum_fee_per_gas_wei,
            source,
            includes_l1_fee,
        });
    }

    pub fn publish_native_conversion(&self, captured_unix_us: u64, price_token_a: Decimal) {
        if price_token_a <= Decimal::ZERO {
            return;
        }
        self.write().native_conversion = Some(NativeConversionTelemetrySample {
            captured_unix_us,
            price_token_a,
        });
    }

    pub fn publish_receipt(
        &self,
        protocol: DexProtocol,
        gas_used: u64,
        effective_gas_price_wei: u128,
        l1_fee_wei: u128,
    ) {
        if gas_used == 0 || effective_gas_price_wei == 0 {
            return;
        }
        self.write().receipts[protocol.index()] = Some(DexReceiptCostTelemetrySample {
            captured_unix_us: unix_timestamp_us(),
            gas_used,
            effective_gas_price_wei,
            l1_fee_wei,
        });
    }

    pub fn snapshot(&self) -> PreTradeCostSnapshot {
        let inputs = *self.read();
        PreTradeCostSnapshot {
            gas_price: inputs.gas_price,
            native_conversion: inputs.native_conversion,
            receipts: inputs.receipts,
        }
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
    pub fn receipt(self, protocol: DexProtocol) -> Option<DexReceiptCostTelemetrySample> {
        self.receipts[protocol.index()]
    }
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

        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.gas_price.unwrap().maximum_fee_per_gas_wei, 200);
        assert_eq!(snapshot.native_conversion.unwrap().captured_unix_us, 10);
        assert_eq!(
            snapshot.receipt(DexProtocol::UniswapV3).unwrap().gas_used,
            101
        );
        assert_eq!(
            snapshot.receipt(DexProtocol::UniswapV4).unwrap().gas_used,
            202
        );
    }
}

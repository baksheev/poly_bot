use std::{
    sync::{Arc, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};

use alloy_primitives::{Address, B256};
use rust_decimal::Decimal;

pub const GAS_PRICE_HISTORY_DEPTH: usize = 8;
pub const NATIVE_CONVERSION_HISTORY_DEPTH: usize = 32;
pub const RECEIPT_HISTORY_DEPTH: usize = 4;
const MAX_RECEIPT_ROUTES: usize = 16;
const DEX_PROTOCOL_COUNT: usize = 5;

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
    PancakeSwapV3,
    CamelotV3,
    LynexAlgebraV1_9,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DexPoolCostKey {
    UniswapV3(Address),
    UniswapV4(B256),
    PancakeSwapV3(Address),
    CamelotV3(Address),
    LynexAlgebraV1_9(Address),
}

impl DexPoolCostKey {
    pub const fn protocol(self) -> DexProtocol {
        match self {
            Self::UniswapV3(_) => DexProtocol::UniswapV3,
            Self::UniswapV4(_) => DexProtocol::UniswapV4,
            Self::PancakeSwapV3(_) => DexProtocol::PancakeSwapV3,
            Self::CamelotV3(_) => DexProtocol::CamelotV3,
            Self::LynexAlgebraV1_9(_) => DexProtocol::LynexAlgebraV1_9,
        }
    }

    pub fn label(self) -> String {
        match self {
            Self::UniswapV3(address) => format!("uniswap_v3:{address:#x}"),
            Self::UniswapV4(pool_id) => format!("uniswap_v4:{pool_id:#x}"),
            Self::PancakeSwapV3(address) => format!("pancakeswap_v3:{address:#x}"),
            Self::CamelotV3(address) => format!("camelot_v3:{address:#x}"),
            Self::LynexAlgebraV1_9(address) => format!("lynex_algebra_v1_9:{address:#x}"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DexRouteCostKey {
    pub pool: DexPoolCostKey,
    pub token_in: Address,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReceiptCostMatchScope {
    ExactRoute,
    SameProtocolBootstrap,
}

impl ReceiptCostMatchScope {
    pub const fn label(self) -> &'static str {
        match self {
            Self::ExactRoute => "exact_pool_and_input_token",
            Self::SameProtocolBootstrap => "same_protocol_bootstrap_fallback",
        }
    }
}

impl DexProtocol {
    pub const fn label(self) -> &'static str {
        match self {
            Self::UniswapV3 => "uniswap_v3",
            Self::UniswapV4 => "uniswap_v4",
            Self::PancakeSwapV3 => "pancakeswap_v3",
            Self::CamelotV3 => "camelot_v3",
            Self::LynexAlgebraV1_9 => "lynex_algebra_v1_9",
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::UniswapV3 => 0,
            Self::UniswapV4 => 1,
            Self::PancakeSwapV3 => 2,
            Self::CamelotV3 => 3,
            Self::LynexAlgebraV1_9 => 4,
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
    pub source_event_unix_us: Option<u64>,
    pub block_number: Option<u64>,
    pub gas_used: u64,
    pub effective_gas_price_wei: u128,
    pub l1_fee_wei: u128,
    pub source: ReceiptCostTelemetrySource,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SelectedDexReceiptCostTelemetrySample {
    pub sample: DexReceiptCostTelemetrySample,
    pub match_scope: ReceiptCostMatchScope,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TemporalHistory<T: Copy, const N: usize> {
    samples: [Option<T>; N],
}

impl<T: Copy, const N: usize> Default for TemporalHistory<T, N> {
    fn default() -> Self {
        Self { samples: [None; N] }
    }
}

impl<T: Copy, const N: usize> TemporalHistory<T, N> {
    fn insert(&mut self, sample: T, sample_time: impl Fn(&T) -> u64) {
        let timestamp = sample_time(&sample);
        if let Some(index) = self
            .samples
            .iter()
            .position(|existing| existing.is_some_and(|value| sample_time(&value) == timestamp))
        {
            self.samples[index] = Some(sample);
            return;
        }
        let insertion_index = self
            .samples
            .iter()
            .position(|existing| existing.is_none_or(|value| timestamp > sample_time(&value)))
            .unwrap_or(N);
        if insertion_index == N {
            return;
        }
        for index in (insertion_index + 1..N).rev() {
            self.samples[index] = self.samples[index - 1];
        }
        self.samples[insertion_index] = Some(sample);
    }

    fn at_or_before(self, timestamp: u64, sample_time: impl Fn(&T) -> u64) -> Option<T> {
        self.samples
            .into_iter()
            .flatten()
            .find(|sample| sample_time(sample) <= timestamp)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RouteReceiptHistory {
    key: DexRouteCostKey,
    history: TemporalHistory<DexReceiptCostTelemetrySample, RECEIPT_HISTORY_DEPTH>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PreTradeCostSnapshot {
    gas_prices: TemporalHistory<GasPriceTelemetrySample, GAS_PRICE_HISTORY_DEPTH>,
    native_conversions:
        TemporalHistory<NativeConversionTelemetrySample, NATIVE_CONVERSION_HISTORY_DEPTH>,
    protocol_receipts:
        [TemporalHistory<DexReceiptCostTelemetrySample, RECEIPT_HISTORY_DEPTH>; DEX_PROTOCOL_COUNT],
    route_receipts: [Option<RouteReceiptHistory>; MAX_RECEIPT_ROUTES],
}

#[derive(Clone, Copy, Debug, Default)]
struct PreTradeCostInputs {
    gas_prices: TemporalHistory<GasPriceTelemetrySample, GAS_PRICE_HISTORY_DEPTH>,
    native_conversions:
        TemporalHistory<NativeConversionTelemetrySample, NATIVE_CONVERSION_HISTORY_DEPTH>,
    protocol_receipts:
        [TemporalHistory<DexReceiptCostTelemetrySample, RECEIPT_HISTORY_DEPTH>; DEX_PROTOCOL_COUNT],
    route_receipts: [Option<RouteReceiptHistory>; MAX_RECEIPT_ROUTES],
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
        self.write()
            .gas_prices
            .insert(sample, |sample| sample.captured_unix_us);
    }

    pub fn publish_native_conversion(&self, captured_unix_us: u64, price_token_a: Decimal) {
        if !self.enabled || price_token_a <= Decimal::ZERO {
            return;
        }
        let sample = NativeConversionTelemetrySample {
            captured_unix_us,
            price_token_a,
        };
        self.write()
            .native_conversions
            .insert(sample, |sample| sample.captured_unix_us);
    }

    pub fn publish_receipt(
        &self,
        route: DexRouteCostKey,
        gas_used: u64,
        effective_gas_price_wei: u128,
        l1_fee_wei: u128,
        block_number: u64,
    ) {
        self.publish_route_receipt_with_source(
            route,
            gas_used,
            effective_gas_price_wei,
            l1_fee_wei,
            Some(block_number),
            None,
            ReceiptCostTelemetrySource::LiveExecution,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub fn publish_protocol_receipt_with_source(
        &self,
        protocol: DexProtocol,
        gas_used: u64,
        effective_gas_price_wei: u128,
        l1_fee_wei: u128,
        block_number: Option<u64>,
        source_event_unix_us: Option<u64>,
        source: ReceiptCostTelemetrySource,
    ) {
        if !self.enabled || gas_used == 0 || effective_gas_price_wei == 0 {
            return;
        }
        let sample = DexReceiptCostTelemetrySample {
            captured_unix_us: unix_timestamp_us(),
            source_event_unix_us,
            block_number,
            gas_used,
            effective_gas_price_wei,
            l1_fee_wei,
            source,
        };
        self.write().protocol_receipts[protocol.index()]
            .insert(sample, |sample| sample.captured_unix_us);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn publish_route_receipt_with_source(
        &self,
        route: DexRouteCostKey,
        gas_used: u64,
        effective_gas_price_wei: u128,
        l1_fee_wei: u128,
        block_number: Option<u64>,
        source_event_unix_us: Option<u64>,
        source: ReceiptCostTelemetrySource,
    ) {
        if !self.enabled || gas_used == 0 || effective_gas_price_wei == 0 {
            return;
        }
        let sample = DexReceiptCostTelemetrySample {
            captured_unix_us: unix_timestamp_us(),
            source_event_unix_us,
            block_number,
            gas_used,
            effective_gas_price_wei,
            l1_fee_wei,
            source,
        };
        let mut inputs = self.write();
        if let Some(existing) = inputs
            .route_receipts
            .iter_mut()
            .flatten()
            .find(|existing| existing.key == route)
        {
            existing
                .history
                .insert(sample, |sample| sample.captured_unix_us);
            return;
        }
        if let Some(empty) = inputs
            .route_receipts
            .iter_mut()
            .find(|entry| entry.is_none())
        {
            let mut history = TemporalHistory::default();
            history.insert(sample, |sample| sample.captured_unix_us);
            *empty = Some(RouteReceiptHistory {
                key: route,
                history,
            });
        }
    }

    pub fn snapshot(&self) -> Option<PreTradeCostSnapshot> {
        if !self.enabled {
            return None;
        }
        let inputs = *self.read();
        Some(PreTradeCostSnapshot {
            gas_prices: inputs.gas_prices,
            native_conversions: inputs.native_conversions,
            protocol_receipts: inputs.protocol_receipts,
            route_receipts: inputs.route_receipts,
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
        self.gas_prices
            .at_or_before(captured_unix_us, |sample| sample.captured_unix_us)
    }

    pub fn native_conversion_at_or_before(
        self,
        captured_unix_us: u64,
    ) -> Option<NativeConversionTelemetrySample> {
        self.native_conversions
            .at_or_before(captured_unix_us, |sample| sample.captured_unix_us)
    }

    pub fn receipt_at_or_before(
        self,
        route: DexRouteCostKey,
        captured_unix_us: u64,
    ) -> Option<SelectedDexReceiptCostTelemetrySample> {
        if let Some(sample) = self
            .route_receipts
            .into_iter()
            .flatten()
            .find(|entry| entry.key == route)
            .and_then(|entry| {
                entry
                    .history
                    .at_or_before(captured_unix_us, |sample| sample.captured_unix_us)
            })
        {
            return Some(SelectedDexReceiptCostTelemetrySample {
                sample,
                match_scope: ReceiptCostMatchScope::ExactRoute,
            });
        }
        self.protocol_receipts[route.pool.protocol().index()]
            .at_or_before(captured_unix_us, |sample| sample.captured_unix_us)
            .map(|sample| SelectedDexReceiptCostTelemetrySample {
                sample,
                match_scope: ReceiptCostMatchScope::SameProtocolBootstrap,
            })
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
    use alloy_primitives::{Address, B256};
    use rust_decimal::Decimal;

    use super::{
        DexPoolCostKey, DexProtocol, DexRouteCostKey, GasPriceTelemetrySource,
        NATIVE_CONVERSION_HISTORY_DEPTH, PreTradeCostTelemetry, ReceiptCostMatchScope,
        ReceiptCostTelemetrySource,
    };

    #[test]
    fn camelot_cost_identity_never_aliases_uniswap() {
        let address = Address::from([0x42; 20]);
        let camelot = DexPoolCostKey::CamelotV3(address);
        let uniswap = DexPoolCostKey::UniswapV3(address);

        assert_ne!(camelot, uniswap);
        assert_eq!(camelot.protocol(), DexProtocol::CamelotV3);
        assert_eq!(camelot.label(), format!("camelot_v3:{address:#x}"));
        assert_eq!(DexProtocol::CamelotV3.label(), "camelot_v3");
    }

    #[test]
    fn snapshot_keeps_protocol_receipts_separate() {
        let telemetry = PreTradeCostTelemetry::default();
        telemetry.publish_gas_price(100, 200, GasPriceTelemetrySource::Rpc, true);
        telemetry.publish_native_conversion(10, Decimal::new(3_500, 0));
        let v3_route = DexRouteCostKey {
            pool: DexPoolCostKey::UniswapV3(Address::repeat_byte(0x11)),
            token_in: Address::repeat_byte(0x22),
        };
        let v4_route = DexRouteCostKey {
            pool: DexPoolCostKey::UniswapV4(B256::repeat_byte(0x33)),
            token_in: Address::repeat_byte(0x44),
        };
        telemetry.publish_receipt(v3_route, 101, 99, 7, 10);
        telemetry.publish_receipt(v4_route, 202, 88, 6, 11);

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
                .receipt_at_or_before(v3_route, u64::MAX)
                .unwrap()
                .sample
                .gas_used,
            101
        );
        assert_eq!(
            snapshot
                .receipt_at_or_before(v4_route, u64::MAX)
                .unwrap()
                .sample
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
    fn conversion_history_covers_bursts_without_lookahead() {
        let telemetry = PreTradeCostTelemetry::default();
        for timestamp in 1..=NATIVE_CONVERSION_HISTORY_DEPTH as u64 {
            telemetry.publish_native_conversion(timestamp, Decimal::from(timestamp));
        }

        let snapshot = telemetry.snapshot().unwrap();
        assert_eq!(
            snapshot
                .native_conversion_at_or_before(1)
                .unwrap()
                .captured_unix_us,
            1
        );
        assert!(snapshot.native_conversion_at_or_before(0).is_none());
    }

    #[test]
    fn route_receipt_wins_over_protocol_bootstrap() {
        let telemetry = PreTradeCostTelemetry::default();
        let route = DexRouteCostKey {
            pool: DexPoolCostKey::UniswapV3(Address::repeat_byte(0x55)),
            token_in: Address::repeat_byte(0x66),
        };
        telemetry.publish_protocol_receipt_with_source(
            DexProtocol::UniswapV3,
            250,
            10,
            1,
            Some(9),
            Some(1),
            ReceiptCostTelemetrySource::JournalBootstrap,
        );
        telemetry.publish_receipt(route, 125, 9, 1, 10);

        let selected = telemetry
            .snapshot()
            .unwrap()
            .receipt_at_or_before(route, u64::MAX)
            .unwrap();
        assert_eq!(selected.match_scope, ReceiptCostMatchScope::ExactRoute);
        assert_eq!(selected.sample.gas_used, 125);
    }

    #[test]
    fn protocol_bootstrap_is_an_explicit_route_fallback() {
        let telemetry = PreTradeCostTelemetry::default();
        let route = DexRouteCostKey {
            pool: DexPoolCostKey::UniswapV4(B256::repeat_byte(0x77)),
            token_in: Address::repeat_byte(0x88),
        };
        telemetry.publish_protocol_receipt_with_source(
            DexProtocol::UniswapV4,
            222,
            11,
            0,
            Some(12),
            Some(2),
            ReceiptCostTelemetrySource::JournalBootstrap,
        );

        let selected = telemetry
            .snapshot()
            .unwrap()
            .receipt_at_or_before(route, u64::MAX)
            .unwrap();
        assert_eq!(
            selected.match_scope,
            ReceiptCostMatchScope::SameProtocolBootstrap
        );
        assert_eq!(selected.sample.gas_used, 222);
    }

    #[test]
    fn disabled_telemetry_never_produces_a_snapshot() {
        let telemetry = PreTradeCostTelemetry::disabled();
        telemetry.publish_native_conversion(10, Decimal::new(3_500, 0));
        assert!(telemetry.snapshot().is_none());
    }
}

use std::{path::PathBuf, str::FromStr, time::Duration};

use anyhow::{Context, ensure};
use rust_decimal::Decimal;

use crate::{
    binance::{
        account::BinanceAccountClient,
        execution::{
            BinanceExecutionService, BinanceOrderOutcome, BinanceOrderRequest,
            BinanceOrderRequestKind,
        },
        user_data::UserDataStream,
        ws_api::BinanceWsApiClient,
    },
    config::AppConfig,
    domain::config::{LoadedDomainConfig, PairConfig},
    market_data::{MarketEvent, binance::BookTickerFeed},
    state::TopOfBook,
};

pub const LIVE_CONFIRMATION: &str = "I_UNDERSTAND_BINANCE_LIVE_10_USDC";
pub const MAX_QUOTE_USDC: Decimal = Decimal::TEN;
const MAX_PRICE_DEVIATION_BPS: u16 = 50;
const BALANCE_ATTEMPTS: usize = 20;
const BALANCE_DELAY: Duration = Duration::from_millis(200);

#[derive(Clone, Copy, Debug)]
struct BinanceFilters {
    step: Decimal,
    tick: Decimal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinanceCanaryKind {
    Limit,
    Market,
}

impl BinanceCanaryKind {
    pub fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "limit" => Ok(Self::Limit),
            "market" => Ok(Self::Market),
            _ => anyhow::bail!("--order-type must be limit or market"),
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Limit => "limit",
            Self::Market => "market",
        }
    }
}

#[derive(Clone, Debug)]
pub struct BinanceBalancePoint {
    pub wld: Decimal,
    pub usdc: Decimal,
}

#[derive(Clone, Debug)]
pub struct BinanceCanaryOutcome {
    pub kind: BinanceCanaryKind,
    pub before: BinanceBalancePoint,
    pub after: BinanceBalancePoint,
    pub buy: BinanceOrderOutcome,
    pub sell: BinanceOrderOutcome,
    pub fallback_sell: Option<BinanceOrderOutcome>,
    pub wld_received: Decimal,
    pub wld_sell_quantity: Decimal,
}

pub async fn execute_order_round_trip(
    config: &AppConfig,
    domain: &LoadedDomainConfig,
    kind: BinanceCanaryKind,
    quote_usdc: Decimal,
    price_deviation_bps: u16,
    journal_path: PathBuf,
    live_confirmation: &str,
) -> anyhow::Result<BinanceCanaryOutcome> {
    ensure!(
        live_confirmation == LIVE_CONFIRMATION,
        "live Binance canary requires the exact confirmation phrase"
    );
    ensure!(
        quote_usdc > Decimal::ZERO && quote_usdc <= MAX_QUOTE_USDC,
        "Binance canary quote amount must be between 0 and 10 USDC"
    );
    ensure!(
        price_deviation_bps <= MAX_PRICE_DEVIATION_BPS,
        "Binance canary price deviation exceeds 50 bps"
    );
    ensure!(
        domain.snapshot().pairs.len() == 1,
        "Binance canary requires exactly one configured pair"
    );
    let pair = &domain.snapshot().pairs[0];
    ensure!(
        pair.binance.symbol == "WLDUSDC"
            && pair.binance.base_asset == "WLD"
            && pair.binance.quote_asset == "USDC",
        "Binance canary is restricted to WLDUSDC"
    );
    let step = Decimal::from_str(&pair.binance.step_size)
        .context("configured Binance step size is invalid")?;
    let tick = Decimal::from_str(&pair.binance.tick_size)
        .context("configured Binance tick size is invalid")?;
    let filters = BinanceFilters { step, tick };

    let mut account = BinanceAccountClient::from_env(config)?;
    let state = account.hydrate(&pair.binance.symbol).await?;
    ensure!(
        state.account.account_type == "SPOT" && state.account.can_trade,
        "Binance account is not a trade-enabled Spot account"
    );
    let before = balance_point(&state.account)?;
    ensure!(
        before.usdc >= quote_usdc,
        "Binance USDC balance is below the canary cap"
    );
    ensure_zero_locked(&state.account, "WLD")?;
    ensure_zero_locked(&state.account, "USDC")?;

    let mut preflight = BinanceWsApiClient::connect(config).await?;
    let open_orders = preflight.open_orders(&pair.binance.symbol).await?;
    ensure!(
        open_orders.is_empty(),
        "Binance account has open WLDUSDC orders; canary refused"
    );
    let clock_offset_ms = preflight.clock_offset_ms();
    drop(preflight);

    let user_data = UserDataStream::connect(config, clock_offset_ms).await?;
    let service = BinanceExecutionService::spawn(user_data.api(), journal_path, 4).await?;
    let run_id = format!("rustval{}{}", unix_timestamp_ms()?, kind_suffix(kind));
    let book = fresh_book(config, &pair.binance.symbol).await?;
    let buy_request = buy_request(
        pair,
        kind,
        &run_id,
        quote_usdc,
        price_deviation_bps,
        filters,
        &book,
    )?;
    let buy = service.execute(buy_request).await?;
    ensure_buy_executed(kind, &buy)?;

    let after_buy = wait_for_wld_increase(&mut account, &before).await?;
    let wld_received = after_buy.wld - before.wld;
    let wld_sell_quantity = round_down(wld_received, step);
    ensure!(
        wld_sell_quantity > Decimal::ZERO,
        "received WLD is below one Binance step"
    );

    let sell_book = fresh_book(config, &pair.binance.symbol).await?;
    ensure!(
        sell_book.bid_quantity >= wld_sell_quantity,
        "top Binance bid cannot absorb the capped recovery quantity"
    );
    let sell_request = sell_request(
        pair,
        kind,
        &run_id,
        wld_sell_quantity,
        price_deviation_bps,
        tick,
        &sell_book,
    )?;
    let sell = service.execute(sell_request).await?;
    let mut sold = sell.order.executed_qty;
    let fallback_sell = if sold < wld_sell_quantity {
        let remaining = round_down(wld_sell_quantity - sold, step);
        if remaining > Decimal::ZERO {
            let recovery_book = fresh_book(config, &pair.binance.symbol).await?;
            ensure!(
                recovery_book.bid_quantity >= remaining,
                "top Binance bid cannot absorb LIMIT recovery remainder"
            );
            let fallback = service
                .execute(BinanceOrderRequest {
                    operation_id: format!("{run_id}-fallback-sell"),
                    client_order_id: format!("{run_id}fs"),
                    symbol: pair.binance.symbol.clone(),
                    kind: BinanceOrderRequestKind::MarketSell {
                        quantity: remaining,
                    },
                })
                .await?;
            ensure!(
                fallback.order.status == "FILLED",
                "Binance fallback MARKET sell did not fill"
            );
            sold += fallback.order.executed_qty;
            Some(fallback)
        } else {
            None
        }
    } else {
        None
    };
    ensure!(
        sold >= wld_sell_quantity,
        "Binance canary did not unwind the sellable WLD delta"
    );

    let after = wait_for_wld_unwind(&mut account, &before, step).await?;
    let residual = after.wld - before.wld;
    ensure!(
        residual < step,
        "Binance canary left a sellable WLD residual"
    );
    tracing::info!(
        order_type = kind.label(),
        buy_order_id = buy.order.order_id,
        buy_client_order_id = %buy.order.client_order_id,
        buy_status = %buy.order.status,
        buy_executed_base = %buy.order.executed_qty,
        buy_executed_quote = %buy.order.cummulative_quote_qty,
        sell_order_id = sell.order.order_id,
        sell_client_order_id = %sell.order.client_order_id,
        sell_status = %sell.order.status,
        sell_executed_base = %sell.order.executed_qty,
        sell_executed_quote = %sell.order.cummulative_quote_qty,
        fallback_order_id = fallback_sell.as_ref().map(|order| order.order.order_id),
        wld_before = %before.wld,
        wld_after = %after.wld,
        usdc_before = %before.usdc,
        usdc_after = %after.usdc,
        "Binance capped live order canary completed"
    );
    Ok(BinanceCanaryOutcome {
        kind,
        before,
        after,
        buy,
        sell,
        fallback_sell,
        wld_received,
        wld_sell_quantity,
    })
}

fn buy_request(
    pair: &PairConfig,
    kind: BinanceCanaryKind,
    run_id: &str,
    quote_usdc: Decimal,
    deviation_bps: u16,
    filters: BinanceFilters,
    book: &TopOfBook,
) -> anyhow::Result<BinanceOrderRequest> {
    let kind = match kind {
        BinanceCanaryKind::Market => BinanceOrderRequestKind::MarketBuy {
            quote_quantity: quote_usdc,
        },
        BinanceCanaryKind::Limit => {
            let price = round_up(apply_bps_up(book.ask_price, deviation_bps)?, filters.tick);
            let quantity = round_down(quote_usdc / price, filters.step);
            ensure!(quantity > Decimal::ZERO, "LIMIT buy rounds to zero");
            ensure!(
                quantity * price <= quote_usdc,
                "LIMIT buy exceeds the 10 USDC cap"
            );
            BinanceOrderRequestKind::LimitIoc {
                side: "BUY".to_owned(),
                quantity,
                price,
            }
        }
    };
    Ok(BinanceOrderRequest {
        operation_id: format!("{run_id}-buy"),
        client_order_id: format!("{run_id}b"),
        symbol: pair.binance.symbol.clone(),
        kind,
    })
}

fn sell_request(
    pair: &PairConfig,
    kind: BinanceCanaryKind,
    run_id: &str,
    quantity: Decimal,
    deviation_bps: u16,
    tick: Decimal,
    book: &TopOfBook,
) -> anyhow::Result<BinanceOrderRequest> {
    let kind = match kind {
        BinanceCanaryKind::Market => BinanceOrderRequestKind::MarketSell { quantity },
        BinanceCanaryKind::Limit => BinanceOrderRequestKind::LimitIoc {
            side: "SELL".to_owned(),
            quantity,
            price: round_down(apply_bps_down(book.bid_price, deviation_bps)?, tick),
        },
    };
    Ok(BinanceOrderRequest {
        operation_id: format!("{run_id}-sell"),
        client_order_id: format!("{run_id}s"),
        symbol: pair.binance.symbol.clone(),
        kind,
    })
}

fn ensure_buy_executed(
    kind: BinanceCanaryKind,
    outcome: &BinanceOrderOutcome,
) -> anyhow::Result<()> {
    ensure!(
        outcome.order.executed_qty > Decimal::ZERO,
        "Binance BUY completed without an execution"
    );
    if kind == BinanceCanaryKind::Market {
        ensure!(
            outcome.order.status == "FILLED",
            "Binance MARKET buy did not fill"
        );
    } else {
        ensure!(
            matches!(outcome.order.status.as_str(), "FILLED" | "EXPIRED"),
            "Binance IOC buy returned an unexpected status"
        );
    }
    Ok(())
}

async fn fresh_book(config: &AppConfig, symbol: &str) -> anyhow::Result<TopOfBook> {
    let mut feed = BookTickerFeed::new(config, symbol.to_owned());
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match feed.next_event().await {
                MarketEvent::BinanceTopOfBook(book) => return Ok(book),
                MarketEvent::FeedConnected { .. } => {}
                MarketEvent::FeedDisconnected { reason, .. } => {
                    anyhow::bail!("Binance bookTicker disconnected: {reason}")
                }
                MarketEvent::FeedHeartbeat { .. } => {}
                MarketEvent::BinanceDepthApplied { .. } => {}
            }
        }
    })
    .await
    .context("timed out waiting for Binance bookTicker")?
}

async fn wait_for_wld_increase(
    account: &mut BinanceAccountClient,
    before: &BinanceBalancePoint,
) -> anyhow::Result<BinanceBalancePoint> {
    wait_for_balance(account, |current| current.wld > before.wld).await
}

async fn wait_for_wld_unwind(
    account: &mut BinanceAccountClient,
    before: &BinanceBalancePoint,
    step: Decimal,
) -> anyhow::Result<BinanceBalancePoint> {
    wait_for_balance(account, |current| current.wld - before.wld < step).await
}

async fn wait_for_balance(
    account: &mut BinanceAccountClient,
    predicate: impl Fn(&BinanceBalancePoint) -> bool,
) -> anyhow::Result<BinanceBalancePoint> {
    let mut last = None;
    for _ in 0..BALANCE_ATTEMPTS {
        let state = account.account_information().await?;
        let point = balance_point(&state)?;
        if predicate(&point) {
            return Ok(point);
        }
        last = Some(point);
        tokio::time::sleep(BALANCE_DELAY).await;
    }
    let last = last.context("Binance balance polling returned no snapshot")?;
    anyhow::bail!(
        "Binance balances did not reach the expected state; last WLD={}, USDC={}",
        last.wld,
        last.usdc
    )
}

fn balance_point(
    account: &crate::binance::account::AccountInformation,
) -> anyhow::Result<BinanceBalancePoint> {
    let wld = account
        .balances
        .iter()
        .find(|balance| balance.asset == "WLD")
        .context("Binance account has no WLD balance")?;
    let usdc = account
        .balances
        .iter()
        .find(|balance| balance.asset == "USDC")
        .context("Binance account has no USDC balance")?;
    Ok(BinanceBalancePoint {
        wld: wld.free,
        usdc: usdc.free,
    })
}

fn ensure_zero_locked(
    account: &crate::binance::account::AccountInformation,
    asset: &str,
) -> anyhow::Result<()> {
    let balance = account
        .balances
        .iter()
        .find(|balance| balance.asset == asset)
        .with_context(|| format!("Binance account has no {asset} balance"))?;
    ensure!(
        balance.locked.is_zero(),
        "Binance {asset} balance is locked by another operation"
    );
    Ok(())
}

fn apply_bps_up(value: Decimal, bps: u16) -> anyhow::Result<Decimal> {
    value
        .checked_mul(Decimal::from(10_000_u32 + u32::from(bps)))
        .and_then(|value| value.checked_div(Decimal::from(10_000_u32)))
        .context("Binance protected BUY price overflow")
}

fn apply_bps_down(value: Decimal, bps: u16) -> anyhow::Result<Decimal> {
    value
        .checked_mul(Decimal::from(10_000_u32 - u32::from(bps)))
        .and_then(|value| value.checked_div(Decimal::from(10_000_u32)))
        .context("Binance protected SELL price overflow")
}

fn round_down(value: Decimal, increment: Decimal) -> Decimal {
    (value / increment).floor() * increment
}

fn round_up(value: Decimal, increment: Decimal) -> Decimal {
    (value / increment).ceil() * increment
}

fn kind_suffix(kind: BinanceCanaryKind) -> &'static str {
    match kind {
        BinanceCanaryKind::Limit => "l",
        BinanceCanaryKind::Market => "m",
    }
}

fn unix_timestamp_ms() -> anyhow::Result<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system time is before Unix epoch")?
        .as_millis()
        .try_into()
        .context("Unix timestamp does not fit u64")
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::{apply_bps_down, apply_bps_up, round_down, round_up};

    #[test]
    fn rounds_prices_and_quantities_conservatively() {
        let tick = Decimal::new(1, 3);
        let step = Decimal::new(1, 1);
        assert_eq!(round_up(Decimal::new(38181, 5), tick), Decimal::new(382, 3));
        assert_eq!(
            round_down(Decimal::new(26179, 3), step),
            Decimal::new(261, 1)
        );
        assert_eq!(
            apply_bps_up(Decimal::new(38, 2), 50).unwrap(),
            Decimal::new(3819, 4)
        );
        assert_eq!(
            apply_bps_down(Decimal::new(38, 2), 50).unwrap(),
            Decimal::new(3781, 4)
        );
    }
}

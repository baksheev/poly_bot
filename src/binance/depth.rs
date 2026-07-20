use std::{collections::BTreeMap, str::FromStr};

use anyhow::{Context, ensure};
use rust_decimal::Decimal;
use serde::Deserialize;

#[derive(Clone, Debug, PartialEq)]
pub struct DepthLevel {
    pub price: Decimal,
    pub quantity: Decimal,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DepthSnapshot {
    pub last_update_id: u64,
    pub bids: Vec<DepthLevel>,
    pub asks: Vec<DepthLevel>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DepthUpdate {
    pub symbol: String,
    pub first_update_id: u64,
    pub final_update_id: u64,
    pub bids: Vec<DepthLevel>,
    pub asks: Vec<DepthLevel>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DepthApplyResult {
    Applied,
    Stale,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DepthExecutionQuote {
    pub base_quantity: Decimal,
    pub quote_quantity: Decimal,
    pub worst_price: Decimal,
    pub levels_consumed: usize,
}

/// Sequence-consistent Binance Spot depth owned by the trading state task.
#[derive(Clone, Debug)]
pub struct SpotDepthBook {
    symbol: String,
    last_update_id: u64,
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
}

impl SpotDepthBook {
    pub fn from_snapshot(symbol: String, snapshot: DepthSnapshot) -> anyhow::Result<Self> {
        validate_symbol(&symbol)?;
        ensure!(
            snapshot.last_update_id > 0,
            "depth snapshot update ID is zero"
        );
        let mut book = Self {
            symbol,
            last_update_id: snapshot.last_update_id,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        };
        replace_levels(&mut book.bids, snapshot.bids)?;
        replace_levels(&mut book.asks, snapshot.asks)?;
        book.validate_top()?;
        Ok(book)
    }

    pub fn last_update_id(&self) -> u64 {
        self.last_update_id
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    pub fn matches_top(
        &self,
        symbol: &str,
        update_id: u64,
        bid_price: Decimal,
        bid_quantity: Decimal,
        ask_price: Decimal,
        ask_quantity: Decimal,
    ) -> bool {
        self.symbol == symbol
            && self.last_update_id >= update_id
            && self
                .best_bid()
                .is_some_and(|bid| bid.price == bid_price && bid.quantity == bid_quantity)
            && self
                .best_ask()
                .is_some_and(|ask| ask.price == ask_price && ask.quantity == ask_quantity)
    }

    pub fn from_top(
        symbol: String,
        update_id: u64,
        bid_price: Decimal,
        bid_quantity: Decimal,
        ask_price: Decimal,
        ask_quantity: Decimal,
    ) -> anyhow::Result<Self> {
        Self::from_snapshot(
            symbol,
            DepthSnapshot {
                last_update_id: update_id,
                bids: vec![DepthLevel {
                    price: bid_price,
                    quantity: bid_quantity,
                }],
                asks: vec![DepthLevel {
                    price: ask_price,
                    quantity: ask_quantity,
                }],
            },
        )
    }

    /// Replaces the possibly stale top of a recent sequence-consistent book
    /// with the current bookTicker top. Better stale levels are removed while
    /// deeper levels are retained, so recovery quotes never assume liquidity
    /// ahead of the observed top.
    pub fn reconciled_with_top(
        &self,
        update_id: u64,
        bid_price: Decimal,
        bid_quantity: Decimal,
        ask_price: Decimal,
        ask_quantity: Decimal,
    ) -> anyhow::Result<Self> {
        let mut book = self.clone();
        book.bids.retain(|price, _| *price <= bid_price);
        book.asks.retain(|price, _| *price >= ask_price);
        book.bids.insert(bid_price, bid_quantity);
        book.asks.insert(ask_price, ask_quantity);
        book.last_update_id = update_id;
        book.validate_top()?;
        Ok(book)
    }

    pub fn best_bid(&self) -> Option<DepthLevel> {
        self.bids
            .last_key_value()
            .map(|(price, quantity)| DepthLevel {
                price: *price,
                quantity: *quantity,
            })
    }

    pub fn best_ask(&self) -> Option<DepthLevel> {
        self.asks
            .first_key_value()
            .map(|(price, quantity)| DepthLevel {
                price: *price,
                quantity: *quantity,
            })
    }

    pub fn apply(&mut self, update: DepthUpdate) -> anyhow::Result<DepthApplyResult> {
        ensure!(update.symbol == self.symbol, "depth update symbol mismatch");
        ensure!(
            update.first_update_id <= update.final_update_id,
            "depth update ID range is reversed"
        );
        if update.final_update_id <= self.last_update_id {
            return Ok(DepthApplyResult::Stale);
        }
        let expected = self
            .last_update_id
            .checked_add(1)
            .context("depth update ID overflow")?;
        ensure!(
            update.first_update_id <= expected && update.final_update_id >= expected,
            "Binance depth gap: expected {expected}, received {}..{}",
            update.first_update_id,
            update.final_update_id
        );
        apply_levels(&mut self.bids, update.bids)?;
        apply_levels(&mut self.asks, update.asks)?;
        self.last_update_id = update.final_update_id;
        self.validate_top()?;
        Ok(DepthApplyResult::Applied)
    }

    pub fn quote_market_sell(
        &self,
        base_quantity: Decimal,
    ) -> anyhow::Result<Option<DepthExecutionQuote>> {
        quote_levels(self.bids.iter().rev(), base_quantity)
    }

    pub fn quote_market_buy(
        &self,
        base_quantity: Decimal,
    ) -> anyhow::Result<Option<DepthExecutionQuote>> {
        quote_levels(self.asks.iter(), base_quantity)
    }

    fn validate_top(&self) -> anyhow::Result<()> {
        let bid = self.best_bid().context("Binance depth has no bids")?;
        let ask = self.best_ask().context("Binance depth has no asks")?;
        ensure!(bid.price < ask.price, "Binance depth book is crossed");
        Ok(())
    }
}

pub fn parse_depth_snapshot(payload: &[u8]) -> anyhow::Result<DepthSnapshot> {
    let wire: WireDepthSnapshot =
        serde_json::from_slice(payload).context("invalid Binance depth snapshot JSON")?;
    Ok(DepthSnapshot {
        last_update_id: wire.last_update_id,
        bids: parse_levels(wire.bids)?,
        asks: parse_levels(wire.asks)?,
    })
}

pub fn parse_depth_update(payload: &[u8], expected_symbol: &str) -> anyhow::Result<DepthUpdate> {
    validate_symbol(expected_symbol)?;
    let wire: WireDepthUpdate<'_> =
        serde_json::from_slice(payload).context("invalid Binance depth update JSON")?;
    ensure!(
        wire.event_type == "depthUpdate",
        "unexpected Binance depth event"
    );
    ensure!(
        wire.symbol == expected_symbol,
        "depth update symbol mismatch"
    );
    Ok(DepthUpdate {
        symbol: wire.symbol.to_owned(),
        first_update_id: wire.first_update_id,
        final_update_id: wire.final_update_id,
        bids: parse_levels(wire.bids)?,
        asks: parse_levels(wire.asks)?,
    })
}

fn quote_levels<'a>(
    levels: impl Iterator<Item = (&'a Decimal, &'a Decimal)>,
    base_quantity: Decimal,
) -> anyhow::Result<Option<DepthExecutionQuote>> {
    ensure!(
        base_quantity > Decimal::ZERO,
        "base quantity must be positive"
    );
    let mut remaining = base_quantity;
    let mut quote_quantity = Decimal::ZERO;
    let mut levels_consumed = 0_usize;
    for (price, available) in levels {
        let taken = remaining.min(*available);
        quote_quantity = quote_quantity
            .checked_add(
                taken
                    .checked_mul(*price)
                    .context("depth quote multiplication overflow")?,
            )
            .context("depth quote sum overflow")?;
        remaining = remaining
            .checked_sub(taken)
            .context("depth quote remaining quantity underflow")?;
        levels_consumed = levels_consumed.saturating_add(1);
        if remaining.is_zero() {
            return Ok(Some(DepthExecutionQuote {
                base_quantity,
                quote_quantity,
                worst_price: *price,
                levels_consumed,
            }));
        }
    }
    Ok(None)
}

fn replace_levels(
    destination: &mut BTreeMap<Decimal, Decimal>,
    levels: Vec<DepthLevel>,
) -> anyhow::Result<()> {
    destination.clear();
    for level in levels {
        validate_level(&level, false)?;
        ensure!(
            destination.insert(level.price, level.quantity).is_none(),
            "duplicate depth snapshot price"
        );
    }
    Ok(())
}

fn apply_levels(
    destination: &mut BTreeMap<Decimal, Decimal>,
    levels: Vec<DepthLevel>,
) -> anyhow::Result<()> {
    for level in levels {
        validate_level(&level, true)?;
        if level.quantity.is_zero() {
            destination.remove(&level.price);
        } else {
            destination.insert(level.price, level.quantity);
        }
    }
    Ok(())
}

fn validate_level(level: &DepthLevel, allow_zero_quantity: bool) -> anyhow::Result<()> {
    ensure!(level.price > Decimal::ZERO, "depth price must be positive");
    if allow_zero_quantity {
        ensure!(
            level.quantity >= Decimal::ZERO,
            "depth quantity must not be negative"
        );
    } else {
        ensure!(
            level.quantity > Decimal::ZERO,
            "snapshot depth quantity must be positive"
        );
    }
    Ok(())
}

fn parse_levels(levels: Vec<[String; 2]>) -> anyhow::Result<Vec<DepthLevel>> {
    levels
        .into_iter()
        .map(|[price, quantity]| {
            Ok(DepthLevel {
                price: Decimal::from_str(&price).context("invalid Binance depth price")?,
                quantity: Decimal::from_str(&quantity).context("invalid Binance depth quantity")?,
            })
        })
        .collect()
}

fn validate_symbol(symbol: &str) -> anyhow::Result<()> {
    ensure!(
        !symbol.is_empty()
            && symbol
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit()),
        "Binance depth symbol is invalid"
    );
    Ok(())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireDepthSnapshot {
    last_update_id: u64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

#[derive(Deserialize)]
struct WireDepthUpdate<'a> {
    #[serde(rename = "e")]
    event_type: &'a str,
    #[serde(rename = "s")]
    symbol: &'a str,
    #[serde(rename = "U")]
    first_update_id: u64,
    #[serde(rename = "u")]
    final_update_id: u64,
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rust_decimal::Decimal;

    use super::{DepthApplyResult, SpotDepthBook, parse_depth_snapshot, parse_depth_update};

    fn snapshot() -> SpotDepthBook {
        SpotDepthBook::from_snapshot(
            "WLDUSDC".to_owned(),
            parse_depth_snapshot(
                br#"{"lastUpdateId":100,"bids":[["1.00","2"],["0.99","5"]],"asks":[["1.01","3"],["1.02","6"]]}"#,
            )
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn recent_depth_reconciliation_replaces_the_top_and_keeps_deeper_levels() {
        let book = snapshot();
        let reconciled = book
            .reconciled_with_top(
                12,
                Decimal::from_str("1.00").unwrap(),
                Decimal::ONE,
                Decimal::from_str("1.01").unwrap(),
                Decimal::ONE,
            )
            .unwrap();

        assert!(reconciled.matches_top(
            "WLDUSDC",
            12,
            Decimal::from_str("1.00").unwrap(),
            Decimal::ONE,
            Decimal::from_str("1.01").unwrap(),
            Decimal::ONE,
        ));
        assert_eq!(
            reconciled
                .quote_market_sell(Decimal::from(3))
                .unwrap()
                .unwrap()
                .worst_price,
            Decimal::from_str("0.99").unwrap()
        );
        assert_eq!(
            reconciled
                .quote_market_buy(Decimal::from(4))
                .unwrap()
                .unwrap()
                .worst_price,
            Decimal::from_str("1.02").unwrap()
        );
    }

    #[test]
    fn recent_depth_reconciliation_removes_levels_better_than_the_current_top() {
        let book = SpotDepthBook::from_snapshot(
            "WLDUSDC".to_owned(),
            parse_depth_snapshot(
                br#"{"lastUpdateId":100,"bids":[["1.00","9"],["0.99","5"],["0.98","6"]],"asks":[["1.01","9"],["1.02","5"],["1.03","6"]]}"#,
            )
            .unwrap(),
        )
        .unwrap();
        let reconciled = book
            .reconciled_with_top(
                102,
                Decimal::from_str("0.99").unwrap(),
                Decimal::ONE,
                Decimal::from_str("1.02").unwrap(),
                Decimal::ONE,
            )
            .unwrap();

        assert_eq!(
            reconciled.best_bid().unwrap().price,
            Decimal::from_str("0.99").unwrap()
        );
        assert_eq!(
            reconciled.best_ask().unwrap().price,
            Decimal::from_str("1.02").unwrap()
        );
        assert_eq!(
            reconciled
                .quote_market_sell(Decimal::from(2))
                .unwrap()
                .unwrap()
                .worst_price,
            Decimal::from_str("0.98").unwrap()
        );
        assert_eq!(
            reconciled
                .quote_market_buy(Decimal::from(2))
                .unwrap()
                .unwrap()
                .worst_price,
            Decimal::from_str("1.03").unwrap()
        );
    }

    #[test]
    fn applies_overlapping_sequence_and_removes_zero_level() {
        let mut book = snapshot();
        let update = parse_depth_update(
            br#"{"e":"depthUpdate","s":"WLDUSDC","U":100,"u":102,"b":[["1.00","0"],["0.98","8"]],"a":[["1.01","4"]]}"#,
            "WLDUSDC",
        )
        .unwrap();
        assert_eq!(book.apply(update).unwrap(), DepthApplyResult::Applied);
        assert_eq!(book.last_update_id(), 102);
        assert_eq!(book.best_bid().unwrap().price, Decimal::new(99, 2));
        assert_eq!(book.best_ask().unwrap().quantity, Decimal::new(4, 0));
        assert!(book.matches_top(
            "WLDUSDC",
            102,
            Decimal::new(99, 2),
            Decimal::new(5, 0),
            Decimal::new(101, 2),
            Decimal::new(4, 0),
        ));
        assert!(!book.matches_top(
            "WLDUSDC",
            103,
            Decimal::new(99, 2),
            Decimal::new(5, 0),
            Decimal::new(101, 2),
            Decimal::new(4, 0),
        ));
    }

    #[test]
    fn rejects_sequence_gap_without_mutating_book() {
        let mut book = snapshot();
        let update = parse_depth_update(
            br#"{"e":"depthUpdate","s":"WLDUSDC","U":102,"u":103,"b":[],"a":[]}"#,
            "WLDUSDC",
        )
        .unwrap();
        assert!(book.apply(update).is_err());
        assert_eq!(book.last_update_id(), 100);
    }

    #[test]
    fn quotes_full_market_quantity_across_levels_or_fails_closed() {
        let book = snapshot();
        let sell = book.quote_market_sell(Decimal::new(4, 0)).unwrap().unwrap();
        assert_eq!(sell.quote_quantity, Decimal::new(398, 2));
        assert_eq!(sell.worst_price, Decimal::new(99, 2));
        assert_eq!(sell.levels_consumed, 2);

        let buy = book.quote_market_buy(Decimal::new(8, 0)).unwrap().unwrap();
        assert_eq!(buy.quote_quantity, Decimal::new(813, 2));
        assert!(
            book.quote_market_buy(Decimal::new(10, 0))
                .unwrap()
                .is_none()
        );
    }
}

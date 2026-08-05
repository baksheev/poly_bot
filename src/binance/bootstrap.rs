use std::path::PathBuf;

use anyhow::{Context, ensure};
use rust_decimal::Decimal;

use crate::{
    binance::{
        account::BinanceAccountClient,
        capital::select_capital_routes,
        execution::{
            BinanceExecutionService, BinanceOrderOutcome, BinanceOrderRequest,
            BinanceOrderRequestKind,
        },
        order_journal::BinanceOrderJournalScope,
        user_data::UserDataStream,
    },
    config::AppConfig,
};

pub const ARB_BOOTSTRAP_CONFIRMATION: &str = "BOOTSTRAP_ARB_WITH_500_USDC";
pub const ARB_BOOTSTRAP_QUOTE_USDC: Decimal = Decimal::from_parts(500, 0, 0, false, 0);
const SYMBOL: &str = "ARBUSDC";
const OPERATION_ID: &str = "rustarb-arb-bootstrap-v1";
const CLIENT_ORDER_ID: &str = "rustarbarbbootstrapv1";

pub async fn bootstrap_arb_inventory(
    config: &AppConfig,
    quote_usdc: Decimal,
    journal_path: PathBuf,
    confirmation: &str,
) -> anyhow::Result<BinanceOrderOutcome> {
    ensure!(
        confirmation == ARB_BOOTSTRAP_CONFIRMATION,
        "ARB inventory bootstrap requires the exact confirmation phrase"
    );
    ensure!(
        quote_usdc == ARB_BOOTSTRAP_QUOTE_USDC,
        "ARB inventory bootstrap is pinned to exactly 500 USDC"
    );

    let mut account = BinanceAccountClient::from_env(config)?;
    let state = account.hydrate(SYMBOL).await?;
    ensure!(
        state.account.account_type == "SPOT" && state.account.can_trade,
        "Binance account is not a trade-enabled Spot account"
    );
    ensure!(
        state.symbol_rules.status == "TRADING",
        "ARBUSDC is not trading"
    );
    ensure!(
        state.symbol_rules.base_asset == "ARB" && state.symbol_rules.quote_asset == "USDC",
        "ARBUSDC live asset identity differs from the bootstrap artifact"
    );
    ensure!(
        state.open_orders.is_empty(),
        "Binance account has open ARBUSDC orders; bootstrap refused"
    );
    let usdc = state
        .account
        .balances
        .iter()
        .find(|balance| balance.asset == "USDC")
        .context("Binance account has no USDC balance")?;
    let arb = state
        .account
        .balances
        .iter()
        .find(|balance| balance.asset == "ARB")
        .context("Binance account has no ARB balance")?;
    ensure!(
        usdc.locked.is_zero() && arb.locked.is_zero(),
        "ARB bootstrap assets are locked by another operation"
    );
    ensure!(
        usdc.free >= quote_usdc,
        "Binance free USDC balance is below the 500 USDC bootstrap"
    );
    let coins = account.all_coin_information().await?;
    for asset in ["USDC", "ARB"] {
        let capital = select_capital_routes(&coins, asset, "ARBITRUM", "OPTIMISM")?;
        let direct = capital
            .direct
            .as_ref()
            .filter(|route| route.network == "ARBITRUM")
            .with_context(|| format!("{asset} has no direct Arbitrum capital route"))?;
        ensure!(
            capital.deposit_all_enabled
                && capital.withdrawal_all_enabled
                && direct.deposit_available()
                && direct.withdrawal_available(),
            "{asset} direct Arbitrum capital route is not fully available"
        );
    }

    let user_data = UserDataStream::connect(config, state.clock_offset_ms).await?;
    let service = BinanceExecutionService::spawn_scoped(
        user_data.api(),
        journal_path,
        1,
        BinanceOrderJournalScope {
            schema_version: BinanceOrderJournalScope::SCHEMA_VERSION,
            account_id: "binance-spot:primary".to_owned(),
            strategy_id: "strategy:arbitrum-usdc-arb".to_owned(),
        },
    )
    .await?;
    let outcome = service
        .execute(BinanceOrderRequest {
            operation_id: OPERATION_ID.to_owned(),
            client_order_id: CLIENT_ORDER_ID.to_owned(),
            symbol: SYMBOL.to_owned(),
            kind: BinanceOrderRequestKind::MarketBuy {
                quote_quantity: quote_usdc,
            },
            latency_origin: None,
        })
        .await?;
    ensure!(
        outcome.order.status == "FILLED"
            && outcome.order.executed_qty > Decimal::ZERO
            && outcome.order.cummulative_quote_qty > Decimal::ZERO
            && outcome.order.cummulative_quote_qty <= quote_usdc,
        "ARB inventory bootstrap did not produce one bounded terminal fill"
    );
    tracing::info!(
        operation_id = OPERATION_ID,
        client_order_id = CLIENT_ORDER_ID,
        order_id = outcome.order.order_id,
        executed_arb = %outcome.order.executed_qty,
        spent_usdc = %outcome.order.cummulative_quote_qty,
        reconciled_after_unknown = outcome.reconciled_after_unknown,
        "ARB Binance inventory bootstrap completed"
    );
    Ok(outcome)
}

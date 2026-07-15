use std::time::Duration;

use anyhow::bail;
use arb_bot::{
    config::{self, Cli, Command},
    engine::TradingEngine,
    market_data::binance::spawn_book_ticker_connectors,
    telemetry::TelemetryWriter,
};
use clap::Parser;
use tokio::time::MissedTickBehavior;
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    init_tracing();

    let cli = Cli::parse();
    cli.config.validate()?;

    match cli.command {
        Command::Run => run(cli.config).await,
        Command::Migrate => TelemetryWriter::new(&cli.config).migrate().await,
        Command::Check => {
            tracing::info!(
                service = %cli.config.service_name,
                engine_id = %cli.config.engine_id,
                gcp_project_id = %cli.config.gcp_project_id,
                gcp_region = %cli.config.gcp_region,
                binance_symbols = ?cli.config.normalized_binance_symbols(),
                telemetry_enabled = cli.config.clickhouse_enabled(),
                "configuration is valid"
            );
            Ok(())
        }
    }
}

async fn run(config: config::AppConfig) -> anyhow::Result<()> {
    let (telemetry, writer) = TelemetryWriter::new(&config).channel();
    let writer_task = tokio::spawn(writer.run());
    let (market_sender, mut market_receiver) =
        tokio::sync::mpsc::channel(config.market_event_channel_capacity);
    let connector_tasks = spawn_book_ticker_connectors(&config, market_sender.clone());
    drop(market_sender);

    let mut engine = TradingEngine::new(config.clone(), telemetry);
    engine.start();

    tracing::info!(
        service = %config.service_name,
        engine_id = %config.engine_id,
        gcp_project_id = %config.gcp_project_id,
        gcp_region = %config.gcp_region,
        binance_symbols = ?config.normalized_binance_symbols(),
        clickhouse_enabled = config.clickhouse_enabled(),
        "read-only arbitrage shadow service started"
    );

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    let health_interval =
        Duration::from_millis((config.market_data_max_age_ms / 4).clamp(100, 1_000));
    let mut health_tick = tokio::time::interval(health_interval);
    health_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            _ = health_tick.tick() => engine.refresh_health(),
            event = market_receiver.recv() => {
                let Some(event) = event else {
                    bail!("all Binance market-data connector tasks stopped unexpectedly");
                };
                engine.on_market_event(event);
            }
        }
    }

    engine.shutdown();
    for task in connector_tasks {
        task.abort();
        let _ = task.await;
    }
    drop(engine);

    writer_task.await??;
    tracing::info!("read-only arbitrage shadow service stopped");
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt()
        .with_env_filter(filter)
        .json()
        .with_current_span(false)
        .init();
}

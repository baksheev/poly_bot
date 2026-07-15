use clap::Parser;
use poly_bot::{
    config::{self, Cli, Command},
    engine::TradingEngine,
    telemetry::TelemetryWriter,
};
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
                gcp_project_id = %cli.config.gcp_project_id,
                gcp_region = %cli.config.gcp_region,
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
    let mut engine = TradingEngine::new(config.clone(), telemetry);

    tracing::info!(
        service = %config.service_name,
        gcp_project_id = %config.gcp_project_id,
        gcp_region = %config.gcp_region,
        clickhouse_enabled = config.clickhouse_enabled(),
        "trading service started"
    );

    engine.mark_ready();
    tokio::signal::ctrl_c().await?;
    engine.shutdown();
    drop(engine);

    writer_task.await??;
    tracing::info!("trading service stopped");
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

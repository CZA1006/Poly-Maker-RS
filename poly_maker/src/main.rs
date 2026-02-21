use anyhow::Result;
use tracing_subscriber::EnvFilter;

mod config;
mod execution;
mod gamma;
mod strategy;
mod ws_market;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    let config = config::Config::from_env()?;
    println!("host={}, funder={}", config.clob_host, config.funder);
    if let Some(budget) = config.total_budget_usdc {
        println!("total_budget_usdc={}", budget);
    }
    println!(
        "live_mode={}, execution_mode={}, execution_adapter={}, user_ws_enabled={}, settlement_enabled={}",
        config.live_mode,
        config.execution_mode,
        config.execution_adapter,
        config.user_ws_enabled,
        config.settlement_enabled
    );
    println!("boot ok");
    ws_market::run(&config).await?;
    Ok(())
}

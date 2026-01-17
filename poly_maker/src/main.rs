use anyhow::Result;
use tracing_subscriber::EnvFilter;

mod config;
mod gamma;
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
    println!("boot ok");
    ws_market::run(&config).await?;
    Ok(())
}

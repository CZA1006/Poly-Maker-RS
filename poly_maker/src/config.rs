use anyhow::{bail, Context, Result};
use std::env;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Config {
    pub clob_host: String,
    pub ws_host: String,
    pub market_slug: Option<String>,
    pub pk: String,
    pub clob_api_key: String,
    pub clob_api_secret: String,
    pub clob_api_passphrase: String,
    pub funder: String,
    pub chain_id: u64,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let clob_host = get_required("CLOB_HOST")?;
        let ws_host = get_required("WS_HOST")?;
        let market_slug = env::var("MARKET_SLUG")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let pk = get_required("PK")?;
        let clob_api_key = get_required("CLOB_API_KEY")?;
        let clob_api_secret = get_required("CLOB_API_SECRET")?;
        let clob_api_passphrase = get_required("CLOB_API_PASSPHRASE")?;
        let funder = get_required("FUNDER")?;
        let chain_id = get_required("CHAIN_ID")?
            .parse::<u64>()
            .context("CHAIN_ID must be a valid u64")?;

        Ok(Self {
            clob_host,
            ws_host,
            market_slug,
            pk,
            clob_api_key,
            clob_api_secret,
            clob_api_passphrase,
            funder,
            chain_id,
        })
    }
}

fn get_required(key: &str) -> Result<String> {
    let value = env::var(key).with_context(|| format!("missing env var {key}"))?;
    if value.trim().is_empty() {
        bail!("env var {key} is empty");
    }
    Ok(value)
}

use anyhow::{bail, Context, Result};
use std::env;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Config {
    pub clob_host: String,
    pub ws_host: String,
    pub market_slug: Option<String>,
    pub total_budget_usdc: Option<f64>,
    pub live_mode: bool,
    pub execution_mode: String,
    pub execution_adapter: String,
    pub user_ws_enabled: bool,
    pub settlement_enabled: bool,
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
        let total_budget_usdc = env::var("TOTAL_BUDGET_USDC")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value > 0.0);
        let total_budget_usdc = total_budget_usdc.or_else(|| {
            env::var("MAX_NET_INVEST_USDC")
                .ok()
                .and_then(|value| value.parse::<f64>().ok())
                .filter(|value| value.is_finite() && *value > 0.0)
        });
        let live_mode_env = read_bool_env("LIVE_MODE", false);
        let execution_mode = env::var("EXECUTION_MODE")
            .ok()
            .map(|value| value.trim().to_ascii_lowercase())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| {
                if live_mode_env {
                    "live_shadow".to_string()
                } else {
                    "paper".to_string()
                }
            });
        if !matches!(execution_mode.as_str(), "paper" | "live_shadow" | "live") {
            bail!(
                "invalid EXECUTION_MODE={}; expected one of: paper, live_shadow, live",
                execution_mode
            );
        }
        let live_mode = live_mode_env || execution_mode != "paper";
        let execution_adapter = env::var("EXECUTION_ADAPTER")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "paper".to_string());
        let user_ws_enabled = read_bool_env("USER_WS_ENABLED", false);
        let settlement_enabled = read_bool_env("SETTLEMENT_ENABLED", false);
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
            total_budget_usdc,
            live_mode,
            execution_mode,
            execution_adapter,
            user_ws_enabled,
            settlement_enabled,
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

fn read_bool_env(key: &str, default: bool) -> bool {
    match env::var(key) {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => default,
        },
        Err(_) => default,
    }
}

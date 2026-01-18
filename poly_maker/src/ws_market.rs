use anyhow::{bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use chrono::Utc;
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::env;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::config::Config;
use crate::gamma;

#[derive(Debug, Clone, Default)]
struct BestQuote {
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    last_update_ts_ms: i64,
    source: QuoteSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuoteSource {
    Book,
    PriceChange,
    BestBidAsk,
}

impl Default for QuoteSource {
    fn default() -> Self {
        QuoteSource::Book
    }
}

#[derive(Debug, Clone)]
struct TickSizeCacheEntry {
    tick_size: f64,
    updated_at_ms: u64,
    source: &'static str,
}

#[derive(Debug, Default)]
struct QuoteHealth {
    symmetric_seconds: u32,
    symmetric_warned: bool,
    last_extreme_warn_ms: u64,
    last_stale_warn_ms: HashMap<String, i64>,
}

#[derive(Debug, Clone)]
struct AssetSelection {
    asset_ids: Vec<String>,
    gamma_tick: Option<f64>,
    market_slug: Option<String>,
    condition_id: Option<String>,
    event_slug: Option<String>,
    score: Option<i64>,
    #[allow(dead_code)]
    event_start_time: Option<String>,
    #[allow(dead_code)]
    start_time: Option<String>,
    #[allow(dead_code)]
    end_date: Option<String>,
}

#[derive(Debug, Default)]
struct PriceChangeStats {
    msgs_total: u64,
    entries_total: u64,
    best_updates: u64,
    last_log_ms: u64,
}

#[derive(Debug, Default)]
struct QuoteUpdateStats {
    applied_book: u64,
    applied_price_change: u64,
    applied_best_bid_ask: u64,
    ignored_book_extreme: u64,
    ignored_book_lower_priority: u64,
    last_log_ms: i64,
}

#[derive(Debug, Default)]
struct GammaLogThrottle {
    last_key: Option<String>,
    last_log_ms: i64,
    printed_total: u64,
    suppressed_total: u64,
}

impl GammaLogThrottle {
    fn should_log(&mut self, key: &str, now_ms: i64, throttle_ms: i64) -> bool {
        if let Some(last) = self.last_key.as_deref() {
            if last == key && now_ms - self.last_log_ms < throttle_ms {
                self.suppressed_total += 1;
                return false;
            }
        }
        self.last_key = Some(key.to_string());
        self.last_log_ms = now_ms;
        self.printed_total += 1;
        true
    }
}

#[derive(Debug)]
struct AwaitingFirstPacket {
    generation: u64,
    pending: HashSet<String>,
    start_ms: i64,
    total: usize,
    new_slug: Option<String>,
    new_ids_hash: String,
    final_event_type: Option<String>,
}

#[derive(Debug, Clone)]
struct RolloverLogContext {
    enabled: bool,
    verbose: bool,
    series_slug: String,
    rollover_gen: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TradeLeg {
    Up,
    Down,
}

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
#[derive(PartialEq, Eq)]
enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy)]
#[derive(PartialEq, Eq)]
enum TradeKind {
    Maker,
    Taker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DryRunMode {
    Recommend,
    Paper,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ApplyKind {
    BestAction,
    BestMakerOnly,
    BestTakerOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoundPhase {
    Idle,
    Leg1Filled,
    Done,
}

#[derive(Debug, Clone, Copy)]
enum DenyReason {
    TakerDisabled,
    MarginTarget,
    NoImprove,
    Cooldown,
    NoQuote,
    TotalBudgetCap,
    RoundBudgetCap,
    ReserveForPair,
    LegCapValueUp,
    LegCapValueDown,
    LegCapSharesUp,
    LegCapSharesDown,
    TailFreeze,
    TailClose,
    LockedMaxRounds,
    LockedTailFreeze,
    LockedWaitingPair,
    LockedPolicyHold,
}
#[derive(Debug, Clone, Copy)]
struct CandidateAction {
    name: &'static str,
    leg: TradeLeg,
    side: TradeSide,
    kind: TradeKind,
    qty: f64,
}

#[derive(Debug, Clone)]
struct SimResult {
    ok: bool,
    #[allow(dead_code)]
    fill_qty: f64,
    fill_price: Option<f64>,
    #[allow(dead_code)]
    fee_estimate: f64,
    spent_delta_usdc: f64,
    new_qty_up: f64,
    new_cost_up: f64,
    new_qty_down: f64,
    new_cost_down: f64,
    #[allow(dead_code)]
    new_avg_up: Option<f64>,
    #[allow(dead_code)]
    new_avg_down: Option<f64>,
    new_pair_cost: Option<f64>,
    new_hedgeable: f64,
    new_unhedged_up: f64,
    new_unhedged_down: f64,
    new_unhedged_value_up: f64,
    new_unhedged_value_down: f64,
    improves_hedge: bool,
}

#[derive(Debug, Clone)]
struct GateResult {
    allow: bool,
    deny_reason: Option<DenyReason>,
    can_start_new_round: bool,
    budget_remaining_round: f64,
    budget_remaining_total: f64,
    reserve_needed_usdc: Option<f64>,
}

#[derive(Debug, Clone)]
struct DryRunParams {
    improve_min: f64,
    margin_target: f64,
    safety_margin: f64,
    total_budget_usdc: f64,
    max_rounds: u64,
    round_budget_usdc: f64,
    round_leg1_fraction: f64,
    max_unhedged_value: f64,
    cap_unhedged_value: Option<f64>,
    max_unhedged_shares: Option<f64>,
    cooldown_secs: u64,
    tail_freeze_secs: u64,
    tail_close_secs: u64,
    decision_every_ms: u64,
    mode: DryRunMode,
    apply_kind: ApplyKind,
    pair_bonus: f64,
}

#[derive(Debug, Clone)]
struct DryRunLedger {
    market_slug: String,
    #[allow(dead_code)]
    series_slug: String,
    market_select_mode: String,
    up_id: String,
    down_id: String,
    #[allow(dead_code)]
    tick_size: f64,
    tick_size_current: f64,
    tick_size_source: String,
    #[allow(dead_code)]
    start_ts: u64,
    end_ts: u64,
    qty_up: f64,
    cost_up: f64,
    qty_down: f64,
    cost_down: f64,
    spent_total_usdc: f64,
    spent_round_usdc: f64,
    round_idx: u64,
    round_state: RoundPhase,
    round_leg1: Option<TradeLeg>,
    round_qty_target: f64,
    best_bid_up: Option<f64>,
    best_ask_up: Option<f64>,
    best_bid_down: Option<f64>,
    best_ask_down: Option<f64>,
    last_decision_ts_ms: u64,
    decision_seq: u64,
    locked: bool,
    #[allow(dead_code)]
    locked_hedgeable: f64,
    #[allow(dead_code)]
    locked_pair_cost: Option<f64>,
    #[allow(dead_code)]
    locked_at_ts_ms: u64,
}

#[derive(Debug, Clone)]
struct RoundPlan {
    phase: RoundPhase,
    planned_leg1: Option<TradeLeg>,
    qty_target: Option<f64>,
    balance_leg: Option<TradeLeg>,
    balance_qty: Option<f64>,
    can_start_new_round: bool,
    budget_remaining_round: f64,
    budget_remaining_total: f64,
    reserve_needed_usdc: Option<f64>,
}

pub async fn run(config: &Config) -> Result<()> {
    let ws_host = get_required("WS_HOST")?;
    let ws_path = get_required("WS_PATH")?;
    let ws_url = format!("{ws_host}{ws_path}");
    tracing::info!(ws_url = %ws_url, "ws url");
    let clob_host = config.clob_host.clone();
    let print_raw_ws = read_print_raw_ws();
    let ws_debug_dump_once = read_ws_debug_dump_once();
    let ws_custom_features = read_ws_custom_features();
    let rollover_log_json = read_rollover_log_json();
    let rollover_log_verbose = read_rollover_log_verbose();
    let market_slug = config
        .market_slug
        .as_ref()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let series_slug = read_series_slug()?;
    let market_select_mode = if market_slug.is_some() {
        "fixed_market_slug".to_string()
    } else if series_slug.is_some() {
        "series_latest".to_string()
    } else {
        "env_asset_ids".to_string()
    };
    if let Some(ref slug) = market_slug {
        tracing::info!(market_slug = %slug, "using market_slug");
        if series_slug.is_some() {
            tracing::info!(series_slug = ?series_slug, "MARKET_SLUG set; ignoring SERIES_SLUG");
        }
    } else {
        tracing::info!(series_slug = ?series_slug, "using series_slug");
    }
    let mut backoff_secs = 1u64;
    let series_slug_value = series_slug.clone().unwrap_or_default();
    let mut dryrun: Option<DryRunLedger> = None;

    loop {
        let (selection, initial_gamma_stats) = if let Some(ref slug) = market_slug {
            let result = load_asset_ids_from_market_slug(slug).await;
            let (selection, stats) = match result {
                Ok(result) => result,
                Err(err) => {
                    log_fatal_error("gamma_fetch_market_by_slug", &err.to_string(), slug);
                    return Err(err);
                }
            };
            (selection, Some(stats))
        } else if let Some(ref slug) = series_slug {
            let (selection, stats) = load_asset_ids_from_gamma(slug).await?;
            (selection, Some(stats))
        } else {
            let asset_ids = load_asset_ids_from_env()?;
            tracing::info!(
                n = asset_ids.len(),
                ids = %asset_ids.join(","),
                "using assets_ids"
            );
            (
                AssetSelection {
                    asset_ids,
                    gamma_tick: None,
                    market_slug: None,
                    condition_id: None,
                    event_slug: None,
                    score: None,
                    event_start_time: None,
                    start_time: None,
                    end_date: None,
                },
                None,
            )
        };
        let allow_rollover = market_slug.is_none() && series_slug.is_some();
        let selection_slug = selection.market_slug.clone().unwrap_or_default();

        match dryrun.as_ref() {
            Some(ledger) if ledger.market_slug == selection_slug => {
                tracing::info!(
                    market_slug = %ledger.market_slug,
                    round_idx = ledger.round_idx,
                    qty_up = ledger.qty_up,
                    qty_down = ledger.qty_down,
                    spent_total_usdc = ledger.spent_total_usdc,
                    "dryrun_reused"
                );
            }
            Some(ledger) => {
                tracing::info!(
                    old_slug = %ledger.market_slug,
                    new_slug = %selection_slug,
                    reason = "market_slug_changed",
                    "dryrun_reset"
                );
                dryrun = Some(build_dryrun_ledger(
                    &series_slug_value,
                    &selection,
                    &market_select_mode,
                ));
            }
            None => {
                tracing::info!(
                    new_slug = %selection_slug,
                    reason = "init",
                    "dryrun_reset"
                );
                dryrun = Some(build_dryrun_ledger(
                    &series_slug_value,
                    &selection,
                    &market_select_mode,
                ));
            }
        }

        let attempt = connect_and_run(
            &ws_url,
            &selection,
            dryrun
                .as_mut()
                .expect("dryrun ledger should be initialized"),
            initial_gamma_stats,
            if allow_rollover {
                series_slug.as_deref()
            } else {
                None
            },
            allow_rollover,
            &market_select_mode,
            print_raw_ws,
            ws_debug_dump_once,
            ws_custom_features,
            rollover_log_json,
            rollover_log_verbose,
            &clob_host,
        )
        .await;
        if let Err(err) = attempt {
            tracing::warn!(error = %err, "ws connection ended with error");
        } else {
            tracing::info!("ws connection ended");
        }

        let wait_secs = backoff_secs.min(30);
        tracing::info!(wait_secs, "reconnect scheduled");
        tokio::time::sleep(Duration::from_secs(wait_secs)).await;
        backoff_secs = (backoff_secs * 2).min(30);
    }
}

async fn connect_and_run(
    ws_url: &str,
    selection: &AssetSelection,
    dryrun: &mut DryRunLedger,
    initial_gamma_stats: Option<gamma::GammaPickStats>,
    series_slug: Option<&str>,
    refresh_on_empty: bool,
    market_select_mode: &str,
    print_raw_ws: bool,
    ws_debug_dump_once: bool,
    ws_custom_features: bool,
    rollover_log_json: bool,
    rollover_log_verbose: bool,
    clob_host: &str,
) -> Result<()> {
    let allow_future_secs = read_allow_future_secs();
    let allow_past_secs = read_allow_past_secs();
    let (ws_stream, _response) = match tokio_tungstenite::connect_async(ws_url).await {
        Ok(result) => result,
        Err(err) => {
            tracing::warn!(?err, "connect failed");
            log_error_chain(&err);
            return Err(err).context("failed to connect");
        }
    };
    tracing::info!(ws_url, "ws connected");

    let (mut write, mut read) = ws_stream.split();
    let (sender, mut receiver) = mpsc::unbounded_channel::<String>();
    let write_task = tokio::spawn(async move {
        while let Some(msg) = receiver.recv().await {
            if let Err(err) = write.send(Message::Text(msg)).await {
                tracing::warn!(error = %err, "ws write failed");
                break;
            }
        }
    });

    let mut current_selection = selection.clone();
    let initial_payload =
        build_initial_subscribe_payload(&current_selection.asset_ids, ws_custom_features);
    send_ws_message(&sender, initial_payload.clone())?;
    tracing::info!(
        payload = %initial_payload,
        assets_len = current_selection.asset_ids.len(),
        "subscription sent"
    );

    let mut cache: HashMap<String, BestQuote> = current_selection
        .asset_ids
        .iter()
        .cloned()
        .map(|id| (id, BestQuote::default()))
        .collect();
    let mut tick_cache: HashMap<String, TickSizeCacheEntry> = HashMap::new();
    let mut quote_health = QuoteHealth::default();
    let http_client = Client::new();
    let mut dumped_snapshot = false;
    let mut dumped_non_book = false;
    let mut rollover_generation: u64 = 0;
    let mut awaiting_first_packet: Option<AwaitingFirstPacket> = None;
    let mut rollover_in_progress = false;
    let dryrun_params = read_dryrun_params();
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    let mut price_change_stats = PriceChangeStats::default();
    let mut update_stats = QuoteUpdateStats::default();
    ticker.tick().await;
    let roll_poll_secs = read_roll_poll_secs();
    let mut roll_interval = tokio::time::interval(Duration::from_secs(roll_poll_secs));
    roll_interval.tick().await;
    let series_slug = series_slug.map(str::to_string);
    let has_series_slug = series_slug.is_some();
    let series_slug_value = series_slug.clone().unwrap_or_default();
    let gamma_log_throttle_ms = read_gamma_log_throttle_ms();
    let mut gamma_pick_throttle = GammaLogThrottle::default();
    let mut gamma_fastpath_throttle = GammaLogThrottle::default();

    if let Some(stats) = initial_gamma_stats.as_ref() {
        let log_ctx = RolloverLogContext {
            enabled: rollover_log_json,
            verbose: rollover_log_verbose,
            series_slug: series_slug_value.clone(),
            rollover_gen: rollover_generation,
        };
        let logged = log_gamma_pick_stats_json(
            &log_ctx,
            stats,
            selection,
            &mut gamma_pick_throttle,
            gamma_log_throttle_ms,
        );
        if stats.fastpath_hit {
            log_gamma_fastpath_hit_json(
                &log_ctx,
                stats,
                selection,
                &mut gamma_fastpath_throttle,
                gamma_log_throttle_ms,
            );
        }
        if logged {
            log_gamma_selected_market_json(&log_ctx, stats, selection);
        }
    }

    'ws_loop: loop {
        tokio::select! {
            _ = ticker.tick() => {
                let selection_snapshot = current_selection.clone();
                if let Err(err) = log_best_quotes(
                    &selection_snapshot.asset_ids,
                    selection_snapshot.gamma_tick,
                    &cache,
                    &mut tick_cache,
                    &http_client,
                    clob_host,
                    &mut quote_health,
                ).await {
                    tracing::warn!(error = %err, "failed to compute best quotes");
                }
                let now_ms = now_ts_ms();
                if should_run_dryrun(&dryrun, now_ms, dryrun_params.decision_every_ms) {
                    dryrun.last_decision_ts_ms = now_ms;
                    dryrun.decision_seq = dryrun.decision_seq.saturating_add(1);
                    dryrun.update_from_cache(&cache);
                    let now_ts = Utc::now().timestamp();
                    let now_ts = u64::try_from(now_ts).unwrap_or(0);
                    dryrun.update_round_state(&dryrun_params, now_ts);
                    dryrun.update_lock(&dryrun_params, now_ms);
                    let log_ctx = RolloverLogContext {
                        enabled: rollover_log_json,
                        verbose: rollover_log_verbose,
                        series_slug: series_slug_value.clone(),
                        rollover_gen: rollover_generation,
                    };
                    log_dryrun_snapshot(&dryrun, &dryrun_params, &log_ctx);
                    let candidates = build_dryrun_candidates(&dryrun, &dryrun_params);
                    let best_any = select_best_candidate(&candidates, |_| true);
                    let applied = if dryrun_params.mode == DryRunMode::Paper {
                        let _apply_kind = dryrun_params.apply_kind;
                        select_best_candidate(&candidates, |action| {
                            action.kind == TradeKind::Maker
                        })
                    } else {
                        None
                    };
                    log_dryrun_candidates(
                        &dryrun,
                        &candidates,
                        &best_any,
                        &applied,
                        &log_ctx,
                        &dryrun_params,
                    );
                    if let Some(applied) = applied {
                        if applied.gate.allow && applied.sim.ok {
                            log_dryrun_apply(&dryrun, &applied, &dryrun_params, &log_ctx);
                            apply_simulated_trade(
                                dryrun,
                                &applied.action,
                                &applied.sim,
                                &dryrun_params,
                                now_ts,
                            );
                        }
                    }
                }
            }
            _ = roll_interval.tick(), if has_series_slug => {
                if let Some(ref slug) = series_slug {
                    match gamma::pick_latest_open_market_by_series(slug).await {
                        Ok(selected) => {
                            let stats = gamma::take_last_pick_stats();
                            let now = Utc::now().timestamp();
                            let start_ts = selected
                                .event_start_time
                                .as_deref()
                                .or(selected.start_time.as_deref())
                                .and_then(parse_iso_to_unix_secs);
                            let end_ts = selected.end_date.as_deref().and_then(parse_iso_to_unix_secs);
                            if let Some(start_ts) = start_ts {
                                if start_ts > now + allow_future_secs {
                                    tracing::warn!(
                                        slug = %selected.slug,
                                        eventStartTime = ?selected.event_start_time,
                                        startTime = ?selected.start_time,
                                        endDate = ?selected.end_date,
                                        now,
                                        "market is future-dated; still subscribing (gamma-selected)"
                                    );
                                }
                            }
                            if let Some(end_ts) = end_ts {
                                if end_ts < now - allow_past_secs {
                                    tracing::warn!(
                                        slug = %selected.slug,
                                        eventStartTime = ?selected.event_start_time,
                                        startTime = ?selected.start_time,
                                        endDate = ?selected.end_date,
                                        now,
                                        "market is stale-dated; still subscribing (gamma-selected)"
                                    );
                                }
                            }
                            let next_selection = AssetSelection {
                                asset_ids: selected.token_ids.to_vec(),
                                gamma_tick: selected.tick_size,
                                market_slug: Some(selected.slug),
                                condition_id: Some(selected.condition_id),
                                event_slug: selected.event_slug,
                                score: selected.score,
                                event_start_time: selected.event_start_time,
                                start_time: selected.start_time,
                                end_date: selected.end_date,
                            };
                            let will_rollover = should_rollover(&current_selection, &next_selection);
                            let log_gen = if will_rollover {
                                rollover_generation.wrapping_add(1)
                            } else {
                                rollover_generation
                            };
                            let log_ctx = RolloverLogContext {
                                enabled: rollover_log_json,
                                verbose: rollover_log_verbose,
                                series_slug: series_slug_value.clone(),
                                rollover_gen: log_gen,
                            };
                            let logged = log_gamma_pick_stats_json(
                                &log_ctx,
                                &stats,
                                &next_selection,
                                &mut gamma_pick_throttle,
                                gamma_log_throttle_ms,
                            );
                            if stats.fastpath_hit {
                                log_gamma_fastpath_hit_json(
                                    &log_ctx,
                                    &stats,
                                    &next_selection,
                                    &mut gamma_fastpath_throttle,
                                    gamma_log_throttle_ms,
                                );
                            }
                            if logged {
                                log_gamma_selected_market_json(&log_ctx, &stats, &next_selection);
                            }
                            if will_rollover {
                                if rollover_in_progress {
                                    tracing::debug!("rollover already in progress; skip poll tick");
                                    continue;
                                }
                                rollover_in_progress = true;
                                rollover_generation = log_gen;
                                let old_slug = current_selection.market_slug.clone();
                                let old_ids = current_selection.asset_ids.clone();
                                let old_condition = current_selection.condition_id.clone();
                                let new_condition = next_selection.condition_id.clone();
                                let reason = rollover_reason(&current_selection, &next_selection);
                                let now_ms = now_ts_ms_i64();
                                let old_ids_hash = hash_ids(&old_ids);
                                let new_ids_hash = hash_ids(&next_selection.asset_ids);
                                let mut old_ids_log = old_ids.clone();
                                let mut new_ids_log = next_selection.asset_ids.clone();
                                old_ids_log.sort();
                                new_ids_log.sort();
                                let event_start_ts = start_ts
                                    .and_then(|value| u64::try_from(value).ok());
                                let event_end_ts =
                                    end_ts.and_then(|value| u64::try_from(value).ok());
                                let now_ts = u64::try_from(now).unwrap_or(0);
                                let allow_future_secs_u64 =
                                    u64::try_from(allow_future_secs).unwrap_or(0);
                                let allow_past_secs_u64 =
                                    u64::try_from(allow_past_secs).unwrap_or(0);
                                let score = next_selection.score;
                                let mut data = serde_json::Map::new();
                                data.insert("reason".to_string(), Value::String(reason));
                                data.insert("old_slug".to_string(), to_value_string(old_slug.clone()));
                                data.insert(
                                    "new_slug".to_string(),
                                    to_value_string(next_selection.market_slug.clone()),
                                );
                                data.insert(
                                    "old_condition_id".to_string(),
                                    to_value_string(old_condition.clone()),
                                );
                                data.insert(
                                    "new_condition_id".to_string(),
                                    to_value_string(new_condition.clone()),
                                );
                                data.insert("old_ids".to_string(), to_value_strings(&old_ids_log));
                                data.insert(
                                    "new_ids".to_string(),
                                    to_value_strings(&new_ids_log),
                                );
                                data.insert("old_ids_hash".to_string(), Value::String(old_ids_hash));
                                data.insert("new_ids_hash".to_string(), Value::String(new_ids_hash.clone()));
                                data.insert(
                                    "tick_size".to_string(),
                                    next_selection
                                        .gamma_tick
                                        .map(Value::from)
                                        .unwrap_or(Value::Null),
                                );
                                data.insert(
                                    "custom_feature_enabled".to_string(),
                                    Value::Bool(ws_custom_features),
                                );
                                data.insert(
                                    "allow_future_secs".to_string(),
                                    Value::Number(serde_json::Number::from(allow_future_secs_u64)),
                                );
                                data.insert(
                                    "allow_past_secs".to_string(),
                                    Value::Number(serde_json::Number::from(allow_past_secs_u64)),
                                );
                                data.insert(
                                    "now_ts".to_string(),
                                    Value::Number(serde_json::Number::from(now_ts)),
                                );
                                data.insert(
                                    "event_start_ts".to_string(),
                                    event_start_ts
                                        .map(|value| Value::Number(serde_json::Number::from(value)))
                                        .unwrap_or(Value::Null),
                                );
                                data.insert(
                                    "event_end_ts".to_string(),
                                    event_end_ts
                                        .map(|value| Value::Number(serde_json::Number::from(value)))
                                        .unwrap_or(Value::Null),
                                );
                                data.insert(
                                    "event_slug".to_string(),
                                    to_value_string(next_selection.event_slug.clone()),
                                );
                                data.insert(
                                    "condition_id".to_string(),
                                    to_value_string(next_selection.condition_id.clone()),
                                );
                                data.insert(
                                    "score".to_string(),
                                    score
                                        .map(Value::from)
                                        .unwrap_or(Value::Null),
                                );
                                log_jsonl(&log_ctx, "rollover_begin", data);

                                send_unsubscribe(&sender, &old_ids, ws_custom_features, &log_ctx)?;
                                send_subscribe(
                                    &sender,
                                    &next_selection.asset_ids,
                                    ws_custom_features,
                                    &log_ctx,
                                )?;
                                let old_market_slug = dryrun.market_slug.clone();
                                let new_market_slug =
                                    next_selection.market_slug.clone().unwrap_or_default();
                                let cleared_qty_up = dryrun.qty_up;
                                let cleared_cost_up = dryrun.cost_up;
                                let cleared_qty_down = dryrun.qty_down;
                                let cleared_cost_down = dryrun.cost_down;
                                current_selection = next_selection;
                                let mut reset_data = serde_json::Map::new();
                                reset_data.insert(
                                    "old_market_slug".to_string(),
                                    Value::String(old_market_slug),
                                );
                                reset_data.insert(
                                    "new_market_slug".to_string(),
                                    Value::String(new_market_slug),
                                );
                                reset_data.insert(
                                    "reset_reason".to_string(),
                                    Value::String("rollover".to_string()),
                                );
                                reset_data.insert(
                                    "cleared_qty_up".to_string(),
                                    Value::from(cleared_qty_up),
                                );
                                reset_data.insert(
                                    "cleared_cost_up".to_string(),
                                    Value::from(cleared_cost_up),
                                );
                                reset_data.insert(
                                    "cleared_qty_down".to_string(),
                                    Value::from(cleared_qty_down),
                                );
                                reset_data.insert(
                                    "cleared_cost_down".to_string(),
                                    Value::from(cleared_cost_down),
                                );
                                log_jsonl(&log_ctx, "dryrun_reset", reset_data);
                                *dryrun = build_dryrun_ledger(
                                    &series_slug_value,
                                    &current_selection,
                                    &market_select_mode,
                                );
                                let cleared_best_quotes = cache.len();
                                let cleared_tick_cache = tick_cache.len();
                                let cleared_pending_first_packet = awaiting_first_packet.is_some();
                                let cleared_seen_new_ids = awaiting_first_packet
                                    .as_ref()
                                    .map(|state| state.total.saturating_sub(state.pending.len()))
                                    .unwrap_or(0);
                                cache.clear();
                                for id in &current_selection.asset_ids {
                                    cache.insert(id.clone(), BestQuote::default());
                                }
                                tick_cache.clear();
                                quote_health = QuoteHealth::default();
                                dumped_snapshot = false;
                                dumped_non_book = false;
                                awaiting_first_packet = Some(AwaitingFirstPacket {
                                    generation: log_ctx.rollover_gen,
                                    pending: current_selection.asset_ids.iter().cloned().collect(),
                                    start_ms: now_ms,
                                    total: current_selection.asset_ids.len(),
                                    new_slug: current_selection.market_slug.clone(),
                                    new_ids_hash: new_ids_hash,
                                    final_event_type: None,
                                });
                                let mut data = serde_json::Map::new();
                                data.insert(
                                    "cleared_best_quotes".to_string(),
                                    Value::Number(serde_json::Number::from(cleared_best_quotes as u64)),
                                );
                                data.insert(
                                    "cleared_tick_cache".to_string(),
                                    Value::Number(serde_json::Number::from(cleared_tick_cache as u64)),
                                );
                                data.insert("cleared_quote_health".to_string(), Value::Bool(true));
                                data.insert(
                                    "cleared_pending_first_packet".to_string(),
                                    Value::Bool(cleared_pending_first_packet),
                                );
                                data.insert(
                                    "cleared_seen_new_ids".to_string(),
                                    Value::Number(serde_json::Number::from(cleared_seen_new_ids as u64)),
                                );
                                log_jsonl(&log_ctx, "rollover_cache_reset", data);
                            }
                        }
                        Err(err) => {
                            tracing::warn!(error = %err, "failed to refresh market from gamma");
                        }
                    }
                }
            }
            msg = read.next() => {
                let msg = match msg {
                    Some(msg) => msg,
                    None => break 'ws_loop,
                };
                let msg = msg.context("ws read error")?;
                match msg {
                    Message::Text(text) => {
                        if text.trim() == "[]" {
                            if refresh_on_empty {
                                tracing::warn!("empty snapshot, will refresh market from gamma");
                                bail!("empty snapshot");
                            } else {
                                tracing::warn!("ws text message is empty array");
                                continue;
                            }
                        }

                        if print_raw_ws {
                            let truncated = truncate_text(&text, 1000);
                            tracing::info!(text = %truncated, "ws raw text");
                        } else {
                            tracing::debug!(len = text.len(), "ws text message");
                        }

                        match handle_ws_text(
                            &text,
                            &mut cache,
                            &mut price_change_stats,
                            &mut update_stats,
                            dryrun,
                            &mut awaiting_first_packet,
                            &series_slug_value,
                            rollover_log_json,
                            rollover_log_verbose,
                            &mut rollover_in_progress,
                        ) {
                            Ok(kind) => {
                                if ws_debug_dump_once {
                                    match kind {
                                        WsMessageKind::BookSnapshot if !dumped_snapshot => {
                                            tracing::info!(raw_snapshot = %text, "RAW_SNAPSHOT_ONCE");
                                            dumped_snapshot = true;
                                        }
                                        WsMessageKind::NonBook if !dumped_non_book => {
                                            tracing::info!(raw = %text, "RAW_FIRST_NON_BOOK_ONCE");
                                            dumped_non_book = true;
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            Err(err) => {
                                tracing::warn!(error = %err, "failed to handle ws message");
                            }
                        }
                    }
                    Message::Binary(data) => {
                        tracing::info!(len = data.len(), "ws binary message");
                    }
                    Message::Ping(data) => {
                        tracing::info!(len = data.len(), "ws ping");
                    }
                    Message::Pong(data) => {
                        tracing::info!(len = data.len(), "ws pong");
                    }
                    Message::Close(frame) => {
                        if let Some(frame) = frame {
                            tracing::info!(
                                code = ?frame.code,
                                reason = %frame.reason,
                                "ws close"
                            );
                        } else {
                            tracing::info!("ws close");
                        }
                        break 'ws_loop;
                    }
                    Message::Frame(_) => {
                        tracing::info!("ws raw frame");
                    }
                }
            }
        }
    }

    drop(sender);
    let _ = write_task.await;
    bail!("ws stream closed")
}

fn get_required(key: &str) -> Result<String> {
    let value = env::var(key).with_context(|| format!("missing env var {key}"))?;
    if value.trim().is_empty() {
        bail!("env var {key} is empty");
    }
    Ok(value)
}

fn read_series_slug() -> Result<Option<String>> {
    match env::var("SERIES_SLUG") {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else if trimmed.contains("up-or-down-15m") {
                let corrected = trimmed.replace("up-or-down-15m", "updown-15m");
                tracing::warn!(
                    original = %trimmed,
                    corrected = %corrected,
                    "SERIES_SLUG normalized from up-or-down-15m to updown-15m"
                );
                Ok(Some(corrected))
            } else {
                Ok(Some(trimmed.to_string()))
            }
        }
        _ => Ok(None),
    }
}

async fn load_asset_ids_from_gamma(
    series_slug: &str,
) -> Result<(AssetSelection, gamma::GammaPickStats)> {
    let selected = gamma::pick_latest_open_market_by_series(series_slug).await?;
    let stats = gamma::take_last_pick_stats();
    Ok((
        AssetSelection {
            asset_ids: selected.token_ids.to_vec(),
            gamma_tick: selected.tick_size,
            market_slug: Some(selected.slug),
            condition_id: Some(selected.condition_id),
            event_slug: selected.event_slug,
            score: selected.score,
            event_start_time: selected.event_start_time,
            start_time: selected.start_time,
            end_date: selected.end_date,
        },
        stats,
    ))
}

async fn load_asset_ids_from_market_slug(
    market_slug: &str,
) -> Result<(AssetSelection, gamma::GammaPickStats)> {
    let selected = gamma::fetch_fixed_market_info_by_slug(market_slug).await?;
    let stats = gamma::take_last_pick_stats();
    tracing::info!(
        market_slug = %selected.slug,
        up_id = %selected.up_token_id,
        down_id = %selected.down_token_id,
        condition_id = %selected.condition_id,
        "resolved_fixed_market_tokens"
    );
    Ok((
        AssetSelection {
            asset_ids: vec![selected.up_token_id.clone(), selected.down_token_id.clone()],
            gamma_tick: selected.tick_size,
            market_slug: Some(selected.slug),
            condition_id: Some(selected.condition_id),
            event_slug: selected.event_slug,
            score: None,
            event_start_time: selected.event_start_time,
            start_time: selected.start_time,
            end_date: selected.end_date,
        },
        stats,
    ))
}

fn log_fatal_error(where_: &str, message: &str, market_slug: &str) {
    let mut data = serde_json::Map::new();
    data.insert("where".to_string(), Value::String(where_.to_string()));
    data.insert("message".to_string(), Value::String(message.to_string()));
    data.insert(
        "market_slug".to_string(),
        Value::String(market_slug.to_string()),
    );
    let mut envelope = serde_json::Map::new();
    envelope.insert("kind".to_string(), Value::String("fatal_error".to_string()));
    envelope.insert("data".to_string(), Value::Object(data));
    println!("{}", Value::Object(envelope).to_string());
}

fn load_asset_ids_from_env() -> Result<Vec<String>> {
    match env::var("TEST_TOKEN_IDS") {
        Ok(raw) if !raw.trim().is_empty() => {
            let ids: Vec<String> = raw
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect();
            if ids.is_empty() {
                bail!("env var TEST_TOKEN_IDS has no valid entries");
            }
            Ok(ids)
        }
        Ok(_) | Err(_) => {
            let id = get_required("TEST_TOKEN_ID")?;
            Ok(vec![id])
        }
    }
}

fn read_print_raw_ws() -> bool {
    match env::var("PRINT_RAW_WS") {
        Ok(value) => matches!(value.as_str(), "1" | "true" | "TRUE"),
        Err(_) => false,
    }
}

fn read_ws_debug_dump_once() -> bool {
    match env::var("WS_DEBUG_DUMP_ONCE") {
        Ok(value) => matches!(value.as_str(), "1" | "true" | "TRUE"),
        Err(_) => false,
    }
}

fn read_ws_custom_features() -> bool {
    match env::var("WS_CUSTOM_FEATURES") {
        Ok(value) => matches!(value.as_str(), "1" | "true" | "TRUE"),
        Err(_) => false,
    }
}

fn read_rollover_log_json() -> bool {
    match env::var("ROLLOVER_LOG_JSON") {
        Ok(value) => matches!(value.as_str(), "1" | "true" | "TRUE"),
        Err(_) => true,
    }
}

fn read_rollover_log_verbose() -> bool {
    match env::var("ROLLOVER_LOG_VERBOSE") {
        Ok(value) => matches!(value.as_str(), "1" | "true" | "TRUE"),
        Err(_) => false,
    }
}

fn read_allow_future_secs() -> i64 {
    match env::var("ALLOW_FUTURE_SECS") {
        Ok(value) => value.parse::<i64>().ok().filter(|v| *v > 0).unwrap_or(120),
        Err(_) => 120,
    }
}

fn read_allow_past_secs() -> i64 {
    match env::var("ALLOW_PAST_SECS") {
        Ok(value) => value.parse::<i64>().ok().filter(|v| *v > 0).unwrap_or(1800),
        Err(_) => 1800,
    }
}

fn read_roll_poll_secs() -> u64 {
    match env::var("ROLL_POLL_SECS") {
        Ok(value) => value.parse::<u64>().ok().filter(|v| *v > 0).unwrap_or(10),
        Err(_) => 10,
    }
}

fn read_gamma_log_throttle_ms() -> i64 {
    match env::var("GAMMA_LOG_THROTTLE_MS") {
        Ok(value) => value
            .parse::<i64>()
            .ok()
            .filter(|v| *v >= 0)
            .unwrap_or(30_000),
        Err(_) => 30_000,
    }
}

fn read_dryrun_params() -> DryRunParams {
    let improve_min = env::var("DRYRUN_IMPROVE_MIN")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| *v >= 0.0)
        .unwrap_or(0.002);
    let margin_target = env::var("DRYRUN_MARGIN_TARGET")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| *v >= 0.0)
        .unwrap_or(0.010);
    let safety_margin = env::var("DRYRUN_SAFETY_MARGIN")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| *v >= 0.0)
        .unwrap_or(0.005);
    let mut total_budget_usdc = env::var("TOTAL_BUDGET_USDC")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| v.is_finite() && *v > 0.0)
        .unwrap_or(10.0);
    let legacy_budget = env::var("MAX_NET_INV_USDC")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| v.is_finite() && *v > 0.0);
    if env::var("TOTAL_BUDGET_USDC").is_err() {
        if let Some(value) = legacy_budget {
            total_budget_usdc = value;
            tracing::warn!(
                total_budget_usdc = value,
                "MAX_NET_INV_USDC is deprecated; use TOTAL_BUDGET_USDC"
            );
        }
    }
    let max_rounds = env::var("MAX_ROUNDS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(2);
    let round_budget_usdc = env::var("ROUND_BUDGET_USDC")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| v.is_finite() && *v > 0.0)
        .unwrap_or_else(|| total_budget_usdc / max_rounds.max(1) as f64);
    let round_leg1_fraction = env::var("ROUND_LEG1_FRACTION")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| *v > 0.0 && *v < 1.0)
        .unwrap_or(0.45);
    let cap_unhedged_value = env::var("DRYRUN_MAX_UNHEDGED_VALUE")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| *v > 0.0);
    let max_unhedged_value = cap_unhedged_value.unwrap_or(50.0);
    let max_unhedged_shares = env::var("DRYRUN_MAX_UNHEDGED_SHARES")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| *v > 0.0);
    let cooldown_secs = env::var("DRYRUN_COOLDOWN_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(180);
    let tail_freeze_secs = env::var("DRYRUN_TAIL_FREEZE_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(300);
    let tail_close_secs = env::var("DRYRUN_TAIL_CLOSE_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(180);
    let decision_every_ms = env::var("DRYRUN_DECISION_EVERY_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1000);
    let mode = match env::var("DRYRUN_MODE").ok().as_deref() {
        Some("paper") | Some("PAPER") => DryRunMode::Paper,
        _ => DryRunMode::Recommend,
    };
    let apply_kind = match env::var("DRYRUN_APPLY_KIND").ok().as_deref() {
        Some("best_maker_only") => ApplyKind::BestMakerOnly,
        Some("best_taker_only") => ApplyKind::BestTakerOnly,
        _ => ApplyKind::BestAction,
    };
    let pair_bonus = env::var("DRYRUN_PAIR_BONUS")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| v.is_finite())
        .unwrap_or(0.001);
    DryRunParams {
        improve_min,
        margin_target,
        safety_margin,
        total_budget_usdc,
        max_rounds,
        round_budget_usdc,
        round_leg1_fraction,
        max_unhedged_value,
        cap_unhedged_value: Some(max_unhedged_value),
        max_unhedged_shares,
        cooldown_secs,
        tail_freeze_secs,
        tail_close_secs,
        decision_every_ms,
        mode,
        apply_kind,
        pair_bonus,
    }
}

fn send_ws_message(sender: &mpsc::UnboundedSender<String>, payload: String) -> Result<()> {
    if sender.send(payload).is_err() {
        bail!("ws write channel closed");
    }
    Ok(())
}

fn send_subscribe(
    sender: &mpsc::UnboundedSender<String>,
    asset_ids: &[String],
    custom_features: bool,
    log_ctx: &RolloverLogContext,
) -> Result<()> {
    let payload_value = build_subscribe_payload_value(asset_ids, custom_features);
    let payload = payload_value.to_string();
    send_ws_message(sender, payload.clone())?;
    let mut sorted_ids = asset_ids.to_vec();
    sorted_ids.sort();
    let mut data = serde_json::Map::new();
    data.insert("operation".to_string(), Value::String("subscribe".to_string()));
    data.insert(
        "assets_len".to_string(),
        Value::Number(serde_json::Number::from(sorted_ids.len() as u64)),
    );
    data.insert("assets_ids".to_string(), to_value_strings(&sorted_ids));
    data.insert(
        "custom_feature_enabled".to_string(),
        Value::Bool(custom_features),
    );
    data.insert("payload".to_string(), payload_value);
    log_jsonl(log_ctx, "rollover_subscribe_sent", data);
    Ok(())
}

fn send_unsubscribe(
    sender: &mpsc::UnboundedSender<String>,
    asset_ids: &[String],
    custom_features: bool,
    log_ctx: &RolloverLogContext,
) -> Result<()> {
    let payload_value = build_unsubscribe_payload_value(asset_ids, custom_features);
    let payload = payload_value.to_string();
    send_ws_message(sender, payload.clone())?;
    let mut sorted_ids = asset_ids.to_vec();
    sorted_ids.sort();
    let mut data = serde_json::Map::new();
    data.insert("operation".to_string(), Value::String("unsubscribe".to_string()));
    data.insert(
        "assets_len".to_string(),
        Value::Number(serde_json::Number::from(sorted_ids.len() as u64)),
    );
    data.insert("assets_ids".to_string(), to_value_strings(&sorted_ids));
    data.insert(
        "custom_feature_enabled".to_string(),
        Value::Bool(custom_features),
    );
    data.insert("payload".to_string(), payload_value);
    log_jsonl(log_ctx, "rollover_unsubscribe_sent", data);
    Ok(())
}

fn build_initial_subscribe_payload(asset_ids: &[String], custom_features: bool) -> String {
    build_initial_subscribe_payload_value(asset_ids, custom_features).to_string()
}

fn build_initial_subscribe_payload_value(
    asset_ids: &[String],
    custom_features: bool,
) -> Value {
    json!({
        "type": "MARKET",
        "assets_ids": asset_ids,
        "markets": [],
        "auth": null,
        "custom_feature_enabled": custom_features
    })
}

fn build_subscribe_payload_value(asset_ids: &[String], custom_features: bool) -> Value {
    json!({
        "type":"MARKET",
        "operation": "subscribe",
        "assets_ids": asset_ids,
        "markets": [],
        "auth": null,
        "custom_feature_enabled": custom_features
    })
}

fn build_unsubscribe_payload_value(asset_ids: &[String], custom_features: bool) -> Value {
    json!({
        "type":"MARKET",
        "operation": "unsubscribe",
        "assets_ids": asset_ids,
        "markets": [],
        "auth": null,
        "custom_feature_enabled": custom_features
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WsMessageKind {
    BookSnapshot,
    NonBook,
}

fn handle_ws_text(
    text: &str,
    cache: &mut HashMap<String, BestQuote>,
    stats: &mut PriceChangeStats,
    update_stats: &mut QuoteUpdateStats,
    dryrun: &mut DryRunLedger,
    awaiting_first_packet: &mut Option<AwaitingFirstPacket>,
    series_slug: &str,
    rollover_log_json: bool,
    rollover_log_verbose: bool,
    rollover_in_progress: &mut bool,
) -> Result<WsMessageKind> {
    let value: Value = serde_json::from_str(text).context("invalid ws json")?;
    let log_ctx = RolloverLogContext {
        enabled: rollover_log_json,
        verbose: rollover_log_verbose,
        series_slug: series_slug.to_string(),
        rollover_gen: awaiting_first_packet
            .as_ref()
            .map(|state| state.generation)
            .unwrap_or(0),
    };
    let mut saw_book = false;
    let mut saw_non_book = false;

    match value {
        Value::Array(items) => {
            for item in items {
                let kind = handle_ws_event(
                    &item,
                    cache,
                    stats,
                    update_stats,
                    dryrun,
                    awaiting_first_packet,
                    &log_ctx,
                    rollover_in_progress,
                )?;
                match kind {
                    WsMessageKind::BookSnapshot => saw_book = true,
                    WsMessageKind::NonBook => saw_non_book = true,
                }
            }
        }
        Value::Object(_) => {
            let kind = handle_ws_event(
                &value,
                cache,
                stats,
                update_stats,
                dryrun,
                awaiting_first_packet,
                &log_ctx,
                rollover_in_progress,
            )?;
            match kind {
                WsMessageKind::BookSnapshot => saw_book = true,
                WsMessageKind::NonBook => saw_non_book = true,
            }
        }
        _ => {
            tracing::debug!("ws message ignored");
            return Ok(WsMessageKind::NonBook);
        }
    }

    if saw_book {
        Ok(WsMessageKind::BookSnapshot)
    } else if saw_non_book {
        Ok(WsMessageKind::NonBook)
    } else {
        Ok(WsMessageKind::NonBook)
    }
}

fn handle_ws_event(
    value: &Value,
    cache: &mut HashMap<String, BestQuote>,
    stats: &mut PriceChangeStats,
    update_stats: &mut QuoteUpdateStats,
    dryrun: &mut DryRunLedger,
    awaiting_first_packet: &mut Option<AwaitingFirstPacket>,
    log_ctx: &RolloverLogContext,
    rollover_in_progress: &mut bool,
) -> Result<WsMessageKind> {
    let event_type = value.get("event_type").and_then(|v| v.as_str());
    match event_type {
        Some("book") => {
            apply_book_event(
                value,
                cache,
                update_stats,
                awaiting_first_packet,
                log_ctx,
                rollover_in_progress,
            );
            Ok(WsMessageKind::BookSnapshot)
        }
        Some("price_change") => {
            apply_price_change_event(
                value,
                cache,
                stats,
                update_stats,
                awaiting_first_packet,
                log_ctx,
                rollover_in_progress,
            );
            Ok(WsMessageKind::NonBook)
        }
        Some("best_bid_ask") => {
            apply_best_bid_ask_event(
                value,
                cache,
                update_stats,
                awaiting_first_packet,
                log_ctx,
                rollover_in_progress,
            );
            Ok(WsMessageKind::NonBook)
        }
        Some("tick_size_change") => {
            apply_tick_size_change_event(value, dryrun);
            Ok(WsMessageKind::NonBook)
        }
        _ => Ok(WsMessageKind::NonBook),
    }
}

fn apply_book_event(
    event: &Value,
    cache: &mut HashMap<String, BestQuote>,
    update_stats: &mut QuoteUpdateStats,
    awaiting_first_packet: &mut Option<AwaitingFirstPacket>,
    log_ctx: &RolloverLogContext,
    rollover_in_progress: &mut bool,
) {
    let asset_id = match event.get("asset_id").and_then(|v| v.as_str()) {
        Some(id) => id,
        None => return,
    };
    if !cache.contains_key(asset_id) {
        return;
    }
    let bids = event.get("bids").cloned().unwrap_or(Value::Null);
    let asks = event.get("asks").cloned().unwrap_or(Value::Null);
    let best_bid = best_bid_from_levels(&bids);
    let best_ask = best_ask_from_levels(&asks);
    let ts_ms = event.get("timestamp").and_then(parse_timestamp_ms);
    note_rollover_packet(
        awaiting_first_packet,
        asset_id,
        "book",
        ts_ms,
        log_ctx,
        rollover_in_progress,
        best_bid,
        best_ask,
    );
    try_update_best(
        cache,
        asset_id,
        best_bid,
        best_ask,
        ts_ms,
        QuoteSource::Book,
        update_stats,
    );
}

fn apply_price_change_event(
    event: &Value,
    cache: &mut HashMap<String, BestQuote>,
    stats: &mut PriceChangeStats,
    update_stats: &mut QuoteUpdateStats,
    awaiting_first_packet: &mut Option<AwaitingFirstPacket>,
    log_ctx: &RolloverLogContext,
    rollover_in_progress: &mut bool,
) {
    let price_changes = match event.get("price_changes").and_then(|v| v.as_array()) {
        Some(items) => items,
        None => return,
    };
    let ts_ms = event.get("timestamp").and_then(parse_timestamp_ms);
    stats.msgs_total += 1;
    stats.entries_total += price_changes.len() as u64;

    let mut applied = 0u64;
    for change in price_changes {
        let asset_id = match change.get("asset_id").and_then(|v| v.as_str()) {
            Some(id) => id,
            None => continue,
        };
        if !cache.contains_key(asset_id) {
            continue;
        }
        let best_bid = change.get("best_bid").and_then(parse_f64_value);
        let best_ask = change.get("best_ask").and_then(parse_f64_value);
        note_rollover_packet(
            awaiting_first_packet,
            asset_id,
            "price_change",
            ts_ms,
            log_ctx,
            rollover_in_progress,
            best_bid,
            best_ask,
        );
        if best_bid.is_none() && best_ask.is_none() {
            continue;
        }
        if try_update_best(
            cache,
            asset_id,
            best_bid,
            best_ask,
            ts_ms,
            QuoteSource::PriceChange,
            update_stats,
        ) {
            applied += 1;
        }
    }

    stats.best_updates += applied;
    maybe_log_price_change_stats(stats);
}

fn apply_best_bid_ask_event(
    event: &Value,
    cache: &mut HashMap<String, BestQuote>,
    update_stats: &mut QuoteUpdateStats,
    awaiting_first_packet: &mut Option<AwaitingFirstPacket>,
    log_ctx: &RolloverLogContext,
    rollover_in_progress: &mut bool,
) {
    let asset_id = match event.get("asset_id").and_then(|v| v.as_str()) {
        Some(id) => id,
        None => return,
    };
    if !cache.contains_key(asset_id) {
        return;
    }
    let best_bid = event.get("best_bid").and_then(parse_f64_value);
    let best_ask = event.get("best_ask").and_then(parse_f64_value);
    if best_bid.is_none() && best_ask.is_none() {
        return;
    }
    let ts_ms = event.get("timestamp").and_then(parse_timestamp_ms);
    note_rollover_packet(
        awaiting_first_packet,
        asset_id,
        "best_bid_ask",
        ts_ms,
        log_ctx,
        rollover_in_progress,
        best_bid,
        best_ask,
    );
    try_update_best(
        cache,
        asset_id,
        best_bid,
        best_ask,
        ts_ms,
        QuoteSource::BestBidAsk,
        update_stats,
    );
}

fn apply_tick_size_change_event(event: &Value, dryrun: &mut DryRunLedger) {
    let tick_size = event
        .get("tick_size")
        .or_else(|| event.get("tickSize"))
        .or_else(|| event.get("new_tick_size"))
        .and_then(parse_f64_value);
    if let Some(tick_size) = tick_size {
        if tick_size.is_finite() && tick_size > 0.0 {
            dryrun.tick_size_current = tick_size;
            dryrun.tick_size_source = "ws_tick_size_change".to_string();
        }
    }
}

fn try_update_best(
    cache: &mut HashMap<String, BestQuote>,
    asset_id: &str,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    timestamp_ms: Option<i64>,
    source: QuoteSource,
    stats: &mut QuoteUpdateStats,
) -> bool {
    if best_bid.is_none() && best_ask.is_none() {
        return false;
    }

    let quote = cache.entry(asset_id.to_string()).or_default();
    let now_ms = now_ts_ms_i64();
    let ts_ms = timestamp_ms.unwrap_or(now_ms);
    let has_best = quote.best_bid.is_some() || quote.best_ask.is_some();
    let current_priority = source_priority(quote.source);
    let new_priority = source_priority(source);
    let age_ms = age_ms(now_ms, quote.last_update_ts_ms);
    let stale = quote.last_update_ts_ms == 0 || age_ms > 3_000;

    if has_best && new_priority < current_priority && !stale {
        if source == QuoteSource::Book {
            stats.ignored_book_lower_priority += 1;
            maybe_log_quote_update_stats(stats);
        }
        return false;
    }

    if source == QuoteSource::Book {
        if let (Some(bid), Some(ask)) = (best_bid, best_ask) {
            if bid <= 0.01 + 1e-9 && ask >= 0.99 - 1e-9 {
                if matches!(quote.source, QuoteSource::BestBidAsk | QuoteSource::PriceChange)
                    && age_ms < 2_000
                {
                    stats.ignored_book_extreme += 1;
                    maybe_log_quote_update_stats(stats);
                    return false;
                }
            }
        }
    }

    let old_bid = quote.best_bid;
    let old_ask = quote.best_ask;

    if let Some(bid) = best_bid {
        quote.best_bid = Some(bid);
    }
    if let Some(ask) = best_ask {
        quote.best_ask = Some(ask);
    }
    quote.last_update_ts_ms = ts_ms;
    quote.source = source;

    let changed = price_changed(old_bid, quote.best_bid) || price_changed(old_ask, quote.best_ask);
    if changed {
        let label = match source {
            QuoteSource::Book => "book applied",
            QuoteSource::PriceChange => "price_change applied",
            QuoteSource::BestBidAsk => "best_bid_ask applied",
        };
        tracing::info!(
            asset_id = %asset_id,
            old_bid = ?old_bid,
            old_ask = ?old_ask,
            new_bid = ?quote.best_bid,
            new_ask = ?quote.best_ask,
            ts = ts_ms,
            "{label}"
        );
    }

    match source {
        QuoteSource::Book => stats.applied_book += 1,
        QuoteSource::PriceChange => stats.applied_price_change += 1,
        QuoteSource::BestBidAsk => stats.applied_best_bid_ask += 1,
    }
    maybe_log_quote_update_stats(stats);
    true
}

fn price_changed(old: Option<f64>, new: Option<f64>) -> bool {
    match (old, new) {
        (Some(a), Some(b)) => (a - b).abs() > 1e-12,
        (None, Some(_)) | (Some(_), None) => true,
        (None, None) => false,
    }
}

fn parse_f64_value(value: &Value) -> Option<f64> {
    match value {
        Value::String(raw) => raw.parse::<f64>().ok(),
        Value::Number(num) => num.as_f64(),
        _ => None,
    }
}

fn parse_timestamp_ms(value: &Value) -> Option<i64> {
    match value {
        Value::Number(num) => num.as_i64().or_else(|| num.as_u64().map(|v| v as i64)),
        Value::String(raw) => raw.parse::<i64>().ok(),
        _ => None,
    }
}

fn best_bid_from_levels(levels: &Value) -> Option<f64> {
    let items = levels.as_array()?;
    items
        .iter()
        .filter_map(price_from_level)
        .max_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
}

fn best_ask_from_levels(levels: &Value) -> Option<f64> {
    let items = levels.as_array()?;
    items
        .iter()
        .filter_map(price_from_level)
        .min_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
}

fn price_from_level(level: &Value) -> Option<f64> {
    match level {
        Value::Array(values) => values.get(0).and_then(parse_f64_value),
        Value::Object(map) => map
            .get("price")
            .and_then(parse_f64_value)
            .or_else(|| map.get("p").and_then(parse_f64_value)),
        _ => parse_f64_value(level),
    }
}

fn maybe_log_price_change_stats(stats: &mut PriceChangeStats) {
    let now_ms = now_ts_ms();
    if stats.last_log_ms == 0 {
        stats.last_log_ms = now_ms;
        return;
    }
    if now_ms.saturating_sub(stats.last_log_ms) >= 10_000 {
        tracing::info!(
            msgs = stats.msgs_total,
            entries = stats.entries_total,
            best_updates = stats.best_updates,
            "price_change stats"
        );
        stats.last_log_ms = now_ms;
    }
}

fn source_priority(source: QuoteSource) -> u8 {
    match source {
        QuoteSource::Book => 0,
        QuoteSource::PriceChange => 1,
        QuoteSource::BestBidAsk => 2,
    }
}

fn maybe_log_quote_update_stats(stats: &mut QuoteUpdateStats) {
    let now_ms = now_ts_ms_i64();
    if stats.last_log_ms == 0 {
        stats.last_log_ms = now_ms;
        return;
    }
    if age_ms(now_ms, stats.last_log_ms) >= 10_000 {
        tracing::info!(
            applied_book = stats.applied_book,
            applied_price_change = stats.applied_price_change,
            applied_best_bid_ask = stats.applied_best_bid_ask,
            ignored_book_extreme = stats.ignored_book_extreme,
            ignored_book_lower_priority = stats.ignored_book_lower_priority,
            "quote health"
        );
        stats.last_log_ms = now_ms;
    }
}

fn note_rollover_packet(
    awaiting: &mut Option<AwaitingFirstPacket>,
    asset_id: &str,
    event_type: &str,
    timestamp_ms: Option<i64>,
    log_ctx: &RolloverLogContext,
    rollover_in_progress: &mut bool,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
) {
    let Some(state) = awaiting.as_mut() else {
        return;
    };
    if state.pending.remove(asset_id) {
        let now_ms = now_ts_ms_i64();
        let latency_ms = timestamp_ms.map(|ts| age_ms(now_ms, ts));
        state.final_event_type = Some(event_type.to_string());
        let mut data = serde_json::Map::new();
        data.insert("asset_id".to_string(), Value::String(asset_id.to_string()));
        data.insert("event_type".to_string(), Value::String(event_type.to_string()));
        data.insert(
            "ws_ts_ms".to_string(),
            timestamp_ms
                .map(|value| Value::Number(serde_json::Number::from(value)))
                .unwrap_or(Value::Null),
        );
        data.insert(
            "latency_ms".to_string(),
            latency_ms
                .map(|value| Value::Number(serde_json::Number::from(value)))
                .unwrap_or(Value::Null),
        );
        data.insert(
            "best_bid".to_string(),
            best_bid.map(Value::from).unwrap_or(Value::Null),
        );
        data.insert(
            "best_ask".to_string(),
            best_ask.map(Value::from).unwrap_or(Value::Null),
        );
        log_jsonl(log_ctx, "rollover_first_packet", data);
        if log_ctx.verbose {
            tracing::info!(
                generation = state.generation,
                asset_id = %asset_id,
                event_type = %event_type,
                ts = ?timestamp_ms,
                "rollover first_packet ok"
            );
        }
        if state.pending.is_empty() {
            let duration_ms = age_ms(now_ms, state.start_ms);
            let mut data = serde_json::Map::new();
            data.insert(
                "new_ids_seen".to_string(),
                Value::Number(serde_json::Number::from(state.total as u64)),
            );
            data.insert(
                "duration_ms".to_string(),
                Value::Number(serde_json::Number::from(duration_ms)),
            );
            data.insert(
                "new_slug".to_string(),
                to_value_string(state.new_slug.clone()),
            );
            data.insert(
                "new_ids_hash".to_string(),
                Value::String(state.new_ids_hash.clone()),
            );
            data.insert(
                "final_event_type".to_string(),
                state
                    .final_event_type
                    .as_deref()
                    .map(|value| Value::String(value.to_string()))
                    .unwrap_or(Value::Null),
            );
            log_jsonl(log_ctx, "rollover_done", data);
            *awaiting = None;
            *rollover_in_progress = false;
        }
    }
}

fn should_rollover(current: &AssetSelection, next: &AssetSelection) -> bool {
    if current.market_slug != next.market_slug {
        return true;
    }
    let mut current_ids = current.asset_ids.clone();
    let mut next_ids = next.asset_ids.clone();
    current_ids.sort();
    next_ids.sort();
    current_ids != next_ids
}

fn rollover_reason(current: &AssetSelection, next: &AssetSelection) -> String {
    let slug_changed = current.market_slug != next.market_slug;
    let mut current_ids = current.asset_ids.clone();
    let mut next_ids = next.asset_ids.clone();
    current_ids.sort();
    next_ids.sort();
    let ids_changed = current_ids != next_ids;
    match (slug_changed, ids_changed) {
        (true, true) => "both_changed".to_string(),
        (true, false) => "slug_changed".to_string(),
        (false, true) => "token_ids_changed".to_string(),
        (false, false) => "no_change".to_string(),
    }
}

fn log_gamma_pick_stats_json(
    log_ctx: &RolloverLogContext,
    stats: &gamma::GammaPickStats,
    selection: &AssetSelection,
    throttle: &mut GammaLogThrottle,
    throttle_ms: i64,
) -> bool {
    if !log_ctx.enabled {
        return false;
    }
    let now_ms = now_ts_ms_i64();
    let ids_hash = hash_ids(&selection.asset_ids);
    let event_slug = stats
        .event_slug
        .clone()
        .or(selection.event_slug.clone())
        .unwrap_or_default();
    let key = format!("{}:{}:{}", stats.mode, event_slug, ids_hash);
    if !throttle.should_log(&key, now_ms, throttle_ms) {
        return false;
    }

    let mut data = serde_json::Map::new();
    data.insert("mode".to_string(), Value::String(stats.mode.clone()));
    data.insert(
        "pages_scanned".to_string(),
        Value::Number(serde_json::Number::from(stats.pages_scanned)),
    );
    data.insert(
        "total_events_seen".to_string(),
        Value::Number(serde_json::Number::from(stats.total_events_seen)),
    );
    data.insert(
        "matched_events".to_string(),
        Value::Number(serde_json::Number::from(stats.matched_events)),
    );
    data.insert(
        "candidates_total".to_string(),
        Value::Number(serde_json::Number::from(stats.candidates_total)),
    );
    data.insert(
        "qualified_total".to_string(),
        Value::Number(serde_json::Number::from(stats.qualified_total)),
    );
    data.insert(
        "rejected_no_market".to_string(),
        Value::Number(serde_json::Number::from(stats.rejected_no_market)),
    );
    data.insert(
        "rejected_closed".to_string(),
        Value::Number(serde_json::Number::from(stats.rejected_closed)),
    );
    data.insert(
        "rejected_not_accepting_orders".to_string(),
        Value::Number(serde_json::Number::from(
            stats.rejected_not_accepting_orders,
        )),
    );
    data.insert(
        "rejected_no_orderbook".to_string(),
        Value::Number(serde_json::Number::from(stats.rejected_no_orderbook)),
    );
    data.insert(
        "decode_errors".to_string(),
        Value::Number(serde_json::Number::from(stats.decode_errors)),
    );
    data.insert(
        "http_errors".to_string(),
        Value::Number(serde_json::Number::from(stats.http_errors)),
    );
    data.insert(
        "best_score".to_string(),
        stats.best_score.map(Value::from).unwrap_or(Value::Null),
    );
    let selected_slug = stats
        .selected_market_slug
        .clone()
        .or(selection.market_slug.clone());
    data.insert("selected_market_slug".to_string(), to_value_string(selected_slug));
    let event_slug = stats.event_slug.clone().or(selection.event_slug.clone());
    data.insert("event_slug".to_string(), to_value_string(event_slug));
    data.insert(
        "printed_total".to_string(),
        Value::Number(serde_json::Number::from(throttle.printed_total)),
    );
    data.insert(
        "suppressed_total".to_string(),
        Value::Number(serde_json::Number::from(throttle.suppressed_total)),
    );
    log_jsonl(log_ctx, "gamma_pick_stats", data);
    true
}

fn log_gamma_fastpath_hit_json(
    log_ctx: &RolloverLogContext,
    stats: &gamma::GammaPickStats,
    selection: &AssetSelection,
    throttle: &mut GammaLogThrottle,
    throttle_ms: i64,
) -> bool {
    if !log_ctx.enabled {
        return false;
    }
    let now_ms = now_ts_ms_i64();
    let ids_hash = hash_ids(&selection.asset_ids);
    let event_slug = stats
        .event_slug
        .clone()
        .or(selection.event_slug.clone())
        .unwrap_or_default();
    let key = format!("fastpath:{}:{}", event_slug, ids_hash);
    if !throttle.should_log(&key, now_ms, throttle_ms) {
        return false;
    }

    let mut sorted_ids = selection.asset_ids.clone();
    sorted_ids.sort();
    let mut data = serde_json::Map::new();
    data.insert("event_slug".to_string(), Value::String(event_slug));
    let selected_slug = stats
        .selected_market_slug
        .clone()
        .or(selection.market_slug.clone());
    data.insert("selected_market_slug".to_string(), to_value_string(selected_slug));
    data.insert(
        "assets_len".to_string(),
        Value::Number(serde_json::Number::from(sorted_ids.len() as u64)),
    );
    data.insert("assets_ids".to_string(), to_value_strings(&sorted_ids));
    data.insert("ids_hash".to_string(), Value::String(ids_hash));
    data.insert("mode".to_string(), Value::String(stats.mode.clone()));
    data.insert(
        "printed_total".to_string(),
        Value::Number(serde_json::Number::from(throttle.printed_total)),
    );
    data.insert(
        "suppressed_total".to_string(),
        Value::Number(serde_json::Number::from(throttle.suppressed_total)),
    );
    log_jsonl(log_ctx, "gamma_fastpath_hit", data);
    true
}

fn log_gamma_selected_market_json(
    log_ctx: &RolloverLogContext,
    stats: &gamma::GammaPickStats,
    selection: &AssetSelection,
) {
    if !log_ctx.enabled {
        return;
    }
    let now_ts = Utc::now().timestamp();
    let start_ts = selection
        .event_start_time
        .as_deref()
        .or(selection.start_time.as_deref())
        .and_then(parse_iso_to_unix_secs);
    let end_ts = selection
        .end_date
        .as_deref()
        .and_then(parse_iso_to_unix_secs);
    let mut sorted_ids = selection.asset_ids.clone();
    sorted_ids.sort();

    let mut data = serde_json::Map::new();
    data.insert(
        "matched_by".to_string(),
        to_value_string(stats.matched_by.clone()),
    );
    let selected_slug = stats
        .selected_market_slug
        .clone()
        .or(selection.market_slug.clone());
    data.insert("selected_market_slug".to_string(), to_value_string(selected_slug));
    data.insert(
        "condition_id".to_string(),
        to_value_string(selection.condition_id.clone()),
    );
    let event_slug = stats.event_slug.clone().or(selection.event_slug.clone());
    data.insert("event_slug".to_string(), to_value_string(event_slug));
    data.insert("token_ids".to_string(), to_value_strings(&sorted_ids));
    data.insert(
        "tick_size".to_string(),
        selection
            .gamma_tick
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    let event_start_iso = selection
        .event_start_time
        .clone()
        .or(selection.start_time.clone());
    data.insert(
        "event_start_iso".to_string(),
        to_value_string(event_start_iso),
    );
    data.insert(
        "end_date_iso".to_string(),
        to_value_string(selection.end_date.clone()),
    );
    let now_ts_u64 = u64::try_from(now_ts).unwrap_or(0);
    data.insert(
        "now_ts".to_string(),
        Value::Number(serde_json::Number::from(now_ts_u64)),
    );
    data.insert(
        "start_ts".to_string(),
        start_ts.map(Value::from).unwrap_or(Value::Null),
    );
    data.insert(
        "end_ts".to_string(),
        end_ts.map(Value::from).unwrap_or(Value::Null),
    );
    data.insert(
        "score".to_string(),
        selection.score.map(Value::from).unwrap_or(Value::Null),
    );
    log_jsonl(log_ctx, "gamma_selected_market", data);
}

fn log_jsonl(log_ctx: &RolloverLogContext, kind: &str, data: serde_json::Map<String, Value>) {
    if !log_ctx.enabled {
        return;
    }
    let mut envelope = serde_json::Map::new();
    envelope.insert("kind".to_string(), Value::String(kind.to_string()));
    envelope.insert(
        "ts_ms".to_string(),
        Value::Number(serde_json::Number::from(now_ts_ms_i64())),
    );
    envelope.insert(
        "series_slug".to_string(),
        Value::String(log_ctx.series_slug.clone()),
    );
    envelope.insert(
        "rollover_gen".to_string(),
        Value::Number(serde_json::Number::from(log_ctx.rollover_gen)),
    );
    envelope.insert("data".to_string(), serde_json::Value::Object(data));
    tracing::info!("{}", serde_json::Value::Object(envelope).to_string());
}

fn build_dryrun_ledger(
    series_slug: &str,
    selection: &AssetSelection,
    market_select_mode: &str,
) -> DryRunLedger {
    let up_id = selection.asset_ids.get(0).cloned().unwrap_or_default();
    let down_id = selection.asset_ids.get(1).cloned().unwrap_or_default();
    let start_ts = selection
        .event_start_time
        .as_deref()
        .or(selection.start_time.as_deref())
        .and_then(parse_iso_to_unix_secs)
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or(0);
    let end_ts = selection
        .end_date
        .as_deref()
        .and_then(parse_iso_to_unix_secs)
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or(0);
    let (tick_size_current, tick_size_source) = match selection.gamma_tick {
        Some(value) => (value, "gamma".to_string()),
        None => (0.01, "unknown".to_string()),
    };
    DryRunLedger {
        market_slug: selection.market_slug.clone().unwrap_or_default(),
        series_slug: series_slug.to_string(),
        market_select_mode: market_select_mode.to_string(),
        up_id,
        down_id,
        tick_size: selection.gamma_tick.unwrap_or(0.01),
        tick_size_current,
        tick_size_source,
        start_ts,
        end_ts,
        qty_up: 0.0,
        cost_up: 0.0,
        qty_down: 0.0,
        cost_down: 0.0,
        spent_total_usdc: 0.0,
        spent_round_usdc: 0.0,
        round_idx: 0,
        round_state: RoundPhase::Idle,
        round_leg1: None,
        round_qty_target: 0.0,
        best_bid_up: None,
        best_ask_up: None,
        best_bid_down: None,
        best_ask_down: None,
        last_decision_ts_ms: 0,
        decision_seq: 0,
        locked: false,
        locked_hedgeable: 0.0,
        locked_pair_cost: None,
        locked_at_ts_ms: 0,
    }
}

impl DryRunLedger {
    fn update_from_cache(&mut self, cache: &HashMap<String, BestQuote>) {
        if let Some(quote) = cache.get(&self.up_id) {
            self.best_bid_up = quote.best_bid;
            self.best_ask_up = quote.best_ask;
        }
        if let Some(quote) = cache.get(&self.down_id) {
            self.best_bid_down = quote.best_bid;
            self.best_ask_down = quote.best_ask;
        }
    }

    fn avg_up(&self) -> Option<f64> {
        avg_cost(self.cost_up, self.qty_up)
    }

    fn avg_down(&self) -> Option<f64> {
        avg_cost(self.cost_down, self.qty_down)
    }

    fn pair_cost(&self) -> Option<f64> {
        match (self.avg_up(), self.avg_down()) {
            (Some(up), Some(down)) => Some(up + down),
            _ => None,
        }
    }

    fn hedgeable(&self) -> f64 {
        self.qty_up.min(self.qty_down)
    }

    fn unhedged_up(&self) -> f64 {
        (self.qty_up - self.qty_down).max(0.0)
    }

    fn unhedged_down(&self) -> f64 {
        (self.qty_down - self.qty_up).max(0.0)
    }

    fn update_lock(&mut self, params: &DryRunParams, now_ms: u64) {
        if self.locked {
            return;
        }
        if let Some(pair_cost) = self.pair_cost() {
            if pair_cost <= 1.0 - params.safety_margin && self.hedgeable() > 0.0 {
                self.locked = true;
                self.locked_hedgeable = self.hedgeable();
                self.locked_pair_cost = Some(pair_cost);
                self.locked_at_ts_ms = now_ms;
            }
        }
    }

    fn round_state_str(&self) -> &'static str {
        match self.round_state {
            RoundPhase::Idle => "idle",
            RoundPhase::Leg1Filled => "leg1_filled",
            RoundPhase::Done => "done",
        }
    }

    fn round_leg1_str(&self) -> Option<&'static str> {
        match self.round_leg1 {
            Some(TradeLeg::Up) => Some("UP"),
            Some(TradeLeg::Down) => Some("DOWN"),
            None => None,
        }
    }

    fn update_round_state(&mut self, params: &DryRunParams, now_ts: u64) {
        if self.round_state == RoundPhase::Done {
            return;
        }
        let time_left_secs = if self.end_ts > now_ts {
            self.end_ts - now_ts
        } else {
            0
        };
        if self.round_state == RoundPhase::Idle
            && (self.round_idx >= params.max_rounds || time_left_secs <= params.tail_freeze_secs)
        {
            self.round_state = RoundPhase::Done;
            self.round_leg1 = None;
            self.round_qty_target = 0.0;
            self.spent_round_usdc = 0.0;
        }
    }
}

fn should_run_dryrun(ledger: &DryRunLedger, now_ms: u64, interval_ms: u64) -> bool {
    ledger.last_decision_ts_ms == 0 || now_ms.saturating_sub(ledger.last_decision_ts_ms) >= interval_ms
}

fn avg_cost(cost: f64, qty: f64) -> Option<f64> {
    if qty > 0.0 {
        Some(cost / qty)
    } else {
        None
    }
}

fn mid_price(bid: Option<f64>, ask: Option<f64>) -> Option<f64> {
    match (bid, ask) {
        (Some(b), Some(a)) => Some((b + a) / 2.0),
        _ => None,
    }
}

fn price_for_unhedged(bid: Option<f64>, ask: Option<f64>) -> Option<f64> {
    mid_price(bid, ask).or(ask).or(bid)
}

fn compute_round_qty_target(
    price_leg1: f64,
    price_leg2: f64,
    round_budget_usdc: f64,
    leg1_fraction: f64,
) -> Option<f64> {
    if price_leg1 <= 0.0 || price_leg2 <= 0.0 || round_budget_usdc <= 0.0 {
        return None;
    }
    let leg1_budget = round_budget_usdc * leg1_fraction;
    let mut qty = (leg1_budget / price_leg1).floor();
    let max_pair_qty = (round_budget_usdc / (price_leg1 + price_leg2)).floor();
    if max_pair_qty < qty {
        qty = max_pair_qty;
    }
    if qty <= 0.0 {
        return None;
    }
    Some(qty)
}

fn build_round_plan(ledger: &DryRunLedger, params: &DryRunParams, now_ts: u64) -> RoundPlan {
    let time_left_secs = if ledger.end_ts > now_ts {
        ledger.end_ts - now_ts
    } else {
        0
    };
    let abs_net = (ledger.qty_up - ledger.qty_down).abs();
    let eps = 1e-9;
    let budget_remaining_round = (params.round_budget_usdc - ledger.spent_round_usdc).max(0.0);
    let budget_remaining_total = (params.total_budget_usdc - ledger.spent_total_usdc).max(0.0);

    if ledger.round_state == RoundPhase::Leg1Filled {
        return RoundPlan {
            phase: ledger.round_state,
            planned_leg1: ledger.round_leg1,
            qty_target: if ledger.round_qty_target > 0.0 {
                Some(ledger.round_qty_target)
            } else {
                None
            },
            balance_leg: None,
            balance_qty: None,
            can_start_new_round: false,
            budget_remaining_round,
            budget_remaining_total,
            reserve_needed_usdc: None,
        };
    }

    let can_open_round = ledger.round_state == RoundPhase::Idle
        && ledger.round_idx < params.max_rounds
        && time_left_secs > params.tail_freeze_secs;

    if can_open_round {
        if let (Some(price_up), Some(price_down)) = (ledger.best_bid_up, ledger.best_bid_down) {
            let (leg1, price1, price2) = if price_up <= price_down {
                (TradeLeg::Up, price_up, price_down)
            } else {
                (TradeLeg::Down, price_down, price_up)
            };
            let qty_target = compute_round_qty_target(
                price1,
                price2,
                params.round_budget_usdc,
                params.round_leg1_fraction,
            );
            let reserve_needed_usdc = match leg1 {
                TradeLeg::Up => ledger.best_ask_down.or(ledger.best_bid_down),
                TradeLeg::Down => ledger.best_ask_up.or(ledger.best_bid_up),
            }
            .and_then(|price| qty_target.map(|qty| qty * price));
            let can_start_new_round = qty_target.is_some()
                && budget_remaining_round > 0.0
                && budget_remaining_total > 0.0;
            return RoundPlan {
                phase: RoundPhase::Idle,
                planned_leg1: Some(leg1),
                qty_target,
                balance_leg: None,
                balance_qty: None,
                can_start_new_round,
                budget_remaining_round,
                budget_remaining_total,
                reserve_needed_usdc,
            };
        }
    }

    if abs_net > eps {
        let balance_leg = if ledger.qty_up > ledger.qty_down {
            TradeLeg::Down
        } else {
            TradeLeg::Up
        };
        return RoundPlan {
            phase: ledger.round_state,
            planned_leg1: None,
            qty_target: None,
            balance_leg: Some(balance_leg),
            balance_qty: Some(abs_net),
            can_start_new_round: false,
            budget_remaining_round,
            budget_remaining_total,
            reserve_needed_usdc: None,
        };
    }

    RoundPlan {
        phase: ledger.round_state,
        planned_leg1: None,
        qty_target: None,
        balance_leg: None,
        balance_qty: None,
        can_start_new_round: false,
        budget_remaining_round,
        budget_remaining_total,
        reserve_needed_usdc: None,
    }
}

fn simulate_trade(ledger: &DryRunLedger, action: &CandidateAction) -> SimResult {
    let (best_bid, best_ask) = match action.leg {
        TradeLeg::Up => (ledger.best_bid_up, ledger.best_ask_up),
        TradeLeg::Down => (ledger.best_bid_down, ledger.best_ask_down),
    };
    let fill_price = match (action.side, action.kind) {
        (TradeSide::Buy, TradeKind::Taker) => best_ask,
        (TradeSide::Buy, TradeKind::Maker) => best_bid,
        (TradeSide::Sell, TradeKind::Taker) => best_bid,
        (TradeSide::Sell, TradeKind::Maker) => best_ask,
    };

    let mut new_qty_up = ledger.qty_up;
    let mut new_cost_up = ledger.cost_up;
    let mut new_qty_down = ledger.qty_down;
    let mut new_cost_down = ledger.cost_down;

    let mut ok = true;
    let mut fill_qty = action.qty;
    let fill_price_out = fill_price;

    if fill_price_out.is_none() {
        ok = false;
        fill_qty = 0.0;
    }

    if ok {
        match (action.leg, action.side) {
            (TradeLeg::Up, TradeSide::Buy) => {
                new_qty_up += fill_qty;
                new_cost_up += fill_qty * fill_price_out.unwrap_or(0.0);
            }
            (TradeLeg::Down, TradeSide::Buy) => {
                new_qty_down += fill_qty;
                new_cost_down += fill_qty * fill_price_out.unwrap_or(0.0);
            }
            (TradeLeg::Up, TradeSide::Sell) => {
                if ledger.qty_up < fill_qty {
                    ok = false;
                    fill_qty = 0.0;
                } else if let Some(avg) = ledger.avg_up() {
                    new_qty_up -= fill_qty;
                    new_cost_up = (new_cost_up - avg * fill_qty).max(0.0);
                } else {
                    ok = false;
                    fill_qty = 0.0;
                }
            }
            (TradeLeg::Down, TradeSide::Sell) => {
                if ledger.qty_down < fill_qty {
                    ok = false;
                    fill_qty = 0.0;
                } else if let Some(avg) = ledger.avg_down() {
                    new_qty_down -= fill_qty;
                    new_cost_down = (new_cost_down - avg * fill_qty).max(0.0);
                } else {
                    ok = false;
                    fill_qty = 0.0;
                }
            }
        }
    }

    let new_avg_up = avg_cost(new_cost_up, new_qty_up);
    let new_avg_down = avg_cost(new_cost_down, new_qty_down);
    let new_pair_cost = match (new_avg_up, new_avg_down) {
        (Some(up), Some(down)) => Some(up + down),
        _ => None,
    };
    let new_hedgeable = new_qty_up.min(new_qty_down);
    let new_unhedged_up = (new_qty_up - new_qty_down).max(0.0);
    let new_unhedged_down = (new_qty_down - new_qty_up).max(0.0);
    let new_unhedged_value_up = price_for_unhedged(ledger.best_bid_up, ledger.best_ask_up)
        .unwrap_or(0.0)
        * new_unhedged_up;
    let new_unhedged_value_down =
        price_for_unhedged(ledger.best_bid_down, ledger.best_ask_down)
            .unwrap_or(0.0)
            * new_unhedged_down;
    let current_unhedged_value_up =
        price_for_unhedged(ledger.best_bid_up, ledger.best_ask_up)
            .unwrap_or(0.0)
            * ledger.unhedged_up();
    let current_unhedged_value_down =
        price_for_unhedged(ledger.best_bid_down, ledger.best_ask_down)
            .unwrap_or(0.0)
            * ledger.unhedged_down();
    let improves_hedge = new_hedgeable > ledger.hedgeable()
        || (new_unhedged_value_up + new_unhedged_value_down)
            < (current_unhedged_value_up + current_unhedged_value_down);
    let cost_before = ledger.cost_up + ledger.cost_down;
    let cost_after = new_cost_up + new_cost_down;
    let spent_delta_usdc = (cost_after - cost_before).max(0.0);

    SimResult {
        ok,
        fill_qty,
        fill_price: fill_price_out,
        fee_estimate: 0.0,
        spent_delta_usdc,
        new_qty_up,
        new_cost_up,
        new_qty_down,
        new_cost_down,
        new_avg_up,
        new_avg_down,
        new_pair_cost,
        new_hedgeable,
        new_unhedged_up,
        new_unhedged_down,
        new_unhedged_value_up,
        new_unhedged_value_down,
        improves_hedge,
    }
}

fn evaluate_action_gate(
    ledger: &DryRunLedger,
    action: &CandidateAction,
    sim: &SimResult,
    params: &DryRunParams,
    now_ts: u64,
    round_plan: &RoundPlan,
) -> GateResult {
    let budget_remaining_round = round_plan.budget_remaining_round;
    let budget_remaining_total = round_plan.budget_remaining_total;
    let mut reserve_needed_usdc = None;
    if round_plan.phase == RoundPhase::Idle {
        if let Some(planned_leg1) = round_plan.planned_leg1 {
            if action.leg == planned_leg1 {
                let other_price = match planned_leg1 {
                    TradeLeg::Up => ledger.best_ask_down.or(ledger.best_bid_down),
                    TradeLeg::Down => ledger.best_ask_up.or(ledger.best_bid_up),
                };
                if let Some(other_price) = other_price {
                    reserve_needed_usdc = Some(action.qty * other_price);
                }
            }
        }
    }
    let mk_gate = |allow: bool, deny_reason: Option<DenyReason>| GateResult {
        allow,
        deny_reason,
        can_start_new_round: round_plan.can_start_new_round,
        budget_remaining_round,
        budget_remaining_total,
        reserve_needed_usdc,
    };

    if action.kind == TradeKind::Taker {
        return mk_gate(false, Some(DenyReason::TakerDisabled));
    }
    if !sim.ok {
        return mk_gate(false, Some(DenyReason::NoQuote));
    }
    let spent_total_after = ledger.spent_total_usdc + sim.spent_delta_usdc;
    let spent_round_after = ledger.spent_round_usdc + sim.spent_delta_usdc;
    let current_net = ledger.qty_up - ledger.qty_down;
    let sim_net = sim.new_qty_up - sim.new_qty_down;
    let abs_net = current_net.abs();
    let abs_sim = sim_net.abs();
    let eps = 1e-9;
    let is_reducing_risk = abs_sim + eps < abs_net;
    if spent_total_after > params.total_budget_usdc + 1e-9 {
        return mk_gate(false, Some(DenyReason::TotalBudgetCap));
    }
    if spent_round_after > params.round_budget_usdc + 1e-9 && !is_reducing_risk {
        return mk_gate(false, Some(DenyReason::RoundBudgetCap));
    }

    if let Some(planned_leg1) = round_plan.planned_leg1 {
        if round_plan.phase == RoundPhase::Idle && action.leg != planned_leg1 {
            return mk_gate(false, Some(DenyReason::ReserveForPair));
        }
        if round_plan.phase == RoundPhase::Leg1Filled && action.leg == planned_leg1 {
            return mk_gate(false, Some(DenyReason::ReserveForPair));
        }
        if round_plan.phase == RoundPhase::Idle && action.leg == planned_leg1 {
            if let Some(reserve_needed) = reserve_needed_usdc {
                if spent_round_after + reserve_needed > params.round_budget_usdc + 1e-9 {
                    return mk_gate(false, Some(DenyReason::ReserveForPair));
                }
                if spent_total_after + reserve_needed > params.total_budget_usdc + 1e-9 {
                    return mk_gate(false, Some(DenyReason::TotalBudgetCap));
                }
            }
        }
    }

    let current_pair_cost = ledger.pair_cost();
    let sim_pair_cost = sim.new_pair_cost;
    let time_left_secs = if ledger.end_ts > now_ts {
        ledger.end_ts - now_ts
    } else {
        0
    };
    let would_increase_abs_net_leg = abs_sim > abs_net + eps;
    let would_reduce_abs_net_leg = abs_sim + eps < abs_net;

    if time_left_secs <= params.tail_close_secs {
        if abs_net <= eps {
            return mk_gate(false, Some(DenyReason::TailClose));
        }
        if !would_reduce_abs_net_leg {
            return mk_gate(false, Some(DenyReason::TailClose));
        }
    } else if time_left_secs <= params.tail_freeze_secs && would_increase_abs_net_leg {
        return mk_gate(false, Some(DenyReason::TailFreeze));
    }

    if ledger.locked {
        if time_left_secs <= params.tail_freeze_secs && would_increase_abs_net_leg {
            return mk_gate(false, Some(DenyReason::LockedTailFreeze));
        }
        if ledger.round_state == RoundPhase::Leg1Filled {
            if let Some(leg1) = ledger.round_leg1 {
                if action.leg == leg1 {
                    return mk_gate(false, Some(DenyReason::LockedWaitingPair));
                }
            }
        }
        if ledger.round_idx >= params.max_rounds && would_increase_abs_net_leg {
            return mk_gate(false, Some(DenyReason::LockedMaxRounds));
        }
        if !round_plan.can_start_new_round && would_increase_abs_net_leg {
            return mk_gate(false, Some(DenyReason::LockedPolicyHold));
        }
    }

    if time_left_secs < params.cooldown_secs && would_increase_abs_net_leg {
        return mk_gate(false, Some(DenyReason::Cooldown));
    }

    if sim.new_unhedged_value_up > params.max_unhedged_value
        || sim.new_unhedged_value_down > params.max_unhedged_value
    {
        return mk_gate(
            false,
            Some(if sim.new_unhedged_value_up > params.max_unhedged_value {
                DenyReason::LegCapValueUp
            } else {
                DenyReason::LegCapValueDown
            }),
        );
    }
    if let Some(max_shares) = params.max_unhedged_shares {
        if sim.new_unhedged_up > max_shares {
            return mk_gate(false, Some(DenyReason::LegCapSharesUp));
        }
        if sim.new_unhedged_down > max_shares {
            return mk_gate(false, Some(DenyReason::LegCapSharesDown));
        }
    }
    if sim.new_unhedged_value_up >= params.max_unhedged_value {
        return mk_gate(false, Some(DenyReason::LegCapValueUp));
    }
    if sim.new_unhedged_value_down >= params.max_unhedged_value {
        return mk_gate(false, Some(DenyReason::LegCapValueDown));
    }

    if let Some(sim_pair_cost) = sim_pair_cost {
        if sim_pair_cost >= 1.0 - params.safety_margin {
            return mk_gate(false, Some(DenyReason::MarginTarget));
        }
    }

    if let (Some(current_pair_cost), Some(sim_pair_cost)) = (current_pair_cost, sim_pair_cost) {
        if sim_pair_cost > current_pair_cost - params.improve_min && !sim.improves_hedge {
            return mk_gate(false, Some(DenyReason::NoImprove));
        }
    }

    mk_gate(true, None)
}

fn build_dryrun_candidates(
    ledger: &DryRunLedger,
    params: &DryRunParams,
) -> Vec<(CandidateAction, SimResult, GateResult, Option<f64>)> {
    let now_ts = Utc::now().timestamp();
    let now_ts = u64::try_from(now_ts).unwrap_or(0);
    let round_plan = build_round_plan(ledger, params, now_ts);
    let qty_for_leg = |leg: TradeLeg| -> Option<f64> {
        if let Some(balance_leg) = round_plan.balance_leg {
            if balance_leg == leg {
                return round_plan.balance_qty;
            }
            return None;
        }
        round_plan.qty_target
    };
    let mut actions = Vec::new();
    if let Some(qty) = qty_for_leg(TradeLeg::Up) {
        actions.push(CandidateAction {
            name: "BUY_UP_MAKER",
            leg: TradeLeg::Up,
            side: TradeSide::Buy,
            kind: TradeKind::Maker,
            qty,
        });
    }
    if let Some(qty) = qty_for_leg(TradeLeg::Down) {
        actions.push(CandidateAction {
            name: "BUY_DOWN_MAKER",
            leg: TradeLeg::Down,
            side: TradeSide::Buy,
            kind: TradeKind::Maker,
            qty,
        });
    }
    let mut out = Vec::with_capacity(actions.len());
    for action in actions {
        let sim = simulate_trade(ledger, &action);
        let gate = evaluate_action_gate(ledger, &action, &sim, params, now_ts, &round_plan);
        let score = if gate.allow {
            let mut base = sim
                .new_pair_cost
                .or(sim.fill_price)
                .unwrap_or(1.0);
            if sim.new_pair_cost.is_some() {
                base -= params.pair_bonus;
            }
            let risk_penalty = (sim.new_unhedged_value_up + sim.new_unhedged_value_down) * 0.001;
            Some(base + risk_penalty)
        } else {
            None
        };
        out.push((action, sim, gate, score));
    }
    out
}

fn log_dryrun_snapshot(
    ledger: &DryRunLedger,
    params: &DryRunParams,
    log_ctx: &RolloverLogContext,
) {
    if !log_ctx.enabled {
        return;
    }
    let now_ts = Utc::now().timestamp();
    let now_ts = u64::try_from(now_ts).unwrap_or(0);
    let round_plan = build_round_plan(ledger, params, now_ts);
    let time_left_secs = if ledger.end_ts > now_ts {
        ledger.end_ts - now_ts
    } else {
        0
    };
    let avg_up = ledger.avg_up();
    let avg_down = ledger.avg_down();
    let pair_cost = ledger.pair_cost();
    let hedgeable = ledger.hedgeable();
    let unhedged_up = ledger.unhedged_up();
    let unhedged_down = ledger.unhedged_down();
    let unhedged_value_up =
        price_for_unhedged(ledger.best_bid_up, ledger.best_ask_up).unwrap_or(0.0) * unhedged_up;
    let unhedged_value_down = price_for_unhedged(ledger.best_bid_down, ledger.best_ask_down)
        .unwrap_or(0.0)
        * unhedged_down;
    let cooldown_active = ledger.end_ts > 0 && time_left_secs < params.cooldown_secs;
    let tail_mode = if time_left_secs <= params.tail_close_secs {
        "close"
    } else if time_left_secs <= params.tail_freeze_secs {
        "freeze"
    } else {
        "none"
    };

    let mut data = serde_json::Map::new();
    data.insert(
        "market_slug".to_string(),
        Value::String(ledger.market_slug.clone()),
    );
    data.insert(
        "market_select_mode".to_string(),
        Value::String(ledger.market_select_mode.clone()),
    );
    data.insert("up_id".to_string(), Value::String(ledger.up_id.clone()));
    data.insert(
        "down_id".to_string(),
        Value::String(ledger.down_id.clone()),
    );
    data.insert(
        "now_ts".to_string(),
        Value::Number(serde_json::Number::from(now_ts)),
    );
    data.insert(
        "time_left_secs".to_string(),
        Value::Number(serde_json::Number::from(time_left_secs)),
    );
    data.insert(
        "dryrun_mode".to_string(),
        Value::String(match params.mode {
            DryRunMode::Recommend => "recommend",
            DryRunMode::Paper => "paper",
        }
        .to_string()),
    );
    data.insert(
        "paper_trading_enabled".to_string(),
        Value::Bool(params.mode == DryRunMode::Paper),
    );
    data.insert(
        "tick_size_current".to_string(),
        Value::from(ledger.tick_size_current),
    );
    data.insert(
        "tick_size_source".to_string(),
        Value::String(ledger.tick_size_source.clone()),
    );
    data.insert(
        "cooldown_active".to_string(),
        Value::Bool(cooldown_active),
    );
    data.insert(
        "tail_mode".to_string(),
        Value::String(tail_mode.to_string()),
    );
    data.insert(
        "can_start_new_round".to_string(),
        Value::Bool(round_plan.can_start_new_round),
    );
    data.insert(
        "budget_remaining_round".to_string(),
        Value::from(round_plan.budget_remaining_round),
    );
    data.insert(
        "budget_remaining_total".to_string(),
        Value::from(round_plan.budget_remaining_total),
    );
    data.insert(
        "reserve_needed_usdc".to_string(),
        round_plan
            .reserve_needed_usdc
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "spent_total_usdc".to_string(),
        Value::from(ledger.spent_total_usdc),
    );
    data.insert(
        "spent_round_usdc".to_string(),
        Value::from(ledger.spent_round_usdc),
    );
    data.insert(
        "round_idx".to_string(),
        Value::Number(serde_json::Number::from(ledger.round_idx)),
    );
    data.insert(
        "round_state".to_string(),
        Value::String(ledger.round_state_str().to_string()),
    );
    data.insert(
        "round_leg1".to_string(),
        ledger
            .round_leg1_str()
            .map(|value| Value::String(value.to_string()))
            .unwrap_or(Value::Null),
    );
    data.insert(
        "round_qty_target".to_string(),
        Value::from(ledger.round_qty_target),
    );
    data.insert(
        "total_budget_usdc".to_string(),
        Value::from(params.total_budget_usdc),
    );
    data.insert(
        "round_budget_usdc".to_string(),
        Value::from(params.round_budget_usdc),
    );
    data.insert("locked".to_string(), Value::Bool(ledger.locked));
    data.insert(
        "cooldown_secs".to_string(),
        Value::Number(serde_json::Number::from(params.cooldown_secs)),
    );
    data.insert(
        "safety_margin".to_string(),
        Value::from(params.safety_margin),
    );
    data.insert(
        "margin_target".to_string(),
        Value::from(params.margin_target),
    );
    data.insert(
        "best_bid_up".to_string(),
        ledger.best_bid_up.map(Value::from).unwrap_or(Value::Null),
    );
    data.insert(
        "best_ask_up".to_string(),
        ledger.best_ask_up.map(Value::from).unwrap_or(Value::Null),
    );
    data.insert(
        "best_bid_down".to_string(),
        ledger.best_bid_down.map(Value::from).unwrap_or(Value::Null),
    );
    data.insert(
        "best_ask_down".to_string(),
        ledger.best_ask_down.map(Value::from).unwrap_or(Value::Null),
    );
    data.insert("qty_up".to_string(), Value::from(ledger.qty_up));
    data.insert("cost_up".to_string(), Value::from(ledger.cost_up));
    data.insert(
        "avg_up".to_string(),
        avg_up.map(Value::from).unwrap_or(Value::Null),
    );
    data.insert("qty_down".to_string(), Value::from(ledger.qty_down));
    data.insert("cost_down".to_string(), Value::from(ledger.cost_down));
    data.insert(
        "avg_down".to_string(),
        avg_down.map(Value::from).unwrap_or(Value::Null),
    );
    data.insert(
        "pair_cost".to_string(),
        pair_cost.map(Value::from).unwrap_or(Value::Null),
    );
    data.insert("hedgeable".to_string(), Value::from(hedgeable));
    data.insert("unhedged_up".to_string(), Value::from(unhedged_up));
    data.insert(
        "unhedged_down".to_string(),
        Value::from(unhedged_down),
    );
    data.insert(
        "unhedged_value_up".to_string(),
        Value::from(unhedged_value_up),
    );
    data.insert(
        "unhedged_value_down".to_string(),
        Value::from(unhedged_value_down),
    );
    log_jsonl(log_ctx, "dryrun_snapshot", data);
}

fn select_best_candidate<F>(
    candidates: &[(CandidateAction, SimResult, GateResult, Option<f64>)],
    filter: F,
) -> Option<CandidateEval>
where
    F: Fn(&CandidateAction) -> bool,
{
    let mut best: Option<CandidateEval> = None;
    let mut best_score = f64::INFINITY;
    for (action, sim, gate, score) in candidates {
        if !filter(action) {
            continue;
        }
        if !gate.allow {
            continue;
        }
        if let Some(score) = score {
            if *score < best_score {
                best_score = *score;
                best = Some(CandidateEval {
                    action: *action,
                    sim: sim.clone(),
                    gate: gate.clone(),
                    score: Some(*score),
                });
            }
        }
    }
    best
}

fn log_dryrun_candidates(
    ledger: &DryRunLedger,
    candidates: &[(CandidateAction, SimResult, GateResult, Option<f64>)],
    best: &Option<CandidateEval>,
    applied: &Option<CandidateEval>,
    log_ctx: &RolloverLogContext,
    params: &DryRunParams,
) {
    if !log_ctx.enabled {
        return;
    }
    let mut candidates_values = Vec::with_capacity(candidates.len());
    for (action, sim, gate, score) in candidates {
        candidates_values.push(candidate_to_value(action, sim, gate, *score, params));
    }

    let mut data = serde_json::Map::new();
    data.insert(
        "market_slug".to_string(),
        Value::String(ledger.market_slug.clone()),
    );
    data.insert(
        "decision_seq".to_string(),
        Value::Number(serde_json::Number::from(ledger.decision_seq)),
    );
    data.insert("candidates".to_string(), Value::Array(candidates_values));
    let best_value = best
        .as_ref()
        .map(|best| candidate_to_value(&best.action, &best.sim, &best.gate, best.score, params))
        .unwrap_or(Value::Null);
    data.insert("best_action".to_string(), best_value);
    let applied_value = applied
        .as_ref()
        .map(|best| candidate_to_value(&best.action, &best.sim, &best.gate, best.score, params))
        .unwrap_or(Value::Null);
    data.insert("applied_action".to_string(), applied_value);
    let apply_reason = if params.mode == DryRunMode::Paper && applied.is_some() {
        Value::String("paper_mode".to_string())
    } else {
        Value::Null
    };
    data.insert("apply_reason".to_string(), apply_reason);
    log_jsonl(log_ctx, "dryrun_candidates", data);
}

#[derive(Debug, Clone)]
struct CandidateEval {
    action: CandidateAction,
    sim: SimResult,
    gate: GateResult,
    score: Option<f64>,
}

fn candidate_to_value(
    action: &CandidateAction,
    sim: &SimResult,
    gate: &GateResult,
    score: Option<f64>,
    params: &DryRunParams,
) -> Value {
    let mut data = serde_json::Map::new();
    data.insert("action".to_string(), Value::String(action.name.to_string()));
    data.insert(
        "leg".to_string(),
        Value::String(match action.leg {
            TradeLeg::Up => "UP",
            TradeLeg::Down => "DOWN",
        }
        .to_string()),
    );
    data.insert(
        "side".to_string(),
        Value::String(match action.side {
            TradeSide::Buy => "BUY",
            TradeSide::Sell => "SELL",
        }
        .to_string()),
    );
    data.insert(
        "kind".to_string(),
        Value::String(match action.kind {
            TradeKind::Maker => "MAKER",
            TradeKind::Taker => "TAKER",
        }
        .to_string()),
    );
    data.insert("qty".to_string(), Value::from(action.qty));
    data.insert(
        "fill_price".to_string(),
        sim.fill_price.map(Value::from).unwrap_or(Value::Null),
    );
    data.insert(
        "sim_pair_cost".to_string(),
        sim.new_pair_cost.map(Value::from).unwrap_or(Value::Null),
    );
    data.insert(
        "sim_hedgeable".to_string(),
        Value::from(sim.new_hedgeable),
    );
    data.insert(
        "sim_unhedged_up".to_string(),
        Value::from(sim.new_unhedged_up),
    );
    data.insert(
        "sim_unhedged_down".to_string(),
        Value::from(sim.new_unhedged_down),
    );
    data.insert("improves_hedge".to_string(), Value::Bool(sim.improves_hedge));
    data.insert(
        "cap_unhedged_value".to_string(),
        params
            .cap_unhedged_value
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "cap_unhedged_shares".to_string(),
        params
            .max_unhedged_shares
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "sim_unhedged_value_up".to_string(),
        Value::from(sim.new_unhedged_value_up),
    );
    data.insert(
        "sim_unhedged_value_down".to_string(),
        Value::from(sim.new_unhedged_value_down),
    );
    data.insert(
        "can_start_new_round".to_string(),
        Value::Bool(gate.can_start_new_round),
    );
    data.insert(
        "budget_remaining_round".to_string(),
        Value::from(gate.budget_remaining_round),
    );
    data.insert(
        "budget_remaining_total".to_string(),
        Value::from(gate.budget_remaining_total),
    );
    data.insert(
        "reserve_needed_usdc".to_string(),
        gate.reserve_needed_usdc.map(Value::from).unwrap_or(Value::Null),
    );
    let cap_value = params.max_unhedged_value;
    data.insert(
        "would_violate_cap_value_up".to_string(),
        Value::Bool(sim.new_unhedged_value_up >= cap_value),
    );
    data.insert(
        "would_violate_cap_value_down".to_string(),
        Value::Bool(sim.new_unhedged_value_down >= cap_value),
    );
    let would_violate_cap_shares_up = params
        .max_unhedged_shares
        .map(|cap| sim.new_unhedged_up >= cap)
        .unwrap_or(false);
    let would_violate_cap_shares_down = params
        .max_unhedged_shares
        .map(|cap| sim.new_unhedged_down >= cap)
        .unwrap_or(false);
    data.insert(
        "would_violate_cap_shares_up".to_string(),
        Value::Bool(would_violate_cap_shares_up),
    );
    data.insert(
        "would_violate_cap_shares_down".to_string(),
        Value::Bool(would_violate_cap_shares_down),
    );
    let pair_bonus_applied = gate.allow && sim.new_pair_cost.is_some();
    data.insert(
        "pair_bonus_applied".to_string(),
        Value::Bool(pair_bonus_applied),
    );
    data.insert("allow".to_string(), Value::Bool(gate.allow));
    data.insert(
        "deny_reason".to_string(),
        gate.deny_reason
            .as_ref()
            .map(|value| Value::String(deny_reason_to_str(*value).to_string()))
            .unwrap_or(Value::Null),
    );
    data.insert(
        "score".to_string(),
        score.map(Value::from).unwrap_or(Value::Null),
    );
    Value::Object(data)
}

fn deny_reason_to_str(value: DenyReason) -> &'static str {
    match value {
        DenyReason::TakerDisabled => "taker_disabled",
        DenyReason::MarginTarget => "margin_target",
        DenyReason::NoImprove => "no_improve",
        DenyReason::Cooldown => "cooldown",
        DenyReason::NoQuote => "no_quote",
        DenyReason::TotalBudgetCap => "total_budget_cap",
        DenyReason::RoundBudgetCap => "round_budget_cap",
        DenyReason::ReserveForPair => "reserve_for_pair",
        DenyReason::LegCapValueUp => "leg_cap_value_up",
        DenyReason::LegCapValueDown => "leg_cap_value_down",
        DenyReason::LegCapSharesUp => "leg_cap_shares_up",
        DenyReason::LegCapSharesDown => "leg_cap_shares_down",
        DenyReason::TailFreeze => "tail_freeze",
        DenyReason::TailClose => "tail_close",
        DenyReason::LockedMaxRounds => "locked_max_rounds",
        DenyReason::LockedTailFreeze => "locked_tail_freeze",
        DenyReason::LockedWaitingPair => "locked_waiting_pair",
        DenyReason::LockedPolicyHold => "locked_policy_hold",
    }
}

fn log_dryrun_apply(
    ledger: &DryRunLedger,
    applied: &CandidateEval,
    params: &DryRunParams,
    log_ctx: &RolloverLogContext,
) {
    if !log_ctx.enabled {
        return;
    }
    let mut data = serde_json::Map::new();
    data.insert(
        "decision_seq".to_string(),
        Value::Number(serde_json::Number::from(ledger.decision_seq)),
    );
    data.insert(
        "applied_action".to_string(),
        candidate_to_value(
            &applied.action,
            &applied.sim,
            &applied.gate,
            applied.score,
            params,
        ),
    );
    data.insert("before_qty_up".to_string(), Value::from(ledger.qty_up));
    data.insert("before_cost_up".to_string(), Value::from(ledger.cost_up));
    data.insert(
        "before_qty_down".to_string(),
        Value::from(ledger.qty_down),
    );
    data.insert(
        "before_cost_down".to_string(),
        Value::from(ledger.cost_down),
    );
    data.insert("after_qty_up".to_string(), Value::from(applied.sim.new_qty_up));
    data.insert("after_cost_up".to_string(), Value::from(applied.sim.new_cost_up));
    data.insert(
        "after_qty_down".to_string(),
        Value::from(applied.sim.new_qty_down),
    );
    data.insert(
        "after_cost_down".to_string(),
        Value::from(applied.sim.new_cost_down),
    );
    data.insert(
        "note".to_string(),
        Value::String("paper_only_no_orders".to_string()),
    );
    log_jsonl(log_ctx, "dryrun_apply", data);
}

fn apply_simulated_trade(
    ledger: &mut DryRunLedger,
    action: &CandidateAction,
    sim: &SimResult,
    params: &DryRunParams,
    now_ts: u64,
) {
    if !sim.ok {
        return;
    }
    ledger.qty_up = sim.new_qty_up;
    ledger.cost_up = sim.new_cost_up;
    ledger.qty_down = sim.new_qty_down;
    ledger.cost_down = sim.new_cost_down;
    if sim.spent_delta_usdc > 0.0 {
        ledger.spent_total_usdc += sim.spent_delta_usdc;
        ledger.spent_round_usdc += sim.spent_delta_usdc;
    }

    let time_left_secs = if ledger.end_ts > now_ts {
        ledger.end_ts - now_ts
    } else {
        0
    };
    let eps = 1e-9;
    match ledger.round_state {
        RoundPhase::Idle => {
            if time_left_secs > params.tail_freeze_secs
                && ledger.round_idx < params.max_rounds
                && action.side == TradeSide::Buy
            {
                ledger.round_state = RoundPhase::Leg1Filled;
                ledger.round_leg1 = Some(action.leg);
                ledger.round_qty_target = action.qty;
            }
        }
        RoundPhase::Leg1Filled => {
            if let Some(leg1) = ledger.round_leg1 {
                let balanced = (sim.new_qty_up - sim.new_qty_down).abs() <= eps;
                if action.leg != leg1 && balanced {
                    ledger.round_idx = ledger.round_idx.saturating_add(1);
                    ledger.round_leg1 = None;
                    ledger.round_qty_target = 0.0;
                    ledger.spent_round_usdc = 0.0;
                    if ledger.round_idx >= params.max_rounds || time_left_secs <= params.tail_freeze_secs
                    {
                        ledger.round_state = RoundPhase::Done;
                    } else {
                        ledger.round_state = RoundPhase::Idle;
                    }
                }
            }
        }
        RoundPhase::Done => {}
    }
}

fn hash_ids(ids: &[String]) -> String {
    let mut sorted = ids.to_vec();
    sorted.sort();
    let mut hash: u64 = 0xcbf29ce484222325;
    for id in sorted {
        for byte in id.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{:016x}", hash)
}

fn to_value_string(value: Option<String>) -> Value {
    match value {
        Some(val) => Value::String(val),
        None => Value::Null,
    }
}

fn to_value_strings(values: &[String]) -> Value {
    Value::Array(values.iter().map(|id| Value::String(id.clone())).collect())
}

#[derive(Debug, Deserialize)]
struct BookLevel {
    price: String,
    #[allow(dead_code)]
    size: String,
}

#[derive(Debug, Deserialize)]
struct BookResp {
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
}

async fn log_best_quotes(
    asset_ids: &[String],
    gamma_tick: Option<f64>,
    cache: &HashMap<String, BestQuote>,
    tick_cache: &mut HashMap<String, TickSizeCacheEntry>,
    http_client: &Client,
    clob_host: &str,
    health: &mut QuoteHealth,
) -> Result<()> {
    let mut parts = Vec::with_capacity(asset_ids.len());

    update_quote_health(asset_ids, cache, health);

    for asset_id in asset_ids {
        let quote = cache.get(asset_id).cloned().unwrap_or_default();
        let bid = quote.best_bid;
        let ask = quote.best_ask;
        let tick_entry = get_tick_size(
            asset_id,
            gamma_tick,
            tick_cache,
            http_client,
            clob_host,
        )
        .await?;
        let tick = tick_entry.tick_size;
        let (buy_price, sell_price, buy_ok, sell_ok) = match (bid, ask) {
            (Some(bid), Some(ask)) => {
                let buy_price = postonly_buy_price(bid, ask, tick);
                let sell_price = postonly_sell_price(bid, ask, tick);
                (
                    buy_price,
                    sell_price,
                    buy_price.is_some(),
                    sell_price.is_some(),
                )
            }
            _ => (None, None, false, false),
        };

        let part = format!(
            "asset_id={asset_id} bid={} ask={} tick={}(src={}) buy={} ok={} sell={} ok={}",
            format_opt_price(bid),
            format_opt_price(ask),
            format_price(tick),
            tick_entry.source,
            format_opt_price(buy_price),
            buy_ok,
            format_opt_price(sell_price),
            sell_ok,
        );
        parts.push(part);
    }

    tracing::info!("{}", parts.join(" | "));
    Ok(())
}

async fn get_tick_size(
    asset_id: &str,
    gamma_tick: Option<f64>,
    cache: &mut HashMap<String, TickSizeCacheEntry>,
    http_client: &Client,
    clob_host: &str,
) -> Result<TickSizeCacheEntry> {
    let now_ms = now_ts_ms();
    if let Some(entry) = cache.get(asset_id) {
        if now_ms.saturating_sub(entry.updated_at_ms) <= 60_000 {
            return Ok(entry.clone());
        }
    }

    if let Some(tick) = gamma_tick.filter(|value| value.is_finite() && *value > 0.0) {
        let entry = TickSizeCacheEntry {
            tick_size: tick,
            updated_at_ms: now_ms,
            source: "gamma",
        };
        cache.insert(asset_id.to_string(), entry.clone());
        return Ok(entry);
    }

    match estimate_tick_size_from_book(asset_id, http_client, clob_host).await {
        Ok(tick) => {
            let (tick_size, source) = if tick < 1e-6 || tick > 0.5 {
                (0.01, "clob_book_estimate_default")
            } else {
                (tick, "clob_book_estimate")
            };
            let entry = TickSizeCacheEntry {
                tick_size,
                updated_at_ms: now_ms,
                source,
            };
            cache.insert(asset_id.to_string(), entry.clone());
            Ok(entry)
        }
        Err(err) => {
            tracing::warn!(asset_id, error = %err, "failed to estimate tick size");
            let entry = TickSizeCacheEntry {
                tick_size: 0.01,
                updated_at_ms: now_ms,
                source: "clob_book_estimate_default",
            };
            cache.insert(asset_id.to_string(), entry.clone());
            Ok(entry)
        }
    }
}

async fn estimate_tick_size_from_book(
    asset_id: &str,
    http_client: &Client,
    clob_host: &str,
) -> Result<f64> {
    let clob_host = clob_host.trim_end_matches('/');
    let url = format!("{clob_host}/book?token_id={asset_id}");
    let response = http_client
        .get(url)
        .send()
        .await
        .context("failed to fetch clob book")?;

    if !response.status().is_success() {
        bail!("clob book response status {}", response.status());
    }

    let book: BookResp = response.json().await.context("invalid clob book response")?;
    let mut prices = Vec::new();

    for level in book.bids.iter().take(50) {
        if let Ok(value) = level.price.parse::<f64>() {
            prices.push(value);
        }
    }
    for level in book.asks.iter().take(50) {
        if let Ok(value) = level.price.parse::<f64>() {
            prices.push(value);
        }
    }

    let tick = estimate_tick_from_prices(&mut prices)
        .context("unable to estimate tick size")?;
    Ok(tick)
}

fn estimate_tick_from_prices(prices: &mut Vec<f64>) -> Option<f64> {
    prices.retain(|value| value.is_finite());
    prices.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    prices.dedup_by(|a, b| (*a - *b).abs() < 1e-9);

    let mut min_diff: Option<f64> = None;
    for window in prices.windows(2) {
        let diff = window[1] - window[0];
        if diff > 0.0 {
            min_diff = Some(match min_diff {
                Some(current) => current.min(diff),
                None => diff,
            });
        }
    }
    min_diff
}

fn postonly_buy_price(best_bid: f64, best_ask: f64, tick: f64) -> Option<f64> {
    let p = (best_bid + tick).min(best_ask - tick);
    if p.is_finite() && p > 0.0 && p < best_ask {
        Some(p)
    } else {
        None
    }
}

fn postonly_sell_price(best_bid: f64, best_ask: f64, tick: f64) -> Option<f64> {
    let p = (best_ask - tick).max(best_bid + tick);
    if p.is_finite() && p < 1.0 && p > best_bid {
        Some(p)
    } else {
        None
    }
}

fn update_quote_health(
    asset_ids: &[String],
    cache: &HashMap<String, BestQuote>,
    health: &mut QuoteHealth,
) {
    if asset_ids.len() >= 2 {
        let first = cache.get(&asset_ids[0]).cloned().unwrap_or_default();
        let second = cache.get(&asset_ids[1]).cloned().unwrap_or_default();

        let symmetric = matches!(
            (first.best_bid, first.best_ask, second.best_bid, second.best_ask),
            (Some(b1), Some(a1), Some(b2), Some(a2))
                if (b1 - b2).abs() < 1e-9 && (a1 - a2).abs() < 1e-9
        );

        if symmetric {
            health.symmetric_seconds += 1;
            if health.symmetric_seconds > 5 && !health.symmetric_warned {
                tracing::warn!("quotes look symmetric for both tokens; verify parsing");
                health.symmetric_warned = true;
            }
        } else {
            health.symmetric_seconds = 0;
            health.symmetric_warned = false;
        }
    }

    let now_ms = now_ts_ms();
    if now_ms.saturating_sub(health.last_extreme_warn_ms) >= 30_000 {
        for asset_id in asset_ids {
            if let Some(quote) = cache.get(asset_id) {
                if let (Some(bid), Some(ask)) = (quote.best_bid, quote.best_ask) {
                    if bid <= 0.01 + 1e-6 && ask >= 0.99 - 1e-6 {
                        tracing::warn!("quotes stuck at extremes; ensure price_changes updates are applied");
                        health.last_extreme_warn_ms = now_ms;
                        break;
                    }
                }
            }
        }
    }

    let now_ms_i64 = now_ts_ms_i64();
    for asset_id in asset_ids {
        let last_update = cache.get(asset_id).map(|quote| quote.last_update_ts_ms);
        if let Some(last_update) = last_update {
            if last_update == 0 {
                continue;
            }
            let quote_age_ms = age_ms(now_ms_i64, last_update);
            if quote_age_ms > 5_000 {
                let last_warn = health
                    .last_stale_warn_ms
                    .get(asset_id)
                    .copied()
                    .unwrap_or(0);
                if age_ms(now_ms_i64, last_warn) >= 5_000 {
                    tracing::warn!(
                        asset_id = %asset_id,
                        age_ms = quote_age_ms,
                        bid = ?cache.get(asset_id).and_then(|q| q.best_bid),
                        ask = ?cache.get(asset_id).and_then(|q| q.best_ask),
                        "stale ws best_quote"
                    );
                    health
                        .last_stale_warn_ms
                        .insert(asset_id.to_string(), now_ms_i64);
                }
            }
        }
    }
}

fn format_opt_price(value: Option<f64>) -> String {
    match value {
        Some(price) => format!("{price:.6}"),
        None => "na".to_string(),
    }
}

fn format_price(value: f64) -> String {
    if value.is_finite() {
        format!("{value:.6}")
    } else {
        "na".to_string()
    }
}

fn now_ts_ms() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as u64,
        Err(_) => 0,
    }
}

fn now_ts_ms_i64() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(_) => 0,
    }
}

fn age_ms(now_ms: i64, last_ms: i64) -> i64 {
    if now_ms >= last_ms {
        now_ms - last_ms
    } else {
        0
    }
}

fn parse_iso_to_unix_secs(value: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt| dt.timestamp())
}

fn log_error_chain(err: &dyn Error) {
    let mut source = err.source();
    while let Some(cause) = source {
        tracing::warn!(source = %cause, "error source");
        source = cause.source();
    }
}

fn truncate_text(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    text.chars().take(max_chars).collect()
}

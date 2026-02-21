use anyhow::{bail, Context, Result};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::env;
use std::error::Error;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::config::Config;
use crate::execution::{
    ClobExecutionAdapter, ExecutionAdapter, OpenOrder, OrderIntent, OrderSide,
    PaperExecutionAdapter, PlaceAck,
};
use crate::gamma;
use crate::strategy;

#[derive(Debug, Clone, Default)]
struct BestQuote {
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    exchange_ts_ms: Option<i64>,
    recv_ts_ms: i64,
    latency_ms: Option<i64>,
    best_bid_size: Option<f64>,
    best_ask_size: Option<f64>,
    bid_consumption_rate: f64,
    ask_consumption_rate: f64,
    bid_levels: Vec<DepthLevel>,
    ask_levels: Vec<DepthLevel>,
    last_update_ts_ms: i64,
    source: QuoteSource,
}

#[derive(Debug, Clone, Copy, Default)]
struct DepthLevel {
    price: f64,
    size: f64,
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

const DEPTH_CONSUMPTION_EWMA_ALPHA: f64 = 0.35;
const DEPTH_MIN_DT_SECS: f64 = 0.05;
const DEPTH_PRICE_MOVE_DEPLETION_RATIO: f64 = 0.0025;
const DEPTH_PRICE_MOVE_MAX_OBSERVED_PER_SEC: f64 = 20.0;
const PAPER_REBALANCE_QUEUE_AHEAD_MULT: f64 = 2.0;
const PAPER_MAX_CONCURRENT_OPEN_ORDERS_TOTAL: usize = 2;
const PAPER_MAX_CONCURRENT_OPEN_ORDERS_PER_LEG: usize = 1;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    Leg1Accumulating,
    Leg2Balancing,
    Done,
}

#[derive(Debug, Clone, Copy)]
enum DenyReason {
    TakerDisabled,
    EntryWorstPair,
    HedgeNotRecoverable,
    HedgeMarginInsufficient,
    OpenMarginTooThin,
    EntryEdgeTooThin,
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
    LockedStrictAbsNet,
    LockedMaxRounds,
    LockedTailFreeze,
    LockedWaitingPair,
    LockedPolicyHold,
    LowMakerFillProb,
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
    maker_fill_prob: Option<f64>,
    maker_queue_ahead: Option<f64>,
    maker_expected_consumed: Option<f64>,
    maker_consumption_rate: Option<f64>,
    maker_horizon_secs: Option<f64>,
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
    hedge_recoverable_now: Option<bool>,
    required_opp_avg_price_cap: Option<f64>,
    current_opp_best_ask: Option<f64>,
    required_hedge_qty: Option<f64>,
    hedge_margin_to_opp_ask: Option<f64>,
    hedge_margin_required: Option<f64>,
    hedge_margin_ok: Option<bool>,
    entry_quote_base_postonly_price: Option<f64>,
    entry_quote_dynamic_cap_price: Option<f64>,
    entry_quote_final_price: Option<f64>,
    entry_quote_cap_active: Option<bool>,
    entry_quote_cap_bind: Option<bool>,
    passive_gap_abs: Option<f64>,
    passive_gap_ticks: Option<f64>,
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
    total_budget_source: String,
    max_rounds: u64,
    round_budget_usdc: f64,
    round_budget_source: String,
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
    lock_strict_abs_net: bool,
    round_budget_strict: bool,
    lock_min_completed_rounds: u64,
    lock_min_time_left_secs: u64,
    lock_min_spent_ratio: f64,
    lock_force_time_left_secs: u64,
    tail_close_ignore_margin: bool,
    vol_entry_min_bps: f64,
    vol_entry_lookback_ticks: u64,
    reversal_entry_enabled: bool,
    reversal_min_discount_bps: f64,
    reversal_min_momentum_bps: f64,
    reversal_fast_ema_ticks: u64,
    reversal_slow_ema_ticks: u64,
    entry_turn_confirm_ticks: u64,
    entry_turn_min_rebound_bps: f64,
    entry_pair_buffer_bps: f64,
    round_pair_wait_secs: u64,
    leg2_rebalance_discount_bps: f64,
    force_pair_max_pair_cost: f64,
    no_risk_hard_pair_cap: f64,
    no_risk_pair_limit_mode: strategy::params::NoRiskPairLimitMode,
    no_risk_enforce_tail: bool,
    no_risk_entry_hedge_price_mode: strategy::params::NoRiskEntryHedgePriceMode,
    no_risk_entry_ask_slippage_bps: f64,
    no_risk_entry_worst_hedge_bps: f64,
    no_risk_entry_pair_headroom_bps: f64,
    entry_dynamic_cap_enabled: bool,
    entry_dynamic_cap_headroom_bps: f64,
    entry_dynamic_cap_min_price: f64,
    entry_dynamic_cap_apply_to_net_increase_only: bool,
    entry_target_margin_min_ticks: f64,
    entry_target_margin_min_bps: f64,
    entry_max_passive_ticks_for_net_increase: f64,
    no_risk_late_new_round_buffer_secs: u64,
    no_risk_require_completable_round: bool,
    no_risk_strict_zero_unmatched: bool,
    no_risk_hedge_recoverability_enforce: bool,
    no_risk_hedge_recoverability_eps_bps: f64,
    hedge_recoverability_margin_enforce: bool,
    hedge_recoverability_margin_min_ticks: f64,
    hedge_recoverability_margin_min_bps: f64,
    hedge_recoverability_margin_apply_to_net_increase_only: bool,
    open_order_risk_guard: bool,
    open_order_risk_buffer_bps: f64,
    open_order_risk_guard_require_paired: bool,
    open_order_max_age_secs: u64,
    open_order_unrecoverable_grace_ms: u64,
    paper_timeout_target_fill_prob: f64,
    paper_timeout_progress_extend_min: f64,
    paper_timeout_progress_extend_secs: u64,
    paper_timeout_max_extends: u64,
    paper_requote_require_price_move: bool,
    paper_requote_stale_ms_hard: u64,
    paper_requote_retain_fill_draw: bool,
    requote_min_fill_prob_uplift: f64,
    requote_queue_stickiness_ratio: f64,
    requote_stickiness_min_age_secs: u64,
    round_slice_count: u64,
    round_dynamic_slicing_enabled: bool,
    round_min_slices: u64,
    round_max_slices: u64,
    round_min_slice_qty: f64,
    entry_pair_regression_mode: strategy::params::EntryPairRegressionMode,
    entry_pair_regression_soft_band_bps: f64,
    entry_edge_min_bps: f64,
    entry_fill_prob_min: f64,
    open_min_fill_prob: f64,
    open_margin_surplus_min: f64,
    inventory_skew_alpha_bps: f64,
    lock_allow_reopen_before_freeze: bool,
    lock_reopen_max_rounds: u64,
    round_min_start_gap_secs: u64,
    opening_no_trade_secs: u64,
    min_apply_interval_ms: u64,
    maker_fill_estimator_enabled: bool,
    maker_fill_horizon_secs: u64,
    maker_min_fill_prob: f64,
    maker_queue_ahead_mult: f64,
    maker_fill_passive_queue_penalty_per_tick: f64,
    maker_fill_passive_decay_k: f64,
    maker_flow_floor_per_sec: f64,
    entry_passive_gap_soft_max_ticks: f64,
    entry_max_top_book_share: f64,
    entry_max_flow_utilization: f64,
    entry_min_timeout_flow_ratio: f64,
    entry_fallback_enabled: bool,
    entry_fallback_deny_streak: u64,
    entry_fallback_window_secs: u64,
    entry_fallback_duration_secs: u64,
    entry_fallback_hedge_mode: strategy::params::NoRiskEntryHedgePriceMode,
    entry_fallback_worst_hedge_bps: f64,
    check_summary_skip_plot: bool,
    paper_resting_fill_enabled: bool,
    paper_order_timeout_secs: u64,
    paper_order_ack_delay_ms: u64,
    paper_queue_depth_levels: usize,
    paper_requote_stale_ms: u64,
    paper_min_requote_interval_ms: u64,
    paper_requote_price_delta_ticks: u64,
    paper_requote_progress_retain: f64,
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
    round_leg1_entered_ts: u64,
    round_leg2_entered_ts: u64,
    round_leg2_anchor_price: Option<f64>,
    round_leg1_target_qty: f64,
    round_leg1_filled_qty: f64,
    round_leg2_target_qty: f64,
    round_leg2_filled_qty: f64,
    last_apply_ts_ms: u64,
    last_round_complete_ts: u64,
    lock_reopen_used_rounds: u64,
    best_bid_up: Option<f64>,
    best_ask_up: Option<f64>,
    best_bid_down: Option<f64>,
    best_ask_down: Option<f64>,
    exchange_ts_ms_up: Option<i64>,
    recv_ts_ms_up: Option<i64>,
    latency_ms_up: Option<i64>,
    exchange_ts_ms_down: Option<i64>,
    recv_ts_ms_down: Option<i64>,
    latency_ms_down: Option<i64>,
    best_bid_size_up: Option<f64>,
    best_ask_size_up: Option<f64>,
    best_bid_size_down: Option<f64>,
    best_ask_size_down: Option<f64>,
    bid_consumption_rate_up: f64,
    ask_consumption_rate_up: f64,
    bid_consumption_rate_down: f64,
    ask_consumption_rate_down: f64,
    prev_mid_up: Option<f64>,
    prev_mid_down: Option<f64>,
    mid_up_fast_ema: Option<f64>,
    mid_down_fast_ema: Option<f64>,
    mid_up_slow_ema: Option<f64>,
    mid_down_slow_ema: Option<f64>,
    pair_mid_vol_bps: f64,
    mid_up_momentum_bps: f64,
    mid_down_momentum_bps: f64,
    mid_up_discount_bps: f64,
    mid_down_discount_bps: f64,
    last_decision_ts_ms: u64,
    decision_seq: u64,
    entry_worst_pair_deny_streak: u64,
    entry_worst_pair_streak_started_ts: u64,
    entry_fallback_until_ts: u64,
    open_orders_active: u64,
    locked: bool,
    #[allow(dead_code)]
    locked_hedgeable: f64,
    #[allow(dead_code)]
    locked_pair_cost: Option<f64>,
    #[allow(dead_code)]
    locked_at_ts_ms: u64,
    open_count_window: u64,
    fill_count_window: u64,
    timeout_extend_count_window: u64,
    requote_count_window: u64,
    waiting_skip_count_window: u64,
    risk_guard_cancel_count_window: u64,
    risk_guard_cancel_pair_cap_count_window: u64,
    risk_guard_cancel_other_count_window: u64,
    resting_hard_risk_cancel_count_window: u64,
    resting_soft_recheck_cancel_count_window: u64,
    stale_cancel_count_window: u64,
    close_window_cancel_count_window: u64,
    entry_worst_pair_block_count_window: u64,
    unrecoverable_block_count_window: u64,
    cancel_unrecoverable_count_window: u64,
    max_executed_sim_pair_cost_window: Option<f64>,
    tail_pair_cap_block_count_window: u64,
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
    vol_entry_bps: f64,
    vol_entry_ok: bool,
    reversal_up_ok: bool,
    reversal_down_ok: bool,
    turn_up_ok: bool,
    turn_down_ok: bool,
    first_leg_turning_score: Option<f64>,
    entry_worst_pair_cost: Option<f64>,
    entry_worst_pair_ok: bool,
    entry_timeout_flow_ratio: Option<f64>,
    entry_timeout_flow_ok: bool,
    entry_fillability_ok: bool,
    entry_edge_bps: Option<f64>,
    entry_regime_score: Option<f64>,
    entry_depth_cap_qty: Option<f64>,
    entry_flow_cap_qty: Option<f64>,
    slice_count_planned: Option<u32>,
    slice_qty_current: Option<f64>,
    entry_final_qty_slice: Option<f64>,
    entry_fallback_active: bool,
    entry_fallback_armed: bool,
    entry_fallback_trigger_reason: Option<String>,
    entry_fallback_blocked_by_recoverability: bool,
    new_round_cutoff_secs: u64,
    late_new_round_blocked: bool,
    pair_quality_ok: bool,
    pair_regression_ok: bool,
    can_open_round_base_ok: bool,
    can_start_block_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecutionMode {
    Paper,
    LiveShadow,
    Live,
}

impl ExecutionMode {
    fn as_str(self) -> &'static str {
        match self {
            ExecutionMode::Paper => "paper",
            ExecutionMode::LiveShadow => "live_shadow",
            ExecutionMode::Live => "live",
        }
    }
}

struct ExecutionRuntime {
    mode: ExecutionMode,
    adapter_name: String,
    user_ws_enabled: bool,
    sim_fill_enabled: bool,
    adapter: Box<dyn ExecutionAdapter>,
}

#[derive(Debug, Clone)]
struct PaperRestingOrder {
    order_id: String,
    client_order_id: String,
    token_id: String,
    leg: TradeLeg,
    side: TradeSide,
    price: f64,
    remaining_qty: f64,
    queue_ahead: f64,
    consumed_qty: f64,
    fill_draw: f64,
    placed_ts_ms: u64,
    last_requote_ts_ms: u64,
    active_after_ts_ms: u64,
    last_eval_ts_ms: u64,
    horizon_secs: u64,
    timeout_extend_count: u64,
    first_unrecoverable_detected_ts_ms: Option<u64>,
    first_margin_fail_detected_ts_ms: Option<u64>,
}

#[derive(Debug, Clone)]
enum UserWsEvent {
    Status(serde_json::Map<String, Value>),
    Error(serde_json::Map<String, Value>),
    Fill(UserWsFillEvent),
    Order(serde_json::Map<String, Value>),
}

#[derive(Debug, Clone)]
struct UserWsFillEvent {
    token_id: String,
    side: TradeSide,
    price: f64,
    size: f64,
    raw_type: String,
    order_id: Option<String>,
    client_order_id: Option<String>,
    market_slug: Option<String>,
    ts_ms: i64,
}

pub async fn run(config: &Config) -> Result<()> {
    let ws_host = get_required("WS_HOST")?;
    let ws_path = get_required("WS_PATH")?;
    let ws_url = format!("{ws_host}{ws_path}");
    tracing::info!(ws_url = %ws_url, "ws url");
    let clob_host = config.clob_host.clone();
    let execution_mode = parse_execution_mode(&config.execution_mode)?;
    let execution_adapter = config.execution_adapter.clone();
    let user_ws_enabled = config.user_ws_enabled;
    let sim_fill_enabled = read_sim_fill();
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
            execution_mode,
            &execution_adapter,
            user_ws_enabled,
            sim_fill_enabled,
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
    execution_mode: ExecutionMode,
    execution_adapter: &str,
    user_ws_enabled: bool,
    sim_fill_enabled: bool,
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
    let mut execution_runtime = build_execution_runtime(
        execution_mode,
        execution_adapter,
        clob_host,
        user_ws_enabled,
        sim_fill_enabled,
    );
    let (user_ws_tx, mut user_ws_rx) = mpsc::unbounded_channel::<UserWsEvent>();
    let mut user_ws_task: Option<tokio::task::JoinHandle<()>> = None;
    if execution_runtime.user_ws_enabled {
        if let Some(user_ws_url) = read_user_ws_url() {
            user_ws_task = Some(spawn_user_ws_loop(
                user_ws_url.clone(),
                read_user_ws_subscribe_payload(),
                user_ws_tx.clone(),
            ));
            tracing::info!(user_ws_url = %user_ws_url, "user ws loop started");
        } else {
            tracing::warn!(
                "USER_WS_ENABLED=true but USER_WS_URL/WS_HOST missing; user ws disabled"
            );
            execution_runtime.user_ws_enabled = false;
        }
    }
    tracing::info!(
        execution_mode = execution_runtime.mode.as_str(),
        execution_adapter = %execution_runtime.adapter_name,
        user_ws_enabled = execution_runtime.user_ws_enabled,
        sim_fill_enabled = execution_runtime.sim_fill_enabled,
        "execution runtime initialized"
    );
    let ws_loop_tick_ms = read_ws_loop_tick_ms();
    let quote_log_every_ms = read_quote_log_every_ms();
    let ws_book_depth_levels = read_ws_book_depth_levels();
    tracing::info!(
        ws_loop_tick_ms,
        quote_log_every_ms,
        ws_book_depth_levels,
        "ws runtime tuning"
    );
    let mut ticker = tokio::time::interval(Duration::from_millis(ws_loop_tick_ms));
    let mut price_change_stats = PriceChangeStats::default();
    let mut update_stats = QuoteUpdateStats::default();
    let mut last_quote_log_ms: u64 = 0;
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
    let mut paper_resting_orders: HashMap<String, PaperRestingOrder> = HashMap::new();
    let mut paper_requote_progress: HashMap<String, f64> = HashMap::new();
    let mut paper_requote_fill_draw: HashMap<String, f64> = HashMap::new();

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
    {
        let log_ctx = RolloverLogContext {
            enabled: rollover_log_json,
            verbose: rollover_log_verbose,
            series_slug: series_slug_value.clone(),
            rollover_gen: rollover_generation,
        };
        log_user_ws_status(&execution_runtime, &log_ctx, "connected");
    }

    'ws_loop: loop {
        tokio::select! {
            _ = ticker.tick() => {
                let selection_snapshot = current_selection.clone();
                let now_ms = now_ts_ms();
                if last_quote_log_ms == 0
                    || now_ms.saturating_sub(last_quote_log_ms) >= quote_log_every_ms
                {
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
                    last_quote_log_ms = now_ms;
                }
                if should_run_dryrun(&dryrun, now_ms, dryrun_params.decision_every_ms) {
                    dryrun.last_decision_ts_ms = now_ms;
                    dryrun.decision_seq = dryrun.decision_seq.saturating_add(1);
                    dryrun.update_from_cache(&cache);
                    dryrun.update_pair_mid_vol_bps(
                        dryrun_params.vol_entry_lookback_ticks,
                        dryrun_params.reversal_fast_ema_ticks,
                        dryrun_params.reversal_slow_ema_ticks,
                    );
                    let now_ts = Utc::now().timestamp();
                    let now_ts = u64::try_from(now_ts).unwrap_or(0);
                    let log_ctx = RolloverLogContext {
                        enabled: rollover_log_json,
                        verbose: rollover_log_verbose,
                        series_slug: series_slug_value.clone(),
                        rollover_gen: rollover_generation,
                    };
                    process_paper_resting_fills(
                        dryrun,
                        &dryrun_params,
                        now_ts,
                        now_ms,
                        &mut execution_runtime,
                        &mut paper_resting_orders,
                        &mut paper_requote_progress,
                        &mut paper_requote_fill_draw,
                        &log_ctx,
                    )
                    .await;
                    dryrun.open_orders_active = paper_resting_orders.len() as u64;
                    dryrun.update_round_state(&dryrun_params, now_ts);
                    dryrun.update_lock(&dryrun_params, now_ms);
                    log_dryrun_snapshot(&dryrun, &dryrun_params, &log_ctx);
                    let candidates = build_dryrun_candidates(&dryrun, &dryrun_params);
                    let best_any = select_best_candidate(&candidates, |_| true);
                    update_entry_fallback_state(
                        dryrun,
                        &dryrun_params,
                        now_ts,
                        &candidates,
                        &best_any,
                    );
                    let execute_enabled = dryrun_params.mode == DryRunMode::Paper
                        || execution_runtime.mode != ExecutionMode::Paper;
                    let applied = if execute_enabled {
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
                    let time_left_secs = if dryrun.end_ts > now_ts {
                        dryrun.end_ts - now_ts
                    } else {
                        0
                    };
                    let tail_mode = if time_left_secs <= dryrun_params.tail_close_secs {
                        "close"
                    } else if time_left_secs <= dryrun_params.tail_freeze_secs {
                        "freeze"
                    } else {
                        "none"
                    };
                    if let Some(applied) = applied {
                        if applied.gate.allow && applied.sim.ok {
                            execute_candidate_action(
                                dryrun,
                                &applied,
                                &dryrun_params,
                                now_ts,
                                now_ms,
                                &mut execution_runtime,
                                &log_ctx,
                                &mut paper_resting_orders,
                                &mut paper_requote_progress,
                                &mut paper_requote_fill_draw,
                            )
                            .await;
                        } else if tail_mode != "none" {
                            log_live_skip(
                                &dryrun,
                                "gate_denied_or_sim_failed",
                                tail_mode,
                                &execution_runtime,
                                &log_ctx,
                            );
                        }
                    } else if tail_mode != "none" {
                        log_live_skip(
                            &dryrun,
                            "no_candidate",
                            tail_mode,
                            &execution_runtime,
                            &log_ctx,
                        );
                    }
                }
            }
            Some(user_event) = user_ws_rx.recv(), if execution_runtime.user_ws_enabled => {
                handle_user_ws_event(
                    dryrun,
                    &dryrun_params,
                    &execution_runtime,
                    user_event,
                    rollover_log_json,
                    rollover_log_verbose,
                    &series_slug_value,
                    rollover_generation,
                );
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
                                if let Some(old_market_slug) =
                                    old_slug.as_deref().filter(|slug| !slug.is_empty())
                                {
                                    let cancel_result =
                                        execution_runtime.adapter.cancel_all(old_market_slug).await;
                                    log_live_cancel(
                                        old_market_slug,
                                        dryrun.decision_seq,
                                        "cancel_all",
                                        &format!("cancel-all-{}", old_market_slug),
                                        &format!("cancel-all-{}", old_market_slug),
                                        None,
                                        None,
                                        None,
                                        &execution_runtime,
                                        &cancel_result,
                                        &log_ctx,
                                    );
                                    if let Err(err) = cancel_result {
                                        tracing::warn!(
                                            market_slug = %old_market_slug,
                                            error = %err,
                                            "cancel_all failed during rollover"
                                        );
                                    }
                                }
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
                            ws_book_depth_levels,
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

    {
        let log_ctx = RolloverLogContext {
            enabled: rollover_log_json,
            verbose: rollover_log_verbose,
            series_slug: series_slug_value.clone(),
            rollover_gen: rollover_generation,
        };
        log_user_ws_status(&execution_runtime, &log_ctx, "disconnected");
    }
    if let Some(task) = user_ws_task.take() {
        task.abort();
    }
    drop(sender);
    drop(user_ws_tx);
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

fn read_ws_loop_tick_ms() -> u64 {
    match env::var("WS_LOOP_TICK_MS") {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|v| *v >= 10)
            .unwrap_or(100),
        Err(_) => 100,
    }
}

fn read_quote_log_every_ms() -> u64 {
    match env::var("QUOTE_LOG_EVERY_MS") {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|v| *v >= 100)
            .unwrap_or(1_000),
        Err(_) => 1_000,
    }
}

fn read_ws_book_depth_levels() -> usize {
    match env::var("WS_BOOK_DEPTH_LEVELS") {
        Ok(value) => value
            .parse::<usize>()
            .ok()
            .filter(|v| *v > 0 && *v <= 100)
            .unwrap_or(10),
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

fn read_sim_fill() -> bool {
    match env::var("SIM_FILL") {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes"
        ),
        Err(_) => false,
    }
}

fn read_user_ws_url() -> Option<String> {
    if let Ok(url) = env::var("USER_WS_URL") {
        let trimmed = url.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    let ws_host = env::var("WS_HOST").ok()?;
    let ws_host = ws_host.trim().trim_end_matches('/');
    if ws_host.is_empty() {
        return None;
    }
    let user_ws_path = env::var("USER_WS_PATH").unwrap_or_else(|_| "/ws/user".to_string());
    let path = if user_ws_path.starts_with('/') {
        user_ws_path
    } else {
        format!("/{user_ws_path}")
    };
    Some(format!("{ws_host}{path}"))
}

fn read_user_ws_subscribe_payload() -> Option<String> {
    env::var("USER_WS_SUBSCRIBE_PAYLOAD")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn user_ws_default_subscribe_payload() -> String {
    json!({
        "type": "subscribe",
        "channel": "user"
    })
    .to_string()
}

fn map_get_str<'a>(map: &'a serde_json::Map<String, Value>, keys: &[&str]) -> Option<&'a str> {
    for key in keys {
        if let Some(v) = map.get(*key).and_then(Value::as_str) {
            return Some(v);
        }
    }
    None
}

fn map_get_num(map: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<f64> {
    for key in keys {
        if let Some(value) = map.get(*key) {
            match value {
                Value::Number(v) => {
                    if let Some(num) = v.as_f64() {
                        return Some(num);
                    }
                }
                Value::String(v) => {
                    if let Ok(num) = v.parse::<f64>() {
                        return Some(num);
                    }
                }
                _ => {}
            }
        }
    }
    None
}

fn parse_trade_side_str(raw: &str) -> Option<TradeSide> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "buy" | "b" => Some(TradeSide::Buy),
        "sell" | "s" => Some(TradeSide::Sell),
        _ => None,
    }
}

fn parse_user_ws_object(map: &serde_json::Map<String, Value>, now_ms: i64) -> Vec<UserWsEvent> {
    let mut events = Vec::new();
    let raw_type = map_get_str(map, &["type", "event", "channel", "raw_type"])
        .unwrap_or("unknown")
        .to_string();
    let raw_type_lc = raw_type.to_ascii_lowercase();

    let token_id =
        map_get_str(map, &["token_id", "asset_id", "token", "tokenId"]).map(str::to_string);
    let side =
        map_get_str(map, &["side", "taker_side", "order_side"]).and_then(parse_trade_side_str);
    let price = map_get_num(
        map,
        &["price", "filled_price", "avg_price", "execution_price"],
    );
    let size = map_get_num(map, &["size", "qty", "filled_size", "amount"]);
    let order_id = map_get_str(map, &["order_id", "orderId", "id"]).map(str::to_string);
    let client_order_id =
        map_get_str(map, &["client_order_id", "clientOrderId"]).map(str::to_string);
    let market_slug =
        map_get_str(map, &["market_slug", "market", "marketSlug"]).map(str::to_string);
    let ts_ms = map_get_num(map, &["ts_ms", "timestamp_ms", "timestamp"])
        .map(|v| v as i64)
        .unwrap_or(now_ms);

    let looks_fill = raw_type_lc.contains("fill")
        || raw_type_lc.contains("trade")
        || raw_type_lc.contains("match")
        || map.contains_key("fill_id");
    if looks_fill {
        if let (Some(token_id), Some(side), Some(price), Some(size)) =
            (token_id.clone(), side, price, size)
        {
            events.push(UserWsEvent::Fill(UserWsFillEvent {
                token_id,
                side,
                price,
                size,
                raw_type: raw_type.clone(),
                order_id: order_id.clone(),
                client_order_id: client_order_id.clone(),
                market_slug: market_slug.clone(),
                ts_ms,
            }));
        }
    }

    let looks_order = raw_type_lc.contains("order")
        || map.contains_key("status")
        || map.contains_key("order_id")
        || map.contains_key("orderId");
    if looks_order {
        let mut data = map.clone();
        data.insert("raw_type".to_string(), Value::String(raw_type));
        if let Some(ts) = data.get("ts_ms").cloned() {
            data.insert("ts_ms".to_string(), ts);
        } else {
            data.insert("ts_ms".to_string(), Value::from(ts_ms));
        }
        events.push(UserWsEvent::Order(data));
    }

    events
}

fn parse_user_ws_message(text: &str, now_ms: i64) -> Vec<UserWsEvent> {
    let value: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    match value {
        Value::Object(map) => parse_user_ws_object(&map, now_ms),
        Value::Array(items) => {
            let mut out = Vec::new();
            for item in items {
                if let Value::Object(map) = item {
                    out.extend(parse_user_ws_object(&map, now_ms));
                }
            }
            out
        }
        _ => Vec::new(),
    }
}

fn make_user_ws_status_data(
    conn_id: &str,
    state: &str,
    attempt: u64,
    backoff_secs: u64,
    since_connected_ms: Option<i64>,
    err_kind: Option<&str>,
    message: Option<String>,
) -> serde_json::Map<String, Value> {
    let ts_ms = now_ts_ms_i64();
    let mut data = serde_json::Map::new();
    data.insert("conn_id".to_string(), Value::String(conn_id.to_string()));
    data.insert("state".to_string(), Value::String(state.to_string()));
    data.insert("attempt".to_string(), Value::from(attempt));
    data.insert("backoff_secs".to_string(), Value::from(backoff_secs));
    data.insert("ts_ms".to_string(), Value::from(ts_ms));
    data.insert(
        "since_connected_ms".to_string(),
        since_connected_ms.map(Value::from).unwrap_or(Value::Null),
    );
    data.insert(
        "err_kind".to_string(),
        err_kind
            .map(|s| Value::String(s.to_string()))
            .unwrap_or(Value::Null),
    );
    data.insert(
        "message".to_string(),
        message.map(Value::String).unwrap_or(Value::Null),
    );
    data
}

fn send_user_ws_status(
    tx: &mpsc::UnboundedSender<UserWsEvent>,
    conn_id: &str,
    state: &str,
    attempt: u64,
    backoff_secs: u64,
    since_connected_ms: Option<i64>,
    err_kind: Option<&str>,
    message: Option<String>,
) {
    let data = make_user_ws_status_data(
        conn_id,
        state,
        attempt,
        backoff_secs,
        since_connected_ms,
        err_kind,
        message,
    );
    let _ = tx.send(UserWsEvent::Status(data));
}

fn send_user_ws_error(
    tx: &mpsc::UnboundedSender<UserWsEvent>,
    conn_id: &str,
    attempt: u64,
    backoff_secs: u64,
    err_kind: &str,
    message: String,
) {
    let data = make_user_ws_status_data(
        conn_id,
        "error",
        attempt,
        backoff_secs,
        None,
        Some(err_kind),
        Some(message),
    );
    let _ = tx.send(UserWsEvent::Error(data));
}

fn spawn_user_ws_loop(
    user_ws_url: String,
    subscribe_payload: Option<String>,
    tx: mpsc::UnboundedSender<UserWsEvent>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut attempt: u64 = 0;
        let mut backoff_secs: u64 = 1;
        let mut conn_seq: u64 = 0;
        loop {
            attempt = attempt.saturating_add(1);
            conn_seq = conn_seq.saturating_add(1);
            let conn_id = format!("userws-{}-{}", now_ts_ms_i64(), conn_seq);
            send_user_ws_status(
                &tx,
                &conn_id,
                "connecting",
                attempt,
                backoff_secs,
                None,
                None,
                None,
            );

            match tokio_tungstenite::connect_async(&user_ws_url).await {
                Ok((ws_stream, _resp)) => {
                    send_user_ws_status(
                        &tx,
                        &conn_id,
                        "connected",
                        attempt,
                        backoff_secs,
                        Some(0),
                        None,
                        None,
                    );
                    backoff_secs = 1;
                    let connected_at_ms = now_ts_ms_i64();
                    let (mut write, mut read) = ws_stream.split();
                    let payload = subscribe_payload
                        .clone()
                        .unwrap_or_else(user_ws_default_subscribe_payload);
                    if let Err(err) = write.send(Message::Text(payload)).await {
                        send_user_ws_error(
                            &tx,
                            &conn_id,
                            attempt,
                            backoff_secs,
                            "send_subscribe",
                            err.to_string(),
                        );
                    } else {
                        send_user_ws_status(
                            &tx,
                            &conn_id,
                            "subscribed",
                            attempt,
                            backoff_secs,
                            Some(now_ts_ms_i64().saturating_sub(connected_at_ms)),
                            None,
                            None,
                        );
                    }

                    let mut got_first_msg = false;
                    loop {
                        match read.next().await {
                            Some(Ok(Message::Text(text))) => {
                                if !got_first_msg {
                                    got_first_msg = true;
                                    send_user_ws_status(
                                        &tx,
                                        &conn_id,
                                        "recv_first_msg",
                                        attempt,
                                        backoff_secs,
                                        Some(now_ts_ms_i64().saturating_sub(connected_at_ms)),
                                        None,
                                        None,
                                    );
                                }
                                let events = parse_user_ws_message(&text, now_ts_ms_i64());
                                for event in events {
                                    let _ = tx.send(event);
                                }
                            }
                            Some(Ok(Message::Binary(_))) => {}
                            Some(Ok(Message::Ping(_))) => {}
                            Some(Ok(Message::Pong(_))) => {}
                            Some(Ok(Message::Close(frame))) => {
                                send_user_ws_status(
                                    &tx,
                                    &conn_id,
                                    "disconnected",
                                    attempt,
                                    backoff_secs,
                                    Some(now_ts_ms_i64().saturating_sub(connected_at_ms)),
                                    Some("close_frame"),
                                    frame.map(|f| f.reason.to_string()),
                                );
                                break;
                            }
                            Some(Ok(Message::Frame(_))) => {}
                            Some(Err(err)) => {
                                send_user_ws_error(
                                    &tx,
                                    &conn_id,
                                    attempt,
                                    backoff_secs,
                                    "read_error",
                                    err.to_string(),
                                );
                                send_user_ws_status(
                                    &tx,
                                    &conn_id,
                                    "disconnected",
                                    attempt,
                                    backoff_secs,
                                    Some(now_ts_ms_i64().saturating_sub(connected_at_ms)),
                                    Some("read_error"),
                                    Some(err.to_string()),
                                );
                                break;
                            }
                            None => {
                                send_user_ws_status(
                                    &tx,
                                    &conn_id,
                                    "disconnected",
                                    attempt,
                                    backoff_secs,
                                    Some(now_ts_ms_i64().saturating_sub(connected_at_ms)),
                                    Some("stream_eof"),
                                    None,
                                );
                                break;
                            }
                        }
                    }
                }
                Err(err) => {
                    send_user_ws_error(
                        &tx,
                        &conn_id,
                        attempt,
                        backoff_secs,
                        "connect_error",
                        err.to_string(),
                    );
                    send_user_ws_status(
                        &tx,
                        &conn_id,
                        "disconnected",
                        attempt,
                        backoff_secs,
                        None,
                        Some("connect_error"),
                        Some(err.to_string()),
                    );
                }
            }
            tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
            backoff_secs = (backoff_secs * 2).min(30);
        }
    })
}

fn parse_execution_mode(raw: &str) -> Result<ExecutionMode> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "paper" => Ok(ExecutionMode::Paper),
        "live_shadow" => Ok(ExecutionMode::LiveShadow),
        "live" => Ok(ExecutionMode::Live),
        other => bail!(
            "invalid execution mode '{}'; expected paper/live_shadow/live",
            other
        ),
    }
}

fn build_execution_runtime(
    mode: ExecutionMode,
    adapter_name: &str,
    clob_host: &str,
    user_ws_enabled: bool,
    sim_fill_enabled: bool,
) -> ExecutionRuntime {
    let requested_adapter = adapter_name.trim().to_ascii_lowercase();
    let (normalized_adapter, adapter): (String, Box<dyn ExecutionAdapter>) = match mode {
        ExecutionMode::LiveShadow => {
            if requested_adapter != "paper" {
                tracing::warn!(
                    mode = mode.as_str(),
                    requested_adapter = %requested_adapter,
                    "live_shadow forces paper adapter to avoid placing real orders"
                );
            }
            (
                "paper".to_string(),
                Box::new(PaperExecutionAdapter::default()),
            )
        }
        ExecutionMode::Paper => (
            "paper".to_string(),
            Box::new(PaperExecutionAdapter::default()),
        ),
        ExecutionMode::Live => match requested_adapter.as_str() {
            "clob" => (
                "clob".to_string(),
                Box::new(ClobExecutionAdapter::from_env(clob_host.to_string())),
            ),
            "paper" => (
                "paper".to_string(),
                Box::new(PaperExecutionAdapter::default()),
            ),
            other => {
                tracing::warn!(
                    adapter = %other,
                    "unknown EXECUTION_ADAPTER; fallback to paper"
                );
                (
                    "paper".to_string(),
                    Box::new(PaperExecutionAdapter::default()),
                )
            }
        },
    };
    ExecutionRuntime {
        mode,
        adapter_name: normalized_adapter,
        user_ws_enabled,
        sim_fill_enabled,
        adapter,
    }
}

fn read_dryrun_params() -> DryRunParams {
    let params = strategy::params::StrategyParams::from_env();
    let paper_resting_fill_enabled = match env::var("PAPER_RESTING_FILL_ENABLED") {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => true,
    };
    let paper_order_timeout_secs = env::var("PAPER_ORDER_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(params.maker_fill_horizon_secs.max(5));
    let paper_order_ack_delay_ms = env::var("PAPER_ORDER_ACK_DELAY_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    let paper_queue_depth_levels = env::var("PAPER_QUEUE_DEPTH_LEVELS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1);
    let paper_requote_stale_ms = env::var("PAPER_REQUOTE_STALE_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1500);
    let paper_min_requote_interval_ms = env::var("PAPER_MIN_REQUOTE_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(3000);
    let paper_requote_price_delta_ticks = env::var("PAPER_REQUOTE_PRICE_DELTA_TICKS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(2);
    let paper_requote_require_price_move = match env::var("PAPER_REQUOTE_REQUIRE_PRICE_MOVE") {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => true,
    };
    let paper_requote_stale_ms_hard = env::var("PAPER_REQUOTE_STALE_MS_HARD")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(5000);
    let paper_requote_progress_retain = env::var("PAPER_REQUOTE_PROGRESS_RETAIN")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| v.is_finite() && *v >= 0.0 && *v <= 1.0)
        .unwrap_or(0.35);
    if params.total_budget_source == "MAX_NET_INVEST_USDC" {
        tracing::warn!(
            total_budget_usdc = params.total_budget_usdc,
            "MAX_NET_INVEST_USDC is deprecated; use TOTAL_BUDGET_USDC"
        );
    }

    tracing::info!(
        total_budget_usdc = params.total_budget_usdc,
        total_budget_source = %params.total_budget_source,
        round_budget_usdc = params.round_budget_usdc,
        round_budget_source = %params.round_budget_source,
        lock_strict_abs_net = params.lock_strict_abs_net,
        round_budget_strict = params.round_budget_strict,
        lock_min_completed_rounds = params.lock_min_completed_rounds,
        lock_min_time_left_secs = params.lock_min_time_left_secs,
        lock_min_spent_ratio = params.lock_min_spent_ratio,
        lock_force_time_left_secs = params.lock_force_time_left_secs,
        tail_close_ignore_margin = params.tail_close_ignore_margin,
        vol_entry_min_bps = params.vol_entry_min_bps,
        vol_entry_lookback_ticks = params.vol_entry_lookback_ticks,
        reversal_entry_enabled = params.reversal_entry_enabled,
        reversal_min_discount_bps = params.reversal_min_discount_bps,
        reversal_min_momentum_bps = params.reversal_min_momentum_bps,
        reversal_fast_ema_ticks = params.reversal_fast_ema_ticks,
        reversal_slow_ema_ticks = params.reversal_slow_ema_ticks,
        entry_turn_confirm_ticks = params.entry_turn_confirm_ticks,
        entry_turn_min_rebound_bps = params.entry_turn_min_rebound_bps,
        entry_pair_buffer_bps = params.entry_pair_buffer_bps,
        round_pair_wait_secs = params.round_pair_wait_secs,
        leg2_rebalance_discount_bps = params.leg2_rebalance_discount_bps,
        force_pair_max_pair_cost = params.force_pair_max_pair_cost,
        no_risk_hard_pair_cap = params.no_risk_hard_pair_cap,
        no_risk_pair_limit_mode = params.no_risk_pair_limit_mode.as_str(),
        no_risk_enforce_tail = params.no_risk_enforce_tail,
        no_risk_entry_hedge_price_mode = params.no_risk_entry_hedge_price_mode.as_str(),
        no_risk_entry_ask_slippage_bps = params.no_risk_entry_ask_slippage_bps,
        no_risk_entry_worst_hedge_bps = params.no_risk_entry_worst_hedge_bps,
        no_risk_entry_pair_headroom_bps = params.no_risk_entry_pair_headroom_bps,
        entry_dynamic_cap_enabled = params.entry_dynamic_cap_enabled,
        entry_dynamic_cap_headroom_bps = params.entry_dynamic_cap_headroom_bps,
        entry_dynamic_cap_min_price = params.entry_dynamic_cap_min_price,
        entry_dynamic_cap_apply_to_net_increase_only = params.entry_dynamic_cap_apply_to_net_increase_only,
        no_risk_late_new_round_buffer_secs = params.no_risk_late_new_round_buffer_secs,
        no_risk_require_completable_round = params.no_risk_require_completable_round,
        no_risk_strict_zero_unmatched = params.no_risk_strict_zero_unmatched,
        no_risk_hedge_recoverability_enforce = params.no_risk_hedge_recoverability_enforce,
        no_risk_hedge_recoverability_eps_bps = params.no_risk_hedge_recoverability_eps_bps,
        open_order_risk_guard = params.open_order_risk_guard,
        open_order_risk_buffer_bps = params.open_order_risk_buffer_bps,
        open_order_risk_guard_require_paired = params.open_order_risk_guard_require_paired,
        open_order_max_age_secs = params.open_order_max_age_secs,
        round_slice_count = params.round_slice_count,
        round_dynamic_slicing_enabled = params.round_dynamic_slicing_enabled,
        round_min_slices = params.round_min_slices,
        round_max_slices = params.round_max_slices,
        round_min_slice_qty = params.round_min_slice_qty,
        entry_pair_regression_mode = params.entry_pair_regression_mode.as_str(),
        entry_pair_regression_soft_band_bps = params.entry_pair_regression_soft_band_bps,
        entry_edge_min_bps = params.entry_edge_min_bps,
        entry_fill_prob_min = params.entry_fill_prob_min,
        inventory_skew_alpha_bps = params.inventory_skew_alpha_bps,
        lock_allow_reopen_before_freeze = params.lock_allow_reopen_before_freeze,
        lock_reopen_max_rounds = params.lock_reopen_max_rounds,
        round_min_start_gap_secs = params.round_min_start_gap_secs,
        opening_no_trade_secs = params.opening_no_trade_secs,
        min_apply_interval_ms = params.min_apply_interval_ms,
        maker_fill_estimator_enabled = params.maker_fill_estimator_enabled,
        maker_fill_horizon_secs = params.maker_fill_horizon_secs,
        maker_min_fill_prob = params.maker_min_fill_prob,
        open_min_fill_prob = params.open_min_fill_prob,
        maker_queue_ahead_mult = params.maker_queue_ahead_mult,
        maker_flow_floor_per_sec = params.maker_flow_floor_per_sec,
        requote_min_fill_prob_uplift = params.requote_min_fill_prob_uplift,
        requote_queue_stickiness_ratio = params.requote_queue_stickiness_ratio,
        requote_stickiness_min_age_secs = params.requote_stickiness_min_age_secs,
        entry_max_top_book_share = params.entry_max_top_book_share,
        entry_max_flow_utilization = params.entry_max_flow_utilization,
        entry_min_timeout_flow_ratio = params.entry_min_timeout_flow_ratio,
        entry_fallback_enabled = params.entry_fallback_enabled,
        entry_fallback_deny_streak = params.entry_fallback_deny_streak,
        entry_fallback_window_secs = params.entry_fallback_window_secs,
        entry_fallback_duration_secs = params.entry_fallback_duration_secs,
        entry_fallback_hedge_mode = params.entry_fallback_hedge_mode.as_str(),
        entry_fallback_worst_hedge_bps = params.entry_fallback_worst_hedge_bps,
        check_summary_skip_plot = params.check_summary_skip_plot,
        paper_resting_fill_enabled,
        paper_order_timeout_secs,
        paper_order_ack_delay_ms,
        paper_queue_depth_levels,
        paper_requote_stale_ms,
        paper_min_requote_interval_ms,
        paper_requote_price_delta_ticks,
        paper_requote_require_price_move,
        paper_requote_stale_ms_hard,
        paper_requote_retain_fill_draw = params.paper_requote_retain_fill_draw,
        paper_requote_progress_retain,
        "dryrun budget/risk config"
    );

    DryRunParams {
        improve_min: params.improve_min,
        margin_target: params.margin_target,
        safety_margin: params.safety_margin,
        total_budget_usdc: params.total_budget_usdc,
        total_budget_source: params.total_budget_source,
        max_rounds: params.max_rounds,
        round_budget_usdc: params.round_budget_usdc,
        round_budget_source: params.round_budget_source,
        round_leg1_fraction: params.round_leg1_fraction,
        max_unhedged_value: params.max_unhedged_value,
        cap_unhedged_value: params.cap_unhedged_value,
        max_unhedged_shares: params.max_unhedged_shares,
        cooldown_secs: params.cooldown_secs,
        tail_freeze_secs: params.tail_freeze_secs,
        tail_close_secs: params.tail_close_secs,
        decision_every_ms: params.decision_every_ms,
        mode: match params.mode {
            strategy::state::DryRunMode::Recommend => DryRunMode::Recommend,
            strategy::state::DryRunMode::Paper => DryRunMode::Paper,
        },
        apply_kind: match params.apply_kind {
            strategy::state::ApplyKind::BestAction => ApplyKind::BestAction,
            strategy::state::ApplyKind::BestMakerOnly => ApplyKind::BestMakerOnly,
            strategy::state::ApplyKind::BestTakerOnly => ApplyKind::BestTakerOnly,
        },
        pair_bonus: params.pair_bonus,
        lock_strict_abs_net: params.lock_strict_abs_net,
        round_budget_strict: params.round_budget_strict,
        lock_min_completed_rounds: params.lock_min_completed_rounds,
        lock_min_time_left_secs: params.lock_min_time_left_secs,
        lock_min_spent_ratio: params.lock_min_spent_ratio,
        lock_force_time_left_secs: params.lock_force_time_left_secs,
        tail_close_ignore_margin: params.tail_close_ignore_margin,
        vol_entry_min_bps: params.vol_entry_min_bps,
        vol_entry_lookback_ticks: params.vol_entry_lookback_ticks,
        reversal_entry_enabled: params.reversal_entry_enabled,
        reversal_min_discount_bps: params.reversal_min_discount_bps,
        reversal_min_momentum_bps: params.reversal_min_momentum_bps,
        reversal_fast_ema_ticks: params.reversal_fast_ema_ticks,
        reversal_slow_ema_ticks: params.reversal_slow_ema_ticks,
        entry_turn_confirm_ticks: params.entry_turn_confirm_ticks,
        entry_turn_min_rebound_bps: params.entry_turn_min_rebound_bps,
        entry_pair_buffer_bps: params.entry_pair_buffer_bps,
        round_pair_wait_secs: params.round_pair_wait_secs,
        leg2_rebalance_discount_bps: params.leg2_rebalance_discount_bps,
        force_pair_max_pair_cost: params.force_pair_max_pair_cost,
        no_risk_hard_pair_cap: params.no_risk_hard_pair_cap,
        no_risk_pair_limit_mode: params.no_risk_pair_limit_mode,
        no_risk_enforce_tail: params.no_risk_enforce_tail,
        no_risk_entry_hedge_price_mode: params.no_risk_entry_hedge_price_mode,
        no_risk_entry_ask_slippage_bps: params.no_risk_entry_ask_slippage_bps,
        no_risk_entry_worst_hedge_bps: params.no_risk_entry_worst_hedge_bps,
        no_risk_entry_pair_headroom_bps: params.no_risk_entry_pair_headroom_bps,
        entry_dynamic_cap_enabled: params.entry_dynamic_cap_enabled,
        entry_dynamic_cap_headroom_bps: params.entry_dynamic_cap_headroom_bps,
        entry_dynamic_cap_min_price: params.entry_dynamic_cap_min_price,
        entry_dynamic_cap_apply_to_net_increase_only: params
            .entry_dynamic_cap_apply_to_net_increase_only,
        entry_target_margin_min_ticks: params.entry_target_margin_min_ticks,
        entry_target_margin_min_bps: params.entry_target_margin_min_bps,
        entry_max_passive_ticks_for_net_increase: params.entry_max_passive_ticks_for_net_increase,
        no_risk_late_new_round_buffer_secs: params.no_risk_late_new_round_buffer_secs,
        no_risk_require_completable_round: params.no_risk_require_completable_round,
        no_risk_strict_zero_unmatched: params.no_risk_strict_zero_unmatched,
        no_risk_hedge_recoverability_enforce: params.no_risk_hedge_recoverability_enforce,
        no_risk_hedge_recoverability_eps_bps: params.no_risk_hedge_recoverability_eps_bps,
        hedge_recoverability_margin_enforce: params.hedge_recoverability_margin_enforce,
        hedge_recoverability_margin_min_ticks: params.hedge_recoverability_margin_min_ticks,
        hedge_recoverability_margin_min_bps: params.hedge_recoverability_margin_min_bps,
        hedge_recoverability_margin_apply_to_net_increase_only: params
            .hedge_recoverability_margin_apply_to_net_increase_only,
        open_order_risk_guard: params.open_order_risk_guard,
        open_order_risk_buffer_bps: params.open_order_risk_buffer_bps,
        open_order_risk_guard_require_paired: params.open_order_risk_guard_require_paired,
        open_order_max_age_secs: params.open_order_max_age_secs,
        open_order_unrecoverable_grace_ms: params.open_order_unrecoverable_grace_ms,
        paper_timeout_target_fill_prob: params.paper_timeout_target_fill_prob,
        paper_timeout_progress_extend_min: params.paper_timeout_progress_extend_min,
        paper_timeout_progress_extend_secs: params.paper_timeout_progress_extend_secs,
        paper_timeout_max_extends: params.paper_timeout_max_extends,
        round_slice_count: params.round_slice_count,
        round_dynamic_slicing_enabled: params.round_dynamic_slicing_enabled,
        round_min_slices: params.round_min_slices,
        round_max_slices: params.round_max_slices,
        round_min_slice_qty: params.round_min_slice_qty,
        entry_pair_regression_mode: params.entry_pair_regression_mode,
        entry_pair_regression_soft_band_bps: params.entry_pair_regression_soft_band_bps,
        entry_edge_min_bps: params.entry_edge_min_bps,
        entry_fill_prob_min: params.entry_fill_prob_min,
        open_min_fill_prob: params.open_min_fill_prob,
        open_margin_surplus_min: params.open_margin_surplus_min,
        inventory_skew_alpha_bps: params.inventory_skew_alpha_bps,
        lock_allow_reopen_before_freeze: params.lock_allow_reopen_before_freeze,
        lock_reopen_max_rounds: params.lock_reopen_max_rounds,
        round_min_start_gap_secs: params.round_min_start_gap_secs,
        opening_no_trade_secs: params.opening_no_trade_secs,
        min_apply_interval_ms: params.min_apply_interval_ms,
        maker_fill_estimator_enabled: params.maker_fill_estimator_enabled,
        maker_fill_horizon_secs: params.maker_fill_horizon_secs,
        maker_min_fill_prob: params.maker_min_fill_prob,
        maker_queue_ahead_mult: params.maker_queue_ahead_mult,
        maker_fill_passive_queue_penalty_per_tick: params.maker_fill_passive_queue_penalty_per_tick,
        maker_fill_passive_decay_k: params.maker_fill_passive_decay_k,
        maker_flow_floor_per_sec: params.maker_flow_floor_per_sec,
        entry_passive_gap_soft_max_ticks: params.entry_passive_gap_soft_max_ticks,
        entry_max_top_book_share: params.entry_max_top_book_share,
        entry_max_flow_utilization: params.entry_max_flow_utilization,
        entry_min_timeout_flow_ratio: params.entry_min_timeout_flow_ratio,
        entry_fallback_enabled: params.entry_fallback_enabled,
        entry_fallback_deny_streak: params.entry_fallback_deny_streak,
        entry_fallback_window_secs: params.entry_fallback_window_secs,
        entry_fallback_duration_secs: params.entry_fallback_duration_secs,
        entry_fallback_hedge_mode: params.entry_fallback_hedge_mode,
        entry_fallback_worst_hedge_bps: params.entry_fallback_worst_hedge_bps,
        check_summary_skip_plot: params.check_summary_skip_plot,
        paper_resting_fill_enabled,
        paper_order_timeout_secs,
        paper_order_ack_delay_ms,
        paper_queue_depth_levels,
        paper_requote_stale_ms,
        paper_min_requote_interval_ms,
        paper_requote_price_delta_ticks,
        paper_requote_require_price_move,
        paper_requote_stale_ms_hard,
        paper_requote_retain_fill_draw: params.paper_requote_retain_fill_draw,
        paper_requote_progress_retain,
        requote_min_fill_prob_uplift: params.requote_min_fill_prob_uplift,
        requote_queue_stickiness_ratio: params.requote_queue_stickiness_ratio,
        requote_stickiness_min_age_secs: params.requote_stickiness_min_age_secs,
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
    data.insert(
        "operation".to_string(),
        Value::String("subscribe".to_string()),
    );
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
    data.insert(
        "operation".to_string(),
        Value::String("unsubscribe".to_string()),
    );
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

fn build_initial_subscribe_payload_value(asset_ids: &[String], custom_features: bool) -> Value {
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
    depth_levels: usize,
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
                    depth_levels,
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
                depth_levels,
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
    depth_levels: usize,
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
                depth_levels,
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
    depth_levels: usize,
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
    let bid_levels = parse_book_levels(&bids, BookSide::Bid, depth_levels);
    let ask_levels = parse_book_levels(&asks, BookSide::Ask, depth_levels);
    let best_bid = bid_levels.first().map(|level| level.price);
    let best_ask = ask_levels.first().map(|level| level.price);
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
        Some(bid_levels),
        Some(ask_levels),
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
            None,
            None,
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
        None,
        None,
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
    bid_levels: Option<Vec<DepthLevel>>,
    ask_levels: Option<Vec<DepthLevel>>,
    stats: &mut QuoteUpdateStats,
) -> bool {
    if best_bid.is_none() && best_ask.is_none() {
        return false;
    }

    let quote = cache.entry(asset_id.to_string()).or_default();
    let now_ms = now_ts_ms_i64();
    let exchange_ts_ms = timestamp_ms.filter(|value| *value > 0);
    let recv_ts_ms = now_ms;
    let ts_ms = exchange_ts_ms.unwrap_or(recv_ts_ms);
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
                if matches!(
                    quote.source,
                    QuoteSource::BestBidAsk | QuoteSource::PriceChange
                ) && age_ms < 2_000
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
    let old_bid_size = quote.best_bid_size;
    let old_ask_size = quote.best_ask_size;
    let old_ts_ms = quote.last_update_ts_ms;

    if let Some(bid) = best_bid {
        quote.best_bid = Some(bid);
    }
    if let Some(ask) = best_ask {
        quote.best_ask = Some(ask);
    }
    if let Some(levels) = bid_levels {
        let new_bid_size = levels.first().map(|level| level.size);
        let new_bid_price = levels.first().map(|level| level.price).or(best_bid);
        if let Some(rate) = estimate_consumption_rate(
            BookSide::Bid,
            old_bid,
            old_bid_size,
            new_bid_price,
            new_bid_size,
            old_ts_ms,
            ts_ms,
            quote.bid_consumption_rate,
        ) {
            quote.bid_consumption_rate = rate;
        }
        quote.best_bid_size = new_bid_size;
        quote.bid_levels = levels;
    } else if let Some(rate) = estimate_consumption_rate(
        BookSide::Bid,
        old_bid,
        old_bid_size,
        quote.best_bid,
        quote.best_bid_size,
        old_ts_ms,
        ts_ms,
        quote.bid_consumption_rate,
    ) {
        quote.bid_consumption_rate = rate;
    }
    if let Some(levels) = ask_levels {
        let new_ask_size = levels.first().map(|level| level.size);
        let new_ask_price = levels.first().map(|level| level.price).or(best_ask);
        if let Some(rate) = estimate_consumption_rate(
            BookSide::Ask,
            old_ask,
            old_ask_size,
            new_ask_price,
            new_ask_size,
            old_ts_ms,
            ts_ms,
            quote.ask_consumption_rate,
        ) {
            quote.ask_consumption_rate = rate;
        }
        quote.best_ask_size = new_ask_size;
        quote.ask_levels = levels;
    } else if let Some(rate) = estimate_consumption_rate(
        BookSide::Ask,
        old_ask,
        old_ask_size,
        quote.best_ask,
        quote.best_ask_size,
        old_ts_ms,
        ts_ms,
        quote.ask_consumption_rate,
    ) {
        quote.ask_consumption_rate = rate;
    }
    quote.exchange_ts_ms = exchange_ts_ms;
    quote.recv_ts_ms = recv_ts_ms;
    quote.latency_ms = exchange_ts_ms.map(|exchange| (recv_ts_ms - exchange).max(0));
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
            exchange_ts_ms = ?quote.exchange_ts_ms,
            recv_ts_ms = quote.recv_ts_ms,
            latency_ms = ?quote.latency_ms,
            ts_used_ms = ts_ms,
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

fn estimate_consumption_rate(
    side: BookSide,
    old_price: Option<f64>,
    old_size: Option<f64>,
    new_price: Option<f64>,
    new_size: Option<f64>,
    old_ts_ms: i64,
    new_ts_ms: i64,
    prev_rate: f64,
) -> Option<f64> {
    let old_price = old_price?;
    let new_price = new_price?;
    let dt_ms = (new_ts_ms - old_ts_ms).max(1) as f64;
    let dt_secs = (dt_ms / 1000.0).max(DEPTH_MIN_DT_SECS);
    let same_price = (old_price - new_price).abs() <= 1e-12;
    let observed = if same_price {
        let old_size = old_size.unwrap_or(0.0);
        let new_size = new_size.unwrap_or(0.0);
        if old_size <= new_size + 1e-9 {
            return None;
        }
        clamp_consumption_rate((old_size - new_size) / dt_secs)
    } else {
        let is_depletion_move = match side {
            BookSide::Bid => new_price < old_price - 1e-12,
            BookSide::Ask => new_price > old_price + 1e-12,
        };
        if !is_depletion_move {
            return None;
        }
        let queue_size = old_size.or(new_size).unwrap_or(0.0);
        if queue_size <= 1e-9 {
            return None;
        }
        clamp_consumption_rate((queue_size * DEPTH_PRICE_MOVE_DEPLETION_RATIO) / dt_secs)
    };
    if !observed.is_finite() || observed <= 0.0 {
        return None;
    }
    let next = if prev_rate > 0.0 {
        DEPTH_CONSUMPTION_EWMA_ALPHA * observed + (1.0 - DEPTH_CONSUMPTION_EWMA_ALPHA) * prev_rate
    } else {
        observed
    };
    Some(clamp_consumption_rate(next))
}

fn clamp_consumption_rate(value: f64) -> f64 {
    value.clamp(0.0, DEPTH_PRICE_MOVE_MAX_OBSERVED_PER_SEC)
}

#[derive(Debug, Clone, Copy)]
enum BookSide {
    Bid,
    Ask,
}

fn parse_book_levels(levels: &Value, side: BookSide, max_levels: usize) -> Vec<DepthLevel> {
    let mut parsed: Vec<DepthLevel> = levels
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(parse_depth_level)
                .filter(|level| level.price.is_finite() && level.price > 0.0)
                .collect()
        })
        .unwrap_or_default();

    parsed.sort_by(|a, b| match side {
        BookSide::Bid => b.price.partial_cmp(&a.price).unwrap_or(Ordering::Equal),
        BookSide::Ask => a.price.partial_cmp(&b.price).unwrap_or(Ordering::Equal),
    });

    let mut merged: Vec<DepthLevel> = Vec::with_capacity(parsed.len());
    for level in parsed {
        if let Some(last) = merged.last_mut() {
            if (last.price - level.price).abs() < 1e-12 {
                last.size += level.size;
                continue;
            }
        }
        merged.push(level);
    }

    if merged.len() > max_levels {
        merged.truncate(max_levels);
    }
    merged
}

fn parse_depth_level(level: &Value) -> Option<DepthLevel> {
    let (price, size) = match level {
        Value::Array(values) => {
            let price = values.get(0).and_then(parse_f64_value)?;
            let size = values.get(1).and_then(parse_f64_value).unwrap_or(0.0);
            (price, size)
        }
        Value::Object(map) => {
            let price = map
                .get("price")
                .and_then(parse_f64_value)
                .or_else(|| map.get("p").and_then(parse_f64_value))?;
            let size = map
                .get("size")
                .and_then(parse_f64_value)
                .or_else(|| map.get("s").and_then(parse_f64_value))
                .or_else(|| map.get("quantity").and_then(parse_f64_value))
                .or_else(|| map.get("qty").and_then(parse_f64_value))
                .or_else(|| map.get("amount").and_then(parse_f64_value))
                .unwrap_or(0.0);
            (price, size)
        }
        _ => {
            let price = parse_f64_value(level)?;
            (price, 0.0)
        }
    };

    let size = if size.is_finite() && size > 0.0 {
        size
    } else {
        0.0
    };
    Some(DepthLevel { price, size })
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
        data.insert(
            "event_type".to_string(),
            Value::String(event_type.to_string()),
        );
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
    data.insert(
        "selected_market_slug".to_string(),
        to_value_string(selected_slug),
    );
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
    data.insert(
        "selected_market_slug".to_string(),
        to_value_string(selected_slug),
    );
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
    data.insert(
        "selected_market_slug".to_string(),
        to_value_string(selected_slug),
    );
    data.insert(
        "condition_id".to_string(),
        to_value_string(selection.condition_id.clone()),
    );
    let event_slug = stats.event_slug.clone().or(selection.event_slug.clone());
    data.insert("event_slug".to_string(), to_value_string(event_slug));
    data.insert("token_ids".to_string(), to_value_strings(&sorted_ids));
    data.insert(
        "tick_size".to_string(),
        selection.gamma_tick.map(Value::from).unwrap_or(Value::Null),
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
        round_leg1_entered_ts: 0,
        round_leg2_entered_ts: 0,
        round_leg2_anchor_price: None,
        round_leg1_target_qty: 0.0,
        round_leg1_filled_qty: 0.0,
        round_leg2_target_qty: 0.0,
        round_leg2_filled_qty: 0.0,
        last_apply_ts_ms: 0,
        last_round_complete_ts: 0,
        lock_reopen_used_rounds: 0,
        best_bid_up: None,
        best_ask_up: None,
        best_bid_down: None,
        best_ask_down: None,
        exchange_ts_ms_up: None,
        recv_ts_ms_up: None,
        latency_ms_up: None,
        exchange_ts_ms_down: None,
        recv_ts_ms_down: None,
        latency_ms_down: None,
        best_bid_size_up: None,
        best_ask_size_up: None,
        best_bid_size_down: None,
        best_ask_size_down: None,
        bid_consumption_rate_up: 0.0,
        ask_consumption_rate_up: 0.0,
        bid_consumption_rate_down: 0.0,
        ask_consumption_rate_down: 0.0,
        prev_mid_up: None,
        prev_mid_down: None,
        mid_up_fast_ema: None,
        mid_down_fast_ema: None,
        mid_up_slow_ema: None,
        mid_down_slow_ema: None,
        pair_mid_vol_bps: 0.0,
        mid_up_momentum_bps: 0.0,
        mid_down_momentum_bps: 0.0,
        mid_up_discount_bps: 0.0,
        mid_down_discount_bps: 0.0,
        last_decision_ts_ms: 0,
        decision_seq: 0,
        entry_worst_pair_deny_streak: 0,
        entry_worst_pair_streak_started_ts: 0,
        entry_fallback_until_ts: 0,
        open_orders_active: 0,
        locked: false,
        locked_hedgeable: 0.0,
        locked_pair_cost: None,
        locked_at_ts_ms: 0,
        open_count_window: 0,
        fill_count_window: 0,
        timeout_extend_count_window: 0,
        requote_count_window: 0,
        waiting_skip_count_window: 0,
        risk_guard_cancel_count_window: 0,
        risk_guard_cancel_pair_cap_count_window: 0,
        risk_guard_cancel_other_count_window: 0,
        resting_hard_risk_cancel_count_window: 0,
        resting_soft_recheck_cancel_count_window: 0,
        stale_cancel_count_window: 0,
        close_window_cancel_count_window: 0,
        entry_worst_pair_block_count_window: 0,
        unrecoverable_block_count_window: 0,
        cancel_unrecoverable_count_window: 0,
        max_executed_sim_pair_cost_window: None,
        tail_pair_cap_block_count_window: 0,
    }
}

impl DryRunLedger {
    fn update_from_cache(&mut self, cache: &HashMap<String, BestQuote>) {
        if let Some(quote) = cache.get(&self.up_id) {
            self.best_bid_up = quote.best_bid;
            self.best_ask_up = quote.best_ask;
            self.exchange_ts_ms_up = quote.exchange_ts_ms;
            self.recv_ts_ms_up = Some(quote.recv_ts_ms);
            self.latency_ms_up = quote.latency_ms;
            self.best_bid_size_up = quote.best_bid_size;
            self.best_ask_size_up = quote.best_ask_size;
            self.bid_consumption_rate_up = quote.bid_consumption_rate;
            self.ask_consumption_rate_up = quote.ask_consumption_rate;
        }
        if let Some(quote) = cache.get(&self.down_id) {
            self.best_bid_down = quote.best_bid;
            self.best_ask_down = quote.best_ask;
            self.exchange_ts_ms_down = quote.exchange_ts_ms;
            self.recv_ts_ms_down = Some(quote.recv_ts_ms);
            self.latency_ms_down = quote.latency_ms;
            self.best_bid_size_down = quote.best_bid_size;
            self.best_ask_size_down = quote.best_ask_size;
            self.bid_consumption_rate_down = quote.bid_consumption_rate;
            self.ask_consumption_rate_down = quote.ask_consumption_rate;
        }
    }

    fn update_pair_mid_vol_bps(
        &mut self,
        lookback_ticks: u64,
        reversal_fast_ema_ticks: u64,
        reversal_slow_ema_ticks: u64,
    ) {
        fn update_ema(prev: Option<f64>, sample: f64, ticks: u64) -> f64 {
            let lookback = ticks.max(1) as f64;
            let alpha = (2.0 / (lookback + 1.0)).clamp(0.0, 1.0);
            let base = prev.unwrap_or(sample);
            alpha * sample + (1.0 - alpha) * base
        }
        let Some(mid_up) = mid_price(self.best_bid_up, self.best_ask_up) else {
            return;
        };
        let Some(mid_down) = mid_price(self.best_bid_down, self.best_ask_down) else {
            return;
        };
        if self.prev_mid_up.is_none() || self.prev_mid_down.is_none() {
            self.prev_mid_up = Some(mid_up);
            self.prev_mid_down = Some(mid_down);
            self.mid_up_fast_ema = Some(mid_up);
            self.mid_down_fast_ema = Some(mid_down);
            self.mid_up_slow_ema = Some(mid_up);
            self.mid_down_slow_ema = Some(mid_down);
            self.pair_mid_vol_bps = 0.0;
            self.mid_up_momentum_bps = 0.0;
            self.mid_down_momentum_bps = 0.0;
            self.mid_up_discount_bps = 0.0;
            self.mid_down_discount_bps = 0.0;
            return;
        }
        let prev_mid_up = self.prev_mid_up.unwrap_or(mid_up);
        let prev_mid_down = self.prev_mid_down.unwrap_or(mid_down);
        self.mid_up_momentum_bps = if prev_mid_up > 0.0 {
            ((mid_up - prev_mid_up) / prev_mid_up) * 10_000.0
        } else {
            0.0
        };
        self.mid_down_momentum_bps = if prev_mid_down > 0.0 {
            ((mid_down - prev_mid_down) / prev_mid_down) * 10_000.0
        } else {
            0.0
        };
        let ret_up_bps = if prev_mid_up > 0.0 {
            ((mid_up - prev_mid_up).abs() / prev_mid_up) * 10_000.0
        } else {
            0.0
        };
        let ret_down_bps = if prev_mid_down > 0.0 {
            ((mid_down - prev_mid_down).abs() / prev_mid_down) * 10_000.0
        } else {
            0.0
        };
        let inst_vol_bps = 0.5 * (ret_up_bps + ret_down_bps);
        let lookback = lookback_ticks.max(1) as f64;
        let alpha = (2.0 / (lookback + 1.0)).clamp(0.0, 1.0);
        self.pair_mid_vol_bps = alpha * inst_vol_bps + (1.0 - alpha) * self.pair_mid_vol_bps;

        let fast_up = update_ema(self.mid_up_fast_ema, mid_up, reversal_fast_ema_ticks);
        let fast_down = update_ema(self.mid_down_fast_ema, mid_down, reversal_fast_ema_ticks);
        let slow_up = update_ema(self.mid_up_slow_ema, mid_up, reversal_slow_ema_ticks);
        let slow_down = update_ema(self.mid_down_slow_ema, mid_down, reversal_slow_ema_ticks);
        self.mid_up_fast_ema = Some(fast_up);
        self.mid_down_fast_ema = Some(fast_down);
        self.mid_up_slow_ema = Some(slow_up);
        self.mid_down_slow_ema = Some(slow_down);
        self.mid_up_discount_bps = if slow_up > 0.0 {
            ((slow_up - mid_up) / slow_up) * 10_000.0
        } else {
            0.0
        };
        self.mid_down_discount_bps = if slow_down > 0.0 {
            ((slow_down - mid_down) / slow_down) * 10_000.0
        } else {
            0.0
        };
        self.prev_mid_up = Some(mid_up);
        self.prev_mid_down = Some(mid_down);
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
        if self.round_idx < params.lock_min_completed_rounds {
            return;
        }
        let now_ts = now_ms / 1000;
        let time_left_secs = if self.end_ts > now_ts {
            self.end_ts - now_ts
        } else {
            0
        };
        if params.lock_min_time_left_secs > 0 {
            if time_left_secs > params.lock_min_time_left_secs {
                return;
            }
        }
        let spent_ratio = if params.total_budget_usdc > 0.0 {
            (self.spent_total_usdc / params.total_budget_usdc).clamp(0.0, 1.0)
        } else {
            1.0
        };
        let spent_ready = spent_ratio + 1e-9 >= params.lock_min_spent_ratio;
        let force_lock_time_left_secs = if params.lock_force_time_left_secs > 0 {
            params.lock_force_time_left_secs.min(params.tail_close_secs)
        } else {
            0
        };
        let force_by_time = force_lock_time_left_secs > 0 && time_left_secs <= force_lock_time_left_secs;
        if !spent_ready && !force_by_time {
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
            RoundPhase::Leg1Accumulating => "leg1_accumulating",
            RoundPhase::Leg2Balancing => "leg2_balancing",
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
        // Keep runtime round-state transition semantics aligned with strategy::state::Ledger.
        let mut strategy_ledger = to_strategy_ledger(self);
        strategy_ledger.update_round_state(&to_strategy_params(params), now_ts);
        copy_back_from_strategy_ledger(self, strategy_ledger);
    }
}

fn should_run_dryrun(ledger: &DryRunLedger, now_ms: u64, interval_ms: u64) -> bool {
    ledger.last_decision_ts_ms == 0
        || now_ms.saturating_sub(ledger.last_decision_ts_ms) >= interval_ms
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

fn evaluate_hedge_recoverability_ws(
    ledger: &DryRunLedger,
    params: &DryRunParams,
    new_qty_up: f64,
    new_cost_up: f64,
    new_qty_down: f64,
    new_cost_down: f64,
) -> (
    Option<bool>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<bool>,
) {
    let net_before = (ledger.qty_up - ledger.qty_down).abs();
    let net_after = (new_qty_up - new_qty_down).abs();
    if net_after <= net_before + 1e-9 {
        return (None, None, None, None, None, None, None);
    }

    let (required_hedge_qty, total_qty_after_hedge, current_opp_best_ask) =
        if new_qty_up > new_qty_down {
            (new_qty_up - new_qty_down, new_qty_up, ledger.best_ask_down)
        } else {
            (new_qty_down - new_qty_up, new_qty_down, ledger.best_ask_up)
        };

    if required_hedge_qty <= 1e-9 || total_qty_after_hedge <= 1e-9 {
        return (
            Some(true),
            None,
            current_opp_best_ask,
            Some(required_hedge_qty.max(0.0)),
            None,
            None,
            None,
        );
    }

    let total_cost_after_fill = new_cost_up + new_cost_down;
    let required_opp_avg_price_cap = (params.no_risk_hard_pair_cap * total_qty_after_hedge
        - total_cost_after_fill)
        / required_hedge_qty;
    let eps_mult = 1.0 + params.no_risk_hedge_recoverability_eps_bps.max(0.0) / 10_000.0;
    let hedge_recoverable_now = if !required_opp_avg_price_cap.is_finite() {
        false
    } else if required_opp_avg_price_cap <= 0.0 {
        false
    } else if let Some(opp_ask) = current_opp_best_ask {
        opp_ask <= required_opp_avg_price_cap * eps_mult + 1e-9
    } else {
        false
    };

    let (hedge_margin_to_opp_ask, hedge_margin_required, hedge_margin_ok) =
        match current_opp_best_ask {
            Some(opp_ask) if opp_ask.is_finite() && opp_ask >= 0.0 => {
                let margin_to_opp_ask = required_opp_avg_price_cap - opp_ask;
                let margin_required = (params.hedge_recoverability_margin_min_ticks.max(0.0)
                    * ledger.tick_size_current.max(1e-9))
                .max(opp_ask * params.hedge_recoverability_margin_min_bps.max(0.0) / 10_000.0);
                (
                    Some(margin_to_opp_ask),
                    Some(margin_required),
                    Some(margin_to_opp_ask + 1e-9 >= margin_required),
                )
            }
            _ => (None, None, None),
        };

    (
        Some(hedge_recoverable_now),
        Some(required_opp_avg_price_cap),
        current_opp_best_ask,
        Some(required_hedge_qty),
        hedge_margin_to_opp_ask,
        hedge_margin_required,
        hedge_margin_ok,
    )
}

fn build_round_plan(ledger: &DryRunLedger, params: &DryRunParams, now_ts: u64) -> RoundPlan {
    let plan = strategy::planner::build_round_plan(
        &to_strategy_ledger(ledger),
        &to_strategy_params(params),
        now_ts,
    );
    RoundPlan {
        phase: from_strategy_round_phase(plan.phase),
        planned_leg1: plan.planned_leg1.map(from_strategy_trade_leg),
        qty_target: plan.qty_target,
        balance_leg: plan.balance_leg.map(from_strategy_trade_leg),
        balance_qty: plan.balance_qty,
        can_start_new_round: plan.can_start_new_round,
        budget_remaining_round: plan.budget_remaining_round,
        budget_remaining_total: plan.budget_remaining_total,
        reserve_needed_usdc: plan.reserve_needed_usdc,
        vol_entry_bps: plan.vol_entry_bps,
        vol_entry_ok: plan.vol_entry_ok,
        reversal_up_ok: plan.reversal_up_ok,
        reversal_down_ok: plan.reversal_down_ok,
        turn_up_ok: plan.turn_up_ok,
        turn_down_ok: plan.turn_down_ok,
        first_leg_turning_score: plan.first_leg_turning_score,
        entry_worst_pair_cost: plan.entry_worst_pair_cost,
        entry_worst_pair_ok: plan.entry_worst_pair_ok,
        entry_timeout_flow_ratio: plan.entry_timeout_flow_ratio,
        entry_timeout_flow_ok: plan.entry_timeout_flow_ok,
        entry_fillability_ok: plan.entry_fillability_ok,
        entry_edge_bps: plan.entry_edge_bps,
        entry_regime_score: plan.entry_regime_score,
        entry_depth_cap_qty: plan.entry_depth_cap_qty,
        entry_flow_cap_qty: plan.entry_flow_cap_qty,
        slice_count_planned: plan.slice_count_planned,
        slice_qty_current: plan.slice_qty_current,
        entry_final_qty_slice: plan.entry_final_qty_slice,
        entry_fallback_active: plan.entry_fallback_active,
        entry_fallback_armed: plan.entry_fallback_armed,
        entry_fallback_trigger_reason: plan.entry_fallback_trigger_reason,
        entry_fallback_blocked_by_recoverability: plan.entry_fallback_blocked_by_recoverability,
        new_round_cutoff_secs: plan.new_round_cutoff_secs,
        late_new_round_blocked: plan.late_new_round_blocked,
        pair_quality_ok: plan.pair_quality_ok,
        pair_regression_ok: plan.pair_regression_ok,
        can_open_round_base_ok: plan.can_open_round_base_ok,
        can_start_block_reason: plan.can_start_block_reason,
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
    let new_unhedged_value_up =
        price_for_unhedged(ledger.best_bid_up, ledger.best_ask_up).unwrap_or(0.0) * new_unhedged_up;
    let new_unhedged_value_down = price_for_unhedged(ledger.best_bid_down, ledger.best_ask_down)
        .unwrap_or(0.0)
        * new_unhedged_down;
    let (
        hedge_recoverable_now,
        required_opp_avg_price_cap,
        current_opp_best_ask,
        required_hedge_qty,
    ) = (None, None, None, None);
    let (hedge_margin_to_opp_ask, hedge_margin_required, hedge_margin_ok) = (None, None, None);
    let current_unhedged_value_up = price_for_unhedged(ledger.best_bid_up, ledger.best_ask_up)
        .unwrap_or(0.0)
        * ledger.unhedged_up();
    let current_unhedged_value_down =
        price_for_unhedged(ledger.best_bid_down, ledger.best_ask_down).unwrap_or(0.0)
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
        maker_fill_prob: None,
        maker_queue_ahead: None,
        maker_expected_consumed: None,
        maker_consumption_rate: None,
        maker_horizon_secs: None,
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
        hedge_recoverable_now,
        required_opp_avg_price_cap,
        current_opp_best_ask,
        required_hedge_qty,
        hedge_margin_to_opp_ask,
        hedge_margin_required,
        hedge_margin_ok,
        entry_quote_base_postonly_price: None,
        entry_quote_dynamic_cap_price: None,
        entry_quote_final_price: None,
        entry_quote_cap_active: None,
        entry_quote_cap_bind: None,
        passive_gap_abs: None,
        passive_gap_ticks: None,
        improves_hedge,
    }
}

fn evaluate_action_gate_with_context(
    ledger: &DryRunLedger,
    action: &CandidateAction,
    sim: &SimResult,
    params: &DryRunParams,
    now_ts: u64,
    round_plan: &RoundPlan,
    context: strategy::gate::GateContext,
) -> GateResult {
    from_strategy_gate_result(strategy::gate::evaluate_action_gate_with_context(
        &to_strategy_ledger(ledger),
        &to_strategy_candidate_action(action),
        &to_strategy_sim_result(sim),
        &to_strategy_params(params),
        now_ts,
        &to_strategy_round_plan(round_plan),
        context,
    ))
}

fn is_soft_recheck_deny(reason: Option<DenyReason>) -> bool {
    matches!(
        reason,
        Some(
            DenyReason::EntryWorstPair
                | DenyReason::EntryEdgeTooThin
                | DenyReason::OpenMarginTooThin
                | DenyReason::ReserveForPair
                | DenyReason::NoImprove
                | DenyReason::Cooldown
                | DenyReason::LockedStrictAbsNet
                | DenyReason::LockedMaxRounds
                | DenyReason::LockedTailFreeze
                | DenyReason::LockedWaitingPair
                | DenyReason::LockedPolicyHold
                | DenyReason::LowMakerFillProb
        )
    )
}

fn to_strategy_trade_leg(value: TradeLeg) -> strategy::state::TradeLeg {
    match value {
        TradeLeg::Up => strategy::state::TradeLeg::Up,
        TradeLeg::Down => strategy::state::TradeLeg::Down,
    }
}

fn to_strategy_trade_side(value: TradeSide) -> strategy::state::TradeSide {
    match value {
        TradeSide::Buy => strategy::state::TradeSide::Buy,
        TradeSide::Sell => strategy::state::TradeSide::Sell,
    }
}

fn to_strategy_trade_kind(value: TradeKind) -> strategy::state::TradeKind {
    match value {
        TradeKind::Maker => strategy::state::TradeKind::Maker,
        TradeKind::Taker => strategy::state::TradeKind::Taker,
    }
}

fn to_strategy_round_phase(value: RoundPhase) -> strategy::state::RoundPhase {
    match value {
        RoundPhase::Idle => strategy::state::RoundPhase::Idle,
        RoundPhase::Leg1Accumulating => strategy::state::RoundPhase::Leg1Accumulating,
        RoundPhase::Leg2Balancing => strategy::state::RoundPhase::Leg2Balancing,
        RoundPhase::Done => strategy::state::RoundPhase::Done,
    }
}

fn to_strategy_candidate_action(value: &CandidateAction) -> strategy::simulate::CandidateAction {
    strategy::simulate::CandidateAction {
        name: value.name,
        leg: to_strategy_trade_leg(value.leg),
        side: to_strategy_trade_side(value.side),
        kind: to_strategy_trade_kind(value.kind),
        qty: value.qty,
    }
}

fn to_strategy_sim_result(value: &SimResult) -> strategy::simulate::SimResult {
    strategy::simulate::SimResult {
        ok: value.ok,
        fill_qty: value.fill_qty,
        fill_price: value.fill_price,
        maker_fill_prob: value.maker_fill_prob,
        maker_queue_ahead: value.maker_queue_ahead,
        maker_expected_consumed: value.maker_expected_consumed,
        maker_consumption_rate: value.maker_consumption_rate,
        maker_horizon_secs: value.maker_horizon_secs,
        fee_estimate: value.fee_estimate,
        spent_delta_usdc: value.spent_delta_usdc,
        new_qty_up: value.new_qty_up,
        new_cost_up: value.new_cost_up,
        new_qty_down: value.new_qty_down,
        new_cost_down: value.new_cost_down,
        new_avg_up: value.new_avg_up,
        new_avg_down: value.new_avg_down,
        new_pair_cost: value.new_pair_cost,
        new_hedgeable: value.new_hedgeable,
        new_unhedged_up: value.new_unhedged_up,
        new_unhedged_down: value.new_unhedged_down,
        new_unhedged_value_up: value.new_unhedged_value_up,
        new_unhedged_value_down: value.new_unhedged_value_down,
        hedge_recoverable_now: value.hedge_recoverable_now,
        required_opp_avg_price_cap: value.required_opp_avg_price_cap,
        current_opp_best_ask: value.current_opp_best_ask,
        required_hedge_qty: value.required_hedge_qty,
        hedge_margin_to_opp_ask: value.hedge_margin_to_opp_ask,
        hedge_margin_required: value.hedge_margin_required,
        hedge_margin_ok: value.hedge_margin_ok,
        entry_quote_base_postonly_price: value.entry_quote_base_postonly_price,
        entry_quote_dynamic_cap_price: value.entry_quote_dynamic_cap_price,
        entry_quote_final_price: value.entry_quote_final_price,
        entry_quote_cap_active: value.entry_quote_cap_active,
        entry_quote_cap_bind: value.entry_quote_cap_bind,
        passive_gap_abs: value.passive_gap_abs,
        passive_gap_ticks: value.passive_gap_ticks,
        improves_hedge: value.improves_hedge,
    }
}

fn to_strategy_round_plan(value: &RoundPlan) -> strategy::planner::RoundPlan {
    strategy::planner::RoundPlan {
        phase: to_strategy_round_phase(value.phase),
        planned_leg1: value.planned_leg1.map(to_strategy_trade_leg),
        qty_target: value.qty_target,
        balance_leg: value.balance_leg.map(to_strategy_trade_leg),
        balance_qty: value.balance_qty,
        can_start_new_round: value.can_start_new_round,
        budget_remaining_round: value.budget_remaining_round,
        budget_remaining_total: value.budget_remaining_total,
        reserve_needed_usdc: value.reserve_needed_usdc,
        vol_entry_bps: value.vol_entry_bps,
        vol_entry_ok: value.vol_entry_ok,
        reversal_up_ok: value.reversal_up_ok,
        reversal_down_ok: value.reversal_down_ok,
        turn_up_ok: value.turn_up_ok,
        turn_down_ok: value.turn_down_ok,
        first_leg_turning_score: value.first_leg_turning_score,
        entry_worst_pair_cost: value.entry_worst_pair_cost,
        entry_worst_pair_ok: value.entry_worst_pair_ok,
        entry_timeout_flow_ratio: value.entry_timeout_flow_ratio,
        entry_timeout_flow_ok: value.entry_timeout_flow_ok,
        entry_fillability_ok: value.entry_fillability_ok,
        entry_edge_bps: value.entry_edge_bps,
        entry_regime_score: value.entry_regime_score,
        entry_depth_cap_qty: value.entry_depth_cap_qty,
        entry_flow_cap_qty: value.entry_flow_cap_qty,
        slice_count_planned: value.slice_count_planned,
        slice_qty_current: value.slice_qty_current,
        entry_final_qty_slice: value.entry_final_qty_slice,
        entry_fallback_active: value.entry_fallback_active,
        entry_fallback_armed: value.entry_fallback_armed,
        entry_fallback_trigger_reason: value.entry_fallback_trigger_reason.clone(),
        entry_fallback_blocked_by_recoverability: value.entry_fallback_blocked_by_recoverability,
        new_round_cutoff_secs: value.new_round_cutoff_secs,
        late_new_round_blocked: value.late_new_round_blocked,
        pair_quality_ok: value.pair_quality_ok,
        pair_regression_ok: value.pair_regression_ok,
        can_open_round_base_ok: value.can_open_round_base_ok,
        can_start_block_reason: value.can_start_block_reason.clone(),
    }
}

fn from_strategy_trade_leg(value: strategy::state::TradeLeg) -> TradeLeg {
    match value {
        strategy::state::TradeLeg::Up => TradeLeg::Up,
        strategy::state::TradeLeg::Down => TradeLeg::Down,
    }
}

fn from_strategy_trade_side(value: strategy::state::TradeSide) -> TradeSide {
    match value {
        strategy::state::TradeSide::Buy => TradeSide::Buy,
        strategy::state::TradeSide::Sell => TradeSide::Sell,
    }
}

fn from_strategy_trade_kind(value: strategy::state::TradeKind) -> TradeKind {
    match value {
        strategy::state::TradeKind::Maker => TradeKind::Maker,
        strategy::state::TradeKind::Taker => TradeKind::Taker,
    }
}

fn from_strategy_round_phase(value: strategy::state::RoundPhase) -> RoundPhase {
    match value {
        strategy::state::RoundPhase::Idle => RoundPhase::Idle,
        strategy::state::RoundPhase::Leg1Accumulating => RoundPhase::Leg1Accumulating,
        strategy::state::RoundPhase::Leg2Balancing => RoundPhase::Leg2Balancing,
        strategy::state::RoundPhase::Done => RoundPhase::Done,
    }
}

fn from_strategy_deny_reason(value: strategy::state::DenyReason) -> DenyReason {
    match value {
        strategy::state::DenyReason::TakerDisabled => DenyReason::TakerDisabled,
        strategy::state::DenyReason::EntryWorstPair => DenyReason::EntryWorstPair,
        strategy::state::DenyReason::HedgeNotRecoverable => DenyReason::HedgeNotRecoverable,
        strategy::state::DenyReason::HedgeMarginInsufficient => DenyReason::HedgeMarginInsufficient,
        strategy::state::DenyReason::OpenMarginTooThin => DenyReason::OpenMarginTooThin,
        strategy::state::DenyReason::EntryEdgeTooThin => DenyReason::EntryEdgeTooThin,
        strategy::state::DenyReason::MarginTarget => DenyReason::MarginTarget,
        strategy::state::DenyReason::NoImprove => DenyReason::NoImprove,
        strategy::state::DenyReason::Cooldown => DenyReason::Cooldown,
        strategy::state::DenyReason::NoQuote => DenyReason::NoQuote,
        strategy::state::DenyReason::TotalBudgetCap => DenyReason::TotalBudgetCap,
        strategy::state::DenyReason::RoundBudgetCap => DenyReason::RoundBudgetCap,
        strategy::state::DenyReason::ReserveForPair => DenyReason::ReserveForPair,
        strategy::state::DenyReason::LegCapValueUp => DenyReason::LegCapValueUp,
        strategy::state::DenyReason::LegCapValueDown => DenyReason::LegCapValueDown,
        strategy::state::DenyReason::LegCapSharesUp => DenyReason::LegCapSharesUp,
        strategy::state::DenyReason::LegCapSharesDown => DenyReason::LegCapSharesDown,
        strategy::state::DenyReason::TailFreeze => DenyReason::TailFreeze,
        strategy::state::DenyReason::TailClose => DenyReason::TailClose,
        strategy::state::DenyReason::LockedStrictAbsNet => DenyReason::LockedStrictAbsNet,
        strategy::state::DenyReason::LockedMaxRounds => DenyReason::LockedMaxRounds,
        strategy::state::DenyReason::LockedTailFreeze => DenyReason::LockedTailFreeze,
        strategy::state::DenyReason::LockedWaitingPair => DenyReason::LockedWaitingPair,
        strategy::state::DenyReason::LockedPolicyHold => DenyReason::LockedPolicyHold,
        strategy::state::DenyReason::LowMakerFillProb => DenyReason::LowMakerFillProb,
    }
}

fn to_strategy_params(params: &DryRunParams) -> strategy::params::StrategyParams {
    strategy::params::StrategyParams {
        improve_min: params.improve_min,
        margin_target: params.margin_target,
        safety_margin: params.safety_margin,
        total_budget_usdc: params.total_budget_usdc,
        total_budget_source: params.total_budget_source.clone(),
        max_rounds: params.max_rounds,
        round_budget_usdc: params.round_budget_usdc,
        round_budget_source: params.round_budget_source.clone(),
        round_leg1_fraction: params.round_leg1_fraction,
        max_unhedged_value: params.max_unhedged_value,
        cap_unhedged_value: params.cap_unhedged_value,
        max_unhedged_shares: params.max_unhedged_shares,
        cooldown_secs: params.cooldown_secs,
        tail_freeze_secs: params.tail_freeze_secs,
        tail_close_secs: params.tail_close_secs,
        decision_every_ms: params.decision_every_ms,
        mode: match params.mode {
            DryRunMode::Recommend => strategy::state::DryRunMode::Recommend,
            DryRunMode::Paper => strategy::state::DryRunMode::Paper,
        },
        apply_kind: match params.apply_kind {
            ApplyKind::BestAction => strategy::state::ApplyKind::BestAction,
            ApplyKind::BestMakerOnly => strategy::state::ApplyKind::BestMakerOnly,
            ApplyKind::BestTakerOnly => strategy::state::ApplyKind::BestTakerOnly,
        },
        pair_bonus: params.pair_bonus,
        lock_strict_abs_net: params.lock_strict_abs_net,
        round_budget_strict: params.round_budget_strict,
        lock_min_completed_rounds: params.lock_min_completed_rounds,
        lock_min_time_left_secs: params.lock_min_time_left_secs,
        lock_min_spent_ratio: params.lock_min_spent_ratio,
        lock_force_time_left_secs: params.lock_force_time_left_secs,
        tail_close_ignore_margin: params.tail_close_ignore_margin,
        vol_entry_min_bps: params.vol_entry_min_bps,
        vol_entry_lookback_ticks: params.vol_entry_lookback_ticks,
        reversal_entry_enabled: params.reversal_entry_enabled,
        reversal_min_discount_bps: params.reversal_min_discount_bps,
        reversal_min_momentum_bps: params.reversal_min_momentum_bps,
        reversal_fast_ema_ticks: params.reversal_fast_ema_ticks,
        reversal_slow_ema_ticks: params.reversal_slow_ema_ticks,
        entry_turn_confirm_ticks: params.entry_turn_confirm_ticks,
        entry_turn_min_rebound_bps: params.entry_turn_min_rebound_bps,
        entry_pair_buffer_bps: params.entry_pair_buffer_bps,
        round_pair_wait_secs: params.round_pair_wait_secs,
        leg2_rebalance_discount_bps: params.leg2_rebalance_discount_bps,
        force_pair_max_pair_cost: params.force_pair_max_pair_cost,
        no_risk_hard_pair_cap: params.no_risk_hard_pair_cap,
        no_risk_pair_limit_mode: params.no_risk_pair_limit_mode,
        no_risk_enforce_tail: params.no_risk_enforce_tail,
        no_risk_entry_hedge_price_mode: params.no_risk_entry_hedge_price_mode,
        no_risk_entry_ask_slippage_bps: params.no_risk_entry_ask_slippage_bps,
        no_risk_entry_worst_hedge_bps: params.no_risk_entry_worst_hedge_bps,
        no_risk_entry_pair_headroom_bps: params.no_risk_entry_pair_headroom_bps,
        entry_dynamic_cap_enabled: params.entry_dynamic_cap_enabled,
        entry_dynamic_cap_headroom_bps: params.entry_dynamic_cap_headroom_bps,
        entry_dynamic_cap_min_price: params.entry_dynamic_cap_min_price,
        entry_dynamic_cap_apply_to_net_increase_only: params
            .entry_dynamic_cap_apply_to_net_increase_only,
        entry_target_margin_min_ticks: params.entry_target_margin_min_ticks,
        entry_target_margin_min_bps: params.entry_target_margin_min_bps,
        entry_max_passive_ticks_for_net_increase: params.entry_max_passive_ticks_for_net_increase,
        no_risk_late_new_round_buffer_secs: params.no_risk_late_new_round_buffer_secs,
        no_risk_require_completable_round: params.no_risk_require_completable_round,
        no_risk_strict_zero_unmatched: params.no_risk_strict_zero_unmatched,
        no_risk_hedge_recoverability_enforce: params.no_risk_hedge_recoverability_enforce,
        no_risk_hedge_recoverability_eps_bps: params.no_risk_hedge_recoverability_eps_bps,
        hedge_recoverability_margin_enforce: params.hedge_recoverability_margin_enforce,
        hedge_recoverability_margin_min_ticks: params.hedge_recoverability_margin_min_ticks,
        hedge_recoverability_margin_min_bps: params.hedge_recoverability_margin_min_bps,
        hedge_recoverability_margin_apply_to_net_increase_only: params
            .hedge_recoverability_margin_apply_to_net_increase_only,
        open_order_risk_guard: params.open_order_risk_guard,
        open_order_risk_buffer_bps: params.open_order_risk_buffer_bps,
        open_order_risk_guard_require_paired: params.open_order_risk_guard_require_paired,
        open_order_max_age_secs: params.open_order_max_age_secs,
        open_order_unrecoverable_grace_ms: params.open_order_unrecoverable_grace_ms,
        paper_timeout_target_fill_prob: params.paper_timeout_target_fill_prob,
        paper_timeout_progress_extend_min: params.paper_timeout_progress_extend_min,
        paper_timeout_progress_extend_secs: params.paper_timeout_progress_extend_secs,
        paper_timeout_max_extends: params.paper_timeout_max_extends,
        paper_requote_require_price_move: params.paper_requote_require_price_move,
        paper_requote_stale_ms_hard: params.paper_requote_stale_ms_hard,
        paper_requote_retain_fill_draw: params.paper_requote_retain_fill_draw,
        requote_min_fill_prob_uplift: params.requote_min_fill_prob_uplift,
        requote_queue_stickiness_ratio: params.requote_queue_stickiness_ratio,
        requote_stickiness_min_age_secs: params.requote_stickiness_min_age_secs,
        round_slice_count: params.round_slice_count,
        round_dynamic_slicing_enabled: params.round_dynamic_slicing_enabled,
        round_min_slices: params.round_min_slices,
        round_max_slices: params.round_max_slices,
        round_min_slice_qty: params.round_min_slice_qty,
        entry_pair_regression_mode: params.entry_pair_regression_mode,
        entry_pair_regression_soft_band_bps: params.entry_pair_regression_soft_band_bps,
        entry_edge_min_bps: params.entry_edge_min_bps,
        entry_fill_prob_min: params.entry_fill_prob_min,
        open_min_fill_prob: params.open_min_fill_prob,
        open_margin_surplus_min: params.open_margin_surplus_min,
        inventory_skew_alpha_bps: params.inventory_skew_alpha_bps,
        lock_allow_reopen_before_freeze: params.lock_allow_reopen_before_freeze,
        lock_reopen_max_rounds: params.lock_reopen_max_rounds,
        round_min_start_gap_secs: params.round_min_start_gap_secs,
        opening_no_trade_secs: params.opening_no_trade_secs,
        min_apply_interval_ms: params.min_apply_interval_ms,
        maker_fill_estimator_enabled: params.maker_fill_estimator_enabled,
        maker_fill_horizon_secs: params.maker_fill_horizon_secs,
        maker_min_fill_prob: params.maker_min_fill_prob,
        maker_queue_ahead_mult: params.maker_queue_ahead_mult,
        maker_fill_passive_queue_penalty_per_tick: params.maker_fill_passive_queue_penalty_per_tick,
        maker_fill_passive_decay_k: params.maker_fill_passive_decay_k,
        maker_flow_floor_per_sec: params.maker_flow_floor_per_sec,
        entry_passive_gap_soft_max_ticks: params.entry_passive_gap_soft_max_ticks,
        entry_max_top_book_share: params.entry_max_top_book_share,
        entry_max_flow_utilization: params.entry_max_flow_utilization,
        entry_min_timeout_flow_ratio: params.entry_min_timeout_flow_ratio,
        entry_fallback_enabled: params.entry_fallback_enabled,
        entry_fallback_deny_streak: params.entry_fallback_deny_streak,
        entry_fallback_window_secs: params.entry_fallback_window_secs,
        entry_fallback_duration_secs: params.entry_fallback_duration_secs,
        entry_fallback_hedge_mode: params.entry_fallback_hedge_mode,
        entry_fallback_worst_hedge_bps: params.entry_fallback_worst_hedge_bps,
        check_summary_skip_plot: params.check_summary_skip_plot,
    }
}

fn to_strategy_ledger(ledger: &DryRunLedger) -> strategy::state::Ledger {
    strategy::state::Ledger {
        market_slug: ledger.market_slug.clone(),
        series_slug: ledger.series_slug.clone(),
        market_select_mode: ledger.market_select_mode.clone(),
        up_id: ledger.up_id.clone(),
        down_id: ledger.down_id.clone(),
        tick_size: ledger.tick_size,
        tick_size_current: ledger.tick_size_current,
        tick_size_source: ledger.tick_size_source.clone(),
        start_ts: ledger.start_ts,
        end_ts: ledger.end_ts,
        qty_up: ledger.qty_up,
        cost_up: ledger.cost_up,
        qty_down: ledger.qty_down,
        cost_down: ledger.cost_down,
        spent_total_usdc: ledger.spent_total_usdc,
        spent_round_usdc: ledger.spent_round_usdc,
        round_idx: ledger.round_idx,
        round_state: match ledger.round_state {
            RoundPhase::Idle => strategy::state::RoundPhase::Idle,
            RoundPhase::Leg1Accumulating => strategy::state::RoundPhase::Leg1Accumulating,
            RoundPhase::Leg2Balancing => strategy::state::RoundPhase::Leg2Balancing,
            RoundPhase::Done => strategy::state::RoundPhase::Done,
        },
        round_leg1: ledger.round_leg1.map(to_strategy_trade_leg),
        round_qty_target: ledger.round_qty_target,
        round_leg1_entered_ts: ledger.round_leg1_entered_ts,
        round_leg2_entered_ts: ledger.round_leg2_entered_ts,
        round_leg2_anchor_price: ledger.round_leg2_anchor_price,
        round_leg1_target_qty: ledger.round_leg1_target_qty,
        round_leg1_filled_qty: ledger.round_leg1_filled_qty,
        round_leg2_target_qty: ledger.round_leg2_target_qty,
        round_leg2_filled_qty: ledger.round_leg2_filled_qty,
        last_apply_ts_ms: ledger.last_apply_ts_ms,
        last_round_complete_ts: ledger.last_round_complete_ts,
        lock_reopen_used_rounds: ledger.lock_reopen_used_rounds,
        best_bid_up: ledger.best_bid_up,
        best_ask_up: ledger.best_ask_up,
        best_bid_down: ledger.best_bid_down,
        best_ask_down: ledger.best_ask_down,
        best_bid_size_up: ledger.best_bid_size_up,
        best_ask_size_up: ledger.best_ask_size_up,
        best_bid_size_down: ledger.best_bid_size_down,
        best_ask_size_down: ledger.best_ask_size_down,
        bid_consumption_rate_up: ledger.bid_consumption_rate_up,
        ask_consumption_rate_up: ledger.ask_consumption_rate_up,
        bid_consumption_rate_down: ledger.bid_consumption_rate_down,
        ask_consumption_rate_down: ledger.ask_consumption_rate_down,
        pair_mid_vol_bps: ledger.pair_mid_vol_bps,
        mid_up_momentum_bps: ledger.mid_up_momentum_bps,
        mid_down_momentum_bps: ledger.mid_down_momentum_bps,
        mid_up_discount_bps: ledger.mid_up_discount_bps,
        mid_down_discount_bps: ledger.mid_down_discount_bps,
        last_decision_ts_ms: ledger.last_decision_ts_ms,
        decision_seq: ledger.decision_seq,
        entry_worst_pair_deny_streak: ledger.entry_worst_pair_deny_streak,
        entry_worst_pair_streak_started_ts: ledger.entry_worst_pair_streak_started_ts,
        entry_fallback_until_ts: ledger.entry_fallback_until_ts,
        open_orders_active: ledger.open_orders_active,
        lock_state: if ledger.locked {
            strategy::state::LockState::Locked
        } else {
            strategy::state::LockState::Unlocked
        },
        locked_hedgeable: ledger.locked_hedgeable,
        locked_pair_cost: ledger.locked_pair_cost,
        locked_at_ts_ms: ledger.locked_at_ts_ms,
    }
}

fn copy_back_from_strategy_ledger(target: &mut DryRunLedger, source: strategy::state::Ledger) {
    target.qty_up = source.qty_up;
    target.cost_up = source.cost_up;
    target.qty_down = source.qty_down;
    target.cost_down = source.cost_down;
    target.spent_total_usdc = source.spent_total_usdc;
    target.spent_round_usdc = source.spent_round_usdc;
    target.round_idx = source.round_idx;
    target.round_state = from_strategy_round_phase(source.round_state);
    target.round_leg1 = source.round_leg1.map(from_strategy_trade_leg);
    target.round_qty_target = source.round_qty_target;
    target.round_leg1_entered_ts = source.round_leg1_entered_ts;
    target.round_leg2_entered_ts = source.round_leg2_entered_ts;
    target.round_leg2_anchor_price = source.round_leg2_anchor_price;
    target.round_leg1_target_qty = source.round_leg1_target_qty;
    target.round_leg1_filled_qty = source.round_leg1_filled_qty;
    target.round_leg2_target_qty = source.round_leg2_target_qty;
    target.round_leg2_filled_qty = source.round_leg2_filled_qty;
    target.last_apply_ts_ms = source.last_apply_ts_ms;
    target.last_round_complete_ts = source.last_round_complete_ts;
    target.lock_reopen_used_rounds = source.lock_reopen_used_rounds;
    target.locked = source.lock_state == strategy::state::LockState::Locked;
    target.locked_hedgeable = source.locked_hedgeable;
    target.locked_pair_cost = source.locked_pair_cost;
    target.locked_at_ts_ms = source.locked_at_ts_ms;
    target.pair_mid_vol_bps = source.pair_mid_vol_bps;
    target.mid_up_momentum_bps = source.mid_up_momentum_bps;
    target.mid_down_momentum_bps = source.mid_down_momentum_bps;
    target.mid_up_discount_bps = source.mid_up_discount_bps;
    target.mid_down_discount_bps = source.mid_down_discount_bps;
}

fn from_strategy_candidate_action(value: strategy::simulate::CandidateAction) -> CandidateAction {
    CandidateAction {
        name: value.name,
        leg: from_strategy_trade_leg(value.leg),
        side: from_strategy_trade_side(value.side),
        kind: from_strategy_trade_kind(value.kind),
        qty: value.qty,
    }
}

fn from_strategy_sim_result(value: strategy::simulate::SimResult) -> SimResult {
    SimResult {
        ok: value.ok,
        fill_qty: value.fill_qty,
        fill_price: value.fill_price,
        maker_fill_prob: value.maker_fill_prob,
        maker_queue_ahead: value.maker_queue_ahead,
        maker_expected_consumed: value.maker_expected_consumed,
        maker_consumption_rate: value.maker_consumption_rate,
        maker_horizon_secs: value.maker_horizon_secs,
        fee_estimate: value.fee_estimate,
        spent_delta_usdc: value.spent_delta_usdc,
        new_qty_up: value.new_qty_up,
        new_cost_up: value.new_cost_up,
        new_qty_down: value.new_qty_down,
        new_cost_down: value.new_cost_down,
        new_avg_up: value.new_avg_up,
        new_avg_down: value.new_avg_down,
        new_pair_cost: value.new_pair_cost,
        new_hedgeable: value.new_hedgeable,
        new_unhedged_up: value.new_unhedged_up,
        new_unhedged_down: value.new_unhedged_down,
        new_unhedged_value_up: value.new_unhedged_value_up,
        new_unhedged_value_down: value.new_unhedged_value_down,
        hedge_recoverable_now: value.hedge_recoverable_now,
        required_opp_avg_price_cap: value.required_opp_avg_price_cap,
        current_opp_best_ask: value.current_opp_best_ask,
        required_hedge_qty: value.required_hedge_qty,
        hedge_margin_to_opp_ask: value.hedge_margin_to_opp_ask,
        hedge_margin_required: value.hedge_margin_required,
        hedge_margin_ok: value.hedge_margin_ok,
        entry_quote_base_postonly_price: value.entry_quote_base_postonly_price,
        entry_quote_dynamic_cap_price: value.entry_quote_dynamic_cap_price,
        entry_quote_final_price: value.entry_quote_final_price,
        entry_quote_cap_active: value.entry_quote_cap_active,
        entry_quote_cap_bind: value.entry_quote_cap_bind,
        passive_gap_abs: value.passive_gap_abs,
        passive_gap_ticks: value.passive_gap_ticks,
        improves_hedge: value.improves_hedge,
    }
}

fn from_strategy_gate_result(value: strategy::gate::GateResult) -> GateResult {
    GateResult {
        allow: value.allow,
        deny_reason: value.deny_reason.map(from_strategy_deny_reason),
        can_start_new_round: value.can_start_new_round,
        budget_remaining_round: value.budget_remaining_round,
        budget_remaining_total: value.budget_remaining_total,
        reserve_needed_usdc: value.reserve_needed_usdc,
    }
}

fn build_dryrun_candidates(
    ledger: &DryRunLedger,
    params: &DryRunParams,
) -> Vec<(CandidateAction, SimResult, GateResult, Option<f64>)> {
    let now_ts = Utc::now().timestamp();
    let now_ts = u64::try_from(now_ts).unwrap_or(0);
    let decision = strategy::decide(strategy::StrategyInput {
        ledger: to_strategy_ledger(ledger),
        params: to_strategy_params(params),
        now_ts,
    });

    decision
        .candidates
        .into_iter()
        .map(|value| {
            (
                from_strategy_candidate_action(value.action),
                from_strategy_sim_result(value.sim),
                from_strategy_gate_result(value.gate),
                value.score,
            )
        })
        .collect()
}

fn update_entry_fallback_state(
    ledger: &mut DryRunLedger,
    params: &DryRunParams,
    now_ts: u64,
    candidates: &[(CandidateAction, SimResult, GateResult, Option<f64>)],
    best_any: &Option<CandidateEval>,
) {
    let eps = 1e-9;
    ledger.open_orders_active = ledger.open_orders_active.max(0);
    if !params.entry_fallback_enabled {
        ledger.entry_worst_pair_deny_streak = 0;
        ledger.entry_worst_pair_streak_started_ts = 0;
        ledger.entry_fallback_until_ts = 0;
        return;
    }
    if ledger.entry_fallback_until_ts > 0 && now_ts > ledger.entry_fallback_until_ts {
        ledger.entry_fallback_until_ts = 0;
    }

    let abs_net = (ledger.qty_up - ledger.qty_down).abs();
    if abs_net > eps
        || ledger.open_orders_active > 0
        || ledger.round_idx > 0
        || ledger.round_state != RoundPhase::Idle
    {
        ledger.entry_worst_pair_deny_streak = 0;
        ledger.entry_worst_pair_streak_started_ts = 0;
        ledger.entry_fallback_until_ts = 0;
        return;
    }

    let entry_blocked = best_any.is_none()
        && !candidates.is_empty()
        && candidates
            .iter()
            .all(|(_, _, gate, _)| matches!(gate.deny_reason, Some(DenyReason::EntryWorstPair)));
    if entry_blocked {
        let window_expired = params.entry_fallback_window_secs > 0
            && ledger.entry_worst_pair_streak_started_ts > 0
            && now_ts
                > ledger
                    .entry_worst_pair_streak_started_ts
                    .saturating_add(params.entry_fallback_window_secs);
        if ledger.entry_worst_pair_deny_streak == 0 || window_expired {
            ledger.entry_worst_pair_deny_streak = 1;
            ledger.entry_worst_pair_streak_started_ts = now_ts;
        } else {
            ledger.entry_worst_pair_deny_streak =
                ledger.entry_worst_pair_deny_streak.saturating_add(1);
        }
        if ledger.entry_worst_pair_deny_streak >= params.entry_fallback_deny_streak
            && params.entry_fallback_duration_secs > 0
        {
            ledger.entry_fallback_until_ts = ledger
                .entry_fallback_until_ts
                .max(now_ts.saturating_add(params.entry_fallback_duration_secs));
        }
    } else {
        ledger.entry_worst_pair_deny_streak = 0;
        ledger.entry_worst_pair_streak_started_ts = 0;
    }
}

fn log_dryrun_snapshot(ledger: &DryRunLedger, params: &DryRunParams, log_ctx: &RolloverLogContext) {
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
    let strategy_params = to_strategy_params(params);
    let effective_max_rounds = strategy_params
        .effective_max_rounds(ledger.spent_total_usdc, time_left_secs)
        .max(ledger.round_idx.saturating_add(1));
    let round_done_reason = if ledger.round_state == RoundPhase::Done {
        if time_left_secs <= params.tail_close_secs {
            Some("tail_close")
        } else if ledger.round_idx >= effective_max_rounds {
            Some("max_rounds")
        } else if ledger.locked {
            Some("locked")
        } else {
            Some("other")
        }
    } else {
        None
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
    data.insert("down_id".to_string(), Value::String(ledger.down_id.clone()));
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
        Value::String(
            match params.mode {
                DryRunMode::Recommend => "recommend",
                DryRunMode::Paper => "paper",
            }
            .to_string(),
        ),
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
    data.insert("cooldown_active".to_string(), Value::Bool(cooldown_active));
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
        "effective_max_rounds".to_string(),
        Value::Number(serde_json::Number::from(effective_max_rounds)),
    );
    data.insert(
        "round_state".to_string(),
        Value::String(ledger.round_state_str().to_string()),
    );
    data.insert(
        "round_done_reason".to_string(),
        round_done_reason
            .map(|v| Value::String(v.to_string()))
            .unwrap_or(Value::Null),
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
        "round_leg1_target_qty".to_string(),
        Value::from(ledger.round_leg1_target_qty),
    );
    data.insert(
        "round_leg1_filled_qty".to_string(),
        Value::from(ledger.round_leg1_filled_qty),
    );
    data.insert(
        "round_leg2_target_qty".to_string(),
        Value::from(ledger.round_leg2_target_qty),
    );
    data.insert(
        "round_leg2_filled_qty".to_string(),
        Value::from(ledger.round_leg2_filled_qty),
    );
    data.insert(
        "round_leg1_entered_ts".to_string(),
        Value::Number(serde_json::Number::from(ledger.round_leg1_entered_ts)),
    );
    data.insert(
        "round_leg2_entered_ts".to_string(),
        Value::Number(serde_json::Number::from(ledger.round_leg2_entered_ts)),
    );
    data.insert(
        "last_apply_ts_ms".to_string(),
        Value::Number(serde_json::Number::from(ledger.last_apply_ts_ms)),
    );
    data.insert(
        "last_round_complete_ts".to_string(),
        Value::Number(serde_json::Number::from(ledger.last_round_complete_ts)),
    );
    data.insert(
        "lock_reopen_used_rounds".to_string(),
        Value::Number(serde_json::Number::from(ledger.lock_reopen_used_rounds)),
    );
    data.insert(
        "open_count_window".to_string(),
        Value::Number(serde_json::Number::from(ledger.open_count_window)),
    );
    data.insert(
        "fill_count_window".to_string(),
        Value::Number(serde_json::Number::from(ledger.fill_count_window)),
    );
    data.insert(
        "timeout_extend_count_window".to_string(),
        Value::Number(serde_json::Number::from(ledger.timeout_extend_count_window)),
    );
    data.insert(
        "requote_count_window".to_string(),
        Value::Number(serde_json::Number::from(ledger.requote_count_window)),
    );
    data.insert(
        "waiting_skip_count_window".to_string(),
        Value::Number(serde_json::Number::from(ledger.waiting_skip_count_window)),
    );
    data.insert(
        "risk_guard_cancel_count_window".to_string(),
        Value::Number(serde_json::Number::from(
            ledger.risk_guard_cancel_count_window,
        )),
    );
    data.insert(
        "risk_guard_cancel_pair_cap_count_window".to_string(),
        Value::Number(serde_json::Number::from(
            ledger.risk_guard_cancel_pair_cap_count_window,
        )),
    );
    data.insert(
        "risk_guard_cancel_other_count_window".to_string(),
        Value::Number(serde_json::Number::from(
            ledger.risk_guard_cancel_other_count_window,
        )),
    );
    data.insert(
        "resting_hard_risk_cancel_count_window".to_string(),
        Value::Number(serde_json::Number::from(
            ledger.resting_hard_risk_cancel_count_window,
        )),
    );
    data.insert(
        "resting_soft_recheck_cancel_count_window".to_string(),
        Value::Number(serde_json::Number::from(
            ledger.resting_soft_recheck_cancel_count_window,
        )),
    );
    data.insert(
        "stale_cancel_count_window".to_string(),
        Value::Number(serde_json::Number::from(ledger.stale_cancel_count_window)),
    );
    data.insert(
        "close_window_cancel_count_window".to_string(),
        Value::Number(serde_json::Number::from(
            ledger.close_window_cancel_count_window,
        )),
    );
    data.insert(
        "entry_worst_pair_block_count_window".to_string(),
        Value::Number(serde_json::Number::from(
            ledger.entry_worst_pair_block_count_window,
        )),
    );
    data.insert(
        "unrecoverable_block_count_window".to_string(),
        Value::Number(serde_json::Number::from(
            ledger.unrecoverable_block_count_window,
        )),
    );
    data.insert(
        "cancel_unrecoverable_count_window".to_string(),
        Value::Number(serde_json::Number::from(
            ledger.cancel_unrecoverable_count_window,
        )),
    );
    data.insert(
        "max_executed_sim_pair_cost_window".to_string(),
        ledger
            .max_executed_sim_pair_cost_window
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "tail_pair_cap_block_count_window".to_string(),
        Value::Number(serde_json::Number::from(
            ledger.tail_pair_cap_block_count_window,
        )),
    );
    data.insert(
        "total_budget_usdc".to_string(),
        Value::from(params.total_budget_usdc),
    );
    data.insert("max_rounds".to_string(), Value::from(params.max_rounds));
    data.insert(
        "total_budget_source".to_string(),
        Value::String(params.total_budget_source.clone()),
    );
    data.insert(
        "round_budget_usdc".to_string(),
        Value::from(params.round_budget_usdc),
    );
    data.insert(
        "round_budget_source".to_string(),
        Value::String(params.round_budget_source.clone()),
    );
    data.insert(
        "lock_min_completed_rounds".to_string(),
        Value::Number(serde_json::Number::from(params.lock_min_completed_rounds)),
    );
    data.insert(
        "lock_min_time_left_secs".to_string(),
        Value::Number(serde_json::Number::from(params.lock_min_time_left_secs)),
    );
    data.insert(
        "lock_min_spent_ratio".to_string(),
        Value::from(params.lock_min_spent_ratio),
    );
    data.insert(
        "lock_force_time_left_secs".to_string(),
        Value::Number(serde_json::Number::from(params.lock_force_time_left_secs)),
    );
    data.insert(
        "lock_strict_abs_net".to_string(),
        Value::Bool(params.lock_strict_abs_net),
    );
    data.insert(
        "round_budget_strict".to_string(),
        Value::Bool(params.round_budget_strict),
    );
    data.insert(
        "tail_close_ignore_margin".to_string(),
        Value::Bool(params.tail_close_ignore_margin),
    );
    data.insert(
        "no_risk_hard_pair_cap".to_string(),
        Value::from(params.no_risk_hard_pair_cap),
    );
    data.insert(
        "no_risk_pair_limit_mode".to_string(),
        Value::String(params.no_risk_pair_limit_mode.as_str().to_string()),
    );
    data.insert(
        "no_risk_enforce_tail".to_string(),
        Value::Bool(params.no_risk_enforce_tail),
    );
    data.insert(
        "no_risk_entry_hedge_price_mode".to_string(),
        Value::String(params.no_risk_entry_hedge_price_mode.as_str().to_string()),
    );
    data.insert(
        "no_risk_entry_ask_slippage_bps".to_string(),
        Value::from(params.no_risk_entry_ask_slippage_bps),
    );
    data.insert(
        "no_risk_entry_worst_hedge_bps".to_string(),
        Value::from(params.no_risk_entry_worst_hedge_bps),
    );
    data.insert(
        "no_risk_entry_pair_headroom_bps".to_string(),
        Value::from(params.no_risk_entry_pair_headroom_bps),
    );
    data.insert(
        "entry_dynamic_cap_enabled".to_string(),
        Value::Bool(params.entry_dynamic_cap_enabled),
    );
    data.insert(
        "entry_dynamic_cap_headroom_bps".to_string(),
        Value::from(params.entry_dynamic_cap_headroom_bps),
    );
    data.insert(
        "entry_dynamic_cap_min_price".to_string(),
        Value::from(params.entry_dynamic_cap_min_price),
    );
    data.insert(
        "entry_dynamic_cap_apply_to_net_increase_only".to_string(),
        Value::Bool(params.entry_dynamic_cap_apply_to_net_increase_only),
    );
    data.insert(
        "entry_target_margin_min_ticks".to_string(),
        Value::from(params.entry_target_margin_min_ticks),
    );
    data.insert(
        "entry_target_margin_min_bps".to_string(),
        Value::from(params.entry_target_margin_min_bps),
    );
    data.insert(
        "entry_max_passive_ticks_for_net_increase".to_string(),
        Value::from(params.entry_max_passive_ticks_for_net_increase),
    );
    data.insert(
        "no_risk_late_new_round_buffer_secs".to_string(),
        Value::from(params.no_risk_late_new_round_buffer_secs),
    );
    data.insert(
        "no_risk_require_completable_round".to_string(),
        Value::Bool(params.no_risk_require_completable_round),
    );
    data.insert(
        "no_risk_strict_zero_unmatched".to_string(),
        Value::Bool(params.no_risk_strict_zero_unmatched),
    );
    data.insert(
        "no_risk_hedge_recoverability_enforce".to_string(),
        Value::Bool(params.no_risk_hedge_recoverability_enforce),
    );
    data.insert(
        "no_risk_hedge_recoverability_eps_bps".to_string(),
        Value::from(params.no_risk_hedge_recoverability_eps_bps),
    );
    data.insert(
        "hedge_recoverability_margin_enforce".to_string(),
        Value::Bool(params.hedge_recoverability_margin_enforce),
    );
    data.insert(
        "hedge_recoverability_margin_min_ticks".to_string(),
        Value::from(params.hedge_recoverability_margin_min_ticks),
    );
    data.insert(
        "hedge_recoverability_margin_min_bps".to_string(),
        Value::from(params.hedge_recoverability_margin_min_bps),
    );
    data.insert(
        "hedge_recoverability_margin_apply_to_net_increase_only".to_string(),
        Value::Bool(params.hedge_recoverability_margin_apply_to_net_increase_only),
    );
    data.insert(
        "open_order_risk_guard".to_string(),
        Value::Bool(params.open_order_risk_guard),
    );
    data.insert(
        "open_order_risk_buffer_bps".to_string(),
        Value::from(params.open_order_risk_buffer_bps),
    );
    data.insert(
        "open_order_risk_guard_require_paired".to_string(),
        Value::Bool(params.open_order_risk_guard_require_paired),
    );
    data.insert(
        "open_order_max_age_secs".to_string(),
        Value::from(params.open_order_max_age_secs),
    );
    data.insert(
        "open_order_unrecoverable_grace_ms".to_string(),
        Value::from(params.open_order_unrecoverable_grace_ms),
    );
    data.insert(
        "maker_fill_estimator_enabled".to_string(),
        Value::Bool(params.maker_fill_estimator_enabled),
    );
    data.insert(
        "maker_fill_horizon_secs".to_string(),
        Value::from(params.maker_fill_horizon_secs),
    );
    data.insert(
        "maker_min_fill_prob".to_string(),
        Value::from(params.maker_min_fill_prob),
    );
    data.insert(
        "maker_queue_ahead_mult".to_string(),
        Value::from(params.maker_queue_ahead_mult),
    );
    data.insert(
        "maker_fill_passive_queue_penalty_per_tick".to_string(),
        Value::from(params.maker_fill_passive_queue_penalty_per_tick),
    );
    data.insert(
        "maker_fill_passive_decay_k".to_string(),
        Value::from(params.maker_fill_passive_decay_k),
    );
    data.insert(
        "maker_flow_floor_per_sec".to_string(),
        Value::from(params.maker_flow_floor_per_sec),
    );
    data.insert(
        "entry_max_top_book_share".to_string(),
        Value::from(params.entry_max_top_book_share),
    );
    data.insert(
        "entry_max_flow_utilization".to_string(),
        Value::from(params.entry_max_flow_utilization),
    );
    data.insert(
        "paper_resting_fill_enabled".to_string(),
        Value::Bool(params.paper_resting_fill_enabled),
    );
    data.insert(
        "paper_order_timeout_secs".to_string(),
        Value::from(params.paper_order_timeout_secs),
    );
    data.insert(
        "paper_order_ack_delay_ms".to_string(),
        Value::from(params.paper_order_ack_delay_ms),
    );
    data.insert(
        "paper_queue_depth_levels".to_string(),
        Value::from(params.paper_queue_depth_levels as u64),
    );
    data.insert(
        "paper_requote_stale_ms".to_string(),
        Value::from(params.paper_requote_stale_ms),
    );
    data.insert(
        "paper_min_requote_interval_ms".to_string(),
        Value::from(params.paper_min_requote_interval_ms),
    );
    data.insert(
        "paper_requote_price_delta_ticks".to_string(),
        Value::from(params.paper_requote_price_delta_ticks),
    );
    data.insert(
        "paper_requote_require_price_move".to_string(),
        Value::Bool(params.paper_requote_require_price_move),
    );
    data.insert(
        "paper_requote_stale_ms_hard".to_string(),
        Value::from(params.paper_requote_stale_ms_hard),
    );
    data.insert(
        "paper_requote_retain_fill_draw".to_string(),
        Value::Bool(params.paper_requote_retain_fill_draw),
    );
    data.insert(
        "paper_requote_progress_retain".to_string(),
        Value::from(params.paper_requote_progress_retain),
    );
    data.insert(
        "requote_min_fill_prob_uplift".to_string(),
        Value::from(params.requote_min_fill_prob_uplift),
    );
    data.insert(
        "requote_queue_stickiness_ratio".to_string(),
        Value::from(params.requote_queue_stickiness_ratio),
    );
    data.insert(
        "requote_stickiness_min_age_secs".to_string(),
        Value::from(params.requote_stickiness_min_age_secs),
    );
    data.insert(
        "open_min_fill_prob".to_string(),
        Value::from(params.open_min_fill_prob),
    );
    let risk_unfinished = params.no_risk_enforce_tail
        && time_left_secs <= params.tail_close_secs
        && (ledger.qty_up - ledger.qty_down).abs() > 1e-9;
    data.insert("risk_unfinished".to_string(), Value::Bool(risk_unfinished));
    data.insert(
        "vol_entry_min_bps".to_string(),
        Value::from(params.vol_entry_min_bps),
    );
    data.insert(
        "vol_entry_lookback_ticks".to_string(),
        Value::Number(serde_json::Number::from(params.vol_entry_lookback_ticks)),
    );
    data.insert(
        "reversal_entry_enabled".to_string(),
        Value::Bool(params.reversal_entry_enabled),
    );
    data.insert(
        "reversal_min_discount_bps".to_string(),
        Value::from(params.reversal_min_discount_bps),
    );
    data.insert(
        "reversal_min_momentum_bps".to_string(),
        Value::from(params.reversal_min_momentum_bps),
    );
    data.insert(
        "reversal_fast_ema_ticks".to_string(),
        Value::Number(serde_json::Number::from(params.reversal_fast_ema_ticks)),
    );
    data.insert(
        "reversal_slow_ema_ticks".to_string(),
        Value::Number(serde_json::Number::from(params.reversal_slow_ema_ticks)),
    );
    data.insert(
        "entry_turn_confirm_ticks".to_string(),
        Value::Number(serde_json::Number::from(params.entry_turn_confirm_ticks)),
    );
    data.insert(
        "entry_turn_min_rebound_bps".to_string(),
        Value::from(params.entry_turn_min_rebound_bps),
    );
    data.insert(
        "entry_pair_buffer_bps".to_string(),
        Value::from(params.entry_pair_buffer_bps),
    );
    data.insert(
        "round_pair_wait_secs".to_string(),
        Value::Number(serde_json::Number::from(params.round_pair_wait_secs)),
    );
    data.insert(
        "leg2_rebalance_discount_bps".to_string(),
        Value::from(params.leg2_rebalance_discount_bps),
    );
    data.insert(
        "force_pair_max_pair_cost".to_string(),
        Value::from(params.force_pair_max_pair_cost),
    );
    data.insert(
        "round_slice_count".to_string(),
        Value::Number(serde_json::Number::from(params.round_slice_count)),
    );
    data.insert(
        "round_dynamic_slicing_enabled".to_string(),
        Value::Bool(params.round_dynamic_slicing_enabled),
    );
    data.insert(
        "round_min_slices".to_string(),
        Value::Number(serde_json::Number::from(params.round_min_slices)),
    );
    data.insert(
        "round_max_slices".to_string(),
        Value::Number(serde_json::Number::from(params.round_max_slices)),
    );
    data.insert(
        "round_min_slice_qty".to_string(),
        Value::from(params.round_min_slice_qty),
    );
    data.insert(
        "entry_pair_regression_mode".to_string(),
        Value::String(params.entry_pair_regression_mode.as_str().to_string()),
    );
    data.insert(
        "entry_pair_regression_soft_band_bps".to_string(),
        Value::from(params.entry_pair_regression_soft_band_bps),
    );
    data.insert(
        "entry_edge_min_bps".to_string(),
        Value::from(params.entry_edge_min_bps),
    );
    data.insert(
        "entry_fill_prob_min".to_string(),
        Value::from(params.entry_fill_prob_min),
    );
    data.insert(
        "entry_passive_gap_soft_max_ticks".to_string(),
        Value::from(params.entry_passive_gap_soft_max_ticks),
    );
    data.insert(
        "inventory_skew_alpha_bps".to_string(),
        Value::from(params.inventory_skew_alpha_bps),
    );
    data.insert(
        "entry_min_timeout_flow_ratio".to_string(),
        Value::from(params.entry_min_timeout_flow_ratio),
    );
    data.insert(
        "entry_fallback_enabled".to_string(),
        Value::Bool(params.entry_fallback_enabled),
    );
    data.insert(
        "entry_fallback_deny_streak".to_string(),
        Value::from(params.entry_fallback_deny_streak),
    );
    data.insert(
        "entry_fallback_window_secs".to_string(),
        Value::from(params.entry_fallback_window_secs),
    );
    data.insert(
        "entry_fallback_duration_secs".to_string(),
        Value::from(params.entry_fallback_duration_secs),
    );
    data.insert(
        "entry_fallback_hedge_mode".to_string(),
        Value::String(params.entry_fallback_hedge_mode.as_str().to_string()),
    );
    data.insert(
        "entry_fallback_worst_hedge_bps".to_string(),
        Value::from(params.entry_fallback_worst_hedge_bps),
    );
    data.insert(
        "lock_allow_reopen_before_freeze".to_string(),
        Value::Bool(params.lock_allow_reopen_before_freeze),
    );
    data.insert(
        "lock_reopen_max_rounds".to_string(),
        Value::Number(serde_json::Number::from(params.lock_reopen_max_rounds)),
    );
    data.insert(
        "round_min_start_gap_secs".to_string(),
        Value::Number(serde_json::Number::from(params.round_min_start_gap_secs)),
    );
    data.insert(
        "opening_no_trade_secs".to_string(),
        Value::Number(serde_json::Number::from(params.opening_no_trade_secs)),
    );
    data.insert(
        "min_apply_interval_ms".to_string(),
        Value::Number(serde_json::Number::from(params.min_apply_interval_ms)),
    );
    data.insert(
        "round_plan_vol_entry_bps".to_string(),
        Value::from(round_plan.vol_entry_bps),
    );
    data.insert(
        "round_plan_vol_entry_ok".to_string(),
        Value::Bool(round_plan.vol_entry_ok),
    );
    data.insert(
        "round_plan_reversal_up_ok".to_string(),
        Value::Bool(round_plan.reversal_up_ok),
    );
    data.insert(
        "round_plan_reversal_down_ok".to_string(),
        Value::Bool(round_plan.reversal_down_ok),
    );
    data.insert(
        "round_plan_turn_up_ok".to_string(),
        Value::Bool(round_plan.turn_up_ok),
    );
    data.insert(
        "round_plan_turn_down_ok".to_string(),
        Value::Bool(round_plan.turn_down_ok),
    );
    data.insert(
        "round_plan_entry_worst_pair_cost".to_string(),
        round_plan
            .entry_worst_pair_cost
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "round_plan_entry_worst_pair_ok".to_string(),
        Value::Bool(round_plan.entry_worst_pair_ok),
    );
    data.insert(
        "round_plan_entry_timeout_flow_ratio".to_string(),
        round_plan
            .entry_timeout_flow_ratio
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "round_plan_entry_timeout_flow_ok".to_string(),
        Value::Bool(round_plan.entry_timeout_flow_ok),
    );
    data.insert(
        "round_plan_entry_fillability_ok".to_string(),
        Value::Bool(round_plan.entry_fillability_ok),
    );
    data.insert(
        "round_plan_pair_quality_ok".to_string(),
        Value::Bool(round_plan.pair_quality_ok),
    );
    data.insert(
        "round_plan_pair_regression_ok".to_string(),
        Value::Bool(round_plan.pair_regression_ok),
    );
    data.insert(
        "round_plan_can_open_round_base_ok".to_string(),
        Value::Bool(round_plan.can_open_round_base_ok),
    );
    data.insert(
        "round_plan_entry_edge_bps".to_string(),
        round_plan
            .entry_edge_bps
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "round_plan_can_start_block_reason".to_string(),
        round_plan
            .can_start_block_reason
            .as_ref()
            .map(|v| Value::String(v.clone()))
            .unwrap_or(Value::Null),
    );
    data.insert(
        "round_plan_entry_regime_score".to_string(),
        round_plan
            .entry_regime_score
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "round_plan_entry_depth_cap_qty".to_string(),
        round_plan
            .entry_depth_cap_qty
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "round_plan_entry_flow_cap_qty".to_string(),
        round_plan
            .entry_flow_cap_qty
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "round_plan_slice_count_planned".to_string(),
        round_plan
            .slice_count_planned
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "round_plan_slice_qty_current".to_string(),
        round_plan
            .slice_qty_current
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "round_plan_entry_final_qty_slice".to_string(),
        round_plan
            .entry_final_qty_slice
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "round_plan_entry_fallback_active".to_string(),
        Value::Bool(round_plan.entry_fallback_active),
    );
    data.insert(
        "round_plan_entry_fallback_armed".to_string(),
        Value::Bool(round_plan.entry_fallback_armed),
    );
    data.insert(
        "round_plan_entry_fallback_trigger_reason".to_string(),
        round_plan
            .entry_fallback_trigger_reason
            .as_ref()
            .map(|v| Value::String(v.clone()))
            .unwrap_or(Value::Null),
    );
    data.insert(
        "round_plan_entry_fallback_blocked_by_recoverability".to_string(),
        Value::Bool(round_plan.entry_fallback_blocked_by_recoverability),
    );
    data.insert(
        "round_plan_new_round_cutoff_secs".to_string(),
        Value::from(round_plan.new_round_cutoff_secs),
    );
    data.insert(
        "round_plan_late_new_round_blocked".to_string(),
        Value::Bool(round_plan.late_new_round_blocked),
    );
    data.insert(
        "first_leg_turning_score".to_string(),
        round_plan
            .first_leg_turning_score
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "entry_worst_pair_deny_streak".to_string(),
        Value::from(ledger.entry_worst_pair_deny_streak),
    );
    data.insert(
        "entry_worst_pair_streak_started_ts".to_string(),
        Value::from(ledger.entry_worst_pair_streak_started_ts),
    );
    data.insert(
        "entry_fallback_until_ts".to_string(),
        Value::from(ledger.entry_fallback_until_ts),
    );
    data.insert(
        "open_orders_active".to_string(),
        Value::from(ledger.open_orders_active),
    );
    data.insert(
        "pair_mid_vol_bps".to_string(),
        Value::from(ledger.pair_mid_vol_bps),
    );
    data.insert(
        "mid_up_momentum_bps".to_string(),
        Value::from(ledger.mid_up_momentum_bps),
    );
    data.insert(
        "mid_down_momentum_bps".to_string(),
        Value::from(ledger.mid_down_momentum_bps),
    );
    data.insert(
        "mid_up_discount_bps".to_string(),
        Value::from(ledger.mid_up_discount_bps),
    );
    data.insert(
        "mid_down_discount_bps".to_string(),
        Value::from(ledger.mid_down_discount_bps),
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
        "exchange_ts_ms_up".to_string(),
        ledger
            .exchange_ts_ms_up
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "recv_ts_ms_up".to_string(),
        ledger.recv_ts_ms_up.map(Value::from).unwrap_or(Value::Null),
    );
    data.insert(
        "latency_ms_up".to_string(),
        ledger.latency_ms_up.map(Value::from).unwrap_or(Value::Null),
    );
    data.insert(
        "best_bid_size_up".to_string(),
        ledger
            .best_bid_size_up
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "best_ask_size_up".to_string(),
        ledger
            .best_ask_size_up
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "bid_consumption_rate_up".to_string(),
        Value::from(ledger.bid_consumption_rate_up),
    );
    data.insert(
        "ask_consumption_rate_up".to_string(),
        Value::from(ledger.ask_consumption_rate_up),
    );
    data.insert(
        "best_bid_down".to_string(),
        ledger.best_bid_down.map(Value::from).unwrap_or(Value::Null),
    );
    data.insert(
        "best_ask_down".to_string(),
        ledger.best_ask_down.map(Value::from).unwrap_or(Value::Null),
    );
    data.insert(
        "exchange_ts_ms_down".to_string(),
        ledger
            .exchange_ts_ms_down
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "recv_ts_ms_down".to_string(),
        ledger
            .recv_ts_ms_down
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "latency_ms_down".to_string(),
        ledger
            .latency_ms_down
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "best_bid_size_down".to_string(),
        ledger
            .best_bid_size_down
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "best_ask_size_down".to_string(),
        ledger
            .best_ask_size_down
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "bid_consumption_rate_down".to_string(),
        Value::from(ledger.bid_consumption_rate_down),
    );
    data.insert(
        "ask_consumption_rate_down".to_string(),
        Value::from(ledger.ask_consumption_rate_down),
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
    data.insert("unhedged_down".to_string(), Value::from(unhedged_down));
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
        Value::String(
            match action.leg {
                TradeLeg::Up => "UP",
                TradeLeg::Down => "DOWN",
            }
            .to_string(),
        ),
    );
    data.insert(
        "side".to_string(),
        Value::String(
            match action.side {
                TradeSide::Buy => "BUY",
                TradeSide::Sell => "SELL",
            }
            .to_string(),
        ),
    );
    data.insert(
        "kind".to_string(),
        Value::String(
            match action.kind {
                TradeKind::Maker => "MAKER",
                TradeKind::Taker => "TAKER",
            }
            .to_string(),
        ),
    );
    data.insert("qty".to_string(), Value::from(action.qty));
    data.insert(
        "fill_price".to_string(),
        sim.fill_price.map(Value::from).unwrap_or(Value::Null),
    );
    data.insert(
        "maker_fill_prob".to_string(),
        sim.maker_fill_prob.map(Value::from).unwrap_or(Value::Null),
    );
    data.insert(
        "maker_queue_ahead".to_string(),
        sim.maker_queue_ahead
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "maker_expected_consumed".to_string(),
        sim.maker_expected_consumed
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "maker_consumption_rate".to_string(),
        sim.maker_consumption_rate
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "maker_horizon_secs".to_string(),
        sim.maker_horizon_secs
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "sim_pair_cost".to_string(),
        sim.new_pair_cost.map(Value::from).unwrap_or(Value::Null),
    );
    data.insert("sim_hedgeable".to_string(), Value::from(sim.new_hedgeable));
    data.insert(
        "sim_unhedged_up".to_string(),
        Value::from(sim.new_unhedged_up),
    );
    data.insert(
        "sim_unhedged_down".to_string(),
        Value::from(sim.new_unhedged_down),
    );
    data.insert(
        "hedge_recoverable_now".to_string(),
        sim.hedge_recoverable_now
            .map(Value::Bool)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "required_opp_avg_price_cap".to_string(),
        sim.required_opp_avg_price_cap
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "current_opp_best_ask".to_string(),
        sim.current_opp_best_ask
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "required_hedge_qty".to_string(),
        sim.required_hedge_qty
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "hedge_margin_to_opp_ask".to_string(),
        sim.hedge_margin_to_opp_ask
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "hedge_margin_required".to_string(),
        sim.hedge_margin_required
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "open_margin_surplus".to_string(),
        match (sim.hedge_margin_to_opp_ask, sim.hedge_margin_required) {
            (Some(actual), Some(required)) => Value::from(actual - required),
            _ => Value::Null,
        },
    );
    data.insert(
        "hedge_margin_ok".to_string(),
        sim.hedge_margin_ok.map(Value::Bool).unwrap_or(Value::Null),
    );
    data.insert(
        "entry_quote_base_postonly_price".to_string(),
        sim.entry_quote_base_postonly_price
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "entry_quote_dynamic_cap_price".to_string(),
        sim.entry_quote_dynamic_cap_price
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "entry_quote_final_price".to_string(),
        sim.entry_quote_final_price
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "entry_quote_cap_active".to_string(),
        sim.entry_quote_cap_active
            .map(Value::Bool)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "entry_quote_cap_bind".to_string(),
        sim.entry_quote_cap_bind
            .map(Value::Bool)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "passive_gap_abs".to_string(),
        sim.passive_gap_abs.map(Value::from).unwrap_or(Value::Null),
    );
    data.insert(
        "passive_gap_ticks".to_string(),
        sim.passive_gap_ticks
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "improves_hedge".to_string(),
        Value::Bool(sim.improves_hedge),
    );
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
        gate.reserve_needed_usdc
            .map(Value::from)
            .unwrap_or(Value::Null),
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
        DenyReason::EntryWorstPair => "entry_worst_pair",
        DenyReason::HedgeNotRecoverable => "hedge_not_recoverable",
        DenyReason::HedgeMarginInsufficient => "hedge_margin_insufficient",
        DenyReason::OpenMarginTooThin => "open_margin_too_thin",
        DenyReason::EntryEdgeTooThin => "entry_edge_too_thin",
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
        DenyReason::LockedStrictAbsNet => "locked_strict_abs_net",
        DenyReason::LockedMaxRounds => "locked_max_rounds",
        DenyReason::LockedTailFreeze => "locked_tail_freeze",
        DenyReason::LockedWaitingPair => "locked_waiting_pair",
        DenyReason::LockedPolicyHold => "locked_policy_hold",
        DenyReason::LowMakerFillProb => "low_maker_fill_prob",
    }
}

fn log_dryrun_apply(
    before: &DryRunLedger,
    after: &DryRunLedger,
    applied: &CandidateEval,
    executed: bool,
    params: &DryRunParams,
    log_ctx: &RolloverLogContext,
) {
    if !log_ctx.enabled {
        return;
    }
    let mut data = serde_json::Map::new();
    data.insert(
        "decision_seq".to_string(),
        Value::Number(serde_json::Number::from(after.decision_seq)),
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
    data.insert("executed".to_string(), Value::Bool(executed));
    data.insert("before_qty_up".to_string(), Value::from(before.qty_up));
    data.insert("before_cost_up".to_string(), Value::from(before.cost_up));
    data.insert("before_qty_down".to_string(), Value::from(before.qty_down));
    data.insert(
        "before_cost_down".to_string(),
        Value::from(before.cost_down),
    );
    data.insert(
        "before_spent_total_usdc".to_string(),
        Value::from(before.spent_total_usdc),
    );
    data.insert(
        "before_spent_round_usdc".to_string(),
        Value::from(before.spent_round_usdc),
    );
    data.insert(
        "before_round_idx".to_string(),
        Value::Number(serde_json::Number::from(before.round_idx)),
    );
    data.insert("after_qty_up".to_string(), Value::from(after.qty_up));
    data.insert("after_cost_up".to_string(), Value::from(after.cost_up));
    data.insert("after_qty_down".to_string(), Value::from(after.qty_down));
    data.insert("after_cost_down".to_string(), Value::from(after.cost_down));
    data.insert(
        "after_spent_total_usdc".to_string(),
        Value::from(after.spent_total_usdc),
    );
    data.insert(
        "after_spent_round_usdc".to_string(),
        Value::from(after.spent_round_usdc),
    );
    data.insert(
        "after_round_idx".to_string(),
        Value::Number(serde_json::Number::from(after.round_idx)),
    );
    data.insert(
        "note".to_string(),
        Value::String("paper_only_apply_result".to_string()),
    );
    log_jsonl(log_ctx, "dryrun_apply", data);
}

fn action_token_id<'a>(ledger: &'a DryRunLedger, action: &CandidateAction) -> &'a str {
    match action.leg {
        TradeLeg::Up => &ledger.up_id,
        TradeLeg::Down => &ledger.down_id,
    }
}

fn action_order_side(action: &CandidateAction) -> OrderSide {
    match action.side {
        TradeSide::Buy => OrderSide::Buy,
        TradeSide::Sell => OrderSide::Sell,
    }
}

fn build_client_order_id(ledger: &DryRunLedger, action: &CandidateAction) -> String {
    let leg = match action.leg {
        TradeLeg::Up => "up",
        TradeLeg::Down => "down",
    };
    let side = match action.side {
        TradeSide::Buy => "buy",
        TradeSide::Sell => "sell",
    };
    format!(
        "{}-{}-{}-{}",
        ledger.market_slug, ledger.decision_seq, leg, side
    )
}

fn stable_hash_hex(input: &str) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in input.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{:016x}", hash)
}

fn log_user_ws_status(execution: &ExecutionRuntime, log_ctx: &RolloverLogContext, status: &str) {
    if !log_ctx.enabled {
        return;
    }
    let mut data = serde_json::Map::new();
    data.insert("status".to_string(), Value::String(status.to_string()));
    data.insert("state".to_string(), Value::String(status.to_string()));
    data.insert("conn_id".to_string(), Value::String("runtime".to_string()));
    data.insert("attempt".to_string(), Value::from(0_u64));
    data.insert("backoff_secs".to_string(), Value::from(0_u64));
    data.insert("ts_ms".to_string(), Value::from(now_ts_ms_i64()));
    data.insert("since_connected_ms".to_string(), Value::Null);
    data.insert("err_kind".to_string(), Value::Null);
    data.insert(
        "execution_mode".to_string(),
        Value::String(execution.mode.as_str().to_string()),
    );
    data.insert(
        "adapter".to_string(),
        Value::String(execution.adapter_name.clone()),
    );
    data.insert(
        "user_ws_enabled".to_string(),
        Value::Bool(execution.user_ws_enabled),
    );
    data.insert(
        "sim_fill_enabled".to_string(),
        Value::Bool(execution.sim_fill_enabled),
    );
    log_jsonl(log_ctx, "user_ws_status", data);
}

fn log_live_intent(
    ledger: &DryRunLedger,
    applied: &CandidateEval,
    execution: &ExecutionRuntime,
    log_ctx: &RolloverLogContext,
) {
    if !log_ctx.enabled {
        return;
    }
    let client_order_id = build_client_order_id(ledger, &applied.action);
    let order_id_str = client_order_id.clone();
    let would_send_hash = stable_hash_hex(&format!(
        "{}|{}|{}|{}|{}",
        action_token_id(ledger, &applied.action),
        match applied.action.side {
            TradeSide::Buy => "BUY",
            TradeSide::Sell => "SELL",
        },
        applied.sim.fill_price.unwrap_or(0.0),
        applied.action.qty,
        ledger.tick_size_current
    ));
    let mut data = serde_json::Map::new();
    data.insert(
        "decision_seq".to_string(),
        Value::Number(serde_json::Number::from(ledger.decision_seq)),
    );
    data.insert(
        "market_slug".to_string(),
        Value::String(ledger.market_slug.clone()),
    );
    data.insert(
        "token_id".to_string(),
        Value::String(action_token_id(ledger, &applied.action).to_string()),
    );
    data.insert(
        "side".to_string(),
        Value::String(
            match applied.action.side {
                TradeSide::Buy => "BUY",
                TradeSide::Sell => "SELL",
            }
            .to_string(),
        ),
    );
    data.insert(
        "action_kind".to_string(),
        Value::String(
            match applied.action.kind {
                TradeKind::Maker => "MAKER",
                TradeKind::Taker => "TAKER",
            }
            .to_string(),
        ),
    );
    data.insert("qty".to_string(), Value::from(applied.action.qty));
    data.insert("size".to_string(), Value::from(applied.action.qty));
    data.insert("post_only".to_string(), Value::Bool(true));
    data.insert(
        "tick_size".to_string(),
        Value::from(ledger.tick_size_current),
    );
    data.insert(
        "tick_size_current".to_string(),
        Value::from(ledger.tick_size_current),
    );
    data.insert(
        "price".to_string(),
        applied
            .sim
            .fill_price
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "execution_mode".to_string(),
        Value::String(execution.mode.as_str().to_string()),
    );
    data.insert(
        "adapter".to_string(),
        Value::String(execution.adapter_name.clone()),
    );
    data.insert(
        "client_order_id".to_string(),
        Value::String(client_order_id),
    );
    data.insert("order_id_str".to_string(), Value::String(order_id_str));
    data.insert(
        "would_send_hash".to_string(),
        Value::String(would_send_hash),
    );
    data.insert("ts_ms".to_string(), Value::from(now_ts_ms_i64()));
    log_jsonl(log_ctx, "live_intent", data);
}

fn log_live_place(
    ledger: &DryRunLedger,
    applied: &CandidateEval,
    execution: &ExecutionRuntime,
    ack: &PlaceAck,
    log_ctx: &RolloverLogContext,
) {
    if !log_ctx.enabled {
        return;
    }
    let client_order_id = build_client_order_id(ledger, &applied.action);
    let order_id_str = ack
        .order_id
        .clone()
        .unwrap_or_else(|| client_order_id.clone());
    let would_send_hash = stable_hash_hex(&format!(
        "{}|{}|{}|{}|{}",
        action_token_id(ledger, &applied.action),
        match applied.action.side {
            TradeSide::Buy => "BUY",
            TradeSide::Sell => "SELL",
        },
        applied.sim.fill_price.unwrap_or(0.0),
        applied.action.qty,
        ledger.tick_size_current
    ));
    let mut data = serde_json::Map::new();
    data.insert(
        "decision_seq".to_string(),
        Value::Number(serde_json::Number::from(ledger.decision_seq)),
    );
    data.insert(
        "market_slug".to_string(),
        Value::String(ledger.market_slug.clone()),
    );
    data.insert(
        "token_id".to_string(),
        Value::String(action_token_id(ledger, &applied.action).to_string()),
    );
    data.insert(
        "side".to_string(),
        Value::String(
            match applied.action.side {
                TradeSide::Buy => "BUY",
                TradeSide::Sell => "SELL",
            }
            .to_string(),
        ),
    );
    data.insert(
        "order_type".to_string(),
        Value::String("POST_ONLY".to_string()),
    );
    data.insert(
        "price".to_string(),
        applied
            .sim
            .fill_price
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert("qty".to_string(), Value::from(applied.action.qty));
    data.insert("size".to_string(), Value::from(applied.action.qty));
    data.insert("post_only".to_string(), Value::Bool(true));
    data.insert(
        "tick_size".to_string(),
        Value::from(ledger.tick_size_current),
    );
    data.insert(
        "tick_size_current".to_string(),
        Value::from(ledger.tick_size_current),
    );
    data.insert("accepted".to_string(), Value::Bool(ack.accepted));
    data.insert(
        "order_id".to_string(),
        ack.order_id
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    data.insert(
        "reason".to_string(),
        ack.reason.clone().map(Value::String).unwrap_or(Value::Null),
    );
    data.insert(
        "adapter".to_string(),
        Value::String(execution.adapter_name.clone()),
    );
    data.insert(
        "execution_mode".to_string(),
        Value::String(execution.mode.as_str().to_string()),
    );
    data.insert(
        "client_order_id".to_string(),
        Value::String(client_order_id),
    );
    data.insert("order_id_str".to_string(), Value::String(order_id_str));
    data.insert(
        "would_send_hash".to_string(),
        Value::String(would_send_hash),
    );
    data.insert("ts_ms".to_string(), Value::from(now_ts_ms_i64()));
    log_jsonl(log_ctx, "live_place", data);
}

fn log_live_cancel(
    market_slug: &str,
    decision_seq: u64,
    reason: &str,
    client_order_id: &str,
    order_id_str: &str,
    token_id: Option<&str>,
    side: Option<&str>,
    price: Option<f64>,
    execution: &ExecutionRuntime,
    result: &Result<usize>,
    log_ctx: &RolloverLogContext,
) {
    if !log_ctx.enabled {
        return;
    }
    let mut data = serde_json::Map::new();
    data.insert(
        "market_slug".to_string(),
        Value::String(market_slug.to_string()),
    );
    data.insert("decision_seq".to_string(), Value::from(decision_seq));
    data.insert("reason".to_string(), Value::String(reason.to_string()));
    data.insert(
        "client_order_id".to_string(),
        Value::String(client_order_id.to_string()),
    );
    data.insert(
        "order_id_str".to_string(),
        Value::String(order_id_str.to_string()),
    );
    data.insert(
        "would_send_hash".to_string(),
        Value::String(stable_hash_hex(&format!(
            "{}|{}|{}|{}|{}",
            market_slug, decision_seq, reason, client_order_id, order_id_str
        ))),
    );
    data.insert(
        "token_id".to_string(),
        token_id
            .map(|v| Value::String(v.to_string()))
            .unwrap_or(Value::Null),
    );
    data.insert(
        "side".to_string(),
        side.map(|v| Value::String(v.to_string()))
            .unwrap_or(Value::Null),
    );
    data.insert(
        "price".to_string(),
        price.map(Value::from).unwrap_or(Value::Null),
    );
    data.insert(
        "execution_mode".to_string(),
        Value::String(execution.mode.as_str().to_string()),
    );
    data.insert(
        "adapter".to_string(),
        Value::String(execution.adapter_name.clone()),
    );
    match result {
        Ok(canceled) => {
            data.insert("ok".to_string(), Value::Bool(true));
            data.insert("canceled_count".to_string(), Value::from(*canceled as u64));
            data.insert("error".to_string(), Value::Null);
        }
        Err(err) => {
            data.insert("ok".to_string(), Value::Bool(false));
            data.insert("canceled_count".to_string(), Value::from(0_u64));
            data.insert("error".to_string(), Value::String(err.to_string()));
        }
    }
    data.insert("ts_ms".to_string(), Value::from(now_ts_ms_i64()));
    log_jsonl(log_ctx, "live_cancel", data);
}

fn log_live_skip(
    ledger: &DryRunLedger,
    reason: &str,
    tail_mode: &str,
    execution: &ExecutionRuntime,
    log_ctx: &RolloverLogContext,
) {
    if !log_ctx.enabled {
        return;
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
    data.insert("reason".to_string(), Value::String(reason.to_string()));
    data.insert(
        "tail_mode".to_string(),
        Value::String(tail_mode.to_string()),
    );
    data.insert(
        "execution_mode".to_string(),
        Value::String(execution.mode.as_str().to_string()),
    );
    data.insert(
        "adapter".to_string(),
        Value::String(execution.adapter_name.clone()),
    );
    data.insert("ts_ms".to_string(), Value::from(now_ts_ms_i64()));
    log_jsonl(log_ctx, "live_skip", data);
}

fn log_user_ws_fill(
    ledger: &DryRunLedger,
    applied: &CandidateEval,
    execution: &ExecutionRuntime,
    raw_type: &str,
    log_ctx: &RolloverLogContext,
) {
    if !log_ctx.enabled {
        return;
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
    data.insert(
        "round_idx".to_string(),
        Value::Number(serde_json::Number::from(ledger.round_idx)),
    );
    data.insert(
        "leg".to_string(),
        Value::String(
            match applied.action.leg {
                TradeLeg::Up => "UP",
                TradeLeg::Down => "DOWN",
            }
            .to_string(),
        ),
    );
    data.insert(
        "token_id".to_string(),
        Value::String(action_token_id(ledger, &applied.action).to_string()),
    );
    data.insert(
        "side".to_string(),
        Value::String(
            match applied.action.side {
                TradeSide::Buy => "BUY",
                TradeSide::Sell => "SELL",
            }
            .to_string(),
        ),
    );
    data.insert(
        "price".to_string(),
        applied
            .sim
            .fill_price
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    data.insert("qty".to_string(), Value::from(applied.action.qty));
    data.insert("raw_type".to_string(), Value::String(raw_type.to_string()));
    data.insert(
        "execution_mode".to_string(),
        Value::String(execution.mode.as_str().to_string()),
    );
    data.insert(
        "adapter".to_string(),
        Value::String(execution.adapter_name.clone()),
    );
    log_jsonl(log_ctx, "user_ws_fill", data);
}

fn log_reconcile_snapshot(
    market_slug: &str,
    execution: &ExecutionRuntime,
    open_orders_result: &Result<Vec<OpenOrder>>,
    log_ctx: &RolloverLogContext,
) {
    if !log_ctx.enabled {
        return;
    }
    let mut data = serde_json::Map::new();
    data.insert(
        "market_slug".to_string(),
        Value::String(market_slug.to_string()),
    );
    data.insert(
        "execution_mode".to_string(),
        Value::String(execution.mode.as_str().to_string()),
    );
    data.insert(
        "adapter".to_string(),
        Value::String(execution.adapter_name.clone()),
    );
    match open_orders_result {
        Ok(open_orders) => {
            data.insert("ok".to_string(), Value::Bool(true));
            data.insert(
                "open_order_count".to_string(),
                Value::from(open_orders.len() as u64),
            );
            let preview: Vec<Value> = open_orders
                .iter()
                .take(10)
                .map(|order| {
                    json!({
                        "order_id": order.order_id,
                        "client_order_id": order.client_order_id,
                        "token_id": order.token_id,
                        "price": order.price,
                        "qty": order.qty,
                    })
                })
                .collect();
            data.insert("open_orders_preview".to_string(), Value::Array(preview));
            data.insert("error".to_string(), Value::Null);
        }
        Err(err) => {
            data.insert("ok".to_string(), Value::Bool(false));
            data.insert("open_order_count".to_string(), Value::from(0_u64));
            data.insert("open_orders_preview".to_string(), Value::Array(Vec::new()));
            data.insert("error".to_string(), Value::String(err.to_string()));
        }
    }
    log_jsonl(log_ctx, "reconcile_snapshot", data);
}

fn apply_user_ws_fill_to_ledger(
    ledger: &mut DryRunLedger,
    params: &DryRunParams,
    fill: &UserWsFillEvent,
) {
    if let Some(fill_market_slug) = fill.market_slug.as_ref() {
        if !fill_market_slug.is_empty() && fill_market_slug != &ledger.market_slug {
            return;
        }
    }
    let leg = if fill.token_id == ledger.up_id {
        Some(TradeLeg::Up)
    } else if fill.token_id == ledger.down_id {
        Some(TradeLeg::Down)
    } else {
        None
    };
    let Some(leg) = leg else {
        return;
    };
    if fill.size <= 0.0 || fill.price <= 0.0 {
        return;
    }
    let pre_abs_net = (ledger.qty_up - ledger.qty_down).abs();

    match (leg, fill.side) {
        (TradeLeg::Up, TradeSide::Buy) => {
            ledger.qty_up += fill.size;
            ledger.cost_up += fill.size * fill.price;
            ledger.spent_total_usdc += fill.size * fill.price;
            ledger.spent_round_usdc += fill.size * fill.price;
        }
        (TradeLeg::Down, TradeSide::Buy) => {
            ledger.qty_down += fill.size;
            ledger.cost_down += fill.size * fill.price;
            ledger.spent_total_usdc += fill.size * fill.price;
            ledger.spent_round_usdc += fill.size * fill.price;
        }
        (TradeLeg::Up, TradeSide::Sell) => {
            if ledger.qty_up > 0.0 {
                let reduce = fill.size.min(ledger.qty_up);
                if let Some(avg) = ledger.avg_up() {
                    ledger.qty_up -= reduce;
                    ledger.cost_up = (ledger.cost_up - reduce * avg).max(0.0);
                }
            }
        }
        (TradeLeg::Down, TradeSide::Sell) => {
            if ledger.qty_down > 0.0 {
                let reduce = fill.size.min(ledger.qty_down);
                if let Some(avg) = ledger.avg_down() {
                    ledger.qty_down -= reduce;
                    ledger.cost_down = (ledger.cost_down - reduce * avg).max(0.0);
                }
            }
        }
    }

    let now_ts = u64::try_from(Utc::now().timestamp()).unwrap_or(0);
    let time_left_secs = if ledger.end_ts > now_ts {
        ledger.end_ts - now_ts
    } else {
        0
    };
    let eps = 1e-9;
    match ledger.round_state {
        RoundPhase::Idle => {
            if time_left_secs > params.tail_close_secs && fill.side == TradeSide::Buy && pre_abs_net <= eps {
                ledger.round_state = RoundPhase::Leg1Accumulating;
                ledger.round_leg1 = Some(leg);
                ledger.round_qty_target = fill.size;
                ledger.round_leg1_entered_ts = now_ts;
            }
        }
        RoundPhase::Leg1Accumulating => {
            if let Some(leg1) = ledger.round_leg1 {
                let balanced = (ledger.qty_up - ledger.qty_down).abs() <= eps;
                if leg != leg1 && balanced {
                    ledger.round_idx = ledger.round_idx.saturating_add(1);
                    ledger.round_leg1 = None;
                    ledger.round_qty_target = 0.0;
                    ledger.round_leg1_entered_ts = 0;
                    ledger.spent_round_usdc = 0.0;
                    if time_left_secs <= params.tail_close_secs {
                        ledger.round_state = RoundPhase::Done;
                    } else {
                        ledger.round_state = RoundPhase::Idle;
                    }
                }
            }
        }
        RoundPhase::Leg2Balancing => {}
        RoundPhase::Done => {}
    }

    let post_abs_net = (ledger.qty_up - ledger.qty_down).abs();
    if ledger.round_state == RoundPhase::Leg2Balancing && post_abs_net <= eps {
        ledger.round_leg1 = None;
        ledger.round_qty_target = 0.0;
        ledger.round_leg1_entered_ts = 0;
        ledger.round_leg2_entered_ts = 0;
        ledger.round_leg2_anchor_price = None;
        ledger.round_leg1_target_qty = 0.0;
        ledger.round_leg1_filled_qty = 0.0;
        ledger.round_leg2_target_qty = 0.0;
        ledger.round_leg2_filled_qty = 0.0;
        ledger.spent_round_usdc = 0.0;
        ledger.round_state = if time_left_secs <= params.tail_close_secs {
            RoundPhase::Done
        } else {
            RoundPhase::Idle
        };
    }
    ledger.update_lock(params, now_ts_ms());
}

fn handle_user_ws_event(
    ledger: &mut DryRunLedger,
    params: &DryRunParams,
    execution: &ExecutionRuntime,
    event: UserWsEvent,
    rollover_log_json: bool,
    rollover_log_verbose: bool,
    series_slug_value: &str,
    rollover_generation: u64,
) {
    let log_ctx = RolloverLogContext {
        enabled: rollover_log_json,
        verbose: rollover_log_verbose,
        series_slug: series_slug_value.to_string(),
        rollover_gen: rollover_generation,
    };
    match event {
        UserWsEvent::Status(mut data) => {
            data.insert(
                "execution_mode".to_string(),
                Value::String(execution.mode.as_str().to_string()),
            );
            data.insert(
                "adapter".to_string(),
                Value::String(execution.adapter_name.clone()),
            );
            log_jsonl(&log_ctx, "user_ws_status", data);
        }
        UserWsEvent::Error(mut data) => {
            data.insert(
                "execution_mode".to_string(),
                Value::String(execution.mode.as_str().to_string()),
            );
            data.insert(
                "adapter".to_string(),
                Value::String(execution.adapter_name.clone()),
            );
            log_jsonl(&log_ctx, "user_ws_error", data);
        }
        UserWsEvent::Order(mut data) => {
            if data.get("raw_type").is_none() {
                data.insert("raw_type".to_string(), Value::String("order".to_string()));
            }
            if data.get("ts_ms").is_none() {
                data.insert("ts_ms".to_string(), Value::from(now_ts_ms_i64()));
            }
            data.insert(
                "tick_size".to_string(),
                Value::from(ledger.tick_size_current),
            );
            data.insert(
                "tick_size_current".to_string(),
                Value::from(ledger.tick_size_current),
            );
            data.insert(
                "execution_mode".to_string(),
                Value::String(execution.mode.as_str().to_string()),
            );
            data.insert(
                "adapter".to_string(),
                Value::String(execution.adapter_name.clone()),
            );
            log_jsonl(&log_ctx, "user_ws_order", data);
        }
        UserWsEvent::Fill(fill) => {
            let mut data = serde_json::Map::new();
            data.insert(
                "market_slug".to_string(),
                Value::String(
                    fill.market_slug
                        .clone()
                        .unwrap_or_else(|| ledger.market_slug.clone()),
                ),
            );
            data.insert("token_id".to_string(), Value::String(fill.token_id.clone()));
            data.insert(
                "side".to_string(),
                Value::String(
                    match fill.side {
                        TradeSide::Buy => "BUY",
                        TradeSide::Sell => "SELL",
                    }
                    .to_string(),
                ),
            );
            data.insert("price".to_string(), Value::from(fill.price));
            data.insert("size".to_string(), Value::from(fill.size));
            data.insert("raw_type".to_string(), Value::String(fill.raw_type.clone()));
            data.insert(
                "order_id".to_string(),
                fill.order_id
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            );
            data.insert(
                "client_order_id".to_string(),
                fill.client_order_id
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            );
            data.insert("ts_ms".to_string(), Value::from(fill.ts_ms));
            data.insert(
                "tick_size".to_string(),
                Value::from(ledger.tick_size_current),
            );
            data.insert(
                "tick_size_current".to_string(),
                Value::from(ledger.tick_size_current),
            );
            data.insert(
                "execution_mode".to_string(),
                Value::String(execution.mode.as_str().to_string()),
            );
            data.insert(
                "adapter".to_string(),
                Value::String(execution.adapter_name.clone()),
            );
            log_jsonl(&log_ctx, "user_ws_fill", data);
            apply_user_ws_fill_to_ledger(ledger, params, &fill);
        }
    }
}

fn log_synthetic_user_ws_order(
    ledger: &DryRunLedger,
    execution: &ExecutionRuntime,
    order_id: &str,
    client_order_id: &str,
    token_id: &str,
    side: &str,
    price: f64,
    size: f64,
    status: &str,
    raw_type: &str,
    log_ctx: &RolloverLogContext,
) {
    log_synthetic_user_ws_order_with_extra(
        ledger,
        execution,
        order_id,
        client_order_id,
        token_id,
        side,
        price,
        size,
        status,
        raw_type,
        None,
        log_ctx,
    );
}

fn log_synthetic_user_ws_order_with_extra(
    ledger: &DryRunLedger,
    execution: &ExecutionRuntime,
    order_id: &str,
    client_order_id: &str,
    token_id: &str,
    side: &str,
    price: f64,
    size: f64,
    status: &str,
    raw_type: &str,
    extra: Option<&serde_json::Map<String, Value>>,
    log_ctx: &RolloverLogContext,
) {
    if !log_ctx.enabled {
        return;
    }
    let mut data = serde_json::Map::new();
    data.insert(
        "market_slug".to_string(),
        Value::String(ledger.market_slug.clone()),
    );
    data.insert("order_id".to_string(), Value::String(order_id.to_string()));
    data.insert(
        "order_id_str".to_string(),
        Value::String(order_id.to_string()),
    );
    data.insert(
        "client_order_id".to_string(),
        Value::String(client_order_id.to_string()),
    );
    data.insert("token_id".to_string(), Value::String(token_id.to_string()));
    if let Some(leg) = leg_from_token_id(ledger, token_id) {
        data.insert(
            "leg".to_string(),
            Value::String(
                match leg {
                    TradeLeg::Up => "UP",
                    TradeLeg::Down => "DOWN",
                }
                .to_string(),
            ),
        );
    }
    data.insert("side".to_string(), Value::String(side.to_string()));
    data.insert("price".to_string(), Value::from(price));
    data.insert("size".to_string(), Value::from(size));
    data.insert("qty".to_string(), Value::from(size));
    data.insert("status".to_string(), Value::String(status.to_string()));
    data.insert("raw_type".to_string(), Value::String(raw_type.to_string()));
    data.insert(
        "decision_seq".to_string(),
        Value::Number(serde_json::Number::from(ledger.decision_seq)),
    );
    data.insert(
        "round_idx".to_string(),
        Value::Number(serde_json::Number::from(ledger.round_idx)),
    );
    data.insert(
        "tick_size".to_string(),
        Value::from(ledger.tick_size_current),
    );
    data.insert(
        "tick_size_current".to_string(),
        Value::from(ledger.tick_size_current),
    );
    data.insert(
        "execution_mode".to_string(),
        Value::String(execution.mode.as_str().to_string()),
    );
    data.insert(
        "adapter".to_string(),
        Value::String(execution.adapter_name.clone()),
    );
    if let Some(extra) = extra {
        for (k, v) in extra {
            data.insert(k.clone(), v.clone());
        }
    }
    data.insert("ts_ms".to_string(), Value::from(now_ts_ms_i64()));
    log_jsonl(log_ctx, "user_ws_order", data);
}

fn leg_from_token_id(ledger: &DryRunLedger, token_id: &str) -> Option<TradeLeg> {
    if token_id == ledger.up_id {
        Some(TradeLeg::Up)
    } else if token_id == ledger.down_id {
        Some(TradeLeg::Down)
    } else {
        None
    }
}

fn deterministic_unit_from_order(order_id: &str, client_order_id: &str, placed_ts_ms: u64) -> f64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in order_id
        .as_bytes()
        .iter()
        .chain(client_order_id.as_bytes().iter())
        .chain(placed_ts_ms.to_le_bytes().iter())
    {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    let mantissa = hash & ((1u64 << 53) - 1);
    (mantissa as f64) / ((1u64 << 53) as f64)
}

fn best_price_and_rate_for_order(
    ledger: &DryRunLedger,
    leg: TradeLeg,
    side: TradeSide,
) -> (Option<f64>, f64, Option<f64>) {
    match (leg, side) {
        (TradeLeg::Up, TradeSide::Buy) => (
            ledger.best_bid_up,
            ledger.bid_consumption_rate_up,
            ledger.best_bid_size_up,
        ),
        (TradeLeg::Up, TradeSide::Sell) => (
            ledger.best_ask_up,
            ledger.ask_consumption_rate_up,
            ledger.best_ask_size_up,
        ),
        (TradeLeg::Down, TradeSide::Buy) => (
            ledger.best_bid_down,
            ledger.bid_consumption_rate_down,
            ledger.best_bid_size_down,
        ),
        (TradeLeg::Down, TradeSide::Sell) => (
            ledger.best_ask_down,
            ledger.ask_consumption_rate_down,
            ledger.best_ask_size_down,
        ),
    }
}

fn best_bid_ask_for_leg(ledger: &DryRunLedger, leg: TradeLeg) -> (Option<f64>, Option<f64>) {
    match leg {
        TradeLeg::Up => (ledger.best_bid_up, ledger.best_ask_up),
        TradeLeg::Down => (ledger.best_bid_down, ledger.best_ask_down),
    }
}

fn desired_postonly_price_for_order(
    ledger: &DryRunLedger,
    params: &DryRunParams,
    leg: TradeLeg,
    side: TradeSide,
    qty: f64,
    tick: f64,
) -> Option<f64> {
    let (best_bid, best_ask) = best_bid_ask_for_leg(ledger, leg);
    match (side, best_bid, best_ask) {
        (TradeSide::Buy, _, _) => {
            let current_net = ledger.qty_up - ledger.qty_down;
            let projected_net_after = match leg {
                TradeLeg::Up => current_net + qty.max(0.0),
                TradeLeg::Down => current_net - qty.max(0.0),
            };
            let projected_net_increase = projected_net_after.abs() > current_net.abs() + 1e-9;
            let opposite_best_ask = match leg {
                TradeLeg::Up => ledger.best_ask_down,
                TradeLeg::Down => ledger.best_ask_up,
            };
            strategy::simulate::compute_maker_buy_quote(
                best_bid,
                best_ask,
                opposite_best_ask,
                tick,
                params.no_risk_hard_pair_cap,
                params.entry_dynamic_cap_enabled,
                params.entry_dynamic_cap_headroom_bps,
                params.entry_dynamic_cap_min_price,
                params.entry_dynamic_cap_apply_to_net_increase_only,
                params.entry_target_margin_min_ticks,
                params.entry_target_margin_min_bps,
                params.entry_max_passive_ticks_for_net_increase,
                projected_net_increase,
            )
            .final_price
        }
        (TradeSide::Sell, Some(bid), Some(ask)) => postonly_sell_price(bid, ask, tick),
        _ => None,
    }
}

fn estimate_queue_ahead_for_order(
    top_size_opt: Option<f64>,
    fallback_qty: f64,
    depth_levels: usize,
    queue_ahead_mult: f64,
    side: TradeSide,
    order_price: f64,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    tick: f64,
) -> f64 {
    let improves_top = match (side, best_bid, best_ask) {
        (TradeSide::Buy, Some(bid), Some(ask)) => {
            order_price > bid + tick * 0.5 && order_price < ask - tick * 0.5
        }
        (TradeSide::Sell, Some(bid), Some(ask)) => {
            order_price + tick * 0.5 < ask && order_price > bid + tick * 0.5
        }
        _ => false,
    };
    if improves_top {
        return 0.0;
    }
    let depth_scale = depth_levels.max(1) as f64;
    let visible_ahead = top_size_opt.unwrap_or(0.0) * depth_scale;
    let fallback = fallback_qty.max(0.0);
    (visible_ahead.max(fallback) * queue_ahead_mult.max(0.0)).max(0.0)
}

fn adaptive_order_horizon_secs(
    base_timeout_secs: u64,
    max_age_secs: u64,
    queue_ahead: f64,
    qty: f64,
    consume_rate: f64,
    timeout_target_fill_prob: f64,
) -> u64 {
    let base = base_timeout_secs.max(1);
    let max_age = max_age_secs.max(base);
    let rate = consume_rate.max(1e-6);
    let target_fill_prob = timeout_target_fill_prob.clamp(0.05, 0.95);
    let required_ratio = -(1.0 - target_fill_prob).ln();
    let required_secs = ((queue_ahead.max(0.0) + qty.max(1e-9)) * required_ratio / rate).ceil();
    let required_secs = if required_secs.is_finite() && required_secs > 0.0 {
        required_secs as u64
    } else {
        base
    };
    base.max(required_secs).min(max_age)
}

fn should_extend_timeout(
    fill_progress_ratio: f64,
    timeout_extend_count: u64,
    progress_extend_min: f64,
    progress_extend_secs: u64,
    max_extends: u64,
) -> bool {
    progress_extend_secs > 0
        && timeout_extend_count < max_extends
        && fill_progress_ratio + 1e-9 >= progress_extend_min
}

fn estimate_maker_fill_prob_for_order(
    params: &DryRunParams,
    side: TradeSide,
    order_price: f64,
    qty: f64,
    queue_ahead: f64,
    observed_rate: f64,
    horizon_secs: f64,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    tick: f64,
) -> f64 {
    let improves_top = match (side, best_bid, best_ask) {
        (TradeSide::Buy, Some(bid), Some(ask)) => {
            order_price > bid + tick * 0.5 && order_price < ask - tick * 0.5
        }
        (TradeSide::Sell, Some(bid), Some(ask)) => {
            order_price + tick * 0.5 < ask && order_price > bid + tick * 0.5
        }
        _ => false,
    };
    let passive_ticks = match (side, best_bid, best_ask) {
        (TradeSide::Buy, Some(bid), Some(_)) if order_price < bid - tick * 0.5 => {
            ((bid - order_price) / tick.max(1e-9)).max(0.0)
        }
        (TradeSide::Sell, Some(_), Some(ask)) if order_price > ask + tick * 0.5 => {
            ((order_price - ask) / tick.max(1e-9)).max(0.0)
        }
        _ => 0.0,
    };
    let queue_ahead = if improves_top {
        0.0
    } else {
        queue_ahead.max(0.0)
    };
    let effective_queue_ahead = queue_ahead
        * (1.0 + passive_ticks * params.maker_fill_passive_queue_penalty_per_tick.max(0.0));
    let consume_rate = observed_rate.max(params.maker_flow_floor_per_sec);
    let expected_consumed = consume_rate * horizon_secs.max(1e-6);
    let queue_and_order = effective_queue_ahead + qty.max(1e-9);
    let flow_ratio = (expected_consumed / queue_and_order.max(1e-9)).max(0.0);
    let mut fill_prob = (1.0 - (-flow_ratio).exp()).clamp(0.0, 1.0);
    let decay_k = params.maker_fill_passive_decay_k.max(0.0);
    if decay_k > 0.0 && passive_ticks > 0.0 {
        fill_prob *= (-decay_k * passive_ticks).exp();
        fill_prob = fill_prob.clamp(0.0, 1.0);
    }
    fill_prob
}

fn build_resting_fill_sim_result(
    ledger: &DryRunLedger,
    params: &DryRunParams,
    leg: TradeLeg,
    side: TradeSide,
    qty: f64,
    price: f64,
    queue_ahead: f64,
    consumed_qty: f64,
    consumption_rate: f64,
    horizon_secs: u64,
) -> Option<SimResult> {
    if qty <= 0.0 || price <= 0.0 {
        return None;
    }

    let mut new_qty_up = ledger.qty_up;
    let mut new_cost_up = ledger.cost_up;
    let mut new_qty_down = ledger.qty_down;
    let mut new_cost_down = ledger.cost_down;

    match (leg, side) {
        (TradeLeg::Up, TradeSide::Buy) => {
            new_qty_up += qty;
            new_cost_up += qty * price;
        }
        (TradeLeg::Down, TradeSide::Buy) => {
            new_qty_down += qty;
            new_cost_down += qty * price;
        }
        (TradeLeg::Up, TradeSide::Sell) => {
            if ledger.qty_up < qty {
                return None;
            }
            let avg = ledger.avg_up()?;
            new_qty_up -= qty;
            new_cost_up = (new_cost_up - avg * qty).max(0.0);
        }
        (TradeLeg::Down, TradeSide::Sell) => {
            if ledger.qty_down < qty {
                return None;
            }
            let avg = ledger.avg_down()?;
            new_qty_down -= qty;
            new_cost_down = (new_cost_down - avg * qty).max(0.0);
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
    let new_unhedged_value_up =
        price_for_unhedged(ledger.best_bid_up, ledger.best_ask_up).unwrap_or(0.0) * new_unhedged_up;
    let new_unhedged_value_down = price_for_unhedged(ledger.best_bid_down, ledger.best_ask_down)
        .unwrap_or(0.0)
        * new_unhedged_down;
    let (
        hedge_recoverable_now,
        required_opp_avg_price_cap,
        current_opp_best_ask,
        required_hedge_qty,
        hedge_margin_to_opp_ask,
        hedge_margin_required,
        hedge_margin_ok,
    ) = evaluate_hedge_recoverability_ws(
        ledger,
        params,
        new_qty_up,
        new_cost_up,
        new_qty_down,
        new_cost_down,
    );
    let current_unhedged_value_up = price_for_unhedged(ledger.best_bid_up, ledger.best_ask_up)
        .unwrap_or(0.0)
        * ledger.unhedged_up();
    let current_unhedged_value_down =
        price_for_unhedged(ledger.best_bid_down, ledger.best_ask_down).unwrap_or(0.0)
            * ledger.unhedged_down();
    let improves_hedge = new_hedgeable > ledger.hedgeable()
        || (new_unhedged_value_up + new_unhedged_value_down)
            < (current_unhedged_value_up + current_unhedged_value_down);
    let cost_before = ledger.cost_up + ledger.cost_down;
    let cost_after = new_cost_up + new_cost_down;
    let spent_delta_usdc = (cost_after - cost_before).max(0.0);

    Some(SimResult {
        ok: true,
        fill_qty: qty,
        fill_price: Some(price),
        maker_fill_prob: Some(1.0),
        maker_queue_ahead: Some(queue_ahead),
        maker_expected_consumed: Some(consumed_qty),
        maker_consumption_rate: Some(consumption_rate),
        maker_horizon_secs: Some(horizon_secs as f64),
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
        hedge_recoverable_now,
        required_opp_avg_price_cap,
        current_opp_best_ask,
        required_hedge_qty,
        hedge_margin_to_opp_ask,
        hedge_margin_required,
        hedge_margin_ok,
        entry_quote_base_postonly_price: None,
        entry_quote_dynamic_cap_price: None,
        entry_quote_final_price: None,
        entry_quote_cap_active: None,
        entry_quote_cap_bind: None,
        passive_gap_abs: None,
        passive_gap_ticks: None,
        improves_hedge,
    })
}

fn paper_progress_key(token_id: &str, side: TradeSide) -> String {
    let side_str = match side {
        TradeSide::Buy => "buy",
        TradeSide::Sell => "sell",
    };
    format!("{token_id}:{side_str}")
}

async fn process_paper_resting_fills(
    ledger: &mut DryRunLedger,
    params: &DryRunParams,
    now_ts: u64,
    now_ms: u64,
    execution: &mut ExecutionRuntime,
    resting_orders: &mut HashMap<String, PaperRestingOrder>,
    requote_progress: &mut HashMap<String, f64>,
    requote_fill_draw: &mut HashMap<String, f64>,
    log_ctx: &RolloverLogContext,
) {
    if execution.mode != ExecutionMode::Paper || !params.paper_resting_fill_enabled {
        return;
    }

    let open_orders = match execution
        .adapter
        .fetch_open_orders(&ledger.market_slug)
        .await
    {
        Ok(orders) => orders,
        Err(err) => {
            tracing::warn!(error = %err, "failed to fetch paper open orders for fill simulation");
            return;
        }
    };
    let mut open_order_ids = HashSet::new();
    for order in &open_orders {
        open_order_ids.insert(order.order_id.clone());
    }
    resting_orders.retain(|order_id, _| open_order_ids.contains(order_id));

    let mut to_remove = Vec::new();
    for order in open_orders {
        let side = match order.side {
            OrderSide::Buy => TradeSide::Buy,
            OrderSide::Sell => TradeSide::Sell,
        };
        let Some(leg) = leg_from_token_id(ledger, &order.token_id) else {
            continue;
        };

        let (_, _, top_size_opt) = best_price_and_rate_for_order(ledger, leg, side);
        let (best_bid, best_ask) = best_bid_ask_for_leg(ledger, leg);
        let fallback_queue = estimate_queue_ahead_for_order(
            top_size_opt,
            order.qty,
            params.paper_queue_depth_levels,
            params.maker_queue_ahead_mult,
            side,
            order.price,
            best_bid,
            best_ask,
            ledger.tick_size_current.max(1e-6),
        );
        let state = resting_orders
            .entry(order.order_id.clone())
            .or_insert_with(|| PaperRestingOrder {
                order_id: order.order_id.clone(),
                client_order_id: order.client_order_id.clone(),
                token_id: order.token_id.clone(),
                leg,
                side,
                price: order.price,
                remaining_qty: order.qty.max(0.0),
                queue_ahead: fallback_queue.max(0.0),
                consumed_qty: 0.0,
                fill_draw: deterministic_unit_from_order(
                    &order.order_id,
                    &order.client_order_id,
                    now_ms,
                ),
                placed_ts_ms: now_ms,
                last_requote_ts_ms: now_ms,
                active_after_ts_ms: now_ms.saturating_add(params.paper_order_ack_delay_ms),
                last_eval_ts_ms: now_ms,
                horizon_secs: params.paper_order_timeout_secs,
                timeout_extend_count: 0,
                first_unrecoverable_detected_ts_ms: None,
                first_margin_fail_detected_ts_ms: None,
            });

        if state.remaining_qty <= 1e-9 {
            to_remove.push(state.order_id.clone());
            continue;
        }

        let expiry_ms = state
            .placed_ts_ms
            .saturating_add(state.horizon_secs.saturating_mul(1000));
        if now_ms >= expiry_ms {
            let side_str = match state.side {
                TradeSide::Buy => "buy",
                TradeSide::Sell => "sell",
            };
            let (best_price_opt, _, _) = best_price_and_rate_for_order(ledger, leg, side);
            let tick = ledger.tick_size_current.max(1e-6);
            let passive_ticks = match (side, best_price_opt) {
                (TradeSide::Buy, Some(best_price)) => {
                    ((best_price - state.price) / tick.max(1e-9)).max(0.0)
                }
                (TradeSide::Sell, Some(best_price)) => {
                    ((state.price - best_price) / tick.max(1e-9)).max(0.0)
                }
                _ => 0.0,
            };
            let required_consumption = state.queue_ahead.max(0.0) + state.remaining_qty.max(0.0);
            let fill_progress_ratio = if required_consumption > 1e-9 {
                (state.consumed_qty / required_consumption).clamp(0.0, 1.0)
            } else {
                0.0
            };
            let can_extend_timeout = should_extend_timeout(
                fill_progress_ratio,
                state.timeout_extend_count,
                params.paper_timeout_progress_extend_min,
                params.paper_timeout_progress_extend_secs,
                params.paper_timeout_max_extends,
            );
            if can_extend_timeout {
                let old_horizon_secs = state.horizon_secs;
                state.horizon_secs = state
                    .horizon_secs
                    .saturating_add(params.paper_timeout_progress_extend_secs);
                state.timeout_extend_count = state.timeout_extend_count.saturating_add(1);
                ledger.timeout_extend_count_window =
                    ledger.timeout_extend_count_window.saturating_add(1);
                let mut extra = serde_json::Map::new();
                extra.insert(
                    "queue_ahead_at_open".to_string(),
                    Value::from(state.queue_ahead),
                );
                extra.insert(
                    "required_consumption".to_string(),
                    Value::from(required_consumption),
                );
                extra.insert(
                    "consumed_qty_at_extend".to_string(),
                    Value::from(state.consumed_qty),
                );
                extra.insert(
                    "fill_progress_ratio".to_string(),
                    Value::from(fill_progress_ratio),
                );
                extra.insert(
                    "passive_ticks_at_extend".to_string(),
                    Value::from(passive_ticks),
                );
                extra.insert(
                    "old_horizon_secs".to_string(),
                    Value::from(old_horizon_secs),
                );
                extra.insert(
                    "new_horizon_secs".to_string(),
                    Value::from(state.horizon_secs),
                );
                extra.insert(
                    "timeout_extend_count".to_string(),
                    Value::from(state.timeout_extend_count),
                );
                log_synthetic_user_ws_order_with_extra(
                    ledger,
                    execution,
                    &state.order_id,
                    &state.client_order_id,
                    &state.token_id,
                    side_str,
                    state.price,
                    state.remaining_qty,
                    "open",
                    "sim_order_timeout_extend",
                    Some(&extra),
                    log_ctx,
                );
                continue;
            }
            if let Err(err) = execution.adapter.cancel(&state.order_id).await {
                tracing::warn!(order_id = %state.order_id, error = %err, "paper cancel on timeout failed");
            }
            let mut extra = serde_json::Map::new();
            extra.insert(
                "queue_ahead_at_open".to_string(),
                Value::from(state.queue_ahead),
            );
            extra.insert(
                "required_consumption".to_string(),
                Value::from(required_consumption),
            );
            extra.insert(
                "consumed_qty_at_cancel".to_string(),
                Value::from(state.consumed_qty),
            );
            extra.insert(
                "fill_progress_ratio".to_string(),
                Value::from(fill_progress_ratio),
            );
            extra.insert(
                "passive_ticks_at_cancel".to_string(),
                Value::from(passive_ticks),
            );
            extra.insert(
                "order_age_secs".to_string(),
                Value::from(now_ms.saturating_sub(state.placed_ts_ms) as f64 / 1000.0),
            );
            extra.insert("horizon_secs".to_string(), Value::from(state.horizon_secs));
            log_synthetic_user_ws_order_with_extra(
                ledger,
                execution,
                &state.order_id,
                &state.client_order_id,
                &state.token_id,
                side_str,
                state.price,
                state.remaining_qty,
                "canceled",
                "sim_order_cancel_timeout",
                Some(&extra),
                log_ctx,
            );
            to_remove.push(state.order_id.clone());
            continue;
        }

        if now_ms < state.active_after_ts_ms {
            continue;
        }
        if now_ms <= state.last_eval_ts_ms {
            continue;
        }
        let dt_secs = (now_ms - state.last_eval_ts_ms) as f64 / 1000.0;
        state.last_eval_ts_ms = now_ms;
        if dt_secs <= 0.0 {
            continue;
        }

        let (best_price_opt, observed_rate, _) = best_price_and_rate_for_order(ledger, leg, side);
        let Some(_best_price) = best_price_opt else {
            continue;
        };

        let current_net = ledger.qty_up - ledger.qty_down;
        let would_reduce_abs_net = match state.leg {
            TradeLeg::Up => current_net < -1e-9,
            TradeLeg::Down => current_net > 1e-9,
        };
        let time_left_secs = if ledger.end_ts > now_ts {
            ledger.end_ts - now_ts
        } else {
            0
        };
        let in_tail_close = time_left_secs <= params.tail_close_secs;
        let in_balancing_phase = ledger.round_state == RoundPhase::Leg2Balancing;
        let queue_base = if would_reduce_abs_net && (in_tail_close || in_balancing_phase) {
            // Rebalancing orders should not be perpetually blocked by very large top-of-book
            // queue estimates in paper mode.
            state
                .queue_ahead
                .min((state.remaining_qty * PAPER_REBALANCE_QUEUE_AHEAD_MULT).max(0.0))
        } else {
            state.queue_ahead
        };
        let effective_queue_ahead = queue_base;

        let consume_rate = observed_rate.max(params.maker_flow_floor_per_sec);
        state.consumed_qty += consume_rate * dt_secs;
        let required_consumption = effective_queue_ahead + state.remaining_qty;
        let queue_and_order = required_consumption.max(1e-9);
        let flow_ratio = (state.consumed_qty / queue_and_order).max(0.0);
        let fill_prob_elapsed = (1.0 - (-flow_ratio).exp()).clamp(0.0, 1.0);
        let probabilistic_fill_ready = fill_prob_elapsed + 1e-9 >= state.fill_draw;
        if state.consumed_qty + 1e-9 < required_consumption && !probabilistic_fill_ready {
            continue;
        }

        let fill_qty = state.remaining_qty;
        let Some(sim) = build_resting_fill_sim_result(
            ledger,
            params,
            state.leg,
            state.side,
            fill_qty,
            state.price,
            state.queue_ahead,
            state.consumed_qty,
            consume_rate,
            state.horizon_secs,
        ) else {
            continue;
        };
        let action = CandidateAction {
            name: "RESTING_MAKER_FILL",
            leg: state.leg,
            side: state.side,
            kind: TradeKind::Maker,
            qty: fill_qty,
        };
        let round_plan = build_round_plan(ledger, params, now_ts);
        let gate = evaluate_action_gate_with_context(
            ledger,
            &action,
            &sim,
            params,
            now_ts,
            &round_plan,
            strategy::gate::GateContext::RestingFill,
        );
        if !gate.allow {
            let soft_recheck = is_soft_recheck_deny(gate.deny_reason);
            let cancel_raw_type = if soft_recheck {
                ledger.resting_soft_recheck_cancel_count_window = ledger
                    .resting_soft_recheck_cancel_count_window
                    .saturating_add(1);
                "sim_order_cancel_recheck_soft"
            } else if matches!(
                gate.deny_reason,
                Some(DenyReason::HedgeNotRecoverable | DenyReason::HedgeMarginInsufficient)
            ) {
                ledger.unrecoverable_block_count_window =
                    ledger.unrecoverable_block_count_window.saturating_add(1);
                ledger.cancel_unrecoverable_count_window =
                    ledger.cancel_unrecoverable_count_window.saturating_add(1);
                "sim_order_cancel_unrecoverable"
            } else {
                ledger.risk_guard_cancel_count_window =
                    ledger.risk_guard_cancel_count_window.saturating_add(1);
                ledger.resting_hard_risk_cancel_count_window = ledger
                    .resting_hard_risk_cancel_count_window
                    .saturating_add(1);
                if matches!(gate.deny_reason, Some(DenyReason::MarginTarget)) {
                    ledger.risk_guard_cancel_pair_cap_count_window = ledger
                        .risk_guard_cancel_pair_cap_count_window
                        .saturating_add(1);
                } else {
                    ledger.risk_guard_cancel_other_count_window = ledger
                        .risk_guard_cancel_other_count_window
                        .saturating_add(1);
                }
                if in_tail_close && matches!(gate.deny_reason, Some(DenyReason::MarginTarget)) {
                    ledger.tail_pair_cap_block_count_window =
                        ledger.tail_pair_cap_block_count_window.saturating_add(1);
                }
                "sim_order_cancel_risk_guard"
            };
            if let Err(err) = execution.adapter.cancel(&state.order_id).await {
                tracing::warn!(
                    order_id = %state.order_id,
                    deny_reason = ?gate.deny_reason,
                    error = %err,
                    "paper cancel on gate deny failed"
                );
            } else {
                let side_str = match state.side {
                    TradeSide::Buy => "buy",
                    TradeSide::Sell => "sell",
                };
                let mut extra = serde_json::Map::new();
                let reason_detail = match gate.deny_reason {
                    Some(DenyReason::MarginTarget) => "pair_cap",
                    Some(DenyReason::HedgeNotRecoverable) => "hedge_not_recoverable",
                    Some(DenyReason::HedgeMarginInsufficient) => "hedge_margin_insufficient",
                    Some(DenyReason::TailFreeze) | Some(DenyReason::TailClose) => "tail_guard",
                    Some(DenyReason::TotalBudgetCap) | Some(DenyReason::RoundBudgetCap) => {
                        "budget_guard"
                    }
                    _ => "other_guard",
                };
                extra.insert(
                    "cancel_reason_detail".to_string(),
                    Value::String(reason_detail.to_string()),
                );
                extra.insert(
                    "projected_pair_cost".to_string(),
                    sim.new_pair_cost.map(Value::from).unwrap_or(Value::Null),
                );
                extra.insert(
                    "pair_guard_limit".to_string(),
                    Value::from(params.no_risk_hard_pair_cap),
                );
                extra.insert(
                    "required_opp_avg_price_cap".to_string(),
                    sim.required_opp_avg_price_cap
                        .map(Value::from)
                        .unwrap_or(Value::Null),
                );
                extra.insert(
                    "current_opp_best_ask".to_string(),
                    sim.current_opp_best_ask
                        .map(Value::from)
                        .unwrap_or(Value::Null),
                );
                extra.insert(
                    "required_hedge_qty".to_string(),
                    sim.required_hedge_qty
                        .map(Value::from)
                        .unwrap_or(Value::Null),
                );
                extra.insert(
                    "hedge_margin_to_opp_ask".to_string(),
                    sim.hedge_margin_to_opp_ask
                        .map(Value::from)
                        .unwrap_or(Value::Null),
                );
                extra.insert(
                    "hedge_margin_required".to_string(),
                    sim.hedge_margin_required
                        .map(Value::from)
                        .unwrap_or(Value::Null),
                );
                extra.insert(
                    "hedge_margin_ok".to_string(),
                    sim.hedge_margin_ok.map(Value::Bool).unwrap_or(Value::Null),
                );
                log_synthetic_user_ws_order_with_extra(
                    ledger,
                    execution,
                    &state.order_id,
                    &state.client_order_id,
                    &state.token_id,
                    side_str,
                    state.price,
                    state.remaining_qty,
                    "canceled",
                    cancel_raw_type,
                    Some(&extra),
                    log_ctx,
                );
                to_remove.push(state.order_id.clone());
            }
            continue;
        }
        let before = ledger.clone();
        let applied = CandidateEval {
            action,
            sim: sim.clone(),
            gate,
            score: None,
        };
        apply_simulated_trade(ledger, &applied.action, &applied.sim, params, now_ts);
        ledger.fill_count_window = ledger.fill_count_window.saturating_add(1);
        if let Some(sim_pair_cost) = applied.sim.new_pair_cost {
            ledger.max_executed_sim_pair_cost_window = Some(
                ledger
                    .max_executed_sim_pair_cost_window
                    .map(|v| v.max(sim_pair_cost))
                    .unwrap_or(sim_pair_cost),
            );
        }
        let executed = (before.qty_up - ledger.qty_up).abs() > 1e-9
            || (before.qty_down - ledger.qty_down).abs() > 1e-9
            || (before.cost_up - ledger.cost_up).abs() > 1e-9
            || (before.cost_down - ledger.cost_down).abs() > 1e-9
            || (before.spent_total_usdc - ledger.spent_total_usdc).abs() > 1e-9
            || (before.spent_round_usdc - ledger.spent_round_usdc).abs() > 1e-9
            || before.round_idx != ledger.round_idx
            || before.round_state != ledger.round_state;
        log_dryrun_apply(&before, ledger, &applied, executed, params, log_ctx);
        log_user_ws_fill(
            ledger,
            &applied,
            execution,
            "sim_fill_paper_resting",
            log_ctx,
        );
        let side_str = match state.side {
            TradeSide::Buy => "buy",
            TradeSide::Sell => "sell",
        };
        log_synthetic_user_ws_order(
            ledger,
            execution,
            &state.order_id,
            &state.client_order_id,
            &state.token_id,
            side_str,
            state.price,
            fill_qty,
            "filled",
            "sim_order_filled_resting",
            log_ctx,
        );
        if let Err(err) = execution.adapter.cancel(&state.order_id).await {
            tracing::warn!(order_id = %state.order_id, error = %err, "paper cancel after fill failed");
        }
        to_remove.push(state.order_id.clone());
    }

    for order_id in to_remove {
        if let Some(state) = resting_orders.remove(&order_id) {
            let key = paper_progress_key(&state.token_id, state.side);
            requote_progress.remove(&key);
            requote_fill_draw.remove(&key);
        }
    }
}

async fn execute_candidate_action(
    ledger: &mut DryRunLedger,
    applied: &CandidateEval,
    params: &DryRunParams,
    now_ts: u64,
    now_ms: u64,
    execution: &mut ExecutionRuntime,
    log_ctx: &RolloverLogContext,
    resting_orders: &mut HashMap<String, PaperRestingOrder>,
    requote_progress: &mut HashMap<String, f64>,
    requote_fill_draw: &mut HashMap<String, f64>,
) {
    log_live_intent(ledger, applied, execution, log_ctx);
    if execution.mode == ExecutionMode::Paper && params.paper_resting_fill_enabled {
        match execution
            .adapter
            .fetch_open_orders(&ledger.market_slug)
            .await
        {
            Ok(open_orders) if !open_orders.is_empty() => {
                let mut repriced_any = false;
                let mut canceled_for_rebalance = false;
                let mut canceled_ids: HashSet<String> = HashSet::new();
                let tick = ledger.tick_size_current.max(1e-6);
                let time_left_secs = if ledger.end_ts > now_ts {
                    ledger.end_ts - now_ts
                } else {
                    0
                };
                let in_tail_close = time_left_secs <= params.tail_close_secs;
                let new_round_cutoff_secs = params
                    .tail_close_secs
                    .saturating_add(params.round_pair_wait_secs)
                    .saturating_add(params.no_risk_late_new_round_buffer_secs);
                let close_window_active = params.no_risk_require_completable_round
                    && time_left_secs <= new_round_cutoff_secs;
                let round_plan_now = build_round_plan(ledger, params, now_ts);
                let current_abs_net = (ledger.qty_up - ledger.qty_down).abs();
                let pair_guard_limit = (params.no_risk_hard_pair_cap
                    - params.open_order_risk_buffer_bps / 10_000.0)
                    .max(0.0);
                let mut repricing_orders: Vec<(
                    String,
                    String,
                    String,
                    String,
                    f64,
                    f64,
                    TradeSide,
                    Option<f64>,
                    Option<f64>,
                    Option<f64>,
                    String,
                )> = Vec::new();
                for order in &open_orders {
                    let Some(open_leg) = leg_from_token_id(ledger, &order.token_id) else {
                        continue;
                    };
                    let side = match order.side {
                        OrderSide::Buy => TradeSide::Buy,
                        OrderSide::Sell => TradeSide::Sell,
                    };
                    let state_snapshot = resting_orders.get(&order.order_id).map(|state| {
                        (
                            state.placed_ts_ms,
                            state.active_after_ts_ms,
                            state.last_requote_ts_ms,
                            state.consumed_qty,
                            state.queue_ahead,
                            state.horizon_secs,
                        )
                    });
                    let stale_age_cancel = params.open_order_max_age_secs > 0
                        && state_snapshot
                            .map(|state| {
                                let age_exceeded = now_ms
                                    >= state.0.saturating_add(
                                        params.open_order_max_age_secs.saturating_mul(1000),
                                    );
                                let no_progress = state.3 <= 1e-9;
                                age_exceeded && no_progress
                            })
                            .unwrap_or(false);
                    let projected_buy_sim = if side == TradeSide::Buy {
                        build_resting_fill_sim_result(
                            ledger,
                            params,
                            open_leg,
                            side,
                            order.qty.max(0.0),
                            order.price,
                            0.0,
                            0.0,
                            0.0,
                            params.paper_order_timeout_secs.max(1),
                        )
                    } else {
                        None
                    };
                    let projected_pair_cost_opt =
                        projected_buy_sim.as_ref().and_then(|sim| sim.new_pair_cost);
                    let risk_guard_cancel =
                        if params.open_order_risk_guard && side == TradeSide::Buy {
                            let pair_check_enabled = !params.open_order_risk_guard_require_paired
                                || projected_pair_cost_opt.is_some();
                            pair_check_enabled
                                && projected_pair_cost_opt
                                    .map(|pair| pair > pair_guard_limit + 1e-9)
                                    .unwrap_or(false)
                        } else {
                            false
                        };
                    let close_window_cancel = if close_window_active && side == TradeSide::Buy {
                        projected_buy_sim
                            .as_ref()
                            .map(|sim| {
                                (sim.new_qty_up - sim.new_qty_down).abs() > current_abs_net + 1e-9
                            })
                            .unwrap_or(false)
                    } else {
                        false
                    };
                    let unrecoverable_now = params.no_risk_hedge_recoverability_enforce
                        && side == TradeSide::Buy
                        && projected_buy_sim
                            .as_ref()
                            .and_then(|sim| sim.hedge_recoverable_now)
                            .map(|recoverable| !recoverable)
                            .unwrap_or(false);
                    let margin_rule_applies = projected_buy_sim
                        .as_ref()
                        .map(|sim| {
                            if params.hedge_recoverability_margin_apply_to_net_increase_only {
                                (sim.new_qty_up - sim.new_qty_down).abs() > current_abs_net + 1e-9
                            } else {
                                true
                            }
                        })
                        .unwrap_or(false);
                    let hedge_margin_now = params.hedge_recoverability_margin_enforce
                        && side == TradeSide::Buy
                        && margin_rule_applies
                        && projected_buy_sim
                            .as_ref()
                            .map(|sim| sim.hedge_margin_ok != Some(true))
                            .unwrap_or(false);
                    let mut unrecoverable_cancel = false;
                    let mut hedge_margin_cancel = false;
                    if side == TradeSide::Buy {
                        if let Some(state_mut) = resting_orders.get_mut(&order.order_id) {
                            if unrecoverable_now {
                                let first_detected = state_mut
                                    .first_unrecoverable_detected_ts_ms
                                    .get_or_insert(now_ms);
                                unrecoverable_cancel = now_ms.saturating_sub(*first_detected)
                                    >= params.open_order_unrecoverable_grace_ms;
                            } else {
                                state_mut.first_unrecoverable_detected_ts_ms = None;
                            }
                            if hedge_margin_now {
                                let first_detected = state_mut
                                    .first_margin_fail_detected_ts_ms
                                    .get_or_insert(now_ms);
                                hedge_margin_cancel = now_ms.saturating_sub(*first_detected)
                                    >= params.open_order_unrecoverable_grace_ms;
                            } else {
                                state_mut.first_margin_fail_detected_ts_ms = None;
                            }
                        } else {
                            let immediate = params.open_order_unrecoverable_grace_ms == 0;
                            unrecoverable_cancel = unrecoverable_now && immediate;
                            hedge_margin_cancel = hedge_margin_now && immediate;
                        }
                    }
                    let entry_worst_pair_cancel = params.no_risk_require_completable_round
                        && side == TradeSide::Buy
                        && current_abs_net <= 1e-9
                        && round_plan_now.phase == RoundPhase::Idle
                        && round_plan_now.planned_leg1 == Some(open_leg)
                        && !round_plan_now.entry_worst_pair_ok;
                    if stale_age_cancel
                        || risk_guard_cancel
                        || close_window_cancel
                        || unrecoverable_cancel
                        || hedge_margin_cancel
                        || entry_worst_pair_cancel
                    {
                        match execution.adapter.cancel(&order.order_id).await {
                            Ok(_) => {
                                canceled_ids.insert(order.order_id.clone());
                                if let Some(prev_state) = resting_orders.remove(&order.order_id) {
                                    let key =
                                        paper_progress_key(&prev_state.token_id, prev_state.side);
                                    requote_progress.remove(&key);
                                    requote_fill_draw.remove(&key);
                                }
                                let side_str = match side {
                                    TradeSide::Buy => "buy",
                                    TradeSide::Sell => "sell",
                                };
                                if close_window_cancel {
                                    ledger.close_window_cancel_count_window =
                                        ledger.close_window_cancel_count_window.saturating_add(1);
                                    log_synthetic_user_ws_order(
                                        ledger,
                                        execution,
                                        &order.order_id,
                                        &order.client_order_id,
                                        &order.token_id,
                                        side_str,
                                        order.price,
                                        order.qty,
                                        "canceled",
                                        "sim_order_cancel_close_window",
                                        log_ctx,
                                    );
                                } else if unrecoverable_cancel || hedge_margin_cancel {
                                    ledger.unrecoverable_block_count_window =
                                        ledger.unrecoverable_block_count_window.saturating_add(1);
                                    ledger.cancel_unrecoverable_count_window =
                                        ledger.cancel_unrecoverable_count_window.saturating_add(1);
                                    let mut extra = serde_json::Map::new();
                                    extra.insert(
                                        "cancel_reason_detail".to_string(),
                                        Value::String(
                                            if hedge_margin_cancel {
                                                "hedge_margin_insufficient"
                                            } else {
                                                "hedge_not_recoverable"
                                            }
                                            .to_string(),
                                        ),
                                    );
                                    extra.insert(
                                        "projected_pair_cost".to_string(),
                                        projected_pair_cost_opt
                                            .map(Value::from)
                                            .unwrap_or(Value::Null),
                                    );
                                    extra.insert(
                                        "pair_guard_limit".to_string(),
                                        Value::from(pair_guard_limit),
                                    );
                                    extra.insert(
                                        "required_opp_avg_price_cap".to_string(),
                                        projected_buy_sim
                                            .as_ref()
                                            .and_then(|sim| sim.required_opp_avg_price_cap)
                                            .map(Value::from)
                                            .unwrap_or(Value::Null),
                                    );
                                    extra.insert(
                                        "current_opp_best_ask".to_string(),
                                        projected_buy_sim
                                            .as_ref()
                                            .and_then(|sim| sim.current_opp_best_ask)
                                            .map(Value::from)
                                            .unwrap_or(Value::Null),
                                    );
                                    extra.insert(
                                        "required_hedge_qty".to_string(),
                                        projected_buy_sim
                                            .as_ref()
                                            .and_then(|sim| sim.required_hedge_qty)
                                            .map(Value::from)
                                            .unwrap_or(Value::Null),
                                    );
                                    extra.insert(
                                        "hedge_margin_to_opp_ask".to_string(),
                                        projected_buy_sim
                                            .as_ref()
                                            .and_then(|sim| sim.hedge_margin_to_opp_ask)
                                            .map(Value::from)
                                            .unwrap_or(Value::Null),
                                    );
                                    extra.insert(
                                        "hedge_margin_required".to_string(),
                                        projected_buy_sim
                                            .as_ref()
                                            .and_then(|sim| sim.hedge_margin_required)
                                            .map(Value::from)
                                            .unwrap_or(Value::Null),
                                    );
                                    extra.insert(
                                        "hedge_margin_ok".to_string(),
                                        projected_buy_sim
                                            .as_ref()
                                            .and_then(|sim| sim.hedge_margin_ok)
                                            .map(Value::Bool)
                                            .unwrap_or(Value::Null),
                                    );
                                    log_synthetic_user_ws_order_with_extra(
                                        ledger,
                                        execution,
                                        &order.order_id,
                                        &order.client_order_id,
                                        &order.token_id,
                                        side_str,
                                        order.price,
                                        order.qty,
                                        "canceled",
                                        "sim_order_cancel_unrecoverable",
                                        Some(&extra),
                                        log_ctx,
                                    );
                                } else if entry_worst_pair_cancel {
                                    ledger.entry_worst_pair_block_count_window = ledger
                                        .entry_worst_pair_block_count_window
                                        .saturating_add(1);
                                    log_synthetic_user_ws_order(
                                        ledger,
                                        execution,
                                        &order.order_id,
                                        &order.client_order_id,
                                        &order.token_id,
                                        side_str,
                                        order.price,
                                        order.qty,
                                        "canceled",
                                        "sim_order_cancel_entry_worst_pair",
                                        log_ctx,
                                    );
                                } else if risk_guard_cancel {
                                    ledger.risk_guard_cancel_count_window =
                                        ledger.risk_guard_cancel_count_window.saturating_add(1);
                                    ledger.risk_guard_cancel_pair_cap_count_window = ledger
                                        .risk_guard_cancel_pair_cap_count_window
                                        .saturating_add(1);
                                    if in_tail_close {
                                        ledger.tail_pair_cap_block_count_window = ledger
                                            .tail_pair_cap_block_count_window
                                            .saturating_add(1);
                                    }
                                    let mut extra = serde_json::Map::new();
                                    extra.insert(
                                        "cancel_reason_detail".to_string(),
                                        Value::String("pair_cap".to_string()),
                                    );
                                    extra.insert(
                                        "projected_pair_cost".to_string(),
                                        projected_pair_cost_opt
                                            .map(Value::from)
                                            .unwrap_or(Value::Null),
                                    );
                                    extra.insert(
                                        "pair_guard_limit".to_string(),
                                        Value::from(pair_guard_limit),
                                    );
                                    log_synthetic_user_ws_order_with_extra(
                                        ledger,
                                        execution,
                                        &order.order_id,
                                        &order.client_order_id,
                                        &order.token_id,
                                        side_str,
                                        order.price,
                                        order.qty,
                                        "canceled",
                                        "sim_order_cancel_risk_guard",
                                        Some(&extra),
                                        log_ctx,
                                    );
                                } else {
                                    ledger.stale_cancel_count_window =
                                        ledger.stale_cancel_count_window.saturating_add(1);
                                    log_synthetic_user_ws_order(
                                        ledger,
                                        execution,
                                        &order.order_id,
                                        &order.client_order_id,
                                        &order.token_id,
                                        side_str,
                                        order.price,
                                        order.qty,
                                        "canceled",
                                        "sim_order_cancel_stale_age",
                                        log_ctx,
                                    );
                                }
                            }
                            Err(err) => {
                                tracing::warn!(
                                    order_id = %order.order_id,
                                    error = %err,
                                    "failed to cancel guarded paper order"
                                );
                            }
                        }
                        continue;
                    }
                    let (best_price_opt, observed_rate_current, _) =
                        best_price_and_rate_for_order(ledger, open_leg, side);
                    let at_or_better_now = match (side, best_price_opt) {
                        (TradeSide::Buy, Some(best_price)) => {
                            order.price + tick * 0.5 >= best_price
                        }
                        (TradeSide::Sell, Some(best_price)) => {
                            order.price - tick * 0.5 <= best_price
                        }
                        (_, None) => true,
                    };
                    let stale_off_top = params.paper_requote_stale_ms > 0
                        && state_snapshot
                            .map(|state| {
                                now_ms >= state.1.saturating_add(params.paper_requote_stale_ms)
                            })
                            .unwrap_or(false)
                        && !at_or_better_now;
                    let stale_by_age_hard = params.paper_requote_stale_ms_hard > 0
                        && state_snapshot
                            .map(|state| {
                                now_ms >= state.1.saturating_add(params.paper_requote_stale_ms_hard)
                            })
                            .unwrap_or(false);
                    let requote_interval_ok = params.paper_min_requote_interval_ms == 0
                        || state_snapshot
                            .map(|state| {
                                now_ms
                                    >= state.2.saturating_add(params.paper_min_requote_interval_ms)
                            })
                            .unwrap_or(true);
                    let desired_price_for_order = desired_postonly_price_for_order(
                        ledger, params, open_leg, side, order.qty, tick,
                    );
                    let (best_bid, best_ask) = best_bid_ask_for_leg(ledger, open_leg);
                    let stale_by_price = if let Some(desired_price) = desired_price_for_order {
                        let directional_improve = match side {
                            TradeSide::Buy => desired_price > order.price + tick * 0.5,
                            TradeSide::Sell => desired_price + tick * 0.5 < order.price,
                        };
                        let delta_ticks =
                            ((desired_price - order.price).abs() / tick).floor() as u64;
                        let delta_ok = params.paper_requote_price_delta_ticks == 0
                            || delta_ticks >= params.paper_requote_price_delta_ticks;
                        directional_improve && delta_ok
                    } else {
                        false
                    };
                    let has_progress = state_snapshot.map(|state| state.3 > 1e-9).unwrap_or(false);
                    let order_age_secs = state_snapshot
                        .map(|state| now_ms.saturating_sub(state.0) as f64 / 1000.0)
                        .unwrap_or(0.0);
                    let queue_remaining = state_snapshot
                        .map(|state| (state.4 - state.3).max(0.0))
                        .unwrap_or(0.0);
                    let horizon_secs_eval = state_snapshot
                        .map(|state| state.5.max(1) as f64)
                        .unwrap_or(params.paper_order_timeout_secs.max(1) as f64);
                    let current_fill_prob = Some(estimate_maker_fill_prob_for_order(
                        params,
                        side,
                        order.price,
                        order.qty.max(0.0),
                        queue_remaining,
                        observed_rate_current,
                        horizon_secs_eval,
                        best_bid,
                        best_ask,
                        tick,
                    ));
                    let desired_fill_prob = desired_price_for_order.map(|desired_price| {
                        let (_, observed_rate, top_size_opt) =
                            best_price_and_rate_for_order(ledger, open_leg, side);
                        let desired_queue = estimate_queue_ahead_for_order(
                            top_size_opt,
                            order.qty.max(0.0),
                            params.paper_queue_depth_levels,
                            params.maker_queue_ahead_mult,
                            side,
                            desired_price,
                            best_bid,
                            best_ask,
                            tick,
                        );
                        estimate_maker_fill_prob_for_order(
                            params,
                            side,
                            desired_price,
                            order.qty.max(0.0),
                            desired_queue,
                            observed_rate,
                            horizon_secs_eval,
                            best_bid,
                            best_ask,
                            tick,
                        )
                    });
                    let fill_prob_uplift = match (current_fill_prob, desired_fill_prob) {
                        (Some(prev), Some(new)) => Some(new - prev),
                        _ => None,
                    };
                    let uplift_ok = fill_prob_uplift
                        .map(|u| u + 1e-9 >= params.requote_min_fill_prob_uplift)
                        .unwrap_or(false);
                    let expected_consumed = observed_rate_current
                        .max(params.maker_flow_floor_per_sec)
                        * horizon_secs_eval;
                    let queue_sticky = if expected_consumed <= 1e-9 {
                        false
                    } else {
                        (queue_remaining / expected_consumed)
                            > params.requote_queue_stickiness_ratio
                            && order_age_secs < params.requote_stickiness_min_age_secs as f64
                    };
                    let price_move_ok = if params.paper_requote_require_price_move {
                        stale_by_price
                    } else {
                        true
                    };
                    let should_requote = requote_interval_ok
                        && (!has_progress || stale_by_age_hard)
                        && uplift_ok
                        && !queue_sticky
                        && stale_by_price
                        && price_move_ok
                        && (stale_off_top || stale_by_age_hard);
                    if should_requote {
                        let side_str = match side {
                            TradeSide::Buy => "buy".to_string(),
                            TradeSide::Sell => "sell".to_string(),
                        };
                        let requote_reason = if stale_by_age_hard {
                            "hard_stale_with_uplift".to_string()
                        } else {
                            "price_improve".to_string()
                        };
                        repricing_orders.push((
                            order.order_id.clone(),
                            order.client_order_id.clone(),
                            order.token_id.clone(),
                            side_str,
                            order.price,
                            order.qty,
                            side,
                            current_fill_prob,
                            desired_fill_prob,
                            fill_prob_uplift,
                            requote_reason,
                        ));
                    }
                }
                for (
                    order_id,
                    client_order_id,
                    token_id,
                    side_str,
                    price,
                    qty,
                    side,
                    requote_prev_fill_prob,
                    requote_new_fill_prob,
                    requote_fill_prob_uplift,
                    requote_reason,
                ) in repricing_orders
                {
                    match execution.adapter.cancel(&order_id).await {
                        Ok(_) => {
                            repriced_any = true;
                            ledger.requote_count_window =
                                ledger.requote_count_window.saturating_add(1);
                            canceled_ids.insert(order_id.clone());
                            if let Some(prev_state) = resting_orders.remove(&order_id) {
                                let key = paper_progress_key(&token_id, side);
                                let retained = (prev_state.consumed_qty
                                    * params.paper_requote_progress_retain)
                                    .max(0.0);
                                if retained > 0.0 {
                                    let cur = requote_progress.get(&key).copied().unwrap_or(0.0);
                                    requote_progress.insert(key.clone(), cur.max(retained));
                                }
                                if params.paper_requote_retain_fill_draw {
                                    requote_fill_draw.insert(key, prev_state.fill_draw);
                                } else {
                                    requote_fill_draw.remove(&key);
                                }
                            }
                            let mut extra = serde_json::Map::new();
                            extra.insert(
                                "requote_prev_fill_prob".to_string(),
                                requote_prev_fill_prob
                                    .map(Value::from)
                                    .unwrap_or(Value::Null),
                            );
                            extra.insert(
                                "requote_new_fill_prob".to_string(),
                                requote_new_fill_prob
                                    .map(Value::from)
                                    .unwrap_or(Value::Null),
                            );
                            extra.insert(
                                "requote_fill_prob_uplift".to_string(),
                                requote_fill_prob_uplift
                                    .map(Value::from)
                                    .unwrap_or(Value::Null),
                            );
                            extra.insert(
                                "requote_block_reason".to_string(),
                                Value::String(requote_reason),
                            );
                            log_synthetic_user_ws_order_with_extra(
                                ledger,
                                execution,
                                &order_id,
                                &client_order_id,
                                &token_id,
                                &side_str,
                                price,
                                qty,
                                "canceled",
                                "sim_order_cancel_requote",
                                Some(&extra),
                                log_ctx,
                            );
                        }
                        Err(err) => {
                            tracing::warn!(
                                order_id = %order_id,
                                error = %err,
                                "failed to cancel stale paper order for repricing"
                            );
                        }
                    }
                }
                let mut has_remaining_open_orders = open_orders
                    .iter()
                    .any(|o| !canceled_ids.contains(&o.order_id));
                if repriced_any && has_remaining_open_orders {
                    ledger.waiting_skip_count_window =
                        ledger.waiting_skip_count_window.saturating_add(1);
                    log_live_skip(
                        ledger,
                        "paper_requote_open_order",
                        "none",
                        execution,
                        log_ctx,
                    );
                    return;
                }
                let net = ledger.qty_up - ledger.qty_down;
                let would_reduce_abs_net = match applied.action.leg {
                    TradeLeg::Up => net < -1e-9,
                    TradeLeg::Down => net > 1e-9,
                };
                let in_balancing_phase = ledger.round_state == RoundPhase::Leg2Balancing;
                if would_reduce_abs_net && (in_tail_close || in_balancing_phase) {
                    for order in &open_orders {
                        if canceled_ids.contains(&order.order_id) {
                            continue;
                        }
                        let Some(open_leg) = leg_from_token_id(ledger, &order.token_id) else {
                            continue;
                        };
                        if open_leg != applied.action.leg {
                            match execution.adapter.cancel(&order.order_id).await {
                                Ok(_) => {
                                    canceled_for_rebalance = true;
                                    canceled_ids.insert(order.order_id.clone());
                                    if let Some(prev_state) = resting_orders.remove(&order.order_id)
                                    {
                                        let key = paper_progress_key(
                                            &prev_state.token_id,
                                            prev_state.side,
                                        );
                                        requote_progress.remove(&key);
                                        requote_fill_draw.remove(&key);
                                    }
                                }
                                Err(err) => {
                                    tracing::warn!(
                                        order_id = %order.order_id,
                                        error = %err,
                                        "failed to cancel blocking paper open order"
                                    );
                                }
                            }
                        }
                    }
                }
                let remaining_open_orders: Vec<&OpenOrder> = open_orders
                    .iter()
                    .filter(|o| !canceled_ids.contains(&o.order_id))
                    .collect();
                has_remaining_open_orders = !remaining_open_orders.is_empty();
                if canceled_for_rebalance && has_remaining_open_orders {
                    ledger.waiting_skip_count_window =
                        ledger.waiting_skip_count_window.saturating_add(1);
                    log_live_skip(
                        ledger,
                        "paper_cancel_open_order_for_rebalance",
                        "none",
                        execution,
                        log_ctx,
                    );
                    return;
                }
                if has_remaining_open_orders {
                    let mut active_total = 0usize;
                    let mut active_same_leg = 0usize;
                    for order in &remaining_open_orders {
                        let Some(open_leg) = leg_from_token_id(ledger, &order.token_id) else {
                            continue;
                        };
                        active_total += 1;
                        if open_leg == applied.action.leg {
                            active_same_leg += 1;
                        }
                    }
                    let can_parallel_open = applied.action.side == TradeSide::Buy
                        && active_total < PAPER_MAX_CONCURRENT_OPEN_ORDERS_TOTAL
                        && active_same_leg < PAPER_MAX_CONCURRENT_OPEN_ORDERS_PER_LEG;
                    if !can_parallel_open {
                        ledger.waiting_skip_count_window =
                            ledger.waiting_skip_count_window.saturating_add(1);
                        log_live_skip(
                            ledger,
                            "paper_waiting_open_order",
                            "none",
                            execution,
                            log_ctx,
                        );
                        return;
                    }
                }
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "failed to fetch open orders before placing paper maker order"
                );
            }
        }
    }
    let token_id = action_token_id(ledger, &applied.action).to_string();
    let intent = OrderIntent {
        market_slug: ledger.market_slug.clone(),
        token_id,
        side: action_order_side(&applied.action),
        price: applied.sim.fill_price.unwrap_or(0.0),
        qty: applied.action.qty,
        post_only: true,
        client_order_id: build_client_order_id(ledger, &applied.action),
    };

    let place_outcome: Result<PlaceAck> = execution.adapter.place_post_only(intent).await;

    let ack = match place_outcome {
        Ok(ack) => ack,
        Err(err) => PlaceAck {
            accepted: false,
            order_id: None,
            reason: Some(err.to_string()),
        },
    };
    log_live_place(ledger, applied, execution, &ack, log_ctx);
    let side_str = match applied.action.side {
        TradeSide::Buy => "buy",
        TradeSide::Sell => "sell",
    };
    let token_id = action_token_id(ledger, &applied.action).to_string();
    let client_order_id = build_client_order_id(ledger, &applied.action);
    let order_id_str = ack
        .order_id
        .clone()
        .unwrap_or_else(|| client_order_id.clone());
    if ack.accepted && execution.mode == ExecutionMode::Paper {
        ledger.open_count_window = ledger.open_count_window.saturating_add(1);
    }
    if ack.accepted && !execution.user_ws_enabled {
        let mut extra = serde_json::Map::new();
        extra.insert(
            "entry_quote_base_postonly_price".to_string(),
            applied
                .sim
                .entry_quote_base_postonly_price
                .map(Value::from)
                .unwrap_or(Value::Null),
        );
        extra.insert(
            "entry_quote_dynamic_cap_price".to_string(),
            applied
                .sim
                .entry_quote_dynamic_cap_price
                .map(Value::from)
                .unwrap_or(Value::Null),
        );
        extra.insert(
            "entry_quote_final_price".to_string(),
            applied
                .sim
                .entry_quote_final_price
                .map(Value::from)
                .unwrap_or(Value::Null),
        );
        extra.insert(
            "entry_quote_cap_active".to_string(),
            applied
                .sim
                .entry_quote_cap_active
                .map(Value::Bool)
                .unwrap_or(Value::Null),
        );
        extra.insert(
            "entry_quote_cap_bind".to_string(),
            applied
                .sim
                .entry_quote_cap_bind
                .map(Value::Bool)
                .unwrap_or(Value::Null),
        );
        extra.insert(
            "hedge_margin_to_opp_ask".to_string(),
            applied
                .sim
                .hedge_margin_to_opp_ask
                .map(Value::from)
                .unwrap_or(Value::Null),
        );
        extra.insert(
            "hedge_margin_required".to_string(),
            applied
                .sim
                .hedge_margin_required
                .map(Value::from)
                .unwrap_or(Value::Null),
        );
        extra.insert(
            "open_margin_surplus".to_string(),
            match (
                applied.sim.hedge_margin_to_opp_ask,
                applied.sim.hedge_margin_required,
            ) {
                (Some(actual), Some(required)) => Value::from(actual - required),
                _ => Value::Null,
            },
        );
        extra.insert(
            "hedge_margin_ok".to_string(),
            applied
                .sim
                .hedge_margin_ok
                .map(Value::Bool)
                .unwrap_or(Value::Null),
        );
        log_synthetic_user_ws_order_with_extra(
            ledger,
            execution,
            &order_id_str,
            &client_order_id,
            &token_id,
            side_str,
            applied.sim.fill_price.unwrap_or(0.0),
            applied.action.qty,
            "open",
            "sim_order_open",
            Some(&extra),
            log_ctx,
        );
    }

    if execution.mode == ExecutionMode::Paper && params.paper_resting_fill_enabled && ack.accepted {
        if let Some(order_id) = ack.order_id.clone() {
            let side = applied.action.side;
            let leg = applied.action.leg;
            let placed_ts_ms = now_ms;
            let current_net = ledger.qty_up - ledger.qty_down;
            let would_reduce_abs_net = match leg {
                TradeLeg::Up => current_net < -1e-9,
                TradeLeg::Down => current_net > 1e-9,
            };
            let time_left_secs = if ledger.end_ts > now_ts {
                ledger.end_ts - now_ts
            } else {
                0
            };
            let in_tail_close = time_left_secs <= params.tail_close_secs;
            let in_balancing_phase = ledger.round_state == RoundPhase::Leg2Balancing;
            let base_timeout_secs = params.paper_order_timeout_secs;
            let queue_ahead = applied
                .sim
                .maker_queue_ahead
                .unwrap_or_else(|| {
                    let (_, _, top_size_opt) = best_price_and_rate_for_order(ledger, leg, side);
                    let (best_bid, best_ask) = best_bid_ask_for_leg(ledger, leg);
                    estimate_queue_ahead_for_order(
                        top_size_opt,
                        applied.action.qty,
                        params.paper_queue_depth_levels,
                        params.maker_queue_ahead_mult,
                        side,
                        applied.sim.fill_price.unwrap_or(0.0),
                        best_bid,
                        best_ask,
                        ledger.tick_size_current.max(1e-6),
                    )
                })
                .max(0.0);
            let observed_rate = applied.sim.maker_consumption_rate.unwrap_or_else(|| {
                let (_, rate, _) = best_price_and_rate_for_order(ledger, leg, side);
                rate
            });
            let mut order_horizon_secs = adaptive_order_horizon_secs(
                base_timeout_secs,
                params.open_order_max_age_secs,
                queue_ahead,
                applied.action.qty,
                observed_rate.max(params.maker_flow_floor_per_sec),
                params.paper_timeout_target_fill_prob,
            );
            if would_reduce_abs_net && (in_tail_close || in_balancing_phase) {
                order_horizon_secs =
                    order_horizon_secs.max(params.round_pair_wait_secs.saturating_mul(3).max(12));
            }
            let progress_key = paper_progress_key(&token_id, side);
            let retained_consumed = requote_progress.remove(&progress_key).unwrap_or(0.0);
            let retained_fill_draw = requote_fill_draw.remove(&progress_key);
            let initial_consumed = retained_consumed
                .min((queue_ahead + applied.action.qty.max(0.0)).max(0.0))
                .max(0.0);
            resting_orders.insert(
                order_id.clone(),
                PaperRestingOrder {
                    order_id: order_id.clone(),
                    client_order_id: client_order_id.clone(),
                    token_id: token_id.clone(),
                    leg,
                    side,
                    price: applied.sim.fill_price.unwrap_or(0.0),
                    remaining_qty: applied.action.qty.max(0.0),
                    queue_ahead,
                    consumed_qty: initial_consumed,
                    fill_draw: retained_fill_draw.unwrap_or_else(|| {
                        deterministic_unit_from_order(&order_id, &client_order_id, placed_ts_ms)
                    }),
                    placed_ts_ms,
                    last_requote_ts_ms: placed_ts_ms,
                    active_after_ts_ms: placed_ts_ms
                        .saturating_add(params.paper_order_ack_delay_ms),
                    last_eval_ts_ms: placed_ts_ms,
                    horizon_secs: order_horizon_secs,
                    timeout_extend_count: 0,
                    first_unrecoverable_detected_ts_ms: None,
                    first_margin_fail_detected_ts_ms: None,
                },
            );
        }
    }

    let should_simulate_fill = match execution.mode {
        ExecutionMode::Paper => !params.paper_resting_fill_enabled,
        ExecutionMode::LiveShadow => true,
        ExecutionMode::Live => execution.sim_fill_enabled || !execution.user_ws_enabled,
    };
    let accepted_or_simulated = ack.accepted;
    if accepted_or_simulated && should_simulate_fill {
        let before = ledger.clone();
        apply_simulated_trade(ledger, &applied.action, &applied.sim, params, now_ts);
        if execution.mode == ExecutionMode::Paper {
            ledger.fill_count_window = ledger.fill_count_window.saturating_add(1);
        }
        if let Some(sim_pair_cost) = applied.sim.new_pair_cost {
            ledger.max_executed_sim_pair_cost_window = Some(
                ledger
                    .max_executed_sim_pair_cost_window
                    .map(|v| v.max(sim_pair_cost))
                    .unwrap_or(sim_pair_cost),
            );
        }
        let executed = (before.qty_up - ledger.qty_up).abs() > 1e-9
            || (before.qty_down - ledger.qty_down).abs() > 1e-9
            || (before.cost_up - ledger.cost_up).abs() > 1e-9
            || (before.cost_down - ledger.cost_down).abs() > 1e-9
            || (before.spent_total_usdc - ledger.spent_total_usdc).abs() > 1e-9
            || (before.spent_round_usdc - ledger.spent_round_usdc).abs() > 1e-9
            || before.round_idx != ledger.round_idx
            || before.round_state != ledger.round_state;
        log_dryrun_apply(&before, ledger, applied, executed, params, log_ctx);
        if execution.sim_fill_enabled {
            let raw_type = match execution.mode {
                ExecutionMode::Paper => "sim_fill_paper",
                ExecutionMode::LiveShadow => "sim_fill_shadow",
                ExecutionMode::Live => "sim_fill_live",
            };
            log_user_ws_fill(ledger, applied, execution, raw_type, log_ctx);
        }
    }

    if !execution.user_ws_enabled && !matches!(execution.mode, ExecutionMode::Paper) {
        log_synthetic_user_ws_order(
            ledger,
            execution,
            &order_id_str,
            &client_order_id,
            &token_id,
            side_str,
            applied.sim.fill_price.unwrap_or(0.0),
            applied.action.qty,
            "filled",
            "sim_order_filled",
            log_ctx,
        );
    }

    if !execution.user_ws_enabled
        && execution.mode == ExecutionMode::Live
        && !execution.sim_fill_enabled
    {
        tracing::warn!(
            market_slug = %ledger.market_slug,
            "live mode active without USER_WS_ENABLED and SIM_FILL; ledger updates will stall"
        );
    }

    let open_orders_result = execution
        .adapter
        .fetch_open_orders(&ledger.market_slug)
        .await;
    log_reconcile_snapshot(&ledger.market_slug, execution, &open_orders_result, log_ctx);
}

fn apply_simulated_trade(
    ledger: &mut DryRunLedger,
    action: &CandidateAction,
    sim: &SimResult,
    params: &DryRunParams,
    now_ts: u64,
) {
    let mut strategy_ledger = to_strategy_ledger(ledger);
    let strategy_action = strategy::simulate::CandidateAction {
        name: action.name,
        leg: to_strategy_trade_leg(action.leg),
        side: match action.side {
            TradeSide::Buy => strategy::state::TradeSide::Buy,
            TradeSide::Sell => strategy::state::TradeSide::Sell,
        },
        kind: match action.kind {
            TradeKind::Maker => strategy::state::TradeKind::Maker,
            TradeKind::Taker => strategy::state::TradeKind::Taker,
        },
        qty: action.qty,
    };
    let strategy_sim = strategy::simulate::SimResult {
        ok: sim.ok,
        fill_qty: sim.fill_qty,
        fill_price: sim.fill_price,
        maker_fill_prob: sim.maker_fill_prob,
        maker_queue_ahead: sim.maker_queue_ahead,
        maker_expected_consumed: sim.maker_expected_consumed,
        maker_consumption_rate: sim.maker_consumption_rate,
        maker_horizon_secs: sim.maker_horizon_secs,
        fee_estimate: sim.fee_estimate,
        spent_delta_usdc: sim.spent_delta_usdc,
        new_qty_up: sim.new_qty_up,
        new_cost_up: sim.new_cost_up,
        new_qty_down: sim.new_qty_down,
        new_cost_down: sim.new_cost_down,
        new_avg_up: sim.new_avg_up,
        new_avg_down: sim.new_avg_down,
        new_pair_cost: sim.new_pair_cost,
        new_hedgeable: sim.new_hedgeable,
        new_unhedged_up: sim.new_unhedged_up,
        new_unhedged_down: sim.new_unhedged_down,
        new_unhedged_value_up: sim.new_unhedged_value_up,
        new_unhedged_value_down: sim.new_unhedged_value_down,
        hedge_recoverable_now: sim.hedge_recoverable_now,
        required_opp_avg_price_cap: sim.required_opp_avg_price_cap,
        current_opp_best_ask: sim.current_opp_best_ask,
        required_hedge_qty: sim.required_hedge_qty,
        hedge_margin_to_opp_ask: sim.hedge_margin_to_opp_ask,
        hedge_margin_required: sim.hedge_margin_required,
        hedge_margin_ok: sim.hedge_margin_ok,
        entry_quote_base_postonly_price: sim.entry_quote_base_postonly_price,
        entry_quote_dynamic_cap_price: sim.entry_quote_dynamic_cap_price,
        entry_quote_final_price: sim.entry_quote_final_price,
        entry_quote_cap_active: sim.entry_quote_cap_active,
        entry_quote_cap_bind: sim.entry_quote_cap_bind,
        passive_gap_abs: sim.passive_gap_abs,
        passive_gap_ticks: sim.passive_gap_ticks,
        improves_hedge: sim.improves_hedge,
    };
    strategy::simulate::apply_simulated_trade(
        &mut strategy_ledger,
        &strategy_action,
        &strategy_sim,
        &to_strategy_params(params),
        now_ts,
    );
    copy_back_from_strategy_ledger(ledger, strategy_ledger);
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
        let top_bid_size = quote.bid_levels.first().map(|level| level.size);
        let top_ask_size = quote.ask_levels.first().map(|level| level.size);
        let bid_depth_levels = quote.bid_levels.len();
        let ask_depth_levels = quote.ask_levels.len();
        let bid_consume_rate = quote.bid_consumption_rate;
        let ask_consume_rate = quote.ask_consumption_rate;
        let exchange_ts_ms = quote.exchange_ts_ms;
        let recv_ts_ms = if quote.recv_ts_ms > 0 {
            Some(quote.recv_ts_ms)
        } else {
            None
        };
        let latency_ms = quote.latency_ms;
        let tick_entry =
            get_tick_size(asset_id, gamma_tick, tick_cache, http_client, clob_host).await?;
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
            "asset_id={asset_id} bid={} ask={} bid_sz={} ask_sz={} depth={}/{} consume_bid={:.4}/s consume_ask={:.4}/s exchange_ts_ms={} recv_ts_ms={} latency_ms={} tick={}(src={}) buy={} ok={} sell={} ok={}",
            format_opt_price(bid),
            format_opt_price(ask),
            format_opt_price(top_bid_size),
            format_opt_price(top_ask_size),
            bid_depth_levels,
            ask_depth_levels,
            bid_consume_rate,
            ask_consume_rate,
            format_opt_i64(exchange_ts_ms),
            format_opt_i64(recv_ts_ms),
            format_opt_i64(latency_ms),
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

    let book: BookResp = response
        .json()
        .await
        .context("invalid clob book response")?;
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

    let tick = estimate_tick_from_prices(&mut prices).context("unable to estimate tick size")?;
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
                        tracing::warn!(
                            "quotes stuck at extremes; ensure price_changes updates are applied"
                        );
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

fn format_opt_i64(value: Option<i64>) -> String {
    match value {
        Some(raw) => raw.to_string(),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn approx_eq(a: f64, b: f64) -> bool {
        (a - b).abs() <= 1e-9
    }

    fn test_dryrun_ledger() -> DryRunLedger {
        DryRunLedger {
            market_slug: "m".to_string(),
            series_slug: "s".to_string(),
            market_select_mode: "fixed_market_slug".to_string(),
            up_id: "up".to_string(),
            down_id: "down".to_string(),
            tick_size: 0.01,
            tick_size_current: 0.01,
            tick_size_source: "test".to_string(),
            start_ts: 0,
            end_ts: 900,
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
            round_leg1_entered_ts: 0,
            round_leg2_entered_ts: 0,
            round_leg2_anchor_price: None,
            round_leg1_target_qty: 0.0,
            round_leg1_filled_qty: 0.0,
            round_leg2_target_qty: 0.0,
            round_leg2_filled_qty: 0.0,
            last_apply_ts_ms: 0,
            last_round_complete_ts: 0,
            lock_reopen_used_rounds: 0,
            best_bid_up: None,
            best_ask_up: None,
            best_bid_down: None,
            best_ask_down: None,
            exchange_ts_ms_up: None,
            recv_ts_ms_up: None,
            latency_ms_up: None,
            exchange_ts_ms_down: None,
            recv_ts_ms_down: None,
            latency_ms_down: None,
            best_bid_size_up: None,
            best_ask_size_up: None,
            best_bid_size_down: None,
            best_ask_size_down: None,
            bid_consumption_rate_up: 0.0,
            ask_consumption_rate_up: 0.0,
            bid_consumption_rate_down: 0.0,
            ask_consumption_rate_down: 0.0,
            prev_mid_up: None,
            prev_mid_down: None,
            mid_up_fast_ema: None,
            mid_down_fast_ema: None,
            mid_up_slow_ema: None,
            mid_down_slow_ema: None,
            pair_mid_vol_bps: 0.0,
            mid_up_momentum_bps: 0.0,
            mid_down_momentum_bps: 0.0,
            mid_up_discount_bps: 0.0,
            mid_down_discount_bps: 0.0,
            last_decision_ts_ms: 0,
            decision_seq: 0,
            entry_worst_pair_deny_streak: 0,
            entry_worst_pair_streak_started_ts: 0,
            entry_fallback_until_ts: 0,
            open_orders_active: 0,
            locked: false,
            locked_hedgeable: 0.0,
            locked_pair_cost: None,
            locked_at_ts_ms: 0,
            open_count_window: 0,
            fill_count_window: 0,
            timeout_extend_count_window: 0,
            requote_count_window: 0,
            waiting_skip_count_window: 0,
            risk_guard_cancel_count_window: 0,
            risk_guard_cancel_pair_cap_count_window: 0,
            risk_guard_cancel_other_count_window: 0,
            resting_hard_risk_cancel_count_window: 0,
            resting_soft_recheck_cancel_count_window: 0,
            stale_cancel_count_window: 0,
            close_window_cancel_count_window: 0,
            entry_worst_pair_block_count_window: 0,
            unrecoverable_block_count_window: 0,
            cancel_unrecoverable_count_window: 0,
            max_executed_sim_pair_cost_window: None,
            tail_pair_cap_block_count_window: 0,
        }
    }

    #[test]
    fn same_price_depletion_rate_is_capped() {
        let rate = estimate_consumption_rate(
            BookSide::Bid,
            Some(0.50),
            Some(1000.0),
            Some(0.50),
            Some(0.0),
            1_000,
            1_001,
            0.0,
        )
        .expect("rate");
        assert!(approx_eq(rate, DEPTH_PRICE_MOVE_MAX_OBSERVED_PER_SEC));
    }

    #[test]
    fn price_move_depletion_rate_is_capped() {
        let rate = estimate_consumption_rate(
            BookSide::Ask,
            Some(0.50),
            Some(1000.0),
            Some(0.51),
            Some(0.0),
            1_000,
            1_001,
            0.0,
        )
        .expect("rate");
        assert!(approx_eq(rate, DEPTH_PRICE_MOVE_MAX_OBSERVED_PER_SEC));
    }

    #[test]
    fn ewma_output_is_capped() {
        let rate = estimate_consumption_rate(
            BookSide::Bid,
            Some(0.50),
            Some(2.0),
            Some(0.50),
            Some(1.0),
            1_000,
            2_000,
            100.0,
        )
        .expect("rate");
        assert!(approx_eq(rate, DEPTH_PRICE_MOVE_MAX_OBSERVED_PER_SEC));
    }

    #[test]
    fn non_depletion_move_returns_none() {
        let rate = estimate_consumption_rate(
            BookSide::Bid,
            Some(0.50),
            Some(100.0),
            Some(0.51),
            Some(50.0),
            1_000,
            1_500,
            1.0,
        );
        assert!(rate.is_none());
    }

    #[test]
    fn leg_mid_vol_detects_opposite_moves_even_when_sum_is_constant() {
        let mut ledger = test_dryrun_ledger();
        // First sample initializes prev mids.
        ledger.best_bid_up = Some(0.39);
        ledger.best_ask_up = Some(0.41); // mid 0.40
        ledger.best_bid_down = Some(0.59);
        ledger.best_ask_down = Some(0.61); // mid 0.60
        ledger.update_pair_mid_vol_bps(10, 6, 30);
        assert!(approx_eq(ledger.pair_mid_vol_bps, 0.0));

        // Second sample keeps sum(mid_up + mid_down)=1.0 but both legs move.
        ledger.best_bid_up = Some(0.44);
        ledger.best_ask_up = Some(0.46); // mid 0.45
        ledger.best_bid_down = Some(0.54);
        ledger.best_ask_down = Some(0.56); // mid 0.55
        ledger.update_pair_mid_vol_bps(10, 6, 30);
        assert!(ledger.pair_mid_vol_bps > 100.0);
    }

    #[test]
    fn leg_mid_vol_requires_both_sides_present() {
        let mut ledger = test_dryrun_ledger();
        ledger.best_bid_up = Some(0.49);
        ledger.best_ask_up = Some(0.50);
        ledger.best_bid_down = None;
        ledger.best_ask_down = None;
        ledger.update_pair_mid_vol_bps(10, 6, 30);
        assert!(approx_eq(ledger.pair_mid_vol_bps, 0.0));
    }

    #[test]
    fn timeout_extend_allows_when_progress_threshold_met() {
        assert!(should_extend_timeout(0.50, 0, 0.50, 12, 1));
        assert!(should_extend_timeout(0.61, 0, 0.50, 12, 1));
    }

    #[test]
    fn timeout_extend_blocks_when_no_budget_or_no_progress() {
        assert!(!should_extend_timeout(0.49, 0, 0.50, 12, 1));
        assert!(!should_extend_timeout(0.80, 1, 0.50, 12, 1));
        assert!(!should_extend_timeout(0.80, 0, 0.50, 0, 1));
    }
}

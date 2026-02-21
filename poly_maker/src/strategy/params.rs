use std::env;

use super::state::{ApplyKind, DryRunMode};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoRiskPairLimitMode {
    HardCapOnly,
    LegacyMarginThenCap,
}

impl NoRiskPairLimitMode {
    fn from_env(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "hard_cap_only" | "hardcap" | "hard" => Some(Self::HardCapOnly),
            "legacy_margin_then_cap" | "legacy" => Some(Self::LegacyMarginThenCap),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::HardCapOnly => "hard_cap_only",
            Self::LegacyMarginThenCap => "legacy_margin_then_cap",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoRiskEntryHedgePriceMode {
    Ask,
    BidPlusBps,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryPairRegressionMode {
    StrictMonotonic,
    SoftBand,
    CapEdge,
}

impl EntryPairRegressionMode {
    fn from_env(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "strict_monotonic" | "strict" | "monotonic" => Some(Self::StrictMonotonic),
            "soft_band" | "softband" | "band" => Some(Self::SoftBand),
            "cap_edge" | "capedge" | "edge" => Some(Self::CapEdge),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::StrictMonotonic => "strict_monotonic",
            Self::SoftBand => "soft_band",
            Self::CapEdge => "cap_edge",
        }
    }
}

impl NoRiskEntryHedgePriceMode {
    fn from_env(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "ask" => Some(Self::Ask),
            "bid_plus_bps" | "bidplusbps" | "bid" => Some(Self::BidPlusBps),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ask => "ask",
            Self::BidPlusBps => "bid_plus_bps",
        }
    }
}

#[derive(Debug, Clone)]
pub struct StrategyParams {
    pub improve_min: f64,
    pub margin_target: f64,
    pub safety_margin: f64,
    pub total_budget_usdc: f64,
    pub total_budget_source: String,
    pub max_rounds: u64,
    pub round_budget_usdc: f64,
    pub round_budget_source: String,
    pub round_leg1_fraction: f64,
    pub max_unhedged_value: f64,
    pub cap_unhedged_value: Option<f64>,
    pub max_unhedged_shares: Option<f64>,
    pub cooldown_secs: u64,
    pub tail_freeze_secs: u64,
    pub tail_close_secs: u64,
    pub decision_every_ms: u64,
    pub mode: DryRunMode,
    pub apply_kind: ApplyKind,
    pub pair_bonus: f64,
    pub lock_strict_abs_net: bool,
    pub round_budget_strict: bool,
    pub lock_min_completed_rounds: u64,
    pub lock_min_time_left_secs: u64,
    pub lock_min_spent_ratio: f64,
    pub lock_force_time_left_secs: u64,
    pub tail_close_ignore_margin: bool,
    pub vol_entry_min_bps: f64,
    pub vol_entry_lookback_ticks: u64,
    pub reversal_entry_enabled: bool,
    pub reversal_min_discount_bps: f64,
    pub reversal_min_momentum_bps: f64,
    pub reversal_fast_ema_ticks: u64,
    pub reversal_slow_ema_ticks: u64,
    pub entry_turn_confirm_ticks: u64,
    pub entry_turn_min_rebound_bps: f64,
    pub entry_pair_buffer_bps: f64,
    pub round_pair_wait_secs: u64,
    pub leg2_rebalance_discount_bps: f64,
    pub force_pair_max_pair_cost: f64,
    pub no_risk_hard_pair_cap: f64,
    pub no_risk_pair_limit_mode: NoRiskPairLimitMode,
    pub no_risk_enforce_tail: bool,
    pub no_risk_entry_hedge_price_mode: NoRiskEntryHedgePriceMode,
    pub no_risk_entry_ask_slippage_bps: f64,
    pub no_risk_entry_worst_hedge_bps: f64,
    pub no_risk_entry_pair_headroom_bps: f64,
    pub entry_dynamic_cap_enabled: bool,
    pub entry_dynamic_cap_headroom_bps: f64,
    pub entry_dynamic_cap_min_price: f64,
    pub entry_dynamic_cap_apply_to_net_increase_only: bool,
    pub entry_target_margin_min_ticks: f64,
    pub entry_target_margin_min_bps: f64,
    pub entry_max_passive_ticks_for_net_increase: f64,
    pub no_risk_late_new_round_buffer_secs: u64,
    pub no_risk_require_completable_round: bool,
    pub no_risk_strict_zero_unmatched: bool,
    pub no_risk_hedge_recoverability_enforce: bool,
    pub no_risk_hedge_recoverability_eps_bps: f64,
    pub hedge_recoverability_margin_enforce: bool,
    pub hedge_recoverability_margin_min_ticks: f64,
    pub hedge_recoverability_margin_min_bps: f64,
    pub hedge_recoverability_margin_apply_to_net_increase_only: bool,
    pub open_order_risk_guard: bool,
    pub open_order_risk_buffer_bps: f64,
    pub open_order_risk_guard_require_paired: bool,
    pub open_order_max_age_secs: u64,
    pub open_order_unrecoverable_grace_ms: u64,
    pub paper_timeout_target_fill_prob: f64,
    pub paper_timeout_progress_extend_min: f64,
    pub paper_timeout_progress_extend_secs: u64,
    pub paper_timeout_max_extends: u64,
    pub paper_requote_require_price_move: bool,
    pub paper_requote_stale_ms_hard: u64,
    pub paper_requote_retain_fill_draw: bool,
    pub requote_min_fill_prob_uplift: f64,
    pub requote_queue_stickiness_ratio: f64,
    pub requote_stickiness_min_age_secs: u64,
    pub round_slice_count: u64,
    pub round_dynamic_slicing_enabled: bool,
    pub round_min_slices: u64,
    pub round_max_slices: u64,
    pub round_min_slice_qty: f64,
    pub entry_pair_regression_mode: EntryPairRegressionMode,
    pub entry_pair_regression_soft_band_bps: f64,
    pub entry_edge_min_bps: f64,
    pub entry_fill_prob_min: f64,
    pub open_min_fill_prob: f64,
    pub open_margin_surplus_min: f64,
    pub inventory_skew_alpha_bps: f64,
    pub lock_allow_reopen_before_freeze: bool,
    pub lock_reopen_max_rounds: u64,
    pub round_min_start_gap_secs: u64,
    pub opening_no_trade_secs: u64,
    pub min_apply_interval_ms: u64,
    pub maker_fill_estimator_enabled: bool,
    pub maker_fill_horizon_secs: u64,
    pub maker_min_fill_prob: f64,
    pub maker_queue_ahead_mult: f64,
    pub maker_fill_passive_queue_penalty_per_tick: f64,
    pub maker_fill_passive_decay_k: f64,
    pub maker_flow_floor_per_sec: f64,
    pub entry_passive_gap_soft_max_ticks: f64,
    pub entry_max_top_book_share: f64,
    pub entry_max_flow_utilization: f64,
    pub entry_min_timeout_flow_ratio: f64,
    pub entry_fallback_enabled: bool,
    pub entry_fallback_deny_streak: u64,
    pub entry_fallback_window_secs: u64,
    pub entry_fallback_duration_secs: u64,
    pub entry_fallback_hedge_mode: NoRiskEntryHedgePriceMode,
    pub entry_fallback_worst_hedge_bps: f64,
    pub check_summary_skip_plot: bool,
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

impl StrategyParams {
    pub fn effective_pair_limit(&self, force_pair_after_wait: bool) -> f64 {
        let hard_cap = self.no_risk_hard_pair_cap.min(1.0);
        match self.no_risk_pair_limit_mode {
            NoRiskPairLimitMode::HardCapOnly => hard_cap,
            NoRiskPairLimitMode::LegacyMarginThenCap => {
                if force_pair_after_wait {
                    self.force_pair_max_pair_cost.min(hard_cap)
                } else {
                    (1.0 - self.margin_target).min(hard_cap)
                }
            }
        }
    }

    pub fn effective_max_rounds(&self, spent_total_usdc: f64, time_left_secs: u64) -> u64 {
        let base = self.max_rounds.max(1);
        if base < 4 {
            return base;
        }
        // Spent/time-driven dynamic rounds:
        // If budget utilization is still low while there is ample time left, allow extra rounds
        // to keep trading density instead of entering Done too early.
        let spent_ratio = if self.total_budget_usdc > 0.0 {
            (spent_total_usdc / self.total_budget_usdc).clamp(0.0, 1.0)
        } else {
            1.0
        };
        let min_time_left_for_extra = self.tail_freeze_secs.saturating_add(180);
        if time_left_secs <= min_time_left_for_extra {
            return base;
        }
        if spent_ratio + 1e-9 < 0.20 {
            // Strong extension when utilization is very low.
            return base.saturating_add(base.min(8));
        }
        if spent_ratio + 1e-9 < 0.35 {
            // Mild extension to keep round density from stalling in the middle of the window.
            return base.saturating_add((base / 2).max(1).min(4));
        }
        base
    }

    pub fn effective_lock_reopen_max_rounds(&self, spent_total_usdc: f64, time_left_secs: u64) -> u64 {
        let base = self.lock_reopen_max_rounds;
        if base == 0 {
            return 0;
        }
        let min_time_left_for_extra = self.tail_freeze_secs.saturating_add(240);
        if time_left_secs <= min_time_left_for_extra {
            return base;
        }
        let spent_ratio = if self.total_budget_usdc > 0.0 {
            (spent_total_usdc / self.total_budget_usdc).clamp(0.0, 1.0)
        } else {
            1.0
        };
        if spent_ratio + 1e-9 < 0.20 {
            return base.saturating_add(4);
        }
        if spent_ratio + 1e-9 < 0.35 {
            return base.saturating_add(2);
        }
        base
    }

    pub fn effective_open_margin_surplus_min(
        &self,
        spent_total_usdc: f64,
        time_left_secs: u64,
    ) -> f64 {
        let base = self.open_margin_surplus_min.max(0.0);
        if base <= 0.0 {
            return 0.0;
        }
        let spent_ratio = if self.total_budget_usdc > 0.0 {
            (spent_total_usdc / self.total_budget_usdc).clamp(0.0, 1.0)
        } else {
            1.0
        };
        // Early-window adaptive relax:
        // while budget utilization is still low and there is ample time left,
        // avoid hard-blocking all candidates on a near-zero surplus boundary.
        let relax_spent_ratio = (self.lock_min_spent_ratio * 0.5).clamp(0.0, 0.25);
        let relax_time_left_secs = self
            .lock_force_time_left_secs
            .max(self.tail_freeze_secs.saturating_add(120));
        if relax_spent_ratio > 0.0
            && time_left_secs >= relax_time_left_secs
            && spent_ratio + 1e-9 < relax_spent_ratio
        {
            return 0.0;
        }
        base
    }

    pub fn from_env() -> Self {
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

        let total_budget_env = env::var("TOTAL_BUDGET_USDC")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v > 0.0);
        let legacy_budget = env::var("MAX_NET_INVEST_USDC")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v > 0.0);
        let (total_budget_usdc, total_budget_source) = if let Some(value) = total_budget_env {
            (value, "TOTAL_BUDGET_USDC".to_string())
        } else if let Some(value) = legacy_budget {
            (value, "MAX_NET_INVEST_USDC".to_string())
        } else {
            (10.0, "default(10.0)".to_string())
        };

        let max_rounds = env::var("MAX_ROUNDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(2);
        let round_budget_env = env::var("ROUND_BUDGET_USDC")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v > 0.0);
        let (round_budget_usdc, round_budget_source) = if let Some(value) = round_budget_env {
            (value, "ROUND_BUDGET_USDC".to_string())
        } else {
            (
                total_budget_usdc / max_rounds.max(1) as f64,
                "derived(total_budget/max_rounds)".to_string(),
            )
        };

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
        let lock_strict_abs_net = read_bool_env("LOCK_STRICT_ABS_NET", true);
        let round_budget_strict = read_bool_env("ROUND_BUDGET_STRICT", false);
        let lock_min_completed_rounds = env::var("LOCK_MIN_COMPLETED_ROUNDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(1)
            .max(1);
        let lock_min_time_left_secs = env::var("LOCK_MIN_TIME_LEFT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let lock_min_spent_ratio = env::var("LOCK_MIN_SPENT_RATIO")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0 && *v <= 1.0)
            .unwrap_or(0.35);
        let lock_force_time_left_secs = env::var("LOCK_FORCE_TIME_LEFT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(540);
        let tail_close_ignore_margin = read_bool_env("TAIL_CLOSE_IGNORE_MARGIN", true);
        let vol_entry_min_bps = env::var("VOL_ENTRY_MIN_BPS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(0.0);
        let vol_entry_lookback_ticks = env::var("VOL_ENTRY_LOOKBACK_TICKS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(10);
        let reversal_entry_enabled = read_bool_env("REVERSAL_ENTRY_ENABLED", true);
        let reversal_min_discount_bps = env::var("REVERSAL_MIN_DISCOUNT_BPS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(6.0);
        let reversal_min_momentum_bps = env::var("REVERSAL_MIN_MOMENTUM_BPS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite())
            .unwrap_or(2.0);
        let reversal_fast_ema_ticks = env::var("REVERSAL_FAST_EMA_TICKS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(6);
        let reversal_slow_ema_ticks = env::var("REVERSAL_SLOW_EMA_TICKS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(30)
            .max(reversal_fast_ema_ticks);
        let entry_turn_confirm_ticks = env::var("ENTRY_TURN_CONFIRM_TICKS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(4);
        let entry_turn_min_rebound_bps = env::var("ENTRY_TURN_MIN_REBOUND_BPS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(8.0);
        let entry_pair_buffer_bps = env::var("ENTRY_PAIR_BUFFER_BPS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(0.0);
        let round_pair_wait_secs = env::var("ROUND_PAIR_WAIT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(5);
        let leg2_rebalance_discount_bps = env::var("LEG2_REBALANCE_DISCOUNT_BPS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(0.0);
        let force_pair_max_pair_cost = env::var("FORCE_PAIR_MAX_PAIR_COST")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v > 0.0)
            .unwrap_or(1.0);
        let no_risk_hard_pair_cap = env::var("NO_RISK_HARD_PAIR_CAP")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v > 0.0)
            .map(|v| v.min(1.0))
            .unwrap_or(0.995);
        let no_risk_pair_limit_mode = env::var("NO_RISK_PAIR_LIMIT_MODE")
            .ok()
            .and_then(|v| NoRiskPairLimitMode::from_env(&v))
            .unwrap_or(NoRiskPairLimitMode::HardCapOnly);
        let no_risk_enforce_tail = read_bool_env("NO_RISK_ENFORCE_TAIL", true);
        let no_risk_entry_hedge_price_mode = env::var("NO_RISK_ENTRY_HEDGE_PRICE_MODE")
            .ok()
            .and_then(|v| NoRiskEntryHedgePriceMode::from_env(&v))
            .unwrap_or(NoRiskEntryHedgePriceMode::Ask);
        let no_risk_entry_ask_slippage_bps = env::var("NO_RISK_ENTRY_ASK_SLIPPAGE_BPS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(5.0);
        let no_risk_entry_worst_hedge_bps = env::var("NO_RISK_ENTRY_WORST_HEDGE_BPS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(15.0);
        let no_risk_entry_pair_headroom_bps = env::var("NO_RISK_ENTRY_PAIR_HEADROOM_BPS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(5.0);
        let entry_dynamic_cap_enabled = read_bool_env("ENTRY_DYNAMIC_CAP_ENABLED", true);
        let entry_dynamic_cap_headroom_bps = env::var("ENTRY_DYNAMIC_CAP_HEADROOM_BPS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(8.0);
        let entry_dynamic_cap_min_price = env::var("ENTRY_DYNAMIC_CAP_MIN_PRICE")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v > 0.0)
            .unwrap_or(0.01);
        let entry_dynamic_cap_apply_to_net_increase_only =
            read_bool_env("ENTRY_DYNAMIC_CAP_APPLY_TO_NET_INCREASE_ONLY", true);
        let entry_target_margin_min_ticks = env::var("ENTRY_TARGET_MARGIN_MIN_TICKS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(0.8);
        let entry_target_margin_min_bps = env::var("ENTRY_TARGET_MARGIN_MIN_BPS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(10.0);
        let entry_max_passive_ticks_for_net_increase =
            env::var("ENTRY_MAX_PASSIVE_TICKS_FOR_NET_INCREASE")
                .ok()
                .and_then(|v| v.parse::<f64>().ok())
                .filter(|v| v.is_finite() && *v >= 0.0)
                .unwrap_or(1.5);
        let no_risk_late_new_round_buffer_secs = env::var("NO_RISK_LATE_NEW_ROUND_BUFFER_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(120);
        let no_risk_require_completable_round =
            read_bool_env("NO_RISK_REQUIRE_COMPLETABLE_ROUND", true);
        let no_risk_strict_zero_unmatched = read_bool_env("NO_RISK_STRICT_ZERO_UNMATCHED", true);
        let no_risk_hedge_recoverability_enforce =
            read_bool_env("NO_RISK_HEDGE_RECOVERABILITY_ENFORCE", true);
        let no_risk_hedge_recoverability_eps_bps = env::var("NO_RISK_HEDGE_RECOVERABILITY_EPS_BPS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(2.0);
        let hedge_recoverability_margin_enforce =
            read_bool_env("HEDGE_RECOVERABILITY_MARGIN_ENFORCE", true);
        let hedge_recoverability_margin_min_ticks =
            env::var("HEDGE_RECOVERABILITY_MARGIN_MIN_TICKS")
                .ok()
                .and_then(|v| v.parse::<f64>().ok())
                .filter(|v| v.is_finite() && *v >= 0.0)
                .unwrap_or(0.5);
        let hedge_recoverability_margin_min_bps = env::var("HEDGE_RECOVERABILITY_MARGIN_MIN_BPS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(8.0);
        let hedge_recoverability_margin_apply_to_net_increase_only = read_bool_env(
            "HEDGE_RECOVERABILITY_MARGIN_APPLY_TO_NET_INCREASE_ONLY",
            true,
        );
        let open_order_risk_guard = read_bool_env("OPEN_ORDER_RISK_GUARD", true);
        let open_order_risk_buffer_bps = env::var("OPEN_ORDER_RISK_BUFFER_BPS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(20.0);
        let open_order_risk_guard_require_paired =
            read_bool_env("OPEN_ORDER_RISK_GUARD_REQUIRE_PAIRED", true);
        let open_order_max_age_secs = env::var("OPEN_ORDER_MAX_AGE_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(45);
        let open_order_unrecoverable_grace_ms = env::var("OPEN_ORDER_UNRECOVERABLE_GRACE_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(2500);
        let paper_timeout_target_fill_prob = env::var("PAPER_TIMEOUT_TARGET_FILL_PROB")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v > 0.0 && *v < 1.0)
            .unwrap_or(0.60);
        let paper_timeout_progress_extend_min = env::var("PAPER_TIMEOUT_PROGRESS_EXTEND_MIN")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0 && *v <= 1.0)
            .unwrap_or(0.50);
        let paper_timeout_progress_extend_secs = env::var("PAPER_TIMEOUT_PROGRESS_EXTEND_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(12);
        let paper_timeout_max_extends = env::var("PAPER_TIMEOUT_MAX_EXTENDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(1);
        let paper_requote_require_price_move =
            read_bool_env("PAPER_REQUOTE_REQUIRE_PRICE_MOVE", true);
        let paper_requote_stale_ms_hard = env::var("PAPER_REQUOTE_STALE_MS_HARD")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(5000);
        let paper_requote_retain_fill_draw = read_bool_env("PAPER_REQUOTE_RETAIN_FILL_DRAW", false);
        let requote_min_fill_prob_uplift = env::var("REQUOTE_MIN_FILL_PROB_UPLIFT")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(0.015);
        let requote_queue_stickiness_ratio = env::var("REQUOTE_QUEUE_STICKINESS_RATIO")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v > 0.0)
            .unwrap_or(4.0);
        let requote_stickiness_min_age_secs = env::var("REQUOTE_STICKINESS_MIN_AGE_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(20);
        let round_slice_count = env::var("ROUND_SLICE_COUNT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(1);
        let round_dynamic_slicing_enabled = read_bool_env("ROUND_DYNAMIC_SLICING_ENABLED", true);
        let round_min_slices = env::var("ROUND_MIN_SLICES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(4);
        let round_max_slices = env::var("ROUND_MAX_SLICES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(12)
            .max(round_min_slices);
        let round_min_slice_qty = env::var("ROUND_MIN_SLICE_QTY")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v > 0.0)
            .unwrap_or(1.0);
        let entry_pair_regression_mode = env::var("ENTRY_PAIR_REGRESSION_MODE")
            .ok()
            .and_then(|v| EntryPairRegressionMode::from_env(&v))
            .unwrap_or(EntryPairRegressionMode::CapEdge);
        let entry_pair_regression_soft_band_bps = env::var("ENTRY_PAIR_REGRESSION_SOFT_BAND_BPS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(40.0);
        let entry_edge_min_bps = env::var("ENTRY_EDGE_MIN_BPS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(20.0);
        let entry_fill_prob_min = env::var("ENTRY_FILL_PROB_MIN")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(0.0);
        let open_min_fill_prob = env::var("OPEN_MIN_FILL_PROB")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(0.06);
        let open_margin_surplus_min = env::var("OPEN_MARGIN_SURPLUS_MIN")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(0.0005);
        let inventory_skew_alpha_bps = env::var("INVENTORY_SKEW_ALPHA_BPS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(0.0);
        let lock_allow_reopen_before_freeze =
            read_bool_env("LOCK_ALLOW_REOPEN_BEFORE_FREEZE", false);
        let lock_reopen_max_rounds = env::var("LOCK_REOPEN_MAX_ROUNDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let round_min_start_gap_secs = env::var("ROUND_MIN_START_GAP_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let opening_no_trade_secs = env::var("OPENING_NO_TRADE_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let min_apply_interval_ms = env::var("MIN_APPLY_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let maker_fill_estimator_enabled = read_bool_env("MAKER_FILL_ESTIMATOR_ENABLED", true);
        let maker_fill_horizon_secs = env::var("MAKER_FILL_HORIZON_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(8);
        let maker_min_fill_prob = env::var("MAKER_MIN_FILL_PROB")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0 && *v <= 1.0)
            .unwrap_or(entry_fill_prob_min.clamp(0.0, 1.0));
        let maker_queue_ahead_mult = env::var("MAKER_QUEUE_AHEAD_MULT")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(1.0);
        let maker_fill_passive_queue_penalty_per_tick =
            env::var("MAKER_FILL_PASSIVE_QUEUE_PENALTY_PER_TICK")
                .ok()
                .and_then(|v| v.parse::<f64>().ok())
                .filter(|v| v.is_finite() && *v >= 0.0)
                .unwrap_or(1.25);
        let maker_fill_passive_decay_k = env::var("MAKER_FILL_PASSIVE_DECAY_K")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(0.35);
        let maker_flow_floor_per_sec = env::var("MAKER_FLOW_FLOOR_PER_SEC")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(0.01);
        let entry_passive_gap_soft_max_ticks = env::var("ENTRY_PASSIVE_GAP_SOFT_MAX_TICKS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(3.0);
        let entry_max_top_book_share = env::var("ENTRY_MAX_TOP_BOOK_SHARE")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v > 0.0)
            .unwrap_or(0.5);
        let entry_max_flow_utilization = env::var("ENTRY_MAX_FLOW_UTILIZATION")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v > 0.0)
            .unwrap_or(0.8);
        let entry_min_timeout_flow_ratio = env::var("ENTRY_MIN_TIMEOUT_FLOW_RATIO")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(0.0);
        let entry_fallback_enabled = read_bool_env("ENTRY_FALLBACK_ENABLED", true);
        let entry_fallback_deny_streak = env::var("ENTRY_FALLBACK_DENY_STREAK")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(40);
        let entry_fallback_window_secs = env::var("ENTRY_FALLBACK_WINDOW_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(120);
        let entry_fallback_duration_secs = env::var("ENTRY_FALLBACK_DURATION_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(120);
        let entry_fallback_hedge_mode = env::var("ENTRY_FALLBACK_HEDGE_MODE")
            .ok()
            .and_then(|v| NoRiskEntryHedgePriceMode::from_env(&v))
            .unwrap_or(NoRiskEntryHedgePriceMode::BidPlusBps);
        let entry_fallback_worst_hedge_bps = env::var("ENTRY_FALLBACK_WORST_HEDGE_BPS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(10.0);
        let check_summary_skip_plot = read_bool_env("CHECK_SUMMARY_SKIP_PLOT", true);

        Self {
            improve_min,
            margin_target,
            safety_margin,
            total_budget_usdc,
            total_budget_source,
            max_rounds,
            round_budget_usdc,
            round_budget_source,
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
            lock_strict_abs_net,
            round_budget_strict,
            lock_min_completed_rounds,
            lock_min_time_left_secs,
            lock_min_spent_ratio,
            lock_force_time_left_secs,
            tail_close_ignore_margin,
            vol_entry_min_bps,
            vol_entry_lookback_ticks,
            reversal_entry_enabled,
            reversal_min_discount_bps,
            reversal_min_momentum_bps,
            reversal_fast_ema_ticks,
            reversal_slow_ema_ticks,
            entry_turn_confirm_ticks,
            entry_turn_min_rebound_bps,
            entry_pair_buffer_bps,
            round_pair_wait_secs,
            leg2_rebalance_discount_bps,
            force_pair_max_pair_cost,
            no_risk_hard_pair_cap,
            no_risk_pair_limit_mode,
            no_risk_enforce_tail,
            no_risk_entry_hedge_price_mode,
            no_risk_entry_ask_slippage_bps,
            no_risk_entry_worst_hedge_bps,
            no_risk_entry_pair_headroom_bps,
            entry_dynamic_cap_enabled,
            entry_dynamic_cap_headroom_bps,
            entry_dynamic_cap_min_price,
            entry_dynamic_cap_apply_to_net_increase_only,
            entry_target_margin_min_ticks,
            entry_target_margin_min_bps,
            entry_max_passive_ticks_for_net_increase,
            no_risk_late_new_round_buffer_secs,
            no_risk_require_completable_round,
            no_risk_strict_zero_unmatched,
            no_risk_hedge_recoverability_enforce,
            no_risk_hedge_recoverability_eps_bps,
            hedge_recoverability_margin_enforce,
            hedge_recoverability_margin_min_ticks,
            hedge_recoverability_margin_min_bps,
            hedge_recoverability_margin_apply_to_net_increase_only,
            open_order_risk_guard,
            open_order_risk_buffer_bps,
            open_order_risk_guard_require_paired,
            open_order_max_age_secs,
            open_order_unrecoverable_grace_ms,
            paper_timeout_target_fill_prob,
            paper_timeout_progress_extend_min,
            paper_timeout_progress_extend_secs,
            paper_timeout_max_extends,
            paper_requote_require_price_move,
            paper_requote_stale_ms_hard,
            paper_requote_retain_fill_draw,
            requote_min_fill_prob_uplift,
            requote_queue_stickiness_ratio,
            requote_stickiness_min_age_secs,
            round_slice_count,
            round_dynamic_slicing_enabled,
            round_min_slices,
            round_max_slices,
            round_min_slice_qty,
            entry_pair_regression_mode,
            entry_pair_regression_soft_band_bps,
            entry_edge_min_bps,
            entry_fill_prob_min,
            open_min_fill_prob,
            open_margin_surplus_min,
            inventory_skew_alpha_bps,
            lock_allow_reopen_before_freeze,
            lock_reopen_max_rounds,
            round_min_start_gap_secs,
            opening_no_trade_secs,
            min_apply_interval_ms,
            maker_fill_estimator_enabled,
            maker_fill_horizon_secs,
            maker_min_fill_prob,
            maker_queue_ahead_mult,
            maker_fill_passive_queue_penalty_per_tick,
            maker_fill_passive_decay_k,
            maker_flow_floor_per_sec,
            entry_passive_gap_soft_max_ticks,
            entry_max_top_book_share,
            entry_max_flow_utilization,
            entry_min_timeout_flow_ratio,
            entry_fallback_enabled,
            entry_fallback_deny_streak,
            entry_fallback_window_secs,
            entry_fallback_duration_secs,
            entry_fallback_hedge_mode,
            entry_fallback_worst_hedge_bps,
            check_summary_skip_plot,
        }
    }
}

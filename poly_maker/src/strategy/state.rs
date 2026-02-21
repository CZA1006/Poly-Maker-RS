use super::params::StrategyParams;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeLeg {
    Up,
    Down,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeKind {
    Maker,
    Taker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DryRunMode {
    Recommend,
    Paper,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyKind {
    BestAction,
    BestMakerOnly,
    BestTakerOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoundPhase {
    Idle,
    Leg1Accumulating,
    Leg2Balancing,
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TailMode {
    None,
    Freeze,
    Close,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockState {
    Unlocked,
    Locked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DenyReason {
    TakerDisabled,
    EntryWorstPair,
    MarginTarget,
    NoImprove,
    Cooldown,
    NoQuote,
    TotalBudgetCap,
    RoundBudgetCap,
    ReserveForPair,
    HedgeNotRecoverable,
    HedgeMarginInsufficient,
    OpenMarginTooThin,
    EntryEdgeTooThin,
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

#[derive(Debug, Clone)]
pub struct Ledger {
    pub market_slug: String,
    pub series_slug: String,
    pub market_select_mode: String,
    pub up_id: String,
    pub down_id: String,
    pub tick_size: f64,
    pub tick_size_current: f64,
    pub tick_size_source: String,
    pub start_ts: u64,
    pub end_ts: u64,
    pub qty_up: f64,
    pub cost_up: f64,
    pub qty_down: f64,
    pub cost_down: f64,
    pub spent_total_usdc: f64,
    pub spent_round_usdc: f64,
    pub round_idx: u64,
    pub round_state: RoundPhase,
    pub round_leg1: Option<TradeLeg>,
    pub round_qty_target: f64,
    pub round_leg1_entered_ts: u64,
    pub round_leg2_entered_ts: u64,
    pub round_leg2_anchor_price: Option<f64>,
    pub round_leg1_target_qty: f64,
    pub round_leg1_filled_qty: f64,
    pub round_leg2_target_qty: f64,
    pub round_leg2_filled_qty: f64,
    pub last_apply_ts_ms: u64,
    pub last_round_complete_ts: u64,
    pub lock_reopen_used_rounds: u64,
    pub best_bid_up: Option<f64>,
    pub best_ask_up: Option<f64>,
    pub best_bid_down: Option<f64>,
    pub best_ask_down: Option<f64>,
    pub best_bid_size_up: Option<f64>,
    pub best_ask_size_up: Option<f64>,
    pub best_bid_size_down: Option<f64>,
    pub best_ask_size_down: Option<f64>,
    pub bid_consumption_rate_up: f64,
    pub ask_consumption_rate_up: f64,
    pub bid_consumption_rate_down: f64,
    pub ask_consumption_rate_down: f64,
    pub pair_mid_vol_bps: f64,
    pub mid_up_momentum_bps: f64,
    pub mid_down_momentum_bps: f64,
    pub mid_up_discount_bps: f64,
    pub mid_down_discount_bps: f64,
    pub last_decision_ts_ms: u64,
    pub decision_seq: u64,
    pub entry_worst_pair_deny_streak: u64,
    pub entry_worst_pair_streak_started_ts: u64,
    pub entry_fallback_until_ts: u64,
    pub open_orders_active: u64,
    pub lock_state: LockState,
    pub locked_hedgeable: f64,
    pub locked_pair_cost: Option<f64>,
    pub locked_at_ts_ms: u64,
}

impl Ledger {
    pub fn avg_up(&self) -> Option<f64> {
        avg_cost(self.cost_up, self.qty_up)
    }

    pub fn avg_down(&self) -> Option<f64> {
        avg_cost(self.cost_down, self.qty_down)
    }

    pub fn pair_cost(&self) -> Option<f64> {
        match (self.avg_up(), self.avg_down()) {
            (Some(up), Some(down)) => Some(up + down),
            _ => None,
        }
    }

    pub fn hedgeable(&self) -> f64 {
        self.qty_up.min(self.qty_down)
    }

    pub fn unhedged_up(&self) -> f64 {
        (self.qty_up - self.qty_down).max(0.0)
    }

    pub fn unhedged_down(&self) -> f64 {
        (self.qty_down - self.qty_up).max(0.0)
    }

    pub fn update_lock(&mut self, params: &StrategyParams, now_ms: u64) {
        if self.lock_state == LockState::Locked {
            return;
        }
        if self.round_idx < params.lock_min_completed_rounds {
            return;
        }
        let now_ts = now_ms / 1000;
        let time_left_secs = self.time_left_secs(now_ts);
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
                self.lock_state = LockState::Locked;
                self.locked_hedgeable = self.hedgeable();
                self.locked_pair_cost = Some(pair_cost);
                self.locked_at_ts_ms = now_ms;
            }
        }
    }

    pub fn update_round_state(&mut self, params: &StrategyParams, now_ts: u64) {
        if self.round_state == RoundPhase::Done {
            return;
        }
        let time_left_secs = self.time_left_secs(now_ts);
        let eps = 1e-9;

        if self.round_state == RoundPhase::Leg1Accumulating && self.round_leg1_filled_qty > eps {
            let leg2_target = self.round_leg1_filled_qty.max(0.0);
            if self.round_leg2_target_qty + eps < leg2_target {
                self.round_state = RoundPhase::Leg2Balancing;
                self.round_leg2_entered_ts = now_ts;
                self.round_leg2_anchor_price = self.round_leg1.and_then(|leg1| match leg1 {
                    TradeLeg::Up => self.best_bid_down,
                    TradeLeg::Down => self.best_bid_up,
                });
                self.round_leg2_target_qty = leg2_target;
                self.round_leg2_filled_qty = self.round_leg2_filled_qty.min(leg2_target);
                return;
            }
        }

        if self.round_state == RoundPhase::Leg1Accumulating
            && self.round_leg1_entered_ts > 0
            && now_ts
                >= self
                    .round_leg1_entered_ts
                    .saturating_add(params.round_pair_wait_secs)
        {
            let leg2_target = self.round_leg1_filled_qty.max(0.0);
            if leg2_target <= eps {
                // Timeout with no leg1 progress: reset round state and wait for next opportunity.
                self.round_leg1 = None;
                self.round_qty_target = 0.0;
                self.round_leg1_entered_ts = 0;
                self.round_leg2_entered_ts = 0;
                self.round_leg2_anchor_price = None;
                self.round_leg1_target_qty = 0.0;
                self.round_leg1_filled_qty = 0.0;
                self.round_leg2_target_qty = 0.0;
                self.round_leg2_filled_qty = 0.0;
                self.spent_round_usdc = 0.0;
                self.round_state = if time_left_secs <= params.tail_close_secs {
                    RoundPhase::Done
                } else {
                    RoundPhase::Idle
                };
                return;
            }

            self.round_state = RoundPhase::Leg2Balancing;
            self.round_leg2_entered_ts = now_ts;
            self.round_leg2_anchor_price = self.round_leg1.and_then(|leg1| match leg1 {
                TradeLeg::Up => self.best_bid_down,
                TradeLeg::Down => self.best_bid_up,
            });
            self.round_leg2_target_qty = leg2_target;
            self.round_leg2_filled_qty = self.round_leg2_filled_qty.min(leg2_target);
            return;
        }

        if self.round_state == RoundPhase::Leg2Balancing
            && self.spent_round_usdc + eps >= params.round_budget_usdc
        {
            self.round_idx = self.round_idx.saturating_add(1);
            self.last_round_complete_ts = now_ts;

            let abs_net = (self.qty_up - self.qty_down).abs();
            self.spent_round_usdc = 0.0;

            if abs_net > eps {
                // Round budget is consumed but inventory is still imbalanced.
                // Roll to next balancing round and keep reducing net exposure.
                self.round_state = RoundPhase::Leg2Balancing;
                self.round_leg1 = Some(if self.qty_up >= self.qty_down {
                    TradeLeg::Up
                } else {
                    TradeLeg::Down
                });
                self.round_qty_target = abs_net;
                self.round_leg1_entered_ts = 0;
                self.round_leg2_entered_ts = now_ts;
                self.round_leg2_anchor_price = Some(if self.qty_up >= self.qty_down {
                    self.best_bid_down.unwrap_or(0.0)
                } else {
                    self.best_bid_up.unwrap_or(0.0)
                })
                .filter(|v| *v > 0.0);
                self.round_leg1_target_qty = 0.0;
                self.round_leg1_filled_qty = 0.0;
                self.round_leg2_target_qty = abs_net;
                self.round_leg2_filled_qty = 0.0;
                return;
            }

            self.round_leg1 = None;
            self.round_qty_target = 0.0;
            self.round_leg1_entered_ts = 0;
            self.round_leg2_entered_ts = 0;
            self.round_leg2_anchor_price = None;
            self.round_leg1_target_qty = 0.0;
            self.round_leg1_filled_qty = 0.0;
            self.round_leg2_target_qty = 0.0;
            self.round_leg2_filled_qty = 0.0;
            if time_left_secs <= params.tail_close_secs {
                self.round_state = RoundPhase::Done;
            } else {
                self.round_state = RoundPhase::Idle;
            }
            return;
        }

        if self.round_state == RoundPhase::Idle && time_left_secs <= params.tail_close_secs {
            self.round_state = RoundPhase::Done;
            self.round_leg1 = None;
            self.round_qty_target = 0.0;
            self.spent_round_usdc = 0.0;
        }
    }

    pub fn time_left_secs(&self, now_ts: u64) -> u64 {
        if self.end_ts > now_ts {
            self.end_ts - now_ts
        } else {
            0
        }
    }
}

pub fn avg_cost(cost: f64, qty: f64) -> Option<f64> {
    if qty > 0.0 {
        Some(cost / qty)
    } else {
        None
    }
}

pub fn mid_price(bid: Option<f64>, ask: Option<f64>) -> Option<f64> {
    match (bid, ask) {
        (Some(b), Some(a)) => Some((b + a) / 2.0),
        _ => None,
    }
}

pub fn price_for_unhedged(bid: Option<f64>, ask: Option<f64>) -> Option<f64> {
    mid_price(bid, ask).or(ask).or(bid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::params::{NoRiskEntryHedgePriceMode, NoRiskPairLimitMode, StrategyParams};

    fn base_params() -> StrategyParams {
        StrategyParams {
            improve_min: 0.002,
            margin_target: 0.010,
            safety_margin: 0.005,
            total_budget_usdc: 10.0,
            total_budget_source: "test".to_string(),
            max_rounds: 2,
            round_budget_usdc: 5.0,
            round_budget_source: "test".to_string(),
            round_leg1_fraction: 0.45,
            max_unhedged_value: 50.0,
            cap_unhedged_value: Some(50.0),
            max_unhedged_shares: None,
            cooldown_secs: 180,
            tail_freeze_secs: 300,
            tail_close_secs: 180,
            decision_every_ms: 200,
            mode: DryRunMode::Paper,
            apply_kind: ApplyKind::BestAction,
            pair_bonus: 0.0,
            lock_strict_abs_net: true,
            round_budget_strict: false,
            lock_min_completed_rounds: 1,
            lock_min_time_left_secs: 0,
            lock_min_spent_ratio: 0.35,
            lock_force_time_left_secs: 540,
            tail_close_ignore_margin: true,
            vol_entry_min_bps: 0.0,
            vol_entry_lookback_ticks: 10,
            reversal_entry_enabled: false,
            reversal_min_discount_bps: 0.0,
            reversal_min_momentum_bps: 0.0,
            reversal_fast_ema_ticks: 6,
            reversal_slow_ema_ticks: 30,
            entry_turn_confirm_ticks: 4,
            entry_turn_min_rebound_bps: 8.0,
            entry_pair_buffer_bps: 0.0,
            round_pair_wait_secs: 5,
            leg2_rebalance_discount_bps: 0.0,
            force_pair_max_pair_cost: 1.0,
            no_risk_hard_pair_cap: 0.995,
            no_risk_pair_limit_mode: NoRiskPairLimitMode::HardCapOnly,
            no_risk_enforce_tail: true,
            no_risk_entry_hedge_price_mode: NoRiskEntryHedgePriceMode::Ask,
            no_risk_entry_ask_slippage_bps: 5.0,
            no_risk_entry_worst_hedge_bps: 15.0,
            no_risk_entry_pair_headroom_bps: 5.0,
            entry_dynamic_cap_enabled: true,
            entry_dynamic_cap_headroom_bps: 5.0,
            entry_dynamic_cap_min_price: 0.01,
            entry_dynamic_cap_apply_to_net_increase_only: true,
            entry_target_margin_min_ticks: 0.8,
            entry_target_margin_min_bps: 10.0,
            entry_max_passive_ticks_for_net_increase: 1.5,
            no_risk_late_new_round_buffer_secs: 120,
            no_risk_require_completable_round: true,
            no_risk_strict_zero_unmatched: true,
            no_risk_hedge_recoverability_enforce: true,
            no_risk_hedge_recoverability_eps_bps: 2.0,
            hedge_recoverability_margin_enforce: true,
            hedge_recoverability_margin_min_ticks: 1.0,
            hedge_recoverability_margin_min_bps: 8.0,
            hedge_recoverability_margin_apply_to_net_increase_only: true,
            open_order_risk_guard: true,
            open_order_risk_buffer_bps: 20.0,
            open_order_risk_guard_require_paired: true,
            open_order_max_age_secs: 45,
            open_order_unrecoverable_grace_ms: 2500,
            paper_timeout_target_fill_prob: 0.60,
            paper_timeout_progress_extend_min: 0.50,
            paper_timeout_progress_extend_secs: 12,
            paper_timeout_max_extends: 1,
            paper_requote_require_price_move: true,
            paper_requote_stale_ms_hard: 5000,
            paper_requote_retain_fill_draw: false,
            requote_min_fill_prob_uplift: 0.015,
            requote_queue_stickiness_ratio: 4.0,
            requote_stickiness_min_age_secs: 20,
            round_slice_count: 1,
            round_dynamic_slicing_enabled: true,
            round_min_slices: 4,
            round_max_slices: 12,
            round_min_slice_qty: 1.0,
            entry_pair_regression_mode:
                crate::strategy::params::EntryPairRegressionMode::StrictMonotonic,
            entry_pair_regression_soft_band_bps: 40.0,
            entry_edge_min_bps: 0.0,
            entry_fill_prob_min: 0.0,
            open_min_fill_prob: 0.06,
            open_margin_surplus_min: 0.001,
            inventory_skew_alpha_bps: 0.0,
            lock_allow_reopen_before_freeze: false,
            lock_reopen_max_rounds: 0,
            round_min_start_gap_secs: 0,
            opening_no_trade_secs: 0,
            min_apply_interval_ms: 0,
            maker_fill_estimator_enabled: true,
            maker_fill_horizon_secs: 4,
            maker_min_fill_prob: 0.15,
            maker_queue_ahead_mult: 1.5,
            maker_fill_passive_queue_penalty_per_tick: 0.0,
            maker_fill_passive_decay_k: 0.35,
            maker_flow_floor_per_sec: 0.01,
            entry_passive_gap_soft_max_ticks: 3.0,
            entry_max_top_book_share: 0.5,
            entry_max_flow_utilization: 0.8,
            entry_min_timeout_flow_ratio: 0.0,
            entry_fallback_enabled: true,
            entry_fallback_deny_streak: 40,
            entry_fallback_window_secs: 120,
            entry_fallback_duration_secs: 120,
            entry_fallback_hedge_mode: NoRiskEntryHedgePriceMode::BidPlusBps,
            entry_fallback_worst_hedge_bps: 10.0,
            check_summary_skip_plot: true,
        }
    }

    fn base_ledger() -> Ledger {
        Ledger {
            market_slug: "m".to_string(),
            series_slug: "s".to_string(),
            market_select_mode: "fixed".to_string(),
            up_id: "up".to_string(),
            down_id: "down".to_string(),
            tick_size: 0.01,
            tick_size_current: 0.01,
            tick_size_source: "test".to_string(),
            start_ts: 0,
            end_ts: 1_000,
            qty_up: 5.0,
            cost_up: 2.4,
            qty_down: 5.0,
            cost_down: 2.55,
            spent_total_usdc: 4.95,
            spent_round_usdc: 4.95,
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
            best_bid_up: Some(0.48),
            best_ask_up: Some(0.49),
            best_bid_down: Some(0.51),
            best_ask_down: Some(0.52),
            best_bid_size_up: Some(10.0),
            best_ask_size_up: Some(10.0),
            best_bid_size_down: Some(10.0),
            best_ask_size_down: Some(10.0),
            bid_consumption_rate_up: 0.5,
            ask_consumption_rate_up: 0.5,
            bid_consumption_rate_down: 0.5,
            ask_consumption_rate_down: 0.5,
            pair_mid_vol_bps: 0.0,
            mid_up_momentum_bps: 0.0,
            mid_down_momentum_bps: 0.0,
            mid_up_discount_bps: 0.0,
            mid_down_discount_bps: 0.0,
            lock_state: LockState::Unlocked,
            locked_hedgeable: 0.0,
            locked_pair_cost: None,
            locked_at_ts_ms: 0,
            last_decision_ts_ms: 0,
            decision_seq: 0,
            entry_worst_pair_deny_streak: 0,
            entry_worst_pair_streak_started_ts: 0,
            entry_fallback_until_ts: 0,
            open_orders_active: 0,
        }
    }

    #[test]
    fn update_lock_respects_min_completed_rounds() {
        let mut params = base_params();
        let mut ledger = base_ledger();
        params.lock_min_completed_rounds = 2;
        ledger.round_idx = 1;
        ledger.update_lock(&params, 900_000);
        assert_eq!(ledger.lock_state, LockState::Unlocked);

        ledger.round_idx = 2;
        ledger.update_lock(&params, 900_000);
        assert_eq!(ledger.lock_state, LockState::Locked);
    }

    #[test]
    fn update_lock_respects_min_time_left() {
        let mut params = base_params();
        let mut ledger = base_ledger();
        params.lock_min_time_left_secs = 100;
        ledger.round_idx = 1;

        ledger.update_lock(&params, 800_000); // time_left=200
        assert_eq!(ledger.lock_state, LockState::Unlocked);

        ledger.update_lock(&params, 920_000); // time_left=80
        assert_eq!(ledger.lock_state, LockState::Locked);
    }

    #[test]
    fn update_lock_requires_pair_cost_condition() {
        let params = base_params();
        let mut ledger = base_ledger();
        ledger.round_idx = 1;
        ledger.cost_down = 2.7; // pair_cost > 1 - safety_margin
        ledger.update_lock(&params, 900_000);
        assert_eq!(ledger.lock_state, LockState::Unlocked);
    }

    #[test]
    fn update_round_state_rolls_leg2_when_budget_hit_and_unbalanced() {
        let params = base_params();
        let mut ledger = base_ledger();
        ledger.end_ts = 2_000;
        ledger.round_state = RoundPhase::Leg2Balancing;
        ledger.round_idx = 0;
        ledger.spent_round_usdc = params.round_budget_usdc;
        ledger.qty_up = 8.0;
        ledger.qty_down = 6.0;
        ledger.round_leg1 = Some(TradeLeg::Down);
        ledger.round_leg2_target_qty = 6.0;
        ledger.round_leg2_filled_qty = 4.0;

        ledger.update_round_state(&params, 1_000);

        assert_eq!(ledger.round_idx, 1);
        assert_eq!(ledger.round_state, RoundPhase::Leg2Balancing);
        assert_eq!(ledger.round_leg1, Some(TradeLeg::Up));
        assert_eq!(ledger.round_leg2_target_qty, 2.0);
        assert_eq!(ledger.round_leg2_filled_qty, 0.0);
        assert_eq!(ledger.spent_round_usdc, 0.0);
    }

    #[test]
    fn update_round_state_moves_to_leg2_after_leg1_wait_timeout() {
        let mut params = base_params();
        params.round_pair_wait_secs = 5;
        let mut ledger = base_ledger();
        ledger.end_ts = 2_000;
        ledger.round_state = RoundPhase::Leg1Accumulating;
        ledger.round_leg1 = Some(TradeLeg::Down);
        ledger.round_leg1_entered_ts = 100;
        ledger.round_leg1_target_qty = 20.0;
        ledger.round_leg1_filled_qty = 14.0;
        ledger.round_leg2_target_qty = 20.0;
        ledger.round_leg2_filled_qty = 0.0;

        ledger.update_round_state(&params, 106);

        assert_eq!(ledger.round_state, RoundPhase::Leg2Balancing);
        assert_eq!(ledger.round_leg2_entered_ts, 106);
        assert_eq!(ledger.round_leg2_target_qty, 14.0);
    }

    #[test]
    fn update_round_state_timeout_with_no_leg1_progress_resets_to_idle() {
        let mut params = base_params();
        params.round_pair_wait_secs = 5;
        let mut ledger = base_ledger();
        ledger.end_ts = 2_000;
        ledger.round_state = RoundPhase::Leg1Accumulating;
        ledger.round_leg1 = Some(TradeLeg::Up);
        ledger.round_leg1_entered_ts = 100;
        ledger.round_leg1_target_qty = 10.0;
        ledger.round_leg1_filled_qty = 0.0;
        ledger.spent_round_usdc = 1.2;

        ledger.update_round_state(&params, 106);

        assert_eq!(ledger.round_state, RoundPhase::Idle);
        assert_eq!(ledger.round_leg1, None);
        assert_eq!(ledger.round_leg1_target_qty, 0.0);
        assert_eq!(ledger.round_leg2_target_qty, 0.0);
        assert_eq!(ledger.spent_round_usdc, 0.0);
    }
}

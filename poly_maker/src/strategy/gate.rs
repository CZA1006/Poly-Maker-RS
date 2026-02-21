use super::params::StrategyParams;
use super::planner::RoundPlan;
use super::simulate::{CandidateAction, SimResult};
use super::state::{DenyReason, Ledger, LockState, RoundPhase, TradeKind, TradeSide};

const MARGIN_TARGET_EPS: f64 = 1e-9;

fn effective_pair_limit(params: &StrategyParams, force_pair_after_wait: bool) -> f64 {
    params.effective_pair_limit(force_pair_after_wait)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateContext {
    CandidateSelection,
    RestingFill,
}

#[derive(Debug, Clone)]
pub struct GateResult {
    pub allow: bool,
    pub deny_reason: Option<DenyReason>,
    pub can_start_new_round: bool,
    pub budget_remaining_round: f64,
    pub budget_remaining_total: f64,
    pub reserve_needed_usdc: Option<f64>,
}

pub fn evaluate_action_gate(
    ledger: &Ledger,
    action: &CandidateAction,
    sim: &SimResult,
    params: &StrategyParams,
    now_ts: u64,
    round_plan: &RoundPlan,
) -> GateResult {
    evaluate_action_gate_with_context(
        ledger,
        action,
        sim,
        params,
        now_ts,
        round_plan,
        GateContext::CandidateSelection,
    )
}

pub fn evaluate_action_gate_with_context(
    ledger: &Ledger,
    action: &CandidateAction,
    sim: &SimResult,
    params: &StrategyParams,
    now_ts: u64,
    round_plan: &RoundPlan,
    context: GateContext,
) -> GateResult {
    let candidate_selection = matches!(context, GateContext::CandidateSelection);
    let budget_remaining_round = round_plan.budget_remaining_round;
    let budget_remaining_total = round_plan.budget_remaining_total;
    let mut reserve_needed_usdc = None;
    if candidate_selection && round_plan.phase == RoundPhase::Idle {
        if let Some(planned_leg1) = round_plan.planned_leg1 {
            if action.leg == planned_leg1 {
                let other_price = match planned_leg1 {
                    super::state::TradeLeg::Up => ledger.best_ask_down.or(ledger.best_bid_down),
                    super::state::TradeLeg::Down => ledger.best_ask_up.or(ledger.best_bid_up),
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
        let estimator_no_fill = action.kind == TradeKind::Maker
            && params.maker_fill_estimator_enabled
            && sim.fill_price.is_some()
            && sim.maker_fill_prob.is_some();
        if estimator_no_fill {
            return mk_gate(false, Some(DenyReason::LowMakerFillProb));
        }
        return mk_gate(false, Some(DenyReason::NoQuote));
    }
    let spent_total_after = ledger.spent_total_usdc + sim.spent_delta_usdc;
    let spent_round_after = ledger.spent_round_usdc + sim.spent_delta_usdc;
    let current_net = ledger.qty_up - ledger.qty_down;
    let sim_net = sim.new_qty_up - sim.new_qty_down;
    let abs_net = current_net.abs();
    let abs_sim = sim_net.abs();
    let eps = 1e-9;
    let would_increase_abs_net_leg = abs_sim > abs_net + eps;
    let would_reduce_abs_net_leg = abs_sim + eps < abs_net;
    let is_new_opening_leg =
        round_plan.phase == RoundPhase::Idle && abs_net <= eps && abs_sim > abs_net + eps;
    let in_leg1_phase = ledger.round_state == RoundPhase::Leg1Accumulating;
    let in_leg2_phase = ledger.round_state == RoundPhase::Leg2Balancing;
    let leg1_wait_expired = in_leg1_phase
        && ledger.round_leg1_entered_ts > 0
        && now_ts
            >= ledger
                .round_leg1_entered_ts
                .saturating_add(params.round_pair_wait_secs);
    let leg2_wait_expired = in_leg2_phase
        && ledger.round_leg2_entered_ts > 0
        && now_ts
            >= ledger
                .round_leg2_entered_ts
                .saturating_add(params.round_pair_wait_secs);
    let force_pair_after_wait = (leg1_wait_expired || leg2_wait_expired)
        && ledger
            .round_leg1
            .map(|leg1| action.leg != leg1)
            .unwrap_or(false);
    let time_left_secs = ledger.time_left_secs(now_ts);
    let is_reducing_risk = abs_sim + eps < abs_net;
    if params.no_risk_hedge_recoverability_enforce
        && would_increase_abs_net_leg
        && matches!(sim.hedge_recoverable_now, Some(false))
    {
        return mk_gate(false, Some(DenyReason::HedgeNotRecoverable));
    }
    let hedge_margin_rule_applies = action.side == super::state::TradeSide::Buy
        && if params.hedge_recoverability_margin_apply_to_net_increase_only {
            would_increase_abs_net_leg
        } else {
            true
        };
    if params.hedge_recoverability_margin_enforce
        && hedge_margin_rule_applies
        && sim.hedge_margin_ok == Some(false)
    {
        return mk_gate(false, Some(DenyReason::HedgeMarginInsufficient));
    }
    let effective_open_margin_surplus_min =
        params.effective_open_margin_surplus_min(ledger.spent_total_usdc, time_left_secs);
    if candidate_selection
        && action.side == TradeSide::Buy
        && would_increase_abs_net_leg
        && effective_open_margin_surplus_min > 0.0
    {
        // Allow a small tick-scaled tolerance so quotes sitting right on the boundary
        // (e.g. 0.000995 vs 0.0010) are not rejected due to precision/rounding noise.
        let surplus_eps = (ledger.tick_size_current.max(1e-9) * 0.001).max(5e-6);
        let margin_surplus = match (sim.hedge_margin_to_opp_ask, sim.hedge_margin_required) {
            (Some(actual), Some(required)) => Some(actual - required),
            _ => None,
        };
        if margin_surplus
            .map(|v| v + surplus_eps < effective_open_margin_surplus_min)
            .unwrap_or(false)
        {
            return mk_gate(false, Some(DenyReason::OpenMarginTooThin));
        }
    }
    if spent_total_after > params.total_budget_usdc + 1e-9 {
        return mk_gate(false, Some(DenyReason::TotalBudgetCap));
    }
    if spent_round_after > params.round_budget_usdc + 1e-9
        && (params.round_budget_strict || !is_reducing_risk)
    {
        return mk_gate(false, Some(DenyReason::RoundBudgetCap));
    }
    if candidate_selection
        && params.no_risk_require_completable_round
        && is_new_opening_leg
        && !round_plan.entry_worst_pair_ok
    {
        return mk_gate(false, Some(DenyReason::EntryWorstPair));
    }
    if candidate_selection
        && is_new_opening_leg
        && params.entry_edge_min_bps > 0.0
        && round_plan.planned_leg1 == Some(action.leg)
        && round_plan
            .entry_edge_bps
            .map(|v| v + 1e-9 < params.entry_edge_min_bps)
            .unwrap_or(true)
    {
        return mk_gate(false, Some(DenyReason::EntryEdgeTooThin));
    }

    if candidate_selection {
        if let Some(planned_leg1) = round_plan.planned_leg1 {
            if round_plan.phase == RoundPhase::Idle && action.leg != planned_leg1 {
                return mk_gate(false, Some(DenyReason::ReserveForPair));
            }
            if round_plan.phase == RoundPhase::Leg1Accumulating
                && leg1_wait_expired
                && action.leg == planned_leg1
            {
                // After leg1 wait timeout, stop extending the same side and force transition
                // to balancing actions.
                return mk_gate(false, Some(DenyReason::ReserveForPair));
            }
            if round_plan.phase == RoundPhase::Leg1Accumulating
                && action.leg != planned_leg1
                && !force_pair_after_wait
            {
                return mk_gate(false, Some(DenyReason::ReserveForPair));
            }
            if round_plan.phase == RoundPhase::Leg1Accumulating && action.leg == planned_leg1 {
                let projected_pair_cost = match planned_leg1 {
                    super::state::TradeLeg::Up => sim
                        .new_avg_up
                        .zip(ledger.best_ask_down)
                        .map(|(avg_up, hedge_px)| avg_up + hedge_px),
                    super::state::TradeLeg::Down => sim
                        .new_avg_down
                        .zip(ledger.best_ask_up)
                        .map(|(avg_down, hedge_px)| avg_down + hedge_px),
                };
                let pair_limit = effective_pair_limit(params, false);
                match projected_pair_cost {
                    Some(v) if v > pair_limit + MARGIN_TARGET_EPS => {
                        return mk_gate(false, Some(DenyReason::MarginTarget));
                    }
                    Some(_) => {}
                    None => return mk_gate(false, Some(DenyReason::ReserveForPair)),
                }
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
    }

    let current_pair_cost = ledger.pair_cost();
    let sim_pair_cost = sim.new_pair_cost;
    let in_tail_close = time_left_secs <= params.tail_close_secs;
    let effective_max_rounds = params
        .effective_max_rounds(ledger.spent_total_usdc, time_left_secs)
        .max(ledger.round_idx.saturating_add(1));
    if candidate_selection && params.opening_no_trade_secs > 0 && now_ts >= ledger.start_ts {
        let elapsed = now_ts - ledger.start_ts;
        if elapsed < params.opening_no_trade_secs && would_increase_abs_net_leg {
            return mk_gate(false, Some(DenyReason::Cooldown));
        }
    }

    if in_tail_close {
        if abs_net <= eps {
            return mk_gate(false, Some(DenyReason::TailClose));
        }
        if !would_reduce_abs_net_leg {
            return mk_gate(false, Some(DenyReason::TailClose));
        }
    } else if time_left_secs <= params.tail_close_secs && would_increase_abs_net_leg {
        return mk_gate(false, Some(DenyReason::TailFreeze));
    }
    if candidate_selection
        && params.no_risk_require_completable_round
        && round_plan.late_new_round_blocked
        && would_increase_abs_net_leg
    {
        return mk_gate(false, Some(DenyReason::TailFreeze));
    }

    let in_leg2_target_action = round_plan.phase == RoundPhase::Leg2Balancing
        && ledger
            .round_leg1
            .map(|leg1| action.leg != leg1)
            .unwrap_or(false)
        && action.side == super::state::TradeSide::Buy;
    if candidate_selection
        && params.leg2_rebalance_discount_bps > 0.0
        && in_leg2_target_action
        && !force_pair_after_wait
        && !in_tail_close
    {
        if let (Some(anchor_px), Some(fill_px)) = (ledger.round_leg2_anchor_price, sim.fill_price) {
            if anchor_px > 0.0 {
                let required_px =
                    (anchor_px * (1.0 - params.leg2_rebalance_discount_bps / 10_000.0)).max(0.0);
                if fill_px > required_px + MARGIN_TARGET_EPS {
                    return mk_gate(false, Some(DenyReason::Cooldown));
                }
            }
        }
    }

    let now_ms = now_ts.saturating_mul(1000);
    if candidate_selection
        && params.min_apply_interval_ms > 0
        && ledger.last_apply_ts_ms > 0
        && now_ms
            < ledger
                .last_apply_ts_ms
                .saturating_add(params.min_apply_interval_ms)
        && !(in_tail_close && would_reduce_abs_net_leg)
    {
        return mk_gate(false, Some(DenyReason::Cooldown));
    }

    let lock_reopen_ok = candidate_selection
        && ledger.lock_state == LockState::Locked
        && params.lock_allow_reopen_before_freeze
        && time_left_secs > params.tail_close_secs
        && abs_net <= eps
        && round_plan.phase == RoundPhase::Idle
        && round_plan.can_start_new_round
        && round_plan.planned_leg1 == Some(action.leg)
        && ledger.lock_reopen_used_rounds
            < params.effective_lock_reopen_max_rounds(ledger.spent_total_usdc, time_left_secs);

    if candidate_selection && ledger.lock_state == LockState::Locked {
        if params.lock_strict_abs_net && would_increase_abs_net_leg && !lock_reopen_ok {
            return mk_gate(false, Some(DenyReason::LockedStrictAbsNet));
        }
        if time_left_secs <= params.tail_close_secs && would_increase_abs_net_leg {
            return mk_gate(false, Some(DenyReason::LockedTailFreeze));
        }
        if ledger.round_state == RoundPhase::Leg1Accumulating {
            if let Some(leg1) = ledger.round_leg1 {
                if action.leg == leg1 {
                    return mk_gate(false, Some(DenyReason::LockedWaitingPair));
                }
            }
        }
        if time_left_secs <= params.tail_close_secs
            && ledger.round_idx >= effective_max_rounds
            && would_increase_abs_net_leg
        {
            return mk_gate(false, Some(DenyReason::LockedMaxRounds));
        }
        if !round_plan.can_start_new_round && would_increase_abs_net_leg && !lock_reopen_ok {
            return mk_gate(false, Some(DenyReason::LockedPolicyHold));
        }
    }

    if candidate_selection && time_left_secs < params.cooldown_secs && would_increase_abs_net_leg {
        return mk_gate(false, Some(DenyReason::Cooldown));
    }

    let bypass_low_fill_prob = (in_tail_close && would_reduce_abs_net_leg) || force_pair_after_wait;
    let low_fill_gate_applies = candidate_selection
        && round_plan.can_start_new_round
        && round_plan.planned_leg1 == Some(action.leg);
    if candidate_selection
        && low_fill_gate_applies
        && !bypass_low_fill_prob
        && action.kind == TradeKind::Maker
        && params.maker_fill_estimator_enabled
    {
        if let Some(fill_prob) = sim.maker_fill_prob {
            let has_observed_flow_signal = sim
                .maker_consumption_rate
                .map(|rate| rate > params.maker_flow_floor_per_sec + 1e-9)
                .unwrap_or(false);
            let open_quality_floor = if action.side == TradeSide::Buy && would_increase_abs_net_leg
            {
                params.open_min_fill_prob.max(0.0)
            } else {
                0.0
            };
            let fill_prob_threshold = params.entry_fill_prob_min.max(open_quality_floor);
            if has_observed_flow_signal && fill_prob + 1e-9 < fill_prob_threshold {
                return mk_gate(false, Some(DenyReason::LowMakerFillProb));
            }
        }
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

    let hard_pair_cap = params.no_risk_hard_pair_cap.min(1.0);
    if let Some(sim_pair_cost) = sim_pair_cost {
        if sim_pair_cost > hard_pair_cap + MARGIN_TARGET_EPS {
            return mk_gate(false, Some(DenyReason::MarginTarget));
        }
    }

    let bypass_pair_quality_checks = candidate_selection
        && params.tail_close_ignore_margin
        && !params.no_risk_enforce_tail
        && in_tail_close
        && would_reduce_abs_net_leg;
    if candidate_selection && !bypass_pair_quality_checks {
        let force_pair_margin_relax = force_pair_after_wait && would_reduce_abs_net_leg;
        if let Some(sim_pair_cost) = sim_pair_cost {
            let margin_limit = effective_pair_limit(params, force_pair_margin_relax);
            if sim_pair_cost > margin_limit + MARGIN_TARGET_EPS {
                return mk_gate(false, Some(DenyReason::MarginTarget));
            }
        }
        if let (Some(current_pair_cost), Some(sim_pair_cost)) = (current_pair_cost, sim_pair_cost) {
            let skip_no_improve_for_cap_edge = candidate_selection
                && round_plan.phase == RoundPhase::Idle
                && round_plan.can_start_new_round
                && matches!(
                    params.entry_pair_regression_mode,
                    super::params::EntryPairRegressionMode::CapEdge
                )
                && would_increase_abs_net_leg;
            if sim_pair_cost > current_pair_cost - params.improve_min
                && !sim.improves_hedge
                && !skip_no_improve_for_cap_edge
                && !(force_pair_after_wait && would_reduce_abs_net_leg)
            {
                return mk_gate(false, Some(DenyReason::NoImprove));
            }
        }
    }

    mk_gate(true, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::params::{NoRiskEntryHedgePriceMode, NoRiskPairLimitMode};
    use crate::strategy::planner::RoundPlan;
    use crate::strategy::simulate::{CandidateAction, SimResult};
    use crate::strategy::state::{
        ApplyKind, DryRunMode, LockState, RoundPhase, TradeKind, TradeLeg, TradeSide,
    };

    fn base_params() -> StrategyParams {
        StrategyParams {
            improve_min: 0.002,
            margin_target: 0.01,
            safety_margin: 0.005,
            total_budget_usdc: 100.0,
            total_budget_source: "test".to_string(),
            max_rounds: 2,
            round_budget_usdc: 50.0,
            round_budget_source: "test".to_string(),
            round_leg1_fraction: 0.45,
            max_unhedged_value: 100.0,
            cap_unhedged_value: Some(100.0),
            max_unhedged_shares: None,
            cooldown_secs: 180,
            tail_freeze_secs: 300,
            tail_close_secs: 180,
            decision_every_ms: 200,
            mode: DryRunMode::Paper,
            apply_kind: ApplyKind::BestAction,
            pair_bonus: 0.001,
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
            maker_fill_horizon_secs: 8,
            maker_min_fill_prob: 0.05,
            maker_queue_ahead_mult: 1.0,
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
            qty_up: 10.0,
            cost_up: 4.0,
            qty_down: 5.0,
            cost_down: 2.0,
            spent_total_usdc: 6.0,
            spent_round_usdc: 6.0,
            round_idx: 0,
            round_state: RoundPhase::Idle,
            round_leg1: None,
            round_qty_target: 5.0,
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
            best_bid_up: Some(0.40),
            best_ask_up: Some(0.41),
            best_bid_down: Some(0.60),
            best_ask_down: Some(0.61),
            best_bid_size_up: Some(20.0),
            best_ask_size_up: Some(18.0),
            best_bid_size_down: Some(22.0),
            best_ask_size_down: Some(19.0),
            bid_consumption_rate_up: 1.2,
            ask_consumption_rate_up: 0.9,
            bid_consumption_rate_down: 1.1,
            ask_consumption_rate_down: 0.8,
            pair_mid_vol_bps: 0.0,
            mid_up_momentum_bps: 0.0,
            mid_down_momentum_bps: 0.0,
            mid_up_discount_bps: 0.0,
            mid_down_discount_bps: 0.0,
            last_decision_ts_ms: 0,
            decision_seq: 1,
            entry_worst_pair_deny_streak: 0,
            entry_worst_pair_streak_started_ts: 0,
            entry_fallback_until_ts: 0,
            open_orders_active: 0,
            lock_state: LockState::Unlocked,
            locked_hedgeable: 0.0,
            locked_pair_cost: None,
            locked_at_ts_ms: 0,
        }
    }

    fn base_round_plan() -> RoundPlan {
        RoundPlan {
            phase: RoundPhase::Idle,
            planned_leg1: None,
            qty_target: None,
            balance_leg: Some(TradeLeg::Down),
            balance_qty: Some(5.0),
            can_start_new_round: false,
            budget_remaining_round: 44.0,
            budget_remaining_total: 94.0,
            reserve_needed_usdc: None,
            vol_entry_bps: 0.0,
            vol_entry_ok: true,
            reversal_up_ok: true,
            reversal_down_ok: true,
            turn_up_ok: true,
            turn_down_ok: true,
            first_leg_turning_score: None,
            entry_worst_pair_cost: None,
            entry_worst_pair_ok: true,
            entry_timeout_flow_ratio: None,
            entry_timeout_flow_ok: true,
            entry_fillability_ok: true,
            entry_edge_bps: None,
            entry_regime_score: None,
            entry_depth_cap_qty: None,
            entry_flow_cap_qty: None,
            slice_count_planned: None,
            slice_qty_current: None,
            entry_final_qty_slice: None,
            entry_fallback_active: false,
            entry_fallback_armed: false,
            entry_fallback_trigger_reason: None,
            entry_fallback_blocked_by_recoverability: false,
            new_round_cutoff_secs: 305,
            late_new_round_blocked: false,
            pair_quality_ok: false,
            pair_regression_ok: false,
            can_open_round_base_ok: false,
            can_start_block_reason: None,
        }
    }

    fn base_action() -> CandidateAction {
        CandidateAction {
            name: "BUY_DOWN_MAKER",
            leg: TradeLeg::Down,
            side: TradeSide::Buy,
            kind: TradeKind::Maker,
            qty: 1.0,
        }
    }

    fn base_sim() -> SimResult {
        SimResult {
            ok: true,
            fill_qty: 1.0,
            fill_price: Some(0.60),
            maker_fill_prob: Some(0.5),
            maker_queue_ahead: Some(1.0),
            maker_expected_consumed: Some(2.0),
            maker_consumption_rate: Some(0.25),
            maker_horizon_secs: Some(8.0),
            fee_estimate: 0.0,
            spent_delta_usdc: 0.60,
            new_qty_up: 10.0,
            new_cost_up: 4.0,
            new_qty_down: 6.0,
            new_cost_down: 2.6,
            new_avg_up: Some(0.4),
            new_avg_down: Some(0.43333333333333335),
            new_pair_cost: Some(0.999),
            new_hedgeable: 6.0,
            new_unhedged_up: 4.0,
            new_unhedged_down: 0.0,
            new_unhedged_value_up: 1.64,
            new_unhedged_value_down: 0.0,
            hedge_recoverable_now: None,
            required_opp_avg_price_cap: None,
            current_opp_best_ask: None,
            required_hedge_qty: None,
            hedge_margin_to_opp_ask: None,
            hedge_margin_required: None,
            hedge_margin_ok: None,
            entry_quote_base_postonly_price: None,
            entry_quote_dynamic_cap_price: None,
            entry_quote_final_price: None,
            entry_quote_cap_active: None,
            entry_quote_cap_bind: None,
            passive_gap_abs: None,
            passive_gap_ticks: None,
            improves_hedge: true,
        }
    }

    #[test]
    fn tail_close_can_bypass_margin_when_reducing_abs_net() {
        let mut params = base_params();
        params.no_risk_enforce_tail = false;
        params.no_risk_hard_pair_cap = 1.0;
        let ledger = base_ledger();
        let action = base_action();
        let sim = base_sim();
        let round_plan = base_round_plan();

        let result = evaluate_action_gate(&ledger, &action, &sim, &params, 850, &round_plan);

        assert!(result.allow);
        assert!(result.deny_reason.is_none());
    }

    #[test]
    fn tail_close_cannot_bypass_hard_pair_cap_when_enforced() {
        let params = base_params();
        let ledger = base_ledger();
        let action = base_action();
        let mut sim = base_sim();
        sim.new_pair_cost = Some(0.999);
        let round_plan = base_round_plan();

        let result = evaluate_action_gate(&ledger, &action, &sim, &params, 850, &round_plan);

        assert!(!result.allow);
        assert_eq!(result.deny_reason, Some(DenyReason::MarginTarget));
    }

    #[test]
    fn locked_strict_abs_net_denies_risk_increase() {
        let params = base_params();
        let mut ledger = base_ledger();
        ledger.lock_state = LockState::Locked;
        ledger.qty_down = 10.0;
        let action = CandidateAction {
            leg: TradeLeg::Up,
            name: "BUY_UP_MAKER",
            ..base_action()
        };
        let mut sim = base_sim();
        sim.new_qty_up = 11.0;
        sim.new_qty_down = 10.0;
        sim.new_unhedged_up = 1.0;
        sim.new_unhedged_value_up = 0.41;
        sim.new_pair_cost = Some(0.95);
        let round_plan = base_round_plan();

        let result = evaluate_action_gate(&ledger, &action, &sim, &params, 400, &round_plan);

        assert!(!result.allow);
        assert_eq!(result.deny_reason, Some(DenyReason::LockedStrictAbsNet));
    }

    #[test]
    fn tail_close_denies_non_reducing_abs_net() {
        let params = base_params();
        let ledger = base_ledger();
        let action = base_action();
        let mut sim = base_sim();
        sim.new_qty_up = 10.0;
        sim.new_qty_down = 5.0;
        sim.new_unhedged_up = 5.0;
        sim.improves_hedge = false;
        let round_plan = base_round_plan();

        let result = evaluate_action_gate(&ledger, &action, &sim, &params, 850, &round_plan);

        assert!(!result.allow);
        assert_eq!(result.deny_reason, Some(DenyReason::TailClose));
    }

    #[test]
    fn leg1_wait_timeout_denies_pair_cost_above_break_even() {
        let mut params = base_params();
        params.round_pair_wait_secs = 5;
        params.margin_target = 0.005;

        let mut ledger = base_ledger();
        ledger.round_state = RoundPhase::Leg1Accumulating;
        ledger.round_leg1 = Some(TradeLeg::Up);
        ledger.round_leg1_entered_ts = 100;
        ledger.qty_up = 8.0;
        ledger.qty_down = 0.0;

        let action = CandidateAction {
            name: "BUY_DOWN_MAKER",
            leg: TradeLeg::Down,
            side: TradeSide::Buy,
            kind: TradeKind::Maker,
            qty: 8.0,
        };
        let mut sim = base_sim();
        sim.new_pair_cost = Some(1.02);
        sim.improves_hedge = false;
        sim.maker_fill_prob = Some(0.01);
        sim.maker_consumption_rate = Some(1.0);
        let round_plan = RoundPlan {
            phase: RoundPhase::Leg1Accumulating,
            planned_leg1: Some(TradeLeg::Up),
            qty_target: Some(8.0),
            balance_leg: None,
            balance_qty: None,
            can_start_new_round: false,
            budget_remaining_round: 44.0,
            budget_remaining_total: 94.0,
            reserve_needed_usdc: None,
            vol_entry_bps: 0.0,
            vol_entry_ok: true,
            reversal_up_ok: true,
            reversal_down_ok: true,
            turn_up_ok: true,
            turn_down_ok: true,
            first_leg_turning_score: None,
            entry_worst_pair_cost: None,
            entry_worst_pair_ok: true,
            entry_timeout_flow_ratio: None,
            entry_timeout_flow_ok: true,
            entry_fillability_ok: true,
            entry_edge_bps: None,
            entry_regime_score: None,
            entry_depth_cap_qty: None,
            entry_flow_cap_qty: None,
            slice_count_planned: None,
            slice_qty_current: None,
            entry_final_qty_slice: None,
            entry_fallback_active: false,
            entry_fallback_armed: false,
            entry_fallback_trigger_reason: None,
            entry_fallback_blocked_by_recoverability: false,
            new_round_cutoff_secs: 305,
            late_new_round_blocked: false,
            pair_quality_ok: false,
            pair_regression_ok: false,
            can_open_round_base_ok: false,
            can_start_block_reason: None,
        };

        let result = evaluate_action_gate(&ledger, &action, &sim, &params, 110, &round_plan);

        assert!(!result.allow);
        assert_eq!(result.deny_reason, Some(DenyReason::MarginTarget));
    }

    #[test]
    fn leg2_wait_timeout_allows_pairing_under_hard_cap_without_improve() {
        let mut params = base_params();
        params.round_pair_wait_secs = 5;
        params.margin_target = 0.005;
        params.force_pair_max_pair_cost = 1.0;

        let mut ledger = base_ledger();
        ledger.round_state = RoundPhase::Leg2Balancing;
        ledger.round_leg1 = Some(TradeLeg::Down);
        ledger.round_leg2_entered_ts = 100;
        ledger.qty_up = 50.0;
        ledger.qty_down = 54.0;

        let action = CandidateAction {
            name: "BUY_UP_MAKER",
            leg: TradeLeg::Up,
            side: TradeSide::Buy,
            kind: TradeKind::Maker,
            qty: 4.0,
        };
        let mut sim = base_sim();
        sim.new_pair_cost = Some(0.994);
        sim.improves_hedge = false;
        sim.maker_fill_prob = Some(0.01);
        sim.maker_consumption_rate = Some(1.0);
        sim.new_qty_up = 54.0;
        sim.new_qty_down = 54.0;
        sim.new_unhedged_up = 0.0;
        sim.new_unhedged_down = 0.0;
        let round_plan = RoundPlan {
            phase: RoundPhase::Leg2Balancing,
            planned_leg1: None,
            qty_target: None,
            balance_leg: Some(TradeLeg::Up),
            balance_qty: Some(4.0),
            can_start_new_round: false,
            budget_remaining_round: 44.0,
            budget_remaining_total: 94.0,
            reserve_needed_usdc: None,
            vol_entry_bps: 0.0,
            vol_entry_ok: true,
            reversal_up_ok: true,
            reversal_down_ok: true,
            turn_up_ok: true,
            turn_down_ok: true,
            first_leg_turning_score: None,
            entry_worst_pair_cost: None,
            entry_worst_pair_ok: true,
            entry_timeout_flow_ratio: None,
            entry_timeout_flow_ok: true,
            entry_fillability_ok: true,
            entry_edge_bps: None,
            entry_regime_score: None,
            entry_depth_cap_qty: None,
            entry_flow_cap_qty: None,
            slice_count_planned: None,
            slice_qty_current: None,
            entry_final_qty_slice: None,
            entry_fallback_active: false,
            entry_fallback_armed: false,
            entry_fallback_trigger_reason: None,
            entry_fallback_blocked_by_recoverability: false,
            new_round_cutoff_secs: 305,
            late_new_round_blocked: false,
            pair_quality_ok: false,
            pair_regression_ok: false,
            can_open_round_base_ok: false,
            can_start_block_reason: None,
        };

        let result = evaluate_action_gate(&ledger, &action, &sim, &params, 110, &round_plan);

        assert!(result.allow);
    }

    #[test]
    fn leg2_wait_timeout_respects_force_pair_max_pair_cost() {
        let mut params = base_params();
        params.round_pair_wait_secs = 5;
        params.margin_target = 0.005;
        params.force_pair_max_pair_cost = 0.995;

        let mut ledger = base_ledger();
        ledger.round_state = RoundPhase::Leg2Balancing;
        ledger.round_leg1 = Some(TradeLeg::Down);
        ledger.round_leg2_entered_ts = 100;
        ledger.qty_up = 50.0;
        ledger.qty_down = 54.0;

        let action = CandidateAction {
            name: "BUY_UP_MAKER",
            leg: TradeLeg::Up,
            side: TradeSide::Buy,
            kind: TradeKind::Maker,
            qty: 4.0,
        };
        let mut sim = base_sim();
        sim.new_pair_cost = Some(0.998);
        sim.improves_hedge = true;
        sim.new_qty_up = 54.0;
        sim.new_qty_down = 54.0;
        sim.new_unhedged_up = 0.0;
        sim.new_unhedged_down = 0.0;
        let round_plan = RoundPlan {
            phase: RoundPhase::Leg2Balancing,
            planned_leg1: None,
            qty_target: None,
            balance_leg: Some(TradeLeg::Up),
            balance_qty: Some(4.0),
            can_start_new_round: false,
            budget_remaining_round: 44.0,
            budget_remaining_total: 94.0,
            reserve_needed_usdc: None,
            vol_entry_bps: 0.0,
            vol_entry_ok: true,
            reversal_up_ok: true,
            reversal_down_ok: true,
            turn_up_ok: true,
            turn_down_ok: true,
            first_leg_turning_score: None,
            entry_worst_pair_cost: None,
            entry_worst_pair_ok: true,
            entry_timeout_flow_ratio: None,
            entry_timeout_flow_ok: true,
            entry_fillability_ok: true,
            entry_edge_bps: None,
            entry_regime_score: None,
            entry_depth_cap_qty: None,
            entry_flow_cap_qty: None,
            slice_count_planned: None,
            slice_qty_current: None,
            entry_final_qty_slice: None,
            entry_fallback_active: false,
            entry_fallback_armed: false,
            entry_fallback_trigger_reason: None,
            entry_fallback_blocked_by_recoverability: false,
            new_round_cutoff_secs: 305,
            late_new_round_blocked: false,
            pair_quality_ok: false,
            pair_regression_ok: false,
            can_open_round_base_ok: false,
            can_start_block_reason: None,
        };

        let result = evaluate_action_gate(&ledger, &action, &sim, &params, 110, &round_plan);

        assert!(!result.allow);
        assert_eq!(result.deny_reason, Some(DenyReason::MarginTarget));
    }

    #[test]
    fn leg2_rebalance_discount_blocks_early_high_price_fill() {
        let mut params = base_params();
        params.leg2_rebalance_discount_bps = 300.0; // require >=3% discount from leg2 anchor

        let mut ledger = base_ledger();
        ledger.round_state = RoundPhase::Leg2Balancing;
        ledger.round_leg1 = Some(TradeLeg::Up);
        ledger.round_leg2_anchor_price = Some(0.80);
        ledger.qty_up = 10.0;
        ledger.qty_down = 7.0;

        let action = CandidateAction {
            name: "BUY_DOWN_MAKER",
            leg: TradeLeg::Down,
            side: TradeSide::Buy,
            kind: TradeKind::Maker,
            qty: 1.0,
        };
        let mut sim = base_sim();
        sim.fill_price = Some(0.79); // too expensive vs required <= 0.776
        sim.new_qty_up = 10.0;
        sim.new_qty_down = 8.0;
        sim.new_unhedged_up = 2.0;
        sim.new_unhedged_down = 0.0;
        sim.new_pair_cost = Some(0.99);
        let round_plan = RoundPlan {
            phase: RoundPhase::Leg2Balancing,
            planned_leg1: None,
            qty_target: None,
            balance_leg: Some(TradeLeg::Down),
            balance_qty: Some(1.0),
            can_start_new_round: false,
            budget_remaining_round: 44.0,
            budget_remaining_total: 94.0,
            reserve_needed_usdc: None,
            vol_entry_bps: 0.0,
            vol_entry_ok: true,
            reversal_up_ok: true,
            reversal_down_ok: true,
            turn_up_ok: true,
            turn_down_ok: true,
            first_leg_turning_score: None,
            entry_worst_pair_cost: None,
            entry_worst_pair_ok: true,
            entry_timeout_flow_ratio: None,
            entry_timeout_flow_ok: true,
            entry_fillability_ok: true,
            entry_edge_bps: None,
            entry_regime_score: None,
            entry_depth_cap_qty: None,
            entry_flow_cap_qty: None,
            slice_count_planned: None,
            slice_qty_current: None,
            entry_final_qty_slice: None,
            entry_fallback_active: false,
            entry_fallback_armed: false,
            entry_fallback_trigger_reason: None,
            entry_fallback_blocked_by_recoverability: false,
            new_round_cutoff_secs: 305,
            late_new_round_blocked: false,
            pair_quality_ok: false,
            pair_regression_ok: false,
            can_open_round_base_ok: false,
            can_start_block_reason: None,
        };

        let result = evaluate_action_gate(&ledger, &action, &sim, &params, 400, &round_plan);
        assert!(!result.allow);
        assert_eq!(result.deny_reason, Some(DenyReason::Cooldown));
    }

    #[test]
    fn leg2_rebalance_discount_bypassed_after_wait_timeout() {
        let mut params = base_params();
        params.round_pair_wait_secs = 5;
        params.leg2_rebalance_discount_bps = 300.0;

        let mut ledger = base_ledger();
        ledger.round_state = RoundPhase::Leg1Accumulating;
        ledger.round_leg1 = Some(TradeLeg::Up);
        ledger.round_leg1_entered_ts = 100;
        ledger.round_leg2_anchor_price = Some(0.80);
        ledger.qty_up = 8.0;
        ledger.qty_down = 0.0;

        let action = CandidateAction {
            name: "BUY_DOWN_MAKER",
            leg: TradeLeg::Down,
            side: TradeSide::Buy,
            kind: TradeKind::Maker,
            qty: 8.0,
        };
        let mut sim = base_sim();
        sim.fill_price = Some(0.79); // fails discount, but timeout should bypass
        sim.new_pair_cost = Some(0.994);
        sim.improves_hedge = true;
        sim.new_qty_up = 8.0;
        sim.new_qty_down = 8.0;
        sim.new_unhedged_up = 0.0;
        sim.new_unhedged_down = 0.0;
        let round_plan = RoundPlan {
            phase: RoundPhase::Leg1Accumulating,
            planned_leg1: Some(TradeLeg::Up),
            qty_target: Some(8.0),
            balance_leg: None,
            balance_qty: None,
            can_start_new_round: false,
            budget_remaining_round: 44.0,
            budget_remaining_total: 94.0,
            reserve_needed_usdc: None,
            vol_entry_bps: 0.0,
            vol_entry_ok: true,
            reversal_up_ok: true,
            reversal_down_ok: true,
            turn_up_ok: true,
            turn_down_ok: true,
            first_leg_turning_score: None,
            entry_worst_pair_cost: None,
            entry_worst_pair_ok: true,
            entry_timeout_flow_ratio: None,
            entry_timeout_flow_ok: true,
            entry_fillability_ok: true,
            entry_edge_bps: None,
            entry_regime_score: None,
            entry_depth_cap_qty: None,
            entry_flow_cap_qty: None,
            slice_count_planned: None,
            slice_qty_current: None,
            entry_final_qty_slice: None,
            entry_fallback_active: false,
            entry_fallback_armed: false,
            entry_fallback_trigger_reason: None,
            entry_fallback_blocked_by_recoverability: false,
            new_round_cutoff_secs: 305,
            late_new_round_blocked: false,
            pair_quality_ok: false,
            pair_regression_ok: false,
            can_open_round_base_ok: false,
            can_start_block_reason: None,
        };

        let result = evaluate_action_gate(&ledger, &action, &sim, &params, 110, &round_plan);
        assert!(result.allow);
    }

    #[test]
    fn force_pair_after_wait_does_not_bypass_min_apply_interval() {
        let mut params = base_params();
        params.round_pair_wait_secs = 5;
        params.min_apply_interval_ms = 800;

        let mut ledger = base_ledger();
        ledger.round_state = RoundPhase::Leg1Accumulating;
        ledger.round_leg1 = Some(TradeLeg::Up);
        ledger.round_leg1_entered_ts = 100;
        ledger.last_apply_ts_ms = 109_500;
        ledger.qty_up = 8.0;
        ledger.qty_down = 0.0;

        let action = CandidateAction {
            name: "BUY_DOWN_MAKER",
            leg: TradeLeg::Down,
            side: TradeSide::Buy,
            kind: TradeKind::Maker,
            qty: 8.0,
        };
        let mut sim = base_sim();
        sim.new_pair_cost = Some(0.99);
        sim.improves_hedge = true;
        sim.new_qty_up = 8.0;
        sim.new_qty_down = 8.0;
        sim.new_unhedged_up = 0.0;
        sim.new_unhedged_down = 0.0;
        let round_plan = RoundPlan {
            phase: RoundPhase::Leg1Accumulating,
            planned_leg1: Some(TradeLeg::Up),
            qty_target: Some(8.0),
            balance_leg: None,
            balance_qty: None,
            can_start_new_round: false,
            budget_remaining_round: 44.0,
            budget_remaining_total: 94.0,
            reserve_needed_usdc: None,
            vol_entry_bps: 0.0,
            vol_entry_ok: true,
            reversal_up_ok: true,
            reversal_down_ok: true,
            turn_up_ok: true,
            turn_down_ok: true,
            first_leg_turning_score: None,
            entry_worst_pair_cost: None,
            entry_worst_pair_ok: true,
            entry_timeout_flow_ratio: None,
            entry_timeout_flow_ok: true,
            entry_fillability_ok: true,
            entry_edge_bps: None,
            entry_regime_score: None,
            entry_depth_cap_qty: None,
            entry_flow_cap_qty: None,
            slice_count_planned: None,
            slice_qty_current: None,
            entry_final_qty_slice: None,
            entry_fallback_active: false,
            entry_fallback_armed: false,
            entry_fallback_trigger_reason: None,
            entry_fallback_blocked_by_recoverability: false,
            new_round_cutoff_secs: 305,
            late_new_round_blocked: false,
            pair_quality_ok: false,
            pair_regression_ok: false,
            can_open_round_base_ok: false,
            can_start_block_reason: None,
        };

        let result = evaluate_action_gate(&ledger, &action, &sim, &params, 110, &round_plan);

        assert!(!result.allow);
        assert_eq!(result.deny_reason, Some(DenyReason::Cooldown));
    }

    #[test]
    fn leg1_wait_timeout_denies_more_same_side_adds() {
        let mut params = base_params();
        params.round_pair_wait_secs = 5;

        let mut ledger = base_ledger();
        ledger.round_state = RoundPhase::Leg1Accumulating;
        ledger.round_leg1 = Some(TradeLeg::Down);
        ledger.round_leg1_entered_ts = 100;
        ledger.qty_up = 0.0;
        ledger.qty_down = 10.0;

        let action = CandidateAction {
            name: "BUY_DOWN_MAKER",
            leg: TradeLeg::Down,
            side: TradeSide::Buy,
            kind: TradeKind::Maker,
            qty: 2.0,
        };
        let mut sim = base_sim();
        sim.new_avg_down = Some(0.52);
        sim.new_pair_cost = Some(1.0);
        let round_plan = RoundPlan {
            phase: RoundPhase::Leg1Accumulating,
            planned_leg1: Some(TradeLeg::Down),
            qty_target: Some(2.0),
            balance_leg: None,
            balance_qty: None,
            can_start_new_round: false,
            budget_remaining_round: 44.0,
            budget_remaining_total: 94.0,
            reserve_needed_usdc: None,
            vol_entry_bps: 0.0,
            vol_entry_ok: true,
            reversal_up_ok: true,
            reversal_down_ok: true,
            turn_up_ok: true,
            turn_down_ok: true,
            first_leg_turning_score: None,
            entry_worst_pair_cost: None,
            entry_worst_pair_ok: true,
            entry_timeout_flow_ratio: None,
            entry_timeout_flow_ok: true,
            entry_fillability_ok: true,
            entry_edge_bps: None,
            entry_regime_score: None,
            entry_depth_cap_qty: None,
            entry_flow_cap_qty: None,
            slice_count_planned: None,
            slice_qty_current: None,
            entry_final_qty_slice: None,
            entry_fallback_active: false,
            entry_fallback_armed: false,
            entry_fallback_trigger_reason: None,
            entry_fallback_blocked_by_recoverability: false,
            new_round_cutoff_secs: 305,
            late_new_round_blocked: false,
            pair_quality_ok: false,
            pair_regression_ok: false,
            can_open_round_base_ok: false,
            can_start_block_reason: None,
        };

        let result = evaluate_action_gate(&ledger, &action, &sim, &params, 106, &round_plan);

        assert!(!result.allow);
        assert_eq!(result.deny_reason, Some(DenyReason::ReserveForPair));
    }

    #[test]
    fn leg1_same_side_denied_when_projected_pair_cost_above_break_even() {
        let params = base_params();
        let mut ledger = base_ledger();
        ledger.round_state = RoundPhase::Leg1Accumulating;
        ledger.round_leg1 = Some(TradeLeg::Up);
        ledger.round_leg1_entered_ts = 100;
        ledger.best_bid_down = Some(0.62);

        let action = CandidateAction {
            name: "BUY_UP_MAKER",
            leg: TradeLeg::Up,
            side: TradeSide::Buy,
            kind: TradeKind::Maker,
            qty: 1.0,
        };
        let mut sim = base_sim();
        sim.new_avg_up = Some(0.42);
        sim.new_pair_cost = Some(1.04);
        let round_plan = RoundPlan {
            phase: RoundPhase::Leg1Accumulating,
            planned_leg1: Some(TradeLeg::Up),
            qty_target: Some(1.0),
            balance_leg: None,
            balance_qty: None,
            can_start_new_round: false,
            budget_remaining_round: 44.0,
            budget_remaining_total: 94.0,
            reserve_needed_usdc: None,
            vol_entry_bps: 0.0,
            vol_entry_ok: true,
            reversal_up_ok: true,
            reversal_down_ok: true,
            turn_up_ok: true,
            turn_down_ok: true,
            first_leg_turning_score: None,
            entry_worst_pair_cost: None,
            entry_worst_pair_ok: true,
            entry_timeout_flow_ratio: None,
            entry_timeout_flow_ok: true,
            entry_fillability_ok: true,
            entry_edge_bps: None,
            entry_regime_score: None,
            entry_depth_cap_qty: None,
            entry_flow_cap_qty: None,
            slice_count_planned: None,
            slice_qty_current: None,
            entry_final_qty_slice: None,
            entry_fallback_active: false,
            entry_fallback_armed: false,
            entry_fallback_trigger_reason: None,
            entry_fallback_blocked_by_recoverability: false,
            new_round_cutoff_secs: 305,
            late_new_round_blocked: false,
            pair_quality_ok: false,
            pair_regression_ok: false,
            can_open_round_base_ok: false,
            can_start_block_reason: None,
        };

        let result = evaluate_action_gate(&ledger, &action, &sim, &params, 102, &round_plan);

        assert!(!result.allow);
        assert_eq!(result.deny_reason, Some(DenyReason::MarginTarget));
    }

    #[test]
    fn leg1_same_side_projection_uses_opposite_ask() {
        let params = base_params();
        let mut ledger = base_ledger();
        ledger.round_state = RoundPhase::Leg1Accumulating;
        ledger.round_leg1 = Some(TradeLeg::Up);
        ledger.round_leg1_entered_ts = 100;
        ledger.best_bid_down = Some(0.50);
        ledger.best_ask_down = Some(0.70);

        let action = CandidateAction {
            name: "BUY_UP_MAKER",
            leg: TradeLeg::Up,
            side: TradeSide::Buy,
            kind: TradeKind::Maker,
            qty: 1.0,
        };
        let mut sim = base_sim();
        sim.new_avg_up = Some(0.30);
        sim.new_pair_cost = Some(0.90);
        let round_plan = RoundPlan {
            phase: RoundPhase::Leg1Accumulating,
            planned_leg1: Some(TradeLeg::Up),
            qty_target: Some(1.0),
            balance_leg: None,
            balance_qty: None,
            can_start_new_round: false,
            budget_remaining_round: 44.0,
            budget_remaining_total: 94.0,
            reserve_needed_usdc: None,
            vol_entry_bps: 0.0,
            vol_entry_ok: true,
            reversal_up_ok: true,
            reversal_down_ok: true,
            turn_up_ok: true,
            turn_down_ok: true,
            first_leg_turning_score: None,
            entry_worst_pair_cost: None,
            entry_worst_pair_ok: true,
            entry_timeout_flow_ratio: None,
            entry_timeout_flow_ok: true,
            entry_fillability_ok: true,
            entry_edge_bps: None,
            entry_regime_score: None,
            entry_depth_cap_qty: None,
            entry_flow_cap_qty: None,
            slice_count_planned: None,
            slice_qty_current: None,
            entry_final_qty_slice: None,
            entry_fallback_active: false,
            entry_fallback_armed: false,
            entry_fallback_trigger_reason: None,
            entry_fallback_blocked_by_recoverability: false,
            new_round_cutoff_secs: 305,
            late_new_round_blocked: false,
            pair_quality_ok: false,
            pair_regression_ok: false,
            can_open_round_base_ok: false,
            can_start_block_reason: None,
        };

        let result = evaluate_action_gate(&ledger, &action, &sim, &params, 102, &round_plan);
        assert!(!result.allow);
        assert_eq!(result.deny_reason, Some(DenyReason::MarginTarget));
    }

    #[test]
    fn idle_opening_leg_denied_when_entry_worst_pair_fails() {
        let params = base_params();
        let mut ledger = base_ledger();
        ledger.qty_up = 5.0;
        ledger.qty_down = 5.0;
        let action = CandidateAction {
            name: "BUY_UP_MAKER",
            leg: TradeLeg::Up,
            side: TradeSide::Buy,
            kind: TradeKind::Maker,
            qty: 1.0,
        };
        let mut sim = base_sim();
        sim.new_qty_up = 6.0;
        sim.new_qty_down = 5.0;
        sim.new_unhedged_up = 1.0;
        sim.new_unhedged_down = 0.0;
        sim.new_pair_cost = Some(0.99);
        let round_plan = RoundPlan {
            entry_worst_pair_ok: false,
            planned_leg1: Some(TradeLeg::Up),
            ..base_round_plan()
        };

        let result = evaluate_action_gate(&ledger, &action, &sim, &params, 400, &round_plan);
        assert!(!result.allow);
        assert_eq!(result.deny_reason, Some(DenyReason::EntryWorstPair));
    }

    #[test]
    fn hard_cap_only_mode_allows_pair_above_legacy_margin_limit() {
        let mut params = base_params();
        params.margin_target = 0.01;
        let mut ledger = base_ledger();
        ledger.qty_up = 5.0;
        ledger.qty_down = 5.0;
        let action = CandidateAction {
            name: "BUY_UP_MAKER",
            leg: TradeLeg::Up,
            side: TradeSide::Buy,
            kind: TradeKind::Maker,
            qty: 1.0,
        };
        let mut sim = base_sim();
        sim.new_qty_up = 6.0;
        sim.new_qty_down = 5.0;
        sim.new_unhedged_up = 1.0;
        sim.new_unhedged_down = 0.0;
        sim.new_pair_cost = Some(0.992);
        let round_plan = RoundPlan {
            entry_worst_pair_ok: true,
            planned_leg1: Some(TradeLeg::Up),
            ..base_round_plan()
        };

        params.no_risk_pair_limit_mode = NoRiskPairLimitMode::HardCapOnly;
        let hard_result = evaluate_action_gate(&ledger, &action, &sim, &params, 400, &round_plan);
        assert!(hard_result.allow);

        params.no_risk_pair_limit_mode = NoRiskPairLimitMode::LegacyMarginThenCap;
        let legacy_result = evaluate_action_gate(&ledger, &action, &sim, &params, 400, &round_plan);
        assert!(!legacy_result.allow);
        assert_eq!(legacy_result.deny_reason, Some(DenyReason::MarginTarget));
    }

    #[test]
    fn late_new_round_cutoff_denies_increasing_abs_net() {
        let params = base_params();
        let mut ledger = base_ledger();
        ledger.qty_up = 5.0;
        ledger.qty_down = 5.0;
        let action = CandidateAction {
            name: "BUY_UP_MAKER",
            leg: TradeLeg::Up,
            side: TradeSide::Buy,
            kind: TradeKind::Maker,
            qty: 1.0,
        };
        let mut sim = base_sim();
        sim.new_qty_up = 6.0;
        sim.new_qty_down = 5.0;
        sim.new_unhedged_up = 1.0;
        sim.new_unhedged_down = 0.0;
        sim.new_pair_cost = Some(0.99);
        let round_plan = RoundPlan {
            planned_leg1: Some(TradeLeg::Up),
            late_new_round_blocked: true,
            pair_quality_ok: false,
            pair_regression_ok: false,
            can_open_round_base_ok: false,
            can_start_block_reason: None,
            ..base_round_plan()
        };

        let result = evaluate_action_gate(&ledger, &action, &sim, &params, 400, &round_plan);
        assert!(!result.allow);
        assert_eq!(result.deny_reason, Some(DenyReason::TailFreeze));
    }

    #[test]
    fn low_maker_fill_prob_denied_outside_tail_close() {
        let mut params = base_params();
        params.entry_fill_prob_min = 0.30;
        let ledger = base_ledger();
        let action = base_action();
        let mut sim = base_sim();
        sim.maker_fill_prob = Some(0.10);
        let round_plan = RoundPlan {
            phase: RoundPhase::Idle,
            planned_leg1: Some(TradeLeg::Down),
            qty_target: Some(1.0),
            balance_leg: None,
            balance_qty: None,
            can_start_new_round: true,
            ..base_round_plan()
        };

        let result = evaluate_action_gate(&ledger, &action, &sim, &params, 400, &round_plan);

        assert!(!result.allow);
        assert_eq!(result.deny_reason, Some(DenyReason::LowMakerFillProb));
    }

    #[test]
    fn low_maker_fill_prob_not_denied_when_only_floor_signal_exists() {
        let mut params = base_params();
        params.entry_fill_prob_min = 0.30;
        let ledger = base_ledger();
        let action = base_action();
        let mut sim = base_sim();
        sim.maker_fill_prob = Some(0.10);
        sim.maker_consumption_rate = Some(params.maker_flow_floor_per_sec);
        sim.new_pair_cost = Some(0.95);
        let round_plan = RoundPlan {
            phase: RoundPhase::Idle,
            planned_leg1: Some(TradeLeg::Down),
            qty_target: Some(1.0),
            balance_leg: None,
            balance_qty: None,
            can_start_new_round: true,
            ..base_round_plan()
        };

        let result = evaluate_action_gate(&ledger, &action, &sim, &params, 400, &round_plan);

        assert!(result.allow);
        assert!(result.deny_reason.is_none());
    }

    #[test]
    fn low_maker_fill_prob_not_applied_when_round_cannot_start() {
        let mut params = base_params();
        params.entry_fill_prob_min = 0.30;
        let ledger = base_ledger();
        let action = base_action();
        let mut sim = base_sim();
        sim.maker_fill_prob = Some(0.10);
        sim.maker_consumption_rate = Some(params.maker_flow_floor_per_sec + 1.0);
        sim.new_pair_cost = Some(0.95);
        let round_plan = RoundPlan {
            phase: RoundPhase::Idle,
            planned_leg1: Some(TradeLeg::Down),
            qty_target: Some(1.0),
            balance_leg: None,
            balance_qty: None,
            can_start_new_round: false,
            ..base_round_plan()
        };

        let result = evaluate_action_gate(&ledger, &action, &sim, &params, 400, &round_plan);

        assert!(result.allow);
        assert!(result.deny_reason.is_none());
    }

    #[test]
    fn estimator_zero_fill_is_not_classified_as_no_quote() {
        let params = base_params();
        let ledger = base_ledger();
        let action = base_action();
        let mut sim = base_sim();
        sim.ok = false;
        sim.fill_qty = 0.0;
        sim.fill_price = Some(0.60);
        sim.maker_fill_prob = Some(0.0);
        let round_plan = base_round_plan();

        let result = evaluate_action_gate(&ledger, &action, &sim, &params, 400, &round_plan);

        assert!(!result.allow);
        assert_eq!(result.deny_reason, Some(DenyReason::LowMakerFillProb));
    }

    #[test]
    fn missing_quote_still_classified_as_no_quote() {
        let params = base_params();
        let ledger = base_ledger();
        let action = base_action();
        let mut sim = base_sim();
        sim.ok = false;
        sim.fill_qty = 0.0;
        sim.fill_price = None;
        sim.maker_fill_prob = None;
        let round_plan = base_round_plan();

        let result = evaluate_action_gate(&ledger, &action, &sim, &params, 400, &round_plan);

        assert!(!result.allow);
        assert_eq!(result.deny_reason, Some(DenyReason::NoQuote));
    }

    #[test]
    fn round_budget_strict_blocks_even_reducing_risk_when_over_cap() {
        let mut params = base_params();
        params.round_budget_usdc = 6.0;
        params.round_budget_strict = true;
        let ledger = base_ledger();
        let action = base_action();
        let sim = base_sim();
        let round_plan = base_round_plan();

        let result = evaluate_action_gate(&ledger, &action, &sim, &params, 400, &round_plan);

        assert!(!result.allow);
        assert_eq!(result.deny_reason, Some(DenyReason::RoundBudgetCap));
    }

    #[test]
    fn opening_no_trade_blocks_net_increasing_actions() {
        let mut params = base_params();
        params.opening_no_trade_secs = 60;
        let mut ledger = base_ledger();
        ledger.start_ts = 0;
        let action = CandidateAction {
            name: "BUY_UP_MAKER",
            leg: TradeLeg::Up,
            side: TradeSide::Buy,
            kind: TradeKind::Maker,
            qty: 1.0,
        };
        let mut sim = base_sim();
        sim.new_qty_up = 11.0;
        sim.new_qty_down = 5.0;
        sim.new_unhedged_up = 6.0;
        sim.new_unhedged_down = 0.0;
        sim.new_unhedged_value_up = 2.46;
        sim.new_pair_cost = Some(0.995);
        let round_plan = base_round_plan();

        let result = evaluate_action_gate(&ledger, &action, &sim, &params, 30, &round_plan);

        assert!(!result.allow);
        assert_eq!(result.deny_reason, Some(DenyReason::Cooldown));
    }

    #[test]
    fn resting_fill_skips_entry_worst_pair_soft_gate() {
        let params = base_params();
        let mut ledger = base_ledger();
        ledger.qty_up = 5.0;
        ledger.qty_down = 5.0;
        let action = CandidateAction {
            name: "RESTING_MAKER_FILL",
            leg: TradeLeg::Up,
            side: TradeSide::Buy,
            kind: TradeKind::Maker,
            qty: 1.0,
        };
        let mut sim = base_sim();
        sim.new_qty_up = 6.0;
        sim.new_qty_down = 5.0;
        sim.new_unhedged_up = 1.0;
        sim.new_unhedged_down = 0.0;
        sim.new_pair_cost = Some(0.99);
        let round_plan = RoundPlan {
            entry_worst_pair_ok: false,
            planned_leg1: Some(TradeLeg::Up),
            ..base_round_plan()
        };

        let selection_result = evaluate_action_gate_with_context(
            &ledger,
            &action,
            &sim,
            &params,
            400,
            &round_plan,
            GateContext::CandidateSelection,
        );
        assert!(!selection_result.allow);
        assert_eq!(
            selection_result.deny_reason,
            Some(DenyReason::EntryWorstPair)
        );

        let resting_result = evaluate_action_gate_with_context(
            &ledger,
            &action,
            &sim,
            &params,
            400,
            &round_plan,
            GateContext::RestingFill,
        );
        assert!(resting_result.allow);
        assert_eq!(resting_result.deny_reason, None);
    }

    #[test]
    fn resting_fill_still_respects_hard_pair_cap() {
        let params = base_params();
        let mut ledger = base_ledger();
        ledger.qty_up = 5.0;
        ledger.qty_down = 5.0;
        let action = CandidateAction {
            name: "RESTING_MAKER_FILL",
            leg: TradeLeg::Up,
            side: TradeSide::Buy,
            kind: TradeKind::Maker,
            qty: 1.0,
        };
        let mut sim = base_sim();
        sim.new_qty_up = 6.0;
        sim.new_qty_down = 5.0;
        sim.new_unhedged_up = 1.0;
        sim.new_unhedged_down = 0.0;
        sim.new_pair_cost = Some(1.0);
        let round_plan = RoundPlan {
            entry_worst_pair_ok: false,
            planned_leg1: Some(TradeLeg::Up),
            ..base_round_plan()
        };

        let resting_result = evaluate_action_gate_with_context(
            &ledger,
            &action,
            &sim,
            &params,
            400,
            &round_plan,
            GateContext::RestingFill,
        );
        assert!(!resting_result.allow);
        assert_eq!(resting_result.deny_reason, Some(DenyReason::MarginTarget));
    }

    #[test]
    fn candidate_selection_denies_unrecoverable_hedge() {
        let params = base_params();
        let mut ledger = base_ledger();
        ledger.qty_up = 14.0;
        ledger.qty_down = 14.0;
        let action = CandidateAction {
            name: "BUY_DOWN_MAKER",
            leg: TradeLeg::Down,
            side: TradeSide::Buy,
            kind: TradeKind::Maker,
            qty: 16.0,
        };
        let mut sim = base_sim();
        sim.new_qty_up = 14.0;
        sim.new_qty_down = 30.0;
        sim.new_pair_cost = Some(0.95);
        sim.new_unhedged_up = 0.0;
        sim.new_unhedged_down = 16.0;
        sim.hedge_recoverable_now = Some(false);
        sim.required_opp_avg_price_cap = Some(0.509375);
        sim.current_opp_best_ask = Some(0.58);
        sim.required_hedge_qty = Some(16.0);
        let round_plan = RoundPlan {
            planned_leg1: Some(TradeLeg::Down),
            entry_worst_pair_ok: true,
            ..base_round_plan()
        };

        let result = evaluate_action_gate_with_context(
            &ledger,
            &action,
            &sim,
            &params,
            400,
            &round_plan,
            GateContext::CandidateSelection,
        );
        assert!(!result.allow);
        assert_eq!(result.deny_reason, Some(DenyReason::HedgeNotRecoverable));
    }

    #[test]
    fn resting_fill_denies_unrecoverable_hedge() {
        let params = base_params();
        let mut ledger = base_ledger();
        ledger.qty_up = 14.0;
        ledger.qty_down = 14.0;
        let action = CandidateAction {
            name: "RESTING_MAKER_FILL",
            leg: TradeLeg::Down,
            side: TradeSide::Buy,
            kind: TradeKind::Maker,
            qty: 16.0,
        };
        let mut sim = base_sim();
        sim.new_qty_up = 14.0;
        sim.new_qty_down = 30.0;
        sim.new_pair_cost = Some(0.95);
        sim.new_unhedged_up = 0.0;
        sim.new_unhedged_down = 16.0;
        sim.hedge_recoverable_now = Some(false);
        sim.required_opp_avg_price_cap = Some(0.509375);
        sim.current_opp_best_ask = Some(0.58);
        sim.required_hedge_qty = Some(16.0);
        let round_plan = RoundPlan {
            planned_leg1: Some(TradeLeg::Down),
            entry_worst_pair_ok: false,
            ..base_round_plan()
        };

        let result = evaluate_action_gate_with_context(
            &ledger,
            &action,
            &sim,
            &params,
            400,
            &round_plan,
            GateContext::RestingFill,
        );
        assert!(!result.allow);
        assert_eq!(result.deny_reason, Some(DenyReason::HedgeNotRecoverable));
    }

    #[test]
    fn candidate_selection_denies_insufficient_hedge_margin() {
        let mut params = base_params();
        params.no_risk_hedge_recoverability_enforce = false;
        let ledger = base_ledger();
        let action = base_action();
        let mut sim = base_sim();
        sim.new_qty_up = 10.0;
        sim.new_qty_down = 17.0;
        sim.new_unhedged_up = 0.0;
        sim.new_unhedged_down = 7.0;
        sim.hedge_margin_to_opp_ask = Some(0.001);
        sim.hedge_margin_required = Some(0.004);
        sim.hedge_margin_ok = Some(false);
        let round_plan = RoundPlan {
            planned_leg1: Some(TradeLeg::Down),
            entry_worst_pair_ok: true,
            ..base_round_plan()
        };

        let result = evaluate_action_gate_with_context(
            &ledger,
            &action,
            &sim,
            &params,
            400,
            &round_plan,
            GateContext::CandidateSelection,
        );
        assert!(!result.allow);
        assert_eq!(
            result.deny_reason,
            Some(DenyReason::HedgeMarginInsufficient)
        );
    }

    #[test]
    fn candidate_selection_allows_sufficient_hedge_margin() {
        let mut params = base_params();
        params.no_risk_hedge_recoverability_enforce = false;
        let ledger = base_ledger();
        let action = base_action();
        let mut sim = base_sim();
        sim.new_qty_up = 10.0;
        sim.new_qty_down = 17.0;
        sim.new_pair_cost = Some(0.98);
        sim.new_unhedged_up = 0.0;
        sim.new_unhedged_down = 7.0;
        sim.hedge_margin_to_opp_ask = Some(0.006);
        sim.hedge_margin_required = Some(0.005);
        sim.hedge_margin_ok = Some(true);
        let round_plan = RoundPlan {
            planned_leg1: Some(TradeLeg::Down),
            entry_worst_pair_ok: true,
            ..base_round_plan()
        };

        let result = evaluate_action_gate_with_context(
            &ledger,
            &action,
            &sim,
            &params,
            400,
            &round_plan,
            GateContext::CandidateSelection,
        );
        assert!(result.allow);
    }

    #[test]
    fn reducing_risk_not_blocked_by_hedge_margin_rule() {
        let params = base_params();
        let mut ledger = base_ledger();
        ledger.qty_up = 5.0;
        ledger.qty_down = 10.0;
        ledger.cost_up = 2.3;
        ledger.cost_down = 5.8;
        ledger.round_state = RoundPhase::Leg2Balancing;
        ledger.round_leg1 = Some(TradeLeg::Down);
        let action = CandidateAction {
            name: "BUY_UP_MAKER",
            leg: TradeLeg::Up,
            side: TradeSide::Buy,
            kind: TradeKind::Maker,
            qty: 1.0,
        };
        let mut sim = base_sim();
        sim.new_qty_up = 6.0;
        sim.new_qty_down = 10.0;
        sim.new_pair_cost = Some(0.98);
        sim.new_unhedged_up = 0.0;
        sim.new_unhedged_down = 4.0;
        sim.hedge_margin_ok = Some(false);
        let round_plan = RoundPlan {
            phase: RoundPhase::Leg2Balancing,
            planned_leg1: Some(TradeLeg::Down),
            ..base_round_plan()
        };

        let result = evaluate_action_gate_with_context(
            &ledger,
            &action,
            &sim,
            &params,
            400,
            &round_plan,
            GateContext::CandidateSelection,
        );
        assert!(result.allow);
    }

    #[test]
    fn candidate_selection_denies_open_margin_too_thin() {
        let mut params = base_params();
        params.no_risk_hedge_recoverability_enforce = false;
        params.hedge_recoverability_margin_enforce = false;
        params.open_margin_surplus_min = 0.001;
        params.lock_min_spent_ratio = 0.0;

        let ledger = base_ledger();
        let action = base_action();
        let mut sim = base_sim();
        sim.new_qty_up = 10.0;
        sim.new_qty_down = 17.0;
        sim.new_unhedged_up = 0.0;
        sim.new_unhedged_down = 7.0;
        sim.hedge_margin_to_opp_ask = Some(0.0052);
        sim.hedge_margin_required = Some(0.0050);
        let round_plan = RoundPlan {
            planned_leg1: Some(TradeLeg::Down),
            entry_worst_pair_ok: true,
            ..base_round_plan()
        };

        let result = evaluate_action_gate_with_context(
            &ledger,
            &action,
            &sim,
            &params,
            400,
            &round_plan,
            GateContext::CandidateSelection,
        );
        assert!(!result.allow);
        assert_eq!(result.deny_reason, Some(DenyReason::OpenMarginTooThin));
    }

    #[test]
    fn candidate_selection_allows_open_margin_near_threshold_with_eps() {
        let mut params = base_params();
        params.no_risk_hedge_recoverability_enforce = false;
        params.hedge_recoverability_margin_enforce = false;
        params.no_risk_hard_pair_cap = 1.0;
        params.open_margin_surplus_min = 0.001;
        params.lock_min_spent_ratio = 0.0;

        let mut ledger = base_ledger();
        ledger.tick_size_current = 0.01;
        let action = base_action();
        let mut sim = base_sim();
        sim.new_qty_up = 10.0;
        sim.new_qty_down = 17.0;
        sim.new_unhedged_up = 0.0;
        sim.new_unhedged_down = 7.0;
        // Surplus is just below threshold; epsilon should prevent false rejection.
        sim.hedge_margin_to_opp_ask = Some(0.005995);
        sim.hedge_margin_required = Some(0.005);
        let round_plan = RoundPlan {
            planned_leg1: Some(TradeLeg::Down),
            entry_worst_pair_ok: true,
            ..base_round_plan()
        };

        let result = evaluate_action_gate_with_context(
            &ledger,
            &action,
            &sim,
            &params,
            400,
            &round_plan,
            GateContext::CandidateSelection,
        );
        assert!(result.allow);
    }

    #[test]
    fn candidate_selection_relaxes_open_margin_when_spent_is_low_early_window() {
        let mut params = base_params();
        params.no_risk_hedge_recoverability_enforce = false;
        params.hedge_recoverability_margin_enforce = false;
        params.open_margin_surplus_min = 0.001;
        params.no_risk_hard_pair_cap = 1.0;
        params.lock_min_spent_ratio = 0.35;
        params.lock_force_time_left_secs = 540;
        params.total_budget_usdc = 100.0;

        let mut ledger = base_ledger();
        ledger.spent_total_usdc = 0.0;
        // With now_ts=400 and end_ts=1000, time_left=600 (>540), so relaxation applies.
        let action = base_action();
        let mut sim = base_sim();
        sim.new_qty_up = 10.0;
        sim.new_qty_down = 17.0;
        sim.new_unhedged_up = 0.0;
        sim.new_unhedged_down = 7.0;
        sim.hedge_margin_to_opp_ask = Some(0.0052);
        sim.hedge_margin_required = Some(0.0050);
        let round_plan = RoundPlan {
            planned_leg1: Some(TradeLeg::Down),
            entry_worst_pair_ok: true,
            ..base_round_plan()
        };

        let result = evaluate_action_gate_with_context(
            &ledger,
            &action,
            &sim,
            &params,
            400,
            &round_plan,
            GateContext::CandidateSelection,
        );
        assert!(result.allow);
    }
}

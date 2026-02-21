use super::params::{
    EntryPairRegressionMode, NoRiskEntryHedgePriceMode, StrategyParams,
};
use super::simulate::compute_maker_buy_quote;
use super::state::{avg_cost, Ledger, RoundPhase, TradeLeg};

#[derive(Debug, Clone)]
pub struct RoundPlan {
    pub phase: RoundPhase,
    pub planned_leg1: Option<TradeLeg>,
    pub qty_target: Option<f64>,
    pub balance_leg: Option<TradeLeg>,
    pub balance_qty: Option<f64>,
    pub can_start_new_round: bool,
    pub budget_remaining_round: f64,
    pub budget_remaining_total: f64,
    pub reserve_needed_usdc: Option<f64>,
    pub vol_entry_bps: f64,
    pub vol_entry_ok: bool,
    pub reversal_up_ok: bool,
    pub reversal_down_ok: bool,
    pub turn_up_ok: bool,
    pub turn_down_ok: bool,
    pub first_leg_turning_score: Option<f64>,
    pub entry_worst_pair_cost: Option<f64>,
    pub entry_worst_pair_ok: bool,
    pub entry_timeout_flow_ratio: Option<f64>,
    pub entry_timeout_flow_ok: bool,
    pub entry_fillability_ok: bool,
    pub entry_edge_bps: Option<f64>,
    pub entry_regime_score: Option<f64>,
    pub entry_depth_cap_qty: Option<f64>,
    pub entry_flow_cap_qty: Option<f64>,
    pub slice_count_planned: Option<u32>,
    pub slice_qty_current: Option<f64>,
    pub entry_final_qty_slice: Option<f64>,
    pub entry_fallback_active: bool,
    pub entry_fallback_armed: bool,
    pub entry_fallback_trigger_reason: Option<String>,
    pub entry_fallback_blocked_by_recoverability: bool,
    pub new_round_cutoff_secs: u64,
    pub late_new_round_blocked: bool,
    pub pair_quality_ok: bool,
    pub pair_regression_ok: bool,
    pub can_open_round_base_ok: bool,
    pub can_start_block_reason: Option<String>,
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

fn cap_round_qty_by_visible_depth(
    ledger: &Ledger,
    leg1: TradeLeg,
    qty_target: f64,
    entry_max_top_book_share: f64,
) -> Option<f64> {
    if qty_target <= 0.0 {
        return None;
    }
    let top_size = match leg1 {
        TradeLeg::Up => ledger.best_bid_size_up,
        TradeLeg::Down => ledger.best_bid_size_down,
    };
    let Some(top_size) = top_size else {
        return Some(qty_target);
    };
    if top_size <= 0.0 || entry_max_top_book_share <= 0.0 {
        return None;
    }
    let depth_cap = (top_size * entry_max_top_book_share).floor();
    if depth_cap <= 0.0 {
        return None;
    }
    Some(qty_target.min(depth_cap))
}

fn cap_round_qty_by_expected_flow(
    ledger: &Ledger,
    leg1: TradeLeg,
    qty_target: f64,
    params: &StrategyParams,
    entry_max_flow_utilization: f64,
) -> Option<f64> {
    if qty_target <= 0.0 {
        return None;
    }
    if entry_max_flow_utilization <= 0.0 || params.maker_fill_horizon_secs == 0 {
        return Some(qty_target);
    }
    let consume_rate = match leg1 {
        TradeLeg::Up => ledger.bid_consumption_rate_up,
        TradeLeg::Down => ledger.bid_consumption_rate_down,
    }
    .max(params.maker_flow_floor_per_sec);
    let flow_cap =
        (consume_rate * params.maker_fill_horizon_secs as f64 * entry_max_flow_utilization)
            .floor();
    if flow_cap <= 0.0 {
        return None;
    }
    Some(qty_target.min(flow_cap))
}

fn compute_dynamic_slice_count(
    ledger: &Ledger,
    params: &StrategyParams,
    leg: TradeLeg,
    total_qty: f64,
    time_left_secs: u64,
    pacing_gap_ratio: f64,
    entry_max_top_book_share: f64,
    entry_max_flow_utilization: f64,
) -> u64 {
    if total_qty <= 0.0 {
        return 1;
    }
    if !params.round_dynamic_slicing_enabled {
        return params.round_slice_count.max(1);
    }
    let base_min_slices = params.round_min_slices.max(1);
    let spent_ratio = if params.total_budget_usdc > 0.0 {
        (ledger.spent_total_usdc / params.total_budget_usdc).clamp(0.0, 1.0)
    } else {
        1.0
    };
    let ample_time_for_aggressive_sizing =
        time_left_secs > params.tail_close_secs.saturating_add(240);
    let sufficiently_large_target = total_qty
        >= (params.round_min_slice_qty.max(1.0) * base_min_slices as f64).max(4.0);
    let min_slices = if base_min_slices <= 2 {
        base_min_slices
    } else if ample_time_for_aggressive_sizing
        && sufficiently_large_target
        && pacing_gap_ratio + 1e-9 >= 0.18
    {
        base_min_slices.saturating_sub(3).max(1)
    } else if ample_time_for_aggressive_sizing
        && sufficiently_large_target
        && pacing_gap_ratio + 1e-9 >= 0.10
    {
        base_min_slices.saturating_sub(2).max(1)
    } else if ample_time_for_aggressive_sizing
        && sufficiently_large_target
        && spent_ratio + 1e-9 < 0.20
    {
        base_min_slices.saturating_sub(2).max(2)
    } else if ample_time_for_aggressive_sizing
        && sufficiently_large_target
        && spent_ratio + 1e-9 < 0.35
    {
        base_min_slices.saturating_sub(1).max(2)
    } else {
        base_min_slices
    };
    let max_slices_base = params.round_max_slices.max(min_slices);
    let max_slices = if ample_time_for_aggressive_sizing && pacing_gap_ratio + 1e-9 >= 0.12 {
        max_slices_base.saturating_sub(2).max(min_slices)
    } else {
        max_slices_base
    };
    let top_size = match leg {
        TradeLeg::Up => ledger.best_bid_size_up,
        TradeLeg::Down => ledger.best_bid_size_down,
    }
    .unwrap_or(total_qty)
    .max(params.round_min_slice_qty);
    let consume_rate = match leg {
        TradeLeg::Up => ledger.bid_consumption_rate_up,
        TradeLeg::Down => ledger.bid_consumption_rate_down,
    }
    .max(params.maker_flow_floor_per_sec);
    let horizon = params.maker_fill_horizon_secs.max(1) as f64;
    let depth_unit =
        (top_size * entry_max_top_book_share.max(0.0)).max(params.round_min_slice_qty);
    let flow_unit = (consume_rate * horizon * entry_max_flow_utilization.max(0.0))
        .max(params.round_min_slice_qty);
    let per_slice_cap = depth_unit.min(flow_unit).max(params.round_min_slice_qty);
    let derived = (total_qty / per_slice_cap).ceil() as u64;
    derived.clamp(min_slices, max_slices)
}

fn utilization_boost_active(
    ledger: &Ledger,
    params: &StrategyParams,
    now_ts: u64,
    time_left_secs: u64,
    pacing_gap_ratio: f64,
) -> bool {
    if params.total_budget_usdc <= 0.0 {
        return false;
    }
    let elapsed_secs = now_ts.saturating_sub(ledger.start_ts);
    if ledger.round_idx == 0 && elapsed_secs < 120 {
        return false;
    }
    let spent_ratio = (ledger.spent_total_usdc / params.total_budget_usdc).clamp(0.0, 1.0);
    let min_time_left_for_boost = params.tail_close_secs.saturating_add(180);
    let deficit_triggered = pacing_gap_ratio + 1e-9 >= 0.08;
    (spent_ratio + 1e-9 < 0.30 || deficit_triggered) && time_left_secs > min_time_left_for_boost
}

fn effective_entry_limits(
    params: &StrategyParams,
    boost_active: bool,
    spent_ratio: f64,
    time_left_secs: u64,
    pacing_gap_ratio: f64,
) -> (f64, f64) {
    let base_top_share = params.entry_max_top_book_share.max(0.0);
    let base_flow_util = params.entry_max_flow_utilization.max(0.0);
    if !boost_active {
        return (base_top_share, base_flow_util);
    }
    let mut boosted_top_share = (base_top_share * 1.5)
        .max(base_top_share + 0.05)
        .min(0.5);
    let mut boosted_flow_util = (base_flow_util * 1.5)
        .max(base_flow_util + 0.10)
        .min(0.8);
    let ample_time_for_aggressive_sizing = time_left_secs > params.tail_close_secs.saturating_add(240);
    if ample_time_for_aggressive_sizing && pacing_gap_ratio + 1e-9 >= 0.18 {
        boosted_top_share = boosted_top_share
            .max((base_top_share + 0.20).min(0.75))
            .min(0.75);
        boosted_flow_util = boosted_flow_util
            .max((base_flow_util + 0.35).min(0.95))
            .min(0.95);
    } else if ample_time_for_aggressive_sizing && pacing_gap_ratio + 1e-9 >= 0.10 {
        boosted_top_share = boosted_top_share
            .max((base_top_share + 0.15).min(0.70))
            .min(0.70);
        boosted_flow_util = boosted_flow_util
            .max((base_flow_util + 0.30).min(0.92))
            .min(0.92);
    } else if ample_time_for_aggressive_sizing && spent_ratio + 1e-9 < 0.20 {
        boosted_top_share = boosted_top_share
            .max((base_top_share + 0.15).min(0.65))
            .min(0.65);
        boosted_flow_util = boosted_flow_util
            .max((base_flow_util + 0.25).min(0.90))
            .min(0.90);
    } else if ample_time_for_aggressive_sizing && spent_ratio + 1e-9 < 0.35 {
        boosted_top_share = boosted_top_share
            .max((base_top_share + 0.10).min(0.60))
            .min(0.60);
        boosted_flow_util = boosted_flow_util
            .max((base_flow_util + 0.20).min(0.85))
            .min(0.85);
    }
    (boosted_top_share.max(0.0), boosted_flow_util.max(0.0))
}

fn compute_spent_pacing_gap_ratio(
    ledger: &Ledger,
    params: &StrategyParams,
    now_ts: u64,
) -> f64 {
    if params.total_budget_usdc <= 0.0 {
        return 0.0;
    }
    let spent_ratio = (ledger.spent_total_usdc / params.total_budget_usdc).clamp(0.0, 1.0);
    let total_window = ledger.end_ts.saturating_sub(ledger.start_ts).max(1);
    let opening_no_trade = params.opening_no_trade_secs.min(total_window.saturating_sub(1));
    let tradable_window = total_window
        .saturating_sub(params.tail_close_secs)
        .saturating_sub(opening_no_trade)
        .max(1);
    let elapsed = now_ts
        .saturating_sub(ledger.start_ts)
        .min(total_window)
        .saturating_sub(opening_no_trade)
        .min(tradable_window);
    let target_spent_ratio = (elapsed as f64 / tradable_window as f64).clamp(0.0, 1.0);
    (target_spent_ratio - spent_ratio).max(0.0)
}

fn effective_round_budget_usdc(
    ledger: &Ledger,
    params: &StrategyParams,
    time_left_secs: u64,
    pacing_gap_ratio: f64,
) -> f64 {
    let base = params.round_budget_usdc.max(0.0);
    if base <= 0.0 {
        return 0.0;
    }
    let remaining_total = (params.total_budget_usdc - ledger.spent_total_usdc).max(0.0);
    if remaining_total <= 0.0 {
        return 0.0;
    }
    let mut budget = base;
    let ample_time_for_catchup = time_left_secs > params.tail_close_secs.saturating_add(180);
    if ample_time_for_catchup && pacing_gap_ratio + 1e-9 >= 0.18 {
        budget *= 1.9;
    } else if ample_time_for_catchup && pacing_gap_ratio + 1e-9 >= 0.10 {
        budget *= 1.5;
    } else if ample_time_for_catchup {
        let spent_ratio = (ledger.spent_total_usdc / params.total_budget_usdc).clamp(0.0, 1.0);
        if spent_ratio + 1e-9 < 0.35 {
            budget *= 1.2;
        }
    }
    budget.min(remaining_total)
}

fn compute_slice_qty(total_qty: f64, params: &StrategyParams, slice_count: u64) -> Option<f64> {
    if total_qty <= 0.0 {
        return None;
    }
    let slice_count = slice_count.max(1) as f64;
    let base_slice = (total_qty / slice_count).ceil();
    let slice_qty = base_slice.max(params.round_min_slice_qty).min(total_qty);
    if slice_qty <= 0.0 {
        None
    } else {
        Some(slice_qty)
    }
}

fn estimate_timeout_flow_ratio_for_leg(
    ledger: &Ledger,
    leg1: TradeLeg,
    qty: f64,
    params: &StrategyParams,
) -> Option<f64> {
    if qty <= 0.0 {
        return None;
    }
    if params.maker_fill_horizon_secs == 0 {
        return None;
    }
    let top_size = match leg1 {
        TradeLeg::Up => ledger.best_bid_size_up,
        TradeLeg::Down => ledger.best_bid_size_down,
    }
    .unwrap_or(0.0)
    .max(0.0);
    let consume_rate = match leg1 {
        TradeLeg::Up => ledger.bid_consumption_rate_up,
        TradeLeg::Down => ledger.bid_consumption_rate_down,
    }
    .max(params.maker_flow_floor_per_sec)
    .max(0.0);
    let queue_ahead = top_size.max(qty) * params.maker_queue_ahead_mult.max(0.0);
    let denom = (queue_ahead + qty).max(1e-9);
    Some((consume_rate * params.maker_fill_horizon_secs as f64) / denom)
}

fn projected_pair_cost_after_pair_slice(ledger: &Ledger, qty: f64) -> Option<f64> {
    if qty <= 0.0 {
        return None;
    }
    let price_up = ledger.best_bid_up?;
    let price_down = ledger.best_bid_down?;
    if price_up <= 0.0 || price_down <= 0.0 {
        return None;
    }

    let buy_up_qty = qty;
    let buy_up_cost = qty * price_up;
    let buy_down_qty = qty;
    let buy_down_cost = qty * price_down;
    let new_qty_up = ledger.qty_up + buy_up_qty;
    let new_cost_up = ledger.cost_up + buy_up_cost;
    let new_qty_down = ledger.qty_down + buy_down_qty;
    let new_cost_down = ledger.cost_down + buy_down_cost;
    let new_avg_up = avg_cost(new_cost_up, new_qty_up)?;
    let new_avg_down = avg_cost(new_cost_down, new_qty_down)?;
    Some(new_avg_up + new_avg_down)
}

fn projected_pair_cost_with_worst_hedge(
    ledger: &Ledger,
    params: &StrategyParams,
    leg1: TradeLeg,
    qty: f64,
    hedge_mode: NoRiskEntryHedgePriceMode,
    ask_slippage_bps: f64,
    worst_hedge_bps: f64,
) -> Option<f64> {
    if qty <= 0.0 {
        return None;
    }
    let (leg1_best_bid, leg1_best_ask, opposite_best_ask) = match leg1 {
        TradeLeg::Up => (ledger.best_bid_up, ledger.best_ask_up, ledger.best_ask_down),
        TradeLeg::Down => (
            ledger.best_bid_down,
            ledger.best_ask_down,
            ledger.best_ask_up,
        ),
    };
    let p1 = compute_maker_buy_quote(
        leg1_best_bid,
        leg1_best_ask,
        opposite_best_ask,
        ledger.tick_size_current.max(1e-6),
        params.no_risk_hard_pair_cap,
        params.entry_dynamic_cap_enabled,
        params.entry_dynamic_cap_headroom_bps,
        params.entry_dynamic_cap_min_price,
        params.entry_dynamic_cap_apply_to_net_increase_only,
        params.entry_target_margin_min_ticks,
        params.entry_target_margin_min_bps,
        params.entry_max_passive_ticks_for_net_increase,
        true,
    )
    .final_price?;
    let p2_worst = match hedge_mode {
        NoRiskEntryHedgePriceMode::Ask => {
            let hedge_ask = match leg1 {
                TradeLeg::Up => ledger.best_ask_down?,
                TradeLeg::Down => ledger.best_ask_up?,
            };
            hedge_ask * (1.0 + ask_slippage_bps.max(0.0) / 10_000.0)
        }
        NoRiskEntryHedgePriceMode::BidPlusBps => {
            let hedge_bid = match leg1 {
                TradeLeg::Up => ledger.best_bid_down?,
                TradeLeg::Down => ledger.best_bid_up?,
            };
            hedge_bid * (1.0 + worst_hedge_bps.max(0.0) / 10_000.0)
        }
    };
    if p1 <= 0.0 || p2_worst <= 0.0 {
        return None;
    }
    let (new_qty_up, new_cost_up, new_qty_down, new_cost_down) = match leg1 {
        TradeLeg::Up => (
            ledger.qty_up + qty,
            ledger.cost_up + qty * p1,
            ledger.qty_down + qty,
            ledger.cost_down + qty * p2_worst,
        ),
        TradeLeg::Down => (
            ledger.qty_up + qty,
            ledger.cost_up + qty * p2_worst,
            ledger.qty_down + qty,
            ledger.cost_down + qty * p1,
        ),
    };
    let new_avg_up = avg_cost(new_cost_up, new_qty_up)?;
    let new_avg_down = avg_cost(new_cost_down, new_qty_down)?;
    Some(new_avg_up + new_avg_down)
}

fn reserve_price_for_worst_hedge(
    ledger: &Ledger,
    leg1: TradeLeg,
    hedge_mode: NoRiskEntryHedgePriceMode,
    ask_slippage_bps: f64,
    worst_hedge_bps: f64,
) -> Option<f64> {
    match hedge_mode {
        NoRiskEntryHedgePriceMode::Ask => {
            let hedge_ask = match leg1 {
                TradeLeg::Up => ledger.best_ask_down?,
                TradeLeg::Down => ledger.best_ask_up?,
            };
            Some(hedge_ask * (1.0 + ask_slippage_bps.max(0.0) / 10_000.0))
        }
        NoRiskEntryHedgePriceMode::BidPlusBps => {
            let hedge_bid = match leg1 {
                TradeLeg::Up => ledger.best_bid_down?,
                TradeLeg::Down => ledger.best_bid_up?,
            };
            Some(hedge_bid * (1.0 + worst_hedge_bps.max(0.0) / 10_000.0))
        }
    }
}

fn reversal_ok_for_leg(ledger: &Ledger, params: &StrategyParams, leg: TradeLeg) -> bool {
    if !params.reversal_entry_enabled {
        return true;
    }
    let (discount_bps, momentum_bps) = match leg {
        TradeLeg::Up => (ledger.mid_up_discount_bps, ledger.mid_up_momentum_bps),
        TradeLeg::Down => (ledger.mid_down_discount_bps, ledger.mid_down_momentum_bps),
    };
    discount_bps + 1e-9 >= params.reversal_min_discount_bps
        && momentum_bps + 1e-9 >= params.reversal_min_momentum_bps
}

fn turn_confirm_ok_for_leg(ledger: &Ledger, params: &StrategyParams, leg: TradeLeg) -> bool {
    if params.entry_turn_confirm_ticks == 0 || params.entry_turn_min_rebound_bps <= 0.0 {
        return true;
    }
    let (discount_bps, momentum_bps) = match leg {
        TradeLeg::Up => (ledger.mid_up_discount_bps, ledger.mid_up_momentum_bps),
        TradeLeg::Down => (ledger.mid_down_discount_bps, ledger.mid_down_momentum_bps),
    };
    let min_momentum_per_tick =
        params.entry_turn_min_rebound_bps / params.entry_turn_confirm_ticks.max(1) as f64;
    discount_bps + 1e-9 >= params.entry_turn_min_rebound_bps
        && momentum_bps + 1e-9 >= min_momentum_per_tick
}

fn turning_score_for_leg(ledger: &Ledger, params: &StrategyParams, leg: TradeLeg) -> f64 {
    let (discount_bps, momentum_bps) = match leg {
        TradeLeg::Up => (ledger.mid_up_discount_bps, ledger.mid_up_momentum_bps),
        TradeLeg::Down => (ledger.mid_down_discount_bps, ledger.mid_down_momentum_bps),
    };
    let rebound_target = params.entry_turn_min_rebound_bps.max(1e-9);
    let momentum_target = if params.entry_turn_confirm_ticks > 0 {
        (rebound_target / params.entry_turn_confirm_ticks as f64).max(1e-9)
    } else {
        rebound_target
    };
    let discount_good = (discount_bps / rebound_target).clamp(0.0, 1.0);
    let momentum_good = (momentum_bps / momentum_target).clamp(0.0, 1.0);
    (1.0 - (0.6 * discount_good + 0.4 * momentum_good)).clamp(0.0, 1.0)
}

fn entry_regime_score(
    ledger: &Ledger,
    params: &StrategyParams,
    leg: TradeLeg,
    reversal_ok: bool,
    turn_ok: bool,
    vol_entry_bps: f64,
) -> f64 {
    let vol_score = if params.vol_entry_min_bps <= 0.0 {
        1.0
    } else {
        (vol_entry_bps / params.vol_entry_min_bps).clamp(0.0, 2.0) / 2.0
    };
    let reversal_score = if reversal_ok { 1.0 } else { 0.0 };
    let turn_score = if turn_ok {
        (1.0 - turning_score_for_leg(ledger, params, leg)).clamp(0.0, 1.0)
    } else {
        0.0
    };
    (0.45 * vol_score + 0.35 * reversal_score + 0.20 * turn_score).clamp(0.0, 1.0)
}

#[derive(Debug, Clone)]
struct LegEntryEvaluation {
    leg: TradeLeg,
    qty_slice: Option<f64>,
    slice_count: Option<u32>,
    reserve_needed_usdc: Option<f64>,
    timeout_flow_ratio: Option<f64>,
    timeout_flow_ok: bool,
    fillability_ok: bool,
    entry_worst_pair_cost: Option<f64>,
    entry_worst_pair_ok: bool,
    entry_edge_bps: Option<f64>,
    pair_quality_ok: bool,
    pair_regression_ok: bool,
    entry_depth_cap_qty: Option<f64>,
    entry_flow_cap_qty: Option<f64>,
    regime_score: f64,
    score: Option<f64>,
}

fn entry_fallback_active(
    ledger: &Ledger,
    params: &StrategyParams,
    now_ts: u64,
    abs_net: f64,
) -> bool {
    let eps = 1e-9;
    params.entry_fallback_enabled
        && ledger.round_idx == 0
        && ledger.round_state == RoundPhase::Idle
        && abs_net <= eps
        && ledger.open_orders_active == 0
        && ledger.entry_fallback_until_ts > 0
        && now_ts <= ledger.entry_fallback_until_ts
}

pub fn build_round_plan(ledger: &Ledger, params: &StrategyParams, now_ts: u64) -> RoundPlan {
    let time_left_secs = ledger.time_left_secs(now_ts);
    let pacing_gap_ratio = compute_spent_pacing_gap_ratio(ledger, params, now_ts);
    let boost_active =
        utilization_boost_active(ledger, params, now_ts, time_left_secs, pacing_gap_ratio);
    let spent_ratio = if params.total_budget_usdc > 0.0 {
        (ledger.spent_total_usdc / params.total_budget_usdc).clamp(0.0, 1.0)
    } else {
        1.0
    };
    let (entry_max_top_book_share, entry_max_flow_utilization) =
        effective_entry_limits(
            params,
            boost_active,
            spent_ratio,
            time_left_secs,
            pacing_gap_ratio,
        );
    let round_budget_for_new_round =
        effective_round_budget_usdc(ledger, params, time_left_secs, pacing_gap_ratio);
    let abs_net = (ledger.qty_up - ledger.qty_down).abs();
    let eps = 1e-9;
    let fallback_active = entry_fallback_active(ledger, params, now_ts, abs_net);
    let fallback_trigger_reason = if fallback_active {
        Some("entry_worst_pair_streak".to_string())
    } else {
        None
    };
    let entry_hedge_mode = if fallback_active {
        params.entry_fallback_hedge_mode
    } else {
        params.no_risk_entry_hedge_price_mode
    };
    let entry_worst_hedge_bps = if fallback_active {
        params.entry_fallback_worst_hedge_bps
    } else {
        params.no_risk_entry_worst_hedge_bps
    };
    let budget_remaining_round = (params.round_budget_usdc - ledger.spent_round_usdc).max(0.0);
    let budget_remaining_total = (params.total_budget_usdc - ledger.spent_total_usdc).max(0.0);
    let before_market_start = now_ts < ledger.start_ts;
    let vol_entry_bps = ledger.pair_mid_vol_bps;
    let vol_entry_ok = params.vol_entry_min_bps <= 0.0 || vol_entry_bps >= params.vol_entry_min_bps;
    let reversal_up_ok = reversal_ok_for_leg(ledger, params, TradeLeg::Up);
    let reversal_down_ok = reversal_ok_for_leg(ledger, params, TradeLeg::Down);
    let turn_up_ok = turn_confirm_ok_for_leg(ledger, params, TradeLeg::Up);
    let turn_down_ok = turn_confirm_ok_for_leg(ledger, params, TradeLeg::Down);
    let turn_gate_enabled =
        params.entry_turn_confirm_ticks > 0 && params.entry_turn_min_rebound_bps > 0.0;
    let new_round_cutoff_secs = params
        .tail_close_secs
        .saturating_add(params.round_pair_wait_secs)
        .saturating_add(params.no_risk_late_new_round_buffer_secs);
    let late_new_round_blocked =
        params.no_risk_require_completable_round && time_left_secs <= new_round_cutoff_secs;

    // Warm-up mode: before the window starts we keep websocket/book sync running
    // but do not emit any trading actions.
    if before_market_start {
        return RoundPlan {
            phase: ledger.round_state,
            planned_leg1: None,
            qty_target: None,
            balance_leg: None,
            balance_qty: None,
            can_start_new_round: false,
            budget_remaining_round,
            budget_remaining_total,
            reserve_needed_usdc: None,
            vol_entry_bps,
            vol_entry_ok,
            reversal_up_ok,
            reversal_down_ok,
            turn_up_ok,
            turn_down_ok,
            first_leg_turning_score: None,
            entry_worst_pair_cost: None,
            entry_worst_pair_ok: false,
            entry_timeout_flow_ratio: None,
            entry_timeout_flow_ok: false,
            entry_fillability_ok: false,
            entry_edge_bps: None,
            entry_regime_score: None,
            entry_depth_cap_qty: None,
            entry_flow_cap_qty: None,
            slice_count_planned: None,
            slice_qty_current: None,
            entry_final_qty_slice: None,
            entry_fallback_active: fallback_active,
            entry_fallback_armed: fallback_active,
            entry_fallback_trigger_reason: fallback_trigger_reason.clone(),
            entry_fallback_blocked_by_recoverability: false,
            new_round_cutoff_secs,
            late_new_round_blocked,
            pair_quality_ok: false,
            pair_regression_ok: false,
            can_open_round_base_ok: false,
            can_start_block_reason: Some("before_start".to_string()),
        };
    }

    if ledger.round_state == RoundPhase::Leg1Accumulating {
        let remaining = (ledger.round_leg1_target_qty - ledger.round_leg1_filled_qty).max(0.0);
        let slice_count = ledger
            .round_leg1
            .map(|leg| {
                compute_dynamic_slice_count(
                    ledger,
                    params,
                    leg,
                    ledger.round_leg1_target_qty,
                    time_left_secs,
                    pacing_gap_ratio,
                    entry_max_top_book_share,
                    entry_max_flow_utilization,
                )
            })
            .unwrap_or_else(|| params.round_slice_count.max(1));
        let slice_qty = compute_slice_qty(ledger.round_leg1_target_qty, params, slice_count)
            .map(|v| v.min(remaining))
            .filter(|v| *v > eps);
        return RoundPlan {
            phase: ledger.round_state,
            planned_leg1: ledger.round_leg1,
            qty_target: slice_qty,
            balance_leg: None,
            balance_qty: None,
            can_start_new_round: false,
            budget_remaining_round,
            budget_remaining_total,
            reserve_needed_usdc: None,
            vol_entry_bps,
            vol_entry_ok,
            reversal_up_ok,
            reversal_down_ok,
            turn_up_ok,
            turn_down_ok,
            first_leg_turning_score: None,
            entry_worst_pair_cost: None,
            entry_worst_pair_ok: false,
            entry_timeout_flow_ratio: None,
            entry_timeout_flow_ok: false,
            entry_fillability_ok: false,
            entry_edge_bps: None,
            entry_regime_score: None,
            entry_depth_cap_qty: None,
            entry_flow_cap_qty: None,
            slice_count_planned: Some(slice_count as u32),
            slice_qty_current: slice_qty,
            entry_final_qty_slice: None,
            entry_fallback_active: fallback_active,
            entry_fallback_armed: fallback_active,
            entry_fallback_trigger_reason: fallback_trigger_reason.clone(),
            entry_fallback_blocked_by_recoverability: false,
            new_round_cutoff_secs,
            late_new_round_blocked,
            pair_quality_ok: false,
            pair_regression_ok: false,
            can_open_round_base_ok: false,
            can_start_block_reason: Some("leg1_accumulating".to_string()),
        };
    }
    if ledger.round_state == RoundPhase::Leg2Balancing {
        let leg2 = match ledger.round_leg1 {
            Some(TradeLeg::Up) => Some(TradeLeg::Down),
            Some(TradeLeg::Down) => Some(TradeLeg::Up),
            None => None,
        };
        let leg2_price = match leg2 {
            Some(TradeLeg::Up) => ledger.best_bid_up,
            Some(TradeLeg::Down) => ledger.best_bid_down,
            None => None,
        };
        let affordable_qty = if let Some(price) = leg2_price {
            if price > 0.0 {
                (budget_remaining_round.min(budget_remaining_total) / price).max(0.0)
            } else {
                0.0
            }
        } else {
            0.0
        };
        let remaining = (ledger.round_leg2_target_qty - ledger.round_leg2_filled_qty)
            .max(0.0)
            .min(affordable_qty);
        let slice_count = leg2
            .map(|leg| {
                compute_dynamic_slice_count(
                    ledger,
                    params,
                    leg,
                    remaining,
                    time_left_secs,
                    pacing_gap_ratio,
                    entry_max_top_book_share,
                    entry_max_flow_utilization,
                )
            })
            .unwrap_or_else(|| params.round_slice_count.max(1));
        let slice_qty = compute_slice_qty(remaining, params, slice_count)
            .map(|v| v.min(remaining))
            .filter(|v| *v > eps);
        return RoundPlan {
            phase: ledger.round_state,
            planned_leg1: None,
            qty_target: None,
            balance_leg: leg2,
            balance_qty: slice_qty,
            can_start_new_round: false,
            budget_remaining_round,
            budget_remaining_total,
            reserve_needed_usdc: None,
            vol_entry_bps,
            vol_entry_ok,
            reversal_up_ok,
            reversal_down_ok,
            turn_up_ok,
            turn_down_ok,
            first_leg_turning_score: None,
            entry_worst_pair_cost: None,
            entry_worst_pair_ok: false,
            entry_timeout_flow_ratio: None,
            entry_timeout_flow_ok: false,
            entry_fillability_ok: false,
            entry_edge_bps: None,
            entry_regime_score: None,
            entry_depth_cap_qty: None,
            entry_flow_cap_qty: None,
            slice_count_planned: Some(slice_count as u32),
            slice_qty_current: slice_qty,
            entry_final_qty_slice: None,
            entry_fallback_active: fallback_active,
            entry_fallback_armed: fallback_active,
            entry_fallback_trigger_reason: fallback_trigger_reason.clone(),
            entry_fallback_blocked_by_recoverability: false,
            new_round_cutoff_secs,
            late_new_round_blocked,
            pair_quality_ok: false,
            pair_regression_ok: false,
            can_open_round_base_ok: false,
            can_start_block_reason: Some("leg2_balancing".to_string()),
        };
    }

    let round_gap_ok = params.round_min_start_gap_secs == 0
        || ledger.last_round_complete_ts == 0
        || now_ts
            >= ledger
                .last_round_complete_ts
                .saturating_add(params.round_min_start_gap_secs);
    let effective_max_rounds = params
        .effective_max_rounds(ledger.spent_total_usdc, time_left_secs)
        .max(ledger.round_idx.saturating_add(1));
    let entry_pair_limit = params
        .effective_pair_limit(false)
        .min((1.0 - params.entry_pair_buffer_bps / 10_000.0).max(0.0));
    let carry_pair_ok = ledger
        .pair_cost()
        .map(|pc| pc <= entry_pair_limit + eps)
        .unwrap_or(true);

    let can_open_round_base_reason = if ledger.round_state != RoundPhase::Idle {
        Some("phase_not_idle")
    } else if abs_net > eps {
        Some("abs_net_nonzero")
    } else if time_left_secs <= params.tail_close_secs && ledger.round_idx >= effective_max_rounds {
        Some("max_rounds")
    } else if time_left_secs <= params.tail_close_secs {
        Some("tail_close")
    } else if late_new_round_blocked {
        Some("late_new_round")
    } else if !round_gap_ok {
        Some("round_start_gap")
    } else if !vol_entry_ok {
        Some("vol_entry")
    } else if !carry_pair_ok {
        Some("carry_pair")
    } else {
        None
    };
    let can_open_round_base_ok = can_open_round_base_reason.is_none();
    let can_open_round = can_open_round_base_ok;

    if can_open_round && ledger.best_bid_up.is_some() && ledger.best_bid_down.is_some() {
        let directional_gate_enabled = params.reversal_entry_enabled || turn_gate_enabled;
        let leg_gate_ok = |leg: TradeLeg| match leg {
            TradeLeg::Up => reversal_up_ok && (!turn_gate_enabled || turn_up_ok),
            TradeLeg::Down => reversal_down_ok && (!turn_gate_enabled || turn_down_ok),
        };

        let evaluate_leg = |leg: TradeLeg| -> LegEntryEvaluation {
            let (price1, price2, reversal_ok, turn_ok) = match leg {
                TradeLeg::Up => (
                    ledger.best_bid_up.unwrap_or(0.0),
                    ledger.best_bid_down.unwrap_or(0.0),
                    reversal_up_ok,
                    turn_up_ok,
                ),
                TradeLeg::Down => (
                    ledger.best_bid_down.unwrap_or(0.0),
                    ledger.best_bid_up.unwrap_or(0.0),
                    reversal_down_ok,
                    turn_down_ok,
                ),
            };
            let qty_target_raw = compute_round_qty_target(
                price1,
                price2,
                round_budget_for_new_round,
                params.round_leg1_fraction,
            );
            let entry_depth_cap_qty = qty_target_raw
                .and_then(|qty| {
                    cap_round_qty_by_visible_depth(
                        ledger,
                        leg,
                        qty,
                        entry_max_top_book_share,
                    )
                });
            let entry_flow_cap_qty = entry_depth_cap_qty
                .and_then(|qty| {
                    cap_round_qty_by_expected_flow(
                        ledger,
                        leg,
                        qty,
                        params,
                        entry_max_flow_utilization,
                    )
                });
            let qty_target = entry_flow_cap_qty;
            let slice_count =
                qty_target.map(|qty| {
                    compute_dynamic_slice_count(
                        ledger,
                        params,
                        leg,
                        qty,
                        time_left_secs,
                        pacing_gap_ratio,
                        entry_max_top_book_share,
                        entry_max_flow_utilization,
                    )
                });
            let qty_slice = qty_target
                .and_then(|qty| {
                    let count = slice_count.unwrap_or_else(|| params.round_slice_count.max(1));
                    compute_slice_qty(qty, params, count)
                })
                .filter(|qty| *qty + eps >= params.round_min_slice_qty);

            let projected_pair_cost =
                qty_slice.and_then(|qty| projected_pair_cost_after_pair_slice(ledger, qty));
            let entry_worst_pair_cost = qty_slice.and_then(|qty| {
                projected_pair_cost_with_worst_hedge(
                    ledger,
                    params,
                    leg,
                    qty,
                    entry_hedge_mode,
                    params.no_risk_entry_ask_slippage_bps,
                    entry_worst_hedge_bps,
                )
            });
            let passive_gap_ticks = match leg {
                TradeLeg::Up => {
                    compute_maker_buy_quote(
                        ledger.best_bid_up,
                        ledger.best_ask_up,
                        ledger.best_ask_down,
                        ledger.tick_size_current.max(1e-6),
                        params.no_risk_hard_pair_cap,
                        params.entry_dynamic_cap_enabled,
                        params.entry_dynamic_cap_headroom_bps,
                        params.entry_dynamic_cap_min_price,
                        params.entry_dynamic_cap_apply_to_net_increase_only,
                        params.entry_target_margin_min_ticks,
                        params.entry_target_margin_min_bps,
                        params.entry_max_passive_ticks_for_net_increase,
                        true,
                    )
                    .passive_gap_ticks
                }
                TradeLeg::Down => {
                    compute_maker_buy_quote(
                        ledger.best_bid_down,
                        ledger.best_ask_down,
                        ledger.best_ask_up,
                        ledger.tick_size_current.max(1e-6),
                        params.no_risk_hard_pair_cap,
                        params.entry_dynamic_cap_enabled,
                        params.entry_dynamic_cap_headroom_bps,
                        params.entry_dynamic_cap_min_price,
                        params.entry_dynamic_cap_apply_to_net_increase_only,
                        params.entry_target_margin_min_ticks,
                        params.entry_target_margin_min_bps,
                        params.entry_max_passive_ticks_for_net_increase,
                        true,
                    )
                    .passive_gap_ticks
                }
            };
            let entry_worst_limit = (params.effective_pair_limit(false)
                - params.no_risk_entry_pair_headroom_bps / 10_000.0)
                .max(0.0);
            let entry_worst_pair_ok = entry_worst_pair_cost
                .map(|v| v <= entry_worst_limit + 1e-9)
                .unwrap_or(false);
            let entry_edge_bps = entry_worst_pair_cost
                .map(|v| ((entry_worst_limit - v) / entry_worst_limit.max(1e-9)) * 10_000.0);
            let entry_timeout_flow_ratio = qty_slice
                .and_then(|qty| estimate_timeout_flow_ratio_for_leg(ledger, leg, qty, params));
            let entry_timeout_flow_min = params
                .entry_min_timeout_flow_ratio
                .max((params.entry_fill_prob_min + 0.02).min(0.95));
            let entry_timeout_flow_ok = entry_timeout_flow_min <= 0.0
                || entry_timeout_flow_ratio
                    .map(|v| v + 1e-9 >= entry_timeout_flow_min)
                    .unwrap_or(false);
            let fill_prob_est =
                entry_timeout_flow_ratio.map(|v| (1.0 - (-v.max(0.0)).exp()).clamp(0.0, 1.0));
            let fill_prob_est = fill_prob_est.map(|base| {
                let decay_k = params.maker_fill_passive_decay_k.max(0.0);
                if decay_k <= 0.0 {
                    return base;
                }
                let pt = passive_gap_ticks.unwrap_or(0.0).max(0.0);
                (base * (-decay_k * pt).exp()).clamp(0.0, 1.0)
            });
            let fill_prob_floor = params.entry_fill_prob_min.max(params.open_min_fill_prob);
            let fill_prob_ok = fill_prob_floor <= 0.0
                || fill_prob_est
                    .map(|v| v + 1e-9 >= fill_prob_floor)
                    .unwrap_or(false);
            let passive_gap_ok = params.entry_passive_gap_soft_max_ticks <= 0.0
                || passive_gap_ticks
                    .map(|v| v <= params.entry_passive_gap_soft_max_ticks + 1e-9)
                    .unwrap_or(true);
            let fillability_ok = entry_timeout_flow_ok && fill_prob_ok && passive_gap_ok;
            let pair_quality_ok = projected_pair_cost
                .map(|v| v <= entry_pair_limit + 1e-9)
                .unwrap_or(false);
            let pair_regression_ok = match params.entry_pair_regression_mode {
                EntryPairRegressionMode::StrictMonotonic => {
                    match (ledger.pair_cost(), projected_pair_cost) {
                        (Some(current), Some(next)) => next <= current + params.improve_min + 1e-9,
                        _ => true,
                    }
                }
                EntryPairRegressionMode::SoftBand => {
                    let soft_band =
                        (params.entry_pair_regression_soft_band_bps.max(0.0) / 10_000.0).max(0.0);
                    match (ledger.pair_cost(), projected_pair_cost) {
                        (Some(current), Some(next)) => next <= current + soft_band + 1e-9,
                        _ => true,
                    }
                }
                EntryPairRegressionMode::CapEdge => entry_edge_bps
                    .map(|v| v + 1e-9 >= params.entry_edge_min_bps)
                    .unwrap_or(false),
            };
            let reserve_needed_usdc = reserve_price_for_worst_hedge(
                ledger,
                leg,
                entry_hedge_mode,
                params.no_risk_entry_ask_slippage_bps,
                entry_worst_hedge_bps,
            )
            .and_then(|price| qty_slice.map(|qty| qty * price));
            let regime_score =
                entry_regime_score(ledger, params, leg, reversal_ok, turn_ok, vol_entry_bps);
            let turn_good = (1.0 - turning_score_for_leg(ledger, params, leg)).clamp(0.0, 1.0);
            let edge_component = entry_edge_bps.unwrap_or(-10_000.0) / 100.0;
            let fill_component = fill_prob_est.unwrap_or(0.0);
            let top_size = match leg {
                TradeLeg::Up => ledger.best_bid_size_up.unwrap_or(0.0),
                TradeLeg::Down => ledger.best_bid_size_down.unwrap_or(0.0),
            };
            let queue_penalty = if top_size > 0.0 {
                (1.0 / top_size).min(1.0)
            } else {
                1.0
            };
            let score = if qty_slice.is_some() {
                let passive_penalty = passive_gap_ticks.unwrap_or(0.0).max(0.0) * 0.35;
                Some(
                    1.8 * edge_component
                        + 1.0 * regime_score
                        + 0.5 * turn_good
                        + 0.8 * fill_component
                        - 0.6 * queue_penalty
                        - passive_penalty,
                )
            } else {
                None
            };

            LegEntryEvaluation {
                leg,
                qty_slice,
                slice_count: slice_count.map(|v| v as u32),
                reserve_needed_usdc,
                timeout_flow_ratio: entry_timeout_flow_ratio,
                timeout_flow_ok: entry_timeout_flow_ok,
                fillability_ok,
                entry_worst_pair_cost,
                entry_worst_pair_ok,
                entry_edge_bps,
                pair_quality_ok,
                pair_regression_ok,
                entry_depth_cap_qty,
                entry_flow_cap_qty,
                regime_score,
                score,
            }
        };

        let mut candidates = vec![evaluate_leg(TradeLeg::Up), evaluate_leg(TradeLeg::Down)];
        if directional_gate_enabled {
            let mut gated: Vec<LegEntryEvaluation> = candidates
                .iter()
                .cloned()
                .filter(|e| leg_gate_ok(e.leg))
                .collect();
            if gated.is_empty() && (ledger.round_idx > 0 || boost_active) {
                // After at least one completed round, allow an edge-based fallback so the
                // strategy can continue small rounds under hard no-risk when reversal/turn
                // signals are temporarily absent.
                gated = candidates
                    .iter()
                    .cloned()
                    .filter(|e| {
                        e.qty_slice.is_some()
                            && e.entry_worst_pair_ok
                            && e.entry_edge_bps
                                .map(|v| v + 1e-9 >= params.entry_edge_min_bps)
                                .unwrap_or(false)
                    })
                    .collect();
            }
            candidates = gated;
        }
        if candidates.is_empty() {
            return RoundPlan {
                phase: RoundPhase::Idle,
                planned_leg1: None,
                qty_target: None,
                balance_leg: None,
                balance_qty: None,
                can_start_new_round: false,
                budget_remaining_round,
                budget_remaining_total,
                reserve_needed_usdc: None,
                vol_entry_bps,
                vol_entry_ok,
                reversal_up_ok,
                reversal_down_ok,
                turn_up_ok,
                turn_down_ok,
                first_leg_turning_score: None,
                entry_worst_pair_cost: None,
                entry_worst_pair_ok: false,
                entry_timeout_flow_ratio: None,
                entry_timeout_flow_ok: false,
                entry_fillability_ok: false,
                entry_edge_bps: None,
                entry_regime_score: None,
                entry_depth_cap_qty: None,
                entry_flow_cap_qty: None,
                slice_count_planned: None,
                slice_qty_current: None,
                entry_final_qty_slice: None,
                entry_fallback_active: fallback_active,
                entry_fallback_armed: fallback_active,
                entry_fallback_trigger_reason: fallback_trigger_reason.clone(),
                entry_fallback_blocked_by_recoverability: false,
                new_round_cutoff_secs,
                late_new_round_blocked,
                pair_quality_ok: false,
                pair_regression_ok: false,
                can_open_round_base_ok,
                can_start_block_reason: Some("directional_gate".to_string()),
            };
        }

        candidates.sort_by(|a, b| {
            let ascore = a.score.unwrap_or(f64::NEG_INFINITY);
            let bscore = b.score.unwrap_or(f64::NEG_INFINITY);
            bscore
                .partial_cmp(&ascore)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let selected = candidates
            .first()
            .cloned()
            .unwrap_or_else(|| evaluate_leg(TradeLeg::Up));
        let hard_feasible = selected.qty_slice.is_some()
            && selected.entry_worst_pair_ok
            && selected.pair_quality_ok
            && selected.pair_regression_ok;
        let edge_ok = selected
            .entry_edge_bps
            .map(|v| v + 1e-9 >= params.entry_edge_min_bps)
            .unwrap_or(false);
        let can_start_new_round = selected.qty_slice.is_some()
            && budget_remaining_round > 0.0
            && budget_remaining_total > 0.0
            && hard_feasible
            && edge_ok
            && selected.fillability_ok
            && selected.regime_score > 0.0
            && (!params.no_risk_require_completable_round || selected.entry_worst_pair_ok);
        let can_start_block_reason = if can_start_new_round {
            None
        } else if !can_open_round_base_ok {
            can_open_round_base_reason.map(|v| v.to_string())
        } else if selected.qty_slice.is_none() {
            Some("qty_slice".to_string())
        } else if budget_remaining_round <= 0.0 {
            Some("round_budget".to_string())
        } else if budget_remaining_total <= 0.0 {
            Some("total_budget".to_string())
        } else if !selected.entry_worst_pair_ok {
            Some("entry_worst_pair".to_string())
        } else if !selected.pair_quality_ok {
            Some("pair_quality".to_string())
        } else if !selected.pair_regression_ok {
            Some("pair_regression".to_string())
        } else if !edge_ok {
            Some("entry_edge".to_string())
        } else if !selected.fillability_ok {
            Some("fillability".to_string())
        } else if selected.regime_score <= 0.0 {
            Some("regime_score".to_string())
        } else {
            Some("unknown".to_string())
        };

        return RoundPlan {
            phase: RoundPhase::Idle,
            planned_leg1: Some(selected.leg),
            qty_target: selected.qty_slice,
            balance_leg: None,
            balance_qty: None,
            can_start_new_round,
            budget_remaining_round,
            budget_remaining_total,
            reserve_needed_usdc: selected.reserve_needed_usdc,
            vol_entry_bps,
            vol_entry_ok,
            reversal_up_ok,
            reversal_down_ok,
            turn_up_ok,
            turn_down_ok,
            first_leg_turning_score: Some(turning_score_for_leg(ledger, params, selected.leg)),
            entry_worst_pair_cost: selected.entry_worst_pair_cost,
            entry_worst_pair_ok: selected.entry_worst_pair_ok,
            entry_timeout_flow_ratio: selected.timeout_flow_ratio,
            entry_timeout_flow_ok: selected.timeout_flow_ok,
            entry_fillability_ok: selected.fillability_ok,
            entry_edge_bps: selected.entry_edge_bps,
            entry_regime_score: Some(selected.regime_score),
            entry_depth_cap_qty: selected.entry_depth_cap_qty,
            entry_flow_cap_qty: selected.entry_flow_cap_qty,
            slice_count_planned: selected.slice_count,
            slice_qty_current: selected.qty_slice,
            entry_final_qty_slice: selected.qty_slice,
            entry_fallback_active: fallback_active,
            entry_fallback_armed: fallback_active,
            entry_fallback_trigger_reason: fallback_trigger_reason.clone(),
            entry_fallback_blocked_by_recoverability: fallback_active
                && !selected.entry_worst_pair_ok,
            new_round_cutoff_secs,
            late_new_round_blocked,
            pair_quality_ok: selected.pair_quality_ok,
            pair_regression_ok: selected.pair_regression_ok,
            can_open_round_base_ok,
            can_start_block_reason,
        };
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
            vol_entry_bps,
            vol_entry_ok,
            reversal_up_ok,
            reversal_down_ok,
            turn_up_ok,
            turn_down_ok,
            first_leg_turning_score: None,
            entry_worst_pair_cost: None,
            entry_worst_pair_ok: false,
            entry_timeout_flow_ratio: None,
            entry_timeout_flow_ok: false,
            entry_fillability_ok: false,
            entry_edge_bps: None,
            entry_regime_score: None,
            entry_depth_cap_qty: None,
            entry_flow_cap_qty: None,
            slice_count_planned: None,
            slice_qty_current: None,
            entry_final_qty_slice: None,
            entry_fallback_active: fallback_active,
            entry_fallback_armed: fallback_active,
            entry_fallback_trigger_reason: fallback_trigger_reason.clone(),
            entry_fallback_blocked_by_recoverability: false,
            new_round_cutoff_secs,
            late_new_round_blocked,
            pair_quality_ok: false,
            pair_regression_ok: false,
            can_open_round_base_ok: false,
            can_start_block_reason: Some("balance_required".to_string()),
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
        vol_entry_bps,
        vol_entry_ok,
        reversal_up_ok,
        reversal_down_ok,
        turn_up_ok,
        turn_down_ok,
        first_leg_turning_score: None,
        entry_worst_pair_cost: None,
        entry_worst_pair_ok: false,
        entry_timeout_flow_ratio: None,
        entry_timeout_flow_ok: false,
        entry_fillability_ok: false,
        entry_edge_bps: None,
        entry_regime_score: None,
        entry_depth_cap_qty: None,
        entry_flow_cap_qty: None,
        slice_count_planned: None,
        slice_qty_current: None,
        entry_final_qty_slice: None,
        entry_fallback_active: fallback_active,
        entry_fallback_armed: fallback_active,
        entry_fallback_trigger_reason: fallback_trigger_reason.clone(),
        entry_fallback_blocked_by_recoverability: false,
        new_round_cutoff_secs,
        late_new_round_blocked,
        pair_quality_ok: false,
        pair_regression_ok: false,
        can_open_round_base_ok,
        can_start_block_reason: can_open_round_base_reason
            .map(|v| v.to_string())
            .or_else(|| Some("missing_quotes".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::params::{NoRiskEntryHedgePriceMode, NoRiskPairLimitMode};
    use crate::strategy::state::{ApplyKind, DryRunMode, LockState};

    fn base_params() -> StrategyParams {
        StrategyParams {
            improve_min: 0.002,
            margin_target: 0.01,
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
            entry_turn_confirm_ticks: 0,
            entry_turn_min_rebound_bps: 0.0,
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
            entry_pair_regression_mode: EntryPairRegressionMode::StrictMonotonic,
            entry_pair_regression_soft_band_bps: 40.0,
            entry_edge_min_bps: 0.0,
            entry_fill_prob_min: 0.0,
            open_min_fill_prob: 0.0,
            open_margin_surplus_min: 0.001,
            inventory_skew_alpha_bps: 0.0,
            lock_allow_reopen_before_freeze: false,
            lock_reopen_max_rounds: 0,
            round_min_start_gap_secs: 0,
            opening_no_trade_secs: 0,
            min_apply_interval_ms: 0,
            maker_fill_estimator_enabled: true,
            maker_fill_horizon_secs: 8,
            maker_min_fill_prob: 0.0,
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

    fn base_ledger(start_ts: u64, now_ts: u64) -> Ledger {
        Ledger {
            market_slug: "btc-updown-15m-test".to_string(),
            series_slug: "btc-updown-15m".to_string(),
            market_select_mode: "fixed_market_slug".to_string(),
            up_id: "up".to_string(),
            down_id: "down".to_string(),
            tick_size: 0.01,
            tick_size_current: 0.01,
            tick_size_source: "gamma".to_string(),
            start_ts,
            end_ts: now_ts + 900,
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
            best_bid_up: Some(0.39),
            best_ask_up: Some(0.40),
            best_bid_down: Some(0.60),
            best_ask_down: Some(0.60),
            best_bid_size_up: Some(100.0),
            best_ask_size_up: Some(100.0),
            best_bid_size_down: Some(100.0),
            best_ask_size_down: Some(100.0),
            bid_consumption_rate_up: 1.0,
            ask_consumption_rate_up: 1.0,
            bid_consumption_rate_down: 1.0,
            ask_consumption_rate_down: 1.0,
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
            lock_state: LockState::Unlocked,
            locked_hedgeable: 0.0,
            locked_pair_cost: None,
            locked_at_ts_ms: 0,
        }
    }

    #[test]
    fn before_start_emits_no_actions() {
        let now_ts = 1_000;
        let ledger = base_ledger(now_ts + 60, now_ts);
        let params = base_params();
        let plan = build_round_plan(&ledger, &params, now_ts);
        assert!(!plan.can_start_new_round);
        assert!(plan.planned_leg1.is_none());
        assert!(plan.qty_target.is_none());
        assert!(plan.balance_leg.is_none());
        assert!(plan.balance_qty.is_none());
    }

    #[test]
    fn after_start_can_open_round() {
        let now_ts = 1_000;
        let ledger = base_ledger(now_ts - 60, now_ts);
        let params = base_params();
        let plan = build_round_plan(&ledger, &params, now_ts);
        assert!(plan.can_start_new_round);
        assert!(plan.planned_leg1.is_some());
        assert!(plan.qty_target.is_some());
    }

    #[test]
    fn low_vol_blocks_new_round_when_threshold_enabled() {
        let now_ts = 1_000;
        let ledger = base_ledger(now_ts - 60, now_ts);
        let mut params = base_params();
        params.vol_entry_min_bps = 5.0;
        let plan = build_round_plan(&ledger, &params, now_ts);
        assert!(!plan.can_start_new_round);
        assert!(!plan.vol_entry_ok);
    }

    #[test]
    fn high_vol_allows_new_round_when_threshold_enabled() {
        let now_ts = 1_000;
        let mut ledger = base_ledger(now_ts - 60, now_ts);
        ledger.pair_mid_vol_bps = 12.0;
        let mut params = base_params();
        params.vol_entry_min_bps = 5.0;
        let plan = build_round_plan(&ledger, &params, now_ts);
        assert!(plan.can_start_new_round);
        assert!(plan.vol_entry_ok);
    }

    #[test]
    fn reversal_gate_blocks_when_enabled_without_signal() {
        let now_ts = 1_000;
        let mut ledger = base_ledger(now_ts - 60, now_ts);
        ledger.pair_mid_vol_bps = 20.0;
        let mut params = base_params();
        params.reversal_entry_enabled = true;
        params.reversal_min_discount_bps = 20.0;
        params.reversal_min_momentum_bps = 5.0;
        let plan = build_round_plan(&ledger, &params, now_ts);
        assert!(!plan.can_start_new_round);
        assert!(!plan.reversal_up_ok);
        assert!(!plan.reversal_down_ok);
    }

    #[test]
    fn reversal_gate_allows_matching_leg_signal() {
        let now_ts = 1_000;
        let mut ledger = base_ledger(now_ts - 60, now_ts);
        ledger.pair_mid_vol_bps = 20.0;
        ledger.mid_up_discount_bps = 30.0;
        ledger.mid_up_momentum_bps = 8.0;
        ledger.mid_down_discount_bps = 0.0;
        ledger.mid_down_momentum_bps = -3.0;
        let mut params = base_params();
        params.reversal_entry_enabled = true;
        params.reversal_min_discount_bps = 20.0;
        params.reversal_min_momentum_bps = 5.0;
        let plan = build_round_plan(&ledger, &params, now_ts);
        assert!(plan.can_start_new_round);
        assert_eq!(plan.planned_leg1, Some(TradeLeg::Up));
        assert!(plan.reversal_up_ok);
        assert!(!plan.reversal_down_ok);
    }

    #[test]
    fn turn_confirm_gate_blocks_when_enabled_without_rebound() {
        let now_ts = 1_000;
        let mut ledger = base_ledger(now_ts - 60, now_ts);
        ledger.pair_mid_vol_bps = 20.0;
        ledger.mid_up_discount_bps = 3.0;
        ledger.mid_up_momentum_bps = 0.5;
        ledger.mid_down_discount_bps = 2.0;
        ledger.mid_down_momentum_bps = 0.3;
        let mut params = base_params();
        params.entry_turn_confirm_ticks = 4;
        params.entry_turn_min_rebound_bps = 8.0;
        let plan = build_round_plan(&ledger, &params, now_ts);
        assert!(!plan.can_start_new_round);
        assert!(!plan.turn_up_ok);
        assert!(!plan.turn_down_ok);
    }

    #[test]
    fn turn_confirm_gate_allows_when_rebound_and_momentum_met() {
        let now_ts = 1_000;
        let mut ledger = base_ledger(now_ts - 60, now_ts);
        ledger.pair_mid_vol_bps = 20.0;
        ledger.mid_up_discount_bps = 15.0;
        ledger.mid_up_momentum_bps = 4.0;
        ledger.mid_down_discount_bps = 2.0;
        ledger.mid_down_momentum_bps = 0.2;
        let mut params = base_params();
        params.entry_turn_confirm_ticks = 4;
        params.entry_turn_min_rebound_bps = 8.0;
        let plan = build_round_plan(&ledger, &params, now_ts);
        assert!(plan.can_start_new_round);
        assert_eq!(plan.planned_leg1, Some(TradeLeg::Up));
        assert!(plan.turn_up_ok);
        assert!(!plan.turn_down_ok);
    }

    #[test]
    fn unbalanced_inventory_prefers_balance_over_new_round() {
        let now_ts = 1_000;
        let mut ledger = base_ledger(now_ts - 60, now_ts);
        ledger.qty_up = 3.0;
        ledger.qty_down = 1.0;
        ledger.pair_mid_vol_bps = 20.0;
        let mut params = base_params();
        params.vol_entry_min_bps = 5.0;
        let plan = build_round_plan(&ledger, &params, now_ts);
        assert!(!plan.can_start_new_round);
        assert_eq!(plan.balance_leg, Some(TradeLeg::Down));
        assert_eq!(plan.balance_qty, Some(2.0));
    }

    #[test]
    fn entry_quality_blocks_round_when_projected_pair_cost_too_high() {
        let now_ts = 1_000;
        let mut ledger = base_ledger(now_ts - 60, now_ts);
        ledger.best_bid_up = Some(0.52);
        ledger.best_bid_down = Some(0.50);
        let mut params = base_params();
        params.margin_target = 0.01; // margin limit = 0.99
        let plan = build_round_plan(&ledger, &params, now_ts);
        assert!(!plan.can_start_new_round);
        assert!(plan.planned_leg1.is_some());
    }

    #[test]
    fn entry_quality_blocks_round_when_pair_cost_regresses() {
        let now_ts = 1_000;
        let mut ledger = base_ledger(now_ts - 60, now_ts);
        // Existing inventory has cheap pair cost.
        ledger.qty_up = 20.0;
        ledger.cost_up = 9.0; // avg 0.45
        ledger.qty_down = 20.0;
        ledger.cost_down = 9.2; // avg 0.46 => pair 0.91
                                // Current book is much worse.
        ledger.best_bid_up = Some(0.52);
        ledger.best_bid_down = Some(0.46);
        let mut params = base_params();
        params.margin_target = 0.0; // isolate regression gate
        params.improve_min = 0.002;
        let plan = build_round_plan(&ledger, &params, now_ts);
        assert!(!plan.can_start_new_round);
    }

    #[test]
    fn entry_worst_pair_blocks_new_round_when_worst_hedge_is_too_expensive() {
        let now_ts = 1_000;
        let mut ledger = base_ledger(now_ts - 60, now_ts);
        ledger.best_bid_up = Some(0.40);
        ledger.best_bid_down = Some(0.80);
        ledger.best_ask_down = Some(0.81);
        ledger.mid_up_discount_bps = 12.0;
        ledger.mid_up_momentum_bps = 3.0;
        ledger.mid_down_discount_bps = 0.0;
        ledger.mid_down_momentum_bps = 0.0;
        let mut params = base_params();
        params.entry_dynamic_cap_enabled = false;
        params.reversal_entry_enabled = true;
        params.reversal_min_discount_bps = 6.0;
        params.reversal_min_momentum_bps = 2.0;
        let plan = build_round_plan(&ledger, &params, now_ts);
        assert!(!plan.can_start_new_round);
        assert_eq!(plan.planned_leg1, Some(TradeLeg::Up));
        assert!(!plan.entry_worst_pair_ok);
        assert!(plan.entry_worst_pair_cost.unwrap_or(0.0) > params.no_risk_hard_pair_cap);
    }

    #[test]
    fn timeout_flow_ratio_gate_blocks_low_fillability_entry() {
        let now_ts = 1_000;
        let mut ledger = base_ledger(now_ts - 60, now_ts);
        ledger.best_bid_size_up = Some(2_000.0);
        ledger.bid_consumption_rate_up = 1.0;
        ledger.best_bid_up = Some(0.39);
        ledger.best_bid_down = Some(0.60);
        ledger.mid_up_discount_bps = 12.0;
        ledger.mid_up_momentum_bps = 3.0;
        ledger.mid_down_discount_bps = 0.0;
        ledger.mid_down_momentum_bps = 0.0;
        let mut params = base_params();
        params.entry_min_timeout_flow_ratio = 0.12;
        params.reversal_entry_enabled = true;
        params.reversal_min_discount_bps = 6.0;
        params.reversal_min_momentum_bps = 2.0;
        let plan = build_round_plan(&ledger, &params, now_ts);
        assert_eq!(plan.planned_leg1, Some(TradeLeg::Up));
        assert!(!plan.entry_timeout_flow_ok);
        assert!(!plan.entry_fillability_ok);
        assert!(!plan.can_start_new_round);
    }

    #[test]
    fn late_new_round_cutoff_blocks_new_opening_rounds() {
        let now_ts = 1_000;
        let mut ledger = base_ledger(now_ts - 60, now_ts);
        ledger.end_ts = now_ts + 300; // below cutoff (tail_close + wait + buffer = 305)
        let params = base_params();
        let plan = build_round_plan(&ledger, &params, now_ts);
        assert!(plan.late_new_round_blocked);
        assert_eq!(plan.new_round_cutoff_secs, 305);
        assert!(!plan.can_start_new_round);
    }

    #[test]
    fn entry_buffer_blocks_opening_when_projected_pair_cost_above_buffered_limit() {
        let now_ts = 1_000;
        let mut ledger = base_ledger(now_ts - 60, now_ts);
        // Equal bids give projected pair cost ~= 1.0.
        ledger.best_bid_up = Some(0.50);
        ledger.best_bid_down = Some(0.50);
        let mut params = base_params();
        params.entry_pair_buffer_bps = 50.0; // requires projected <= 0.995
        let plan = build_round_plan(&ledger, &params, now_ts);
        assert!(!plan.can_start_new_round);
        assert!(plan.planned_leg1.is_some());
    }

    #[test]
    fn carry_pair_cost_above_entry_limit_blocks_new_round() {
        let now_ts = 1_000;
        let mut ledger = base_ledger(now_ts - 60, now_ts);
        ledger.qty_up = 10.0;
        ledger.cost_up = 5.1; // avg 0.51
        ledger.qty_down = 10.0;
        ledger.cost_down = 4.95; // avg 0.495 => pair 1.005
        let mut params = base_params();
        params.entry_pair_buffer_bps = 0.0; // entry limit = 1.0
        let plan = build_round_plan(&ledger, &params, now_ts);
        assert!(!plan.can_start_new_round);
        assert!(plan.planned_leg1.is_none());
    }

    #[test]
    fn hard_cap_only_mode_does_not_apply_legacy_margin_tightening() {
        let now_ts = 1_000;
        let mut ledger = base_ledger(now_ts - 60, now_ts);
        ledger.best_bid_up = Some(0.496);
        ledger.best_ask_up = Some(0.496);
        ledger.best_bid_down = Some(0.496);
        ledger.best_ask_down = Some(0.496);

        let mut hard_mode = base_params();
        hard_mode.margin_target = 0.01;
        hard_mode.no_risk_pair_limit_mode = NoRiskPairLimitMode::HardCapOnly;
        let hard_plan = build_round_plan(&ledger, &hard_mode, now_ts);
        assert!(hard_plan.can_start_new_round);

        let mut legacy_mode = hard_mode.clone();
        legacy_mode.no_risk_pair_limit_mode = NoRiskPairLimitMode::LegacyMarginThenCap;
        let legacy_plan = build_round_plan(&ledger, &legacy_mode, now_ts);
        assert!(!legacy_plan.can_start_new_round);
    }

    #[test]
    fn ask_hedge_mode_blocks_entries_that_bid_plus_bps_mode_would_allow() {
        let now_ts = 1_000;
        let mut ledger = base_ledger(now_ts - 60, now_ts);
        ledger.best_bid_up = Some(0.40);
        ledger.best_ask_up = Some(0.41);
        ledger.best_bid_down = Some(0.50);
        ledger.best_ask_down = Some(0.70);
        ledger.mid_up_discount_bps = 12.0;
        ledger.mid_up_momentum_bps = 3.0;
        ledger.mid_down_discount_bps = 0.0;
        ledger.mid_down_momentum_bps = 0.0;

        let mut ask_mode = base_params();
        ask_mode.entry_dynamic_cap_enabled = false;
        ask_mode.no_risk_entry_hedge_price_mode = NoRiskEntryHedgePriceMode::Ask;
        ask_mode.reversal_entry_enabled = true;
        ask_mode.reversal_min_discount_bps = 6.0;
        ask_mode.reversal_min_momentum_bps = 2.0;
        let ask_plan = build_round_plan(&ledger, &ask_mode, now_ts);
        assert!(!ask_plan.can_start_new_round);
        assert!(!ask_plan.entry_worst_pair_ok);

        let mut bid_mode = ask_mode.clone();
        bid_mode.no_risk_entry_hedge_price_mode = NoRiskEntryHedgePriceMode::BidPlusBps;
        let bid_plan = build_round_plan(&ledger, &bid_mode, now_ts);
        assert!(bid_plan.can_start_new_round);
        assert!(bid_plan.entry_worst_pair_ok);
    }

    #[test]
    fn entry_worst_pair_uses_dynamic_quote_price_instead_of_raw_best_bid() {
        let now_ts = 1_000;
        let mut ledger = base_ledger(now_ts - 60, now_ts);
        // With raw best_bid + worst hedge this would look too expensive.
        ledger.best_bid_up = Some(0.50);
        ledger.best_ask_up = Some(0.51);
        ledger.best_bid_down = Some(0.50);
        ledger.best_ask_down = Some(0.50);
        ledger.mid_up_discount_bps = 12.0;
        ledger.mid_up_momentum_bps = 3.0;
        ledger.mid_down_discount_bps = 0.0;
        ledger.mid_down_momentum_bps = 0.0;

        let mut params = base_params();
        params.reversal_entry_enabled = true;
        params.reversal_min_discount_bps = 6.0;
        params.reversal_min_momentum_bps = 2.0;
        params.entry_max_passive_ticks_for_net_increase = 3.0;
        let plan = build_round_plan(&ledger, &params, now_ts);
        assert!(plan.entry_worst_pair_ok);
        assert!(plan.entry_worst_pair_cost.unwrap_or(1.0) <= params.no_risk_hard_pair_cap);
    }

    #[test]
    fn entry_worst_pair_denies_when_dynamic_quote_price_is_unavailable() {
        let now_ts = 1_000;
        let mut ledger = base_ledger(now_ts - 60, now_ts);
        ledger.best_bid_up = Some(0.50);
        ledger.best_ask_up = Some(0.51);
        ledger.best_bid_down = Some(0.50);
        // Forces dynamic cap price below min quote threshold.
        ledger.best_ask_down = Some(1.00);
        ledger.mid_up_discount_bps = 12.0;
        ledger.mid_up_momentum_bps = 3.0;
        ledger.mid_down_discount_bps = 0.0;
        ledger.mid_down_momentum_bps = 0.0;

        let mut params = base_params();
        params.reversal_entry_enabled = true;
        params.reversal_min_discount_bps = 6.0;
        params.reversal_min_momentum_bps = 2.0;
        let plan = build_round_plan(&ledger, &params, now_ts);
        assert!(!plan.entry_worst_pair_ok);
        assert!(!plan.can_start_new_round);
        assert!(plan.entry_worst_pair_cost.is_none());
    }

    #[test]
    fn cap_edge_mode_allows_new_round_even_if_pair_regresses_vs_current_position() {
        let now_ts = 1_000;
        let mut ledger = base_ledger(now_ts - 60, now_ts);
        // Existing book position has a very good pair, making strict monotonic hard to satisfy.
        ledger.qty_up = 20.0;
        ledger.cost_up = 8.4; // avg 0.42
        ledger.qty_down = 20.0;
        ledger.cost_down = 8.5; // avg 0.425 => pair 0.845
        // New entry is still inside hard-cap with positive edge.
        ledger.best_bid_up = Some(0.47);
        ledger.best_ask_up = Some(0.48);
        ledger.best_bid_down = Some(0.47);
        ledger.best_ask_down = Some(0.47);

        let mut params = base_params();
        params.entry_pair_regression_mode = EntryPairRegressionMode::CapEdge;
        params.entry_edge_min_bps = 20.0;
        let plan = build_round_plan(&ledger, &params, now_ts);

        assert!(plan.pair_quality_ok);
        assert!(plan.pair_regression_ok);
        assert!(plan.can_start_new_round);
    }

    #[test]
    fn strict_monotonic_mode_rejects_same_setup_when_pair_regresses_vs_current_position() {
        let now_ts = 1_000;
        let mut ledger = base_ledger(now_ts - 60, now_ts);
        ledger.qty_up = 20.0;
        ledger.cost_up = 8.4; // avg 0.42
        ledger.qty_down = 20.0;
        ledger.cost_down = 8.5; // avg 0.425 => pair 0.845
        ledger.best_bid_up = Some(0.47);
        ledger.best_ask_up = Some(0.48);
        ledger.best_bid_down = Some(0.47);
        ledger.best_ask_down = Some(0.47);

        let mut params = base_params();
        params.entry_pair_regression_mode = EntryPairRegressionMode::StrictMonotonic;
        params.entry_edge_min_bps = 20.0;
        let plan = build_round_plan(&ledger, &params, now_ts);

        assert!(!plan.pair_regression_ok);
        assert!(!plan.can_start_new_round);
        assert_eq!(plan.can_start_block_reason.as_deref(), Some("pair_regression"));
    }

    #[test]
    fn soft_band_mode_respects_band_threshold() {
        let now_ts = 1_000;
        let mut ledger = base_ledger(now_ts - 60, now_ts);
        ledger.qty_up = 20.0;
        ledger.cost_up = 8.4; // avg 0.42
        ledger.qty_down = 20.0;
        ledger.cost_down = 8.5; // avg 0.425 => pair 0.845
        ledger.best_bid_up = Some(0.47);
        ledger.best_ask_up = Some(0.48);
        ledger.best_bid_down = Some(0.47);
        ledger.best_ask_down = Some(0.47);

        let mut params = base_params();
        params.entry_pair_regression_mode = EntryPairRegressionMode::SoftBand;
        params.entry_edge_min_bps = 20.0;
        params.entry_pair_regression_soft_band_bps = 1_300.0; // 0.13, enough for ~0.94
        let allowed = build_round_plan(&ledger, &params, now_ts);
        assert!(allowed.pair_regression_ok);
        assert!(allowed.can_start_new_round);

        params.entry_pair_regression_soft_band_bps = 1.0; // 0.0001, too tight
        let blocked = build_round_plan(&ledger, &params, now_ts);
        assert!(!blocked.pair_regression_ok);
        assert!(!blocked.can_start_new_round);
    }

    #[test]
    fn max_rounds_six_allows_round_idx_one_to_open_new_round() {
        let now_ts = 1_000;
        let mut ledger = base_ledger(now_ts - 60, now_ts);
        ledger.round_idx = 1;
        let mut params = base_params();
        params.max_rounds = 6;
        let plan = build_round_plan(&ledger, &params, now_ts);
        assert!(plan.can_open_round_base_ok);
        assert!(plan.can_start_new_round);
    }

    #[test]
    fn dynamic_round_extension_allows_open_when_budget_utilization_is_low() {
        let now_ts = 1_000;
        let mut ledger = base_ledger(now_ts - 60, now_ts);
        ledger.round_idx = 6;
        ledger.spent_total_usdc = 2.0; // very low utilization vs total budget
        let mut params = base_params();
        params.max_rounds = 6;
        params.total_budget_usdc = 100.0;
        let plan = build_round_plan(&ledger, &params, now_ts);
        assert!(plan.can_open_round_base_ok);
        assert_ne!(plan.can_start_block_reason.as_deref(), Some("max_rounds"));
    }

    #[test]
    fn low_utilization_boost_relaxes_directional_gate_after_warmup() {
        let now_ts = 1_000;
        let mut ledger = base_ledger(now_ts - 180, now_ts);
        ledger.pair_mid_vol_bps = 20.0;
        ledger.mid_up_discount_bps = 0.0;
        ledger.mid_up_momentum_bps = 0.0;
        ledger.mid_down_discount_bps = 0.0;
        ledger.mid_down_momentum_bps = 0.0;
        ledger.spent_total_usdc = 2.0; // 2% utilization with total budget=100
        let mut params = base_params();
        params.total_budget_usdc = 100.0;
        params.reversal_entry_enabled = true;
        params.reversal_min_discount_bps = 20.0;
        params.reversal_min_momentum_bps = 5.0;
        let plan = build_round_plan(&ledger, &params, now_ts);
        assert!(plan.can_start_new_round);
        assert!(plan.planned_leg1.is_some());
    }

    #[test]
    fn low_utilization_boost_increases_entry_slice_capacity() {
        let mut params = base_params();
        params.total_budget_usdc = 100.0;
        params.round_budget_usdc = 22.0;
        params.round_dynamic_slicing_enabled = false;
        params.round_slice_count = 1;
        params.entry_max_top_book_share = 0.20;
        params.entry_max_flow_utilization = 0.35;

        let now_unboost = 1_000;
        let mut ledger_unboost = base_ledger(now_unboost - 60, now_unboost);
        ledger_unboost.best_bid_size_up = Some(11.0);
        ledger_unboost.best_bid_size_down = Some(11.0);
        ledger_unboost.bid_consumption_rate_up = 1.0;
        ledger_unboost.bid_consumption_rate_down = 1.0;
        ledger_unboost.spent_total_usdc = 2.0;
        let plan_unboost = build_round_plan(&ledger_unboost, &params, now_unboost);

        let now_boost = 1_240;
        let mut ledger_boost = base_ledger(now_boost - 180, now_boost);
        ledger_boost.best_bid_size_up = Some(11.0);
        ledger_boost.best_bid_size_down = Some(11.0);
        ledger_boost.bid_consumption_rate_up = 1.0;
        ledger_boost.bid_consumption_rate_down = 1.0;
        ledger_boost.spent_total_usdc = 2.0;
        let plan_boost = build_round_plan(&ledger_boost, &params, now_boost);

        assert!(plan_unboost.qty_target.is_some());
        assert!(plan_boost.qty_target.is_some());
        assert!(plan_boost.qty_target.unwrap_or(0.0) > plan_unboost.qty_target.unwrap_or(0.0));
    }
}

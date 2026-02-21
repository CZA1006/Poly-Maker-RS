use super::params::StrategyParams;
use super::state::{avg_cost, price_for_unhedged, Ledger, RoundPhase, TradeLeg, TradeSide};

#[derive(Debug, Clone, Copy)]
pub struct CandidateAction {
    pub name: &'static str,
    pub leg: TradeLeg,
    pub side: TradeSide,
    pub kind: super::state::TradeKind,
    pub qty: f64,
}

#[derive(Debug, Clone)]
pub struct SimResult {
    pub ok: bool,
    pub fill_qty: f64,
    pub fill_price: Option<f64>,
    pub maker_fill_prob: Option<f64>,
    pub maker_queue_ahead: Option<f64>,
    pub maker_expected_consumed: Option<f64>,
    pub maker_consumption_rate: Option<f64>,
    pub maker_horizon_secs: Option<f64>,
    pub fee_estimate: f64,
    pub spent_delta_usdc: f64,
    pub new_qty_up: f64,
    pub new_cost_up: f64,
    pub new_qty_down: f64,
    pub new_cost_down: f64,
    pub new_avg_up: Option<f64>,
    pub new_avg_down: Option<f64>,
    pub new_pair_cost: Option<f64>,
    pub new_hedgeable: f64,
    pub new_unhedged_up: f64,
    pub new_unhedged_down: f64,
    pub new_unhedged_value_up: f64,
    pub new_unhedged_value_down: f64,
    pub hedge_recoverable_now: Option<bool>,
    pub required_opp_avg_price_cap: Option<f64>,
    pub current_opp_best_ask: Option<f64>,
    pub required_hedge_qty: Option<f64>,
    pub hedge_margin_to_opp_ask: Option<f64>,
    pub hedge_margin_required: Option<f64>,
    pub hedge_margin_ok: Option<bool>,
    pub entry_quote_base_postonly_price: Option<f64>,
    pub entry_quote_dynamic_cap_price: Option<f64>,
    pub entry_quote_final_price: Option<f64>,
    pub entry_quote_cap_active: Option<bool>,
    pub entry_quote_cap_bind: Option<bool>,
    pub passive_gap_abs: Option<f64>,
    pub passive_gap_ticks: Option<f64>,
    pub improves_hedge: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct MakerBuyQuote {
    pub base_postonly_price: Option<f64>,
    pub dynamic_cap_price: Option<f64>,
    pub final_price: Option<f64>,
    pub cap_active: bool,
    pub cap_bind: bool,
    pub passive_gap_abs: Option<f64>,
    pub passive_gap_ticks: Option<f64>,
}

fn floor_to_tick(price: f64, tick: f64) -> Option<f64> {
    if !price.is_finite() || !tick.is_finite() || tick <= 0.0 {
        return None;
    }
    let ticks = (price / tick).floor();
    if !ticks.is_finite() {
        return None;
    }
    Some((ticks * tick).max(0.0))
}

pub fn compute_maker_buy_quote(
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    opposite_best_ask: Option<f64>,
    tick: f64,
    hard_pair_cap: f64,
    dynamic_cap_enabled: bool,
    dynamic_cap_headroom_bps: f64,
    dynamic_cap_min_price: f64,
    dynamic_cap_apply_to_net_increase_only: bool,
    entry_target_margin_min_ticks: f64,
    entry_target_margin_min_bps: f64,
    _entry_max_passive_ticks_for_net_increase: f64,
    projected_net_increase: bool,
) -> MakerBuyQuote {
    let base_postonly_price = match (best_bid, best_ask) {
        (Some(bid), Some(ask)) => postonly_buy_price(bid, ask, tick).or(Some(bid)),
        _ => best_bid,
    };
    let cap_requested =
        dynamic_cap_enabled && (!dynamic_cap_apply_to_net_increase_only || projected_net_increase);
    let dynamic_cap_price = if cap_requested {
        opposite_best_ask
            .map(|opp_ask| {
                let target_margin = (entry_target_margin_min_ticks.max(0.0) * tick.max(1e-9))
                    .max(opp_ask * entry_target_margin_min_bps.max(0.0) / 10_000.0);
                hard_pair_cap
                    - dynamic_cap_headroom_bps.max(0.0) / 10_000.0
                    - opp_ask
                    - target_margin
            })
            .and_then(|price| floor_to_tick(price, tick))
    } else {
        None
    };
    let mut final_price = base_postonly_price;
    if let Some(cap_price) = dynamic_cap_price {
        final_price = match final_price {
            Some(base_price) => Some(base_price.min(cap_price)),
            None => Some(cap_price),
        };
    }
    if let Some(price) = final_price {
        if !price.is_finite() || price < dynamic_cap_min_price.max(1e-9) {
            final_price = None;
        }
    }
    let cap_active = cap_requested && dynamic_cap_price.is_some();
    let cap_bind = match (base_postonly_price, dynamic_cap_price, final_price) {
        (Some(base), Some(cap), Some(final_px)) => {
            cap < base - 1e-9 && (final_px - cap).abs() <= tick.max(1e-6)
        }
        _ => false,
    };
    let passive_gap_abs = match (base_postonly_price, final_price) {
        (Some(base), Some(final_px)) => Some((base - final_px).max(0.0)),
        _ => None,
    };
    let passive_gap_ticks = passive_gap_abs.map(|v| v / tick.max(1e-9));
    MakerBuyQuote {
        base_postonly_price,
        dynamic_cap_price,
        final_price,
        cap_active,
        cap_bind,
        passive_gap_abs,
        passive_gap_ticks,
    }
}

fn evaluate_hedge_recoverability(
    ledger: &Ledger,
    params: &StrategyParams,
    new_qty_up: f64,
    new_cost_up: f64,
    new_qty_down: f64,
    new_cost_down: f64,
) -> (Option<bool>, Option<f64>, Option<f64>, Option<f64>) {
    let net_before = (ledger.qty_up - ledger.qty_down).abs();
    let net_after = (new_qty_up - new_qty_down).abs();
    if net_after <= net_before + 1e-9 {
        return (None, None, None, None);
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

    (
        Some(hedge_recoverable_now),
        Some(required_opp_avg_price_cap),
        current_opp_best_ask,
        Some(required_hedge_qty),
    )
}

fn compute_round_target_qty_for_leg1(
    ledger: &Ledger,
    leg1: TradeLeg,
    params: &StrategyParams,
) -> Option<f64> {
    let (price_leg1, price_leg2) = match leg1 {
        TradeLeg::Up => (ledger.best_bid_up?, ledger.best_bid_down?),
        TradeLeg::Down => (ledger.best_bid_down?, ledger.best_bid_up?),
    };
    if price_leg1 <= 0.0 || price_leg2 <= 0.0 || params.round_budget_usdc <= 0.0 {
        return None;
    }
    let leg1_budget = params.round_budget_usdc * params.round_leg1_fraction;
    let mut qty = (leg1_budget / price_leg1).floor();
    let max_pair_qty = (params.round_budget_usdc / (price_leg1 + price_leg2)).floor();
    if max_pair_qty < qty {
        qty = max_pair_qty;
    }
    if qty <= 0.0 {
        None
    } else {
        let top_size = match leg1 {
            TradeLeg::Up => ledger.best_bid_size_up,
            TradeLeg::Down => ledger.best_bid_size_down,
        };
        if params.entry_max_top_book_share <= 0.0 {
            return Some(qty);
        }
        let qty = if let Some(top_size) = top_size {
            if top_size <= 0.0 {
                return None;
            }
            let depth_cap = (top_size * params.entry_max_top_book_share).floor();
            if depth_cap <= 0.0 {
                return None;
            }
            qty.min(depth_cap)
        } else {
            qty
        };
        if params.entry_max_flow_utilization <= 0.0 || params.maker_fill_horizon_secs == 0 {
            return Some(qty);
        }
        let consume_rate = match leg1 {
            TradeLeg::Up => ledger.bid_consumption_rate_up,
            TradeLeg::Down => ledger.bid_consumption_rate_down,
        }
        .max(params.maker_flow_floor_per_sec);
        let flow_cap = (consume_rate
            * params.maker_fill_horizon_secs as f64
            * params.entry_max_flow_utilization)
            .floor();
        if flow_cap <= 0.0 {
            return None;
        }
        Some(qty.min(flow_cap))
    }
}

pub fn simulate_trade(
    ledger: &Ledger,
    action: &CandidateAction,
    params: &StrategyParams,
    now_ts: u64,
) -> SimResult {
    let (best_bid, best_ask) = match action.leg {
        TradeLeg::Up => (ledger.best_bid_up, ledger.best_ask_up),
        TradeLeg::Down => (ledger.best_bid_down, ledger.best_ask_down),
    };
    let tick = ledger.tick_size_current.max(1e-6);
    let current_net = ledger.qty_up - ledger.qty_down;
    let projected_net_after = match (action.side, action.leg) {
        (TradeSide::Buy, TradeLeg::Up) => current_net + action.qty,
        (TradeSide::Buy, TradeLeg::Down) => current_net - action.qty,
        _ => current_net,
    };
    let projected_net_increase = projected_net_after.abs() > current_net.abs() + 1e-9;
    let opposite_best_ask = match action.leg {
        TradeLeg::Up => ledger.best_ask_down,
        TradeLeg::Down => ledger.best_ask_up,
    };
    let maker_buy_quote =
        if action.side == TradeSide::Buy && action.kind == super::state::TradeKind::Maker {
            Some(compute_maker_buy_quote(
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
            ))
        } else {
            None
        };
    let fill_price = match (action.side, action.kind) {
        (TradeSide::Buy, super::state::TradeKind::Taker) => best_ask,
        (TradeSide::Buy, super::state::TradeKind::Maker) => {
            maker_buy_quote.and_then(|q| q.final_price)
        }
        (TradeSide::Sell, super::state::TradeKind::Taker) => best_bid,
        (TradeSide::Sell, super::state::TradeKind::Maker) => match (best_bid, best_ask) {
            (Some(bid), Some(ask)) => postonly_sell_price(bid, ask, tick).or(Some(ask)),
            _ => best_ask,
        },
    };

    let mut new_qty_up = ledger.qty_up;
    let mut new_cost_up = ledger.cost_up;
    let mut new_qty_down = ledger.qty_down;
    let mut new_cost_down = ledger.cost_down;

    let mut ok = true;
    let mut fill_qty = action.qty;
    let fill_price_out = fill_price;
    let mut maker_fill_prob = None;
    let mut maker_queue_ahead = None;
    let mut maker_expected_consumed = None;
    let mut maker_consumption_rate = None;
    let mut maker_horizon_secs = None;

    if fill_price_out.is_none() {
        ok = false;
        fill_qty = 0.0;
    }

    if ok
        && params.maker_fill_estimator_enabled
        && action.kind == super::state::TradeKind::Maker
        && action.qty > 0.0
    {
        let (queue_size, observed_rate) = match (action.leg, action.side) {
            (TradeLeg::Up, TradeSide::Buy) => (
                ledger.best_bid_size_up.unwrap_or(0.0),
                ledger.bid_consumption_rate_up,
            ),
            (TradeLeg::Up, TradeSide::Sell) => (
                ledger.best_ask_size_up.unwrap_or(0.0),
                ledger.ask_consumption_rate_up,
            ),
            (TradeLeg::Down, TradeSide::Buy) => (
                ledger.best_bid_size_down.unwrap_or(0.0),
                ledger.bid_consumption_rate_down,
            ),
            (TradeLeg::Down, TradeSide::Sell) => (
                ledger.best_ask_size_down.unwrap_or(0.0),
                ledger.ask_consumption_rate_down,
            ),
        };
        let horizon_secs = ledger
            .time_left_secs(now_ts)
            .min(params.maker_fill_horizon_secs)
            .max(1) as f64;
        let raw_queue_ahead = (queue_size * params.maker_queue_ahead_mult).max(0.0);
        let improves_top = match (action.side, fill_price_out, best_bid, best_ask) {
            (TradeSide::Buy, Some(price), Some(bid), Some(ask)) => {
                price > bid + tick * 0.5 && price < ask - tick * 0.5
            }
            (TradeSide::Sell, Some(price), Some(bid), Some(ask)) => {
                price + tick * 0.5 < ask && price > bid + tick * 0.5
            }
            _ => false,
        };
        let queue_ahead = if improves_top { 0.0 } else { raw_queue_ahead };
        let passive_ticks = match (action.side, fill_price_out, best_bid, best_ask) {
            (TradeSide::Buy, Some(price), Some(bid), Some(_ask)) if price < bid - tick * 0.5 => {
                ((bid - price) / tick.max(1e-9)).max(0.0)
            }
            (TradeSide::Sell, Some(price), Some(_bid), Some(ask)) if price > ask + tick * 0.5 => {
                ((price - ask) / tick.max(1e-9)).max(0.0)
            }
            _ => 0.0,
        };
        let effective_queue_ahead = queue_ahead
            * (1.0 + passive_ticks * params.maker_fill_passive_queue_penalty_per_tick.max(0.0));
        let consume_rate = observed_rate.max(params.maker_flow_floor_per_sec);
        let expected_consumed = consume_rate * horizon_secs;
        // Smooth fill curve with diminishing returns as expected flow exceeds queue.
        let queue_and_order = effective_queue_ahead + action.qty.max(1e-9);
        let flow_ratio = (expected_consumed / queue_and_order).max(0.0);
        let mut fill_prob = (1.0 - (-flow_ratio).exp()).clamp(0.0, 1.0);
        let decay_k = params.maker_fill_passive_decay_k.max(0.0);
        if decay_k > 0.0 && passive_ticks > 0.0 {
            fill_prob *= (-decay_k * passive_ticks).exp();
            fill_prob = fill_prob.clamp(0.0, 1.0);
        }

        maker_fill_prob = Some(fill_prob);
        maker_queue_ahead = Some(effective_queue_ahead);
        maker_expected_consumed = Some(expected_consumed);
        maker_consumption_rate = Some(consume_rate);
        maker_horizon_secs = Some(horizon_secs);
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
    ) = evaluate_hedge_recoverability(
        ledger,
        params,
        new_qty_up,
        new_cost_up,
        new_qty_down,
        new_cost_down,
    );
    let (hedge_margin_to_opp_ask, hedge_margin_required, hedge_margin_ok) =
        match (required_opp_avg_price_cap, current_opp_best_ask) {
            (Some(required_cap), Some(opp_ask))
                if required_cap.is_finite() && opp_ask.is_finite() && opp_ask >= 0.0 =>
            {
                let margin_to_opp_ask = required_cap - opp_ask;
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
        maker_fill_prob,
        maker_queue_ahead,
        maker_expected_consumed,
        maker_consumption_rate,
        maker_horizon_secs,
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
        entry_quote_base_postonly_price: maker_buy_quote.and_then(|q| q.base_postonly_price),
        entry_quote_dynamic_cap_price: maker_buy_quote.and_then(|q| q.dynamic_cap_price),
        entry_quote_final_price: maker_buy_quote.and_then(|q| q.final_price),
        entry_quote_cap_active: maker_buy_quote.map(|q| q.cap_active),
        entry_quote_cap_bind: maker_buy_quote.map(|q| q.cap_bind),
        passive_gap_abs: maker_buy_quote.and_then(|q| q.passive_gap_abs),
        passive_gap_ticks: maker_buy_quote.and_then(|q| q.passive_gap_ticks),
        improves_hedge,
    }
}

pub fn postonly_buy_price(best_bid: f64, best_ask: f64, tick: f64) -> Option<f64> {
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

pub fn apply_simulated_trade(
    ledger: &mut Ledger,
    action: &CandidateAction,
    sim: &SimResult,
    params: &StrategyParams,
    now_ts: u64,
) {
    if !sim.ok || sim.fill_qty <= 1e-9 {
        return;
    }
    let pre_abs_net = (ledger.qty_up - ledger.qty_down).abs();
    let now_ms = now_ts.saturating_mul(1000);
    ledger.qty_up = sim.new_qty_up;
    ledger.cost_up = sim.new_cost_up;
    ledger.qty_down = sim.new_qty_down;
    ledger.cost_down = sim.new_cost_down;
    if sim.spent_delta_usdc > 0.0 {
        ledger.spent_total_usdc += sim.spent_delta_usdc;
        ledger.spent_round_usdc += sim.spent_delta_usdc;
    }
    ledger.last_apply_ts_ms = now_ms;

    let time_left_secs = ledger.time_left_secs(now_ts);
    let effective_max_rounds = params
        .effective_max_rounds(ledger.spent_total_usdc, time_left_secs)
        .max(ledger.round_idx.saturating_add(1));
    let eps = 1e-9;
    match ledger.round_state {
        RoundPhase::Idle => {
            if time_left_secs > params.tail_close_secs
                && ledger.round_idx < effective_max_rounds
                && action.side == TradeSide::Buy
                && pre_abs_net <= eps
            {
                let target_qty = compute_round_target_qty_for_leg1(ledger, action.leg, params)
                    .unwrap_or(action.qty)
                    .max(action.qty);
                ledger.round_state = RoundPhase::Leg1Accumulating;
                ledger.round_leg1 = Some(action.leg);
                ledger.round_qty_target = target_qty;
                ledger.round_leg1_entered_ts = now_ts;
                ledger.round_leg2_entered_ts = 0;
                ledger.round_leg2_anchor_price = None;
                ledger.round_leg1_target_qty = target_qty;
                ledger.round_leg1_filled_qty = sim.fill_qty.min(target_qty);
                ledger.round_leg2_target_qty = target_qty;
                ledger.round_leg2_filled_qty = 0.0;
                if ledger.lock_state == super::state::LockState::Locked
                    && params.lock_allow_reopen_before_freeze
                    && time_left_secs > params.tail_close_secs
                    && ledger.lock_reopen_used_rounds
                        < params
                            .effective_lock_reopen_max_rounds(ledger.spent_total_usdc, time_left_secs)
                {
                    ledger.lock_reopen_used_rounds =
                        ledger.lock_reopen_used_rounds.saturating_add(1);
                }
                if ledger.round_leg1_filled_qty > eps {
                    ledger.round_state = RoundPhase::Leg2Balancing;
                    ledger.round_leg2_entered_ts = now_ts;
                    ledger.round_leg2_target_qty = ledger.round_leg1_filled_qty;
                    ledger.round_leg2_filled_qty = ledger
                        .round_leg2_filled_qty
                        .min(ledger.round_leg2_target_qty);
                    ledger.round_leg2_anchor_price =
                        ledger.round_leg1.and_then(|leg1| match leg1 {
                            TradeLeg::Up => ledger.best_bid_down,
                            TradeLeg::Down => ledger.best_bid_up,
                        });
                }
            }
        }
        RoundPhase::Leg1Accumulating => {
            if let Some(leg1) = ledger.round_leg1 {
                if action.leg == leg1 {
                    ledger.round_leg1_filled_qty = (ledger.round_leg1_filled_qty + sim.fill_qty)
                        .min(ledger.round_leg1_target_qty);
                    if ledger.round_leg1_filled_qty > eps {
                        ledger.round_state = RoundPhase::Leg2Balancing;
                        ledger.round_leg2_entered_ts = now_ts;
                        ledger.round_leg2_target_qty = ledger.round_leg1_filled_qty;
                        ledger.round_leg2_filled_qty = ledger
                            .round_leg2_filled_qty
                            .min(ledger.round_leg2_target_qty);
                        ledger.round_leg2_anchor_price =
                            ledger.round_leg1.and_then(|leg1| match leg1 {
                                TradeLeg::Up => ledger.best_bid_down,
                                TradeLeg::Down => ledger.best_bid_up,
                            });
                    }
                } else if ledger.round_leg1_entered_ts > 0
                    && now_ts
                        >= ledger
                            .round_leg1_entered_ts
                            .saturating_add(params.round_pair_wait_secs)
                {
                    // Timeout fallback: stop waiting for more leg1 and force balancing on leg2
                    // with the currently accumulated leg1 inventory.
                    let leg2_target = ledger.round_leg1_filled_qty.max(0.0);
                    ledger.round_leg2_target_qty = leg2_target;
                    ledger.round_state = RoundPhase::Leg2Balancing;
                    ledger.round_leg2_entered_ts = now_ts;
                    ledger.round_leg2_anchor_price =
                        ledger.round_leg1.and_then(|leg1| match leg1 {
                            TradeLeg::Up => ledger.best_bid_down,
                            TradeLeg::Down => ledger.best_bid_up,
                        });
                    ledger.round_leg2_filled_qty =
                        (ledger.round_leg2_filled_qty + sim.fill_qty).min(leg2_target);

                    if leg2_target <= eps || ledger.round_leg2_filled_qty + eps >= leg2_target {
                        ledger.round_idx = ledger.round_idx.saturating_add(1);
                        ledger.last_round_complete_ts = now_ts;
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
                        if time_left_secs <= params.tail_close_secs {
                            ledger.round_state = RoundPhase::Done;
                        } else {
                            ledger.round_state = RoundPhase::Idle;
                        }
                    }
                }
            }
        }
        RoundPhase::Leg2Balancing => {
            if let Some(leg1) = ledger.round_leg1 {
                if action.leg != leg1 {
                    ledger.round_leg2_filled_qty = (ledger.round_leg2_filled_qty + sim.fill_qty)
                        .min(ledger.round_leg2_target_qty);
                }
                if ledger.round_leg2_filled_qty + eps >= ledger.round_leg2_target_qty {
                    ledger.round_idx = ledger.round_idx.saturating_add(1);
                    ledger.last_round_complete_ts = now_ts;
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
                    if time_left_secs <= params.tail_close_secs {
                        ledger.round_state = RoundPhase::Done;
                    } else {
                        ledger.round_state = RoundPhase::Idle;
                    }
                }
            }
        }
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
            total_budget_usdc: 100.0,
            total_budget_source: "test".to_string(),
            max_rounds: 5,
            round_budget_usdc: 20.0,
            round_budget_source: "test".to_string(),
            round_leg1_fraction: 0.40,
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
            round_slice_count: 4,
            round_dynamic_slicing_enabled: true,
            round_min_slices: 4,
            round_max_slices: 12,
            round_min_slice_qty: 2.0,
            entry_pair_regression_mode:
                crate::strategy::params::EntryPairRegressionMode::StrictMonotonic,
            entry_pair_regression_soft_band_bps: 40.0,
            entry_edge_min_bps: 0.0,
            entry_fill_prob_min: 0.0,
            open_min_fill_prob: 0.06,
            open_margin_surplus_min: 0.001,
            inventory_skew_alpha_bps: 8.0,
            lock_allow_reopen_before_freeze: true,
            lock_reopen_max_rounds: 2,
            round_min_start_gap_secs: 0,
            opening_no_trade_secs: 0,
            min_apply_interval_ms: 800,
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
            end_ts: 2_000,
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
            best_bid_up: Some(0.45),
            best_ask_up: Some(0.46),
            best_bid_down: Some(0.55),
            best_ask_down: Some(0.56),
            best_bid_size_up: Some(100.0),
            best_ask_size_up: Some(100.0),
            best_bid_size_down: Some(100.0),
            best_ask_size_down: Some(100.0),
            bid_consumption_rate_up: 1.0,
            ask_consumption_rate_up: 1.0,
            bid_consumption_rate_down: 1.0,
            ask_consumption_rate_down: 1.0,
            pair_mid_vol_bps: 12.0,
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

    fn sim_for(ledger: &Ledger, leg: TradeLeg, qty: f64, price: f64) -> SimResult {
        let mut new_qty_up = ledger.qty_up;
        let mut new_cost_up = ledger.cost_up;
        let mut new_qty_down = ledger.qty_down;
        let mut new_cost_down = ledger.cost_down;
        if leg == TradeLeg::Up {
            new_qty_up += qty;
            new_cost_up += qty * price;
        } else {
            new_qty_down += qty;
            new_cost_down += qty * price;
        }
        SimResult {
            ok: true,
            fill_qty: qty,
            fill_price: Some(price),
            maker_fill_prob: Some(0.5),
            maker_queue_ahead: Some(1.0),
            maker_expected_consumed: Some(2.0),
            maker_consumption_rate: Some(1.0),
            maker_horizon_secs: Some(4.0),
            fee_estimate: 0.0,
            spent_delta_usdc: qty * price,
            new_qty_up,
            new_cost_up,
            new_qty_down,
            new_cost_down,
            new_avg_up: avg_cost(new_cost_up, new_qty_up),
            new_avg_down: avg_cost(new_cost_down, new_qty_down),
            new_pair_cost: None,
            new_hedgeable: new_qty_up.min(new_qty_down),
            new_unhedged_up: (new_qty_up - new_qty_down).max(0.0),
            new_unhedged_down: (new_qty_down - new_qty_up).max(0.0),
            new_unhedged_value_up: 0.0,
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
    fn min_apply_interval_not_enforced_in_apply_path() {
        let params = base_params();
        let mut ledger = base_ledger();
        let action = CandidateAction {
            name: "BUY_UP_MAKER",
            leg: TradeLeg::Up,
            side: TradeSide::Buy,
            kind: super::super::state::TradeKind::Maker,
            qty: 2.0,
        };
        let sim = sim_for(&ledger, TradeLeg::Up, 2.0, 0.45);

        apply_simulated_trade(&mut ledger, &action, &sim, &params, 1_000);
        let first_apply_ts = ledger.last_apply_ts_ms;
        let spent_after_first = ledger.spent_total_usdc;
        let sim_second = sim_for(&ledger, TradeLeg::Up, 2.0, 0.45);
        apply_simulated_trade(&mut ledger, &action, &sim_second, &params, 1_000);

        assert_eq!(ledger.last_apply_ts_ms, first_apply_ts);
        assert!(ledger.spent_total_usdc > spent_after_first);
    }

    #[test]
    fn dynamic_cap_quote_binds_net_increase_buy() {
        let quote = compute_maker_buy_quote(
            Some(0.49),
            Some(0.50),
            Some(0.51),
            0.01,
            0.995,
            true,
            5.0,
            0.01,
            true,
            0.0,
            0.0,
            1.5,
            true,
        );
        assert_eq!(quote.base_postonly_price, Some(0.49));
        assert_eq!(quote.dynamic_cap_price, Some(0.48));
        assert_eq!(quote.final_price, Some(0.48));
        assert!(quote.cap_active);
        assert!(quote.cap_bind);
    }

    #[test]
    fn dynamic_cap_quote_not_applied_when_not_net_increase() {
        let quote = compute_maker_buy_quote(
            Some(0.49),
            Some(0.50),
            Some(0.51),
            0.01,
            0.995,
            true,
            5.0,
            0.01,
            true,
            0.0,
            0.0,
            1.5,
            false,
        );
        assert_eq!(quote.base_postonly_price, Some(0.49));
        assert_eq!(quote.dynamic_cap_price, None);
        assert_eq!(quote.final_price, Some(0.49));
        assert!(!quote.cap_active);
        assert!(!quote.cap_bind);
    }

    #[test]
    fn dynamic_cap_quote_keeps_price_when_passive_gap_exceeds_legacy_limit() {
        let quote = compute_maker_buy_quote(
            Some(0.49),
            Some(0.50),
            Some(0.53),
            0.01,
            0.995,
            true,
            8.0,
            0.01,
            true,
            0.0,
            0.0,
            1.5,
            true,
        );
        assert_eq!(quote.base_postonly_price, Some(0.49));
        assert_eq!(quote.dynamic_cap_price, Some(0.46));
        assert_eq!(quote.final_price, Some(0.46));
        assert!(quote.passive_gap_ticks.unwrap_or(0.0) > 1.5);
    }

    #[test]
    fn passive_gap_decay_reduces_fill_prob_for_more_passive_quotes() {
        let mut params = base_params();
        params.entry_dynamic_cap_enabled = true;
        params.entry_dynamic_cap_headroom_bps = 8.0;
        params.entry_target_margin_min_ticks = 0.0;
        params.entry_target_margin_min_bps = 0.0;
        params.maker_fill_passive_decay_k = 0.35;

        let mut tight = base_ledger();
        tight.best_bid_up = Some(0.49);
        tight.best_ask_up = Some(0.50);
        tight.best_ask_down = Some(0.50);
        tight.best_bid_size_up = Some(60.0);
        tight.bid_consumption_rate_up = 5.0;

        let mut passive = tight.clone();
        passive.best_ask_down = Some(0.53);

        let action = CandidateAction {
            name: "BUY_UP_MAKER",
            leg: TradeLeg::Up,
            side: TradeSide::Buy,
            kind: super::super::state::TradeKind::Maker,
            qty: 2.0,
        };

        let tight_sim = simulate_trade(&tight, &action, &params, 1_000);
        let passive_sim = simulate_trade(&passive, &action, &params, 1_000);

        assert!(tight_sim.passive_gap_ticks.unwrap_or(0.0) < 0.5);
        assert!(passive_sim.passive_gap_ticks.unwrap_or(0.0) >= 2.0);
        assert!(
            passive_sim.maker_fill_prob.unwrap_or(1.0) < tight_sim.maker_fill_prob.unwrap_or(0.0)
        );
    }

    #[test]
    fn round_transitions_leg1_to_leg2_and_completes() {
        let mut params = base_params();
        params.min_apply_interval_ms = 0;
        let mut ledger = base_ledger();

        let leg1_action = CandidateAction {
            name: "BUY_UP_MAKER",
            leg: TradeLeg::Up,
            side: TradeSide::Buy,
            kind: super::super::state::TradeKind::Maker,
            qty: 2.0,
        };
        let sim_leg1_first = sim_for(&ledger, TradeLeg::Up, 2.0, 0.45);
        apply_simulated_trade(&mut ledger, &leg1_action, &sim_leg1_first, &params, 1_000);
        assert_eq!(ledger.round_state, RoundPhase::Leg2Balancing);
        assert!(ledger.round_leg2_target_qty >= 2.0);

        assert!(ledger.round_leg2_entered_ts > 0);

        let leg2_action = CandidateAction {
            name: "BUY_DOWN_MAKER",
            leg: TradeLeg::Down,
            side: TradeSide::Buy,
            kind: super::super::state::TradeKind::Maker,
            qty: 2.0,
        };
        let remain_leg2 = (ledger.round_leg2_target_qty - ledger.round_leg2_filled_qty).max(0.0);
        let sim_leg2 = sim_for(&ledger, TradeLeg::Down, remain_leg2, 0.55);
        apply_simulated_trade(&mut ledger, &leg2_action, &sim_leg2, &params, 1_002);

        assert!(ledger.round_idx >= 1);
        assert!(matches!(
            ledger.round_state,
            RoundPhase::Idle | RoundPhase::Done
        ));
        assert_eq!(ledger.round_leg2_entered_ts, 0);
    }

    #[test]
    fn idle_fill_while_unbalanced_does_not_start_new_round() {
        let params = base_params();
        let mut ledger = base_ledger();
        ledger.round_state = RoundPhase::Idle;
        ledger.qty_up = 10.0;
        ledger.cost_up = 5.0;
        ledger.qty_down = 8.0;
        ledger.cost_down = 4.0;

        let action = CandidateAction {
            name: "BUY_DOWN_MAKER",
            leg: TradeLeg::Down,
            side: TradeSide::Buy,
            kind: super::super::state::TradeKind::Maker,
            qty: 2.0,
        };
        let sim = sim_for(&ledger, TradeLeg::Down, 2.0, 0.60);
        apply_simulated_trade(&mut ledger, &action, &sim, &params, 1_000);

        assert!(matches!(ledger.round_state, RoundPhase::Idle | RoundPhase::Done));
        assert_eq!(ledger.round_leg1, None);
        assert!((ledger.qty_up - ledger.qty_down).abs() <= 1e-9);
    }

    #[test]
    fn leg2_balancing_state_is_normalized_when_inventory_already_balanced() {
        let mut params = base_params();
        params.max_rounds = 6;
        let mut ledger = base_ledger();
        ledger.round_state = RoundPhase::Leg2Balancing;
        ledger.round_idx = 3;
        ledger.round_leg1 = Some(TradeLeg::Down);
        ledger.round_qty_target = 2.0;
        ledger.round_leg1_target_qty = 2.0;
        ledger.round_leg1_filled_qty = 2.0;
        ledger.round_leg2_target_qty = 2.0;
        ledger.round_leg2_filled_qty = 0.0;
        ledger.qty_up = 18.0;
        ledger.cost_up = 7.2;
        ledger.qty_down = 16.0;
        ledger.cost_down = 8.8;

        let action = CandidateAction {
            name: "BUY_DOWN_MAKER",
            leg: TradeLeg::Down,
            side: TradeSide::Buy,
            kind: super::super::state::TradeKind::Maker,
            qty: 2.0,
        };
        let sim = sim_for(&ledger, TradeLeg::Down, 2.0, 0.60);
        apply_simulated_trade(&mut ledger, &action, &sim, &params, 1_000);

        assert!(matches!(ledger.round_state, RoundPhase::Idle | RoundPhase::Done));
        assert_eq!(ledger.round_leg1, None);
        assert_eq!(ledger.round_leg2_target_qty, 0.0);
        assert_eq!(ledger.round_leg2_filled_qty, 0.0);
        assert!((ledger.qty_up - ledger.qty_down).abs() <= 1e-9);
    }

    #[test]
    fn reaching_effective_max_rounds_before_tail_close_does_not_force_done() {
        let mut params = base_params();
        params.max_rounds = 2;
        params.total_budget_usdc = 100.0;
        params.tail_close_secs = 180;

        let mut ledger = base_ledger();
        ledger.round_state = RoundPhase::Leg2Balancing;
        ledger.round_idx = 1;
        ledger.round_leg1 = Some(TradeLeg::Up);
        ledger.round_leg2_target_qty = 1.0;
        ledger.round_leg2_filled_qty = 0.0;
        ledger.qty_up = 1.0;
        ledger.cost_up = 0.45;
        ledger.qty_down = 0.0;
        ledger.cost_down = 0.0;
        ledger.spent_total_usdc = 40.0;
        ledger.end_ts = 10_000;

        let action = CandidateAction {
            name: "BUY_DOWN_MAKER",
            leg: TradeLeg::Down,
            side: TradeSide::Buy,
            kind: super::super::state::TradeKind::Maker,
            qty: 1.0,
        };
        let sim = sim_for(&ledger, TradeLeg::Down, 1.0, 0.55);
        apply_simulated_trade(&mut ledger, &action, &sim, &params, 1_000);

        assert_eq!(ledger.round_idx, 2);
        assert_eq!(ledger.round_state, RoundPhase::Idle);
    }

    #[test]
    fn estimator_probability_is_not_hard_pinned_at_one_with_large_queue() {
        let params = base_params();
        let mut ledger = base_ledger();
        ledger.best_bid_size_up = Some(400.0);
        ledger.bid_consumption_rate_up = 20.0;
        let action = CandidateAction {
            name: "BUY_UP_MAKER",
            leg: TradeLeg::Up,
            side: TradeSide::Buy,
            kind: super::super::state::TradeKind::Maker,
            qty: 4.0,
        };

        let sim = simulate_trade(&ledger, &action, &params, 1_000);
        let fill_prob = sim.maker_fill_prob.unwrap_or(0.0);
        assert!(fill_prob >= 0.0);
        assert!(fill_prob < 0.9);
    }

    #[test]
    fn hedge_recoverability_flags_unrecoverable_second_leg() {
        let mut params = base_params();
        params.no_risk_hard_pair_cap = 0.995;
        params.no_risk_hedge_recoverability_enforce = true;
        params.no_risk_hedge_recoverability_eps_bps = 2.0;
        params.entry_dynamic_cap_enabled = false;

        let mut ledger = base_ledger();
        ledger.qty_up = 14.0;
        ledger.cost_up = 14.0 * 0.43;
        ledger.qty_down = 14.0;
        ledger.cost_down = 14.0 * 0.56;
        ledger.best_ask_up = Some(0.58);
        ledger.best_bid_down = Some(0.49);
        ledger.best_ask_down = Some(0.50);

        let action = CandidateAction {
            name: "BUY_DOWN_MAKER",
            leg: TradeLeg::Down,
            side: TradeSide::Buy,
            kind: super::super::state::TradeKind::Maker,
            qty: 16.0,
        };
        let sim = simulate_trade(&ledger, &action, &params, 1_000);
        assert_eq!(sim.hedge_recoverable_now, Some(false));
        assert!(sim.required_opp_avg_price_cap.unwrap_or(0.0) < 0.52);
        assert_eq!(sim.current_opp_best_ask, Some(0.58));
        assert_eq!(sim.required_hedge_qty, Some(16.0));
        assert!(sim.hedge_margin_to_opp_ask.unwrap_or(0.0) < 0.0);
        assert!(sim.hedge_margin_required.unwrap_or(0.0) > 0.0);
        assert_eq!(sim.hedge_margin_ok, Some(false));
    }

    #[test]
    fn hedge_margin_threshold_respects_min_ticks_setting() {
        let mut params = base_params();
        params.no_risk_hard_pair_cap = 0.995;
        params.entry_dynamic_cap_enabled = true;
        params.entry_dynamic_cap_headroom_bps = 5.0;
        params.entry_target_margin_min_ticks = 0.0;
        params.entry_target_margin_min_bps = 0.0;
        params.hedge_recoverability_margin_min_bps = 8.0;

        let mut ledger = base_ledger();
        ledger.tick_size_current = 0.01;
        ledger.best_bid_up = Some(0.49);
        ledger.best_ask_up = Some(0.50);
        ledger.best_bid_down = Some(0.50);
        ledger.best_ask_down = Some(0.509_004_975_124_378_1);

        let action = CandidateAction {
            name: "BUY_UP_MAKER",
            leg: TradeLeg::Up,
            side: TradeSide::Buy,
            kind: super::super::state::TradeKind::Maker,
            qty: 16.0,
        };

        params.hedge_recoverability_margin_min_ticks = 1.0;
        let strict_sim = simulate_trade(&ledger, &action, &params, 1_000);
        assert_eq!(strict_sim.hedge_recoverable_now, Some(true));
        assert!(
            strict_sim
                .hedge_margin_to_opp_ask
                .expect("margin-to-ask should exist")
                > 0.005
        );
        assert_eq!(strict_sim.hedge_margin_required, Some(0.01));
        assert_eq!(strict_sim.hedge_margin_ok, Some(false));

        params.hedge_recoverability_margin_min_ticks = 0.5;
        let relaxed_sim = simulate_trade(&ledger, &action, &params, 1_000);
        assert_eq!(relaxed_sim.hedge_recoverable_now, Some(true));
        assert_eq!(relaxed_sim.hedge_margin_required, Some(0.005));
        assert_eq!(relaxed_sim.hedge_margin_ok, Some(true));
    }
}

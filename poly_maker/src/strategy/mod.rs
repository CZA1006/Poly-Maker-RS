pub mod gate;
pub mod params;
pub mod planner;
pub mod simulate;
pub mod state;

use gate::evaluate_action_gate;
use gate::GateResult;
use params::StrategyParams;
use planner::{build_round_plan, RoundPlan};
use simulate::{simulate_trade, CandidateAction, SimResult};
use state::{Ledger, TradeKind, TradeLeg, TradeSide};

#[derive(Debug, Clone)]
pub struct CandidateOutcome {
    pub action: CandidateAction,
    pub sim: SimResult,
    pub gate: GateResult,
    pub score: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct StrategyInput {
    pub ledger: Ledger,
    pub params: StrategyParams,
    pub now_ts: u64,
}

#[derive(Debug, Clone)]
pub struct StrategyDecision {
    pub round_plan: RoundPlan,
    pub candidates: Vec<CandidateOutcome>,
}

fn build_actions_from_round_plan(round_plan: &RoundPlan) -> Vec<CandidateAction> {
    let mut actions = Vec::new();
    if let Some(balance_leg) = round_plan.balance_leg {
        if let Some(qty) = round_plan.balance_qty {
            if qty > 0.0 {
                actions.push(CandidateAction {
                    name: if balance_leg == TradeLeg::Up {
                        "BUY_UP_MAKER"
                    } else {
                        "BUY_DOWN_MAKER"
                    },
                    leg: balance_leg,
                    side: TradeSide::Buy,
                    kind: TradeKind::Maker,
                    qty,
                });
            }
        }
    } else if round_plan.can_start_new_round {
        if let (Some(planned_leg1), Some(qty)) = (round_plan.planned_leg1, round_plan.qty_target) {
            if qty > 0.0 {
                actions.push(CandidateAction {
                    name: if planned_leg1 == TradeLeg::Up {
                        "BUY_UP_MAKER"
                    } else {
                        "BUY_DOWN_MAKER"
                    },
                    leg: planned_leg1,
                    side: TradeSide::Buy,
                    kind: TradeKind::Maker,
                    qty,
                });
            }
        }
    }
    actions
}

pub fn decide(input: StrategyInput) -> StrategyDecision {
    let round_plan = build_round_plan(&input.ledger, &input.params, input.now_ts);
    let actions = build_actions_from_round_plan(&round_plan);

    let mut candidates = Vec::with_capacity(actions.len());
    for action in actions {
        let sim = simulate_trade(&input.ledger, &action, &input.params, input.now_ts);
        let gate = evaluate_action_gate(
            &input.ledger,
            &action,
            &sim,
            &input.params,
            input.now_ts,
            &round_plan,
        );
        let score = if gate.allow {
            let edge_bps = if round_plan.planned_leg1 == Some(action.leg) {
                round_plan.entry_edge_bps.unwrap_or(0.0)
            } else {
                -10_000.0
            };
            let regime_score = if round_plan.planned_leg1 == Some(action.leg) {
                round_plan.entry_regime_score.unwrap_or(0.0)
            } else {
                0.0
            };
            let turning_good =
                (1.0 - round_plan.first_leg_turning_score.unwrap_or(1.0)).clamp(0.0, 1.0);
            let fill_prob = if action.kind == TradeKind::Maker {
                sim.maker_fill_prob
                    .or_else(|| {
                        round_plan
                            .entry_timeout_flow_ratio
                            .map(|r| (1.0 - (-r.max(0.0)).exp()).clamp(0.0, 1.0))
                    })
                    .unwrap_or(0.0)
            } else {
                1.0
            };
            let queue_penalty = sim
                .maker_queue_ahead
                .map(|q| (q / (q + action.qty.max(1e-6))).clamp(0.0, 1.0))
                .unwrap_or(0.0);
            let passive_penalty = sim.passive_gap_ticks.unwrap_or(0.0).max(0.0) * 0.35;
            let tick = input.ledger.tick_size_current.max(1e-6);
            let margin_surplus = match (sim.hedge_margin_to_opp_ask, sim.hedge_margin_required) {
                (Some(actual), Some(required)) => actual - required,
                _ => 0.0,
            };
            let survival_score = (margin_surplus / tick).clamp(-3.0, 3.0);
            let risk_penalty = (sim.new_unhedged_value_up + sim.new_unhedged_value_down) * 0.001;
            let quality = 1.8 * (edge_bps / 100.0)
                + 1.0 * regime_score
                + 0.7 * turning_good
                + 1.0 * fill_prob
                + 0.6 * survival_score
                - 0.8 * queue_penalty
                - passive_penalty;
            // ws_market keeps the minimum score, so negate quality.
            Some(-quality + risk_penalty)
        } else {
            None
        };
        candidates.push(CandidateOutcome {
            action,
            sim,
            gate,
            score,
        });
    }

    StrategyDecision {
        round_plan,
        candidates,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::state::RoundPhase;

    fn base_round_plan() -> RoundPlan {
        RoundPlan {
            phase: RoundPhase::Idle,
            planned_leg1: None,
            qty_target: None,
            balance_leg: None,
            balance_qty: None,
            can_start_new_round: false,
            budget_remaining_round: 0.0,
            budget_remaining_total: 0.0,
            reserve_needed_usdc: None,
            vol_entry_bps: 0.0,
            vol_entry_ok: false,
            reversal_up_ok: false,
            reversal_down_ok: false,
            turn_up_ok: false,
            turn_down_ok: false,
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
            entry_fallback_active: false,
            entry_fallback_armed: false,
            entry_fallback_trigger_reason: None,
            entry_fallback_blocked_by_recoverability: false,
            new_round_cutoff_secs: 0,
            late_new_round_blocked: false,
            pair_quality_ok: false,
            pair_regression_ok: false,
            can_open_round_base_ok: false,
            can_start_block_reason: None,
        }
    }

    #[test]
    fn no_opening_actions_when_round_cannot_start() {
        let plan = RoundPlan {
            planned_leg1: Some(TradeLeg::Up),
            qty_target: Some(4.0),
            can_start_new_round: false,
            ..base_round_plan()
        };
        let actions = build_actions_from_round_plan(&plan);
        assert!(actions.is_empty());
    }

    #[test]
    fn only_planned_leg_action_when_round_can_start() {
        let plan = RoundPlan {
            planned_leg1: Some(TradeLeg::Down),
            qty_target: Some(3.0),
            can_start_new_round: true,
            ..base_round_plan()
        };
        let actions = build_actions_from_round_plan(&plan);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].leg, TradeLeg::Down);
        assert!((actions[0].qty - 3.0).abs() <= 1e-9);
    }
}

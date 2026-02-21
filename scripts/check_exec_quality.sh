#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ORIG_PWD="$(pwd)"
cd "$REPO_ROOT"

usage() {
  echo "Usage: $0 <jsonl_file>"
}

if [[ $# -ne 1 ]]; then
  usage
  exit 1
fi

INPUT="$1"
if [[ "$INPUT" = /* ]]; then
  FILE="$INPUT"
else
  FILE="${ORIG_PWD}/${INPUT}"
fi

if [[ ! -f "$FILE" ]]; then
  echo "File not found: $FILE"
  exit 1
fi

python3 - <<'PY' "$FILE"
import json
import os
import statistics
import sys
from collections import Counter
from pathlib import Path

path = Path(sys.argv[1])
selected = 0
best_action_nonnull = 0
open_count = 0
fill_count = 0
requote_count = 0
cancel_unrecoverable_count = 0
cancel_timeout_count = 0
wait_count = 0
turn_scores = []
turn_scores_gate_active = []
turn_gate_fields_seen = 0
open_margin_surplus_samples = []
allow_fill_prob_samples = []
hedge_not_recoverable_denies = 0
hedge_margin_insufficient_denies = 0
entry_worst_pair_denies = 0
no_quote_denies = 0
low_maker_fill_prob_denies = 0
total_candidate_denies = 0
max_risk_guard_cancel = None
max_risk_guard_pair_cap_cancel = None
max_stale_cancel = None
max_tail_pair_cap_block = None
max_close_window_cancel = None
max_entry_worst_pair_block = None
max_unrecoverable_block = None
max_cancel_unrecoverable = None
max_resting_soft_recheck_cancel = None
max_timeout_extend = None
max_executed_pair_snapshot = None
snapshot_hard_cap = None
slice_qty_samples = []
final_spent_total_usdc = None
final_total_budget_usdc = None
can_start_true_count = 0
snapshot_count = 0
block_reason_counts = Counter()
noop_deny_ratio_min = float(os.environ.get("NOOP_DENY_RATIO_MIN", "0.70"))
noop_min_denies = int(os.environ.get("NOOP_MIN_DENIES", "10"))
noop_include_entry_worst_pair = os.environ.get(
    "NOOP_INCLUDE_ENTRY_WORST_PAIR", "true"
).strip().lower() in {"1", "true", "yes", "on"}

for raw in path.open():
    raw = raw.strip()
    if not raw:
        continue
    obj = json.loads(raw)
    kind = obj.get("kind")
    data = obj.get("data", {}) or {}
    if kind == "dryrun_candidates":
        if data.get("best_action") is not None:
            best_action_nonnull += 1
        if data.get("applied_action") is not None:
            selected += 1
        for cand in data.get("candidates", []) or []:
            deny_reason = cand.get("deny_reason") or ""
            if deny_reason:
                total_candidate_denies += 1
            else:
                maker_fill_prob = cand.get("maker_fill_prob")
                if isinstance(maker_fill_prob, (int, float)):
                    allow_fill_prob_samples.append(float(maker_fill_prob))
            if deny_reason == "hedge_not_recoverable":
                hedge_not_recoverable_denies += 1
            if deny_reason == "hedge_margin_insufficient":
                hedge_margin_insufficient_denies += 1
            if deny_reason == "entry_worst_pair":
                entry_worst_pair_denies += 1
            if deny_reason == "no_quote":
                no_quote_denies += 1
            if deny_reason == "low_maker_fill_prob":
                low_maker_fill_prob_denies += 1
    elif kind == "user_ws_order":
        rt = data.get("raw_type")
        if rt == "sim_order_open":
            open_count += 1
            open_margin_surplus = data.get("open_margin_surplus")
            if isinstance(open_margin_surplus, (int, float)):
                open_margin_surplus_samples.append(float(open_margin_surplus))
            margin_to_opp_ask = data.get("hedge_margin_to_opp_ask")
            margin_required = data.get("hedge_margin_required")
            if (
                not isinstance(open_margin_surplus, (int, float))
                and isinstance(margin_to_opp_ask, (int, float))
                and isinstance(
                margin_required, (int, float)
                )
            ):
                open_margin_surplus_samples.append(
                    float(margin_to_opp_ask) - float(margin_required)
                )
        elif rt in ("sim_order_filled_resting", "sim_order_filled"):
            fill_count += 1
        elif rt == "sim_order_cancel_requote":
            requote_count += 1
        elif rt == "sim_order_cancel_unrecoverable":
            cancel_unrecoverable_count += 1
        elif rt == "sim_order_cancel_timeout":
            cancel_timeout_count += 1
    elif kind == "live_skip":
        if data.get("reason") == "paper_waiting_open_order":
            wait_count += 1
    elif kind == "dryrun_snapshot":
        snapshot_count += 1
        if data.get("can_start_new_round") is True:
            can_start_true_count += 1
        block_reason = data.get("round_plan_can_start_block_reason")
        if isinstance(block_reason, str) and block_reason:
            block_reason_counts[block_reason] += 1
        score = data.get("first_leg_turning_score")
        turn_up_ok_raw = data.get("round_plan_turn_up_ok")
        turn_down_ok_raw = data.get("round_plan_turn_down_ok")
        turn_up_ok = turn_up_ok_raw is True
        turn_down_ok = turn_down_ok_raw is True
        if turn_up_ok_raw is not None or turn_down_ok_raw is not None:
            turn_gate_fields_seen += 1
        turn_gate_enabled_cfg = (
            float(data.get("entry_turn_min_rebound_bps") or 0.0) > 0.0
            and int(data.get("entry_turn_confirm_ticks") or 0) > 0
        )
        turn_gate_active = turn_gate_enabled_cfg and (turn_up_ok or turn_down_ok)
        if isinstance(score, (int, float)):
            turn_scores.append(float(score))
            if turn_gate_active:
                turn_scores_gate_active.append(float(score))
        rg = data.get("risk_guard_cancel_count_window")
        if isinstance(rg, (int, float)):
            value = float(rg)
            max_risk_guard_cancel = value if max_risk_guard_cancel is None else max(max_risk_guard_cancel, value)
        rg_pair = data.get("risk_guard_cancel_pair_cap_count_window")
        if isinstance(rg_pair, (int, float)):
            value = float(rg_pair)
            max_risk_guard_pair_cap_cancel = value if max_risk_guard_pair_cap_cancel is None else max(max_risk_guard_pair_cap_cancel, value)
        st = data.get("stale_cancel_count_window")
        if isinstance(st, (int, float)):
            value = float(st)
            max_stale_cancel = value if max_stale_cancel is None else max(max_stale_cancel, value)
        tb = data.get("tail_pair_cap_block_count_window")
        if isinstance(tb, (int, float)):
            value = float(tb)
            max_tail_pair_cap_block = value if max_tail_pair_cap_block is None else max(max_tail_pair_cap_block, value)
        cw = data.get("close_window_cancel_count_window")
        if isinstance(cw, (int, float)):
            value = float(cw)
            max_close_window_cancel = value if max_close_window_cancel is None else max(max_close_window_cancel, value)
        ew = data.get("entry_worst_pair_block_count_window")
        if isinstance(ew, (int, float)):
            value = float(ew)
            max_entry_worst_pair_block = value if max_entry_worst_pair_block is None else max(max_entry_worst_pair_block, value)
        ub = data.get("unrecoverable_block_count_window")
        if isinstance(ub, (int, float)):
            value = float(ub)
            max_unrecoverable_block = value if max_unrecoverable_block is None else max(max_unrecoverable_block, value)
        cu = data.get("cancel_unrecoverable_count_window")
        if isinstance(cu, (int, float)):
            value = float(cu)
            max_cancel_unrecoverable = value if max_cancel_unrecoverable is None else max(max_cancel_unrecoverable, value)
        rs = data.get("resting_soft_recheck_cancel_count_window")
        if isinstance(rs, (int, float)):
            value = float(rs)
            max_resting_soft_recheck_cancel = value if max_resting_soft_recheck_cancel is None else max(max_resting_soft_recheck_cancel, value)
        te = data.get("timeout_extend_count_window")
        if isinstance(te, (int, float)):
            value = float(te)
            max_timeout_extend = value if max_timeout_extend is None else max(max_timeout_extend, value)
        mp = data.get("max_executed_sim_pair_cost_window")
        if isinstance(mp, (int, float)):
            value = float(mp)
            max_executed_pair_snapshot = value if max_executed_pair_snapshot is None else max(max_executed_pair_snapshot, value)
        if snapshot_hard_cap is None and isinstance(data.get("no_risk_hard_pair_cap"), (int, float)):
            snapshot_hard_cap = float(data.get("no_risk_hard_pair_cap"))
        slice_qty = data.get("round_plan_slice_qty_current")
        if isinstance(slice_qty, (int, float)):
            slice_qty_samples.append(float(slice_qty))
        spent_total = data.get("spent_total_usdc")
        if isinstance(spent_total, (int, float)):
            final_spent_total_usdc = float(spent_total)
        total_budget = data.get("total_budget_usdc")
        if isinstance(total_budget, (int, float)):
            final_total_budget_usdc = float(total_budget)


def out(ok: bool, msg: str):
    print(("PASS: " if ok else "FAIL: ") + msg)
    return 0 if ok else 1


fails = 0
fill_rate_min = float(os.environ.get("FILL_RATE_MIN", "0.10"))
wait_activity_ratio_max = float(os.environ.get("WAIT_ACTIVITY_RATIO_MAX", os.environ.get("WAIT_PER_SELECTED_MAX", "0.65")))
requote_per_open_max = float(os.environ.get("REQUOTE_PER_OPEN_MAX", "0.60"))
cancel_unrecoverable_per_open_max = float(
    os.environ.get("CANCEL_UNRECOVERABLE_PER_OPEN_MAX", "0.35")
)
turn_p50_max = float(os.environ.get("TURNING_SCORE_P50_MAX", "0.38"))
turn_score_require_gate = os.environ.get("TURN_SCORE_REQUIRE_GATE", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
median_slice_qty_max = float(os.environ.get("MEDIAN_SLICE_QTY_MAX", "8.0"))
open_margin_surplus_p50_min = float(os.environ.get("OPEN_MARGIN_SURPLUS_P50_MIN", "0.001"))
no_quote_ratio_warn_max = float(os.environ.get("NO_QUOTE_RATIO_WARN_MAX", "0.60"))
low_fill_ratio_warn_max = float(os.environ.get("LOW_FILL_RATIO_WARN_MAX", "0.60"))
allow_fill_prob_p50_min = float(os.environ.get("ALLOW_FILL_PROB_P50_MIN", "0.07"))
cancel_timeout_per_open_max = float(os.environ.get("CANCEL_TIMEOUT_PER_OPEN_MAX", "0.25"))
hard_cap = float(os.environ.get("NO_RISK_HARD_PAIR_CAP", snapshot_hard_cap if snapshot_hard_cap is not None else 0.995))
recoverability_denies = hedge_not_recoverable_denies + hedge_margin_insufficient_denies
opportunity_denies = recoverability_denies + (
    entry_worst_pair_denies if noop_include_entry_worst_pair else 0
)
no_opportunity_window = (
    selected == 0
    and best_action_nonnull == 0
    and open_count == 0
    and total_candidate_denies >= noop_min_denies
    and (
        (opportunity_denies / total_candidate_denies)
        if total_candidate_denies > 0
        else 0.0
    )
    >= noop_deny_ratio_min - 1e-12
)
opportunity_deny_ratio = (
    (opportunity_denies / total_candidate_denies)
    if total_candidate_denies > 0
    else 0.0
)
no_quote_ratio = (
    (no_quote_denies / total_candidate_denies)
    if total_candidate_denies > 0
    else 0.0
)
low_fill_ratio = (
    (low_maker_fill_prob_denies / total_candidate_denies)
    if total_candidate_denies > 0
    else 0.0
)
fails += out(
    True,
    "no_opportunity_window="
    + ("true" if no_opportunity_window else "false")
    + (
        f" (opportunity_denies={opportunity_denies} "
        f"hedge_not_recoverable_denies={hedge_not_recoverable_denies} "
        f"hedge_margin_insufficient_denies={hedge_margin_insufficient_denies} "
        f"entry_worst_pair_denies={entry_worst_pair_denies} "
        f"total_denies={total_candidate_denies} ratio={opportunity_deny_ratio:.4f} "
        f"min_ratio={noop_deny_ratio_min:.2f} min_denies={noop_min_denies} "
        f"include_entry_worst_pair={str(noop_include_entry_worst_pair).lower()})"
    ),
)
if no_opportunity_window:
    fails += out(
        True,
        f"no_quote_ratio observed (no_opportunity_window=true ratio={no_quote_ratio:.4f}, no_quote_denies={no_quote_denies}, total_denies={total_candidate_denies})",
    )
    fails += out(
        True,
        f"low_fill_ratio observed (no_opportunity_window=true ratio={low_fill_ratio:.4f}, low_fill_denies={low_maker_fill_prob_denies}, total_denies={total_candidate_denies})",
    )
else:
    if no_quote_ratio <= no_quote_ratio_warn_max + 1e-12:
        fails += out(
            True,
            f"no_quote_ratio <= {no_quote_ratio_warn_max:.2f} (actual={no_quote_ratio:.4f}, no_quote_denies={no_quote_denies}, total_denies={total_candidate_denies})",
        )
    else:
        fails += out(
            True,
            f"no_quote_ratio WARN (actual={no_quote_ratio:.4f} > {no_quote_ratio_warn_max:.2f}, no_quote_denies={no_quote_denies}, total_denies={total_candidate_denies})",
        )
    if low_fill_ratio <= low_fill_ratio_warn_max + 1e-12:
        fails += out(
            True,
            f"low_fill_ratio <= {low_fill_ratio_warn_max:.2f} (actual={low_fill_ratio:.4f}, low_fill_denies={low_maker_fill_prob_denies}, total_denies={total_candidate_denies})",
        )
    else:
        fails += out(
            True,
            f"low_fill_ratio WARN (actual={low_fill_ratio:.4f} > {low_fill_ratio_warn_max:.2f}, low_fill_denies={low_maker_fill_prob_denies}, total_denies={total_candidate_denies})",
        )

if no_opportunity_window:
    fails += out(True, "allow_fill_prob_p50 check skipped (no_opportunity_window=true)")
elif allow_fill_prob_samples:
    allow_fill_prob_p50 = statistics.median(allow_fill_prob_samples)
    fails += out(
        allow_fill_prob_p50 + 1e-9 >= allow_fill_prob_p50_min,
        f"allow_fill_prob_p50 >= {allow_fill_prob_p50_min:.4f} (actual={allow_fill_prob_p50:.4f}, n={len(allow_fill_prob_samples)})",
    )
else:
    fails += out(False, "allow_fill_prob_p50 check (no allowed maker_fill_prob samples)")

spent_total = final_spent_total_usdc if final_spent_total_usdc is not None else 0.0
spent_ratio = (
    spent_total / final_total_budget_usdc
    if isinstance(final_total_budget_usdc, (int, float)) and final_total_budget_usdc > 0
    else None
)
can_start_ratio = (can_start_true_count / snapshot_count) if snapshot_count > 0 else 0.0
top_blocks = ",".join([f"{k}:{v}" for k, v in block_reason_counts.most_common(5)]) or "none"
fails += out(
    True,
    "spent_utilization observed "
    f"(spent_total={spent_total:.6f} "
    f"spent_ratio={(spent_ratio if spent_ratio is not None else float('nan')):.6f} "
    f"can_start_new_round_true_ratio={can_start_ratio:.4f} block_reason_top={top_blocks})",
)

if no_opportunity_window:
    fails += out(True, "fill_rate check skipped (no_opportunity_window=true)")
elif open_count > 0:
    fill_rate = fill_count / open_count
    fails += out(
        fill_rate >= fill_rate_min,
        f"fill_rate >= {fill_rate_min:.2f} (actual={fill_rate:.4f}, fills={fill_count}, open={open_count})",
    )
else:
    fails += out(False, "fill_rate check (open_count=0)")

if no_opportunity_window:
    fails += out(True, "wait_per_selected check skipped (no_opportunity_window=true)")
elif (wait_count + selected) > 0:
    wait_per_selected = (wait_count / selected) if selected > 0 else float("inf")
    wait_activity_ratio = wait_count / (wait_count + selected)
    fails += out(
        wait_activity_ratio <= wait_activity_ratio_max,
        f"wait_per_selected(activity_ratio) <= {wait_activity_ratio_max:.2f} "
        f"(actual={wait_activity_ratio:.4f}, waiting={wait_count}, selected={selected}, legacy_wait_per_selected={wait_per_selected:.4f})",
    )
else:
    fails += out(False, "wait_per_selected check (selected+waiting=0)")

if no_opportunity_window:
    fails += out(True, "requote_per_open check skipped (no_opportunity_window=true)")
elif open_count > 0:
    requote_per_open = requote_count / open_count
    fails += out(
        requote_per_open <= requote_per_open_max,
        f"requote_per_open <= {requote_per_open_max:.2f} (actual={requote_per_open:.4f}, requote={requote_count}, open={open_count})",
    )
else:
    fails += out(False, "requote_per_open check (open_count=0)")

if no_opportunity_window:
    fails += out(
        True, "cancel_timeout_per_open check skipped (no_opportunity_window=true)"
    )
elif open_count > 0:
    cancel_timeout_per_open = cancel_timeout_count / open_count
    fails += out(
        cancel_timeout_per_open <= cancel_timeout_per_open_max,
        "cancel_timeout_per_open <= "
        f"{cancel_timeout_per_open_max:.2f} "
        f"(actual={cancel_timeout_per_open:.4f}, "
        f"cancel_timeout={cancel_timeout_count}, open={open_count})",
    )
else:
    fails += out(False, "cancel_timeout_per_open check (open_count=0)")

if no_opportunity_window:
    fails += out(
        True, "cancel_unrecoverable_per_open check skipped (no_opportunity_window=true)"
    )
elif open_count > 0:
    cancel_unrecoverable_per_open = cancel_unrecoverable_count / open_count
    fails += out(
        cancel_unrecoverable_per_open <= cancel_unrecoverable_per_open_max,
        "cancel_unrecoverable_per_open <= "
        f"{cancel_unrecoverable_per_open_max:.2f} "
        f"(actual={cancel_unrecoverable_per_open:.4f}, "
        f"cancel_unrecoverable={cancel_unrecoverable_count}, open={open_count})",
    )
else:
    fails += out(False, "cancel_unrecoverable_per_open check (open_count=0)")

if no_opportunity_window:
    fails += out(True, "open_margin_surplus_p50 check skipped (no_opportunity_window=true)")
elif open_count > 0 and open_margin_surplus_samples:
    margin_surplus_p50 = statistics.median(open_margin_surplus_samples)
    fails += out(
        margin_surplus_p50 + 1e-9 >= open_margin_surplus_p50_min,
        f"open_margin_surplus_p50 >= {open_margin_surplus_p50_min:.4f} "
        f"(actual={margin_surplus_p50:.6f}, n={len(open_margin_surplus_samples)})",
    )
elif open_count > 0:
    fails += out(False, "open_margin_surplus_p50 check (no open margin samples)")
else:
    fails += out(False, "open_margin_surplus_p50 check (open_count=0)")

if no_opportunity_window:
    fails += out(True, "median_slice_qty check skipped (no_opportunity_window=true)")
elif slice_qty_samples:
    med_slice_qty = statistics.median(slice_qty_samples)
    fails += out(
        med_slice_qty <= median_slice_qty_max + 1e-9,
        f"median_slice_qty <= {median_slice_qty_max:.2f} (actual={med_slice_qty:.4f}, n={len(slice_qty_samples)})",
    )
else:
    fails += out(False, "median_slice_qty check (no snapshot slice_qty samples)")

turn_samples = turn_scores_gate_active if turn_score_require_gate else turn_scores
turn_scope = "gate_active" if turn_score_require_gate else "all"
fails += out(
    True,
    f"turn_score_samples observed (scope={turn_scope}, gate_fields_seen={turn_gate_fields_seen}, total_n={len(turn_scores)}, gate_active_n={len(turn_scores_gate_active)})",
)

if turn_samples:
    p50 = statistics.median(turn_samples)
    fails += out(
        p50 <= turn_p50_max,
        f"first_leg_turning_score_p50[{turn_scope}] <= {turn_p50_max:.2f} (actual={p50:.4f}, n={len(turn_samples)})",
    )
else:
    if no_opportunity_window:
        fails += out(True, "first_leg_turning_score_p50 check skipped (no_opportunity_window=true)")
    elif turn_score_require_gate and turn_scores:
        fails += out(True, "first_leg_turning_score_p50 check skipped (turn_gate_active_samples=0)")
    else:
        fails += out(False, "first_leg_turning_score_p50 check (no score samples)")

if max_risk_guard_cancel is not None:
    fails += out(True, f"risk_guard_cancel_count_window observed (actual_max={max_risk_guard_cancel:.0f})")
else:
    fails += out(False, "risk_guard_cancel_count_window check (missing)")

if max_risk_guard_pair_cap_cancel is not None:
    fails += out(
        True,
        f"risk_guard_cancel_pair_cap_count_window observed (actual_max={max_risk_guard_pair_cap_cancel:.0f})",
    )
else:
    fails += out(False, "risk_guard_cancel_pair_cap_count_window check (missing)")

if max_stale_cancel is not None:
    fails += out(True, f"stale_cancel_count_window observed (actual_max={max_stale_cancel:.0f})")
else:
    fails += out(False, "stale_cancel_count_window check (missing)")

if max_tail_pair_cap_block is not None:
    fails += out(True, f"tail_pair_cap_block_count_window observed (actual_max={max_tail_pair_cap_block:.0f})")
else:
    fails += out(False, "tail_pair_cap_block_count_window check (missing)")

if max_close_window_cancel is not None:
    fails += out(True, f"close_window_cancel_count_window observed (actual_max={max_close_window_cancel:.0f})")
else:
    fails += out(False, "close_window_cancel_count_window check (missing)")

if max_entry_worst_pair_block is not None:
    fails += out(True, f"entry_worst_pair_block_count_window observed (actual_max={max_entry_worst_pair_block:.0f})")
else:
    fails += out(False, "entry_worst_pair_block_count_window check (missing)")

if max_unrecoverable_block is not None:
    fails += out(True, f"unrecoverable_block_count_window observed (actual_max={max_unrecoverable_block:.0f})")
else:
    fails += out(False, "unrecoverable_block_count_window check (missing)")

if max_cancel_unrecoverable is not None:
    fails += out(True, f"cancel_unrecoverable_count_window observed (actual_max={max_cancel_unrecoverable:.0f})")
else:
    fails += out(False, "cancel_unrecoverable_count_window check (missing)")

if max_resting_soft_recheck_cancel is not None:
    fails += out(
        max_resting_soft_recheck_cancel <= 0.0 + 1e-9,
        f"resting_soft_recheck_cancel_count_window == 0 (actual_max={max_resting_soft_recheck_cancel:.0f})",
    )
else:
    fails += out(False, "resting_soft_recheck_cancel_count_window check (missing)")

if max_timeout_extend is not None:
    fails += out(True, f"timeout_extend_count_window observed (actual_max={max_timeout_extend:.0f})")
else:
    fails += out(True, "timeout_extend_count_window observed (missing in this window schema)")

if max_executed_pair_snapshot is not None:
    fails += out(
        max_executed_pair_snapshot <= hard_cap + 1e-9,
        f"max_executed_sim_pair_cost_window <= hard_cap (hard_cap={hard_cap:.6f}, actual_max={max_executed_pair_snapshot:.6f})",
    )
else:
    if no_opportunity_window:
        fails += out(True, "max_executed_sim_pair_cost_window check skipped (no_opportunity_window=true)")
    else:
        fails += out(False, "max_executed_sim_pair_cost_window check (missing)")

if fails:
    print(f"FAILED: {fails} checks failed")
    sys.exit(1)

print("PASS: execution quality checks passed")
PY

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
import re
import statistics
import sys
from collections import Counter
from pathlib import Path

path = Path(sys.argv[1])
applied_ts_ms = []
max_round_idx = 0
snapshots = []
slice_qty_samples = []
market_slug = None
executed_sim_pairs = []
snapshot_hard_cap = None
snapshot_enforce_tail = None
snapshot_strict_zero_unmatched = None
final_spent_total_usdc = None
final_total_budget_usdc = None
can_start_true_count = 0
snapshot_count = 0
block_reason_counts = Counter()
unrecoverable_executed_actions = 0
best_action_nonnull = 0
hedge_not_recoverable_denies = 0
hedge_margin_insufficient_denies = 0
entry_worst_pair_denies = 0
no_quote_denies = 0
total_candidate_denies = 0
sim_order_open_count = 0
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
    if kind == "dryrun_candidates":
        data = obj.get("data", {})
        if data.get("best_action") is not None:
            best_action_nonnull += 1
        for cand in data.get("candidates", []) or []:
            deny_reason = cand.get("deny_reason") or ""
            if deny_reason:
                total_candidate_denies += 1
            if deny_reason == "hedge_not_recoverable":
                hedge_not_recoverable_denies += 1
            if deny_reason == "hedge_margin_insufficient":
                hedge_margin_insufficient_denies += 1
            if deny_reason == "entry_worst_pair":
                entry_worst_pair_denies += 1
            if deny_reason == "no_quote":
                no_quote_denies += 1
    elif kind == "user_ws_order":
        data = obj.get("data", {})
        if data.get("raw_type") == "sim_order_open":
            sim_order_open_count += 1
    elif kind == "dryrun_apply":
        data = obj.get("data", {})
        if data.get("executed") is False:
            continue
        ts_ms = obj.get("ts_ms")
        if isinstance(ts_ms, (int, float)):
            applied_ts_ms.append(int(ts_ms))
        action = data.get("applied_action")
        if isinstance(action, dict):
            sim_pair = action.get("sim_pair_cost")
            if isinstance(sim_pair, (int, float)):
                executed_sim_pairs.append(float(sim_pair))
            sim_unhedged_up = action.get("sim_unhedged_up")
            sim_unhedged_down = action.get("sim_unhedged_down")
            hedge_recoverable = action.get("hedge_recoverable_now")
            if (
                isinstance(sim_unhedged_up, (int, float))
                and isinstance(sim_unhedged_down, (int, float))
                and (float(sim_unhedged_up) > 1e-9 or float(sim_unhedged_down) > 1e-9)
                and hedge_recoverable is False
            ):
                unrecoverable_executed_actions += 1
    elif kind == "dryrun_snapshot":
        data = obj.get("data", {})
        snapshots.append(data)
        snapshot_count += 1
        if data.get("can_start_new_round") is True:
            can_start_true_count += 1
        block_reason = data.get("round_plan_can_start_block_reason")
        if isinstance(block_reason, str) and block_reason:
            block_reason_counts[block_reason] += 1
        slice_qty = data.get("round_plan_slice_qty_current")
        if isinstance(slice_qty, (int, float)):
            slice_qty_samples.append(float(slice_qty))
        ridx = data.get("round_idx")
        if isinstance(ridx, (int, float)):
            max_round_idx = max(max_round_idx, int(ridx))
        if market_slug is None and isinstance(data.get("market_slug"), str):
            market_slug = data["market_slug"]
        if snapshot_hard_cap is None:
            v = data.get("no_risk_hard_pair_cap")
            if isinstance(v, (int, float)):
                snapshot_hard_cap = float(v)
        if snapshot_enforce_tail is None:
            v = data.get("no_risk_enforce_tail")
            if isinstance(v, bool):
                snapshot_enforce_tail = v
        if snapshot_strict_zero_unmatched is None:
            v = data.get("no_risk_strict_zero_unmatched")
            if isinstance(v, bool):
                snapshot_strict_zero_unmatched = v
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
apply_count = len(applied_ts_ms)
recoverability_denies = hedge_not_recoverable_denies + hedge_margin_insufficient_denies
opportunity_denies = recoverability_denies + (
    entry_worst_pair_denies if noop_include_entry_worst_pair else 0
)
no_opportunity_window = (
    apply_count == 0
    and best_action_nonnull == 0
    and sim_order_open_count == 0
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
fails += out(
    True,
    f"no_quote_ratio observed (actual={no_quote_ratio:.4f}, no_quote_denies={no_quote_denies}, total_denies={total_candidate_denies})",
)
min_round_idx_env = os.environ.get("MIN_ROUND_IDX", "").strip()
min_apply_count_env = os.environ.get("MIN_APPLY_COUNT", "").strip()
max_rounds_env = os.environ.get("MAX_ROUNDS", "").strip()
max_rounds = int(max_rounds_env) if max_rounds_env else 0
if min_round_idx_env:
    min_round_idx = int(min_round_idx_env)
else:
    if max_rounds > 0 and max_rounds <= 2:
        min_round_idx = 1
    else:
        min_round_idx = 3

if min_apply_count_env:
    min_apply_count = int(min_apply_count_env)
elif max_rounds > 0:
    # By default, require at least ~2 applied fills per configured round,
    # while capping at 6 so larger-round configs keep the old bar.
    min_apply_count = min(6, max(2, max_rounds * 2))
else:
    min_apply_count = 6

if no_opportunity_window:
    fails += out(
        True,
        f"apply_count check skipped (no_opportunity_window=true actual={apply_count})",
    )
    fails += out(
        True,
        f"max_round_idx check skipped (no_opportunity_window=true actual={max_round_idx})",
    )
else:
    fails += out(
        apply_count >= min_apply_count,
        f"apply_count >= {min_apply_count} (actual={apply_count})",
    )
    fails += out(
        max_round_idx >= min_round_idx,
        f"max_round_idx >= {min_round_idx} (actual={max_round_idx})",
    )

hard_cap = float(os.environ.get("NO_RISK_HARD_PAIR_CAP", snapshot_hard_cap if snapshot_hard_cap is not None else 0.995))
if executed_sim_pairs:
    max_exec_pair = max(executed_sim_pairs)
    fails += out(
        max_exec_pair <= 1.0 + 1e-9,
        f"executed_sim_pair_cost <= 1.0 (actual_max={max_exec_pair:.6f})",
    )
    fails += out(
        max_exec_pair <= hard_cap + 1e-9,
        f"executed_sim_pair_cost <= hard_cap (hard_cap={hard_cap:.6f}, actual_max={max_exec_pair:.6f})",
    )
else:
    fails += out(True, "executed_sim_pair_cost checks (no executed sim_pair_cost samples)")

fails += out(
    unrecoverable_executed_actions == 0,
    f"executed actions with hedge_recoverable_now=false == 0 (actual={unrecoverable_executed_actions})",
)

start_ts = None
if market_slug:
    m = re.search(r"-(\d{10})$", market_slug)
    if m:
        start_ts = int(m.group(1))

if start_ts is not None and apply_count > 0:
    cutoff_ms = start_ts * 1000 + 60_000
    first60 = sum(1 for ts in applied_ts_ms if ts < cutoff_ms)
    ratio = first60 / apply_count
    fails += out(ratio < 0.40, f"first_60s_apply_ratio < 40% (actual={ratio:.2%}, first60={first60}, total={apply_count})")
else:
    if no_opportunity_window:
        fails += out(True, "first_60s_apply_ratio check skipped (no_opportunity_window=true)")
    else:
        fails += out(False, "first_60s_apply_ratio check (missing start_ts or apply_count=0)")

if len(applied_ts_ms) >= 2:
    applied_ts_ms.sort()
    deltas = [(applied_ts_ms[i] - applied_ts_ms[i - 1]) / 1000.0 for i in range(1, len(applied_ts_ms))]
    p50 = statistics.median(deltas)
    apply_interval_min = float(os.environ.get("APPLY_INTERVAL_P50_MIN", "0.8"))
    apply_interval_eps = float(
        os.environ.get("APPLY_INTERVAL_P50_EPS", "0.005")
    )
    apply_interval_min_small_slice = float(
        os.environ.get("APPLY_INTERVAL_P50_MIN_SMALL_SLICE", "0.3")
    )
    if slice_qty_samples and statistics.median(slice_qty_samples) <= 4.0 + 1e-9:
        apply_interval_min = apply_interval_min_small_slice
    fails += out(
        p50 + apply_interval_eps >= apply_interval_min,
        (
            f"apply_interval_p50 >= {apply_interval_min:.3f}s "
            f"(actual={p50:.3f}s eps={apply_interval_eps:.3f}s)"
        ),
    )
else:
    if no_opportunity_window:
        fails += out(True, "apply_interval_p50 check skipped (no_opportunity_window=true)")
    else:
        fails += out(False, "apply_interval_p50 check (need >=2 applied actions)")

tail_rows = [s for s in snapshots if isinstance(s.get("time_left_secs"), (int, float)) and s["time_left_secs"] <= 180]
strict_tail_env = os.environ.get("NO_RISK_ENFORCE_TAIL", "").strip().lower()
if strict_tail_env in {"1", "true", "yes", "on"}:
    strict_tail = True
elif strict_tail_env in {"0", "false", "no", "off"}:
    strict_tail = False
else:
    strict_tail = snapshot_enforce_tail if snapshot_enforce_tail is not None else True

if tail_rows:
    balanced = True
    unfinished_rows = 0
    for s in tail_rows:
        q_up = s.get("qty_up")
        q_dn = s.get("qty_down")
        if not isinstance(q_up, (int, float)) or not isinstance(q_dn, (int, float)):
            balanced = False
            break
        if abs(float(q_up) - float(q_dn)) > 1e-9:
            risk_unfinished = bool(s.get("risk_unfinished"))
            if strict_tail and risk_unfinished:
                unfinished_rows += 1
                continue
            balanced = False
            break
    if strict_tail:
        fails += out(
            balanced,
            f"tail_close strict check (rows={len(tail_rows)} unfinished={unfinished_rows})",
        )
    else:
        fails += out(balanced, f"tail_close qty_up == qty_down (rows={len(tail_rows)})")
else:
    fails += out(False, "tail_close balance check (no snapshots with time_left_secs<=180)")

strict_zero_env = os.environ.get("NO_RISK_STRICT_ZERO_UNMATCHED", "").strip().lower()
if strict_zero_env in {"1", "true", "yes", "on"}:
    strict_zero_unmatched = True
elif strict_zero_env in {"0", "false", "no", "off"}:
    strict_zero_unmatched = False
else:
    strict_zero_unmatched = (
        snapshot_strict_zero_unmatched
        if snapshot_strict_zero_unmatched is not None
        else True
    )
unmatched_loss_max = float(os.environ.get("UNMATCHED_LOSS_MAX_USDC", "0"))
if snapshots:
    last = snapshots[-1]
    q_up = last.get("qty_up")
    q_dn = last.get("qty_down")
    c_up = last.get("cost_up")
    c_dn = last.get("cost_down")
    if all(isinstance(v, (int, float)) for v in (q_up, q_dn, c_up, c_dn)):
        q_up = float(q_up)
        q_dn = float(q_dn)
        c_up = float(c_up)
        c_dn = float(c_dn)
        final_abs_net = abs(q_up - q_dn)
        if q_up > q_dn and q_up > 1e-9:
            unmatched_loss = c_up * ((q_up - q_dn) / q_up)
        elif q_dn > q_up and q_dn > 1e-9:
            unmatched_loss = c_dn * ((q_dn - q_up) / q_dn)
        else:
            unmatched_loss = 0.0
        if strict_zero_unmatched:
            fails += out(
                final_abs_net <= 1e-9,
                f"final_abs_net == 0 (actual={final_abs_net:.6f})",
            )
        fails += out(
            unmatched_loss <= unmatched_loss_max + 1e-9,
            f"unmatched_loss_usdc <= {unmatched_loss_max:.6f} (actual={unmatched_loss:.6f})",
        )
    else:
        fails += out(False, "strict unmatched checks (missing qty/cost in final snapshot)")
else:
    fails += out(False, "strict unmatched checks (missing snapshots)")

pair_cost_final = None
if snapshots:
    last = snapshots[-1]
    pc = last.get("pair_cost")
    if isinstance(pc, (int, float)):
        pair_cost_final = float(pc)

last_risk_unfinished = bool(snapshots[-1].get("risk_unfinished")) if snapshots else False
if pair_cost_final is not None:
    fails += out(pair_cost_final < 1.0, f"pair_cost_final < 1.0 (actual={pair_cost_final:.6f})")
else:
    if no_opportunity_window:
        fails += out(True, "pair_cost_final check skipped (no_opportunity_window=true)")
    elif strict_tail and last_risk_unfinished:
        fails += out(True, "pair_cost_final check skipped (strict tail risk_unfinished=true)")
    else:
        fails += out(False, "pair_cost_final check (missing numeric pair_cost)")

if no_opportunity_window:
    fails += out(True, "spent_utilization checks skipped (no_opportunity_window=true)")
else:
    min_spent_total = float(os.environ.get("MIN_SPENT_TOTAL_USDC", "15"))
    min_spent_ratio = float(os.environ.get("MIN_SPENT_RATIO", "0.15"))
    spent_total = final_spent_total_usdc if final_spent_total_usdc is not None else 0.0
    total_budget = final_total_budget_usdc if final_total_budget_usdc not in (None, 0.0) else None
    spent_ratio = (spent_total / total_budget) if total_budget and total_budget > 0 else None
    spent_ok = spent_total + 1e-9 >= min_spent_total
    ratio_ok = (spent_ratio is not None) and (spent_ratio + 1e-9 >= min_spent_ratio)
    fails += out(
        spent_ok or ratio_ok,
        "spent utilization "
        f"(spent_total={spent_total:.6f} min_total={min_spent_total:.6f} "
        f"spent_ratio={(spent_ratio if spent_ratio is not None else float('nan')):.6f} "
        f"min_ratio={min_spent_ratio:.6f})",
    )
    if not (spent_ok or ratio_ok):
        can_start_ratio = (
            (can_start_true_count / snapshot_count) if snapshot_count > 0 else 0.0
        )
        top_blocks = ",".join(
            [f"{k}:{v}" for k, v in block_reason_counts.most_common(5)]
        ) or "none"
        fails += out(
            True,
            f"spent_utilization_diagnostic can_start_new_round_true_ratio={can_start_ratio:.4f} block_reason_top={top_blocks}",
        )

if fails:
    print(f"FAILED: {fails} checks failed")
    sys.exit(1)

print("PASS: round quality checks passed")
PY

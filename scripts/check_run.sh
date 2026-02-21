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
  usage
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required. Install jq and retry."
  exit 1
fi

fail_count=0

pass() {
  echo "PASS: $1"
}

fail() {
  echo "FAIL: $1"
  fail_count=$((fail_count + 1))
}

info() {
  echo "INFO: $1"
}

# -------------------------
# Check A: JSON parse on first N lines
# -------------------------
N=200
parse_ok=true
line_count=0
while IFS= read -r line; do
  line_count=$((line_count + 1))
  if [[ -z "$line" ]]; then
    parse_ok=false
    break
  fi
  if ! echo "$line" | jq -e . >/dev/null 2>&1; then
    parse_ok=false
    break
  fi
done < <(head -n "$N" "$FILE")

if [[ "$parse_ok" == "true" ]]; then
  pass "json_parse (${line_count} lines)"
else
  fail "json_parse (${line_count} lines)"
fi

# -------------------------
# Check A2: fatal_error must not exist
# -------------------------
fatal_count="$(jq -rs '[ .[] | select(.kind=="fatal_error") ] | length' "$FILE")"
if [[ "$fatal_count" -gt 0 ]]; then
  fatal_info="$(jq -rs '
    [ .[] | select(.kind=="fatal_error") ][0].data
    | {where, message, market_slug}
  ' "$FILE")"
  fail "fatal_error present (count=$fatal_count info=$fatal_info)"
else
  pass "no fatal_error"
fi

# NOTE:
# All following checks use `jq -s` (slurp) so the entire JSONL becomes
# one array, and each count is a single integer (avoids multi-line 0/1 outputs).

# -------------------------
# Check B: deny_reason must not contain legacy risk_limit
# -------------------------
risk_count="$(jq -rs '
  [ .[]
    | select(.kind=="dryrun_candidates")
    | .data.candidates[]?
    | (.deny_reason // empty)
    | tostring
    | select(test("risk_limit"))
  ] | length
' "$FILE")"

if [[ "$risk_count" -eq 0 ]]; then
  pass "no legacy risk_limit deny_reason"
else
  fail "no legacy risk_limit deny_reason (found=$risk_count)"
fi

# -------------------------
# Check B2: deny_reason must not contain net_invest_cap
# -------------------------
net_invest_cap_count="$(jq -rs '
  [ .[]
    | select(.kind=="dryrun_candidates")
    | .data.candidates[]?
    | (.deny_reason // empty)
    | tostring
    | select(.=="net_invest_cap")
  ] | length
' "$FILE")"

if [[ "$net_invest_cap_count" -eq 0 ]]; then
  pass "no legacy net_invest_cap deny_reason"
else
  fail "no legacy net_invest_cap deny_reason (found=$net_invest_cap_count)"
fi

# -------------------------
# Check C: leg_cap fields consistency
# -------------------------
legcap_total="$(jq -rs '
  [ .[]
    | select(.kind=="dryrun_candidates")
    | .data.candidates[]?
    | select(.deny_reason=="leg_cap_value_up" or .deny_reason=="leg_cap_value_down")
  ] | length
' "$FILE")"

legcap_bad="$(jq -rs '
  [ .[]
    | select(.kind=="dryrun_candidates")
    | .data.candidates[]?
    | select(.deny_reason=="leg_cap_value_up" or .deny_reason=="leg_cap_value_down")
    | select(
        (.cap_unhedged_value == null)
        or (.deny_reason=="leg_cap_value_up" and .would_violate_cap_value_up != true)
        or (.deny_reason=="leg_cap_value_down" and .would_violate_cap_value_down != true)
      )
  ] | length
' "$FILE")"

if [[ "$legcap_bad" -eq 0 ]]; then
  pass "leg_cap fields consistent (matches=$legcap_total)"
else
  fail "leg_cap fields consistent (bad=$legcap_bad matches=$legcap_total)"
fi

# --- Check D (Option A, robust) ---
base_name="$(basename "$FILE")"

apply_count="$(jq -rs '[ .[] | select(.kind=="dryrun_apply") ] | length' "$FILE")"

best_action_nonnull_count="$(jq -rs '
  [ .[]
    | select(.kind=="dryrun_candidates")
    | select(.data.best_action != null)
  ] | length
' "$FILE")"

sim_order_open_count="$(jq -rs '
  [ .[]
    | select(.kind=="user_ws_order")
    | .data.raw_type
    | select(.=="sim_order_open")
  ] | length
' "$FILE")"

hedge_not_recoverable_deny_count="$(jq -rs '
  [ .[]
    | select(.kind=="dryrun_candidates")
    | .data.candidates[]?
    | (.deny_reason // "")
    | select(.=="hedge_not_recoverable")
  ] | length
' "$FILE")"
hedge_margin_insufficient_deny_count="$(jq -rs '
  [ .[]
    | select(.kind=="dryrun_candidates")
    | .data.candidates[]?
    | (.deny_reason // "")
    | select(.=="hedge_margin_insufficient")
  ] | length
' "$FILE")"
entry_worst_pair_deny_count="$(jq -rs '
  [ .[]
    | select(.kind=="dryrun_candidates")
    | .data.candidates[]?
    | (.deny_reason // "")
    | select(.=="entry_worst_pair")
  ] | length
' "$FILE")"
no_quote_deny_count="$(jq -rs '
  [ .[]
    | select(.kind=="dryrun_candidates")
    | .data.candidates[]?
    | (.deny_reason // "")
    | select(.=="no_quote")
  ] | length
' "$FILE")"
low_fill_deny_count="$(jq -rs '
  [ .[]
    | select(.kind=="dryrun_candidates")
    | .data.candidates[]?
    | (.deny_reason // "")
    | select(.=="low_maker_fill_prob")
  ] | length
' "$FILE")"
total_candidate_deny_count="$(jq -rs '
  [ .[]
    | select(.kind=="dryrun_candidates")
    | .data.candidates[]?
    | (.deny_reason // "")
    | select(. != "")
  ] | length
' "$FILE")"
noop_deny_ratio_min="${NOOP_DENY_RATIO_MIN:-0.70}"
noop_min_denies="${NOOP_MIN_DENIES:-10}"
noop_include_entry_worst_pair_lc="$(printf '%s' "${NOOP_INCLUDE_ENTRY_WORST_PAIR:-true}" | tr '[:upper:]' '[:lower:]')"
if [[ "$noop_include_entry_worst_pair_lc" =~ ^(1|true|yes|on)$ ]]; then
  opportunity_deny_count="$((hedge_not_recoverable_deny_count + hedge_margin_insufficient_deny_count + entry_worst_pair_deny_count))"
else
  opportunity_deny_count="$((hedge_not_recoverable_deny_count + hedge_margin_insufficient_deny_count))"
fi
opportunity_deny_ratio="$(
  OPP_DENY="$opportunity_deny_count" TOTAL_DENY="$total_candidate_deny_count" python3 - <<'PY'
import os
o = float(os.environ.get("OPP_DENY", "0") or 0)
t = float(os.environ.get("TOTAL_DENY", "0") or 0)
print(f"{(o / t) if t > 0 else 0.0:.8f}")
PY
)"
no_quote_deny_ratio="$(
  NOQ_DENY="$no_quote_deny_count" TOTAL_DENY="$total_candidate_deny_count" python3 - <<'PY'
import os
nq = float(os.environ.get("NOQ_DENY", "0") or 0)
t = float(os.environ.get("TOTAL_DENY", "0") or 0)
print(f"{(nq / t) if t > 0 else 0.0:.8f}")
PY
)"
no_quote_dominant_ratio_min="${NO_QUOTE_DOMINANT_RATIO_MIN:-0.60}"
low_fill_dominant_ratio_min="${LOW_FILL_DOMINANT_RATIO_MIN:-0.60}"
low_fill_deny_ratio="$(
  LF_DENY="$low_fill_deny_count" TOTAL_DENY="$total_candidate_deny_count" python3 - <<'PY'
import os
lf = float(os.environ.get("LF_DENY", "0") or 0)
t = float(os.environ.get("TOTAL_DENY", "0") or 0)
print(f"{(lf / t) if t > 0 else 0.0:.8f}")
PY
)"

no_opportunity_window="false"
if [[ "$apply_count" -eq 0 \
  && "$best_action_nonnull_count" -eq 0 \
  && "$sim_order_open_count" -eq 0 \
  && "$total_candidate_deny_count" -ge "$noop_min_denies" \
  && "$(OPP_RATIO="$opportunity_deny_ratio" MIN_RATIO="$noop_deny_ratio_min" python3 - <<'PY'
import os
ratio = float(os.environ.get("OPP_RATIO", "0") or 0)
threshold = float(os.environ.get("MIN_RATIO", "0.70") or 0.70)
print("1" if ratio + 1e-12 >= threshold else "0")
PY
)" -eq 1 ]]; then
  no_opportunity_window="true"
fi

# Collect top deny reasons (from candidates list) for diagnosis
deny_reason_top="$(jq -rs '
  [ .[]
    | select(.kind=="dryrun_candidates")
    | .data.candidates[]?
    | (.deny_reason // "null")
  ]
  | group_by(.)
  | map({deny_reason: .[0], count: length})
  | sort_by(-.count)
  | .[:6]
' "$FILE" 2>/dev/null || echo '[]')"

if [[ "$base_name" == *paper* ]]; then
  skip_plot="$(printf '%s' "${CHECK_SUMMARY_SKIP_PLOT:-true}" | tr '[:upper:]' '[:lower:]')"
  if [[ "$skip_plot" =~ ^(1|true|yes|on)$ ]]; then
    if python3 scripts/summary_run.py "$FILE" >/dev/null 2>&1; then
      pass "summary_run generated CSV/MD"
    else
      fail "summary_run failed"
    fi
  else
    if python3 scripts/summary_run.py "$FILE" --plot >/dev/null 2>&1; then
      pass "summary_run generated CSV/MD"
    else
      fail "summary_run failed"
    fi
  fi

  if [[ "$apply_count" -gt 0 ]]; then
    pass "paper has dryrun_apply (count=$apply_count)"
  elif [[ "$best_action_nonnull_count" -gt 0 ]]; then
    pass "paper has best_action (apply_count=$apply_count best_action_nonnull=$best_action_nonnull_count)"
  elif [[ "$no_opportunity_window" == "true" ]]; then
    pass "paper no_opportunity_window=true (opportunity_denies=$opportunity_deny_count hedge_not_recoverable_denies=$hedge_not_recoverable_deny_count hedge_margin_insufficient_denies=$hedge_margin_insufficient_deny_count entry_worst_pair_denies=$entry_worst_pair_deny_count total_denies=$total_candidate_deny_count ratio=$opportunity_deny_ratio min_ratio=$noop_deny_ratio_min min_denies=$noop_min_denies include_entry_worst_pair=$noop_include_entry_worst_pair_lc)"
  elif [[ "$apply_count" -eq 0 \
      && "$best_action_nonnull_count" -eq 0 \
      && "$sim_order_open_count" -eq 0 \
      && "$total_candidate_deny_count" -ge "$noop_min_denies" \
      && "$(NOQ_RATIO="$no_quote_deny_ratio" MIN_RATIO="$no_quote_dominant_ratio_min" python3 - <<'PY'
import os
ratio = float(os.environ.get("NOQ_RATIO", "0") or 0)
threshold = float(os.environ.get("MIN_RATIO", "0.60") or 0.60)
print("1" if ratio + 1e-12 >= threshold else "0")
PY
)" -eq 1 ]]; then
    fail "paper no_quote_dominant_window=true (no_quote_denies=$no_quote_deny_count total_denies=$total_candidate_deny_count ratio=$no_quote_deny_ratio min_ratio=$no_quote_dominant_ratio_min) deny_reason_top=$deny_reason_top"
  elif [[ "$apply_count" -eq 0 \
      && "$best_action_nonnull_count" -eq 0 \
      && "$sim_order_open_count" -eq 0 \
      && "$total_candidate_deny_count" -ge "$noop_min_denies" \
      && "$(LOW_FILL_RATIO="$low_fill_deny_ratio" MIN_RATIO="$low_fill_dominant_ratio_min" python3 - <<'PY'
import os
ratio = float(os.environ.get("LOW_FILL_RATIO", "0") or 0)
threshold = float(os.environ.get("MIN_RATIO", "0.60") or 0.60)
print("1" if ratio + 1e-12 >= threshold else "0")
PY
)" -eq 1 ]]; then
    fail "paper low_fill_dominant_window=true (low_fill_denies=$low_fill_deny_count total_denies=$total_candidate_deny_count ratio=$low_fill_deny_ratio min_ratio=$low_fill_dominant_ratio_min) deny_reason_top=$deny_reason_top"
  else
    fail "paper must have dryrun_apply OR best_action (apply_count=$apply_count best_action_nonnull=$best_action_nonnull_count no_quote_denies=$no_quote_deny_count low_fill_denies=$low_fill_deny_count total_denies=$total_candidate_deny_count no_quote_ratio=$no_quote_deny_ratio low_fill_ratio=$low_fill_deny_ratio) deny_reason_top=$deny_reason_top"
  fi

  taker_best_count="$(jq -rs '
    [ .[]
      | select(.kind=="dryrun_candidates")
      | .data.best_action?
      | select(. != null)
      | .kind
      | select(.=="TAKER")
    ] | length
  ' "$FILE")"
  taker_applied_count="$(jq -rs '
    [ .[]
      | select(.kind=="dryrun_candidates")
      | .data.applied_action?
      | select(. != null)
      | .kind
      | select(.=="TAKER")
    ] | length
  ' "$FILE")"
  if [[ "$taker_best_count" -eq 0 && "$taker_applied_count" -eq 0 ]]; then
    pass "taker=0 (best_action/applied_action)"
  else
    fail "taker=0 (best_action=$taker_best_count applied_action=$taker_applied_count)"
  fi

  decisions_csv="${FILE%.jsonl}_decisions.csv"
  timeseries_csv="${FILE%.jsonl}_timeseries.csv"
  snapshot_total_budget="$(jq -rs '
    [ .[]
      | select(.kind=="dryrun_snapshot")
      | .data.total_budget_usdc
      | select(type=="number")
    ] | first // empty
  ' "$FILE")"
  snapshot_round_budget="$(jq -rs '
    [ .[]
      | select(.kind=="dryrun_snapshot")
      | .data.round_budget_usdc
      | select(type=="number")
    ] | first // empty
  ' "$FILE")"
  snapshot_no_risk_hard_pair_cap="$(jq -rs '
    [ .[]
      | select(.kind=="dryrun_snapshot")
      | .data.no_risk_hard_pair_cap
      | select(type=="number")
    ] | first // empty
  ' "$FILE")"
  snapshot_no_risk_enforce_tail="$(jq -rs '
    [ .[]
      | select(.kind=="dryrun_snapshot")
      | .data.no_risk_enforce_tail
      | select(type=="boolean")
    ] | first // empty
  ' "$FILE")"

  total_budget="${TOTAL_BUDGET_USDC:-${MAX_NET_INVEST_USDC:-${snapshot_total_budget:-10}}}"
  max_rounds="${MAX_ROUNDS:-2}"
  round_budget="${ROUND_BUDGET_USDC:-${snapshot_round_budget:-}}"
  if [[ -z "$round_budget" ]]; then
    round_budget="$(TOTAL_BUDGET_USDC="$total_budget" MAX_ROUNDS="$max_rounds" python3 - <<'PY'
import os
total = float(os.environ.get("TOTAL_BUDGET_USDC", os.environ.get("MAX_NET_INVEST_USDC", "10")))
max_rounds = int(os.environ.get("MAX_ROUNDS", "2"))
max_rounds = max(1, max_rounds)
print(total / max_rounds)
PY
)"
  fi
  if FILE_JSONL="$FILE" TOTAL_BUDGET_USDC="$total_budget" ROUND_BUDGET_USDC="$round_budget" python3 - <<'PY'
import json
import os
import sys

jsonl_file = os.environ.get("FILE_JSONL", "")
total_cap = float(os.environ.get("TOTAL_BUDGET_USDC", "10"))
round_cap = float(os.environ.get("ROUND_BUDGET_USDC", "5"))

if not jsonl_file or not os.path.exists(jsonl_file):
    print(f"missing jsonl file: {jsonl_file}", file=sys.stderr)
    sys.exit(1)

def parse_float(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return None
    try:
        return float(value)
    except Exception:
        return None

max_total_spent = 0.0
max_round_spent = {}
with open(jsonl_file) as fh:
    for line in fh:
        if not line.strip():
            continue
        obj = json.loads(line)
        if obj.get("kind") != "dryrun_snapshot":
            continue
        data = obj.get("data", {})
        total_spent = parse_float(data.get("spent_total_usdc"))
        if total_spent is not None:
            max_total_spent = max(max_total_spent, total_spent)
        round_spent = parse_float(data.get("spent_round_usdc"))
        round_idx = data.get("round_idx")
        if round_spent is not None:
            if isinstance(round_idx, (int, float)):
                round_key = str(int(round_idx))
            elif round_idx is not None and str(round_idx).strip() != "":
                round_key = str(round_idx).strip()
            else:
                round_key = "unknown"
            current = max_round_spent.get(round_key, 0.0)
            if round_spent > current:
                max_round_spent[round_key] = round_spent

if max_total_spent > total_cap + 1e-9:
    print(
        f"total_budget_cap exceeded total_spent={max_total_spent} total_cap={total_cap}",
        file=sys.stderr,
    )
    sys.exit(1)

for idx, spent in max_round_spent.items():
    if spent > round_cap + 1e-9:
        print(
            f"round_budget_cap exceeded round_idx={idx} spent={spent} round_cap={round_cap}",
            file=sys.stderr,
        )
        sys.exit(1)
sys.exit(0)
PY
  then
    pass "total/round budget caps (total=${total_budget} round=${round_budget})"
  else
    fail "total/round budget caps (total=${total_budget} round=${round_budget})"
  fi

  no_risk_hard_cap="${NO_RISK_HARD_PAIR_CAP:-${snapshot_no_risk_hard_pair_cap:-0.995}}"
  if FILE_JSONL="$FILE" NO_RISK_HARD_PAIR_CAP="$no_risk_hard_cap" python3 - <<'PY'
import json
import os
import sys

jsonl_file = os.environ.get("FILE_JSONL", "")
hard_cap = float(os.environ.get("NO_RISK_HARD_PAIR_CAP", "0.995"))
if not jsonl_file or not os.path.exists(jsonl_file):
    print(f"missing jsonl file: {jsonl_file}", file=sys.stderr)
    sys.exit(1)

max_exec_pair = None
for line in open(jsonl_file):
    if not line.strip():
        continue
    obj = json.loads(line)
    if obj.get("kind") != "dryrun_apply":
        continue
    data = obj.get("data", {})
    if data.get("executed") is False:
        continue
    action = data.get("applied_action") if isinstance(data.get("applied_action"), dict) else {}
    pair_cost = action.get("sim_pair_cost")
    if isinstance(pair_cost, (int, float)):
        pair_cost = float(pair_cost)
        if max_exec_pair is None or pair_cost > max_exec_pair:
            max_exec_pair = pair_cost

if max_exec_pair is not None and max_exec_pair > 1.0 + 1e-9:
    print(f"executed sim_pair_cost exceeded 1.0 (max={max_exec_pair})", file=sys.stderr)
    sys.exit(1)
if max_exec_pair is not None and max_exec_pair > hard_cap + 1e-9:
    print(
        f"executed sim_pair_cost exceeded no_risk_hard_pair_cap "
        f"(max={max_exec_pair} hard_cap={hard_cap})",
        file=sys.stderr,
    )
    sys.exit(1)
sys.exit(0)
PY
  then
    pass "executed sim_pair_cost cap (<=1.0 and <=${no_risk_hard_cap})"
  else
    fail "executed sim_pair_cost cap (<=1.0 and <=${no_risk_hard_cap})"
  fi

  tail_close_secs="${DRYRUN_TAIL_CLOSE_SECS:-180}"
  pair_wait_secs="${ROUND_PAIR_WAIT_SECS:-5}"
  if FILE_JSONL="$FILE" TIMESERIES_CSV="$timeseries_csv" DRYRUN_TAIL_CLOSE_SECS="$tail_close_secs" ROUND_PAIR_WAIT_SECS="$pair_wait_secs" PAPER_RESTING_FILL_ENABLED="${PAPER_RESTING_FILL_ENABLED:-}" PAPER_ORDER_TIMEOUT_SECS="${PAPER_ORDER_TIMEOUT_SECS:-0}" python3 - <<'PY'
import csv
import json
import os
import sys

jsonl_file = os.environ.get("FILE_JSONL", "")
timeseries = os.environ.get("TIMESERIES_CSV", "")
tail_close = float(os.environ.get("DRYRUN_TAIL_CLOSE_SECS", "180"))
pair_wait = float(os.environ.get("ROUND_PAIR_WAIT_SECS", "5"))
paper_timeout = float(os.environ.get("PAPER_ORDER_TIMEOUT_SECS", "0") or 0)
stall_timeout_mult = float(os.environ.get("ROUND_STALL_TIMEOUT_MULT", "10") or 10)
resting_env = os.environ.get("PAPER_RESTING_FILL_ENABLED", "").strip().lower()

if not timeseries or not os.path.exists(timeseries):
    print(f"missing timeseries csv: {timeseries}", file=sys.stderr)
    sys.exit(1)

def parse_bool(value):
    return value in {"1", "true", "yes", "on"}

resting_enabled = parse_bool(resting_env) if resting_env else False
if jsonl_file and os.path.exists(jsonl_file):
    with open(jsonl_file) as fh:
        for line in fh:
            if not line.strip():
                continue
            obj = json.loads(line)
            if obj.get("kind") != "dryrun_snapshot":
                continue
            data = obj.get("data", {})
            value = data.get("paper_resting_fill_enabled")
            if isinstance(value, bool):
                resting_enabled = value
            timeout_value = data.get("paper_order_timeout_secs")
            if isinstance(timeout_value, (int, float)) and timeout_value > 0:
                paper_timeout = float(timeout_value)
            break

stall_timeout = pair_wait
if resting_enabled and paper_timeout > 0:
    stall_timeout = max(pair_wait, paper_timeout * stall_timeout_mult)

def parse_float(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return None
    try:
        return float(value)
    except Exception:
        return None

with open(timeseries, newline="") as fh:
    reader = csv.DictReader(fh)
    leg1_progress_ts = None
    leg1_last_filled_qty = None
    for row in reader:
        time_left = parse_float(row.get("time_left_secs"))
        if time_left is None:
            continue
        round_state = row.get("round_state") or ""
        ts_ms = parse_float(row.get("ts_ms"))
        if time_left > tail_close and round_state == "leg1_accumulating":
            if ts_ms is None:
                continue
            filled_qty = parse_float(row.get("round_leg1_filled_qty"))
            if leg1_progress_ts is None:
                leg1_progress_ts = ts_ms
                leg1_last_filled_qty = filled_qty
            else:
                # New semantics: only fail when leg1 accumulation makes no progress for too long.
                if (
                    filled_qty is not None
                    and leg1_last_filled_qty is not None
                    and filled_qty > leg1_last_filled_qty + 1e-9
                ):
                    leg1_progress_ts = ts_ms
                    leg1_last_filled_qty = filled_qty
                elif filled_qty is not None and leg1_last_filled_qty is None:
                    leg1_progress_ts = ts_ms
                    leg1_last_filled_qty = filled_qty

            if leg1_progress_ts is not None and ts_ms - leg1_progress_ts > stall_timeout * 1000.0:
                print(
                    "leg1_accumulating stalled too long "
                    f"ts_ms={row.get('ts_ms')} "
                    f"decision_seq={row.get('decision_seq')} "
                    f"time_left_secs={time_left} "
                    f"round_state={round_state} "
                    f"round_leg1_filled_qty={filled_qty} "
                    f"stall_timeout_secs={stall_timeout}",
                    file=sys.stderr,
                )
                sys.exit(1)
        else:
            leg1_progress_ts = None
            leg1_last_filled_qty = None
sys.exit(0)
PY
  then
    pass "leg1_accumulating stall check"
  else
    fail "leg1_accumulating stall check"
  fi

  no_risk_enforce_tail="${NO_RISK_ENFORCE_TAIL:-${snapshot_no_risk_enforce_tail:-true}}"
  if FILE_JSONL="$FILE" DRYRUN_TAIL_CLOSE_SECS="$tail_close_secs" NO_RISK_ENFORCE_TAIL="$no_risk_enforce_tail" python3 - <<'PY'
import os
import sys
import json

jsonl_file = os.environ.get("FILE_JSONL", "")
tail_close = float(os.environ.get("DRYRUN_TAIL_CLOSE_SECS", "180"))
strict_tail = str(os.environ.get("NO_RISK_ENFORCE_TAIL", "true")).strip().lower() in {"1","true","yes","on"}

if not jsonl_file or not os.path.exists(jsonl_file):
    print(f"missing jsonl file: {jsonl_file}", file=sys.stderr)
    sys.exit(1)

def parse_float(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return None
    try:
        return float(value)
    except Exception:
        return None

tail_rows = 0
unfinished_rows = 0
with open(jsonl_file) as fh:
    for line in fh:
        if not line.strip():
            continue
        obj = json.loads(line)
        if obj.get("kind") != "dryrun_snapshot":
            continue
        row = obj.get("data", {})
        time_left = parse_float(row.get("time_left_secs"))
        if time_left is None or time_left > tail_close:
            continue
        tail_rows += 1
        qty_up = parse_float(row.get("qty_up"))
        qty_down = parse_float(row.get("qty_down"))
        balanced = qty_up is not None and qty_down is not None and abs(qty_up - qty_down) <= 1e-9
        if balanced:
            continue
        risk_unfinished = bool(row.get("risk_unfinished"))
        if strict_tail and risk_unfinished:
            unfinished_rows += 1
            continue
        detail = (
            f"tail_close imbalance ts_ms={obj.get('ts_ms')} "
            f"decision_seq={row.get('decision_seq')} "
            f"time_left_secs={time_left} qty_up={qty_up} qty_down={qty_down} "
            f"risk_unfinished={risk_unfinished}"
        )
        if strict_tail:
            print(
                "tail_close imbalance without risk_unfinished flag " + detail,
                file=sys.stderr,
            )
        else:
            print(detail, file=sys.stderr)
        sys.exit(1)
if tail_rows == 0:
    print("tail_close balance check (no snapshots with time_left_secs<=tail_close)", file=sys.stderr)
    sys.exit(1)
if strict_tail and unfinished_rows > 0:
    print(f"tail_close strict mode accepted unfinished rows={unfinished_rows}", file=sys.stderr)
sys.exit(0)
PY
  then
    no_risk_enforce_tail_lc="$(printf '%s' "$no_risk_enforce_tail" | tr '[:upper:]' '[:lower:]')"
    if [[ "$no_risk_enforce_tail_lc" =~ ^(1|true|yes|on)$ ]]; then
      pass "tail_close strict check (balanced or risk_unfinished, <=${tail_close_secs}s)"
    else
      pass "tail_close balance (<=${tail_close_secs}s)"
    fi
  else
    no_risk_enforce_tail_lc="$(printf '%s' "$no_risk_enforce_tail" | tr '[:upper:]' '[:lower:]')"
    if [[ "$no_risk_enforce_tail_lc" =~ ^(1|true|yes|on)$ ]]; then
      fail "tail_close strict check (balanced or risk_unfinished, <=${tail_close_secs}s)"
    else
      fail "tail_close balance (<=${tail_close_secs}s)"
    fi
  fi

  max_rounds="${MAX_ROUNDS:-2}"
  if FILE_JSONL="$FILE" MAX_ROUNDS="$max_rounds" NO_OPPORTUNITY_WINDOW="$no_opportunity_window" python3 - <<'PY'
import json
import os
import sys

jsonl_file = os.environ.get("FILE_JSONL", "")
max_rounds = int(os.environ.get("MAX_ROUNDS", "2"))
no_opportunity_window = os.environ.get("NO_OPPORTUNITY_WINDOW", "false").strip().lower() in {"1", "true", "yes", "on"}
if not jsonl_file or not os.path.exists(jsonl_file):
    print(f"missing jsonl file: {jsonl_file}", file=sys.stderr)
    sys.exit(1)

def parse_float(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return None
    try:
        return float(value)
    except Exception:
        return None

max_round_idx = None
apply_count = 0
saw_locked_deny = False
last_apply_ts_ms = None
snapshots = []

with open(jsonl_file) as fh:
    for line in fh:
        if not line.strip():
            continue
        obj = json.loads(line)
        kind = obj.get("kind")
        if kind == "dryrun_snapshot":
            data = obj.get("data", {})
            snapshots.append(data)
            round_idx = data.get("round_idx")
            if isinstance(round_idx, (int, float)):
                round_idx_val = int(round_idx)
                if max_round_idx is None or round_idx_val > max_round_idx:
                    max_round_idx = round_idx_val
        elif kind == "dryrun_apply":
            data = obj.get("data", {})
            if data.get("executed") is False:
                continue
            apply_count += 1
            ts_ms = obj.get("ts_ms")
            if isinstance(ts_ms, (int, float)):
                ts_ms = int(ts_ms)
                if last_apply_ts_ms is None or ts_ms > last_apply_ts_ms:
                    last_apply_ts_ms = ts_ms
        elif kind == "dryrun_candidates":
            data = obj.get("data", {})
            for cand in data.get("candidates", []):
                deny_reason = str(cand.get("deny_reason") or "")
                if deny_reason in (
                    "locked_strict_abs_net",
                    "locked_policy_hold",
                    "locked_max_rounds",
                ):
                    saw_locked_deny = True

final_balanced = False
if snapshots:
    final_qty_up = parse_float(snapshots[-1].get("qty_up"))
    final_qty_down = parse_float(snapshots[-1].get("qty_down"))
    if final_qty_up is not None and final_qty_down is not None:
        final_balanced = abs(final_qty_up - final_qty_down) <= 1e-9

post_apply_unbalanced = False
if last_apply_ts_ms is not None:
    with open(jsonl_file) as fh:
        for line in fh:
            if not line.strip():
                continue
            obj = json.loads(line)
            if obj.get("kind") != "dryrun_snapshot":
                continue
            ts_ms = obj.get("ts_ms")
            if not isinstance(ts_ms, (int, float)) or int(ts_ms) <= last_apply_ts_ms:
                continue
            data = obj.get("data", {})
            unhedged_up = parse_float(data.get("unhedged_up")) or 0.0
            unhedged_down = parse_float(data.get("unhedged_down")) or 0.0
            if unhedged_up > 1e-9 or unhedged_down > 1e-9:
                post_apply_unbalanced = True
                break

lock_hold_complete = (
    (max_round_idx or 0) >= 1
    and apply_count >= 2
    and saw_locked_deny
    and final_balanced
    and not post_apply_unbalanced
)

if no_opportunity_window:
    sys.exit(0)

if max_rounds >= 2:
    rounds_progressed = (max_round_idx or 0) >= 2 or apply_count >= 4
    if not rounds_progressed and not lock_hold_complete:
        print(
            "insufficient rounds: "
            f"max_round_idx={max_round_idx} "
            f"apply_count={apply_count} "
            f"saw_locked_deny={saw_locked_deny} "
            f"final_balanced={final_balanced} "
            f"post_apply_unbalanced={post_apply_unbalanced}",
            file=sys.stderr,
        )
        sys.exit(1)
sys.exit(0)
PY
  then
    pass "rounds progressed (max_rounds=${max_rounds})"
  else
    fail "rounds progressed (max_rounds=${max_rounds})"
  fi
fi
# --- end Check D ---

# -------------------------
# Check E: fixed_market_slug should be constant
# -------------------------
fixed_snapshots="$(jq -rs '
  [ .[]
    | select(.kind=="dryrun_snapshot")
    | select(.data.market_select_mode=="fixed_market_slug")
    | .data.market_slug
  ] | length
' "$FILE")"

if [[ "$fixed_snapshots" -gt 0 ]]; then
  fixed_unique="$(jq -rs '
    [ .[]
      | select(.kind=="dryrun_snapshot")
      | select(.data.market_select_mode=="fixed_market_slug")
      | .data.market_slug
    ] | unique | length
  ' "$FILE")"
  if [[ "$fixed_unique" -eq 1 ]]; then
    pass "fixed_market_slug constant market_slug (count=$fixed_snapshots)"
  else
    fail "fixed_market_slug constant market_slug (unique=$fixed_unique count=$fixed_snapshots)"
  fi
else
  info "fixed_market_slug check skipped (no fixed-mode snapshots)"
fi

if [[ "$fail_count" -gt 0 ]]; then
  echo "FAILED: ${fail_count} checks failed"
  exit 1
fi

echo "PASS: all checks passed"
exit 0

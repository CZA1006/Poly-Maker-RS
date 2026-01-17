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
  if python3 scripts/summary_run.py "$FILE" >/dev/null 2>&1; then
    pass "summary_run generated CSV/MD"
  else
    fail "summary_run failed"
  fi

  if [[ "$apply_count" -gt 0 ]]; then
    pass "paper has dryrun_apply (count=$apply_count)"
  elif [[ "$best_action_nonnull_count" -gt 0 ]]; then
    pass "paper has best_action (apply_count=$apply_count best_action_nonnull=$best_action_nonnull_count)"
  else
    fail "paper must have dryrun_apply OR best_action (apply_count=$apply_count best_action_nonnull=$best_action_nonnull_count) deny_reason_top=$deny_reason_top"
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
  total_budget="${TOTAL_BUDGET_USDC:-${MAX_NET_INVEST_USDC:-10}}"
  max_rounds="${MAX_ROUNDS:-2}"
  round_budget="${ROUND_BUDGET_USDC:-}"
  if [[ -z "$round_budget" ]]; then
    round_budget="$(python3 - <<'PY'
import os
total = float(os.environ.get("TOTAL_BUDGET_USDC", os.environ.get("MAX_NET_INVEST_USDC", "10")))
max_rounds = int(os.environ.get("MAX_ROUNDS", "2"))
max_rounds = max(1, max_rounds)
print(total / max_rounds)
PY
)"
  fi
  if DECISIONS_CSV="$decisions_csv" TOTAL_BUDGET_USDC="$total_budget" ROUND_BUDGET_USDC="$round_budget" python3 - <<'PY'
import csv
import os
import sys

decisions = os.environ.get("DECISIONS_CSV", "")
total_cap = float(os.environ.get("TOTAL_BUDGET_USDC", "10"))
round_cap = float(os.environ.get("ROUND_BUDGET_USDC", "5"))

if not decisions or not os.path.exists(decisions):
    print(f"missing decisions csv: {decisions}", file=sys.stderr)
    sys.exit(1)

def parse_float(value):
    if value is None:
        return None
    value = value.strip()
    if value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None

total_spent = 0.0
round_spent = {}
with open(decisions, newline="") as fh:
    reader = csv.DictReader(fh)
    for row in reader:
        qty = parse_float(row.get("applied_action_qty"))
        price = parse_float(row.get("applied_action_fill_price"))
        if qty is None or price is None:
            continue
        spend = qty * price
        total_spent += spend
        round_idx = row.get("round_idx")
        if round_idx is None or str(round_idx).strip() == "":
            print(f"round_idx missing decision_seq={row.get('decision_seq')}", file=sys.stderr)
            sys.exit(1)
        round_spent.setdefault(round_idx, 0.0)
        round_spent[round_idx] += spend

if total_spent > total_cap + 1e-9:
    print(
        f"total_budget_cap exceeded total_spent={total_spent} total_cap={total_cap}",
        file=sys.stderr,
    )
    sys.exit(1)

for idx, spent in round_spent.items():
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

  tail_close_secs="${DRYRUN_TAIL_CLOSE_SECS:-180}"
  pair_wait_secs="${ROUND_PAIR_WAIT_SECS:-5}"
  if TIMESERIES_CSV="$timeseries_csv" DRYRUN_TAIL_CLOSE_SECS="$tail_close_secs" ROUND_PAIR_WAIT_SECS="$pair_wait_secs" python3 - <<'PY'
import csv
import os
import sys

timeseries = os.environ.get("TIMESERIES_CSV", "")
tail_close = float(os.environ.get("DRYRUN_TAIL_CLOSE_SECS", "180"))
pair_wait = float(os.environ.get("ROUND_PAIR_WAIT_SECS", "5"))

if not timeseries or not os.path.exists(timeseries):
    print(f"missing timeseries csv: {timeseries}", file=sys.stderr)
    sys.exit(1)

def parse_float(value):
    if value is None:
        return None
    value = value.strip()
    if value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None

with open(timeseries, newline="") as fh:
    reader = csv.DictReader(fh)
    leg1_start_ts = None
    for row in reader:
        time_left = parse_float(row.get("time_left_secs"))
        if time_left is None:
            continue
        round_state = row.get("round_state") or ""
        ts_ms = parse_float(row.get("ts_ms"))
        if time_left > tail_close and round_state == "leg1_filled":
            if ts_ms is not None and leg1_start_ts is None:
                leg1_start_ts = ts_ms
            if ts_ms is not None and leg1_start_ts is not None:
                if ts_ms - leg1_start_ts > pair_wait * 1000.0:
                    print(
                        "leg1_filled too long "
                        f"ts_ms={row.get('ts_ms')} "
                        f"decision_seq={row.get('decision_seq')} "
                        f"time_left_secs={time_left} "
                        f"round_state={round_state}",
                        file=sys.stderr,
                    )
                    sys.exit(1)
        else:
            leg1_start_ts = None
        if time_left <= tail_close:
            qty_up = parse_float(row.get("qty_up"))
            qty_down = parse_float(row.get("qty_down"))
            if qty_up is None or qty_down is None or qty_up != qty_down:
                print(
                    "tail_close imbalance "
                    f"ts_ms={row.get('ts_ms')} "
                    f"decision_seq={row.get('decision_seq')} "
                    f"time_left_secs={time_left} "
                    f"qty_up={qty_up} qty_down={qty_down}",
                    file=sys.stderr,
                )
                sys.exit(1)
sys.exit(0)
PY
  then
    pass "tail_close balance (<=${tail_close_secs}s)"
  else
    fail "tail_close balance (<=${tail_close_secs}s)"
  fi

  max_rounds="${MAX_ROUNDS:-2}"
  if DECISIONS_CSV="$decisions_csv" MAX_ROUNDS="$max_rounds" python3 - <<'PY'
import csv
import os
import sys

decisions = os.environ.get("DECISIONS_CSV", "")
max_rounds = int(os.environ.get("MAX_ROUNDS", "2"))
if not decisions or not os.path.exists(decisions):
    print(f"missing decisions csv: {decisions}", file=sys.stderr)
    sys.exit(1)

max_round_idx = None
apply_count = 0
with open(decisions, newline="") as fh:
    reader = csv.DictReader(fh)
    for row in reader:
        round_idx = row.get("round_idx")
        if round_idx is not None and str(round_idx).strip() != "":
            try:
                round_idx_val = int(float(round_idx))
            except Exception:
                continue
            if max_round_idx is None or round_idx_val > max_round_idx:
                max_round_idx = round_idx_val
        if row.get("applied_action_action"):
            apply_count += 1

if max_rounds >= 2:
    if (max_round_idx or 0) < 2 and apply_count < 4:
        print(
            f"insufficient rounds: max_round_idx={max_round_idx} apply_count={apply_count}",
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

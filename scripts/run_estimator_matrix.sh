#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

usage() {
  cat <<'EOF'
Usage:
  scripts/run_estimator_matrix.sh [options]

Options:
  --symbol <sym>                Default: btc
  --start-mode <now|prev|next>  Default: next
  --wait-until-window-start     Default: enabled
  --no-wait-until-window-start
  --windows-per-market <n>      Default: 1
  --max-window-attempts <n>     Default: 200
  --run-secs <n>                Default: 930
  --global-total-budget <f>     Default: 10
  --per-market-budget-cap <f>   Default: 10
  --max-rounds <n>              Default: 2
  --round-budget <f>            Default: 5
  --round-leg1-fraction <f>     Default: 0.45
  --tail-freeze-secs <n>        Default: 300
  --tail-close-secs <n>         Default: 180
  --decision-every-ms <n>       Default: 200

Profiles (fixed):
  1) baseline    -> MFE=false
  2) h4_q10      -> MFE=true, horizon=4, queue_mult=1.0
  3) h4_q15      -> MFE=true, horizon=4, queue_mult=1.5

Example:
  scripts/run_estimator_matrix.sh --symbol btc --start-mode next --run-secs 930
EOF
}

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required. Install jq and retry."
  exit 1
fi

if ! command -v python >/dev/null 2>&1; then
  echo "python is required in current environment."
  exit 1
fi

symbol="btc"
start_mode="next"
wait_flag="--wait-until-window-start"
windows_per_market="1"
max_window_attempts="200"
run_secs="930"
global_total_budget="10"
per_market_budget_cap="10"
max_rounds="2"
round_budget="5"
round_leg1_fraction="0.45"
tail_freeze_secs="300"
tail_close_secs="180"
decision_every_ms="200"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --symbol) symbol="$2"; shift 2 ;;
    --start-mode) start_mode="$2"; shift 2 ;;
    --wait-until-window-start) wait_flag="--wait-until-window-start"; shift 1 ;;
    --no-wait-until-window-start) wait_flag="--no-wait-until-window-start"; shift 1 ;;
    --windows-per-market) windows_per_market="$2"; shift 2 ;;
    --max-window-attempts) max_window_attempts="$2"; shift 2 ;;
    --run-secs) run_secs="$2"; shift 2 ;;
    --global-total-budget) global_total_budget="$2"; shift 2 ;;
    --per-market-budget-cap) per_market_budget_cap="$2"; shift 2 ;;
    --max-rounds) max_rounds="$2"; shift 2 ;;
    --round-budget) round_budget="$2"; shift 2 ;;
    --round-leg1-fraction) round_leg1_fraction="$2"; shift 2 ;;
    --tail-freeze-secs) tail_freeze_secs="$2"; shift 2 ;;
    --tail-close-secs) tail_close_secs="$2"; shift 2 ;;
    --decision-every-ms) decision_every_ms="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1"; usage; exit 1 ;;
  esac
done

COMMON_ARGS=(
  --symbols "$symbol"
  --start-mode "$start_mode"
  "$wait_flag"
  --windows-per-market "$windows_per_market"
  --max-window-attempts "$max_window_attempts"
  --parallelism 1
  --global-total-budget "$global_total_budget"
  --per-market-budget-cap "$per_market_budget_cap"
  --budget-strategy equal
  --max-rounds "$max_rounds"
  --round-budget "$round_budget"
  --round-leg1-fraction "$round_leg1_fraction"
  --tail-freeze-secs "$tail_freeze_secs"
  --tail-close-secs "$tail_close_secs"
  --decision-every-ms "$decision_every_ms"
  --run-secs "$run_secs"
)

profiles=(
  "baseline false 8 1.0"
  "h4_q10 true 4 1.0"
  "h4_q15 true 4 1.5"
)

declare -a RUN_DESCRIPTORS=()
baseline_jsonl=""

echo "=== Estimator Matrix Run ==="
echo "symbol=$symbol start_mode=$start_mode wait_flag=$wait_flag run_secs=$run_secs"
echo "common_args=${COMMON_ARGS[*]}"

for row in "${profiles[@]}"; do
  read -r name enabled horizon qmult <<<"$row"
  echo
  echo ">>> profile=$name enabled=$enabled horizon=$horizon queue_mult=$qmult"

  MAKER_FILL_ESTIMATOR_ENABLED="$enabled" \
  MAKER_FILL_HORIZON_SECS="$horizon" \
  MAKER_QUEUE_AHEAD_MULT="$qmult" \
  python scripts/run_multi_paper_safe.py "${COMMON_ARGS[@]}"

  latest_dir="$(ls -td "logs/multi/${symbol}/"* | head -1)"
  jsonl_file="$(ls "$latest_dir"/*_paper.jsonl | head -1)"
  echo "profile=$name latest_dir=$latest_dir"
  echo "profile=$name jsonl=$jsonl_file"

  bash scripts/check_run.sh "$jsonl_file"
  bash scripts/check_fill_estimator.sh "$jsonl_file"

  RUN_DESCRIPTORS+=("${name}=${jsonl_file}")
  if [[ "$name" == "baseline" ]]; then
    baseline_jsonl="$jsonl_file"
  fi
done

echo
echo "=== A/B checks vs baseline ==="
if [[ -z "$baseline_jsonl" ]]; then
  echo "baseline run missing, abort."
  exit 1
fi

for desc in "${RUN_DESCRIPTORS[@]}"; do
  name="${desc%%=*}"
  path="${desc#*=}"
  if [[ "$name" == "baseline" ]]; then
    continue
  fi
  echo "A/B profile=$name vs baseline"
  bash scripts/check_fill_estimator.sh "$path" --baseline "$baseline_jsonl"
done

echo
echo "=== Matrix conclusion ==="
compare_cmd=(python scripts/compare_estimator_matrix.py)
for desc in "${RUN_DESCRIPTORS[@]}"; do
  compare_cmd+=(--run "$desc")
done
"${compare_cmd[@]}"

echo
echo "DONE: estimator matrix finished."

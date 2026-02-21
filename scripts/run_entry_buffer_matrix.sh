#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

usage() {
  cat <<'EOF'
Usage: scripts/run_entry_buffer_matrix.sh [options]

Run multi-window paper tests for multiple ENTRY_PAIR_BUFFER_BPS profiles,
then summarize PnL and pick the best profile.

Options:
  --buffers CSV               Buffer bps list (default: 0,20,30)
  --symbols CSV               Symbols list (default: btc)
  --windows N                 Windows per market per profile (default: 10)
  --out-root DIR              Matrix output root (default: logs/multi_batches)
  --tag NAME                  Matrix tag (default: entry_buffer_matrix_<epoch>)
  --start-mode MODE           now|prev|next (default: next)
  --parallelism N             Runner parallelism (default: 1)
  --max-window-attempts N     Max attempts per market (default: 600)
  --run-secs N                Runtime per window (default: 900)
  --global-total-budget F     Default: 100
  --per-market-budget-cap F   Default: 100
  --budget-strategy STR       equal|weighted (default: equal)
  --max-rounds N              Default: 2
  --round-budget F            Default: 20
  --round-leg1-fraction F     Default: 0.40
  --tail-freeze-secs N        Default: 300
  --tail-close-secs N         Default: 180
  --decision-every-ms N       Default: 200
  --help                      Show this message

Env thresholds for batch acceptance (used by check_multi_pnl.sh):
  TOTAL_PNL_MIN   (default 0.0)
  WINDOW_PNL_MIN  (default -2.0)
  WIN_RATIO_MIN   (default 0.50)
EOF
}

buffers_csv="0,20,30"
symbols_csv="btc"
windows_per_market=10
out_root="logs/multi_batches"
tag="entry_buffer_matrix_$(date +%s)"
start_mode="next"
parallelism=1
max_window_attempts=600
run_secs=900
global_total_budget=100
per_market_budget_cap=100
budget_strategy="equal"
max_rounds=2
round_budget=20
round_leg1_fraction=0.40
tail_freeze_secs=300
tail_close_secs=180
decision_every_ms=200

while [[ $# -gt 0 ]]; do
  case "$1" in
    --buffers) buffers_csv="$2"; shift 2 ;;
    --symbols) symbols_csv="$2"; shift 2 ;;
    --windows) windows_per_market="$2"; shift 2 ;;
    --out-root) out_root="$2"; shift 2 ;;
    --tag) tag="$2"; shift 2 ;;
    --start-mode) start_mode="$2"; shift 2 ;;
    --parallelism) parallelism="$2"; shift 2 ;;
    --max-window-attempts) max_window_attempts="$2"; shift 2 ;;
    --run-secs) run_secs="$2"; shift 2 ;;
    --global-total-budget) global_total_budget="$2"; shift 2 ;;
    --per-market-budget-cap) per_market_budget_cap="$2"; shift 2 ;;
    --budget-strategy) budget_strategy="$2"; shift 2 ;;
    --max-rounds) max_rounds="$2"; shift 2 ;;
    --round-budget) round_budget="$2"; shift 2 ;;
    --round-leg1-fraction) round_leg1_fraction="$2"; shift 2 ;;
    --tail-freeze-secs) tail_freeze_secs="$2"; shift 2 ;;
    --tail-close-secs) tail_close_secs="$2"; shift 2 ;;
    --decision-every-ms) decision_every_ms="$2"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *)
      echo "unknown arg: $1"
      usage
      exit 1
      ;;
  esac
done

matrix_dir="${out_root}/${tag}"
mkdir -p "$matrix_dir"
matrix_csv="${matrix_dir}/matrix_results.csv"

cat > "$matrix_csv" <<'CSV'
profile,entry_pair_buffer_bps,runs,total_pnl_usdc,worst_window_pnl_usdc,win_ratio,status,batch_root,summary_csv
CSV

capture_mtimes() {
  local out_file="$1"
  python3 - <<'PY' "$out_file"
import os
import pathlib
import sys

out = pathlib.Path(sys.argv[1])
rows = []
root = pathlib.Path("logs/multi")
if root.exists():
    for p in root.rglob("*_paper.jsonl"):
        try:
            rows.append((str(p), os.path.getmtime(p)))
        except OSError:
            continue
rows.sort(key=lambda x: x[0])
with out.open("w", encoding="utf-8") as f:
    for path, mt in rows:
        f.write(f"{path}\t{mt}\n")
PY
}

diff_changed_jsonl() {
  local before_file="$1"
  local after_file="$2"
  local out_file="$3"
  python3 - <<'PY' "$before_file" "$after_file" "$out_file"
import pathlib
import sys

before_path = pathlib.Path(sys.argv[1])
after_path = pathlib.Path(sys.argv[2])
out_path = pathlib.Path(sys.argv[3])

def load(path):
    d = {}
    if not path.exists():
        return d
    for raw in path.read_text(encoding="utf-8").splitlines():
        if not raw.strip():
            continue
        try:
            p, m = raw.split("\t", 1)
            d[p] = float(m)
        except Exception:
            continue
    return d

before = load(before_path)
after = load(after_path)
changed = []
for p, mt in after.items():
    prev = before.get(p)
    if prev is None or mt > prev + 1e-9:
        changed.append(p)
changed.sort()
out_path.write_text("\n".join(changed) + ("\n" if changed else ""), encoding="utf-8")
PY
}

parse_profile_metrics() {
  local summary_csv="$1"
  python3 - <<'PY' "$summary_csv"
import csv
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
rows = list(csv.DictReader(path.open(newline="", encoding="utf-8")))
pnls = []
for r in rows:
    try:
        pnls.append(float(r.get("window_pnl_usdc") or 0.0))
    except Exception:
        pnls.append(0.0)
runs = len(pnls)
total = sum(pnls) if pnls else 0.0
worst = min(pnls) if pnls else 0.0
wins = sum(1 for v in pnls if v > 0.0)
win_ratio = (wins / runs) if runs else 0.0
print(f"{runs},{total:.6f},{worst:.6f},{win_ratio:.6f}")
PY
}

IFS=',' read -r -a buffers <<< "$buffers_csv"

echo "matrix_dir=$matrix_dir"
echo "buffers=${buffers[*]}"
echo "symbols=$symbols_csv windows_per_market=$windows_per_market run_secs=$run_secs"

for b in "${buffers[@]}"; do
  buffer_bps="$(echo "$b" | xargs)"
  if [[ -z "$buffer_bps" ]]; then
    continue
  fi

  profile="b${buffer_bps}"
  profile_root="${matrix_dir}/${profile}"
  mkdir -p "$profile_root"

  before_file="$(mktemp)"
  after_file="$(mktemp)"
  changed_file="$(mktemp)"
  capture_mtimes "$before_file"

  echo "=== profile=${profile} ENTRY_PAIR_BUFFER_BPS=${buffer_bps} ==="
  ENTRY_PAIR_BUFFER_BPS="$buffer_bps" \
  python3 scripts/run_multi_paper_safe.py \
    --symbols "$symbols_csv" \
    --start-mode "$start_mode" \
    --wait-until-window-start \
    --windows-per-market "$windows_per_market" \
    --max-window-attempts "$max_window_attempts" \
    --parallelism "$parallelism" \
    --global-total-budget "$global_total_budget" \
    --per-market-budget-cap "$per_market_budget_cap" \
    --budget-strategy "$budget_strategy" \
    --max-rounds "$max_rounds" \
    --round-budget "$round_budget" \
    --round-leg1-fraction "$round_leg1_fraction" \
    --tail-freeze-secs "$tail_freeze_secs" \
    --tail-close-secs "$tail_close_secs" \
    --decision-every-ms "$decision_every_ms" \
    --run-secs "$run_secs"

  capture_mtimes "$after_file"
  diff_changed_jsonl "$before_file" "$after_file" "$changed_file"

  changed_count="$(grep -c . "$changed_file" || true)"
  if [[ "${changed_count}" -eq 0 ]]; then
    echo "WARN: no changed jsonl found for profile=${profile}"
  fi

  while IFS= read -r jsonl; do
    [[ -z "$jsonl" ]] && continue
    run_dir="$(dirname "$jsonl")"
    symbol_dir="$(dirname "$run_dir")"
    symbol="$(basename "$symbol_dir")"
    window_ts="$(basename "$run_dir")"
    target_symbol_dir="${profile_root}/${symbol}"
    target_window_dir="${target_symbol_dir}/${window_ts}"
    mkdir -p "$target_symbol_dir"
    rm -rf "$target_window_dir"
    ln -s "$(cd "$run_dir" && pwd)" "$target_window_dir"
  done < "$changed_file"

  summary_prefix="${profile_root}/summary"
  python3 scripts/summarize_multi.py --root "$profile_root" --out-prefix "$summary_prefix"
  summary_csv="${summary_prefix}.csv"

  status="PASS"
  if ! TOTAL_PNL_MIN="${TOTAL_PNL_MIN:-0.0}" \
       WINDOW_PNL_MIN="${WINDOW_PNL_MIN:--2.0}" \
       WIN_RATIO_MIN="${WIN_RATIO_MIN:-0.50}" \
       bash scripts/check_multi_pnl.sh "$summary_csv"; then
    status="FAIL"
  fi

  metrics="$(parse_profile_metrics "$summary_csv")"
  IFS=',' read -r runs total_pnl worst_pnl win_ratio <<< "$metrics"
  echo "${profile},${buffer_bps},${runs},${total_pnl},${worst_pnl},${win_ratio},${status},${profile_root},${summary_csv}" >> "$matrix_csv"

  rm -f "$before_file" "$after_file" "$changed_file"
done

echo
echo "=== Matrix Results ==="
cat "$matrix_csv"
echo
python3 - <<'PY' "$matrix_csv"
import csv
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
rows = list(csv.DictReader(path.open(newline="", encoding="utf-8")))
if not rows:
    print("no rows")
    sys.exit(0)
best = max(rows, key=lambda r: float(r.get("total_pnl_usdc") or 0.0))
print(
    "best_profile={p} entry_pair_buffer_bps={b} total_pnl_usdc={t} "
    "worst_window_pnl_usdc={w} win_ratio={wr} status={s}".format(
        p=best.get("profile"),
        b=best.get("entry_pair_buffer_bps"),
        t=best.get("total_pnl_usdc"),
        w=best.get("worst_window_pnl_usdc"),
        wr=best.get("win_ratio"),
        s=best.get("status"),
    )
)
PY

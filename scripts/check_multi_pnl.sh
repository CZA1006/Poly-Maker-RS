#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 <summary_csv>"
  echo "Env:"
  echo "  TOTAL_PNL_MIN   (default: 0.0)"
  echo "  WINDOW_PNL_MIN  (default: -2.0)"
  echo "  WIN_RATIO_MIN   (default: 0.50)"
  echo "  MAX_CONSEC_NOOP (default: 1; fail if >=2 consecutive no-op windows)"
}

if [[ $# -ne 1 ]]; then
  usage
  exit 1
fi

INPUT="$1"
if [[ ! -f "$INPUT" ]]; then
  echo "File not found: $INPUT"
  exit 1
fi

python3 - <<'PY' "$INPUT"
import csv
import os
import sys

path = sys.argv[1]
total_pnl_min = float(os.environ.get("TOTAL_PNL_MIN", "0.0"))
window_pnl_min = float(os.environ.get("WINDOW_PNL_MIN", "-2.0"))
win_ratio_min = float(os.environ.get("WIN_RATIO_MIN", "0.50"))
max_consec_noop = int(os.environ.get("MAX_CONSEC_NOOP", "1"))

rows = []
with open(path, newline="") as f:
    reader = csv.DictReader(f)
    rows = list(reader)

def out(ok: bool, msg: str) -> int:
    print(("PASS: " if ok else "FAIL: ") + msg)
    return 0 if ok else 1

fails = 0
fails += out(len(rows) > 0, f"summary rows > 0 (actual={len(rows)})")
if not rows:
    print("FAILED: 1 checks failed")
    sys.exit(1)

window_pnls = []
def parse_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}

for r in rows:
    try:
        window_pnls.append(float(r.get("window_pnl_usdc") or 0.0))
    except Exception:
        window_pnls.append(0.0)

total_pnl = sum(window_pnls)
worst_window = min(window_pnls) if window_pnls else 0.0
wins = sum(1 for v in window_pnls if v > 0.0)
win_ratio = wins / len(window_pnls) if window_pnls else 0.0

fails += out(total_pnl >= total_pnl_min, f"total_pnl >= {total_pnl_min:.6f} (actual={total_pnl:.6f})")
fails += out(
    worst_window >= window_pnl_min,
    f"worst_window_pnl >= {window_pnl_min:.6f} (actual={worst_window:.6f})",
)
fails += out(
    win_ratio >= win_ratio_min,
    f"win_ratio >= {win_ratio_min:.2%} (actual={win_ratio:.2%}, wins={wins}/{len(window_pnls)})",
)

if rows and "no_opportunity_window" in rows[0]:
    best_streak = 0
    current_streak = 0
    best_start = None
    best_end = None
    current_start = None
    for idx, row in enumerate(rows):
        is_noop = parse_bool(row.get("no_opportunity_window", ""))
        window = row.get("window_ts") or str(idx + 1)
        if is_noop:
            if current_streak == 0:
                current_start = window
            current_streak += 1
            if current_streak > best_streak:
                best_streak = current_streak
                best_start = current_start
                best_end = window
        else:
            current_streak = 0
            current_start = None
    range_str = f"{best_start}..{best_end}" if best_streak > 0 else "n/a"
    fails += out(
        best_streak <= max_consec_noop,
        f"consecutive no_opportunity_window <= {max_consec_noop} (actual={best_streak}, range={range_str})",
    )
else:
    fails += out(True, "consecutive no_opportunity_window check skipped (column missing)")

print(
    f"INFO: totals runs={len(window_pnls)} total_pnl={total_pnl:.6f} "
    f"avg_pnl={(total_pnl/len(window_pnls)):.6f} worst_window={worst_window:.6f} "
    f"win_ratio={win_ratio:.2%}"
)

if fails:
    print(f"FAILED: {fails} checks failed")
    sys.exit(1)
print("PASS: multi-window pnl checks passed")
PY

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
  if [[ "$apply_count" -gt 0 ]]; then
    pass "paper has dryrun_apply (count=$apply_count)"
  elif [[ "$best_action_nonnull_count" -gt 0 ]]; then
    pass "paper has best_action (apply_count=$apply_count best_action_nonnull=$best_action_nonnull_count)"
  else
    fail "paper must have dryrun_apply OR best_action (apply_count=$apply_count best_action_nonnull=$best_action_nonnull_count) deny_reason_top=$deny_reason_top"
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

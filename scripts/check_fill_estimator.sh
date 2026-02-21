#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ORIG_PWD="$(pwd)"
cd "$REPO_ROOT"

usage() {
  cat <<'EOF'
Usage:
  scripts/check_fill_estimator.sh <enabled_jsonl> [--baseline <disabled_jsonl>]

Examples:
  scripts/check_fill_estimator.sh logs/multi/btc/1770.../btc_..._paper.jsonl
  scripts/check_fill_estimator.sh logs/.../enabled.jsonl --baseline logs/.../disabled.jsonl
EOF
}

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required. Install jq and retry."
  exit 1
fi

if [[ $# -lt 1 || $# -gt 3 ]]; then
  usage
  exit 1
fi

resolve_path() {
  local input="$1"
  if [[ "$input" = /* ]]; then
    printf '%s\n' "$input"
  else
    printf '%s\n' "${ORIG_PWD}/${input}"
  fi
}

ENABLED_FILE="$(resolve_path "$1")"
BASELINE_FILE=""

if [[ $# -eq 3 ]]; then
  if [[ "$2" != "--baseline" ]]; then
    usage
    exit 1
  fi
  BASELINE_FILE="$(resolve_path "$3")"
fi

if [[ ! -f "$ENABLED_FILE" ]]; then
  echo "File not found: $ENABLED_FILE"
  exit 1
fi
if [[ -n "$BASELINE_FILE" && ! -f "$BASELINE_FILE" ]]; then
  echo "File not found: $BASELINE_FILE"
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

jq_count() {
  local query="$1"
  local file="$2"
  jq -rs "$query" "$file"
}

summary_metrics() {
  local file="$1"
  jq -rs '
    {
      snapshots: ([.[] | select(.kind=="dryrun_snapshot")] | length),
      candidate_events: ([.[] | select(.kind=="dryrun_candidates")] | length),
      candidate_rows: ([.[] | select(.kind=="dryrun_candidates") | .data.candidates[]?] | length),
      applied_actions: ([.[] | select(.kind=="dryrun_candidates") | select(.data.applied_action != null)] | length),
      low_fill_denies: ([.[] | select(.kind=="dryrun_candidates") | .data.candidates[]? | select(.deny_reason=="low_maker_fill_prob")] | length),
      maker_fill_prob_nonnull: ([.[] | select(.kind=="dryrun_candidates") | .data.candidates[]? | .maker_fill_prob | select(. != null)] | length),
      latency_nonnull: ([.[] | select(.kind=="dryrun_snapshot") | .data | (.latency_ms_up, .latency_ms_down) | select(. != null)] | length),
      latency_negative: ([.[] | select(.kind=="dryrun_snapshot") | .data | (.latency_ms_up, .latency_ms_down) | select(. != null and . < 0)] | length),
      estimator_true: ([.[] | select(.kind=="dryrun_snapshot") | .data.maker_fill_estimator_enabled | select(. == true)] | length),
      estimator_false: ([.[] | select(.kind=="dryrun_snapshot") | .data.maker_fill_estimator_enabled | select(. == false)] | length)
    }
  ' "$file"
}

echo "=== Fill Estimator Check ==="
info "enabled_file=$ENABLED_FILE"
if [[ -n "$BASELINE_FILE" ]]; then
  info "baseline_file=$BASELINE_FILE"
fi

# -------------------------
# Check 1: JSON parse
# -------------------------
if jq -e . "$ENABLED_FILE" >/dev/null 2>&1; then
  pass "enabled json parse"
else
  fail "enabled json parse"
fi

if [[ -n "$BASELINE_FILE" ]]; then
  if jq -e . "$BASELINE_FILE" >/dev/null 2>&1; then
    pass "baseline json parse"
  else
    fail "baseline json parse"
  fi
fi

# -------------------------
# Check 2: snapshot/candidate core fields
# -------------------------
snapshots_enabled="$(jq_count '[ .[] | select(.kind=="dryrun_snapshot") ] | length' "$ENABLED_FILE")"
candidates_enabled="$(jq_count '[ .[] | select(.kind=="dryrun_candidates") | .data.candidates[]? ] | length' "$ENABLED_FILE")"

if [[ "$snapshots_enabled" -gt 0 ]]; then
  pass "enabled has dryrun_snapshot (count=$snapshots_enabled)"
else
  fail "enabled has dryrun_snapshot"
fi

if [[ "$candidates_enabled" -gt 0 ]]; then
  pass "enabled has candidate rows (count=$candidates_enabled)"
else
  fail "enabled has candidate rows"
fi

missing_snapshot_keys="$(jq_count '
  [ .[]
    | select(.kind=="dryrun_snapshot")
    | .data
    | select(
        (has("exchange_ts_ms_up") | not) or
        (has("recv_ts_ms_up") | not) or
        (has("latency_ms_up") | not) or
        (has("exchange_ts_ms_down") | not) or
        (has("recv_ts_ms_down") | not) or
        (has("latency_ms_down") | not)
      )
  ] | length
' "$ENABLED_FILE")"
if [[ "$missing_snapshot_keys" -eq 0 ]]; then
  pass "snapshot timestamp keys present"
else
  fail "snapshot timestamp keys present (missing_rows=$missing_snapshot_keys)"
fi

missing_candidate_keys="$(jq_count '
  [ .[]
    | select(.kind=="dryrun_candidates")
    | .data.candidates[]?
    | select(
        (has("maker_fill_prob") | not) or
        (has("maker_queue_ahead") | not) or
        (has("maker_expected_consumed") | not) or
        (has("maker_consumption_rate") | not) or
        (has("maker_horizon_secs") | not)
      )
  ] | length
' "$ENABLED_FILE")"
if [[ "$missing_candidate_keys" -eq 0 ]]; then
  pass "candidate estimator keys present"
else
  fail "candidate estimator keys present (missing_rows=$missing_candidate_keys)"
fi

# -------------------------
# Check 3: latency sanity
# -------------------------
latency_nonnull_enabled="$(jq_count '
  [ .[]
    | select(.kind=="dryrun_snapshot")
    | .data
    | (.latency_ms_up, .latency_ms_down)
    | select(. != null)
  ] | length
' "$ENABLED_FILE")"

latency_negative_enabled="$(jq_count '
  [ .[]
    | select(.kind=="dryrun_snapshot")
    | .data
    | (.latency_ms_up, .latency_ms_down)
    | select(. != null and . < 0)
  ] | length
' "$ENABLED_FILE")"

if [[ "$latency_nonnull_enabled" -gt 0 ]]; then
  pass "latency observed (nonnull_count=$latency_nonnull_enabled)"
else
  fail "latency observed (nonnull_count=0)"
fi

if [[ "$latency_negative_enabled" -eq 0 ]]; then
  pass "latency non-negative"
else
  fail "latency non-negative (negative_count=$latency_negative_enabled)"
fi

# -------------------------
# Check 4: estimator behavior in enabled run
# -------------------------
estimator_true_enabled="$(jq_count '
  [ .[]
    | select(.kind=="dryrun_snapshot")
    | .data.maker_fill_estimator_enabled
    | select(. == true)
  ] | length
' "$ENABLED_FILE")"
if [[ "$estimator_true_enabled" -gt 0 ]]; then
  pass "estimator enabled snapshots observed (count=$estimator_true_enabled)"
else
  fail "estimator enabled snapshots observed"
fi

maker_fill_prob_nonnull_enabled="$(jq_count '
  [ .[]
    | select(.kind=="dryrun_candidates")
    | .data.candidates[]?
    | .maker_fill_prob
    | select(. != null)
  ] | length
' "$ENABLED_FILE")"
if [[ "$candidates_enabled" -gt 0 && "$maker_fill_prob_nonnull_enabled" -gt 0 ]]; then
  pass "maker_fill_prob populated (nonnull_count=$maker_fill_prob_nonnull_enabled)"
else
  fail "maker_fill_prob populated (candidates=$candidates_enabled nonnull=$maker_fill_prob_nonnull_enabled)"
fi

# -------------------------
# Check 5: optional A/B comparison
# -------------------------
if [[ -n "$BASELINE_FILE" ]]; then
  estimator_false_baseline="$(jq_count '
    [ .[]
      | select(.kind=="dryrun_snapshot")
      | .data.maker_fill_estimator_enabled
      | select(. == false)
    ] | length
  ' "$BASELINE_FILE")"
  if [[ "$estimator_false_baseline" -gt 0 ]]; then
    pass "baseline snapshots show estimator disabled (count=$estimator_false_baseline)"
  else
    fail "baseline snapshots show estimator disabled"
  fi

  maker_fill_prob_nonnull_baseline="$(jq_count '
    [ .[]
      | select(.kind=="dryrun_candidates")
      | .data.candidates[]?
      | .maker_fill_prob
      | select(. != null)
    ] | length
  ' "$BASELINE_FILE")"
  if [[ "$maker_fill_prob_nonnull_baseline" -eq 0 ]]; then
    pass "baseline maker_fill_prob stays null (count=0)"
  else
    fail "baseline maker_fill_prob stays null (nonnull_count=$maker_fill_prob_nonnull_baseline)"
  fi

  enabled_metrics="$(summary_metrics "$ENABLED_FILE")"
  baseline_metrics="$(summary_metrics "$BASELINE_FILE")"
  info "enabled_metrics=$enabled_metrics"
  info "baseline_metrics=$baseline_metrics"

  enabled_applied="$(echo "$enabled_metrics" | jq -r '.applied_actions')"
  baseline_applied="$(echo "$baseline_metrics" | jq -r '.applied_actions')"
  enabled_low_fill="$(echo "$enabled_metrics" | jq -r '.low_fill_denies')"
  baseline_low_fill="$(echo "$baseline_metrics" | jq -r '.low_fill_denies')"
  enabled_prob_nonnull="$(echo "$enabled_metrics" | jq -r '.maker_fill_prob_nonnull')"
  baseline_prob_nonnull="$(echo "$baseline_metrics" | jq -r '.maker_fill_prob_nonnull')"

  info "ab_delta applied_actions=$((enabled_applied - baseline_applied)) (enabled=$enabled_applied baseline=$baseline_applied)"
  info "ab_delta low_fill_denies=$((enabled_low_fill - baseline_low_fill)) (enabled=$enabled_low_fill baseline=$baseline_low_fill)"
  info "ab_delta maker_fill_prob_nonnull=$((enabled_prob_nonnull - baseline_prob_nonnull)) (enabled=$enabled_prob_nonnull baseline=$baseline_prob_nonnull)"
  pass "a/b metrics emitted"
fi

# Final status
if [[ "$fail_count" -gt 0 ]]; then
  echo "FAILED: ${fail_count} checks failed"
  exit 1
fi

echo "PASS: fill estimator checks passed"
exit 0

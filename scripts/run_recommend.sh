#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

load_env_defaults() {
  local dotenv="${REPO_ROOT}/.env"
  if [[ ! -f "$dotenv" ]]; then
    echo "INFO: .env not found, continuing without it"
    return 0
  fi
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "$line" ]] && continue
    [[ "$line" =~ ^[[:space:]]*# ]] && continue
    if [[ "$line" =~ ^[[:space:]]*([A-Za-z_][A-Za-z0-9_]*)= ]]; then
      local key="${BASH_REMATCH[1]}"
      local value="${line#*=}"
      key="${key#"${key%%[![:space:]]*}"}"
      key="${key%"${key##*[![:space:]]}"}"
      if [[ -z "${!key:-}" ]]; then
        export "${key}=${value}"
      fi
    fi
  done < "$dotenv"
}

warn_on_legacy_series_slug() {
  if [[ -n "${SERIES_SLUG:-}" && "${SERIES_SLUG}" == *"-up-or-down-"* ]]; then
    local suggested="${SERIES_SLUG/-up-or-down-/-updown-}"
    echo "WARNING: SERIES_SLUG looks like legacy format: '${SERIES_SLUG}'. Consider '${suggested}' (warn-only)."
  fi
}

load_env_defaults

: "${CLOB_HOST:?missing env var CLOB_HOST (e.g. https://clob.polymarket.com)}"
: "${WS_HOST:?missing env var WS_HOST (e.g. wss://ws-subscriptions-clob.polymarket.com)}"
: "${WS_PATH:?missing env var WS_PATH (e.g. /ws/market)}"
: "${GAMMA_HOST:?missing env var GAMMA_HOST (e.g. https://gamma-api.polymarket.com)}"
if [[ -z "${MARKET_SLUG:-}" ]]; then
  : "${SERIES_SLUG:?missing env var SERIES_SLUG (e.g. xrp-updown-15m)}"
fi

warn_on_legacy_series_slug

mkdir -p logs
RUN_ID="$(date +%Y%m%d_%H%M%S)"
MODE="recommend"

LOG_DIR="${REPO_ROOT}/logs"
FULL_LOG="${LOG_DIR}/${RUN_ID}_${MODE}_full.log"
JSONL_LOG="${LOG_DIR}/${RUN_ID}_${MODE}.jsonl"
EXTRACTOR="${LOG_DIR}/extract_jsonl.py"

echo "repo_root=${REPO_ROOT}"
echo "run_id=${RUN_ID} mode=${MODE}"
echo "full_log=${FULL_LOG}"
echo "jsonl_log=${JSONL_LOG}"
if [[ -n "${MARKET_SLUG:-}" ]]; then
  echo "market_select_mode=fixed_market_slug market_slug=${MARKET_SLUG}"
elif [[ -n "${SERIES_SLUG:-}" ]]; then
  echo "market_select_mode=series_latest series_slug=${SERIES_SLUG}"
else
  echo "market_select_mode=env_asset_ids"
fi

cat > "${EXTRACTOR}" <<'PY'
import sys, re, json
for line in sys.stdin:
    m = re.search(r'(\{.*\})', line)
    if not m:
        continue
    s = m.group(1)
    try:
        json.loads(s)
    except Exception:
        continue
    print(s)
PY

set -m

PIPE_PID=""
RUN_PGID=""
STOPPED=0
IN_STAGE2=0

start_stage1_bg() {
  ( cargo run --manifest-path poly_maker/Cargo.toml 2>&1 | tee "${FULL_LOG}" ) &
  PIPE_PID=$!
  RUN_PGID="$(ps -o pgid= -p "${PIPE_PID}" | tr -d ' ')"
  echo "INFO: stage1 started PIPE_PID=${PIPE_PID} PGID=${RUN_PGID}" >&2

  for _ in $(seq 1 10); do
    if [[ -f "${FULL_LOG}" ]]; then
      return 0
    fi
    sleep 0.1
  done

  echo "ERROR: stage1 did not create full_log: ${FULL_LOG}" >&2
  return 1
}

stop_group() {
  if [[ "${STOPPED}" == "1" ]]; then
    return 0
  fi
  STOPPED=1

  echo "INFO: stopping (PIPE_PID=${PIPE_PID:-} PGID=${RUN_PGID:-}) ..." >&2

  if [[ -n "${RUN_PGID:-}" ]]; then
    kill -TERM -"${RUN_PGID}" >/dev/null 2>&1 || true
    sleep 0.8
    kill -KILL -"${RUN_PGID}" >/dev/null 2>&1 || true
  fi

  pkill -TERM -f "poly_maker/target/debug/poly_maker" >/dev/null 2>&1 || true
  pkill -TERM -f "tee .*_recommend_full\.log" >/dev/null 2>&1 || true
  sleep 0.2
  pkill -KILL -f "poly_maker/target/debug/poly_maker" >/dev/null 2>&1 || true
}

cleanup() {
  if [[ "${IN_STAGE2}" == "1" ]]; then
    return 0
  fi
  stop_group
}

trap 'stop_group' INT TERM
trap 'cleanup' EXIT

echo "INFO: running. Ctrl+C to stop. If RUN_SECS is set, script will stop automatically and then extract JSONL." >&2

set +e
start_stage1_bg || exit 1

if [[ -n "${RUN_SECS:-}" ]]; then
  sleep "${RUN_SECS}"
  echo "INFO: RUN_SECS reached (${RUN_SECS}s), stopping..." >&2
  stop_group
fi

wait "${PIPE_PID}" >/dev/null 2>&1 || true
set -e

trap - INT TERM
IN_STAGE2=1

echo "INFO: stage1 ended (STOPPED=${STOPPED}), entering stage2..." >&2

if [[ ! -f "${FULL_LOG}" ]]; then
  echo "ERROR: full_log missing, cannot extract jsonl: ${FULL_LOG}" >&2
  exit 1
fi

echo "INFO: stage2 extracting jsonl -> ${JSONL_LOG}" >&2
python3 "${EXTRACTOR}" < "${FULL_LOG}" > "${JSONL_LOG}"
echo "INFO: stage2 done, jsonl_bytes=$(wc -c < "${JSONL_LOG}")" >&2

if [[ ! -s "${JSONL_LOG}" ]]; then
  echo "ERROR: jsonl_log is empty: ${JSONL_LOG}" >&2
  exit 1
fi

./scripts/check_run.sh "${JSONL_LOG}"

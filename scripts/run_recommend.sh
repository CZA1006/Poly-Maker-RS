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

RUN_ID="${RUN_ID:-$(date +%Y%m%d_%H%M%S)}"
MODE="recommend"

LOG_DIR="${LOG_DIR:-${REPO_ROOT}/logs}"
mkdir -p "${LOG_DIR}"
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

PIPE_PID=""
RUN_PGID=""
STOPPED=0
IN_STAGE2=0

children_of_pid() {
  ps -ax -o pid=,ppid= | awk -v p="$1" '$2==p {print $1}'
}

descendants_of_pid() {
  local root="$1"
  local queue="$root"
  local seen=" $root "
  local out=""

  while [[ -n "$queue" ]]; do
    local pid="${queue%% *}"
    if [[ "$queue" == *" "* ]]; then
      queue="${queue#* }"
    else
      queue=""
    fi
    for child in $(children_of_pid "$pid"); do
      if [[ "$seen" != *" $child "* ]]; then
        seen+=" $child "
        out+="$child "
        if [[ -n "$queue" ]]; then
          queue+=" $child"
        else
          queue="$child"
        fi
      fi
    done
  done

  echo "$out"
}

find_poly_maker_pid_from_wrapper() {
  local root="$1"
  for pid in $(descendants_of_pid "$root"); do
    local cmd
    cmd="$(ps -p "$pid" -o command= 2>/dev/null || true)"
    if [[ "$cmd" == *"poly_maker/target/debug/poly_maker"* ]]; then
      echo "$pid"
      return 0
    fi
  done
  return 1
}

refresh_run_pgid() {
  if [[ -z "${PIPE_PID:-}" ]]; then
    return 1
  fi
  local poly_pid
  poly_pid="$(find_poly_maker_pid_from_wrapper "$PIPE_PID")"
  if [[ -n "$poly_pid" ]]; then
    RUN_PGID="$(ps -o pgid= -p "$poly_pid" | tr -d ' ')"
    echo "INFO: resolved poly_pid=${poly_pid} run_pgid=${RUN_PGID}" >&2
    return 0
  fi
  return 1
}

start_stage1_bg() {
  local cmd
  cmd="cargo run --manifest-path poly_maker/Cargo.toml 2>&1 | tee \"${FULL_LOG}\""

  if command -v setsid >/dev/null 2>&1; then
    setsid bash -c "${cmd}" &
  else
    python3 - "${cmd}" <<'PY' &
import os, sys
os.setsid()
os.execvp("bash", ["bash", "-c", sys.argv[1]])
PY
  fi

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

  refresh_run_pgid || true
  echo "INFO: stopping (PIPE_PID=${PIPE_PID:-} PGID=${RUN_PGID:-}) ..." >&2

  if [[ -n "${RUN_PGID:-}" ]]; then
    kill -TERM -"${RUN_PGID}" >/dev/null 2>&1 || true
    for _ in $(seq 1 20); do
      if ! kill -0 -"${RUN_PGID}" >/dev/null 2>&1; then
        return 0
      fi
      sleep 0.1
    done
    kill -KILL -"${RUN_PGID}" >/dev/null 2>&1 || true
  fi

  echo "WARNING: fallback pkill may kill other runs" >&2
  pkill -TERM -f "poly_maker/target/debug/poly_maker" >/dev/null 2>&1 || true
  pkill -KILL -f "poly_maker/target/debug/poly_maker" >/dev/null 2>&1 || true
}

cleanup() {
  if [[ "${IN_STAGE2}" == "1" ]]; then
    return 0
  fi
  stop_group
}

trap 'stop_group; exit 130' INT
trap 'stop_group; exit 143' TERM
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

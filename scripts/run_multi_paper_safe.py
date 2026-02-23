#!/usr/bin/env python3
import argparse
import asyncio
import csv
import json
import math
import os
import signal
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

DEFAULT_CLOB_HOST = "https://clob.polymarket.com"
DEFAULT_WS_HOST = "wss://ws-subscriptions-clob.polymarket.com"
DEFAULT_WS_PATH = "/ws/market"
DEFAULT_GAMMA_HOST = "https://gamma-api.polymarket.com"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run multiple Polymarket 15m markets safely in parallel (paper mode)."
    )
    parser.add_argument(
        "--symbols",
        default="btc,eth,sol,xrp",
        help="Comma-separated symbols (default: btc,eth,sol,xrp)",
    )
    parser.add_argument(
        "--windows-per-market",
        type=int,
        default=0,
        help="Number of valid windows to run per market (0 = infinite)",
    )
    parser.add_argument(
        "--start-mode",
        choices=["now", "prev", "next"],
        default="prev",
        help="Start from previous, current, or next 15m window (default: prev)",
    )
    parser.add_argument(
        "--sleep-on-invalid-secs",
        type=int,
        default=30,
        help="Sleep seconds after invalid market (default: 30)",
    )
    parser.add_argument(
        "--max-window-attempts",
        type=int,
        default=0,
        help="Max attempts per market (0 = infinite)",
    )
    parser.add_argument(
        "--wait-until-window-start",
        dest="wait_until_window_start",
        action="store_true",
        default=True,
        help="Wait until selected 15m window start timestamp before running (default: true)",
    )
    parser.add_argument(
        "--no-wait-until-window-start",
        dest="wait_until_window_start",
        action="store_false",
        help="Start immediately after slug validation without waiting for window start",
    )
    parser.add_argument(
        "--parallelism",
        type=int,
        default=0,
        help="Parallelism limit (default: number of symbols)",
    )
    parser.add_argument(
        "--global-total-budget",
        type=float,
        default=10.0,
        help="Global total budget across all symbols (default: 10.0)",
    )
    parser.add_argument(
        "--per-market-budget-cap",
        type=float,
        default=0.0,
        help="Optional hard cap per market budget (0 = disabled)",
    )
    parser.add_argument(
        "--budget-strategy",
        choices=["equal", "weighted"],
        default="equal",
        help="Budget allocation strategy across symbols (default: equal)",
    )
    parser.add_argument(
        "--symbol-weights",
        default="",
        help="Comma-separated weights, e.g. btc=3,eth=2,sol=1,xrp=1 (used when budget-strategy=weighted)",
    )
    parser.add_argument(
        "--total-budget",
        type=float,
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--budget-roll-mode",
        choices=["fixed", "pnl_compound"],
        default="fixed",
        help="Per-window budget mode: fixed or compound using previous window pnl (default: fixed)",
    )
    parser.add_argument(
        "--rolling-budget-floor",
        type=float,
        default=1.0,
        help="Minimum per-window budget when --budget-roll-mode=pnl_compound (default: 1.0)",
    )
    parser.add_argument("--max-rounds", type=int, default=6)
    parser.add_argument("--round-budget", type=float, default=0.0)
    parser.add_argument("--round-leg1-fraction", type=float, default=0.45)
    parser.add_argument("--tail-freeze-secs", type=int, default=300)
    parser.add_argument("--tail-close-secs", type=int, default=180)
    parser.add_argument("--decision-every-ms", type=int, default=200)
    parser.add_argument(
        "--run-secs",
        type=int,
        default=900,
        help="Max runtime seconds per window (actual runtime is clamped to window end)",
    )
    parser.add_argument("--gamma-host", default=os.environ.get("GAMMA_HOST", ""))
    return parser.parse_args()


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def make_window_start(start_mode: str) -> int:
    now = int(time.time())
    t0 = (now // 900) * 900
    if start_mode == "prev":
        return t0 - 900
    if start_mode == "next":
        return t0 + 900
    return t0


def market_slug(symbol: str, t: int) -> str:
    return f"{symbol}-updown-15m-{t}"


def resolve_window_run_secs(configured_run_secs: int, window_start_ts: int) -> int:
    now_ts = int(time.time())
    remaining_to_window_end = (window_start_ts + 900) - now_ts
    remaining_to_window_end = max(1, remaining_to_window_end)
    if configured_run_secs <= 0:
        return remaining_to_window_end
    return max(1, min(configured_run_secs, remaining_to_window_end))


async def run_validate(slug: str, gamma_host: str) -> Tuple[bool, str]:
    cmd = [sys.executable, "scripts/validate_market_slug.py", slug]
    if gamma_host:
        cmd.extend(["--gamma-host", gamma_host])
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        cwd=repo_root(),
    )
    out, _ = await proc.communicate()
    text = (out or b"").decode("utf-8", errors="ignore").strip()
    ok = proc.returncode == 0
    return ok, text


def parse_symbol_weights(raw: str, symbols: List[str]) -> Dict[str, float]:
    weights = {sym: 1.0 for sym in symbols}
    if not raw.strip():
        return weights
    for part in raw.split(","):
        item = part.strip()
        if not item:
            continue
        if "=" not in item:
            raise SystemExit(f"invalid symbol weight item: {item}")
        key, value = item.split("=", 1)
        symbol = key.strip().lower()
        if symbol not in weights:
            raise SystemExit(f"unknown symbol in --symbol-weights: {symbol}")
        try:
            weight = float(value.strip())
        except Exception as exc:
            raise SystemExit(f"invalid weight for {symbol}: {value}") from exc
        if not (weight > 0 and weight < float("inf")):
            raise SystemExit(f"invalid non-positive weight for {symbol}: {value}")
        weights[symbol] = weight
    return weights


def resolve_global_total_budget(args: argparse.Namespace) -> float:
    budget = args.global_total_budget
    if args.total_budget is not None:
        if abs(args.global_total_budget - 10.0) < 1e-9:
            budget = args.total_budget
            print(
                "WARN: --total-budget is deprecated; use --global-total-budget",
                flush=True,
            )
        elif abs(args.total_budget - args.global_total_budget) > 1e-9:
            print(
                "WARN: both --total-budget and --global-total-budget set; using --global-total-budget",
                flush=True,
            )
    if not (budget > 0 and budget < float("inf")):
        raise SystemExit(f"invalid global budget: {budget}")
    return budget


def safe_float(value) -> Optional[float]:
    try:
        if value is None:
            return None
        out = float(value)
        if not math.isfinite(out):
            return None
        return out
    except Exception:
        return None


def iter_jsonl(path: Path) -> Iterable[dict]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def compute_window_pnl_from_snapshot(snapshot: dict) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    qty_up = safe_float(snapshot.get("qty_up"))
    qty_down = safe_float(snapshot.get("qty_down"))
    spent_total = safe_float(snapshot.get("spent_total_usdc"))
    pair_cost = safe_float(snapshot.get("pair_cost"))
    cost_up = safe_float(snapshot.get("cost_up"))
    cost_down = safe_float(snapshot.get("cost_down"))

    if qty_up is None or qty_down is None:
        return None, None, None

    hedgeable_shares = max(0.0, min(qty_up, qty_down))
    unmatched_loss_usdc = None

    # Keep the same strict accounting used by scripts/summarize_multi.py.
    if spent_total is not None:
        window_pnl = hedgeable_shares - spent_total
        if (
            cost_up is not None
            and cost_down is not None
            and qty_up > 0.0
            and qty_down > 0.0
        ):
            avg_up = cost_up / qty_up
            avg_down = cost_down / qty_down
            if qty_up > qty_down:
                unmatched_loss_usdc = max(0.0, cost_up - avg_up * hedgeable_shares)
            elif qty_down > qty_up:
                unmatched_loss_usdc = max(0.0, cost_down - avg_down * hedgeable_shares)
            else:
                unmatched_loss_usdc = 0.0
        return hedgeable_shares, window_pnl, unmatched_loss_usdc

    if pair_cost is None:
        return hedgeable_shares, None, unmatched_loss_usdc
    window_pnl = (1.0 - pair_cost) * hedgeable_shares
    return hedgeable_shares, window_pnl, unmatched_loss_usdc


def find_latest_paper_jsonl(log_dir: Path) -> Optional[Path]:
    files = sorted(log_dir.glob("*_paper.jsonl"))
    if not files:
        return None
    files.sort(key=lambda p: p.stat().st_mtime)
    return files[-1]


def extract_last_snapshot_from_jsonl(jsonl_path: Path) -> Optional[dict]:
    last_snapshot = None
    last_ts = None
    for obj in iter_jsonl(jsonl_path):
        if obj.get("kind") != "dryrun_snapshot":
            continue
        data = obj.get("data", {})
        ts = obj.get("ts_ms")
        if ts is None:
            ts = data.get("ts_ms")
        if last_ts is None or (ts is not None and ts >= last_ts):
            last_ts = ts
            last_snapshot = data
    return last_snapshot


def ensure_curve_csv(path: Path) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "symbol",
                "window_idx",
                "window_start_ts",
                "slug",
                "start_budget_usdc",
                "window_pnl_usdc",
                "end_budget_usdc",
                "spent_total_usdc",
                "final_pair_cost",
                "final_qty_up",
                "final_qty_down",
                "hedgeable_shares",
                "unmatched_loss_usdc",
                "jsonl_path",
                "parse_ok",
            ]
        )


def append_curve_row(path: Path, row: List[object]) -> None:
    ensure_curve_csv(path)
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(row)


def allocate_market_budgets(
    symbols: List[str], args: argparse.Namespace, global_total_budget: float
) -> Dict[str, float]:
    per_market_cap = args.per_market_budget_cap
    if per_market_cap < 0:
        raise SystemExit("--per-market-budget-cap must be >= 0")

    if args.budget_strategy == "equal":
        raw = {sym: global_total_budget / max(1, len(symbols)) for sym in symbols}
    else:
        weights = parse_symbol_weights(args.symbol_weights, symbols)
        total_weight = sum(weights.values())
        if total_weight <= 0:
            raise SystemExit("sum of symbol weights must be > 0")
        raw = {
            sym: global_total_budget * (weights[sym] / total_weight) for sym in symbols
        }

    if per_market_cap > 0:
        return {sym: min(value, per_market_cap) for sym, value in raw.items()}
    return raw


def build_env(
    args: argparse.Namespace,
    slug: str,
    symbol: str,
    t: int,
    log_dir: Path,
    market_budget: float,
    global_total_budget: float,
    run_secs: int,
) -> dict:
    env = os.environ.copy()
    # This runner is intentionally paper-only.
    env["EXECUTION_MODE"] = "paper"
    env["EXECUTION_ADAPTER"] = "paper"
    env["LIVE_MODE"] = "false"
    env["USER_WS_ENABLED"] = "false"
    env["SIM_FILL"] = "false"
    env.setdefault("CLOB_HOST", DEFAULT_CLOB_HOST)
    env.setdefault("WS_HOST", DEFAULT_WS_HOST)
    env.setdefault("WS_PATH", DEFAULT_WS_PATH)
    env.setdefault("GAMMA_HOST", args.gamma_host or DEFAULT_GAMMA_HOST)
    # Paper mode should not require real credentials.
    env.setdefault("PK", "paper_dummy_pk")
    env.setdefault("CLOB_API_KEY", "paper_dummy_key")
    env.setdefault("CLOB_API_SECRET", "paper_dummy_secret")
    env.setdefault("CLOB_API_PASSPHRASE", "paper_dummy_passphrase")
    env.setdefault("FUNDER", "paper_dummy_funder")
    env.setdefault("CHAIN_ID", "137")
    env["RUN_SECS"] = str(run_secs)
    # Multi-window orchestration should hand off quickly between adjacent windows.
    # Run validation checks after the batch instead of blocking the next window here.
    env["RUN_CHECK_ON_EXIT"] = "0"
    env["DRYRUN_MODE"] = "paper"
    env["ALLOW_TAKER"] = "false"
    env["TOTAL_BUDGET_USDC"] = str(market_budget)
    env["GLOBAL_TOTAL_BUDGET_USDC"] = str(global_total_budget)
    env["MARKET_BUDGET_USDC"] = str(market_budget)
    env["MULTI_BUDGET_STRATEGY"] = str(args.budget_strategy)
    env["MAX_ROUNDS"] = str(args.max_rounds)
    if args.round_budget > 0:
        env["ROUND_BUDGET_USDC"] = str(min(args.round_budget, market_budget))
    else:
        env["ROUND_BUDGET_USDC"] = str(market_budget / max(1, args.max_rounds))
    env["ROUND_LEG1_FRACTION"] = str(args.round_leg1_fraction)
    env["DRYRUN_TAIL_FREEZE_SECS"] = str(args.tail_freeze_secs)
    env["DRYRUN_TAIL_CLOSE_SECS"] = str(args.tail_close_secs)
    env["DRYRUN_DECISION_EVERY_MS"] = str(args.decision_every_ms)
    env["MARKET_SLUG"] = slug
    env["RUN_ID"] = f"{symbol}_{t}"
    env["LOG_DIR"] = str(log_dir)
    env["NO_RISK_PAIR_LIMIT_MODE"] = "hard_cap_only"
    env["ENTRY_PAIR_REGRESSION_MODE"] = "cap_edge"
    env["ENTRY_PAIR_REGRESSION_SOFT_BAND_BPS"] = "40"
    env["NO_RISK_ENTRY_HEDGE_PRICE_MODE"] = "ask"
    env["NO_RISK_HARD_PAIR_CAP"] = "0.995"
    env["ENTRY_DYNAMIC_CAP_HEADROOM_BPS"] = "5"
    env["ENTRY_TARGET_MARGIN_MIN_TICKS"] = "0.3"
    env["ENTRY_TARGET_MARGIN_MIN_BPS"] = "5"
    env["ENTRY_MAX_PASSIVE_TICKS_FOR_NET_INCREASE"] = "1.5"
    env["OPEN_MIN_FILL_PROB"] = "0.06"
    env["HEDGE_RECOVERABILITY_MARGIN_ENFORCE"] = "true"
    env["HEDGE_RECOVERABILITY_MARGIN_MIN_TICKS"] = "0.5"
    env["HEDGE_RECOVERABILITY_MARGIN_MIN_BPS"] = "8"
    env["HEDGE_RECOVERABILITY_MARGIN_APPLY_TO_NET_INCREASE_ONLY"] = "true"
    env["NO_RISK_HEDGE_RECOVERABILITY_ENFORCE"] = "true"
    env["NO_RISK_HEDGE_RECOVERABILITY_EPS_BPS"] = "2"
    env["OPEN_ORDER_UNRECOVERABLE_GRACE_MS"] = "2500"
    env["MAKER_FILL_PASSIVE_QUEUE_PENALTY_PER_TICK"] = "1.25"
    env["MAKER_FILL_PASSIVE_DECAY_K"] = "0.35"
    env["ENTRY_PASSIVE_GAP_SOFT_MAX_TICKS"] = "3.0"
    env["ROUND_DYNAMIC_SLICING_ENABLED"] = "true"
    env["ROUND_MIN_SLICES"] = "3"
    env["ROUND_MAX_SLICES"] = "10"
    env["ENTRY_MAX_TOP_BOOK_SHARE"] = "0.35"
    env["ENTRY_MAX_FLOW_UTILIZATION"] = "0.75"
    env["MAKER_FILL_HORIZON_SECS"] = "12"
    env["ENTRY_EDGE_MIN_BPS"] = "20"
    env["ENTRY_FILL_PROB_MIN"] = "0.05"
    env["MAKER_MIN_FILL_PROB"] = "0.05"
    # Avoid early strict lock right after the first completed pair so later rounds
    # can continue under hard no-risk constraints.
    env["LOCK_MIN_COMPLETED_ROUNDS"] = "5"
    env["LOCK_MIN_SPENT_RATIO"] = "0.55"
    env["LOCK_FORCE_TIME_LEFT_SECS"] = str(max(0, args.tail_close_secs))
    env["LOCK_ALLOW_REOPEN_BEFORE_FREEZE"] = "true"
    env["LOCK_REOPEN_MAX_ROUNDS"] = str(max(1, args.max_rounds - 1))
    env["NO_RISK_LATE_NEW_ROUND_BUFFER_SECS"] = "0"
    env["PAPER_MIN_REQUOTE_INTERVAL_MS"] = "5000"
    env["PAPER_REQUOTE_PRICE_DELTA_TICKS"] = "3"
    env["PAPER_REQUOTE_STALE_MS_HARD"] = "25000"
    env["REQUOTE_MIN_FILL_PROB_UPLIFT"] = "0.015"
    env["REQUOTE_QUEUE_STICKINESS_RATIO"] = "4.0"
    env["REQUOTE_STICKINESS_MIN_AGE_SECS"] = "20"
    env["OPEN_ORDER_MAX_AGE_SECS"] = "90"
    env["PAPER_ORDER_TIMEOUT_SECS"] = "18"
    env["PAPER_TIMEOUT_TARGET_FILL_PROB"] = "0.60"
    env["PAPER_TIMEOUT_PROGRESS_EXTEND_MIN"] = "0.50"
    env["PAPER_TIMEOUT_PROGRESS_EXTEND_SECS"] = "12"
    env["PAPER_TIMEOUT_MAX_EXTENDS"] = "1"
    env["OPEN_MARGIN_SURPLUS_MIN"] = "0.0005"
    return env


class RunState:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._pgids: Dict[int, int] = {}

    async def register(self, proc: asyncio.subprocess.Process) -> None:
        try:
            pgid = os.getpgid(proc.pid)
        except Exception:
            pgid = proc.pid
        async with self._lock:
            self._pgids[proc.pid] = pgid
        print(f"registered pid={proc.pid} pgid={pgid}", flush=True)

    async def unregister(self, proc: asyncio.subprocess.Process) -> None:
        async with self._lock:
            self._pgids.pop(proc.pid, None)

    async def kill_all(self, sig: signal.Signals) -> None:
        async with self._lock:
            pgids = list(self._pgids.values())
        for pgid in pgids:
            try:
                os.killpg(pgid, sig)
            except ProcessLookupError:
                continue
            except Exception:
                continue


async def run_paper(env: dict, run_state: RunState, stop_event: asyncio.Event) -> int:
    proc = await asyncio.create_subprocess_exec(
        "bash",
        "scripts/run_paper.sh",
        cwd=repo_root(),
        env=env,
        start_new_session=True,
    )
    await run_state.register(proc)
    try:
        if stop_event.is_set():
            await run_state.kill_all(signal.SIGTERM)
        return await proc.wait()
    finally:
        await run_state.unregister(proc)


async def wait_until_window_start(
    symbol: str,
    slug: str,
    window_start_ts: int,
    stop_event: asyncio.Event,
) -> bool:
    """Return False if stop was requested before window start."""
    wait_secs = window_start_ts - int(time.time())
    if wait_secs <= 0:
        return True
    print(
        f"[{symbol}] waiting {wait_secs}s until window start ts={window_start_ts} slug={slug}",
        flush=True,
    )
    deadline = time.time() + wait_secs
    while not stop_event.is_set():
        remain = deadline - time.time()
        if remain <= 0:
            return True
        await asyncio.sleep(min(1.0, remain))
    return False


async def worker(
    symbol: str,
    args: argparse.Namespace,
    market_budget: float,
    global_total_budget: float,
    session_ts: int,
    sem: asyncio.Semaphore,
    stop_event: asyncio.Event,
    run_state: RunState,
):
    if args.rolling_budget_floor <= 0:
        raise RuntimeError("--rolling-budget-floor must be > 0")

    curve_csv_path = (
        repo_root()
        / "logs"
        / "multi"
        / symbol
        / f"budget_curve_{session_ts}.csv"
    )
    ensure_curve_csv(curve_csv_path)

    t = make_window_start(args.start_mode)
    first_slug = market_slug(symbol, t)
    print(
        f"[{symbol}] start_mode={args.start_mode} first_window_start={t} slug={first_slug}",
        flush=True,
    )
    completed = 0
    attempts = 0
    while not stop_event.is_set():
        if args.windows_per_market and completed >= args.windows_per_market:
            return
        if args.max_window_attempts and attempts >= args.max_window_attempts:
            raise RuntimeError(f"max_window_attempts reached for {symbol}")

        slug = market_slug(symbol, t)
        attempts += 1
        ok, reason = await run_validate(slug, args.gamma_host)
        if not ok:
            print(f"[{symbol}] validate failed slug={slug} {reason}", flush=True)
            await asyncio.sleep(args.sleep_on_invalid_secs)
            t += 900
            continue

        log_dir = repo_root() / "logs" / "multi" / symbol / str(t)
        log_dir.mkdir(parents=True, exist_ok=True)
        if args.wait_until_window_start:
            if not await wait_until_window_start(symbol, slug, t, stop_event):
                return
        window_run_secs = resolve_window_run_secs(args.run_secs, t)
        window_end_ts = t + 900
        now_ts = int(time.time())
        remaining_to_window_end = max(0, window_end_ts - now_ts)
        print(
            (
                f"[{symbol}] window timing slug={slug} now_ts={now_ts} "
                f"window_end_ts={window_end_ts} remaining_to_end={remaining_to_window_end}s "
                f"run_secs={window_run_secs}"
            ),
            flush=True,
        )
        env = build_env(
            args,
            slug,
            symbol,
            t,
            log_dir,
            market_budget=market_budget,
            global_total_budget=global_total_budget,
            run_secs=window_run_secs,
        )

        async with sem:
            print(
                f"[{symbol}] run window slug={slug} budget={market_budget:.4f} log_dir={log_dir}",
                flush=True,
            )
            print(
                (
                    f"[{symbol}] risk_params pair_limit_mode={env.get('NO_RISK_PAIR_LIMIT_MODE')} "
                    f"pair_regression_mode={env.get('ENTRY_PAIR_REGRESSION_MODE')} "
                    f"pair_regression_soft_band_bps={env.get('ENTRY_PAIR_REGRESSION_SOFT_BAND_BPS')} "
                    f"entry_hedge_mode={env.get('NO_RISK_ENTRY_HEDGE_PRICE_MODE')} "
                    f"hard_cap={env.get('NO_RISK_HARD_PAIR_CAP')} "
                    f"max_rounds={env.get('MAX_ROUNDS')} "
                    f"entry_dynamic_cap_headroom_bps={env.get('ENTRY_DYNAMIC_CAP_HEADROOM_BPS')} "
                    f"entry_target_margin_min_ticks={env.get('ENTRY_TARGET_MARGIN_MIN_TICKS')} "
                    f"entry_target_margin_min_bps={env.get('ENTRY_TARGET_MARGIN_MIN_BPS')} "
                    f"entry_max_passive_ticks={env.get('ENTRY_MAX_PASSIVE_TICKS_FOR_NET_INCREASE')} "
                    f"open_min_fill_prob={env.get('OPEN_MIN_FILL_PROB')} "
                    f"margin_enforce={env.get('HEDGE_RECOVERABILITY_MARGIN_ENFORCE')} "
                    f"margin_min_ticks={env.get('HEDGE_RECOVERABILITY_MARGIN_MIN_TICKS')} "
                    f"margin_min_bps={env.get('HEDGE_RECOVERABILITY_MARGIN_MIN_BPS')} "
                    f"recoverability_enforce={env.get('NO_RISK_HEDGE_RECOVERABILITY_ENFORCE')} "
                    f"unrecoverable_grace_ms={env.get('OPEN_ORDER_UNRECOVERABLE_GRACE_MS')} "
                    f"passive_penalty_per_tick={env.get('MAKER_FILL_PASSIVE_QUEUE_PENALTY_PER_TICK')} "
                    f"passive_decay_k={env.get('MAKER_FILL_PASSIVE_DECAY_K')} "
                    f"passive_gap_soft_max_ticks={env.get('ENTRY_PASSIVE_GAP_SOFT_MAX_TICKS')} "
                    f"dynamic_slicing={env.get('ROUND_DYNAMIC_SLICING_ENABLED')} "
                    f"slice_range=[{env.get('ROUND_MIN_SLICES')},{env.get('ROUND_MAX_SLICES')}] "
                    f"entry_top_share={env.get('ENTRY_MAX_TOP_BOOK_SHARE')} "
                    f"entry_flow_util={env.get('ENTRY_MAX_FLOW_UTILIZATION')} "
                    f"entry_edge_min_bps={env.get('ENTRY_EDGE_MIN_BPS')} "
                    f"entry_fill_prob_min={env.get('ENTRY_FILL_PROB_MIN')} "
                    f"maker_min_fill_prob={env.get('MAKER_MIN_FILL_PROB')} "
                    f"maker_fill_horizon_secs={env.get('MAKER_FILL_HORIZON_SECS')} "
                    f"lock_min_completed_rounds={env.get('LOCK_MIN_COMPLETED_ROUNDS')} "
                    f"lock_min_spent_ratio={env.get('LOCK_MIN_SPENT_RATIO')} "
                    f"lock_force_time_left_secs={env.get('LOCK_FORCE_TIME_LEFT_SECS')} "
                    f"lock_allow_reopen_before_freeze={env.get('LOCK_ALLOW_REOPEN_BEFORE_FREEZE')} "
                    f"lock_reopen_max_rounds={env.get('LOCK_REOPEN_MAX_ROUNDS')} "
                    f"paper_min_requote_interval_ms={env.get('PAPER_MIN_REQUOTE_INTERVAL_MS')} "
                    f"paper_requote_price_delta_ticks={env.get('PAPER_REQUOTE_PRICE_DELTA_TICKS')} "
                    f"paper_requote_stale_ms_hard={env.get('PAPER_REQUOTE_STALE_MS_HARD')} "
                    f"requote_min_fill_prob_uplift={env.get('REQUOTE_MIN_FILL_PROB_UPLIFT')} "
                    f"requote_queue_stickiness_ratio={env.get('REQUOTE_QUEUE_STICKINESS_RATIO')} "
                    f"requote_stickiness_min_age_secs={env.get('REQUOTE_STICKINESS_MIN_AGE_SECS')} "
                    f"open_order_max_age_secs={env.get('OPEN_ORDER_MAX_AGE_SECS')} "
                    f"paper_order_timeout_secs={env.get('PAPER_ORDER_TIMEOUT_SECS')} "
                    f"paper_timeout_target_fill_prob={env.get('PAPER_TIMEOUT_TARGET_FILL_PROB')} "
                    f"paper_timeout_progress_extend_min={env.get('PAPER_TIMEOUT_PROGRESS_EXTEND_MIN')} "
                    f"paper_timeout_progress_extend_secs={env.get('PAPER_TIMEOUT_PROGRESS_EXTEND_SECS')} "
                    f"paper_timeout_max_extends={env.get('PAPER_TIMEOUT_MAX_EXTENDS')} "
                    f"open_margin_surplus_min={env.get('OPEN_MARGIN_SURPLUS_MIN')}"
                ),
                flush=True,
            )
            code = await run_paper(env, run_state, stop_event)
        if code != 0:
            raise RuntimeError(f"run_paper failed slug={slug} code={code}")

        current_budget = market_budget
        jsonl_path = find_latest_paper_jsonl(log_dir)
        parse_ok = False
        window_pnl_usdc = None
        end_budget = current_budget
        spent_total_usdc = None
        final_pair_cost = None
        final_qty_up = None
        final_qty_down = None
        hedgeable_shares = None
        unmatched_loss_usdc = None
        if jsonl_path is None:
            print(
                f"[{symbol}] WARN: no *_paper.jsonl found under {log_dir}, keep budget unchanged",
                flush=True,
            )
        else:
            snap = extract_last_snapshot_from_jsonl(jsonl_path)
            if snap is None:
                print(
                    f"[{symbol}] WARN: no dryrun_snapshot in {jsonl_path}, keep budget unchanged",
                    flush=True,
                )
            else:
                parse_ok = True
                spent_total_usdc = safe_float(snap.get("spent_total_usdc"))
                final_pair_cost = safe_float(snap.get("pair_cost"))
                final_qty_up = safe_float(snap.get("qty_up"))
                final_qty_down = safe_float(snap.get("qty_down"))
                hedgeable_shares, window_pnl_usdc, unmatched_loss_usdc = (
                    compute_window_pnl_from_snapshot(snap)
                )
                if (
                    args.budget_roll_mode == "pnl_compound"
                    and window_pnl_usdc is not None
                ):
                    end_budget = max(
                        args.rolling_budget_floor, current_budget + window_pnl_usdc
                    )
                    market_budget = end_budget
                else:
                    market_budget = current_budget
                print(
                    (
                        f"[{symbol}] settle slug={slug} budget_start={current_budget:.4f} "
                        f"window_pnl={window_pnl_usdc if window_pnl_usdc is not None else 'NA'} "
                        f"budget_end={end_budget:.4f} "
                        f"spent_total={spent_total_usdc if spent_total_usdc is not None else 'NA'} "
                        f"pair_cost_final={final_pair_cost if final_pair_cost is not None else 'NA'} "
                        f"jsonl={jsonl_path}"
                    ),
                    flush=True,
                )

        append_curve_row(
            curve_csv_path,
            [
                symbol,
                completed + 1,
                t,
                slug,
                f"{current_budget:.10f}",
                "" if window_pnl_usdc is None else f"{window_pnl_usdc:.10f}",
                f"{end_budget:.10f}",
                "" if spent_total_usdc is None else f"{spent_total_usdc:.10f}",
                "" if final_pair_cost is None else f"{final_pair_cost:.10f}",
                "" if final_qty_up is None else f"{final_qty_up:.10f}",
                "" if final_qty_down is None else f"{final_qty_down:.10f}",
                "" if hedgeable_shares is None else f"{hedgeable_shares:.10f}",
                "" if unmatched_loss_usdc is None else f"{unmatched_loss_usdc:.10f}",
                str(jsonl_path) if jsonl_path is not None else "",
                "1" if parse_ok else "0",
            ],
        )

        completed += 1
        t += 900


async def main_async():
    args = parse_args()
    if not args.gamma_host:
        args.gamma_host = os.environ.get("GAMMA_HOST", DEFAULT_GAMMA_HOST)
    symbols = [s.strip().lower() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        raise SystemExit("no symbols provided")
    global_total_budget = resolve_global_total_budget(args)
    market_budgets = allocate_market_budgets(symbols, args, global_total_budget)
    session_ts = int(time.time())
    total_assigned = sum(market_budgets.values())
    print(
        (
            f"budget_config strategy={args.budget_strategy} "
            f"global_total_budget={global_total_budget:.4f} "
            f"per_market_cap={args.per_market_budget_cap:.4f} "
            f"total_assigned={total_assigned:.4f} "
            f"budget_roll_mode={args.budget_roll_mode} "
            f"rolling_budget_floor={args.rolling_budget_floor:.4f}"
        ),
        flush=True,
    )
    for sym in symbols:
        curve_path = (
            repo_root() / "logs" / "multi" / sym / f"budget_curve_{session_ts}.csv"
        )
        print(
            f"[{sym}] market_budget={market_budgets[sym]:.4f} budget_curve_csv={curve_path}",
            flush=True,
        )

    parallelism = args.parallelism or len(symbols)
    sem = asyncio.Semaphore(max(1, parallelism))
    stop_event = asyncio.Event()
    run_state = RunState()

    loop = asyncio.get_running_loop()

    async def _kill_all() -> None:
        await run_state.kill_all(signal.SIGTERM)
        await asyncio.sleep(2)
        await run_state.kill_all(signal.SIGKILL)

    def _handle_signal():
        if not stop_event.is_set():
            print("signal received, stopping...", flush=True)
            stop_event.set()
        asyncio.create_task(_kill_all())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    tasks = [
        asyncio.create_task(
            worker(
                sym,
                args,
                market_budgets[sym],
                global_total_budget,
                session_ts,
                sem,
                stop_event,
                run_state,
            )
        )
        for sym in symbols
    ]
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
    for task in done:
        if task.exception():
            stop_event.set()
            await run_state.kill_all(signal.SIGTERM)
            for pending_task in pending:
                pending_task.cancel()
            raise task.exception()

    if stop_event.is_set():
        await run_state.kill_all(signal.SIGTERM)
        for pending_task in pending:
            pending_task.cancel()


def main():
    try:
        asyncio.run(main_async())
        return 0
    except KeyboardInterrupt:
        return 2
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

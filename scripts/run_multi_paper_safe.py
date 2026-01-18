#!/usr/bin/env python3
import argparse
import asyncio
import os
import signal
import sys
import time
from pathlib import Path
from typing import Dict, Tuple


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
        "--parallelism",
        type=int,
        default=0,
        help="Parallelism limit (default: number of symbols)",
    )
    parser.add_argument("--total-budget", type=float, default=10.0)
    parser.add_argument("--max-rounds", type=int, default=2)
    parser.add_argument("--round-budget", type=float, default=0.0)
    parser.add_argument("--round-leg1-fraction", type=float, default=0.45)
    parser.add_argument("--tail-freeze-secs", type=int, default=300)
    parser.add_argument("--tail-close-secs", type=int, default=180)
    parser.add_argument("--decision-every-ms", type=int, default=200)
    parser.add_argument("--run-secs", type=int, default=900)
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


def build_env(args: argparse.Namespace, slug: str, symbol: str, t: int, log_dir: Path) -> dict:
    env = os.environ.copy()
    env["RUN_SECS"] = str(args.run_secs)
    env["DRYRUN_MODE"] = "paper"
    env["ALLOW_TAKER"] = "false"
    env["TOTAL_BUDGET_USDC"] = str(args.total_budget)
    env["MAX_ROUNDS"] = str(args.max_rounds)
    if args.round_budget > 0:
        env["ROUND_BUDGET_USDC"] = str(args.round_budget)
    else:
        env["ROUND_BUDGET_USDC"] = str(args.total_budget / max(1, args.max_rounds))
    env["ROUND_LEG1_FRACTION"] = str(args.round_leg1_fraction)
    env["DRYRUN_TAIL_FREEZE_SECS"] = str(args.tail_freeze_secs)
    env["DRYRUN_TAIL_CLOSE_SECS"] = str(args.tail_close_secs)
    env["DRYRUN_DECISION_EVERY_MS"] = str(args.decision_every_ms)
    env["MARKET_SLUG"] = slug
    env["RUN_ID"] = f"{symbol}_{t}"
    env["LOG_DIR"] = str(log_dir)
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


async def worker(
    symbol: str,
    args: argparse.Namespace,
    sem: asyncio.Semaphore,
    stop_event: asyncio.Event,
    run_state: RunState,
):
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
        env = build_env(args, slug, symbol, t, log_dir)

        async with sem:
            print(f"[{symbol}] run window slug={slug} log_dir={log_dir}", flush=True)
            code = await run_paper(env, run_state, stop_event)
        if code != 0:
            raise RuntimeError(f"run_paper failed slug={slug} code={code}")

        completed += 1
        t += 900


async def main_async():
    args = parse_args()
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        raise SystemExit("no symbols provided")

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
        asyncio.create_task(worker(sym, args, sem, stop_event, run_state))
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

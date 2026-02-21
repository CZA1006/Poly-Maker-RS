#!/usr/bin/env python3
import argparse
import csv
import json
import math
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Summarize multi-market paper runs under logs/multi/."
    )
    parser.add_argument(
        "--root",
        default="logs/multi",
        help="Root directory containing per-market logs (default: logs/multi)",
    )
    parser.add_argument(
        "--out-prefix",
        default="logs/multi_summary",
        help="Output prefix for summary files (default: logs/multi_summary)",
    )
    parser.add_argument(
        "--plot",
        action="store_true",
        help="Generate PnL curve PNG via matplotlib (default: off)",
    )
    parser.add_argument(
        "--last-n",
        type=int,
        default=0,
        help="Only include the most recent N windows by window_ts across the selected root (default: 0 = all)",
    )
    parser.add_argument(
        "--noop-deny-ratio-min",
        type=float,
        default=0.70,
        help="Minimum opportunity-deny ratio to classify no-opportunity window (default: 0.70)",
    )
    parser.add_argument(
        "--noop-min-denies",
        type=int,
        default=10,
        help="Minimum deny count required before no-opportunity classification (default: 10)",
    )
    return parser.parse_args()


def iter_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def summarize_run(path: Path):
    fatal = False
    apply_count = 0
    best_action_nonnull = 0
    sim_order_open_count = 0
    hedge_not_recoverable_denies = 0
    hedge_margin_insufficient_denies = 0
    entry_worst_pair_denies = 0
    total_candidate_denies = 0
    deny_counter = Counter()
    last_snapshot = None
    last_ts = None
    max_round_idx = None
    spent_total_usdc = None
    final_qty_up = None
    final_qty_down = None
    final_pair_cost = None
    final_cost_up = None
    final_cost_down = None

    for obj in iter_jsonl(path):
        kind = obj.get("kind")
        if kind == "fatal_error":
            fatal = True
        elif kind == "dryrun_apply":
            apply_count += 1
        elif kind == "dryrun_candidates":
            data = obj.get("data", {})
            if data.get("best_action") is not None:
                best_action_nonnull += 1
            for cand in data.get("candidates", []) or []:
                deny = cand.get("deny_reason")
                if deny is not None:
                    deny_counter[str(deny)] += 1
                    total_candidate_denies += 1
                    if str(deny) == "hedge_not_recoverable":
                        hedge_not_recoverable_denies += 1
                    if str(deny) == "hedge_margin_insufficient":
                        hedge_margin_insufficient_denies += 1
                    if str(deny) == "entry_worst_pair":
                        entry_worst_pair_denies += 1
        elif kind == "user_ws_order":
            data = obj.get("data", {})
            if data.get("raw_type") == "sim_order_open":
                sim_order_open_count += 1
        elif kind == "dryrun_snapshot":
            data = obj.get("data", {})
            ts = obj.get("ts_ms")
            if ts is None:
                ts = data.get("ts_ms")
            if last_ts is None or (ts is not None and ts >= last_ts):
                last_ts = ts
                last_snapshot = data

            round_idx = data.get("round_idx")
            if isinstance(round_idx, (int, float)):
                if max_round_idx is None or round_idx > max_round_idx:
                    max_round_idx = round_idx

    if last_snapshot:
        spent_total_usdc = last_snapshot.get("spent_total_usdc")
        final_qty_up = last_snapshot.get("qty_up")
        final_qty_down = last_snapshot.get("qty_down")
        final_pair_cost = last_snapshot.get("pair_cost")
        final_cost_up = last_snapshot.get("cost_up")
        final_cost_down = last_snapshot.get("cost_down")

    deny_top = ",".join([k for k, _ in deny_counter.most_common(3)])

    return {
        "fatal": fatal,
        "apply_count": apply_count,
        "best_action_nonnull_count": best_action_nonnull,
        "sim_order_open_count": sim_order_open_count,
        "hedge_not_recoverable_denies": hedge_not_recoverable_denies,
        "hedge_margin_insufficient_denies": hedge_margin_insufficient_denies,
        "entry_worst_pair_denies": entry_worst_pair_denies,
        "total_candidate_denies": total_candidate_denies,
        "max_round_idx": max_round_idx,
        "spent_total_usdc": spent_total_usdc,
        "final_qty_up": final_qty_up,
        "final_qty_down": final_qty_down,
        "final_pair_cost": final_pair_cost,
        "final_cost_up": final_cost_up,
        "final_cost_down": final_cost_down,
        "deny_top": deny_top,
    }


def safe_float(value):
    try:
        if value is None:
            return None
        out = float(value)
        if not math.isfinite(out):
            return None
        return out
    except Exception:
        return None


def compute_window_pnl(
    final_pair_cost, final_qty_up, final_qty_down, spent_total_usdc, final_cost_up, final_cost_down
):
    pair_cost = safe_float(final_pair_cost)
    qty_up = safe_float(final_qty_up)
    qty_down = safe_float(final_qty_down)
    spent_total = safe_float(spent_total_usdc)
    cost_up = safe_float(final_cost_up)
    cost_down = safe_float(final_cost_down)
    if qty_up is None or qty_down is None:
        return None, None, None
    hedgeable_shares = max(0.0, min(qty_up, qty_down))
    unmatched_loss_usdc = None

    # Strict arbitrage accounting:
    # unmatched side settles to 0, so PnL = matched payout - total spend.
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

    # Backward-compatible fallback when spend field is missing.
    if pair_cost is None:
        return hedgeable_shares, None, unmatched_loss_usdc
    window_pnl = (1.0 - pair_cost) * hedgeable_shares
    return hedgeable_shares, window_pnl, unmatched_loss_usdc


def parse_window_ts(value):
    try:
        if value is None:
            return 0
        return int(str(value).strip())
    except Exception:
        return 0


def infer_symbol_window(root: Path, rel: Path, run_id: str):
    parts = rel.parts
    symbol = "unknown"
    window_ts = ""

    if len(parts) >= 3:
        # root like logs/multi -> symbol/window/file
        symbol = parts[0]
        window_ts = parts[1]
    elif len(parts) >= 2:
        # root like logs/multi/<symbol> -> window/file
        parent = parts[0]
        if str(parent).isdigit():
            window_ts = str(parent)
            root_name = root.name
            symbol = root_name if root_name and not root_name.isdigit() else "unknown"
        else:
            symbol = str(parent)
    else:
        symbol = root.name if root.name and not root.name.isdigit() else "unknown"

    if not window_ts:
        m = re.search(r"(\d{10})", run_id)
        if m:
            window_ts = m.group(1)

    return symbol, window_ts


def main():
    args = parse_args()
    noop_include_entry_worst_pair = (
        os.environ.get("NOOP_INCLUDE_ENTRY_WORST_PAIR", "true").strip().lower()
        in {"1", "true", "yes", "on"}
    )
    root = Path(args.root)
    out_prefix = Path(args.out_prefix)
    out_prefix.parent.mkdir(parents=True, exist_ok=True)

    jsonl_files = sorted(root.glob("**/*_paper.jsonl"))
    if not jsonl_files:
        print(f"no jsonl files found under {root}", file=sys.stderr)
        return 1

    rows = []
    for path in jsonl_files:
        rel = path.relative_to(root)
        run_id = path.stem.replace("_paper", "")
        symbol, window_ts = infer_symbol_window(root, rel, run_id)

        summary = summarize_run(path)
        row = {
            "symbol": symbol,
            "window_ts": window_ts,
            "run_id": run_id,
            "jsonl_path": str(path),
            "pass_fail": "FAIL" if summary["fatal"] else "PASS",
            "apply_count": summary["apply_count"],
            "best_action_nonnull_count": summary["best_action_nonnull_count"],
            "sim_order_open_count": summary["sim_order_open_count"],
            "hedge_not_recoverable_denies": summary["hedge_not_recoverable_denies"],
            "hedge_margin_insufficient_denies": summary[
                "hedge_margin_insufficient_denies"
            ],
            "entry_worst_pair_denies": summary["entry_worst_pair_denies"],
            "total_candidate_denies": summary["total_candidate_denies"],
            "max_round_idx": summary["max_round_idx"],
            "spent_total_usdc": summary["spent_total_usdc"],
            "final_qty_up": summary["final_qty_up"],
            "final_qty_down": summary["final_qty_down"],
            "final_pair_cost": summary["final_pair_cost"],
            "final_cost_up": summary["final_cost_up"],
            "final_cost_down": summary["final_cost_down"],
            "deny_top": summary["deny_top"],
        }
        recoverability_denies = (
            summary["hedge_not_recoverable_denies"]
            + summary["hedge_margin_insufficient_denies"]
        )
        recoverability_deny_ratio = (
            (recoverability_denies / summary["total_candidate_denies"])
            if summary["total_candidate_denies"] > 0
            else 0.0
        )
        opportunity_denies = recoverability_denies + (
            summary["entry_worst_pair_denies"] if noop_include_entry_worst_pair else 0
        )
        opportunity_deny_ratio = (
            (opportunity_denies / summary["total_candidate_denies"])
            if summary["total_candidate_denies"] > 0
            else 0.0
        )
        row["hedge_not_recoverable_deny_ratio"] = recoverability_deny_ratio
        row["recoverability_denies"] = recoverability_denies
        row["recoverability_deny_ratio"] = recoverability_deny_ratio
        row["opportunity_denies"] = opportunity_denies
        row["opportunity_deny_ratio"] = opportunity_deny_ratio
        row["noop_include_entry_worst_pair"] = noop_include_entry_worst_pair
        row["no_opportunity_window"] = (
            row["best_action_nonnull_count"] == 0
            and row["apply_count"] == 0
            and row["sim_order_open_count"] == 0
            and row["total_candidate_denies"] >= args.noop_min_denies
            and opportunity_deny_ratio >= args.noop_deny_ratio_min - 1e-12
        )
        hedgeable_shares, window_pnl, unmatched_loss_usdc = compute_window_pnl(
            row["final_pair_cost"],
            row["final_qty_up"],
            row["final_qty_down"],
            row["spent_total_usdc"],
            row["final_cost_up"],
            row["final_cost_down"],
        )
        row["final_hedgeable_shares"] = hedgeable_shares
        row["unmatched_loss_usdc"] = unmatched_loss_usdc
        row["window_pnl_usdc"] = window_pnl
        rows.append(row)

    if args.last_n and args.last_n > 0:
        rows.sort(
            key=lambda row: (
                parse_window_ts(row.get("window_ts")),
                str(row.get("symbol", "")),
                str(row.get("run_id", "")),
            )
        )
        rows = rows[-args.last_n :]

    per_symbol = defaultdict(list)
    for row in rows:
        per_symbol[row.get("symbol", "unknown")].append(row)

    def sort_key(row):
        return (str(row.get("symbol", "")), parse_window_ts(row.get("window_ts")))

    rows.sort(key=sort_key)
    cumulative_by_symbol = defaultdict(float)
    cumulative_total = 0.0
    for row in rows:
        pnl = safe_float(row.get("window_pnl_usdc"))
        pnl = pnl if pnl is not None else 0.0
        symbol = row.get("symbol", "unknown")
        cumulative_by_symbol[symbol] += pnl
        cumulative_total += pnl
        row["symbol_cum_pnl_usdc"] = cumulative_by_symbol[symbol]
        row["total_cum_pnl_usdc"] = cumulative_total

    csv_path = out_prefix.with_suffix(".csv")
    md_path = out_prefix.with_suffix(".md")
    pnl_curve_csv = out_prefix.parent / f"{out_prefix.name}_pnl_curve.csv"
    pnl_curve_png = out_prefix.parent / f"{out_prefix.name}_pnl_curve.png"

    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "symbol",
                "window_ts",
                "run_id",
                "jsonl_path",
                "pass_fail",
                "apply_count",
                "best_action_nonnull_count",
                "sim_order_open_count",
                "hedge_not_recoverable_denies",
                "hedge_margin_insufficient_denies",
                "entry_worst_pair_denies",
                "total_candidate_denies",
                "hedge_not_recoverable_deny_ratio",
                "recoverability_denies",
                "recoverability_deny_ratio",
                "opportunity_denies",
                "opportunity_deny_ratio",
                "noop_include_entry_worst_pair",
                "no_opportunity_window",
                "max_round_idx",
                "spent_total_usdc",
                "final_qty_up",
                "final_qty_down",
                "final_pair_cost",
                "final_cost_up",
                "final_cost_down",
                "final_hedgeable_shares",
                "unmatched_loss_usdc",
                "window_pnl_usdc",
                "symbol_cum_pnl_usdc",
                "total_cum_pnl_usdc",
                "deny_top",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    with pnl_curve_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "symbol",
                "window_ts",
                "window_index",
                "window_pnl_usdc",
                "symbol_cum_pnl_usdc",
                "total_cum_pnl_usdc",
                "jsonl_path",
            ],
        )
        writer.writeheader()
        symbol_indices = defaultdict(int)
        for row in rows:
            symbol = row.get("symbol", "unknown")
            symbol_indices[symbol] += 1
            writer.writerow(
                {
                    "symbol": symbol,
                    "window_ts": row.get("window_ts"),
                    "window_index": symbol_indices[symbol],
                    "window_pnl_usdc": row.get("window_pnl_usdc"),
                    "symbol_cum_pnl_usdc": row.get("symbol_cum_pnl_usdc"),
                    "total_cum_pnl_usdc": row.get("total_cum_pnl_usdc"),
                    "jsonl_path": row.get("jsonl_path"),
                }
            )

    lines = []
    lines.append("# Multi Run Summary")
    lines.append("")
    lines.append(f"Root: `{root}`")
    if args.last_n and args.last_n > 0:
        lines.append(f"Window filter: last {args.last_n}")
    else:
        lines.append("Window filter: all")
    lines.append(f"Runs: {len(rows)}")
    total_pnl = sum(
        safe_float(r.get("window_pnl_usdc")) or 0.0
        for r in rows
    )
    lines.append(f"Total PnL (all runs): {total_pnl:.6f} USDC")
    lines.append("")
    lines.append("## By Symbol")
    lines.append("")
    lines.append(
        "| symbol | runs | pass | fail | apply_count | max_round_idx | spent_total_usdc | final_pair_cost | symbol_total_pnl_usdc | deny_top |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---|")
    for symbol, items in sorted(per_symbol.items()):
        pass_count = sum(1 for r in items if r["pass_fail"] == "PASS")
        fail_count = sum(1 for r in items if r["pass_fail"] == "FAIL")
        apply_total = sum(int(r["apply_count"] or 0) for r in items)
        max_round = max(
            [r["max_round_idx"] for r in items if r["max_round_idx"] is not None] or [0]
        )
        last = max(items, key=lambda r: r["window_ts"])
        lines.append(
            "| {symbol} | {runs} | {passc} | {failc} | {apply_total} | {max_round} | {spent} | {pair_cost} | {symbol_total_pnl:.6f} | {deny_top} |".format(
                symbol=symbol,
                runs=len(items),
                passc=pass_count,
                failc=fail_count,
                apply_total=apply_total,
                max_round=max_round,
                spent=last.get("spent_total_usdc"),
                pair_cost=last.get("final_pair_cost"),
                symbol_total_pnl=(safe_float(last.get("symbol_cum_pnl_usdc")) or 0.0),
                deny_top=last.get("deny_top"),
            )
        )

    lines.append("")
    lines.append("## Runs")
    lines.append("")
    lines.append(
        "| symbol | window_ts | pass_fail | apply_count | max_round_idx | spent_total_usdc | final_qty_up | final_qty_down | final_pair_cost | final_hedgeable_shares | unmatched_loss_usdc | window_pnl_usdc | symbol_cum_pnl_usdc | total_cum_pnl_usdc | deny_top |"
    )
    lines.append("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|")
    for row in rows:
        lines.append(
            "| {symbol} | {window_ts} | {pass_fail} | {apply_count} | {max_round_idx} | {spent_total_usdc} | {final_qty_up} | {final_qty_down} | {final_pair_cost} | {final_hedgeable_shares} | {unmatched_loss_usdc} | {window_pnl_usdc} | {symbol_cum_pnl_usdc} | {total_cum_pnl_usdc} | {deny_top} |".format(
                **row
            )
        )

    # Optional PnL curve plot
    if args.plot:
        try:
            import matplotlib.pyplot as plt  # type: ignore

            fig, ax = plt.subplots(figsize=(10, 5))
            plotted = False
            for symbol, items in sorted(per_symbol.items()):
                series = sorted(items, key=sort_key)
                xs = []
                ys = []
                for idx, row in enumerate(series, 1):
                    xs.append(idx)
                    ys.append(safe_float(row.get("symbol_cum_pnl_usdc")) or 0.0)
                if xs:
                    ax.plot(xs, ys, marker="o", label=symbol)
                    plotted = True

            if plotted:
                ax.axhline(0.0, color="gray", linewidth=1, linestyle="--")
                ax.set_xlabel("Window Index (per symbol)")
                ax.set_ylabel("Cumulative PnL (USDC)")
                ax.set_title("Cumulative PnL Curve")
                ax.legend()
                fig.tight_layout()
                fig.savefig(pnl_curve_png)
            plt.close(fig)
        except Exception:
            pass

    lines.append("")
    lines.append("## PnL Curve")
    lines.append(f"- Curve CSV: `{pnl_curve_csv.name}`")
    if pnl_curve_png.exists():
        lines.append(f"- See {pnl_curve_png.name}")
    else:
        if args.plot:
            lines.append("- PNG not generated (matplotlib missing or no data)")
        else:
            lines.append("- PNG skipped (run with `--plot` to generate)")

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"wrote {csv_path}")
    print(f"wrote {pnl_curve_csv}")
    print(f"wrote {md_path}")
    if pnl_curve_png.exists():
        print(f"wrote {pnl_curve_png}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

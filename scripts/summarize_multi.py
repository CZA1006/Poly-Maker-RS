#!/usr/bin/env python3
import argparse
import csv
import json
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
    deny_counter = Counter()
    last_snapshot = None
    last_ts = None
    max_round_idx = None
    spent_total_usdc = None
    final_qty_up = None
    final_qty_down = None
    final_pair_cost = None

    for obj in iter_jsonl(path):
        kind = obj.get("kind")
        if kind == "fatal_error":
            fatal = True
        elif kind == "dryrun_apply":
            apply_count += 1
        elif kind == "dryrun_candidates":
            data = obj.get("data", {})
            for cand in data.get("candidates", []) or []:
                deny = cand.get("deny_reason")
                if deny is not None:
                    deny_counter[str(deny)] += 1
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

    deny_top = ",".join([k for k, _ in deny_counter.most_common(3)])

    return {
        "fatal": fatal,
        "apply_count": apply_count,
        "max_round_idx": max_round_idx,
        "spent_total_usdc": spent_total_usdc,
        "final_qty_up": final_qty_up,
        "final_qty_down": final_qty_down,
        "final_pair_cost": final_pair_cost,
        "deny_top": deny_top,
    }


def main():
    args = parse_args()
    root = Path(args.root)
    out_prefix = Path(args.out_prefix)
    out_prefix.parent.mkdir(parents=True, exist_ok=True)

    jsonl_files = sorted(root.glob("**/*_paper.jsonl"))
    if not jsonl_files:
        print(f"no jsonl files found under {root}", file=sys.stderr)
        return 1

    rows = []
    per_symbol = defaultdict(list)
    for path in jsonl_files:
        rel = path.relative_to(root)
        parts = rel.parts
        symbol = parts[0] if len(parts) >= 3 else "unknown"
        window_ts = parts[1] if len(parts) >= 3 else ""
        run_id = path.stem.replace("_paper", "")

        summary = summarize_run(path)
        row = {
            "symbol": symbol,
            "window_ts": window_ts,
            "run_id": run_id,
            "jsonl_path": str(path),
            "pass_fail": "FAIL" if summary["fatal"] else "PASS",
            "apply_count": summary["apply_count"],
            "max_round_idx": summary["max_round_idx"],
            "spent_total_usdc": summary["spent_total_usdc"],
            "final_qty_up": summary["final_qty_up"],
            "final_qty_down": summary["final_qty_down"],
            "final_pair_cost": summary["final_pair_cost"],
            "deny_top": summary["deny_top"],
        }
        rows.append(row)
        per_symbol[symbol].append(row)

    csv_path = out_prefix.with_suffix(".csv")
    md_path = out_prefix.with_suffix(".md")

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
                "max_round_idx",
                "spent_total_usdc",
                "final_qty_up",
                "final_qty_down",
                "final_pair_cost",
                "deny_top",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    lines = []
    lines.append("# Multi Run Summary")
    lines.append("")
    lines.append(f"Root: `{root}`")
    lines.append(f"Runs: {len(rows)}")
    lines.append("")
    lines.append("## By Symbol")
    lines.append("")
    lines.append(
        "| symbol | runs | pass | fail | apply_count | max_round_idx | spent_total_usdc | final_pair_cost | deny_top |"
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
            "| {symbol} | {runs} | {passc} | {failc} | {apply_total} | {max_round} | {spent} | {pair_cost} | {deny_top} |".format(
                symbol=symbol,
                runs=len(items),
                passc=pass_count,
                failc=fail_count,
                apply_total=apply_total,
                max_round=max_round,
                spent=last.get("spent_total_usdc"),
                pair_cost=last.get("final_pair_cost"),
                deny_top=last.get("deny_top"),
            )
        )

    lines.append("")
    lines.append("## Runs")
    lines.append("")
    lines.append(
        "| symbol | window_ts | pass_fail | apply_count | max_round_idx | spent_total_usdc | final_qty_up | final_qty_down | final_pair_cost | deny_top |"
    )
    lines.append("|---|---|---|---:|---:|---:|---:|---:|---:|---|")
    for row in rows:
        lines.append(
            "| {symbol} | {window_ts} | {pass_fail} | {apply_count} | {max_round_idx} | {spent_total_usdc} | {final_qty_up} | {final_qty_down} | {final_pair_cost} | {deny_top} |".format(
                **row
            )
        )

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"wrote {csv_path}")
    print(f"wrote {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

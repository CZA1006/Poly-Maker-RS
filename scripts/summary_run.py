#!/usr/bin/env python3
import argparse
import csv
import json
import math
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Summarize a run JSONL into markdown, csv, and optional png."
    )
    parser.add_argument("jsonl_path", help="Path to logs/<run_id>_<mode>.jsonl")
    return parser.parse_args()


def parse_run_id_mode(path: Path):
    m = re.match(r"^(?P<run>\d{8}_\d{6})_(?P<mode>[^.]+)\.jsonl$", path.name)
    if not m:
        return "run", "unknown"
    return m.group("run"), m.group("mode")


def safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def load_jsonl(path: Path):
    parse_errors = 0
    for raw in path.read_text().splitlines():
        raw = raw.strip()
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            parse_errors += 1
            continue
        yield obj, parse_errors
    if parse_errors:
        yield None, parse_errors


def main():
    args = parse_args()
    input_path = Path(args.jsonl_path)
    if not input_path.is_file():
        print(f"error: file not found: {input_path}", file=sys.stderr)
        return 1

    run_id, mode = parse_run_id_mode(input_path)
    out_prefix = f"{run_id}_{mode}"
    out_dir = input_path.parent

    summary_md = out_dir / f"{out_prefix}_summary.md"
    decisions_csv = out_dir / f"{out_prefix}_decisions.csv"
    timeseries_csv = out_dir / f"{out_prefix}_timeseries.csv"
    denies_csv = out_dir / f"{out_prefix}_denies.csv"
    summary_png = out_dir / f"{out_prefix}_summary.png"

    decisions = {}
    snapshots = []
    deny_counts = Counter()
    parse_errors = 0
    series_slug = None
    apply_count = 0
    decision_count = 0
    best_action_nonnull = 0
    margin_target_denies = 0
    cap_value_up_hits = 0
    cap_value_down_hits = 0
    cap_shares_up_hits = 0
    cap_shares_down_hits = 0

    last_snapshot = None
    last_decision_seq = None
    last_decision_market_slug = None
    seen_decision_seqs = set()

    for item, errs in load_jsonl(input_path):
        if item is None:
            parse_errors = errs
            continue
        parse_errors = errs
        kind = item.get("kind")
        data = item.get("data") if isinstance(item.get("data"), dict) else {}
        ts_ms = item.get("ts_ms")
        if series_slug is None:
            series_slug = item.get("series_slug")

        if kind == "dryrun_snapshot":
            snap = dict(data)
            snap["ts_ms"] = ts_ms
            if snap.get("decision_seq") is None and last_decision_seq is not None:
                snap_market = snap.get("market_slug")
                if (
                    snap_market is None
                    or last_decision_market_slug is None
                    or snap_market == last_decision_market_slug
                ):
                    snap["decision_seq"] = last_decision_seq
            snapshots.append(snap)
            last_snapshot = snap
        elif kind == "dryrun_candidates":
            decision_seq = data.get("decision_seq")
            if decision_seq is None:
                continue
            decision_seq = int(decision_seq)
            if decision_seq not in seen_decision_seqs:
                seen_decision_seqs.add(decision_seq)
                decision_count += 1
            decision = decisions.setdefault(decision_seq, {})
            decision["decision_seq"] = decision_seq
            decision["ts_ms"] = ts_ms or decision.get("ts_ms")
            decision["market_slug"] = data.get("market_slug") or decision.get("market_slug")
            decision["apply_reason"] = data.get("apply_reason")
            last_decision_seq = decision_seq
            last_decision_market_slug = decision.get("market_slug")

            if last_snapshot is not None:
                if decision.get("market_slug") is None or decision.get("market_slug") == last_snapshot.get(
                    "market_slug"
                ):
                    for key in [
                        "time_left_secs",
                        "cooldown_active",
                        "pair_cost",
                        "qty_up",
                        "avg_up",
                        "qty_down",
                        "avg_down",
                        "hedgeable",
                        "unhedged_up",
                        "unhedged_down",
                        "unhedged_value_up",
                        "unhedged_value_down",
                    ]:
                        if key not in decision:
                            decision[key] = last_snapshot.get(key)

            best_action = data.get("best_action") if isinstance(data.get("best_action"), dict) else None
            if best_action:
                best_action_nonnull += 1
            decision["best_action_action"] = best_action.get("action") if best_action else None
            decision["best_action_kind"] = best_action.get("kind") if best_action else None
            decision["best_action_leg"] = best_action.get("leg") if best_action else None
            decision["best_action_fill_price"] = best_action.get("fill_price") if best_action else None
            decision["best_action_qty"] = best_action.get("qty") if best_action else None

            applied_action = data.get("applied_action") if isinstance(data.get("applied_action"), dict) else None
            decision["applied_action_action"] = applied_action.get("action") if applied_action else None
            decision["applied_action_kind"] = applied_action.get("kind") if applied_action else None
            decision["applied_action_leg"] = applied_action.get("leg") if applied_action else None
            decision["applied_action_fill_price"] = applied_action.get("fill_price") if applied_action else None
            decision["applied_action_qty"] = applied_action.get("qty") if applied_action else None

            allow_count = 0
            deny_count = 0
            per_decision_denies = Counter()

            for cand in data.get("candidates", []) if isinstance(data.get("candidates"), list) else []:
                allow = bool(cand.get("allow"))
                deny_reason = cand.get("deny_reason")
                if allow:
                    allow_count += 1
                else:
                    deny_count += 1
                    deny_key = deny_reason if deny_reason is not None else "null"
                    per_decision_denies[deny_key] += 1
                    deny_counts[deny_key] += 1
                    if deny_reason == "margin_target":
                        margin_target_denies += 1
                if cand.get("would_violate_cap_value_up") is True:
                    cap_value_up_hits += 1
                if cand.get("would_violate_cap_value_down") is True:
                    cap_value_down_hits += 1
                if cand.get("would_violate_cap_shares_up") is True:
                    cap_shares_up_hits += 1
                if cand.get("would_violate_cap_shares_down") is True:
                    cap_shares_down_hits += 1

            decision["allow_count"] = allow_count
            decision["deny_count"] = deny_count
            decision["deny_reason_top3"] = ",".join(
                [f"{k}:{v}" for k, v in per_decision_denies.most_common(3)]
            )
        elif kind == "dryrun_apply":
            apply_count += 1
            data = item.get("data") if isinstance(item.get("data"), dict) else {}
            decision_seq = data.get("decision_seq")
            if decision_seq is not None:
                decision = decisions.setdefault(int(decision_seq), {})
                decision["decision_seq"] = int(decision_seq)
                applied_action = data.get("applied_action") if isinstance(data.get("applied_action"), dict) else None
                decision["applied_action_action"] = applied_action.get("action") if applied_action else None
                decision["applied_action_kind"] = applied_action.get("kind") if applied_action else None
                decision["applied_action_leg"] = applied_action.get("leg") if applied_action else None
                decision["applied_action_fill_price"] = applied_action.get("fill_price") if applied_action else None
                decision["applied_action_qty"] = applied_action.get("qty") if applied_action else None

    for decision in decisions.values():
        pair_cost = decision.get("pair_cost")
        if pair_cost is None:
            avg_up = safe_float(decision.get("avg_up"))
            avg_down = safe_float(decision.get("avg_down"))
            if avg_up is not None and avg_down is not None:
                decision["pair_cost"] = avg_up + avg_down

    decisions_rows = []
    for seq in sorted(decisions.keys()):
        decision = decisions[seq]
        decisions_rows.append(
            [
                decision.get("decision_seq"),
                decision.get("ts_ms"),
                decision.get("market_slug"),
                decision.get("time_left_secs"),
                decision.get("cooldown_active"),
                decision.get("pair_cost"),
                decision.get("qty_up"),
                decision.get("avg_up"),
                decision.get("qty_down"),
                decision.get("avg_down"),
                decision.get("hedgeable"),
                decision.get("unhedged_up"),
                decision.get("unhedged_down"),
                decision.get("unhedged_value_up"),
                decision.get("unhedged_value_down"),
                decision.get("best_action_action"),
                decision.get("best_action_kind"),
                decision.get("best_action_leg"),
                decision.get("best_action_fill_price"),
                decision.get("best_action_qty"),
                decision.get("applied_action_action"),
                decision.get("applied_action_kind"),
                decision.get("applied_action_leg"),
                decision.get("applied_action_fill_price"),
                decision.get("applied_action_qty"),
                decision.get("apply_reason"),
                decision.get("allow_count"),
                decision.get("deny_count"),
                decision.get("deny_reason_top3"),
            ]
        )

    with decisions_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "decision_seq",
                "ts_ms",
                "market_slug",
                "time_left_secs",
                "cooldown_active",
                "pair_cost",
                "qty_up",
                "avg_up",
                "qty_down",
                "avg_down",
                "hedgeable",
                "unhedged_up",
                "unhedged_down",
                "unhedged_value_up",
                "unhedged_value_down",
                "best_action_action",
                "best_action_kind",
                "best_action_leg",
                "best_action_fill_price",
                "best_action_qty",
                "applied_action_action",
                "applied_action_kind",
                "applied_action_leg",
                "applied_action_fill_price",
                "applied_action_qty",
                "apply_reason",
                "allow_count",
                "deny_count",
                "deny_reason_top3",
            ]
        )
        writer.writerows(decisions_rows)

    timeseries_rows = []
    pair_cost_points_total = 0
    pair_cost_points_missing = 0
    for snap in sorted(snapshots, key=lambda s: s.get("ts_ms") or 0):
        pair_cost = snap.get("pair_cost")
        if pair_cost is None:
            avg_up = safe_float(snap.get("avg_up"))
            avg_down = safe_float(snap.get("avg_down"))
            if avg_up is not None and avg_down is not None:
                pair_cost = avg_up + avg_down
        pair_cost_points_total += 1
        if pair_cost is None:
            pair_cost_points_missing += 1
        timeseries_rows.append(
            [
                snap.get("ts_ms"),
                snap.get("decision_seq"),
                snap.get("time_left_secs"),
                pair_cost,
                snap.get("hedgeable"),
                snap.get("unhedged_value_up"),
                snap.get("unhedged_value_down"),
                snap.get("qty_up"),
                snap.get("qty_down"),
            ]
        )

    with timeseries_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "ts_ms",
                "decision_seq",
                "time_left_secs",
                "pair_cost",
                "hedgeable",
                "unhedged_value_up",
                "unhedged_value_down",
                "qty_up",
                "qty_down",
            ]
        )
        writer.writerows(timeseries_rows)

    # Optional plot
    try:
        import matplotlib.pyplot as plt  # type: ignore

        if timeseries_rows:
            ts0 = timeseries_rows[0][0] or 0
            xs = [((row[0] or ts0) - ts0) / 1000.0 for row in timeseries_rows]
            pair_costs = [
                safe_float(row[3]) if safe_float(row[3]) is not None else math.nan
                for row in timeseries_rows
            ]
            hedgeable = [safe_float(row[4]) for row in timeseries_rows]
            unhedged_up = [safe_float(row[5]) for row in timeseries_rows]
            unhedged_down = [safe_float(row[6]) for row in timeseries_rows]

            fig, axes = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
            axes[0].plot(xs, pair_costs, label="pair_cost")
            axes[0].set_ylabel("pair_cost")
            axes[0].legend()

            axes[1].plot(xs, hedgeable, label="hedgeable")
            axes[1].plot(xs, unhedged_up, label="unhedged_value_up")
            axes[1].plot(xs, unhedged_down, label="unhedged_value_down")
            axes[1].set_xlabel("time (s)")
            axes[1].legend()

            fig.tight_layout()
            fig.savefig(summary_png)
            plt.close(fig)
    except Exception as exc:
        print(
            f"INFO: matplotlib not available or plotting failed: {exc}. Skipping PNG.",
            file=sys.stderr,
        )

    deny_rows = []
    total_denies = sum(deny_counts.values())
    for reason, count in deny_counts.most_common():
        pct = (count / total_denies * 100.0) if total_denies else 0.0
        deny_rows.append([reason, count, f"{pct:.2f}"])

    with denies_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["deny_reason", "count", "pct"])
        writer.writerows(deny_rows)

    cooldown_hits = sum(1 for snap in snapshots if snap.get("cooldown_active") is True)
    summary_lines = []
    summary_lines.append(f"# Run Summary ({run_id}, {mode})")
    summary_lines.append("")
    summary_lines.append(f"- series_slug: {series_slug or 'unknown'}")
    summary_lines.append(f"- decision_count: {decision_count}")
    summary_lines.append(f"- apply_count: {apply_count}")
    summary_lines.append(f"- best_action_nonnull_count: {best_action_nonnull}")
    summary_lines.append(f"- cooldown_hits: {cooldown_hits}")
    summary_lines.append(f"- margin_target_denies: {margin_target_denies}")
    summary_lines.append(f"- cap_value_up_hits: {cap_value_up_hits}")
    summary_lines.append(f"- cap_value_down_hits: {cap_value_down_hits}")
    summary_lines.append(f"- cap_shares_up_hits: {cap_shares_up_hits}")
    summary_lines.append(f"- cap_shares_down_hits: {cap_shares_down_hits}")
    summary_lines.append(f"- pair_cost_points_total: {pair_cost_points_total}")
    summary_lines.append(f"- pair_cost_points_missing: {pair_cost_points_missing}")
    summary_lines.append(
        f"- pair_cost_points_plotted: {pair_cost_points_total - pair_cost_points_missing}"
    )
    summary_lines.append(f"- parse_errors: {parse_errors}")
    summary_lines.append("")
    summary_lines.append("## Inventory / Pair-Cost Curve")
    if summary_png.exists():
        summary_lines.append(f"- See {summary_png.name}")
    else:
        summary_lines.append("- PNG not generated (matplotlib missing or no data)")
    summary_lines.append("")
    summary_lines.append("## Decision Preview (first 20 rows)")
    summary_lines.append("")
    summary_lines.append(
        "| decision_seq | ts_ms | market_slug | cooldown_active | pair_cost | hedgeable | unhedged_value_up | unhedged_value_down | best_action_action | applied_action_action | allow_count | deny_count | deny_reason_top3 |"
    )
    summary_lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for row in decisions_rows[:20]:
        summary_lines.append(
            f"| {row[0]} | {row[1]} | {row[2]} | {row[4]} | {row[5]} | {row[10]} | {row[13]} | {row[14]} | {row[15]} | {row[20]} | {row[26]} | {row[27]} | {row[28]} |"
        )
    summary_lines.append("")
    summary_lines.append("## Deny Reasons (top 10)")
    for reason, count in deny_counts.most_common(10):
        summary_lines.append(f"- {reason}: {count}")

    summary_md.write_text("\n".join(summary_lines))

    print(f"wrote: {summary_md}")
    print(f"wrote: {decisions_csv}")
    print(f"wrote: {timeseries_csv}")
    print(f"wrote: {denies_csv}")
    if summary_png.exists():
        print(f"wrote: {summary_png}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

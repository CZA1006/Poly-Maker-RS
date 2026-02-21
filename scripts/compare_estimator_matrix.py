#!/usr/bin/env python3
import argparse
import json
import math
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compare estimator matrix runs and emit a single conclusion."
    )
    parser.add_argument(
        "--run",
        action="append",
        required=True,
        help="Run descriptor in the form name=/path/to/*.jsonl",
    )
    return parser.parse_args()


def percentile(values, p):
    if not values:
        return None
    vals = sorted(values)
    k = (len(vals) - 1) * (p / 100.0)
    lo = int(math.floor(k))
    hi = int(math.ceil(k))
    if lo == hi:
        return vals[lo]
    return vals[lo] + (vals[hi] - vals[lo]) * (k - lo)


def load_metrics(path: Path):
    fill_probs = []
    candidate_rows = 0
    deny_low_fill = 0
    applied_actions = 0
    decision_events = 0
    snapshot_events = 0

    with path.open() as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            obj = json.loads(raw)
            kind = obj.get("kind")
            if kind == "dryrun_snapshot":
                snapshot_events += 1
            elif kind == "dryrun_apply":
                applied_actions += 1
            elif kind == "dryrun_candidates":
                decision_events += 1
                data = obj.get("data", {})
                for cand in data.get("candidates", []):
                    candidate_rows += 1
                    if cand.get("deny_reason") == "low_maker_fill_prob":
                        deny_low_fill += 1
                    fp = cand.get("maker_fill_prob")
                    if isinstance(fp, (int, float)):
                        fill_probs.append(float(fp))

    bins = {"eq0": 0, "lt05": 0, "mid": 0, "eq1": 0}
    for v in fill_probs:
        if v <= 1e-12:
            bins["eq0"] += 1
        elif v < 0.05:
            bins["lt05"] += 1
        elif v >= 1.0 - 1e-12:
            bins["eq1"] += 1
        else:
            bins["mid"] += 1

    fill_n = len(fill_probs)
    p50 = percentile(fill_probs, 50)
    p95 = percentile(fill_probs, 95)
    eq0_ratio = (bins["eq0"] / fill_n) if fill_n > 0 else None
    eq1_ratio = (bins["eq1"] / fill_n) if fill_n > 0 else None
    mid_ratio = (bins["mid"] / fill_n) if fill_n > 0 else None
    deny_low_ratio = (deny_low_fill / candidate_rows) if candidate_rows > 0 else 0.0

    return {
        "snapshot_events": snapshot_events,
        "decision_events": decision_events,
        "applied_actions": applied_actions,
        "candidate_rows": candidate_rows,
        "fill_n": fill_n,
        "p50": p50,
        "p95": p95,
        "bins": bins,
        "eq0_ratio": eq0_ratio,
        "eq1_ratio": eq1_ratio,
        "mid_ratio": mid_ratio,
        "deny_low_fill": deny_low_fill,
        "deny_low_ratio": deny_low_ratio,
    }


def score(metrics):
    fill_n = metrics["fill_n"]
    if fill_n <= 0:
        return None
    p50 = metrics["p50"] or 0.0
    mid_ratio = metrics["mid_ratio"] or 0.0
    eq0 = metrics["eq0_ratio"] or 0.0
    eq1 = metrics["eq1_ratio"] or 0.0
    extreme_ratio = eq0 + eq1
    p50_term = max(0.0, 1.0 - abs(p50 - 0.5) / 0.5)
    apply_term = min(metrics["applied_actions"], 2) / 2.0
    overshoot_penalty = max(0.0, extreme_ratio - 0.80)
    return 0.5 * mid_ratio + 0.3 * p50_term + 0.2 * apply_term - 0.5 * overshoot_penalty


def label(metrics):
    fill_n = metrics["fill_n"]
    if fill_n <= 0:
        return "control"
    p50 = metrics["p50"] or 0.0
    eq0 = metrics["eq0_ratio"] or 0.0
    eq1 = metrics["eq1_ratio"] or 0.0
    extreme = eq0 + eq1
    if 0.2 <= p50 <= 0.8 and extreme <= 0.8 and metrics["applied_actions"] >= 2:
        return "good"
    if metrics["applied_actions"] >= 1:
        return "watch"
    return "bad"


def fmt_num(v, digits=4):
    if v is None:
        return "NA"
    return f"{v:.{digits}f}"


def main():
    args = parse_args()
    runs = []
    for item in args.run:
        if "=" not in item:
            raise SystemExit(f"invalid --run format: {item}")
        name, raw_path = item.split("=", 1)
        name = name.strip()
        path = Path(raw_path.strip())
        if not path.is_file():
            raise SystemExit(f"file not found: {path}")
        metrics = load_metrics(path)
        runs.append((name, path, metrics))

    print("=== Estimator Matrix Summary ===")
    print(
        "name,fill_n,p50,p95,eq0_ratio,eq1_ratio,mid_ratio,deny_low_ratio,applied_actions,score,label,path"
    )
    for name, path, metrics in runs:
        sc = score(metrics)
        print(
            ",".join(
                [
                    name,
                    str(metrics["fill_n"]),
                    fmt_num(metrics["p50"]),
                    fmt_num(metrics["p95"]),
                    fmt_num(metrics["eq0_ratio"]),
                    fmt_num(metrics["eq1_ratio"]),
                    fmt_num(metrics["mid_ratio"]),
                    fmt_num(metrics["deny_low_ratio"]),
                    str(metrics["applied_actions"]),
                    fmt_num(sc),
                    label(metrics),
                    str(path),
                ]
            )
        )

    candidates = []
    for name, path, metrics in runs:
        sc = score(metrics)
        if sc is None:
            continue
        candidates.append((sc, name, path, metrics))

    if not candidates:
        print("CONCLUSION: no estimator-enabled run found (all fill_n == 0).")
        return

    candidates.sort(key=lambda x: x[0], reverse=True)
    best = candidates[0]
    _, best_name, best_path, best_metrics = best

    print("=== Conclusion ===")
    print(
        f"best_profile={best_name} score={fmt_num(best[0])} label={label(best_metrics)} path={best_path}"
    )
    print(
        "reason:"
        f" p50={fmt_num(best_metrics['p50'])},"
        f" p95={fmt_num(best_metrics['p95'])},"
        f" eq0_ratio={fmt_num(best_metrics['eq0_ratio'])},"
        f" eq1_ratio={fmt_num(best_metrics['eq1_ratio'])},"
        f" mid_ratio={fmt_num(best_metrics['mid_ratio'])},"
        f" applied_actions={best_metrics['applied_actions']},"
        f" low_fill_deny_ratio={fmt_num(best_metrics['deny_low_ratio'])}"
    )


if __name__ == "__main__":
    main()

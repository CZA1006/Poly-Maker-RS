#!/usr/bin/env python3
import argparse
import csv
import json
import math
import os
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

os.environ.setdefault("MPLBACKEND", "Agg")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate detailed multi-panel story plot for one window jsonl."
    )
    parser.add_argument("jsonl_path", help="Path to <symbol>_<window_ts>_<mode>.jsonl")
    parser.add_argument(
        "--out-dir", default=None, help="Output directory (default: jsonl parent)"
    )
    parser.add_argument("--plot-width", type=float, default=16.0)
    parser.add_argument("--plot-height", type=float, default=14.0)
    parser.add_argument(
        "--zoom-window-secs",
        type=float,
        default=180.0,
        help="Zoom panel window length in seconds (default: 180).",
    )
    parser.add_argument(
        "--max-zoom-annotations",
        type=int,
        default=18,
        help="Max sparse anti-overlap event annotations in zoom panel (default: 18).",
    )
    return parser.parse_args()


def parse_run_id_mode(path: Path):
    m = re.match(r"^(?P<run>\d{8}_\d{6})_(?P<mode>[^.]+)\.jsonl$", path.name)
    if m:
        return m.group("run"), m.group("mode")
    m = re.match(
        r"^(?P<symbol>[a-z0-9]+)_(?P<ts>\d{10})_(?P<mode>[^.]+)\.jsonl$",
        path.name,
    )
    if m:
        return f"{m.group('symbol')}_{m.group('ts')}", m.group("mode")
    return "run", "unknown"


def safe_float(value):
    try:
        if value is None:
            return None
        x = float(value)
        if not math.isfinite(x):
            return None
        return x
    except Exception:
        return None


def load_jsonl(path: Path) -> Iterable[dict]:
    with path.open("r", encoding="utf-8") as handle:
        for raw in handle:
            raw = raw.strip()
            if not raw:
                continue
            try:
                yield json.loads(raw)
            except Exception:
                continue


def infer_leg(data: dict, up_id: str, down_id: str):
    leg = data.get("leg")
    if isinstance(leg, str):
        leg = leg.upper()
        if leg in ("UP", "DOWN"):
            return leg
    token_id = data.get("token_id")
    if isinstance(token_id, str):
        if up_id and token_id == up_id:
            return "UP"
        if down_id and token_id == down_id:
            return "DOWN"
    return "UNKNOWN"


def value_bounds(values: Iterable[Optional[float]]) -> Tuple[float, float]:
    vals = [
        float(v)
        for v in values
        if isinstance(v, (int, float)) and math.isfinite(float(v))
    ]
    if not vals:
        return (0.0, 1.0)
    lo = min(vals)
    hi = max(vals)
    if abs(hi - lo) < 1e-9:
        delta = 0.5 if abs(hi) < 1.0 else abs(hi) * 0.1
        lo -= delta
        hi += delta
    pad = (hi - lo) * 0.08
    return (lo - pad, hi + pad)


def percentile(values: List[float], p: float) -> Optional[float]:
    if not values:
        return None
    xs = sorted(float(v) for v in values)
    if len(xs) == 1:
        return xs[0]
    k = (len(xs) - 1) * max(0.0, min(1.0, p))
    i = int(math.floor(k))
    j = int(math.ceil(k))
    if i == j:
        return xs[i]
    w = k - i
    return xs[i] * (1.0 - w) + xs[j] * w


def action_window(events: List[dict], zoom_secs: float, t_max: float) -> Tuple[float, float]:
    action_ts = [
        e["t_s"]
        for e in events
        if e.get("raw_type")
        in {
            "sim_order_open",
            "sim_order_filled_resting",
            "sim_order_filled",
            "dryrun_apply",
            "sim_order_cancel_requote",
            "sim_order_cancel_unrecoverable",
            "sim_order_cancel_timeout",
        }
    ]
    if not action_ts:
        return (0.0, min(max(zoom_secs, 60.0), max(t_max, zoom_secs)))
    action_ts.sort()
    best_start = action_ts[0]
    best_count = -1
    for ts in action_ts:
        end = ts + zoom_secs
        cnt = 0
        for t in action_ts:
            if ts <= t <= end:
                cnt += 1
        if cnt > best_count:
            best_count = cnt
            best_start = ts
    z0 = max(0.0, min(best_start, max(0.0, t_max - zoom_secs)))
    z1 = min(t_max, z0 + zoom_secs)
    if z1 <= z0:
        z1 = z0 + max(30.0, zoom_secs)
    return (z0, z1)


def parse_market_window_from_slug(slug: Optional[str]) -> Tuple[Optional[int], Optional[float]]:
    if not isinstance(slug, str) or not slug:
        return (None, None)
    m = re.search(r"-(\d+)m-(\d{10})$", slug)
    if m:
        mins = int(m.group(1))
        start_ts_ms = int(m.group(2)) * 1000
        return (start_ts_ms, float(max(1, mins) * 60))
    m2 = re.search(r"-(\d{10})$", slug)
    if m2:
        start_ts_ms = int(m2.group(1)) * 1000
        return (start_ts_ms, None)
    return (None, None)


def assign_fill_roles(events: List[dict]) -> Dict[int, str]:
    # Use dryrun_apply BUY fills as canonical inventory mutations.
    apply_fills = [
        e
        for e in events
        if e.get("raw_type") == "dryrun_apply"
        and e.get("event_type") == "filled"
        and str(e.get("side", "")).upper() == "BUY"
        and e.get("leg") in {"UP", "DOWN"}
        and isinstance(e.get("qty"), (int, float))
    ]
    fill_roles: Dict[int, str] = {}
    net_up_minus_down = 0.0
    for e in apply_fills:
        before = net_up_minus_down
        qty = float(e.get("qty") or 0.0)
        if e["leg"] == "UP":
            net_up_minus_down += qty
        elif e["leg"] == "DOWN":
            net_up_minus_down -= qty
        after = net_up_minus_down
        role = "accumulating"
        if abs(before) > 1e-9 and abs(after) < abs(before) - 1e-9:
            role = "balancing"
        fill_roles[id(e)] = role
    return fill_roles


def assign_ws_fill_roles(events: List[dict]) -> Dict[int, str]:
    ws_fills = [
        e
        for e in events
        if e.get("source") == "user_ws_order"
        and e.get("raw_type") in {"sim_order_filled_resting", "sim_order_filled"}
        and str(e.get("side", "")).upper() == "BUY"
        and e.get("leg") in {"UP", "DOWN"}
        and isinstance(e.get("qty"), (int, float))
    ]
    ws_fills.sort(key=lambda x: (x.get("ts_ms") or 0, x.get("decision_seq") or 0))
    fill_roles: Dict[int, str] = {}
    net_up_minus_down = 0.0
    for e in ws_fills:
        before = net_up_minus_down
        qty = float(e.get("qty") or 0.0)
        if e["leg"] == "UP":
            net_up_minus_down += qty
        elif e["leg"] == "DOWN":
            net_up_minus_down -= qty
        after = net_up_minus_down
        role = "accumulating"
        if abs(before) > 1e-9 and abs(after) < abs(before) - 1e-9:
            role = "balancing"
        fill_roles[id(e)] = role
    return fill_roles


def main():
    args = parse_args()
    input_path = Path(args.jsonl_path)
    if not input_path.is_file():
        print(f"error: file not found: {input_path}", file=sys.stderr)
        return 1

    run_id, mode = parse_run_id_mode(input_path)
    out_prefix = f"{run_id}_{mode}"
    out_dir = Path(args.out_dir) if args.out_dir else input_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    story_png = out_dir / f"{out_prefix}_story.png"
    story_events_csv = out_dir / f"{out_prefix}_story_events.csv"
    story_series_csv = out_dir / f"{out_prefix}_story_series.csv"
    story_md = out_dir / f"{out_prefix}_story.md"

    snapshots: List[dict] = []
    events: List[dict] = []
    up_id = ""
    down_id = ""
    market_slug: Optional[str] = None

    for obj in load_jsonl(input_path):
        kind = obj.get("kind")
        ts_ms = obj.get("ts_ms")
        if not isinstance(ts_ms, (int, float)):
            continue
        data = obj.get("data") if isinstance(obj.get("data"), dict) else {}

        if kind == "dryrun_snapshot":
            if not up_id and isinstance(data.get("up_id"), str):
                up_id = data["up_id"]
            if not down_id and isinstance(data.get("down_id"), str):
                down_id = data["down_id"]
            if market_slug is None and isinstance(data.get("market_slug"), str):
                market_slug = data.get("market_slug")

            snapshots.append(
                {
                    "ts_ms": int(ts_ms),
                    "best_bid_up": safe_float(data.get("best_bid_up")),
                    "best_ask_up": safe_float(data.get("best_ask_up")),
                    "best_bid_down": safe_float(data.get("best_bid_down")),
                    "best_ask_down": safe_float(data.get("best_ask_down")),
                    "pair_cost": safe_float(data.get("pair_cost")),
                    "avg_up": safe_float(data.get("avg_up")),
                    "avg_down": safe_float(data.get("avg_down")),
                    "qty_up": safe_float(data.get("qty_up")) or 0.0,
                    "qty_down": safe_float(data.get("qty_down")) or 0.0,
                    "cost_up": safe_float(data.get("cost_up")) or 0.0,
                    "cost_down": safe_float(data.get("cost_down")) or 0.0,
                    "entry_edge_bps": safe_float(data.get("round_plan_entry_edge_bps")),
                    "entry_regime_score": safe_float(
                        data.get("round_plan_entry_regime_score")
                    ),
                    "slice_count_planned": safe_float(
                        data.get("round_plan_slice_count_planned")
                    ),
                    "slice_qty_current": safe_float(
                        data.get("round_plan_slice_qty_current")
                    ),
                    "round_state": data.get("round_state"),
                }
            )

        elif kind == "dryrun_apply":
            applied = (
                data.get("applied_action")
                if isinstance(data.get("applied_action"), dict)
                else {}
            )
            leg = infer_leg(applied, up_id, down_id)
            events.append(
                {
                    "ts_ms": int(ts_ms),
                    "event_type": "filled",
                    "raw_type": "dryrun_apply",
                    "source": "dryrun_apply",
                    "leg": leg,
                    "side": applied.get("side"),
                    "price": safe_float(applied.get("fill_price")),
                    "qty": safe_float(applied.get("qty")),
                    "decision_seq": data.get("decision_seq"),
                    "round_idx": data.get("after_round_idx"),
                    "order_id": applied.get("order_id"),
                    "cancel_reason_detail": None,
                    "required_opp_avg_price_cap": safe_float(
                        applied.get("required_opp_avg_price_cap")
                    ),
                    "current_opp_best_ask": safe_float(applied.get("current_opp_best_ask")),
                    "required_hedge_qty": safe_float(applied.get("required_hedge_qty")),
                    "hedge_margin_to_opp_ask": safe_float(
                        applied.get("hedge_margin_to_opp_ask")
                    ),
                    "hedge_margin_required": safe_float(applied.get("hedge_margin_required")),
                    "hedge_margin_ok": applied.get("hedge_margin_ok"),
                    "entry_quote_base_postonly_price": safe_float(
                        applied.get("entry_quote_base_postonly_price")
                    ),
                    "entry_quote_dynamic_cap_price": safe_float(
                        applied.get("entry_quote_dynamic_cap_price")
                    ),
                    "entry_quote_final_price": safe_float(
                        applied.get("entry_quote_final_price")
                    ),
                    "entry_quote_cap_active": applied.get("entry_quote_cap_active"),
                    "entry_quote_cap_bind": applied.get("entry_quote_cap_bind"),
                }
            )

        elif kind == "user_ws_order":
            raw_type = data.get("raw_type")
            if not isinstance(raw_type, str) or not raw_type.startswith("sim_order_"):
                continue
            leg = infer_leg(data, up_id, down_id)
            if raw_type in ("sim_order_filled_resting", "sim_order_filled"):
                event_type = "filled"
            elif "cancel" in raw_type:
                event_type = "cancel"
            elif raw_type == "sim_order_open":
                event_type = "open"
            else:
                event_type = "other"
            events.append(
                {
                    "ts_ms": int(ts_ms),
                    "event_type": event_type,
                    "raw_type": raw_type,
                    "source": "user_ws_order",
                    "leg": leg,
                    "side": data.get("side"),
                    "price": safe_float(data.get("price")),
                    "qty": safe_float(
                        data.get("qty")
                        if data.get("qty") is not None
                        else data.get("size")
                    ),
                    "decision_seq": data.get("decision_seq"),
                    "round_idx": data.get("round_idx"),
                    "order_id": data.get("order_id"),
                    "cancel_reason_detail": data.get("cancel_reason_detail"),
                    "required_opp_avg_price_cap": safe_float(
                        data.get("required_opp_avg_price_cap")
                    ),
                    "current_opp_best_ask": safe_float(data.get("current_opp_best_ask")),
                    "required_hedge_qty": safe_float(data.get("required_hedge_qty")),
                    "hedge_margin_to_opp_ask": safe_float(
                        data.get("hedge_margin_to_opp_ask")
                    ),
                    "hedge_margin_required": safe_float(data.get("hedge_margin_required")),
                    "hedge_margin_ok": data.get("hedge_margin_ok"),
                    "entry_quote_base_postonly_price": safe_float(
                        data.get("entry_quote_base_postonly_price")
                    ),
                    "entry_quote_dynamic_cap_price": safe_float(
                        data.get("entry_quote_dynamic_cap_price")
                    ),
                    "entry_quote_final_price": safe_float(
                        data.get("entry_quote_final_price")
                    ),
                    "entry_quote_cap_active": data.get("entry_quote_cap_active"),
                    "entry_quote_cap_bind": data.get("entry_quote_cap_bind"),
                }
            )

    snapshots.sort(key=lambda x: x["ts_ms"])
    events.sort(key=lambda x: x["ts_ms"])

    if not snapshots and not events:
        print(f"error: no usable rows found in {input_path}", file=sys.stderr)
        return 1

    ts0_candidates = []
    if snapshots:
        ts0_candidates.append(snapshots[0]["ts_ms"])
    if events:
        ts0_candidates.append(events[0]["ts_ms"])
    fallback_ts0 = min(ts0_candidates)
    slug_start_ts_ms, slug_window_secs = parse_market_window_from_slug(market_slug)
    ts0 = slug_start_ts_ms if slug_start_ts_ms is not None else fallback_ts0

    series_rows: List[dict] = []
    for s in snapshots:
        mid_up = None
        if s["best_bid_up"] is not None and s["best_ask_up"] is not None:
            mid_up = (s["best_bid_up"] + s["best_ask_up"]) / 2.0
        mid_down = None
        if s["best_bid_down"] is not None and s["best_ask_down"] is not None:
            mid_down = (s["best_bid_down"] + s["best_ask_down"]) / 2.0

        pair_cost = s["pair_cost"]
        if pair_cost is None and s["avg_up"] is not None and s["avg_down"] is not None:
            pair_cost = s["avg_up"] + s["avg_down"]

        series_rows.append(
            {
                "ts_ms": s["ts_ms"],
                "t_s": (s["ts_ms"] - ts0) / 1000.0,
                "best_bid_up": s["best_bid_up"],
                "best_ask_up": s["best_ask_up"],
                "best_bid_down": s["best_bid_down"],
                "best_ask_down": s["best_ask_down"],
                "mid_up": mid_up,
                "mid_down": mid_down,
                "pair_cost": pair_cost,
                "qty_up": s["qty_up"],
                "qty_down": s["qty_down"],
                "qty_total": s["qty_up"] + s["qty_down"],
                "abs_net": abs(s["qty_up"] - s["qty_down"]),
                "cost_up": s["cost_up"],
                "cost_down": s["cost_down"],
                "cost_total": s["cost_up"] + s["cost_down"],
                "entry_edge_bps": s["entry_edge_bps"],
                "entry_regime_score": s["entry_regime_score"],
                "slice_count_planned": s["slice_count_planned"],
                "slice_qty_current": s["slice_qty_current"],
                "round_state": s.get("round_state"),
            }
        )

    cum_buy_up_shares = 0.0
    cum_buy_down_shares = 0.0
    cum_buy_up_usdc = 0.0
    cum_buy_down_usdc = 0.0
    for e in events:
        e["t_s"] = (e["ts_ms"] - ts0) / 1000.0
        if e["event_type"] == "filled" and str(e.get("side", "")).upper() == "BUY":
            qty = float(e.get("qty") or 0.0)
            px = float(e.get("price") or 0.0)
            if e["leg"] == "UP":
                cum_buy_up_shares += qty
                cum_buy_up_usdc += qty * px
            elif e["leg"] == "DOWN":
                cum_buy_down_shares += qty
                cum_buy_down_usdc += qty * px
        e["cum_buy_up_shares"] = cum_buy_up_shares
        e["cum_buy_down_shares"] = cum_buy_down_shares
        e["cum_buy_up_usdc"] = cum_buy_up_usdc
        e["cum_buy_down_usdc"] = cum_buy_down_usdc

    fill_roles = assign_fill_roles(events)
    for e in events:
        e["fill_role"] = fill_roles.get(id(e), "")

    ws_fill_roles = assign_ws_fill_roles(events)
    for e in events:
        if e.get("raw_type") in {"sim_order_filled_resting", "sim_order_filled"} and not e.get(
            "fill_role"
        ):
            e["fill_role"] = ws_fill_roles.get(id(e), "")

    # Propagate fill_role from dryrun_apply to ws fill rows via order_id.
    order_fill_role: Dict[str, str] = {}
    for e in events:
        if e.get("raw_type") != "dryrun_apply":
            continue
        oid = e.get("order_id")
        role = e.get("fill_role")
        if not isinstance(oid, str) or not oid:
            continue
        if role == "balancing":
            order_fill_role[oid] = "balancing"
        elif role == "accumulating" and oid not in order_fill_role:
            order_fill_role[oid] = "accumulating"
    for e in events:
        if e.get("raw_type") not in {"sim_order_filled_resting", "sim_order_filled"}:
            continue
        oid = e.get("order_id")
        if not isinstance(oid, str) or not oid:
            continue
        if not e.get("fill_role"):
            e["fill_role"] = order_fill_role.get(oid, "")

    # Order quality metrics from ws lifecycle.
    order_quality: Dict[str, dict] = {}
    for e in events:
        if e.get("source") != "user_ws_order":
            continue
        oid = e.get("order_id")
        t = e.get("t_s")
        raw = str(e.get("raw_type") or "")
        if not isinstance(oid, str) or not oid or not isinstance(t, (int, float)):
            continue
        rec = order_quality.setdefault(
            oid,
            {
                "open_t": None,
                "first_fill_t": None,
                "terminal_t": None,
                "terminal_type": None,
            },
        )
        t = float(t)
        if raw == "sim_order_open":
            if rec["open_t"] is None or t < rec["open_t"]:
                rec["open_t"] = t
        if raw in {"sim_order_filled_resting", "sim_order_filled"}:
            if rec["first_fill_t"] is None or t < rec["first_fill_t"]:
                rec["first_fill_t"] = t
            if rec["terminal_t"] is None or t >= rec["terminal_t"]:
                rec["terminal_t"] = t
                rec["terminal_type"] = "fill"
        if raw in {
            "sim_order_cancel_requote",
            "sim_order_cancel_unrecoverable",
            "sim_order_cancel_timeout",
            "sim_order_cancel_risk_guard",
        }:
            if rec["terminal_t"] is None or t >= rec["terminal_t"]:
                rec["terminal_t"] = t
                rec["terminal_type"] = raw.replace("sim_order_", "")

    # Write events CSV.
    with story_events_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "ts_ms",
                "t_s",
                "source",
                "event_type",
                "raw_type",
                "fill_role",
                "leg",
                "side",
                "price",
                "qty",
                "order_id",
                "decision_seq",
                "round_idx",
                "cancel_reason_detail",
                "required_opp_avg_price_cap",
                "current_opp_best_ask",
                "required_hedge_qty",
                "hedge_margin_to_opp_ask",
                "hedge_margin_required",
                "hedge_margin_ok",
                "entry_quote_base_postonly_price",
                "entry_quote_dynamic_cap_price",
                "entry_quote_final_price",
                "entry_quote_cap_active",
                "entry_quote_cap_bind",
                "cum_buy_up_shares",
                "cum_buy_down_shares",
                "cum_buy_up_usdc",
                "cum_buy_down_usdc",
            ]
        )
        for e in events:
            w.writerow(
                [
                    e["ts_ms"],
                    e["t_s"],
                    e.get("source"),
                    e["event_type"],
                    e["raw_type"],
                    e.get("fill_role"),
                    e["leg"],
                    e["side"],
                    e["price"],
                    e["qty"],
                    e.get("order_id"),
                    e["decision_seq"],
                    e["round_idx"],
                    e["cancel_reason_detail"],
                    e["required_opp_avg_price_cap"],
                    e["current_opp_best_ask"],
                    e["required_hedge_qty"],
                    e["hedge_margin_to_opp_ask"],
                    e["hedge_margin_required"],
                    e["hedge_margin_ok"],
                    e["entry_quote_base_postonly_price"],
                    e["entry_quote_dynamic_cap_price"],
                    e["entry_quote_final_price"],
                    e["entry_quote_cap_active"],
                    e["entry_quote_cap_bind"],
                    e["cum_buy_up_shares"],
                    e["cum_buy_down_shares"],
                    e["cum_buy_up_usdc"],
                    e["cum_buy_down_usdc"],
                ]
            )

    # Write series CSV.
    with story_series_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "ts_ms",
                "t_s",
                "best_bid_up",
                "best_ask_up",
                "best_bid_down",
                "best_ask_down",
                "mid_up",
                "mid_down",
                "pair_cost",
                "qty_up",
                "qty_down",
                "qty_total",
                "abs_net",
                "cost_up",
                "cost_down",
                "cost_total",
                "entry_edge_bps",
                "entry_regime_score",
                "slice_count_planned",
                "slice_qty_current",
                "round_state",
            ]
        )
        for r in series_rows:
            w.writerow(
                [
                    r["ts_ms"],
                    r["t_s"],
                    r["best_bid_up"],
                    r["best_ask_up"],
                    r["best_bid_down"],
                    r["best_ask_down"],
                    r["mid_up"],
                    r["mid_down"],
                    r["pair_cost"],
                    r["qty_up"],
                    r["qty_down"],
                    r["qty_total"],
                    r["abs_net"],
                    r["cost_up"],
                    r["cost_down"],
                    r["cost_total"],
                    r["entry_edge_bps"],
                    r["entry_regime_score"],
                    r["slice_count_planned"],
                    r["slice_qty_current"],
                    r["round_state"],
                ]
            )

    order_counts = Counter(
        e["raw_type"] for e in events if isinstance(e.get("raw_type"), str)
    )
    open_count = order_counts.get("sim_order_open", 0)
    fill_count_resting = order_counts.get("sim_order_filled_resting", 0) + order_counts.get(
        "sim_order_filled", 0
    )
    fill_count_dryrun_apply = sum(
        1
        for e in events
        if e["event_type"] == "filled" and e["raw_type"] == "dryrun_apply"
    )
    fill_count_used = fill_count_resting if fill_count_resting > 0 else fill_count_dryrun_apply
    fill_rate = (fill_count_used / open_count) if open_count else None
    ws_fill_accumulating_count = sum(
        1
        for e in events
        if e.get("source") == "user_ws_order"
        and e.get("raw_type") in {"sim_order_filled_resting", "sim_order_filled"}
        and e.get("fill_role") == "accumulating"
    )
    ws_fill_balancing_count = sum(
        1
        for e in events
        if e.get("source") == "user_ws_order"
        and e.get("raw_type") in {"sim_order_filled_resting", "sim_order_filled"}
        and e.get("fill_role") == "balancing"
    )

    open_to_fill_latencies = []
    open_to_terminal_latencies = []
    orders_with_open = 0
    orders_with_first_fill = 0
    for rec in order_quality.values():
        o = rec.get("open_t")
        f = rec.get("first_fill_t")
        term = rec.get("terminal_t")
        if isinstance(o, (int, float)):
            orders_with_open += 1
            if isinstance(f, (int, float)) and f >= o:
                orders_with_first_fill += 1
                open_to_fill_latencies.append(float(f - o))
            if isinstance(term, (int, float)) and term >= o:
                open_to_terminal_latencies.append(float(term - o))
    median_open_to_fill_latency_s = percentile(open_to_fill_latencies, 0.5)
    p90_open_to_fill_latency_s = percentile(open_to_fill_latencies, 0.9)
    median_open_to_terminal_latency_s = percentile(open_to_terminal_latencies, 0.5)

    final = series_rows[-1] if series_rows else None
    final_pair_cost = final["pair_cost"] if final else None
    final_qty_up = final["qty_up"] if final else 0.0
    final_qty_down = final["qty_down"] if final else 0.0
    final_abs_net = abs((final_qty_up or 0.0) - (final_qty_down or 0.0))
    final_cost_up = final["cost_up"] if final else 0.0
    final_cost_down = final["cost_down"] if final else 0.0
    hedgeable = min(final_qty_up or 0.0, final_qty_down or 0.0)
    unmatched_cost = max(
        (final_cost_up or 0.0)
        - hedgeable
        * (
            (final_cost_up or 0.0) / (final_qty_up or 1.0)
            if (final_qty_up or 0.0) > 0
            else 0.0
        ),
        0.0,
    ) + max(
        (final_cost_down or 0.0)
        - hedgeable
        * (
            (final_cost_down or 0.0) / (final_qty_down or 1.0)
            if (final_qty_down or 0.0) > 0
            else 0.0
        ),
        0.0,
    )
    window_pnl = None
    if final_pair_cost is not None:
        window_pnl = (1.0 - final_pair_cost) * hedgeable - unmatched_cost

    t_vals = [r["t_s"] for r in series_rows] + [e["t_s"] for e in events] + [1.0]
    t_min = min(t_vals)
    t_max = max(t_vals)
    global_x_min = min(0.0, t_min)
    global_x_max = t_max
    if isinstance(slug_window_secs, (int, float)) and math.isfinite(float(slug_window_secs)):
        global_x_max = max(global_x_max, float(slug_window_secs))
        global_x_min = 0.0

    zoom_start, zoom_end = action_window(
        events,
        zoom_secs=max(30.0, float(args.zoom_window_secs)),
        t_max=t_max,
    )

    # Plot with matplotlib.
    try:
        import matplotlib.pyplot as plt  # type: ignore
        from matplotlib.gridspec import GridSpec  # type: ignore

        up_color = "#1b9e77"
        down_color = "#7b1fa2"
        pair_color = "#1565c0"
        cancel_color = "#111111"
        balance_color = "#ff9800"
        first_leg_fill_color = "#1e88e5"
        balancing_fill_color = "#fb8c00"
        unknown_fill_color = "#90a4ae"

        fig = plt.figure(figsize=(args.plot_width, args.plot_height), constrained_layout=True)
        gs = GridSpec(3, 2, figure=fig, height_ratios=[1.45, 1.0, 1.0])

        # Panel A: best ask + order lifecycle markers.
        ax_a = fig.add_subplot(gs[0, :])
        ax_a.set_title(
            "Market Best Ask (UP/DOWN) + Bot Order Lifecycle (open/fill/cancel) + balancing fills"
        )
        if series_rows:
            ax_a.plot(
                [r["t_s"] for r in series_rows],
                [r["best_ask_up"] for r in series_rows],
                color=up_color,
                linewidth=1.7,
                label="best_ask_up",
            )
            ax_a.plot(
                [r["t_s"] for r in series_rows],
                [r["best_ask_down"] for r in series_rows],
                color=down_color,
                linewidth=1.7,
                label="best_ask_down",
            )

        def scatter_events(
            ax,
            cond,
            marker,
            color,
            label,
            size_base=25.0,
            edge="none",
            alpha=0.9,
        ):
            xs = []
            ys = []
            ss = []
            for e in events:
                if not cond(e):
                    continue
                px = e.get("price")
                if not isinstance(px, (int, float)):
                    continue
                qty = float(e.get("qty") or 0.0)
                xs.append(e["t_s"])
                ys.append(float(px))
                ss.append(size_base + max(0.0, qty) * 14.0)
            if xs:
                kwargs = {}
                if edge != "none":
                    kwargs["edgecolors"] = edge
                    kwargs["linewidths"] = 1.0
                ax.scatter(
                    xs,
                    ys,
                    s=ss,
                    marker=marker,
                    c=color,
                    alpha=alpha,
                    label=label,
                    zorder=4,
                    **kwargs,
                )

        scatter_events(
            ax_a,
            lambda e: e.get("raw_type") == "sim_order_open" and e.get("leg") == "UP",
            marker="^",
            color=up_color,
            label="open_up (size~qty)",
        )
        scatter_events(
            ax_a,
            lambda e: e.get("raw_type") == "sim_order_open" and e.get("leg") == "DOWN",
            marker="v",
            color=down_color,
            label="open_down (size~qty)",
        )
        scatter_events(
            ax_a,
            lambda e: e.get("raw_type") in {"sim_order_filled_resting", "sim_order_filled"}
            and e.get("leg") == "UP",
            marker="o",
            color="none",
            edge=up_color,
            label="filled_up_ws",
            alpha=1.0,
        )
        scatter_events(
            ax_a,
            lambda e: e.get("raw_type") in {"sim_order_filled_resting", "sim_order_filled"}
            and e.get("leg") == "DOWN",
            marker="o",
            color="none",
            edge=down_color,
            label="filled_down_ws",
            alpha=1.0,
        )
        scatter_events(
            ax_a,
            lambda e: e.get("event_type") == "cancel",
            marker="x",
            color=cancel_color,
            label="cancel_ws",
            size_base=28.0,
        )
        scatter_events(
            ax_a,
            lambda e: e.get("raw_type") == "dryrun_apply"
            and e.get("fill_role") == "balancing",
            marker="*",
            color=balance_color,
            label="balancing_fill_apply",
            size_base=42.0,
        )

        ax_a.axvspan(zoom_start, zoom_end, color="#eceff1", alpha=0.35, label="zoom_window")
        ax_a.grid(alpha=0.25)
        ax_a.set_ylabel("Price")
        ax_a.legend(loc="upper left", ncol=3, fontsize=8)
        ax_a.set_xlim(global_x_min, global_x_max)

        # Panel B: inventory shares + abs_net.
        ax_b = fig.add_subplot(gs[1, 0], sharex=ax_a)
        ax_b.set_title("Inventory Shares: qty_up / qty_down and abs_net")
        if series_rows:
            xs = [r["t_s"] for r in series_rows]
            ax_b.plot(xs, [r["qty_up"] for r in series_rows], color=up_color, label="qty_up")
            ax_b.plot(
                xs, [r["qty_down"] for r in series_rows], color=down_color, label="qty_down"
            )
            ax_b2 = ax_b.twinx()
            ax_b2.plot(
                xs,
                [r["abs_net"] for r in series_rows],
                color="#e53935",
                linestyle="--",
                label="abs_net",
            )
            lines, labels = ax_b.get_legend_handles_labels()
            lines2, labels2 = ax_b2.get_legend_handles_labels()
            ax_b.legend(lines + lines2, labels + labels2, loc="upper left", fontsize=8)
        ax_b.grid(alpha=0.25)
        ax_b.set_ylabel("Shares")

        # Panel C: cost basis + pair cost.
        ax_c = fig.add_subplot(gs[1, 1], sharex=ax_a)
        ax_c.set_title("Cost Basis and Pair Cost")
        if series_rows:
            xs = [r["t_s"] for r in series_rows]
            ax_c.plot(xs, [r["cost_up"] for r in series_rows], color=up_color, label="cost_up")
            ax_c.plot(
                xs,
                [r["cost_down"] for r in series_rows],
                color=down_color,
                label="cost_down",
            )
            ax_c.plot(
                xs,
                [r["cost_total"] for r in series_rows],
                color="#37474f",
                label="cost_total",
                linewidth=1.6,
            )
            ax_c2 = ax_c.twinx()
            ax_c2.plot(
                xs,
                [r["pair_cost"] for r in series_rows],
                color=pair_color,
                linestyle="--",
                label="pair_cost",
            )
            ax_c2.axhline(1.0, color="#607d8b", linestyle=":", linewidth=1.0)
            lines, labels = ax_c.get_legend_handles_labels()
            lines2, labels2 = ax_c2.get_legend_handles_labels()
            ax_c.legend(lines + lines2, labels + labels2, loc="upper left", fontsize=8)
        ax_c.grid(alpha=0.25)
        ax_c.set_ylabel("USDC")

        # Panel D: event timeline (categorical y).
        ax_d = fig.add_subplot(gs[2, 0], sharex=ax_a)
        ax_d.set_title("Order Timeline (categorical)")
        y_map = {
            "sim_order_open": 5.0,
            "sim_order_filled_resting": 4.0,
            "sim_order_filled": 4.0,
            "dryrun_apply": 3.2,
            "sim_order_cancel_requote": 2.3,
            "sim_order_cancel_unrecoverable": 1.8,
            "sim_order_cancel_timeout": 1.3,
            "sim_order_cancel_risk_guard": 0.9,
            "sim_order_timeout_extend": 0.4,
        }
        for e in events:
            raw = str(e.get("raw_type") or "")
            if raw not in y_map:
                continue
            t = e.get("t_s")
            if not isinstance(t, (int, float)):
                continue
            y = y_map[raw]
            leg = e.get("leg")
            color = up_color if leg == "UP" else down_color if leg == "DOWN" else "#546e7a"
            qty = float(e.get("qty") or 0.0)
            marker = "o"
            if raw == "sim_order_open":
                marker = "^" if leg == "UP" else "v" if leg == "DOWN" else "o"
            elif raw in {"sim_order_cancel_requote", "sim_order_cancel_unrecoverable", "sim_order_cancel_timeout", "sim_order_cancel_risk_guard"}:
                marker = "x"
            elif raw == "sim_order_timeout_extend":
                marker = "+"
            elif raw == "dryrun_apply":
                marker = "D"
            ax_d.scatter(
                [t],
                [y],
                s=[22.0 + qty * 10.0],
                marker=marker,
                c=[color],
                alpha=0.9,
                linewidths=1.0,
            )
        ax_d.set_yticks(list(y_map.values()))
        ax_d.set_yticklabels(list(y_map.keys()), fontsize=8)
        ax_d.grid(alpha=0.25)
        ax_d.set_xlabel("Seconds from window start")

        # Panel E: local zoom (best ask + actions + per-order lifecycle lanes).
        ax_e = fig.add_subplot(gs[2, 1])
        ax_e.set_title(
            f"Zoom [{zoom_start:.1f}s, {zoom_end:.1f}s]: local market + actions + order lifecycle"
        )

        zoom_series = [r for r in series_rows if zoom_start <= r["t_s"] <= zoom_end]
        zoom_events = [e for e in events if zoom_start <= e["t_s"] <= zoom_end]
        if zoom_series:
            ax_e.plot(
                [r["t_s"] for r in zoom_series],
                [r["best_ask_up"] for r in zoom_series],
                color=up_color,
                linewidth=1.4,
                label="best_ask_up",
            )
            ax_e.plot(
                [r["t_s"] for r in zoom_series],
                [r["best_ask_down"] for r in zoom_series],
                color=down_color,
                linewidth=1.4,
                label="best_ask_down",
            )

        # Overlay action markers on ask curves.
        for e in zoom_events:
            raw = str(e.get("raw_type") or "")
            if raw not in {
                "sim_order_open",
                "sim_order_filled_resting",
                "sim_order_filled",
                "dryrun_apply",
            }:
                continue
            px = e.get("price")
            if not isinstance(px, (int, float)):
                continue
            leg = e.get("leg")
            color = up_color if leg == "UP" else down_color if leg == "DOWN" else "#455a64"
            marker = "o"
            if raw == "sim_order_open":
                marker = "^" if leg == "UP" else "v" if leg == "DOWN" else "o"
            elif raw == "dryrun_apply":
                marker = "*" if e.get("fill_role") == "balancing" else "D"
            qty = float(e.get("qty") or 0.0)
            ax_e.scatter(
                [e["t_s"]],
                [float(px)],
                c=[color],
                marker=marker,
                s=[26 + qty * 12],
                alpha=0.9,
                zorder=4,
            )

        # Build local order lifecycle swimlanes by order_id.
        order_lifecycle: Dict[str, dict] = {}
        for e in zoom_events:
            oid = e.get("order_id")
            if not isinstance(oid, str) or not oid:
                continue
            rec = order_lifecycle.setdefault(
                oid,
                {
                    "leg": e.get("leg"),
                    "open": None,
                    "fills": [],
                    "cancel": None,
                    "first_t": e.get("t_s"),
                    "last_t": e.get("t_s"),
                },
            )
            t = e.get("t_s")
            if isinstance(t, (int, float)):
                if not isinstance(rec.get("first_t"), (int, float)) or t < rec["first_t"]:
                    rec["first_t"] = t
                if not isinstance(rec.get("last_t"), (int, float)) or t > rec["last_t"]:
                    rec["last_t"] = t
            leg = e.get("leg")
            if rec.get("leg") not in {"UP", "DOWN"} and leg in {"UP", "DOWN"}:
                rec["leg"] = leg
            raw = str(e.get("raw_type") or "")
            px = e.get("price")
            qty = e.get("qty")
            if raw == "sim_order_open":
                rec["open"] = (t, px, qty)
            elif raw in {"sim_order_filled_resting", "sim_order_filled"}:
                rec["fills"].append((t, px, qty, e.get("fill_role"), raw))
            elif "cancel" in raw:
                rec["cancel"] = (t, px, qty, raw)

        lifecycle_rows = sorted(
            order_lifecycle.items(),
            key=lambda kv: (
                kv[1]["open"][0]
                if isinstance(kv[1].get("open"), tuple)
                and isinstance(kv[1]["open"][0], (int, float))
                else (kv[1].get("first_t") if isinstance(kv[1].get("first_t"), (int, float)) else 0.0)
            ),
        )
        max_lanes = 12
        lifecycle_rows = lifecycle_rows[:max_lanes]

        ax_e_lane = ax_e.twinx()
        if lifecycle_rows:
            ax_e_lane.set_ylim(0.0, float(len(lifecycle_rows) + 1))
        else:
            ax_e_lane.set_ylim(0.0, 1.0)
        ax_e_lane.set_yticks([])
        ax_e_lane.set_ylabel("order lifecycle lanes", fontsize=8, color="#546e7a")

        for idx, (oid, rec) in enumerate(lifecycle_rows, 1):
            leg = rec.get("leg")
            color = up_color if leg == "UP" else down_color if leg == "DOWN" else "#546e7a"
            open_evt = rec.get("open")
            fills_evt = rec.get("fills") or []
            cancel_evt = rec.get("cancel")

            t_start = None
            if isinstance(open_evt, tuple) and isinstance(open_evt[0], (int, float)):
                t_start = float(open_evt[0])
            elif isinstance(rec.get("first_t"), (int, float)):
                t_start = float(rec["first_t"])
            t_end = t_start
            if fills_evt:
                valid_fill_t = [float(x[0]) for x in fills_evt if isinstance(x[0], (int, float))]
                if valid_fill_t:
                    t_end = max(t_end if isinstance(t_end, float) else valid_fill_t[0], max(valid_fill_t))
            if isinstance(cancel_evt, tuple) and isinstance(cancel_evt[0], (int, float)):
                t_end = max(t_end if isinstance(t_end, float) else float(cancel_evt[0]), float(cancel_evt[0]))
            if t_start is None:
                continue
            if t_end is None:
                t_end = t_start

            ax_e_lane.hlines(y=float(idx), xmin=t_start, xmax=t_end, colors=color, linewidth=1.2, alpha=0.7, zorder=2)
            if isinstance(open_evt, tuple) and isinstance(open_evt[0], (int, float)):
                m = "^" if leg == "UP" else "v" if leg == "DOWN" else "o"
                ax_e_lane.scatter([float(open_evt[0])], [float(idx)], marker=m, c=[color], s=[28], alpha=0.95, zorder=3)
            has_balancing_fill = False
            has_accum_fill = False
            for f_evt in fills_evt:
                if isinstance(f_evt[0], (int, float)):
                    role = f_evt[3] if len(f_evt) > 3 else ""
                    if role == "balancing":
                        c_fill = balancing_fill_color
                        has_balancing_fill = True
                    elif role == "accumulating":
                        c_fill = first_leg_fill_color
                        has_accum_fill = True
                    else:
                        c_fill = unknown_fill_color
                    ax_e_lane.scatter(
                        [float(f_evt[0])],
                        [float(idx)],
                        marker="o",
                        c=[c_fill],
                        s=[20],
                        alpha=0.95,
                        zorder=3,
                    )
            if isinstance(cancel_evt, tuple) and isinstance(cancel_evt[0], (int, float)):
                ax_e_lane.scatter([float(cancel_evt[0])], [float(idx)], marker="x", c=["#111111"], s=[24], alpha=0.95, zorder=3)
            # lane header + terminal/delay quality summary
            ax_e_lane.text(
                t_start,
                float(idx) + 0.12,
                f"{oid[-6:]}",
                fontsize=6.5,
                color="#263238",
                alpha=0.85,
            )
            terminal_t = t_start
            terminal_kind = "open_only"
            if fills_evt:
                valid_fill_t = [float(x[0]) for x in fills_evt if isinstance(x[0], (int, float))]
                if valid_fill_t:
                    t_last_fill = max(valid_fill_t)
                    terminal_t = t_last_fill
                    terminal_kind = "fill_bal" if has_balancing_fill else ("fill_first" if has_accum_fill else "fill")
            if isinstance(cancel_evt, tuple) and isinstance(cancel_evt[0], (int, float)):
                t_cancel = float(cancel_evt[0])
                if t_cancel >= terminal_t:
                    terminal_t = t_cancel
                    terminal_kind = str(cancel_evt[3]).replace("sim_order_", "")
            delay_s = max(0.0, float(terminal_t) - float(t_start))
            ax_e_lane.text(
                min(float(terminal_t) + 0.15, zoom_end - 0.02 * max(1.0, zoom_end - zoom_start)),
                float(idx) - 0.18,
                f"{terminal_kind} {delay_s:.1f}s",
                fontsize=6.2,
                color="#37474f",
                alpha=0.92,
            )

        # Sparse anti-overlap labels for key events only.
        annotate_budget = max(0, int(args.max_zoom_annotations))
        ann_count = 0
        candidates = []
        for e in zoom_events:
            raw = str(e.get("raw_type") or "")
            if raw not in {"sim_order_open", "dryrun_apply", "sim_order_cancel_requote"}:
                continue
            px = e.get("price")
            t = e.get("t_s")
            if not isinstance(px, (int, float)) or not isinstance(t, (int, float)):
                continue
            prio = 3 if raw == "dryrun_apply" and e.get("fill_role") == "balancing" else 2 if raw == "sim_order_open" else 1
            candidates.append((prio, e))
        candidates.sort(key=lambda x: (-x[0], x[1]["t_s"]))

        y_min, y_max = value_bounds(
            [r.get("best_ask_up") for r in zoom_series] + [r.get("best_ask_down") for r in zoom_series]
        )
        min_dx = max(1.5, (zoom_end - zoom_start) * 0.035)
        min_dy = max(0.001, (y_max - y_min) * 0.05)
        placed = []
        offsets = [(4, 6), (4, 16), (4, -8), (4, 26), (-24, 8), (-24, -8)]
        for _, e in candidates:
            if ann_count >= annotate_budget:
                break
            t = float(e["t_s"])
            px = float(e["price"])
            collide = False
            for pt, py in placed:
                if abs(t - pt) <= min_dx and abs(px - py) <= min_dy:
                    collide = True
                    break
            if collide:
                continue
            raw = str(e.get("raw_type") or "")
            leg = e.get("leg")
            qty = float(e.get("qty") or 0.0)
            short = raw.replace("sim_order_", "").replace("dryrun_", "d_")
            leg_short = "U" if leg == "UP" else "D" if leg == "DOWN" else "?"
            role = "/bal" if e.get("fill_role") == "balancing" else ""
            tag = f"{short}{role}/{leg_short}/{qty:.0f}"
            dx, dy = offsets[ann_count % len(offsets)]
            ax_e.annotate(
                tag,
                (t, px),
                xytext=(dx, dy),
                textcoords="offset points",
                fontsize=7,
                color="#263238",
                alpha=0.92,
                bbox=dict(boxstyle="round,pad=0.12", fc="white", ec="none", alpha=0.62),
            )
            placed.append((t, px))
            ann_count += 1

        ax_e.set_xlim(zoom_start, zoom_end)
        ax_e.grid(alpha=0.25)
        # Explicit lane legend for fill role colors.
        ax_e.scatter([], [], c=[first_leg_fill_color], marker="o", s=20, label="lane fill: first-leg")
        ax_e.scatter([], [], c=[balancing_fill_color], marker="o", s=20, label="lane fill: balancing")
        ax_e.scatter([], [], c=[unknown_fill_color], marker="o", s=20, label="lane fill: unknown-role")
        ax_e.scatter([], [], c=["#111111"], marker="x", s=20, label="lane terminal: cancel")
        ax_e.legend(loc="upper left", fontsize=8)
        ax_e.set_xlabel("Seconds from window start")

        # Global layout touches.
        fig.suptitle(
            f"Window Story {run_id} ({mode}) | open={open_count} fill={fill_count_used} "
            f"fill_rate={(f'{fill_rate:.4f}' if fill_rate is not None else 'NA')} "
            f"pair_cost_final={(f'{final_pair_cost:.6f}' if final_pair_cost is not None else 'NA')} "
            f"final_abs_net={final_abs_net:.3f}",
            fontsize=12,
            y=1.01,
        )
        fig.savefig(story_png, dpi=150, bbox_inches="tight")
        plt.close(fig)
    except Exception as exc:
        print(f"INFO: plot generation failed: {exc}", file=sys.stderr)

    balancing_fill_count = sum(
        1 for e in events if e.get("raw_type") == "dryrun_apply" and e.get("fill_role") == "balancing"
    )
    accumulating_fill_count = sum(
        1 for e in events if e.get("raw_type") == "dryrun_apply" and e.get("fill_role") == "accumulating"
    )

    md_lines = [
        f"# Story Summary ({run_id}, {mode})",
        "",
        f"- jsonl: `{input_path}`",
        f"- snapshots: {len(snapshots)}",
        f"- events: {len(events)}",
        f"- open_count: {open_count}",
        f"- fill_count_resting: {fill_count_resting}",
        f"- fill_count_dryrun_apply: {fill_count_dryrun_apply}",
        f"- fill_count_used: {fill_count_used}",
        f"- fill_rate: {(f'{fill_rate:.4f}' if fill_rate is not None else 'NA')}",
        f"- balancing_fill_count (dryrun_apply): {balancing_fill_count}",
        f"- accumulating_fill_count (dryrun_apply): {accumulating_fill_count}",
        f"- ws_fill_accumulating_count: {ws_fill_accumulating_count}",
        f"- ws_fill_balancing_count: {ws_fill_balancing_count}",
        f"- orders_with_open(ws): {orders_with_open}",
        f"- orders_with_first_fill(ws): {orders_with_first_fill}",
        f"- median_open_to_fill_latency_s(ws): {(f'{median_open_to_fill_latency_s:.3f}' if median_open_to_fill_latency_s is not None else 'NA')}",
        f"- p90_open_to_fill_latency_s(ws): {(f'{p90_open_to_fill_latency_s:.3f}' if p90_open_to_fill_latency_s is not None else 'NA')}",
        f"- median_open_to_terminal_latency_s(ws): {(f'{median_open_to_terminal_latency_s:.3f}' if median_open_to_terminal_latency_s is not None else 'NA')}",
        f"- final_pair_cost: {(f'{final_pair_cost:.6f}' if final_pair_cost is not None else 'NA')}",
        f"- final_qty_up: {final_qty_up:.6f}",
        f"- final_qty_down: {final_qty_down:.6f}",
        f"- final_abs_net: {final_abs_net:.6f}",
        f"- window_pnl_usdc: {(f'{window_pnl:.6f}' if window_pnl is not None else 'NA')}",
        f"- x_axis_main_window_secs: [{global_x_min:.3f}, {global_x_max:.3f}]",
        f"- zoom_window: [{zoom_start:.3f}s, {zoom_end:.3f}s]",
        "",
        "## Top order events",
    ]
    for key, cnt in order_counts.most_common(12):
        md_lines.append(f"- {key}: {cnt}")
    md_lines.extend(
        [
            "",
            "## Plot Legend (How to Read)",
            "- Panel A: lines are `best_ask_up` and `best_ask_down`; marker size scales with `qty`.",
            "- Panel A markers: `^/v` are opens (UP/DOWN), hollow circles are WS fills, `x` are cancels, `*` are balancing dryrun_apply fills.",
            "- Gray shaded region in Panel A is the zoom window used by Panel E.",
            "- Panel B: inventory state (`qty_up`, `qty_down`) and `abs_net` (secondary axis).",
            "- Panel C: cost basis (`cost_up/down/total`) and `pair_cost` (secondary axis).",
            "- Panel D: categorical event timeline by raw_type to show event ordering density.",
            "- Panel E: local micro-structure view with order-id lifecycle lanes (open -> fill/cancel).",
            "- Panel E lane fill colors: blue=`first-leg fill`, orange=`balancing fill`, gray=`unknown role`.",
            "- Panel E lane text format: `<terminal_type> <open->terminal delay_s>`.",
            "",
            "## Key CSV Fields",
            "- `story_events.csv`: `raw_type`, `leg`, `price`, `qty`, `order_id`, `fill_role`, `entry_quote_*`, `hedge_margin_*`.",
            "- `story_series.csv`: `best_bid/ask_up/down`, `pair_cost`, `qty_up/down`, `abs_net`, `cost_up/down/total`, `slice_qty_current`.",
            "",
            "## Outputs",
            f"- `{story_events_csv.name}`",
            f"- `{story_series_csv.name}`",
            f"- `{story_png.name}`",
        ]
    )
    story_md.write_text("\n".join(md_lines), encoding="utf-8")

    print(f"wrote: {story_md}")
    print(f"wrote: {story_events_csv}")
    print(f"wrote: {story_series_csv}")
    if story_png.exists():
        print(f"wrote: {story_png}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

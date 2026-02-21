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

os.environ.setdefault("MPLBACKEND", "Agg")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate reference-style multi-panel story plot for one window jsonl."
    )
    parser.add_argument("jsonl_path", help="Path to <symbol>_<window_ts>_<mode>.jsonl")
    parser.add_argument("--out-dir", default=None, help="Output directory (default: jsonl parent)")
    parser.add_argument("--plot-width", type=float, default=13.0)
    parser.add_argument("--plot-height", type=float, default=12.0)
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
        return float(value)
    except Exception:
        return None


def load_jsonl(path: Path):
    for raw in path.read_text().splitlines():
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

    snapshots = []
    events = []
    up_id = ""
    down_id = ""

    for obj in load_jsonl(input_path):
        kind = obj.get("kind")
        ts_ms = obj.get("ts_ms")
        data = obj.get("data") if isinstance(obj.get("data"), dict) else {}

        if kind == "dryrun_snapshot":
            if not up_id and isinstance(data.get("up_id"), str):
                up_id = data["up_id"]
            if not down_id and isinstance(data.get("down_id"), str):
                down_id = data["down_id"]
            snapshots.append(
                {
                    "ts_ms": ts_ms,
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
                }
            )
        elif kind == "dryrun_apply":
            applied = data.get("applied_action") if isinstance(data.get("applied_action"), dict) else {}
            leg = infer_leg(applied, up_id, down_id)
            events.append(
                {
                    "ts_ms": ts_ms,
                    "event_type": "filled",
                    "raw_type": "dryrun_apply",
                    "leg": leg,
                    "side": applied.get("side"),
                    "price": safe_float(applied.get("fill_price")),
                    "qty": safe_float(applied.get("qty")),
                    "decision_seq": data.get("decision_seq"),
                    "round_idx": data.get("after_round_idx"),
                    "cancel_reason_detail": None,
                    "required_opp_avg_price_cap": safe_float(applied.get("required_opp_avg_price_cap")),
                    "current_opp_best_ask": safe_float(applied.get("current_opp_best_ask")),
                    "required_hedge_qty": safe_float(applied.get("required_hedge_qty")),
                    "hedge_margin_to_opp_ask": safe_float(applied.get("hedge_margin_to_opp_ask")),
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
            if not isinstance(raw_type, str):
                continue
            if not raw_type.startswith("sim_order_"):
                continue
            leg = infer_leg(data, up_id, down_id)
            if raw_type == "sim_order_filled_resting":
                event_type = "filled"
            elif "cancel" in raw_type:
                event_type = "cancel"
            elif raw_type == "sim_order_open":
                event_type = "open"
            else:
                event_type = "other"
            events.append(
                {
                    "ts_ms": ts_ms,
                    "event_type": event_type,
                    "raw_type": raw_type,
                    "leg": leg,
                    "side": data.get("side"),
                    "price": safe_float(data.get("price")),
                    "qty": safe_float(data.get("qty") if data.get("qty") is not None else data.get("size")),
                    "decision_seq": data.get("decision_seq"),
                    "round_idx": data.get("round_idx"),
                    "cancel_reason_detail": data.get("cancel_reason_detail"),
                    "required_opp_avg_price_cap": safe_float(data.get("required_opp_avg_price_cap")),
                    "current_opp_best_ask": safe_float(data.get("current_opp_best_ask")),
                    "required_hedge_qty": safe_float(data.get("required_hedge_qty")),
                    "hedge_margin_to_opp_ask": safe_float(data.get("hedge_margin_to_opp_ask")),
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

    snapshots = [x for x in snapshots if isinstance(x.get("ts_ms"), (int, float))]
    events = [x for x in events if isinstance(x.get("ts_ms"), (int, float))]
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
    ts0 = min(ts0_candidates)

    # Build timeseries rows.
    series_rows = []
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
                "mid_up": mid_up,
                "mid_down": mid_down,
                "pair_cost": pair_cost,
                "qty_up": s["qty_up"],
                "qty_down": s["qty_down"],
                "qty_total": s["qty_up"] + s["qty_down"],
                "cost_up": s["cost_up"],
                "cost_down": s["cost_down"],
                "cost_total": s["cost_up"] + s["cost_down"],
                "entry_edge_bps": s["entry_edge_bps"],
                "entry_regime_score": s["entry_regime_score"],
                "slice_count_planned": s["slice_count_planned"],
                "slice_qty_current": s["slice_qty_current"],
            }
        )

    # Build cumulative buy rows from dryrun_apply filled events only.
    cum_buy_up_shares = 0.0
    cum_buy_down_shares = 0.0
    cum_buy_up_usdc = 0.0
    cum_buy_down_usdc = 0.0
    for e in events:
        e["t_s"] = (e["ts_ms"] - ts0) / 1000.0
        if e["event_type"] == "filled" and (e.get("side") or "").upper() == "BUY":
            qty = e["qty"] or 0.0
            px = e["price"] or 0.0
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

    # Write events CSV.
    with story_events_csv.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "ts_ms",
                "t_s",
                "event_type",
                "raw_type",
                "leg",
                "side",
                "price",
                "qty",
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
                    e["event_type"],
                    e["raw_type"],
                    e["leg"],
                    e["side"],
                    e["price"],
                    e["qty"],
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
    with story_series_csv.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "ts_ms",
                "t_s",
                "mid_up",
                "mid_down",
                "pair_cost",
                "qty_up",
                "qty_down",
                "qty_total",
                "cost_up",
                "cost_down",
                "cost_total",
                "entry_edge_bps",
                "entry_regime_score",
                "slice_count_planned",
                "slice_qty_current",
            ]
        )
        for r in series_rows:
            w.writerow(
                [
                    r["ts_ms"],
                    r["t_s"],
                    r["mid_up"],
                    r["mid_down"],
                    r["pair_cost"],
                    r["qty_up"],
                    r["qty_down"],
                    r["qty_total"],
                    r["cost_up"],
                    r["cost_down"],
                    r["cost_total"],
                    r["entry_edge_bps"],
                    r["entry_regime_score"],
                    r["slice_count_planned"],
                    r["slice_qty_current"],
                ]
            )

    # Build summary stats.
    order_counts = Counter(e["raw_type"] for e in events if isinstance(e.get("raw_type"), str))
    open_count = order_counts.get("sim_order_open", 0)
    fill_count_resting = order_counts.get("sim_order_filled_resting", 0)
    fill_count_dryrun_apply = sum(
        1 for e in events if e["event_type"] == "filled" and e["raw_type"] == "dryrun_apply"
    )
    fill_count = (
        fill_count_resting if fill_count_resting > 0 else fill_count_dryrun_apply
    )
    fill_rate = (fill_count / open_count) if open_count else None

    final = series_rows[-1] if series_rows else None
    final_pair_cost = final["pair_cost"] if final else None
    final_qty_up = final["qty_up"] if final else 0.0
    final_qty_down = final["qty_down"] if final else 0.0
    final_abs_net = abs((final_qty_up or 0.0) - (final_qty_down or 0.0))
    final_cost_up = final["cost_up"] if final else 0.0
    final_cost_down = final["cost_down"] if final else 0.0
    hedgeable = min(final_qty_up or 0.0, final_qty_down or 0.0)
    unmatched_cost = max((final_cost_up or 0.0) - hedgeable * ((final_cost_up or 0.0) / (final_qty_up or 1.0) if (final_qty_up or 0.0) > 0 else 0.0), 0.0) + max(
        (final_cost_down or 0.0) - hedgeable * ((final_cost_down or 0.0) / (final_qty_down or 1.0) if (final_qty_down or 0.0) > 0 else 0.0),
        0.0,
    )
    window_pnl = None
    if final_pair_cost is not None:
        window_pnl = (1.0 - final_pair_cost) * hedgeable - unmatched_cost

    # Plot via Pillow to avoid matplotlib/font-manager crashes on some python builds.
    try:
        from PIL import Image, ImageDraw, ImageFont  # type: ignore

        width = max(1200, int(args.plot_width * 100))
        height = max(1000, int(args.plot_height * 100))
        image = Image.new("RGB", (width, height), "white")
        draw = ImageDraw.Draw(image)
        font = ImageFont.load_default()

        left = 70
        right = 20
        top = 40
        bottom = 30
        panel_gap = 12
        panel_height = (height - top - bottom - panel_gap * 3) // 4
        panels = []
        for i in range(4):
            y0 = top + i * (panel_height + panel_gap)
            y1 = y0 + panel_height
            panels.append((left, y0, width - right, y1))

        def value_bounds(values):
            vals = [v for v in values if isinstance(v, (int, float)) and math.isfinite(v)]
            if not vals:
                return (0.0, 1.0)
            lo = min(vals)
            hi = max(vals)
            if abs(hi - lo) < 1e-9:
                lo -= 0.5
                hi += 0.5
            pad = (hi - lo) * 0.08
            return (lo - pad, hi + pad)

        x_vals = [r["t_s"] for r in series_rows] or [0.0, 1.0]
        x_min = min(x_vals)
        x_max = max(x_vals)
        if abs(x_max - x_min) < 1e-9:
            x_max = x_min + 1.0

        def x_to_px(x, panel):
            x0, _, x1, _ = panel
            return x0 + (x - x_min) * (x1 - x0) / (x_max - x_min)

        def y_to_px(y, panel, y_min, y_max):
            _, y0, _, y1 = panel
            return y1 - (y - y_min) * (y1 - y0) / (y_max - y_min)

        def draw_panel_background(panel, title):
            x0, y0, x1, y1 = panel
            draw.rectangle(panel, outline="#999999", width=1)
            draw.text((x0 + 4, y0 + 2), title, fill="black", font=font)

        def draw_series(panel, pts, color, y_min, y_max):
            line = []
            for t, v in pts:
                if v is None or not isinstance(v, (int, float)) or not math.isfinite(v):
                    if len(line) >= 2:
                        draw.line(line, fill=color, width=2)
                    line = []
                    continue
                line.append((x_to_px(t, panel), y_to_px(v, panel, y_min, y_max)))
            if len(line) >= 2:
                draw.line(line, fill=color, width=2)

        draw.text((left, 12), f"Window Story: {run_id} ({mode})", fill="black", font=font)

        # Panel 1
        p1 = panels[0]
        draw_panel_background(p1, "Market Mid + Fills/Cancels")
        mid_up_vals = [r["mid_up"] for r in series_rows]
        mid_dn_vals = [r["mid_down"] for r in series_rows]
        y1_min, y1_max = value_bounds(mid_up_vals + mid_dn_vals + [e.get("price") for e in events])
        draw_series(p1, [(r["t_s"], r["mid_up"]) for r in series_rows], "#1b9e77", y1_min, y1_max)
        draw_series(p1, [(r["t_s"], r["mid_down"]) for r in series_rows], "#7b1fa2", y1_min, y1_max)
        fill_up = [e for e in events if e["event_type"] == "filled" and e["leg"] == "UP" and e["price"] is not None]
        fill_down = [e for e in events if e["event_type"] == "filled" and e["leg"] == "DOWN" and e["price"] is not None]
        cancel_key = {"sim_order_cancel_risk_guard", "sim_order_cancel_requote", "sim_order_cancel_timeout"}
        cancel_events = [e for e in events if e.get("raw_type") in cancel_key and e["price"] is not None]
        for e in fill_up:
            x = x_to_px(e["t_s"], p1)
            y = y_to_px(e["price"], p1, y1_min, y1_max)
            draw.ellipse((x - 3, y - 3, x + 3, y + 3), outline="#1b9e77", fill="#1b9e77")
        for e in fill_down:
            x = x_to_px(e["t_s"], p1)
            y = y_to_px(e["price"], p1, y1_min, y1_max)
            draw.ellipse((x - 3, y - 3, x + 3, y + 3), outline="#7b1fa2", fill="#7b1fa2")
        for e in cancel_events:
            x = x_to_px(e["t_s"], p1)
            y = y_to_px(e["price"], p1, y1_min, y1_max)
            draw.line((x - 3, y - 3, x + 3, y + 3), fill="black", width=1)
            draw.line((x - 3, y + 3, x + 3, y - 3), fill="black", width=1)

        # Panel 2
        p2 = panels[1]
        draw_panel_background(p2, "Cumulative Buys (shares + usdc)")
        event_t = [e["t_s"] for e in events] or [0.0]
        y2_values = [e["cum_buy_up_shares"] for e in events] + [e["cum_buy_down_shares"] for e in events]
        y2_values += [e["cum_buy_up_usdc"] for e in events] + [e["cum_buy_down_usdc"] for e in events]
        y2_min, y2_max = value_bounds(y2_values)
        draw_series(p2, list(zip(event_t, [e["cum_buy_up_shares"] for e in events])), "#1b9e77", y2_min, y2_max)
        draw_series(p2, list(zip(event_t, [e["cum_buy_down_shares"] for e in events])), "#7b1fa2", y2_min, y2_max)
        draw_series(p2, list(zip(event_t, [e["cum_buy_up_usdc"] for e in events])), "#4caf50", y2_min, y2_max)
        draw_series(p2, list(zip(event_t, [e["cum_buy_down_usdc"] for e in events])), "#ab47bc", y2_min, y2_max)

        # Panel 3
        p3 = panels[2]
        draw_panel_background(p3, "Dollar Exposure (cost basis)")
        y3_min, y3_max = value_bounds(
            [r["cost_up"] for r in series_rows] + [r["cost_down"] for r in series_rows] + [r["cost_total"] for r in series_rows]
        )
        draw_series(p3, [(r["t_s"], r["cost_up"]) for r in series_rows], "#1b9e77", y3_min, y3_max)
        draw_series(p3, [(r["t_s"], r["cost_down"]) for r in series_rows], "#e53935", y3_min, y3_max)
        draw_series(p3, [(r["t_s"], r["cost_total"]) for r in series_rows], "#1565c0", y3_min, y3_max)

        # Panel 4
        p4 = panels[3]
        draw_panel_background(p4, "Shares Exposure")
        y4_min, y4_max = value_bounds(
            [r["qty_up"] for r in series_rows] + [r["qty_down"] for r in series_rows] + [r["qty_total"] for r in series_rows]
        )
        draw_series(p4, [(r["t_s"], r["qty_up"]) for r in series_rows], "#1b9e77", y4_min, y4_max)
        draw_series(p4, [(r["t_s"], r["qty_down"]) for r in series_rows], "#7b1fa2", y4_min, y4_max)
        draw_series(p4, [(r["t_s"], r["qty_total"]) for r in series_rows], "#1565c0", y4_min, y4_max)
        draw.text((left + 4, panels[3][3] - 14), "Time (s)", fill="black", font=font)

        summary_lines = [
            f"open={open_count}",
            f"fill={fill_count}",
            f"fill_rate={(f'{fill_rate:.4f}' if fill_rate is not None else 'NA')}",
            f"pair_cost_final={(f'{final_pair_cost:.6f}' if final_pair_cost is not None else 'NA')}",
            f"final_qty_up={final_qty_up:.3f}",
            f"final_qty_down={final_qty_down:.3f}",
            f"final_abs_net={final_abs_net:.3f}",
            f"window_pnl_usdc={(f'{window_pnl:.6f}' if window_pnl is not None else 'NA')}",
        ]
        box_w = 240
        box_h = 14 * len(summary_lines) + 8
        bx1 = width - right - box_w
        by1 = p1[1] + 16
        draw.rectangle((bx1, by1, bx1 + box_w, by1 + box_h), outline="#777777", fill="#ffffff")
        for idx, line in enumerate(summary_lines):
            draw.text((bx1 + 6, by1 + 4 + idx * 14), line, fill="black", font=font)

        image.save(story_png)
    except Exception as exc:
        print(f"INFO: plot generation failed: {exc}", file=sys.stderr)

    md_lines = [
        f"# Story Summary ({run_id}, {mode})",
        "",
        f"- jsonl: `{input_path}`",
        f"- snapshots: {len(snapshots)}",
        f"- events: {len(events)}",
        f"- open_count: {open_count}",
        f"- fill_count_resting: {fill_count_resting}",
        f"- fill_count_dryrun_apply: {fill_count_dryrun_apply}",
        f"- fill_count_used: {fill_count}",
        f"- fill_count: {fill_count}",
        f"- fill_rate: {(f'{fill_rate:.4f}' if fill_rate is not None else 'NA')}",
        f"- final_pair_cost: {(f'{final_pair_cost:.6f}' if final_pair_cost is not None else 'NA')}",
        f"- final_qty_up: {final_qty_up:.6f}",
        f"- final_qty_down: {final_qty_down:.6f}",
        f"- final_abs_net: {final_abs_net:.6f}",
        f"- window_pnl_usdc: {(f'{window_pnl:.6f}' if window_pnl is not None else 'NA')}",
        "",
        "## Top order events",
    ]
    for key, cnt in order_counts.most_common(10):
        md_lines.append(f"- {key}: {cnt}")
    md_lines.extend(
        [
            "",
            "## Outputs",
            f"- `{story_events_csv.name}`",
            f"- `{story_series_csv.name}`",
            f"- `{story_png.name}`",
        ]
    )
    story_md.write_text("\n".join(md_lines))

    print(f"wrote: {story_md}")
    print(f"wrote: {story_events_csv}")
    print(f"wrote: {story_series_csv}")
    if story_png.exists():
        print(f"wrote: {story_png}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

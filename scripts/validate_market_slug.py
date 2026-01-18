#!/usr/bin/env python3
import argparse
import json
import os
import re
import sys
import urllib.request


def parse_args():
    parser = argparse.ArgumentParser(
        description="Validate a 15m market slug via Gamma /markets."
    )
    parser.add_argument("market_slug", help="Market slug to validate")
    parser.add_argument(
        "--gamma-host",
        default=os.environ.get("GAMMA_HOST", "https://gamma-api.polymarket.com"),
        help="Gamma host (default: $GAMMA_HOST or https://gamma-api.polymarket.com)",
    )
    return parser.parse_args()


def top_level_markets(value):
    if isinstance(value, list):
        return value, "array"
    if isinstance(value, dict):
        if isinstance(value.get("markets"), list):
            return value.get("markets"), "object(markets)"
        if isinstance(value.get("data"), list):
            return value.get("data"), "object(data)"
    return None, "other"


def parse_token_ids(value):
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return None
    if isinstance(value, list):
        if all(isinstance(v, str) for v in value):
            return value
        if all(isinstance(v, dict) for v in value):
            ids = []
            for item in value:
                for key in ("id", "tokenId", "token_id"):
                    if isinstance(item.get(key), str):
                        ids.append(item.get(key))
                        break
            return ids if ids else None
    return None


def validate_slug_format(slug):
    m = re.match(r"^.+-updown-15m-(\d+)$", slug)
    if not m:
        return False
    try:
        ts = int(m.group(1))
    except Exception:
        return False
    return ts % 900 == 0


def main():
    args = parse_args()
    slug = args.market_slug
    if not validate_slug_format(slug):
        print("reason=not_15m", flush=True)
        return 2

    gamma_host = args.gamma_host.rstrip("/")
    url = f"{gamma_host}/markets?slug={slug}&limit=1"
    req = urllib.request.Request(url, headers={"User-Agent": "poly-maker-rs/validate"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode("utf-8")
    except Exception as exc:
        print(f"reason=not_found error={exc}", flush=True)
        return 2

    try:
        value = json.loads(body)
    except Exception as exc:
        print(f"reason=not_found error=parse_failed {exc}", flush=True)
        return 2

    markets, top_level = top_level_markets(value)
    if not markets:
        print(f"reason=not_found top_level={top_level}", flush=True)
        return 2

    market = None
    for item in markets:
        if isinstance(item, dict) and item.get("slug") == slug:
            market = item
            break
    if not market:
        print("reason=not_found", flush=True)
        return 2

    if market.get("closed") is True:
        print("reason=closed", flush=True)
        return 2
    if market.get("acceptingOrders") is not True:
        print("reason=not_accepting_orders", flush=True)
        return 2
    if market.get("enableOrderBook") is not True:
        print("reason=no_orderbook", flush=True)
        return 2

    token_ids = None
    for field in ("outcomeTokenIds", "clobTokenIds", "tokenIds"):
        token_ids = parse_token_ids(market.get(field))
        if token_ids:
            break
    if token_ids is None:
        for field in ("tokens", "assets", "outcomeTokens"):
            token_ids = parse_token_ids(market.get(field))
            if token_ids:
                break

    if not token_ids or len(token_ids) != 2:
        print("reason=token_ids_invalid", flush=True)
        return 2

    print("ok", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# Poly-Maker-RS

Rust-based dry-run quoting/analysis tool for Polymarket CLOB markets. It uses Gamma to
select a market (latest by series or fixed by market slug), subscribes to the market
WS channel, and emits JSONL logs for replay and offline analysis.

## Requirements

- Rust 1.75+
- `python3` (for log extraction and summary)
- `jq` (for `scripts/check_run.sh`)
- Optional: `matplotlib` (for `scripts/summary_run.py` PNG plots)

## Repository Layout

- `poly_maker/` - Rust crate
- `scripts/` - run/check/summary helpers
- `logs/` - runtime outputs (ignored by git)
- `.env` - local environment (ignored by git)

## Quick Start

1) Create `poly-maker-rs/.env` with required variables (example values):

```bash
CLOB_HOST=https://clob.polymarket.com
WS_HOST=wss://ws-subscriptions-clob.polymarket.com
WS_PATH=/ws/market
GAMMA_HOST=https://gamma-api.polymarket.com
SERIES_SLUG=xrp-updown-15m
```

2) Run a paper session:

```bash
DRYRUN_MODE=paper \
ROLLOVER_LOG_JSON=1 \
ROLLOVER_LOG_VERBOSE=0 \
RUST_LOG=info \
./scripts/run_paper.sh
```

3) Run a recommend session:

```bash
DRYRUN_MODE=recommend \
ROLLOVER_LOG_JSON=1 \
ROLLOVER_LOG_VERBOSE=0 \
RUST_LOG=info \
./scripts/run_recommend.sh
```

## Fixed Market Mode

To pin a single market and disable rollover, set `MARKET_SLUG` and omit `SERIES_SLUG`:

```bash
MARKET_SLUG=xrp-updown-15m-1768582800 \
DRYRUN_MODE=paper \
./scripts/run_paper.sh
```

In this mode, the program uses Gamma to resolve the two outcome token IDs and
subscribes only to that market.

## Outputs

Each run writes to `logs/`:

- `<run_id>_<mode>_full.log` - full stdout/stderr
- `<run_id>_<mode>.jsonl` - extracted JSON lines

Validation:

```bash
./scripts/check_run.sh logs/<run_id>_paper.jsonl
```

Offline summary (CSV + Markdown + optional PNG):

```bash
python3 scripts/summary_run.py logs/<run_id>_paper.jsonl
```

If `matplotlib` is missing, the PNG is skipped and a note is written in the summary.


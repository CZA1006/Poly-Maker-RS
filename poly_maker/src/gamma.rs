use anyhow::{anyhow, bail, Context, Result};
use reqwest::Client;
use serde::Deserialize;
use std::collections::{HashMap, VecDeque};
use std::env;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::Utc;

#[derive(Debug, Clone, Deserialize)]
pub struct Series {
    #[serde(default)]
    pub slug: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EventMarket {
    pub slug: String,
    #[serde(rename = "conditionId")]
    pub condition_id: String,
    #[serde(default)]
    pub closed: Option<bool>,
    #[serde(default, rename = "acceptingOrders")]
    pub accepting_orders: Option<bool>,
    #[serde(default, rename = "enableOrderBook")]
    pub enable_order_book: Option<bool>,
    #[serde(default, rename = "clobTokenIds")]
    pub clob_token_ids: Option<serde_json::Value>,
    #[serde(default, rename = "orderPriceMinTickSize")]
    pub order_price_min_tick_size: Option<serde_json::Value>,
    #[serde(default, rename = "endDate")]
    pub end_date: Option<String>,
    #[serde(default, rename = "acceptingOrdersTimestamp")]
    pub accepting_orders_timestamp: Option<String>,
    #[serde(default, rename = "eventStartTime")]
    pub event_start_time: Option<String>,
    #[serde(default, rename = "startTime")]
    pub start_time: Option<String>,
    #[serde(default, rename = "createdAt")]
    pub created_at: Option<String>,
    #[serde(default, rename = "updatedAt")]
    pub updated_at: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    pub id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Event {
    #[serde(default)]
    pub slug: Option<String>,
    #[serde(default, rename = "seriesSlug")]
    pub series_slug: Option<String>,
    #[serde(default)]
    pub series: Vec<Series>,
    #[serde(default)]
    pub markets: Vec<EventMarket>,
    #[serde(default)]
    pub title: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SelectedMarket {
    pub slug: String,
    pub condition_id: String,
    pub token_ids: [String; 2],
    pub tick_size: Option<f64>,
    pub event_slug: Option<String>,
    pub score: Option<i64>,
    pub event_start_time: Option<String>,
    pub start_time: Option<String>,
    pub end_date: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FixedMarketInfo {
    pub slug: String,
    pub condition_id: String,
    pub up_token_id: String,
    pub down_token_id: String,
    pub tick_size: Option<f64>,
    pub event_slug: Option<String>,
    pub event_start_time: Option<String>,
    pub start_time: Option<String>,
    pub end_date: Option<String>,
}

#[derive(Debug, Clone)]
struct TokenCandidate {
    id: String,
    label: Option<String>,
}

#[derive(Debug, Clone)]
struct SampleEventInfo {
    slug: Option<String>,
    series_slug: Option<String>,
    series0_slug: Option<String>,
    series_len: usize,
    title: Option<String>,
}

#[derive(Debug, Clone)]
struct BestPick {
    selected: SelectedMarket,
    score: i64,
    start_ts: i64,
    end_ts: i64,
    updated_ts: Option<i64>,
    matched_by: Option<String>,
}

#[derive(Debug, Default, Clone)]
struct CandidateStats {
    candidates_total: u64,
    qualified_total: u64,
    rejected_no_market: u64,
    rejected_closed: u64,
    rejected_not_accepting_orders: u64,
    rejected_no_orderbook: u64,
}

#[derive(Debug, Default, Clone)]
pub struct GammaPickStats {
    pub mode: String,
    pub pages_scanned: u64,
    pub total_events_seen: u64,
    pub matched_events: u64,
    pub candidates_total: u64,
    pub qualified_total: u64,
    pub rejected_no_market: u64,
    pub rejected_closed: u64,
    pub rejected_not_accepting_orders: u64,
    pub rejected_no_orderbook: u64,
    pub decode_errors: u64,
    pub http_errors: u64,
    pub best_score: Option<i64>,
    pub selected_market_slug: Option<String>,
    pub event_slug: Option<String>,
    pub matched_by: Option<String>,
    pub fastpath_hit: bool,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    selected: SelectedMarket,
    start_ts: i64,
    end_ts: i64,
    cached_at_ms: i64,
}

static MARKET_CACHE: OnceLock<Mutex<HashMap<String, CacheEntry>>> = OnceLock::new();
static LAST_PICK_STATS: OnceLock<Mutex<GammaPickStats>> = OnceLock::new();

const CACHE_TTL_MS: i64 = 2500;

pub fn take_last_pick_stats() -> GammaPickStats {
    let stats = LAST_PICK_STATS.get_or_init(|| Mutex::new(GammaPickStats::default()));
    stats
        .lock()
        .map(|guard| guard.clone())
        .unwrap_or_default()
}

fn update_last_pick_stats(stats: &GammaPickStats) {
    let store = LAST_PICK_STATS.get_or_init(|| Mutex::new(GammaPickStats::default()));
    if let Ok(mut guard) = store.lock() {
        *guard = stats.clone();
    }
}

pub async fn fetch_fixed_market_info_by_slug(market_slug: &str) -> Result<FixedMarketInfo> {
    let gamma_host =
        env::var("GAMMA_HOST").unwrap_or_else(|_| "https://gamma-api.polymarket.com".to_string());
    let gamma_host = gamma_host.trim_end_matches('/');
    let client = Client::new();
    let mut stats = GammaPickStats::default();
    stats.mode = "fixed_market_slug".to_string();

    let mut url =
        reqwest::Url::parse(&format!("{gamma_host}/markets")).context("invalid GAMMA_HOST")?;
    url.query_pairs_mut()
        .append_pair("slug", market_slug)
        .append_pair("limit", "1");
    let url_string = url.to_string();
    tracing::info!(gamma_markets_url = %url_string, "gamma markets url");

    let response = client
        .get(url)
        .send()
        .await
        .context("failed to fetch gamma market")?;
    if !response.status().is_success() {
        stats.http_errors += 1;
        update_last_pick_stats(&stats);
        bail!("gamma response status {}", response.status());
    }

    let body = response.text().await.context("failed to read gamma response")?;
    let value: serde_json::Value = match serde_json::from_str(&body) {
        Ok(value) => value,
        Err(err) => {
            stats.decode_errors += 1;
            update_last_pick_stats(&stats);
            log_body_snippet(&body);
            return Err(err).context("failed to parse gamma response");
        }
    };

    let top_level = top_level_kind(&value);
    tracing::info!(gamma_top_level = %top_level, "gamma markets top_level");

    let selected = parse_fixed_market_info_from_value(&value, market_slug, &url_string, &body)?;

    stats.total_events_seen = 1;
    stats.matched_events = 1;
    stats.candidates_total = 1;
    stats.qualified_total = 1;
    stats.selected_market_slug = Some(selected.slug.clone());
    stats.event_slug = selected.event_slug.clone();
    update_last_pick_stats(&stats);
    Ok(selected)
}

pub async fn pick_latest_open_market_by_series(series_slug: &str) -> Result<SelectedMarket> {
    let gamma_host =
        env::var("GAMMA_HOST").unwrap_or_else(|_| "https://gamma-api.polymarket.com".to_string());
    let gamma_host = gamma_host.trim_end_matches('/');
    let client = Client::new();
    let now = Utc::now().timestamp();
    let now_ms = now_millis();
    let mut stats = GammaPickStats::default();
    stats.mode = "scan".to_string();
    let mut recent_event_slugs: VecDeque<String> = VecDeque::with_capacity(10);
    let mut last_sample: Option<SampleEventInfo> = None;
    let mut best_pick: Option<BestPick> = None;
    const MAX_PAGES: u64 = 30;

    if let Some(entry) = get_cached_pick(series_slug, now_ms) {
        let score = score_for_window(now, entry.start_ts, entry.end_ts);
        let mut selected = entry.selected.clone();
        selected.score = Some(score);
        stats.mode = "fastpath".to_string();
        stats.best_score = Some(score);
        stats.selected_market_slug = Some(selected.slug.clone());
        stats.event_slug = selected.event_slug.clone();
        update_last_pick_stats(&stats);
        return Ok(selected);
    }

    if let Some(fast_pick) =
        try_fastpath_event(&client, gamma_host, series_slug, now, &mut stats).await
    {
        let mut selected = fast_pick.selected;
        selected.score = Some(fast_pick.score);
        stats.mode = "fastpath".to_string();
        stats.best_score = Some(fast_pick.score);
        stats.selected_market_slug = Some(selected.slug.clone());
        stats.event_slug = selected.event_slug.clone();
        stats.matched_by = fast_pick.matched_by.clone();
        stats.fastpath_hit = true;
        update_last_pick_stats(&stats);
        return Ok(selected);
    }

    for page in 0..MAX_PAGES {
        let offset = page * 100;
        let mut url =
            reqwest::Url::parse(&format!("{gamma_host}/events")).context("invalid GAMMA_HOST")?;
        url.query_pairs_mut()
            .append_pair("order", "id")
            .append_pair("ascending", "false")
            .append_pair("closed", "false")
            .append_pair("limit", "100")
            .append_pair("offset", &offset.to_string());

        let response = client
            .get(url)
            .send()
            .await
            .context("failed to fetch gamma events")?;

        if !response.status().is_success() {
            stats.http_errors += 1;
            update_last_pick_stats(&stats);
            bail!("gamma response status {}", response.status());
        }

        let body = response.text().await.context("failed to read gamma response")?;
        let value: serde_json::Value = match serde_json::from_str(&body) {
            Ok(value) => value,
            Err(err) => {
                log_body_snippet(&body);
                stats.decode_errors += 1;
                update_last_pick_stats(&stats);
                return Err(err).context("failed to parse gamma response");
            }
        };

        let events_value = if value.is_array() {
            value
        } else if let Some(v) = value.get("events") {
            v.clone()
        } else if let Some(v) = value.get("data") {
            v.clone()
        } else {
            stats.decode_errors += 1;
            update_last_pick_stats(&stats);
            bail!("unexpected gamma response shape");
        };

        let events: Vec<Event> = match serde_json::from_value(events_value) {
            Ok(events) => events,
            Err(err) => {
                log_body_snippet(&body);
                stats.decode_errors += 1;
                update_last_pick_stats(&stats);
                return Err(err).context("failed to decode events");
            }
        };
        tracing::info!(offset, count = events.len(), "events page");
        stats.pages_scanned += 1;

        if events.is_empty() {
            break;
        }

        if let Some(sample) = sample_event_info(&events) {
            tracing::info!(
                sample_event_slug = ?sample.slug,
                sample_seriesSlug = ?sample.series_slug,
                sample_series0_slug = ?sample.series0_slug,
                sample_has_series_len = sample.series_len,
                sample_title = ?sample.title,
                "gamma event sample"
            );
            last_sample = Some(sample);
        } else {
            tracing::info!(offset, "gamma event sample missing");
        }

        for event in events {
            stats.total_events_seen += 1;
            if let Some(slug) = event.slug.as_deref() {
                push_recent_slug(&mut recent_event_slugs, slug);
            }

            let matched_by = match_event_series(&event, series_slug);
            if matched_by.is_none() {
                continue;
            }
            stats.matched_events += 1;
            let event_slug = event.slug.as_deref().unwrap_or(series_slug);
            let series_slugs = collect_series_slugs(&event);
            tracing::info!(
                event_slug = %event_slug,
                seriesSlug = ?event.series_slug,
                series_slugs = ?series_slugs,
                matched_by = matched_by.unwrap_or("unknown"),
                "found series event"
            );

            let batch = candidates_from_event(&event, now);
            stats.candidates_total += batch.stats.candidates_total;
            stats.qualified_total += batch.stats.qualified_total;
            stats.rejected_no_market += batch.stats.rejected_no_market;
            stats.rejected_closed += batch.stats.rejected_closed;
            stats.rejected_not_accepting_orders += batch.stats.rejected_not_accepting_orders;
            stats.rejected_no_orderbook += batch.stats.rejected_no_orderbook;
            for (mut selected, start_ts, end_ts, updated_ts) in batch.candidates {
                let score = if start_ts <= now && now <= end_ts {
                    0
                } else if now < start_ts {
                    start_ts - now
                } else {
                    now - end_ts
                };

                if score == 0 {
                    selected.score = Some(score);
                    stats.best_score = Some(score);
                    stats.selected_market_slug = Some(selected.slug.clone());
                    stats.event_slug = selected.event_slug.clone();
                    stats.matched_by = matched_by.map(|value| value.to_string());
                    stats.fastpath_hit = false;
                    update_last_pick_stats(&stats);
                    let mut cached = selected.clone();
                    cached.score = None;
                    put_cached_pick(series_slug, &cached, start_ts, end_ts, now_ms);
                    return Ok(selected);
                }

                let replace = match &best_pick {
                    None => true,
                    Some(best) => is_better_pick(score, updated_ts, &selected, best),
                };
                if replace {
                    best_pick = Some(BestPick {
                        selected,
                        score,
                        start_ts,
                        end_ts,
                        updated_ts,
                        matched_by: matched_by.map(|value| value.to_string()),
                    });
                }
            }
        }
    }

    if let Some(best) = best_pick {
        let mut selected = best.selected;
        selected.score = Some(best.score);
        stats.best_score = Some(best.score);
        stats.selected_market_slug = Some(selected.slug.clone());
        stats.event_slug = selected.event_slug.clone();
        stats.matched_by = best.matched_by.clone();
        stats.fastpath_hit = false;
        update_last_pick_stats(&stats);
        let mut cached = selected.clone();
        cached.score = None;
        put_cached_pick(series_slug, &cached, best.start_ts, best.end_ts, now_ms);
        return Ok(selected);
    }

    stats.best_score = None;
    if stats.matched_events == 0 {
        update_last_pick_stats(&stats);
        tracing::info!(
            now,
            total_events_seen = stats.total_events_seen,
            recent_event_slugs = ?recent_event_slugs,
            sample_event = ?last_sample,
            pages_scanned = stats.pages_scanned,
            candidates_total = stats.candidates_total,
            best_score = ?stats.best_score,
            "gamma scan summary: 0 matched"
        );
        bail!("no events matched series_slug={series_slug}");
    }

    update_last_pick_stats(&stats);
    tracing::info!(
        now,
        matched_events = stats.matched_events,
        total_events_seen = stats.total_events_seen,
        recent_event_slugs = ?recent_event_slugs,
        sample_event = ?last_sample,
        pages_scanned = stats.pages_scanned,
        candidates_total = stats.candidates_total,
        best_score = ?stats.best_score,
        "gamma scan summary: no eligible markets"
    );
    bail!("no eligible markets found for series_slug={series_slug}")
}

pub fn extract_token_ids(v: &serde_json::Value) -> Result<[String; 2]> {
    match v {
        serde_json::Value::Array(items) => parse_token_ids_array(items),
        serde_json::Value::String(raw) => {
            let inner: serde_json::Value =
                serde_json::from_str(raw).context("clobTokenIds string is not valid JSON")?;
            extract_token_ids(&inner)
        }
        _ => bail!("clobTokenIds must be array or json string"),
    }
}

fn parse_token_ids_array(items: &[serde_json::Value]) -> Result<[String; 2]> {
    if items.len() != 2 {
        bail!("expected 2 token ids, got {}", items.len());
    }
    let mut out: Vec<String> = Vec::with_capacity(2);
    for item in items {
        let id = match item {
            serde_json::Value::String(value) => value.clone(),
            serde_json::Value::Number(value) => value.to_string(),
            _ => bail!("token id must be string or number"),
        };
        out.push(id);
    }
    Ok([out[0].clone(), out[1].clone()])
}

fn log_body_snippet(body: &str) {
    let snippet = if body.chars().count() > 1000 {
        body.chars().take(1000).collect::<String>()
    } else {
        body.to_string()
    };
    tracing::warn!(body_snippet = %snippet, "gamma response body snippet");
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn get_cached_pick(series_slug: &str, now_ms: i64) -> Option<CacheEntry> {
    let cache = MARKET_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = cache.lock().ok()?;
    if let Some(entry) = guard.get(series_slug) {
        if now_ms - entry.cached_at_ms <= CACHE_TTL_MS {
            tracing::debug!(
                series_slug = %series_slug,
                selected_market_slug = %entry.selected.slug,
                "gamma cache hit"
            );
            return Some(entry.clone());
        }
        guard.remove(series_slug);
    }
    None
}

fn put_cached_pick(
    series_slug: &str,
    selected: &SelectedMarket,
    start_ts: i64,
    end_ts: i64,
    now_ms: i64,
) {
    let cache = MARKET_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Ok(mut guard) = cache.lock() {
        guard.insert(
            series_slug.to_string(),
            CacheEntry {
                selected: selected.clone(),
                start_ts,
                end_ts,
                cached_at_ms: now_ms,
            },
        );
    }
}

fn score_for_window(now: i64, start_ts: i64, end_ts: i64) -> i64 {
    if start_ts <= now && now <= end_ts {
        0
    } else if now < start_ts {
        start_ts - now
    } else {
        now - end_ts
    }
}

fn fastpath_event_slugs(series_slug: &str, now: i64) -> Vec<String> {
    if !series_slug.ends_with("-updown-15m") {
        return Vec::new();
    }
    let window_start = (now / 900) * 900;
    let prev_window_start = window_start - 900;
    vec![
        format!("{series_slug}-{window_start}"),
        format!("{series_slug}-{prev_window_start}"),
    ]
}

async fn try_fastpath_event(
    client: &Client,
    gamma_host: &str,
    series_slug: &str,
    now: i64,
    stats: &mut GammaPickStats,
) -> Option<BestPick> {
    for event_slug in fastpath_event_slugs(series_slug, now) {
        let url = format!("{gamma_host}/events/slug/{event_slug}");
        let response = match client.get(&url).send().await {
            Ok(resp) => resp,
            Err(err) => {
                stats.http_errors += 1;
                tracing::info!(event_slug = %event_slug, error = %err, "gamma slug fastpath miss: http error");
                continue;
            }
        };
        let status = response.status();
        if !status.is_success() {
            if status.as_u16() != 404 {
                stats.http_errors += 1;
            }
            tracing::info!(event_slug = %event_slug, status = %status, "gamma slug fastpath miss");
            continue;
        }

        let body = match response.text().await {
            Ok(body) => body,
            Err(err) => {
                stats.decode_errors += 1;
                tracing::info!(
                    event_slug = %event_slug,
                    error = %err,
                    "gamma slug fastpath miss: read body failed"
                );
                continue;
            }
        };
        let value: serde_json::Value = match serde_json::from_str(&body) {
            Ok(value) => value,
            Err(err) => {
                log_body_snippet(&body);
                stats.decode_errors += 1;
                tracing::info!(
                    event_slug = %event_slug,
                    error = %err,
                    "gamma slug fastpath miss: invalid json"
                );
                continue;
            }
        };

        let event_value = if let Some(v) = value.get("event") {
            v.clone()
        } else if let Some(v) = value.get("data") {
            v.clone()
        } else {
            value
        };

        let event: Event = match serde_json::from_value(event_value) {
            Ok(event) => event,
            Err(err) => {
                log_body_snippet(&body);
                stats.decode_errors += 1;
                tracing::info!(
                    event_slug = %event_slug,
                    error = %err,
                    "gamma slug fastpath miss: decode failed"
                );
                continue;
            }
        };

        let matched_by = match_event_series(&event, series_slug);
        if matched_by.is_none() {
            tracing::info!(
                event_slug = %event_slug,
                seriesSlug = ?event.series_slug,
                "gamma slug fastpath miss: series mismatch"
            );
            continue;
        }

        stats.matched_events += 1;
        let batch = candidates_from_event(&event, now);
        stats.candidates_total += batch.stats.candidates_total;
        stats.qualified_total += batch.stats.qualified_total;
        stats.rejected_no_market += batch.stats.rejected_no_market;
        stats.rejected_closed += batch.stats.rejected_closed;
        stats.rejected_not_accepting_orders += batch.stats.rejected_not_accepting_orders;
        stats.rejected_no_orderbook += batch.stats.rejected_no_orderbook;
        if batch.candidates.is_empty() {
            tracing::info!(
                event_slug = %event_slug,
                "gamma slug fastpath miss: no eligible markets"
            );
            continue;
        }

        let mut best: Option<BestPick> = None;
        for (mut selected, start_ts, end_ts, updated_ts) in batch.candidates {
            let score = score_for_window(now, start_ts, end_ts);
            let replace = match &best {
                None => true,
                Some(existing) => is_better_pick(score, updated_ts, &selected, existing),
            };
            if replace {
                if selected.event_slug.is_none() {
                    selected.event_slug = Some(event_slug.clone());
                }
                best = Some(BestPick {
                    selected,
                    score,
                    start_ts,
                    end_ts,
                    updated_ts,
                    matched_by: matched_by.map(|value| value.to_string()),
                });
            }
        }

        if let Some(best) = best {
            return Some(best);
        }
    }

    None
}

fn match_event_series(event: &Event, series_slug: &str) -> Option<&'static str> {
    let target = normalize(series_slug);
    if let Some(slug) = event.series_slug.as_deref() {
        if normalize(slug) == target {
            return Some("seriesSlug");
        }
    }

    for series in &event.series {
        if let Some(slug) = series.slug.as_deref() {
            if normalize(slug) == target {
                return Some("series[].slug");
            }
        }
    }

    if let Some(event_slug) = event.slug.as_deref() {
        let event_slug = normalize(event_slug);
        if event_slug == target || event_slug.starts_with(&format!("{target}-")) {
            return Some("event.slug_prefix");
        }
    }

    None
}

struct CandidateBatch {
    candidates: Vec<(SelectedMarket, i64, i64, Option<i64>)>,
    stats: CandidateStats,
}

fn candidates_from_event(event: &Event, now: i64) -> CandidateBatch {
    let mut candidates: Vec<(SelectedMarket, i64, i64, Option<i64>)> = Vec::new();
    let mut stats = CandidateStats::default();
    let mut reject_missing_token_ids: u64 = 0;
    let mut reject_bad_token_ids: u64 = 0;
    let mut reject_time_missing: u64 = 0;
    let mut future_candidates_count: u64 = 0;
    let mut old_candidates_count: u64 = 0;
    let mut current_candidates_count: u64 = 0;
    let mut missing_time_log_count: u32 = 0;
    const MAX_TIME_MISSING_LOGS: u32 = 3;

    if event.markets.is_empty() {
        stats.rejected_no_market += 1;
        return CandidateBatch { candidates, stats };
    }

    for market in &event.markets {
        if market.closed.unwrap_or(false) {
            stats.rejected_closed += 1;
            continue;
        }
        if !market.accepting_orders.unwrap_or(false) {
            stats.rejected_not_accepting_orders += 1;
            continue;
        }
        if !market.enable_order_book.unwrap_or(false) {
            stats.rejected_no_orderbook += 1;
            continue;
        }

        let token_ids = match market.clob_token_ids.as_ref() {
            Some(value) => match extract_token_ids(value) {
                Ok(ids) => ids,
                Err(err) => {
                    tracing::debug!(error = %err, "invalid clobTokenIds");
                    reject_bad_token_ids += 1;
                    continue;
                }
            },
            None => {
                reject_missing_token_ids += 1;
                continue;
            }
        };
        stats.candidates_total += 1;
        let tick_size = market
            .order_price_min_tick_size
            .as_ref()
            .and_then(parse_tick_size)
            .filter(|value| value.is_finite() && *value > 0.0);
        let selected = SelectedMarket {
            slug: market.slug.clone(),
            condition_id: market.condition_id.clone(),
            token_ids,
            tick_size,
            event_slug: event.slug.clone(),
            score: None,
            event_start_time: market.event_start_time.clone(),
            start_time: market.start_time.clone(),
            end_date: market.end_date.clone(),
        };
        let start_ts = market
            .event_start_time
            .as_deref()
            .or(market.start_time.as_deref())
            .or(market.accepting_orders_timestamp.as_deref())
            .and_then(parse_iso_to_unix);
        let end_ts = market.end_date.as_deref().and_then(parse_iso_to_unix);
        let updated_ts = market
            .updated_at
            .as_deref()
            .or(market.created_at.as_deref())
            .and_then(parse_iso_to_unix);
        if start_ts.is_none() || end_ts.is_none() {
            reject_time_missing += 1;
            if missing_time_log_count < MAX_TIME_MISSING_LOGS {
                missing_time_log_count += 1;
                tracing::warn!(
                    event_slug = ?event.slug,
                    market_slug = %market.slug,
                    eventStartTime = ?market.event_start_time,
                    startTime = ?market.start_time,
                    endDate = ?market.end_date,
                    "market missing time fields; deprioritized"
                );
            }
            continue;
        }

        let start_ts = start_ts.unwrap();
        let end_ts = end_ts.unwrap();
        if start_ts <= now && now <= end_ts {
            current_candidates_count += 1;
        } else if now < start_ts {
            future_candidates_count += 1;
        } else {
            old_candidates_count += 1;
        }

        stats.qualified_total += 1;
        candidates.push((selected, start_ts, end_ts, updated_ts));
    }

    if candidates.is_empty() {
        tracing::warn!(
            event_slug = ?event.slug,
            now,
            reject_closed = stats.rejected_closed,
            reject_not_accepting = stats.rejected_not_accepting_orders,
            reject_no_orderbook = stats.rejected_no_orderbook,
            reject_missing_token_ids,
            reject_bad_token_ids,
            reject_time_missing,
            current_candidates_count,
            future_candidates_count,
            old_candidates_count,
            candidates_count = candidates.len(),
            "no eligible markets in event"
        );
    }

    CandidateBatch { candidates, stats }
}

fn parse_tick_size(value: &serde_json::Value) -> Option<f64> {
    match value {
        serde_json::Value::String(raw) => raw.parse::<f64>().ok(),
        serde_json::Value::Number(num) => num.as_f64(),
        _ => None,
    }
}

fn top_level_kind(value: &serde_json::Value) -> &'static str {
    if value.is_array() {
        return "array";
    }
    if let Some(obj) = value.as_object() {
        if obj.get("markets").is_some() {
            return "object(markets)";
        }
        if obj.get("data").is_some() {
            return "object(data)";
        }
    }
    "other"
}

fn markets_array_from_value<'a>(value: &'a serde_json::Value) -> Option<&'a Vec<serde_json::Value>> {
    if let Some(items) = value.as_array() {
        return Some(items);
    }
    if let Some(obj) = value.as_object() {
        if let Some(items) = obj.get("markets").and_then(|v| v.as_array()) {
            return Some(items);
        }
        if let Some(items) = obj.get("data").and_then(|v| v.as_array()) {
            return Some(items);
        }
    }
    None
}

fn format_json(value: Option<&serde_json::Value>) -> String {
    match value {
        Some(v) => serde_json::to_string(v).unwrap_or_else(|_| "<unserializable>".to_string()),
        None => "null".to_string(),
    }
}

fn market_keys(value: &serde_json::Value) -> Vec<String> {
    value
        .as_object()
        .map(|obj| obj.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default()
}

fn find_market_by_slug<'a>(
    markets: &'a [serde_json::Value],
    slug: &str,
) -> Option<&'a serde_json::Value> {
    markets
        .iter()
        .find(|m| m.get("slug").and_then(|v| v.as_str()) == Some(slug))
}

fn parse_fixed_market_info_from_value(
    value: &serde_json::Value,
    market_slug: &str,
    url: &str,
    body: &str,
) -> Result<FixedMarketInfo> {
    let top_level = top_level_kind(value);
    let markets = match markets_array_from_value(value) {
        Some(items) => items,
        None => {
            log_body_snippet(body);
            bail!(
                "market_slug={market_slug} top_level={top_level} url={url} body_snippet=missing_markets_array"
            );
        }
    };
    if markets.is_empty() {
        log_body_snippet(body);
        bail!("market_slug={market_slug} top_level={top_level} url={url} markets_len=0");
    }
    let market = match find_market_by_slug(markets, market_slug) {
        Some(market) => market,
        None => {
            log_body_snippet(body);
            bail!(
                "market_slug={market_slug} top_level={top_level} url={url} markets_len={}",
                markets.len()
            );
        }
    };

    parse_fixed_market_from_market(market, market_slug, url, top_level)
}

fn parse_fixed_market_from_market(
    market: &serde_json::Value,
    market_slug: &str,
    url: &str,
    top_level: &str,
) -> Result<FixedMarketInfo> {
    let keys = market_keys(market);
    let outcomes_json = format_json(market.get("outcomes"));
    let clob_json = format_json(market.get("clobTokenIds"));

    let slug = market
        .get("slug")
        .and_then(|v| v.as_str())
        .context("market missing slug")?
        .to_string();
    let condition_id = market
        .get("conditionId")
        .and_then(|v| v.as_str())
        .context("market missing conditionId")?
        .to_string();

    let token_candidates = extract_token_candidates_from_market(market, market_slug)
        .map_err(|err| {
            anyhow!(
                "market_slug={market_slug} {err} outcomes={outcomes_json} clobTokenIds={clob_json} top_level={top_level} url={url} market_keys={keys:?}"
            )
        })?;

    let outcome_names = extract_outcome_names_from_market(market);
    let (up_token_id, down_token_id, outcome_mode) =
        resolve_up_down_tokens(&token_candidates, outcome_names.as_deref()).map_err(|err| {
            anyhow!(
                "market_slug={market_slug} {err} outcomes={outcomes_json} clobTokenIds={clob_json} top_level={top_level} url={url} market_keys={keys:?}"
            )
        })?;

    tracing::info!(
        market_slug = %slug,
        condition_id = %condition_id,
        up_token_id = %up_token_id,
        down_token_id = %down_token_id,
        outcome_mode = %outcome_mode,
        outcomes = %outcomes_json,
        clobTokenIds = %clob_json,
        "resolved_fixed_market"
    );

    let tick_size = market
        .get("orderPriceMinTickSize")
        .and_then(parse_tick_size);
    let event_slug = market
        .get("eventSlug")
        .and_then(|v| v.as_str())
        .map(|v| v.to_string());
    let event_start_time = market
        .get("eventStartTime")
        .and_then(|v| v.as_str())
        .map(|v| v.to_string());
    let start_time = market
        .get("startTime")
        .and_then(|v| v.as_str())
        .map(|v| v.to_string());
    let end_date = market
        .get("endDate")
        .and_then(|v| v.as_str())
        .map(|v| v.to_string());

    Ok(FixedMarketInfo {
        slug,
        condition_id,
        up_token_id,
        down_token_id,
        tick_size,
        event_slug,
        event_start_time,
        start_time,
        end_date,
    })
}

fn extract_outcome_names_from_market(market: &serde_json::Value) -> Option<Vec<String>> {
    for key in ["outcomes", "outcomeNames"] {
        if let Some(value) = market.get(key) {
            if let Some(names) = parse_outcome_names(value) {
                if !names.is_empty() {
                    return Some(names);
                }
            }
        }
    }
    None
}

fn parse_outcome_names(value: &serde_json::Value) -> Option<Vec<String>> {
    if let serde_json::Value::String(raw) = value {
        let inner: serde_json::Value = serde_json::from_str(raw).ok()?;
        return parse_outcome_names(&inner);
    }
    let items = value.as_array()?;
    let mut names = Vec::new();
    for item in items {
        match item {
            serde_json::Value::String(raw) => names.push(raw.clone()),
            serde_json::Value::Object(obj) => {
                for key in ["name", "title", "label"] {
                    if let Some(serde_json::Value::String(raw)) = obj.get(key) {
                        names.push(raw.clone());
                        break;
                    }
                }
            }
            _ => {}
        }
    }
    if names.is_empty() {
        None
    } else {
        Some(names)
    }
}

fn extract_token_candidates_from_market(
    market: &serde_json::Value,
    market_slug: &str,
) -> Result<Vec<TokenCandidate>> {
    let field_order = ["outcomeTokenIds", "clobTokenIds", "tokenIds"];
    let mut last_err: Option<anyhow::Error> = None;
    for field in field_order {
        if let Some(value) = market.get(field) {
            match parse_token_candidates(value) {
                Ok(candidates) => return Ok(candidates),
                Err(err) => last_err = Some(err),
            }
        }
    }

    let object_fields = [
        "outcomeTokens",
        "outcome_tokens",
        "tokens",
        "assets",
        "assetTokens",
        "asset_tokens",
    ];
    for field in object_fields {
        if let Some(value) = market.get(field) {
            match parse_token_candidates(value) {
                Ok(candidates) => return Ok(candidates),
                Err(err) => last_err = Some(err),
            }
        }
    }

    if let Some(obj) = market.as_object() {
        for value in obj.values() {
            if let serde_json::Value::Array(items) = value {
                if items.iter().any(|item| token_object_has_id(item)) {
                    match parse_token_candidates(value) {
                        Ok(candidates) => return Ok(candidates),
                        Err(err) => last_err = Some(err),
                    }
                }
            }
        }
    }

    if let Some(err) = last_err {
        return Err(err).context(format!("market slug={market_slug} invalid token id field"));
    }
    bail!("market slug={market_slug} missing token ids")
}

fn parse_token_candidates(value: &serde_json::Value) -> Result<Vec<TokenCandidate>> {
    match value {
        serde_json::Value::Array(items) => {
            let mut out = Vec::new();
            for item in items {
                match item {
                    serde_json::Value::String(raw) => out.push(TokenCandidate {
                        id: raw.clone(),
                        label: None,
                    }),
                    serde_json::Value::Number(num) => out.push(TokenCandidate {
                        id: num.to_string(),
                        label: None,
                    }),
                    serde_json::Value::Object(obj) => {
                        if let Some(candidate) = parse_token_candidate_object(obj) {
                            out.push(candidate);
                        }
                    }
                    _ => {}
                }
            }
            if out.len() != 2 {
                bail!("expected 2 token ids, got {}", out.len());
            }
            Ok(out)
        }
        serde_json::Value::String(raw) => {
            let inner: serde_json::Value =
                serde_json::from_str(raw).context("token id string is not valid JSON")?;
            parse_token_candidates(&inner)
        }
        _ => bail!("token ids must be array or json string"),
    }
}

fn parse_token_candidate_object(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Option<TokenCandidate> {
    let id_keys = [
        "id",
        "tokenId",
        "token_id",
        "clobTokenId",
        "clob_token_id",
        "outcomeTokenId",
        "outcome_token_id",
        "assetId",
        "asset_id",
    ];
    let mut id = None;
    for key in id_keys {
        if let Some(value) = obj.get(key) {
            match value {
                serde_json::Value::String(raw) => {
                    id = Some(raw.clone());
                    break;
                }
                serde_json::Value::Number(num) => {
                    id = Some(num.to_string());
                    break;
                }
                _ => {}
            }
        }
    }
    let id = id?;

    let label_keys = ["outcome", "outcomeName", "name", "label", "side", "title"];
    let mut label = None;
    for key in label_keys {
        if let Some(serde_json::Value::String(raw)) = obj.get(key) {
            label = Some(raw.clone());
            break;
        }
    }

    Some(TokenCandidate { id, label })
}

fn token_object_has_id(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(obj) => {
            let id_keys = [
                "id",
                "tokenId",
                "token_id",
                "clobTokenId",
                "clob_token_id",
                "outcomeTokenId",
                "outcome_token_id",
                "assetId",
                "asset_id",
            ];
            id_keys.iter().any(|key| obj.contains_key(*key))
        }
        _ => false,
    }
}

fn resolve_up_down_tokens(
    tokens: &[TokenCandidate],
    outcome_names: Option<&[String]>,
) -> Result<(String, String, &'static str)> {
    if tokens.len() != 2 {
        bail!("expected 2 token ids, got {}", tokens.len());
    }

    let token_labels: Option<Vec<String>> = if tokens.iter().all(|t| t.label.is_some()) {
        Some(
            tokens
                .iter()
                .map(|t| t.label.clone().unwrap_or_default())
                .collect(),
        )
    } else {
        None
    };

    if let Some(labels) = token_labels {
        return map_by_labels(&labels, tokens);
    }

    if let Some(labels) = outcome_names {
        return map_by_labels(labels, tokens);
    }

    bail!("missing outcome labels for up/down mapping")
}

fn map_by_labels(labels: &[String], tokens: &[TokenCandidate]) -> Result<(String, String, &'static str)> {
    if labels.len() != tokens.len() {
        bail!("outcome labels count mismatch: {} vs {}", labels.len(), tokens.len());
    }
    let mut up_idx = None;
    let mut down_idx = None;
    let mut mode = "up_down";
    for (idx, name) in labels.iter().enumerate() {
        let norm = name.trim().to_lowercase();
        if norm.contains("up") {
            up_idx = Some(idx);
        }
        if norm.contains("down") {
            down_idx = Some(idx);
        }
        if norm == "yes" {
            up_idx = Some(idx);
            mode = "yes_no";
        }
        if norm == "no" {
            down_idx = Some(idx);
            mode = "yes_no";
        }
    }
    match (up_idx, down_idx) {
        (Some(up), Some(down)) if up != down => Ok((
            tokens[up].id.clone(),
            tokens[down].id.clone(),
            mode,
        )),
        _ => bail!("outcomes do not include Up/Down"),
    }
}

fn parse_iso_to_unix(value: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt| dt.timestamp())
}

fn normalize(input: &str) -> String {
    input.trim().to_lowercase()
}

fn collect_series_slugs(event: &Event) -> Vec<String> {
    event
        .series
        .iter()
        .filter_map(|series| series.slug.as_deref())
        .map(|slug| slug.to_string())
        .collect()
}

fn push_recent_slug(recent: &mut VecDeque<String>, slug: &str) {
    if recent.len() >= 10 {
        recent.pop_front();
    }
    recent.push_back(slug.to_string());
}

fn sample_event_info(events: &[Event]) -> Option<SampleEventInfo> {
    let sample = events.iter().find(|event| {
        event.slug.is_some() || event.series_slug.is_some() || !event.series.is_empty()
    })?;
    let series0_slug = sample.series.get(0).and_then(|series| series.slug.clone());
    Some(SampleEventInfo {
        slug: sample.slug.clone(),
        series_slug: sample.series_slug.clone(),
        series0_slug,
        series_len: sample.series.len(),
        title: sample.title.clone(),
    })
}

fn is_better_pick(
    score: i64,
    updated_ts: Option<i64>,
    selected: &SelectedMarket,
    best: &BestPick,
) -> bool {
    if score != best.score {
        return score < best.score;
    }
    match (updated_ts, best.updated_ts) {
        (Some(a), Some(b)) if a != b => return a > b,
        (Some(_), None) => return true,
        (None, Some(_)) => return false,
        _ => {}
    }
    selected.slug > best.selected.slug
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn fixed_market_parses_up_down() {
        let value = json!([
            {
                "slug": "xrp-updown-15m-1768580100",
                "conditionId": "cond",
                "outcomes": ["Up", "Down"],
                "clobTokenIds": ["AAA", "BBB"]
            }
        ]);
        let result = parse_fixed_market_info_from_value(
            &value,
            "xrp-updown-15m-1768580100",
            "http://example/markets?slug=x",
            "",
        )
        .expect("parse fixed market");
        assert_eq!(result.up_token_id, "AAA");
        assert_eq!(result.down_token_id, "BBB");
    }

    #[test]
    fn fixed_market_parses_yes_no() {
        let value = json!([
            {
                "slug": "xrp-updown-15m-1768580100",
                "conditionId": "cond",
                "outcomes": ["Yes", "No"],
                "clobTokenIds": ["YES", "NO"]
            }
        ]);
        let result = parse_fixed_market_info_from_value(
            &value,
            "xrp-updown-15m-1768580100",
            "http://example/markets?slug=x",
            "",
        )
        .expect("parse fixed market");
        assert_eq!(result.up_token_id, "YES");
        assert_eq!(result.down_token_id, "NO");
    }

    #[test]
    fn fixed_market_outcomes_error_includes_json() {
        let value = json!([
            {
                "slug": "xrp-updown-15m-1768580100",
                "conditionId": "cond",
                "outcomes": ["Maybe"],
                "clobTokenIds": ["AAA", "BBB"]
            }
        ]);
        let err = parse_fixed_market_info_from_value(
            &value,
            "xrp-updown-15m-1768580100",
            "http://example/markets?slug=x",
            "",
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("outcomes="));
        assert!(err.contains("clobTokenIds="));
    }

    #[test]
    fn fixed_market_outcomes_string_parses() {
        let value = json!([
            {
                "slug": "xrp-updown-15m-1768580100",
                "conditionId": "cond",
                "outcomes": "[\"Up\", \"Down\"]",
                "clobTokenIds": ["AAA", "BBB"]
            }
        ]);
        let result = parse_fixed_market_info_from_value(
            &value,
            "xrp-updown-15m-1768580100",
            "http://example/markets?slug=x",
            "",
        )
        .expect("parse fixed market");
        assert_eq!(result.up_token_id, "AAA");
        assert_eq!(result.down_token_id, "BBB");
    }
}

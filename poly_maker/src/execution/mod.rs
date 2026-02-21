use anyhow::{bail, Context, Result};
use reqwest::{Client, Method, Url};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::env;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    Buy,
    Sell,
}

impl OrderSide {
    fn as_str(self) -> &'static str {
        match self {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        }
    }
}

#[derive(Debug, Clone)]
pub struct OrderIntent {
    pub market_slug: String,
    pub token_id: String,
    pub side: OrderSide,
    pub price: f64,
    pub qty: f64,
    pub post_only: bool,
    pub client_order_id: String,
}

#[derive(Debug, Clone)]
pub struct OpenOrder {
    pub order_id: String,
    pub client_order_id: String,
    pub market_slug: String,
    pub token_id: String,
    pub side: OrderSide,
    pub price: f64,
    pub qty: f64,
}

#[derive(Debug, Clone)]
pub struct PlaceAck {
    pub accepted: bool,
    pub order_id: Option<String>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CancelAck {
    pub canceled: bool,
    pub order_id: String,
    pub reason: Option<String>,
}

pub trait ExecutionAdapter: Send {
    fn place_post_only<'a>(&'a mut self, intent: OrderIntent) -> BoxFuture<'a, Result<PlaceAck>>;
    fn cancel<'a>(&'a mut self, order_id: &'a str) -> BoxFuture<'a, Result<CancelAck>>;
    fn cancel_all<'a>(&'a mut self, market_slug: &'a str) -> BoxFuture<'a, Result<usize>>;
    fn fetch_open_orders<'a>(
        &'a self,
        market_slug: &'a str,
    ) -> BoxFuture<'a, Result<Vec<OpenOrder>>>;
}

#[derive(Debug, Default)]
pub struct PaperExecutionAdapter {
    seq: u64,
    open_orders_by_market: HashMap<String, Vec<OpenOrder>>,
}

impl ExecutionAdapter for PaperExecutionAdapter {
    fn place_post_only<'a>(&'a mut self, intent: OrderIntent) -> BoxFuture<'a, Result<PlaceAck>> {
        Box::pin(async move {
            if !intent.post_only {
                return Ok(PlaceAck {
                    accepted: false,
                    order_id: None,
                    reason: Some("post_only_required".to_string()),
                });
            }
            if intent.qty <= 0.0 || intent.price <= 0.0 {
                return Ok(PlaceAck {
                    accepted: false,
                    order_id: None,
                    reason: Some("invalid_price_or_qty".to_string()),
                });
            }

            self.seq = self.seq.saturating_add(1);
            let order_id = format!("paper-{}", self.seq);
            let order = OpenOrder {
                order_id: order_id.clone(),
                client_order_id: intent.client_order_id,
                market_slug: intent.market_slug.clone(),
                token_id: intent.token_id,
                side: intent.side,
                price: intent.price,
                qty: intent.qty,
            };
            self.open_orders_by_market
                .entry(intent.market_slug)
                .or_default()
                .push(order);

            Ok(PlaceAck {
                accepted: true,
                order_id: Some(order_id),
                reason: None,
            })
        })
    }

    fn cancel<'a>(&'a mut self, order_id: &'a str) -> BoxFuture<'a, Result<CancelAck>> {
        Box::pin(async move {
            for orders in self.open_orders_by_market.values_mut() {
                if let Some(idx) = orders.iter().position(|o| o.order_id == order_id) {
                    orders.remove(idx);
                    return Ok(CancelAck {
                        canceled: true,
                        order_id: order_id.to_string(),
                        reason: None,
                    });
                }
            }

            Ok(CancelAck {
                canceled: false,
                order_id: order_id.to_string(),
                reason: Some("not_found".to_string()),
            })
        })
    }

    fn cancel_all<'a>(&'a mut self, market_slug: &'a str) -> BoxFuture<'a, Result<usize>> {
        Box::pin(async move {
            let removed = self
                .open_orders_by_market
                .remove(market_slug)
                .map(|orders| orders.len())
                .unwrap_or(0);
            Ok(removed)
        })
    }

    fn fetch_open_orders<'a>(
        &'a self,
        market_slug: &'a str,
    ) -> BoxFuture<'a, Result<Vec<OpenOrder>>> {
        Box::pin(async move {
            Ok(self
                .open_orders_by_market
                .get(market_slug)
                .cloned()
                .unwrap_or_default())
        })
    }
}

#[derive(Debug, Clone)]
pub struct ClobExecutionAdapter {
    pub clob_host: String,
    place_path: String,
    cancel_path: String,
    open_orders_path: String,
    cancel_use_post: bool,
    headers: HashMap<String, String>,
    http: Client,
}

impl ClobExecutionAdapter {
    pub fn from_env(clob_host: String) -> Self {
        let api_key = env::var("CLOB_API_KEY").unwrap_or_default();
        let api_secret = env::var("CLOB_API_SECRET").unwrap_or_default();
        let api_passphrase = env::var("CLOB_API_PASSPHRASE").unwrap_or_default();
        let key_header =
            env::var("CLOB_API_KEY_HEADER").unwrap_or_else(|_| "POLY_API_KEY".to_string());
        let secret_header =
            env::var("CLOB_API_SECRET_HEADER").unwrap_or_else(|_| "POLY_API_SECRET".to_string());
        let passphrase_header = env::var("CLOB_API_PASSPHRASE_HEADER")
            .unwrap_or_else(|_| "POLY_PASSPHRASE".to_string());
        let place_path = env::var("CLOB_PLACE_ORDER_PATH").unwrap_or_else(|_| "/order".to_string());
        let cancel_path =
            env::var("CLOB_CANCEL_ORDER_PATH").unwrap_or_else(|_| "/order".to_string());
        let open_orders_path =
            env::var("CLOB_OPEN_ORDERS_PATH").unwrap_or_else(|_| "/orders".to_string());
        let cancel_use_post = matches!(
            env::var("CLOB_CANCEL_USE_POST")
                .unwrap_or_else(|_| "false".to_string())
                .trim()
                .to_ascii_lowercase()
                .as_str(),
            "1" | "true" | "yes" | "on"
        );

        let mut headers = HashMap::new();
        if !api_key.is_empty() {
            headers.insert(key_header, api_key);
        }
        if !api_secret.is_empty() {
            headers.insert(secret_header, api_secret);
        }
        if !api_passphrase.is_empty() {
            headers.insert(passphrase_header, api_passphrase);
        }
        if let Ok(extra_headers_raw) = env::var("CLOB_EXTRA_HEADERS_JSON") {
            if let Ok(extra) = serde_json::from_str::<HashMap<String, String>>(&extra_headers_raw) {
                for (k, v) in extra {
                    headers.insert(k, v);
                }
            }
        }

        let http = Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap_or_else(|_| Client::new());

        Self {
            clob_host,
            place_path,
            cancel_path,
            open_orders_path,
            cancel_use_post,
            headers,
            http,
        }
    }

    fn build_url(&self, path: &str) -> Result<Url> {
        let base = self.clob_host.trim_end_matches('/');
        let path = if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{path}")
        };
        let full = format!("{base}{path}");
        Url::parse(&full).with_context(|| format!("invalid url: {full}"))
    }

    fn with_headers(&self, mut builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        for (k, v) in &self.headers {
            builder = builder.header(k, v);
        }
        builder
    }

    async fn request_json(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
        query: Option<&[(String, String)]>,
    ) -> Result<Value> {
        let url = self.build_url(path)?;
        let mut builder = self.http.request(method, url);
        builder = self.with_headers(builder);
        if let Some(query) = query {
            builder = builder.query(query);
        }
        if let Some(body) = body {
            builder = builder.json(&body);
        }
        let resp = builder.send().await.context("clob request send failed")?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .context("clob response text decode failed")?;
        if !status.is_success() {
            bail!("clob http error status={} body={}", status, text);
        }
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&text).context("clob response is not valid json")
    }

    fn parse_order_id(value: &Value) -> Option<String> {
        value
            .get("order_id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| value.get("id").and_then(Value::as_str).map(str::to_string))
            .or_else(|| {
                value
                    .get("data")
                    .and_then(|d| d.get("order_id"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
    }

    fn parse_reason(value: &Value) -> Option<String> {
        value
            .get("reason")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| {
                value
                    .get("message")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .or_else(|| {
                value
                    .get("error")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
    }

    fn parse_bool(value: Option<&Value>, default: bool) -> bool {
        match value {
            Some(Value::Bool(v)) => *v,
            Some(Value::String(v)) => matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "ok" | "accepted"
            ),
            Some(Value::Number(v)) => v.as_i64().unwrap_or(0) != 0,
            _ => default,
        }
    }

    fn parse_num(value: Option<&Value>) -> Option<f64> {
        match value {
            Some(Value::Number(v)) => v.as_f64(),
            Some(Value::String(v)) => v.parse::<f64>().ok(),
            _ => None,
        }
    }

    fn parse_open_orders(value: &Value) -> Vec<OpenOrder> {
        let list = if let Some(list) = value.as_array() {
            list
        } else if let Some(list) = value.get("data").and_then(Value::as_array) {
            list
        } else if let Some(list) = value.get("orders").and_then(Value::as_array) {
            list
        } else if let Some(list) = value.get("results").and_then(Value::as_array) {
            list
        } else {
            return Vec::new();
        };

        let mut out = Vec::new();
        for item in list {
            let order_id = item
                .get("order_id")
                .and_then(Value::as_str)
                .or_else(|| item.get("id").and_then(Value::as_str))
                .map(str::to_string)
                .unwrap_or_default();
            if order_id.is_empty() {
                continue;
            }
            let side = match item
                .get("side")
                .and_then(Value::as_str)
                .unwrap_or("BUY")
                .to_ascii_lowercase()
                .as_str()
            {
                "sell" => OrderSide::Sell,
                _ => OrderSide::Buy,
            };
            out.push(OpenOrder {
                order_id,
                client_order_id: item
                    .get("client_order_id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_default(),
                market_slug: item
                    .get("market_slug")
                    .and_then(Value::as_str)
                    .or_else(|| item.get("market").and_then(Value::as_str))
                    .map(str::to_string)
                    .unwrap_or_default(),
                token_id: item
                    .get("token_id")
                    .and_then(Value::as_str)
                    .or_else(|| item.get("asset_id").and_then(Value::as_str))
                    .map(str::to_string)
                    .unwrap_or_default(),
                side,
                price: Self::parse_num(item.get("price")).unwrap_or(0.0),
                qty: Self::parse_num(item.get("size").or_else(|| item.get("qty"))).unwrap_or(0.0),
            });
        }
        out
    }
}

impl ExecutionAdapter for ClobExecutionAdapter {
    fn place_post_only<'a>(&'a mut self, intent: OrderIntent) -> BoxFuture<'a, Result<PlaceAck>> {
        Box::pin(async move {
            if !intent.post_only {
                return Ok(PlaceAck {
                    accepted: false,
                    order_id: None,
                    reason: Some("post_only_required".to_string()),
                });
            }
            if intent.qty <= 0.0 || intent.price <= 0.0 {
                return Ok(PlaceAck {
                    accepted: false,
                    order_id: None,
                    reason: Some("invalid_price_or_qty".to_string()),
                });
            }

            let body = json!({
                "market_slug": intent.market_slug,
                "token_id": intent.token_id,
                "side": intent.side.as_str(),
                "price": intent.price,
                "size": intent.qty,
                "qty": intent.qty,
                "order_type": "GTC",
                "time_in_force": "GTC",
                "post_only": true,
                "client_order_id": intent.client_order_id,
            });

            let value = self
                .request_json(Method::POST, &self.place_path, Some(body), None)
                .await
                .context("place_post_only failed")?;
            let order_id = Self::parse_order_id(&value);
            let accepted = Self::parse_bool(value.get("accepted"), order_id.is_some());
            let reason = Self::parse_reason(&value);
            Ok(PlaceAck {
                accepted,
                order_id,
                reason,
            })
        })
    }

    fn cancel<'a>(&'a mut self, order_id: &'a str) -> BoxFuture<'a, Result<CancelAck>> {
        Box::pin(async move {
            if order_id.trim().is_empty() {
                return Ok(CancelAck {
                    canceled: false,
                    order_id: order_id.to_string(),
                    reason: Some("empty_order_id".to_string()),
                });
            }

            let value = if self.cancel_use_post {
                let body = json!({"order_id": order_id});
                self.request_json(Method::POST, &self.cancel_path, Some(body), None)
                    .await
            } else {
                let path = format!("{}/{}", self.cancel_path.trim_end_matches('/'), order_id);
                self.request_json(Method::DELETE, &path, None, None).await
            }
            .context("cancel request failed")?;

            let canceled = Self::parse_bool(value.get("canceled"), true);
            let reason = Self::parse_reason(&value);
            Ok(CancelAck {
                canceled,
                order_id: order_id.to_string(),
                reason,
            })
        })
    }

    fn cancel_all<'a>(&'a mut self, market_slug: &'a str) -> BoxFuture<'a, Result<usize>> {
        Box::pin(async move {
            let open_orders = self.fetch_open_orders(market_slug).await?;
            let mut canceled = 0usize;
            for order in open_orders {
                let ack = self
                    .cancel(&order.order_id)
                    .await
                    .with_context(|| format!("cancel order {} failed", order.order_id))?;
                if ack.canceled {
                    canceled += 1;
                }
            }
            Ok(canceled)
        })
    }

    fn fetch_open_orders<'a>(
        &'a self,
        market_slug: &'a str,
    ) -> BoxFuture<'a, Result<Vec<OpenOrder>>> {
        Box::pin(async move {
            let query = vec![
                ("market_slug".to_string(), market_slug.to_string()),
                ("market".to_string(), market_slug.to_string()),
                ("status".to_string(), "open".to_string()),
            ];
            let value = self
                .request_json(Method::GET, &self.open_orders_path, None, Some(&query))
                .await
                .context("fetch_open_orders failed")?;
            Ok(Self::parse_open_orders(&value))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn paper_adapter_place_and_cancel() {
        let mut adapter = PaperExecutionAdapter::default();
        let ack = adapter
            .place_post_only(OrderIntent {
                market_slug: "btc-updown-15m-123".to_string(),
                token_id: "token-up".to_string(),
                side: OrderSide::Buy,
                price: 0.42,
                qty: 12.0,
                post_only: true,
                client_order_id: "cid-1".to_string(),
            })
            .await
            .expect("place should succeed");
        assert!(ack.accepted);
        let order_id = ack.order_id.expect("order id");

        let open = adapter
            .fetch_open_orders("btc-updown-15m-123")
            .await
            .expect("fetch open orders");
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].order_id, order_id);

        let canceled = adapter
            .cancel(&order_id)
            .await
            .expect("cancel should succeed");
        assert!(canceled.canceled);

        let open_after = adapter
            .fetch_open_orders("btc-updown-15m-123")
            .await
            .expect("fetch open orders");
        assert!(open_after.is_empty());
    }
}

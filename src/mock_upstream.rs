use std::sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering},
};
use std::time::Instant;

use axum::{
    Json, Router,
    body::Bytes,
    extract::State,
    http::{HeaderValue, StatusCode, header::RETRY_AFTER},
    response::{IntoResponse, Response},
    routing::post,
};
use serde_json::{Value, json};
use tokio::time::{Duration, sleep};

const RATE_LIMIT_DISABLED: u64 = u64::MAX;

#[derive(Clone, Debug)]
pub struct MockBehavior {
    pub chain_id: u64,
    pub block_number: u64,
    pub block_lag: u64,
    pub rate_limit_after: Option<u64>,
    pub retry_after_seconds: Option<u64>,
    pub rate_limit_message: Option<String>,
    pub html: bool,
    pub delay_ms: u64,
    pub status_5xx: Option<u16>,
    pub execution_reverted: bool,
}

impl Default for MockBehavior {
    fn default() -> Self {
        Self {
            chain_id: 1,
            block_number: 1_000_000,
            block_lag: 0,
            rate_limit_after: None,
            retry_after_seconds: None,
            rate_limit_message: None,
            html: false,
            delay_ms: 0,
            status_5xx: None,
            execution_reverted: false,
        }
    }
}

struct MockInner {
    chain_id: AtomicU64,
    block_number: AtomicU64,
    block_lag: AtomicU64,
    rate_limit_after: AtomicU64,
    retry_after_seconds: AtomicU64,
    rate_limit_message: Mutex<Option<String>>,
    html: AtomicBool,
    delay_ms: AtomicU64,
    status_5xx: AtomicU16,
    execution_reverted: AtomicBool,
    requests: AtomicU64,
    rate_stats: Mutex<RateStats>,
}

struct RateStats {
    started: Instant,
    second: u64,
    current: u64,
    maximum: u64,
}

impl Default for RateStats {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            second: 0,
            current: 0,
            maximum: 0,
        }
    }
}

impl RateStats {
    fn record(&mut self) {
        let second = self.started.elapsed().as_secs();
        if second != self.second {
            self.second = second;
            self.current = 0;
        }
        self.current = self.current.saturating_add(1);
        self.maximum = self.maximum.max(self.current);
    }
}

#[derive(Clone)]
pub struct MockController {
    inner: Arc<MockInner>,
}

impl MockController {
    pub fn new(behavior: MockBehavior) -> Self {
        Self {
            inner: Arc::new(MockInner {
                chain_id: AtomicU64::new(behavior.chain_id),
                block_number: AtomicU64::new(behavior.block_number),
                block_lag: AtomicU64::new(behavior.block_lag),
                rate_limit_after: AtomicU64::new(
                    behavior.rate_limit_after.unwrap_or(RATE_LIMIT_DISABLED),
                ),
                retry_after_seconds: AtomicU64::new(behavior.retry_after_seconds.unwrap_or(0)),
                rate_limit_message: Mutex::new(behavior.rate_limit_message),
                html: AtomicBool::new(behavior.html),
                delay_ms: AtomicU64::new(behavior.delay_ms),
                status_5xx: AtomicU16::new(behavior.status_5xx.unwrap_or(0)),
                execution_reverted: AtomicBool::new(behavior.execution_reverted),
                requests: AtomicU64::new(0),
                rate_stats: Mutex::new(RateStats::default()),
            }),
        }
    }

    pub fn request_count(&self) -> u64 {
        self.inner.requests.load(Ordering::SeqCst)
    }

    pub fn reset_request_count(&self) {
        self.inner.requests.store(0, Ordering::SeqCst);
        *lock(&self.inner.rate_stats) = RateStats::default();
    }

    pub fn max_requests_per_second(&self) -> u64 {
        lock(&self.inner.rate_stats).maximum
    }

    pub fn set_rate_limit_after(&self, after: Option<u64>) {
        self.inner
            .rate_limit_after
            .store(after.unwrap_or(RATE_LIMIT_DISABLED), Ordering::SeqCst);
    }

    pub fn set_rate_limit_message(&self, message: Option<String>) {
        *lock(&self.inner.rate_limit_message) = message;
    }

    pub fn set_html(&self, enabled: bool) {
        self.inner.html.store(enabled, Ordering::SeqCst);
    }

    pub fn set_status_5xx(&self, status: Option<u16>) {
        self.inner
            .status_5xx
            .store(status.unwrap_or(0), Ordering::SeqCst);
    }

    pub fn set_chain_id(&self, chain_id: u64) {
        self.inner.chain_id.store(chain_id, Ordering::SeqCst);
    }

    pub fn set_block_number(&self, block_number: u64) {
        self.inner
            .block_number
            .store(block_number, Ordering::SeqCst);
    }

    pub fn set_block_lag(&self, block_lag: u64) {
        self.inner.block_lag.store(block_lag, Ordering::SeqCst);
    }

    pub fn set_delay_ms(&self, delay_ms: u64) {
        self.inner.delay_ms.store(delay_ms, Ordering::SeqCst);
    }

    pub fn set_execution_reverted(&self, enabled: bool) {
        self.inner
            .execution_reverted
            .store(enabled, Ordering::SeqCst);
    }
}

pub fn router(controller: MockController) -> Router {
    Router::new()
        .route("/", post(handle_rpc))
        .with_state(controller)
}

async fn handle_rpc(State(controller): State<MockController>, body: Bytes) -> Response {
    let request_number = controller.inner.requests.fetch_add(1, Ordering::SeqCst) + 1;
    lock(&controller.inner.rate_stats).record();
    let delay_ms = controller.inner.delay_ms.load(Ordering::SeqCst);
    if delay_ms != 0 {
        sleep(Duration::from_millis(delay_ms)).await;
    }

    let status_5xx = controller.inner.status_5xx.load(Ordering::SeqCst);
    if status_5xx != 0 {
        return StatusCode::from_u16(status_5xx)
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
            .into_response();
    }
    if controller.inner.html.load(Ordering::SeqCst) {
        return (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "text/html")],
            "<html><body>mock upstream error</body></html>",
        )
            .into_response();
    }

    let rate_limit_after = controller.inner.rate_limit_after.load(Ordering::SeqCst);
    if rate_limit_after != RATE_LIMIT_DISABLED && request_number > rate_limit_after {
        let mut response = (StatusCode::TOO_MANY_REQUESTS, "mock rate limit").into_response();
        let retry_after = controller.inner.retry_after_seconds.load(Ordering::SeqCst);
        if retry_after != 0
            && let Ok(value) = HeaderValue::from_str(&retry_after.to_string())
        {
            response.headers_mut().insert(RETRY_AFTER, value);
        }
        return response;
    }

    let request: Value = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => {
            return Json(json!({
                "jsonrpc":"2.0",
                "id":null,
                "error":{"code":-32700,"message":"parse error"}
            }))
            .into_response();
        }
    };
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    if let Some(message) = lock(&controller.inner.rate_limit_message).clone() {
        return Json(json!({
            "jsonrpc":"2.0",
            "id":id,
            "error":{"code":-32005,"message":message}
        }))
        .into_response();
    }
    if controller.inner.execution_reverted.load(Ordering::SeqCst) {
        return Json(json!({
            "jsonrpc":"2.0",
            "id":id,
            "error":{"code":3,"message":"execution reverted: mock rejection"}
        }))
        .into_response();
    }

    let result = match request.get("method").and_then(Value::as_str) {
        Some("eth_chainId") => format!("0x{:x}", controller.inner.chain_id.load(Ordering::SeqCst)),
        Some("eth_blockNumber") => format!(
            "0x{:x}",
            controller
                .inner
                .block_number
                .load(Ordering::SeqCst)
                .saturating_sub(controller.inner.block_lag.load(Ordering::SeqCst))
        ),
        _ => "0x1".to_owned(),
    };
    Json(json!({"jsonrpc":"2.0", "id":id, "result":result})).into_response()
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn rate_limit_threshold_is_configurable() {
        let controller = MockController::new(MockBehavior {
            rate_limit_after: Some(1),
            ..MockBehavior::default()
        });
        let app = router(controller);
        let request = || {
            Request::post("/")
                .body(Body::from(
                    json!({"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]})
                        .to_string(),
                ))
                .expect("request")
        };
        assert_eq!(
            app.clone()
                .oneshot(request())
                .await
                .expect("first")
                .status(),
            StatusCode::OK
        );
        let limited = app.oneshot(request()).await.expect("second");
        assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
        let _ = to_bytes(limited.into_body(), usize::MAX)
            .await
            .expect("body");
    }
}

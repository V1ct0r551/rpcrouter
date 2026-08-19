use std::sync::Arc;

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::{StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json, value::RawValue};
use tokio::{sync::Semaphore, task::JoinSet};
use tracing::error;

use crate::{
    forward::{Forwarder, all_endpoints_exhausted},
    guard::{self, BearerAuth, IpRateLimiter},
    metrics::Metrics,
    registry::Registry,
};

#[derive(Clone)]
pub struct AppState {
    registry: Arc<Registry>,
    forwarder: Arc<Forwarder>,
    metrics: Arc<Metrics>,
    batch_limit: usize,
    metrics_enabled: bool,
    max_body_bytes: usize,
    concurrency: Arc<Semaphore>,
    rate_limiter: Option<Arc<IpRateLimiter>>,
    metrics_auth: Option<BearerAuth>,
}

impl AppState {
    pub fn new(registry: Arc<Registry>, forwarder: Arc<Forwarder>, batch_limit: usize) -> Self {
        let metrics = forwarder.metrics();
        Self {
            registry,
            forwarder,
            metrics,
            batch_limit,
            metrics_enabled: true,
            max_body_bytes: crate::config::DEFAULT_MAX_BODY_BYTES,
            concurrency: Arc::new(Semaphore::new(
                crate::config::DEFAULT_MAX_CONCURRENT_REQUESTS,
            )),
            rate_limiter: None,
            metrics_auth: None,
        }
    }

    pub fn with_metrics_enabled(mut self, enabled: bool) -> Self {
        self.metrics_enabled = enabled;
        self
    }

    /// 应用服务器加固参数（请求体上限、并发上限、每 IP 限速、/metrics 鉴权）。
    pub fn with_hardening(
        mut self,
        max_body_bytes: usize,
        max_concurrent_requests: usize,
        per_ip_rate_limit: Option<(u64, u64)>,
        metrics_auth_token: Option<String>,
    ) -> Self {
        self.max_body_bytes = max_body_bytes;
        self.concurrency = Arc::new(Semaphore::new(max_concurrent_requests));
        self.rate_limiter =
            per_ip_rate_limit.map(|(rps, burst)| Arc::new(IpRateLimiter::new(rps, burst)));
        self.metrics_auth = metrics_auth_token.map(|token| BearerAuth::new(&token));
        self
    }
}

pub fn router(state: AppState) -> Router {
    let mut router = Router::new()
        .route("/healthz", get(healthz))
        .route("/chains", get(chains))
        .route("/rpc/{chain_id}", post(rpc));

    // /metrics 支持可选的 bearer token 鉴权。
    if let Some(auth) = state.metrics_auth.clone() {
        router = router.route("/metrics", get(metrics).layer(auth));
    } else {
        router = router.route("/metrics", get(metrics));
    }

    router.with_state(state)
}

/// 构建带入口防护层（请求体上限/并发背压/每 IP 限速）的服务。
///
/// 防护层必须包在 Router 外层（Router::layer 需要 Sync，而防护层不是），
/// 故用 tower::ServiceBuilder 直接包裹 Router 服务。
pub fn guarded_service_from_state(
    state: AppState,
) -> tower::util::BoxCloneService<
    axum::http::Request<axum::body::Body>,
    Response,
    std::convert::Infallible,
> {
    let guard_metrics = Arc::clone(&state.metrics);
    let concurrency = Arc::clone(&state.concurrency);
    let rate_limiter = state.rate_limiter.clone();
    let max_body_bytes = state.max_body_bytes;
    let router = router(state);
    tower::ServiceBuilder::new()
        .layer_fn(move |inner| {
            guard::guarded_service(
                inner,
                max_body_bytes,
                Arc::clone(&concurrency),
                Arc::clone(&guard_metrics),
                rate_limiter.clone(),
            )
        })
        .service(router)
}

async fn healthz() -> Json<Value> {
    Json(json!({"status": "ok"}))
}

async fn chains(State(state): State<AppState>) -> Json<Value> {
    Json(json!({"chains": state.registry.summaries().await}))
}

async fn metrics(State(state): State<AppState>) -> Response {
    if !state.metrics_enabled {
        return StatusCode::NOT_FOUND.into_response();
    }
    match state.metrics.encode(&state.registry).await {
        Ok(body) => (
            StatusCode::OK,
            [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
            body,
        )
            .into_response(),
        Err(error) => {
            error!(error = %error, "metrics encoding failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn rpc(Path(chain_id): Path<u64>, State(state): State<AppState>, body: Bytes) -> Json<Value> {
    let request: Box<RawValue> = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => return Json(jsonrpc_error(Value::Null, -32700, "Parse error")),
    };

    match request.get().trim_start().as_bytes().first() {
        Some(b'{') => Json(state.forwarder.execute_raw(chain_id, &request).await),
        Some(b'[') => {
            let requests: Vec<Box<RawValue>> = match serde_json::from_str(request.get()) {
                Ok(requests) => requests,
                Err(_) => return Json(jsonrpc_error(Value::Null, -32700, "Parse error")),
            };
            if requests.is_empty() {
                return Json(jsonrpc_error(
                    Value::Null,
                    -32600,
                    "Invalid Request: batch must not be empty",
                ));
            }
            if requests.len() > state.batch_limit {
                return Json(jsonrpc_error(
                    Value::Null,
                    -32600,
                    &format!(
                        "Invalid Request: batch exceeds limit of {}",
                        state.batch_limit
                    ),
                ));
            }
            Json(execute_batch(chain_id, &state, requests).await)
        }
        _ => Json(jsonrpc_error(Value::Null, -32600, "Invalid Request")),
    }
}

async fn execute_batch(chain_id: u64, state: &AppState, requests: Vec<Box<RawValue>>) -> Value {
    let mut results = vec![None; requests.len()];
    let request_ids: Vec<_> = requests.iter().map(|request| request_id(request)).collect();
    let mut tasks = JoinSet::new();

    for (index, request) in requests.into_iter().enumerate() {
        if !request.get().trim_start().starts_with('{') {
            results[index] = Some(jsonrpc_error(Value::Null, -32600, "Invalid Request"));
            continue;
        }
        let forwarder = Arc::clone(&state.forwarder);
        tasks.spawn(async move { (index, forwarder.execute_raw(chain_id, &request).await) });
    }

    while let Some(task) = tasks.join_next().await {
        match task {
            Ok((index, response)) => results[index] = Some(response),
            Err(join_error) => error!(error = %join_error, "batch forwarding task failed"),
        }
    }

    Value::Array(
        results
            .into_iter()
            .enumerate()
            .map(|(index, response)| {
                response.unwrap_or_else(|| {
                    all_endpoints_exhausted(chain_id, request_ids[index].clone())
                })
            })
            .collect(),
    )
}

fn request_id(request: &RawValue) -> Value {
    #[derive(Deserialize)]
    struct IdOnly {
        #[serde(default)]
        id: Value,
    }

    serde_json::from_str::<IdOnly>(request.get()).map_or(Value::Null, |request| request.id)
}

fn jsonrpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": code, "message": message}
    })
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use axum::{
        body::{Body, to_bytes},
        extract::{Path, State},
        http::{Request, StatusCode, header::CONTENT_TYPE},
        response::{IntoResponse, Response},
    };
    use tower::ServiceExt;

    use crate::{
        chainlist::{ChainEndpoints, ChainlistSnapshot},
        config::{Config, UpstreamConfig},
    };

    use super::*;

    #[derive(Clone)]
    struct MockState(Arc<AtomicUsize>);

    async fn mock_upstream(
        Path(behavior): Path<String>,
        State(state): State<MockState>,
        body: Bytes,
    ) -> Response {
        state.0.fetch_add(1, Ordering::SeqCst);
        match behavior.as_str() {
            "limited" => StatusCode::TOO_MANY_REQUESTS.into_response(),
            "failure" => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            "html" => (
                StatusCode::OK,
                [(CONTENT_TYPE, "text/html")],
                "<html>not an RPC response</html>",
            )
                .into_response(),
            "slow" => {
                tokio::time::sleep(Duration::from_millis(80)).await;
                Json(json!({"jsonrpc":"2.0", "id":1, "result":"too late"})).into_response()
            }
            "rpc" => {
                let request: Value = serde_json::from_slice(&body).expect("single JSON request");
                assert!(request.is_object(), "batch must be split before forwarding");
                let method = request["method"].as_str().unwrap_or_default();
                match method {
                    "slow_method" => tokio::time::sleep(Duration::from_millis(30)).await,
                    "medium_method" => tokio::time::sleep(Duration::from_millis(15)).await,
                    _ => {}
                }
                Json(json!({
                    "jsonrpc": "2.0",
                    "id": request.get("id").cloned().unwrap_or(Value::Null),
                    "result": method
                }))
                .into_response()
            }
            _ => StatusCode::NOT_FOUND.into_response(),
        }
    }

    async fn spawn_mock() -> (String, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/{behavior}", post(mock_upstream))
            .with_state(MockState(Arc::clone(&calls)));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind local mock");
        let address = listener.local_addr().expect("mock address");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve mock");
        });
        (format!("http://{address}"), calls)
    }

    fn test_config(timeout_ms: u64) -> Config {
        Config {
            chains: vec![1],
            chain_overrides: Vec::new(),
            upstream: UpstreamConfig {
                request_timeout_ms: timeout_ms,
                slow_threshold_ms: 50,
                deadline_ms: 500,
                max_attempts: 4,
                default_rps: 100,
                default_concurrency: 8,
            },
            ..Config::default()
        }
    }

    async fn test_app(urls: &[String], timeout_ms: u64) -> Router {
        let config = test_config(timeout_ms);
        let registry = Arc::new(Registry::new(&config));
        registry
            .apply_snapshot(&ChainlistSnapshot {
                chains: vec![ChainEndpoints {
                    chain_id: 1,
                    name: "Test Chain".to_owned(),
                    endpoints: urls.to_vec(),
                }],
            })
            .await;
        for endpoint in registry.all_endpoints(1).await {
            let latency = if endpoint.url().ends_with("/failure") {
                Duration::from_millis(1)
            } else {
                Duration::from_millis(10)
            };
            endpoint.record_success(tokio::time::Instant::now(), latency, true);
            endpoint.record_success(tokio::time::Instant::now(), latency, true);
        }
        let forwarder = Arc::new(
            Forwarder::new(Arc::clone(&registry), &config).expect("create test forwarder"),
        );
        router(AppState::new(
            registry,
            forwarder,
            config.server.batch_limit,
        ))
    }

    async fn post_json(app: Router, path: &str, value: Value) -> Value {
        let response = app
            .oneshot(
                Request::post(path)
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(value.to_string()))
                    .expect("request"),
            )
            .await
            .expect("router response");
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        serde_json::from_slice(&bytes).expect("JSON response")
    }

    #[tokio::test]
    async fn retries_a_different_endpoint_after_failure() {
        let (base, calls) = spawn_mock().await;
        let app = test_app(&[format!("{base}/failure"), format!("{base}/rpc")], 100).await;
        let response = post_json(
            app,
            "/rpc/1",
            json!({"jsonrpc":"2.0", "id":9, "method":"eth_blockNumber", "params":[]}),
        )
        .await;
        assert_eq!(response["id"], 9);
        assert_eq!(response["result"], "eth_blockNumber");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn returns_gateway_error_after_all_failure_signals() {
        let (base, calls) = spawn_mock().await;
        let app = test_app(
            &[
                format!("{base}/limited"),
                format!("{base}/failure"),
                format!("{base}/html"),
                format!("{base}/slow"),
            ],
            20,
        )
        .await;
        let response = post_json(
            app,
            "/rpc/1",
            json!({"jsonrpc":"2.0", "id":"request-a", "method":"eth_blockNumber", "params":[]}),
        )
        .await;
        assert_eq!(response["id"], "request-a");
        assert_eq!(response["error"]["code"], -32000);
        assert_eq!(
            response["error"]["message"],
            "rpcrouter: all upstream endpoints exhausted for chain 1"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn splits_batch_and_restores_original_order() {
        let (base, calls) = spawn_mock().await;
        let app = test_app(&[format!("{base}/rpc")], 100).await;
        let response = post_json(
            app,
            "/rpc/1",
            json!([
                {"jsonrpc":"2.0", "id":1, "method":"slow_method", "params":[]},
                {"jsonrpc":"2.0", "id":2, "method":"fast_method", "params":[]},
                {"jsonrpc":"2.0", "id":3, "method":"medium_method", "params":[]}
            ]),
        )
        .await;
        let responses = response.as_array().expect("batch response");
        assert_eq!(responses[0]["id"], 1);
        assert_eq!(responses[1]["id"], 2);
        assert_eq!(responses[2]["id"], 3);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn exposes_health_and_chain_pool_summary() {
        let (base, _) = spawn_mock().await;
        let app = test_app(&[format!("{base}/rpc")], 100).await;
        let health = app
            .clone()
            .oneshot(
                Request::get("/healthz")
                    .body(Body::empty())
                    .expect("health request"),
            )
            .await
            .expect("health response");
        assert_eq!(health.status(), StatusCode::OK);

        let chains = app
            .clone()
            .oneshot(
                Request::get("/chains")
                    .body(Body::empty())
                    .expect("chains request"),
            )
            .await
            .expect("chains response");
        let body = to_bytes(chains.into_body(), usize::MAX)
            .await
            .expect("chains body");
        let value: Value = serde_json::from_slice(&body).expect("chains JSON");
        assert_eq!(value["chains"][0]["chainId"], 1);
        assert_eq!(value["chains"][0]["active"], 1);

        let metrics = app
            .oneshot(
                Request::get("/metrics")
                    .body(Body::empty())
                    .expect("metrics request"),
            )
            .await
            .expect("metrics response");
        assert_eq!(metrics.status(), StatusCode::OK);
        assert_eq!(
            metrics.headers()[CONTENT_TYPE],
            "text/plain; version=0.0.4; charset=utf-8"
        );
        let body = to_bytes(metrics.into_body(), usize::MAX)
            .await
            .expect("metrics body");
        let body = String::from_utf8(body.to_vec()).expect("metrics text");
        assert!(body.contains("rpcrouter_endpoint_state"));
    }
}

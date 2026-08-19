//! 入口防护（P2.2）+ /metrics 鉴权（P2.6）离线测试。
//!
//! 全部使用本地 mock 上游，禁外网。

use std::{net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    body::{Body, to_bytes},
    extract::ConnectInfo,
    http::{Request, StatusCode, header::CONTENT_TYPE},
    response::Response,
};
use rpcrouter::{
    chainlist::{ChainEndpoints, ChainlistSnapshot},
    config::{Config, UpstreamConfig},
    forward::Forwarder,
    mock_upstream::{MockBehavior, MockController, router as mock_router},
    registry::{EndpointState, Registry},
    server::{AppState, guarded_service_from_state, router as app_router},
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, time::Instant};
use tower::ServiceExt;

async fn spawn_mock(behavior: MockBehavior) -> (String, MockController) {
    let controller = MockController::new(behavior);
    let app = mock_router(controller.clone());
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock upstream");
    let address = listener.local_addr().expect("mock address");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve mock upstream");
    });
    (format!("http://{address}/"), controller)
}

fn config() -> Config {
    Config {
        chains: vec![1],
        chain_overrides: Vec::new(),
        upstream: UpstreamConfig {
            request_timeout_ms: 1000,
            slow_threshold_ms: 800,
            deadline_ms: 2000,
            max_attempts: 4,
            default_rps: 100,
            default_concurrency: 64,
        },
        ..Config::default()
    }
}

/// 构建一个连到本地 mock 的、带指定防护参数的 guard 服务。
async fn guarded_app(
    mock_url: &str,
    max_body_bytes: usize,
    max_concurrent: usize,
    rate_limit: Option<(u64, u64)>,
    metrics_token: Option<String>,
) -> tower::util::BoxCloneService<Request<Body>, Response, std::convert::Infallible> {
    let cfg = config();
    let registry = Arc::new(Registry::new(&cfg));
    registry
        .apply_snapshot(&ChainlistSnapshot {
            chains: vec![ChainEndpoints {
                chain_id: 1,
                name: "Hardening Test".to_owned(),
                endpoints: vec![mock_url.to_owned()],
            }],
        })
        .await;
    let endpoint = registry.endpoint(1, mock_url).await.expect("test endpoint");
    let now = Instant::now();
    endpoint.record_success(now, Duration::from_millis(1), true);
    endpoint.record_success(now, Duration::from_millis(1), true);
    assert_eq!(endpoint.state(now), EndpointState::Active);
    let forwarder = Arc::new(Forwarder::new(Arc::clone(&registry), &cfg).expect("forwarder"));

    let state = AppState::new(registry, forwarder, 10).with_hardening(
        max_body_bytes,
        max_concurrent,
        rate_limit,
        metrics_token,
    );
    guarded_service_from_state(state)
}

fn rpc_request(id: u64) -> Request<Body> {
    Request::post("/rpc/1")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({"jsonrpc":"2.0","id":id,"method":"eth_blockNumber","params":[]}).to_string(),
        ))
        .expect("request")
}

async fn body_json(response: Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    serde_json::from_slice(&bytes).expect("JSON body")
}

#[tokio::test]
async fn oversized_body_is_rejected_with_413_and_jsonrpc_error() {
    let (url, _) = spawn_mock(MockBehavior::default()).await;
    let app = guarded_app(&url, 64, 1024, None, None).await;

    // 构建一个超过 64 字节的合法 JSON-RPC 请求体。
    let big_params: Vec<Value> = (0..200).map(|i| json!(i)).collect();
    let body = json!({"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":big_params});
    assert!(body.to_string().len() > 64);

    let response = app
        .oneshot(
            Request::post("/rpc/1")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let value = body_json(response).await;
    assert_eq!(
        value["error"]["message"],
        "rpcrouter: request body too large"
    );
    assert_eq!(value["error"]["code"], -32600);
}

#[tokio::test]
async fn overload_rejects_concurrent_requests_with_503() {
    // 慢上游让第一个请求挂在飞；并发上限=1 时，第二个请求应被快速拒绝。
    let (url, _) = spawn_mock(MockBehavior {
        delay_ms: 250,
        ..MockBehavior::default()
    })
    .await;
    let app = guarded_app(&url, 1024 * 1024, 1, None, None).await;

    let first = app.clone().oneshot(rpc_request(1));
    let handle = tokio::spawn(first);
    // 等第一个请求进入在飞状态。
    tokio::time::sleep(Duration::from_millis(50)).await;

    let second = app.clone().oneshot(rpc_request(2)).await.expect("second");
    assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);
    let value = body_json(second).await;
    assert_eq!(
        value["error"]["message"],
        "rpcrouter: too many concurrent requests"
    );

    // 第一个请求应正常完成。
    let first = handle.await.expect("first task").expect("first result");
    assert_eq!(first.status(), StatusCode::OK);
}

#[tokio::test]
async fn per_ip_rate_limit_rejects_after_burst() {
    let (url, _) = spawn_mock(MockBehavior::default()).await;
    // rps=2, burst=2：前两个放行，第三个（同 IP、同一秒内）被拒。
    let app = guarded_app(&url, 1024 * 1024, 1024, Some((2, 2)), None).await;
    let ip = "203.0.113.7:1234".parse::<SocketAddr>().expect("addr");

    let send = |id: u64| {
        let mut req = rpc_request(id);
        req.extensions_mut().insert(ConnectInfo(ip));
        app.clone().oneshot(req)
    };

    let first = send(1).await.expect("first");
    assert_eq!(first.status(), StatusCode::OK);
    let second = send(2).await.expect("second");
    assert_eq!(second.status(), StatusCode::OK);
    let third = send(3).await.expect("third");
    assert_eq!(third.status(), StatusCode::TOO_MANY_REQUESTS);
    let value = body_json(third).await;
    assert_eq!(
        value["error"]["message"],
        "rpcrouter: rate limit exceeded for this client"
    );
}

#[tokio::test]
async fn metrics_endpoint_requires_bearer_token_when_configured() {
    let (url, _) = spawn_mock(MockBehavior::default()).await;
    let cfg = config();
    let registry = Arc::new(Registry::new(&cfg));
    registry
        .apply_snapshot(&ChainlistSnapshot {
            chains: vec![ChainEndpoints {
                chain_id: 1,
                name: "Hardening Test".to_owned(),
                endpoints: vec![url.clone()],
            }],
        })
        .await;
    let forwarder = Arc::new(Forwarder::new(Arc::clone(&registry), &cfg).expect("forwarder"));
    let state = AppState::new(registry, forwarder, 10).with_hardening(
        1024 * 1024,
        1024,
        None,
        Some("topsecret".to_owned()),
    );
    let app = app_router(state);

    let no_token = app
        .clone()
        .oneshot(Request::get("/metrics").body(Body::empty()).expect("req"))
        .await
        .expect("resp");
    assert_eq!(no_token.status(), StatusCode::UNAUTHORIZED);

    let bad_token = app
        .clone()
        .oneshot(
            Request::get("/metrics")
                .header(axum::http::header::AUTHORIZATION, "Bearer wrong")
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("resp");
    assert_eq!(bad_token.status(), StatusCode::UNAUTHORIZED);

    let good_token = app
        .oneshot(
            Request::get("/metrics")
                .header(axum::http::header::AUTHORIZATION, "Bearer topsecret")
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("resp");
    assert_eq!(good_token.status(), StatusCode::OK);
    let bytes = to_bytes(good_token.into_body(), usize::MAX)
        .await
        .expect("metrics body");
    // 指标编码成功：endpoint_state 在应用快照后总会出现（无需打真实请求）。
    assert!(String::from_utf8_lossy(&bytes).contains("rpcrouter_endpoint_state"));
}

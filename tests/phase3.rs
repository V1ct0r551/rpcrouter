use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use rpcrouter::{
    chainlist::{ChainEndpoints, ChainlistSnapshot},
    config::{Config, HedgingConfig, UpstreamConfig},
    forward::Forwarder,
    mock_upstream::{MockBehavior, MockController, router as mock_router},
    registry::{Endpoint, EndpointState, Registry},
    signals::{FailureSignal, FaultKind},
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, task::JoinSet, time::Instant};

async fn spawn_mock(behavior: MockBehavior) -> (String, MockController) {
    let controller = MockController::new(behavior);
    let app = mock_router(controller.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
    let address = listener.local_addr().expect("mock address");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve mock");
    });
    (format!("http://{address}/"), controller)
}

fn config(hedging: bool) -> Config {
    Config {
        chains: vec![1],
        chain_overrides: Vec::new(),
        upstream: UpstreamConfig {
            request_timeout_ms: 1_000,
            slow_threshold_ms: 900,
            deadline_ms: 3_000,
            max_attempts: 4,
            default_rps: 100,
            default_concurrency: 64,
        },
        hedging: HedgingConfig {
            enabled: hedging,
            delay_ms: 300,
            max_percent: 10,
            min_active_endpoints: 2,
        },
        ..Config::default()
    }
}

async fn setup(config: &Config, urls: &[String]) -> Arc<Registry> {
    let registry = Arc::new(Registry::new(config));
    registry
        .apply_snapshot(&ChainlistSnapshot {
            chains: vec![ChainEndpoints {
                chain_id: 1,
                name: "Phase 3".to_owned(),
                endpoints: urls.to_vec(),
            }],
        })
        .await;
    registry
}

fn activate(endpoint: &Endpoint, latency: Duration) {
    let now = Instant::now();
    endpoint.record_success(now, latency, true);
    endpoint.record_success(now, latency, true);
}

fn block_request(id: u64) -> Value {
    json!({"jsonrpc":"2.0","id":id,"method":"eth_blockNumber","params":[]})
}

#[tokio::test]
async fn response_cache_rewrites_ids_and_avoids_second_upstream_call() {
    let (url, mock) = spawn_mock(MockBehavior {
        delay_ms: 5,
        ..MockBehavior::default()
    })
    .await;
    let config = config(false);
    let registry = setup(&config, std::slice::from_ref(&url)).await;
    let endpoint = registry.endpoint(1, &url).await.expect("endpoint");
    activate(&endpoint, Duration::from_millis(5));
    let forwarder = Forwarder::new(Arc::clone(&registry), &config).expect("forwarder");

    let first = forwarder.execute(1, block_request(1)).await;
    let second = forwarder.execute(1, block_request(2)).await;
    assert_eq!(first["id"], 1);
    assert_eq!(second["id"], 2);
    assert_eq!(first["result"], second["result"]);
    assert_eq!(mock.request_count(), 1);
    forwarder.cache().sync().await;
    assert_eq!(forwarder.cache().entry_count(), 1);
}

#[tokio::test]
async fn concurrent_cache_misses_collapse_to_one_upstream_request() {
    let (url, mock) = spawn_mock(MockBehavior {
        delay_ms: 50,
        ..MockBehavior::default()
    })
    .await;
    let config = config(false);
    let registry = setup(&config, std::slice::from_ref(&url)).await;
    let endpoint = registry.endpoint(1, &url).await.expect("endpoint");
    activate(&endpoint, Duration::from_millis(5));
    let forwarder = Arc::new(Forwarder::new(registry, &config).expect("forwarder"));
    let mut tasks = JoinSet::new();
    for id in 0..32 {
        let forwarder = Arc::clone(&forwarder);
        tasks.spawn(async move { (id, forwarder.execute(1, block_request(id)).await) });
    }
    while let Some(result) = tasks.join_next().await {
        let (id, response) = result.expect("task");
        assert_eq!(response["id"], id);
        assert!(response.get("result").is_some());
    }
    assert_eq!(mock.request_count(), 1);
}

#[derive(Clone)]
struct FailFirstState(Arc<AtomicU64>);

async fn fail_first(State(state): State<FailFirstState>, body: Bytes) -> Response {
    let call = state.0.fetch_add(1, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(40)).await;
    if call == 0 {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let request: Value = serde_json::from_slice(&body).expect("request");
    Json(json!({"jsonrpc":"2.0","id":request["id"],"result":"0xabc"})).into_response()
}

#[tokio::test]
async fn failed_leader_is_not_shared_and_followers_retry_independently() {
    let calls = Arc::new(AtomicU64::new(0));
    let app = Router::new()
        .route("/", post(fail_first))
        .with_state(FailFirstState(Arc::clone(&calls)));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    let url = format!("http://{address}/");
    let config = config(false);
    let registry = setup(&config, std::slice::from_ref(&url)).await;
    let endpoint = registry.endpoint(1, &url).await.expect("endpoint");
    activate(&endpoint, Duration::from_millis(5));
    let forwarder = Arc::new(Forwarder::new(registry, &config).expect("forwarder"));

    let leader = {
        let forwarder = Arc::clone(&forwarder);
        tokio::spawn(async move { forwarder.execute(1, block_request(1)).await })
    };
    tokio::time::sleep(Duration::from_millis(5)).await;
    let mut followers = JoinSet::new();
    for id in 2..10 {
        let forwarder = Arc::clone(&forwarder);
        followers.spawn(async move { forwarder.execute(1, block_request(id)).await });
    }
    assert_eq!(leader.await.expect("leader")["error"]["code"], -32000);
    while let Some(result) = followers.join_next().await {
        assert!(result.expect("follower").get("result").is_some());
    }
    let calls_after_followers = calls.load(Ordering::SeqCst);
    assert!(calls_after_followers > 1);
    assert!(
        forwarder
            .execute(1, block_request(99))
            .await
            .get("result")
            .is_some()
    );
    assert_eq!(calls.load(Ordering::SeqCst), calls_after_followers);
}

#[tokio::test]
async fn hedging_is_budgeted_and_disabled_when_pool_is_unhealthy() {
    let (slow_url, slow) = spawn_mock(MockBehavior {
        delay_ms: 400,
        ..MockBehavior::default()
    })
    .await;
    let (fast_url, fast) = spawn_mock(MockBehavior {
        delay_ms: 10,
        ..MockBehavior::default()
    })
    .await;
    let config = config(true);
    let registry = setup(&config, &[slow_url.clone(), fast_url.clone()]).await;
    let slow_endpoint = registry.endpoint(1, &slow_url).await.expect("slow");
    let fast_endpoint = registry.endpoint(1, &fast_url).await.expect("fast");
    activate(&slow_endpoint, Duration::from_millis(1));
    activate(&fast_endpoint, Duration::from_millis(500));
    let forwarder =
        Arc::new(Forwarder::new(Arc::clone(&registry), &config).expect("hedged forwarder"));

    let mut tasks = JoinSet::new();
    for id in 0..10 {
        let forwarder = Arc::clone(&forwarder);
        tasks.spawn(async move {
            forwarder
                .execute(
                    1,
                    json!({
                        "jsonrpc":"2.0",
                        "id":id,
                        "method":"eth_getTransactionReceipt",
                        "params":[format!("0x{id:064x}")]
                    }),
                )
                .await
        });
    }
    while let Some(result) = tasks.join_next().await {
        assert!(result.expect("hedged request").get("result").is_some());
    }
    assert_eq!(slow.request_count(), 10);
    assert_eq!(fast.request_count(), 1);
    assert_eq!(forwarder.hedge_counts(), (10, 1));

    fast_endpoint.record_failure(Instant::now(), FailureSignal::new(FaultKind::RateLimited));
    assert!(matches!(
        fast_endpoint.state(Instant::now()),
        EndpointState::Cooling { .. }
    ));
    slow.reset_request_count();
    fast.reset_request_count();
    let mut unhealthy_tasks = JoinSet::new();
    for id in 10..20 {
        let forwarder = Arc::clone(&forwarder);
        unhealthy_tasks.spawn(async move {
            forwarder
                .execute(
                    1,
                    json!({
                        "jsonrpc":"2.0",
                        "id":id,
                        "method":"eth_getTransactionReceipt",
                        "params":[format!("0x{id:064x}")]
                    }),
                )
                .await
        });
    }
    while let Some(result) = unhealthy_tasks.join_next().await {
        assert!(result.expect("unhealthy request").get("result").is_some());
    }
    assert_eq!(slow.request_count(), 10);
    assert_eq!(fast.request_count(), 0);
    assert_eq!(forwarder.hedge_counts(), (20, 1));
}

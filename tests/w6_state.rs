use std::{sync::Arc, time::Duration};

use rpcrouter::{
    chainlist::{ChainEndpoints, ChainlistSnapshot},
    config::{Config, HedgingConfig, UpstreamConfig},
    forward::Forwarder,
    mock_upstream::{MockBehavior, MockController, router as mock_router},
    registry::Registry,
    state::{MemoryStore, StateStore},
};
use serde_json::json;
use tokio::{net::TcpListener, time::Instant};

async fn spawn_mock() -> (String, MockController) {
    let controller = MockController::new(MockBehavior::default());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = mock_router(controller.clone());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{address}/"), controller)
}

#[tokio::test]
async fn ten_thousand_requests_do_not_call_state_store() {
    let store = MemoryStore::new();
    store.bootstrap().await.unwrap();
    let before = store.call_count();
    let (url, mock) = spawn_mock().await;
    let config = Config {
        chains: vec![1],
        upstream: UpstreamConfig {
            request_timeout_ms: 1000,
            slow_threshold_ms: 900,
            deadline_ms: 3000,
            max_attempts: 2,
            default_rps: 100,
            default_concurrency: 64,
        },
        hedging: HedgingConfig {
            enabled: false,
            ..HedgingConfig::default()
        },
        ..Config::default()
    };
    let registry = Arc::new(Registry::new(&config));
    registry
        .apply_snapshot(&ChainlistSnapshot {
            chains: vec![ChainEndpoints {
                chain_id: 1,
                name: "state hot path".into(),
                endpoints: vec![url.clone()],
            }],
        })
        .await;
    let endpoint = registry.endpoint(1, &url).await.unwrap();
    endpoint.record_success(Instant::now(), Duration::from_millis(1), true);
    endpoint.record_success(Instant::now(), Duration::from_millis(1), true);
    let forwarder = Forwarder::new(registry, &config).unwrap();
    forwarder
        .execute(
            1,
            json!({"jsonrpc":"2.0","id":0,"method":"eth_blockNumber","params":[]}),
        )
        .await;
    forwarder.cache().sync().await;
    for id in 1..=10_000 {
        let response = forwarder
            .execute(
                1,
                json!({"jsonrpc":"2.0","id":id,"method":"eth_blockNumber","params":[]}),
            )
            .await;
        assert!(response.get("result").is_some());
    }
    assert_eq!(
        store.call_count(),
        before,
        "request path must not touch StateStore"
    );
    assert_eq!(mock.request_count(), 1, "cache must carry the hot path");
}

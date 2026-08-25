use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Json, Router,
    body::{Body, Bytes, to_bytes},
    extract::{Path, State},
    http::{Request, StatusCode, header::CONTENT_TYPE},
    routing::post,
};
use rpcrouter::{
    chainlist::{Catalog, CatalogChain, CatalogEndpoint, ChainEndpoints, ChainlistSnapshot},
    config::{Config, DiscoveryConfig},
    forward::Forwarder,
    probe::{ProbeManager, spawn as spawn_probes},
    registry::{ChainStateLabel, Registry},
    server::{AppState, router},
};
use serde_json::{Value, json};
use tokio::{task::JoinSet, time::Instant};
use tower::ServiceExt;

#[derive(Clone)]
struct ChainMockState {
    chain_id: u64,
    calls: Arc<AtomicUsize>,
    chain_id_calls: Arc<AtomicUsize>,
}

async fn chain_mock(State(state): State<ChainMockState>, body: Bytes) -> Json<Value> {
    state.calls.fetch_add(1, Ordering::SeqCst);
    let request: Value = serde_json::from_slice(&body).expect("mock request");
    let result = match request["method"].as_str().expect("method") {
        "eth_chainId" => {
            state.chain_id_calls.fetch_add(1, Ordering::SeqCst);
            format!("0x{:x}", state.chain_id)
        }
        "eth_blockNumber" => format!("0x{:x}", 10_000 + state.chain_id),
        method => panic!("unexpected method {method}"),
    };
    Json(json!({"jsonrpc":"2.0", "id":request["id"], "result":result}))
}

async fn spawn_chain_mock(chain_id: u64) -> (String, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let chain_id_calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route("/", post(chain_mock))
        .with_state(ChainMockState {
            chain_id,
            calls: Arc::clone(&calls),
            chain_id_calls: Arc::clone(&chain_id_calls),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock");
    let address = listener.local_addr().expect("mock address");
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve mock") });
    (format!("http://{address}/"), calls, chain_id_calls)
}

fn dynamic_config() -> Config {
    let mut config = Config {
        chains: vec![],
        discovery: DiscoveryConfig {
            enabled: true,
            max_hot_chains: 100,
            idle_seconds: 600,
            ..Default::default()
        },
        ..Config::default()
    };
    config.upstream.default_rps = 1_000;
    config.upstream.default_concurrency = 8;
    config.upstream.request_timeout_ms = 500;
    config.upstream.deadline_ms = 2_000;
    config.probe.max_concurrency = 16;
    config.probe.request_timeout_ms = 500;
    config.probe.min_interval_seconds = 3_600;
    config.probe.max_interval_seconds = 3_600;
    config
}

fn catalog_chain(chain_id: u64, endpoints: Vec<String>) -> CatalogChain {
    CatalogChain {
        chain_id,
        name: format!("Chain {chain_id}"),
        short_name: Some(format!("c{chain_id}")),
        chain: Some(format!("C{chain_id}")),
        slug: None,
        is_testnet: false,
        native_symbol: Some("ETH".to_owned()),
        explorer_url: None,
        status: Some("active".to_owned()),
        tvl: None,
        endpoints: endpoints
            .into_iter()
            .map(|url| CatalogEndpoint {
                url,
                tracking: None,
            })
            .collect(),
    }
}

async fn post_rpc(app: Router, chain_id: u64) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::post(format!("/rpc/{chain_id}"))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"jsonrpc":"2.0","id":chain_id,"method":"eth_blockNumber","params":[]})
                        .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    (status, serde_json::from_slice(&body).expect("json"))
}

#[tokio::test]
async fn fifty_chain_cold_start_is_broad_and_removes_wrong_chain_endpoints() {
    const CHAINS: usize = 50;
    let config = dynamic_config();
    let registry = Arc::new(Registry::new(&config));
    let mut urls = Vec::with_capacity(CHAINS);
    for index in 0..CHAINS {
        let chain_id = 10_000 + index as u64;
        urls.push(spawn_chain_mock(chain_id).await.0);
    }

    let chains: Vec<_> = (0..CHAINS)
        .map(|index| {
            let chain_id = 10_000 + index as u64;
            let mut endpoints = vec![urls[index].clone()];
            if index < 10 {
                endpoints.push(urls[(index + 1) % CHAINS].clone());
            }
            catalog_chain(chain_id, endpoints)
        })
        .collect();
    let by_id = chains
        .iter()
        .enumerate()
        .map(|(index, chain)| (chain.chain_id, index))
        .collect();
    registry
        .set_catalog(Arc::new(Catalog { chains, by_id }))
        .await;
    assert_eq!(registry.chain_counts().await.dormant, CHAINS as u64);

    let probes = Arc::new(ProbeManager::new(Arc::clone(&registry), &config).expect("probes"));
    spawn_probes(probes);
    let forwarder = Arc::new(Forwarder::new(Arc::clone(&registry), &config).expect("forwarder"));
    let app = router(AppState::new(
        Arc::clone(&registry),
        forwarder,
        config.server.batch_limit,
    ));
    let mut requests = JoinSet::new();
    for index in 0..CHAINS {
        requests.spawn(post_rpc(app.clone(), 10_000 + index as u64));
    }
    while let Some(result) = requests.join_next().await {
        let (status, body) = result.expect("request task");
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(body.get("result").is_some(), "{body}");
    }
    assert_eq!(registry.user_visible_errors(), 0);
    assert_eq!(registry.chain_counts().await.hot, CHAINS as u64);

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut removed = true;
        for index in 0..10 {
            let chain_id = 10_000 + index as u64;
            if registry
                .endpoint(chain_id, &urls[(index + 1) % CHAINS])
                .await
                .is_some()
            {
                removed = false;
                break;
            }
        }
        if removed {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "wrong-chain endpoints were not removed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[derive(Clone)]
struct ConcurrencyState {
    current: Arc<AtomicUsize>,
    max: Arc<AtomicUsize>,
    total: Arc<AtomicUsize>,
    per_current: Arc<Vec<AtomicUsize>>,
    per_max: Arc<Vec<AtomicUsize>>,
}

fn update_max(max: &AtomicUsize, value: usize) {
    max.fetch_max(value, Ordering::SeqCst);
}

async fn concurrency_mock(
    Path(index): Path<usize>,
    State(state): State<ConcurrencyState>,
    body: Bytes,
) -> Json<Value> {
    let current = state.current.fetch_add(1, Ordering::SeqCst) + 1;
    update_max(&state.max, current);
    let endpoint_current = state.per_current[index].fetch_add(1, Ordering::SeqCst) + 1;
    update_max(&state.per_max[index], endpoint_current);
    state.total.fetch_add(1, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(2)).await;
    let request: Value = serde_json::from_slice(&body).expect("request");
    state.per_current[index].fetch_sub(1, Ordering::SeqCst);
    state.current.fetch_sub(1, Ordering::SeqCst);
    let result = if request["method"] == "eth_chainId" {
        "0x1".to_owned()
    } else {
        "0x100".to_owned()
    };
    Json(json!({"jsonrpc":"2.0","id":request["id"],"result":result}))
}

#[tokio::test]
async fn probe_pool_bounds_five_hundred_endpoints_and_deduplicates_each_endpoint() {
    const ENDPOINTS: usize = 500;
    let state = ConcurrencyState {
        current: Arc::new(AtomicUsize::new(0)),
        max: Arc::new(AtomicUsize::new(0)),
        total: Arc::new(AtomicUsize::new(0)),
        per_current: Arc::new((0..ENDPOINTS).map(|_| AtomicUsize::new(0)).collect()),
        per_max: Arc::new((0..ENDPOINTS).map(|_| AtomicUsize::new(0)).collect()),
    };
    let app = Router::new()
        .route("/{index}", post(concurrency_mock))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("address");
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });

    let mut config = dynamic_config();
    config.chains = vec![1];
    config.probe.max_concurrency = 4;
    let registry = Arc::new(Registry::new(&config));
    registry
        .apply_snapshot(&ChainlistSnapshot {
            chains: vec![ChainEndpoints {
                chain_id: 1,
                name: "Probe breadth".to_owned(),
                endpoints: (0..ENDPOINTS)
                    .map(|index| format!("http://{address}/{index}"))
                    .collect(),
            }],
        })
        .await;
    let manager = Arc::new(ProbeManager::new(registry, &config).expect("manager"));
    spawn_probes(Arc::clone(&manager));

    let deadline = Instant::now() + Duration::from_secs(10);
    while state.total.load(Ordering::SeqCst) < ENDPOINTS
        || manager.queue_depth() != 0
        || manager.in_flight() != 0
    {
        assert!(Instant::now() < deadline, "probe pool did not drain");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(state.max.load(Ordering::SeqCst) <= 4);
    assert!(
        state
            .per_max
            .iter()
            .all(|max| max.load(Ordering::SeqCst) <= 1)
    );
    assert_eq!(state.total.load(Ordering::SeqCst), ENDPOINTS * 2);
    assert_eq!(manager.in_flight(), 0);
}

#[tokio::test]
async fn dormant_chain_is_not_probed_until_activation_kick() {
    let (url, _, chain_id_calls) = spawn_chain_mock(4242).await;
    let config = dynamic_config();
    let registry = Arc::new(Registry::new(&config));
    registry
        .set_catalog(Arc::new(Catalog {
            chains: vec![catalog_chain(4242, vec![url])],
            by_id: HashMap::from([(4242, 0)]),
        }))
        .await;
    let manager = Arc::new(ProbeManager::new(Arc::clone(&registry), &config).expect("manager"));
    spawn_probes(manager);
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(chain_id_calls.load(Ordering::SeqCst), 0);

    let forwarder = Arc::new(Forwarder::new(Arc::clone(&registry), &config).expect("forwarder"));
    let app = router(AppState::new(
        Arc::clone(&registry),
        forwarder,
        config.server.batch_limit,
    ));
    let (status, _) = post_rpc(app, 4242).await;
    assert_eq!(status, StatusCode::OK);
    let deadline = Instant::now() + Duration::from_secs(2);
    while chain_id_calls.load(Ordering::SeqCst) == 0 {
        assert!(
            Instant::now() < deadline,
            "activation kick did not probe immediately"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(registry.summaries().await[0].state, ChainStateLabel::Hot);
}

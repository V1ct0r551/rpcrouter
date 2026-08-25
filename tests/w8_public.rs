use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header::AUTHORIZATION},
};
use rpcrouter::{
    admin::AdminState,
    chainlist::{Catalog, CatalogChain, CatalogEndpoint, ChainEndpoints, ChainlistSnapshot},
    config::Config,
    forward::Forwarder,
    mock_upstream::{MockBehavior, MockController, router as mock_router},
    registry::{EndpointState, Registry},
    server::{AppState, router as app_router},
    state::{MemoryStore, StateRuntimeSnapshot, StateStore},
};
use serde_json::Value;
use tokio::{net::TcpListener, time::Instant};
use tower::ServiceExt;

async fn mock() -> (String, MockController) {
    let controller = MockController::new(MockBehavior::default());
    let served = controller.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, mock_router(served)).await.unwrap();
    });
    (format!("http://{address}/private-upstream"), controller)
}

fn static_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "rpcrouter-w8-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(dir.join("assets")).unwrap();
    std::fs::write(
        dir.join("index.html"),
        "<!doctype html><script src=\"/dashboard/assets/app.js\"></script>",
    )
    .unwrap();
    std::fs::write(dir.join("assets/app.js"), "console.log('ok')").unwrap();
    dir
}

async fn app(
    token: Option<&str>,
    public_site: bool,
    static_dir: Option<PathBuf>,
) -> (Router, PathBuf) {
    let (url, _controller) = mock().await;
    let mut config = Config {
        chains: Vec::new(),
        ..Config::default()
    };
    config.admin.auth_token = token.map(str::to_owned);
    config.admin.public_site = public_site;
    config.admin.static_dir = static_dir.clone();
    let registry = Arc::new(Registry::new(&config));
    registry
        .set_catalog(Arc::new(Catalog {
            chains: vec![
                CatalogChain {
                    chain_id: 1,
                    name: "Ethereum".into(),
                    short_name: Some("eth".into()),
                    chain: None,
                    slug: None,
                    is_testnet: false,
                    native_symbol: Some("ETH".into()),
                    explorer_url: Some("https://explorer.example/".into()),
                    status: Some("active".into()),
                    tvl: None,
                    endpoints: vec![CatalogEndpoint {
                        url: url.clone(),
                        tracking: None,
                    }],
                },
                CatalogChain {
                    chain_id: 2,
                    name: "Test Chain".into(),
                    short_name: Some("test".into()),
                    chain: None,
                    slug: None,
                    is_testnet: true,
                    native_symbol: Some("TST".into()),
                    explorer_url: None,
                    status: Some("active".into()),
                    tvl: None,
                    endpoints: Vec::new(),
                },
            ],
            by_id: HashMap::from([(1, 0), (2, 1)]),
        }))
        .await;
    registry
        .apply_snapshot(&ChainlistSnapshot {
            chains: vec![ChainEndpoints {
                chain_id: 1,
                name: "Ethereum".into(),
                endpoints: vec![url.clone()],
            }],
        })
        .await;
    registry.resolve_for_request(1).await.unwrap();
    let endpoint = registry.endpoint(1, &url).await.unwrap();
    endpoint.record_success(Instant::now(), Duration::from_millis(1), true);
    endpoint.record_success(Instant::now(), Duration::from_millis(1), true);
    assert_eq!(endpoint.state(Instant::now()), EndpointState::Active);
    let forwarder = Arc::new(Forwarder::new(registry.clone(), &config).unwrap());
    let store = Arc::new(MemoryStore::new());
    store.bootstrap().await.unwrap();
    let admin = AdminState {
        registry: registry.clone(),
        forwarder: forwarder.clone(),
        metrics: forwarder.metrics(),
        store,
        chainlist: None,
        probe: None,
        config,
        started: std::time::Instant::now(),
        state_runtime: StateRuntimeSnapshot::new("memory", "test", "test-1"),
    };
    (
        app_router(AppState::new(registry, forwarder, 10).with_admin(admin)),
        static_dir.unwrap_or_default(),
    )
}

async fn response(service: &Router, path: &str, auth: Option<&str>) -> axum::response::Response {
    let mut request = Request::get(path);
    if let Some(auth) = auth {
        request = request.header(AUTHORIZATION, auth);
    }
    service
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn json_body(response: axum::response::Response) -> Value {
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
}

#[tokio::test]
async fn public_api_ignores_admin_authorization_and_sets_cache_header() {
    let (service, _) = app(Some("secret"), true, None).await;
    for path in [
        "/api/public/overview",
        "/api/public/chains",
        "/api/public/chains/1",
    ] {
        for auth in [None, Some("Bearer wrong")] {
            let response = response(&service, path, auth).await;
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            assert_eq!(response.headers()["cache-control"], "public, max-age=5");
        }
    }
}

#[tokio::test]
async fn public_rows_are_redacted_and_disabled_chains_are_hidden() {
    let (service, _) = app(Some("secret"), true, None).await;
    let body = json_body(response(&service, "/api/public/chains", None).await).await;
    let serialized = body.to_string();
    for forbidden in [
        "endpointRows",
        "settings",
        "userVisibleErrorsTotal",
        "private-upstream",
    ] {
        assert!(!serialized.contains(forbidden), "leaked {forbidden}");
    }
    assert_eq!(body["items"][0]["nativeSymbol"], "ETH");
    let keys = body["items"][0]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        keys,
        [
            "active",
            "cacheHitsTotal",
            "cacheLookupsTotal",
            "catalogEndpoints",
            "chainId",
            "endpoints",
            "explorerUrl",
            "head",
            "ingressTotal",
            "isTestnet",
            "name",
            "nativeSymbol",
            "shortName",
            "state",
            "status",
        ]
        .into_iter()
        .collect()
    );
    let disabled = service
        .clone()
        .oneshot(
            Request::post("/admin/api/chains/1/disable")
                .header(AUTHORIZATION, "Bearer secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(disabled.status(), StatusCode::OK);
    let body = json_body(response(&service, "/api/public/chains", None).await).await;
    assert!(
        body["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["chainId"] != 1)
    );
    assert_eq!(
        response(&service, "/api/public/chains/1", None)
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        response(&service, "/api/public/chains/999", None)
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn public_spa_routes_are_safe_and_obey_configuration() {
    let dir = static_dir();
    let (service, cleanup) = app(None, true, Some(dir.clone())).await;
    for path in ["/", "/chain/1", "/chain/1/"] {
        let response = response(&service, path, None).await;
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert!(
            response.headers()["content-type"]
                .to_str()
                .unwrap()
                .starts_with("text/html")
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8_lossy(&body).contains("/dashboard/assets/app.js"));
    }
    for path in ["/chain/../x", "/%2e%2e"] {
        assert_eq!(
            response(&service, path, None).await.status(),
            StatusCode::NOT_FOUND
        );
    }
    let (disabled, _) = app(None, false, Some(dir)).await;
    for path in [
        "/",
        "/chain/1",
        "/chain/1/",
        "/api/public/overview",
        "/api/public/chains",
        "/api/public/chains/1",
    ] {
        assert_eq!(
            response(&disabled, path, None).await.status(),
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        response(&disabled, "/dashboard/", None).await.status(),
        StatusCode::OK
    );
    let (no_static, _) = app(None, true, None).await;
    assert_eq!(
        response(&no_static, "/", None).await.status(),
        StatusCode::NOT_FOUND
    );
    std::fs::remove_dir_all(cleanup).unwrap();
}

#[tokio::test]
async fn legacy_and_admin_routes_keep_their_contracts() {
    let (service, _) = app(Some("secret"), true, None).await;
    assert_eq!(
        response(&service, "/chains", None).await.status(),
        StatusCode::OK
    );
    assert_eq!(
        response(&service, "/admin/api/overview", None)
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        response(&service, "/admin/api/overview", Some("Bearer secret"))
            .await
            .status(),
        StatusCode::OK
    );
}

#[test]
fn public_site_config_defaults_on_and_accepts_explicit_off() {
    let default = Config::from_toml("chains = [1]").unwrap();
    assert!(default.admin.public_site);
    let disabled = Config::from_toml("chains = [1]\n[admin]\npublic_site = false").unwrap();
    assert!(!disabled.admin.public_site);
}

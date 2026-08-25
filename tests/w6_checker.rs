use std::{collections::HashMap, sync::Arc};

use rpcrouter::{
    chainlist::{Catalog, CatalogChain, CatalogEndpoint, ChainEndpoints, ChainlistSnapshot},
    config::Config,
    registry::Registry,
    state::{ChainOverrideState, EndpointOverrideState, RedisStore, StateStore, endpoint_key},
};

fn catalog(id: u64, urls: &[String]) -> CatalogChain {
    CatalogChain {
        chain_id: id,
        name: format!("Chain {id}"),
        short_name: None,
        chain: None,
        slug: None,
        is_testnet: false,
        native_symbol: None,
        explorer_url: None,
        status: Some("active".into()),
        tvl: None,
        endpoints: urls
            .iter()
            .map(|url| CatalogEndpoint {
                url: url.clone(),
                tracking: None,
            })
            .collect(),
    }
}

#[tokio::test]
async fn runtime_endpoint_overrides_survive_pinned_and_refresh_paths() {
    let config = Config {
        chains: vec![1],
        ..Config::default()
    };
    let registry = Arc::new(Registry::new(&config));
    let pinned = vec!["http://one.invalid".into(), "http://two.invalid".into()];
    let dynamic = vec!["http://three.invalid".into(), "http://four.invalid".into()];
    let chains = vec![catalog(1, &pinned), catalog(4242, &dynamic)];
    registry
        .set_catalog(Arc::new(Catalog {
            by_id: chains
                .iter()
                .enumerate()
                .map(|(i, c)| (c.chain_id, i))
                .collect::<HashMap<_, _>>(),
            chains,
        }))
        .await;
    let mut overrides = rpcrouter::state::Overrides::default();
    overrides.endpoints.insert(
        endpoint_key(1, &pinned[0]),
        EndpointOverrideState {
            url: pinned[0].clone(),
            disabled: Some(true),
            rps: Some(3),
            concurrency: Some(1),
        },
    );
    overrides.endpoints.insert(
        endpoint_key(4242, &dynamic[0]),
        EndpointOverrideState {
            url: dynamic[0].clone(),
            disabled: Some(true),
            rps: None,
            concurrency: None,
        },
    );
    overrides.endpoints.insert(
        endpoint_key(4242, &dynamic[1]),
        EndpointOverrideState {
            url: dynamic[1].clone(),
            disabled: None,
            rps: Some(3),
            concurrency: Some(1),
        },
    );
    let snapshot = ChainlistSnapshot {
        chains: vec![
            ChainEndpoints {
                chain_id: 1,
                name: "one".into(),
                endpoints: pinned.clone(),
            },
            ChainEndpoints {
                chain_id: 4242,
                name: "dynamic".into(),
                endpoints: dynamic.clone(),
            },
        ],
    };
    registry.apply_overrides(&overrides).await;
    registry.apply_snapshot(&snapshot).await;
    assert!(
        registry
            .all_endpoints(1)
            .await
            .iter()
            .all(|e| e.url() != pinned[0])
    );
    registry.resolve_for_request(4242).await.unwrap();
    assert!(
        registry
            .all_endpoints(4242)
            .await
            .iter()
            .all(|e| e.url() != dynamic[0])
    );
    assert_eq!(
        registry
            .all_endpoints(4242)
            .await
            .iter()
            .find(|e| e.url() == dynamic[1])
            .unwrap()
            .rps(),
        3
    );
    registry.apply_snapshot(&snapshot).await;
    assert!(
        registry
            .all_endpoints(4242)
            .await
            .iter()
            .all(|e| e.url() != dynamic[0])
    );
}

#[tokio::test]
#[ignore]
async fn redis_concurrent_structured_writes_keep_all_members() {
    let Ok(url) = std::env::var("REDIS_URL") else {
        eprintln!("skipping Redis test: REDIS_URL is not set");
        return;
    };
    let namespace = format!("checker-structured-{}", std::process::id());
    let a = RedisStore::connect(&url, &namespace).await.unwrap();
    let b = RedisStore::connect(&url, &namespace).await.unwrap();
    a.reset().await.unwrap();
    let left = async {
        for id in 0..60 {
            a.put_chain_override(id, &ChainOverrideState::default())
                .await
                .unwrap();
        }
    };
    let right = async {
        for id in 60..120 {
            b.put_chain_override(id, &ChainOverrideState::default())
                .await
                .unwrap();
        }
    };
    tokio::join!(left, right);
    assert_eq!(a.load_overrides().await.unwrap().chains.len(), 120);
    a.reset().await.unwrap();
}

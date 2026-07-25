use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use dashmap::DashMap;
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use serde::Serialize;
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};

use crate::{
    chainlist::{ChainEndpoints, ChainlistSnapshot},
    config::Config,
};

pub struct Endpoint {
    url: String,
    rps: u32,
    concurrency: usize,
    bucket: DefaultDirectRateLimiter,
    inflight: Arc<Semaphore>,
    last_seen: AtomicU64,
}

impl Endpoint {
    fn new(url: String, rps: u32, concurrency: usize, now: u64) -> Self {
        let quota = Quota::per_second(NonZeroU32::new(rps).expect("validated nonzero RPS"))
            .allow_burst(NonZeroU32::new(rps).expect("validated nonzero RPS"));
        Self {
            url,
            rps,
            concurrency,
            bucket: RateLimiter::direct(quota),
            inflight: Arc::new(Semaphore::new(concurrency)),
            last_seen: AtomicU64::new(now),
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn rps(&self) -> u32 {
        self.rps
    }

    pub fn concurrency(&self) -> usize {
        self.concurrency
    }

    pub fn try_acquire(self: &Arc<Self>) -> Option<EndpointLease> {
        let permit = Arc::clone(&self.inflight).try_acquire_owned().ok()?;
        if self.bucket.check().is_err() {
            drop(permit);
            return None;
        }
        Some(EndpointLease {
            endpoint: Arc::clone(self),
            _permit: permit,
        })
    }
}

pub struct EndpointLease {
    endpoint: Arc<Endpoint>,
    _permit: OwnedSemaphorePermit,
}

impl EndpointLease {
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }
}

struct ChainState {
    chain_id: u64,
    name: RwLock<String>,
    endpoints: RwLock<Vec<Arc<Endpoint>>>,
    cursor: AtomicUsize,
}

impl ChainState {
    fn new(chain_id: u64, name: String) -> Self {
        Self {
            chain_id,
            name: RwLock::new(name),
            endpoints: RwLock::new(Vec::new()),
            cursor: AtomicUsize::new(0),
        }
    }
}

pub struct Registry {
    chains: DashMap<u64, Arc<ChainState>>,
    config: Config,
}

impl Registry {
    pub fn new(config: &Config) -> Self {
        Self {
            chains: DashMap::new(),
            config: config.clone(),
        }
    }

    /// 刷新时复用同 URL 的端点对象，避免丢失出站限流与并发状态。
    pub async fn apply_snapshot(&self, snapshot: &ChainlistSnapshot) {
        let source_by_chain: HashMap<_, _> = snapshot
            .chains
            .iter()
            .map(|chain| (chain.chain_id, chain))
            .collect();
        let now = unix_seconds();

        for &chain_id in &self.config.chains {
            let source = source_by_chain.get(&chain_id).copied();
            let name = source
                .map(|chain| chain.name.clone())
                .unwrap_or_else(|| format!("Chain {chain_id}"));
            let state = self
                .chains
                .entry(chain_id)
                .or_insert_with(|| Arc::new(ChainState::new(chain_id, name.clone())))
                .clone();
            if source.is_some() {
                *state.name.write().await = name;
            }
            self.merge_chain(&state, source, now).await;
        }
    }

    async fn merge_chain(
        &self,
        state: &Arc<ChainState>,
        source: Option<&ChainEndpoints>,
        now: u64,
    ) {
        let previous = state.endpoints.read().await.clone();
        let previous_by_url: HashMap<_, _> = previous
            .iter()
            .map(|endpoint| (endpoint.url.clone(), Arc::clone(endpoint)))
            .collect();
        let chain_override = self.config.chain_override(state.chain_id);
        let disabled: HashSet<&str> = chain_override
            .into_iter()
            .flat_map(|chain| chain.disabled_endpoints.iter().map(String::as_str))
            .collect();
        let mut desired = Vec::new();
        if let Some(source) = source {
            desired.extend(source.endpoints.iter().cloned());
        }
        if let Some(chain) = chain_override {
            desired.extend(chain.extra_endpoints.iter().cloned());
        }

        let mut present = HashSet::new();
        let mut merged = Vec::new();
        for url in desired {
            if disabled.contains(url.as_str()) || !present.insert(url.clone()) {
                continue;
            }
            if let Some(endpoint) = previous_by_url.get(&url) {
                endpoint.last_seen.store(now, Ordering::Relaxed);
                merged.push(Arc::clone(endpoint));
            } else {
                let (rps, concurrency) = self.config.endpoint_limits(state.chain_id, &url);
                merged.push(Arc::new(Endpoint::new(url, rps, concurrency, now)));
            }
        }

        for endpoint in previous {
            let age = now.saturating_sub(endpoint.last_seen.load(Ordering::Relaxed));
            if !present.contains(endpoint.url())
                && !disabled.contains(endpoint.url())
                && age < self.config.chainlist.stale_grace_seconds
            {
                present.insert(endpoint.url.clone());
                merged.push(endpoint);
            }
        }
        *state.endpoints.write().await = merged;
    }

    /// 返回从轮转游标开始的全池快照；调用方再按出站闸筛选可用端点。
    pub async fn candidates(&self, chain_id: u64) -> Vec<Arc<Endpoint>> {
        let Some(state) = self.chains.get(&chain_id).map(|item| Arc::clone(&item)) else {
            return Vec::new();
        };
        let endpoints = state.endpoints.read().await;
        if endpoints.is_empty() {
            return Vec::new();
        }
        let start = state.cursor.fetch_add(1, Ordering::Relaxed) % endpoints.len();
        (0..endpoints.len())
            .map(|offset| Arc::clone(&endpoints[(start + offset) % endpoints.len()]))
            .collect()
    }

    pub async fn summaries(&self) -> Vec<ChainSummary> {
        let mut states: Vec<_> = self
            .chains
            .iter()
            .map(|entry| Arc::clone(entry.value()))
            .collect();
        states.sort_by_key(|state| state.chain_id);

        let mut summaries = Vec::with_capacity(states.len());
        for state in states {
            let name = state.name.read().await.clone();
            let endpoint_count = state.endpoints.read().await.len();
            summaries.push(ChainSummary {
                chain_id: state.chain_id,
                name,
                endpoints: endpoint_count,
                active: endpoint_count,
            });
        }
        summaries
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainSummary {
    pub chain_id: u64,
    pub name: String,
    pub endpoints: usize,
    pub active: usize,
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(urls: &[&str]) -> ChainlistSnapshot {
        ChainlistSnapshot {
            chains: vec![ChainEndpoints {
                chain_id: 1,
                name: "Ethereum Mainnet".to_owned(),
                endpoints: urls.iter().map(ToString::to_string).collect(),
            }],
        }
    }

    #[tokio::test]
    async fn rotates_and_preserves_endpoint_state_on_refresh() {
        let config = Config {
            chains: vec![1],
            ..Config::default()
        };
        let registry = Registry::new(&config);
        registry
            .apply_snapshot(&snapshot(&["http://one", "http://two", "http://three"]))
            .await;

        let first = registry.candidates(1).await;
        let second = registry.candidates(1).await;
        assert_eq!(first[0].url(), "http://one");
        assert_eq!(second[0].url(), "http://two");

        registry
            .apply_snapshot(&snapshot(&["http://one", "http://two", "http://three"]))
            .await;
        let refreshed = registry.candidates(1).await;
        let refreshed_one = refreshed
            .iter()
            .find(|endpoint| endpoint.url() == "http://one")
            .expect("refreshed endpoint");
        assert!(Arc::ptr_eq(&first[0], refreshed_one));
    }

    #[tokio::test]
    async fn enforces_endpoint_concurrency_and_rate_tokens() {
        let config = Config {
            chains: vec![1],
            chain_overrides: Vec::new(),
            upstream: crate::config::UpstreamConfig {
                default_rps: 1,
                default_concurrency: 1,
                ..crate::config::UpstreamConfig::default()
            },
            ..Config::default()
        };
        let registry = Registry::new(&config);
        registry
            .apply_snapshot(&snapshot(&["http://limited"]))
            .await;
        let endpoint = registry.candidates(1).await.remove(0);

        let lease = endpoint.try_acquire().expect("first request is allowed");
        assert!(endpoint.try_acquire().is_none(), "concurrency slot is held");
        drop(lease);
        assert!(endpoint.try_acquire().is_none(), "rate token was consumed");
        assert_eq!(endpoint.rps(), 1);
        assert_eq!(endpoint.concurrency(), 1);
    }

    #[tokio::test]
    async fn drops_disappeared_endpoint_when_grace_is_disabled() {
        let config = Config {
            chains: vec![1],
            chainlist: crate::config::ChainlistConfig {
                stale_grace_seconds: 0,
                ..crate::config::ChainlistConfig::default()
            },
            ..Config::default()
        };
        let registry = Registry::new(&config);
        registry.apply_snapshot(&snapshot(&["http://old"])).await;
        registry.apply_snapshot(&snapshot(&[])).await;
        assert!(registry.candidates(1).await.is_empty());
    }
}

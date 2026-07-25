use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use rand::Rng;
use reqwest::{Client, header::CONTENT_TYPE};
use serde_json::{Value, json};
use tokio::{
    sync::{Mutex, Semaphore},
    time::{Instant, sleep, timeout},
};
use tracing::{debug, info, warn};

use crate::{
    config::Config,
    registry::{Endpoint, EndpointState, Registry},
    signals::{FailureSignal, FaultKind, ResponseClassification, classify_response},
};

const PROBE_BODY_LIMIT: usize = 1024 * 1024;
const SCHEDULER_TICK: Duration = Duration::from_millis(250);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProbeOutcome {
    Passed,
    Failed(FaultKind),
    RemovedWrongChain { actual: u64 },
    Skipped,
}

pub struct ProbeManager {
    registry: Arc<Registry>,
    client: Client,
    chains: Vec<u64>,
    global_slots: Arc<Semaphore>,
    schedules: Mutex<HashMap<(u64, String), Instant>>,
    min_interval: Duration,
    max_interval: Duration,
    request_timeout: Duration,
    slow_threshold: Duration,
}

impl ProbeManager {
    pub fn new(registry: Arc<Registry>, config: &Config) -> Result<Self> {
        let client = Client::builder()
            .user_agent(concat!("rpcrouter/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("failed to build probe HTTP client")?;
        Ok(Self {
            registry,
            client,
            chains: config.chains.clone(),
            global_slots: Arc::new(Semaphore::new(config.probe.max_concurrency)),
            schedules: Mutex::new(HashMap::new()),
            min_interval: Duration::from_secs(config.probe.min_interval_seconds),
            max_interval: Duration::from_secs(config.probe.max_interval_seconds),
            request_timeout: Duration::from_millis(config.probe.request_timeout_ms),
            slow_threshold: Duration::from_millis(config.upstream.slow_threshold_ms),
        })
    }

    pub async fn run(self: Arc<Self>) {
        loop {
            self.schedule_due_probes().await;
            sleep(SCHEDULER_TICK).await;
        }
    }

    async fn schedule_due_probes(self: &Arc<Self>) {
        let now = Instant::now();
        let mut listed = Vec::new();
        for &chain_id in &self.chains {
            listed.extend(
                self.registry
                    .all_endpoints(chain_id)
                    .await
                    .into_iter()
                    .map(|endpoint| (chain_id, endpoint)),
            );
        }
        let present: HashSet<_> = listed
            .iter()
            .map(|(chain_id, endpoint)| (*chain_id, endpoint.url().to_owned()))
            .collect();
        let mut due = Vec::new();
        {
            let mut schedules = self.schedules.lock().await;
            schedules.retain(|key, _| present.contains(key));
            for (chain_id, endpoint) in listed {
                let key = (chain_id, endpoint.url().to_owned());
                let next = schedules.entry(key).or_insert(now);
                if let EndpointState::Cooling { until, .. } = endpoint.state(now)
                    && now < until
                {
                    *next = until;
                    continue;
                }
                if now >= *next {
                    *next = now + self.jittered_interval();
                    due.push((chain_id, endpoint));
                }
            }
        }

        for (chain_id, endpoint) in due {
            let manager = Arc::clone(self);
            tokio::spawn(async move {
                manager.probe_endpoint(chain_id, endpoint).await;
            });
        }
    }

    pub async fn probe_endpoint(&self, chain_id: u64, endpoint: Arc<Endpoint>) -> ProbeOutcome {
        self.probe_endpoint_at(chain_id, endpoint, Instant::now())
            .await
    }

    /// 显式时钟入口用于无需真实等待冷却窗口的确定性测试。
    pub async fn probe_endpoint_at(
        &self,
        chain_id: u64,
        endpoint: Arc<Endpoint>,
        now: Instant,
    ) -> ProbeOutcome {
        if !endpoint.begin_probe(now) {
            return ProbeOutcome::Skipped;
        }
        let Ok(_global_slot) = Arc::clone(&self.global_slots).acquire_owned().await else {
            return ProbeOutcome::Skipped;
        };
        let started = Instant::now();

        let chain_id_response = match self
            .rpc_call(&endpoint, "eth_chainId", json!("rpcrouter-probe-chain"))
            .await
        {
            Ok(response) => response,
            Err(None) => return ProbeOutcome::Skipped,
            Err(Some(signal)) => {
                endpoint.record_failure(now + started.elapsed(), signal.clone());
                return ProbeOutcome::Failed(signal.kind);
            }
        };
        let Some(actual_chain_id) = parse_hex_result(&chain_id_response) else {
            let signal = FailureSignal::new(FaultKind::InvalidResponse);
            endpoint.record_failure(now + started.elapsed(), signal.clone());
            return ProbeOutcome::Failed(signal.kind);
        };
        if actual_chain_id != chain_id {
            self.registry
                .remove_endpoint(chain_id, endpoint.url())
                .await;
            warn!(
                expected_chain_id = chain_id,
                actual_chain_id,
                endpoint = endpoint.url(),
                "probe removed endpoint with mismatched chain ID"
            );
            return ProbeOutcome::RemovedWrongChain {
                actual: actual_chain_id,
            };
        }

        let block_response = match self
            .rpc_call(&endpoint, "eth_blockNumber", json!("rpcrouter-probe-block"))
            .await
        {
            Ok(response) => response,
            Err(None) => return ProbeOutcome::Skipped,
            Err(Some(signal)) => {
                endpoint.record_failure(now + started.elapsed(), signal.clone());
                return ProbeOutcome::Failed(signal.kind);
            }
        };
        let Some(height) = parse_hex_result(&block_response) else {
            let signal = FailureSignal::new(FaultKind::InvalidResponse);
            endpoint.record_failure(now + started.elapsed(), signal.clone());
            return ProbeOutcome::Failed(signal.kind);
        };

        let latency = started.elapsed();
        let finished = now + latency;
        endpoint.record_success(finished, latency, true);
        self.registry
            .record_probe_height(chain_id, &endpoint, height, finished)
            .await;
        debug!(chain_id, endpoint = endpoint.url(), height, "probe passed");
        ProbeOutcome::Passed
    }

    async fn rpc_call(
        &self,
        endpoint: &Arc<Endpoint>,
        method: &str,
        request_id: Value,
    ) -> std::result::Result<Value, Option<FailureSignal>> {
        let Some(lease) = endpoint.try_acquire_probe() else {
            return Err(None);
        };
        let request = json!({
            "jsonrpc": "2.0",
            "id": request_id.clone(),
            "method": method,
            "params": []
        });
        let request_body = serde_json::to_vec(&request)
            .map_err(|_| Some(FailureSignal::new(FaultKind::InvalidResponse)))?;
        let started = Instant::now();
        let response = timeout(
            self.request_timeout,
            self.client
                .post(lease.endpoint().url())
                .header(CONTENT_TYPE, "application/json")
                .body(request_body)
                .send(),
        )
        .await;
        let mut response = match response {
            Err(_) => return Err(Some(FailureSignal::new(FaultKind::Timeout))),
            Ok(Err(_)) => return Err(Some(FailureSignal::new(FaultKind::Transport))),
            Ok(Ok(response)) => response,
        };
        let status = response.status();
        let headers = response.headers().clone();
        let mut body = Vec::new();
        loop {
            let chunk = match response.chunk().await {
                Ok(chunk) => chunk,
                Err(_) => return Err(Some(FailureSignal::new(FaultKind::Transport))),
            };
            let Some(chunk) = chunk else {
                break;
            };
            if body.len().saturating_add(chunk.len()) > PROBE_BODY_LIMIT {
                return Err(Some(FailureSignal::new(FaultKind::InvalidResponse)));
            }
            body.extend_from_slice(&chunk);
        }
        match classify_response(
            status,
            &headers,
            &body,
            started.elapsed(),
            &request_id,
            self.slow_threshold,
        ) {
            ResponseClassification::Valid(value) => Ok(value),
            ResponseClassification::Degraded { fault, .. } => Err(Some(FailureSignal::new(fault))),
            ResponseClassification::Failure(signal) => Err(Some(signal)),
        }
    }

    pub fn jittered_interval(&self) -> Duration {
        let min_millis = self.min_interval.as_millis().min(u128::from(u64::MAX)) as u64;
        let max_millis = self.max_interval.as_millis().min(u128::from(u64::MAX)) as u64;
        Duration::from_millis(rand::rng().random_range(min_millis..=max_millis))
    }
}

fn parse_hex_result(response: &Value) -> Option<u64> {
    let value = response.get("result")?.as_str()?;
    let digits = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))?;
    u64::from_str_radix(digits, 16).ok()
}

pub fn spawn(manager: Arc<ProbeManager>) {
    tokio::spawn(async move {
        info!("health probe scheduler started");
        manager.run().await;
    });
}

#[cfg(test)]
mod tests {
    use axum::{Json, Router, body::Bytes, extract::State, routing::post};

    use crate::{
        chainlist::{ChainEndpoints, ChainlistSnapshot},
        config::{ProbeConfig, UpstreamConfig},
    };

    use super::*;

    #[derive(Clone)]
    struct MockState {
        chain_id: u64,
        height: u64,
    }

    async fn mock_rpc(State(state): State<MockState>, body: Bytes) -> Json<Value> {
        let request: Value = serde_json::from_slice(&body).expect("probe request");
        let result = match request["method"].as_str().expect("probe method") {
            "eth_chainId" => format!("0x{:x}", state.chain_id),
            "eth_blockNumber" => format!("0x{:x}", state.height),
            method => panic!("unexpected method {method}"),
        };
        Json(json!({"jsonrpc":"2.0", "id":request["id"], "result":result}))
    }

    async fn mock_url(chain_id: u64, height: u64) -> String {
        let app = Router::new()
            .route("/", post(mock_rpc))
            .with_state(MockState { chain_id, height });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock");
        let address = listener.local_addr().expect("mock address");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve mock");
        });
        format!("http://{address}/")
    }

    fn test_config() -> Config {
        Config {
            chains: vec![1],
            chain_overrides: Vec::new(),
            upstream: UpstreamConfig {
                slow_threshold_ms: 100,
                default_rps: 100,
                ..UpstreamConfig::default()
            },
            probe: ProbeConfig {
                min_interval_seconds: 15,
                max_interval_seconds: 30,
                max_concurrency: 2,
                request_timeout_ms: 100,
                max_block_lag: 5,
            },
            ..Config::default()
        }
    }

    async fn setup(url: &str) -> (Arc<Registry>, ProbeManager, Arc<Endpoint>) {
        let config = test_config();
        let registry = Arc::new(Registry::new(&config));
        registry
            .apply_snapshot(&ChainlistSnapshot {
                chains: vec![ChainEndpoints {
                    chain_id: 1,
                    name: "Test".to_owned(),
                    endpoints: vec![url.to_owned()],
                }],
            })
            .await;
        let endpoint = registry.endpoint(1, url).await.expect("endpoint");
        let manager = ProbeManager::new(Arc::clone(&registry), &config).expect("probe manager");
        (registry, manager, endpoint)
    }

    #[tokio::test]
    async fn two_probe_passes_activate_endpoint_and_track_head() {
        let url = mock_url(1, 1234).await;
        let (registry, manager, endpoint) = setup(&url).await;
        assert_eq!(
            manager.probe_endpoint(1, Arc::clone(&endpoint)).await,
            ProbeOutcome::Passed
        );
        assert_eq!(
            endpoint.state(Instant::now()),
            EndpointState::Probation { passes: 1 }
        );
        assert_eq!(
            manager.probe_endpoint(1, Arc::clone(&endpoint)).await,
            ProbeOutcome::Passed
        );
        assert_eq!(endpoint.state(Instant::now()), EndpointState::Active);
        assert_eq!(registry.head(1), 1234);
    }

    #[tokio::test]
    async fn wrong_chain_id_is_removed() {
        let url = mock_url(143, 1234).await;
        let (registry, manager, endpoint) = setup(&url).await;
        assert_eq!(
            manager.probe_endpoint(1, endpoint).await,
            ProbeOutcome::RemovedWrongChain { actual: 143 }
        );
        assert!(registry.all_endpoints(1).await.is_empty());
    }

    #[test]
    fn jitter_is_uniformly_bounded() {
        let config = test_config();
        let manager =
            ProbeManager::new(Arc::new(Registry::new(&config)), &config).expect("probe manager");
        let mut distinct = HashSet::new();
        for _ in 0..100 {
            let interval = manager.jittered_interval();
            assert!(interval >= Duration::from_secs(15));
            assert!(interval <= Duration::from_secs(30));
            distinct.insert(interval);
        }
        assert!(distinct.len() > 1);
    }
}

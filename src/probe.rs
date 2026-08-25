use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use rand::Rng;
use reqwest::{Client, header::CONTENT_TYPE};
use serde_json::{Value, json};
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinSet,
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

type ProbeQueueRx = mpsc::Receiver<(u64, Arc<Endpoint>)>;

struct ProbeGuard<'a>(&'a Endpoint);

impl Drop for ProbeGuard<'_> {
    fn drop(&mut self) {
        self.0.end_probe();
    }
}

struct InFlightGuard<'a>(&'a AtomicU64);

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

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
    schedules: Mutex<HashMap<(u64, String), Instant>>,
    min_interval: Duration,
    max_interval: Duration,
    request_timeout: Duration,
    slow_threshold: Duration,
    /// 有界工作池：due 端点入队，worker 消费。
    queue_tx: mpsc::Sender<(u64, Arc<Endpoint>)>,
    /// 工作池接收端（在 start_workers 中取出）。
    queue_rx: Mutex<Option<ProbeQueueRx>>,
    /// 激活 kick 通道。
    kick_rx: Mutex<tokio::sync::broadcast::Receiver<u64>>,
    /// 在飞探针计数。
    in_flight: Arc<AtomicU64>,
    /// 队列深度（近似，由 channel 长度估算）。
    queue_depth: Arc<AtomicU64>,
    queued: StdMutex<HashSet<(u64, String)>>,
    /// 工作池并发数。
    max_concurrency: usize,
}

impl ProbeManager {
    pub fn new(registry: Arc<Registry>, config: &Config) -> Result<Self> {
        let client = Client::builder()
            .user_agent(concat!("rpcrouter/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("failed to build probe HTTP client")?;
        let (queue_tx, queue_rx) = mpsc::channel(4096);
        let kick_rx = registry.activation_channel().subscribe();
        // 共享 Registry 的原子计数器，使 metrics encode 能读到最新值。
        let in_flight = Arc::clone(&registry.probe_in_flight);
        let queue_depth = Arc::clone(&registry.probe_queue_depth);
        Ok(Self {
            registry,
            client,
            schedules: Mutex::new(HashMap::new()),
            min_interval: Duration::from_secs(config.probe.min_interval_seconds),
            max_interval: Duration::from_secs(config.probe.max_interval_seconds),
            request_timeout: Duration::from_millis(config.probe.request_timeout_ms),
            slow_threshold: Duration::from_millis(config.upstream.slow_threshold_ms),
            queue_tx,
            queue_rx: Mutex::new(Some(queue_rx)),
            kick_rx: Mutex::new(kick_rx),
            in_flight,
            queue_depth,
            queued: StdMutex::new(HashSet::new()),
            max_concurrency: config.probe.max_concurrency,
        })
    }

    /// 取出并启动有界工作池（必须在 manager 被 Arc 包装后调用，仅一次）。
    fn take_worker_pool(self: &Arc<Self>) -> ProbeQueueRx {
        self.queue_rx
            .try_lock()
            .expect("queue_rx lock")
            .take()
            .expect("workers already started")
    }

    pub async fn run(self: Arc<Self>) {
        loop {
            self.schedule_due_probes().await;
            self.process_kicks().await;
            sleep(SCHEDULER_TICK).await;
        }
    }

    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    pub fn queue_depth(&self) -> u64 {
        self.queue_depth.load(Ordering::Relaxed)
    }

    fn enqueue(&self, chain_id: u64, endpoint: Arc<Endpoint>) {
        let key = (chain_id, endpoint.url().to_owned());
        let mut queued = self
            .queued
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !queued.insert(key.clone()) {
            return;
        }
        if self.queue_tx.try_send((chain_id, endpoint)).is_ok() {
            self.queue_depth.fetch_add(1, Ordering::Relaxed);
        } else {
            queued.remove(&key);
        }
    }

    fn mark_received(&self) {
        self.queue_depth.fetch_sub(1, Ordering::Relaxed);
    }

    fn complete(&self, chain_id: u64, endpoint: &Endpoint) {
        let mut queued = self
            .queued
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        queued.remove(&(chain_id, endpoint.url().to_owned()));
    }

    async fn schedule_after_completion(&self, chain_id: u64, endpoint: &Endpoint) {
        self.schedules.lock().await.insert(
            (chain_id, endpoint.url().to_owned()),
            Instant::now() + self.jittered_interval(),
        );
    }

    async fn schedule_due_probes(self: &Arc<Self>) {
        let now = Instant::now();
        let hot_chains = self.registry.hot_chain_ids();

        let mut listed = Vec::new();
        for chain_id in hot_chains {
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
            self.enqueue(chain_id, endpoint);
        }
    }

    /// 处理激活 kick：收到 kick 后立即将该链全部端点入队。
    async fn process_kicks(self: &Arc<Self>) {
        let mut rx = self.kick_rx.lock().await;
        loop {
            let chain_id = match rx.try_recv() {
                Ok(chain_id) => chain_id,
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
            };
            let now = Instant::now();
            let endpoints = self.registry.all_endpoints(chain_id).await;
            let mut schedules = self.schedules.lock().await;
            for endpoint in endpoints {
                let key = (chain_id, endpoint.url().to_owned());
                if let EndpointState::Cooling { until, .. } = endpoint.state(now)
                    && now < until
                {
                    schedules.insert(key, until);
                    continue;
                }
                let should_kick = schedules.get(&key).is_none_or(|next| *next <= now);
                schedules.insert(key, now + self.jittered_interval());
                if should_kick {
                    self.enqueue(chain_id, endpoint);
                }
            }
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
        let _probe_guard = ProbeGuard(&endpoint);
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        let _in_flight_guard = InFlightGuard(&self.in_flight);
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

/// 有界工作池：最多 N 个探针任务同时运行；不会为每个排队项创建等待信号量的任务。
async fn worker_pool(manager: Arc<ProbeManager>, mut rx: ProbeQueueRx) {
    let mut active = JoinSet::new();
    loop {
        while active.len() >= manager.max_concurrency {
            let _ = active.join_next().await;
        }
        tokio::select! {
            item = rx.recv() => {
                let Some((chain_id, endpoint)) = item else { break };
                manager.mark_received();
                let manager = Arc::clone(&manager);
                active.spawn(async move {
                    manager.probe_endpoint(chain_id, Arc::clone(&endpoint)).await;
                    manager.schedule_after_completion(chain_id, &endpoint).await;
                    manager.complete(chain_id, &endpoint);
                });
            }
            joined = active.join_next(), if !active.is_empty() => {
                let _ = joined;
            }
        }
    }
    while active.join_next().await.is_some() {}
}

fn parse_hex_result(response: &Value) -> Option<u64> {
    let value = response.get("result")?.as_str()?;
    let digits = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))?;
    u64::from_str_radix(digits, 16).ok()
}

pub fn spawn(manager: Arc<ProbeManager>) {
    let rx = manager.take_worker_pool();
    tokio::spawn(worker_pool(Arc::clone(&manager), rx));
    tokio::spawn(async move {
        info!("health probe scheduler started");
        manager.run().await;
    });
}

#[cfg(test)]
mod tests {
    use axum::{Json, Router, body::Bytes, extract::State, routing::post};

    use crate::{
        chainlist::{Catalog, CatalogChain, CatalogEndpoint, ChainEndpoints, ChainlistSnapshot},
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

    #[tokio::test]
    async fn jitter_is_uniformly_bounded() {
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

    #[tokio::test]
    async fn dormant_chain_not_in_hot_chain_ids() {
        let config = Config {
            chains: vec![],
            discovery: crate::config::DiscoveryConfig {
                enabled: true,
                ..Default::default()
            },
            ..test_config()
        };
        let registry = Arc::new(Registry::new(&config));
        // 设置 catalog 含 chain 1（dormant，因为没有 pinned）。
        let catalog = Catalog {
            chains: vec![CatalogChain {
                chain_id: 1,
                name: "DormantChain".to_owned(),
                short_name: None,
                chain: None,
                slug: None,
                is_testnet: false,
                native_symbol: None,
                explorer_url: None,
                status: None,
                tvl: None,
                endpoints: vec![CatalogEndpoint {
                    url: "https://rpc.example".to_owned(),
                    tracking: None,
                }],
            }],
            by_id: HashMap::from([(1, 0)]),
        };
        registry.set_catalog(Arc::new(catalog)).await;
        // dormant 链不在 hot_chain_ids 中。
        assert!(registry.hot_chain_ids().is_empty());
    }
}

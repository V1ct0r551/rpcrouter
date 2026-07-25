use std::{
    collections::{HashMap, HashSet, VecDeque},
    num::NonZeroU32,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use dashmap::DashMap;
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use rand::Rng;
use serde::Serialize;
use tokio::{
    sync::{OwnedSemaphorePermit, RwLock, Semaphore},
    time::{Duration, Instant},
};

use crate::{
    chainlist::{ChainEndpoints, ChainlistSnapshot},
    config::Config,
    signals::{FailureSignal, FaultKind},
};

const ERROR_WINDOW: Duration = Duration::from_secs(60);
const STRIKE_DECAY_INTERVAL: Duration = Duration::from_secs(15 * 60);
const FAULTS_BEFORE_COOLING: u8 = 3;
const PROBATION_PASSES_REQUIRED: u8 = 2;

#[derive(Clone, Debug, PartialEq)]
pub enum EndpointState {
    Active,
    Cooling { until: Instant, strikes: u32 },
    Probation { passes: u8 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HealthStatus {
    Active,
    Cooling { until: Instant },
    Probation { passes: u8 },
}

struct HealthState {
    status: HealthStatus,
    strikes: u32,
    healthy_since: Option<Instant>,
    consecutive_faults: u8,
}

impl HealthState {
    fn probation() -> Self {
        Self {
            status: HealthStatus::Probation { passes: 0 },
            strikes: 0,
            healthy_since: None,
            consecutive_faults: 0,
        }
    }

    fn decay_strikes(&mut self, now: Instant) {
        if !matches!(self.status, HealthStatus::Active) {
            return;
        }
        let Some(healthy_since) = self.healthy_since else {
            return;
        };
        let elapsed = now.saturating_duration_since(healthy_since);
        let steps = elapsed.as_secs() / STRIKE_DECAY_INTERVAL.as_secs();
        if steps == 0 {
            return;
        }
        self.strikes = self
            .strikes
            .saturating_sub(steps.min(u64::from(u32::MAX)) as u32);
        self.healthy_since = Some(now);
    }
}

#[derive(Default)]
struct OutcomeWindow {
    entries: VecDeque<(Instant, bool)>,
}

impl OutcomeWindow {
    fn record(&mut self, now: Instant, failed: bool) {
        self.prune(now);
        self.entries.push_back((now, failed));
    }

    fn error_rate(&mut self, now: Instant) -> f64 {
        self.prune(now);
        if self.entries.is_empty() {
            return 0.0;
        }
        let failures = self.entries.iter().filter(|(_, failed)| *failed).count();
        failures as f64 / self.entries.len() as f64
    }

    fn prune(&mut self, now: Instant) {
        while self
            .entries
            .front()
            .is_some_and(|(at, _)| now.saturating_duration_since(*at) > ERROR_WINDOW)
        {
            self.entries.pop_front();
        }
    }
}

#[derive(Default)]
struct EndpointStats {
    outbound_requests: AtomicU64,
    failures: AtomicU64,
    rate_limited: AtomicU64,
    cooling_events: AtomicU64,
    probe_successes: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointStatsSnapshot {
    pub outbound_requests: u64,
    pub failures: u64,
    pub rate_limited: u64,
    pub cooling_events: u64,
    pub probe_successes: u64,
}

impl EndpointStats {
    fn snapshot(&self) -> EndpointStatsSnapshot {
        EndpointStatsSnapshot {
            outbound_requests: self.outbound_requests.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            rate_limited: self.rate_limited.load(Ordering::Relaxed),
            cooling_events: self.cooling_events.load(Ordering::Relaxed),
            probe_successes: self.probe_successes.load(Ordering::Relaxed),
        }
    }
}

pub struct Endpoint {
    url: String,
    rps: u32,
    concurrency: usize,
    bucket: DefaultDirectRateLimiter,
    inflight: Arc<Semaphore>,
    last_seen: AtomicU64,
    health: Mutex<HealthState>,
    outcomes: Mutex<OutcomeWindow>,
    latency_ewma_micros: AtomicU64,
    lag: AtomicU64,
    height_observation: Mutex<Option<(u64, Instant)>>,
    stats: EndpointStats,
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
            health: Mutex::new(HealthState::probation()),
            outcomes: Mutex::new(OutcomeWindow::default()),
            latency_ewma_micros: AtomicU64::new(0),
            lag: AtomicU64::new(0),
            height_observation: Mutex::new(None),
            stats: EndpointStats::default(),
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

    pub fn state(&self, now: Instant) -> EndpointState {
        let mut health = lock(&self.health);
        health.decay_strikes(now);
        match health.status {
            HealthStatus::Active => EndpointState::Active,
            HealthStatus::Cooling { until } => EndpointState::Cooling {
                until,
                strikes: health.strikes,
            },
            HealthStatus::Probation { passes } => EndpointState::Probation { passes },
        }
    }

    pub fn is_active(&self) -> bool {
        matches!(lock(&self.health).status, HealthStatus::Active)
    }

    pub fn begin_probe(&self, now: Instant) -> bool {
        let mut health = lock(&self.health);
        health.decay_strikes(now);
        match health.status {
            HealthStatus::Cooling { until } if now < until => false,
            HealthStatus::Cooling { .. } => {
                health.status = HealthStatus::Probation { passes: 0 };
                health.consecutive_faults = 0;
                true
            }
            HealthStatus::Active | HealthStatus::Probation { .. } => true,
        }
    }

    pub fn try_acquire(self: &Arc<Self>) -> Option<EndpointLease> {
        if !self.is_active() {
            return None;
        }
        self.acquire_outbound()
    }

    pub fn try_acquire_probe(self: &Arc<Self>) -> Option<EndpointLease> {
        self.acquire_outbound()
    }

    fn acquire_outbound(self: &Arc<Self>) -> Option<EndpointLease> {
        let permit = Arc::clone(&self.inflight).try_acquire_owned().ok()?;
        if self.bucket.check().is_err() {
            drop(permit);
            return None;
        }
        self.stats.outbound_requests.fetch_add(1, Ordering::Relaxed);
        Some(EndpointLease {
            endpoint: Arc::clone(self),
            _permit: permit,
        })
    }

    pub fn record_success(&self, now: Instant, latency: Duration, probe: bool) {
        self.update_latency(latency);
        lock(&self.outcomes).record(now, false);
        if probe {
            self.stats.probe_successes.fetch_add(1, Ordering::Relaxed);
        }
        let mut health = lock(&self.health);
        health.decay_strikes(now);
        health.consecutive_faults = 0;
        if let HealthStatus::Probation { passes } = health.status {
            let passes = passes.saturating_add(1);
            health.status = if passes >= PROBATION_PASSES_REQUIRED {
                health.healthy_since = Some(now);
                HealthStatus::Active
            } else {
                HealthStatus::Probation { passes }
            };
        }
    }

    pub fn record_degraded(&self, now: Instant, latency: Duration, kind: FaultKind) {
        self.update_latency(latency);
        self.record_failure(now, FailureSignal::new(kind));
    }

    pub fn record_failure(&self, now: Instant, signal: FailureSignal) {
        self.stats.failures.fetch_add(1, Ordering::Relaxed);
        if signal.kind == FaultKind::RateLimited {
            self.stats.rate_limited.fetch_add(1, Ordering::Relaxed);
        }
        lock(&self.outcomes).record(now, true);

        let mut health = lock(&self.health);
        health.decay_strikes(now);
        if let HealthStatus::Cooling { until } = health.status {
            if let Some(retry_after) = signal.retry_after {
                health.status = HealthStatus::Cooling {
                    until: until.max(now + retry_after),
                };
            }
            return;
        }

        health.consecutive_faults = health.consecutive_faults.saturating_add(1);
        if matches!(health.status, HealthStatus::Active) {
            health.healthy_since = Some(now);
        }
        let should_cool = signal.kind.requires_immediate_cooling()
            || matches!(health.status, HealthStatus::Probation { .. })
            || health.consecutive_faults >= FAULTS_BEFORE_COOLING;
        if !should_cool {
            return;
        }

        health.strikes = health.strikes.saturating_add(1);
        health.healthy_since = None;
        health.consecutive_faults = 0;
        let policy_delay = cooldown_for_strikes(health.strikes);
        let cooldown = signal
            .retry_after
            .map_or(policy_delay, |retry_after| retry_after.max(policy_delay));
        health.status = HealthStatus::Cooling {
            until: now + cooldown,
        };
        self.stats.cooling_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn score(&self, now: Instant) -> f64 {
        let latency_ms = match self.latency_ewma_micros.load(Ordering::Relaxed) {
            0 => 1_000.0,
            micros => micros as f64 / 1_000.0,
        };
        let error_penalty = lock(&self.outcomes).error_rate(now) * 5_000.0;
        let lag_penalty = self.lag.load(Ordering::Relaxed) as f64 * 250.0;
        let used_slots = self
            .concurrency
            .saturating_sub(self.inflight.available_permits());
        let concurrency_penalty = used_slots as f64 / self.concurrency as f64 * 500.0;
        latency_ms + error_penalty + lag_penalty + concurrency_penalty
    }

    pub fn latency_ewma_micros(&self) -> u64 {
        self.latency_ewma_micros.load(Ordering::Relaxed)
    }

    pub fn lag(&self) -> u64 {
        self.lag.load(Ordering::Relaxed)
    }

    pub fn stats(&self) -> EndpointStatsSnapshot {
        self.stats.snapshot()
    }

    fn update_latency(&self, latency: Duration) {
        let sample = latency.as_micros().min(u128::from(u64::MAX)) as u64;
        let mut previous = self.latency_ewma_micros.load(Ordering::Relaxed);
        loop {
            let updated = if previous == 0 {
                sample
            } else {
                previous.saturating_mul(4).saturating_add(sample) / 5
            };
            match self.latency_ewma_micros.compare_exchange_weak(
                previous,
                updated,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => previous = actual,
            }
        }
    }

    fn observe_height(&self, height: u64, now: Instant) {
        *lock(&self.height_observation) = Some((height, now));
    }

    fn recent_height(&self, now: Instant, freshness: Duration) -> Option<u64> {
        lock(&self.height_observation).and_then(|(height, observed_at)| {
            (now.saturating_duration_since(observed_at) <= freshness).then_some(height)
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
    rejected: RwLock<HashSet<String>>,
    head: AtomicU64,
}

impl ChainState {
    fn new(chain_id: u64, name: String) -> Self {
        Self {
            chain_id,
            name: RwLock::new(name),
            endpoints: RwLock::new(Vec::new()),
            rejected: RwLock::new(HashSet::new()),
            head: AtomicU64::new(0),
        }
    }
}

pub struct Registry {
    chains: DashMap<u64, Arc<ChainState>>,
    config: Config,
    user_visible_errors: AtomicU64,
}

impl Registry {
    pub fn new(config: &Config) -> Self {
        Self {
            chains: DashMap::new(),
            config: config.clone(),
            user_visible_errors: AtomicU64::new(0),
        }
    }

    /// 刷新时复用同 URL 的端点对象，避免丢失健康与出站限流状态。
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
        let rejected = state.rejected.read().await.clone();
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
            if disabled.contains(url.as_str())
                || rejected.contains(&url)
                || !present.insert(url.clone())
            {
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
                && !rejected.contains(endpoint.url())
                && age < self.config.chainlist.stale_grace_seconds
            {
                present.insert(endpoint.url.clone());
                merged.push(endpoint);
            }
        }
        *state.endpoints.write().await = merged;
    }

    /// 对 Active 集反复执行 P2C，生成不同端点的尝试顺序。
    pub async fn candidates(&self, chain_id: u64) -> Vec<Arc<Endpoint>> {
        let mut pool: Vec<_> = self
            .all_endpoints(chain_id)
            .await
            .into_iter()
            .filter(|endpoint| endpoint.is_active())
            .collect();
        let now = Instant::now();
        let mut ordered = Vec::with_capacity(pool.len());
        let mut rng = rand::rng();
        while pool.len() > 1 {
            let first = rng.random_range(0..pool.len());
            let mut second = rng.random_range(0..pool.len() - 1);
            if second >= first {
                second += 1;
            }
            let selected = if pool[first].score(now) <= pool[second].score(now) {
                first
            } else {
                second
            };
            ordered.push(pool.swap_remove(selected));
        }
        ordered.extend(pool);
        ordered
    }

    pub async fn all_endpoints(&self, chain_id: u64) -> Vec<Arc<Endpoint>> {
        let Some(state) = self.chain(chain_id) else {
            return Vec::new();
        };
        state.endpoints.read().await.clone()
    }

    pub async fn endpoint(&self, chain_id: u64, url: &str) -> Option<Arc<Endpoint>> {
        self.all_endpoints(chain_id)
            .await
            .into_iter()
            .find(|endpoint| endpoint.url() == url)
    }

    pub async fn remove_endpoint(&self, chain_id: u64, url: &str) -> bool {
        let Some(state) = self.chain(chain_id) else {
            return false;
        };
        state.rejected.write().await.insert(url.to_owned());
        let mut endpoints = state.endpoints.write().await;
        let previous_len = endpoints.len();
        endpoints.retain(|endpoint| endpoint.url() != url);
        endpoints.len() != previous_len
    }

    pub async fn record_probe_height(
        &self,
        chain_id: u64,
        endpoint: &Arc<Endpoint>,
        height: u64,
        now: Instant,
    ) {
        endpoint.observe_height(height, now);
        let Some(state) = self.chain(chain_id) else {
            return;
        };
        let freshness =
            Duration::from_secs(self.config.probe.max_interval_seconds.saturating_mul(2));
        let endpoints = state.endpoints.read().await.clone();
        let heights: Vec<_> = endpoints
            .iter()
            .filter_map(|item| item.recent_height(now, freshness))
            .collect();
        let Some(head) = trimmed_max(&heights) else {
            return;
        };
        state.head.store(head, Ordering::Relaxed);
        for item in &endpoints {
            if let Some(item_height) = item.recent_height(now, freshness) {
                item.lag
                    .store(head.saturating_sub(item_height), Ordering::Relaxed);
            }
        }
        if head.saturating_sub(height) > self.config.lag_threshold(chain_id) {
            endpoint.record_failure(now, FailureSignal::new(FaultKind::Lagging));
        }
    }

    pub fn head(&self, chain_id: u64) -> u64 {
        self.chain(chain_id)
            .map_or(0, |state| state.head.load(Ordering::Relaxed))
    }

    pub fn record_user_visible_error(&self) {
        self.user_visible_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn user_visible_errors(&self) -> u64 {
        self.user_visible_errors.load(Ordering::Relaxed)
    }

    pub async fn summaries(&self) -> Vec<ChainSummary> {
        let mut states: Vec<_> = self
            .chains
            .iter()
            .map(|entry| Arc::clone(entry.value()))
            .collect();
        states.sort_by_key(|state| state.chain_id);
        let now = Instant::now();

        let mut summaries = Vec::with_capacity(states.len());
        for state in states {
            let name = state.name.read().await.clone();
            let endpoints = state.endpoints.read().await;
            let mut active = 0;
            let mut cooling = 0;
            let mut probation = 0;
            for endpoint in endpoints.iter() {
                match endpoint.state(now) {
                    EndpointState::Active => active += 1,
                    EndpointState::Cooling { .. } => cooling += 1,
                    EndpointState::Probation { .. } => probation += 1,
                }
            }
            summaries.push(ChainSummary {
                chain_id: state.chain_id,
                name,
                endpoints: endpoints.len(),
                active,
                cooling,
                probation,
                head: state.head.load(Ordering::Relaxed),
            });
        }
        summaries
    }

    fn chain(&self, chain_id: u64) -> Option<Arc<ChainState>> {
        self.chains
            .get(&chain_id)
            .map(|state| Arc::clone(state.value()))
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainSummary {
    pub chain_id: u64,
    pub name: String,
    pub endpoints: usize,
    pub active: usize,
    pub cooling: usize,
    pub probation: usize,
    pub head: u64,
}

pub fn cooldown_for_strikes(strikes: u32) -> Duration {
    let seconds = match strikes {
        0 | 1 => 30,
        2 => 60,
        3 => 5 * 60,
        4 => 15 * 60,
        5 => 30 * 60,
        _ => 60 * 60,
    };
    Duration::from_secs(seconds)
}

pub fn trimmed_max(heights: &[u64]) -> Option<u64> {
    if heights.is_empty() {
        return None;
    }
    let mut heights = heights.to_vec();
    heights.sort_unstable();
    let trim = if heights.len() >= 3 {
        (heights.len() / 10).max(1)
    } else {
        0
    };
    heights.get(heights.len() - 1 - trim).copied()
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
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

    fn activate(endpoint: &Endpoint, now: Instant, latency_ms: u64) {
        endpoint.record_success(now, Duration::from_millis(latency_ms), true);
        endpoint.record_success(now, Duration::from_millis(latency_ms), true);
        assert_eq!(endpoint.state(now), EndpointState::Active);
    }

    #[test]
    fn exponential_cooldown_and_retry_after_are_enforced() {
        let endpoint = Endpoint::new("http://limited".to_owned(), 15, 8, 0);
        let mut now = Instant::now();
        activate(&endpoint, now, 10);
        for (strike, expected) in [30, 60, 300, 900, 1_800, 3_600].into_iter().enumerate() {
            let retry_after = (strike == 0).then_some(Duration::from_secs(45));
            endpoint.record_failure(
                now,
                FailureSignal {
                    kind: FaultKind::RateLimited,
                    retry_after,
                },
            );
            let expected = if strike == 0 { 45 } else { expected };
            let EndpointState::Cooling { until, strikes } = endpoint.state(now) else {
                panic!("endpoint must be cooling");
            };
            assert_eq!(strikes, strike as u32 + 1);
            assert_eq!(
                until.saturating_duration_since(now),
                Duration::from_secs(expected)
            );
            now = until;
            assert!(endpoint.begin_probe(now));
            endpoint.record_success(now, Duration::from_millis(10), true);
            endpoint.record_success(now, Duration::from_millis(10), true);
        }
    }

    #[test]
    fn strikes_decay_after_quiet_period() {
        let endpoint = Endpoint::new("http://limited".to_owned(), 15, 8, 0);
        let start = Instant::now();
        activate(&endpoint, start, 10);
        endpoint.record_failure(start, FailureSignal::new(FaultKind::RateLimited));
        let recovered = start + Duration::from_secs(30);
        assert!(endpoint.begin_probe(recovered));
        endpoint.record_success(recovered, Duration::from_millis(10), true);
        endpoint.record_success(recovered, Duration::from_millis(10), true);
        let later = recovered + STRIKE_DECAY_INTERVAL + Duration::from_secs(1);
        endpoint.record_failure(later, FailureSignal::new(FaultKind::RateLimited));
        let EndpointState::Cooling { until, strikes } = endpoint.state(later) else {
            panic!("endpoint must be cooling");
        };
        assert_eq!(strikes, 1);
        assert_eq!(
            until.saturating_duration_since(later),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn ordinary_faults_cool_after_threshold() {
        let endpoint = Endpoint::new("http://flaky".to_owned(), 15, 8, 0);
        let now = Instant::now();
        activate(&endpoint, now, 10);
        endpoint.record_failure(now, FailureSignal::new(FaultKind::ServerError));
        endpoint.record_failure(now, FailureSignal::new(FaultKind::ServerError));
        assert_eq!(endpoint.state(now), EndpointState::Active);
        endpoint.record_failure(now, FailureSignal::new(FaultKind::ServerError));
        assert!(matches!(endpoint.state(now), EndpointState::Cooling { .. }));
    }

    #[tokio::test]
    async fn p2c_prefers_lower_scored_active_endpoint() {
        let config = Config {
            chains: vec![1],
            ..Config::default()
        };
        let registry = Registry::new(&config);
        registry
            .apply_snapshot(&snapshot(&["http://slow", "http://fast"]))
            .await;
        let now = Instant::now();
        let slow = registry.endpoint(1, "http://slow").await.expect("slow");
        let fast = registry.endpoint(1, "http://fast").await.expect("fast");
        activate(&slow, now, 500);
        activate(&fast, now, 10);
        assert_eq!(registry.candidates(1).await[0].url(), "http://fast");

        registry
            .apply_snapshot(&snapshot(&["http://slow", "http://fast"]))
            .await;
        assert!(Arc::ptr_eq(
            &fast,
            &registry
                .endpoint(1, "http://fast")
                .await
                .expect("refreshed")
        ));
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
        let endpoint = registry
            .endpoint(1, "http://limited")
            .await
            .expect("endpoint");
        activate(&endpoint, Instant::now(), 10);

        let lease = endpoint.try_acquire().expect("first request is allowed");
        assert!(endpoint.try_acquire().is_none(), "concurrency slot is held");
        drop(lease);
        assert!(endpoint.try_acquire().is_none(), "rate token was consumed");
    }

    #[tokio::test]
    async fn removes_rejected_endpoint_across_refresh() {
        let config = Config {
            chains: vec![1],
            ..Config::default()
        };
        let registry = Registry::new(&config);
        registry.apply_snapshot(&snapshot(&["http://wrong"])).await;
        assert!(registry.remove_endpoint(1, "http://wrong").await);
        registry.apply_snapshot(&snapshot(&["http://wrong"])).await;
        assert!(registry.all_endpoints(1).await.is_empty());
    }

    #[test]
    fn computes_trimmed_max_without_high_outlier() {
        assert_eq!(trimmed_max(&[]), None);
        assert_eq!(trimmed_max(&[10]), Some(10));
        assert_eq!(trimmed_max(&[10, 11]), Some(11));
        assert_eq!(trimmed_max(&[100, 101, 10_000]), Some(101));
        assert_eq!(trimmed_max(&[98, 99, 100, 101, 10_000]), Some(101));
    }
}

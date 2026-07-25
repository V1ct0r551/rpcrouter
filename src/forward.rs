use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use reqwest::{Client, header::CONTENT_TYPE};
use serde::Deserialize;
use serde_json::{Value, json, value::RawValue};
use tokio::time::{Instant, timeout};
use tracing::debug;

use crate::{
    cache::{CacheLookup, CachedResponse, ResponseCache},
    classify::Classifier,
    config::Config,
    hedge::HedgeGate,
    metrics::Metrics,
    registry::{Endpoint, EndpointLease, Registry},
    signals::{FailureSignal, FaultKind, ResponseClassification, classify_response},
};

const MAX_UPSTREAM_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

pub struct Forwarder {
    registry: Arc<Registry>,
    client: Client,
    classifier: Classifier,
    cache: ResponseCache,
    metrics: Arc<Metrics>,
    hedge_gate: HedgeGate,
    hedge_delay: Duration,
    hedge_minimum_active: usize,
    request_timeout: Duration,
    slow_threshold: Duration,
    deadline: Duration,
    max_attempts: usize,
}

impl Forwarder {
    pub fn new(registry: Arc<Registry>, config: &Config) -> Result<Self> {
        let metrics = Arc::new(Metrics::new().context("failed to create metrics registry")?);
        Self::with_metrics(registry, config, metrics)
    }

    pub fn with_metrics(
        registry: Arc<Registry>,
        config: &Config,
        metrics: Arc<Metrics>,
    ) -> Result<Self> {
        let client = Client::builder()
            .user_agent(concat!("rpcrouter/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("failed to build upstream HTTP client")?;
        Ok(Self {
            registry,
            client,
            classifier: Classifier::new(config),
            cache: ResponseCache::new(config),
            metrics,
            hedge_gate: HedgeGate::new(config),
            hedge_delay: Duration::from_millis(config.hedging.delay_ms),
            hedge_minimum_active: config.hedging.min_active_endpoints,
            request_timeout: Duration::from_millis(config.upstream.request_timeout_ms),
            slow_threshold: Duration::from_millis(config.upstream.slow_threshold_ms),
            deadline: Duration::from_millis(config.upstream.deadline_ms),
            max_attempts: config.upstream.max_attempts,
        })
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        Arc::clone(&self.metrics)
    }

    pub fn cache(&self) -> &ResponseCache {
        &self.cache
    }

    pub fn hedge_counts(&self) -> (u64, u64) {
        self.hedge_gate.counts()
    }

    pub async fn execute(&self, chain_id: u64, request: Value) -> Value {
        let serialized = match serde_json::to_string(&request) {
            Ok(serialized) => serialized,
            Err(error) => {
                let request_id = request.get("id").cloned().unwrap_or(Value::Null);
                debug!(chain_id, error = %error, "JSON-RPC request serialization failed");
                self.metrics.record_ingress(chain_id);
                return self.exhausted(chain_id, request_id);
            }
        };
        let raw = match RawValue::from_string(serialized) {
            Ok(raw) => raw,
            Err(error) => {
                let request_id = request.get("id").cloned().unwrap_or(Value::Null);
                debug!(chain_id, error = %error, "JSON-RPC RawValue conversion failed");
                self.metrics.record_ingress(chain_id);
                return self.exhausted(chain_id, request_id);
            }
        };
        self.execute_raw(chain_id, &raw).await
    }

    pub async fn execute_raw(&self, chain_id: u64, request: &RawValue) -> Value {
        self.metrics.record_ingress(chain_id);
        let started = Instant::now();
        let metadata: RawRequestMetadata<'_> = match serde_json::from_str(request.get()) {
            Ok(metadata) => metadata,
            Err(error) => {
                debug!(chain_id, error = %error, "JSON-RPC metadata parsing failed");
                let response = self.exhausted(chain_id, Value::Null);
                self.metrics.record_latency(chain_id, started.elapsed());
                return response;
            }
        };
        let request_id = metadata.id;
        let read_only = metadata
            .method
            .is_some_and(|method| self.classifier.is_read_only(method));
        let cache_plan = metadata.method.and_then(|method| {
            self.classifier.cache_plan(
                chain_id,
                method,
                metadata.params,
                self.registry.head(chain_id),
            )
        });

        let response = if let Some(plan) = cache_plan {
            match self.cache.lookup(plan).await {
                CacheLookup::Hit(cached) => {
                    self.metrics.record_cache_lookup(chain_id, true);
                    self.metrics.record_failover_depth(chain_id, 0);
                    cached.with_id(request_id.clone())
                }
                CacheLookup::Leader(leader) => {
                    self.metrics.record_cache_lookup(chain_id, false);
                    self.metrics.record_cache_miss_role(chain_id, false);
                    let response = self
                        .execute_uncached(
                            chain_id,
                            request.get().as_bytes(),
                            &request_id,
                            read_only,
                        )
                        .await;
                    if let Some(cached) = CachedResponse::from_plan_success(&response, plan) {
                        self.cache.insert(plan, Arc::clone(&cached)).await;
                        leader.complete_success(cached);
                    } else {
                        leader.complete_failure();
                    }
                    response
                }
                CacheLookup::Follower(follower) => {
                    self.metrics.record_cache_lookup(chain_id, false);
                    if let Some(cached) = follower.wait().await {
                        self.metrics.record_cache_miss_role(chain_id, true);
                        self.metrics.record_failover_depth(chain_id, 0);
                        cached.with_id(request_id.clone())
                    } else {
                        self.metrics.record_cache_miss_role(chain_id, false);
                        let response = self
                            .execute_uncached(
                                chain_id,
                                request.get().as_bytes(),
                                &request_id,
                                read_only,
                            )
                            .await;
                        if let Some(cached) = CachedResponse::from_plan_success(&response, plan) {
                            self.cache.insert(plan, cached).await;
                        }
                        response
                    }
                }
            }
        } else {
            self.execute_uncached(chain_id, request.get().as_bytes(), &request_id, read_only)
                .await
        };
        self.metrics.record_latency(chain_id, started.elapsed());
        response
    }

    async fn execute_uncached(
        &self,
        chain_id: u64,
        body: &[u8],
        request_id: &Value,
        read_only: bool,
    ) -> Value {
        let expires_at = Instant::now() + self.deadline;
        let candidates = self.registry.candidates(chain_id).await;
        let mut next_candidate = 0;
        let mut started_attempts = 0;
        let mut failures = 0;

        let hedge_eligible = read_only
            && self.hedge_gate.enabled()
            && candidates.len() >= 2
            && self
                .registry
                .healthy_for_hedging(chain_id, self.hedge_minimum_active)
                .await;
        if hedge_eligible {
            let primary_endpoint = Arc::clone(&candidates[0]);
            next_candidate = 1;
            if let Some(primary_lease) = primary_endpoint.try_acquire() {
                started_attempts += 1;
                self.hedge_gate.record_primary();
                self.metrics
                    .record_upstream(chain_id, primary_endpoint.url());
                let mut primary = Box::pin(self.perform_attempt(
                    primary_endpoint,
                    primary_lease,
                    body,
                    request_id,
                    expires_at,
                ));
                tokio::select! {
                    completion = &mut primary => {
                        match self.apply_completion(chain_id, completion, started_attempts, failures) {
                            Ok(response) => return response,
                            Err(()) => failures += 1,
                        }
                    }
                    () = tokio::time::sleep(self.hedge_delay) => {
                        if started_attempts < self.max_attempts
                            && self.registry
                                .healthy_for_hedging(chain_id, self.hedge_minimum_active)
                                .await
                        {
                            let hedge_endpoint = Arc::clone(&candidates[1]);
                            next_candidate = 2;
                            if let Some(hedge_lease) = hedge_endpoint.try_acquire() {
                                if self.hedge_gate.try_acquire() {
                                    started_attempts += 1;
                                    self.metrics.record_upstream(chain_id, hedge_endpoint.url());
                                    self.metrics.record_hedge(chain_id);
                                    let mut hedge = Box::pin(self.perform_attempt(
                                        hedge_endpoint,
                                        hedge_lease,
                                        body,
                                        request_id,
                                        expires_at,
                                    ));
                                    let (first, second) = tokio::select! {
                                        completion = &mut primary => (completion, hedge),
                                        completion = &mut hedge => (completion, primary),
                                    };
                                    match self.apply_completion(chain_id, first, started_attempts, failures) {
                                        Ok(response) => return response,
                                        Err(()) => failures += 1,
                                    }
                                    let second = second.await;
                                    match self.apply_completion(chain_id, second, started_attempts, failures) {
                                        Ok(response) => return response,
                                        Err(()) => failures += 1,
                                    }
                                } else {
                                    drop(hedge_lease);
                                    let completion = primary.await;
                                    match self.apply_completion(chain_id, completion, started_attempts, failures) {
                                        Ok(response) => return response,
                                        Err(()) => failures += 1,
                                    }
                                }
                            } else {
                                let completion = primary.await;
                                match self.apply_completion(chain_id, completion, started_attempts, failures) {
                                    Ok(response) => return response,
                                    Err(()) => failures += 1,
                                }
                            }
                        } else {
                            let completion = primary.await;
                            match self.apply_completion(chain_id, completion, started_attempts, failures) {
                                Ok(response) => return response,
                                Err(()) => failures += 1,
                            }
                        }
                    }
                }
            }
        }

        while next_candidate < candidates.len() && started_attempts < self.max_attempts {
            let endpoint = Arc::clone(&candidates[next_candidate]);
            next_candidate += 1;
            if Instant::now() >= expires_at {
                break;
            }
            let Some(lease) = endpoint.try_acquire() else {
                continue;
            };
            started_attempts += 1;
            self.hedge_gate.record_primary();
            self.metrics.record_upstream(chain_id, endpoint.url());
            let completion = self
                .perform_attempt(endpoint, lease, body, request_id, expires_at)
                .await;
            match self.apply_completion(chain_id, completion, started_attempts, failures) {
                Ok(response) => return response,
                Err(()) => failures += 1,
            }
        }

        self.metrics.record_failover_depth(chain_id, failures);
        self.exhausted(chain_id, request_id.clone())
    }

    async fn perform_attempt(
        &self,
        endpoint: Arc<Endpoint>,
        _lease: EndpointLease,
        body: &[u8],
        request_id: &Value,
        expires_at: Instant,
    ) -> AttemptCompletion {
        let started = Instant::now();
        let remaining = expires_at.saturating_duration_since(started);
        let result = if remaining.is_zero() {
            AttemptResult::Failure(FailureSignal::new(FaultKind::Timeout))
        } else {
            match timeout(
                self.request_timeout.min(remaining),
                self.send_attempt(endpoint.url(), body, request_id, started),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => AttemptResult::Failure(FailureSignal::new(FaultKind::Timeout)),
            }
        };
        let finished = Instant::now();
        AttemptCompletion {
            endpoint,
            result,
            finished,
            latency: finished.saturating_duration_since(started),
        }
    }

    fn apply_completion(
        &self,
        chain_id: u64,
        completion: AttemptCompletion,
        attempt: usize,
        failures: usize,
    ) -> std::result::Result<Value, ()> {
        match completion.result {
            AttemptResult::Valid(response) => {
                completion
                    .endpoint
                    .record_success(completion.finished, completion.latency, false);
                self.metrics.record_failover_depth(chain_id, failures);
                Ok(response)
            }
            AttemptResult::Degraded { response, fault } => {
                completion
                    .endpoint
                    .record_degraded(completion.finished, completion.latency, fault);
                self.metrics.record_failover_depth(chain_id, failures);
                Ok(response)
            }
            AttemptResult::Failure(signal) => {
                completion
                    .endpoint
                    .record_failure(completion.finished, signal.clone());
                debug!(
                    chain_id,
                    endpoint = completion.endpoint.url(),
                    attempt,
                    fault = ?signal.kind,
                    "upstream attempt failed"
                );
                Err(())
            }
        }
    }

    async fn send_attempt(
        &self,
        url: &str,
        body: &[u8],
        request_id: &Value,
        started: Instant,
    ) -> AttemptResult {
        let mut response = match self
            .client
            .post(url)
            .header(CONTENT_TYPE, "application/json")
            .body(body.to_vec())
            .send()
            .await
        {
            Ok(response) => response,
            Err(_) => {
                return AttemptResult::Failure(FailureSignal::new(FaultKind::Transport));
            }
        };
        let status = response.status();
        let headers = response.headers().clone();
        let mut response_body = Vec::new();
        loop {
            let chunk = match response.chunk().await {
                Ok(chunk) => chunk,
                Err(_) => {
                    return AttemptResult::Failure(FailureSignal::new(FaultKind::Transport));
                }
            };
            let Some(chunk) = chunk else {
                break;
            };
            if response_body.len().saturating_add(chunk.len()) > MAX_UPSTREAM_RESPONSE_BYTES {
                return AttemptResult::Failure(FailureSignal::new(FaultKind::InvalidResponse));
            }
            response_body.extend_from_slice(&chunk);
        }
        match classify_response(
            status,
            &headers,
            &response_body,
            started.elapsed(),
            request_id,
            self.slow_threshold,
        ) {
            ResponseClassification::Valid(response) => AttemptResult::Valid(response),
            ResponseClassification::Degraded { response, fault } => {
                AttemptResult::Degraded { response, fault }
            }
            ResponseClassification::Failure(signal) => AttemptResult::Failure(signal),
        }
    }

    fn exhausted(&self, chain_id: u64, request_id: Value) -> Value {
        self.registry.record_user_visible_error();
        self.metrics.record_user_visible_error(chain_id);
        all_endpoints_exhausted(chain_id, request_id)
    }
}

#[derive(Deserialize)]
struct RawRequestMetadata<'a> {
    #[serde(default)]
    method: Option<&'a str>,
    #[serde(default, borrow)]
    params: Option<&'a RawValue>,
    #[serde(default)]
    id: Value,
}

enum AttemptResult {
    Valid(Value),
    Degraded { response: Value, fault: FaultKind },
    Failure(FailureSignal),
}

struct AttemptCompletion {
    endpoint: Arc<Endpoint>,
    result: AttemptResult,
    finished: Instant,
    latency: Duration,
}

pub fn all_endpoints_exhausted(chain_id: u64, request_id: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {
            "code": -32000,
            "message": format!("rpcrouter: all upstream endpoints exhausted for chain {chain_id}")
        }
    })
}

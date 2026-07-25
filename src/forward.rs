use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use reqwest::{Client, header::CONTENT_TYPE};
use serde_json::{Value, json};
use tokio::time::{Instant, timeout};
use tracing::debug;

use crate::{
    config::Config,
    registry::Registry,
    signals::{FailureSignal, FaultKind, ResponseClassification, classify_response},
};

const MAX_UPSTREAM_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

pub struct Forwarder {
    registry: Arc<Registry>,
    client: Client,
    request_timeout: Duration,
    slow_threshold: Duration,
    deadline: Duration,
    max_attempts: usize,
}

impl Forwarder {
    pub fn new(registry: Arc<Registry>, config: &Config) -> Result<Self> {
        let client = Client::builder()
            .user_agent(concat!("rpcrouter/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("failed to build upstream HTTP client")?;
        Ok(Self {
            registry,
            client,
            request_timeout: Duration::from_millis(config.upstream.request_timeout_ms),
            slow_threshold: Duration::from_millis(config.upstream.slow_threshold_ms),
            deadline: Duration::from_millis(config.upstream.deadline_ms),
            max_attempts: config.upstream.max_attempts,
        })
    }

    pub async fn execute(&self, chain_id: u64, request: Value) -> Value {
        let request_id = request.get("id").cloned().unwrap_or(Value::Null);
        let body = match serde_json::to_vec(&request) {
            Ok(body) => body,
            Err(error) => {
                debug!(chain_id, error = %error, "JSON-RPC request serialization failed");
                return self.exhausted(chain_id, request_id);
            }
        };
        let expires_at = Instant::now() + self.deadline;
        let mut attempts = 0;

        for endpoint in self.registry.candidates(chain_id).await {
            if attempts >= self.max_attempts {
                break;
            }
            let remaining = expires_at.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let Some(lease) = endpoint.try_acquire() else {
                continue;
            };
            attempts += 1;
            let started = Instant::now();
            let attempt_timeout = self.request_timeout.min(remaining);
            let result = timeout(
                attempt_timeout,
                self.send_attempt(lease.endpoint().url(), &body, &request_id, started),
            )
            .await;
            let finished = Instant::now();
            let latency = finished.saturating_duration_since(started);

            match result {
                Ok(AttemptResult::Valid(response)) => {
                    endpoint.record_success(finished, latency, false);
                    return response;
                }
                Ok(AttemptResult::Degraded { response, fault }) => {
                    endpoint.record_degraded(finished, latency, fault);
                    return response;
                }
                Ok(AttemptResult::Failure(signal)) => {
                    endpoint.record_failure(finished, signal.clone());
                    debug!(
                        chain_id,
                        endpoint = endpoint.url(),
                        attempt = attempts,
                        fault = ?signal.kind,
                        "upstream attempt failed"
                    );
                }
                Err(_) => {
                    endpoint.record_failure(finished, FailureSignal::new(FaultKind::Timeout));
                    debug!(
                        chain_id,
                        endpoint = endpoint.url(),
                        attempt = attempts,
                        "upstream attempt timed out"
                    );
                }
            }
        }

        self.exhausted(chain_id, request_id)
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
        all_endpoints_exhausted(chain_id, request_id)
    }
}

enum AttemptResult {
    Valid(Value),
    Degraded { response: Value, fault: FaultKind },
    Failure(FailureSignal),
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

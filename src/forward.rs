use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use reqwest::{Client, StatusCode, header::CONTENT_TYPE};
use serde_json::{Value, json};
use tokio::time::{Instant, timeout};
use tracing::debug;

use crate::{config::Config, registry::Registry};

const MAX_UPSTREAM_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

pub struct Forwarder {
    registry: Arc<Registry>,
    client: Client,
    request_timeout: Duration,
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
                return all_endpoints_exhausted(chain_id, request_id);
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
            let attempt_timeout = self.request_timeout.min(remaining);
            let result = timeout(
                attempt_timeout,
                self.send_attempt(lease.endpoint().url(), &body, &request_id),
            )
            .await;

            match result {
                Ok(Ok(response)) => return response,
                Ok(Err(error)) => debug!(
                    chain_id,
                    endpoint = lease.endpoint().url(),
                    attempt = attempts,
                    error = %error,
                    "upstream attempt failed"
                ),
                Err(_) => debug!(
                    chain_id,
                    endpoint = lease.endpoint().url(),
                    attempt = attempts,
                    "upstream attempt timed out"
                ),
            }
        }

        all_endpoints_exhausted(chain_id, request_id)
    }

    async fn send_attempt(&self, url: &str, body: &[u8], request_id: &Value) -> Result<Value> {
        let mut response = self
            .client
            .post(url)
            .header(CONTENT_TYPE, "application/json")
            .body(body.to_vec())
            .send()
            .await
            .context("upstream request failed")?;
        let status = response.status();
        if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
            bail!("upstream returned HTTP {status}");
        }

        let mut response_body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .context("failed to read upstream response")?
        {
            if response_body.len().saturating_add(chunk.len()) > MAX_UPSTREAM_RESPONSE_BYTES {
                bail!("upstream response exceeds size limit");
            }
            response_body.extend_from_slice(&chunk);
        }
        let value: Value =
            serde_json::from_slice(&response_body).context("upstream returned non-JSON body")?;
        if !is_single_jsonrpc_response(&value, request_id) {
            bail!("upstream returned an invalid JSON-RPC response");
        }
        Ok(value)
    }
}

fn is_single_jsonrpc_response(value: &Value, request_id: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    (object.contains_key("result") || object.contains_key("error"))
        && object.get("id").is_some_and(|id| id == request_id)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_single_response_id() {
        assert!(is_single_jsonrpc_response(
            &json!({"jsonrpc":"2.0", "id":7, "result":"0x1"}),
            &json!(7)
        ));
        assert!(!is_single_jsonrpc_response(
            &json!({"jsonrpc":"2.0", "id":8, "result":"0x1"}),
            &json!(7)
        ));
        assert!(!is_single_jsonrpc_response(&json!([{"id":7}]), &json!(7)));
    }
}

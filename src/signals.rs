use std::{sync::OnceLock, time::Duration};

use regex::Regex;
use reqwest::{
    StatusCode,
    header::{CONTENT_TYPE, HeaderMap, RETRY_AFTER},
};
use serde_json::Value;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultKind {
    RateLimited,
    Authentication,
    ServerError,
    ClientError,
    Html,
    NonJson,
    InvalidResponse,
    RpcError,
    Timeout,
    Transport,
    Slow,
    Lagging,
}

impl FaultKind {
    pub fn requires_immediate_cooling(self) -> bool {
        matches!(self, Self::RateLimited | Self::Authentication)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FailureSignal {
    pub kind: FaultKind,
    pub retry_after: Option<Duration>,
}

impl FailureSignal {
    pub const fn new(kind: FaultKind) -> Self {
        Self {
            kind,
            retry_after: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ResponseClassification {
    Valid(Value),
    Degraded { response: Value, fault: FaultKind },
    Failure(FailureSignal),
}

pub fn classify_response(
    status: StatusCode,
    headers: &HeaderMap,
    body: &[u8],
    elapsed: Duration,
    request_id: &Value,
    slow_threshold: Duration,
) -> ResponseClassification {
    let body_text = String::from_utf8_lossy(body);

    if status == StatusCode::TOO_MANY_REQUESTS {
        return ResponseClassification::Failure(FailureSignal {
            kind: FaultKind::RateLimited,
            retry_after: parse_retry_after(headers),
        });
    }
    if status == StatusCode::FORBIDDEN && is_quota_message(&body_text) {
        return ResponseClassification::Failure(FailureSignal {
            kind: FaultKind::RateLimited,
            retry_after: parse_retry_after(headers),
        });
    }
    if status == StatusCode::FORBIDDEN && is_authentication_message(&body_text) {
        return ResponseClassification::Failure(FailureSignal::new(FaultKind::Authentication));
    }
    if status.is_server_error() {
        return ResponseClassification::Failure(FailureSignal::new(FaultKind::ServerError));
    }
    if is_html(headers, &body_text) {
        return ResponseClassification::Failure(FailureSignal::new(FaultKind::Html));
    }

    let response: Value = match serde_json::from_slice(body) {
        Ok(response) => response,
        Err(_) => {
            return ResponseClassification::Failure(FailureSignal::new(FaultKind::NonJson));
        }
    };
    let Some(object) = response.as_object() else {
        return ResponseClassification::Failure(FailureSignal::new(FaultKind::InvalidResponse));
    };
    if object.get("id") != Some(request_id)
        || (!object.contains_key("result") && !object.contains_key("error"))
    {
        return ResponseClassification::Failure(FailureSignal::new(FaultKind::InvalidResponse));
    }

    if let Some(error) = object.get("error") {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if is_quota_message(message) {
            return ResponseClassification::Failure(FailureSignal {
                kind: FaultKind::RateLimited,
                retry_after: parse_retry_after(headers),
            });
        }
        if is_authentication_message(message) {
            return ResponseClassification::Failure(FailureSignal::new(FaultKind::Authentication));
        }
        if !is_chain_error(error) {
            return ResponseClassification::Failure(FailureSignal::new(FaultKind::RpcError));
        }
    } else if !status.is_success() {
        return ResponseClassification::Failure(FailureSignal::new(FaultKind::ClientError));
    }

    if elapsed > slow_threshold {
        ResponseClassification::Degraded {
            response,
            fault: FaultKind::Slow,
        }
    } else {
        ResponseClassification::Valid(response)
    }
}

pub fn is_chain_error(error: &Value) -> bool {
    let code = error.get("code").and_then(Value::as_i64);
    if matches!(code, Some(-32700 | -32600 | -32601 | -32602)) {
        return true;
    }
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    message.contains("execution reverted")
        || message.contains("revert reason")
        || message.contains("already known")
        || message.contains("nonce too low")
        || message.contains("insufficient funds")
}

pub fn parse_retry_after(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?.trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let retry_at = httpdate::parse_http_date(value).ok()?;
    Some(
        retry_at
            .duration_since(std::time::SystemTime::now())
            .unwrap_or(Duration::ZERO),
    )
}

fn is_quota_message(message: &str) -> bool {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN
        .get_or_init(|| {
            Regex::new(
                r"(?i)rate[ _-]?limit|too many requests|request rate exceeded|compute units?|capacity|throttl|quota",
            )
            .expect("valid quota signal regex")
        })
        .is_match(message)
}

fn is_authentication_message(message: &str) -> bool {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN
        .get_or_init(|| {
            Regex::new(
                r"(?i)api[ _-]?key|authentication|unauthori[sz]ed|project[ _-]?id|access[ _-]?token|missing credentials|invalid credentials",
            )
            .expect("valid authentication signal regex")
        })
        .is_match(message)
}

fn is_html(headers: &HeaderMap, body: &str) -> bool {
    let content_type_is_html = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().contains("text/html"));
    let trimmed = body.trim_start().to_ascii_lowercase();
    content_type_is_html || trimmed.starts_with("<!doctype html") || trimmed.starts_with("<html")
}

#[cfg(test)]
mod tests {
    use reqwest::header::HeaderValue;
    use serde_json::json;

    use super::*;

    fn classify(status: StatusCode, headers: HeaderMap, value: Value) -> ResponseClassification {
        classify_response(
            status,
            &headers,
            value.to_string().as_bytes(),
            Duration::from_millis(10),
            &json!(1),
            Duration::from_secs(4),
        )
    }

    #[test]
    fn recognizes_http_rate_limit_and_retry_after() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("17"));
        assert_eq!(
            classify_response(
                StatusCode::TOO_MANY_REQUESTS,
                &headers,
                b"rate limited",
                Duration::ZERO,
                &json!(1),
                Duration::from_secs(4),
            ),
            ResponseClassification::Failure(FailureSignal {
                kind: FaultKind::RateLimited,
                retry_after: Some(Duration::from_secs(17)),
            })
        );
    }

    #[test]
    fn recognizes_provider_quota_and_authentication_messages() {
        for message in [
            "rate limit exceeded",
            "Too Many Requests",
            "request rate exceeded",
            "compute unit capacity exhausted",
            "request throttled",
            "monthly quota exceeded",
        ] {
            let result = classify(
                StatusCode::OK,
                HeaderMap::new(),
                json!({"jsonrpc":"2.0", "id":1, "error":{"code":-32000, "message":message}}),
            );
            assert!(matches!(
                result,
                ResponseClassification::Failure(FailureSignal {
                    kind: FaultKind::RateLimited,
                    ..
                })
            ));
        }

        let auth = classify(
            StatusCode::OK,
            HeaderMap::new(),
            json!({"jsonrpc":"2.0", "id":1, "error":{"code":-32000, "message":"API key is required"}}),
        );
        assert!(matches!(
            auth,
            ResponseClassification::Failure(FailureSignal {
                kind: FaultKind::Authentication,
                ..
            })
        ));
    }

    #[test]
    fn passes_chain_errors_through() {
        for error in [
            json!({"code":3, "message":"execution reverted: denied"}),
            json!({"code":-32601, "message":"method not found"}),
            json!({"code":-32602, "message":"invalid params"}),
        ] {
            let response = json!({"jsonrpc":"2.0", "id":1, "error":error});
            assert_eq!(
                classify(StatusCode::OK, HeaderMap::new(), response.clone()),
                ResponseClassification::Valid(response)
            );
        }
    }

    #[test]
    fn rejects_html_non_json_server_errors_and_wrong_ids() {
        let mut html_headers = HeaderMap::new();
        html_headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/html"));
        let cases = [
            (
                classify_response(
                    StatusCode::OK,
                    &html_headers,
                    b"<html>bad gateway</html>",
                    Duration::ZERO,
                    &json!(1),
                    Duration::from_secs(4),
                ),
                FaultKind::Html,
            ),
            (
                classify_response(
                    StatusCode::OK,
                    &HeaderMap::new(),
                    b"not json",
                    Duration::ZERO,
                    &json!(1),
                    Duration::from_secs(4),
                ),
                FaultKind::NonJson,
            ),
            (
                classify(
                    StatusCode::BAD_GATEWAY,
                    HeaderMap::new(),
                    json!({"id":1,"result":"unused"}),
                ),
                FaultKind::ServerError,
            ),
            (
                classify(
                    StatusCode::OK,
                    HeaderMap::new(),
                    json!({"jsonrpc":"2.0","id":2,"result":"0x1"}),
                ),
                FaultKind::InvalidResponse,
            ),
        ];
        for (classification, expected) in cases {
            assert!(matches!(
                classification,
                ResponseClassification::Failure(FailureSignal { kind, .. }) if kind == expected
            ));
        }
    }

    #[test]
    fn marks_slow_valid_response_as_degraded() {
        let response = json!({"jsonrpc":"2.0", "id":1, "result":"0x1"});
        assert_eq!(
            classify_response(
                StatusCode::OK,
                &HeaderMap::new(),
                response.to_string().as_bytes(),
                Duration::from_secs(5),
                &json!(1),
                Duration::from_secs(4),
            ),
            ResponseClassification::Degraded {
                response,
                fault: FaultKind::Slow,
            }
        );
    }
}

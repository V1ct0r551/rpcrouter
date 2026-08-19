//! 入口防护层（tower 中间件）。
//!
//! 在请求进入数据面转发**之前**做三层防护：
//! 1. 请求体大小上限（HTTP 413 + JSON-RPC 错误体）。
//! 2. 全局并发/背压（HTTP 503 + JSON-RPC 错误体），过载快速拒绝。
//! 3. 每 IP 限速（可选，HTTP 429 + JSON-RPC 错误体）。
//!
//! 语义边界：这些拒绝都发生在转发前，属于**入口侧防护**，**不**计入
//! `user_visible_errors`（那是上游侧承诺指标——请求已进入转发但所有上游端点耗尽）。
//! 入口拒绝只累计到 `rpcrouter_ingress_rejected_total{reason=...}` 这一独立计数。

use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use axum::{
    body::Body,
    extract::ConnectInfo,
    http::{HeaderValue, Request, StatusCode, header::AUTHORIZATION},
    response::Response,
};
use serde_json::{Value, json};
use tokio::sync::Semaphore;
use tower::{Layer, Service, ServiceExt, util::BoxCloneService};
use tracing::debug;

use crate::metrics::Metrics;

/// 入口防护拒绝原因，对应 `rpcrouter_ingress_rejected_total` 的 reason 标签。
pub const REASON_OVERLOAD: &str = "overload";
pub const REASON_BODY_TOO_LARGE: &str = "body_too_large";
pub const REASON_RATE_LIMITED: &str = "rate_limited";

/// 每 IP 限速器（governor）。默认关闭时由上层决定是否构建。
pub struct IpRateLimiter {
    limiter: governor::RateLimiter<
        SocketAddr,
        governor::state::keyed::DefaultKeyedStateStore<SocketAddr>,
        governor::clock::DefaultClock,
    >,
}

impl IpRateLimiter {
    /// 后台清理周期：周期性地丢弃过期 IP 桶，防止每个唯一 IP 永久驻留内存。
    pub const HOUSEKEEPING_INTERVAL: Duration = Duration::from_secs(60);

    pub fn new(requests_per_second: u64, burst: u64) -> Self {
        let rps = std::num::NonZeroU32::new(requests_per_second as u32)
            .expect("rate must be greater than zero");
        let burst =
            std::num::NonZeroU32::new(burst as u32).expect("burst must be greater than zero");
        // governor 0.10: per_second(rps) 的桶容量 = rps，allow_burst 再把容量撑大到 burst。
        let quota = governor::Quota::per_second(rps).allow_burst(burst);
        let limiter = governor::RateLimiter::keyed(quota);
        Self { limiter }
    }

    /// 尝试放行给定客户端地址；返回 false 表示该 IP 超限。
    pub fn allow(&self, addr: SocketAddr) -> bool {
        self.limiter.check_key(&addr).is_ok()
    }

    /// 启动后台清理任务：周期性调用 retain_recent() 丢弃已恢复到"全新"状态的桶，
    /// 保证每 IP 限速的内存有界（每个唯一 IP 不会永久驻留）。
    pub fn spawn_housekeeping(self: &Arc<Self>) {
        let limiter = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Self::HOUSEKEEPING_INTERVAL);
            // 先 tick 掉第一次立即触发，从下一周期开始清理。
            interval.tick().await;
            loop {
                interval.tick().await;
                limiter.limiter.retain_recent();
            }
        });
    }
}

/// 构建带防护层的服务。
///
/// `rate_limiter` 为 None 表示每 IP 限速关闭。
pub fn guarded_service<S>(
    inner: S,
    max_body_bytes: usize,
    semaphore: Arc<Semaphore>,
    metrics: Arc<Metrics>,
    rate_limiter: Option<Arc<IpRateLimiter>>,
) -> BoxCloneService<Request<Body>, Response, std::convert::Infallible>
where
    S: Service<Request<Body>, Response = Response, Error = std::convert::Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    let guarded = ConcurrencyLimit::new(inner, semaphore, metrics.clone());
    let guarded = BodyLimit::new(guarded, max_body_bytes, metrics.clone());
    match rate_limiter {
        Some(limiter) => PerIpRateLimit::new(guarded, limiter, metrics).boxed_clone(),
        None => guarded.boxed_clone(),
    }
}

/// 并发/背压层：超过 `semaphore` 容量的在飞请求立即拒绝（HTTP 503 + JSON-RPC 错误体）。
pub struct ConcurrencyLimit<S> {
    inner: S,
    semaphore: Arc<Semaphore>,
    metrics: Arc<Metrics>,
}

impl<S> ConcurrencyLimit<S> {
    pub fn new(inner: S, semaphore: Arc<Semaphore>, metrics: Arc<Metrics>) -> Self {
        Self {
            inner,
            semaphore,
            metrics,
        }
    }
}

impl<S> Clone for ConcurrencyLimit<S>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            semaphore: Arc::clone(&self.semaphore),
            metrics: Arc::clone(&self.metrics),
        }
    }
}

impl<S> Service<Request<Body>> for ConcurrencyLimit<S>
where
    S: Service<Request<Body>, Response = Response, Error = std::convert::Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = Response;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        // try_acquire_owned：拿不到 permit 说明已过载，立即拒绝，不排队。
        match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => {
                let guard = InFlightGuard::new(Arc::clone(&self.metrics));
                let mut inner = self.inner.clone();
                Box::pin(async move {
                    let _permit = permit;
                    let _guard = guard;
                    let response = inner.ready().await.unwrap().call(request).await.unwrap();
                    Ok(response)
                })
            }
            Err(_) => {
                self.metrics.record_ingress_rejected(REASON_OVERLOAD);
                debug!("ingress overloaded; rejecting request");
                Box::pin(async move {
                    Ok(rejection(
                        StatusCode::SERVICE_UNAVAILABLE,
                        -32000,
                        "rpcrouter: too many concurrent requests",
                    ))
                })
            }
        }
    }
}

/// 在飞计数 drop guard：构造时 +1，Drop 时 -1。
/// 用 RAII 保证即使请求 future 被取消（比如强制退出/超时），in_flight gauge 也一定回落。
struct InFlightGuard {
    metrics: Arc<Metrics>,
}

impl InFlightGuard {
    fn new(metrics: Arc<Metrics>) -> Self {
        metrics.in_flight_inc();
        Self { metrics }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.metrics.in_flight_dec();
    }
}

/// 请求体大小层：超限返回 HTTP 413 + JSON-RPC 错误体。
pub struct BodyLimit<S> {
    inner: S,
    limit: usize,
    metrics: Arc<Metrics>,
}

impl<S> BodyLimit<S> {
    pub fn new(inner: S, limit: usize, metrics: Arc<Metrics>) -> Self {
        Self {
            inner,
            limit,
            metrics,
        }
    }
}

impl<S> Clone for BodyLimit<S>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            limit: self.limit,
            metrics: Arc::clone(&self.metrics),
        }
    }
}

impl<S> Service<Request<Body>> for BodyLimit<S>
where
    S: Service<Request<Body>, Response = Response, Error = std::convert::Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = Response;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let (parts, body) = request.into_parts();
        let limit = self.limit;
        let metrics = Arc::clone(&self.metrics);
        let mut inner = self.inner.clone();
        Box::pin(async move {
            // axum::body::to_bytes 在 body 超过 limit 时返回错误，正好用来判超限。
            let body = match axum::body::to_bytes(body, limit).await {
                Ok(bytes) => bytes,
                Err(_) => {
                    metrics.record_ingress_rejected(REASON_BODY_TOO_LARGE);
                    debug!(limit, "request body exceeds configured limit");
                    return Ok(rejection(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        -32600,
                        "rpcrouter: request body too large",
                    ));
                }
            };
            let request = Request::from_parts(parts, Body::from(body));
            inner.ready().await.unwrap().call(request).await
        })
    }
}

/// 每 IP 限速层：超限返回 HTTP 429 + JSON-RPC 错误体。依赖 ConnectInfo<SocketAddr>。
pub struct PerIpRateLimit<S> {
    inner: S,
    limiter: Arc<IpRateLimiter>,
    metrics: Arc<Metrics>,
}

impl<S> PerIpRateLimit<S> {
    pub fn new(inner: S, limiter: Arc<IpRateLimiter>, metrics: Arc<Metrics>) -> Self {
        Self {
            inner,
            limiter,
            metrics,
        }
    }
}

impl<S> Clone for PerIpRateLimit<S>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            limiter: Arc::clone(&self.limiter),
            metrics: Arc::clone(&self.metrics),
        }
    }
}

impl<S> Service<Request<Body>> for PerIpRateLimit<S>
where
    S: Service<Request<Body>, Response = Response, Error = std::convert::Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = Response;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        // 取客户端 IP；拿不到 ConnectInfo 时放行（正常请求经 connect-info 注入总会带）。
        let allowed = request
            .extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .is_none_or(|info| self.limiter.allow(info.0));
        if !allowed {
            self.metrics.record_ingress_rejected(REASON_RATE_LIMITED);
            debug!("per-IP rate limit exceeded");
            Box::pin(async move {
                Ok(rejection(
                    StatusCode::TOO_MANY_REQUESTS,
                    -32000,
                    "rpcrouter: rate limit exceeded for this client",
                ))
            })
        } else {
            let mut inner = self.inner.clone();
            Box::pin(async move { inner.ready().await.unwrap().call(request).await })
        }
    }
}

/// 统一的 JSON-RPC 错误响应体。
pub fn rejection(status: StatusCode, code: i64, message: &str) -> Response {
    let body = json!({
        "jsonrpc": "2.0",
        "id": Value::Null,
        "error": {"code": code, "message": message}
    });
    Response::builder()
        .status(status)
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .expect("valid rejection response")
}

/// /metrics 端点鉴权层：校验 Authorization: Bearer <token>。
#[derive(Clone)]
pub struct BearerAuth {
    expected: HeaderValue,
}

impl BearerAuth {
    pub fn new(token: &str) -> Self {
        let value = format!("Bearer {token}");
        Self {
            expected: HeaderValue::from_str(&value).expect("valid bearer header"),
        }
    }
}

impl<S> Layer<S> for BearerAuth {
    type Service = BearerAuthService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        BearerAuthService {
            inner,
            expected: self.expected.clone(),
        }
    }
}

#[derive(Clone)]
pub struct BearerAuthService<S> {
    inner: S,
    expected: HeaderValue,
}

impl<S> Service<Request<Body>> for BearerAuthService<S>
where
    S: Service<Request<Body>, Response = Response, Error = std::convert::Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = Response;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let authorized = request
            .headers()
            .get(AUTHORIZATION)
            .is_some_and(|value| value == self.expected);
        if authorized {
            let mut inner = self.inner.clone();
            Box::pin(async move { inner.ready().await.unwrap().call(request).await })
        } else {
            Box::pin(async move {
                Ok(Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .body(Body::empty())
                    .expect("valid 401 response"))
            })
        }
    }
}

//! W6b 管理面：只读观测与显式鉴权的运行时控制。
use std::{collections::HashMap, sync::Arc, time::Instant};

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, State},
    http::{
        HeaderMap, Method, StatusCode, Uri,
        header::{AUTHORIZATION, CONTENT_TYPE},
    },
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tower_http::cors::CorsLayer;
use tracing::warn;

use crate::{
    chainlist::{ChainlistLoader, catalog_document},
    config::Config,
    forward::Forwarder,
    metrics::Metrics,
    probe::ProbeManager,
    registry::{EndpointState, Registry},
    state::{
        ChainOverrideState, EndpointOverrideState, Overrides, StateExport, StateRuntimeSnapshot,
        StateStore, endpoint_key,
    },
};

#[derive(Clone)]
pub struct AdminState {
    pub registry: Arc<Registry>,
    pub forwarder: Arc<Forwarder>,
    pub metrics: Arc<Metrics>,
    pub store: Arc<dyn StateStore>,
    pub chainlist: Option<Arc<ChainlistLoader>>,
    pub probe: Option<Arc<ProbeManager>>,
    pub config: Config,
    pub started: Instant,
    pub state_runtime: Arc<tokio::sync::RwLock<StateRuntimeSnapshot>>,
}

#[derive(Debug, Deserialize, Default)]
pub struct ChainQuery {
    pub state: Option<String>,
    pub q: Option<String>,
    pub testnet: Option<bool>,
    pub sort: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
pub struct CacheClear {
    pub chain_id: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SettingsPatch {
    pub block_time_ms: Option<Option<u64>>,
    pub confirmation_depth: Option<Option<u64>>,
    pub tip_ttl_ms: Option<Option<u64>>,
    pub max_block_lag: Option<Option<u64>>,
}

#[derive(Debug, Deserialize, Default)]
pub struct EndpointAction {
    pub url: String,
    pub seconds: Option<u64>,
    pub rps: Option<u32>,
    pub concurrency: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct ConfirmReset {
    pub confirm: bool,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointRow {
    pub url: String,
    pub tracking: Option<String>,
    pub state: String,
    pub strikes: u32,
    pub cooling_until_unix: Option<u64>,
    pub latency_ewma_ms: f64,
    pub lag: u64,
    pub rps: u32,
    pub concurrency: usize,
    pub disabled: bool,
    pub source: String,
    pub last_fault: Option<String>,
    pub stats: crate::registry::EndpointStatsSnapshot,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainRow {
    pub chain_id: u64,
    pub name: String,
    pub short_name: Option<String>,
    pub is_testnet: bool,
    pub status: Option<String>,
    pub state: String,
    pub pinned: bool,
    pub disabled: bool,
    pub catalog_endpoints: usize,
    pub endpoints: usize,
    pub active: usize,
    pub cooling: usize,
    pub probation: usize,
    pub head: u64,
    pub last_ingress_unix: u64,
    pub ingress_total: u64,
    pub cache_hits_total: u64,
    pub cache_lookups_total: u64,
    pub upstream_total: u64,
    pub user_visible_errors_total: u64,
    pub settings: Value,
    #[serde(rename = "endpoints", skip_serializing_if = "Option::is_none")]
    pub endpoint_rows: Option<Vec<EndpointRow>>,
}

fn err(status: StatusCode, code: &str, message: impl Into<String>) -> Response {
    (
        status,
        Json(json!({"error":{"code":code,"message":message.into()}})),
    )
        .into_response()
}

#[allow(clippy::result_large_err)]
fn auth(headers: &HeaderMap, method: &Method, token: Option<&str>) -> Result<(), Response> {
    if let Some(token) = token {
        let expected = format!("Bearer {token}");
        if headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok()) != Some(expected.as_str()) {
            return Err(err(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "Bearer token is required",
            ));
        }
    } else if *method != Method::GET && *method != Method::HEAD {
        return Err(err(
            StatusCode::FORBIDDEN,
            "admin_disabled",
            "write operations require admin.auth_token",
        ));
    }
    Ok(())
}

pub fn router(state: AdminState) -> Router {
    let enabled = state.config.admin.enabled;
    let mut router = Router::new()
        .route("/admin/api/overview", get(overview))
        .route("/admin/api/chains", get(chains))
        .route("/admin/api/chains/{id}", get(chain_detail))
        .route("/admin/api/overrides", get(overrides))
        .route("/admin/api/state", get(state_info))
        .route("/admin/api/state/export", get(state_export))
        .route("/admin/api/chainlist/refresh", post(chainlist_refresh))
        .route("/admin/api/cache/clear", post(cache_clear))
        .route("/admin/api/chains/{id}/settings", put(chain_settings))
        .route(
            "/admin/api/chains/{id}/endpoints/{action}",
            post(endpoint_action),
        )
        .route("/admin/api/chains/{id}/{action}", post(chain_action))
        .route("/admin/api/state/import", post(state_import))
        .route("/admin/api/state/reset", post(state_reset))
        .route("/dashboard", get(static_file))
        .route("/dashboard/", get(static_file))
        .route("/dashboard/{*path}", get(static_file))
        .with_state(state.clone());
    if !state.config.admin.cors_allow_origins.is_empty() {
        let mut layer = CorsLayer::new().allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::OPTIONS,
        ]);
        for origin in &state.config.admin.cors_allow_origins {
            if let Ok(value) = origin.parse::<axum::http::HeaderValue>() {
                layer = layer.allow_origin(value);
            }
        }
        router = router.layer(layer);
    }
    if enabled {
        router
    } else {
        Router::new().with_state(state)
    }
}

async fn overview(State(s): State<AdminState>, headers: HeaderMap) -> Response {
    if let Err(r) = auth(&headers, &Method::GET, s.config.admin.auth_token.as_deref()) {
        return r;
    }
    let counts = s.registry.chain_counts().await;
    let summaries = s.registry.summaries().await;
    let mut active = 0;
    let mut cooling = 0;
    let mut probation = 0;
    for x in &summaries {
        active += x.active;
        cooling += x.cooling;
        probation += x.probation;
    }
    let rs = match s.chainlist.as_ref() {
        Some(loader) => Some(loader.refresh_state().await),
        None => None,
    };
    let rs = rs.unwrap_or_else(|| crate::chainlist::RefreshState {
        source: crate::chainlist::RefreshSource::Fixture,
        last_refresh_unix: 0,
        etag: None,
        catalog_chains: s.registry.catalog_chain_count() as usize,
        catalog_endpoints: s.registry.catalog_endpoint_count() as usize,
        last_error: None,
        refreshing: false,
    });
    let export = s.store.export().await.unwrap_or_default();
    let total = s.registry.catalog().await.map_or(0, |c| c.chains.len());
    let traffic = summaries
        .iter()
        .map(|x| s.metrics.chain_snapshot(x.chain_id))
        .fold(
            crate::metrics::ChainMetricsSnapshot::default(),
            |mut a, b| {
                a.ingress += b.ingress;
                a.cache_hits += b.cache_hits;
                a.cache_lookups += b.cache_lookups;
                a.coalesced += b.coalesced;
                a.upstream += b.upstream;
                a.user_visible_errors += b.user_visible_errors;
                a.hedges += b.hedges;
                a
            },
        );
    Json(json!({"process":{"version":env!("CARGO_PKG_VERSION"),"uptimeSeconds":s.started.elapsed().as_secs()},"chainlist":{"source":rs.source.label(),"lastRefreshUnix":rs.last_refresh_unix,"etag":rs.etag,"catalogChains":rs.catalog_chains,"catalogEndpoints":rs.catalog_endpoints,"refreshSeconds":s.config.chainlist.refresh_seconds,"lastError":rs.last_error,"refreshing":rs.refreshing},"chains":{"catalog":total,"pinned":counts.pinned,"hot":counts.hot,"dormant":counts.dormant,"disabled":counts.disabled},"endpoints":{"materialized":summaries.iter().map(|x|x.endpoints).sum::<usize>(),"active":active,"cooling":cooling,"probation":probation},"traffic":{"ingressTotal":traffic.ingress,"cacheHitsTotal":traffic.cache_hits,"cacheLookupsTotal":traffic.cache_lookups,"coalescedTotal":traffic.coalesced,"upstreamTotal":traffic.upstream,"userVisibleErrorsTotal":traffic.user_visible_errors,"ingressRejectedTotal":s.metrics.ingress_rejected_total(),"hedgesTotal":traffic.hedges,"inFlight":s.metrics.in_flight()},"state":{"backend":s.store.backend_name(),"overrides":export.overrides.chains.len()+export.overrides.endpoints.len()},"probe":{"queueDepth":s.registry.probe_queue_depth.load(std::sync::atomic::Ordering::Relaxed),"inFlight":s.registry.probe_in_flight.load(std::sync::atomic::Ordering::Relaxed),"maxConcurrency":s.config.probe.max_concurrency},"cache":{"entries":s.forwarder.cache().entry_count(),"weightedBytes":s.forwarder.cache().weighted_size(),"maxBytes":s.config.cache.max_bytes},"total":total})).into_response()
}

async fn chains(
    State(s): State<AdminState>,
    headers: HeaderMap,
    Query(query): Query<ChainQuery>,
) -> Response {
    if let Err(r) = auth(&headers, &Method::GET, s.config.admin.auth_token.as_deref()) {
        return r;
    }
    let mut rows = build_rows(&s, None).await;
    if let Some(state) = query.state.as_deref().filter(|x| *x != "all") {
        rows.retain(|r| r.state.eq_ignore_ascii_case(state));
    }
    if let Some(testnet) = query.testnet {
        rows.retain(|r| r.is_testnet == testnet);
    }
    if let Some(q) = query.q.as_deref() {
        let q = q.to_ascii_lowercase();
        rows.retain(|r| {
            r.chain_id.to_string().contains(&q)
                || r.name.to_ascii_lowercase().contains(&q)
                || r.short_name
                    .as_deref()
                    .is_some_and(|x| x.to_ascii_lowercase().contains(&q))
        });
    }
    match query.sort.as_deref() {
        Some("name") => rows.sort_by_key(|r| r.name.clone()),
        Some("traffic") => rows.sort_by_key(|r| std::cmp::Reverse(r.ingress_total)),
        _ => rows.sort_by_key(|r| r.chain_id),
    }
    let total = rows.len();
    let offset = query.offset.unwrap_or(0).min(total);
    let limit = query.limit.unwrap_or(100).min(200);
    let items = rows
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();
    Json(json!({"total":total,"items":items})).into_response()
}

async fn chain_detail(
    State(s): State<AdminState>,
    headers: HeaderMap,
    Path(id): Path<u64>,
) -> Response {
    if let Err(r) = auth(&headers, &Method::GET, s.config.admin.auth_token.as_deref()) {
        return r;
    }
    if !s.registry.chain_in_catalog(id).await {
        return err(StatusCode::NOT_FOUND, "not_found", "unknown chain");
    };
    let mut rows = build_rows(&s, Some(id)).await;
    rows.pop().map_or_else(
        || err(StatusCode::NOT_FOUND, "not_found", "unknown chain"),
        |r| Json(r).into_response(),
    )
}

async fn overrides(State(s): State<AdminState>, headers: HeaderMap) -> Response {
    if let Err(r) = auth(&headers, &Method::GET, s.config.admin.auth_token.as_deref()) {
        return r;
    }
    match s.store.export().await {
        Ok(x) => Json(x.overrides).into_response(),
        Err(e) => err(
            StatusCode::SERVICE_UNAVAILABLE,
            "state_store_unavailable",
            e.to_string(),
        ),
    }
}
async fn state_info(State(s): State<AdminState>, headers: HeaderMap) -> Response {
    if let Err(r) = auth(&headers, &Method::GET, s.config.admin.auth_token.as_deref()) {
        return r;
    }
    let snapshot = s.state_runtime.read().await.clone();
    Json(json!({
        "backend": snapshot.backend,
        "namespace": snapshot.namespace,
        "instanceId": snapshot.instance_id,
        "up": snapshot.up,
        "schemaVersion": snapshot.schema_version,
        "dirtyEndpoints": snapshot.dirty_endpoints,
        "lastFlushUnix": snapshot.last_flush_unix,
        "lastFlushResult": snapshot.last_flush_result,
        "lastFlushDurationMs": snapshot.last_flush_duration_ms,
        "lastPingUnix": snapshot.last_ping_unix,
    }))
    .into_response()
}
async fn state_export(State(s): State<AdminState>, headers: HeaderMap) -> Response {
    if let Err(r) = auth(&headers, &Method::GET, s.config.admin.auth_token.as_deref()) {
        return r;
    }
    match s.store.export().await {
        Ok(x) => Json(x).into_response(),
        Err(e) => err(
            StatusCode::SERVICE_UNAVAILABLE,
            "state_store_unavailable",
            e.to_string(),
        ),
    }
}

async fn chainlist_refresh(State(s): State<AdminState>, headers: HeaderMap) -> Response {
    if let Err(r) = auth(
        &headers,
        &Method::POST,
        s.config.admin.auth_token.as_deref(),
    ) {
        return r;
    }
    let Some(loader) = &s.chainlist else {
        return err(
            StatusCode::SERVICE_UNAVAILABLE,
            "not_found",
            "chainlist loader unavailable",
        );
    };
    match loader.refresh().await {
        Ok(Some(x)) => {
            let chain_count = x.catalog.chains.len();
            let refresh = loader.refresh_state().await;
            if let Err(error) = s
                .store
                .set_catalog_metadata(
                    &catalog_document(x.catalog.as_ref()),
                    refresh.etag.as_deref(),
                    crate::registry::unix_seconds(),
                )
                .await
            {
                return err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "state_store_unavailable",
                    error.to_string(),
                );
            }
            s.registry.set_catalog(x.catalog).await;
            s.registry.apply_snapshot(&x.snapshot).await;
            Json(json!({"source":x.source.label(),"chains":chain_count})).into_response()
        }
        Ok(None) => err(
            StatusCode::CONFLICT,
            "conflict",
            "chainlist refresh is already running",
        ),
        Err(e) => err(
            StatusCode::SERVICE_UNAVAILABLE,
            "state_store_unavailable",
            e.to_string(),
        ),
    }
}
async fn cache_clear(
    State(s): State<AdminState>,
    headers: HeaderMap,
    Json(body): Json<CacheClear>,
) -> Response {
    if let Err(r) = auth(
        &headers,
        &Method::POST,
        s.config.admin.auth_token.as_deref(),
    ) {
        return r;
    }
    if let Some(id) = body.chain_id {
        s.forwarder.cache().clear_chain(id).await
    } else {
        s.forwarder.cache().clear().await
    };
    audit(&s, "cache.clear", "all").await;
    Json(json!({"ok":true})).into_response()
}

async fn chain_action(
    State(s): State<AdminState>,
    headers: HeaderMap,
    Path((id, action)): Path<(u64, String)>,
) -> Response {
    if let Err(r) = auth(
        &headers,
        &Method::POST,
        s.config.admin.auth_token.as_deref(),
    ) {
        return r;
    }
    if !["activate", "demote", "pin", "unpin", "enable", "disable"].contains(&action.as_str()) {
        return err(StatusCode::NOT_FOUND, "not_found", "unknown chain action");
    };
    if !s.registry.chain_in_catalog(id).await {
        return err(StatusCode::NOT_FOUND, "unknown_chain", "unknown chain");
    }
    let mut override_value = existing_chain_override(&s, id).await;
    match action.as_str() {
        "activate" => {
            let mut hot = s.registry.hot_chain_snapshot();
            if !hot.iter().any(|(chain_id, _)| *chain_id == id) {
                hot.push((id, crate::registry::unix_seconds()));
            }
            if let Err(error) = s.store.set_hot_chains(&hot).await {
                return err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "state_store_unavailable",
                    error.to_string(),
                );
            }
            let _ = s.registry.resolve_for_request(id).await;
        }
        "demote" => {
            let hot = s
                .registry
                .hot_chain_snapshot()
                .into_iter()
                .filter(|(chain_id, _)| *chain_id != id)
                .collect::<Vec<_>>();
            if let Err(error) = s.store.set_hot_chains(&hot).await {
                return err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "state_store_unavailable",
                    error.to_string(),
                );
            }
            s.registry.demote(id, "admin").await;
        }
        "pin" => {
            override_value.pinned = Some(true);
        }
        "unpin" => {
            override_value.pinned = Some(false);
        }
        "enable" => {
            override_value.disabled = Some(false);
        }
        "disable" => {
            override_value.disabled = Some(true);
        }
        _ => {}
    }
    if ["pin", "unpin", "enable", "disable"].contains(&action.as_str()) {
        if let Err(r) = persist_chain(&s, id, &override_value).await {
            return r;
        };
        if action == "pin" {
            s.registry.set_pinned(id, true).await;
        }
        if action == "unpin" {
            s.registry.set_pinned(id, false).await;
        }
        if action == "enable" {
            s.registry.set_disabled(id, false).await;
        }
        if action == "disable" {
            s.registry.set_disabled(id, true).await;
        }
    }
    audit(&s, &format!("chain.{action}"), &id.to_string()).await;
    Json(json!({"chainId":id,"state":s.registry.resolve_for_request(id).await.map_or_else(|| "unknown".to_owned(), |value| format!("{:?}", value.state_label()).to_ascii_lowercase())})).into_response()
}

async fn chain_settings(
    State(s): State<AdminState>,
    headers: HeaderMap,
    Path(id): Path<u64>,
    Json(patch): Json<SettingsPatch>,
) -> Response {
    if let Err(r) = auth(&headers, &Method::PUT, s.config.admin.auth_token.as_deref()) {
        return r;
    }
    if !s.registry.chain_in_catalog(id).await {
        return err(StatusCode::NOT_FOUND, "unknown_chain", "unknown chain");
    }
    let mut v = existing_chain_override(&s, id).await;
    if let Some(x) = patch.block_time_ms {
        v.block_time_ms = x
    }
    if let Some(x) = patch.confirmation_depth {
        v.confirmation_depth = x
    }
    if let Some(x) = patch.tip_ttl_ms {
        v.tip_ttl_ms = x
    }
    if let Some(x) = patch.max_block_lag {
        v.max_block_lag = x
    }
    if let Err(r) = persist_chain(&s, id, &v).await {
        return r;
    };
    s.registry.apply_override(id, v.clone()).await;
    s.forwarder
        .apply_chain_settings(id, v.confirmation_depth, v.tip_ttl_ms);
    audit(&s, "chain.settings", &id.to_string()).await;
    Json(v).into_response()
}

async fn endpoint_action(
    State(s): State<AdminState>,
    headers: HeaderMap,
    Path((id, action)): Path<(u64, String)>,
    Json(body): Json<EndpointAction>,
) -> Response {
    if let Err(r) = auth(
        &headers,
        &Method::POST,
        s.config.admin.auth_token.as_deref(),
    ) {
        return r;
    }
    let endpoint = match s.registry.endpoint(id, &body.url).await {
        Some(e) => e,
        None if action == "add" => {
            if !s.registry.add_runtime_endpoint(id, body.url.clone()).await {
                return err(StatusCode::NOT_FOUND, "unknown_chain", "chain unavailable");
            };
            let value = EndpointOverrideState {
                url: body.url.clone(),
                disabled: Some(false),
                rps: body.rps,
                concurrency: body.concurrency,
            };
            if let Err(r) = persist_endpoint(&s, id, &value).await {
                let _ = s.registry.remove_runtime_endpoint(id, &body.url).await;
                return r;
            };
            audit(&s, "endpoint.add", &body.url).await;
            return Json(json!({"url":body.url,"state":"probation"})).into_response();
        }
        None if action == "enable" || action == "limits" => {
            let value = EndpointOverrideState {
                url: body.url.clone(),
                disabled: (action == "enable").then_some(false),
                rps: body.rps,
                concurrency: body.concurrency,
            };
            if let Err(r) = persist_endpoint(&s, id, &value).await {
                return r;
            }
            s.registry.set_endpoint_override(id, value).await;
            let Some(endpoint) = s.registry.endpoint(id, &body.url).await else {
                return err(StatusCode::NOT_FOUND, "not_found", "endpoint not found");
            };
            audit(&s, &format!("endpoint.{action}"), &body.url).await;
            return Json(json!({"url":body.url,"state":format!("{:?}",endpoint.state(Instant::now().into())).to_ascii_lowercase()})).into_response();
        }
        None => return err(StatusCode::NOT_FOUND, "not_found", "endpoint not found"),
    };
    match action.as_str() {
        "disable" => {
            let v = EndpointOverrideState {
                url: body.url.clone(),
                disabled: Some(true),
                rps: None,
                concurrency: None,
            };
            if let Err(r) = persist_endpoint(&s, id, &v).await {
                return r;
            };
            s.registry.set_endpoint_override(id, v).await;
        }
        "enable" => {
            let v = EndpointOverrideState {
                url: body.url.clone(),
                disabled: Some(false),
                rps: None,
                concurrency: None,
            };
            if let Err(r) = persist_endpoint(&s, id, &v).await {
                return r;
            };
            s.registry.set_endpoint_override(id, v).await;
        }
        "cool" => endpoint.cool_for(std::time::Duration::from_secs(body.seconds.unwrap_or(60))),
        "reset" => endpoint.reset_health(),
        "limits" => {
            let v = EndpointOverrideState {
                url: body.url.clone(),
                disabled: None,
                rps: body.rps,
                concurrency: body.concurrency,
            };
            if let Err(r) = persist_endpoint(&s, id, &v).await {
                return r;
            };
            s.registry.set_endpoint_override(id, v).await;
        }
        "remove" => {
            if !s.registry.runtime_endpoint_exists(id, &body.url).await {
                return err(
                    StatusCode::CONFLICT,
                    "conflict",
                    "endpoint is not a runtime endpoint",
                );
            };
            if let Err(e) = s
                .store
                .delete_endpoint_override(&endpoint_key(id, &body.url))
                .await
            {
                return err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "state_store_unavailable",
                    e.to_string(),
                );
            };
            let _ = s.registry.remove_runtime_endpoint(id, &body.url).await;
        }
        "probe" => {
            let Some(probe) = &s.probe else {
                return err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "not_found",
                    "probe manager unavailable",
                );
            };
            let outcome = probe.probe_endpoint(id, endpoint).await;
            audit(&s, "endpoint.probe", &body.url).await;
            let outcome = match outcome {
                crate::probe::ProbeOutcome::Passed => json!({"state":"passed"}),
                crate::probe::ProbeOutcome::Skipped => json!({"state":"skipped"}),
                crate::probe::ProbeOutcome::Failed(kind) => {
                    json!({"state":"failed","fault":format!("{kind:?}").to_ascii_lowercase()})
                }
                crate::probe::ProbeOutcome::RemovedWrongChain { actual } => {
                    json!({"state":"removedWrongChain","actualChainId":actual})
                }
            };
            return Json(json!({"outcome":outcome})).into_response();
        }
        _ => {
            return err(
                StatusCode::NOT_FOUND,
                "not_found",
                "unknown endpoint action",
            );
        }
    };
    audit(&s, &format!("endpoint.{action}"), &body.url).await;
    Json(json!({"url":body.url,"state":format!("{:?}",endpoint.state(Instant::now().into())).to_ascii_lowercase()})).into_response()
}

async fn state_import(
    State(s): State<AdminState>,
    headers: HeaderMap,
    Json(value): Json<StateExport>,
) -> Response {
    if let Err(r) = auth(
        &headers,
        &Method::POST,
        s.config.admin.auth_token.as_deref(),
    ) {
        return r;
    }
    if let Err(e) = s.store.import(&value).await {
        return err(
            StatusCode::SERVICE_UNAVAILABLE,
            "state_store_unavailable",
            e.to_string(),
        );
    };
    s.registry.apply_overrides(&value.overrides).await;
    s.forwarder.apply_state_overrides(&value.overrides);
    audit(&s, "state.import", "namespace").await;
    Json(json!({"ok":true})).into_response()
}
async fn state_reset(
    State(s): State<AdminState>,
    headers: HeaderMap,
    Json(value): Json<ConfirmReset>,
) -> Response {
    if let Err(r) = auth(
        &headers,
        &Method::POST,
        s.config.admin.auth_token.as_deref(),
    ) {
        return r;
    }
    if !value.confirm {
        return err(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "confirm must be true",
        );
    };
    if let Err(e) = s.store.reset().await {
        return err(
            StatusCode::SERVICE_UNAVAILABLE,
            "state_store_unavailable",
            e.to_string(),
        );
    };
    s.registry.apply_overrides(&Overrides::default()).await;
    s.forwarder.apply_state_overrides(&Overrides::default());
    s.forwarder.cache().clear().await;
    audit(&s, "state.reset", "namespace").await;
    Json(json!({"ok":true})).into_response()
}

async fn static_file(State(s): State<AdminState>, uri: Uri) -> Response {
    let Some(dir) = &s.config.admin.static_dir else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let path = uri.path().trim_start_matches("/dashboard/");
    let path = if path.is_empty() { "index.html" } else { path };
    if path.split('/').any(|part| part == "..") {
        return StatusCode::NOT_FOUND.into_response();
    }
    let full = dir.join(path);
    let full = if tokio::fs::metadata(&full).await.is_ok() {
        full
    } else {
        dir.join("index.html")
    };
    match tokio::fs::read(&full).await {
        Ok(bytes) => (
            StatusCode::OK,
            [(CONTENT_TYPE, content_type(&full))],
            Body::from(bytes),
        )
            .into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}
fn content_type(path: &std::path::Path) -> &'static str {
    match path.extension().and_then(|x| x.to_str()) {
        Some("js") => "text/javascript",
        Some("css") => "text/css",
        Some("json") => "application/json",
        Some("svg") => "image/svg+xml",
        _ => "text/html; charset=utf-8",
    }
}

async fn audit(s: &AdminState, what: &str, target: &str) {
    if let Err(e) = s.store.append_audit(what, target).await {
        warn!(error=%e,what,target,"admin audit append failed")
    }
}
async fn existing_chain_override(s: &AdminState, id: u64) -> ChainOverrideState {
    s.store
        .export()
        .await
        .ok()
        .and_then(|x| x.overrides.chains.get(&id).cloned())
        .unwrap_or_default()
}
async fn persist_chain(
    s: &AdminState,
    id: u64,
    value: &ChainOverrideState,
) -> Result<(), Response> {
    s.store.put_chain_override(id, value).await.map_err(|e| {
        err(
            StatusCode::SERVICE_UNAVAILABLE,
            "state_store_unavailable",
            e.to_string(),
        )
    })?;
    Ok(())
}
async fn persist_endpoint(
    s: &AdminState,
    id: u64,
    value: &EndpointOverrideState,
) -> Result<(), Response> {
    s.store
        .put_endpoint_override(&endpoint_key(id, &value.url), value)
        .await
        .map_err(|e| {
            err(
                StatusCode::SERVICE_UNAVAILABLE,
                "state_store_unavailable",
                e.to_string(),
            )
        })?;
    Ok(())
}

async fn build_rows(s: &AdminState, only: Option<u64>) -> Vec<ChainRow> {
    let catalog = s.registry.catalog().await;
    let summaries: HashMap<u64, _> = s
        .registry
        .summaries()
        .await
        .into_iter()
        .map(|x| (x.chain_id, x))
        .collect();
    let mut ids = Vec::new();
    if let Some(c) = &catalog {
        ids.extend(c.chains.iter().map(|x| x.chain_id));
    }
    if let Some(id) = only {
        ids.retain(|x| *x == id);
    }
    ids.sort_unstable();
    ids.dedup();
    let mut rows = Vec::new();
    for id in ids {
        let c = catalog.as_ref().and_then(|x| x.lookup(id));
        let summary = summaries.get(&id);
        let state = summary.map_or("dormant".to_owned(), |x| {
            format!("{:?}", x.state).to_ascii_lowercase()
        });
        let metrics = s.metrics.chain_snapshot(id);
        let settings = s.registry.chain_settings(id);
        let endpoint_rows = if only.is_some() {
            Some(build_endpoint_rows(s, id, c).await)
        } else {
            None
        };
        rows.push(ChainRow{chain_id:id,name:c.map_or_else(||format!("Chain {id}"),|x|x.name.clone()),short_name:c.and_then(|x|x.short_name.clone()),is_testnet:c.is_some_and(|x|x.is_testnet),status:c.and_then(|x|x.status.clone()),state:state.clone(),pinned:state=="pinned",disabled:state=="disabled",catalog_endpoints:c.map_or(0,|x|x.endpoints.len()),endpoints:summary.map_or(0,|x|x.endpoints),active:summary.map_or(0,|x|x.active),cooling:summary.map_or(0,|x|x.cooling),probation:summary.map_or(0,|x|x.probation),head:summary.map_or(0,|x|x.head),last_ingress_unix:s.registry.chain_last_ingress(id),ingress_total:metrics.ingress,cache_hits_total:metrics.cache_hits,cache_lookups_total:metrics.cache_lookups,upstream_total:metrics.upstream,user_visible_errors_total:metrics.user_visible_errors,settings:json!({"blockTimeMs":settings.0,"confirmationDepth":settings.1,"tipTtlMs":settings.2,"maxBlockLag":settings.3,"source":settings.4}),endpoint_rows});
    }
    rows
}
async fn build_endpoint_rows(
    s: &AdminState,
    id: u64,
    c: Option<&crate::chainlist::CatalogChain>,
) -> Vec<EndpointRow> {
    let mut out = Vec::new();
    for e in s.registry.all_endpoints(id).await {
        let state = e.state(Instant::now().into());
        let (name, strikes, cooling) = match state {
            EndpointState::Active => ("active".to_owned(), 0, None),
            EndpointState::Probation { passes } => ("probation".to_owned(), passes as u32, None),
            EndpointState::Cooling { until, strikes } => (
                "cooling".to_owned(),
                strikes,
                Some(
                    crate::registry::unix_seconds()
                        + until
                            .saturating_duration_since(Instant::now().into())
                            .as_secs(),
                ),
            ),
        };
        let tracking = c.and_then(|x| {
            x.endpoints
                .iter()
                .find(|x| x.url == e.url())
                .and_then(|x| x.tracking.clone())
        });
        let h = e.health_snapshot(id);
        out.push(EndpointRow {
            url: e.url().to_owned(),
            tracking,
            state: name,
            strikes,
            cooling_until_unix: cooling,
            latency_ewma_ms: e.latency_ewma_micros() as f64 / 1000.0,
            lag: e.lag(),
            rps: e.rps(),
            concurrency: e.concurrency(),
            disabled: false,
            source: if c.is_some() {
                "chainlist".into()
            } else {
                "runtime".into()
            },
            last_fault: None,
            stats: e.stats(),
        });
        let _ = h;
    }
    out
}

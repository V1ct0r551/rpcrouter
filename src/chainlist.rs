use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use reqwest::{
    Client, StatusCode, Url,
    header::{ETAG, HeaderValue, IF_NONE_MATCH},
};
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::warn;

use crate::config::Config;

const BUILTIN_FIXTURE: &[u8] = include_bytes!("../fixtures/rpcs.sample.json");

// ── 旧兼容类型（v1，仍被测试与 registry 引用） ──

#[derive(Clone, Debug)]
pub struct ChainEndpoints {
    pub chain_id: u64,
    pub name: String,
    pub endpoints: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct ChainlistSnapshot {
    pub chains: Vec<ChainEndpoints>,
}

// ── 新目录类型（v2） ──

/// 一条链的完整目录元数据。
#[derive(Clone, Debug)]
pub struct CatalogChain {
    pub chain_id: u64,
    pub name: String,
    pub short_name: Option<String>,
    pub chain: Option<String>,
    pub slug: Option<String>,
    pub is_testnet: bool,
    pub native_symbol: Option<String>,
    pub explorer_url: Option<String>,
    /// chainlist 的 status 字段（"active"/"incubating"/"deprecated"…）。
    pub status: Option<String>,
    pub tvl: Option<f64>,
    /// 过滤后的公开 https 端点（含 tracking）。
    pub endpoints: Vec<CatalogEndpoint>,
}

/// 目录里的单个端点（含 tracking 元数据）。
#[derive(Clone, Debug)]
pub struct CatalogEndpoint {
    pub url: String,
    pub tracking: Option<String>,
}

/// 完整的目录快照：所有链的元数据 + 端点列表。
#[derive(Clone, Debug)]
pub struct Catalog {
    pub chains: Vec<CatalogChain>,
    /// 按 chain_id 查找链在 `chains` 中的索引。
    pub by_id: HashSet<u64>,
}

impl Catalog {
    pub fn lookup(&self, chain_id: u64) -> Option<&CatalogChain> {
        // 先用 HashSet 快速判断是否存在，再线性扫描找到索引。
        // 对 ~2877 条链的规模，线性扫描是可接受的；
        // 但为了热路径更廉价，by_id 用于快速判断 unknown。
        if !self.by_id.contains(&chain_id) {
            return None;
        }
        self.chains.iter().find(|c| c.chain_id == chain_id)
    }

    pub fn chain_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.chains.iter().map(|c| c.chain_id)
    }
}

/// 目录刷新来源。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RefreshSource {
    Network,
    NotModified,
    Memory,
    Disk,
    Fixture,
}

/// 兼容旧代码的类型别名。
pub type SnapshotSource = RefreshSource;

/// 最新的刷新状态（可对外查询）。
#[derive(Clone, Debug)]
pub struct RefreshState {
    pub source: RefreshSource,
    pub last_refresh_unix: u64,
    pub etag: Option<String>,
    pub catalog_chains: usize,
    pub catalog_endpoints: usize,
    pub last_error: Option<String>,
    pub refreshing: bool,
}

/// 一次加载结果。
#[derive(Clone, Debug)]
pub struct LoadResult {
    pub catalog: Arc<Catalog>,
    pub source: RefreshSource,
    /// 旧兼容字段。
    pub snapshot: Arc<ChainlistSnapshot>,
}

#[derive(Default)]
struct LoaderState {
    etag: Option<HeaderValue>,
    last_success: Option<Arc<Catalog>>,
    last_snapshot: Option<Arc<ChainlistSnapshot>>,
    /// 刷新锁：manual refresh 与周期刷新互斥。
    refreshing: bool,
}

pub struct ChainlistLoader {
    client: Client,
    url: String,
    cache_path: PathBuf,
    discovery_enabled: bool,
    pinned_chains: HashSet<u64>,
    state: Mutex<LoaderState>,
}

impl ChainlistLoader {
    pub fn new(config: &Config) -> Result<Self> {
        Self::with_client(
            Client::builder()
                .user_agent(concat!("rpcrouter/", env!("CARGO_PKG_VERSION")))
                .timeout(Duration::from_secs(15))
                .build()
                .context("failed to build chainlist HTTP client")?,
            config.chainlist.url.clone(),
            config.chainlist.cache_path.clone(),
            config.discovery.enabled,
            config.chains.iter().copied(),
        )
    }

    pub fn with_client(
        client: Client,
        url: String,
        cache_path: PathBuf,
        discovery_enabled: bool,
        pinned_chains: impl IntoIterator<Item = u64>,
    ) -> Result<Self> {
        Url::parse(&url).with_context(|| format!("invalid chainlist URL {url}"))?;
        Ok(Self {
            client,
            url,
            cache_path,
            discovery_enabled,
            pinned_chains: pinned_chains.into_iter().collect(),
            state: Mutex::new(LoaderState::default()),
        })
    }

    /// 兼容旧代码：用 allowed_chains 过滤，返回旧格式。
    pub fn with_client_legacy(
        client: Client,
        url: String,
        cache_path: PathBuf,
        allowed_chains: impl IntoIterator<Item = u64>,
    ) -> Result<Self> {
        let allowed: HashSet<_> = allowed_chains.into_iter().collect();
        Self::with_client(client, url, cache_path, false, allowed)
    }

    /// 优先联网刷新；失败后依次回退到进程内快照、磁盘缓存和内置样例。
    pub async fn load(&self) -> Result<LoadResult> {
        match self.fetch_network().await {
            Ok(result) => return Ok(result),
            Err(error) => warn!(error = %error, "chainlist network refresh failed"),
        }

        {
            let state = self.state.lock().await;
            if let (Some(catalog), Some(snapshot)) =
                (state.last_success.clone(), state.last_snapshot.clone())
            {
                return Ok(LoadResult {
                    catalog,
                    snapshot,
                    source: RefreshSource::Memory,
                });
            }
        }

        match tokio::fs::read(&self.cache_path).await {
            Ok(bytes) => match parse_catalog(&bytes, self.discovery_enabled, &self.pinned_chains) {
                Ok((catalog, snapshot)) => {
                    return Ok(self.remember(catalog, snapshot, RefreshSource::Disk).await);
                }
                Err(error) => warn!(
                    path = %self.cache_path.display(),
                    error = %error,
                    "chainlist disk cache is invalid"
                ),
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => warn!(
                path = %self.cache_path.display(),
                error = %error,
                "chainlist disk cache could not be read"
            ),
        }

        let (catalog, snapshot) =
            parse_catalog(BUILTIN_FIXTURE, self.discovery_enabled, &self.pinned_chains)
                .context("built-in chainlist fixture is invalid")?;
        Ok(self
            .remember(catalog, snapshot, RefreshSource::Fixture)
            .await)
    }

    /// 手动刷新入口（供 W6 调用），与周期刷新互斥。
    /// 返回 Ok(LoadResult) 成功；刷新进行中返回 None。
    pub async fn refresh(&self) -> Result<Option<LoadResult>> {
        let mut state = self.state.lock().await;
        if state.refreshing {
            return Ok(None);
        }
        state.refreshing = true;
        drop(state);

        let result = self.load().await;

        self.state.lock().await.refreshing = false;
        result.map(Some)
    }

    /// 查询最近一次刷新状态（供 metrics 与 Admin API）。
    pub async fn refresh_state(&self) -> RefreshState {
        let state = self.state.lock().await;
        let (catalog_chains, catalog_endpoints) = state
            .last_success
            .as_ref()
            .map(|c| {
                let endpoints: usize = c.chains.iter().map(|ch| ch.endpoints.len()).sum();
                (c.chains.len(), endpoints)
            })
            .unwrap_or((0, 0));
        RefreshState {
            source: RefreshSource::Network, // 由调用方覆盖
            last_refresh_unix: 0,           // 由调用方覆盖
            etag: None,
            catalog_chains,
            catalog_endpoints,
            last_error: None,
            refreshing: state.refreshing,
        }
    }

    async fn fetch_network(&self) -> Result<LoadResult> {
        let etag = self.state.lock().await.etag.clone();
        let mut request = self.client.get(&self.url);
        if let Some(etag) = etag {
            request = request.header(IF_NONE_MATCH, etag);
        }

        let response = request.send().await.context("chainlist request failed")?;
        if response.status() == StatusCode::NOT_MODIFIED {
            let state = self.state.lock().await;
            let catalog = state
                .last_success
                .clone()
                .context("chainlist returned 304 without an in-memory catalog")?;
            let snapshot = state
                .last_snapshot
                .clone()
                .context("chainlist returned 304 without an in-memory snapshot")?;
            return Ok(LoadResult {
                catalog,
                snapshot,
                source: RefreshSource::NotModified,
            });
        }
        if !response.status().is_success() {
            bail!("chainlist returned HTTP {}", response.status());
        }

        let response_etag = response.headers().get(ETAG).cloned();
        let bytes = response
            .bytes()
            .await
            .context("failed to read chainlist response")?;
        let (catalog, snapshot) =
            parse_catalog(&bytes, self.discovery_enabled, &self.pinned_chains)
                .context("chainlist response is invalid")?;
        if let Err(error) = persist_cache(&self.cache_path, &bytes).await {
            warn!(
                path = %self.cache_path.display(),
                error = %error,
                "chainlist disk cache could not be updated"
            );
        }

        let catalog = Arc::new(catalog);
        let snapshot = Arc::new(snapshot);
        let mut state = self.state.lock().await;
        state.etag = response_etag;
        state.last_success = Some(Arc::clone(&catalog));
        state.last_snapshot = Some(Arc::clone(&snapshot));
        Ok(LoadResult {
            catalog,
            snapshot,
            source: RefreshSource::Network,
        })
    }

    async fn remember(
        &self,
        catalog: Catalog,
        snapshot: ChainlistSnapshot,
        source: RefreshSource,
    ) -> LoadResult {
        let catalog = Arc::new(catalog);
        let snapshot = Arc::new(snapshot);
        let mut state = self.state.lock().await;
        state.last_success = Some(Arc::clone(&catalog));
        state.last_snapshot = Some(Arc::clone(&snapshot));
        LoadResult {
            catalog,
            snapshot,
            source,
        }
    }
}

async fn persist_cache(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let temporary_path = path.with_extension("json.tmp");
    tokio::fs::write(&temporary_path, bytes)
        .await
        .with_context(|| format!("failed to write {}", temporary_path.display()))?;
    tokio::fs::rename(&temporary_path, path)
        .await
        .with_context(|| format!("failed to replace {}", path.display()))?;
    Ok(())
}

/// 解析全部链为 Catalog（+ 兼容的 ChainlistSnapshot）。
/// 过滤规则：https-only、剔 `${KEY}` 模板、剔带 userinfo、去重、去 fragment。
/// discovery_enabled=false 时只保留 pinned 链。
pub fn parse_catalog(
    bytes: &[u8],
    discovery_enabled: bool,
    pinned_chains: &HashSet<u64>,
) -> Result<(Catalog, ChainlistSnapshot)> {
    let document: ChainlistDocument =
        serde_json::from_slice(bytes).context("invalid chainlist JSON")?;
    let records = match document {
        ChainlistDocument::List(records) => records,
        ChainlistDocument::Wrapped { chains } => chains,
    };

    let all_chains: Vec<CatalogChain> = records
        .into_iter()
        .filter(|record| discovery_enabled || pinned_chains.contains(&record.chain_id))
        .map(|record| {
            let mut seen = HashSet::new();
            let endpoints: Vec<CatalogEndpoint> = record
                .rpc
                .into_iter()
                .filter_map(|entry| {
                    let tracking = entry.tracking();
                    let url = entry.into_url()?;
                    let normalized = normalize_public_https_url(&url)?;
                    if !seen.insert(normalized.clone()) {
                        return None;
                    }
                    Some(CatalogEndpoint {
                        url: normalized,
                        tracking,
                    })
                })
                .collect();
            CatalogChain {
                chain_id: record.chain_id,
                name: record.name,
                short_name: record.short_name,
                chain: record.chain,
                slug: record.slug,
                is_testnet: record.is_testnet.unwrap_or(false),
                native_symbol: record.native_currency.and_then(|c| c.symbol),
                explorer_url: record
                    .explorers
                    .and_then(|e| e.first().map(|x| x.url.clone())),
                status: record.status,
                tvl: record.tvl,
                endpoints,
            }
        })
        .collect();

    let by_id: HashSet<u64> = all_chains.iter().map(|c| c.chain_id).collect();

    // 构造兼容的旧格式快照
    let snapshot_chains = all_chains
        .iter()
        .map(|c| ChainEndpoints {
            chain_id: c.chain_id,
            name: c.name.clone(),
            endpoints: c.endpoints.iter().map(|e| e.url.clone()).collect(),
        })
        .collect();

    let catalog = Catalog {
        chains: all_chains,
        by_id,
    };
    let snapshot = ChainlistSnapshot {
        chains: snapshot_chains,
    };
    Ok((catalog, snapshot))
}

/// 旧版 parse_and_filter（兼容现有测试）。始终解析全部链，按 allowed_chains 过滤。
pub fn parse_and_filter(bytes: &[u8], allowed_chains: &HashSet<u64>) -> Result<ChainlistSnapshot> {
    let (_, snapshot) = parse_catalog(bytes, false, allowed_chains)?;
    Ok(snapshot)
}

fn normalize_public_https_url(raw: &str) -> Option<String> {
    if raw.contains("${") {
        return None;
    }
    let mut url = Url::parse(raw).ok()?;
    if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
        return None;
    }
    url.set_fragment(None);
    Some(url.to_string())
}

// ── JSON 反序列化结构 ──

#[derive(Deserialize)]
#[serde(untagged)]
enum ChainlistDocument {
    List(Vec<ChainRecord>),
    Wrapped { chains: Vec<ChainRecord> },
}

#[derive(Deserialize)]
struct ChainRecord {
    name: String,
    #[serde(rename = "chainId")]
    chain_id: u64,
    #[serde(default)]
    #[serde(rename = "shortName")]
    short_name: Option<String>,
    #[serde(default)]
    chain: Option<String>,
    #[serde(default)]
    slug: Option<String>,
    #[serde(default)]
    is_testnet: Option<bool>,
    #[serde(default)]
    #[serde(rename = "nativeCurrency")]
    native_currency: Option<NativeCurrency>,
    #[serde(default)]
    explorers: Option<Vec<Explorer>>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    tvl: Option<f64>,
    #[serde(default)]
    rpc: Vec<RpcEntry>,
}

#[derive(Deserialize)]
struct NativeCurrency {
    #[serde(default)]
    symbol: Option<String>,
}

#[derive(Deserialize)]
struct Explorer {
    url: String,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RpcEntry {
    Object {
        url: String,
        #[serde(default)]
        tracking: Option<String>,
    },
    String(String),
}

impl RpcEntry {
    fn into_url(self) -> Option<String> {
        match self {
            Self::Object { url, .. } | Self::String(url) => Some(url),
        }
    }

    fn tracking(&self) -> Option<String> {
        match self {
            Self::Object { tracking, .. } => tracking.clone(),
            Self::String(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{SystemTime, UNIX_EPOCH},
    };

    use axum::{
        Router,
        extract::State,
        http::{HeaderMap, StatusCode, header::ETAG},
        response::IntoResponse,
        routing::get,
    };

    use super::*;

    fn allowed() -> HashSet<u64> {
        HashSet::from([1, 143])
    }

    fn temporary_cache_path(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "rpcrouter-{label}-{}-{nonce}.json",
            std::process::id()
        ))
    }

    async fn spawn_server(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind local server");
        let address = listener.local_addr().expect("local address");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve local app");
        });
        format!("http://{address}/rpcs.json")
    }

    #[test]
    fn fixture_is_filtered_and_deduplicated() {
        let snapshot = parse_and_filter(BUILTIN_FIXTURE, &allowed()).expect("parse fixture");
        assert_eq!(snapshot.chains.len(), 2);

        let ethereum = snapshot
            .chains
            .iter()
            .find(|chain| chain.chain_id == 1)
            .expect("Ethereum fixture");
        assert_eq!(ethereum.endpoints.len(), 3);
        assert!(
            ethereum
                .endpoints
                .iter()
                .all(|url| url.starts_with("https://") && !url.contains("${"))
        );

        let monad = snapshot
            .chains
            .iter()
            .find(|chain| chain.chain_id == 143)
            .expect("Monad fixture");
        assert_eq!(monad.endpoints.len(), 6);
    }

    #[test]
    fn fixture_covers_every_repository_chain() {
        let config = Config::from_toml(include_str!("../config.toml")).expect("repository config");
        let allowed: HashSet<_> = config.chains.iter().copied().collect();
        let snapshot = parse_and_filter(BUILTIN_FIXTURE, &allowed).expect("parse fixture");
        let fixture_chains: HashSet<_> =
            snapshot.chains.iter().map(|chain| chain.chain_id).collect();

        assert_eq!(fixture_chains, allowed);
        assert!(snapshot.chains.iter().all(|chain| {
            !chain.endpoints.is_empty()
                && chain
                    .endpoints
                    .iter()
                    .all(|endpoint| endpoint.starts_with("https://"))
        }));
    }

    #[test]
    fn catalog_parses_all_chains_with_metadata() {
        let (catalog, snapshot) =
            parse_catalog(BUILTIN_FIXTURE, true, &HashSet::new()).expect("parse catalog");
        // 有至少 2 条链
        assert!(catalog.chains.len() >= 2);
        // 有 chain 字段
        assert!(
            catalog
                .chains
                .iter()
                .any(|c| c.chain.as_deref() == Some("ETH"))
        );
        // 有端点
        let eth = catalog.lookup(1).expect("ETH in catalog");
        assert!(!eth.endpoints.is_empty());
        // 兼容快照一致
        assert_eq!(snapshot.chains.len(), catalog.chains.len());
    }

    #[test]
    fn catalog_parse_when_discovery_disabled_filters_to_pinned() {
        let pinned: HashSet<u64> = HashSet::from([1]);
        let (catalog, _) = parse_catalog(BUILTIN_FIXTURE, false, &pinned).expect("parse catalog");
        assert_eq!(catalog.chains.len(), 1);
        assert_eq!(catalog.chains[0].chain_id, 1);
    }

    #[test]
    fn catalog_unknown_chain_returns_none() {
        let (catalog, _) =
            parse_catalog(BUILTIN_FIXTURE, true, &HashSet::new()).expect("parse catalog");
        assert!(catalog.lookup(999999).is_none());
    }

    #[tokio::test]
    async fn sends_etag_and_uses_memory_for_network_failure() {
        #[derive(Clone)]
        struct MockState(Arc<AtomicUsize>);

        async fn handler(State(state): State<MockState>, headers: HeaderMap) -> impl IntoResponse {
            let request = state.0.fetch_add(1, Ordering::SeqCst);
            match request {
                0 => (StatusCode::OK, [(ETAG, "\"fixture-v1\"")], BUILTIN_FIXTURE).into_response(),
                1 if headers
                    .get(IF_NONE_MATCH)
                    .is_some_and(|value| value == "\"fixture-v1\"") =>
                {
                    StatusCode::NOT_MODIFIED.into_response()
                }
                _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            }
        }

        let calls = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/rpcs.json", get(handler))
            .with_state(MockState(Arc::clone(&calls)));
        let url = spawn_server(app).await;
        let cache_path = temporary_cache_path("etag");
        let loader =
            ChainlistLoader::with_client_legacy(Client::new(), url, cache_path.clone(), [1, 143])
                .expect("loader");

        assert_eq!(
            loader.load().await.expect("network load").source,
            RefreshSource::Network
        );
        assert_eq!(
            loader.load().await.expect("not modified").source,
            RefreshSource::NotModified
        );
        assert_eq!(
            loader.load().await.expect("memory fallback").source,
            RefreshSource::Memory
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        let _ = tokio::fs::remove_file(cache_path).await;
    }

    #[tokio::test]
    async fn falls_back_to_disk_then_builtin_fixture() {
        let app = Router::new().route(
            "/rpcs.json",
            get(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        );
        let url = spawn_server(app).await;
        let disk_path = temporary_cache_path("disk");
        tokio::fs::write(&disk_path, BUILTIN_FIXTURE)
            .await
            .expect("write disk cache");
        let disk_loader = ChainlistLoader::with_client_legacy(
            Client::new(),
            url.clone(),
            disk_path.clone(),
            [1, 143],
        )
        .expect("disk loader");
        assert_eq!(
            disk_loader.load().await.expect("disk fallback").source,
            RefreshSource::Disk
        );

        let missing_path = temporary_cache_path("missing");
        let fixture_loader =
            ChainlistLoader::with_client_legacy(Client::new(), url, missing_path, [1, 143])
                .expect("fixture loader");
        assert_eq!(
            fixture_loader
                .load()
                .await
                .expect("fixture fallback")
                .source,
            RefreshSource::Fixture
        );
        let _ = tokio::fs::remove_file(disk_path).await;
    }
}

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
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

impl RefreshSource {
    /// Prometheus `source` 标签值（小写 snake_case，与 DESIGN-v2 §8 一致）。
    pub fn label(&self) -> &'static str {
        match self {
            Self::Network => "network",
            Self::NotModified => "not_modified",
            Self::Memory => "memory",
            Self::Disk => "disk",
            Self::Fixture => "fixture",
        }
    }
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
    /// 最近一次成功刷新的 Unix 秒时间戳。
    last_refresh_unix: u64,
    /// 最近一次成功刷新的来源。
    last_source: Option<RefreshSource>,
    /// 最近一次错误消息。
    last_error: Option<String>,
}

pub struct ChainlistLoader {
    client: Client,
    url: String,
    cache_path: PathBuf,
    discovery_enabled: bool,
    include_testnets: bool,
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
            config.discovery.include_testnets,
            config.chains.iter().copied(),
        )
    }

    pub fn with_client(
        client: Client,
        url: String,
        cache_path: PathBuf,
        discovery_enabled: bool,
        include_testnets: bool,
        pinned_chains: impl IntoIterator<Item = u64>,
    ) -> Result<Self> {
        Url::parse(&url).with_context(|| format!("invalid chainlist URL {url}"))?;
        Ok(Self {
            client,
            url,
            cache_path,
            discovery_enabled,
            include_testnets,
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
        Self::with_client(client, url, cache_path, false, true, allowed)
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
            Ok(bytes) => match parse_catalog(
                &bytes,
                self.discovery_enabled,
                self.include_testnets,
                &self.pinned_chains,
            ) {
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

        let (catalog, snapshot) = parse_catalog(
            BUILTIN_FIXTURE,
            self.discovery_enabled,
            self.include_testnets,
            &self.pinned_chains,
        )
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
            source: state.last_source.unwrap_or(RefreshSource::Fixture),
            last_refresh_unix: state.last_refresh_unix,
            etag: state
                .etag
                .as_ref()
                .and_then(|v| v.to_str().ok().map(String::from)),
            catalog_chains,
            catalog_endpoints,
            last_error: state.last_error.clone(),
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
            let mut state = self.state.lock().await;
            state.last_refresh_unix = unix_ts();
            state.last_source = Some(RefreshSource::NotModified);
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
        let (catalog, snapshot) = parse_catalog(
            &bytes,
            self.discovery_enabled,
            self.include_testnets,
            &self.pinned_chains,
        )
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
        state.last_refresh_unix = unix_ts();
        state.last_source = Some(RefreshSource::Network);
        state.last_error = None;
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
        state.last_refresh_unix = unix_ts();
        state.last_source = Some(source);
        LoadResult {
            catalog,
            snapshot,
            source,
        }
    }
}

fn unix_ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
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
/// discovery_enabled=false 时只保留 pinned 链；include_testnets=false 时过滤 testnet（pinned 除外）。
pub fn parse_catalog(
    bytes: &[u8],
    discovery_enabled: bool,
    include_testnets: bool,
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
        .filter(|record| {
            let pinned = pinned_chains.contains(&record.chain_id);
            let is_testnet = record.is_testnet.unwrap_or(false);
            // pinned 始终保留；discovery 开启时按 include_testnets 决定是否纳入 testnet。
            pinned || (discovery_enabled && (include_testnets || !is_testnet))
        })
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
    let (_, snapshot) = parse_catalog(bytes, false, true, allowed_chains)?;
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
    #[serde(rename = "isTestnet")]
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
        // 至少 1 个有效 https 端点，无 wss 或 ${KEY} 模板。
        assert!(!ethereum.endpoints.is_empty());
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
        assert!(!monad.endpoints.is_empty());
        assert!(
            monad
                .endpoints
                .iter()
                .all(|url| url.starts_with("https://") && !url.contains("${"))
        );
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
            parse_catalog(BUILTIN_FIXTURE, true, true, &HashSet::new()).expect("parse catalog");
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
        let (catalog, _) =
            parse_catalog(BUILTIN_FIXTURE, false, true, &pinned).expect("parse catalog");
        assert_eq!(catalog.chains.len(), 1);
        assert_eq!(catalog.chains[0].chain_id, 1);
    }

    #[test]
    fn catalog_excludes_testnets_when_include_testnets_false() {
        let (catalog, _) =
            parse_catalog(BUILTIN_FIXTURE, true, false, &HashSet::new()).expect("parse catalog");
        // include_testnets=false 时 testnet 链（Hoodi/Sepolia）不进目录。
        assert!(!catalog.chains.iter().any(|c| c.is_testnet));
        // 主网链仍在。
        assert!(catalog.lookup(1).is_some());
    }

    #[test]
    fn catalog_keeps_pinned_testnet_when_include_testnets_false() {
        let pinned: HashSet<u64> = HashSet::from([560048]);
        let (catalog, _) =
            parse_catalog(BUILTIN_FIXTURE, true, false, &pinned).expect("parse catalog");
        // pinned 的 testnet 链仍保留。
        assert!(catalog.lookup(560048).is_some());
        // 其他 testnet 链被过滤。
        assert!(!catalog.lookup(11155111).is_some());
    }

    #[test]
    fn catalog_unknown_chain_returns_none() {
        let (catalog, _) =
            parse_catalog(BUILTIN_FIXTURE, true, true, &HashSet::new()).expect("parse catalog");
        assert!(catalog.lookup(999999).is_none());
    }

    #[test]
    fn fixture_has_testnet_chains() {
        let (catalog, _) =
            parse_catalog(BUILTIN_FIXTURE, true, true, &HashSet::new()).expect("parse catalog");
        let testnets: Vec<_> = catalog.chains.iter().filter(|c| c.is_testnet).collect();
        assert!(
            testnets.len() >= 2,
            "fixture must contain at least 2 testnet chains"
        );
        // 验证 testnet chain_id 均正确。
        let testnet_ids: HashSet<u64> = testnets.iter().map(|c| c.chain_id).collect();
        assert!(testnet_ids.contains(&560048)); // Hoodi
        assert!(testnet_ids.contains(&11155111)); // Sepolia
    }

    #[test]
    fn fixture_has_zero_endpoint_chain() {
        let (catalog, _) =
            parse_catalog(BUILTIN_FIXTURE, true, true, &HashSet::new()).expect("parse catalog");
        let zero_ep: Vec<_> = catalog
            .chains
            .iter()
            .filter(|c| c.endpoints.is_empty())
            .collect();
        assert!(
            !zero_ep.is_empty(),
            "fixture must contain at least one chain with 0 endpoints"
        );
        // Factory 127 在真实 chainlist 中无端点。
        assert!(zero_ep.iter().any(|c| c.chain_id == 127));
    }

    #[test]
    fn fixture_has_status_fields() {
        let (catalog, _) =
            parse_catalog(BUILTIN_FIXTURE, true, true, &HashSet::new()).expect("parse catalog");
        let with_status: Vec<_> = catalog
            .chains
            .iter()
            .filter(|c| c.status.is_some())
            .collect();
        assert!(
            !with_status.is_empty(),
            "fixture must contain at least one chain with a status field"
        );
        // Base (8453) 和 Ink (57073) 有 status="active"。
        assert!(with_status.iter().any(|c| c.chain_id == 8453));
    }

    #[test]
    fn fixture_has_tracking_endpoints() {
        let (catalog, _) =
            parse_catalog(BUILTIN_FIXTURE, true, true, &HashSet::new()).expect("parse catalog");
        let has_tracking = catalog
            .chains
            .iter()
            .any(|c| c.endpoints.iter().any(|e| e.tracking.is_some()));
        assert!(
            has_tracking,
            "fixture must have endpoints with tracking field"
        );
    }

    #[test]
    fn fixture_filters_template_and_wss_endpoints() {
        let (catalog, _) =
            parse_catalog(BUILTIN_FIXTURE, true, true, &HashSet::new()).expect("parse catalog");
        // 所有端点 URL 必须是 https 且不含 ${。
        for chain in &catalog.chains {
            for ep in &chain.endpoints {
                assert!(
                    ep.url.starts_with("https://"),
                    "endpoint URL must be https: {}",
                    ep.url
                );
                assert!(
                    !ep.url.contains("${"),
                    "endpoint URL must not contain template: {}",
                    ep.url
                );
            }
        }
        // 验证：Arbitrum (42161) 的 fixture 包含 ${KEY} 模板端点，解析后应被过滤。
        let arb = catalog.lookup(42161).expect("Arbitrum in fixture");
        // 模板端点已被过滤，所有端点都是 https。
        assert!(
            arb.endpoints.iter().all(|e| !e.url.contains("${")),
            "Arbitrum endpoints must not contain template variables"
        );
    }

    #[test]
    fn fixture_catalog_dedup_assertion() {
        let (catalog, _) =
            parse_catalog(BUILTIN_FIXTURE, true, true, &HashSet::new()).expect("parse catalog");
        // 每条链的端点 URL 无重复。
        for chain in &catalog.chains {
            let mut seen = HashSet::new();
            for ep in &chain.endpoints {
                assert!(
                    seen.insert(&ep.url),
                    "duplicate endpoint URL in chain {}: {}",
                    chain.chain_id,
                    ep.url
                );
            }
        }
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

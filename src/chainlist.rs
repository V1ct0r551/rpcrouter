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
use tokio::{fs, sync::Mutex};
use tracing::warn;

use crate::config::Config;

const BUILTIN_FIXTURE: &[u8] = include_bytes!("../fixtures/rpcs.sample.json");

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotSource {
    Network,
    NotModified,
    Memory,
    Disk,
    Fixture,
}

#[derive(Clone, Debug)]
pub struct LoadResult {
    pub snapshot: Arc<ChainlistSnapshot>,
    pub source: SnapshotSource,
}

#[derive(Default)]
struct LoaderState {
    etag: Option<HeaderValue>,
    last_success: Option<Arc<ChainlistSnapshot>>,
}

pub struct ChainlistLoader {
    client: Client,
    url: String,
    cache_path: PathBuf,
    allowed_chains: HashSet<u64>,
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
            config.chains.iter().copied(),
        )
    }

    pub fn with_client(
        client: Client,
        url: String,
        cache_path: PathBuf,
        allowed_chains: impl IntoIterator<Item = u64>,
    ) -> Result<Self> {
        Url::parse(&url).with_context(|| format!("invalid chainlist URL {url}"))?;
        Ok(Self {
            client,
            url,
            cache_path,
            allowed_chains: allowed_chains.into_iter().collect(),
            state: Mutex::new(LoaderState::default()),
        })
    }

    /// 优先联网刷新；失败后依次回退到进程内快照、磁盘缓存和内置样例。
    pub async fn load(&self) -> Result<LoadResult> {
        match self.fetch_network().await {
            Ok(result) => return Ok(result),
            Err(error) => warn!(error = %error, "chainlist network refresh failed"),
        }

        if let Some(snapshot) = self.state.lock().await.last_success.clone() {
            return Ok(LoadResult {
                snapshot,
                source: SnapshotSource::Memory,
            });
        }

        match fs::read(&self.cache_path).await {
            Ok(bytes) => match parse_and_filter(&bytes, &self.allowed_chains) {
                Ok(snapshot) => {
                    return Ok(self.remember(snapshot, SnapshotSource::Disk).await);
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

        let snapshot = parse_and_filter(BUILTIN_FIXTURE, &self.allowed_chains)
            .context("built-in chainlist fixture is invalid")?;
        Ok(self.remember(snapshot, SnapshotSource::Fixture).await)
    }

    async fn fetch_network(&self) -> Result<LoadResult> {
        let etag = self.state.lock().await.etag.clone();
        let mut request = self.client.get(&self.url);
        if let Some(etag) = etag {
            request = request.header(IF_NONE_MATCH, etag);
        }

        let response = request.send().await.context("chainlist request failed")?;
        if response.status() == StatusCode::NOT_MODIFIED {
            let snapshot = self
                .state
                .lock()
                .await
                .last_success
                .clone()
                .context("chainlist returned 304 without an in-memory snapshot")?;
            return Ok(LoadResult {
                snapshot,
                source: SnapshotSource::NotModified,
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
        let snapshot = parse_and_filter(&bytes, &self.allowed_chains)
            .context("chainlist response is invalid")?;
        if let Err(error) = persist_cache(&self.cache_path, &bytes).await {
            warn!(
                path = %self.cache_path.display(),
                error = %error,
                "chainlist disk cache could not be updated"
            );
        }

        let snapshot = Arc::new(snapshot);
        let mut state = self.state.lock().await;
        state.etag = response_etag;
        state.last_success = Some(Arc::clone(&snapshot));
        Ok(LoadResult {
            snapshot,
            source: SnapshotSource::Network,
        })
    }

    async fn remember(&self, snapshot: ChainlistSnapshot, source: SnapshotSource) -> LoadResult {
        let snapshot = Arc::new(snapshot);
        self.state.lock().await.last_success = Some(Arc::clone(&snapshot));
        LoadResult { snapshot, source }
    }
}

async fn persist_cache(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let temporary_path = path.with_extension("json.tmp");
    fs::write(&temporary_path, bytes)
        .await
        .with_context(|| format!("failed to write {}", temporary_path.display()))?;
    fs::rename(&temporary_path, path)
        .await
        .with_context(|| format!("failed to replace {}", path.display()))?;
    Ok(())
}

pub fn parse_and_filter(bytes: &[u8], allowed_chains: &HashSet<u64>) -> Result<ChainlistSnapshot> {
    let document: ChainlistDocument =
        serde_json::from_slice(bytes).context("invalid chainlist JSON")?;
    let records = match document {
        ChainlistDocument::List(records) => records,
        ChainlistDocument::Wrapped { chains } => chains,
    };

    let chains = records
        .into_iter()
        .filter(|record| allowed_chains.contains(&record.chain_id))
        .map(|record| {
            let mut seen = HashSet::new();
            let endpoints = record
                .rpc
                .into_iter()
                .filter_map(RpcEntry::into_url)
                .filter_map(normalize_public_https_url)
                .filter(|url| seen.insert(url.clone()))
                .collect();
            ChainEndpoints {
                chain_id: record.chain_id,
                name: record.name,
                endpoints,
            }
        })
        .collect();
    Ok(ChainlistSnapshot { chains })
}

fn normalize_public_https_url(raw: String) -> Option<String> {
    if raw.contains("${") {
        return None;
    }
    let mut url = Url::parse(&raw).ok()?;
    if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
        return None;
    }
    url.set_fragment(None);
    Some(url.to_string())
}

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
    rpc: Vec<RpcEntry>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RpcEntry {
    Object { url: String },
    String(String),
}

impl RpcEntry {
    fn into_url(self) -> Option<String> {
        match self {
            Self::Object { url } | Self::String(url) => Some(url),
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
        let loader = ChainlistLoader::with_client(Client::new(), url, cache_path.clone(), [1, 143])
            .expect("loader");

        assert_eq!(
            loader.load().await.expect("network load").source,
            SnapshotSource::Network
        );
        assert_eq!(
            loader.load().await.expect("not modified").source,
            SnapshotSource::NotModified
        );
        assert_eq!(
            loader.load().await.expect("memory fallback").source,
            SnapshotSource::Memory
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        let _ = fs::remove_file(cache_path).await;
    }

    #[tokio::test]
    async fn falls_back_to_disk_then_builtin_fixture() {
        let app = Router::new().route(
            "/rpcs.json",
            get(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        );
        let url = spawn_server(app).await;
        let disk_path = temporary_cache_path("disk");
        fs::write(&disk_path, BUILTIN_FIXTURE)
            .await
            .expect("write disk cache");
        let disk_loader =
            ChainlistLoader::with_client(Client::new(), url.clone(), disk_path.clone(), [1, 143])
                .expect("disk loader");
        assert_eq!(
            disk_loader.load().await.expect("disk fallback").source,
            SnapshotSource::Disk
        );

        let missing_path = temporary_cache_path("missing");
        let fixture_loader =
            ChainlistLoader::with_client(Client::new(), url, missing_path, [1, 143])
                .expect("fixture loader");
        assert_eq!(
            fixture_loader
                .load()
                .await
                .expect("fixture fallback")
                .source,
            SnapshotSource::Fixture
        );
        let _ = fs::remove_file(disk_path).await;
    }
}

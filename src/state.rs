//! 持久状态镜像。数据面只读 Registry 内存；本模块只在启动、控制操作和后台 flush 使用。

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use redis::{
    AsyncCommands,
    aio::{ConnectionManager, ConnectionManagerConfig},
    pipe,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    sync::{Mutex, RwLock},
    time::{Duration, timeout},
};
use tracing::{info, warn};

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct ChainOverrideState {
    pub pinned: Option<bool>,
    pub disabled: Option<bool>,
    pub block_time_ms: Option<u64>,
    pub confirmation_depth: Option<u64>,
    pub tip_ttl_ms: Option<u64>,
    pub max_block_lag: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EndpointOverrideState {
    pub url: String,
    pub disabled: Option<bool>,
    pub rps: Option<u32>,
    pub concurrency: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct HealthSnapshot {
    pub chain_id: u64,
    pub url: String,
    pub state: String,
    pub cooling_until_unix: Option<u64>,
    pub strikes: u32,
    pub latency_ewma_us: u64,
    pub lag: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct Overrides {
    pub chains: BTreeMap<u64, ChainOverrideState>,
    pub endpoints: BTreeMap<String, EndpointOverrideState>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct BootstrapState {
    pub schema_version: u32,
    pub catalog: Option<Value>,
    pub overrides: Overrides,
    pub health: Vec<HealthSnapshot>,
    pub hot_chains: Vec<(u64, u64)>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct StateExport {
    pub schema_version: u32,
    pub catalog: Option<Value>,
    pub overrides: Overrides,
    pub health: Vec<HealthSnapshot>,
    pub hot_chains: Vec<(u64, u64)>,
}

#[async_trait]
pub trait StateStore: Send + Sync {
    async fn bootstrap(&self) -> Result<BootstrapState>;
    async fn set_catalog(&self, catalog: &Value) -> Result<()>;
    async fn load_overrides(&self) -> Result<Overrides>;
    async fn put_chain_override(&self, chain_id: u64, value: &ChainOverrideState) -> Result<()>;
    async fn delete_chain_override(&self, chain_id: u64) -> Result<()>;
    async fn put_endpoint_override(&self, key: &str, value: &EndpointOverrideState) -> Result<()>;
    async fn delete_endpoint_override(&self, key: &str) -> Result<()>;
    async fn flush_health(&self, batch: &[HealthSnapshot]) -> Result<()>;
    async fn load_health(&self) -> Result<Vec<HealthSnapshot>>;
    async fn set_hot_chains(&self, chains: &[(u64, u64)]) -> Result<()>;
    async fn append_audit(&self, what: &str, target: &str) -> Result<()>;
    async fn export(&self) -> Result<StateExport>;
    async fn import(&self, value: &StateExport) -> Result<()>;
    async fn reset(&self) -> Result<()>;
    async fn health(&self) -> bool;
    fn call_count(&self) -> u64 {
        0
    }
    fn backend_name(&self) -> &'static str {
        "unknown"
    }
    fn audit_count(&self) -> u64 {
        0
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct FileDocument {
    schema_version: u32,
    catalog: Option<Value>,
    overrides: Overrides,
    health: Vec<HealthSnapshot>,
    hot_chains: Vec<(u64, u64)>,
    audit: Vec<AuditEntry>,
    seeded_at: u64,
    last_flush_at: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct AuditEntry {
    at: u64,
    what: String,
    target: String,
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

#[derive(Clone)]
pub struct MemoryStore {
    inner: Arc<Mutex<FileDocument>>,
    calls: Arc<AtomicU64>,
}
impl Default for MemoryStore {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(FileDocument {
                schema_version: SCHEMA_VERSION,
                ..Default::default()
            })),
            calls: Arc::new(AtomicU64::new(0)),
        }
    }
}
impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
    fn bump(&self) {
        self.calls.fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait]
impl StateStore for MemoryStore {
    async fn bootstrap(&self) -> Result<BootstrapState> {
        self.bump();
        let d = self.inner.lock().await;
        if d.schema_version != SCHEMA_VERSION {
            bail!("unsupported state schema version {}", d.schema_version);
        }
        Ok(BootstrapState {
            schema_version: d.schema_version,
            catalog: d.catalog.clone(),
            overrides: d.overrides.clone(),
            health: d.health.clone(),
            hot_chains: d.hot_chains.clone(),
        })
    }
    async fn set_catalog(&self, catalog: &Value) -> Result<()> {
        self.bump();
        self.inner.lock().await.catalog = Some(catalog.clone());
        Ok(())
    }
    async fn load_overrides(&self) -> Result<Overrides> {
        self.bump();
        Ok(self.inner.lock().await.overrides.clone())
    }
    async fn put_chain_override(&self, id: u64, v: &ChainOverrideState) -> Result<()> {
        self.bump();
        self.inner
            .lock()
            .await
            .overrides
            .chains
            .insert(id, v.clone());
        Ok(())
    }
    async fn delete_chain_override(&self, id: u64) -> Result<()> {
        self.bump();
        self.inner.lock().await.overrides.chains.remove(&id);
        Ok(())
    }
    async fn put_endpoint_override(&self, key: &str, v: &EndpointOverrideState) -> Result<()> {
        self.bump();
        self.inner
            .lock()
            .await
            .overrides
            .endpoints
            .insert(key.to_owned(), v.clone());
        Ok(())
    }
    async fn delete_endpoint_override(&self, key: &str) -> Result<()> {
        self.bump();
        self.inner.lock().await.overrides.endpoints.remove(key);
        Ok(())
    }
    async fn flush_health(&self, batch: &[HealthSnapshot]) -> Result<()> {
        self.bump();
        let mut d = self.inner.lock().await;
        for h in batch {
            if let Some(old) = d
                .health
                .iter_mut()
                .find(|x| x.chain_id == h.chain_id && x.url == h.url)
            {
                *old = h.clone()
            } else {
                d.health.push(h.clone())
            }
        }
        d.last_flush_at = now();
        Ok(())
    }
    async fn load_health(&self) -> Result<Vec<HealthSnapshot>> {
        self.bump();
        Ok(self.inner.lock().await.health.clone())
    }
    async fn set_hot_chains(&self, chains: &[(u64, u64)]) -> Result<()> {
        self.bump();
        self.inner.lock().await.hot_chains = chains.to_vec();
        Ok(())
    }
    async fn append_audit(&self, what: &str, target: &str) -> Result<()> {
        self.bump();
        let mut d = self.inner.lock().await;
        d.audit.push(AuditEntry {
            at: now(),
            what: what.to_owned(),
            target: target.to_owned(),
        });
        let excess = d.audit.len().saturating_sub(10_000);
        if excess > 0 {
            d.audit.drain(..excess);
        }
        Ok(())
    }
    async fn export(&self) -> Result<StateExport> {
        self.bump();
        let d = self.inner.lock().await;
        Ok(StateExport {
            schema_version: d.schema_version,
            catalog: d.catalog.clone(),
            overrides: d.overrides.clone(),
            health: d.health.clone(),
            hot_chains: d.hot_chains.clone(),
        })
    }
    async fn import(&self, v: &StateExport) -> Result<()> {
        self.bump();
        if v.schema_version != SCHEMA_VERSION {
            bail!("unsupported state schema version {}", v.schema_version);
        }
        let mut d = self.inner.lock().await;
        d.schema_version = v.schema_version;
        d.catalog = v.catalog.clone();
        d.overrides = v.overrides.clone();
        d.health = v.health.clone();
        d.hot_chains = v.hot_chains.clone();
        Ok(())
    }
    async fn reset(&self) -> Result<()> {
        self.bump();
        *self.inner.lock().await = FileDocument {
            schema_version: SCHEMA_VERSION,
            seeded_at: now(),
            ..Default::default()
        };
        Ok(())
    }
    async fn health(&self) -> bool {
        true
    }
    fn call_count(&self) -> u64 {
        self.calls.load(Ordering::Relaxed)
    }
    fn backend_name(&self) -> &'static str {
        "memory"
    }
    fn audit_count(&self) -> u64 {
        self.inner.try_lock().map_or(0, |d| d.audit.len() as u64)
    }
}

#[derive(Clone)]
pub struct FileStore {
    path: PathBuf,
    inner: Arc<Mutex<FileDocument>>,
    calls: Arc<AtomicU64>,
}
impl FileStore {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let d = match tokio::fs::read(&path).await {
            Ok(b) => match serde_json::from_slice::<FileDocument>(&b) {
                Ok(document) if document.schema_version == SCHEMA_VERSION => document,
                Ok(document) => {
                    let corrupt = PathBuf::from(format!("{}.corrupt-{}", path.display(), now()));
                    let _ = tokio::fs::rename(&path, &corrupt).await;
                    warn!(path=%path.display(), schema=document.schema_version, "state file schema is unsupported; starting empty");
                    FileDocument {
                        schema_version: SCHEMA_VERSION,
                        seeded_at: now(),
                        ..Default::default()
                    }
                }
                Err(error) => {
                    let corrupt = PathBuf::from(format!("{}.corrupt-{}", path.display(), now()));
                    let _ = tokio::fs::rename(&path, &corrupt).await;
                    warn!(path=%path.display(), error=%error, "state file is corrupt; starting empty");
                    FileDocument {
                        schema_version: SCHEMA_VERSION,
                        seeded_at: now(),
                        ..Default::default()
                    }
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => FileDocument {
                schema_version: SCHEMA_VERSION,
                seeded_at: now(),
                ..Default::default()
            },
            Err(e) => return Err(e.into()),
        };
        Ok(Self {
            path,
            inner: Arc::new(Mutex::new(d)),
            calls: Arc::new(AtomicU64::new(0)),
        })
    }
    async fn save(&self) -> Result<()> {
        let d = self.inner.lock().await.clone();
        if let Some(p) = self.path.parent() {
            tokio::fs::create_dir_all(p).await?;
        }
        let tmp = self.path.with_extension("json.tmp");
        let bytes = serde_json::to_vec(&d)?;
        if tokio::fs::read(&self.path).await.ok().as_deref() == Some(bytes.as_slice()) {
            return Ok(());
        }
        tokio::fs::write(&tmp, bytes).await?;
        tokio::fs::rename(tmp, &self.path).await?;
        Ok(())
    }
    fn bump(&self) {
        self.calls.fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait]
impl StateStore for FileStore {
    async fn bootstrap(&self) -> Result<BootstrapState> {
        self.bump();
        let d = self.inner.lock().await;
        Ok(BootstrapState {
            schema_version: d.schema_version,
            catalog: d.catalog.clone(),
            overrides: d.overrides.clone(),
            health: d.health.clone(),
            hot_chains: d.hot_chains.clone(),
        })
    }
    async fn set_catalog(&self, v: &Value) -> Result<()> {
        self.bump();
        self.inner.lock().await.catalog = Some(v.clone());
        self.save().await
    }
    async fn load_overrides(&self) -> Result<Overrides> {
        self.bump();
        Ok(self.inner.lock().await.overrides.clone())
    }
    async fn put_chain_override(&self, id: u64, v: &ChainOverrideState) -> Result<()> {
        self.bump();
        self.inner
            .lock()
            .await
            .overrides
            .chains
            .insert(id, v.clone());
        self.save().await
    }
    async fn delete_chain_override(&self, id: u64) -> Result<()> {
        self.bump();
        self.inner.lock().await.overrides.chains.remove(&id);
        self.save().await
    }
    async fn put_endpoint_override(&self, k: &str, v: &EndpointOverrideState) -> Result<()> {
        self.bump();
        self.inner
            .lock()
            .await
            .overrides
            .endpoints
            .insert(k.to_owned(), v.clone());
        self.save().await
    }
    async fn delete_endpoint_override(&self, k: &str) -> Result<()> {
        self.bump();
        self.inner.lock().await.overrides.endpoints.remove(k);
        self.save().await
    }
    async fn flush_health(&self, b: &[HealthSnapshot]) -> Result<()> {
        self.bump();
        let mut d = self.inner.lock().await;
        for h in b {
            if let Some(x) = d
                .health
                .iter_mut()
                .find(|x| x.chain_id == h.chain_id && x.url == h.url)
            {
                *x = h.clone()
            } else {
                d.health.push(h.clone())
            }
        }
        d.last_flush_at = now();
        drop(d);
        self.save().await
    }
    async fn load_health(&self) -> Result<Vec<HealthSnapshot>> {
        self.bump();
        Ok(self.inner.lock().await.health.clone())
    }
    async fn set_hot_chains(&self, c: &[(u64, u64)]) -> Result<()> {
        self.bump();
        self.inner.lock().await.hot_chains = c.to_vec();
        self.save().await
    }
    async fn append_audit(&self, w: &str, t: &str) -> Result<()> {
        self.bump();
        self.inner.lock().await.audit.push(AuditEntry {
            at: now(),
            what: w.to_owned(),
            target: t.to_owned(),
        });
        self.save().await
    }
    async fn export(&self) -> Result<StateExport> {
        self.bump();
        let d = self.inner.lock().await;
        Ok(StateExport {
            schema_version: d.schema_version,
            catalog: d.catalog.clone(),
            overrides: d.overrides.clone(),
            health: d.health.clone(),
            hot_chains: d.hot_chains.clone(),
        })
    }
    async fn import(&self, v: &StateExport) -> Result<()> {
        self.bump();
        if v.schema_version != SCHEMA_VERSION {
            bail!("unsupported state schema version {}", v.schema_version)
        }
        let mut d = self.inner.lock().await;
        d.schema_version = v.schema_version;
        d.catalog = v.catalog.clone();
        d.overrides = v.overrides.clone();
        d.health = v.health.clone();
        d.hot_chains = v.hot_chains.clone();
        drop(d);
        self.save().await
    }
    async fn reset(&self) -> Result<()> {
        self.bump();
        *self.inner.lock().await = FileDocument {
            schema_version: SCHEMA_VERSION,
            seeded_at: now(),
            ..Default::default()
        };
        self.save().await
    }
    async fn health(&self) -> bool {
        true
    }
    fn call_count(&self) -> u64 {
        self.calls.load(Ordering::Relaxed)
    }
    fn backend_name(&self) -> &'static str {
        "file"
    }
    fn audit_count(&self) -> u64 {
        self.inner.try_lock().map_or(0, |d| d.audit.len() as u64)
    }
}

#[derive(Clone)]
pub struct RedisStore {
    manager: Arc<Mutex<ConnectionManager>>,
    prefix: String,
    instance_id: String,
    calls: Arc<AtomicU64>,
    health_ttl_seconds: u64,
}
impl RedisStore {
    pub async fn connect(url: &str, namespace: &str) -> Result<Self> {
        Self::connect_with_ttl(url, namespace, 86_400).await
    }

    pub async fn connect_with_ttl(
        url: &str,
        namespace: &str,
        health_ttl_seconds: u64,
    ) -> Result<Self> {
        let client = redis::Client::open(url).context("invalid redis URL")?;
        if namespace.contains(':') || namespace.contains('}') || namespace.trim().is_empty() {
            bail!("state namespace contains invalid characters");
        }
        let manager_config = ConnectionManagerConfig::new()
            .set_number_of_retries(0)
            .set_connection_timeout(std::time::Duration::from_secs(2))
            .set_response_timeout(std::time::Duration::from_secs(5));
        let manager = timeout(
            Duration::from_secs(2),
            ConnectionManager::new_with_config(client, manager_config),
        )
        .await
        .context("Redis connection timed out")?
        .context("failed to connect to Redis")?;
        Ok(Self {
            manager: Arc::new(Mutex::new(manager)),
            prefix: format!("{{{namespace}}}"),
            instance_id: std::env::var("RPCROUTER_INSTANCE_ID")
                .ok()
                .filter(|id| !id.is_empty())
                .unwrap_or_else(|| {
                    format!(
                        "{}-{}",
                        std::env::var("HOSTNAME").unwrap_or_else(|_| "rpcrouter".into()),
                        std::process::id()
                    )
                }),
            calls: Arc::new(AtomicU64::new(0)),
            health_ttl_seconds,
        })
    }
    fn key(&self, s: &str) -> String {
        format!("{}:{s}", self.prefix)
    }
    fn bump(&self) {
        self.calls.fetch_add(1, Ordering::Relaxed);
    }
    async fn indexed_json(&self, index: &str) -> Result<Vec<(String, String)>> {
        let mut c = self.manager.lock().await;
        let keys: Vec<String> = timeout(Duration::from_secs(5), c.smembers(self.key(index)))
            .await
            .context("Redis index read timed out")??;
        let mut out = Vec::new();
        for key in keys {
            let raw: Option<String> = timeout(
                Duration::from_secs(5),
                redis::cmd("HGET")
                    .arg(&key)
                    .arg("json")
                    .query_async(&mut *c),
            )
            .await
            .context("Redis indexed value read timed out")??;
            if let Some(raw) = raw {
                out.push((key, raw));
            }
        }
        Ok(out)
    }
    async fn scan_namespace(&self) -> Result<Vec<String>> {
        let mut cursor = 0u64;
        let mut out = Vec::new();
        let mut c = self.manager.lock().await;
        loop {
            let (next, mut keys): (u64, Vec<String>) = timeout(
                Duration::from_secs(5),
                redis::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(format!("{}:*", self.prefix))
                    .arg("COUNT")
                    .arg(1000)
                    .query_async(&mut *c),
            )
            .await
            .context("Redis namespace scan timed out")??;
            out.append(&mut keys);
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl StateStore for RedisStore {
    async fn bootstrap(&self) -> Result<BootstrapState> {
        self.bump();
        let mut c = self.manager.lock().await;
        let schema: Option<String> = timeout(
            Duration::from_secs(3),
            redis::cmd("HGET")
                .arg(self.key("meta"))
                .arg("schema_version")
                .query_async(&mut *c),
        )
        .await
        .context("Redis bootstrap timed out")??;
        let is_empty = schema.is_none();
        let schema_version = schema
            .and_then(|x| x.parse().ok())
            .unwrap_or(SCHEMA_VERSION);
        if schema_version != SCHEMA_VERSION {
            bail!("unsupported state schema version {schema_version}");
        }
        if is_empty {
            let _: () = timeout(
                Duration::from_secs(3),
                redis::cmd("HSET")
                    .arg(self.key("meta"))
                    .arg("schema_version")
                    .arg(SCHEMA_VERSION)
                    .arg("seeded_at")
                    .arg(now())
                    .query_async(&mut *c),
            )
            .await
            .context("Redis schema seed timed out")??;
        }
        let catalog_raw: Option<String> = timeout(
            Duration::from_secs(3),
            redis::cmd("GET")
                .arg(self.key("catalog"))
                .query_async(&mut *c),
        )
        .await
        .context("Redis catalog read timed out")??;
        let hot_raw: Vec<(u64, u64)> = timeout(
            Duration::from_secs(3),
            c.zrange_withscores(self.key(&format!("hot:{}", self.instance_id)), 0, -1),
        )
        .await
        .context("Redis hot set read timed out")??;
        drop(c);
        Ok(BootstrapState {
            schema_version,
            catalog: catalog_raw
                .map(|raw| serde_json::from_str(&raw))
                .transpose()?,
            overrides: self.load_overrides().await?,
            health: self.load_health().await?,
            hot_chains: hot_raw,
        })
    }
    async fn set_catalog(&self, v: &Value) -> Result<()> {
        self.bump();
        let raw = serde_json::to_string(v)?;
        let mut c = self.manager.lock().await;
        let _: () = pipe()
            .cmd("SET")
            .arg(self.key("catalog"))
            .arg(raw)
            .cmd("HSET")
            .arg(self.key("meta"))
            .arg("schema_version")
            .arg(SCHEMA_VERSION)
            .arg("fetched_at")
            .arg(now())
            .query_async(&mut *c)
            .await?;
        Ok(())
    }
    async fn load_overrides(&self) -> Result<Overrides> {
        self.bump();
        let mut result = Overrides::default();
        for (key, raw) in self.indexed_json("override:index").await? {
            if let Some(id) = key
                .strip_prefix(&format!("{}:override:chain:", self.prefix))
                .and_then(|id| id.parse().ok())
            {
                result.chains.insert(id, serde_json::from_str(&raw)?);
            } else if let Some(endpoint) =
                key.strip_prefix(&format!("{}:override:endpoint:", self.prefix))
            {
                result
                    .endpoints
                    .insert(endpoint.to_owned(), serde_json::from_str(&raw)?);
            }
        }
        Ok(result)
    }
    async fn put_chain_override(&self, id: u64, v: &ChainOverrideState) -> Result<()> {
        self.bump();
        let key = self.key(&format!("override:chain:{id}"));
        let mut c = self.manager.lock().await;
        let _: () = pipe()
            .atomic()
            .cmd("HSET")
            .arg(&key)
            .arg("json")
            .arg(serde_json::to_string(v)?)
            .cmd("SADD")
            .arg(self.key("override:index"))
            .arg(&key)
            .query_async(&mut *c)
            .await?;
        Ok(())
    }
    async fn delete_chain_override(&self, id: u64) -> Result<()> {
        self.bump();
        let key = self.key(&format!("override:chain:{id}"));
        let mut c = self.manager.lock().await;
        let _: () = pipe()
            .atomic()
            .cmd("DEL")
            .arg(&key)
            .cmd("SREM")
            .arg(self.key("override:index"))
            .arg(&key)
            .query_async(&mut *c)
            .await?;
        Ok(())
    }
    async fn put_endpoint_override(&self, k: &str, v: &EndpointOverrideState) -> Result<()> {
        self.bump();
        let key = self.key(&format!("override:endpoint:{k}"));
        let mut c = self.manager.lock().await;
        let _: () = pipe()
            .atomic()
            .cmd("HSET")
            .arg(&key)
            .arg("json")
            .arg(serde_json::to_string(v)?)
            .cmd("SADD")
            .arg(self.key("override:index"))
            .arg(&key)
            .query_async(&mut *c)
            .await?;
        Ok(())
    }
    async fn delete_endpoint_override(&self, k: &str) -> Result<()> {
        self.bump();
        let key = self.key(&format!("override:endpoint:{k}"));
        let mut c = self.manager.lock().await;
        let _: () = pipe()
            .atomic()
            .cmd("DEL")
            .arg(&key)
            .cmd("SREM")
            .arg(self.key("override:index"))
            .arg(&key)
            .query_async(&mut *c)
            .await?;
        Ok(())
    }
    async fn flush_health(&self, b: &[HealthSnapshot]) -> Result<()> {
        self.bump();
        let mut p = pipe();
        for h in b {
            let key = self.key(&format!(
                "health:{}:{}",
                h.chain_id,
                endpoint_key(h.chain_id, &h.url)
                    .split_once(':')
                    .map_or("unknown", |x| x.1)
            ));
            p.cmd("HSET")
                .arg(&key)
                .arg("json")
                .arg(serde_json::to_string(h)?)
                .cmd("EXPIRE")
                .arg(&key)
                .arg(self.health_ttl_seconds)
                .cmd("SADD")
                .arg(self.key("health:index"))
                .arg(&key);
        }
        p.cmd("HSET")
            .arg(self.key("meta"))
            .arg("last_flush_at")
            .arg(now());
        let mut c = self.manager.lock().await;
        let _: () = p.query_async(&mut *c).await?;
        Ok(())
    }
    async fn load_health(&self) -> Result<Vec<HealthSnapshot>> {
        self.bump();
        let mut result = Vec::new();
        for (_, raw) in self.indexed_json("health:index").await? {
            result.push(serde_json::from_str(&raw)?);
        }
        Ok(result)
    }
    async fn set_hot_chains(&self, c: &[(u64, u64)]) -> Result<()> {
        self.bump();
        let mut p = pipe();
        let hot_key = self.key(&format!("hot:{}", self.instance_id));
        p.cmd("DEL").arg(&hot_key);
        for (id, score) in c {
            p.cmd("ZADD").arg(&hot_key).arg(*score).arg(*id);
        }
        p.cmd("EXPIRE").arg(&hot_key).arg(60);
        let mut conn = self.manager.lock().await;
        let _: () = p.query_async(&mut *conn).await?;
        Ok(())
    }
    async fn append_audit(&self, w: &str, t: &str) -> Result<()> {
        self.bump();
        let mut c = self.manager.lock().await;
        let _: () = pipe()
            .cmd("XADD")
            .arg(self.key("audit"))
            .arg("MAXLEN")
            .arg("~")
            .arg(10_000)
            .arg("*")
            .arg("what")
            .arg(w)
            .arg("target")
            .arg(t)
            .query_async(&mut *c)
            .await?;
        Ok(())
    }
    async fn export(&self) -> Result<StateExport> {
        self.bump();
        let d = self.bootstrap().await?;
        Ok(StateExport {
            schema_version: d.schema_version,
            catalog: d.catalog,
            overrides: d.overrides,
            health: d.health,
            hot_chains: d.hot_chains,
        })
    }
    async fn import(&self, v: &StateExport) -> Result<()> {
        self.bump();
        if v.schema_version != SCHEMA_VERSION {
            bail!("unsupported state schema version {}", v.schema_version)
        }
        let old_keys = self.scan_namespace().await?;
        let mut p = pipe();
        p.atomic();
        for key in old_keys {
            p.cmd("DEL").arg(key);
        }
        p.cmd("HSET")
            .arg(self.key("meta"))
            .arg("schema_version")
            .arg(SCHEMA_VERSION)
            .arg("seeded_at")
            .arg(now());
        if let Some(catalog) = &v.catalog {
            p.cmd("SET")
                .arg(self.key("catalog"))
                .arg(serde_json::to_string(catalog)?);
        }
        for (id, value) in &v.overrides.chains {
            let key = self.key(&format!("override:chain:{id}"));
            p.cmd("HSET")
                .arg(&key)
                .arg("json")
                .arg(serde_json::to_string(value)?)
                .cmd("SADD")
                .arg(self.key("override:index"))
                .arg(&key);
        }
        for (name, value) in &v.overrides.endpoints {
            let key = self.key(&format!("override:endpoint:{name}"));
            p.cmd("HSET")
                .arg(&key)
                .arg("json")
                .arg(serde_json::to_string(value)?)
                .cmd("SADD")
                .arg(self.key("override:index"))
                .arg(&key);
        }
        for health in &v.health {
            let key = self.key(&format!(
                "health:{}:{}",
                health.chain_id,
                endpoint_key(health.chain_id, &health.url)
            ));
            p.cmd("HSET")
                .arg(&key)
                .arg("json")
                .arg(serde_json::to_string(health)?)
                .cmd("EXPIRE")
                .arg(&key)
                .arg(self.health_ttl_seconds)
                .cmd("SADD")
                .arg(self.key("health:index"))
                .arg(&key);
        }
        for (id, score) in &v.hot_chains {
            p.cmd("ZADD")
                .arg(self.key(&format!("hot:{}", self.instance_id)))
                .arg(*score)
                .arg(*id);
        }
        let mut c = self.manager.lock().await;
        let _: () = timeout(Duration::from_secs(5), p.query_async(&mut *c))
            .await
            .context("Redis import timed out")??;
        Ok(())
    }
    async fn reset(&self) -> Result<()> {
        self.bump();
        let keys = self.scan_namespace().await?;
        let mut p = pipe();
        p.atomic();
        for key in keys {
            p.cmd("DEL").arg(key);
        }
        let mut c = self.manager.lock().await;
        let _: () = timeout(Duration::from_secs(5), p.query_async(&mut *c))
            .await
            .context("Redis reset timed out")??;
        Ok(())
    }
    async fn health(&self) -> bool {
        let mut c = self.manager.lock().await;
        timeout(
            Duration::from_secs(5),
            redis::cmd("PING").query_async::<String>(&mut *c),
        )
        .await
        .is_ok_and(|result| result.is_ok())
    }
    fn call_count(&self) -> u64 {
        self.calls.load(Ordering::Relaxed)
    }
    fn backend_name(&self) -> &'static str {
        "redis"
    }
}

pub fn endpoint_key(chain_id: u64, url: &str) -> String {
    format!(
        "{chain_id}:{}",
        blake3::hash(url.as_bytes()).to_hex().to_string()[..16].to_owned()
    )
}

/// Redis 可选模式的降级门面：文件镜像始终写入，Redis 断线时继续服务并可后台重连回灌。
pub struct ResilientStore {
    fallback: Arc<FileStore>,
    primary: RwLock<Option<Arc<RedisStore>>>,
    url: String,
    namespace: String,
    health_ttl_seconds: u64,
}

impl ResilientStore {
    pub async fn open(url: &str, namespace: &str, ttl: u64, file_path: &Path) -> Result<Self> {
        let fallback = Arc::new(FileStore::open(file_path).await?);
        let primary = match timeout(
            Duration::from_secs(2),
            RedisStore::connect_with_ttl(url, namespace, ttl),
        )
        .await
        {
            Ok(Ok(store)) => Some(Arc::new(store)),
            Ok(Err(error)) => {
                warn!(error=%error, "optional Redis state store unavailable; using local cache");
                None
            }
            Err(_) => {
                warn!("optional Redis state store connection timed out; using local cache");
                None
            }
        };
        Ok(Self {
            fallback,
            primary: RwLock::new(primary),
            url: url.to_owned(),
            namespace: namespace.to_owned(),
            health_ttl_seconds: ttl,
        })
    }
    async fn primary(&self) -> Option<Arc<RedisStore>> {
        self.primary.read().await.clone()
    }
    async fn failed(&self) {
        *self.primary.write().await = None;
    }
    pub async fn reconnect(&self) -> Result<bool> {
        if self.primary().await.is_some() {
            return Ok(false);
        }
        let redis = Arc::new(
            RedisStore::connect_with_ttl(&self.url, &self.namespace, self.health_ttl_seconds)
                .await?,
        );
        // Redis is authoritative once it has a schema marker; never import the local
        // fallback over a non-empty Redis namespace.
        let _ = redis.bootstrap().await?;
        let local = self.fallback.export().await?;
        if !local.health.is_empty() {
            redis.flush_health(&local.health).await?;
        }
        info!("Redis state reconnected; local fallback was not imported over Redis");
        *self.primary.write().await = Some(redis);
        Ok(true)
    }
}

#[async_trait]
impl StateStore for ResilientStore {
    async fn bootstrap(&self) -> Result<BootstrapState> {
        if let Some(p) = self.primary().await {
            match p.bootstrap().await {
                Ok(v) => return Ok(v),
                Err(_) => self.failed().await,
            }
        }
        self.fallback.bootstrap().await
    }
    async fn set_catalog(&self, v: &Value) -> Result<()> {
        self.fallback.set_catalog(v).await?;
        if let Some(p) = self.primary().await
            && p.set_catalog(v).await.is_err()
        {
            self.failed().await
        }
        Ok(())
    }
    async fn load_overrides(&self) -> Result<Overrides> {
        if let Some(p) = self.primary().await {
            match p.load_overrides().await {
                Ok(v) => return Ok(v),
                Err(_) => self.failed().await,
            }
        }
        self.fallback.load_overrides().await
    }
    async fn put_chain_override(&self, id: u64, v: &ChainOverrideState) -> Result<()> {
        self.fallback.put_chain_override(id, v).await?;
        if let Some(p) = self.primary().await
            && p.put_chain_override(id, v).await.is_err()
        {
            self.failed().await
        }
        Ok(())
    }
    async fn delete_chain_override(&self, id: u64) -> Result<()> {
        self.fallback.delete_chain_override(id).await?;
        if let Some(p) = self.primary().await
            && p.delete_chain_override(id).await.is_err()
        {
            self.failed().await
        }
        Ok(())
    }
    async fn put_endpoint_override(&self, k: &str, v: &EndpointOverrideState) -> Result<()> {
        self.fallback.put_endpoint_override(k, v).await?;
        if let Some(p) = self.primary().await
            && p.put_endpoint_override(k, v).await.is_err()
        {
            self.failed().await
        }
        Ok(())
    }
    async fn delete_endpoint_override(&self, k: &str) -> Result<()> {
        self.fallback.delete_endpoint_override(k).await?;
        if let Some(p) = self.primary().await
            && p.delete_endpoint_override(k).await.is_err()
        {
            self.failed().await
        }
        Ok(())
    }
    async fn flush_health(&self, b: &[HealthSnapshot]) -> Result<()> {
        self.fallback.flush_health(b).await?;
        if let Some(p) = self.primary().await
            && p.flush_health(b).await.is_err()
        {
            self.failed().await
        }
        Ok(())
    }
    async fn load_health(&self) -> Result<Vec<HealthSnapshot>> {
        if let Some(p) = self.primary().await {
            match p.load_health().await {
                Ok(v) => return Ok(v),
                Err(_) => self.failed().await,
            }
        }
        self.fallback.load_health().await
    }
    async fn set_hot_chains(&self, c: &[(u64, u64)]) -> Result<()> {
        self.fallback.set_hot_chains(c).await?;
        if let Some(p) = self.primary().await
            && p.set_hot_chains(c).await.is_err()
        {
            self.failed().await
        }
        Ok(())
    }
    async fn append_audit(&self, w: &str, t: &str) -> Result<()> {
        self.fallback.append_audit(w, t).await?;
        if let Some(p) = self.primary().await
            && p.append_audit(w, t).await.is_err()
        {
            self.failed().await
        }
        Ok(())
    }
    async fn export(&self) -> Result<StateExport> {
        if let Some(primary) = self.primary().await {
            return primary.export().await;
        }
        self.fallback.export().await
    }
    async fn import(&self, v: &StateExport) -> Result<()> {
        self.fallback.import(v).await?;
        if let Some(p) = self.primary().await
            && p.import(v).await.is_err()
        {
            self.failed().await
        }
        Ok(())
    }
    async fn reset(&self) -> Result<()> {
        self.fallback.reset().await?;
        if let Some(p) = self.primary().await
            && p.reset().await.is_err()
        {
            self.failed().await
        }
        Ok(())
    }
    async fn health(&self) -> bool {
        match self.primary().await {
            Some(primary) => primary.health().await,
            None => false,
        }
    }
    fn backend_name(&self) -> &'static str {
        "redis"
    }
    fn audit_count(&self) -> u64 {
        self.fallback.audit_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_round_trip_reset_and_call_counter() {
        let store = MemoryStore::new();
        assert_eq!(
            store.bootstrap().await.unwrap().overrides,
            Overrides::default()
        );
        store
            .put_chain_override(
                1,
                &ChainOverrideState {
                    pinned: Some(true),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let export = store.export().await.unwrap();
        store.reset().await.unwrap();
        assert!(store.export().await.unwrap().overrides.chains.is_empty());
        store.import(&export).await.unwrap();
        assert_eq!(
            store
                .load_overrides()
                .await
                .unwrap()
                .chains
                .get(&1)
                .and_then(|x| x.pinned),
            Some(true)
        );
        assert!(store.call_count() >= 5);
    }

    #[tokio::test]
    async fn file_store_is_atomic_and_persistent() {
        let dir = std::env::temp_dir().join(format!("rpcrouter-state-test-{}", now()));
        let path = dir.join("state.json");
        let _ = std::fs::create_dir_all(&dir);
        let store = FileStore::open(&path).await.unwrap();
        store
            .put_endpoint_override(
                "1:x",
                &EndpointOverrideState {
                    url: "http://x".into(),
                    disabled: Some(true),
                    rps: None,
                    concurrency: None,
                },
            )
            .await
            .unwrap();
        let again = FileStore::open(&path).await.unwrap();
        assert_eq!(
            again.load_overrides().await.unwrap().endpoints["1:x"].disabled,
            Some(true)
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn corrupt_or_old_state_file_is_quarantined() {
        let dir = std::env::temp_dir().join(format!("rpcrouter-state-corrupt-{}", now()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let path = dir.join("state.json");
        tokio::fs::write(&path, b"not-json").await.unwrap();
        let store = FileStore::open(&path).await.unwrap();
        assert_eq!(
            store.bootstrap().await.unwrap().schema_version,
            SCHEMA_VERSION
        );
        assert!(!path.exists());
        assert!(std::fs::read_dir(&dir).unwrap().count() >= 1);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    #[ignore]
    async fn redis_round_trip_pipeline_and_namespace_reset() {
        let Ok(url) = std::env::var("REDIS_URL") else {
            eprintln!("skipping Redis test: REDIS_URL is not set");
            return;
        };
        let ns = format!("rpcrouter-test-{}", now());
        let store = RedisStore::connect(&url, &ns).await.unwrap();
        store.reset().await.unwrap();
        let first = store.bootstrap().await.unwrap();
        assert_eq!(first.schema_version, SCHEMA_VERSION);
        store
            .put_chain_override(
                1,
                &ChainOverrideState {
                    pinned: Some(true),
                    disabled: Some(false),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let batch: Vec<_> = (0..2001)
            .map(|i| HealthSnapshot {
                chain_id: 1,
                url: format!("http://endpoint-{i}"),
                state: "probation".into(),
                cooling_until_unix: None,
                strikes: 0,
                latency_ewma_us: i,
                lag: 0,
            })
            .collect();
        store.flush_health(&batch).await.unwrap();
        let exported = store.export().await.unwrap();
        assert_eq!(exported.health.len(), 2001);
        store.reset().await.unwrap();
        assert!(store.bootstrap().await.unwrap().health.is_empty());
        store.import(&exported).await.unwrap();
        assert_eq!(
            store.load_overrides().await.unwrap().chains[&1].pinned,
            Some(true)
        );
        let other = RedisStore::connect(&url, &format!("{ns}-other"))
            .await
            .unwrap();
        other
            .put_chain_override(
                9,
                &ChainOverrideState {
                    disabled: Some(true),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        store.reset().await.unwrap();
        assert_eq!(
            other.load_overrides().await.unwrap().chains[&9].disabled,
            Some(true)
        );
        other.reset().await.unwrap();
    }
}

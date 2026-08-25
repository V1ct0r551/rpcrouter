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
use redis::{AsyncCommands, aio::ConnectionManager, pipe};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, RwLock};

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
            Ok(b) => serde_json::from_slice(&b).context("invalid state file")?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => FileDocument {
                schema_version: SCHEMA_VERSION,
                seeded_at: now(),
                ..Default::default()
            },
            Err(e) => return Err(e.into()),
        };
        if d.schema_version != SCHEMA_VERSION {
            bail!("unsupported state schema version {}", d.schema_version);
        }
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
        tokio::fs::write(&tmp, serde_json::to_vec_pretty(&d)?).await?;
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
}

#[derive(Clone)]
pub struct RedisStore {
    manager: Arc<Mutex<ConnectionManager>>,
    prefix: String,
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
        let manager = ConnectionManager::new(client)
            .await
            .context("failed to connect to Redis")?;
        Ok(Self {
            manager: Arc::new(Mutex::new(manager)),
            prefix: format!("{{{namespace}}}"),
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
    async fn get_doc(&self) -> Result<FileDocument> {
        let mut c = self.manager.lock().await;
        let raw: Option<String> = c.get(self.key("document")).await?;
        Ok(raw
            .map(|x| serde_json::from_str(&x))
            .transpose()?
            .unwrap_or(FileDocument {
                schema_version: SCHEMA_VERSION,
                seeded_at: now(),
                ..Default::default()
            }))
    }
    async fn put_doc(&self, d: &FileDocument) -> Result<()> {
        let mut c = self.manager.lock().await;
        let raw = serde_json::to_string(d)?;
        let _: () = c.set(self.key("document"), raw).await?;
        Ok(())
    }
}

#[async_trait]
impl StateStore for RedisStore {
    async fn bootstrap(&self) -> Result<BootstrapState> {
        self.bump();
        let d = self.get_doc().await?;
        if d.schema_version != SCHEMA_VERSION {
            bail!("unsupported state schema version {}", d.schema_version)
        }
        let mut c = self.manager.lock().await;
        let _: () = pipe()
            .atomic()
            .cmd("HSET")
            .arg(self.key("meta"))
            .arg("schema_version")
            .arg(SCHEMA_VERSION)
            .arg("seeded_at")
            .arg(d.seeded_at)
            .cmd("SET")
            .arg(self.key("document"))
            .arg(serde_json::to_string(&d)?)
            .query_async(&mut *c)
            .await?;
        Ok(BootstrapState {
            schema_version: d.schema_version,
            catalog: d.catalog,
            overrides: d.overrides,
            health: d.health,
            hot_chains: d.hot_chains,
        })
    }
    async fn set_catalog(&self, v: &Value) -> Result<()> {
        self.bump();
        let mut d = self.get_doc().await?;
        d.catalog = Some(v.clone());
        let raw = serde_json::to_string(v)?;
        let mut c = self.manager.lock().await;
        let _: () = pipe()
            .cmd("SET")
            .arg(self.key("catalog"))
            .arg(raw)
            .cmd("SET")
            .arg(self.key("document"))
            .arg(serde_json::to_string(&d)?)
            .query_async(&mut *c)
            .await?;
        Ok(())
    }
    async fn load_overrides(&self) -> Result<Overrides> {
        self.bump();
        Ok(self.get_doc().await?.overrides)
    }
    async fn put_chain_override(&self, id: u64, v: &ChainOverrideState) -> Result<()> {
        self.bump();
        let mut d = self.get_doc().await?;
        d.overrides.chains.insert(id, v.clone());
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
            .cmd("SET")
            .arg(self.key("document"))
            .arg(serde_json::to_string(&d)?)
            .query_async(&mut *c)
            .await?;
        Ok(())
    }
    async fn delete_chain_override(&self, id: u64) -> Result<()> {
        self.bump();
        let mut d = self.get_doc().await?;
        d.overrides.chains.remove(&id);
        self.put_doc(&d).await
    }
    async fn put_endpoint_override(&self, k: &str, v: &EndpointOverrideState) -> Result<()> {
        self.bump();
        let mut d = self.get_doc().await?;
        d.overrides.endpoints.insert(k.to_owned(), v.clone());
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
            .cmd("SET")
            .arg(self.key("document"))
            .arg(serde_json::to_string(&d)?)
            .query_async(&mut *c)
            .await?;
        Ok(())
    }
    async fn delete_endpoint_override(&self, k: &str) -> Result<()> {
        self.bump();
        let mut d = self.get_doc().await?;
        d.overrides.endpoints.remove(k);
        self.put_doc(&d).await
    }
    async fn flush_health(&self, b: &[HealthSnapshot]) -> Result<()> {
        self.bump();
        let mut d = self.get_doc().await?;
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
            .arg(d.last_flush_at)
            .cmd("SET")
            .arg(self.key("document"))
            .arg(serde_json::to_string(&d)?);
        let mut c = self.manager.lock().await;
        let _: () = p.query_async(&mut *c).await?;
        Ok(())
    }
    async fn load_health(&self) -> Result<Vec<HealthSnapshot>> {
        self.bump();
        Ok(self.get_doc().await?.health)
    }
    async fn set_hot_chains(&self, c: &[(u64, u64)]) -> Result<()> {
        self.bump();
        let mut d = self.get_doc().await?;
        d.hot_chains = c.to_vec();
        let mut p = pipe();
        p.cmd("DEL").arg(self.key("chains:hot"));
        for (id, score) in c {
            p.cmd("ZADD")
                .arg(self.key("chains:hot"))
                .arg(*score)
                .arg(*id);
        }
        p.cmd("SET")
            .arg(self.key("document"))
            .arg(serde_json::to_string(&d)?);
        let mut conn = self.manager.lock().await;
        let _: () = p.query_async(&mut *conn).await?;
        Ok(())
    }
    async fn append_audit(&self, w: &str, t: &str) -> Result<()> {
        self.bump();
        let mut d = self.get_doc().await?;
        d.audit.push(AuditEntry {
            at: now(),
            what: w.to_owned(),
            target: t.to_owned(),
        });
        let excess = d.audit.len().saturating_sub(10_000);
        if excess > 0 {
            d.audit.drain(..excess);
        }
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
            .cmd("SET")
            .arg(self.key("document"))
            .arg(serde_json::to_string(&d)?)
            .query_async(&mut *c)
            .await?;
        Ok(())
    }
    async fn export(&self) -> Result<StateExport> {
        self.bump();
        let d = self.get_doc().await?;
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
        let d = FileDocument {
            schema_version: v.schema_version,
            catalog: v.catalog.clone(),
            overrides: v.overrides.clone(),
            health: v.health.clone(),
            hot_chains: v.hot_chains.clone(),
            seeded_at: now(),
            ..Default::default()
        };
        let mut c = self.manager.lock().await;
        let _: () = pipe()
            .atomic()
            .cmd("DEL")
            .arg(self.key("document"))
            .arg(self.key("override:index"))
            .arg(self.key("health:index"))
            .arg(self.key("chains:hot"))
            .cmd("SET")
            .arg(self.key("document"))
            .arg(serde_json::to_string(&d)?)
            .cmd("HSET")
            .arg(self.key("meta"))
            .arg("schema_version")
            .arg(SCHEMA_VERSION)
            .arg("seeded_at")
            .arg(d.seeded_at)
            .query_async(&mut *c)
            .await?;
        Ok(())
    }
    async fn reset(&self) -> Result<()> {
        self.bump();
        let mut c = self.manager.lock().await;
        let overrides: Vec<String> = c
            .smembers(self.key("override:index"))
            .await
            .unwrap_or_default();
        let health: Vec<String> = c
            .smembers(self.key("health:index"))
            .await
            .unwrap_or_default();
        let mut p = pipe();
        p.atomic()
            .cmd("DEL")
            .arg(self.key("document"))
            .arg(self.key("meta"))
            .arg(self.key("catalog"))
            .arg(self.key("catalog:etag"))
            .arg(self.key("override:index"))
            .arg(self.key("health:index"))
            .arg(self.key("chains:hot"))
            .arg(self.key("audit"));
        for key in overrides.into_iter().chain(health) {
            p.arg(key);
        }
        let _: () = p.query_async(&mut *c).await?;
        Ok(())
    }
    async fn health(&self) -> bool {
        self.get_doc().await.is_ok()
    }
    fn call_count(&self) -> u64 {
        self.calls.load(Ordering::Relaxed)
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
        let primary = RedisStore::connect_with_ttl(url, namespace, ttl)
            .await
            .ok()
            .map(Arc::new);
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
        let snapshot = self.fallback.export().await?;
        redis.import(&snapshot).await?;
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
        self.primary()
            .await
            .is_some_and(|p| p.health_ttl_seconds > 0)
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

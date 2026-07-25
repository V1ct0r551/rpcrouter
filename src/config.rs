use std::{
    collections::HashSet,
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

pub const MAX_BATCH_SIZE: usize = 100;

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    pub listen: SocketAddr,
    pub metrics_enabled: bool,
    pub chains: Vec<u64>,
    pub server: ServerConfig,
    pub chainlist: ChainlistConfig,
    pub upstream: UpstreamConfig,
    pub probe: ProbeConfig,
    pub cache: CacheConfig,
    pub hedging: HedgingConfig,
    pub chain_overrides: Vec<ChainOverride>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: "0.0.0.0:8545"
                .parse()
                .expect("valid default listen address"),
            metrics_enabled: true,
            chains: vec![1, 143],
            server: ServerConfig::default(),
            chainlist: ChainlistConfig::default(),
            upstream: UpstreamConfig::default(),
            probe: ProbeConfig::default(),
            cache: CacheConfig::default(),
            hedging: HedgingConfig::default(),
            chain_overrides: Vec::new(),
        }
    }
}

impl Config {
    /// 配置文件不存在时使用内置默认值，保证零配置启动。
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        match fs::read_to_string(path) {
            Ok(contents) => Self::from_toml(&contents)
                .with_context(|| format!("failed to parse config {}", path.display())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(error) => {
                Err(error).with_context(|| format!("failed to read config {}", path.display()))
            }
        }
    }

    pub fn from_toml(contents: &str) -> Result<Self> {
        let config: Self = toml::from_str(contents).context("invalid TOML")?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.chains.is_empty() {
            bail!("chains must not be empty");
        }
        let unique_chains: HashSet<_> = self.chains.iter().copied().collect();
        if unique_chains.len() != self.chains.len() {
            bail!("chains must not contain duplicates");
        }
        if self.server.batch_limit == 0 || self.server.batch_limit > MAX_BATCH_SIZE {
            bail!("server.batch_limit must be between 1 and {MAX_BATCH_SIZE}");
        }
        if self.chainlist.refresh_seconds == 0 {
            bail!("chainlist.refresh_seconds must be greater than zero");
        }
        if self.upstream.request_timeout_ms == 0 || self.upstream.deadline_ms == 0 {
            bail!("upstream timeouts must be greater than zero");
        }
        if self.upstream.slow_threshold_ms == 0 {
            bail!("upstream.slow_threshold_ms must be greater than zero");
        }
        if self.upstream.max_attempts == 0 || self.upstream.max_attempts > 4 {
            bail!("upstream.max_attempts must be between 1 and 4");
        }
        if self.upstream.default_rps == 0 || self.upstream.default_concurrency == 0 {
            bail!("upstream rate and concurrency limits must be greater than zero");
        }
        if self.probe.min_interval_seconds == 0
            || self.probe.max_interval_seconds < self.probe.min_interval_seconds
        {
            bail!("probe interval must be a valid nonzero range");
        }
        if self.probe.max_concurrency == 0 || self.probe.request_timeout_ms == 0 {
            bail!("probe concurrency and timeout must be greater than zero");
        }
        if self.cache.max_bytes == 0 || self.cache.immutable_ttl_seconds < 60 * 60 {
            bail!("cache capacity must be nonzero and immutable TTL must be at least one hour");
        }
        if self.hedging.delay_ms == 0
            || self.hedging.max_percent == 0
            || self.hedging.max_percent > 10
            || self.hedging.min_active_endpoints < 2
        {
            bail!("hedging requires a delay, max_percent 1..=10, and at least two endpoints");
        }
        for chain in &self.chain_overrides {
            if !unique_chains.contains(&chain.chain_id) {
                bail!(
                    "chain override {} is not present in the chains allowlist",
                    chain.chain_id
                );
            }
            for endpoint in &chain.endpoint_overrides {
                if endpoint.rps == Some(0) || endpoint.concurrency == Some(0) {
                    bail!("endpoint limits must be greater than zero");
                }
            }
        }
        Ok(())
    }

    pub fn chain_override(&self, chain_id: u64) -> Option<&ChainOverride> {
        self.chain_overrides
            .iter()
            .find(|chain| chain.chain_id == chain_id)
    }

    pub fn endpoint_limits(&self, chain_id: u64, url: &str) -> (u32, usize) {
        let configured = self
            .chain_override(chain_id)
            .and_then(|chain| chain.endpoint_overrides.iter().find(|item| item.url == url));
        (
            configured
                .and_then(|endpoint| endpoint.rps)
                .unwrap_or(self.upstream.default_rps),
            configured
                .and_then(|endpoint| endpoint.concurrency)
                .unwrap_or(self.upstream.default_concurrency),
        )
    }

    pub fn lag_threshold(&self, chain_id: u64) -> u64 {
        self.chain_override(chain_id)
            .and_then(|chain| chain.max_block_lag)
            .unwrap_or(self.probe.max_block_lag)
    }

    pub fn confirmation_depth(&self, chain_id: u64) -> u64 {
        self.chain_override(chain_id)
            .and_then(|chain| chain.confirmation_depth)
            .unwrap_or(64)
    }

    pub fn block_time_ms(&self, chain_id: u64) -> u64 {
        self.chain_override(chain_id)
            .and_then(|chain| chain.block_time_ms)
            .unwrap_or(match chain_id {
                1 => 12_000,
                143 => 400,
                _ => 2_000,
            })
    }

    pub fn tip_ttl_ms(&self, chain_id: u64) -> u64 {
        self.chain_override(chain_id)
            .and_then(|chain| chain.tip_ttl_ms)
            .unwrap_or_else(|| self.block_time_ms(chain_id).min(2_000))
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub batch_limit: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            batch_limit: MAX_BATCH_SIZE,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ChainlistConfig {
    pub url: String,
    pub refresh_seconds: u64,
    pub stale_grace_seconds: u64,
    pub cache_path: PathBuf,
}

impl Default for ChainlistConfig {
    fn default() -> Self {
        Self {
            url: "https://chainlist.org/rpcs.json".to_owned(),
            refresh_seconds: 6 * 60 * 60,
            stale_grace_seconds: 24 * 60 * 60,
            cache_path: PathBuf::from("./data/rpcs.json"),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct UpstreamConfig {
    pub request_timeout_ms: u64,
    pub slow_threshold_ms: u64,
    pub deadline_ms: u64,
    pub max_attempts: usize,
    pub default_rps: u32,
    pub default_concurrency: usize,
}

impl Default for UpstreamConfig {
    fn default() -> Self {
        Self {
            request_timeout_ms: 5_000,
            slow_threshold_ms: 4_000,
            deadline_ms: 15_000,
            max_attempts: 4,
            default_rps: 15,
            default_concurrency: 8,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ChainOverride {
    pub chain_id: u64,
    pub block_time_ms: Option<u64>,
    pub confirmation_depth: Option<u64>,
    pub tip_ttl_ms: Option<u64>,
    pub max_block_lag: Option<u64>,
    pub extra_endpoints: Vec<String>,
    pub disabled_endpoints: Vec<String>,
    pub endpoint_overrides: Vec<EndpointOverride>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct EndpointOverride {
    pub url: String,
    pub rps: Option<u32>,
    pub concurrency: Option<usize>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ProbeConfig {
    pub min_interval_seconds: u64,
    pub max_interval_seconds: u64,
    pub max_concurrency: usize,
    pub request_timeout_ms: u64,
    pub max_block_lag: u64,
}

impl Default for ProbeConfig {
    fn default() -> Self {
        Self {
            min_interval_seconds: 15,
            max_interval_seconds: 30,
            max_concurrency: 32,
            request_timeout_ms: 5_000,
            max_block_lag: 5,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct CacheConfig {
    pub max_bytes: u64,
    pub immutable_ttl_seconds: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_bytes: 512 * 1024 * 1024,
            immutable_ttl_seconds: 60 * 60,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct HedgingConfig {
    pub enabled: bool,
    pub delay_ms: u64,
    pub max_percent: u32,
    pub min_active_endpoints: usize,
}

impl Default for HedgingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            delay_ms: 300,
            max_percent: 10,
            min_active_endpoints: 2,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_file_uses_safe_defaults() {
        let config = Config::load("this-file-must-not-exist.toml").expect("load defaults");
        assert_eq!(config.chains, [1, 143]);
        assert_eq!(config.upstream.default_rps, 15);
        assert_eq!(config.upstream.default_concurrency, 8);
        assert_eq!(config.upstream.max_attempts, 4);
    }

    #[test]
    fn rejects_unsafe_phase_one_limits() {
        let error = Config::from_toml(
            r#"
                chains = [1]
                [server]
                batch_limit = 101
            "#,
        )
        .expect_err("batch limit should fail");
        assert!(error.to_string().contains("batch_limit"));
    }

    #[test]
    fn endpoint_limit_override_wins() {
        let config = Config::from_toml(
            r#"
                chains = [1]
                [[chain_overrides]]
                chain_id = 1
                [[chain_overrides.endpoint_overrides]]
                url = "https://rpc.example"
                rps = 7
                concurrency = 3
            "#,
        )
        .expect("parse config");
        assert_eq!(config.endpoint_limits(1, "https://rpc.example"), (7, 3));
        assert_eq!(config.endpoint_limits(1, "https://other.example"), (15, 8));
    }

    #[test]
    fn repository_config_is_valid() {
        let config = Config::from_toml(include_str!("../config.toml")).expect("repository config");
        assert_eq!(config.chains, [1, 143]);
    }

    #[test]
    fn partial_config_can_select_one_chain() {
        let config = Config::from_toml("chains = [1]").expect("single-chain config");
        assert_eq!(config.chains, [1]);
        assert!(config.chain_overrides.is_empty());
    }
}

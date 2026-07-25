use std::{env, path::PathBuf};

use anyhow::Result;
use rpcrouter::config::Config;
use tracing::info;
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("rpcrouter=info")),
        )
        .init();

    let config_path = env::var_os("RPCROUTER_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config.toml"));
    let config = Config::load(&config_path)?;
    info!(path = %config_path.display(), listen = %config.listen, "configuration loaded");
    Ok(())
}

use std::{env, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Result;
use rpcrouter::{
    chainlist::ChainlistLoader,
    config::Config,
    forward::Forwarder,
    probe::{ProbeManager, spawn as spawn_probes},
    registry::Registry,
    server::{AppState, router},
};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("rpcrouter=info")),
        )
        .init();

    let config_path = env::var_os("RPCROUTER_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config.toml"));
    let mut config = Config::load(&config_path)?;
    config.apply_env_overrides()?;
    info!(path = %config_path.display(), listen = %config.listen, "configuration loaded");

    let chainlist = Arc::new(ChainlistLoader::new(&config)?);
    let registry = Arc::new(Registry::new(&config));
    let initial = chainlist.load().await?;
    info!(source = ?initial.source, chains = initial.snapshot.chains.len(), "chainlist loaded");
    registry.apply_snapshot(&initial.snapshot).await;

    spawn_chainlist_refresh(
        Arc::clone(&chainlist),
        Arc::clone(&registry),
        Duration::from_secs(config.chainlist.refresh_seconds),
    );
    let probes = Arc::new(ProbeManager::new(Arc::clone(&registry), &config)?);
    spawn_probes(probes);

    let forwarder = Arc::new(Forwarder::new(Arc::clone(&registry), &config)?);
    let app = router(
        AppState::new(registry, forwarder, config.server.batch_limit)
            .with_metrics_enabled(config.metrics_enabled),
    );
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    info!(listen = %config.listen, "rpcrouter listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

fn spawn_chainlist_refresh(
    chainlist: Arc<ChainlistLoader>,
    registry: Arc<Registry>,
    refresh_interval: Duration,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(refresh_interval);
        interval.tick().await;
        loop {
            interval.tick().await;
            match chainlist.load().await {
                Ok(result) => {
                    registry.apply_snapshot(&result.snapshot).await;
                    info!(source = ?result.source, "chainlist refresh completed");
                }
                Err(error) => error!(error = %error, "chainlist refresh exhausted all fallbacks"),
            }
        }
    });
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        error!(error = %error, "failed to install shutdown signal handler");
    }
    info!("shutdown signal received");
}

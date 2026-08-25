use std::{env, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Result;
use axum::ServiceExt;
use rpcrouter::{
    chainlist::ChainlistLoader,
    config::Config,
    forward::Forwarder,
    probe::{ProbeManager, spawn as spawn_probes},
    registry::Registry,
    server::{AppState, guarded_service_from_state},
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
    info!(source = ?initial.source, chains = initial.catalog.chains.len(), "chainlist loaded");
    registry.set_catalog(initial.catalog).await;
    registry.apply_snapshot(&initial.snapshot).await;

    spawn_chainlist_refresh(
        Arc::clone(&chainlist),
        Arc::clone(&registry),
        Duration::from_secs(config.chainlist.refresh_seconds),
    );
    let probes = Arc::new(ProbeManager::new(Arc::clone(&registry), &config)?);
    spawn_probes(probes);

    // 启动 housekeeping 后台任务（每 30s 一次）。
    spawn_housekeeping(Arc::clone(&registry));

    let forwarder = Arc::new(Forwarder::new(Arc::clone(&registry), &config)?);
    let per_ip = if config.server.per_ip_rate_limit.enabled {
        Some((
            config.server.per_ip_rate_limit.requests_per_second,
            config.server.per_ip_rate_limit.burst,
        ))
    } else {
        None
    };
    let app = guarded_service_from_state(
        AppState::new(registry, forwarder, config.server.batch_limit)
            .with_metrics_enabled(config.metrics_enabled)
            .with_hardening(
                config.server.max_body_bytes,
                config.server.max_concurrent_requests,
                per_ip,
                config.server.metrics_auth_token.clone(),
            ),
    );

    // 用 connect-info 注入客户端地址，供每 IP 限速层读取。
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    info!(listen = %config.listen, "rpcrouter listening");

    // 优雅退出：收到 SIGTERM/SIGINT 后停收新请求并排空在飞请求；
    // 排空超过 shutdown_deadline_ms 则强制退出。
    let drain_deadline = Duration::from_millis(config.server.shutdown_deadline_ms);
    let serve = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal());

    tokio::select! {
        result = serve => result?,
        () = drain_after_signal(shutdown_signal(), drain_deadline) => {
            // 排空超时：axum 的连接是独立 tokio::spawn 任务，#[tokio::main] 在 main 返回时
            // drop runtime 会无限等待这些任务（慢客户端/挂起连接会让进程退不出去），
            // 且无法与正常退出（码 0）区分。故这里立即用非零退出码硬退出。
            error!(deadline_ms = %config.server.shutdown_deadline_ms,
                "shutdown drain deadline exceeded; forcing process exit");
            std::process::exit(forced_shutdown_exit_code());
        }
    }
    Ok(())
}

/// 强制退出（排空超时）使用的进程退出码，非零以便与正常退出（0）区分。
fn forced_shutdown_exit_code() -> i32 {
    1
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
                    registry.set_catalog(result.catalog).await;
                    registry.apply_snapshot(&result.snapshot).await;
                    info!(source = ?result.source, "chainlist refresh completed");
                }
                Err(error) => error!(error = %error, "chainlist refresh exhausted all fallbacks"),
            }
        }
    });
}

/// 后台 housekeeping：每 30 秒执行一次 idle 降级 + LRU 淘汰。
fn spawn_housekeeping(registry: Arc<Registry>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        interval.tick().await; // 跳过第一次立即触发。
        loop {
            interval.tick().await;
            registry.housekeeping().await;
        }
    });
}

/// 等待 SIGTERM 或 SIGINT（Ctrl-C）。任一信号到达即返回。
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");
        tokio::select! {
            _ = sigterm.recv() => info!("SIGTERM received"),
            _ = sigint.recv() => info!("SIGINT received"),
        }
    }
    #[cfg(not(unix))]
    {
        if let Err(error) = tokio::signal::ctrl_c().await {
            error!(error = %error, "failed to install shutdown signal handler");
        }
    }
    info!("shutdown signal received");
}

/// 收到退出信号后启动排空 deadline 计时：信号一到即开始倒计时，
/// 到期返回，触发主循环强制退出。
///
/// `signal` 抽象成参数以便逻辑层测试：生产传 `shutdown_signal()`，测试传已触发的 future。
async fn drain_after_signal(
    signal: impl std::future::Future<Output = ()> + Send,
    deadline: Duration,
) {
    signal.await;
    info!(deadline = ?deadline, "graceful shutdown started; draining in-flight requests");
    tokio::time::sleep(deadline).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forced_shutdown_uses_nonzero_exit_code() {
        // 强退必须非零，正常退出（Ok(())）隐式为 0，二者据此区分（供进程管理器/脚本判断）。
        assert_ne!(forced_shutdown_exit_code(), 0);
    }

    #[test]
    fn drain_after_signal_waits_for_deadline() {
        // 逻辑层验证 drain_after_signal：信号一触发即开始倒计时，至少等到 deadline 才返回。
        let start = tokio::time::Instant::now();
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("runtime")
            .block_on(async {
                // 传一个已触发的信号 future（async {} 立即完成），模拟 SIGTERM 已到达。
                drain_after_signal(async {}, Duration::from_millis(80)).await;
            });
        assert!(
            start.elapsed() >= Duration::from_millis(80),
            "drain deadline window must be respected"
        );
    }
}

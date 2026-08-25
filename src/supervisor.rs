//! 后台任务监督器：任务 panic/意外退出时记录日志并指数退避重启。
use futures_util::FutureExt;
use std::{future::Future, sync::Arc, time::Duration};
use tokio::time::sleep;
use tracing::{error, warn};

use crate::metrics::Metrics;

pub fn spawn<F, Fut>(name: &'static str, metrics: Arc<Metrics>, factory: F)
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        let mut backoff = Duration::from_millis(100);
        loop {
            let started = tokio::time::Instant::now();
            let result = std::panic::AssertUnwindSafe(factory()).catch_unwind().await;
            match result {
                Ok(()) => error!(task = name, "background task exited unexpectedly"),
                Err(_) => error!(task = name, "background task panicked"),
            }
            metrics.record_background_task_restart(name);
            if started.elapsed() >= Duration::from_secs(5 * 60) {
                backoff = Duration::from_millis(100);
            }
            warn!(
                task = name,
                backoff_ms = backoff.as_millis(),
                "restarting background task"
            );
            sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(30));
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::time::{Duration, sleep};

    #[tokio::test]
    async fn panic_task_is_restarted_and_counted() {
        let metrics = Arc::new(Metrics::new().expect("metrics"));
        let runs = Arc::new(AtomicUsize::new(0));
        let runs2 = Arc::clone(&runs);
        spawn("panic-test", Arc::clone(&metrics), move || {
            let runs = Arc::clone(&runs2);
            async move {
                if runs.fetch_add(1, Ordering::SeqCst) == 0 {
                    panic!("injected")
                }
            }
        });
        sleep(Duration::from_millis(350)).await;
        assert!(runs.load(Ordering::SeqCst) >= 2);
        assert!(metrics.background_task_restarts("panic-test") >= 1);
    }
}

//! 优雅退出（P2.1）进程级测试。
//!
//! 说明取舍：直接 spawn 编译出的二进制做进程级验证最接近真实行为，但脆弱——
//! 依赖 `kill` 命令、端口可用性、后台任务时序，且需要离线启动（chainlist 走本地
//! cache，无外网端点，避免测试触网）。因此这两个用例默认 `#[ignore]`，在 CI/本机
//! 明确需要时单独跑：
//!   cargo test --test graceful_shutdown -- --ignored
//!
//! 逻辑层的 deadline/退出码决策另有 main.rs 内的纯函数单测兜底。

use std::{
    fs,
    io::Write,
    net::{TcpListener as StdTcpListener, TcpStream},
    path::Path,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

/// 获取一个空闲端口（bind 后立即 drop）。
fn free_port() -> u16 {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind free port");
    listener.local_addr().expect("local addr").port()
}

/// 写一个最小 chainlist 快照（chain 1 无外网端点），避免测试触网。
fn write_snapshot(path: &Path) {
    let body = r#"[{"name":"Graceful Test","chain":"TST","rpc":[],"chainId":1,"networkId":1}]"#;
    fs::write(path, body).expect("write snapshot");
}

/// 写一个最小 config：cache 指向本地快照，listen 用空闲端口，关闭每 IP 限速。
fn write_config(path: &Path, listen_port: u16, cache_path: &Path, deadline_ms: u64) {
    let content = format!(
        r#"
listen = "127.0.0.1:{listen_port}"
metrics_enabled = true
chains = [1]

[server]
batch_limit = 10
shutdown_deadline_ms = {deadline_ms}

[chainlist]
refresh_seconds = 3600
cache_path = "{cache_path}"
"#,
        cache_path = cache_path.display(),
    );
    fs::write(path, content).expect("write config");
}

/// 启动二进制并等待其开始监听端口；返回子进程。
fn start_binary(dir: &Path, listen_port: u16, deadline_ms: u64) -> Child {
    let cache_path = dir.join("rpcs.json");
    let config_path = dir.join("config.toml");
    write_snapshot(&cache_path);
    write_config(&config_path, listen_port, &cache_path, deadline_ms);

    let mut child = Command::new(env!("CARGO_BIN_EXE_rpcrouter"))
        .env("RPCROUTER_CONFIG", &config_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn binary");

    // 轮询端口直到可连接（最多 10s）。
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut ready = false;
    while Instant::now() < deadline {
        if TcpStream::connect(("127.0.0.1", listen_port)).is_ok() {
            ready = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    if !ready {
        let _ = child.kill();
        panic!("binary did not start listening in time");
    }
    child
}

fn send_sigterm(pid: u32) {
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .expect("run kill");
    assert!(status.success(), "kill -TERM failed");
}

fn wait_exit(mut child: Child, timeout: Duration) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            return Some(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            return None;
        }
        thread::sleep(Duration::from_millis(50));
    }
}

/// (a) 在飞请求在 deadline 内排空 → SIGTERM 后正常退出，退出码 0。
#[ignore]
#[test]
fn sigterm_drains_and_exits_zero() {
    let dir = std::env::temp_dir().join(format!("rpcrouter-graceful-a-{}", std::process::id()));
    fs::create_dir_all(&dir).expect("create temp dir");
    let port = free_port();
    let child = start_binary(&dir, port, 10_000);
    let pid = child.id();
    thread::sleep(Duration::from_millis(200));
    send_sigterm(pid);
    let status =
        wait_exit(child, Duration::from_secs(15)).expect("process should exit after SIGTERM");
    assert!(status.success(), "expected exit code 0, got {status:?}");
    let _ = fs::remove_dir_all(&dir);
}

/// (b) 挂起连接导致排空超时 → deadline 后进程必须强退，且退出码非零。
#[ignore]
#[test]
fn hung_connection_forces_nonzero_exit() {
    let dir = std::env::temp_dir().join(format!("rpcrouter-graceful-b-{}", std::process::id()));
    fs::create_dir_all(&dir).expect("create temp dir");
    let port = free_port();
    // 极短 deadline（200ms），且先建一个不关闭的 TCP 连接再发 SIGTERM，制造挂起。
    let child = start_binary(&dir, port, 200);
    let pid = child.id();

    // 打开一条长连接并保持不关闭（模拟慢客户端）。
    let mut conn = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    // 发送一个不完整 HTTP 请求头，让服务端一直等待。
    let _ =
        conn.write_all(b"POST /rpc/1 HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\npartial");

    thread::sleep(Duration::from_millis(300));
    send_sigterm(pid);

    // 进程应在 deadline（200ms）附近强退，退出码非零。给它 5s 余量。
    let status = wait_exit(child, Duration::from_secs(5))
        .expect("process should force-exit after drain deadline");
    assert!(
        !status.success(),
        "expected non-zero exit code, got {status:?}"
    );
    let _ = fs::remove_dir_all(&dir);
}

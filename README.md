# rpcrouter

rpcrouter 是面向 EVM 链的 JSON-RPC 路由网关。它从 Chainlist 汇集无需 API key 的公开 RPC，
在每条链内按健康度选点，并把上游限频、超时和故障通过冷却、换点重试与只读请求 hedging
对调用方隐藏。响应缓存和 in-flight 折叠承接高重复读流量，使公开节点池只处理少量未命中；
所有端点同时不可用时，网关才返回 `-32000`。

仓库随附的 `config.toml` 启用 Ethereum、Monad、BSC、Polygon、Arbitrum One、Base、
OP Mainnet 和 Avalanche C-Chain。项目仅代理 HTTP JSON-RPC，不提供 WebSocket、鉴权或计费。

## 工作原理

请求按 `chainId` 进入独立端点池。缓存把确定老块、链 tip 和不可缓存方法分开处理；相同缓存键
的并发 miss 只由一个 leader 请求上游。Active 端点使用 P2C 评分选点，评分综合延迟 EWMA 与
块高滞后。429、quota、HTML、5xx、超时等端点故障会触发指数冷却，冷却到期后须连续通过两次
探针才能回池；请求参数错误、`execution reverted` 等链级错误则原样透传。

每个端点默认受 15 rps 令牌桶和 8 个并发位保护。探针也计入同一额度，网关不会通过伪装或
激进重试规避公共服务条款。

## 快速开始

需要 Rust 1.97 或更高版本。仓库内配置可以直接启动：

```sh
cargo run --release --bin rpcrouter
```

默认监听 `0.0.0.0:8545`。启动后，端点先处于 Probation；完成健康探针后才承接流量。

```sh
curl -sS http://127.0.0.1:8545/rpc/1 \
  -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}'
```

用 `RPCROUTER_CONFIG=/path/to/config.toml` 指定其他配置文件；`RUST_LOG` 控制日志级别。

## 配置

根级 `listen`、`metrics_enabled` 和 `chains` 分别控制监听地址、指标端点和链允许清单。
主要配置段如下：

| 配置段 | 用途 |
|---|---|
| `server` | JSON-RPC batch 上限（最大 100） |
| `chainlist` | 数据源、6 小时刷新、陈旧宽限和磁盘缓存路径 |
| `upstream` | 单次/总超时、重试次数、默认端点 rps 与并发限制 |
| `probe` | 15–30 秒探针抖动、全局并发和允许块高滞后 |
| `cache` | 按响应字节加权的容量（默认 512 MiB）和不可变 TTL |
| `hedging` | 只读请求第二发延迟、全局占比和健康池门槛 |
| `chain_overrides` | 每链块时间、确认深度 K、tip TTL、附加/屏蔽端点及端点限额 |

仓库配置使用偏保守的缓存确认深度。BSC 按 Maxwell 升级后的约 750ms 出块配置；Polygon
约 2s、Arbitrum 约 250ms，Base、OP 与 Avalanche 约 2s。tip TTL 不超过对应块时间和 2s。
附加公开端点前应先确认其服务条款，并通过 `endpoint_overrides` 下调供应商声明的额度。

## HTTP 接口

- `POST /rpc/{chainId}`：接收 JSON-RPC 2.0 单请求或 batch；batch 拆分并发执行后按输入保序。
- `GET /chains`：返回各链端点总数、Active/Cooling/Probation 数量及跟踪到的 head。
- `GET /healthz`：进程存活时返回 `{"status":"ok"}`；不代表任一上游当前可用。
- `GET /metrics`：Prometheus 文本指标；`metrics_enabled = false` 时返回 404。

`/metrics` 包含按链的入口、缓存命中/折叠、上游、用户可见错误、延迟、failover 和 hedge
指标，以及按端点的请求、429、冷却事件与状态。JSON-RPC 上游耗尽仍使用 HTTP 200，响应体为
code `-32000` 的标准 JSON-RPC error。

## 性能验证

Phase 3 本机离线压测以 10,000 QPS 调度 60 秒，共 600,000 次请求：实际
9,999.836 QPS，hit + fold 为 99.9948%，p99 4.126ms，用户可见错误为 0，且 mock 端点
峰值 3 QPS，未超过 15 rps 上限。环境、方法和 429 摘除/回池时间线见
[Phase 3 压测报告](docs/reports/loadtest-phase3.md)。

## 部署注意事项

- 使用 `cargo build --release --bin rpcrouter`，以受限的非 root 账户运行，并将配置和
  `data/rpcs.json` 所在目录持久化；滚动前先通过 `/healthz` 和 `/chains` 检查实例状态。
- 高并发部署应提高文件描述符上限，例如 shell 的 `ulimit -n 65535` 或 systemd 的
  `LimitNOFILE=65535`，同时确认反向代理的连接池、请求体大小和超时不少于网关 deadline。
- 只在可信网络暴露管理接口；如需公网服务，应在前置代理实现 TLS、访问控制和入口限流。
- release 构建不会改变对外保护：诚实发送 `rpcrouter/<version>` User-Agent，默认严格限制
  单端点 15 rps/8 并发，429 时退避换点。运营者应尊重每个公共 RPC 的条款，不把多端点聚合
  当作绕过单一供应商配额的手段。

# 生产就绪验收报告（ROADMAP P1 + P2）

> 2026-08-20。四条工作流（部署 / 入口加固 / 监控运维 / CI）经 maker 实现 + checker
> 单轮对抗审查 + must-fix 修复后合入 main，并做了合并后的真实环境集成验收。
> 本报告只记录**实测结果**；实现细节见各提交与 docs/OPERATIONS.md。

## 交付范围

| 工作流 | 内容 | 分支（已合入） |
|---|---|---|
| 部署 P1 | Dockerfile / RPCROUTER_* env 覆写 / docker-compose / systemd / release profile / README | w1-deploy |
| 加固 P2.1/2.2/2.6 | 优雅退出 / 413 体限 / 503 背压 / 每 IP 限速开关 / metrics bearer 鉴权 | w2-hardening |
| 运维 P2.3 | Prometheus 告警 5 条 / Grafana 仪表盘 + provisioning / soak.sh / OPERATIONS.md | w3-ops |
| CI P2.4 | GitHub Actions（三门槛 + ignored 用例 + release 构建 + 1k QPS 离线冒烟） | w4-ci |

对抗审查共产出 5 条 must-fix，全部修复后合入：deadline 强退不生效（runtime Drop
阻塞）、per-IP 限速内存无界、compose 未挂载 alerts.yml（Prometheus 起不来）、
CI 缺 job 超时、CI toolchain 缺 rustfmt/clippy 组件。

## 真实环境验收结果（合并后 main，镜像 rpcrouter:rc1）

### 1. Docker 全链路（真实公开节点）

- `docker build` 通过（多阶段，content 约 36MB，非 root，HEALTHCHECK）。
- `docker run` 单命令起服务，8 链 `eth_blockNumber` 全部有效返回：
  ETH(1)、BSC(56)、Polygon(137)、Arbitrum(42161)、Base(8453)、OP(10)、
  Avalanche(43114)、Monad(143)。
- 交叉验证：链 1 / 8453 的 `eth_chainId`（0x1 / 0x2105）与 `eth_getBalance` 均有效。

### 2. 加固特性实测（容器内）

- >256KB 请求体 → **HTTP 413 + JSON-RPC 错误体**；
  `rpcrouter_ingress_rejected_total{reason="body_too_large"}` 计数 +1。
- `docker stop`（SIGTERM）→ **0.26s 优雅退出，退出码 0**（deadline 上限 10s）。
- 进程级用例（SIGTERM 排空→0；挂起连接超 deadline→强退非零码）在 CI 以
  `cargo test -- --ignored` 常跑。

### 3. 监控栈实跑（compose monitoring profile）

- 网关 + Prometheus + Grafana 三容器 up；Prometheus target `rpcrouter` **up**；
  **5 条告警规则全部加载**（inactive）；Grafana `/api/health` ok。

### 4. CI 首跑（GitHub Actions, run 32312038589）

- fmt / clippy(-D warnings) / test / ignored 用例 / release 构建 / 离线冒烟全绿，3m55s。
- 冒烟数字：achieved_qps ≈ 1000，p99 0.16ms，user_visible_errors=0，failed=0
 （宽松阈值 p99≤200ms + 吞吐下限 ≥ requested/2，防静默放行）。

### 5. 真实网络 soak（30 分钟 × 5 QPS × 8 链，release 裸进程）

- 4561 请求 **0 错误**，逐链均匀（各 570±1），`user_visible_errors` 增量 **0**，
  入口拒绝 0。
- 期间上游真实发生 **301 次 429、1016 次冷却摘除事件**（多为后台探测触发），
  客户端零感知——限频摘除 + 透明转移机制在真实网络下被验证。
- 网关 RSS 31 分钟后 46.5MB，无增长迹象。缓存命中率 11.3%（5 QPS 低负载下符合预期，
  10k QPS 压测下为 98%+，见 loadtest-phase3.md）。
- 注意：本次 soak 的 rss.csv 曲线误采了包装 shell 的 PID（`--pid` 传错），曲线无效，
  上述 RSS 为验收时对真实进程的直接采样；24h 长跑时请把 `--pid` 指向 rpcrouter 进程。

## 已知遗留（不阻塞生产，按需跟进）

- **P2.5 的 24h soak 未跑**（本次 30 分钟）；`scripts/soak.sh --duration 86400` 已就绪。
- per-IP 限速读的是直连 IP，未处理 X-Forwarded-For——反代部署时开启无效（默认 off）。
- /healthz、/metrics 仍在全局并发防护圈内，监控流量风暴理论上可挤占 RPC 配额。
- Dockerfile 依赖层与源码层未分离，源码改动会重编全部依赖（构建慢，不影响运行）。
- CI actions 未 SHA 固定；loadtest 客户端无请求超时（依赖 job 级 30 分钟超时兜底）。
- 冒烟裁决刻意忽略 loadtest 内置的命中率≥98% 验收（低 QPS 冒烟不适用），缓存效率
  回归需靠手动 10k 压测把关。

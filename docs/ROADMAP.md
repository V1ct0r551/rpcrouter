# ROADMAP — v1 之后的任务规划

> 2026-07-26 收官时制定，2026-08-25 增补 P3（动态全链 + Dashboard），原 P3/P4 顺延为 P4/P5。
> v1 已交付（三硬指标达标，见 docs/reports/loadtest-phase3.md）。
> 以下按优先级排列，未排期；每项开工前照旧流程：主会话细化验收 → Codex 实现 → 主会话评审。

## P1 部署（让它跑在服务器上）

> **✅ P1 全部完成**（2026-08-20 验收，见 docs/reports/prod-readiness.md）：
> Dockerfile 多阶段（rust:1.97 → bookworm-slim，36MB，非 root + HEALTHCHECK）、
> `RPCROUTER_*` 环境变量覆写、docker-compose（含 monitoring profile）、systemd 样例、
> release profile 调优；`docker run` 单命令起服务后 8 链真实 smoke 通过。

1. **Dockerfile**：多阶段构建（rust:1.97 → debian-slim/distroless），release + strip；
   `data/` 挂卷（chainlist 磁盘缓存跨重启复用）。
2. **docker-compose.yml**：环境变量化关键配置项（listen、chains、缓存容量）。
3. **systemd unit** 样例：`LimitNOFILE=65536`（压测报告已注明 fd 上限是本机瓶颈）、
   Restart=always——冷启动 Probation 兜底已保证重启可用。
4. **release profile 调优**：`lto = "thin"`、`codegen-units = 1`。
5. 验收：`docker run` 一条命令起服务，8 链 smoke 通过；README 部署章节更新。

## P2 生产可用（放心接真实流量）

> **✅ P2.1–P2.4 / P2.6 已完成，P2.5 部分完成**（2026-08-20 验收，见 docs/reports/prod-readiness.md）：
> 优雅退出、入口防护（请求体上限/全局背压/每 IP 限速）、/metrics bearer token 鉴权、
> Prometheus 告警 5 条 + Grafana 仪表盘（monitoring profile 实跑验证）、GitHub Actions CI
> 首跑绿灯（fmt/clippy/test/进程级优雅退出用例/release 构建/1k QPS 离线冒烟）。
> P2.5 soak：真实网络 30 分钟 × 5 QPS × 8 链已过（0 错误，摘除机制被真实 429 触发验证）；
> **24h 长跑仍待执行**（scripts/soak.sh 已就绪，`--duration 86400` 即可）。
>
> **语义边界（重要）**：`user_visible_errors` 是**上游侧承诺指标**——请求已进入数据面
> 转发，但所有上游端点耗尽（见 forward.rs 的 `exhausted()`）。入口防护（过载 503、
> 请求体过大 413、每 IP 限速 429）发生在转发**之前**，属于**入口侧拒绝**，只累计到独立的
> `rpcrouter_ingress_rejected_total{reason=...}`，**不**计入 `user_visible_errors`。
> 告警规则若要表达“上游承诺失败”，只看 `user_visible_errors`；`ingress_rejected` 是过载信号，
> 二者不可混用。

1. **优雅退出**：SIGTERM → 停收新请求、在飞请求排空（deadline 内）再退出。
2. **入口防护**：请求体大小上限、全局并发/背压、每 IP 可选限速开关。
3. **告警面**：Prometheus 告警规则清单——`user_visible_errors > 0`、某链 active 端点数
   低水位、cache hit 骤降、上游 429 率突增；Grafana 仪表盘 JSON 入库。
4. **CI**：GitHub Actions 跑三门槛 + mock 压测冒烟档（如 1k QPS × 10s，防性能回归）；
   正式 10k 压测保持本机/专机手动。
5. **soak**：真实网络低 QPS（≤5）长跑 24h，观察摘除/回池分布与内存曲线。
6. /metrics 绑定内网地址或加简单鉴权开关。

## P3 动态全链目录 + 状态控制 Dashboard（2026-08-25 立项，进行中）

> 需求：从固定 8 链扩展到**实时动态获取并支持 chainlist `rpcs.json` 里的全部链**
> （2877 条），并提供状态控制 Dashboard（独立 React 工程 `dashboard/`，经 REST API 与
> 网关通信）。方案见 `docs/DESIGN-v2.md`，任务拆解与验收见 `docs/TASKS-v2.md`。

1. **W5 动态目录与链生命周期**：Catalog 全量解析；链 pinned/hot/dormant/disabled 生命周期
   （按需激活、idle 降级、LRU 上限）；未知链 404 / 无端点 503 / 禁用 403 且不计
   `user_visible_errors`；探针有界工作池只覆盖激活链；chainlist 刷新 1h + 状态可观测。
2. **W6 状态存储层（Redis）+ Admin REST API**：`StateStore`（Redis / file / memory）持久化运行时
   覆写、端点健康快照、热链集合与审计，支持从零初始化 / 整体覆盖导入 / 重置，Redis 不可用可降级；
   `/admin/api/*` 只读 + 控制接口，bearer 鉴权（无 token 则控制接口 403），可选托管前端静态产物。
3. **W7 React Dashboard**：总览 / 链列表 / 链详情 / 设置；亮暗主题；CI 前端 job；镜像内置。

## P4 命名链路由（对齐 ChainUp 网关形态）

参照 api.chainup.net 的 demo：`POST /{chain_slug}/{api_key}` + `CONSISTENT-HASH` 头。

1. **slug 路由**：`/ethereum/...` 等链名路径与 `/rpc/{chainId}` 并存；内置 slug→chainId
   映射（ethereum→1、bsc→56、polygon→137、arbitrum→42161、avax→43114、base→8453、
   op→10、monad→143…），config 可增改；未知 slug 返回 404 + 可用清单。
2. **API keys 路径段**：v1 语义为「兼容形态」——可选开关：off（忽略该段）/ static（比对
   配置内 key 表，计数按 key 打点）。完整鉴权计费不做（见 AGENTS.md 非目标）。
3. **CONSISTENT-HASH 头**：置 true 时按（key 或调用方标识）一致性哈希粘住上游端点
   （健康前提下），服务 filter/分页等对上游状态敏感的调用序列；落在 select 层，
   与 P2C 并存。
4. **非 EVM 链**（ChainUp 清单里的 solana/ton/tron/bitcoin/aptos/sui…）：协议各异
   （非以太坊 JSON-RPC），列为长期方向；如要做，先从 Solana（JSON-RPC 形态最接近）
   单独立项调研，不进本仓库 v1 架构假设。

## P5 长期

- WebSocket 订阅透传（eth_subscribe）
- **横向扩展**（DESIGN-v2 §12）：**已选定方案 A** 按 chainId 一致性哈希分片部署（`deploy/nginx-shard.conf`，
  零代码；compose `cluster` profile 在 W6 交付）；Phase B 为备选： 实例注册/集群视图、覆写与冷却事件 pub/sub 广播、Redis Lua 分布式每端点
  令牌桶、可选 L2 共享缓存、自路由模式、pinned 分片感知
- simd-json 热路径（压测显示当前 p99 4.6ms，未到瓶颈，暂缓）
- chainlist 之外的端点源聚合（eRPC 公开目录等，许可与条款先行评估）

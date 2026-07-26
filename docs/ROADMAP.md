# ROADMAP — v1 之后的任务规划

> 2026-07-26 收官时制定。v1 已交付（三硬指标达标，见 docs/reports/loadtest-phase3.md）。
> 以下按优先级排列，未排期；每项开工前照旧流程：主会话细化验收 → Codex 实现 → 主会话复验。

## P1 部署（让它跑在服务器上）

1. **Dockerfile**：多阶段构建（rust:1.97 → debian-slim/distroless），release + strip；
   暴露 8545；`data/` 挂卷（chainlist 磁盘缓存跨重启复用）。
2. **docker-compose.yml**：网关 + 可选 Prometheus/Grafana；环境变量覆写 config 关键项
   （listen、chains、缓存容量）。
3. **systemd unit** 样例：`LimitNOFILE=65536`（压测报告已注明 fd 上限是本机瓶颈）、
   Restart=always——冷启动 Probation 兜底已保证重启无故障窗。
4. release profile 调优：`lto = "thin"`、`codegen-units = 1`。
5. 验收：`docker run` 一条命令起服务，8 链 smoke 通过；README 部署章节更新。

## P2 生产可用（放心接真实流量）

1. **优雅退出**：SIGTERM → 停收新请求、在飞请求排空（deadline 内）再退出。
2. **入口防护**：请求体大小上限、全局并发/背压（tower 层已有基础，补默认值与拒绝语义）、
   每 IP 可选限速开关。
3. **告警面**：Prometheus 告警规则清单——`user_visible_errors > 0`、某链 active 端点数
   低水位、cache hit 骤降、上游 429 率突增；Grafana 仪表盘 JSON 入库。
4. **CI**：GitHub Actions 跑三门槛 + mock 压测冒烟档（如 1k QPS × 10s，防性能回归）；
   正式 10k 压测保持本机/专机手动。
5. **soak**：真实网络低 QPS（≤5）长跑 24h，观察摘除/回池分布与内存曲线。
6. /metrics 绑定内网地址或加简单鉴权开关。

## P3 命名链路由（对齐 ChainUp 网关形态）

参照 api.chainup.net 的 demo：`POST /{chain_slug}/{api_key}` + `CONSISTENT-HASH` 头。

1. **slug 路由**：`/ethereum/...` 等链名路径与 `/rpc/{chainId}` 并存；内置 slug→chainId
   映射（ethereum→1、bsc→56、polygon→137、arbitrum→42161、avax→43114、base→8453、
   op→10、monad→143…），config 可增改；未知 slug 返回 404 + 可用清单。
2. **API key 路径段**：v1 语义为「兼容形态」——可选开关：off（忽略该段）/ static（比对
   配置内 key 表，计数按 key 打点）。完整鉴权计费不做（见 AGENTS.md 非目标）。
3. **CONSISTENT-HASH 头**：置 true 时按（key 或调用方标识）一致性哈希粘住上游端点
   （健康前提下），服务 filter/分页等对上游状态敏感的调用序列；落在 select 层，
   与 P2C 并存。
4. **非 EVM 链**（ChainUp 清单里的 solana/ton/tron/bitcoin/aptos/sui…）：协议各异
   （非以太坊 JSON-RPC），列为长期方向；如要做，先从 Solana（JSON-RPC 形态最接近）
   单独立项调研，不进本仓库 v1 架构假设。

## P4 长期

- WebSocket 订阅透传（eth_subscribe）
- 多实例共享缓存（Redis）与分布式出站限流
- simd-json 热路径（压测显示当前 p99 4.6ms，未到瓶颈，暂缓）
- chainlist 之外的端点源聚合（eRPC 公开目录等，许可与条款先行评估）

# 开源 EVM JSON-RPC 负载均衡网关调研

> 调研日：2026-07-25 | 数据来源：GitHub API + 官方 README/docs（已联网核实）

## 结论（先看这个）

**推荐 C：Rust 自研 + 标准 crates，设计上重点借鉴 eRPC / proxyd / blutgang。**

| 选项 | 判定 | 理由 |
|------|------|------|
| **A** 直接部署/fork | ❌ 不作为主路径 | 无项目同时满足：Rust + 公共节点池 + 10k QPS 级去重缓存 + 429 冷却回池 + chainlist。最近似 **eRPC**（Go）可作对照原型，不宜作最终栈。 |
| **B** 纯自研借鉴设计 | 部分成立 | 设计源明确（下表）；需落到 crates 层才可落地。 |
| **C** 自研 + crates | ✅ **首选** | 与仓库 Rust/tokio 硬约束一致；能力可按硬指标裁剪；避免 GPL 传染与死库包袱。 |

**硬指标差距总览**：所有项目均未证明「单链 10k QPS × 仅公共节点」；10k 必须靠 **请求折叠 + 分层缓存**，公共池只吃 miss。eRPC 功能最接近，但语言/许可/定位不同。

---

## 项目对照表

| 项目 | 语言 | License | 活跃度 | ★ 约 | 核心能力摘要 | 距硬指标差距 |
|------|------|---------|--------|------|--------------|--------------|
| [erpc/erpc](https://github.com/erpc/erpc) | Go | Apache-2.0 | **很高**：release `0.1.1`(2026-06)；commit ~2026-07-23 | ~744 | LB/评分选择；retry+**hedge**+circuit breaker；reorg-aware 永久缓存；**in-flight 去重**；多链 `/project/evm/{chainId}`；零配置公开端点目录（`evm-public-endpoints.erpc.cloud`，**非 chainlist.org**）+ Envio | 能力最近；**非 Rust**；面向 provider 聚合；公开端点无 SLA；生产需配付费 upstream |
| [ethereum-optimism/infra/proxyd](https://github.com/ethereum-optimism/infra/tree/main/proxyd) | Go | MIT（infra） | **很高**：`proxyd/v4.29.1`(2026-07-17) | infra ~45 | method 白名单；backend group LB；retry；**consensus 感知 + ban(默认 5min)**；error/latency 滑动窗；Redis **不可变方法**缓存；WS | **运维型**（自管节点/供应商）；缓存面窄；无公共池/chainlist；非 10k 公共池场景 |
| [emeraldpay/dshackle](https://github.com/emeraldpay/dshackle) | **Rust**（已迁） | Apache-2.0 | **高**：`v0.17.0`(2026-04)；commit ~2026-07-22 | ~349 | 故障转移；按 height/peers/方法路由；边缓存(mem/Redis)；gRPC+JSON-RPC；ETH+BTC | 节点/托管向；**无 chainlist 公共池**；重协议与一致性，非公开节点 429 池 |
| [drpcorg/dshackle](https://github.com/drpcorg/dshackle) | Kotlin | Apache-2.0 | **高**：`v0.79.x`(2026-06)；provider 侧 fork | ~71 | dRPC 生产 fork；多 upstream 聚合 | 非 Rust；B2B 提供商栈 |
| [rainshowerLabs/blutgang](https://github.com/rainshowerLabs/blutgang) | Rust | **GPL-2.0** | **中低**：release `0.3.6`/`0.4.0-alpha`(2024-06)；commit ~2025-11 | ~366 | 历史查询 **本地 DB 缓存**(sled/rocksdb)；latency MA；health 摘落后节点；max_per_second；retry；WS | **无多链/chainlist**；GPL 传染；无成熟 429 评分回池；不定位 10k 公共池 |
| [llamanodes/web3-proxy](https://github.com/llamanodes/web3-proxy) | Rust | **GPL-3.0** | **停滞**：last commit **2023-12**；无 release | ~162 | soft/hard limit；按最新块+延迟选 RPC；缓存；用户体系/计费向 | **已停更**；GPL-3；偏商业 RPC 产品，非公共池网关 |
| [status-im/eth-rpc-proxy](https://github.com/status-im/eth-rpc-proxy) | Go | MPL-2.0 | 中：commit ~2026-03 | ~8 | 健康检查 + failover；多链；Prom/Grafana | 体量小；缓存/10k/公共池未覆盖 |
| [DODOEX/web3-rpc-proxy](https://github.com/DODOEX/web3-rpc-proxy) | Go | MIT | 低：~2024-09 | ~12 | 集群代理；选优/最新高度 | 不活跃；能力文档薄 |

**chainlist**：所列项目均**无原生 chainlist.org 集成**。eRPC 用自建公开端点 catalog（理念相近，数据源不同）。

---

## 建议借鉴的具体设计（写入 DESIGN 时对照）

| 来源 | 借鉴点 |
|------|--------|
| **eRPC** | 缓存按 finality 分桶（finalized 长 TTL / tip 短 TTL 或禁缓存）；**in-flight 相同 method+params 折叠**；selection 周期评分（延迟/错误/落后）再准入；hedge 降尾延迟；429/限频 → 熔断摘除 |
| **proxyd** | 1min 滑动窗 error rate + latency 阈值；**ban 冷却时长后回池**；consensus 组内 RR；不可变方法缓存键（hash 类参数） |
| **blutgang** | 历史只读 RPC 持久缓存；per-RPC `max_per_second`/`max_consecutive`；latency 滑动平均排序 |
| **web3-proxy（只读设计）** | soft_limit（开始降权）vs hard_limit（视为限频）；优先低 `active_requests`+低延迟 |

---

## C：Rust crates 分层建议

| Crate | 层 | 用途 |
|-------|----|------|
| **axum** (+ hyper/tower) | 入口 HTTP | JSON-RPC POST 服务、中间件栈 |
| **tower** | 中间件 | timeout、concurrency limit、trace |
| **reqwest** / hyper client | 上游出站 | 连接池、HTTP/1.1 上游 JSON-RPC |
| **alloy**（primitives + rpc-types） | 协议 | chainId/块标签/类型安全；勿绑全节点 |
| **serde_json** + **simd-json**(可选) | 编解码 | 热路径解析 method/params 做缓存键 |
| **moka** | L1 缓存 | 进程内 async cache；tip 短 TTL / finalized 长 TTL |
| **dashmap** | 并发状态 | per-endpoint 健康分、冷却表、in-flight 去重 map |
| **governor** | 限流 | **出站** per-endpoint QPS 上限（保护公共节点，非伪装绕限） |
| **tokio** | 运行时 | 并发、hedge(`select`)、定时健康探针 |
| **prometheus** / metrics | 可观测 | QPS、命中率、429、摘除、failover |
| **blake3** 或 ahash | 缓存键 | `chainId \| method \| canonical(params)` 哈希 |
| **thiserror/anyhow** | 错误 | 内部错误 vs 对用户统一 JSON-RPC error（全池耗尽才暴露） |

可选后续：`redis`（多实例共享缓存）、`governor` 分布式改 Redis token bucket。

---

## 与 A 的边界说明

- **原型对照**：可本地起 eRPC 验证「公开池 + 缓存 + failover」产品假设，**不**作为 rpcrouter 代码基线。
- **不要 fork**：web3-proxy（死+GPL3）、blutgang（GPL2+单链向）、proxyd（OP 运维模型）。
- **dshackle(Rust)**：可作健康/路由参考实现阅读，目标用户不同，不宜整体采用。

---

## 验收映射（实现时）

1. **10k QPS**：压测 mock 上游；命中路径零上游；miss 路径折叠。
2. **429 摘除**：识别 HTTP 429 / 厂商限频 body / HTML / timeout → `cooldown_until` → 探针成功回池。
3. **用户无感**：单 upstream 失败不回传；仅 `all_endpoints_exhausted` 时 JSON-RPC error。
4. **链**：ETH + Monad 先通；节点清单：chainlist 拉取 + 本地样例（单测禁外网）。

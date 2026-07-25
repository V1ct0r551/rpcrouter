# rpcrouter 架构方案（DESIGN v1）

> 2026-07-25 主会话制定。输入：`docs/research/` 两份调研。
> 硬指标：单链 10000 QPS；智能摘除限频节点（冷却后回池）；用户端无错误感知；ETH + Monad 先跑通。

## 0. 总体判断（调研结论，不再复述论证）

- **纯公共池裸转发扛不住 10k QPS**：单个公共端点可持续约 10–25 QPS。10k 必须靠
  **响应缓存 + in-flight 请求折叠**承接绝大多数流量，公共池只吃未命中。
- 端点数据源主选 `https://chainlist.org/rpcs.json`（约 2MB，6–24h 拉一次 + ETag），
  备选 `https://chainid.network/chains.json` + 内置快照。实测 ETH(1) ~76 个 https 端点，
  **Monad 主网 chainId=143** ~16 个（官方端点限 15–25 rps）。
- 复用策略：Rust 自研。crates：axum/tower/reqwest/moka/dashmap/governor/tokio/blake3/
  prometheus/serde_json（alloy-primitives 按需）。设计借鉴 eRPC（finality 分桶缓存、
  in-flight 折叠、评分再准入、hedge）与 proxyd（滑动窗 → ban 冷却 → 回池）。

## 1. 请求路径（数据面）

```
POST /rpc/{chainId}
 → ① 解析：单个或 batch（batch 拆子请求、保序合并返回；上限 100 条）
 → ② 方法分类：可缓存性分桶（§3）
 → ③ 缓存查询（moka）：命中 → 直接返回
 → ④ in-flight 折叠：相同键已在飞 → 挂起等它的结果
 → ⑤ 选点（§4）→ 出站闸（per-endpoint 令牌桶 + 并发位）
 → ⑥ reqwest 转发（超时）→ 响应判定（§5）
 失败：标记端点 + 换点重试（预算内）；成功：回填缓存 + 唤醒折叠等待者 → 返回
```

请求级预算：总 deadline 默认 15s；最多尝试 4 个不同端点；只读方法可选 hedging（§6）。

## 2. 核心状态

```
Registry = DashMap<chainId, ChainState>
ChainState { name, head: AtomicU64, endpoints: Vec<Arc<Endpoint>>（RCU 式整体换指针）}
Endpoint {
  url,
  state: Active | Cooling { until, strikes } | Probation { passes },
  latency_ewma, err_window(60s 滑动窗), lag(块高滞后),
  bucket: 令牌桶(默认 15 rps，可按端点覆写),
  inflight: Semaphore(默认 8),
  stats: 累计计数（供 /chains /metrics）
}
```

## 3. 缓存（10k QPS 的主承力结构）

- **键**：`blake3(chainId | method | canonical(params))`；canonical = 紧凑保序 JSON。
- **分桶**（借鉴 eRPC finality 思想）：
  - **不可变**：引用了确定旧块的读（`eth_getBlockByNumber(n < head-K)`、老交易的
    receipt/tx、`eth_chainId` 等）→ 长 TTL（≥1h），以容量淘汰为主。K=确认深度，
    默认 64，可按链覆写。
  - **tip 敏感**：latest/pending 相关（`eth_blockNumber`、`eth_call@latest`、
    `eth_getBalance@latest`、`eth_gasPrice`、`eth_feeHistory` 等）→ **键内掺当前 head**
    （head 前进自然失效）+ 短 TTL 兜底：默认 `min(块时间, 2s)`；ETH≈2s，Monad≈400ms，按链配置。
  - **不缓存**：`eth_sendRawTransaction`、filter/subscription 类、`eth_getTransactionCount@latest`
    （nonce 敏感）、未知方法默认不缓存。
- **折叠**：未命中的相同键并发请求只放一发上游，其余等待复用同一结果（对 latest 类流量
  命中率贡献极大）。
- 容量：moka 按响应字节加权，默认上限 512MB。

## 4. 端点池与选点

- 池来源：chainlist 刷新（§7）+ 配置 extra 端点（如 Monad 官方端点，按官方文档标 rps 上限）。
- 选点：Active 集内 **P2C**（随机取二选优）；分 = f(latency_ewma, err_rate, lag)。
  令牌桶无票或并发满的端点视为本次不可选——高压下流量自然摊开。
- **出站保护（硬性，TOS 友好）**：对单个公共端点的出站 QPS/并发永不超上限；429 退避换点，
  绝不硬打；UA 诚实标 `rpcrouter/<ver>`。

## 5. 端点故障判定与摘除/回池（智能摘除）

- **立即冷却（限频信号）**：HTTP 429（尊重 `Retry-After`）；403 + quota 语义；JSON-RPC error
  message 匹配 `/rate limit|too many requests|request rate exceeded|compute unit|capacity|throttl|quota/i`；
  「需要 API key/认证」类响应。
- **短冷却/降权（劣质信号）**：5xx、CF 52x、HTML/非 JSON body、超时、延迟 > 3–5s、
  块高滞后超阈值；`eth_chainId` 不匹配 → 直接从该链池剔除。
- **不算故障（必须原样透传）**：`execution reverted`、`-32602` 参数错、`-32601` 方法不存在
  等链级/请求级正常错误——那是正确答案。
- 冷却时长：指数退避 30s → 1m → 5m → 15m（strikes 累计，封顶 1h）；`Retry-After` 优先。
- 回池：冷却到期 → Probation，探针连续 2 次通过 → Active；strikes 随时间衰减。

## 6. 用户无错误感知（错误语义规范）

- 端点级失败**绝不回传**：换点重试直至预算耗尽。
- 链级错误原样透传（见 §5 白名单）。
- 全池/预算耗尽：HTTP 200 + JSON-RPC error `{code:-32000, message:"rpcrouter: all upstream
  endpoints exhausted for chain <id>"}` —— 唯一允许的网关自产错误，计入
  `user_visible_errors` 指标（目标 ≈ 0）。
- `eth_sendRawTransaction`：同一签名交易幂等，可换点重试；"already known" 视为成功语义透传。
- **hedging**（只读方法，默认开）：300ms（或按 p95 自适应）无响应 → 另一端点发第二发，
  取先到者；hedge 流量占出站 ≤10%，池不健康时自动关。

## 7. 控制面（后台任务）

- **chainlist 刷新**：默认 6h + ETag；过滤 https-only、剔 `${KEY}` 模板、去重；**合并**进
  Registry（保留既有端点运行态；新端点从 Probation 起步；消失端点宽限 24h 再移除）。
  三级回退：内存上次成功 → 磁盘缓存（`./data/rpcs.json`）→ 仓库内置 fixture。
- **健康探针**：每端点 15–30s（带抖动）：`eth_chainId` 校验 + `eth_blockNumber`
  （更新 latency/lag）；探针流量计入端点令牌桶；Cooling 端点到期才探；全局探针并发上限。
- **head 跟踪**：探针聚合出每链 head（取截尾最大值容错），供 lag 评分与 §3 tip 键。

## 8. 可观测性

- `/metrics`（Prometheus）：按链——ingress qps、cache hit%、coalesce%、upstream qps、
  `user_visible_errors`、latency p50/95/99、failover 深度；按端点——qps、429、冷却事件、状态。
- `/chains`：池概览（active/cooling/probation 数、head、命中率）。`/healthz`。
- tracing 结构化日志，消息英文。

## 9. 性能工程（10k QPS 落地）

- 热路径零拷贝优先：`serde_json::value::RawValue` 透传 params/result，不整体重序列化；
  simd-json 留作后备优化，先不引。
- 压测：workspace 内 `mock-upstream` bin（可配延迟、429 阈值、错误注入）+
  `scripts/loadtest.sh`（oha 或 vegeta）。验收场景见 TASKS Phase 3。
- tokio 多 worker；moka/dashmap 高并发结构；热路径无全局锁。

## 10. 配置

`config.toml`（零配置可跑，默认 chains=[1,143]）：listen、metrics 开关、chains 允许清单 +
每链覆写（block_time、确认深度 K、TTL、池参数）、endpoint 覆写（extra/屏蔽/rps 上限）、
刷新/探针间隔、缓存容量、hedging 开关、deadline/attempts。

## 11. v1 非目标

WebSocket 订阅透传、多实例共享缓存（Redis）、鉴权/计费、方法白名单治理、非 EVM 链。

## 12. 风险与对策

- 公共端点大面积抖动/限频 → 缓存兜底 + `user_visible_errors` 告警。
- chainlist 不可用 → 三级回退（§7）。
- Monad 池小且官方 15–25 rps → 出站上限严格执行、TTL 按 400ms 块时间调优；
  testnet(10143) 可另行接入演示。
- 缓存正确性（reorg）：tip 桶掺 head + 短 TTL；不可变桶只收 head-K 之前的数据。

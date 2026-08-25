# rpcrouter 架构方案 v2 —— 动态全链目录 + 状态控制 Dashboard

> 2026-08-25 主会话制定。前置：`docs/DESIGN.md`（v1，仍然有效，本文只写增量）。
> 目标：
> 1. 从静态 8 链扩展到 **实时动态获取并支持 chainlist `rpcs.json` 里的全部链**
>    （实测 2877 条链 / 2732 条至少有 1 个公开 https 端点 / 5562 个 https 端点）。
> 2. 提供 **状态控制 Dashboard**：独立 React 前端项目（仓库根目录 `dashboard/`），
>    通过 Rust 网关暴露的 REST API 观测与控制运行态。
>
> 硬指标不变：单链 10k QPS、智能摘除限频节点、用户端无错误感知。

## 0. 核心判断

- **不能把 2877 条链全部当作 v1 的「已启用链」**。v1 对每个端点每 15–30s 探一次
  （2 次 RPC），5562 个端点意味着 ~500–750 req/s 的常驻后台流量打向公共节点，既浪费
  也违反项目「对公共端点友好」的约定；且 chainlist 里大量死端点会用超时吃光探针并发。
- 因此链要有**生命周期**：目录里的链默认 **dormant**（只保留元数据，零成本），
  **首个请求到达时按需激活**（materialize 端点池 → 冷启动路径直接用 Probation 端点承接
  流量，v1 已支持）→ 探针只覆盖激活链；无流量一段时间后自动降级回 dormant。
  配置里的 `chains` 列表升级为 **pinned**：启动即激活、永不降级（向后兼容旧语义）。
- 「实时动态获取」= chainlist 刷新周期从 6h 缩到 **1h**（chainlist.org 返回 ETag，
  304 时零流量）+ Dashboard 可手动触发刷新 + 目录/刷新状态可观测。
- Dashboard 是**独立前端工程**（React + TypeScript + Vite，`dashboard/`），
  不嵌入 Rust 二进制；Rust 侧只提供 `/admin/api/*` REST 接口（JSON），可选地把
  `dashboard/dist` 当静态目录托管（单机部署省一个 nginx）。控制类接口必须有
  bearer token 鉴权，未配置 token 时控制接口一律 403（安全默认）。

## 1. 目录（Catalog）与链生命周期

```
Catalog（Arc<...>，每次 chainlist 刷新整体替换）
  chains: Vec<CatalogChain>          // 全部链（含 0 端点链、testnet、deprecated）
  by_id:  HashMap<u64, usize>
CatalogChain {
  chain_id, name, short_name, chain（如 "ETH"）, slug: Option, is_testnet,
  native_symbol, explorer_url: Option, status: Option（active/incubating/deprecated…）,
  tvl: Option<f64>,
  endpoints: Vec<CatalogEndpoint { url, tracking: Option<String> }>   // 过滤后的公开 https
}
过滤规则沿用 v1：https-only、剔 `${KEY}` 模板、剔带 userinfo、去重、去 fragment。
```

```
ChainState（只为 materialized 的链存在）
  chain_id, name, pinned: AtomicBool, disabled: AtomicBool,
  last_ingress_unix: AtomicU64（粗粒度秒；只在值变化时写，避免 10k QPS 下缓存行争用）,
  endpoints: RwLock<Vec<Arc<Endpoint>>>（v1 原样：健康状态机/令牌桶/并发位）,
  rejected, head（v1 原样）
```

链的四种可见状态（Dashboard 用语）：

| 状态 | 含义 | 探针 | 承接流量 |
|---|---|---|---|
| `pinned` | config `chains` 或运行时 pin；启动即激活，永不降级 | 是 | 是 |
| `hot` | 目录链被请求激活（materialized），近期有流量 | 是 | 是 |
| `dormant` | 目录里有、未 materialize（或已降级）；只有元数据 | 否 | 首个请求触发激活 |
| `disabled` | config `discovery.deny` 或运行时 disable | 否 | 拒绝（403） |

生命周期规则：

- **激活**：`Registry::resolve_for_request(chain_id)`（热路径，必须廉价：DashMap get +
  一次原子读；dormant 时才走慢路径 materialize）。materialize = 从 Catalog 取端点 +
  `chain_overrides.extra_endpoints` − `disabled_endpoints` − 运行时 disabled，端点从
  Probation 起步；同时向探针调度器发一次「立即探测该链」的 kick，使 Probation→Active
  在 ~2 个探针周期内完成。首个请求走 v1 冷启动路径（Probation 端点可用）。
- **降级**：后台 housekeeping（每 30s）：非 pinned 的 hot 链 `now - last_ingress >
  discovery.idle_seconds`（默认 600）→ dormant（丢弃端点运行态；再次激活重新从
  Probation 起步——这是可接受的，因为端点健康在几分钟后本来就该重新验证）。
- **上限**：非 pinned hot 链数量 > `discovery.max_hot_chains`（默认 256）时，按
  `last_ingress` LRU 降级最久未用的，pinned 永不淘汰。这把扫描式流量（有人遍历
  /rpc/1..3000）造成的探针放大限制在可控范围。
- **未知链**：chain_id 不在目录也不在 pinned → **HTTP 404** + JSON-RPC 错误体
  `{"code":-32000,"message":"rpcrouter: unknown chain id N"}`，计入
  `rpcrouter_ingress_rejected_total{reason="unknown_chain"}`，**不**计入
  `user_visible_errors`（v1 把未知链算成上游耗尽是缺陷，本次修正）。
- **已知链但目录里 0 个可用端点**（145 条）：**HTTP 503** + JSON-RPC 错误体
  `"rpcrouter: chain N has no public endpoints"`，`reason="no_endpoints"`，同样不算
  用户可见错误（我们从未承诺）。语义边界与 P2 一致：**端点池非空但全部尝试失败**才是
  `user_visible_errors`（上游承诺失败）。
- **disabled**：HTTP 403 + JSON-RPC 错误体，`reason="chain_disabled"`。

链解析在 `server.rs` 的路由层做一次（batch 也只做一次），Forwarder 签名保持 `chain_id`
不变，降低改动面。

## 2. 每链参数的默认值（无覆写时）

`Classifier` 不再只预计算 `config.chains`：任意 chain_id 的确认深度 / tip TTL 走
`Config::confirmation_depth / tip_ttl_ms` 的默认路径（64 块、`min(block_time, 2s)`，
block_time 未知按 2s）。2s 的 tip TTL 对快链意味着最多 2s 的 tip 陈旧——与 ETH 现状相同，
可接受；运维可用 `chain_overrides` 或 Dashboard 运行时覆写调整。`chain_overrides` 不再
要求 chain_id 出现在 `chains` 列表里（激活时生效）。

（后续可选：由探针观测到的 head 推进速率自动估算块时间，本期不做。）

## 3. 探针调度器改造

- 调度对象从 `config.chains` 改为 `registry.hot_chain_ids()`（pinned + hot）。
- 改成**有界工作池**：due 列表进队列，N=`probe.max_concurrency` 个 worker 消费；不再
  每个 due 端点 `tokio::spawn` 一个任务挂在信号量上（调度落后时会无界堆积、同一端点
  重复排队）。`next` 时间在探测**完成**后设置。
- 新增 kick 通道：激活链时立即把该链全部端点入队（跳过抖动等待）。
- 指标：`rpcrouter_probe_queue_depth`、`rpcrouter_probe_in_flight`。
- 死端点自然收敛：超时/传输错误 3 次进 Cooling，指数退避到 1h——v1 机制不变。

## 4. chainlist 刷新

- 默认 `refresh_seconds` 6h → **3600**；ETag 304 零流量；失败三级回退不变。
- 刷新结果 = 新 Catalog 整体替换 + 对所有 materialized 链执行 v1 merge（保留运行态、
  新端点 Probation、消失端点 24h 宽限）。
- 刷新状态可观测：`rpcrouter_chainlist_last_refresh_timestamp_seconds`、
  `rpcrouter_chainlist_refresh_total{source=network|not_modified|memory|disk|fixture}`、
  `rpcrouter_catalog_chains`、`rpcrouter_catalog_endpoints`；Admin API 暴露最近一次来源 /
  时间 / etag / 错误。
- Admin API 可手动触发刷新（与周期刷新互斥，进行中返回 409）。

## 5. 配置增量

```toml
chains = [1, 143, 56, 137, 42161, 8453, 10, 43114]   # 语义升级为 pinned（向后兼容）

[discovery]
enabled = true            # false = 只服务 pinned 链（等价 v1 行为）
include_testnets = true
deny = []                 # chainId 黑名单（拒绝 403）
max_hot_chains = 256
idle_seconds = 600

[chainlist]
refresh_seconds = 3600    # 默认由 21600 改为 3600

[admin]
enabled = true
# auth_token = "..."      # 未配置：只读接口开放、控制接口 403；配置后 /admin/api/* 全部需 Bearer
# static_dir = "./dashboard/dist"   # 可选：托管前端构建产物到 /dashboard/
# cors_allow_origins = ["http://localhost:5173"]   # 可选：前端独立域名/开发服务器

[state]                   # 持久状态存储（§11）
backend = "redis"         # "redis" | "file"（file = data/state.json，单机零依赖回退）
redis_url = "redis://127.0.0.1:6379/0"
namespace = "rpcrouter"   # key 前缀；换前缀 = 换一套全新状态
required = false          # true：连不上 Redis 启动失败；false：降级为内存+磁盘缓存运行并后台重连
flush_interval_ms = 2000  # 端点健康快照 write-behind 周期
health_ttl_seconds = 86400
```

环境变量：`RPCROUTER_DISCOVERY_ENABLED`、`RPCROUTER_DISCOVERY_MAX_HOT_CHAINS`、
`RPCROUTER_DISCOVERY_IDLE_SECONDS`、`RPCROUTER_ADMIN_TOKEN`、`RPCROUTER_ADMIN_STATIC_DIR`、
`RPCROUTER_STATE_BACKEND`、`RPCROUTER_REDIS_URL`、`RPCROUTER_STATE_NAMESPACE`、`RPCROUTER_STATE_RESET`。
校验：`discovery.enabled=false` 时 `chains` 不得为空；`enabled=true` 时允许为空。

## 6. Admin REST API（Rust 侧，`/admin/api/*`）

通用约定：JSON、字段 camelCase（与现有 `/chains` 输出一致）；错误体统一
`{"error":{"code":"unknown_chain|unauthorized|forbidden|admin_disabled|invalid_argument|conflict|not_found","message":"..."}}`
配合 HTTP 400/401/403/404/409。鉴权：`[admin].auth_token` 已配置 → 所有 `/admin/api/*`
需 `Authorization: Bearer <token>`（复用 v1 `BearerAuth`）；未配置 → GET 开放、
POST/PUT/DELETE 一律 403 `admin_disabled`。`[admin].enabled=false` → 整个 `/admin` 404。

只读：

| 方法 路径 | 返回 |
|---|---|
| `GET /admin/api/overview` | 进程（version/uptime）、chainlist（source/lastRefreshUnix/etag/catalogChains/catalogEndpoints/refreshSeconds/lastError/refreshing）、链计数（catalog/pinned/hot/dormant/disabled）、端点计数（materialized/active/cooling/probation）、流量累计（ingressTotal/cacheHitsTotal/cacheLookupsTotal/coalescedTotal/upstreamTotal/userVisibleErrorsTotal/ingressRejectedTotal/hedgesTotal/inFlight）、探针（queueDepth/inFlight/maxConcurrency）、缓存（entries/weightedBytes/maxBytes） |
| `GET /admin/api/chains?state=all|pinned|hot|dormant|disabled&q=<子串匹配 name/shortName/chainId>&testnet=true|false&sort=traffic|chainId|name&limit=&offset=` | `{total, items:[ChainRow]}`；ChainRow = chainId,name,shortName,isTestnet,status,state,pinned,disabled,catalogEndpoints,endpoints,active,cooling,probation,head,lastIngressUnix,ingressTotal,cacheHitsTotal,cacheLookupsTotal,upstreamTotal,userVisibleErrorsTotal,settings{blockTimeMs,confirmationDepth,tipTtlMs,maxBlockLag,source:"default|config|runtime"} |
| `GET /admin/api/chains/{id}` | ChainRow + `endpoints:[EndpointRow]`；EndpointRow = url,tracking,state,strikes,coolingUntilUnix,latencyEwmaMs,lag,rps,concurrency,disabled,source:"chainlist|config|runtime",lastFault,stats{outboundRequests,failures,rateLimited,coolingEvents,probeSuccesses} |
| `GET /admin/api/overrides` | 当前持久化的运行时覆写文档 |

控制（全部幂等，返回操作后的对象）：

| 方法 路径 | 作用 |
|---|---|
| `POST /admin/api/chainlist/refresh` | 立即刷新（进行中 409） |
| `POST /admin/api/cache/clear` `{chainId?}` | 清响应缓存（全部或单链） |
| `POST /admin/api/chains/{id}/activate` / `demote` / `pin` / `unpin` / `enable` / `disable` | 链生命周期控制（pin/disable 持久化） |
| `PUT /admin/api/chains/{id}/settings` `{blockTimeMs?,confirmationDepth?,tipTtlMs?,maxBlockLag?}` | 运行时覆写链参数（持久化；null 删除） |
| `POST /admin/api/chains/{id}/endpoints/{action}` body `{url, ...}` | action ∈ `disable`/`enable`（持久化）、`cool {seconds}`、`reset`（清 strikes→Probation）、`probe`（立即探一次，返回结果）、`limits {rps?,concurrency?}`（持久化）、`add`（运行时附加端点，持久化）、`remove`（只允许删 runtime 附加的） |

运行时覆写经 **状态存储层（§11）** 持久化（默认 Redis；`state.backend="file"` 时为
`data/state.json` 原子写），启动时加载并叠加在 config.toml 之上（优先级：runtime > config >
default）。另有状态管理接口：`GET /admin/api/state`（后端/连通性/schema/最近 flush）、
`GET /admin/api/state/export`（全量 JSON 导出）、`POST /admin/api/state/import`（整体覆盖导入）、
`POST /admin/api/state/reset`（清空本命名空间并从零重新初始化，需 token + `{"confirm":true}`）。
`/chains`、`/healthz`、`/metrics` 保持不变（`/chains` 增加 `state` 字段）。

## 7. Dashboard（`dashboard/`，独立 React 工程）

- 技术栈：React 18 + TypeScript + Vite；数据层用 TanStack Query 轮询（overview 2s、
  链详情 2s、链列表 5s、dormant 目录 60s）；图表用 uPlot 或 Recharts（二选一，不手写
  D3）；**不引入任何运行时外链资源**（字体/CDN 脚本），构建产物纯静态。
- 开发：`vite.config.ts` 把 `/admin` 代理到 `http://127.0.0.1:8545`；生产：
  `npm run build` → `dashboard/dist`，由网关 `[admin].static_dir` 托管在 `/dashboard/`
  （SPA fallback），或任意静态服务器 + 反代。
- 页面：
  1. **总览**：stat tiles（目录链数 / 激活链数 / 端点 active·cooling·probation / 入口 QPS /
     缓存命中率 / 用户可见错误 / 在飞 / 探针队列 / chainlist 最近刷新 + 来源）+ 近 5 分钟
     QPS 与命中率折线（客户端从轮询增量计算，缓冲在内存）+ 「刷新 chainlist」「清缓存」按钮。
  2. **链列表**：可搜索（name/shortName/chainId）、按状态与 testnet 过滤、按流量排序的表；
     行内显示状态色块 + 文字（不只靠颜色）、端点 active/total、head、1 分钟 QPS、命中率、
     UVE；行操作：activate / pin / disable。
  3. **链详情**：参数卡（当前生效值 + 来源 + 可编辑）、端点表（状态、strikes、冷却剩余、
     延迟 EWMA、lag、rps/并发、请求/失败/429/冷却次数、last fault）、端点操作
     （disable/enable/cool/reset/probe/limits/add）。
  4. **设置**：token 输入（存 localStorage，仅随请求头发送）、轮询间隔、主题（跟随系统 +
     手动切换）。
- 视觉规范（对齐 `dataviz` 方法，亮/暗两套）：
  - 表面：light `#fcfcfb` / dark `#1a1a19`；文字 primary `#0b0b0b`/`#ffffff`，
    secondary `#52514e`/`#c3c2b7`。
  - 状态色固定：active/good `#0ca30c`、probation/warning `#fab219`、cooling/serious
    `#ec835a`、error/critical `#d03b3b`、dormant 中性灰；状态永远「色块 + 文字/图标」。
  - 系列色按固定顺序：`#2a78d6`（QPS）、`#eb6834`（上游 QPS）、`#1baf7a`（命中率）…，
    暗色对应 `#3987e5`/`#d95926`/`#199e70`。单轴，不做双 Y 轴；细线 2px；悬停有 tooltip。
  - 数值文字用文字色，不用系列色；表格是一等公民（所有图表数据都有表格视图或即为表格）。
- 质量门槛：`npm run lint`（eslint）、`npm run typecheck`（tsc --noEmit）、`npm test`
  （vitest + testing-library：表格过滤/排序、状态映射、token 头注入、错误提示）、
  `npm run build` 全绿；测试不访问网络（用 msw 或手写 fetch mock）。

## 8. 可观测性增量

- 新 Prometheus 指标：`rpcrouter_chains{state="pinned|hot|dormant|disabled"}`（Gauge）、
  `rpcrouter_chain_pinned{chain_id}`（Gauge，1=pinned，供告警只盯 pinned 链）、
  `rpcrouter_catalog_chains`、`rpcrouter_catalog_endpoints`、`rpcrouter_probe_queue_depth`、
  `rpcrouter_probe_in_flight`、`rpcrouter_chainlist_last_refresh_timestamp_seconds`、
  `rpcrouter_chainlist_refresh_total{source}`、`rpcrouter_chain_activations_total`、
  `rpcrouter_catalog_records_skipped_total`（逐记录容错解析跳过数）、
  `rpcrouter_cold_start_failures_total{chain_id}`（无 Active 端点时的冷启动失败，不计 UVE）、
  `rpcrouter_chain_demotions_total{reason="idle|lru|admin"}`；`ingress_rejected` 新增
  reason `unknown_chain|no_endpoints|chain_disabled`。
- 端点级指标只覆盖 materialized 链（基数受 `max_hot_chains` 约束）。
- 告警调整：`RpcrouterChainActiveEndpointsLow` 只对 `rpcrouter_chain_pinned==1` 的链求值
  （动态链里 1744 条只有 1 个端点，原表达式会常燃）。Grafana 增加「链生命周期 / 目录 /
  探针队列」一行。

## 9. 安全与 TOS

- 控制接口默认关闭（无 token 即 403），token 只走 Authorization 头（无 cookie → 无 CSRF）。
- 动态激活不会突破 v1 的出站保护：每端点 15 rps / 8 并发上限、429 退避换点、诚实 UA。
- `max_hot_chains` + `idle_seconds` 把后台探针流量限制在有界范围；dormant 链零外呼。
- 不做端点 URL 之外的任何采集；`tracking` 字段只作展示。

## 10. 非目标（本期）

命名链路由（`/ethereum/...`，ROADMAP P4）、非 EVM 链、WebSocket、多实例共享**响应缓存**与
分布式出站限流（P5）、基于探针的块时间自适应、Dashboard 用户体系（单 token 即可）。

## 11. 状态存储层（Redis 持久镜像，2026-08-25 增补）

> 用户要求：程序重启不丢状态；Redis 必须支持**从 0 初始化**与**整体覆盖**。

### 11.1 原则

- **内存仍是数据面的唯一真相，Redis 是持久镜像**。10k QPS 热路径（选点、缓存、健康计数）
  零 Redis 调用；Redis 只在三种时机参与：启动 bootstrap（读）、管理操作（同步写，写成功才
  返回 200）、后台 write-behind（每 `flush_interval_ms` 批量写脏端点健康快照）。
- **响应缓存不进 Redis**（每请求一次网络往返会毁掉 p99；多实例共享缓存留 P5）。
- **Redis 不可用不影响服务**：`required=false`（默认）时降级为内存 + 磁盘缓存运行，后台指数
  退避重连，恢复后补一次全量 flush；`rpcrouter_state_store_up` 指标 + 告警。`required=true`
  用于必须保证状态持久的部署（连不上直接启动失败）。
- 单机零依赖仍然可用：`backend="file"` 用 `data/state.json`（同一 `StateStore` trait 的文件
  实现，原子写），语义与 Redis 完全一致；单测全部跑在内存/文件实现上，Redis 实现用
  `#[ignore]` 集成测试 + CI service container 覆盖。

### 11.2 数据模型（全部 key 带 `{namespace}:` 前缀）

| key | 类型 | 内容 | 写入时机 |
|---|---|---|---|
| `meta` | hash | `schema_version`、`seeded_at`、`last_flush_at`、`instance_id` | 初始化 / 每次 flush |
| `catalog` / `catalog:etag` | string(JSON, gzip) | chainlist 快照（替代/并列磁盘缓存） | 每次成功刷新 |
| `override:chain:{id}` | hash | `pinned` `disabled` `block_time_ms` `confirmation_depth` `tip_ttl_ms` `max_block_lag` | 管理操作（同步） |
| `override:endpoint:{id}:{blake3(url)[:16]}` | hash | `url` `disabled` `rps` `concurrency` `source=runtime` | 管理操作（同步） |
| `override:index` | set | 所有 override key 的索引（避免 SCAN） | 随上两项 |
| `health:{id}:{urlhash}` | hash（TTL `health_ttl_seconds`） | `state` `cooling_until_unix` `strikes` `latency_ewma_us` `lag` 累计计数 | write-behind，只写脏端点 |
| `chains:hot` | zset（score = last_ingress_unix） | 激活链集合 | write-behind |
| `audit` | stream（MAXLEN ≈ 10000） | 管理操作审计：who(token 指纹)/what/target/before/after | 管理操作 |

启动恢复：`chains:hot` 里 `now - score < idle_seconds` 的链预激活（避免重启后首个请求冷启动）；
`health:*` 里仍在冷却期的端点恢复为 Cooling（重启后不再去撞已知限频端点），其余端点照常从
Probation 起步——不恢复 Active（探针必须重新证明）。

### 11.3 从零初始化 / 覆盖 / 重置（用户明确要求）

1. **从零初始化**：启动时 `meta` 不存在 → 视为空库：写 `meta`（schema_version = 当前版本）、
   写 `catalog`（首次拉取结果）；覆写为空。**config.toml 不会被复制进 Redis**——它是叠加层，
   改配置文件后重启即生效，不会被历史状态遮蔽。
2. **schema 版本**：`meta.schema_version` 与二进制不一致 → 有迁移则迁移，否则按 `required`
   决定：报错退出或忽略旧状态从零初始化（记 warn）。
3. **整体覆盖**：`POST /admin/api/state/import`（body = export 格式 JSON）→ 事务内清空
   `override:*` / `chains:hot` / `health:*` 后写入新内容并立即应用到内存。
4. **重置**：`rpcrouter --reset-state` 或 `RPCROUTER_STATE_RESET=1`（启动时）或
   `POST /admin/api/state/reset {"confirm":true}`（运行时）→ 删除本命名空间全部 key 并按 1 重新
   初始化；换 `namespace` 也等价于一套全新状态（用于灰度/回滚）。
5. 所有写操作只触及本命名空间，可与其他应用共用一个 Redis；key 使用 `{namespace}` hash tag
   风格前缀，兼容 Redis Cluster。

### 11.4 实现要点

- `state::StateStore` trait（async）：`bootstrap()`, `load_overrides()`, `put_chain_override()`,
  `put_endpoint_override()`, `delete_*()`, `flush_health(batch)`, `load_health()`,
  `set_hot_chains()`, `append_audit()`, `export()`, `import()`, `reset()`, `health()`；
  实现：`MemoryStore`（测试）、`FileStore`（`data/state.json`）、`RedisStore`。
- crate：`redis`（`tokio-comp` + `connection-manager`，自动重连）；批量写用 pipeline，
  import/reset 用 `MULTI/EXEC`。
- write-behind：Registry 维护脏端点集合（DashSet），flush 任务每周期取走、pipeline 写入；
  单次上限 2000 条，超出下周期续写。
- 指标：`rpcrouter_state_store_up`、`rpcrouter_state_flush_total{result}`、
  `rpcrouter_state_flush_duration_seconds`、`rpcrouter_state_dirty_endpoints`。
- docker-compose：新增 `redis:7-alpine` 服务（`--appendonly yes` + 卷）；网关默认
  `RPCROUTER_REDIS_URL=redis://redis:6379/0`。

## 12. 多实例与横向扩展路线（2026-08-25 增补）

> 用户问题：部署 rpcrouter 的机器要扛住所有链的访问，流量单点怎么办？更平滑的横向扩展方式？

**决策（2026-08-25，用户选定方案 A）：按 chainId 一致性哈希分片 + 无状态实例 + Redis 共享镜像；
Phase B 各项仅作为 P5 备选，按需再立项。** 不用轮询：轮询会把每条链的
缓存/折叠/健康学习/每端点出站限流复制 N 份，缓存命中率下降、公共节点实际承受 N×15 rps、探针 ×N。

### Phase A（已选定；零代码改动，部署即得）
- N 个实例（同一 config + 同一 Redis）放在 nginx/HAProxy/云 LB 后，LB 对 `/rpc/{chainId}` 做
  **一致性哈希**（样例 `deploy/nginx-shard.conf`）。每条链固定落一个实例：
  - 缓存与 in-flight 折叠局部性最好；每端点 15 rps / 8 并发上限仍然准确（不随实例数放大）；
  - v2 的按需激活让每个实例只探测自己分到的链（无需分片配置）；pinned 链会被所有实例探测，
    N≤5 时可接受，更多实例时把 pinned 改成「只 pin 本实例分片内的链」（Phase B）；
  - 加节点只迁移 ~1/N 的链；节点故障时环上下一个实例接管，冷启动路径 + Redis 健康快照
    （已知冷却端点不再撞）让接管近乎无感。
- 单实例容量：本机 10k QPS p99 4ms（loadtest-phase3），N 实例 ≈ N×10k；LB 层 nginx 单机
  5–10 万 rps 以上；LB 自身用 keepalived/云 LB/多 A 记录做 HA；跨地域用 GeoDNS 分集群。
- Redis HA 用 Sentinel/托管服务；因 `state.required=false` 降级模式，Redis 故障不影响出流量。

### Phase B（未选定，P5 备选，按需）
1. **实例注册与集群视图**：`{ns}:instance:{id}` 心跳 hash（TTL 15s）+ 各实例摘要；
   `GET /admin/api/cluster` 任一实例返回全集群；dashboard 集群页。
2. **覆写广播**：管理操作写 Redis 后 `PUBLISH {ns}:events`，所有实例订阅并即时应用
   （dashboard 改一次，全集群生效）；端点冷却事件同样广播，避免多个实例各撞一次 429。
3. **分布式每端点令牌桶**（Redis Lua，原子）：只在一条链跨多实例（超热链副本、或 LB 非哈希）
   时启用；只有缓存未命中流量调用（10k QPS 下约 200 rps），不进命中路径。
4. **L2 共享响应缓存**（可选）：L1 moka 未命中再查 Redis（≤1ms），跨实例去重上游请求。
5. **自路由模式**（可选，面向 K8s Service/普通轮询 LB）：实例收到不属于自己分片的链时按
   Redis 里的 peer 列表转发给 owner，代价是多一跳；分片函数与心跳集合共同决定归属。
6. pinned 分片感知：`pinned` 只对本实例分片内的链生效，避免探针 ×N。

### 容量与告警
- 每实例 `rpcrouter_in_flight_requests`、`ingress_rejected{overload}` 是扩容信号；
  `rpcrouter_chains{state="hot"}` 与探针队列深度反映分片是否失衡。

## 13. 实现偏差记录（W5，maker 回写）

- Catalog 的 `by_id` 使用 `HashMap<u64, usize>`，使目录热路径查找为 O(1)；其余目录元数据与过滤规则保持不变。
- 探针调度使用固定并发的 `JoinSet` 工作池和有界 channel，并在端点级去重；不再为每个排队项创建等待信号量的任务。
- 刷新周期通过 `ChainlistLoader::refresh()` 与手动刷新共享互斥状态；Memory/Disk/Fixture 回退不会伪造新鲜刷新时间。
- `discovery.enabled=false` 在 registry 路由层拒绝非 pinned 目录链，保持 v1 语义；deny 与 pinned 的冲突在配置校验阶段拒绝。
- 已知限制：`rpcrouter_chain_pinned` 标签数最坏为 materialized 链数；极端 `Retry-After` 仍可能触发 `Instant` 加法边界；墙钟向后跳可能延迟/集中一次 idle 降级；未知链 Classifier 仍使用默认 TTL，而配置中 1/143 有专用值；RPC 条目自身仍要求字符串或含字符串 `url` 的对象。上述项不影响 W5 的动态目录、生命周期边界与离线验收，留待后续独立加固。

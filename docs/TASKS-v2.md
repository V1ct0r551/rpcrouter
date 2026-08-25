# TASKS v2 — 动态全链目录 + 状态控制 Dashboard（dsh 执行，主会话验收）

> 统一门槛（每个工作流交付时必须全绿）：
> Rust：`cargo fmt --check` && `cargo clippy --all-targets -- -D warnings` && `cargo test`
> （本机用 `rustup override set 1.97.0`，系统默认 1.95 会被 cargo 拒绝）。
> 前端：`npm run lint` && `npm run typecheck` && `npm test` && `npm run build`。
> **测试禁止访问外网**：chainlist 用 `fixtures/`，上游行为用进程内 mock；前端测试 mock fetch。
> 实现细节以 `docs/DESIGN-v2.md` 为准（v1 部分见 `docs/DESIGN.md`），本文只定范围与验收。
> 流程：maker 在独立 worktree 分支实现 → checker 单轮对抗审查 → maker 修 must-fix →
> 主会话合入 main。提交英文 conventional 风格，粒度按逻辑步骤。

## W5 — 动态目录与链生命周期（分支 `w5-dynamic-chains`）

范围（DESIGN-v2 §1–§5、§8）：
1. `chainlist`：`parse_and_filter` 升级为解析**全部链**为 `Catalog`（保留 name/shortName/
   chain/slug/isTestnet/nativeCurrency.symbol/explorers[0].url/status/tvl、端点 `tracking`），
   过滤规则不变；`discovery.enabled=false` 时只保留 pinned 链。刷新状态（source/时间/etag/
   错误/进行中）可查询；提供手动刷新入口（供 W6 调用），与周期刷新互斥。
   默认 `refresh_seconds` 改 3600（config.toml、docker-compose、README 同步）。
2. `config`：`[discovery]`（enabled/include_testnets/deny/max_hot_chains/idle_seconds）+
   环境变量覆写；`chains` 语义升级为 pinned；`chain_overrides` 不再要求 chain_id 在
   `chains` 内；校验规则见 DESIGN-v2 §5。
3. `registry`：Catalog 持有与整体替换；ChainState 增加 pinned/disabled/last_ingress；
   `resolve_for_request`（热路径廉价、dormant 惰性 materialize）；activate/demote/pin/
   unpin/set_disabled；housekeeping（idle 降级 + LRU 上限，pinned 永不淘汰）；
   `hot_chain_ids`；summaries 增加 `state`。
4. `server`/`forward`：路由层解析链一次；未知链 404、无端点 503、禁用 403（JSON-RPC 错误体 +
   `ingress_rejected{reason}`，**不计** `user_visible_errors`）；`Classifier` 对任意链
   给默认参数。
5. `probe`：调度对象改为 hot 链；有界工作池；激活 kick；队列/在飞指标。
6. `metrics`：DESIGN-v2 §8 新指标；`ops/prometheus/alerts.yml` 的 ActiveEndpointsLow 只盯
   pinned 链；Grafana 仪表盘加「链生命周期/目录/探针」行；`docs/OPERATIONS.md` 指标字典同步。
7. fixture：`fixtures/rpcs.sample.json` 扩充（从本机 `data/rpcs.json` 裁剪，≤80KB）：
   含现有 8 链、≥2 条 testnet、≥1 条 0 端点链、≥1 条带 status 的链、带 `tracking` 的端点、
   `${KEY}` 模板与 wss 条目（用于过滤断言）。现有 `fixture_covers_every_repository_chain`
   等测试保持通过。
8. 文档：README「配置/HTTP 接口」段落更新；`docs/DESIGN-v2.md` 如有实现偏差回写说明。

验收（全部离线）：
- a) 目录解析：fixture 全部链进目录（含 0 端点链），端点过滤/去重断言，testnet 计数正确；
- b) 生命周期：dormant 链首个请求成功（进程内 mock 上游，冷启动 Probation 路径）并变 hot；
     idle 超时后降级（可注入时钟或把 idle_seconds 设为 1s 真等）；超过 max_hot_chains 时
     LRU 淘汰且 pinned 不被淘汰；再次请求可重新激活；
- c) 语义边界：未知链 → 404 + `-32000` 体，`user_visible_errors` 不增、`ingress_rejected
     {reason="unknown_chain"}` +1；0 端点链 → 503；deny 链 → 403；batch 请求同样只解析一次；
- d) 广度：**50 条链冷启动用例**——50 个进程内 mock 上游（各自返回不同 eth_chainId），
     目录 50 链均 dormant，并发各打 1 个 `eth_blockNumber`，全部成功、0 用户可见错误、
     错 chainId 的端点被剔除；
- e) 探针：dormant 链不被探测；激活后该链端点在 kick 后被立即探测；调度器在 500 个端点、
     并发 4 的情况下不堆积重复任务（断言同一端点同一时刻至多一个在飞探针）；
- f) `discovery.enabled=false` 行为等价 v1（现有全部测试不改语义通过）；
- g) 性能：`scripts/ci-smoke.sh` 通过；本机 `cargo run --release --bin loadtest`（10k×60s）
     与 loadtest-phase3.md 对比 p99 无明显退化（±20% 内），结果写进交付说明。

## W6 — Admin REST API + 运行时覆写持久化（分支 `w6-admin-api`，基于 W5 合入后的 main）

范围（DESIGN-v2 §6、§9）：
1. `[admin]` 配置（enabled/auth_token/static_dir/cors_allow_origins）+ 环境变量；
   鉴权规则：token 已配置 → `/admin/api/*` 全部 Bearer；未配置 → GET 开放、写操作 403
   `admin_disabled`；`enabled=false` → `/admin` 整体 404。CORS 仅对配置的 origin 开放。
2. 只读接口：`overview`、`chains`（过滤/搜索/排序/分页）、`chains/{id}`（含端点行）、
   `overrides`。链列表在 2877 链目录下 `?state=dormant&limit=200` 响应 < 50ms（本机）。
3. 控制接口：chainlist refresh、cache clear、链 activate/demote/pin/unpin/enable/disable、
   链 settings 覆写、端点 disable/enable/cool/reset/probe/limits/add/remove。
   端点 `limits` 运行时生效（可重建 Endpoint 对象，健康状态重置可接受）；`probe` 同步
   返回 ProbeOutcome。
4. 运行时覆写 `data/overrides.json`：结构、原子写、启动加载、叠加优先级 runtime > config >
   default；损坏文件只告警不阻塞启动。
5. 静态托管：`static_dir` 配置时 `/dashboard/` 提供 SPA（index.html fallback），无 token
   要求；目录不存在时启动告警。
6. 文档：README 新增「Admin API」章节（含 curl 示例）；OPERATIONS 新增「运行时控制」章节。

验收（离线，axum `oneshot` + 进程内 mock 上游）：
- a) 鉴权矩阵：无 token 配置时 GET 200 / POST 403；配置 token 后无头 401、错 token 401、
     对头 200；`enabled=false` 404；
- b) 每个控制接口至少一个用例断言**运行态确实变化**（如 disable 端点后 candidates 不含它；
     cool 后 state=Cooling 且流量归零；reset 后 Probation；pin 后 housekeeping 不淘汰；
     settings 覆写后 Classifier 出的 tip TTL 变化；cache clear 后下一次请求打到上游）；
- c) 覆写持久化 round-trip：写→重建 Registry 加载→生效；损坏文件不阻塞启动；
- d) overview/chains/chains/{id} 字段与 DESIGN-v2 §6 契约一致（用 serde 结构体 + 快照断言）；
- e) `static_dir` 存在时 `/dashboard/`、`/dashboard/chains/1` 均返回 index.html；三门槛全绿。

## W7 — React Dashboard（分支 `w7-dashboard`，目录 `dashboard/`，基于 W6 合入后的 main）

范围（DESIGN-v2 §7）：
1. 脚手架：Vite + React 18 + TypeScript + eslint + vitest + testing-library；`package.json`
   scripts：dev/build/preview/lint/typecheck/test；`vite.config.ts` 代理 `/admin` →
   `http://127.0.0.1:8545`（可用 `VITE_API_BASE` 覆写）；`dashboard/README.md`。
   锁文件入库（`package-lock.json`）；`.gitignore` 忽略 `dashboard/node_modules`、
   `dashboard/dist`。
2. API 客户端：一处封装 fetch（bearer 头、错误体解析、超时）；TanStack Query 轮询；
   类型与 DESIGN-v2 §6 契约一致（`src/api/types.ts`）。
3. 页面：总览（stat tiles + 5 分钟折线 + 刷新/清缓存）、链列表（搜索/过滤/排序/分页、
   行操作）、链详情（参数卡可编辑、端点表 + 操作、危险操作二次确认）、设置（token、
   轮询间隔、主题）。路由：`/dashboard/`、`/dashboard/chains/:id`、`/dashboard/settings`
   （`base: '/dashboard/'`）。
4. 视觉：DESIGN-v2 §7 的色彩与标记规范；亮/暗两套；状态永远色块 + 文字；无外链资源；
   响应式到 1024px 宽。
5. CI：`.github/workflows/ci.yml` 增加 `dashboard` job（node 22、`npm ci`、lint/typecheck/
   test/build），与 Rust job 并行。
6. 部署：`Dockerfile` 增加 node 构建阶段把 `dashboard/dist` 拷进镜像 `/app/dashboard`，
   `RPCROUTER_ADMIN_STATIC_DIR=/app/dashboard` 默认开启；docker-compose 示例同步；
   README 新增「Dashboard」章节（开发/构建/托管/鉴权）。

验收：
- a) 前端四门槛全绿；测试覆盖：token 头注入与 401 处理、链表过滤/排序、状态→颜色+文字映射、
     QPS 增量计算（两次快照）、危险操作确认；
- b) 主会话本机联调：起网关（配置 admin token）+ `npm run dev`，总览/列表/详情/控制操作
     实际生效（截图或录屏路径写进交付说明）；
- c) `docker build` 成功且镜像内 `/dashboard/` 可打开。

## 交付说明模板（maker 每轮结束时用）

```
分支/提交：...
完成项：#... 
未完成/偏离 DESIGN 的点与原因：...
门槛结果：fmt/clippy/test（用例数）/（前端四门槛）
验收对照：a) ... b) ...
性能数字（W5）：...
需要主会话决策的问题：...
```

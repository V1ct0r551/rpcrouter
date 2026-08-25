# ROADMAP P3 验收报告 —— 动态全链目录 + 状态控制 Dashboard

> 2026-08-25（立项与交付同日）。主会话做架构设计（docs/DESIGN-v2.md）与任务拆解（docs/TASKS-v2.md），
> codex（gpt-5.6-sol）实现，Claude 子代理做设计符合性评审与对抗审查（每工作流最多一轮），主会话合入。
> 本报告只记录**实测结果**；实现细节见各 merge commit 与 docs/reports/{loadtest-w5,w6-state-admin}.md。

## 交付范围

| 工作流 | 内容 | merge commit |
|---|---|---|
| W5 动态目录 + 链生命周期 | Catalog 全量解析（2887 链 / 5562 端点）、pinned/hot/dormant/disabled 生命周期、按需激活 + idle/LRU 降级、未知链 404 / 无端点 503 / 禁用 403（入口拒绝，不计 UVE）、冷启动失败单独计数、有界探针池 + 激活 kick、chainlist 1h 刷新 + 状态可观测 | `addc624` |
| W6 状态存储层 + Admin REST API | `StateStore`（Memory / File / Redis / Resilient）：结构化 key 为真相、从零初始化 / 覆盖导入 / 重置、Redis 不可达 ≤3s 降级启动且控制写 503；后台任务监督器；`/admin/api/*` 只读 + 控制接口，bearer 鉴权、静态托管防穿越、输入校验、审计；compose redis + cluster 分片 profile（横向扩展方案 A） | `79b481a` |
| W7 React Dashboard | `dashboard/`（Vite + React 18 + TS + TanStack Query + Recharts）：总览 / 链列表 / 链详情 / 设置（token、状态存储 export/import/reset）；亮暗主题；CI dashboard job；镜像内置 `/dashboard/` | 本次 merge |

审查产出：W5 评审 12 条 + checker 4 must-fix / 11 should-fix；W6 两轮 checker 共 17 条 must-fix
（含静态目录路径穿越、Redis 拒连启动阻塞 >420s、降级期写丢失、指标基数爆炸等），全部修复后合入。

## 实测结果（main）

### 1. 门槛
- Rust：`cargo fmt --check` / `clippy -D warnings` / `cargo test`（**133 用例**）/ `--ignored`（6，含 Redis 集成与本地全量 chainlist 解析）全绿。
- 前端：`npm run lint && npm run typecheck && npm test（6 用例）&& npm run build` 全绿。
- CI：Rust job（含 `services: redis`）+ dashboard job 并行。

### 2. 性能（硬指标 1：单链 10k QPS）
- W5 合入后 10k×60s：p99 **1.49 ms**；W6 合入后 **1.07–1.73 ms**（与 W5 ±20% 内）；hit+coalesce 99.995%，UVE 0，600000/600000 成功。v1 基线 4.1 ms。
- ci-smoke（1k QPS×10s）：p99 0.15 ms，UVE 0。

### 3. 真实网络广度（硬指标 4：全链）
- 目录 2887 链 / 5562 公开 https 端点全部进入（`rpcrouter_chainlist_refresh_total{source="network"}`）。
- 抽样 80 链（TVL 前 30 + 随机 50）：**47/80 成功**，首请求 p50 618 ms / p90 1075 ms，
  二次请求命中缓存 p50 0 ms；失败 33 条全部为 chainlist 内端点已死亡的链（goerli 系、discontinued、单端点僵尸链）。
- 归因：`user_visible_errors_total` **0**，死链全部落到 `cold_start_failures_total`——SLO 告警不再被数据源里的死链污染（硬指标 3 的度量保持有效）。
- 抗扫描：2000 个不存在的 chainId → 全部 404、`ingress_rejected{reason="unknown_chain"}=2000`，RSS 61.8 MB 无增长。

### 4. 状态存储与降级（W6）
- Redis 拒连：启动到 `/healthz` 0.44–1.19 s；黑洞：2.3 s；`required=true` 拒连 6 ms 明确失败。
- 空闲时 Redis 流量每 tick ≈1 KB（结构化 key，无 1MB document 往返）。
- 降级期控制写 503 且内存态不变、`GET /admin/api/state` 的 `writable=false`；恢复后以 Redis 为准重新应用覆写。
- reset 只清本命名空间；import/export 闭环（不含 catalog）；损坏本地状态文件改名 `.corrupt-*` 后按空库起。
- cluster profile 实测：链 1、56、137 各只落到一个实例（nginx 按 chainId 一致性哈希）。

### 5. Admin API 安全面（W6b）
- 鉴权：无 token 配置时 GET 开放 / 写 403；配置后缺头 / 错 token / 变体 scheme 均 401；日志与错误体不含 token。
- 静态托管：`/dashboard//etc/passwd`、`///etc/passwd`、`../`、`%2e%2e` 等向量全部 404；SPA 回退只对无扩展名路径。
- 输入校验：`rps=0` → 400 不落库；settings `null` 删除、未知字段 400；`endpoints/add` 只接受安全 https（默认拒绝私网）。
- `GET /admin/api/chains?state=all` 后 `/metrics` 行数不变（1991 → 1991）。

### 6. Dashboard（W7）
- 真实后端联调：总览（2887 链目录）、链列表搜索/过滤、链详情（78 个 endpointRows）、缓存清理生效；`writable=false` 时控制按钮禁用。
- 镜像内置：main 上 `docker build -t rpcrouter:p3 .` 成功（148 MB，含 node 构建阶段）；容器内 `/dashboard/` 200 text/html，`/admin/api/overview` 返回 2887 链目录；构建产物无外链运行时资源。

## 已知遗留（不阻塞，记入 DESIGN-v2 §13 / TASKS-v2）
- W6 should-fix S7（optional 模式 Redis schema 不一致的从零初始化）、S8（审计 before/after 细化）、S12（API reset 后立即重新 seed）为最小实现。
- overview 里 file 后端的 `state.up/writable` 为 null（`/admin/api/state` 正确）。
- Dashboard 详情页未提供 runtime 端点 remove 按钮（API 已有）。
- 24h 真实网络 soak（P2 遗留）仍待执行；多实例 Phase B（pub/sub 广播、分布式令牌桶）为 P5 备选。

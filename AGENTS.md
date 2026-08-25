# rpcrouter — 项目指南

## 这是什么

rpcrouter 是一个 Rust 实现的区块链节点 RPC 路由网关：聚合 [chainlist](https://chainlist.org)
上各链的公开 RPC 节点池，对外暴露统一的 JSON-RPC 入口（按 chainId 路由），通过池内
负载均衡、健康评分与透明失败转移，提供免 API key、无单点限流的 RPC 服务。

## 硬指标（验收标准）

1. **单链扛住 10000 QPS**——必然依赖请求去重 + 响应缓存，公开节点池只承接缓存未命中。
2. **智能摘除限频节点**——识别 429 / 各家限频错误码 / HTML 错误页 / 超时，把被限频的
   公共节点冷却摘除，恢复后自动回池。
3. **用户端无错误感知**——上游失败对调用方透明（failover / 重试 / hedging），只有全池
   耗尽才返回错误。
4. 链覆盖：**先用 ETH 和 Monad 跑通全流程**，随后铺开主流链（BSC、Polygon、Arbitrum、
   Base、OP、Avalanche 等）。

## 协作模式（ccteam 三角分工）

- **主会话（Claude）**：仓库治理——方案制定、任务拆解、评审把关、提交管理。
  不亲自写大规模实现代码，控制自身上下文膨胀。
- **Grok**：调研——开源项目复用评估、chainlist 数据源、限频行为盘点等。产出落到
  `docs/research/`。
- **Codex / dsh**：实现——按 `docs/TASKS*.md` 的阶段任务开发，交付可编译、带测试的代码。
  P1 起实际用 ccteam 调度 dsh（maker=xy/deepseek-v4-pro，checker=xy/deepseek-v4-flash），
  maker 在独立 git worktree 分支开发，checker 单轮对抗审查，主会话合入。

全程自主推进，不向用户中途提问。

## 关键文档

- `docs/research/` — Grok 调研产出（只读参考）
- `docs/DESIGN.md` — 架构方案（主会话维护）
- `docs/TASKS.md` — v1 阶段任务拆解与验收标准（已完成，存档）
- `docs/ROADMAP.md` — v1 后规划：部署 / 生产可用 / 动态全链+Dashboard / 命名链路由（P1–P5）
- `docs/DESIGN-v2.md` / `docs/TASKS-v2.md` — P3 动态全链目录 + 状态控制 Dashboard 的方案与任务拆解

## 技术与工程约定

- Rust edition 2024（工具链 1.97+）；async 栈用 tokio。
- 提交门槛：`cargo fmt --check` && `cargo clippy -- -D warnings` && `cargo test` 全绿。
- 单测放各模块 `#[cfg(test)]`；**测试禁止访问外网**——chainlist/节点数据用内置样例，
  上游行为用本地 mock；压测也用本地 mock 上游。
- 日志与对外错误消息用英文（便于检索），代码注释与文档用中文。
- 公开节点质量参差（限频、2xx 返回 HTML、区块滞后、假数据）：写任何上游交互逻辑时
  默认**上游不可信**。
- 不做规避第三方服务条款的事（伪装 UA 绕封锁、对单一节点激进重试等）；对单个公共
  端点设并发/频率上限，遇 429 退避换节点而不是硬打。

## 当前状态

- [x] 仓库治理初始化（本文件、git）
- [x] Grok 调研：开源复用评估 / chainlist 数据方案（docs/research/）
- [x] DESIGN.md / TASKS.md
- [x] Codex 分阶段实现 Phase 1–4（含冷启动 Probation 兜底修复）
- [x] 验收：8 链真实 E2E；10k QPS 压测双跑验证（docs/reports/loadtest-phase3.md）；
      429 摘除/回池时间线复现；`user_visible_errors == 0`

v1 已交付（2026-07-26 验收）。后续任务规划统一沉淀在 `docs/ROADMAP.md`。

- [x] ROADMAP P1 部署 + P2 生产可用（2026-08-20 验收，maker/checker 对抗流程交付；
      真实环境验收见 `docs/reports/prod-readiness.md`：docker 8 链 smoke、加固特性实测、
      监控栈实跑、CI 首跑绿灯、30 分钟真实网络 soak 0 错误）。遗留：24h soak。
- [ ] ROADMAP P3 动态全链目录 + 状态控制 Dashboard（2026-08-25 立项；W5 → W6 → W7 串行
      推进，见 docs/TASKS-v2.md）。前端为独立 React 工程 `dashboard/`，前端门槛：
      `npm run lint && npm run typecheck && npm test && npm run build`。
  - [x] W5 动态目录 + 链生命周期（2026-08-25 合入：目录 2887 链 / 5562 端点，pinned/hot/dormant/
        disabled 生命周期，未知链 404 / 无端点 503 / 禁用 403 不计 UVE，冷启动失败单独计数，
        有界探针池；10k 压测 p99 1.5ms、UVE 0，见 docs/reports/loadtest-w5.md）。
  - [ ] W6a Redis 状态存储层 → W6b Admin REST API → W7 React dashboard。

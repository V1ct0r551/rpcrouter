# W5 动态目录与生命周期压测记录

## 离线压测

使用 release 构建、进程内 mock 上游、10,000 请求并发调度 60 秒：

- achieved_qps：9,999.699
- p50：0.169ms
- p99：1.494ms
- hit + coalesce：99.9948%
- user_visible_errors：0

与 `docs/reports/loadtest-phase3.md` 的基线（约 9,999.8 QPS、p99 约 4.1ms、命中/折叠 99.9948%、UVE 0）相比，吞吐与错误率保持，尾延迟更低。

## 真实 chainlist smoke

release 二进制读取真实 `data/rpcs.json`/network chainlist：2,887 条链、5,562 个端点进入目录，72 条链按需激活，48 条成功，32 条因目录中已死亡的 1–2 个端点耗尽失败；失败均为数据现实（Goerli、MXCdiscontinued、僵尸链等）。首请求 p50 534ms、p90 1.2s，二次缓存命中 0ms，RSS 59MB，探针队列 0。


## 主会话合并后验收（main `addc624`，2026-08-25）

- 门槛：`cargo fmt --check` / `cargo clippy --all-targets -- -D warnings` / `cargo test`（112）/
  `cargo test -- --ignored`（3，含本地全量 chainlist 解析：2887 链 / 5562 端点 / 0 跳过）全绿。
- 真实网络广度 smoke（`scripts/multichain-smoke.sh`，TVL 前 30 + 随机 50，release 二进制）：
  目录 2887 链 / 5562 端点（source=network）；72 条链按需激活；**47/80 成功**
  （TVL 前 30 里 25/30），首请求 p50 618 ms / p90 1075 ms，
  二次请求命中缓存 p50 0 ms；失败 33 条全部为 chainlist 内端点已死亡的链
  （goerli 系测试网、discontinued、单端点僵尸链）。
- 归因验证：`rpcrouter_user_visible_errors_total` **0 条序列**，`rpcrouter_cold_start_failures_total` 33 条——
  死链不再污染上游承诺指标。
- 抗扫描验证：连续请求 2000 个不存在的 chainId → 全部 404、`ingress_rejected{reason="unknown_chain"}=2000`，
  进程 RSS 61.8 MB 无增长（checker M1 修复生效）。
- 遗留：checker S5（后台任务监督/自动重启）并入 W6a；N4–N7 记录于 DESIGN-v2 §13。

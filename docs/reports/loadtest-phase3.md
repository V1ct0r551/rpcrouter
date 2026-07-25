# Phase 3 本机压测报告

日期：2026-07-25（US/Pacific）。命令：`scripts/loadtest.sh`。

## 环境与方法

- 主机：`VirtualMac2,1`，Apple ARM64 虚拟机，8 CPU；Darwin 25.5.0；文件描述符上限 256。
- 工具链：Rust 1.97.0；`rpcrouter` 与压测器均使用 `--release`。
- 本机未安装 oha/vegeta/wrk/hey，因此使用仓库内 `loadtest` 异步客户端：64 个定时 worker，
  以 10,000 QPS 调度 60 秒，共 600,000 个真实 HTTP JSON-RPC 请求。
- 两个进程内 `mock-upstream` 均附加 5ms 延迟，端点配置上限为 15 rps、并发 8。
- 请求为 `eth_blockNumber`；第 10 秒向优先端点注入 HTTP 429，第 20 秒解除。
  压测探针固定使用合法抖动范围下界 15 秒，以便在 60 秒窗口内观察完整回池。
- mock 以自然 1 秒窗记录每端点峰值；原始 JSON 保存在被 git 忽略的
  `data/loadtest-phase3.json`，本报告记录同一次成功运行的结果。

## 结果

| 指标 | 实测 | 验收 | 结论 |
|---|---:|---:|---|
| 成功请求 | 600,000 / 600,000 | 无用户错误 | 通过 |
| 实际吞吐 | 9,999.836 QPS | 10,000 QPS 档 | 通过 |
| p50 / p99 | 0.218ms / 4.126ms | p99 ≤ 50ms | 通过 |
| cache hit | 99.6845% | — | 通过 |
| miss 中折叠 | 98.4152% | — | 通过 |
| hit + fold / ingress | 99.9948% | ≥ 98% | 通过 |
| 数据面上游请求 | 32 | 公开池仅承接 miss | 通过 |
| `user_visible_errors` | 0 | 0 | 通过 |

端点侧证据：storm 端点共 12 请求、峰值 3 QPS；healthy 端点共 34 请求、峰值
3 QPS；两者均未突破配置的 15 rps。统计包含健康探针，因此比数据面上游请求总数多。

## 429 摘除与回池时间线

| 相对时间 | 事件 |
|---:|---|
| 10.001s | 开启 HTTP 429 风暴 |
| 10.051s | 端点进入 `Cooling{strikes=1}`，后续用户流量归零 |
| 20.001s | mock 停止返回 429 |
| 40.039s | 冷却到期，进入 Probation |
| 40.141s | 第一次完整探针通过（passes=1） |
| 55.174s | 第二次完整探针通过，恢复 Active |
| 60.001s | 压测结束；最终状态 Active |

整个风暴、摘除与回池期间 600,000 个调用全部成功，`user_visible_errors == 0`。

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

#!/usr/bin/env bash
# 压测冒烟脚本（ROADMAP P2.4）：在 CI / 本机跑一档低 QPS 离线压测，
# 断言“用户可见错误 == 0”且“p99 低于宽松阈值”，失败时非零退出并打印摘要。
#
# 设计取舍：
#   - 复用 src/bin/loadtest.rs：它内部用 rpcrouter::mock_upstream 在进程内起 mock 上游
#     （spawn_mock），并输出完整 JSON 报告。因此本脚本只驱动 loadtest 一个二进制即可
#     覆盖“mock 上游 + 压测器”两个 bin 的冒烟（standalone 的 mock-upstream 二进制
#     用于手工调试上游行为，loadtest 不接收外部上游地址，二者不直接拼装）。
#   - 用 --no-storm：冒烟只验证常态转发热路径，不做 429 摘除风暴，缩短时长。
#   - loadtest 自带的严格验收（p99<=50ms 等）面向正式 10k 档；本脚本另用更宽松的
#     p99 阈值（默认 200ms，参考 loadtest-phase3.md 本机 10k 档实测 p99=4.126ms，
#     留足余量防 CI 机器抖动误报）自行裁决，并把 loadtest 的退出码只当作参考。
#   - 全离线：loadtest 用进程内 mock 上游 + 内存 cache，不触任何外网。

set -u

# 可调参数（env 覆盖），给出与 loadtest 默认一致的语义。
QPS="${QPS:-1000}"
DURATION="${DURATION:-10}"
CONCURRENCY="${CONCURRENCY:-32}"
# p99 宽松阈值（毫秒）：足够宽松防 CI 抖动误报，仍能拦灾难性性能回归。
P99_THRESHOLD_MS="${P99_THRESHOLD_MS:-200}"

# 优先直接执行已构建的 release 二进制（CI 已先 cargo build --release）；
# 否则退回 cargo run（本机直接跑脚本时用）。
LOADTEST_BIN="$(pwd)/target/release/loadtest"
if [[ -x "$LOADTEST_BIN" ]]; then
    RUN_CMD=("$LOADTEST_BIN")
else
    RUN_CMD=(cargo run --release --bin loadtest --)
fi

echo "== ci-smoke: running offline load smoke (qps=${QPS}, duration=${DURATION}s, concurrency=${CONCURRENCY})"

# loadtest 输出 JSON 报告到 stdout；其退出码我们不作为唯一裁决依据，
# 因为自带阈值比冒烟更严格（见文件头注释）。仍保存 JSON 供解析。
REPORT_JSON="$(mktemp)"
# 不用 set -e 覆盖这条命令：允许 loadtest 以非零退出，由下方 python 裁决。
if ! "${RUN_CMD[@]}" \
    --no-storm \
    --qps "${QPS}" \
    --duration "${DURATION}" \
    --concurrency "${CONCURRENCY}" >"$REPORT_JSON" 2> >(sed 's/^/    /' >&2); then
    echo "    (loadtest exited non-zero — its built-in strict acceptance (p99<=50ms, hit-rate>=98%, endpoint rps limit) is for the full 10k benchmark; smoke verdict below re-judges with the loose CI thresholds)"
fi

# 用 python3 解析 JSON 并断言；python3 在 ubuntu-latest 与常见本机均可用。
# 捕获 python 退出码，脚本以它为准退出（见文件末尾 exit，防止被清理命令覆盖）。
python3 - "$REPORT_JSON" "${P99_THRESHOLD_MS}" <<'PY'
import json
import sys

report_path, threshold_ms = sys.argv[1], float(sys.argv[2])
try:
    with open(report_path, encoding="utf-8") as fh:
        report = json.load(fh)
except (OSError, ValueError) as error:
    print(f"FAIL: could not parse loadtest JSON report: {error}")
    sys.exit(1)

qps = report.get("achieved_qps", 0.0)
p99 = report.get("p99_ms", 0.0)
user_errors = report.get("user_visible_errors", -1)
failed = report.get("failed_requests", -1)
scheduled = report.get("scheduled_requests", 0)

print("== ci-smoke summary ==")
print(f"  achieved_qps      : {qps:.1f}")
print(f"  p99_ms            : {p99:.3f}")
print(f"  user_visible_errors: {user_errors}")
print(f"  failed_requests   : {failed}")
print(f"  scheduled_requests: {scheduled}")
print(f"  p99 threshold     : {threshold_ms:.0f} ms")

ok = True
if user_errors != 0:
    print("FAIL: user_visible_errors must be 0")
    ok = False
if p99 > threshold_ms:
    print(f"FAIL: p99 {p99:.1f} ms exceeds threshold {threshold_ms:.0f} ms")
    ok = False
if failed != 0:
    print("FAIL: failed_requests must be 0")
    ok = False
requested = report.get("requested_qps", 0)
if qps < requested / 2:
    print(f"FAIL: achieved_qps {qps:.1f} is below half of requested {requested}; load did not actually run")
    ok = False

if ok:
    print("PASS: offline load smoke acceptance criteria met")
    sys.exit(0)
sys.exit(1)
PY
VERDICT=$?

# 清理临时文件；清理命令的退出码不能覆盖冒烟裁决，显式以 python 退出码退出。
rm -f "$REPORT_JSON"
exit "$VERDICT"

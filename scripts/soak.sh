#!/usr/bin/env bash
#
# rpcrouter 低 QPS（≤5）真实网络长跑脚本（soak test）。
#
# 目的：在接近真实网络、低负载下长时间观察网关的端点摘除/回池分布、缓存行为与内存曲线，
# 对应 ROADMAP P2.5。与 loadtest.sh 的高压压测互补——soak 关注长时间稳定性而非峰值吞吐。
#
# 用法：
#   scripts/soak.sh [--url BASE] [--duration SECS] [--qps N] [--chains LIST]
#                   [--pid GATEWAY_PID] [--interval SECS] [--method METHOD] [--out DIR]
#
# 默认指向已运行的网关实例（http://127.0.0.1:8545，config.toml 默认监听）。
# 使用 `--pid <网关进程 PID>` 可让脚本采样网关进程 RSS 以绘制内存曲线；省略时尝试 pgrep。
#
# 产出（写入 --out 目录）：
#   metrics-<ts>.txt   每次采样的原始 /metrics 快照（可直接 grep 核对指标名）
#   rss.csv            内存曲线数据：timestamp,rss_kib
#   events.csv         端点冷却事件增量（摘除）与回池（active）变化
#   summary.json       结束时汇总：QPS 实况、错误计数、摘除/回池事件、内存曲线摘要
set -euo pipefail

URL="http://127.0.0.1:8545"
DURATION=3600
QPS=5
CHAINS="1,143,56"
PID=""
INTERVAL=15
METHOD="eth_blockNumber"
OUT="data/soak"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --url) URL="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --qps) QPS="$2"; shift 2 ;;
    --chains) CHAINS="$2"; shift 2 ;;
    --pid) PID="$2"; shift 2 ;;
    --interval) INTERVAL="$2"; shift 2 ;;
    --method) METHOD="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

# QPS 上限约束：soak 必须低负载（≤5），超限视为配置错误。
if [[ "$QPS" -lt 1 || "$QPS" -gt 5 ]]; then
  echo "error: --qps 必须在 1..=5（soak 为低负载长跑，QPS=$QPS 超出范围）" >&2
  exit 2
fi
if [[ "$DURATION" -lt 1 ]]; then
  echo "error: --duration 必须为正数" >&2
  exit 2
fi
if [[ "$INTERVAL" -lt 1 ]]; then
  echo "error: --interval 必须为正数" >&2
  exit 2
fi

# 若未显式给 PID，尝试定位正在运行的网关进程（默认二进制名 rpcrouter）。
if [[ -z "$PID" ]]; then
  PID="$(pgrep -f 'rpcrouter$' | head -n1 || true)"
fi

IFS=',' read -r -a CHAIN_ARRAY <<< "$CHAINS"
if [[ "${#CHAIN_ARRAY[@]}" -lt 1 ]]; then
  echo "error: --chains 至少需要一个 chain_id" >&2
  exit 2
fi

mkdir -p "$OUT"
TS0="$(date +%s)"
START_S=$(date +%s)
END_S=$((START_S + DURATION))
CHAIN_COUNT=${#CHAIN_ARRAY[@]}

echo "soak starting: url=$URL duration=${DURATION}s qps=$QPS chains=$CHAINS pid=${PID:-none} out=$OUT"
echo "target method: $METHOD"

# ---- 请求发送器：以目标 QPS 匀速打真实 HTTP 请求，round-robin 分发到各链 ----
# 每个链的请求计数与错误计数用关联数组累计。
# ---- 请求驱动：以目标 QPS 匀速打真实 HTTP 请求，round-robin 分发到各链 ----
# 请求计数不在此处统计：curl 在后台子 shell 中运行，数组改动无法回传父 shell。
# 改为由网关 /metrics 快照（rpcrouter_chain_ingress_requests_total 等）在末尾计算权威计数。
send_burst() {
  local chain_id i sent body req_id
  sent=0
  # 每 1 秒发 QPS 个请求；用 sleep 逼近匀速。
  while true; do
    for i in $(seq 1 "$QPS"); do
      chain_id="${CHAIN_ARRAY[ $(( (i - 1 + sent) % CHAIN_COUNT )) ]}"
      req_id="${TS0}-${sent}-${i}"
      body="{\"jsonrpc\":\"2.0\",\"id\":\"$req_id\",\"method\":\"$METHOD\",\"params\":[]}"
      curl -s -o /dev/null --max-time 20 -X POST "$URL/rpc/$chain_id" \
        -H 'Content-Type: application/json' -d "$body" || true
      sent=$((sent + 1))
    done
    # 每轮 1 秒节奏
    sleep 1
  done
}

# ---- 采样器：周期抓 /metrics 快照 + 进程 RSS ----
# 端点事件检测：对比相邻快照的 rpcrouter_endpoint_state 单热 gauge。
#   端点 active=1 -> active=0（转为 cooling/probation）= 摘除（removal）
#   端点 active=0 -> active=1（回池）                       = 回池（return_to_pool）
# 事件写入 events.csv，格式：ts,chain_id,event,endpoint
last_metrics=""
declare -A PREV_ACTIVE=()   # "chain endpoint" -> 1 表示上一快照 active
has_prev_active=0           # 是否已采样到上一份 active 集合
removal_events=0
return_events=0
rss_rows=0
sampled=0
reject_before=""
uve_before=""

sample_once() {
  local ts metrics rss kb f
  ts="$(date +%s)"
  metrics="$OUT/metrics-${ts}.txt"
  # 抓 /metrics 快照（可能带鉴权；默认未鉴权）
  if ! curl -s --max-time 20 "$URL/metrics" -o "$metrics"; then
    echo "  warning: /metrics 抓取失败 (ts=$ts)" >&2
  fi
  # 采样进程 RSS（从 /proc/<pid>/status 的 VmRSS，单位 kB）
  if [[ -n "$PID" && -r "/proc/$PID/status" ]]; then
    kb="$(awk '/^VmRSS:/{print $2}' "/proc/$PID/status" || true)"
    if [[ -n "$kb" ]]; then
      echo "$ts,$kb" >> "$OUT/rss.csv"
      rss_rows=$((rss_rows + 1))
    fi
  fi

  # 端点摘除/回池事件检测：基于 rpcrouter_endpoint_state 单热 gauge。
  # 指标名形如：rpcrouter_endpoint_state{chain_id="1",endpoint="http://...",state="active"} 1
  if [[ -f "$metrics" && -s "$metrics" ]]; then
    declare -A CUR_ACTIVE=()
    while IFS= read -r line; do
      [[ "$line" == rpcrouter_endpoint_state* ]] || continue
      [[ "$line" == *state=\"active\"* ]] || continue
      # 解析 chain_id 与 endpoint 标签
      local chain ep val
      chain="$(echo "$line" | sed -n 's/.*chain_id="\([^"]*\)".*/\1/p')"
      ep="$(echo "$line" | sed -n 's/.*endpoint="\([^"]*\)".*/\1/p')"
      [[ -z "$chain" || -z "$ep" ]] && continue
      CUR_ACTIVE["$chain"$'\x1f'"$ep"]=1
    done < "$metrics"

    # 与上一快照对比：新 active（回池）与消失的 active（摘除）
    if [[ "$has_prev_active" -eq 1 ]]; then
      for key in "${!PREV_ACTIVE[@]}"; do
        if [[ -z "${CUR_ACTIVE[$key]:-}" ]]; then
          local chain ep
          chain="${key%%$'\x1f'*}"
          ep="${key#*$'\x1f'}"
          echo "$ts,$chain,removal,$ep" >> "$OUT/events.csv"
          removal_events=$((removal_events + 1))
        fi
      done
      for key in "${!CUR_ACTIVE[@]}"; do
        if [[ -z "${PREV_ACTIVE[$key]:-}" ]]; then
          local chain ep
          chain="${key%%$'\x1f'*}"
          ep="${key#*$'\x1f'}"
          echo "$ts,$chain,return_to_pool,$ep" >> "$OUT/events.csv"
          return_events=$((return_events + 1))
        fi
      done
    fi
    PREV_ACTIVE=()
    for key in "${!CUR_ACTIVE[@]}"; do PREV_ACTIVE["$key"]=1; done
    has_prev_active=1
  fi

  sampled=$((sampled + 1))
  last_metrics="$metrics"
}

# 用聚合 429 计数与 user_visible_errors 计数做事件增量统计。
# 用单个 awk 直接读文件，避免 pipefail 下 grep 无匹配的退出码干扰；awk 恒输出恰一行。
total_rejections() {
  local f="${1:-$last_metrics}"
  awk -F' ' '/^rpcrouter_ingress_rejected_total/{s+=$NF} END{print s+0}' "$f" 2>/dev/null || echo 0
}
total_uve() {
  local f="${1:-$last_metrics}"
  awk -F' ' '/^rpcrouter_user_visible_errors_total/{s+=$NF} END{print s+0}' "$f" 2>/dev/null || echo 0
}
# 取某链在某个 /metrics 快照里的入口请求累计（rpcrouter_chain_ingress_requests_total）。
chain_ingress_in() {
  local chain_id="$1" f="${2:-$last_metrics}"
  awk -F' ' -v c="$chain_id" \
    '/^rpcrouter_chain_ingress_requests_total\{chain_id="/{
       line=$0; if (line ~ "chain_id=\"" c "\"") { n=$NF }
     }
     END{print n+0}' "$f" 2>/dev/null || echo 0
}
# 取某链在某快照里的用户可见错误累计（rpcrouter_user_visible_errors_total）。
chain_uve_in() {
  local chain_id="$1" f="${2:-$last_metrics}"
  awk -F' ' -v c="$chain_id" \
    '/^rpcrouter_user_visible_errors_total\{chain_id="/{
       line=$0; if (line ~ "chain_id=\"" c "\"") { n=$NF }
     }
     END{print n+0}' "$f" 2>/dev/null || echo 0
}
# 记录基线/最后的入口与错误累计，用于计算权威 delta。
declare -A CHAIN_INGRESS_BASELINE=()
declare -A CHAIN_INGRESS_FINAL=()
declare -A CHAIN_UVE_BASELINE=()
declare -A CHAIN_UVE_FINAL=()

# 预热一次，取得基线（累计计数）。
sample_once
reject_before="$(total_rejections)"
uve_before="$(total_uve)"
for c in "${CHAIN_ARRAY[@]}"; do
  CHAIN_INGRESS_BASELINE[$c]="$(chain_ingress_in "$c" "$last_metrics")"
  CHAIN_UVE_BASELINE[$c]="$(chain_uve_in "$c" "$last_metrics")"
done
echo "  baseline: ingress_rejected=$reject_before user_visible_errors=$uve_before"

# 启动请求发送器（后台）与采样循环（前台主循环）。
send_burst &
BURST_PID=$!
trap 'kill $BURST_PID 2>/dev/null || true' EXIT

while [[ "$(date +%s)" -lt "$END_S" ]]; do
  sample_once
  sleep "$INTERVAL"
done

kill "$BURST_PID" 2>/dev/null || true
wait "$BURST_PID" 2>/dev/null || true
# 最后再采样一次收尾。
sample_once

# ---- 汇总 ----
elapsed=$(( $(date +%s) - START_S ))
[[ "$elapsed" -lt 1 ]] && elapsed=1

reject_after="$(total_rejections)"
uve_after="$(total_uve)"
reject_delta=$(( ${reject_after:-0} - ${reject_before:-0} ))
[[ "$reject_delta" -lt 0 ]] && reject_delta=0
uve_delta=$(( ${uve_after:-0} - ${uve_before:-0} ))
[[ "$uve_delta" -lt 0 ]] && uve_delta=0

# 权威请求计数：以网关 /metrics 的入口累计 delta 为准（后台 curl 子 shell 无法回传计数）。
total_ok=0
declare -A CHAIN_OK_DELTA=()
declare -A CHAIN_ERR_DELTA=()
for c in "${CHAIN_ARRAY[@]}"; do
  final_in="$(chain_ingress_in "$c" "$last_metrics")"
  final_uv="$(chain_uve_in "$c" "$last_metrics")"
  ok=$(( ${final_in:-0} - ${CHAIN_INGRESS_BASELINE[$c]:-0} ))
  err=$(( ${final_uv:-0} - ${CHAIN_UVE_BASELINE[$c]:-0} ))
  [[ "$ok" -lt 0 ]] && ok=0
  [[ "$err" -lt 0 ]] && err=0
  CHAIN_OK_DELTA[$c]="$ok"
  CHAIN_ERR_DELTA[$c]="$err"
  total_ok=$((total_ok + ok))
done
total_err=$uve_delta
total_reject=$reject_delta

# 内存曲线摘要
mem_min=""; mem_max=""; mem_last=""; mem_rows=0
if [[ -s "$OUT/rss.csv" ]]; then
  mem_rows="$(wc -l < "$OUT/rss.csv")"
  mem_min="$(awk -F, 'NR==1{min=$2} $2<min{min=$2} END{print min}' "$OUT/rss.csv")"
  mem_max="$(awk -F, 'NR==1{max=$2} $2>max{max=$2} END{print max}' "$OUT/rss.csv")"
  mem_last="$(tail -n1 "$OUT/rss.csv" | cut -d, -f2)"
fi

# 写出 JSON 汇总（构建 per-chain 对象时去掉尾逗号，保证合法 JSON）
chain_ok_entries=""
chain_err_entries=""
for c in "${CHAIN_ARRAY[@]}"; do
  chain_ok_entries+="\"$c\": ${CHAIN_OK_DELTA[$c]:-0},"
  chain_err_entries+="\"$c\": ${CHAIN_ERR_DELTA[$c]:-0},"
done
chain_ok_entries="${chain_ok_entries%,}"
chain_err_entries="${chain_err_entries%,}"

cat > "$OUT/summary.json" <<EOF
{
  "url": "$URL",
  "duration_seconds": $DURATION,
  "elapsed_seconds": $elapsed,
  "target_qps": $QPS,
  "chains": "$CHAINS",
  "method": "$METHOD",
  "pid": ${PID:-null},
  "request_totals": {
    "ok": $total_ok,
    "error": $total_err,
    "ingress_rejected_http": $total_reject,
    "success_rate": $(awk -v a="$total_ok" -v b="$total_err" -v c="$total_reject" 'BEGIN{if(a+b+c==0)print 0; else print a/(a+b+c)}')
  },
  "metrics_deltas": {
    "ingress_rejected_total": $reject_delta,
    "user_visible_errors_total": $uve_delta
  },
  "endpoint_events": {
    "removal_count": $removal_events,
    "return_to_pool_count": $return_events
  },
  "memory_curve_kib": {
    "rows": $mem_rows,
    "min": ${mem_min:-null},
    "max": ${mem_max:-null},
    "last": ${mem_last:-null}
  },
  "snapshots_taken": $sampled,
  "per_chain_ok": { $chain_ok_entries },
  "per_chain_error": { $chain_err_entries }
}
EOF

echo "---- soak summary ----"
echo "elapsed=${elapsed}s ok=$total_ok err=$total_err reject=$total_reject"
echo "metrics deltas: user_visible_errors=+$uve_delta ingress_rejected=+$reject_delta"
echo "endpoint events: removal=$removal_events return_to_pool=$return_events (see $OUT/events.csv)"
echo "memory curve rows=$mem_rows min=${mem_min:-n/a} max=${mem_max:-n/a} last=${mem_last:-n/a} (KiB)"
echo "snapshots: $sampled  output dir: $OUT"
echo "detail JSON: $OUT/summary.json"

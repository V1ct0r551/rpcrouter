#!/usr/bin/env bash
# 多链广度 smoke（ROADMAP P3 验收工具，真实网络，不进 CI）：
# 从 chainlist 全量数据里抽样链（tvl 前 N + 随机 M），对运行中的网关逐链打 eth_blockNumber，
# 统计成功率、首个请求（冷激活）耗时与二次请求耗时，列出失败链与原因。
#
# 用法：scripts/multichain-smoke.sh [GATEWAY=http://127.0.0.1:8545] [TOP=40] [RANDOM_N=40] [RPCS=data/rpcs.json]
# 判定：结果 JSON 打到 stdout（summary + rows），退出码 0；本脚本不设硬阈值——
# 公开池质量参差，由验收人结合「有可用端点的链」成功率与失败原因裁决。
set -u
GATEWAY="${GATEWAY:-${1:-http://127.0.0.1:8545}}"
TOP="${TOP:-${2:-40}}"
RANDOM_N="${RANDOM_N:-${3:-40}}"
RPCS="${RPCS:-${4:-data/rpcs.json}}"
CONCURRENCY="${CONCURRENCY:-8}"
TIMEOUT="${TIMEOUT:-20}"
SEED="${SEED:-42}"

python3 - "$GATEWAY" "$TOP" "$RANDOM_N" "$RPCS" "$CONCURRENCY" "$TIMEOUT" "$SEED" <<'PY'
import json, random, sys, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor

gateway, top, rand_n, rpcs_path, conc, timeout, seed = sys.argv[1:8]
top, rand_n, conc, timeout, seed = int(top), int(rand_n), int(conc), float(timeout), int(seed)

chains = json.load(open(rpcs_path, encoding="utf-8"))
def public_https(ch):
    out = []
    for r in ch.get("rpc", []):
        u = r["url"] if isinstance(r, dict) else r
        if u.startswith("https://") and "${" not in u:
            out.append(u)
    return out
usable = [c for c in chains if public_https(c)]
usable.sort(key=lambda c: -(c.get("tvl") or 0))
sample = usable[:top]
rng = random.Random(seed)
rest = usable[top:]
sample += rng.sample(rest, min(rand_n, len(rest)))

def call(chain_id, rid):
    body = json.dumps({"jsonrpc": "2.0", "id": rid, "method": "eth_blockNumber", "params": []}).encode()
    req = urllib.request.Request(f"{gateway}/rpc/{chain_id}", data=body, headers={"content-type": "application/json"})
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status, payload = resp.status, resp.read()
    except urllib.error.HTTPError as e:
        status, payload = e.code, e.read()
    except Exception as e:  # 超时/连接错误
        return {"ok": False, "status": None, "ms": round((time.perf_counter() - t0) * 1000), "error": type(e).__name__}
    ms = round((time.perf_counter() - t0) * 1000)
    try:
        v = json.loads(payload)
    except Exception:
        return {"ok": False, "status": status, "ms": ms, "error": "non-json body"}
    if status == 200 and isinstance(v.get("result"), str) and v["result"].startswith("0x"):
        return {"ok": True, "status": status, "ms": ms, "height": int(v["result"], 16)}
    err = (v.get("error") or {}).get("message") if isinstance(v, dict) else None
    return {"ok": False, "status": status, "ms": ms, "error": err or "unexpected response"}

def probe_chain(ch):
    cid = ch["chainId"]
    first = call(cid, 1)
    second = call(cid, 2) if first["ok"] else None
    return {"chainId": cid, "name": ch.get("name"), "shortName": ch.get("shortName"),
            "isTestnet": bool(ch.get("isTestnet")), "publicEndpoints": len(public_https(ch)),
            "first": first, "second": second}

started = time.time()
with ThreadPoolExecutor(max_workers=conc) as pool:
    rows = list(pool.map(probe_chain, sample))
ok = [r for r in rows if r["first"]["ok"]]
fail = [r for r in rows if not r["first"]["ok"]]
def pct(xs, p):
    if not xs: return None
    xs = sorted(xs); return xs[min(len(xs) - 1, int(round(p * (len(xs) - 1))))]
first_ms = [r["first"]["ms"] for r in ok]
second_ms = [r["second"]["ms"] for r in ok if r["second"]]
by_reason = {}
for r in fail:
    key = f'{r["first"]["status"]}:{r["first"]["error"]}'
    by_reason[key] = by_reason.get(key, 0) + 1
summary = {
    "gateway": gateway, "sampled": len(rows), "top_by_tvl": top, "random": rand_n,
    "ok": len(ok), "failed": len(fail), "success_ratio": round(len(ok) / max(1, len(rows)), 4),
    "first_request_ms_p50": pct(first_ms, .5), "first_request_ms_p90": pct(first_ms, .9),
    "second_request_ms_p50": pct(second_ms, .5), "second_request_ms_p90": pct(second_ms, .9),
    "failures_by_reason": by_reason, "elapsed_s": round(time.time() - started, 1),
}
print(json.dumps({"summary": summary, "rows": rows}, ensure_ascii=False, indent=1))
print(f"== multichain-smoke: {len(ok)}/{len(rows)} chains ok ({summary['success_ratio']*100:.1f}%), "
      f"first p50 {summary['first_request_ms_p50']} ms / p90 {summary['first_request_ms_p90']} ms, "
      f"second p50 {summary['second_request_ms_p50']} ms", file=sys.stderr)
for r in fail[:30]:
    print(f"   FAIL chain {r['chainId']} ({r['shortName']}, {r['publicEndpoints']} eps): "
          f"HTTP {r['first']['status']} {r['first']['error']}", file=sys.stderr)
PY

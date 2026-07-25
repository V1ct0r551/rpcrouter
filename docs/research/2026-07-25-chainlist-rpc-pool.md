# chainlist / 公开 RPC 调研（2026-07-25）

标注：**【测】**本机实测 · **【官】**官方文档 · **【估】**经验估算。

## 结论

1. **主选**：`https://chainlist.org/rpcs.json`（DefiLlama 官方 API，CORS `*`，**【测】2,090,615 B**）。
2. **备选**：`https://chainid.network/chains.json`（ethereum-lists，**【测】1.1 MB**，RPC 少）± 自维护列表；`extraRpcs.js` 仅作 diff。
3. **ETH 可先跑通**；**Monad mainnet 已上线，chainId=143**；官方公开 RPC 明确 **15–25 rps**。
4. **单链 10k QPS 必须缓存/去重**；裸打公共池不可行。

---

## 1. chainlist 数据入口

| 源 | URL | 大小【测】 |
|---|---|---|
| **主选** | https://chainlist.org/rpcs.json | 2.09 MB |
| 备选 A | https://chainid.network/chains.json | 1.14 MB |
| 备选 B | https://raw.githubusercontent.com/DefiLlama/chainlist/main/constants/extraRpcs.js | 312 KB（JS） |
| 仓库 | https://github.com/DefiLlama/chainlist | README 明示 API=`/rpcs.json` |
| 基础链目录 | https://github.com/ethereum-lists/chains | 身份源，RPC 常含 API key 模板 |

**关系**：ethereum-lists 提供链身份 + 少量 rpc；DefiLlama `extraRpcs.js` 增补大量公共端点与 `tracking`；**rpcs.json = 合并产物**（另有 `tvl`/`isTestnet`/`chainSlug` 等站点字段）。

**Schema【测】**：顶层 `list` 共 **2821** 链；必有 `name,chain,rpc,nativeCurrency,shortName,chainId,isTestnet`；常见 `networkId,infoURL,faucets,explorers,icon,tvl,features,slip44,parent,chainSlug,status,title,redFlags,ens`。`rpc[]` 元素为 `{url, tracking?}`，`tracking`∈`none|yes|limited|unspecified|no`（约 76% 条目无 tracking，来自 lists 侧）。API **无** `trackingDetails`（仅源码 extraRpcs 有）。全局 **5663** 条 rpc。

**更新/限流**：无官方 SLA；【估】网关 **6–24h** 拉一次 + ETag。5 次连拉均 **200**，未见 429；Cloudflare 托管、`Access-Control-Allow-Origin: *`。【估】仍应低频、标 UA。

**拉取建议**：主选 rpcs.json；过滤 `wss`（若先 HTTPS）、`${API_KEY}` 模板、健康探测失败项。不推荐只解析 extraRpcs.js。

---

## 2. 主流链端点数量【测】rpcs.json

| 链 | id | https | wss |
|---|---:|---:|---:|
| ETH | 1 | **76** | 8 |
| BSC | 56 | 53 | 7 |
| Polygon | 137 | 34 | 4 |
| Arbitrum One | 42161 | 32 | 3 |
| Base | 8453 | 35 | 4 |
| OP Mainnet | 10 | 31 | 4 |
| Avalanche C | 43114 | 25 | 2 |
| **Monad** | **143** | **16** | **2** |
| Monad Testnet | 10143 | 9 | 2 |

ethereum-lists 对照【测】：ETH 仅 18 rpc；Monad **仅 1**（`https://rpc.monad.xyz`）→ **公共池必须用 chainlist**。

### Monad 专项

| 项 | 值 | 依据 |
|---|---|---|
| Mainnet | **已上线**，出块中 | 【测】`eth_blockNumber` 持续增长 |
| chainId | **143** (`0x8f`) | 【官】+【测】`eth_chainId` |
| 官方 RPC | rpc/rpc1–4.monad.xyz、rpc-mainnet.monadinfra.com 等 | 【官】docs |
| 官方限频 | rpc **25 rps**；rpc1 **15 rps**；rpc2/3 **300/10s**；infra **20 rps** | 【官】 |
| Testnet | **10143** (`0x279f`)，`https://testnet-rpc.monad.xyz` | 【测】 |
| chainlist 池 | mainnet 18 总 / testnet 11 总 | 【测】 |

文档：https://docs.monad.xyz/developer-essentials/network-information

---

## 3. 限频/拒绝信号

### 【测】抽样

| 端点 | 形态 |
|---|---|
| publicnode eth | 200 OK；20 并发全 200（未打满限） |
| 1rpc / drpc / blastapi / tatum / tenderly | 200 JSON |
| **cloudflare-eth.com** | **HTTP 429** 纯文本 *Rate limiting threshold exceeded* |
| **rpc.ankr.com/eth**（无 key） | HTTP 200 + `error.code=-32000` 要求 API key |
| eth.llamarpc.com | **HTTP 521** CF 源站挂（≠限频） |
| monad 官方/drpc | 200 OK |

### 各家差异（【官】+ 社区）

- **Alchemy**：HTTP **429** + CU/s 文案；JSON 码也可为 429。【官】error-reference
- **Infura 经典**：`project ID request rate exceeded`；历史上常 **-32005**（勿当全球唯一码）
- **dRPC free**：~**120k CU/min/IP** ≈ **~100 eth_call/s**，高压可降至 ~40/s【官】
- **OnFinality free**：~**40 RU/s**【官】
- **`-32000..-32099`** 实现保留，**限频码不统一**；靠 HTTP + message 关键词

### 网关判定「限频/不可用」清单

**立即冷却换节点**：① HTTP 429 ② HTTP 403 且含 quota/capacity/limit ③ error.message 匹配 `rate limit|too many requests|request rate exceeded|compute units|capacity|throttl|quota` ④ 无 key 却要求认证（Ankr 式）⑤ 历史 Infura `-32005` 类 message。

**短冷却/降权（未必限频）**：HTTP 5xx / CF 520–524；body 为 HTML/`error code: 5xx`；200 但非 JSON；【估】延迟 **>3–5s** 或错误率飙升。

**勿当限频**：`execution reverted`、`-32602` 参数错、`-32601` 方法不支持。

---

## 4. 量级与 10k QPS

**单点可持续 QPS【估】**（轻读方法、单 IP）：宽松公共 **5–50**；文档 free tier **~40–100**；Monad 官方 **15–25**【官】；严限端点 **≪5**。设计按 **10–25 QPS/健康端点** 保守取值。

**上游负载**：`upstream ≈ client_qps × (1−hit) × (1−coalesce)`

| 命中+去重 | 未命中 QPS | 池需求【估】 |
|---|---:|---|
| 99% 命中 + 再合并 50% | ~50 | 3–5 健康节点 |
| 95% | ~500 | 20–50 节点 |
| 90% | ~1000 | 池打满仍易 429 |
| 0% 裸转 | 10000 | **不可行** |

**建议【估】**：缓存+coalescing **≥98–99.5%**；ETH 活跃池 **≥15–30** HTTPS 健康节点；Monad **≥8–12** 且 per-endpoint 令牌桶对齐官方 rps；429 指数冷却；hedging 慎用（双倍耗配额）。

---

## URL 全表

https://chainlist.org/rpcs.json · https://chainlist.org/ · https://chainlist.org/chain/1 · https://chainlist.org/chain/143 · https://chainlist.org/chain/10143 · https://github.com/DefiLlama/chainlist · https://raw.githubusercontent.com/DefiLlama/chainlist/main/constants/extraRpcs.js · https://chainid.network/chains.json · https://github.com/ethereum-lists/chains · https://docs.monad.xyz/developer-essentials/network-information · https://rpc.monad.xyz · https://rpc1.monad.xyz · https://testnet-rpc.monad.xyz · https://drpc.org/docs/howitworks/ratelimiting · https://www.alchemy.com/docs/reference/error-reference · https://ethereum-rpc.publicnode.com · https://cloudflare-eth.com

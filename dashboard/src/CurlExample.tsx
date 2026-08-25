import { useState } from 'react';
import { curlExample, rpcUrl } from './utils';

export function CurlExample({ chainId, method = 'eth_blockNumber', params = [], title = 'Developer access' }: { chainId: string | number; method?: string; params?: unknown[]; title?: string }) {
  const origin = window.location.origin; const [copied, setCopied] = useState(false); const text = curlExample(origin, chainId, method, params);
  const copy = () => navigator.clipboard?.writeText(text).then(() => { setCopied(true); setTimeout(() => setCopied(false), 1500) });
  return <div className="card"><div className="card-head"><h2>{title}</h2><button onClick={copy}>{copied ? 'Copied' : 'Copy'}</button></div><p className="muted">JSON-RPC endpoint: <code>{rpcUrl(origin, chainId)}</code> — POST any standard JSON-RPC request; no API key required.</p><pre className="code-block"><code>{text}</code></pre></div>
}

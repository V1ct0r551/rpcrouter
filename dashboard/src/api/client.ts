// 开发模式始终走 Vite 代理，避免本机联调产生跨域；生产构建可指定独立 API 根地址。
const API_BASE = import.meta.env.DEV ? '' : (import.meta.env.VITE_API_BASE || '');
const TOKEN_KEY = 'rpcrouter.adminToken';
export class ApiError extends Error { constructor(public status: number, message: string, public body?: unknown) { super(message) } }
export const getToken = () => localStorage.getItem(TOKEN_KEY) || '';
export const setToken = (token: string) => token ? localStorage.setItem(TOKEN_KEY, token) : localStorage.removeItem(TOKEN_KEY);
export async function apiFetch<T>(path: string, init: RequestInit = {}, timeoutMs = 10000): Promise<T> {
  const headers = new Headers(init.headers); headers.set('Accept', 'application/json');
  if (init.body && !headers.has('Content-Type')) headers.set('Content-Type', 'application/json');
  const token = getToken(); if (token) headers.set('Authorization', `Bearer ${token}`);
  const controller = new AbortController(); const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(`${API_BASE}${path}`, { ...init, headers, signal: init.signal || controller.signal });
    const text = await response.text(); let body: unknown; try { body = text ? JSON.parse(text) : undefined } catch { body = text }
    if (response.status === 401) { window.dispatchEvent(new CustomEvent('rpcrouter:unauthorized')); throw new ApiError(401, 'Unauthorized', body) }
    if (!response.ok) { const message = typeof body === 'object' && body && 'error' in body ? String((body as {error:{message?:string}}).error.message || 'Request failed') : `Request failed (${response.status})`; throw new ApiError(response.status, message, body) }
    return body as T;
  } finally { clearTimeout(timer) }
}
export const post = <T>(path: string, body: unknown = {}) => apiFetch<T>(path, { method: 'POST', body: JSON.stringify(body) });
export const put = <T>(path: string, body: unknown) => apiFetch<T>(path, { method: 'PUT', body: JSON.stringify(body) });

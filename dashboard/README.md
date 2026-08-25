# rpcrouter Dashboard

The dashboard is a static React/Vite application for the `/admin/api/*` endpoints.

```sh
npm ci
npm run dev                 # proxies /admin to 127.0.0.1:8545
VITE_API_BASE=http://host:8545 npm run dev
npm run lint && npm run typecheck && npm test && npm run build
```

The production build is `dist/` and is served by rpcrouter at `/dashboard/` when
`RPCROUTER_ADMIN_STATIC_DIR=/app/dashboard` is configured. Enter the admin bearer
token in Settings; it is kept in browser local storage and sent only as an
`Authorization` request header.

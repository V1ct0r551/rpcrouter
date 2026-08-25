# W6 State/Admin verification

## Branch and verification

- Branch: `w6-state-admin`
- State catalog persistence uses gzip JSON (`flate2`) at `{namespace}:catalog`, with
  `{namespace}:catalog:etag` and `{namespace}:catalog:fetched_at` metadata.
- Offline catalog fallback test: `chainlist::tests::state_store_catalog_is_used_before_disk_and_fixture`.
- Runtime state metadata is supplied from the shared flush/ping snapshot; `/admin/api/state`
  performs no StateStore calls.

## Gate results

| Check | Result |
|---|---|
| `cargo fmt --check` | pass |
| `cargo clippy --all-targets -- -D warnings` | pass |
| `cargo test` | pass, 98 library tests + 1 loadtest unit + 28 binary/integration tests |
| `REDIS_URL=redis://127.0.0.1:6379/0 cargo test -- --ignored` | pass, 6 ignored tests |

Redis ignored coverage includes gzip catalog round-trip/metadata, structured concurrent
override writes, namespace reset, and local chainlist parsing.

## M1 startup timing

With the optional Redis backend and no reachable Redis:

- refused connection (`127.0.0.1:1`): `/healthz` in approximately **442 ms**;
- accept-without-response blackhole: `/healthz` in approximately **2.34 s**;
- required mode with refused connection: explicit startup error in approximately **6 ms**.

The timings are bounded by the Redis connection/response timeouts; no redis-rs internal retry
backoff is used.

## Cluster sharding smoke

The compose `cluster` profile was exercised with three rpcrouter instances sharing Redis and the
nginx shard configuration. One request was sent for each of chain IDs **1**, **56**, and **137**.
The `/chains` hot collections showed each chain on exactly one instance (no duplicate ownership),
and subsequent requests stayed on the same shard.

## Admin API smoke examples

```sh
curl -s http://127.0.0.1:8545/admin/api/overview
curl -s 'http://127.0.0.1:8545/admin/api/chains?state=dormant&limit=200'
curl -s -X POST -H 'Authorization: Bearer "$RPCROUTER_ADMIN_TOKEN"' \
  -H 'content-type: application/json' \
  -d '{"url":"https://node.example"}' \
  http://127.0.0.1:8545/admin/api/chains/1/endpoints/disable
curl -s -H 'Authorization: Bearer "$RPCROUTER_ADMIN_TOKEN"' \
  http://127.0.0.1:8545/admin/api/state
curl -s -H 'Authorization: Bearer "$RPCROUTER_ADMIN_TOKEN"' \
  http://127.0.0.1:8545/admin/api/state/export
```

The offline axum `oneshot` suite (`tests/w6_admin.rs`) exercised the auth matrix, every control
family, persistence failure responses, export/import/reset, audit records, and SPA fallback.

## Performance

The release load test (`10,000 QPS`, `60 s`, local mock upstream) completed **600,000/600,000**
requests with **0 UVE**, achieved QPS **9999.9046**, and p99 **1.733 ms** (within the W5 1.49 ms
baseline ±20% tolerance).

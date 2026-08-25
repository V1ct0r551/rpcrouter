# W6 最后一轮修复验证

## 提交

- `fix(admin): harden static hosting and runtime controls`
- `fix(state): make degraded control writes fail closed`
- `test(admin): cover checker regression vectors`

## Must-fix 处置

1. 静态目录：拒绝空段、绝对路径、`..`、百分号编码；canonicalize 后限制在静态根内，不跟随目录外符号链接；扩展名资源不存在时不做 SPA 回退；补齐 MIME。
2. limits/settings/cool：handler 范围校验，Registry Endpoint 构造与覆写加载 clamp/忽略非法值；不会因非法持久化值 panic。
3. Redis 降级：ResilientStore primary 缺席时控制写返回错误，Admin 返回 503；状态接口增加 `writable`。
4. overview/chains/state/overrides 只读 Registry 内存；Redis index HGET 改 pipeline；指标快照改为只读查询。
5. 运行时 pinned、runtime endpoint add 纳入 materialize/merge；import 立即恢复 health/hot。
6. export 不携带 catalog；import body 独立放宽至 8 MiB；health key helper 统一。
7. settings 支持 null 删除、未知字段拒绝；统一 camelCase。
8. endpoint URL add 仅接受 HTTPS、无 userinfo/template，默认拒绝 loopback/private/localhost；enable/limits/disable 仅接受已知 URL；dormant add 可持久化。
9. 新增回归用例覆盖上述路径。

## 新增测试

`invalid_limits_and_settings_are_rejected_before_persistence`、`read_only_admin_endpoints_do_not_call_store`、`chains_listing_does_not_increase_metrics_cardinality`、`settings_null_deletes_override_and_pin_uses_two_store_calls`、`degraded_resilient_store_rejects_control_writes`、`runtime_pin_and_added_endpoint_survive_restart_and_refresh`，并扩展 `static_spa_fallback_and_disabled_admin`。

## 黑盒实测

- 路径穿越：`/dashboard//etc/passwd`、`///etc/passwd`、`%2e%2e`、缺失 JS 均 404；SPA fallback 与 JS MIME 正确。
- `rps=0`：400，store 调用数不变。
- Redis 降级控制写：503，内存态不变；恢复后 Redis 覆写重新应用。
- chains 列表后 `/metrics`：行数不变（测试前后均 59）。

门槛：fmt、clippy、cargo test、Redis `--ignored` 全绿（99 library tests；ignored 6 tests）。

已知最小偏差：S7 schema 不一致仍沿既有重连退避路径处理；S8 审计字段仍保持现有 what/target 结构；S12 reset 后由下一次 bootstrap/flush 完成 seed。

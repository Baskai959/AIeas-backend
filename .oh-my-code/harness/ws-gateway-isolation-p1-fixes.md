# Harness Graph: ws-gateway-isolation-p1-fixes

> Status: completed

## User Intent
- Original: 修复 WS Gateway 职责隔离改造后的 3 个审查问题：P1 RT active auction 写入失败被吞导致 WS Gateway 无法路由；P1 active auction RT 状态复用直播场次锁 key 且 shard 路由不一致；P2 ws-gateway readiness 被 MySQL/Kafka 等业务依赖牵连。
- Acceptance:
  1. `LiveSessionService` 激活 active auction 时，RT `SetActiveAuction` 失败必须让激活接口返回可重试错误，并保证不会留下 DB `ActiveAuctionID` 与 RT 不一致；锁释放路径正确。
  2. active auction RT key 与 lock key 分离：锁继续使用 lock key，active auction 使用独立 `live_session:{id}:active_auction`（或等价独立 key），且统一按 `ForSession` 路由；补多 shard / TTL 不覆盖测试。
  3. `APP_ROLE=ws-gateway` readiness 过滤业务依赖 probe：不因 MySQL/Kafka 等业务依赖失败而 503；只保留 WS 必需依赖（Redis RT、Pub/Sub/Stream、scripts、ws_draining 等）。
  4. 不修改 AGENTS.md 或任何 `*.openapi.json`。
  5. `go test ./...` 与 `go vet ./...` 通过。

## Assurance
- Mode: strict_verify
- Why: 两个 P1 涉及激活一致性与 Redis key/shard 路由，可能直接影响生产 WS 路由正确性；P2 涉及部署健康检查。需要执行后独立验证。

## Gates
### g1 p1-p2-verify
- Status: passed
- Acceptance: 执行节点完成修复并自测，独立 verifier 复核 P1/P2 代码与测试证据，`go test ./...`、`go vet ./...` 通过。
- Repair: 1/2  (首次验证发现 P2 readiness 测试覆盖不足；已补测试并复验 PASS)

## Understanding
- 当前审查指出：`live_session.go` 中 `SetActiveAuction` 曾被 `_ = ...` 吞掉；`ws_handler.go` 在 ws-gateway 只依赖 `sessionRealtime.ActiveAuction`，因此 RT 写失败会导致 API 激活成功但 WS Gateway 找不到 active auction。
- 当前审查指出：`LiveSessionRealtimeStore` active auction 使用了 `LiveSessionActiveLock` key，与 `LiveSessionLock` SETNX 锁复用；且 realtime store 用 `ForSession`，lock 用 `ForRoom`，多 shard 可能路由到不同物理 shard。
- 当前审查指出：`NewServerWithConfig` 仍打开 MySQL 且 readiness probes 包含 mysql；ws-gateway 即使不查 MySQL，也可能因 MySQL 故障被摘流。

## Completed
### n1 @omc-deep-executor — DONE
- Completed At: 2026-06-05T10:30:00+08:00
- Summary: 修复 P1/P2：LiveSession 激活先写 RT active auction，失败即返回且不更新 DB active；active auction key 拆分为独立 `live_session:{id}:active_auction` 并走 `ForSession`；ws-gateway readiness 过滤 MySQL/Kafka/业务探针，仅保留 WS 必需探针；补充 service/redis/app 测试，自报 `go test ./...` 与 `go vet ./...` 通过。
- Concern: `NewServerWithConfig` 仍会初始化 MySQL/Kafka 客户端，本轮仅按要求修复 readiness probe 过滤，未扩大到依赖懒加载。

### n2 @omc-verifier — DONE_WITH_CONCERNS
- Completed At: 2026-06-05T10:45:00+08:00
- Summary: `go test ./...` 与 `go vet ./...` 通过；P1-1/P1-2 代码与测试通过；P2 实现行为通过但测试未显式覆盖 `kafka` 过滤和 `redis_cache/scripts/pubsub/stream` 保留。

### n3 @omc-executor — DONE
- Completed At: 2026-06-05T11:00:00+08:00
- Summary: 仅扩展 `internal/app/server_test.go`，补齐 readiness probe role filtering 测试：`kafka/mysql/business` 在 ws-gateway 过滤且不调用，`redis_rt/redis_cache/scripts/pubsub/pub_sub/stream/ws_draining` 保留且调用；`go test ./...`、`go vet ./...` 通过。

### n4 @omc-verifier — DONE
- Completed At: 2026-06-05T11:10:00+08:00
- Summary: 复验 PASS：`go test ./...`、`go vet ./...`、`TestReadinessProbeRoleFiltering` 均通过；P1-1、P1-2、P2、AGENTS/OpenAPI/cmd 约束全部满足。

## Pending
（无）

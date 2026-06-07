# Harness Graph: ws-gateway-isolation

> Status: completed

## User Intent
- Original: 在 `/Users/bytedance/study/AI电商/backend` 中一步到位完成 API Backend / WebSocket Gateway 职责隔离改造；ws-gateway 真正只负责连接、订阅、心跳、快照、replay、Pub/Sub fan-out、慢客户端隔离，并通过 `go test ./...` 与 `go vet ./...`。
- Acceptance:
  1. 必读文件已被执行节点读取：`AGENTS.md`、`docs/API网关与WebSocket网关实施提示词.md`、`docs/WebSocket断线重连客户端实现指南.md`、`docs/拍卖高并发一致性技术方案.md`、`internal/app/server.go`、`internal/transport/http/ws_handler.go`、`internal/transport/ws/hub.go`、`internal/config/config.go`。
  2. `all` 模式保持历史行为：REST + WS + worker 全部可用。
  3. `api` 模式只注册 REST/MCP/ops，不注册 `/ws/*`，不启动 WS Pub/Sub fan-out。
  4. `ws-gateway` 模式只注册 `/ws/*` + `/healthz` + `/readyz` + `/metrics` + `/ping`，不注册 REST/MCP。
  5. `ws-gateway` 不启动业务 worker：BidRecordWriter、KafkaBidRecordWriter、RedisBidEventKafkaBridge、BidRankingWorker、BidStreamTrimWorker、BidRecordReconciler、DepositReconciler、OrderTimeoutWorker、非 WS 必需后台任务。
  6. `ws-gateway` 不直接做出价裁决：`bid.place` 不调用 `BidService.Place`，返回 `bid.ack` + `{accepted:false, reason:"BID_THROUGH_API_REQUIRED"}`；`all` 保留原 WS 出价兼容行为。
  7. `ws-gateway` snapshot 不查 MySQL fallback：Redis RT 命中下发 `room.snapshot`；RT miss/error 下发 `room.snapshot_required`（含 `auctionId/reason/serverTime`）；不调用 `AuctionService.State` fallback MySQL。
  8. `/ws/live-sessions/:session_id` 在 `ws-gateway` 不依赖 MySQL session repo；优先 Redis RT / LiveSessionRealtimeStore，缺失时允许 session-only 连接或明确错误，不 fallback MySQL。
  9. Hub draining 与 readiness 联动：draining 后 `/readyz` 返回 503，新握手 503 + Retry-After，shutdown hook 先 broadcast/drain 再停 worker/关资源。
  10. Nginx 示例体现 API/WS 各自 readiness，WS idle timeout >= 90s，Upgrade/Connection 正确。
  11. 不修改既有 OpenAPI 聚合文件；如需协议说明只改/新增 Markdown。
  12. 新增/更新测试覆盖角色路由、ws-gateway bid.place disabled、snapshot RT-only、live-session 不查 MySQL、draining readiness、worker helper；`gofmt`、`go test ./...`、`go vet ./...` 通过。

## Assurance
- Mode: strict_verify
- Why: 改动跨 composition root、WS handler、Hub/readiness、worker 生命周期、实时存储与测试；用户要求全量 test/vet 通过，需要独立验证 gate。

## Gates
### g1 implement-verify
- Status: passed
- Acceptance: 执行节点完成实现并自测，独立 verifier 复核所有硬性目标、OpenAPI 未改、`go test ./...` 与 `go vet ./...` 通过。
- Repair: 0/2

## Understanding
- 先前已实现基础 `APP_ROLE=all|api|ws-gateway`、握手限流、ping jitter、draining、gateway.draining、lastSeq replay、Nginx 示例。
- 本轮目标是进一步让 `ws-gateway` 职责收敛：不启动业务 worker、不本地裁决 bid.place、不做 MySQL snapshot fallback、不用 MySQL 查询 live-session active auction，并让 readiness 与 draining 联动。
- `internal/app/server.go` 必须保持唯一 composition root；不新增 cmd composition root。

## Completed
### n1 @omc-deep-executor — DONE_WITH_CONCERNS
- Completed At: 2026-06-04T15:00:00+08:00
- Summary: 已读取 8 个必读文件；完成 role helper、业务 worker 与 WS consumer 启动隔离、ws-gateway bid.place disabled、RT-only snapshot、LiveSessionRealtimeStore active auction 扩展、draining readiness、nginx 更新和测试补充；自报 `gofmt`、`go test ./...`、`go vet ./...` 通过。
- Concern: 为保持唯一 composition root 与控制重构范围，`NewServerWithDependencies` 仍复用现有服务装配路径构建部分服务实例；但 ws-gateway 不暴露 REST/MCP、不启动业务 worker，WS 出价/snapshot/live-session 不走 MySQL fallback。

### n2 @omc-verifier — DONE
- Completed At: 2026-06-04T15:20:00+08:00
- Summary: 独立验证 PASS：`go test ./...`、`go vet ./...` 通过；角色路由、worker gating、bid.place disabled、RT-only snapshot、live-session realtime lookup、draining readiness、Nginx、OpenAPI/AGENTS/cmd 约束均有源码/测试证据。

## Pending
（无）

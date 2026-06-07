# Harness Graph: split-gateway-backend

> Status: completed

## User Intent
- Original: 拆分部署拓扑为 API Gateway/Ingress + API Backend + WebSocket Gateway，并增强 WS 稳定性（握手限流、draining、close reason、ping jitter、lastSeq replay 等）。新增 APP_ROLE 切换，提供部署示例与客户端重连指南，更新测试。
- Acceptance:
  1. `internal/app/server.go` 仍是唯一 composition root；新增 role 选择能力（all/api/ws-gateway），api-only 不挂 /ws/*，ws-gateway-only 只挂 /ws/* + /healthz + /readyz + /metrics + 必要鉴权。
  2. WS Hub/Handler 在保持现状能力前提下增加：握手限流（IP/user/auction）、draining 模式 + close 1012/1001、可观测 close reason、ping jitter、lastSeq replay + room.snapshot_required。
  3. 配置：新增 APP_ROLE 与 ws.* 可调项；同步更新 `configs/config.yaml` 与 `.env.example`。
  4. 部署示例：新增 `deploy/...`（nginx 或 k8s ingress 任选其一），/api/*、/mcp/*、/metrics → API Backend，/ws/* → WS Gateway，idle timeout >= 90s，使用 /healthz、/readyz。
  5. 文档：更新（或新增独立文件）`docs/WebSocket断线重连客户端实现指南.md` 描述客户端重连协议；不修改既有 OpenAPI。
  6. 测试：扩展 `internal/transport/ws/hub_test.go` / handler 测试覆盖 lastSeq replay、snapshot_required、draining、握手限流、慢客户端断开。
  7. `go test ./...`、`go vet ./...` 通过；保留单进程兼容；保留 Redis Pub/Sub fan-out；出价裁决仍走 Redis Lua（WS gateway 不查 MySQL）。

## Assurance
- Mode: strict_verify
- Why: 用户显式要求不破坏 `go test ./...` 与 AGENTS.md 约束，且改动跨 composition root + transport + config + docs + test，回滚成本中等偏高，需要独立验证关闸。

## Gates
### g1 design-review
- Status: passed
- Acceptance: 架构方案明确说明：role 配置形态、composition root 拆分方式（新增构造函数 vs 在 NewServer 内分支）、WS 增强落点、配置项命名、向后兼容策略、是否新增 cmd/ 子命令；并通过 critic 评审。
- Repair: 1/2  (用户决策化解 critic 的 MUST-FIX #1/#2；其余 must/should-fix 由 orchestrator 合并到 n4 dispatch spec)

### g2 implement-verify
- Status: passed
- Acceptance: `go vet ./...` 与 `go test ./...` 通过；新增/修改的 ws 测试通过；既有 OpenAPI 文件未被修改；apply role=api 时 /ws/* 未注册；apply role=ws-gateway 时 /api/* 未注册；harness 验证证据齐全。
- Repair: 1/2  (首次独立验证发现 3 个缺口；局部 repair 后复验 PASS)

## User Decision Log
- D1 (2026-06-04): WS Gateway 数据访问边界 = 选项 A —— **ws-gateway 仍连 MySQL+Redis，但不注册 /api/* 路由、不启动 worker**。直接复用真实 `BidService / LiveSessionService / AuctionService`（持真实 MySQL repo），bid 终裁仍在 Redis Lua，MySQL 持久化由 BidService 同步路径自然完成。无需"内存仓替代"，无需新增 HTTP RPC 跨服务调用。

## Final Spec (architect's design + critic's must/should-fix merged)

### 数据访问 (覆盖 critic MUST-FIX #1/#2)
- ws-gateway 进程仍走 `appconfig.MustLoad` 全量装配 MySQL pool + Redis pool；只是 `newServerWithServices` 在 role=ws-gateway 分支跳过 `registerAPIRoutes`、跳过 worker 启动 (FeatureFlag invalidator / OrderTimeoutWorker / EventRelay 看场景 / BidRecordWriter / BidRankingWorker / DepositReconciler / OnlineCounter janitor 等)。
- 但 **EventRelay + PubSubBroadcaster 在 ws-gateway 必须启动**（Pub/Sub fan-out 的本地分发依赖它们）；OnlineCounter janitor 也保留。
- role=api 跳过 `registerWSRoutes` 与 Hub/EventLog/PubSubBroadcaster/EventRelay 启动；其余 worker 保持启动。
- role=all 默认全量启动，与今天等价。

### B4 修订 (覆盖 critic MUST-FIX #3/#4)
- **不修改 `Hub.ReplaySince` 当前的内存 fallback 行为**：`source.ReplaySince` 失败时仍回退到 `room.ReplaySince`；handler 仅依赖 `(events, complete)` 决定是否下发 `room.snapshot_required`。这保持 role=all 行为零变更，避免回归。
- **read goroutine 显式分类**：读循环改为 `for { _, _, err := readMessage(conn); if err != nil { reason := classifyReadErr(err); client.CloseWithReason(reason); return }; ... }`；`classifyReadErr`：deadline-exceeded / timeout → `pong_timeout`，其它 → `read_closed`（让外层 select 不必再写 reason）。

### B4 修订 (覆盖 critic SHOULD-FIX #1/#2)
- **`gateway.draining` 用 per-Client Deliver**（不进 Broadcast → 不占 seq、不进 history）。Hub 内新增 `broadcastDraining(envelope)` 直接遍历所有 room 内 Client 调 `Deliver`，跳过 NextSeq。
- **OnShutdown 顺序**：(1) `hub.BeginDrain()` → (2) `hub.AwaitDrained(ctx, DrainTimeout)` → (3) cancel broadcaster + relay context → (4) Redis pool close → (5) MySQL close。在 `server.go` 现有 `OnShutdown` 钩子链顶部插入 (1)(2)(3)，原 Redis/MySQL 关闭逻辑保留为 (4)(5)。

### B4 修订 (覆盖 critic SHOULD-FIX #3 anonymous limiter)
- `HandshakeLimiter.Allow(ip, userID, auctionID)`：当 `userID == "" || userID == "anonymous"` 时**跳过 perUser 桶**，仅对 perIP / perAuction 计数。

### B4 修订 (覆盖 critic SHOULD-FIX #5 close frame 签名)
- `writeCloseFrame(conn, reason)` **签名保持不变**；新增内部函数 `closeCodeForReason(reason string) int` 查表；`writeCloseFrame` 内部 `code := closeCodeForReason(reason)` 后构造 control frame。测试可断言 `WriteControl` data 前两字节为对应 close code。

### B5 修订 (覆盖 critic SHOULD-FIX #4)
- 同 PR 必须更新两处 `HubMetrics` 实现：`internal/transport/ws/hub_metrics_test.go` 的 `fakeHubMetrics` 与 `internal/infra/observability/metrics/observers.go` 的 `*Registry` —— 否则编译失败。

### B6 修订 (覆盖 critic SHOULD-FIX #6)
- `deploy/nginx/aieas.conf` 中 `upstream aieas_ws_gateway` 块顶部加注释 `# 必须使用默认 round-robin / least_conn；禁止 ip_hash 或 sticky_cookie，否则 ws-gateway BeginDrain 时客户端可能被绑定到同一台`。

### B5 注入路径 (覆盖 critic MUST-FIX #5)
执行器须按以下接线落地：
- `WSHandler` 新增字段：`pingJitter time.Duration`、`handshakeLimiter *corews.HandshakeLimiter`、`metrics ws.HubMetrics`；构造器扩展（同时改 `server.go:727-729` 处的 `httptransport.NewWSHandler(...)` 调用，传入 `cfg.WebSocket.PingJitter.Std()`、limiter 实例、`deps.MetricsRegistry`）。
- `HandshakeLimiter` 在 `app/server.go` 中构造，归 `ServerDependencies` 持有（新增字段 `WSHandshakeLimiter *wstransport.HandshakeLimiter`，可空时由 `NewServerWithDependencies` 兜底构造）。
- `Hub.SetMetrics(deps.MetricsRegistry)` 调用点保持在 `server.go:425` 不动。
- `Drain` 触发钩子：在 `server.go` 已有 `OnShutdown` 装配处（约 `:160-182`）顶部插入 `hub.Drain(ctx, cfg.WebSocket.DrainTimeout.Std())` 调用，在 broadcaster cancel / Redis pool close 之前。

## Completed
### n1 @omc-explore — DONE
- Completed At: 2026-06-04T12:00:00+08:00
- Summary: 完成 A-G 全章节盘点，给出 path:line 锚点；明确"已存在"/"部分存在"/"NOT FOUND"现状；并发现 AGENTS.md 与真实路由漂移、已有 WS OpenAPI 文件等关键事实。

### n2 @omc-architect — DONE
- Completed At: 2026-06-04T12:30:00+08:00
- Summary: 输出 B1-B8 完整设计稿（APP_ROLE / WS 配置 / 组合根拆分 / 稳定性加固 / metrics / nginx / 客户端指南 / 测试清单）。

### n3 @omc-critic — DONE_WITH_CONCERNS
- Completed At: 2026-06-04T12:50:00+08:00
- Summary: 5 个 MUST-FIX + 7 个 SHOULD-FIX + 5 个 NIT。MUST #1/#2 由用户决策（D1）化解；其余在 Final Spec 中合并修订。

### n4 @omc-deep-executor — DONE
- Completed At: 2026-06-04T13:30:00+08:00
- Summary: 完成 APP_ROLE / WS 配置扩展、role-aware route registration、Hub draining、HandshakeLimiter、handler 限流与 close code、metrics、nginx 示例、客户端重连指南与测试补充；自报 `go vet ./...`、目标包测试、`go test ./...` 均通过。

### n5 @omc-verifier — FAILED
- Completed At: 2026-06-04T13:45:00+08:00
- Summary: 验证 Go 门禁通过，但发现 3 个阻断缺口：handshake rate limit 负值校验缺失、nginx no-sticky 注释缺失、handler 层 draining/limiter/write-timeout/ping-jitter 测试不足。

### n6 @omc-executor — DONE
- Completed At: 2026-06-04T14:00:00+08:00
- Summary: 局部修复 n5 缺口：补充 handshake rate limit 校验与测试、nginx no-sticky 注释、WS handler draining/limiter/write-timeout/ping-jitter 测试；自测 `go vet ./...`、目标包测试、`go test ./...` 均通过。

### n7 @omc-verifier — DONE
- Completed At: 2026-06-04T14:10:00+08:00
- Summary: 复验 PASS：`go vet ./...`、目标包测试、`go test ./...` 均通过；3 个历史缺口均有文件与测试证据；`AGENTS.md` 和 `*.openapi.json` 无 diff；role route 与 Hub replay fallback 非回归确认。

## Pending
（无）

## Understanding
- 仓库根 `/Users/bytedance/study/AI电商/backend`，模块 `aieas_backend`，Hertz + hertz-contrib/websocket。
- **真实 WS 路由**：`GET /ws/auctions/:auction_id`、`GET /ws/live-sessions/:session_id`（AGENTS.md 写的 live-rooms/room_id 与代码已漂移）。
- composition root 入口：`NewServer → NewServerWithConfig → NewServerWithDependencies → newServerWithServices`（`internal/app/server.go:50/55/258/632`）。`ServerDependencies` 字段已枚举。
- 全局中间件链 `Recovery → RequestID → Tracing → Metrics → RateLimiter → Audit`；`/healthz /readyz /metrics /ping` 通过 `IsObservabilitySkipPath` 跳过；ws 升级路径**当前未被跳过**。
- 已有能力：lastSeq replay（`Hub.ReplaySince` + `RedisReplaySource`）、Pub/Sub fan-out（`PubSubBroadcaster`）、`HubMetrics` 4 方法、`room.snapshot_required`（handler 内已下发）、慢消费者检测（buffer 满即 close，reason="slow_consumer"）。
- **缺失能力**：APP_ROLE / 角色路由切换；Hub 无 `Start/Run/Close/Drain`；无 draining 状态机；ping 无每连接 jitter；握手无 per-IP/user/auction 限流；close code 仅 1000；close reason 仅 `slow_consumer/unsubscribe/closed/read_closed/write_closed`（无 `pong_timeout/gateway_draining`）；`websocketWriteTimeout = 5s` 硬编码；replay limit 256 硬编码；handshake reject 无指标；draining 广播无指标。
- 配置侧：已存在 `WebSocketConfig{ReadLimitBytes, SendBufferSize, PingInterval, PongTimeout}`（`internal/config/config.go:131`），`applyEnv` 段在 `config.go:739-750`。
- 测试侧：`hub_test.go`(14 用例) + `hub_metrics_test.go`(4) + `event_log_test.go`(2) + `ws_handler_test.go`(8)。无 draining / handshake limit / snapshot_required envelope 路径测试。
- 文档：`docs/WebSocket{接口文档,断线重连客户端实现指南,用户端交互协议}.md` 已存在；**`docs/API/WebSocket直播场次改造.openapi.json` 也已存在**——属用户约束的"既有 OpenAPI 文件"，禁止修改。
- handler 间接触达 MySQL 的边界：`h.bids.Place`（bid.place 上行）、`h.sessions.ActiveAuctionAndSession`、`h.auctions.State`（room snapshot）。WS Gateway 模式下需要替代/拒绝/远调策略。

## Completed
### n1 @omc-explore — DONE
- Completed At: 2026-06-04T12:00:00+08:00
- Summary: 完成 A-G 全章节盘点，给出 path:line 锚点；明确"已存在"/"部分存在"/"NOT FOUND"现状；并发现 AGENTS.md 与真实路由漂移、已有 WS OpenAPI 文件等关键事实。

<!-- canonical Pending 段已迁移至文首 (与 Completed 同区块) -->

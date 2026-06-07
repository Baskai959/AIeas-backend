# Harness Graph: code-layering-refactor-20260605

> Status: completed

## User Intent
- Original: 按照 `/Users/bytedance/study/AI电商/backend/docs/代码分层改造方案.md` 中的四个阶段实现代码分层。
- Acceptance: 读取方案文档，按四个阶段完成代码分层改造；保持现有行为兼容；必要测试更新；最终运行验证并说明修改范围、阶段完成情况、测试结果与剩余风险。

## Assurance
- Mode: strict_verify
- Why: 该任务涉及跨包结构调整与潜在大范围重构，阶段输出会影响后续阶段，需要阶段边界清晰并进行独立验证。

## Gates
### g1 plan-ready
- Status: passed
- Acceptance: 已从方案文档提取四个阶段目标、文件边界、实施顺序、风险与验收标准，且不需要用户再补充信息即可进入实现。
- Repair: 0/2

### g2 implementation-complete
- Status: passed
- Acceptance: 四个阶段对应代码分层改造均已完成；未引入新的 composition root；未破坏 AGENTS.md 约束；必要测试已更新。
- Repair: 0/2

### g3 verification-pass
- Status: passed
- Acceptance: 独立验证确认四阶段目标均满足，`go test ./...` 与 `go vet ./...` 已运行并通过或明确记录环境性阻塞。
- Repair: 1/2

## Understanding
- 仓库主目录：`/Users/bytedance/study/AI电商/backend`。
- 方案文档路径：`docs/代码分层改造方案.md`。
- 需先读取方案文档，但长文档内容与跨文件设计判断交给专门节点处理，主会话只保留图状态摘要。
- 项目约束：composition root 仍应保持在 `internal/app/server.go`；优先遵守 `AGENTS.md` 中的分层与 wiring 规则。
- 规划节点提取四个实施阶段：1）拆轻量装配函数；2）接口收敛；3）构造函数依赖显式化；4）按业务模块迁移文件。
- 第 0 阶段“冻结边界”作为前置约束处理，不作为用户要求的四个实施阶段。
- 保守边界：阶段 4 不一次性搬完整目录树，优先从 `live_session` 的最小 ports/reader 边界开始。
- 规划节点将详细计划保存到 `.oh-my-code/plans/code-layering-four-phases-20260605.md`，执行节点可读取该计划。
- 实现节点完成四阶段保守落地：新增 `internal/app/service_wiring.go`；新增 `internal/transport/http/usecases.go`；为 `AuctionService`/`BidService`/`HammerService`/`LiveSessionService` 增加 Deps 显式构造；新增 `internal/modules/live_session/ports/ports.go`；将 live_session repository lock/realtime 接口 alias 到 ports。
- 实现节点自报 `go test ./internal/app ./internal/transport/http ./internal/transport/ws ./internal/service`、`go test ./...`、`go vet ./...` 均通过，仍需独立验证。
- 独立验证失败：Phase 2 未完全满足“handler 不直接访问 repository/infra”。证据：`internal/transport/http/usecases.go` 直接 import repository 并嵌入 `repository.AuctionRealtimeStore`；`auction_handler.go` 和 `live_session_handler.go` 直接 import `internal/infra/objectstorage`。
- 独立验证失败：`go test -race ./internal/transport/ws ./internal/service` 中 `internal/service/bid_stream_worker_test.go` 的 `trackingBidLog.activeCalls` 存在数据竞争。
- 修复节点完成最小修复：`WSAuctionRealtimeSnapshotProvider` 改为 HTTP 层最小接口；HTTP 层新增 `ImageUploader` 抽象并由 app adapter 适配 objectstorage；`AuctionHandler`/`LiveSessionHandler`/`AuthHandler` 移除 objectstorage 直接依赖；`trackingBidLog.activeCalls` 改为 atomic 读写。
- 修复节点自报 `go test ./internal/transport/http ./internal/service`、`go test -race ./internal/transport/ws ./internal/service`、`go test ./...`、`go vet ./...` 均通过，仍需最终独立复验。
- 最终复验 PASS：四阶段代码落地、composition root/入口约束、HTTP 生产文件依赖边界、live_session ports 边界、APP_ROLE 行为均通过；`go test ./...`、`go test -count=1 ./...`、`go vet ./...`、`go test -race -count=1 ./internal/transport/ws ./internal/service` 均通过。

## Completed
### n1 @omc-planner — DONE_WITH_CONCERNS
- Completed At: 2026-06-05T00:00:00+08:00
- Summary: 已读取方案文档与必要上下文，提取四阶段实施计划、依赖顺序、测试策略与风险边界。关注项：规划节点因自身约束创建了 `.oh-my-code/plans/code-layering-four-phases-20260605.md`，未修改业务代码。

### n2 @omc-deep-executor — DONE
- Completed At: 2026-06-05T00:00:00+08:00
- Summary: 完成四阶段保守最小代码落地：app 同包 wiring helper、handler usecase 接口收敛、四个 service Deps 显式构造、live_session 最小 ports 边界；自测通过。

### n3 @omc-verifier — FAILED
- Completed At: 2026-06-05T00:00:00+08:00
- Summary: 必跑 `go test ./...` 与 `go vet ./...` 通过，但验收失败：handler 仍直接依赖 repository/infra；建议 race 检查失败于 `trackingBidLog.activeCalls` 数据竞争。

### n4 @omc-executor — DONE
- Completed At: 2026-06-05T00:00:00+08:00
- Summary: 最小修复 Phase 2 边界与 race 问题：移除 HTTP handler/usecase 对 repository/objectstorage 的直接依赖，新增 HTTP ImageUploader 及 app 适配器，修复测试 fake 原子计数；自测通过。

### n5 @omc-verifier — DONE
- Completed At: 2026-06-05T00:00:00+08:00
- Summary: 最终只读复验通过，确认四阶段分层改造满足验收；全量测试、uncached 全量测试、vet、race 聚焦测试均通过。

## Pending

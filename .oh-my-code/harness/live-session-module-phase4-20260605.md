# Harness Graph: live-session-module-phase4-20260605

> Status: completed

## User Intent
- Original: 继续做“代码分层改造第 4 阶段：按业务模块迁移文件”。第一批只做 live_session 模块，建立并落地 live_session ports / app use case / adapter 边界，不迁移 auction/order/risk/auth 等其它模块。
- Acceptance: `internal/modules/live_session/ports` 边界完善且不依赖 service/transport/GORM/Redis client；`internal/modules/live_session/app` 暴露 use case 接口；LiveSessionHandler 和 WSHandler 的 live_session 依赖转到 module app/ports；行为不变；指定测试/vet 通过；保护项通过。

## Assurance
- Mode: strict_verify
- Why: 模块边界迁移会影响 transport/service/repository/app wiring，多处 import 容易产生循环依赖或协议回归，需要独立验证。

## Gates
### g1 implementation-complete
- Status: passed
- Acceptance: 已阅读指定文件；完成 live_session ports/app 边界、handler/WS 依赖调整、route wiring 注入；不做其它模块迁移；自测通过。
- Repair: 0/2

### g2 verification-pass
- Status: passed
- Acceptance: 独立验证 ports/app 边界、依赖方向、行为保护项；运行 `go test ./internal/modules/live_session/...`、`go test ./internal/transport/http ./internal/transport/mcp`、`go test ./internal/service`、`go test ./internal/app`、`go test ./...`、`go vet ./...`。
- Repair: 0/2

## Understanding
- 仓库主目录：`/Users/bytedance/study/AI电商/backend`。
- 前三阶段已完成 app wiring 拆分、handler/usecase 接口收敛、核心 service Deps 注入。
- 当前已有 `internal/modules/live_session/ports/ports.go`，本轮需完善 ports 并新增 `internal/modules/live_session/app`。
- 硬约束：`internal/app/server.go` 仍唯一 composition root；不新增 cmd；不改 AGENTS.md；不改 docs/API OpenAPI/Apifox；不改 REST/WS 协议；不改 schema/migration；小步迁移。
- 实现节点完成：新增 `internal/modules/live_session/app/usecases.go`；扩展 `internal/modules/live_session/ports/ports.go`；`transport/http/usecases.go` 中 live_session 专属接口降为 module app/ports alias；`LiveSessionHandler`、`WSHandler` 改为依赖 module app/ports；repository/service 保留最小 alias 过渡。
- 实现节点自测：指定 live_session module、transport/http+mcp、service、app、全量测试和 vet 均通过；保护项通过。
- 独立验证 PASS：ports 仅依赖 domain/context/time，不依赖 service/transport/GORM/Redis/Hertz；module app 暴露 usecase；LiveSessionHandler/WSHandler 依赖 module app/ports；transport/http usecases 中 live_session 接口为 alias；指定测试/vet 和保护项通过。
- 非阻断关注：REST/WS 行为不变只能通过路由/编译/测试间接验证；worktree 有前序修改，提交前需拆分归因。

## Completed
### n1 @omc-deep-executor — DONE
- Completed At: 2026-06-05T00:00:00+08:00
- Summary: 完成 live_session 模块边界第一批迁移，建立 app usecase 与 ports 边界并调整 handler/WS 依赖；未迁移其它模块，自测通过。

### n2 @omc-verifier — DONE_WITH_CONCERNS
- Completed At: 2026-06-05T00:00:00+08:00
- Summary: 独立验证通过；关注项为行为不变缺少端到端基线和 worktree 前序修改归因问题，均不阻塞本轮验收。

## Pending

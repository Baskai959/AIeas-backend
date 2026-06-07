# Harness Graph: app-wiring-phase1-20260605

> Status: completed

## User Intent
- Original: 推进第 1 阶段分层，不做大规模业务文件迁移。重点降低 `internal/app/server.go` 复杂度，保持行为不变；新增 `worker_wiring.go`、`route_wiring.go`、`gateway_wiring.go` 或 `runtime_wiring.go`，迁出 worker 启动、handler/route 注册、APP_ROLE/runtime 判定逻辑；保留对外 API；跑测试和 vet。
- Acceptance: `server.go` 行数明显下降；worker 启动逻辑和路由注册逻辑不再堆在 `server.go`；APP_ROLE 路由/worker/readiness/draining 行为保持；`go test ./...` 与 `go vet ./...` 通过。

## Assurance
- Mode: strict_verify
- Why: 本轮是结构性重构，要求行为完全一致且需要全量测试/vet 证明。

## Gates
### g1 implementation-complete
- Status: passed
- Acceptance: 新增/更新 app 同包 wiring 文件，完成 worker/route/runtime 逻辑迁出；不改 AGENTS.md/OpenAPI/cmd；不做业务文件迁移；自测通过。
- Repair: 0/2

### g2 verification-pass
- Status: passed
- Acceptance: 独立验证 `server.go` 职责下降、APP_ROLE 行为保持、readiness/draining 保持，`go test ./...` 与 `go vet ./...` 通过。
- Repair: 0/2

## Understanding
- 仓库主目录：`/Users/bytedance/study/AI电商/backend`。
- 前序已存在 `internal/app/service_wiring.go`，本轮继续第 1 阶段 app 同包 wiring 拆分。
- 约束：`internal/app/server.go` 仍是唯一 composition root；新增 app 同包 helper 不构成新装配中心；不新增 cmd；不改 AGENTS.md；不改已有 OpenAPI；不迁移 service/repository/domain 业务文件。
- 必须保留对外 API：`NewServer()`、`NewServerWithConfig()`、`NewServerWithDependencies()`、`NewServerWithUserRepository()`、`NewServerWithAuth()`。
- 实现节点完成：新增 `internal/app/worker_wiring.go`、`internal/app/route_wiring.go`、`internal/app/runtime_wiring.go`；`server.go` 只保留高层编排、server 创建、中间件、observability 和 route/worker wiring 调用。
- 实现节点自测：APP_ROLE/readiness 聚焦测试、`go test ./...`、`go vet ./...` 均通过，仍需独立验证。
- 独立验证 PASS：`server.go` 当前 1055 行，相对 HEAD 1353 行减少 298 行；worker/route/runtime helper 已迁出；APP_ROLE route/worker/readiness/draining 聚焦测试通过；`go test ./...`、`go test -count=1 ./...`、`go vet ./...` 通过。
- 非阻断关注：当前 worktree 存在较多前序 service/repository 改动，提交前建议拆分归因；不影响本轮 app wiring 验收。

## Completed
### n1 @omc-deep-executor — DONE
- Completed At: 2026-06-05T00:00:00+08:00
- Summary: 已迁出 worker 启动、route 注册、runtime/APP_ROLE/readiness helper 到 app 同包 wiring 文件，保持对外 API 和 APP_ROLE 行为；自测通过。

### n2 @omc-verifier — DONE_WITH_CONCERNS
- Completed At: 2026-06-05T00:00:00+08:00
- Summary: 验证通过，确认 `server.go` 职责下降和行为保持；关注项为 worktree 中有前序 service/repository 改动混杂，建议提交前拆分归因。

## Pending

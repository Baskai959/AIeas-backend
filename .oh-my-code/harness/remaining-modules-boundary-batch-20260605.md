# Harness Graph: remaining-modules-boundary-batch-20260605

> Status: completed

## User Intent
- Original: 下一步直接做“剩余模块边界批量迁移”，一次覆盖 deposit、risk、auth、marketplace、admin、ai / live_analysis、mcp，以及 config / feature_flag / audit / dashboard 作为 admin/support 边界处理；采用 `internal/modules/<module>/{app/usecases.go,ports/ports.go}` 形式，业务逻辑继续留在 `internal/service`，repository 实现继续留在 `internal/repository`，handler 和 MCP transport 改为依赖 module app 接口。
- Acceptance: 批量建立剩余模块 app/ports 边界；HTTP handler 与 MCP transport 收敛到 module app 接口；不改 `AGENTS.md`、`docs/API`、cmd、新 migration、REST/WS/MCP 协议；`go test ./...` 与 `go vet ./...` 通过。

## Assurance
- Mode: ralph_strict
- Why: 本轮是多模块批量边界迁移，范围广且要求全量测试/vet 通过，必须执行独立验证并在失败时有界修复。

## Gates
### g1 implementation-acceptance
- Status: passed
- Acceptance: 仅做边界抽取，不迁移 service/repository 业务逻辑；建立剩余模块 app/ports 边界并让 handler/MCP transport 依赖 module app 接口；不改变协议、schema、cmd、AGENTS.md、docs/API。
- Repair: 0/2

### g2 verification-acceptance
- Status: passed
- Acceptance: `go test ./...`、`go vet ./...` 通过；保护项检查通过；如遇前序 out-of-scope 差异需做范围归因。
- Repair: 0/2

## Understanding
- 前序已完成：app wiring 拆分、handler/use case 接口收敛、核心 service Deps 构造注入、live_session 模块边界迁移、auction 模块边界迁移、order 模块边界迁移。
- 本轮要一次覆盖剩余模块：deposit、risk、auth、marketplace、admin、ai、live_analysis、mcp，以及 config / feature_flag / audit / dashboard 作为 admin/support 边界处理。
- 用户明确要求：业务逻辑继续保留在 `internal/service`，repository 实现继续保留在 `internal/repository`，快速完成模块边界收敛，不把工程炸开。
- 实现节点已为剩余模块新增 app/ports 边界，并将 HTTP handlers 与 MCP transport 收敛到 module app 接口。
- 独立验证已确认：`go test ./...` 与 `go vet ./...` 通过；保护项检查通过；`gateway.draining` 属于前序 ws-gateway out-of-scope 差异，不阻塞本轮验收。

## Completed
### n1 @omc-deep-executor — DONE
- Completed At: 2026-06-05T17:49:54+08:00
- Summary: 完成剩余模块边界批量迁移，覆盖 deposit、risk、auth、marketplace、admin、ai、live_analysis、mcp，并将 config/feature_flag/audit/dashboard 作为 admin/support 边界处理。
- Deliverables: 新增 `internal/modules/{auth,deposit,risk,marketplace,admin,ai,live_analysis,mcp}/{app/usecases.go,ports/ports.go}`；调整 `internal/transport/http/usecases.go`、`auth_handler.go`、`auction_handler.go`、`live_analysis_handler.go`、`ai_assistant_handler.go`、`internal/transport/mcp/{auth.go,handler.go,resources.go,tools.go,types.go}`。
- Findings: 业务逻辑仍留在 `internal/service`，repository 实现仍留在 `internal/repository`；`ports` 未依赖禁止层；`go test ./...` 与 `go vet ./...` 自检通过。

### n2 @omc-verifier — DONE_WITH_CONCERNS（scoped concern resolved）
- Completed At: 2026-06-05T17:49:54+08:00
- Summary: 独立验证批量迁移验收项，确认模块边界、handler/MCP 依赖收敛、保护项与全量测试/vet 均通过；发现 `gateway.draining` 差异，但归因为前序 ws-gateway out-of-scope 变更。
- Deliverables: 全量 `go test ./...` / `go vet ./...` 结果；模块存在性与 ports 禁止依赖检查；HTTP/MCP transport 依赖收敛验证；保护项检查与 scoped 协议差异归因。
- Findings: 验收标准 1~9、11~12 通过；协议面第 10 条按本轮 scoped 验收通过；`AGENTS.md`、`docs/API`、`migrations` 未变更；未新增 `cmd/api-backend` 或 `cmd/ws-gateway`。

## Pending
无。

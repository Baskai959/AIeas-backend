# Harness Graph: service-repository-relocation-round2-20260605

> Status: in_progress

## User Intent
- Original: 第二轮逐模块搬 service/repository 实现。
- Acceptance: 在第一轮已完成的模块边界基础上，开始第二轮逐模块把 service / repository 实现向各模块内部迁移，同时保持 `internal/app/server.go` / `internal/app` 为唯一 composition root，不改变 `AGENTS.md`、`docs/API`、cmd、migration、REST/WS/MCP 协议，并最终能通过验证。

## Assurance
- Mode: ralph_strict
- Why: 这是高风险架构迁移，涉及实现移动与依赖重构，必须先收敛迁移方案，再实施并独立验证。

## Gates
### g1 migration-plan-acceptance
- Status: passed
- Acceptance: 明确第二轮逐模块搬迁的可执行顺序、每模块 service/repository 实现迁移边界、保留项、风险点与验证策略，且不突破 composition root / 协议 / schema 约束。
- Repair: 0/2

### g2 implementation-acceptance
- Status: active
- Acceptance: 按已收敛方案完成至少一个可闭环的实现迁移批次，且实现不破坏约束、不扩散到超范围模块。
- Repair: 0/2

### g3 verification-acceptance
- Status: pending
- Acceptance: 对本轮实现执行必要测试 / vet / 保护项检查并给出 scoped 结论；如遇前序 out-of-scope 差异需完成归因。
- Repair: 0/2

## Understanding
- 第一轮已完成：模块 app/ports 边界收敛，handlers 与 MCP transport 已改为依赖 module app 接口。
- 本轮要做的是第二轮：逐模块搬 service/repository 实现，而不是继续停留在 interface alias / boundary 抽取阶段。
- 当前范围大、耦合深，若不先收敛迁移顺序与安全边界，直接批量搬迁的失败风险高。
- 规划节点已收敛 6 个批次的迁移路线，并建议先实施最小可闭环第一批：`auth + live_analysis`。
- 第一批目标：验证“模块内 app service + module-owned repository + internal/app wiring shim”模板，而不触碰 core realtime/trade 链路。
- 实现节点已完成首批 `auth + live_analysis`：`AuthService` 与 `LiveAnalysisService` concrete 下沉到 module app，`live_analysis_report` MySQL concrete repository 下沉到 module repository，并通过最小 shim 保持旧调用面稳定。
- 独立验证已确认：所有 scoped 测试与 `go test ./...` / `go vet ./...` 通过；`internal/app` 仍是唯一 composition root；唯一非阻塞关注点是 `internal/service/auth.go` 仍保留 `HTTPStatusAndCode` 辅助函数。
- 用户已明确继续下一批；按既定方案，本轮进入 Batch 2：`ai + risk`。
- Batch 2 目标：把 `AIAssistantService` 与 `RiskService` concrete 下沉到各自模块 app，并在不破坏 composition root / 协议 / schema 的前提下评估是否需要同步下沉 owner repository concrete。
- Batch 2 已完成：`AIAssistantService` 与 `RiskService` concrete 已下沉到模块 app，risk owner repository concrete 已下沉到 `internal/modules/risk/repository/`，旧 `internal/service` / `internal/repository` 文件已退化为薄 shim。
- Batch 2 独立验证已通过：所有 scoped 测试、全量 `go test ./...`、`go vet ./...`、保护项检查均通过；唯一残留是由于 dirty worktree 导致“未顺手迁移无关模块”的历史归因只能部分验证，但不构成本轮失败。
- 用户已明确继续下一批；按既定方案，本轮进入 Batch 3：`deposit + marketplace`。
- Batch 3 目标：把 `DepositService` 与 `MarketplaceService` concrete 下沉到模块 app，并在 owner 清晰处把最小必要 repository concrete 下沉到模块内，同时继续保持 `internal/app` 为唯一 composition root。
- Batch 3 已完成：`DepositService` 与 `MarketplaceService` concrete 已下沉到模块 app，deposit owner repository concrete 已下沉到 `internal/modules/deposit/repository/`，旧 `internal/service` / `internal/repository` 文件已退化为薄 shim。
- Batch 3 独立验证已通过：所有 scoped 测试、全量 `go test ./...`、`go vet ./...`、保护项检查均通过；唯一残留是 dirty worktree 导致“未顺手迁移无关模块”的历史归因只能部分验证，但不构成本轮失败。
- 用户已明确继续下一批；按既定方案，本轮进入 Batch 4：`admin / support`。
- Batch 4 目标：把 `AdminService` 与 support 能力（`config / feature_flag / audit / dashboard`）的 concrete 实现收敛到 `internal/modules/admin/`，并在 owner 清晰处下沉最小必要 repository concrete，同时保持 `internal/app` 为唯一 composition root。
- Batch 4 已完成：`AdminService`、`FeatureFlagService` 与 admin/support helper concrete 已下沉到 `internal/modules/admin/app/`，`config / audit / dashboard` concrete repository 已下沉到 `internal/modules/admin/repository/`，旧 `internal/service` / `internal/repository` 文件已退化为薄 shim。
- Batch 4 独立验证已通过：所有 scoped 测试、全量 `go test ./...`、`go vet ./...`、保护项检查均通过；唯一残留是 dirty worktree 导致“未顺手迁移无关模块”的历史归因只能部分验证，但不构成本轮失败。
- 用户已明确继续下一批；按既定方案，本轮进入 Batch 5：`mcp façade`。
- Batch 5 目标：把 `MCPReadService` 与 `MCPControlService` concrete 下沉到 `internal/modules/mcp/app/`，保持其 façade 定位，不去认领 auction/order/live_session 等真实 owner repository，同时继续保持 `internal/app` 为唯一 composition root。

## Completed
### n1 @omc-planner — DONE_WITH_CONCERNS
- Completed At: 2026-06-05T18:01:36+08:00
- Summary: 产出第二轮逐模块搬 service/repository 实现的可执行迁移方案，按低耦合叶子模块到核心交易链路分 6 批推进，并建议先做 `auth + live_analysis`。
- Deliverables: `/Users/bytedance/study/AI电商/backend/.oh-my-code/plans/service-repository-relocation-round2-plan-20260605.md`
- Findings: `internal/app` 仍作为唯一 composition root；shared support（如 `UserRepository`、`TxManager`、Redis realtime/lock）不宜在首批强拆 owner；首批 `auth + live_analysis` 风险最低。

### n2 @omc-deep-executor — DONE
- Completed At: 2026-06-05T18:17:43+08:00
- Summary: 完成第二轮首批 `auth + live_analysis` 实现搬迁，将 auth/live_analysis service concrete 下沉到模块内，并将 live_analysis report MySQL repository concrete 下沉到模块 repository，同时保留最小 shim 维持旧 wiring 调用面。
- Deliverables: `internal/modules/auth/app/service.go`、`internal/modules/live_analysis/app/service.go`、`internal/modules/live_analysis/repository/mysql.go`；更新 `internal/modules/auth/app/usecases.go`、`internal/modules/live_analysis/app/usecases.go`；瘦身 `internal/service/auth.go`、`internal/service/live_analysis.go`、`internal/repository/live_analysis_report.go`。
- Findings: `go test ./internal/modules/auth/... ./internal/modules/live_analysis/...`、`go test ./internal/transport/http ./internal/transport/mcp ./internal/app`、`go test ./internal/service`、`go test ./...`、`go vet ./...` 通过；中途修复一次未使用 import。

### n3 @omc-verifier — DONE_WITH_CONCERNS
- Completed At: 2026-06-05T18:17:43+08:00
- Summary: 独立验证首批 `auth + live_analysis` scoped 验收，确认 concrete 实现已下沉、shim 兼容有效、composition root 与保护项保持；发现 `internal/service/auth.go` 仍保留辅助函数，但不阻塞本轮通过。
- Deliverables: scoped 文件验证、wiring 验证、保护项检查、协议面 scoped 归因、全量 `go test ./...` / `go vet ./...` 结果。
- Findings: 验收项 1~3、5~12 通过；第 4 项基本通过但存在“非纯 shim”关注点；`gateway.draining` 属于前序 ws-gateway out-of-scope 差异。

### n4 @omc-deep-executor — DONE
- Completed At: 2026-06-05T19:15:20+08:00
- Summary: 完成第二轮 Batch 2 `ai + risk` 实现迁移，将 AI 助手与风险控制的 concrete service 下沉到模块内，并将 risk owner repository concrete 下沉到模块 repository；旧 `internal/service` / `internal/repository` 对应文件退化为薄 shim。
- Deliverables: `internal/modules/ai/app/service.go`、`internal/modules/risk/app/service.go`、`internal/modules/risk/repository/repository.go`；更新 `internal/service/ai_assistant.go`、`internal/service/risk.go`、`internal/repository/risk.go`、`internal/repository/dashboard.go`。
- Findings: `internal/app` wiring 调用面保持不变；通过 `SnapshotEvents()` 修复迁移后 dashboard 对内存 risk repo 私有字段的访问。

### n5 @omc-verifier — DONE_WITH_CONCERNS
- Completed At: 2026-06-05T19:15:20+08:00
- Summary: 独立验证 Batch 2 `ai + risk` scoped 验收，确认 concrete 下沉、shim 兼容、composition root 与保护项保持，且全量测试/vet 通过；dirty worktree 导致“未顺手迁移无关模块”的历史归因只能部分验证。
- Deliverables: scoped 文件核验、wiring/构造器搜索、保护项检查、协议面 scoped 检查、全量 `go test ./...` / `go vet ./...` 结果。
- Findings: 验收项 1~5、7~12 通过；第 6 项做到了部分验证并未发现明确违规 concrete 迁移；`gateway.draining` 属于前序 ws-gateway out-of-scope 差异。

### n6 @omc-deep-executor — DONE
- Completed At: 2026-06-05T19:57:49+08:00
- Summary: 完成第二轮 Batch 3 `deposit + marketplace` 实现迁移，将 deposit/marketplace 的 concrete service 下沉到模块 app，并将 deposit owner repository concrete 下沉到模块 repository；旧 `internal/service` / `internal/repository` 对应文件退化为薄 shim。
- Deliverables: `internal/modules/deposit/app/service.go`、`internal/modules/deposit/repository/repository.go`、`internal/modules/marketplace/app/service.go`；更新 `internal/service/deposit.go`、`internal/service/marketplace.go`、`internal/repository/deposit.go`。
- Findings: `internal/app` wiring 调用面保持不变；marketplace 因 owner repository 不清晰，本批仅迁 service，不扩散迁移额外 repository。

### n7 @omc-verifier — DONE_WITH_CONCERNS
- Completed At: 2026-06-05T19:57:49+08:00
- Summary: 独立验证 Batch 3 `deposit + marketplace` scoped 验收，确认 concrete 下沉、shim 兼容、composition root 与保护项保持，且全量测试/vet 通过；dirty worktree 导致“未顺手迁移无关模块”的历史归因只能部分验证。
- Deliverables: scoped 文件核验、wiring/构造器搜索、保护项检查、协议面 scoped 检查、全量 `go test ./...` / `go vet ./...` 结果。
- Findings: 验收项 1~5、7~12 通过；第 6 项做到了部分验证并未发现明确违规 concrete 迁移；`gateway.draining` 属于前序 ws-gateway out-of-scope 差异。

### n8 @omc-deep-executor — DONE
- Completed At: 2026-06-05T20:30:55+08:00
- Summary: 完成第二轮 Batch 4 `admin / support` 实现迁移，将 admin/feature_flag 及 support helper concrete 下沉到 `internal/modules/admin/app/`，并将 `config / audit / dashboard` concrete repository 下沉到 `internal/modules/admin/repository/`；旧 `internal/service` / `internal/repository` 对应文件退化为薄 shim。
- Deliverables: `internal/modules/admin/app/service.go`、`internal/modules/admin/repository/repository.go`；更新 `internal/service/admin.go`、`internal/service/feature_flag.go`、`internal/service/blacklist_strategy.go`、`internal/repository/config.go`、`internal/repository/audit.go`、`internal/repository/dashboard.go`；删除 `internal/repository/config_memory.go`、`internal/repository/audit_memory.go`。
- Findings: 为消除循环依赖收窄了 `internal/modules/admin/ports/ports.go`；在 `internal/service/auction.go` 增补 `AdminUpdateStatus(...)` 作为 admin 最小桥接；`internal/app` wiring 调用面保持不变。

### n9 @omc-verifier — DONE_WITH_CONCERNS
- Completed At: 2026-06-05T20:30:55+08:00
- Summary: 独立验证 Batch 4 `admin / support` scoped 验收，确认 concrete 下沉、shim 兼容、composition root 与保护项保持，且全量测试/vet 通过；dirty worktree 导致“未顺手迁移无关模块”的历史归因只能部分验证。
- Deliverables: scoped 文件核验、wiring/构造器搜索、保护项检查、协议面 scoped 检查、全量 `go test ./...` / `go vet ./...` 结果。
- Findings: 验收项 1~5、7~12 通过；第 6 项做到了部分归因验证；`gateway.draining` 属于前序 ws-gateway out-of-scope 差异。

## Pending
### n10 @omc-deep-executor — gate:g2
- Goal: 按既定路线实施第二轮 Batch 5：`mcp façade` 实现迁移。
- Acceptance: 将 `MCPReadService` 与 `MCPControlService` concrete 下沉到 `internal/modules/mcp/app/`；旧 `internal/service/mcp_*.go` 退化为最小 shim/alias/constructor 转发；保持 `internal/app` 为唯一 composition root；不去认领其它模块 owner repository；完成格式化、自检与必要测试修正。

### n11 @omc-verifier — depends:n10 — gate:g3
- Goal: 独立验证 Batch 5 `mcp façade` 实现迁移是否满足 scoped 验收标准。
- Acceptance: 执行 scoped 测试 / vet / 保护项检查并给出通过/失败/环境阻塞证据。

# Harness Graph: interface-convergence-phase2-20260605

> Status: completed

## User Intent
- Original: 继续做“代码分层改造第 2 阶段：接口收敛”。让 HTTP handler 尽量依赖最小 use case 接口，而不是完整 concrete service struct；保持 API、WebSocket 协议、测试行为不变；不迁移业务目录，不新增 cmd，不改 OpenAPI。
- Acceptance: 指定 handler 的构造函数改为最小 use case/interface；concrete service 自然满足接口；app route/service wiring 继续注入 concrete service；必要测试通过；保护项确认。

## Assurance
- Mode: strict_verify
- Why: 这是跨 handler/service wiring 的结构性改造，必须保持公开 API/WS 行为和全量测试稳定。

## Gates
### g1 implementation-complete
- Status: passed
- Acceptance: 已阅读指定文件；完成 Auth/Auction/Bid/Hammer/LiveSession/Order/Admin/Marketplace/LiveAnalysis/AI/MCP/Risk 等现有 handler 所需接口收敛；不做第 3 阶段/业务迁移；自测通过。
- Repair: 0/2

### g2 verification-pass
- Status: passed
- Acceptance: 独立验证 handler 构造函数不再直接要求完整 concrete service；app wiring 注入正常；保护项未破坏；`go test ./internal/transport/http ./internal/service`、`go test ./...`、`go vet ./...` 通过。
- Repair: 1/2

## Understanding
- 仓库主目录：`/Users/bytedance/study/AI电商/backend`。
- 第 1 阶段 app wiring 已拆分；`internal/app/server.go` 仍为唯一 composition root。
- 当前已有 `internal/transport/http/usecases.go`，前序已覆盖部分 auction/live_session/order/ws/image 接口；本轮需要扩展到用户列出的核心 handler。
- 约束：不改 `AGENTS.md`；不改 `docs/API/*.openapi.json` / `*.apifox.json`；不新增 `cmd/api-backend` 或 `cmd/ws-gateway`；不迁移 domain/service/repository 到 modules；不做第 3 阶段构造系统性改动。
- 实现节点完成接口收敛：扩展 `internal/transport/http/usecases.go`，覆盖 Auth/Auction/Bid/Deposit/Hammer/LiveSession/Order/Admin/Marketplace/LiveAnalysis/AI/Risk；MCP transport 定义 MCPRead/MCPControl/AI notifier 最小接口；相关 handler 构造函数改为接口依赖；app wiring 继续注入 concrete service。
- 实现节点自测：`go test ./internal/transport/http ./internal/service`、`go test ./...`、`go vet ./...` 通过；保护项自检通过，仍需独立验证。
- 首轮独立验证：接口收敛、app wiring、保护项和测试/vet 均通过；但以 HEAD 为基准发现 `internal/transport/ws/envelope.go` 新增 `gateway.draining`，判定当前 worktree 不满足“不改变 WebSocket envelope 类型”。
- 范围重判：`internal/transport/ws/envelope.go` 在本轮开始前的系统 GitStatus 中已经是已修改文件，属于前序 ws-gateway 改动；本轮接口收敛实现节点未报告修改该文件。因此需要按本轮范围复验，不将前序 worktree 差异作为本轮 gate failure。
- Scoped 复验 PASS：HTTP/MCP handler 构造函数已收敛到最小 usecase/interface；app wiring concrete 注入有效；REST route literal 与请求/响应 tag 在 scoped diff 中无变化；保护项通过；指定测试/vet 通过。
- 非阻断关注：worktree 仍包含本轮开始前已有的 WS/gateway/docs/deploy 等大量改动，提交前建议按阶段拆分归因。

## Completed
### n1 @omc-deep-executor — DONE
- Completed At: 2026-06-05T00:00:00+08:00
- Summary: 完成第 2 阶段接口收敛，HTTP/MCP handler 从 concrete service 指针转为最小 usecase/interface；app wiring 维持 concrete service 注入；订单超时 worker 做轻量接口收敛；自测通过。

### n2 @omc-verifier — FAILED
- Completed At: 2026-06-05T00:00:00+08:00
- Summary: 验证确认接口收敛和测试/vet 通过，但因前序 `internal/transport/ws/envelope.go` 相对 HEAD 存在 `gateway.draining` 类型新增而判定失败。该文件为本轮开始前已存在修改，需要 scoped re-verify。

### n3 @omc-verifier — DONE_WITH_CONCERNS
- Completed At: 2026-06-05T00:00:00+08:00
- Summary: 按本轮接口收敛范围复验通过；前序 WS envelope 改动不计入本轮 gate。关注项为 worktree 混有前序改动，影响提交归因但不阻塞本轮验收。

## Pending

# Harness Graph: deps-injection-phase3-20260605

> Status: completed

## User Intent
- Original: 进入代码分层改造第 3 阶段，减少 `AuctionService`、`BidService`、`HammerService`、`LiveSessionService` 的 `SetXxx` 后注入，把核心必需依赖改为 `XxxServiceDeps` 构造注入，保留真正可选 hook/避免循环依赖的 setter；保持行为、API/WS 协议和测试不变。
- Acceptance: 四个核心 service 的关键依赖经 Deps 显式构造注入；`internal/app/service_wiring.go` 连续 `SetXxx` 明显下降；保留 setter 有原因；指定测试、全量测试和 vet 通过；保护项通过。

## Assurance
- Mode: strict_verify
- Why: 涉及核心竞拍/出价/落槌/直播间 service 初始化顺序，错误可能导致半初始化或行为回归，必须独立验证。

## Gates
### g1 implementation-complete
- Status: passed
- Acceptance: 统计当前 SetXxx；扩展/使用四个 service 的 Deps 构造；app wiring 减少核心 setter；必要测试 fixture 更新；gofmt 和自测通过。
- Repair: 0/2

### g2 verification-pass
- Status: passed
- Acceptance: 独立验证 Deps 字段、setter 减少与保留原因、接口兼容、保护项；运行 `go test ./internal/service`、`go test ./internal/transport/http ./internal/transport/mcp`、`go test ./internal/app`、`go test ./...`、`go vet ./...`。
- Repair: 0/2

## Understanding
- 仓库主目录：`/Users/bytedance/study/AI电商/backend`。
- 第 1 阶段 app wiring 拆分已完成；第 2 阶段 handler/usecase 接口收敛已完成。
- 本轮明确不做第 4 阶段模块目录迁移，不新增 cmd，不改 AGENTS.md，不改 docs/API OpenAPI/Apifox，不改变 REST/WS 协议。
- 当前四个 service 已存在初步 `XxxServiceDeps` 与 `NewXxxServiceWithDeps`，本轮应优先扩展它们而不是新增平行构造函数。
- 实现节点完成：`AuctionServiceDeps` 新增 `OnClose`；`HammerServiceDeps` 新增 `OnClose`；Bid/LiveSession 使用既有 Deps 覆盖核心依赖；app service wiring 中核心连续 setter 从 5 个降至 1 个，仅保留 `hammerService.SetLiveSessionService` 以避免构造期循环依赖。
- 实现节点更新测试 fixture：`bid_hammer_deposit_order_test.go`、`bid_pre_reject_test.go`、`live_agent_hook_test.go`、`mcp_control_test.go`、`ws_handler_test.go` 等改用 Deps 构造；自测指定命令和 vet 通过。
- 独立验证 PASS：四个 service 的关键依赖已由 Deps 构造注入；app wiring 仅保留 `hammerService.SetLiveSessionService` 这一必要循环依赖 setter；指定测试和 `go vet ./...` 通过；保护项通过。
- 非阻断关注：当前 worktree 仍有前序 WS/gateway/transport 与 `internal/modules` 改动，建议提交前拆分归因；未发现本轮 Deps 注入破坏公开 API/WS 或执行第 4 阶段迁移。

## Completed
### n1 @omc-deep-executor — DONE
- Completed At: 2026-06-05T00:00:00+08:00
- Summary: 完成第 3 阶段 Deps 显式构造注入，减少 app wiring 后注入并标注保留 setter 原因；测试 fixture 已更新，自测通过。

### n2 @omc-verifier — DONE_WITH_CONCERNS
- Completed At: 2026-06-05T00:00:00+08:00
- Summary: 独立验证通过；关注项为 worktree 混有前序改动，影响提交归因但不阻塞本轮验收。

## Pending

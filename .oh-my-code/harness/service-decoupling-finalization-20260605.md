# Harness Graph: service-decoupling-finalization-20260605

> Status: in_progress

## User Intent
- Original: 先清掉模块 app/ports 的基础设施泄漏。再把 internal/service 里的真实实现迁到模块或 app runtime。最后把生产代码对 internal/service 的 import 全部替换掉。跑 go test ./...、go vet ./...，确认 protected paths 没被误动。
- Acceptance: 在不误动 protected paths 的前提下，完成模块 app/ports 基础设施泄漏清理、迁走 `internal/service` 中剩余真实实现到模块或 app runtime、替换生产代码对 `internal/service` 的 import，并通过 `go test ./...`、`go vet ./...` 与 protected-path 校验。

## Assurance
- Mode: strict_verify
- Why: 这是跨层重构与依赖收口，错误会级联影响 wiring、模块边界与运行时行为，必须以独立验证作为收口门禁。

## Gates
### g1 boundary-and-import-clean
- Status: passed
- Acceptance: 模块 `app/ports` 中基础设施泄漏被清理；生产代码不再 import `internal/service`；剩余真实实现已迁至模块或 app runtime。
- Repair: 0/2

### g2 build-and-protected-paths-green
- Status: failed
- Acceptance: `go test ./...` 与 `go vet ./...` 通过，且 protected paths 未被误动。
- Repair: 0/2

## Understanding
- 本任务是此前分层重构的继续收口，用户要求按“边界清理 → 实现迁移 → import 替换 → 独立验证”顺序完成。
- `internal/app` 仍需保持唯一 composition root；验证需覆盖 protected paths 未被误动。
- 勘探确认泄漏重点不在 `ports`，而在 `app`：`internal/modules/auth/app/service.go`、`internal/modules/admin/app/service.go`、`internal/modules/deposit/app/service.go`、`internal/modules/auction/app/{service,bid_service,hammer_service}.go`。
- `internal/service/auth.go` 中的 `HTTPStatusAndCode` 是生产 import 清理的公共前置；`service/product_description.go` 是 DTO/contract 收口的另一关键源头。
- `internal/service` 中仍有真实 runtime/worker 实现残留，主要集中在 `live_agent_hook.go`、`risk_control.go`、`timer.go`、`deposit_reconciler.go`、`bid_record_worker.go`、`bid_ranking_worker.go`、`kafka_workers.go`。
- 生产代码 import `internal/service` 的主要消费面集中在 `internal/app/{server,service_wiring,route_wiring,worker_wiring}.go`、`internal/transport/http/*`、`internal/transport/mcp/*`、`internal/infra/agent/product_description.go`、`internal/infra/tts/doubao.go`。
- 需要保护或最小化改动的高风险共享写面包括：`docs/`、`deploy/`、`.oh-my-code/`、`configs/`、`.env.example`、`internal/config/`、`internal/repository/`、`internal/infra/{redis,observability}/`、`internal/transport/ws/`；`internal/app/*_wiring.go` 虽需修改，但要严格限域。
- 实现节点已将多类 runtime/worker 真实实现迁入 `internal/app/runtime/`，并把部分公共 contract / telemetry abstraction 收敛到模块边界；当前待独立验证确认 `internal/service` 是否仅剩薄 shim/adapter、生产代码 import 是否清零、以及 protected paths 是否未被误动。
- 独立验证确认：`internal/modules/**/{app,ports}` 维度的 infra 泄漏已基本清理干净，但生产代码仍在 import `aieas_backend/internal/service`，且 `internal/service/authz.go`、`internal/service/risk_control.go` 仍属于真实实现残留。
- 独立验证同时确认：`go test ./...` 与 `go vet ./...` 均通过，但当前工作树中的 protected paths 仍存在大量改动，按“未被误动/保持 green”口径不能判定通过。
- 修复节点已进一步收口剩余缺口：`internal/app/{event_wiring,repository_wiring,route_wiring,server,service_wiring,worker_wiring}.go` 已去除生产态对 `internal/service` 的 direct import；`internal/service/authz.go` 已删除，`internal/service/risk_control.go` 已改为指向 `internal/modules/risk/app` 的薄兼容层。
- 最终复验确认：`internal/modules/**/{app,ports}` 维度的基础设施泄漏已清理；生产代码对 `internal/service` 的 import 已清零；`internal/service` 仅剩薄 shim/alias/adapter/兼容层，因此 g1 通过。
- 最终复验同时确认：`go test ./...` 与 `go vet ./...` 均通过；但 protected paths 当前工作树存在大量已改动项，且在未提供可归因基线（commit range / diff range）的前提下，无法证明这些改动不是本轮误卷入，因此 g2 仍失败。

## Completed
### n1 @omc-explore — DONE_WITH_CONCERNS
- Completed At: 2026-06-05T00:00:00+08:00
- Summary: 已完成泄漏/残留/消费面/保护路径勘探；确认 `ports` 未命中典型基础设施泄漏，优先改造面集中在 `app` 泄漏、`service/auth.go` 与 `service/product_description.go` 这两个公共源头，以及 app/transport/infra 的生产 import 消费面。

### n2 @omc-deep-executor — DONE_WITH_CONCERNS
- Completed At: 2026-06-06T00:00:00+08:00
- Summary: 已完成主要实现迁移与消费面替换：多个 runtime/worker 已迁入 `internal/app/runtime/`，模块 app 的部分基础设施泄漏已改为 app-local abstraction / runtime adapter，生产代码对 `internal/service` 的消费面已被大幅替换；但仍需独立验证确认 import 是否已清零、`internal/service` 是否仅剩薄层、以及 protected paths 未被误动。

### n3 @omc-verifier — DONE
- Completed At: 2026-06-06T00:00:00+08:00
- Summary: 独立验证结论为两道 gate 均未通过：模块 app/ports 泄漏维度已基本达标，但生产代码仍 import `internal/service`，且 `internal/service/authz.go`、`internal/service/risk_control.go` 仍是非薄层真实实现，因此 g1 失败；`go test ./...` 与 `go vet ./...` 通过，但 protected paths 当前工作树并不 green，因此 g2 失败。

### n4 @omc-deep-executor — DONE
- Completed At: 2026-06-06T00:00:00+08:00
- Summary: 已修复 verifier 暴露的剩余缺口：`internal/app` 指定的 6 个生产文件已去除对 `internal/service` 的 import；`internal/service/authz.go` 已删除，`internal/service/risk_control.go` 已收敛为指向 `internal/modules/risk/app` 的薄兼容层；并完成了 gofmt、import 扫描、局部测试与全量构建烟检。

### n5 @omc-verifier — DONE
- Completed At: 2026-06-06T00:00:00+08:00
- Summary: 最终复验结论为 g1 通过、g2 失败：模块 app/ports 泄漏、生产 import 清理、`internal/service` 残余真实实现三项均已达标；`go test ./...` 与 `go vet ./...` 也已通过。唯一剩余阻断是 protected paths 当前工作树归因不可证，按保守口径不能判定为 green。

## Pending

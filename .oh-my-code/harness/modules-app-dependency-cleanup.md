# Harness Graph: modules-app-dependency-cleanup

> Status: in_progress

## User Intent
- Original: 应优先清理 modules/*/app 对 internal/repository、internal/transport/ws、Redis client、observability concrete package 的直接依赖。
- Acceptance: 识别并优先清理 `internal/modules/*/app` 对 `internal/repository`、`internal/transport/ws`、Redis concrete client/package、observability concrete package 的直接依赖；在不改变协议/组合根约束的前提下完成必要实现与验证。

## Assurance
- Mode: strict_verify
- Why: 这是模块边界收口工作，会影响后续模块演进；需要保证编译、测试与边界清理同时成立。

## Gates
### g1 dependency-boundary-clean
- Status: failed
- Acceptance: 目标范围内高优先级 `modules/*/app` 直接依赖被消除或显著收敛，且替换为模块 ports / 抽象接口 / 组合根适配。
- Repair: 0/2

### g2 build-and-test-green
- Status: passed
- Acceptance: 至少通过目标范围测试；如改动影响较广，则通过 `go test ./...` 与 `go vet ./...`。
- Repair: 0/2

## Understanding
- 当前 Round 2 Batch 6 已把 core trade 的部分 concrete service/repository 迁入模块，但 `modules/*/app` 仍可能残留对 `internal/repository`、`internal/transport/ws`、Redis / observability concrete 包的直接依赖。
- 需要优先做边界清理，而不是新增组合根或改协议。
- 探索结果显示高优先级清理面集中在核心 trade：`internal/modules/auction/app/bid_service.go`、`internal/modules/auction/app/service.go`、`internal/modules/auction/app/hammer_service.go`、`internal/modules/live_session/app/service.go`。
- `order/app` 在本次四类目标上未命中，可暂不处理。
- `admin/app` 命中 Redis concrete client，`deposit/app` 命中 observability concrete，但不属于本轮最高优先级。
- 实现节点已完成高优先级清理：`auction/app` 已改为依赖 module ports / app-local 抽象，去掉对 `internal/repository`、`internal/transport/ws`、observability concrete package 的直接依赖；`live_session/app` 已去掉对 `internal/repository` 的直接依赖，并将默认 fallback 内聚为 app 内部实现。
- `internal/service` 与 `internal/app/service_wiring.go` 已新增适配注入，保持 `internal/app` 为唯一 composition root。

## Completed
### n1 @omc-explore — DONE
- Completed At: 2026-06-05T00:00:00+08:00
- Summary: 已完成 `internal/modules/*/app` 直接依赖扫描；高优先级整改面锁定在 `auction/app` 与 `live_session/app`，`order/app` 相对干净。

### n2 @omc-deep-executor — DONE
- Completed At: 2026-06-05T00:00:00+08:00
- Summary: 已完成 `auction/app` 与 `live_session/app` 的高优先级 concrete 依赖清理，并通过实现侧自检。

### n3 @omc-verifier — DONE
- Completed At: 2026-06-05T00:00:00+08:00
- Summary: 独立验证确认 `auction/app` 与 `live_session/app` 已清掉 `internal/repository`、`internal/transport/ws`、Redis concrete client 及 observability concrete package 的直接依赖；但 `auction/app` 仍直接 import OTel concrete tracing API（`go.opentelemetry.io/otel/attribute` / `codes`），因此 g1 未通过。`go test ./...` 与 `go vet ./...` 均通过，g2 通过。

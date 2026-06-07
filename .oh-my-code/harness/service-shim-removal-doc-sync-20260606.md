# Harness Graph: service-shim-removal-doc-sync-20260606

> Status: in_progress

## User Intent
- Original: ：删除 service shim、清理跨模块 app 引用、同步文档。
- Acceptance: 删除不再需要的 `internal/service` shim；清理生产代码中的跨模块 `app` 直接引用，尽量收敛到各模块 `ports` / 本地抽象 / `internal/app` runtime；文档不同步不再作为阻断项；并通过构建/测试与独立验证收口。

## Assurance
- Mode: strict_verify
- Why: 这是上一轮分层收口后的继续清理，涉及删除兼容层、调整模块边界与文档同步，错误会级联影响 wiring、引用面和后续演进。

## Gates
### g1 shim-and-boundary-clean
- Status: passed
- Acceptance: 不再需要的 `internal/service` shim 被删除；跨模块 `app` 直接引用被清理或显著收敛；替代边界清晰且不引入新的 composition root。
- Repair: 0/2

### g2 build-and-doc-sync-green
- Status: passed
- Acceptance: 文档不同步不作为阻断项；`go test ./...` 与 `go vet ./...` 通过；代码变更范围与用户意图一致。
- Repair: 0/2

## Understanding
- 当前请求是此前 service decoupling 收口后的下一阶段，目标从“保留薄兼容层”推进到“进一步删除 shim、清理跨模块 app 引用，并补齐文档同步”。
- 这次不仅是代码修改，还包含文档同步，因此需要先识别哪些 shim 现在已真正无消费方、哪些跨模块 app 依赖仍存在、以及哪些文档需要最小同步。
- 勘探确认：生产代码已不再 import `internal/service`，剩余阻塞主要来自测试文件；因此 shim 删除可分两批推进：一批纯 shim 可直接删除，另一批需先迁测试引用。
- 当前真正的生产边界问题集中在跨模块 `app` 直接引用，而非 `internal/service` 本身，重点在 `internal/app/service_wiring.go`、`internal/app/route_wiring.go`、`internal/transport/http/*`、`internal/transport/mcp/types.go`，以及少数 module/ports 内部仍反向引用别的 module `app`。
- 需要同步的最小文档面已收敛到：`AGENTS.md` 与 `docs/` 下明确描述旧 `internal/service` 分层/旧路径的设计与协议文档；`README.md` 为空，不是本轮重点。
- 实现节点已删除一批纯 shim，并用测试专用兼容桥接维持现有测试；同时收敛了 transport/app wiring 的部分 concrete app 依赖，并同步了 `AGENTS.md` 与若干 `docs/*.md` 对旧 `internal/service` 主业务层的描述。剩余是否完全达标，需要独立验证确认。
- 最终复验确认：A-list shim 已删除，`internal/service` 未重新进入生产主链路；跨模块 `app` 直接引用已显著收敛，且未引入新的 composition root，因此 g1 通过。
- 用户已明确说明开发过程文档不重要，文档同步不再作为阻断项；当前剩余待确认点只剩代码范围纯度是否可接受，以及在新口径下 `go test ./...` / `go vet ./...` 是否足以让 g2 通过。
- 新口径下复验确认：`go test -count=1 ./...` 与 `go vet ./...` 均通过，且未发现仍能单独阻断 g2 的最小代码侧问题；因此本轮在忽略文档阻断后可以收口。

## Completed
### n1 @omc-explore — DONE_WITH_CONCERNS
- Completed At: 2026-06-06T00:00:00+08:00
- Summary: 已完成 shim 删除/跨模块 app 引用/文档同步范围勘探：纯 shim 可直接删除，测试仍引用的 shim 需先迁测试；生产边界问题集中在 app wiring、transport 与少数 module/ports 的 direct app 引用；文档最小同步面集中在 `AGENTS.md` 与若干仍描述旧 `internal/service` 分层的 `docs/*.md`。

### n2 @omc-deep-executor — DONE
- Completed At: 2026-06-06T00:00:00+08:00
- Summary: 已删除一批 A-list service shim，保留测试专用兼容桥接以避免恢复生产 shim；修复了 `internal/app/server.go` 的接口边界问题，收敛了部分 transport/app wiring 的 concrete app 依赖，并同步了 `AGENTS.md` 与多份仍描述旧 `internal/service` 分层/路径的文档；`gofmt`、`go test ./...`、`go vet ./...` 均已通过实现侧自检。

### n3 @omc-verifier — DONE
- Completed At: 2026-06-06T00:00:00+08:00
- Summary: 最终独立验收结论为 g1 通过、g2 失败：A-list shim 删除、`internal/service` 生产边界退出、跨模块 `app` 直连显著收敛、`go test ./...` 与 `go vet ./...` 均已达标；但文档同步仍残留旧 `service` 主业务层/旧装配路径描述，且当前工作树存在明显超出本任务范围的扩张改动，故不能判定整体通过。

### n4 @omc-verifier — DONE
- Completed At: 2026-06-06T00:00:00+08:00
- Summary: 按“开发过程文档不重要”的新口径复验后，确认本轮可以收口：A-list shim 删除、`internal/service` 未回到生产主链路、跨模块 `app` 直连已显著收敛、未引入新的 composition root，且 `go test -count=1 ./...` 与 `go vet ./...` 均通过；未发现仍能单独阻断 g2 的最小代码侧问题。

## Pending

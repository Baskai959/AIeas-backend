# Harness Graph: auction-consistency-p0

> Status: completed

## User Intent
- Original: 按差距分析清单的 P0 三项做修正；P0-1 不能全局把 `MySQLAuctionRepository.Update` 改为带 `version + status IN (...)` 条件，必须新增专用方法 `CloseWithVersion(ctx, auction, expectedVersion, allowedFromStatuses)` 仅在落槌关闭路径用乐观锁；普通 Update 保持现状
- Acceptance: P0-1 落槌乐观锁（专用方法）、P0-2 启动拍卖 TCC 中间态（WARMING_UP）、P0-3 出价热路径去前置 MySQL 查询；普通 Update 全量调用未被改动；测试 + 构建 + vet 全绿

## Assurance
- Mode: strict_verify
- Why: schema 变更 + 状态机分支，blast radius 中等不可逆；已通过独立 verifier 校验未越界

## Gates
### g1 implementation-acceptance
- Status: passed (2026-05-28T00:00:00+08:00)
- Acceptance: 通过 — A/B/C/D/E 全部命中证据；CAS 仅作用于 hammer，普通 Update 调用方未被改动；Lua 未改；越界文件均未触碰

## Understanding
- 状态机原本就允许 `READY → WARMING_UP → RUNNING`，本轮无需扩状态机
- `CloseWithVersion` 唯一调用点 `internal/service/hammer.go:185`
- `ErrOptimisticConflict` sentinel 落在 `internal/domain/user.go:70`
- 新 migration 编号 `00012_auction_lot_version.sql`
- 5 项新增单测全部 PASS；`go build` / `go vet` / `go test ./...` 全绿
- Verifier 提了 1 个非阻塞观察：`hammer.go:171` 在 CAS 前赋值 `current.Status = result.Status`，当前 SQL 语义无误（依赖 expectedVersion + allowedFromStatuses 白名单），可后续清理

## Completed
### n1 @omc-deep-executor — DONE
- Completed At: 2026-05-28T00:00:00+08:00
- Summary: 实施 P0 三项；新增 `CloseWithVersion` 专用方法（MySQL + Memory）、`ErrOptimisticConflict`、migration 00012、`docs/ddl.sql` 同步、`startWithTiming` TCC 化、`bid.place` 在 stream-enabled 跳过 `FindByRequestID`；新增 5 项单测；自检命令全绿
### n2 @omc-verifier — DONE — gate:g1 passed
- Completed At: 2026-05-28T00:00:00+08:00
- Summary: 五维度逐项核验通过；反向 grep 确认 `MySQLAuctionRepository.Update` 调用方未被改成 CAS、`CloseWithVersion` 仅 hammer 调用、Lua 未改、限流/Pub-Sub/Feature Flag/Reconciler/WS snapshot 等 D 项均未越界；构建测试全部通过

## Pending
（无）

# Harness Graph: auction-consistency-p1

> Status: completed (with concerns)

## User Intent
- Original: 继续推进 P1（Deposit Reconciler / WS 握手 snapshot+serverTime / AutoClosed 守卫）
- Acceptance: P1-A/B/C 三项落地、build/vet/test 全绿、P0 套不回归、P2/P3 范围未越界

## Assurance
- Mode: strict_verify
- Why: 三项独立但都涉及在线行为；已通过独立 verifier 校验关键路径

## Gates
### g1 implementation-acceptance
- Status: passed-with-concerns (2026-05-28T00:00:00+08:00)
- Acceptance: A/B/C 守卫主体均按规范落地；D 越界检查无红线；E 全绿。Concern：`bid.go` 在执行期间被并行的黑名单策略改动捎带（新增 `bidAboveAllowedMax` helper + 拒因调整），与 P1-C 守卫无关但混入同一文件，记录为非阻塞 concern。

## Understanding
- P1-A：`internal/service/deposit_reconciler.go` 与 BidRecordReconciler 同形；状态白名单硬编码 `RUNNING/EXTENDED/HAMMER_PENDING/WARMING_UP`；30s 默认间隔；`server.go:370-374` 装配 + `OnShutdown` 注册优雅关闭。
- P1-B：`TypeRoomSnapshot` 常量；`ws_handler.go:150 deliverRoomSnapshot` 在 upgrade 后立即 push；payload 含 `currentPrice/leaderBidderId/endTime/seq/status/serverTime + degraded/source`；RT-first 兜底 DB；2 条新测试 PASS。
- P1-C：`bid.go:321` 守卫 `Accepted && !Duplicate && AutoClosed && hammer != nil`；`TestBidServiceCapPriceDuplicateDoesNotInvokeHammer` 用 `auction_hammer_total/duplicate_total` 指标断言。
- P0 套（5 条）+ deposit_reconciler 6 条 + WS snapshot 2 条全部 PASS；`go build/vet/test ./...` 全绿。
- 越界检查：限流仍单机内存、Lua 未改、无 Pub/Sub Subscriber、无 Kafka 依赖、无 Feature Flag、无 WS Gateway 拆分、CloseWithVersion 仍仅 `hammer.go:185` 单点调用、Update 调用方未被改动。
- 非阻塞 concern：`bid.go:252,405-419` 黑名单策略相关改动 + `bid_hammer_deposit_order_test.go:312` 拒因调整与 P1-C 无关，建议下次拆分提交。

## Completed
### n1 @omc-deep-executor — DONE
- Completed At: 2026-05-28T00:00:00+08:00
- Summary: 实施 P1-A 后台 reconciler（30s ticker / 4 状态白名单 / RT 缺失补齐 / 已注册指标 / OnShutdown 关闭）；P1-B WS 握手主动 push room.snapshot 帧（RT-first + DB 退化 + degraded 标记）；P1-C `!result.Duplicate` 守卫 + 测试；自检 build/vet/test 全绿
### n2 @omc-verifier — DONE — gate:g1 passed-with-concerns
- Completed At: 2026-05-28T00:00:00+08:00
- Summary: 五维度核验通过；A/B/C 守卫主体合规；D 越界全无红线；E 全绿；P0 套不回归。识别到一处与 P1-C 无关的黑名单策略改动捎带在 `bid.go`，建议下轮拆分提交，不阻塞收尾。

## Pending
（无）

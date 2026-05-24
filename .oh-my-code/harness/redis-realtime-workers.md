# Harness Graph: redis-realtime-workers

> Status: completed

## User Intent
- Original: 1.引入 Redis Stream 或等价事件日志，支撑跨实例 WS 补偿与事件广播。
2.在线人数 Redis 化，并处理实例心跳 TTL 与异常退出。
3. 实现异步 `bid_record` worker、失败补偿与对账任务。
- Acceptance: 引入 Redis Stream 或等价事件日志用于跨实例 WS 事件广播与 lastSeq 补偿；在线人数 Redis 化具备实例心跳 TTL 和异常退出清理；bid_record 写入改为异步 worker 并具备失败补偿/对账任务；相关配置、测试和 `go test ./...` 验证通过，或明确环境性阻塞。

## Assurance
- Mode: strict_verify
- Why: 改动覆盖实时事件一致性、在线人数准确性与竞价记录可靠性，必须先设计再实现并由独立验证把关。

## Gates
### g1 design-accepted
- Status: passed
- Acceptance: 方案明确 Redis Stream/事件日志、跨实例 WS 广播/补偿、在线人数心跳 TTL、bid_record worker/失败补偿/对账的接口边界、关键数据结构、测试策略与风险。
- Repair: 1/1

### g2 implementation-verified
- Status: passed
- Acceptance: 代码实现满足用户三项要求，保留现有单测/内存 fallback，新增或更新有效测试，`go test ./...` 通过或失败为明确环境性阻塞。
- Repair: 1/2

## Understanding
- 请求包含三个强相关能力：Redis Stream/事件日志、在线人数心跳 TTL、异步 bid_record worker 与对账。它们共享 Redis 基础设施和实时事件模型，先做设计可降低重复改动和接口错配风险。
- 上一轮已完成基础在线人数 Redis 化：Hub 支持 `OnlineCounter`，生产注入 Redis ZSET 计数器；但验证关注点指出仍缺少实例心跳 TTL/异常退出清理的更完整机制与真实 Redis 专项测试。
- 架构节点建议以每拍场 Redis Stream 作为事实事件日志主干：`auction:{id}:stream` + `auction:{id}:seq`，Stream ID 使用 `{seq}-0`，支撑 WS lastSeq 补偿、跨实例 relay、本地投递和 bid_record worker。
- 架构节点建议在线人数保留 ZSET，但加入 `Touch`、实例心跳 key、实例连接索引与 janitor，异常退出由短 TTL 被动清理 + 主动死实例清理兜底。
- 架构节点建议 bid_record worker 从 Stream 的 bid 事件消费，MySQL 以 request_id 幂等写入，失败通过 PEL/retry/DLQ 处理，对账任务扫描 Stream 补写缺失记录；强一致长期建议把 bid 事件 XADD 放入 `bid.lua`。
- 方案审查未通过：必须补齐 bid 事件与 Redis Lua 的原子边界、Hub 外部 Stream seq 水位/回放模型、本地广播与 EventRelay 去重/单一路径、worker 与现有同步写 `bid_record` 的迁移关系、稳定事件 schema、对账 checkpoint/裁剪处理、在线人数 Join/Touch/Leave/Janitor 状态机和测试策略。
- 修正版设计要求：生产 Stream-enabled 模式下 `bid.lua` 必须原子写 Stream，不允许服务层 accepted 后补写；`BidService.Place` 不再同步写 `bid_record` 或直接房间广播，只返回 `bid.ack`；EventRelay 是唯一事实广播入口；WS 重连补偿必须优先走 Redis Stream；在线人数新增 Join/Touch/Leave/Janitor 状态机；worker 按 Stream 消费、幂等写库、retry/DLQ/对账。
- 复审通过 g1，但实现必须遵守：Lua 中所有可预检失败条件必须在 `XADD` 前完成；EventRelay 需本地去重/顺序保护；worker/reconcile 必须共用幂等冲突规则；Stream 裁剪不能破坏 WS 补偿和 worker/reconcile 滞后窗口。
- 实现节点报告：已完成 Redis Stream 事实事件主干、WS EventRelay/Stream replay、在线人数 Touch/Janitor、异步 bid_record worker/reconcile 的可测试 MVP；`bid.lua` 写 Stream 并返回 seq/streamID；Stream-enabled `BidService` 跳过同步 `persistBid` 和直接事实广播；相关测试与 `go test ./...` 通过。
- 实现节点关注：EventRelay/worker/reconcile 参数尚未接入配置；Reconcile 主要依赖 `auction:active_streams`，尚未合并 MySQL running/recent closed；Stream-enabled 模式主要依赖 Redis blacklist。
- 独立验证未通过 g2：主体代码存在且 `go test -count=1 ./...` 通过，但缺少 Redis Stream/EventRelay/BidService Stream-enabled/OnlineCounter TTL+Janitor/BidRecordWriter/Reconciler 关键路径测试；`go vet ./...` 因 `internal/transport/http/ws_handler_test.go:65` 复制 Hertz Request 失败；Reconciler 只有 RunOnce，未见生产定时调度。
- 修复节点报告：已补 Redis Stream/EventLog、EventRelay、BidService stream-enabled、OnlineCounter、BidRecordWriter、Reconciler 关键路径测试；修复 Hertz Request vet 问题；新增 Reconciler Start 调度并在 server 初始化接入；`go test -count=1 ./...` 与 `go vet ./...` 通过。
- 最终验证确认 g2 通过：Redis Stream/EventLog、WS EventRelay、OnlineCounter TTL+Janitor、BidRecordWriter、BidRecordReconciler 均有实现与针对性测试；Reconciler 已接入 app/server 调度；定向关键路径测试、`go test -count=1 ./...`、`go vet ./...` 均通过。

## Completed
### n1 @omc-architect — DONE_WITH_CONCERNS
- Completed At: 2026-05-23T21:39:20+08:00
- Summary: 给出三项能力的一体化设计，主线为 Redis Stream 事件日志 + EventRelay + OnlineCounter Touch/Janitor + BidRecord worker/DLQ/reconcile，并指出服务层 append bid event 的 crash 窗口风险。

### n2 @omc-critic — DONE_WITH_CONCERNS
- Completed At: 2026-05-23T21:42:47+08:00
- Summary: g1 未通过；方向可行但设计缺少关键约束，不能直接进入实现。

### n2r @omc-architect — DONE
- Completed At: 2026-05-23T21:48:09+08:00
- Summary: 修正设计并补齐 critic 八类问题，明确以 Lua 原子写 Stream、EventRelay 单一路径广播、Stream replay、在线人数状态机、worker 幂等/DLQ/对账作为实现契约。

### n2rv @omc-critic — DONE_WITH_CONCERNS
- Completed At: 2026-05-23T21:50:09+08:00
- Summary: g1 通过；设计足够指导 MVP 实现，但强调 Redis Lua 非自动回滚、EventRelay 幂等投递、worker/reconcile 幂等规则一致等实现约束。

### n3 @omc-deep-executor — DONE_WITH_CONCERNS
- Completed At: 2026-05-23T22:00:32+08:00
- Summary: 完成可测试 MVP 实现并自测通过；留下配置化、MySQL active stream 发现和 DB blacklist 同步等非阻塞关注点。

### n4 @omc-verifier — DONE
- Completed At: 2026-05-23T22:04:44+08:00
- Summary: g2 未通过；确认主体实现存在且全量 go test 通过，但关键路径测试不足、go vet 失败、Reconciler 未调度。

### n5 @omc-deep-executor — DONE
- Completed At: 2026-05-23T22:13:36+08:00
- Summary: 修复 g2 缺口，补齐关键路径测试、修复 go vet、接入 Reconciler 周期调度并自测通过。

### n6 @omc-verifier — DONE
- Completed At: 2026-05-23T22:17:43+08:00
- Summary: g2 通过；确认用户三项需求主体实现、关键路径测试、Reconciler 调度、全量测试与 vet 均满足验收。

## Pending

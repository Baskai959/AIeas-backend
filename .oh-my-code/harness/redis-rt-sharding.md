# Harness Graph: redis-rt-sharding

> Status: completed

## User Intent
- Original: 127.0.0.1:6382 是 RT 的第二个分片节点（不是读写分离备份）。把上一轮拆分中的 `RT.Primary/Secondary` 占位改造为真正的 RT 客户端分片：cache=6380(allkeys-lfu/512mb)、rt-1=6381(noeviction/1gb)、rt-2=6382(noeviction/1gb)。
- Acceptance:
  1. 配置 schema 升级为多 shard 列表（`redis.rt.shards: [{addr,...}, {addr,...}]`），yaml/env/.env.example 同步；Validate 至少 1 shard，所有 shard addr 互不相同且不等于 cache.addr。
  2. 实现 `ShardedRTClient`（应用层路由），所有 RT 组件改为通过 shard key 选 shard；同一 auction/同一 live_session/同一 live_room 的所有 key 落到同一 shard（保证 Lua EVAL 与 multi-key 命令工作）。
  3. Lua 脚本（`bid.lua`/`hammer.lua`）在所有 shard 上 ScriptLoad；NOSCRIPT 自动重试覆盖任意 shard。
  4. Bid Stream（XADD/XREADGROUP/XACK/XPending/XClaim/DLQ/Reconcile checkpoint）在每个 shard 独立运行；`BidRecordWorker` 与 `Reconciler` 能同时消费多个 shard。
  5. LiveRoomLock / OnlineCounter / LiveSessionRealtime / AuctionRealtimeStore / EventLog 与 ScriptRegistry 全部走 shard 路由，且同一聚合根的 key 同 shard。
  6. metrics 注入 instance label 区分 `rt-0` / `rt-1` / `cache`；readiness probe 给每个 shard 一个组件 key。
  7. 全局/无明确分片键的 RT key（如 `risk:blacklist:user`、`auction:active_streams`）有明确归属策略（固定到 shard 0 或在所有 shard 上 fan-out），不能丢功能。
  8. 现有功能行为不变；`go build ./...`、`go vet ./...`、`go test ./...` 全绿。

## Assurance
- Mode: strict_verify
- Why: 涉及 Redis 分片路由 + Lua/Stream/锁的同 shard 约束，错一个会导致拍卖核心链路损坏；构建 + 测试必须通过；评审 gate 必须先把"分片键 + 同 shard 约束"敲定再进实施。

## Gates
### g1 plan-review
- Status: pending
- Acceptance: 设计能 1) 给出每个 RT key 的分片键（auctionID/sessionID/roomID/全局）2) 保证同一聚合根的所有 key 同 shard 3) Bid Stream + Worker + Reconciler 多 shard 调度方案清晰 4) ScriptRegistry 多 shard 加载与 NOSCRIPT 重试 5) Validate/Default/applyEnv 完整 6) 文件改动清单。
- Repair: 0/2

### g2 build-and-test
- Status: pending
- Acceptance: build/vet/test 全绿；现有 miniredis 测试通过；新增 sharding 单测覆盖路由一致性与 fan-out。
- Repair: 0/2

## Understanding
- 上一轮已落地：`RedisRTClient`/`RedisCacheClient` 具名结构、`RedisConfig{RT.Primary, RT.Secondary*, Cache}`、`LayeredCache`（L1+L2+三防）、ItemService 接入、双 metrics hook、双 readiness。
- 本轮需替换：`RT.Primary/Secondary` schema → `RT.Shards[]` 多分片；`*RedisRTClient` 单实例 → `ShardedRTClient`（按 key 选 shard）。
- RT 组件清单（来自上一轮 n1 探索）：
  - `AuctionRealtimeStore`（Lua bid/hammer + HSET/SADD/ZADD，按 auctionID）
  - `EventLog`（XADD/XREADGROUP/XACK/...，按 auctionID 的 stream key + active_streams 全局 SET）
  - `LiveRoomLock`（按 roomID）
  - `OnlineCounter`（按 auctionID + ws instance 全局 keys）
  - `LiveSessionRealtimeStore`（按 sessionID）
  - `ScriptRegistry`（每个 shard 都要 LoadAll）
  - `BidRecordWorker` / `Reconciler`（消费每个 shard 的 stream，间接）
- KeyBuilder 已有；分片键映射策略需在调用侧或 KeyBuilder 上定义辅助方法。

## Decisions (locked)
- shards 固定 2 个：rt-0=127.0.0.1:6381(noeviction/1gb), rt-1=127.0.0.1:6382(noeviction/1gb)；cache=127.0.0.1:6380(allkeys-lfu/512mb)
- 路由：fnv32(hashKey) % N，按 auctionID/sessionID/roomID
- 黑名单：从 Lua 移除；RT 不再持有黑名单（删除 RealtimeStore.SetBlacklisted/IsBlacklisted、RiskService 双写、BidService.realtime.IsBlacklisted）；source of truth = MySQL + Redis-Cache (LayeredCache 三防)
- active_streams：每 shard 一份；ActiveAuctions per-shard 单读
- OnlineCounter：**不迁 Cache**，留在 RT shard。按 auctionID 路由 ZSET；跨 shard 全局 key (WSInstances/OnlineInstanceConns/WSInstanceHeartbeat) 固定到 shard 0
- KEYS=10（保留 active_streams 进 Lua，删除 KEYS[6]=blacklist，KEYS[7..11] 各 -1）
- Hub.SetReplaySource shard-aware（per-shard EventLog）
- Worker 模型：**per-shard EventLog + per-shard worker goroutine**；ActiveAuctions per-shard；DLQ 固定 shard 0
- DLQ 兜底：写 Redis DLQ stream 失败时回退 INSERT 到 MySQL bid_record_dlq 表
- BlacklistChecker：Cache 命中即返回；Cache miss 落 MySQL；MySQL 故障默认放行（避免阻断出价）+ metrics 告警；空值 sentinel 短 TTL

## Completed

## Pending
### n1 @omc-explore — RT key 与多 key 操作映射
- Goal: 列出每个 RT key 的分片键（auctionID/sessionID/roomID/global），标注哪些 Lua 与 multi-key 命令必须落同 shard，哪些是全局 key。
- Acceptance: 输出每个 RT key 的 shard 决策建议表 + 已知风险。

### n2 @omc-architect — Sharding 设计 — depends:n1
- Goal: 给出 ShardedRTClient 接口、配置 schema、组件构造改造、Bid Stream 多 shard 调度、ScriptRegistry 多 shard 加载、全局 key 归属策略、metrics/readiness 标签方案、改动清单。
- Acceptance: 满足 g1。

### n3 @omc-critic — 设计评审 — depends:n2 — gate:g1
- Goal: 评审 n2 设计的同 shard 约束是否完整、Stream/Worker 调度是否会丢消息、回归风险。
- Acceptance: 通过 g1 才进入实施。

### n4 @omc-deep-executor — 实施 — depends:n3
- Goal: 按设计实施分片改造（config + ShardedRTClient + 组件 + Worker + Scripts + observability）。
- Acceptance: 改动落地、自检 build/vet/test 通过、保留行为。

### n5 @omc-verifier — 独立验证 — depends:n4 — gate:g2
- Goal: 验证 build/vet/test、metrics shard 标签、readiness 多 probe、RT 路由一致性。
- Acceptance: 满足 g2。

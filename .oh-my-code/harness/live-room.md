# Harness Graph: live-room

> Status: in_progress

## User Intent
- Original: 加上直播间（拍卖房间），一个房间商家可上架多款竞拍品；同一时刻只有一个拍品在拍卖（Redis 分布式锁）；改造数据库表，注意现有设计；同时修改默认模块 OpenAPI 文档。
- Acceptance: 完成直播间领域模型、迁移、仓储/服务/HTTP/WS 入口、Redis 分布式活拍锁、OpenAPI 与测试。

## Assurance
- Mode: strict_verify
- Why: 直播间是竞拍入口与 WS 房间业务容器，错误会破坏现有出价/落锤链路。

## Design Summary

### 数据库迁移 — `migrations/00002_live_room.sql`
新增表 `live_room`（最小字段，复用 user 作为商家）：
- `id` BIGINT UNSIGNED PK AUTO_INCREMENT
- `merchant_id` BIGINT UNSIGNED NOT NULL（关联 user.id）
- `title` VARCHAR(128) NOT NULL
- `description` VARCHAR(1024) DEFAULT NULL
- `cover_url` VARCHAR(512) DEFAULT NULL
- `status` VARCHAR(16) NOT NULL DEFAULT 'OFFLINE' — `OFFLINE/LIVE/CLOSED`
- `active_auction_id` BIGINT DEFAULT NULL — 当前唯一在拍 lot；锁释放时清空
- `created_at` / `updated_at`
- KEY `idx_merchant_status (merchant_id, status)`
- KEY `idx_active_auction (active_auction_id)`

兼容性扩展（`auction_lot` 加列）：
- `live_room_id` BIGINT UNSIGNED NOT NULL DEFAULT 0（0 表示遗留无房间）
- `KEY idx_live_room_status (live_room_id, status)`

### 领域模型 — `internal/domain/live_room.go`（新文件）
- `LiveRoomStatus`（`OFFLINE/LIVE/CLOSED`）+ `Valid` / `CanTransitionLiveRoom`
- `LiveRoom`、`LiveRoomFilter`、`LiveRoomPatch`
- `AuctionLot` 增加 `LiveRoomID uint64` 字段（json `liveRoomId`），repo `auctionRow`/CRUD/Memory 同步透传

### 仓储 — `internal/repository/live_room.go` 等（新文件）
- `LiveRoomRepository` 接口（Create/FindByID/List/Update/Delete + ListLots(roomID)）
- `MySQLLiveRoomRepository`（gorm，table `live_room`）
- `MemoryLiveRoomRepository`

### 服务 — `internal/service/live_room.go`（新文件）
- `LiveRoomService` 提供 CRUD（仅商家本人/管理员），开播/下播。
- `ActivateAuction(roomID, auctionID)`：
  1. 校验 lot 属于该 room 且状态可启动；
  2. **Redis SETNX** `live_room:{id}:active`（值=auctionID, TTL=lot.endTime+冗余）；
  3. 写 `live_room.active_auction_id`；
  4. 调用 `AuctionService.Start`；
  5. 失败回滚 SETNX。
- `DeactivateAuction`：拍品落锤/取消时调用（hammer hook 或显式 API）— DEL Redis key、清空 `active_auction_id`。
- 通过 `repository.AuctionRepository` 关联 lot；用 `redis.LockManager` 抽象。

### Redis 分布式锁 — `internal/infra/redis/live_room_lock.go`（新文件）+ `KeyBuilder.LiveRoomActive`
- `LiveRoomLock` 接口：`Acquire(ctx, roomID, auctionID, ttl)` / `Release(ctx, roomID, auctionID)` / `Current(ctx, roomID)`。
- 实现：基于 `SET key value NX PX ttl` 与 Lua 释放（仅当 value 匹配时 DEL）。
- 内存实现 `MemoryLiveRoomLock` 用于测试。
- 注入 `LiveRoomService`。

### HTTP — `internal/transport/http/live_room_handler.go`（新文件）
路由（在 `server.go` v1 内新增）：
- `POST   /api/v1/live-rooms`              merchant/admin — 创建
- `GET    /api/v1/live-rooms`              public/auth — 列表
- `GET    /api/v1/live-rooms/:id`          public/auth — 详情
- `PATCH  /api/v1/live-rooms/:id`          merchant/admin — 更新
- `DELETE /api/v1/live-rooms/:id`          merchant/admin — 删除
- `GET    /api/v1/live-rooms/:id/lots`     public/auth — 房间内拍品列表
- `POST   /api/v1/live-rooms/:id/activate` merchant/admin（含幂等） — 激活某拍品（body: { auctionId })
- `POST   /api/v1/live-rooms/:id/deactivate` merchant/admin — 强制释放（管理用）

### Hammer 钩子
`HammerService.Hammer` 在拍品终态时通知 `LiveRoomService.OnAuctionClosed(auctionID)`，自动释放锁；通过新增可选回调 `OnAuctionClosed func(ctx, auctionID, liveRoomID)` 字段，由 server.go 装配，避免循环依赖。

### WebSocket
- 保持现有 `/ws/auctions/:auction_id` 不变。
- 新增 `/ws/live-rooms/:room_id`：根据房间当前 `active_auction_id` 路由订阅 `Hub.Subscribe(activeAuctionID, client)`；房间无 active 时返回 4404。
- 客户端可继续通过 `room.subscribe { auctionId }` 切换。

### OpenAPI（`docs/默认模块.openapi.json`）
- 新增 tag `LiveRoom`
- 新增 `LiveRoom` schema 与所有路径条目（Create/List/Get/Patch/Delete/Lots/Activate/Deactivate）
- WebSocket tag 增加 `connectLiveRoomWebSocket`

### 测试
- `internal/repository/live_room_memory_test.go`（已包含 list/find/update）
- `internal/service/live_room_test.go`：CRUD 鉴权、Activate 锁互斥、自动释放
- 复用 `MemoryLiveRoomLock` 模拟分布式锁

## Gates
### g1 live-room-design
- Status: completed
- Acceptance: 上述设计稿明确表/接口/流程/兼容策略，无需进一步澄清。

### g2 live-room-implementation
- Status: pending
- Acceptance: 代码与迁移到位，go build / 受影响 go test 通过，OpenAPI 更新。

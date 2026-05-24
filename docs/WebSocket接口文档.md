# WebSocket 接口文档

## 元数据

| 字段 | 值 |
|---|---|
| 生成日期 | 2026-05-24 |
| 基于 HEAD short hash | `a9b5f7b` |
| 适用服务 | `aieas_backend` 实时竞拍后端 |
| 代码依据 | 当前仓库源码，重点参考 `internal/app/server.go`、`internal/transport/http/ws_handler.go`、`internal/transport/ws/*.go`、`internal/service/{bid,auction,hammer}.go`、`scripts/lua/{bid,hammer}.lua` |

## 1. 概览

### 1.1 协议与传输

| 项 | 说明 |
|---|---|
| 协议 | WebSocket，服务端使用 `github.com/hertz-contrib/websocket` 的 `websocket.HertzUpgrader` |
| 注册位置 | `internal/app/server.go` |
| 端点 | `GET /ws/auctions/:auction_id`、`GET /ws/live-rooms/:room_id` |
| 鉴权 | JWT Bearer 鉴权，路由挂载 `authHandler.AuthMiddleware()` |
| 消息帧 | 业务消息使用文本帧，内容为 JSON envelope |
| 二进制帧 | 不作为业务协议使用；服务端读到非文本帧会直接忽略 |
| 服务端 Ping | 使用 WebSocket 协议级 Ping control frame，不是业务 JSON 消息 |

### 1.2 端点

| 端点 | 作用 | 订阅房间 |
|---|---|---|
| `GET /ws/auctions/:auction_id` | 直接连接某个拍品的实时事件流 | `auction_id` 对应的 Hub room |
| `GET /ws/live-rooms/:room_id` | 连接直播间当前活跃拍品的实时事件流 | 先通过 `LiveRoomService.ActiveAuctionID(room_id)` 找到当前 `auctionId`，再订阅该拍品 Hub room |

注意：`/ws/auctions/:auction_id` 在建立连接时只校验路径参数与 JWT，不会先校验拍卖是否存在；拍卖不存在通常会在后续 `bid.place` 时通过 `bid.ack` 返回错误。`/ws/live-rooms/:room_id` 会在升级前校验直播间和当前活跃拍品。

### 1.3 获取 access token

客户端先调用登录接口获取 `accessToken`：

```http
POST /api/v1/auth/login
Content-Type: application/json

{
  "account": "buyer@example.com",
  "password": "password",
  "role": "BUYER"
}
```

成功响应结构来自 `service.LoginResult`：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "accessToken": "<jwt>",
    "refreshToken": "rft_xxx",
    "expiresIn": 43200,
    "user": {
      "id": "u_1001",
      "account": "buyer@example.com",
      "role": "BUYER"
    }
  },
  "trace_id": "trc_..."
}
```

WebSocket 连接时优先推荐使用请求头：

```http
Authorization: Bearer <accessToken>
```

源码也支持 `token` query 参数作为备选：`?token=<accessToken>`。生产环境建议优先使用 Header，避免 token 出现在 URL 日志中。

## 2. 连接与升级

### 2.1 请求头

浏览器原生 `WebSocket` API 不能自定义 `Authorization` Header；如果必须在浏览器中连接，当前服务端源码支持使用 `token` query 参数。非浏览器客户端建议使用 Header。

```http
GET /ws/auctions/10001?lastSeq=128 HTTP/1.1
Host: api.example.com
Connection: Upgrade
Upgrade: websocket
Sec-WebSocket-Key: <browser-generated>
Sec-WebSocket-Version: 13
Authorization: Bearer <accessToken>
```

浏览器形式：

```text
wss://api.example.com/ws/auctions/10001?token=<accessToken>&lastSeq=128
wss://api.example.com/ws/live-rooms/20001?token=<accessToken>&lastSeq=128
```

### 2.2 `lastSeq` query

| 参数 | 类型 | 说明 |
|---|---|---|
| `lastSeq` | `int64` | 客户端最近已处理的服务端 envelope 顶层 `seq`；服务端会尝试补发 `seq > lastSeq` 的历史窗口事件 |

源码实际参数名为 `lastSeq`，不是 `last_seq`。如果缺失、非数字或小于 0，服务端按 0 处理，不进行补偿。

### 2.3 升级前常见 HTTP 错误

升级前错误使用统一 REST 响应结构：

```json
{
  "code": 10001,
  "message": "缺少访问令牌",
  "data": null,
  "trace_id": "trc_..."
}
```

| 场景 | HTTP 状态 | 业务 code | message | 代码来源 |
|---|---:|---:|---|---|
| 缺少 `Authorization` 且缺少 `token` query | 401 | 10001 | 缺少访问令牌 | `AuthMiddleware` |
| `Authorization` 不是 `Bearer ` 前缀 | 401 | 10002 | 访问令牌无效或已过期 | `AuthMiddleware` |
| JWT 解析失败或过期 | 401 | 10002 | 访问令牌无效或已过期 | `AuthMiddleware` |
| 路径参数 `auction_id` / `room_id` 非正整数 | 400 | 20001 | 参数不合法 | `parseUintParam` |
| Hub 订阅失败 | 500 | 90001 | 系统内部错误 | `WSHandler.Auction` / `WSHandler.LiveRoom` |
| 直播间服务未注入 | 500 | 90001 | 系统内部错误 | `WSHandler.LiveRoom` |
| 直播间不存在 | 404 | 31001 | 直播间不存在 | `writeLiveRoomError` |
| 直播间状态错误 | 409 | 31003 | 直播间状态不允许此操作 | `writeLiveRoomError` |
| 直播间当前无在拍品 | 409 | 31005 | 直播间当前无在拍品 | `WSHandler.LiveRoom` |
| 其他服务错误 | 500 | 90001 | 系统内部错误 | `HTTPStatusAndCode` 默认分支 |

### 2.4 鉴权失败、拍卖不存在、房间无活动拍品

| 场景 | 实际处理 |
|---|---|
| 鉴权失败 | 在 WebSocket Upgrade 前直接返回 HTTP 401，不进入 WebSocket 协议 |
| `/ws/auctions/:auction_id` 对应拍卖不存在 | 连接阶段不校验；后续 `bid.place` 会调用 `BidService.Place`，拍卖仓储查询失败时以 `bid.ack` 返回 `accepted=false`、`code=20004`、`reason="资源不存在"` |
| `/ws/live-rooms/:room_id` 房间不存在 | Upgrade 前返回 HTTP 404 / code 31001 |
| `/ws/live-rooms/:room_id` 房间存在但 `ActiveAuctionID == 0` | Upgrade 前返回 HTTP 409 / code 31005 / `直播间当前无在拍品` |

## 3. 消息 Envelope

### 3.1 服务端到客户端 Envelope

结构来自 `internal/transport/ws/envelope.go`：

```json
{
  "type": "bid.accepted",
  "requestId": "bid-20260524-0001",
  "seq": 129,
  "ack": false,
  "payload": {}
}
```

| 字段 | 类型 | 是否可能省略 | 含义 |
|---|---|---|---|
| `type` | `string` | 否 | 事件类型或响应类型 |
| `requestId` | `string` | 是 | 对应客户端请求的幂等/关联 ID；广播事件通常为空并省略 |
| `seq` | `int64` | 是 | Hub room 内递增序号；用于断线补偿。没有进入 Hub 广播历史的直连响应可能没有 `seq` |
| `ack` | `boolean` | 是 | 仅 `ack` envelope 会为 `true`；省略时按 `false` 处理 |
| `payload` | `object` | 是 | 事件业务载荷；不同 `type` 不同 |

说明：Go `time.Time` 通过 JSON 序列化为 RFC3339/RFC3339Nano 字符串，例如 `2026-05-24T12:00:00Z`。Redis Stream 回放的出价事件 payload 使用毫秒时间戳字段，例如 `endTsMs`、`createdAtMs`、`bidTsMs`。

### 3.2 客户端到服务端 Envelope

客户端发送的文本帧也反序列化为同一个 envelope 结构：

```json
{
  "type": "bid.place",
  "requestId": "bid-20260524-0001",
  "seq": 0,
  "payload": {
    "auctionId": 10001,
    "price": 129900,
    "idempotencyKey": "bid-20260524-0001"
  }
}
```

| 字段 | 类型 | 必填 | 含义 |
|---|---|---|---|
| `type` | `string` | 是 | 客户端消息类型，例如 `bid.place`、`ping`、`heartbeat` |
| `requestId` | `string` | 建议必填 | 请求关联 ID；`bid.place` 会作为出价幂等键的第一优先级 |
| `seq` | `int64` | 否 | 客户端自带序号；服务端 `ack`/`pong` 可能原样带回 |
| `ack` | `boolean` | 否 | 客户端无需设置 |
| `payload` | `object` | 按类型 | 业务载荷 |

### 3.3 金额单位

所有金额字段均为 `int64` 整数，单位为「分」，不要使用浮点数。

| 字段 | 说明 |
|---|---|
| `price` | 出价金额或成交价，单位分 |
| `currentPrice` | 当前最高价，单位分 |
| `startPrice`、`reservePrice`、`depositAmount` | REST 状态/拍品字段，单位分 |

## 4. 心跳与连接保活

### 4.1 服务端协议级 Ping/Pong

| 项 | 源码默认值 | 说明 |
|---|---:|---|
| `pingInterval` | 20 秒 | 写协程每个周期发送 WebSocket Ping control frame |
| `pongTimeout` | 60 秒 | 建连后设置 read deadline；收到 Pong 后延长到 `now + pongTimeout` |
| `readLimitBytes` | 65536 字节 | 单条读消息大小限制 |
| `websocketWriteTimeout` | 5 秒 | 每次写 JSON 或 Ping 前设置 write deadline |
| `sendBufferSize` | 256 | 生产默认配置；构造器兜底值为 64 |

客户端必须让 WebSocket 实现正常回复协议级 Pong。浏览器会自动处理协议级 Ping/Pong。

### 4.2 业务级心跳

除协议级 Ping/Pong 外，源码还支持业务消息：

```json
{
  "type": "heartbeat",
  "requestId": "hb-1"
}
```

响应：

```json
{
  "type": "heartbeat.ack",
  "requestId": "hb-1",
  "payload": {
    "ts": 1789999999123
  }
}
```

也支持：

```json
{
  "type": "ping",
  "requestId": "ping-1",
  "seq": 7
}
```

典型响应可能包含一个通用 `ack` 和一个 `pong`：

```json
{
  "type": "pong",
  "requestId": "ping-1",
  "seq": 7
}
```

### 4.3 慢消费者关闭行为

Hub 向客户端投递事件时使用有界 channel。如果客户端消费太慢导致发送缓冲区满：

1. `Client.Deliver` 将客户端标记为关闭，内部原因记录为 `slow_consumer`。
2. 后续广播会从 room 中移除该客户端。
3. 服务端写协程发现 outbound channel 关闭后退出，连接最终关闭。

源码没有向前端发送自定义 WebSocket Close frame 或业务错误事件来说明 `slow_consumer`，客户端应通过 `onclose` 触发重连，并携带最近处理的 `lastSeq`。

## 5. 服务端到客户端事件清单

以下为当前源码中可下发到前端的业务 envelope `type`。其中 `bid.accepted` / `bid.rejected` 在 Redis Stream 模式下也可能由 EventRelay 回放/转发，payload 字段会与普通 Go 广播略有差异，文档分别说明。

### 5.1 `room.online`

| 项 | 说明 |
|---|---|
| 触发时机 | 客户端订阅、取消订阅、慢消费者被移除时，Hub 广播当前在线数 |
| 是否带 `seq` | 是 |
| payload 字段 | `auctionId:uint64`、`online:int` |

```json
{
  "type": "room.online",
  "seq": 1,
  "payload": {
    "auctionId": 10001,
    "online": 23
  }
}
```

### 5.2 `auction.started`

| 项 | 说明 |
|---|---|
| 触发时机 | 商家/管理员调用 `POST /api/v1/auctions/:id/start` 成功后 |
| payload 字段 | `auctionId:uint64`、`state:AuctionState` |
| 金额字段 | `state.currentPrice` 单位分 |

```json
{
  "type": "auction.started",
  "seq": 10,
  "payload": {
    "auctionId": 10001,
    "state": {
      "auctionId": 10001,
      "status": "RUNNING",
      "currentPrice": 100000,
      "startTime": "2026-05-24T12:00:00Z",
      "endTime": "2026-05-24T12:10:00Z",
      "lastBidTsMs": 0,
      "extendCount": 0,
      "version": 1789999200000,
      "source": "redis"
    }
  }
}
```

### 5.3 `bid.accepted`

| 项 | 说明 |
|---|---|
| 触发时机 | 出价被接受；非 Redis Stream 模式由 `BidService.publishBidResult` 广播，Redis Stream 模式由 `EventRelay` 从 stream 转发 |
| payload 字段 | 见下方示例 |
| 金额字段 | `price`、`currentPrice` 单位分 |

Go 广播模式示例：

```json
{
  "type": "bid.accepted",
  "seq": 21,
  "payload": {
    "requestId": "bid-1",
    "auctionId": 10001,
    "bidderId": "u_1001",
    "price": 129900,
    "accepted": true,
    "currentPrice": 129900,
    "leaderBidderId": "u_1001",
    "endTime": "2026-05-24T12:10:00Z",
    "extended": false,
    "version": 1789999300001,
    "event": "bid.accepted",
    "riskResult": "ALLOW"
  }
}
```

Redis Stream 转发/补偿模式示例：

```json
{
  "type": "bid.accepted",
  "seq": 22,
  "payload": {
    "requestId": "bid-1",
    "auctionId": 10001,
    "bidderId": "u_1001",
    "price": 129900,
    "accepted": true,
    "reason": "",
    "currentPrice": 129900,
    "leaderBidderId": "u_1001",
    "endTsMs": 1789999800000,
    "extended": false,
    "extendCount": 0,
    "seq": 22,
    "streamId": "22-0",
    "createdAtMs": 1789999300000,
    "bidTsMs": 1789999300000,
    "source": "live_ws",
    "event": "bid.accepted",
    "riskResult": "ALLOW"
  }
}
```

### 5.4 `bid.rejected`

| 项 | 说明 |
|---|---|
| 触发时机 | 出价未被接受；可能由 Lua、内存实时仓储或黑名单前置逻辑生成 |
| payload 字段 | 与 `BidResult` 或 `BidEvent.PayloadJSON()` 对齐 |
| 金额字段 | `price`、`currentPrice` 单位分 |

```json
{
  "type": "bid.rejected",
  "seq": 23,
  "payload": {
    "requestId": "bid-2",
    "auctionId": 10001,
    "bidderId": "u_1002",
    "price": 130000,
    "accepted": false,
    "reason": "BELOW_MIN_INCREMENT",
    "currentPrice": 129900,
    "leaderBidderId": "u_1001",
    "endTime": "2026-05-24T12:10:00Z",
    "event": "bid.rejected",
    "riskResult": "REJECT"
  }
}
```

### 5.5 `bid.ack`

| 项 | 说明 |
|---|---|
| 触发时机 | 服务端处理当前连接发送的 `bid.place` 后，向当前客户端返回结果 |
| 是否广播 | 否，只投递给当前连接 |
| requestId | 使用 envelope `requestId`、payload `requestId`、payload `idempotencyKey` 三者中第一个非空值 |

成功示例：

```json
{
  "type": "bid.ack",
  "requestId": "bid-1",
  "payload": {
    "requestId": "bid-1",
    "auctionId": 10001,
    "bidderId": "u_1001",
    "price": 129900,
    "accepted": true,
    "currentPrice": 129900,
    "leaderBidderId": "u_1001",
    "endTime": "2026-05-24T12:10:00Z",
    "version": 1789999300001,
    "event": "bid.accepted",
    "riskResult": "ALLOW"
  }
}
```

失败示例：

```json
{
  "type": "bid.ack",
  "requestId": "bid-2",
  "payload": {
    "accepted": false,
    "code": 20004,
    "reason": "资源不存在"
  }
}
```

### 5.6 `ranking.updated`

| 项 | 说明 |
|---|---|
| 触发时机 | 出价接受后，服务端读取 Top 10 排名成功时广播 |
| payload 字段 | `auctionId:uint64`、`ranking:RankingEntry[]` |
| 金额字段 | `ranking[].price` 单位分 |

```json
{
  "type": "ranking.updated",
  "seq": 24,
  "payload": {
    "auctionId": 10001,
    "ranking": [
      { "rank": 1, "bidderId": "u_1001", "price": 129900 },
      { "rank": 2, "bidderId": "u_1002", "price": 120000 }
    ]
  }
}
```

### 5.7 `timer.extended`

| 项 | 说明 |
|---|---|
| 触发时机 | 出价被接受且触发防狙击延时 `result.Extended == true` |
| payload 字段 | `auctionId:uint64`、`endTime:time`、`extendCount:int` |

```json
{
  "type": "timer.extended",
  "seq": 25,
  "payload": {
    "auctionId": 10001,
    "endTime": "2026-05-24T12:10:30Z",
    "extendCount": 1
  }
}
```

### 5.8 `timer.tick`

| 项 | 说明 |
|---|---|
| 触发时机 | `TimerScheduler` 每秒读取实时拍卖状态；状态为 `RUNNING` 或 `EXTENDED` 时广播 |
| payload 字段 | `auctionId:uint64`、`endTime:time`、`remainingMs:int64`、`status:string` |

```json
{
  "type": "timer.tick",
  "seq": 26,
  "payload": {
    "auctionId": 10001,
    "endTime": "2026-05-24T12:10:30Z",
    "remainingMs": 28500,
    "status": "EXTENDED"
  }
}
```

### 5.9 `auction.closed`

| 项 | 说明 |
|---|---|
| 触发时机 | 手动落槌或倒计时到期自动落槌成功后 |
| payload 字段 | `auctionId:uint64`、`status:string`、`winnerId:string`、`price:int64`、`closedAt:time`、可选 `orderId:uint64` |
| 金额字段 | `price` 单位分 |

```json
{
  "type": "auction.closed",
  "seq": 30,
  "payload": {
    "auctionId": 10001,
    "status": "CLOSED_WON",
    "winnerId": "u_1001",
    "price": 129900,
    "closedAt": "2026-05-24T12:10:30Z",
    "orderId": 90001
  }
}
```

### 5.10 `room.snapshot_required`

| 项 | 说明 |
|---|---|
| 触发时机 | 重连携带 `lastSeq > 0`，但服务端历史窗口或 Redis Stream 无法完整补偿 |
| 是否带 `seq` | 通常不带；这是当前连接的直投消息 |
| 客户端动作 | 调用 `GET /api/v1/auctions/:id/state` 拉取权威快照 |

```json
{
  "type": "room.snapshot_required",
  "payload": {
    "auctionId": 10001,
    "lastSeq": 128,
    "reason": "EVENT_WINDOW_EXPIRED"
  }
}
```

### 5.11 `pong`

| 项 | 说明 |
|---|---|
| 触发时机 | 客户端发送业务消息 `type=ping` |
| payload | 源码中无 payload |

```json
{
  "type": "pong",
  "requestId": "ping-1",
  "seq": 7
}
```

### 5.12 `heartbeat.ack`

| 项 | 说明 |
|---|---|
| 触发时机 | 客户端发送业务消息 `type=heartbeat` |
| payload 字段 | `ts:int64`，服务端当前 UTC 毫秒时间戳 |

```json
{
  "type": "heartbeat.ack",
  "requestId": "hb-1",
  "payload": {
    "ts": 1789999999123
  }
}
```

### 5.13 `ack`

| 项 | 说明 |
|---|---|
| 触发时机 | 进入 `Hub.HandleInbound` 且客户端 envelope 带 `requestId` 时先返回通用 ACK；`bid.place`、`room.subscribe`、`room.unsubscribe`、`heartbeat` 被 `WSHandler` 直接处理，不走这个通用 ACK |
| payload 字段 | `requestId:string`、`seq:int64` |

```json
{
  "type": "ack",
  "requestId": "ping-1",
  "seq": 7,
  "ack": true,
  "payload": {
    "requestId": "ping-1",
    "seq": 7
  }
}
```

### 5.14 `room.subscribed`、`subscribed`

| 类型 | 触发时机 | payload |
|---|---|---|
| `room.subscribed` | 客户端发送 `room.subscribe` | `{"auctionId": 10001}` |
| `subscribed` | 客户端发送 `subscribe`，由 Hub 默认处理 | 源码无 payload |

```json
{
  "type": "room.subscribed",
  "requestId": "sub-1",
  "payload": {
    "auctionId": 10001
  }
}
```

### 5.15 `room.unsubscribed`

| 项 | 说明 |
|---|---|
| 触发时机 | 客户端发送 `room.unsubscribe` |
| payload 字段 | `auctionId:uint64` |

```json
{
  "type": "room.unsubscribed",
  "requestId": "unsub-1",
  "payload": {
    "auctionId": 10001
  }
}
```

### 5.16 `announcement`

| 项 | 说明 |
|---|---|
| 触发时机 | 客户端发送 `announcement` 后，Hub 将该 envelope 广播给同一拍品房间 |
| payload | 源码不限制字段，原样广播客户端 envelope 的 payload |

```json
{
  "type": "announcement",
  "seq": 31,
  "payload": {
    "text": "本轮拍卖即将结束"
  }
}
```

说明：源码未对 `announcement` 做角色校验或 payload schema 校验。客户端对接时如无业务授权约定，不建议普通买家端发送该消息。

### 5.17 `risk.event`

| 项 | 说明 |
|---|---|
| 触发时机 | `RiskService.RecordEvent` 成功写入风险事件后广播；当前出价频控 `FREQ_LIMIT` 会记录 `BID_FREQ` |
| payload 字段 | `RiskEvent` 结构 |

```json
{
  "type": "risk.event",
  "seq": 32,
  "payload": {
    "id": 1,
    "eventType": "BID_FREQ",
    "userId": "u_1001",
    "auctionId": 10001,
    "severity": "MID",
    "payload": {
      "requestId": "bid-3",
      "auctionId": 10001,
      "accepted": false,
      "reason": "FREQ_LIMIT"
    },
    "status": "PENDING",
    "createdAt": "2026-05-24T12:00:00Z"
  }
}
```

### 5.18 `error`

| 项 | 说明 |
|---|---|
| 触发时机 | JSON 解析失败、Hub 收到空 `type`、或内部处理发现 client 缺失 |
| payload 字段 | `message:string` |

```json
{
  "type": "error",
  "requestId": "req-1",
  "payload": {
    "message": "message type required"
  }
}
```

### 5.19 `auction.state` 支持情况

当前源码没有广播 `auction.state` 事件；拍卖状态快照通过 REST `GET /api/v1/auctions/:id/state` 获取。客户端不要依赖 `auction.state` WebSocket 事件。

## 6. 客户端到服务端事件清单

### 6.1 `bid.place`

| 项 | 说明 |
|---|---|
| 发送时机 | 买家提交出价 |
| 必填字段 | envelope `type=bid.place`；`price:int64`；幂等 ID 建议放 envelope `requestId` |
| 可选字段 | `payload.auctionId`，不传时使用当前连接订阅的 `client.AuctionID` |
| 金额字段 | `price` 单位分 |
| 响应 | `bid.ack`；成功后还可能广播 `bid.accepted`、`ranking.updated`、`timer.extended` |

```json
{
  "type": "bid.place",
  "requestId": "bid-20260524-0001",
  "payload": {
    "auctionId": 10001,
    "price": 129900,
    "idempotencyKey": "bid-20260524-0001"
  }
}
```

出价幂等字段承载顺序以源码为准：

1. envelope 顶层 `requestId`
2. `payload.requestId`
3. `payload.idempotencyKey`

三者都为空时，`BidService.Place` 会因 `RequestID == ""` 返回参数错误，`bid.ack` 中表现为：

```json
{
  "type": "bid.ack",
  "payload": {
    "accepted": false,
    "code": 20001,
    "reason": "参数不合法"
  }
}
```

### 6.2 `ping`

| 项 | 说明 |
|---|---|
| 发送时机 | 客户端业务级保活或测延迟 |
| 必填字段 | `type` |
| 响应 | 如果带 `requestId`，先返回通用 `ack`，再返回 `pong` |

```json
{
  "type": "ping",
  "requestId": "ping-1",
  "seq": 7
}
```

### 6.3 `heartbeat`

| 项 | 说明 |
|---|---|
| 发送时机 | 客户端业务级保活 |
| 必填字段 | `type` |
| 响应 | `heartbeat.ack`，payload 带服务端毫秒时间戳 `ts` |

```json
{
  "type": "heartbeat",
  "requestId": "hb-1"
}
```

### 6.4 `room.subscribe`

| 项 | 说明 |
|---|---|
| 发送时机 | 连接后切换或确认订阅某个拍品 room |
| payload | `auctionId:uint64`；不传或为 0 时使用当前 `client.AuctionID` |
| 响应 | `room.subscribed`，payload 带最终订阅的 `auctionId` |

```json
{
  "type": "room.subscribe",
  "requestId": "sub-1",
  "payload": {
    "auctionId": 10002
  }
}
```

说明：当前源码支持 `room.subscribe`。它会调用 `h.hub.Subscribe(payload.AuctionID, client)`；如果切到新 `auctionId`，Hub 会先从旧 room 取消订阅再加入新 room。

### 6.5 `room.unsubscribe`

| 项 | 说明 |
|---|---|
| 发送时机 | 客户端主动取消某个拍品 room 订阅 |
| payload | `auctionId:uint64`；不传或为 0 时使用当前 `client.AuctionID` |
| 响应 | `room.unsubscribed` |

```json
{
  "type": "room.unsubscribe",
  "requestId": "unsub-1",
  "payload": {
    "auctionId": 10002
  }
}
```

### 6.6 `subscribe`

| 项 | 说明 |
|---|---|
| 支持情况 | 当前源码支持 `subscribe`，由 Hub 默认处理 |
| 行为 | 重新订阅当前 `client.AuctionID` |
| 响应 | 如果带 `requestId`，返回通用 `ack`，随后返回 `subscribed` |

```json
{
  "type": "subscribe",
  "requestId": "sub-basic-1"
}
```

### 6.7 `announcement`

| 项 | 说明 |
|---|---|
| 支持情况 | 当前源码支持，收到后广播原 envelope 到当前拍品 room |
| payload | 源码不限制 |
| 响应 | 如果带 `requestId`，先返回通用 `ack`；广播给 room 的事件类型仍为 `announcement` |

```json
{
  "type": "announcement",
  "requestId": "ann-1",
  "payload": {
    "text": "拍卖即将结束"
  }
}
```

### 6.8 不支持或无明确语义的消息

| 消息 | 当前行为 |
|---|---|
| 空 `type` | 返回 `error`，payload `message=message type required` |
| 未知非空 `type` 且带 `requestId` | 仅返回通用 `ack`，无业务处理 |
| 未知非空 `type` 且不带 `requestId` | 无响应 |
| 二进制帧 | 服务端忽略 |

## 7. 断线重连与 seq 补偿

### 7.1 客户端保存最近 `seq`

客户端应在处理每条服务端 envelope 后，如果顶层 `seq > 0`，更新本地 `lastSeq`。建议按拍品维度保存：

```text
lastSeqByAuction[auctionId] = max(lastSeqByAuction[auctionId], envelope.seq)
```

### 7.2 重连携带 `lastSeq`

```text
wss://api.example.com/ws/auctions/10001?token=<accessToken>&lastSeq=128
```

服务端写协程启动后会先调用 `replayMissed(client, lastSeq)`：

1. `lastSeq <= 0`：不补偿。
2. 如果配置了 Redis `EventLog`，优先从 Redis Stream 补发，默认单次补偿窗口 256 条。
3. 如果 Redis replay 不可用，则使用 Hub 内存 `history`，默认窗口 256 条。
4. 补偿完整时，逐条投递历史 envelope。
5. 补偿不完整时，投递 `room.snapshot_required`。

### 7.3 历史窗口不完整

当客户端收到：

```json
{
  "type": "room.snapshot_required",
  "payload": {
    "auctionId": 10001,
    "lastSeq": 128,
    "reason": "EVENT_WINDOW_EXPIRED"
  }
}
```

应立即调用 REST 兜底接口获取状态快照，然后继续消费新事件。

### 7.4 REST 兜底：`GET /api/v1/auctions/:id/state`

请求：

```http
GET /api/v1/auctions/10001/state
Authorization: Bearer <accessToken>
```

响应 `data` 为 `AuctionState`：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "auctionId": 10001,
    "status": "RUNNING",
    "currentPrice": 129900,
    "leaderBidderId": "u_1001",
    "startTime": "2026-05-24T12:00:00Z",
    "endTime": "2026-05-24T12:10:30Z",
    "lastBidTsMs": 1789999300000,
    "extendCount": 1,
    "version": 1789999300001,
    "source": "redis"
  },
  "trace_id": "trc_..."
}
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `source` | `string` | `redis` 表示来自实时状态；`db` 表示 Redis 无状态时回退到数据库拍品记录 |
| `version` | `int64` | Redis 实时状态下为状态版本；DB 回退时为拍品 `UpdatedAt.UnixMilli()` |
| `currentPrice` | `int64` | 当前价，单位分 |

## 8. 错误码与状态机错误

### 8.1 `bid.ack` 中的服务错误

`bid.place` 调用服务层返回 Go error 时，`WSHandler` 会通过 `service.HTTPStatusAndCode` 映射为 `bid.ack.payload.code` 和 `reason`。

| Go 错误 | code | reason |
|---|---:|---|
| `domain.ErrTokenMissing` | 10001 | 缺少访问令牌 |
| `domain.ErrTokenInvalid` | 10002 | 访问令牌无效或已过期 |
| `domain.ErrForbidden` | 10003 | 无访问权限 |
| `domain.ErrAccountDisabled` | 10005 | 账号已停用 |
| `domain.ErrInvalidPassword` | 10004 | 登录失败 |
| `domain.ErrInvalidArgument` | 20001 | 参数不合法 |
| `domain.ErrUserNotFound` / `domain.ErrNotFound` | 20004 | 资源不存在 |
| `domain.ErrConflict` | 20009 | 资源冲突 |
| `domain.ErrInvalidState` | 20010 | 状态不允许 |
| `domain.ErrIdempotencyKey` | 20011 | 缺少幂等键 |
| 其他错误 | 90001 | 系统内部错误 |

### 8.2 出价拒绝 `reason`

出价进入实时仓储/Lua 后，拒绝通常表现为 `accepted=false` 且 `reason` 为下表之一。

| reason | 触发条件 | 来源 |
|---|---|---|
| `INVALID_STATE` | 实时状态不是 `RUNNING` 或 `EXTENDED` | `bid.lua` / `MemoryRealtimeStore` |
| `AUCTION_NOT_READY` | 内存实时仓储中拍卖状态不存在 | `MemoryRealtimeStore` |
| `BLACKLIST` | 出价用户在黑名单中 | `bid.lua` / `BidService` / `MemoryRealtimeStore` |
| `NOT_ENROLLED` | 用户未报名 | `bid.lua` / `MemoryRealtimeStore` |
| `DEPOSIT_NOT_READY` | 用户保证金未就绪 | `bid.lua` / `MemoryRealtimeStore` |
| `FREQ_LIMIT` | 超出频控，默认 1000ms 内超过 10 次 | `bid.lua` / `MemoryRealtimeStore` |
| `BELOW_MIN_INCREMENT` | 出价低于当前价 + 最小加价幅度；非领先者同价追平是源码允许的特殊情况 | `bid.lua` / `BidService` / `MemoryRealtimeStore` |
| `BID_SERVICE_UNAVAILABLE` | WebSocket Handler 未注入 BidService | `WSHandler.handleBidPlace` |
| `INVALID_PAYLOAD` | `bid.place.payload` JSON 解析失败 | `WSHandler.handleBidPlace` |

### 8.3 状态机相关

| 状态 | 说明 |
|---|---|
| 可出价状态 | `RUNNING`、`EXTENDED` |
| 不可出价状态 | `DRAFT`、`PENDING_AUDIT`、`READY`、`WARMING_UP`、`HAMMER_PENDING`、`CLOSED_WON`、`CLOSED_FAILED`、`SETTLED`，会导致 `INVALID_STATE` |
| 落槌成功状态 | `CLOSED_WON` 或 `CLOSED_FAILED` |
| 落槌状态非法 | 未到结束时间且非强制落槌时，服务层返回 `domain.ErrInvalidState`，REST 映射 code 20010 |

### 8.4 错误事件结构

WebSocket 业务错误事件结构：

```json
{
  "type": "error",
  "requestId": "req-1",
  "payload": {
    "message": "invalid json"
  }
}
```

`bid.place` 的业务拒绝不使用 `error`，而使用 `bid.ack` 或广播 `bid.rejected`。

## 9. 完整 JS Browser 伪代码

浏览器不能设置 WebSocket `Authorization` Header，因此示例使用 `token` query。实际项目请注意避免把 token 输出到日志。

```js
const API_BASE = "https://api.example.com";
const WS_BASE = "wss://api.example.com";

let accessToken = "";
let auctionId = 10001;
let lastSeq = Number(localStorage.getItem(`auction:${auctionId}:lastSeq`) || "0");
let ws = null;
let reconnectTimer = 0;

const handlers = {
  onAuctionStarted(payload) {},
  onBidAccepted(payload) {},
  onBidRejected(payload) {},
  onBidAck(payload, requestId) {},
  onRankingUpdated(payload) {},
  onTimerTick(payload) {},
  onTimerExtended(payload) {},
  onAuctionClosed(payload) {},
  onRoomOnline(payload) {},
  onError(payload) {}
};

async function login() {
  const res = await fetch(`${API_BASE}/api/v1/auth/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      account: "buyer@example.com",
      password: "password",
      role: "BUYER"
    })
  });
  const body = await res.json();
  if (!res.ok || body.code !== 0) throw new Error(body.message || "登录失败");
  accessToken = body.data.accessToken;
}

function connectAuction() {
  const url = new URL(`${WS_BASE}/ws/auctions/${auctionId}`);
  url.searchParams.set("token", accessToken);
  if (lastSeq > 0) url.searchParams.set("lastSeq", String(lastSeq));

  ws = new WebSocket(url.toString());

  ws.onopen = () => {
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = 0;
    }
  };

  ws.onmessage = async (event) => {
    const envelope = JSON.parse(event.data);
    if (typeof envelope.seq === "number" && envelope.seq > lastSeq) {
      lastSeq = envelope.seq;
      localStorage.setItem(`auction:${auctionId}:lastSeq`, String(lastSeq));
    }
    await dispatchEnvelope(envelope);
  };

  ws.onclose = () => scheduleReconnect();
  ws.onerror = () => {
    if (ws && ws.readyState === WebSocket.OPEN) ws.close();
  };
}

async function dispatchEnvelope(envelope) {
  const payload = envelope.payload || {};
  switch (envelope.type) {
    case "auction.started":
      handlers.onAuctionStarted(payload);
      break;
    case "bid.accepted":
      handlers.onBidAccepted(payload);
      break;
    case "bid.rejected":
      handlers.onBidRejected(payload);
      break;
    case "bid.ack":
      handlers.onBidAck(payload, envelope.requestId);
      break;
    case "ranking.updated":
      handlers.onRankingUpdated(payload);
      break;
    case "timer.tick":
      handlers.onTimerTick(payload);
      break;
    case "timer.extended":
      handlers.onTimerExtended(payload);
      break;
    case "auction.closed":
      handlers.onAuctionClosed(payload);
      break;
    case "room.online":
      handlers.onRoomOnline(payload);
      break;
    case "room.snapshot_required":
      await reloadAuctionState(payload.auctionId);
      break;
    case "pong":
    case "heartbeat.ack":
    case "ack":
    case "room.subscribed":
    case "room.unsubscribed":
      break;
    case "error":
      handlers.onError(payload);
      break;
    default:
      break;
  }
}

function placeBid(priceCent) {
  if (!ws || ws.readyState !== WebSocket.OPEN) throw new Error("WebSocket 未连接");
  const requestId = `bid-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  ws.send(JSON.stringify({
    type: "bid.place",
    requestId,
    payload: {
      auctionId,
      price: priceCent,
      idempotencyKey: requestId
    }
  }));
  return requestId;
}

function sendHeartbeat() {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  ws.send(JSON.stringify({
    type: "heartbeat",
    requestId: `hb-${Date.now()}`
  }));
}

async function reloadAuctionState(id) {
  const res = await fetch(`${API_BASE}/api/v1/auctions/${id}/state`, {
    headers: { Authorization: `Bearer ${accessToken}` }
  });
  const body = await res.json();
  if (!res.ok || body.code !== 0) throw new Error(body.message || "拉取拍卖状态失败");
  applyAuctionState(body.data);
}

function applyAuctionState(state) {
  auctionId = state.auctionId;
}

function scheduleReconnect() {
  if (reconnectTimer) return;
  reconnectTimer = window.setTimeout(() => {
    reconnectTimer = 0;
    connectAuction();
  }, 1000);
}

async function main() {
  await login();
  connectAuction();
  window.setInterval(sendHeartbeat, 30000);
}
```

## 10. 附录

### 10.1 事件速查表

| 方向 | type | 是否当前源码支持 | 说明 |
|---|---|---|---|
| S→C | `room.online` | 是 | 在线人数变化 |
| S→C | `auction.started` | 是 | 拍卖启动 |
| S→C | `bid.accepted` | 是 | 出价接受广播 |
| S→C | `bid.rejected` | 是 | 出价拒绝广播 |
| S→C | `bid.ack` | 是 | 当前连接出价响应 |
| S→C | `ranking.updated` | 是 | Top 10 排名更新 |
| S→C | `timer.extended` | 是 | 防狙击延时 |
| S→C | `timer.tick` | 是 | 倒计时 tick |
| S→C | `auction.closed` | 是 | 落槌结束 |
| S→C | `room.snapshot_required` | 是 | 历史事件无法完整补偿 |
| S→C | `pong` | 是 | 业务 `ping` 响应 |
| S→C | `heartbeat.ack` | 是 | 业务 `heartbeat` 响应 |
| S→C | `ack` | 是 | Hub 通用确认 |
| S→C | `room.subscribed` | 是 | `room.subscribe` 响应 |
| S→C | `subscribed` | 是 | `subscribe` 响应 |
| S→C | `room.unsubscribed` | 是 | `room.unsubscribe` 响应 |
| S→C | `announcement` | 是 | 客户端 `announcement` 原样广播 |
| S→C | `risk.event` | 是 | 风险事件广播 |
| S→C | `error` | 是 | JSON 错误或空类型错误 |
| S→C | `auction.state` | 否 | 当前源码不广播该事件，请使用 REST state 接口 |
| C→S | `bid.place` | 是 | 提交出价 |
| C→S | `ping` | 是 | 业务 ping |
| C→S | `heartbeat` | 是 | 业务心跳 |
| C→S | `room.subscribe` | 是 | 订阅指定拍品 room |
| C→S | `room.unsubscribe` | 是 | 取消订阅指定拍品 room |
| C→S | `subscribe` | 是 | 订阅当前拍品 room |
| C→S | `announcement` | 是 | 广播公告 envelope |

### 10.2 关闭码速查表

当前源码没有定义应用层 WebSocket Close Code，也没有主动写 Close control frame。可观测关闭主要来自下列内部原因或传输错误：

| 内部原因 / 场景 | 是否发送自定义关闭码 | 客户端表现 | 建议处理 |
|---|---|---|---|
| 客户端主动关闭或读协程读失败，内部原因 `read_closed` | 否 | `onclose` | 带 `lastSeq` 重连 |
| 写 JSON 或 Ping 失败，内部原因 `write_closed` | 否 | `onclose` | 带 `lastSeq` 重连 |
| 取消订阅，内部原因 `unsubscribe` | 否 | 可能关闭对应连接输出 | 如仍需观看则重新连接或重新订阅 |
| 慢消费者，内部原因 `slow_consumer` | 否 | `onclose` 或不再收到消息 | 优化消费速度，重连并用 `lastSeq` 补偿 |
| Pong 超时 | 否 | 底层读超时后连接关闭 | 确保客户端正常回复协议级 Pong |

### 10.3 代码索引路径列表

| 内容 | 路径 |
|---|---|
| 路由注册、WS Handler 构造参数 | `internal/app/server.go` |
| Upgrade、鉴权后连接处理、`lastSeq`、入站消息处理 | `internal/transport/http/ws_handler.go` |
| Envelope 结构、`ack`、`error` | `internal/transport/ws/envelope.go` |
| Hub room、seq、history、慢消费者、在线数、默认入站事件 | `internal/transport/ws/hub.go` |
| Redis Stream replay / EventRelay | `internal/transport/ws/event_log.go`、`internal/infra/redis/event_log.go` |
| 出价服务、`bid.ack`、`bid.accepted`、`ranking.updated`、`timer.extended`、`bid.rejected` | `internal/service/bid.go` |
| 拍卖启动与 REST state | `internal/service/auction.go`、`internal/transport/http/auction_handler.go` |
| 落槌与 `auction.closed` | `internal/service/hammer.go` |
| 定时器与 `timer.tick` | `internal/service/timer.go` |
| 风控事件 `risk.event` | `internal/service/risk.go` |
| Lua 出价原子逻辑与拒绝原因 | `scripts/lua/bid.lua` |
| Lua 落槌原子逻辑 | `scripts/lua/hammer.lua` |
| 登录和 JWT Bearer 鉴权 | `internal/transport/http/auth_handler.go`、`internal/service/auth.go` |
| 默认 WebSocket 配置 | `internal/config/config.go` |

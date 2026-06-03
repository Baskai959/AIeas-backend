# WebSocket 用户端交互协议

本文面向用户端前端接入，说明当前后端实际支持的 WebSocket 连接方式、上行消息、下行消息和字段含义。

源码依据：

- `internal/app/server.go`
- `internal/transport/http/ws_handler.go`
- `internal/transport/ws/envelope.go`
- `internal/transport/ws/hub.go`
- `internal/domain/{auction,increment_rule}.go`
- `internal/service/{auction,bid,hammer,timer,risk,ai_assistant,mcp_control}.go`
- `internal/infra/redis/event_log.go`
- `scripts/lua/bid.lua`

## 1. 连接地址

当前实际注册的 WebSocket 路由不带 `/api/v1` 前缀。

| 地址 | 用途 | 订阅范围 |
| --- | --- | --- |
| `GET /ws/auctions/:auction_id` | 直接订阅某个拍品的实时事件 | `auction_id` 对应的拍品房间 |
| `GET /ws/live-sessions/:session_id` | 订阅某个直播场次 | 场次当前活跃拍品事件，以及场次维度事件 |

鉴权与 REST 共用 JWT：

| 方式 | 示例 | 说明 |
| --- | --- | --- |
| Header | `Authorization: Bearer <accessToken>` | 非浏览器客户端优先使用 |
| Query | `/ws/auctions/10001?token=<accessToken>` | 浏览器原生 `WebSocket` 不能设置 Header 时使用 |

断线重连可携带 `lastSeq`：

```text
wss://example.com/ws/auctions/10001?token=<accessToken>&lastSeq=128
wss://example.com/ws/live-sessions/20001?token=<accessToken>&lastSeq=128
```

服务端会尝试重放 `seq > lastSeq` 的历史事件。若历史窗口不足，会下发 `room.snapshot_required`，客户端应重新拉取 REST 快照后再继续处理增量事件。

## 2. 统一消息信封

所有业务文本帧都是 JSON，外层结构如下：

```json
{
  "type": "bid.accepted",
  "requestId": "req-001",
  "seq": 129,
  "ack": false,
  "liveSessionId": 20001,
  "payload": {}
}
```

| 字段 | 类型 | 方向 | 必填 | 含义 |
| --- | --- | --- | --- | --- |
| `type` | string | 双向 | 是 | 消息类型，使用小写点分命名，例如 `bid.place`、`bid.accepted` |
| `requestId` | string | 双向 | 否 | 请求关联 ID。用户端发起 `bid.place` 时建议必填，也会作为出价幂等键 |
| `seq` | int64 | 主要下行 | 否 | 房间事件递增序号。客户端应使用它做去重和乱序保护 |
| `ack` | bool | 下行 | 否 | 通用 ACK 标记。仅 `type=ack` 时为 `true` |
| `liveSessionId` | uint64 | 下行 | 否 | 场次维度消息所属直播场次。普通拍品房间消息不一定有外层值 |
| `payload` | object | 双向 | 否 | 业务载荷，结构随 `type` 变化 |

处理规则：

- 客户端应记录每个拍品房间已处理的最大 `seq`。
- 若收到 `seq <= lastHandledSeq` 的消息，应忽略，避免重复渲染。
- `room.snapshot` 的 `seq` 在 `payload.seq` 中，表示快照对齐到的房间水位。
- `bid.accepted` / `bid.rejected` 的 `payload.seq` 与外层 `seq` 通常一致，但客户端以外层 `seq` 做房间级去重即可。
- 服务端还会发送 WebSocket 协议级 Ping control frame，浏览器会自动回复 Pong，不是 JSON 业务消息。

## 3. 客户端上行消息

### 3.1 `bid.place`

提交出价。

```json
{
  "type": "bid.place",
  "requestId": "bid-10001-001",
  "payload": {
    "auctionId": 10001,
    "price": 120000,
    "expectedCurrentPrice": 110000
  }
}
```

| 字段 | 类型 | 必填 | 含义 |
| --- | --- | --- | --- |
| `requestId` | string | 建议必填 | 出价请求 ID。用于响应关联和幂等 |
| `payload.auctionId` | uint64 | 否 | 拍品 ID。省略时使用连接路径中的 `auction_id` 或场次当前活跃拍品 |
| `payload.price` | int64 | 是 | 出价金额，单位为分。必须满足拍品的 `incrementRule`、`capPrice` 和单次最大步数限制 |
| `payload.expectedCurrentPrice` | int64 | 是 | 客户端看到的当前价，服务端用于并发校验和单次最大步数校验。缺失时返回 `MISSING_EXPECTED_STATE` |

响应：

- 当前连接收到 `bid.ack`
- 所有订阅该拍品房间的客户端可能收到 `bid.accepted`、`bid.rejected`、`ranking.updated`、`timer.extended`、`auction.closed`

### 3.2 `room.subscribe`

订阅拍品房间。连接建立时已自动订阅一次，通常不需要再发。切换拍品时可使用。

```json
{
  "type": "room.subscribe",
  "requestId": "sub-001",
  "payload": {
    "auctionId": 10002
  }
}
```

| 字段 | 类型 | 必填 | 含义 |
| --- | --- | --- | --- |
| `requestId` | string | 否 | 请求关联 ID |
| `payload.auctionId` | uint64 | 否 | 要订阅的拍品 ID。省略时使用当前连接的拍品 ID |

响应：

```json
{
  "type": "room.subscribed",
  "requestId": "sub-001",
  "payload": {
    "auctionId": 10002
  }
}
```

### 3.3 `room.unsubscribe`

取消订阅拍品房间。

```json
{
  "type": "room.unsubscribe",
  "requestId": "unsub-001",
  "payload": {
    "auctionId": 10002
  }
}
```

| 字段 | 类型 | 必填 | 含义 |
| --- | --- | --- | --- |
| `requestId` | string | 否 | 请求关联 ID |
| `payload.auctionId` | uint64 | 否 | 要取消订阅的拍品 ID。省略时使用当前连接的拍品 ID |

响应：

```json
{
  "type": "room.unsubscribed",
  "requestId": "unsub-001",
  "payload": {
    "auctionId": 10002
  }
}
```

### 3.4 `heartbeat`

业务心跳。用于刷新服务端在线计数，协议级 Ping/Pong 仍由 WebSocket 底层处理。

```json
{
  "type": "heartbeat",
  "requestId": "hb-001"
}
```

响应：

```json
{
  "type": "heartbeat.ack",
  "requestId": "hb-001",
  "payload": {
    "ts": 1730000000000
  }
}
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `payload.ts` | int64 | 服务端当前 Unix 毫秒时间 |

## 4. 服务端下行消息

### 4.1 `room.snapshot`

连接建立后服务端主动下发的拍品快照。用于首屏渲染，避免等待下一次出价或倒计时。

```json
{
  "type": "room.snapshot",
  "payload": {
    "auctionId": 10001,
    "status": "RUNNING",
    "startPrice": 100000,
    "capPrice": 300000,
    "incrementRule": {
      "type": "fixed",
      "amount": 1000,
      "maxBidSteps": 10
    },
    "currentPrice": 110000,
    "leaderBidderId": "u_1001",
    "startTime": 1730000000000,
    "endTime": 1730000300000,
    "extendCount": 1,
    "version": 12,
    "seq": 128,
    "serverTime": 1730000010000,
    "source": "redis",
    "degraded": false
  }
}
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `payload.auctionId` | uint64 | 拍品 ID |
| `payload.status` | string | 拍品状态，例如 `RUNNING`、`EXTENDED`、`CLOSED_WON` |
| `payload.startPrice` | int64 | 起拍价，单位为分 |
| `payload.capPrice` | int64 | 封顶价，单位为分；`0` 表示不设置封顶价 |
| `payload.incrementRule` | object | 拍品加价规则。Redis 状态缺失且 DB 降级也会尽量补齐；可能在历史状态缺失时省略 |
| `payload.currentPrice` | int64 | 当前价，单位为分 |
| `payload.leaderBidderId` | string | 当前领先用户 ID，可能为空 |
| `payload.startTime` | int64 | 开始时间，Unix 毫秒 |
| `payload.endTime` | int64 | 结束时间，Unix 毫秒 |
| `payload.extendCount` | int | 已延时次数 |
| `payload.version` | int64 | 拍卖状态版本 |
| `payload.seq` | int64 | 快照对应的房间水位，后续只处理 `seq` 更大的增量 |
| `payload.serverTime` | int64 | 服务端当前 Unix 毫秒时间，用于计算时钟偏移 |
| `payload.source` | string | 快照来源，常见为 `redis` 或 `db` |
| `payload.degraded` | bool | 是否从 DB 降级读取。`true` 表示实时态可能不是最新 |

`incrementRule` 当前支持两种格式：

固定加价：

```json
{
  "type": "fixed",
  "amount": 100,
  "maxBidSteps": 10
}
```

阶梯加价：

```json
{
  "type": "ladder",
  "maxBidSteps": 5,
  "steps": [
    { "min": 0, "max": 10000, "amount": 100 },
    { "min": 10000, "amount": 500 }
  ]
}
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `type` | string | `fixed` 固定加价，`ladder` 阶梯加价 |
| `amount` | int64 | 固定加价金额，单位为分；仅 `fixed` 使用，必须大于 0 |
| `maxBidSteps` | int | 单次出价最多跨越的加价步数，必须大于 0 |
| `steps[].min` | int64 | 阶梯下界，单位为分，包含该值 |
| `steps[].max` | int64 | 阶梯上界，单位为分，不包含该值；最后一个阶梯必须省略 |
| `steps[].amount` | int64 | 当前阶梯的加价金额，单位为分，必须大于 0 |

创建拍品时如果未传 `incrementRule`，后端默认写入 `{"type":"fixed","amount":100,"maxBidSteps":10}`。用户端可用快照里的 `incrementRule` 约束出价按钮和输入框，但最终裁决以后端为准。

### 4.2 `room.online`

在线人数变化。

```json
{
  "type": "room.online",
  "seq": 129,
  "payload": {
    "auctionId": 10001,
    "online": 23
  }
}
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `seq` | int64 | 房间事件序号 |
| `payload.auctionId` | uint64 | 拍品 ID |
| `payload.online` | int | 当前在线买家数。商家和管理员连接不计入买家在线数 |

### 4.3 `room.snapshot_required`

重连补偿失败时下发。客户端应走 REST 快照恢复，然后以新快照水位继续接收增量。

```json
{
  "type": "room.snapshot_required",
  "payload": {
    "auctionId": 10001,
    "lastSeq": 12,
    "reason": "EVENT_WINDOW_EXPIRED"
  }
}
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `payload.auctionId` | uint64 | 拍品 ID |
| `payload.lastSeq` | int64 | 客户端重连时携带的旧水位 |
| `payload.reason` | string | 原因，目前为 `EVENT_WINDOW_EXPIRED` |

### 4.4 `bid.ack`

当前连接提交 `bid.place` 后的直接响应。

成功示例：

```json
{
  "type": "bid.ack",
  "requestId": "bid-10001-001",
  "payload": {
    "requestId": "bid-10001-001",
    "auctionId": 10001,
    "liveSessionId": 20001,
    "bidderId": "u_1001",
    "bidderNickname": "用户A",
    "price": 120000,
    "accepted": true,
    "currentPrice": 120000,
    "leaderBidderId": "u_1001",
    "endTime": "2026-06-01T12:00:30Z",
    "extended": false,
    "extendCount": 1,
    "version": 13,
    "seq": 130,
    "streamId": "130-0",
    "event": "bid.accepted",
    "riskResult": "ALLOW",
    "auctionStatus": "RUNNING"
  }
}
```

失败示例：

```json
{
  "type": "bid.ack",
  "requestId": "bid-10001-001",
  "payload": {
    "accepted": false,
    "reason": "MISSING_EXPECTED_STATE"
  }
}
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `payload.requestId` | string | 出价请求 ID |
| `payload.auctionId` | uint64 | 拍品 ID |
| `payload.liveSessionId` | uint64 | 所属直播场次 ID，可能省略 |
| `payload.bidderId` | string | 出价用户 ID |
| `payload.bidderNickname` | string | 出价用户昵称，可能省略 |
| `payload.price` | int64 | 本次出价金额，单位为分 |
| `payload.accepted` | bool | 是否接受本次出价 |
| `payload.duplicate` | bool | 是否为重复请求命中幂等结果 |
| `payload.reason` | string | 拒绝原因或错误原因 |
| `payload.code` | int | 服务层错误码，仅部分错误响应出现 |
| `payload.currentPrice` | int64 | 服务端裁决后的当前价，单位为分 |
| `payload.leaderBidderId` | string | 当前领先用户 ID |
| `payload.endTime` | string | 拍品结束时间，Go `time.Time` JSON 格式 |
| `payload.extended` | bool | 本次出价是否触发反狙击延时 |
| `payload.extendCount` | int | 已延时次数 |
| `payload.version` | int64 | 拍卖状态版本 |
| `payload.seq` | int64 | 出价事件序号 |
| `payload.streamId` | string | Redis Stream ID，Redis 实时路径下出现 |
| `payload.event` | string | 对应广播事件，`bid.accepted` 或 `bid.rejected` |
| `payload.riskResult` | string | 风控结果，`ALLOW`、`REJECT` 或 `REVIEW` |
| `payload.auctionStatus` | string | 裁决后拍品状态 |
| `payload.autoClosed` | bool | 是否因达到封顶价等原因自动触发关闭 |

常见失败 `reason`：

| reason | 含义 |
| --- | --- |
| `BID_SERVICE_UNAVAILABLE` | WebSocket Handler 未注入出价服务 |
| `INVALID_PAYLOAD` | `payload` JSON 解析失败或存在未知字段 |
| `MISSING_EXPECTED_STATE` | 缺少 `expectedCurrentPrice` |
| `STALE_AUCTION_STATE` | 客户端 `expectedCurrentPrice` 与服务端当前价不一致，或客户端状态已经过期 |
| `BELOW_START_PRICE` | 出价小于或等于起拍价 |
| `PRICE_STEP_MISMATCH` | 出价不符合当前加价步长 |
| `BELOW_MIN_INCREMENT` | 出价未达到当前价加一个最小加价步长 |
| `ABOVE_MAX_BID_STEPS` | 出价超过基于服务端当前价计算出的单次最大允许价 |
| `ABOVE_EXPECTED_MAX_BID_STEPS` | 客户端状态落后时，出价超过基于 `expectedCurrentPrice` 计算出的单次最大允许价 |
| `ABOVE_CAP_PRICE` | 出价超过封顶价 |
| `INVALID_STATE` | 拍品不处于 `RUNNING` 或 `EXTENDED` |
| `NOT_ENROLLED` | 买家未报名该拍品 |
| `DEPOSIT_NOT_READY` | 保证金未冻结或未就绪 |
| `BLACKLIST` | 用户命中黑名单 |
| `FREQ_LIMIT` | 出价频率过高 |
| 其他中文文案 | 服务层错误经 HTTP 错误映射后的提示 |

### 4.5 `bid.accepted`

出价被接受后的房间广播。所有订阅该拍品的客户端都会收到。

Go 直连路径下，payload 与 `bid.ack` 成功时的 `BidResult` 基本一致：

```json
{
  "type": "bid.accepted",
  "seq": 130,
  "payload": {
    "requestId": "bid-10001-001",
    "auctionId": 10001,
    "liveSessionId": 20001,
    "bidderId": "u_1001",
    "bidderNickname": "用户A",
    "price": 120000,
    "accepted": true,
    "currentPrice": 120000,
    "leaderBidderId": "u_1001",
    "endTime": "2026-06-01T12:00:30Z",
    "extended": false,
    "extendCount": 1,
    "version": 13,
    "seq": 130,
    "streamId": "130-0",
    "event": "bid.accepted",
    "riskResult": "ALLOW",
    "auctionStatus": "RUNNING"
  }
}
```

Redis Stream / PubSub 路径下，payload 字段名略有差异：

```json
{
  "type": "bid.accepted",
  "seq": 130,
  "payload": {
    "requestId": "bid-10001-001",
    "auctionId": 10001,
    "liveSessionId": 20001,
    "bidderId": "u_1001",
    "bidderNickname": "用户A",
    "price": 120000,
    "accepted": true,
    "reason": "",
    "currentPrice": 120000,
    "leaderBidderId": "u_1001",
    "endTsMs": 1730000300000,
    "extended": false,
    "extendCount": 1,
    "seq": 130,
    "streamId": "130-0",
    "createdAtMs": 1730000000000,
    "bidTsMs": 1730000000000,
    "source": "live_ws",
    "event": "bid.accepted",
    "riskResult": "ALLOW",
    "auctionStatus": "RUNNING",
    "autoClosed": false
  }
}
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `seq` | int64 | 房间事件序号。客户端去重以外层 `seq` 为准 |
| `payload.price` | int64 | 本次出价金额，单位为分 |
| `payload.currentPrice` | int64 | 最新当前价，单位为分 |
| `payload.leaderBidderId` | string | 最新领先用户 ID |
| `payload.endTime` | string | 结束时间，Go `time.Time` JSON 格式，Go 直连路径常见 |
| `payload.endTsMs` | int64 | 结束时间，Unix 毫秒，Redis 路径常见 |
| `payload.extended` | bool | 是否触发延时 |
| `payload.extendCount` | int | 已延时次数 |
| `payload.autoClosed` | bool | 是否自动触发关闭 |

其他字段含义与 `bid.ack` 相同。

### 4.6 `bid.rejected`

出价被拒绝后的房间广播。payload 字段与 `bid.accepted` 类似，但 `accepted=false`，`reason` 表示拒绝原因。

```json
{
  "type": "bid.rejected",
  "seq": 131,
  "payload": {
    "requestId": "bid-10001-002",
    "auctionId": 10001,
    "bidderId": "u_1002",
    "price": 121000,
    "accepted": false,
    "reason": "FREQ_LIMIT",
    "currentPrice": 120000,
    "leaderBidderId": "u_1001",
    "seq": 131,
    "event": "bid.rejected",
    "riskResult": "REJECT"
  }
}
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `payload.accepted` | bool | 固定为 `false` |
| `payload.reason` | string | 拒绝原因 |
| `payload.currentPrice` | int64 | 拒绝后仍然有效的当前价 |
| `payload.leaderBidderId` | string | 当前领先用户 ID |

### 4.7 `ranking.updated`

排行榜更新。通常在 `bid.accepted` 后下发。

```json
{
  "type": "ranking.updated",
  "seq": 132,
  "payload": {
    "auctionId": 10001,
    "ranking": [
      {
        "rank": 1,
        "bidderId": "u_1001",
        "bidderNickname": "用户A",
        "price": 120000
      }
    ]
  }
}
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `payload.auctionId` | uint64 | 拍品 ID |
| `payload.ranking` | array | Top N 排名，当前服务默认取前 10 |
| `payload.ranking[].rank` | int | 名次，从 1 开始 |
| `payload.ranking[].bidderId` | string | 出价用户 ID |
| `payload.ranking[].bidderNickname` | string | 出价用户昵称，可能省略 |
| `payload.ranking[].price` | int64 | 出价金额，单位为分 |

### 4.8 `timer.extended`

出价触发反狙击延时后下发。

```json
{
  "type": "timer.extended",
  "seq": 133,
  "payload": {
    "auctionId": 10001,
    "endTime": "2026-06-01T12:01:00Z",
    "extendCount": 2
  }
}
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `payload.auctionId` | uint64 | 拍品 ID |
| `payload.endTime` | string | 新结束时间，Go `time.Time` JSON 格式 |
| `payload.extendCount` | int | 已延时次数 |

### 4.9 `timer.tick`

倒计时同步事件。服务端 TimerScheduler 会周期性读取 Redis 权威状态并广播。

```json
{
  "type": "timer.tick",
  "seq": 134,
  "payload": {
    "auctionId": 10001,
    "endTime": "2026-06-01T12:01:00Z",
    "remainingMs": 24500,
    "status": "RUNNING"
  }
}
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `payload.auctionId` | uint64 | 拍品 ID |
| `payload.endTime` | string | 当前权威结束时间 |
| `payload.remainingMs` | int64 | 服务端计算的剩余毫秒数 |
| `payload.status` | string | 当前拍品状态 |

前端不应完全依赖每秒 tick 才更新倒计时。推荐用 `room.snapshot` / `bid.accepted` / `timer.extended` / `timer.tick` 中的 `endTime` 或 `endTsMs` 校准本地倒计时。

### 4.10 `auction.started`

拍品开拍后下发。

```json
{
  "type": "auction.started",
  "seq": 135,
  "payload": {
    "auctionId": 10001,
    "state": {
      "auctionId": 10001,
      "status": "RUNNING",
      "startPrice": 100000,
      "capPrice": 300000,
      "incrementRule": {
        "type": "fixed",
        "amount": 1000,
        "maxBidSteps": 10
      },
      "currentPrice": 100000,
      "leaderBidderId": "",
      "bidCount": 0,
      "startTime": "2026-06-01T12:00:00Z",
      "endTime": "2026-06-01T12:30:00Z",
      "lastBidTsMs": 0,
      "extendCount": 0,
      "version": 1,
      "source": "redis"
    }
  }
}
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `payload.auctionId` | uint64 | 拍品 ID |
| `payload.state` | object | 开拍后的拍品实时状态 |
| `payload.state.startPrice` | int64 | 起拍价，单位为分 |
| `payload.state.capPrice` | int64 | 封顶价，单位为分；`0` 表示不设置封顶价 |
| `payload.state.incrementRule` | object | 拍品加价规则，格式同 `room.snapshot.payload.incrementRule` |
| `payload.state.currentPrice` | int64 | 当前价，单位为分 |
| `payload.state.startTime` | string | 开始时间，Go `time.Time` JSON 格式 |
| `payload.state.endTime` | string | 结束时间，Go `time.Time` JSON 格式 |
| `payload.state.lastBidTsMs` | int64 | 最近出价时间，Unix 毫秒 |
| `payload.state.source` | string | 状态来源 |

### 4.11 `auction.closed`

拍品落槌或关闭后下发。客户端应停止倒计时并禁用出价。

```json
{
  "type": "auction.closed",
  "seq": 136,
  "payload": {
    "auctionId": 10001,
    "status": "CLOSED_WON",
    "winnerId": "u_1001",
    "price": 120000,
    "closedAt": "2026-06-01T12:01:00Z",
    "orderId": 90001
  }
}
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `payload.auctionId` | uint64 | 拍品 ID |
| `payload.status` | string | 关闭状态，常见为 `CLOSED_WON` 或 `CLOSED_FAILED` |
| `payload.winnerId` | string | 成交用户 ID，流拍时可能为空 |
| `payload.price` | int64 | 成交价，单位为分 |
| `payload.closedAt` | string | 关闭时间，Go `time.Time` JSON 格式 |
| `payload.orderId` | uint64 | 成交订单 ID，仅成交且订单生成成功时出现 |

### 4.12 `live_session.ended`

直播场次结束事件。订阅 `/ws/live-sessions/:session_id` 的客户端会收到。

```json
{
  "type": "live_session.ended",
  "liveSessionId": 20001,
  "payload": {
    "liveSessionId": 20001,
    "status": "CLOSED"
  }
}
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `liveSessionId` | uint64 | 外层场次 ID |
| `payload.liveSessionId` | uint64 | 场次 ID |
| `payload.status` | string | 场次状态 |

### 4.13 `live.voice_broadcast`

AI/TTS 直播语音播报事件。订阅对应直播场次的客户端会收到。

```json
{
  "type": "live.voice_broadcast",
  "requestId": "voice-001",
  "liveSessionId": 20001,
  "payload": {
    "liveSessionId": 20001,
    "text": "现在为大家介绍这件拍品",
    "requestId": "voice-001",
    "audioBase64": "BASE64_AUDIO",
    "audioFormat": "pcm",
    "encoding": "base64",
    "sampleRate": 24000,
    "channels": 1,
    "voice": "zh_female",
    "provider": "doubao",
    "audioBytes": 12345,
    "createdAt": "2026-06-01T12:00:00Z"
  }
}
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `payload.liveSessionId` | uint64 | 直播场次 ID |
| `payload.text` | string | 播报文本 |
| `payload.requestId` | string | 请求关联 ID |
| `payload.audioBase64` | string | Base64 编码后的音频内容 |
| `payload.audioFormat` | string | 音频格式，例如 `pcm` |
| `payload.encoding` | string | 编码方式 |
| `payload.sampleRate` | int | 采样率 |
| `payload.channels` | int | 声道数 |
| `payload.voice` | string | 音色，可能省略 |
| `payload.provider` | string | TTS 提供方，可能省略 |
| `payload.audioBytes` | int | 原始音频字节数 |
| `payload.createdAt` | string | 生成时间，Go `time.Time` JSON 格式 |

### 4.14 `ai.assistant.switch`

AI 直播助手开关变更事件。用户端可据此切换 AI 直播状态展示。

```json
{
  "type": "ai.assistant.switch",
  "liveSessionId": 20001,
  "payload": {
    "eventId": "ai-20001-1730000000000000000",
    "kind": "switch",
    "status": "enabled",
    "toolName": "ai_live_assistant",
    "merchantId": "u_2001",
    "liveSessionId": 20001,
    "enabled": true,
    "message": "直播场次20001AI直播助手已开启",
    "createdAt": "2026-06-01T12:00:00Z"
  }
}
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `payload.eventId` | string | AI 助手事件 ID |
| `payload.kind` | string | 固定为 `switch` |
| `payload.status` | string | `enabled` 或 `disabled` |
| `payload.toolName` | string | 固定为 `ai_live_assistant` |
| `payload.merchantId` | string | 商家 ID |
| `payload.liveSessionId` | uint64 | 直播场次 ID |
| `payload.enabled` | bool | AI 直播助手是否开启 |
| `payload.message` | string | 展示文案 |
| `payload.createdAt` | string | 事件创建时间 |

### 4.15 `ai.assistant.broadcast`

AI 助手准备生成直播播报时下发。通常随后可能收到 `live.voice_broadcast`。

```json
{
  "type": "ai.assistant.broadcast",
  "requestId": "voice-001",
  "liveSessionId": 20001,
  "payload": {
    "eventId": "ai-20001-1730000000000000000",
    "kind": "broadcast",
    "status": "running",
    "toolName": "live_voice_broadcast",
    "merchantId": "u_2001",
    "liveSessionId": 20001,
    "requestId": "voice-001",
    "message": "AI 正在生成直播播报",
    "broadcastText": "现在为大家介绍这件拍品",
    "createdAt": "2026-06-01T12:00:00Z"
  }
}
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `payload.kind` | string | 固定为 `broadcast` |
| `payload.status` | string | 当前状态，通常为 `running` |
| `payload.toolName` | string | 工具名，当前为 `live_voice_broadcast` |
| `payload.broadcastText` | string | AI 准备播报的文本 |
| 其他字段 | - | 与 `ai.assistant.switch` 中同名字段含义一致 |

### 4.16 `ai.assistant.status`

AI 助手运行状态事件。用于展示 AI 控制操作执行进度。

```json
{
  "type": "ai.assistant.status",
  "requestId": "op-001",
  "liveSessionId": 20001,
  "payload": {
    "eventId": "ai-20001-1730000000000000000",
    "kind": "status",
    "status": "completed",
    "toolName": "operate_live_session_lot",
    "merchantId": "u_2001",
    "liveSessionId": 20001,
    "requestId": "op-001",
    "message": "操作已完成",
    "createdAt": "2026-06-01T12:00:00Z"
  }
}
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `payload.kind` | string | 固定为 `status` |
| `payload.status` | string | 状态，例如 `completed`、`failed` |
| `payload.toolName` | string | AI 正在执行或已执行的工具名 |
| `payload.message` | string | 状态提示 |
| 其他字段 | - | 与 `AIAssistantEvent` 同名字段含义一致 |

### 4.17 `ai.assistant.permission_request`

AI 助手执行控制操作前需要商家确认时下发。用户端如果只是观众端，通常只需要展示或忽略；商家端应通过 REST 接口提交审批结果。

```json
{
  "type": "ai.assistant.permission_request",
  "requestId": "approval-001",
  "liveSessionId": 20001,
  "payload": {
    "eventId": "ai-20001-1730000000000000000",
    "kind": "permission",
    "status": "pending",
    "toolName": "operate_live_session_lot",
    "merchantId": "u_2001",
    "liveSessionId": 20001,
    "requestId": "approval-001",
    "permission": "ASK",
    "message": "AI 请求执行直播控制操作",
    "expiresAt": "2026-06-01T12:05:00Z",
    "createdAt": "2026-06-01T12:00:00Z"
  }
}
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `payload.kind` | string | 固定为 `permission` |
| `payload.status` | string | `pending`、`approved`、`rejected` 或 `timeout`。只有 `pending` 会使用该下行 type |
| `payload.permission` | string | 商家 AI 权限策略 |
| `payload.expiresAt` | string | 确认超时时间，可能省略 |
| `payload.requestId` | string | 审批请求 ID |
| 其他字段 | - | 与 `AIAssistantEvent` 同名字段含义一致 |

### 4.18 `risk.event`

风控事件广播。当前实现会广播到拍品房间，因此用户端可能收到；普通买家端建议默认忽略，运营或调试端可展示。

```json
{
  "type": "risk.event",
  "seq": 137,
  "payload": {
    "id": 1,
    "eventType": "BID_FREQ",
    "userId": "u_1001",
    "auctionId": 10001,
    "severity": "MID",
    "payload": {},
    "status": "PENDING",
    "createdAt": "2026-06-01T12:00:00Z"
  }
}
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `payload.id` | uint64 | 风控事件 ID |
| `payload.eventType` | string | 风控事件类型，例如 `BID_FREQ`、`AUTO_BLACKLIST` |
| `payload.userId` | string | 相关用户 ID |
| `payload.auctionId` | uint64 | 相关拍品 ID |
| `payload.severity` | string | 风险等级 |
| `payload.payload` | object | 原始风控上下文 |
| `payload.status` | string | 处理状态，`PENDING`、`REVIEWED` 或 `IGNORED` |
| `payload.reviewedBy` | string | 审核人，可能省略 |
| `payload.reviewedAt` | string | 审核时间，可能省略 |
| `payload.createdAt` | string | 创建时间 |

### 4.19 `ack`

Hub 默认通用 ACK。`bid.place`、`room.subscribe`、`room.unsubscribe`、`heartbeat` 由 `WSHandler` 单独处理，不会走这个通用 ACK。

```json
{
  "type": "ack",
  "requestId": "req-001",
  "seq": 1,
  "ack": true,
  "payload": {
    "requestId": "req-001",
    "seq": 1
  }
}
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `ack` | bool | 固定为 `true` |
| `payload.requestId` | string | 被确认的请求 ID |
| `payload.seq` | int64 | 被确认的请求序号 |

### 4.20 `error`

业务错误事件。当前主要在 JSON 无法解析、消息类型缺失、内部默认处理错误时出现；出价失败通常使用 `bid.ack` 表达，不使用 `error`。

```json
{
  "type": "error",
  "requestId": "req-001",
  "payload": {
    "message": "invalid json"
  }
}
```

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `payload.message` | string | 错误信息 |

### 4.21 `pong` 和 `heartbeat.ack`

`pong` 是对业务 `ping` 的响应；`heartbeat.ack` 是对业务 `heartbeat` 的响应。用户端推荐使用 `heartbeat`，同时让浏览器自动处理协议级 Ping/Pong。

```json
{
  "type": "pong",
  "requestId": "ping-001"
}
```

```json
{
  "type": "heartbeat.ack",
  "requestId": "hb-001",
  "payload": {
    "ts": 1730000000000
  }
}
```

## 5. 用户端推荐处理流程

1. 进入拍品或直播场次页面后建立 WebSocket 连接。
2. 收到 `room.snapshot` 后初始化 UI：当前价、起拍价、封顶价、加价规则、领先者、状态、倒计时、`lastSeq=payload.seq`。
3. 对所有带外层 `seq` 的消息做去重：只处理 `seq > lastSeq`，处理后更新 `lastSeq`。
4. 收到 `bid.accepted` 后更新当前价、领先者、倒计时、延时次数。
5. 收到 `ranking.updated` 后刷新排行榜。
6. 收到 `timer.extended` 或 `timer.tick` 后校准本地倒计时。
7. 收到 `auction.closed` 后停止倒计时并禁用出价。
8. 收到 `live_session.ended` 后退出直播场次或展示已结束状态。
9. WebSocket 断开后重连，并携带最近处理的 `lastSeq`。
10. 收到 `room.snapshot_required` 后重新拉取 REST 状态，不要继续依赖旧增量。

## 6. 当前未实现或不建议依赖的事件

当前源码没有实际广播下列事件，用户端不要依赖：

| 事件 | 说明 |
| --- | --- |
| `order.created` | 当前成交订单 ID 通过 `auction.closed.payload.orderId` 返回 |
| `bidder.overtaken` | 当前未实现单独广播 |
| `auction.state` | 当前状态快照通过 REST 或 `room.snapshot` 获取 |

`announcement` 在 Hub 默认入站处理中存在，会把客户端发送的消息广播到当前房间，但当前没有业务鉴权和场景约束。用户端不建议使用。

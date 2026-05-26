# 只读 MCP 服务设计与上游对接文档

## 1. 目标与边界

本文档定义 `aieas_backend` 第一阶段只读 MCP 服务的代码分层、MCP 资源/工具接口、权限边界和上游对接方式。

第一阶段 MCP 只提供读取能力，不提供任何会改变业务状态的接口：

- 读取用户、商家、商品、拍品、直播间、直播场次。
- 读取每场直播的拍品、出价记录、交易订单、成交汇总。
- 读取拍品实时状态、直播间统计、风险/审计只读数据。
- 不开放创建、更新、删除、出价、落槌、支付、审核、封禁等写操作。

协议基线：

- MCP 数据层使用 JSON-RPC 2.0。
- 远程服务使用 Streamable HTTP 风格入口，统一 endpoint 为 `/mcp`；当前只读实现使用 `POST /mcp` 处理 JSON-RPC 调用，`GET /mcp` 暂不启用服务端主动流，返回 405。
- 资源通过 `resources/templates/list` 和 `resources/read` 暴露。
- 只读工具通过 `tools/list` 和 `tools/call` 暴露，用于没有资源选择器或需要结构化查询参数的上游。

参考：

- MCP Server Concepts: https://modelcontextprotocol.io/docs/learn/server-concepts
- MCP Streamable HTTP Transport: https://modelcontextprotocol.io/specification/2025-06-18/basic/transports
- MCP Go SDK: https://go.sdk.modelcontextprotocol.io/

## 2. 总体设计

### 2.1 分层原则

MCP 是新的协议适配层，不是新的业务实现层。它只能调用 service/read model，不直接访问 MySQL、Redis、GORM 或 Lua 脚本。

```text
MCP Client / Host
  -> POST /mcp
  -> internal/transport/mcp
       - JSON-RPC / Streamable HTTP
       - MCP resources/tools/prompts registry
       - MCP auth context
       - MCP error mapping
       - MCP DTO rendering
  -> internal/service
       - existing services
       - optional ReadModelService / MCPReadService facade
  -> internal/repository
       - existing repository interfaces
       - read-optimized query methods where necessary
  -> MySQL / Redis
```

### 2.2 建议代码结构

所有 MCP 协议接口放在 `internal/transport/mcp` 包内，集中注册，避免散落到 HTTP handler 或 service 文件中。

```text
internal/
  app/
    server.go
      # 唯一装配点：构造 MCP read facade、handler，挂载 /mcp

  service/
    mcp_read.go
      # 可选：只读聚合 facade，不包含 MCP/JSON-RPC 概念
      # 负责跨 service 组合：直播场次详情、成交汇总、商家概览等

  transport/
    mcp/
      handler.go
        # Hertz endpoint：POST /mcp；GET /mcp 显式返回 405
      registry.go
        # RegisterResources、RegisterTools、RegisterPrompts
      auth.go
        # Authorization Bearer -> actorID/actorRole
      context.go
        # MCP request context、traceID、pagination defaults
      resources.go
        # resources/templates/list、resources/read URI 分发
      tools.go
        # read-only tools/list、tools/call 分发
      dto.go
        # 对上游稳定输出 DTO，避免直接暴露内部领域结构变动
      errors.go
        # domain/service error -> JSON-RPC/MCP error
      schema.go
        # tool inputSchema 定义
```

装配规则：

- `internal/app/server.go` 是唯一新增入口。
- `newServerWithServices(...)` 增加 `mcpReadService` 或由已有 service 构造 `mcp.Handler`。
- `/mcp` 与 `/api/v1` 同级挂载，不放在 `/api/v1` 下。
- 继续复用全局 `RecoveryMiddleware`、`RequestIDMiddleware`、`RateLimiter`、`AuditMiddleware`。
- MCP 自己解析 `Authorization`，但 token 解析逻辑复用 `AuthService.ParseAccessToken`，不要复制 JWT 实现。

### 2.3 建议新增 read model

当前已有查询能力能覆盖大部分资源，但直播场次出价/成交查询建议补充 read-optimized 方法，避免服务层循环查拍品再截断。

建议新增：

```go
// internal/repository/bid.go
type BidRepository interface {
    Create(ctx context.Context, bid *domain.BidRecord) error
    FindByRequestID(ctx context.Context, requestID string) (domain.BidRecord, error)
    ListByAuction(ctx context.Context, auctionID uint64, limit int) ([]domain.BidRecord, error)
    ListByLiveSession(ctx context.Context, sessionID uint64, limit, offset int) ([]domain.BidRecord, error)
}

// internal/domain/ops.go
type OrderFilter struct {
    WinnerID      string
    SellerID      string
    LiveSessionID uint64
    Status        OrderStatus
    PayStatus     PayStatus
    Limit         int
    Offset        int
}
```

`ListByLiveSession` 建议默认按 `bid_ts_ms DESC, id DESC` 返回，适合直播复盘；如果需要排行榜，单独提供 `ListTopBidsByAuction` 或 tool 参数 `sort=priceDesc`。

## 3. Endpoint 与传输

### 3.1 MCP Endpoint

```http
POST /mcp
```

`GET /mcp` 已挂载，但第一阶段不提供服务端主动消息流；客户端应使用 `POST /mcp` 完成 `initialize`、`resources/read`、`tools/call` 等只读调用。

Headers：

| Header | 必填 | 说明 |
|---|---|---|
| `Authorization: Bearer <accessToken>` | 是 | 复用现有登录接口签发的 JWT access token。 |
| `MCP-Protocol-Version: 2025-06-18` | 建议 | 上游声明协议版本。缺失时服务端按当前兼容版本处理。 |
| `Content-Type: application/json` | POST 必填 | JSON-RPC 请求体。 |
| `Accept: application/json, text/event-stream` | 建议 | Streamable HTTP 客户端建议同时支持 JSON 与 SSE。 |
| `X-Trace-Id` | 可选 | 透传链路追踪 ID。 |

### 3.2 初始化示例

Request：

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "initialize",
  "params": {
    "protocolVersion": "2025-06-18",
    "capabilities": {},
    "clientInfo": {
      "name": "upstream-agent",
      "version": "1.0.0"
    }
  }
}
```

Response：

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "protocolVersion": "2025-06-18",
    "capabilities": {
      "resources": {},
      "tools": {},
      "prompts": {}
    },
    "serverInfo": {
      "name": "aieas-readonly-mcp",
      "version": "1.0.0"
    }
  }
}
```

## 4. 鉴权与权限

### 4.1 角色

沿用现有角色：

- `buyer`
- `merchant`
- `admin`

### 4.2 权限矩阵

| 数据 | buyer | merchant | admin |
|---|---|---|---|
| 当前登录用户信息 | 仅本人 | 仅本人 | 仅本人 |
| 用户列表 | 禁止 | 禁止 | 允许 |
| 商家资料 | 仅公开安全字段 | 仅本人商家资料 | 任意商家 |
| 商品 | 仅随公开直播/拍品可见的安全字段 | 本商家商品 | 全部 |
| 拍品 | 公开直播相关拍品和自身订单相关拍品 | 本商家拍品 | 全部 |
| 拍品实时状态 | 已认证用户可读 | 已认证用户可读 | 已认证用户可读 |
| 直播间 | 仅 `LIVE` 房间 | 本商家房间 | 全部 |
| 直播场次 | 禁止，除非后续定义公开回放 | 本商家场次 | 全部 |
| 场次出价记录 | 禁止 | 本商家场次 | 全部 |
| 场次交易订单 | 仅本人中奖订单，默认不走场次接口 | 本商家场次订单 | 全部 |
| 成交汇总 | 禁止，除非后续定义公开统计 | 本商家场次 | 全部 |
| 风险事件/审计日志 | 禁止 | 自己操作日志可选 | 全部 |

安全字段说明：

- 用户与商家输出使用 `domain.SafeUser` 风格，不暴露 `account`、`passwordHash`、token、手机号等敏感字段。
- buyer 侧不输出其他用户的账号信息；出价列表默认不对 buyer 开放。
- MCP 不接受 `actorId`、`actorRole` 参数，调用身份只来自 JWT。

## 5. 返回格式

### 5.1 Resource 返回

所有 resource 内容使用 JSON 文本，`mimeType=application/json`。

```json
{
  "schemaVersion": "aieas.mcp.readonly.v1",
  "traceId": "trc_...",
  "data": {}
}
```

`resources/read` 响应示例：

```json
{
  "contents": [
    {
      "uri": "aieas://live-sessions/90001/settlement-summary",
      "mimeType": "application/json",
      "text": "{\"schemaVersion\":\"aieas.mcp.readonly.v1\",\"traceId\":\"trc_...\",\"data\":{}}"
    }
  ]
}
```

### 5.2 Tool 返回

只读 tools 返回 JSON 内容。若 SDK 支持 structured output，可同时返回 `structuredContent`；否则以 `content[0].text` 为准。

```json
{
  "content": [
    {
      "type": "text",
      "mimeType": "application/json",
      "text": "{\"schemaVersion\":\"aieas.mcp.readonly.v1\",\"traceId\":\"trc_...\",\"data\":{}}"
    }
  ],
  "isError": false
}
```

### 5.3 分页

统一分页参数：

| 参数 | 类型 | 默认 | 最大 | 说明 |
|---|---:|---:|---:|---|
| `limit` | integer | 20 | 100 | 返回条数。 |
| `offset` | integer | 0 | - | 偏移量，小于 0 按 0 处理。 |

列表响应：

```json
{
  "schemaVersion": "aieas.mcp.readonly.v1",
  "traceId": "trc_...",
  "data": {
    "items": [],
    "page": {
      "limit": 20,
      "offset": 0,
      "hasMore": false
    }
  }
}
```

第一阶段仓储不强制返回 total，`hasMore` 可通过 `len(items) == limit` 推断。

### 5.4 时间与金额

- 时间统一输出 RFC3339/RFC3339Nano UTC 字符串，例如 `2026-05-26T08:00:00Z`。
- 金额统一使用整数分，字段名保留现有 `Cent` 或业务字段，如 `dealPrice`、`gmvCent`。
- ID：实体 ID 使用 `uint64` JSON number；用户 ID 使用 string。

## 6. Resources 设计

URI scheme 固定为 `aieas://`。资源名使用 kebab-case，字段名保持现有 lowerCamelCase JSON 风格。

### 6.1 Resource Templates

| URI Template | 名称 | 说明 | 权限 |
|---|---|---|---|
| `aieas://users/{userId}` | `user` | 用户安全信息。 | 本人或 admin |
| `aieas://users?role={role}&status={status}&keyword={keyword}&limit={limit}&offset={offset}` | `users-list` | 用户列表。 | admin |
| `aieas://merchants/{merchantId}` | `merchant` | 商家安全资料和经营概览。 | 本商家或 admin；buyer 仅公开字段 |
| `aieas://merchants/{merchantId}/live-sessions?status={status}&limit={limit}&offset={offset}` | `merchant-live-sessions` | 商家直播场次列表。 | 本商家或 admin |
| `aieas://items/{itemId}` | `item` | 商品详情。 | 本商家或 admin；公开拍品关联时可返回安全字段 |
| `aieas://items?sellerId={sellerId}&status={status}&category={category}&limit={limit}&offset={offset}` | `items-list` | 商品列表。 | merchant/admin |
| `aieas://auction-lots/{auctionId}` | `auction-lot` | 拍品详情。 | 本商家/admin；公开直播拍品可读 |
| `aieas://auction-lots/{auctionId}/state` | `auction-state` | 拍品实时状态。 | 已认证用户 |
| `aieas://auction-lots?sellerId={sellerId}&status={status}&itemId={itemId}&liveRoomId={liveRoomId}&limit={limit}&offset={offset}` | `auction-lots-list` | 拍品列表。 | merchant/admin；buyer 仅公开直播范围 |
| `aieas://live-rooms/{roomId}` | `live-room` | 直播间详情。 | buyer 仅 LIVE；merchant 本人；admin 全部 |
| `aieas://live-rooms?merchantId={merchantId}&status={status}&limit={limit}&offset={offset}` | `live-rooms-list` | 直播间列表。 | buyer 仅 LIVE；merchant 本人；admin 全部 |
| `aieas://live-rooms/{roomId}/lots` | `live-room-lots` | 直播间挂载拍品。 | 同直播间 |
| `aieas://live-rooms/{roomId}/stats` | `live-room-stats` | 当前在拍统计、在线、当前价。 | 同直播间 |
| `aieas://live-rooms/{roomId}/live-sessions?status={status}&limit={limit}&offset={offset}` | `live-room-sessions` | 某直播间场次列表。 | 本商家或 admin |
| `aieas://live-sessions/{sessionId}` | `live-session` | 直播场次详情。 | 本商家或 admin |
| `aieas://live-sessions/{sessionId}/lots` | `live-session-lots` | 场次内拍品。 | 本商家或 admin |
| `aieas://live-sessions/{sessionId}/bids?limit={limit}&offset={offset}&sort={sort}` | `live-session-bids` | 场次出价记录。 | 本商家或 admin |
| `aieas://live-sessions/{sessionId}/orders?status={status}&payStatus={payStatus}&limit={limit}&offset={offset}` | `live-session-orders` | 场次交易订单。 | 本商家或 admin |
| `aieas://live-sessions/{sessionId}/settlement-summary` | `live-session-settlement-summary` | 场次成交情况汇总。 | 本商家或 admin |
| `aieas://orders/{orderId}` | `order` | 订单详情。 | 买家本人、卖家本人或 admin |
| `aieas://orders?winnerId={winnerId}&sellerId={sellerId}&status={status}&payStatus={payStatus}&limit={limit}&offset={offset}` | `orders-list` | 订单列表。 | 按角色自动收敛 |
| `aieas://risk/events?status={status}&eventType={eventType}&userId={userId}&limit={limit}&offset={offset}` | `risk-events` | 风险事件列表。 | admin |
| `aieas://audit-logs?operatorId={operatorId}&action={action}&limit={limit}&offset={offset}` | `audit-logs` | 审计日志列表。 | admin；merchant 可选仅本人 |

### 6.2 Resource 读取示例

Request：

```json
{
  "jsonrpc": "2.0",
  "id": 10,
  "method": "resources/read",
  "params": {
    "uri": "aieas://live-sessions/90001/settlement-summary"
  }
}
```

Response text 解码后：

```json
{
  "schemaVersion": "aieas.mcp.readonly.v1",
  "traceId": "trc_...",
  "data": {
    "session": {
      "id": 90001,
      "liveRoomId": 30001,
      "merchantId": "u_2001",
      "title": "春拍专场",
      "status": "ENDED",
      "openedAt": "2026-05-26T10:00:00Z",
      "closedAt": "2026-05-26T12:00:00Z",
      "lotsTotal": 12,
      "lotsSold": 9,
      "lotsUnsold": 3,
      "bidCount": 184,
      "gmvCent": 9820000,
      "viewerPeak": 532,
      "viewerTotal": 3100
    },
    "settlement": {
      "soldCount": 9,
      "unsoldCount": 3,
      "totalDealCent": 9820000,
      "paidOrderCount": 7,
      "unpaidOrderCount": 2,
      "topDeal": {
        "auctionId": 100082,
        "orderId": 70012,
        "winnerId": "u_1001",
        "dealPrice": 2300000,
        "payStatus": "PAID"
      }
    }
  }
}
```

## 7. Read-only Tools 设计

Resources 是主接口；Tools 是便于模型按参数查询的只读函数。所有 tool 名称使用 snake_case，并加 `read_` 前缀强调无副作用。

### 7.1 Tool 列表

| Tool | 说明 | 主要输入 |
|---|---|---|
| `read_user` | 读取用户安全信息。 | `userId` |
| `read_users` | 查询用户列表。 | `role,status,keyword,limit,offset` |
| `read_merchant` | 读取商家资料和概览。 | `merchantId` |
| `read_items` | 查询商品列表。 | `sellerId,status,category,limit,offset` |
| `read_item` | 读取商品详情。 | `itemId` |
| `read_auction_lots` | 查询拍品列表。 | `sellerId,status,itemId,liveRoomId,limit,offset` |
| `read_auction_lot` | 读取拍品详情。 | `auctionId` |
| `read_auction_state` | 读取拍品实时状态。 | `auctionId` |
| `read_live_rooms` | 查询直播间列表。 | `merchantId,status,limit,offset` |
| `read_live_room` | 读取直播间详情。 | `roomId` |
| `read_live_room_lots` | 读取直播间挂载拍品。 | `roomId,limit,offset` |
| `read_live_room_stats` | 读取直播间当前统计。 | `roomId` |
| `read_live_sessions` | 查询直播场次。 | `merchantId,roomId,status,limit,offset` |
| `read_live_session` | 读取直播场次详情。 | `sessionId` |
| `read_live_session_lots` | 读取场次内拍品。 | `sessionId,limit,offset` |
| `read_live_session_bids` | 读取场次出价记录。 | `sessionId,sort,limit,offset` |
| `read_live_session_orders` | 读取场次交易订单。 | `sessionId,status,payStatus,limit,offset` |
| `read_live_session_settlement` | 读取场次成交汇总。 | `sessionId` |
| `read_orders` | 查询订单列表。 | `winnerId,sellerId,status,payStatus,limit,offset` |
| `read_order` | 读取订单详情。 | `orderId` |
| `read_risk_events` | 查询风险事件。 | `status,eventType,userId,limit,offset` |
| `read_audit_logs` | 查询审计日志。 | `operatorId,action,limit,offset` |

### 7.2 Tool Schema 示例

`read_live_session_bids`：

```json
{
  "name": "read_live_session_bids",
  "description": "读取某场直播的出价记录。只读，无副作用。merchant 只能读取自己的场次，admin 可读取全部。",
  "inputSchema": {
    "type": "object",
    "properties": {
      "sessionId": {
        "type": "integer",
        "minimum": 1,
        "description": "直播场次 ID"
      },
      "sort": {
        "type": "string",
        "enum": ["timeDesc", "timeAsc", "priceDesc"],
        "default": "timeDesc"
      },
      "limit": {
        "type": "integer",
        "minimum": 1,
        "maximum": 100,
        "default": 20
      },
      "offset": {
        "type": "integer",
        "minimum": 0,
        "default": 0
      }
    },
    "required": ["sessionId"]
  }
}
```

调用示例：

```json
{
  "jsonrpc": "2.0",
  "id": 20,
  "method": "tools/call",
  "params": {
    "name": "read_live_session_bids",
    "arguments": {
      "sessionId": 90001,
      "sort": "timeDesc",
      "limit": 50,
      "offset": 0
    }
  }
}
```

返回 text 解码后：

```json
{
  "schemaVersion": "aieas.mcp.readonly.v1",
  "traceId": "trc_...",
  "data": {
    "items": [
      {
        "id": 801,
        "requestId": "bid-001",
        "auctionId": 100082,
        "liveSessionId": 90001,
        "bidderId": "u_1001",
        "bidPrice": 2300000,
        "bidTsMs": 1779789600123,
        "source": "ws",
        "riskResult": "ALLOW",
        "createdAt": "2026-05-26T10:00:00.123Z"
      }
    ],
    "page": {
      "limit": 50,
      "offset": 0,
      "hasMore": false
    }
  }
}
```

`read_live_session_settlement`：

```json
{
  "name": "read_live_session_settlement",
  "description": "读取某场直播的成交汇总，包括成交拍品数、流拍数、GMV、订单支付状态和最高成交。",
  "inputSchema": {
    "type": "object",
    "properties": {
      "sessionId": {
        "type": "integer",
        "minimum": 1
      }
    },
    "required": ["sessionId"]
  }
}
```

## 8. DTO 建议

不要直接把领域对象作为 MCP 协议承诺。建议在 `internal/transport/mcp/dto.go` 定义稳定 DTO。

### 8.1 MerchantProfile

```json
{
  "merchant": {
    "id": "u_2001",
    "nickname": "商家001",
    "role": "merchant",
    "status": "ACTIVE"
  },
  "liveRoom": {
    "id": 30001,
    "title": "春拍专场",
    "status": "LIVE",
    "activeAuctionId": 100082
  },
  "summary": {
    "liveSessionCount": 18,
    "soldLotCount": 126,
    "gmvCent": 38600000
  }
}
```

### 8.2 LiveSessionDetail

```json
{
  "session": {},
  "room": {},
  "merchant": {},
  "stats": {
    "lotsTotal": 12,
    "lotsSold": 9,
    "lotsUnsold": 3,
    "bidCount": 184,
    "gmvCent": 9820000,
    "viewerPeak": 532,
    "viewerTotal": 3100
  }
}
```

### 8.3 SettlementSummary

```json
{
  "sessionId": 90001,
  "soldCount": 9,
  "unsoldCount": 3,
  "totalDealCent": 9820000,
  "paidOrderCount": 7,
  "unpaidOrderCount": 2,
  "timeoutOrderCount": 0,
  "cancelledOrderCount": 0,
  "topDeal": {
    "auctionId": 100082,
    "orderId": 70012,
    "winnerId": "u_1001",
    "dealPrice": 2300000,
    "payStatus": "PAID"
  }
}
```

## 9. 错误映射

MCP 传输层使用 JSON-RPC error。业务错误放入 `error.data`，便于上游做可观测性和用户提示。

| 后端错误 | JSON-RPC code | HTTP 状态建议 | message |
|---|---:|---:|---|
| 参数错误 | `-32602` | 400 | `invalid params` |
| 未登录/Token 无效 | `-32001` | 401 | `unauthorized` |
| 无权限 | `-32003` | 403 | `forbidden` |
| 资源不存在 | `-32004` | 404 | `not found` |
| 状态冲突 | `-32009` | 409 | `conflict` |
| 内部错误 | `-32603` | 500 | `internal error` |

示例：

```json
{
  "jsonrpc": "2.0",
  "id": 20,
  "error": {
    "code": -32003,
    "message": "forbidden",
    "data": {
      "traceId": "trc_...",
      "businessCode": 10003,
      "detail": "actor cannot read this live session"
    }
  }
}
```

## 10. 上游接入流程

1. 通过现有 REST 登录接口获取 `accessToken`。

```http
POST /api/v1/auth/login
Content-Type: application/json
```

2. 初始化 MCP。

```http
POST /mcp
Authorization: Bearer <accessToken>
Content-Type: application/json
Accept: application/json, text/event-stream
MCP-Protocol-Version: 2025-06-18
```

3. 调用 `resources/templates/list` 发现资源模板，或调用 `tools/list` 发现只读工具。
4. 使用 `resources/read` 读取 URI，或使用 `tools/call` 按结构化参数查询。
5. 根据 `traceId` 对齐服务端日志、审计日志和上游调用日志。

## 11. 实现顺序建议

1. 新增 `internal/transport/mcp` 包骨架和 `/mcp` endpoint。
2. 接入 `initialize`、`resources/templates/list`、`resources/read`、`tools/list`。
3. 先实现核心只读资源：
   - `live-rooms`
   - `auction-lots`
   - `auction-state`
   - `live-sessions`
   - `live-session-lots`
   - `live-session-bids`
   - `live-session-orders`
   - `live-session-settlement-summary`
4. 补齐 `users`、`merchants`、`items`、`orders`。
5. 补齐 admin-only 的 `risk-events`、`audit-logs`。
6. 增加 `go test ./...` 覆盖：
   - MCP 鉴权失败。
   - merchant 越权读取其他商家场次。
   - admin 读取任意场次。
   - buyer 无法读取场次出价/订单。
   - resource URI 参数错误。
   - tool schema 参数错误。

## 12. 第一阶段不做

- 不实现 `tools/call` 写操作。
- 不开放 `place_bid`、`hammer_auction`、`pay_order`。
- 不开放数据库任意 SQL 查询。
- 不返回用户密码哈希、token、完整账号敏感字段。
- 不把 MCP endpoint 放到 `/api/v1` REST 分组内。
- 不让 MCP handler 直接访问 repository 或 Redis。

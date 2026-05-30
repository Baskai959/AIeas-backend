# 直播控制 MCP 对接文档

本文档说明 `aieas_backend` 面向 Agent/上游系统暴露的直播控制 MCP 接口。该 MCP 只包含商家直播中的控制能力：获取当前直播间控制台上下文、拍品上架、拍品下架、开始讲解、落槌、下播。

只读数据查询使用独立 MCP：`POST /mcp/read`。直播控制使用本文档的独立 MCP：`POST /mcp/control`。

## 1. 服务信息

- MCP Endpoint：`POST /mcp/control`
- Transport：Streamable HTTP，当前实现使用 JSON-RPC 2.0 请求/响应。
- 鉴权：请求头 `X-API-Key: <mcp.control.apiKey>`。
- 不使用用户登录 `accessToken`。
- 服务端调用身份来自配置项 `mcp.control.actorID` / `mcp.control.actorRole`。
- Server name：`aieas-control-mcp`
- Server version：`1.2.0`
- 业务响应 schema：`aieas.mcp.control.v1`

配置示例：

```yaml
mcp:
  read:
    apiKey: "replace-with-read-secret"
    actorID: "u_9001"
    actorRole: "admin"
  control:
    apiKey: "replace-with-control-secret"
    actorID: "u_9001"
    actorRole: "admin"
```

环境变量可覆盖：

- `MCP_CONTROL_API_KEY`
- `MCP_CONTROL_ACTOR_ID`
- `MCP_CONTROL_ACTOR_ROLE`

权限规则：

- `actorRole=merchant` 时，只能读取和操作自己的商家直播间。
- `actorRole=admin` 时，可以读取和操作任意商家直播间。
- `actorRole=buyer` 不允许调用商家控制类工具。

## 2. 调用格式

初始化：

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "initialize",
  "params": {
    "protocolVersion": "2025-06-18",
    "capabilities": {},
    "clientInfo": {
      "name": "merchant-agent",
      "version": "1.0.0"
    }
  }
}
```

获取工具列表：

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "tools/list",
  "params": {}
}
```

调用工具：

```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "method": "tools/call",
  "params": {
    "name": "get_merchant_live_control_context",
    "arguments": {
      "merchantId": "u_2001"
    }
  }
}
```

业务成功响应的 `content[0].text` 是 JSON 字符串，结构固定：

```json
{
  "schemaVersion": "aieas.mcp.control.v1",
  "traceId": "req_xxx",
  "data": {}
}
```

## 3. 直播控制工具

### 3.1 get_merchant_live_control_context

获取某个商家当前直播间的控制台上下文。请求参数只需要商家 ID。

参数：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `merchantId` | string | 是 | 商家用户 ID |

请求示例：

```json
{
  "jsonrpc": "2.0",
  "id": 10,
  "method": "tools/call",
  "params": {
    "name": "get_merchant_live_control_context",
    "arguments": {
      "merchantId": "u_2001"
    }
  }
}
```

返回 `data` 结构：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `merchantId` | string | 商家用户 ID |
| `room` | object | 当前商家的直播间信息 |
| `session` | object/null | 当前直播场次；没有场次时为空 |
| `stats` | object | 直播间统计信息；`stats.online` 为当前在线人数，`stats.currentPrice` 为当前价 |
| `currentAuctionState` | object/null | 当前讲解拍品的实时拍卖状态；无讲解中拍品时为空 |
| `lots.explainingLot` | object/null | 当前讲解中的拍品 |
| `lots.roomLots` | array | 当前直播间已挂载的拍品 |
| `lots.sessionLots` | array | 当前直播场次出现过的拍品 |
| `lots.soldLots` | array | 当前场次已成交拍品 |
| `lots.unsoldLots` | array | 当前场次已流拍拍品 |
| `lots.upcomingLots` | array | 当前直播间待讲解拍品 |
| `lots.candidateLots` | array | 商家可上架到直播间的候选拍品 |

响应示例：

```json
{
  "schemaVersion": "aieas.mcp.control.v1",
  "traceId": "req_7f4c",
  "data": {
    "merchantId": "u_2001",
    "room": {
      "id": 80001,
      "merchantId": "u_2001",
      "status": "LIVE",
      "activeAuctionId": 91001
    },
    "session": {
      "id": 70001,
      "liveRoomId": 80001,
      "merchantId": "u_2001",
      "status": "LIVE"
    },
    "stats": {
      "roomId": 80001,
      "online": 128,
      "lotsTotal": 6,
      "activeAuctionId": 91001,
      "currentBidCount": 36,
      "currentRemainSeconds": 82,
      "currentPrice": 120000
    },
    "currentAuctionState": {
      "auctionId": 91001,
      "status": "RUNNING",
      "currentPrice": 120000,
      "leaderBidderId": "u_1001",
      "startTime": "2026-05-28T10:20:00Z",
      "endTime": "2026-05-28T10:30:00Z",
      "remainSeconds": 82,
      "lastBidTsMs": 1780000000000,
      "extendCount": 1,
      "version": 1780000000001,
      "source": "redis"
    },
    "lots": {
      "explainingLot": {
        "auctionId": 91001,
        "sellerId": "u_2001",
        "status": "RUNNING"
      },
      "roomLots": [],
      "sessionLots": [],
      "soldLots": [],
      "unsoldLots": [],
      "upcomingLots": [],
      "candidateLots": []
    }
  }
}
```

### 3.2 operate_live_session_lot

模拟商家在直播中的拍品操作。参数为直播场次 ID、拍品 ID 和操作动作。

参数：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `liveSessionId` | integer | 是 | 当前直播场次 ID，必须是该直播间当前 `LIVE` 场次 |
| `auctionId` | integer | 是 | 拍品 ID |
| `action` | string | 是 | 操作动作，见下表 |
| `durationSec` | integer | 否 | `startExplain` 时可指定讲解/拍卖时长，单位秒 |
| `force` | boolean | 否 | `hammer`/`endLive` 时是否强制结束；MCP 默认 `true` |
| `requestId` | string | 否 | `hammer` 的可选幂等请求 ID；不传时服务端生成 |

`action` 支持：

| action | 说明 | 主要业务效果 |
| --- | --- | --- |
| `onShelf` | 上架拍品 | 将商家的候选拍品挂载到当前直播间 |
| `offShelf` | 下架拍品 | 将未讲解或未成交拍品从当前直播间移除 |
| `startExplain` | 开始讲解 | 激活直播间中的拍品，成为当前讲解/拍卖拍品 |
| `hammer` | 落槌 | 结束当前讲解拍品；有最高有效出价则成交，否则流拍 |
| `endLive` | 下播 | 结束当前直播间活跃拍品并关闭当前直播状态 |

上架示例：

```json
{
  "jsonrpc": "2.0",
  "id": 20,
  "method": "tools/call",
  "params": {
    "name": "operate_live_session_lot",
    "arguments": {
      "liveSessionId": 70001,
      "auctionId": 91001,
      "action": "onShelf"
    }
  }
}
```

开始讲解示例：

```json
{
  "jsonrpc": "2.0",
  "id": 21,
  "method": "tools/call",
  "params": {
    "name": "operate_live_session_lot",
    "arguments": {
      "liveSessionId": 70001,
      "auctionId": 91001,
      "action": "startExplain",
      "durationSec": 600
    }
  }
}
```

落槌示例：

```json
{
  "jsonrpc": "2.0",
  "id": 22,
  "method": "tools/call",
  "params": {
    "name": "operate_live_session_lot",
    "arguments": {
      "liveSessionId": 70001,
      "auctionId": 91001,
      "action": "hammer",
      "force": true,
      "requestId": "merchant-agent-70001-91001-hammer"
    }
  }
}
```

下架示例：

```json
{
  "jsonrpc": "2.0",
  "id": 23,
  "method": "tools/call",
  "params": {
    "name": "operate_live_session_lot",
    "arguments": {
      "liveSessionId": 70001,
      "auctionId": 91002,
      "action": "offShelf"
    }
  }
}
```

下播示例：

```json
{
  "jsonrpc": "2.0",
  "id": 24,
  "method": "tools/call",
  "params": {
    "name": "operate_live_session_lot",
    "arguments": {
      "liveSessionId": 70001,
      "auctionId": 91001,
      "action": "endLive",
      "force": true
    }
  }
}
```

返回 `data` 结构：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `action` | string | 归一化后的操作动作 |
| `liveSessionId` | integer | 直播场次 ID |
| `auctionId` | integer | 拍品 ID |
| `lot` | object/null | 操作后的拍品信息 |
| `room` | object/null | `endLive` 等操作后的直播间信息 |
| `hammerResult` | object/null | `hammer` 的落槌结果 |
| `order` | object/null | `hammer` 成交时生成的订单 |
| `removed` | boolean | `offShelf` 成功时为 `true` |
| `context` | object | 操作完成后的最新直播控制台上下文，结构同 `get_merchant_live_control_context` |

响应示例：

```json
{
  "schemaVersion": "aieas.mcp.control.v1",
  "traceId": "req_8c10",
  "data": {
    "action": "startExplain",
    "liveSessionId": 70001,
    "auctionId": 91001,
    "lot": {
      "auctionId": 91001,
      "sellerId": "u_2001",
      "status": "RUNNING"
    },
    "context": {
      "merchantId": "u_2001",
      "room": {
        "id": 80001,
        "merchantId": "u_2001",
        "status": "LIVE",
        "activeAuctionId": 91001
      },
      "session": {
        "id": 70001,
        "liveRoomId": 80001,
        "merchantId": "u_2001",
        "status": "LIVE"
      },
      "stats": {
        "roomId": 80001,
        "online": 0,
        "lotsTotal": 1,
        "activeAuctionId": 91001,
        "currentBidCount": 0,
        "currentRemainSeconds": 600,
        "currentPrice": 1000
      },
      "currentAuctionState": {
        "auctionId": 91001,
        "status": "RUNNING",
        "currentPrice": 1000,
        "startTime": "2026-05-28T10:20:00Z",
        "endTime": "2026-05-28T10:30:00Z",
        "remainSeconds": 600,
        "version": 1780000000001,
        "source": "redis"
      },
      "lots": {
        "explainingLot": {
          "auctionId": 91001,
          "sellerId": "u_2001",
          "status": "RUNNING"
        },
        "roomLots": [],
        "sessionLots": [],
        "soldLots": [],
        "unsoldLots": [],
        "upcomingLots": [],
        "candidateLots": []
      }
    }
  }
}
```

## 4. 业务约束

- `operate_live_session_lot.liveSessionId` 必须指向当前直播间正在直播的场次。
- 当前在线人数在 `stats.online`；当前最高价和最高价用户在 `currentAuctionState.currentPrice` / `currentAuctionState.leaderBidderId`。
- 商家角色只能操作自己的直播场次和拍品。
- `startExplain` 只能激活已挂载到该直播间且状态允许开始的拍品。
- `hammer` 要求传入的 `auctionId` 是当前正在讲解的拍品。
- `offShelf` 不用于结束正在讲解中的拍品；正在讲解的拍品应使用 `hammer` 或 `endLive`。
- `endLive` 会走直播间已有下播流程；如有活跃拍品，会由业务服务处理关闭。

## 5. 错误响应

MCP 传输层使用 JSON-RPC error：

```json
{
  "jsonrpc": "2.0",
  "id": 22,
  "error": {
    "code": -32602,
    "message": "invalid params",
    "data": {
      "traceId": "req_xxx",
      "type": "INVALID_ARGUMENT"
    }
  }
}
```

常见错误：

| type | 场景 |
| --- | --- |
| `INVALID_ARGUMENT` | 参数缺失、ID 为 0、action 不支持 |
| `FORBIDDEN` | MCP 配置身份无权访问该商家 |
| `NOT_FOUND` | 商家直播间、场次或拍品不存在 |
| `INVALID_STATE` | 场次不是当前直播中场次，或操作与拍品/直播间当前状态冲突 |
| `UNAUTHORIZED` | `X-API-Key` 缺失或错误 |

## 6. Agent 调用建议

- 操作前先调用 `get_merchant_live_control_context`，确认当前场次、正在讲解拍品和可上架候选拍品。
- 每次 `operate_live_session_lot` 成功后直接使用返回的 `context` 刷新 Agent 视图，不需要额外再查一次。
- `hammer` 建议传入稳定 `requestId`，便于上游重试时识别同一次落槌意图。
- 上游不要直接根据自然语言拼接内部 ID；应先读上下文，再从 `candidateLots`、`roomLots`、`explainingLot` 中选择明确的 `auctionId`。

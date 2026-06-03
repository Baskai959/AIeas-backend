# AI电商拍卖系统 API 文档

本文按当前代码实现整理，来源以 `internal/app/server.go` 的实际注册路由和 `internal/transport/http` handlers 为准。

机器可导入版本：

- OpenAPI 3.0 JSON：[当前项目接口.openapi.json](./当前项目接口.openapi.json)
- WebSocket 用户端交互协议：[../WebSocket用户端交互协议.md](../WebSocket用户端交互协议.md)

重要变更：

- 当前没有 `/api/v1/items` 商品 REST 路由；拍品已承载标题、图片、描述、价格等展示字段。
- 当前直播入口使用 `live-sessions`，不是旧版 `live-rooms`。
- 当前 WebSocket 实际路由是 `/ws/auctions/:auction_id` 与 `/ws/live-sessions/:session_id`。
- 拍品创建、更新、实时状态和 WebSocket 首帧快照均支持传递拍卖规则：`startPrice`、`capPrice`、`incrementRule`。

## 1. 基础约定

| 项 | 约定 |
| --- | --- |
| REST 基础路径 | `/api/v1` |
| WebSocket 路径 | 根路径下 `/ws/...`，不带 `/api/v1` |
| MCP 路径 | 根路径下 `/mcp/read`、`/mcp/control` |
| REST 数据格式 | `application/json`；上传接口为 `multipart/form-data` |
| 金额单位 | 分，类型为 `int64` |
| 实体 ID | 拍品、场次、订单等为 `uint64`；用户 ID 为字符串 |
| 时间格式 | REST JSON 多数为 RFC3339；明确标注 `*Ms` 的字段为 Unix 毫秒 |

REST 成功和错误统一响应：

```json
{
  "code": 0,
  "message": "success",
  "data": {},
  "trace_id": "trc_..."
}
```

## 2. 鉴权与幂等

JWT 鉴权：

```http
Authorization: Bearer <accessToken>
```

浏览器 WebSocket 不能设置 Header 时，可使用：

```text
/ws/auctions/10001?token=<accessToken>
```

回调鉴权：

```http
X-Callback-Key: <callbackKey>
```

MCP 鉴权：

```http
X-API-Key: <mcpApiKey>
```

状态变更接口通过 `Idempotency-Key` 支持幂等。当前代码中挂了 `WithIdempotency` 的接口应携带该 Header，尤其是开拍、取消、落槌、支付、直播场次写操作和管理员写操作。

```http
Idempotency-Key: <stable-request-id>
```

## 3. 当前 REST API 清单

### 3.1 Auth

| Method | Path | 说明 | 鉴权 |
| --- | --- | --- | --- |
| `POST` | `/api/v1/auth/login` | 用户/商家登录 | 无 |
| `POST` | `/api/v1/admin/auth/login` | 管理员登录 | 无 |
| `POST` | `/api/v1/auth/refresh` | 刷新访问令牌 | 无 |
| `GET` | `/api/v1/auth/me` | 当前用户信息 | JWT |
| `POST` | `/api/v1/auth/logout` | 退出登录 | JWT |

### 3.2 Auction

| Method | Path | 说明 | 鉴权 |
| --- | --- | --- | --- |
| `GET` | `/api/v1/images/*key` | 图片代理读取 | 无 |
| `POST` | `/api/v1/auctions/audit/callback` | 拍品 AI 审核回调 | Callback Key |
| `GET` | `/api/v1/auctions/:id/state` | 拍品实时状态 | JWT |
| `POST` | `/api/v1/auctions/:id/enroll` | 买家报名并冻结保证金 | JWT + buyer |
| `POST` | `/api/v1/auctions/description/optimize` | AI 优化拍品描述，`multipart/form-data` | JWT + merchant/admin |
| `POST` | `/api/v1/auctions/images` | 上传拍品图片，`multipart/form-data` | JWT + merchant/admin |
| `POST` | `/api/v1/auctions` | 创建拍品 | JWT + merchant/admin |
| `GET` | `/api/v1/auctions` | 拍品列表 | JWT + merchant/admin |
| `GET` | `/api/v1/auctions/:id` | 拍品详情 | JWT + merchant/admin |
| `PATCH` | `/api/v1/auctions/:id` | 更新拍品 | JWT + merchant/admin |
| `DELETE` | `/api/v1/auctions/:id` | 删除拍品 | JWT + merchant/admin |
| `POST` | `/api/v1/auctions/:id/start` | 开拍 | JWT + merchant/admin + 幂等 |
| `POST` | `/api/v1/auctions/:id/cancel` | 取消拍品 | JWT + merchant/admin + 幂等 |
| `POST` | `/api/v1/auctions/:id/hammer` | 手动落槌 | JWT + merchant/admin + 幂等 |

常用查询参数：

| 参数 | 说明 |
| --- | --- |
| `sellerId` | 按商家筛选 |
| `status` | 拍品状态 |
| `category` | 类目 |
| `keyword` | 关键词 |
| `liveSessionId` | 所属直播场次 |
| `limit` / `offset` | 分页 |

拍品拍卖规则字段：

| 字段 | 出现位置 | 类型 | 含义 |
| --- | --- | --- | --- |
| `startPrice` | 创建、更新、详情、列表、实时状态、WebSocket 快照 | int64 | 起拍价，单位为分 |
| `reservePrice` | 创建、更新、详情、列表、规则快照 | int64 | 保留价，单位为分 |
| `capPrice` | 创建、更新、详情、列表、实时状态、WebSocket 快照 | int64 | 封顶价，单位为分；`0` 表示不设置封顶价 |
| `incrementRule` | 创建、更新、详情、列表、实时状态、WebSocket 快照 | object | 加价规则；创建时不传则默认固定加价 `100` 分，单次最多 `10` 步 |
| `antiSnipingSec` | 创建、更新、详情、列表、规则快照 | int | 反狙击触发窗口，单位秒；不传或小于等于 0 时默认 `15` |
| `antiExtendSec` | 创建、更新、详情、列表、规则快照 | int | 单次延时时长，单位秒；不传或小于等于 0 时默认 `30` |
| `antiExtendMode` | 创建、更新、详情、列表、规则快照 | string | 延时模式：`ADD` 表示在原结束时间上累加，`RESET` 表示重置为当前时间加延时 |
| `durationSec` | 创建、更新、详情、列表、规则快照 | int | 拍卖持续秒数 |
| `ruleSnapshot` | 详情、列表 | object | 创建或更新时固化的展示与拍卖规则快照，用于审核和追溯 |

`incrementRule` 支持固定加价：

```json
{
  "type": "fixed",
  "amount": 100,
  "maxBidSteps": 10
}
```

也支持阶梯加价：

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

校验规则：金额均为分；`amount` 和 `maxBidSteps` 必须大于 0；阶梯最多 50 段，第一段 `min` 必须为 0，相邻阶梯必须连续，`max` 为不包含上界，最后一段必须省略 `max`。`capPrice > 0` 时必须大于 `startPrice`，且 `reservePrice` 不能大于 `capPrice`。

`GET /api/v1/auctions/:id/state` 返回的实时状态会带 `startPrice`、`capPrice`、`incrementRule`，前端应与 WebSocket `room.snapshot` 使用同一套字段初始化出价控件。

拍品 AI 审核回调兼容多种字段名：请求 ID 可用 `requestId` 或 `request_id`；审核结论可用 `auditResult`、`audit_result`、`decision`、`conclusion`、`isApproved`、`is_approved` 或 `success` 表达；拒绝原因和风险标签同时兼容 camelCase 与 snake_case。

### 3.3 LiveSession

| Method | Path | 说明 | 鉴权 |
| --- | --- | --- | --- |
| `GET` | `/api/v1/live-sessions` | 直播场次列表 | JWT |
| `GET` | `/api/v1/live-sessions/:id` | 直播场次详情 | JWT |
| `GET` | `/api/v1/live-sessions/:id/lots` | 场次拍品列表 | JWT |
| `GET` | `/api/v1/live-sessions/:id/bids` | 场次出价记录 | JWT |
| `GET` | `/api/v1/live-sessions/:id/orders` | 场次订单列表 | JWT |
| `GET` | `/api/v1/live-sessions/:id/stats` | 场次统计 | JWT |
| `GET` | `/api/v1/merchants/:merchantId/live-sessions` | 指定商家的场次列表 | JWT + merchant/admin |
| `POST` | `/api/v1/live-sessions` | 创建场次 | JWT + merchant/admin |
| `PATCH` | `/api/v1/live-sessions/:id` | 更新场次 | JWT + merchant/admin + 幂等 |
| `POST` | `/api/v1/live-sessions/:id/start` | 开播 | JWT + merchant/admin + 幂等 |
| `POST` | `/api/v1/live-sessions/:id/end` | 下播 | JWT + merchant/admin + 幂等 |
| `POST` | `/api/v1/live-sessions/:id/lots` | 挂载拍品 | JWT + merchant/admin + 幂等 |
| `DELETE` | `/api/v1/live-sessions/:id/lots/:auctionId` | 移除拍品 | JWT + merchant/admin + 幂等 |
| `POST` | `/api/v1/live-sessions/:id/activate` | 激活当前讲解/拍卖拍品 | JWT + merchant/admin + 幂等 |
| `POST` | `/api/v1/live-sessions/:id/deactivate` | 取消当前讲解/拍卖拍品 | JWT + merchant/admin + 幂等 |
| `POST` | `/api/v1/live-sessions/:id/cover` | 上传场次封面，`multipart/form-data` | JWT + merchant/admin + 幂等 |
| `GET` | `/api/v1/live-sessions/:id/agent-hook` | 读取 Agent Hook 开关 | JWT + merchant/admin |
| `PATCH` | `/api/v1/live-sessions/:id/agent-hook` | 更新 Agent Hook 开关 | JWT + merchant/admin + 幂等 |

### 3.4 LiveAnalysis

| Method | Path | 说明 | 鉴权 |
| --- | --- | --- | --- |
| `POST` | `/api/v1/live-analysis/reports` | 创建直播总结报告任务 | JWT + merchant/admin + 幂等 |
| `GET` | `/api/v1/live-analysis/reports/:liveSessionId` | 查询直播总结报告 | JWT + merchant/admin |
| `POST` | `/api/v1/live-analysis/callback` | 直播总结报告回调 | Callback Key |

### 3.5 AIAssistant

| Method | Path | 说明 | 鉴权 |
| --- | --- | --- | --- |
| `GET` | `/api/v1/ai-assistant/permission` | 读取商家 AI 助手权限 | JWT + merchant/admin |
| `PATCH` | `/api/v1/ai-assistant/permission` | 更新商家 AI 助手权限 | JWT + merchant/admin + 幂等 |
| `POST` | `/api/v1/ai-assistant/approvals/:requestId/decision` | 提交 AI 助手审批结果 | JWT + merchant/admin + 幂等 |

权限枚举：

| 值 | 含义 |
| --- | --- |
| `ASK` | 每次控制操作前询问商家 |
| `ALLOW` | 自动允许 |
| `DENY` | 自动拒绝 |

### 3.6 Order

| Method | Path | 说明 | 鉴权 |
| --- | --- | --- | --- |
| `GET` | `/api/v1/orders` | 订单列表 | JWT |
| `GET` | `/api/v1/orders/mine` | 我的订单 | JWT |
| `GET` | `/api/v1/orders/:id` | 订单详情 | JWT |
| `POST` | `/api/v1/orders/:id/pay` | 支付订单 | JWT + 幂等 |

常用查询参数：`winnerId`、`sellerId`、`status`、`payStatus`、`limit`、`offset`。

### 3.7 Admin

| Method | Path | 说明 | 鉴权 |
| --- | --- | --- | --- |
| `GET` | `/api/v1/audit-logs` | 商家/管理员查看自己的审计日志 | JWT + merchant/admin |
| `GET` | `/api/v1/admin/auctions` | 管理员拍品列表 | JWT + admin |
| `POST` | `/api/v1/admin/auctions/:id/audit` | 管理员审核拍品 | JWT + admin + 幂等 |
| `POST` | `/api/v1/admin/auctions/:id/cancel` | 管理员取消拍品 | JWT + admin + 幂等 |
| `POST` | `/api/v1/admin/auctions/:id/close` | 管理员关闭拍品 | JWT + admin + 幂等 |
| `GET` | `/api/v1/admin/users` | 管理员用户列表 | JWT + admin |
| `PATCH` | `/api/v1/admin/users/:id` | 更新用户状态 | JWT + admin + 幂等 |
| `POST` | `/api/v1/admin/blacklist` | 加入黑名单 | JWT + admin + 幂等 |
| `DELETE` | `/api/v1/admin/blacklist/:user_id` | 移除黑名单 | JWT + admin + 幂等 |
| `GET` | `/api/v1/admin/blacklist` | 黑名单列表 | JWT + admin |
| `GET` | `/api/v1/admin/risk/blacklist-strategy` | 读取黑名单策略 | JWT + admin |
| `PUT` | `/api/v1/admin/risk/blacklist-strategy` | 更新黑名单策略 | JWT + admin + 幂等 |
| `GET` | `/api/v1/admin/feature-flags/:key` | 读取功能开关 | JWT + admin |
| `PUT` | `/api/v1/admin/feature-flags/:key` | 更新功能开关 | JWT + admin + 幂等 |
| `GET` | `/api/v1/admin/orders` | 管理员订单列表 | JWT + admin |
| `GET` | `/api/v1/admin/dashboard/metrics` | 管理员业务看板指标 | JWT + admin |
| `GET` | `/api/v1/admin/audit-logs` | 管理员审计日志 | JWT + admin |
| `GET` | `/api/v1/admin/risk/events` | 管理员风控事件列表 | JWT + admin |
| `PATCH` | `/api/v1/admin/risk/events/:id` | 处理风控事件 | JWT + admin + 幂等 |

## 4. WebSocket API

| Method | Path | 说明 | 鉴权 |
| --- | --- | --- | --- |
| `GET` | `/ws/auctions/:auction_id` | 订阅拍品实时事件 | JWT Header 或 `token` query |
| `GET` | `/ws/live-sessions/:session_id` | 订阅直播场次实时事件 | JWT Header 或 `token` query |

连接参数：

| 参数 | 说明 |
| --- | --- |
| `token` | 浏览器 WebSocket 鉴权备用方式 |
| `lastSeq` | 断线重连补偿游标 |

消息格式和字段含义见：[../WebSocket用户端交互协议.md](../WebSocket用户端交互协议.md)。

连接建立后的 `room.snapshot` 会下发当前拍品的 `startPrice`、`capPrice`、`incrementRule`；开拍广播 `auction.started.payload.state` 也包含同一套规则字段。出价失败时，`bid.ack.payload.reason` 可能返回 `PRICE_STEP_MISMATCH`、`ABOVE_MAX_BID_STEPS`、`ABOVE_CAP_PRICE` 等规则校验原因。

当前用户端常用下行事件包括：

`room.snapshot`、`room.online`、`room.snapshot_required`、`bid.ack`、`bid.accepted`、`bid.rejected`、`ranking.updated`、`timer.extended`、`timer.tick`、`auction.started`、`auction.closed`、`live_session.ended`、`live.voice_broadcast`、`ai.assistant.switch`、`ai.assistant.broadcast`、`ai.assistant.status`、`ai.assistant.permission_request`。

## 5. MCP API

MCP 使用 JSON-RPC 2.0，鉴权 Header 为 `X-API-Key`。

| Method | Path | 说明 |
| --- | --- | --- |
| `POST` | `/mcp/read` | 只读 MCP JSON-RPC |
| `GET` | `/mcp/read` | 探测，当前返回 405 |
| `POST` | `/mcp/control` | 控制 MCP JSON-RPC |
| `GET` | `/mcp/control` | 探测，当前返回 405 |

请求示例：

```json
{
  "jsonrpc": "2.0",
  "id": "1",
  "method": "tools/list",
  "params": {}
}
```

## 6. 运维端点

| Method | Path | 说明 | 鉴权 |
| --- | --- | --- | --- |
| `GET` | `/ping` | 简单 ping | 无 |
| `GET` | `/healthz` | 存活检查 | 无 |
| `GET` | `/readyz` | MySQL、Redis、脚本、Kafka 等就绪检查 | 无 |
| `GET` | `/metrics` | Prometheus 指标 | 可选 metrics token |

`/healthz`、`/readyz`、`/metrics` 的实际路径可由 observability 配置调整；上表是默认路径。

## 7. 主要枚举

| 枚举 | 值 |
| --- | --- |
| `Role` | `buyer`、`merchant`、`admin` |
| `UserStatus` | `ACTIVE`、`DISABLED` |
| `AuctionStatus` | `DRAFT`、`PENDING_AUDIT`、`AUDIT_REJECTED`、`READY`、`WARMING_UP`、`RUNNING`、`EXTENDED`、`HAMMER_PENDING`、`CLOSED_WON`、`CLOSED_FAILED`、`SETTLED` |
| `AuctionType` | `ENGLISH` |
| `AuctionExtendMode` | `ADD`、`RESET` |
| `LiveSessionStatus` | `DRAFT`、`SCHEDULED`、`LIVE`、`ENDED`、`CANCELLED` |
| `OrderStatus` | `CREATED`、`PAID`、`TIMEOUT`、`CANCELLED` |
| `PayStatus` | `UNPAID`、`PAID`、`REFUNDED` |
| `RiskEventStatus` | `PENDING`、`REVIEWED`、`IGNORED` |
| `MerchantAIPermission` | `ASK`、`ALLOW`、`DENY` |

## 8. OpenAPI 导入说明

将 [当前项目接口.openapi.json](./当前项目接口.openapi.json) 导入 Apifox、Swagger UI、Postman 或其他 OpenAPI 3.0 工具即可查看完整路径、请求体、参数和安全方案。

这份 OpenAPI 文件是当前实现的聚合快照。后续新增单个功能接口时，仍按项目约定新增独立 `docs/API/<功能名>.openapi.json`，不要直接回写旧的聚合主文件。

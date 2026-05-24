# 「实时竞拍大师」3 周 MVP 直播竞拍系统 API 文档

## 1. 文档说明

- 版本号：v1
- API 基础地址：`https://api.example.com/api/v1`（演示环境）
- 协议：HTTP/1.1 + Hertz WebSocket（`github.com/hertz-contrib/websocket`）
- 数据格式：`application/json; charset=utf-8`
- 时间格式：ISO 8601；时间戳字段使用毫秒，例如 `bidTsMs: 1769155200000`
- 金额字段：统一单位「分」，例如 100 元表示为 `10000`
- JSON 字段命名：统一 camelCase，例如 `currentPrice`、`leaderBidderId`、`bidTsMs`

---

## 2. 通用约定

### 2.1 鉴权

除登录、公开列表等明确说明可匿名访问的接口外，均需携带：

```http
Authorization: Bearer <jwt_token>
```

JWT claims：

| claim | 类型 | 含义 |
| --- | --- | --- |
| sub | string | 用户 ID |
| role | string | 角色，取值：`buyer`、`merchant`、`admin` |
| exp | int | 过期时间，Unix 秒级时间戳 |
| iat | int | 签发时间，Unix 秒级时间戳 |

- JWT 有效期：12 小时
- 角色不满足时返回 `403` 与业务错误码 `10003`

### 2.2 幂等

- REST 写操作建议携带请求头：`Idempotency-Key: <uuid>`
- 落锤、支付等关键写操作必须携带 `Idempotency-Key`
- REST 幂等缓存优先使用 Redis，缓存 key 由用户 ID、HTTP method、path、`Idempotency-Key` 组成；缓存 TTL 由 `idempotency.ttl` / `IDEMPOTENCY_TTL` 配置，默认 24h
- WebSocket 出价使用报文字段 `requestId` 做客户端请求幂等与响应关联

### 2.3 通用响应结构

```json
{
  "code": 0,
  "message": "success",
  "data": {},
  "trace_id": "trc_202605230001"
}
```

### 2.4 错误码表

| code | 常量名 | HTTP 状态 | 含义 | 典型场景 |
| ---: | --- | ---: | --- | --- |
| 0 | OK | 200 | 成功 | 请求处理成功 |
| 90001 | INTERNAL_ERROR | 500 | 系统内部错误 | 数据库异常、未知 panic |
| 90002 | SERVICE_UNAVAILABLE | 503 | 服务暂不可用 | 依赖服务不可用 |
| 90003 | TOO_MANY_REQUESTS | 429 | 请求过于频繁 | 超过接口限流阈值 |
| 90004 | IDEMPOTENCY_CONFLICT | 409 | 幂等键冲突 | 同一 `Idempotency-Key` 对应不同请求体 |
| 90005 | TRACE_REQUIRED | 400 | 缺少必要追踪参数 | 网关要求追踪头但未传入 |
| 10001 | AUTH_TOKEN_MISSING | 401 | 缺少访问令牌 | 未传 `Authorization` |
| 10002 | AUTH_TOKEN_INVALID | 401 | 访问令牌无效或过期 | JWT 过期、签名错误 |
| 10003 | AUTH_FORBIDDEN | 403 | 无访问权限 | 普通用户访问 admin 接口 |
| 10004 | AUTH_LOGIN_FAILED | 401 | 登录失败 | 密码错误、账号不存在 |
| 10005 | AUTH_ACCOUNT_DISABLED | 403 | 账号已停用 | 用户被平台禁用 |
| 20001 | PARAM_INVALID | 400 | 参数不合法 | 字段类型错误、枚举非法 |
| 20002 | PARAM_MISSING | 400 | 缺少必填参数 | 未传 `auctionId` |
| 20003 | PARAM_PAGE_INVALID | 400 | 分页参数不合法 | `page_size` 超过上限 |
| 20004 | PARAM_TIME_RANGE_INVALID | 400 | 时间范围不合法 | `startTime` 晚于 `endTime` |
| 30001 | ITEM_NOT_FOUND | 404 | 商品不存在 | 查询不存在的商品 ID |
| 30002 | ITEM_PERMISSION_DENIED | 403 | 无商品操作权限 | 商家修改非本人商品 |
| 30003 | ITEM_STATUS_INVALID | 409 | 商品状态不允许操作 | 已绑定拍品的商品被删除 |
| 40001 | AUCTION_NOT_FOUND | 404 | 拍品不存在 | 查询不存在的拍品 ID |
| 40002 | AUCTION_STATUS_INVALID | 409 | 拍品状态不允许操作 | 非 `DRAFT/READY` 状态修改拍品 |
| 40003 | AUCTION_START_PRICE_INVALID | 400 | 起拍价不合法 | 起拍价小于 0；0 元起拍允许 |
| 40004 | AUCTION_NOT_STARTED | 409 | 拍卖尚未开始 | 未开始时出价 |
| 40005 | AUCTION_CLOSED | 409 | 拍卖已结束 | 已落锤后继续出价 |
| 40006 | AUCTION_ENROLL_REQUIRED | 403 | 未报名或未缴保证金 | 出价前未报名 |
| 50001 | BID_TOO_LOW | 409 | 出价过低 | 出价未达到当前价加最低加价幅度 |
| 50002 | BID_SELF_OVERBID | 409 | 不能连续自我加价 | 领先者再次出价 |
| 50003 | BID_DUPLICATE_REQUEST | 409 | 重复出价请求 | `requestId` 重复提交 |
| 50004 | BID_RATE_LIMITED | 429 | 出价频率受限 | 单用户单房间超过 10 QPS |
| 50005 | BID_RISK_REJECTED | 403 | 出价被风控拒绝 | 黑名单、异常频率、关联账号 |
| 60001 | ORDER_NOT_FOUND | 404 | 订单不存在 | 查询不存在的订单 ID |
| 60002 | ORDER_STATUS_INVALID | 409 | 订单状态不允许操作 | 已支付订单重复支付 |
| 60003 | ORDER_PAYMENT_FAILED | 502 | 支付失败 | 支付渠道返回失败 |
| 70001 | DEPOSIT_REQUIRED | 403 | 需要保证金 | 报名或出价前未冻结保证金 |
| 70002 | DEPOSIT_INSUFFICIENT | 403 | 保证金不足 | 可用余额低于保证金要求 |
| 70003 | DEPOSIT_LEDGER_NOT_FOUND | 404 | 保证金流水不存在 | 查询不存在的流水 |
| 80001 | RISK_USER_BLACKLISTED | 403 | 用户在黑名单中 | 黑名单用户出价或报名 |
| 80002 | RISK_RULE_BLOCKED | 403 | 命中风控规则 | 高频、撞库、恶意哄抬 |
| 80003 | RISK_EVENT_NOT_FOUND | 404 | 风控事件不存在 | 处理不存在的事件 |

错误响应示例：

```json
{
  "code": 50001,
  "message": "出价需不低于当前价加最低加价幅度",
  "data": {
    "currentPrice": 12000,
    "minNextPrice": 13000
  },
  "trace_id": "trc_202605230002"
}
```

### 2.5 分页

请求参数：

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| page | int | 否 | 1 | 页码，从 1 开始 |
| page_size | int | 否 | 20 | 每页数量，最大 100 |

分页响应：

```json
{
  "items": [],
  "total": 100,
  "page": 1,
  "page_size": 20
}
```

### 2.6 限流

- 出价：单用户单房间 ≤ 10 QPS
- 通用 REST 接口：单用户 ≤ 50 QPS
- 超限返回 HTTP `429`，错误码 `90003` 或 `50004`

---

## 3. REST API 详细列表

通用 Header：

```http
Content-Type: application/json; charset=utf-8
Authorization: Bearer <jwt_token>
Idempotency-Key: <uuid>   # 写操作建议携带，关键写操作必传
```

### 3.1 鉴权 Auth

#### POST /api/v1/auth/login

- 用途：用户或商家登录
- Header：`Content-Type`
- Body 示例：

```json
{
  "account": "buyer001",
  "password": "Passw0rd!",
  "role": "buyer"
}
```

- 请求示例：

```bash
curl -X POST 'https://api.example.com/api/v1/auth/login' \
  -H 'Content-Type: application/json; charset=utf-8' \
  -d '{"account":"buyer001","password":"Passw0rd!","role":"buyer"}'
```

- Response 示例：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "accessToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.demo",
    "refreshToken": "rft_9c8b7a",
    "expiresIn": 43200,
    "user": {
      "id": "u_1001",
      "nickname": "竞拍用户001",
      "role": "buyer"
    }
  },
  "trace_id": "trc_auth_login"
}
```

- 常见错误码：`10004`、`10005`、`20001`
- 备注：JWT 有效期 12 小时。

#### POST /api/v1/auth/logout

- 用途：退出登录，使当前 token 进入服务端黑名单
- Header：`Authorization`
- Body 示例：

```json
{
  "refreshToken": "rft_9c8b7a"
}
```

- 请求示例：

```bash
curl -X POST 'https://api.example.com/api/v1/auth/logout' \
  -H 'Authorization: Bearer <jwt_token>' \
  -H 'Content-Type: application/json; charset=utf-8' \
  -d '{"refreshToken":"rft_9c8b7a"}'
```

- Response 示例：

```json
{"code":0,"message":"success","data":{"loggedOut":true},"trace_id":"trc_auth_logout"}
```

- 常见错误码：`10001`、`10002`
- 备注：客户端应同时清除本地 token。

#### POST /api/v1/auth/refresh

- 用途：刷新访问令牌
- Header：`Content-Type`
- Body 示例：

```json
{
  "refreshToken": "rft_9c8b7a"
}
```

- 请求示例：

```bash
curl -X POST 'https://api.example.com/api/v1/auth/refresh' \
  -H 'Content-Type: application/json; charset=utf-8' \
  -d '{"refreshToken":"rft_9c8b7a"}'
```

- Response 示例：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "accessToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.new",
    "expiresIn": 43200
  },
  "trace_id": "trc_auth_refresh"
}
```

- 常见错误码：`10002`、`10005`
- 备注：刷新失败后需重新登录。

#### GET /api/v1/auth/me

- 用途：获取当前登录用户信息
- Header：`Authorization`
- Query：无
- 请求示例：

```bash
curl 'https://api.example.com/api/v1/auth/me' \
  -H 'Authorization: Bearer <jwt_token>'
```

- Response 示例：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "id": "u_1001",
    "nickname": "竞拍用户001",
    "role": "buyer",
    "status": "ACTIVE"
  },
  "trace_id": "trc_auth_me"
}
```

- 常见错误码：`10001`、`10002`
- 备注：用于前端初始化会话。

### 3.2 商品 Item（商家端）

#### POST /api/v1/items

- 用途：商家创建商品
- Header：`Authorization`、`Content-Type: multipart/form-data`、`Idempotency-Key`
- FormData 字段：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `title` | string | 是 | 商品标题 |
| `category` | string | 是 | 类目 |
| `brand` | string | 否 | 品牌 |
| `conditionGrade` | string | 否 | `NEW`、`LIKE_NEW`、`GOOD`、`FAIR`，默认 `NEW` |
| `status` | string | 否 | `DRAFT`、`READY`、`LISTED`、`OFFLINE`，默认 `DRAFT` |
| `description` | string | 否 | 商品描述 |
| `images` | file[] | 否 | 可重复提交的图片文件字段；单张图片不超过 2MB，后端上传对象存储后保存 URL 数组 |

- 请求示例：

```bash
curl -X POST 'https://api.example.com/api/v1/items' \
  -H 'Authorization: Bearer <merchant_jwt>' \
  -H 'Idempotency-Key: item-create-001' \
  -F 'title=孤品手作陶瓷杯' \
  -F 'category=home' \
  -F 'brand=DemoBrand' \
  -F 'conditionGrade=NEW' \
  -F 'description=直播间限量拍卖款' \
  -F 'images=@/path/to/item-1.jpg' \
  -F 'images=@/path/to/item-2.jpg'
```

- Response 示例：

```json
{"code":0,"message":"success","data":{"id":1001,"sellerId":"u_2001","title":"孤品手作陶瓷杯","category":"home","brand":"DemoBrand","conditionGrade":"NEW","images":["https://aieas.tos-cn-boe.volces.com/abc.png"],"description":"直播间限量拍卖款","status":"DRAFT"},"trace_id":"trc_item_create"}
```

- 常见错误码：`10003`、`20001`、`90004`
- 备注：接口不接收图片 URL。图片必须以二进制文件提交，单张图片大小限制 2MB 以内，响应中的 `images` 为对象存储访问 URL。

#### GET /api/v1/items

- 用途：商家查询自己的商品列表
- Header：`Authorization`
- Query：`page`、`page_size`、`keyword`、`status`
- 请求示例：

```bash
curl 'https://api.example.com/api/v1/items?page=1&page_size=20&keyword=陶瓷' \
  -H 'Authorization: Bearer <merchant_jwt>'
```

- Response 示例：

```json
{"code":0,"message":"success","data":{"items":[{"id":"item_1001","title":"孤品手作陶瓷杯","marketPrice":12900,"status":"ACTIVE"}],"total":1,"page":1,"page_size":20},"trace_id":"trc_item_list"}
```

- 常见错误码：`10002`、`20003`
- 备注：仅返回当前商家的商品。

#### GET /api/v1/items/{id}

- 用途：查询商品详情
- Header：`Authorization`
- Query：无
- 请求示例：

```bash
curl 'https://api.example.com/api/v1/items/item_1001' \
  -H 'Authorization: Bearer <merchant_jwt>'
```

- Response 示例：

```json
{"code":0,"message":"success","data":{"id":"item_1001","title":"孤品手作陶瓷杯","description":"直播间限量拍卖款","images":["https://aieas.tos-cn-boe.volces.com/abc.png"],"category":"home","marketPrice":12900,"status":"ACTIVE"},"trace_id":"trc_item_get"}
```

- 常见错误码：`30001`、`30002`
- 备注：商家只能访问自己的商品。

#### PATCH /api/v1/items/{id}

- 用途：更新商品信息
- Header：`Authorization`、`Content-Type: multipart/form-data`、`Idempotency-Key`
- Body：同创建接口的 FormData 字段，全部字段均可选；提交新的 `images` 文件时会替换原图片 URL 数组，不提交 `images` 则保持原图片不变；单张图片不超过 2MB。

- 请求示例：

```bash
curl -X PATCH 'https://api.example.com/api/v1/items/item_1001' \
  -H 'Authorization: Bearer <merchant_jwt>' \
  -H 'Idempotency-Key: item-patch-001' \
  -F 'title=孤品手作陶瓷杯升级款' \
  -F 'description=直播间限量拍卖升级款' \
  -F 'images=@/path/to/new-item-1.jpg'
```

- Response 示例：

```json
{"code":0,"message":"success","data":{"id":1001,"title":"孤品手作陶瓷杯升级款","images":["https://aieas.tos-cn-boe.volces.com/new.png"],"status":"DRAFT"},"trace_id":"trc_item_patch"}
```

- 常见错误码：`30001`、`30002`、`30003`
- 备注：已进入拍卖流程的商品仅允许修改非关键展示字段。

#### DELETE /api/v1/items/{id}

- 用途：删除商品
- Header：`Authorization`、`Idempotency-Key`
- Body 示例：无
- 请求示例：

```bash
curl -X DELETE 'https://api.example.com/api/v1/items/item_1001' \
  -H 'Authorization: Bearer <merchant_jwt>' \
  -H 'Idempotency-Key: item-delete-001'
```

- Response 示例：

```json
{"code":0,"message":"success","data":{"deleted":true},"trace_id":"trc_item_delete"}
```

- 常见错误码：`30001`、`30002`、`30003`
- 备注：已关联进行中拍品的商品不可删除。

### 3.3 拍品 Auction

#### POST /api/v1/auctions

- 用途：商家创建拍品；起拍价允许 0 元，`startPrice` 可为 `0`，但不可小于 0
- Header：`Authorization`、`Content-Type`、`Idempotency-Key`
- Body 示例：

```json
{
  "itemId": "item_1001",
  "title": "陶瓷杯 0 元起拍专场",
  "startPrice": 0,
  "minIncrement": 100,
  "depositAmount": 5000,
  "scheduledStartTime": "2026-05-23T20:00:00+08:00",
  "durationSec": 600,
  "ruleSnapshot": {
    "startPrice": 0,
    "minIncrement": 100,
    "antiSnipeSec": 15,
    "extendSec": 10
  }
}
```

- 请求示例：

```bash
curl -X POST 'https://api.example.com/api/v1/auctions' \
  -H 'Authorization: Bearer <merchant_jwt>' \
  -H 'Content-Type: application/json; charset=utf-8' \
  -H 'Idempotency-Key: auction-create-001' \
  -d '{"itemId":"item_1001","title":"陶瓷杯 0 元起拍专场","startPrice":0,"minIncrement":100,"depositAmount":5000,"scheduledStartTime":"2026-05-23T20:00:00+08:00","durationSec":600,"ruleSnapshot":{"startPrice":0,"minIncrement":100,"antiSnipeSec":15,"extendSec":10}}'
```

- Response 示例：

```json
{"code":0,"message":"success","data":{"id":"auc_1001","itemId":"item_1001","title":"陶瓷杯 0 元起拍专场","startPrice":0,"currentPrice":0,"leaderBidderId":"","status":"DRAFT","endTsMs":0},"trace_id":"trc_auction_create"}
```

- 常见错误码：`40003`、`30001`、`10003`
- 备注：若 `startPrice < 0` 返回 `40003`；`startPrice = 0` 为合法 0 元起拍。

#### GET /api/v1/auctions

- 用途：查询拍品列表
- Header：匿名可访问；登录后可返回报名状态
- Query：`page`、`page_size`、`status`、`keyword`
- 请求示例：

```bash
curl 'https://api.example.com/api/v1/auctions?page=1&page_size=20&status=READY'
```

- Response 示例：

```json
{"code":0,"message":"success","data":{"items":[{"id":"auc_1001","title":"陶瓷杯 0 元起拍专场","startPrice":0,"currentPrice":0,"leaderBidderId":"","status":"READY","endTsMs":0}],"total":1,"page":1,"page_size":20},"trace_id":"trc_auction_list"}
```

- 常见错误码：`20003`、`20001`
- 备注：公开列表默认不返回风控与商家内部字段。

#### GET /api/v1/auctions/{id}

- 用途：查询拍品详情
- Header：匿名可访问
- Query：无
- 请求示例：

```bash
curl 'https://api.example.com/api/v1/auctions/auc_1001'
```

- Response 示例：

```json
{"code":0,"message":"success","data":{"id":"auc_1001","itemId":"item_1001","title":"陶瓷杯 0 元起拍专场","startPrice":0,"currentPrice":1200,"leaderBidderId":"u_1002","status":"LIVE","endTsMs":1769170200000,"ruleSnapshot":{"startPrice":0,"minIncrement":100,"antiSnipeSec":15,"extendSec":10}},"trace_id":"trc_auction_get"}
```

- 常见错误码：`40001`
- 备注：`currentPrice` 单位为分。

#### PATCH /api/v1/auctions/{id}

- 用途：更新拍品配置，仅 `DRAFT/READY` 状态允许
- Header：`Authorization`、`Content-Type`、`Idempotency-Key`
- Body 示例：

```json
{
  "title": "陶瓷杯福利专场",
  "minIncrement": 200,
  "scheduledStartTime": "2026-05-23T21:00:00+08:00"
}
```

- 请求示例：

```bash
curl -X PATCH 'https://api.example.com/api/v1/auctions/auc_1001' \
  -H 'Authorization: Bearer <merchant_jwt>' \
  -H 'Content-Type: application/json; charset=utf-8' \
  -H 'Idempotency-Key: auction-patch-001' \
  -d '{"title":"陶瓷杯福利专场","minIncrement":200,"scheduledStartTime":"2026-05-23T21:00:00+08:00"}'
```

- Response 示例：

```json
{"code":0,"message":"success","data":{"id":"auc_1001","title":"陶瓷杯福利专场","minIncrement":200,"status":"READY"},"trace_id":"trc_auction_patch"}
```

- 常见错误码：`40001`、`40002`、`30002`
- 备注：`LIVE/CLOSED/CANCELED` 状态不可修改。

#### POST /api/v1/auctions/{id}/start

- 用途：商家开始拍卖
- Header：`Authorization`、`Idempotency-Key`
- Body 示例：

```json
{
  "operatorRemark": "直播间正式开拍"
}
```

- 请求示例：

```bash
curl -X POST 'https://api.example.com/api/v1/auctions/auc_1001/start' \
  -H 'Authorization: Bearer <merchant_jwt>' \
  -H 'Content-Type: application/json; charset=utf-8' \
  -H 'Idempotency-Key: auction-start-001' \
  -d '{"operatorRemark":"直播间正式开拍"}'
```

- Response 示例：

```json
{"code":0,"message":"success","data":{"id":"auc_1001","status":"LIVE","startTsMs":1769169600000,"endTsMs":1769170200000},"trace_id":"trc_auction_start"}
```

- 常见错误码：`40001`、`40002`、`10003`
- 备注：开始后通过 WS 广播 `auction.started`。

#### POST /api/v1/auctions/{id}/hammer

- 用途：商家落锤成交，必须携带 `Idempotency-Key`
- Header：`Authorization`、`Content-Type`、`Idempotency-Key`
- Body 示例：

```json
{
  "hammerRemark": "主播确认成交"
}
```

- 请求示例：

```bash
curl -X POST 'https://api.example.com/api/v1/auctions/auc_1001/hammer' \
  -H 'Authorization: Bearer <merchant_jwt>' \
  -H 'Content-Type: application/json; charset=utf-8' \
  -H 'Idempotency-Key: hammer-auc-1001-001' \
  -d '{"hammerRemark":"主播确认成交"}'
```

- Response 示例：

```json
{"code":0,"message":"success","data":{"auctionId":"auc_1001","status":"CLOSED","winnerBidderId":"u_1002","finalPrice":5600,"orderId":"ord_1001"},"trace_id":"trc_auction_hammer"}
```

- 常见错误码：`40001`、`40002`、`90004`
- 备注：成功后广播 `auction.closed` 与 `order.created`。

#### POST /api/v1/auctions/{id}/cancel

- 用途：商家取消拍卖
- Header：`Authorization`、`Content-Type`、`Idempotency-Key`
- Body 示例：

```json
{
  "reason": "商品临时下架"
}
```

- 请求示例：

```bash
curl -X POST 'https://api.example.com/api/v1/auctions/auc_1001/cancel' \
  -H 'Authorization: Bearer <merchant_jwt>' \
  -H 'Content-Type: application/json; charset=utf-8' \
  -H 'Idempotency-Key: auction-cancel-001' \
  -d '{"reason":"商品临时下架"}'
```

- Response 示例：

```json
{"code":0,"message":"success","data":{"id":"auc_1001","status":"CANCELED"},"trace_id":"trc_auction_cancel"}
```

- 常见错误码：`40001`、`40002`、`10003`
- 备注：取消后释放未成交用户保证金。

#### GET /api/v1/auctions/{id}/state

- 用途：获取拍品实时状态快照
- Header：匿名可访问
- Query：无
- 请求示例：

```bash
curl 'https://api.example.com/api/v1/auctions/auc_1001/state'
```

- Response 示例：

```json
{"code":0,"message":"success","data":{"auctionId":"auc_1001","status":"LIVE","currentPrice":5600,"leaderBidderId":"u_1002","bidCount":18,"endTsMs":1769170200000,"serverTsMs":1769169900000},"trace_id":"trc_auction_state"}
```

- 常见错误码：`40001`
- 备注：前端重连后可用该接口恢复状态。

#### POST /api/v1/auctions/{id}/enroll

- 用途：用户报名拍卖并冻结保证金
- Header：`Authorization`、`Content-Type`、`Idempotency-Key`
- Body 示例：

```json
{
  "depositPayChannel": "MOCK_PAY"
}
```

- 请求示例：

```bash
curl -X POST 'https://api.example.com/api/v1/auctions/auc_1001/enroll' \
  -H 'Authorization: Bearer <buyer_jwt>' \
  -H 'Content-Type: application/json; charset=utf-8' \
  -H 'Idempotency-Key: enroll-auc-1001-u-1001' \
  -d '{"depositPayChannel":"MOCK_PAY"}'
```

- Response 示例：

```json
{"code":0,"message":"success","data":{"auctionId":"auc_1001","userId":"u_1001","enrolled":true,"depositLedgerId":"dep_1001","depositAmount":5000,"depositStatus":"FROZEN"},"trace_id":"trc_auction_enroll"}
```

- 常见错误码：`70001`、`70002`、`80001`
- 备注：报名成功后才允许出价。

#### GET /api/v1/auctions/mine

- 用途：查询当前用户相关拍品；买家返回已报名/参拍，商家返回自有拍品
- Header：`Authorization`
- Query：`page`、`page_size`、`roleView`、`status`
- 请求示例：

```bash
curl 'https://api.example.com/api/v1/auctions/mine?page=1&page_size=20&roleView=buyer' \
  -H 'Authorization: Bearer <jwt_token>'
```

- Response 示例：

```json
{"code":0,"message":"success","data":{"items":[{"id":"auc_1001","title":"陶瓷杯福利专场","currentPrice":5600,"leaderBidderId":"u_1002","status":"LIVE","endTsMs":1769170200000}],"total":1,"page":1,"page_size":20},"trace_id":"trc_auction_mine"}
```

- 常见错误码：`10001`、`10002`、`20003`
- 备注：`roleView` 可为 `buyer` 或 `merchant`。

### 3.4 订单 Order

#### GET /api/v1/orders

- 用途：商家查询订单列表
- Header：`Authorization`
- Query：`page`、`page_size`、`status`、`auctionId`
- 请求示例：

```bash
curl 'https://api.example.com/api/v1/orders?page=1&page_size=20&status=PENDING_PAY' \
  -H 'Authorization: Bearer <merchant_jwt>'
```

- Response 示例：

```json
{"code":0,"message":"success","data":{"items":[{"id":"ord_1001","auctionId":"auc_1001","buyerId":"u_1002","merchantId":"m_1001","amount":5600,"status":"PENDING_PAY"}],"total":1,"page":1,"page_size":20},"trace_id":"trc_order_list"}
```

- 常见错误码：`10003`、`20003`
- 备注：商家仅能查看自有拍品产生的订单。

#### GET /api/v1/orders/mine

- 用途：买家查询自己的订单
- Header：`Authorization`
- Query：`page`、`page_size`、`status`
- 请求示例：

```bash
curl 'https://api.example.com/api/v1/orders/mine?page=1&page_size=20' \
  -H 'Authorization: Bearer <buyer_jwt>'
```

- Response 示例：

```json
{"code":0,"message":"success","data":{"items":[{"id":"ord_1001","auctionId":"auc_1001","buyerId":"u_1002","amount":5600,"status":"PENDING_PAY"}],"total":1,"page":1,"page_size":20},"trace_id":"trc_order_mine"}
```

- 常见错误码：`10001`、`10002`
- 备注：金额 `amount` 单位为分。

#### GET /api/v1/orders/{id}

- 用途：查询订单详情
- Header：`Authorization`
- Query：无
- 请求示例：

```bash
curl 'https://api.example.com/api/v1/orders/ord_1001' \
  -H 'Authorization: Bearer <jwt_token>'
```

- Response 示例：

```json
{"code":0,"message":"success","data":{"id":"ord_1001","auctionId":"auc_1001","buyerId":"u_1002","merchantId":"m_1001","amount":5600,"status":"PENDING_PAY","createdAt":"2026-05-23T20:10:00+08:00","paidAt":""},"trace_id":"trc_order_get"}
```

- 常见错误码：`60001`、`10003`
- 备注：买家、商家、管理员按权限查看。

#### POST /api/v1/orders/{id}/pay

- 用途：买家支付订单
- Header：`Authorization`、`Content-Type`、`Idempotency-Key`
- Body 示例：

```json
{
  "payChannel": "MOCK_PAY"
}
```

- 请求示例：

```bash
curl -X POST 'https://api.example.com/api/v1/orders/ord_1001/pay' \
  -H 'Authorization: Bearer <buyer_jwt>' \
  -H 'Content-Type: application/json; charset=utf-8' \
  -H 'Idempotency-Key: pay-ord-1001-001' \
  -d '{"payChannel":"MOCK_PAY"}'
```

- Response 示例：

```json
{"code":0,"message":"success","data":{"id":"ord_1001","status":"PAID","amount":5600,"paidAt":"2026-05-23T20:12:00+08:00"},"trace_id":"trc_order_pay"}
```

- 常见错误码：`60001`、`60002`、`60003`
- 备注：支付成功后可释放保证金或抵扣尾款，按业务配置执行。

### 3.5 平台管理 Admin（role=admin）

#### POST /api/v1/admin/auth/login

- 用途：管理员登录
- Header：`Content-Type`
- Body 示例：`{"account":"admin001","password":"AdminPassw0rd!"}`
- 请求示例：

```bash
curl -X POST 'https://api.example.com/api/v1/admin/auth/login' \
  -H 'Content-Type: application/json; charset=utf-8' \
  -d '{"account":"admin001","password":"AdminPassw0rd!"}'
```

- Response 示例：

```json
{"code":0,"message":"success","data":{"accessToken":"<admin_jwt>","expiresIn":43200,"user":{"id":"adm_1001","role":"admin","nickname":"平台管理员"}},"trace_id":"trc_admin_login"}
```

- 常见错误码：`10004`、`10005`
- 备注：仅签发 `role=admin` 的 JWT。

#### GET /api/v1/admin/auctions

- 用途：后台查询全量拍品
- Header：`Authorization: Bearer <admin_jwt>`
- Query：`page`、`page_size`、`status`、`merchantId`
- 请求示例：`curl 'https://api.example.com/api/v1/admin/auctions?page=1&page_size=20&status=READY' -H 'Authorization: Bearer <admin_jwt>'`
- Response 示例：

```json
{"code":0,"message":"success","data":{"items":[{"id":"auc_1001","merchantId":"m_1001","title":"陶瓷杯福利专场","status":"READY","currentPrice":0,"leaderBidderId":"","endTsMs":0}],"total":1,"page":1,"page_size":20},"trace_id":"trc_admin_auction_list"}
```

- 常见错误码：`10003`、`20003`
- 备注：后台可跨商家查询。

#### POST /api/v1/admin/auctions/{id}/audit

- 用途：审核拍品
- Header：`Authorization`、`Content-Type`、`Idempotency-Key`
- Body 示例：`{"auditResult":"APPROVED","reason":"符合平台规范"}`
- 请求示例：`curl -X POST 'https://api.example.com/api/v1/admin/auctions/auc_1001/audit' -H 'Authorization: Bearer <admin_jwt>' -H 'Content-Type: application/json; charset=utf-8' -H 'Idempotency-Key: admin-audit-001' -d '{"auditResult":"APPROVED","reason":"符合平台规范"}'`
- Response 示例：`{"code":0,"message":"success","data":{"id":"auc_1001","auditStatus":"APPROVED","status":"READY"},"trace_id":"trc_admin_audit"}`
- 常见错误码：`40001`、`40002`
- 备注：审核通过后拍品可开拍。

#### POST /api/v1/admin/auctions/{id}/cancel

- 用途：平台强制取消拍品
- Header：`Authorization`、`Content-Type`、`Idempotency-Key`
- Body 示例：`{"reason":"违规商品"}`
- 请求示例：`curl -X POST 'https://api.example.com/api/v1/admin/auctions/auc_1001/cancel' -H 'Authorization: Bearer <admin_jwt>' -H 'Content-Type: application/json; charset=utf-8' -H 'Idempotency-Key: admin-cancel-001' -d '{"reason":"违规商品"}'`
- Response 示例：`{"code":0,"message":"success","data":{"id":"auc_1001","status":"CANCELED"},"trace_id":"trc_admin_cancel"}`
- 常见错误码：`40001`、`40002`
- 备注：会写入审计日志。

#### POST /api/v1/admin/auctions/{id}/close

- 用途：平台强制关闭拍品
- Header：`Authorization`、`Content-Type`、`Idempotency-Key`
- Body 示例：`{"reason":"直播中断，平台介入关闭"}`
- 请求示例：`curl -X POST 'https://api.example.com/api/v1/admin/auctions/auc_1001/close' -H 'Authorization: Bearer <admin_jwt>' -H 'Content-Type: application/json; charset=utf-8' -H 'Idempotency-Key: admin-close-001' -d '{"reason":"直播中断，平台介入关闭"}'`
- Response 示例：`{"code":0,"message":"success","data":{"id":"auc_1001","status":"CLOSED"},"trace_id":"trc_admin_close"}`
- 常见错误码：`40001`、`40002`
- 备注：是否生成订单取决于当前是否存在有效领先出价。

#### GET /api/v1/admin/users

- 用途：查询平台用户
- Header：`Authorization`
- Query：`page`、`page_size`、`role`、`status`、`keyword`
- 请求示例：`curl 'https://api.example.com/api/v1/admin/users?page=1&page_size=20&role=buyer' -H 'Authorization: Bearer <admin_jwt>'`
- Response 示例：`{"code":0,"message":"success","data":{"items":[{"id":"u_1001","nickname":"竞拍用户001","role":"buyer","status":"ACTIVE","riskLevel":"LOW"}],"total":1,"page":1,"page_size":20},"trace_id":"trc_admin_users"}`
- 常见错误码：`10003`、`20003`
- 备注：敏感字段脱敏返回。

#### PATCH /api/v1/admin/users/{id}

- 用途：更新用户状态或风险等级
- Header：`Authorization`、`Content-Type`、`Idempotency-Key`
- Body 示例：`{"status":"DISABLED","riskLevel":"HIGH","reason":"恶意出价"}`
- 请求示例：`curl -X PATCH 'https://api.example.com/api/v1/admin/users/u_1001' -H 'Authorization: Bearer <admin_jwt>' -H 'Content-Type: application/json; charset=utf-8' -H 'Idempotency-Key: admin-user-patch-001' -d '{"status":"DISABLED","riskLevel":"HIGH","reason":"恶意出价"}'`
- Response 示例：`{"code":0,"message":"success","data":{"id":"u_1001","status":"DISABLED","riskLevel":"HIGH"},"trace_id":"trc_admin_user_patch"}`
- 常见错误码：`10003`、`10005`
- 备注：状态变更会影响后续登录与出价。

#### POST /api/v1/admin/blacklist

- 用途：加入黑名单
- Header：`Authorization`、`Content-Type`、`Idempotency-Key`
- Body 示例：`{"userId":"u_1001","reason":"多次恶意竞价","expireAt":"2026-06-23T00:00:00+08:00"}`
- 请求示例：`curl -X POST 'https://api.example.com/api/v1/admin/blacklist' -H 'Authorization: Bearer <admin_jwt>' -H 'Content-Type: application/json; charset=utf-8' -H 'Idempotency-Key: blacklist-add-001' -d '{"userId":"u_1001","reason":"多次恶意竞价","expireAt":"2026-06-23T00:00:00+08:00"}'`
- Response 示例：`{"code":0,"message":"success","data":{"userId":"u_1001","blacklisted":true},"trace_id":"trc_blacklist_add"}`
- 常见错误码：`80001`、`10003`
- 备注：黑名单用户不能报名和出价。

#### DELETE /api/v1/admin/blacklist/{user_id}

- 用途：移出黑名单
- Header：`Authorization`、`Idempotency-Key`
- Body 示例：无
- 请求示例：`curl -X DELETE 'https://api.example.com/api/v1/admin/blacklist/u_1001' -H 'Authorization: Bearer <admin_jwt>' -H 'Idempotency-Key: blacklist-remove-001'`
- Response 示例：`{"code":0,"message":"success","data":{"userId":"u_1001","blacklisted":false},"trace_id":"trc_blacklist_delete"}`
- 常见错误码：`10003`、`20001`
- 备注：移出后用户状态仍需为 `ACTIVE` 才能出价。

#### GET /api/v1/admin/blacklist

- 用途：查询黑名单
- Header：`Authorization`
- Query：`page`、`page_size`、`keyword`
- 请求示例：`curl 'https://api.example.com/api/v1/admin/blacklist?page=1&page_size=20' -H 'Authorization: Bearer <admin_jwt>'`
- Response 示例：`{"code":0,"message":"success","data":{"items":[{"userId":"u_1001","reason":"多次恶意竞价","expireAt":"2026-06-23T00:00:00+08:00"}],"total":1,"page":1,"page_size":20},"trace_id":"trc_blacklist_list"}`
- 常见错误码：`10003`、`20003`
- 备注：过期黑名单可由后台任务自动解除。

#### GET /api/v1/admin/orders

- 用途：后台查询全量订单
- Header：`Authorization`
- Query：`page`、`page_size`、`status`、`buyerId`、`merchantId`
- 请求示例：`curl 'https://api.example.com/api/v1/admin/orders?page=1&page_size=20&status=PAID' -H 'Authorization: Bearer <admin_jwt>'`
- Response 示例：`{"code":0,"message":"success","data":{"items":[{"id":"ord_1001","auctionId":"auc_1001","buyerId":"u_1002","merchantId":"m_1001","amount":5600,"status":"PAID"}],"total":1,"page":1,"page_size":20},"trace_id":"trc_admin_orders"}`
- 常见错误码：`10003`、`20003`
- 备注：用于客服与运营对账。

#### GET /api/v1/admin/audit-logs

- 用途：查询审计日志
- Header：`Authorization`
- Query：`page`、`page_size`、`operatorId`、`action`、`startTime`、`endTime`
- 请求示例：`curl 'https://api.example.com/api/v1/admin/audit-logs?page=1&page_size=20&action=AUCTION_AUDIT' -H 'Authorization: Bearer <admin_jwt>'`
- Response 示例：`{"code":0,"message":"success","data":{"items":[{"id":"log_1001","operatorId":"adm_1001","action":"AUCTION_AUDIT","targetId":"auc_1001","createdAt":"2026-05-23T19:00:00+08:00"}],"total":1,"page":1,"page_size":20},"trace_id":"trc_audit_logs"}`
- 常见错误码：`10003`、`20004`
- 备注：审计日志不可由 API 修改。

#### GET /api/v1/admin/risk/rules

- 用途：查询风控规则
- Header：`Authorization`
- Query：无
- 请求示例：`curl 'https://api.example.com/api/v1/admin/risk/rules' -H 'Authorization: Bearer <admin_jwt>'`
- Response 示例：`{"code":0,"message":"success","data":{"bidMaxQps":10,"blacklistEnabled":true,"selfOverbidBlocked":true,"suspiciousIpThreshold":5},"trace_id":"trc_risk_rules_get"}`
- 常见错误码：`10003`
- 备注：规则变更对新请求即时生效。

#### PUT /api/v1/admin/risk/rules

- 用途：更新风控规则
- Header：`Authorization`、`Content-Type`、`Idempotency-Key`
- Body 示例：`{"bidMaxQps":10,"blacklistEnabled":true,"selfOverbidBlocked":true,"suspiciousIpThreshold":5}`
- 请求示例：`curl -X PUT 'https://api.example.com/api/v1/admin/risk/rules' -H 'Authorization: Bearer <admin_jwt>' -H 'Content-Type: application/json; charset=utf-8' -H 'Idempotency-Key: risk-rules-put-001' -d '{"bidMaxQps":10,"blacklistEnabled":true,"selfOverbidBlocked":true,"suspiciousIpThreshold":5}'`
- Response 示例：`{"code":0,"message":"success","data":{"updated":true,"version":3},"trace_id":"trc_risk_rules_put"}`
- 常见错误码：`10003`、`20001`
- 备注：建议前端展示二次确认。

#### GET /api/v1/admin/risk/events

- 用途：查询风控事件
- Header：`Authorization`
- Query：`page`、`page_size`、`status`、`riskType`、`userId`
- 请求示例：`curl 'https://api.example.com/api/v1/admin/risk/events?page=1&page_size=20&status=OPEN' -H 'Authorization: Bearer <admin_jwt>'`
- Response 示例：`{"code":0,"message":"success","data":{"items":[{"id":"risk_1001","userId":"u_1001","auctionId":"auc_1001","riskType":"HIGH_FREQ_BID","status":"OPEN","createdAt":"2026-05-23T20:01:00+08:00"}],"total":1,"page":1,"page_size":20},"trace_id":"trc_risk_events"}`
- 常见错误码：`10003`、`20003`
- 备注：高危事件会通过 WS `risk.event` 推送到管理房间。

#### PATCH /api/v1/admin/risk/events/{id}

- 用途：处理风控事件
- Header：`Authorization`、`Content-Type`、`Idempotency-Key`
- Body 示例：`{"status":"RESOLVED","handleResult":"已加入黑名单","remark":"人工复核确认"}`
- 请求示例：`curl -X PATCH 'https://api.example.com/api/v1/admin/risk/events/risk_1001' -H 'Authorization: Bearer <admin_jwt>' -H 'Content-Type: application/json; charset=utf-8' -H 'Idempotency-Key: risk-event-patch-001' -d '{"status":"RESOLVED","handleResult":"已加入黑名单","remark":"人工复核确认"}'`
- Response 示例：`{"code":0,"message":"success","data":{"id":"risk_1001","status":"RESOLVED","handleResult":"已加入黑名单"},"trace_id":"trc_risk_event_patch"}`
- 常见错误码：`80003`、`10003`
- 备注：处理记录进入审计日志。

#### GET /api/v1/admin/dashboard/metrics

- 用途：获取管理后台看板指标
- Header：`Authorization`
- Query：`startTime`、`endTime`
- 请求示例：`curl 'https://api.example.com/api/v1/admin/dashboard/metrics?startTime=2026-05-23T00:00:00%2B08:00&endTime=2026-05-23T23:59:59%2B08:00' -H 'Authorization: Bearer <admin_jwt>'`
- Response 示例：`{"code":0,"message":"success","data":{"liveAuctionCount":12,"gmv":986000,"paidOrderCount":88,"riskEventCount":3,"activeBidderCount":560},"trace_id":"trc_dashboard_metrics"}`
- 常见错误码：`10003`、`20004`
- 备注：`gmv` 单位为分。

#### GET /api/v1/admin/configs

- 用途：查询平台配置
- Header：`Authorization`
- Query：无
- 请求示例：`curl 'https://api.example.com/api/v1/admin/configs' -H 'Authorization: Bearer <admin_jwt>'`
- Response 示例：`{"code":0,"message":"success","data":{"depositDefaultAmount":5000,"bidAntiSnipeSec":15,"bidExtendSec":10},"trace_id":"trc_configs_get"}`
- 常见错误码：`10003`
- 备注：配置值按后端实际类型返回。

#### PUT /api/v1/admin/configs/{key}

- 用途：更新单项平台配置
- Header：`Authorization`、`Content-Type`、`Idempotency-Key`
- Body 示例：`{"value":6000,"remark":"调整默认保证金"}`
- 请求示例：`curl -X PUT 'https://api.example.com/api/v1/admin/configs/depositDefaultAmount' -H 'Authorization: Bearer <admin_jwt>' -H 'Content-Type: application/json; charset=utf-8' -H 'Idempotency-Key: config-put-001' -d '{"value":6000,"remark":"调整默认保证金"}'`
- Response 示例：`{"code":0,"message":"success","data":{"key":"depositDefaultAmount","value":6000,"updated":true},"trace_id":"trc_config_put"}`
- 常见错误码：`10003`、`20001`
- 备注：配置更新写入审计日志。

### 3.6 AI 加分接口

#### POST /api/v1/ai/start-price-suggest

- 用途：根据商品信息建议起拍价；返回金额单位为分，可返回 0 表示适合 0 元起拍
- Header：`Authorization`、`Content-Type`、`Idempotency-Key`
- Body 示例：

```json
{
  "itemId": "item_1001",
  "marketPrice": 12900,
  "category": "home",
  "strategy": "TRAFFIC_FIRST"
}
```

- 请求示例：

```bash
curl -X POST 'https://api.example.com/api/v1/ai/start-price-suggest' \
  -H 'Authorization: Bearer <merchant_jwt>' \
  -H 'Content-Type: application/json; charset=utf-8' \
  -H 'Idempotency-Key: ai-price-001' \
  -d '{"itemId":"item_1001","marketPrice":12900,"category":"home","strategy":"TRAFFIC_FIRST"}'
```

- Response 示例：

```json
{"code":0,"message":"success","data":{"suggestedStartPrice":0,"suggestedMinIncrement":100,"reason":"低门槛起拍有利于直播间互动和快速聚集人气"},"trace_id":"trc_ai_price"}
```

- 常见错误码：`30001`、`20001`、`90002`
- 备注：AI 建议仅供商家确认，不自动创建拍品。

#### POST /api/v1/ai/announcement

- 用途：生成直播间公告文案
- Header：`Authorization`、`Content-Type`、`Idempotency-Key`
- Body 示例：

```json
{
  "auctionId": "auc_1001",
  "tone": "EXCITED",
  "sellingPoints": ["手作孤品", "0 元起拍", "限时 10 分钟"]
}
```

- 请求示例：

```bash
curl -X POST 'https://api.example.com/api/v1/ai/announcement' \
  -H 'Authorization: Bearer <merchant_jwt>' \
  -H 'Content-Type: application/json; charset=utf-8' \
  -H 'Idempotency-Key: ai-announcement-001' \
  -d '{"auctionId":"auc_1001","tone":"EXCITED","sellingPoints":["手作孤品","0 元起拍","限时 10 分钟"]}'
```

- Response 示例：

```json
{"code":0,"message":"success","data":{"announcement":"陶瓷杯福利专场即将开拍，0 元起拍，10 分钟限时竞价，喜欢的朋友抓紧报名！"},"trace_id":"trc_ai_announcement"}
```

- 常见错误码：`40001`、`20001`、`90002`
- 备注：商家可将结果通过 WS `announcement` 广播给房间用户。

---

## 4. WebSocket 协议

### 4.1 连接

- 连接地址：`wss://api.example.com/ws/auctions/<auction_id>?token=<jwt>&lastSeq=<seq>`
- 升级时校验 JWT；校验失败关闭连接，建议关闭码 `4401`
- 服务端每 30s 发送 ping；客户端 60s 未响应则断开
- 客户端断线重连可携带 `lastSeq`，服务端会补发近期窗口内 `seq > lastSeq` 的事件；若窗口已过期，会发送 `room.snapshot_required`，客户端应调用 `GET /api/v1/auctions/{id}/state` 拉取快照

连接示例：

```bash
wscat -c 'wss://api.example.com/ws/auctions/1001?token=<jwt>&lastSeq=102'
```

### 4.2 统一 JSON 报文

```json
{
  "type": "bid.place",
  "payload": {},
  "requestId": "req_001",
  "seq": 1
}
```

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| type | string | 是 | 事件名 |
| payload | object | 是 | 事件载荷 |
| requestId | string | 客户端请求必填 | 请求幂等与响应关联 |
| seq | int64 | 服务端推送必填 | 服务端房间内递增序号 |

### 4.3 客户端 → 服务端事件

#### bid.place

```json
{
  "type": "bid.place",
  "requestId": "req_bid_001",
  "payload": {
    "auctionId": "auc_1001",
    "bidPrice": 5600,
    "clientTsMs": 1769169900000
  }
}
```

#### room.subscribe

```json
{
  "type": "room.subscribe",
  "requestId": "req_sub_001",
  "payload": {
    "room": "auction:auc_1001"
  }
}
```

#### room.unsubscribe

```json
{
  "type": "room.unsubscribe",
  "requestId": "req_unsub_001",
  "payload": {
    "room": "auction:auc_1001"
  }
}
```

#### heartbeat

```json
{
  "type": "heartbeat",
  "requestId": "req_hb_001",
  "payload": {
    "clientTsMs": 1769169900000
  }
}
```

### 4.4 服务端 → 客户端事件

#### bid.ack

```json
{
  "type": "bid.ack",
  "requestId": "req_bid_001",
  "seq": 101,
  "payload": {
    "accepted": true,
    "auctionId": "auc_1001",
    "bidId": "bid_1001",
    "serverTsMs": 1769169900123
  }
}
```

#### bid.accepted

```json
{
  "type": "bid.accepted",
  "seq": 102,
  "payload": {
    "auctionId": "auc_1001",
    "bidId": "bid_1001",
    "bidderId": "u_1002",
    "bidPrice": 5600,
    "bidTsMs": 1769169900123,
    "currentPrice": 5600,
    "leaderBidderId": "u_1002"
  }
}
```

#### bid.rejected

```json
{
  "type": "bid.rejected",
  "requestId": "req_bid_002",
  "seq": 103,
  "payload": {
    "auctionId": "auc_1001",
    "code": 50001,
    "message": "出价需不低于当前价加最低加价幅度",
    "currentPrice": 5600,
    "minNextPrice": 5700
  }
}
```

#### ranking.updated

```json
{
  "type": "ranking.updated",
  "seq": 104,
  "payload": {
    "auctionId": "auc_1001",
    "items": [
      {"rank": 1, "bidderId": "u_1002", "nicknameMask": "用户**02", "bidPrice": 5600, "bidTsMs": 1769169900123},
      {"rank": 2, "bidderId": "u_1001", "nicknameMask": "用户**01", "bidPrice": 5500, "bidTsMs": 1769169899123}
    ]
  }
}
```

#### bidder.overtaken

```json
{
  "type": "bidder.overtaken",
  "seq": 105,
  "payload": {
    "auctionId": "auc_1001",
    "bidderId": "u_1001",
    "newLeaderBidderId": "u_1002",
    "currentPrice": 5600
  }
}
```

#### timer.tick

```json
{
  "type": "timer.tick",
  "seq": 106,
  "payload": {
    "auctionId": "auc_1001",
    "serverTsMs": 1769169901000,
    "endTsMs": 1769170200000,
    "remainingMs": 299000
  }
}
```

#### timer.extended

```json
{
  "type": "timer.extended",
  "seq": 107,
  "payload": {
    "auctionId": "auc_1001",
    "reason": "ANTI_SNIPE",
    "oldEndTsMs": 1769170200000,
    "newEndTsMs": 1769170210000,
    "extendMs": 10000
  }
}
```

#### auction.started

```json
{
  "type": "auction.started",
  "seq": 108,
  "payload": {
    "auctionId": "auc_1001",
    "status": "LIVE",
    "startTsMs": 1769169600000,
    "endTsMs": 1769170200000
  }
}
```

#### auction.closed

```json
{
  "type": "auction.closed",
  "seq": 109,
  "payload": {
    "auctionId": "auc_1001",
    "status": "CLOSED",
    "winnerBidderId": "u_1002",
    "finalPrice": 5600,
    "closedTsMs": 1769170200000
  }
}
```

#### order.created

```json
{
  "type": "order.created",
  "seq": 110,
  "payload": {
    "orderId": "ord_1001",
    "auctionId": "auc_1001",
    "buyerId": "u_1002",
    "amount": 5600,
    "status": "PENDING_PAY"
  }
}
```

#### announcement

```json
{
  "type": "announcement",
  "seq": 111,
  "payload": {
    "auctionId": "auc_1001",
    "level": "INFO",
    "content": "陶瓷杯福利专场即将结束，喜欢的朋友抓紧出价！",
    "serverTsMs": 1769169950000
  }
}
```

#### risk.event

```json
{
  "type": "risk.event",
  "seq": 112,
  "payload": {
    "id": "risk_1001",
    "auctionId": "auc_1001",
    "userId": "u_1001",
    "riskType": "HIGH_FREQ_BID",
    "level": "HIGH",
    "message": "用户出价频率异常",
    "createdTsMs": 1769169905000
  }
}
```

### 4.5 错误事件：error

```json
{
  "type": "error",
  "requestId": "req_bid_003",
  "seq": 113,
  "payload": {
    "code": 10002,
    "message": "访问令牌无效或过期",
    "retryable": false
  }
}
```

### 4.6 房间命名

| 房间名 | 用途 | 权限 |
| --- | --- | --- |
| `auction:{auctionId}` | 拍品公开竞拍房间 | 已登录用户；部分只读消息可匿名按产品策略开放 |
| `auction:{auctionId}:control` | 商家控制房间 | 拍品所属商家、管理员 |
| `admin:platform-monitor` | 平台监控房间 | `role=admin` |

---

## 5. 数据结构定义

### LoginRequest

| 字段名 | 类型 | 是否必填 | 含义 |
| --- | --- | --- | --- |
| account | string | 是 | 登录账号 |
| password | string | 是 | 登录密码 |
| role | string | 否 | 期望角色：`buyer`、`merchant`；管理员使用后台登录接口 |

### LoginResponse

| 字段名 | 类型 | 是否必填 | 含义 |
| --- | --- | --- | --- |
| accessToken | string | 是 | JWT 访问令牌 |
| refreshToken | string | 否 | 刷新令牌 |
| expiresIn | int | 是 | 有效期秒数，默认 43200 |
| user | object | 是 | 登录用户信息 |

### ItemDTO

| 字段名 | 类型 | 是否必填 | 含义 |
| --- | --- | --- | --- |
| id | string | 是 | 商品 ID |
| merchantId | string | 是 | 商家 ID |
| title | string | 是 | 商品标题 |
| description | string | 否 | 商品描述 |
| images | string[] | 否 | 商品图片 URL 列表 |
| category | string | 否 | 商品类目 |
| marketPrice | int64 | 否 | 市场价，单位分 |
| status | string | 是 | 商品状态：`ACTIVE`、`DELETED` |
| createdAt | string | 是 | 创建时间，ISO 8601 |

### AuctionRuleSnapshot

| 字段名 | 类型 | 是否必填 | 含义 |
| --- | --- | --- | --- |
| startPrice | int64 | 是 | 起拍价，单位分，允许为 0 |
| minIncrement | int64 | 是 | 最低加价幅度，单位分 |
| antiSnipeSec | int | 是 | 防狙击触发窗口，单位秒 |
| extendSec | int | 是 | 防狙击延长时间，单位秒 |
| depositAmount | int64 | 否 | 保证金金额，单位分 |

### AuctionDTO

| 字段名 | 类型 | 是否必填 | 含义 |
| --- | --- | --- | --- |
| id | string | 是 | 拍品 ID |
| itemId | string | 是 | 商品 ID |
| merchantId | string | 是 | 商家 ID |
| title | string | 是 | 拍品标题 |
| startPrice | int64 | 是 | 起拍价，单位分，允许为 0 |
| currentPrice | int64 | 是 | 当前价，单位分 |
| leaderBidderId | string | 否 | 当前领先用户 ID，无领先者为空字符串 |
| status | string | 是 | 拍品状态 |
| startTsMs | int64 | 否 | 实际开始时间戳，毫秒 |
| endTsMs | int64 | 否 | 预计结束时间戳，毫秒 |
| ruleSnapshot | AuctionRuleSnapshot | 是 | 拍卖规则快照 |

### BidDTO

| 字段名 | 类型 | 是否必填 | 含义 |
| --- | --- | --- | --- |
| id | string | 是 | 出价 ID |
| auctionId | string | 是 | 拍品 ID |
| bidderId | string | 是 | 出价用户 ID |
| bidPrice | int64 | 是 | 出价金额，单位分 |
| bidTsMs | int64 | 是 | 出价服务端时间戳，毫秒 |
| requestId | string | 是 | WebSocket 出价请求 ID |
| status | string | 是 | 出价状态：`ACCEPTED`、`REJECTED` |

### RankingItem

| 字段名 | 类型 | 是否必填 | 含义 |
| --- | --- | --- | --- |
| rank | int | 是 | 排名，从 1 开始 |
| bidderId | string | 是 | 用户 ID |
| nicknameMask | string | 是 | 脱敏昵称 |
| bidPrice | int64 | 是 | 出价金额，单位分 |
| bidTsMs | int64 | 是 | 出价时间戳，毫秒 |

### OrderDTO

| 字段名 | 类型 | 是否必填 | 含义 |
| --- | --- | --- | --- |
| id | string | 是 | 订单 ID |
| auctionId | string | 是 | 拍品 ID |
| buyerId | string | 是 | 买家 ID |
| merchantId | string | 是 | 商家 ID |
| amount | int64 | 是 | 订单金额，单位分 |
| status | string | 是 | 订单状态 |
| createdAt | string | 是 | 创建时间，ISO 8601 |
| paidAt | string | 否 | 支付时间，ISO 8601 |

### DepositLedgerDTO

| 字段名 | 类型 | 是否必填 | 含义 |
| --- | --- | --- | --- |
| id | string | 是 | 保证金流水 ID |
| auctionId | string | 是 | 拍品 ID |
| userId | string | 是 | 用户 ID |
| amount | int64 | 是 | 保证金金额，单位分 |
| status | string | 是 | 保证金状态 |
| createdAt | string | 是 | 创建时间，ISO 8601 |
| releasedAt | string | 否 | 释放时间，ISO 8601 |

### AdminUserDTO

| 字段名 | 类型 | 是否必填 | 含义 |
| --- | --- | --- | --- |
| id | string | 是 | 用户 ID |
| nickname | string | 是 | 昵称 |
| role | string | 是 | 角色 |
| status | string | 是 | 账号状态 |
| riskLevel | string | 是 | 风险等级：`LOW`、`MEDIUM`、`HIGH` |
| createdAt | string | 是 | 创建时间，ISO 8601 |

### RiskEventDTO

| 字段名 | 类型 | 是否必填 | 含义 |
| --- | --- | --- | --- |
| id | string | 是 | 风控事件 ID |
| userId | string | 是 | 用户 ID |
| auctionId | string | 否 | 关联拍品 ID |
| riskType | string | 是 | 风险类型 |
| level | string | 是 | 风险级别：`LOW`、`MEDIUM`、`HIGH` |
| status | string | 是 | 事件状态 |
| message | string | 是 | 风险说明 |
| createdAt | string | 是 | 创建时间，ISO 8601 |
| handledAt | string | 否 | 处理时间，ISO 8601 |

---

## 6. 调用示例

### 6.1 完整出价流程

#### 1）curl 登录

```bash
curl -X POST 'https://api.example.com/api/v1/auth/login' \
  -H 'Content-Type: application/json; charset=utf-8' \
  -d '{"account":"buyer001","password":"Passw0rd!","role":"buyer"}'
```

记录返回的 `accessToken`，后续记为 `<buyer_jwt>`。

#### 2）curl 报名

```bash
curl -X POST 'https://api.example.com/api/v1/auctions/auc_1001/enroll' \
  -H 'Authorization: Bearer <buyer_jwt>' \
  -H 'Content-Type: application/json; charset=utf-8' \
  -H 'Idempotency-Key: enroll-demo-001' \
  -d '{"depositPayChannel":"MOCK_PAY"}'
```

#### 3）wscat 连接

```bash
wscat -c 'wss://api.example.com/ws?token=<buyer_jwt>&auctionId=auc_1001'
```

#### 4）发送 bid.place

```json
{
  "type": "bid.place",
  "requestId": "req_demo_bid_001",
  "payload": {
    "auctionId": "auc_1001",
    "bidPrice": 5600,
    "clientTsMs": 1769169900000
  }
}
```

#### 5）接收 bid.ack / ranking.updated

```json
{
  "type": "bid.ack",
  "requestId": "req_demo_bid_001",
  "seq": 201,
  "payload": {
    "accepted": true,
    "auctionId": "auc_1001",
    "bidId": "bid_1001",
    "serverTsMs": 1769169900123
  }
}
```

```json
{
  "type": "ranking.updated",
  "seq": 202,
  "payload": {
    "auctionId": "auc_1001",
    "items": [
      {"rank": 1, "bidderId": "u_1001", "nicknameMask": "用户**01", "bidPrice": 5600, "bidTsMs": 1769169900123}
    ]
  }
}
```

### 6.2 商家落锤示例

```bash
curl -X POST 'https://api.example.com/api/v1/auctions/auc_1001/hammer' \
  -H 'Authorization: Bearer <merchant_jwt>' \
  -H 'Content-Type: application/json; charset=utf-8' \
  -H 'Idempotency-Key: hammer-demo-001' \
  -d '{"hammerRemark":"主播确认成交"}'
```

成功后会返回订单 ID，并向房间推送 `auction.closed` 与 `order.created`。

### 6.3 后台审核示例

```bash
curl -X POST 'https://api.example.com/api/v1/admin/auctions/auc_1001/audit' \
  -H 'Authorization: Bearer <admin_jwt>' \
  -H 'Content-Type: application/json; charset=utf-8' \
  -H 'Idempotency-Key: admin-audit-demo-001' \
  -d '{"auditResult":"APPROVED","reason":"符合平台规范"}'
```

---

## 7. 兼容性与版本演进

- `/api/v1` 路径版本锁定，v1 内不做破坏性变更
- 新增响应字段必须向后兼容，前端应忽略未知字段
- 枚举新增值视为兼容变更，前端需提供默认展示
- 字段删除、字段语义改变、路径改名、必填字段新增等 Breaking change 必须升级至 `/api/v2`
- WebSocket 新增事件类型需保持已有事件语义不变；破坏性协议变更需升级版本

---

## 8. 附录

### 8.1 状态枚举速查

#### 拍品状态 AuctionStatus

| 状态 | 含义 |
| --- | --- |
| DRAFT | 草稿，商家可编辑 |
| PENDING_AUDIT | 待平台审核 |
| READY | 已就绪，可开拍 |
| LIVE | 竞拍中 |
| CLOSED | 已结束 |
| CANCELED | 已取消 |

#### 订单状态 OrderStatus

| 状态 | 含义 |
| --- | --- |
| PENDING_PAY | 待支付 |
| PAID | 已支付 |
| CANCELED | 已取消 |
| REFUNDED | 已退款 |

#### 保证金状态 DepositStatus

| 状态 | 含义 |
| --- | --- |
| INIT | 初始化 |
| FROZEN | 已冻结 |
| RELEASED | 已释放 |
| DEDUCTED | 已抵扣 |
| REFUNDING | 退款中 |
| REFUNDED | 已退款 |

#### 风险事件状态 RiskEventStatus

| 状态 | 含义 |
| --- | --- |
| OPEN | 待处理 |
| PROCESSING | 处理中 |
| RESOLVED | 已处理 |
| IGNORED | 已忽略 |

### 8.2 角色权限速查表

| 能力 | buyer | merchant | admin |
| --- | --- | --- | --- |
| 登录与查看个人信息 | 是 | 是 | 是 |
| 浏览拍品 | 是 | 是 | 是 |
| 报名拍品 | 是 | 否 | 否 |
| WebSocket 出价 | 是 | 否 | 否 |
| 查看自己的订单 | 是 | 否 | 是 |
| 支付订单 | 是 | 否 | 否 |
| 创建/管理商品 | 否 | 是 | 是 |
| 创建/管理自有拍品 | 否 | 是 | 是 |
| 开始/落锤自有拍品 | 否 | 是 | 是 |
| 审核拍品 | 否 | 否 | 是 |
| 管理用户与黑名单 | 否 | 否 | 是 |
| 管理风控规则和事件 | 否 | 否 | 是 |
| 查看平台看板与审计日志 | 否 | 否 | 是 |

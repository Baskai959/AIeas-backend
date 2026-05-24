# 前端 API 回归与后端问题记录

日期：2026-05-24

## 回归范围

本次按商家端前端实际调用路径回归了以下接口：

- 认证：`POST /api/v1/auth/login`、`GET /api/v1/auth/me`
- 商品：`GET /api/v1/items`、`POST /api/v1/items`、`GET /api/v1/items/{id}`、`PATCH /api/v1/items/{id}`
- 拍品：`GET /api/v1/auctions`、`POST /api/v1/auctions`、`GET /api/v1/auctions/{id}`、`PATCH /api/v1/auctions/{id}`、`GET /api/v1/auctions/{id}/state`、`POST /api/v1/auctions/{id}/cancel`
- 直播间：`GET /api/v1/live-rooms`、`GET /api/v1/live-rooms/{id}`、`PATCH /api/v1/live-rooms/{id}`、`GET /api/v1/live-rooms/{id}/lots`、`POST /api/v1/live-rooms/{id}/lots`、`POST /api/v1/live-rooms/{id}/activate`、`POST /api/v1/live-rooms/{id}/deactivate`、`DELETE /api/v1/live-rooms/{id}/lots/{auctionId}`
- 订单：`GET /api/v1/orders`
- 日志：`GET /api/v1/admin/audit-logs`

## 通过项

- 商品列表、创建、详情、编辑均返回 `code=0`。
- 拍品以 `PENDING_AUDIT` 创建、详情查询、规则编辑均返回 `code=0`。
- 拍品经管理员审核后，上架到直播间、开拍、查询实时状态、取消讲解、下架均可调用成功。
- 订单列表返回 `code=0`。

## 需要后端处理的问题

### 1. 开拍时长被后端忽略

处理状态：已修复。`POST /api/v1/live-rooms/{id}/activate` 读取 `durationSec`，大于 0 时以后端当前时间作为 `startTime`，按 `durationSec` 计算 `endTime`；`durationMinutes` 允许传入但不作为计算依据。

前端开拍传参：

```json
{
  "auctionId": 51992240194048,
  "durationMinutes": 10,
  "durationSec": 600
}
```

接口：`POST /api/v1/live-rooms/1/activate`

实际结果：接口返回成功，但最终拍品 `endTime - startTime = 3600 秒`，不是用户选择的 10 分钟。

期望：后端以 `durationSec` 为准计算 `startTime` 与 `endTime`。如果接口不打算接收 `durationMinutes`，可以忽略该字段，但不能忽略 `durationSec`。

### 2. 取消讲解后直播间仍保留已关闭拍品为 activeAuctionId

处理状态：已修复。拍品取消进入终态后会触发直播间 active 清理，释放直播间拍品锁并清空 `activeAuctionId`，直播间状态保持 `LIVE`。

操作链路：

1. `PATCH /api/v1/live-rooms/1` 设置直播间为 `LIVE`
2. `POST /api/v1/live-rooms/1/lots` 上架拍品
3. `POST /api/v1/live-rooms/1/activate` 开拍
4. `POST /api/v1/auctions/{auctionId}/cancel` 取消讲解
5. `GET /api/v1/live-rooms/1`

实际结果：

```json
{
  "status": "LIVE",
  "activeAuctionId": 51992391188992
}
```

同时拍品已经是：

```json
{
  "status": "CLOSED_FAILED",
  "liveRoomId": 1
}
```

期望：取消讲解后拍品应进入终态，直播间应继续 `LIVE`，但 `activeAuctionId` 应清空。直播间没有当前讲解拍品时也应保持可用。

### 3. 当前没有“清空当前拍品但保持直播中”的接口

处理状态：已修复。`POST /api/v1/live-rooms/{id}/deactivate` 调整为“结束当前拍品讲解”：只清空 `activeAuctionId` 并释放锁，直播间保持 `LIVE`。下播仍由直播间状态编辑触发。

调用 `POST /api/v1/live-rooms/{id}/deactivate` 可以清掉当前拍品，但会把直播间状态改成 `OFFLINE`。

期望：后端提供或调整接口，使“结束当前拍品讲解”和“关闭直播间”分离：

- 结束当前拍品：清空 `activeAuctionId`，直播间保持 `LIVE`
- 关闭直播间：明确由下播操作触发，直播间变为 `OFFLINE` 或 `CLOSED`

### 4. 阶梯加价规则仍被当前服务拒绝

处理状态：已确认并补回归。`ladder/steps/min/max/amount` 统一格式在 `PENDING_AUDIT` 状态下可创建成功；`READY` 被拒绝是审核流程限制，前端应继续按 `PENDING_AUDIT` 提交后走管理员审核。

按 `/Users/bytedance/study/AI电商/backend/docs/API/拍品加价规则变更.openapi.json` 发送：

```json
{
  "itemId": 1003,
  "auctionType": "ENGLISH",
  "startPrice": 100,
  "reservePrice": 5000,
  "depositAmount": 0,
  "incrementRule": {
    "type": "ladder",
    "steps": [
      { "min": 0, "max": 1000, "amount": 100 },
      { "min": 1000, "amount": 200 }
    ]
  },
  "antiSnipingSec": 15,
  "antiExtendSec": 30,
  "status": "READY"
}
```

实际结果：`400 / code=20001 / 参数不合法`。

补充：当前服务也拒绝商家直接创建 `READY` 状态拍品。如果这是审核流程要求，前端已按 `PENDING_AUDIT` 提交；但阶梯规则在合法状态下仍需要后端确认当前运行版本是否已合入 openapi 里的新格式。

### 5. 商家端操作日志接口无权限

处理状态：已修复。新增 `GET /api/v1/audit-logs`，merchant/admin 可访问，只返回当前登录账号自己的操作日志；管理员全量查询仍使用 `GET /api/v1/admin/audit-logs`。

前端当前调用：`GET /api/v1/admin/audit-logs?page=1&page_size=5`

商家账号返回：

```json
{
  "code": 10003,
  "message": "无访问权限"
}
```

期望：如果商家端需要展示操作日志，后端应提供商家可访问的日志接口，例如 `GET /api/v1/audit-logs`，只返回当前商家的操作记录；或者明确商家端不开放该模块，前端再移除入口。

# WebSocket 断线重连客户端实现指南

## 目标

客户端需要在网络抖动、服务端发布、Gateway 实例切换、慢连接断开时自动恢复实时拍卖状态，并避免旧消息覆盖新状态。

核心原则：

- 客户端负责自动重连。
- 服务端负责提供 `room.snapshot`、`lastSeq` replay、`room.snapshot_required`。
- 所有服务端增量事件按 `seq` 去重。
- 本地倒计时只用于展示，最终状态以后端事件或 REST state 为准。

## 连接参数

连接地址：

```text
wss://api.example.com/ws/auctions/{auctionId}?token={accessToken}&lastSeq={lastSeq}
```

客户端需要维护：

- `auctionId`
- `accessToken`
- `lastSeq`
- `retryCount`
- `manualClosed`
- `reconnectTimer`

`lastSeq` 建议按拍品维度保存到内存和 `localStorage`：

```text
auction:{auctionId}:lastSeq
```

## 消息处理

每收到一条服务端 envelope：

1. 如果 `seq > 0` 且 `seq <= lastSeq`，直接丢弃。
2. 如果 `seq > lastSeq`，更新本地 `lastSeq`。
3. 根据 `type` 分发业务事件。
4. 如果收到 `room.snapshot_required`，调用 REST 状态接口兜底。

兜底接口：

```http
GET /api/v1/auctions/{auctionId}/state
Authorization: Bearer <accessToken>
```

## 重连策略

使用指数退避 + jitter，避免大量客户端同时重连。

建议参数：

- 初始延迟：500ms
- 最大延迟：10s
- jitter：0 到 `min(1000ms, 当前退避值)`
- 连接成功后重置 `retryCount=0`

示例：

```js
function nextReconnectDelay(retryCount) {
  const base = 500;
  const max = 10000;
  const exp = Math.min(max, base * Math.pow(2, retryCount));
  const jitter = Math.floor(Math.random() * Math.min(1000, exp));
  return exp + jitter;
}
```

## 浏览器实现示例

```js
let ws = null;
let retryCount = 0;
let reconnectTimer = null;
let manualClosed = false;
let lastSeq = Number(localStorage.getItem(`auction:${auctionId}:lastSeq`) || "0");

function connectAuctionWS() {
  const url = new URL(`${WS_BASE}/ws/auctions/${auctionId}`);
  url.searchParams.set("token", accessToken);
  if (lastSeq > 0) url.searchParams.set("lastSeq", String(lastSeq));

  ws = new WebSocket(url.toString());

  ws.onopen = () => {
    retryCount = 0;
    clearReconnectTimer();
  };

  ws.onmessage = async (event) => {
    const env = JSON.parse(event.data);

    if (typeof env.seq === "number" && env.seq > 0) {
      if (env.seq <= lastSeq) return;
      lastSeq = env.seq;
      localStorage.setItem(`auction:${auctionId}:lastSeq`, String(lastSeq));
    }

    if (env.type === "room.snapshot_required") {
      await reloadAuctionState(env.payload.auctionId);
      return;
    }

    if (env.type === "gateway.draining") {
      closeAndReconnectSoon();
      return;
    }

    dispatchEnvelope(env);
  };

  ws.onerror = () => {
    try {
      ws.close();
    } catch {}
  };

  ws.onclose = () => {
    if (!manualClosed) scheduleReconnect();
  };
}

function scheduleReconnect() {
  if (reconnectTimer) return;

  const delay = nextReconnectDelay(retryCount++);
  reconnectTimer = setTimeout(async () => {
    reconnectTimer = null;
    await refreshTokenIfNeeded();
    connectAuctionWS();
  }, delay);
}

function closeAndReconnectSoon() {
  try {
    ws.close();
  } catch {}
  scheduleReconnect();
}

function clearReconnectTimer() {
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
}

function stopWS() {
  manualClosed = true;
  clearReconnectTimer();
  try {
    ws.close();
  } catch {}
}
```

## 业务心跳

浏览器会自动回复协议级 Pong。业务层可以额外每 30s 发送一次：

```js
function sendHeartbeat() {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  ws.send(JSON.stringify({
    type: "heartbeat",
    requestId: `hb-${Date.now()}`
  }));
}
```

如果连续多个业务心跳周期没有收到任何服务端消息，可以主动关闭连接并触发重连。

## 必须处理的服务端事件

- `room.snapshot`
- `room.snapshot_required`
- `bid.accepted`
- `timer.extended`
- `auction.closed`
- `room.online`
- `bid.ack`
- `heartbeat.ack`
- `gateway.draining`
- `error`

## 页面与网络状态

- 页面切到后台时不要频繁断开重连，避免移动端网络恢复后产生重连风暴。
- 浏览器触发 `online` 事件时，如果当前 WebSocket 不可用，可以立即尝试重连。
- 浏览器触发 `offline` 事件时，保留 `lastSeq`，等待网络恢复后重连。
- token 即将过期时优先刷新 token，再建立新 WebSocket。

## 注意事项

- 出价按钮不能只依赖本地倒计时判断拍卖结束。
- 重连后必须携带 `lastSeq`。
- 收到重复或旧 `seq` 必须丢弃。
- `snapshot_required` 表示历史事件补不全，必须拉 REST state。
- `room.snapshot` 是重连后状态对齐的基础，客户端应以其 `seq` 和状态字段刷新当前页面。
- token 过期时先刷新 token，再重连。
- 客户端不应依赖 sticky session；重连到任意 Gateway 实例都应该能恢复。

## lastSeq 持久化与顺序处理

- `lastSeq` 必须按拍品维度持久化（建议内存 + `localStorage` 双写），刷新页面或切换 Gateway 后继续携带。
- 只接受 `seq > lastSeq` 的增量帧；`seq <= lastSeq` 视为重复或乱序旧消息，直接丢弃。
- 如果发现业务状态明显缺口（例如本地价格低于后续事件隐含状态），应主动调用 REST state 重新对齐，而不是回放旧消息覆盖新状态。

## gateway.draining 处理

服务端优雅下线时会向已连接客户端直投：

```json
{"type":"gateway.draining","payload":{"retryAfterMs":5000}}
```

客户端收到后应：

1. 停止发送新的业务消息。
2. 主动关闭当前 WebSocket。
3. 使用 `payload.retryAfterMs` 作为最小等待时间，再叠加少量 jitter 后重连。
4. 重连时继续携带持久化的 `lastSeq`，不要假设会回到同一台 Gateway。

## room.snapshot_required 兜底

当服务端无法完整 replay `lastSeq` 之后的历史窗口时，会下发 `room.snapshot_required`。客户端必须调用：

```http
GET /api/v1/auctions/{auctionId}/state
Authorization: Bearer <accessToken>
```

拿到状态后重建页面基线；后续仍按 WebSocket 增量事件继续更新。不要把 `room.snapshot_required` 当作普通错误忽略。

## JWT 刷新

- 每次重连前检查 access token 过期时间，临近过期时先 refresh，再建立 WebSocket。
- 如果握手阶段返回 401/403，先刷新 token；刷新失败再引导用户重新登录。
- 刷新 token 不应清空 `lastSeq`，否则会扩大服务端 replay 压力。

## 后台标签页与网络切换

- 页面进入后台时不要主动高频断开/重连；浏览器可能节流 timer，退避逻辑应允许延迟执行。
- `offline` 事件触发时保留 `lastSeq` 和 UI 状态，暂停立即重连。
- `online` 事件触发时，如果 WebSocket 非 OPEN，可跳过当前剩余退避并立即尝试一次重连。

## 关闭码矩阵

| 原因 | Close Code | 客户端建议 |
| --- | --- | --- |
| `closed` / `unsubscribe` / `read_closed` / `write_closed` | 1000 | 非手动关闭时按普通退避重连 |
| `slow_consumer` | 1008 | 降低本地处理开销，重新连接后用 `lastSeq` 补齐 |
| `pong_timeout` | 1001 | 网络或后台节流导致，指数退避重连 |
| `gateway_draining` | 1001 | 按 `gateway.draining.retryAfterMs` 等待后重连 |
| 未知内部错误 | 1011 | 退避重连；连续失败应上报客户端日志 |

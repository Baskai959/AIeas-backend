# API 网关与 WebSocket 网关实施提示词

本文档用于交给 AI 工程助手执行网关拆分与 WebSocket 稳定性改造工作。

## 提示词

```text
你是 Go 后端工程代理。请在仓库 /Users/bytedance/study/AI电商/backend 中实现生产化入口拆分方案：API Gateway 入口配置 + 独立 WebSocket Gateway 运行模式 + 客户端断线重连协议配套。

必须先阅读：
1. AGENTS.md
2. docs/拍卖高并发一致性技术方案.md
3. internal/app/server.go
4. internal/transport/http/ws_handler.go
5. internal/transport/ws/hub.go
6. docs/WebSocket接口文档.md

目标：
- API Gateway / Ingress 负责 TLS、健康检查、负载均衡、HTTP 与 WS 路由转发、超时配置。
- API Backend 继续负责 REST 业务、鉴权、出价裁决、订单、保证金、管理后台。
- WebSocket Gateway 独立承载 /ws/* 长连接，负责连接管理、心跳保活、订阅、snapshot、lastSeq replay、Pub/Sub fan-out、慢客户端隔离。
- 自动重连由客户端实现；服务端提供 lastSeq 补偿、snapshot_required、gateway.draining、合理 close code、握手限流和可观测指标。

约束：
- 遵守 AGENTS.md。internal/app/server.go 仍是唯一 composition root；如果新增 cmd/ws-gateway 或 cmd/api-backend，只能调用 app 层构造函数，不要在 cmd 内直接 new repo/service。
- 不要破坏现有 go test ./...。
- 不要修改既有 OpenAPI 文件；如新增接口/协议文档，新增独立文件。
- 保留当前单进程运行模式，新增 gateway/backend 独立运行模式应向后兼容。
- WebSocket Gateway 不应查 MySQL 做复杂业务，不应参与出价最终裁决；出价仍走 Redis Lua 裁决。若保留 WS 上行 bid.place，必须明确它调用 API Backend 或注入 BidService 的边界。
- Gateway 多实例广播不能依赖本机 Hub，必须继续通过 Redis Pub/Sub / Stream / Kafka 事件总线 fan-out。
- 客户端重连不能依赖 sticky session 正确性。

建议实现项：
1. 配置
   - 增加 server/gateway 运行模式配置，例如 APP_ROLE=all|api|ws-gateway 或等价配置。
   - 为 WebSocket 增加可配置项：握手限流、drain 超时、close grace、ping jitter、replay limit。
   - 更新 configs/config.yaml 和 .env.example。

2. app/server.go
   - 抽出/新增构造函数：全量服务、API-only、WS-gateway-only。
   - API-only 不注册 /ws/*。
   - WS-gateway-only 只注册 /ws/*、/healthz、/readyz、/metrics，以及必要鉴权依赖。
   - 继续注入 Hub、OnlineCounter、EventLog、PubSubBroadcaster、RedisReplaySource、metrics。

3. WebSocket 稳定性
   - 服务端 ping 保持现有能力，可增加每连接初始 jitter，避免同一时刻批量 ping。
   - 握手入口增加按 IP / user / auction 的限流，防止重连风暴。
   - 增加 draining 模式：停止接新连接，向旧连接广播 gateway.draining，随后 close code 1012 或 1001。
   - close reason 尽量可观测：read_closed、write_closed、pong_timeout、slow_consumer、gateway_draining。
   - 保留 lastSeq replay；补不全下发 room.snapshot_required。

4. API Gateway / Ingress 示例
   - 新增部署示例文件，例如 deploy/nginx/aieas.conf 或 deploy/k8s/ingress.yaml。
   - /api/*、/mcp/*、/metrics 按需转发到 API Backend。
   - /ws/* 转发到 WebSocket Gateway，并正确设置 Upgrade/Connection 头。
   - LB idle timeout 必须大于 WS pingInterval，建议 >= 90s。
   - 健康检查使用 /healthz 和 /readyz。

5. 文档
   - 更新 docs/WebSocket断线重连客户端实现指南.md。
   - 写清客户端保存 lastSeq、指数退避+jitter、snapshot_required 兜底、token 刷新、去重、防乱序、页面切后台策略。
   - 更新相关 README 或运维文档，说明部署拓扑。

6. 测试
   - 增加 WS handler / hub 单测：lastSeq replay、snapshot_required、draining、握手限流、慢客户端断开。
   - 能跑 go test ./... 和 go vet ./...。
   - 如果无法跑集成依赖，明确说明原因。
```

## 验收清单

- 单进程 `all` 模式仍可运行原有 HTTP 与 WebSocket。
- API-only 模式不注册 `/ws/*`。
- WS-gateway-only 模式只暴露 WebSocket 与运维端点。
- `/ws/*` 可跨 Gateway 实例通过 Redis Pub/Sub / Stream 接收同一拍卖事件。
- 客户端断线后携带 `lastSeq` 重连，能补发缺失事件。
- replay 不完整时客户端收到 `room.snapshot_required` 并能拉 REST 状态兜底。
- 滚动发布时 Gateway 支持 draining，不再接收新连接，并通知旧连接重连。
- 服务端握手限流能缓解重连风暴。
- `go test ./...` 与 `go vet ./...` 通过，或明确列出不可运行原因。

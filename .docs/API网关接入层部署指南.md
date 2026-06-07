# API 网关接入层部署指南

本文档说明生产环境如何通过部署层 API Gateway / Nginx / Ingress / Envoy 提供统一入口。改造目标不是新增 Go API Gateway 进程，而是继续使用同一个 `main.go` / `internal/app/server.go` composition root，通过 `APP_ROLE=api|ws-gateway|all` 运行不同后端池。

## 部署拓扑

```text
Client
  |
  v
API Gateway / Nginx / Ingress / Envoy  (TLS termination, routing, LB, rate limit)
  |-- /api/*    -> aieas_api pool        (APP_ROLE=api)
  |-- /mcp/*    -> aieas_api pool        (APP_ROLE=api)
  |-- /metrics  -> aieas_api pool 或 Prometheus 直接抓取 Pod 内部端点
  |-- /healthz  -> gateway 自身 liveness 或 aieas_api /healthz
  |-- /readyz   -> aieas_api /readyz
  `-- /ws/*     -> aieas_ws_gateway pool (APP_ROLE=ws-gateway)
```

WebSocket 必须始终进入 `APP_ROLE=ws-gateway` 后端池；不要对 `/ws` 开启 sticky session。断线重连正确性依赖客户端携带 `lastSeq`、服务端 replay 与 snapshot 兜底，而不是连接亲和。

## 路由规则

| 入口路径 | 目标后端池 | 说明 |
| --- | --- | --- |
| `/api/*` | `aieas_api` | REST/API 业务入口，包含登录、拍品、直播场次、订单、管理端等。 |
| `/mcp/*` | `aieas_api` | MCP read/control 入口，仅 API backend 暴露。 |
| `/metrics` | `aieas_api` 或 Pod 内部抓取 | 默认示例代理到 API backend；更严格的生产环境可移除公网路由，由 Prometheus 直接抓取各 Pod 内部端点。 |
| `/healthz` | Gateway 自身或 `aieas_api` | liveness：只表示网关/进程存活，不代表业务依赖就绪。 |
| `/readyz` | `aieas_api` | API backend readiness：检查 MySQL、Redis、Redis Lua scripts、Kafka 等业务依赖。 |
| `/ws/*` | `aieas_ws_gateway` | WebSocket Upgrade 与长连接，只进入 ws-gateway 后端池。 |

## 后端角色与环境变量示例

API backend 实例：

```env
APP_ROLE=api
SERVER_ADDR=:8888
MYSQL_DSN=auction:auction@tcp(mysql:3306)/auction?charset=utf8mb4&parseTime=true&loc=Local
REDIS_RT_SHARD_0_ADDR=redis-rt-0:6379
REDIS_CACHE_ADDR=redis-cache:6379
KAFKA_ENABLED=true
KAFKA_BROKERS=kafka-0:9092,kafka-1:9092
OBSERVABILITY_HEALTH_LIVENESS_PATH=/healthz
OBSERVABILITY_HEALTH_READINESS_PATH=/readyz
```

WS gateway 实例：

```env
APP_ROLE=ws-gateway
SERVER_ADDR=:8889
REDIS_RT_SHARD_0_ADDR=redis-rt-0:6379
REDIS_CACHE_ADDR=redis-cache:6379
WEBSOCKET_PING_INTERVAL=20s
WEBSOCKET_PONG_TIMEOUT=60s
WEBSOCKET_DRAIN_TIMEOUT=30s
WEBSOCKET_REPLAY_LIMIT=256
```

`APP_ROLE=ws-gateway` 只注册 `/ws/*` 与运维端点，不注册 `/api/*`、`/mcp/*`，不启动业务 worker，不做 WS 本地出价裁决，也不走 MySQL snapshot fallback。

`APP_ROLE=all` 用于本地开发或兼容单体部署，保留历史 REST/MCP/WS 全量能力。

## Nginx 示例

Nginx 配置见 `deploy/nginx/aieas.conf`，核心要求：

- `upstream aieas_api` 至少包含多个 `APP_ROLE=api` 实例。
- `upstream aieas_ws_gateway` 至少包含多个 `APP_ROLE=ws-gateway` 实例。
- `/ws/` 必须设置 `proxy_http_version 1.1`、`Upgrade`、`Connection`，并保证 `proxy_read_timeout` 与 `proxy_send_timeout` 均 `>= 90s` 且大于 `websocket.pingInterval`。
- 保留 `Host`、`X-Real-IP`、`X-Forwarded-For`、`X-Forwarded-Proto`。
- 不使用 `ip_hash`、sticky cookie 或其他 `/ws` 会话亲和。
- Nginx OSS 没有主动 upstream health check；Kubernetes 场景通过 `readinessProbe` 控制 Pod 是否进入 Service endpoints，VM 场景需由外部 LB 或发布系统摘流。

## Kubernetes Ingress 示例

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: aieas
  annotations:
    nginx.ingress.kubernetes.io/proxy-read-timeout: "90"
    nginx.ingress.kubernetes.io/proxy-send-timeout: "90"
    nginx.ingress.kubernetes.io/proxy-body-size: "20m"
    nginx.ingress.kubernetes.io/limit-rps: "20"
spec:
  tls:
    - hosts: ["api.example.com"]
      secretName: aieas-tls
  rules:
    - host: api.example.com
      http:
        paths:
          - path: /api/
            pathType: Prefix
            backend:
              service:
                name: aieas-api
                port:
                  number: 8888
          - path: /mcp/
            pathType: Prefix
            backend:
              service:
                name: aieas-api
                port:
                  number: 8888
          - path: /ws/
            pathType: Prefix
            backend:
              service:
                name: aieas-ws-gateway
                port:
                  number: 8889
```

## 健康检查与摘流

API backend readiness：

- `/healthz` 是 liveness，只表示进程存活。
- `/readyz` 检查 MySQL、Redis RT、Redis cache、Redis Lua scripts、Kafka 等业务依赖；失败返回 503，网关/LB 应停止分配新 REST/MCP/API 流量。

WS gateway readiness：

- `/readyz` 不检查 MySQL/Kafka，只检查 WS 所需 Redis、scripts、Pub/Sub/Stream 类依赖与 Hub draining 状态。
- Hub 进入 draining 后 `/readyz` 必须返回 503，使网关停止分配新 WebSocket 握手。
- 已建立连接会收到 `gateway.draining`，随后客户端按 lastSeq/replay/snapshot 机制重连到其他实例。

Kubernetes Probe 示例：

```yaml
readinessProbe:
  httpGet:
    path: /readyz
    port: 8889
  periodSeconds: 5
  failureThreshold: 1
livenessProbe:
  httpGet:
    path: /healthz
    port: 8889
  periodSeconds: 10
```

## 滚动发布流程

1. 新版本 Pod 启动并通过 `/readyz` 后加入对应 Service endpoints。
2. 发布系统对旧 ws-gateway 触发优雅终止，应用进入 Hub draining。
3. 旧 ws-gateway `/readyz` 返回 503，Kubernetes readinessProbe 将其从 endpoints 摘除；Nginx OSS 本身不主动探活，依赖 endpoints/LB 摘流。
4. 旧连接收到 `gateway.draining`，在 `WEBSOCKET_DRAIN_TIMEOUT` 内断开。
5. 客户端携带 `lastSeq` 指数退避重连，服务端 replay 缺失事件；replay 不完整时下发 snapshot_required，客户端拉取 REST 状态兜底。
6. drain 超时后进程退出，剩余连接由客户端重连恢复。

## API 网关能力边界

- TLS 终止：证书与 TLS 策略在 Gateway/Nginx/Ingress 层维护。
- 路由隔离：`/api`、`/mcp` 与 `/ws` 后端池分离，WebSocket 不进入 API backend。
- 负载均衡：HTTP 短请求与 WebSocket 长连接分别对不同 upstream 负载均衡；`/ws` 不启用 sticky session。
- 基础限流：网关层对 `/api/` 设置基础 RPS，对登录/出价/订单/直播写操作设置更严格限流；`/ws/` 做粗粒度握手限流。应用内 `HandshakeLimiter` 继续按 IP/user/auction 精细限制。
- 请求体大小：在网关层设置 `client_max_body_size` / Ingress body-size，避免大请求直接打到后端。
- WebSocket Upgrade：显式保留 HTTP/1.1、`Upgrade`、`Connection` 与转发头。
- 健康检查：API backend 与 ws-gateway 分别以 `/readyz` 控制摘流。
- 超时配置：`/ws/` idle timeout 必须大于 `WEBSOCKET_PING_INTERVAL`，生产建议 `>= 90s`。

## 验收清单

- [ ] `APP_ROLE=api` 不暴露 `/ws/*`，但暴露 `/api/*`、`/mcp/*` 与运维端点。
- [ ] `APP_ROLE=ws-gateway` 不暴露 `/api/*`、`/mcp/*`，但暴露 `/ws/*` 与运维端点。
- [ ] `APP_ROLE=all` 保持单体兼容。
- [ ] `/ws/*` 必须路由到 `aieas_ws_gateway`，并支持 WebSocket Upgrade。
- [ ] `/ws` 未开启 sticky session。
- [ ] Nginx/Ingress 的 WebSocket timeout `>= 90s` 且大于 `websocket.pingInterval`。
- [ ] API backend readiness 会因 MySQL/Kafka 等业务依赖失败返回 503。
- [ ] ws-gateway readiness 不因 MySQL/Kafka 探针失败而失败。
- [ ] ws-gateway draining 后 `/readyz` 返回 503。
- [ ] 发布流程能先摘流、再断开旧连接，客户端可通过 lastSeq/replay/snapshot 恢复。

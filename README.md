# AIeas Backend

AIeas Backend 是 AI 电商直播竞拍系统的 Go 后端服务，负责用户认证、直播间、拍品、出价裁决、订单履约、WebSocket 实时同步、MCP 工具接口、Agent 回调、指标与健康检查等核心能力。

## 核心能力

- 直播间与拍品管理：商家创建直播场次、上架/下架拍品、预约开拍、开拍、落槌、下播。
- 实时竞拍：Redis Lua 原子裁决、Kafka 异步出价命令、排行榜、倒计时、防狙击延时、成交/流拍状态机。
- 用户交易闭环：登录鉴权、报名/保证金、出价、订单生成、支付、商家发货、用户确认收货。
- 实时通信：直播间 WebSocket、事件补偿、在线人数、评论、拍品状态、出价结果、排行榜、AI 语音播报。
- Agent/MCP：只读 MCP、控制 MCP、商品描述/审核、直播助手、直播总结报告回调。
- 运维观测：`/healthz`、`/readyz`、Prometheus `/metrics`、结构化日志、OpenTelemetry tracing。

## 技术栈

- Go `1.26.2`
- CloudWeGo Hertz + `hertz-contrib/websocket`
- MySQL 8.x + GORM + Goose migrations
- Redis 7.x：实时状态、Lua 脚本、Stream、分布式锁、在线计数、幂等缓存
- Kafka 3.x：出价事件、异步出价命令、拍卖/订单事件
- Prometheus / Grafana / OpenTelemetry
- Volcengine TOS 对象存储，可按配置关闭

## 项目结构

```text
.
├── main.go                    # 服务入口
├── cmd/db                     # 数据库迁移与开发账号初始化 CLI
├── configs                    # 默认配置，config.yaml / config.docker.yaml
├── migrations                 # Goose SQL 迁移
├── scripts/lua                # Redis Lua 脚本
├── internal
│   ├── app                    # 组合根、依赖装配、路由注册、后台 worker
│   ├── config                 # YAML + .env + 环境变量配置加载
│   ├── domain                 # 领域实体、状态机、领域错误
│   ├── infra                  # MySQL、Redis、Kafka、对象存储、TTS、观测基础设施
│   ├── modules                # 业务模块应用层、端口、仓储实现
│   │   ├── auction            # 拍品、出价、落槌
│   │   ├── live_session       # 直播场次
│   │   ├── order              # 订单与履约
│   │   ├── auth/user/admin    # 账号、权限、后台管理
│   │   ├── mcp/ai             # MCP 与 AI 助手
│   │   └── ...
│   └── transport
│       ├── http               # REST handler、中间件、错误映射
│       ├── ws                 # WebSocket Hub、Client、Envelope、PubSub relay
│       └── mcp                # MCP HTTP transport
├── docs                       # API、DDL、Grafana 面板、设计文档
├── docker                     # Docker 环境变量模板与 MySQL 初始化脚本
└── docker-compose.yml         # 本地/单机部署依赖编排
```

装配规则：具体仓储、服务、Handler、Worker 的构造集中在 `internal/app`，不要在业务模块里新增第二个组合根。

## 环境依赖

本地运行至少需要：

- Go `1.26.2`
- MySQL `8.x`
- Redis `7.x`
- Kafka `3.x`
- 可选：Agent 服务 `/Users/bytedance/study/AI电商/Agent`
- 可选：Volcengine TOS、豆包 TTS 凭证

默认端口：

| 服务 | 默认地址 |
| --- | --- |
| Backend HTTP / REST / MCP / WS | `http://127.0.0.1:8888` |
| MySQL | `127.0.0.1:3306` |
| Redis cache | `127.0.0.1:6380` |
| Redis RT shard 0 | `127.0.0.1:6381` |
| Redis RT shard 1 | `127.0.0.1:6382` |
| Kafka | `127.0.0.1:9092` |
| Kafka UI | `http://127.0.0.1:8082` |
| Prometheus | `http://127.0.0.1:9090` |
| Grafana | `http://127.0.0.1:3000` |

## 配置说明

配置加载顺序：

```text
internal/config.Default()
→ configs/config.yaml 或 -config 指定文件
→ .env
→ 进程环境变量中的白名单密钥/凭证
```

约定：

- 普通配置写在 `configs/config.yaml`，例如端口、MySQL/Redis/Kafka 地址、业务开关、超时、topic。
- 密钥和凭证写在 `.env`，不要提交真实 `.env`。
- 环境变量只覆盖配置代码白名单里的密钥/凭证，不覆盖 MySQL DSN、Kafka broker、服务端口等普通配置。
- 本地可复制模板：

```bash
cp .env.example .env
```

常用密钥变量：

```env
JWT_SECRET=replace-with-a-strong-local-secret
REDIS_RT_SHARD_0_PASSWORD=
REDIS_RT_SHARD_1_PASSWORD=
REDIS_CACHE_PASSWORD=
OBJECT_STORAGE_ACCESS_KEY=
OBJECT_STORAGE_SECRET_KEY=
AGENT_LIVE_ANALYSIS_CALLBACK_API_KEY=
DOUBAO_TTS_APP_ID=
DOUBAO_TTS_ACK_TOKEN=
MCP_READ_API_KEY=
MCP_CONTROL_API_KEY=
OBSERVABILITY_METRICS_AUTH_TOKEN=
```

生产环境必须替换 `JWT_SECRET`、MCP API key、metrics token、对象存储和 TTS 凭证。

本地没有对象存储凭证时，需要把 `configs/config.yaml` 里的 `objectStorage.enabled` 改为 `false`；否则启动校验会要求 `OBJECT_STORAGE_ACCESS_KEY` 和 `OBJECT_STORAGE_SECRET_KEY`。

## 本地启动

### 1. 启动依赖

推荐先用 Docker Compose 启动 MySQL、Redis、Kafka：

```bash
docker compose up -d mysql redis-rt-1 redis-rt-2 redis-cache kafka kafka-init kafka-ui
```

确认依赖健康：

```bash
docker compose ps
```

如果你使用本机已有 MySQL/Redis/Kafka，修改 `configs/config.yaml` 中对应地址即可。

### 2. 安装依赖并编译

```bash
go mod download
go build ./...
```

### 3. 执行数据库迁移

```bash
go run ./cmd/db -config configs/config.yaml migrate status
go run ./cmd/db -config configs/config.yaml migrate up
```

### 4. 初始化开发账号

```bash
go run ./cmd/db -config configs/config.yaml seed-dev
```

默认账号：

| 角色 | 账号 | 密码 |
| --- | --- | --- |
| 买家 | `buyer001` | `Passw0rd!` |
| 商家 | `merchant001` | `Passw0rd!` |
| 管理员 | `admin001` | `AdminPassw0rd!` |

### 5. 启动后端

启动前确认：

- `.env` 中已设置 `JWT_SECRET`。
- 如果 `objectStorage.enabled=true`，已设置 TOS access key 和 secret key；否则先改为 `false`。
- 如果不启动 Agent 服务，商品描述、商品审核、直播分析、直播助手相关功能会在调用时失败或降级，主竞拍链路仍可联调。

```bash
go run . -config configs/config.yaml
```

健康检查：

```bash
curl http://127.0.0.1:8888/healthz
curl http://127.0.0.1:8888/readyz
```

登录示例：

```bash
curl -X POST http://127.0.0.1:8888/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"account":"buyer001","password":"Passw0rd!"}'
```

## Docker Compose 启动完整环境

复制 Docker 环境变量模板：

```bash
cp docker/prod.env.example docker/prod.env
```

按需修改 `docker/prod.env`，然后启动：

- 必须替换 `JWT_SECRET`、MCP API key、metrics token。
- 如果保留 `configs/config.docker.yaml` 中的 `objectStorage.enabled=true`，必须填写 `OBJECT_STORAGE_ACCESS_KEY` 和 `OBJECT_STORAGE_SECRET_KEY`；否则先在 `configs/config.docker.yaml` 里关闭对象存储。
- `docker/prod.env` 中的 `MYSQL_ROOT_PASSWORD` 要和 `configs/config.docker.yaml` 的 `mysql.dsn` 密码一致。

```bash
docker compose up -d --build
```

服务地址：

- 后端：`http://127.0.0.1:8888`
- Kafka UI：`http://127.0.0.1:8082`
- Prometheus：`http://127.0.0.1:9090`
- Grafana：`http://127.0.0.1:3000`

查看日志：

```bash
docker compose logs -f app
```

Compose 的 MySQL 容器首次初始化会加载 `docker/mysql-init/001_init.sql` 和 `docs/ddl.sql`。后续新增迁移建议在宿主机执行：

```bash
go run ./cmd/db -config configs/config.yaml migrate status
go run ./cmd/db -config configs/config.yaml migrate up
```

执行前确认 `configs/config.yaml` 指向 Docker 暴露出来的 MySQL 地址。

## 常用命令

| 任务 | 命令 |
| --- | --- |
| 编译 | `go build ./...` |
| 启动 | `go run . -config configs/config.yaml` |
| 全量测试 | `go test ./...` |
| Race 测试 | `go test -race ./...` |
| 单包测试 | `go test ./internal/transport/http -run TestName -v` |
| 格式化 | `gofmt -w .` |
| Vet | `go vet ./...` |
| 迁移状态 | `go run ./cmd/db -config configs/config.yaml migrate status` |
| 执行迁移 | `go run ./cmd/db -config configs/config.yaml migrate up` |
| 回滚一版 | `go run ./cmd/db -config configs/config.yaml migrate down` |
| 初始化开发账号 | `go run ./cmd/db -config configs/config.yaml seed-dev` |

## API 与实时协议

REST 基础路径：

```text
/api/v1
```

主要路由：

- 认证：`POST /auth/login`、`POST /auth/refresh`、`GET /auth/me`、`POST /auth/logout`
- 拍品：`/auctions`、`/auctions/:id/state`、`/auctions/:id/ranking`、`/auctions/:id/enroll`、`/auctions/:id/hammer`
- 直播场次：`/live-sessions`、`/live-sessions/:id/lots`、`/live-sessions/:id/stats`、`/live-sessions/:id/start`、`/live-sessions/:id/end`
- 订单：`/orders`、`/orders/mine`、`/orders/:id/pay`、`/orders/:id/ship`、`/orders/:id/receive`
- 市场搜索：`/search/lots`、`/categories`、`/merchants/:id`
- AI/MCP：`/ai-assistant/*`、`/mcp/read`、`/mcp/control`、`/live-analysis/*`

WebSocket：

```text
GET /ws/live-rooms/:room_id
GET /ws/live-sessions/:session_id
GET /ws/auctions/:auction_id
```

常见事件：

- `room.snapshot` / `room.online`
- `lot.updated` / `lot.removed`
- `bid.ack` / `bid.result` / `bid.accepted`
- `ranking.updated`
- `chat.message`
- `live.voice_broadcast`
- `ai.status`

前端联调建议走同源反代：

```text
前端 /api  → Backend /api
前端 /ws   → Backend /ws
```

这样可以避免浏览器 CORS 和 WebSocket 跨域问题。

## Agent 联调

后端默认通过 `configs/config.yaml` 调用 Agent：

```text
http://127.0.0.1:8000/api/v1/product-description
http://127.0.0.1:8000/api/v1/product-audit
http://127.0.0.1:8000/api/v1/live-analysis/async
http://127.0.0.1:8000/api/v1/live-assistant
```

本地启动 Agent：

```bash
cd /Users/bytedance/study/AI电商/Agent
uv run python main.py
```

如果只想先跑通后端主流程，可以在 `configs/config.yaml` 中临时关闭或调整：

- `agent.productAuditEnabled`
- `agent.productDescriptionURL`
- `agent.liveAuctionHookURL`
- `agent.liveAnalysisURL`
- `doubaoTTS.*`

## 观测与健康检查

- `GET /healthz`：进程存活检查。
- `GET /readyz`：MySQL、Redis、Lua 脚本等依赖就绪检查。
- `GET /metrics`：Prometheus 指标，默认需要 `Bearer` token 或 `X-Metrics-Token`。
- 日志格式由 `observability.format` 控制，开发建议 `text`，生产建议 `json`。
- Tracing 默认关闭，开启时配置 `observability.tracing.enabled=true` 和 OTLP endpoint。

## 开发注意事项

- 所有写接口尽量带 `Idempotency-Key`，后端会对关键写操作做幂等保护。
- 金额统一使用整数分，不使用浮点数。
- 新增数据库字段必须新增 `migrations/000xx_*.sql`，不要修改已应用迁移。
- 新增 API 文档时在 `docs/API/<功能名>.openapi.json` 新建独立文件。
- WebSocket 给用户端同步的业务变更应按直播间维度广播，并通过网关/Redis 事件传播，不要只打到本进程 Hub。
- 多实例竞价依赖 Redis Lua、Kafka command、in-flight drain 和落锤屏障保证一致性，改出价链路前先补测试。

## 常见问题

### 访问接口返回“访问令牌无效或已过期”

检查请求是否带了：

```http
Authorization: Bearer <accessToken>
```

如果 token 过期，调用 `POST /api/v1/auth/refresh` 获取新的 access token。

### 浏览器报 CORS

开发期推荐前端 Vite/Nginx 同源反代 `/api` 和 `/ws`。如果前端直连后端，确认 `server.cors.allowOrigins` 允许当前 Origin，且预检请求能返回 CORS 响应头。

### WebSocket 收不到跨实例广播

确认 Redis、Kafka、PubSub relay、进程角色配置正常。多实例部署时，用户连接通常在 WS 网关进程，业务进程不能只调用本进程 Hub，需要通过实时事件总线广播。

### Kafka topic 不存在

Docker Compose 会通过 `kafka-init` 自动创建 topic。手动检查：

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 127.0.0.1:9092 \
  --list
```

### 数据库字段不存在

先查看迁移状态，再执行迁移：

```bash
go run ./cmd/db -config configs/config.yaml migrate status
go run ./cmd/db -config configs/config.yaml migrate up
```

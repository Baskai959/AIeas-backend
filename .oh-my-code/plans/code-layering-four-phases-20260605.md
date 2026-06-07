# 代码分层改造四阶段实施计划

## 背景与边界

- 来源文档：`docs/代码分层改造方案.md`。
- 该文档包含“第 0 阶段：冻结边界”和后续 4 个实施阶段。本文按用户要求提取并规划四个实施阶段：第 1～4 阶段；第 0 阶段作为前置约束，不要求额外改代码。
- 项目约束：Go + Hertz；`internal/app/server.go` 保持唯一 composition root；服务 wiring 仍在 `internal/app/server.go`；不新增其他 composition root；优先扩展/拆分现有 app 内 wiring；当前工作区已有大量前序修改，实施需保守、分批、每阶段独立可验证。

## 阶段概览

| 阶段 | 名称 | 目标 | 文档验收标准 | 推荐顺序 |
| --- | --- | --- | --- | --- |
| 第 1 阶段 | 拆轻量装配函数 | 降低 `internal/app/server.go` 单函数复杂度，不改变包结构 | `NewServerWithConfig` 行数明显减少；`NewServerWithDependencies` 仍保留测试入口；`go test ./...`、`go vet ./...` 通过 | 先做 |
| 第 2 阶段 | 接口收敛 | handler 和 worker 依赖 use case 接口，不再依赖完整 service struct | handler 测试不需要构造完整 service 图；`go test ./internal/transport/http ./internal/service` 通过 | 依赖阶段 1 |
| 第 3 阶段 | 构造函数依赖显式化 | 减少 `SetXxx`，避免半初始化 service | `server.go` 中连续 `SetXxx` 数量下降；核心 service 构造后即可使用；单测不依赖调用顺序补齐 service | 依赖阶段 2 |
| 第 4 阶段 | 按业务模块迁移文件 | 逐步形成 `internal/modules/<module>` 业务模块目录 | 每个模块迁移后跑相关包测试；全量 `go test ./...`、`go vet ./...` 通过 | 最后做，且按模块拆小批 |

## 当前代码发现

- `internal/app/server.go` 是唯一 app wiring 文件，当前还包含平台依赖打开、仓储构造、服务构造、worker 启动、handler 构造与路由注册。
- `NewServerWithConfig` 位于 `internal/app/server.go:55`，包含 logger/metrics/tracing、Redis/MySQL/Kafka 客户端、仓储与 `ServerDependencies` 构造。
- `NewServerWithDependencies` 位于 `internal/app/server.go:288`，包含 memory fallback、service 构造、`SetXxx` 后注入、worker 启动和最终调用 `newServerWithServices`。
- `newServerWithServices` 位于 `internal/app/server.go:681`，当前既构造 handler，也注册 API/MCP/WS 路由，并按 role 控制 API/WS 路由。
- APP_ROLE 判断函数已有：`shouldRegisterAPIRoutes`、`shouldRegisterWSRoutes`、`shouldStartBusinessWorkers`、`shouldStartWSConsumers`，位于 `internal/app/server.go:199-213`。
- 当前 handler 构造函数直接依赖 concrete service：例如 `NewAuctionHandler`、`NewLiveSessionHandler`、`NewOrderHandler`、`NewWSHandler` 等位于 `internal/transport/http/*_handler.go`。
- 当前目录仍是横向包：`internal/domain`、`internal/service`、`internal/repository`、`internal/infra`、`internal/transport`；尚无 `internal/modules/`。
- 已有 role/readyz 相关测试在 `internal/app/server_test.go`，覆盖 all/api/ws-gateway 路由、worker 与 readiness probe 行为。

## 分阶段执行计划

### Step 1: 第 1 阶段——拆轻量装配函数

**What:** 在不改变包结构和业务行为的前提下，把 `internal/app/server.go` 内的 wiring 拆到同包轻量函数。建议只在 `internal/app` 内新增/扩展如下文件：`platform_wiring.go`、`repository_wiring.go`、`service_wiring.go`、`worker_wiring.go`、`route_wiring.go`。保留 `NewServerWithConfig`、`NewServerWithDependencies`、`NewServerWithUserRepository`、`NewServerWithAuth` 对外签名不变。先抽纯搬运函数：平台客户端与观测构造、仓储 deps 构造、service deps 构造、worker 启停、handler/route 注册。

**Agent:** executor

**References:**
- `docs/代码分层改造方案.md:321-351`（阶段名称、目标、建议拆分、验收）
- `internal/app/server.go:55-185`（`NewServerWithConfig` 平台/仓储构造）
- `internal/app/server.go:288-557`（`NewServerWithDependencies` fallback/service/worker）
- `internal/app/server.go:681-913`（handler 构造与路由注册）

**MUST NOT:**
- 不改 `internal/app/server.go` 作为唯一 composition root 的事实；新增文件只能是 `package app` 内部 wiring helper。
- 不新增 `cmd/api-backend`、`cmd/ws-gateway` 或其他启动入口。
- 不改业务逻辑、不改 API/WS 协议、不改 AGENTS.md。
- 不引入 `internal/platform` 或 `internal/modules` 目录；阶段 1 只做函数拆分。

**Verify:**
- `go test ./internal/app ./internal/transport/http ./internal/transport/ws`
- `go test ./...`
- `go vet ./...`
- 人工/脚本检查：`NewServerWithConfig`、`NewServerWithDependencies`、`NewServerWithUserRepository`、`NewServerWithAuth` 函数签名未变化；`internal/app/server_test.go` 中 APP_ROLE/readyz 用例仍通过。

**Parallel:** 必须最先执行；后续阶段依赖本阶段拆出的 service/route/worker wiring 边界。阶段内可并行拆 `platform/repository` 与 `route`，但最终合并时需串行处理 `ServerDependencies` 数据流。

**测试策略:** 以回归为主。优先跑 `internal/app` 角色路由与 readiness 测试，再跑全量；不要求新增大量测试，除非拆分中引入了新的可测 helper 或发现现有覆盖不足。

**风险与保守边界:** 最大风险是搬运中改变 shutdown 顺序、worker 启动条件或 role route gating。保守做法是只抽函数、不改条件表达式、不改依赖构造顺序；所有 close/shutdown hook 保持原顺序。

### Step 2: 第 2 阶段——接口收敛

**What:** 为核心 handler/worker 引入最小 use case 接口，使 handler 依赖接口而不是完整 concrete service。按“先定义接口、concrete service 原地实现、再切 handler 构造函数”的顺序执行。接口优先放在现有包内以降低 import churn：可在 `internal/service` 定义 use case 接口，或在 `internal/transport/http` 定义 handler 所需窄接口；不急于迁入 `modules/<name>/ports`。优先处理高价值边界：`AuthUseCase`、`AuctionCommandUseCase`、`AuctionQueryUseCase`、`BidUseCase`、`HammerUseCase`、`LiveSessionCommandUseCase`、`LiveSessionQueryUseCase`、`OrderUseCase`、`RiskUseCase`。第一批建议只落地 auction/live_session/ws/order 相关窄接口，避免一次性覆盖所有 handler。

**Agent:** deep-executor

**References:**
- `docs/代码分层改造方案.md:353-376`（阶段名称、目标、接口清单、验收）
- `docs/代码分层改造方案.md:246-250`（Auction 模块阶段目标）
- `docs/代码分层改造方案.md:274-278`（Live Session 与 WS gateway 端口重点）
- `internal/transport/http/auction_handler.go`、`live_session_handler.go`、`order_handler.go`、`ws_handler.go`（当前 handler 依赖 concrete service）
- `internal/app/server.go:777-790`（handler 构造和 WS handler 后注入）

**MUST NOT:**
- 不为了接口收敛移动文件到 `internal/modules`；不创建大而全的 ports 包。
- 不改 service 方法行为、不改 DTO JSON 字段、不改鉴权/幂等中间件。
- 不让 handler 直接访问 repository、Redis、MySQL 或 infra。
- 不删除旧 concrete service；只让其满足接口。

**Verify:**
- `go test ./internal/transport/http ./internal/service`
- `go test ./internal/app -run 'Test.*Role|Test.*Ready|Test.*WS|Test.*Route'`
- `go test ./...`
- `go vet ./...`
- 编译检查：handler 构造函数参数类型不再是相关完整 `*service.XService`，而是窄接口；但 `internal/app/server.go` 仍由 concrete service 完成注入。

**Parallel:** 依赖 Step 1。阶段内可按 handler 分批并行：auction/order、live_session/ws 可并行，但涉及 `WSHandler` 与 `LiveSessionService`/`BidService` 的接口边界时需合并协调，避免接口重复或循环依赖。

**测试策略:** 增补/调整 handler 单测，使用 fake use case 覆盖参数解析、鉴权、错误翻译；service 测试继续用 memory repo。若第一批只切 auction/live_session/ws/order，则只新增这些 handler 的 fake 测试，不强制全 handler 一次性改造。

**风险与保守边界:** 风险是接口过宽导致只是换名，或接口放置位置导致循环依赖。保守做法是接口按 handler 实际调用方法裁剪；优先同包窄接口，后续模块迁移时再移动到 `modules/<name>/ports`。

### Step 3: 第 3 阶段——构造函数依赖显式化

**What:** 对高风险 service 逐步引入 `Deps` 结构体构造，减少核心依赖的 `SetXxx` 后注入。第一批仅处理方案点名的 `AuctionService`、`BidService`、`HammerService`、`LiveSessionService`，且每次只改一个 service：新增 `XServiceDeps`，保留旧构造函数作为薄包装或同步迁移调用点；将 repository、realtime、tx、publisher、timer、lock、ID generator、metrics 等核心依赖构造时传入。允许保留少量运行时 hook/optional hook，例如测试替换、`OnClose`、可选 agent hook、metrics 等，但核心必需依赖不得再依赖调用顺序补齐。

**Agent:** deep-executor

**References:**
- `docs/代码分层改造方案.md:377-395`（阶段名称、目标、服务清单、验收）
- `docs/代码分层改造方案.md:147-160`（Deps 构造示例）
- `internal/app/server.go:397-452`（当前核心 service 连续 `SetXxx` 后注入）
- `internal/app/server.go:574-620`（`NewServerWithAuth` 测试入口重复后注入）
- `internal/service/auction.go`、`bid.go`、`hammer.go`、`live_session.go`

**MUST NOT:**
- 不一次性改所有 service；不改业务规则或状态机。
- 不移除必要的测试入口；`NewServerWithDependencies` 的 memory fallback 仍保留。
- 不把 service 构造逻辑移出 `internal/app/server.go` 之外的 composition root；helper 仍在 `internal/app`。
- 不为了“减少 SetXxx”删除必要的 hook，例如 auction/hammer close 回调需保持行为。

**Verify:**
- 单 service 改造后先跑对应包：`go test ./internal/service -run 'Auction|Bid|Hammer|LiveSession'`
- `go test ./internal/app ./internal/transport/http`
- `go test ./...`
- `go vet ./...`
- 人工/脚本检查：`internal/app/server.go` 中 `auctionService.Set*`、`bidService.Set*`、`hammerService.Set*`、`liveSessionService.Set*` 连续后注入数量下降；核心 service 构造后无需额外调用必需依赖 setter 即可工作。

**Parallel:** 依赖 Step 2。阶段内建议串行：AuctionService → HammerService → BidService → LiveSessionService。原因是它们存在 close 回调、timer、live session 激活、bid/hammer 互调等隐藏依赖。

**测试策略:** 每个 service 改造配套更新现有 service 单测夹具；保留 memory repo hermetic 测试。涉及 Bid/Hammer/LiveSession 并发和实时状态时追加 `go test -race ./internal/service ./internal/transport/ws`。

**风险与保守边界:** 最大风险是构造顺序改变导致 timer、OnClose、LiveSession 激活链路回归。保守做法是先新增 Deps 构造并让旧构造代理到新构造，调用点逐步迁移；每迁移一个 service 即提交/验证，不和阶段 4 文件移动混做。

### Step 4: 第 4 阶段——按业务模块迁移文件

**What:** 在前三阶段接口和构造边界稳定后，再按模块小步迁移到 `internal/modules/<name>`。推荐顺序与文档一致：`live_session` → `auction` → `order/deposit` → `risk` → `auth/marketplace/ai`。每个模块内部严格按“先 ports、concrete service 留原包实现新接口、handler 切接口、adapter 实现迁移、最后移动 domain/app 文件”的顺序。首批只建议落地 `live_session` 的最小迁移：`ports` 中放 `LiveSessionRealtimeReader`/`SessionLock`/查询端口，服务实现暂留 `internal/service`，WS handler 只依赖 realtime reader 端口，为后续 ws-gateway 启动期去 MySQL 铺路。

**Agent:** deep-executor

**References:**
- `docs/代码分层改造方案.md:396-419`（阶段名称、目标、模块推荐顺序、每模块迁移步骤、验收）
- `docs/代码分层改造方案.md:216-250`（Auction 模块边界与迁移对象）
- `docs/代码分层改造方案.md:252-278`（Live Session 模块边界与迁移对象）
- `docs/代码分层改造方案.md:421-439`（API/WS gateway 未来 dependency graph 收益）
- 当前候选文件：`internal/domain/*`、`internal/service/*`、`internal/repository/*`、`internal/infra/redis/*`、`internal/transport/http/*_handler.go`

**MUST NOT:**
- 不一次性搬完整目录树；不做全仓 import 大迁移。
- 不在阶段 4 同时做大规模业务逻辑重写、schema 变更、OpenAPI 改动或部署配置改动。
- 不新增 composition root；`internal/app/server.go` 仍统一 new concrete adapter/service 并注册路由/worker。
- 不让 `domain` 依赖 repository/service/infra/transport/config；不让 `ports` 依赖 MySQL/Redis/Kafka/Hertz。
- 不改 AGENTS.md，不吸收无关长文档。

**Verify:**
- 每迁移一个模块先跑相关包，例如 live_session：`go test ./internal/service ./internal/repository ./internal/infra/redis ./internal/transport/http ./internal/app`
- auction 模块涉及实时/worker 时：`go test -race ./internal/transport/ws ./internal/service`
- 每个模块完成后：`go test ./...`
- 每个模块完成后：`go vet ./...`
- 结构检查：新增 `internal/modules/<name>/ports` 后，不存在 ports 反向 import `internal/infra`、`internal/transport`、Hertz、GORM、Redis 客户端。

**Parallel:** 依赖 Step 3。模块之间原则上串行，先 `live_session` 再 `auction`；`risk`、`auth`、`marketplace` 可在 live_session/auction 稳定后并行评估，但不建议在当前大量前序修改工作区中并行落地。

**测试策略:** ports 用 fake/memory adapter；handler 用 mock use case；adapter 用现有 memory 或 Redis/MySQL 集成测试。保留现有 MySQL + Redis 集成测试；后续如真正实现 ws-gateway 启动期不打开 MySQL，需要增加 role-based 装配测试：`APP_ROLE=ws-gateway` 不构造 MySQL/Kafka，`APP_ROLE=api` 不构造 WS-only worker，`APP_ROLE=all` 兼容。

**风险与保守边界:** 最大风险是文件移动造成 import 大面积冲突、循环依赖和工作区前序修改冲突。保守做法是先复制/定义 ports，不移动 domain/app；只在一个模块验证绿后再迁移下一个模块；必要时在阶段 4 的首个 MR/commit 中只新增 ports 与适配，不搬文件。

## 阶段依赖图

```text
第 0 阶段（冻结边界，文档/规范，前置约束）
    ↓
第 1 阶段：拆轻量装配函数
    ↓
第 2 阶段：接口收敛
    ↓
第 3 阶段：构造函数依赖显式化
    ↓
第 4 阶段：按业务模块迁移文件
```

推荐执行顺序必须串行。阶段 1 降低 `server.go` 修改冲突；阶段 2 让 handler/worker 依赖接口；阶段 3 固化构造依赖；阶段 4 才进行模块目录迁移。

## 总体验收标准

- 每阶段必须通过：
  - `go test ./...`
  - `go vet ./...`
- 涉及并发、Hub、Redis Stream、WS gateway 的阶段额外通过：
  - `go test -race ./internal/transport/ws ./internal/service`
- APP_ROLE 行为保持：
  - `all` 注册 API + WS，启动 business workers + WS consumers。
  - `api` 注册 API，不注册 WS，不启动 WS consumers。
  - `ws-gateway` 注册 WS，不注册 API/MCP，不启动 business workers。
- composition root 保持：所有 concrete repo/service/adapter/handler/worker wiring 仍由 `internal/app/server.go` 及同包 helper 发起。

## 明确不应做

- 不新增任何新的 composition root 或启动入口：不新增 `cmd/api-backend`、`cmd/ws-gateway` 等。
- 不把 service wiring 移出 `internal/app/server.go` 管控范围；可拆同包 helper，但不引入第二装配中心。
- 不修改 AGENTS.md。
- 不做无关业务逻辑改动，不改 API/WS 协议，不改 JSON 字段。
- 不做 schema/migration/OpenAPI/部署配置改动，除非后续阶段明确需要且单独验收。
- 不一次性建立完整 `internal/modules` 大树并搬全量文件。
- 不让 handler 直接访问 repository/infra，不让 domain 依赖外部层，不让 ports 依赖具体技术库。
- 不在已有大量前序修改基础上扩大无关范围；每阶段、每模块小步提交与验证。

## 主要风险与回退策略

| 风险 | 易发阶段 | 控制/回退 |
| --- | --- | --- |
| `server.go` 拆分改变启动、shutdown 或 role gating 顺序 | 阶段 1 | 只搬运，不改表达式；保留原 shutdown hook 顺序；失败时回退单个 helper 抽取 |
| 接口定义过宽或位置不当导致循环依赖 | 阶段 2 | 用 handler 所需最小接口；优先同包或 `service` 包窄接口；模块 ports 留到阶段 4 |
| 减少 `SetXxx` 时改变 service 初始化语义 | 阶段 3 | 一个 service 一次；旧构造函数代理新 Deps 构造；保留 optional hook |
| 文件移动引发 import 大面积冲突 | 阶段 4 | 先 ports 后迁移；先 live_session 最小闭环；不和业务重写混做 |
| WS gateway 回归或重新引入 MySQL 依赖 | 阶段 4 | 保留/增强 APP_ROLE、readyz、snapshot fallback、bid.place disabled 测试 |


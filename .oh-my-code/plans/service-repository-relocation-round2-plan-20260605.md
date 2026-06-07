# 第二轮逐模块搬迁 service/repository 实施计划（2026-06-05）

## 目标与验收

- 目标：在第一轮已完成的 `internal/modules/<module>/{app,ports}` 边界基础上，逐模块把 service / repository concrete implementation 迁入模块内部。
- 约束：`internal/app` 继续作为唯一 composition root；不新增 `cmd/api-backend` / `cmd/ws-gateway`；不改 `AGENTS.md`；不改 `docs/API`；不新增 migration、不改 schema；不改公开 REST / WS / MCP 协议。
- 全局验收命令：
  - `go test ./...`
  - `go vet ./...`
  - `git diff --name-only -- AGENTS.md docs/API migrations cmd`
  - 关键 smoke：`go test ./internal/app ./internal/transport/http ./internal/transport/mcp ./internal/transport/ws`

## 现状依据

- composition root 已拆到 `internal/app/server.go:42-137`、`internal/app/repository_wiring.go:27-74`、`internal/app/service_wiring.go:147-277`，但 concrete 仍主要在 `internal/service` 与 `internal/repository`。
- 第一轮边界已就位：`internal/modules/{auth,deposit,risk,marketplace,admin,ai,live_analysis,mcp,auction,live_session,order}/{app,ports}`。
- 已有过渡别名先例：`internal/repository/live_session.go:15-18`、`internal/repository/live_session_lock.go:11-13`、`internal/repository/live_session_realtime.go:11-18`。
- 当前高耦合中心仍在 `internal/app/service_wiring.go:153-277`：`AuctionService`、`HammerService`、`LiveSessionService`、`BidService`、`AdminService`、`MCPReadService`、`MCPControlService` 彼此交织。

## 推荐迁移顺序（按批）

1. **Batch 1：auth + live_analysis**
2. **Batch 2：ai + risk**
3. **Batch 3：deposit + marketplace**
4. **Batch 4：admin/support（config / feature_flag / audit / dashboard）**
5. **Batch 5：mcp facade（read + control）**
6. **Batch 6：core realtime/trade（order → live_session → auction/bid/hammer/workers）**

排序理由：先做低耦合、低事务、低实时性的叶子模块，先验证“模块内 service + module-owned repository + app wiring shim”的迁移手法；再逐步处理带黑名单/报名/聚合查询的中耦合模块；最后才处理 auction/live_session/order/bid/hammer 这一组强耦合、跨事务、跨 realtime worker 的核心链路。

## 目标目录建议

### 通用规则

- 小模块优先：
  - `internal/modules/<module>/app/service.go`
  - `internal/modules/<module>/app/dto.go`
  - `internal/modules/<module>/app/errors.go`
  - `internal/modules/<module>/ports/ports.go`
  - `internal/modules/<module>/app/*_test.go`
- 拥有持久化实现的模块再增加：
  - `internal/modules/<module>/repository/mysql.go`
  - `internal/modules/<module>/repository/memory.go`
  - `internal/modules/<module>/repository/model.go`
  - `internal/modules/<module>/repository/*_test.go`
- 拥有 worker / realtime adapter 的模块增加：
  - `internal/modules/<module>/runtime/*.go` 或 `internal/modules/<module>/repository/realtime_*.go`

### 模块分型建议

- **仅迁 service、不新增 module repository**：`auth`、`ai`、`marketplace`、`mcp`
  - 原因：它们当前不拥有独立表或“唯一真源”仓储；主要消费其它模块 ports 或 shared support repo。
- **迁 service + module-owned repository**：`live_analysis`、`risk`、`deposit`、`order`、`live_session`、`auction`、`admin/support`
- **shared support 暂留 `internal/repository`**：`user_*`、`tx.go`、通用 helper、无 owner 的共享适配。

## composition root 保持方式

- 所有 concrete 仍由 `internal/app` 构造，模块内部只暴露构造函数，不做自装配。
- `internal/app/repository_wiring.go` 负责 new concrete repository / adapter；`internal/app/service_wiring.go` 负责 new module app service；`internal/app/worker_wiring.go` 负责 start worker。
- 迁移期允许 `internal/repository` 保留 **alias / constructor shim**，用于给未迁完调用方继续提供稳定入口；但 shim 只做转发，不再承载业务实现。
- `ServerDependencies` 不要一次性重写；按批把字段从旧 `repository.XRepository` / `*service.XService` 逐步替换为模块 ports / app 接口，保证 `internal/app` 仍是唯一依赖汇聚点。

## repository 迁移策略

1. **模块已有 ports 时，不再重复搬接口**：直接以 `internal/modules/<module>/ports/ports.go` 作为真接口。
2. **先迁 implementation，再保留 shim**：
   - 例：把 `internal/repository/live_analysis_report.go` 的 MySQL / Memory 实现迁到 `internal/modules/live_analysis/repository/`；
   - `internal/repository/live_analysis_report.go` 暂改为 type alias + constructor forwarding。
3. **shared repo 不强行“认领”**：`UserRepository` 当前跨 auth/admin/marketplace/ai/mcp/live_session 等多模块使用，本轮不建议硬拆出 user 模块；先留在 `internal/repository/user_*.go`。
4. **Redis / realtime / lock 采用 adapter 下沉，不搬 infra client**：
   - 例如 live_session lock / realtime 的 owner 是 `live_session`，但 Redis client / key builder / script registry 仍留 `internal/infra/redis`；
   - 模块内只新增薄 adapter constructor 或 module repository wrapper。
5. **跨事务边界先保留 shared TxManager**：`repository.TxManager` / `NewGORMTxManager` 暂不迁入模块，直到核心链路稳定。

## 测试迁移策略

- **跟模块移动**：纯模块 app/service 单测、module repository memory/mysql 单测、模块本地 DTO/错误测试。
- **留在 app / transport / integration 层**：
  - `internal/app/server_test.go`、`internal/app/mysql_redis_integration_test.go`
  - `internal/transport/http/*_test.go`
  - `internal/transport/ws/*_test.go`
  - 任何显式验证跨模块编排、HTTP/WS/MCP 协议、APP_ROLE/composition root 的测试
- **核心交叉测试暂不急于拆散**：如 `internal/service/bid_hammer_deposit_order_test.go`、`mcp_control_test.go` 这类跨模块契约测试，可在核心批次完成前先保留原地或上移到 `internal/app`，不要边迁边碎裂。

## Step 1: 迁移 Batch 1（auth + live_analysis）

**What:** 先落地两个低耦合模块，验证“module app service + module repository + internal/app wiring shim”模式。`auth` 仅迁 service，`live_analysis` 同时迁 service 与 report repository。

**Agent:** executor

**References:**
- `internal/modules/auth/app/usecases.go:9-25`
- `internal/modules/auth/ports/ports.go:10-22`
- `internal/service/auth.go:18-182`
- `internal/modules/live_analysis/app/usecases.go:10-21`
- `internal/modules/live_analysis/ports/ports.go:9-43`
- `internal/service/live_analysis.go:48-107`
- `internal/repository/live_analysis_report.go:13-107`
- `internal/app/service_wiring.go:147-152,205-209`
- `internal/app/repository_wiring.go:43-45`

**MUST NOT:**
- 不创建 user module；`UserRepository` 仍留 `internal/repository/user_*.go`。
- 不改 auth / live_analysis 的公开 HTTP/MCP DTO JSON 结构。
- 不在这一批顺手动 `ai` / `mcp` / `admin`。

**Verify:**
- `go test ./internal/modules/auth/... ./internal/modules/live_analysis/...`
- `go test ./internal/transport/http ./internal/transport/mcp ./internal/app`
- `go test ./...`
- `go vet ./...`

**Parallel:** `auth` 与 `live_analysis` 可以并行；两者只在 `internal/app/service_wiring.go` 汇合，最终合并时串行处理 wiring 即可。

## Step 2: 迁移 Batch 2（ai + risk）

**What:** 迁移 AI 助手与风控模块。`ai` 迁 app service 与本地状态机；`risk` 迁 service、blacklist cache 接口对接与 risk repository。为下一批 `deposit` 的黑名单依赖先清 owner。

**Agent:** executor

**References:**
- `internal/modules/ai/app/usecases.go:11-48`
- `internal/modules/ai/ports/ports.go:10-19`
- `internal/service/ai_assistant.go:22-219`
- `internal/modules/risk/app/usecases.go:10-24`
- `internal/modules/risk/ports/ports.go:11-40`
- `internal/service/risk.go:13-174`
- `internal/repository/risk.go:15-156`
- `internal/app/service_wiring.go:153-159,209,250-255`
- `internal/app/repository_wiring.go:49,67`

**MUST NOT:**
- 不把 `FeatureFlagService`、`BlacklistStrategy` 一起塞进 risk；它们属于 admin/support。
- 不把 `ai` 的 transport notifier/hub 适配直接下沉到 transport 包依赖模块内部。

**Verify:**
- `go test ./internal/modules/ai/... ./internal/modules/risk/...`
- `go test ./internal/transport/http ./internal/transport/mcp ./internal/app`
- `go test ./...`
- `go vet ./...`

**Parallel:** `ai` 与 `risk` 可并行；但若 `ai` 需要复用新的 status notifier adapter，需等待 wiring 统一落地。

## Step 3: 迁移 Batch 3（deposit + marketplace）

**What:** 在 risk owner 稳定后迁 `deposit`，同时迁 `marketplace` 读模型装配。`deposit` 除 service 外还要一起迁 `deposit_reconciler.go`；`marketplace` 只迁 app service，不认领 auction/order/user/session 仓储 ownership。

**Agent:** deep-executor

**References:**
- `internal/modules/deposit/app/usecases.go:10-17`
- `internal/modules/deposit/ports/ports.go:9-39`
- `internal/service/deposit.go:18-172`
- `internal/service/deposit_reconciler.go:19-117`
- `internal/repository/deposit.go:16-111`
- `internal/modules/marketplace/app/usecases.go:9-22`
- `internal/modules/marketplace/ports/ports.go:9-48`
- `internal/service/marketplace.go:13-259`
- `internal/app/service_wiring.go:158-160,247-249`

**MUST NOT:**
- 不在这一批动 `order` / `auction` / `live_session` 的 owner repository。
- 不为 marketplace 新建“聚合仓储”，仍以现有模块 ports 组合读取。

**Verify:**
- `go test ./internal/modules/deposit/... ./internal/modules/marketplace/...`
- `go test ./internal/transport/http ./internal/app`
- `go test ./...`
- `go vet ./...`

**Parallel:** 先 `deposit` 后 `marketplace`。因为 marketplace 读取 deposit/order/session/auction 多边接口，最好在 deposit owner 落稳后再切。

## Step 4: 迁移 Batch 4（admin/support）

**What:** 迁 `admin` app service，并把 `config` / `feature_flag` / `audit` / `dashboard` 作为 admin/support 边界下沉：`ConfigRepository`、`AuditRepository`、`DashboardRepository` concrete 可迁入 `internal/modules/admin/repository/`；`FeatureFlagService` 与 `blacklist_strategy.go` 迁入 `internal/modules/admin/app/`。

**Agent:** deep-executor

**References:**
- `internal/modules/admin/app/usecases.go:10-34`
- `internal/modules/admin/ports/ports.go:12-62`
- `internal/service/admin.go:12-220`
- `internal/service/feature_flag.go:19-212`
- `internal/service/blacklist_strategy.go:13-69`
- `internal/repository/config.go:14-99`
- `internal/repository/audit.go:12-109`
- `internal/repository/dashboard.go:13-220`
- `internal/app/service_wiring.go:250-255`
- `internal/app/repository_wiring.go:45,50-51,61`

**MUST NOT:**
- 不在 admin/support 批次改 auction/order/live_session 核心业务规则。
- 不把跨模块 orchestration 再次拉回 admin 内部；admin 仍应依赖其它模块 app 接口。

**Verify:**
- `go test ./internal/modules/admin/...`
- `go test ./internal/transport/http ./internal/app`
- `go test ./...`
- `go vet ./...`

**Parallel:** 必须等待 Step 2-3 完成后再做；因为 admin 聚合了 risk/order/auction/live_session/config/flags。

## Step 5: 迁移 Batch 5（mcp facade）

**What:** 最后迁 façade 型的 MCP read/control service。MCP 自己不认领 auction/order/session repo owner，只迁 `app/read_service.go`、`app/control_service.go`、`app/dto.go`；若需要扩展投影器，再新增 `projector.go`。MCP 仍通过其它模块 app / ports 组装，不复制业务规则。

**Agent:** deep-executor

**References:**
- `internal/modules/mcp/app/usecases.go:10-46`
- `internal/modules/mcp/ports/ports.go:15-126`
- `internal/service/mcp_read.go:18-249`
- `internal/service/mcp_control.go:17-256`
- `internal/app/service_wiring.go:255-276`
- `internal/transport/mcp/handler.go`
- `internal/transport/mcp/tools.go`

**MUST NOT:**
- 不把 MCP 变成新的 composition root。
- 不把 admin/marketplace/auction/order 的 DTO 再复制一套到 mcp repository。

**Verify:**
- `go test ./internal/modules/mcp/...`
- `go test ./internal/transport/mcp ./internal/app`
- `go test ./...`
- `go vet ./...`

**Parallel:** 必须等待 Step 1-4；MCP 依赖范围最广，不适合提前。

## Step 6: 迁移 Batch 6（core realtime/trade：order → live_session → auction/bid/hammer/workers）

**What:** 最后处理强耦合核心链路。建议拆成同一大批次内的三个串行子波次：
- 6a `order`：先迁 `OrderService` 与 `OrderRepository`
- 6b `live_session`：再迁 `LiveSessionService`、`LiveSessionRepository`、`LiveSessionLock`、`LiveSessionRealtimeStore`
- 6c `auction`：最后迁 `AuctionService`、`BidService`、`HammerService`、`TimerScheduler`、`bid_*worker`、`kafka_workers.go`、`auction/deposit/order` 相关 runtime adapter

**Agent:** deep-executor

**References:**
- `internal/modules/order/app/usecases.go:9-25`
- `internal/service/order.go:20-220`
- `internal/repository/order.go:15-189`
- `internal/modules/live_session/app/usecases.go:10-57`
- `internal/modules/live_session/ports/ports.go:10-97`
- `internal/service/live_session.go:22-174,224-1065`
- `internal/repository/live_session.go:15-163`
- `internal/repository/live_session_lock.go:11-83`
- `internal/repository/live_session_realtime.go:11-120`
- `internal/modules/auction/app/usecases.go:10-70`
- `internal/modules/auction/ports/ports.go:11-175`
- `internal/service/auction.go:20-237`
- `internal/service/bid.go`
- `internal/service/hammer.go:21-220`
- `internal/service/timer.go`
- `internal/service/bid_record_worker.go`
- `internal/service/bid_ranking_worker.go`
- `internal/service/kafka_workers.go`
- `internal/app/service_wiring.go:174-277`
- `internal/app/worker_wiring.go:34-82`

**MUST NOT:**
- 不把 6a/6b/6c 合成一次性大爆炸 rename。
- 不同时改协议、worker 行为、缓存策略、Redis key 设计。
- 不删除 `internal/repository` shim，直到 `internal/app`、transport、测试 全部切完。

**Verify:**
- `go test ./internal/modules/order/... ./internal/modules/live_session/... ./internal/modules/auction/...`
- `go test ./internal/transport/http ./internal/transport/ws ./internal/transport/mcp ./internal/app`
- `go test -race ./internal/transport/ws ./internal/modules/auction/... ./internal/modules/live_session/...`
- `go test ./...`
- `go vet ./...`

**Parallel:** 只能串行：6a → 6b → 6c。`HammerService`、`BidService`、`AuctionService`、`LiveSessionService` 之间存在显式循环协作，必须保守处理。

## 高风险点与控制

- **import cycle**：核心风险来自 `auction ↔ live_session ↔ mcp/admin` 与 app wiring。控制方式：模块只依赖 ports / app 接口；跨模块调用一律收口到 `internal/modules/<peer>/app` 或 `<peer>/ports`，不要直接 import 对方 repository package。
- **shared domain / shared repo owner 缺失**：`user_*`、`tx.go`、通用 helper 暂不迁，避免为了“纯模块化”硬造 user/support module。
- **realtime / worker owner**：Redis client、Kafka client、Hub 仍留 `internal/infra` / `internal/app`; 只迁 worker/service 本体到 owning module，避免把平台层混入模块。
- **cross-module transaction**：`deposit`、`hammer`、`order`、`live_session` 当前共享 `TxManager`。本轮只迁代码位置，不改事务边界模型。
- **MCP/read model**：MCP 是 façade，不应拥有 auction/order/session 真源 repository；否则会把读模型和业务 owner 搅乱。
- **admin/support 聚合边界**：`feature_flag`、`config`、`audit`、`dashboard` 可作为 admin/support owner；但 admin 不应回收 auction/order/risk 的内部规则。

## 最小可闭环第一批建议

**建议第一批只做 `auth + live_analysis`。**

原因：

1. `AuthService` 依赖最轻，仅依赖 `UserRepository` 与 JWT manager（`internal/service/auth.go:18-50`），可以先验证 service 迁移而不碰 module-owned repo owner 问题。
2. `LiveAnalysisService` 依赖清晰：`LiveAnalysisReportRepository + LiveSessionRepository + requester`（`internal/service/live_analysis.go:48-70`），可以完整验证“service + module repository + app wiring shim”。
3. 二者都不涉及 Redis 锁、realtime worker、跨模块事务、MCP façade 聚合，失败面最小。
4. 第一批成功后，能直接沉淀出本轮通用模板：
   - module app constructor 形态
   - module repository mysql/memory 文件落位
   - `internal/repository` alias/shim 模式
   - `internal/app/service_wiring.go` / `repository_wiring.go` 的切换方式

## 本轮明确不要做的事

- 不从 `auction/live_session/bid/hammer` 开始。
- 不一次性消灭 `internal/service` / `internal/repository`。
- 不把 `internal/infra/redis`、`internal/infra/mysql` 整包搬进模块。
- 不为 `UserRepository` 硬拆 user module。
- 不在同一批同时做“代码迁移 + 业务重写 + worker 重构 + 协议改动”。
- 不提前删除过渡 alias/shim；每批至少保留一个稳定回退点。

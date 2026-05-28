# 可观测性 — 双 Redis 与分布式改造同步指南

> 本文是《[可观测性设计方案](./可观测性设计方案.md)》的**改造增量篇**：当 `aieas_backend` 由"单 Redis + 单进程"演进为"RT/Cache 双 Redis + 多副本/多分片分布式架构"时，**哪些可观测能力需要同步改造、改在哪个文件、怎么验收**。
>
> 本文不重复《可观测性设计方案》中已经定稿的指标、Trace、日志规范，只覆盖改造时的"动点清单"。当前实装现状以 `AGENTS.md` 的 `## Observability` 章节为准。

## 1. 目的与范围

适用场景：

- **场景 A（双 Redis）**：把目前单实例 Redis（既承担热数据也承担缓存）拆分为 **RT 实例**（拍卖热路径、Stream、在线人数、保证金集合、分布式锁）与 **Cache 实例**（幂等键、缓存旁路、布隆过滤器）。
- **场景 B（分布式架构）**：把单进程 `aieas_backend` 部署为多副本（同时也可能引入水平分片、独立 WS Gateway、独立 Worker）。

不在范围：业务层的拆分顺序、容量规划、数据迁移方案——这些由架构方案文档负责。本文只回答："改完之后，可观测面要动哪些代码与运维配置"。

## 2. 当前架构基线（改造起点）

| 维度 | 现状 | 关键代码点 |
| ---- | ---- | ---------- |
| Redis 实例 | 单实例 `default` | `internal/app/server.go` 中 `redisinfra.NewMetricsHook(metricsRegistry, "default")` |
| Metrics instance label | 已为 Redis 指标预留 `instance` 维度，单实例固定 `"default"` | `internal/infra/observability/metrics/registry.go` 中 `redisCommandDuration`/`redisCommandErrors` 的 label 集为 `{instance, op}` |
| Redis Tracing | `redisotel.InstrumentTracing(rdb)` 单 client 注入 | `internal/app/server.go` |
| Hub | 单进程，`replay window` 在内存中维护，`online presence` 已下沉 Redis | `internal/transport/ws/hub.go` |
| EventLog/Stream | 单 producer-consumer，`traceparent`/`tracestate` 通过 `bid.lua` ARGV[17]/[18] 注入 stream payload | `scripts/lua/bid.lua`、`internal/infra/redis/event_log.go`、`internal/service/bid_record_worker.go` |
| Trace 透传 | W3C TraceContext + Baggage，HTTP header 与 map carrier（`tracing.InjectMap` / `ExtractMap`） | `internal/infra/observability/tracing/tracing.go` |
| `/readyz` probe | 三项：`mysql` / `redis` / `scripts`，map 顺序固定按 key 排序 | `buildReadinessProbes`（`server.go`）+ `ReadinessHandler`（`internal/transport/http/observability.go`） |
| `/metrics` | Prometheus pull，单端点，`OBSERVABILITY_METRICS_AUTH_TOKEN` 鉴权 | `registerObservabilityRoutes`（`server.go`） |
| 限流 | `httptransport.NewRateLimiter(240, 1m)` 进程内令牌桶 | `internal/transport/http/middleware.go` |

---

## 3. 双 Redis 改造（RT vs Cache 拆分）

### 3.1 启用 `instance` label 的两个值

**改造点**：`metrics.RedisHook` 的 instance 参数从单一 `"default"` 改为 `"rt"` / `"cache"`。

- 文件：`internal/app/server.go`
  - 当前：`rdb.AddHook(redisinfra.NewMetricsHook(metricsRegistry, "default"))`（约 802 行）
  - 改为：构造 `rdbRT`、`rdbCache` 两个 `*redisgo.Client`，分别 `AddHook(NewMetricsHook(reg, "rt"))` 和 `AddHook(NewMetricsHook(reg, "cache"))`。
- 文件：`internal/infra/redis/metrics_hook.go`（无需改实现，instance 为外部注入维度）。
- 调用方复核：所有 `realtime_store.go`、`event_log.go`、`live_room_lock.go`、`online_counter.go`、`idempotency_store.go`、`script_registry.go` 在调用 `*redisgo.Client` 时**必须用对的 client**。建议为这两个 client 增加类型别名（如 `RedisRTClient` / `RedisCacheClient`），让编译器替你校验。

**验收**：`/metrics` 抓一次，`redis_command_duration_seconds_bucket` 必须同时出现 `instance="rt"` 与 `instance="cache"` 两组样本。

### 3.2 `/readyz` 拓扑扩展

**改造点**：`buildReadinessProbes` 把单 `redis` 项拆成 `redis_rt` 与 `redis_cache`。

- 文件：`internal/app/server.go` 的 `buildReadinessProbes(db, rdb, scripts)`
  - 签名扩展为 `buildReadinessProbes(db, rdbRT, rdbCache, scripts)`，注入两条独立 probe。
- 文件：`internal/transport/http/observability.go` 中 `ReadinessHandler` 无需改，它对 probe 数量是无感的；仅响应体里 `components` 字段会自动从 `{redis: ok}` 变成 `{redis_rt: ok, redis_cache: ok}`。
- **降级语义建议**：默认仍保留"任一 probe 失败 → 整体 503"。如果未来需要"Cache 故障可降级运行"，建议在 `ReadinessProbe` 闭包外面再封一层 `optional probe` 标记，由 handler 决定是否计入 ready 判定，**而不是**在 probe 内部 `return nil` 装作健康。

**验收**：手动 `docker stop redis-cache`，`/readyz` 返回 503，`components.redis_rt=ok`、`components.redis_cache=<err>`；恢复 cache 后 200。

### 3.3 `redisotel` Tracing 双实例区分

**改造点**：两个 client 都需要单独 `redisotel.InstrumentTracing`。

- 文件：`internal/app/server.go`，当前形如 `if err := redisotel.InstrumentTracing(rdb); err != nil {...}`，改成对 `rdbRT` 与 `rdbCache` 各调用一次。
- 副作用：span attribute 自带 `db.name` / `server.address`，trace UI 上自然按实例分组，**无需**自己加 `redis.instance` attribute。
- **Lua span**：`script_registry.go` 的 Eval 路径（人工 `tracing.StartSpan(... "redis.lua.<script>")`）若希望区分实例，可在 attribute 里加 `redis.instance="rt"`；推荐保留：Lua 永远跑在 RT，attribute 写死 `"rt"` 即可。

**验收**：在 trace UI 上对一次出价请求展开 span 树，`redis.GET enrolled:*` 这类 span 的 `server.address` 应反映 RT 实例 IP；缓存读如商品详情则反映 Cache 实例 IP。

### 3.4 EventLog / Stream / 锁的实例归属

**约定（必须在改造同步落到代码与文档）**：

| 数据 | 实例 | 原因 |
| ---- | ---- | ---- |
| `bid_stream` (XADD/XREADGROUP) | **RT** | 拍卖热路径，强一致、不可丢 |
| `online_counter` (HSET + ZADD) | **RT** | 房间在线人数实时聚合 |
| `live_room_lock` (SETNX + Lua DEL) | **RT** | 串行化 ActivateAuction |
| `enrolled` / `deposits` set | **RT** | bid.lua 直接读 |
| `IdempotencyStore` | **Cache** | 旁路键，可重建（首次落空走 MySQL 兜底） |
| 商品详情 / 直播间详情缓存 | **Cache** | 显示用，回源 MySQL |

**Trace 透传不要跨实例拆**：单次出价 `HTTP → bid.lua（RT）→ XADD bid_stream（RT）→ BidRecordWorker（RT）` 的 traceparent 仅由 RT 链路承载。Cache 上的查询 span 是 sibling，不参与该出价 trace 的"接力"。这一原则要写进 `event_log.go` 的注释，避免后续误改。

### 3.5 metrics 基数评估

加入 `instance="rt"|"cache"` 后，相关指标基数变化：

- `redis_command_duration_seconds{instance, op}`：op 约 25 个常见命令 × 2 instance × histogram 12 桶 ≈ 600 系列。可控。
- `redis_command_errors_total{instance, op}`：≈ 50 系列。可控。
- `redis_lua_duration_seconds{script}`：脚本只跑 RT，不加 instance 维度，保持现状。
- `redis_stream_xadd_total{stream, result}`：stream 只在 RT，**不**加 instance 维度。
- `redis_lock_acquire_total{lock, result}`：锁只在 RT，**不**加 instance 维度。

**结论**：仅 `redis_command_*` 两条加 instance 维度，其余保持现状，避免 label 蔓延。

### 3.6 配置 schema

**改造点**：`internal/config/config.go`

- 把 `RedisConfig` 拆为 `RedisRTConfig` + `RedisCacheConfig`，或保留 `RedisConfig` 并扩出 `RT` / `Cache` 子结构。
- 同步：
  - `Default()` 给两个实例各一份默认值（推荐 RT/Cache DB 用不同编号，避免 keyspace 冲突）。
  - `applyEnv` 拆出 `REDIS_RT_ADDR` / `REDIS_RT_DB` / `REDIS_RT_PASSWORD`，`REDIS_CACHE_*` 同理。
  - `Validate()`：两实例 DB 必须 ≥ 0；如果 addr 相同，DB 必须不同（否则两个 client 共用 keyspace，等于没拆）。
- 同步更新：`configs/config.yaml`、`.env.example`。

**验收**：`go test ./internal/config/... -run TestConfig` 全绿；缺失 `REDIS_RT_ADDR` 时启动失败并明确指出哪一项。

### 3.7 Grafana / 告警

- Dashboard 中 `redis_command_duration_seconds` 类查询在 PromQL 里**显式**加 `instance` 选择器；按 `rt` / `cache` 拆双面板。
- 告警阈值要分别设：RT P99 > 5ms 立即告警；Cache P99 > 20ms 才告警（容忍度不同）。
- Redis 自身指标（`redis_exporter`）在每个实例侧独立部署，按 Prometheus job 区分。

### 3.8 双 Redis 改造验收清单

```text
[ ] go test ./... 全绿
[ ] /metrics 中可看到 instance="rt" 与 instance="cache" 两组样本
[ ] /readyz 在停掉 cache 实例时 503，components.redis_rt=ok / redis_cache=<err>
[ ] 一次出价 trace：HTTP → bid.lua(RT) → BidRecordWorker，traceparent 不丢失
[ ] Cache 实例查询的 span 在 trace UI 上以 sibling 形式展开，server.address 不同
[ ] redis_lua_* / redis_stream_* / redis_lock_* 没有意外多出 instance 维度
[ ] 配置文件 / .env.example / Validate 三处同步
[ ] Grafana dashboard 双面板拆分完成，旧 dashboard 已快照备份
```

---

## 4. 分布式架构改造（多副本 / 多分片）

### 4.1 进程身份：`service.instance.id`

**改造点**：`internal/infra/observability/tracing/tracing.go` 的 `buildResource`。

- 当前 resource 只包含 `service.name`、`process.*`、`host.*`。
- 加上 `semconv.ServiceInstanceID(<pod-name 或 hostname>)`，从 `POD_NAME` / `HOSTNAME` env 读取，启动期一次性确定。
- **不要**把 pod 名加到 Prometheus label 里：Prom 端通过 `job` + 自动注入的 `instance` 维度就能区分，再叠 pod 名会让 metric 系列数 × 副本数线性膨胀。

**验收**：trace UI 上展开任一 span，资源属性里能看到 `service.instance.id=<pod>`；按 instance 分组聚合可以分别看每个副本的链路画像。

### 4.2 Hub 多副本化

**当前痛点**：`internal/transport/ws/hub.go` 的 `Hub` 持有进程内 `rooms`（含 `history` replay window）与 `sessionClients`。多副本下：

- **online_counter**：已基于 Redis（`internal/infra/redis/online_counter.go`），跨副本天然一致——保持现状。
- **replay window**：当前每副本独立维护 `history []Envelope`，副本 A 持有的客户端断线后重连到副本 B 会丢失 replay。

**两条改造路线**：

1. **Stream 化方案（推荐）**：让 `Hub.replaySource` 由 Redis Stream（`bid_stream`）直接回放，而不是进程内数组。`event_log.go` 已支持按 `lastSeq` 反查 stream，扩展为 ReplaySource 实现即可。可观测面无需改动 — `ObserveWSBroadcast` 仍在每副本独立。
2. **Sticky session 兜底**：在 LB / Ingress 层按 `auctionId` 一致性哈希到固定副本，replay 仍走进程内。**注意**：sticky 命中率必须监控，建议加 `ws_sticky_route_total{result="hit|miss"}` counter，由 LB 侧统计或在 Hub 第一次 join 时按 client 提供的 last-pod 比对推断。

**Metrics 行为变化**：

- `IncWSConnect/Disconnect`：每副本独立计数 → Prom 端 `sum by (job)(rate(...))` 即得全局连接增删速率；不需要做全局聚合。
- `ws_connections` gauge：每副本独立 → Prom 端 `sum(ws_connections)` 即得全局在线连接数；**不要**改成由 Redis 汇总后所有副本回写同一个 gauge（会导致计数翻倍）。
- `ObserveWSBroadcast(elapsed, fanout)`：fanout 是该副本扇出的客户端数，多副本下 `sum(rate(ws_broadcast_fanout_total))` 自然等于全局推送条数。

**改造时不要做的事**：不要把 `ws_connections` 这类 gauge 改成"从 Redis 读全局值，每副本回写"——这会让 Prom 看到 N 倍重复值。Gauge 永远只反映**本副本视角**，全局通过 PromQL 聚合。

### 4.3 跨进程 Trace 透传补全

| 跨边界 | 现状 | 改造点 |
| ------ | ---- | ------ |
| HTTP server 入口 | `TracingMiddleware` 自动 Extract（`internal/transport/http/observability.go`） | 无需改 |
| 出站 HTTP 客户端 | Agent client 已用 `otelhttp.NewTransport`（G5） | **新增**任何内部服务调用都必须用 `otelhttp.NewTransport`，禁止裸 `http.Client` |
| Redis Stream | `bid.lua` ARGV[17]/[18] 注入 traceparent；`BidRecordWorker.consume` `tracing.ExtractMap` 续接 | 多 consumer group 下每个 consumer 独立 ExtractMap → StartSpan，trace tree 自动聚合，**无需**改 |
| gRPC（如未来引入） | — | producer/consumer 必须分别用 `otelgrpc.NewClientHandler` / `NewServerHandler` |
| Kafka / RocketMQ（如未来引入） | — | producer 写 message header inject、consumer ExtractMap 续接，模式与 event_log 完全一致；可直接复用 `tracing.InjectMap` / `ExtractMap` |
| WebSocket | 无（WS 上行仅在 Gateway 入口建 span） | 不改：单条 WS 帧建 span 数据量过大，参考设计方案 5.5 |

**铁律**：每跨一次进程边界，必须在写入侧 inject、读取侧 extract，**绝不**依赖隐式上下文传递。

### 4.4 `/readyz` 的分布式语义

**改造点**：保持 `buildReadinessProbes` 只检查"本副本自身的下游依赖"。

- 不要在 probe 里 ping **其它服务的 HTTP 端点**——会形成级联失败：上游服务暂停 → 所有副本同时 503 → LB 摘除全部实例 → 雪崩。
- 跨服务依赖通过 metrics + alert 暴露：`http_requests_total{status="5xx"}` 突增告警，而不是把它编码进 readiness。
- `mysql.ping` / `redis_rt.ping` / `redis_cache.ping` / `scripts.loaded` 这种"本副本不能干活"的强依赖，才放在 `/readyz`。

**验收**：拔掉某副本到 RT 的连接，**只该副本** 503，其他副本继续 200。

### 4.5 Metrics 聚合查询

PromQL 写法在多副本下要刻意调整：

```promql
# 错误写法（漏掉副本维度，且分母含义不清）
rate(aieas_http_requests_total[1m])

# 正确写法：按业务维度聚合，跨副本累加
sum by (route, status)(rate(aieas_http_requests_total[1m]))
```

- **counter**：`sum(rate(...))` 自动跨副本累加。
- **gauge**：业务语义决定。`http_inflight_requests` / `ws_connections` 用 `sum`；`ws_broadcast_queue_length` 看是否要 `max` 检查最坏副本。
- **histogram**：分位数查询前先 `sum(rate(...)) by (le, ...)`，再 `histogram_quantile`，**不能**在每副本算分位再平均。

**Dashboard 迁移**：双 Redis 改造引起的 label 集变化（多了 `instance`）会让历史曲线在改造时刻断层。**改造前必须**：

1. 快照当前 dashboard JSON；
2. 在新 dashboard 里用 `or vector(0)` 兜底处理改造前的样本缺口；
3. 把告警规则同步双化（`instance="rt"` 单独告，`instance="cache"` 单独告）。

### 4.6 日志集中化

`internal/infra/observability/logger.go` 的 slog 实现无须改动，但运维侧需要：

- **生产环境强制 JSON**：`OBSERVABILITY_FORMAT=json` 让 `WithTraceContext` 自动注入 `trace_id` / `span_id`，便于 Loki / ELK 按 trace_id 跨副本检索同一请求的日志。
- 日志采集 sidecar（fluent-bit / promtail）按 `pod_name` 标 label，但**不要**反向把 pod_name 写回业务字段。
- 多行 stack trace（Go panic）在采集端按 `trace_id` 聚合。如果采集器不支持，改用 `recover()` 后单条 ERROR 日志，把 stack 编码到一个字段里。
- **底线**：跨副本排障时，"`trace_id=xxx` 的全部日志"必须能一次查出来；做不到就先回头修日志管线。

### 4.7 Idempotency 与分布式锁

- `IdempotencyStore` 已在 Redis（Cache 实例）：多副本天然分布式 — 无需改。
- `LiveRoomLock` 已在 Redis（RT 实例）：SETNX + Lua DEL — 无需改。
- **建议补观测点**（当前 `metrics/observers.go` 暂缺）：
  - `idempotency_outcome_total{outcome="hit|miss|conflict"}`：能在分布式重复请求场景下区分"幂等命中"与"真重试"。
  - `live_room_lock_acquire_duration_seconds`：分布式下锁竞争延迟会变大，需提前埋点观察。

### 4.8 限流分布式化

**当前**：`httptransport.NewRateLimiter(240, time.Minute)` 是进程内令牌桶（`internal/transport/http/middleware.go`）。多副本下，每副本独立限额，全局配额 = N × 240/min，行为不可预期。

**改造方向**（与本文仅观测面相关的部分）：

- 切换到基于 Redis 的滑动窗口或 token bucket（Redis Cache 实例）。
- 观测点必须改造：
  - 新增 `rate_limit_reject_total{route}` counter（接口路由维度），由各副本独立 `Inc()`；Prom 端 `sum by (route)(rate(...))` 即全局拒绝速率。
  - **不要双写**：拒绝时只在被拒副本计数一次；不要再"广播"到其它副本。
  - Redis 侧的限流键 TTL/计数可由 `redis_exporter` 暴露，不需要业务再上报。

### 4.9 分布式改造验收清单

```text
[ ] 启动 ≥ 2 副本，trace UI 中单条出价 trace 跨副本串联（service.instance.id 不同的 span 出现在同一棵 trace tree）
[ ] /readyz：单副本下游断开仅该副本 503，其余副本 200
[ ] /metrics：sum by (route)(rate(http_requests_total[1m])) 与单进程压测结果按副本数线性匹配
[ ] WS 客户端在副本间漂移（断连 + 重连）replay 行为一致：要么走 Stream replay，要么 sticky 命中率 > 99%
[ ] 跨进程 Trace 链路完整：HTTP 入口 → 内部 RPC → Redis Stream → Worker，无中断
[ ] 日志按 trace_id 跨副本可查；JSON 模式下 trace_id / span_id 字段稳定
[ ] 限流改造后 sum(rate(rate_limit_reject_total)) 与压测注入速率匹配，无副本间双写
[ ] 没有把 pod_name / 副本数等高基数维度写入 Prometheus label
```

---

## 5. 风险与回滚

- **Dashboard / 告警断层**：双 Redis 改造让 `redis_command_*` 的 label 集多出 `instance` 维度，旧的 `redis_command_duration_seconds{op="GET"}` 选择器会同时命中两实例，分位数图形会在改造时刻发生跳变。**改造前**先快照 Grafana JSON，**改造后**逐项把面板加 `instance` 选择器。
- **WS replay 不连续**：分布式改造前若没有把 replay window 下沉到 Stream、又没做 sticky session，重连客户端会出现"断档窗口期"——表现为前端 `seq <= lastSeq` 重复丢弃后又拉到旧事件。改造前必须在测试环境用 chaos 工具断开单副本验证。
- **回滚策略**：保留兼容期内的"单 Redis 路径"或"单进程路径"作为 feature flag（如 `OBSERVABILITY_REDIS_INSTANCES=single|dual`），灰度一段时间观测无异常再删旧路径。**不要**直接合并删掉旧分支，否则一旦回滚必须改代码。
- **改造前置条件**：分布式改造前确认所有进程内可变状态（rate limiter、idempotency 缓存、replay window）已外置到 Redis / MySQL；任何 `sync.Map` / `atomic.*` 单副本变量在多副本下都是潜在的不一致源。

---

## 6. 一页改造检查表

> 改造时按此表逐项过；每完成一项打勾。本表是 §3.x / §4.x 全部要点的汇总，可直接 `cp` 到 PR 描述里。

### 双 Redis（RT/Cache）

```text
[ ] config.RedisConfig 拆分为 RT / Cache，Default / applyEnv / Validate 同步
[ ] configs/config.yaml 与 .env.example 同步双实例字段
[ ] server.go composition root 构造 rdbRT / rdbCache 两个 client
[ ] rdbRT.AddHook(NewMetricsHook(reg, "rt"))；rdbCache.AddHook(NewMetricsHook(reg, "cache"))
[ ] redisotel.InstrumentTracing 对两个 client 各调用一次
[ ] buildReadinessProbes 入参扩展，probe map 改为 redis_rt + redis_cache
[ ] EventLog / online_counter / live_room_lock / enrolled set / scripts 显式绑定 RT
[ ] IdempotencyStore / 缓存旁路 显式绑定 Cache
[ ] 类型别名 RedisRTClient / RedisCacheClient 防误用（可选但推荐）
[ ] redis_lua_* / redis_stream_* / redis_lock_* 维度未被意外加上 instance
[ ] Grafana dashboard 旧版快照备份；新版面板按 instance 拆分
[ ] 告警规则双化（rt / cache 阈值独立）
[ ] /readyz 故障注入测试通过
[ ] go test ./... 与 go vet ./... 全绿
```

### 分布式（多副本/多分片）

```text
[ ] tracing.buildResource 注入 service.instance.id（来自 POD_NAME/HOSTNAME）
[ ] 不要把 pod_name / 副本数加进 Prometheus label
[ ] Hub.replaySource 切换为 Redis Stream，或 LB sticky session 命中率监控就绪
[ ] WS 相关 gauge 仍反映"本副本视角"，未被改成全局回写
[ ] 所有出站 HTTP 客户端使用 otelhttp.NewTransport
[ ] gRPC（若引入）使用 otelgrpc.NewClient/ServerHandler
[ ] 任何新增 MQ producer/consumer 用 tracing.InjectMap/ExtractMap 透传 traceparent
[ ] /readyz 仅检查本副本的强依赖；禁止 ping 其它服务 HTTP 端点
[ ] OBSERVABILITY_FORMAT=json 在生产强制开启
[ ] 日志采集按 trace_id 可跨副本聚合；多行 stack 处理就绪
[ ] 限流改为 Redis 后端，新增 rate_limit_reject_total{route} counter；副本间不重复计数
[ ] 补充 idempotency_outcome_total / live_room_lock_acquire_duration_seconds 观测点
[ ] PromQL 全部改为 sum by (...)(rate(...)) 形态；histogram 用 sum by (le,...) + histogram_quantile
[ ] 多副本下单条出价 trace 跨进程完整串联，验证通过
[ ] 兼容期 feature flag 就绪，灰度方案与回滚步骤明确
```

---

> 本文与《[可观测性设计方案](./可观测性设计方案.md)》、`AGENTS.md` 的 `## Observability` 章节配套使用：前者讲"应该长成什么样"，后者讲"现在长成什么样"，本文讲"改造时怎么从后者过渡到前者"。改造完成后，请把对应小节的现状回写到 `AGENTS.md`，并把本文中已完成的项移入"已落地"档案。

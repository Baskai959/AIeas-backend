# Harness Graph: redis-rt-cache-split

> Status: completed

## User Intent
- Original: 梳理项目中所有用到 Redis 的地方，然后参考 `docs/拍卖高并发一致性技术方案.md` 中的「Redis 分层缓存设计」部分，对 Redis 做职责拆分（暂不做读写分离），拆为 RT 与 Cache 两套；增加分层缓存设计，配置 TTL 与失效策略；并按文档实现穿透/击穿/雪崩防护。Redis 配置需要在 yaml 中留配置口。
- Acceptance:
  1. 列出当前项目中所有 Redis 使用点（key、用途、读写模式、调用方）。
  2. Redis 客户端拆为 RT (实时强一致) 与 Cache (读多缓存) 两套，注入到对应模块；端点：Cache `127.0.0.1:6380`，RT-1 `127.0.0.1:6381`，RT-2 `127.0.0.1:6382`（RT 暂用 RT-1 主，留双实例配置位）。
  3. yaml 配置位完整：`redis.rt.*` 与 `redis.cache.*`（addr/password/db/poolSize/dialTimeout/readTimeout/writeTimeout/...），并在 `internal/config/config.go` 与 `.env.example` 同步留口。
  4. Cache 实现分层缓存（L1 本地 + L2 Redis Cache）+ TTL/抖动；穿透（空对象+布隆/短 TTL）、击穿（singleflight+互斥锁）、雪崩（TTL 抖动+多级降级）防护按文档落地。
  5. 现有功能（拍品/直播间/竞价/在线计数/事件流/幂等/锁/脚本）行为不变；`go build ./...` `go vet ./...` `go test ./...` 通过；可观测性 instance label 区分 `rt`/`cache`。

## Assurance
- Mode: strict_verify
- Why: 涉及 Redis 客户端注入面广，影响竞价/锁/脚本/可观测性；构建与测试必须通过；改动需有验证 gate，但失败不强制无界修复（用户未要求 ralph_strict）。

## Gates
### g1 plan-review
- Status: pending
- Acceptance: 设计能 1) 全覆盖现有 Redis 使用点的归属（RT vs Cache）2) 与 `internal/app/server.go` 注入面契合 3) 分层缓存与三防策略路径清晰 4) 配置位与 metrics instance 标签覆盖完整 5) 列出明确文件改动清单。
- Repair: 0/2

### g2 build-and-test
- Status: pending
- Acceptance: `go build ./...`、`go vet ./...`、`go test ./...` 全绿；可观测性 readiness 探针仍能区分 rt/cache。
- Repair: 0/2

## Understanding
- 项目使用 `github.com/redis/go-redis/v9`，统一通过 `internal/infra/redis` 提供 client/lua/lock/online_counter/event_log/idempotency/realtime_store/metrics_hook 等。
- 装配点是 `internal/app/server.go`（唯一 composition root）。
- 已有 `internal/infra/observability` + `metrics` 体系，Redis hook 的 instance label 当前默认 `"default"`，需扩展为 `rt`/`cache`。
- 设计参考文档：`docs/拍卖高并发一致性技术方案.md` 中「Redis 分层缓存设计」。

## Completed
### n1 @omc-explore — DONE
- Completed At: 2026-05-27T00:00:00+08:00
- Summary: 梳理 8 个 Redis 持有点 + 5 个直接 *redis.Client 文件；prefix 已支持；docker compose 三端口 6380/6381/6382 已就绪；设计文档 §9 三防方案摘录完整。

### n2 @omc-architect — DONE_WITH_CONCERNS
- Completed At: 2026-05-27T00:00:00+08:00
- Summary: 出 client.go 具名结构 + cfg.RT/Cache + LayeredCache(L1 sync.Map+L2 Cache+singleflight+jitter+空值)+ 三防 + Item.Get 本期接入。

### n3 @omc-critic — DONE_WITH_CONCERNS — gate:g1=passed_with_fixes
- Completed At: 2026-05-27T00:00:00+08:00
- Summary: PASS_WITH_REQUIRED_FIXES。5 项必修，已并入 n4 实施约束。

## Pending
### n4 @omc-deep-executor — 实施改造 — depends:n3
- Goal: 按 n2 设计 + n3 修订项落地。
- Acceptance: 改动落地、保持现有行为、自检 build/vet/test 通过。

### n5 @omc-verifier — 独立验证 — depends:n4 — gate:g2
- Goal: 验证 build/vet/test、metrics instance、readiness 双 probe。
- Acceptance: 满足 g2。

## Decisions（n3 修订项的 orchestrator 裁决）
- D1 旧 REDIS_* env 不保留兼容（直接迁移）。
- D2 `redis.rt.primary.addr` 与 `redis.cache.addr` 必须不同 → 硬校验失败（Validate 返回 error）。
- D3 LayeredCache 注入：通过构造形参 + `ServerDependencies.ItemCache cache.Cache`；Memory/无 Redis 路径用 `cache.Noop{}` 直透 loader。
- D4 采纳 critic optional：`idempotency_redis.go` 与 `script_registry.go` 形参保持接口不改，依靠嵌入透传方法集。
- D5 空值 sentinel 由原 `__NULL__` 字节串改为结构化标记（实现可选；若困难允许保留 `__NULL__` 字符串但需在注释中标注 collision 风险）。
- D6 `Open(ctx, RedisInstanceConfig) (*redisgo.Client, error)` 保留（小写返回原始 client，仅供包内 OpenRT/OpenCache 复用）；外部 API 用 `OpenRT/OpenCache`。`openClients` 返回 `(*gorm.DB, *RedisRTClient, *RedisCacheClient, error)`。

# Harness Graph: observability-impl

> Status: in_progress

## User Intent
- Original: 根据 /Users/bytedance/study/AI电商/backend/docs/可观测性设计方案.md 为项目添加可观测能力。
- Acceptance:
  - 设计方案中规定的可观测能力（logs / metrics / traces / 健康检查 / 业务监控等）按方案落地到 aieas_backend。
  - 配置项、依赖、初始化、注入路径完整闭环；`go build ./...` 与 `go test ./...` 通过；新增功能有最小覆盖测试。
  - 不破坏现有路由、鉴权、Hub、Lua 脚本等已有能力；composition root 仍是 `internal/app/server.go`。

## Assurance
- Mode: strict_verify
- Why: 多文件实装 + 关键基础设施改动（中间件链、composition root、配置），需要独立验收闭环；但用户未要求 ralph 强修复，先采用 strict_verify。

## Gates
### g1 plan-clarity
- Status: pending
- Acceptance: 实施清单覆盖文档全部能力维度，每项落到具体文件/函数/接口；用户确认后才进入实施。
- Repair: 0/2

### g2 build-and-test
- Status: PASSED (2026-05-27)
- Evidence: `go build ./...` exit 0；`go vet ./...` exit 0；`go test ./... -count=1` 全部 ok（13 包，含 app/config/domain/service/transport/observability/redis 等）。Verifier 二轮独立复核 G1–G13 全 PASS。
- Repair: 0/3

## Understanding
- 项目为 Go + Hertz + GORM + Redis 的实时拍卖直播间后端。
- 已有可观测基础：slog logger（`internal/infra/observability/logger.go`、GORM 桥接 `gormlogger.go`），中间件包含 RequestID/Recovery/Audit/RateLimiter（`internal/transport/http/middleware.go`），无 metrics/tracing。
- composition root 唯一在 `internal/app/server.go`。
- 配置 schema 在 `internal/config/config.go`（含 `ObservabilityConfig`），加载顺序 default→yaml→.env→env。

## Completed
### n1 @omc-explore — DONE
- Completed At: 2026-05-26T11:00:00+08:00
- Summary: 通读设计方案 627 行，盘点现状（slog/GORM logger/RequestID/Audit/RateLimiter/Hub stats/EventLog/ScriptRegistry，但完全缺 Prometheus/OTel/healthz/readyz）；产出 35 项 TODO（T1–T35）覆盖基础设施→composition root→middleware→业务埋点→端点→测试→配置文档；列出 12 项冲突 C1–C12 与默认建议。

### n2 user-confirmation — DONE
- Completed At: 2026-05-26T11:30:00+08:00
- Summary: 用户确认采用全部默认建议；C8 明确维持单 Redis 实例，仅保留 `instance` label 占位，后续再拆 RT/Cache。Gate g1 PASSED。

## Failed / Re-dispatch
### n3 @omc-deep-executor — FAILED (incomplete)
- Completed Surface Layer: metrics registry/observers、HTTP middleware (Metrics/Tracing skeleton)、config 字段、observability_test、health handler skeleton、tracelogger.go、部分 service 注入 metrics、Hub 接收 metrics。
- Verifier (n4) 发现 11 项缺口未落地：
  1. tracing 包仅有 Setup/StartSpan/TraceID/SpanID，缺 Init/InjectHTTP/ExtractHTTP/InjectMap/ExtractMap。
  2. service 层（Bid.Place / Hammer.Hammer / Deposit / Auction state transitions）未创建任何 span，仅有 metrics 计数。
  3. `/readyz` 没有真实依赖 ping（mysql、redis、scripts loaded）。
  4. GORM 没有 otelgorm 注入；Redis 没有 redisotel.InstrumentTracing + RedisHook（metrics）。
  5. Agent HTTP client 没有 otelhttp.Transport。
  6. go.mod 缺少 otelhttp、redisotel/v9、otelgorm 依赖。
  7. gormlogger 没有把 trace_id 注入 slog 上下文。
  8. MCP handler 完全没有 span / metrics 埋点。
  9. event_log 写入没有注入 traceparent；BidRecordWorker 消费没有 ExtractMap 续传 trace。
  10. Hub.SetMetrics 接受具体 `*metrics.Registry`，应改为窄接口（HubMetrics）。
  11. Config: SampleRatio 默认 1.0（应 0.1）、sampler 名称使用下划线、Validate 未检查 enabled=true 时 endpoint 必填；ServerDependencies 字段命名 `Metrics` 与文档 `MetricsRegistry` 不一致。
- 缺少测试: health_handler_test.go、script_registry_metrics_test.go、hub_metrics_test.go、gormlogger_trace_test.go、config_test.go 新字段覆盖。
- 缺少 AGENTS.md 可观测章节。

## Pending
### n4 @omc-verifier — depends:n3-remediation — gate:g2
- Goal: 二轮独立验收；逐项核对 11 缺口已闭合；build/vet/test 全绿；可观测端点行为合规；现有用例无回归。
- Acceptance: 给出每项核对结论与证据（文件:行号 / 命令输出）。

### n5 @omc-writer — depends:g2
- Goal: 生成《双 Redis 与分布式架构改造时的可观测性同步改造指南》文档（用户明确要求）。
- Acceptance: 覆盖 instance label 拆分、跨实例 trace 透传、healthz/readyz 拓扑扩展、metrics 维度规划、partition 化 Hub 的指标变化、跨进程 trace propagation 注意点等。

## Completed (G1–G13 remediation)
### n3-remediation @omc-deep-executor — DONE
- Completed At: 2026-05-27
- Closed Gaps:
  - G1 tracing 包补齐 Init/InjectHTTP/ExtractHTTP/InjectMap/ExtractMap (`internal/infra/observability/tracing/tracing.go`).
  - G2 service 层 span 埋点（Bid/Hammer/Deposit/Auction）。
  - G3 `/readyz` mysql/redis/scripts 真实 ping，503 含 component map。
  - G4 GORM otelgorm 插件 + Redis redisotel 注入 + 自定义 `redis.Hook` 上报 `redis_command_*` 指标 (`internal/infra/redis/metrics_hook.go`)。
  - G5 Agent HTTP client `otelhttp.NewTransport`。
  - G6 go.mod 添加 otelhttp / redisotel/v9 / otelgorm 依赖。
  - G7 gormlogger 经 `WithTraceContext` 注入 `trace_id` / `span_id`（仅 JSON 模式）。
  - G8 MCP handler `mcp.tool.call` / `mcp.resource.read` span，低基数 status 标签。
  - G9 event_log trace propagation: `bid.lua` 新增 ARGV[17]/[18] traceparent/tracestate；`PlaceBid` 通过 `tracing.InjectMap` 写入；`BidRecordWorker.handleEvent` 用 `tracing.ExtractMap` 续接 `bid_record.consume` span。
  - G10 `ws.HubMetrics` 接口化（4 方法），ws 包不再依赖 metrics 包。
  - G11 Config 默认 `SampleRatio=0.1`、tracing 启用时强制 endpoint（stdout/noop 例外）、`ServerDependencies.Metrics → MetricsRegistry` 命名对齐。
  - G12 测试覆盖：
    - `internal/config/config_test.go` 新增 TracingDefaults / NormalizeFillsSampleRatio / ValidateRequiresEndpoint / AcceptsStdoutWithoutEndpoint / RejectsInvalidExporter / RejectsOutOfRangeSampleRatio。
    - `internal/transport/http/health_handler_test.go` 覆盖 ReadinessHandler 全绿/失败 503/空 probe/nil probe。
    - `internal/infra/redis/script_registry_metrics_test.go` 覆盖 redis_lua_duration 上报与 errClass 分类（noscript/timeout/connection/busy/error）。
    - `internal/transport/ws/hub_metrics_test.go` 用 fakeHubMetrics 覆盖 connect/disconnect/broadcast/slow_consumer/nil-safe。
    - `internal/infra/observability/gormlogger_test.go` 新增 TraceContextInjectsTraceID / OmitsTraceIDWithoutSpan。
  - G13 AGENTS.md 新增 `## Observability` 章节（中间件顺序、metrics、tracing、logging、health、Hub、MCP/Agent/Redis hooks、新增观测点指引）。
- Verification:
  - `go build ./...` clean.
  - `go test ./...` clean — 见 packages: app, config, domain, infra/agent, infra/idgen, infra/observability(+/metrics, +/tracing), infra/redis, service, transport/http, transport/ws 全部 ok。


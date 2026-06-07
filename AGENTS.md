# AGENTS.md

Authoritative project context for AI agents working in this repository. Keep this file accurate; downstream agents rely on it before reading code.

## Project Overview

`aieas_backend` is a Go service implementing a real-time auction / live-room (拍卖直播间) backend.

- Language: Go (module `aieas_backend`, see `go.mod` for current toolchain).
- Web framework: CloudWeGo Hertz (`github.com/cloudwego/hertz`) + `hertz-contrib/websocket`.
- Persistence: MySQL via GORM (`gorm.io/driver/mysql`) + raw `database/sql` for migrations.
- Realtime/state: Redis (`github.com/redis/go-redis/v9`) — auction state, bid stream, online counter, distributed locks.
- Migrations: Goose (`github.com/pressly/goose/v3`), SQL files under `migrations/`.
- Object storage: Volcengine TOS (`ve-tos-golang-sdk`), optional, gated by config.
- Entry point: `main.go` → `internal/app.NewServer()` → `server.Hertz`.

### Domain Model (high level)
- **Item** (商品) → owned by merchant, raw catalog entity.
- **AuctionLot** (拍品) — an item put up for auction, with status machine `DRAFT → PENDING_AUDIT → READY → WARMING_UP → RUNNING → EXTENDED → HAMMER_PENDING → CLOSED_WON|CLOSED_FAILED → SETTLED`. Cancellable from non-terminal states.
- **LiveRoom** (直播间) — merchant-owned room hosting many `AuctionLot`s. Status machine `OFFLINE ↔ LIVE → CLOSED`. At most **one** lot per room may be running concurrently, enforced by `LiveRoomLock` (Redis SETNX + Lua-safe DEL).
- **Bid / Deposit / Order / Risk / Audit** — supporting aggregates. Bidding goes through Lua scripts (`scripts/lua/bid.lua`, `hammer.lua`) for atomicity.

### Layered Architecture (DDD-ish)
```
cmd/db                 # CLI: goose migrate / seed-dev
main.go                # process entry
internal/
  app/                 # composition root (server.go: NewServer, NewServerWithDependencies)
  config/              # YAML + env + .env loader, Validate
  domain/              # value objects, entities, status machines, errors
  repository/          # interfaces + GORM (mysql) + Memory implementations
  modules/*/app/       # module application layer / use cases / orchestration
  service/             # legacy test-facing compatibility package; not the main business layer
  transport/
    http/              # Hertz handlers, middleware, idempotency, response helpers
    ws/                # Hub, Client, Envelope, online counter, event relay
  infra/
    mysql/             # connection helpers
    redis/             # client, key builder, scripts, online_counter, event_log, live_room_lock
    objectstorage/     # TOS uploader (+ DisabledUploader fallback)
    observability/     # slog logger
pkg/jwt/               # JWT manager (HS256)
migrations/            # goose SQL: 00001_init_schema.sql, 00002_live_room.sql, ...
scripts/lua/           # Redis Lua scripts loaded by ScriptRegistry
configs/config.yaml    # default config; can be overridden via env / .env
docs/                  # API & 设计 docs (incl. 默认模块.openapi.json)
```

Wiring rule: `app/server.go` is the **only** place that constructs concrete repos/services and registers HTTP/WS routes. Do not introduce other composition roots; extend `ServerDependencies` instead.

## Build & Commands

All commands assume cwd = repository root.

| Task                 | Command                                                                |
|----------------------|------------------------------------------------------------------------|
| Build all            | `go build ./...`                                                       |
| Run server           | `go run .`  (loads `configs/config.yaml`, optional `.env`)             |
| Test all             | `go test ./...`                                                        |
| Test with race       | `go test -race ./...`                                                  |
| Test single package  | `go test ./internal/service -run TestName -v`                          |
| Lint                 | `golangci-lint run` (config in `.golangci.yml`: govet, ineffassign, staticcheck, unused) |
| Format               | `gofmt -w .` (or `goimports -w .`)                                     |
| Vet                  | `go vet ./...`                                                         |
| Migrate up           | `go run ./cmd/db -config configs/config.yaml migrate up`               |
| Migrate down         | `go run ./cmd/db -config configs/config.yaml migrate down`             |
| Migrate status       | `go run ./cmd/db -config configs/config.yaml migrate status`           |
| Seed dev users       | `go run ./cmd/db -config configs/config.yaml seed-dev`                 |

The `-config` flag walks up parent dirs to find the file; running from a sub-dir is fine. Migrations require a reachable MySQL DSN (default points to `127.0.0.1:3306`).

## Code Style

- Format with `gofmt`; do not introduce other formatters.
- Imports: stdlib first, then `aieas_backend/...`, then third-party (matches existing files).
- Package naming: lowercase, single-word (`service`, `repository`, `httptransport` alias for `internal/transport/http`).
- Errors:
  - Define sentinel errors at the package that owns the concept (`domain.ErrNotFound`, `domain.ErrForbidden`, `domain.ErrInvalidState`, `domain.ErrInvalidArgument`, `service.ErrLiveRoomBusy`, etc.).
  - Wrap with `fmt.Errorf("...: %w", err)`; check with `errors.Is`.
  - Handlers translate domain/service errors to HTTP via the existing helpers in `internal/transport/http/response.go`.
- Naming: exported identifiers use `CamelCase`; acronyms keep canonical case (`ID`, `URL`, `TTL`).
- JSON: tags use `lowerCamelCase` (e.g. `liveRoomId`, `currentRemainSeconds`); use `,omitempty` for optional/zero fields.
- Comments: keep doc comments above exported identifiers in Chinese or English consistent with the surrounding file (existing code is mixed). Don't add commentary for self-evident code.
- Concurrency: prefer `context.Context` as the first parameter; protect mutable state with `sync.Mutex`/`sync.RWMutex` (see `MemoryLiveRoomRepository`, `Hub`).
- Time: store `time.Time` in UTC inside repos (memory impl uses `time.Now().UTC()`); rely on MySQL DATETIME(3) for persistence.
- Money: integer cents (`int64`); never floats.

## Testing

- Unit tests live next to the code (`*_test.go`). Memory repos under `internal/repository/*_memory.go` are the default test substrates.
- Module/service compatibility tests use small fixtures (e.g. `newRealtimeAuctionFixture` in `internal/service/bid_hammer_deposit_order_test.go`).
- Integration tests against MySQL + Redis exist under `internal/app/mysql_redis_integration_test.go`; they require live services and are skipped if the env is missing.
- WebSocket Hub tests live in `internal/transport/ws/hub_test.go`.
- When adding a feature:
  1. Extend the relevant `Memory*` repo so tests can run hermetically.
  2. Add a service-level table-driven test covering happy path + key error branches (forbidden, invalid state, conflict).
  3. Run `go test ./...` and `go vet ./...` before declaring done.
- Avoid sleeps; use the existing fake clocks / direct state assertions where possible.

## Security

- Authn: JWT (HS256) issued by `pkg/jwt`. Secret comes from `JWT_SECRET` env or `jwt.secret` config; **never commit a real secret**. The default `change-me-in-local-dev` is for local only.
- Authz: role-based via `httptransport.RoleAuth(domain.RoleMerchant, ...)` route middleware. Service layer also enforces ownership via `canAccessSellerOwned` — keep both layers; do not bypass service checks even if route guard is present.
- Secrets: `.env` is git-ignored (`.gitignore`); `.env.example` is the template. Never log secrets, JWT bearer tokens, or full DSNs.
- Idempotency: state-mutating endpoints require `Idempotency-Key` header and use `IdempotencyStore` (Redis-backed in prod, memory in tests). Preserve this for any new write endpoint.
- Input validation: validate at handler boundary (parse + basic shape) and re-validate invariants in service. Treat all external input as untrusted.
- Distributed locks: `LiveRoomLock.Acquire` uses SETNX with TTL; release via Lua to avoid clobbering another holder. Always pair `Acquire` with a `Release` on every error branch (see `LiveRoomService.ActivateAuction` for the pattern).
- Rate limiting: `httptransport.NewRateLimiter(240, time.Minute)` is mounted globally; tune via code, not per-request bypasses.
- SQL: GORM with parameterized queries — never build SQL by string concatenation. Migrations are append-only; create a new `0000N_*.sql` file rather than editing applied ones.

## Configuration

- Source order (later wins): `Default()` in `internal/config/config.go` → `configs/config.yaml` → `.env` (auto-discovered up the tree) → process env vars.
- Required at startup: `server.addr`, `jwt.secret`, `jwt.accessTokenTTL > 0`, `idempotency.ttl > 0`, valid Redis DB. If `objectStorage.enabled=true`, all TOS fields must be set.
- Env var naming mirrors YAML, e.g. `MYSQL_DSN`, `REDIS_ADDR`, `JWT_SECRET`, `AUCTION_MIN_INCREMENT_CENT`, `OBJECT_STORAGE_ENABLED`.
- Observability: `observability.format` (`text` for dev/colored, `json` for prod) and `observability.slowSQLThresholdMs` (GORM slow-SQL warning threshold, ms; <=0 disables) — env names `OBSERVABILITY_FORMAT` / `OBSERVABILITY_SLOW_SQL_THRESHOLD_MS`. `text` mode auto-disables ANSI when stdout is not a TTY; GORM logs are bridged to slog (no ANSI leakage).
- Add new config: extend the struct in `internal/config/config.go`, add to `Default()`, hook into `applyEnv` if env-overridable, and update both `configs/config.yaml` and `.env.example`.

## Observability

The observability layer is wired exclusively in `internal/app/server.go`; new metrics/traces should plug into the existing primitives instead of bypassing them.

- **Middleware order is fixed** (C2): `Recovery → RequestID → Tracing → Metrics → RateLimiter → Audit`. Tracing must run before Metrics so labels stay aligned. Operational endpoints (`/metrics`, `/healthz`, `/readyz`, `/ping`) skip metrics, rate limiting, and audit via `httptransport.IsObservabilitySkipPath`.
- **Metrics** (`internal/infra/observability/metrics`) — Prometheus registry. Defaults: enabled, namespace `aieas`, exposed at `/metrics` (auth via `OBSERVABILITY_METRICS_AUTH_TOKEN`, `Bearer` or `X-Metrics-Token`). Label cardinality is locked: HTTP `route` uses Hertz `FullPath()`; status is bucketed (`2xx`/`4xx`/`5xx`); `instance` label on Redis metrics defaults to `"default"` (C8 single Redis). Never add per-user / per-auction labels.
- **Tracing** (`internal/infra/observability/tracing`) — OpenTelemetry with W3C TraceContext + Baggage propagator. Default: **disabled**, exporter `otlphttp`, sampler `parent_based_traceid_ratio`, ratio `0.1`. When enabled, `endpoint` is required for `otlphttp`/`otlpgrpc`; `stdout`/`noop` exporters are exempt. Cross-process trace propagation: HTTP via headers, Redis Streams via `traceparent`/`tracestate` XADD fields injected in `bid.lua` and re-extracted in `BidRecordWorker` (G9).
- **Logging** — slog. `text` mode is for dev (no `trace_id` injected to keep one-line output readable); `json` mode wraps the handler with `WithTraceContext`, which adds `trace_id` / `span_id` when an active span exists. GORM logs are bridged via `NewGormLogger` (no ANSI leakage); slow-SQL above `slowSQLThresholdMs` upgrades to WARN.
- **Health / readiness** — `/healthz` is liveness only (always 200 if process is up). `/readyz` runs the `ReadinessProbes` map (`mysql`, `redis`, `scripts`) wired in `buildReadinessProbes`; any probe failure returns 503 with a `components` map naming the offender. Add a probe via `ServerDependencies.ReadinessProbes`.
- **Hub metrics** are decoupled (G10): `ws.HubMetrics` is a 4-method interface (`IncWSConnect` / `IncWSDisconnect` / `ObserveWSBroadcast` / `IncWSSlowClientDisconnect`). The ws package never imports `metrics`; tests pass a fake. The concrete `*metrics.Registry` satisfies the interface and is injected from `server.go`.
- **MCP / Agent / Redis hooks** — MCP handlers emit `mcp.tool.call` / `mcp.resource.read` spans with low-cardinality status labels (G8). Outbound agent HTTP uses `otelhttp.NewTransport` (G5). Redis client carries `redisotel` tracing plus a custom `redis.Hook` (`internal/infra/redis/metrics_hook.go`) emitting `redis_command_duration_seconds` and `redis_command_errors_total` with `instance` label. Redis Lua errors are classified into low-cardinality buckets (`noscript` / `busy` / `timeout` / `connection` / `error`) by `classifyRedisLuaError`.
- **Adding observation points** — for new HTTP routes nothing extra is needed (middleware covers). For new background workers, start a span via `tracing.StartSpan(ctx, "domain.op", attrs...)` and emit a counter through the registry; for cross-process work, propagate via `tracing.InjectMap` / `ExtractMap`. Always check `Enabled()` only when the cost of building attributes is non-trivial — registry / provider methods are themselves nil-safe.

## API Surface (cheat sheet)

REST is mounted under `/api/v1` (see `internal/app/server.go`):

- Auth: `POST /auth/login`, `POST /auth/refresh`, `GET /auth/me`, `POST /auth/logout`, `POST /admin/auth/login`.
- Items (merchant/admin): `POST|GET|PATCH|DELETE /items[/:id]`.
- Auctions (merchant/admin write, all auth read state/enroll): `/auctions`, `/auctions/:id/{state,enroll,start,cancel,hammer}` (mutations are idempotent).
- Live rooms:
  - Read (any auth): `GET /live-rooms`, `GET /live-rooms/:id`, `GET /live-rooms/:id/lots`, `GET /live-rooms/:id/stats`.
  - Write (merchant/admin): `POST /live-rooms`, `PATCH /live-rooms/:id`, `DELETE /live-rooms/:id`, `POST /live-rooms/:id/{activate,deactivate,lots}` (idempotent), `DELETE /live-rooms/:id/lots/:auctionId`.
- Orders: `/orders`, `/orders/mine`, `/orders/:id`, `POST /orders/:id/pay`.
- Admin: `/admin/auctions`, `/admin/users`, `/admin/blacklist`, `/admin/orders`, `/admin/audit-logs`, `/admin/risk/events`.

WebSocket:
- `GET /ws/auctions/:auction_id` — direct auction stream.
- `GET /ws/live-rooms/:room_id` — routes to the room's current `ActiveAuctionID` Hub channel.

OpenAPI 文档存放于 `docs/API/`：聚合主文件 `docs/API/AI电商拍卖系统.apifox.json`，模块文件如 `docs/API/默认模块.openapi.json`、`docs/API/商品图片上传接口更新.openapi.json` 等。

**新增 OpenAPI 接口时必须新增独立文件，不要改写已有的 OpenAPI 文件。** 规则：

- 文件位置：`docs/API/<功能名>.openapi.json`，命名描述本次新增接口的语义（例：`直播间统计.openapi.json`）。
- 文件格式：参照 `docs/API/AI电商拍卖系统.apifox.json` 的结构（`openapi`/`info`/`paths`/`components` 等顶层字段齐全），确保独立文件本身是合法 OpenAPI，可被 Apifox 直接导入。
- 范围：每个独立文件只承载本次新增的 paths 与必要 components；公共 schema 通过 `$ref` 引用现有定义或在该文件内自包含，不要回写到聚合主文件或既有模块文件。
- 既有文件（`AI电商拍卖系统.apifox.json`、`默认模块.openapi.json` 等）原则上**只读**，仅在修复明显错误时才直接编辑，并在 commit message 中说明。

## Conventions for Agents

- Read `internal/app/server.go` first to understand wiring; that file enumerates every service and route.
- Prefer extending an existing file over creating a new one. New files only when introducing a clearly separate concept.
- When changing schema: add a new `migrations/0000N_*.sql` (Up + Down), update `docs/ddl.sql`, and bump GORM mappings in `internal/repository/*_mysql*.go`.
- When adding API endpoints: add a **new** `docs/API/<功能名>.openapi.json`，参考 `docs/API/AI电商拍卖系统.apifox.json` 的格式；不要修改已有 OpenAPI 文件。
- When adding a service dependency: pass it through `ServerDependencies` and the `newServerWithServices` signature; add a memory fallback in `NewServerWithDependencies` so tests stay hermetic.
- Don't introduce backwards-compat shims unless explicitly requested; delete unused code.
- Keep handlers thin: parse → call service → translate error → write response via helpers in `internal/transport/http/response.go`.
- Money is integer cents (`int64`). IDs are `uint64` for entities (lots, rooms, items) and `string` for users (`u_xxxx`).

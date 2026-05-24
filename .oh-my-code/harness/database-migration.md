# Harness Graph: database-migration

> Status: in_progress

## User Intent
- Original: We need implement the user's database migration engineering plan in /Users/bytedance/study/AI电商/backend. Use goose migrations manually executed via cmd/db. Please analyze repo state, create a task graph, and delegate/execute nodes as appropriate in your workspace. Required outcome: migrations/00001_init_schema.sql with goose Up/Down from current DDL excluding demo seed, cmd/db supporting -config, migrate up/down/status, seed-dev using config mysql.dsn and MYSQL_DSN env override via existing config loader, idempotent dev seed for buyer001/merchant001/admin001/disabled001, tests for cmd/db argument parsing at least, and go test ./... green. Do not revert user changes. Return changed files and verification evidence.
- Acceptance: Repo contains the requested goose migration, cmd/db supports config-driven migrate and seed-dev workflows with MYSQL_DSN override, dev seed is idempotent for buyer001/merchant001/admin001/disabled001, argument parsing tests exist, and `go test ./...` passes without reverting user changes.

## Assurance
- Mode: ralph_strict
- Why: User requires an implementation plus `go test ./...` green, so final verification is a required gate and failures must trigger bounded repair.

## Gates
### g1 final-verification
- Status: pending
- Acceptance: Independent verifier confirms required files/behavior and reports `go test ./...` passing.
- Repair: 0/2

## Understanding
- Graph selected: n1 explore repo state, n2 implement migration/CLI/tests, n3 independently verify.
- Current schema source is `docs/ddl.sql`; its schema block should become goose `Up`, while demo user/item/auction/config inserts must stay out of the migration.
- `migrations/` exists but is empty.
- Existing config loader maps `mysql.dsn` and already supports `MYSQL_DSN` override.
- Add a new `cmd/db` command tree; keep argument parsing pure and unit tested.
- Seed-dev should upsert the four requested accounts with repository password hashing, separate from schema migration.
- The repo already has unrelated dirty/untracked changes; do not revert them.
- Implementation added `migrations/00001_init_schema.sql`, `cmd/db`, parser/dispatch/seed tests, and goose/MySQL dependency metadata. Executor reported fresh `go test -count=1 ./...` passing.

## Completed
### n1 @omc-explore — DONE_WITH_CONCERNS
- Completed At: 2026-05-23T18:44:32+08:00
- Summary: Identified `docs/ddl.sql` as DDL source, confirmed demo seed exclusion boundary, found config/mysql DSN behavior, recommended `cmd/db` command layout and standard Go test conventions. Baseline `go test ./...` passed before implementation.
- Concerns: Docs reference `golang-migrate` but user requires goose; config contains real-looking DSN values, so implementation/tests should avoid printing or hardcoding secrets. Seed IDs need a deliberate choice because DB IDs are BIGINT while memory seeds use `u_` prefixed strings.
### n2 @omc-executor — DONE
- Completed At: 2026-05-23T18:50:49+08:00
- Summary: Implemented goose migration and `cmd/db` CLI for `migrate up|down|status` and `seed-dev`; seed-dev uses existing password hashing and numeric IDs 1001/2001/9001/1002. Added parser/dispatch tests and seed invariant tests.
- Deliverables: `migrations/00001_init_schema.sql`; `cmd/db/main.go`, `cmd/db/cli.go`, `cmd/db/db.go`, `cmd/db/seed.go`, `cmd/db/cli_test.go`, `cmd/db/seed_test.go`; `go.mod`/`go.sum` dependency metadata.
- Evidence: Executor reported migration has goose Up/Down, no `INSERT`, schema matches `docs/ddl.sql` schema section ignoring comments/blanks, and `go test -count=1 ./cmd/db` plus `go test -count=1 ./...` passed.
- Concerns: Workspace still contains unrelated pre-existing dirty/untracked files.

## Pending
### n3 @omc-verifier — depends:n2 — gate:g1
- Goal: Independently verify changed files and behavior.
- Acceptance: Confirms goose Up/Down migration exists and excludes demo seed, cmd/db argument parsing and DSN behavior are covered, seed-dev is idempotent in code, and `go test ./...` passes.

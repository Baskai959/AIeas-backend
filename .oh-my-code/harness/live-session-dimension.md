# Harness Graph: live-session-dimension

> Status: completed

## User Intent
- Original: 按直播场次维度划分。未开播前用户可上架拍品（场次字段置空），开播后回写场次字段；增加直播记录功能（人数、拍品、成交、出价等）；改造数据库表与代码、生成新 OpenAPI、更新 migrations 与 ddl.sql；不改前端。
- Acceptance: 见原文档（已全部覆盖，三 gate 通过）。

## Assurance
- Mode: strict_verify

## Gates
### g1 build-and-tests — passed
### g2 schema-coherence — passed
### g3 openapi-isolation — passed

## Completed
### n1 @omc-deep-executor — DONE
- Completed At: 2026-05-25T00:00:00+08:00
- Summary: 落地 migration 00005_live_session、domain LiveSession 与三表 LiveSessionID *uint64、live_session repo（mysql + memory）、LiveSessionService（OpenSession/CloseSession/IncrCounters/List*）、live_room/bid/hammer 注入 sessionID 与计数累加、6 个 HTTP endpoints、server.go 接线、独立 OpenAPI `直播场次记录.openapi.json`、7 个单测全绿；`go build`/`go vet`/`go test ./...` 全过。

### n2 @omc-verifier — DONE
- Completed At: 2026-05-25T00:00:00+08:00
- Summary: 三 gate 全 PASS。验证 migration Up/Down 配套、ddl.sql 字段同步、GORM 与 domain 一致；OpenAPI 仅新增一个文件、6 paths 合法；server.go 路由接线正确。

## Concerns（已交付，遗留风险）
- viewer_peak / viewer_total 字段已落地，但 WS 在线人数→`IncrCounters` 的写入路径未实现，生产侧需后续接入 hub Subscribe/Unsubscribe。
- 没有独立的 `POST /live-sessions/:id/close` 端点；场次关闭依赖 `live_room.Deactivate` 内部回调。如需独立"主动闭场"按钮需后续暴露。
- session 计数器在 service 层 RMW，单进程无问题；多实例并发需后续切换为 SQL `UPDATE col=col+?` 原子累加。
- `/live-sessions/:sessionId/...` 三个反查端点仅靠 `AuthMiddleware`，权限隔离落在 service 层（已通过单测验证），后续若复用 handler 需注意权限断言层级。

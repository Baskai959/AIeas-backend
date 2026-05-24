# Harness Graph: redis-online-count

> Status: completed

## User Intent
- Original: 完成多实例在线人数 Redis 化
- Acceptance: 在线人数不再仅依赖单实例内存统计；多实例连接/断开同一拍场房间时在线人数可通过 Redis 聚合保持一致；实现具备降级/测试覆盖；`go test ./...` 通过。

## Assurance
- Mode: strict_verify
- Why: 涉及实时 WebSocket 多实例一致性，需实现后由独立验证确认行为与测试健康。

## Gates
### g1 redis-online-count-verification
- Status: passed
- Acceptance: 实现 Redis 化在线人数或等价共享存储方案；单实例内存路径不被破坏；新增/更新测试覆盖多实例在线人数；全量 Go 测试通过或给出明确环境性阻塞。
- Repair: 0/2

## Understanding
- 当前请求聚焦“多实例在线人数 Redis 化”，范围应限制在在线人数统计/WS 房间连接计数相关代码，不扩展到其他实时能力重构。
- 实现节点报告：Hub 已接入可选 `OnlineCounter` 抽象，生产路径注入 Redis ZSET 计数器，未配置或失败时回退本地内存计数；Redis member 使用实例前缀 + clientID，避免不同实例同 clientID 冲突。
- 实现节点报告：新增多 Hub 聚合、慢消费者移除幂等扣减、共享计数器失败回退等测试，并声明 `go test ./...` 已通过。
- 验证节点确认：生产 `NewServerWithConfig` 已注入 Redis online counter；两个 Hub 共享 counter 的多实例聚合、fallback、慢消费者/重复退订幂等均有测试覆盖；`go test -count=1 ./...` 通过。
- 验证节点关注：`go vet ./...` 存在既有 `internal/transport/http/ws_handler_test.go:65` 复制 lock value 问题，不影响本 gate；Redis online counter 暂无真实 Redis ZSET/TTL 专门测试。

## Completed
### n1 @omc-deep-executor — DONE
- Completed At: 2026-05-23T20:53:44+08:00
- Summary: 完成 Redis 化在线人数实现，新增内存/Redis 在线计数器与 Hub 注入，更新连接/断开/慢消费者路径，补充测试并自测通过。

### n2 @omc-verifier — DONE_WITH_CONCERNS
- Completed At: 2026-05-23T20:56:37+08:00
- Summary: g1 gate 通过；独立验证代码接入、测试覆盖和 `go test -count=1 ./...` 通过，记录非阻塞 vet/Redis 专项测试关注点。

## Pending

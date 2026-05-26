# Harness Graph: live-session-followup

> Status: completed

## User Intent
- Original:
  1. WebSocket 改造到对应场次直播。
  2. 关播只关商家直播间，但要同步处理拍品状态。
  3. 多实例冲突检查；计数器改 Redis + 同步 SQL。
- Acceptance: 见原文档；四个 gate 全部通过。

## Assurance
- Mode: strict_verify

## Gates
### g1 build-and-tests — passed
### g2 close-state-coherence — passed
### g3 ws-session-routing — passed
### g4 counter-redis-coherence — passed

## Completed
### n3 @omc-explore — DONE
- Completed At: 2026-05-25T00:00:00+08:00
- Summary: 诊断关播路径不收尾拍品（Lua 仍接受 bid、保证金未释放），建议走 Hammer(Force=true)；诊断 IncrCounters 是 service mutex RMW，多实例不互斥，建议 HINCRBY + viewer_peak Lua CAS + close 时 flush。

### n4 @omc-deep-executor — DONE
- Completed At: 2026-05-25T00:00:00+08:00
- Summary: 落地 (1) DeactivateAuction 内调 hammer.Hammer Force=true 收尾 lot；(2) WS envelope 加 liveSessionId、hub 加 sessionID→clients 反向索引与 BroadcastSessionEnd、ws_handler 接 sessionID、CloseSession 通过 onEnded 钩子触发广播；(3) 新建 redis live_session_realtime store（HINCRBY + Lua CAS + Reset）+ memory fallback + service.FlushCountersToDB 在 close 时一次性同步。新增 7+ 测试。

### n5 @omc-verifier — DONE
- Completed At: 2026-05-25T00:00:00+08:00
- Summary: 四 gate 全 PASS。build/vet/test 全绿；Deactivate 三个用例 PASS；Hub session 索引/广播/Stats 三个用例 PASS；realtime store + Flush + ViewerPeak max + onEnded hook 四个用例 PASS；原 7 个 LiveSession 用例不破坏。

## Concerns（已交付，遗留风险）
- BroadcastSessionEnd 是实例本地索引；多实例部署需补 Redis Pub/Sub 跨实例广播。
- BumpViewerPeak 的 GET 假设 key 类型为 STRING；外部误写为 HASH 会报错（建议加 pcall 防御）。
- IncrCounters 在 realtime store 报错时 fail-fast；是否回落 RMW 看后续韧性需求。
- Hub.BroadcastSessionEnd 仅清 session 索引不执行 room Unsubscribe，依赖客户端收到事件后主动断连的契约（前端约定）。

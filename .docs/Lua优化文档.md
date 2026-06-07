1. 最大优化点：把 Lua 缩成“只做竞价裁决”

你现在 Lua 做了这些事：

幂等判断
报名校验
保证金校验
限频
读取状态
解析加价规则 JSON
价格校验
防狙击延时
状态更新
写 Stream
PUBLISH 广播
排行榜更新
活跃拍品集合维护
构造完整 JSON 响应

但真正必须原子的只有：

读取当前价
校验出价是否有效
更新最高价/领先者/version/end_ts
写一条可靠事件
返回裁决结果

建议目标是把 Lua 压缩成：

GET/HMGET state
价格校验
HSET state
XADD accepted/rejected event 可选
SET idem
return 简短数组

不要让 Lua 直接负责广播、排行榜、复杂 JSON 构造。

⸻

2. PUBLISH 建议移出 Lua

你现在成功路径里有：

redis.call("PUBLISH", channel, payload)

这个建议去掉。

原因是 PUBLISH 不是竞价状态一致性的核心操作。它属于“通知层”。现在它在 Lua 内部，会增加每次成功出价的 Redis 执行时间。

建议改成：

Lua:
  HSET state
  XADD stream
  return result
Go Stream Consumer:
  XREADGROUP
  PUBLISH / WebSocket 广播
  落库 MySQL
  更新排行榜

也就是：

竞价裁决链路：短、强一致
消息广播链路：异步、可重试

这样 ACK 延迟会明显下降。

⸻

3. 排行榜更新建议异步化

你现在成功出价后做：

local old_member = redis.call("HGET", user_bids_key, bidder_id)
if old_member and old_member ~= false then
  redis.call("ZREM", ranking_key, old_member)
end
local ranking_member = string.format("%019d:%013d:%s", price, 9999999999999 - now_ms, bidder_id)
redis.call("ZADD", ranking_key, 0, ranking_member)
redis.call("HSET", user_bids_key, bidder_id, ranking_member)

这个在核心路径里偏重。

如果排行榜只是页面展示，不是判断出价成功的必要条件，可以移到 Stream 消费者里异步维护。

核心 Lua 只保留：

HSET state current_price leader_bidder_id version bid_count end_ts_ms
XADD bid event

排行榜由消费者根据 accepted event 更新。

这样可以减少一次成功出价里的：

HGET
ZREM
ZADD
HSET
string.format

这些操作。

⸻

4. increment_rule 不要每次 Lua 里 JSON 解析

你每次请求都会执行：

local ok, parsed = pcall(cjson.decode, increment_rule)

这在高 QPS 下很亏。加价规则一般不会频繁变化，不应该每次出价都解析 JSON。

建议你在创建拍卖或规则变更时，提前写入 Redis Hash：

increment_type = fixed / ladder
current_increment_amount = 100
current_max_bid_steps = 5
next_ladder_threshold = 1000000

Lua 中直接读：

local increment_amount = tonumber(status_fields[x]) or min_increment
local max_bid_steps = tonumber(status_fields[y]) or 1

如果是阶梯加价，可以只在价格跨区间时由后端或 Lua 更新当前档位，不要每次解析整个 JSON。

你现在这段：

for _, step in ipairs(rule_steps) do
  ...
end

如果阶梯多，会进一步拉长 Lua 执行时间。

⸻

5. 拒绝路径也写幂等，可能放大低价请求成本

你的拒绝函数：

local function reject(reason)
  local result = build_result(false, reason, current_price, leader_id, end_ts, extend_count, false, version, 0, "", false, status, false)
  if request_id ~= "" then
    redis.call("SET", idem_key, result, "PX", idem_ttl_ms)
  end
  return result
end

这意味着所有 rejected 请求都会：

cjson.encode
SET idem_key PX

如果压测里大量低价请求、状态过期请求、频控请求，Redis 会被这些“无效请求的幂等写入”拖慢。

可以考虑：

方案 A：只对 accepted 写完整幂等结果

拒绝请求只返回，不写幂等：

if accepted then
  SET idem_key result PX
end

缺点是重复 rejected 请求不会复用旧结果。

方案 B：只对部分拒绝原因写幂等

比如这些可以不写：

BELOW_MIN_INCREMENT
STALE_AUCTION_STATE
PRICE_STEP_MISMATCH
FREQ_LIMIT

这些多数是瞬时状态导致的失败，不一定值得缓存。

方案 C：拒绝结果写极简值

不要写完整 JSON：

redis.call("SET", idem_key, "REJECT:" .. reason, "PX", idem_ttl_ms)

Go 层再补全响应字段。

⸻

6. 返回值不要在 Lua 里构造完整 JSON

你现在每次 accepted/rejected 都执行：

cjson.encode(result)

而且 result 字段非常多：

requestId
auctionId
liveSessionId
bidderId
bidderNickname
price
accepted
reason
currentPrice
leaderBidderId
endTsMs
extended
extendCount
version
seq
streamId
createdAtMs
bidTsMs
source
duplicate
event
riskResult
auctionStatus
autoClosed

这对 Lua 来说不是免费的。

建议 Lua 返回数组，让 Go 组装 JSON：

return {
  accepted and 1 or 0,
  reason,
  tostring(current_price),
  leader_id,
  tostring(end_ts),
  tostring(version),
  tostring(seq),
  stream_id,
  status
}

Go 后端拿到数组后再构造 ACK JSON。

这样 Lua 少做：

table 构造
cjson.encode
大量字符串 key 处理

在高 QPS 下会有明显收益。

⸻

7. live_session_id 尽量不要 Lua 里兜底读取

你现在：

local live_session_id = number_or_zero(live_session_id_arg)
if live_session_id == 0 then
  live_session_id = number_or_zero(redis.call("HGET", state_key, "live_session_id"))
end

但后面 HMGET state_key 又读了很多字段。

建议直接把 live_session_id 放进前面的 HMGET，不要额外 HGET 一次。

例如：

local status_fields = redis.call("HMGET", state_key,
  "status",
  "current_price",
  ...
  "bid_count",
  "live_session_id"
)

这样可以减少一次 Redis 内部命令调用。

⸻

8. active_streams_key 不建议放在核心 Lua 里

你现在成功路径有：

redis.call("SADD", active_streams_key, tostring(auction_id))

这个问题有两个：

第一，它增加成功出价路径耗时。

第二，如果你以后上 Redis Cluster，它很可能是全局 key，会破坏按 auction_id 分片的设计。Redis Cluster 下，多 key Lua 要求相关 key 落在同一个 slot，否则脚本无法正常执行多 key 操作。

建议移到异步消费者：

Stream Consumer 收到 accepted event 后：
SADD active_streams_key auction_id

或者改成分片内 key：

auction:{auction_id}:active_marker

⸻

9. 限频位置可以调整

现在你在价格校验前做限频：

local count = redis.call("INCR", freq_key)
if count == 1 then
  redis.call("PEXPIRE", freq_key, freq_window_ms)
end
if count > freq_limit_count then
  return reject("FREQ_LIMIT")
end

如果大量低价请求进来，它们也会写 freq_key。

你可以分两种业务策略：

策略一：低价乱点也要计入限频

那保留现在的位置。

策略二：明显无效价格不计入限频

那先做价格快速校验，再做限频。

例如先判断：

if price <= current_price then
  return reject("BELOW_MIN_INCREMENT")
end

再做：

INCR freq_key
PEXPIRE freq_key

这样低价压测时 Redis 写操作会减少。

⸻

10. expected_current_price 可以更早拒绝

你现在有：

if expected_current_price > current_price then
  return reject("STALE_AUCTION_STATE")
end

这个判断有点奇怪。

通常客户端携带的 expected_current_price 如果小于 Redis 当前价，说明客户端状态落后，更像是 stale：

if expected_current_price < current_price then
  return reject("STALE_AUCTION_STATE")
end

你现在的逻辑是：

expected_current_price > current_price 才拒绝

也就是客户端认为价格比 Redis 更高才拒绝。这个可以解释为你不信任客户端超前状态，但对于拍卖场景，更常见的问题是客户端价格落后。

当然你后面又有：

if expected_current_price < current_price and price > expected_max_allowed then
  return reject("ABOVE_EXPECTED_MAX_BID_STEPS")
end

以及：

elseif price < current_price + increment_amount then
  return reject("BELOW_MIN_INCREMENT")
end

所以落后请求最终也会被拒绝，但会走更多逻辑。

建议增加一个更早的快速拒绝策略：

if expected_current_price < current_price then
  local min_need = current_price + increment_amount
  if price < min_need then
    return reject("STALE_AUCTION_STATE")
  end
end

或者更强一点：

if expected_current_price ~= current_price then
  return reject("STALE_AUCTION_STATE")
end

但这个会降低并发容忍度。是否采用取决于你的业务：
如果你希望客户端必须基于最新价格出价，就用严格模式；如果允许客户端一次加多步追价，就保留宽松模式。

⸻

11. 成功事件可以先 XADD，后 HSET 吗？

你现在顺序是：

local seq, stream_id = append_accepted_event(...)
HSET state ...

也就是先写 Stream，再写状态。

由于 Lua 是原子执行，外部看不到中间状态，所以一致性上没问题。

但是从语义上我更建议：

HSET state
XADD event
return

这样脚本结构更清晰：先完成状态变更，再记录事件。

不过这不是性能大头，只是可维护性优化。

⸻

12. XADD MAXLEN ~ 10000 也可能带来抖动

你现在每次成功出价：

redis.call("XADD", stream_key, "MAXLEN", "~", "10000", stream_id, ...)

MAXLEN ~ 是近似裁剪，比精确裁剪好，但仍可能在某些时候触发额外工作。

如果你对 ACK 延迟极度敏感，可以考虑：

核心 Lua 只 XADD，不裁剪
后台任务定期 XTRIM

例如：

每隔 1s 或 5s：
XTRIM auction:{id}:stream MAXLEN ~ 10000

这样可以减少成功路径上的偶发延迟抖动。

⸻

13. key 设计要为 Redis Cluster 做准备

你现在有 10 个 KEYS：

state_key
ranking_key
idem_key
enrolled_key
deposit_key
freq_key
user_bids_key
stream_key
seq_key
active_streams_key

如果以后用 Redis Cluster，所有 key 必须落在同一个 hash slot。

建议所有单拍品相关 key 统一用 hash tag：

auction:{55144997847552}:state
auction:{55144997847552}:ranking
auction:{55144997847552}:idem:{request_id}
auction:{55144997847552}:enrolled
auction:{55144997847552}:deposit
auction:{55144997847552}:freq:{bidder_id}
auction:{55144997847552}:user_bids
auction:{55144997847552}:stream
auction:{55144997847552}:seq

但这个：

active_streams_key

如果是全局的，不适合放在这个 Lua 里。

⸻

14. 可以拆成两个 Lua：快速拒绝脚本 + 成功裁决脚本吗？

可以，但一般不建议拆太复杂。

更好的方式是：

Go 层预过滤明显无效请求
Lua 层做最终原子裁决

Go 层缓存：

current_price
increment_amount
max_bid_steps
status
version
end_ts_ms

Go 层只做安全拒绝：

price <= cached_current_price
price < cached_current_price + increment_amount
auction not running
client expected version clearly stale

只要 Go 判断“可能成功”，再进入 Lua。

这样可以把大量低价请求挡在 Redis 外面。

注意：
Go 层不能直接判定成功，成功必须由 Lua 决定。Go 层只适合做“明显失败”的预过滤。

⸻

15. 推荐的优化后 Lua 结构

目标版本可以变成这样：

-- 1. 幂等判断
local existing = redis.call("GET", idem_key)
if existing then
  return existing
end
-- 2. 一次 HMGET 读取必要状态
local fields = redis.call("HMGET", state_key,
  "status",
  "current_price",
  "start_price",
  "cap_price",
  "leader_bidder_id",
  "end_ts_ms",
  "extend_count",
  "version",
  "bid_count",
  "increment_amount",
  "max_bid_steps"
)
-- 3. 状态快速拒绝
-- 4. 价格快速拒绝
-- 5. 频控，可选
-- 6. 防狙击延期
-- 7. HSET 状态
-- 8. XADD 事件
-- 9. SET 幂等
-- 10. 返回短数组，不返回大 JSON

然后把这些移出 Lua：

PUBLISH
排行榜 ZSET 更新
active_streams SADD
复杂 JSON 构造
复杂 increment_rule JSON 解析
部分 rejected 幂等写入

⸻

16. 我建议你按这三个版本迭代

V1：低成本修改版

马上能做，风险小：

去掉 Lua 内 PUBLISH
去掉 Lua 内 active_streams_key SADD
返回数组，不返回完整 JSON
live_session_id 放进 HMGET

预计收益：ACK 延迟下降，成功路径变短。

⸻

V2：核心路径瘦身版

需要改一点业务链路：

排行榜异步维护
拒绝请求不写完整幂等 JSON
increment_rule 预计算，不在 Lua 里 decode
XTRIM 后台做

预计收益：单拍品热点 QPS 明显提升。

⸻

V3：平台级扩展版

架构层优化：

Go 层本地状态缓存，预过滤低价请求
Redis Cluster 按 auction_id 分片
全局 key 从 Lua 中移除
Stream Consumer 负责广播、排行榜、落库

预计收益：多拍品整体 QPS 提升，单热点延迟更稳定。

⸻

最值得你先改的 5 个点

按性价比排序：

1. PUBLISH 移出 Lua
2. 排行榜 ZREM/ZADD/HSET 移出 Lua
3. increment_rule 不要每次 cjson.decode
4. Lua 返回数组，不返回完整 JSON
5. 低价请求在 Go 层预过滤，别全部进 Redis

你的当前脚本适合“功能完整性”，但不适合“单拍品 1000 QPS 热点竞价”。压测文档里也可以这么写：

当前版本采用 Redis Lua 保证单拍品竞价裁决的原子性，但脚本承担了状态校验、资格校验、频控、事件写入、广播、排行榜维护和响应构造等多项职责。在单拍品高并发场景下，所有请求集中进入同一 Lua 脚本串行执行，导致 Redis 执行队列积压。后续优化方向是将 Lua 收敛为最小竞价裁决单元，将广播、排行榜、活跃流维护和复杂响应构造迁移到异步链路。
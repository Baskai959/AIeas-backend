package redis

import "strings"

const (
	ScriptBidPlace       = "bid.place"
	ScriptHammer         = "auction.hammer"
	ScriptRateLimit      = "ratelimit.token_bucket"
	ScriptMarkEnrollment = "auction.mark_enrollment"
)

func DefaultScripts() map[string]string {
	return map[string]string{
		ScriptBidPlace:       strings.TrimSpace(bidLua),
		ScriptHammer:         strings.TrimSpace(hammerLua),
		ScriptRateLimit:      strings.TrimSpace(rateLimitLua),
		ScriptMarkEnrollment: strings.TrimSpace(markEnrollmentLua),
	}
}

const markEnrollmentLua = `
local enrolled_key = KEYS[1]
local deposits_key = KEYS[2]
local state_key = KEYS[3]
local user_id = ARGV[1]

if user_id == nil or user_id == "" then
  return redis.error_reply("invalid enrollment arguments")
end

local added = redis.call("SADD", enrolled_key, user_id)
redis.call("SADD", deposits_key, user_id)

if added == 1 and redis.call("EXISTS", state_key) == 1 then
  redis.call("HINCRBY", state_key, "participant_count", 1)
  redis.call("HINCRBY", state_key, "version", 1)
end

return added
`

const bidLua = `
local state_key = KEYS[1]
local ranking_key = KEYS[2]
local idem_key = KEYS[3]
local freq_key = KEYS[4]
local user_bids_key = KEYS[5]
local stream_key = KEYS[6]
local seq_key = KEYS[7]

local request_id = ARGV[1]
local auction_id = tonumber(ARGV[2])
local bidder_id = ARGV[3]
local price = tonumber(ARGV[4])
local now_ms = tonumber(ARGV[5])
local min_increment = tonumber(ARGV[6])
local anti_snipe_ms = tonumber(ARGV[7])
local extend_ms = tonumber(ARGV[8])
local max_extend_count = tonumber(ARGV[9])
local freq_limit_count = tonumber(ARGV[10])
local freq_window_ms = tonumber(ARGV[11])
local idem_ttl_ms = tonumber(ARGV[12])
local source = ARGV[13]
local anti_extend_mode = ARGV[14]
local expected_current_price_raw = ARGV[15]
local trace_parent = ARGV[16]
local trace_state = ARGV[17]
local live_session_id_arg = ARGV[18]
local bidder_nickname = ARGV[19]
local bidder_avatar_url = ARGV[20]

if request_id == nil then request_id = "" end
if bidder_id == nil then bidder_id = "" end
if source == nil or source == "" then source = "live_ws" end
if anti_extend_mode == nil or anti_extend_mode == "" then anti_extend_mode = "ADD" end
anti_extend_mode = string.upper(tostring(anti_extend_mode))
if trace_parent == nil then trace_parent = "" end
if trace_state == nil then trace_state = "" end
if bidder_nickname == nil then bidder_nickname = "" end
if bidder_avatar_url == nil then bidder_avatar_url = "" end

if auction_id == nil or auction_id <= 0 or bidder_id == "" or price == nil or price <= 0 or now_ms == nil or now_ms <= 0 then
  return redis.error_reply("invalid bid arguments")
end
if min_increment == nil then min_increment = 1 end
if anti_snipe_ms == nil then anti_snipe_ms = 0 end
if extend_ms == nil then extend_ms = 0 end
if max_extend_count == nil then max_extend_count = 0 end
if freq_limit_count == nil then freq_limit_count = 0 end
if freq_window_ms == nil then freq_window_ms = 0 end
if idem_ttl_ms == nil or idem_ttl_ms <= 0 then idem_ttl_ms = 30000 end

local function number_or_zero(value)
  if value == false or value == nil or value == "" then
    return 0
  end
  return tonumber(value) or 0
end

local function string_or_empty(value)
  if value == false or value == nil then
    return ""
  end
  return value
end

local live_session_id_from_arg = number_or_zero(live_session_id_arg)

local function build_result_array(accepted, reason, current_price, leader_id, end_ts, extend_count, extended, version, seq, stream_id, duplicate, auction_status, auto_closed, result_live_session_id)
  return {
    request_id,
    auction_id,
    result_live_session_id,
    bidder_id,
    bidder_nickname,
    price,
    accepted and 1 or 0,
    duplicate and 1 or 0,
    reason or "",
    current_price,
    leader_id,
    end_ts,
    extended and 1 or 0,
    extend_count,
    version,
    seq,
    stream_id,
    accepted and "bid.accepted" or "bid.rejected",
    accepted and "ALLOW" or "REJECT",
    auction_status or "",
    auto_closed and 1 or 0,
    now_ms,
    now_ms,
    bidder_avatar_url
  }
end

local function legacy_result_to_array(decoded)
  return {
    decoded["requestId"] or request_id,
    tonumber(decoded["auctionId"]) or auction_id,
    tonumber(decoded["liveSessionId"]) or live_session_id_from_arg,
    decoded["bidderId"] or bidder_id,
    decoded["bidderNickname"] or bidder_nickname,
    tonumber(decoded["price"]) or price,
    decoded["accepted"] and 1 or 0,
    1,
    decoded["reason"] or "",
    tonumber(decoded["currentPrice"]) or 0,
    decoded["leaderBidderId"] or "",
    tonumber(decoded["endTsMs"]) or 0,
    decoded["extended"] and 1 or 0,
    tonumber(decoded["extendCount"]) or 0,
    tonumber(decoded["version"]) or 0,
    tonumber(decoded["seq"]) or 0,
    decoded["streamId"] or "",
    decoded["event"] or (decoded["accepted"] and "bid.accepted" or "bid.rejected"),
    decoded["riskResult"] or (decoded["accepted"] and "ALLOW" or "REJECT"),
    decoded["auctionStatus"] or "",
    decoded["autoClosed"] and 1 or 0,
    tonumber(decoded["createdAtMs"]) or now_ms,
    tonumber(decoded["bidTsMs"]) or now_ms,
    decoded["bidderAvatarUrl"] or decoded["avatarUrl"] or bidder_avatar_url
  }
end

local existing = redis.call("GET", idem_key)
if existing then
  local ok, decoded = pcall(cjson.decode, existing)
  if ok and type(decoded) == "table" then
    if decoded[1] ~= nil then
      decoded[8] = 1
      return decoded
    end
    return legacy_result_to_array(decoded)
  end
  return existing
end

local status_fields = redis.call("HMGET", state_key,
  "status",
  "current_price",
  "start_price",
  "cap_price",
  "leader_bidder_id",
  "end_ts_ms",
  "extend_count",
  "version",
  "increment_rule",
  "increment_amount",
  "max_bid_steps",
  "bid_count",
  "increment_rule_type",
  "increment_fixed_amount",
  "live_session_id"
)
local status = string_or_empty(status_fields[1])
local current_price = number_or_zero(status_fields[2])
local start_price_raw = status_fields[3]
local start_price = number_or_zero(start_price_raw)
local cap_price = number_or_zero(status_fields[4])
local leader_id = string_or_empty(status_fields[5])
local end_ts = number_or_zero(status_fields[6])
local extend_count = number_or_zero(status_fields[7])
local version = number_or_zero(status_fields[8])
local increment_rule = string_or_empty(status_fields[9])
local increment_amount_raw = status_fields[10]
local max_bid_steps_raw = status_fields[11]
local bid_count = number_or_zero(status_fields[12])
local increment_rule_type = string.lower(string_or_empty(status_fields[13]))
local increment_fixed_amount = number_or_zero(status_fields[14])
local live_session_id_state = number_or_zero(status_fields[15])

local live_session_id = live_session_id_from_arg
if live_session_id == 0 then
  live_session_id = live_session_id_state
end

local function append_accepted_event(current_price, leader_id, end_ts, extend_count, extended, auction_status, auto_closed)
  local seq = redis.call("INCR", seq_key)
  local stream_id = tostring(seq) .. "-0"
  redis.call("XADD", stream_key, stream_id,
    "request_id", request_id,
    "auction_id", tostring(auction_id),
    "live_session_id", tostring(live_session_id),
    "bidder_id", bidder_id,
    "bidder_nickname", bidder_nickname,
    "bidder_avatar_url", bidder_avatar_url,
    "bid_price", tostring(price),
    "bid_ts_ms", tostring(now_ms),
    "source", source,
    "risk_result", "ALLOW",
    "reject_reason", "",
    "accepted", "1",
    "current_price", tostring(current_price),
    "leader_bidder_id", leader_id,
    "end_ts_ms", tostring(end_ts),
    "extended", extended and "1" or "0",
    "extend_count", tostring(extend_count),
    "auction_status", auction_status,
    "auto_closed", auto_closed and "1" or "0",
    "seq", tostring(seq),
    "stream_id", stream_id,
    "created_at_ms", tostring(now_ms),
    "event_type", "bid.accepted",
    "traceparent", trace_parent,
    "tracestate", trace_state
  )
  return seq, stream_id
end

local function positive_number(value)
  local parsed = tonumber(value)
  if parsed ~= nil and parsed > 0 then
    return parsed
  end
  return nil
end

if (start_price_raw == false or start_price_raw == nil or start_price_raw == "") and current_price > 0 then
  start_price = current_price
end

local rule_decoded_ok = false
local rule_decoded = nil
local rule_type = ""
local rule_max_steps_override = nil
local rule_fixed_amount = nil
local rule_steps = nil
local fast_fixed = false

if increment_rule_type == "fixed" and increment_fixed_amount > 0 then
  fast_fixed = true
  rule_type = "fixed"
  rule_fixed_amount = increment_fixed_amount
else
  if increment_rule ~= "" then
    local ok, parsed = pcall(cjson.decode, increment_rule)
    if ok and type(parsed) == "table" then
      rule_decoded_ok = true
      rule_decoded = parsed
    end
  end
  if rule_decoded_ok then
    local parsed_steps = tonumber(rule_decoded["maxBidSteps"])
    if parsed_steps ~= nil and parsed_steps > 0 then
      rule_max_steps_override = parsed_steps
    end
    rule_type = string.lower(tostring(rule_decoded["type"] or ""))
    if rule_type == "fixed" then
      rule_fixed_amount = positive_number(rule_decoded["amount"])
    elseif rule_type == "ladder" and type(rule_decoded["steps"]) == "table" then
      rule_steps = rule_decoded["steps"]
    end
  end
end

local fallback_increment_amount = positive_number(increment_amount_raw)
local fallback_max_bid_steps = tonumber(max_bid_steps_raw) or 1
if fallback_max_bid_steps <= 0 then
  fallback_max_bid_steps = 1
end

local function increment_rule_for_price(current_price_for_rule)
  local amount = fallback_increment_amount or positive_number(min_increment) or 1
  local max_steps = rule_max_steps_override or fallback_max_bid_steps
  if max_steps <= 0 then
    max_steps = 1
  end
  if fast_fixed then
    if rule_fixed_amount ~= nil then
      amount = rule_fixed_amount
    end
    return amount, max_steps
  end
  if not rule_decoded_ok then
    return amount, max_steps
  end
  if rule_type == "fixed" then
    if rule_fixed_amount ~= nil then
      amount = rule_fixed_amount
    end
    return amount, max_steps
  end
  if rule_type == "ladder" and rule_steps ~= nil then
    for _, step in ipairs(rule_steps) do
      if type(step) == "table" then
        local step_min = tonumber(step["min"]) or 0
        local step_max = tonumber(step["max"])
        local step_amount = positive_number(step["amount"])
        if step_amount ~= nil and current_price_for_rule >= step_min and (step_max == nil or current_price_for_rule < step_max) then
          return step_amount, max_steps
        end
      end
    end
  end
  return amount, max_steps
end

local function reject(reason)
  local result = build_result_array(false, reason, current_price, leader_id, end_ts, extend_count, false, version, 0, "", false, status, false, live_session_id)
  if request_id ~= "" then
    redis.call("SET", idem_key, cjson.encode(result), "PX", idem_ttl_ms)
  end
  return result
end

if status ~= "RUNNING" and status ~= "EXTENDED" then
  return reject("INVALID_STATE")
end

local expected_current_price = tonumber(expected_current_price_raw)
if expected_current_price_raw == nil or expected_current_price_raw == "" or expected_current_price == nil or expected_current_price < 0 then
  return reject("MISSING_EXPECTED_STATE")
end
if expected_current_price > current_price then
  return reject("STALE_AUCTION_STATE")
end

if freq_limit_count > 0 and freq_window_ms > 0 then
  local count = redis.call("INCR", freq_key)
  if count == 1 then
    redis.call("PEXPIRE", freq_key, freq_window_ms)
  end
  if count > freq_limit_count then
    return reject("FREQ_LIMIT")
  end
end

local increment_amount, max_bid_steps = increment_rule_for_price(current_price)
local expected_increment_amount, expected_max_bid_steps = increment_rule_for_price(expected_current_price)
if price <= start_price then
  return reject("BELOW_START_PRICE")
end
if cap_price > 0 and price > cap_price then
  return reject("ABOVE_CAP_PRICE")
end
local expected_max_allowed = expected_current_price + expected_increment_amount * expected_max_bid_steps
if cap_price > 0 and expected_max_allowed > cap_price then
  expected_max_allowed = cap_price
end
if expected_current_price < current_price and price > expected_max_allowed then
  return reject("ABOVE_EXPECTED_MAX_BID_STEPS")
end
local is_cap_bid = cap_price > 0 and price == cap_price
if (not is_cap_bid) and ((price - current_price) % increment_amount) ~= 0 then
  return reject("PRICE_STEP_MISMATCH")
end
if is_cap_bid then
  if price <= current_price then
    return reject("BELOW_MIN_INCREMENT")
  end
elseif price < current_price + increment_amount then
  return reject("BELOW_MIN_INCREMENT")
end
local max_allowed = current_price + increment_amount * max_bid_steps
if cap_price > 0 and max_allowed > cap_price then
  max_allowed = cap_price
end
if price > max_allowed then
  return reject("ABOVE_MAX_BID_STEPS")
end

local extended = false
local auto_closed = is_cap_bid
if auto_closed then
  status = "CLOSED_WON"
  end_ts = now_ms
elseif anti_snipe_ms > 0 and extend_ms > 0 and extend_count < max_extend_count then
  if end_ts - now_ms <= anti_snipe_ms then
    if anti_extend_mode == "RESET" then
      end_ts = now_ms + extend_ms
    else
      end_ts = end_ts + extend_ms
    end
    extend_count = extend_count + 1
    status = "EXTENDED"
    extended = true
  end
end

version = version + 1
bid_count = bid_count + 1
local next_price = price
local next_leader = bidder_id
local seq, stream_id = append_accepted_event(next_price, next_leader, end_ts, extend_count, extended, status, auto_closed)
if auto_closed then
  redis.call("HSET", state_key,
    "status", status,
    "current_price", next_price,
    "leader_bidder_id", next_leader,
    "last_bid_ts_ms", now_ms,
    "end_ts_ms", end_ts,
    "extend_count", extend_count,
    "version", version,
    "bid_count", bid_count,
    "closed_at_ms", now_ms
  )
else
  redis.call("HSET", state_key,
    "status", status,
    "current_price", next_price,
    "leader_bidder_id", next_leader,
    "last_bid_ts_ms", now_ms,
    "end_ts_ms", end_ts,
    "extend_count", extend_count,
    "version", version,
    "bid_count", bid_count
  )
end

local result = build_result_array(true, "", next_price, next_leader, end_ts, extend_count, extended, version, seq, stream_id, false, status, auto_closed, live_session_id)
if request_id ~= "" then
  redis.call("SET", idem_key, cjson.encode(result), "PX", idem_ttl_ms)
end
return result
`

const hammerLua = `
local state_key = KEYS[1]
local close_lock_key = KEYS[2]

local request_id = ARGV[1]
local auction_id = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local idem_ttl_ms = tonumber(ARGV[4])
local reserve_price = tonumber(ARGV[5]) or 0
local force = ARGV[6] == "1" or ARGV[6] == "true"

local function number_or_zero(value)
  if value == false or value == nil or value == "" then
    return 0
  end
  return tonumber(value) or 0
end

local function string_or_empty(value)
  if value == false or value == nil then
    return ""
  end
  return value
end

local existing = redis.call("GET", close_lock_key)
if existing then
  return existing
end

local status = redis.call("HGET", state_key, "status")
local version = number_or_zero(redis.call("HGET", state_key, "version"))
local winner_id = string_or_empty(redis.call("HGET", state_key, "leader_bidder_id"))
local price = number_or_zero(redis.call("HGET", state_key, "current_price"))
local end_ts = number_or_zero(redis.call("HGET", state_key, "end_ts_ms"))

if status == false or status == nil then
  local missing = cjson.encode({
    requestId = request_id,
    auctionId = auction_id,
    status = "NOT_FOUND",
    winnerId = "",
    price = 0,
    closedAtMs = now_ms,
    version = version
  })
  return missing
end

if status == "CLOSED_WON" or status == "CLOSED_FAILED" or status == "SETTLED" then
  local closed = cjson.encode({
    requestId = request_id,
    auctionId = auction_id,
    status = status,
    winnerId = winner_id,
    price = price,
    closedAtMs = now_ms,
    version = version
  })
  redis.call("SET", close_lock_key, closed, "PX", idem_ttl_ms)
  return closed
end

if not force and now_ms < end_ts then
  local not_ended = cjson.encode({
    requestId = request_id,
    auctionId = auction_id,
    status = "NOT_ENDED",
    winnerId = "",
    price = 0,
    closedAtMs = now_ms,
    version = version
  })
  return not_ended
end

if winner_id ~= "" and price > 0 and price >= reserve_price then
  status = "CLOSED_WON"
else
  winner_id = ""
  price = 0
  status = "CLOSED_FAILED"
end

version = version + 1
redis.call("HSET", state_key,
  "status", status,
  "leader_bidder_id", winner_id,
  "current_price", price,
  "closed_at_ms", now_ms,
  "version", version
)

local result = cjson.encode({
  requestId = request_id,
  auctionId = auction_id,
  status = status,
  winnerId = winner_id,
  price = price,
  closedAtMs = now_ms,
  version = version
})
redis.call("SET", close_lock_key, result, "PX", idem_ttl_ms)
return result
`

// rateLimitLua 实现一个原子的令牌桶（token bucket）限流。
//
// KEYS[1] : bucket hash key holding {tokens, ts_ms}
// ARGV[1] : capacity (max tokens, integer)
// ARGV[2] : refill_rate_per_ms (float, tokens added per millisecond)
// ARGV[3] : now_ms
// ARGV[4] : ttl_ms (key TTL；> capacity / refill_rate_per_ms)
// ARGV[5] : cost (>=1)
//
// 返回 { allowed (1|0), remaining_tokens (int), retry_after_ms (int) }。
//
// 注意：本脚本必须与 scripts/lua/rate_limit.lua 字节同步（去尾首空白）。
const rateLimitLua = `
-- Token-bucket rate limit (atomic).
--
-- KEYS[1] : bucket hash key holding {tokens, ts_ms}
-- ARGV[1] : capacity (max tokens)
-- ARGV[2] : refill_rate_per_ms (float; tokens added per ms)
-- ARGV[3] : now_ms
-- ARGV[4] : ttl_ms (key TTL — should be longer than (capacity / refill_rate_per_ms))
-- ARGV[5] : cost (tokens consumed per call; >=1)
--
-- Returns: { allowed (1|0), remaining_tokens (int), retry_after_ms (int) }

local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate_per_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local ttl_ms = tonumber(ARGV[4])
local cost = tonumber(ARGV[5])

if capacity == nil or capacity <= 0 then capacity = 1 end
if refill_rate_per_ms == nil or refill_rate_per_ms <= 0 then refill_rate_per_ms = capacity / 1000.0 end
if now_ms == nil or now_ms <= 0 then now_ms = 0 end
if ttl_ms == nil or ttl_ms <= 0 then ttl_ms = 60000 end
if cost == nil or cost <= 0 then cost = 1 end

local bucket = redis.call("HMGET", key, "tokens", "ts_ms")
local tokens = tonumber(bucket[1])
local last_ts = tonumber(bucket[2])

if tokens == nil then tokens = capacity end
if last_ts == nil or last_ts <= 0 then last_ts = now_ms end

local elapsed = now_ms - last_ts
if elapsed < 0 then elapsed = 0 end
local refill = elapsed * refill_rate_per_ms
tokens = tokens + refill
if tokens > capacity then tokens = capacity end

local allowed = 0
local retry_after_ms = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  local missing = cost - tokens
  if refill_rate_per_ms > 0 then
    retry_after_ms = math.ceil(missing / refill_rate_per_ms)
  else
    retry_after_ms = ttl_ms
  end
end

redis.call("HSET", key, "tokens", tostring(tokens), "ts_ms", tostring(now_ms))
redis.call("PEXPIRE", key, ttl_ms)

local remaining = math.floor(tokens)
if remaining < 0 then remaining = 0 end
return { allowed, remaining, retry_after_ms }
`

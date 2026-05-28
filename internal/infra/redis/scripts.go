package redis

import "strings"

const (
	ScriptBidPlace = "bid.place"
	ScriptHammer   = "auction.hammer"
)

func DefaultScripts() map[string]string {
	return map[string]string{
		ScriptBidPlace: strings.TrimSpace(bidLua),
		ScriptHammer:   strings.TrimSpace(hammerLua),
	}
}

const bidLua = `
local state_key = KEYS[1]
local ranking_key = KEYS[2]
local idem_key = KEYS[3]
local enrolled_key = KEYS[4]
local deposit_key = KEYS[5]
local freq_key = KEYS[6]
local user_bids_key = KEYS[7]
local stream_key = KEYS[8]
local seq_key = KEYS[9]
local active_streams_key = KEYS[10]

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
local expected_version_raw = ARGV[16]
local trace_parent = ARGV[17]
local trace_state = ARGV[18]

if request_id == nil then request_id = "" end
if bidder_id == nil then bidder_id = "" end
if source == nil or source == "" then source = "live_ws" end
if anti_extend_mode == nil or anti_extend_mode == "" then anti_extend_mode = "ADD" end
anti_extend_mode = string.upper(tostring(anti_extend_mode))
if trace_parent == nil then trace_parent = "" end
if trace_state == nil then trace_state = "" end

if auction_id == nil or auction_id <= 0 or bidder_id == "" or price == nil or price <= 0 or now_ms == nil or now_ms <= 0 then
  return redis.error_reply("invalid bid arguments")
end
if min_increment == nil then min_increment = 1 end
if anti_snipe_ms == nil then anti_snipe_ms = 0 end
if extend_ms == nil then extend_ms = 0 end
if max_extend_count == nil then max_extend_count = 0 end
if freq_limit_count == nil then freq_limit_count = 0 end
if freq_window_ms == nil then freq_window_ms = 0 end
if idem_ttl_ms == nil or idem_ttl_ms <= 0 then idem_ttl_ms = 86400000 end

local function assert_type(key, expected)
  local key_type = redis.call("TYPE", key).ok
  if key_type ~= "none" and key_type ~= expected then
    error("WRONGTYPE " .. key .. " expected " .. expected .. " got " .. key_type)
  end
end

assert_type(state_key, "hash")
assert_type(ranking_key, "zset")
assert_type(idem_key, "string")
assert_type(enrolled_key, "set")
assert_type(deposit_key, "set")
assert_type(freq_key, "string")
assert_type(user_bids_key, "hash")
assert_type(stream_key, "stream")
assert_type(seq_key, "string")
assert_type(active_streams_key, "set")

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

local function build_result(accepted, reason, current_price, leader_id, end_ts, extend_count, extended, version, seq, stream_id, duplicate, auction_status, auto_closed)
  local result = {
    requestId = request_id,
    auctionId = auction_id,
    bidderId = bidder_id,
    price = price,
    accepted = accepted,
    reason = reason,
    currentPrice = current_price,
    leaderBidderId = leader_id,
    endTsMs = end_ts,
    extended = extended,
    extendCount = extend_count,
    version = version,
    seq = seq,
    streamId = stream_id,
    createdAtMs = now_ms,
    bidTsMs = now_ms,
    source = source,
    duplicate = duplicate,
    event = accepted and "bid.accepted" or "bid.rejected",
    riskResult = accepted and "ALLOW" or "REJECT",
    auctionStatus = auction_status,
    autoClosed = auto_closed
  }
  return cjson.encode(result)
end

local function append_event(accepted, reason, current_price, leader_id, end_ts, extend_count, extended, version, auction_status, auto_closed)
  local seq = redis.call("INCR", seq_key)
  local stream_id = tostring(seq) .. "-0"
  local event_type = accepted and "bid.accepted" or "bid.rejected"
  redis.call("XADD", stream_key, stream_id,
    "request_id", request_id,
    "auction_id", tostring(auction_id),
    "bidder_id", bidder_id,
    "bid_price", tostring(price),
    "bid_ts_ms", tostring(now_ms),
    "source", source,
    "risk_result", accepted and "ALLOW" or "REJECT",
    "reject_reason", reason,
    "accepted", accepted and "1" or "0",
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
    "event_type", event_type,
    "traceparent", trace_parent,
    "tracestate", trace_state
  )
  redis.call("SADD", active_streams_key, tostring(auction_id))
  return seq, stream_id
end

local existing = redis.call("GET", idem_key)
if existing then
  return existing
end

local status = redis.call("HGET", state_key, "status")
local current_price = number_or_zero(redis.call("HGET", state_key, "current_price"))
local start_price_raw = redis.call("HGET", state_key, "start_price")
local start_price = number_or_zero(start_price_raw)
local cap_price = number_or_zero(redis.call("HGET", state_key, "cap_price"))
local leader_id = string_or_empty(redis.call("HGET", state_key, "leader_bidder_id"))
local end_ts = number_or_zero(redis.call("HGET", state_key, "end_ts_ms"))
local extend_count = number_or_zero(redis.call("HGET", state_key, "extend_count"))
local version = number_or_zero(redis.call("HGET", state_key, "version"))
local increment_rule = string_or_empty(redis.call("HGET", state_key, "increment_rule"))

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

local function increment_rule_for_price(rule_raw, fallback_amount, current_price_for_rule)
  local amount = positive_number(redis.call("HGET", state_key, "increment_amount")) or positive_number(fallback_amount) or 1
  local max_steps = tonumber(redis.call("HGET", state_key, "max_bid_steps")) or 1
  if max_steps <= 0 then
    max_steps = 1
  end
  if rule_raw == "" then
    return amount, max_steps
  end
  local ok, rule = pcall(cjson.decode, rule_raw)
  if not ok or type(rule) ~= "table" then
    return amount, max_steps
  end
  local parsed_steps = tonumber(rule["maxBidSteps"])
  if parsed_steps ~= nil and parsed_steps > 0 then
    max_steps = parsed_steps
  end
  local rule_type = string.lower(tostring(rule["type"] or ""))
  if rule_type == "fixed" then
    local parsed_amount = positive_number(rule["amount"])
    if parsed_amount ~= nil then
      amount = parsed_amount
    end
    return amount, max_steps
  end
  if rule_type == "ladder" and type(rule["steps"]) == "table" then
    for _, step in ipairs(rule["steps"]) do
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
  local seq, stream_id = append_event(false, reason, current_price, leader_id, end_ts, extend_count, false, version, status, false)
  local result = build_result(false, reason, current_price, leader_id, end_ts, extend_count, false, version, seq, stream_id, false, status, false)
  if request_id ~= "" then
    redis.call("SET", idem_key, result, "PX", idem_ttl_ms)
  end
  return result
end

if status ~= "RUNNING" and status ~= "EXTENDED" then
  return reject("INVALID_STATE")
end

if redis.call("SISMEMBER", enrolled_key, bidder_id) ~= 1 then
  return reject("NOT_ENROLLED")
end

if redis.call("SISMEMBER", deposit_key, bidder_id) ~= 1 then
  return reject("DEPOSIT_NOT_READY")
end

if expected_current_price_raw ~= nil and expected_current_price_raw ~= "" then
  local expected_current_price = tonumber(expected_current_price_raw)
  if expected_current_price == nil or expected_current_price ~= current_price then
    return reject("STALE_AUCTION_STATE")
  end
end

if expected_version_raw ~= nil and expected_version_raw ~= "" then
  local expected_version = tonumber(expected_version_raw)
  if expected_version == nil or expected_version ~= version then
    return reject("STALE_AUCTION_STATE")
  end
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

local increment_amount, max_bid_steps = increment_rule_for_price(increment_rule, min_increment, current_price)
if price <= start_price then
  return reject("BELOW_START_PRICE")
end
if cap_price > 0 and price > cap_price then
  return reject("ABOVE_CAP_PRICE")
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
local next_price = price
local next_leader = bidder_id
local seq, stream_id = append_event(true, "", next_price, next_leader, end_ts, extend_count, extended, version, status, auto_closed)
redis.call("HSET", state_key,
  "status", status,
  "current_price", next_price,
  "leader_bidder_id", next_leader,
  "last_bid_ts_ms", now_ms,
  "end_ts_ms", end_ts,
  "extend_count", extend_count,
  "version", version,
  "closed_at_ms", auto_closed and now_ms or string_or_empty(redis.call("HGET", state_key, "closed_at_ms"))
)
local old_member = redis.call("HGET", user_bids_key, bidder_id)
if old_member and old_member ~= false then
  redis.call("ZREM", ranking_key, old_member)
end
local ranking_member = string.format("%019d:%013d:%s", price, 9999999999999 - now_ms, bidder_id)
redis.call("ZADD", ranking_key, 0, ranking_member)
redis.call("HSET", user_bids_key, bidder_id, ranking_member)

local result = build_result(true, "", next_price, next_leader, end_ts, extend_count, extended, version, seq, stream_id, false, status, auto_closed)
if request_id ~= "" then
  redis.call("SET", idem_key, result, "PX", idem_ttl_ms)
end
return result
`

const hammerLua = `
local state_key = KEYS[1]
local ranking_key = KEYS[2]
local close_lock_key = KEYS[3]

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

local top = redis.call("ZREVRANGE", ranking_key, 0, 0, "WITHSCORES")
if #top >= 2 then
  local parsed_price, parsed_winner = string.match(top[1], "^(%d+):%d+:(.+)$")
  winner_id = parsed_winner or top[1]
  price = tonumber(parsed_price) or tonumber(top[2])
  if price >= reserve_price then
    status = "CLOSED_WON"
  else
    winner_id = ""
    price = 0
    status = "CLOSED_FAILED"
  end
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

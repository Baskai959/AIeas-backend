local state_key = KEYS[1]
local ranking_key = KEYS[2]
local idem_key = KEYS[3]
local enrolled_key = KEYS[4]
local deposit_key = KEYS[5]
local blacklist_key = KEYS[6]
local freq_key = KEYS[7]
local user_bids_key = KEYS[8]
local stream_key = KEYS[9]
local seq_key = KEYS[10]
local active_streams_key = KEYS[11]

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

if request_id == nil then request_id = "" end
if bidder_id == nil then bidder_id = "" end
if source == nil or source == "" then source = "live_ws" end

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
assert_type(blacklist_key, "set")
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

local function build_result(accepted, reason, current_price, leader_id, end_ts, extend_count, extended, version, seq, stream_id, duplicate)
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
    riskResult = accepted and "ALLOW" or "REJECT"
  }
  return cjson.encode(result)
end

local function append_event(accepted, reason, current_price, leader_id, end_ts, extend_count, extended, version)
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
    "seq", tostring(seq),
    "stream_id", stream_id,
    "created_at_ms", tostring(now_ms),
    "event_type", event_type
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

local function min_increment_for_price(rule_raw, current, fallback)
  if fallback == nil or fallback <= 0 then
    fallback = 1
  end
  if rule_raw == "" then
    return fallback
  end
  local ok, rule = pcall(cjson.decode, rule_raw)
  if not ok or type(rule) ~= "table" then
    return fallback
  end
  local rule_type = rule["type"]
  if rule_type ~= nil and rule_type ~= "" then
    rule_type = string.lower(tostring(rule_type))
  end
  if rule_type == "fixed" then
    return positive_number(rule["amount"]) or fallback
  end
  if rule_type == "ladder" and type(rule["steps"]) == "table" then
    for _, step in ipairs(rule["steps"]) do
      if type(step) == "table" then
        local amount = positive_number(step["amount"])
        local min = tonumber(step["min"]) or 0
        local max = tonumber(step["max"])
        if amount ~= nil and current >= min and (max == nil or current < max) then
          return amount
        end
      end
    end
  end
  return fallback
end

local function reject(reason)
  local seq, stream_id = append_event(false, reason, current_price, leader_id, end_ts, extend_count, false, version)
  local result = build_result(false, reason, current_price, leader_id, end_ts, extend_count, false, version, seq, stream_id, false)
  if request_id ~= "" then
    redis.call("SET", idem_key, result, "PX", idem_ttl_ms)
  end
  return result
end

if status ~= "RUNNING" and status ~= "EXTENDED" then
  return reject("INVALID_STATE")
end

if redis.call("SISMEMBER", blacklist_key, bidder_id) == 1 then
  return reject("BLACKLIST")
end

if redis.call("SISMEMBER", enrolled_key, bidder_id) ~= 1 then
  return reject("NOT_ENROLLED")
end

if redis.call("SISMEMBER", deposit_key, bidder_id) ~= 1 then
  return reject("DEPOSIT_NOT_READY")
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

min_increment = min_increment_for_price(increment_rule, current_price, min_increment)
local tie_bid = leader_id ~= "" and bidder_id ~= leader_id and price == current_price
if not tie_bid and price < current_price + min_increment then
  return reject("BELOW_MIN_INCREMENT")
end

local extended = false
if not tie_bid and anti_snipe_ms > 0 and extend_ms > 0 and extend_count < max_extend_count then
  if end_ts - now_ms <= anti_snipe_ms then
    end_ts = end_ts + extend_ms
    extend_count = extend_count + 1
    status = "EXTENDED"
    extended = true
  end
end

version = version + 1
local next_price = tie_bid and current_price or price
local next_leader = tie_bid and leader_id or bidder_id
local seq, stream_id = append_event(true, "", next_price, next_leader, end_ts, extend_count, extended, version)
redis.call("HSET", state_key,
  "status", status,
  "current_price", next_price,
  "leader_bidder_id", next_leader,
  "last_bid_ts_ms", now_ms,
  "end_ts_ms", end_ts,
  "extend_count", extend_count,
  "version", version
)
local old_member = redis.call("HGET", user_bids_key, bidder_id)
if old_member and old_member ~= false then
  redis.call("ZREM", ranking_key, old_member)
end
local ranking_member = string.format("%019d:%013d:%s", price, 9999999999999 - now_ms, bidder_id)
redis.call("ZADD", ranking_key, 0, ranking_member)
redis.call("HSET", user_bids_key, bidder_id, ranking_member)

local result = build_result(true, "", next_price, next_leader, end_ts, extend_count, extended, version, seq, stream_id, false)
if request_id ~= "" then
  redis.call("SET", idem_key, result, "PX", idem_ttl_ms)
end
return result

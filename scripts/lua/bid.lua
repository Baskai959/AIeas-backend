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
local trace_parent = ARGV[16]
local trace_state = ARGV[17]
local live_session_id_arg = ARGV[18]
local bidder_nickname = ARGV[19]

if request_id == nil then request_id = "" end
if bidder_id == nil then bidder_id = "" end
if source == nil or source == "" then source = "live_ws" end
if anti_extend_mode == nil or anti_extend_mode == "" then anti_extend_mode = "ADD" end
anti_extend_mode = string.upper(tostring(anti_extend_mode))
if trace_parent == nil then trace_parent = "" end
if trace_state == nil then trace_state = "" end
if bidder_nickname == nil then bidder_nickname = "" end

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

local live_session_id = number_or_zero(live_session_id_arg)
if live_session_id == 0 then
  live_session_id = number_or_zero(redis.call("HGET", state_key, "live_session_id"))
end

local function build_result(accepted, reason, current_price, leader_id, end_ts, extend_count, extended, version, seq, stream_id, duplicate, auction_status, auto_closed)
  local result = {
    requestId = request_id,
    auctionId = auction_id,
    liveSessionId = live_session_id,
    bidderId = bidder_id,
    bidderNickname = bidder_nickname,
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

local function append_accepted_event(current_price, leader_id, end_ts, extend_count, extended, version, auction_status, auto_closed)
  local seq = redis.call("INCR", seq_key)
  local stream_id = tostring(seq) .. "-0"
  redis.call("XADD", stream_key, "MAXLEN", "~", "10000", stream_id,
    "request_id", request_id,
    "auction_id", tostring(auction_id),
    "live_session_id", tostring(live_session_id),
    "bidder_id", bidder_id,
    "bidder_nickname", bidder_nickname,
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
  local payload = build_result(true, "", current_price, leader_id, end_ts, extend_count, extended, version, seq, stream_id, false, auction_status, auto_closed)
  local channel = "auction:" .. tostring(auction_id) .. ":events"
  redis.call("PUBLISH", channel, payload)
  redis.call("SADD", active_streams_key, tostring(auction_id))
  return seq, stream_id
end

local existing = redis.call("GET", idem_key)
if existing then
  local ok, decoded = pcall(cjson.decode, existing)
  if ok and type(decoded) == "table" then
    decoded["duplicate"] = true
    return cjson.encode(decoded)
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
  "bid_count"
)
local status = status_fields[1]
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
if increment_rule ~= "" then
  local ok, parsed = pcall(cjson.decode, increment_rule)
  if ok and type(parsed) == "table" then
    rule_decoded_ok = true
    rule_decoded = parsed
  end
end

local rule_type = ""
local rule_max_steps_override = nil
local rule_fixed_amount = nil
local rule_steps = nil
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
  local result = build_result(false, reason, current_price, leader_id, end_ts, extend_count, false, version, 0, "", false, status, false)
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
local seq, stream_id = append_accepted_event(next_price, next_leader, end_ts, extend_count, extended, version, status, auto_closed)
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

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

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

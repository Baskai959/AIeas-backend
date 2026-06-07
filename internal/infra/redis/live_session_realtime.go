package redis

import (
	"context"
	"strconv"

	"aieas_backend/internal/domain"

	redisgo "github.com/redis/go-redis/v9"
)

// LiveSessionRealtimeStore 是 live_session 实时计数端口的 Redis 实现。
//
// 计数 HASH key 形如 live_session:%d:counters，字段：
//
//	lots_total / lots_sold / lots_unsold / bid_count / gmv_cent / viewer_total
//
// viewer_peak 单独放 STRING key live_session:%d:viewer_peak，使用 Lua CAS 取 max。
//
// v2 起按 sessionID 走分片：所有 live_session:<id>:* 落到 ForSession(id) 的 shard，
// 同一场次的计数 / 峰值在同一 shard 上原子可见。
type LiveSessionRealtimeStore struct {
	sharded *ShardedRTClient
	keys    KeyBuilder
}

func NewLiveSessionRealtimeStore(sharded *ShardedRTClient, keys KeyBuilder) *LiveSessionRealtimeStore {
	return &LiveSessionRealtimeStore{sharded: sharded, keys: keys}
}

const liveSessionViewerPeakLua = `
local cur = tonumber(redis.call("GET", KEYS[1])) or 0
local v = tonumber(ARGV[1]) or 0
if v > cur then
  redis.call("SET", KEYS[1], v)
  return v
end
return cur
`

func (s *LiveSessionRealtimeStore) shardForSession(sessionID uint64) *RedisRTClient {
	if s == nil || s.sharded == nil {
		return nil
	}
	return s.sharded.ForSession(sessionID)
}

// IncrCounters 对场次计数 HASH 多字段做 HINCRBY；零字段跳过。
func (s *LiveSessionRealtimeStore) IncrCounters(ctx context.Context, sessionID uint64, c domain.LiveSessionCounters) error {
	if s == nil || s.sharded == nil || sessionID == 0 {
		return nil
	}
	client := s.shardForSession(sessionID)
	if client == nil {
		return nil
	}
	key := s.keys.LiveSessionCounters(sessionID)
	pipe := client.Pipeline()
	wrote := false
	if c.LotsTotalDelta != 0 {
		pipe.HIncrBy(ctx, key, "lots_total", int64(c.LotsTotalDelta))
		wrote = true
	}
	if c.LotsSoldDelta != 0 {
		pipe.HIncrBy(ctx, key, "lots_sold", int64(c.LotsSoldDelta))
		wrote = true
	}
	if c.LotsUnsoldDelta != 0 {
		pipe.HIncrBy(ctx, key, "lots_unsold", int64(c.LotsUnsoldDelta))
		wrote = true
	}
	if c.BidCountDelta != 0 {
		pipe.HIncrBy(ctx, key, "bid_count", int64(c.BidCountDelta))
		wrote = true
	}
	if c.GMVCentDelta != 0 {
		pipe.HIncrBy(ctx, key, "gmv_cent", c.GMVCentDelta)
		wrote = true
	}
	if c.ViewerTotalAdd != 0 {
		pipe.HIncrBy(ctx, key, "viewer_total", int64(c.ViewerTotalAdd))
		wrote = true
	}
	if !wrote {
		return nil
	}
	_, err := pipe.Exec(ctx)
	return err
}

// BumpViewerPeak 通过 Lua CAS 把 viewer_peak 推高到 max(cur, value)，并返回最新值。
func (s *LiveSessionRealtimeStore) BumpViewerPeak(ctx context.Context, sessionID uint64, value int) (int, error) {
	if s == nil || s.sharded == nil || sessionID == 0 {
		return 0, nil
	}
	client := s.shardForSession(sessionID)
	if client == nil {
		return 0, nil
	}
	key := s.keys.LiveSessionViewerPeak(sessionID)
	res, err := client.Eval(ctx, liveSessionViewerPeakLua, []string{key}, value).Result()
	if err != nil {
		return 0, err
	}
	switch v := res.(type) {
	case int64:
		return int(v), nil
	case string:
		parsed, perr := strconv.ParseInt(v, 10, 64)
		if perr != nil {
			return 0, perr
		}
		return int(parsed), nil
	default:
		return 0, nil
	}
}

// LoadCounters 读取计数 HASH 与 viewer_peak 的当前值。
func (s *LiveSessionRealtimeStore) LoadCounters(ctx context.Context, sessionID uint64) (domain.LiveSessionCounters, int, error) {
	if s == nil || s.sharded == nil || sessionID == 0 {
		return domain.LiveSessionCounters{}, 0, nil
	}
	client := s.shardForSession(sessionID)
	if client == nil {
		return domain.LiveSessionCounters{}, 0, nil
	}
	values, err := client.HGetAll(ctx, s.keys.LiveSessionCounters(sessionID)).Result()
	if err != nil {
		return domain.LiveSessionCounters{}, 0, err
	}
	counters := domain.LiveSessionCounters{
		LotsTotalDelta:  int(parseInt(values["lots_total"], 0)),
		LotsSoldDelta:   int(parseInt(values["lots_sold"], 0)),
		LotsUnsoldDelta: int(parseInt(values["lots_unsold"], 0)),
		BidCountDelta:   int(parseInt(values["bid_count"], 0)),
		GMVCentDelta:    parseInt(values["gmv_cent"], 0),
		ViewerTotalAdd:  int(parseInt(values["viewer_total"], 0)),
	}
	peakRaw, err := client.Get(ctx, s.keys.LiveSessionViewerPeak(sessionID)).Result()
	if err != nil && err != redisgo.Nil {
		return counters, 0, err
	}
	peak := int(parseInt(peakRaw, 0))
	return counters, peak, nil
}

func (s *LiveSessionRealtimeStore) SetActiveAuction(ctx context.Context, sessionID uint64, auctionID uint64) error {
	if s == nil || s.sharded == nil || sessionID == 0 {
		return nil
	}
	client := s.shardForSession(sessionID)
	if client == nil {
		return nil
	}
	key := s.keys.LiveSessionActiveAuction(sessionID)
	if auctionID == 0 {
		return client.Del(ctx, key).Err()
	}
	return client.Set(ctx, key, strconv.FormatUint(auctionID, 10), 0).Err()
}

func (s *LiveSessionRealtimeStore) ClearActiveAuction(ctx context.Context, sessionID uint64) error {
	return s.SetActiveAuction(ctx, sessionID, 0)
}

func (s *LiveSessionRealtimeStore) ActiveAuction(ctx context.Context, sessionID uint64) (uint64, bool, error) {
	if s == nil || s.sharded == nil || sessionID == 0 {
		return 0, false, nil
	}
	client := s.shardForSession(sessionID)
	if client == nil {
		return 0, false, nil
	}
	value, err := client.Get(ctx, s.keys.LiveSessionActiveAuction(sessionID)).Result()
	if err != nil {
		if err == redisgo.Nil {
			return 0, false, nil
		}
		return 0, false, err
	}
	auctionID, err := strconv.ParseUint(value, 10, 64)
	if err != nil || auctionID == 0 {
		return 0, false, err
	}
	return auctionID, true, nil
}

// Reset 在场次关闭后清理 redis key。
func (s *LiveSessionRealtimeStore) Reset(ctx context.Context, sessionID uint64) error {
	if s == nil || s.sharded == nil || sessionID == 0 {
		return nil
	}
	client := s.shardForSession(sessionID)
	if client == nil {
		return nil
	}
	return client.Del(ctx,
		s.keys.LiveSessionCounters(sessionID),
		s.keys.LiveSessionViewerPeak(sessionID),
		s.keys.LiveSessionActiveAuction(sessionID),
	).Err()
}

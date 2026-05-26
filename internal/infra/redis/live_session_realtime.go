package redis

import (
	"context"
	"strconv"

	"aieas_backend/internal/domain"

	redisgo "github.com/redis/go-redis/v9"
)

// LiveSessionRealtimeStore 是 repository.LiveSessionRealtimeStore 的 Redis 实现。
//
// 计数 HASH key 形如 live_session:%d:counters，字段：
//
//	lots_total / lots_sold / lots_unsold / bid_count / gmv_cent / viewer_total
//
// viewer_peak 单独放 STRING key live_session:%d:viewer_peak，使用 Lua CAS 取 max。
type LiveSessionRealtimeStore struct {
	client *redisgo.Client
	keys   KeyBuilder
}

func NewLiveSessionRealtimeStore(client *redisgo.Client, keys KeyBuilder) *LiveSessionRealtimeStore {
	return &LiveSessionRealtimeStore{client: client, keys: keys}
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

// IncrCounters 对场次计数 HASH 多字段做 HINCRBY；零字段跳过。
func (s *LiveSessionRealtimeStore) IncrCounters(ctx context.Context, sessionID uint64, c domain.LiveSessionCounters) error {
	if s == nil || s.client == nil || sessionID == 0 {
		return nil
	}
	key := s.keys.LiveSessionCounters(sessionID)
	pipe := s.client.Pipeline()
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
	if s == nil || s.client == nil || sessionID == 0 {
		return 0, nil
	}
	key := s.keys.LiveSessionViewerPeak(sessionID)
	res, err := s.client.Eval(ctx, liveSessionViewerPeakLua, []string{key}, value).Result()
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
	if s == nil || s.client == nil || sessionID == 0 {
		return domain.LiveSessionCounters{}, 0, nil
	}
	values, err := s.client.HGetAll(ctx, s.keys.LiveSessionCounters(sessionID)).Result()
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
	peakRaw, err := s.client.Get(ctx, s.keys.LiveSessionViewerPeak(sessionID)).Result()
	if err != nil && err != redisgo.Nil {
		return counters, 0, err
	}
	peak := int(parseInt(peakRaw, 0))
	return counters, peak, nil
}

// Reset 在场次关闭后清理 redis key。
func (s *LiveSessionRealtimeStore) Reset(ctx context.Context, sessionID uint64) error {
	if s == nil || s.client == nil || sessionID == 0 {
		return nil
	}
	return s.client.Del(ctx,
		s.keys.LiveSessionCounters(sessionID),
		s.keys.LiveSessionViewerPeak(sessionID),
	).Err()
}

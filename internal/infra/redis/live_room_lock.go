package redis

import (
	"context"
	"errors"
	"strconv"
	"time"

	redisgo "github.com/redis/go-redis/v9"
)

// LiveRoomLock 提供基于 Redis SETNX 的"直播间内同一时刻仅一个拍品在拍"分布式锁。
// 锁键为 KeyBuilder.LiveRoomActiveLock(roomID)，值为持锁拍品 ID 的字符串。
type LiveRoomLock struct {
	client *redisgo.Client
	keys   KeyBuilder
}

func NewLiveRoomLock(client *redisgo.Client, keys KeyBuilder) *LiveRoomLock {
	return &LiveRoomLock{client: client, keys: keys}
}

// Acquire 尝试为 roomID 抢占 auctionID 锁。同一 auctionID 重入视为成功并刷新 TTL。
func (l *LiveRoomLock) Acquire(ctx context.Context, roomID uint64, auctionID uint64, ttl time.Duration) (bool, uint64, error) {
	if l.client == nil {
		return false, 0, errors.New("redis client is nil")
	}
	key := l.keys.LiveRoomActiveLock(roomID)
	value := strconv.FormatUint(auctionID, 10)
	if ttl <= 0 {
		ttl = 0
	}
	ok, err := l.client.SetNX(ctx, key, value, ttl).Result()
	if err != nil {
		return false, 0, err
	}
	if ok {
		return true, auctionID, nil
	}
	current, err := l.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redisgo.Nil) {
			// 锁已经过期被回收，再尝试一次。
			ok2, err := l.client.SetNX(ctx, key, value, ttl).Result()
			if err != nil {
				return false, 0, err
			}
			if ok2 {
				return true, auctionID, nil
			}
			current, err = l.client.Get(ctx, key).Result()
			if err != nil && !errors.Is(err, redisgo.Nil) {
				return false, 0, err
			}
		} else {
			return false, 0, err
		}
	}
	holder, _ := strconv.ParseUint(current, 10, 64)
	if holder == auctionID {
		// 重入：刷新 TTL 后视为获取成功。
		if ttl > 0 {
			_ = l.client.Expire(ctx, key, ttl).Err()
		}
		return true, auctionID, nil
	}
	return false, holder, nil
}

// liveRoomReleaseScript 仅在当前 value 等于持有者时才删除。
const liveRoomReleaseScript = `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end`

func (l *LiveRoomLock) Release(ctx context.Context, roomID uint64, auctionID uint64) error {
	if l.client == nil {
		return errors.New("redis client is nil")
	}
	key := l.keys.LiveRoomActiveLock(roomID)
	value := strconv.FormatUint(auctionID, 10)
	if err := l.client.Eval(ctx, liveRoomReleaseScript, []string{key}, value).Err(); err != nil {
		if !errors.Is(err, redisgo.Nil) {
			return err
		}
	}
	return nil
}

func (l *LiveRoomLock) Current(ctx context.Context, roomID uint64) (uint64, error) {
	if l.client == nil {
		return 0, errors.New("redis client is nil")
	}
	value, err := l.client.Get(ctx, l.keys.LiveRoomActiveLock(roomID)).Result()
	if err != nil {
		if errors.Is(err, redisgo.Nil) {
			return 0, nil
		}
		return 0, err
	}
	holder, _ := strconv.ParseUint(value, 10, 64)
	return holder, nil
}

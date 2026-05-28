package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	redisgo "github.com/redis/go-redis/v9"
)

const defaultOnlineCounterTTL = 24 * time.Hour

var nextOnlineCounterInstance atomic.Uint64

// OnlineCounter 维护"在线会话"集合：每个 (auction, instance, conn) 三元组一份
// ZSET 成员，TTL 自然清理。
//
// v2 起按 auctionID 走分片：online:auction:<id> 落到 ForAuction(id) 的 shard，
// 而 ws:instances / ws:instance:<id> / online:instance:<id>:conns 这些"按 instance"
// 的全局 key 固定到 ForGlobal()（shard 0），保证多实例 Janitor 仍可枚举所有 instance。
type OnlineCounter struct {
	sharded    *ShardedRTClient
	keys       KeyBuilder
	ttl        time.Duration
	instanceID string
}

func NewOnlineCounter(sharded *ShardedRTClient, keys KeyBuilder, ttl time.Duration) *OnlineCounter {
	if ttl <= 0 {
		ttl = defaultOnlineCounterTTL
	}
	instanceID := fmt.Sprintf("inst-%d-%d", time.Now().UTC().UnixNano(), nextOnlineCounterInstance.Add(1))
	return &OnlineCounter{sharded: sharded, keys: keys, ttl: ttl, instanceID: instanceID}
}

func (c *OnlineCounter) InstanceID() string { return c.instanceID }

// auctionShard 返回 auctionID 落到的 RT shard 客户端。
func (c *OnlineCounter) auctionShard(auctionID uint64) *RedisRTClient {
	if c == nil || c.sharded == nil {
		return nil
	}
	return c.sharded.ForAuction(auctionID)
}

// globalShard 返回承载全局 key 的 RT shard 客户端。
func (c *OnlineCounter) globalShard() *RedisRTClient {
	if c == nil || c.sharded == nil {
		return nil
	}
	return c.sharded.ForGlobal()
}

func (c *OnlineCounter) Join(ctx context.Context, auctionID uint64, connectionID string) (int, error) {
	if err := c.validate(connectionID); err != nil {
		return 0, err
	}
	nowMS := time.Now().UTC().UnixMilli()
	expiresMS := nowMS + c.ttl.Milliseconds()
	instanceID, _ := splitOnlineMember(connectionID)
	if instanceID == "" {
		instanceID = c.instanceID
		connectionID = instanceID + ":" + connectionID
	}
	// 全局 key 上写 instance 心跳与 conn 索引（按 instance 而非 auction 路由）。
	gShard := c.globalShard()
	gPipe := gShard.Pipeline()
	gPipe.Set(ctx, c.keys.WSInstanceHeartbeat(instanceID), "1", c.ttl)
	gPipe.SAdd(ctx, c.keys.WSInstances(), instanceID)
	gPipe.SAdd(ctx, c.keys.OnlineInstanceConns(instanceID), fmt.Sprintf("%d|%s", auctionID, connectionID))
	gPipe.Expire(ctx, c.keys.OnlineInstanceConns(instanceID), c.ttl+time.Hour)
	if _, err := gPipe.Exec(ctx); err != nil {
		return 0, err
	}
	// 拍卖维度的 ZSET 落到 auction 的 shard。
	aShard := c.auctionShard(auctionID)
	key := c.keys.OnlineAuction(auctionID)
	aPipe := aShard.Pipeline()
	aPipe.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(nowMS, 10))
	aPipe.ZAdd(ctx, key, redisgo.Z{Score: float64(expiresMS), Member: connectionID})
	aPipe.Expire(ctx, key, c.ttl+time.Hour)
	card := aPipe.ZCard(ctx, key)
	if _, err := aPipe.Exec(ctx); err != nil {
		return 0, err
	}
	return clampNonNegative(card.Val()), nil
}

func (c *OnlineCounter) Leave(ctx context.Context, auctionID uint64, connectionID string) (int, error) {
	if err := c.validate(connectionID); err != nil {
		return 0, err
	}
	nowMS := time.Now().UTC().UnixMilli()
	instanceID, _ := splitOnlineMember(connectionID)
	if instanceID != "" {
		gShard := c.globalShard()
		if err := gShard.SRem(ctx, c.keys.OnlineInstanceConns(instanceID), fmt.Sprintf("%d|%s", auctionID, connectionID)).Err(); err != nil {
			return 0, err
		}
	}
	aShard := c.auctionShard(auctionID)
	key := c.keys.OnlineAuction(auctionID)
	pipe := aShard.Pipeline()
	pipe.ZRem(ctx, key, connectionID)
	pipe.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(nowMS, 10))
	card := pipe.ZCard(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return clampNonNegative(card.Val()), nil
}

func (c *OnlineCounter) Touch(ctx context.Context, auctionID uint64, connectionID string) (int, error) {
	if err := c.validate(connectionID); err != nil {
		return 0, err
	}
	nowMS := time.Now().UTC().UnixMilli()
	expiresMS := nowMS + c.ttl.Milliseconds()
	instanceID, _ := splitOnlineMember(connectionID)
	if instanceID == "" {
		instanceID = c.instanceID
		connectionID = instanceID + ":" + connectionID
	}
	gShard := c.globalShard()
	gPipe := gShard.Pipeline()
	gPipe.Set(ctx, c.keys.WSInstanceHeartbeat(instanceID), "1", c.ttl)
	gPipe.SAdd(ctx, c.keys.WSInstances(), instanceID)
	gPipe.SAdd(ctx, c.keys.OnlineInstanceConns(instanceID), fmt.Sprintf("%d|%s", auctionID, connectionID))
	gPipe.Expire(ctx, c.keys.OnlineInstanceConns(instanceID), c.ttl+time.Hour)
	if _, err := gPipe.Exec(ctx); err != nil {
		return 0, err
	}
	aShard := c.auctionShard(auctionID)
	key := c.keys.OnlineAuction(auctionID)
	aPipe := aShard.Pipeline()
	aPipe.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(nowMS, 10))
	aPipe.ZAdd(ctx, key, redisgo.Z{Score: float64(expiresMS), Member: connectionID})
	card := aPipe.ZCard(ctx, key)
	if _, err := aPipe.Exec(ctx); err != nil {
		return 0, err
	}
	return clampNonNegative(card.Val()), nil
}

func (c *OnlineCounter) CleanupDeadInstances(ctx context.Context) error {
	if c == nil || c.sharded == nil {
		return fmt.Errorf("redis online counter is not configured")
	}
	gShard := c.globalShard()
	instances, err := gShard.SMembers(ctx, c.keys.WSInstances()).Result()
	if err != nil {
		return err
	}
	for _, instanceID := range instances {
		exists, err := gShard.Exists(ctx, c.keys.WSInstanceHeartbeat(instanceID)).Result()
		if err != nil {
			return err
		}
		if exists > 0 {
			continue
		}
		members, err := gShard.SMembers(ctx, c.keys.OnlineInstanceConns(instanceID)).Result()
		if err != nil {
			return err
		}
		// 按 auction 分桶，分别下到对应 shard。
		buckets := make(map[*RedisRTClient][]struct {
			key    string
			member string
		})
		for _, member := range members {
			parts := strings.SplitN(member, "|", 2)
			if len(parts) != 2 {
				continue
			}
			auctionID, err := strconv.ParseUint(parts[0], 10, 64)
			if err != nil {
				continue
			}
			shard := c.auctionShard(auctionID)
			if shard == nil {
				continue
			}
			buckets[shard] = append(buckets[shard], struct {
				key    string
				member string
			}{key: c.keys.OnlineAuction(auctionID), member: parts[1]})
		}
		for shard, ops := range buckets {
			pipe := shard.Pipeline()
			for _, op := range ops {
				pipe.ZRem(ctx, op.key, op.member)
			}
			if _, err := pipe.Exec(ctx); err != nil {
				return err
			}
		}
		gPipe := gShard.Pipeline()
		gPipe.Del(ctx, c.keys.OnlineInstanceConns(instanceID))
		gPipe.SRem(ctx, c.keys.WSInstances(), instanceID)
		if _, err := gPipe.Exec(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (c *OnlineCounter) StartJanitor(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Minute
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = c.CleanupDeadInstances(ctx)
			}
		}
	}()
}

func (c *OnlineCounter) Count(ctx context.Context, auctionID uint64) (int, error) {
	if c == nil || c.sharded == nil {
		return 0, fmt.Errorf("redis online counter is not configured")
	}
	nowMS := time.Now().UTC().UnixMilli()
	aShard := c.auctionShard(auctionID)
	key := c.keys.OnlineAuction(auctionID)
	pipe := aShard.Pipeline()
	pipe.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(nowMS, 10))
	card := pipe.ZCard(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return clampNonNegative(card.Val()), nil
}

func (c *OnlineCounter) validate(connectionID string) error {
	if c == nil || c.sharded == nil {
		return fmt.Errorf("redis online counter is not configured")
	}
	if strings.TrimSpace(connectionID) == "" {
		return fmt.Errorf("connection id is required")
	}
	return nil
}

func clampNonNegative(value int64) int {
	if value <= 0 {
		return 0
	}
	return int(value)
}

func splitOnlineMember(member string) (string, string) {
	idx := strings.IndexByte(member, ':')
	if idx <= 0 || idx+1 >= len(member) {
		return "", member
	}
	return member[:idx], member[idx+1:]
}

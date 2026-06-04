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

// DefaultOnlineCounterTTL controls how long stale websocket presence survives
// when a process exits before it can remove its connections.
const DefaultOnlineCounterTTL = 90 * time.Second

var nextOnlineCounterInstance atomic.Uint64

// OnlineCounter 维护"在线用户"集合：auction 维度按 userID 去重计数，
// 同一用户的多个连接记录在 user connection set 中，TTL 自然清理。
//
// v2 起支持两种存储：
//   - RT 分片：online:auction:<id>:users 落到 ForAuction(id) 的 shard；ws:instances
//     等全局 key 固定到 ForGlobal()。
//   - Cache 实例：在线人数允许短暂延迟时可整体放到 Redis-Cache，避免主 RT
//     出价池被 WS 心跳 / 在线计数 pipeline 抢占。
type OnlineCounter struct {
	sharded    *ShardedRTClient
	cache      *RedisCacheClient
	keys       KeyBuilder
	ttl        time.Duration
	instanceID string
}

func NewOnlineCounter(sharded *ShardedRTClient, keys KeyBuilder, ttl time.Duration) *OnlineCounter {
	if ttl <= 0 {
		ttl = DefaultOnlineCounterTTL
	}
	instanceID := fmt.Sprintf("inst-%d-%d", time.Now().UTC().UnixNano(), nextOnlineCounterInstance.Add(1))
	return &OnlineCounter{sharded: sharded, keys: keys, ttl: ttl, instanceID: instanceID}
}

func NewOnlineCounterOnCache(cache *RedisCacheClient, keys KeyBuilder, ttl time.Duration) *OnlineCounter {
	if ttl <= 0 {
		ttl = DefaultOnlineCounterTTL
	}
	instanceID := fmt.Sprintf("inst-%d-%d", time.Now().UTC().UnixNano(), nextOnlineCounterInstance.Add(1))
	return &OnlineCounter{cache: cache, keys: keys, ttl: ttl, instanceID: instanceID}
}

func (c *OnlineCounter) InstanceID() string { return c.instanceID }

// auctionClient 返回 auctionID 对应的在线计数 Redis client。cache 模式下所有
// auction 共享 Redis-Cache；RT 模式下按 auctionID 分片。
func (c *OnlineCounter) auctionClient(auctionID uint64) *redisgo.Client {
	if c == nil {
		return nil
	}
	if c.cache != nil {
		return c.cache.Client
	}
	if c.sharded == nil {
		return nil
	}
	shard := c.sharded.ForAuction(auctionID)
	if shard == nil {
		return nil
	}
	return shard.Client
}

// globalClient 返回承载 ws:instances 等全局 key 的 Redis client。
func (c *OnlineCounter) globalClient() *redisgo.Client {
	if c == nil {
		return nil
	}
	if c.cache != nil {
		return c.cache.Client
	}
	if c.sharded == nil {
		return nil
	}
	shard := c.sharded.ForGlobal()
	if shard == nil {
		return nil
	}
	return shard.Client
}

func (c *OnlineCounter) Join(ctx context.Context, auctionID uint64, connectionID, userID string) (int, error) {
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
	userID = normalizeOnlineUserID(userID, connectionID)
	// 全局 key 上写 instance 心跳与 conn 索引（按 instance 而非 auction 路由）。
	gClient := c.globalClient()
	if gClient == nil {
		return 0, fmt.Errorf("redis online counter is not configured")
	}
	gPipe := gClient.Pipeline()
	gPipe.Set(ctx, c.keys.WSInstanceHeartbeat(instanceID), "1", c.ttl)
	gPipe.SAdd(ctx, c.keys.WSInstances(), instanceID)
	gPipe.SAdd(ctx, c.keys.OnlineInstanceConns(instanceID), onlineIndexMember(auctionID, connectionID, userID))
	gPipe.Expire(ctx, c.keys.OnlineInstanceConns(instanceID), c.ttl+time.Hour)
	if _, err := gPipe.Exec(ctx); err != nil {
		return 0, err
	}
	// 拍卖维度的用户 ZSET 落到 auction 的 shard；同一 userID 多连接只算一次。
	aClient := c.auctionClient(auctionID)
	if aClient == nil {
		return 0, fmt.Errorf("redis online counter is not configured")
	}
	key := c.keys.OnlineAuctionUsers(auctionID)
	userConnsKey := c.keys.OnlineAuctionUserConns(auctionID, userID)
	aPipe := aClient.Pipeline()
	aPipe.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(nowMS, 10))
	aPipe.SAdd(ctx, userConnsKey, connectionID)
	aPipe.Expire(ctx, userConnsKey, c.ttl+time.Hour)
	aPipe.ZAdd(ctx, key, redisgo.Z{Score: float64(expiresMS), Member: userID})
	aPipe.Expire(ctx, key, c.ttl+time.Hour)
	card := aPipe.ZCard(ctx, key)
	if _, err := aPipe.Exec(ctx); err != nil {
		return 0, err
	}
	return clampNonNegative(card.Val()), nil
}

func (c *OnlineCounter) Leave(ctx context.Context, auctionID uint64, connectionID, userID string) (int, error) {
	if err := c.validate(connectionID); err != nil {
		return 0, err
	}
	nowMS := time.Now().UTC().UnixMilli()
	instanceID, _ := splitOnlineMember(connectionID)
	if instanceID == "" {
		instanceID = c.instanceID
		connectionID = instanceID + ":" + connectionID
	}
	userID = normalizeOnlineUserID(userID, connectionID)
	gClient := c.globalClient()
	if gClient == nil {
		return 0, fmt.Errorf("redis online counter is not configured")
	}
	if err := gClient.SRem(ctx, c.keys.OnlineInstanceConns(instanceID), onlineIndexMember(auctionID, connectionID, userID)).Err(); err != nil {
		return 0, err
	}
	aClient := c.auctionClient(auctionID)
	if aClient == nil {
		return 0, fmt.Errorf("redis online counter is not configured")
	}
	key := c.keys.OnlineAuctionUsers(auctionID)
	userConnsKey := c.keys.OnlineAuctionUserConns(auctionID, userID)
	if err := aClient.SRem(ctx, userConnsKey, connectionID).Err(); err != nil {
		return 0, err
	}
	remaining, err := aClient.SCard(ctx, userConnsKey).Result()
	if err != nil {
		return 0, err
	}
	pipe := aClient.Pipeline()
	pipe.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(nowMS, 10))
	if remaining <= 0 {
		pipe.ZRem(ctx, key, userID)
		pipe.Del(ctx, userConnsKey)
	}
	card := pipe.ZCard(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return clampNonNegative(card.Val()), nil
}

func (c *OnlineCounter) Touch(ctx context.Context, auctionID uint64, connectionID, userID string) (int, error) {
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
	userID = normalizeOnlineUserID(userID, connectionID)
	gClient := c.globalClient()
	if gClient == nil {
		return 0, fmt.Errorf("redis online counter is not configured")
	}
	gPipe := gClient.Pipeline()
	gPipe.Set(ctx, c.keys.WSInstanceHeartbeat(instanceID), "1", c.ttl)
	gPipe.SAdd(ctx, c.keys.WSInstances(), instanceID)
	gPipe.SAdd(ctx, c.keys.OnlineInstanceConns(instanceID), onlineIndexMember(auctionID, connectionID, userID))
	gPipe.Expire(ctx, c.keys.OnlineInstanceConns(instanceID), c.ttl+time.Hour)
	if _, err := gPipe.Exec(ctx); err != nil {
		return 0, err
	}
	aClient := c.auctionClient(auctionID)
	if aClient == nil {
		return 0, fmt.Errorf("redis online counter is not configured")
	}
	key := c.keys.OnlineAuctionUsers(auctionID)
	userConnsKey := c.keys.OnlineAuctionUserConns(auctionID, userID)
	aPipe := aClient.Pipeline()
	aPipe.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(nowMS, 10))
	aPipe.SAdd(ctx, userConnsKey, connectionID)
	aPipe.Expire(ctx, userConnsKey, c.ttl+time.Hour)
	aPipe.ZAdd(ctx, key, redisgo.Z{Score: float64(expiresMS), Member: userID})
	aPipe.Expire(ctx, key, c.ttl+time.Hour)
	card := aPipe.ZCard(ctx, key)
	if _, err := aPipe.Exec(ctx); err != nil {
		return 0, err
	}
	return clampNonNegative(card.Val()), nil
}

func (c *OnlineCounter) CleanupDeadInstances(ctx context.Context) error {
	if c == nil || (c.sharded == nil && c.cache == nil) {
		return fmt.Errorf("redis online counter is not configured")
	}
	gClient := c.globalClient()
	if gClient == nil {
		return fmt.Errorf("redis online counter is not configured")
	}
	instances, err := gClient.SMembers(ctx, c.keys.WSInstances()).Result()
	if err != nil {
		return err
	}
	for _, instanceID := range instances {
		exists, err := gClient.Exists(ctx, c.keys.WSInstanceHeartbeat(instanceID)).Result()
		if err != nil {
			return err
		}
		if exists > 0 {
			continue
		}
		members, err := gClient.SMembers(ctx, c.keys.OnlineInstanceConns(instanceID)).Result()
		if err != nil {
			return err
		}
		for _, member := range members {
			auctionID, connectionID, userID, ok := splitOnlineIndexMember(member)
			if !ok {
				continue
			}
			if err := c.removeUserConnection(ctx, auctionID, connectionID, userID, time.Now().UTC().UnixMilli()); err != nil {
				return err
			}
		}
		gPipe := gClient.Pipeline()
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
	if c == nil || (c.sharded == nil && c.cache == nil) {
		return 0, fmt.Errorf("redis online counter is not configured")
	}
	nowMS := time.Now().UTC().UnixMilli()
	aClient := c.auctionClient(auctionID)
	if aClient == nil {
		return 0, fmt.Errorf("redis online counter is not configured")
	}
	key := c.keys.OnlineAuctionUsers(auctionID)
	pipe := aClient.Pipeline()
	pipe.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(nowMS, 10))
	card := pipe.ZCard(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return clampNonNegative(card.Val()), nil
}

func (c *OnlineCounter) removeUserConnection(ctx context.Context, auctionID uint64, connectionID, userID string, nowMS int64) error {
	aClient := c.auctionClient(auctionID)
	if aClient == nil {
		return nil
	}
	key := c.keys.OnlineAuctionUsers(auctionID)
	userConnsKey := c.keys.OnlineAuctionUserConns(auctionID, userID)
	if err := aClient.SRem(ctx, userConnsKey, connectionID).Err(); err != nil {
		return err
	}
	remaining, err := aClient.SCard(ctx, userConnsKey).Result()
	if err != nil {
		return err
	}
	pipe := aClient.Pipeline()
	pipe.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(nowMS, 10))
	if remaining <= 0 {
		pipe.ZRem(ctx, key, userID)
		pipe.Del(ctx, userConnsKey)
	}
	_, err = pipe.Exec(ctx)
	return err
}

func (c *OnlineCounter) validate(connectionID string) error {
	if c == nil || (c.sharded == nil && c.cache == nil) {
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

func normalizeOnlineUserID(userID, connectionID string) string {
	if strings.TrimSpace(userID) == "" {
		return "conn:" + connectionID
	}
	return "user:" + userID
}

func onlineIndexMember(auctionID uint64, connectionID, userID string) string {
	return fmt.Sprintf("%d|%s|%s", auctionID, connectionID, userID)
}

func splitOnlineIndexMember(member string) (uint64, string, string, bool) {
	parts := strings.SplitN(member, "|", 3)
	if len(parts) < 2 {
		return 0, "", "", false
	}
	auctionID, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, "", "", false
	}
	connectionID := parts[1]
	userID := ""
	if len(parts) == 3 {
		userID = parts[2]
	} else {
		userID = normalizeOnlineUserID("", connectionID)
	}
	return auctionID, connectionID, userID, true
}

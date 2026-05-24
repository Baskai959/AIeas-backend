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

type OnlineCounter struct {
	client     *redisgo.Client
	keys       KeyBuilder
	ttl        time.Duration
	instanceID string
}

func NewOnlineCounter(client *redisgo.Client, keys KeyBuilder, ttl time.Duration) *OnlineCounter {
	if ttl <= 0 {
		ttl = defaultOnlineCounterTTL
	}
	instanceID := fmt.Sprintf("inst-%d-%d", time.Now().UTC().UnixNano(), nextOnlineCounterInstance.Add(1))
	return &OnlineCounter{client: client, keys: keys, ttl: ttl, instanceID: instanceID}
}

func (c *OnlineCounter) InstanceID() string { return c.instanceID }

func (c *OnlineCounter) Join(ctx context.Context, auctionID uint64, connectionID string) (int, error) {
	if err := c.validate(connectionID); err != nil {
		return 0, err
	}
	nowMS := time.Now().UTC().UnixMilli()
	expiresMS := nowMS + c.ttl.Milliseconds()
	key := c.keys.OnlineAuction(auctionID)
	instanceID, _ := splitOnlineMember(connectionID)
	if instanceID == "" {
		instanceID = c.instanceID
		connectionID = instanceID + ":" + connectionID
	}
	pipe := c.client.Pipeline()
	pipe.Set(ctx, c.keys.WSInstanceHeartbeat(instanceID), "1", c.ttl)
	pipe.SAdd(ctx, c.keys.WSInstances(), instanceID)
	pipe.SAdd(ctx, c.keys.OnlineInstanceConns(instanceID), fmt.Sprintf("%d|%s", auctionID, connectionID))
	pipe.Expire(ctx, c.keys.OnlineInstanceConns(instanceID), c.ttl+time.Hour)
	pipe.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(nowMS, 10))
	pipe.ZAdd(ctx, key, redisgo.Z{Score: float64(expiresMS), Member: connectionID})
	pipe.Expire(ctx, key, c.ttl+time.Hour)
	card := pipe.ZCard(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return clampNonNegative(card.Val()), nil
}

func (c *OnlineCounter) Leave(ctx context.Context, auctionID uint64, connectionID string) (int, error) {
	if err := c.validate(connectionID); err != nil {
		return 0, err
	}
	nowMS := time.Now().UTC().UnixMilli()
	key := c.keys.OnlineAuction(auctionID)
	instanceID, _ := splitOnlineMember(connectionID)
	pipe := c.client.Pipeline()
	pipe.ZRem(ctx, key, connectionID)
	if instanceID != "" {
		pipe.SRem(ctx, c.keys.OnlineInstanceConns(instanceID), fmt.Sprintf("%d|%s", auctionID, connectionID))
	}
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
	key := c.keys.OnlineAuction(auctionID)
	instanceID, _ := splitOnlineMember(connectionID)
	if instanceID == "" {
		instanceID = c.instanceID
		connectionID = instanceID + ":" + connectionID
	}
	pipe := c.client.Pipeline()
	pipe.Set(ctx, c.keys.WSInstanceHeartbeat(instanceID), "1", c.ttl)
	pipe.SAdd(ctx, c.keys.WSInstances(), instanceID)
	pipe.SAdd(ctx, c.keys.OnlineInstanceConns(instanceID), fmt.Sprintf("%d|%s", auctionID, connectionID))
	pipe.Expire(ctx, c.keys.OnlineInstanceConns(instanceID), c.ttl+time.Hour)
	pipe.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(nowMS, 10))
	pipe.ZAdd(ctx, key, redisgo.Z{Score: float64(expiresMS), Member: connectionID})
	card := pipe.ZCard(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return clampNonNegative(card.Val()), nil
}

func (c *OnlineCounter) CleanupDeadInstances(ctx context.Context) error {
	if c == nil || c.client == nil {
		return fmt.Errorf("redis online counter is not configured")
	}
	instances, err := c.client.SMembers(ctx, c.keys.WSInstances()).Result()
	if err != nil {
		return err
	}
	for _, instanceID := range instances {
		exists, err := c.client.Exists(ctx, c.keys.WSInstanceHeartbeat(instanceID)).Result()
		if err != nil || exists > 0 {
			if err != nil {
				return err
			}
			continue
		}
		members, err := c.client.SMembers(ctx, c.keys.OnlineInstanceConns(instanceID)).Result()
		if err != nil {
			return err
		}
		pipe := c.client.Pipeline()
		for _, member := range members {
			parts := strings.SplitN(member, "|", 2)
			if len(parts) != 2 {
				continue
			}
			auctionID, err := strconv.ParseUint(parts[0], 10, 64)
			if err != nil {
				continue
			}
			pipe.ZRem(ctx, c.keys.OnlineAuction(auctionID), parts[1])
		}
		pipe.Del(ctx, c.keys.OnlineInstanceConns(instanceID))
		pipe.SRem(ctx, c.keys.WSInstances(), instanceID)
		if _, err := pipe.Exec(ctx); err != nil {
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
	if c == nil || c.client == nil {
		return 0, fmt.Errorf("redis online counter is not configured")
	}
	nowMS := time.Now().UTC().UnixMilli()
	key := c.keys.OnlineAuction(auctionID)
	pipe := c.client.Pipeline()
	pipe.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(nowMS, 10))
	card := pipe.ZCard(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return clampNonNegative(card.Val()), nil
}

func (c *OnlineCounter) validate(connectionID string) error {
	if c == nil || c.client == nil {
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

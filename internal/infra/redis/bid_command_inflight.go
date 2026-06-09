package redis

import (
	"context"
	"strconv"
	"strings"
	"time"

	redisgo "github.com/redis/go-redis/v9"
)

const DefaultBidCommandInFlightTTL = 2 * time.Minute

// BidCommandInFlightTracker tracks bid commands that have entered the async
// Kafka arbitration path but have not finished Redis Lua arbitration yet.
//
// The key is per auction and shared by all backend instances, so hammer drain
// can wait for commands published by other WS/API instances.
type BidCommandInFlightTracker struct {
	sharded *ShardedRTClient
	keys    KeyBuilder
	ttl     time.Duration
}

func NewBidCommandInFlightTracker(sharded *ShardedRTClient, keys KeyBuilder, ttl time.Duration) *BidCommandInFlightTracker {
	if ttl <= 0 {
		ttl = DefaultBidCommandInFlightTTL
	}
	return &BidCommandInFlightTracker{sharded: sharded, keys: keys, ttl: ttl}
}

func (t *BidCommandInFlightTracker) TrackBidCommand(ctx context.Context, auctionID uint64, bidID string) error {
	if t == nil || auctionID == 0 {
		return nil
	}
	bidID = strings.TrimSpace(bidID)
	if bidID == "" {
		return nil
	}
	client := t.clientForAuction(auctionID)
	if client == nil {
		return nil
	}
	key := t.keys.AuctionBidCommandInFlight(auctionID)
	nowMS := time.Now().UTC().UnixMilli()
	pipe := client.TxPipeline()
	pipe.ZAdd(ctx, key, redisgo.Z{Score: float64(nowMS), Member: bidID})
	pipe.Expire(ctx, key, t.ttl)
	_, err := pipe.Exec(ctx)
	return err
}

func (t *BidCommandInFlightTracker) ReleaseBidCommand(ctx context.Context, auctionID uint64, bidID string) error {
	if t == nil || auctionID == 0 {
		return nil
	}
	bidID = strings.TrimSpace(bidID)
	if bidID == "" {
		return nil
	}
	client := t.clientForAuction(auctionID)
	if client == nil {
		return nil
	}
	return client.ZRem(ctx, t.keys.AuctionBidCommandInFlight(auctionID), bidID).Err()
}

// PendingForAuction implements auctionapp.HammerDrainCoordinator. Redis errors
// are treated as "still pending" so hammer waits until drainMaxWait instead of
// silently finalizing on an unknown distributed state.
func (t *BidCommandInFlightTracker) PendingForAuction(auctionID uint64) int {
	if t == nil || auctionID == 0 {
		return 0
	}
	client := t.clientForAuction(auctionID)
	if client == nil {
		return 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	key := t.keys.AuctionBidCommandInFlight(auctionID)
	cutoffMS := time.Now().UTC().Add(-t.ttl).UnixMilli()
	pipe := client.TxPipeline()
	pipe.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(cutoffMS, 10))
	card := pipe.ZCard(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil {
		return 1
	}
	return int(card.Val())
}

func (t *BidCommandInFlightTracker) clientForAuction(auctionID uint64) *RedisRTClient {
	if t == nil || t.sharded == nil {
		return nil
	}
	return t.sharded.ForAuction(auctionID)
}

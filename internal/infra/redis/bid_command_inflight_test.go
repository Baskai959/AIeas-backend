package redis

import (
	"context"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	redisgo "github.com/redis/go-redis/v9"
)

func TestBidCommandInFlightTrackerTrackReleaseAndTrim(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	client := redisgo.NewClient(&redisgo.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	keys := NewKeyBuilder("test")
	rt := &RedisRTClient{Client: client}
	sharded := NewShardedRTClientFromShards([]*RedisRTClient{rt})
	tracker := NewBidCommandInFlightTracker(sharded, keys, 50*time.Millisecond)
	auctionID := uint64(9001)

	if got := tracker.PendingForAuction(auctionID); got != 0 {
		t.Fatalf("initial pending = %d, want 0", got)
	}
	if err := tracker.TrackBidCommand(ctx, auctionID, "bid-1"); err != nil {
		t.Fatalf("track bid-1: %v", err)
	}
	if err := tracker.TrackBidCommand(ctx, auctionID, "bid-2"); err != nil {
		t.Fatalf("track bid-2: %v", err)
	}
	if got := tracker.PendingForAuction(auctionID); got != 2 {
		t.Fatalf("pending after track = %d, want 2", got)
	}
	if err := tracker.ReleaseBidCommand(ctx, auctionID, "bid-1"); err != nil {
		t.Fatalf("release bid-1: %v", err)
	}
	if got := tracker.PendingForAuction(auctionID); got != 1 {
		t.Fatalf("pending after release = %d, want 1", got)
	}
	time.Sleep(80 * time.Millisecond)
	if got := tracker.PendingForAuction(auctionID); got != 0 {
		t.Fatalf("pending after ttl trim = %d, want 0", got)
	}
}

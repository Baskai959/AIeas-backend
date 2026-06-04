package redis

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	redisgo "github.com/redis/go-redis/v9"
)

// cmdCounterHook 统计执行过的 XGROUP CREATE / XREADGROUP 命令次数；用于断言
// EventLog 缓存命中后不再重复发送 XGroupCreateMkStream。
type cmdCounterHook struct {
	xgroupCreate int64
	xreadGroup   int64
}

func (h *cmdCounterHook) DialHook(next redisgo.DialHook) redisgo.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (h *cmdCounterHook) ProcessHook(next redisgo.ProcessHook) redisgo.ProcessHook {
	return func(ctx context.Context, cmd redisgo.Cmder) error {
		name := strings.ToUpper(cmd.Name())
		args := cmd.Args()
		if name == "XGROUP" && len(args) >= 2 {
			if sub, ok := args[1].(string); ok && strings.EqualFold(sub, "CREATE") {
				atomic.AddInt64(&h.xgroupCreate, 1)
			}
		}
		if name == "XREADGROUP" {
			atomic.AddInt64(&h.xreadGroup, 1)
		}
		return next(ctx, cmd)
	}
}

func (h *cmdCounterHook) ProcessPipelineHook(next redisgo.ProcessPipelineHook) redisgo.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redisgo.Cmder) error {
		return next(ctx, cmds)
	}
}

func newEventLogTestRig(t *testing.T) (*EventLog, *cmdCounterHook, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redisgo.NewClient(&redisgo.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	hook := &cmdCounterHook{}
	client.AddHook(hook)
	rt := &RedisRTClient{Client: client}
	sharded := NewShardedRTClientFromShards([]*RedisRTClient{rt})
	keys := NewKeyBuilder("test")
	log := NewEventLog(sharded, keys)
	return log, hook, mr
}

// TestEventLogEnsureCachesAfterFirstSuccess: 多次 ReadConsumerGroup 只触发一次
// XGroupCreateMkStream（缓存命中后跳过）。
func TestEventLogEnsureCachesAfterFirstSuccess(t *testing.T) {
	log, hook, _ := newEventLogTestRig(t)
	ctx := context.Background()
	auctionID := uint64(1001)
	for i := 0; i < 5; i++ {
		if _, err := log.ReadConsumerGroup(ctx, auctionID, BidRecordConsumerGroup, "c1", 1, time.Millisecond); err != nil {
			t.Fatalf("read#%d: %v", i, err)
		}
	}
	if got := atomic.LoadInt64(&hook.xgroupCreate); got != 1 {
		t.Fatalf("XGROUP CREATE got=%d want=1", got)
	}
	if got := atomic.LoadInt64(&hook.xreadGroup); got != 5 {
		t.Fatalf("XREADGROUP got=%d want=5", got)
	}
}

// TestEventLogEnsureCachesOnBusyGroup: 第一次 XGroupCreateMkStream 返回 BUSYGROUP，
// 缓存仍被标记，后续不再发 XGroupCreateMkStream。
func TestEventLogEnsureCachesOnBusyGroup(t *testing.T) {
	log, hook, mr := newEventLogTestRig(t)
	ctx := context.Background()
	auctionID := uint64(2002)
	stream := log.keys.AuctionStream(auctionID)
	c := redisgo.NewClient(&redisgo.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	if err := c.XGroupCreateMkStream(ctx, stream, BidRecordConsumerGroup, "0-0").Err(); err != nil {
		t.Fatalf("seed group: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := log.EnsureConsumerGroup(ctx, auctionID, BidRecordConsumerGroup); err != nil {
			t.Fatalf("ensure#%d: %v", i, err)
		}
	}
	if got := atomic.LoadInt64(&hook.xgroupCreate); got != 1 {
		t.Fatalf("XGROUP CREATE got=%d want=1 (BUSYGROUP must mark cache)", got)
	}
}

// TestEventLogEnsureCacheRecoversOnNoGroup: XREADGROUP 返回 NOGROUP 时，缓存被清理、
// 重新 EnsureConsumerGroup、并重试一次 XREADGROUP。
func TestEventLogEnsureCacheRecoversOnNoGroup(t *testing.T) {
	log, hook, mr := newEventLogTestRig(t)
	ctx := context.Background()
	auctionID := uint64(3003)
	if _, err := log.ReadConsumerGroup(ctx, auctionID, BidRecordConsumerGroup, "c1", 1, time.Millisecond); err != nil {
		t.Fatalf("warmup read: %v", err)
	}
	if got := atomic.LoadInt64(&hook.xgroupCreate); got != 1 {
		t.Fatalf("after warmup XGROUP CREATE got=%d want=1", got)
	}

	stream := log.keys.AuctionStream(auctionID)
	mr.Del(stream)

	beforeCreate := atomic.LoadInt64(&hook.xgroupCreate)
	beforeRead := atomic.LoadInt64(&hook.xreadGroup)
	if _, err := log.ReadConsumerGroup(ctx, auctionID, BidRecordConsumerGroup, "c1", 1, time.Millisecond); err != nil {
		t.Fatalf("recover read: %v", err)
	}
	createDelta := atomic.LoadInt64(&hook.xgroupCreate) - beforeCreate
	readDelta := atomic.LoadInt64(&hook.xreadGroup) - beforeRead
	if createDelta != 1 {
		t.Fatalf("recover XGROUP CREATE delta=%d want=1", createDelta)
	}
	if readDelta != 2 {
		t.Fatalf("recover XREADGROUP delta=%d want=2 (initial NOGROUP + retry)", readDelta)
	}

	beforeCreate = atomic.LoadInt64(&hook.xgroupCreate)
	if _, err := log.ReadConsumerGroup(ctx, auctionID, BidRecordConsumerGroup, "c1", 1, time.Millisecond); err != nil {
		t.Fatalf("post-recover read: %v", err)
	}
	if delta := atomic.LoadInt64(&hook.xgroupCreate) - beforeCreate; delta != 0 {
		t.Fatalf("post-recover XGROUP CREATE delta=%d want=0", delta)
	}
}

// TestEventLogEnsureNotCachedOnOtherError: 非 BUSYGROUP / 成功的错误时缓存不被
// 标记，下一次仍会重试。
func TestEventLogEnsureNotCachedOnOtherError(t *testing.T) {
	log, _, mr := newEventLogTestRig(t)
	ctx := context.Background()
	mr.Close()
	err := log.EnsureConsumerGroup(ctx, 4004, BidRecordConsumerGroup)
	if err == nil {
		t.Fatalf("expected error from closed miniredis")
	}
	if _, ok := log.ensuredGroups.Load(ensureGroupKey{shardIdx: 0, auctionID: 4004, group: BidRecordConsumerGroup}); ok {
		t.Fatalf("ensure cache must not mark on connection error")
	}
}

// TestEventLogUpdateAcceptedRankingTwoBidsBySameUser 验证同一用户连续两次出价后，
// ranking ZSet 中只剩新 member、user_bids 只剩新编码，且编码格式与 Lua 历史实现一致。
func TestEventLogUpdateAcceptedRankingTwoBidsBySameUser(t *testing.T) {
	log, _, _ := newEventLogTestRig(t)
	ctx := context.Background()
	auctionID := uint64(5005)
	bidder := "u_1001"
	price1, ts1 := int64(1100), int64(1700000000000)
	price2, ts2 := int64(1200), int64(1700000005000)

	if err := log.UpdateAcceptedRanking(ctx, auctionID, bidder, price1, ts1, 1); err != nil {
		t.Fatalf("first update: %v", err)
	}
	if err := log.UpdateAcceptedRanking(ctx, auctionID, bidder, price2, ts2, 2); err != nil {
		t.Fatalf("second update: %v", err)
	}

	client := log.shardForAuction(auctionID).Client
	rankingKey := log.keys.AuctionBids(auctionID)
	userBidsKey := log.keys.AuctionUserBids(auctionID)

	members, err := client.ZRange(ctx, rankingKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("zrange: %v", err)
	}
	want := FormatRankingMember(price2, ts2, bidder)
	if len(members) != 1 || members[0] != want {
		t.Fatalf("ranking ZSet members got=%v want=[%s]", members, want)
	}

	got, err := client.HGet(ctx, userBidsKey, bidder).Result()
	if err != nil {
		t.Fatalf("hget user_bids: %v", err)
	}
	if got != want {
		t.Fatalf("user_bids encoding got=%s want=%s", got, want)
	}

	// 锁定展示排行榜使用的字节格式：%019d:%013d:%s
	if want != "0000000000000001200:8299999994999:u_1001" {
		t.Fatalf("ranking_member encoding drift: got %s", want)
	}
}

func TestEventLogUpdateAcceptedRankingUsesDedicatedRankingClient(t *testing.T) {
	log, _, _ := newEventLogTestRig(t)
	rankingMR := miniredis.RunT(t)
	rankingClient := redisgo.NewClient(&redisgo.Options{Addr: rankingMR.Addr()})
	t.Cleanup(func() { _ = rankingClient.Close() })
	log.SetRankingShardedRT(NewShardedRTClientFromShards([]*RedisRTClient{{Client: rankingClient}}))

	ctx := context.Background()
	auctionID := uint64(5505)
	bidder := "u_ranking_cache"
	price, ts := int64(1300), int64(1700000007000)
	if err := log.UpdateAcceptedRanking(ctx, auctionID, bidder, price, ts, 7); err != nil {
		t.Fatalf("update ranking: %v", err)
	}

	mainClient := log.shardForAuction(auctionID).Client
	if got, err := mainClient.ZCard(ctx, log.keys.AuctionBids(auctionID)).Result(); err != nil || got != 0 {
		t.Fatalf("main RT ranking key should stay empty, zcard=%d err=%v", got, err)
	}
	if got, err := rankingClient.ZCard(ctx, log.keys.AuctionBids(auctionID)).Result(); err != nil || got != 1 {
		t.Fatalf("dedicated ranking key should be updated, zcard=%d err=%v", got, err)
	}
	if got, err := rankingClient.HGet(ctx, log.keys.AuctionUserBids(auctionID), bidder).Result(); err != nil || got != FormatRankingMember(price, ts, bidder) {
		t.Fatalf("dedicated user_bids got=%s err=%v", got, err)
	}
}

// TestEventLogUpdateAcceptedRankingPreventsOutOfOrder 验证 (price, bidTSMS, seq) 三元组
// 防乱序：旧 seq=1 在 seq=2 之后到达不应覆盖；同 price/ts 不同 seq、同 price 不同 ts 的
// 比较器都应正确生效。
func TestEventLogUpdateAcceptedRankingPreventsOutOfOrder(t *testing.T) {
	log, _, _ := newEventLogTestRig(t)
	ctx := context.Background()
	auctionID := uint64(6006)
	bidder := "u_1001"

	// seq=2 先到 (price=1200, ts=2000), 然后 seq=1 (price=1100, ts=1000) 后到 — 旧应不覆盖.
	if err := log.UpdateAcceptedRanking(ctx, auctionID, bidder, 1200, 2000, 2); err != nil {
		t.Fatalf("seq2: %v", err)
	}
	if err := log.UpdateAcceptedRanking(ctx, auctionID, bidder, 1100, 1000, 1); err != nil {
		t.Fatalf("seq1: %v", err)
	}
	client := log.shardForAuction(auctionID).Client
	got, err := client.HGet(ctx, log.keys.AuctionUserBids(auctionID), bidder).Result()
	if err != nil {
		t.Fatalf("hget: %v", err)
	}
	want := FormatRankingMember(1200, 2000, bidder)
	if got != want {
		t.Fatalf("out-of-order seq=1 must not overwrite seq=2; user_bids got=%s want=%s", got, want)
	}
	members, err := client.ZRange(ctx, log.keys.AuctionBids(auctionID), 0, -1).Result()
	if err != nil {
		t.Fatalf("zrange: %v", err)
	}
	if len(members) != 1 || members[0] != want {
		t.Fatalf("ranking ZSet got=%v want=[%s]", members, want)
	}

	// 同 price 不同 ts: ts 大者赢
	auction2 := uint64(6007)
	if err := log.UpdateAcceptedRanking(ctx, auction2, bidder, 1500, 5000, 1); err != nil {
		t.Fatalf("auction2 first: %v", err)
	}
	if err := log.UpdateAcceptedRanking(ctx, auction2, bidder, 1500, 6000, 2); err != nil {
		t.Fatalf("auction2 newer ts: %v", err)
	}
	got2, _ := client.HGet(ctx, log.keys.AuctionUserBids(auction2), bidder).Result()
	if got2 != FormatRankingMember(1500, 6000, bidder) {
		t.Fatalf("same price newer ts must replace, got %s", got2)
	}

	// 同 price 同 ts 不同 seq: seq 大者赢
	auction3 := uint64(6008)
	if err := log.UpdateAcceptedRanking(ctx, auction3, bidder, 2000, 9000, 5); err != nil {
		t.Fatalf("auction3 seq5: %v", err)
	}
	if err := log.UpdateAcceptedRanking(ctx, auction3, bidder, 2000, 9000, 4); err != nil {
		t.Fatalf("auction3 seq4: %v", err)
	}
	got3, _ := client.HGet(ctx, log.keys.AuctionUserBids(auction3), bidder).Result()
	if got3 != FormatRankingMember(2000, 9000, bidder) {
		t.Fatalf("same price/ts smaller seq must not overwrite, got %s", got3)
	}
}

// TestEventLogUpdateAcceptedRankingConcurrentDifferentBidders 验证热点拍卖中不同用户
// 并发更新同一个 ranking/user_bids key 时不会再出现 WATCH key 级冲突导致的失败。
func TestEventLogUpdateAcceptedRankingConcurrentDifferentBidders(t *testing.T) {
	log, _, _ := newEventLogTestRig(t)
	ctx := context.Background()
	auctionID := uint64(7007)

	const bidders = 64
	errCh := make(chan error, bidders)
	var wg sync.WaitGroup
	for i := 0; i < bidders; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			bidder := "u_concurrent_" + strconv.Itoa(i)
			price := int64(1000 + i)
			ts := int64(1700000000000 + i)
			if err := log.UpdateAcceptedRanking(ctx, auctionID, bidder, price, ts, int64(i+1)); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent ranking update failed: %v", err)
	}

	client := log.shardForAuction(auctionID).Client
	count, err := client.ZCard(ctx, log.keys.AuctionBids(auctionID)).Result()
	if err != nil {
		t.Fatalf("zcard: %v", err)
	}
	if count != bidders {
		t.Fatalf("ranking ZSet count got=%d want=%d", count, bidders)
	}
}

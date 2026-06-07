package service

import (
	"context"
	"encoding/json"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	redisinfra "aieas_backend/internal/infra/redis"
	"aieas_backend/internal/tests/repository"
	corews "aieas_backend/internal/transport/ws"
)

func TestBidServiceStreamEnabledDoesNotPersistOrBroadcastDirectly(t *testing.T) {
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	auction := domain.AuctionLot{AuctionID: 10001, SellerID: "u_2001", AuctionType: domain.AuctionTypeEnglish, StartPrice: 1000, ReservePrice: 1000, CapPrice: 2000, IncrementRule: json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":10}`), Status: domain.AuctionStatusRunning, StartTime: time.Now().Add(-time.Minute), EndTime: time.Now().Add(time.Hour)}
	if err := auctionRepo.Create(ctx, &auction); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	bids := &trackingBidRepo{findErr: domain.ErrNotFound}
	realtime := &streamEnabledRealtime{result: domain.BidResult{RequestID: "req-1", AuctionID: auction.AuctionID, BidderID: "u_1001", Price: 1100, Accepted: true, CurrentPrice: 1100, Seq: 3, StreamID: "3-0", Event: "bid.accepted"}}
	publisher := &trackingPublisher{}
	svc := NewBidService(bids, auctionRepo, realtime, nil, publisher, appconfig.Default().Auction)

	result, err := svc.Place(ctx, PlaceBidInput{RequestID: "req-1", AuctionID: auction.AuctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000)})
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if !result.Accepted || result.Seq != 3 || result.StreamID != "3-0" {
		t.Fatalf("unexpected result from stream realtime: %+v", result)
	}
	if bids.createCalls != 0 {
		t.Fatalf("stream-enabled bid service must not synchronously persist bid, createCalls=%d", bids.createCalls)
	}
	if publisher.broadcasts != 0 {
		t.Fatalf("stream-enabled bid service must not directly broadcast fact events, broadcasts=%d", publisher.broadcasts)
	}
}

func TestBidServicePrecheckRejectDoesNotPersistOrPublish(t *testing.T) {
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	auction := domain.AuctionLot{AuctionID: 10001, SellerID: "u_2001", AuctionType: domain.AuctionTypeEnglish, StartPrice: 1000, ReservePrice: 1000, CapPrice: 2000, IncrementRule: json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":10}`), Status: domain.AuctionStatusRunning, StartTime: time.Now().Add(-time.Minute), EndTime: time.Now().Add(time.Hour)}
	if err := auctionRepo.Create(ctx, &auction); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	bids := &trackingBidRepo{findErr: domain.ErrNotFound}
	realtime := &streamEnabledRealtime{result: domain.BidResult{Accepted: false, Reason: domain.BidRejectStepMismatch, CurrentPrice: 1000, AuctionStatus: domain.AuctionStatusRunning, Version: 1, Event: "bid.rejected", RiskResult: domain.BidRiskReject}}
	publisher := &trackingPublisher{}
	svc := NewBidService(bids, auctionRepo, realtime, nil, publisher, appconfig.Default().Auction)

	result, err := svc.Place(ctx, PlaceBidInput{RequestID: "req-reject", AuctionID: auction.AuctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1050, ExpectedCurrentPrice: expectedCurrentPrice(1000)})
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if result.Accepted || result.Reason != domain.BidRejectStepMismatch {
		t.Fatalf("expected precheck rejection, got %+v", result)
	}
	if bids.createCalls != 0 {
		t.Fatalf("precheck rejection should not persist bid_record, createCalls=%d", bids.createCalls)
	}
	if publisher.broadcasts != 0 {
		t.Fatalf("precheck rejection should not broadcast to room, broadcasts=%d", publisher.broadcasts)
	}
}

func TestBidServiceUsesAuctionRepoForStaticParams(t *testing.T) {
	ctx := context.Background()
	rule := json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":10}`)
	bids := &trackingBidRepo{findErr: domain.ErrNotFound}
	auctionRepo := repository.NewMemoryAuctionRepository()
	liveSession := uint64(90001)
	auction := domain.AuctionLot{AuctionID: 10001, SellerID: "u_2001", AuctionType: domain.AuctionTypeEnglish, StartPrice: 1000, CapPrice: 2000, IncrementRule: rule, Status: domain.AuctionStatusRunning, LiveSessionID: &liveSession, StartTime: time.Now().Add(-time.Minute), EndTime: time.Now().Add(time.Hour)}
	if err := auctionRepo.Create(ctx, &auction); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	realtime := &streamEnabledRealtime{
		result: domain.BidResult{RequestID: "rt-state", AuctionID: 10001, BidderID: "u_1001", Price: 1100, Accepted: true, CurrentPrice: 1100, Event: "bid.accepted"},
	}
	svc := NewBidService(bids, auctionRepo, realtime, nil, nil, appconfig.Default().Auction)

	result, err := svc.Place(ctx, PlaceBidInput{RequestID: "rt-state", AuctionID: 10001, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000)})
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if !result.Accepted {
		t.Fatalf("expected accepted result, got %+v", result)
	}
	if realtime.lastInput.LiveSessionID != 90001 || realtime.lastInput.StartPrice != 1000 || realtime.lastInput.CapPrice != 2000 || realtime.lastInput.IncrementRule.Type != domain.IncrementRuleTypeFixed {
		t.Fatalf("bid input did not use auction static params: %+v", realtime.lastInput)
	}
	if realtime.getStateCalls != 0 {
		t.Fatalf("main path must not call GetAuctionState, got=%d", realtime.getStateCalls)
	}
}

func TestBidServiceCachesAuctionMetadata(t *testing.T) {
	ctx := context.Background()
	inner := repository.NewMemoryAuctionRepository()
	auction := domain.AuctionLot{AuctionID: 10002, SellerID: "u_2001", AuctionType: domain.AuctionTypeEnglish, StartPrice: 1000, ReservePrice: 1000, CapPrice: 2000, IncrementRule: json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":10}`), Status: domain.AuctionStatusRunning, StartTime: time.Now().Add(-time.Minute), EndTime: time.Now().Add(time.Hour)}
	if err := inner.Create(ctx, &auction); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	auctionRepo := &countingAuctionRepo{AuctionRepository: inner}
	realtime := &streamEnabledRealtime{result: domain.BidResult{AuctionID: auction.AuctionID, Accepted: true, CurrentPrice: 1100, Event: "bid.accepted"}}
	svc := NewBidService(&trackingBidRepo{findErr: domain.ErrNotFound}, auctionRepo, realtime, nil, nil, appconfig.Default().Auction)

	for i := 0; i < 2; i++ {
		requestID := "cache-bid-" + strconv.Itoa(i)
		if _, err := svc.Place(ctx, PlaceBidInput{RequestID: requestID, AuctionID: auction.AuctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000)}); err != nil {
			t.Fatalf("place %d: %v", i, err)
		}
	}
	if auctionRepo.findCalls != 1 {
		t.Fatalf("auction metadata should be cached across hot bids, FindByID calls=%d", auctionRepo.findCalls)
	}
}

func TestBidServiceFastRejectsStaleBidAfterPrerequisites(t *testing.T) {
	ctx := context.Background()
	rule := json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":10}`)
	auctionRepo := repository.NewMemoryAuctionRepository()
	auction := domain.AuctionLot{AuctionID: 10003, SellerID: "u_2001", AuctionType: domain.AuctionTypeEnglish, StartPrice: 1000, ReservePrice: 1000, CapPrice: 0, IncrementRule: rule, Status: domain.AuctionStatusRunning, StartTime: time.Now().Add(-time.Minute), EndTime: time.Now().Add(time.Hour)}
	if err := auctionRepo.Create(ctx, &auction); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	realtime := &streamEnabledRealtime{}
	bids := &trackingBidRepo{findErr: domain.ErrNotFound}
	svc := NewBidService(bids, auctionRepo, realtime, nil, nil, appconfig.Default().Auction)
	svc.StoreBidRealtimeStateForTest(domain.AuctionState{
		AuctionID:     auction.AuctionID,
		Status:        domain.AuctionStatusRunning,
		StartPrice:    1000,
		IncrementRule: rule,
		CurrentPrice:  2000,
		EndTime:       auction.EndTime,
		Version:       9,
	}, time.Now().Add(bidRealtimeStateCacheTTL))

	result, err := svc.Place(ctx, PlaceBidInput{RequestID: "stale-fast", AuctionID: auction.AuctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000)})
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if result.Accepted || result.Reason != domain.BidRejectBelowMinIncrement {
		t.Fatalf("expected stale bid fast reject, got %+v", result)
	}
	if realtime.prereqCalls != 0 || realtime.lastInput.RequestID != "" || bids.createCalls != 0 || realtime.getStateCalls != 0 {
		t.Fatalf("stale fast reject should skip prerequisites, lua, persistence and Redis state; prereq=%d input=%+v create=%d getState=%d", realtime.prereqCalls, realtime.lastInput, bids.createCalls, realtime.getStateCalls)
	}
}

func TestBidServiceLocalStateCacheOnlyRejectsClearlyInvalidStaleBid(t *testing.T) {
	ctx := context.Background()
	rule := json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":10}`)
	auctionRepo := repository.NewMemoryAuctionRepository()
	auction := domain.AuctionLot{AuctionID: 10004, SellerID: "u_2001", AuctionType: domain.AuctionTypeEnglish, StartPrice: 1000, ReservePrice: 1000, CapPrice: 2000, IncrementRule: rule, Status: domain.AuctionStatusRunning, StartTime: time.Now().Add(-time.Minute), EndTime: time.Now().Add(time.Hour)}
	if err := auctionRepo.Create(ctx, &auction); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	realtime := &streamEnabledRealtime{
		stateOK: true,
		state: domain.AuctionState{
			AuctionID:     auction.AuctionID,
			Status:        domain.AuctionStatusRunning,
			StartPrice:    1000,
			CapPrice:      2000,
			IncrementRule: rule,
			CurrentPrice:  1000,
			StartTime:     auction.StartTime,
			EndTime:       auction.EndTime,
			Version:       1,
		},
		result: domain.BidResult{AuctionID: auction.AuctionID, Accepted: true, CurrentPrice: 1100, Version: 2, Event: "bid.accepted", AuctionStatus: domain.AuctionStatusRunning},
	}
	svc := NewBidService(&trackingBidRepo{findErr: domain.ErrNotFound}, auctionRepo, realtime, nil, nil, appconfig.Default().Auction)

	first, err := svc.Place(ctx, PlaceBidInput{RequestID: "local-cache-first", AuctionID: auction.AuctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000)})
	if err != nil || !first.Accepted {
		t.Fatalf("expected first bid accepted, result=%+v err=%v", first, err)
	}
	if realtime.placeBidCalls != 1 {
		t.Fatalf("expected first bid to call realtime once, calls=%d", realtime.placeBidCalls)
	}

	stale, err := svc.Place(ctx, PlaceBidInput{RequestID: "local-cache-stale", AuctionID: auction.AuctionID, BidderID: "u_1002", UserRole: domain.RoleBuyer, Price: 1100, ExpectedCurrentPrice: expectedCurrentPrice(1000)})
	if err != nil {
		t.Fatalf("stale place: %v", err)
	}
	if stale.Accepted || stale.Reason != domain.BidRejectBelowMinIncrement {
		t.Fatalf("expected local cached state to reject stale same-price bid, got %+v", stale)
	}
	if realtime.placeBidCalls != 1 {
		t.Fatalf("local cached stale reject should skip realtime lua, calls=%d", realtime.placeBidCalls)
	}

	realtime.result = domain.BidResult{AuctionID: auction.AuctionID, Accepted: true, CurrentPrice: 1200, Version: 3, Event: "bid.accepted", AuctionStatus: domain.AuctionStatusRunning}
	higher, err := svc.Place(ctx, PlaceBidInput{RequestID: "local-cache-higher", AuctionID: auction.AuctionID, BidderID: "u_1003", UserRole: domain.RoleBuyer, Price: 1200, ExpectedCurrentPrice: expectedCurrentPrice(1000)})
	if err != nil {
		t.Fatalf("higher place: %v", err)
	}
	if !higher.Accepted {
		t.Fatalf("higher stale-version bid should still be allowed to reach realtime when price is valid, got %+v", higher)
	}
	if realtime.placeBidCalls != 2 {
		t.Fatalf("valid higher bid should reach realtime lua, calls=%d", realtime.placeBidCalls)
	}
}

func TestBidServicePreRedisLocalStateRejectSkipsRedisAndLua(t *testing.T) {
	ctx := context.Background()
	rule := json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":10}`)
	realtime := &streamEnabledRealtime{}
	auctionRepo := repository.NewMemoryAuctionRepository()
	auction := domain.AuctionLot{
		AuctionID:     10005,
		SellerID:      "u_2001",
		AuctionType:   domain.AuctionTypeEnglish,
		StartPrice:    1000,
		ReservePrice:  1000,
		CapPrice:      3000,
		IncrementRule: rule,
		Status:        domain.AuctionStatusRunning,
		StartTime:     time.Now().Add(-time.Minute),
		EndTime:       time.Now().Add(time.Hour),
	}
	if err := auctionRepo.Create(ctx, &auction); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	svc := NewBidService(&trackingBidRepo{findErr: domain.ErrNotFound}, auctionRepo, realtime, nil, nil, appconfig.Default().Auction)
	svc.StoreBidRealtimeStateForTest(domain.AuctionState{
		AuctionID:      10005,
		LiveSessionID:  90005,
		Status:         domain.AuctionStatusRunning,
		StartPrice:     1000,
		CapPrice:       3000,
		IncrementRule:  rule,
		CurrentPrice:   2000,
		LeaderBidderID: "u_1000",
		EndTime:        time.Now().Add(time.Hour),
		Version:        9,
	}, time.Now().Add(bidRealtimeStateCacheTTL))

	result, err := svc.Place(ctx, PlaceBidInput{
		RequestID:            "pre-redis-local-reject",
		AuctionID:            10005,
		BidderID:             "u_1001",
		UserRole:             domain.RoleBuyer,
		Price:                1100,
		ExpectedCurrentPrice: expectedCurrentPrice(1000),
	})
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if result.Accepted || result.Reason != domain.BidRejectBelowMinIncrement || result.CurrentPrice != 2000 || result.LiveSessionID != 90005 {
		t.Fatalf("expected pre-redis local reject from cached state, got %+v", result)
	}
	if realtime.prereqCalls != 0 || realtime.getStateCalls != 0 || realtime.placeBidCalls != 0 {
		t.Fatalf("pre-redis local reject should skip prerequisites, redis state and lua, prereq=%d getState=%d place=%d", realtime.prereqCalls, realtime.getStateCalls, realtime.placeBidCalls)
	}
}

type trackingBidRepo struct {
	createCalls  int
	createErr    error
	existing     domain.BidRecord
	findErr      error
	batchCalls   int
	batchSizes   []int
	batchErr     error
	batchAllRows [][]domain.BidRecord
}

func (r *trackingBidRepo) Create(ctx context.Context, bid *domain.BidRecord) error {
	_ = ctx
	r.createCalls++
	if r.createErr != nil {
		return r.createErr
	}
	if bid.ID == 0 {
		bid.ID = uint64(r.createCalls)
	}
	return nil
}

func (r *trackingBidRepo) CreateIgnoreBatch(ctx context.Context, records []domain.BidRecord) error {
	_ = ctx
	r.batchCalls++
	r.batchSizes = append(r.batchSizes, len(records))
	dup := append([]domain.BidRecord(nil), records...)
	r.batchAllRows = append(r.batchAllRows, dup)
	if r.batchErr != nil {
		return r.batchErr
	}
	return nil
}

func (r *trackingBidRepo) FindByRequestID(ctx context.Context, requestID string) (domain.BidRecord, error) {
	_ = ctx
	_ = requestID
	if r.findErr != nil {
		return domain.BidRecord{}, r.findErr
	}
	if r.existing.RequestID == "" {
		return domain.BidRecord{}, domain.ErrNotFound
	}
	return r.existing, nil
}

func (r *trackingBidRepo) ListByAuction(ctx context.Context, auctionID uint64, limit int) ([]domain.BidRecord, error) {
	_ = ctx
	_ = auctionID
	_ = limit
	return nil, nil
}

func (r *trackingBidRepo) CountByAuction(ctx context.Context, auctionID uint64) (int, error) {
	_ = ctx
	_ = auctionID
	return 0, nil
}

func (r *trackingBidRepo) ListByLiveSession(ctx context.Context, sessionID uint64, sortBy string, limit, offset int) ([]domain.BidRecord, error) {
	_ = ctx
	_ = sessionID
	_ = sortBy
	_ = limit
	_ = offset
	return nil, nil
}

type streamEnabledRealtime struct {
	result        domain.BidResult
	state         domain.AuctionState
	stateOK       bool
	lastInput     domain.BidInput
	getStateCalls int
	prereqCalls   int
	placeBidCalls int
}

func (s *streamEnabledRealtime) StreamEnabled() bool { return true }

func (s *streamEnabledRealtime) InitAuction(ctx context.Context, auction domain.AuctionLot, minIncrement int64) (domain.AuctionState, error) {
	_ = ctx
	_ = minIncrement
	return domain.AuctionState{AuctionID: auction.AuctionID, Status: auction.Status, CurrentPrice: auction.StartPrice, StartTime: auction.StartTime, EndTime: auction.EndTime}, nil
}
func (s *streamEnabledRealtime) GetAuctionState(ctx context.Context, auctionID uint64) (domain.AuctionState, bool, error) {
	_ = ctx
	_ = auctionID
	s.getStateCalls++
	return s.state, s.stateOK, nil
}
func (s *streamEnabledRealtime) MarkEnrollment(ctx context.Context, auctionID uint64, userID string) error {
	_ = ctx
	_ = auctionID
	_ = userID
	return nil
}
func (s *streamEnabledRealtime) BidPrerequisites(ctx context.Context, auctionID uint64, userID string) (bool, bool, error) {
	_ = ctx
	_ = auctionID
	_ = userID
	s.prereqCalls++
	return true, true, nil
}
func (s *streamEnabledRealtime) PlaceBid(ctx context.Context, input domain.BidInput) (domain.BidResult, error) {
	_ = ctx
	s.placeBidCalls++
	s.lastInput = input
	s.result.RequestID = input.RequestID
	s.result.AuctionID = input.AuctionID
	s.result.BidderID = input.BidderID
	return s.result, nil
}
func (s *streamEnabledRealtime) Hammer(ctx context.Context, input domain.HammerInput) (domain.HammerResult, error) {
	_ = ctx
	_ = input
	return domain.HammerResult{}, nil
}
func (s *streamEnabledRealtime) TopN(ctx context.Context, auctionID uint64, limit int) ([]domain.RankingEntry, error) {
	_ = ctx
	_ = auctionID
	_ = limit
	return nil, nil
}

type countingAuctionRepo struct {
	repository.AuctionRepository
	findCalls int
}

func (r *countingAuctionRepo) FindByID(ctx context.Context, id uint64) (domain.AuctionLot, error) {
	r.findCalls++
	return r.AuctionRepository.FindByID(ctx, id)
}

type trackingPublisher struct{ broadcasts int }

func (p *trackingPublisher) Broadcast(auctionID uint64, env corews.Envelope) int {
	_ = auctionID
	_ = env
	p.broadcasts++
	return 1
}

type trackingBidEventProducer struct {
	events     []redisinfra.BidEvent
	err        error
	batchCalls int
	batchSizes []int
}

func (p *trackingBidEventProducer) PublishBidEvent(ctx context.Context, event redisinfra.BidEvent) error {
	_ = ctx
	if p.err != nil {
		return p.err
	}
	p.events = append(p.events, event)
	return nil
}

func (p *trackingBidEventProducer) PublishBidEvents(ctx context.Context, events []redisinfra.BidEvent) error {
	_ = ctx
	p.batchCalls++
	p.batchSizes = append(p.batchSizes, len(events))
	if p.err != nil {
		return p.err
	}
	p.events = append(p.events, events...)
	return nil
}

type trackingKafkaBidConsumer struct {
	events []redisinfra.BidEvent
	acks   []string
	idx    int
}

func (c *trackingKafkaBidConsumer) FetchBidEvent(ctx context.Context) (redisinfra.BidEvent, func(context.Context) error, error) {
	if c.idx >= len(c.events) {
		<-ctx.Done()
		return redisinfra.BidEvent{}, nil, ctx.Err()
	}
	event := c.events[c.idx]
	c.idx++
	ack := func(context.Context) error {
		c.acks = append(c.acks, event.StreamID)
		return nil
	}
	return event, ack, nil
}

type trackingBidLog struct {
	auctions       []uint64
	acks           []string
	dlqReasons     []string
	checkpoints    map[uint64]int64
	setCheckpoints map[uint64]int64
	replayEvents   []redisinfra.BidEvent
	replayComplete bool
	activeCalls    int64
}

func (l *trackingBidLog) Enabled() bool { return true }

func (l *trackingBidLog) ShardCount() int { return 1 }

func (l *trackingBidLog) ActiveAuctions(ctx context.Context) ([]uint64, error) {
	_ = ctx
	atomic.AddInt64(&l.activeCalls, 1)
	return l.auctions, nil
}

func (l *trackingBidLog) ActiveAuctionsOnShard(ctx context.Context, shardIdx int) ([]uint64, error) {
	_ = ctx
	_ = shardIdx
	atomic.AddInt64(&l.activeCalls, 1)
	return l.auctions, nil
}

func (l *trackingBidLog) ClaimStaleBidRecordEvents(ctx context.Context, auctionID uint64, consumer string, minIdle time.Duration, max int64) ([]redisinfra.BidEvent, error) {
	_ = ctx
	_ = auctionID
	_ = consumer
	_ = minIdle
	_ = max
	return nil, nil
}

func (l *trackingBidLog) ReadBidRecordGroup(ctx context.Context, auctionID uint64, consumer string, count int64, block time.Duration) ([]redisinfra.BidEvent, error) {
	_ = ctx
	_ = auctionID
	_ = consumer
	_ = count
	_ = block
	return nil, nil
}

func (l *trackingBidLog) AckBidRecord(ctx context.Context, auctionID uint64, ids ...string) error {
	_ = ctx
	_ = auctionID
	l.acks = append(l.acks, ids...)
	return nil
}

func (l *trackingBidLog) WriteBidRecordDLQ(ctx context.Context, event redisinfra.BidEvent, reason string) error {
	_ = ctx
	_ = event
	l.dlqReasons = append(l.dlqReasons, reason)
	return nil
}

func (l *trackingBidLog) ReconcileCheckpoint(ctx context.Context, auctionID uint64) (int64, error) {
	_ = ctx
	if l.checkpoints == nil {
		return 0, nil
	}
	return l.checkpoints[auctionID], nil
}

func (l *trackingBidLog) ReplayBidEvents(ctx context.Context, auctionID uint64, lastSeq int64, limit int64) ([]redisinfra.BidEvent, bool, error) {
	_ = ctx
	_ = auctionID
	_ = lastSeq
	_ = limit
	return l.replayEvents, l.replayComplete, nil
}

func (l *trackingBidLog) SetReconcileCheckpoint(ctx context.Context, auctionID uint64, seq int64) error {
	_ = ctx
	if l.setCheckpoints == nil {
		l.setCheckpoints = make(map[uint64]int64)
	}
	l.setCheckpoints[auctionID] = seq
	return nil
}

// claimCountingBidLog 在 trackingBidLog 上多记录 ClaimStaleBidRecordEvents 调用次数,
// 用于断言 XPENDING 巡检受 claim interval 节流.
type claimCountingBidLog struct {
	trackingBidLog
	claimCalls int
}

func (l *claimCountingBidLog) ClaimStaleBidRecordEvents(ctx context.Context, auctionID uint64, consumer string, minIdle time.Duration, max int64) ([]redisinfra.BidEvent, error) {
	_ = ctx
	_ = auctionID
	_ = consumer
	_ = minIdle
	_ = max
	l.claimCalls++
	return nil, nil
}

// trackingRankingLog 满足 bidRankingEventLog 接口,记录 ranking 调用 / DLQ / ack.
type trackingRankingLog struct {
	auctions     []uint64
	acks         []string
	dlqReasons   []string
	rankingCalls []rankingCall
	rankingErr   error
}

type rankingCall struct {
	auctionID uint64
	bidderID  string
	price     int64
	bidTSMS   int64
	seq       int64
}

func (l *trackingRankingLog) Enabled() bool   { return true }
func (l *trackingRankingLog) ShardCount() int { return 1 }
func (l *trackingRankingLog) ActiveAuctions(ctx context.Context) ([]uint64, error) {
	_ = ctx
	return l.auctions, nil
}
func (l *trackingRankingLog) ActiveAuctionsOnShard(ctx context.Context, shardIdx int) ([]uint64, error) {
	_ = ctx
	_ = shardIdx
	return l.auctions, nil
}
func (l *trackingRankingLog) ClaimStaleConsumerEvents(ctx context.Context, auctionID uint64, group, consumer string, minIdle time.Duration, max int64) ([]redisinfra.BidEvent, error) {
	_ = ctx
	_ = auctionID
	_ = group
	_ = consumer
	_ = minIdle
	_ = max
	return nil, nil
}
func (l *trackingRankingLog) ReadConsumerGroup(ctx context.Context, auctionID uint64, group, consumer string, count int64, block time.Duration) ([]redisinfra.BidEvent, error) {
	_ = ctx
	_ = auctionID
	_ = group
	_ = consumer
	_ = count
	_ = block
	return nil, nil
}
func (l *trackingRankingLog) AckConsumerGroup(ctx context.Context, auctionID uint64, group string, ids ...string) error {
	_ = ctx
	_ = auctionID
	_ = group
	l.acks = append(l.acks, ids...)
	return nil
}
func (l *trackingRankingLog) WriteBidRecordDLQ(ctx context.Context, event redisinfra.BidEvent, reason string) error {
	_ = ctx
	_ = event
	l.dlqReasons = append(l.dlqReasons, reason)
	return nil
}
func (l *trackingRankingLog) UpdateAcceptedRanking(ctx context.Context, auctionID uint64, bidderID string, price, bidTSMS, seq int64) error {
	_ = ctx
	l.rankingCalls = append(l.rankingCalls, rankingCall{auctionID: auctionID, bidderID: bidderID, price: price, bidTSMS: bidTSMS, seq: seq})
	return l.rankingErr
}

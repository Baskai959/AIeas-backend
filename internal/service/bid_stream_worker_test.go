package service

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	redisinfra "aieas_backend/internal/infra/redis"
	"aieas_backend/internal/repository"
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
	svc.storeBidRealtimeState(domain.AuctionState{
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
	svc.storeBidRealtimeState(domain.AuctionState{
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

func TestBidRecordWriterAckDlqAndPendingSemantics(t *testing.T) {
	ctx := context.Background()
	base := redisinfra.BidEvent{AuctionID: 10001, StreamID: "1-0", Seq: 1, RequestID: "req-1", EventType: "bid.accepted", BidderID: "u_1001", BidPrice: 1100, BidTSMS: 1700000000000, Source: "live_ws", RiskResult: domain.BidRiskAllow, Accepted: true, CreatedAtMS: 1700000000000}

	t.Run("OK ACK", func(t *testing.T) {
		repo := &trackingBidRepo{findErr: domain.ErrNotFound}
		log := &trackingBidLog{}
		writer := &BidRecordWriter{repo: repo, log: log, maxRetries: 5}
		writer.handleEvents(ctx, []redisinfra.BidEvent{base})
		if len(log.acks) != 1 || log.acks[0] != "1-0" || len(log.dlqReasons) != 0 {
			t.Fatalf("expected ack without dlq, log=%+v", log)
		}
	})

	t.Run("rejected skipped ACK", func(t *testing.T) {
		event := base
		event.StreamID = "reject-0"
		event.EventType = "bid.rejected"
		event.Accepted = false
		event.RiskResult = domain.BidRiskReject
		event.RejectReason = domain.BidRejectBelowMinIncrement
		repo := &trackingBidRepo{findErr: domain.ErrNotFound}
		log := &trackingBidLog{}
		writer := &BidRecordWriter{repo: repo, log: log, maxRetries: 5}
		writer.handleEvents(ctx, []redisinfra.BidEvent{event})
		if len(log.acks) != 1 || log.acks[0] != "reject-0" || len(log.dlqReasons) != 0 {
			t.Fatalf("expected rejected event to be skipped and acked, log=%+v", log)
		}
		if repo.createCalls != 0 {
			t.Fatalf("rejected event should not persist bid_record, createCalls=%d", repo.createCalls)
		}
	})

	t.Run("duplicate consistent ACK", func(t *testing.T) {
		repo := &trackingBidRepo{batchErr: errors.New("simulate batch fail"), createErr: domain.ErrConflict, existing: base.ToBidRecord()}
		log := &trackingBidLog{}
		writer := &BidRecordWriter{repo: repo, log: log, maxRetries: 5}
		writer.handleEvents(ctx, []redisinfra.BidEvent{base})
		if len(log.acks) != 1 || len(log.dlqReasons) != 0 {
			t.Fatalf("expected duplicate consistent ack, log=%+v", log)
		}
		if repo.createCalls != 0 {
			t.Fatalf("duplicate consistent event should not insert before idempotency check, got createCalls=%d", repo.createCalls)
		}
	})

	t.Run("duplicate conflict DLQ", func(t *testing.T) {
		conflict := base.ToBidRecord()
		conflict.BidPrice = 1200
		repo := &trackingBidRepo{batchErr: errors.New("simulate batch fail"), createErr: domain.ErrConflict, existing: conflict}
		log := &trackingBidLog{}
		writer := &BidRecordWriter{repo: repo, log: log, maxRetries: 5}
		writer.handleEvents(ctx, []redisinfra.BidEvent{base})
		if len(log.acks) != 1 || len(log.dlqReasons) != 1 || log.dlqReasons[0] != "DUPLICATE_CONFLICT" {
			t.Fatalf("expected duplicate conflict dlq+ack, log=%+v", log)
		}
		if repo.createCalls != 0 {
			t.Fatalf("duplicate conflict event should not insert before idempotency check, got createCalls=%d", repo.createCalls)
		}
	})

	t.Run("temporary failure stays pending", func(t *testing.T) {
		repo := &trackingBidRepo{batchErr: errors.New("simulate batch fail"), createErr: errors.New("temporary mysql outage")}
		log := &trackingBidLog{}
		writer := &BidRecordWriter{repo: repo, log: log, maxRetries: 5}
		writer.handleEvents(ctx, []redisinfra.BidEvent{base})
		if len(log.acks) != 0 || len(log.dlqReasons) != 0 {
			t.Fatalf("temporary failure should stay pending, log=%+v", log)
		}
	})

	t.Run("max retry DLQ", func(t *testing.T) {
		event := base
		event.StreamID = "5-0"
		event.Deliveries = 5
		repo := &trackingBidRepo{batchErr: errors.New("simulate batch fail"), createErr: errors.New("temporary mysql outage")}
		log := &trackingBidLog{}
		writer := &BidRecordWriter{repo: repo, log: log, maxRetries: 5}
		writer.handleEvents(ctx, []redisinfra.BidEvent{event})
		if len(log.acks) != 1 || len(log.dlqReasons) != 1 || log.dlqReasons[0] != "MAX_RETRIES" {
			t.Fatalf("expected max retry dlq+ack, log=%+v", log)
		}
	})
}

// TestBidRecordWriterBatchAck 验证批量消费多条 accepted 事件后,
// CreateIgnoreBatch 调 1 次,Stream ACK 与事件数等量.
func TestBidRecordWriterBatchAck(t *testing.T) {
	ctx := context.Background()
	events := []redisinfra.BidEvent{
		{AuctionID: 10001, StreamID: "1-0", Seq: 1, RequestID: "req-1", EventType: "bid.accepted", BidderID: "u_1001", BidPrice: 1100, BidTSMS: 1700000000000, Source: "live_ws", RiskResult: domain.BidRiskAllow, Accepted: true, CreatedAtMS: 1700000000000},
		{AuctionID: 10001, StreamID: "2-0", Seq: 2, RequestID: "req-2", EventType: "bid.accepted", BidderID: "u_1002", BidPrice: 1200, BidTSMS: 1700000001000, Source: "live_ws", RiskResult: domain.BidRiskAllow, Accepted: true, CreatedAtMS: 1700000001000},
		{AuctionID: 10001, StreamID: "3-0", Seq: 3, RequestID: "req-3", EventType: "bid.accepted", BidderID: "u_1003", BidPrice: 1300, BidTSMS: 1700000002000, Source: "live_ws", RiskResult: domain.BidRiskAllow, Accepted: true, CreatedAtMS: 1700000002000},
	}
	repo := &trackingBidRepo{findErr: domain.ErrNotFound}
	log := &trackingBidLog{}
	writer := &BidRecordWriter{repo: repo, log: log, maxRetries: 5}
	writer.handleEvents(ctx, events)
	if repo.batchCalls != 1 {
		t.Fatalf("expected 1 CreateIgnoreBatch call, got %d", repo.batchCalls)
	}
	if len(repo.batchSizes) != 1 || repo.batchSizes[0] != 3 {
		t.Fatalf("expected batch size 3, got %v", repo.batchSizes)
	}
	if len(log.acks) != 3 {
		t.Fatalf("expected 3 acks, got %v", log.acks)
	}
	if repo.createCalls != 0 {
		t.Fatalf("batch path should not call Create, got %d", repo.createCalls)
	}
}

// TestBidRecordWriterBatchFallbackPerEvent 验证批量写失败时降级逐条 (Create + 各自 ack/dlq).
func TestBidRecordWriterBatchFallbackPerEvent(t *testing.T) {
	ctx := context.Background()
	events := []redisinfra.BidEvent{
		{AuctionID: 10001, StreamID: "1-0", Seq: 1, RequestID: "req-1", EventType: "bid.accepted", BidderID: "u_1001", BidPrice: 1100, BidTSMS: 1700000000000, Source: "live_ws", RiskResult: domain.BidRiskAllow, Accepted: true, CreatedAtMS: 1700000000000},
		{AuctionID: 10001, StreamID: "2-0", Seq: 2, RequestID: "req-2", EventType: "bid.accepted", BidderID: "u_1002", BidPrice: 1200, BidTSMS: 1700000001000, Source: "live_ws", RiskResult: domain.BidRiskAllow, Accepted: true, CreatedAtMS: 1700000001000},
	}
	repo := &trackingBidRepo{batchErr: errors.New("batch insert failed"), findErr: domain.ErrNotFound}
	log := &trackingBidLog{}
	writer := &BidRecordWriter{repo: repo, log: log, maxRetries: 5}
	writer.handleEvents(ctx, events)
	if repo.batchCalls != 1 {
		t.Fatalf("expected 1 batch attempt, got %d", repo.batchCalls)
	}
	if repo.createCalls != 2 {
		t.Fatalf("fallback should call Create per event, got %d", repo.createCalls)
	}
	if len(log.acks) != 2 {
		t.Fatalf("fallback should ack per success, got %v", log.acks)
	}
}

// TestKafkaBidRecordWriterBatchFetchAndAck 验证 Kafka 写库 worker 会在短窗口内攒批,
// CreateIgnoreBatch 成功后再提交这批事件的 ACK。
func TestKafkaBidRecordWriterBatchFetchAndAck(t *testing.T) {
	ctx := context.Background()
	events := []redisinfra.BidEvent{
		{AuctionID: 10001, StreamID: "1-0", Seq: 1, RequestID: "kreq-1", EventType: "bid.accepted", BidderID: "u_1001", BidPrice: 1100, BidTSMS: 1700000000000, Source: "live_ws", RiskResult: domain.BidRiskAllow, Accepted: true, CreatedAtMS: 1700000000000},
		{AuctionID: 10001, StreamID: "2-0", Seq: 2, RequestID: "kreq-2", EventType: "bid.accepted", BidderID: "u_1002", BidPrice: 1200, BidTSMS: 1700000001000, Source: "live_ws", RiskResult: domain.BidRiskAllow, Accepted: true, CreatedAtMS: 1700000001000},
		{AuctionID: 10001, StreamID: "3-0", Seq: 3, RequestID: "kreq-3", EventType: "bid.accepted", BidderID: "u_1003", BidPrice: 1300, BidTSMS: 1700000002000, Source: "live_ws", RiskResult: domain.BidRiskAllow, Accepted: true, CreatedAtMS: 1700000002000},
	}
	repo := &trackingBidRepo{findErr: domain.ErrNotFound}
	consumer := &trackingKafkaBidConsumer{events: events}
	writer := &KafkaBidRecordWriter{repo: repo, consumer: consumer, retryBackoff: time.Millisecond}

	batch, ok := writer.fetchBatch(ctx)
	if !ok {
		t.Fatal("fetch batch returned not ok")
	}
	if len(batch) != len(events) {
		t.Fatalf("batch size got=%d want=%d", len(batch), len(events))
	}
	writer.flushBatch(ctx, batch)

	if repo.batchCalls != 1 {
		t.Fatalf("expected 1 CreateIgnoreBatch call, got %d", repo.batchCalls)
	}
	if len(repo.batchSizes) != 1 || repo.batchSizes[0] != len(events) {
		t.Fatalf("expected kafka batch size %d, got %v", len(events), repo.batchSizes)
	}
	if len(consumer.acks) != len(events) {
		t.Fatalf("expected %d kafka acks, got %v", len(events), consumer.acks)
	}
	if repo.createCalls != 0 {
		t.Fatalf("kafka batch path should not call Create, got %d", repo.createCalls)
	}
}

// TestKafkaBidRecordWriterBatchFallbackPerEvent 验证 Kafka 批量写失败时仍降级到
// 逐条幂等写库,成功事件逐条 ACK。
func TestKafkaBidRecordWriterBatchFallbackPerEvent(t *testing.T) {
	ctx := context.Background()
	events := []redisinfra.BidEvent{
		{AuctionID: 10001, StreamID: "1-0", Seq: 1, RequestID: "kreq-fallback-1", EventType: "bid.accepted", BidderID: "u_1001", BidPrice: 1100, BidTSMS: 1700000000000, Source: "live_ws", RiskResult: domain.BidRiskAllow, Accepted: true, CreatedAtMS: 1700000000000},
		{AuctionID: 10001, StreamID: "2-0", Seq: 2, RequestID: "kreq-fallback-2", EventType: "bid.accepted", BidderID: "u_1002", BidPrice: 1200, BidTSMS: 1700000001000, Source: "live_ws", RiskResult: domain.BidRiskAllow, Accepted: true, CreatedAtMS: 1700000001000},
	}
	repo := &trackingBidRepo{batchErr: errors.New("batch insert failed"), findErr: domain.ErrNotFound}
	acks := make([]string, 0, len(events))
	writer := &KafkaBidRecordWriter{repo: repo, retryBackoff: time.Millisecond}

	writer.flushBatch(ctx, kafkaPendingForTest(ctx, events, &acks))

	if repo.batchCalls != 1 {
		t.Fatalf("expected 1 batch attempt, got %d", repo.batchCalls)
	}
	if repo.createCalls != len(events) {
		t.Fatalf("fallback should call Create per event, got %d", repo.createCalls)
	}
	if len(acks) != len(events) {
		t.Fatalf("fallback should ack per success, got %v", acks)
	}
}

// TestRedisBidEventKafkaBridgeBatchPublishesAndAcks 验证 Redis Stream -> Kafka bridge
// 会批量发布 accepted 事件,并对成功发布的同拍卖 stream id 批量 ACK。
func TestRedisBidEventKafkaBridgeBatchPublishesAndAcks(t *testing.T) {
	ctx := context.Background()
	accepted1 := redisinfra.BidEvent{AuctionID: 10001, StreamID: "1-0", Seq: 1, RequestID: "req-1", EventType: "bid.accepted", BidderID: "u_1001", BidPrice: 1100, BidTSMS: 1700000000000, Accepted: true}
	accepted2 := redisinfra.BidEvent{AuctionID: 10001, StreamID: "2-0", Seq: 2, RequestID: "req-2", EventType: "bid.accepted", BidderID: "u_1002", BidPrice: 1200, BidTSMS: 1700000001000, Accepted: true}
	rejected := redisinfra.BidEvent{AuctionID: 10001, StreamID: "rej-0", Seq: 3, RequestID: "req-rej", EventType: "bid.rejected", BidderID: "u_1003", BidPrice: 1200, BidTSMS: 1700000002000, Accepted: false}
	log := &trackingRankingLog{}
	producer := &trackingBidEventProducer{}
	bridge := &RedisBidEventKafkaBridge{log: log, producer: producer, group: redisinfra.BidKafkaBridgeConsumerGroup, maxRetries: 5}

	bridge.handleEvents(ctx, []redisinfra.BidEvent{accepted1, rejected, accepted2})

	if producer.batchCalls != 1 {
		t.Fatalf("expected 1 kafka batch publish, got %d", producer.batchCalls)
	}
	if len(producer.batchSizes) != 1 || producer.batchSizes[0] != 2 {
		t.Fatalf("expected batch size 2 accepted events, got %v", producer.batchSizes)
	}
	if len(producer.events) != 2 {
		t.Fatalf("expected 2 published accepted events, got %d", len(producer.events))
	}
	if len(log.acks) != 3 {
		t.Fatalf("expected accepted batch + rejected skip acks, got %v", log.acks)
	}
}

func TestRedisBidEventKafkaBridgeDefaultsToFastBatchPoll(t *testing.T) {
	bridge := NewRedisBidEventKafkaBridge(nil, &trackingBidEventProducer{}, "", "")
	if bridge.interval != bidRecordBatchMaxLatency {
		t.Fatalf("bridge interval got=%s want=%s", bridge.interval, bidRecordBatchMaxLatency)
	}
}

// TestBidRecordWriterClaimStaleNotEveryRound 验证 ClaimStale 受 claim interval 约束:
// 5 轮 runOnceAll 内, ClaimStale 调用 ≤ 1 次.
func TestBidRecordWriterClaimStaleNotEveryRound(t *testing.T) {
	ctx := context.Background()
	repo := &trackingBidRepo{findErr: domain.ErrNotFound}
	log := &claimCountingBidLog{trackingBidLog: trackingBidLog{auctions: []uint64{10001}}}
	writer := &BidRecordWriter{repo: repo, log: log, maxRetries: 5, claimIdle: 30 * time.Second, claimInterval: 15 * time.Second}
	state := newClaimState()
	for i := 0; i < 5; i++ {
		writer.runOnceAll(ctx, "c1", state)
	}
	if log.claimCalls > 1 {
		t.Fatalf("expected ClaimStale ≤ 1 call within claim interval, got %d", log.claimCalls)
	}
}

// TestBidRankingWorkerAcceptedUpdatesAndRejectedSkips 验证 ranking worker 对
// accepted 调 UpdateAcceptedRanking 并 ack;rejected 直接 ack 不调 ranking 更新.
func TestBidRankingWorkerAcceptedUpdatesAndRejectedSkips(t *testing.T) {
	ctx := context.Background()
	accepted := redisinfra.BidEvent{AuctionID: 10001, StreamID: "1-0", Seq: 1, RequestID: "req-1", EventType: "bid.accepted", BidderID: "u_1001", BidPrice: 1100, BidTSMS: 1700000000000, Accepted: true}
	rejected := redisinfra.BidEvent{AuctionID: 10001, StreamID: "rej-0", Seq: 2, RequestID: "req-rej", EventType: "bid.rejected", BidderID: "u_1002", BidPrice: 1100, BidTSMS: 1700000001000, Accepted: false}
	log := &trackingRankingLog{}
	worker := &BidRankingWorker{log: log, maxRetries: 5}
	worker.handleEvents(ctx, []redisinfra.BidEvent{accepted, rejected})
	if len(log.rankingCalls) != 1 {
		t.Fatalf("expected 1 ranking update for accepted, got %d", len(log.rankingCalls))
	}
	got := log.rankingCalls[0]
	if got.auctionID != accepted.AuctionID || got.bidderID != accepted.BidderID || got.price != accepted.BidPrice || got.bidTSMS != accepted.BidTSMS || got.seq != accepted.Seq {
		t.Fatalf("ranking call mismatch: %+v", got)
	}
	if len(log.acks) != 2 {
		t.Fatalf("both events must be acked (rejected skip-ack + accepted ack), got %v", log.acks)
	}
}

// TestBidRankingWorkerFailureKeepsPendingAndDLQOnMaxRetries 验证 ranking 更新失败
// 不 ACK (留 PEL),且 deliveries 超阈值时写 DLQ + ack.
func TestBidRankingWorkerFailureKeepsPendingAndDLQOnMaxRetries(t *testing.T) {
	ctx := context.Background()
	t.Run("失败留 PEL 不 ACK", func(t *testing.T) {
		event := redisinfra.BidEvent{AuctionID: 10001, StreamID: "1-0", Seq: 1, RequestID: "req-1", EventType: "bid.accepted", BidderID: "u_1001", BidPrice: 1100, BidTSMS: 1700000000000, Accepted: true, Deliveries: 1}
		log := &trackingRankingLog{rankingErr: errors.New("redis outage")}
		worker := &BidRankingWorker{log: log, maxRetries: 5}
		worker.handleEvents(ctx, []redisinfra.BidEvent{event})
		if len(log.acks) != 0 {
			t.Fatalf("ranking failure must not ACK, got %v", log.acks)
		}
		if len(log.dlqReasons) != 0 {
			t.Fatalf("ranking failure under retries must not DLQ, got %v", log.dlqReasons)
		}
	})
	t.Run("超阈值 DLQ + ACK", func(t *testing.T) {
		event := redisinfra.BidEvent{AuctionID: 10001, StreamID: "5-0", Seq: 5, RequestID: "req-5", EventType: "bid.accepted", BidderID: "u_1001", BidPrice: 1100, BidTSMS: 1700000000000, Accepted: true, Deliveries: 5}
		log := &trackingRankingLog{rankingErr: errors.New("redis outage")}
		worker := &BidRankingWorker{log: log, maxRetries: 5}
		worker.handleEvents(ctx, []redisinfra.BidEvent{event})
		if len(log.acks) != 1 || log.acks[0] != "5-0" {
			t.Fatalf("expected dlq+ack for max retries, acks=%v", log.acks)
		}
		if len(log.dlqReasons) != 1 || log.dlqReasons[0] != "RANKING_MAX_RETRIES" {
			t.Fatalf("expected RANKING_MAX_RETRIES DLQ, got %v", log.dlqReasons)
		}
	})
}

func TestBidRecordReconcilerCheckpointGapDLQAndBackfill(t *testing.T) {
	ctx := context.Background()
	events := []redisinfra.BidEvent{{AuctionID: 10001, StreamID: "2-0", Seq: 2, RequestID: "req-2", EventType: "bid.accepted", BidderID: "u_1001", BidPrice: 1100, BidTSMS: 1700000000000, Source: "live_ws", RiskResult: domain.BidRiskAllow, Accepted: true, CreatedAtMS: 1700000000000}}
	repo := &trackingBidRepo{findErr: domain.ErrNotFound}
	log := &trackingBidLog{auctions: []uint64{10001}, checkpoints: map[uint64]int64{10001: 1}, replayEvents: events, replayComplete: true}
	reconciler := &BidRecordReconciler{repo: repo, log: log}

	if err := reconciler.RunOnce(ctx); err != nil {
		t.Fatalf("run once: %v", err)
	}
	if repo.createCalls != 1 || log.setCheckpoints[10001] != 2 || len(log.dlqReasons) != 0 {
		t.Fatalf("expected backfill and checkpoint advance, createCalls=%d log=%+v", repo.createCalls, log)
	}

	gapLog := &trackingBidLog{auctions: []uint64{10001}, checkpoints: map[uint64]int64{10001: 2}, replayComplete: false}
	gapReconciler := &BidRecordReconciler{repo: &trackingBidRepo{}, log: gapLog}
	if err := gapReconciler.RunOnce(ctx); err != nil {
		t.Fatalf("gap run once: %v", err)
	}
	if len(gapLog.dlqReasons) != 1 || gapLog.dlqReasons[0] != "RECONCILE_GAP" {
		t.Fatalf("expected reconcile gap dlq, log=%+v", gapLog)
	}
}

func TestBidRecordReconcilerStartRunsPeriodically(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := &trackingBidLog{auctions: []uint64{10001}, checkpoints: map[uint64]int64{10001: 0}, replayComplete: true}
	reconciler := &BidRecordReconciler{repo: &trackingBidRepo{}, log: log}
	reconciler.Start(ctx, time.Hour)

	deadline := time.After(time.Second)
	for {
		if log.activeCalls > 0 {
			return
		}
		select {
		case <-deadline:
			t.Fatal("expected reconciler loop to run immediately when started")
		case <-time.After(10 * time.Millisecond):
		}
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

func kafkaPendingForTest(ctx context.Context, events []redisinfra.BidEvent, acks *[]string) []kafkaBidRecordPending {
	pending := make([]kafkaBidRecordPending, 0, len(events))
	for _, event := range events {
		event := event
		ack := func(context.Context) error {
			*acks = append(*acks, event.StreamID)
			return nil
		}
		pending = append(pending, kafkaBidRecordPending{event: event, ack: ack, ctx: ctx})
	}
	return pending
}

type trackingBidLog struct {
	auctions       []uint64
	acks           []string
	dlqReasons     []string
	checkpoints    map[uint64]int64
	setCheckpoints map[uint64]int64
	replayEvents   []redisinfra.BidEvent
	replayComplete bool
	activeCalls    int
}

func (l *trackingBidLog) Enabled() bool { return true }

func (l *trackingBidLog) ShardCount() int { return 1 }

func (l *trackingBidLog) ActiveAuctions(ctx context.Context) ([]uint64, error) {
	_ = ctx
	l.activeCalls++
	return l.auctions, nil
}

func (l *trackingBidLog) ActiveAuctionsOnShard(ctx context.Context, shardIdx int) ([]uint64, error) {
	_ = ctx
	_ = shardIdx
	l.activeCalls++
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

package service

import (
	"context"
	"errors"
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
	auction := domain.AuctionLot{AuctionID: 10001, SellerID: "u_2001", AuctionType: domain.AuctionTypeEnglish, StartPrice: 1000, ReservePrice: 1000, Status: domain.AuctionStatusRunning, StartTime: time.Now().Add(-time.Minute), EndTime: time.Now().Add(time.Hour)}
	if err := auctionRepo.Create(ctx, &auction); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	bids := &trackingBidRepo{findErr: domain.ErrNotFound}
	realtime := &streamEnabledRealtime{result: domain.BidResult{RequestID: "req-1", AuctionID: auction.AuctionID, BidderID: "u_1001", Price: 1100, Accepted: true, CurrentPrice: 1100, Seq: 3, StreamID: "3-0", Event: "bid.accepted"}}
	publisher := &trackingPublisher{}
	svc := NewBidService(bids, auctionRepo, realtime, nil, publisher, appconfig.Default().Auction)

	result, err := svc.Place(ctx, PlaceBidInput{RequestID: "req-1", AuctionID: auction.AuctionID, BidderID: "u_1001", UserRole: domain.RoleBuyer, Price: 1100})
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

	t.Run("duplicate consistent ACK", func(t *testing.T) {
		repo := &trackingBidRepo{createErr: domain.ErrConflict, existing: base.ToBidRecord()}
		log := &trackingBidLog{}
		writer := &BidRecordWriter{repo: repo, log: log, maxRetries: 5}
		writer.handleEvents(ctx, []redisinfra.BidEvent{base})
		if len(log.acks) != 1 || len(log.dlqReasons) != 0 {
			t.Fatalf("expected duplicate consistent ack, log=%+v", log)
		}
	})

	t.Run("duplicate conflict DLQ", func(t *testing.T) {
		conflict := base.ToBidRecord()
		conflict.BidPrice = 1200
		repo := &trackingBidRepo{createErr: domain.ErrConflict, existing: conflict}
		log := &trackingBidLog{}
		writer := &BidRecordWriter{repo: repo, log: log, maxRetries: 5}
		writer.handleEvents(ctx, []redisinfra.BidEvent{base})
		if len(log.acks) != 1 || len(log.dlqReasons) != 1 || log.dlqReasons[0] != "DUPLICATE_CONFLICT" {
			t.Fatalf("expected duplicate conflict dlq+ack, log=%+v", log)
		}
	})

	t.Run("temporary failure stays pending", func(t *testing.T) {
		repo := &trackingBidRepo{createErr: errors.New("temporary mysql outage")}
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
		repo := &trackingBidRepo{createErr: errors.New("temporary mysql outage")}
		log := &trackingBidLog{}
		writer := &BidRecordWriter{repo: repo, log: log, maxRetries: 5}
		writer.handleEvents(ctx, []redisinfra.BidEvent{event})
		if len(log.acks) != 1 || len(log.dlqReasons) != 1 || log.dlqReasons[0] != "MAX_RETRIES" {
			t.Fatalf("expected max retry dlq+ack, log=%+v", log)
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
	createCalls int
	createErr   error
	existing    domain.BidRecord
	findErr     error
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

type streamEnabledRealtime struct{ result domain.BidResult }

func (s *streamEnabledRealtime) StreamEnabled() bool { return true }

func (s *streamEnabledRealtime) InitAuction(ctx context.Context, auction domain.AuctionLot, minIncrement int64) (domain.AuctionState, error) {
	_ = ctx
	_ = minIncrement
	return domain.AuctionState{AuctionID: auction.AuctionID, Status: auction.Status, CurrentPrice: auction.StartPrice, StartTime: auction.StartTime, EndTime: auction.EndTime}, nil
}
func (s *streamEnabledRealtime) GetAuctionState(ctx context.Context, auctionID uint64) (domain.AuctionState, bool, error) {
	_ = ctx
	_ = auctionID
	return domain.AuctionState{}, false, nil
}
func (s *streamEnabledRealtime) MarkEnrollment(ctx context.Context, auctionID uint64, userID string) error {
	_ = ctx
	_ = auctionID
	_ = userID
	return nil
}
func (s *streamEnabledRealtime) PlaceBid(ctx context.Context, input domain.BidInput) (domain.BidResult, error) {
	_ = ctx
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
func (s *streamEnabledRealtime) IsBlacklisted(ctx context.Context, userID string) (bool, error) {
	_ = ctx
	_ = userID
	return false, nil
}
func (s *streamEnabledRealtime) SetBlacklisted(ctx context.Context, userID string, blacklisted bool) error {
	_ = ctx
	_ = userID
	_ = blacklisted
	return nil
}

type trackingPublisher struct{ broadcasts int }

func (p *trackingPublisher) Broadcast(auctionID uint64, env corews.Envelope) int {
	_ = auctionID
	_ = env
	p.broadcasts++
	return 1
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

func (l *trackingBidLog) ActiveAuctions(ctx context.Context) ([]uint64, error) {
	_ = ctx
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

package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"aieas_backend/internal/domain"
	redisinfra "aieas_backend/internal/infra/redis"
)

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

func TestBidRankingWorkerAcceptedUpdatesAndRejectedSkips(t *testing.T) {
	ctx := context.Background()
	accepted := redisinfra.BidEvent{AuctionID: 10001, StreamID: "1-0", Seq: 1, RequestID: "req-1", EventType: "bid.accepted", BidderID: "u_1001", BidPrice: 1100, BidTSMS: 1700000000000, Accepted: true}
	rejected := redisinfra.BidEvent{AuctionID: 10001, StreamID: "rej-0", Seq: 2, RequestID: "req-rej", EventType: "bid.rejected", BidderID: "u_1002", BidPrice: 1100, BidTSMS: 1700000001000, Accepted: false}
	log := &trackingRankingLog{}
	worker := &BidRankingWorker{log: log, maxRetries: 5}
	var updated []uint64
	worker.SetRankingUpdatedCallback(func(auctionID uint64) {
		updated = append(updated, auctionID)
	})
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
	if len(updated) != 1 || updated[0] != accepted.AuctionID {
		t.Fatalf("expected ranking update callback for accepted event, got %v", updated)
	}
}

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
		if atomic.LoadInt64(&log.activeCalls) > 0 {
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
	activeCalls    int64
}

func (l *trackingBidLog) Enabled() bool   { return true }
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

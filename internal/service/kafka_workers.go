package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/observability/metrics"
	"aieas_backend/internal/infra/observability/tracing"
	redisinfra "aieas_backend/internal/infra/redis"
	"aieas_backend/internal/repository"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type BidEventKafkaProducer interface {
	PublishBidEvent(ctx context.Context, event redisinfra.BidEvent) error
}

type BidEventKafkaBatchProducer interface {
	PublishBidEvents(ctx context.Context, events []redisinfra.BidEvent) error
}

type BidEventKafkaConsumer interface {
	FetchBidEvent(ctx context.Context) (redisinfra.BidEvent, func(context.Context) error, error)
}

type SettlementEventPublisher interface {
	PublishAuctionClosed(ctx context.Context, auction domain.AuctionLot, result domain.HammerResult, order *domain.OrderDeal) error
	PublishOrderCreated(ctx context.Context, order domain.OrderDeal) error
}

type RedisBidEventKafkaBridge struct {
	log           bidKafkaBridgeLog
	producer      BidEventKafkaProducer
	group         string
	consumer      string
	maxRetries    int64
	claimIdle     time.Duration
	interval      time.Duration
	claimInterval time.Duration
	metrics       *metrics.Registry
}

type bidKafkaBridgeLog interface {
	Enabled() bool
	ShardCount() int
	ActiveAuctions(ctx context.Context) ([]uint64, error)
	ActiveAuctionsOnShard(ctx context.Context, shardIdx int) ([]uint64, error)
	ClaimStaleConsumerEvents(ctx context.Context, auctionID uint64, group, consumer string, minIdle time.Duration, max int64) ([]redisinfra.BidEvent, error)
	ReadConsumerGroup(ctx context.Context, auctionID uint64, group, consumer string, count int64, block time.Duration) ([]redisinfra.BidEvent, error)
	AckConsumerGroup(ctx context.Context, auctionID uint64, group string, ids ...string) error
	WriteBidRecordDLQ(ctx context.Context, event redisinfra.BidEvent, reason string) error
}

func NewRedisBidEventKafkaBridge(log *redisinfra.EventLog, producer BidEventKafkaProducer, group, consumer string) *RedisBidEventKafkaBridge {
	if group == "" {
		group = redisinfra.BidKafkaBridgeConsumerGroup
	}
	if consumer == "" {
		consumer = fmt.Sprintf("bid-kafka-%d", time.Now().UTC().UnixNano())
	}
	return &RedisBidEventKafkaBridge{
		log:           log,
		producer:      producer,
		group:         group,
		consumer:      consumer,
		maxRetries:    5,
		claimIdle:     30 * time.Second,
		interval:      bidRecordBatchMaxLatency,
		claimInterval: defaultClaimInterval,
	}
}

// SetMetrics 注入压测所需的 Redis Stream -> Kafka worker 指标。nil 安全。
func (b *RedisBidEventKafkaBridge) SetMetrics(reg *metrics.Registry) {
	if b == nil {
		return
	}
	b.metrics = reg
}

func (b *RedisBidEventKafkaBridge) Start(ctx context.Context) {
	if b == nil || b.log == nil || b.producer == nil || !b.log.Enabled() {
		return
	}
	shardCount := b.log.ShardCount()
	if shardCount <= 1 {
		go b.loopAllShards(ctx)
		return
	}
	for shardIdx := 0; shardIdx < shardCount; shardIdx++ {
		idx := shardIdx
		go b.loopShard(ctx, idx)
	}
}

func (b *RedisBidEventKafkaBridge) loopAllShards(ctx context.Context) {
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()
	state := newClaimState()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.runOnceAll(ctx, state)
		}
	}
}

func (b *RedisBidEventKafkaBridge) loopShard(ctx context.Context, shardIdx int) {
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()
	state := newClaimState()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.runOnceShard(ctx, shardIdx, state)
		}
	}
}

func (b *RedisBidEventKafkaBridge) consumerForShard(shardIdx int) string {
	return fmt.Sprintf("%s-s%d", b.consumer, shardIdx)
}

func (b *RedisBidEventKafkaBridge) runOnceAll(ctx context.Context, state *claimState) {
	auctions, err := b.log.ActiveAuctions(ctx)
	if err != nil {
		b.observeWorker("poll_error")
		return
	}
	b.processAuctions(ctx, auctions, b.consumer, state)
}

func (b *RedisBidEventKafkaBridge) runOnceShard(ctx context.Context, shardIdx int, state *claimState) {
	auctions, err := b.log.ActiveAuctionsOnShard(ctx, shardIdx)
	if err != nil {
		b.observeWorker("poll_error")
		return
	}
	b.processAuctions(ctx, auctions, b.consumerForShard(shardIdx), state)
}

func (b *RedisBidEventKafkaBridge) processAuctions(ctx context.Context, auctions []uint64, consumer string, state *claimState) {
	doClaim := state.shouldClaim(time.Now(), b.claimInterval)
	for _, auctionID := range auctions {
		if doClaim {
			claimed, err := b.log.ClaimStaleConsumerEvents(ctx, auctionID, b.group, consumer, b.claimIdle, bidRecordBatchMaxN)
			if err == nil {
				b.handleEvents(ctx, claimed)
			} else {
				b.observeWorker("poll_error")
			}
		}
		events, err := b.log.ReadConsumerGroup(ctx, auctionID, b.group, consumer, bidRecordBatchMaxN, 10*time.Millisecond)
		if err == nil {
			b.handleEvents(ctx, events)
		} else {
			b.observeWorker("poll_error")
		}
	}
	if doClaim {
		state.markClaimed(time.Now())
	}
}

func (b *RedisBidEventKafkaBridge) handleEvents(ctx context.Context, events []redisinfra.BidEvent) {
	if len(events) == 0 {
		return
	}
	batch := make([]kafkaBridgePending, 0, len(events))
	for _, event := range events {
		if pending, ok := b.preFilter(ctx, event); ok {
			batch = append(batch, pending)
		}
	}
	if len(batch) > 0 {
		b.publishBatch(ctx, batch)
	}
}

type kafkaBridgePending struct {
	event redisinfra.BidEvent
	ctx   context.Context
}

func (b *RedisBidEventKafkaBridge) preFilter(parentCtx context.Context, event redisinfra.BidEvent) (kafkaBridgePending, bool) {
	ctx := tracing.ExtractMap(parentCtx, event.TraceCarrier())
	if event.EventType != "bid.accepted" || !event.Accepted {
		_ = b.log.AckConsumerGroup(ctx, event.AuctionID, b.group, event.StreamID)
		b.observeWorker("skip")
		return kafkaBridgePending{}, false
	}
	if event.Deliveries >= b.maxRetries {
		_ = b.log.WriteBidRecordDLQ(ctx, event, "KAFKA_PUBLISH_MAX_RETRIES")
		_ = b.log.AckConsumerGroup(ctx, event.AuctionID, b.group, event.StreamID)
		b.observeDLQ("KAFKA_PUBLISH_MAX_RETRIES")
		b.observeWorker("dlq")
		return kafkaBridgePending{}, false
	}
	return kafkaBridgePending{event: event, ctx: ctx}, true
}

func (b *RedisBidEventKafkaBridge) publishBatch(parentCtx context.Context, batch []kafkaBridgePending) {
	if len(batch) == 0 {
		return
	}
	events := make([]redisinfra.BidEvent, 0, len(batch))
	for _, p := range batch {
		events = append(events, p.event)
	}
	ctx, span := tracing.StartSpan(parentCtx, "bid_event.kafka_publish_batch",
		attribute.Int("events.count", len(events)),
	)
	err := b.publishBidEvents(ctx, events)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(attribute.String("kafka_publish.result", "batch_temporary_failure"))
		span.End()
		for _, p := range batch {
			b.handleEvent(p.ctx, p.event)
		}
		return
	}
	b.ackPublishedBatch(ctx, batch)
	span.SetAttributes(attribute.String("kafka_publish.result", "ok"))
	span.End()
}

func (b *RedisBidEventKafkaBridge) publishBidEvents(ctx context.Context, events []redisinfra.BidEvent) error {
	if len(events) == 0 {
		return nil
	}
	if batchProducer, ok := b.producer.(BidEventKafkaBatchProducer); ok {
		return batchProducer.PublishBidEvents(ctx, events)
	}
	for _, event := range events {
		if err := b.producer.PublishBidEvent(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (b *RedisBidEventKafkaBridge) ackPublishedBatch(ctx context.Context, batch []kafkaBridgePending) {
	groups := make(map[uint64][]string)
	for _, p := range batch {
		groups[p.event.AuctionID] = append(groups[p.event.AuctionID], p.event.StreamID)
		b.observeWorker("ok")
	}
	for auctionID, ids := range groups {
		_ = b.log.AckConsumerGroup(ctx, auctionID, b.group, ids...)
	}
}

func (b *RedisBidEventKafkaBridge) handleEvent(parentCtx context.Context, event redisinfra.BidEvent) {
	ctx := tracing.ExtractMap(parentCtx, event.TraceCarrier())
	ctx, span := tracing.StartSpan(ctx, "bid_event.kafka_publish",
		attribute.Int64("auction.id", int64(event.AuctionID)),
		attribute.String("event.type", event.EventType),
		attribute.Int64("event.seq", event.Seq),
		attribute.String("event.stream_id", event.StreamID),
	)
	defer span.End()

	if event.EventType != "bid.accepted" || !event.Accepted {
		_ = b.log.AckConsumerGroup(ctx, event.AuctionID, b.group, event.StreamID)
		span.SetAttributes(attribute.String("kafka_publish.result", "skip"))
		b.observeWorker("skip")
		return
	}
	if event.Deliveries >= b.maxRetries {
		_ = b.log.WriteBidRecordDLQ(ctx, event, "KAFKA_PUBLISH_MAX_RETRIES")
		_ = b.log.AckConsumerGroup(ctx, event.AuctionID, b.group, event.StreamID)
		span.SetAttributes(attribute.String("kafka_publish.result", "dlq_max_retries"))
		b.observeDLQ("KAFKA_PUBLISH_MAX_RETRIES")
		b.observeWorker("dlq")
		return
	}
	if err := b.producer.PublishBidEvent(ctx, event); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(attribute.String("kafka_publish.result", "temporary_failure"))
		b.observeWorker("error")
		return
	}
	_ = b.log.AckConsumerGroup(ctx, event.AuctionID, b.group, event.StreamID)
	span.SetAttributes(attribute.String("kafka_publish.result", "ok"))
	b.observeWorker("ok")
}

func (b *RedisBidEventKafkaBridge) observeWorker(result string) {
	if b != nil && b.metrics != nil {
		b.metrics.IncWorkerTask("bid_kafka_bridge", result)
	}
}

func (b *RedisBidEventKafkaBridge) observeDLQ(reason string) {
	if b != nil && b.metrics != nil {
		b.metrics.IncWorkerBidRecordDLQ(reason)
	}
}

type KafkaBidRecordWriter struct {
	repo         repository.BidRepository
	consumer     BidEventKafkaConsumer
	retryBackoff time.Duration
	metrics      *metrics.Registry
}

func NewKafkaBidRecordWriter(repo repository.BidRepository, consumer BidEventKafkaConsumer) *KafkaBidRecordWriter {
	return &KafkaBidRecordWriter{repo: repo, consumer: consumer, retryBackoff: time.Second}
}

// SetMetrics 注入压测所需的 Kafka -> MySQL worker 指标。nil 安全。
func (w *KafkaBidRecordWriter) SetMetrics(reg *metrics.Registry) {
	if w == nil {
		return
	}
	w.metrics = reg
}

func (w *KafkaBidRecordWriter) Start(ctx context.Context) {
	if w == nil || w.repo == nil || w.consumer == nil {
		return
	}
	go w.loop(ctx)
}

func (w *KafkaBidRecordWriter) loop(ctx context.Context) {
	for {
		batch, ok := w.fetchBatch(ctx)
		if !ok {
			return
		}
		w.flushBatch(ctx, batch)
	}
}

type kafkaBidRecordPending struct {
	event redisinfra.BidEvent
	ack   func(context.Context) error
	ctx   context.Context
}

func (w *KafkaBidRecordWriter) fetchBatch(ctx context.Context) ([]kafkaBidRecordPending, bool) {
	batch := make([]kafkaBidRecordPending, 0, bidRecordBatchMaxN)
	var deadline time.Time
	for len(batch) < bidRecordBatchMaxN {
		fetchCtx := ctx
		cancel := func() {}
		if len(batch) > 0 {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				break
			}
			fetchCtx, cancel = context.WithTimeout(ctx, remaining)
		}
		event, ack, err := w.consumer.FetchBidEvent(fetchCtx)
		fetchCtxErr := fetchCtx.Err()
		cancel()
		if err != nil {
			if ack != nil {
				_ = ack(ctx)
				slog.Default().Warn("skip malformed kafka bid event", "error", err)
				w.observeConsume("malformed")
				continue
			}
			if ctx.Err() != nil {
				return nil, false
			}
			if len(batch) > 0 && fetchCtxErr != nil {
				break
			}
			slog.Default().Warn("fetch kafka bid event failed", "error", err)
			w.observeConsume("fetch_error")
			if len(batch) == 0 {
				sleepContext(ctx, w.retryBackoff)
				continue
			}
			break
		}
		pending, ok := w.preFilter(ctx, event, ack)
		if !ok {
			continue
		}
		if len(batch) == 0 {
			deadline = time.Now().Add(bidRecordBatchMaxLatency)
		}
		batch = append(batch, pending)
	}
	return batch, true
}

func (w *KafkaBidRecordWriter) preFilter(parentCtx context.Context, event redisinfra.BidEvent, ack func(context.Context) error) (kafkaBidRecordPending, bool) {
	ctx := tracing.ExtractMap(parentCtx, event.TraceCarrier())
	if event.EventType != "bid.accepted" || !event.Accepted {
		if ack != nil {
			_ = ack(ctx)
		}
		w.observeConsume("skip")
		return kafkaBidRecordPending{}, false
	}
	return kafkaBidRecordPending{event: event, ack: ack, ctx: ctx}, true
}

func (w *KafkaBidRecordWriter) flushBatch(ctx context.Context, batch []kafkaBidRecordPending) {
	if len(batch) == 0 {
		return
	}
	records := make([]domain.BidRecord, 0, len(batch))
	for _, p := range batch {
		rec := p.event.ToBidRecord()
		if rec.CreatedAt.IsZero() {
			rec.CreatedAt = time.Now().UTC()
		}
		records = append(records, rec)
	}
	writeStart := time.Now()
	err := w.repo.CreateIgnoreBatch(ctx, records)
	w.observeWrite(time.Since(writeStart))
	if err == nil {
		for _, p := range batch {
			if p.ack != nil {
				_ = p.ack(p.ctx)
			}
			w.observeConsume("ok")
		}
		return
	}
	for _, p := range batch {
		w.handleEvent(p.ctx, p.event, p.ack)
	}
}

func (w *KafkaBidRecordWriter) handleEvent(parentCtx context.Context, event redisinfra.BidEvent, ack func(context.Context) error) {
	ctx := tracing.ExtractMap(parentCtx, event.TraceCarrier())
	ctx, span := tracing.StartSpan(ctx, "bid_record.kafka_consume",
		attribute.Int64("auction.id", int64(event.AuctionID)),
		attribute.String("event.type", event.EventType),
		attribute.Int64("event.seq", event.Seq),
		attribute.String("event.stream_id", event.StreamID),
	)
	defer span.End()

	if event.EventType != "bid.accepted" || !event.Accepted {
		if ack != nil {
			_ = ack(ctx)
		}
		span.SetAttributes(attribute.String("bid_record.result", "skip"))
		w.observeConsume("skip")
		return
	}
	for {
		writeStart := time.Now()
		result, err := WriteBidRecordIdempotent(ctx, w.repo, event)
		w.observeWrite(time.Since(writeStart))
		switch result {
		case BidRecordWriteSkipped:
			span.SetAttributes(attribute.String("bid_record.result", "skip"))
			w.observeConsume("skip")
		case BidRecordWriteOK:
			span.SetAttributes(attribute.String("bid_record.result", "ok"))
			w.observeConsume("ok")
		case BidRecordWriteDuplicateConsistent:
			span.SetAttributes(attribute.String("bid_record.result", "duplicate_consistent"))
			w.observeConsume("duplicate_consistent")
		case BidRecordWriteDuplicateConflict:
			span.SetAttributes(attribute.String("bid_record.result", "duplicate_conflict"))
			w.observeConsume("duplicate_conflict")
			if err != nil {
				span.RecordError(err)
			}
		default:
			span.SetAttributes(attribute.String("bid_record.result", "temporary_failure"))
			w.observeConsume("temporary_failure")
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			if !sleepContext(ctx, w.retryBackoff) {
				return
			}
			continue
		}
		if ack != nil {
			_ = ack(ctx)
		}
		return
	}
}

func (w *KafkaBidRecordWriter) observeConsume(result string) {
	if w != nil && w.metrics != nil {
		w.metrics.IncWorkerBidRecordConsume(result)
	}
}

func (w *KafkaBidRecordWriter) observeWrite(elapsed time.Duration) {
	if w != nil && w.metrics != nil {
		w.metrics.ObserveWorkerBidRecordWrite(elapsed)
	}
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

package runtime

import (
	"context"
	"fmt"
	"time"

	"aieas_backend/internal/infra/observability/metrics"
	"aieas_backend/internal/infra/observability/tracing"
	redisinfra "aieas_backend/internal/infra/redis"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// BidRankingWorker 独立消费 bid stream，只做排行榜更新，不做落库。
// 与 BidRecordWriter 共享同一 Redis Stream，但用独立 consumer group
// `bid-ranking-updaters`，避免 MySQL 慢拖慢排行榜。
type BidRankingWorker struct {
	log           bidRankingEventLog
	consumer      string
	maxRetries    int64
	claimIdle     time.Duration
	interval      time.Duration
	claimInterval time.Duration
	metrics       *metrics.Registry
	onUpdated     func(auctionID uint64)
}

type bidRankingEventLog interface {
	Enabled() bool
	ShardCount() int
	ActiveAuctions(ctx context.Context) ([]uint64, error)
	ActiveAuctionsOnShard(ctx context.Context, shardIdx int) ([]uint64, error)
	ClaimStaleConsumerEvents(ctx context.Context, auctionID uint64, group, consumer string, minIdle time.Duration, max int64) ([]redisinfra.BidEvent, error)
	ReadConsumerGroup(ctx context.Context, auctionID uint64, group, consumer string, count int64, block time.Duration) ([]redisinfra.BidEvent, error)
	AckConsumerGroup(ctx context.Context, auctionID uint64, group string, ids ...string) error
	WriteBidRecordDLQ(ctx context.Context, event redisinfra.BidEvent, reason string) error
	UpdateAcceptedRanking(ctx context.Context, auctionID uint64, bidderID string, price, bidTSMS, seq int64) error
}

func NewBidRankingWorker(log *redisinfra.EventLog, consumer string) *BidRankingWorker {
	if consumer == "" {
		consumer = fmt.Sprintf("bid-ranking-%s-%d", hostnameOrPid(), time.Now().UTC().UnixNano())
	}
	return &BidRankingWorker{
		log:           log,
		consumer:      consumer,
		maxRetries:    5,
		claimIdle:     30 * time.Second,
		interval:      bidRecordBatchMaxLatency,
		claimInterval: defaultClaimInterval,
	}
}

func (w *BidRankingWorker) SetMetrics(reg *metrics.Registry) {
	if w == nil {
		return
	}
	w.metrics = reg
}

func (w *BidRankingWorker) SetRankingUpdatedCallback(callback func(auctionID uint64)) {
	if w == nil {
		return
	}
	w.onUpdated = callback
}

func (w *BidRankingWorker) Start(ctx context.Context) {
	if w == nil || w.log == nil || !w.log.Enabled() {
		return
	}
	shardCount := w.log.ShardCount()
	if shardCount <= 1 {
		for idx := 0; idx < workersPerShard; idx++ {
			i := idx
			go w.loopAllShards(ctx, i)
		}
		return
	}
	for shardIdx := 0; shardIdx < shardCount; shardIdx++ {
		for idx := 0; idx < workersPerShard; idx++ {
			s, i := shardIdx, idx
			go w.loopShard(ctx, s, i)
		}
	}
}

func (w *BidRankingWorker) loopAllShards(ctx context.Context, consumerIdx int) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	consumer := w.consumerName(-1, consumerIdx)
	state := newClaimState()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.runOnceAll(ctx, consumer, state)
		}
	}
}

func (w *BidRankingWorker) loopShard(ctx context.Context, shardIdx, consumerIdx int) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	consumer := w.consumerName(shardIdx, consumerIdx)
	state := newClaimState()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.runOnceShard(ctx, shardIdx, consumer, state)
		}
	}
}

func (w *BidRankingWorker) consumerName(shardIdx, consumerIdx int) string {
	if shardIdx < 0 {
		return fmt.Sprintf("%s-i%d", w.consumer, consumerIdx)
	}
	return fmt.Sprintf("%s-s%d-i%d", w.consumer, shardIdx, consumerIdx)
}

func (w *BidRankingWorker) runOnceAll(ctx context.Context, consumer string, state *claimState) {
	auctions, err := w.log.ActiveAuctions(ctx)
	if err != nil {
		w.observeConsume("poll_error")
		return
	}
	w.processAuctions(ctx, auctions, consumer, state)
}

func (w *BidRankingWorker) runOnceShard(ctx context.Context, shardIdx int, consumer string, state *claimState) {
	auctions, err := w.log.ActiveAuctionsOnShard(ctx, shardIdx)
	if err != nil {
		w.observeConsume("poll_error")
		return
	}
	w.processAuctions(ctx, auctions, consumer, state)
}

func (w *BidRankingWorker) processAuctions(ctx context.Context, auctions []uint64, consumer string, state *claimState) {
	doClaim := state.shouldClaim(time.Now(), w.claimInterval)
	for _, auctionID := range auctions {
		if doClaim {
			claimed, err := w.log.ClaimStaleConsumerEvents(ctx, auctionID, redisinfra.BidRankingConsumerGroup, consumer, w.claimIdle, 32)
			if err == nil {
				w.handleEvents(ctx, claimed)
			} else {
				w.observeConsume("poll_error")
			}
		}
		events, err := w.log.ReadConsumerGroup(ctx, auctionID, redisinfra.BidRankingConsumerGroup, consumer, bidRecordBatchMaxN, 10*time.Millisecond)
		if err == nil {
			w.handleEvents(ctx, events)
		} else {
			w.observeConsume("poll_error")
		}
	}
	if doClaim {
		state.markClaimed(time.Now())
	}
}

// handleEvents 单条逐个处理 ranking 更新；ranking 失败保留 PEL 重试（不 ACK）。
func (w *BidRankingWorker) handleEvents(ctx context.Context, events []redisinfra.BidEvent) {
	for _, event := range events {
		w.handleEvent(ctx, event)
	}
}

func (w *BidRankingWorker) handleEvent(parentCtx context.Context, event redisinfra.BidEvent) {
	ctx := tracing.ExtractMap(parentCtx, event.TraceCarrier())
	ctx, span := tracing.StartSpan(ctx, "bid_ranking.consume",
		attribute.Int64("auction.id", int64(event.AuctionID)),
		attribute.String("event.type", event.EventType),
		attribute.Int64("event.seq", event.Seq),
		attribute.String("event.stream_id", event.StreamID),
	)
	defer span.End()

	if event.EventType != "bid.accepted" || !event.Accepted {
		_ = w.log.AckConsumerGroup(ctx, event.AuctionID, redisinfra.BidRankingConsumerGroup, event.StreamID)
		span.SetAttributes(attribute.String("bid_ranking.result", "skip"))
		w.observeConsume("skip")
		return
	}
	if event.BidderID == "" || event.BidPrice <= 0 {
		_ = w.log.AckConsumerGroup(ctx, event.AuctionID, redisinfra.BidRankingConsumerGroup, event.StreamID)
		span.SetAttributes(attribute.String("bid_ranking.result", "skip"))
		w.observeConsume("skip")
		return
	}
	if event.Deliveries >= w.maxRetries {
		_ = w.log.WriteBidRecordDLQ(ctx, event, "RANKING_MAX_RETRIES")
		_ = w.log.AckConsumerGroup(ctx, event.AuctionID, redisinfra.BidRankingConsumerGroup, event.StreamID)
		span.SetAttributes(attribute.String("bid_ranking.result", "dlq"))
		w.observeDLQ("RANKING_MAX_RETRIES")
		w.observeConsume("dlq")
		return
	}
	if err := w.log.UpdateAcceptedRanking(ctx, event.AuctionID, event.BidderID, event.BidPrice, event.BidTSMS, event.Seq); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(attribute.String("bid_ranking.result", "temporary_failure"))
		w.observeConsume("temporary_failure")
		return
	}
	_ = w.log.AckConsumerGroup(ctx, event.AuctionID, redisinfra.BidRankingConsumerGroup, event.StreamID)
	if w.onUpdated != nil {
		w.onUpdated(event.AuctionID)
	}
	span.SetAttributes(attribute.String("bid_ranking.result", "ok"))
	w.observeConsume("ok")
}

func (w *BidRankingWorker) observeConsume(result string) {
	if w != nil && w.metrics != nil {
		w.metrics.IncWorkerBidRankingConsume(result)
	}
}

func (w *BidRankingWorker) observeDLQ(reason string) {
	if w != nil && w.metrics != nil {
		w.metrics.IncWorkerBidRecordDLQ(reason)
	}
}

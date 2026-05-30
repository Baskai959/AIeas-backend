package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/observability/metrics"
	"aieas_backend/internal/infra/observability/tracing"
	redisinfra "aieas_backend/internal/infra/redis"
	"aieas_backend/internal/repository"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type BidRecordWriter struct {
	repo       repository.BidRepository
	log        bidRecordEventLog
	consumer   string
	maxRetries int64
	claimIdle  time.Duration
	interval   time.Duration
	metrics    *metrics.Registry
}

type bidRecordEventLog interface {
	Enabled() bool
	ShardCount() int
	ActiveAuctions(ctx context.Context) ([]uint64, error)
	ActiveAuctionsOnShard(ctx context.Context, shardIdx int) ([]uint64, error)
	ClaimStaleBidRecordEvents(ctx context.Context, auctionID uint64, consumer string, minIdle time.Duration, max int64) ([]redisinfra.BidEvent, error)
	ReadBidRecordGroup(ctx context.Context, auctionID uint64, consumer string, count int64, block time.Duration) ([]redisinfra.BidEvent, error)
	AckBidRecord(ctx context.Context, auctionID uint64, ids ...string) error
	WriteBidRecordDLQ(ctx context.Context, event redisinfra.BidEvent, reason string) error
}

func NewBidRecordWriter(repo repository.BidRepository, log *redisinfra.EventLog, consumer string) *BidRecordWriter {
	if consumer == "" {
		consumer = fmt.Sprintf("bid-record-%d", time.Now().UTC().UnixNano())
	}
	return &BidRecordWriter{repo: repo, log: log, consumer: consumer, maxRetries: 5, claimIdle: 30 * time.Second, interval: time.Second}
}

// SetMetrics 注入压测所需的 worker 指标。nil 安全。
func (w *BidRecordWriter) SetMetrics(reg *metrics.Registry) {
	if w == nil {
		return
	}
	w.metrics = reg
}

// Start 为每个 RT shard 启动一个独立的 worker goroutine：
// 每个 goroutine 只巡检自己那一片的 active_streams，避免跨 shard 故障互相放大。
// shardCount<=1（含 fallback 单实例）时退化为单 goroutine 巡检全集合。
func (w *BidRecordWriter) Start(ctx context.Context) {
	if w == nil || w.repo == nil || w.log == nil || !w.log.Enabled() {
		return
	}
	shardCount := w.log.ShardCount()
	if shardCount <= 1 {
		go w.loopAllShards(ctx)
		return
	}
	for shardIdx := 0; shardIdx < shardCount; shardIdx++ {
		idx := shardIdx
		go w.loopShard(ctx, idx)
	}
}

func (w *BidRecordWriter) loopAllShards(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.runOnceAll(ctx)
		}
	}
}

func (w *BidRecordWriter) loopShard(ctx context.Context, shardIdx int) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.runOnceShard(ctx, shardIdx)
		}
	}
}

// consumerForShard 把 shard 索引拼到 consumer 名上，便于在 Redis XINFO 中识别归属。
func (w *BidRecordWriter) consumerForShard(shardIdx int) string {
	return fmt.Sprintf("%s-s%d", w.consumer, shardIdx)
}

func (w *BidRecordWriter) runOnceAll(ctx context.Context) {
	auctions, err := w.log.ActiveAuctions(ctx)
	if err != nil {
		w.observeConsume("poll_error")
		return
	}
	w.processAuctions(ctx, auctions, w.consumer)
}

func (w *BidRecordWriter) runOnceShard(ctx context.Context, shardIdx int) {
	auctions, err := w.log.ActiveAuctionsOnShard(ctx, shardIdx)
	if err != nil {
		w.observeConsume("poll_error")
		return
	}
	w.processAuctions(ctx, auctions, w.consumerForShard(shardIdx))
}

func (w *BidRecordWriter) processAuctions(ctx context.Context, auctions []uint64, consumer string) {
	for _, auctionID := range auctions {
		claimed, err := w.log.ClaimStaleBidRecordEvents(ctx, auctionID, consumer, w.claimIdle, 32)
		if err == nil {
			w.handleEvents(ctx, claimed)
		} else {
			w.observeConsume("poll_error")
		}
		events, err := w.log.ReadBidRecordGroup(ctx, auctionID, consumer, 64, 10*time.Millisecond)
		if err == nil {
			w.handleEvents(ctx, events)
		} else {
			w.observeConsume("poll_error")
		}
	}
}

func (w *BidRecordWriter) handleEvents(ctx context.Context, events []redisinfra.BidEvent) {
	for _, event := range events {
		w.handleEvent(ctx, event)
	}
}

func (w *BidRecordWriter) handleEvent(parentCtx context.Context, event redisinfra.BidEvent) {
	ctx := tracing.ExtractMap(parentCtx, event.TraceCarrier())
	ctx, span := tracing.StartSpan(ctx, "bid_record.consume",
		attribute.Int64("auction.id", int64(event.AuctionID)),
		attribute.String("event.type", event.EventType),
		attribute.Int64("event.seq", event.Seq),
		attribute.String("event.stream_id", event.StreamID),
	)
	defer span.End()

	if event.EventType != "bid.accepted" && event.EventType != "bid.rejected" {
		_ = w.log.AckBidRecord(ctx, event.AuctionID, event.StreamID)
		span.SetAttributes(attribute.String("bid_record.result", "skip"))
		w.observeConsume("skip")
		return
	}
	if event.Deliveries >= w.maxRetries {
		_ = w.log.WriteBidRecordDLQ(ctx, event, "MAX_RETRIES")
		_ = w.log.AckBidRecord(ctx, event.AuctionID, event.StreamID)
		span.SetAttributes(attribute.String("bid_record.result", "dlq_max_retries"))
		w.observeDLQ("MAX_RETRIES")
		w.observeConsume("dlq")
		return
	}
	writeStart := time.Now()
	result, err := WriteBidRecordIdempotent(ctx, w.repo, event)
	w.observeWrite(time.Since(writeStart))
	switch result {
	case BidRecordWriteOK, BidRecordWriteDuplicateConsistent:
		_ = w.log.AckBidRecord(ctx, event.AuctionID, event.StreamID)
		if result == BidRecordWriteOK {
			span.SetAttributes(attribute.String("bid_record.result", "ok"))
			w.observeConsume("ok")
		} else {
			span.SetAttributes(attribute.String("bid_record.result", "duplicate_consistent"))
			w.observeConsume("duplicate_consistent")
		}
	case BidRecordWriteDuplicateConflict:
		_ = w.log.WriteBidRecordDLQ(ctx, event, "DUPLICATE_CONFLICT")
		_ = w.log.AckBidRecord(ctx, event.AuctionID, event.StreamID)
		span.SetAttributes(attribute.String("bid_record.result", "duplicate_conflict"))
		w.observeDLQ("DUPLICATE_CONFLICT")
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
		if err != nil && event.Deliveries+1 >= w.maxRetries {
			_ = w.log.WriteBidRecordDLQ(ctx, event, "MAX_RETRIES")
			_ = w.log.AckBidRecord(ctx, event.AuctionID, event.StreamID)
			span.SetAttributes(attribute.String("bid_record.result", "dlq_max_retries"))
			w.observeDLQ("MAX_RETRIES")
			w.observeConsume("dlq")
		}
	}
}

func (w *BidRecordWriter) observeConsume(result string) {
	if w != nil && w.metrics != nil {
		w.metrics.IncWorkerBidRecordConsume(result)
	}
}

func (w *BidRecordWriter) observeWrite(elapsed time.Duration) {
	if w != nil && w.metrics != nil {
		w.metrics.ObserveWorkerBidRecordWrite(elapsed)
	}
}

func (w *BidRecordWriter) observeDLQ(reason string) {
	if w != nil && w.metrics != nil {
		w.metrics.IncWorkerBidRecordDLQ(reason)
	}
}

type BidRecordWriteResult int

const (
	BidRecordWriteOK BidRecordWriteResult = iota
	BidRecordWriteDuplicateConsistent
	BidRecordWriteDuplicateConflict
	BidRecordWriteTemporaryFailure
)

func WriteBidRecordIdempotent(ctx context.Context, repo repository.BidRepository, event redisinfra.BidEvent) (BidRecordWriteResult, error) {
	record := event.ToBidRecord()
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	existing, findErr := repo.FindByRequestID(ctx, event.RequestID)
	if findErr == nil {
		if bidRecordConsistent(existing, record) {
			return BidRecordWriteDuplicateConsistent, nil
		}
		return BidRecordWriteDuplicateConflict, domain.ErrConflict
	}
	if !errors.Is(findErr, domain.ErrNotFound) {
		return BidRecordWriteTemporaryFailure, findErr
	}
	err := repo.Create(ctx, &record)
	if err == nil {
		return BidRecordWriteOK, nil
	}
	if !errors.Is(err, domain.ErrConflict) && !isDuplicateError(err) {
		return BidRecordWriteTemporaryFailure, err
	}
	existing, findErr = repo.FindByRequestID(ctx, event.RequestID)
	if findErr != nil {
		return BidRecordWriteTemporaryFailure, findErr
	}
	if bidRecordConsistent(existing, event.ToBidRecord()) {
		return BidRecordWriteDuplicateConsistent, nil
	}
	return BidRecordWriteDuplicateConflict, err
}

func bidRecordConsistent(existing, incoming domain.BidRecord) bool {
	return existing.RequestID == incoming.RequestID && existing.AuctionID == incoming.AuctionID && existing.BidderID == incoming.BidderID && existing.BidPrice == incoming.BidPrice && existing.BidTSMS == incoming.BidTSMS && existing.Source == incoming.Source && existing.RiskResult == incoming.RiskResult && existing.RejectReason == incoming.RejectReason
}

func isDuplicateError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate") || strings.Contains(msg, "unique") || strings.Contains(msg, "1062")
}

type BidRecordReconciler struct {
	repo    repository.BidRepository
	log     bidRecordReconcileLog
	metrics *metrics.Registry
}

type bidRecordReconcileLog interface {
	Enabled() bool
	ShardCount() int
	ActiveAuctions(ctx context.Context) ([]uint64, error)
	ActiveAuctionsOnShard(ctx context.Context, shardIdx int) ([]uint64, error)
	ReconcileCheckpoint(ctx context.Context, auctionID uint64) (int64, error)
	ReplayBidEvents(ctx context.Context, auctionID uint64, lastSeq int64, limit int64) ([]redisinfra.BidEvent, bool, error)
	SetReconcileCheckpoint(ctx context.Context, auctionID uint64, seq int64) error
	WriteBidRecordDLQ(ctx context.Context, event redisinfra.BidEvent, reason string) error
}

func NewBidRecordReconciler(repo repository.BidRepository, log *redisinfra.EventLog) *BidRecordReconciler {
	return &BidRecordReconciler{repo: repo, log: log}
}

// SetMetrics 注入压测所需的补偿巡检指标。nil 安全。
func (r *BidRecordReconciler) SetMetrics(reg *metrics.Registry) {
	if r == nil {
		return
	}
	r.metrics = reg
}

// Start 为每个 RT shard 起一个独立的 reconcile goroutine。
// shardCount<=1 时退化为单 goroutine 巡检全集合，行为与未分片时一致。
func (r *BidRecordReconciler) Start(ctx context.Context, interval time.Duration) {
	if r == nil || r.repo == nil || r.log == nil || !r.log.Enabled() {
		return
	}
	if interval <= 0 {
		interval = time.Minute
	}
	shardCount := r.log.ShardCount()
	if shardCount <= 1 {
		go r.loopAll(ctx, interval)
		return
	}
	for shardIdx := 0; shardIdx < shardCount; shardIdx++ {
		idx := shardIdx
		go r.loopShard(ctx, idx, interval)
	}
}

func (r *BidRecordReconciler) loopAll(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	_ = r.RunOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = r.RunOnce(ctx)
		}
	}
}

func (r *BidRecordReconciler) loopShard(ctx context.Context, shardIdx int, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	_ = r.RunOnceShard(ctx, shardIdx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = r.RunOnceShard(ctx, shardIdx)
		}
	}
}

func (r *BidRecordReconciler) RunOnce(ctx context.Context) (err error) {
	if r == nil || r.repo == nil || r.log == nil || !r.log.Enabled() {
		return nil
	}
	defer func() { r.observeTask("bid_record_reconcile", err) }()
	auctions, err := r.log.ActiveAuctions(ctx)
	if err != nil {
		return err
	}
	return r.reconcileAuctions(ctx, auctions)
}

// RunOnceShard 对单个 shard 上的 active_streams 跑一轮 reconcile；
// 错误立即向上冒泡（与 RunOnce 行为一致）。
func (r *BidRecordReconciler) RunOnceShard(ctx context.Context, shardIdx int) (err error) {
	if r == nil || r.repo == nil || r.log == nil || !r.log.Enabled() {
		return nil
	}
	defer func() { r.observeTask("bid_record_reconcile", err) }()
	auctions, err := r.log.ActiveAuctionsOnShard(ctx, shardIdx)
	if err != nil {
		return err
	}
	return r.reconcileAuctions(ctx, auctions)
}

func (r *BidRecordReconciler) reconcileAuctions(ctx context.Context, auctions []uint64) error {
	for _, auctionID := range auctions {
		lastSeq, err := r.log.ReconcileCheckpoint(ctx, auctionID)
		if err != nil {
			return err
		}
		events, complete, err := r.log.ReplayBidEvents(ctx, auctionID, lastSeq, 512)
		if err != nil {
			return err
		}
		if !complete {
			_ = r.log.WriteBidRecordDLQ(ctx, redisinfra.BidEvent{AuctionID: auctionID, Seq: lastSeq}, "RECONCILE_GAP")
			r.observeDLQ("RECONCILE_GAP")
			continue
		}
		for _, event := range events {
			if event.EventType != "bid.accepted" && event.EventType != "bid.rejected" {
				continue
			}
			writeStart := time.Now()
			result, err := WriteBidRecordIdempotent(ctx, r.repo, event)
			r.observeWrite(time.Since(writeStart))
			if result == BidRecordWriteDuplicateConflict {
				_ = r.log.WriteBidRecordDLQ(ctx, event, "RECONCILE_DUPLICATE_CONFLICT")
				r.observeDLQ("RECONCILE_DUPLICATE_CONFLICT")
			} else if result == BidRecordWriteTemporaryFailure {
				return err
			}
			if event.Seq > lastSeq {
				lastSeq = event.Seq
			}
		}
		if err := r.log.SetReconcileCheckpoint(ctx, auctionID, lastSeq); err != nil {
			return err
		}
	}
	return nil
}

func (r *BidRecordReconciler) observeTask(worker string, err error) {
	if r == nil || r.metrics == nil {
		return
	}
	result := "ok"
	if err != nil {
		result = "error"
	}
	r.metrics.IncWorkerTask(worker, result)
}

func (r *BidRecordReconciler) observeWrite(elapsed time.Duration) {
	if r != nil && r.metrics != nil {
		r.metrics.ObserveWorkerBidRecordWrite(elapsed)
	}
}

func (r *BidRecordReconciler) observeDLQ(reason string) {
	if r != nil && r.metrics != nil {
		r.metrics.IncWorkerBidRecordDLQ(reason)
	}
}

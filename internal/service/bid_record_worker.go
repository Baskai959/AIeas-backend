package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
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

const (
	workersPerShard          = 4
	bidRecordBatchMaxN       = 256
	bidRecordBatchMaxLatency = 50 * time.Millisecond
	defaultClaimInterval     = 15 * time.Second
	defaultBidStreamMaxLen   = 10000
	defaultBidStreamTrimTick = time.Second
)

type BidRecordWriter struct {
	repo          repository.BidRepository
	log           bidRecordEventLog
	consumer      string
	maxRetries    int64
	claimIdle     time.Duration
	interval      time.Duration
	claimInterval time.Duration
	metrics       *metrics.Registry
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

type BidStreamTrimWorker struct {
	log    bidStreamTrimLog
	maxLen int64
}

type bidStreamTrimLog interface {
	Enabled() bool
	ShardCount() int
	ActiveAuctions(ctx context.Context) ([]uint64, error)
	ActiveAuctionsOnShard(ctx context.Context, shardIdx int) ([]uint64, error)
	TrimAuctionStream(ctx context.Context, auctionID uint64, maxLen int64) error
}

func NewBidStreamTrimWorker(log *redisinfra.EventLog, maxLen int64) *BidStreamTrimWorker {
	if maxLen <= 0 {
		maxLen = defaultBidStreamMaxLen
	}
	return &BidStreamTrimWorker{log: log, maxLen: maxLen}
}

func (w *BidStreamTrimWorker) Start(ctx context.Context, interval time.Duration) {
	if w == nil || w.log == nil || !w.log.Enabled() {
		return
	}
	if interval <= 0 {
		interval = defaultBidStreamTrimTick
	}
	shardCount := w.log.ShardCount()
	if shardCount <= 1 {
		go w.loopAll(ctx, interval)
		return
	}
	for shardIdx := 0; shardIdx < shardCount; shardIdx++ {
		idx := shardIdx
		go w.loopShard(ctx, idx, interval)
	}
}

func (w *BidStreamTrimWorker) loopAll(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	w.runOnceAll(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.runOnceAll(ctx)
		}
	}
}

func (w *BidStreamTrimWorker) loopShard(ctx context.Context, shardIdx int, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	w.runOnceShard(ctx, shardIdx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.runOnceShard(ctx, shardIdx)
		}
	}
}

func (w *BidStreamTrimWorker) runOnceAll(ctx context.Context) {
	auctions, err := w.log.ActiveAuctions(ctx)
	if err != nil {
		return
	}
	w.trimAuctions(ctx, auctions)
}

func (w *BidStreamTrimWorker) runOnceShard(ctx context.Context, shardIdx int) {
	auctions, err := w.log.ActiveAuctionsOnShard(ctx, shardIdx)
	if err != nil {
		return
	}
	w.trimAuctions(ctx, auctions)
}

func (w *BidStreamTrimWorker) trimAuctions(ctx context.Context, auctions []uint64) {
	for _, auctionID := range auctions {
		_ = w.log.TrimAuctionStream(ctx, auctionID, w.maxLen)
	}
}

func NewBidRecordWriter(repo repository.BidRepository, log *redisinfra.EventLog, consumer string) *BidRecordWriter {
	if consumer == "" {
		consumer = fmt.Sprintf("bid-record-%s-%d", hostnameOrPid(), time.Now().UTC().UnixNano())
	}
	return &BidRecordWriter{
		repo:          repo,
		log:           log,
		consumer:      consumer,
		maxRetries:    5,
		claimIdle:     30 * time.Second,
		interval:      bidRecordBatchMaxLatency,
		claimInterval: defaultClaimInterval,
	}
}

// SetMetrics 注入压测所需的 worker 指标。nil 安全。
func (w *BidRecordWriter) SetMetrics(reg *metrics.Registry) {
	if w == nil {
		return
	}
	w.metrics = reg
}

// Start 为每个 RT shard 启动 workersPerShard 个独立 consumer goroutine：
// 同 shard 内多 consumer 共享同一个 consumer group，由 Redis Streams 自动分配消息；
// shardCount<=1（含 fallback 单实例）时退化为单 shard 启 workersPerShard 个 consumer。
func (w *BidRecordWriter) Start(ctx context.Context) {
	if w == nil || w.repo == nil || w.log == nil || !w.log.Enabled() {
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

func (w *BidRecordWriter) loopAllShards(ctx context.Context, consumerIdx int) {
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

func (w *BidRecordWriter) loopShard(ctx context.Context, shardIdx, consumerIdx int) {
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

// consumerName 把 shard / consumer 索引拼到 consumer 名上，便于 Redis XINFO 区分。
func (w *BidRecordWriter) consumerName(shardIdx, consumerIdx int) string {
	if shardIdx < 0 {
		return fmt.Sprintf("%s-i%d", w.consumer, consumerIdx)
	}
	return fmt.Sprintf("%s-s%d-i%d", w.consumer, shardIdx, consumerIdx)
}

func (w *BidRecordWriter) runOnceAll(ctx context.Context, consumer string, state *claimState) {
	auctions, err := w.log.ActiveAuctions(ctx)
	if err != nil {
		w.observeConsume("poll_error")
		return
	}
	w.processAuctions(ctx, auctions, consumer, state)
}

func (w *BidRecordWriter) runOnceShard(ctx context.Context, shardIdx int, consumer string, state *claimState) {
	auctions, err := w.log.ActiveAuctionsOnShard(ctx, shardIdx)
	if err != nil {
		w.observeConsume("poll_error")
		return
	}
	w.processAuctions(ctx, auctions, consumer, state)
}

func (w *BidRecordWriter) processAuctions(ctx context.Context, auctions []uint64, consumer string, state *claimState) {
	doClaim := state.shouldClaim(time.Now(), w.claimInterval)
	batch := make([]bidRecordPending, 0, bidRecordBatchMaxN)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		w.flushBatch(ctx, batch)
		batch = batch[:0]
	}
	enqueue := func(events []redisinfra.BidEvent) {
		for _, event := range events {
			if pending, ok := w.preFilter(ctx, event); ok {
				batch = append(batch, pending)
				if len(batch) >= bidRecordBatchMaxN {
					flush()
				}
			}
		}
	}
	for _, auctionID := range auctions {
		if doClaim {
			claimed, err := w.log.ClaimStaleBidRecordEvents(ctx, auctionID, consumer, w.claimIdle, 32)
			if err == nil {
				enqueue(claimed)
			} else {
				w.observeConsume("poll_error")
			}
		}
		events, err := w.log.ReadBidRecordGroup(ctx, auctionID, consumer, bidRecordBatchMaxN, 10*time.Millisecond)
		if err == nil {
			enqueue(events)
		} else {
			w.observeConsume("poll_error")
		}
	}
	flush()
	if doClaim {
		state.markClaimed(time.Now())
	}
}

// preFilter 处理无需写库的事件：rejected/duplicate skip、Deliveries 超阈值 DLQ。
// 返回 false 表示已处理（ack 完毕），不进入批量。
func (w *BidRecordWriter) preFilter(parentCtx context.Context, event redisinfra.BidEvent) (bidRecordPending, bool) {
	ctx := tracing.ExtractMap(parentCtx, event.TraceCarrier())
	if event.EventType != "bid.accepted" || !event.Accepted {
		_ = w.log.AckBidRecord(ctx, event.AuctionID, event.StreamID)
		w.observeConsume("skip")
		return bidRecordPending{}, false
	}
	if event.Deliveries >= w.maxRetries {
		_ = w.log.WriteBidRecordDLQ(ctx, event, "MAX_RETRIES")
		_ = w.log.AckBidRecord(ctx, event.AuctionID, event.StreamID)
		w.observeDLQ("MAX_RETRIES")
		w.observeConsume("dlq")
		return bidRecordPending{}, false
	}
	return bidRecordPending{event: event, ctx: ctx}, true
}

type bidRecordPending struct {
	event redisinfra.BidEvent
	ctx   context.Context
}

func (w *BidRecordWriter) flushBatch(ctx context.Context, batch []bidRecordPending) {
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
		w.batchAck(ctx, batch)
		return
	}
	// 批量整体失败 → 降级逐条 WriteBidRecordIdempotent，精准识别冲突 / 临时失败。
	for _, p := range batch {
		w.handleEventFallback(p.ctx, p.event)
	}
}

func (w *BidRecordWriter) batchAck(ctx context.Context, batch []bidRecordPending) {
	type ackKey struct{ auctionID uint64 }
	groups := make(map[ackKey][]string)
	for _, p := range batch {
		key := ackKey{auctionID: p.event.AuctionID}
		groups[key] = append(groups[key], p.event.StreamID)
		w.observeConsume("ok")
	}
	for key, ids := range groups {
		_ = w.log.AckBidRecord(ctx, key.auctionID, ids...)
	}
}

func (w *BidRecordWriter) handleEventFallback(parentCtx context.Context, event redisinfra.BidEvent) {
	ctx, span := tracing.StartSpan(parentCtx, "bid_record.consume",
		attribute.Int64("auction.id", int64(event.AuctionID)),
		attribute.String("event.type", event.EventType),
		attribute.Int64("event.seq", event.Seq),
		attribute.String("event.stream_id", event.StreamID),
	)
	defer span.End()
	writeStart := time.Now()
	result, err := WriteBidRecordIdempotent(ctx, w.repo, event)
	w.observeWrite(time.Since(writeStart))
	switch result {
	case BidRecordWriteSkipped:
		_ = w.log.AckBidRecord(ctx, event.AuctionID, event.StreamID)
		span.SetAttributes(attribute.String("bid_record.result", "skip"))
		w.observeConsume("skip")
	case BidRecordWriteOK:
		_ = w.log.AckBidRecord(ctx, event.AuctionID, event.StreamID)
		span.SetAttributes(attribute.String("bid_record.result", "ok"))
		w.observeConsume("ok")
	case BidRecordWriteDuplicateConsistent:
		_ = w.log.AckBidRecord(ctx, event.AuctionID, event.StreamID)
		span.SetAttributes(attribute.String("bid_record.result", "duplicate_consistent"))
		w.observeConsume("duplicate_consistent")
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

// handleEvents 批量处理一组事件。供测试 / 兼容调用方使用：先做 preFilter（rejected
// 直接 ack；超阈值 DLQ），剩下进入批量写库 → 批量 ACK；批量失败降级逐条。
func (w *BidRecordWriter) handleEvents(ctx context.Context, events []redisinfra.BidEvent) {
	if len(events) == 0 {
		return
	}
	batch := make([]bidRecordPending, 0, len(events))
	for _, event := range events {
		if pending, ok := w.preFilter(ctx, event); ok {
			batch = append(batch, pending)
		}
	}
	if len(batch) == 0 {
		return
	}
	w.flushBatch(ctx, batch)
}

type claimState struct {
	lastClaimAt time.Time
}

func newClaimState() *claimState { return &claimState{} }

func (s *claimState) shouldClaim(now time.Time, interval time.Duration) bool {
	if s == nil {
		return true
	}
	if s.lastClaimAt.IsZero() {
		return true
	}
	if interval <= 0 {
		return true
	}
	return now.Sub(s.lastClaimAt) >= interval
}

func (s *claimState) markClaimed(now time.Time) {
	if s != nil {
		s.lastClaimAt = now
	}
}

func hostnameOrPid() string {
	if name, err := os.Hostname(); err == nil && strings.TrimSpace(name) != "" {
		return name
	}
	return strconv.Itoa(os.Getpid())
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
	BidRecordWriteSkipped
)

type bidRecordInsertIgnorer interface {
	CreateIgnore(ctx context.Context, bid *domain.BidRecord) (bool, error)
}

func WriteBidRecordIdempotent(ctx context.Context, repo repository.BidRepository, event redisinfra.BidEvent) (BidRecordWriteResult, error) {
	if event.EventType != "bid.accepted" || !event.Accepted {
		return BidRecordWriteSkipped, nil
	}
	record := event.ToBidRecord()
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	if inserter, ok := repo.(bidRecordInsertIgnorer); ok {
		inserted, err := inserter.CreateIgnore(ctx, &record)
		if err != nil {
			return BidRecordWriteTemporaryFailure, err
		}
		if inserted {
			return BidRecordWriteOK, nil
		}
		return BidRecordWriteDuplicateConsistent, nil
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
			if event.EventType != "bid.accepted" || !event.Accepted {
				if event.Seq > lastSeq {
					lastSeq = event.Seq
				}
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

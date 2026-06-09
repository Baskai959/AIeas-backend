package runtime

import (
	"context"
	"encoding/json"
	"log/slog"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"aieas_backend/internal/domain"
	kafkainfra "aieas_backend/internal/infra/kafka"
	"aieas_backend/internal/infra/observability/metrics"
	"aieas_backend/internal/infra/observability/tracing"
	auctionapp "aieas_backend/internal/modules/auction/app"
	corews "aieas_backend/internal/transport/ws"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// BidCommandConsumer 顺序拉取竞价命令。由 kafka.BidCommandReader 实现。
type BidCommandConsumer interface {
	FetchBidCommand(ctx context.Context) (kafkainfra.BidCommand, func(context.Context) error, error)
}

// BidArbitrator 复用原 Lua 链路执行裁决。由 *auctionapp.BidService 实现。
type BidArbitrator interface {
	ArbitrateFromCommand(ctx context.Context, cmd auctionapp.BidCommandSnapshot) (domain.BidResult, error)
}

// BidResultDelivery 定向推送裁决结果（bid.result）并登记重发态。由 *ws.BidAsyncCoordinator 实现。
type BidResultDelivery interface {
	DeliverBidResult(sessionID, auctionID uint64, userID string, p corews.BidResultPayload)
}

type BidCommandInFlightReleaser interface {
	ReleaseBidCommand(ctx context.Context, auctionID uint64, bidID string) error
}

// BidPartitionLagSource 可选：暴露 reader 当前已观测 partition 的 lag map，
// 用于 bid_kafka_partition_lag 指标周期采集。线上的 *kafkainfra.BidCommandReader
// 实现该接口；测试 fake 可以不实现。
//
// 多 reader 实例（同一 group 跨进程）下，每个实例只会看到自己被分配的 partition，
// 分别上报，不会与其他实例产生 series 冲突。
type BidPartitionLagSource interface {
	PartitionLag() map[int]int64
}

// BidDecisionWorker 从 aieas.bid.commands 并发消费命令，调用 ArbitrateFromCommand
// 复用 Lua 裁决，映射 finalStatus（ACCEPTED|REJECTED），并通过协调器定向推送
// bid.result。
//
// 路线 X：放弃 Kafka partition FIFO，主消费 goroutine 仅做 fetch + 投递；
// 实际裁决在 worker pool 中并发执行，同 auction 串行性由 Redis 同 shard 单线程
// 兜底（bid.lua + idem_key）。worker pool 大小由 PoolSize 控制；
// commit 模式由 CommitMode 控制（single | batch），batch 模式由后台 committer
// goroutine 攒批后顺序 commit。
type BidDecisionWorker struct {
	consumer      BidCommandConsumer
	arbitrator    BidArbitrator
	delivery      BidResultDelivery
	retryBackoff  time.Duration
	metrics       *metrics.Registry
	poolSize      int
	commitMode    string
	commitBatchN  int
	commitMaxWait time.Duration
	shutdownGrace time.Duration
	lagPollPeriod time.Duration
	inFlight      BidCommandInFlightReleaser
}

// BidDecisionWorkerOptions 控制 worker pool 与 commit 行为。零值取默认。
type BidDecisionWorkerOptions struct {
	PoolSize             int
	CommitMode           string
	CommitBatchSize      int
	CommitMaxLatencyMs   int
	ShutdownGracePeriod  time.Duration
	PartitionLagPollSecs int
}

const (
	defaultBidDecisionPoolSize        = 32
	defaultBidDecisionCommitBatchSize = 64
	defaultBidDecisionCommitMaxWaitMS = 200
	defaultBidDecisionShutdownGrace   = 5 * time.Second
	defaultBidDecisionLagPollSec      = 5
)

func NewBidDecisionWorker(consumer BidCommandConsumer, arbitrator BidArbitrator, delivery BidResultDelivery) *BidDecisionWorker {
	return NewBidDecisionWorkerWithOptions(consumer, arbitrator, delivery, BidDecisionWorkerOptions{})
}

// NewBidDecisionWorkerWithOptions 构造可配置的 worker。负值/0/非法值会归一化回默认。
func NewBidDecisionWorkerWithOptions(consumer BidCommandConsumer, arbitrator BidArbitrator, delivery BidResultDelivery, opts BidDecisionWorkerOptions) *BidDecisionWorker {
	pool := opts.PoolSize
	if pool <= 0 {
		pool = defaultBidDecisionPoolSize
	}
	mode := strings.ToLower(strings.TrimSpace(opts.CommitMode))
	if mode != "single" && mode != "batch" {
		mode = "batch"
	}
	batchN := opts.CommitBatchSize
	if batchN <= 0 {
		batchN = defaultBidDecisionCommitBatchSize
	}
	maxWait := time.Duration(opts.CommitMaxLatencyMs) * time.Millisecond
	if maxWait <= 0 {
		maxWait = time.Duration(defaultBidDecisionCommitMaxWaitMS) * time.Millisecond
	}
	grace := opts.ShutdownGracePeriod
	if grace <= 0 {
		grace = defaultBidDecisionShutdownGrace
	}
	lagPoll := time.Duration(opts.PartitionLagPollSecs) * time.Second
	if lagPoll <= 0 {
		lagPoll = time.Duration(defaultBidDecisionLagPollSec) * time.Second
	}
	return &BidDecisionWorker{
		consumer:      consumer,
		arbitrator:    arbitrator,
		delivery:      delivery,
		retryBackoff:  time.Second,
		poolSize:      pool,
		commitMode:    mode,
		commitBatchN:  batchN,
		commitMaxWait: maxWait,
		shutdownGrace: grace,
		lagPollPeriod: lagPoll,
	}
}

// SetMetrics 注入裁决 worker 指标。nil 安全。
func (w *BidDecisionWorker) SetMetrics(reg *metrics.Registry) {
	if w == nil {
		return
	}
	w.metrics = reg
}

func (w *BidDecisionWorker) SetInFlightReleaser(releaser BidCommandInFlightReleaser) {
	if w == nil {
		return
	}
	w.inFlight = releaser
}

func (w *BidDecisionWorker) Start(ctx context.Context) {
	if w == nil || w.consumer == nil || w.arbitrator == nil {
		return
	}
	go w.loop(ctx)
}

// commitJob 是 batch commit 队列中的一个待提交项，记录 fetch 时刻用于
// bid_worker_commit_lag_seconds 指标。
type commitJob struct {
	commit  func(context.Context) error
	fetched time.Time
}

func (w *BidDecisionWorker) loop(parentCtx context.Context) {
	pool := w.poolSize
	if pool <= 0 {
		pool = defaultBidDecisionPoolSize
	}
	sem := make(chan struct{}, pool)
	var wg sync.WaitGroup

	// commitCh 仅在 batch 模式下使用；committer goroutine 顺序消费、攒批 commit。
	var commitCh chan commitJob
	committerDone := make(chan struct{})
	if w.commitMode == "batch" {
		commitCh = make(chan commitJob, pool*2)
		go w.committerLoop(parentCtx, commitCh, committerDone)
	} else {
		close(committerDone)
	}

	// partition lag 周期采集。
	if lagSrc, ok := w.consumer.(BidPartitionLagSource); ok && w.metrics != nil {
		go w.lagSamplerLoop(parentCtx, lagSrc)
	}

	for {
		if parentCtx.Err() != nil {
			break
		}
		cmd, commit, err := w.consumer.FetchBidCommand(parentCtx)
		fetchedAt := time.Now()
		if err != nil {
			if commit != nil {
				// 坏消息：跳过并提交，避免卡死 partition。
				_ = commit(parentCtx)
				slog.Default().Warn("skip malformed bid command", "error", err)
				w.observe("malformed")
				w.observeCommitLag(fetchedAt)
				continue
			}
			if parentCtx.Err() != nil {
				break
			}
			slog.Default().Warn("fetch bid command failed", "error", err)
			w.observe("fetch_error")
			if !sleepContext(parentCtx, w.retryBackoff) {
				break
			}
			continue
		}

		// 信号量限并发：达到 PoolSize 上限时 fetch 阻塞，防止内存爆炸。
		acquired := false
		select {
		case sem <- struct{}{}:
			acquired = true
		case <-parentCtx.Done():
		}
		if !acquired {
			// shutdown：放弃这一条 fetch 出来的命令；不再调 commit，
			// 由 Lua idem_key 兜底保证下次 rebalance 重放也不会重复生效。
			break
		}

		wg.Add(1)
		go func(cmd kafkainfra.BidCommand, commit func(context.Context) error, fetchedAt time.Time) {
			defer wg.Done()
			defer func() { <-sem }()
			defer func() {
				if r := recover(); r != nil {
					slog.Default().Error("bid decision worker panic recovered",
						"auction_id", cmd.AuctionID, "bid_id", cmd.BidID,
						"panic", r, "stack", string(debug.Stack()))
					w.observe("panic")
				}
			}()
			defer w.releaseInFlight(cmd)
			if w.metrics != nil {
				w.metrics.IncBidWorkerPoolInflight()
				defer w.metrics.DecBidWorkerPoolInflight()
			}
			w.handle(parentCtx, cmd)
			if commit != nil {
				w.dispatchCommit(parentCtx, commitCh, commitJob{commit: commit, fetched: fetchedAt})
			}
		}(cmd, commit, fetchedAt)
	}

	// 优雅关闭：
	// 1) 等待已派发的 in-flight goroutine 完成（最多 shutdownGrace）。
	// 2) 关闭 commitCh，等待 committer flush 剩余 commit。
	// 3) 超时则放弃，重启后由 Lua idem_key 兜底。
	graceCtx, graceCancel := context.WithTimeout(context.Background(), w.shutdownGrace)
	defer graceCancel()
	doneWG := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneWG)
	}()
	select {
	case <-doneWG:
	case <-graceCtx.Done():
		slog.Default().Warn("bid decision worker shutdown timeout, in-flight goroutines leaked",
			"grace", w.shutdownGrace)
	}
	if commitCh != nil {
		close(commitCh)
		// committer 自己在 channel 关闭后退出。
		select {
		case <-committerDone:
		case <-graceCtx.Done():
		}
	}
}

func (w *BidDecisionWorker) releaseInFlight(cmd kafkainfra.BidCommand) {
	if w == nil || w.inFlight == nil || cmd.AuctionID == 0 || strings.TrimSpace(cmd.BidID) == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := w.inFlight.ReleaseBidCommand(ctx, cmd.AuctionID, cmd.BidID); err != nil {
		slog.Default().Warn("release bid command in-flight failed",
			"auction_id", cmd.AuctionID, "bid_id", cmd.BidID, "error", err)
	}
}

// dispatchCommit 把 commit job 投递给 batch committer，或在 single 模式下立即执行。
// 关闭中（commitCh==nil 或被关闭）时同步执行确保 at-least-once。
func (w *BidDecisionWorker) dispatchCommit(ctx context.Context, commitCh chan<- commitJob, job commitJob) {
	if w.commitMode != "batch" || commitCh == nil {
		// single 模式：派发 goroutine 直接 commit，保留与历史行为一致的语义。
		_ = job.commit(ctx)
		w.observeCommitLag(job.fetched)
		return
	}
	defer func() {
		// commitCh 关闭时 send 会 panic；fallback 到同步 commit 兜底。
		if r := recover(); r != nil {
			_ = job.commit(ctx)
			w.observeCommitLag(job.fetched)
		}
	}()
	select {
	case commitCh <- job:
	case <-ctx.Done():
		// 关闭路径：直接同步 commit 一次，让 offset 落地。
		_ = job.commit(context.Background())
		w.observeCommitLag(job.fetched)
	}
}

// committerLoop 顺序消费 commitCh：每 BatchSize 条或每 MaxLatency 触发一次
// flush；channel 关闭后 flush 剩余项后退出。
//
// 注意：当前 BidCommandConsumer 接口仅暴露 per-msg commit 闭包（每个闭包内部
// 调 segmentio kafka-go Reader.CommitMessages(msg)），committer 顺序串行调用，
// 等价于按到达顺序攒批一次性提交。segmentio 内部按 offset 推进，commit 顺序
// 保持不变；批量 commit 失败时下次 rebalance 重放，由 Lua idem_key（30s TTL）
// 兜底保证不重复生效。
func (w *BidDecisionWorker) committerLoop(parentCtx context.Context, ch <-chan commitJob, done chan<- struct{}) {
	defer close(done)
	batch := make([]commitJob, 0, w.commitBatchN)
	timer := time.NewTimer(w.commitMaxWait)
	defer timer.Stop()
	resetTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(w.commitMaxWait)
	}

	flush := func() {
		if len(batch) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		for _, job := range batch {
			if err := job.commit(ctx); err != nil {
				slog.Default().Warn("bid command commit failed (batch)", "error", err)
			}
			w.observeCommitLag(job.fetched)
		}
		cancel()
		batch = batch[:0]
		resetTimer()
	}

	for {
		select {
		case job, ok := <-ch:
			if !ok {
				flush()
				return
			}
			batch = append(batch, job)
			if len(batch) >= w.commitBatchN {
				flush()
			}
		case <-timer.C:
			flush()
			resetTimer()
		case <-parentCtx.Done():
			// drain 剩余 channel 内容后退出（由 close(commitCh) 触发上面的 ok==false 分支）。
		}
	}
}

func (w *BidDecisionWorker) lagSamplerLoop(ctx context.Context, src BidPartitionLagSource) {
	t := time.NewTicker(w.lagPollPeriod)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			lags := src.PartitionLag()
			for partition, lag := range lags {
				w.metrics.SetBidKafkaPartitionLag(partition, lag)
			}
		}
	}
}

func (w *BidDecisionWorker) handle(parentCtx context.Context, cmd kafkainfra.BidCommand) {
	ctx := tracing.ExtractMap(parentCtx, map[string]string{
		"traceparent": cmd.TraceParent,
		"tracestate":  cmd.TraceState,
	})
	ctx, span := tracing.StartSpan(ctx, "bid_decision.arbitrate",
		attribute.Int64("auction.id", int64(cmd.AuctionID)),
		attribute.String("bid.id", cmd.BidID),
	)
	defer span.End()

	start := time.Now()
	result, err := w.arbitrator.ArbitrateFromCommand(ctx, toSnapshot(cmd))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		w.observeDecision(time.Since(start), "error")
		w.observeOutcome("error")
		// 裁决出错：推一帧 REJECTED 终态，避免前端无限等待。
		w.deliver(cmd, corews.BidResultPayload{
			BidID:        cmd.BidID,
			AuctionID:    cmd.AuctionID,
			FinalStatus:  "REJECTED",
			Reason:       "ARBITRATION_ERROR",
			ServerTimeMS: time.Now().UTC().UnixMilli(),
		})
		return
	}
	finalStatus, outcome := mapDecisionStatus(result)
	w.observeDecision(time.Since(start), strings.ToLower(finalStatus))
	w.observeOutcome(outcome)
	span.SetAttributes(attribute.String("bid.final_status", finalStatus))

	serverTimeMS := time.Now().UTC().UnixMilli()
	if result.ServerTime != nil {
		serverTimeMS = result.ServerTime.UTC().UnixMilli()
	}
	w.deliver(cmd, corews.BidResultPayload{
		BidID:          cmd.BidID,
		AuctionID:      cmd.AuctionID,
		FinalStatus:    finalStatus,
		Reason:         result.Reason,
		CurrentPrice:   result.CurrentPrice,
		LeaderBidderID: result.LeaderBidderID,
		EndTimeMS:      result.EndTime.UTC().UnixMilli(),
		ServerTimeMS:   serverTimeMS,
		ResultSeq:      result.Seq,
	})
}

func (w *BidDecisionWorker) deliver(cmd kafkainfra.BidCommand, p corews.BidResultPayload) {
	if w.delivery == nil {
		return
	}
	start := time.Now()
	w.delivery.DeliverBidResult(cmd.LiveSessionID, cmd.AuctionID, cmd.UserID, p)
	w.observePush(time.Since(start))
}

// mapDecisionStatus 映射裁决结果到终态。Duplicate 命中也推终态（保持一致）：
// 已接受的重复 → ACCEPTED，否则 REJECTED。reason 取 Lua 真实拒因。
func mapDecisionStatus(result domain.BidResult) (finalStatus, outcome string) {
	if result.Accepted {
		return "ACCEPTED", "accepted"
	}
	if result.Duplicate {
		return "REJECTED", "duplicate"
	}
	return "REJECTED", "rejected"
}

// toSnapshot 把 kafka 命令转换为 auction app 的命令快照。
func toSnapshot(cmd kafkainfra.BidCommand) auctionapp.BidCommandSnapshot {
	var rule domain.IncrementRule
	if len(cmd.IncrementRule) > 0 {
		if parsed, err := domain.ParseIncrementRule(json.RawMessage(cmd.IncrementRule)); err == nil {
			rule = parsed
		}
	}
	return auctionapp.BidCommandSnapshot{
		BidID:                cmd.BidID,
		AuctionID:            cmd.AuctionID,
		LiveSessionID:        cmd.LiveSessionID,
		UserID:               cmd.UserID,
		SellerID:             cmd.SellerID,
		Price:                cmd.Price,
		ExpectedCurrentPrice: cmd.ExpectedCurrentPrice,
		Source:               cmd.Source,
		MinIncrement:         cmd.MinIncrement,
		AntiSnipingMS:        cmd.AntiSnipingMS,
		AntiExtendMS:         cmd.AntiExtendMS,
		AntiExtendMode:       domain.NormalizeAuctionExtendMode(domain.AuctionExtendMode(cmd.AntiExtendMode)),
		MaxExtendCount:       cmd.MaxExtendCount,
		FreqLimitCount:       cmd.FreqLimitCount,
		FreqWindowMS:         cmd.FreqWindowMS,
		StartPrice:           cmd.StartPrice,
		CapPrice:             cmd.CapPrice,
		IncrementRule:        rule,
		BidderNickname:       cmd.BidderNickname,
		BidderAvatarURL:      cmd.BidderAvatarURL,
	}
}

func (w *BidDecisionWorker) observe(result string) {
	if w != nil && w.metrics != nil {
		w.metrics.IncWorkerTask("bid_decision", result)
	}
}

func (w *BidDecisionWorker) observeDecision(elapsed time.Duration, result string) {
	if w != nil && w.metrics != nil {
		w.metrics.ObserveBidDecisionDuration(result, elapsed)
	}
}

func (w *BidDecisionWorker) observeOutcome(outcome string) {
	if w != nil && w.metrics != nil {
		w.metrics.IncBidDecisionOutcome(outcome)
	}
}

func (w *BidDecisionWorker) observePush(elapsed time.Duration) {
	if w != nil && w.metrics != nil {
		w.metrics.ObserveBidResultPush(elapsed)
	}
}

func (w *BidDecisionWorker) observeCommitLag(fetched time.Time) {
	if w == nil || w.metrics == nil || fetched.IsZero() {
		return
	}
	w.metrics.ObserveBidWorkerCommitLag(time.Since(fetched))
}

package runtime

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
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

// BidDecisionWorker 从 aieas.bid.commands 顺序消费命令，调用 ArbitrateFromCommand
// 复用 Lua 裁决，映射 finalStatus（ACCEPTED|REJECTED），并通过协调器定向推送
// bid.result。单 partition 单 goroutine、partition 内不再并发，保证同 auction 顺序。
type BidDecisionWorker struct {
	consumer     BidCommandConsumer
	arbitrator   BidArbitrator
	delivery     BidResultDelivery
	retryBackoff time.Duration
	metrics      *metrics.Registry
}

func NewBidDecisionWorker(consumer BidCommandConsumer, arbitrator BidArbitrator, delivery BidResultDelivery) *BidDecisionWorker {
	return &BidDecisionWorker{
		consumer:     consumer,
		arbitrator:   arbitrator,
		delivery:     delivery,
		retryBackoff: time.Second,
	}
}

// SetMetrics 注入裁决 worker 指标。nil 安全。
func (w *BidDecisionWorker) SetMetrics(reg *metrics.Registry) {
	if w == nil {
		return
	}
	w.metrics = reg
}

func (w *BidDecisionWorker) Start(ctx context.Context) {
	if w == nil || w.consumer == nil || w.arbitrator == nil {
		return
	}
	go w.loop(ctx)
}

func (w *BidDecisionWorker) loop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		cmd, commit, err := w.consumer.FetchBidCommand(ctx)
		if err != nil {
			if commit != nil {
				// 坏消息：跳过并提交，避免卡死 partition。
				_ = commit(ctx)
				slog.Default().Warn("skip malformed bid command", "error", err)
				w.observe("malformed")
				continue
			}
			if ctx.Err() != nil {
				return
			}
			slog.Default().Warn("fetch bid command failed", "error", err)
			w.observe("fetch_error")
			if !sleepContext(ctx, w.retryBackoff) {
				return
			}
			continue
		}
		w.handle(ctx, cmd)
		if commit != nil {
			_ = commit(ctx)
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

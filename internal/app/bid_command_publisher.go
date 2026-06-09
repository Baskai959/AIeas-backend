package app

import (
	"context"
	"encoding/json"

	kafkainfra "aieas_backend/internal/infra/kafka"
	auctionapp "aieas_backend/internal/modules/auction/app"
)

// publishRejectMetrics 是 publisher 闸门拒绝出价命令时的可选埋点接口。
// 由 *metrics.Registry 实现；nil 安全。
type publishRejectMetrics interface {
	IncBidCommandPublishReject(reason string)
}

type bidCommandInFlightTracker interface {
	TrackBidCommand(ctx context.Context, auctionID uint64, bidID string) error
	ReleaseBidCommand(ctx context.Context, auctionID uint64, bidID string) error
	PendingForAuction(auctionID uint64) int
}

// kafkaBidCommandPublisher 把 auction app 的 BidCommandSnapshot 适配为 kafka.BidCommand
// 并投递到命令流。Route X 下 Kafka key 使用 bid/request 维度而非 auctionId；
// 同拍品正确性由 Redis Lua / idem key 保证。producer 为 nil 时不应构造本适配器（强制走同步降级）。
//
// gate 为可选 publisher 闸门：当 auction 进入 HAMMER_PENDING 时，
// gate.IsClosed(auctionID) 返回 true，本适配器直接拒绝该 auctionId 的新命令，返回
// auctionapp.ErrHammerPending；ws handler 异步分支收到该 sentinel 回 REJECTED。
type kafkaBidCommandPublisher struct {
	producer *kafkainfra.Producer
	gate     *auctionapp.HammerPublisherGate
	tracker  bidCommandInFlightTracker
	metrics  publishRejectMetrics
}

func newKafkaBidCommandPublisher(producer *kafkainfra.Producer) *kafkaBidCommandPublisher {
	if producer == nil {
		return nil
	}
	return &kafkaBidCommandPublisher{producer: producer}
}

// SetGate 注入 publisher 闸门。装配期由 server 调用。nil 表示不启用闸门保护（同步模式）。
func (p *kafkaBidCommandPublisher) SetGate(gate *auctionapp.HammerPublisherGate) {
	if p == nil {
		return
	}
	p.gate = gate
}

func (p *kafkaBidCommandPublisher) SetInFlightTracker(tracker bidCommandInFlightTracker) {
	if p == nil {
		return
	}
	p.tracker = tracker
}

// SetMetrics 注入拒绝埋点。nil 安全。
func (p *kafkaBidCommandPublisher) SetMetrics(m publishRejectMetrics) {
	if p == nil {
		return
	}
	p.metrics = m
}

func (p *kafkaBidCommandPublisher) PublishBidCommand(ctx context.Context, cmd auctionapp.BidCommandSnapshot) error {
	if p == nil {
		return nil
	}
	// 闸门检查必须先于 producer nil 检查，确保即使没有 producer 仍然能断言 HAMMER_PENDING 拒绝。
	if p.gate != nil && p.gate.IsClosed(cmd.AuctionID) {
		if p.metrics != nil {
			p.metrics.IncBidCommandPublishReject("hammer_pending")
		}
		return auctionapp.ErrHammerPending
	}
	if p.producer == nil {
		return nil
	}
	if p.tracker != nil {
		if err := p.tracker.TrackBidCommand(ctx, cmd.AuctionID, cmd.BidID); err != nil {
			return err
		}
	}
	var rule json.RawMessage
	if raw, err := json.Marshal(cmd.IncrementRule); err == nil {
		rule = raw
	}
	err := p.producer.PublishBidCommand(ctx, kafkainfra.BidCommand{
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
		AntiExtendMode:       string(cmd.AntiExtendMode),
		MaxExtendCount:       cmd.MaxExtendCount,
		FreqLimitCount:       cmd.FreqLimitCount,
		FreqWindowMS:         cmd.FreqWindowMS,
		StartPrice:           cmd.StartPrice,
		CapPrice:             cmd.CapPrice,
		IncrementRule:        rule,
		BidderNickname:       cmd.BidderNickname,
		BidderAvatarURL:      cmd.BidderAvatarURL,
		PreCheckPassed:       true,
	})
	if err != nil && p.tracker != nil {
		_ = p.tracker.ReleaseBidCommand(context.Background(), cmd.AuctionID, cmd.BidID)
	}
	return err
}

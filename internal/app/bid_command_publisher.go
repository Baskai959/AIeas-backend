package app

import (
	"context"
	"encoding/json"

	kafkainfra "aieas_backend/internal/infra/kafka"
	auctionapp "aieas_backend/internal/modules/auction/app"
)

// kafkaBidCommandPublisher 把 auction app 的 BidCommandSnapshot 适配为 kafka.BidCommand
// 并投递到命令流（key=auctionId）。producer 为 nil 时不应构造本适配器（强制走同步降级）。
type kafkaBidCommandPublisher struct {
	producer *kafkainfra.Producer
}

func newKafkaBidCommandPublisher(producer *kafkainfra.Producer) *kafkaBidCommandPublisher {
	if producer == nil {
		return nil
	}
	return &kafkaBidCommandPublisher{producer: producer}
}

func (p *kafkaBidCommandPublisher) PublishBidCommand(ctx context.Context, cmd auctionapp.BidCommandSnapshot) error {
	if p == nil || p.producer == nil {
		return nil
	}
	var rule json.RawMessage
	if raw, err := json.Marshal(cmd.IncrementRule); err == nil {
		rule = raw
	}
	return p.producer.PublishBidCommand(ctx, kafkainfra.BidCommand{
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
}

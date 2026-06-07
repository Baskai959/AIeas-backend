package app

import (
	"context"
	"encoding/json"
	"time"

	"aieas_backend/internal/domain"
)

// AuctionSnapshotCache 保存出价热路径所需的拍品运行期快照。
// 生产环境由 RedisCache 承载，BidService 在本地缓存 miss 后优先读取它，避免回源 MySQL。
type AuctionSnapshotCache interface {
	Get(ctx context.Context, auctionID uint64) (AuctionRuntimeSnapshot, string, bool, error)
	Set(ctx context.Context, snapshot AuctionRuntimeSnapshot, ttl time.Duration) error
	Invalidate(ctx context.Context, auctionID uint64) error
}

// AuctionRuntimeSnapshot 是拍品开拍后对出价链路稳定可用的业务字段快照。
type AuctionRuntimeSnapshot struct {
	AuctionID      uint64                   `json:"auctionId"`
	SellerID       string                   `json:"sellerId"`
	LiveSessionID  uint64                   `json:"liveSessionId,omitempty"`
	StartPrice     int64                    `json:"startPrice"`
	CapPrice       int64                    `json:"capPrice"`
	IncrementRule  json.RawMessage          `json:"incrementRule,omitempty"`
	AntiSnipingSec int                      `json:"antiSnipingSec"`
	AntiExtendSec  int                      `json:"antiExtendSec"`
	AntiExtendMode domain.AuctionExtendMode `json:"antiExtendMode"`
	Status         domain.AuctionStatus     `json:"status"`
	StartTime      time.Time                `json:"startTime"`
	EndTime        time.Time                `json:"endTime"`
}

func AuctionRuntimeSnapshotFromLot(auction domain.AuctionLot) AuctionRuntimeSnapshot {
	snapshot := AuctionRuntimeSnapshot{
		AuctionID:      auction.AuctionID,
		SellerID:       auction.SellerID,
		StartPrice:     auction.StartPrice,
		CapPrice:       auction.CapPrice,
		IncrementRule:  append(json.RawMessage(nil), auction.IncrementRule...),
		AntiSnipingSec: auction.AntiSnipingSec,
		AntiExtendSec:  auction.AntiExtendSec,
		AntiExtendMode: domain.NormalizeAuctionExtendMode(auction.AntiExtendMode),
		Status:         auction.Status,
		StartTime:      auction.StartTime,
		EndTime:        auction.EndTime,
	}
	if auction.LiveSessionID != nil {
		snapshot.LiveSessionID = *auction.LiveSessionID
	}
	return snapshot
}

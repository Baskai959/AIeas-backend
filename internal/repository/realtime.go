package repository

import (
	"context"

	"aieas_backend/internal/domain"
)

// AuctionRealtimeStore 是 RT 路径上拍卖状态 / 出价 / 落槌 / 排名的抽象。
//
// v2 起黑名单不再走 RT：黑名单由 MySQL（source of truth）+ LayeredCache 提供，
// 在 service 层（BidService / DepositService）作为前置门面拦截，不再下沉到 Lua。
// 这样可以减少每次出价时 RT 的 SISMEMBER 调用，并把跨 shard 共享的 user 黑名单
// key 从 RT 移除（避免把全局黑名单复制到每个 shard）。
type AuctionRealtimeStore interface {
	InitAuction(ctx context.Context, auction domain.AuctionLot, minIncrement int64) (domain.AuctionState, error)
	GetAuctionState(ctx context.Context, auctionID uint64) (domain.AuctionState, bool, error)
	MarkEnrollment(ctx context.Context, auctionID uint64, userID string) error
	PlaceBid(ctx context.Context, input domain.BidInput) (domain.BidResult, error)
	Hammer(ctx context.Context, input domain.HammerInput) (domain.HammerResult, error)
	TopN(ctx context.Context, auctionID uint64, limit int) ([]domain.RankingEntry, error)
}

type NoopRealtimeStore struct{}

func (NoopRealtimeStore) InitAuction(ctx context.Context, auction domain.AuctionLot, minIncrement int64) (domain.AuctionState, error) {
	_ = ctx
	_ = minIncrement
	state := domain.AuctionState{
		AuctionID:     auction.AuctionID,
		Status:        auction.Status,
		StartPrice:    auction.StartPrice,
		CapPrice:      auction.CapPrice,
		IncrementRule: append([]byte(nil), auction.IncrementRule...),
		CurrentPrice:  auction.StartPrice,
		StartTime:     auction.StartTime,
		EndTime:       auction.EndTime,
		Source:        "db",
	}
	if auction.LiveSessionID != nil {
		state.LiveSessionID = *auction.LiveSessionID
	}
	return state, nil
}

func (NoopRealtimeStore) GetAuctionState(ctx context.Context, auctionID uint64) (domain.AuctionState, bool, error) {
	_ = ctx
	_ = auctionID
	return domain.AuctionState{}, false, nil
}

func (NoopRealtimeStore) MarkEnrollment(ctx context.Context, auctionID uint64, userID string) error {
	_ = ctx
	_ = auctionID
	_ = userID
	return nil
}

func (NoopRealtimeStore) PlaceBid(ctx context.Context, input domain.BidInput) (domain.BidResult, error) {
	_ = ctx
	_ = input
	return domain.BidResult{}, domain.ErrInvalidState
}

func (NoopRealtimeStore) Hammer(ctx context.Context, input domain.HammerInput) (domain.HammerResult, error) {
	_ = ctx
	_ = input
	return domain.HammerResult{}, domain.ErrInvalidState
}

func (NoopRealtimeStore) TopN(ctx context.Context, auctionID uint64, limit int) ([]domain.RankingEntry, error) {
	_ = ctx
	_ = auctionID
	_ = limit
	return []domain.RankingEntry{}, nil
}

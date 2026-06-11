package app

import (
	"context"

	"aieas_backend/internal/domain"
)

// MarketplaceUseCase 暴露商城/搜索模块对 HTTP transport 的最小应用边界。
type MarketplaceUseCase interface {
	SearchLots(ctx context.Context, filter domain.AuctionSearchFilter) ([]domain.AuctionLot, int64, error)
	GetLot(ctx context.Context, id uint64) (domain.AuctionLot, error)
	MyParticipations(ctx context.Context, userID string, role domain.Role, limit, offset int) ([]domain.AuctionParticipationRecord, error)
	Categories(ctx context.Context) []domain.Category
	SearchMerchants(ctx context.Context, viewerID string, viewerRole domain.Role, keyword string, limit, offset int) ([]domain.MerchantView, error)
	GetMerchant(ctx context.Context, viewerID string, viewerRole domain.Role, merchantID string) (domain.MerchantView, error)
	FollowMerchant(ctx context.Context, buyerID string, role domain.Role, merchantID string) (domain.MerchantView, error)
	UnfollowMerchant(ctx context.Context, buyerID string, role domain.Role, merchantID string) (domain.MerchantView, error)
	MyFollowedMerchants(ctx context.Context, buyerID string, role domain.Role, limit, offset int) ([]domain.FollowedMerchant, int64, error)
}

// LiveSessionPresenter 暴露商城模块对直播场次视图组装的边界。
type LiveSessionPresenter interface {
	LiveSessionView(ctx context.Context, session domain.LiveSession) domain.LiveSessionView
}

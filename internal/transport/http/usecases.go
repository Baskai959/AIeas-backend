package http

import (
	"context"
	"errors"
	"io"

	adminapp "aieas_backend/internal/modules/admin/app"
	aiapp "aieas_backend/internal/modules/ai/app"
	aiports "aieas_backend/internal/modules/ai/ports"
	auctionapp "aieas_backend/internal/modules/auction/app"
	authapp "aieas_backend/internal/modules/auth/app"
	depositapp "aieas_backend/internal/modules/deposit/app"
	liveanalysisapp "aieas_backend/internal/modules/live_analysis/app"
	livesessionapp "aieas_backend/internal/modules/live_session/app"
	livesessionports "aieas_backend/internal/modules/live_session/ports"
	marketplaceapp "aieas_backend/internal/modules/marketplace/app"
	orderapp "aieas_backend/internal/modules/order/app"
	riskapp "aieas_backend/internal/modules/risk/app"
)

var (
	ErrImageStorageDisabled  = errors.New("image storage disabled")
	ErrInvalidImageObjectKey = errors.New("invalid image object key")
	ErrImageObjectNotFound   = errors.New("image object not found")
)

const ImageProxyPathPrefix = "/api/v1/images/"

type ImageUploadInput struct {
	Filename    string
	ContentType string
	Size        int64
	Body        io.Reader
}

type ImageDownloadOutput struct {
	Content       io.ReadCloser
	ContentType   string
	ContentLength int64
}

type ImageUploader interface {
	Upload(ctx context.Context, in ImageUploadInput) (string, error)
	Download(ctx context.Context, key string) (ImageDownloadOutput, error)
}

type DisabledImageUploader struct{}

func (DisabledImageUploader) Upload(ctx context.Context, in ImageUploadInput) (string, error) {
	_ = ctx
	_ = in
	return "", ErrImageStorageDisabled
}

func (DisabledImageUploader) Download(ctx context.Context, key string) (ImageDownloadOutput, error) {
	_ = ctx
	_ = key
	return ImageDownloadOutput{}, ErrImageStorageDisabled
}

// 过渡期 alias：auth 专属接口已迁到 modules/auth/app。
// TODO(auth boundary): 待旧 handler 依赖全部收敛后删除 transport 层 alias。
type AuthUseCase = authapp.AuthUseCase
type AuthUpdateProfileInput = authapp.UpdateProfileInput

// 过渡期 alias：auction 专属接口已迁到 modules/auction/app。
// TODO(auction phase4): 待旧 handler 依赖全部收敛后删除 transport 层 alias。
type AuctionCommandUseCase = auctionapp.AuctionCommandUseCase
type AuctionQueryUseCase = auctionapp.AuctionQueryUseCase
type AuctionUseCase = auctionapp.AuctionUseCase
type BidUseCase = auctionapp.BidUseCase
type HammerUseCase = auctionapp.HammerUseCase
type PlaceBidInput = auctionapp.PlaceBidInput

// 过渡期 alias：deposit 专属接口已迁到 modules/deposit/app。
// TODO(deposit boundary): 待旧 handler 依赖全部收敛后删除 transport 层 alias。
type DepositUseCase = depositapp.DepositUseCase
type DepositEnrollInput = depositapp.EnrollInput

// 过渡期 alias：order 专属接口已迁到 modules/order/app。
// TODO(order phase4): 待旧 handler 依赖全部收敛后删除 transport 层 alias。
type OrderUseCase = orderapp.OrderUseCase

// 过渡期 alias：live_session 专属接口已迁到 modules/live_session/app。
// TODO(live_session phase4): 待旧 handler 依赖全部收敛后删除 transport 层 alias。
type LiveSessionCommandUseCase = livesessionapp.LiveSessionCommandUseCase
type LiveSessionQueryUseCase = livesessionapp.LiveSessionQueryUseCase
type LiveSessionUseCase = livesessionapp.LiveSessionUseCase
type LiveSessionCreateInput = livesessionapp.CreateLiveSessionInput
type LiveSessionUpdateInput = livesessionapp.UpdateLiveSessionInput
type LiveSessionActivateInput = livesessionapp.ActivateLiveSessionAuctionInput

var ErrLiveSessionBusy = livesessionapp.ErrLiveSessionBusy
var ErrLotAlreadyMounted = livesessionapp.ErrLotAlreadyMounted
var ErrLiveSessionLotInvalidState = livesessionapp.ErrLiveSessionLotInvalidState

// 过渡期 alias：admin/support 专属接口已迁到 modules/admin/app。
// TODO(admin boundary): 待旧 handler 依赖全部收敛后删除 transport 层 alias。
type AdminUseCase = adminapp.AdminUseCase

// 过渡期 alias：marketplace 专属接口已迁到 modules/marketplace/app。
// TODO(marketplace boundary): 待旧 handler 依赖全部收敛后删除 transport 层 alias。
type MarketplaceUseCase = marketplaceapp.MarketplaceUseCase
type MarketplaceLiveSessionPresenter = marketplaceapp.LiveSessionPresenter

// 过渡期 alias：live_analysis 专属接口已迁到 modules/live_analysis/app。
// TODO(live_analysis boundary): 待旧 handler 依赖全部收敛后删除 transport 层 alias。
type LiveAnalysisUseCase = liveanalysisapp.LiveAnalysisUseCase
type LiveAnalysisCreateReportInput = liveanalysisapp.CreateReportInput
type LiveAnalysisCallbackInput = liveanalysisapp.CallbackInput

// 过渡期 alias：ai 专属接口已迁到 modules/ai/app。
// TODO(ai boundary): 待旧 handler 依赖全部收敛后删除 transport 层 alias。
type AIAssistantUseCase = aiapp.AIAssistantUseCase
type AIAssistantStatusNotifier = aiports.StatusNotifier
type AIAssistantPermissionInput = aiapp.PermissionInput
type AIAssistantPermissionUpdateInput = aiapp.PermissionUpdateInput
type AIAssistantDecisionInput = aiapp.DecisionInput
type AIAssistantEvent = aiapp.Event
type ProductDescriptionInput = aiapp.ProductDescriptionInput
type ProductDescriptionResult = aiapp.ProductDescriptionResult
type ProductDescriptionGenerator = aiapp.ProductDescriptionGenerator
type DisabledProductDescriptionGenerator = aiapp.DisabledProductDescriptionGenerator

type AuctionCreateInput = auctionapp.CreateAuctionInput
type AuctionAuditCallbackInput = auctionapp.AuctionAuditCallbackInput
type AuctionUpdateInput = auctionapp.UpdateAuctionInput

// 过渡期 alias：risk 专属接口已迁到 modules/risk/app。
// TODO(risk boundary): 待旧 handler 依赖全部收敛后删除 transport 层 alias。
type RiskControlUseCase = riskapp.RiskControlUseCase

type WSBidUseCase = auctionapp.WSBidUseCase

type WSAuctionRankingUseCase = auctionapp.WSAuctionRankingUseCase

type WSAuctionStateUseCase = auctionapp.WSAuctionStateUseCase

type WSLiveSessionLookupUseCase = livesessionapp.WSLiveSessionLookupUseCase

type WSAuctionRealtimeSnapshotProvider = auctionapp.WSAuctionRealtimeSnapshotProvider

type WSLiveSessionRealtimeReader = livesessionports.LiveSessionRealtimeReader

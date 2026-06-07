package service

import (
	"context"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/observability/tracing"
	auctionapp "aieas_backend/internal/modules/auction/app"
	"aieas_backend/internal/tests/repository"
)

type AuctionService = auctionapp.AuctionService
type AuctionIDGenerator = auctionapp.AuctionIDGenerator
type CreateAuctionInput = auctionapp.CreateAuctionInput
type UpdateAuctionInput = auctionapp.UpdateAuctionInput
type AuctionAuditCallbackInput = auctionapp.AuctionAuditCallbackInput
type AuctionAuditCallbackResult = auctionapp.AuctionAuditCallbackResult
type AuctionSnapshotCache = auctionapp.AuctionSnapshotCache
type AuctionRuntimeSnapshot = auctionapp.AuctionRuntimeSnapshot

func AuctionRuntimeSnapshotFromLot(auction domain.AuctionLot) AuctionRuntimeSnapshot {
	return auctionapp.AuctionRuntimeSnapshotFromLot(auction)
}

type AuctionServiceDeps struct {
	Auctions         repository.AuctionRepository
	Bids             repository.BidRepository
	Tx               repository.TxManager
	Realtime         repository.AuctionRealtimeStore
	Publisher        EventPublisher
	Timer            auctionapp.AuctionTimer
	IDGen            AuctionIDGenerator
	ProductAuditor   auctionapp.ProductAuditor
	AuditImageLoader auctionapp.ProductAuditImageLoader
	LiveAgentHook    auctionapp.AuctionLiveAgentHook
	OnClose          func(ctx context.Context, auctionID uint64)
	AuctionConfig    appconfig.AuctionConfig
	ProductAuditOn   bool
	ProductAuditSet  bool
	AuctionSnapshots AuctionSnapshotCache
	Tracing          *tracing.Provider
}

func NewAuctionService(auctions repository.AuctionRepository, tx repository.TxManager) *AuctionService {
	return auctionapp.NewAuctionService(auctions, tx)
}

func NewAuctionServiceWithDeps(deps AuctionServiceDeps) *AuctionService {
	return auctionapp.NewAuctionServiceWithDeps(auctionapp.AuctionServiceDeps{
		Auctions:         deps.Auctions,
		Bids:             deps.Bids,
		Tx:               deps.Tx,
		Realtime:         deps.Realtime,
		Publisher:        auctionEventPublisherAdapter{publisher: deps.Publisher},
		Timer:            deps.Timer,
		IDGen:            deps.IDGen,
		ProductAuditor:   deps.ProductAuditor,
		AuditImageLoader: deps.AuditImageLoader,
		LiveAgentHook:    deps.LiveAgentHook,
		OnClose:          deps.OnClose,
		AuctionConfig:    deps.AuctionConfig,
		ProductAuditOn:   deps.ProductAuditOn,
		ProductAuditSet:  deps.ProductAuditSet,
		AuctionSnapshots: deps.AuctionSnapshots,
		Tracer:           auctionTracerAdapter{provider: deps.Tracing},
	})
}

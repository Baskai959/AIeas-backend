package service

import (
	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/observability/metrics"
	"aieas_backend/internal/infra/observability/tracing"
	adminports "aieas_backend/internal/modules/admin/ports"
	auctionapp "aieas_backend/internal/modules/auction/app"
	"aieas_backend/internal/tests/repository"

	appconfig "aieas_backend/internal/config"
)

type BidService = auctionapp.BidService
type PlaceBidInput = auctionapp.PlaceBidInput

type BidServiceDeps struct {
	Bids             repository.BidRepository
	Auctions         repository.AuctionRepository
	Realtime         repository.AuctionRealtimeStore
	Risk             auctionapp.BidRiskService
	Publisher        EventPublisher
	Hammer           auctionapp.BidHammer
	Sessions         auctionapp.BidSessionCounterWriter
	Config           appconfig.AuctionConfig
	Metrics          *metrics.Registry
	Tracing          *tracing.Provider
	LiveAgentHook    auctionapp.BidLiveAgentHook
	Configs          adminports.ConfigRepository
	RiskControls     auctionapp.BidRiskControlProvider
	Users            auctionapp.BidUserReader
	AuctionSnapshots AuctionSnapshotCache
}

type bidAuctionSnapshot = auctionapp.BidAuctionSnapshot

const samePriceInflightLimit = auctionapp.SamePriceInflightLimit
const samePriceGateIdleTTL = auctionapp.SamePriceGateIdleTTL
const bidRealtimeStateCacheTTL = auctionapp.BidRealtimeStateCacheTTL

func NewBidService(bids repository.BidRepository, auctions repository.AuctionRepository, realtime repository.AuctionRealtimeStore, risk *RiskService, publisher EventPublisher, cfg appconfig.AuctionConfig) *BidService {
	return auctionapp.NewBidService(bids, auctions, realtime, risk, auctionEventPublisherAdapter{publisher: publisher}, cfg)
}

func NewBidServiceWithDeps(deps BidServiceDeps) *BidService {
	if deps.Realtime == nil {
		deps.Realtime = repository.NoopRealtimeStore{}
	}
	return auctionapp.NewBidServiceWithDeps(auctionapp.BidServiceDeps{
		Bids:             deps.Bids,
		Auctions:         deps.Auctions,
		Realtime:         deps.Realtime,
		Risk:             deps.Risk,
		Publisher:        auctionEventPublisherAdapter{publisher: deps.Publisher},
		Hammer:           deps.Hammer,
		Sessions:         deps.Sessions,
		Config:           deps.Config,
		Metrics:          deps.Metrics,
		Tracer:           auctionTracerAdapter{provider: deps.Tracing},
		LiveAgentHook:    deps.LiveAgentHook,
		Configs:          deps.Configs,
		RiskControls:     deps.RiskControls,
		Users:            deps.Users,
		AuctionSnapshots: deps.AuctionSnapshots,
	})
}

func snapshotFloorPreRejectReason(in PlaceBidInput, state domain.AuctionState, stateOK bool, auction bidAuctionSnapshot, rule domain.IncrementRule) (string, bool) {
	return auctionapp.SnapshotFloorPreRejectReason(in, state, stateOK, auction, rule)
}

func samePriceGateKey(auctionID uint64, expectedCurrentPrice, price int64) string {
	return auctionapp.SamePriceGateKey(auctionID, expectedCurrentPrice, price)
}

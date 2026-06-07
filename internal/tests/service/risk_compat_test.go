package service

import (
	riskapp "aieas_backend/internal/modules/risk/app"
	riskports "aieas_backend/internal/modules/risk/ports"
	"aieas_backend/internal/tests/repository"
	wstransport "aieas_backend/internal/transport/ws"
)

type BlacklistCache = riskports.BlacklistCache
type RiskService = riskapp.RiskService

func NewRiskService(repo repository.RiskRepository, realtime repository.AuctionRealtimeStore, publisher EventPublisher) *RiskService {
	_ = realtime
	return riskapp.NewRiskService(repo, eventPublisherAdapter{publisher: publisher})
}

type eventPublisherAdapter struct {
	publisher EventPublisher
}

func (a eventPublisherAdapter) Broadcast(auctionID uint64, env riskports.EventEnvelope) int {
	if a.publisher == nil {
		return 0
	}
	return a.publisher.Broadcast(auctionID, wstransport.Envelope{
		Type:      env.Type,
		RequestID: env.RequestID,
		Payload:   env.Payload,
	})
}

package service

import (
	"context"

	"aieas_backend/internal/domain"
	livesessionapp "aieas_backend/internal/modules/live_session/app"
	livesessionports "aieas_backend/internal/modules/live_session/ports"
	"aieas_backend/internal/tests/repository"
)

var (
	ErrLiveSessionBusy            = livesessionapp.ErrLiveSessionBusy
	ErrLotAlreadyMounted          = livesessionapp.ErrLotAlreadyMounted
	ErrLiveSessionLotInvalidState = livesessionapp.ErrLiveSessionLotInvalidState
)

type OnlineCounter = livesessionapp.OnlineCounter
type AIAssistantSwitchNotifier = livesessionapp.AIAssistantSwitchNotifier
type LiveSessionLotEventNotifier = livesessionapp.LiveSessionLotEventNotifier

type LiveSessionService = livesessionapp.LiveSessionService
type CreateLiveSessionInput = livesessionapp.CreateLiveSessionInput
type UpdateLiveSessionInput = livesessionapp.UpdateLiveSessionInput
type ActivateLiveSessionAuctionInput = livesessionapp.ActivateLiveSessionAuctionInput
type LiveSessionStats = livesessionapp.LiveSessionStats

type LiveSessionServiceDeps struct {
	Sessions        livesessionports.LiveSessionRepository
	Auctions        livesessionports.AuctionLotRepository
	Tx              repository.TxManager
	Lock            livesessionports.LiveSessionLock
	Auction         *AuctionService
	Bids            livesessionports.BidReader
	Orders          livesessionports.OrderReader
	Users           livesessionports.UserReader
	AuctionRealtime livesessionapp.LiveSessionAuctionRealtimeStore
	OnlineCounter   OnlineCounter
	SessionRealtime livesessionports.LiveSessionRealtimeStore
	OnEnded         func(ctx context.Context, session domain.LiveSession)
	LiveAgentHook   livesessionapp.LiveSessionAgentHook
	LotEvents       LiveSessionLotEventNotifier
	AISwitch        AIAssistantSwitchNotifier
}

func NewLiveSessionService(sessions livesessionports.LiveSessionRepository, auctions livesessionports.AuctionLotRepository) *LiveSessionService {
	return livesessionapp.NewLiveSessionService(sessions, auctions)
}

func NewLiveSessionServiceWithDeps(deps LiveSessionServiceDeps) *LiveSessionService {
	appDeps := livesessionapp.LiveSessionServiceDeps{
		Sessions:        deps.Sessions,
		Auctions:        deps.Auctions,
		Tx:              deps.Tx,
		Lock:            deps.Lock,
		Auction:         deps.Auction,
		Bids:            deps.Bids,
		Orders:          deps.Orders,
		Users:           deps.Users,
		AuctionRealtime: deps.AuctionRealtime,
		OnlineCounter:   deps.OnlineCounter,
		SessionRealtime: deps.SessionRealtime,
		OnEnded:         deps.OnEnded,
		LotEvents:       deps.LotEvents,
		AISwitch:        deps.AISwitch,
	}
	appDeps.LiveAgentHook = deps.LiveAgentHook
	return livesessionapp.NewLiveSessionServiceWithDeps(appDeps)
}

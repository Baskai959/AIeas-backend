package service

import (
	"context"
	"errors"
	"strings"
	"time"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

type BidService struct {
	bids      repository.BidRepository
	auctions  repository.AuctionRepository
	realtime  repository.AuctionRealtimeStore
	risk      *RiskService
	publisher EventPublisher
	cfg       appconfig.AuctionConfig
}

type PlaceBidInput struct {
	RequestID string
	AuctionID uint64
	BidderID  string
	UserRole  domain.Role
	Price     int64
	Source    string
}

func NewBidService(bids repository.BidRepository, auctions repository.AuctionRepository, realtime repository.AuctionRealtimeStore, risk *RiskService, publisher EventPublisher, cfg appconfig.AuctionConfig) *BidService {
	if realtime == nil {
		realtime = repository.NoopRealtimeStore{}
	}
	if cfg.MinIncrementCent <= 0 {
		cfg.MinIncrementCent = 1
	}
	if cfg.AntiSnipeMs <= 0 {
		cfg.AntiSnipeMs = 30000
	}
	if cfg.ExtendMs <= 0 {
		cfg.ExtendMs = 30000
	}
	if cfg.MaxExtendCount <= 0 {
		cfg.MaxExtendCount = 20
	}
	if cfg.FreqLimitCount <= 0 {
		cfg.FreqLimitCount = 10
	}
	if cfg.FreqWindowMs <= 0 {
		cfg.FreqWindowMs = 1000
	}
	return &BidService{bids: bids, auctions: auctions, realtime: realtime, risk: risk, publisher: publisher, cfg: cfg}
}

func (s *BidService) Place(ctx context.Context, in PlaceBidInput) (domain.BidResult, error) {
	in.RequestID = strings.TrimSpace(in.RequestID)
	in.BidderID = strings.TrimSpace(in.BidderID)
	if in.RequestID == "" || in.AuctionID == 0 || in.BidderID == "" || in.Price <= 0 || in.UserRole != domain.RoleBuyer {
		return domain.BidResult{}, domain.ErrInvalidArgument
	}
	if s.bids != nil {
		record, err := s.bids.FindByRequestID(ctx, in.RequestID)
		if err == nil {
			return bidResultFromRecord(record), nil
		}
		if !errors.Is(err, domain.ErrNotFound) {
			return domain.BidResult{}, err
		}
	}
	if result, ok, err := realtimeBidResultByRequestID(ctx, s.realtime, in.AuctionID, in.RequestID); err != nil {
		return domain.BidResult{}, err
	} else if ok {
		result.Duplicate = true
		return result, nil
	}
	auction, err := s.auctions.FindByID(ctx, in.AuctionID)
	if err != nil {
		return domain.BidResult{}, err
	}
	now := time.Now().UTC()
	if in.Source == "" {
		in.Source = "live_ws"
	}
	streamEnabled := bidStreamEnabled(s.realtime)
	blacklisted := false
	if s.risk != nil {
		isBlacklisted, err := s.risk.IsBlacklisted(ctx, in.BidderID)
		if err != nil {
			return domain.BidResult{}, err
		}
		blacklisted = isBlacklisted
		if blacklisted && !streamEnabled {
			result := domain.BidResult{
				RequestID:    in.RequestID,
				AuctionID:    in.AuctionID,
				BidderID:     in.BidderID,
				Price:        in.Price,
				Accepted:     false,
				Reason:       "BLACKLIST",
				CurrentPrice: auction.StartPrice,
				EndTime:      auction.EndTime,
				Event:        "bid.rejected",
				RiskResult:   domain.BidRiskReject,
			}
			s.persistBid(ctx, in, result, now)
			s.publishBidResult(ctx, result)
			return result, nil
		}
	} else if ok, err := s.realtime.IsBlacklisted(ctx, in.BidderID); err != nil {
		return domain.BidResult{}, err
	} else {
		blacklisted = ok
	}
	state, stateOK, err := s.realtime.GetAuctionState(ctx, in.AuctionID)
	if err != nil {
		return domain.BidResult{}, err
	}
	minIncrementPrice := auction.StartPrice
	if stateOK {
		minIncrementPrice = state.CurrentPrice
	}
	minIncrement := domain.MinIncrementForPrice(auction.IncrementRule, minIncrementPrice, s.cfg.MinIncrementCent)
	if stateOK && !blacklisted && (state.Status == domain.AuctionStatusRunning || state.Status == domain.AuctionStatusExtended) {
		prerequisitesReady, err := realtimeBidPrerequisitesReady(ctx, s.realtime, in.AuctionID, in.BidderID)
		if err != nil {
			return domain.BidResult{}, err
		}
		if prerequisitesReady && belowMinimumIncrement(state, in.BidderID, in.Price, minIncrement) {
			result := rejectBidFromState(in, state, "BELOW_MIN_INCREMENT")
			s.persistBid(ctx, in, result, now)
			s.publishBidResult(ctx, result)
			return result, nil
		}
	}
	result, err := s.realtime.PlaceBid(ctx, domain.BidInput{
		RequestID:      in.RequestID,
		AuctionID:      in.AuctionID,
		BidderID:       in.BidderID,
		Price:          in.Price,
		Now:            now,
		Source:         in.Source,
		MinIncrement:   minIncrement,
		AntiSnipingMS:  int64(auction.AntiSnipingSec) * 1000,
		AntiExtendMS:   int64(auction.AntiExtendSec) * 1000,
		MaxExtendCount: s.cfg.MaxExtendCount,
		FreqLimitCount: s.cfg.FreqLimitCount,
		FreqWindowMS:   s.cfg.FreqWindowMs,
		IdempotencyTTL: 24 * time.Hour,
	})
	if err != nil {
		return domain.BidResult{}, err
	}
	if result.RiskResult == "" {
		if result.Accepted {
			result.RiskResult = domain.BidRiskAllow
		} else {
			result.RiskResult = domain.BidRiskReject
		}
	}
	if !streamEnabled {
		s.persistBid(ctx, in, result, now)
	}
	if result.Reason == "FREQ_LIMIT" && s.risk != nil {
		s.risk.RecordEvent(ctx, "BID_FREQ", in.BidderID, in.AuctionID, domain.RiskSeverityMid, result)
	}
	if !streamEnabled {
		s.publishBidResult(ctx, result)
	}
	return result, nil
}

func (s *BidService) TopN(ctx context.Context, auctionID uint64, limit int) ([]domain.RankingEntry, error) {
	if auctionID == 0 {
		return nil, domain.ErrInvalidArgument
	}
	return s.realtime.TopN(ctx, auctionID, limit)
}

func bidStreamEnabled(realtime repository.AuctionRealtimeStore) bool {
	type streamStore interface{ StreamEnabled() bool }
	store, ok := realtime.(streamStore)
	return ok && store.StreamEnabled()
}

func realtimeBidResultByRequestID(ctx context.Context, realtime repository.AuctionRealtimeStore, auctionID uint64, requestID string) (domain.BidResult, bool, error) {
	type bidResultReader interface {
		BidResultByRequestID(context.Context, uint64, string) (domain.BidResult, bool, error)
	}
	if strings.TrimSpace(requestID) == "" {
		return domain.BidResult{}, false, nil
	}
	reader, ok := realtime.(bidResultReader)
	if !ok {
		return domain.BidResult{}, false, nil
	}
	return reader.BidResultByRequestID(ctx, auctionID, requestID)
}

func realtimeBidPrerequisitesReady(ctx context.Context, realtime repository.AuctionRealtimeStore, auctionID uint64, bidderID string) (bool, error) {
	type prerequisiteReader interface {
		BidPrerequisites(context.Context, uint64, string) (bool, bool, error)
	}
	reader, ok := realtime.(prerequisiteReader)
	if !ok {
		return false, nil
	}
	enrolled, depositReady, err := reader.BidPrerequisites(ctx, auctionID, bidderID)
	if err != nil {
		return false, err
	}
	return enrolled && depositReady, nil
}

func belowMinimumIncrement(state domain.AuctionState, bidderID string, price int64, minIncrement int64) bool {
	if minIncrement <= 0 {
		minIncrement = 1
	}
	tieBid := state.LeaderBidderID != "" && bidderID != state.LeaderBidderID && price == state.CurrentPrice
	return !tieBid && price < state.CurrentPrice+minIncrement
}

func rejectBidFromState(in PlaceBidInput, state domain.AuctionState, reason string) domain.BidResult {
	return domain.BidResult{
		RequestID:      in.RequestID,
		AuctionID:      in.AuctionID,
		BidderID:       in.BidderID,
		Price:          in.Price,
		Accepted:       false,
		Reason:         reason,
		CurrentPrice:   state.CurrentPrice,
		LeaderBidderID: state.LeaderBidderID,
		EndTime:        state.EndTime,
		ExtendCount:    state.ExtendCount,
		Version:        state.Version,
		Event:          "bid.rejected",
		RiskResult:     domain.BidRiskReject,
	}
}

func (s *BidService) persistBid(ctx context.Context, in PlaceBidInput, result domain.BidResult, now time.Time) {
	if s.bids == nil {
		return
	}
	record := domain.BidRecord{
		RequestID:    in.RequestID,
		AuctionID:    in.AuctionID,
		BidderID:     in.BidderID,
		BidPrice:     in.Price,
		BidTSMS:      now.UnixMilli(),
		Source:       in.Source,
		RiskResult:   result.RiskResult,
		RejectReason: result.Reason,
		CreatedAt:    now,
	}
	if result.Accepted {
		record.RiskResult = domain.BidRiskAllow
		record.RejectReason = ""
	}
	if err := s.bids.Create(ctx, &record); err != nil && !errors.Is(err, domain.ErrConflict) {
		return
	}
}

func (s *BidService) publishBidResult(ctx context.Context, result domain.BidResult) {
	if result.Accepted {
		broadcastJSON(s.publisher, result.AuctionID, "bid.accepted", result)
		if ranking, err := s.TopN(ctx, result.AuctionID, 10); err == nil {
			broadcastJSON(s.publisher, result.AuctionID, "ranking.updated", map[string]interface{}{
				"auctionId": result.AuctionID,
				"ranking":   ranking,
			})
		}
		if result.Extended {
			broadcastJSON(s.publisher, result.AuctionID, "timer.extended", map[string]interface{}{
				"auctionId":   result.AuctionID,
				"endTime":     result.EndTime,
				"extendCount": result.ExtendCount,
			})
		}
		return
	}
	broadcastJSON(s.publisher, result.AuctionID, "bid.rejected", result)
}

func bidResultFromRecord(record domain.BidRecord) domain.BidResult {
	accepted := record.RejectReason == ""
	result := domain.BidResult{
		RequestID:  record.RequestID,
		AuctionID:  record.AuctionID,
		BidderID:   record.BidderID,
		Price:      record.BidPrice,
		Accepted:   accepted,
		Duplicate:  true,
		Reason:     record.RejectReason,
		RiskResult: record.RiskResult,
	}
	if accepted {
		result.CurrentPrice = record.BidPrice
		result.Event = "bid.accepted"
	} else {
		result.Event = "bid.rejected"
	}
	return result
}

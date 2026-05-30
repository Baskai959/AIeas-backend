package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

type MCPLiveControlDependencies struct {
	Auctions       repository.AuctionRepository
	Sessions       repository.LiveSessionRepository
	LiveSessionSvc *LiveSessionService
	AuctionSvc     *AuctionService
	HammerSvc      *HammerService
}

type MCPControlService struct {
	auctions   repository.AuctionRepository
	sessions   repository.LiveSessionRepository
	sessionSvc *LiveSessionService
	auctionSvc *AuctionService
	hammerSvc  *HammerService
}

func NewMCPControlService(deps MCPLiveControlDependencies) *MCPControlService {
	return &MCPControlService{
		auctions:   deps.Auctions,
		sessions:   deps.Sessions,
		sessionSvc: deps.LiveSessionSvc,
		auctionSvc: deps.AuctionSvc,
		hammerSvc:  deps.HammerSvc,
	}
}

type MCPLiveControlContext struct {
	MerchantID          string                      `json:"merchantId"`
	Session             *domain.LiveSession         `json:"session,omitempty"`
	Stats               LiveSessionStats            `json:"stats"`
	CurrentAuctionState *MCPLiveCurrentAuctionState `json:"currentAuctionState,omitempty"`
	Lots                MCPLiveControlLotState      `json:"lots"`
}

type MCPLiveControlLotState struct {
	ExplainingLot *domain.AuctionLot  `json:"explainingLot,omitempty"`
	SessionLots   []domain.AuctionLot `json:"sessionLots"`
	SoldLots      []domain.AuctionLot `json:"soldLots"`
	UnsoldLots    []domain.AuctionLot `json:"unsoldLots"`
	UpcomingLots  []domain.AuctionLot `json:"upcomingLots"`
	CandidateLots []domain.AuctionLot `json:"candidateLots"`
}

type MCPLiveCurrentAuctionState struct {
	AuctionID      uint64               `json:"auctionId"`
	Status         domain.AuctionStatus `json:"status"`
	CurrentPrice   int64                `json:"currentPrice"`
	LeaderBidderID string               `json:"leaderBidderId,omitempty"`
	StartTime      time.Time            `json:"startTime"`
	EndTime        time.Time            `json:"endTime"`
	RemainSeconds  int64                `json:"remainSeconds"`
	LastBidTSMS    int64                `json:"lastBidTsMs,omitempty"`
	ExtendCount    int                  `json:"extendCount,omitempty"`
	Version        int64                `json:"version,omitempty"`
	Source         string               `json:"source,omitempty"`
}

type MCPLiveLotOperationInput struct {
	LiveSessionID uint64
	AuctionID     uint64
	Action        string
	DurationSec   int
	Force         bool
	RequestID     string
}

type MCPLiveLotOperationResult struct {
	Action        string                 `json:"action"`
	LiveSessionID uint64                 `json:"liveSessionId"`
	AuctionID     uint64                 `json:"auctionId"`
	Lot           *domain.AuctionLot     `json:"lot,omitempty"`
	Session       *domain.LiveSession    `json:"session,omitempty"`
	HammerResult  *domain.HammerResult   `json:"hammerResult,omitempty"`
	Order         *domain.OrderDeal      `json:"order,omitempty"`
	Removed       bool                   `json:"removed,omitempty"`
	Context       *MCPLiveControlContext `json:"context,omitempty"`
}

func (s *MCPControlService) ReadMerchantLiveControlContext(ctx context.Context, merchantID string, actor MCPActor) (MCPLiveControlContext, error) {
	if err := requireMCPActor(actor); err != nil {
		return MCPLiveControlContext{}, err
	}
	merchantID = strings.TrimSpace(merchantID)
	if merchantID == "" && actor.Role == domain.RoleMerchant {
		merchantID = actor.ID
	}
	if merchantID == "" || s == nil || s.sessions == nil || s.sessionSvc == nil {
		return MCPLiveControlContext{}, domain.ErrInvalidArgument
	}
	if !canAccessSellerOwned(actor.ID, actor.Role, merchantID) {
		return MCPLiveControlContext{}, domain.ErrForbidden
	}
	session, ok, err := s.currentLiveControlSession(ctx, merchantID)
	if err != nil {
		return MCPLiveControlContext{}, err
	}
	if !ok {
		return MCPLiveControlContext{MerchantID: merchantID}, nil
	}
	return s.buildLiveControlContext(ctx, session, actor)
}

func (s *MCPControlService) OperateLiveSessionLot(ctx context.Context, in MCPLiveLotOperationInput, actor MCPActor) (MCPLiveLotOperationResult, error) {
	if err := requireMCPActor(actor); err != nil {
		return MCPLiveLotOperationResult{}, err
	}
	if s == nil || s.sessions == nil || s.sessionSvc == nil || in.LiveSessionID == 0 || in.AuctionID == 0 {
		return MCPLiveLotOperationResult{}, domain.ErrInvalidArgument
	}
	session, err := s.requireLiveControlSession(ctx, in.LiveSessionID, actor)
	if err != nil {
		return MCPLiveLotOperationResult{}, err
	}
	action := normalizeMCPLiveLotAction(in.Action)
	if action == "" {
		return MCPLiveLotOperationResult{}, domain.ErrInvalidArgument
	}
	result := MCPLiveLotOperationResult{Action: action, LiveSessionID: session.ID, AuctionID: in.AuctionID}

	switch action {
	case "onShelf":
		lot, err := s.sessionSvc.MountAuction(ctx, session.ID, in.AuctionID, actor.ID, actor.Role)
		if err != nil {
			return MCPLiveLotOperationResult{}, err
		}
		result.Lot = &lot
	case "offShelf":
		if err := s.sessionSvc.UnmountAuction(ctx, session.ID, in.AuctionID, actor.ID, actor.Role); err != nil {
			return MCPLiveLotOperationResult{}, err
		}
		result.Removed = true
		if s.auctions != nil {
			if lot, err := s.auctions.FindByID(ctx, in.AuctionID); err == nil {
				result.Lot = &lot
			}
		}
	case "startExplain":
		lot, err := s.sessionSvc.ActivateAuctionWithOptions(ctx, ActivateLiveSessionAuctionInput{
			SessionID:   session.ID,
			AuctionID:   in.AuctionID,
			ActorID:     actor.ID,
			ActorRole:   actor.Role,
			DurationSec: in.DurationSec,
		})
		if err != nil {
			return MCPLiveLotOperationResult{}, err
		}
		result.Lot = &lot
	case "hammer":
		if session.ActiveAuctionID != in.AuctionID {
			return MCPLiveLotOperationResult{}, domain.ErrInvalidState
		}
		if s.hammerSvc == nil {
			return MCPLiveLotOperationResult{}, domain.ErrNotFound
		}
		requestID := strings.TrimSpace(in.RequestID)
		if requestID == "" {
			requestID = fmt.Sprintf("mcp-hammer-%d-%d-%d", session.ID, in.AuctionID, time.Now().UTC().UnixNano())
		}
		hammerResult, order, err := s.hammerSvc.Hammer(ctx, domain.HammerInput{
			RequestID: requestID,
			AuctionID: in.AuctionID,
			ActorID:   actor.ID,
			ActorRole: actor.Role,
			ClosedBy:  actor.ID,
			Force:     in.Force,
			Now:       time.Now().UTC(),
		})
		if err != nil {
			return MCPLiveLotOperationResult{}, err
		}
		result.HammerResult = &hammerResult
		result.Order = order
		if s.auctions != nil {
			if lot, err := s.auctions.FindByID(ctx, in.AuctionID); err == nil {
				result.Lot = &lot
			}
		}
	case "endLive":
		if session.ActiveAuctionID != 0 && session.ActiveAuctionID != in.AuctionID {
			return MCPLiveLotOperationResult{}, domain.ErrInvalidState
		}
		updated, err := s.sessionSvc.DeactivateAuction(ctx, session.ID, actor.ID, actor.Role)
		if err != nil {
			return MCPLiveLotOperationResult{}, err
		}
		result.Session = &updated
	default:
		return MCPLiveLotOperationResult{}, domain.ErrInvalidArgument
	}

	latestSession, err := s.sessions.Get(ctx, session.ID)
	if err != nil {
		return MCPLiveLotOperationResult{}, err
	}
	context, err := s.buildLiveControlContext(ctx, latestSession, actor)
	if err != nil {
		return MCPLiveLotOperationResult{}, err
	}
	result.Context = &context
	return result, nil
}

func (s *MCPControlService) requireLiveControlSession(ctx context.Context, sessionID uint64, actor MCPActor) (domain.LiveSession, error) {
	session, err := s.sessions.Get(ctx, sessionID)
	if err != nil {
		return domain.LiveSession{}, err
	}
	if session.Status != domain.LiveSessionStatusLive {
		return domain.LiveSession{}, domain.ErrInvalidState
	}
	if !canAccessSellerOwned(actor.ID, actor.Role, session.MerchantID) {
		return domain.LiveSession{}, domain.ErrForbidden
	}
	return session, nil
}

func (s *MCPControlService) buildLiveControlContext(ctx context.Context, session domain.LiveSession, actor MCPActor) (MCPLiveControlContext, error) {
	stats, err := s.sessionSvc.Stats(ctx, session.ID, actor.ID, actor.Role)
	if err != nil {
		return MCPLiveControlContext{}, err
	}
	out := MCPLiveControlContext{MerchantID: session.MerchantID, Session: &session, Stats: stats}
	sessionLots, err := s.sessionSvc.ListLots(ctx, session.ID, actor.ID, actor.Role)
	if err != nil {
		return MCPLiveControlContext{}, err
	}
	var sellerLots []domain.AuctionLot
	if s.auctions != nil {
		sellerLots, err = s.auctions.List(ctx, domain.AuctionFilter{SellerID: session.MerchantID, Limit: 100})
		if err != nil {
			return MCPLiveControlContext{}, err
		}
	}
	if session.ActiveAuctionID != 0 && s.auctions != nil {
		if lot, err := s.auctions.FindByID(ctx, session.ActiveAuctionID); err == nil {
			out.Lots.ExplainingLot = &lot
		} else if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return MCPLiveControlContext{}, err
		}
	}
	if session.ActiveAuctionID != 0 {
		currentState, err := s.readCurrentAuctionState(ctx, session.ActiveAuctionID, actor)
		if err != nil {
			return MCPLiveControlContext{}, err
		}
		out.CurrentAuctionState = currentState
	}

	for _, lot := range sellerLots {
		if lot.LiveSessionID != nil && *lot.LiveSessionID == session.ID {
			out.Lots.SessionLots = append(out.Lots.SessionLots, lot)
			switch lot.Status {
			case domain.AuctionStatusClosedWon:
				out.Lots.SoldLots = append(out.Lots.SoldLots, lot)
			case domain.AuctionStatusClosedFailed:
				out.Lots.UnsoldLots = append(out.Lots.UnsoldLots, lot)
			}
		}
		if lot.LiveSessionID == nil && isMCPMountCandidate(lot.Status) {
			out.Lots.CandidateLots = append(out.Lots.CandidateLots, lot)
		}
	}
	for _, lot := range sessionLots {
		if lot.AuctionID == session.ActiveAuctionID || lot.Status.Terminal() {
			continue
		}
		out.Lots.UpcomingLots = append(out.Lots.UpcomingLots, lot)
	}
	return out, nil
}

func (s *MCPControlService) readCurrentAuctionState(ctx context.Context, auctionID uint64, actor MCPActor) (*MCPLiveCurrentAuctionState, error) {
	var state domain.AuctionState
	var err error
	if s.auctionSvc != nil {
		state, err = s.auctionSvc.State(ctx, auctionID, actor.ID, actor.Role)
	} else if s.auctions != nil {
		lot, findErr := s.auctions.FindByID(ctx, auctionID)
		if findErr != nil {
			err = findErr
		} else {
			currentPrice := lot.StartPrice
			if lot.DealPrice != nil {
				currentPrice = *lot.DealPrice
			}
			leaderBidderID := ""
			if lot.WinnerID != nil {
				leaderBidderID = *lot.WinnerID
			}
			state = domain.AuctionState{
				AuctionID:      lot.AuctionID,
				Status:         lot.Status,
				CurrentPrice:   currentPrice,
				LeaderBidderID: leaderBidderID,
				StartTime:      lot.StartTime,
				EndTime:        lot.EndTime,
				Version:        lot.UpdatedAt.UnixMilli(),
				Source:         "db",
			}
		}
	} else {
		return nil, nil
	}
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return mcpCurrentAuctionStateFromDomain(state), nil
}

func mcpCurrentAuctionStateFromDomain(state domain.AuctionState) *MCPLiveCurrentAuctionState {
	remain := int64(0)
	if !state.EndTime.IsZero() {
		remain = int64(time.Until(state.EndTime).Seconds())
		if remain < 0 {
			remain = 0
		}
	}
	return &MCPLiveCurrentAuctionState{
		AuctionID:      state.AuctionID,
		Status:         state.Status,
		CurrentPrice:   state.CurrentPrice,
		LeaderBidderID: state.LeaderBidderID,
		StartTime:      state.StartTime,
		EndTime:        state.EndTime,
		RemainSeconds:  remain,
		LastBidTSMS:    state.LastBidTSMS,
		ExtendCount:    state.ExtendCount,
		Version:        state.Version,
		Source:         state.Source,
	}
}

func (s *MCPControlService) currentLiveControlSession(ctx context.Context, merchantID string) (domain.LiveSession, bool, error) {
	if s.sessions == nil {
		return domain.LiveSession{}, false, nil
	}
	if session, err := s.sessions.GetActiveByMerchantID(ctx, merchantID); err == nil {
		return session, true, nil
	} else if err != nil && !errors.Is(err, domain.ErrNotFound) {
		return domain.LiveSession{}, false, err
	}
	sessions, err := s.sessions.List(ctx, domain.LiveSessionFilter{MerchantID: merchantID, Limit: 1})
	if err != nil {
		return domain.LiveSession{}, false, err
	}
	if len(sessions) == 0 {
		return domain.LiveSession{}, false, nil
	}
	return sessions[0], true, nil
}

func normalizeMCPLiveLotAction(action string) string {
	switch strings.ToLower(strings.TrimSpace(action)) {
	case "onshelf", "on_shelf", "mount", "上架":
		return "onShelf"
	case "offshelf", "off_shelf", "unmount", "下架":
		return "offShelf"
	case "startexplain", "start_explain", "activate", "讲解", "开始讲解":
		return "startExplain"
	case "hammer", "close", "落槌", "成交":
		return "hammer"
	case "endlive", "end_live", "deactivateroom", "deactivate_room", "下播":
		return "endLive"
	default:
		return ""
	}
}

func isMCPMountCandidate(status domain.AuctionStatus) bool {
	switch status {
	case domain.AuctionStatusDraft, domain.AuctionStatusPendingAudit, domain.AuctionStatusReady:
		return true
	default:
		return false
	}
}

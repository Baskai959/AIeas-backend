package service

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

type AuctionService struct {
	auctions  repository.AuctionRepository
	items     repository.ItemRepository
	tx        repository.TxManager
	idGen     AuctionIDGenerator
	realtime  repository.AuctionRealtimeStore
	publisher EventPublisher
	timer     *TimerScheduler
	onClose   func(ctx context.Context, auctionID uint64)
	cfg       appconfig.AuctionConfig
}

type AuctionIDGenerator interface {
	NextAuctionID() (uint64, error)
}

type CreateAuctionInput struct {
	ActorID           string
	ActorRole         domain.Role
	AuctionID         uint64
	ItemID            uint64
	SellerID          string
	LiveRoomID        uint64
	AuctionType       domain.AuctionType
	StartPrice        int64
	ReservePrice      int64
	IncrementRule     json.RawMessage
	AntiSnipingSec    int
	AntiExtendSec     int
	DepositAmount     int64
	Status            domain.AuctionStatus
	StartTime         time.Time
	EndTime           time.Time
	allowSystemStatus bool
}

type UpdateAuctionInput struct {
	ActorID           string
	ActorRole         domain.Role
	StartPrice        *int64
	ReservePrice      *int64
	IncrementRule     *json.RawMessage
	AntiSnipingSec    *int
	AntiExtendSec     *int
	DepositAmount     *int64
	Status            *domain.AuctionStatus
	StartTime         *time.Time
	EndTime           *time.Time
	allowSystemStatus bool
}

func NewAuctionService(auctions repository.AuctionRepository, items repository.ItemRepository, tx repository.TxManager) *AuctionService {
	if tx == nil {
		tx = repository.NoopTxManager{}
	}
	return &AuctionService{auctions: auctions, items: items, tx: tx, realtime: repository.NoopRealtimeStore{}, cfg: appconfig.Default().Auction}
}

func (s *AuctionService) SetRealtime(realtime repository.AuctionRealtimeStore) {
	if realtime == nil {
		realtime = repository.NoopRealtimeStore{}
	}
	s.realtime = realtime
}

func (s *AuctionService) SetPublisher(publisher EventPublisher) {
	s.publisher = publisher
}

func (s *AuctionService) SetTimer(timer *TimerScheduler) {
	s.timer = timer
}

func (s *AuctionService) SetOnClose(fn func(ctx context.Context, auctionID uint64)) {
	s.onClose = fn
}

func (s *AuctionService) SetAuctionConfig(cfg appconfig.AuctionConfig) {
	s.cfg = cfg
}

func (s *AuctionService) SetIDGenerator(idGen AuctionIDGenerator) {
	s.idGen = idGen
}

func (s *AuctionService) Create(ctx context.Context, in CreateAuctionInput) (domain.AuctionLot, error) {
	if in.ItemID == 0 || strings.TrimSpace(in.ActorID) == "" {
		return domain.AuctionLot{}, domain.ErrInvalidArgument
	}
	if in.AuctionType == "" {
		in.AuctionType = domain.AuctionTypeEnglish
	}
	if !in.AuctionType.Valid() {
		return domain.AuctionLot{}, domain.ErrInvalidArgument
	}
	item, err := s.items.FindByID(ctx, in.ItemID)
	if err != nil {
		return domain.AuctionLot{}, err
	}
	sellerID := strings.TrimSpace(in.SellerID)
	if in.ActorRole == domain.RoleMerchant {
		sellerID = in.ActorID
	}
	if sellerID == "" {
		sellerID = item.SellerID
	}
	if !canAccessSellerOwned(in.ActorID, in.ActorRole, sellerID) || item.SellerID != sellerID {
		return domain.AuctionLot{}, domain.ErrForbidden
	}
	if in.StartPrice < 0 || in.ReservePrice < 0 || in.DepositAmount < 0 {
		return domain.AuctionLot{}, domain.ErrInvalidArgument
	}
	if in.AntiSnipingSec <= 0 {
		in.AntiSnipingSec = 15
	}
	if in.AntiExtendSec <= 0 {
		in.AntiExtendSec = 30
	}
	if len(in.IncrementRule) == 0 {
		in.IncrementRule = domain.DefaultIncrementRule()
	}
	if err := domain.ValidateIncrementRule(in.IncrementRule); err != nil {
		return domain.AuctionLot{}, domain.ErrInvalidArgument
	}
	if in.Status == "" {
		in.Status = domain.AuctionStatusDraft
	}
	if !in.Status.Valid() {
		return domain.AuctionLot{}, domain.ErrInvalidArgument
	}
	if !in.allowSystemStatus && !isEditableAuctionStatus(in.Status) {
		return domain.AuctionLot{}, domain.ErrInvalidArgument
	}
	now := time.Now().UTC()
	if in.StartTime.IsZero() {
		in.StartTime = now.Add(time.Minute)
	}
	if in.EndTime.IsZero() {
		in.EndTime = in.StartTime.Add(time.Hour)
	}
	if !in.EndTime.After(in.StartTime) {
		return domain.AuctionLot{}, domain.ErrInvalidArgument
	}
	snapshot, err := buildRuleSnapshot(in)
	if err != nil {
		return domain.AuctionLot{}, err
	}
	auctionID := in.AuctionID
	if auctionID == 0 && s.idGen != nil {
		auctionID, err = s.idGen.NextAuctionID()
		if err != nil {
			return domain.AuctionLot{}, err
		}
	}
	auction := domain.AuctionLot{
		AuctionID:      auctionID,
		ItemID:         in.ItemID,
		SellerID:       sellerID,
		LiveRoomID:     in.LiveRoomID,
		AuctionType:    in.AuctionType,
		StartPrice:     in.StartPrice,
		ReservePrice:   in.ReservePrice,
		IncrementRule:  append([]byte(nil), in.IncrementRule...),
		AntiSnipingSec: in.AntiSnipingSec,
		AntiExtendSec:  in.AntiExtendSec,
		DepositAmount:  in.DepositAmount,
		Status:         in.Status,
		RuleSnapshot:   snapshot,
		StartTime:      in.StartTime,
		EndTime:        in.EndTime,
	}
	if err := s.tx.WithinTx(ctx, func(txCtx context.Context) error {
		return s.auctions.Create(txCtx, &auction)
	}); err != nil {
		return domain.AuctionLot{}, err
	}
	return auction, nil
}

func (s *AuctionService) Get(ctx context.Context, id uint64, actorID string, actorRole domain.Role) (domain.AuctionLot, error) {
	auction, err := s.auctions.FindByID(ctx, id)
	if err != nil {
		return domain.AuctionLot{}, err
	}
	if !canAccessSellerOwned(actorID, actorRole, auction.SellerID) {
		return domain.AuctionLot{}, domain.ErrForbidden
	}
	return auction, nil
}

func (s *AuctionService) List(ctx context.Context, filter domain.AuctionFilter, actorID string, actorRole domain.Role) ([]domain.AuctionLot, error) {
	if actorRole == domain.RoleMerchant {
		filter.SellerID = actorID
	}
	return s.auctions.List(ctx, filter)
}

func (s *AuctionService) Update(ctx context.Context, id uint64, in UpdateAuctionInput) (domain.AuctionLot, error) {
	var auction domain.AuctionLot
	if err := s.tx.WithinTx(ctx, func(txCtx context.Context) error {
		current, err := s.auctions.FindByID(txCtx, id)
		if err != nil {
			return err
		}
		if !canAccessSellerOwned(in.ActorID, in.ActorRole, current.SellerID) {
			return domain.ErrForbidden
		}
		if current.Status == domain.AuctionStatusRunning || current.Status == domain.AuctionStatusExtended || current.Status.Terminal() {
			if hasAuctionContentPatch(in) {
				return domain.ErrInvalidState
			}
		}
		if in.StartPrice != nil {
			if *in.StartPrice < 0 {
				return domain.ErrInvalidArgument
			}
			current.StartPrice = *in.StartPrice
		}
		if in.ReservePrice != nil {
			if *in.ReservePrice < 0 {
				return domain.ErrInvalidArgument
			}
			current.ReservePrice = *in.ReservePrice
		}
		if in.IncrementRule != nil {
			if err := domain.ValidateIncrementRule(*in.IncrementRule); err != nil {
				return domain.ErrInvalidArgument
			}
			current.IncrementRule = append([]byte(nil), (*in.IncrementRule)...)
		}
		if in.AntiSnipingSec != nil {
			if *in.AntiSnipingSec <= 0 {
				return domain.ErrInvalidArgument
			}
			current.AntiSnipingSec = *in.AntiSnipingSec
		}
		if in.AntiExtendSec != nil {
			if *in.AntiExtendSec <= 0 {
				return domain.ErrInvalidArgument
			}
			current.AntiExtendSec = *in.AntiExtendSec
		}
		if in.DepositAmount != nil {
			if *in.DepositAmount < 0 {
				return domain.ErrInvalidArgument
			}
			current.DepositAmount = *in.DepositAmount
		}
		if in.StartTime != nil {
			current.StartTime = *in.StartTime
		}
		if in.EndTime != nil {
			current.EndTime = *in.EndTime
		}
		if !current.EndTime.After(current.StartTime) {
			return domain.ErrInvalidArgument
		}
		if in.Status != nil {
			if !in.allowSystemStatus && !isEditableAuctionStatus(*in.Status) {
				return domain.ErrInvalidArgument
			}
			if !in.Status.Valid() || !domain.CanTransitionAuction(current.Status, *in.Status) {
				return domain.ErrInvalidState
			}
			current.Status = *in.Status
		}
		snapshot, err := snapshotFromAuction(current)
		if err != nil {
			return err
		}
		current.RuleSnapshot = snapshot
		if err := s.auctions.Update(txCtx, &current); err != nil {
			return err
		}
		auction = current
		return nil
	}); err != nil {
		return domain.AuctionLot{}, err
	}
	return auction, nil
}

func (s *AuctionService) Delete(ctx context.Context, id uint64, actorID string, actorRole domain.Role) error {
	return s.tx.WithinTx(ctx, func(txCtx context.Context) error {
		auction, err := s.auctions.FindByID(txCtx, id)
		if err != nil {
			return err
		}
		if !canAccessSellerOwned(actorID, actorRole, auction.SellerID) {
			return domain.ErrForbidden
		}
		if auction.Status == domain.AuctionStatusRunning || auction.Status == domain.AuctionStatusExtended || auction.Status.Terminal() {
			return domain.ErrInvalidState
		}
		return s.auctions.Delete(txCtx, id)
	})
}

func (s *AuctionService) Start(ctx context.Context, id uint64, actorID string, actorRole domain.Role) (domain.AuctionLot, error) {
	return s.StartWithTiming(ctx, id, actorID, actorRole, time.Time{}, time.Time{})
}

func (s *AuctionService) StartWithTiming(ctx context.Context, id uint64, actorID string, actorRole domain.Role, startTime, endTime time.Time) (domain.AuctionLot, error) {
	status := domain.AuctionStatusRunning
	input := UpdateAuctionInput{ActorID: actorID, ActorRole: actorRole, Status: &status, allowSystemStatus: true}
	if !startTime.IsZero() {
		start := startTime.UTC()
		input.StartTime = &start
	}
	if !endTime.IsZero() {
		end := endTime.UTC()
		input.EndTime = &end
	}
	auction, err := s.Update(ctx, id, input)
	if err != nil {
		return domain.AuctionLot{}, err
	}
	minIncrement := domain.MinIncrementForPrice(auction.IncrementRule, auction.StartPrice, s.cfg.MinIncrementCent)
	state, err := s.realtime.InitAuction(ctx, auction, minIncrement)
	if err != nil {
		return domain.AuctionLot{}, err
	}
	broadcastJSON(s.publisher, id, "auction.started", map[string]interface{}{
		"auctionId": id,
		"state":     state,
	})
	if s.timer != nil {
		s.timer.Schedule(id)
	}
	return auction, nil
}

func (s *AuctionService) Cancel(ctx context.Context, id uint64, actorID string, actorRole domain.Role) (domain.AuctionLot, error) {
	status := domain.AuctionStatusClosedFailed
	auction, err := s.Update(ctx, id, UpdateAuctionInput{ActorID: actorID, ActorRole: actorRole, Status: &status, allowSystemStatus: true})
	if err != nil {
		return domain.AuctionLot{}, err
	}
	if s.onClose != nil {
		s.onClose(ctx, id)
	}
	return auction, nil
}

func (s *AuctionService) State(ctx context.Context, id uint64, actorID string, actorRole domain.Role) (domain.AuctionState, error) {
	_ = actorID
	_ = actorRole
	if state, ok, err := s.realtime.GetAuctionState(ctx, id); err != nil {
		return domain.AuctionState{}, err
	} else if ok {
		return state, nil
	}
	auction, err := s.auctions.FindByID(ctx, id)
	if err != nil {
		return domain.AuctionState{}, err
	}
	currentPrice := auction.StartPrice
	leaderID := ""
	if auction.DealPrice != nil {
		currentPrice = *auction.DealPrice
	}
	if auction.WinnerID != nil {
		leaderID = *auction.WinnerID
	}
	return domain.AuctionState{
		AuctionID:      auction.AuctionID,
		Status:         auction.Status,
		CurrentPrice:   currentPrice,
		LeaderBidderID: leaderID,
		StartTime:      auction.StartTime,
		EndTime:        auction.EndTime,
		Version:        auction.UpdatedAt.UnixMilli(),
		Source:         "db",
	}, nil
}

func buildRuleSnapshot(in CreateAuctionInput) (json.RawMessage, error) {
	payload := map[string]interface{}{
		"auctionType":    in.AuctionType,
		"incrementRule":  json.RawMessage(in.IncrementRule),
		"antiSnipingSec": in.AntiSnipingSec,
		"antiExtendSec":  in.AntiExtendSec,
		"depositPolicy": map[string]interface{}{
			"amount": in.DepositAmount,
		},
	}
	return json.Marshal(payload)
}

func snapshotFromAuction(auction domain.AuctionLot) (json.RawMessage, error) {
	payload := map[string]interface{}{
		"auctionType":    auction.AuctionType,
		"incrementRule":  json.RawMessage(auction.IncrementRule),
		"antiSnipingSec": auction.AntiSnipingSec,
		"antiExtendSec":  auction.AntiExtendSec,
		"depositPolicy": map[string]interface{}{
			"amount": auction.DepositAmount,
		},
	}
	return json.Marshal(payload)
}

func isEditableAuctionStatus(status domain.AuctionStatus) bool {
	return status == domain.AuctionStatusDraft || status == domain.AuctionStatusPendingAudit
}

func hasAuctionContentPatch(in UpdateAuctionInput) bool {
	return in.StartPrice != nil || in.ReservePrice != nil || in.IncrementRule != nil ||
		in.AntiSnipingSec != nil || in.AntiExtendSec != nil || in.DepositAmount != nil ||
		in.StartTime != nil || in.EndTime != nil
}

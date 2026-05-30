package service

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/observability/tracing"
	"aieas_backend/internal/repository"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type AuctionService struct {
	auctions  repository.AuctionRepository
	items     repository.ItemRepository
	bids      repository.BidRepository
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
	AuctionType       domain.AuctionType
	StartPrice        int64
	ReservePrice      int64
	CapPrice          int64
	IncrementRule     json.RawMessage
	AntiSnipingSec    int
	AntiExtendSec     int
	AntiExtendMode    domain.AuctionExtendMode
	DepositAmount     int64
	Status            domain.AuctionStatus
	StartTime         time.Time
	EndTime           time.Time
	DurationSec       int
	allowSystemStatus bool
}

type UpdateAuctionInput struct {
	ActorID           string
	ActorRole         domain.Role
	StartPrice        *int64
	ReservePrice      *int64
	CapPrice          *int64
	IncrementRule     *json.RawMessage
	AntiSnipingSec    *int
	AntiExtendSec     *int
	AntiExtendMode    *domain.AuctionExtendMode
	DepositAmount     *int64
	Status            *domain.AuctionStatus
	StartTime         *time.Time
	EndTime           *time.Time
	DurationSec       *int
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

func (s *AuctionService) SetBidRepository(bids repository.BidRepository) {
	s.bids = bids
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
	if in.DepositAmount < 0 {
		return domain.AuctionLot{}, domain.ErrInvalidArgument
	}
	if in.AntiSnipingSec <= 0 {
		in.AntiSnipingSec = 15
	}
	if in.AntiExtendSec <= 0 {
		in.AntiExtendSec = 30
	}
	in.AntiExtendMode = domain.NormalizeAuctionExtendMode(in.AntiExtendMode)
	if !in.AntiExtendMode.Valid() || in.DurationSec < 0 {
		return domain.AuctionLot{}, domain.ErrInvalidArgument
	}
	if len(in.IncrementRule) == 0 {
		in.IncrementRule = domain.DefaultIncrementRule()
	}
	if err := domain.ValidateIncrementRule(in.IncrementRule); err != nil {
		return domain.AuctionLot{}, domain.ErrInvalidArgument
	}
	if err := domain.ValidateAuctionPricing(in.StartPrice, in.ReservePrice, in.CapPrice, in.IncrementRule); err != nil {
		return domain.AuctionLot{}, domain.ErrInvalidArgument
	}
	in.Status = normalizeClientAuctionStatus(in.Status)
	if !in.Status.Valid() {
		return domain.AuctionLot{}, domain.ErrInvalidArgument
	}
	if !in.allowSystemStatus && !isClientWritableAuctionStatus(in.Status) {
		return domain.AuctionLot{}, domain.ErrInvalidArgument
	}
	startTime, endTime, durationSec, err := normalizeAuctionTiming(in.StartTime, in.EndTime, in.DurationSec)
	if err != nil {
		return domain.AuctionLot{}, domain.ErrInvalidArgument
	}
	in.StartTime = startTime
	in.EndTime = endTime
	in.DurationSec = durationSec
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
		AuctionType:    in.AuctionType,
		StartPrice:     in.StartPrice,
		ReservePrice:   in.ReservePrice,
		CapPrice:       in.CapPrice,
		IncrementRule:  append([]byte(nil), in.IncrementRule...),
		AntiSnipingSec: in.AntiSnipingSec,
		AntiExtendSec:  in.AntiExtendSec,
		AntiExtendMode: in.AntiExtendMode,
		DepositAmount:  in.DepositAmount,
		Status:         in.Status,
		RuleSnapshot:   snapshot,
		StartTime:      in.StartTime,
		EndTime:        in.EndTime,
		DurationSec:    in.DurationSec,
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
	return s.enrichAuctionLot(ctx, auction), nil
}

func (s *AuctionService) List(ctx context.Context, filter domain.AuctionFilter, actorID string, actorRole domain.Role) ([]domain.AuctionLot, error) {
	if actorRole == domain.RoleMerchant {
		filter.SellerID = actorID
	}
	auctions, err := s.auctions.List(ctx, filter)
	if err != nil {
		return nil, err
	}
	return s.enrichAuctionLots(ctx, auctions), nil
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
		if !in.allowSystemStatus && !isEditableAuctionStatus(current.Status) && hasAuctionContentPatch(in) {
			return domain.ErrInvalidState
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
		if in.CapPrice != nil {
			current.CapPrice = *in.CapPrice
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
		if in.AntiExtendMode != nil {
			mode := domain.NormalizeAuctionExtendMode(*in.AntiExtendMode)
			if !mode.Valid() {
				return domain.ErrInvalidArgument
			}
			current.AntiExtendMode = mode
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
		if in.DurationSec != nil {
			if *in.DurationSec < 0 {
				return domain.ErrInvalidArgument
			}
			current.DurationSec = *in.DurationSec
		}
		if hasAuctionTimingPatch(in) {
			startTime, endTime, durationSec, err := normalizeAuctionTiming(current.StartTime, current.EndTime, current.DurationSec)
			if err != nil {
				return domain.ErrInvalidArgument
			}
			current.StartTime = startTime
			current.EndTime = endTime
			current.DurationSec = durationSec
		}
		if hasAuctionPricingPatch(in) {
			if err := domain.ValidateAuctionPricing(current.StartPrice, current.ReservePrice, current.CapPrice, current.IncrementRule); err != nil {
				return domain.ErrInvalidArgument
			}
		}
		current.AntiExtendMode = domain.NormalizeAuctionExtendMode(current.AntiExtendMode)
		if in.Status != nil {
			nextStatus := *in.Status
			if !in.allowSystemStatus {
				nextStatus = normalizeClientAuctionStatus(nextStatus)
			}
			if !nextStatus.Valid() {
				return domain.ErrInvalidArgument
			}
			if !in.allowSystemStatus && !isClientWritableAuctionStatus(nextStatus) {
				return domain.ErrInvalidArgument
			}
			if !domain.CanTransitionAuction(current.Status, nextStatus) {
				return domain.ErrInvalidState
			}
			current.Status = nextStatus
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
	ctx, span := tracing.StartSpan(ctx, "auction.start",
		attribute.Int64("auction.id", int64(id)),
		attribute.String("actor.id", actorID),
		attribute.String("actor.role", string(actorRole)),
	)
	defer span.End()
	auction, err := s.startWithTiming(ctx, id, actorID, actorRole, startTime, endTime)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return domain.AuctionLot{}, err
	}
	span.SetAttributes(
		attribute.String("auction.status", string(auction.Status)),
		attribute.Int64("auction.start_time_ms", auction.StartTime.UnixMilli()),
		attribute.Int64("auction.end_time_ms", auction.EndTime.UnixMilli()),
	)
	return auction, nil
}

func (s *AuctionService) startWithTiming(ctx context.Context, id uint64, actorID string, actorRole domain.Role, startTime, endTime time.Time) (domain.AuctionLot, error) {
	current, err := s.auctions.FindByID(ctx, id)
	if err != nil {
		return domain.AuctionLot{}, err
	}
	if !canAccessSellerOwned(actorID, actorRole, current.SellerID) {
		return domain.AuctionLot{}, domain.ErrForbidden
	}
	now := time.Now().UTC()
	if startTime.IsZero() && endTime.IsZero() {
		switch {
		case current.DurationSec > 0 && (current.EndTime.IsZero() || !current.EndTime.After(now)):
			startTime = now
			endTime = now.Add(time.Duration(current.DurationSec) * time.Second)
		case current.EndTime.IsZero():
			startTime = now
			endTime = now.Add(time.Hour)
		case current.StartTime.IsZero():
			startTime = now
		}
	} else if startTime.IsZero() || endTime.IsZero() || !endTime.After(startTime) {
		return domain.AuctionLot{}, domain.ErrInvalidArgument
	}
	// P0-2 TCC：READY → WARMING_UP → InitAuction → RUNNING。
	// Step1：先把 status 置为 WARMING_UP 并写入 startTime/endTime；事务/校验保持原样。
	warming := domain.AuctionStatusWarmingUp
	warmInput := UpdateAuctionInput{ActorID: actorID, ActorRole: actorRole, Status: &warming, allowSystemStatus: true}
	if !startTime.IsZero() {
		start := startTime.UTC()
		warmInput.StartTime = &start
	}
	if !endTime.IsZero() {
		end := endTime.UTC()
		warmInput.EndTime = &end
	}
	warmed, err := s.Update(ctx, id, warmInput)
	if err != nil {
		return domain.AuctionLot{}, err
	}
	if warmed.EndTime.IsZero() || !warmed.EndTime.After(now) {
		return domain.AuctionLot{}, domain.ErrInvalidArgument
	}
	// Step2：调 realtime.InitAuction。失败时不回退状态——保持 WARMING_UP 让监控/对账观察，
	// 由调用方/告警决定后续动作（本轮不引入 reconciler）。
	// 传给 InitAuction 的 auction 状态使用最终目标 RUNNING：MySQL 仍是 WARMING_UP，
	// 但 Redis 直接写入 RUNNING 语义，避免 Step3 之后 RT 状态滞后于 MySQL。
	rtAuction := warmed
	rtAuction.Status = domain.AuctionStatusRunning
	minIncrement := domain.MinIncrementForPrice(rtAuction.IncrementRule, rtAuction.StartPrice, s.cfg.MinIncrementCent)
	state, err := s.realtime.InitAuction(ctx, rtAuction, minIncrement)
	if err != nil {
		return domain.AuctionLot{}, err
	}
	// Step3：InitAuction 成功后再把 status 置为 RUNNING；timing 已在 Step1 写入，无需重复传。
	running := domain.AuctionStatusRunning
	auction, err := s.Update(ctx, id, UpdateAuctionInput{ActorID: actorID, ActorRole: actorRole, Status: &running, allowSystemStatus: true})
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
	ctx, span := tracing.StartSpan(ctx, "auction.cancel",
		attribute.Int64("auction.id", int64(id)),
		attribute.String("actor.id", actorID),
		attribute.String("actor.role", string(actorRole)),
	)
	defer span.End()
	status := domain.AuctionStatusClosedFailed
	auction, err := s.Update(ctx, id, UpdateAuctionInput{ActorID: actorID, ActorRole: actorRole, Status: &status, allowSystemStatus: true})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return domain.AuctionLot{}, err
	}
	span.SetAttributes(attribute.String("auction.status", string(auction.Status)))
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

func (s *AuctionService) enrichAuctionLots(ctx context.Context, lots []domain.AuctionLot) []domain.AuctionLot {
	if len(lots) == 0 {
		return lots
	}
	out := make([]domain.AuctionLot, len(lots))
	for i := range lots {
		out[i] = s.enrichAuctionLot(ctx, lots[i])
	}
	return out
}

func (s *AuctionService) enrichAuctionLot(ctx context.Context, lot domain.AuctionLot) domain.AuctionLot {
	if s.bids != nil {
		if count, err := s.bids.CountByAuction(ctx, lot.AuctionID); err == nil {
			lot.BidCount = count
		}
		if records, err := s.bids.ListByAuction(ctx, lot.AuctionID, 1); err == nil && len(records) > 0 {
			if lot.CurrentPrice == 0 {
				lot.CurrentPrice = records[0].BidPrice
			}
			if lot.LeaderBidderID == "" {
				lot.LeaderBidderID = records[0].BidderID
			}
		}
	}
	if s.realtime != nil {
		if state, ok, err := s.realtime.GetAuctionState(ctx, lot.AuctionID); err == nil && ok {
			lot.CurrentPrice = state.CurrentPrice
			lot.LeaderBidderID = state.LeaderBidderID
			if !state.EndTime.IsZero() {
				lot.EndTime = state.EndTime
			}
			if state.Status.Valid() {
				lot.Status = state.Status
			}
		}
	}
	if lot.CurrentPrice == 0 && lot.DealPrice != nil {
		lot.CurrentPrice = *lot.DealPrice
	}
	return lot
}

func buildRuleSnapshot(in CreateAuctionInput) (json.RawMessage, error) {
	payload := map[string]interface{}{
		"auctionType":    in.AuctionType,
		"startPrice":     in.StartPrice,
		"reservePrice":   in.ReservePrice,
		"capPrice":       in.CapPrice,
		"incrementRule":  json.RawMessage(in.IncrementRule),
		"antiSnipingSec": in.AntiSnipingSec,
		"antiExtendSec":  in.AntiExtendSec,
		"antiExtendMode": domain.NormalizeAuctionExtendMode(in.AntiExtendMode),
		"durationSec":    in.DurationSec,
		"depositPolicy": map[string]interface{}{
			"amount": in.DepositAmount,
		},
	}
	return json.Marshal(payload)
}

func snapshotFromAuction(auction domain.AuctionLot) (json.RawMessage, error) {
	payload := map[string]interface{}{
		"auctionType":    auction.AuctionType,
		"startPrice":     auction.StartPrice,
		"reservePrice":   auction.ReservePrice,
		"capPrice":       auction.CapPrice,
		"incrementRule":  json.RawMessage(auction.IncrementRule),
		"antiSnipingSec": auction.AntiSnipingSec,
		"antiExtendSec":  auction.AntiExtendSec,
		"antiExtendMode": domain.NormalizeAuctionExtendMode(auction.AntiExtendMode),
		"durationSec":    auction.DurationSec,
		"depositPolicy": map[string]interface{}{
			"amount": auction.DepositAmount,
		},
	}
	return json.Marshal(payload)
}

func isEditableAuctionStatus(status domain.AuctionStatus) bool {
	return status == domain.AuctionStatusDraft || status == domain.AuctionStatusPendingAudit || status == domain.AuctionStatusReady
}

func normalizeClientAuctionStatus(status domain.AuctionStatus) domain.AuctionStatus {
	if status == "" {
		return domain.AuctionStatusReady
	}
	return status
}

func isClientWritableAuctionStatus(status domain.AuctionStatus) bool {
	return status == domain.AuctionStatusDraft || status == domain.AuctionStatusReady
}

func hasAuctionContentPatch(in UpdateAuctionInput) bool {
	return in.StartPrice != nil || in.ReservePrice != nil || in.CapPrice != nil || in.IncrementRule != nil ||
		in.AntiSnipingSec != nil || in.AntiExtendSec != nil || in.AntiExtendMode != nil || in.DepositAmount != nil ||
		in.StartTime != nil || in.EndTime != nil || in.DurationSec != nil
}

func hasAuctionPricingPatch(in UpdateAuctionInput) bool {
	return in.StartPrice != nil || in.ReservePrice != nil || in.CapPrice != nil || in.IncrementRule != nil
}

func hasAuctionTimingPatch(in UpdateAuctionInput) bool {
	return in.StartTime != nil || in.EndTime != nil || in.DurationSec != nil
}

func normalizeAuctionTiming(startTime, endTime time.Time, durationSec int) (time.Time, time.Time, int, error) {
	if durationSec < 0 {
		return time.Time{}, time.Time{}, 0, domain.ErrInvalidArgument
	}
	if !startTime.IsZero() {
		startTime = startTime.UTC()
	}
	if !endTime.IsZero() {
		endTime = endTime.UTC()
	}
	if startTime.IsZero() && !endTime.IsZero() {
		return time.Time{}, time.Time{}, 0, domain.ErrInvalidArgument
	}
	if !startTime.IsZero() && durationSec > 0 {
		endTime = startTime.Add(time.Duration(durationSec) * time.Second)
	}
	if !startTime.IsZero() && !endTime.IsZero() && !endTime.After(startTime) {
		return time.Time{}, time.Time{}, 0, domain.ErrInvalidArgument
	}
	return startTime, endTime, durationSec, nil
}

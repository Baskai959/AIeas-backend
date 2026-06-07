package app

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	auctionports "aieas_backend/internal/modules/auction/ports"
	orderports "aieas_backend/internal/modules/order/ports"
)

type HammerEventPublisher = auctionports.EventPublisher

type LiveSessionCounterWriter interface {
	IncrCounters(ctx context.Context, sessionID uint64, c domain.LiveSessionCounters) error
}

type HammerLiveAgentHook interface {
	EmitAuctionClosed(ctx context.Context, merchantID string, sessionID, auctionID uint64, status domain.AuctionStatus, price int64, auto bool, reason string)
}

type HammerService struct {
	auctions   auctionports.AuctionRepository
	bids       auctionports.BidRepository
	orders     auctionports.OrderRepository
	deposits   auctionports.DepositRepository
	realtime   auctionports.AuctionRealtimeStore
	tx         auctionports.TxManager
	publisher  HammerEventPublisher
	orderID    auctionports.OrderIDGenerator
	sessions   LiveSessionCounterWriter
	onClose    func(ctx context.Context, auctionID uint64)
	metrics    AuctionMetrics
	tracer     AuctionTracer
	hook       HammerLiveAgentHook
	events     auctionports.SettlementEventPublisher
	payTimeout time.Duration
}

type HammerServiceDeps struct {
	Auctions        auctionports.AuctionRepository
	Bids            auctionports.BidRepository
	Orders          auctionports.OrderRepository
	Deposits        auctionports.DepositRepository
	Realtime        auctionports.AuctionRealtimeStore
	Tx              auctionports.TxManager
	Publisher       HammerEventPublisher
	OrderID         auctionports.OrderIDGenerator
	Sessions        LiveSessionCounterWriter
	Metrics         AuctionMetrics
	Tracer          AuctionTracer
	LiveAgentHook   HammerLiveAgentHook
	Events          auctionports.SettlementEventPublisher
	OnClose         func(ctx context.Context, auctionID uint64)
	OrderPayTimeout time.Duration
}

func NewHammerService(auctions auctionports.AuctionRepository, orders auctionports.OrderRepository, deposits auctionports.DepositRepository, realtime auctionports.AuctionRealtimeStore, tx auctionports.TxManager, publisher HammerEventPublisher) *HammerService {
	return NewHammerServiceWithDeps(HammerServiceDeps{Auctions: auctions, Orders: orders, Deposits: deposits, Realtime: realtime, Tx: tx, Publisher: publisher})
}

func NewHammerServiceWithDeps(deps HammerServiceDeps) *HammerService {
	realtime := deps.Realtime
	if realtime == nil {
		realtime = noopRealtimeStore{}
	}
	tx := deps.Tx
	if tx == nil {
		tx = noopTxManager{}
	}
	payTimeout := deps.OrderPayTimeout
	if payTimeout <= 0 {
		payTimeout = orderports.DefaultPayTimeout
	}
	return &HammerService{auctions: deps.Auctions, bids: deps.Bids, orders: deps.Orders, deposits: deps.Deposits, realtime: realtime, tx: tx, publisher: deps.Publisher, orderID: deps.OrderID, sessions: deps.Sessions, onClose: deps.OnClose, metrics: deps.Metrics, tracer: deps.Tracer, hook: deps.LiveAgentHook, events: deps.Events, payTimeout: payTimeout}
}

func (s *HammerService) SetOnClose(fn func(ctx context.Context, auctionID uint64)) {
	s.onClose = fn
}

func (s *HammerService) SetOrderIDGenerator(idGen auctionports.OrderIDGenerator) {
	s.orderID = idGen
}

func (s *HammerService) SetOrderPayTimeout(timeout time.Duration) {
	if timeout > 0 {
		s.payTimeout = timeout
	}
}

func (s *HammerService) SetLiveSessionService(sessions LiveSessionCounterWriter) {
	s.sessions = sessions
}

func (s *HammerService) SetLiveAgentHookService(hook HammerLiveAgentHook) {
	s.hook = hook
}

func (s *HammerService) SetMetrics(reg AuctionMetrics) {
	s.metrics = reg
}

func (s *HammerService) SetSettlementEventPublisher(publisher auctionports.SettlementEventPublisher) {
	s.events = publisher
}

func (s *HammerService) Hammer(ctx context.Context, in domain.HammerInput) (domain.HammerResult, *domain.OrderDeal, error) {
	ctx, span := startAuctionSpan(ctx, s.tracer, "hammer.close",
		Int64Attr("auction.id", int64(in.AuctionID)),
		StringAttr("hammer.request_id", in.RequestID),
		StringAttr("actor.id", in.ActorID),
		BoolAttr("hammer.force", in.Force),
	)
	defer span.End()
	start := time.Now()
	result, order, err := s.hammerInternal(ctx, in)
	elapsed := time.Since(start)
	span.SetAttributes(
		StringAttr("auction.status", string(result.Status)),
		BoolAttr("hammer.duplicate", result.Duplicate),
		Int64Attr("hammer.price", result.Price),
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(AuctionStatusError, err.Error())
	}
	if s.metrics != nil {
		switch {
		case err != nil:
			s.metrics.ObserveHammer("error", elapsed)
		case result.Duplicate:
			s.metrics.IncHammerDuplicate()
			s.metrics.ObserveHammer("duplicate", elapsed)
		case result.Status == domain.AuctionStatusClosedWon:
			s.metrics.ObserveHammer("won", elapsed)
		case result.Status == domain.AuctionStatusClosedFailed:
			s.metrics.ObserveHammer("failed", elapsed)
		default:
			s.metrics.ObserveHammer("other", elapsed)
		}
	}
	return result, order, err
}

func (s *HammerService) hammerInternal(ctx context.Context, in domain.HammerInput) (domain.HammerResult, *domain.OrderDeal, error) {
	in.RequestID = strings.TrimSpace(in.RequestID)
	if in.RequestID == "" {
		in.RequestID = "hammer-" + strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	}
	if in.AuctionID == 0 {
		return domain.HammerResult{}, nil, domain.ErrInvalidArgument
	}
	if in.Now.IsZero() {
		in.Now = time.Now().UTC()
	}
	if in.IdempotencyTTL <= 0 {
		in.IdempotencyTTL = 24 * time.Hour
	}
	auction, err := s.auctions.FindByID(ctx, in.AuctionID)
	if err != nil {
		return domain.HammerResult{}, nil, err
	}
	if in.ActorRole != domain.RoleAdmin && !(in.ActorRole == domain.RoleMerchant && in.ActorID == auction.SellerID) {
		return domain.HammerResult{}, nil, domain.ErrForbidden
	}
	if terminal, order, ok, err := s.existingCloseResult(ctx, auction, in.RequestID); err != nil {
		return domain.HammerResult{}, nil, err
	} else if ok {
		terminal.Duplicate = true
		return terminal, order, nil
	}
	effectiveEnd := auction.EndTime
	if state, ok, err := s.realtime.GetAuctionState(ctx, in.AuctionID); err != nil {
		return domain.HammerResult{}, nil, err
	} else if ok && !state.EndTime.IsZero() {
		effectiveEnd = state.EndTime
	}
	if !in.Force && in.Now.Before(effectiveEnd) {
		return domain.HammerResult{}, nil, domain.ErrInvalidState
	}
	in.ReservePrice = auction.ReservePrice
	result, err := s.realtime.Hammer(ctx, in)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) && !in.Now.Before(effectiveEnd) {
			result, err = s.fallbackHammerFromBidRecords(ctx, auction, in)
			if err != nil {
				return domain.HammerResult{}, nil, err
			}
		} else {
			return domain.HammerResult{}, nil, err
		}
	}
	var order *domain.OrderDeal
	txStart := time.Now()
	txErr := s.tx.WithinTx(ctx, func(txCtx context.Context) error {
		current, err := s.auctions.FindByID(txCtx, in.AuctionID)
		if err != nil {
			return err
		}
		closedAt := result.ClosedAt
		current.Status = result.Status
		current.ClosedAt = &closedAt
		if in.ClosedBy != "" {
			current.ClosedBy = in.ClosedBy
		} else {
			current.ClosedBy = in.ActorID
		}
		if result.Status == domain.AuctionStatusClosedWon && (result.WinnerID == "" || result.Price < current.ReservePrice) {
			result.Status = domain.AuctionStatusClosedFailed
			result.WinnerID = ""
			result.Price = 0
		}
		current.Status = result.Status
		if result.Status == domain.AuctionStatusClosedWon {
			current.WinnerID = &result.WinnerID
			current.DealPrice = &result.Price
		} else {
			current.WinnerID = nil
			current.DealPrice = nil
		}
		expectedVersion := current.Version
		allowedFrom := []domain.AuctionStatus{
			domain.AuctionStatusRunning,
			domain.AuctionStatusExtended,
			domain.AuctionStatusHammerPending,
		}
		if err := s.auctions.CloseWithVersion(txCtx, &current, expectedVersion, allowedFrom); err != nil {
			return err
		}
		if result.Status != domain.AuctionStatusClosedWon {
			return s.releaseDeposits(txCtx, current.AuctionID, "released_by_auction_failed", nil)
		}
		var orderID uint64
		if s.orderID != nil {
			generated, err := s.orderID.NextOrderID()
			if err != nil {
				return err
			}
			orderID = generated
		}
		depositAmount := current.DepositAmount
		dealOrder := &domain.OrderDeal{
			ID:            orderID,
			AuctionID:     current.AuctionID,
			LiveSessionID: cloneLiveSessionID(current.LiveSessionID),
			LotSnapshot:   buildLotDealSnapshot(current, result.Price, result.WinnerID, closedAt),
			WinnerID:      result.WinnerID,
			SellerID:      current.SellerID,
			DealPrice:     result.Price,
			DepositAmount: depositAmount,
			Status:        domain.OrderStatusCreated,
			PayStatus:     domain.PayStatusUnpaid,
			PayDeadline:   ptrTime(in.Now.Add(s.payTimeout)),
			CreatedAt:     in.Now,
			UpdatedAt:     in.Now,
		}
		created, _, err := s.orders.CreateIfAbsentByAuction(txCtx, dealOrder)
		if err != nil {
			return err
		}
		order = &created
		if s.deposits != nil {
			if deposit, err := s.deposits.FindByAuctionUser(txCtx, current.AuctionID, result.WinnerID); err == nil {
				deposit.Status = domain.DepositStatusCaptured
				deposit.RelatedOrderID = &created.ID
				deposit.Remark = "captured_by_hammer"
				if err := s.deposits.Update(txCtx, &deposit); err != nil {
					return err
				}
			}
			return s.releaseDeposits(txCtx, current.AuctionID, "released_by_hammer", &result.WinnerID)
		}
		return nil
	})
	if s.metrics != nil {
		s.metrics.ObserveHammerMySQLTx(time.Since(txStart))
		if txErr != nil && !errors.Is(txErr, domain.ErrOptimisticConflict) && !errors.Is(txErr, domain.ErrInvalidState) {
			s.metrics.IncHammerMySQLFail()
		}
	}
	if txErr != nil {
		err := txErr
		if errors.Is(err, domain.ErrOptimisticConflict) {
			if s.metrics != nil {
				s.metrics.IncHammerOptimisticConflict()
			}
			return domain.HammerResult{}, nil, err
		}
		if errors.Is(err, domain.ErrInvalidState) {
			if existing, fetchErr := s.auctions.FindByID(ctx, in.AuctionID); fetchErr == nil {
				if terminal, terminalOrder, ok, terminalErr := s.existingCloseResult(ctx, existing, in.RequestID); terminalErr == nil && ok {
					terminal.Duplicate = true
					return terminal, terminalOrder, nil
				}
			}
			return domain.HammerResult{}, nil, err
		}
		return domain.HammerResult{}, nil, err
	}
	if s.sessions != nil && auction.LiveSessionID != nil {
		counters := domain.LiveSessionCounters{}
		if result.Status == domain.AuctionStatusClosedWon {
			counters.LotsSoldDelta = 1
			counters.GMVCentDelta = result.Price
		} else {
			counters.LotsUnsoldDelta = 1
		}
		_ = s.sessions.IncrCounters(ctx, *auction.LiveSessionID, counters)
	}
	s.publishSettlementEvents(ctx, auction, result, order, in)
	payload := map[string]interface{}{
		"auctionId":  result.AuctionID,
		"status":     result.Status,
		"winnerId":   result.WinnerID,
		"price":      result.Price,
		"closedAt":   result.ClosedAt,
		"serverTime": result.ClosedAt,
	}
	if auction.LiveSessionID != nil {
		payload["liveSessionId"] = *auction.LiveSessionID
	}
	if order != nil {
		payload["orderId"] = order.ID
	}
	broadcastJSON(s.publisher, result.AuctionID, "auction.closed", payload)
	if s.hook != nil && auction.LiveSessionID != nil {
		reason := ""
		if result.Status == domain.AuctionStatusClosedFailed {
			reason = "未达到保留价或无人有效出价"
		}
		s.hook.EmitAuctionClosed(ctx, auction.SellerID, *auction.LiveSessionID, auction.AuctionID, result.Status, result.Price, isAutoHammerInput(in), reason)
	}
	if s.onClose != nil {
		s.onClose(ctx, result.AuctionID)
	}
	return result, order, nil
}

func isAutoHammerInput(in domain.HammerInput) bool {
	closedBy := strings.ToUpper(strings.TrimSpace(in.ClosedBy))
	if closedBy == "SYSTEM" || closedBy == "CAP_PRICE" {
		return true
	}
	requestID := strings.ToLower(strings.TrimSpace(in.RequestID))
	return strings.HasPrefix(requestID, "auto-") || strings.HasPrefix(requestID, "cap-")
}

func buildLotDealSnapshot(lot domain.AuctionLot, dealPrice int64, winnerID string, closedAt time.Time) json.RawMessage {
	payload := map[string]interface{}{
		"auctionId":     lot.AuctionID,
		"sellerId":      lot.SellerID,
		"liveSessionId": lot.LiveSessionID,
		"title":         lot.Title,
		"description":   lot.Description,
		"category":      lot.Category,
		"brand":         lot.Brand,
		"condition":     lot.ConditionGrade,
		"imageUrls":     lot.ImageURLs,
		"coverUrl":      lot.CoverURL,
		"startPrice":    lot.StartPrice,
		"reservePrice":  lot.ReservePrice,
		"capPrice":      lot.CapPrice,
		"incrementRule": json.RawMessage(lot.IncrementRule),
		"depositAmount": lot.DepositAmount,
		"dealPrice":     dealPrice,
		"winnerId":      winnerID,
		"closedAt":      closedAt,
	}
	data, _ := json.Marshal(payload)
	return data
}

func (s *HammerService) publishSettlementEvents(ctx context.Context, auction domain.AuctionLot, result domain.HammerResult, order *domain.OrderDeal, in domain.HammerInput) {
	if s.events == nil {
		return
	}
	eventAuction := auction
	eventAuction.Status = result.Status
	eventAuction.ClosedAt = &result.ClosedAt
	if in.ClosedBy != "" {
		eventAuction.ClosedBy = in.ClosedBy
	} else {
		eventAuction.ClosedBy = in.ActorID
	}
	if result.Status == domain.AuctionStatusClosedWon {
		eventAuction.WinnerID = &result.WinnerID
		eventAuction.DealPrice = &result.Price
	} else {
		eventAuction.WinnerID = nil
		eventAuction.DealPrice = nil
	}
	if err := s.events.PublishAuctionClosed(ctx, eventAuction, result, order); err != nil {
		slog.Default().Warn("publish auction closed kafka event failed", "auction_id", result.AuctionID, "error", err)
	}
	if order != nil {
		if err := s.events.PublishOrderCreated(ctx, *order); err != nil {
			slog.Default().Warn("publish order created kafka event failed", "auction_id", result.AuctionID, "order_id", order.ID, "error", err)
		}
	}
}

func (s *HammerService) fallbackHammerFromBidRecords(ctx context.Context, auction domain.AuctionLot, in domain.HammerInput) (domain.HammerResult, error) {
	result := domain.HammerResult{
		RequestID: in.RequestID,
		AuctionID: auction.AuctionID,
		Status:    domain.AuctionStatusClosedFailed,
		ClosedAt:  in.Now,
		Version:   auction.Version + 1,
	}
	records, err := listAuctionBidRecordsForRound(ctx, s.bids, auction.AuctionID, auction.StartTime.UnixMilli(), 1)
	if err != nil {
		return domain.HammerResult{}, err
	}
	if len(records) == 0 {
		return result, nil
	}
	top := records[0]
	if top.BidderID == "" || top.BidPrice < auction.ReservePrice {
		return result, nil
	}
	result.Status = domain.AuctionStatusClosedWon
	result.WinnerID = top.BidderID
	result.Price = top.BidPrice
	return result, nil
}

func (s *HammerService) existingCloseResult(ctx context.Context, auction domain.AuctionLot, requestID string) (domain.HammerResult, *domain.OrderDeal, bool, error) {
	if !auction.Status.Terminal() {
		return domain.HammerResult{}, nil, false, nil
	}
	closedAt := time.Now().UTC()
	if auction.ClosedAt != nil {
		closedAt = *auction.ClosedAt
	}
	result := domain.HammerResult{RequestID: requestID, AuctionID: auction.AuctionID, Status: auction.Status, ClosedAt: closedAt}
	if auction.WinnerID != nil {
		result.WinnerID = *auction.WinnerID
	}
	if auction.DealPrice != nil {
		result.Price = *auction.DealPrice
	}
	var order *domain.OrderDeal
	if auction.Status == domain.AuctionStatusClosedWon && s.orders != nil {
		if existing, err := s.orders.FindByAuctionID(ctx, auction.AuctionID); err == nil {
			order = &existing
		} else if err != domain.ErrNotFound {
			return domain.HammerResult{}, nil, false, err
		}
	}
	return result, order, true, nil
}

func (s *HammerService) releaseDeposits(ctx context.Context, auctionID uint64, remark string, winnerID *string) error {
	if s.deposits == nil {
		return nil
	}
	deposits, err := s.deposits.ListByAuction(ctx, auctionID)
	if err != nil {
		return err
	}
	for _, deposit := range deposits {
		if winnerID != nil && deposit.UserID == *winnerID {
			continue
		}
		if deposit.Status != domain.DepositStatusReady && deposit.Status != domain.DepositStatusPending {
			continue
		}
		deposit.Status = domain.DepositStatusReleased
		deposit.RelatedOrderID = nil
		deposit.Remark = remark
		if err := s.deposits.Update(ctx, &deposit); err != nil {
			return err
		}
	}
	return nil
}

func ptrTime(t time.Time) *time.Time {
	return &t
}

func cloneLiveSessionID(p *uint64) *uint64 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

func broadcastJSON(publisher HammerEventPublisher, auctionID uint64, eventType string, payload interface{}) {
	if publisher == nil || auctionID == 0 || eventType == "" {
		return
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	publisher.Broadcast(auctionID, auctionports.EventEnvelope{Type: eventType, Payload: raw})
}

type noopTxManager struct{}

func (noopTxManager) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}

type noopRealtimeStore struct{}

func (noopRealtimeStore) GetAuctionState(ctx context.Context, auctionID uint64) (domain.AuctionState, bool, error) {
	_ = ctx
	_ = auctionID
	return domain.AuctionState{}, false, nil
}

func (noopRealtimeStore) TopN(ctx context.Context, auctionID uint64, limit int) ([]domain.RankingEntry, error) {
	_ = ctx
	_ = auctionID
	_ = limit
	return nil, nil
}

func (noopRealtimeStore) InitAuction(ctx context.Context, auction domain.AuctionLot, minIncrement int64) (domain.AuctionState, error) {
	_ = ctx
	_ = auction
	_ = minIncrement
	return domain.AuctionState{}, nil
}

func (noopRealtimeStore) MarkEnrollment(ctx context.Context, auctionID uint64, userID string) error {
	_ = ctx
	_ = auctionID
	_ = userID
	return nil
}

func (noopRealtimeStore) BidPrerequisites(ctx context.Context, auctionID uint64, userID string) (bool, bool, error) {
	_ = ctx
	_ = auctionID
	_ = userID
	return false, false, nil
}

func (noopRealtimeStore) PlaceBid(ctx context.Context, input domain.BidInput) (domain.BidResult, error) {
	_ = ctx
	_ = input
	return domain.BidResult{}, nil
}

func (noopRealtimeStore) Hammer(ctx context.Context, input domain.HammerInput) (domain.HammerResult, error) {
	_ = ctx
	return domain.HammerResult{RequestID: input.RequestID, AuctionID: input.AuctionID, Status: domain.AuctionStatusClosedFailed, ClosedAt: input.Now}, nil
}

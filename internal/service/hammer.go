package service

import (
	"context"
	"strconv"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/observability/metrics"
	"aieas_backend/internal/infra/observability/tracing"
	"aieas_backend/internal/repository"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type HammerService struct {
	auctions  repository.AuctionRepository
	orders    repository.OrderRepository
	deposits  repository.DepositRepository
	realtime  repository.AuctionRealtimeStore
	tx        repository.TxManager
	publisher EventPublisher
	orderID   OrderIDGenerator
	sessions  *LiveSessionService
	onClose   func(ctx context.Context, auctionID uint64)
	metrics   *metrics.Registry
	hook      *LiveAgentHookService
}

type OrderIDGenerator interface {
	NextOrderID() (uint64, error)
}

func NewHammerService(auctions repository.AuctionRepository, orders repository.OrderRepository, deposits repository.DepositRepository, realtime repository.AuctionRealtimeStore, tx repository.TxManager, publisher EventPublisher) *HammerService {
	if realtime == nil {
		realtime = repository.NoopRealtimeStore{}
	}
	if tx == nil {
		tx = repository.NoopTxManager{}
	}
	return &HammerService{auctions: auctions, orders: orders, deposits: deposits, realtime: realtime, tx: tx, publisher: publisher}
}

// SetOnClose 注册拍卖终态回调（如直播间锁释放），不影响主流程错误。
func (s *HammerService) SetOnClose(fn func(ctx context.Context, auctionID uint64)) {
	s.onClose = fn
}

func (s *HammerService) SetOrderIDGenerator(idGen OrderIDGenerator) {
	s.orderID = idGen
}

// SetLiveSessionService 注入直播场次服务，用于在 Hammer 时回填 order.live_session_id 与累加场次成交计数。
func (s *HammerService) SetLiveSessionService(sessions *LiveSessionService) {
	s.sessions = sessions
}

// SetLiveAgentHookService 注入直播拍卖事件 hook。
func (s *HammerService) SetLiveAgentHookService(hook *LiveAgentHookService) {
	s.hook = hook
}

// SetMetrics 注入观测性 Registry。nil 安全。
func (s *HammerService) SetMetrics(reg *metrics.Registry) {
	s.metrics = reg
}

// Hammer 触发拍卖落槌，根据结果返回 hammer result + 可选成交订单。
// 包装 hammerInternal 在外层统一打点：result label 取自落槌结果或错误分类。
func (s *HammerService) Hammer(ctx context.Context, in domain.HammerInput) (domain.HammerResult, *domain.OrderDeal, error) {
	ctx, span := tracing.StartSpan(ctx, "hammer.close",
		attribute.Int64("auction.id", int64(in.AuctionID)),
		attribute.String("hammer.request_id", in.RequestID),
		attribute.String("actor.id", in.ActorID),
		attribute.Bool("hammer.force", in.Force),
	)
	defer span.End()
	start := time.Now()
	result, order, err := s.hammerInternal(ctx, in)
	elapsed := time.Since(start)
	span.SetAttributes(
		attribute.String("auction.status", string(result.Status)),
		attribute.Bool("hammer.duplicate", result.Duplicate),
		attribute.Int64("hammer.price", result.Price),
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
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
		in.RequestID = "hammer-" + strconvFormatTime(time.Now().UTC())
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
		return domain.HammerResult{}, nil, err
	}
	var order *domain.OrderDeal
	if err := s.tx.WithinTx(ctx, func(txCtx context.Context) error {
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
		if result.Status == domain.AuctionStatusClosedWon {
			current.WinnerID = &result.WinnerID
			current.DealPrice = &result.Price
		} else {
			current.WinnerID = nil
			current.DealPrice = nil
		}
		if err := s.auctions.Update(txCtx, &current); err != nil {
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
			WinnerID:      result.WinnerID,
			SellerID:      current.SellerID,
			DealPrice:     result.Price,
			DepositAmount: depositAmount,
			Status:        domain.OrderStatusCreated,
			PayStatus:     domain.PayStatusUnpaid,
			PayDeadline:   ptrTime(in.Now.Add(24 * time.Hour)),
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
	}); err != nil {
		return domain.HammerResult{}, nil, err
	}
	// 终态后累加场次计数：成交（CLOSED_WON）→ lots_sold/gmv；流拍/失败 → lots_unsold。
	// 不在 tx 内完成以避免外部锁竞争影响主流程；失败仅日志级别忽略（已通过 Hub 广播）。
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
	payload := map[string]interface{}{
		"auctionId": result.AuctionID,
		"status":    result.Status,
		"winnerId":  result.WinnerID,
		"price":     result.Price,
		"closedAt":  result.ClosedAt,
	}
	if order != nil {
		payload["orderId"] = order.ID
	}
	broadcastJSON(s.publisher, result.AuctionID, "auction.closed", payload)
	if s.hook != nil && result.Status == domain.AuctionStatusClosedWon && auction.LiveRoomID != 0 {
		s.hook.EmitHammerWon(ctx, auction.SellerID, auction.LiveRoomID, auction.AuctionID, result.Price)
	}
	if s.onClose != nil {
		s.onClose(ctx, result.AuctionID)
	}
	return result, order, nil
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

func strconvFormatTime(t time.Time) string {
	return strconv.FormatInt(t.UnixNano(), 10)
}

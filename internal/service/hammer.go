package service

import (
	"context"
	"strconv"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

type HammerService struct {
	auctions  repository.AuctionRepository
	orders    repository.OrderRepository
	deposits  repository.DepositRepository
	realtime  repository.AuctionRealtimeStore
	tx        repository.TxManager
	publisher EventPublisher
	orderID   OrderIDGenerator
	onClose   func(ctx context.Context, auctionID uint64)
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

func (s *HammerService) Hammer(ctx context.Context, in domain.HammerInput) (domain.HammerResult, *domain.OrderDeal, error) {
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

func strconvFormatTime(t time.Time) string {
	return strconv.FormatInt(t.UnixNano(), 10)
}

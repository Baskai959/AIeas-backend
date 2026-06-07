package app

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	orderports "aieas_backend/internal/modules/order/ports"
)

const (
	DefaultOrderPayTimeout           = orderports.DefaultPayTimeout
	DefaultOrderTimeoutScanInterval  = orderports.DefaultTimeoutScanInterval
	DefaultOrderTimeoutScanBatchSize = orderports.DefaultTimeoutScanBatchSize
)

type OrderService struct {
	orders orderports.OrderRepository
	users  orderports.UserReader
	tx     orderports.TxManager
}

func NewOrderService(orders orderports.OrderRepository, tx orderports.TxManager) *OrderService {
	return &OrderService{orders: orders, tx: tx}
}

func (s *OrderService) SetUserRepository(users orderports.UserReader) {
	if s == nil {
		return
	}
	s.users = users
}

func (s *OrderService) List(ctx context.Context, filter domain.OrderFilter, actorID string, actorRole domain.Role) ([]domain.OrderDeal, error) {
	switch actorRole {
	case domain.RoleAdmin:
	case domain.RoleMerchant:
		filter.SellerID = actorID
	case domain.RoleBuyer:
		filter.WinnerID = actorID
	default:
		return nil, domain.ErrForbidden
	}
	orders, err := s.orders.List(ctx, filter)
	if err != nil {
		return nil, err
	}
	return s.enrichWinnerNicknames(ctx, orders), nil
}

func (s *OrderService) Mine(ctx context.Context, actorID string, actorRole domain.Role, filter domain.OrderFilter) ([]domain.OrderDeal, error) {
	if strings.TrimSpace(actorID) == "" || actorRole != domain.RoleBuyer {
		return nil, domain.ErrForbidden
	}
	filter.WinnerID = actorID
	orders, err := s.orders.List(ctx, filter)
	if err != nil {
		return nil, err
	}
	return s.enrichWinnerNicknames(ctx, orders), nil
}

func (s *OrderService) Get(ctx context.Context, id uint64, actorID string, actorRole domain.Role) (domain.OrderDeal, error) {
	order, err := s.orders.FindByID(ctx, id)
	if err != nil {
		return domain.OrderDeal{}, err
	}
	if !canAccessOrder(order, actorID, actorRole) {
		return domain.OrderDeal{}, domain.ErrForbidden
	}
	return s.enrichWinnerNickname(ctx, order), nil
}

func (s *OrderService) Pay(ctx context.Context, id uint64, actorID string, actorRole domain.Role) (domain.OrderDeal, error) {
	var order domain.OrderDeal
	var resultErr error
	txErr := s.tx.WithinTx(ctx, func(txCtx context.Context) error {
		current, err := s.orders.FindByID(txCtx, id)
		if err != nil {
			return err
		}
		if actorRole != domain.RoleAdmin && !(actorRole == domain.RoleBuyer && current.WinnerID == actorID) {
			return domain.ErrForbidden
		}
		if current.PayStatus == domain.PayStatusPaid && current.Status == domain.OrderStatusPaid {
			order = current
			return nil
		}
		if current.Status != domain.OrderStatusCreated {
			return domain.ErrInvalidState
		}
		now := time.Now().UTC()
		expectedVersion := current.Version
		if current.PaymentExpired(now) {
			if err := current.MarkTimeout(now); err != nil {
				return err
			}
			if err := s.orders.UpdateStatusWithVersion(txCtx, &current, expectedVersion, []domain.OrderStatus{domain.OrderStatusCreated}); err != nil {
				return err
			}
			resultErr = domain.ErrInvalidState
			return nil
		}
		if err := current.MarkPaid(now); err != nil {
			return err
		}
		if err := s.orders.UpdateStatusWithVersion(txCtx, &current, expectedVersion, []domain.OrderStatus{domain.OrderStatusCreated}); err != nil {
			return err
		}
		order = current
		return nil
	})
	if txErr != nil {
		if recovered, ok, err := s.resolvePayFinalState(ctx, id, txErr); ok {
			return recovered, err
		}
		return domain.OrderDeal{}, txErr
	}
	if resultErr != nil {
		return domain.OrderDeal{}, resultErr
	}
	return s.enrichWinnerNickname(ctx, order), nil
}

func (s *OrderService) Ship(ctx context.Context, id uint64, actorID string, actorRole domain.Role) (domain.OrderDeal, error) {
	var order domain.OrderDeal
	txErr := s.tx.WithinTx(ctx, func(txCtx context.Context) error {
		current, err := s.orders.FindByID(txCtx, id)
		if err != nil {
			return err
		}
		if actorRole != domain.RoleAdmin && !(actorRole == domain.RoleMerchant && current.SellerID == actorID) {
			return domain.ErrForbidden
		}
		expectedVersion := current.Version
		if err := current.MarkShipped(time.Now().UTC()); err != nil {
			return err
		}
		if err := s.orders.UpdateFulfillmentWithVersion(txCtx, &current, expectedVersion, []domain.FulfillmentStatus{domain.FulfillmentStatusUnshipped}); err != nil {
			return err
		}
		order = current
		return nil
	})
	if txErr != nil {
		return domain.OrderDeal{}, txErr
	}
	return s.enrichWinnerNickname(ctx, order), nil
}

func (s *OrderService) Receive(ctx context.Context, id uint64, actorID string, actorRole domain.Role) (domain.OrderDeal, error) {
	var order domain.OrderDeal
	txErr := s.tx.WithinTx(ctx, func(txCtx context.Context) error {
		current, err := s.orders.FindByID(txCtx, id)
		if err != nil {
			return err
		}
		if actorRole != domain.RoleAdmin && !(actorRole == domain.RoleBuyer && current.WinnerID == actorID) {
			return domain.ErrForbidden
		}
		expectedVersion := current.Version
		if err := current.MarkReceived(time.Now().UTC()); err != nil {
			return err
		}
		if err := s.orders.UpdateFulfillmentWithVersion(txCtx, &current, expectedVersion, []domain.FulfillmentStatus{domain.FulfillmentStatusShipped}); err != nil {
			return err
		}
		order = current
		return nil
	})
	if txErr != nil {
		return domain.OrderDeal{}, txErr
	}
	return s.enrichWinnerNickname(ctx, order), nil
}

func (s *OrderService) CloseExpiredOnce(ctx context.Context, now time.Time, limit int) (int, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	if limit <= 0 {
		limit = DefaultOrderTimeoutScanBatchSize
	}
	candidates, err := s.orders.ListPayTimeoutCandidates(ctx, now, limit)
	if err != nil {
		return 0, err
	}
	closed := 0
	for _, candidate := range candidates {
		ok, err := s.closeExpiredCandidate(ctx, candidate.ID, now)
		if err != nil {
			return closed, err
		}
		if ok {
			closed++
		}
	}
	return closed, nil
}

func (s *OrderService) StartTimeoutWorker(ctx context.Context, interval time.Duration, batchSize int) {
	if interval <= 0 {
		interval = DefaultOrderTimeoutScanInterval
	}
	if batchSize <= 0 {
		batchSize = DefaultOrderTimeoutScanBatchSize
	}
	run := func() {
		closed, err := s.CloseExpiredOnce(ctx, time.Now().UTC(), batchSize)
		if err != nil {
			if ctx.Err() == nil {
				slog.Default().Warn("close expired orders failed", "error", err)
			}
			return
		}
		if closed > 0 {
			slog.Default().Info("expired orders closed", "count", closed)
		}
	}
	go func() {
		run()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				run()
			}
		}
	}()
}

func (s *OrderService) closeExpiredCandidate(ctx context.Context, id uint64, now time.Time) (bool, error) {
	closed := false
	err := s.tx.WithinTx(ctx, func(txCtx context.Context) error {
		current, err := s.orders.FindByID(txCtx, id)
		if err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				return nil
			}
			return err
		}
		if current.Status != domain.OrderStatusCreated || current.PayStatus != domain.PayStatusUnpaid || !current.PaymentExpired(now) {
			return nil
		}
		expectedVersion := current.Version
		if err := current.MarkTimeout(now); err != nil {
			if errors.Is(err, domain.ErrInvalidState) {
				return nil
			}
			return err
		}
		if err := s.orders.UpdateStatusWithVersion(txCtx, &current, expectedVersion, []domain.OrderStatus{domain.OrderStatusCreated}); err != nil {
			if errors.Is(err, domain.ErrOptimisticConflict) || errors.Is(err, domain.ErrInvalidState) {
				return nil
			}
			return err
		}
		closed = true
		return nil
	})
	return closed, err
}

func (s *OrderService) resolvePayFinalState(ctx context.Context, id uint64, err error) (domain.OrderDeal, bool, error) {
	if !errors.Is(err, domain.ErrOptimisticConflict) && !errors.Is(err, domain.ErrInvalidState) {
		return domain.OrderDeal{}, false, nil
	}
	latest, findErr := s.orders.FindByID(ctx, id)
	if findErr != nil {
		return domain.OrderDeal{}, true, findErr
	}
	if latest.Status == domain.OrderStatusPaid && latest.PayStatus == domain.PayStatusPaid {
		return s.enrichWinnerNickname(ctx, latest), true, nil
	}
	if latest.Status.Terminal() {
		return domain.OrderDeal{}, true, domain.ErrInvalidState
	}
	return domain.OrderDeal{}, true, err
}

func (s *OrderService) enrichWinnerNicknames(ctx context.Context, orders []domain.OrderDeal) []domain.OrderDeal {
	if len(orders) == 0 || s == nil || s.users == nil {
		return orders
	}
	out := make([]domain.OrderDeal, len(orders))
	copy(out, orders)
	cache := make(map[string]string, len(out))
	for i := range out {
		winnerID := strings.TrimSpace(out[i].WinnerID)
		if winnerID == "" {
			continue
		}
		if nickname, ok := cache[winnerID]; ok {
			out[i].WinnerNickname = nickname
			continue
		}
		user, err := s.users.FindByID(winnerID)
		if err != nil {
			cache[winnerID] = ""
			continue
		}
		nickname := strings.TrimSpace(user.Nickname)
		cache[winnerID] = nickname
		out[i].WinnerNickname = nickname
	}
	return out
}

func (s *OrderService) enrichWinnerNickname(ctx context.Context, order domain.OrderDeal) domain.OrderDeal {
	if s == nil || s.users == nil || strings.TrimSpace(order.WinnerID) == "" {
		return order
	}
	user, err := s.users.FindByID(strings.TrimSpace(order.WinnerID))
	if err != nil {
		return order
	}
	order.WinnerNickname = strings.TrimSpace(user.Nickname)
	return order
}

func canAccessOrder(order domain.OrderDeal, actorID string, actorRole domain.Role) bool {
	switch actorRole {
	case domain.RoleAdmin:
		return true
	case domain.RoleMerchant:
		return actorID != "" && order.SellerID == actorID
	case domain.RoleBuyer:
		return actorID != "" && order.WinnerID == actorID
	default:
		return false
	}
}

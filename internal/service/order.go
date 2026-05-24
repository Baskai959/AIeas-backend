package service

import (
	"context"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

type OrderService struct {
	orders repository.OrderRepository
	tx     repository.TxManager
}

func NewOrderService(orders repository.OrderRepository, tx repository.TxManager) *OrderService {
	if tx == nil {
		tx = repository.NoopTxManager{}
	}
	return &OrderService{orders: orders, tx: tx}
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
	return s.orders.List(ctx, filter)
}

func (s *OrderService) Mine(ctx context.Context, actorID string, actorRole domain.Role, filter domain.OrderFilter) ([]domain.OrderDeal, error) {
	if strings.TrimSpace(actorID) == "" || actorRole != domain.RoleBuyer {
		return nil, domain.ErrForbidden
	}
	filter.WinnerID = actorID
	return s.orders.List(ctx, filter)
}

func (s *OrderService) Get(ctx context.Context, id uint64, actorID string, actorRole domain.Role) (domain.OrderDeal, error) {
	order, err := s.orders.FindByID(ctx, id)
	if err != nil {
		return domain.OrderDeal{}, err
	}
	if !canAccessOrder(order, actorID, actorRole) {
		return domain.OrderDeal{}, domain.ErrForbidden
	}
	return order, nil
}

func (s *OrderService) Pay(ctx context.Context, id uint64, actorID string, actorRole domain.Role) (domain.OrderDeal, error) {
	var order domain.OrderDeal
	if err := s.tx.WithinTx(ctx, func(txCtx context.Context) error {
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
		current.Status = domain.OrderStatusPaid
		current.PayStatus = domain.PayStatusPaid
		current.PaidAt = &now
		if err := s.orders.Update(txCtx, &current); err != nil {
			return err
		}
		order = current
		return nil
	}); err != nil {
		return domain.OrderDeal{}, err
	}
	return order, nil
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

package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

func TestOrderServiceCloseExpiredOnceMarksTimeout(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	deadline := now.Add(-time.Hour)
	repo := repository.NewMemoryOrderRepository()
	order, _, err := repo.CreateIfAbsentByAuction(ctx, &domain.OrderDeal{
		AuctionID:     10001,
		WinnerID:      "u_1001",
		SellerID:      "u_2001",
		DealPrice:     12000,
		DepositAmount: 1000,
		Status:        domain.OrderStatusCreated,
		PayStatus:     domain.PayStatusUnpaid,
		PayDeadline:   &deadline,
	})
	if err != nil {
		t.Fatalf("seed order: %v", err)
	}
	svc := NewOrderService(repo, repository.NoopTxManager{})

	closed, err := svc.CloseExpiredOnce(ctx, now, 10)
	if err != nil {
		t.Fatalf("close expired: %v", err)
	}
	if closed != 1 {
		t.Fatalf("expected 1 closed order, got %d", closed)
	}
	got, err := repo.FindByID(ctx, order.ID)
	if err != nil {
		t.Fatalf("find order: %v", err)
	}
	if got.Status != domain.OrderStatusTimeout || got.PayStatus != domain.PayStatusUnpaid || got.ClosedAt == nil || got.Version != order.Version+1 {
		t.Fatalf("unexpected timeout order: %+v", got)
	}
	if _, err := svc.Pay(ctx, order.ID, "u_1001", domain.RoleBuyer); !errors.Is(err, domain.ErrInvalidState) {
		t.Fatalf("expected timed-out order pay rejection, got %v", err)
	}
}

func TestOrderServicePayWinsBeforeTimeout(t *testing.T) {
	ctx := context.Background()
	deadline := time.Now().UTC().Add(time.Hour)
	repo := repository.NewMemoryOrderRepository()
	order, _, err := repo.CreateIfAbsentByAuction(ctx, &domain.OrderDeal{
		AuctionID:     10002,
		WinnerID:      "u_1001",
		SellerID:      "u_2001",
		DealPrice:     12000,
		DepositAmount: 1000,
		Status:        domain.OrderStatusCreated,
		PayStatus:     domain.PayStatusUnpaid,
		PayDeadline:   &deadline,
	})
	if err != nil {
		t.Fatalf("seed order: %v", err)
	}
	svc := NewOrderService(repo, repository.NoopTxManager{})

	paid, err := svc.Pay(ctx, order.ID, "u_1001", domain.RoleBuyer)
	if err != nil {
		t.Fatalf("pay: %v", err)
	}
	if paid.Status != domain.OrderStatusPaid || paid.PayStatus != domain.PayStatusPaid || paid.PaidAt == nil || paid.Version != order.Version+1 {
		t.Fatalf("unexpected paid order: %+v", paid)
	}
	closed, err := svc.CloseExpiredOnce(ctx, deadline.Add(time.Hour), 10)
	if err != nil {
		t.Fatalf("close expired after pay: %v", err)
	}
	if closed != 0 {
		t.Fatalf("expected paid order not to timeout, closed=%d", closed)
	}
	got, err := repo.FindByID(ctx, order.ID)
	if err != nil {
		t.Fatalf("find order: %v", err)
	}
	if got.Status != domain.OrderStatusPaid || got.Version != paid.Version {
		t.Fatalf("paid order was changed by timeout scan: %+v", got)
	}
}

func TestOrderStatusCASResolvesPayTimeoutRace(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 30, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name         string
		timeoutFirst bool
		wantStatus   domain.OrderStatus
	}{
		{name: "pay_wins", wantStatus: domain.OrderStatusPaid},
		{name: "timeout_wins", timeoutFirst: true, wantStatus: domain.OrderStatusTimeout},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := repository.NewMemoryOrderRepository()
			deadline := now
			order, _, err := repo.CreateIfAbsentByAuction(ctx, &domain.OrderDeal{
				AuctionID:     uint64(20000 + i),
				WinnerID:      "u_1001",
				SellerID:      "u_2001",
				DealPrice:     12000,
				DepositAmount: 1000,
				Status:        domain.OrderStatusCreated,
				PayStatus:     domain.PayStatusUnpaid,
				PayDeadline:   &deadline,
			})
			if err != nil {
				t.Fatalf("seed order: %v", err)
			}
			paySnapshot, err := repo.FindByID(ctx, order.ID)
			if err != nil {
				t.Fatalf("read pay snapshot: %v", err)
			}
			timeoutSnapshot, err := repo.FindByID(ctx, order.ID)
			if err != nil {
				t.Fatalf("read timeout snapshot: %v", err)
			}

			if tt.timeoutFirst {
				if err := timeoutSnapshot.MarkTimeout(now); err != nil {
					t.Fatalf("mark timeout: %v", err)
				}
				if err := repo.UpdateStatusWithVersion(ctx, &timeoutSnapshot, order.Version, []domain.OrderStatus{domain.OrderStatusCreated}); err != nil {
					t.Fatalf("timeout CAS: %v", err)
				}
				if err := paySnapshot.MarkPaid(now); err != nil {
					t.Fatalf("mark paid: %v", err)
				}
				if err := repo.UpdateStatusWithVersion(ctx, &paySnapshot, order.Version, []domain.OrderStatus{domain.OrderStatusCreated}); !errors.Is(err, domain.ErrInvalidState) {
					t.Fatalf("expected stale pay CAS invalid state, got %v", err)
				}
			} else {
				if err := paySnapshot.MarkPaid(now); err != nil {
					t.Fatalf("mark paid: %v", err)
				}
				if err := repo.UpdateStatusWithVersion(ctx, &paySnapshot, order.Version, []domain.OrderStatus{domain.OrderStatusCreated}); err != nil {
					t.Fatalf("pay CAS: %v", err)
				}
				if err := timeoutSnapshot.MarkTimeout(now); err != nil {
					t.Fatalf("mark timeout: %v", err)
				}
				if err := repo.UpdateStatusWithVersion(ctx, &timeoutSnapshot, order.Version, []domain.OrderStatus{domain.OrderStatusCreated}); !errors.Is(err, domain.ErrInvalidState) {
					t.Fatalf("expected stale timeout CAS invalid state, got %v", err)
				}
			}

			got, err := repo.FindByID(ctx, order.ID)
			if err != nil {
				t.Fatalf("find final order: %v", err)
			}
			if got.Status != tt.wantStatus || got.Version != order.Version+1 {
				t.Fatalf("unexpected final order: %+v", got)
			}
		})
	}
}

package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"aieas_backend/internal/domain"
	orderapp "aieas_backend/internal/modules/order/app"
	"aieas_backend/internal/tests/repository"
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
	svc := orderapp.NewOrderService(repo, repository.NoopTxManager{})

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
	svc := orderapp.NewOrderService(repo, repository.NoopTxManager{})

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

func TestOrderServiceShipAndReceiveLifecycle(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	repo := repository.NewMemoryOrderRepository()
	order, _, err := repo.CreateIfAbsentByAuction(ctx, &domain.OrderDeal{
		AuctionID:         10003,
		WinnerID:          "u_1001",
		SellerID:          "u_2001",
		DealPrice:         12000,
		DepositAmount:     1000,
		Status:            domain.OrderStatusPaid,
		PayStatus:         domain.PayStatusPaid,
		FulfillmentStatus: domain.FulfillmentStatusUnshipped,
		PaidAt:            &now,
	})
	if err != nil {
		t.Fatalf("seed order: %v", err)
	}
	svc := orderapp.NewOrderService(repo, repository.NoopTxManager{})

	if _, err := svc.Receive(ctx, order.ID, "u_1001", domain.RoleBuyer); !errors.Is(err, domain.ErrInvalidState) {
		t.Fatalf("expected receive before ship invalid state, got %v", err)
	}
	if _, err := svc.Ship(ctx, order.ID, "u_1001", domain.RoleBuyer); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("expected buyer ship forbidden, got %v", err)
	}
	shipped, err := svc.Ship(ctx, order.ID, "u_2001", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("ship: %v", err)
	}
	if shipped.FulfillmentStatus != domain.FulfillmentStatusShipped || shipped.ShippedAt == nil || shipped.ReceivedAt != nil || shipped.Version != order.Version+1 {
		t.Fatalf("unexpected shipped order: %+v", shipped)
	}
	shippedAgain, err := svc.Ship(ctx, order.ID, "u_2001", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("ship again should be idempotent: %v", err)
	}
	if shippedAgain.FulfillmentStatus != domain.FulfillmentStatusShipped || shippedAgain.Version != shipped.Version {
		t.Fatalf("unexpected idempotent ship result: %+v", shippedAgain)
	}
	if _, err := svc.Receive(ctx, order.ID, "u_2001", domain.RoleMerchant); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("expected merchant receive forbidden, got %v", err)
	}
	received, err := svc.Receive(ctx, order.ID, "u_1001", domain.RoleBuyer)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if received.FulfillmentStatus != domain.FulfillmentStatusReceived || received.ShippedAt == nil || received.ReceivedAt == nil || received.Version != shipped.Version+1 {
		t.Fatalf("unexpected received order: %+v", received)
	}
	receivedAgain, err := svc.Receive(ctx, order.ID, "u_1001", domain.RoleBuyer)
	if err != nil {
		t.Fatalf("receive again should be idempotent: %v", err)
	}
	if receivedAgain.FulfillmentStatus != domain.FulfillmentStatusReceived || receivedAgain.Version != received.Version {
		t.Fatalf("unexpected idempotent receive result: %+v", receivedAgain)
	}
}

func TestOrderServiceShipRequiresPaidOrder(t *testing.T) {
	ctx := context.Background()
	deadline := time.Now().UTC().Add(time.Hour)
	repo := repository.NewMemoryOrderRepository()
	order, _, err := repo.CreateIfAbsentByAuction(ctx, &domain.OrderDeal{
		AuctionID:         10004,
		WinnerID:          "u_1001",
		SellerID:          "u_2001",
		DealPrice:         12000,
		DepositAmount:     1000,
		Status:            domain.OrderStatusCreated,
		PayStatus:         domain.PayStatusUnpaid,
		FulfillmentStatus: domain.FulfillmentStatusUnshipped,
		PayDeadline:       &deadline,
	})
	if err != nil {
		t.Fatalf("seed order: %v", err)
	}
	svc := orderapp.NewOrderService(repo, repository.NoopTxManager{})

	if _, err := svc.Ship(ctx, order.ID, "u_2001", domain.RoleMerchant); !errors.Is(err, domain.ErrInvalidState) {
		t.Fatalf("expected unpaid order ship invalid state, got %v", err)
	}
}

func TestOrderServiceMatchesPrefixedAndNumericUserIDs(t *testing.T) {
	ctx := context.Background()
	deadline := time.Now().UTC().Add(time.Hour)
	repo := repository.NewMemoryOrderRepository()
	order, _, err := repo.CreateIfAbsentByAuction(ctx, &domain.OrderDeal{
		AuctionID:         10005,
		WinnerID:          "1001",
		SellerID:          "2001",
		DealPrice:         12000,
		DepositAmount:     1000,
		Status:            domain.OrderStatusCreated,
		PayStatus:         domain.PayStatusUnpaid,
		FulfillmentStatus: domain.FulfillmentStatusUnshipped,
		PayDeadline:       &deadline,
	})
	if err != nil {
		t.Fatalf("seed order: %v", err)
	}
	svc := orderapp.NewOrderService(repo, repository.NoopTxManager{})

	mine, err := svc.Mine(ctx, "u_1001", domain.RoleBuyer, domain.OrderFilter{})
	if err != nil {
		t.Fatalf("mine: %v", err)
	}
	if len(mine) != 1 || mine[0].ID != order.ID {
		t.Fatalf("unexpected mine orders: %+v", mine)
	}
	paid, err := svc.Pay(ctx, order.ID, "u_1001", domain.RoleBuyer)
	if err != nil {
		t.Fatalf("pay with prefixed buyer id: %v", err)
	}
	shipped, err := svc.Ship(ctx, paid.ID, "u_2001", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("ship with prefixed merchant id: %v", err)
	}
	if _, err := svc.Receive(ctx, shipped.ID, "1001", domain.RoleBuyer); err != nil {
		t.Fatalf("receive with numeric buyer id: %v", err)
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

package service

import (
	"context"
	"testing"

	"aieas_backend/internal/domain"
	adminapp "aieas_backend/internal/modules/admin/app"
	"aieas_backend/internal/tests/repository"
)

func TestAdminServiceAuditAuctionRejectsToAuditRejected(t *testing.T) {
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	auctionSvc := NewAuctionService(auctionRepo, repository.NoopTxManager{})
	adminSvc := adminapp.NewAdminService(nil, auctionSvc, nil, nil, nil, nil)

	lot, err := auctionSvc.Create(ctx, CreateAuctionInput{
		ActorID:        "u_2001",
		ActorRole:      domain.RoleMerchant,
		Title:          "Rejected Watch",
		Category:       "luxury",
		ConditionGrade: domain.ConditionGood,
		Description:    "rare watch",
		StartPrice:     1000,
		ReservePrice:   5000,
		CapPrice:       6000,
		AuctionType:    domain.AuctionTypeEnglish,
		DepositAmount:  100,
		Status:         domain.AuctionStatusPendingAudit,
	})
	if err != nil {
		t.Fatalf("create auction: %v", err)
	}

	rejected, err := adminSvc.AuditAuction(ctx, lot.AuctionID, false, "admin")
	if err != nil {
		t.Fatalf("reject auction: %v", err)
	}
	if rejected.Status != domain.AuctionStatusAuditRejected {
		t.Fatalf("expected AUDIT_REJECTED, got %s", rejected.Status)
	}
}

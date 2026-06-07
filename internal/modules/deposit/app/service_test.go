package app

import (
	"context"
	"testing"
	"time"

	"aieas_backend/internal/domain"
	mysqlinfra "aieas_backend/internal/infra/mysql"
	realtimeinfra "aieas_backend/internal/infra/realtime"
	auctionrepo "aieas_backend/internal/modules/auction/repository"
	depositrepo "aieas_backend/internal/modules/deposit/repository"
)

type recordingParticipantNotifier struct {
	auctionID        uint64
	participantCount int
	calls            int
}

func (r *recordingParticipantNotifier) NotifyParticipantUpdated(ctx context.Context, auctionID uint64, participantCount int) int {
	_ = ctx
	r.auctionID = auctionID
	r.participantCount = participantCount
	r.calls++
	return 1
}

func TestEnrollBroadcastsParticipantCount(t *testing.T) {
	ctx := context.Background()
	auctionRepo := auctionrepo.NewMemoryAuctionRepository()
	depositRepo := depositrepo.NewMemoryDepositRepository()
	realtime := realtimeinfra.NewMemoryRealtimeStore()
	notifier := &recordingParticipantNotifier{}
	svc := NewDepositService(depositRepo, auctionRepo, realtime, nil, mysqlinfra.NoopTxManager{})
	svc.SetParticipantNotifier(notifier)

	auction := domain.AuctionLot{
		AuctionID:      90000021,
		SellerID:       "merchant_01",
		Title:          "参与人数广播测试拍品",
		Category:       "jewelry",
		ConditionGrade: domain.ConditionGood,
		StartPrice:     1000,
		ReservePrice:   1000,
		CapPrice:       2000,
		IncrementRule:  []byte(`{"type":"fixed","amount":100,"maxBidSteps":10}`),
		DepositAmount:  100,
		Status:         domain.AuctionStatusRunning,
		StartTime:      time.Now().UTC().Add(-time.Minute),
		EndTime:        time.Now().UTC().Add(time.Minute),
	}
	if err := auctionRepo.Create(ctx, &auction); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	if _, err := realtime.InitAuction(ctx, auction, 100); err != nil {
		t.Fatalf("init realtime auction: %v", err)
	}

	deposit, err := svc.Enroll(ctx, EnrollInput{AuctionID: auction.AuctionID, UserID: "u_1001", UserRole: domain.RoleBuyer})
	if err != nil {
		t.Fatalf("enroll: %v", err)
	}
	if deposit.Status != domain.DepositStatusReady {
		t.Fatalf("expected ready deposit, got %+v", deposit)
	}
	if notifier.calls != 1 || notifier.auctionID != auction.AuctionID || notifier.participantCount != 1 {
		t.Fatalf("expected participant broadcast count=1, got calls=%d auctionID=%d count=%d", notifier.calls, notifier.auctionID, notifier.participantCount)
	}
}

package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

type mcpControlFixture struct {
	svc      *MCPControlService
	auctions *repository.MemoryAuctionRepository
	sessions *repository.MemoryLiveSessionRepository
	session  domain.LiveSession
}

func newMCPControlFixture(t *testing.T) mcpControlFixture {
	t.Helper()
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	sessionRepo := repository.NewMemoryLiveSessionRepository()
	realtimeStore := repository.NewMemoryRealtimeStore()
	auctionSvc := NewAuctionService(auctionRepo, nil, repository.NoopTxManager{})
	auctionSvc.SetRealtime(realtimeStore)
	sessionSvc := NewLiveSessionService(sessionRepo, auctionRepo)
	sessionSvc.SetWriteDeps(repository.NoopTxManager{}, repository.NewMemoryLiveSessionLock(), auctionSvc)
	sessionSvc.SetStatsDeps(repository.NewMemoryBidRepository(), realtimeStore, nil)
	now := time.Now().UTC()
	session := domain.LiveSession{
		MerchantID: "m_1",
		Title:      "直播场次",
		Status:     domain.LiveSessionStatusLive,
		OpenedAt:   &now,
	}
	if err := sessionRepo.Create(ctx, &session); err != nil {
		t.Fatalf("create session: %v", err)
	}
	svc := NewMCPControlService(MCPLiveControlDependencies{
		Auctions:       auctionRepo,
		Sessions:       sessionRepo,
		LiveSessionSvc: sessionSvc,
		AuctionSvc:     auctionSvc,
	})
	return mcpControlFixture{svc: svc, auctions: auctionRepo, sessions: sessionRepo, session: session}
}

func TestMCPControlServiceReadAndOperate(t *testing.T) {
	fixture := newMCPControlFixture(t)
	ctx := context.Background()
	lot := mcpControlReadyLot(91001, "m_1")
	if err := fixture.auctions.Create(ctx, &lot); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	actor := MCPActor{ID: "m_1", Role: domain.RoleMerchant}

	contextPayload, err := fixture.svc.ReadMerchantLiveControlContext(ctx, "m_1", actor)
	if err != nil {
		t.Fatalf("read control context: %v", err)
	}
	if contextPayload.Session == nil || contextPayload.Session.ID != fixture.session.ID {
		t.Fatalf("expected active session, got %+v", contextPayload.Session)
	}
	if len(contextPayload.Lots.CandidateLots) != 1 || contextPayload.Lots.CandidateLots[0].AuctionID != lot.AuctionID {
		t.Fatalf("expected candidate lot, got %+v", contextPayload.Lots.CandidateLots)
	}

	result, err := fixture.svc.OperateLiveSessionLot(ctx, MCPLiveLotOperationInput{
		LiveSessionID: fixture.session.ID,
		AuctionID:     lot.AuctionID,
		Action:        "上架",
	}, actor)
	if err != nil {
		t.Fatalf("operate on shelf: %v", err)
	}
	if result.Action != "onShelf" || result.Lot == nil || result.Lot.LiveSessionID == nil || *result.Lot.LiveSessionID != fixture.session.ID {
		t.Fatalf("unexpected operation result: %+v", result)
	}
	if result.Context == nil || len(result.Context.Lots.UpcomingLots) != 1 {
		t.Fatalf("expected refreshed context with upcoming lot, got %+v", result.Context)
	}

	result, err = fixture.svc.OperateLiveSessionLot(ctx, MCPLiveLotOperationInput{
		LiveSessionID: fixture.session.ID,
		AuctionID:     lot.AuctionID,
		Action:        "startExplain",
		DurationSec:   600,
	}, actor)
	if err != nil {
		t.Fatalf("operate start explain: %v", err)
	}
	if result.Context == nil || result.Context.CurrentAuctionState == nil {
		t.Fatalf("expected refreshed context with current auction state, got %+v", result.Context)
	}
	if result.Context.CurrentAuctionState.AuctionID != lot.AuctionID || result.Context.CurrentAuctionState.CurrentPrice != lot.StartPrice {
		t.Fatalf("unexpected current auction state: %+v", result.Context.CurrentAuctionState)
	}
	if result.Context.CurrentAuctionState.RemainSeconds <= 0 {
		t.Fatalf("expected positive remain seconds, got %+v", result.Context.CurrentAuctionState)
	}

}

func TestMCPControlServiceErrors(t *testing.T) {
	tests := []struct {
		name    string
		run     func(context.Context, mcpControlFixture) error
		wantErr error
	}{
		{
			name: "merchant cannot read another merchant context",
			run: func(ctx context.Context, fixture mcpControlFixture) error {
				_, err := fixture.svc.ReadMerchantLiveControlContext(ctx, "m_1", MCPActor{ID: "m_2", Role: domain.RoleMerchant})
				return err
			},
			wantErr: domain.ErrForbidden,
		},
		{
			name: "operate rejects ended session",
			run: func(ctx context.Context, fixture mcpControlFixture) error {
				now := time.Now().UTC()
				ended := domain.LiveSession{MerchantID: "m_1", Status: domain.LiveSessionStatusEnded, OpenedAt: &now}
				if err := fixture.sessions.Create(ctx, &ended); err != nil {
					return err
				}
				lot := mcpControlReadyLot(91002, "m_1")
				if err := fixture.auctions.Create(ctx, &lot); err != nil {
					return err
				}
				_, err := fixture.svc.OperateLiveSessionLot(ctx, MCPLiveLotOperationInput{LiveSessionID: ended.ID, AuctionID: lot.AuctionID, Action: "onShelf"}, MCPActor{ID: "m_1", Role: domain.RoleMerchant})
				return err
			},
			wantErr: domain.ErrInvalidState,
		},
		{
			name: "operate rejects unknown action",
			run: func(ctx context.Context, fixture mcpControlFixture) error {
				lot := mcpControlReadyLot(91003, "m_1")
				if err := fixture.auctions.Create(ctx, &lot); err != nil {
					return err
				}
				_, err := fixture.svc.OperateLiveSessionLot(ctx, MCPLiveLotOperationInput{LiveSessionID: fixture.session.ID, AuctionID: lot.AuctionID, Action: "unknown"}, MCPActor{ID: "m_1", Role: domain.RoleMerchant})
				return err
			},
			wantErr: domain.ErrInvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.run(context.Background(), newMCPControlFixture(t)); !errors.Is(err, tt.wantErr) {
				t.Fatalf("expected %v, got %v", tt.wantErr, err)
			}
		})
	}
}

func mcpControlReadyLot(id uint64, sellerID string) domain.AuctionLot {
	now := time.Now().UTC()
	return domain.AuctionLot{
		AuctionID:     id,
		ItemID:        id + 1000,
		SellerID:      sellerID,
		AuctionType:   domain.AuctionTypeEnglish,
		StartPrice:    1000,
		IncrementRule: domain.DefaultIncrementRule(),
		Status:        domain.AuctionStatusReady,
		StartTime:     now,
		EndTime:       now.Add(time.Hour),
		DurationSec:   600,
	}
}

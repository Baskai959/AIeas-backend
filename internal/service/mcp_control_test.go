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
	rooms    *repository.MemoryLiveRoomRepository
	sessions *repository.MemoryLiveSessionRepository
	room     domain.LiveRoom
	session  domain.LiveSession
}

func newMCPControlFixture(t *testing.T) mcpControlFixture {
	t.Helper()
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	roomRepo := repository.NewMemoryLiveRoomRepository()
	sessionRepo := repository.NewMemoryLiveSessionRepository()
	roomSvc := NewLiveRoomService(roomRepo, auctionRepo, repository.NoopTxManager{}, repository.NewMemoryLiveRoomLock())
	room, err := roomSvc.Create(ctx, CreateLiveRoomInput{
		ActorID:   "m_1",
		ActorRole: domain.RoleMerchant,
		Title:     "直播间",
		Status:    domain.LiveRoomStatusLive,
	})
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	now := time.Now().UTC()
	session := domain.LiveSession{
		LiveRoomID: room.ID,
		MerchantID: room.MerchantID,
		Title:      room.Title,
		Status:     domain.LiveSessionStatusLive,
		OpenedAt:   now,
	}
	if err := sessionRepo.Create(ctx, &session); err != nil {
		t.Fatalf("create session: %v", err)
	}
	svc := NewMCPControlService(MCPLiveControlDependencies{
		Auctions:    auctionRepo,
		Rooms:       roomRepo,
		Sessions:    sessionRepo,
		LiveRoomSvc: roomSvc,
	})
	return mcpControlFixture{svc: svc, auctions: auctionRepo, rooms: roomRepo, sessions: sessionRepo, room: room, session: session}
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
	if result.Action != "onShelf" || result.Lot == nil || result.Lot.LiveRoomID != fixture.room.ID {
		t.Fatalf("unexpected operation result: %+v", result)
	}
	if result.Context == nil || len(result.Context.Lots.UpcomingLots) != 1 {
		t.Fatalf("expected refreshed context with upcoming lot, got %+v", result.Context)
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
				ended := domain.LiveSession{LiveRoomID: fixture.room.ID, MerchantID: "m_1", Status: domain.LiveSessionStatusEnded, OpenedAt: time.Now().UTC()}
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
		AuctionID:   id,
		ItemID:      id + 1000,
		SellerID:    sellerID,
		AuctionType: domain.AuctionTypeEnglish,
		StartPrice:  1000,
		Status:      domain.AuctionStatusReady,
		StartTime:   now,
		EndTime:     now.Add(time.Hour),
		DurationSec: 600,
	}
}

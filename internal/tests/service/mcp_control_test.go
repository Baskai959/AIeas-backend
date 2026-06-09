package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/tests/repository"
)

type mcpControlFixture struct {
	svc         *MCPControlService
	auctions    *repository.MemoryAuctionRepository
	sessions    *repository.MemoryLiveSessionRepository
	session     domain.LiveSession
	synthesizer *fakeLiveVoiceSynthesizer
	broadcaster *fakeLiveVoiceBroadcaster
}

func newMCPControlFixture(t *testing.T) mcpControlFixture {
	t.Helper()
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	sessionRepo := repository.NewMemoryLiveSessionRepository()
	realtimeStore := repository.NewMemoryRealtimeStore()
	bidRepo := repository.NewMemoryBidRepository()
	auctionSvc := NewAuctionServiceWithDeps(AuctionServiceDeps{Auctions: auctionRepo, Tx: repository.NoopTxManager{}, Realtime: realtimeStore})
	sessionSvc := NewLiveSessionServiceWithDeps(LiveSessionServiceDeps{Sessions: sessionRepo, Auctions: auctionRepo, Tx: repository.NoopTxManager{}, Lock: repository.NewMemoryLiveSessionLock(), Auction: auctionSvc, Bids: bidRepo, AuctionRealtime: realtimeStore})
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
	synthesizer := &fakeLiveVoiceSynthesizer{result: LiveVoiceSynthesisResult{
		Audio:       []byte{0x01, 0x02, 0x03},
		AudioFormat: "pcm_s16le",
		Encoding:    "pcm_s16le",
		SampleRate:  24000,
		Channels:    1,
		Voice:       "zh_female_vv_jupiter_bigtts",
		Provider:    "doubao",
	}}
	broadcaster := &fakeLiveVoiceBroadcaster{delivered: 1}
	svc := NewMCPControlService(MCPLiveControlDependencies{
		Auctions:             auctionRepo,
		Sessions:             sessionRepo,
		LiveSessionSvc:       sessionSvc,
		AuctionSvc:           auctionSvc,
		LiveVoiceSynthesizer: synthesizer,
		LiveVoiceBroadcaster: broadcaster,
	})
	return mcpControlFixture{svc: svc, auctions: auctionRepo, sessions: sessionRepo, session: session, synthesizer: synthesizer, broadcaster: broadcaster}
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

	voice, err := fixture.svc.CreateLiveVoiceBroadcast(ctx, MCPLiveVoiceBroadcastInput{
		LiveSessionID: fixture.session.ID,
		Text:          "请大家关注当前拍品的品相细节。",
		RequestID:     "voice-1",
	}, actor)
	if err != nil {
		t.Fatalf("create live voice broadcast: %v", err)
	}
	if voice.Status != "BROADCASTED" || voice.LiveSessionID != fixture.session.ID || voice.RequestID != "voice-1" || voice.Text == "" || voice.AudioBytes != 3 || voice.Delivered != 1 {
		t.Fatalf("unexpected voice broadcast result: %+v", voice)
	}
	if fixture.synthesizer.input.Text != voice.Text || fixture.broadcaster.payload.AudioBase64 == "" {
		t.Fatalf("expected synthesized audio payload, input=%+v payload=%+v", fixture.synthesizer.input, fixture.broadcaster.payload)
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
		{
			name: "start explain requires durationSec",
			run: func(ctx context.Context, fixture mcpControlFixture) error {
				lot := mcpControlReadyLot(91004, "m_1")
				if err := fixture.auctions.Create(ctx, &lot); err != nil {
					return err
				}
				if _, err := fixture.svc.OperateLiveSessionLot(ctx, MCPLiveLotOperationInput{LiveSessionID: fixture.session.ID, AuctionID: lot.AuctionID, Action: "onShelf"}, MCPActor{ID: "m_1", Role: domain.RoleMerchant}); err != nil {
					return err
				}
				_, err := fixture.svc.OperateLiveSessionLot(ctx, MCPLiveLotOperationInput{LiveSessionID: fixture.session.ID, AuctionID: lot.AuctionID, Action: "startExplain"}, MCPActor{ID: "m_1", Role: domain.RoleMerchant})
				return err
			},
			wantErr: domain.ErrInvalidArgument,
		},
		{
			name: "voice rejects empty text",
			run: func(ctx context.Context, fixture mcpControlFixture) error {
				_, err := fixture.svc.CreateLiveVoiceBroadcast(ctx, MCPLiveVoiceBroadcastInput{LiveSessionID: fixture.session.ID, Text: "   "}, MCPActor{ID: "m_1", Role: domain.RoleMerchant})
				return err
			},
			wantErr: domain.ErrInvalidArgument,
		},
		{
			name: "voice rejects buyer actor",
			run: func(ctx context.Context, fixture mcpControlFixture) error {
				_, err := fixture.svc.CreateLiveVoiceBroadcast(ctx, MCPLiveVoiceBroadcastInput{LiveSessionID: fixture.session.ID, Text: "hello"}, MCPActor{ID: "u_1", Role: domain.RoleBuyer})
				return err
			},
			wantErr: domain.ErrForbidden,
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

func TestMCPLiveLotActionMessagesUseLotName(t *testing.T) {
	lotName := "复古机械表"
	messages := []string{
		mcpLiveLotActionApprovalMessage("startExplain", lotName),
		mcpLiveLotActionRunningMessage("startExplain", lotName),
		mcpLiveLotActionCompletedMessage("startExplain", lotName),
	}
	for _, message := range messages {
		if !strings.Contains(message, lotName) {
			t.Fatalf("expected message to contain lot name %q, got %q", lotName, message)
		}
		if strings.Contains(message, "91001") {
			t.Fatalf("expected message not to expose auction id, got %q", message)
		}
	}
}

func mcpControlReadyLot(id uint64, sellerID string) domain.AuctionLot {
	now := time.Now().UTC()
	return domain.AuctionLot{
		AuctionID:      id,
		SellerID:       sellerID,
		AuctionType:    domain.AuctionTypeEnglish,
		StartPrice:     1000,
		IncrementRule:  domain.DefaultIncrementRule(),
		AntiSnipingSec: 15,
		AntiExtendSec:  30,
		AntiExtendMode: domain.AuctionExtendModeAdd,
		Status:         domain.AuctionStatusReady,
		StartTime:      now,
		EndTime:        now.Add(time.Hour),
		DurationSec:    600,
	}
}

type fakeLiveVoiceSynthesizer struct {
	input  LiveVoiceSynthesisInput
	result LiveVoiceSynthesisResult
	err    error
}

func (f *fakeLiveVoiceSynthesizer) SynthesizeLiveVoice(ctx context.Context, in LiveVoiceSynthesisInput) (LiveVoiceSynthesisResult, error) {
	_ = ctx
	f.input = in
	return f.result, f.err
}

type fakeLiveVoiceBroadcaster struct {
	delivered int
	payload   LiveVoiceBroadcastPayload
	err       error
}

func (f *fakeLiveVoiceBroadcaster) BroadcastLiveVoice(ctx context.Context, liveSessionID uint64, payload LiveVoiceBroadcastPayload) (int, error) {
	_ = ctx
	_ = liveSessionID
	f.payload = payload
	return f.delivered, f.err
}

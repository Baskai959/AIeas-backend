package http

import (
	"context"
	"testing"

	"aieas_backend/internal/domain"
	livesessionapp "aieas_backend/internal/modules/live_session/app"
)

func TestLiveSessionViewIncludesPlaybackModeFromAIAssistantSnapshot(t *testing.T) {
	session := domain.LiveSession{ID: 9001, MerchantID: "u_2001", Title: "春拍直播", Status: domain.LiveSessionStatusLive}
	tests := []struct {
		name        string
		enabled     bool
		videoSource string
	}{
		{name: "digital human", enabled: true, videoSource: "digitalHuman"},
		{name: "recorded", enabled: false, videoSource: "recorded"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := NewLiveSessionHandler(nil, fakeLiveSessionQuery{
				snapshot: livesessionapp.AIAssistantSwitchSnapshot{
					LiveSessionID: session.ID,
					MerchantID:    session.MerchantID,
					Enabled:       tt.enabled,
				},
			}, nil)
			handler.SetMarketplaceService(fakeLiveSessionPresenter{
				view: domain.LiveSessionView{LiveSession: session, MerchantName: "云上珠宝"},
			})

			got, ok := handler.liveSessionView(context.Background(), session).(domain.LiveSessionView)
			if !ok {
				t.Fatalf("expected LiveSessionView, got %T", got)
			}
			if got.AIAssistantEnabled != tt.enabled {
				t.Fatalf("expected aiAssistantEnabled=%v, got %v", tt.enabled, got.AIAssistantEnabled)
			}
			if got.VideoSource != tt.videoSource {
				t.Fatalf("expected videoSource=%q, got %q", tt.videoSource, got.VideoSource)
			}
		})
	}
}

type fakeLiveSessionQuery struct {
	snapshot livesessionapp.AIAssistantSwitchSnapshot
}

func (f fakeLiveSessionQuery) ListVisibleFiltered(ctx context.Context, filter domain.LiveSessionFilter, actorID string, actorRole domain.Role) ([]domain.LiveSession, error) {
	_ = ctx
	_ = filter
	_ = actorID
	_ = actorRole
	return nil, nil
}

func (f fakeLiveSessionQuery) ListByMerchantFiltered(ctx context.Context, filter domain.LiveSessionFilter, actorID string, actorRole domain.Role) ([]domain.LiveSession, error) {
	_ = ctx
	_ = filter
	_ = actorID
	_ = actorRole
	return nil, nil
}

func (f fakeLiveSessionQuery) Get(ctx context.Context, id uint64) (domain.LiveSession, error) {
	_ = ctx
	_ = id
	return domain.LiveSession{}, nil
}

func (f fakeLiveSessionQuery) ListLots(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) ([]domain.AuctionLot, error) {
	_ = ctx
	_ = sessionID
	_ = actorID
	_ = actorRole
	return nil, nil
}

func (f fakeLiveSessionQuery) ListAuctionBids(ctx context.Context, sessionID, auctionID uint64, limit int, actorID string, actorRole domain.Role) ([]domain.BidRecord, error) {
	_ = ctx
	_ = sessionID
	_ = auctionID
	_ = limit
	_ = actorID
	_ = actorRole
	return nil, nil
}

func (f fakeLiveSessionQuery) ListBids(ctx context.Context, sessionID uint64, limit int, actorID string, actorRole domain.Role) ([]domain.BidRecord, error) {
	_ = ctx
	_ = sessionID
	_ = limit
	_ = actorID
	_ = actorRole
	return nil, nil
}

func (f fakeLiveSessionQuery) ListOrders(ctx context.Context, sessionID uint64, limit, offset int, actorID string, actorRole domain.Role) ([]domain.OrderDeal, error) {
	_ = ctx
	_ = sessionID
	_ = limit
	_ = offset
	_ = actorID
	_ = actorRole
	return nil, nil
}

func (f fakeLiveSessionQuery) Stats(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) (livesessionapp.LiveSessionStats, error) {
	_ = ctx
	_ = sessionID
	_ = actorID
	_ = actorRole
	return livesessionapp.LiveSessionStats{}, nil
}

func (f fakeLiveSessionQuery) AgentHookConfig(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) (livesessionapp.LiveAgentHookConfig, error) {
	_ = ctx
	_ = sessionID
	_ = actorID
	_ = actorRole
	return livesessionapp.LiveAgentHookConfig{}, nil
}

func (f fakeLiveSessionQuery) AIAssistantSwitchSnapshot(ctx context.Context, sessionID uint64) (livesessionapp.AIAssistantSwitchSnapshot, error) {
	_ = ctx
	_ = sessionID
	return f.snapshot, nil
}

type fakeLiveSessionPresenter struct {
	view domain.LiveSessionView
}

func (f fakeLiveSessionPresenter) LiveSessionView(ctx context.Context, session domain.LiveSession) domain.LiveSessionView {
	_ = ctx
	if f.view.ID == 0 {
		return domain.LiveSessionView{LiveSession: session}
	}
	return f.view
}

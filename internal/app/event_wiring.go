package app

import (
	"context"
	"encoding/json"

	"aieas_backend/internal/domain"
	aiapp "aieas_backend/internal/modules/ai/app"
	liveanalysisapp "aieas_backend/internal/modules/live_analysis/app"
	mcpapp "aieas_backend/internal/modules/mcp/app"
	wstransport "aieas_backend/internal/transport/ws"
)

type RealtimeEventPublisher interface {
	PublishAuctionEvent(ctx context.Context, auctionID uint64, eventType, requestID string, seq int64, payload json.RawMessage) error
	PublishLiveSessionEvent(ctx context.Context, liveSessionID uint64, eventType, requestID string, seq int64, payload json.RawMessage, onlineOnly bool) error
	PublishLiveSessionUserEvent(ctx context.Context, liveSessionID uint64, userID, eventType, requestID string, seq int64, payload json.RawMessage) error
}

// buildLiveSessionEndedHook 构造 LiveSession 闭播完成后的回调。
//
// 闭播路径会在 LiveSessionService.CloseSession 完成 MySQL 状态机切换后异步触发：
// 通过实时事件总线或 Hub.BroadcastSessionEnd 把 live_session.ended 事件推送给所有订阅
// 了该 sessionID 的客户端，并触发本场直播的 AI 总结报告生成。
//
// eventPublisher、hub 和 liveAnalysis 都为空时返回 nil，使 LiveSessionService 跳过回调注入。
func buildLiveSessionEndedHook(hub *wstransport.Hub, eventPublisher RealtimeEventPublisher, liveAnalysis *liveanalysisapp.LiveAnalysisService) func(ctx context.Context, session domain.LiveSession) {
	if hub == nil && eventPublisher == nil && liveAnalysis == nil {
		return nil
	}
	return func(ctx context.Context, session domain.LiveSession) {
		if session.ID == 0 {
			return
		}
		payload, _ := json.Marshal(map[string]interface{}{
			"liveSessionId": session.ID,
			"status":        session.Status,
		})
		if eventPublisher != nil {
			if err := eventPublisher.PublishLiveSessionEvent(ctx, session.ID, "live_session.ended", "", 0, payload, false); err == nil {
				if liveAnalysis != nil {
					_, _ = liveAnalysis.StartReportForSession(ctx, session)
				}
				return
			}
		}
		if hub != nil {
			hub.BroadcastSessionEnd(session.ID, payload)
		}
		if liveAnalysis != nil {
			_, _ = liveAnalysis.StartReportForSession(ctx, session)
		}
	}
}

type liveVoiceHubBroadcaster struct {
	hub            *wstransport.Hub
	eventPublisher RealtimeEventPublisher
}

type liveSessionLotHubNotifier struct {
	hub            *wstransport.Hub
	eventPublisher RealtimeEventPublisher
}

type aiAssistantHubNotifier struct {
	hub            *wstransport.Hub
	eventPublisher RealtimeEventPublisher
}

type bidResultDelivery struct {
	coordinator    *wstransport.BidAsyncCoordinator
	eventPublisher RealtimeEventPublisher
}

func (d bidResultDelivery) DeliverBidResult(sessionID, auctionID uint64, userID string, payload wstransport.BidResultPayload) {
	if d.eventPublisher != nil && sessionID != 0 && userID != "" {
		raw, err := json.Marshal(payload)
		if err == nil {
			if err := d.eventPublisher.PublishLiveSessionUserEvent(context.Background(), sessionID, userID, wstransport.TypeBidResult, payload.BidID, payload.ResultSeq, raw); err == nil {
				return
			}
		}
	}
	if d.coordinator != nil {
		d.coordinator.DeliverBidResult(sessionID, auctionID, userID, payload)
	}
}

func (n aiAssistantHubNotifier) NotifyAIAssistantEvent(ctx context.Context, liveSessionID uint64, event aiapp.Event) (int, error) {
	if liveSessionID == 0 {
		return 0, nil
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return 0, err
	}
	eventType := wstransport.TypeAIAssistantStatus
	switch event.Kind {
	case "permission":
		if event.Status == "pending" {
			eventType = wstransport.TypeAIAssistantPermissionRequest
		}
	case "broadcast":
		eventType = wstransport.TypeAIAssistantBroadcast
	case "switch":
		eventType = wstransport.TypeAIAssistantSwitch
	}
	if n.eventPublisher != nil {
		if err := n.eventPublisher.PublishLiveSessionEvent(ctx, liveSessionID, eventType, event.RequestID, 0, raw, false); err == nil {
			return 0, nil
		} else if n.hub == nil {
			return 0, err
		}
	}
	if n.hub == nil {
		return 0, nil
	}
	return n.hub.BroadcastLiveSession(liveSessionID, wstransport.Envelope{
		Type:      eventType,
		RequestID: event.RequestID,
		Payload:   raw,
	}), nil
}

func (b liveVoiceHubBroadcaster) BroadcastLiveVoice(ctx context.Context, liveSessionID uint64, payload mcpapp.LiveVoiceBroadcastPayload) (int, error) {
	if liveSessionID == 0 {
		return 0, nil
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	if b.eventPublisher != nil {
		if err := b.eventPublisher.PublishLiveSessionEvent(ctx, liveSessionID, wstransport.TypeLiveVoiceBroadcast, payload.RequestID, 0, raw, true); err == nil {
			return 0, nil
		} else if b.hub == nil {
			return 0, err
		}
	}
	if b.hub == nil {
		return 0, nil
	}
	return b.hub.BroadcastLiveSessionOnlineClients(liveSessionID, wstransport.Envelope{
		Type:      wstransport.TypeLiveVoiceBroadcast,
		RequestID: payload.RequestID,
		Payload:   raw,
	}), nil
}

func (n liveSessionLotHubNotifier) NotifyLotMounted(ctx context.Context, merchantID string, liveSessionID, auctionID uint64) int {
	return n.broadcast(ctx, merchantID, liveSessionID, auctionID, "mounted", wstransport.TypeLiveSessionLotMounted)
}

func (n liveSessionLotHubNotifier) NotifyLotUnmounted(ctx context.Context, merchantID string, liveSessionID, auctionID uint64) int {
	return n.broadcast(ctx, merchantID, liveSessionID, auctionID, "unmounted", wstransport.TypeLiveSessionLotUnmounted)
}

func (n liveSessionLotHubNotifier) NotifyLotChanged(ctx context.Context, merchantID string, liveSessionID, auctionID uint64, action string) int {
	if action == "" {
		action = "changed"
	}
	return n.broadcast(ctx, merchantID, liveSessionID, auctionID, action, wstransport.TypeLiveSessionLotChanged)
}

func (n liveSessionLotHubNotifier) broadcast(ctx context.Context, merchantID string, liveSessionID, auctionID uint64, action, eventType string) int {
	if liveSessionID == 0 || auctionID == 0 {
		return 0
	}
	raw, err := json.Marshal(map[string]interface{}{
		"liveSessionId": liveSessionID,
		"auctionId":     auctionID,
		"merchantId":    merchantID,
		"action":        action,
	})
	if err != nil {
		return 0
	}
	if n.eventPublisher != nil {
		if err := n.eventPublisher.PublishLiveSessionEvent(ctx, liveSessionID, eventType, "", 0, raw, false); err == nil {
			return 0
		} else if n.hub == nil {
			return 0
		}
	}
	if n.hub == nil {
		return 0
	}
	return n.hub.BroadcastLiveSession(liveSessionID, wstransport.Envelope{
		Type:    eventType,
		Payload: raw,
	})
}

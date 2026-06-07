package app

import (
	"context"
	"testing"
	"time"
)

func TestAIAssistantServiceNotifySwitchIncludesLiveRoomPlaybackMode(t *testing.T) {
	notifier := &recordingEventNotifier{}
	service := NewAIAssistantService(nil, notifier)

	service.NotifySwitch(context.Background(), 9001, "u_2001", true)

	if notifier.liveSessionID != 9001 {
		t.Fatalf("expected liveSessionID 9001, got %d", notifier.liveSessionID)
	}
	if notifier.event.Kind != "switch" || notifier.event.Status != "enabled" {
		t.Fatalf("unexpected switch event: %+v", notifier.event)
	}
	if notifier.event.Enabled == nil || !*notifier.event.Enabled {
		t.Fatalf("expected enabled switch event, got %+v", notifier.event.Enabled)
	}
	if notifier.event.VideoSource != "digitalHuman" {
		t.Fatalf("expected digitalHuman video source, got %q", notifier.event.VideoSource)
	}
	if notifier.event.LiveRoom["videoSource"] != "digitalHuman" || notifier.event.LiveRoom["aiAssistantEnabled"] != true {
		t.Fatalf("expected liveRoom playback mode in event, got %+v", notifier.event.LiveRoom)
	}
}

func TestAIAssistantServiceRequestApprovalDefaultsAllowAfterTimeout(t *testing.T) {
	notifier := &recordingEventNotifier{}
	service := NewAIAssistantService(nil, notifier)
	service.timeout = 10 * time.Millisecond

	decision, err := service.RequestApproval(context.Background(), ApprovalInput{
		MerchantID:    "u_2001",
		LiveSessionID: 9001,
		ToolName:      "operate_live_session_lot",
		RequestID:     "approval-default-allow-1",
		Message:       "AI 请求开始讲解测试拍品，是否允许执行？",
	})
	if err != nil {
		t.Fatalf("expected timeout to default allow without error, got %v", err)
	}
	if !decision.Approved || decision.RequestID != "approval-default-allow-1" {
		t.Fatalf("expected approved decision, got %+v", decision)
	}
	if service.ApprovalTimeout() != 10*time.Millisecond {
		t.Fatalf("expected custom approval timeout, got %s", service.ApprovalTimeout())
	}
	if len(notifier.events) < 2 {
		t.Fatalf("expected pending and approved events, got %+v", notifier.events)
	}
	pending := notifier.events[0]
	if pending.Kind != "permission" || pending.Status != "pending" || pending.ExpiresAt == nil {
		t.Fatalf("expected pending permission event with expiry, got %+v", pending)
	}
	approved := notifier.events[len(notifier.events)-1]
	if approved.Kind != "permission" || approved.Status != "approved" || approved.Message != "30 秒内未操作，已默认允许执行" {
		t.Fatalf("expected timeout approved event, got %+v", approved)
	}
}

func TestAIAssistantServiceDefaultApprovalTimeoutIsThirtySeconds(t *testing.T) {
	service := NewAIAssistantService(nil, nil)

	if service.ApprovalTimeout() != 30*time.Second {
		t.Fatalf("expected default approval timeout 30s, got %s", service.ApprovalTimeout())
	}
}

type recordingEventNotifier struct {
	liveSessionID uint64
	event         Event
	events        []Event
}

func (r *recordingEventNotifier) NotifyAIAssistantEvent(ctx context.Context, liveSessionID uint64, event Event) (int, error) {
	_ = ctx
	r.liveSessionID = liveSessionID
	r.event = event
	r.events = append(r.events, event)
	return 1, nil
}

package runtime

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"aieas_backend/internal/domain"
	auctionapp "aieas_backend/internal/modules/auction/app"
	livesessionapp "aieas_backend/internal/modules/live_session/app"
	"aieas_backend/internal/tests/repository"
)

func TestLiveAgentHookServiceEmitsHighestBidWithNickname(t *testing.T) {
	invoker := newRecordingLiveAgentHookInvoker()
	hook := NewLiveAgentHookService(repository.NewMemoryConfigRepository(), repository.NewSeedUserRepository(), invoker)
	hook.SetHighestBidQuietPeriod(20 * time.Millisecond)
	ctx := context.Background()

	if _, err := hook.SetConfig(ctx, "u_2001", "u_2001", true); err != nil {
		t.Fatalf("enable hook: %v", err)
	}
	hook.EmitHighestBid(ctx, "u_2001", 90001, "u_1001", 12345)
	hook.EmitHighestBid(ctx, "u_2001", 90001, "u_1001", 13000)

	got := invoker.wait(t)
	if got.sessionID != "90001" || !strings.Contains(got.question, "直播场次90001") || !strings.Contains(got.question, "竞拍用户001") || !strings.Contains(got.question, "13000分") {
		t.Fatalf("unexpected highest bid hook: %+v", got)
	}
	invoker.assertNoCall(t, 30*time.Millisecond)
}

func TestLiveAgentHookServiceEmitsAuctionClosedEvents(t *testing.T) {
	invoker := newRecordingLiveAgentHookInvoker()
	hook := NewLiveAgentHookService(repository.NewMemoryConfigRepository(), repository.NewSeedUserRepository(), invoker)
	ctx := context.Background()

	if _, err := hook.SetConfig(ctx, "u_2001", "u_2001", true); err != nil {
		t.Fatalf("enable hook: %v", err)
	}
	hook.EmitAuctionClosed(ctx, "u_2001", 90001, 91001, domain.AuctionStatusClosedWon, 12345, false, "")
	if got := invoker.wait(t); got.sessionID != "90001" || !strings.Contains(got.question, "拍品91001落锤成交") || !strings.Contains(got.question, "12345分") {
		t.Fatalf("unexpected hammer won hook: %+v", got)
	}
	hook.EmitAuctionClosed(ctx, "u_2001", 90001, 91001, domain.AuctionStatusClosedWon, 13000, true, "")
	if got := invoker.wait(t); got.sessionID != "90001" || !strings.Contains(got.question, "自动落锤成交") || !strings.Contains(got.question, "13000分") {
		t.Fatalf("unexpected auto hammer won hook: %+v", got)
	}
	hook.EmitAuctionClosed(ctx, "u_2001", 90001, 91001, domain.AuctionStatusClosedFailed, 0, true, "")
	if got := invoker.wait(t); got.sessionID != "90001" || !strings.Contains(got.question, "自动落锤流拍") {
		t.Fatalf("unexpected auto hammer failed hook: %+v", got)
	}
	hook.EmitAuctionCancelled(ctx, "u_2001", 90001, 91001)
	if got := invoker.wait(t); got.sessionID != "90001" || !strings.Contains(got.question, "拍品91001已取消") {
		t.Fatalf("unexpected cancel hook: %+v", got)
	}
}

func TestLiveAgentHookServiceUsesConfiguredInvokeTimeout(t *testing.T) {
	invoker := newDeadlineRecordingLiveAgentHookInvoker()
	hook := NewLiveAgentHookService(repository.NewMemoryConfigRepository(), repository.NewSeedUserRepository(), invoker)
	hook.SetInvokeTimeout(2 * time.Second)
	ctx := context.Background()

	if _, err := hook.SetConfig(ctx, "u_2001", "u_2001", true); err != nil {
		t.Fatalf("enable hook: %v", err)
	}
	hook.EmitConfigChanged(ctx, "u_2001", 90001, true)

	deadline := invoker.waitDeadline(t)
	if deadline < time.Second || deadline > 2500*time.Millisecond {
		t.Fatalf("expected configured hook timeout near 2s, got %s", deadline)
	}
}

func TestLiveSessionEmitsLiveAgentHooks(t *testing.T) {
	ctx := context.Background()
	invoker := newRecordingLiveAgentHookInvoker()
	hook := NewLiveAgentHookService(repository.NewMemoryConfigRepository(), repository.NewSeedUserRepository(), invoker)

	auctionRepo := repository.NewMemoryAuctionRepository()
	realtime := repository.NewMemoryRealtimeStore()
	aiSwitch := newRecordingAIAssistantSwitchNotifier()
	auctionSvc := auctionapp.NewAuctionServiceWithDeps(auctionapp.AuctionServiceDeps{Auctions: auctionRepo, Tx: repository.NoopTxManager{}, Realtime: realtime, LiveAgentHook: hook})
	sessionSvc := livesessionapp.NewLiveSessionServiceWithDeps(livesessionapp.LiveSessionServiceDeps{Sessions: repository.NewMemoryLiveSessionRepository(), Auctions: auctionRepo, Tx: repository.NoopTxManager{}, Lock: repository.NewMemoryLiveSessionLock(), Auction: auctionSvc, LiveAgentHook: hook, AISwitch: aiSwitch})

	session, err := sessionSvc.Create(ctx, livesessionapp.CreateLiveSessionInput{
		ActorID:   "u_2001",
		ActorRole: domain.RoleMerchant,
		Title:     "直播场次",
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := sessionSvc.UpdateAgentHookConfig(ctx, session.ID, "u_2001", domain.RoleMerchant, true); err != nil {
		t.Fatalf("enable hook: %v", err)
	}
	if got := aiSwitch.wait(t); got.sessionID != session.ID || got.merchantID != "u_2001" || !got.enabled {
		t.Fatalf("unexpected ai switch enabled event: %+v", got)
	}
	if got := invoker.wait(t); got.sessionID != strconv.FormatUint(session.ID, 10) || got.question != "直播场次"+strconv.FormatUint(session.ID, 10)+"AI直播助手已开启" {
		t.Fatalf("unexpected hook config changed: %+v", got)
	}

	started, err := sessionSvc.Start(ctx, session.ID, "u_2001", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("start session: %v", err)
	}
	if got := invoker.wait(t); got.sessionID != strconv.FormatUint(started.ID, 10) || !strings.Contains(got.question, "直播场次"+strconv.FormatUint(started.ID, 10)) || !strings.Contains(got.question, "开播了") {
		t.Fatalf("unexpected live started hook: %+v", got)
	}

	lot := domain.AuctionLot{
		AuctionID:      91001,
		SellerID:       "u_2001",
		Title:          "茶盏",
		Category:       "瓷器",
		ConditionGrade: domain.ConditionGood,
		Description:    "品相完整",
		AuctionType:    domain.AuctionTypeEnglish,
		StartPrice:     1000,
		ReservePrice:   1000,
		IncrementRule:  domain.DefaultIncrementRule(),
		Status:         domain.AuctionStatusReady,
		DurationSec:    600,
	}
	if err := auctionRepo.Create(ctx, &lot); err != nil {
		t.Fatalf("create lot: %v", err)
	}
	if _, err := sessionSvc.MountAuction(ctx, started.ID, lot.AuctionID, "u_2001", domain.RoleMerchant); err != nil {
		t.Fatalf("mount lot: %v", err)
	}
	if got := invoker.wait(t); got.sessionID != strconv.FormatUint(started.ID, 10) || !strings.Contains(got.question, "拍品91001已上架") {
		t.Fatalf("unexpected lot mounted hook: %+v", got)
	}
	if _, err := sessionSvc.ActivateAuctionWithOptions(ctx, livesessionapp.ActivateLiveSessionAuctionInput{SessionID: started.ID, AuctionID: lot.AuctionID, ActorID: "u_2001", ActorRole: domain.RoleMerchant, DurationSec: 600}); err != nil {
		t.Fatalf("activate lot: %v", err)
	}
	if got := invoker.wait(t); got.sessionID != strconv.FormatUint(started.ID, 10) || !strings.Contains(got.question, "拍品91001开始拍卖/讲解") || !strings.Contains(got.question, "600秒") {
		t.Fatalf("unexpected lot started hook: %+v", got)
	}
	if _, err := sessionSvc.DeactivateAuction(ctx, started.ID, "u_2001", domain.RoleMerchant); err != nil {
		t.Fatalf("deactivate lot: %v", err)
	}
	if got := invoker.wait(t); got.sessionID != strconv.FormatUint(started.ID, 10) || !strings.Contains(got.question, "拍品91001已取消拍卖/讲解") {
		t.Fatalf("unexpected lot cancelled hook: %+v", got)
	}
	if err := sessionSvc.UnmountAuction(ctx, started.ID, lot.AuctionID, "u_2001", domain.RoleMerchant); err != nil {
		t.Fatalf("unmount lot: %v", err)
	}
	if got := invoker.wait(t); got.sessionID != strconv.FormatUint(started.ID, 10) || !strings.Contains(got.question, "拍品91001已下架") {
		t.Fatalf("unexpected lot unmounted hook: %+v", got)
	}

	scheduledLot := domain.AuctionLot{
		AuctionID:      91002,
		SellerID:       "u_2001",
		Title:          "预约茶器",
		Category:       "瓷器",
		ConditionGrade: domain.ConditionGood,
		Description:    "预约开拍",
		AuctionType:    domain.AuctionTypeEnglish,
		StartPrice:     1000,
		ReservePrice:   1000,
		IncrementRule:  domain.DefaultIncrementRule(),
		Status:         domain.AuctionStatusReady,
		DurationSec:    300,
	}
	if err := auctionRepo.Create(ctx, &scheduledLot); err != nil {
		t.Fatalf("create scheduled lot: %v", err)
	}
	if _, err := sessionSvc.MountAuction(ctx, started.ID, scheduledLot.AuctionID, "u_2001", domain.RoleMerchant); err != nil {
		t.Fatalf("mount scheduled lot: %v", err)
	}
	if got := invoker.wait(t); got.sessionID != strconv.FormatUint(started.ID, 10) || !strings.Contains(got.question, "拍品91002已上架") {
		t.Fatalf("unexpected scheduled lot mounted hook: %+v", got)
	}
	scheduledStart := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	if _, err := sessionSvc.ActivateAuctionWithOptions(ctx, livesessionapp.ActivateLiveSessionAuctionInput{SessionID: started.ID, AuctionID: scheduledLot.AuctionID, ActorID: "u_2001", ActorRole: domain.RoleMerchant, DurationSec: 300, StartTime: &scheduledStart}); err != nil {
		t.Fatalf("schedule lot: %v", err)
	}
	if got := invoker.wait(t); got.sessionID != strconv.FormatUint(started.ID, 10) ||
		!strings.Contains(got.question, "拍品91002已预约开拍") ||
		!strings.Contains(got.question, scheduledStart.Format(time.RFC3339)) ||
		!strings.Contains(got.question, "300秒") {
		t.Fatalf("unexpected scheduled lot hook: %+v", got)
	}

	if _, err := sessionSvc.MountAuction(ctx, started.ID, lot.AuctionID, "u_2001", domain.RoleMerchant); err != nil {
		t.Fatalf("mount lot before cancel: %v", err)
	}
	_ = invoker.wait(t)
	if _, err := auctionSvc.Cancel(ctx, lot.AuctionID, "u_2001", domain.RoleMerchant); err != nil {
		t.Fatalf("cancel lot: %v", err)
	}
	if got := invoker.wait(t); got.sessionID != strconv.FormatUint(started.ID, 10) || !strings.Contains(got.question, "拍品91001已取消") {
		t.Fatalf("unexpected auction cancelled hook: %+v", got)
	}

	if _, err := sessionSvc.UpdateAgentHookConfig(ctx, session.ID, "u_2001", domain.RoleMerchant, false); err != nil {
		t.Fatalf("disable hook: %v", err)
	}
	if got := aiSwitch.wait(t); got.sessionID != session.ID || got.merchantID != "u_2001" || got.enabled {
		t.Fatalf("unexpected ai switch disabled event: %+v", got)
	}
	select {
	case got := <-invoker.ch:
		t.Fatalf("disable hook should not emit message, got %+v", got)
	case <-time.After(20 * time.Millisecond):
	}
}

type recordingAIAssistantSwitchNotifier struct {
	ch chan recordingAIAssistantSwitchCall
}

type recordingAIAssistantSwitchCall struct {
	sessionID  uint64
	merchantID string
	enabled    bool
}

func newRecordingAIAssistantSwitchNotifier() *recordingAIAssistantSwitchNotifier {
	return &recordingAIAssistantSwitchNotifier{ch: make(chan recordingAIAssistantSwitchCall, 8)}
}

func (r *recordingAIAssistantSwitchNotifier) NotifySwitch(ctx context.Context, liveSessionID uint64, merchantID string, enabled bool) {
	_ = ctx
	r.ch <- recordingAIAssistantSwitchCall{sessionID: liveSessionID, merchantID: merchantID, enabled: enabled}
}

func (r *recordingAIAssistantSwitchNotifier) wait(t *testing.T) recordingAIAssistantSwitchCall {
	t.Helper()
	select {
	case got := <-r.ch:
		return got
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ai assistant switch event")
	}
	return recordingAIAssistantSwitchCall{}
}

type recordingLiveAgentHookInvoker struct {
	ch chan recordingLiveAgentHookCall
}

type recordingLiveAgentHookCall struct {
	sessionID string
	question  string
}

func newRecordingLiveAgentHookInvoker() *recordingLiveAgentHookInvoker {
	return &recordingLiveAgentHookInvoker{ch: make(chan recordingLiveAgentHookCall, 8)}
}

func (r *recordingLiveAgentHookInvoker) InvokeLiveAgentHook(ctx context.Context, sessionID, question string) error {
	_ = ctx
	r.ch <- recordingLiveAgentHookCall{sessionID: sessionID, question: question}
	return nil
}

func (r *recordingLiveAgentHookInvoker) wait(t *testing.T) recordingLiveAgentHookCall {
	t.Helper()
	select {
	case got := <-r.ch:
		return got
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for live agent hook message")
	}
	return recordingLiveAgentHookCall{}
}

func (r *recordingLiveAgentHookInvoker) assertNoCall(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case got := <-r.ch:
		t.Fatalf("unexpected live agent hook message: %+v", got)
	case <-time.After(timeout):
	}
}

type deadlineRecordingLiveAgentHookInvoker struct {
	ch chan time.Duration
}

func newDeadlineRecordingLiveAgentHookInvoker() *deadlineRecordingLiveAgentHookInvoker {
	return &deadlineRecordingLiveAgentHookInvoker{ch: make(chan time.Duration, 1)}
}

func (r *deadlineRecordingLiveAgentHookInvoker) InvokeLiveAgentHook(ctx context.Context, sessionID, question string) error {
	_ = sessionID
	_ = question
	deadline, ok := ctx.Deadline()
	if !ok {
		r.ch <- 0
		return nil
	}
	r.ch <- time.Until(deadline)
	return nil
}

func (r *deadlineRecordingLiveAgentHookInvoker) waitDeadline(t *testing.T) time.Duration {
	t.Helper()
	select {
	case got := <-r.ch:
		return got
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for live agent hook deadline")
	}
	return 0
}

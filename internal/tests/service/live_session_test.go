package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/tests/repository"
)

type failingSetActiveAuctionStore struct {
	*repository.MemoryLiveSessionRealtimeStore
	err error
}

func (s *failingSetActiveAuctionStore) SetActiveAuction(ctx context.Context, sessionID uint64, auctionID uint64) error {
	if s.err != nil {
		return s.err
	}
	return s.MemoryLiveSessionRealtimeStore.SetActiveAuction(ctx, sessionID, auctionID)
}

type fakeLiveSessionOnlineCounter struct {
	auctionOnline map[uint64]int
	sessionOnline map[uint64]int
}

func (c fakeLiveSessionOnlineCounter) OnlineCount(auctionID uint64) int {
	return c.auctionOnline[auctionID]
}

func (c fakeLiveSessionOnlineCounter) LiveSessionOnlineCount(liveSessionID uint64) int {
	return c.sessionOnline[liveSessionID]
}

type recordingLiveSessionLotEvents struct {
	changedAction  string
	changedID      uint64
	changedSession uint64
}

func (r *recordingLiveSessionLotEvents) NotifyLotMounted(ctx context.Context, merchantID string, sessionID, auctionID uint64) int {
	_ = ctx
	_ = merchantID
	_ = sessionID
	_ = auctionID
	return 1
}

func (r *recordingLiveSessionLotEvents) NotifyLotUnmounted(ctx context.Context, merchantID string, sessionID, auctionID uint64) int {
	_ = ctx
	_ = merchantID
	_ = sessionID
	_ = auctionID
	return 1
}

func (r *recordingLiveSessionLotEvents) NotifyLotChanged(ctx context.Context, merchantID string, sessionID, auctionID uint64, action string) int {
	_ = ctx
	_ = merchantID
	r.changedSession = sessionID
	r.changedID = auctionID
	r.changedAction = action
	return 1
}

func newLiveSessionFixture(t *testing.T) (*LiveSessionService, *repository.MemoryLiveSessionRepository, repository.AuctionRepository) {
	t.Helper()
	sessionRepo := repository.NewMemoryLiveSessionRepository()
	auctionRepo := repository.NewMemoryAuctionRepository()
	svc := NewLiveSessionService(sessionRepo, auctionRepo)
	return svc, sessionRepo, auctionRepo
}

func createStartedLiveSession(t *testing.T, svc *LiveSessionService, merchantID, title string) domain.LiveSession {
	t.Helper()
	ctx := context.Background()
	created, err := svc.Create(ctx, CreateLiveSessionInput{ActorID: merchantID, ActorRole: domain.RoleMerchant, Title: title})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	started, err := svc.Start(ctx, created.ID, merchantID, domain.RoleMerchant)
	if err != nil {
		t.Fatalf("start session: %v", err)
	}
	return started
}

func TestLiveSessionServiceCloseSessionTransitionAndIdempotent(t *testing.T) {
	svc, _, _ := newLiveSessionFixture(t)
	ctx := context.Background()

	opened := createStartedLiveSession(t, svc, "m_2", "Live")
	closed, err := svc.CloseSession(ctx, opened.ID)
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	if closed.Status != domain.LiveSessionStatusEnded {
		t.Fatalf("expected ENDED, got %s", closed.Status)
	}
	if closed.ClosedAt == nil {
		t.Fatalf("closedAt should be set")
	}

	// idempotent close
	again, err := svc.CloseSession(ctx, opened.ID)
	if err != nil {
		t.Fatalf("close again: %v", err)
	}
	if again.Status != domain.LiveSessionStatusEnded {
		t.Fatalf("expected ENDED on second close, got %s", again.Status)
	}
}

func TestLiveSessionServiceIncrCounters(t *testing.T) {
	svc, _, _ := newLiveSessionFixture(t)
	ctx := context.Background()

	opened := createStartedLiveSession(t, svc, "m_3", "Live")

	if err := svc.IncrCounters(ctx, opened.ID, domain.LiveSessionCounters{LotsTotalDelta: 2, BidCountDelta: 5, GMVCentDelta: 1000, ViewerPeakAtMin: 3}); err != nil {
		t.Fatalf("incr: %v", err)
	}
	if err := svc.IncrCounters(ctx, opened.ID, domain.LiveSessionCounters{LotsSoldDelta: 1, GMVCentDelta: 500, ViewerPeakAtMin: 2}); err != nil {
		t.Fatalf("incr again: %v", err)
	}

	got, err := svc.Get(ctx, opened.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.LotsTotal != 2 {
		t.Fatalf("lotsTotal=%d, want 2", got.LotsTotal)
	}
	if got.LotsSold != 1 {
		t.Fatalf("lotsSold=%d, want 1", got.LotsSold)
	}
	if got.BidCount != 5 {
		t.Fatalf("bidCount=%d, want 5", got.BidCount)
	}
	if got.GMVCent != 1500 {
		t.Fatalf("gmv=%d, want 1500", got.GMVCent)
	}
	// ViewerPeak should be max(3, 2) = 3
	if got.ViewerPeak != 3 {
		t.Fatalf("viewerPeak=%d, want 3 (max-only update)", got.ViewerPeak)
	}
}

func TestLiveSessionServiceListByMerchantOverridesForMerchantRole(t *testing.T) {
	svc, _, _ := newLiveSessionFixture(t)
	ctx := context.Background()
	createStartedLiveSession(t, svc, "m_self", "live")
	// 商家传 merchantId=other 也会被强制改写为自身。
	sessions, err := svc.ListByMerchant(ctx, "m_other", "", "m_self", domain.RoleMerchant, 20, 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(sessions) != 1 || sessions[0].MerchantID != "m_self" {
		t.Fatalf("expected self merchant only, got %#v", sessions)
	}
}

func TestLiveSessionServiceListByMerchantAllowsBuyerLiveOnly(t *testing.T) {
	svc, _, _ := newLiveSessionFixture(t)
	ctx := context.Background()
	live := createStartedLiveSession(t, svc, "m_public", "珠宝直播")
	if _, err := svc.Create(ctx, CreateLiveSessionInput{ActorID: "m_public", ActorRole: domain.RoleMerchant, Title: "珠宝预告"}); err != nil {
		t.Fatalf("create draft session: %v", err)
	}

	sessions, err := svc.ListByMerchantFiltered(ctx, domain.LiveSessionFilter{MerchantID: "m_public", Keyword: "珠宝", Limit: 20}, "u_buyer", domain.RoleBuyer)
	if err != nil {
		t.Fatalf("buyer list merchant sessions: %v", err)
	}
	if len(sessions) != 1 || sessions[0].ID != live.ID || sessions[0].Status != domain.LiveSessionStatusLive {
		t.Fatalf("buyer should see only merchant live sessions, got %#v", sessions)
	}

	sessions, err = svc.ListByMerchantFiltered(ctx, domain.LiveSessionFilter{MerchantID: "m_public", Status: domain.LiveSessionStatusDraft, Limit: 20}, "u_buyer", domain.RoleBuyer)
	if err != nil {
		t.Fatalf("buyer list merchant draft sessions: %v", err)
	}
	if len(sessions) != 0 {
		t.Fatalf("buyer should not see draft merchant sessions, got %#v", sessions)
	}
}

func TestLiveSessionServiceListVisibleAllowsBuyerLiveSessionsOnly(t *testing.T) {
	svc, _, _ := newLiveSessionFixture(t)
	ctx := context.Background()
	live := createStartedLiveSession(t, svc, "m_public", "live")
	if _, err := svc.Create(ctx, CreateLiveSessionInput{ActorID: "m_public", ActorRole: domain.RoleMerchant, Title: "draft"}); err != nil {
		t.Fatalf("create draft session: %v", err)
	}

	sessions, err := svc.ListVisible(ctx, "", "", "u_buyer", domain.RoleBuyer, 20, 0)
	if err != nil {
		t.Fatalf("list visible as buyer: %v", err)
	}
	if len(sessions) != 1 || sessions[0].ID != live.ID {
		t.Fatalf("buyer should see only LIVE sessions, got %#v", sessions)
	}

	sessions, err = svc.ListVisible(ctx, "", domain.LiveSessionStatusDraft, "u_buyer", domain.RoleBuyer, 20, 0)
	if err != nil {
		t.Fatalf("list draft as buyer: %v", err)
	}
	if len(sessions) != 0 {
		t.Fatalf("buyer should not see DRAFT sessions, got %#v", sessions)
	}
}

func TestLiveSessionServiceListVisibleSupportsKeywordAndSort(t *testing.T) {
	svc, _, _ := newLiveSessionFixture(t)
	ctx := context.Background()

	first := createStartedLiveSession(t, svc, "m_public_1", "珠宝专场")
	second := createStartedLiveSession(t, svc, "m_public_2", "数码专场")
	if _, err := svc.Create(ctx, CreateLiveSessionInput{ActorID: "m_public_3", ActorRole: domain.RoleMerchant, Title: "文玩专场"}); err != nil {
		t.Fatalf("create draft session: %v", err)
	}

	sessions, err := svc.ListVisibleFiltered(ctx, domain.LiveSessionFilter{Keyword: "专场", Sort: "oldest", Limit: 20}, "u_buyer", domain.RoleBuyer)
	if err != nil {
		t.Fatalf("list visible with keyword: %v", err)
	}
	if len(sessions) != 2 || sessions[0].ID != first.ID || sessions[1].ID != second.ID {
		t.Fatalf("expected buyer live sessions sorted oldest first, got %#v", sessions)
	}

	sessions, err = svc.ListVisibleFiltered(ctx, domain.LiveSessionFilter{Keyword: "珠宝", Limit: 20}, "u_buyer", domain.RoleBuyer)
	if err != nil {
		t.Fatalf("list visible with exact keyword: %v", err)
	}
	if len(sessions) != 1 || sessions[0].ID != first.ID {
		t.Fatalf("expected keyword to match title, got %#v", sessions)
	}

	sessions, err = svc.ListVisibleFiltered(ctx, domain.LiveSessionFilter{Keyword: "文玩", Limit: 20}, "u_buyer", domain.RoleBuyer)
	if err != nil {
		t.Fatalf("list visible with draft keyword: %v", err)
	}
	if len(sessions) != 0 {
		t.Fatalf("buyer should not see matching draft sessions, got %#v", sessions)
	}
}

func TestLiveSessionServiceListLotsFilterBySession(t *testing.T) {
	svc, _, auctionRepo := newLiveSessionFixture(t)
	ctx := context.Background()
	opened := createStartedLiveSession(t, svc, "m_x", "live")
	now := time.Now().UTC()
	other := uint64(99999)
	in := domain.AuctionLot{AuctionID: 7001, SellerID: "m_x", AuctionType: domain.AuctionTypeEnglish, AntiSnipingSec: 15, AntiExtendSec: 30, AntiExtendMode: domain.AuctionExtendModeAdd, Status: domain.AuctionStatusReady, LiveSessionID: &opened.ID, StartTime: now, EndTime: now.Add(time.Hour)}
	out := domain.AuctionLot{AuctionID: 7002, SellerID: "m_x", AuctionType: domain.AuctionTypeEnglish, AntiSnipingSec: 15, AntiExtendSec: 30, AntiExtendMode: domain.AuctionExtendModeAdd, Status: domain.AuctionStatusReady, LiveSessionID: &other, StartTime: now, EndTime: now.Add(time.Hour)}
	none := domain.AuctionLot{AuctionID: 7003, SellerID: "m_x", AuctionType: domain.AuctionTypeEnglish, AntiSnipingSec: 15, AntiExtendSec: 30, AntiExtendMode: domain.AuctionExtendModeAdd, Status: domain.AuctionStatusReady, StartTime: now, EndTime: now.Add(time.Hour)}
	for _, lot := range []domain.AuctionLot{in, out, none} {
		l := lot
		if err := auctionRepo.Create(ctx, &l); err != nil {
			t.Fatalf("create lot: %v", err)
		}
	}
	lots, err := svc.ListLots(ctx, opened.ID, "m_x", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("list lots: %v", err)
	}
	if len(lots) != 1 || lots[0].AuctionID != 7001 {
		t.Fatalf("expected only AuctionID 7001, got %#v", lots)
	}
}

func TestLiveSessionServiceListLotsAllowsBuyerForLiveSessionWithIncrementRule(t *testing.T) {
	svc, _, auctionRepo := newLiveSessionFixture(t)
	ctx := context.Background()
	live := createStartedLiveSession(t, svc, "m_public_lots", "live")
	now := time.Now().UTC()
	rule := json.RawMessage(`{"type":"ladder","maxBidSteps":3,"steps":[{"min":0,"max":10000,"amount":500},{"min":10000,"amount":1000}]}`)
	lot := domain.AuctionLot{
		AuctionID:      7101,
		SellerID:       "m_public_lots",
		LiveSessionID:  &live.ID,
		Title:          "公开直播拍品",
		Category:       "collectible",
		ConditionGrade: domain.ConditionGood,
		AuctionType:    domain.AuctionTypeEnglish,
		StartPrice:     1000,
		ReservePrice:   1000,
		CapPrice:       20000,
		IncrementRule:  rule,
		AntiSnipingSec: 15,
		AntiExtendSec:  30,
		AntiExtendMode: domain.AuctionExtendModeAdd,
		Status:         domain.AuctionStatusReady,
		StartTime:      now,
		EndTime:        now.Add(time.Hour),
	}
	if err := auctionRepo.Create(ctx, &lot); err != nil {
		t.Fatalf("create lot: %v", err)
	}

	lots, err := svc.ListLots(ctx, live.ID, "u_buyer", domain.RoleBuyer)
	if err != nil {
		t.Fatalf("list lots as buyer: %v", err)
	}
	if len(lots) != 1 || lots[0].AuctionID != lot.AuctionID {
		t.Fatalf("expected buyer to see live lot, got %#v", lots)
	}
	if string(lots[0].IncrementRule) != string(rule) {
		t.Fatalf("incrementRule missing from buyer lot response: %s", string(lots[0].IncrementRule))
	}

	draft, err := svc.Create(ctx, CreateLiveSessionInput{ActorID: "m_public_lots", ActorRole: domain.RoleMerchant, Title: "draft"})
	if err != nil {
		t.Fatalf("create draft session: %v", err)
	}
	if _, err := svc.ListLots(ctx, draft.ID, "u_buyer", domain.RoleBuyer); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("buyer should not list draft session lots, got %v", err)
	}
}

func TestLiveSessionServiceStatsAllowsBuyerForLiveSession(t *testing.T) {
	svc, _, _ := newLiveSessionFixture(t)
	ctx := context.Background()
	live := createStartedLiveSession(t, svc, "m_stats_public", "live")

	stats, err := svc.Stats(ctx, live.ID, "u_buyer", domain.RoleBuyer)
	if err != nil {
		t.Fatalf("stats as buyer for LIVE session: %v", err)
	}
	if stats.LiveSessionID != live.ID {
		t.Fatalf("stats liveSessionId=%d, want %d", stats.LiveSessionID, live.ID)
	}

	draft, err := svc.Create(ctx, CreateLiveSessionInput{ActorID: "m_stats_public", ActorRole: domain.RoleMerchant, Title: "draft"})
	if err != nil {
		t.Fatalf("create draft session: %v", err)
	}
	if _, err := svc.Stats(ctx, draft.ID, "u_buyer", domain.RoleBuyer); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("buyer stats for DRAFT should be forbidden, got %v", err)
	}
}

func TestLiveSessionServiceCanTransitionRule(t *testing.T) {
	if !domain.CanTransitionLiveSession(domain.LiveSessionStatusLive, domain.LiveSessionStatusEnded) {
		t.Fatalf("LIVE -> ENDED should be allowed")
	}
	if domain.CanTransitionLiveSession(domain.LiveSessionStatusEnded, domain.LiveSessionStatusLive) {
		t.Fatalf("ENDED -> LIVE should be forbidden")
	}
}

// TestLiveSessionServiceIncrCountersGoesThroughRealtimeStore 验证：
// 注入 RealtimeStore 后，IncrCounters 不再 RMW MySQL，而是走 store；MySQL 行只在 Flush/Close 时才更新。
func TestLiveSessionServiceIncrCountersGoesThroughRealtimeStore(t *testing.T) {
	svc, _, _ := newLiveSessionFixture(t)
	store := repository.NewMemoryLiveSessionRealtimeStore()
	svc.SetRealtimeStore(store)
	ctx := context.Background()

	opened := createStartedLiveSession(t, svc, "m_rt", "live")
	if err := svc.IncrCounters(ctx, opened.ID, domain.LiveSessionCounters{LotsTotalDelta: 1, BidCountDelta: 4, GMVCentDelta: 100, ViewerPeakAtMin: 5}); err != nil {
		t.Fatalf("incr: %v", err)
	}
	// MySQL 行此时还未被 IncrCounters 触碰
	got, err := svc.Get(ctx, opened.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.LotsTotal != 0 || got.BidCount != 0 || got.GMVCent != 0 || got.ViewerPeak != 0 {
		t.Fatalf("expected MySQL row untouched until flush, got %+v", got)
	}
	stats, err := svc.Stats(ctx, opened.ID, "m_rt", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.LotsTotal != 1 || stats.BidCount != 4 || stats.GMVCent != 100 || stats.ViewerPeak != 5 {
		t.Fatalf("stats should include realtime counters, got %+v", stats)
	}
	// store 内累积可见
	counters, peak, err := store.LoadCounters(ctx, opened.ID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if counters.LotsTotalDelta != 1 || counters.BidCountDelta != 4 || counters.GMVCentDelta != 100 {
		t.Fatalf("store deltas wrong: %+v", counters)
	}
	if peak != 5 {
		t.Fatalf("peak=%d want 5", peak)
	}
}

// TestLiveSessionServiceFlushCountersToDB 验证 FlushCountersToDB 把 store 中的累积计数一次性写进 MySQL，
// 并清零 store。
func TestLiveSessionServiceFlushCountersToDB(t *testing.T) {
	svc, _, _ := newLiveSessionFixture(t)
	store := repository.NewMemoryLiveSessionRealtimeStore()
	svc.SetRealtimeStore(store)
	ctx := context.Background()

	opened := createStartedLiveSession(t, svc, "m_flush", "live")
	if err := svc.IncrCounters(ctx, opened.ID, domain.LiveSessionCounters{LotsTotalDelta: 2, LotsSoldDelta: 1, ViewerPeakAtMin: 7}); err != nil {
		t.Fatalf("incr1: %v", err)
	}
	if err := svc.IncrCounters(ctx, opened.ID, domain.LiveSessionCounters{LotsTotalDelta: 1, ViewerPeakAtMin: 3}); err != nil {
		t.Fatalf("incr2: %v", err)
	}
	if err := svc.FlushCountersToDB(ctx, opened.ID); err != nil {
		t.Fatalf("flush: %v", err)
	}
	got, err := svc.Get(ctx, opened.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.LotsTotal != 3 || got.LotsSold != 1 || got.ViewerPeak != 7 {
		t.Fatalf("after flush wrong: %+v", got)
	}
	// 二次 flush 是 no-op（store 已 Reset 由 Close 路径负责；FlushCountersToDB 单独不会 reset，
	// 但累积值已经被写入 MySQL，再次 flush 会重复加，只在 Close 时才 reset。
	// 这里我们手动 Reset 模拟 Close 后的 store 状态。）
	if err := store.Reset(ctx, opened.ID); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if err := svc.FlushCountersToDB(ctx, opened.ID); err != nil {
		t.Fatalf("re-flush: %v", err)
	}
	again, _ := svc.Get(ctx, opened.ID)
	if again.LotsTotal != 3 || again.ViewerPeak != 7 {
		t.Fatalf("re-flush should be noop after reset, got %+v", again)
	}
}

// TestLiveSessionServiceCloseFlushesAndResetsStore 验证 CloseSession 会先 flush，
// 再切 ENDED，再清空 realtime store。
func TestLiveSessionServiceCloseFlushesAndResetsStore(t *testing.T) {
	svc, _, _ := newLiveSessionFixture(t)
	store := repository.NewMemoryLiveSessionRealtimeStore()
	svc.SetRealtimeStore(store)
	ctx := context.Background()

	opened := createStartedLiveSession(t, svc, "m_close", "live")
	if err := svc.IncrCounters(ctx, opened.ID, domain.LiveSessionCounters{LotsTotalDelta: 5, GMVCentDelta: 9999, ViewerPeakAtMin: 11}); err != nil {
		t.Fatalf("incr: %v", err)
	}
	closed, err := svc.CloseSession(ctx, opened.ID)
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	if closed.Status != domain.LiveSessionStatusEnded {
		t.Fatalf("expected ENDED, got %s", closed.Status)
	}
	if closed.LotsTotal != 5 || closed.GMVCent != 9999 || closed.ViewerPeak != 11 {
		t.Fatalf("close should have flushed deltas, got %+v", closed)
	}
	counters, peak, _ := store.LoadCounters(ctx, opened.ID)
	if counters.LotsTotalDelta != 0 || counters.GMVCentDelta != 0 || peak != 0 {
		t.Fatalf("expected store reset after close, got counters=%+v peak=%d", counters, peak)
	}
}

// TestLiveSessionServiceOnEndedHookFiresOnceAfterTransition 验证：
//   - LIVE -> ENDED 真正切状态时回调被触发；
//   - 已 ENDED 的幂等 close 不会再次触发；
//   - 回调收到的 LiveSession 是 ENDED 后的最终态。
func TestLiveSessionServiceOnEndedHookFiresOnceAfterTransition(t *testing.T) {
	svc, _, _ := newLiveSessionFixture(t)
	ctx := context.Background()

	calls := make(chan domain.LiveSession, 4)
	svc.SetOnEnded(func(_ context.Context, session domain.LiveSession) {
		calls <- session
	})

	opened := createStartedLiveSession(t, svc, "m_hook", "live")
	if _, err := svc.CloseSession(ctx, opened.ID); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case got := <-calls:
		if got.ID != opened.ID || got.Status != domain.LiveSessionStatusEnded {
			t.Fatalf("hook saw wrong session: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("onEnded hook did not fire within 1s")
	}

	// 幂等 close 不应再触发回调。
	if _, err := svc.CloseSession(ctx, opened.ID); err != nil {
		t.Fatalf("close again: %v", err)
	}
	select {
	case extra := <-calls:
		t.Fatalf("hook fired on idempotent close: %+v", extra)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestLiveSessionServiceMainlineLifecycleAndSingleLivePerMerchant(t *testing.T) {
	svc, _, _ := newLiveSessionFixture(t)
	ctx := context.Background()

	first, err := svc.Create(ctx, CreateLiveSessionInput{ActorID: "m_main", ActorRole: domain.RoleMerchant, Title: "早场"})
	if err != nil {
		t.Fatalf("create first: %v", err)
	}
	second, err := svc.Create(ctx, CreateLiveSessionInput{ActorID: "m_main", ActorRole: domain.RoleMerchant, Title: "晚场"})
	if err != nil {
		t.Fatalf("create second: %v", err)
	}
	started, err := svc.Start(ctx, first.ID, "m_main", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("start first: %v", err)
	}
	if started.Status != domain.LiveSessionStatusLive || started.OpenedAt == nil || started.OpenedAt.IsZero() || started.ActiveAuctionID != 0 {
		t.Fatalf("started session wrong: %+v", started)
	}
	if _, err := svc.Start(ctx, second.ID, "m_main", domain.RoleMerchant); !errors.Is(err, domain.ErrInvalidState) {
		t.Fatalf("same merchant second LIVE should fail, got %v", err)
	}
	ended, err := svc.End(ctx, first.ID, "m_main", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("end first: %v", err)
	}
	if ended.Status != domain.LiveSessionStatusEnded || ended.ClosedAt == nil || ended.ActiveAuctionID != 0 {
		t.Fatalf("ended session wrong: %+v", ended)
	}
}

func TestLiveSessionServiceMountActivateDeactivateAndUnmountRules(t *testing.T) {
	svc, _, auctionRepo := newLiveSessionFixture(t)
	svc.SetWriteDeps(repository.NoopTxManager{}, repository.NewMemoryLiveSessionLock(), nil)
	ctx := context.Background()
	session, err := svc.Create(ctx, CreateLiveSessionInput{ActorID: "m_lot", ActorRole: domain.RoleMerchant, Title: "拍品场"})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := svc.Start(ctx, session.ID, "m_lot", domain.RoleMerchant); err != nil {
		t.Fatalf("start session: %v", err)
	}
	now := time.Now().UTC()
	lot := domain.AuctionLot{AuctionID: 8101, SellerID: "m_lot", AuctionType: domain.AuctionTypeEnglish, AntiSnipingSec: 15, AntiExtendSec: 30, AntiExtendMode: domain.AuctionExtendModeAdd, Status: domain.AuctionStatusReady, StartPrice: 100, StartTime: now, EndTime: now.Add(time.Hour)}
	if err := auctionRepo.Create(ctx, &lot); err != nil {
		t.Fatalf("create lot: %v", err)
	}
	mounted, err := svc.MountAuction(ctx, session.ID, lot.AuctionID, "m_lot", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("mount by auctionId only: %v", err)
	}
	if mounted.LiveSessionID == nil || *mounted.LiveSessionID != session.ID {
		t.Fatalf("mounted session id wrong: %+v", mounted)
	}
	active, err := svc.ActivateAuctionWithOptions(ctx, ActivateLiveSessionAuctionInput{SessionID: session.ID, AuctionID: lot.AuctionID, ActorID: "m_lot", ActorRole: domain.RoleMerchant})
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	if active.Status != domain.AuctionStatusReady && active.Status != domain.AuctionStatusRunning {
		t.Fatalf("unexpected active lot: %+v", active)
	}
	if err := svc.UnmountAuction(ctx, session.ID, lot.AuctionID, "m_lot", domain.RoleMerchant); !errors.Is(err, domain.ErrInvalidState) {
		t.Fatalf("active lot should not unmount, got %v", err)
	}
	if _, err := svc.DeactivateAuction(ctx, session.ID, "m_lot", domain.RoleMerchant); err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if _, err := svc.ActivateAuctionWithOptions(ctx, ActivateLiveSessionAuctionInput{SessionID: session.ID, AuctionID: lot.AuctionID, ActorID: "m_lot", ActorRole: domain.RoleMerchant}); err != nil {
		t.Fatalf("reactivate after deactivate: %v", err)
	}
}

func TestLiveSessionServiceDeactivateAuctionBroadcastsLotChanged(t *testing.T) {
	sessionRepo := repository.NewMemoryLiveSessionRepository()
	auctionRepo := repository.NewMemoryAuctionRepository()
	lotEvents := &recordingLiveSessionLotEvents{}
	svc := NewLiveSessionServiceWithDeps(LiveSessionServiceDeps{
		Sessions:  sessionRepo,
		Auctions:  auctionRepo,
		LotEvents: lotEvents,
	})
	svc.SetWriteDeps(repository.NoopTxManager{}, repository.NewMemoryLiveSessionLock(), nil)
	ctx := context.Background()
	session := createStartedLiveSession(t, svc, "m_cancel_current", "取消当前拍品场")
	now := time.Now().UTC()
	lot := domain.AuctionLot{
		AuctionID:      8102,
		SellerID:       "m_cancel_current",
		AuctionType:    domain.AuctionTypeEnglish,
		Status:         domain.AuctionStatusReady,
		StartPrice:     100,
		AntiSnipingSec: 15,
		AntiExtendSec:  30,
		AntiExtendMode: domain.AuctionExtendModeAdd,
		StartTime:      now,
		EndTime:        now.Add(time.Hour),
		LiveSessionID:  &session.ID,
	}
	if err := auctionRepo.Create(ctx, &lot); err != nil {
		t.Fatalf("create lot: %v", err)
	}
	if _, err := svc.ActivateAuctionWithOptions(ctx, ActivateLiveSessionAuctionInput{SessionID: session.ID, AuctionID: lot.AuctionID, ActorID: "m_cancel_current", ActorRole: domain.RoleMerchant}); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if _, err := svc.DeactivateAuction(ctx, session.ID, "m_cancel_current", domain.RoleMerchant); err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if lotEvents.changedSession != session.ID || lotEvents.changedID != lot.AuctionID || lotEvents.changedAction != "cancelled" {
		t.Fatalf("expected cancelled lot changed event, got session=%d auction=%d action=%q", lotEvents.changedSession, lotEvents.changedID, lotEvents.changedAction)
	}
}

func TestLiveSessionServiceScheduleFutureAuctionDoesNotActivateSession(t *testing.T) {
	sessionRepo := repository.NewMemoryLiveSessionRepository()
	auctionRepo := repository.NewMemoryAuctionRepository()
	lotEvents := &recordingLiveSessionLotEvents{}
	svc := NewLiveSessionServiceWithDeps(LiveSessionServiceDeps{
		Sessions:  sessionRepo,
		Auctions:  auctionRepo,
		LotEvents: lotEvents,
	})
	svc.SetWriteDeps(repository.NoopTxManager{}, repository.NewMemoryLiveSessionLock(), nil)
	ctx := context.Background()
	session := createStartedLiveSession(t, svc, "m_schedule", "预约开拍场")
	now := time.Now().UTC()
	lot := domain.AuctionLot{
		AuctionID:      8110,
		SellerID:       "m_schedule",
		LiveSessionID:  &session.ID,
		AuctionType:    domain.AuctionTypeEnglish,
		Status:         domain.AuctionStatusReady,
		StartPrice:     100,
		AntiSnipingSec: 15,
		AntiExtendSec:  30,
		AntiExtendMode: domain.AuctionExtendModeAdd,
		StartTime:      now,
		EndTime:        now.Add(time.Hour),
	}
	if err := auctionRepo.Create(ctx, &lot); err != nil {
		t.Fatalf("create lot: %v", err)
	}
	startAt := now.Add(10 * time.Minute)
	scheduled, err := svc.ActivateAuctionWithOptions(ctx, ActivateLiveSessionAuctionInput{SessionID: session.ID, AuctionID: lot.AuctionID, ActorID: "m_schedule", ActorRole: domain.RoleMerchant, DurationSec: 600, StartTime: &startAt})
	if err != nil {
		t.Fatalf("schedule auction: %v", err)
	}
	if scheduled.Status != domain.AuctionStatusWarmingUp {
		t.Fatalf("expected WARMING_UP, got %s", scheduled.Status)
	}
	if !scheduled.StartTime.Equal(startAt) || !scheduled.EndTime.Equal(startAt.Add(10*time.Minute)) || scheduled.DurationSec != 600 {
		t.Fatalf("scheduled timing wrong: %+v", scheduled)
	}
	gotSession, err := sessionRepo.Get(ctx, session.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if gotSession.ActiveAuctionID != 0 {
		t.Fatalf("future scheduled auction should not activate session, activeAuctionID=%d", gotSession.ActiveAuctionID)
	}
	if lotEvents.changedSession != session.ID || lotEvents.changedID != lot.AuctionID || lotEvents.changedAction != "scheduled" {
		t.Fatalf("expected scheduled lot changed event, got session=%d auction=%d action=%q", lotEvents.changedSession, lotEvents.changedID, lotEvents.changedAction)
	}
	if err := svc.UnmountAuction(ctx, session.ID, lot.AuctionID, "m_schedule", domain.RoleMerchant); !errors.Is(err, domain.ErrInvalidState) {
		t.Fatalf("scheduled lot should require cancellation before unmount, got %v", err)
	}
	if _, err := svc.DeactivateAuction(ctx, session.ID, "m_schedule", domain.RoleMerchant); err != nil {
		t.Fatalf("cancel scheduled auction: %v", err)
	}
	cancelledLot, err := auctionRepo.FindByID(ctx, lot.AuctionID)
	if err != nil {
		t.Fatalf("find cancelled scheduled lot: %v", err)
	}
	if cancelledLot.Status != domain.AuctionStatusReady || !cancelledLot.StartTime.IsZero() || !cancelledLot.EndTime.IsZero() {
		t.Fatalf("expected scheduled lot reset to READY with timing cleared, got %+v", cancelledLot)
	}
	if err := svc.UnmountAuction(ctx, session.ID, lot.AuctionID, "m_schedule", domain.RoleMerchant); err != nil {
		t.Fatalf("unmount cancelled scheduled lot: %v", err)
	}
}

func TestLiveSessionServiceActivateDueScheduledAuctionStartsAuction(t *testing.T) {
	svc, sessionRepo, auctionRepo := newLiveSessionFixture(t)
	realtime := repository.NewMemoryRealtimeStore()
	auctionSvc := NewAuctionServiceWithDeps(AuctionServiceDeps{Auctions: auctionRepo, Tx: repository.NoopTxManager{}, Realtime: realtime})
	svc.SetWriteDeps(repository.NoopTxManager{}, repository.NewMemoryLiveSessionLock(), auctionSvc)
	svc.SetStatsDeps(nil, realtime, nil)
	ctx := context.Background()
	session := createStartedLiveSession(t, svc, "m_due_schedule", "到点开拍场")
	now := time.Now().UTC()
	startAt := now.Add(-time.Second)
	lot := domain.AuctionLot{
		AuctionID:      8111,
		SellerID:       "m_due_schedule",
		LiveSessionID:  &session.ID,
		Title:          "预约拍品",
		Category:       "collectible",
		ConditionGrade: domain.ConditionGood,
		AuctionType:    domain.AuctionTypeEnglish,
		Status:         domain.AuctionStatusWarmingUp,
		StartPrice:     100,
		IncrementRule:  domain.DefaultIncrementRule(),
		AntiSnipingSec: 15,
		AntiExtendSec:  30,
		AntiExtendMode: domain.AuctionExtendModeAdd,
		StartTime:      startAt,
		EndTime:        startAt.Add(10 * time.Minute),
		DurationSec:    600,
	}
	if err := auctionRepo.Create(ctx, &lot); err != nil {
		t.Fatalf("create lot: %v", err)
	}
	started, err := svc.ActivateDueScheduledAuction(ctx, lot, now)
	if err != nil {
		t.Fatalf("activate due scheduled auction: %v", err)
	}
	if started.Status != domain.AuctionStatusRunning {
		t.Fatalf("expected RUNNING, got %s", started.Status)
	}
	if !started.StartTime.After(startAt) || !started.EndTime.Equal(started.StartTime.Add(10*time.Minute)) {
		t.Fatalf("started timing should use actual activation time and configured duration, got %+v", started)
	}
	gotSession, err := sessionRepo.Get(ctx, session.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if gotSession.ActiveAuctionID != lot.AuctionID {
		t.Fatalf("active auction id=%d, want %d", gotSession.ActiveAuctionID, lot.AuctionID)
	}
	state, ok, err := realtime.GetAuctionState(ctx, lot.AuctionID)
	if err != nil {
		t.Fatalf("get realtime state: %v", err)
	}
	if !ok || state.Status != domain.AuctionStatusRunning {
		t.Fatalf("expected running realtime state, ok=%v state=%+v", ok, state)
	}
}

func TestLiveSessionServiceActivateAuctionReturnsErrorWhenRealtimeWriteFails(t *testing.T) {
	svc, sessionRepo, auctionRepo := newLiveSessionFixture(t)
	lock := repository.NewMemoryLiveSessionLock()
	svc.SetWriteDeps(repository.NoopTxManager{}, lock, nil)
	rt := &failingSetActiveAuctionStore{
		MemoryLiveSessionRealtimeStore: repository.NewMemoryLiveSessionRealtimeStore(),
		err:                            fmt.Errorf("redis rt unavailable"),
	}
	svc.SetRealtimeStore(rt)
	ctx := context.Background()
	session := createStartedLiveSession(t, svc, "m_rt_fail", "rt fail")
	now := time.Now().UTC()
	lot := domain.AuctionLot{AuctionID: 8103, SellerID: "m_rt_fail", LiveSessionID: &session.ID, AuctionType: domain.AuctionTypeEnglish, AntiSnipingSec: 15, AntiExtendSec: 30, AntiExtendMode: domain.AuctionExtendModeAdd, Status: domain.AuctionStatusReady, StartPrice: 100, StartTime: now, EndTime: now.Add(time.Hour)}
	if err := auctionRepo.Create(ctx, &lot); err != nil {
		t.Fatalf("create lot: %v", err)
	}

	_, err := svc.ActivateAuctionWithOptions(ctx, ActivateLiveSessionAuctionInput{SessionID: session.ID, AuctionID: lot.AuctionID, ActorID: "m_rt_fail", ActorRole: domain.RoleMerchant})
	if err == nil || !errors.Is(err, rt.err) {
		t.Fatalf("expected realtime write error, got %v", err)
	}
	gotSession, err := sessionRepo.Get(ctx, session.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if gotSession.ActiveAuctionID != 0 {
		t.Fatalf("ActiveAuctionID=%d, want 0 after realtime failure", gotSession.ActiveAuctionID)
	}
	activeID, ok, err := rt.MemoryLiveSessionRealtimeStore.ActiveAuction(ctx, session.ID)
	if err != nil {
		t.Fatalf("read realtime active: %v", err)
	}
	if ok || activeID != 0 {
		t.Fatalf("realtime active=%d ok=%v, want empty", activeID, ok)
	}
	holder, err := lock.Current(ctx, session.ID)
	if err != nil {
		t.Fatalf("lock current: %v", err)
	}
	if holder != 0 {
		t.Fatalf("lock holder=%d, want released", holder)
	}
}

func TestLiveSessionServiceRestartClosedFailedLot(t *testing.T) {
	svc, _, auctionRepo := newLiveSessionFixture(t)
	ctx := context.Background()
	session, err := svc.Create(ctx, CreateLiveSessionInput{ActorID: "m_restart", ActorRole: domain.RoleMerchant, Title: "重开场"})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := svc.Start(ctx, session.ID, "m_restart", domain.RoleMerchant); err != nil {
		t.Fatalf("start session: %v", err)
	}
	now := time.Now().UTC()
	closedAt := now.Add(-time.Minute)
	dealPrice := int64(100)
	lot := domain.AuctionLot{
		AuctionID:      8102,
		SellerID:       "m_restart",
		LiveSessionID:  &session.ID,
		AuctionType:    domain.AuctionTypeEnglish,
		Status:         domain.AuctionStatusClosedFailed,
		StartPrice:     100,
		AntiSnipingSec: 15,
		AntiExtendSec:  30,
		AntiExtendMode: domain.AuctionExtendModeAdd,
		DurationSec:    600,
		StartTime:      now.Add(-time.Minute),
		EndTime:        now,
		DealPrice:      &dealPrice,
		ClosedAt:       &closedAt,
		ClosedBy:       "merchant",
	}
	if err := auctionRepo.Create(ctx, &lot); err != nil {
		t.Fatalf("create lot: %v", err)
	}
	active, err := svc.ActivateAuctionWithOptions(ctx, ActivateLiveSessionAuctionInput{SessionID: session.ID, AuctionID: lot.AuctionID, ActorID: "m_restart", ActorRole: domain.RoleMerchant, DurationSec: 600})
	if err != nil {
		t.Fatalf("restart closed failed lot: %v", err)
	}
	if active.Status != domain.AuctionStatusReady {
		t.Fatalf("expected reset status READY, got %s", active.Status)
	}
	if active.DealPrice != nil || active.ClosedAt != nil || active.ClosedBy != "" {
		t.Fatalf("closed fields should be cleared: %+v", active)
	}
	gotSession, err := svc.Get(ctx, session.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if gotSession.ActiveAuctionID != lot.AuctionID {
		t.Fatalf("active auction id=%d, want %d", gotSession.ActiveAuctionID, lot.AuctionID)
	}
}

func TestLiveSessionServiceListAuctionBidsUsesCurrentRound(t *testing.T) {
	svc, sessionRepo, auctionRepo := newLiveSessionFixture(t)
	ctx := context.Background()
	bidRepo := repository.NewMemoryBidRepository()
	svc.SetStatsDeps(bidRepo, nil, nil)
	session := createStartedLiveSession(t, svc, "m_round", "重拍场")
	now := time.Now().UTC()
	lot := domain.AuctionLot{
		AuctionID:      8103,
		SellerID:       "m_round",
		LiveSessionID:  &session.ID,
		AuctionType:    domain.AuctionTypeEnglish,
		Status:         domain.AuctionStatusRunning,
		StartPrice:     100,
		AntiSnipingSec: 15,
		AntiExtendSec:  30,
		AntiExtendMode: domain.AuctionExtendModeAdd,
		StartTime:      now,
		EndTime:        now.Add(10 * time.Minute),
	}
	if err := auctionRepo.Create(ctx, &lot); err != nil {
		t.Fatalf("create lot: %v", err)
	}
	session.ActiveAuctionID = lot.AuctionID
	if err := sessionRepo.Update(ctx, &session); err != nil {
		t.Fatalf("mark active auction: %v", err)
	}
	oldBid := domain.BidRecord{RequestID: "old-round", AuctionID: lot.AuctionID, LiveSessionID: &session.ID, BidderID: "u_old", BidPrice: 500, BidTSMS: now.Add(-time.Minute).UnixMilli(), RiskResult: domain.BidRiskAllow}
	newBid := domain.BidRecord{RequestID: "new-round", AuctionID: lot.AuctionID, LiveSessionID: &session.ID, BidderID: "u_new", BidPrice: 200, BidTSMS: now.Add(time.Second).UnixMilli(), RiskResult: domain.BidRiskAllow}
	if err := bidRepo.Create(ctx, &oldBid); err != nil {
		t.Fatalf("create old bid: %v", err)
	}
	if err := bidRepo.Create(ctx, &newBid); err != nil {
		t.Fatalf("create new bid: %v", err)
	}
	records, err := svc.ListAuctionBids(ctx, session.ID, lot.AuctionID, 10, "m_round", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("list auction bids: %v", err)
	}
	if len(records) != 1 || records[0].RequestID != "new-round" {
		t.Fatalf("expected only current round bid, got %+v", records)
	}
	lots, err := svc.ListLots(ctx, session.ID, "m_round", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("list lots: %v", err)
	}
	if len(lots) != 1 || lots[0].BidCount != 1 || lots[0].CurrentPrice != newBid.BidPrice || lots[0].LeaderBidderID != newBid.BidderID {
		t.Fatalf("expected lot stats from current round only, got %+v", lots)
	}
	stats, err := svc.Stats(ctx, session.ID, "m_round", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.CurrentBidCount != 1 {
		t.Fatalf("expected current round bid count 1, got %+v", stats)
	}
}

func TestLiveSessionServiceStatsUsesLiveSessionOnlineWithoutActiveAuction(t *testing.T) {
	svc, _, _ := newLiveSessionFixture(t)
	ctx := context.Background()
	session := createStartedLiveSession(t, svc, "m_online", "在线统计场")
	svc.SetStatsDeps(nil, nil, fakeLiveSessionOnlineCounter{sessionOnline: map[uint64]int{session.ID: 3}})

	stats, err := svc.Stats(ctx, session.ID, "m_online", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.ActiveAuctionID != 0 {
		t.Fatalf("expected no active auction, got %+v", stats)
	}
	if stats.Online != 3 {
		t.Fatalf("expected live session online 3, got %+v", stats)
	}
}

func TestLiveSessionServiceSoldLotCannotUnmountAndSessionQueries(t *testing.T) {
	svc, _, auctionRepo := newLiveSessionFixture(t)
	ctx := context.Background()
	session, err := svc.Create(ctx, CreateLiveSessionInput{ActorID: "m_sold", ActorRole: domain.RoleMerchant, Title: "成交场"})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	sid := session.ID
	price := int64(500)
	lot := domain.AuctionLot{AuctionID: 8201, SellerID: "m_sold", AuctionType: domain.AuctionTypeEnglish, Status: domain.AuctionStatusClosedWon, LiveSessionID: &sid, DealPrice: &price}
	if err := auctionRepo.Create(ctx, &lot); err != nil {
		t.Fatalf("create sold lot: %v", err)
	}
	if err := svc.UnmountAuction(ctx, session.ID, lot.AuctionID, "m_sold", domain.RoleMerchant); !errors.Is(err, domain.ErrInvalidState) {
		t.Fatalf("sold lot should not unmount, got %v", err)
	}
	lots, err := svc.ListLots(ctx, session.ID, "m_sold", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("list lots: %v", err)
	}
	if len(lots) != 1 || lots[0].AuctionID != lot.AuctionID {
		t.Fatalf("session lots wrong: %+v", lots)
	}
	stats, err := svc.Stats(ctx, session.ID, "m_sold", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.LotsTotal != 1 {
		t.Fatalf("stats should aggregate by liveSessionId, got %+v", stats)
	}
}

func TestLiveSessionServiceStatsPreferRealtimeBidCount(t *testing.T) {
	svc, sessionRepo, auctionRepo := newLiveSessionFixture(t)
	ctx := context.Background()
	realtime := repository.NewMemoryRealtimeStore()
	svc.SetStatsDeps(repository.NewMemoryBidRepository(), realtime, nil)

	session := createStartedLiveSession(t, svc, "m_stats", "实时统计场")
	now := time.Now().UTC()
	lot := domain.AuctionLot{
		AuctionID:     8301,
		SellerID:      "m_stats",
		LiveSessionID: &session.ID,
		AuctionType:   domain.AuctionTypeEnglish,
		Status:        domain.AuctionStatusRunning,
		StartPrice:    1000,
		ReservePrice:  1000,
		IncrementRule: json.RawMessage(`{"type":"fixed","amount":100,"maxBidSteps":3}`),
		StartTime:     now.Add(-time.Second),
		EndTime:       now.Add(5 * time.Minute),
	}
	if err := auctionRepo.Create(ctx, &lot); err != nil {
		t.Fatalf("create lot: %v", err)
	}
	session.ActiveAuctionID = lot.AuctionID
	if err := sessionRepo.Update(ctx, &session); err != nil {
		t.Fatalf("mark active auction: %v", err)
	}
	if _, err := realtime.InitAuction(ctx, lot, 100); err != nil {
		t.Fatalf("init realtime auction: %v", err)
	}
	if err := realtime.MarkEnrollment(ctx, lot.AuctionID, "u_stats"); err != nil {
		t.Fatalf("mark enrollment: %v", err)
	}
	bid, err := realtime.PlaceBid(ctx, domain.BidInput{
		RequestID:            "stats-bid-1",
		AuctionID:            lot.AuctionID,
		LiveSessionID:        session.ID,
		BidderID:             "u_stats",
		Price:                1100,
		ExpectedCurrentPrice: expectedCurrentPrice(1000),
		Now:                  now,
		MinIncrement:         100,
		Source:               "live_ws",
	})
	if err != nil || !bid.Accepted {
		t.Fatalf("place bid: result=%+v err=%v", bid, err)
	}

	stats, err := svc.Stats(ctx, session.ID, "m_stats", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.CurrentPrice != 1100 || stats.CurrentBidCount != 1 {
		t.Fatalf("expected realtime price/count, got %+v", stats)
	}
}

// TestMemoryLiveSessionRealtimeStoreBumpViewerPeakMaxSemantics 验证 Memory 实现的 max 语义不丢更新。
func TestMemoryLiveSessionRealtimeStoreBumpViewerPeakMaxSemantics(t *testing.T) {
	store := repository.NewMemoryLiveSessionRealtimeStore()
	ctx := context.Background()
	const sessionID uint64 = 7777
	if v, err := store.BumpViewerPeak(ctx, sessionID, 5); err != nil || v != 5 {
		t.Fatalf("first bump: v=%d err=%v", v, err)
	}
	if v, err := store.BumpViewerPeak(ctx, sessionID, 3); err != nil || v != 5 {
		t.Fatalf("smaller bump should keep 5, got v=%d err=%v", v, err)
	}
	if v, err := store.BumpViewerPeak(ctx, sessionID, 9); err != nil || v != 9 {
		t.Fatalf("larger bump should overwrite, got v=%d err=%v", v, err)
	}
	_, peak, _ := store.LoadCounters(ctx, sessionID)
	if peak != 9 {
		t.Fatalf("LoadCounters peak=%d want 9", peak)
	}
}

// fakeLiveAgentHook 是用于断言 AI 托管开关持久化数字人标识的最小桩实现。
type fakeLiveAgentHook struct {
	enabled bool
}

func (h *fakeLiveAgentHook) EmitLiveStarted(context.Context, string, uint64)             {}
func (h *fakeLiveAgentHook) EmitLotMounted(context.Context, string, uint64, uint64)      {}
func (h *fakeLiveAgentHook) EmitLotUnmounted(context.Context, string, uint64, uint64)    {}
func (h *fakeLiveAgentHook) EmitLotStarted(context.Context, string, uint64, uint64, int) {}
func (h *fakeLiveAgentHook) EmitLotScheduled(context.Context, string, uint64, uint64, time.Time, int) {
}
func (h *fakeLiveAgentHook) EmitLotCancelled(context.Context, string, uint64, uint64) {}
func (h *fakeLiveAgentHook) GetConfig(context.Context, string) (LiveAgentHookConfig, error) {
	return LiveAgentHookConfig{Enabled: h.enabled}, nil
}
func (h *fakeLiveAgentHook) SetConfig(_ context.Context, _, _ string, enabled bool) (LiveAgentHookConfig, error) {
	h.enabled = enabled
	return LiveAgentHookConfig{Enabled: enabled}, nil
}
func (h *fakeLiveAgentHook) EmitConfigChanged(context.Context, string, uint64, bool) {}

// TestLiveSessionServiceUpdateAgentHookPersistsDigitalHumanFlag 验证：开启 AI 托管会把
// 场次自身持久化为数字人直播间，关闭则回落，刷新重拉（重新 Get）后标识仍稳定。
func TestLiveSessionServiceUpdateAgentHookPersistsDigitalHumanFlag(t *testing.T) {
	svc, sessionRepo, _ := newLiveSessionFixture(t)
	svc.SetLiveAgentHookService(&fakeLiveAgentHook{})
	ctx := context.Background()

	opened := createStartedLiveSession(t, svc, "m_dh", "数字人专场")
	if opened.IsDigitalHuman {
		t.Fatalf("expected new session not digital human by default")
	}

	if _, err := svc.UpdateAgentHookConfig(ctx, opened.ID, "m_dh", domain.RoleMerchant, true); err != nil {
		t.Fatalf("enable agent hook: %v", err)
	}
	reloaded, err := sessionRepo.Get(ctx, opened.ID)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if !reloaded.IsDigitalHuman {
		t.Fatalf("expected persisted IsDigitalHuman=true after enabling agent hook")
	}

	if _, err := svc.UpdateAgentHookConfig(ctx, opened.ID, "m_dh", domain.RoleMerchant, false); err != nil {
		t.Fatalf("disable agent hook: %v", err)
	}
	reloaded, err = sessionRepo.Get(ctx, opened.ID)
	if err != nil {
		t.Fatalf("reload after disable: %v", err)
	}
	if reloaded.IsDigitalHuman {
		t.Fatalf("expected persisted IsDigitalHuman=false after disabling agent hook")
	}
}

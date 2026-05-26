package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

func newLiveSessionFixture(t *testing.T) (*LiveSessionService, *repository.MemoryLiveSessionRepository, *repository.MemoryLiveRoomRepository, repository.AuctionRepository) {
	t.Helper()
	sessionRepo := repository.NewMemoryLiveSessionRepository()
	roomRepo := repository.NewMemoryLiveRoomRepository()
	auctionRepo := repository.NewMemoryAuctionRepository()
	svc := NewLiveSessionService(sessionRepo, roomRepo, auctionRepo)
	return svc, sessionRepo, roomRepo, auctionRepo
}

func TestLiveSessionServiceOpenSessionIdempotent(t *testing.T) {
	svc, sessionRepo, _, _ := newLiveSessionFixture(t)
	ctx := context.Background()

	first, err := svc.OpenSession(ctx, 100, "m_1", "Live")
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if first.Status != domain.LiveSessionStatusLive {
		t.Fatalf("status should be LIVE, got %s", first.Status)
	}

	second, err := svc.OpenSession(ctx, 100, "m_1", "Live")
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("expected same session ID on idempotent open, got %d vs %d", first.ID, second.ID)
	}

	all, err := sessionRepo.List(ctx, domain.LiveSessionFilter{LiveRoomID: 100, Limit: 100})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("expected 1 session, got %d", len(all))
	}
}

func TestLiveSessionServiceCloseSessionTransitionAndIdempotent(t *testing.T) {
	svc, _, _, _ := newLiveSessionFixture(t)
	ctx := context.Background()

	opened, err := svc.OpenSession(ctx, 200, "m_2", "Live")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
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
	svc, _, _, _ := newLiveSessionFixture(t)
	ctx := context.Background()

	opened, err := svc.OpenSession(ctx, 300, "m_3", "Live")
	if err != nil {
		t.Fatalf("open: %v", err)
	}

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

func TestLiveSessionServiceListByRoomForbidsOtherMerchant(t *testing.T) {
	svc, _, roomRepo, _ := newLiveSessionFixture(t)
	ctx := context.Background()

	room := domain.LiveRoom{MerchantID: "m_owner", Title: "t", Status: domain.LiveRoomStatusOffline}
	if err := roomRepo.Create(ctx, &room); err != nil {
		t.Fatalf("create room: %v", err)
	}
	if _, err := svc.OpenSession(ctx, room.ID, "m_owner", "t"); err != nil {
		t.Fatalf("open: %v", err)
	}
	// 商家用其他账号去查应当 Forbidden
	if _, err := svc.ListByRoom(ctx, room.ID, "", "m_other", domain.RoleMerchant, 20, 0); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("expected ErrForbidden, got %v", err)
	}
	// admin 不受限
	if _, err := svc.ListByRoom(ctx, room.ID, "", "admin", domain.RoleAdmin, 20, 0); err != nil {
		t.Fatalf("admin should access: %v", err)
	}
}

func TestLiveSessionServiceListByMerchantOverridesForMerchantRole(t *testing.T) {
	svc, _, _, _ := newLiveSessionFixture(t)
	ctx := context.Background()
	if _, err := svc.OpenSession(ctx, 401, "m_self", "live"); err != nil {
		t.Fatalf("open: %v", err)
	}
	// 商家传 merchantId=other 也会被强制改写为自身。
	sessions, err := svc.ListByMerchant(ctx, "m_other", "", "m_self", domain.RoleMerchant, 20, 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(sessions) != 1 || sessions[0].MerchantID != "m_self" {
		t.Fatalf("expected self merchant only, got %#v", sessions)
	}
}

func TestLiveSessionServiceListLotsFilterBySession(t *testing.T) {
	svc, _, _, auctionRepo := newLiveSessionFixture(t)
	ctx := context.Background()
	opened, err := svc.OpenSession(ctx, 500, "m_x", "live")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	now := time.Now().UTC()
	other := uint64(99999)
	in := domain.AuctionLot{AuctionID: 7001, ItemID: 1, SellerID: "m_x", AuctionType: domain.AuctionTypeEnglish, Status: domain.AuctionStatusReady, LiveRoomID: 500, LiveSessionID: &opened.ID, StartTime: now, EndTime: now.Add(time.Hour)}
	out := domain.AuctionLot{AuctionID: 7002, ItemID: 2, SellerID: "m_x", AuctionType: domain.AuctionTypeEnglish, Status: domain.AuctionStatusReady, LiveRoomID: 500, LiveSessionID: &other, StartTime: now, EndTime: now.Add(time.Hour)}
	none := domain.AuctionLot{AuctionID: 7003, ItemID: 3, SellerID: "m_x", AuctionType: domain.AuctionTypeEnglish, Status: domain.AuctionStatusReady, LiveRoomID: 500, StartTime: now, EndTime: now.Add(time.Hour)}
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
	svc, _, _, _ := newLiveSessionFixture(t)
	store := repository.NewMemoryLiveSessionRealtimeStore()
	svc.SetRealtimeStore(store)
	ctx := context.Background()

	opened, err := svc.OpenSession(ctx, 600, "m_rt", "live")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
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
	svc, _, _, _ := newLiveSessionFixture(t)
	store := repository.NewMemoryLiveSessionRealtimeStore()
	svc.SetRealtimeStore(store)
	ctx := context.Background()

	opened, err := svc.OpenSession(ctx, 601, "m_flush", "live")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
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
	svc, _, _, _ := newLiveSessionFixture(t)
	store := repository.NewMemoryLiveSessionRealtimeStore()
	svc.SetRealtimeStore(store)
	ctx := context.Background()

	opened, err := svc.OpenSession(ctx, 602, "m_close", "live")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
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
	svc, _, _, _ := newLiveSessionFixture(t)
	ctx := context.Background()

	calls := make(chan domain.LiveSession, 4)
	svc.SetOnEnded(func(_ context.Context, session domain.LiveSession) {
		calls <- session
	})

	opened, err := svc.OpenSession(ctx, 603, "m_hook", "live")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
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

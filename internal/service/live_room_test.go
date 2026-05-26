package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

func newLiveRoomFixture(t *testing.T) (*LiveRoomService, repository.AuctionRepository, *repository.MemoryLiveRoomLock, *repository.MemoryLiveRoomRepository) {
	t.Helper()
	auctionRepo := repository.NewMemoryAuctionRepository()
	roomRepo := repository.NewMemoryLiveRoomRepository()
	lock := repository.NewMemoryLiveRoomLock()
	svc := NewLiveRoomService(roomRepo, auctionRepo, repository.NoopTxManager{}, lock)
	return svc, auctionRepo, lock, roomRepo
}

func TestLiveRoomServiceCreateMerchantOverride(t *testing.T) {
	svc, _, _, _ := newLiveRoomFixture(t)
	ctx := context.Background()

	room, err := svc.Create(ctx, CreateLiveRoomInput{
		ActorID:    "m_1",
		ActorRole:  domain.RoleMerchant,
		MerchantID: "m_other",
		Title:      "我的直播间",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if room.MerchantID != "m_1" {
		t.Fatalf("merchant should be overridden to actor, got %s", room.MerchantID)
	}
	if room.Status != domain.LiveRoomStatusOffline {
		t.Fatalf("default status should be OFFLINE, got %s", room.Status)
	}
	if room.ID == 0 {
		t.Fatalf("expected non-zero ID")
	}
}

func TestLiveRoomServiceCreateRequiresTitle(t *testing.T) {
	svc, _, _, _ := newLiveRoomFixture(t)
	if _, err := svc.Create(context.Background(), CreateLiveRoomInput{ActorID: "m_1", ActorRole: domain.RoleMerchant, Title: "  "}); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got %v", err)
	}
}

func TestLiveRoomServiceListBuyerOnlySeesLiveRooms(t *testing.T) {
	svc, _, _, _ := newLiveRoomFixture(t)
	ctx := context.Background()
	fixtures := []CreateLiveRoomInput{
		{ActorID: "m_live", ActorRole: domain.RoleMerchant, Title: "live", Status: domain.LiveRoomStatusLive},
		{ActorID: "m_offline", ActorRole: domain.RoleMerchant, Title: "offline", Status: domain.LiveRoomStatusOffline},
		{ActorID: "m_closed", ActorRole: domain.RoleMerchant, Title: "closed", Status: domain.LiveRoomStatusClosed},
	}
	for _, in := range fixtures {
		if _, err := svc.Create(ctx, in); err != nil {
			t.Fatalf("create %s room: %v", in.ActorID, err)
		}
	}

	rooms, err := svc.List(ctx, domain.LiveRoomFilter{Limit: 10}, "u_1", domain.RoleBuyer)
	if err != nil {
		t.Fatalf("buyer list: %v", err)
	}
	if len(rooms) != 1 || rooms[0].Status != domain.LiveRoomStatusLive {
		t.Fatalf("expected buyer to see only live room, got %+v", rooms)
	}

	rooms, err = svc.List(ctx, domain.LiveRoomFilter{Status: domain.LiveRoomStatusClosed, Limit: 10}, "u_1", domain.RoleBuyer)
	if err != nil {
		t.Fatalf("buyer list with closed filter: %v", err)
	}
	if len(rooms) != 1 || rooms[0].Status != domain.LiveRoomStatusLive {
		t.Fatalf("expected buyer status filter to be forced to LIVE, got %+v", rooms)
	}
}

func TestLiveRoomServiceUpdateForbidden(t *testing.T) {
	svc, _, _, _ := newLiveRoomFixture(t)
	ctx := context.Background()
	room, err := svc.Create(ctx, CreateLiveRoomInput{ActorID: "m_1", ActorRole: domain.RoleMerchant, Title: "t"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := svc.Update(ctx, room.ID, UpdateLiveRoomInput{ActorID: "m_2", ActorRole: domain.RoleMerchant}); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("expected ErrForbidden, got %v", err)
	}
}

func TestLiveRoomServiceActivateAuctionLock(t *testing.T) {
	svc, auctionRepo, lock, _ := newLiveRoomFixture(t)
	ctx := context.Background()

	room, err := svc.Create(ctx, CreateLiveRoomInput{ActorID: "m_1", ActorRole: domain.RoleMerchant, Title: "t"})
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	now := time.Now().UTC()
	a1 := domain.AuctionLot{
		AuctionID: 5001, ItemID: 1, SellerID: "m_1", AuctionType: domain.AuctionTypeEnglish,
		Status: domain.AuctionStatusReady, LiveRoomID: room.ID,
		StartTime: now, EndTime: now.Add(time.Hour),
	}
	if err := auctionRepo.Create(ctx, &a1); err != nil {
		t.Fatalf("create a1: %v", err)
	}
	a2 := domain.AuctionLot{
		AuctionID: 5002, ItemID: 2, SellerID: "m_1", AuctionType: domain.AuctionTypeEnglish,
		Status: domain.AuctionStatusReady, LiveRoomID: room.ID,
		StartTime: now, EndTime: now.Add(time.Hour),
	}
	if err := auctionRepo.Create(ctx, &a2); err != nil {
		t.Fatalf("create a2: %v", err)
	}

	if _, err := svc.ActivateAuction(ctx, room.ID, a1.AuctionID, "m_1", domain.RoleMerchant); err != nil {
		t.Fatalf("activate a1: %v", err)
	}
	current, err := lock.Current(ctx, room.ID)
	if err != nil {
		t.Fatalf("current: %v", err)
	}
	if current != a1.AuctionID {
		t.Fatalf("expected lock holder %d, got %d", a1.AuctionID, current)
	}

	if _, err := svc.ActivateAuction(ctx, room.ID, a2.AuctionID, "m_1", domain.RoleMerchant); !errors.Is(err, ErrLiveRoomBusy) {
		t.Fatalf("expected ErrLiveRoomBusy, got %v", err)
	}

	// Re-activating same auction should be re-entrant.
	if _, err := svc.ActivateAuction(ctx, room.ID, a1.AuctionID, "m_1", domain.RoleMerchant); err != nil {
		t.Fatalf("re-activate same: %v", err)
	}
}

func TestLiveRoomServiceActivateRequiresMembership(t *testing.T) {
	svc, auctionRepo, _, _ := newLiveRoomFixture(t)
	ctx := context.Background()
	room, err := svc.Create(ctx, CreateLiveRoomInput{ActorID: "m_1", ActorRole: domain.RoleMerchant, Title: "t"})
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	now := time.Now().UTC()
	stray := domain.AuctionLot{AuctionID: 6001, ItemID: 9, SellerID: "m_1", AuctionType: domain.AuctionTypeEnglish, Status: domain.AuctionStatusReady, LiveRoomID: 0, StartTime: now, EndTime: now.Add(time.Hour)}
	if err := auctionRepo.Create(ctx, &stray); err != nil {
		t.Fatalf("create stray: %v", err)
	}
	if _, err := svc.ActivateAuction(ctx, room.ID, stray.AuctionID, "m_1", domain.RoleMerchant); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got %v", err)
	}
}

func TestLiveRoomServiceOnAuctionClosedReleasesLock(t *testing.T) {
	svc, auctionRepo, lock, _ := newLiveRoomFixture(t)
	ctx := context.Background()
	room, err := svc.Create(ctx, CreateLiveRoomInput{ActorID: "m_1", ActorRole: domain.RoleMerchant, Title: "t"})
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	now := time.Now().UTC()
	a := domain.AuctionLot{AuctionID: 7001, ItemID: 1, SellerID: "m_1", AuctionType: domain.AuctionTypeEnglish, Status: domain.AuctionStatusReady, LiveRoomID: room.ID, StartTime: now, EndTime: now.Add(time.Hour)}
	if err := auctionRepo.Create(ctx, &a); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	if _, err := svc.ActivateAuction(ctx, room.ID, a.AuctionID, "m_1", domain.RoleMerchant); err != nil {
		t.Fatalf("activate: %v", err)
	}

	svc.OnAuctionClosed(ctx, a.AuctionID)

	current, err := lock.Current(ctx, room.ID)
	if err != nil {
		t.Fatalf("current: %v", err)
	}
	if current != 0 {
		t.Fatalf("expected lock released, got holder %d", current)
	}
	got, err := svc.Get(ctx, room.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ActiveAuctionID != 0 || got.Status != domain.LiveRoomStatusLive {
		t.Fatalf("expected room to keep live with no active auction, got %+v", got)
	}
}

func TestLiveRoomServiceDeactivateKeepsRoomLive(t *testing.T) {
	svc, auctionRepo, _, _ := newLiveRoomFixture(t)
	ctx := context.Background()
	room, err := svc.Create(ctx, CreateLiveRoomInput{ActorID: "m_1", ActorRole: domain.RoleMerchant, Title: "t"})
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	now := time.Now().UTC()
	a := domain.AuctionLot{AuctionID: 7101, ItemID: 1, SellerID: "m_1", AuctionType: domain.AuctionTypeEnglish, Status: domain.AuctionStatusReady, LiveRoomID: room.ID, StartTime: now, EndTime: now.Add(time.Hour)}
	if err := auctionRepo.Create(ctx, &a); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	if _, err := svc.ActivateAuction(ctx, room.ID, a.AuctionID, "m_1", domain.RoleMerchant); err != nil {
		t.Fatalf("activate: %v", err)
	}

	got, err := svc.DeactivateAuction(ctx, room.ID, "m_1", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if got.ActiveAuctionID != 0 || got.Status != domain.LiveRoomStatusLive {
		t.Fatalf("expected room to keep live with no active auction, got %+v", got)
	}
}

func TestLiveRoomServiceActivateUsesDurationSec(t *testing.T) {
	svc, auctionRepo, _, _ := newLiveRoomFixture(t)
	ctx := context.Background()
	auctionSvc := NewAuctionService(auctionRepo, repository.NewMemoryItemRepository(), repository.NoopTxManager{})
	svc.SetAuctionService(auctionSvc)
	room, err := svc.Create(ctx, CreateLiveRoomInput{ActorID: "m_1", ActorRole: domain.RoleMerchant, Title: "t"})
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	now := time.Now().UTC().Add(time.Hour)
	a := domain.AuctionLot{AuctionID: 7201, ItemID: 1, SellerID: "m_1", AuctionType: domain.AuctionTypeEnglish, Status: domain.AuctionStatusReady, LiveRoomID: room.ID, StartTime: now, EndTime: now.Add(time.Hour)}
	if err := auctionRepo.Create(ctx, &a); err != nil {
		t.Fatalf("create auction: %v", err)
	}

	started, err := svc.ActivateAuctionWithOptions(ctx, ActivateAuctionInput{RoomID: room.ID, AuctionID: a.AuctionID, ActorID: "m_1", ActorRole: domain.RoleMerchant, DurationSec: 600})
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	if got := int(started.EndTime.Sub(started.StartTime).Seconds()); got != 600 {
		t.Fatalf("expected 600 second duration, got %d", got)
	}
	stored, err := auctionRepo.FindByID(ctx, a.AuctionID)
	if err != nil {
		t.Fatalf("find auction: %v", err)
	}
	if got := int(stored.EndTime.Sub(stored.StartTime).Seconds()); got != 600 {
		t.Fatalf("expected stored 600 second duration, got %d", got)
	}
}

func TestAuctionCancelClearsActiveAuctionAndKeepsLive(t *testing.T) {
	svc, auctionRepo, _, _ := newLiveRoomFixture(t)
	ctx := context.Background()
	auctionSvc := NewAuctionService(auctionRepo, repository.NewMemoryItemRepository(), repository.NoopTxManager{})
	auctionSvc.SetOnClose(svc.OnAuctionClosed)
	svc.SetAuctionService(auctionSvc)
	room, err := svc.Create(ctx, CreateLiveRoomInput{ActorID: "m_1", ActorRole: domain.RoleMerchant, Title: "t"})
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	now := time.Now().UTC()
	a := domain.AuctionLot{AuctionID: 7301, ItemID: 1, SellerID: "m_1", AuctionType: domain.AuctionTypeEnglish, Status: domain.AuctionStatusReady, LiveRoomID: room.ID, StartTime: now, EndTime: now.Add(time.Hour)}
	if err := auctionRepo.Create(ctx, &a); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	if _, err := svc.ActivateAuction(ctx, room.ID, a.AuctionID, "m_1", domain.RoleMerchant); err != nil {
		t.Fatalf("activate: %v", err)
	}

	if _, err := auctionSvc.Cancel(ctx, a.AuctionID, "m_1", domain.RoleMerchant); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	got, err := svc.Get(ctx, room.ID)
	if err != nil {
		t.Fatalf("get room: %v", err)
	}
	if got.ActiveAuctionID != 0 || got.Status != domain.LiveRoomStatusLive {
		t.Fatalf("expected cancel to clear active auction and keep live room, got %+v", got)
	}
}

func TestLiveRoomServiceDeleteBlockedWhileActive(t *testing.T) {
	svc, auctionRepo, _, _ := newLiveRoomFixture(t)
	ctx := context.Background()
	room, err := svc.Create(ctx, CreateLiveRoomInput{ActorID: "m_1", ActorRole: domain.RoleMerchant, Title: "t"})
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	now := time.Now().UTC()
	a := domain.AuctionLot{AuctionID: 8001, ItemID: 1, SellerID: "m_1", AuctionType: domain.AuctionTypeEnglish, Status: domain.AuctionStatusReady, LiveRoomID: room.ID, StartTime: now, EndTime: now.Add(time.Hour)}
	if err := auctionRepo.Create(ctx, &a); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	if _, err := svc.ActivateAuction(ctx, room.ID, a.AuctionID, "m_1", domain.RoleMerchant); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if err := svc.Delete(ctx, room.ID, "m_1", domain.RoleMerchant); !errors.Is(err, domain.ErrInvalidState) {
		t.Fatalf("expected ErrInvalidState, got %v", err)
	}
}

func TestMemoryLiveRoomLockTTLExpiry(t *testing.T) {
	lock := repository.NewMemoryLiveRoomLock()
	ctx := context.Background()
	ok, _, err := lock.Acquire(ctx, 1, 100, 5*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("first acquire: ok=%v err=%v", ok, err)
	}
	ok, holder, _ := lock.Acquire(ctx, 1, 200, time.Hour)
	if ok || holder != 100 {
		t.Fatalf("expected busy by 100, got ok=%v holder=%d", ok, holder)
	}
	time.Sleep(15 * time.Millisecond)
	ok, _, err = lock.Acquire(ctx, 1, 200, time.Hour)
	if err != nil || !ok {
		t.Fatalf("expected re-acquire after expiry, ok=%v err=%v", ok, err)
	}
}

func TestLiveRoomServiceMountAuctionSuccess(t *testing.T) {
	svc, auctionRepo, _, _ := newLiveRoomFixture(t)
	ctx := context.Background()
	room, err := svc.Create(ctx, CreateLiveRoomInput{ActorID: "m_1", ActorRole: domain.RoleMerchant, Title: "t"})
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	now := time.Now().UTC()
	a := domain.AuctionLot{
		AuctionID: 9001, ItemID: 1, SellerID: "m_1", AuctionType: domain.AuctionTypeEnglish,
		Status: domain.AuctionStatusReady, LiveRoomID: 0,
		StartTime: now, EndTime: now.Add(time.Hour),
	}
	if err := auctionRepo.Create(ctx, &a); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	lot, err := svc.MountAuction(ctx, room.ID, a.AuctionID, "m_1", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("mount: %v", err)
	}
	if lot.LiveRoomID != room.ID {
		t.Fatalf("expected lot.LiveRoomID=%d, got %d", room.ID, lot.LiveRoomID)
	}
	stored, err := auctionRepo.FindByID(ctx, a.AuctionID)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if stored.LiveRoomID != room.ID {
		t.Fatalf("expected stored LiveRoomID=%d, got %d", room.ID, stored.LiveRoomID)
	}
}

func TestLiveRoomServiceMountAuctionForbiddenForeignSeller(t *testing.T) {
	svc, auctionRepo, _, _ := newLiveRoomFixture(t)
	ctx := context.Background()
	room, err := svc.Create(ctx, CreateLiveRoomInput{ActorID: "m_1", ActorRole: domain.RoleMerchant, Title: "t"})
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	now := time.Now().UTC()
	a := domain.AuctionLot{
		AuctionID: 9101, ItemID: 1, SellerID: "m_other", AuctionType: domain.AuctionTypeEnglish,
		Status: domain.AuctionStatusReady, LiveRoomID: 0,
		StartTime: now, EndTime: now.Add(time.Hour),
	}
	if err := auctionRepo.Create(ctx, &a); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	if _, err := svc.MountAuction(ctx, room.ID, a.AuctionID, "m_1", domain.RoleMerchant); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("expected ErrForbidden, got %v", err)
	}
}

func TestLiveRoomServiceMountAuctionConflictAlreadyMounted(t *testing.T) {
	svc, auctionRepo, _, _ := newLiveRoomFixture(t)
	ctx := context.Background()
	room, err := svc.Create(ctx, CreateLiveRoomInput{ActorID: "m_1", ActorRole: domain.RoleMerchant, Title: "t"})
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	now := time.Now().UTC()
	a := domain.AuctionLot{
		AuctionID: 9201, ItemID: 1, SellerID: "m_1", AuctionType: domain.AuctionTypeEnglish,
		Status: domain.AuctionStatusReady, LiveRoomID: 99,
		StartTime: now, EndTime: now.Add(time.Hour),
	}
	if err := auctionRepo.Create(ctx, &a); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	if _, err := svc.MountAuction(ctx, room.ID, a.AuctionID, "m_1", domain.RoleMerchant); !errors.Is(err, ErrLotAlreadyMounted) {
		t.Fatalf("expected ErrLotAlreadyMounted, got %v", err)
	}
}

func TestLiveRoomServiceMountAuctionInvalidStateRunning(t *testing.T) {
	svc, auctionRepo, _, _ := newLiveRoomFixture(t)
	ctx := context.Background()
	room, err := svc.Create(ctx, CreateLiveRoomInput{ActorID: "m_1", ActorRole: domain.RoleMerchant, Title: "t"})
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	now := time.Now().UTC()
	a := domain.AuctionLot{
		AuctionID: 9301, ItemID: 1, SellerID: "m_1", AuctionType: domain.AuctionTypeEnglish,
		Status: domain.AuctionStatusRunning, LiveRoomID: 0,
		StartTime: now, EndTime: now.Add(time.Hour),
	}
	if err := auctionRepo.Create(ctx, &a); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	if _, err := svc.MountAuction(ctx, room.ID, a.AuctionID, "m_1", domain.RoleMerchant); !errors.Is(err, domain.ErrInvalidState) {
		t.Fatalf("expected ErrInvalidState, got %v", err)
	}
}

func TestLiveRoomServiceUnmountAuctionRejectActive(t *testing.T) {
	svc, auctionRepo, _, _ := newLiveRoomFixture(t)
	ctx := context.Background()
	room, err := svc.Create(ctx, CreateLiveRoomInput{ActorID: "m_1", ActorRole: domain.RoleMerchant, Title: "t"})
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	now := time.Now().UTC()
	a := domain.AuctionLot{
		AuctionID: 9401, ItemID: 1, SellerID: "m_1", AuctionType: domain.AuctionTypeEnglish,
		Status: domain.AuctionStatusReady, LiveRoomID: room.ID,
		StartTime: now, EndTime: now.Add(time.Hour),
	}
	if err := auctionRepo.Create(ctx, &a); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	if _, err := svc.ActivateAuction(ctx, room.ID, a.AuctionID, "m_1", domain.RoleMerchant); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if err := svc.UnmountAuction(ctx, room.ID, a.AuctionID, "m_1", domain.RoleMerchant); !errors.Is(err, domain.ErrInvalidState) {
		t.Fatalf("expected ErrInvalidState, got %v", err)
	}
}

func TestLiveRoomServiceUnmountAuctionSuccess(t *testing.T) {
	svc, auctionRepo, _, _ := newLiveRoomFixture(t)
	ctx := context.Background()
	room, err := svc.Create(ctx, CreateLiveRoomInput{ActorID: "m_1", ActorRole: domain.RoleMerchant, Title: "t"})
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	now := time.Now().UTC()
	a := domain.AuctionLot{
		AuctionID: 9501, ItemID: 1, SellerID: "m_1", AuctionType: domain.AuctionTypeEnglish,
		Status: domain.AuctionStatusReady, LiveRoomID: 0,
		StartTime: now, EndTime: now.Add(time.Hour),
	}
	if err := auctionRepo.Create(ctx, &a); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	if _, err := svc.MountAuction(ctx, room.ID, a.AuctionID, "m_1", domain.RoleMerchant); err != nil {
		t.Fatalf("mount: %v", err)
	}
	if err := svc.UnmountAuction(ctx, room.ID, a.AuctionID, "m_1", domain.RoleMerchant); err != nil {
		t.Fatalf("unmount: %v", err)
	}
	stored, err := auctionRepo.FindByID(ctx, a.AuctionID)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if stored.LiveRoomID != 0 {
		t.Fatalf("expected LiveRoomID reset, got %d", stored.LiveRoomID)
	}
	lots, err := svc.ListLots(ctx, room.ID)
	if err != nil {
		t.Fatalf("list lots: %v", err)
	}
	if len(lots) != 0 {
		t.Fatalf("expected empty lots, got %d", len(lots))
	}
}

func TestLiveRoomServiceStatsBasic(t *testing.T) {
	svc, auctionRepo, _, _ := newLiveRoomFixture(t)
	ctx := context.Background()
	room, err := svc.Create(ctx, CreateLiveRoomInput{ActorID: "m_1", ActorRole: domain.RoleMerchant, Title: "t"})
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	now := time.Now().UTC()
	for i, id := range []uint64{9601, 9602, 9603} {
		a := domain.AuctionLot{
			AuctionID: id, ItemID: uint64(i + 1), SellerID: "m_1", AuctionType: domain.AuctionTypeEnglish,
			Status: domain.AuctionStatusReady, LiveRoomID: 0,
			StartTime: now, EndTime: now.Add(time.Hour),
		}
		if err := auctionRepo.Create(ctx, &a); err != nil {
			t.Fatalf("create auction %d: %v", id, err)
		}
		if _, err := svc.MountAuction(ctx, room.ID, id, "m_1", domain.RoleMerchant); err != nil {
			t.Fatalf("mount %d: %v", id, err)
		}
	}
	stats, err := svc.Stats(ctx, room.ID)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.RoomID != room.ID {
		t.Fatalf("expected RoomID=%d, got %d", room.ID, stats.RoomID)
	}
	if stats.LotsTotal != 3 {
		t.Fatalf("expected LotsTotal=3, got %d", stats.LotsTotal)
	}
	if stats.ActiveAuctionID != 0 {
		t.Fatalf("expected ActiveAuctionID=0, got %d", stats.ActiveAuctionID)
	}
	if stats.Online != 0 || stats.CurrentBidCount != 0 || stats.CurrentPrice != 0 || stats.CurrentRemainSeconds != 0 {
		t.Fatalf("expected zero current* fields when no active auction, got %+v", stats)
	}
}

func TestCreateLiveRoomDuplicateMerchantRejected(t *testing.T) {
	svc, _, _, _ := newLiveRoomFixture(t)
	ctx := context.Background()
	if _, err := svc.Create(ctx, CreateLiveRoomInput{ActorID: "m_1", ActorRole: domain.RoleMerchant, Title: "首播间"}); err != nil {
		t.Fatalf("first create: %v", err)
	}
	_, err := svc.Create(ctx, CreateLiveRoomInput{ActorID: "m_1", ActorRole: domain.RoleMerchant, Title: "重复直播间"})
	if !errors.Is(err, ErrLiveRoomAlreadyExists) {
		t.Fatalf("expected ErrLiveRoomAlreadyExists, got %v", err)
	}
}

func TestCreateLiveRoomDifferentMerchantsAllowed(t *testing.T) {
	svc, _, _, _ := newLiveRoomFixture(t)
	ctx := context.Background()
	r1, err := svc.Create(ctx, CreateLiveRoomInput{ActorID: "m_1", ActorRole: domain.RoleMerchant, Title: "m1 直播间"})
	if err != nil {
		t.Fatalf("create m_1: %v", err)
	}
	r2, err := svc.Create(ctx, CreateLiveRoomInput{ActorID: "m_2", ActorRole: domain.RoleMerchant, Title: "m2 直播间"})
	if err != nil {
		t.Fatalf("create m_2: %v", err)
	}
	if r1.ID == 0 || r2.ID == 0 || r1.ID == r2.ID {
		t.Fatalf("expected distinct non-zero room IDs, got r1=%d r2=%d", r1.ID, r2.ID)
	}
	if r1.MerchantID != "m_1" || r2.MerchantID != "m_2" {
		t.Fatalf("merchant binding wrong: r1=%s r2=%s", r1.MerchantID, r2.MerchantID)
	}
}

func TestMountAuctionAllowedWhenRoomOffline(t *testing.T) {
	svc, auctionRepo, _, _ := newLiveRoomFixture(t)
	ctx := context.Background()
	room, err := svc.Create(ctx, CreateLiveRoomInput{ActorID: "m_1", ActorRole: domain.RoleMerchant, Title: "预热直播间"})
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	if room.Status != domain.LiveRoomStatusOffline {
		t.Fatalf("precondition: expected OFFLINE, got %s", room.Status)
	}
	now := time.Now().UTC()
	a := domain.AuctionLot{
		AuctionID: 9701, ItemID: 1, SellerID: "m_1", AuctionType: domain.AuctionTypeEnglish,
		Status: domain.AuctionStatusReady, LiveRoomID: 0,
		StartTime: now, EndTime: now.Add(time.Hour),
	}
	if err := auctionRepo.Create(ctx, &a); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	lot, err := svc.MountAuction(ctx, room.ID, a.AuctionID, "m_1", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("mount on OFFLINE room: %v", err)
	}
	if lot.LiveRoomID != room.ID {
		t.Fatalf("expected lot.LiveRoomID=%d, got %d", room.ID, lot.LiveRoomID)
	}
	got, err := svc.Get(ctx, room.ID)
	if err != nil {
		t.Fatalf("get room: %v", err)
	}
	if got.Status != domain.LiveRoomStatusOffline {
		t.Fatalf("room status should remain OFFLINE after mount, got %s", got.Status)
	}
}

// TestDeactivateWithHammerClosesActiveLot 验证 DeactivateAuction 在有 HammerService 时，
// 会先通过 Hammer 把 RUNNING 的 lot 关闭（Force=true），再释放房间锁。
func TestDeactivateWithHammerClosesActiveLot(t *testing.T) {
	svc, auctionRepo, _, _ := newLiveRoomFixture(t)
	ctx := context.Background()

	// 构建完整 HammerService（需要 deposit/order/realtime）
	depositRepo := repository.NewMemoryDepositRepository()
	orderRepo := repository.NewMemoryOrderRepository()
	realtime := repository.NewMemoryRealtimeStore()
	hammerSvc := NewHammerService(auctionRepo, orderRepo, depositRepo, realtime, repository.NoopTxManager{}, nil)
	hammerSvc.SetOnClose(svc.OnAuctionClosed)
	svc.SetHammerService(hammerSvc)

	room, err := svc.Create(ctx, CreateLiveRoomInput{ActorID: "m_1", ActorRole: domain.RoleMerchant, Title: "t"})
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	now := time.Now().UTC()
	a := domain.AuctionLot{
		AuctionID: 10001, ItemID: 1, SellerID: "m_1", AuctionType: domain.AuctionTypeEnglish,
		Status: domain.AuctionStatusRunning, LiveRoomID: room.ID,
		StartTime: now.Add(-10 * time.Minute), EndTime: now.Add(-time.Minute),
	}
	if err := auctionRepo.Create(ctx, &a); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	// HammerService 需要 realtime store 中存在该 auction 的状态条目。
	if _, err := realtime.InitAuction(ctx, a, 100); err != nil {
		t.Fatalf("init realtime auction: %v", err)
	}
	if _, err := svc.ActivateAuction(ctx, room.ID, a.AuctionID, "m_1", domain.RoleMerchant); err != nil {
		t.Fatalf("activate: %v", err)
	}

	deactivated, err := svc.DeactivateAuction(ctx, room.ID, "m_1", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if deactivated.ActiveAuctionID != 0 {
		t.Fatalf("expected ActiveAuctionID=0, got %d", deactivated.ActiveAuctionID)
	}
	// 验证 auction 已被 Hammer 关闭
	auction, err := auctionRepo.FindByID(ctx, a.AuctionID)
	if err != nil {
		t.Fatalf("find auction: %v", err)
	}
	if !auction.Status.Terminal() {
		t.Fatalf("expected terminal status, got %s", auction.Status)
	}
}

// TestDeactivateNoHammerStillReleasesLock 验证当 HammerService 未注入时，
// DeactivateAuction 仍然能释放房间锁（兜底路径）。
func TestDeactivateNoHammerStillReleasesLock(t *testing.T) {
	svc, auctionRepo, _, _ := newLiveRoomFixture(t)
	ctx := context.Background()
	// 不 SetHammerService — 测试兜底

	room, err := svc.Create(ctx, CreateLiveRoomInput{ActorID: "m_1", ActorRole: domain.RoleMerchant, Title: "t"})
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	now := time.Now().UTC()
	a := domain.AuctionLot{
		AuctionID: 10101, ItemID: 1, SellerID: "m_1", AuctionType: domain.AuctionTypeEnglish,
		Status: domain.AuctionStatusRunning, LiveRoomID: room.ID,
		StartTime: now.Add(-5 * time.Minute), EndTime: now.Add(-time.Minute),
	}
	if err := auctionRepo.Create(ctx, &a); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	if _, err := svc.ActivateAuction(ctx, room.ID, a.AuctionID, "m_1", domain.RoleMerchant); err != nil {
		t.Fatalf("activate: %v", err)
	}

	deactivated, err := svc.DeactivateAuction(ctx, room.ID, "m_1", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if deactivated.ActiveAuctionID != 0 {
		t.Fatalf("expected ActiveAuctionID=0 even without hammer, got %d", deactivated.ActiveAuctionID)
	}
}

// TestDeactivateWithAlreadyTerminalLot 验证当 active lot 已是终态（比如被外部操作关闭），
// DeactivateAuction 不应 panic，也应正确释放锁。
func TestDeactivateWithAlreadyTerminalLot(t *testing.T) {
	svc, auctionRepo, _, _ := newLiveRoomFixture(t)
	ctx := context.Background()
	depositRepo := repository.NewMemoryDepositRepository()
	orderRepo := repository.NewMemoryOrderRepository()
	realtime := repository.NewMemoryRealtimeStore()
	hammerSvc := NewHammerService(auctionRepo, orderRepo, depositRepo, realtime, repository.NoopTxManager{}, nil)
	hammerSvc.SetOnClose(svc.OnAuctionClosed)
	svc.SetHammerService(hammerSvc)

	room, err := svc.Create(ctx, CreateLiveRoomInput{ActorID: "m_1", ActorRole: domain.RoleMerchant, Title: "t"})
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	now := time.Now().UTC()
	a := domain.AuctionLot{
		AuctionID: 10201, ItemID: 1, SellerID: "m_1", AuctionType: domain.AuctionTypeEnglish,
		Status: domain.AuctionStatusClosedFailed, LiveRoomID: room.ID,
		StartTime: now.Add(-10 * time.Minute), EndTime: now.Add(-time.Minute),
	}
	if err := auctionRepo.Create(ctx, &a); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	// 强制设置 room.ActiveAuctionID 为已终态的 lot（模拟状态不一致）
	roomVal, _ := svc.Get(ctx, room.ID)
	roomVal.ActiveAuctionID = a.AuctionID
	roomVal.Status = domain.LiveRoomStatusLive
	_ = svc.rooms.Update(ctx, &roomVal)

	deactivated, err := svc.DeactivateAuction(ctx, room.ID, "m_1", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("deactivate with terminal lot: %v", err)
	}
	if deactivated.ActiveAuctionID != 0 {
		t.Fatalf("expected ActiveAuctionID=0, got %d", deactivated.ActiveAuctionID)
	}
}

package service

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

func TestLiveAgentHookServiceEmitsHighestBidWithNickname(t *testing.T) {
	invoker := newRecordingLiveAgentHookInvoker()
	hook := NewLiveAgentHookService(repository.NewMemoryConfigRepository(), repository.NewSeedUserRepository(), invoker)
	ctx := context.Background()

	if _, err := hook.SetConfig(ctx, "u_2001", "u_2001", true); err != nil {
		t.Fatalf("enable hook: %v", err)
	}
	hook.EmitHighestBid(ctx, "u_2001", 90001, "u_1001", 12345)

	msg := invoker.wait(t)
	if !strings.Contains(msg, "直播间90001") || !strings.Contains(msg, "竞拍用户001") || !strings.Contains(msg, "12345分") {
		t.Fatalf("unexpected highest bid hook message: %q", msg)
	}
}

func TestLiveRoomSessionAndItemEmitLiveAgentHooks(t *testing.T) {
	ctx := context.Background()
	invoker := newRecordingLiveAgentHookInvoker()
	hook := NewLiveAgentHookService(repository.NewMemoryConfigRepository(), repository.NewSeedUserRepository(), invoker)

	auctionRepo := repository.NewMemoryAuctionRepository()
	roomRepo := repository.NewMemoryLiveRoomRepository()
	roomSvc := NewLiveRoomService(roomRepo, auctionRepo, repository.NoopTxManager{}, repository.NewMemoryLiveRoomLock())
	roomSvc.SetLiveAgentHookService(hook)
	sessionSvc := NewLiveSessionService(repository.NewMemoryLiveSessionRepository(), roomRepo, auctionRepo)
	sessionSvc.SetLiveAgentHookService(hook)

	room, err := roomSvc.Create(ctx, CreateLiveRoomInput{
		ActorID:   "u_2001",
		ActorRole: domain.RoleMerchant,
		Title:     "直播间",
	})
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	if _, err := roomSvc.UpdateAgentHookConfig(ctx, room.ID, "u_2001", domain.RoleMerchant, true); err != nil {
		t.Fatalf("enable hook: %v", err)
	}
	if msg := invoker.wait(t); msg != "直播间"+strconv.FormatUint(room.ID, 10)+"AI直播助手已开启" {
		t.Fatalf("unexpected hook config changed message: %q", msg)
	}

	session, err := sessionSvc.OpenSession(ctx, room.ID, room.MerchantID, room.Title)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	roomIDText := strconv.FormatUint(room.ID, 10)
	if msg := invoker.wait(t); !strings.Contains(msg, "直播间"+roomIDText+"的直播场次") || !strings.Contains(msg, "开播了") || !strings.Contains(msg, strconv.FormatUint(session.ID, 10)) {
		t.Fatalf("unexpected live started hook message: %q", msg)
	}

	itemRepo := repository.NewMemoryItemRepository()
	itemSvc := NewItemService(itemRepo)
	itemSvc.SetLiveAgentHookService(hook)
	item := domain.Item{
		SellerID:       room.MerchantID,
		Title:          "商品",
		Category:       "分类",
		ConditionGrade: domain.ConditionNew,
		Images:         []byte("[]"),
		Status:         domain.ItemStatusReady,
	}
	if err := itemRepo.Create(ctx, &item); err != nil {
		t.Fatalf("create item: %v", err)
	}
	listed := domain.ItemStatusListed
	if _, err := itemSvc.Update(ctx, item.ID, UpdateItemInput{ActorID: "admin", ActorRole: domain.RoleAdmin, Status: &listed}); err != nil {
		t.Fatalf("list item: %v", err)
	}
	itemIDText := strconv.FormatUint(item.ID, 10)
	if msg := invoker.wait(t); msg != "商品"+itemIDText+"上架了" {
		t.Fatalf("unexpected item listed hook message: %q", msg)
	}
	offline := domain.ItemStatusOffline
	if _, err := itemSvc.Update(ctx, item.ID, UpdateItemInput{ActorID: "admin", ActorRole: domain.RoleAdmin, Status: &offline}); err != nil {
		t.Fatalf("offline item: %v", err)
	}
	if msg := invoker.wait(t); msg != "商品"+itemIDText+"下架了" {
		t.Fatalf("unexpected item offline hook message: %q", msg)
	}

	if _, err := roomSvc.UpdateAgentHookConfig(ctx, room.ID, "u_2001", domain.RoleMerchant, false); err != nil {
		t.Fatalf("disable hook: %v", err)
	}
	select {
	case msg := <-invoker.ch:
		t.Fatalf("disable hook should not emit message, got %q", msg)
	case <-time.After(20 * time.Millisecond):
	}
}

type recordingLiveAgentHookInvoker struct {
	ch chan string
}

func newRecordingLiveAgentHookInvoker() *recordingLiveAgentHookInvoker {
	return &recordingLiveAgentHookInvoker{ch: make(chan string, 8)}
}

func (r *recordingLiveAgentHookInvoker) InvokeLiveAgentHook(ctx context.Context, message string) error {
	_ = ctx
	r.ch <- message
	return nil
}

func (r *recordingLiveAgentHookInvoker) wait(t *testing.T) string {
	t.Helper()
	select {
	case msg := <-r.ch:
		return msg
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for live agent hook message")
	}
	return ""
}

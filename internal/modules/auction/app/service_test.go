package app

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"aieas_backend/internal/domain"
	auctionports "aieas_backend/internal/modules/auction/ports"
	auctionrepo "aieas_backend/internal/modules/auction/repository"
)

type recordingAuctionEventPublisher struct {
	calls     int
	auctionID uint64
	env       auctionports.EventEnvelope
}

func (p *recordingAuctionEventPublisher) Broadcast(auctionID uint64, env auctionports.EventEnvelope) int {
	p.calls++
	p.auctionID = auctionID
	p.env = env
	return 1
}

func TestAuctionServiceCancelBroadcastsClosedEventWithLiveSessionID(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	liveSessionID := uint64(90001)
	auctionRepo := auctionrepo.NewMemoryAuctionRepository()
	publisher := &recordingAuctionEventPublisher{}
	auction := domain.AuctionLot{
		AuctionID:     91001,
		SellerID:      "u_2001",
		LiveSessionID: &liveSessionID,
		Title:         "cancelled lot",
		AuctionType:   domain.AuctionTypeEnglish,
		StartPrice:    1200,
		Status:        domain.AuctionStatusReady,
		StartTime:     now,
		EndTime:       now.Add(time.Hour),
	}
	if err := auctionRepo.Create(ctx, &auction); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	svc := NewAuctionServiceWithDeps(AuctionServiceDeps{
		Auctions:  auctionRepo,
		Publisher: publisher,
	})

	cancelled, err := svc.Cancel(ctx, auction.AuctionID, "u_2001", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("cancel auction: %v", err)
	}
	if cancelled.Status != domain.AuctionStatusClosedFailed {
		t.Fatalf("cancelled status=%s want %s", cancelled.Status, domain.AuctionStatusClosedFailed)
	}
	if publisher.calls != 2 || publisher.auctionID != auction.AuctionID || publisher.env.Type != "auction.closed" {
		t.Fatalf("unexpected broadcast: calls=%d auctionID=%d env=%+v", publisher.calls, publisher.auctionID, publisher.env)
	}
	var payload struct {
		AuctionID     uint64               `json:"auctionId"`
		LiveSessionID uint64               `json:"liveSessionId"`
		Status        domain.AuctionStatus `json:"status"`
		WinnerID      string               `json:"winnerId"`
		Price         int64                `json:"price"`
		ClosedAt      time.Time            `json:"closedAt"`
	}
	if err := json.Unmarshal(publisher.env.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload.AuctionID != auction.AuctionID || payload.LiveSessionID != liveSessionID || payload.Status != domain.AuctionStatusClosedFailed || payload.WinnerID != "" || payload.Price != auction.StartPrice || payload.ClosedAt.IsZero() {
		t.Fatalf("unexpected closed payload: %+v", payload)
	}
}

func TestAuctionServiceUpdateBroadcastsLiveSessionLotChanged(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	liveSessionID := uint64(90002)
	auctionRepo := auctionrepo.NewMemoryAuctionRepository()
	publisher := &recordingAuctionEventPublisher{}
	auction := domain.AuctionLot{
		AuctionID:      91002,
		SellerID:       "u_2001",
		LiveSessionID:  &liveSessionID,
		Title:          "old title",
		Description:    "old description",
		Category:       "collectible",
		ConditionGrade: domain.ConditionGood,
		AuctionType:    domain.AuctionTypeEnglish,
		StartPrice:     1200,
		Status:         domain.AuctionStatusReady,
		StartTime:      now,
		EndTime:        now.Add(time.Hour),
	}
	if err := auctionRepo.Create(ctx, &auction); err != nil {
		t.Fatalf("create auction: %v", err)
	}
	svc := NewAuctionServiceWithDeps(AuctionServiceDeps{
		Auctions:  auctionRepo,
		Publisher: publisher,
	})

	nextTitle := "new title"
	updated, err := svc.Update(ctx, auction.AuctionID, UpdateAuctionInput{ActorID: "u_2001", ActorRole: domain.RoleMerchant, Title: &nextTitle})
	if err != nil {
		t.Fatalf("update auction: %v", err)
	}
	if updated.Title != nextTitle {
		t.Fatalf("updated title=%q want %q", updated.Title, nextTitle)
	}
	if publisher.calls != 1 || publisher.auctionID != auction.AuctionID || publisher.env.Type != "live_session.lot_changed" {
		t.Fatalf("unexpected broadcast: calls=%d auctionID=%d env=%+v", publisher.calls, publisher.auctionID, publisher.env)
	}
	var payload struct {
		AuctionID     uint64 `json:"auctionId"`
		LiveSessionID uint64 `json:"liveSessionId"`
		MerchantID    string `json:"merchantId"`
		Action        string `json:"action"`
	}
	if err := json.Unmarshal(publisher.env.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload.AuctionID != auction.AuctionID || payload.LiveSessionID != liveSessionID || payload.MerchantID != "u_2001" || payload.Action != "updated" {
		t.Fatalf("unexpected changed payload: %+v", payload)
	}
}

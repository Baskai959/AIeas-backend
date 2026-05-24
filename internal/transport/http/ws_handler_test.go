package http

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
	"aieas_backend/internal/service"
	corews "aieas_backend/internal/transport/ws"

	"github.com/cloudwego/hertz/pkg/app"
)

func TestWSHandlerBidPlaceAckAndBroadcast(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	hub := corews.NewHub()
	bidSvc, auctionID := newWSBidFixture(t, cfg, hub)
	handler := NewWSHandler(hub, bidSvc, 8, 65536, time.Second, 2*time.Second)
	client := corews.NewClient("c1", "u_1001", auctionID, 8)
	if err := hub.Subscribe(auctionID, client); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	payload, _ := json.Marshal(map[string]interface{}{"price": 1100})
	responses := handler.handleInbound(ctx, client, corews.Envelope{Type: "bid.place", RequestID: "ws-bid-1", Payload: payload})
	if len(responses) != 1 || responses[0].Type != "bid.ack" {
		t.Fatalf("expected bid.ack, got %+v", responses)
	}
	var ack domain.BidResult
	if err := json.Unmarshal(responses[0].Payload, &ack); err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	if !ack.Accepted || ack.CurrentPrice != 1100 {
		t.Fatalf("unexpected bid ack: %+v", ack)
	}

	seenAccepted := false
	for i := 0; i < 3; i++ {
		select {
		case env := <-client.Outbound():
			if env.Type == "bid.accepted" {
				seenAccepted = true
			}
		default:
		}
	}
	if !seenAccepted {
		t.Fatal("expected bid.accepted broadcast")
	}
}

func TestWSHandlerParseLastSeq(t *testing.T) {
	req := httptest.NewRequest("GET", "/ws/auctions/10001?lastSeq=42", nil)
	c := app.NewContext(1)
	c.Request.SetRequestURI(req.URL.RequestURI())
	if got := parseLastSeq(c); got != 42 {
		t.Fatalf("expected lastSeq 42, got %d", got)
	}
}

func newWSBidFixture(t *testing.T, cfg appconfig.AuctionConfig, hub *corews.Hub) (*service.BidService, uint64) {
	t.Helper()
	ctx := context.Background()
	itemRepo := repository.NewMemoryItemRepository()
	auctionRepo := repository.NewMemoryAuctionRepository()
	bidRepo := repository.NewMemoryBidRepository()
	depositRepo := repository.NewMemoryDepositRepository()
	riskRepo := repository.NewMemoryRiskRepository()
	realtime := repository.NewMemoryRealtimeStore()
	riskSvc := service.NewRiskService(riskRepo, realtime, hub)
	depositSvc := service.NewDepositService(depositRepo, auctionRepo, realtime, riskSvc, repository.NoopTxManager{})
	auctionSvc := service.NewAuctionService(auctionRepo, itemRepo, repository.NoopTxManager{})
	auctionSvc.SetRealtime(realtime)
	auctionSvc.SetPublisher(hub)
	auctionSvc.SetAuctionConfig(cfg)
	bidSvc := service.NewBidService(bidRepo, auctionRepo, realtime, riskSvc, hub, cfg)

	item := domain.Item{SellerID: "u_2001", Title: "Watch", Category: "luxury", ConditionGrade: domain.ConditionNew, Status: domain.ItemStatusReady}
	if err := itemRepo.Create(ctx, &item); err != nil {
		t.Fatalf("create item: %v", err)
	}
	rule, _ := json.Marshal(map[string]interface{}{"type": "fixed", "amount": 100})
	auction, err := auctionSvc.Create(ctx, service.CreateAuctionInput{
		ActorID:        "u_2001",
		ActorRole:      domain.RoleMerchant,
		ItemID:         item.ID,
		AuctionType:    domain.AuctionTypeEnglish,
		StartPrice:     1000,
		ReservePrice:   1000,
		IncrementRule:  rule,
		AntiSnipingSec: 60,
		AntiExtendSec:  30,
		DepositAmount:  100,
		Status:         domain.AuctionStatusPendingAudit,
		StartTime:      time.Now().UTC().Add(-time.Minute),
		EndTime:        time.Now().UTC().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("create auction: %v", err)
	}
	auction.Status = domain.AuctionStatusReady
	if err := auctionRepo.Update(ctx, &auction); err != nil {
		t.Fatalf("approve auction: %v", err)
	}
	if _, err := auctionSvc.Start(ctx, auction.AuctionID, "u_2001", domain.RoleMerchant); err != nil {
		t.Fatalf("start auction: %v", err)
	}
	if _, err := depositSvc.Enroll(ctx, service.EnrollInput{AuctionID: auction.AuctionID, UserID: "u_1001", UserRole: domain.RoleBuyer}); err != nil {
		t.Fatalf("enroll: %v", err)
	}
	return bidSvc, auction.AuctionID
}

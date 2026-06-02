package http

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
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
	payload, _ := json.Marshal(map[string]interface{}{"price": 1100, "expectedCurrentPrice": 1000})
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

func TestWSHandlerBidPlaceRequiresExpectedState(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	hub := corews.NewHub()
	bidSvc, auctionID := newWSBidFixture(t, cfg, hub)
	handler := NewWSHandler(hub, bidSvc, 8, 65536, time.Second, 2*time.Second)
	client := corews.NewClient("c1", "u_1001", auctionID, 8)

	payload, _ := json.Marshal(map[string]interface{}{"price": 1100})
	responses := handler.handleInbound(ctx, client, corews.Envelope{Type: "bid.place", RequestID: "ws-bid-missing-state", Payload: payload})
	if len(responses) != 1 || responses[0].Type != "bid.ack" {
		t.Fatalf("expected bid.ack, got %+v", responses)
	}
	var ack struct {
		Accepted bool   `json:"accepted"`
		Reason   string `json:"reason"`
	}
	if err := json.Unmarshal(responses[0].Payload, &ack); err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	if ack.Accepted || ack.Reason != "MISSING_EXPECTED_STATE" {
		t.Fatalf("expected missing expected state rejection, got %+v", ack)
	}
}

func TestWSHandlerRejectedBidOnlyReturnsAck(t *testing.T) {
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
	for {
		select {
		case <-client.Outbound():
		default:
			goto drained
		}
	}

drained:
	payload, _ := json.Marshal(map[string]interface{}{"price": 1050, "expectedCurrentPrice": 1000})
	responses := handler.handleInbound(ctx, client, corews.Envelope{Type: "bid.place", RequestID: "ws-bid-reject", Payload: payload})
	if len(responses) != 1 || responses[0].Type != "bid.ack" {
		t.Fatalf("expected bid.ack, got %+v", responses)
	}
	var ack domain.BidResult
	if err := json.Unmarshal(responses[0].Payload, &ack); err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	if ack.Accepted || ack.Reason == "" {
		t.Fatalf("expected rejected ack, got %+v", ack)
	}
	select {
	case env := <-client.Outbound():
		t.Fatalf("rejected bid should not be broadcast to room, got %+v", env)
	default:
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

func TestWSHandlerSafeWriteRecoversPanic(t *testing.T) {
	err := writeJSONWithDeadline(panickingFrameWriter{}, corews.Envelope{Type: "room.online"})
	if err == nil || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("expected recovered panic error, got %v", err)
	}
}

func TestWSHandlerSafeReadRecoversPanic(t *testing.T) {
	_, _, err := readMessage(panickingFrameReader{})
	if err == nil || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("expected recovered read panic error, got %v", err)
	}
}

// TestWSHandlerDeliverRoomSnapshotFromRT 验证 P1-B：握手后 deliverRoomSnapshot
// 走 RT (auctionService.State 优先 RT)，下发的 envelope.type=room.snapshot，
// payload 含 currentPrice/leaderBidderId/endTime/seq/status/serverTime，且
// degraded=false（state.Source="redis"）。
func TestWSHandlerDeliverRoomSnapshotFromRT(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	hub := corews.NewHub()
	bidSvc, auctionSvc, auctionID := newWSBidFixtureWithAuctionService(t, cfg, hub)
	handler := NewWSHandler(hub, bidSvc, 8, 65536, time.Second, 2*time.Second)
	handler.SetAuctionService(auctionSvc)

	client := corews.NewClient("c1", "u_1001", auctionID, 8)
	if err := hub.Subscribe(auctionID, client); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	// Subscribe 后 Hub 会自动广播 room.online；先把它收掉，让后续 select 命中 snapshot。
	for {
		select {
		case env := <-client.Outbound():
			if env.Type == corews.TypeRoomSnapshot {
				t.Fatalf("snapshot delivered before deliverRoomSnapshot was called")
			}
			continue
		default:
		}
		break
	}

	before := time.Now().UTC().UnixMilli()
	handler.deliverRoomSnapshot(ctx, client)
	after := time.Now().UTC().UnixMilli()

	select {
	case env := <-client.Outbound():
		if env.Type != corews.TypeRoomSnapshot {
			t.Fatalf("expected room.snapshot, got %s", env.Type)
		}
		var payload map[string]interface{}
		if err := json.Unmarshal(env.Payload, &payload); err != nil {
			t.Fatalf("decode snapshot payload: %v", err)
		}
		for _, key := range []string{"auctionId", "status", "startPrice", "capPrice", "incrementRule", "currentPrice", "leaderBidderId", "endTime", "seq", "serverTime", "source", "degraded"} {
			if _, ok := payload[key]; !ok {
				t.Fatalf("snapshot payload missing %q: %+v", key, payload)
			}
		}
		rule, ok := payload["incrementRule"].(map[string]interface{})
		if !ok || rule["type"] != "fixed" {
			t.Fatalf("unexpected incrementRule in snapshot: %+v", payload["incrementRule"])
		}
		if degraded, _ := payload["degraded"].(bool); degraded {
			t.Fatalf("expected degraded=false from RT-served snapshot, got payload=%+v", payload)
		}
		if src, _ := payload["source"].(string); src != "redis" {
			t.Fatalf("expected source=redis, got %v", payload["source"])
		}
		serverTime, ok := payload["serverTime"].(float64)
		if !ok {
			t.Fatalf("serverTime should be number, got %T", payload["serverTime"])
		}
		if int64(serverTime) < before || int64(serverTime) > after+1 {
			t.Fatalf("serverTime=%v out of [%d,%d]", serverTime, before, after)
		}
	default:
		t.Fatalf("expected snapshot envelope on outbound, got none")
	}
}

// TestWSHandlerDeliverRoomSnapshotSkipsWhenNoAuctionService 验证未注入
// auctionService 时 deliverRoomSnapshot 是 noop（不阻塞、不下发任何帧）。
func TestWSHandlerDeliverRoomSnapshotSkipsWhenNoAuctionService(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	hub := corews.NewHub()
	bidSvc, auctionID := newWSBidFixture(t, cfg, hub)
	handler := NewWSHandler(hub, bidSvc, 8, 65536, time.Second, 2*time.Second)
	// 不调用 SetAuctionService。
	client := corews.NewClient("c2", "u_1001", auctionID, 8)
	handler.deliverRoomSnapshot(ctx, client)

	select {
	case env := <-client.Outbound():
		t.Fatalf("expected no snapshot frame when auctionService is unset, got %+v", env)
	default:
	}
}

func newWSBidFixture(t *testing.T, cfg appconfig.AuctionConfig, hub *corews.Hub) (*service.BidService, uint64) {
	bidSvc, _, auctionID := newWSBidFixtureWithAuctionService(t, cfg, hub)
	return bidSvc, auctionID
}

type panickingFrameWriter struct{}

func (panickingFrameWriter) SetWriteDeadline(time.Time) error {
	panic("nil hijack connection")
}

func (panickingFrameWriter) WriteJSON(v interface{}) error {
	return nil
}

func (panickingFrameWriter) WriteMessage(messageType int, data []byte) error {
	return nil
}

type panickingFrameReader struct{}

func (panickingFrameReader) ReadMessage() (int, []byte, error) {
	panic("nil hijack connection")
}

func newWSBidFixtureWithAuctionService(t *testing.T, cfg appconfig.AuctionConfig, hub *corews.Hub) (*service.BidService, *service.AuctionService, uint64) {
	t.Helper()
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	bidRepo := repository.NewMemoryBidRepository()
	depositRepo := repository.NewMemoryDepositRepository()
	riskRepo := repository.NewMemoryRiskRepository()
	realtime := repository.NewMemoryRealtimeStore()
	riskSvc := service.NewRiskService(riskRepo, realtime, hub)
	depositSvc := service.NewDepositService(depositRepo, auctionRepo, realtime, riskSvc, repository.NoopTxManager{})
	auctionSvc := service.NewAuctionService(auctionRepo, repository.NoopTxManager{})
	auctionSvc.SetRealtime(realtime)
	auctionSvc.SetPublisher(hub)
	auctionSvc.SetAuctionConfig(cfg)
	bidSvc := service.NewBidService(bidRepo, auctionRepo, realtime, riskSvc, hub, cfg)

	rule, _ := json.Marshal(map[string]interface{}{"type": "fixed", "amount": 100, "maxBidSteps": 10})
	auction, err := auctionSvc.Create(ctx, service.CreateAuctionInput{
		ActorID:        "u_2001",
		ActorRole:      domain.RoleMerchant,
		Title:          "Watch",
		Category:       "luxury",
		ConditionGrade: domain.ConditionNew,
		Description:    "rare watch",
		AuctionType:    domain.AuctionTypeEnglish,
		StartPrice:     1000,
		ReservePrice:   1000,
		CapPrice:       2000,
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
	return bidSvc, auctionSvc, auction.AuctionID
}

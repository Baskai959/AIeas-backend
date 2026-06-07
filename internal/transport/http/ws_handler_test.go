package http

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	auctionapp "aieas_backend/internal/modules/auction/app"
	auctionports "aieas_backend/internal/modules/auction/ports"
	depositapp "aieas_backend/internal/modules/deposit/app"
	riskapp "aieas_backend/internal/modules/risk/app"
	"aieas_backend/internal/tests/repository"
	corews "aieas_backend/internal/transport/ws"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/hertz-contrib/websocket"
)

func TestWSHandlerBidPlaceAckAndBroadcast(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	hub := corews.NewHub()
	bidSvc, auctionID := newWSBidFixture(t, cfg, hub)
	handler := NewWSHandler(hub, bidSvc, 8, 65536, time.Second, 2*time.Second, 5*time.Second, 0, time.Second, nil, nil)
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

func TestWSHandlerBidPlaceAcceptsStringAuctionID(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	hub := corews.NewHub()
	bidSvc, auctionID := newWSBidFixture(t, cfg, hub)
	handler := NewWSHandler(hub, bidSvc, 8, 65536, time.Second, 2*time.Second, 5*time.Second, 0, time.Second, nil, nil)
	client := corews.NewClient("c1", "u_1001", 0, 8)
	if err := hub.Subscribe(auctionID, client); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	payload, _ := json.Marshal(map[string]interface{}{"auctionId": strconv.FormatUint(auctionID, 10), "price": 1100, "expectedCurrentPrice": 1000})
	responses := handler.handleInbound(ctx, client, corews.Envelope{Type: "bid.place", RequestID: "ws-bid-string-id", Payload: payload})
	if len(responses) != 1 || responses[0].Type != "bid.ack" {
		t.Fatalf("expected bid.ack, got %+v", responses)
	}
	var ack domain.BidResult
	if err := json.Unmarshal(responses[0].Payload, &ack); err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	if !ack.Accepted || ack.AuctionID != auctionID || ack.CurrentPrice != 1100 {
		t.Fatalf("unexpected bid ack: %+v", ack)
	}
}

func TestWSHandlerBidPlaceRequiresExpectedState(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	hub := corews.NewHub()
	bidSvc, auctionID := newWSBidFixture(t, cfg, hub)
	handler := NewWSHandler(hub, bidSvc, 8, 65536, time.Second, 2*time.Second, 5*time.Second, 0, time.Second, nil, nil)
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

func TestWSHandlerBidPlaceDisabledRequiresAPIAndDoesNotCallLocalService(t *testing.T) {
	handler := NewWSHandler(corews.NewHub(), nil, 8, 65536, time.Second, 2*time.Second, 5*time.Second, 0, time.Second, nil, nil)
	handler.SetBidPlaceMode(WSBidPlaceDisabled)
	client := corews.NewClient("c1", "u_1001", 10001, 8)
	payload, _ := json.Marshal(map[string]interface{}{"price": 1100, "expectedCurrentPrice": 1000})

	responses := handler.handleInbound(context.Background(), client, corews.Envelope{Type: "bid.place", RequestID: "ws-bid-disabled", Payload: payload})
	if len(responses) != 1 || responses[0].Type != "bid.ack" || responses[0].RequestID != "ws-bid-disabled" {
		t.Fatalf("expected bid.ack with original request id, got %+v", responses)
	}
	var ack struct {
		Accepted bool   `json:"accepted"`
		Reason   string `json:"reason"`
	}
	if err := json.Unmarshal(responses[0].Payload, &ack); err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	if ack.Accepted || ack.Reason != "BID_THROUGH_API_REQUIRED" {
		t.Fatalf("unexpected disabled bid ack: %+v", ack)
	}
}

func TestWSHandlerRejectedBidOnlyReturnsAck(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	hub := corews.NewHub()
	bidSvc, auctionID := newWSBidFixture(t, cfg, hub)
	handler := NewWSHandler(hub, bidSvc, 8, 65536, time.Second, 2*time.Second, 5*time.Second, 0, time.Second, nil, nil)
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

func TestWSHandlerChatSendAckAndBroadcast(t *testing.T) {
	hub := corews.NewHub()
	handler := NewWSHandler(hub, nil, 8, 65536, time.Second, 2*time.Second, 5*time.Second, 0, time.Second, nil, nil)
	sender := corews.NewClientWithSession("c1", "u_1001", 0, 9001, 8)
	receiver := corews.NewClientWithSession("c2", "u_1002", 0, 9001, 8)
	if err := hub.SubscribeLiveSessionOnly(9001, sender); err != nil {
		t.Fatalf("subscribe sender: %v", err)
	}
	if err := hub.SubscribeLiveSessionOnly(9001, receiver); err != nil {
		t.Fatalf("subscribe receiver: %v", err)
	}
	for {
		select {
		case <-sender.Outbound():
		case <-receiver.Outbound():
		default:
			goto onlineDrained
		}
	}

onlineDrained:
	payload, _ := json.Marshal(map[string]interface{}{"roomId": "9001", "content": "这件很漂亮", "clientMessageId": "client-chat-1"})

	responses := handler.handleInbound(context.Background(), sender, corews.Envelope{Type: "chat.send", RequestID: "chat-1", Payload: payload})
	if len(responses) != 1 || responses[0].Type != "chat.ack" || responses[0].RequestID != "chat-1" {
		t.Fatalf("expected chat.ack, got %+v", responses)
	}
	var ack struct {
		RoomID          string `json:"roomId"`
		ClientMessageID string `json:"clientMessageId"`
		MessageID       string `json:"messageId"`
		CreatedAt       string `json:"createdAt"`
	}
	if err := json.Unmarshal(responses[0].Payload, &ack); err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	if ack.RoomID != "9001" || ack.ClientMessageID != "client-chat-1" || ack.MessageID == "" || ack.CreatedAt == "" {
		t.Fatalf("unexpected chat ack: %+v", ack)
	}

	select {
	case env := <-receiver.Outbound():
		if env.Type != "chat.message" || env.RequestID != "chat-1" || env.LiveSessionID != 9001 {
			t.Fatalf("expected chat.message broadcast, got %+v", env)
		}
		var message struct {
			ID              string `json:"id"`
			RoomID          string `json:"roomId"`
			UserID          string `json:"userId"`
			Nickname        string `json:"nickname"`
			Content         string `json:"content"`
			ClientMessageID string `json:"clientMessageId"`
			CreatedAt       string `json:"createdAt"`
		}
		if err := json.Unmarshal(env.Payload, &message); err != nil {
			t.Fatalf("decode chat message: %v", err)
		}
		if message.RoomID != "9001" || message.UserID != "u_1001" || message.Nickname != "u_1001" || message.Content != "这件很漂亮" || message.ClientMessageID != "client-chat-1" || message.ID != ack.MessageID || message.CreatedAt != ack.CreatedAt {
			t.Fatalf("unexpected chat message: %+v", message)
		}
	case <-time.After(time.Second):
		t.Fatal("expected chat.message broadcast")
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

func TestWSHandlerAuctionRejectsDrainingBeforeUpgradeAndRecordsMetric(t *testing.T) {
	hub := corews.NewHub()
	metrics := newFakeWSHandlerMetrics()
	hub.BeginDrain(5000)
	handler := NewWSHandler(hub, nil, 8, 65536, time.Second, 2*time.Second, 5*time.Second, 0, time.Second, nil, metrics)
	h := server.Default()
	h.GET("/ws/auctions/:auction_id", handler.Auction)

	resp := ut.PerformRequest(h.Engine, consts.MethodGet, "/ws/auctions/10001", nil)
	if resp.Code != consts.StatusServiceUnavailable {
		t.Fatalf("expected draining handshake rejection 503 before upgrade, got %d body=%s", resp.Code, resp.Body.String())
	}
	if got := string(resp.Header().Get("Retry-After")); got != "5" {
		t.Fatalf("expected Retry-After=5, got %q", got)
	}
	if got := metrics.handshakeRejects("draining"); got != 1 {
		t.Fatalf("expected one draining handshake reject metric, got %d", got)
	}
}

func TestWSHandlerAuctionRejectsLimiterBeforeUpgradeAndRecordsReason(t *testing.T) {
	hub := corews.NewHub()
	metrics := newFakeWSHandlerMetrics()
	limiter := corews.NewHandshakeLimiter(0, 0, 1)
	handler := NewWSHandler(hub, nil, 8, 65536, time.Second, 2*time.Second, 5*time.Second, 0, time.Second, limiter, metrics)
	h := server.Default()
	h.GET("/ws/auctions/:auction_id", handler.Auction)

	first := ut.PerformRequest(h.Engine, consts.MethodGet, "/ws/auctions/10001", nil)
	if first.Code == consts.StatusTooManyRequests {
		t.Fatalf("first handshake should consume the auction token instead of being limited, body=%s", first.Body.String())
	}
	second := ut.PerformRequest(h.Engine, consts.MethodGet, "/ws/auctions/10001", nil)
	if second.Code != consts.StatusTooManyRequests {
		t.Fatalf("expected rate-limited handshake rejection 429 before upgrade, got %d body=%s", second.Code, second.Body.String())
	}
	if got := string(second.Header().Get("Retry-After")); got != "60" {
		t.Fatalf("expected Retry-After=60, got %q", got)
	}
	if got := metrics.handshakeRejects("rate_limit_auction"); got != 1 {
		t.Fatalf("expected one rate_limit_auction handshake reject metric, got %d", got)
	}
}

func TestWSHandlerConfigurableWriteTimeout(t *testing.T) {
	customTimeout := 250 * time.Millisecond
	handler := NewWSHandler(corews.NewHub(), nil, 8, 65536, time.Second, 2*time.Second, customTimeout, 0, time.Second, nil, nil)
	if handler.writeTimeout != customTimeout {
		t.Fatalf("expected handler write timeout %s, got %s", customTimeout, handler.writeTimeout)
	}

	before := time.Now()
	writer := &capturingFrameWriter{}
	if err := writeJSONWithDeadline(writer, corews.Envelope{Type: "room.online"}, customTimeout); err != nil {
		t.Fatalf("write json with deadline: %v", err)
	}
	after := time.Now()
	if writer.deadline.Before(before.Add(customTimeout)) || writer.deadline.After(after.Add(customTimeout+50*time.Millisecond)) {
		t.Fatalf("deadline %s not based on custom timeout %s in [%s,%s]", writer.deadline, customTimeout, before, after)
	}

	fallback := NewWSHandler(corews.NewHub(), nil, 8, 65536, time.Second, 2*time.Second, 0, 0, time.Second, nil, nil)
	if fallback.writeTimeout != defaultWebsocketWriteTimeout {
		t.Fatalf("expected fallback write timeout %s, got %s", defaultWebsocketWriteTimeout, fallback.writeTimeout)
	}
}

func TestWSHandlerInitialPingJitterDeterministicAndBounded(t *testing.T) {
	if got := (&WSHandler{}).initialPingJitterAt("client-1", time.Unix(100, 0)); got != 0 {
		t.Fatalf("expected disabled ping jitter to be zero, got %s", got)
	}

	handler := &WSHandler{pingJitter: 5 * time.Second}
	now := time.Unix(100, 123)
	got := handler.initialPingJitterAt("client-1", now)
	if got < 0 || got >= handler.pingJitter {
		t.Fatalf("expected jitter in [0,%s), got %s", handler.pingJitter, got)
	}
	if again := handler.initialPingJitterAt("client-1", now); again != got {
		t.Fatalf("expected deterministic jitter for same client and time, got %s then %s", got, again)
	}
	if other := handler.initialPingJitterAt("client-2", now); other < 0 || other >= handler.pingJitter {
		t.Fatalf("expected other client jitter in [0,%s), got %s", handler.pingJitter, other)
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

func TestWSHandlerCloseCodeForReason(t *testing.T) {
	cases := map[string]int{
		"":                 1000,
		"closed":           1000,
		"unsubscribe":      1000,
		"read_closed":      1000,
		"write_closed":     1000,
		"slow_consumer":    1008,
		"pong_timeout":     1001,
		"gateway_draining": 1001,
		"boom":             1011,
	}
	for reason, want := range cases {
		if got := closeCodeForReason(reason); got != want {
			t.Fatalf("closeCodeForReason(%q)=%d want %d", reason, got, want)
		}
	}
}

func TestWSHandlerReadCloseReasonClassifiesTimeout(t *testing.T) {
	if got := readCloseReason(fakeTimeoutError{}); got != "pong_timeout" {
		t.Fatalf("expected pong_timeout, got %q", got)
	}
	if got := readCloseReason(errors.New("eof")); got != "read_closed" {
		t.Fatalf("expected read_closed, got %q", got)
	}
	if got := readCloseReason(wrappedNetError{err: fakeTimeoutError{}}); got != "pong_timeout" {
		t.Fatalf("expected wrapped timeout as pong_timeout, got %q", got)
	}
}

type fakeTimeoutError struct{}

func (fakeTimeoutError) Error() string   { return "timeout" }
func (fakeTimeoutError) Timeout() bool   { return true }
func (fakeTimeoutError) Temporary() bool { return true }

var _ net.Error = fakeTimeoutError{}

type wrappedNetError struct{ err error }

func (w wrappedNetError) Error() string { return "wrapped: " + w.err.Error() }
func (w wrappedNetError) Unwrap() error { return w.err }

// TestWSHandlerDeliverRoomSnapshotFromRT 验证 P1-B：握手后 deliverRoomSnapshot
// 走 RT (auctionService.State 优先 RT)，下发的 envelope.type=room.snapshot，
// payload 含 currentPrice/leaderBidderId/participantCount/endTime/seq/status/serverTime，且
// degraded=false（state.Source="redis"）。
func TestWSHandlerDeliverRoomSnapshotFromRT(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	hub := corews.NewHub()
	bidSvc, auctionSvc, auctionID := newWSBidFixtureWithAuctionService(t, cfg, hub)
	handler := NewWSHandler(hub, bidSvc, 8, 65536, time.Second, 2*time.Second, 5*time.Second, 0, time.Second, nil, nil)
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
		for _, key := range []string{"auctionId", "status", "startPrice", "capPrice", "incrementRule", "currentPrice", "leaderBidderId", "participantCount", "endTime", "seq", "serverTime", "source", "degraded"} {
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

func TestWSHandlerDeliverRoomSnapshotRealtimeOnlyHitAndMiss(t *testing.T) {
	ctx := context.Background()
	store := repository.NewMemoryRealtimeStore()
	hub := corews.NewHub()
	handler := NewWSHandler(hub, nil, 8, 65536, time.Second, 2*time.Second, 5*time.Second, 0, time.Second, nil, nil)
	handler.SetRealtimeSnapshotProvider(store)
	handler.SetAllowDBSnapshotFallback(false)

	now := time.Now().UTC()
	state, err := store.InitAuction(ctx, domain.AuctionLot{AuctionID: 30001, Status: domain.AuctionStatusRunning, StartPrice: 1000, CapPrice: 2000, StartTime: now, EndTime: now.Add(time.Hour), IncrementRule: domain.DefaultIncrementRule()}, 100)
	if err != nil || state.AuctionID == 0 {
		t.Fatalf("init realtime state: state=%+v err=%v", state, err)
	}
	client := corews.NewClient("c-hit", "u_1001", 30001, 8)
	handler.deliverRoomSnapshot(ctx, client)
	select {
	case env := <-client.Outbound():
		if env.Type != corews.TypeRoomSnapshot {
			t.Fatalf("expected room.snapshot hit, got %+v", env)
		}
	default:
		t.Fatal("expected snapshot from realtime hit")
	}

	missClient := corews.NewClient("c-miss", "u_1001", 30002, 8)
	handler.deliverRoomSnapshot(ctx, missClient)
	select {
	case env := <-missClient.Outbound():
		if env.Type != "room.snapshot_required" {
			t.Fatalf("expected snapshot_required miss, got %+v", env)
		}
		var payload struct {
			AuctionID  uint64 `json:"auctionId"`
			Reason     string `json:"reason"`
			ServerTime int64  `json:"serverTime"`
		}
		if err := json.Unmarshal(env.Payload, &payload); err != nil {
			t.Fatalf("decode snapshot_required: %v", err)
		}
		if payload.AuctionID != 30002 || payload.Reason == "" || payload.ServerTime == 0 {
			t.Fatalf("unexpected snapshot_required payload: %+v", payload)
		}
	default:
		t.Fatal("expected snapshot_required from realtime miss")
	}
}

func TestWSHandlerDeliverInitialRankingForRunningAuction(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	hub := corews.NewHub()
	bidSvc, auctionSvc, auctionID := newWSBidFixtureWithAuctionService(t, cfg, hub)
	handler := NewWSHandler(hub, bidSvc, 8, 65536, time.Second, 2*time.Second, 5*time.Second, 0, time.Second, nil, nil)
	handler.SetAuctionService(auctionSvc)
	handler.SetAuctionRankingService(bidSvc)

	expectedCurrentPrice := int64(1000)
	result, err := bidSvc.Place(ctx, auctionapp.PlaceBidInput{
		RequestID:            "initial-ranking-bid-1",
		AuctionID:            auctionID,
		BidderID:             "u_1001",
		UserRole:             domain.RoleBuyer,
		Price:                1100,
		ExpectedCurrentPrice: &expectedCurrentPrice,
		Source:               "test",
	})
	if err != nil || !result.Accepted {
		t.Fatalf("seed accepted bid: result=%+v err=%v", result, err)
	}

	client := corews.NewClientWithSession("c-ranking", "u_1002", auctionID, 9001, 8)
	handler.deliverInitialRanking(ctx, client)

	select {
	case env := <-client.Outbound():
		if env.Type != corews.TypeRankingUpdated {
			t.Fatalf("expected ranking.updated, got %s", env.Type)
		}
		if env.LiveSessionID != 9001 {
			t.Fatalf("expected liveSessionId on envelope, got %d", env.LiveSessionID)
		}
		var payload struct {
			AuctionID     uint64                `json:"auctionId"`
			LiveSessionID uint64                `json:"liveSessionId"`
			Ranking       []domain.RankingEntry `json:"ranking"`
		}
		if err := json.Unmarshal(env.Payload, &payload); err != nil {
			t.Fatalf("decode ranking payload: %v", err)
		}
		if payload.AuctionID != auctionID || payload.LiveSessionID != 9001 {
			t.Fatalf("unexpected ranking payload scope: %+v", payload)
		}
		if len(payload.Ranking) != 1 || payload.Ranking[0].Rank != 1 || payload.Ranking[0].BidderID != "u_1001" || payload.Ranking[0].Price != 1100 {
			t.Fatalf("unexpected initial ranking: %+v", payload.Ranking)
		}
	default:
		t.Fatal("expected initial ranking envelope")
	}
}

func TestWSHandlerDeliverInitialRankingForRunningAuctionWithNoBids(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	cfg.FreqLimitCount = 100
	hub := corews.NewHub()
	bidSvc, auctionSvc, auctionID := newWSBidFixtureWithAuctionService(t, cfg, hub)
	handler := NewWSHandler(hub, bidSvc, 8, 65536, time.Second, 2*time.Second, 5*time.Second, 0, time.Second, nil, nil)
	handler.SetAuctionService(auctionSvc)
	handler.SetAuctionRankingService(bidSvc)

	client := corews.NewClientWithSession("c-empty-ranking", "u_1002", auctionID, 9001, 8)
	handler.deliverInitialRanking(ctx, client)

	select {
	case env := <-client.Outbound():
		if env.Type != corews.TypeRankingUpdated {
			t.Fatalf("expected ranking.updated, got %s", env.Type)
		}
		var payload struct {
			Ranking []domain.RankingEntry `json:"ranking"`
		}
		if err := json.Unmarshal(env.Payload, &payload); err != nil {
			t.Fatalf("decode ranking payload: %v", err)
		}
		if len(payload.Ranking) != 0 {
			t.Fatalf("expected empty initial ranking, got %+v", payload.Ranking)
		}
	default:
		t.Fatal("expected empty initial ranking envelope")
	}
}

// TestWSHandlerDeliverRoomSnapshotSkipsWhenNoAuctionService 验证未注入
// auctionService 时 deliverRoomSnapshot 是 noop（不阻塞、不下发任何帧）。
func TestWSHandlerDeliverRoomSnapshotSkipsWhenNoAuctionService(t *testing.T) {
	ctx := context.Background()
	cfg := appconfig.Default().Auction
	hub := corews.NewHub()
	bidSvc, auctionID := newWSBidFixture(t, cfg, hub)
	handler := NewWSHandler(hub, bidSvc, 8, 65536, time.Second, 2*time.Second, 5*time.Second, 0, time.Second, nil, nil)
	// 不调用 SetAuctionService。
	client := corews.NewClient("c2", "u_1001", auctionID, 8)
	handler.deliverRoomSnapshot(ctx, client)

	select {
	case env := <-client.Outbound():
		t.Fatalf("expected no snapshot frame when auctionService is unset, got %+v", env)
	default:
	}
}

func newWSBidFixture(t *testing.T, cfg appconfig.AuctionConfig, hub *corews.Hub) (*auctionapp.BidService, uint64) {
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

type capturingFrameWriter struct {
	deadline time.Time
}

func (w *capturingFrameWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadline = deadline
	return nil
}

func (w *capturingFrameWriter) WriteJSON(v interface{}) error {
	return nil
}

func (w *capturingFrameWriter) WriteMessage(messageType int, data []byte) error {
	if messageType != websocket.PingMessage {
		return errors.New("unexpected message type")
	}
	return nil
}

type fakeWSHandlerMetrics struct {
	mu      sync.Mutex
	rejects map[string]int
}

func newFakeWSHandlerMetrics() *fakeWSHandlerMetrics {
	return &fakeWSHandlerMetrics{rejects: make(map[string]int)}
}

func (f *fakeWSHandlerMetrics) IncWSConnect() {}

func (f *fakeWSHandlerMetrics) IncWSDisconnect(reason string) {}

func (f *fakeWSHandlerMetrics) ObserveWSBroadcast(elapsed time.Duration, fanout int) {}

func (f *fakeWSHandlerMetrics) IncWSSlowClientDisconnect() {}

func (f *fakeWSHandlerMetrics) IncWSHandshakeReject(reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rejects[reason]++
}

func (f *fakeWSHandlerMetrics) IncWSDraining() {}

func (f *fakeWSHandlerMetrics) handshakeRejects(reason string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rejects[reason]
}

type wsAuctionEventPublisherAdapter struct {
	hub *corews.Hub
}

func (a wsAuctionEventPublisherAdapter) Broadcast(auctionID uint64, env auctionports.EventEnvelope) int {
	if a.hub == nil {
		return 0
	}
	return a.hub.Broadcast(auctionID, corews.Envelope{
		Type:      env.Type,
		RequestID: env.RequestID,
		Seq:       env.Seq,
		Payload:   env.Payload,
	})
}

func newWSBidFixtureWithAuctionService(t *testing.T, cfg appconfig.AuctionConfig, hub *corews.Hub) (*auctionapp.BidService, *auctionapp.AuctionService, uint64) {
	t.Helper()
	ctx := context.Background()
	auctionRepo := repository.NewMemoryAuctionRepository()
	bidRepo := repository.NewMemoryBidRepository()
	depositRepo := repository.NewMemoryDepositRepository()
	riskRepo := repository.NewMemoryRiskRepository()
	realtime := repository.NewMemoryRealtimeStore()
	riskSvc := riskapp.NewRiskService(riskRepo, nil)
	depositSvc := depositapp.NewDepositService(depositRepo, auctionRepo, realtime, riskSvc, repository.NoopTxManager{})
	publisher := wsAuctionEventPublisherAdapter{hub: hub}
	auctionSvc := auctionapp.NewAuctionServiceWithDeps(auctionapp.AuctionServiceDeps{Auctions: auctionRepo, Tx: repository.NoopTxManager{}, Realtime: realtime, Publisher: publisher, AuctionConfig: cfg})
	bidSvc := auctionapp.NewBidServiceWithDeps(auctionapp.BidServiceDeps{Bids: bidRepo, Auctions: auctionRepo, Realtime: realtime, Risk: riskSvc, Publisher: publisher, Config: cfg})

	rule, _ := json.Marshal(map[string]interface{}{"type": "fixed", "amount": 100, "maxBidSteps": 10})
	auction, err := auctionSvc.Create(ctx, auctionapp.CreateAuctionInput{
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
	if _, err := depositSvc.Enroll(ctx, depositapp.EnrollInput{AuctionID: auction.AuctionID, UserID: "u_1001", UserRole: domain.RoleBuyer}); err != nil {
		t.Fatalf("enroll: %v", err)
	}
	return bidSvc, auctionSvc, auction.AuctionID
}

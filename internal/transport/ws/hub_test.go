package ws

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestHubBroadcastDoesNotBlockOnSlowConsumer(t *testing.T) {
	hub := NewHub()
	fast := NewClient("fast", "u1", 10001, 1)
	slow := NewClient("slow", "u2", 10001, 1)
	if err := hub.Subscribe(10001, fast); err != nil {
		t.Fatalf("subscribe fast: %v", err)
	}
	drainPresence(t, fast)
	if err := hub.Subscribe(10001, slow); err != nil {
		t.Fatalf("subscribe slow: %v", err)
	}
	drainPresence(t, fast, slow)
	slow.Deliver(Envelope{Type: "prefill"})

	delivered := hub.Broadcast(10001, Envelope{Type: "announcement"})
	if delivered != 1 {
		t.Fatalf("expected only fast client delivery, got %d", delivered)
	}
	if slow.Dropped() != 1 {
		t.Fatalf("expected slow client dropped count 1, got %d", slow.Dropped())
	}
	if !slow.Closed() || slow.CloseReason() != "slow_consumer" {
		t.Fatalf("expected slow client closed by slow consumer policy, closed=%v reason=%q", slow.Closed(), slow.CloseReason())
	}
	select {
	case env := <-fast.Outbound():
		if env.Type != "announcement" || env.Seq == 0 {
			t.Fatalf("unexpected fast envelope: %+v", env)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for fast client envelope")
	}
}

func TestHubReplaySinceAndSnapshotGap(t *testing.T) {
	hub := NewHubWithEventWindow(2)
	client := NewClient("c1", "u1", 20001, 8)
	if err := hub.Subscribe(20001, client); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	drainPresence(t, client)
	hub.Broadcast(20001, Envelope{Type: "e1"})
	hub.Broadcast(20001, Envelope{Type: "e2"})
	hub.Broadcast(20001, Envelope{Type: "e3"})

	missed, complete := hub.ReplaySince(20001, 3)
	if !complete || len(missed) != 1 || missed[0].Type != "e3" {
		t.Fatalf("expected replay of e3, complete=%v missed=%+v", complete, missed)
	}
	_, complete = hub.ReplaySince(20001, 1)
	if complete {
		t.Fatal("expected incomplete replay when lastSeq is outside event window")
	}
}

func TestHubDeduplicatesBidEventsByPubSeq(t *testing.T) {
	hub := NewHub()
	client := NewClient("c1", "u1", 21001, 4)
	if err := hub.Subscribe(21001, client); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	drainPresence(t, client)

	env := Envelope{Type: "bid.accepted", Seq: 9, Payload: []byte(`{"seq":9}`)}
	if delivered := hub.Broadcast(21001, env); delivered != 1 {
		t.Fatalf("expected first delivery, got %d", delivered)
	}
	select {
	case got := <-client.Outbound():
		if got.Seq != 9 || got.Type != "bid.accepted" {
			t.Fatalf("unexpected envelope: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first bid event")
	}
	if delivered := hub.Broadcast(21001, env); delivered != 0 {
		t.Fatalf("expected duplicate seq to be skipped, got %d", delivered)
	}
	select {
	case dup := <-client.Outbound():
		t.Fatalf("unexpected duplicate delivery: %+v", dup)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestHubOnlineCountUpdatesOnSubscribeUnsubscribeAndSlowConsumer(t *testing.T) {
	hub := NewHub()
	c1 := NewClient("c1", "u1", 30001, 4)
	c2 := NewClient("c2", "u2", 30001, 1)
	if err := hub.Subscribe(30001, c1); err != nil {
		t.Fatalf("subscribe c1: %v", err)
	}
	drainPresence(t, c1)
	if hub.OnlineCount(30001) != 1 {
		t.Fatalf("expected online count 1, got %d", hub.OnlineCount(30001))
	}
	if err := hub.Subscribe(30001, c2); err != nil {
		t.Fatalf("subscribe c2: %v", err)
	}
	if hub.OnlineCount(30001) != 2 {
		t.Fatalf("expected online count 2, got %d", hub.OnlineCount(30001))
	}
	drainPresence(t, c1, c2)
	c2.Deliver(Envelope{Type: "prefill"})
	hub.Broadcast(30001, Envelope{Type: "announcement"})
	if hub.OnlineCount(30001) != 1 {
		t.Fatalf("expected slow consumer removed from online count, got %d", hub.OnlineCount(30001))
	}
	hub.Unsubscribe(30001, c1.ID)
	if hub.OnlineCount(30001) != 0 {
		t.Fatalf("expected online count 0 after unsubscribe, got %d", hub.OnlineCount(30001))
	}
}

func TestHubSharedOnlineCounterAggregatesMultipleInstances(t *testing.T) {
	counter := NewMemoryOnlineCounter()
	hub1 := NewHubWithOnlineCounter(counter)
	hub2 := NewHubWithOnlineCounter(counter)
	c1 := NewClient("c1", "u1", 31001, 8)
	c2 := NewClient("c2", "u2", 31001, 8)
	if err := hub1.Subscribe(31001, c1); err != nil {
		t.Fatalf("subscribe c1: %v", err)
	}
	if err := hub2.Subscribe(31001, c2); err != nil {
		t.Fatalf("subscribe c2: %v", err)
	}
	if got := hub1.OnlineCount(31001); got != 2 {
		t.Fatalf("hub1 expected shared online count 2, got %d", got)
	}
	if got := hub2.OnlineCount(31001); got != 2 {
		t.Fatalf("hub2 expected shared online count 2, got %d", got)
	}
	if online := lastPresenceOnline(t, c2); online != 2 {
		t.Fatalf("expected presence payload online 2, got %d", online)
	}

	hub1.Unsubscribe(31001, c1.ID)
	if got := hub2.OnlineCount(31001); got != 1 {
		t.Fatalf("expected shared online count 1 after c1 leaves, got %d", got)
	}
	hub1.Unsubscribe(31001, c1.ID)
	if got := hub2.OnlineCount(31001); got != 1 {
		t.Fatalf("expected duplicate unsubscribe not to decrement below 1, got %d", got)
	}
	hub2.Unsubscribe(31001, c2.ID)
	if got := hub1.OnlineCount(31001); got != 0 {
		t.Fatalf("expected shared online count 0 after all leave, got %d", got)
	}
}

func TestHubSharedOnlineCounterRemovesSlowConsumerOnce(t *testing.T) {
	counter := NewMemoryOnlineCounter()
	hub1 := NewHubWithOnlineCounter(counter)
	hub2 := NewHubWithOnlineCounter(counter)
	fast := NewClient("fast", "u1", 32001, 8)
	slow := NewClient("slow", "u2", 32001, 1)
	if err := hub1.Subscribe(32001, fast); err != nil {
		t.Fatalf("subscribe fast: %v", err)
	}
	if err := hub2.Subscribe(32001, slow); err != nil {
		t.Fatalf("subscribe slow: %v", err)
	}
	drainPresence(t, fast, slow)
	slow.Deliver(Envelope{Type: "prefill"})
	hub2.Broadcast(32001, Envelope{Type: "announcement"})
	if got := hub1.OnlineCount(32001); got != 1 {
		t.Fatalf("expected slow consumer removed from shared count once, got %d", got)
	}
	hub2.Unsubscribe(32001, slow.ID)
	if got := hub1.OnlineCount(32001); got != 1 {
		t.Fatalf("expected duplicate slow-consumer removal not to decrement again, got %d", got)
	}
}

func TestHubOnlineCounterFallsBackToLocalCount(t *testing.T) {
	hub := NewHubWithOnlineCounter(failingOnlineCounter{})
	c1 := NewClient("c1", "u1", 33001, 4)
	if err := hub.Subscribe(33001, c1); err != nil {
		t.Fatalf("subscribe c1: %v", err)
	}
	if got := hub.OnlineCount(33001); got != 1 {
		t.Fatalf("expected local fallback online count 1, got %d", got)
	}
	hub.Unsubscribe(33001, c1.ID)
	hub.Unsubscribe(33001, c1.ID)
	if got := hub.OnlineCount(33001); got != 0 {
		t.Fatalf("expected local fallback online count 0, got %d", got)
	}
}

func TestHubHandleInboundAckAndPing(t *testing.T) {
	hub := NewHub()
	client := NewClient("c1", "u1", 10001, 4)
	if err := hub.Subscribe(10001, client); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	responses := hub.HandleInbound(context.Background(), client, Envelope{Type: "ping", RequestID: "req-1", Seq: 7})
	if len(responses) != 2 {
		t.Fatalf("expected ack and pong, got %+v", responses)
	}
	if responses[0].Type != "ack" || !responses[0].Ack || responses[0].RequestID != "req-1" {
		t.Fatalf("unexpected ack: %+v", responses[0])
	}
	if responses[1].Type != "pong" || responses[1].Seq != 7 {
		t.Fatalf("unexpected pong: %+v", responses[1])
	}
}

func TestHubHandleInboundRoomAliasesAndHeartbeat(t *testing.T) {
	hub := NewHub()
	client := NewClient("c1", "u1", 10001, 4)
	responses := hub.HandleInbound(context.Background(), client, Envelope{Type: "room.subscribe", RequestID: "sub-1"})
	if len(responses) != 2 || responses[1].Type != "room.subscribed" {
		t.Fatalf("expected room.subscribed response, got %+v", responses)
	}
	if room, ok := hub.Room(10001); !ok || room.ClientCount() != 1 {
		t.Fatalf("expected subscribed room, room=%+v ok=%v", room, ok)
	}
	heartbeat := hub.HandleInbound(context.Background(), client, Envelope{Type: "heartbeat", RequestID: "hb-1"})
	if len(heartbeat) != 2 || heartbeat[1].Type != "heartbeat.ack" {
		t.Fatalf("expected heartbeat ack, got %+v", heartbeat)
	}
	unsub := hub.HandleInbound(context.Background(), client, Envelope{Type: "room.unsubscribe", RequestID: "unsub-1"})
	if len(unsub) != 2 || unsub[1].Type != "room.unsubscribed" {
		t.Fatalf("expected room.unsubscribed response, got %+v", unsub)
	}
}

func TestHubBroadcastSessionEndDeliversAndCleansSessionIndex(t *testing.T) {
	hub := NewHub()
	const sessionID uint64 = 9001
	c1 := NewClientWithSession("c1", "u1", 40001, sessionID, 8)
	c2 := NewClientWithSession("c2", "u2", 40002, sessionID, 8)
	other := NewClientWithSession("c3", "u3", 40001, 9999, 8)
	if err := hub.Subscribe(40001, c1); err != nil {
		t.Fatalf("subscribe c1: %v", err)
	}
	if err := hub.Subscribe(40002, c2); err != nil {
		t.Fatalf("subscribe c2: %v", err)
	}
	if err := hub.Subscribe(40001, other); err != nil {
		t.Fatalf("subscribe other: %v", err)
	}
	if got := hub.SessionClientCount(sessionID); got != 2 {
		t.Fatalf("expected 2 session clients, got %d", got)
	}
	drainPresence(t, c1, c2, other)

	payload := json.RawMessage(`{"liveSessionId":9001,"status":"ENDED"}`)
	delivered := hub.BroadcastSessionEnd(sessionID, payload)
	if delivered != 2 {
		t.Fatalf("expected 2 delivered, got %d", delivered)
	}
	if got := hub.SessionClientCount(sessionID); got != 0 {
		t.Fatalf("expected session index cleaned, got %d", got)
	}
	for _, c := range []*Client{c1, c2} {
		select {
		case env := <-c.Outbound():
			if env.Type != "live_session.ended" || env.LiveSessionID != sessionID {
				t.Fatalf("unexpected envelope on %s: %+v", c.ID, env)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for live_session.ended on %s", c.ID)
		}
	}
	// other client (different session) should not receive live_session.ended
	select {
	case env := <-other.Outbound():
		if env.Type == "live_session.ended" {
			t.Fatalf("other client got unexpected live_session.ended: %+v", env)
		}
	default:
	}
}

func TestHubBroadcastSessionEndZeroIDIsNoop(t *testing.T) {
	hub := NewHub()
	if got := hub.BroadcastSessionEnd(0, nil); got != 0 {
		t.Fatalf("expected 0 delivered for zero session id, got %d", got)
	}
}

func TestHubStatsExposesRoomsClientsAndSessions(t *testing.T) {
	hub := NewHub()
	const sessionID uint64 = 9100
	c1 := NewClientWithSession("c1", "u1", 41001, sessionID, 8)
	c2 := NewClientWithSession("c2", "u2", 41002, sessionID, 8)
	c3 := NewClient("c3", "u3", 41001, 8)
	if err := hub.Subscribe(41001, c1); err != nil {
		t.Fatalf("subscribe c1: %v", err)
	}
	if err := hub.Subscribe(41002, c2); err != nil {
		t.Fatalf("subscribe c2: %v", err)
	}
	if err := hub.Subscribe(41001, c3); err != nil {
		t.Fatalf("subscribe c3: %v", err)
	}
	stats := hub.Stats()
	if stats.Rooms != 2 {
		t.Fatalf("expected 2 rooms, got %d", stats.Rooms)
	}
	if stats.Clients != 3 {
		t.Fatalf("expected 3 clients, got %d", stats.Clients)
	}
	if stats.LiveSessions != 1 {
		t.Fatalf("expected 1 live session, got %d", stats.LiveSessions)
	}
	if got := stats.SessionClients[sessionID]; got != 2 {
		t.Fatalf("expected session %d to have 2 clients, got %d", sessionID, got)
	}
}

func drainPresence(t *testing.T, clients ...*Client) {
	t.Helper()
	for _, client := range clients {
		for {
			select {
			case _, ok := <-client.Outbound():
				if !ok {
					goto nextClient
				}
			default:
				goto nextClient
			}
		}
	nextClient:
	}
}

func lastPresenceOnline(t *testing.T, client *Client) int {
	t.Helper()
	online := -1
	for {
		select {
		case env, ok := <-client.Outbound():
			if !ok {
				return online
			}
			if env.Type != "room.online" {
				continue
			}
			var payload struct {
				Online int `json:"online"`
			}
			if err := json.Unmarshal(env.Payload, &payload); err != nil {
				t.Fatalf("decode presence payload: %v", err)
			}
			online = payload.Online
		default:
			return online
		}
	}
}

type failingOnlineCounter struct{}

func (failingOnlineCounter) Join(ctx context.Context, auctionID uint64, connectionID string) (int, error) {
	return 0, errors.New("shared counter unavailable")
}

func (failingOnlineCounter) Leave(ctx context.Context, auctionID uint64, connectionID string) (int, error) {
	return 0, errors.New("shared counter unavailable")
}

func (failingOnlineCounter) Touch(ctx context.Context, auctionID uint64, connectionID string) (int, error) {
	return 0, errors.New("shared counter unavailable")
}

func (failingOnlineCounter) Count(ctx context.Context, auctionID uint64) (int, error) {
	return 0, errors.New("shared counter unavailable")
}

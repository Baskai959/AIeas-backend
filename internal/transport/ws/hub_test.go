package ws

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
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

func TestHubBeginDrainDeliversWithoutConsumingSeqOrHistory(t *testing.T) {
	hub := NewHubWithEventWindow(4)
	metrics := &fakeHubMetrics{}
	hub.SetMetrics(metrics)
	client := NewClient("c1", "u1", 20002, 8)
	if err := hub.Subscribe(20002, client); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	drainPresence(t, client)
	hub.Broadcast(20002, Envelope{Type: "e1"})
	before, ok := hub.Room(20002)
	if !ok {
		t.Fatal("expected room")
	}
	beforeSeq := before.CurrentSeq()
	beforeMissed, beforeComplete := hub.ReplaySince(20002, 0)
	drainPresence(t, client)

	hub.BeginDrain(1234)
	hub.BeginDrain(5678)

	if !hub.IsDraining() {
		t.Fatal("hub should be draining")
	}
	if metrics.drainingCount != 2 {
		t.Fatalf("expected draining metric per BeginDrain call, got %d", metrics.drainingCount)
	}
	room, _ := hub.Room(20002)
	if seq := room.CurrentSeq(); seq != beforeSeq {
		t.Fatalf("drain must not consume seq, before=%d got=%d", beforeSeq, seq)
	}
	missed, complete := hub.ReplaySince(20002, 0)
	if complete != beforeComplete || len(missed) != len(beforeMissed) {
		t.Fatalf("drain must not change replay history, before complete=%v len=%d after complete=%v len=%d", beforeComplete, len(beforeMissed), complete, len(missed))
	}
	for _, env := range missed {
		if env.Type == TypeGatewayDraining {
			t.Fatalf("drain envelope must not enter replay history: %+v", missed)
		}
	}

	found := 0
	for len(client.Outbound()) > 0 {
		env := <-client.Outbound()
		if env.Type == TypeGatewayDraining {
			found++
			var payload map[string]int
			if err := json.Unmarshal(env.Payload, &payload); err != nil {
				t.Fatalf("decode draining payload: %v", err)
			}
			if payload["retryAfterMs"] == 0 {
				t.Fatalf("missing retryAfterMs: %+v", payload)
			}
		}
	}
	if found != 2 {
		t.Fatalf("expected two direct draining envelopes, got %d", found)
	}
}

func TestHubSubscribeRejectsDuringDrainAndAwaitForceCloses(t *testing.T) {
	hub := NewHub()
	client := NewClient("c1", "u1", 20003, 8)
	if err := hub.Subscribe(20003, client); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	drainPresence(t, client)
	hub.BeginDrain(1000)
	if err := hub.Subscribe(20003, NewClient("c2", "u2", 20003, 8)); !errors.Is(err, ErrHubDraining) {
		t.Fatalf("expected ErrHubDraining from Subscribe, got %v", err)
	}
	if err := hub.SubscribeLiveSessionOnly(99, NewClientWithSession("c3", "u3", 0, 99, 8)); !errors.Is(err, ErrHubDraining) {
		t.Fatalf("expected ErrHubDraining from SubscribeLiveSessionOnly, got %v", err)
	}
	if err := hub.AwaitDrained(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("await drained: %v", err)
	}
	if !client.Closed() || client.CloseReason() != "gateway_draining" {
		t.Fatalf("expected force close with gateway_draining, closed=%v reason=%q", client.Closed(), client.CloseReason())
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
	merchant := NewClient("merchant", "m1", 30001, 4)
	merchant.CountOnline = false
	if err := hub.Subscribe(30001, c1); err != nil {
		t.Fatalf("subscribe c1: %v", err)
	}
	drainPresence(t, c1)
	if hub.OnlineCount(30001) != 1 {
		t.Fatalf("expected online count 1, got %d", hub.OnlineCount(30001))
	}
	if err := hub.Subscribe(30001, merchant); err != nil {
		t.Fatalf("subscribe merchant: %v", err)
	}
	if hub.OnlineCount(30001) != 1 {
		t.Fatalf("expected non-buyer client to be excluded from online count, got %d", hub.OnlineCount(30001))
	}
	drainPresence(t, c1, merchant)
	if err := hub.Subscribe(30001, c2); err != nil {
		t.Fatalf("subscribe c2: %v", err)
	}
	if hub.OnlineCount(30001) != 2 {
		t.Fatalf("expected online count 2, got %d", hub.OnlineCount(30001))
	}
	drainPresence(t, c1, c2, merchant)
	c2.Deliver(Envelope{Type: "prefill"})
	hub.Broadcast(30001, Envelope{Type: "announcement"})
	if hub.OnlineCount(30001) != 1 {
		t.Fatalf("expected slow consumer removed from online count, got %d", hub.OnlineCount(30001))
	}
	hub.Unsubscribe(30001, merchant.ID)
	if hub.OnlineCount(30001) != 1 {
		t.Fatalf("expected merchant unsubscribe to keep online count 1, got %d", hub.OnlineCount(30001))
	}
	hub.Unsubscribe(30001, c1.ID)
	if hub.OnlineCount(30001) != 0 {
		t.Fatalf("expected online count 0 after unsubscribe, got %d", hub.OnlineCount(30001))
	}
}

func TestHubLiveSessionOnlineCountWithoutActiveAuction(t *testing.T) {
	counter := NewMemoryOnlineCounter()
	hub := NewHubWithOnlineCounter(counter)
	buyer := NewClientWithSession("buyer", "u1", 0, 9001, 4)
	merchant := NewClientWithSession("merchant", "m1", 0, 9001, 4)
	merchant.CountOnline = false

	if err := hub.SubscribeLiveSessionOnly(9001, buyer); err != nil {
		t.Fatalf("subscribe buyer: %v", err)
	}
	if got := hub.LiveSessionOnlineCount(9001); got != 1 {
		t.Fatalf("expected live session online count 1, got %d", got)
	}
	if got := hub.OnlineCount(9001); got != 0 {
		t.Fatalf("live session online should not collide with auction online count, got %d", got)
	}
	if online := lastPresenceOnline(t, buyer); online != 1 {
		t.Fatalf("expected live session presence online 1, got %d", online)
	}

	if err := hub.SubscribeLiveSessionOnly(9001, merchant); err != nil {
		t.Fatalf("subscribe merchant: %v", err)
	}
	if got := hub.LiveSessionOnlineCount(9001); got != 1 {
		t.Fatalf("merchant should not be counted as buyer online, got %d", got)
	}
	drainPresence(t, buyer, merchant)
	hub.UnsubscribeClient(buyer)
	if got := hub.LiveSessionOnlineCount(9001); got != 0 {
		t.Fatalf("expected live session online count 0 after buyer leaves, got %d", got)
	}
}

func TestHubLiveSessionOnlineCountWithActiveAuction(t *testing.T) {
	hub := NewHubWithOnlineCounter(NewMemoryOnlineCounter())
	client := NewClientWithSession("buyer", "u1", 31002, 9002, 4)
	if err := hub.Subscribe(31002, client); err != nil {
		t.Fatalf("subscribe active auction client: %v", err)
	}
	if got := hub.OnlineCount(31002); got != 1 {
		t.Fatalf("expected auction online count 1, got %d", got)
	}
	if got := hub.LiveSessionOnlineCount(9002); got != 1 {
		t.Fatalf("expected live session online count 1, got %d", got)
	}
	hub.UnsubscribeClient(client)
	if got := hub.OnlineCount(31002); got != 0 {
		t.Fatalf("expected auction online count 0 after leave, got %d", got)
	}
	if got := hub.LiveSessionOnlineCount(9002); got != 0 {
		t.Fatalf("expected live session online count 0 after leave, got %d", got)
	}
}

func TestHubPublishesLiveSessionOnlinePresence(t *testing.T) {
	publisher := &fakeLiveSessionEventPublisher{}
	hub := NewHubWithOnlineCounter(NewMemoryOnlineCounter())
	hub.SetLiveSessionEventPublisher(publisher)
	client := NewClientWithSession("buyer-live-session-publish", "u1", 0, 9003, 4)

	if err := hub.SubscribeLiveSessionOnly(9003, client); err != nil {
		t.Fatalf("subscribe live session client: %v", err)
	}
	if got := hub.LiveSessionOnlineCount(9003); got != 1 {
		t.Fatalf("expected live session online count 1, got %d", got)
	}
	if len(publisher.calls) != 1 {
		t.Fatalf("expected one live session publish, got %d", len(publisher.calls))
	}
	call := publisher.calls[0]
	if call.LiveSessionID != 9003 || call.EventType != TypeRoomOnline || call.OnlineOnly {
		t.Fatalf("unexpected publish call: %+v", call)
	}
	var payload struct {
		LiveSessionID uint64 `json:"liveSessionId"`
		Online        int    `json:"online"`
	}
	if err := json.Unmarshal(call.Payload, &payload); err != nil {
		t.Fatalf("decode publish payload: %v", err)
	}
	if payload.LiveSessionID != 9003 || payload.Online != 1 {
		t.Fatalf("unexpected publish payload: %+v", payload)
	}
	assertNoPresence(t, client)
}

func TestHubFallsBackToLocalLiveSessionOnlinePresenceWhenPublisherFails(t *testing.T) {
	publisher := &fakeLiveSessionEventPublisher{err: errors.New("publish failed")}
	hub := NewHubWithOnlineCounter(NewMemoryOnlineCounter())
	hub.SetLiveSessionEventPublisher(publisher)
	client := NewClientWithSession("buyer-live-session-fallback", "u1", 0, 9004, 4)

	if err := hub.SubscribeLiveSessionOnly(9004, client); err != nil {
		t.Fatalf("subscribe live session client: %v", err)
	}
	if len(publisher.calls) != 1 {
		t.Fatalf("expected one live session publish attempt, got %d", len(publisher.calls))
	}
	if online := lastPresenceOnline(t, client); online != 1 {
		t.Fatalf("expected local fallback online 1, got %d", online)
	}
}

func TestHubDebouncesPresenceForLargeRooms(t *testing.T) {
	hub := NewHub()
	hub.SetPresenceImmediateFanoutLimit(1)
	hub.SetPresenceBroadcastDelay(time.Hour)
	c1 := NewClient("c1", "u1", 34001, 4)
	c2 := NewClient("c2", "u2", 34001, 4)
	if err := hub.Subscribe(34001, c1); err != nil {
		t.Fatalf("subscribe c1: %v", err)
	}
	drainPresence(t, c1)
	if err := hub.Subscribe(34001, c2); err != nil {
		t.Fatalf("subscribe c2: %v", err)
	}
	assertNoPresence(t, c1)
	assertNoPresence(t, c2)

	hub.flushPresence(34001)
	if online := lastPresenceOnline(t, c1); online != 2 {
		t.Fatalf("expected debounced c1 online 2, got %d", online)
	}
	if online := lastPresenceOnline(t, c2); online != 2 {
		t.Fatalf("expected debounced c2 online 2, got %d", online)
	}
}

func TestHubUnsubscribeClientDoesNotRemoveReplacementWithSameID(t *testing.T) {
	hub := NewHub()
	oldClient := NewClient("same", "u1", 35001, 4)
	replacement := NewClient("same", "u2", 35001, 4)
	if err := hub.Subscribe(35001, oldClient); err != nil {
		t.Fatalf("subscribe old: %v", err)
	}
	drainPresence(t, oldClient)
	if err := hub.Subscribe(35001, replacement); err != nil {
		t.Fatalf("subscribe replacement: %v", err)
	}
	if !oldClient.Closed() || oldClient.CloseReason() != "closed" {
		t.Fatalf("expected old client closed on replacement, closed=%v reason=%q", oldClient.Closed(), oldClient.CloseReason())
	}

	hub.UnsubscribeClient(oldClient)
	room, ok := hub.Room(35001)
	if !ok || room.ClientCount() != 1 {
		t.Fatalf("expected replacement to remain subscribed, room=%+v ok=%v", room, ok)
	}
	if replacement.Closed() {
		t.Fatalf("replacement should not be closed by old client teardown, reason=%q", replacement.CloseReason())
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

func TestHubThrottlesOnlineTouch(t *testing.T) {
	counter := &countingOnlineCounter{MemoryOnlineCounter: NewMemoryOnlineCounter()}
	hub := NewHubWithOnlineCounter(counter)
	hub.SetOnlineTouchInterval(time.Hour)
	client := NewClient("c1", "u1", 36001, 4)
	if err := hub.Subscribe(36001, client); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	drainPresence(t, client)

	if got := hub.Touch(36001, client.ID); got != 1 {
		t.Fatalf("first throttled touch should return local fallback count 1, got %d", got)
	}
	if got := hub.Touch(36001, client.ID); got != 1 {
		t.Fatalf("second throttled touch should return local fallback count 1, got %d", got)
	}
	if got := counter.touchCount.Load(); got != 0 {
		t.Fatalf("touches within interval should not hit shared counter, got %d", got)
	}

	hub.SetOnlineTouchInterval(0)
	if got := hub.Touch(36001, client.ID); got != 1 {
		t.Fatalf("unthrottled touch should still return online count 1, got %d", got)
	}
	if got := counter.touchCount.Load(); got != 1 {
		t.Fatalf("disabled throttle should hit shared counter once, got %d", got)
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

func TestHubBroadcastLiveSessionDeliversWithoutCleaningSessionIndex(t *testing.T) {
	hub := NewHub()
	const sessionID uint64 = 9002
	c1 := NewClientWithSession("c1", "u1", 40011, sessionID, 8)
	c2 := NewClientWithSession("c2", "u2", 40012, sessionID, 8)
	if err := hub.Subscribe(40011, c1); err != nil {
		t.Fatalf("subscribe c1: %v", err)
	}
	if err := hub.Subscribe(40012, c2); err != nil {
		t.Fatalf("subscribe c2: %v", err)
	}
	drainPresence(t, c1, c2)

	delivered := hub.BroadcastLiveSession(sessionID, Envelope{Type: TypeLiveVoiceBroadcast, RequestID: "voice-1", Payload: json.RawMessage(`{"audioBase64":"AQI="}`)})
	if delivered != 2 {
		t.Fatalf("expected 2 delivered, got %d", delivered)
	}
	if got := hub.SessionClientCount(sessionID); got != 2 {
		t.Fatalf("expected session index retained, got %d", got)
	}
	for _, c := range []*Client{c1, c2} {
		select {
		case env := <-c.Outbound():
			if env.Type != TypeLiveVoiceBroadcast || env.LiveSessionID != sessionID || env.RequestID != "voice-1" {
				t.Fatalf("unexpected envelope on %s: %+v", c.ID, env)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for live voice broadcast on %s", c.ID)
		}
	}
}

func TestHubBroadcastLiveSessionOnlineClientsOnly(t *testing.T) {
	hub := NewHub()
	const sessionID uint64 = 9003
	buyer := NewClientWithSession("buyer", "u1", 0, sessionID, 8)
	merchant := NewClientWithSession("merchant", "m1", 0, sessionID, 8)
	admin := NewClientWithSession("admin", "a1", 0, sessionID, 8)
	merchant.CountOnline = false
	admin.CountOnline = false
	if err := hub.SubscribeLiveSessionOnly(sessionID, buyer); err != nil {
		t.Fatalf("subscribe buyer: %v", err)
	}
	if err := hub.SubscribeLiveSessionOnly(sessionID, merchant); err != nil {
		t.Fatalf("subscribe merchant: %v", err)
	}
	if err := hub.SubscribeLiveSessionOnly(sessionID, admin); err != nil {
		t.Fatalf("subscribe admin: %v", err)
	}
	drainPresence(t, buyer, merchant, admin)

	delivered := hub.BroadcastLiveSessionOnlineClients(sessionID, Envelope{Type: TypeLiveVoiceBroadcast, RequestID: "voice-online-1", Payload: json.RawMessage(`{"audioBase64":"AQI="}`)})
	if delivered != 1 {
		t.Fatalf("expected 1 delivered, got %d", delivered)
	}
	select {
	case env := <-buyer.Outbound():
		if env.Type != TypeLiveVoiceBroadcast || env.LiveSessionID != sessionID || env.RequestID != "voice-online-1" {
			t.Fatalf("unexpected buyer envelope: %+v", env)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for buyer live voice broadcast")
	}
	select {
	case env := <-merchant.Outbound():
		t.Fatalf("merchant should not receive online-client voice broadcast: %+v", env)
	default:
	}
	select {
	case env := <-admin.Outbound():
		t.Fatalf("admin should not receive online-client voice broadcast: %+v", env)
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

func assertNoPresence(t *testing.T, client *Client) {
	t.Helper()
	select {
	case env := <-client.Outbound():
		t.Fatalf("unexpected immediate presence for %s: %+v", client.ID, env)
	default:
	}
}

type failingOnlineCounter struct{}

func (failingOnlineCounter) Join(ctx context.Context, auctionID uint64, connectionID, userID string) (int, error) {
	return 0, errors.New("shared counter unavailable")
}

func (failingOnlineCounter) Leave(ctx context.Context, auctionID uint64, connectionID, userID string) (int, error) {
	return 0, errors.New("shared counter unavailable")
}

func (failingOnlineCounter) Touch(ctx context.Context, auctionID uint64, connectionID, userID string) (int, error) {
	return 0, errors.New("shared counter unavailable")
}

func (failingOnlineCounter) Count(ctx context.Context, auctionID uint64) (int, error) {
	return 0, errors.New("shared counter unavailable")
}

type countingOnlineCounter struct {
	*MemoryOnlineCounter
	touchCount atomic.Int64
}

func (c *countingOnlineCounter) Touch(ctx context.Context, auctionID uint64, connectionID, userID string) (int, error) {
	c.touchCount.Add(1)
	return c.MemoryOnlineCounter.Touch(ctx, auctionID, connectionID, userID)
}

type liveSessionPublishCall struct {
	LiveSessionID uint64
	EventType     string
	RequestID     string
	Seq           int64
	Payload       json.RawMessage
	OnlineOnly    bool
}

type fakeLiveSessionEventPublisher struct {
	err   error
	calls []liveSessionPublishCall
}

func (p *fakeLiveSessionEventPublisher) PublishLiveSessionEvent(_ context.Context, liveSessionID uint64, eventType, requestID string, seq int64, payload json.RawMessage, onlineOnly bool) error {
	p.calls = append(p.calls, liveSessionPublishCall{
		LiveSessionID: liveSessionID,
		EventType:     eventType,
		RequestID:     requestID,
		Seq:           seq,
		Payload:       append(json.RawMessage(nil), payload...),
		OnlineOnly:    onlineOnly,
	})
	return p.err
}

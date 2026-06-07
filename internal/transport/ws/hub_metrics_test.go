// hub_metrics_test.go 验证 G10：Hub 通过窄 HubMetrics 接口（而非 metrics.Registry
// 具体类型）打点。所以这里用一个 fakeHubMetrics 充当 spy，验证 Subscribe /
// Unsubscribe / Broadcast / 慢消费者关闭路径都触发了对应的 metric 调用。
package ws

import (
	"sync"
	"testing"
	"time"
)

// fakeHubMetrics 记录 HubMetrics 接口上每个方法被调用的次数与参数。
type fakeHubMetrics struct {
	mu                    sync.Mutex
	connectCalls          int
	disconnectReasons     []string
	broadcasts            []broadcastEvent
	slowClientDisconnects int
	handshakeRejects      []string
	drainingCount         int
}

type broadcastEvent struct {
	Elapsed time.Duration
	Fanout  int
}

func (f *fakeHubMetrics) IncWSConnect() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connectCalls++
}

func (f *fakeHubMetrics) IncWSDisconnect(reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.disconnectReasons = append(f.disconnectReasons, reason)
}

func (f *fakeHubMetrics) ObserveWSBroadcast(elapsed time.Duration, fanout int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.broadcasts = append(f.broadcasts, broadcastEvent{Elapsed: elapsed, Fanout: fanout})
}

func (f *fakeHubMetrics) IncWSSlowClientDisconnect() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.slowClientDisconnects++
}

func (f *fakeHubMetrics) IncWSHandshakeReject(reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.handshakeRejects = append(f.handshakeRejects, reason)
}

func (f *fakeHubMetrics) IncWSDraining() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.drainingCount++
}

func (f *fakeHubMetrics) snapshot() (int, []string, []broadcastEvent, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	reasons := append([]string(nil), f.disconnectReasons...)
	bcasts := append([]broadcastEvent(nil), f.broadcasts...)
	return f.connectCalls, reasons, bcasts, f.slowClientDisconnects
}

// 编译期断言：fakeHubMetrics 必须实现 HubMetrics 接口。
var _ HubMetrics = (*fakeHubMetrics)(nil)

func TestHubSetMetricsRecordsConnectAndDisconnect(t *testing.T) {
	hub := NewHub()
	m := &fakeHubMetrics{}
	hub.SetMetrics(m)

	client := NewClient("c1", "u1", 50001, 4)
	if err := hub.Subscribe(50001, client); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	hub.Unsubscribe(50001, client.ID)

	connects, reasons, _, _ := m.snapshot()
	if connects != 1 {
		t.Fatalf("expected 1 IncWSConnect call, got %d", connects)
	}
	if len(reasons) != 1 || reasons[0] != "unsubscribe" {
		t.Fatalf("expected single disconnect reason=unsubscribe, got %v", reasons)
	}
}

func TestHubSetMetricsObservesBroadcast(t *testing.T) {
	hub := NewHub()
	m := &fakeHubMetrics{}
	hub.SetMetrics(m)

	client := NewClient("c1", "u1", 50002, 8)
	if err := hub.Subscribe(50002, client); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	drainPresence(t, client)

	delivered := hub.Broadcast(50002, Envelope{Type: "announcement"})
	if delivered != 1 {
		t.Fatalf("expected 1 delivery, got %d", delivered)
	}
	_, _, broadcasts, _ := m.snapshot()
	if len(broadcasts) != 1 {
		t.Fatalf("expected 1 broadcast observation, got %d", len(broadcasts))
	}
	if broadcasts[0].Fanout != 1 {
		t.Fatalf("expected fanout=1, got %d", broadcasts[0].Fanout)
	}
}

func TestHubSetMetricsRecordsSlowClientDisconnect(t *testing.T) {
	hub := NewHub()
	m := &fakeHubMetrics{}
	hub.SetMetrics(m)

	slow := NewClient("slow", "u1", 50003, 1)
	if err := hub.Subscribe(50003, slow); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	drainPresence(t, slow)
	// fill the buffer so the next broadcast forces slow_consumer close.
	slow.Deliver(Envelope{Type: "prefill"})

	hub.Broadcast(50003, Envelope{Type: "announcement"})

	_, reasons, _, slowClose := m.snapshot()
	if slowClose != 1 {
		t.Fatalf("expected 1 IncWSSlowClientDisconnect call, got %d", slowClose)
	}
	hasSlow := false
	for _, r := range reasons {
		if r == "slow_consumer" {
			hasSlow = true
		}
	}
	if !hasSlow {
		t.Fatalf("expected disconnect reason=slow_consumer, got %v", reasons)
	}
}

func TestHubSetMetricsNilSafe(t *testing.T) {
	// Hub 默认不携带 metrics；订阅 / 广播路径不应 panic。
	hub := NewHub()
	client := NewClient("c1", "u1", 50004, 4)
	if err := hub.Subscribe(50004, client); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	drainPresence(t, client)
	hub.Broadcast(50004, Envelope{Type: "announcement"})

	// 显式重置为 nil 也必须安全。
	hub.SetMetrics(nil)
	hub.Broadcast(50004, Envelope{Type: "announcement-2"})
}

package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const defaultEventWindow = 256
const defaultOnlineCounterTimeout = 200 * time.Millisecond

var nextHubInstanceID atomic.Uint64

type OnlineCounter interface {
	Join(ctx context.Context, auctionID uint64, connectionID string) (int, error)
	Touch(ctx context.Context, auctionID uint64, connectionID string) (int, error)
	Leave(ctx context.Context, auctionID uint64, connectionID string) (int, error)
	Count(ctx context.Context, auctionID uint64) (int, error)
}

type ReplaySource interface {
	ReplaySince(ctx context.Context, auctionID uint64, lastSeq int64) ([]Envelope, bool, error)
}

type Hub struct {
	mu             sync.RWMutex
	rooms          map[uint64]*Room
	eventWindow    int
	onlineCounter  OnlineCounter
	replaySource   ReplaySource
	onlineTimeout  time.Duration
	instancePrefix string
}

func (h *Hub) SetReplaySource(source ReplaySource) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.replaySource = source
}

type Room struct {
	AuctionID   uint64
	mu          sync.RWMutex
	clients     map[string]*Client
	seq         atomic.Int64
	history     []Envelope
	disconnects atomic.Int64
}

type Client struct {
	ID          string
	UserID      string
	AuctionID   uint64
	send        chan Envelope
	sendMu      sync.Mutex
	closed      atomic.Bool
	dropped     atomic.Int64
	failures    atomic.Int64
	closeReason atomic.Value
}

func NewHub() *Hub {
	return NewHubWithEventWindow(defaultEventWindow)
}

func NewHubWithEventWindow(eventWindow int) *Hub {
	return NewHubWithEventWindowAndOnlineCounter(eventWindow, nil)
}

func NewHubWithOnlineCounter(counter OnlineCounter) *Hub {
	return NewHubWithEventWindowAndOnlineCounter(defaultEventWindow, counter)
}

func NewHubWithEventWindowAndOnlineCounter(eventWindow int, counter OnlineCounter) *Hub {
	if eventWindow <= 0 {
		eventWindow = defaultEventWindow
	}
	seq := nextHubInstanceID.Add(1)
	return &Hub{
		rooms:          make(map[uint64]*Room),
		eventWindow:    eventWindow,
		onlineCounter:  counter,
		onlineTimeout:  defaultOnlineCounterTimeout,
		instancePrefix: fmt.Sprintf("hub-%d-%d", time.Now().UnixNano(), seq),
	}
}

func NewClient(id, userID string, auctionID uint64, bufferSize int) *Client {
	if bufferSize <= 0 {
		bufferSize = 32
	}
	return &Client{ID: id, UserID: userID, AuctionID: auctionID, send: make(chan Envelope, bufferSize)}
}

func (h *Hub) Room(auctionID uint64) (*Room, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	room, ok := h.rooms[auctionID]
	return room, ok
}

func (h *Hub) GetOrCreateRoom(auctionID uint64) *Room {
	h.mu.RLock()
	room := h.rooms[auctionID]
	h.mu.RUnlock()
	if room != nil {
		return room
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if room = h.rooms[auctionID]; room != nil {
		return room
	}
	room = &Room{AuctionID: auctionID, clients: make(map[string]*Client), history: make([]Envelope, 0, h.eventWindow)}
	h.rooms[auctionID] = room
	return room
}

func (h *Hub) Subscribe(auctionID uint64, client *Client) error {
	if client == nil || client.ID == "" {
		return fmt.Errorf("client is required")
	}
	if client.AuctionID != 0 && client.AuctionID != auctionID {
		h.Unsubscribe(client.AuctionID, client.ID)
	}
	client.AuctionID = auctionID
	room := h.GetOrCreateRoom(auctionID)
	room.Add(client)
	h.broadcastPresence(room, h.joinOnline(auctionID, client.ID, room.ClientCount()))
	return nil
}

func (h *Hub) Unsubscribe(auctionID uint64, clientID string) {
	room, ok := h.Room(auctionID)
	if !ok {
		return
	}
	if room.Remove(clientID) {
		h.broadcastPresence(room, h.leaveOnline(auctionID, clientID, room.ClientCount()))
	}
}

func (h *Hub) Broadcast(auctionID uint64, env Envelope) int {
	room := h.GetOrCreateRoom(auctionID)
	delivered, removed := room.Broadcast(env)
	if len(removed) > 0 {
		for _, clientID := range removed {
			_ = h.leaveOnline(auctionID, clientID, room.ClientCount())
		}
		h.broadcastPresence(room, h.onlineCount(auctionID, room.ClientCount()))
	}
	return delivered
}

func (h *Hub) ReplaySince(auctionID uint64, lastSeq int64) ([]Envelope, bool) {
	h.mu.RLock()
	source := h.replaySource
	h.mu.RUnlock()
	if source != nil && lastSeq > 0 {
		if missed, complete, err := source.ReplaySince(context.Background(), auctionID, lastSeq); err == nil {
			return missed, complete
		}
	}
	room, ok := h.Room(auctionID)
	if !ok || lastSeq <= 0 {
		return nil, true
	}
	return room.ReplaySince(lastSeq)
}

func (h *Hub) Touch(auctionID uint64, clientID string) int {
	room, ok := h.Room(auctionID)
	fallback := 0
	if ok {
		fallback = room.ClientCount()
	}
	return h.touchOnline(auctionID, clientID, fallback)
}

func (h *Hub) OnlineCount(auctionID uint64) int {
	room, ok := h.Room(auctionID)
	if !ok {
		return h.onlineCount(auctionID, 0)
	}
	return h.onlineCount(auctionID, room.ClientCount())
}

func (h *Hub) joinOnline(auctionID uint64, clientID string, fallback int) int {
	if h.onlineCounter == nil {
		return fallback
	}
	ctx, cancel := h.onlineCounterContext()
	defer cancel()
	count, err := h.onlineCounter.Join(ctx, auctionID, h.onlineMemberID(clientID))
	if err != nil || count < 0 {
		return fallback
	}
	return count
}

func (h *Hub) leaveOnline(auctionID uint64, clientID string, fallback int) int {
	if h.onlineCounter == nil {
		return fallback
	}
	ctx, cancel := h.onlineCounterContext()
	defer cancel()
	count, err := h.onlineCounter.Leave(ctx, auctionID, h.onlineMemberID(clientID))
	if err != nil || count < 0 {
		return fallback
	}
	return count
}

func (h *Hub) touchOnline(auctionID uint64, clientID string, fallback int) int {
	if h.onlineCounter == nil {
		return fallback
	}
	ctx, cancel := h.onlineCounterContext()
	defer cancel()
	count, err := h.onlineCounter.Touch(ctx, auctionID, h.onlineMemberID(clientID))
	if err != nil || count < 0 {
		return fallback
	}
	return count
}

func (h *Hub) onlineCount(auctionID uint64, fallback int) int {
	if h.onlineCounter == nil {
		return fallback
	}
	ctx, cancel := h.onlineCounterContext()
	defer cancel()
	count, err := h.onlineCounter.Count(ctx, auctionID)
	if err != nil || count < 0 {
		return fallback
	}
	return count
}

func (h *Hub) onlineCounterContext() (context.Context, context.CancelFunc) {
	timeout := h.onlineTimeout
	if timeout <= 0 {
		timeout = defaultOnlineCounterTimeout
	}
	return context.WithTimeout(context.Background(), timeout)
}

func (h *Hub) onlineMemberID(clientID string) string {
	return h.instancePrefix + ":" + clientID
}

func (h *Hub) broadcastPresence(room *Room, online int) {
	if room == nil {
		return
	}
	removed := room.BroadcastPresence(online)
	for attempts := 0; len(removed) > 0 && attempts < 3; attempts++ {
		for _, clientID := range removed {
			_ = h.leaveOnline(room.AuctionID, clientID, room.ClientCount())
		}
		removed = room.BroadcastPresence(h.onlineCount(room.AuctionID, room.ClientCount()))
	}
}

func (h *Hub) DisconnectCount(auctionID uint64) int64 {
	room, ok := h.Room(auctionID)
	if !ok {
		return 0
	}
	return room.disconnects.Load()
}

func (h *Hub) HandleInbound(ctx context.Context, client *Client, env Envelope) []Envelope {
	_ = ctx
	if client == nil {
		return []Envelope{ErrorEnvelope(env.RequestID, "client missing")}
	}
	responses := make([]Envelope, 0, 2)
	if env.RequestID != "" {
		responses = append(responses, AckEnvelope(env.RequestID, env.Seq))
	}
	switch env.Type {
	case "ping", "heartbeat":
		responseType := "pong"
		if env.Type == "heartbeat" {
			responseType = "heartbeat.ack"
		}
		responses = append(responses, Envelope{Type: responseType, RequestID: env.RequestID, Seq: env.Seq})
	case "subscribe", "room.subscribe":
		_ = h.Subscribe(client.AuctionID, client)
		responseType := "subscribed"
		if env.Type == "room.subscribe" {
			responseType = "room.subscribed"
		}
		responses = append(responses, Envelope{Type: responseType, RequestID: env.RequestID, Seq: env.Seq})
	case "room.unsubscribe":
		h.Unsubscribe(client.AuctionID, client.ID)
		responses = append(responses, Envelope{Type: "room.unsubscribed", RequestID: env.RequestID, Seq: env.Seq})
	case "announcement":
		out := env
		if out.Seq == 0 {
			if room, ok := h.Room(client.AuctionID); ok {
				out.Seq = room.NextSeq()
			}
		}
		h.Broadcast(client.AuctionID, out)
	default:
		if env.Type == "" {
			responses = append(responses, ErrorEnvelope(env.RequestID, "message type required"))
		}
	}
	return responses
}

func (h *Hub) StartHeartbeat(ctx context.Context, auctionID uint64, interval time.Duration) {
	if interval <= 0 {
		interval = 20 * time.Second
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				payload := []byte(fmt.Sprintf(`{"ts":%d}`, now.UnixMilli()))
				h.Broadcast(auctionID, Envelope{Type: "ping", Payload: payload})
			}
		}
	}()
}

func (r *Room) Add(client *Client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if old := r.clients[client.ID]; old != nil && old != client {
		old.Close()
	}
	r.clients[client.ID] = client
}

func (r *Room) Remove(clientID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if client := r.clients[clientID]; client != nil {
		client.CloseWithReason("unsubscribe")
		delete(r.clients, clientID)
		r.disconnects.Add(1)
		return true
	}
	return false
}

func (r *Room) Broadcast(env Envelope) (int, []string) {
	r.mu.Lock()
	if env.Seq == 0 {
		env.Seq = r.NextSeq()
	} else {
		r.observeSeq(env.Seq)
	}
	r.appendHistoryLocked(env)
	clients := make([]*Client, 0, len(r.clients))
	for _, client := range r.clients {
		clients = append(clients, client)
	}
	r.mu.Unlock()

	delivered := 0
	var slow []string
	for _, client := range clients {
		if client.Deliver(env) {
			delivered++
		} else if client.Closed() {
			slow = append(slow, client.ID)
		}
	}
	if len(slow) > 0 {
		r.mu.Lock()
		removed := slow[:0]
		for _, clientID := range slow {
			if client := r.clients[clientID]; client != nil && client.Closed() {
				delete(r.clients, clientID)
				r.disconnects.Add(1)
				removed = append(removed, clientID)
			}
		}
		r.mu.Unlock()
		slow = removed
	}
	return delivered, slow
}

func (r *Room) ClientCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.clients)
}

func (r *Room) ReplaySince(lastSeq int64) ([]Envelope, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if lastSeq >= r.seq.Load() {
		return nil, true
	}
	if len(r.history) == 0 {
		return nil, false
	}
	first := r.history[0].Seq
	if lastSeq < first-1 {
		return nil, false
	}
	replayed := make([]Envelope, 0, len(r.history))
	for _, env := range r.history {
		if env.Seq > lastSeq {
			replayed = append(replayed, env)
		}
	}
	return replayed, true
}

func (r *Room) appendHistoryLocked(env Envelope) {
	if env.Seq <= 0 {
		return
	}
	if cap(r.history) == 0 {
		r.history = make([]Envelope, 0, defaultEventWindow)
	}
	if len(r.history) == cap(r.history) {
		copy(r.history, r.history[1:])
		r.history[len(r.history)-1] = env
		return
	}
	r.history = append(r.history, env)
}

func (r *Room) BroadcastPresence(online int) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.broadcastPresenceLocked(online)
}

func (r *Room) broadcastPresenceLocked(online int) []string {
	if online < 0 {
		online = 0
	}
	payload, _ := json.Marshal(map[string]interface{}{"auctionId": r.AuctionID, "online": online})
	env := Envelope{Type: "room.online", Seq: r.NextSeq(), Payload: payload}
	r.appendHistoryLocked(env)
	removed := make([]string, 0)
	for _, client := range r.clients {
		if !client.Deliver(env) && client.Closed() {
			delete(r.clients, client.ID)
			r.disconnects.Add(1)
			removed = append(removed, client.ID)
		}
	}
	return removed
}

func (r *Room) NextSeq() int64 {
	return r.seq.Add(1)
}

func (r *Room) observeSeq(seq int64) {
	for {
		current := r.seq.Load()
		if seq <= current {
			return
		}
		if r.seq.CompareAndSwap(current, seq) {
			return
		}
	}
}

func (c *Client) Deliver(env Envelope) bool {
	if c == nil || c.closed.Load() {
		return false
	}
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if c.closed.Load() {
		return false
	}
	select {
	case c.send <- env:
		return true
	default:
		c.dropped.Add(1)
		c.closeLocked("slow_consumer")
		return false
	}
}

func (c *Client) Outbound() <-chan Envelope {
	return c.send
}

func (c *Client) Dropped() int64 {
	return c.dropped.Load()
}

func (c *Client) SendFailures() int64 {
	return c.failures.Load()
}

func (c *Client) MarkSendFailure() int64 {
	return c.failures.Add(1)
}

func (c *Client) Closed() bool {
	return c == nil || c.closed.Load()
}

func (c *Client) CloseReason() string {
	if c == nil {
		return ""
	}
	if value := c.closeReason.Load(); value != nil {
		if reason, ok := value.(string); ok {
			return reason
		}
	}
	return ""
}

func (c *Client) Close() {
	c.CloseWithReason("closed")
}

func (c *Client) CloseWithReason(reason string) {
	if reason == "" {
		reason = "closed"
	}
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	c.closeLocked(reason)
}

func (c *Client) closeLocked(reason string) {
	if c.closed.CompareAndSwap(false, true) {
		c.closeReason.Store(reason)
		close(c.send)
	}
}

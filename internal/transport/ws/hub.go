package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const defaultEventWindow = 256
const defaultOnlineCounterTimeout = 200 * time.Millisecond
const defaultPresenceBroadcastDelay = 200 * time.Millisecond
const defaultPresenceImmediateFanoutLimit = 16
const defaultOnlineTouchInterval = 30 * time.Second

var nextHubInstanceID atomic.Uint64

type OnlineCounter interface {
	Join(ctx context.Context, auctionID uint64, connectionID, userID string) (int, error)
	Touch(ctx context.Context, auctionID uint64, connectionID, userID string) (int, error)
	Leave(ctx context.Context, auctionID uint64, connectionID, userID string) (int, error)
	Count(ctx context.Context, auctionID uint64) (int, error)
}

type ReplaySource interface {
	ReplaySince(ctx context.Context, auctionID uint64, lastSeq int64) ([]Envelope, bool, error)
}

// HubMetrics 是 Hub 在打点路径上依赖的最小指标接口，避免 ws 包反向依赖
// 具体的 metrics.Registry 类型，便于测试与可观测后端替换。
type HubMetrics interface {
	IncWSConnect()
	IncWSDisconnect(reason string)
	ObserveWSBroadcast(elapsed time.Duration, fanout int)
	IncWSSlowClientDisconnect()
}

type Hub struct {
	mu             sync.RWMutex
	rooms          map[uint64]*Room
	sessionClients map[uint64]map[string]*Client // liveSessionId -> clientID -> client
	eventWindow    int
	onlineCounter  OnlineCounter
	replaySource   ReplaySource
	onlineTimeout  time.Duration
	instancePrefix string
	metrics        HubMetrics
	presenceMu     sync.Mutex
	presenceDelay  time.Duration
	presenceLimit  int
	presenceTimers map[uint64]*presenceUpdate
	onlineTouchMu  sync.Mutex
	onlineTouchTTL time.Duration
	onlineTouched  map[string]time.Time
}

type presenceUpdate struct {
	room  *Room
	timer *time.Timer
}

func (h *Hub) SetReplaySource(source ReplaySource) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.replaySource = source
}

// SetMetrics 注入观测性指标实现。nil 安全：传 nil 等同于关闭打点。
func (h *Hub) SetMetrics(m HubMetrics) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.metrics = m
}

type Room struct {
	AuctionID   uint64
	mu          sync.RWMutex
	clients     map[string]*Client
	seq         atomic.Int64
	pubSeq      atomic.Int64
	history     []Envelope
	disconnects atomic.Int64
}

type removedClient struct {
	ID            string
	UserID        string
	LiveSessionID uint64
	CountOnline   bool
}

type Client struct {
	ID            string
	UserID        string
	AuctionID     uint64
	LiveSessionID uint64
	CountOnline   bool
	send          chan Envelope
	sendMu        sync.Mutex
	closed        atomic.Bool
	dropped       atomic.Int64
	failures      atomic.Int64
	closeReason   atomic.Value
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
		sessionClients: make(map[uint64]map[string]*Client),
		eventWindow:    eventWindow,
		onlineCounter:  counter,
		onlineTimeout:  defaultOnlineCounterTimeout,
		instancePrefix: fmt.Sprintf("hub-%d-%d", time.Now().UnixNano(), seq),
		presenceDelay:  defaultPresenceBroadcastDelay,
		presenceLimit:  defaultPresenceImmediateFanoutLimit,
		presenceTimers: make(map[uint64]*presenceUpdate),
		onlineTouchTTL: defaultOnlineTouchInterval,
		onlineTouched:  make(map[string]time.Time),
	}
}

func (h *Hub) SetPresenceBroadcastDelay(delay time.Duration) {
	if h == nil {
		return
	}
	h.presenceMu.Lock()
	defer h.presenceMu.Unlock()
	h.presenceDelay = delay
}

func (h *Hub) SetPresenceImmediateFanoutLimit(limit int) {
	if h == nil {
		return
	}
	h.presenceMu.Lock()
	defer h.presenceMu.Unlock()
	h.presenceLimit = limit
}

// SetOnlineTouchInterval 设置在线人数续期间隔。interval<=0 表示不节流。
func (h *Hub) SetOnlineTouchInterval(interval time.Duration) {
	if h == nil {
		return
	}
	h.onlineTouchMu.Lock()
	defer h.onlineTouchMu.Unlock()
	h.onlineTouchTTL = interval
	h.onlineTouched = make(map[string]time.Time)
}

func NewClient(id, userID string, auctionID uint64, bufferSize int) *Client {
	if bufferSize <= 0 {
		bufferSize = 32
	}
	return &Client{ID: id, UserID: userID, AuctionID: auctionID, CountOnline: true, send: make(chan Envelope, bufferSize)}
}

// NewClientWithSession 构造一个带 liveSessionId 关联的 Client。
// liveSessionId 为 0 时与 NewClient 行为一致（不进入 session 反查表）。
func NewClientWithSession(id, userID string, auctionID, liveSessionID uint64, bufferSize int) *Client {
	if bufferSize <= 0 {
		bufferSize = 32
	}
	return &Client{ID: id, UserID: userID, AuctionID: auctionID, LiveSessionID: liveSessionID, CountOnline: true, send: make(chan Envelope, bufferSize)}
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
	h.registerSessionClient(client)
	fallback := room.OnlineClientCount()
	online := h.onlineCount(auctionID, fallback)
	if client.CountOnline {
		online = h.joinOnline(auctionID, client.ID, client.UserID, fallback)
		h.markOnlineTouched(auctionID, client.ID)
	}
	h.emitPresence(room, online)
	if reg := h.metricsSnapshot(); reg != nil {
		reg.IncWSConnect()
	}
	return nil
}

func (h *Hub) SubscribeLiveSessionOnly(liveSessionID uint64, client *Client) error {
	if h == nil || client == nil || client.ID == "" || liveSessionID == 0 {
		return fmt.Errorf("live session client is required")
	}
	client.AuctionID = 0
	client.LiveSessionID = liveSessionID
	h.registerSessionClient(client)
	if reg := h.metricsSnapshot(); reg != nil {
		reg.IncWSConnect()
	}
	return nil
}

func (h *Hub) Unsubscribe(auctionID uint64, clientID string) {
	if auctionID == 0 {
		h.unregisterSessionClientByID(clientID)
		if reg := h.metricsSnapshot(); reg != nil {
			reg.IncWSDisconnect("unsubscribe")
		}
		return
	}
	room, ok := h.Room(auctionID)
	if !ok {
		return
	}
	if removed, sessionID, countOnline, userID := room.removeReturning(clientID); removed {
		if sessionID != 0 {
			h.unregisterSessionClient(sessionID, clientID)
		}
		fallback := room.OnlineClientCount()
		online := h.onlineCount(auctionID, fallback)
		if countOnline {
			online = h.leaveOnline(auctionID, clientID, userID, fallback)
		}
		h.emitPresence(room, online)
		if reg := h.metricsSnapshot(); reg != nil {
			reg.IncWSDisconnect("unsubscribe")
		}
	}
}

func (h *Hub) UnsubscribeClient(client *Client) {
	if h == nil || client == nil {
		return
	}
	if client.AuctionID == 0 {
		h.unregisterSessionClient(client.LiveSessionID, client.ID)
		if reg := h.metricsSnapshot(); reg != nil {
			reg.IncWSDisconnect("unsubscribe")
		}
		return
	}
	room, ok := h.Room(client.AuctionID)
	if !ok {
		return
	}
	if removed, sessionID, countOnline, userID := room.removeClientReturning(client); removed {
		if sessionID != 0 {
			h.unregisterSessionClient(sessionID, client.ID)
		}
		fallback := room.OnlineClientCount()
		online := h.onlineCount(client.AuctionID, fallback)
		if countOnline {
			online = h.leaveOnline(client.AuctionID, client.ID, userID, fallback)
		}
		h.emitPresence(room, online)
		if reg := h.metricsSnapshot(); reg != nil {
			reg.IncWSDisconnect("unsubscribe")
		}
	}
}

func (h *Hub) Broadcast(auctionID uint64, env Envelope) int {
	room := h.GetOrCreateRoom(auctionID)
	start := time.Now()
	delivered, removed := room.Broadcast(env)
	elapsed := time.Since(start)
	if reg := h.metricsSnapshot(); reg != nil {
		reg.ObserveWSBroadcast(elapsed, delivered)
		for range removed {
			reg.IncWSSlowClientDisconnect()
			reg.IncWSDisconnect("slow_consumer")
		}
	}
	if len(removed) > 0 {
		for _, client := range removed {
			removed, sessionID, countOnline, userID := room.removeReturning(client.ID)
			if !removed {
				countOnline = client.CountOnline
				sessionID = client.LiveSessionID
				userID = client.UserID
			}
			if countOnline {
				_ = h.leaveOnline(auctionID, client.ID, userID, room.OnlineClientCount())
			}
			if sessionID != 0 {
				h.unregisterSessionClient(sessionID, client.ID)
			} else {
				h.unregisterSessionClientByID(client.ID)
			}
		}
		h.emitPresence(room, h.onlineCount(auctionID, room.OnlineClientCount()))
	}
	return delivered
}

// BroadcastLiveSession 把事件推送给订阅了某 liveSessionId 的客户端。
//
// 与 BroadcastSessionEnd 不同，它不会清理 session 反查表，适合直播过程中的
// 控制台事件，例如语音播报。
func (h *Hub) BroadcastLiveSession(liveSessionID uint64, env Envelope) int {
	if h == nil || liveSessionID == 0 {
		return 0
	}
	env.LiveSessionID = liveSessionID
	h.mu.RLock()
	bucket := h.sessionClients[liveSessionID]
	clients := make([]*Client, 0, len(bucket))
	for _, c := range bucket {
		clients = append(clients, c)
	}
	h.mu.RUnlock()
	if len(clients) == 0 {
		return 0
	}
	delivered := 0
	for _, c := range clients {
		if c.Deliver(env) {
			delivered++
		}
	}
	return delivered
}

// metricsSnapshot 在锁内拷贝当前 metrics 指针，避免在打点路径长期持有锁。
func (h *Hub) metricsSnapshot() HubMetrics {
	if h == nil {
		return nil
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.metrics
}

// BroadcastSessionEnd 把 LiveSessionEnded 事件推送给所有订阅了该 liveSessionId 的客户端，
// 并把它们从 session 反查表中移除。HTTP 层的 ws_handler 会随后通过 Unsubscribe 把客户端从所属 room 中清理掉。
//
// 返回投递成功的客户端数量。
func (h *Hub) BroadcastSessionEnd(liveSessionID uint64, payload json.RawMessage) int {
	if h == nil || liveSessionID == 0 {
		return 0
	}
	h.mu.Lock()
	bucket := h.sessionClients[liveSessionID]
	clients := make([]*Client, 0, len(bucket))
	for _, c := range bucket {
		clients = append(clients, c)
	}
	delete(h.sessionClients, liveSessionID)
	h.mu.Unlock()
	if len(clients) == 0 {
		return 0
	}
	env := Envelope{Type: "live_session.ended", LiveSessionID: liveSessionID, Payload: payload}
	delivered := 0
	for _, c := range clients {
		if c.Deliver(env) {
			delivered++
		}
	}
	return delivered
}

// SessionClientCount 返回某 liveSessionId 下当前订阅的客户端数量（仅本实例视角）。
func (h *Hub) SessionClientCount(liveSessionID uint64) int {
	if h == nil || liveSessionID == 0 {
		return 0
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.sessionClients[liveSessionID])
}

// HubStats 描述 Hub 的实例级运行状态，对外暴露用于运维 / 调试入口。
type HubStats struct {
	Rooms          int            `json:"rooms"`
	Clients        int            `json:"clients"`
	LiveSessions   int            `json:"liveSessions"`
	SessionClients map[uint64]int `json:"liveSessionId,omitempty"`
}

// Stats 返回 Hub 的实例级状态快照。SessionClients map 的 key 是 liveSessionId，value 是该 session 下挂着的客户端数。
func (h *Hub) Stats() HubStats {
	if h == nil {
		return HubStats{}
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	stats := HubStats{
		Rooms:          len(h.rooms),
		LiveSessions:   len(h.sessionClients),
		SessionClients: make(map[uint64]int, len(h.sessionClients)),
	}
	for _, room := range h.rooms {
		stats.Clients += room.ClientCount()
	}
	for sessionID, bucket := range h.sessionClients {
		stats.SessionClients[sessionID] = len(bucket)
	}
	return stats
}

// registerSessionClient 把 client.LiveSessionID -> client 加入反查表；为 0 时跳过。
func (h *Hub) registerSessionClient(client *Client) {
	if client == nil || client.LiveSessionID == 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	bucket := h.sessionClients[client.LiveSessionID]
	if bucket == nil {
		bucket = make(map[string]*Client)
		h.sessionClients[client.LiveSessionID] = bucket
	}
	bucket[client.ID] = client
}

func (h *Hub) unregisterSessionClient(sessionID uint64, clientID string) {
	if sessionID == 0 || clientID == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	bucket, ok := h.sessionClients[sessionID]
	if !ok {
		return
	}
	delete(bucket, clientID)
	if len(bucket) == 0 {
		delete(h.sessionClients, sessionID)
	}
}

// unregisterSessionClientByID 在不知道 sessionID 的情况下兜底清理所有 bucket 中匹配 clientID 的条目，
// 避免 slow_consumer 关闭路径漏删反查表项。
func (h *Hub) unregisterSessionClientByID(clientID string) {
	if clientID == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for sessionID, bucket := range h.sessionClients {
		if _, ok := bucket[clientID]; ok {
			delete(bucket, clientID)
			if len(bucket) == 0 {
				delete(h.sessionClients, sessionID)
			}
		}
	}
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
	userID := ""
	if ok {
		fallback = room.OnlineClientCount()
		countOnline, foundUserID := room.ClientPresence(clientID)
		if !countOnline {
			return h.onlineCount(auctionID, fallback)
		}
		userID = foundUserID
	}
	if h.shouldSkipOnlineTouch(auctionID, clientID) {
		return fallback
	}
	return h.touchOnline(auctionID, clientID, userID, fallback)
}

func (h *Hub) OnlineCount(auctionID uint64) int {
	room, ok := h.Room(auctionID)
	if !ok {
		return h.onlineCount(auctionID, 0)
	}
	return h.onlineCount(auctionID, room.OnlineClientCount())
}

func (h *Hub) joinOnline(auctionID uint64, clientID, userID string, fallback int) int {
	if h.onlineCounter == nil {
		return fallback
	}
	ctx, cancel := h.onlineCounterContext()
	defer cancel()
	count, err := h.onlineCounter.Join(ctx, auctionID, h.onlineMemberID(clientID), userID)
	if err != nil || count < 0 {
		return fallback
	}
	return count
}

func (h *Hub) leaveOnline(auctionID uint64, clientID, userID string, fallback int) int {
	h.clearOnlineTouched(auctionID, clientID)
	if h.onlineCounter == nil {
		return fallback
	}
	ctx, cancel := h.onlineCounterContext()
	defer cancel()
	count, err := h.onlineCounter.Leave(ctx, auctionID, h.onlineMemberID(clientID), userID)
	if err != nil || count < 0 {
		return fallback
	}
	return count
}

func (h *Hub) touchOnline(auctionID uint64, clientID, userID string, fallback int) int {
	if h.onlineCounter == nil {
		return fallback
	}
	ctx, cancel := h.onlineCounterContext()
	defer cancel()
	count, err := h.onlineCounter.Touch(ctx, auctionID, h.onlineMemberID(clientID), userID)
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

func (h *Hub) markOnlineTouched(auctionID uint64, clientID string) {
	if h == nil || auctionID == 0 || clientID == "" {
		return
	}
	h.onlineTouchMu.Lock()
	defer h.onlineTouchMu.Unlock()
	if h.onlineTouched == nil {
		h.onlineTouched = make(map[string]time.Time)
	}
	h.onlineTouched[h.onlineTouchKey(auctionID, clientID)] = time.Now()
}

func (h *Hub) clearOnlineTouched(auctionID uint64, clientID string) {
	if h == nil || auctionID == 0 || clientID == "" {
		return
	}
	h.onlineTouchMu.Lock()
	defer h.onlineTouchMu.Unlock()
	delete(h.onlineTouched, h.onlineTouchKey(auctionID, clientID))
}

func (h *Hub) shouldSkipOnlineTouch(auctionID uint64, clientID string) bool {
	if h == nil || auctionID == 0 || clientID == "" {
		return false
	}
	now := time.Now()
	h.onlineTouchMu.Lock()
	defer h.onlineTouchMu.Unlock()
	if h.onlineTouchTTL <= 0 {
		return false
	}
	if h.onlineTouched == nil {
		h.onlineTouched = make(map[string]time.Time)
	}
	key := h.onlineTouchKey(auctionID, clientID)
	if last, ok := h.onlineTouched[key]; ok && now.Sub(last) < h.onlineTouchTTL {
		return true
	}
	h.onlineTouched[key] = now
	return false
}

func (h *Hub) onlineTouchKey(auctionID uint64, clientID string) string {
	return strconv.FormatUint(auctionID, 10) + ":" + clientID
}

func (h *Hub) broadcastPresence(room *Room, online int) {
	if room == nil {
		return
	}
	removed := room.BroadcastPresence(online)
	for attempts := 0; len(removed) > 0 && attempts < 3; attempts++ {
		for _, client := range removed {
			if client.CountOnline {
				_ = h.leaveOnline(room.AuctionID, client.ID, client.UserID, room.OnlineClientCount())
			}
			if client.LiveSessionID != 0 {
				h.unregisterSessionClient(client.LiveSessionID, client.ID)
			} else {
				h.unregisterSessionClientByID(client.ID)
			}
		}
		removed = room.BroadcastPresence(h.onlineCount(room.AuctionID, room.OnlineClientCount()))
	}
}

func (h *Hub) emitPresence(room *Room, online int) {
	if h == nil || room == nil {
		return
	}
	if h.shouldBroadcastPresenceImmediately(room) {
		h.cancelScheduledPresence(room.AuctionID)
		h.broadcastPresence(room, online)
		return
	}
	h.schedulePresence(room)
}

func (h *Hub) shouldBroadcastPresenceImmediately(room *Room) bool {
	h.presenceMu.Lock()
	limit := h.presenceLimit
	h.presenceMu.Unlock()
	if limit <= 0 {
		return true
	}
	return room.ClientCount() <= limit
}

func (h *Hub) schedulePresence(room *Room) {
	h.presenceMu.Lock()
	delay := h.presenceDelay
	if delay <= 0 {
		h.presenceMu.Unlock()
		online := h.onlineCount(room.AuctionID, room.OnlineClientCount())
		h.broadcastPresence(room, online)
		return
	}
	if h.presenceTimers == nil {
		h.presenceTimers = make(map[uint64]*presenceUpdate)
	}
	update := h.presenceTimers[room.AuctionID]
	if update == nil {
		update = &presenceUpdate{}
		h.presenceTimers[room.AuctionID] = update
	}
	update.room = room
	if update.timer != nil {
		h.presenceMu.Unlock()
		return
	}
	auctionID := room.AuctionID
	update.timer = time.AfterFunc(delay, func() {
		h.flushPresence(auctionID)
	})
	h.presenceMu.Unlock()
}

func (h *Hub) flushPresence(auctionID uint64) {
	h.presenceMu.Lock()
	update := h.presenceTimers[auctionID]
	if update == nil {
		h.presenceMu.Unlock()
		return
	}
	delete(h.presenceTimers, auctionID)
	room := update.room
	h.presenceMu.Unlock()
	if room == nil {
		return
	}
	online := h.onlineCount(auctionID, room.OnlineClientCount())
	h.broadcastPresence(room, online)
}

func (h *Hub) cancelScheduledPresence(auctionID uint64) {
	h.presenceMu.Lock()
	defer h.presenceMu.Unlock()
	update := h.presenceTimers[auctionID]
	if update == nil {
		return
	}
	if update.timer != nil {
		update.timer.Stop()
	}
	delete(h.presenceTimers, auctionID)
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
		if env.Type == "heartbeat" && client.CountOnline {
			h.Touch(client.AuctionID, client.ID)
		}
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
	removed, _, _, _ := r.removeReturning(clientID)
	return removed
}

// removeReturning 与 Remove 一致，但额外返回该 client 关联的 LiveSessionID，
// 供 Hub 同步清理 session 反查表。
func (r *Room) removeReturning(clientID string) (bool, uint64, bool, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if client := r.clients[clientID]; client != nil {
		sessionID := client.LiveSessionID
		countOnline := client.CountOnline
		userID := client.UserID
		client.CloseWithReason("unsubscribe")
		delete(r.clients, clientID)
		r.disconnects.Add(1)
		return true, sessionID, countOnline, userID
	}
	return false, 0, false, ""
}

func (r *Room) removeClientReturning(target *Client) (bool, uint64, bool, string) {
	if target == nil {
		return false, 0, false, ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	client := r.clients[target.ID]
	if client == nil || client != target {
		return false, 0, false, ""
	}
	sessionID := client.LiveSessionID
	countOnline := client.CountOnline
	userID := client.UserID
	client.CloseWithReason("unsubscribe")
	delete(r.clients, client.ID)
	r.disconnects.Add(1)
	return true, sessionID, countOnline, userID
}

func (r *Room) Broadcast(env Envelope) (int, []removedClient) {
	r.mu.Lock()
	if r.isDuplicatePubEventLocked(env) {
		r.mu.Unlock()
		return 0, nil
	}
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
	var slow []removedClient
	for _, client := range clients {
		if client.Deliver(env) {
			delivered++
		} else if client.Closed() {
			slow = append(slow, removedClient{ID: client.ID, UserID: client.UserID, LiveSessionID: client.LiveSessionID, CountOnline: client.CountOnline})
		}
	}
	if len(slow) > 0 {
		r.mu.Lock()
		removed := slow[:0]
		for _, slowClient := range slow {
			if client := r.clients[slowClient.ID]; client != nil && client.Closed() {
				delete(r.clients, slowClient.ID)
				r.disconnects.Add(1)
				removed = append(removed, slowClient)
			}
		}
		r.mu.Unlock()
		slow = removed
	}
	return delivered, slow
}

func (r *Room) isDuplicatePubEventLocked(env Envelope) bool {
	if env.Seq <= 0 || !dedupeByPubSeq(env.Type) {
		return false
	}
	for {
		current := r.pubSeq.Load()
		if env.Seq <= current {
			return true
		}
		if r.pubSeq.CompareAndSwap(current, env.Seq) {
			return false
		}
	}
}

func dedupeByPubSeq(eventType string) bool {
	switch eventType {
	case "bid.accepted", "bid.rejected":
		return true
	default:
		return false
	}
}

func (r *Room) ClientCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.clients)
}

func (r *Room) OnlineClientCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	count := 0
	for _, client := range r.clients {
		if client.CountOnline {
			count++
		}
	}
	return count
}

func (r *Room) ClientPresence(clientID string) (bool, string) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	client := r.clients[clientID]
	if client == nil {
		return false, ""
	}
	return client.CountOnline, client.UserID
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

func (r *Room) BroadcastPresence(online int) []removedClient {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.broadcastPresenceLocked(online)
}

func (r *Room) broadcastPresenceLocked(online int) []removedClient {
	if online < 0 {
		online = 0
	}
	payload, _ := json.Marshal(map[string]interface{}{"auctionId": r.AuctionID, "online": online})
	env := Envelope{Type: "room.online", Seq: r.NextSeq(), Payload: payload}
	r.appendHistoryLocked(env)
	removed := make([]removedClient, 0)
	for _, client := range r.clients {
		if !client.Deliver(env) && client.Closed() {
			delete(r.clients, client.ID)
			r.disconnects.Add(1)
			removed = append(removed, removedClient{ID: client.ID, UserID: client.UserID, LiveSessionID: client.LiveSessionID, CountOnline: client.CountOnline})
		}
	}
	return removed
}

func (r *Room) NextSeq() int64 {
	return r.seq.Add(1)
}

// CurrentSeq 返回当前 Room 已经分发过的最大 seq；客户端用它作为
// "已对齐到此点"的水位线（snapshot 帧用此值），后续广播 seq>this 即增量。
// 0 表示房间尚未分发过任何事件。
func (r *Room) CurrentSeq() int64 {
	return r.seq.Load()
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

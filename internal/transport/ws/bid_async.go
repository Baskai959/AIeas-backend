package ws

import (
	"encoding/json"
	"strings"
	"sync"
	"time"
)

// 异步竞价队列保护 / 结果重发的默认参数（MVP，进程内）。
const (
	defaultMaxPendingPerAuction = 500
	defaultResultAckTimeout     = 2 * time.Second
	defaultMaxResendAttempts    = 3
)

// 队列保护拒因常量（回给前端的 bid.ack.reason）。
const (
	BidQueueRejectHotAuctionFull = "HOT_AUCTION_QUEUE_FULL"
	BidQueueRejectUserPending    = "USER_BID_ALREADY_PENDING"
)

// BidAsyncMetrics 是异步竞价协调器在打点路径上依赖的最小指标接口，
// 避免 ws 包反向依赖具体 metrics.Registry。所有实现需 nil 安全。
type BidAsyncMetrics interface {
	SetBidPendingQueueSize(size int)
	ObserveBidResultPush(elapsed time.Duration)
	ObserveBidResultDuration(outcome string, elapsed time.Duration)
	IncBidResultAckTimeout()
}

// bidPendingEntry 是一条待裁决/待 ack 的进程内记录。
type bidPendingEntry struct {
	bidID     string
	auctionID uint64
	sessionID uint64
	userID    string
	createdAt time.Time
	env       Envelope // 已裁决的 bid.result 帧，用于重发；裁决前为零值。
	resolved  bool     // 是否已收到裁决结果（worker 推送 bid.result 后置 true）。
	attempts  int      // 已重发次数。
	timer     *time.Timer
}

// BidAsyncCoordinator 维护异步竞价的进程内状态：
//   - per-auction 待裁决计数（队列保护）；
//   - per-(user,auction) pending 标记（防止同用户同拍品重复在途）；
//   - 裁决结果的定向推送 + ack 超时重发。
//
// 释放时机：收到 bid.result.ack / 重发超限 / 连接断开任一触发。
type BidAsyncCoordinator struct {
	hub     *Hub
	metrics BidAsyncMetrics

	maxPendingPerAuction int
	ackTimeout           time.Duration
	maxResendAttempts    int

	mu             sync.Mutex
	pending        map[string]*bidPendingEntry // bidID -> entry
	auctionCount   map[uint64]int              // auctionID -> 待裁决数
	userAuctionKey map[string]string           // "userID:auctionID" -> bidID
}

// NewBidAsyncCoordinator 构造协调器。hub 用于定向推送 bid.result。
// 任一参数<=0 时使用默认值。
func NewBidAsyncCoordinator(hub *Hub, maxPendingPerAuction int, ackTimeout time.Duration, maxResendAttempts int) *BidAsyncCoordinator {
	if maxPendingPerAuction <= 0 {
		maxPendingPerAuction = defaultMaxPendingPerAuction
	}
	if ackTimeout <= 0 {
		ackTimeout = defaultResultAckTimeout
	}
	if maxResendAttempts <= 0 {
		maxResendAttempts = defaultMaxResendAttempts
	}
	return &BidAsyncCoordinator{
		hub:                  hub,
		maxPendingPerAuction: maxPendingPerAuction,
		ackTimeout:           ackTimeout,
		maxResendAttempts:    maxResendAttempts,
		pending:              make(map[string]*bidPendingEntry),
		auctionCount:         make(map[uint64]int),
		userAuctionKey:       make(map[string]string),
	}
}

// SetMetrics 注入观测性指标实现。nil 安全。
func (c *BidAsyncCoordinator) SetMetrics(m BidAsyncMetrics) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.metrics = m
}

func userAuctionPendingKey(userID string, auctionID uint64) string {
	return userID + ":" + uint64ToString(auctionID)
}

func uint64ToString(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

// TryEnqueue 在入队前做队列保护检查并登记 pending。
// 返回 ok=false 时 reason 为拒因（HOT_AUCTION_QUEUE_FULL / USER_BID_ALREADY_PENDING）。
func (c *BidAsyncCoordinator) TryEnqueue(auctionID, sessionID uint64, userID, bidID string) (bool, string) {
	if c == nil {
		return true, ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	uaKey := userAuctionPendingKey(userID, auctionID)
	if _, ok := c.userAuctionKey[uaKey]; ok {
		return false, BidQueueRejectUserPending
	}
	if c.auctionCount[auctionID] >= c.maxPendingPerAuction {
		return false, BidQueueRejectHotAuctionFull
	}
	c.pending[bidID] = &bidPendingEntry{
		bidID:     bidID,
		auctionID: auctionID,
		sessionID: sessionID,
		userID:    userID,
		createdAt: time.Now(),
	}
	c.auctionCount[auctionID]++
	c.userAuctionKey[uaKey] = bidID
	c.updateGaugeLocked()
	return true, ""
}

// OnDecision 在 worker 完成裁决后定向推送 bid.result，并启动 ack 超时重发。
// 若该 bidID 不在 pending（已被释放/未登记），仍尽力推送一次但不登记重发。
func (c *BidAsyncCoordinator) OnDecision(sessionID, auctionID uint64, userID, bidID string, env Envelope) {
	if c == nil {
		return
	}
	var queuedAt time.Time
	var recordResultDuration bool
	c.mu.Lock()
	entry, ok := c.pending[bidID]
	if !ok {
		c.mu.Unlock()
		c.deliver(sessionID, userID, env)
		return
	}
	if entry.sessionID == 0 {
		entry.sessionID = sessionID
	}
	queuedAt = entry.createdAt
	recordResultDuration = !queuedAt.IsZero()
	entry.env = env
	entry.resolved = true
	c.armResendLocked(entry)
	c.mu.Unlock()
	c.deliver(sessionID, userID, env)
	if recordResultDuration && c.metrics != nil {
		c.metrics.ObserveBidResultDuration(bidResultOutcome(env), time.Since(queuedAt))
	}
}

// HandleAck 收到 bid.result.ack：停止重发并释放 pending。
func (c *BidAsyncCoordinator) HandleAck(bidID string) {
	if c == nil || bidID == "" {
		return
	}
	c.mu.Lock()
	c.releaseLocked(bidID)
	c.mu.Unlock()
}

// ReleaseUser 在连接断开时释放该用户在某直播场次内的全部 pending，避免计数泄漏。
func (c *BidAsyncCoordinator) ReleaseUser(userID string) {
	if c == nil || userID == "" {
		return
	}
	c.mu.Lock()
	ids := make([]string, 0)
	for bidID, entry := range c.pending {
		if entry.userID == userID {
			ids = append(ids, bidID)
		}
	}
	for _, bidID := range ids {
		c.releaseLocked(bidID)
	}
	c.mu.Unlock()
}

// PendingQueueSize 返回当前全局待裁决总数（用于 gauge / 测试断言）。
func (c *BidAsyncCoordinator) PendingQueueSize() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

// PendingForAuction 返回某 auction 当前的待裁决数。屏障判断是否可以真正落锤
// 使用：pending=0 表示 in-flight 全部排空（已 publish 未裁决的命令、worker 处理中、
// 已 deliver 但等待 ack 的全部计入）。
func (c *BidAsyncCoordinator) PendingForAuction(auctionID uint64) int {
	if c == nil || auctionID == 0 {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.auctionCount[auctionID]
}

func (c *BidAsyncCoordinator) deliver(sessionID uint64, userID string, env Envelope) {
	if c.hub == nil || sessionID == 0 {
		return
	}
	start := time.Now()
	c.hub.DeliverToUserInSession(sessionID, userID, env)
	if c.metrics != nil {
		c.metrics.ObserveBidResultPush(time.Since(start))
	}
}

func bidResultOutcome(env Envelope) string {
	var payload struct {
		FinalStatus string `json:"finalStatus"`
		Accepted    bool   `json:"accepted"`
	}
	if len(env.Payload) > 0 {
		_ = json.Unmarshal(env.Payload, &payload)
	}
	switch strings.ToUpper(strings.TrimSpace(payload.FinalStatus)) {
	case "ACCEPTED", "ALLOW", "ALLOWED":
		return "accepted"
	case "REJECTED":
		return "rejected"
	}
	if payload.Accepted {
		return "accepted"
	}
	return "unknown"
}

// armResendLocked 在持锁状态下安排一次 ack 超时重发。
func (c *BidAsyncCoordinator) armResendLocked(entry *bidPendingEntry) {
	if entry == nil {
		return
	}
	if entry.timer != nil {
		entry.timer.Stop()
	}
	bidID := entry.bidID
	entry.timer = time.AfterFunc(c.ackTimeout, func() {
		c.onResendTimeout(bidID)
	})
}

func (c *BidAsyncCoordinator) onResendTimeout(bidID string) {
	c.mu.Lock()
	entry, ok := c.pending[bidID]
	if !ok || !entry.resolved {
		c.mu.Unlock()
		return
	}
	entry.attempts++
	if entry.attempts > c.maxResendAttempts {
		// 超限：停止重发并释放队列计数（MVP 进程内不再保留重发态）。
		// TODO: 可增强为把终态结果留存到 Redis，支持跨进程/重连补偿。
		c.releaseLocked(bidID)
		c.mu.Unlock()
		if c.metrics != nil {
			c.metrics.IncBidResultAckTimeout()
		}
		return
	}
	sessionID := entry.sessionID
	userID := entry.userID
	env := entry.env
	c.armResendLocked(entry)
	c.mu.Unlock()
	if c.metrics != nil {
		c.metrics.IncBidResultAckTimeout()
	}
	c.deliver(sessionID, userID, env)
}

// releaseLocked 释放某 bidID 的全部状态（持锁）。
func (c *BidAsyncCoordinator) releaseLocked(bidID string) {
	entry, ok := c.pending[bidID]
	if !ok {
		return
	}
	if entry.timer != nil {
		entry.timer.Stop()
		entry.timer = nil
	}
	delete(c.pending, bidID)
	if c.auctionCount[entry.auctionID] > 0 {
		c.auctionCount[entry.auctionID]--
		if c.auctionCount[entry.auctionID] == 0 {
			delete(c.auctionCount, entry.auctionID)
		}
	}
	delete(c.userAuctionKey, userAuctionPendingKey(entry.userID, entry.auctionID))
	c.updateGaugeLocked()
}

func (c *BidAsyncCoordinator) updateGaugeLocked() {
	if c.metrics != nil {
		c.metrics.SetBidPendingQueueSize(len(c.pending))
	}
}

// BidResultPayload 是 bid.result 点对点帧的 payload 结构。
type BidResultPayload struct {
	BidID          string `json:"bidId"`
	AuctionID      uint64 `json:"auctionId"`
	FinalStatus    string `json:"finalStatus"`
	Reason         string `json:"reason,omitempty"`
	CurrentPrice   int64  `json:"currentPrice"`
	LeaderBidderID string `json:"leaderBidderId,omitempty"`
	EndTimeMS      int64  `json:"endTimeMs"`
	ServerTimeMS   int64  `json:"serverTimeMs"`
	ResultSeq      int64  `json:"resultSeq,omitempty"`
}

// BuildBidResultEnvelope 构造一帧 bid.result 信封。requestID 用 bidID 便于前端关联。
func BuildBidResultEnvelope(p BidResultPayload) Envelope {
	raw, _ := json.Marshal(p)
	return Envelope{Type: TypeBidResult, RequestID: p.BidID, Payload: raw}
}

// DeliverBidResult 是 worker 侧便捷入口：构造 bid.result 帧并定向推送 + 登记重发。
func (c *BidAsyncCoordinator) DeliverBidResult(sessionID, auctionID uint64, userID string, p BidResultPayload) {
	if c == nil {
		return
	}
	env := BuildBidResultEnvelope(p)
	c.OnDecision(sessionID, auctionID, userID, p.BidID, env)
}

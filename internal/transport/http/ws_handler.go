package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"aieas_backend/internal/domain"
	auctionapp "aieas_backend/internal/modules/auction/app"
	corews "aieas_backend/internal/transport/ws"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/hertz-contrib/websocket"
)

const (
	defaultWebsocketWriteTimeout = 5 * time.Second
	maxWSChatContentRunes        = 500
)

var nextWSClientSeq atomic.Uint64

type wsUint64 uint64

func (id *wsUint64) UnmarshalJSON(data []byte) error {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" || trimmed == "null" {
		*id = 0
		return nil
	}
	if strings.HasPrefix(trimmed, "\"") {
		var raw string
		if err := json.Unmarshal(data, &raw); err != nil {
			return err
		}
		raw = strings.TrimSpace(raw)
		if raw == "" {
			*id = 0
			return nil
		}
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return err
		}
		*id = wsUint64(parsed)
		return nil
	}
	var parsed uint64
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}
	*id = wsUint64(parsed)
	return nil
}

type WSBidPlaceMode string

const (
	WSBidPlaceLocal    WSBidPlaceMode = "local"
	WSBidPlaceAsync    WSBidPlaceMode = "async"
	WSBidPlaceDisabled WSBidPlaceMode = "disabled"
)

type WSLiveSessionLookupMode string

const (
	WSLiveSessionLookupService  WSLiveSessionLookupMode = "service"
	WSLiveSessionLookupRealtime WSLiveSessionLookupMode = "realtime"
)

type WSHandler struct {
	hub              *corews.Hub
	bids             WSBidUseCase
	rankings         WSAuctionRankingUseCase
	sessions         WSLiveSessionLookupUseCase
	auctions         WSAuctionStateUseCase
	realtimeSnapshot WSAuctionRealtimeSnapshotProvider
	sessionRealtime  WSLiveSessionRealtimeReader
	bidPlaceMode     WSBidPlaceMode
	allowDBSnapshot  bool
	sessionLookup    WSLiveSessionLookupMode
	// 异步竞价（aieas.bid.commands）相关依赖。async 模式仅在 asyncBids 与
	// cmdPublisher 均就绪时生效，否则强制降级同步（绝不丢请求）。
	asyncBids        WSAsyncBidUseCase
	cmdPublisher     BidCommandPublisher
	asyncCoord       *corews.BidAsyncCoordinator
	bidMetrics       WSBidModeMetrics
	sendBufferSize   int
	readLimitBytes   int
	pingInterval     time.Duration
	pongTimeout      time.Duration
	writeTimeout     time.Duration
	closeGrace       time.Duration
	pingJitter       time.Duration
	handshakeLimiter *corews.HandshakeLimiter
	metrics          corews.HubMetrics
	upgrader         websocket.HertzUpgrader
}

// NewWSHandler 构造 WSHandler。pingJitter / handshakeLimiter / metrics 任一可
// 为零值（0 / nil），handler 内部按 nil-safe 路径处理：
//   - pingJitter<=0 时不做初始 ping 错峰；
//   - handshakeLimiter==nil 时跳过握手限流；
//   - metrics==nil 时跳过握手拒绝打点。
func NewWSHandler(
	hub *corews.Hub,
	bids WSBidUseCase,
	sendBufferSize, readLimitBytes int,
	pingInterval, pongTimeout time.Duration,
	writeTimeout, pingJitter, closeGrace time.Duration,
	limiter *corews.HandshakeLimiter,
	metrics corews.HubMetrics,
) *WSHandler {
	if sendBufferSize <= 0 {
		sendBufferSize = 64
	}
	if readLimitBytes <= 0 {
		readLimitBytes = 65536
	}
	if pingInterval <= 0 {
		pingInterval = 20 * time.Second
	}
	if pongTimeout <= 0 {
		pongTimeout = 60 * time.Second
	}
	if writeTimeout <= 0 {
		writeTimeout = defaultWebsocketWriteTimeout
	}
	if pingJitter < 0 {
		pingJitter = 0
	}
	if closeGrace <= 0 {
		closeGrace = time.Second
	}
	return &WSHandler{
		hub:              hub,
		bids:             bids,
		bidPlaceMode:     WSBidPlaceLocal,
		allowDBSnapshot:  true,
		sessionLookup:    WSLiveSessionLookupService,
		sendBufferSize:   sendBufferSize,
		readLimitBytes:   readLimitBytes,
		pingInterval:     pingInterval,
		pongTimeout:      pongTimeout,
		writeTimeout:     writeTimeout,
		closeGrace:       closeGrace,
		pingJitter:       pingJitter,
		handshakeLimiter: limiter,
		metrics:          metrics,
		upgrader: websocket.HertzUpgrader{
			ReadBufferSize:  readLimitBytes,
			WriteBufferSize: readLimitBytes,
			CheckOrigin:     func(ctx *app.RequestContext) bool { return true },
		},
	}
}

// SetLiveSessionService 注入直播场次服务以支持 /ws/live-sessions/:session_id 入口。
func (h *WSHandler) SetLiveSessionService(sessions WSLiveSessionLookupUseCase) {
	h.sessions = sessions
}

// SetAuctionService 注入拍卖服务以在握手后下发 room.snapshot（P1-B）。
// 未注入时握手成功仍可继续，但跳过 snapshot 帧（保留原有读写循环）。
func (h *WSHandler) SetAuctionService(auctions WSAuctionStateUseCase) {
	h.auctions = auctions
}

func (h *WSHandler) SetAuctionRankingService(rankings WSAuctionRankingUseCase) {
	h.rankings = rankings
}

func (h *WSHandler) SetRealtimeSnapshotProvider(store WSAuctionRealtimeSnapshotProvider) {
	h.realtimeSnapshot = store
}

func (h *WSHandler) SetLiveSessionRealtimeStore(store WSLiveSessionRealtimeReader) {
	h.sessionRealtime = store
}

func (h *WSHandler) SetBidPlaceMode(mode WSBidPlaceMode) {
	if mode == "" {
		mode = WSBidPlaceLocal
	}
	h.bidPlaceMode = mode
}

// SetAsyncBidDependencies 注入异步竞价依赖：preCheck 入口、命令发布器、进程内协调器。
// 任一为 nil 时 async 模式自动降级为同步（绝不丢请求）。
func (h *WSHandler) SetAsyncBidDependencies(bids WSAsyncBidUseCase, publisher BidCommandPublisher, coord *corews.BidAsyncCoordinator) {
	h.asyncBids = bids
	h.cmdPublisher = publisher
	h.asyncCoord = coord
}

// asyncReady 报告 async 链路是否完整可用。硬约束降级：缺任一依赖即走同步。
func (h *WSHandler) asyncReady() bool {
	return h.asyncBids != nil && h.cmdPublisher != nil && h.asyncCoord != nil
}

// WSBidModeMetrics 是 ws handler 在竞价模式打点路径上依赖的最小指标接口。
// 所有实现需 nil 安全。
type WSBidModeMetrics interface {
	IncBidPlaceMode(mode string)
	ObserveBidAckDuration(mode, result string, elapsed time.Duration)
	ObserveBidKafkaEnqueue(elapsed time.Duration)
	IncBidQueueReject(reason string)
}

// SetBidModeMetrics 注入竞价模式打点实现。nil 安全。
func (h *WSHandler) SetBidModeMetrics(m WSBidModeMetrics) {
	h.bidMetrics = m
}

func (h *WSHandler) recordBidPlaceMode(mode string) {
	if h == nil || h.bidMetrics == nil {
		return
	}
	h.bidMetrics.IncBidPlaceMode(mode)
}

func (h *WSHandler) recordBidQueueReject(reason string) {
	if h == nil || h.bidMetrics == nil {
		return
	}
	h.bidMetrics.IncBidQueueReject(reason)
}

func (h *WSHandler) recordBidEnqueueDuration(elapsed time.Duration) {
	if h == nil || h.bidMetrics == nil {
		return
	}
	h.bidMetrics.ObserveBidKafkaEnqueue(elapsed)
}

func (h *WSHandler) recordBidAckDuration(mode, result string, elapsed time.Duration) {
	if h == nil || h.bidMetrics == nil {
		return
	}
	h.bidMetrics.ObserveBidAckDuration(mode, result, elapsed)
}

func (h *WSHandler) SetAllowDBSnapshotFallback(allow bool) {
	h.allowDBSnapshot = allow
}

func (h *WSHandler) SetLiveSessionLookupMode(mode WSLiveSessionLookupMode) {
	if mode == "" {
		mode = WSLiveSessionLookupService
	}
	h.sessionLookup = mode
}

// LiveSession 处理 `/ws/live-sessions/:session_id` 和 `/ws/live-rooms/:room_id`
// 入口，将客户端订阅到场次当前活跃拍品事件。
func (h *WSHandler) LiveSession(ctx context.Context, c *app.RequestContext) {
	sessionID, ok := parseLiveSessionWSParam(c)
	if !ok {
		return
	}
	if h.sessions == nil {
		WriteError(c, consts.StatusInternalServerError, 90001, "系统内部错误", nil)
		return
	}
	if h.hub != nil && h.hub.IsDraining() {
		h.recordHandshakeReject("draining")
		c.Response.Header.Set("Retry-After", "5")
		WriteError(c, consts.StatusServiceUnavailable, 50301, "网关排空中，请稍后重试", nil)
		return
	}
	auctionID, liveSessionID := uint64(0), sessionID
	if h.sessionLookup == WSLiveSessionLookupRealtime {
		if h.sessionRealtime == nil {
			WriteError(c, consts.StatusConflict, 20003, "直播场次实时状态不可用", nil)
			return
		}
		activeAuctionID, ok, err := h.sessionRealtime.ActiveAuction(ctx, sessionID)
		if err != nil {
			WriteError(c, consts.StatusConflict, 20003, "直播场次实时状态不可用", nil)
			return
		}
		if ok {
			auctionID = activeAuctionID
		}
	} else {
		var err error
		auctionID, liveSessionID, err = h.sessions.ActiveAuctionAndSession(ctx, sessionID)
		if err != nil {
			writeLiveSessionError(c, err)
			return
		}
	}
	clientID := nextWSClientID()
	userID := AuthUserID(c)
	if userID == "" {
		userID = "anonymous"
	}
	if h.handshakeLimiter != nil {
		if allowed, reason := h.handshakeLimiter.Allow(c.ClientIP(), userID, auctionID); !allowed {
			h.recordHandshakeReject(reason)
			c.Response.Header.Set("Retry-After", "60")
			WriteError(c, consts.StatusTooManyRequests, 42901, "握手频率过高，请稍后重试", nil)
			return
		}
	}
	role := AuthRole(c)
	client := corews.NewClientWithSession(clientID, userID, auctionID, liveSessionID, h.sendBufferSize)
	client.CountOnline = role == domain.RoleBuyer
	lastSeq := parseLastSeq(c)
	if err := h.upgrader.Upgrade(c, func(conn *websocket.Conn) {
		if auctionID == 0 {
			if err := h.hub.SubscribeLiveSessionOnly(liveSessionID, client); err != nil {
				_ = conn.Close()
				return
			}
		} else if err := h.hub.Subscribe(auctionID, client); err != nil {
			_ = conn.Close()
			return
		}
		h.serveConn(context.Background(), conn, client, lastSeq)
	}); err != nil {
		return
	}
}

func parseLiveSessionWSParam(c *app.RequestContext) (uint64, bool) {
	if strings.TrimSpace(c.Param("session_id")) != "" {
		return parseUintParam(c, "session_id")
	}
	return parseUintParam(c, "room_id")
}

func (h *WSHandler) Auction(ctx context.Context, c *app.RequestContext) {
	_ = ctx
	auctionID, ok := parseUintParam(c, "auction_id")
	if !ok {
		return
	}
	if h.hub != nil && h.hub.IsDraining() {
		h.recordHandshakeReject("draining")
		c.Response.Header.Set("Retry-After", "5")
		WriteError(c, consts.StatusServiceUnavailable, 50301, "网关排空中，请稍后重试", nil)
		return
	}
	clientID := nextWSClientID()
	userID := AuthUserID(c)
	if userID == "" {
		userID = "anonymous"
	}
	if h.handshakeLimiter != nil {
		if allowed, reason := h.handshakeLimiter.Allow(c.ClientIP(), userID, auctionID); !allowed {
			h.recordHandshakeReject(reason)
			c.Response.Header.Set("Retry-After", "60")
			WriteError(c, consts.StatusTooManyRequests, 42901, "握手频率过高，请稍后重试", nil)
			return
		}
	}
	role := AuthRole(c)
	client := corews.NewClient(clientID, userID, auctionID, h.sendBufferSize)
	client.CountOnline = role == domain.RoleBuyer
	lastSeq := parseLastSeq(c)
	if err := h.upgrader.Upgrade(c, func(conn *websocket.Conn) {
		if err := h.hub.Subscribe(auctionID, client); err != nil {
			_ = conn.Close()
			return
		}
		h.serveConn(context.Background(), conn, client, lastSeq)
	}); err != nil {
		return
	}
}

// recordHandshakeReject 在 metrics 注入存在时打点；nil-safe。
func (h *WSHandler) recordHandshakeReject(reason string) {
	if h == nil || h.metrics == nil {
		return
	}
	h.metrics.IncWSHandshakeReject(reason)
}

func (h *WSHandler) serveConn(ctx context.Context, conn *websocket.Conn, client *corews.Client, lastSeq int64) {
	defer func() {
		h.hub.UnsubscribeClient(client)
		if h.asyncCoord != nil && client != nil {
			h.asyncCoord.ReleaseUser(client.UserID)
		}
		_ = writeCloseFrameWithGrace(conn, client.CloseReason(), h.closeGrace)
		_ = conn.Close()
	}()
	conn.SetReadLimit(int64(h.readLimitBytes))
	_ = conn.SetReadDeadline(time.Now().Add(h.pongTimeout))
	conn.SetPongHandler(func(string) error {
		h.hub.TouchClient(client)
		return conn.SetReadDeadline(time.Now().Add(h.pongTimeout))
	})

	// P1-B：握手后立即下发房间快照，让客户端不必等首个 broadcast 才能渲染。
	// 优先走 RT (LoadState)，RT 不可用 / state 缺失时降级 MySQL，并在 payload
	// 中标记 degraded=true。snapshot 直接 deliver 到 client，不再走 Hub.Broadcast，
	// 避免 seq 占用与 history 写入。
	h.deliverRoomSnapshot(ctx, client)
	h.deliverInitialRanking(ctx, client)

	writeDone := make(chan struct{})
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				client.MarkSendFailure()
			}
			close(writeDone)
		}()
		h.replayMissed(client, lastSeq)
		var ticker <-chan time.Time
		var tickerStop func()
		startTicker := func() {
			if ticker != nil {
				return
			}
			t := time.NewTicker(h.pingInterval)
			ticker = t.C
			tickerStop = t.Stop
		}
		if jitter := h.initialPingJitter(client.ID); jitter > 0 {
			timer := time.NewTimer(jitter)
			stopTimer := func() {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			}
			for ticker == nil {
				select {
				case env, ok := <-client.Outbound():
					if !ok {
						stopTimer()
						return
					}
					if err := writeJSONWithDeadline(conn, env, h.writeTimeout); err != nil {
						stopTimer()
						client.MarkSendFailure()
						return
					}
				case <-timer.C:
					startTicker()
				}
			}
		} else {
			startTicker()
		}
		if tickerStop != nil {
			defer tickerStop()
		}
		for {
			select {
			case env, ok := <-client.Outbound():
				if !ok {
					return
				}
				if err := writeJSONWithDeadline(conn, env, h.writeTimeout); err != nil {
					client.MarkSendFailure()
					return
				}
			case <-ticker:
				if err := writePingWithDeadline(conn, h.writeTimeout); err != nil {
					client.MarkSendFailure()
					return
				}
			}
		}
	}()
	readDone := make(chan string, 1)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				client.MarkSendFailure()
				readDone <- "read_closed"
				return
			}
		}()
		for {
			messageType, payload, err := readMessage(conn)
			if err != nil {
				readDone <- readCloseReason(err)
				return
			}
			if messageType != websocket.TextMessage {
				continue
			}
			var env corews.Envelope
			if err := json.Unmarshal(payload, &env); err != nil {
				client.Deliver(corews.ErrorEnvelope("", "invalid json"))
				continue
			}
			for _, response := range h.handleInbound(ctx, client, env) {
				client.Deliver(response)
			}
		}
	}()

	select {
	case reason := <-readDone:
		client.CloseWithReason(reason)
		waitWriteDone(writeDone, h.writeTimeout)
	case <-writeDone:
		client.CloseWithReason("write_closed")
	}
}

func (h *WSHandler) initialPingJitter(clientID string) time.Duration {
	return h.initialPingJitterAt(clientID, time.Now())
}

func (h *WSHandler) initialPingJitterAt(clientID string, now time.Time) time.Duration {
	if h == nil || h.pingJitter <= 0 {
		return 0
	}
	seed := now.UnixNano()
	if clientID != "" {
		hash := fnv.New64a()
		_, _ = hash.Write([]byte(clientID))
		seed ^= int64(hash.Sum64())
	}
	return time.Duration(rand.New(rand.NewSource(seed)).Int63n(int64(h.pingJitter)))
}

func (h *WSHandler) replayMissed(client *corews.Client, lastSeq int64) {
	if lastSeq <= 0 {
		return
	}
	missed, complete := h.hub.ReplaySince(client.AuctionID, lastSeq)
	if !complete {
		client.Deliver(snapshotRequiredEnvelope(client.AuctionID, "EVENT_WINDOW_EXPIRED", map[string]interface{}{"lastSeq": lastSeq}))
		return
	}
	for _, env := range missed {
		client.Deliver(env)
	}
}

type websocketFrameWriter interface {
	SetWriteDeadline(time.Time) error
	WriteJSON(v interface{}) error
	WriteMessage(messageType int, data []byte) error
}

type websocketControlWriter interface {
	WriteControl(messageType int, data []byte, deadline time.Time) error
}

type websocketFrameReader interface {
	ReadMessage() (messageType int, p []byte, err error)
}

func readMessage(conn websocketFrameReader) (messageType int, payload []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("websocket read panic: %v", recovered)
		}
	}()
	return conn.ReadMessage()
}

func writeJSONWithDeadline(conn websocketFrameWriter, env corews.Envelope, timeout ...time.Duration) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("websocket write json panic: %v", recovered)
		}
	}()
	if err := conn.SetWriteDeadline(time.Now().Add(resolveWriteTimeout(timeout...))); err != nil {
		return err
	}
	return conn.WriteJSON(env)
}

func writePingWithDeadline(conn websocketFrameWriter, timeout ...time.Duration) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("websocket write ping panic: %v", recovered)
		}
	}()
	if err := conn.SetWriteDeadline(time.Now().Add(resolveWriteTimeout(timeout...))); err != nil {
		return err
	}
	return conn.WriteMessage(websocket.PingMessage, nil)
}

func writeCloseFrame(conn websocketControlWriter, reason string) (err error) {
	return writeCloseFrameWithGrace(conn, reason, time.Second)
}

func writeCloseFrameWithGrace(conn websocketControlWriter, reason string, grace time.Duration) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("websocket close frame panic: %v", recovered)
		}
	}()
	if reason == "" {
		reason = "closed"
	}
	if grace <= 0 {
		grace = time.Second
	}
	return conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(closeCodeForReason(reason), reason),
		time.Now().Add(grace),
	)
}

func waitWriteDone(writeDone <-chan struct{}, timeout ...time.Duration) {
	select {
	case <-writeDone:
	case <-time.After(resolveWriteTimeout(timeout...)):
	}
}

func resolveWriteTimeout(timeout ...time.Duration) time.Duration {
	if len(timeout) > 0 && timeout[0] > 0 {
		return timeout[0]
	}
	return defaultWebsocketWriteTimeout
}

func closeCodeForReason(reason string) int {
	switch reason {
	case "", "closed", "unsubscribe", "read_closed", "write_closed":
		return websocket.CloseNormalClosure
	case "slow_consumer":
		return websocket.ClosePolicyViolation
	case "pong_timeout", "gateway_draining":
		return websocket.CloseGoingAway
	default:
		return websocket.CloseInternalServerErr
	}
}

func readCloseReason(err error) string {
	if err == nil {
		return "read_closed"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "pong_timeout"
	}
	return "read_closed"
}

func nextWSClientID() string {
	return fmt.Sprintf("ws_%d_%d", time.Now().UnixNano(), nextWSClientSeq.Add(1))
}

// deliverRoomSnapshot 在握手完成后立即下发一帧 room.snapshot：
// 优先 RT (auctionService.State 内部已 RT-first → DB fallback)，并基于
// 返回的 Source=="redis"/"db" 推断是否退化。state 加载失败、auction 不存在
// 或 auctionService 未注入时，仅 best-effort 跳过——不阻断后续读写循环。
//
// snapshot 帧的 seq 字段填当前 Hub Room 的 CurrentSeq()，让客户端把它
// 当作"已对齐到此点"，后续 broadcast 只要 seq 严格递增即可保证不重不漏。
func (h *WSHandler) deliverRoomSnapshot(ctx context.Context, client *corews.Client) {
	if client == nil || client.AuctionID == 0 {
		return
	}
	if h.realtimeSnapshot != nil {
		if state, ok, err := h.realtimeSnapshot.GetAuctionState(ctx, client.AuctionID); err == nil && ok {
			h.deliverStateSnapshot(client, state, false)
			return
		} else if !h.allowDBSnapshot {
			reason := "REALTIME_SNAPSHOT_MISSING"
			if err != nil {
				reason = "REALTIME_SNAPSHOT_UNAVAILABLE"
			}
			client.Deliver(snapshotRequiredEnvelope(client.AuctionID, reason, nil))
			return
		}
	} else if !h.allowDBSnapshot {
		client.Deliver(snapshotRequiredEnvelope(client.AuctionID, "REALTIME_SNAPSHOT_UNAVAILABLE", nil))
		return
	}
	if h.auctions == nil {
		return
	}
	state, err := h.auctions.State(ctx, client.AuctionID, client.UserID, domain.RoleBuyer)
	if err != nil {
		if !h.allowDBSnapshot {
			client.Deliver(snapshotRequiredEnvelope(client.AuctionID, "REALTIME_SNAPSHOT_UNAVAILABLE", nil))
		}
		return
	}
	h.deliverStateSnapshot(client, state, strings.EqualFold(state.Source, "db"))
}

func (h *WSHandler) deliverStateSnapshot(client *corews.Client, state domain.AuctionState, degraded bool) {
	var seq int64
	if h.hub != nil {
		if room, ok := h.hub.Room(client.AuctionID); ok && room != nil {
			seq = room.CurrentSeq()
		}
	}
	serverTime := time.Now().UTC().UnixMilli()
	payload := map[string]interface{}{
		"auctionId":        state.AuctionID,
		"status":           state.Status,
		"startPrice":       state.StartPrice,
		"capPrice":         state.CapPrice,
		"currentPrice":     state.CurrentPrice,
		"leaderBidderId":   state.LeaderBidderID,
		"participantCount": state.ParticipantCount,
		"startTime":        state.StartTime.UTC().UnixMilli(),
		"endTime":          state.EndTime.UTC().UnixMilli(),
		"extendCount":      state.ExtendCount,
		"version":          state.Version,
		"seq":              seq,
		"serverTime":       serverTime,
		"source":           state.Source,
		"degraded":         degraded,
	}
	if len(state.IncrementRule) > 0 {
		payload["incrementRule"] = json.RawMessage(state.IncrementRule)
	}
	client.Deliver(jsonEnvelope(corews.TypeRoomSnapshot, "", payload))
}

func (h *WSHandler) deliverInitialRanking(ctx context.Context, client *corews.Client) {
	if client == nil || client.AuctionID == 0 || h.rankings == nil {
		return
	}
	state, ok := h.initialRankingAuctionState(ctx, client)
	if !ok || !shouldDeliverInitialRanking(state.Status) {
		return
	}
	ranking, err := h.rankings.TopN(ctx, client.AuctionID, 10)
	if err != nil {
		return
	}
	payload := map[string]interface{}{
		"auctionId": client.AuctionID,
		"ranking":   ranking,
	}
	if client.LiveSessionID != 0 {
		payload["liveSessionId"] = client.LiveSessionID
	}
	env := jsonEnvelope(corews.TypeRankingUpdated, "", payload)
	env.LiveSessionID = client.LiveSessionID
	client.Deliver(env)
}

func (h *WSHandler) initialRankingAuctionState(ctx context.Context, client *corews.Client) (domain.AuctionState, bool) {
	if h.realtimeSnapshot != nil {
		if state, ok, err := h.realtimeSnapshot.GetAuctionState(ctx, client.AuctionID); err == nil && ok {
			return state, true
		} else if !h.allowDBSnapshot {
			return domain.AuctionState{}, false
		}
	} else if !h.allowDBSnapshot {
		return domain.AuctionState{}, false
	}
	if h.auctions == nil {
		return domain.AuctionState{}, false
	}
	state, err := h.auctions.State(ctx, client.AuctionID, client.UserID, domain.RoleBuyer)
	if err != nil {
		return domain.AuctionState{}, false
	}
	return state, true
}

func shouldDeliverInitialRanking(status domain.AuctionStatus) bool {
	switch status {
	case domain.AuctionStatusRunning, domain.AuctionStatusExtended, domain.AuctionStatusHammerPending:
		return true
	default:
		return false
	}
}

func (h *WSHandler) handleInbound(ctx context.Context, client *corews.Client, env corews.Envelope) []corews.Envelope {
	switch env.Type {
	case "bid.place":
		start := time.Now()
		responses := h.handleBidPlace(ctx, client, env)
		mode, result := bidAckMetricLabels(h.bidPlaceMode, responses)
		h.recordBidAckDuration(mode, result, time.Since(start))
		return responses
	case "bid.result.ack":
		h.handleBidResultAck(env)
		return nil
	case "chat.send":
		return h.handleChatSend(client, env)
	case "room.subscribe":
		var payload struct {
			AuctionID wsUint64 `json:"auctionId"`
		}
		_ = json.Unmarshal(env.Payload, &payload)
		auctionID := uint64(payload.AuctionID)
		if auctionID == 0 {
			auctionID = client.AuctionID
		}
		_ = h.hub.Subscribe(auctionID, client)
		return []corews.Envelope{jsonEnvelope("room.subscribed", env.RequestID, map[string]interface{}{"auctionId": auctionID})}
	case "room.unsubscribe":
		var payload struct {
			AuctionID wsUint64 `json:"auctionId"`
		}
		_ = json.Unmarshal(env.Payload, &payload)
		auctionID := uint64(payload.AuctionID)
		if auctionID == 0 {
			auctionID = client.AuctionID
		}
		h.hub.Unsubscribe(auctionID, client.ID)
		return []corews.Envelope{jsonEnvelope("room.unsubscribed", env.RequestID, map[string]interface{}{"auctionId": auctionID})}
	case "heartbeat":
		if client.CountOnline {
			h.hub.Touch(client.AuctionID, client.ID)
		}
		return []corews.Envelope{jsonEnvelope("heartbeat.ack", env.RequestID, map[string]interface{}{"ts": time.Now().UTC().UnixMilli()})}
	case corews.TypeTimeSync:
		return []corews.Envelope{h.handleTimeSync(env)}
	default:
		return h.hub.HandleInbound(ctx, client, env)
	}
}

func (h *WSHandler) handleTimeSync(env corews.Envelope) corews.Envelope {
	requestID := strings.TrimSpace(env.RequestID)
	var payload struct {
		RequestID        string `json:"requestId"`
		ClientSendTimeMS int64  `json:"clientSendTimeMs"`
		ClientTimeMS     int64  `json:"clientTimeMs"`
	}
	if len(env.Payload) > 0 {
		_ = json.Unmarshal(env.Payload, &payload)
	}
	if requestID == "" {
		requestID = strings.TrimSpace(payload.RequestID)
	}
	now := time.Now().UTC()
	return jsonEnvelope(corews.TypeTimeSyncResult, requestID, map[string]interface{}{
		"requestId":        requestID,
		"clientSendTimeMs": payload.ClientSendTimeMS,
		"clientTimeMs":     payload.ClientTimeMS,
		"serverTime":       now.Format(time.RFC3339Nano),
		"serverTimeMs":     now.UnixMilli(),
	})
}

func bidAckMetricLabels(handlerMode WSBidPlaceMode, responses []corews.Envelope) (string, string) {
	mode := strings.ToLower(strings.TrimSpace(string(handlerMode)))
	if mode == "" {
		mode = "unknown"
	}
	result := "unknown"
	for _, response := range responses {
		if response.Type != "bid.ack" {
			continue
		}
		var payload struct {
			Mode     string `json:"mode"`
			Status   string `json:"status"`
			Accepted bool   `json:"accepted"`
		}
		if len(response.Payload) > 0 {
			_ = json.Unmarshal(response.Payload, &payload)
		}
		if payload.Mode != "" {
			mode = strings.ToLower(strings.TrimSpace(payload.Mode))
		}
		switch strings.ToUpper(strings.TrimSpace(payload.Status)) {
		case "QUEUED":
			result = "queued"
		case "REJECTED":
			result = "rejected"
		case "ACCEPTED", "ALLOW", "ALLOWED":
			result = "accepted"
		default:
			if payload.Accepted {
				result = "accepted"
			} else {
				result = "rejected"
			}
		}
		return mode, result
	}
	return mode, result
}

func (h *WSHandler) handleChatSend(client *corews.Client, env corews.Envelope) []corews.Envelope {
	requestID := strings.TrimSpace(env.RequestID)
	if h.hub == nil || client == nil || client.LiveSessionID == 0 {
		return []corews.Envelope{chatErrorEnvelope(requestID, "", "", "CHAT_UNAVAILABLE")}
	}
	var payload struct {
		RoomID          string `json:"roomId"`
		Content         string `json:"content"`
		ClientMessageID string `json:"clientMessageId"`
	}
	decoder := json.NewDecoder(bytes.NewReader(env.Payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return []corews.Envelope{chatErrorEnvelope(requestID, strconv.FormatUint(client.LiveSessionID, 10), "", "INVALID_CHAT_MESSAGE")}
	}
	roomID := strings.TrimSpace(payload.RoomID)
	expectedRoomID := strconv.FormatUint(client.LiveSessionID, 10)
	content := strings.TrimSpace(payload.Content)
	clientMessageID := strings.TrimSpace(payload.ClientMessageID)
	if roomID != expectedRoomID || content == "" || clientMessageID == "" || len([]rune(content)) > maxWSChatContentRunes {
		return []corews.Envelope{chatErrorEnvelope(requestID, expectedRoomID, clientMessageID, "INVALID_CHAT_MESSAGE")}
	}
	now := time.Now().UTC()
	createdAt := now.Format(time.RFC3339Nano)
	messageID := fmt.Sprintf("chat_%s_%d", expectedRoomID, now.UnixNano())
	message := map[string]interface{}{
		"id":              messageID,
		"roomId":          expectedRoomID,
		"userId":          client.UserID,
		"nickname":        client.UserID,
		"content":         content,
		"clientMessageId": clientMessageID,
		"createdAt":       createdAt,
	}
	h.hub.BroadcastLiveSession(client.LiveSessionID, jsonEnvelope("chat.message", requestID, message))
	return []corews.Envelope{
		jsonEnvelope("chat.ack", requestID, map[string]interface{}{
			"roomId":          expectedRoomID,
			"clientMessageId": clientMessageID,
			"messageId":       messageID,
			"createdAt":       createdAt,
		}),
	}
}

func chatErrorEnvelope(requestID, roomID, clientMessageID, reason string) corews.Envelope {
	return jsonEnvelope("chat.error", requestID, map[string]interface{}{
		"roomId":          roomID,
		"clientMessageId": clientMessageID,
		"message":         reason,
	})
}

func (h *WSHandler) handleBidPlace(ctx context.Context, client *corews.Client, env corews.Envelope) []corews.Envelope {
	if h.bidPlaceMode == WSBidPlaceDisabled {
		return []corews.Envelope{jsonEnvelope("bid.ack", env.RequestID, map[string]interface{}{"accepted": false, "reason": "BID_THROUGH_API_REQUIRED"})}
	}
	if h.bids == nil {
		return []corews.Envelope{jsonEnvelope("bid.ack", env.RequestID, map[string]interface{}{"accepted": false, "reason": "BID_SERVICE_UNAVAILABLE"})}
	}
	var payload struct {
		AuctionID            wsUint64 `json:"auctionId"`
		Price                int64    `json:"price"`
		ExpectedCurrentPrice *int64   `json:"expectedCurrentPrice"`
	}
	decoder := json.NewDecoder(bytes.NewReader(env.Payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return []corews.Envelope{jsonEnvelope("bid.ack", env.RequestID, map[string]interface{}{"accepted": false, "reason": "INVALID_PAYLOAD"})}
	}
	auctionID := uint64(payload.AuctionID)
	if auctionID == 0 {
		auctionID = client.AuctionID
	}
	requestID := strings.TrimSpace(env.RequestID)
	if payload.ExpectedCurrentPrice == nil {
		return []corews.Envelope{jsonEnvelope("bid.ack", requestID, map[string]interface{}{"accepted": false, "reason": "MISSING_EXPECTED_STATE"})}
	}
	in := PlaceBidInput{
		RequestID:            requestID,
		AuctionID:            auctionID,
		BidderID:             client.UserID,
		UserRole:             domain.RoleBuyer,
		Price:                payload.Price,
		ExpectedCurrentPrice: payload.ExpectedCurrentPrice,
		Source:               "live_ws",
	}
	// 异步模式：仅在 async 链路完整就绪时生效，否则强制降级同步（绝不丢请求）。
	if h.bidPlaceMode == WSBidPlaceAsync && h.asyncReady() {
		return h.handleBidPlaceAsync(ctx, client, in)
	}
	h.recordBidPlaceMode("sync")
	result, err := h.bids.Place(ctx, in)
	if err != nil {
		_, code, message := HTTPStatusAndCode(err)
		return []corews.Envelope{jsonEnvelope("bid.ack", requestID, map[string]interface{}{"accepted": false, "code": code, "reason": message})}
	}
	return []corews.Envelope{jsonEnvelope("bid.ack", requestID, result)}
}

// handleBidPlaceAsync 异步分支：preCheck → 队列保护 → 发布命令 → 登记 pending →
// 立即回 QUEUED。preCheck 失败/前置裁定直接回 ASYNC 终态 ack，不入队。
func (h *WSHandler) handleBidPlaceAsync(ctx context.Context, client *corews.Client, in PlaceBidInput) []corews.Envelope {
	h.recordBidPlaceMode("async")
	snapshot, terminal, err := h.asyncBids.PreCheckForAsync(ctx, in)
	if err != nil {
		_, code, message := HTTPStatusAndCode(err)
		return []corews.Envelope{jsonEnvelope("bid.ack", in.RequestID, map[string]interface{}{
			"mode": "ASYNC", "status": "REJECTED", "accepted": false, "code": code, "reason": message,
		})}
	}
	if terminal != nil {
		return []corews.Envelope{jsonEnvelope("bid.ack", in.RequestID, map[string]interface{}{
			"mode": "ASYNC", "status": "REJECTED", "accepted": terminal.Accepted, "reason": terminal.Reason,
			"auctionId": in.AuctionID, "bidId": in.RequestID,
		})}
	}
	// 队列保护：超阈值 / 同用户同拍品已在途直接拒，不入队。
	if ok, reason := h.asyncCoord.TryEnqueue(in.AuctionID, client.LiveSessionID, client.UserID, in.RequestID); !ok {
		h.recordBidQueueReject(reason)
		return []corews.Envelope{jsonEnvelope("bid.ack", in.RequestID, map[string]interface{}{
			"mode": "ASYNC", "status": "REJECTED", "accepted": false, "reason": reason,
			"auctionId": in.AuctionID, "bidId": in.RequestID,
		})}
	}
	snapshot.LiveSessionID = orDefaultUint64(snapshot.LiveSessionID, client.LiveSessionID)
	enqueueStart := time.Now()
	if err := h.cmdPublisher.PublishBidCommand(ctx, snapshot); err != nil {
		// 闸门拒绝（HAMMER_PENDING）：明确回 REJECTED reason=AUCTION_HAMMER_PENDING，
		// 不降级走同步（同步路径同样会被状态机拒），并释放队列计数。
		if errors.Is(err, auctionapp.ErrHammerPending) {
			h.asyncCoord.HandleAck(in.RequestID)
			h.recordBidQueueReject("AUCTION_HAMMER_PENDING")
			return []corews.Envelope{jsonEnvelope("bid.ack", in.RequestID, map[string]interface{}{
				"mode": "ASYNC", "status": "REJECTED", "accepted": false, "reason": "AUCTION_HAMMER_PENDING",
				"auctionId": in.AuctionID, "bidId": in.RequestID,
			})}
		}
		// 发布失败：释放 pending，回退同步执行，绝不丢请求。
		h.asyncCoord.HandleAck(in.RequestID)
		h.recordBidPlaceMode("async_fallback_sync")
		result, syncErr := h.bids.Place(ctx, in)
		if syncErr != nil {
			_, code, message := HTTPStatusAndCode(syncErr)
			return []corews.Envelope{jsonEnvelope("bid.ack", in.RequestID, map[string]interface{}{"accepted": false, "code": code, "reason": message})}
		}
		return []corews.Envelope{jsonEnvelope("bid.ack", in.RequestID, result)}
	}
	h.recordBidEnqueueDuration(time.Since(enqueueStart))
	return []corews.Envelope{jsonEnvelope("bid.ack", in.RequestID, map[string]interface{}{
		"mode": "ASYNC", "status": "QUEUED", "bidId": in.RequestID, "auctionId": in.AuctionID,
	})}
}

// handleBidResultAck 处理入站 bid.result.ack：释放该 bidId 的 pending + 停止重发。
func (h *WSHandler) handleBidResultAck(env corews.Envelope) {
	if h.asyncCoord == nil {
		return
	}
	var payload struct {
		BidID string `json:"bidId"`
	}
	if len(env.Payload) > 0 {
		_ = json.Unmarshal(env.Payload, &payload)
	}
	bidID := strings.TrimSpace(payload.BidID)
	if bidID == "" {
		bidID = strings.TrimSpace(env.RequestID)
	}
	if bidID == "" {
		return
	}
	h.asyncCoord.HandleAck(bidID)
}

func orDefaultUint64(v, fallback uint64) uint64 {
	if v != 0 {
		return v
	}
	return fallback
}

func jsonEnvelope(eventType, requestID string, payload interface{}) corews.Envelope {
	raw, _ := json.Marshal(payload)
	return corews.Envelope{Type: eventType, RequestID: requestID, Payload: raw}
}

func snapshotRequiredEnvelope(auctionID uint64, reason string, extra map[string]interface{}) corews.Envelope {
	payload := map[string]interface{}{
		"auctionId":  auctionID,
		"reason":     reason,
		"serverTime": time.Now().UTC().UnixMilli(),
	}
	for key, value := range extra {
		payload[key] = value
	}
	return jsonEnvelope("room.snapshot_required", "", payload)
}

func parseLastSeq(c *app.RequestContext) int64 {
	value := strings.TrimSpace(c.Query("lastSeq"))
	if value == "" {
		return 0
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 {
		return 0
	}
	return parsed
}

package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/service"
	corews "aieas_backend/internal/transport/ws"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/hertz-contrib/websocket"
)

const websocketWriteTimeout = 5 * time.Second

type WSHandler struct {
	hub            *corews.Hub
	bids           *service.BidService
	rooms          *service.LiveRoomService
	auctions       *service.AuctionService
	sendBufferSize int
	readLimitBytes int
	pingInterval   time.Duration
	pongTimeout    time.Duration
	upgrader       websocket.HertzUpgrader
}

func NewWSHandler(hub *corews.Hub, bids *service.BidService, sendBufferSize, readLimitBytes int, pingInterval, pongTimeout time.Duration) *WSHandler {
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
	return &WSHandler{
		hub:            hub,
		bids:           bids,
		sendBufferSize: sendBufferSize,
		readLimitBytes: readLimitBytes,
		pingInterval:   pingInterval,
		pongTimeout:    pongTimeout,
		upgrader: websocket.HertzUpgrader{
			ReadBufferSize:  readLimitBytes,
			WriteBufferSize: readLimitBytes,
			CheckOrigin:     func(ctx *app.RequestContext) bool { return true },
		},
	}
}

// SetLiveRoomService 注入直播间服务以支持 /ws/live-rooms/:room_id 入口。
func (h *WSHandler) SetLiveRoomService(rooms *service.LiveRoomService) {
	h.rooms = rooms
}

// SetAuctionService 注入拍卖服务以在握手后下发 room.snapshot（P1-B）。
// 未注入时握手成功仍可继续，但跳过 snapshot 帧（保留原有读写循环）。
func (h *WSHandler) SetAuctionService(auctions *service.AuctionService) {
	h.auctions = auctions
}

func (h *WSHandler) Auction(ctx context.Context, c *app.RequestContext) {
	_ = ctx
	auctionID, ok := parseUintParam(c, "auction_id")
	if !ok {
		return
	}
	clientID := fmt.Sprintf("ws_%d", time.Now().UnixNano())
	userID := AuthUserID(c)
	if userID == "" {
		userID = "anonymous"
	}
	client := corews.NewClient(clientID, userID, auctionID, h.sendBufferSize)
	lastSeq := parseLastSeq(c)
	if err := h.hub.Subscribe(auctionID, client); err != nil {
		WriteError(c, consts.StatusInternalServerError, 90001, "系统内部错误", nil)
		return
	}
	if err := h.upgrader.Upgrade(c, func(conn *websocket.Conn) {
		h.serveConn(context.Background(), conn, client, lastSeq)
	}); err != nil {
		h.hub.Unsubscribe(auctionID, client.ID)
	}
}

// LiveRoom 处理 `/ws/live-rooms/:room_id` 入口，将客户端订阅到房间当前活跃拍品的事件房间。
func (h *WSHandler) LiveRoom(ctx context.Context, c *app.RequestContext) {
	roomID, ok := parseUintParam(c, "room_id")
	if !ok {
		return
	}
	if h.rooms == nil {
		WriteError(c, consts.StatusInternalServerError, 90001, "系统内部错误", nil)
		return
	}
	auctionID, sessionID, err := h.rooms.ActiveAuctionAndSession(ctx, roomID)
	if err != nil {
		writeLiveRoomError(c, err)
		return
	}
	if auctionID == 0 {
		WriteError(c, 409, 31005, "直播间当前无在拍品", nil)
		return
	}
	clientID := fmt.Sprintf("ws_%d", time.Now().UnixNano())
	userID := AuthUserID(c)
	if userID == "" {
		userID = "anonymous"
	}
	client := corews.NewClientWithSession(clientID, userID, auctionID, sessionID, h.sendBufferSize)
	lastSeq := parseLastSeq(c)
	if err := h.hub.Subscribe(auctionID, client); err != nil {
		WriteError(c, consts.StatusInternalServerError, 90001, "系统内部错误", nil)
		return
	}
	if err := h.upgrader.Upgrade(c, func(conn *websocket.Conn) {
		h.serveConn(context.Background(), conn, client, lastSeq)
	}); err != nil {
		h.hub.Unsubscribe(auctionID, client.ID)
	}
}

func (h *WSHandler) serveConn(ctx context.Context, conn *websocket.Conn, client *corews.Client, lastSeq int64) {
	defer func() {
		h.hub.Unsubscribe(client.AuctionID, client.ID)
		_ = conn.Close()
	}()
	conn.SetReadLimit(int64(h.readLimitBytes))
	_ = conn.SetReadDeadline(time.Now().Add(h.pongTimeout))
	conn.SetPongHandler(func(string) error {
		h.hub.Touch(client.AuctionID, client.ID)
		return conn.SetReadDeadline(time.Now().Add(h.pongTimeout))
	})

	// P1-B：握手后立即下发房间快照，让客户端不必等首个 broadcast 才能渲染。
	// 优先走 RT (LoadState)，RT 不可用 / state 缺失时降级 MySQL，并在 payload
	// 中标记 degraded=true。snapshot 直接 deliver 到 client，不再走 Hub.Broadcast，
	// 避免 seq 占用与 history 写入。
	h.deliverRoomSnapshot(ctx, client)

	done := make(chan struct{})
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		ticker := time.NewTicker(h.pingInterval)
		defer ticker.Stop()
		h.replayMissed(client, lastSeq)
		for {
			select {
			case env, ok := <-client.Outbound():
				if !ok {
					return
				}
				if err := conn.SetWriteDeadline(time.Now().Add(websocketWriteTimeout)); err != nil {
					client.MarkSendFailure()
					return
				}
				if err := conn.WriteJSON(env); err != nil {
					client.MarkSendFailure()
					return
				}
			case <-ticker.C:
				_ = conn.SetWriteDeadline(time.Now().Add(websocketWriteTimeout))
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					client.MarkSendFailure()
					return
				}
			}
		}
	}()
	go func() {
		defer close(done)
		for {
			messageType, payload, err := conn.ReadMessage()
			if err != nil {
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
	case <-done:
		client.CloseWithReason("read_closed")
	case <-writeDone:
		client.CloseWithReason("write_closed")
	}
}

func (h *WSHandler) replayMissed(client *corews.Client, lastSeq int64) {
	if lastSeq <= 0 {
		return
	}
	missed, complete := h.hub.ReplaySince(client.AuctionID, lastSeq)
	if !complete {
		client.Deliver(jsonEnvelope("room.snapshot_required", "", map[string]interface{}{"auctionId": client.AuctionID, "lastSeq": lastSeq, "reason": "EVENT_WINDOW_EXPIRED"}))
		return
	}
	for _, env := range missed {
		client.Deliver(env)
	}
}

// deliverRoomSnapshot 在握手完成后立即下发一帧 room.snapshot：
// 优先 RT (auctionService.State 内部已 RT-first → DB fallback)，并基于
// 返回的 Source=="redis"/"db" 推断是否退化。state 加载失败、auction 不存在
// 或 auctionService 未注入时，仅 best-effort 跳过——不阻断后续读写循环。
//
// snapshot 帧的 seq 字段填当前 Hub Room 的 CurrentSeq()，让客户端把它
// 当作"已对齐到此点"，后续 broadcast 只要 seq 严格递增即可保证不重不漏。
func (h *WSHandler) deliverRoomSnapshot(ctx context.Context, client *corews.Client) {
	if client == nil || h.auctions == nil {
		return
	}
	state, err := h.auctions.State(ctx, client.AuctionID, client.UserID, domain.RoleBuyer)
	if err != nil {
		// 拉取失败（auction 不存在 / RT+DB 都失败）：跳过 snapshot，
		// 老客户端依旧依赖 first broadcast 渲染——保持原有行为。
		return
	}
	var seq int64
	if room, ok := h.hub.Room(client.AuctionID); ok && room != nil {
		seq = room.CurrentSeq()
	}
	degraded := strings.EqualFold(state.Source, "db")
	payload := map[string]interface{}{
		"auctionId":      state.AuctionID,
		"status":         state.Status,
		"currentPrice":   state.CurrentPrice,
		"leaderBidderId": state.LeaderBidderID,
		"startTime":      state.StartTime.UTC().UnixMilli(),
		"endTime":        state.EndTime.UTC().UnixMilli(),
		"extendCount":    state.ExtendCount,
		"version":        state.Version,
		"seq":            seq,
		"serverTime":     time.Now().UTC().UnixMilli(),
		"source":         state.Source,
		"degraded":       degraded,
	}
	client.Deliver(jsonEnvelope(corews.TypeRoomSnapshot, "", payload))
}

func (h *WSHandler) handleInbound(ctx context.Context, client *corews.Client, env corews.Envelope) []corews.Envelope {
	switch env.Type {
	case "bid.place":
		return h.handleBidPlace(ctx, client, env)
	case "room.subscribe":
		var payload struct {
			AuctionID uint64 `json:"auctionId"`
		}
		_ = json.Unmarshal(env.Payload, &payload)
		if payload.AuctionID == 0 {
			payload.AuctionID = client.AuctionID
		}
		_ = h.hub.Subscribe(payload.AuctionID, client)
		return []corews.Envelope{jsonEnvelope("room.subscribed", env.RequestID, map[string]interface{}{"auctionId": payload.AuctionID})}
	case "room.unsubscribe":
		var payload struct {
			AuctionID uint64 `json:"auctionId"`
		}
		_ = json.Unmarshal(env.Payload, &payload)
		if payload.AuctionID == 0 {
			payload.AuctionID = client.AuctionID
		}
		h.hub.Unsubscribe(payload.AuctionID, client.ID)
		return []corews.Envelope{jsonEnvelope("room.unsubscribed", env.RequestID, map[string]interface{}{"auctionId": payload.AuctionID})}
	case "heartbeat":
		h.hub.Touch(client.AuctionID, client.ID)
		return []corews.Envelope{jsonEnvelope("heartbeat.ack", env.RequestID, map[string]interface{}{"ts": time.Now().UTC().UnixMilli()})}
	default:
		return h.hub.HandleInbound(ctx, client, env)
	}
}

func (h *WSHandler) handleBidPlace(ctx context.Context, client *corews.Client, env corews.Envelope) []corews.Envelope {
	if h.bids == nil {
		return []corews.Envelope{jsonEnvelope("bid.ack", env.RequestID, map[string]interface{}{"accepted": false, "reason": "BID_SERVICE_UNAVAILABLE"})}
	}
	var payload struct {
		AuctionID            uint64 `json:"auctionId"`
		Price                int64  `json:"price"`
		ExpectedCurrentPrice *int64 `json:"expectedCurrentPrice"`
	}
	decoder := json.NewDecoder(bytes.NewReader(env.Payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return []corews.Envelope{jsonEnvelope("bid.ack", env.RequestID, map[string]interface{}{"accepted": false, "reason": "INVALID_PAYLOAD"})}
	}
	if payload.AuctionID == 0 {
		payload.AuctionID = client.AuctionID
	}
	requestID := strings.TrimSpace(env.RequestID)
	if payload.ExpectedCurrentPrice == nil {
		return []corews.Envelope{jsonEnvelope("bid.ack", requestID, map[string]interface{}{"accepted": false, "reason": "MISSING_EXPECTED_STATE"})}
	}
	result, err := h.bids.Place(ctx, service.PlaceBidInput{
		RequestID:            requestID,
		AuctionID:            payload.AuctionID,
		BidderID:             client.UserID,
		UserRole:             domain.RoleBuyer,
		Price:                payload.Price,
		ExpectedCurrentPrice: payload.ExpectedCurrentPrice,
		Source:               "live_ws",
	})
	if err != nil {
		_, code, message := service.HTTPStatusAndCode(err)
		return []corews.Envelope{jsonEnvelope("bid.ack", requestID, map[string]interface{}{"accepted": false, "code": code, "reason": message})}
	}
	return []corews.Envelope{jsonEnvelope("bid.ack", requestID, result)}
}

func jsonEnvelope(eventType, requestID string, payload interface{}) corews.Envelope {
	raw, _ := json.Marshal(payload)
	return corews.Envelope{Type: eventType, RequestID: requestID, Payload: raw}
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

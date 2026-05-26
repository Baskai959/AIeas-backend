package mcp

import (
	"context"
	"encoding/json"
	"strings"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/service"
)

type toolsCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

func (h *Handler) callTool(ctx context.Context, name string, arguments json.RawMessage, actor service.MCPActor, traceID string) (toolCallResult, error) {
	if h.read == nil {
		return toolCallResult{}, domain.ErrNotFound
	}
	data, err := h.toolData(ctx, name, arguments, actor)
	if err != nil {
		return toolCallResult{}, err
	}
	text, err := payloadText(traceID, data)
	if err != nil {
		return toolCallResult{}, err
	}
	return toolCallResult{Content: []textContent{{Type: "text", MIMEType: "application/json", Text: text}}}, nil
}

func (h *Handler) toolData(ctx context.Context, name string, arguments json.RawMessage, actor service.MCPActor) (interface{}, error) {
	switch strings.TrimSpace(name) {
	case "read_user":
		var in struct {
			UserID string `json:"userId"`
		}
		if err := decodeParams(arguments, &in); err != nil {
			return nil, domain.ErrInvalidArgument
		}
		return h.read.ReadUser(ctx, in.UserID, actor)
	case "read_users":
		var in struct {
			Role    domain.Role       `json:"role"`
			Status  domain.UserStatus `json:"status"`
			Keyword string            `json:"keyword"`
			Limit   int               `json:"limit"`
			Offset  int               `json:"offset"`
		}
		if err := decodeParams(arguments, &in); err != nil {
			return nil, domain.ErrInvalidArgument
		}
		filter := domain.UserFilter{Role: in.Role, Status: in.Status, Keyword: strings.TrimSpace(in.Keyword), Limit: normalizeLimit(in.Limit, 20), Offset: normalizeOffset(in.Offset)}
		items, err := h.read.ListUsers(ctx, filter, actor)
		return pagePayload(items, filter.Limit, filter.Offset, len(items)), err
	case "read_merchant":
		var in struct {
			MerchantID string `json:"merchantId"`
		}
		if err := decodeParams(arguments, &in); err != nil {
			return nil, domain.ErrInvalidArgument
		}
		return h.read.ReadMerchant(ctx, in.MerchantID, actor)
	case "read_item":
		var in struct {
			ItemID uint64 `json:"itemId"`
		}
		if err := decodeParams(arguments, &in); err != nil || in.ItemID == 0 {
			return nil, domain.ErrInvalidArgument
		}
		return h.read.ReadItem(ctx, in.ItemID, actor)
	case "read_items":
		var in struct {
			SellerID string            `json:"sellerId"`
			Status   domain.ItemStatus `json:"status"`
			Category string            `json:"category"`
			Limit    int               `json:"limit"`
			Offset   int               `json:"offset"`
		}
		if err := decodeParams(arguments, &in); err != nil {
			return nil, domain.ErrInvalidArgument
		}
		filter := domain.ItemFilter{SellerID: strings.TrimSpace(in.SellerID), Status: in.Status, Category: strings.TrimSpace(in.Category), Limit: normalizeLimit(in.Limit, 20), Offset: normalizeOffset(in.Offset)}
		items, err := h.read.ListItems(ctx, filter, actor)
		return pagePayload(items, filter.Limit, filter.Offset, len(items)), err
	case "read_auction_lot":
		var in struct {
			AuctionID uint64 `json:"auctionId"`
		}
		if err := decodeParams(arguments, &in); err != nil || in.AuctionID == 0 {
			return nil, domain.ErrInvalidArgument
		}
		return h.read.ReadAuctionLot(ctx, in.AuctionID, actor)
	case "read_auction_state":
		var in struct {
			AuctionID uint64 `json:"auctionId"`
		}
		if err := decodeParams(arguments, &in); err != nil || in.AuctionID == 0 {
			return nil, domain.ErrInvalidArgument
		}
		return h.read.ReadAuctionState(ctx, in.AuctionID, actor)
	case "read_auction_lots":
		var in struct {
			SellerID   string               `json:"sellerId"`
			Status     domain.AuctionStatus `json:"status"`
			ItemID     uint64               `json:"itemId"`
			LiveRoomID uint64               `json:"liveRoomId"`
			Limit      int                  `json:"limit"`
			Offset     int                  `json:"offset"`
		}
		if err := decodeParams(arguments, &in); err != nil {
			return nil, domain.ErrInvalidArgument
		}
		filter := domain.AuctionFilter{SellerID: strings.TrimSpace(in.SellerID), Status: in.Status, ItemID: in.ItemID, LiveRoomID: in.LiveRoomID, Limit: normalizeLimit(in.Limit, 20), Offset: normalizeOffset(in.Offset)}
		items, err := h.read.ListAuctionLots(ctx, filter, actor)
		return pagePayload(items, filter.Limit, filter.Offset, len(items)), err
	case "read_live_room":
		var in struct {
			RoomID uint64 `json:"roomId"`
		}
		if err := decodeParams(arguments, &in); err != nil || in.RoomID == 0 {
			return nil, domain.ErrInvalidArgument
		}
		return h.read.ReadLiveRoom(ctx, in.RoomID, actor)
	case "read_live_rooms":
		var in struct {
			MerchantID string                `json:"merchantId"`
			Status     domain.LiveRoomStatus `json:"status"`
			Limit      int                   `json:"limit"`
			Offset     int                   `json:"offset"`
		}
		if err := decodeParams(arguments, &in); err != nil {
			return nil, domain.ErrInvalidArgument
		}
		filter := domain.LiveRoomFilter{MerchantID: strings.TrimSpace(in.MerchantID), Status: in.Status, Limit: normalizeLimit(in.Limit, 20), Offset: normalizeOffset(in.Offset)}
		items, err := h.read.ListLiveRooms(ctx, filter, actor)
		return pagePayload(items, filter.Limit, filter.Offset, len(items)), err
	case "read_live_room_lots":
		var in struct {
			RoomID uint64 `json:"roomId"`
		}
		if err := decodeParams(arguments, &in); err != nil || in.RoomID == 0 {
			return nil, domain.ErrInvalidArgument
		}
		return h.read.ListLiveRoomLots(ctx, in.RoomID, actor)
	case "read_live_room_stats":
		var in struct {
			RoomID uint64 `json:"roomId"`
		}
		if err := decodeParams(arguments, &in); err != nil || in.RoomID == 0 {
			return nil, domain.ErrInvalidArgument
		}
		return h.read.ReadLiveRoomStats(ctx, in.RoomID, actor)
	case "read_live_sessions":
		var in struct {
			MerchantID string                   `json:"merchantId"`
			RoomID     uint64                   `json:"roomId"`
			Status     domain.LiveSessionStatus `json:"status"`
			Limit      int                      `json:"limit"`
			Offset     int                      `json:"offset"`
		}
		if err := decodeParams(arguments, &in); err != nil {
			return nil, domain.ErrInvalidArgument
		}
		filter := domain.LiveSessionFilter{MerchantID: strings.TrimSpace(in.MerchantID), LiveRoomID: in.RoomID, Status: in.Status, Limit: normalizeLimit(in.Limit, 20), Offset: normalizeOffset(in.Offset)}
		items, err := h.read.ListLiveSessions(ctx, filter, actor)
		return pagePayload(items, filter.Limit, filter.Offset, len(items)), err
	case "read_live_session":
		var in struct {
			SessionID uint64 `json:"sessionId"`
		}
		if err := decodeParams(arguments, &in); err != nil || in.SessionID == 0 {
			return nil, domain.ErrInvalidArgument
		}
		return h.read.ReadLiveSession(ctx, in.SessionID, actor)
	case "read_live_session_lots":
		var in struct {
			SessionID uint64 `json:"sessionId"`
		}
		if err := decodeParams(arguments, &in); err != nil || in.SessionID == 0 {
			return nil, domain.ErrInvalidArgument
		}
		return h.read.ListLiveSessionLots(ctx, in.SessionID, actor)
	case "read_live_session_bids":
		var in struct {
			SessionID uint64 `json:"sessionId"`
			Sort      string `json:"sort"`
			Limit     int    `json:"limit"`
			Offset    int    `json:"offset"`
		}
		if err := decodeParams(arguments, &in); err != nil || in.SessionID == 0 {
			return nil, domain.ErrInvalidArgument
		}
		limit := normalizeLimit(in.Limit, 50)
		offset := normalizeOffset(in.Offset)
		items, err := h.read.ListLiveSessionBids(ctx, in.SessionID, in.Sort, limit, offset, actor)
		return pagePayload(items, limit, offset, len(items)), err
	case "read_live_session_orders":
		var in struct {
			SessionID uint64             `json:"sessionId"`
			Status    domain.OrderStatus `json:"status"`
			PayStatus domain.PayStatus   `json:"payStatus"`
			Limit     int                `json:"limit"`
			Offset    int                `json:"offset"`
		}
		if err := decodeParams(arguments, &in); err != nil || in.SessionID == 0 {
			return nil, domain.ErrInvalidArgument
		}
		limit := normalizeLimit(in.Limit, 20)
		offset := normalizeOffset(in.Offset)
		items, err := h.read.ListLiveSessionOrders(ctx, in.SessionID, in.Status, in.PayStatus, limit, offset, actor)
		return pagePayload(items, limit, offset, len(items)), err
	case "read_live_session_settlement":
		var in struct {
			SessionID uint64 `json:"sessionId"`
		}
		if err := decodeParams(arguments, &in); err != nil || in.SessionID == 0 {
			return nil, domain.ErrInvalidArgument
		}
		return h.read.ReadLiveSessionSettlement(ctx, in.SessionID, actor)
	case "read_order":
		var in struct {
			OrderID uint64 `json:"orderId"`
		}
		if err := decodeParams(arguments, &in); err != nil || in.OrderID == 0 {
			return nil, domain.ErrInvalidArgument
		}
		return h.read.ReadOrder(ctx, in.OrderID, actor)
	case "read_orders":
		var in struct {
			WinnerID  string             `json:"winnerId"`
			SellerID  string             `json:"sellerId"`
			Status    domain.OrderStatus `json:"status"`
			PayStatus domain.PayStatus   `json:"payStatus"`
			Limit     int                `json:"limit"`
			Offset    int                `json:"offset"`
		}
		if err := decodeParams(arguments, &in); err != nil {
			return nil, domain.ErrInvalidArgument
		}
		filter := domain.OrderFilter{WinnerID: strings.TrimSpace(in.WinnerID), SellerID: strings.TrimSpace(in.SellerID), Status: in.Status, PayStatus: in.PayStatus, Limit: normalizeLimit(in.Limit, 20), Offset: normalizeOffset(in.Offset)}
		items, err := h.read.ListOrders(ctx, filter, actor)
		return pagePayload(items, filter.Limit, filter.Offset, len(items)), err
	case "read_risk_events":
		var in struct {
			Status    domain.RiskEventStatus `json:"status"`
			EventType string                 `json:"eventType"`
			UserID    string                 `json:"userId"`
			Limit     int                    `json:"limit"`
			Offset    int                    `json:"offset"`
		}
		if err := decodeParams(arguments, &in); err != nil {
			return nil, domain.ErrInvalidArgument
		}
		filter := domain.RiskEventFilter{Status: in.Status, EventType: strings.TrimSpace(in.EventType), UserID: strings.TrimSpace(in.UserID), Limit: normalizeLimit(in.Limit, 20), Offset: normalizeOffset(in.Offset)}
		items, err := h.read.ListRiskEvents(ctx, filter, actor)
		return pagePayload(items, filter.Limit, filter.Offset, len(items)), err
	case "read_audit_logs":
		var in struct {
			OperatorID string `json:"operatorId"`
			Action     string `json:"action"`
			Limit      int    `json:"limit"`
			Offset     int    `json:"offset"`
		}
		if err := decodeParams(arguments, &in); err != nil {
			return nil, domain.ErrInvalidArgument
		}
		filter := domain.AuditFilter{OperatorID: strings.TrimSpace(in.OperatorID), Action: strings.TrimSpace(in.Action), Limit: normalizeLimit(in.Limit, 20), Offset: normalizeOffset(in.Offset)}
		items, err := h.read.ListAuditLogs(ctx, filter, actor)
		return pagePayload(items, filter.Limit, filter.Offset, len(items)), err
	default:
		return nil, domain.ErrNotFound
	}
}

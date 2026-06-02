package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/observability/tracing"
	"aieas_backend/internal/service"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type toolsCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

func (h *Handler) callTool(ctx context.Context, name string, arguments json.RawMessage, actor service.MCPActor, traceID string) (toolCallResult, error) {
	if h.read == nil && h.control == nil {
		return toolCallResult{}, domain.ErrNotFound
	}
	toolName := strings.TrimSpace(name)
	ctx, span := tracing.StartSpan(ctx, "mcp.tool.call",
		attribute.String("mcp.tool", toolName),
		attribute.String("mcp.actor.id", actor.ID),
		attribute.String("mcp.actor.role", string(actor.Role)),
	)
	if traceID != "" {
		span.SetAttributes(attribute.String("trace.request_id", traceID))
	}
	start := time.Now()
	defer span.End()
	liveSessionID, assistantMessage := h.readToolAssistantStatus(toolName, arguments)
	if liveSessionID != 0 && assistantMessage != "" {
		h.notifyReadAssistantStatus(ctx, liveSessionID, actor.ID, toolName, "running", assistantMessage)
	}

	data, err := h.toolData(ctx, toolName, arguments, actor)
	elapsed := time.Since(start)
	status := "ok"
	if err != nil {
		status = mcpStatusFromError(err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.SetAttributes(attribute.String("mcp.result", status))
	if h.metrics != nil {
		h.metrics.ObserveAgentToolCall(toolName, status, elapsed)
	}
	if liveSessionID != 0 && assistantMessage != "" {
		resultStatus := "completed"
		resultMessage := strings.Replace(assistantMessage, "正在", "", 1) + "完成"
		if err != nil {
			resultStatus = "failed"
			resultMessage = strings.Replace(assistantMessage, "正在", "", 1) + "失败"
		}
		h.notifyReadAssistantStatus(ctx, liveSessionID, actor.ID, toolName, resultStatus, resultMessage)
	}
	if err != nil {
		if errors.Is(err, service.ErrAIAssistantUserRejected) || errors.Is(err, service.ErrAIAssistantApprovalTimeout) {
			message := "用户拒绝执行"
			if errors.Is(err, service.ErrAIAssistantApprovalTimeout) {
				message = "用户未确认执行"
			}
			text, encodeErr := h.payloadText(traceID, map[string]interface{}{
				"message": message,
				"reason":  err.Error(),
			})
			if encodeErr != nil {
				span.RecordError(encodeErr)
				span.SetStatus(codes.Error, encodeErr.Error())
				return toolCallResult{}, encodeErr
			}
			return toolCallResult{Content: []textContent{{Type: "text", MIMEType: "application/json", Text: text}}, IsError: true}, nil
		}
		return toolCallResult{}, err
	}
	text, err := h.payloadText(traceID, data)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return toolCallResult{}, err
	}
	return toolCallResult{Content: []textContent{{Type: "text", MIMEType: "application/json", Text: text}}}, nil
}

func (h *Handler) notifyReadAssistantStatus(ctx context.Context, liveSessionID uint64, actorID, toolName, status, message string) {
	if h == nil || h.assistant == nil || h.read == nil || h.control != nil || liveSessionID == 0 {
		return
	}
	h.assistant.NotifyStatus(ctx, liveSessionID, actorID, toolName, status, message, "")
}

func (h *Handler) readToolAssistantStatus(name string, arguments json.RawMessage) (uint64, string) {
	if h == nil || h.assistant == nil || h.read == nil || h.control != nil {
		return 0, ""
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(arguments, &raw); err != nil {
		return 0, ""
	}
	var sessionID uint64
	for _, key := range []string{"sessionId", "liveSessionId"} {
		if value, ok := raw[key]; ok {
			_ = json.Unmarshal(value, &sessionID)
			if sessionID != 0 {
				break
			}
		}
	}
	if sessionID == 0 {
		return 0, ""
	}
	switch strings.TrimSpace(name) {
	case "read_live_session":
		return sessionID, "正在查询直播场次信息"
	case "read_live_session_lots":
		return sessionID, "正在查询直播场次拍品"
	case "read_live_session_bids":
		return sessionID, "正在查询直播场次出价记录"
	case "read_live_session_orders":
		return sessionID, "正在查询直播场次订单"
	case "read_live_session_settlement":
		return sessionID, "正在查询直播场次成交汇总"
	default:
		return sessionID, "正在调用直播场次查询工具"
	}
}

func (h *Handler) toolData(ctx context.Context, name string, arguments json.RawMessage, actor service.MCPActor) (interface{}, error) {
	switch strings.TrimSpace(name) {
	case "read_user":
		if h.read == nil {
			return nil, domain.ErrNotFound
		}
		var in struct {
			UserID string `json:"userId"`
		}
		if err := decodeParams(arguments, &in); err != nil {
			return nil, domain.ErrInvalidArgument
		}
		return h.read.ReadUser(ctx, in.UserID, actor)
	case "read_users":
		if h.read == nil {
			return nil, domain.ErrNotFound
		}
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
		if h.read == nil {
			return nil, domain.ErrNotFound
		}
		var in struct {
			MerchantID string `json:"merchantId"`
		}
		if err := decodeParams(arguments, &in); err != nil {
			return nil, domain.ErrInvalidArgument
		}
		return h.read.ReadMerchant(ctx, in.MerchantID, actor)
	case "read_auction_lot":
		if h.read == nil {
			return nil, domain.ErrNotFound
		}
		var in struct {
			AuctionID uint64 `json:"auctionId"`
		}
		if err := decodeParams(arguments, &in); err != nil || in.AuctionID == 0 {
			return nil, domain.ErrInvalidArgument
		}
		return h.read.ReadAuctionLot(ctx, in.AuctionID, actor)
	case "read_auction_state":
		if h.read == nil {
			return nil, domain.ErrNotFound
		}
		var in struct {
			AuctionID uint64 `json:"auctionId"`
		}
		if err := decodeParams(arguments, &in); err != nil || in.AuctionID == 0 {
			return nil, domain.ErrInvalidArgument
		}
		return h.read.ReadAuctionState(ctx, in.AuctionID, actor)
	case "read_auction_lots":
		if h.read == nil {
			return nil, domain.ErrNotFound
		}
		var in struct {
			SellerID      string               `json:"sellerId"`
			Status        domain.AuctionStatus `json:"status"`
			LiveSessionID uint64               `json:"liveSessionId"`
			Limit         int                  `json:"limit"`
			Offset        int                  `json:"offset"`
		}
		if err := decodeParams(arguments, &in); err != nil {
			return nil, domain.ErrInvalidArgument
		}
		filter := domain.AuctionFilter{SellerID: strings.TrimSpace(in.SellerID), Status: in.Status, LiveSessionID: in.LiveSessionID, Limit: normalizeLimit(in.Limit, 20), Offset: normalizeOffset(in.Offset)}
		items, err := h.read.ListAuctionLots(ctx, filter, actor)
		return pagePayload(items, filter.Limit, filter.Offset, len(items)), err
	case "read_live_sessions":
		if h.read == nil {
			return nil, domain.ErrNotFound
		}
		var in struct {
			MerchantID string                   `json:"merchantId"`
			Status     domain.LiveSessionStatus `json:"status"`
			Limit      int                      `json:"limit"`
			Offset     int                      `json:"offset"`
		}
		if err := decodeParams(arguments, &in); err != nil {
			return nil, domain.ErrInvalidArgument
		}
		filter := domain.LiveSessionFilter{MerchantID: strings.TrimSpace(in.MerchantID), Status: in.Status, Limit: normalizeLimit(in.Limit, 20), Offset: normalizeOffset(in.Offset)}
		items, err := h.read.ListLiveSessions(ctx, filter, actor)
		return pagePayload(items, filter.Limit, filter.Offset, len(items)), err
	case "read_live_session":
		if h.read == nil {
			return nil, domain.ErrNotFound
		}
		var in struct {
			SessionID uint64 `json:"sessionId"`
		}
		if err := decodeParams(arguments, &in); err != nil || in.SessionID == 0 {
			return nil, domain.ErrInvalidArgument
		}
		return h.read.ReadLiveSession(ctx, in.SessionID, actor)
	case "read_live_session_lots":
		if h.read == nil {
			return nil, domain.ErrNotFound
		}
		var in struct {
			SessionID uint64 `json:"sessionId"`
		}
		if err := decodeParams(arguments, &in); err != nil || in.SessionID == 0 {
			return nil, domain.ErrInvalidArgument
		}
		return h.read.ListLiveSessionLots(ctx, in.SessionID, actor)
	case "read_live_session_bids":
		if h.read == nil {
			return nil, domain.ErrNotFound
		}
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
		if h.read == nil {
			return nil, domain.ErrNotFound
		}
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
		if h.read == nil {
			return nil, domain.ErrNotFound
		}
		var in struct {
			SessionID uint64 `json:"sessionId"`
		}
		if err := decodeParams(arguments, &in); err != nil || in.SessionID == 0 {
			return nil, domain.ErrInvalidArgument
		}
		return h.read.ReadLiveSessionSettlement(ctx, in.SessionID, actor)
	case "get_merchant_live_control_context":
		if h.control == nil {
			return nil, domain.ErrNotFound
		}
		var in struct {
			MerchantID string `json:"merchantId"`
		}
		if err := decodeParams(arguments, &in); err != nil {
			return nil, domain.ErrInvalidArgument
		}
		return h.control.ReadMerchantLiveControlContext(ctx, in.MerchantID, actor)
	case "operate_live_session_lot":
		if h.control == nil {
			return nil, domain.ErrNotFound
		}
		var in struct {
			LiveSessionID      uint64 `json:"liveSessionId"`
			AuctionID          uint64 `json:"auctionId"`
			Action             string `json:"action"`
			AuctionDurationSec int    `json:"auctionDurationSec"`
			Force              *bool  `json:"force"`
			RequestID          string `json:"requestId"`
		}
		if err := decodeParams(arguments, &in); err != nil || in.LiveSessionID == 0 || in.AuctionID == 0 || strings.TrimSpace(in.Action) == "" {
			return nil, domain.ErrInvalidArgument
		}
		force := true
		if in.Force != nil {
			force = *in.Force
		}
		return h.control.OperateLiveSessionLot(ctx, service.MCPLiveLotOperationInput{
			LiveSessionID:      in.LiveSessionID,
			AuctionID:          in.AuctionID,
			Action:             in.Action,
			AuctionDurationSec: in.AuctionDurationSec,
			Force:              force,
			RequestID:          in.RequestID,
		}, actor)
	case "live_voice_broadcast":
		if h.control == nil {
			return nil, domain.ErrNotFound
		}
		var in struct {
			LiveSessionID uint64 `json:"liveSessionId"`
			Text          string `json:"text"`
			RequestID     string `json:"requestId"`
		}
		if err := decodeParams(arguments, &in); err != nil || in.LiveSessionID == 0 || strings.TrimSpace(in.Text) == "" {
			return nil, domain.ErrInvalidArgument
		}
		return h.control.CreateLiveVoiceBroadcast(ctx, service.MCPLiveVoiceBroadcastInput{
			LiveSessionID: in.LiveSessionID,
			Text:          in.Text,
			RequestID:     in.RequestID,
		}, actor)
	case "read_order":
		if h.read == nil {
			return nil, domain.ErrNotFound
		}
		var in struct {
			OrderID uint64 `json:"orderId"`
		}
		if err := decodeParams(arguments, &in); err != nil || in.OrderID == 0 {
			return nil, domain.ErrInvalidArgument
		}
		return h.read.ReadOrder(ctx, in.OrderID, actor)
	case "read_orders":
		if h.read == nil {
			return nil, domain.ErrNotFound
		}
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
		if h.read == nil {
			return nil, domain.ErrNotFound
		}
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
		if h.read == nil {
			return nil, domain.ErrNotFound
		}
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

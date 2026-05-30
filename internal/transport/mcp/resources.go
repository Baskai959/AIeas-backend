package mcp

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/observability/tracing"
	"aieas_backend/internal/service"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type resourcesReadParams struct {
	URI string `json:"uri"`
}

func (h *Handler) readResource(ctx context.Context, rawURI string, actor service.MCPActor, traceID string) (resourceReadResult, error) {
	if h.read == nil {
		return resourceReadResult{}, domain.ErrNotFound
	}
	uri := strings.TrimSpace(rawURI)
	ctx, span := tracing.StartSpan(ctx, "mcp.resource.read",
		attribute.String("mcp.uri", uri),
		attribute.String("mcp.actor.id", actor.ID),
		attribute.String("mcp.actor.role", string(actor.Role)),
	)
	if traceID != "" {
		span.SetAttributes(attribute.String("trace.request_id", traceID))
	}
	start := time.Now()
	defer span.End()

	data, err := h.resourceData(ctx, uri, actor)
	elapsed := time.Since(start)
	status := "ok"
	if err != nil {
		status = mcpStatusFromError(err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.SetAttributes(attribute.String("mcp.result", status))
	if h.metrics != nil {
		h.metrics.ObserveAgentToolCall("resources/read", status, elapsed)
	}
	if err != nil {
		return resourceReadResult{}, err
	}
	text, err := h.payloadText(traceID, data)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return resourceReadResult{}, err
	}
	return resourceReadResult{Contents: []resourceContent{{
		URI:      rawURI,
		MIMEType: "application/json",
		Text:     text,
	}}}, nil
}

func (h *Handler) resourceData(ctx context.Context, rawURI string, actor service.MCPActor) (interface{}, error) {
	u, err := url.Parse(strings.TrimSpace(rawURI))
	if err != nil || u.Scheme != "aieas" || u.Host == "" {
		return nil, domain.ErrInvalidArgument
	}
	parts := pathParts(u.Path)
	q := u.Query()
	switch u.Host {
	case "users":
		if len(parts) == 0 {
			filter := domain.UserFilter{
				Role:    domain.Role(q.Get("role")),
				Status:  domain.UserStatus(q.Get("status")),
				Keyword: strings.TrimSpace(q.Get("keyword")),
				Limit:   queryInt(q, "limit", 20),
				Offset:  queryInt(q, "offset", 0),
			}
			items, err := h.read.ListUsers(ctx, filter, actor)
			return pagePayload(items, filter.Limit, filter.Offset, len(items)), err
		}
		if len(parts) == 1 {
			return h.read.ReadUser(ctx, parts[0], actor)
		}
	case "merchants":
		if len(parts) == 1 {
			return h.read.ReadMerchant(ctx, parts[0], actor)
		}
		if len(parts) == 2 && parts[1] == "live-sessions" {
			filter := domain.LiveSessionFilter{
				MerchantID: parts[0],
				Status:     liveSessionStatus(q.Get("status")),
				Limit:      queryInt(q, "limit", 20),
				Offset:     queryInt(q, "offset", 0),
			}
			items, err := h.read.ListLiveSessions(ctx, filter, actor)
			return pagePayload(items, filter.Limit, filter.Offset, len(items)), err
		}
	case "items":
		if len(parts) == 0 {
			filter := domain.ItemFilter{
				SellerID: strings.TrimSpace(q.Get("sellerId")),
				Status:   itemStatus(q.Get("status")),
				Category: strings.TrimSpace(q.Get("category")),
				Limit:    queryInt(q, "limit", 20),
				Offset:   queryInt(q, "offset", 0),
			}
			items, err := h.read.ListItems(ctx, filter, actor)
			return pagePayload(items, filter.Limit, filter.Offset, len(items)), err
		}
		if len(parts) == 1 {
			id, err := parseUintID(parts[0])
			if err != nil {
				return nil, domain.ErrInvalidArgument
			}
			return h.read.ReadItem(ctx, id, actor)
		}
	case "auction-lots":
		if len(parts) == 0 {
			filter := auctionFilterFromQuery(q)
			items, err := h.read.ListAuctionLots(ctx, filter, actor)
			return pagePayload(items, filter.Limit, filter.Offset, len(items)), err
		}
		if len(parts) == 1 || (len(parts) == 2 && parts[1] == "state") {
			id, err := parseUintID(parts[0])
			if err != nil {
				return nil, domain.ErrInvalidArgument
			}
			if len(parts) == 2 {
				return h.read.ReadAuctionState(ctx, id, actor)
			}
			return h.read.ReadAuctionLot(ctx, id, actor)
		}
	case "live-sessions":
		return h.liveSessionResourceData(ctx, parts, q, actor)
	case "orders":
		if len(parts) == 0 {
			filter := orderFilterFromQuery(q)
			items, err := h.read.ListOrders(ctx, filter, actor)
			return pagePayload(items, filter.Limit, filter.Offset, len(items)), err
		}
		if len(parts) == 1 {
			id, err := parseUintID(parts[0])
			if err != nil {
				return nil, domain.ErrInvalidArgument
			}
			return h.read.ReadOrder(ctx, id, actor)
		}
	case "risk":
		if len(parts) == 1 && parts[0] == "events" {
			filter := riskFilterFromQuery(q)
			items, err := h.read.ListRiskEvents(ctx, filter, actor)
			return pagePayload(items, filter.Limit, filter.Offset, len(items)), err
		}
	case "audit-logs":
		if len(parts) == 0 {
			filter := auditFilterFromQuery(q)
			items, err := h.read.ListAuditLogs(ctx, filter, actor)
			return pagePayload(items, filter.Limit, filter.Offset, len(items)), err
		}
	}
	return nil, domain.ErrInvalidArgument
}

func (h *Handler) liveSessionResourceData(ctx context.Context, parts []string, q url.Values, actor service.MCPActor) (interface{}, error) {
	if len(parts) == 0 {
		filter := domain.LiveSessionFilter{
			MerchantID: strings.TrimSpace(q.Get("merchantId")),
			Status:     liveSessionStatus(q.Get("status")),
			Limit:      queryInt(q, "limit", 20),
			Offset:     queryInt(q, "offset", 0),
		}
		items, err := h.read.ListLiveSessions(ctx, filter, actor)
		return pagePayload(items, filter.Limit, filter.Offset, len(items)), err
	}
	id, err := parseUintID(parts[0])
	if err != nil {
		return nil, domain.ErrInvalidArgument
	}
	if len(parts) == 1 {
		return h.read.ReadLiveSession(ctx, id, actor)
	}
	if len(parts) != 2 {
		return nil, domain.ErrInvalidArgument
	}
	switch parts[1] {
	case "lots":
		return h.read.ListLiveSessionLots(ctx, id, actor)
	case "bids":
		limit := queryInt(q, "limit", 50)
		offset := queryInt(q, "offset", 0)
		items, err := h.read.ListLiveSessionBids(ctx, id, strings.TrimSpace(q.Get("sort")), limit, offset, actor)
		return pagePayload(items, limit, offset, len(items)), err
	case "orders":
		limit := queryInt(q, "limit", 20)
		offset := queryInt(q, "offset", 0)
		items, err := h.read.ListLiveSessionOrders(ctx, id, orderStatus(q.Get("status")), payStatus(q.Get("payStatus")), limit, offset, actor)
		return pagePayload(items, limit, offset, len(items)), err
	case "settlement-summary":
		return h.read.ReadLiveSessionSettlement(ctx, id, actor)
	default:
		return nil, domain.ErrInvalidArgument
	}
}

func pathParts(path string) []string {
	path = strings.Trim(path, "/")
	if path == "" {
		return nil
	}
	parts := strings.Split(path, "/")
	out := parts[:0]
	for _, part := range parts {
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func queryInt(q url.Values, name string, fallback int) int {
	value := strings.TrimSpace(q.Get(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func queryUint(q url.Values, name string) uint64 {
	value := strings.TrimSpace(q.Get(name))
	if value == "" {
		return 0
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func auctionFilterFromQuery(q url.Values) domain.AuctionFilter {
	return domain.AuctionFilter{
		SellerID:      strings.TrimSpace(q.Get("sellerId")),
		Status:        auctionStatus(q.Get("status")),
		ItemID:        queryUint(q, "itemId"),
		LiveSessionID: queryUint(q, "liveSessionId"),
		Limit:         queryInt(q, "limit", 20),
		Offset:        queryInt(q, "offset", 0),
	}
}

func orderFilterFromQuery(q url.Values) domain.OrderFilter {
	return domain.OrderFilter{
		WinnerID:      strings.TrimSpace(q.Get("winnerId")),
		SellerID:      strings.TrimSpace(q.Get("sellerId")),
		LiveSessionID: queryUint(q, "liveSessionId"),
		Status:        orderStatus(q.Get("status")),
		PayStatus:     payStatus(q.Get("payStatus")),
		Limit:         queryInt(q, "limit", 20),
		Offset:        queryInt(q, "offset", 0),
	}
}

func riskFilterFromQuery(q url.Values) domain.RiskEventFilter {
	return domain.RiskEventFilter{
		Status:    riskStatus(q.Get("status")),
		EventType: strings.TrimSpace(q.Get("eventType")),
		UserID:    strings.TrimSpace(q.Get("userId")),
		Limit:     queryInt(q, "limit", 20),
		Offset:    queryInt(q, "offset", 0),
	}
}

func auditFilterFromQuery(q url.Values) domain.AuditFilter {
	return domain.AuditFilter{
		OperatorID: strings.TrimSpace(q.Get("operatorId")),
		Action:     strings.TrimSpace(q.Get("action")),
		Limit:      queryInt(q, "limit", 20),
		Offset:     queryInt(q, "offset", 0),
	}
}

func itemStatus(raw string) domain.ItemStatus {
	status := domain.ItemStatus(strings.TrimSpace(raw))
	if status.Valid() {
		return status
	}
	return ""
}

func auctionStatus(raw string) domain.AuctionStatus {
	status := domain.AuctionStatus(strings.TrimSpace(raw))
	if status.Valid() {
		return status
	}
	return ""
}

func liveSessionStatus(raw string) domain.LiveSessionStatus {
	status := domain.LiveSessionStatus(strings.TrimSpace(raw))
	if status.Valid() {
		return status
	}
	return ""
}

func orderStatus(raw string) domain.OrderStatus {
	switch status := domain.OrderStatus(strings.TrimSpace(raw)); status {
	case domain.OrderStatusCreated, domain.OrderStatusPaid, domain.OrderStatusTimeout, domain.OrderStatusCancelled:
		return status
	default:
		return ""
	}
}

func payStatus(raw string) domain.PayStatus {
	switch status := domain.PayStatus(strings.TrimSpace(raw)); status {
	case domain.PayStatusUnpaid, domain.PayStatusPaid, domain.PayStatusRefunded:
		return status
	default:
		return ""
	}
}

func riskStatus(raw string) domain.RiskEventStatus {
	switch status := domain.RiskEventStatus(strings.TrimSpace(raw)); status {
	case domain.RiskEventPending, domain.RiskEventReviewed, domain.RiskEventIgnored:
		return status
	default:
		return ""
	}
}

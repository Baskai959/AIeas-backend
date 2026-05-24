package http

import (
	"context"
	"strings"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/service"

	"github.com/cloudwego/hertz/pkg/app"
)

type OrderHandler struct {
	orders *service.OrderService
}

func NewOrderHandler(orders *service.OrderService) *OrderHandler {
	return &OrderHandler{orders: orders}
}

func (h *OrderHandler) List(ctx context.Context, c *app.RequestContext) {
	filter := orderFilterFromRequest(c)
	orders, err := h.orders.List(ctx, filter, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"orders": orders})
}

func (h *OrderHandler) Mine(ctx context.Context, c *app.RequestContext) {
	filter := orderFilterFromRequest(c)
	orders, err := h.orders.Mine(ctx, AuthUserID(c), AuthRole(c), filter)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"orders": orders})
}

func (h *OrderHandler) Get(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	order, err := h.orders.Get(ctx, id, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, order)
}

func (h *OrderHandler) Pay(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	order, err := h.orders.Pay(ctx, id, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, order)
}

func orderFilterFromRequest(c *app.RequestContext) domain.OrderFilter {
	filter := domain.OrderFilter{
		WinnerID: strings.TrimSpace(c.Query("winnerId")),
		SellerID: strings.TrimSpace(c.Query("sellerId")),
		Limit:    parseQueryInt(c, "limit", 20),
		Offset:   parseQueryInt(c, "offset", 0),
	}
	if status := domain.OrderStatus(strings.TrimSpace(c.Query("status"))); status != "" {
		filter.Status = status
	}
	if payStatus := domain.PayStatus(strings.TrimSpace(c.Query("payStatus"))); payStatus != "" {
		filter.PayStatus = payStatus
	}
	return filter
}

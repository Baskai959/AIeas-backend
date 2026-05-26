package http

import (
	"context"
	"errors"
	"strings"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/service"

	"github.com/cloudwego/hertz/pkg/app"
)

// LiveSessionHandler 暴露直播场次（live_session）相关读接口。
type LiveSessionHandler struct {
	sessions *service.LiveSessionService
}

func NewLiveSessionHandler(sessions *service.LiveSessionService) *LiveSessionHandler {
	return &LiveSessionHandler{sessions: sessions}
}

// ListByRoom 列出指定直播间的场次：GET /live-rooms/:id/sessions
func (h *LiveSessionHandler) ListByRoom(ctx context.Context, c *app.RequestContext) {
	roomID, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	status := domain.LiveSessionStatus(strings.TrimSpace(c.Query("status")))
	if !status.Valid() {
		status = ""
	}
	limit := parseQueryInt(c, "limit", 20)
	offset := parseQueryInt(c, "offset", 0)
	sessions, err := h.sessions.ListByRoom(ctx, roomID, status, AuthUserID(c), AuthRole(c), limit, offset)
	if err != nil {
		writeLiveSessionError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"sessions": sessions})
}

// ListByMerchant 列出某商家的所有场次：GET /merchants/:merchantId/live-sessions
// 商家角色强制以 actorID 为 merchantID；admin 可指定任意 merchant。
func (h *LiveSessionHandler) ListByMerchant(ctx context.Context, c *app.RequestContext) {
	merchantID := strings.TrimSpace(c.Param("merchantId"))
	status := domain.LiveSessionStatus(strings.TrimSpace(c.Query("status")))
	if !status.Valid() {
		status = ""
	}
	limit := parseQueryInt(c, "limit", 20)
	offset := parseQueryInt(c, "offset", 0)
	sessions, err := h.sessions.ListByMerchant(ctx, merchantID, status, AuthUserID(c), AuthRole(c), limit, offset)
	if err != nil {
		writeLiveSessionError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"sessions": sessions})
}

// Get 返回单个场次详情：GET /live-sessions/:sessionId
func (h *LiveSessionHandler) Get(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "sessionId")
	if !ok {
		return
	}
	session, err := h.sessions.Get(ctx, id)
	if err != nil {
		writeLiveSessionError(c, err)
		return
	}
	WriteSuccess(c, session)
}

// Lots 返回某场次内的拍品列表：GET /live-sessions/:sessionId/lots
func (h *LiveSessionHandler) Lots(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "sessionId")
	if !ok {
		return
	}
	lots, err := h.sessions.ListLots(ctx, id, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeLiveSessionError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"lots": lots})
}

// Bids 返回某场次的出价记录：GET /live-sessions/:sessionId/bids
func (h *LiveSessionHandler) Bids(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "sessionId")
	if !ok {
		return
	}
	limit := parseQueryInt(c, "limit", 50)
	bids, err := h.sessions.ListBids(ctx, id, limit, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeLiveSessionError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"bids": bids})
}

// Orders 返回某场次的订单列表：GET /live-sessions/:sessionId/orders
func (h *LiveSessionHandler) Orders(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "sessionId")
	if !ok {
		return
	}
	limit := parseQueryInt(c, "limit", 20)
	offset := parseQueryInt(c, "offset", 0)
	orders, err := h.sessions.ListOrders(ctx, id, limit, offset, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeLiveSessionError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"orders": orders})
}

func writeLiveSessionError(c *app.RequestContext, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		WriteError(c, 404, 32001, "直播场次不存在", nil)
	case errors.Is(err, domain.ErrForbidden):
		WriteError(c, 403, 32002, "无直播场次操作权限", nil)
	case errors.Is(err, domain.ErrInvalidState):
		WriteError(c, 409, 32003, "直播场次状态不允许此操作", nil)
	case errors.Is(err, domain.ErrInvalidArgument):
		WriteError(c, 400, 20001, "参数不合法", nil)
	default:
		writeServiceError(c, err)
	}
}

package http

import (
	"context"
	"errors"
	"strings"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/service"

	"github.com/cloudwego/hertz/pkg/app"
)

type LiveRoomHandler struct {
	rooms *service.LiveRoomService
}

func NewLiveRoomHandler(rooms *service.LiveRoomService) *LiveRoomHandler {
	return &LiveRoomHandler{rooms: rooms}
}

type liveRoomCreateRequest struct {
	MerchantID  string                `json:"merchantId"`
	Title       string                `json:"title"`
	Description string                `json:"description"`
	CoverURL    string                `json:"coverUrl"`
	Status      domain.LiveRoomStatus `json:"status"`
}

type liveRoomPatchRequest struct {
	Title       *string                `json:"title"`
	Description *string                `json:"description"`
	CoverURL    *string                `json:"coverUrl"`
	Status      *domain.LiveRoomStatus `json:"status"`
}

type liveRoomActivateRequest struct {
	AuctionID       uint64 `json:"auctionId"`
	DurationSec     int    `json:"durationSec"`
	DurationMinutes int    `json:"durationMinutes"`
}

type liveRoomMountRequest struct {
	AuctionID uint64 `json:"auctionId"`
}

func (h *LiveRoomHandler) Create(ctx context.Context, c *app.RequestContext) {
	var req liveRoomCreateRequest
	if err := c.BindJSON(&req); err != nil {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	room, err := h.rooms.Create(ctx, service.CreateLiveRoomInput{
		ActorID:     AuthUserID(c),
		ActorRole:   AuthRole(c),
		MerchantID:  req.MerchantID,
		Title:       req.Title,
		Description: req.Description,
		CoverURL:    req.CoverURL,
		Status:      req.Status,
	})
	if err != nil {
		writeLiveRoomError(c, err)
		return
	}
	WriteSuccess(c, room)
}

func (h *LiveRoomHandler) List(ctx context.Context, c *app.RequestContext) {
	filter := domain.LiveRoomFilter{
		MerchantID: strings.TrimSpace(c.Query("merchantId")),
		Limit:      parseQueryInt(c, "limit", 20),
		Offset:     parseQueryInt(c, "offset", 0),
	}
	if status := domain.LiveRoomStatus(strings.TrimSpace(c.Query("status"))); status.Valid() {
		filter.Status = status
	}
	rooms, err := h.rooms.List(ctx, filter, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeLiveRoomError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"rooms": rooms})
}

func (h *LiveRoomHandler) Get(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	room, err := h.rooms.Get(ctx, id)
	if err != nil {
		writeLiveRoomError(c, err)
		return
	}
	WriteSuccess(c, room)
}

func (h *LiveRoomHandler) Update(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	var req liveRoomPatchRequest
	if err := c.BindJSON(&req); err != nil {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	room, err := h.rooms.Update(ctx, id, service.UpdateLiveRoomInput{
		ActorID:     AuthUserID(c),
		ActorRole:   AuthRole(c),
		Title:       req.Title,
		Description: req.Description,
		CoverURL:    req.CoverURL,
		Status:      req.Status,
	})
	if err != nil {
		writeLiveRoomError(c, err)
		return
	}
	WriteSuccess(c, room)
}

func (h *LiveRoomHandler) Delete(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	if err := h.rooms.Delete(ctx, id, AuthUserID(c), AuthRole(c)); err != nil {
		writeLiveRoomError(c, err)
		return
	}
	WriteSuccess(c, map[string]bool{"deleted": true})
}

func (h *LiveRoomHandler) Lots(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	lots, err := h.rooms.ListLots(ctx, id)
	if err != nil {
		writeLiveRoomError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"lots": lots})
}

func (h *LiveRoomHandler) Activate(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	var req liveRoomActivateRequest
	if err := c.BindJSON(&req); err != nil || req.AuctionID == 0 {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	auction, err := h.rooms.ActivateAuctionWithOptions(ctx, service.ActivateAuctionInput{
		RoomID:      id,
		AuctionID:   req.AuctionID,
		ActorID:     AuthUserID(c),
		ActorRole:   AuthRole(c),
		DurationSec: req.DurationSec,
	})
	if err != nil {
		writeLiveRoomError(c, err)
		return
	}
	WriteSuccess(c, auction)
}

func (h *LiveRoomHandler) Deactivate(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	room, err := h.rooms.DeactivateAuction(ctx, id, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeLiveRoomError(c, err)
		return
	}
	WriteSuccess(c, room)
}

// MountLot 将拍品挂载到直播间。
func (h *LiveRoomHandler) MountLot(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	var req liveRoomMountRequest
	if err := c.BindJSON(&req); err != nil || req.AuctionID == 0 {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	lot, err := h.rooms.MountAuction(ctx, id, req.AuctionID, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeLiveRoomError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"lot": lot})
}

// UnmountLot 将拍品从直播间移除。
func (h *LiveRoomHandler) UnmountLot(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	auctionID, ok := parseUintParam(c, "auctionId")
	if !ok {
		return
	}
	if err := h.rooms.UnmountAuction(ctx, id, auctionID, AuthUserID(c), AuthRole(c)); err != nil {
		writeLiveRoomError(c, err)
		return
	}
	WriteSuccess(c, map[string]bool{"removed": true})
}

// Stats 返回直播间实时统计。
func (h *LiveRoomHandler) Stats(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	stats, err := h.rooms.Stats(ctx, id)
	if err != nil {
		writeLiveRoomError(c, err)
		return
	}
	WriteSuccess(c, stats)
}

func writeLiveRoomError(c *app.RequestContext, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		WriteError(c, 404, 31001, "直播间不存在", nil)
	case errors.Is(err, domain.ErrForbidden):
		WriteError(c, 403, 31002, "无直播间操作权限", nil)
	case errors.Is(err, domain.ErrInvalidState):
		WriteError(c, 409, 31003, "直播间状态不允许此操作", nil)
	case errors.Is(err, service.ErrLiveRoomBusy):
		WriteError(c, 409, 31004, "直播间已有拍品在拍", nil)
	case errors.Is(err, service.ErrLotAlreadyMounted):
		WriteError(c, 409, 31006, "拍品已挂入其他直播间", nil)
	case errors.Is(err, service.ErrLiveRoomAlreadyExists):
		WriteError(c, 409, 31007, "商家已存在直播间", nil)
	default:
		writeServiceError(c, err)
	}
}

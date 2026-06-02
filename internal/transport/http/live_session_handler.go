package http

import (
	"context"
	"errors"
	"mime/multipart"
	"strconv"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/objectstorage"
	"aieas_backend/internal/service"

	"github.com/cloudwego/hertz/pkg/app"
)

// LiveSessionHandler 暴露直播场次（live_session）相关读接口。
type LiveSessionHandler struct {
	sessions *service.LiveSessionService
	uploader objectstorage.Uploader
}

func NewLiveSessionHandler(sessions *service.LiveSessionService, uploader objectstorage.Uploader) *LiveSessionHandler {
	if uploader == nil {
		uploader = objectstorage.DisabledUploader{}
	}
	return &LiveSessionHandler{sessions: sessions, uploader: uploader}
}

type liveSessionCreateRequest struct {
	MerchantID         string                   `json:"merchantId"`
	Title              string                   `json:"title"`
	Description        string                   `json:"description"`
	CoverURL           string                   `json:"coverUrl"`
	Status             domain.LiveSessionStatus `json:"status"`
	ScheduledStartTime *time.Time               `json:"scheduledStartTime"`
	PlannedDurationSec int                      `json:"plannedDurationSec"`
}

type liveSessionPatchRequest struct {
	Title              *string                   `json:"title"`
	Description        *string                   `json:"description"`
	CoverURL           *string                   `json:"coverUrl"`
	Status             *domain.LiveSessionStatus `json:"status"`
	ScheduledStartTime *time.Time                `json:"scheduledStartTime"`
	PlannedDurationSec *int                      `json:"plannedDurationSec"`
}

type liveSessionActivateRequest struct {
	AuctionID   uint64 `json:"auctionId"`
	DurationSec int    `json:"durationSec"`
}

type liveSessionMountRequest struct {
	AuctionID uint64 `json:"auctionId"`
}

type liveSessionAgentHookPatchRequest struct {
	Enabled *bool `json:"enabled"`
}

func (h *LiveSessionHandler) Create(ctx context.Context, c *app.RequestContext) {
	var req liveSessionCreateRequest
	if err := c.BindJSON(&req); err != nil {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	session, err := h.sessions.Create(ctx, service.CreateLiveSessionInput{ActorID: AuthUserID(c), ActorRole: AuthRole(c), MerchantID: req.MerchantID, Title: req.Title, Description: req.Description, CoverURL: req.CoverURL, Status: req.Status, ScheduledStartTime: req.ScheduledStartTime, PlannedDurationSec: req.PlannedDurationSec})
	if err != nil {
		writeLiveSessionError(c, err)
		return
	}
	WriteSuccess(c, session)
}

func (h *LiveSessionHandler) List(ctx context.Context, c *app.RequestContext) {
	status := domain.LiveSessionStatus(strings.TrimSpace(c.Query("status")))
	if !status.Valid() {
		status = ""
	}
	sessions, err := h.sessions.ListVisible(ctx, strings.TrimSpace(c.Query("merchantId")), status, AuthUserID(c), AuthRole(c), parseQueryInt(c, "limit", 20), parseQueryInt(c, "offset", 0))
	if err != nil {
		writeLiveSessionError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"sessions": sessions})
}

func (h *LiveSessionHandler) Update(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	var req liveSessionPatchRequest
	if err := c.BindJSON(&req); err != nil {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	session, err := h.sessions.Update(ctx, id, service.UpdateLiveSessionInput{ActorID: AuthUserID(c), ActorRole: AuthRole(c), Title: req.Title, Description: req.Description, CoverURL: req.CoverURL, Status: req.Status, ScheduledStartTime: req.ScheduledStartTime, PlannedDurationSec: req.PlannedDurationSec})
	if err != nil {
		writeLiveSessionError(c, err)
		return
	}
	WriteSuccess(c, session)
}

func (h *LiveSessionHandler) Start(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	session, err := h.sessions.Start(ctx, id, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeLiveSessionError(c, err)
		return
	}
	WriteSuccess(c, session)
}

func (h *LiveSessionHandler) End(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	session, err := h.sessions.End(ctx, id, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeLiveSessionError(c, err)
		return
	}
	WriteSuccess(c, session)
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
	id, ok := liveSessionIDParam(c)
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
	id, ok := liveSessionIDParam(c)
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
	id, ok := liveSessionIDParam(c)
	if !ok {
		return
	}
	limit := parseQueryInt(c, "limit", 50)
	if auctionIDRaw := strings.TrimSpace(c.Query("auctionId")); auctionIDRaw != "" {
		auctionID, err := strconv.ParseUint(auctionIDRaw, 10, 64)
		if err != nil || auctionID == 0 {
			WriteError(c, 400, 20001, "参数不合法", nil)
			return
		}
		bids, err := h.sessions.ListAuctionBids(ctx, id, auctionID, limit, AuthUserID(c), AuthRole(c))
		if err != nil {
			writeLiveSessionError(c, err)
			return
		}
		WriteSuccess(c, map[string]interface{}{"bids": bids})
		return
	}
	bids, err := h.sessions.ListBids(ctx, id, limit, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeLiveSessionError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"bids": bids})
}

// Orders 返回某场次的订单列表：GET /live-sessions/:sessionId/orders
func (h *LiveSessionHandler) Orders(ctx context.Context, c *app.RequestContext) {
	id, ok := liveSessionIDParam(c)
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

func (h *LiveSessionHandler) Stats(ctx context.Context, c *app.RequestContext) {
	id, ok := liveSessionIDParam(c)
	if !ok {
		return
	}
	stats, err := h.sessions.Stats(ctx, id, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeLiveSessionError(c, err)
		return
	}
	WriteSuccess(c, stats)
}

func (h *LiveSessionHandler) MountLot(ctx context.Context, c *app.RequestContext) {
	id, ok := liveSessionIDParam(c)
	if !ok {
		return
	}
	var req liveSessionMountRequest
	if err := c.BindJSON(&req); err != nil || req.AuctionID == 0 {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	lot, err := h.sessions.MountAuction(ctx, id, req.AuctionID, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeLiveSessionError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"lot": lot})
}

func (h *LiveSessionHandler) UnmountLot(ctx context.Context, c *app.RequestContext) {
	id, ok := liveSessionIDParam(c)
	if !ok {
		return
	}
	auctionID, ok := parseUintParam(c, "auctionId")
	if !ok {
		return
	}
	if err := h.sessions.UnmountAuction(ctx, id, auctionID, AuthUserID(c), AuthRole(c)); err != nil {
		writeLiveSessionError(c, err)
		return
	}
	WriteSuccess(c, map[string]bool{"removed": true})
}

func (h *LiveSessionHandler) Activate(ctx context.Context, c *app.RequestContext) {
	id, ok := liveSessionIDParam(c)
	if !ok {
		return
	}
	var req liveSessionActivateRequest
	if err := c.BindJSON(&req); err != nil || req.AuctionID == 0 {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	lot, err := h.sessions.ActivateAuctionWithOptions(ctx, service.ActivateLiveSessionAuctionInput{SessionID: id, AuctionID: req.AuctionID, ActorID: AuthUserID(c), ActorRole: AuthRole(c), DurationSec: req.DurationSec})
	if err != nil {
		writeLiveSessionError(c, err)
		return
	}
	WriteSuccess(c, lot)
}

func (h *LiveSessionHandler) Deactivate(ctx context.Context, c *app.RequestContext) {
	id, ok := liveSessionIDParam(c)
	if !ok {
		return
	}
	session, err := h.sessions.DeactivateAuction(ctx, id, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeLiveSessionError(c, err)
		return
	}
	WriteSuccess(c, session)
}

func (h *LiveSessionHandler) UploadCover(ctx context.Context, c *app.RequestContext) {
	id, ok := liveSessionIDParam(c)
	if !ok {
		return
	}
	if !isMultipartRequest(c) {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	fileHeader, err := c.FormFile("image")
	if err != nil {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	coverURL, err := h.uploadCover(ctx, fileHeader)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	session, err := h.sessions.Update(ctx, id, service.UpdateLiveSessionInput{ActorID: AuthUserID(c), ActorRole: AuthRole(c), CoverURL: &coverURL})
	if err != nil {
		writeLiveSessionError(c, err)
		return
	}
	WriteSuccess(c, session)
}

func (h *LiveSessionHandler) AgentHookConfig(ctx context.Context, c *app.RequestContext) {
	id, ok := liveSessionIDParam(c)
	if !ok {
		return
	}
	cfg, err := h.sessions.AgentHookConfig(ctx, id, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeLiveSessionError(c, err)
		return
	}
	WriteSuccess(c, cfg)
}

func (h *LiveSessionHandler) UpdateAgentHookConfig(ctx context.Context, c *app.RequestContext) {
	id, ok := liveSessionIDParam(c)
	if !ok {
		return
	}
	var req liveSessionAgentHookPatchRequest
	if err := c.BindJSON(&req); err != nil || req.Enabled == nil {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	cfg, err := h.sessions.UpdateAgentHookConfig(ctx, id, AuthUserID(c), AuthRole(c), *req.Enabled)
	if err != nil {
		writeLiveSessionError(c, err)
		return
	}
	WriteSuccess(c, cfg)
}

func (h *LiveSessionHandler) uploadCover(ctx context.Context, fileHeader *multipart.FileHeader) (string, error) {
	if fileHeader == nil || fileHeader.Size <= 0 || fileHeader.Size > maxImageUploadSizeBytes {
		return "", domain.ErrInvalidArgument
	}
	file, err := fileHeader.Open()
	if err != nil {
		return "", err
	}
	coverURL, uploadErr := h.uploader.Upload(ctx, objectstorage.UploadInput{Filename: fileHeader.Filename, ContentType: imageContentType(fileHeader), Size: fileHeader.Size, Body: file})
	closeErr := file.Close()
	if uploadErr != nil {
		return "", uploadErr
	}
	if closeErr != nil {
		return "", closeErr
	}
	return coverURL, nil
}

func liveSessionIDParam(c *app.RequestContext) (uint64, bool) {
	if id, ok := parseUintParam(c, "id"); ok {
		return id, true
	}
	return parseUintParam(c, "sessionId")
}

func writeLiveSessionError(c *app.RequestContext, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		WriteError(c, 404, 32001, "直播场次不存在", nil)
	case errors.Is(err, domain.ErrForbidden):
		WriteError(c, 403, 32002, "无直播场次操作权限", nil)
	case errors.Is(err, service.ErrLiveSessionLotInvalidState):
		WriteError(c, 409, 32005, "拍品状态不允许此操作", nil)
	case errors.Is(err, domain.ErrInvalidState):
		WriteError(c, 409, 32003, "直播场次状态不允许此操作", nil)
	case errors.Is(err, domain.ErrInvalidArgument):
		WriteError(c, 400, 20001, "参数不合法", nil)
	case errors.Is(err, service.ErrLiveSessionBusy), errors.Is(err, service.ErrLotAlreadyMounted):
		WriteError(c, 409, 32004, "直播场次当前拍品冲突", nil)
	default:
		writeServiceError(c, err)
	}
}

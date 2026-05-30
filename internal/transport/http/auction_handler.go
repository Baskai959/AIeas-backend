package http

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/service"

	"github.com/cloudwego/hertz/pkg/app"
)

type AuctionHandler struct {
	auctions *service.AuctionService
	deposits *service.DepositService
	hammers  *service.HammerService
}

func NewAuctionHandler(auctions *service.AuctionService, deposits *service.DepositService, hammers *service.HammerService) *AuctionHandler {
	return &AuctionHandler{auctions: auctions, deposits: deposits, hammers: hammers}
}

type auctionCreateRequest struct {
	AuctionID      uint64                   `json:"auctionId"`
	ItemID         uint64                   `json:"itemId"`
	SellerID       string                   `json:"sellerId"`
	AuctionType    domain.AuctionType       `json:"auctionType"`
	StartPrice     int64                    `json:"startPrice"`
	ReservePrice   int64                    `json:"reservePrice"`
	CapPrice       int64                    `json:"capPrice"`
	IncrementRule  json.RawMessage          `json:"incrementRule"`
	AntiSnipingSec int                      `json:"antiSnipingSec"`
	AntiExtendSec  int                      `json:"antiExtendSec"`
	AntiExtendMode domain.AuctionExtendMode `json:"antiExtendMode"`
	DepositAmount  int64                    `json:"depositAmount"`
	Status         domain.AuctionStatus     `json:"status"`
	StartTime      time.Time                `json:"startTime"`
	EndTime        time.Time                `json:"endTime"`
	DurationSec    int                      `json:"durationSec"`
}

type auctionPatchRequest struct {
	StartPrice     *int64                    `json:"startPrice"`
	ReservePrice   *int64                    `json:"reservePrice"`
	CapPrice       *int64                    `json:"capPrice"`
	IncrementRule  *json.RawMessage          `json:"incrementRule"`
	AntiSnipingSec *int                      `json:"antiSnipingSec"`
	AntiExtendSec  *int                      `json:"antiExtendSec"`
	AntiExtendMode *domain.AuctionExtendMode `json:"antiExtendMode"`
	DepositAmount  *int64                    `json:"depositAmount"`
	Status         *domain.AuctionStatus     `json:"status"`
	StartTime      *time.Time                `json:"startTime"`
	EndTime        *time.Time                `json:"endTime"`
	DurationSec    *int                      `json:"durationSec"`
}

func (h *AuctionHandler) Create(ctx context.Context, c *app.RequestContext) {
	var req auctionCreateRequest
	if err := c.BindJSON(&req); err != nil {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	auction, err := h.auctions.Create(ctx, service.CreateAuctionInput{
		ActorID:        AuthUserID(c),
		ActorRole:      AuthRole(c),
		AuctionID:      req.AuctionID,
		ItemID:         req.ItemID,
		SellerID:       req.SellerID,
		AuctionType:    req.AuctionType,
		StartPrice:     req.StartPrice,
		ReservePrice:   req.ReservePrice,
		CapPrice:       req.CapPrice,
		IncrementRule:  req.IncrementRule,
		AntiSnipingSec: req.AntiSnipingSec,
		AntiExtendSec:  req.AntiExtendSec,
		AntiExtendMode: req.AntiExtendMode,
		DepositAmount:  req.DepositAmount,
		Status:         req.Status,
		StartTime:      req.StartTime,
		EndTime:        req.EndTime,
		DurationSec:    req.DurationSec,
	})
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, auction)
}

func (h *AuctionHandler) List(ctx context.Context, c *app.RequestContext) {
	filter := domain.AuctionFilter{
		SellerID: strings.TrimSpace(c.Query("sellerId")),
		Limit:    parseQueryInt(c, "limit", 20),
		Offset:   parseQueryInt(c, "offset", 0),
	}
	if status := domain.AuctionStatus(strings.TrimSpace(c.Query("status"))); status.Valid() {
		filter.Status = status
	}
	if itemID, ok := parseOptionalUintQuery(c, "itemId"); ok {
		filter.ItemID = itemID
	}
	if sessionID, ok := parseOptionalUintQuery(c, "liveSessionId"); ok {
		filter.LiveSessionID = sessionID
	}
	auctions, err := h.auctions.List(ctx, filter, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"auctions": auctions})
}

func (h *AuctionHandler) Get(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	auction, err := h.auctions.Get(ctx, id, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, auction)
}

func (h *AuctionHandler) Update(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	var req auctionPatchRequest
	if err := c.BindJSON(&req); err != nil {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	auction, err := h.auctions.Update(ctx, id, service.UpdateAuctionInput{
		ActorID:        AuthUserID(c),
		ActorRole:      AuthRole(c),
		StartPrice:     req.StartPrice,
		ReservePrice:   req.ReservePrice,
		CapPrice:       req.CapPrice,
		IncrementRule:  req.IncrementRule,
		AntiSnipingSec: req.AntiSnipingSec,
		AntiExtendSec:  req.AntiExtendSec,
		AntiExtendMode: req.AntiExtendMode,
		DepositAmount:  req.DepositAmount,
		Status:         req.Status,
		StartTime:      req.StartTime,
		EndTime:        req.EndTime,
		DurationSec:    req.DurationSec,
	})
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, auction)
}

func (h *AuctionHandler) Delete(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	if err := h.auctions.Delete(ctx, id, AuthUserID(c), AuthRole(c)); err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, map[string]bool{"deleted": true})
}

func (h *AuctionHandler) Start(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	auction, err := h.auctions.Start(ctx, id, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, auction)
}

func (h *AuctionHandler) Enroll(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	if h.deposits == nil {
		WriteError(c, 500, 90001, "系统内部错误", nil)
		return
	}
	deposit, err := h.deposits.Enroll(ctx, service.EnrollInput{
		AuctionID: id,
		UserID:    AuthUserID(c),
		UserRole:  AuthRole(c),
	})
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, deposit)
}

func (h *AuctionHandler) Hammer(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	if h.hammers == nil {
		WriteError(c, 500, 90001, "系统内部错误", nil)
		return
	}
	result, order, err := h.hammers.Hammer(ctx, domain.HammerInput{
		RequestID: IdempotencyKey(c),
		AuctionID: id,
		ActorID:   AuthUserID(c),
		ActorRole: AuthRole(c),
		ClosedBy:  AuthUserID(c),
		Now:       time.Now().UTC(),
	})
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"result": result, "order": order})
}

func (h *AuctionHandler) Cancel(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	auction, err := h.auctions.Cancel(ctx, id, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, auction)
}

func (h *AuctionHandler) State(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	state, err := h.auctions.State(ctx, id, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, state)
}

func parseOptionalUintQuery(c *app.RequestContext, name string) (uint64, bool) {
	value := strings.TrimSpace(c.Query(name))
	if value == "" {
		return 0, false
	}
	id, err := parseUintString(value)
	if err != nil || id == 0 {
		return 0, false
	}
	return id, true
}

func parseUintString(value string) (uint64, error) {
	var id uint64
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return 0, domain.ErrInvalidArgument
		}
		id = id*10 + uint64(ch-'0')
	}
	return id, nil
}

package http

import (
	"context"
	"strings"

	"aieas_backend/internal/domain"

	"github.com/cloudwego/hertz/pkg/app"
)

type MarketplaceHandler struct {
	marketplace MarketplaceUseCase
}

func NewMarketplaceHandler(marketplace MarketplaceUseCase) *MarketplaceHandler {
	return &MarketplaceHandler{marketplace: marketplace}
}

func (h *MarketplaceHandler) SearchLots(ctx context.Context, c *app.RequestContext) {
	status, ok := auctionStatusQuery(c, "status")
	if !ok {
		return
	}
	lots, total, err := h.marketplace.SearchLots(ctx, domain.AuctionSearchFilter{
		Keyword:    strings.TrimSpace(c.Query("keyword")),
		Sort:       strings.TrimSpace(c.Query("sort")),
		Status:     status,
		CategoryID: strings.TrimSpace(c.Query("categoryId")),
		MerchantID: strings.TrimSpace(c.Query("merchantId")),
		Limit:      parseQueryInt(c, "limit", 20),
		Offset:     parseQueryInt(c, "offset", 0),
	})
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"lots": lots, "total": total})
}

func (h *MarketplaceHandler) Lot(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	lot, err := h.marketplace.GetLot(ctx, id)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, lot)
}

func (h *MarketplaceHandler) MyParticipations(ctx context.Context, c *app.RequestContext) {
	records, err := h.marketplace.MyParticipations(ctx, AuthUserID(c), AuthRole(c), parseQueryInt(c, "limit", 20), parseQueryInt(c, "offset", 0))
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"records": records})
}

func (h *MarketplaceHandler) Categories(ctx context.Context, c *app.RequestContext) {
	WriteSuccess(c, map[string]interface{}{"categories": h.marketplace.Categories(ctx)})
}

func (h *MarketplaceHandler) SearchMerchants(ctx context.Context, c *app.RequestContext) {
	merchants, err := h.marketplace.SearchMerchants(ctx, strings.TrimSpace(c.Query("keyword")), parseQueryInt(c, "limit", 20), parseQueryInt(c, "offset", 0))
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"merchants": merchants})
}

func (h *MarketplaceHandler) Merchant(ctx context.Context, c *app.RequestContext) {
	merchantID := strings.TrimSpace(c.Param("id"))
	if merchantID == "" {
		merchantID = strings.TrimSpace(c.Param("merchantId"))
	}
	merchant, err := h.marketplace.GetMerchant(ctx, merchantID)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, merchant)
}

func auctionStatusQuery(c *app.RequestContext, name string) (domain.AuctionStatus, bool) {
	raw := strings.TrimSpace(c.Query(name))
	if raw == "" {
		return "", true
	}
	status := domain.AuctionStatus(raw)
	if !status.Valid() {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return "", false
	}
	return status, true
}

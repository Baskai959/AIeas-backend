package http

import (
	"context"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/service"

	"github.com/cloudwego/hertz/pkg/app"
)

type AdminHandler struct {
	admin *service.AdminService
}

func NewAdminHandler(admin *service.AdminService) *AdminHandler {
	return &AdminHandler{admin: admin}
}

type adminAuditAuctionRequest struct {
	AuditResult string `json:"auditResult"`
	Reason      string `json:"reason"`
}

type adminReasonRequest struct {
	Reason string `json:"reason"`
}

type adminUpdateUserRequest struct {
	Status    domain.UserStatus `json:"status"`
	RiskLevel string            `json:"riskLevel"`
	Reason    string            `json:"reason"`
}

type adminBlacklistRequest struct {
	UserID   string     `json:"userId"`
	Reason   string     `json:"reason"`
	ExpireAt *time.Time `json:"expireAt"`
}

type adminRiskEventRequest struct {
	Status       domain.RiskEventStatus `json:"status"`
	HandleResult string                 `json:"handleResult"`
	Remark       string                 `json:"remark"`
}

type adminUserView struct {
	ID        string            `json:"id"`
	Nickname  string            `json:"nickname"`
	Role      domain.Role       `json:"role"`
	Status    domain.UserStatus `json:"status,omitempty"`
	RiskLevel string            `json:"riskLevel"`
}

func (h *AdminHandler) ListAuctions(ctx context.Context, c *app.RequestContext) {
	filter := domain.AuctionFilter{SellerID: strings.TrimSpace(c.Query("merchantId")), Limit: adminPageSize(c), Offset: adminOffset(c)}
	if status := domain.AuctionStatus(strings.TrimSpace(c.Query("status"))); status.Valid() {
		filter.Status = status
	}
	auctions, err := h.admin.ListAuctions(ctx, filter)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, adminPageData("items", auctions, c))
}

func (h *AdminHandler) AuditAuction(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	var req adminAuditAuctionRequest
	if err := c.BindJSON(&req); err != nil {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	result := strings.ToUpper(strings.TrimSpace(req.AuditResult))
	if result != "APPROVED" && result != "REJECTED" {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	auction, err := h.admin.AuditAuction(ctx, id, result == "APPROVED", AuthUserID(c))
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"id": auction.AuctionID, "auditStatus": result, "status": auction.Status})
}

func (h *AdminHandler) CancelAuction(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	var req adminReasonRequest
	_ = c.BindJSON(&req)
	auction, err := h.admin.CancelAuction(ctx, id, AuthUserID(c))
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"id": auction.AuctionID, "status": auction.Status})
}

func (h *AdminHandler) CloseAuction(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	var req adminReasonRequest
	_ = c.BindJSON(&req)
	result, order, err := h.admin.CloseAuction(ctx, id, AuthUserID(c), IdempotencyKey(c))
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"result": result, "order": order})
}

func (h *AdminHandler) ListUsers(ctx context.Context, c *app.RequestContext) {
	_ = ctx
	filter := domain.UserFilter{Role: domain.Role(strings.TrimSpace(c.Query("role"))), Status: domain.UserStatus(strings.TrimSpace(c.Query("status"))), Keyword: strings.TrimSpace(c.Query("keyword")), Limit: adminPageSize(c), Offset: adminOffset(c)}
	users, err := h.admin.ListUsers(filter)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	items := make([]adminUserView, 0, len(users))
	for _, user := range users {
		items = append(items, adminUserView{ID: user.ID, Nickname: user.Nickname, Role: user.Role, Status: user.Status, RiskLevel: "LOW"})
	}
	WriteSuccess(c, adminPageData("items", items, c))
}

func (h *AdminHandler) UpdateUser(ctx context.Context, c *app.RequestContext) {
	_ = ctx
	userID := strings.TrimSpace(c.Param("id"))
	var req adminUpdateUserRequest
	if err := c.BindJSON(&req); err != nil {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	user, err := h.admin.UpdateUserStatus(userID, req.Status)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	riskLevel := strings.TrimSpace(req.RiskLevel)
	if riskLevel == "" {
		riskLevel = "LOW"
	}
	WriteSuccess(c, adminUserView{ID: user.ID, Nickname: user.Nickname, Role: user.Role, Status: user.Status, RiskLevel: riskLevel})
}

func (h *AdminHandler) AddBlacklist(ctx context.Context, c *app.RequestContext) {
	var req adminBlacklistRequest
	if err := c.BindJSON(&req); err != nil {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	if err := h.admin.AddBlacklist(ctx, req.UserID, req.Reason, AuthUserID(c), req.ExpireAt); err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"userId": strings.TrimSpace(req.UserID), "blacklisted": true})
}

func (h *AdminHandler) RemoveBlacklist(ctx context.Context, c *app.RequestContext) {
	userID := strings.TrimSpace(c.Param("user_id"))
	if err := h.admin.RemoveBlacklist(ctx, userID); err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"userId": userID, "blacklisted": false})
}

func (h *AdminHandler) ListBlacklist(ctx context.Context, c *app.RequestContext) {
	items, err := h.admin.ListBlacklist(ctx, adminPageSize(c), adminOffset(c))
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, adminPageData("items", items, c))
}

func (h *AdminHandler) ListOrders(ctx context.Context, c *app.RequestContext) {
	filter := orderFilterFromRequest(c)
	orders, err := h.admin.ListOrders(ctx, filter)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, adminPageData("items", orders, c))
}

func (h *AdminHandler) ListAuditLogs(ctx context.Context, c *app.RequestContext) {
	filter := domain.AuditFilter{OperatorID: strings.TrimSpace(c.Query("operatorId")), Action: strings.TrimSpace(c.Query("action")), Limit: adminPageSize(c), Offset: adminOffset(c)}
	if start, ok := parseTimeQuery(c, "startTime"); ok {
		filter.StartTime = &start
	}
	if end, ok := parseTimeQuery(c, "endTime"); ok {
		filter.EndTime = &end
	}
	logs, err := h.admin.ListAuditLogs(ctx, filter)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, adminPageData("items", logs, c))
}

func (h *AdminHandler) ListOwnAuditLogs(ctx context.Context, c *app.RequestContext) {
	filter := domain.AuditFilter{OperatorID: AuthUserID(c), Action: strings.TrimSpace(c.Query("action")), Limit: adminPageSize(c), Offset: adminOffset(c)}
	if start, ok := parseTimeQuery(c, "startTime"); ok {
		filter.StartTime = &start
	}
	if end, ok := parseTimeQuery(c, "endTime"); ok {
		filter.EndTime = &end
	}
	logs, err := h.admin.ListAuditLogs(ctx, filter)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, adminPageData("items", logs, c))
}

func (h *AdminHandler) ListRiskEvents(ctx context.Context, c *app.RequestContext) {
	filter := domain.RiskEventFilter{Status: normalizeRiskEventStatus(c.Query("status")), EventType: strings.TrimSpace(firstNonEmpty(c.Query("riskType"), c.Query("eventType"))), UserID: strings.TrimSpace(c.Query("userId")), Limit: adminPageSize(c), Offset: adminOffset(c)}
	events, err := h.admin.ListRiskEvents(ctx, filter)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, adminPageData("items", events, c))
}

func (h *AdminHandler) HandleRiskEvent(ctx context.Context, c *app.RequestContext) {
	id, ok := parseUintParam(c, "id")
	if !ok {
		return
	}
	var req adminRiskEventRequest
	if err := c.BindJSON(&req); err != nil {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	event, err := h.admin.HandleRiskEvent(ctx, id, normalizeRiskEventStatus(string(req.Status)), AuthUserID(c))
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, event)
}

func adminPage(c *app.RequestContext) int {
	page := parseQueryInt(c, "page", 1)
	if page <= 0 {
		return 1
	}
	return page
}

func adminPageSize(c *app.RequestContext) int {
	if limit := parseQueryInt(c, "limit", 0); limit > 0 {
		return limit
	}
	size := parseQueryInt(c, "page_size", 20)
	if size <= 0 || size > 100 {
		return 20
	}
	return size
}

func adminOffset(c *app.RequestContext) int {
	if offset := parseQueryInt(c, "offset", -1); offset >= 0 {
		return offset
	}
	return (adminPage(c) - 1) * adminPageSize(c)
}

func adminPageData(key string, items interface{}, c *app.RequestContext) map[string]interface{} {
	return map[string]interface{}{key: items, "total": sliceLen(items), "page": adminPage(c), "page_size": adminPageSize(c)}
}

func sliceLen(items interface{}) int {
	switch v := items.(type) {
	case []domain.AuctionLot:
		return len(v)
	case []domain.OrderDeal:
		return len(v)
	case []domain.AuditLog:
		return len(v)
	case []domain.Blacklist:
		return len(v)
	case []domain.RiskEvent:
		return len(v)
	case []adminUserView:
		return len(v)
	default:
		return 0
	}
}

func parseTimeQuery(c *app.RequestContext, name string) (time.Time, bool) {
	value := strings.TrimSpace(c.Query(name))
	if value == "" {
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}

func normalizeRiskEventStatus(status string) domain.RiskEventStatus {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "OPEN", "PENDING":
		return domain.RiskEventPending
	case "RESOLVED", "REVIEWED":
		return domain.RiskEventReviewed
	case "IGNORED":
		return domain.RiskEventIgnored
	default:
		return domain.RiskEventStatus(strings.TrimSpace(status))
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

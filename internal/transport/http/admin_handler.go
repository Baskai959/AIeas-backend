package http

import (
	"context"
	"errors"
	"strconv"
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

type adminBlacklistStrategyRequest struct {
	Enabled                  *bool  `json:"enabled"`
	FrequencyEnabled         *bool  `json:"frequencyEnabled"`
	FrequencyWindowMs        *int64 `json:"frequencyWindowMs"`
	FrequencyMaxRequests     *int   `json:"frequencyMaxRequests"`
	UnreasonablePriceEnabled *bool  `json:"unreasonablePriceEnabled"`
	MissingDepositEnabled    *bool  `json:"missingDepositEnabled"`
	BlacklistDurationSeconds *int64 `json:"blacklistDurationSeconds"`
}

type adminFeatureFlagRequest struct {
	Enabled           *bool    `json:"enabled"`
	RolloutPercentage *int     `json:"rolloutPercentage"`
	Allowlist         []string `json:"allowlist"`
	Description       *string  `json:"description"`
}

type adminRiskEventRequest struct {
	Status       domain.RiskEventStatus `json:"status"`
	HandleResult string                 `json:"handleResult"`
	Remark       string                 `json:"remark"`
}

type adminUserView struct {
	ID          string            `json:"id"`
	Nickname    string            `json:"nickname"`
	Role        domain.Role       `json:"role"`
	Status      domain.UserStatus `json:"status,omitempty"`
	RiskLevel   string            `json:"riskLevel"`
	Blacklisted bool              `json:"blacklisted"`
}

type adminBlacklistView struct {
	ID                uint64            `json:"id"`
	UserID            string            `json:"userId"`
	Nickname          string            `json:"nickname"`
	Role              domain.Role       `json:"role,omitempty"`
	Status            domain.UserStatus `json:"status,omitempty"`
	Reason            string            `json:"reason"`
	CreatedBy         string            `json:"createdBy"`
	CreatedByName     string            `json:"createdByName"`
	CreatedByNickname string            `json:"createdByNickname"`
	CreatedAt         time.Time         `json:"createdAt"`
	ExpiresAt         *time.Time        `json:"expiresAt,omitempty"`
}

type adminAuctionView struct {
	domain.AuctionLot
	SellerNickname       string `json:"sellerNickname"`
	LiveSessionName      string `json:"liveSessionName"`
	LeaderBidderNickname string `json:"leaderBidderNickname"`
	WinnerNickname       string `json:"winnerNickname"`
}

type adminOrderView struct {
	domain.OrderDeal
	WinnerNickname  string `json:"winnerNickname"`
	SellerNickname  string `json:"sellerNickname"`
	LiveSessionName string `json:"liveSessionName"`
	AuctionName     string `json:"auctionName"`
	AuctionTitle    string `json:"auctionTitle"`
}

type adminAuditLogView struct {
	domain.AuditLog
	OperatorName     string `json:"operatorName"`
	OperatorNickname string `json:"operatorNickname"`
	TargetName       string `json:"targetName"`
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
	resolver := newAdminNameResolver(ctx, h)
	items := make([]adminAuctionView, 0, len(auctions))
	for _, auction := range auctions {
		view, err := resolver.auctionView(auction)
		if err != nil {
			writeServiceError(c, err)
			return
		}
		items = append(items, view)
	}
	WriteSuccess(c, adminPageData("items", items, c))
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
	filter := domain.UserFilter{Role: domain.Role(strings.TrimSpace(c.Query("role"))), Status: domain.UserStatus(strings.TrimSpace(c.Query("status"))), Keyword: strings.TrimSpace(c.Query("keyword")), Limit: adminPageSize(c), Offset: adminOffset(c)}
	users, err := h.admin.ListUsers(filter)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	items := make([]adminUserView, 0, len(users))
	for _, user := range users {
		blacklisted, err := h.admin.IsBlacklisted(ctx, user.ID)
		if err != nil {
			writeServiceError(c, err)
			return
		}
		items = append(items, adminUserView{ID: user.ID, Nickname: user.Nickname, Role: user.Role, Status: user.Status, RiskLevel: "LOW", Blacklisted: blacklisted})
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
	views := make([]adminBlacklistView, 0, len(items))
	resolver := newAdminNameResolver(ctx, h)
	for _, item := range items {
		user, err := resolver.user(item.UserID)
		if err != nil {
			writeServiceError(c, err)
			return
		}
		creator, err := resolver.user(item.CreatedBy)
		if err != nil {
			writeServiceError(c, err)
			return
		}
		nickname := user.Nickname
		if nickname == "" {
			nickname = item.UserID
		}
		createdByName := creator.Nickname
		if createdByName == "" {
			createdByName = item.CreatedBy
		}
		views = append(views, adminBlacklistView{
			ID:                item.ID,
			UserID:            item.UserID,
			Nickname:          nickname,
			Role:              user.Role,
			Status:            user.Status,
			Reason:            item.Reason,
			CreatedBy:         item.CreatedBy,
			CreatedByName:     createdByName,
			CreatedByNickname: createdByName,
			CreatedAt:         item.CreatedAt,
			ExpiresAt:         item.ExpiresAt,
		})
	}
	WriteSuccess(c, adminPageData("items", views, c))
}

func (h *AdminHandler) BlacklistStrategyConfig(ctx context.Context, c *app.RequestContext) {
	cfg, err := h.admin.BlacklistStrategyConfig(ctx)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, cfg)
}

func (h *AdminHandler) UpdateBlacklistStrategyConfig(ctx context.Context, c *app.RequestContext) {
	var req adminBlacklistStrategyRequest
	if err := c.BindJSON(&req); err != nil {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	base, err := h.admin.BlacklistStrategyConfig(ctx)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	cfg, err := h.admin.UpdateBlacklistStrategyConfig(ctx, applyBlacklistStrategyRequest(base, req), AuthUserID(c))
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, cfg)
}

func applyBlacklistStrategyRequest(cfg domain.BlacklistStrategyConfig, req adminBlacklistStrategyRequest) domain.BlacklistStrategyConfig {
	if req.Enabled != nil {
		cfg.Enabled = *req.Enabled
	}
	if req.FrequencyEnabled != nil {
		cfg.FrequencyEnabled = *req.FrequencyEnabled
	}
	if req.FrequencyWindowMs != nil {
		cfg.FrequencyWindowMs = *req.FrequencyWindowMs
	}
	if req.FrequencyMaxRequests != nil {
		cfg.FrequencyMaxRequests = *req.FrequencyMaxRequests
	}
	if req.UnreasonablePriceEnabled != nil {
		cfg.UnreasonablePriceEnabled = *req.UnreasonablePriceEnabled
	}
	if req.MissingDepositEnabled != nil {
		cfg.MissingDepositEnabled = *req.MissingDepositEnabled
	}
	if req.BlacklistDurationSeconds != nil {
		cfg.BlacklistDurationSeconds = *req.BlacklistDurationSeconds
	}
	return cfg
}

func (h *AdminHandler) FeatureFlag(ctx context.Context, c *app.RequestContext) {
	flag, err := h.admin.FeatureFlag(ctx, strings.TrimSpace(c.Param("key")))
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, flag)
}

func (h *AdminHandler) UpdateFeatureFlag(ctx context.Context, c *app.RequestContext) {
	key := strings.TrimSpace(c.Param("key"))
	base, err := h.admin.FeatureFlag(ctx, key)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	var req adminFeatureFlagRequest
	if err := c.BindJSON(&req); err != nil {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	if req.Enabled != nil {
		base.Enabled = *req.Enabled
	}
	if req.RolloutPercentage != nil {
		base.RolloutPercentage = *req.RolloutPercentage
	}
	if req.Allowlist != nil {
		base.Allowlist = req.Allowlist
	}
	if req.Description != nil {
		base.Description = strings.TrimSpace(*req.Description)
	}
	flag, err := h.admin.UpdateFeatureFlag(ctx, base, AuthUserID(c))
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, flag)
}

func (h *AdminHandler) ListOrders(ctx context.Context, c *app.RequestContext) {
	filter := orderFilterFromRequest(c)
	orders, err := h.admin.ListOrders(ctx, filter)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	resolver := newAdminNameResolver(ctx, h)
	items := make([]adminOrderView, 0, len(orders))
	for _, order := range orders {
		view, err := resolver.orderView(order)
		if err != nil {
			writeServiceError(c, err)
			return
		}
		items = append(items, view)
	}
	WriteSuccess(c, adminPageData("items", items, c))
}

func (h *AdminHandler) DashboardMetrics(ctx context.Context, c *app.RequestContext) {
	startTime, ok := parseDashboardTimeQuery(c, "startTime")
	if !ok {
		return
	}
	endTime, ok := parseDashboardTimeQuery(c, "endTime")
	if !ok {
		return
	}
	metrics, err := h.admin.DashboardMetrics(ctx, startTime, endTime, strings.TrimSpace(c.Query("bucket")))
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, metrics)
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
	resolver := newAdminNameResolver(ctx, h)
	items := make([]adminAuditLogView, 0, len(logs))
	for _, log := range logs {
		view, err := resolver.auditLogView(log)
		if err != nil {
			writeServiceError(c, err)
			return
		}
		items = append(items, view)
	}
	WriteSuccess(c, adminPageData("items", items, c))
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
	resolver := newAdminNameResolver(ctx, h)
	items := make([]adminAuditLogView, 0, len(logs))
	for _, log := range logs {
		view, err := resolver.auditLogView(log)
		if err != nil {
			writeServiceError(c, err)
			return
		}
		items = append(items, view)
	}
	WriteSuccess(c, adminPageData("items", items, c))
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

type adminNameResolver struct {
	ctx      context.Context
	handler  *AdminHandler
	users    map[string]domain.SafeUser
	sessions map[uint64]domain.LiveSession
	auctions map[uint64]domain.AuctionLot
}

func newAdminNameResolver(ctx context.Context, h *AdminHandler) *adminNameResolver {
	return &adminNameResolver{
		ctx:      ctx,
		handler:  h,
		users:    make(map[string]domain.SafeUser),
		sessions: make(map[uint64]domain.LiveSession),
		auctions: make(map[uint64]domain.AuctionLot),
	}
}

func (r *adminNameResolver) auctionView(auction domain.AuctionLot) (adminAuctionView, error) {
	seller, err := r.user(auction.SellerID)
	if err != nil {
		return adminAuctionView{}, err
	}
	var session domain.LiveSession
	if auction.LiveSessionID != nil {
		session, err = r.session(*auction.LiveSessionID)
		if err != nil {
			return adminAuctionView{}, err
		}
	}
	leader, err := r.user(auction.LeaderBidderID)
	if err != nil {
		return adminAuctionView{}, err
	}
	var winner domain.SafeUser
	if auction.WinnerID != nil {
		winner, err = r.user(*auction.WinnerID)
		if err != nil {
			return adminAuctionView{}, err
		}
	}
	return adminAuctionView{
		AuctionLot:           auction,
		SellerNickname:       seller.Nickname,
		LiveSessionName:      session.Title,
		LeaderBidderNickname: leader.Nickname,
		WinnerNickname:       winner.Nickname,
	}, nil
}

func (r *adminNameResolver) orderView(order domain.OrderDeal) (adminOrderView, error) {
	winner, err := r.user(order.WinnerID)
	if err != nil {
		return adminOrderView{}, err
	}
	seller, err := r.user(order.SellerID)
	if err != nil {
		return adminOrderView{}, err
	}
	var session domain.LiveSession
	if order.LiveSessionID != nil {
		session, err = r.session(*order.LiveSessionID)
		if err != nil {
			return adminOrderView{}, err
		}
	}
	auction, err := r.auction(order.AuctionID)
	if err != nil {
		return adminOrderView{}, err
	}
	title := auction.Title
	return adminOrderView{
		OrderDeal:       order,
		WinnerNickname:  winner.Nickname,
		SellerNickname:  seller.Nickname,
		LiveSessionName: session.Title,
		AuctionName:     title,
		AuctionTitle:    title,
	}, nil
}

func (r *adminNameResolver) auditLogView(log domain.AuditLog) (adminAuditLogView, error) {
	operator, err := r.user(log.OperatorID)
	if err != nil {
		return adminAuditLogView{}, err
	}
	targetName, err := r.targetName(log)
	if err != nil {
		return adminAuditLogView{}, err
	}
	return adminAuditLogView{
		AuditLog:         log,
		OperatorName:     operator.Nickname,
		OperatorNickname: operator.Nickname,
		TargetName:       targetName,
	}, nil
}

func (r *adminNameResolver) user(userID string) (domain.SafeUser, error) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return domain.SafeUser{}, nil
	}
	if user, ok := r.users[userID]; ok {
		return user, nil
	}
	user, err := r.handler.admin.UserByID(userID)
	if err != nil {
		if errors.Is(err, domain.ErrUserNotFound) || errors.Is(err, domain.ErrInvalidArgument) {
			r.users[userID] = domain.SafeUser{}
			return domain.SafeUser{}, nil
		}
		return domain.SafeUser{}, err
	}
	r.users[userID] = user
	return user, nil
}

func (r *adminNameResolver) session(sessionID uint64) (domain.LiveSession, error) {
	if sessionID == 0 {
		return domain.LiveSession{}, nil
	}
	if session, ok := r.sessions[sessionID]; ok {
		return session, nil
	}
	session, err := r.handler.admin.LiveSessionByID(r.ctx, sessionID)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrInvalidArgument) {
			r.sessions[sessionID] = domain.LiveSession{}
			return domain.LiveSession{}, nil
		}
		return domain.LiveSession{}, err
	}
	r.sessions[sessionID] = session
	return session, nil
}

func (r *adminNameResolver) auction(auctionID uint64) (domain.AuctionLot, error) {
	if auctionID == 0 {
		return domain.AuctionLot{}, nil
	}
	if auction, ok := r.auctions[auctionID]; ok {
		return auction, nil
	}
	auction, err := r.handler.admin.AuctionByID(r.ctx, auctionID)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrInvalidArgument) {
			r.auctions[auctionID] = domain.AuctionLot{}
			return domain.AuctionLot{}, nil
		}
		return domain.AuctionLot{}, err
	}
	r.auctions[auctionID] = auction
	return auction, nil
}

func (r *adminNameResolver) targetName(log domain.AuditLog) (string, error) {
	targetID := strings.TrimSpace(log.TargetID)
	switch strings.ToUpper(strings.TrimSpace(log.TargetType)) {
	case "USER":
		return r.userDisplayName(targetID)
	case "ITEM":
		return targetID, nil
	case "AUCTION", "AUCTION_LOT":
		return r.auctionDisplayName(targetID)
	case "LIVE_SESSION":
		return r.sessionDisplayName(targetID)
	case "HTTP":
		return r.httpTargetName(targetID)
	default:
		if targetID == "" {
			return strings.TrimSpace(log.Action), nil
		}
		return targetID, nil
	}
}

func (r *adminNameResolver) httpTargetName(path string) (string, error) {
	cleanPath := strings.Split(strings.TrimSpace(path), "?")[0]
	segments := pathSegments(cleanPath)
	for i, segment := range segments {
		if i+1 >= len(segments) {
			continue
		}
		next := segments[i+1]
		switch segment {
		case "users", "blacklist":
			return r.userDisplayName(next)
		case "auctions":
			return r.auctionDisplayName(next)
		case "live-sessions":
			return r.sessionDisplayName(next)
		case "orders":
			if next != "mine" {
				return "订单 " + next, nil
			}
		}
	}
	if cleanPath == "" {
		return path, nil
	}
	return cleanPath, nil
}

func (r *adminNameResolver) userDisplayName(userID string) (string, error) {
	user, err := r.user(userID)
	if err != nil {
		return "", err
	}
	if user.Nickname != "" {
		return user.Nickname, nil
	}
	return strings.TrimSpace(userID), nil
}

func (r *adminNameResolver) auctionDisplayName(auctionID string) (string, error) {
	id, err := strconv.ParseUint(strings.TrimSpace(auctionID), 10, 64)
	if err != nil || id == 0 {
		return strings.TrimSpace(auctionID), nil
	}
	auction, err := r.auction(id)
	if err != nil {
		return "", err
	}
	if auction.Title != "" {
		return auction.Title, nil
	}
	return strings.TrimSpace(auctionID), nil
}

func (r *adminNameResolver) sessionDisplayName(sessionID string) (string, error) {
	id, err := strconv.ParseUint(strings.TrimSpace(sessionID), 10, 64)
	if err != nil || id == 0 {
		return strings.TrimSpace(sessionID), nil
	}
	session, err := r.session(id)
	if err != nil {
		return "", err
	}
	if session.Title != "" {
		return session.Title, nil
	}
	return strings.TrimSpace(sessionID), nil
}

func pathSegments(path string) []string {
	parts := strings.Split(path, "/")
	segments := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			segments = append(segments, part)
		}
	}
	return segments
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
	case []adminAuctionView:
		return len(v)
	case []domain.OrderDeal:
		return len(v)
	case []adminOrderView:
		return len(v)
	case []domain.AuditLog:
		return len(v)
	case []adminAuditLogView:
		return len(v)
	case []domain.Blacklist:
		return len(v)
	case []domain.RiskEvent:
		return len(v)
	case []adminUserView:
		return len(v)
	case []adminBlacklistView:
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

func parseDashboardTimeQuery(c *app.RequestContext, name string) (*time.Time, bool) {
	value := strings.TrimSpace(c.Query(name))
	if value == "" {
		return nil, true
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return nil, false
	}
	return &parsed, true
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

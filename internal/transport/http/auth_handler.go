package http

import (
	"context"
	"strings"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/service"

	"github.com/cloudwego/hertz/pkg/app"
)

const (
	contextUserID = "auth_user_id"
	contextRole   = "auth_role"
	contextToken  = "auth_token"
	contextExp    = "auth_exp"
)

type AuthHandler struct {
	auth *service.AuthService
}

func NewAuthHandler(auth *service.AuthService) *AuthHandler {
	return &AuthHandler{auth: auth}
}

type loginRequest struct {
	Account  string      `json:"account"`
	Password string      `json:"password"`
	Role     domain.Role `json:"role"`
}

type refreshRequest struct {
	RefreshToken string `json:"refreshToken"`
}

type logoutRequest struct {
	RefreshToken string `json:"refreshToken"`
}

func (h *AuthHandler) Login(ctx context.Context, c *app.RequestContext) {
	var req loginRequest
	if err := c.BindJSON(&req); err != nil || req.Account == "" || req.Password == "" || !req.Role.Valid() {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	result, err := h.auth.Login(req.Account, req.Password, req.Role)
	if err != nil {
		status, code, msg := service.HTTPStatusAndCode(err)
		WriteError(c, status, code, msg, nil)
		return
	}
	WriteSuccess(c, result)
}

func (h *AuthHandler) AdminLogin(ctx context.Context, c *app.RequestContext) {
	var req loginRequest
	if err := c.BindJSON(&req); err != nil || req.Account == "" || req.Password == "" {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	result, err := h.auth.MustAdminLogin(req.Account, req.Password)
	if err != nil {
		status, code, msg := service.HTTPStatusAndCode(err)
		WriteError(c, status, code, msg, nil)
		return
	}
	WriteSuccess(c, result)
}

func (h *AuthHandler) Me(ctx context.Context, c *app.RequestContext) {
	userID := c.GetString(contextUserID)
	user, err := h.auth.Me(userID)
	if err != nil {
		status, code, msg := service.HTTPStatusAndCode(err)
		WriteError(c, status, code, msg, nil)
		return
	}
	WriteSuccess(c, user)
}

func (h *AuthHandler) Refresh(ctx context.Context, c *app.RequestContext) {
	var req refreshRequest
	if err := c.BindJSON(&req); err != nil || req.RefreshToken == "" {
		WriteError(c, 401, 10002, "访问令牌无效或已过期", nil)
		return
	}
	result, err := h.auth.Refresh(req.RefreshToken)
	if err != nil {
		status, code, msg := service.HTTPStatusAndCode(err)
		WriteError(c, status, code, msg, nil)
		return
	}
	WriteSuccess(c, result)
}

func (h *AuthHandler) Logout(ctx context.Context, c *app.RequestContext) {
	var req logoutRequest
	_ = c.BindJSON(&req)
	token := c.GetString(contextToken)
	expiresAt := c.GetInt64(contextExp)
	h.auth.Logout(token, req.RefreshToken, expiresAt)
	WriteSuccess(c, map[string]bool{"loggedOut": true})
}

func (h *AuthHandler) AuthMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		authHeader := strings.TrimSpace(string(c.GetHeader("Authorization")))
		token := strings.TrimSpace(c.Query("token"))
		if authHeader == "" && token == "" {
			AbortError(c, 401, 10001, "缺少访问令牌", nil)
			return
		}
		const prefix = "Bearer "
		if authHeader != "" {
			if !strings.HasPrefix(authHeader, prefix) {
				AbortError(c, 401, 10002, "访问令牌无效或已过期", nil)
				return
			}
			token = strings.TrimSpace(strings.TrimPrefix(authHeader, prefix))
		}
		if token == "" {
			AbortError(c, 401, 10001, "缺少访问令牌", nil)
			return
		}
		claims, err := h.auth.ParseAccessToken(token)
		if err != nil {
			AbortError(c, 401, 10002, "访问令牌无效或已过期", nil)
			return
		}
		c.Set(contextUserID, claims.Subject)
		c.Set(contextRole, claims.Role)
		c.Set(contextToken, token)
		c.Set(contextExp, claims.ExpiresAt)
		c.Next(ctx)
	}
}

func TraceMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		traceID := strings.TrimSpace(string(c.GetHeader("X-Trace-Id")))
		if traceID == "" {
			traceID = TraceID(c)
		}
		c.Set("trace_id", traceID)
		c.Next(ctx)
	}
}

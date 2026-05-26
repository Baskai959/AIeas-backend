package mcp

import (
	"strings"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/service"

	"github.com/cloudwego/hertz/pkg/app"
)

func (h *Handler) actorFromRequest(c *app.RequestContext) (service.MCPActor, error) {
	if h.auth == nil {
		return service.MCPActor{}, domain.ErrTokenInvalid
	}
	authHeader := strings.TrimSpace(string(c.GetHeader("Authorization")))
	if authHeader == "" {
		return service.MCPActor{}, domain.ErrTokenMissing
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(authHeader, prefix) {
		return service.MCPActor{}, domain.ErrTokenInvalid
	}
	token := strings.TrimSpace(strings.TrimPrefix(authHeader, prefix))
	if token == "" {
		return service.MCPActor{}, domain.ErrTokenMissing
	}
	claims, err := h.auth.ParseAccessToken(token)
	if err != nil {
		return service.MCPActor{}, err
	}
	role := domain.Role(claims.Role)
	if claims.Subject == "" || !role.Valid() {
		return service.MCPActor{}, domain.ErrTokenInvalid
	}
	return service.MCPActor{ID: claims.Subject, Role: role}, nil
}

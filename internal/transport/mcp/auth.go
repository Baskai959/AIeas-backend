package mcp

import (
	"crypto/subtle"
	"strings"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/service"

	"github.com/cloudwego/hertz/pkg/app"
)

func (h *Handler) actorFromRequest(c *app.RequestContext) (service.MCPActor, error) {
	if strings.TrimSpace(h.apiKey) == "" {
		return service.MCPActor{}, domain.ErrTokenInvalid
	}
	provided := strings.TrimSpace(string(c.GetHeader("X-API-Key")))
	if provided == "" {
		return service.MCPActor{}, domain.ErrTokenMissing
	}
	if subtle.ConstantTimeCompare([]byte(provided), []byte(h.apiKey)) != 1 {
		return service.MCPActor{}, domain.ErrTokenInvalid
	}
	if strings.TrimSpace(h.apiActor.ID) == "" || !h.apiActor.Role.Valid() {
		return service.MCPActor{}, domain.ErrTokenInvalid
	}
	return h.apiActor, nil
}

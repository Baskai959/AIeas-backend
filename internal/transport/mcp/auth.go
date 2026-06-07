package mcp

import (
	"crypto/subtle"
	"strings"

	"aieas_backend/internal/domain"

	"github.com/cloudwego/hertz/pkg/app"
)

func (h *Handler) actorFromRequest(c *app.RequestContext) (MCPActor, error) {
	if strings.TrimSpace(h.apiKey) == "" {
		return MCPActor{}, domain.ErrTokenInvalid
	}
	provided := strings.TrimSpace(string(c.GetHeader("X-API-Key")))
	if provided == "" {
		return MCPActor{}, domain.ErrTokenMissing
	}
	if subtle.ConstantTimeCompare([]byte(provided), []byte(h.apiKey)) != 1 {
		return MCPActor{}, domain.ErrTokenInvalid
	}
	if strings.TrimSpace(h.apiActor.ID) == "" || !h.apiActor.Role.Valid() {
		return MCPActor{}, domain.ErrTokenInvalid
	}
	return h.apiActor, nil
}

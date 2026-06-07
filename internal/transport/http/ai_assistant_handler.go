package http

import (
	"context"
	"strings"

	"aieas_backend/internal/domain"

	"github.com/cloudwego/hertz/pkg/app"
)

type AIAssistantHandler struct {
	assistant AIAssistantUseCase
}

func NewAIAssistantHandler(assistant AIAssistantUseCase) *AIAssistantHandler {
	return &AIAssistantHandler{assistant: assistant}
}

type aiAssistantPermissionPatchRequest struct {
	Permission domain.MerchantAIPermission `json:"permission"`
}

type aiAssistantApprovalDecisionRequest struct {
	Approved bool `json:"approved"`
}

func (h *AIAssistantHandler) Permission(ctx context.Context, c *app.RequestContext) {
	if h.assistant == nil {
		WriteError(c, 503, 90002, "服务暂不可用", nil)
		return
	}
	permission, err := h.assistant.Permission(ctx, AIAssistantPermissionInput{
		MerchantID: strings.TrimSpace(c.Query("merchantId")),
		ActorID:    AuthUserID(c),
		ActorRole:  AuthRole(c),
	})
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"permission": permission})
}

func (h *AIAssistantHandler) UpdatePermission(ctx context.Context, c *app.RequestContext) {
	if h.assistant == nil {
		WriteError(c, 503, 90002, "服务暂不可用", nil)
		return
	}
	var req aiAssistantPermissionPatchRequest
	if err := c.BindJSON(&req); err != nil || !req.Permission.Valid() {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	permission, err := h.assistant.UpdatePermission(ctx, AIAssistantPermissionUpdateInput{
		MerchantID: strings.TrimSpace(c.Query("merchantId")),
		Permission: req.Permission,
		ActorID:    AuthUserID(c),
		ActorRole:  AuthRole(c),
	})
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, map[string]interface{}{"permission": permission})
}

func (h *AIAssistantHandler) DecideApproval(ctx context.Context, c *app.RequestContext) {
	if h.assistant == nil {
		WriteError(c, 503, 90002, "服务暂不可用", nil)
		return
	}
	requestID := strings.TrimSpace(c.Param("requestId"))
	var req aiAssistantApprovalDecisionRequest
	if err := c.BindJSON(&req); err != nil || requestID == "" {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	decision, err := h.assistant.DecideApproval(ctx, AIAssistantDecisionInput{
		RequestID: requestID,
		Approved:  req.Approved,
		ActorID:   AuthUserID(c),
		ActorRole: AuthRole(c),
	})
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, decision)
}

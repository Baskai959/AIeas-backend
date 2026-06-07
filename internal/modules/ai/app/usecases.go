package app

import (
	"context"
	"time"

	"aieas_backend/internal/domain"
	aiports "aieas_backend/internal/modules/ai/ports"
)

// AIAssistantUseCase 暴露 AI 助手 HTTP 边界。
type AIAssistantUseCase interface {
	Permission(ctx context.Context, in PermissionInput) (domain.MerchantAIPermission, error)
	UpdatePermission(ctx context.Context, in PermissionUpdateInput) (domain.MerchantAIPermission, error)
	DecideApproval(ctx context.Context, in DecisionInput) (ApprovalDecision, error)
}

// ApprovalRequester 暴露 AI 助手审批请求边界。
type ApprovalRequester = aiports.ApprovalRequester

// StatusNotifier 暴露 AI 助手状态通知边界，供 MCP transport 使用。
type StatusNotifier = aiports.StatusNotifier

// BroadcastNotifier 暴露 AI 助手播报通知边界，供 MCP control 使用。
type BroadcastNotifier = aiports.BroadcastNotifier

// ApprovalTimeoutProvider 暴露审批超时读取边界。
type ApprovalTimeoutProvider interface {
	ApprovalTimeout() time.Duration
}

package ports

import (
	"context"
	"time"

	"aieas_backend/internal/domain"
)

// UserRepository 是 AI 助手模块读取/更新商家 AI 权限所需端口。
type UserRepository interface {
	FindByID(id string) (domain.User, error)
	Update(user *domain.User) error
}

// EventNotifier 是 AI 助手事件推送端口。
type EventNotifier interface {
	NotifyAIAssistantEvent(ctx context.Context, liveSessionID uint64, event Event) (int, error)
}

type Event struct {
	EventID       string                      `json:"eventId"`
	Kind          string                      `json:"kind"`
	Status        string                      `json:"status,omitempty"`
	ToolName      string                      `json:"toolName,omitempty"`
	MerchantID    string                      `json:"merchantId,omitempty"`
	LiveSessionID uint64                      `json:"liveSessionId,omitempty"`
	RequestID     string                      `json:"requestId,omitempty"`
	Permission    domain.MerchantAIPermission `json:"permission,omitempty"`
	Enabled       *bool                       `json:"enabled,omitempty"`
	VideoSource   string                      `json:"videoSource,omitempty"`
	LiveRoom      map[string]interface{}      `json:"liveRoom,omitempty"`
	Message       string                      `json:"message"`
	BroadcastText string                      `json:"broadcastText,omitempty"`
	ExpiresAt     *time.Time                  `json:"expiresAt,omitempty"`
	CreatedAt     time.Time                   `json:"createdAt"`
}

type PermissionInput struct {
	MerchantID string
	ActorID    string
	ActorRole  domain.Role
}

type PermissionUpdateInput struct {
	MerchantID string
	Permission domain.MerchantAIPermission
	ActorID    string
	ActorRole  domain.Role
}

type ApprovalInput struct {
	MerchantID    string
	LiveSessionID uint64
	ToolName      string
	RequestID     string
	Message       string
	Timeout       time.Duration
}

type ApprovalDecision struct {
	RequestID     string    `json:"requestId"`
	Approved      bool      `json:"approved"`
	Message       string    `json:"message"`
	DecidedAt     time.Time `json:"decidedAt"`
	LiveSessionID uint64    `json:"liveSessionId,omitempty"`
}

type DecisionInput struct {
	RequestID string
	Approved  bool
	ActorID   string
	ActorRole domain.Role
}

type ApprovalRequester interface {
	RequestApproval(ctx context.Context, in ApprovalInput) (ApprovalDecision, error)
}

type StatusNotifier interface {
	NotifyStatus(ctx context.Context, liveSessionID uint64, merchantID, toolName, status, message, requestID string)
}

type BroadcastNotifier interface {
	NotifyBroadcast(ctx context.Context, liveSessionID uint64, merchantID, text, requestID string)
}

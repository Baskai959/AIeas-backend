package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

const defaultAIAssistantApprovalTimeout = 5 * time.Minute

var (
	ErrAIAssistantUserRejected    = errors.New("ai assistant action rejected by user")
	ErrAIAssistantApprovalTimeout = errors.New("ai assistant approval timeout")
)

type AIAssistantNotifier interface {
	NotifyAIAssistantEvent(ctx context.Context, liveSessionID uint64, event AIAssistantEvent) (int, error)
}

type AIAssistantEvent struct {
	EventID       string                      `json:"eventId"`
	Kind          string                      `json:"kind"`
	Status        string                      `json:"status,omitempty"`
	ToolName      string                      `json:"toolName,omitempty"`
	MerchantID    string                      `json:"merchantId,omitempty"`
	LiveSessionID uint64                      `json:"liveSessionId,omitempty"`
	RequestID     string                      `json:"requestId,omitempty"`
	Permission    domain.MerchantAIPermission `json:"permission,omitempty"`
	Enabled       *bool                       `json:"enabled,omitempty"`
	Message       string                      `json:"message"`
	BroadcastText string                      `json:"broadcastText,omitempty"`
	ExpiresAt     *time.Time                  `json:"expiresAt,omitempty"`
	CreatedAt     time.Time                   `json:"createdAt"`
}

type AIAssistantApprovalInput struct {
	MerchantID    string
	LiveSessionID uint64
	ToolName      string
	RequestID     string
	Message       string
	Timeout       time.Duration
}

type AIAssistantApprovalDecision struct {
	RequestID     string    `json:"requestId"`
	Approved      bool      `json:"approved"`
	Message       string    `json:"message"`
	DecidedAt     time.Time `json:"decidedAt"`
	LiveSessionID uint64    `json:"liveSessionId,omitempty"`
}

type AIAssistantDecisionInput struct {
	RequestID string
	Approved  bool
	ActorID   string
	ActorRole domain.Role
}

type AIAssistantPermissionInput struct {
	MerchantID string
	ActorID    string
	ActorRole  domain.Role
}

type AIAssistantPermissionUpdateInput struct {
	MerchantID string
	Permission domain.MerchantAIPermission
	ActorID    string
	ActorRole  domain.Role
}

type aiAssistantPendingApproval struct {
	requestID     string
	merchantID    string
	liveSessionID uint64
	toolName      string
	message       string
	ch            chan AIAssistantApprovalDecision
	expiresAt     time.Time
}

type AIAssistantService struct {
	users    repository.UserRepository
	notifier AIAssistantNotifier
	now      func() time.Time
	timeout  time.Duration

	mu      sync.Mutex
	pending map[string]*aiAssistantPendingApproval
}

func NewAIAssistantService(users repository.UserRepository, notifier AIAssistantNotifier) *AIAssistantService {
	return &AIAssistantService{
		users:    users,
		notifier: notifier,
		now:      time.Now,
		timeout:  defaultAIAssistantApprovalTimeout,
		pending:  make(map[string]*aiAssistantPendingApproval),
	}
}

func (s *AIAssistantService) Permission(ctx context.Context, in AIAssistantPermissionInput) (domain.MerchantAIPermission, error) {
	merchantID := strings.TrimSpace(in.MerchantID)
	if merchantID == "" && in.ActorRole == domain.RoleMerchant {
		merchantID = in.ActorID
	}
	if s == nil || s.users == nil || merchantID == "" {
		return "", domain.ErrInvalidArgument
	}
	if !canAccessSellerOwned(in.ActorID, in.ActorRole, merchantID) {
		return "", domain.ErrForbidden
	}
	user, err := s.users.FindByID(merchantID)
	if err != nil {
		return "", err
	}
	if user.Role != domain.RoleMerchant {
		return "", domain.ErrInvalidArgument
	}
	return domain.NormalizeMerchantAIPermission(user.AIPermission), nil
}

func (s *AIAssistantService) UpdatePermission(ctx context.Context, in AIAssistantPermissionUpdateInput) (domain.MerchantAIPermission, error) {
	merchantID := strings.TrimSpace(in.MerchantID)
	if merchantID == "" && in.ActorRole == domain.RoleMerchant {
		merchantID = in.ActorID
	}
	if s == nil || s.users == nil || merchantID == "" || !in.Permission.Valid() {
		return "", domain.ErrInvalidArgument
	}
	if !canAccessSellerOwned(in.ActorID, in.ActorRole, merchantID) {
		return "", domain.ErrForbidden
	}
	user, err := s.users.FindByID(merchantID)
	if err != nil {
		return "", err
	}
	if user.Role != domain.RoleMerchant {
		return "", domain.ErrInvalidArgument
	}
	user.AIPermission = in.Permission
	if err := s.users.Update(&user); err != nil {
		return "", err
	}
	return domain.NormalizeMerchantAIPermission(user.AIPermission), nil
}

func (s *AIAssistantService) RequestApproval(ctx context.Context, in AIAssistantApprovalInput) (AIAssistantApprovalDecision, error) {
	if s == nil {
		return AIAssistantApprovalDecision{Approved: true}, nil
	}
	in.MerchantID = strings.TrimSpace(in.MerchantID)
	in.ToolName = strings.TrimSpace(in.ToolName)
	in.Message = strings.TrimSpace(in.Message)
	if in.MerchantID == "" || in.LiveSessionID == 0 || in.ToolName == "" {
		return AIAssistantApprovalDecision{}, domain.ErrInvalidArgument
	}
	permission := domain.MerchantAIPermissionAsk
	if s.users != nil {
		user, err := s.users.FindByID(in.MerchantID)
		if err != nil {
			return AIAssistantApprovalDecision{}, err
		}
		permission = domain.NormalizeMerchantAIPermission(user.AIPermission)
	}
	requestID := strings.TrimSpace(in.RequestID)
	if requestID == "" {
		requestID = fmt.Sprintf("ai-approval-%d-%d", in.LiveSessionID, s.now().UTC().UnixNano())
	}
	switch permission {
	case domain.MerchantAIPermissionAllow:
		decision := AIAssistantApprovalDecision{RequestID: requestID, Approved: true, Message: "已按商家授权自动允许执行", DecidedAt: s.now().UTC(), LiveSessionID: in.LiveSessionID}
		s.notify(ctx, in.LiveSessionID, AIAssistantEvent{Kind: "permission", Status: "approved", ToolName: in.ToolName, MerchantID: in.MerchantID, LiveSessionID: in.LiveSessionID, RequestID: requestID, Permission: permission, Message: decision.Message})
		return decision, nil
	case domain.MerchantAIPermissionDeny:
		s.notify(ctx, in.LiveSessionID, AIAssistantEvent{Kind: "permission", Status: "rejected", ToolName: in.ToolName, MerchantID: in.MerchantID, LiveSessionID: in.LiveSessionID, RequestID: requestID, Permission: permission, Message: "商家 AI 权限已设置为拒绝执行"})
		return AIAssistantApprovalDecision{RequestID: requestID, Approved: false, Message: "用户拒绝执行", DecidedAt: s.now().UTC(), LiveSessionID: in.LiveSessionID}, ErrAIAssistantUserRejected
	}
	timeout := in.Timeout
	if timeout <= 0 {
		timeout = s.timeout
	}
	expiresAt := s.now().UTC().Add(timeout)
	pending := &aiAssistantPendingApproval{
		requestID:     requestID,
		merchantID:    in.MerchantID,
		liveSessionID: in.LiveSessionID,
		toolName:      in.ToolName,
		message:       in.Message,
		ch:            make(chan AIAssistantApprovalDecision, 1),
		expiresAt:     expiresAt,
	}
	s.mu.Lock()
	s.pending[requestID] = pending
	s.mu.Unlock()
	s.notify(ctx, in.LiveSessionID, AIAssistantEvent{Kind: "permission", Status: "pending", ToolName: in.ToolName, MerchantID: in.MerchantID, LiveSessionID: in.LiveSessionID, RequestID: requestID, Permission: permission, Message: in.Message, ExpiresAt: &expiresAt})

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	defer s.removePending(requestID)
	select {
	case decision := <-pending.ch:
		if !decision.Approved {
			return decision, ErrAIAssistantUserRejected
		}
		return decision, nil
	case <-timer.C:
		s.notify(ctx, in.LiveSessionID, AIAssistantEvent{Kind: "permission", Status: "timeout", ToolName: in.ToolName, MerchantID: in.MerchantID, LiveSessionID: in.LiveSessionID, RequestID: requestID, Permission: permission, Message: "商家未确认，本次 AI 控制已取消"})
		return AIAssistantApprovalDecision{RequestID: requestID, Approved: false, Message: "用户未确认执行", DecidedAt: s.now().UTC(), LiveSessionID: in.LiveSessionID}, ErrAIAssistantApprovalTimeout
	case <-ctx.Done():
		return AIAssistantApprovalDecision{}, ctx.Err()
	}
}

func (s *AIAssistantService) DecideApproval(ctx context.Context, in AIAssistantDecisionInput) (AIAssistantApprovalDecision, error) {
	_ = ctx
	requestID := strings.TrimSpace(in.RequestID)
	if s == nil || requestID == "" {
		return AIAssistantApprovalDecision{}, domain.ErrInvalidArgument
	}
	s.mu.Lock()
	pending := s.pending[requestID]
	if pending == nil {
		s.mu.Unlock()
		return AIAssistantApprovalDecision{}, ErrAIAssistantApprovalTimeout
	}
	if !canAccessSellerOwned(in.ActorID, in.ActorRole, pending.merchantID) {
		s.mu.Unlock()
		return AIAssistantApprovalDecision{}, domain.ErrForbidden
	}
	delete(s.pending, requestID)
	s.mu.Unlock()
	status := "approved"
	message := "商家已允许执行"
	if !in.Approved {
		status = "rejected"
		message = "用户拒绝执行"
	}
	decision := AIAssistantApprovalDecision{RequestID: requestID, Approved: in.Approved, Message: message, DecidedAt: s.now().UTC(), LiveSessionID: pending.liveSessionID}
	s.notify(context.Background(), pending.liveSessionID, AIAssistantEvent{Kind: "permission", Status: status, ToolName: pending.toolName, MerchantID: pending.merchantID, LiveSessionID: pending.liveSessionID, RequestID: requestID, Message: message})
	select {
	case pending.ch <- decision:
	default:
	}
	return decision, nil
}

func (s *AIAssistantService) NotifyStatus(ctx context.Context, liveSessionID uint64, merchantID, toolName, status, message, requestID string) {
	if s == nil || liveSessionID == 0 {
		return
	}
	s.notify(ctx, liveSessionID, AIAssistantEvent{
		Kind:          "status",
		Status:        strings.TrimSpace(status),
		ToolName:      strings.TrimSpace(toolName),
		MerchantID:    strings.TrimSpace(merchantID),
		LiveSessionID: liveSessionID,
		RequestID:     strings.TrimSpace(requestID),
		Message:       strings.TrimSpace(message),
	})
}

func (s *AIAssistantService) NotifyBroadcast(ctx context.Context, liveSessionID uint64, merchantID, text, requestID string) {
	if s == nil || liveSessionID == 0 {
		return
	}
	text = strings.TrimSpace(text)
	s.notify(ctx, liveSessionID, AIAssistantEvent{
		Kind:          "broadcast",
		Status:        "running",
		ToolName:      "live_voice_broadcast",
		MerchantID:    strings.TrimSpace(merchantID),
		LiveSessionID: liveSessionID,
		RequestID:     strings.TrimSpace(requestID),
		Message:       "AI 正在生成直播播报",
		BroadcastText: text,
	})
}

func (s *AIAssistantService) NotifySwitch(ctx context.Context, liveSessionID uint64, merchantID string, enabled bool) {
	if s == nil || liveSessionID == 0 {
		return
	}
	status := "disabled"
	message := fmt.Sprintf("直播场次%dAI直播助手已关闭", liveSessionID)
	if enabled {
		status = "enabled"
		message = fmt.Sprintf("直播场次%dAI直播助手已开启", liveSessionID)
	}
	s.notify(ctx, liveSessionID, AIAssistantEvent{
		Kind:          "switch",
		Status:        status,
		ToolName:      "ai_live_assistant",
		MerchantID:    strings.TrimSpace(merchantID),
		LiveSessionID: liveSessionID,
		Enabled:       &enabled,
		Message:       message,
	})
}

func (s *AIAssistantService) removePending(requestID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pending, requestID)
}

func (s *AIAssistantService) notify(ctx context.Context, liveSessionID uint64, event AIAssistantEvent) {
	if s == nil || s.notifier == nil || liveSessionID == 0 {
		return
	}
	now := s.now().UTC()
	if event.CreatedAt.IsZero() {
		event.CreatedAt = now
	}
	if strings.TrimSpace(event.EventID) == "" {
		event.EventID = fmt.Sprintf("ai-%d-%d", liveSessionID, now.UnixNano())
	}
	if event.LiveSessionID == 0 {
		event.LiveSessionID = liveSessionID
	}
	_, _ = s.notifier.NotifyAIAssistantEvent(ctx, liveSessionID, event)
}

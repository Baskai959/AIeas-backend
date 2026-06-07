package app

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"aieas_backend/internal/domain"
	aiports "aieas_backend/internal/modules/ai/ports"
)

const defaultApprovalTimeout = 30 * time.Second

var (
	ErrUserRejected    = errors.New("ai assistant action rejected by user")
	ErrApprovalTimeout = errors.New("ai assistant approval timeout")
)

type EventNotifier = aiports.EventNotifier

type UserRepository interface {
	FindByID(id string) (domain.User, error)
	Update(user *domain.User) error
}

type Event = aiports.Event
type PermissionInput = aiports.PermissionInput
type PermissionUpdateInput = aiports.PermissionUpdateInput
type ApprovalInput = aiports.ApprovalInput
type ApprovalDecision = aiports.ApprovalDecision
type DecisionInput = aiports.DecisionInput

type pendingApproval struct {
	requestID     string
	merchantID    string
	liveSessionID uint64
	toolName      string
	message       string
	ch            chan ApprovalDecision
	expiresAt     time.Time
}

type AIAssistantService struct {
	users    UserRepository
	notifier EventNotifier
	now      func() time.Time
	timeout  time.Duration

	mu      sync.Mutex
	pending map[string]*pendingApproval
}

func NewAIAssistantService(users UserRepository, notifier EventNotifier) *AIAssistantService {
	return &AIAssistantService{
		users:    users,
		notifier: notifier,
		now:      time.Now,
		timeout:  defaultApprovalTimeout,
		pending:  make(map[string]*pendingApproval),
	}
}

func (s *AIAssistantService) ApprovalTimeout() time.Duration {
	if s == nil || s.timeout <= 0 {
		return defaultApprovalTimeout
	}
	return s.timeout
}

func (s *AIAssistantService) Permission(ctx context.Context, in PermissionInput) (domain.MerchantAIPermission, error) {
	merchantID := strings.TrimSpace(in.MerchantID)
	if merchantID == "" && in.ActorRole == domain.RoleMerchant {
		merchantID = in.ActorID
	}
	if s == nil || s.users == nil || merchantID == "" {
		return "", domain.ErrInvalidArgument
	}
	if !canAccessMerchant(in.ActorID, in.ActorRole, merchantID) {
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

func (s *AIAssistantService) UpdatePermission(ctx context.Context, in PermissionUpdateInput) (domain.MerchantAIPermission, error) {
	merchantID := strings.TrimSpace(in.MerchantID)
	if merchantID == "" && in.ActorRole == domain.RoleMerchant {
		merchantID = in.ActorID
	}
	if s == nil || s.users == nil || merchantID == "" || !in.Permission.Valid() {
		return "", domain.ErrInvalidArgument
	}
	if !canAccessMerchant(in.ActorID, in.ActorRole, merchantID) {
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

func (s *AIAssistantService) RequestApproval(ctx context.Context, in ApprovalInput) (ApprovalDecision, error) {
	if s == nil {
		return ApprovalDecision{Approved: true}, nil
	}
	in.MerchantID = strings.TrimSpace(in.MerchantID)
	in.ToolName = strings.TrimSpace(in.ToolName)
	in.Message = strings.TrimSpace(in.Message)
	if in.MerchantID == "" || in.LiveSessionID == 0 || in.ToolName == "" {
		return ApprovalDecision{}, domain.ErrInvalidArgument
	}
	permission := domain.MerchantAIPermissionAsk
	if s.users != nil {
		user, err := s.users.FindByID(in.MerchantID)
		if err != nil {
			return ApprovalDecision{}, err
		}
		permission = domain.NormalizeMerchantAIPermission(user.AIPermission)
	}
	requestID := strings.TrimSpace(in.RequestID)
	if requestID == "" {
		requestID = fmt.Sprintf("ai-approval-%d-%d", in.LiveSessionID, s.now().UTC().UnixNano())
	}
	switch permission {
	case domain.MerchantAIPermissionAllow:
		decision := ApprovalDecision{RequestID: requestID, Approved: true, Message: "已按商家授权自动允许执行", DecidedAt: s.now().UTC(), LiveSessionID: in.LiveSessionID}
		s.notify(ctx, in.LiveSessionID, Event{Kind: "permission", Status: "approved", ToolName: in.ToolName, MerchantID: in.MerchantID, LiveSessionID: in.LiveSessionID, RequestID: requestID, Permission: permission, Message: decision.Message})
		return decision, nil
	case domain.MerchantAIPermissionDeny:
		s.notify(ctx, in.LiveSessionID, Event{Kind: "permission", Status: "rejected", ToolName: in.ToolName, MerchantID: in.MerchantID, LiveSessionID: in.LiveSessionID, RequestID: requestID, Permission: permission, Message: "商家 AI 权限已设置为拒绝执行"})
		return ApprovalDecision{RequestID: requestID, Approved: false, Message: "用户拒绝执行", DecidedAt: s.now().UTC(), LiveSessionID: in.LiveSessionID}, ErrUserRejected
	}
	timeout := in.Timeout
	if timeout <= 0 {
		timeout = s.ApprovalTimeout()
	}
	expiresAt := s.now().UTC().Add(timeout)
	pending := &pendingApproval{
		requestID:     requestID,
		merchantID:    in.MerchantID,
		liveSessionID: in.LiveSessionID,
		toolName:      in.ToolName,
		message:       in.Message,
		ch:            make(chan ApprovalDecision, 1),
		expiresAt:     expiresAt,
	}
	s.mu.Lock()
	s.pending[requestID] = pending
	s.mu.Unlock()
	s.notify(ctx, in.LiveSessionID, Event{Kind: "permission", Status: "pending", ToolName: in.ToolName, MerchantID: in.MerchantID, LiveSessionID: in.LiveSessionID, RequestID: requestID, Permission: permission, Message: in.Message, ExpiresAt: &expiresAt})

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	defer s.removePending(requestID)
	select {
	case decision := <-pending.ch:
		if !decision.Approved {
			return decision, ErrUserRejected
		}
		return decision, nil
	case <-timer.C:
		message := "30 秒内未操作，已默认允许执行"
		decision := ApprovalDecision{RequestID: requestID, Approved: true, Message: message, DecidedAt: s.now().UTC(), LiveSessionID: in.LiveSessionID}
		s.notify(ctx, in.LiveSessionID, Event{Kind: "permission", Status: "approved", ToolName: in.ToolName, MerchantID: in.MerchantID, LiveSessionID: in.LiveSessionID, RequestID: requestID, Permission: permission, Message: message})
		return decision, nil
	case <-ctx.Done():
		return ApprovalDecision{}, ctx.Err()
	}
}

func (s *AIAssistantService) DecideApproval(ctx context.Context, in DecisionInput) (ApprovalDecision, error) {
	_ = ctx
	requestID := strings.TrimSpace(in.RequestID)
	if s == nil || requestID == "" {
		return ApprovalDecision{}, domain.ErrInvalidArgument
	}
	s.mu.Lock()
	pending := s.pending[requestID]
	if pending == nil {
		s.mu.Unlock()
		return ApprovalDecision{}, ErrApprovalTimeout
	}
	if !canAccessMerchant(in.ActorID, in.ActorRole, pending.merchantID) {
		s.mu.Unlock()
		return ApprovalDecision{}, domain.ErrForbidden
	}
	delete(s.pending, requestID)
	s.mu.Unlock()
	status := "approved"
	message := "商家已允许执行"
	if !in.Approved {
		status = "rejected"
		message = "用户拒绝执行"
	}
	decision := ApprovalDecision{RequestID: requestID, Approved: in.Approved, Message: message, DecidedAt: s.now().UTC(), LiveSessionID: pending.liveSessionID}
	s.notify(context.Background(), pending.liveSessionID, Event{Kind: "permission", Status: status, ToolName: pending.toolName, MerchantID: pending.merchantID, LiveSessionID: pending.liveSessionID, RequestID: requestID, Message: message})
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
	s.notify(ctx, liveSessionID, Event{
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
	s.notify(ctx, liveSessionID, Event{
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
	videoSource := "recorded"
	if enabled {
		videoSource = "digitalHuman"
	}
	s.notify(ctx, liveSessionID, Event{
		Kind:          "switch",
		Status:        status,
		ToolName:      "ai_live_assistant",
		MerchantID:    strings.TrimSpace(merchantID),
		LiveSessionID: liveSessionID,
		Enabled:       &enabled,
		VideoSource:   videoSource,
		LiveRoom: map[string]interface{}{
			"id":                 liveSessionID,
			"liveSessionId":      liveSessionID,
			"merchantId":         strings.TrimSpace(merchantID),
			"videoSource":        videoSource,
			"aiAssistantEnabled": enabled,
		},
		Message: message,
	})
}

func (s *AIAssistantService) removePending(requestID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pending, requestID)
}

func (s *AIAssistantService) notify(ctx context.Context, liveSessionID uint64, event Event) {
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

func canAccessMerchant(actorID string, actorRole domain.Role, merchantID string) bool {
	merchantID = strings.TrimSpace(merchantID)
	if merchantID == "" {
		return false
	}
	if actorRole == domain.RoleAdmin {
		return true
	}
	return actorRole == domain.RoleMerchant && strings.TrimSpace(actorID) == merchantID
}

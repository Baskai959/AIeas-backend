package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"aieas_backend/internal/domain"
	liveanalysisports "aieas_backend/internal/modules/live_analysis/ports"
)

type LiveAnalysisService struct {
	reports   liveanalysisports.LiveAnalysisReportRepository
	sessions  liveanalysisports.LiveSessionRepository
	requester liveanalysisports.AsyncRequester
	options   LiveAnalysisOptions
}

func NewLiveAnalysisService(reports liveanalysisports.LiveAnalysisReportRepository, sessions liveanalysisports.LiveSessionRepository, requester liveanalysisports.AsyncRequester, options LiveAnalysisOptions) *LiveAnalysisService {
	if options.MaxAttempts <= 0 {
		options.MaxAttempts = 3
	}
	return &LiveAnalysisService{
		reports:   reports,
		sessions:  sessions,
		requester: requester,
		options:   options,
	}
}

func (s *LiveAnalysisService) CreateReport(ctx context.Context, in CreateReportInput) (LiveAnalysisTask, error) {
	session, err := s.authorizedSession(ctx, in.LiveSessionID, in.ActorID, in.ActorRole)
	if err != nil {
		return LiveAnalysisTask{}, err
	}
	return s.ensureReportForSession(ctx, session)
}

func (s *LiveAnalysisService) GetReport(ctx context.Context, liveSessionID uint64, actorID string, actorRole domain.Role) (LiveAnalysisTask, error) {
	session, err := s.authorizedSession(ctx, liveSessionID, actorID, actorRole)
	if err != nil {
		return LiveAnalysisTask{}, err
	}
	return s.ensureReportForSession(ctx, session)
}

func (s *LiveAnalysisService) StartReportForSession(ctx context.Context, session domain.LiveSession) (LiveAnalysisTask, error) {
	if s == nil || s.reports == nil {
		return LiveAnalysisTask{}, domain.ErrInvalidState
	}
	return s.ensureReportForSession(ctx, session)
}

func (s *LiveAnalysisService) HandleCallback(ctx context.Context, in CallbackInput) (LiveAnalysisTask, error) {
	if s == nil || s.reports == nil {
		return LiveAnalysisTask{}, domain.ErrNotFound
	}
	task, err := s.findCallbackTask(ctx, in)
	if err != nil {
		return LiveAnalysisTask{}, err
	}
	if liveAnalysisCallbackStale(task, in) {
		return task, nil
	}
	status := strings.ToUpper(strings.TrimSpace(in.Status))
	summary := strings.TrimSpace(in.Summary)
	if in.Success && status == "COMPLETED" && summary != "" {
		task.Status = LiveAnalysisTaskSucceeded
		task.Report = summary
		task.ErrorMessage = ""
	} else {
		task.Status = LiveAnalysisTaskFailed
		task.Report = ""
		task.ErrorMessage = liveAnalysisCallbackErrorMessage(in)
	}
	task.UpdatedAt = time.Now().UTC()
	if err := s.reports.Update(ctx, &task); err != nil {
		return LiveAnalysisTask{}, err
	}
	return task, nil
}

func (s *LiveAnalysisService) authorizedSession(ctx context.Context, liveSessionID uint64, actorID string, actorRole domain.Role) (domain.LiveSession, error) {
	if s == nil || s.reports == nil || s.sessions == nil {
		return domain.LiveSession{}, domain.ErrNotFound
	}
	if liveSessionID == 0 {
		return domain.LiveSession{}, domain.ErrInvalidArgument
	}
	session, err := s.sessions.Get(ctx, liveSessionID)
	if err != nil {
		return domain.LiveSession{}, err
	}
	if !canAccessSellerOwned(strings.TrimSpace(actorID), actorRole, session.MerchantID) {
		return domain.LiveSession{}, domain.ErrForbidden
	}
	return session, nil
}

func (s *LiveAnalysisService) ensureReportForSession(ctx context.Context, session domain.LiveSession) (LiveAnalysisTask, error) {
	if s == nil || s.reports == nil {
		return LiveAnalysisTask{}, domain.ErrInvalidState
	}
	if session.ID == 0 || strings.TrimSpace(session.MerchantID) == "" {
		return LiveAnalysisTask{}, domain.ErrInvalidArgument
	}
	task, err := s.reports.FindByLiveSessionID(ctx, session.ID)
	if err == nil {
		return s.ensureExistingReport(ctx, task)
	}
	if !errors.Is(err, domain.ErrNotFound) {
		return LiveAnalysisTask{}, err
	}
	if session.Status != domain.LiveSessionStatusEnded {
		return LiveAnalysisTask{}, domain.ErrInvalidState
	}
	now := time.Now().UTC()
	task = LiveAnalysisTask{
		TaskID:        randomToken("lar"),
		LiveSessionID: session.ID,
		MerchantID:    strings.TrimSpace(session.MerchantID),
		Status:        LiveAnalysisTaskPending,
		Prompt:        buildLiveAnalysisPrompt(session),
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	if err := s.reports.Create(ctx, &task); err != nil {
		if errors.Is(err, domain.ErrConflict) {
			existing, findErr := s.reports.FindByLiveSessionID(ctx, session.ID)
			if findErr == nil {
				return s.ensureExistingReport(ctx, existing)
			}
		}
		return LiveAnalysisTask{}, err
	}
	return s.startAttempt(ctx, &task)
}

func (s *LiveAnalysisService) ensureExistingReport(ctx context.Context, task LiveAnalysisTask) (LiveAnalysisTask, error) {
	switch task.Status {
	case LiveAnalysisTaskSucceeded, LiveAnalysisTaskRunning:
		return task, nil
	case LiveAnalysisTaskPending:
		if task.AttemptCount > 0 {
			task.Status = LiveAnalysisTaskRunning
			return task, nil
		}
		return s.startAttempt(ctx, &task)
	case LiveAnalysisTaskFailed:
		if task.AttemptCount >= s.maxAttempts() {
			return task, nil
		}
		return s.startAttempt(ctx, &task)
	default:
		return LiveAnalysisTask{}, domain.ErrInvalidState
	}
}

func (s *LiveAnalysisService) startAttempt(ctx context.Context, task *LiveAnalysisTask) (LiveAnalysisTask, error) {
	if task == nil {
		return LiveAnalysisTask{}, domain.ErrInvalidArgument
	}
	task.AttemptCount++
	task.Status = LiveAnalysisTaskPending
	task.AgentRequestID = ""
	task.Report = ""
	task.ErrorMessage = ""
	task.UpdatedAt = time.Now().UTC()
	if err := s.reports.Update(ctx, task); err != nil {
		return LiveAnalysisTask{}, err
	}
	if s.requester == nil {
		task.Status = LiveAnalysisTaskFailed
		task.ErrorMessage = "live analysis generator unavailable"
		_ = s.reports.Update(ctx, task)
		return *task, nil
	}
	result, err := s.requester.RequestLiveAnalysis(ctx, liveanalysisports.AsyncRequestInput{
		Prompt:      task.Prompt,
		CallbackURL: strings.TrimSpace(s.options.CallbackURL),
		CallbackContext: map[string]interface{}{
			"taskId":        task.TaskID,
			"liveSessionId": task.LiveSessionID,
			"merchantId":    task.MerchantID,
			"attempt":       task.AttemptCount,
		},
		ToolArguments: map[string]interface{}{
			"sessionId": task.LiveSessionID,
		},
	})
	if err != nil {
		task.Status = LiveAnalysisTaskFailed
		task.ErrorMessage = strings.TrimSpace(err.Error())
		if task.ErrorMessage == "" {
			task.ErrorMessage = "live analysis async request failed"
		}
		_ = s.reports.Update(ctx, task)
		return *task, nil
	}
	task.AgentRequestID = strings.TrimSpace(result.RequestID)
	task.Status = LiveAnalysisTaskRunning
	task.ErrorMessage = ""
	if err := s.reports.Update(ctx, task); err != nil {
		return LiveAnalysisTask{}, err
	}
	return *task, nil
}

func (s *LiveAnalysisService) maxAttempts() int {
	if s == nil || s.options.MaxAttempts <= 0 {
		return 3
	}
	return s.options.MaxAttempts
}

func (s *LiveAnalysisService) findCallbackTask(ctx context.Context, in CallbackInput) (LiveAnalysisTask, error) {
	if taskID := liveAnalysisCallbackTaskID(in); taskID != "" {
		return s.reports.FindByTaskID(ctx, taskID)
	}
	if liveSessionID := uint64FromCallbackContext(in.CallbackContext, "liveSessionId", "live_session_id"); liveSessionID != 0 {
		return s.reports.FindByLiveSessionID(ctx, liveSessionID)
	}
	if requestID := strings.TrimSpace(in.RequestID); requestID != "" {
		task, err := s.reports.FindByAgentRequestID(ctx, requestID)
		if err == nil || !errors.Is(err, domain.ErrNotFound) {
			return task, err
		}
		return s.reports.FindByTaskID(ctx, requestID)
	}
	return LiveAnalysisTask{}, domain.ErrInvalidArgument
}

func liveAnalysisCallbackStale(task LiveAnalysisTask, in CallbackInput) bool {
	requestID := strings.TrimSpace(in.RequestID)
	if task.AgentRequestID != "" && requestID != "" && requestID != task.AgentRequestID {
		return true
	}
	attempt := intFromCallbackContext(in.CallbackContext, "attempt")
	return attempt > 0 && task.AttemptCount > 0 && attempt != task.AttemptCount
}

func liveAnalysisCallbackTaskID(in CallbackInput) string {
	for _, key := range []string{"taskId", "task_id"} {
		if taskID := stringFromCallbackContext(in.CallbackContext, key); taskID != "" {
			return taskID
		}
	}
	return ""
}

func stringFromCallbackContext(ctx map[string]interface{}, key string) string {
	if ctx == nil {
		return ""
	}
	raw, ok := ctx[key]
	if !ok || raw == nil {
		return ""
	}
	switch v := raw.(type) {
	case string:
		return strings.TrimSpace(v)
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

func uint64FromCallbackContext(ctx map[string]interface{}, keys ...string) uint64 {
	for _, key := range keys {
		raw, ok := ctx[key]
		if !ok || raw == nil {
			continue
		}
		switch v := raw.(type) {
		case uint64:
			return v
		case uint:
			return uint64(v)
		case int:
			if v > 0 {
				return uint64(v)
			}
		case int64:
			if v > 0 {
				return uint64(v)
			}
		case float64:
			if v > 0 {
				return uint64(v)
			}
		case json.Number:
			if n, err := strconv.ParseUint(v.String(), 10, 64); err == nil {
				return n
			}
		case string:
			if n, err := strconv.ParseUint(strings.TrimSpace(v), 10, 64); err == nil {
				return n
			}
		default:
			if n, err := strconv.ParseUint(strings.TrimSpace(fmt.Sprint(v)), 10, 64); err == nil {
				return n
			}
		}
	}
	return 0
}

func intFromCallbackContext(ctx map[string]interface{}, key string) int {
	raw, ok := ctx[key]
	if !ok || raw == nil {
		return 0
	}
	switch v := raw.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case json.Number:
		n, _ := strconv.Atoi(v.String())
		return n
	case string:
		n, _ := strconv.Atoi(strings.TrimSpace(v))
		return n
	default:
		n, _ := strconv.Atoi(strings.TrimSpace(fmt.Sprint(v)))
		return n
	}
}

func liveAnalysisCallbackErrorMessage(in CallbackInput) string {
	if in.ErrorMessage != nil && strings.TrimSpace(*in.ErrorMessage) != "" {
		return strings.TrimSpace(*in.ErrorMessage)
	}
	if strings.TrimSpace(in.Summary) != "" {
		return strings.TrimSpace(in.Summary)
	}
	if strings.TrimSpace(in.Status) != "" {
		return "live analysis callback status " + strings.TrimSpace(in.Status)
	}
	return "live analysis callback failed"
}

func buildLiveAnalysisPrompt(session domain.LiveSession) string {
	return fmt.Sprintf("帮我总结商家id为%s直播场次id为%d的直播情况，重点看成交、出价、订单和风险问题。", strings.TrimSpace(session.MerchantID), session.ID)
}

func canAccessSellerOwned(actorID string, actorRole domain.Role, sellerID string) bool {
	sellerID = strings.TrimSpace(sellerID)
	if sellerID == "" {
		return false
	}
	if actorRole == domain.RoleAdmin {
		return true
	}
	return actorRole == domain.RoleMerchant && strings.TrimSpace(actorID) == sellerID
}

func randomToken(prefix string) string {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(b)
}

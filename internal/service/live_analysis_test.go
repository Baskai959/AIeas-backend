package service

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

func TestLiveAnalysisServiceGetReportForEndedSession(t *testing.T) {
	requester := &stubLiveAnalysisRequester{result: LiveAnalysisAsyncResult{RequestID: "agent-1", Status: "ACCEPTED"}}
	reports := repository.NewMemoryLiveAnalysisReportRepository()
	sessions := repository.NewMemoryLiveSessionRepository()
	session := createEndedLiveAnalysisSession(t, sessions, "u_2001")
	svc := NewLiveAnalysisService(reports, sessions, requester, LiveAnalysisOptions{CallbackURL: "http://backend/api/v1/live-analysis/callback", CallbackAPIKey: "callback-key"})

	task, err := svc.GetReport(context.Background(), session.ID, "u_2001", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("get report: %v", err)
	}
	if task.TaskID == "" || task.LiveSessionID != session.ID || task.MerchantID != "u_2001" {
		t.Fatalf("unexpected task: %+v", task)
	}
	if task.Status != LiveAnalysisTaskRunning || task.AttemptCount != 1 || task.Report != "" {
		t.Fatalf("unexpected accepted task: %+v", task)
	}
	if !strings.Contains(requester.inputs[0].Prompt, "直播场次id为"+strconvUint(session.ID)) ||
		requester.inputs[0].CallbackURL != "http://backend/api/v1/live-analysis/callback" ||
		requester.inputs[0].CallbackHeaders["X-Callback-Key"] != "callback-key" ||
		requester.inputs[0].CallbackContext["liveSessionId"] != session.ID ||
		requester.inputs[0].CallbackContext["attempt"] != 1 {
		t.Fatalf("unexpected requester input: %+v", requester.inputs[0])
	}

	got, err := svc.HandleCallback(context.Background(), LiveAnalysisCallbackInput{
		RequestID: "agent-1",
		Success:   true,
		Status:    "COMPLETED",
		Summary:   "直播总结",
		CallbackContext: map[string]interface{}{
			"taskId":        task.TaskID,
			"liveSessionId": session.ID,
			"attempt":       1,
		},
	})
	if err != nil {
		t.Fatalf("handle callback: %v", err)
	}
	if got.Status != LiveAnalysisTaskSucceeded || got.Report != "直播总结" || got.AttemptCount != 1 {
		t.Fatalf("unexpected callback task: %+v", got)
	}
}

func TestLiveAnalysisServiceAuthAndSessionState(t *testing.T) {
	reports := repository.NewMemoryLiveAnalysisReportRepository()
	sessions := repository.NewMemoryLiveSessionRepository()
	ended := createEndedLiveAnalysisSession(t, sessions, "u_2001")
	live := domain.LiveSession{LiveRoomID: 2, MerchantID: "u_2001", Status: domain.LiveSessionStatusLive, OpenedAt: time.Now().UTC()}
	if err := sessions.Create(context.Background(), &live); err != nil {
		t.Fatalf("create live session: %v", err)
	}
	svc := NewLiveAnalysisService(reports, sessions, &stubLiveAnalysisRequester{result: LiveAnalysisAsyncResult{RequestID: "agent-1", Status: "ACCEPTED"}}, LiveAnalysisOptions{CallbackURL: "http://backend/api/v1/live-analysis/callback", CallbackAPIKey: "callback-key"})

	if _, err := svc.GetReport(context.Background(), ended.ID, "u_1001", domain.RoleBuyer); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("expected buyer forbidden, got %v", err)
	}
	if _, err := svc.GetReport(context.Background(), live.ID, "u_2001", domain.RoleMerchant); !errors.Is(err, domain.ErrInvalidState) {
		t.Fatalf("expected live session invalid state, got %v", err)
	}
	if _, err := svc.GetReport(context.Background(), 0, "u_2001", domain.RoleMerchant); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected missing session invalid argument, got %v", err)
	}
}

func TestLiveAnalysisServiceRetriesFailedReportUntilMaxAttempts(t *testing.T) {
	requester := &stubLiveAnalysisRequester{err: errors.New("agent unavailable")}
	reports := repository.NewMemoryLiveAnalysisReportRepository()
	sessions := repository.NewMemoryLiveSessionRepository()
	session := createEndedLiveAnalysisSession(t, sessions, "u_2001")
	svc := NewLiveAnalysisService(reports, sessions, requester, LiveAnalysisOptions{CallbackURL: "http://backend/api/v1/live-analysis/callback", CallbackAPIKey: "callback-key"})

	task, err := svc.GetReport(context.Background(), session.ID, "u_2001", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("first get report: %v", err)
	}
	if task.Status != LiveAnalysisTaskFailed || task.AttemptCount != 1 || task.ErrorMessage == "" {
		t.Fatalf("expected first failed attempt, got %+v", task)
	}

	requester.err = nil
	requester.result = LiveAnalysisAsyncResult{RequestID: "agent-2", Status: "ACCEPTED"}
	task, err = svc.GetReport(context.Background(), session.ID, "u_2001", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("retry get report: %v", err)
	}
	if task.Status != LiveAnalysisTaskRunning || task.AttemptCount != 2 {
		t.Fatalf("expected retry running attempt 2, got %+v", task)
	}

	message := "模型服务超时"
	task, err = svc.HandleCallback(context.Background(), LiveAnalysisCallbackInput{
		RequestID:    "agent-2",
		Success:      false,
		Status:       "FAILED",
		Summary:      "失败摘要",
		ErrorMessage: &message,
		CallbackContext: map[string]interface{}{
			"taskId":        task.TaskID,
			"liveSessionId": session.ID,
			"attempt":       2,
		},
	})
	if err != nil {
		t.Fatalf("failed callback: %v", err)
	}
	if task.Status != LiveAnalysisTaskFailed || task.AttemptCount != 2 || task.ErrorMessage != "模型服务超时" {
		t.Fatalf("unexpected failed callback task: %+v", task)
	}

	requester.result = LiveAnalysisAsyncResult{RequestID: "agent-3", Status: "ACCEPTED"}
	task, err = svc.GetReport(context.Background(), session.ID, "u_2001", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("third attempt get report: %v", err)
	}
	if task.Status != LiveAnalysisTaskRunning || task.AttemptCount != 3 {
		t.Fatalf("expected third running attempt, got %+v", task)
	}
	task, err = svc.HandleCallback(context.Background(), LiveAnalysisCallbackInput{
		RequestID: "agent-3",
		Success:   false,
		Status:    "FAILED",
		Summary:   "仍然失败",
		CallbackContext: map[string]interface{}{
			"taskId":        task.TaskID,
			"liveSessionId": session.ID,
			"attempt":       3,
		},
	})
	if err != nil {
		t.Fatalf("third failed callback: %v", err)
	}
	if task.Status != LiveAnalysisTaskFailed || task.AttemptCount != 3 {
		t.Fatalf("expected final failed task, got %+v", task)
	}

	final, err := svc.GetReport(context.Background(), session.ID, "u_2001", domain.RoleMerchant)
	if err != nil {
		t.Fatalf("final get report: %v", err)
	}
	if final.Status != LiveAnalysisTaskFailed || final.AttemptCount != 3 || requester.calls != 3 {
		t.Fatalf("expected no retry after max attempts, final=%+v calls=%d", final, requester.calls)
	}
}

func createEndedLiveAnalysisSession(t *testing.T, sessions *repository.MemoryLiveSessionRepository, merchantID string) domain.LiveSession {
	t.Helper()
	now := time.Now().UTC()
	session := domain.LiveSession{
		LiveRoomID: 1,
		MerchantID: merchantID,
		Status:     domain.LiveSessionStatusEnded,
		OpenedAt:   now.Add(-time.Hour),
		ClosedAt:   &now,
	}
	if err := sessions.Create(context.Background(), &session); err != nil {
		t.Fatalf("create session: %v", err)
	}
	return session
}

func strconvUint(v uint64) string {
	return strconv.FormatUint(v, 10)
}

type stubLiveAnalysisRequester struct {
	inputs []LiveAnalysisAsyncInput
	result LiveAnalysisAsyncResult
	err    error
	calls  int
}

func (r *stubLiveAnalysisRequester) RequestLiveAnalysis(ctx context.Context, in LiveAnalysisAsyncInput) (LiveAnalysisAsyncResult, error) {
	_ = ctx
	r.inputs = append(r.inputs, in)
	r.calls++
	if r.err != nil {
		return LiveAnalysisAsyncResult{}, r.err
	}
	return r.result, nil
}

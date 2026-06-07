package http

import (
	"context"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
)

type LiveAnalysisHandler struct {
	reports        LiveAnalysisUseCase
	callbackAPIKey string
}

func NewLiveAnalysisHandler(reports LiveAnalysisUseCase, callbackAPIKey string) *LiveAnalysisHandler {
	return &LiveAnalysisHandler{reports: reports, callbackAPIKey: strings.TrimSpace(callbackAPIKey)}
}

type createLiveAnalysisReportRequest struct {
	LiveSessionID uint64 `json:"liveSessionId"`
}

type liveAnalysisCallbackRequest struct {
	RequestID       string                 `json:"request_id"`
	Success         bool                   `json:"success"`
	Status          string                 `json:"status"`
	Summary         string                 `json:"summary"`
	ErrorMessage    *string                `json:"error_message"`
	CallbackContext map[string]interface{} `json:"callback_context"`
	CompletedAt     string                 `json:"completed_at"`
}

func (h *LiveAnalysisHandler) CreateReport(ctx context.Context, c *app.RequestContext) {
	var req createLiveAnalysisReportRequest
	if strings.TrimSpace(string(c.Request.Body())) != "" {
		if err := c.BindJSON(&req); err != nil {
			WriteError(c, 400, 20001, "参数不合法", nil)
			return
		}
	}
	task, err := h.reports.CreateReport(ctx, LiveAnalysisCreateReportInput{
		ActorID:       AuthUserID(c),
		ActorRole:     AuthRole(c),
		LiveSessionID: req.LiveSessionID,
	})
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, task)
}

func (h *LiveAnalysisHandler) GetReport(ctx context.Context, c *app.RequestContext) {
	liveSessionID, ok := parseUintParam(c, "liveSessionId")
	if !ok {
		return
	}
	task, err := h.reports.GetReport(ctx, liveSessionID, AuthUserID(c), AuthRole(c))
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, task)
}

func (h *LiveAnalysisHandler) Callback(ctx context.Context, c *app.RequestContext) {
	var req liveAnalysisCallbackRequest
	if err := c.BindJSON(&req); err != nil {
		WriteError(c, 400, 20001, "参数不合法", nil)
		return
	}
	task, err := h.reports.HandleCallback(ctx, LiveAnalysisCallbackInput{
		RequestID:       req.RequestID,
		Success:         req.Success,
		Status:          req.Status,
		Summary:         req.Summary,
		ErrorMessage:    req.ErrorMessage,
		CallbackContext: req.CallbackContext,
	})
	if err != nil {
		writeServiceError(c, err)
		return
	}
	WriteSuccess(c, task)
}

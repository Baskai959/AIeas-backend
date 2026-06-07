package app

import (
	"context"

	"aieas_backend/internal/domain"
)

type LiveAnalysisTaskStatus = domain.LiveAnalysisReportStatus

const (
	LiveAnalysisTaskPending   LiveAnalysisTaskStatus = domain.LiveAnalysisReportPending
	LiveAnalysisTaskRunning   LiveAnalysisTaskStatus = domain.LiveAnalysisReportRunning
	LiveAnalysisTaskSucceeded LiveAnalysisTaskStatus = domain.LiveAnalysisReportSucceeded
	LiveAnalysisTaskFailed    LiveAnalysisTaskStatus = domain.LiveAnalysisReportFailed
)

type LiveAnalysisTask = domain.LiveAnalysisReport

type CreateReportInput struct {
	ActorID       string
	ActorRole     domain.Role
	LiveSessionID uint64
}

type LiveAnalysisOptions struct {
	CallbackURL    string
	CallbackAPIKey string
	MaxAttempts    int
}

type CallbackInput struct {
	RequestID       string
	Success         bool
	Status          string
	Summary         string
	ErrorMessage    *string
	CallbackContext map[string]interface{}
}

type Task = LiveAnalysisTask

// LiveAnalysisUseCase 暴露直播复盘/分析边界。
type LiveAnalysisUseCase interface {
	CreateReport(ctx context.Context, in CreateReportInput) (LiveAnalysisTask, error)
	GetReport(ctx context.Context, liveSessionID uint64, actorID string, actorRole domain.Role) (LiveAnalysisTask, error)
	HandleCallback(ctx context.Context, in CallbackInput) (LiveAnalysisTask, error)
}

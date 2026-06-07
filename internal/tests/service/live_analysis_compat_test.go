package service

import (
	liveanalysisapp "aieas_backend/internal/modules/live_analysis/app"
	liveanalysisports "aieas_backend/internal/modules/live_analysis/ports"
	"aieas_backend/internal/tests/repository"
)

type LiveAnalysisTaskStatus = liveanalysisapp.LiveAnalysisTaskStatus

const (
	LiveAnalysisTaskPending   LiveAnalysisTaskStatus = liveanalysisapp.LiveAnalysisTaskPending
	LiveAnalysisTaskRunning   LiveAnalysisTaskStatus = liveanalysisapp.LiveAnalysisTaskRunning
	LiveAnalysisTaskSucceeded LiveAnalysisTaskStatus = liveanalysisapp.LiveAnalysisTaskSucceeded
	LiveAnalysisTaskFailed    LiveAnalysisTaskStatus = liveanalysisapp.LiveAnalysisTaskFailed
)

type LiveAnalysisTask = liveanalysisapp.LiveAnalysisTask
type CreateLiveAnalysisReportInput = liveanalysisapp.CreateReportInput
type LiveAnalysisOptions = liveanalysisapp.LiveAnalysisOptions
type LiveAnalysisCallbackInput = liveanalysisapp.CallbackInput
type LiveAnalysisService = liveanalysisapp.LiveAnalysisService

func NewLiveAnalysisService(reports repository.LiveAnalysisReportRepository, sessions repository.LiveSessionRepository, requester liveanalysisports.AsyncRequester, options LiveAnalysisOptions) *LiveAnalysisService {
	if reports == nil {
		reports = repository.NewMemoryLiveAnalysisReportRepository()
	}
	if requester == nil {
		requester = liveanalysisports.DisabledAsyncRequester{}
	}
	return liveanalysisapp.NewLiveAnalysisService(reports, sessions, requester, options)
}

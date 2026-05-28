package domain

import "time"

type LiveAnalysisReportStatus string

const (
	LiveAnalysisReportPending   LiveAnalysisReportStatus = "PENDING"
	LiveAnalysisReportRunning   LiveAnalysisReportStatus = "RUNNING"
	LiveAnalysisReportSucceeded LiveAnalysisReportStatus = "SUCCEEDED"
	LiveAnalysisReportFailed    LiveAnalysisReportStatus = "FAILED"
)

func (s LiveAnalysisReportStatus) Valid() bool {
	switch s {
	case LiveAnalysisReportPending, LiveAnalysisReportRunning, LiveAnalysisReportSucceeded, LiveAnalysisReportFailed:
		return true
	default:
		return false
	}
}

type LiveAnalysisReport struct {
	TaskID         string                   `json:"taskId"`
	AgentRequestID string                   `json:"agentRequestId,omitempty"`
	LiveSessionID  uint64                   `json:"liveSessionId"`
	MerchantID     string                   `json:"merchantId"`
	Status         LiveAnalysisReportStatus `json:"status"`
	AttemptCount   int                      `json:"attemptCount"`
	Prompt         string                   `json:"prompt"`
	Report         string                   `json:"report"`
	ErrorMessage   string                   `json:"errorMessage,omitempty"`
	CreatedAt      time.Time                `json:"createdAt"`
	UpdatedAt      time.Time                `json:"updatedAt"`
}

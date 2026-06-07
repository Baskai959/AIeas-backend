package ports

import (
	"context"
	"errors"

	"aieas_backend/internal/domain"
)

var ErrLiveAnalysisUnavailable = errors.New("live analysis generator unavailable")

// LiveAnalysisReportRepository 是直播分析任务持久化端口。
type LiveAnalysisReportRepository interface {
	Create(ctx context.Context, report *domain.LiveAnalysisReport) error
	FindByTaskID(ctx context.Context, taskID string) (domain.LiveAnalysisReport, error)
	FindByLiveSessionID(ctx context.Context, liveSessionID uint64) (domain.LiveAnalysisReport, error)
	FindByAgentRequestID(ctx context.Context, requestID string) (domain.LiveAnalysisReport, error)
	Update(ctx context.Context, report *domain.LiveAnalysisReport) error
}

// LiveSessionRepository 是直播分析授权场次读取端口。
type LiveSessionRepository interface {
	Get(ctx context.Context, id uint64) (domain.LiveSession, error)
}

// AsyncRequestInput 是直播分析异步请求载荷。
type AsyncRequestInput struct {
	Prompt          string
	CallbackURL     string
	CallbackHeaders map[string]string
	CallbackContext map[string]interface{}
	ToolName        string
	ToolArguments   map[string]interface{}
}

// AsyncRequestResult 是直播分析异步请求结果。
type AsyncRequestResult struct {
	RequestID string
	Status    string
	Message   string
}

// AsyncRequester 是直播分析外部请求器端口。
type AsyncRequester interface {
	RequestLiveAnalysis(ctx context.Context, in AsyncRequestInput) (AsyncRequestResult, error)
}

type DisabledAsyncRequester struct{}

func (DisabledAsyncRequester) RequestLiveAnalysis(ctx context.Context, in AsyncRequestInput) (AsyncRequestResult, error) {
	_ = ctx
	_ = in
	return AsyncRequestResult{}, ErrLiveAnalysisUnavailable
}

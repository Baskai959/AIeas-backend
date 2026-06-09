package ports

import (
	"context"
	"time"

	"aieas_backend/internal/domain"
	aiports "aieas_backend/internal/modules/ai/ports"
)

// UserRepository 是 MCP read 用户查询端口。
type UserRepository interface {
	FindByID(id string) (domain.User, error)
	List(filter domain.UserFilter) ([]domain.User, error)
}

// AuctionRepository 是 MCP read/control 拍品查询端口。
type AuctionRepository interface {
	FindByID(ctx context.Context, id uint64) (domain.AuctionLot, error)
	List(ctx context.Context, filter domain.AuctionFilter) ([]domain.AuctionLot, error)
}

// LiveSessionRepository 是 MCP read/control 直播场次查询端口。
type LiveSessionRepository interface {
	Get(ctx context.Context, id uint64) (domain.LiveSession, error)
	GetActiveByMerchantID(ctx context.Context, merchantID string) (domain.LiveSession, error)
	List(ctx context.Context, filter domain.LiveSessionFilter) ([]domain.LiveSession, error)
}

// BidRepository 是 MCP read 出价查询端口。
type BidRepository interface {
	ListByLiveSession(ctx context.Context, sessionID uint64, sortBy string, limit, offset int) ([]domain.BidRecord, error)
}

// OrderRepository 是 MCP read 订单查询端口。
type OrderRepository interface {
	FindByAuctionID(ctx context.Context, auctionID uint64) (domain.OrderDeal, error)
	List(ctx context.Context, filter domain.OrderFilter) ([]domain.OrderDeal, error)
}

// AuditRepository 是 MCP read 审计日志查询端口。
type AuditRepository interface {
	List(ctx context.Context, filter domain.AuditFilter) ([]domain.AuditLog, error)
}

// RiskUseCase 是 MCP read 风险事件查询端口。
type RiskUseCase interface {
	ListEvents(ctx context.Context, filter domain.RiskEventFilter) ([]domain.RiskEvent, error)
}

// AuctionStateUseCase 是 MCP read/control 读取拍卖状态端口。
type AuctionStateUseCase interface {
	State(ctx context.Context, id uint64, actorID string, actorRole domain.Role) (domain.AuctionState, error)
}

// ActivateLiveSessionAuctionInput 是直播场次内开拍时的最小控制载荷。
type ActivateLiveSessionAuctionInput struct {
	SessionID   uint64
	AuctionID   uint64
	ActorID     string
	ActorRole   domain.Role
	DurationSec int
	StartTime   *time.Time
}

// LiveSessionStats 是 MCP control 展示直播态势时的统计快照。
type LiveSessionStats struct {
	LiveSessionID        uint64 `json:"liveSessionId"`
	Online               int    `json:"online"`
	LotsTotal            int    `json:"lotsTotal"`
	LotsSold             int    `json:"lotsSold"`
	LotsUnsold           int    `json:"lotsUnsold"`
	BidCount             int    `json:"bidCount"`
	GMVCent              int64  `json:"gmvCent"`
	ViewerPeak           int    `json:"viewerPeak"`
	ViewerTotal          int    `json:"viewerTotal"`
	ActiveAuctionID      uint64 `json:"activeAuctionId"`
	CurrentBidCount      int    `json:"currentBidCount"`
	CurrentRemainSeconds int64  `json:"currentRemainSeconds"`
	CurrentPrice         int64  `json:"currentPrice"`
}

// LiveSessionUseCase 是 MCP read/control 调用直播场次能力的端口。
type LiveSessionUseCase interface {
	ListByMerchantFiltered(ctx context.Context, filter domain.LiveSessionFilter, actorID string, actorRole domain.Role) ([]domain.LiveSession, error)
	ListLots(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) ([]domain.AuctionLot, error)
	ListBidsPaged(ctx context.Context, sessionID uint64, sortBy string, limit, offset int, actorID string, actorRole domain.Role) ([]domain.BidRecord, error)
	Stats(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) (LiveSessionStats, error)
	MountAuction(ctx context.Context, sessionID, auctionID uint64, actorID string, actorRole domain.Role) (domain.AuctionLot, error)
	UnmountAuction(ctx context.Context, sessionID, auctionID uint64, actorID string, actorRole domain.Role) error
	ActivateAuctionWithOptions(ctx context.Context, in ActivateLiveSessionAuctionInput) (domain.AuctionLot, error)
	DeactivateAuction(ctx context.Context, sessionID uint64, actorID string, actorRole domain.Role) (domain.LiveSession, error)
}

// OrderUseCase 是 MCP read 查询订单边界端口。
type OrderUseCase interface {
	List(ctx context.Context, filter domain.OrderFilter, actorID string, actorRole domain.Role) ([]domain.OrderDeal, error)
	Get(ctx context.Context, id uint64, actorID string, actorRole domain.Role) (domain.OrderDeal, error)
}

// HammerUseCase 是 MCP control 落槌能力端口。
type HammerUseCase interface {
	Hammer(ctx context.Context, in domain.HammerInput) (domain.HammerResult, *domain.OrderDeal, error)
}

// ApprovalRequester 是 MCP control 请求 AI 审批端口。
type ApprovalRequester = aiports.ApprovalRequester

// StatusNotifier 是 MCP read/control 状态回传端口。
type StatusNotifier = aiports.StatusNotifier

// BroadcastNotifier 是 MCP control 播报状态回传端口。
type BroadcastNotifier = aiports.BroadcastNotifier

// ApprovalInput 是 AI 审批请求载荷。
type ApprovalInput = aiports.ApprovalInput

// ApprovalDecision 是 AI 审批结果。
type ApprovalDecision = aiports.ApprovalDecision

// AIAssistantFacade 汇总 MCP control 使用到的 AI 助手能力。
type AIAssistantFacade interface {
	ApprovalRequester
	StatusNotifier
	BroadcastNotifier
}

// LiveVoiceSynthesizer 是 MCP control 语音合成端口。
type LiveVoiceSynthesizer interface {
	SynthesizeLiveVoice(ctx context.Context, in LiveVoiceSynthesisInput) (LiveVoiceSynthesisResult, error)
}

// LiveVoiceBroadcaster 是 MCP control 语音播报推送端口。
type LiveVoiceBroadcaster interface {
	BroadcastLiveVoice(ctx context.Context, liveSessionID uint64, payload LiveVoiceBroadcastPayload) (int, error)
}

// LiveVoiceSynthesisInput 是语音合成请求载荷。
type LiveVoiceSynthesisInput struct {
	LiveSessionID uint64
	Text          string
	RequestID     string
}

// LiveVoiceSynthesisResult 是语音合成结果。
type LiveVoiceSynthesisResult struct {
	Audio       []byte
	AudioFormat string
	Encoding    string
	SampleRate  int
	Channels    int
	Voice       string
	Provider    string
}

// LiveVoiceBroadcastPayload 是 MCP 播报事件载荷。
type LiveVoiceBroadcastPayload struct {
	LiveSessionID uint64    `json:"liveSessionId"`
	Text          string    `json:"text"`
	RequestID     string    `json:"requestId,omitempty"`
	AudioBase64   string    `json:"audioBase64"`
	AudioFormat   string    `json:"audioFormat"`
	Encoding      string    `json:"encoding"`
	SampleRate    int       `json:"sampleRate"`
	Channels      int       `json:"channels"`
	Voice         string    `json:"voice,omitempty"`
	Provider      string    `json:"provider,omitempty"`
	AudioBytes    int       `json:"audioBytes"`
	CreatedAt     time.Time `json:"createdAt"`
}

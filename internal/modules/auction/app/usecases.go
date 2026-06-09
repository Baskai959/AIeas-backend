package app

import (
	"context"
	"encoding/json"
	"time"

	"aieas_backend/internal/domain"
)

type CreateAuctionInput struct {
	ActorID           string
	ActorRole         domain.Role
	AuctionID         uint64
	SellerID          string
	Title             string
	Description       string
	Category          string
	Brand             string
	ConditionGrade    domain.ConditionGrade
	ImageURLs         []string
	CoverURL          string
	AuctionType       domain.AuctionType
	StartPrice        int64
	ReservePrice      int64
	CapPrice          int64
	IncrementRule     json.RawMessage
	AntiSnipingSec    int
	AntiExtendSec     int
	AntiExtendMode    domain.AuctionExtendMode
	DepositAmount     int64
	Status            domain.AuctionStatus
	StartTime         time.Time
	EndTime           time.Time
	DurationSec       int
	AllowSystemStatus bool
}

type UpdateAuctionInput struct {
	ActorID           string
	ActorRole         domain.Role
	Title             *string
	Description       *string
	Category          *string
	Brand             *string
	ConditionGrade    *domain.ConditionGrade
	ImageURLs         *[]string
	CoverURL          *string
	StartPrice        *int64
	ReservePrice      *int64
	CapPrice          *int64
	IncrementRule     *json.RawMessage
	AntiSnipingSec    *int
	AntiExtendSec     *int
	AntiExtendMode    *domain.AuctionExtendMode
	DepositAmount     *int64
	Status            *domain.AuctionStatus
	AuditRejectReason *string
	StartTime         *time.Time
	EndTime           *time.Time
	DurationSec       *int
	AllowSystemStatus bool
}

type AuctionAuditCallbackInput struct {
	RequestID     string
	Status        string
	Success       bool
	IsApproved    bool
	RejectReasons []string
	RiskLabels    []string
	Context       map[string]any
}

type AuctionAuditCallbackResult struct {
	Accepted      bool     `json:"accepted"`
	RequestID     string   `json:"requestId,omitempty"`
	AuctionID     uint64   `json:"auctionId"`
	Status        string   `json:"status,omitempty"`
	LotStatus     string   `json:"lotStatus,omitempty"`
	Success       bool     `json:"success"`
	IsApproved    bool     `json:"isApproved"`
	RejectReason  string   `json:"rejectReason,omitempty"`
	RejectReasons []string `json:"rejectReasons,omitempty"`
	RiskLabels    []string `json:"riskLabels,omitempty"`
	Scope         string   `json:"scope,omitempty"`
}

type PlaceBidInput struct {
	RequestID            string
	AuctionID            uint64
	BidderID             string
	UserRole             domain.Role
	Price                int64
	ExpectedCurrentPrice *int64
	Source               string
}

// AuctionCommandUseCase 暴露拍卖写用例边界。
type AuctionCommandUseCase interface {
	Create(ctx context.Context, in CreateAuctionInput) (domain.AuctionLot, error)
	HandleAuditCallback(ctx context.Context, in AuctionAuditCallbackInput) (AuctionAuditCallbackResult, error)
	Update(ctx context.Context, id uint64, in UpdateAuctionInput) (domain.AuctionLot, error)
	Delete(ctx context.Context, id uint64, actorID string, actorRole domain.Role) error
	Start(ctx context.Context, id uint64, actorID string, actorRole domain.Role) (domain.AuctionLot, error)
	Cancel(ctx context.Context, id uint64, actorID string, actorRole domain.Role) (domain.AuctionLot, error)
}

// AuctionQueryUseCase 暴露拍卖读用例边界。
type AuctionQueryUseCase interface {
	List(ctx context.Context, filter domain.AuctionFilter, actorID string, actorRole domain.Role) ([]domain.AuctionLot, error)
	Get(ctx context.Context, id uint64, actorID string, actorRole domain.Role) (domain.AuctionLot, error)
	State(ctx context.Context, id uint64, actorID string, actorRole domain.Role) (domain.AuctionState, error)
}

// AuctionUseCase 汇总拍卖读写用例边界。
type AuctionUseCase interface {
	AuctionCommandUseCase
	AuctionQueryUseCase
}

// BidUseCase 暴露出价用例边界。
type BidUseCase interface {
	Place(ctx context.Context, in PlaceBidInput) (domain.BidResult, error)
}

// HammerUseCase 暴露落槌用例边界。
type HammerUseCase interface {
	Hammer(ctx context.Context, in domain.HammerInput) (domain.HammerResult, *domain.OrderDeal, error)
}

// WSBidUseCase 是 WS 出价入口依赖的 auction app 边界。
type WSBidUseCase interface {
	BidUseCase
}

// WSAuctionRankingUseCase 是 WS 初始化排行榜依赖的 auction app 边界。
type WSAuctionRankingUseCase interface {
	TopN(ctx context.Context, auctionID uint64, limit int) ([]domain.RankingEntry, error)
}

// WSAuctionStateUseCase 是 WS 快照 DB fallback 读取拍卖状态的 app 边界。
type WSAuctionStateUseCase interface {
	State(ctx context.Context, id uint64, actorID string, actorRole domain.Role) (domain.AuctionState, error)
}

// WSAuctionRealtimeSnapshotProvider 是 WS 快照 RT-first 读取拍卖状态的 app 边界。
type WSAuctionRealtimeSnapshotProvider interface {
	GetAuctionState(ctx context.Context, auctionID uint64) (domain.AuctionState, bool, error)
}

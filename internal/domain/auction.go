package domain

import (
	"encoding/json"
	"strings"
	"time"
)

type AuctionType string

const (
	AuctionTypeEnglish AuctionType = "ENGLISH"
)

func (t AuctionType) Valid() bool {
	return t == AuctionTypeEnglish
}

type AuctionStatus string

const (
	AuctionStatusDraft         AuctionStatus = "DRAFT"
	AuctionStatusPendingAudit  AuctionStatus = "PENDING_AUDIT"
	AuctionStatusReady         AuctionStatus = "READY"
	AuctionStatusWarmingUp     AuctionStatus = "WARMING_UP"
	AuctionStatusRunning       AuctionStatus = "RUNNING"
	AuctionStatusExtended      AuctionStatus = "EXTENDED"
	AuctionStatusHammerPending AuctionStatus = "HAMMER_PENDING"
	AuctionStatusClosedWon     AuctionStatus = "CLOSED_WON"
	AuctionStatusClosedFailed  AuctionStatus = "CLOSED_FAILED"
	AuctionStatusSettled       AuctionStatus = "SETTLED"
)

type AuctionExtendMode string

const (
	AuctionExtendModeAdd   AuctionExtendMode = "ADD"
	AuctionExtendModeReset AuctionExtendMode = "RESET"
)

func (m AuctionExtendMode) Valid() bool {
	return m == AuctionExtendModeAdd || m == AuctionExtendModeReset
}

func NormalizeAuctionExtendMode(m AuctionExtendMode) AuctionExtendMode {
	normalized := strings.ToUpper(strings.TrimSpace(string(m)))
	if normalized == "" {
		return AuctionExtendModeAdd
	}
	return AuctionExtendMode(normalized)
}

func (s AuctionStatus) Valid() bool {
	switch s {
	case AuctionStatusDraft, AuctionStatusPendingAudit, AuctionStatusReady,
		AuctionStatusWarmingUp, AuctionStatusRunning, AuctionStatusExtended,
		AuctionStatusHammerPending, AuctionStatusClosedWon, AuctionStatusClosedFailed,
		AuctionStatusSettled:
		return true
	default:
		return false
	}
}

func (s AuctionStatus) Terminal() bool {
	switch s {
	case AuctionStatusClosedWon, AuctionStatusClosedFailed, AuctionStatusSettled:
		return true
	default:
		return false
	}
}

func CanTransitionAuction(from, to AuctionStatus) bool {
	if from == to {
		return true
	}
	switch from {
	case AuctionStatusDraft:
		return to == AuctionStatusPendingAudit || to == AuctionStatusReady || to == AuctionStatusClosedFailed
	case AuctionStatusPendingAudit:
		return to == AuctionStatusReady || to == AuctionStatusClosedFailed
	case AuctionStatusReady:
		return to == AuctionStatusWarmingUp || to == AuctionStatusRunning || to == AuctionStatusClosedFailed
	case AuctionStatusWarmingUp:
		return to == AuctionStatusRunning || to == AuctionStatusClosedFailed
	case AuctionStatusRunning:
		return to == AuctionStatusExtended || to == AuctionStatusHammerPending || to == AuctionStatusClosedWon || to == AuctionStatusClosedFailed
	case AuctionStatusExtended:
		return to == AuctionStatusHammerPending || to == AuctionStatusClosedWon || to == AuctionStatusClosedFailed
	case AuctionStatusHammerPending:
		return to == AuctionStatusClosedWon || to == AuctionStatusClosedFailed
	case AuctionStatusClosedWon:
		return to == AuctionStatusSettled
	default:
		return false
	}
}

type AuctionLot struct {
	AuctionID      uint64            `json:"auctionId"`
	ItemID         uint64            `json:"itemId"`
	SellerID       string            `json:"sellerId"`
	LiveRoomID     uint64            `json:"liveRoomId,omitempty"`
	LiveSessionID  *uint64           `json:"liveSessionId,omitempty"`
	AuctionType    AuctionType       `json:"auctionType"`
	StartPrice     int64             `json:"startPrice"`
	ReservePrice   int64             `json:"reservePrice"`
	CapPrice       int64             `json:"capPrice"`
	IncrementRule  json.RawMessage   `json:"incrementRule"`
	AntiSnipingSec int               `json:"antiSnipingSec"`
	AntiExtendSec  int               `json:"antiExtendSec"`
	AntiExtendMode AuctionExtendMode `json:"antiExtendMode"`
	DepositAmount  int64             `json:"depositAmount"`
	Status         AuctionStatus     `json:"status"`
	RuleSnapshot   json.RawMessage   `json:"ruleSnapshot"`
	StartTime      time.Time         `json:"startTime"`
	EndTime        time.Time         `json:"endTime"`
	DurationSec    int               `json:"durationSec,omitempty"`
	WinnerID       *string           `json:"winnerId,omitempty"`
	DealPrice      *int64            `json:"dealPrice,omitempty"`
	ClosedAt       *time.Time        `json:"closedAt,omitempty"`
	ClosedBy       string            `json:"closedBy,omitempty"`
	// Version 是 MySQL 行级乐观锁版本号，仅在落槌路径（CloseWithVersion）参与 CAS。
	Version   int64     `json:"version"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type AuctionFilter struct {
	SellerID   string
	Status     AuctionStatus
	ItemID     uint64
	LiveRoomID uint64
	Limit      int
	Offset     int
}

type AuctionPatch struct {
	StartPrice     *int64
	ReservePrice   *int64
	CapPrice       *int64
	IncrementRule  *json.RawMessage
	AntiSnipingSec *int
	AntiExtendSec  *int
	AntiExtendMode *AuctionExtendMode
	DepositAmount  *int64
	Status         *AuctionStatus
	StartTime      *time.Time
	EndTime        *time.Time
	DurationSec    *int
}

type AuctionState struct {
	AuctionID      uint64        `json:"auctionId"`
	Status         AuctionStatus `json:"status"`
	CurrentPrice   int64         `json:"currentPrice"`
	LeaderBidderID string        `json:"leaderBidderId,omitempty"`
	StartTime      time.Time     `json:"startTime"`
	EndTime        time.Time     `json:"endTime"`
	LastBidTSMS    int64         `json:"lastBidTsMs"`
	ExtendCount    int           `json:"extendCount"`
	Version        int64         `json:"version"`
	Source         string        `json:"source"`
}

type BidRiskResult string

const (
	BidRiskAllow  BidRiskResult = "ALLOW"
	BidRiskReject BidRiskResult = "REJECT"
	BidRiskReview BidRiskResult = "REVIEW"
)

type BidRecord struct {
	ID            uint64
	RequestID     string
	AuctionID     uint64
	LiveSessionID *uint64
	BidderID      string
	BidPrice      int64
	BidTSMS       int64
	Source        string
	RiskResult    BidRiskResult
	RejectReason  string
	CreatedAt     time.Time
}

type BidInput struct {
	RequestID            string
	AuctionID            uint64
	BidderID             string
	Price                int64
	ExpectedCurrentPrice *int64
	Now                  time.Time
	Source               string
	MinIncrement         int64
	AntiSnipingMS        int64
	AntiExtendMS         int64
	AntiExtendMode       AuctionExtendMode
	MaxExtendCount       int
	FreqLimitCount       int
	FreqWindowMS         int64
	IdempotencyTTL       time.Duration
	StartPrice           int64
	CapPrice             int64
	IncrementRule        IncrementRule
}

type BidResult struct {
	RequestID      string        `json:"requestId"`
	AuctionID      uint64        `json:"auctionId"`
	BidderID       string        `json:"bidderId,omitempty"`
	Price          int64         `json:"price,omitempty"`
	Accepted       bool          `json:"accepted"`
	Duplicate      bool          `json:"duplicate,omitempty"`
	Reason         string        `json:"reason,omitempty"`
	CurrentPrice   int64         `json:"currentPrice"`
	LeaderBidderID string        `json:"leaderBidderId,omitempty"`
	EndTime        time.Time     `json:"endTime"`
	Extended       bool          `json:"extended,omitempty"`
	ExtendCount    int           `json:"extendCount,omitempty"`
	Version        int64         `json:"version,omitempty"`
	Seq            int64         `json:"seq,omitempty"`
	StreamID       string        `json:"streamId,omitempty"`
	Event          string        `json:"event,omitempty"`
	RiskResult     BidRiskResult `json:"riskResult"`
	AuctionStatus  AuctionStatus `json:"auctionStatus,omitempty"`
	AutoClosed     bool          `json:"autoClosed,omitempty"`
}

type RankingEntry struct {
	Rank     int    `json:"rank"`
	BidderID string `json:"bidderId"`
	Price    int64  `json:"price"`
}

type HammerInput struct {
	RequestID      string
	AuctionID      uint64
	ActorID        string
	ActorRole      Role
	ClosedBy       string
	Force          bool
	ReservePrice   int64
	Now            time.Time
	IdempotencyTTL time.Duration
}

type HammerResult struct {
	RequestID string        `json:"requestId"`
	AuctionID uint64        `json:"auctionId"`
	Status    AuctionStatus `json:"status"`
	WinnerID  string        `json:"winnerId,omitempty"`
	Price     int64         `json:"price,omitempty"`
	Duplicate bool          `json:"duplicate,omitempty"`
	ClosedAt  time.Time     `json:"closedAt"`
	Version   int64         `json:"version,omitempty"`
}

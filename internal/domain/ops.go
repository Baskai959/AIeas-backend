package domain

import (
	"encoding/json"
	"time"
)

type OrderStatus string

const (
	OrderStatusCreated   OrderStatus = "CREATED"
	OrderStatusPaid      OrderStatus = "PAID"
	OrderStatusTimeout   OrderStatus = "TIMEOUT"
	OrderStatusCancelled OrderStatus = "CANCELLED"
)

type PayStatus string

const (
	PayStatusUnpaid   PayStatus = "UNPAID"
	PayStatusPaid     PayStatus = "PAID"
	PayStatusRefunded PayStatus = "REFUNDED"
)

type OrderDeal struct {
	ID            uint64      `json:"id"`
	AuctionID     uint64      `json:"auctionId"`
	WinnerID      string      `json:"winnerId"`
	SellerID      string      `json:"sellerId"`
	DealPrice     int64       `json:"dealPrice"`
	DepositAmount int64       `json:"depositAmount"`
	Status        OrderStatus `json:"status"`
	PayStatus     PayStatus   `json:"payStatus"`
	PayDeadline   *time.Time  `json:"payDeadline,omitempty"`
	PaidAt        *time.Time  `json:"paidAt,omitempty"`
	ClosedAt      *time.Time  `json:"closedAt,omitempty"`
	CreatedAt     time.Time   `json:"createdAt"`
	UpdatedAt     time.Time   `json:"updatedAt"`
}

type OrderFilter struct {
	WinnerID  string
	SellerID  string
	Status    OrderStatus
	PayStatus PayStatus
	Limit     int
	Offset    int
}

type DepositStatus string

const (
	DepositStatusPending  DepositStatus = "PENDING"
	DepositStatusReady    DepositStatus = "READY"
	DepositStatusCaptured DepositStatus = "CAPTURED"
	DepositStatusReleased DepositStatus = "RELEASED"
	DepositStatusFailed   DepositStatus = "FAILED"
)

type DepositLedger struct {
	ID             uint64        `json:"id"`
	AuctionID      uint64        `json:"auctionId"`
	UserID         string        `json:"userId"`
	Amount         int64         `json:"amount"`
	Status         DepositStatus `json:"status"`
	RelatedOrderID *uint64       `json:"relatedOrderId,omitempty"`
	Remark         string        `json:"remark,omitempty"`
	CreatedAt      time.Time     `json:"createdAt"`
	UpdatedAt      time.Time     `json:"updatedAt"`
}

type AuditLog struct {
	ID           uint64          `json:"id"`
	OperatorID   string          `json:"operatorId"`
	OperatorRole Role            `json:"operatorRole"`
	Action       string          `json:"action"`
	TargetType   string          `json:"targetType"`
	TargetID     string          `json:"targetId"`
	Payload      json.RawMessage `json:"payload,omitempty"`
	IP           string          `json:"ip,omitempty"`
	UserAgent    string          `json:"userAgent,omitempty"`
	CreatedAt    time.Time       `json:"createdAt"`
}

type AuditFilter struct {
	OperatorID string
	Action     string
	StartTime  *time.Time
	EndTime    *time.Time
	Limit      int
	Offset     int
}

type Blacklist struct {
	ID        uint64     `json:"id"`
	UserID    string     `json:"userId"`
	Reason    string     `json:"reason"`
	CreatedBy string     `json:"createdBy"`
	CreatedAt time.Time  `json:"createdAt"`
	ExpiresAt *time.Time `json:"expiresAt,omitempty"`
}

type RiskSeverity string

const (
	RiskSeverityLow  RiskSeverity = "LOW"
	RiskSeverityMid  RiskSeverity = "MID"
	RiskSeverityHigh RiskSeverity = "HIGH"
)

type RiskEventStatus string

const (
	RiskEventPending  RiskEventStatus = "PENDING"
	RiskEventReviewed RiskEventStatus = "REVIEWED"
	RiskEventIgnored  RiskEventStatus = "IGNORED"
)

type RiskEvent struct {
	ID         uint64          `json:"id"`
	EventType  string          `json:"eventType"`
	UserID     string          `json:"userId,omitempty"`
	AuctionID  uint64          `json:"auctionId,omitempty"`
	Severity   RiskSeverity    `json:"severity"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	Status     RiskEventStatus `json:"status"`
	ReviewedBy string          `json:"reviewedBy,omitempty"`
	ReviewedAt *time.Time      `json:"reviewedAt,omitempty"`
	CreatedAt  time.Time       `json:"createdAt"`
}

type RiskEventFilter struct {
	Status    RiskEventStatus
	EventType string
	UserID    string
	Limit     int
	Offset    int
}

type ConfigItem struct {
	Key         string
	Value       json.RawMessage
	Description string
	UpdatedBy   string
	UpdatedAt   time.Time
}

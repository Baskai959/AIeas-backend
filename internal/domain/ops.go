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

func (s OrderStatus) Valid() bool {
	switch s {
	case OrderStatusCreated, OrderStatusPaid, OrderStatusTimeout, OrderStatusCancelled:
		return true
	default:
		return false
	}
}

func (s OrderStatus) Terminal() bool {
	switch s {
	case OrderStatusPaid, OrderStatusTimeout, OrderStatusCancelled:
		return true
	default:
		return false
	}
}

func CanTransitionOrder(from, to OrderStatus) bool {
	if from == to {
		return true
	}
	switch from {
	case OrderStatusCreated:
		return to == OrderStatusPaid || to == OrderStatusTimeout || to == OrderStatusCancelled
	default:
		return false
	}
}

type PayStatus string

const (
	PayStatusUnpaid   PayStatus = "UNPAID"
	PayStatusPaid     PayStatus = "PAID"
	PayStatusRefunded PayStatus = "REFUNDED"
)

type FulfillmentStatus string

const (
	FulfillmentStatusUnshipped FulfillmentStatus = "UNSHIPPED"
	FulfillmentStatusShipped   FulfillmentStatus = "SHIPPED"
	FulfillmentStatusReceived  FulfillmentStatus = "RECEIVED"
)

func (s FulfillmentStatus) Valid() bool {
	switch s {
	case FulfillmentStatusUnshipped, FulfillmentStatusShipped, FulfillmentStatusReceived:
		return true
	default:
		return false
	}
}

func NormalizeFulfillmentStatus(status FulfillmentStatus) FulfillmentStatus {
	if status.Valid() {
		return status
	}
	return FulfillmentStatusUnshipped
}

type OrderDeal struct {
	ID                uint64            `json:"id"`
	AuctionID         uint64            `json:"auctionId"`
	LiveSessionID     *uint64           `json:"liveSessionId,omitempty"`
	LotSnapshot       json.RawMessage   `json:"lotSnapshot,omitempty"`
	WinnerID          string            `json:"winnerId"`
	WinnerNickname    string            `json:"winnerNickname,omitempty"`
	SellerID          string            `json:"sellerId"`
	DealPrice         int64             `json:"dealPrice"`
	DepositAmount     int64             `json:"depositAmount"`
	Status            OrderStatus       `json:"status"`
	PayStatus         PayStatus         `json:"payStatus"`
	FulfillmentStatus FulfillmentStatus `json:"fulfillmentStatus"`
	PayDeadline       *time.Time        `json:"payDeadline,omitempty"`
	PaidAt            *time.Time        `json:"paidAt,omitempty"`
	ShippedAt         *time.Time        `json:"shippedAt,omitempty"`
	ReceivedAt        *time.Time        `json:"receivedAt,omitempty"`
	ClosedAt          *time.Time        `json:"closedAt,omitempty"`
	Version           int64             `json:"version,omitempty"`
	CreatedAt         time.Time         `json:"createdAt"`
	UpdatedAt         time.Time         `json:"updatedAt"`
}

func (o OrderDeal) PaymentExpired(now time.Time) bool {
	return o.PayDeadline != nil && !now.UTC().Before(o.PayDeadline.UTC())
}

func (o *OrderDeal) MarkPaid(now time.Time) error {
	if o == nil {
		return ErrInvalidArgument
	}
	if o.Status == OrderStatusPaid && o.PayStatus == PayStatusPaid {
		return nil
	}
	if !CanTransitionOrder(o.Status, OrderStatusPaid) {
		return ErrInvalidState
	}
	paidAt := now.UTC()
	o.Status = OrderStatusPaid
	o.PayStatus = PayStatusPaid
	o.PaidAt = &paidAt
	return nil
}

func (o *OrderDeal) MarkTimeout(now time.Time) error {
	if o == nil {
		return ErrInvalidArgument
	}
	if o.Status == OrderStatusTimeout {
		return nil
	}
	if !CanTransitionOrder(o.Status, OrderStatusTimeout) || o.PayStatus != PayStatusUnpaid {
		return ErrInvalidState
	}
	closedAt := now.UTC()
	o.Status = OrderStatusTimeout
	o.ClosedAt = &closedAt
	return nil
}

func (o *OrderDeal) MarkShipped(now time.Time) error {
	if o == nil {
		return ErrInvalidArgument
	}
	if o.FulfillmentStatus == FulfillmentStatusShipped {
		return nil
	}
	if o.Status != OrderStatusPaid || o.PayStatus != PayStatusPaid || NormalizeFulfillmentStatus(o.FulfillmentStatus) != FulfillmentStatusUnshipped {
		return ErrInvalidState
	}
	shippedAt := now.UTC()
	o.FulfillmentStatus = FulfillmentStatusShipped
	o.ShippedAt = &shippedAt
	return nil
}

func (o *OrderDeal) MarkReceived(now time.Time) error {
	if o == nil {
		return ErrInvalidArgument
	}
	if o.FulfillmentStatus == FulfillmentStatusReceived {
		return nil
	}
	if o.Status != OrderStatusPaid || o.PayStatus != PayStatusPaid || NormalizeFulfillmentStatus(o.FulfillmentStatus) != FulfillmentStatusShipped {
		return ErrInvalidState
	}
	receivedAt := now.UTC()
	o.FulfillmentStatus = FulfillmentStatusReceived
	o.ReceivedAt = &receivedAt
	return nil
}

type OrderFilter struct {
	WinnerID          string
	SellerID          string
	AuctionID         uint64
	LiveSessionID     uint64
	Status            OrderStatus
	PayStatus         PayStatus
	FulfillmentStatus FulfillmentStatus
	Limit             int
	Offset            int
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

type AdminDashboardMetricsFilter struct {
	StartTime time.Time
	EndTime   time.Time
	Bucket    string
}

type AdminDashboardMetrics struct {
	StartTime   time.Time                  `json:"startTime"`
	EndTime     time.Time                  `json:"endTime"`
	Bucket      string                     `json:"bucket"`
	GeneratedAt time.Time                  `json:"generatedAt"`
	Summary     AdminDashboardSummary      `json:"summary"`
	Current     AdminDashboardCurrent      `json:"current"`
	Breakdowns  AdminDashboardBreakdowns   `json:"breakdowns"`
	Trend       []AdminDashboardTrendPoint `json:"trend"`
}

type AdminDashboardSummary struct {
	DealGMVCent              int64 `json:"dealGmvCent"`
	PaidGMVCent              int64 `json:"paidGmvCent"`
	OrderCreatedCount        int64 `json:"orderCreatedCount"`
	PaidOrderCount           int64 `json:"paidOrderCount"`
	UnpaidOrderCount         int64 `json:"unpaidOrderCount"`
	TimeoutOrderCount        int64 `json:"timeoutOrderCount"`
	CancelledOrderCount      int64 `json:"cancelledOrderCount"`
	AuctionCreatedCount      int64 `json:"auctionCreatedCount"`
	ClosedWonAuctionCount    int64 `json:"closedWonAuctionCount"`
	ClosedFailedAuctionCount int64 `json:"closedFailedAuctionCount"`
	LiveSessionCount         int64 `json:"liveSessionCount"`
	LotsTotal                int64 `json:"lotsTotal"`
	LotsSold                 int64 `json:"lotsSold"`
	LotsUnsold               int64 `json:"lotsUnsold"`
	BidCount                 int64 `json:"bidCount"`
	ActiveBidderCount        int64 `json:"activeBidderCount"`
	RiskEventCount           int64 `json:"riskEventCount"`
	ViewerPeak               int64 `json:"viewerPeak"`
	ViewerTotal              int64 `json:"viewerTotal"`
}

type AdminDashboardCurrent struct {
	RunningAuctionCount    int64 `json:"runningAuctionCount"`
	ActiveLiveSessionCount int64 `json:"activeLiveSessionCount"`
	PendingRiskEventCount  int64 `json:"pendingRiskEventCount"`
}

type AdminDashboardBreakdowns struct {
	AuctionStatus []AdminStatusCount `json:"auctionStatus"`
	OrderStatus   []AdminStatusCount `json:"orderStatus"`
	PayStatus     []AdminStatusCount `json:"payStatus"`
	RiskStatus    []AdminStatusCount `json:"riskStatus"`
}

type AdminStatusCount struct {
	Status string `json:"status"`
	Count  int64  `json:"count"`
}

type AdminDashboardTrendPoint struct {
	BucketStart    time.Time `json:"bucketStart"`
	DealGMVCent    int64     `json:"dealGmvCent"`
	PaidGMVCent    int64     `json:"paidGmvCent"`
	PaidOrderCount int64     `json:"paidOrderCount"`
	BidCount       int64     `json:"bidCount"`
	RiskEventCount int64     `json:"riskEventCount"`
}

package domain

import "time"

// LiveSessionStatus 表示直播场次的生命周期状态。
type LiveSessionStatus string

const (
	LiveSessionStatusDraft     LiveSessionStatus = "DRAFT"
	LiveSessionStatusScheduled LiveSessionStatus = "SCHEDULED"
	LiveSessionStatusLive      LiveSessionStatus = "LIVE"
	LiveSessionStatusEnded     LiveSessionStatus = "ENDED"
	LiveSessionStatusCancelled LiveSessionStatus = "CANCELLED"
)

func (s LiveSessionStatus) Valid() bool {
	switch s {
	case LiveSessionStatusDraft, LiveSessionStatusScheduled, LiveSessionStatusLive, LiveSessionStatusEnded, LiveSessionStatusCancelled:
		return true
	default:
		return false
	}
}

// CanTransitionLiveSession 控制直播场次状态机。
func CanTransitionLiveSession(from, to LiveSessionStatus) bool {
	if from == to {
		return true
	}
	switch from {
	case LiveSessionStatusDraft:
		return to == LiveSessionStatusScheduled || to == LiveSessionStatusLive || to == LiveSessionStatusCancelled
	case LiveSessionStatusScheduled:
		return to == LiveSessionStatusDraft || to == LiveSessionStatusLive || to == LiveSessionStatusCancelled
	case LiveSessionStatusLive:
		return to == LiveSessionStatusEnded
	default:
		return false
	}
}

// LiveSession 表示一次直播场次，承载直播展示信息和开播到下播生命周期。
type LiveSession struct {
	ID                 uint64            `json:"id"`
	MerchantID         string            `json:"merchantId"`
	Title              string            `json:"title"`
	Description        string            `json:"description,omitempty"`
	CoverURL           string            `json:"coverUrl,omitempty"`
	Status             LiveSessionStatus `json:"status"`
	IsDigitalHuman     bool              `json:"isDigitalHuman"`
	ActiveAuctionID    uint64            `json:"activeAuctionId,omitempty"`
	OpenedAt           *time.Time        `json:"openedAt,omitempty"`
	ClosedAt           *time.Time        `json:"closedAt,omitempty"`
	ScheduledStartTime *time.Time        `json:"scheduledStartTime,omitempty"`
	PlannedDurationSec int               `json:"plannedDurationSec,omitempty"`
	LotsTotal          int               `json:"lotsTotal"`
	LotsSold           int               `json:"lotsSold"`
	LotsUnsold         int               `json:"lotsUnsold"`
	BidCount           int               `json:"bidCount"`
	GMVCent            int64             `json:"gmvCent"`
	ViewerPeak         int               `json:"viewerPeak"`
	ViewerTotal        int               `json:"viewerTotal"`
	CreatedAt          time.Time         `json:"createdAt"`
	UpdatedAt          time.Time         `json:"updatedAt"`
}

// LiveSessionFilter 用于场次列表查询。
type LiveSessionFilter struct {
	MerchantID string
	Status     LiveSessionStatus
	Keyword    string
	Sort       string
	OpenedFrom *time.Time
	OpenedTo   *time.Time
	Limit      int
	Offset     int
}

// LiveSessionCounters 增量计数（hammer / bid / 在线峰值更新使用）。
type LiveSessionCounters struct {
	LotsTotalDelta  int
	LotsSoldDelta   int
	LotsUnsoldDelta int
	BidCountDelta   int
	GMVCentDelta    int64
	ViewerTotalAdd  int
	ViewerPeakAtMin int // 与当前 viewer_peak 比较，仅在更大时覆盖
}

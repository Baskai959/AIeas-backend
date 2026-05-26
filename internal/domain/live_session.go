package domain

import "time"

// LiveSessionStatus 表示直播场次的生命周期状态。
type LiveSessionStatus string

const (
	LiveSessionStatusLive  LiveSessionStatus = "LIVE"
	LiveSessionStatusEnded LiveSessionStatus = "ENDED"
)

func (s LiveSessionStatus) Valid() bool {
	switch s {
	case LiveSessionStatusLive, LiveSessionStatusEnded:
		return true
	default:
		return false
	}
}

// CanTransitionLiveSession 控制直播场次状态机：仅允许 LIVE -> ENDED。
func CanTransitionLiveSession(from, to LiveSessionStatus) bool {
	if from == to {
		return true
	}
	if from == LiveSessionStatusLive && to == LiveSessionStatusEnded {
		return true
	}
	return false
}

// LiveSession 表示一次"开播-闭播"的具体直播场次，归属于 LiveRoom，承载本场拍品/出价/成交统计。
type LiveSession struct {
	ID          uint64            `json:"id"`
	LiveRoomID  uint64            `json:"liveRoomId"`
	MerchantID  string            `json:"merchantId"`
	Title       string            `json:"title,omitempty"`
	Status      LiveSessionStatus `json:"status"`
	OpenedAt    time.Time         `json:"openedAt"`
	ClosedAt    *time.Time        `json:"closedAt,omitempty"`
	LotsTotal   int               `json:"lotsTotal"`
	LotsSold    int               `json:"lotsSold"`
	LotsUnsold  int               `json:"lotsUnsold"`
	BidCount    int               `json:"bidCount"`
	GMVCent     int64             `json:"gmvCent"`
	ViewerPeak  int               `json:"viewerPeak"`
	ViewerTotal int               `json:"viewerTotal"`
	CreatedAt   time.Time         `json:"createdAt"`
	UpdatedAt   time.Time         `json:"updatedAt"`
}

// LiveSessionFilter 用于场次列表查询。
type LiveSessionFilter struct {
	LiveRoomID uint64
	MerchantID string
	Status     LiveSessionStatus
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

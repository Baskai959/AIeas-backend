package domain

import "time"

// LiveRoomStatus 表示直播间（拍卖房间）的生命周期状态。
type LiveRoomStatus string

const (
	LiveRoomStatusOffline LiveRoomStatus = "OFFLINE"
	LiveRoomStatusLive    LiveRoomStatus = "LIVE"
	LiveRoomStatusClosed  LiveRoomStatus = "CLOSED"
)

func (s LiveRoomStatus) Valid() bool {
	switch s {
	case LiveRoomStatusOffline, LiveRoomStatusLive, LiveRoomStatusClosed:
		return true
	default:
		return false
	}
}

// CanTransitionLiveRoom 控制直播间状态机：直播间是商家的长期资产，CLOSED 也允许重新开播。
func CanTransitionLiveRoom(from, to LiveRoomStatus) bool {
	if from == to {
		return true
	}
	switch from {
	case LiveRoomStatusOffline:
		return to == LiveRoomStatusLive || to == LiveRoomStatusClosed
	case LiveRoomStatusLive:
		return to == LiveRoomStatusOffline || to == LiveRoomStatusClosed
	case LiveRoomStatusClosed:
		return to == LiveRoomStatusLive || to == LiveRoomStatusOffline
	default:
		return false
	}
}

// LiveRoom 表示一个直播间，由商家创建，可挂载多个拍品（auction_lot），但同一时刻
// 通过 active_auction_id 与 Redis 分布式锁保证只有一个拍品在进行中。
type LiveRoom struct {
	ID              uint64         `json:"id"`
	MerchantID      string         `json:"merchantId"`
	Title           string         `json:"title"`
	Description     string         `json:"description,omitempty"`
	CoverURL        string         `json:"coverUrl,omitempty"`
	Status          LiveRoomStatus `json:"status"`
	ActiveAuctionID uint64         `json:"activeAuctionId,omitempty"`
	CreatedAt       time.Time      `json:"createdAt"`
	UpdatedAt       time.Time      `json:"updatedAt"`
}

type LiveRoomFilter struct {
	MerchantID string
	Status     LiveRoomStatus
	Limit      int
	Offset     int
}

type LiveRoomPatch struct {
	Title           *string
	Description     *string
	CoverURL        *string
	Status          *LiveRoomStatus
	ActiveAuctionID *uint64 // 0 表示清空
}

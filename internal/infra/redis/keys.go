package redis

import (
	"fmt"
	"strings"
)

type KeyBuilder struct {
	prefix string
}

func NewKeyBuilder(prefix string) KeyBuilder {
	return KeyBuilder{prefix: strings.Trim(strings.TrimSpace(prefix), ":")}
}

func (b KeyBuilder) key(format string, args ...interface{}) string {
	key := fmt.Sprintf(format, args...)
	if b.prefix == "" {
		return key
	}
	return b.prefix + ":" + key
}

func (b KeyBuilder) AuctionState(auctionID uint64) string {
	return b.key("auction:%d:state", auctionID)
}

func (b KeyBuilder) AuctionBids(auctionID uint64) string {
	return b.key("auction:%d:bids", auctionID)
}

func (b KeyBuilder) AuctionEnrolled(auctionID uint64) string {
	return b.key("auction:%d:enrolled", auctionID)
}

func (b KeyBuilder) AuctionDeposits(auctionID uint64) string {
	return b.key("auction:%d:deposits", auctionID)
}

func (b KeyBuilder) AuctionUserBids(auctionID uint64) string {
	return b.key("auction:%d:user_bids", auctionID)
}

// AuctionUserLatestSeq 是 per-auction 的 hash：bidderID -> 该用户最新写入 ranking
// 时使用的 BidEvent.Seq；用于排行榜防乱序比较，不影响 ranking_member 编码（保持
// hammer.lua 兼容）。
func (b KeyBuilder) AuctionUserLatestSeq(auctionID uint64) string {
	return b.key("auction:%d:user_latest_seq", auctionID)
}

func (b KeyBuilder) AuctionIdempotency(auctionID uint64, requestID string) string {
	return b.key("auction:%d:idem:%s", auctionID, requestID)
}

func (b KeyBuilder) AuctionCloseLock(auctionID uint64) string {
	return b.key("auction:%d:lock:close", auctionID)
}

func (b KeyBuilder) AuctionStream(auctionID uint64) string {
	return b.key("auction:%d:stream", auctionID)
}

func (b KeyBuilder) AuctionSeq(auctionID uint64) string {
	return b.key("auction:%d:seq", auctionID)
}

func (b KeyBuilder) ActiveStreams() string {
	return b.key("auction:active_streams")
}

func (b KeyBuilder) WSInstanceHeartbeat(instanceID string) string {
	return b.key("ws:instance:%s", instanceID)
}

func (b KeyBuilder) WSInstances() string {
	return b.key("ws:instances")
}

func (b KeyBuilder) OnlineInstanceConns(instanceID string) string {
	return b.key("online:instance:%s:conns", instanceID)
}

func (b KeyBuilder) BidRecordDLQ() string {
	return b.key("bid_record:dlq")
}

func (b KeyBuilder) BidRecordReconcileCheckpoint(auctionID uint64) string {
	return b.key("bid_record:reconcile:auction:%d", auctionID)
}

func (b KeyBuilder) OnlineAuction(auctionID uint64) string {
	return b.key("online:auction:%d", auctionID)
}

func (b KeyBuilder) OnlineAuctionUsers(auctionID uint64) string {
	return b.key("online:auction:%d:users", auctionID)
}

func (b KeyBuilder) OnlineAuctionUserConns(auctionID uint64, userID string) string {
	return b.key("online:auction:%d:user:%s:conns", auctionID, userID)
}

func (b KeyBuilder) BidFrequency(userID string, auctionID uint64) string {
	return b.key("risk:freq:bid:%s:%d", userID, auctionID)
}

func (b KeyBuilder) ConfigItem(configKey string) string {
	return b.key("config:item:%s", configKey)
}

// LiveSessionActiveLock 是直播场次活拍互斥锁的 key（值为持锁 auction id）。
func (b KeyBuilder) LiveSessionActiveLock(sessionID uint64) string {
	return b.key("live_session:%d:active", sessionID)
}

// LiveSessionActiveAuction 是直播场次当前活跃拍品的实时状态 key。
func (b KeyBuilder) LiveSessionActiveAuction(sessionID uint64) string {
	return b.key("live_session:%d:active_auction", sessionID)
}

// LiveSessionCounters 存放直播场次计数 HASH（lots_total/lots_sold/...）。
func (b KeyBuilder) LiveSessionCounters(sessionID uint64) string {
	return b.key("live_session:%d:counters", sessionID)
}

// LiveSessionViewerPeak 存放直播场次的观众峰值 STRING（Lua CAS max）。
func (b KeyBuilder) LiveSessionViewerPeak(sessionID uint64) string {
	return b.key("live_session:%d:viewer_peak", sessionID)
}

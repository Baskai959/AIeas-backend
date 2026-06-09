package ws

import "encoding/json"

// 标准 ws 信封类型常量。新增类型时保持小写点分命名。
const (
	// TypeRoomSnapshot 是连接握手后服务器主动下发的房间快照帧（P1-B）：
	// 把 currentPrice / leaderBidderId / endTime / status / seq / serverTime
	// 一次性推给客户端，避免连上后等出价/extend 才能感知房间状态。
	// 若 RT 不可用导致退化到 MySQL 兜底，payload.degraded=true。
	TypeRoomSnapshot = "room.snapshot"
	// TypeRoomOnline 是直播间在线人数更新帧。
	TypeRoomOnline = "room.online"
	// TypeAuctionParticipantUpdated 是拍品参与人数更新帧。
	TypeAuctionParticipantUpdated = "auction.participant_updated"
	// TypeRankingUpdated 是拍品排行榜更新帧。
	TypeRankingUpdated = "ranking.updated"
	// TypeBidResult 是异步出价裁决终态帧，只定向推给本次出价用户。
	TypeBidResult = "bid.result"
	// TypeLiveVoiceBroadcast 是直播语音播报帧，payload 携带已合成的音频内容。
	TypeLiveVoiceBroadcast = "live.voice_broadcast"
	// TypeAIAssistantStatus 是 AI 助手运行状态帧。
	TypeAIAssistantStatus = "ai.assistant.status"
	// TypeAIAssistantPermissionRequest 是 AI 助手执行控制操作前的商家确认帧。
	TypeAIAssistantPermissionRequest = "ai.assistant.permission_request"
	// TypeAIAssistantBroadcast 是 AI 助手准备播报的文本提示帧。
	TypeAIAssistantBroadcast = "ai.assistant.broadcast"
	// TypeAIAssistantSwitch 是 AI 直播助手开关变更帧，用户端可据此切换 AI 直播状态。
	TypeAIAssistantSwitch = "ai.assistant.switch"
	// TypeLiveSessionLotMounted 是直播间拍品上架帧，客户端收到后应刷新直播间拍品列表。
	TypeLiveSessionLotMounted = "live_session.lot_mounted"
	// TypeLiveSessionLotUnmounted 是直播间拍品下架帧，客户端收到后应刷新直播间拍品列表。
	TypeLiveSessionLotUnmounted = "live_session.lot_unmounted"
	// TypeLiveSessionLotChanged 是直播间拍品状态/时间等信息变更帧，客户端收到后应刷新直播间拍品列表。
	TypeLiveSessionLotChanged = "live_session.lot_changed"
	// TypeGatewayDraining 是 ws-gateway 进入排空状态时下发的提示帧，
	// 客户端收到后应主动断开连接并按 payload.retryAfterMs 退避重连。
	TypeGatewayDraining = "gateway.draining"
	// TypeTimeSync 是客户端主动发起的 WS 校时请求。
	TypeTimeSync = "time.sync"
	// TypeTimeSyncResult 是服务端针对 time.sync 的单次校时响应。
	TypeTimeSyncResult = "time.sync.result"
)

type Envelope struct {
	Type          string          `json:"type"`
	RequestID     string          `json:"requestId,omitempty"`
	Seq           int64           `json:"seq,omitempty"`
	Ack           bool            `json:"ack,omitempty"`
	LiveSessionID uint64          `json:"liveSessionId,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"`
}

func AckEnvelope(requestID string, seq int64) Envelope {
	payload, _ := json.Marshal(map[string]interface{}{
		"requestId": requestID,
		"seq":       seq,
	})
	return Envelope{Type: "ack", RequestID: requestID, Seq: seq, Ack: true, Payload: payload}
}

func ErrorEnvelope(requestID, message string) Envelope {
	payload, _ := json.Marshal(map[string]string{"message": message})
	return Envelope{Type: "error", RequestID: requestID, Payload: payload}
}

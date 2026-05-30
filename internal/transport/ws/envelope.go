package ws

import "encoding/json"

// 标准 ws 信封类型常量。新增类型时保持小写点分命名。
const (
	// TypeRoomSnapshot 是连接握手后服务器主动下发的房间快照帧（P1-B）：
	// 把 currentPrice / leaderBidderId / endTime / status / seq / serverTime
	// 一次性推给客户端，避免连上后等出价/extend 才能感知房间状态。
	// 若 RT 不可用导致退化到 MySQL 兜底，payload.degraded=true。
	TypeRoomSnapshot = "room.snapshot"
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

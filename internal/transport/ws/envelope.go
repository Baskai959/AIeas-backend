package ws

import "encoding/json"

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

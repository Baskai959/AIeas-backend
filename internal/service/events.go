package service

import (
	"encoding/json"

	wstransport "aieas_backend/internal/transport/ws"
)

type EventPublisher interface {
	Broadcast(auctionID uint64, env wstransport.Envelope) int
}

func broadcastJSON(publisher EventPublisher, auctionID uint64, eventType string, payload interface{}) {
	broadcastJSONWithSeq(publisher, auctionID, eventType, 0, payload)
}

func broadcastJSONWithSeq(publisher EventPublisher, auctionID uint64, eventType string, seq int64, payload interface{}) {
	if publisher == nil || auctionID == 0 || eventType == "" {
		return
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	publisher.Broadcast(auctionID, wstransport.Envelope{Type: eventType, Seq: seq, Payload: raw})
}

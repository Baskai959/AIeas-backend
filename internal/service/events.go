package service

import (
	"encoding/json"

	wstransport "aieas_backend/internal/transport/ws"
)

type EventPublisher interface {
	Broadcast(auctionID uint64, env wstransport.Envelope) int
}

func broadcastJSON(publisher EventPublisher, auctionID uint64, eventType string, payload interface{}) {
	if publisher == nil || auctionID == 0 || eventType == "" {
		return
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	publisher.Broadcast(auctionID, wstransport.Envelope{Type: eventType, Payload: raw})
}

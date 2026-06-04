package domain

import "time"

type Category struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	IconName string `json:"iconName"`
}

type LiveSessionView struct {
	LiveSession
	MerchantName string                 `json:"merchantName"`
	VideoSource  string                 `json:"videoSource"`
	VideoURL     string                 `json:"videoUrl"`
	DigitalHuman map[string]interface{} `json:"digitalHuman"`
	OnlineCount  int                    `json:"onlineCount"`
}

type MerchantView struct {
	ID                 string           `json:"id"`
	Name               string           `json:"name"`
	AvatarURL          string           `json:"avatarUrl"`
	FollowerCount      int              `json:"followerCount"`
	LiveRoomID         string           `json:"liveRoomId,omitempty"`
	LiveSessionID      uint64           `json:"liveSessionId,omitempty"`
	CurrentLiveSession *LiveSessionView `json:"currentLiveSession,omitempty"`
}

type AuctionParticipationRecord struct {
	ID            string           `json:"id"`
	UserID        string           `json:"userId"`
	Lot           *AuctionLot      `json:"lot"`
	Room          *LiveSessionView `json:"room"`
	Order         *OrderDeal       `json:"order"`
	DepositAmount int64            `json:"depositAmount"`
	DepositStatus DepositStatus    `json:"depositStatus"`
	EnrolledAt    time.Time        `json:"enrolledAt"`
}

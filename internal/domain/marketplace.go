package domain

import "time"

type Category struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	IconName string `json:"iconName"`
}

type LiveSessionView struct {
	LiveSession
	MerchantName          string                 `json:"merchantName"`
	MerchantFollowerCount int                    `json:"merchantFollowerCount"`
	VideoSource           string                 `json:"videoSource"`
	VideoURL              string                 `json:"videoUrl"`
	DigitalHuman          map[string]interface{} `json:"digitalHuman"`
	AIAssistantEnabled    bool                   `json:"aiAssistantEnabled"`
	OnlineCount           int                    `json:"onlineCount"`
}

type MerchantView struct {
	ID                 string           `json:"id"`
	Name               string           `json:"name"`
	AvatarURL          string           `json:"avatarUrl"`
	Location           string           `json:"location"`
	FollowerCount      int              `json:"followerCount"`
	IsFollowed         bool             `json:"isFollowed"`
	LiveRoomID         string           `json:"liveRoomId,omitempty"`
	LiveSessionID      uint64           `json:"liveSessionId,omitempty"`
	CurrentLiveSession *LiveSessionView `json:"currentLiveSession,omitempty"`
}

type MerchantFollow struct {
	BuyerID    string    `json:"buyerId"`
	MerchantID string    `json:"merchantId"`
	CreatedAt  time.Time `json:"createdAt"`
}

type FollowedMerchant struct {
	Merchant   MerchantView `json:"merchant"`
	FollowedAt time.Time    `json:"followedAt"`
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

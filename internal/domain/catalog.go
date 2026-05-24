package domain

import (
	"encoding/json"
	"time"
)

type ItemStatus string

const (
	ItemStatusDraft   ItemStatus = "DRAFT"
	ItemStatusReady   ItemStatus = "READY"
	ItemStatusListed  ItemStatus = "LISTED"
	ItemStatusOffline ItemStatus = "OFFLINE"
)

func (s ItemStatus) Valid() bool {
	switch s {
	case ItemStatusDraft, ItemStatusReady, ItemStatusListed, ItemStatusOffline:
		return true
	default:
		return false
	}
}

type ConditionGrade string

const (
	ConditionNew     ConditionGrade = "NEW"
	ConditionLikeNew ConditionGrade = "LIKE_NEW"
	ConditionGood    ConditionGrade = "GOOD"
	ConditionFair    ConditionGrade = "FAIR"
)

func (g ConditionGrade) Valid() bool {
	switch g {
	case ConditionNew, ConditionLikeNew, ConditionGood, ConditionFair:
		return true
	default:
		return false
	}
}

type Item struct {
	ID             uint64          `json:"id"`
	SellerID       string          `json:"sellerId"`
	Title          string          `json:"title"`
	Category       string          `json:"category"`
	Brand          string          `json:"brand,omitempty"`
	ConditionGrade ConditionGrade  `json:"conditionGrade"`
	Images         json.RawMessage `json:"images"`
	Description    string          `json:"description,omitempty"`
	Status         ItemStatus      `json:"status"`
	CreatedAt      time.Time       `json:"createdAt"`
	UpdatedAt      time.Time       `json:"updatedAt"`
}

type ItemFilter struct {
	SellerID string
	Status   ItemStatus
	Category string
	Limit    int
	Offset   int
}

type ItemPatch struct {
	Title          *string
	Category       *string
	Brand          *string
	ConditionGrade *ConditionGrade
	Images         *json.RawMessage
	Description    *string
	Status         *ItemStatus
}

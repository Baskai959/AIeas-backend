package service

import (
	"strings"

	"aieas_backend/internal/domain"
)

func canAccessSellerOwned(actorID string, actorRole domain.Role, sellerID string) bool {
	sellerID = strings.TrimSpace(sellerID)
	if sellerID == "" {
		return false
	}
	if actorRole == domain.RoleAdmin {
		return true
	}
	return actorRole == domain.RoleMerchant && strings.TrimSpace(actorID) == sellerID
}

func canReadLiveSession(actorID string, actorRole domain.Role, session domain.LiveSession) bool {
	if canAccessSellerOwned(actorID, actorRole, session.MerchantID) {
		return true
	}
	return actorRole == domain.RoleBuyer && session.Status == domain.LiveSessionStatusLive
}

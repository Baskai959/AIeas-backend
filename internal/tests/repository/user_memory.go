package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"sync"
	"time"

	"aieas_backend/internal/domain"
	marketplaceports "aieas_backend/internal/modules/marketplace/ports"
)

type UserRepository interface {
	FindByAccountAndRole(account string, role domain.Role) (domain.User, error)
	FindByID(id string) (domain.User, error)
	List(filter domain.UserFilter) ([]domain.User, error)
	Update(user *domain.User) error
}

type MemoryUserRepository struct {
	mu        sync.RWMutex
	byID      map[string]domain.User
	byAccount map[string]string
	follows   map[string]domain.MerchantFollow
}

func NewSeedUserRepository() *MemoryUserRepository {
	users := []domain.User{
		{ID: "u_1001", Account: "buyer001", Nickname: "竞拍用户001", Role: domain.RoleBuyer, Status: domain.UserStatusActive, AIPermission: domain.MerchantAIPermissionAsk, PasswordHash: HashPassword("Passw0rd!")},
		{ID: "u_2001", Account: "merchant001", Nickname: "商家001", Role: domain.RoleMerchant, Status: domain.UserStatusActive, AIPermission: domain.MerchantAIPermissionAsk, PasswordHash: HashPassword("Passw0rd!")},
		{ID: "u_9001", Account: "admin001", Nickname: "管理员001", Role: domain.RoleAdmin, Status: domain.UserStatusActive, AIPermission: domain.MerchantAIPermissionAsk, PasswordHash: HashPassword("AdminPassw0rd!")},
		{ID: "u_1002", Account: "disabled001", Nickname: "停用用户001", Role: domain.RoleBuyer, Status: domain.UserStatusDisabled, AIPermission: domain.MerchantAIPermissionAsk, PasswordHash: HashPassword("Passw0rd!")},
	}
	repo := &MemoryUserRepository{byID: make(map[string]domain.User, len(users)), byAccount: make(map[string]string, len(users)), follows: make(map[string]domain.MerchantFollow)}
	for _, user := range users {
		repo.byID[user.ID] = user
		repo.byAccount[accountRoleKey(user.Account, user.Role)] = user.ID
	}
	return repo
}

func (r *MemoryUserRepository) FindByAccountAndRole(account string, role domain.Role) (domain.User, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	id, ok := r.byAccount[accountRoleKey(account, role)]
	if !ok {
		return domain.User{}, domain.ErrUserNotFound
	}
	return r.byID[id], nil
}

func (r *MemoryUserRepository) FindByID(id string) (domain.User, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	user, ok := r.findUserLocked(id)
	if !ok {
		return domain.User{}, domain.ErrUserNotFound
	}
	return user, nil
}

func (r *MemoryUserRepository) List(filter domain.UserFilter) ([]domain.User, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	users := make([]domain.User, 0, len(r.byID))
	keyword := strings.ToLower(strings.TrimSpace(filter.Keyword))
	for _, user := range r.byID {
		if filter.Role.Valid() && user.Role != filter.Role {
			continue
		}
		if filter.Status != "" && user.Status != filter.Status {
			continue
		}
		if keyword != "" {
			haystack := strings.ToLower(user.ID + " " + user.Account + " " + user.Nickname)
			if !strings.Contains(haystack, keyword) {
				continue
			}
		}
		users = append(users, user)
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}
	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 20
	}
	if filter.Offset >= len(users) {
		return []domain.User{}, nil
	}
	end := filter.Offset + filter.Limit
	if end > len(users) {
		end = len(users)
	}
	return users[filter.Offset:end], nil
}

func (r *MemoryUserRepository) Update(user *domain.User) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	existing, ok := r.findUserLocked(user.ID)
	if !ok {
		return domain.ErrUserNotFound
	}
	user.ID = existing.ID
	if user.Account == "" {
		user.Account = existing.Account
	}
	if user.Nickname == "" {
		user.Nickname = existing.Nickname
	}
	if user.AvatarURL == "" {
		user.AvatarURL = existing.AvatarURL
	}
	if user.Location == "" {
		user.Location = existing.Location
	}
	if user.Role == "" {
		user.Role = existing.Role
	}
	if user.PasswordHash == "" {
		user.PasswordHash = existing.PasswordHash
	}
	user.AIPermission = domain.NormalizeMerchantAIPermission(user.AIPermission)
	r.byID[user.ID] = *user
	r.byAccount[accountRoleKey(user.Account, user.Role)] = user.ID
	return nil
}

func (r *MemoryUserRepository) FollowMerchant(_ context.Context, buyerID, merchantID string) (domain.MerchantFollow, error) {
	buyerID = strings.TrimSpace(buyerID)
	merchantID = strings.TrimSpace(merchantID)
	if buyerID == "" || merchantID == "" {
		return domain.MerchantFollow{}, domain.ErrInvalidArgument
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.follows == nil {
		r.follows = make(map[string]domain.MerchantFollow)
	}
	buyerID = r.canonicalUserIDLocked(buyerID)
	merchantID = r.canonicalUserIDLocked(merchantID)
	key := merchantFollowKey(buyerID, merchantID)
	if follow, ok := r.follows[key]; ok {
		return follow, nil
	}
	follow := domain.MerchantFollow{BuyerID: buyerID, MerchantID: merchantID, CreatedAt: time.Now().UTC()}
	r.follows[key] = follow
	return follow, nil
}

func (r *MemoryUserRepository) UnfollowMerchant(_ context.Context, buyerID, merchantID string) error {
	buyerID = strings.TrimSpace(buyerID)
	merchantID = strings.TrimSpace(merchantID)
	if buyerID == "" || merchantID == "" {
		return domain.ErrInvalidArgument
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.follows, merchantFollowKey(r.canonicalUserIDLocked(buyerID), r.canonicalUserIDLocked(merchantID)))
	return nil
}

func (r *MemoryUserRepository) IsFollowingMerchant(_ context.Context, buyerID, merchantID string) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.follows[merchantFollowKey(r.canonicalUserIDLocked(buyerID), r.canonicalUserIDLocked(merchantID))]
	return ok, nil
}

func (r *MemoryUserRepository) CountMerchantFollowers(_ context.Context, merchantID string) (int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	merchantID = normalizeUserIDValue(r.canonicalUserIDLocked(merchantID))
	count := 0
	for _, follow := range r.follows {
		if normalizeUserIDValue(follow.MerchantID) == merchantID {
			count++
		}
	}
	return count, nil
}

func (r *MemoryUserRepository) ListMerchantFollowsByBuyer(_ context.Context, buyerID string, limit, offset int) ([]domain.MerchantFollow, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	buyerID = normalizeUserIDValue(r.canonicalUserIDLocked(buyerID))
	follows := make([]domain.MerchantFollow, 0)
	for _, follow := range r.follows {
		if normalizeUserIDValue(follow.BuyerID) == buyerID {
			follows = append(follows, follow)
		}
	}
	sort.SliceStable(follows, func(i, j int) bool {
		if follows[i].CreatedAt.Equal(follows[j].CreatedAt) {
			return follows[i].MerchantID > follows[j].MerchantID
		}
		return follows[i].CreatedAt.After(follows[j].CreatedAt)
	})
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset >= len(follows) {
		return []domain.MerchantFollow{}, nil
	}
	end := offset + limit
	if end > len(follows) {
		end = len(follows)
	}
	return append([]domain.MerchantFollow(nil), follows[offset:end]...), nil
}

func (r *MemoryUserRepository) CountMerchantFollowsByBuyer(_ context.Context, buyerID string) (int64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	buyerID = normalizeUserIDValue(r.canonicalUserIDLocked(buyerID))
	var count int64
	for _, follow := range r.follows {
		if normalizeUserIDValue(follow.BuyerID) == buyerID {
			count++
		}
	}
	return count, nil
}

func HashPassword(password string) string {
	sum := sha256.Sum256([]byte("aieas-auth-seed:" + password))
	return hex.EncodeToString(sum[:])
}

func accountRoleKey(account string, role domain.Role) string {
	return account + ":" + string(role)
}

func (r *MemoryUserRepository) findUserLocked(id string) (domain.User, bool) {
	if user, ok := r.byID[id]; ok {
		return user, true
	}
	normalized := normalizeUserIDValue(id)
	for _, user := range r.byID {
		if normalizeUserIDValue(user.ID) == normalized {
			return user, true
		}
	}
	return domain.User{}, false
}

func (r *MemoryUserRepository) canonicalUserIDLocked(id string) string {
	if user, ok := r.findUserLocked(id); ok {
		return user.ID
	}
	return strings.TrimSpace(id)
}

func merchantFollowKey(buyerID, merchantID string) string {
	return normalizeUserIDValue(buyerID) + ":" + normalizeUserIDValue(merchantID)
}

func normalizeUserIDValue(id string) string {
	id = strings.TrimSpace(id)
	for _, prefix := range []string{"u_", "U_"} {
		if strings.HasPrefix(id, prefix) {
			return strings.TrimPrefix(id, prefix)
		}
	}
	return id
}

var _ marketplaceports.MerchantFollowRepository = (*MemoryUserRepository)(nil)

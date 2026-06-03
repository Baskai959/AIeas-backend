package repository

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"

	"aieas_backend/internal/domain"
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
}

func NewSeedUserRepository() *MemoryUserRepository {
	users := []domain.User{
		{ID: "u_1001", Account: "buyer001", Nickname: "竞拍用户001", Role: domain.RoleBuyer, Status: domain.UserStatusActive, AIPermission: domain.MerchantAIPermissionAsk, PasswordHash: HashPassword("Passw0rd!")},
		{ID: "u_2001", Account: "merchant001", Nickname: "商家001", Role: domain.RoleMerchant, Status: domain.UserStatusActive, AIPermission: domain.MerchantAIPermissionAsk, PasswordHash: HashPassword("Passw0rd!")},
		{ID: "u_9001", Account: "admin001", Nickname: "管理员001", Role: domain.RoleAdmin, Status: domain.UserStatusActive, AIPermission: domain.MerchantAIPermissionAsk, PasswordHash: HashPassword("AdminPassw0rd!")},
		{ID: "u_1002", Account: "disabled001", Nickname: "停用用户001", Role: domain.RoleBuyer, Status: domain.UserStatusDisabled, AIPermission: domain.MerchantAIPermissionAsk, PasswordHash: HashPassword("Passw0rd!")},
	}
	repo := &MemoryUserRepository{byID: make(map[string]domain.User, len(users)), byAccount: make(map[string]string, len(users))}
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
	user, ok := r.byID[id]
	if !ok {
		return domain.User{}, domain.ErrUserNotFound
	}
	return user, nil
}

func (r *MemoryUserRepository) List(filter domain.UserFilter) ([]domain.User, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	users := make([]domain.User, 0, len(r.byID))
	for _, user := range r.byID {
		if filter.Role.Valid() && user.Role != filter.Role {
			continue
		}
		if filter.Status != "" && user.Status != filter.Status {
			continue
		}
		if filter.Keyword != "" && user.ID != filter.Keyword && user.Account != filter.Keyword && user.Nickname != filter.Keyword {
			continue
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
	existing, ok := r.byID[user.ID]
	if !ok {
		return domain.ErrUserNotFound
	}
	if user.Account == "" {
		user.Account = existing.Account
	}
	if user.Nickname == "" {
		user.Nickname = existing.Nickname
	}
	if user.AvatarURL == "" {
		user.AvatarURL = existing.AvatarURL
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

func HashPassword(password string) string {
	sum := sha256.Sum256([]byte("aieas-auth-seed:" + password))
	return hex.EncodeToString(sum[:])
}

func accountRoleKey(account string, role domain.Role) string {
	return account + ":" + string(role)
}

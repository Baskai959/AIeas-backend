package domain

import "errors"

type Role string

const (
	RoleBuyer    Role = "buyer"
	RoleMerchant Role = "merchant"
	RoleAdmin    Role = "admin"
)

func (r Role) Valid() bool {
	switch r {
	case RoleBuyer, RoleMerchant, RoleAdmin:
		return true
	default:
		return false
	}
}

type UserStatus string

const (
	UserStatusActive   UserStatus = "ACTIVE"
	UserStatusDisabled UserStatus = "DISABLED"
)

type MerchantAIPermission string

const (
	MerchantAIPermissionAsk   MerchantAIPermission = "ASK"
	MerchantAIPermissionAllow MerchantAIPermission = "ALLOW"
	MerchantAIPermissionDeny  MerchantAIPermission = "DENY"
)

func (p MerchantAIPermission) Valid() bool {
	switch p {
	case MerchantAIPermissionAsk, MerchantAIPermissionAllow, MerchantAIPermissionDeny:
		return true
	default:
		return false
	}
}

func NormalizeMerchantAIPermission(permission MerchantAIPermission) MerchantAIPermission {
	if permission.Valid() {
		return permission
	}
	return MerchantAIPermissionAsk
}

type User struct {
	ID           string
	Account      string
	Nickname     string
	AvatarURL    string
	Role         Role
	Status       UserStatus
	AIPermission MerchantAIPermission
	PasswordHash string
}

type UserFilter struct {
	Role    Role
	Status  UserStatus
	Keyword string
	Limit   int
	Offset  int
}

type SafeUser struct {
	ID           string               `json:"id"`
	Nickname     string               `json:"nickname"`
	AvatarURL    string               `json:"avatarUrl,omitempty"`
	Role         Role                 `json:"role"`
	Status       UserStatus           `json:"status,omitempty"`
	AIPermission MerchantAIPermission `json:"aiPermission,omitempty"`
}

func (u User) Safe() SafeUser {
	return SafeUser{ID: u.ID, Nickname: u.Nickname, AvatarURL: u.AvatarURL, Role: u.Role, Status: u.Status, AIPermission: NormalizeMerchantAIPermission(u.AIPermission)}
}

var (
	ErrUserNotFound    = errors.New("user not found")
	ErrAccountDisabled = errors.New("account disabled")
	ErrInvalidPassword = errors.New("invalid password")
	ErrTokenInvalid    = errors.New("token invalid")
	ErrTokenMissing    = errors.New("token missing")
	ErrForbidden       = errors.New("forbidden")
	ErrInvalidArgument = errors.New("invalid argument")
	ErrNotFound        = errors.New("not found")
	ErrConflict        = errors.New("conflict")
	ErrInvalidState    = errors.New("invalid state")
	ErrIdempotencyKey  = errors.New("idempotency key required")
	// ErrOptimisticConflict 表示行级乐观锁版本号 CAS 失败，调用方可重试或视为冲突。
	ErrOptimisticConflict = errors.New("optimistic conflict")
)

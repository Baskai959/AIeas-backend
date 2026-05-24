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

type User struct {
	ID           string
	Account      string
	Nickname     string
	Role         Role
	Status       UserStatus
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
	ID       string     `json:"id"`
	Nickname string     `json:"nickname"`
	Role     Role       `json:"role"`
	Status   UserStatus `json:"status,omitempty"`
}

func (u User) Safe() SafeUser {
	return SafeUser{ID: u.ID, Nickname: u.Nickname, Role: u.Role, Status: u.Status}
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
)

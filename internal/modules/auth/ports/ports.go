package ports

import (
	"time"

	"aieas_backend/internal/domain"
	jwtpkg "aieas_backend/pkg/jwt"
)

// UserRepository 是 auth 模块读取/更新用户资料的持久化端口。
type UserRepository interface {
	FindByAccountAndRole(account string, role domain.Role) (domain.User, error)
	FindByID(id string) (domain.User, error)
	Update(user *domain.User) error
}

// TokenManager 是 auth 模块签发/解析访问令牌的端口。
type TokenManager interface {
	Sign(userID, role string) (string, jwtpkg.Claims, error)
	Parse(token string) (jwtpkg.Claims, error)
	TTL() time.Duration
}

// PasswordHasher 是 auth 模块校验密码摘要的端口。
type PasswordHasher interface {
	Matches(password, passwordHash string) bool
}

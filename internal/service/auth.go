package service

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
	jwtpkg "aieas_backend/pkg/jwt"
)

type AuthService struct {
	users         repository.UserRepository
	jwt           *jwtpkg.Manager
	mu            sync.RWMutex
	refreshTokens map[string]refreshSession
	revokedTokens map[string]time.Time
}

type refreshSession struct {
	UserID    string
	ExpiresAt time.Time
}

type LoginResult struct {
	AccessToken  string          `json:"accessToken"`
	RefreshToken string          `json:"refreshToken"`
	ExpiresIn    int64           `json:"expiresIn"`
	User         domain.SafeUser `json:"user"`
}

type RefreshResult struct {
	AccessToken string `json:"accessToken"`
	ExpiresIn   int64  `json:"expiresIn"`
}

func NewAuthService(users repository.UserRepository, jwt *jwtpkg.Manager) *AuthService {
	return &AuthService{users: users, jwt: jwt, refreshTokens: make(map[string]refreshSession), revokedTokens: make(map[string]time.Time)}
}

func (s *AuthService) Login(account, password string, role domain.Role) (LoginResult, error) {
	if account == "" || password == "" || !role.Valid() {
		return LoginResult{}, domain.ErrInvalidPassword
	}
	user, err := s.users.FindByAccountAndRole(account, role)
	if err != nil {
		return LoginResult{}, domain.ErrInvalidPassword
	}
	if user.Status == domain.UserStatusDisabled {
		return LoginResult{}, domain.ErrAccountDisabled
	}
	if user.PasswordHash != repository.HashPassword(password) {
		return LoginResult{}, domain.ErrInvalidPassword
	}
	accessToken, _, err := s.jwt.Sign(user.ID, string(user.Role))
	if err != nil {
		return LoginResult{}, err
	}
	refreshToken := randomToken("rft")
	s.mu.Lock()
	s.refreshTokens[refreshToken] = refreshSession{UserID: user.ID, ExpiresAt: time.Now().Add(7 * 24 * time.Hour)}
	s.mu.Unlock()
	return LoginResult{AccessToken: accessToken, RefreshToken: refreshToken, ExpiresIn: int64(s.jwt.TTL().Seconds()), User: safeLoginUser(user)}, nil
}

func (s *AuthService) Refresh(refreshToken string) (RefreshResult, error) {
	if refreshToken == "" {
		return RefreshResult{}, domain.ErrTokenInvalid
	}
	s.mu.RLock()
	session, ok := s.refreshTokens[refreshToken]
	s.mu.RUnlock()
	if !ok || time.Now().After(session.ExpiresAt) {
		return RefreshResult{}, domain.ErrTokenInvalid
	}
	user, err := s.users.FindByID(session.UserID)
	if err != nil {
		return RefreshResult{}, domain.ErrTokenInvalid
	}
	if user.Status == domain.UserStatusDisabled {
		return RefreshResult{}, domain.ErrAccountDisabled
	}
	accessToken, _, err := s.jwt.Sign(user.ID, string(user.Role))
	if err != nil {
		return RefreshResult{}, err
	}
	return RefreshResult{AccessToken: accessToken, ExpiresIn: int64(s.jwt.TTL().Seconds())}, nil
}

func (s *AuthService) Logout(accessToken, refreshToken string, expiresAt int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if accessToken != "" {
		s.revokedTokens[accessToken] = time.Unix(expiresAt, 0)
	}
	if refreshToken != "" {
		delete(s.refreshTokens, refreshToken)
	}
}

func (s *AuthService) Me(userID string) (domain.SafeUser, error) {
	user, err := s.users.FindByID(userID)
	if err != nil {
		return domain.SafeUser{}, domain.ErrTokenInvalid
	}
	if user.Status == domain.UserStatusDisabled {
		return domain.SafeUser{}, domain.ErrAccountDisabled
	}
	return user.Safe(), nil
}

func (s *AuthService) ParseAccessToken(token string) (jwtpkg.Claims, error) {
	claims, err := s.jwt.Parse(token)
	if err != nil {
		return jwtpkg.Claims{}, domain.ErrTokenInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for revokedToken, expiresAt := range s.revokedTokens {
		if time.Now().After(expiresAt) {
			delete(s.revokedTokens, revokedToken)
		}
	}
	if _, revoked := s.revokedTokens[token]; revoked {
		return jwtpkg.Claims{}, domain.ErrTokenInvalid
	}
	return claims, nil
}

func (s *AuthService) MustAdminLogin(account, password string) (LoginResult, error) {
	return s.Login(account, password, domain.RoleAdmin)
}

func safeLoginUser(user domain.User) domain.SafeUser {
	safe := user.Safe()
	safe.Status = ""
	return safe
}

func HTTPStatusAndCode(err error) (int, int, string) {
	switch {
	case errors.Is(err, domain.ErrTokenMissing):
		return 401, 10001, "缺少访问令牌"
	case errors.Is(err, domain.ErrTokenInvalid):
		return 401, 10002, "访问令牌无效或已过期"
	case errors.Is(err, domain.ErrForbidden):
		return 403, 10003, "无访问权限"
	case errors.Is(err, domain.ErrAccountDisabled):
		return 403, 10005, "账号已停用"
	case errors.Is(err, domain.ErrInvalidPassword):
		return 401, 10004, "登录失败"
	case errors.Is(err, domain.ErrInvalidArgument):
		return 400, 20001, "参数不合法"
	case errors.Is(err, domain.ErrUserNotFound):
		return 404, 20004, "资源不存在"
	case errors.Is(err, domain.ErrNotFound):
		return 404, 20004, "资源不存在"
	case errors.Is(err, domain.ErrConflict):
		return 409, 20009, "资源冲突"
	case errors.Is(err, domain.ErrInvalidState):
		return 409, 20010, "状态不允许"
	case errors.Is(err, domain.ErrIdempotencyKey):
		return 400, 20011, "缺少幂等键"
	default:
		return 500, 90001, "系统内部错误"
	}
}

func randomToken(prefix string) string {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(b)
}

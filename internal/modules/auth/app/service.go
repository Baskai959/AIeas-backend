package app

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"aieas_backend/internal/domain"
	authports "aieas_backend/internal/modules/auth/ports"
	jwtpkg "aieas_backend/pkg/jwt"
)

type AuthService struct {
	users         authports.UserRepository
	jwt           authports.TokenManager
	hasher        authports.PasswordHasher
	mu            sync.RWMutex
	refreshTokens map[string]refreshSession
	revokedTokens map[string]time.Time
}

type refreshSession struct {
	UserID    string
	ExpiresAt time.Time
}

func NewAuthService(users authports.UserRepository, jwt authports.TokenManager, hasher authports.PasswordHasher) *AuthService {
	return &AuthService{
		users:         users,
		jwt:           jwt,
		hasher:        hasher,
		refreshTokens: make(map[string]refreshSession),
		revokedTokens: make(map[string]time.Time),
	}
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
	if !s.passwordMatches(password, user.PasswordHash) {
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
	return LoginResult{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		ExpiresIn:    int64(s.jwt.TTL().Seconds()),
		User:         safeLoginUser(user),
	}, nil
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

func (s *AuthService) UpdateProfile(in UpdateProfileInput) (domain.SafeUser, error) {
	userID := strings.TrimSpace(in.UserID)
	if userID == "" || (in.Nickname == nil && in.Location == nil) {
		return domain.SafeUser{}, domain.ErrInvalidArgument
	}
	user, err := s.users.FindByID(userID)
	if err != nil {
		return domain.SafeUser{}, err
	}
	if user.Status == domain.UserStatusDisabled {
		return domain.SafeUser{}, domain.ErrAccountDisabled
	}
	if in.Nickname != nil {
		nickname := strings.TrimSpace(*in.Nickname)
		if nickname == "" || utf8.RuneCountInString(nickname) > 64 {
			return domain.SafeUser{}, domain.ErrInvalidArgument
		}
		user.Nickname = nickname
	}
	if in.Location != nil {
		location := strings.TrimSpace(*in.Location)
		if utf8.RuneCountInString(location) > 64 {
			return domain.SafeUser{}, domain.ErrInvalidArgument
		}
		user.Location = location
	}
	if err := s.users.Update(&user); err != nil {
		return domain.SafeUser{}, err
	}
	return user.Safe(), nil
}

func (s *AuthService) UpdateAvatar(userID, avatarURL string) (domain.SafeUser, error) {
	userID = strings.TrimSpace(userID)
	avatarURL = strings.TrimSpace(avatarURL)
	if userID == "" || avatarURL == "" || len(avatarURL) > 512 {
		return domain.SafeUser{}, domain.ErrInvalidArgument
	}
	user, err := s.users.FindByID(userID)
	if err != nil {
		return domain.SafeUser{}, err
	}
	if user.Status == domain.UserStatusDisabled {
		return domain.SafeUser{}, domain.ErrAccountDisabled
	}
	user.AvatarURL = avatarURL
	if err := s.users.Update(&user); err != nil {
		return domain.SafeUser{}, err
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

func (s *AuthService) passwordMatches(password, passwordHash string) bool {
	if s == nil || s.hasher == nil {
		return false
	}
	return s.hasher.Matches(password, passwordHash)
}

func randomToken(prefix string) string {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(b)
}

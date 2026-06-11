package app

import (
	"aieas_backend/internal/domain"
	jwtpkg "aieas_backend/pkg/jwt"
)

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

type UpdateProfileInput struct {
	UserID   string
	Nickname *string
	Location *string
}

// AuthUseCase 暴露认证模块对 HTTP transport 的最小应用边界。
type AuthUseCase interface {
	Login(account, password string, role domain.Role) (LoginResult, error)
	MustAdminLogin(account, password string) (LoginResult, error)
	Me(userID string) (domain.SafeUser, error)
	UpdateProfile(in UpdateProfileInput) (domain.SafeUser, error)
	UpdateAvatar(userID, avatarURL string) (domain.SafeUser, error)
	Refresh(refreshToken string) (RefreshResult, error)
	Logout(accessToken, refreshToken string, expiresAt int64)
	ParseAccessToken(token string) (jwtpkg.Claims, error)
}

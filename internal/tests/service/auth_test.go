package service

import (
	"errors"
	"strings"
	"testing"
	"time"

	"aieas_backend/internal/domain"
	authapp "aieas_backend/internal/modules/auth/app"
	authports "aieas_backend/internal/modules/auth/ports"
	"aieas_backend/internal/tests/repository"
	jwtpkg "aieas_backend/pkg/jwt"
)

func TestAuthServiceUpdateProfile(t *testing.T) {
	tests := []struct {
		name      string
		userID    string
		nickname  string
		location  string
		wantName  string
		wantPlace string
		wantError error
	}{
		{name: "ok", userID: "u_1001", nickname: "  新昵称001  ", location: "  杭州  ", wantName: "新昵称001", wantPlace: "杭州"},
		{name: "empty nickname", userID: "u_1001", nickname: "  ", wantError: domain.ErrInvalidArgument},
		{name: "too long nickname", userID: "u_1001", nickname: strings.Repeat("长", 65), wantError: domain.ErrInvalidArgument},
		{name: "disabled user", userID: "u_1002", nickname: "停用账号新昵称", wantError: domain.ErrAccountDisabled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			users := repository.NewSeedUserRepository()
			svc := authapp.NewAuthService(users, jwtpkg.NewManager("test-secret", time.Hour), authTestPasswordHasher{})
			got, err := svc.UpdateProfile(UpdateProfileInput{UserID: tt.userID, Nickname: &tt.nickname, Location: &tt.location})
			if tt.wantError != nil {
				if !errors.Is(err, tt.wantError) {
					t.Fatalf("expected error %v, got %v", tt.wantError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("update profile: %v", err)
			}
			if got.Nickname != tt.wantName {
				t.Fatalf("expected nickname %q, got %+v", tt.wantName, got)
			}
			if got.Location != tt.wantPlace {
				t.Fatalf("expected location %q, got %+v", tt.wantPlace, got)
			}
			stored, err := users.FindByID(tt.userID)
			if err != nil {
				t.Fatalf("find stored user: %v", err)
			}
			if stored.Nickname != tt.wantName || stored.Location != tt.wantPlace {
				t.Fatalf("expected stored profile name=%q location=%q, got %+v", tt.wantName, tt.wantPlace, stored)
			}
		})
	}
}

func TestAuthServiceUpdateAvatar(t *testing.T) {
	users := repository.NewSeedUserRepository()
	svc := authapp.NewAuthService(users, jwtpkg.NewManager("test-secret", time.Hour), authTestPasswordHasher{})

	got, err := svc.UpdateAvatar("u_1001", " /api/v1/images/avatar.jpg ")
	if err != nil {
		t.Fatalf("update avatar: %v", err)
	}
	if got.AvatarURL != "/api/v1/images/avatar.jpg" {
		t.Fatalf("unexpected avatar payload: %+v", got)
	}
	stored, err := users.FindByID("u_1001")
	if err != nil {
		t.Fatalf("find stored user: %v", err)
	}
	if stored.AvatarURL != "/api/v1/images/avatar.jpg" {
		t.Fatalf("expected stored avatar URL, got %+v", stored)
	}

	if _, err := svc.UpdateAvatar("u_1001", ""); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected empty avatar invalid argument, got %v", err)
	}
	if _, err := svc.UpdateAvatar("u_1002", "/api/v1/images/disabled.jpg"); !errors.Is(err, domain.ErrAccountDisabled) {
		t.Fatalf("expected disabled user error, got %v", err)
	}
}

type authTestPasswordHasher struct{}

func (authTestPasswordHasher) Matches(password, passwordHash string) bool {
	return repository.HashPassword(password) == passwordHash
}

var _ authports.PasswordHasher = authTestPasswordHasher{}

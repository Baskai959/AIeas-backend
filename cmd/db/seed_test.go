package main

import (
	"strings"
	"testing"

	"aieas_backend/internal/repository"
)

func TestDevUsersAlignWithMemorySeedIDs(t *testing.T) {
	want := map[string]devUser{
		"buyer001": {
			ID:           1001,
			Account:      "buyer001",
			PasswordHash: repository.HashPassword("Passw0rd!"),
			Role:         "buyer",
			Status:       "ACTIVE",
		},
		"merchant001": {
			ID:           2001,
			Account:      "merchant001",
			PasswordHash: repository.HashPassword("Passw0rd!"),
			Role:         "merchant",
			Status:       "ACTIVE",
		},
		"admin001": {
			ID:           9001,
			Account:      "admin001",
			PasswordHash: repository.HashPassword("AdminPassw0rd!"),
			Role:         "admin",
			Status:       "ACTIVE",
		},
		"disabled001": {
			ID:           1002,
			Account:      "disabled001",
			PasswordHash: repository.HashPassword("Passw0rd!"),
			Role:         "buyer",
			Status:       "DISABLED",
		},
	}

	if len(devUsers) != len(want) {
		t.Fatalf("devUsers length = %d, want %d", len(devUsers), len(want))
	}
	for _, got := range devUsers {
		expected, ok := want[got.Account]
		if !ok {
			t.Fatalf("unexpected dev user account %q", got.Account)
		}
		if got.ID != expected.ID || got.PasswordHash != expected.PasswordHash || got.Role != expected.Role || got.Status != expected.Status {
			t.Fatalf("dev user %s = %+v, want ID=%d role=%s status=%s", got.Account, got, expected.ID, expected.Role, expected.Status)
		}
	}
}

func TestUpsertDevUserSQLCanonicalizesSeedRows(t *testing.T) {
	for _, clause := range []string{
		"`id` = VALUES(`id`)",
		"`account` = VALUES(`account`)",
		"`password_hash` = VALUES(`password_hash`)",
		"`status` = VALUES(`status`)",
	} {
		if !strings.Contains(upsertDevUserSQL, clause) {
			t.Fatalf("upsert SQL missing clause %q", clause)
		}
	}
}

package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"aieas_backend/internal/config"
)

func TestMigrationSmoke(t *testing.T) {
	if os.Getenv("AIEAS_MIGRATION_SMOKE") != "1" {
		t.Skip("set AIEAS_MIGRATION_SMOKE=1 with MYSQL_DSN pointing at an empty test database")
	}
	dsn := os.Getenv("MYSQL_DSN")
	if strings.TrimSpace(dsn) == "" {
		t.Fatal("MYSQL_DSN is required for migration smoke test")
	}

	cfg := config.Default()
	cfg.MySQL.DSN = dsn

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	runner := dbRunner{}
	for _, direction := range []string{"up", "status", "down"} {
		if err := runner.Migrate(ctx, cfg, direction); err != nil {
			t.Fatalf("migrate %s: %v", direction, err)
		}
	}
}

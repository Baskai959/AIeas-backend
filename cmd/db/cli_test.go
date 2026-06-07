package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"aieas_backend/internal/config"
)

func TestParseArgs(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		want      cliCommand
		wantError bool
	}{
		{
			name: "migrate up with config",
			args: []string{"-config", "configs/config.yaml", "migrate", "up"},
			want: cliCommand{configPath: "configs/config.yaml", name: "migrate", migrateDirection: "up"},
		},
		{
			name: "migrate down",
			args: []string{"migrate", "down"},
			want: cliCommand{configPath: config.DefaultPath, name: "migrate", migrateDirection: "down"},
		},
		{
			name: "migrate status",
			args: []string{"migrate", "status"},
			want: cliCommand{configPath: config.DefaultPath, name: "migrate", migrateDirection: "status"},
		},
		{
			name: "seed dev",
			args: []string{"-config", "custom.yaml", "seed-dev"},
			want: cliCommand{configPath: "custom.yaml", name: "seed-dev"},
		},
		{
			name:      "missing command",
			args:      nil,
			wantError: true,
		},
		{
			name:      "missing migrate direction",
			args:      []string{"migrate"},
			wantError: true,
		},
		{
			name:      "invalid migrate direction",
			args:      []string{"migrate", "redo"},
			wantError: true,
		},
		{
			name:      "seed dev extra arg",
			args:      []string{"seed-dev", "extra"},
			wantError: true,
		},
		{
			name:      "unknown command",
			args:      []string{"unknown"},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseArgs(tt.args)
			if tt.wantError {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseArgs returned error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("parseArgs = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestRunDispatchesMigrateWithFileConfig(t *testing.T) {
	configPath := writeTestConfig(t)
	t.Setenv("MYSQL_DSN", "env-user@tcp(localhost:3306)/envdb")

	runner := &fakeRunner{}
	var stdout bytes.Buffer
	app := &cli{
		loadConfig: config.Load,
		runner:     runner,
		stdout:     &stdout,
		stderr:     &bytes.Buffer{},
	}

	if err := app.run(context.Background(), []string{"-config", configPath, "migrate", "up"}); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	if runner.migrateCalls != 1 {
		t.Fatalf("migrate calls = %d, want 1", runner.migrateCalls)
	}
	if runner.seedCalls != 0 {
		t.Fatalf("seed calls = %d, want 0", runner.seedCalls)
	}
	if runner.direction != "up" {
		t.Fatalf("direction = %q, want up", runner.direction)
	}
	if runner.dsn != "file-user@tcp(localhost:3306)/filedb" {
		t.Fatal("dsn did not come from file config")
	}
	if got := stdout.String(); got != "migrate up complete\n" {
		t.Fatalf("stdout = %q", got)
	}
}

func TestRunDispatchesSeedDev(t *testing.T) {
	configPath := writeTestConfig(t)
	runner := &fakeRunner{}
	var stdout bytes.Buffer
	app := &cli{
		loadConfig: config.Load,
		runner:     runner,
		stdout:     &stdout,
		stderr:     &bytes.Buffer{},
	}

	if err := app.run(context.Background(), []string{"-config", configPath, "seed-dev"}); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	if runner.seedCalls != 1 {
		t.Fatalf("seed calls = %d, want 1", runner.seedCalls)
	}
	if runner.migrateCalls != 0 {
		t.Fatalf("migrate calls = %d, want 0", runner.migrateCalls)
	}
	if runner.dsn != "file-user@tcp(localhost:3306)/filedb" {
		t.Fatal("dsn did not come from file config")
	}
	if got := stdout.String(); got != "seed-dev complete\n" {
		t.Fatalf("stdout = %q", got)
	}
}

func TestRunDoesNotDispatchOnInvalidArgs(t *testing.T) {
	runner := &fakeRunner{}
	app := &cli{
		loadConfig: func(path string) (config.Config, error) {
			t.Fatalf("loadConfig called with %q", path)
			return config.Config{}, nil
		},
		runner: runner,
		stdout: &bytes.Buffer{},
		stderr: &bytes.Buffer{},
	}

	if err := app.run(context.Background(), []string{"migrate", "redo"}); err == nil {
		t.Fatal("expected error")
	}
	if runner.migrateCalls != 0 || runner.seedCalls != 0 {
		t.Fatalf("runner was called: migrate=%d seed=%d", runner.migrateCalls, runner.seedCalls)
	}
}

type fakeRunner struct {
	migrateCalls int
	seedCalls    int
	direction    string
	dsn          string
}

func (f *fakeRunner) Migrate(ctx context.Context, cfg config.Config, direction string) error {
	f.migrateCalls++
	f.direction = direction
	f.dsn = cfg.MySQL.DSN
	return nil
}

func (f *fakeRunner) SeedDev(ctx context.Context, cfg config.Config) error {
	f.seedCalls++
	f.dsn = cfg.MySQL.DSN
	return nil
}

func writeTestConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte("mysql:\n  dsn: \"file-user@tcp(localhost:3306)/filedb\"\n")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

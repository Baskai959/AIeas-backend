package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"aieas_backend/internal/config"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pressly/goose/v3"
)

const migrationsDir = "migrations"

type dbRunner struct{}

func (dbRunner) Migrate(ctx context.Context, cfg config.Config, direction string) error {
	db, err := openMySQL(ctx, cfg.MySQL)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := goose.SetDialect("mysql"); err != nil {
		return fmt.Errorf("set goose dialect: %w", err)
	}
	dir, err := resolvePath(migrationsDir)
	if err != nil {
		return err
	}

	switch direction {
	case "up":
		return wrapGooseErr("migrate up", goose.Up(db, dir))
	case "down":
		return wrapGooseErr("migrate down", goose.Down(db, dir))
	case "status":
		return wrapGooseErr("migrate status", goose.Status(db, dir))
	default:
		return fmt.Errorf("unsupported migrate direction %q", direction)
	}
}

func (dbRunner) SeedDev(ctx context.Context, cfg config.Config) error {
	db, err := openMySQL(ctx, cfg.MySQL)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := seedDevUsers(ctx, db); err != nil {
		return err
	}
	return nil
}

func openMySQL(ctx context.Context, cfg config.MySQLConfig) (*sql.DB, error) {
	db, err := sql.Open("mysql", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime.Std() > 0 {
		db.SetConnMaxLifetime(cfg.ConnMaxLifetime.Std())
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	return db, nil
}

func wrapGooseErr(action string, err error) error {
	if err != nil {
		return fmt.Errorf("%s: %w", action, err)
	}
	return nil
}

func resolvePath(path string) (string, error) {
	if filepath.IsAbs(path) {
		if _, err := os.Stat(path); err != nil {
			return "", fmt.Errorf("stat %q: %w", path, err)
		}
		return path, nil
	}

	wd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get working directory: %w", err)
	}
	for {
		candidate := filepath.Join(wd, path)
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
		parent := filepath.Dir(wd)
		if parent == wd {
			break
		}
		wd = parent
	}
	return "", fmt.Errorf("%q not found from current directory or parents", path)
}

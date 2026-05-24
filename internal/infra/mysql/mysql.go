package mysql

import (
	"context"
	"fmt"
	"time"

	"aieas_backend/internal/config"

	mysqlgorm "gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// Open 打开 MySQL 连接。logger 为 nil 时回退到 GORM 默认 logger（仅用于不需要日志桥接的场景，如 CLI）。
func Open(ctx context.Context, cfg config.MySQLConfig, logger gormlogger.Interface) (*gorm.DB, error) {
	gormCfg := &gorm.Config{}
	if logger != nil {
		gormCfg.Logger = logger
	}
	db, err := gorm.Open(mysqlgorm.Open(cfg.DSN), gormCfg)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("unwrap mysql db: %w", err)
	}
	if cfg.MaxOpenConns > 0 {
		sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime.Std() > 0 {
		sqlDB.SetConnMaxLifetime(cfg.ConnMaxLifetime.Std())
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := sqlDB.PingContext(pingCtx); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	return db, nil
}

func Close(db *gorm.DB) error {
	if db == nil {
		return nil
	}
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("unwrap mysql db: %w", err)
	}
	return sqlDB.Close()
}

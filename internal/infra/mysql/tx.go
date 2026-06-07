package mysql

import (
	"context"

	"gorm.io/gorm"
)

type TxManager interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

type txKey struct{}

type GORMTxManager struct {
	db *gorm.DB
}

func NewGORMTxManager(db *gorm.DB) *GORMTxManager {
	return &GORMTxManager{db: db}
}

func (m *GORMTxManager) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if tx := DBFromContext(ctx); tx != nil {
		return fn(ctx)
	}
	return m.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return fn(context.WithValue(ctx, txKey{}, tx))
	})
}

func DBFromContext(ctx context.Context) *gorm.DB {
	if tx, ok := ctx.Value(txKey{}).(*gorm.DB); ok {
		return tx
	}
	return nil
}

func ResolveDB(ctx context.Context, base *gorm.DB) *gorm.DB {
	if tx := DBFromContext(ctx); tx != nil {
		return tx
	}
	return base.WithContext(ctx)
}

type NoopTxManager struct{}

func (NoopTxManager) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}

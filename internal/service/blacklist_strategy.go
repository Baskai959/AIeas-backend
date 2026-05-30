package service

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/repository"
)

const (
	blacklistStrategyDescription = "自动黑名单策略：频控、异常高价、保证金未满足"
	systemBlacklistActorID       = "0"
)

func readBlacklistStrategyConfig(ctx context.Context, configs repository.ConfigRepository) (domain.BlacklistStrategyConfig, error) {
	cfg := domain.DefaultBlacklistStrategyConfig()
	if configs == nil {
		return cfg, nil
	}
	item, err := configs.FindByKey(ctx, domain.ConfigKeyBlacklistStrategy)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return cfg, nil
		}
		return domain.BlacklistStrategyConfig{}, err
	}
	if len(item.Value) == 0 {
		return cfg, nil
	}
	if err := json.Unmarshal(item.Value, &cfg); err != nil {
		return domain.BlacklistStrategyConfig{}, domain.ErrInvalidArgument
	}
	return domain.NormalizeBlacklistStrategyConfig(cfg)
}

func upsertBlacklistStrategyConfig(ctx context.Context, configs repository.ConfigRepository, cfg domain.BlacklistStrategyConfig, actorID string) (domain.BlacklistStrategyConfig, error) {
	if configs == nil {
		return domain.BlacklistStrategyConfig{}, domain.ErrInvalidState
	}
	normalized, err := domain.NormalizeBlacklistStrategyConfig(cfg)
	if err != nil {
		return domain.BlacklistStrategyConfig{}, err
	}
	raw, err := json.Marshal(normalized)
	if err != nil {
		return domain.BlacklistStrategyConfig{}, err
	}
	item := domain.ConfigItem{
		Key:         domain.ConfigKeyBlacklistStrategy,
		Value:       raw,
		Description: blacklistStrategyDescription,
		UpdatedBy:   actorID,
		UpdatedAt:   time.Now().UTC(),
	}
	if err := configs.Upsert(ctx, &item); err != nil {
		return domain.BlacklistStrategyConfig{}, err
	}
	return normalized, nil
}

func blacklistExpiresAt(cfg domain.BlacklistStrategyConfig, now time.Time) *time.Time {
	if cfg.BlacklistDurationSeconds <= 0 {
		return nil
	}
	expiresAt := now.Add(time.Duration(cfg.BlacklistDurationSeconds) * time.Second)
	return &expiresAt
}

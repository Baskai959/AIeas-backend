package redis

import (
	"context"
	"fmt"
	"time"

	"aieas_backend/internal/config"

	redisgo "github.com/redis/go-redis/v9"
)

func Open(ctx context.Context, cfg config.RedisConfig) (*redisgo.Client, error) {
	client := redisgo.NewClient(&redisgo.Options{
		Addr:     cfg.Addr,
		Username: cfg.Username,
		Password: cfg.Password,
		DB:       cfg.DB,
		PoolSize: cfg.PoolSize,
	})

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return client, nil
}

package redis

import (
	"context"
	"fmt"
	"time"

	"aieas_backend/internal/config"

	redisgo "github.com/redis/go-redis/v9"
)

func Open(ctx context.Context, cfg config.RedisInstanceConfig) (*redisgo.Client, error) {
	client := redisgo.NewClient(&redisgo.Options{
		Addr:         cfg.Addr,
		Username:     cfg.Username,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.PoolSize / 4,
		PoolTimeout:  3 * time.Second,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 1 * time.Second,
	})

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return client, nil
}

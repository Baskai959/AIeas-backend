package http

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	redisgo "github.com/redis/go-redis/v9"
)

type RedisIdempotencyClient interface {
	Get(ctx context.Context, key string) *redisgo.StringCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redisgo.StatusCmd
}

type RedisIdempotencyStore struct {
	client RedisIdempotencyClient
	prefix string
}

func NewRedisIdempotencyStore(client RedisIdempotencyClient, prefix string) *RedisIdempotencyStore {
	prefix = strings.Trim(strings.TrimSpace(prefix), ":")
	if prefix == "" {
		prefix = "idempotency"
	}
	return &RedisIdempotencyStore{client: client, prefix: prefix}
}

func (s *RedisIdempotencyStore) Get(ctx context.Context, key string) (IdempotencyRecord, bool, error) {
	if s == nil || s.client == nil {
		return IdempotencyRecord{}, false, nil
	}
	raw, err := s.client.Get(ctx, s.redisKey(key)).Bytes()
	if err == redisgo.Nil {
		return IdempotencyRecord{}, false, nil
	}
	if err != nil {
		return IdempotencyRecord{}, false, err
	}
	var record IdempotencyRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return IdempotencyRecord{}, false, err
	}
	record.Body = append([]byte(nil), record.Body...)
	return record, true, nil
}

func (s *RedisIdempotencyStore) Set(ctx context.Context, key string, record IdempotencyRecord, ttl time.Duration) error {
	if s == nil || s.client == nil {
		return nil
	}
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	record.Body = append([]byte(nil), record.Body...)
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return s.client.Set(ctx, s.redisKey(key), raw, ttl).Err()
}

func (s *RedisIdempotencyStore) redisKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return s.prefix + ":" + hex.EncodeToString(sum[:])
}

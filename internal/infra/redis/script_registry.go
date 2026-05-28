package redis

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"aieas_backend/internal/infra/observability/metrics"

	redisgo "github.com/redis/go-redis/v9"
)

// ScriptClient 是 ScriptRegistry 调用 Redis 端 SCRIPT LOAD / EVALSHA 时依赖的
// 最小子集；*redisgo.Client 与 *RedisRTClient 都满足。
type ScriptClient interface {
	ScriptLoad(ctx context.Context, script string) *redisgo.StringCmd
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redisgo.Cmd
}

// ScriptRegistry 在多个 RT shard 上加载并缓存 Lua 脚本的 SHA。
//
// 设计要点：
//   - shard 列表与 ShardedRTClient 一一对应；每个 shard 维护独立的 sha 表。
//   - LoadAll 在每个 shard 上 SCRIPT LOAD 全部脚本；任一 shard 失败即返回错误。
//   - EvalOnShard 在指定 shard 上 EVALSHA；遇到 NOSCRIPT 自动重新 LOAD 一次再重试。
//   - 兼容历史单 shard 测试：NewScriptRegistry(client, scripts) 仍然可用，
//     此时所有 EvalOnShard 都落到 shard 0。
type ScriptRegistry struct {
	shardClients []ScriptClient
	mu           sync.RWMutex
	scripts      map[string]string   // name -> body
	sha          []map[string]string // [shardIdx][name] -> sha
	metrics      *metrics.Registry
}

// Script 仅用于对外暴露 SHA 名称对（保持兼容历史 SHA(name) API）；
// 内部统一用 scripts/sha 两张表表达。
type Script struct {
	Name string
	Body string
	SHA  string
}

// NewScriptRegistry 构造单 shard 的 ScriptRegistry，主要给单元测试与 miniredis 用。
func NewScriptRegistry(client ScriptClient, scripts map[string]string) *ScriptRegistry {
	r := newRegistryWithBodies(scripts)
	r.shardClients = []ScriptClient{client}
	r.sha = []map[string]string{makeShaMap(scripts)}
	return r
}

// NewShardedScriptRegistry 基于 ShardedRTClient 构造多 shard 注册表。
// sharded 为 nil / 空时退化为零 shard 注册表，所有 EvalOnShard 会返回错误。
func NewShardedScriptRegistry(sharded *ShardedRTClient, scripts map[string]string) *ScriptRegistry {
	r := newRegistryWithBodies(scripts)
	if sharded == nil {
		return r
	}
	shards := sharded.Shards()
	r.shardClients = make([]ScriptClient, 0, len(shards))
	r.sha = make([]map[string]string, 0, len(shards))
	for _, c := range shards {
		r.shardClients = append(r.shardClients, c)
		r.sha = append(r.sha, makeShaMap(scripts))
	}
	return r
}

func newRegistryWithBodies(scripts map[string]string) *ScriptRegistry {
	body := make(map[string]string, len(scripts))
	for name, b := range scripts {
		body[name] = b
	}
	return &ScriptRegistry{scripts: body}
}

func makeShaMap(scripts map[string]string) map[string]string {
	out := make(map[string]string, len(scripts))
	for name := range scripts {
		out[name] = ""
	}
	return out
}

// SetMetrics 注入观测性 Registry。nil 安全。
func (r *ScriptRegistry) SetMetrics(reg *metrics.Registry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metrics = reg
}

func (r *ScriptRegistry) metricsSnapshot() *metrics.Registry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.metrics
}

// LoadAll 在每个 shard 上 SCRIPT LOAD 所有已注册脚本。
func (r *ScriptRegistry) LoadAll(ctx context.Context) error {
	r.mu.RLock()
	names := make([]string, 0, len(r.scripts))
	for name := range r.scripts {
		names = append(names, name)
	}
	shardCount := len(r.shardClients)
	r.mu.RUnlock()
	for shard := 0; shard < shardCount; shard++ {
		for _, name := range names {
			if _, err := r.loadOnShard(ctx, shard, name); err != nil {
				return fmt.Errorf("load script %q on shard %d: %w", name, shard, err)
			}
		}
	}
	return nil
}

// Eval 是历史的单 shard 入口，固定走 shard 0；保留供单 shard 单元测试使用。
func (r *ScriptRegistry) Eval(ctx context.Context, name string, keys []string, args ...interface{}) (interface{}, error) {
	return r.EvalOnShard(ctx, 0, name, keys, args...)
}

// EvalOnShard 在指定 shard 上 EVALSHA，遇 NOSCRIPT 自动重新 LOAD 后重试一次。
func (r *ScriptRegistry) EvalOnShard(ctx context.Context, shard int, name string, keys []string, args ...interface{}) (interface{}, error) {
	body, ok := r.scriptBody(name)
	if !ok {
		return nil, fmt.Errorf("redis script %q: %w", name, ErrScriptNotFound)
	}
	client, err := r.shardClient(shard)
	if err != nil {
		return nil, err
	}
	sha := r.shaFor(shard, name)
	if sha == "" {
		var loadErr error
		sha, loadErr = r.loadOnShardWithBody(ctx, shard, name, body)
		if loadErr != nil {
			r.recordEvalMetrics(name, 0, loadErr)
			return nil, loadErr
		}
	}
	start := time.Now()
	result, err := client.EvalSha(ctx, sha, keys, args...).Result()
	if err == nil {
		r.recordEvalMetrics(name, time.Since(start), nil)
		return result, nil
	}
	if !isNoScript(err) {
		r.recordEvalMetrics(name, time.Since(start), err)
		return nil, err
	}
	sha, loadErr := r.loadOnShardWithBody(ctx, shard, name, body)
	if loadErr != nil {
		r.recordEvalMetrics(name, time.Since(start), loadErr)
		return nil, loadErr
	}
	retryStart := time.Now()
	result, err = client.EvalSha(ctx, sha, keys, args...).Result()
	r.recordEvalMetrics(name, time.Since(retryStart), err)
	return result, err
}

func (r *ScriptRegistry) recordEvalMetrics(name string, elapsed time.Duration, err error) {
	reg := r.metricsSnapshot()
	if reg == nil {
		return
	}
	errClass := ""
	if err != nil {
		errClass = classifyRedisLuaError(err)
	}
	reg.ObserveRedisLua(name, elapsed, errClass)
}

// classifyRedisLuaError 把底层错误归并为低基数 label：避免裸错误信息进入指标维度。
func classifyRedisLuaError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToUpper(err.Error())
	switch {
	case strings.Contains(msg, "NOSCRIPT"):
		return "noscript"
	case strings.Contains(msg, "BUSY"):
		return "busy"
	case strings.Contains(msg, "TIMEOUT") || strings.Contains(msg, "DEADLINE"):
		return "timeout"
	case strings.Contains(msg, "CONNECTION"):
		return "connection"
	default:
		return "error"
	}
}

// SHA 返回 shard 0 上的 SHA（历史 API 兼容路径）。
func (r *ScriptRegistry) SHA(name string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if _, ok := r.scripts[name]; !ok {
		return "", false
	}
	if len(r.sha) == 0 {
		return "", true
	}
	return r.sha[0][name], true
}

// SHAOnShard 返回指定 shard 上某脚本的 SHA。
func (r *ScriptRegistry) SHAOnShard(shard int, name string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if _, ok := r.scripts[name]; !ok {
		return "", false
	}
	if shard < 0 || shard >= len(r.sha) {
		return "", false
	}
	return r.sha[shard][name], true
}

// Loaded 报告所有 shard 上的所有脚本是否都已 LOAD（SHA 非空）。
// 任一 shard / 任一脚本未 LOAD 都返回 false；零 shard / 零脚本视为未就绪。
func (r *ScriptRegistry) Loaded() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.scripts) == 0 || len(r.sha) == 0 {
		return false
	}
	for _, shaMap := range r.sha {
		for name := range r.scripts {
			if strings.TrimSpace(shaMap[name]) == "" {
				return false
			}
		}
	}
	return true
}

func (r *ScriptRegistry) scriptBody(name string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	body, ok := r.scripts[name]
	return body, ok
}

func (r *ScriptRegistry) shardClient(shard int) (ScriptClient, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if shard < 0 || shard >= len(r.shardClients) {
		return nil, fmt.Errorf("redis script registry: shard index %d out of range (count=%d)", shard, len(r.shardClients))
	}
	c := r.shardClients[shard]
	if c == nil {
		return nil, fmt.Errorf("redis script registry: shard %d has nil client", shard)
	}
	return c, nil
}

func (r *ScriptRegistry) shaFor(shard int, name string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if shard < 0 || shard >= len(r.sha) {
		return ""
	}
	return r.sha[shard][name]
}

func (r *ScriptRegistry) loadOnShard(ctx context.Context, shard int, name string) (string, error) {
	body, ok := r.scriptBody(name)
	if !ok {
		return "", fmt.Errorf("redis script %q: %w", name, ErrScriptNotFound)
	}
	return r.loadOnShardWithBody(ctx, shard, name, body)
}

func (r *ScriptRegistry) loadOnShardWithBody(ctx context.Context, shard int, name, body string) (string, error) {
	client, err := r.shardClient(shard)
	if err != nil {
		return "", err
	}
	sha, err := client.ScriptLoad(ctx, body).Result()
	if err != nil {
		return "", err
	}
	r.mu.Lock()
	if shard >= 0 && shard < len(r.sha) {
		if r.sha[shard] == nil {
			r.sha[shard] = make(map[string]string)
		}
		r.sha[shard][name] = sha
	}
	r.mu.Unlock()
	return sha, nil
}

func isNoScript(err error) bool {
	return err != nil && strings.Contains(strings.ToUpper(err.Error()), "NOSCRIPT")
}

var ErrScriptNotFound = fmt.Errorf("script not found")

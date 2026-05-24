package redis

import (
	"context"
	"fmt"
	"strings"
	"sync"

	redisgo "github.com/redis/go-redis/v9"
)

type ScriptClient interface {
	ScriptLoad(ctx context.Context, script string) *redisgo.StringCmd
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redisgo.Cmd
}

type ScriptRegistry struct {
	client  ScriptClient
	mu      sync.RWMutex
	scripts map[string]*Script
}

type Script struct {
	Name string
	Body string
	SHA  string
}

func NewScriptRegistry(client ScriptClient, scripts map[string]string) *ScriptRegistry {
	items := make(map[string]*Script, len(scripts))
	for name, body := range scripts {
		items[name] = &Script{Name: name, Body: body}
	}
	return &ScriptRegistry{client: client, scripts: items}
}

func (r *ScriptRegistry) LoadAll(ctx context.Context) error {
	r.mu.RLock()
	scripts := make([]*Script, 0, len(r.scripts))
	for _, script := range r.scripts {
		scripts = append(scripts, script)
	}
	r.mu.RUnlock()
	for _, script := range scripts {
		if _, err := r.load(ctx, script); err != nil {
			return err
		}
	}
	return nil
}

func (r *ScriptRegistry) Eval(ctx context.Context, name string, keys []string, args ...interface{}) (interface{}, error) {
	script, ok := r.script(name)
	if !ok {
		return nil, fmt.Errorf("redis script %q: %w", name, ErrScriptNotFound)
	}
	sha := script.SHA
	if sha == "" {
		var err error
		sha, err = r.load(ctx, script)
		if err != nil {
			return nil, err
		}
	}
	cmd := r.client.EvalSha(ctx, sha, keys, args...)
	result, err := cmd.Result()
	if err == nil {
		return result, nil
	}
	if !isNoScript(err) {
		return nil, err
	}
	sha, err = r.load(ctx, script)
	if err != nil {
		return nil, err
	}
	return r.client.EvalSha(ctx, sha, keys, args...).Result()
}

func (r *ScriptRegistry) SHA(name string) (string, bool) {
	script, ok := r.script(name)
	if !ok {
		return "", false
	}
	return script.SHA, true
}

func (r *ScriptRegistry) script(name string) (*Script, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	script, ok := r.scripts[name]
	return script, ok
}

func (r *ScriptRegistry) load(ctx context.Context, script *Script) (string, error) {
	sha, err := r.client.ScriptLoad(ctx, script.Body).Result()
	if err != nil {
		return "", err
	}
	r.mu.Lock()
	if current, ok := r.scripts[script.Name]; ok {
		current.SHA = sha
	}
	r.mu.Unlock()
	return sha, nil
}

func isNoScript(err error) bool {
	return err != nil && strings.Contains(strings.ToUpper(err.Error()), "NOSCRIPT")
}

var ErrScriptNotFound = fmt.Errorf("script not found")

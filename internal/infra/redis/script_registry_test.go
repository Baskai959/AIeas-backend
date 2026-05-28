package redis

import (
	"context"
	"errors"
	"strings"
	"testing"

	redisgo "github.com/redis/go-redis/v9"
)

func TestKeyBuilder(t *testing.T) {
	keys := NewKeyBuilder("app")
	if got := keys.AuctionState(10001); got != "app:auction:10001:state" {
		t.Fatalf("unexpected auction state key %q", got)
	}
	if got := keys.AuctionIdempotency(10001, "req-1"); got != "app:auction:10001:idem:req-1" {
		t.Fatalf("unexpected idempotency key %q", got)
	}
	if got := keys.AuctionEnrolled(10001); got != "app:auction:10001:enrolled" {
		t.Fatalf("unexpected enrolled key %q", got)
	}
	if got := keys.AuctionSeq(10001); got != "app:auction:10001:seq" {
		t.Fatalf("unexpected seq key %q", got)
	}
	if got := keys.ActiveStreams(); got != "app:auction:active_streams" {
		t.Fatalf("unexpected active streams key %q", got)
	}
	if got := keys.WSInstanceHeartbeat("i1"); got != "app:ws:instance:i1" {
		t.Fatalf("unexpected heartbeat key %q", got)
	}
}

func TestScriptRegistryEvalLoadsAndRetriesNoScript(t *testing.T) {
	ctx := context.Background()
	client := &fakeScriptClient{failFirstEval: true}
	registry := NewScriptRegistry(client, map[string]string{"echo": "return ARGV[1]"})

	result, err := registry.Eval(ctx, "echo", []string{"k"}, "v")
	if err != nil {
		t.Fatalf("eval script: %v", err)
	}
	if result != "ok" {
		t.Fatalf("expected ok result, got %#v", result)
	}
	if client.loadCalls != 2 {
		t.Fatalf("expected initial load plus NOSCRIPT reload, got %d", client.loadCalls)
	}
	if client.evalCalls != 2 {
		t.Fatalf("expected eval retry, got %d", client.evalCalls)
	}
	if sha, ok := registry.SHA("echo"); !ok || sha != "sha-2" {
		t.Fatalf("expected updated sha-2, got %q ok=%v", sha, ok)
	}
}

func TestScriptRegistryUnknownScript(t *testing.T) {
	_, err := NewScriptRegistry(&fakeScriptClient{}, nil).Eval(context.Background(), "missing", nil)
	if !errors.Is(err, ErrScriptNotFound) {
		t.Fatalf("expected ErrScriptNotFound, got %v", err)
	}
}

func TestDefaultScriptsIncludesBidAndHammer(t *testing.T) {
	scripts := DefaultScripts()
	if scripts[ScriptBidPlace] == "" || scripts[ScriptHammer] == "" {
		t.Fatalf("expected bid and hammer scripts, got keys=%v", scripts)
	}
	if script := scripts[ScriptBidPlace]; !containsAll(script, "XADD", "auction_id", "event_type", "stream_id") {
		t.Fatalf("bid script must write bid event fields to stream atomically")
	}
}

func containsAll(text string, needles ...string) bool {
	for _, needle := range needles {
		if !strings.Contains(text, needle) {
			return false
		}
	}
	return true
}

type fakeScriptClient struct {
	loadCalls     int
	evalCalls     int
	failFirstEval bool
}

func (f *fakeScriptClient) ScriptLoad(ctx context.Context, script string) *redisgo.StringCmd {
	_ = script
	f.loadCalls++
	cmd := redisgo.NewStringCmd(ctx)
	cmd.SetVal("sha-" + string(rune('0'+f.loadCalls)))
	return cmd
}

func (f *fakeScriptClient) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redisgo.Cmd {
	_ = sha1
	_ = keys
	_ = args
	f.evalCalls++
	cmd := redisgo.NewCmd(ctx)
	if f.failFirstEval && f.evalCalls == 1 {
		cmd.SetErr(errors.New("NOSCRIPT No matching script. Please use EVAL."))
		return cmd
	}
	cmd.SetVal("ok")
	return cmd
}

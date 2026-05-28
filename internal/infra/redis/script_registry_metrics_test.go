// script_registry_metrics_test.go 验证 ScriptRegistry 接入 metrics 后会上报
// redis_lua_duration_seconds / redis_lua_errors_total，并把底层错误归类成
// 低基数 errClass（noscript / timeout / connection / busy / error）。
package redis

import (
	"context"
	"errors"
	"strings"
	"testing"

	"aieas_backend/internal/infra/observability/metrics"

	dto "github.com/prometheus/client_model/go"
	redisgo "github.com/redis/go-redis/v9"
)

// stubScriptClient 只在 Eval 上报某个错误，便于测试 errClass 分类。
type stubScriptClient struct {
	loadCalls int
	evalErr   error
}

func (s *stubScriptClient) ScriptLoad(ctx context.Context, script string) *redisgo.StringCmd {
	_ = script
	s.loadCalls++
	cmd := redisgo.NewStringCmd(ctx)
	cmd.SetVal("sha-stub")
	return cmd
}

func (s *stubScriptClient) EvalSha(ctx context.Context, sha string, keys []string, args ...interface{}) *redisgo.Cmd {
	_ = sha
	_ = keys
	_ = args
	cmd := redisgo.NewCmd(ctx)
	if s.evalErr != nil {
		cmd.SetErr(s.evalErr)
		return cmd
	}
	cmd.SetVal("ok")
	return cmd
}

func TestScriptRegistryRecordsLuaDurationOnSuccess(t *testing.T) {
	reg := metrics.New(metrics.Options{Enabled: true, Namespace: "t"})
	registry := NewScriptRegistry(&stubScriptClient{}, map[string]string{"echo": "return 1"})
	registry.SetMetrics(reg)

	if _, err := registry.Eval(context.Background(), "echo", []string{"k"}, "v"); err != nil {
		t.Fatalf("eval: %v", err)
	}
	if count := histogramCount(t, reg, "t_redis_lua_duration_seconds", map[string]string{"script": "echo"}); count == 0 {
		t.Fatalf("expected redis_lua_duration_seconds to be observed for echo, got 0")
	}
	if v := counterValue(t, reg, "t_redis_lua_errors_total", nil); v != 0 {
		t.Fatalf("expected no error counters on success path, got %v", v)
	}
}

func TestScriptRegistryClassifiesNoScriptError(t *testing.T) {
	reg := metrics.New(metrics.Options{Enabled: true, Namespace: "t"})
	stub := &stubScriptClient{}
	registry := NewScriptRegistry(stub, map[string]string{"echo": "return 1"})
	registry.SetMetrics(reg)

	// 第一次 eval 返回 NOSCRIPT 触发重新加载；recordEvalMetrics 会以 errClass=noscript 计一次。
	stub.evalErr = errors.New("NOSCRIPT No matching script. Please use EVAL.")
	defer func() { stub.evalErr = nil }()
	// 先做一次失败：第一次 Eval 返回 NOSCRIPT，registry 重试时同样失败（保持 evalErr 持续）。
	_, err := registry.Eval(context.Background(), "echo", nil)
	if err == nil {
		t.Fatalf("expected error from stub, got nil")
	}
	v := counterValue(t, reg, "t_redis_lua_errors_total", map[string]string{"script": "echo", "error": "noscript"})
	if v == 0 {
		t.Fatalf("expected redis_lua_errors_total{error=noscript} > 0, got 0")
	}
}

func TestScriptRegistryClassifiesTimeoutAndConnection(t *testing.T) {
	cases := []struct {
		err   error
		class string
	}{
		{errors.New("operation timeout"), "timeout"},
		{errors.New("context deadline exceeded"), "timeout"},
		{errors.New("connection refused"), "connection"},
		{errors.New("BUSY Redis is busy running a script"), "busy"},
		{errors.New("custom application failure"), "error"},
	}
	for _, tc := range cases {
		reg := metrics.New(metrics.Options{Enabled: true, Namespace: "t"})
		stub := &stubScriptClient{evalErr: tc.err}
		registry := NewScriptRegistry(stub, map[string]string{"echo": "return 1"})
		registry.SetMetrics(reg)
		_, _ = registry.Eval(context.Background(), "echo", nil)
		if v := counterValue(t, reg, "t_redis_lua_errors_total", map[string]string{"script": "echo", "error": tc.class}); v == 0 {
			t.Fatalf("expected errClass=%q for %v, got 0 counter", tc.class, tc.err)
		}
	}
}

// histogramCount 从 registry 中查出 metric 的样本计数；labels 必须全匹配。
func histogramCount(t *testing.T, reg *metrics.Registry, name string, labels map[string]string) uint64 {
	t.Helper()
	families, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			if !labelsMatch(m.GetLabel(), labels) {
				continue
			}
			return m.GetHistogram().GetSampleCount()
		}
	}
	return 0
}

// counterValue 返回 counter 在指定 label 子集下的累计值；labels==nil 时累加所有 label 组合。
func counterValue(t *testing.T, reg *metrics.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := reg.Gatherer().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var total float64
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			if labels != nil && !labelsMatch(m.GetLabel(), labels) {
				continue
			}
			total += m.GetCounter().GetValue()
		}
	}
	return total
}

func labelsMatch(actual []*dto.LabelPair, want map[string]string) bool {
	if want == nil {
		return true
	}
	got := make(map[string]string, len(actual))
	for _, lp := range actual {
		got[lp.GetName()] = lp.GetValue()
	}
	for k, v := range want {
		if !strings.EqualFold(got[k], v) {
			return false
		}
	}
	return true
}

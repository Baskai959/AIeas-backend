// Package cache 中的 observer.go 提供 Observer 的两个常用实现：
//   - JSONCodec[T]: 通用 JSON 序列化器，省去业务每次手写 Marshal/Unmarshal。
//   - RegistryObserver: 把 metrics.Registry 适配到 Observer，避免 cache 包反向
//     依赖 observability/metrics（保持依赖方向 server.go 注入）。
package cache

import (
	"encoding/json"
	"time"
)

// JSONCodec 是基于 encoding/json 的通用 Codec。值/指针类型均可作为 T。
type JSONCodec[T any] struct{}

// Encode 直接调用 json.Marshal；空值（如 nil 指针）由 json 包决定输出。
func (JSONCodec[T]) Encode(value T) ([]byte, error) {
	return json.Marshal(value)
}

// Decode 反序列化为 T；调用方需保证 raw 是 Encode 的产物（负缓存的占位
// payload 在 LayeredCache 内部已经被截获，不会进入 Decode）。
func (JSONCodec[T]) Decode(raw []byte) (T, error) {
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		return v, err
	}
	return v, nil
}

// CacheMetricsRecorder 是 RegistryObserver 期望的最小指标接口。
//
// 把这层接口抽出来是为了：
//  1. cache 包不直接依赖 metrics.Registry 具体类型；
//  2. 测试时可以传入 mock，无需启 Prometheus 注册表。
type CacheMetricsRecorder interface {
	ObserveCacheGet(name string, source string, d time.Duration)
	ObserveCacheSet(name string, d time.Duration, err error)
	ObserveCacheInvalidate(name string, d time.Duration, err error)
}

// RegistryObserver 把任意 CacheMetricsRecorder 适配为 Observer。
//
// recorder 为 nil 时所有方法 no-op，方便在禁用 metrics 部署形态下零开销旁路。
type RegistryObserver struct {
	Recorder CacheMetricsRecorder
}

// ObserveGet 透传到 recorder；source 转成字符串以保持 metrics label 类型稳定。
func (o RegistryObserver) ObserveGet(name string, source Source, d time.Duration) {
	if o.Recorder == nil {
		return
	}
	o.Recorder.ObserveCacheGet(name, string(source), d)
}

// ObserveSet 透传到 recorder。
func (o RegistryObserver) ObserveSet(name string, d time.Duration, err error) {
	if o.Recorder == nil {
		return
	}
	o.Recorder.ObserveCacheSet(name, d, err)
}

// ObserveInvalidate 透传到 recorder。
func (o RegistryObserver) ObserveInvalidate(name string, d time.Duration, err error) {
	if o.Recorder == nil {
		return
	}
	o.Recorder.ObserveCacheInvalidate(name, d, err)
}

// Package cache 中的 observer.go 提供通用 JSON 序列化器，省去业务每次手写 Marshal/Unmarshal。
package cache

import "encoding/json"

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

package repository

import (
	"context"
	"sync"
	"time"
)

// LiveSessionLock 实现"同一时刻一个直播场次内只有一个拍品在拍"的互斥语义。
// 实现可基于 Redis SETNX/Lua 释放，也可使用内存版本用于测试。
type LiveSessionLock interface {
	// Acquire 尝试为 session 抢占 auctionID 的锁。
	// 返回 acquired=true 表示获取成功；false 表示已被其它 auction 持有，
	// 此时 currentAuctionID 为持有锁的 auction（用于错误信息）。
	Acquire(ctx context.Context, sessionID uint64, auctionID uint64, ttl time.Duration) (acquired bool, currentAuctionID uint64, err error)
	// Release 仅当当前持有者匹配 auctionID 时才释放。
	Release(ctx context.Context, sessionID uint64, auctionID uint64) error
	// Current 查询当前持锁 auctionID（0 表示无）。
	Current(ctx context.Context, sessionID uint64) (uint64, error)
}

// MemoryLiveSessionLock 是 LiveSessionLock 的内存实现，主要用于测试。
type MemoryLiveSessionLock struct {
	mu    sync.Mutex
	owned map[uint64]memoryLiveSessionEntry
}

type memoryLiveSessionEntry struct {
	auctionID uint64
	expiresAt time.Time
}

func NewMemoryLiveSessionLock() *MemoryLiveSessionLock {
	return &MemoryLiveSessionLock{owned: make(map[uint64]memoryLiveSessionEntry)}
}

func (l *MemoryLiveSessionLock) Acquire(ctx context.Context, sessionID uint64, auctionID uint64, ttl time.Duration) (bool, uint64, error) {
	_ = ctx
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if entry, ok := l.owned[sessionID]; ok {
		if !entry.expiresAt.IsZero() && entry.expiresAt.Before(now) {
			delete(l.owned, sessionID)
		} else if entry.auctionID == auctionID {
			l.owned[sessionID] = memoryLiveSessionEntry{auctionID: auctionID, expiresAt: maybeExpiry(now, ttl)}
			return true, auctionID, nil
		} else {
			return false, entry.auctionID, nil
		}
	}
	l.owned[sessionID] = memoryLiveSessionEntry{auctionID: auctionID, expiresAt: maybeExpiry(now, ttl)}
	return true, auctionID, nil
}

func (l *MemoryLiveSessionLock) Release(ctx context.Context, sessionID uint64, auctionID uint64) error {
	_ = ctx
	l.mu.Lock()
	defer l.mu.Unlock()
	entry, ok := l.owned[sessionID]
	if !ok {
		return nil
	}
	if entry.auctionID == auctionID {
		delete(l.owned, sessionID)
	}
	return nil
}

func (l *MemoryLiveSessionLock) Current(ctx context.Context, sessionID uint64) (uint64, error) {
	_ = ctx
	l.mu.Lock()
	defer l.mu.Unlock()
	entry, ok := l.owned[sessionID]
	if !ok {
		return 0, nil
	}
	if !entry.expiresAt.IsZero() && entry.expiresAt.Before(time.Now()) {
		delete(l.owned, sessionID)
		return 0, nil
	}
	return entry.auctionID, nil
}

func maybeExpiry(now time.Time, ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return now.Add(ttl)
}

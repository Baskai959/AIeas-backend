package repository

import (
	"context"
	"sync"
	"time"
)

// LiveRoomLock 实现"同一时刻一个直播间内只有一个拍品在拍"的互斥语义。
// 实现可基于 Redis SETNX/Lua 释放，也可使用内存版本用于测试。
type LiveRoomLock interface {
	// Acquire 尝试为 room 抢占 auctionID 的锁。
	// 返回 acquired=true 表示获取成功；false 表示已被其它 auction 持有，
	// 此时 currentAuctionID 为持有锁的 auction（用于错误信息）。
	Acquire(ctx context.Context, roomID uint64, auctionID uint64, ttl time.Duration) (acquired bool, currentAuctionID uint64, err error)
	// Release 仅当当前持有者匹配 auctionID 时才释放。
	Release(ctx context.Context, roomID uint64, auctionID uint64) error
	// Current 查询当前持锁 auctionID（0 表示无）。
	Current(ctx context.Context, roomID uint64) (uint64, error)
}

// MemoryLiveRoomLock 是 LiveRoomLock 的内存实现，主要用于测试。
type MemoryLiveRoomLock struct {
	mu    sync.Mutex
	owned map[uint64]memoryLiveRoomEntry
}

type memoryLiveRoomEntry struct {
	auctionID uint64
	expiresAt time.Time
}

func NewMemoryLiveRoomLock() *MemoryLiveRoomLock {
	return &MemoryLiveRoomLock{owned: make(map[uint64]memoryLiveRoomEntry)}
}

func (l *MemoryLiveRoomLock) Acquire(ctx context.Context, roomID uint64, auctionID uint64, ttl time.Duration) (bool, uint64, error) {
	_ = ctx
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if entry, ok := l.owned[roomID]; ok {
		if !entry.expiresAt.IsZero() && entry.expiresAt.Before(now) {
			delete(l.owned, roomID)
		} else if entry.auctionID == auctionID {
			l.owned[roomID] = memoryLiveRoomEntry{auctionID: auctionID, expiresAt: maybeExpiry(now, ttl)}
			return true, auctionID, nil
		} else {
			return false, entry.auctionID, nil
		}
	}
	l.owned[roomID] = memoryLiveRoomEntry{auctionID: auctionID, expiresAt: maybeExpiry(now, ttl)}
	return true, auctionID, nil
}

func (l *MemoryLiveRoomLock) Release(ctx context.Context, roomID uint64, auctionID uint64) error {
	_ = ctx
	l.mu.Lock()
	defer l.mu.Unlock()
	entry, ok := l.owned[roomID]
	if !ok {
		return nil
	}
	if entry.auctionID == auctionID {
		delete(l.owned, roomID)
	}
	return nil
}

func (l *MemoryLiveRoomLock) Current(ctx context.Context, roomID uint64) (uint64, error) {
	_ = ctx
	l.mu.Lock()
	defer l.mu.Unlock()
	entry, ok := l.owned[roomID]
	if !ok {
		return 0, nil
	}
	if !entry.expiresAt.IsZero() && entry.expiresAt.Before(time.Now()) {
		delete(l.owned, roomID)
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

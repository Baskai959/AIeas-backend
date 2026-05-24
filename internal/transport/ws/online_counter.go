package ws

import (
	"context"
	"sync"
)

type MemoryOnlineCounter struct {
	mu      sync.RWMutex
	members map[uint64]map[string]struct{}
}

func NewMemoryOnlineCounter() *MemoryOnlineCounter {
	return &MemoryOnlineCounter{members: make(map[uint64]map[string]struct{})}
}

func (c *MemoryOnlineCounter) Join(ctx context.Context, auctionID uint64, connectionID string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	room := c.members[auctionID]
	if room == nil {
		room = make(map[string]struct{})
		c.members[auctionID] = room
	}
	room[connectionID] = struct{}{}
	return len(room), nil
}

func (c *MemoryOnlineCounter) Leave(ctx context.Context, auctionID uint64, connectionID string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	room := c.members[auctionID]
	if room == nil {
		return 0, nil
	}
	delete(room, connectionID)
	count := len(room)
	if count == 0 {
		delete(c.members, auctionID)
	}
	return count, nil
}

func (c *MemoryOnlineCounter) Touch(ctx context.Context, auctionID uint64, connectionID string) (int, error) {
	return c.Join(ctx, auctionID, connectionID)
}

func (c *MemoryOnlineCounter) Count(ctx context.Context, auctionID uint64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.members[auctionID]), nil
}

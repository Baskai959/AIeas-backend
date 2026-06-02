package ws

import (
	"context"
	"strings"
	"sync"
)

type MemoryOnlineCounter struct {
	mu      sync.RWMutex
	members map[uint64]map[string]map[string]struct{}
}

func NewMemoryOnlineCounter() *MemoryOnlineCounter {
	return &MemoryOnlineCounter{members: make(map[uint64]map[string]map[string]struct{})}
}

func (c *MemoryOnlineCounter) Join(ctx context.Context, auctionID uint64, connectionID, userID string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	room := c.members[auctionID]
	if room == nil {
		room = make(map[string]map[string]struct{})
		c.members[auctionID] = room
	}
	userID = memoryOnlineUserID(userID, connectionID)
	conns := room[userID]
	if conns == nil {
		conns = make(map[string]struct{})
		room[userID] = conns
	}
	conns[connectionID] = struct{}{}
	return len(room), nil
}

func (c *MemoryOnlineCounter) Leave(ctx context.Context, auctionID uint64, connectionID, userID string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	room := c.members[auctionID]
	if room == nil {
		return 0, nil
	}
	userID = memoryOnlineUserID(userID, connectionID)
	if conns := room[userID]; conns != nil {
		delete(conns, connectionID)
		if len(conns) == 0 {
			delete(room, userID)
		}
	}
	count := len(room)
	if count == 0 {
		delete(c.members, auctionID)
	}
	return count, nil
}

func (c *MemoryOnlineCounter) Touch(ctx context.Context, auctionID uint64, connectionID, userID string) (int, error) {
	return c.Join(ctx, auctionID, connectionID, userID)
}

func (c *MemoryOnlineCounter) Count(ctx context.Context, auctionID uint64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.members[auctionID]), nil
}

func memoryOnlineUserID(userID, connectionID string) string {
	if strings.TrimSpace(userID) == "" {
		return "conn:" + connectionID
	}
	return "user:" + userID
}
